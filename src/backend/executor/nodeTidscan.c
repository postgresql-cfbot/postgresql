/*-------------------------------------------------------------------------
 *
 * nodeTidscan.c
 *	  Routines to support direct tid scans of relations
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeTidscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *
 *		ExecTidScan			scans a relation using tids
 *		ExecInitTidScan		creates and initializes state info.
 *		ExecReScanTidScan	rescans the tid relation.
 *		ExecEndTidScan		releases all storage.
 */
#include "postgres.h"

#include <limits.h>

#include "access/relscan.h"
#include "access/sysattr.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "common/int.h"
#include "executor/execdebug.h"
#include "executor/nodeTidscan.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "storage/bufmgr.h"
#include "utils/array.h"
#include "utils/rel.h"


#define IsCTIDVar(node)  \
	((node) != NULL && \
	 IsA((node), Var) && \
	 ((Var *) (node))->varattno == SelfItemPointerAttributeNumber && \
	 ((Var *) (node))->varlevelsup == 0)

typedef enum
{
	TIDEXPR_IN_ARRAY,
	TIDEXPR_EQ,
	TIDEXPR_UPPER_BOUND,
	TIDEXPR_LOWER_BOUND
}			TidExprType;

/* one element in TidExpr's opexprs */
typedef struct TidOpExpr
{
	TidExprType exprtype;		/* type of op */
	ExprState  *exprstate;		/* ExprState for a TID-yielding subexpr */
	bool		inclusive;		/* whether op is inclusive */
}			TidOpExpr;

/* one element in tss_tidexprs */
typedef struct TidExpr
{
	List	   *opexprs;		/* list of individual op exprs */
	CurrentOfExpr *cexpr;		/* For TIDEXPR_CURRENT_OF */
} TidExpr;

/* a range of tids to scan */
typedef struct TidRange
{
	ItemPointerData first;
	ItemPointerData last;
}			TidRange;

/*
 * During construction of the tidrange array, we need to pass it around with its
 * current size and allocated size.  We bundle them into this struct for
 * convenience.
 */
typedef struct TidRangeArray
{
	TidRange   *ranges;
	int			numRanges;
	int			numAllocated;
}			TidRangeArray;

static TidOpExpr * MakeTidOpExpr(OpExpr *expr, TidScanState *tidstate);
static TidOpExpr * MakeTidScalarArrayOpExpr(ScalarArrayOpExpr *saop,
											TidScanState *tidstate);
static List *MakeTidOpExprList(List *exprs, TidScanState *tidstate);
static void TidExprListCreate(TidScanState *tidstate);
static void EnsureTidRangeSpace(TidRangeArray * tidRangeArray, int numNewItems);
static void AddTidRange(TidRangeArray * tidRangeArray,
			ItemPointer first,
			ItemPointer last);
static bool SetTidLowerBound(ItemPointer tid, bool inclusive,
				 ItemPointer lowerBound);
static bool SetTidUpperBound(ItemPointer tid, bool inclusive,
				 ItemPointer upperBound);
static void TidListEval(TidScanState *tidstate);
static bool MergeTidRanges(TidRange * a, TidRange * b);
static int	tidrange_comparator(const void *a, const void *b);
static HeapScanDesc BeginTidRangeScan(TidScanState *node, TidRange * range);
static HeapTuple NextInTidRange(HeapScanDesc scandesc, ScanDirection direction,
			   TidRange * range);
static TupleTableSlot *TidNext(TidScanState *node);


/*
 * For the given 'expr', build and return an appropriate TidOpExpr taking into
 * account the expr's operator and operand order.
 */
static TidOpExpr *
MakeTidOpExpr(OpExpr *expr, TidScanState *tidstate)
{
	Node	   *arg1 = get_leftop((Expr *) expr);
	Node	   *arg2 = get_rightop((Expr *) expr);
	ExprState  *exprstate = NULL;
	bool		invert = false;
	TidOpExpr  *tidopexpr;

	if (IsCTIDVar(arg1))
		exprstate = ExecInitExpr((Expr *) arg2, &tidstate->ss.ps);
	else if (IsCTIDVar(arg2))
	{
		exprstate = ExecInitExpr((Expr *) arg1, &tidstate->ss.ps);
		invert = true;
	}
	else
		elog(ERROR, "could not identify CTID variable");

	tidopexpr = (TidOpExpr *) palloc0(sizeof(TidOpExpr));

	switch (expr->opno)
	{
		case TIDLessEqOperator:
			tidopexpr->inclusive = true;
			/* fall through */
		case TIDLessOperator:
			tidopexpr->exprtype = invert ? TIDEXPR_LOWER_BOUND : TIDEXPR_UPPER_BOUND;
			break;
		case TIDGreaterEqOperator:
			tidopexpr->inclusive = true;
			/* fall through */
		case TIDGreaterOperator:
			tidopexpr->exprtype = invert ? TIDEXPR_UPPER_BOUND : TIDEXPR_LOWER_BOUND;
			break;
		default:
			tidopexpr->exprtype = TIDEXPR_EQ;
	}

	tidopexpr->exprstate = exprstate;

	return tidopexpr;
}

/* For the given 'saop', build and return a TidOpExpr for the scalar array op. */
static TidOpExpr *
MakeTidScalarArrayOpExpr(ScalarArrayOpExpr *saop, TidScanState *tidstate)
{
	TidOpExpr  *tidopexpr;

	Assert(IsCTIDVar(linitial(saop->args)));

	tidopexpr = (TidOpExpr *) palloc0(sizeof(TidOpExpr));
	tidopexpr->exprstate = ExecInitExpr(lsecond(saop->args),
										&tidstate->ss.ps);
	tidopexpr->exprtype = TIDEXPR_IN_ARRAY;

	return tidopexpr;
}

/*
 * Build and return a list of TidOpExprs the the given list of exprs, which
 * are assumed to be OpExprs.
 */
static List *
MakeTidOpExprList(List *exprs, TidScanState *tidstate)
{
	ListCell   *l;
	List	   *tidopexprs = NIL;

	foreach(l, exprs)
	{
		OpExpr	   *opexpr = lfirst(l);
		TidOpExpr  *tidopexpr = MakeTidOpExpr(opexpr, tidstate);

		tidopexprs = lappend(tidopexprs, tidopexpr);
	}

	return tidopexprs;
}

/*
 * Extract the qual subexpressions that yield TIDs to search for,
 * and compile them into ExprStates if they're ordinary expressions.
 *
 * CURRENT OF is a special case that we can't compile usefully;
 * just drop it into the TidExpr list as-is.
 */
static void
TidExprListCreate(TidScanState *tidstate)
{
	TidScan    *node = (TidScan *) tidstate->ss.ps.plan;
	ListCell   *l;

	tidstate->tss_tidexprs = NIL;
	tidstate->tss_isCurrentOf = false;

	/*
	 * If no quals were specified, then a complete scan is assumed.  Make a
	 * TidExpr with an empty list of TidOpExprs.
	 */
	if (node->tidquals == NIL)
	{
		TidExpr    *tidexpr = (TidExpr *) palloc0(sizeof(TidExpr));

		tidstate->tss_tidexprs = lappend(tidstate->tss_tidexprs, tidexpr);
		return;
	}

	foreach(l, node->tidquals)
	{
		Expr	   *expr = (Expr *) lfirst(l);
		TidExpr    *tidexpr = (TidExpr *) palloc0(sizeof(TidExpr));

		if (is_opclause(expr))
		{
			OpExpr	   *opexpr = (OpExpr *) expr;
			TidOpExpr  *tidopexpr = MakeTidOpExpr(opexpr, tidstate);

			tidexpr->opexprs = list_make1(tidopexpr);
		}
		else if (expr && IsA(expr, ScalarArrayOpExpr))
		{
			ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) expr;
			TidOpExpr  *tidopexpr = MakeTidScalarArrayOpExpr(saop, tidstate);

			tidexpr->opexprs = list_make1(tidopexpr);
		}
		else if (expr && IsA(expr, CurrentOfExpr))
		{
			CurrentOfExpr *cexpr = (CurrentOfExpr *) expr;

			/* For CURRENT OF, save the expression in the TidExpr. */
			tidexpr->cexpr = cexpr;
			tidstate->tss_isCurrentOf = true;
		}
		else if (and_clause((Node *) expr))
		{
			tidexpr->opexprs = MakeTidOpExprList(((BoolExpr *) expr)->args,
												 tidstate);
		}
		else
			elog(ERROR, "could not identify CTID expression");

		tidstate->tss_tidexprs = lappend(tidstate->tss_tidexprs, tidexpr);
	}

	/* CurrentOfExpr could never appear OR'd with something else */
	Assert(list_length(tidstate->tss_tidexprs) == 1 ||
		   !tidstate->tss_isCurrentOf);
}

/*
 * Ensure the array of TidRange objects has enough space for new items.
 * Will allocate the array if not yet allocated, and reallocate it if
 * necessary to accomodate new items.
 */
void
EnsureTidRangeSpace(TidRangeArray * tidRangeArray, int numNewItems)
{
	int			requiredSize;

#define MaxTidRanges ((Size) (MaxAllocSize / sizeof(TidRange)))

	/*
	 * This addition should be fine, since numNewItems won't exceed the
	 * maximum array size, which is MaxAllocSize/sizeof(Datum) (see
	 * ArrayGetNItems).
	 */
	requiredSize = tidRangeArray->numRanges + numNewItems;

	if (requiredSize > MaxTidRanges)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("number of tid ranges exceeds the maximum allowed (%d)",
						(int) MaxTidRanges)));

	if (requiredSize <= tidRangeArray->numAllocated)
		return;

	/*
	 * If allocating the array for the first time, start with a size that will
	 * fit nicely into a power of 2 bytes with little wastage.
	 */
#define InitialTidArraySize (int) (256/sizeof(TidRange))

	if (tidRangeArray->numAllocated == 0)
		tidRangeArray->numAllocated = InitialTidArraySize;

	/* It's not safe to double the size unless we're less than half INT_MAX. */
	Assert(requiredSize < INT_MAX / 2);

	while (tidRangeArray->numAllocated < requiredSize)
		tidRangeArray->numAllocated *= 2;

	if (tidRangeArray->ranges == NULL)
		tidRangeArray->ranges = (TidRange *)
			palloc0(tidRangeArray->numAllocated * sizeof(TidRange));
	else
		tidRangeArray->ranges = (TidRange *)
			repalloc(tidRangeArray->ranges,
					 tidRangeArray->numAllocated * sizeof(TidRange));
}

/*
 * Add a tid range to the array.
 *
 * Note: we assume that space for the additional item has already been ensured
 * by the caller!
 */
void
AddTidRange(TidRangeArray * tidRangeArray, ItemPointer first, ItemPointer last)
{
	tidRangeArray->ranges[tidRangeArray->numRanges].first = *first;
	tidRangeArray->ranges[tidRangeArray->numRanges].last = *last;
	tidRangeArray->numRanges++;
}

/*
 * Set a lower bound tid, taking into account the inclusivity of the bound.
 * Return true if the bound is valid.
 */
static bool
SetTidLowerBound(ItemPointer tid, bool inclusive, ItemPointer lowerBound)
{
	OffsetNumber offset;

	*lowerBound = *tid;
	offset = ItemPointerGetOffsetNumberNoCheck(tid);

	if (!inclusive)
	{
		/* Check if the lower bound is actually in the next block. */
		if (offset >= MaxOffsetNumber)
		{
			BlockNumber block = ItemPointerGetBlockNumberNoCheck(lowerBound);

			/*
			 * If the lower bound was already at or above the maximum block
			 * number, then there is no valid range.
			 */
			if (block >= MaxBlockNumber)
				return false;

			ItemPointerSetBlockNumber(lowerBound, block + 1);
			ItemPointerSetOffsetNumber(lowerBound, 1);
		}
		else
			ItemPointerSetOffsetNumber(lowerBound, OffsetNumberNext(offset));
	}
	else if (offset == 0)
		ItemPointerSetOffsetNumber(lowerBound, 1);

	return true;
}

/*
 * Set an upper bound tid, taking into account the inclusivity of the bound.
 * Return true if the bound is valid.
 */
static bool
SetTidUpperBound(ItemPointer tid, bool inclusive, ItemPointer upperBound)
{
	OffsetNumber offset;

	*upperBound = *tid;
	offset = ItemPointerGetOffsetNumberNoCheck(tid);

	/*
	 * Since TID offsets start at 1, an inclusive upper bound with offset 0
	 * can be treated as an exclusive bound.  This has the benefit of
	 * eliminating that block from the scan range.
	 */
	if (inclusive && offset == 0)
		inclusive = false;

	if (!inclusive)
	{
		/* Check if the upper bound is actually in the previous block. */
		if (offset == 0)
		{
			BlockNumber block = ItemPointerGetBlockNumberNoCheck(upperBound);

			/*
			 * If the upper bound was already in block 0, then there is no
			 * valid range.
			 */
			if (block == 0)
				return false;

			ItemPointerSetBlockNumber(upperBound, block - 1);
			ItemPointerSetOffsetNumber(upperBound, MaxOffsetNumber);
		}
		else
			ItemPointerSetOffsetNumber(upperBound, OffsetNumberPrev(offset));
	}

	return true;
}

/* ----------------------------------------------------------------
 *		TidInArrayExprEval
 *
 *		Evaluate a TidOpExpr, creating a set of new TidRanges -- one for each
 *		TID in the expression -- to add to the node's list of ranges to scan.
 * ----------------------------------------------------------------
 */
static void
TidInArrayExprEval(TidOpExpr * tidopexpr, BlockNumber nblocks,
				   TidScanState *tidstate, TidRangeArray * tidRangeArray)
{
	ExprContext *econtext = tidstate->ss.ps.ps_ExprContext;
	bool		isNull;
	Datum		arraydatum;
	ArrayType  *itemarray;
	Datum	   *ipdatums;
	bool	   *ipnulls;
	int			ndatums;
	int			i;

	arraydatum = ExecEvalExprSwitchContext(tidopexpr->exprstate,
										   econtext,
										   &isNull);
	if (isNull)
		return;

	itemarray = DatumGetArrayTypeP(arraydatum);
	deconstruct_array(itemarray,
					  TIDOID, sizeof(ItemPointerData), false, 's',
					  &ipdatums, &ipnulls, &ndatums);

	/* ensure space for all returned TID datums in one swoop */
	EnsureTidRangeSpace(tidRangeArray, ndatums);

	for (i = 0; i < ndatums; i++)
	{
		if (!ipnulls[i])
		{
			ItemPointer itemptr = (ItemPointer) DatumGetPointer(ipdatums[i]);

			if (ItemPointerIsValid(itemptr) &&
				ItemPointerGetBlockNumber(itemptr) < nblocks)
			{
				AddTidRange(tidRangeArray, itemptr, itemptr);
			}
		}
	}
	pfree(ipdatums);
	pfree(ipnulls);
}

/* ----------------------------------------------------------------
 *		TidExprEval
 *
 *		Evaluate a TidExpr, creating a new TidRange to add to the node's
 *		list of ranges to scan.
 * ----------------------------------------------------------------
 */
static void
TidExprEval(TidExpr *expr, BlockNumber nblocks, TidScanState *tidstate,
			TidRangeArray * tidRangeArray)
{
	ExprContext *econtext = tidstate->ss.ps.ps_ExprContext;
	ListCell   *l;
	ItemPointerData lowerBound;
	ItemPointerData upperBound;

	/* The biggest range on an empty table is empty; just skip it. */
	if (nblocks == 0)
		return;

	/* Set the lower and upper bound to scan the whole table. */
	ItemPointerSetBlockNumber(&lowerBound, 0);
	ItemPointerSetOffsetNumber(&lowerBound, 1);
	ItemPointerSetBlockNumber(&upperBound, nblocks - 1);
	ItemPointerSetOffsetNumber(&upperBound, MaxOffsetNumber);

	foreach(l, expr->opexprs)
	{
		TidOpExpr  *tidopexpr = (TidOpExpr *) lfirst(l);

		if (tidopexpr->exprtype == TIDEXPR_IN_ARRAY)
		{
			TidInArrayExprEval(tidopexpr, nblocks, tidstate, tidRangeArray);

			/*
			 * A CTID = ANY expression only exists by itself; there shouldn't
			 * be any other quals alongside it.  TidInArrayExprEval has
			 * already added the ranges, so just return here.
			 */
			Assert(list_length(expr->opexprs) == 1);
			return;
		}
		else
		{
			ItemPointer itemptr;
			bool		isNull;

			/* Evaluate this bound. */
			itemptr = (ItemPointer)
				DatumGetPointer(ExecEvalExprSwitchContext(tidopexpr->exprstate,
														  econtext,
														  &isNull));

			/* If the bound is NULL, *nothing* matches the qual. */
			if (isNull)
				return;

			if (tidopexpr->exprtype == TIDEXPR_EQ && ItemPointerIsValid(itemptr))
			{
				lowerBound = *itemptr;
				upperBound = *itemptr;

				/*
				 * A CTID = ? expression only exists by itself, so set the
				 * range to this single TID, and exit the loop (the remainder
				 * of this function will add the range).
				 */
				Assert(list_length(expr->opexprs) == 1);
				break;
			}

			if (tidopexpr->exprtype == TIDEXPR_LOWER_BOUND)
			{
				ItemPointerData lb;

				if (!SetTidLowerBound(itemptr, tidopexpr->inclusive, &lb))
					return;

				if (ItemPointerCompare(&lb, &lowerBound) > 0)
					lowerBound = lb;
			}

			if (tidopexpr->exprtype == TIDEXPR_UPPER_BOUND)
			{
				ItemPointerData ub;

				if (!SetTidUpperBound(itemptr, tidopexpr->inclusive, &ub))
					return;

				if (ItemPointerCompare(&ub, &upperBound) < 0)
					upperBound = ub;
			}
		}
	}

	/* If the resulting range is not empty, add it to the array. */
	if (ItemPointerCompare(&lowerBound, &upperBound) <= 0)
	{
		EnsureTidRangeSpace(tidRangeArray, 1);
		AddTidRange(tidRangeArray, &lowerBound, &upperBound);
	}
}

/* ----------------------------------------------------------------
 *		TidListEval
 *
 *		Compute the list of TID ranges to be visited, by evaluating the
 *		expressions for them.
 *
 *		(The result is actually an array, not a list.)
 * ----------------------------------------------------------------
 */
static void
TidListEval(TidScanState *tidstate)
{
	ExprContext *econtext = tidstate->ss.ps.ps_ExprContext;
	BlockNumber nblocks;
	TidRangeArray tidRangeArray = {NULL, 0, 0}; /* not yet allocated */
	ListCell   *l;

	/*
	 * We silently discard any TIDs that are out of range at the time of scan
	 * start.  (Since we hold at least AccessShareLock on the table, it won't
	 * be possible for someone to truncate away the blocks we intend to
	 * visit.)
	 */
	nblocks = RelationGetNumberOfBlocks(tidstate->ss.ss_currentRelation);

	foreach(l, tidstate->tss_tidexprs)
	{
		TidExpr    *tidexpr = (TidExpr *) lfirst(l);

		if (tidexpr->cexpr)
		{
			ItemPointerData cursor_tid;

			Assert(tidexpr->cexpr);
			if (execCurrentOf(tidexpr->cexpr, econtext,
							  RelationGetRelid(tidstate->ss.ss_currentRelation),
							  &cursor_tid))
			{
				EnsureTidRangeSpace(&tidRangeArray, 1);
				AddTidRange(&tidRangeArray, &cursor_tid, &cursor_tid);
			}
		}
		else
		{
			TidExprEval(tidexpr, nblocks, tidstate, &tidRangeArray);
		}
	}

	/*
	 * Sort the array of TIDs into order, and eliminate duplicates.
	 * Eliminating duplicates is necessary since we want OR semantics across
	 * the list.  Sorting makes it easier to detect duplicates, and as a bonus
	 * ensures that we will visit the heap in the most efficient way.
	 */
	if (tidRangeArray.numRanges > 1)
	{
		int			lastRange;
		int			i;

		/* CurrentOfExpr could never appear OR'd with something else */
		Assert(!tidstate->tss_isCurrentOf);

		qsort((void *) tidRangeArray.ranges, tidRangeArray.numRanges,
			  sizeof(TidRange), tidrange_comparator);
		lastRange = 0;
		for (i = 1; i < tidRangeArray.numRanges; i++)
		{
			if (!MergeTidRanges(&tidRangeArray.ranges[lastRange],
								&tidRangeArray.ranges[i]))
				tidRangeArray.ranges[++lastRange] = tidRangeArray.ranges[i];
		}
		tidRangeArray.numRanges = lastRange + 1;
	}

	tidstate->tss_TidRanges = tidRangeArray.ranges;
	tidstate->tss_NumTidRanges = tidRangeArray.numRanges;
	tidstate->tss_CurrentTidRange = -1;
}

/*
 * MergeTidRanges
 *		If two ranges overlap, merge them into one.
 *
 * Assumes the two ranges a and b are already ordered by (first, last).
 * Returns true if they were merged, with the result in a.
 */
static bool
MergeTidRanges(TidRange * a, TidRange * b)
{
	/*
	 * If the first range ends before the second one begins, they don't
	 * overlap, and we can't merge them.
	 */
	if (ItemPointerCompare(&a->last, &b->first) < 0)
		return false;

	/*
	 * Since they overlap, the end of the new range should be the maximum of
	 * the original two range ends.
	 */
	if (ItemPointerCompare(&a->last, &b->last) < 0)
		a->last = b->last;
	return true;
}

/*
 * qsort comparator for TidRange items
 */
static int
tidrange_comparator(const void *a, const void *b)
{
	TidRange   *tra = (TidRange *) a;
	TidRange   *trb = (TidRange *) b;
	int			cmp_first = ItemPointerCompare(&tra->first, &trb->first);

	if (cmp_first != 0)
		return cmp_first;
	else
		return ItemPointerCompare(&tra->last, &trb->last);
}

/* ----------------------------------------------------------------
 *		BeginTidRangeScan
 *
 *		Beginning scanning a range of TIDs by setting up the TidScan node's
 *		scandesc, and setting the tss_inScan flag.
 * ----------------------------------------------------------------
 */
static HeapScanDesc
BeginTidRangeScan(TidScanState *node, TidRange * range)
{
	HeapScanDesc scandesc = node->ss.ss_currentScanDesc;
	BlockNumber first_block = ItemPointerGetBlockNumberNoCheck(&range->first);
	BlockNumber last_block = ItemPointerGetBlockNumberNoCheck(&range->last);

	if (!scandesc)
	{
		EState	   *estate = node->ss.ps.state;

		scandesc = heap_beginscan_strat(node->ss.ss_currentRelation,
										estate->es_snapshot,
										0, NULL,
										false, false);
		node->ss.ss_currentScanDesc = scandesc;
	}
	else
		heap_rescan(scandesc, NULL);

	heap_setscanlimits(scandesc, first_block, last_block - first_block + 1);
	node->tss_inScan = true;
	return scandesc;
}

/* ----------------------------------------------------------------
 *		NextInTidRange
 *
 *		Fetch the next tuple when scanning a range of TIDs.
 * ----------------------------------------------------------------
 */
static HeapTuple
NextInTidRange(HeapScanDesc scandesc, ScanDirection direction, TidRange * range)
{
	BlockNumber first_block = ItemPointerGetBlockNumber(&range->first);
	OffsetNumber first_offset = ItemPointerGetOffsetNumber(&range->first);
	BlockNumber last_block = ItemPointerGetBlockNumber(&range->last);
	OffsetNumber last_offset = ItemPointerGetOffsetNumber(&range->last);
	HeapTuple	tuple;

	for (;;)
	{
		BlockNumber block;
		OffsetNumber offset;

		tuple = heap_getnext(scandesc, direction);
		if (!tuple)
			break;

		/* Check that the tuple is within the required range. */
		block = ItemPointerGetBlockNumber(&tuple->t_self);
		offset = ItemPointerGetOffsetNumber(&tuple->t_self);

		/*
		 * If the tuple is in the first block of the range and before the
		 * first requested offset, then we can either skip it (if scanning
		 * forward), or end the scan (if scanning backward).
		 */
		if (block == first_block && offset < first_offset)
		{
			if (ScanDirectionIsForward(direction))
				continue;
			else
				return NULL;
		}

		/* Similarly for the last block, after the last requested offset. */
		if (block == last_block && offset > last_offset)
		{
			if (ScanDirectionIsBackward(direction))
				continue;
			else
				return NULL;
		}

		break;
	}

	return tuple;
}

/* ----------------------------------------------------------------
 *		TidNext
 *
 *		Retrieve a tuple from the TidScan node's currentRelation
 *		using the tids in the TidScanState information.
 *
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
TidNext(TidScanState *node)
{
	HeapScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	Snapshot	snapshot;
	Relation	heapRelation;
	HeapTuple	tuple;
	TupleTableSlot *slot;
	Buffer		buffer = InvalidBuffer;
	int			numRanges;

	/*
	 * extract necessary information from tid scan node
	 */
	scandesc = node->ss.ss_currentScanDesc;
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	snapshot = estate->es_snapshot;
	heapRelation = node->ss.ss_currentRelation;
	slot = node->ss.ss_ScanTupleSlot;

	/* First time through, compute the list of TID ranges to be visited */
	if (node->tss_TidRanges == NULL)
	{
		TidListEval(node);

		node->tss_CurrentTidRange = -1;
	}

	numRanges = node->tss_NumTidRanges;

	/* If the plan direction is backward, invert the direction. */
	if (ScanDirectionIsBackward(((TidScan *) node->ss.ps.plan)->scandir))
	{
		if (ScanDirectionIsForward(direction))
			direction = BackwardScanDirection;
		else if (ScanDirectionIsBackward(direction))
			direction = ForwardScanDirection;
	}

	tuple = NULL;
	for (;;)
	{
		TidRange   *currentRange;

		if (!node->tss_inScan)
		{
			/* Initialize or advance scan position, depending on direction. */
			if (ScanDirectionIsBackward(direction))
			{
				if (node->tss_CurrentTidRange < 0)
				{
					/* initialize for backward scan */
					node->tss_CurrentTidRange = numRanges - 1;
				}
				else
					node->tss_CurrentTidRange--;
			}
			else
			{
				if (node->tss_CurrentTidRange < 0)
				{
					/* initialize for forward scan */
					node->tss_CurrentTidRange = 0;
				}
				else
					node->tss_CurrentTidRange++;
			}
		}

		/* If we've finished iterating over the ranges, exit the loop. */
		if (node->tss_CurrentTidRange >= numRanges ||
			node->tss_CurrentTidRange < 0)
			break;

		currentRange = &node->tss_TidRanges[node->tss_CurrentTidRange];

		/*
		 * For ranges containing a single tuple, we can simply make an attempt
		 * to fetch the tuple directly.
		 */
		if (ItemPointerEquals(&currentRange->first, &currentRange->last))
		{
			/*
			 * We use node->tss_htup as the tuple pointer; note this can't
			 * just be a local variable here, as the scan tuple slot will keep
			 * a pointer to it.
			 */
			tuple = &(node->tss_htup);
			tuple->t_self = currentRange->first;

			/*
			 * For WHERE CURRENT OF, the tuple retrieved from the cursor might
			 * since have been updated; if so, we should fetch the version
			 * that is current according to our snapshot.
			 */
			if (node->tss_isCurrentOf)
				heap_get_latest_tid(heapRelation, snapshot, &tuple->t_self);

			if (heap_fetch(heapRelation, snapshot, tuple, &buffer, false, NULL))
			{
				/*
				 * Store the scanned tuple in the scan tuple slot of the scan
				 * state.  Eventually we will only do this and not return a
				 * tuple.
				 */
				ExecStoreBufferHeapTuple(tuple, /* tuple to store */
										 slot,	/* slot to store in */
										 buffer);	/* buffer associated with
													 * tuple */

				/*
				 * At this point we have an extra pin on the buffer, because
				 * ExecStoreBufferHeapTuple incremented the pin count. Drop
				 * our local pin.
				 */
				ReleaseBuffer(buffer);

				return slot;
			}
			else
			{
				/* No tuple found for this TID range. */
				tuple = NULL;
			}
		}
		else
		{
			/*
			 * For a bigger TID range, we'll use a scan, starting a new one if
			 * we're not already in one.
			 */
			if (!node->tss_inScan)
				scandesc = BeginTidRangeScan(node, currentRange);

			tuple = NextInTidRange(scandesc, direction, currentRange);
			if (tuple)
				break;

			/* No more tuples in this scan, so finish it. */
			node->tss_inScan = false;
		}
	}

	/*
	 * save the tuple and the buffer returned to us by the access methods in
	 * our scan tuple slot and return the slot.  Note also that
	 * ExecStoreBufferHeapTuple will increment the refcount of the buffer; the
	 * refcount will not be dropped until the tuple table slot is cleared.
	 */
	if (tuple)
		ExecStoreBufferHeapTuple(tuple, /* tuple to store */
								 slot,	/* slot to store in */
								 scandesc->rs_cbuf);	/* buffer associated
														 * with this tuple */
	else
		ExecClearTuple(slot);

	return slot;
}

/*
 * TidRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
TidRecheck(TidScanState *node, TupleTableSlot *slot)
{
	/*
	 * XXX shouldn't we check here to make sure tuple matches TID list? In
	 * runtime-key case this is not certain, is it?  However, in the WHERE
	 * CURRENT OF case it might not match anyway ...
	 */
	return true;
}


/* ----------------------------------------------------------------
 *		ExecTidScan(node)
 *
 *		Scans the relation using tids and returns
 *		   the next qualifying tuple in the direction specified.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 *
 *		Conditions:
 *		  -- the "cursor" maintained by the AMI is positioned at the tuple
 *			 returned previously.
 *
 *		Initial States:
 *		  -- the relation indicated is opened for scanning so that the
 *			 "cursor" is positioned before the first qualifying tuple.
 *		  -- tss_CurrentTidRange is -1.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecTidScan(PlanState *pstate)
{
	TidScanState *node = castNode(TidScanState, pstate);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) TidNext,
					(ExecScanRecheckMtd) TidRecheck);
}

/* ----------------------------------------------------------------
 *		ExecReScanTidScan(node)
 * ----------------------------------------------------------------
 */
void
ExecReScanTidScan(TidScanState *node)
{
	if (node->tss_TidRanges)
		pfree(node->tss_TidRanges);

	node->tss_TidRanges = NULL;
	node->tss_NumTidRanges = 0;
	node->tss_CurrentTidRange = -1;
	node->tss_inScan = false;

	ExecScanReScan(&node->ss);
}

/* ----------------------------------------------------------------
 *		ExecEndTidScan
 *
 *		Releases any storage allocated through C routines.
 *		Returns nothing.
 * ----------------------------------------------------------------
 */
void
ExecEndTidScan(TidScanState *node)
{
	HeapScanDesc scan = node->ss.ss_currentScanDesc;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clear out tuple table slots
	 */
	if (node->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/* close heap scan */
	if (scan != NULL)
		heap_endscan(scan);
}

/* ----------------------------------------------------------------
 *		ExecInitTidScan
 *
 *		Initializes the tid scan's state information, creates
 *		scan keys, and opens the base and tid relations.
 *
 *		Parameters:
 *		  node: TidNode node produced by the planner.
 *		  estate: the execution state initialized in InitPlan.
 * ----------------------------------------------------------------
 */
TidScanState *
ExecInitTidScan(TidScan *node, EState *estate, int eflags)
{
	TidScanState *tidstate;
	Relation	currentRelation;

	/*
	 * create state structure
	 */
	tidstate = makeNode(TidScanState);
	tidstate->ss.ps.plan = (Plan *) node;
	tidstate->ss.ps.state = estate;
	tidstate->ss.ps.ExecProcNode = ExecTidScan;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &tidstate->ss.ps);

	/*
	 * mark tid range list as not computed yet
	 */
	tidstate->tss_TidRanges = NULL;
	tidstate->tss_NumTidRanges = 0;
	tidstate->tss_CurrentTidRange = -1;
	tidstate->tss_inScan = false;

	/*
	 * open the scan relation
	 */
	currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);

	tidstate->ss.ss_currentRelation = currentRelation;
	tidstate->ss.ss_currentScanDesc = NULL; /* no heap scan here */

	/*
	 * get the scan type from the relation descriptor.
	 */
	ExecInitScanTupleSlot(estate, &tidstate->ss,
						  RelationGetDescr(currentRelation),
						  &TTSOpsBufferHeapTuple);

	/*
	 * Initialize result type and projection.
	 */
	ExecInitResultTypeTL(&tidstate->ss.ps);
	ExecAssignScanProjectionInfo(&tidstate->ss);

	/*
	 * initialize child expressions
	 */
	tidstate->ss.ps.qual =
		ExecInitQual(node->scan.plan.qual, (PlanState *) tidstate);

	TidExprListCreate(tidstate);

	/*
	 * all done.
	 */
	return tidstate;
}
