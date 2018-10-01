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

#include "access/relscan.h"
#include "access/sysattr.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
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
	TIDEXPR_CURRENT_OF,
	TIDEXPR_IN_ARRAY,
	TIDEXPR_EQ,
	TIDEXPR_LT,
	TIDEXPR_GT,
	TIDEXPR_BETWEEN,
	TIDEXPR_ANY
}			TidExprType;

/* one element in tss_tidexprs */
typedef struct TidExpr
{
	TidExprType type;
	ExprState  *exprstate;		/* ExprState for a TID-yielding subexpr */
	ExprState  *exprstate2;		/* For TIDEXPR_BETWEEN */
	CurrentOfExpr *cexpr;		/* For TIDEXPR_CURRENT_OF */
	bool		strict;			/* Indicates < rather than <=, or > rather */
	bool		strict2;		/* than >= */
} TidExpr;

typedef struct TidRange
{
	ItemPointerData first;
	ItemPointerData last;
}			TidRange;

static ExprState *MakeTidOpExprState(OpExpr *expr, TidScanState *tidstate, bool *strict, bool *invert);
static void TidExprListCreate(TidScanState *tidstate);
static TidRange * EnlargeTidRangeArray(TidRange * tidRanges, int numRanges, int *numAllocRanges);
static bool SetTidLowerBound(ItemPointer tid, bool strict, int nblocks, ItemPointer lowerBound);
static bool SetTidUpperBound(ItemPointer tid, bool strict, int nblocks, ItemPointer upperBound);
static void TidListEval(TidScanState *tidstate);
static bool MergeTidRanges(TidRange * a, TidRange * b);
static int	itemptr_comparator(const void *a, const void *b);
static int	tidrange_comparator(const void *a, const void *b);
static HeapScanDesc BeginTidRangeScan(TidScanState *node, TidRange * range);
static HeapTuple NextInTidRange(HeapScanDesc scandesc, ScanDirection direction, TidRange * range);
static TupleTableSlot *TidNext(TidScanState *node);


/*
 * Create an ExprState corresponding to the value part of a TID comparison.
 * If the comparison operator is > or <, strict is set.
 * If the comparison is of the form VALUE op CTID, then invert is set.
 */
static ExprState *
MakeTidOpExprState(OpExpr *expr, TidScanState *tidstate, bool *strict, bool *invert)
{
	Node	   *arg1 = get_leftop((Expr *) expr);
	Node	   *arg2 = get_rightop((Expr *) expr);
	ExprState  *exprstate = NULL;

	*invert = false;

	if (IsCTIDVar(arg1))
		exprstate = ExecInitExpr((Expr *) arg2, &tidstate->ss.ps);
	else if (IsCTIDVar(arg2))
	{
		exprstate = ExecInitExpr((Expr *) arg1, &tidstate->ss.ps);
		*invert = true;
	}
	else
		elog(ERROR, "could not identify CTID variable");

	*strict = expr->opno == TIDLessOperator || expr->opno == TIDGreaterOperator;

	return exprstate;
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

	if (!node->tidquals)
	{
		TidExpr    *tidexpr = (TidExpr *) palloc0(sizeof(TidExpr));

		tidexpr->type = TIDEXPR_ANY;
		tidstate->tss_tidexprs = lappend(tidstate->tss_tidexprs, tidexpr);
	}

	foreach(l, node->tidquals)
	{
		Expr	   *expr = (Expr *) lfirst(l);
		TidExpr    *tidexpr = (TidExpr *) palloc0(sizeof(TidExpr));

		if (is_opclause(expr))
		{
			OpExpr	   *opexpr = (OpExpr *) expr;
			bool		invert;

			tidexpr->exprstate = MakeTidOpExprState(opexpr, tidstate, &tidexpr->strict, &invert);
			if (opexpr->opno == TIDLessOperator || opexpr->opno == TIDLessEqOperator)
				tidexpr->type = invert ? TIDEXPR_GT : TIDEXPR_LT;
			else if (opexpr->opno == TIDGreaterOperator || opexpr->opno == TIDGreaterEqOperator)
				tidexpr->type = invert ? TIDEXPR_LT : TIDEXPR_GT;
			else
				tidexpr->type = TIDEXPR_EQ;
		}
		else if (expr && IsA(expr, ScalarArrayOpExpr))
		{
			ScalarArrayOpExpr *saex = (ScalarArrayOpExpr *) expr;

			Assert(IsCTIDVar(linitial(saex->args)));
			tidexpr->exprstate = ExecInitExpr(lsecond(saex->args),
											  &tidstate->ss.ps);
			tidexpr->type = TIDEXPR_IN_ARRAY;
		}
		else if (expr && IsA(expr, CurrentOfExpr))
		{
			CurrentOfExpr *cexpr = (CurrentOfExpr *) expr;

			tidexpr->cexpr = cexpr;
			tidexpr->type = TIDEXPR_CURRENT_OF;
			tidstate->tss_isCurrentOf = true;
		}
		else if (and_clause((Node *) expr))
		{
			OpExpr	   *arg1;
			OpExpr	   *arg2;
			bool		invert;
			bool		invert2;

			Assert(list_length(((BoolExpr *) expr)->args) == 2);
			arg1 = (OpExpr *) linitial(((BoolExpr *) expr)->args);
			arg2 = (OpExpr *) lsecond(((BoolExpr *) expr)->args);
			tidexpr->exprstate = MakeTidOpExprState(arg1, tidstate, &tidexpr->strict, &invert);
			tidexpr->exprstate2 = MakeTidOpExprState(arg2, tidstate, &tidexpr->strict2, &invert2);

			/* If the LHS is not the lower bound, swap them. */
			if (invert == (arg1->opno == TIDGreaterOperator || arg1->opno == TIDGreaterEqOperator))
			{
				bool		temp_strict;
				ExprState  *temp_es;

				temp_es = tidexpr->exprstate;
				tidexpr->exprstate = tidexpr->exprstate2;
				tidexpr->exprstate2 = temp_es;

				temp_strict = tidexpr->strict;
				tidexpr->strict = tidexpr->strict2;
				tidexpr->strict2 = temp_strict;
			}

			tidexpr->type = TIDEXPR_BETWEEN;
		}
		else
			elog(ERROR, "could not identify CTID expression");

		tidstate->tss_tidexprs = lappend(tidstate->tss_tidexprs, tidexpr);
	}

	/* CurrentOfExpr could never appear OR'd with something else */
	Assert(list_length(tidstate->tss_tidexprs) == 1 ||
		   !tidstate->tss_isCurrentOf);
}

static TidRange *
EnlargeTidRangeArray(TidRange * tidRanges, int numRanges, int *numAllocRanges)
{
	if (numRanges >= *numAllocRanges)
	{
		*numAllocRanges *= 2;
		tidRanges = (TidRange *)
			repalloc(tidRanges,
					 *numAllocRanges * sizeof(TidRange));
	}
	return tidRanges;
}

/*
 * Set a lower bound tid, taking into account the strictness of the bound.
 * Return false if the lower bound is outside the size of the table.
 */
static bool
SetTidLowerBound(ItemPointer tid, bool strict, int nblocks, ItemPointer lowerBound)
{
	OffsetNumber offset;

	if (tid == NULL)
	{
		ItemPointerSetBlockNumber(lowerBound, 0);
		ItemPointerSetOffsetNumber(lowerBound, 1);
		return true;
	}

	if (ItemPointerGetBlockNumberNoCheck(tid) > nblocks)
		return false;

	*lowerBound = *tid;
	offset = ItemPointerGetOffsetNumberNoCheck(tid);

	if (strict)
		ItemPointerSetOffsetNumber(lowerBound, OffsetNumberNext(offset));
	else if (offset == 0)
		ItemPointerSetOffsetNumber(lowerBound, 1);

	return true;
}

/*
 * Set an upper bound tid, taking into account the strictness of the bound.
 * Return false if the bound excludes anything from the table.
 */
static bool
SetTidUpperBound(ItemPointer tid, bool strict, int nblocks, ItemPointer upperBound)
{
	OffsetNumber offset;

	/* If the table is empty, the range must be empty. */
	if (nblocks == 0)
		return false;

	if (tid == NULL)
	{
		ItemPointerSetBlockNumber(upperBound, nblocks - 1);
		ItemPointerSetOffsetNumber(upperBound, MaxOffsetNumber);
		return true;
	}

	*upperBound = *tid;
	offset = ItemPointerGetOffsetNumberNoCheck(tid);

	/*
	 * If the expression was non-strict (<=) and the offset is 0, then just
	 * pretend it was strict, because offset 0 doesn't exist and we may as
	 * well exclude that block.
	 */
	if (!strict && offset == 0)
		strict = true;

	if (strict)
	{
		if (offset == 0)
		{
			BlockNumber block = ItemPointerGetBlockNumberNoCheck(upperBound);

			/*
			 * If the upper bound was already block 0, then there is no valid
			 * range.
			 */
			if (block == 0)
				return false;

			ItemPointerSetBlockNumber(upperBound, block - 1);
			ItemPointerSetOffsetNumber(upperBound, MaxOffsetNumber);
		}
		else
			ItemPointerSetOffsetNumber(upperBound, OffsetNumberPrev(offset));
	}

	/*
	 * If the upper bound is beyond the last block of the table, truncate it
	 * to the last TID of the last block.
	 */
	if (ItemPointerGetBlockNumberNoCheck(upperBound) > nblocks)
	{
		ItemPointerSetBlockNumber(upperBound, nblocks - 1);
		ItemPointerSetOffsetNumber(upperBound, MaxOffsetNumber);
	}

	return true;
}

/*
 * Compute the list of TIDs to be visited, by evaluating the expressions
 * for them.
 *
 * (The result is actually an array, not a list.)
 */
static void
TidListEval(TidScanState *tidstate)
{
	ExprContext *econtext = tidstate->ss.ps.ps_ExprContext;
	BlockNumber nblocks;
	TidRange   *tidRanges;
	int			numAllocRanges;
	int			numRanges;
	ListCell   *l;

	/*
	 * We silently discard any TIDs that are out of range at the time of scan
	 * start.  (Since we hold at least AccessShareLock on the table, it won't
	 * be possible for someone to truncate away the blocks we intend to
	 * visit.)
	 */
	nblocks = RelationGetNumberOfBlocks(tidstate->ss.ss_currentRelation);

	/*
	 * We initialize the array with enough slots for the case that all quals
	 * are simple OpExprs or CurrentOfExprs.  If there are any
	 * ScalarArrayOpExprs, we may have to enlarge the array.
	 */
	numAllocRanges = list_length(tidstate->tss_tidexprs);
	tidRanges = (TidRange *) palloc0(numAllocRanges * sizeof(TidRange));
	numRanges = 0;

	foreach(l, tidstate->tss_tidexprs)
	{
		TidExpr    *tidexpr = (TidExpr *) lfirst(l);
		ItemPointer itemptr;
		bool		isNull;

		if (tidexpr->exprstate && tidexpr->type == TIDEXPR_EQ)
		{
			itemptr = (ItemPointer)
				DatumGetPointer(ExecEvalExprSwitchContext(tidexpr->exprstate,
														  econtext,
														  &isNull));
			if (!isNull &&
				ItemPointerIsValid(itemptr) &&
				ItemPointerGetBlockNumber(itemptr) < nblocks)
			{
				tidRanges = EnlargeTidRangeArray(tidRanges, numRanges, &numAllocRanges);
				tidRanges[numRanges].first = *itemptr;
				tidRanges[numRanges].last = *itemptr;
				numRanges++;
			}
		}
		else if (tidexpr->exprstate && tidexpr->type == TIDEXPR_LT)
		{
			bool		upper_isNull;
			ItemPointer upper_itemptr = (ItemPointer)
			DatumGetPointer(ExecEvalExprSwitchContext(tidexpr->exprstate,
													  econtext,
													  &upper_isNull));

			if (upper_isNull)
				continue;

			tidRanges = EnlargeTidRangeArray(tidRanges, numRanges, &numAllocRanges);

			SetTidLowerBound(NULL, false, nblocks, &tidRanges[numRanges].first);
			if (SetTidUpperBound(upper_itemptr, tidexpr->strict, nblocks, &tidRanges[numRanges].last))
				numRanges++;
		}
		else if (tidexpr->exprstate && tidexpr->type == TIDEXPR_GT)
		{
			bool		lower_isNull;
			ItemPointer lower_itemptr = (ItemPointer)
			DatumGetPointer(ExecEvalExprSwitchContext(tidexpr->exprstate,
													  econtext,
													  &lower_isNull));

			if (lower_isNull)
				continue;

			tidRanges = EnlargeTidRangeArray(tidRanges, numRanges, &numAllocRanges);

			if (SetTidLowerBound(lower_itemptr, tidexpr->strict, nblocks, &tidRanges[numRanges].first) &&
				SetTidUpperBound(NULL, false, nblocks, &tidRanges[numRanges].last))
				numRanges++;
		}
		else if (tidexpr->exprstate && tidexpr->type == TIDEXPR_BETWEEN)
		{
			bool		lower_isNull,
						upper_isNull;
			ItemPointer lower_itemptr = (ItemPointer)
			DatumGetPointer(ExecEvalExprSwitchContext(tidexpr->exprstate,
													  econtext,
													  &lower_isNull));
			ItemPointer upper_itemptr = (ItemPointer)
			DatumGetPointer(ExecEvalExprSwitchContext(tidexpr->exprstate2,
													  econtext,
													  &upper_isNull));

			if (lower_isNull || upper_isNull)
				continue;

			tidRanges = EnlargeTidRangeArray(tidRanges, numRanges, &numAllocRanges);

			if (SetTidLowerBound(lower_itemptr, tidexpr->strict, nblocks, &tidRanges[numRanges].first) &&
				SetTidUpperBound(upper_itemptr, tidexpr->strict2, nblocks, &tidRanges[numRanges].last))
				numRanges++;
		}
		else if (tidexpr->type == TIDEXPR_ANY)
		{
			tidRanges = EnlargeTidRangeArray(tidRanges, numRanges, &numAllocRanges);
			SetTidLowerBound(NULL, false, nblocks, &tidRanges[numRanges].first);
			SetTidUpperBound(NULL, false, nblocks, &tidRanges[numRanges].last);
			numRanges++;
		}
		else if (tidexpr->exprstate && tidexpr->type == TIDEXPR_IN_ARRAY)
		{
			Datum		arraydatum;
			ArrayType  *itemarray;
			Datum	   *ipdatums;
			bool	   *ipnulls;
			int			ndatums;
			int			i;

			arraydatum = ExecEvalExprSwitchContext(tidexpr->exprstate,
												   econtext,
												   &isNull);
			if (isNull)
				continue;
			itemarray = DatumGetArrayTypeP(arraydatum);
			deconstruct_array(itemarray,
							  TIDOID, sizeof(ItemPointerData), false, 's',
							  &ipdatums, &ipnulls, &ndatums);
			if (numRanges + ndatums > numAllocRanges)
			{
				numAllocRanges = numRanges + ndatums;
				tidRanges = (TidRange *)
					repalloc(tidRanges,
							 numAllocRanges * sizeof(TidRange));
			}
			for (i = 0; i < ndatums; i++)
			{
				if (!ipnulls[i])
				{
					itemptr = (ItemPointer) DatumGetPointer(ipdatums[i]);
					if (ItemPointerIsValid(itemptr) &&
						ItemPointerGetBlockNumber(itemptr) < nblocks)
						tidRanges[numRanges].first = *itemptr;
					tidRanges[numRanges].last = *itemptr;
					numRanges++;
				}
			}
			pfree(ipdatums);
			pfree(ipnulls);
		}
		else if (tidexpr->type == TIDEXPR_CURRENT_OF)
		{
			ItemPointerData cursor_tid;

			Assert(tidexpr->cexpr);
			if (execCurrentOf(tidexpr->cexpr, econtext,
							  RelationGetRelid(tidstate->ss.ss_currentRelation),
							  &cursor_tid))
			{
				/*
				 * A current-of TidExpr only exists by itself, and we should
				 * already have allocated a tidList entry for it.  We don't
				 * need to check whether the tidList array needs to be
				 * resized.
				 */
				Assert(numRanges < numAllocRanges);
				tidRanges[numRanges].first = cursor_tid;
				tidRanges[numRanges].last = cursor_tid;
				numRanges++;
			}
		}
		else
			Assert(false);
	}

	/*
	 * Sort the array of TIDs into order, and eliminate duplicates.
	 * Eliminating duplicates is necessary since we want OR semantics across
	 * the list.  Sorting makes it easier to detect duplicates, and as a bonus
	 * ensures that we will visit the heap in the most efficient way.
	 */
	if (numRanges > 1)
	{
		int			lastRange;
		int			i;

		/* CurrentOfExpr could never appear OR'd with something else */
		Assert(!tidstate->tss_isCurrentOf);

		qsort((void *) tidRanges, numRanges, sizeof(TidRange), tidrange_comparator);
		lastRange = 0;
		for (i = 1; i < numRanges; i++)
		{
			if (!MergeTidRanges(&tidRanges[lastRange], &tidRanges[i]))
				tidRanges[++lastRange] = tidRanges[i];
		}
		numRanges = lastRange + 1;
	}

	tidstate->tss_TidRanges = tidRanges;
	tidstate->tss_NumRanges = numRanges;
	tidstate->tss_TidPtr = -1;
}

/*
 * If two ranges overlap, merge them into one.
 * Assumes the two ranges are already ordered by (first, last).
 * Returns true if they were merged.
 */
static bool
MergeTidRanges(TidRange * a, TidRange * b)
{
	ItemPointerData a_last = a->last;
	ItemPointerData b_last;

	if (!ItemPointerIsValid(&a_last))
		a_last = a->first;

	if (itemptr_comparator(&a_last, &b->first) <= 0)
		return false;

	b_last = b->last;
	if (!ItemPointerIsValid(&b_last))
		b_last = b->first;

	a->last = b->last;
	return true;
}

/*
 * qsort comparator for ItemPointerData items
 */
static int
itemptr_comparator(const void *a, const void *b)
{
	const ItemPointerData *ipa = (const ItemPointerData *) a;
	const ItemPointerData *ipb = (const ItemPointerData *) b;
	BlockNumber ba = ItemPointerGetBlockNumber(ipa);
	BlockNumber bb = ItemPointerGetBlockNumber(ipb);
	OffsetNumber oa = ItemPointerGetOffsetNumber(ipa);
	OffsetNumber ob = ItemPointerGetOffsetNumber(ipb);

	if (ba < bb)
		return -1;
	if (ba > bb)
		return 1;
	if (oa < ob)
		return -1;
	if (oa > ob)
		return 1;
	return 0;
}

/*
 * qsort comparator for TidRange items
 */
static int
tidrange_comparator(const void *a, const void *b)
{
	const		TidRange *tra = (const TidRange *) a;
	const		TidRange *trb = (const TidRange *) b;
	int			cmp_first = itemptr_comparator(&tra->first, &trb->first);

	if (cmp_first != 0)
		return cmp_first;
	else
		return itemptr_comparator(&tra->last, &trb->last);
}

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
		 * TODO if scanning forward, can stop as soon as we see a tuple
		 * greater than last_offset
		 */
		/* similarly with backward, less than, first_offset */
		if (block == first_block && offset < first_offset)
			continue;

		if (block == last_block && offset > last_offset)
			continue;

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
	bool		bBackward;

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

		node->tss_TidPtr = -1;
	}

	numRanges = node->tss_NumRanges;

	/* If the plan direction is backward, invert the direction. */
	if (ScanDirectionIsBackward(((TidScan *) node->ss.ps.plan)->direction))
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
			bBackward = ScanDirectionIsBackward(direction);
			if (bBackward)
			{
				if (node->tss_TidPtr < 0)
				{
					/* initialize for backward scan */
					node->tss_TidPtr = numRanges - 1;
				}
				else
					node->tss_TidPtr--;
			}
			else
			{
				if (node->tss_TidPtr < 0)
				{
					/* initialize for forward scan */
					node->tss_TidPtr = 0;
				}
				else
					node->tss_TidPtr++;
			}
		}

		if (node->tss_TidPtr >= numRanges || node->tss_TidPtr < 0)
			break;

		currentRange = &node->tss_TidRanges[node->tss_TidPtr];

		/* TODO ranges of size 1 should also use a simple tuple fetch */
		if (node->tss_isCurrentOf)
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
				 * ExecStoreHeapTuple incremented the pin count. Drop our
				 * local pin.
				 */
				ReleaseBuffer(buffer);

				return slot;
			}
			else
			{
				tuple = NULL;
			}
		}
		else
		{
			if (!node->tss_inScan)
				scandesc = BeginTidRangeScan(node, currentRange);

			tuple = NextInTidRange(scandesc, direction, currentRange);
			if (tuple)
				break;

			node->tss_inScan = false;
		}
	}

	/*
	 * save the tuple and the buffer returned to us by the access methods in
	 * our scan tuple slot and return the slot.  Note: we pass 'false' because
	 * tuples returned by heap_getnext() are pointers onto disk pages and were
	 * not created with palloc() and so should not be pfree()'d.  Note also
	 * that ExecStoreHeapTuple will increment the refcount of the buffer; the
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
 *		  -- tidPtr is -1.
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
	node->tss_NumRanges = 0;
	node->tss_TidPtr = -1;
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
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/* close heap scan */
	if (scan != NULL)
		heap_endscan(scan);

	/*
	 * close the heap relation.
	 */
	ExecCloseScanRelation(node->ss.ss_currentRelation);
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
	tidstate->tss_NumRanges = 0;
	tidstate->tss_TidPtr = -1;
	tidstate->tss_inScan = false;

	/*
	 * open the base relation and acquire appropriate lock on it.
	 */
	currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);

	tidstate->ss.ss_currentRelation = currentRelation;
	tidstate->ss.ss_currentScanDesc = NULL; /* no heap scan here */

	/*
	 * get the scan type from the relation descriptor.
	 */
	ExecInitScanTupleSlot(estate, &tidstate->ss,
						  RelationGetDescr(currentRelation));

	/*
	 * Initialize result slot, type and projection.
	 */
	ExecInitResultTupleSlotTL(estate, &tidstate->ss.ps);
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
