/*-------------------------------------------------------------------------
 *
 * nodeTidrangescan.c
 *	  Routines to support tid range scans of relations
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeTidrangescan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *
 *		ExecTidRangeScan		scans a relation using a range of tids
 *		ExecInitTidRangeScan	creates and initializes state info.
 *		ExecReScanTidRangeScan	rescans the tid relation.
 *		ExecEndTidRangeScan		releases all storage.
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "catalog/pg_operator.h"
#include "executor/execdebug.h"
#include "executor/nodeTidrangescan.h"
#include "nodes/nodeFuncs.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"


#define IsCTIDVar(node)  \
	((node) != NULL && \
	 IsA((node), Var) && \
	 ((Var *) (node))->varattno == SelfItemPointerAttributeNumber && \
	 ((Var *) (node))->varlevelsup == 0)

typedef enum
{
	TIDEXPR_UPPER_BOUND,
	TIDEXPR_LOWER_BOUND
} TidExprType;

/* one element in TidExpr's opexprs */
typedef struct TidOpExpr
{
	TidExprType exprtype;		/* type of op */
	ExprState  *exprstate;		/* ExprState for a TID-yielding subexpr */
	bool		inclusive;		/* whether op is inclusive */
} TidOpExpr;

/*
 * For the given 'expr', build and return an appropriate TidOpExpr taking into
 * account the expr's operator and operand order.
 */
static TidOpExpr *
MakeTidOpExpr(OpExpr *expr, TidRangeScanState *tidstate)
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
			elog(ERROR, "could not identify CTID expression");
	}

	tidopexpr->exprstate = exprstate;

	return tidopexpr;
}

/*
 * Extract the qual subexpressions that yield TIDs to search for,
 * and compile them into ExprStates if they're ordinary expressions.
 */
static void
TidExprListCreate(TidRangeScanState *tidrangestate)
{
	TidRangeScan *node = (TidRangeScan *) tidrangestate->ss.ps.plan;
	List	   *tidexprs = NIL;
	ListCell   *l;

	foreach(l, node->tidrangequals)
	{
		OpExpr	   *opexpr = lfirst(l);
		TidOpExpr  *tidopexpr = MakeTidOpExpr(opexpr, tidrangestate);

		tidexprs = lappend(tidexprs, tidopexpr);
	}

	tidrangestate->trss_tidexprs = tidexprs;
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
 *		TidRangeEval
 *
 *		Compute the range of TIDs to scan, by evaluating the
 *		expressions for them.
 * ----------------------------------------------------------------
 */
static void
TidRangeEval(TidRangeScanState *node)
{
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	BlockNumber nblocks;
	ItemPointerData lowerBound;
	ItemPointerData upperBound;
	ListCell   *l;

	/*
	 * We silently discard any TIDs that are out of range at the time of scan
	 * start.  (Since we hold at least AccessShareLock on the table, it won't
	 * be possible for someone to truncate away the blocks we intend to
	 * visit.)
	 */
	nblocks = RelationGetNumberOfBlocks(node->ss.ss_currentRelation);


	/* The biggest range on an empty table is empty; just skip it. */
	if (nblocks == 0)
		return;

	/* Set the lower and upper bound to scan the whole table. */
	ItemPointerSetBlockNumber(&lowerBound, 0);
	ItemPointerSetOffsetNumber(&lowerBound, 1);
	ItemPointerSetBlockNumber(&upperBound, nblocks - 1);
	ItemPointerSetOffsetNumber(&upperBound, MaxOffsetNumber);

	foreach(l, node->trss_tidexprs)
	{
		TidOpExpr  *tidopexpr = (TidOpExpr *) lfirst(l);
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

	/* If the resulting range is not empty, use it. */
	if (ItemPointerCompare(&lowerBound, &upperBound) <= 0)
	{
		node->trss_startBlock = ItemPointerGetBlockNumberNoCheck(&lowerBound);
		node->trss_endBlock = ItemPointerGetBlockNumberNoCheck(&upperBound);
		node->trss_startOffset = ItemPointerGetOffsetNumberNoCheck(&lowerBound);
		node->trss_endOffset = ItemPointerGetOffsetNumberNoCheck(&upperBound);
	}
}

/* ----------------------------------------------------------------
 *		NextInTidRange
 *
 *		Fetch the next tuple when scanning a range of TIDs.
 *
 *		Since the heap access method may return tuples that are in the scan
 *		limit, but not within the required TID range, this function will
 *		check for such tuples and skip over them.
 * ----------------------------------------------------------------
 */
static HeapTuple
NextInTidRange(TidRangeScanState *node, TableScanDesc scandesc, ScanDirection direction)
{
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

		/* The tuple should never come from outside the scan limits. */
		Assert(block >= node->trss_startBlock &&
			   block <= node->trss_endBlock);

		/*
		 * If the tuple is in the first block of the range and before the
		 * first requested offset, then we can either skip it (if scanning
		 * forward), or end the scan (if scanning backward).
		 */
		if (block == node->trss_startBlock && offset < node->trss_startOffset)
		{
			if (ScanDirectionIsForward(direction))
				continue;
			else
				tuple = NULL;
		}

		/* Similarly for the last block, after the last requested offset. */
		if (block == node->trss_endBlock && offset > node->trss_endOffset)
		{
			if (ScanDirectionIsBackward(direction))
				continue;
			else
				tuple = NULL;
		}

		break;
	}

	return tuple;
}

/* ----------------------------------------------------------------
 *		TidRangeNext
 *
 *		Retrieve a tuple from the TidRangeScan node's currentRelation
 *		using the tids in the TidRangeScanState information.
 *
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
TidRangeNext(TidRangeScanState *node)
{
	TableScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	HeapTuple	tuple;
	TupleTableSlot *slot;

	/*
	 * extract necessary information from tid scan node
	 */
	scandesc = node->ss.ss_currentScanDesc;
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	slot = node->ss.ss_ScanTupleSlot;

	if (!node->trss_inScan)
	{
		BlockNumber blocks_to_scan;

		/* First time through, compute the list of TID ranges to be visited */
		if (node->trss_startBlock == InvalidBlockNumber)
			TidRangeEval(node);

		if (scandesc == NULL)
		{
			scandesc = table_beginscan_strat(node->ss.ss_currentRelation,
											estate->es_snapshot,
											0, NULL,
											false, false);
			node->ss.ss_currentScanDesc = scandesc;
		}

		/* Compute the number of blocks to scan and set the scan limits. */
		if (node->trss_startBlock == InvalidBlockNumber)
		{
			/* If the range is empty, set the scan limits to zero blocks. */
			node->trss_startBlock = 0;
			blocks_to_scan = 0;
		}
		else
			blocks_to_scan = node->trss_endBlock - node->trss_startBlock + 1;

		heap_setscanlimits(scandesc, node->trss_startBlock, blocks_to_scan);
		node->trss_inScan = true;
	}

	/* Fetch the next tuple. */
	tuple = NextInTidRange(node, scandesc, direction);

	/*
	 * If we've exhuasted all the tuples in the range, reset the inScan flag.
	 * This will cause the heap to be rescanned for any subsequent fetches,
	 * which is important for some cursor operations: for instance, FETCH LAST
	 * fetches all the tuples in order and then fetches one tuple in reverse.
	 */
	if (!tuple)
		node->trss_inScan = false;

	/*
	 * save the tuple and the buffer returned to us by the access methods in
	 * our scan tuple slot and return the slot.  Note also that
	 * ExecStoreBufferHeapTuple will increment the refcount of the buffer; the
	 * refcount will not be dropped until the tuple table slot is cleared.
	 */
	if (tuple)
		ExecStoreBufferHeapTuple(tuple, /* tuple to store */
								 slot,	/* slot to store in */
								 ((HeapScanDesc) scandesc)->rs_cbuf);	/* buffer associated
																		 * with this tuple */
	else
		ExecClearTuple(slot);

	return slot;
}

/*
 * TidRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
TidRangeRecheck(TidRangeScanState *node, TupleTableSlot *slot)
{
	/*
	 * XXX shouldn't we check here to make sure tuple is in TID range? In
	 * runtime-key case this is not certain, is it?
	 */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecTidRangeScan(node)
 *
 *		Scans the relation using tids and returns the next qualifying tuple
 *		in the direction specified.
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
 *		  -- trss_startBlock is InvalidBlockNumber
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecTidRangeScan(PlanState *pstate)
{
	TidRangeScanState *node = castNode(TidRangeScanState, pstate);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) TidRangeNext,
					(ExecScanRecheckMtd) TidRangeRecheck);
}

/* ----------------------------------------------------------------
 *		ExecReScanTidRangeScan(node)
 * ----------------------------------------------------------------
 */
void
ExecReScanTidRangeScan(TidRangeScanState *node)
{
	TableScanDesc scan = node->ss.ss_currentScanDesc;

	if (scan != NULL)
		table_rescan(scan,		/* scan desc */
					 NULL);		/* new scan keys */

	/* mark scan as not in progress, and tid range list as not computed yet */
	node->trss_inScan = false;
	node->trss_startBlock = InvalidBlockNumber;

	ExecScanReScan(&node->ss);
}

/* ----------------------------------------------------------------
 *		ExecEndTidRangeScan
 *
 *		Releases any storage allocated through C routines.
 *		Returns nothing.
 * ----------------------------------------------------------------
 */
void
ExecEndTidRangeScan(TidRangeScanState *node)
{
	TableScanDesc scan = node->ss.ss_currentScanDesc;

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
		table_endscan(scan);
}

/* ----------------------------------------------------------------
 *		ExecInitTidRangeScan
 *
 *		Initializes the tid range scan's state information, creates
 *		scan keys, and opens the base and tid relations.
 *
 *		Parameters:
 *		  node: TidRangeScan node produced by the planner.
 *		  estate: the execution state initialized in InitPlan.
 * ----------------------------------------------------------------
 */
TidRangeScanState *
ExecInitTidRangeScan(TidRangeScan *node, EState *estate, int eflags)
{
	TidRangeScanState *tidrangestate;
	Relation	currentRelation;

	/*
	 * create state structure
	 */
	tidrangestate = makeNode(TidRangeScanState);
	tidrangestate->ss.ps.plan = (Plan *) node;
	tidrangestate->ss.ps.state = estate;
	tidrangestate->ss.ps.ExecProcNode = ExecTidRangeScan;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &tidrangestate->ss.ps);

	/*
	 * mark scan as not in progress, and tid range list as not computed yet
	 */
	tidrangestate->trss_inScan = false;
	tidrangestate->trss_startBlock = InvalidBlockNumber;

	/*
	 * open the scan relation
	 */
	currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);

	tidrangestate->ss.ss_currentRelation = currentRelation;
	tidrangestate->ss.ss_currentScanDesc = NULL;	/* no heap scan here */

	/*
	 * get the scan type from the relation descriptor.
	 */
	ExecInitScanTupleSlot(estate, &tidrangestate->ss,
						  RelationGetDescr(currentRelation),
						  table_slot_callbacks(currentRelation));

	/*
	 * Initialize result type and projection.
	 */
	ExecInitResultTypeTL(&tidrangestate->ss.ps);
	ExecAssignScanProjectionInfo(&tidrangestate->ss);

	/*
	 * initialize child expressions
	 */
	tidrangestate->ss.ps.qual =
		ExecInitQual(node->scan.plan.qual, (PlanState *) tidrangestate);

	TidExprListCreate(tidrangestate);

	/*
	 * all done.
	 */
	return tidrangestate;
}
