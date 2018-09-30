/*-------------------------------------------------------------------------
 *
 * nodeIncremenalSort.c
 *	  Routines to handle incremental sorting of relations.
 *
 * DESCRIPTION
 *
 *		Incremental sort is an optimized variant of multikey sort for cases
 *		when the input is already sorted by a prefix of the sort keys.  For
 *		example when a sort by (key1, key2 ... keyN) is requested, and the
 *		input is already sorted by (key1, key2 ... keyM), M < N, we can
 *		divide the input into groups where keys (key1, ... keyM) are equal,
 *		and only sort on the remaining columns.
 *
 *		Consider the following example.  We have input tuples consisting of
 *		two integers (X, Y) already presorted by X, while it's required to
 *		sort them by both X and Y.  Let input tuples be following.
 *
 *		(1, 5)
 *		(1, 2)
 *		(2, 9)
 *		(2, 1)
 *		(2, 5)
 *		(3, 3)
 *		(3, 7)
 *
 *		Incremental sort algorithm would split the input into the following
 *		groups, which have equal X, and then sort them by Y individually:
 *
 *			(1, 5) (1, 2)
 *			(2, 9) (2, 1) (2, 5)
 *			(3, 3) (3, 7)
 *
 *		After sorting these groups and putting them altogether, we would get
 *		the following result which is sorted by X and Y, as requested:
 *
 *		(1, 2)
 *		(1, 5)
 *		(2, 1)
 *		(2, 5)
 *		(2, 9)
 *		(3, 3)
 *		(3, 7)
 *
 *		Incremental sort may be more efficient than plain sort, parcitularly
 *		on large datasets, as it reduces the amount of data to sort at once,
 *		making it more likely it fits into work_mem (eliminating the need to
 *		spill to disk).  But the main advantage of incremental sort is that
 *		it can start producing rows early, before sorting the whole dataset,
 *		which is a significant benefit especially for queries with LIMIT.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeIncremenalSort.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "executor/execdebug.h"
#include "executor/nodeIncrementalSort.h"
#include "miscadmin.h"
#include "utils/lsyscache.h"
#include "utils/tuplesort.h"

/*
 * Prepare information for presorted_keys comparison.
 */
static void
preparePresortedCols(IncrementalSortState *node)
{
	IncrementalSort	   *plannode = (IncrementalSort *) node->ss.ps.plan;
	int					presortedCols,
						i;

	Assert(IsA(plannode, IncrementalSort));
	presortedCols = plannode->presortedCols;

	node->presorted_keys = (PresortedKeyData *) palloc(presortedCols *
													sizeof(PresortedKeyData));

	for (i = 0; i < presortedCols; i++)
	{
		Oid					equalityOp,
							equalityFunc;
		PresortedKeyData   *key;

		key = &node->presorted_keys[i];
		key->attno = plannode->sort.sortColIdx[i];

		equalityOp = get_equality_op_for_ordering_op(
										plannode->sort.sortOperators[i], NULL);
		if (!OidIsValid(equalityOp))
			elog(ERROR, "missing equality operator for ordering operator %u",
					plannode->sort.sortOperators[i]);

		equalityFunc = get_opcode(equalityOp);
		if (!OidIsValid(equalityFunc))
			elog(ERROR, "missing function for operator %u", equalityOp);

		/* Lookup the comparison function */
		fmgr_info_cxt(equalityFunc, &key->flinfo, CurrentMemoryContext);

		/* We can initialize the callinfo just once and re-use it */
		InitFunctionCallInfoData(key->fcinfo, &key->flinfo, 2,
								plannode->sort.collations[i], NULL, NULL);
		key->fcinfo.argnull[0] = false;
		key->fcinfo.argnull[1] = false;
	}
}

/*
 * Check whether a given tuple belongs to the current sort group.
 *
 * We do this by comparing its first 'presortedCols' column values to
 * the pivot tuple of the current group.
 *
 */
static bool
isCurrentGroup(IncrementalSortState *node, TupleTableSlot *tupleSlot)
{
	int presortedCols, i;
	TupleTableSlot *group_pivot = node->group_pivot;

	Assert(IsA(node->ss.ps.plan, IncrementalSort));

	presortedCols = ((IncrementalSort *) node->ss.ps.plan)->presortedCols;

	/*
	 * We do assume the input is sorted by keys (0, ... n), which means
	 * the tail keys are more likely to change. So we do the comparison
	 * from the end, to minimize the number of function calls.
	 */
	for (i = presortedCols - 1; i >= 0; i--)
	{
		Datum				datumA,
							datumB,
							result;
		bool				isnullA,
							isnullB;
		AttrNumber			attno = node->presorted_keys[i].attno;
		PresortedKeyData   *key;

		datumA = slot_getattr(group_pivot, attno, &isnullA);
		datumB = slot_getattr(tupleSlot, attno, &isnullB);

		/* Special case for NULL-vs-NULL, else use standard comparison */
		if (isnullA || isnullB)
		{
			if (isnullA == isnullB)
				continue;
			else
				return false;
		}

		key = &node->presorted_keys[i];

		key->fcinfo.arg[0] = datumA;
		key->fcinfo.arg[1] = datumB;

		/* just for paranoia's sake, we reset isnull each time */
		key->fcinfo.isnull = false;

		result = FunctionCallInvoke(&key->fcinfo);

		/* Check for null result, since caller is clearly not expecting one */
		if (key->fcinfo.isnull)
			elog(ERROR, "function %u returned NULL", key->flinfo.fn_oid);

		if (!DatumGetBool(result))
			return false;
	}
	return true;
}

/*
 * Sorting many small groups with tuplesort is inefficient. In order to
 * cope with this problem we don't start a new group until the current one
 * contains at least DEFAULT_MIN_GROUP_SIZE tuples.  However, in the case
 * of bounded sort where bound is less than DEFAULT_MIN_GROUP_SIZE we start
 * looking for the new group when bound is done.
 */
#define DEFAULT_MIN_GROUP_SIZE 32

/* ----------------------------------------------------------------
 *		ExecIncrementalSort
 *
 *		Assuming that outer subtree returns tuple presorted by some prefix
 *		of target sort columns, performs incremental sort.  It fetches
 *		groups of tuples where prefix sort columns are equal and sorts them
 *		using tuplesort.  This approach allows to evade sorting of whole
 *		dataset.  Besides taking less memory and being faster, it allows to
 *		start returning tuples before fetching full dataset from outer
 *		subtree.
 *
 *		Conditions:
 *		  -- none.
 *
 *		Initial States:
 *		  -- the outer child is prepared to return the first tuple.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecIncrementalSort(PlanState *pstate)
{
	IncrementalSortState *node = castNode(IncrementalSortState, pstate);
	EState			   *estate;
	ScanDirection		dir;
	Tuplesortstate	   *tuplesortstate;
	TupleTableSlot	   *slot;
	IncrementalSort	   *plannode = (IncrementalSort *) node->ss.ps.plan;
	PlanState		   *outerNode;
	TupleDesc			tupDesc;
	int64				nTuples = 0;
	int64				minGroupSize;

	CHECK_FOR_INTERRUPTS();

	/*
	 * get state info from node
	 */
	SO1_printf("ExecIncrementalSort: %s\n",
			   "entering routine");

	estate = node->ss.ps.state;
	dir = estate->es_direction;
	tuplesortstate = (Tuplesortstate *) node->tuplesortstate;

	/*
	 * Return next tuple from the current sorted group set if available.
	 * If there are no more tuples in the current group, we need to try
	 * to fetch more tuples from the input and build another group.
	 */
	if (node->sort_Done)
	{
		slot = node->ss.ps.ps_ResultTupleSlot;
		if (tuplesort_gettupleslot(tuplesortstate,
									  ScanDirectionIsForward(dir),
									  false, slot, NULL) || node->finished)
			return slot;
	}

	/*
	 * First time through or no tuples in the current group. Read next
	 * batch of tuples from the outer plan and pass them to tuplesort.c.
	 * Subsequent calls just fetch tuples from tuplesort, until the group
	 * is exhausted, at which point we build the next group.
	 */

	SO1_printf("ExecIncrementalSort: %s\n",
			   "sorting subplan");

	/*
	 * Want to scan subplan in the forward direction while creating the
	 * sorted data.
	 */
	estate->es_direction = ForwardScanDirection;

	outerNode = outerPlanState(node);
	tupDesc = ExecGetResultType(outerNode);

	/*
	 * Initialize tuplesort module (needed only before the first group).
	 */
	if (node->tuplesortstate == NULL)
	{
		/*
		 * We are going to process the first group of presorted data.
		 * Initialize support structures for cmpSortPresortedCols - already
		 * sorted columns.
		 */
		preparePresortedCols(node);

		SO1_printf("ExecIncrementalSort: %s\n",
				   "calling tuplesort_begin_heap");

		/*
		 * Pass all the columns to tuplesort.  We pass to tuple sort groups
		 * of at least minGroupSize size.  Thus, these groups doesn't
		 * necessary have equal value of the first column.
		 */
		tuplesortstate = tuplesort_begin_heap(
									tupDesc,
									plannode->sort.numCols,
									plannode->sort.sortColIdx,
									plannode->sort.sortOperators,
									plannode->sort.collations,
									plannode->sort.nullsFirst,
									work_mem,
									NULL,
									false);
		node->tuplesortstate = (void *) tuplesortstate;
	}
	else
	{
		/* Next group of presorted data */
		tuplesort_reset((Tuplesortstate *) node->tuplesortstate);
	}
	node->group_count++;

	/*
	 * Calculate remaining bound for bounded sort and minimal group size
	 * accordingly.
	 */
	if (node->bounded)
	{
		tuplesort_set_bound(tuplesortstate, node->bound - node->bound_Done);
		minGroupSize = Min(DEFAULT_MIN_GROUP_SIZE, node->bound - node->bound_Done);
	}
	else
	{
		minGroupSize = DEFAULT_MIN_GROUP_SIZE;
	}

	/* If we got a leftover tuple from the last group, pass it to tuplesort. */
	if (!TupIsNull(node->group_pivot))
	{
		tuplesort_puttupleslot(tuplesortstate, node->group_pivot);
		ExecClearTuple(node->group_pivot);
		nTuples++;
	}

	/*
	 * Put next group of tuples where presortedCols sort values are equal to
	 * tuplesort.
	 */
	for (;;)
	{
		slot = ExecProcNode(outerNode);

		if (TupIsNull(slot))
		{
			node->finished = true;
			break;
		}

		/*
		 * Accumulate the next group of presorted tuples for tuplesort.
		 * We always accumulate at least minGroupSize tuples, and only
		 * then we start to compare the prefix keys.
		 *
		 * The last tuple is kept as a pivot, so that we can determine if
		 * the subsequent tuples have the same prefix key (same group).
		 */
		if (nTuples < minGroupSize)
		{
			tuplesort_puttupleslot(tuplesortstate, slot);

			/* Keep the last tuple in minimal group as a pivot. */
			if (nTuples == minGroupSize - 1)
				ExecCopySlot(node->group_pivot, slot);
			nTuples++;
		}
		else
		{
			/*
			 * Iterate while presorted cols are the same as in the pivot
			 * tuple.
			 *
			 * After accumulating at least minGroupSize tuples (we don't
			 * know how many groups are there in that set), we need to keep
			 * accumulating until we reach the end of the group. Only then
			 * we can do the sort and output all the tuples.
			 *
			 * We compare the prefix keys to the pivot - if the prefix keys
			 * are the same the tuple belongs to the same group, so we pass
			 * it to the tuplesort.
			 *
			 * If the prefix differs, we've reached the end of the group. We
			 * need to keep the last tuple, so we copy it into the pivot slot
			 * (it does not serve as pivot, though).
			 */
			if (isCurrentGroup(node, slot))
			{
				tuplesort_puttupleslot(tuplesortstate, slot);
				nTuples++;
			}
			else
			{
				ExecCopySlot(node->group_pivot, slot);
				break;
			}
		}
	}

	/*
	 * Complete the sort.
	 */
	tuplesort_performsort(tuplesortstate);

	/*
	 * restore to user specified direction
	 */
	estate->es_direction = dir;

	/*
	 * finally set the sorted flag to true
	 */
	node->sort_Done = true;
	node->bounded_Done = node->bounded;
	if (node->shared_info && node->am_worker)
	{
		TuplesortInstrumentation *si;

		Assert(IsParallelWorker());
		Assert(ParallelWorkerNumber <= node->shared_info->num_workers);
		si = &node->shared_info->sinfo[ParallelWorkerNumber].sinstrument;
		tuplesort_get_stats(tuplesortstate, si);
		node->shared_info->sinfo[ParallelWorkerNumber].group_count =
															node->group_count;
	}

	/*
	 * Adjust bound_Done with number of tuples we've actually sorted.
	 */
	if (node->bounded)
	{
		if (node->finished)
			node->bound_Done = node->bound;
		else
			node->bound_Done = Min(node->bound, node->bound_Done + nTuples);
	}

	SO1_printf("ExecIncrementalSort: %s\n", "sorting done");

	SO1_printf("ExecIncrementalSort: %s\n",
			   "retrieving tuple from tuplesort");

	/*
	 * Get the first or next tuple from tuplesort. Returns NULL if no more
	 * tuples.
	 */
	slot = node->ss.ps.ps_ResultTupleSlot;
	(void) tuplesort_gettupleslot(tuplesortstate,
								  ScanDirectionIsForward(dir),
								  false, slot, NULL);
	return slot;
}

/* ----------------------------------------------------------------
 *		ExecInitIncrementalSort
 *
 *		Creates the run-time state information for the sort node
 *		produced by the planner and initializes its outer subtree.
 * ----------------------------------------------------------------
 */
IncrementalSortState *
ExecInitIncrementalSort(IncrementalSort *node, EState *estate, int eflags)
{
	IncrementalSortState   *incrsortstate;

	SO1_printf("ExecInitIncrementalSort: %s\n",
			   "initializing sort node");

	/*
	 * Incremental sort can't be used with either EXEC_FLAG_REWIND,
	 * EXEC_FLAG_BACKWARD or EXEC_FLAG_MARK, because we hold only current
	 * bucket in tuplesortstate.
	 */
	Assert((eflags & (EXEC_FLAG_REWIND |
					  EXEC_FLAG_BACKWARD |
					  EXEC_FLAG_MARK)) == 0);

	/*
	 * create state structure
	 */
	incrsortstate = makeNode(IncrementalSortState);
	incrsortstate->ss.ps.plan = (Plan *) node;
	incrsortstate->ss.ps.state = estate;
	incrsortstate->ss.ps.ExecProcNode = ExecIncrementalSort;

	incrsortstate->bounded = false;
	incrsortstate->sort_Done = false;
	incrsortstate->finished = false;
	incrsortstate->tuplesortstate = NULL;
	incrsortstate->group_pivot = NULL;
	incrsortstate->bound_Done = 0;
	incrsortstate->group_count = 0;
	incrsortstate->presorted_keys = NULL;

	/*
	 * Miscellaneous initialization
	 *
	 * Sort nodes don't initialize their ExprContexts because they never call
	 * ExecQual or ExecProject.
	 */

	/*
	 * initialize child nodes
	 *
	 * We shield the child node from the need to support REWIND, BACKWARD, or
	 * MARK/RESTORE.
	 */
	eflags &= ~(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

	outerPlanState(incrsortstate) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * Initialize scan slot and type.
	 */
	ExecCreateScanSlotFromOuterPlan(estate, &incrsortstate->ss);

	/*
	 * Initialize return slot and type. No need to initialize projection info because
	 * this node doesn't do projections.
	 */
	ExecInitResultTupleSlotTL(estate, &incrsortstate->ss.ps);
	incrsortstate->ss.ps.ps_ProjInfo = NULL;

	/* make standalone slot to store previous tuple from outer node */
	incrsortstate->group_pivot = MakeSingleTupleTableSlot(
							ExecGetResultType(outerPlanState(incrsortstate)));

	SO1_printf("ExecInitIncrementalSort: %s\n",
			   "sort node initialized");

	return incrsortstate;
}

/* ----------------------------------------------------------------
 *		ExecEndIncrementalSort(node)
 * ----------------------------------------------------------------
 */
void
ExecEndIncrementalSort(IncrementalSortState *node)
{
	SO1_printf("ExecEndIncrementalSort: %s\n",
			   "shutting down sort node");

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ss_ScanTupleSlot);
	/* must drop pointer to sort result tuple */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	/* must drop stanalone tuple slot from outer node */
	ExecDropSingleTupleTableSlot(node->group_pivot);

	/*
	 * Release tuplesort resources
	 */
	if (node->tuplesortstate != NULL)
		tuplesort_end((Tuplesortstate *) node->tuplesortstate);
	node->tuplesortstate = NULL;

	/*
	 * shut down the subplan
	 */
	ExecEndNode(outerPlanState(node));

	SO1_printf("ExecEndIncrementalSort: %s\n",
			   "sort node shutdown");
}

void
ExecReScanIncrementalSort(IncrementalSortState *node)
{
	PlanState  *outerPlan = outerPlanState(node);

	/*
	 * If we haven't sorted yet, just return. If outerplan's chgParam is not
	 * NULL then it will be re-scanned by ExecProcNode, else no reason to
	 * re-scan it at all.
	 */
	if (!node->sort_Done)
		return;

	/* must drop pointer to sort result tuple */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	/*
	 * If subnode is to be rescanned then we forget previous sort results; we
	 * have to re-read the subplan and re-sort.  Also must re-sort if the
	 * bounded-sort parameters changed or we didn't select randomAccess.
	 *
	 * Otherwise we can just rewind and rescan the sorted output.
	 */
	node->sort_Done = false;
	tuplesort_end((Tuplesortstate *) node->tuplesortstate);
	node->tuplesortstate = NULL;
	node->bound_Done = 0;

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);
}

/* ----------------------------------------------------------------
 *						Parallel Query Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecSortEstimate
 *
 *		Estimate space required to propagate sort statistics.
 * ----------------------------------------------------------------
 */
void
ExecIncrementalSortEstimate(IncrementalSortState *node, ParallelContext *pcxt)
{
	Size		size;

	/* don't need this if not instrumenting or no workers */
	if (!node->ss.ps.instrument || pcxt->nworkers == 0)
		return;

	size = mul_size(pcxt->nworkers, sizeof(IncrementalSortInfo));
	size = add_size(size, offsetof(SharedIncrementalSortInfo, sinfo));
	shm_toc_estimate_chunk(&pcxt->estimator, size);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecSortInitializeDSM
 *
 *		Initialize DSM space for sort statistics.
 * ----------------------------------------------------------------
 */
void
ExecIncrementalSortInitializeDSM(IncrementalSortState *node, ParallelContext *pcxt)
{
	Size		size;

	/* don't need this if not instrumenting or no workers */
	if (!node->ss.ps.instrument || pcxt->nworkers == 0)
		return;

	size = offsetof(SharedIncrementalSortInfo, sinfo)
		+ pcxt->nworkers * sizeof(IncrementalSortInfo);
	node->shared_info = shm_toc_allocate(pcxt->toc, size);
	/* ensure any unfilled slots will contain zeroes */
	memset(node->shared_info, 0, size);
	node->shared_info->num_workers = pcxt->nworkers;
	shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id,
				   node->shared_info);
}

/* ----------------------------------------------------------------
 *		ExecSortInitializeWorker
 *
 *		Attach worker to DSM space for sort statistics.
 * ----------------------------------------------------------------
 */
void
ExecIncrementalSortInitializeWorker(IncrementalSortState *node, ParallelWorkerContext *pwcxt)
{
	node->shared_info =
		shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, true);
	node->am_worker = true;
}

/* ----------------------------------------------------------------
 *		ExecSortRetrieveInstrumentation
 *
 *		Transfer sort statistics from DSM to private memory.
 * ----------------------------------------------------------------
 */
void
ExecIncrementalSortRetrieveInstrumentation(IncrementalSortState *node)
{
	Size		size;
	SharedIncrementalSortInfo *si;

	if (node->shared_info == NULL)
		return;

	size = offsetof(SharedIncrementalSortInfo, sinfo)
		+ node->shared_info->num_workers * sizeof(IncrementalSortInfo);
	si = palloc(size);
	memcpy(si, node->shared_info, size);
	node->shared_info = si;
}
