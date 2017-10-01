/*-------------------------------------------------------------------------
 *
 * nodeIncremenalSort.c
 *	  Routines to handle incremental sorting of relations.
 *
 * DESCRIPTION
 *
 *		Incremental sort is specially optimized kind of multikey sort when
 *		input is already presorted by prefix of required keys list.  Thus,
 *		when it's required to sort by (key1, key2 ... keyN) and result is
 *		already sorted by (key1, key2 ... keyM), M < N, we sort groups where
 *		values of (key1, key2 ... keyM) are equal.
 *
 *		Consider following example.  We have input tuples consisting from
 *		two integers (x, y) already presorted by x, while it's required to
 *		sort them by x and y.  Let input tuples be following.
 *
 *		(1, 5)
 *		(1, 2)
 *		(2, 10)
 *		(2, 1)
 *		(2, 5)
 *		(3, 3)
 *		(3, 7)
 *
 *		Incremental sort algorithm would sort by xfollowing groups, which have
 *		equal x, individually:
 *			(1, 5) (1, 2)
 *			(2, 10) (2, 1) (2, 5)
 *			(3, 3) (3, 7)
 *
 *		After sorting these groups and putting them altogether, we would get
 *		following tuple set which is actually sorted by x and y.
 *
 *		(1, 2)
 *		(1, 5)
 *		(2, 1)
 *		(2, 5)
 *		(2, 10)
 *		(3, 3)
 *		(3, 7)
 *
 *		Incremental sort is faster than full sort on large datasets.  But
 *		the case of most huge benefit of incremental sort is queries with
 *		LIMIT because incremental sort can return first tuples without reading
 *		whole input dataset.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
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
 * Check if first "skipCols" sort values are equal.
 */
static bool
cmpSortSkipCols(IncrementalSortState *node, TupleTableSlot *a,
															TupleTableSlot *b)
{
	int n, i;

	Assert(IsA(node->ss.ps.plan, IncrementalSort));

	n = ((IncrementalSort *) node->ss.ps.plan)->skipCols;

	for (i = 0; i < n; i++)
	{
		Datum datumA, datumB, result;
		bool isnullA, isnullB;
		AttrNumber attno = node->skipKeys[i].attno;
		SkipKeyData *key;

		datumA = slot_getattr(a, attno, &isnullA);
		datumB = slot_getattr(b, attno, &isnullB);

		/* Special case for NULL-vs-NULL, else use standard comparison */
		if (isnullA || isnullB)
		{
			if (isnullA == isnullB)
				continue;
			else
				return false;
		}

		key = &node->skipKeys[i];

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
 * Prepare information for skipKeys comparison.
 */
static void
prepareSkipCols(IncrementalSortState *node)
{
	IncrementalSort	   *plannode = (IncrementalSort *) node->ss.ps.plan;
	int					skipCols,
						i;

	Assert(IsA(plannode, IncrementalSort));
	skipCols = plannode->skipCols;

	node->skipKeys = (SkipKeyData *) palloc(skipCols * sizeof(SkipKeyData));

	for (i = 0; i < skipCols; i++)
	{
		Oid equalityOp, equalityFunc;
		SkipKeyData *key;

		key = &node->skipKeys[i];
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


#define MIN_GROUP_SIZE 32

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

	/*
	 * get state info from node
	 */
	SO1_printf("ExecIncrementalSort: %s\n",
			   "entering routine");

	estate = node->ss.ps.state;
	dir = estate->es_direction;
	tuplesortstate = (Tuplesortstate *) node->tuplesortstate;

	/*
	 * Return next tuple from sorted set if any.
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
	 * If first time through, read all tuples from outer plan and pass them to
	 * tuplesort.c. Subsequent calls just fetch tuples from tuplesort.
	 */

	SO1_printf("ExecIncrementalSort: %s\n",
			   "sorting subplan");

	/*
	 * Want to scan subplan in the forward direction while creating the
	 * sorted data.
	 */
	estate->es_direction = ForwardScanDirection;

	/*
	 * Initialize tuplesort module.
	 */
	SO1_printf("ExecIncrementalSort: %s\n",
			   "calling tuplesort_begin");

	outerNode = outerPlanState(node);
	tupDesc = ExecGetResultType(outerNode);

	if (node->tuplesortstate == NULL)
	{
		/*
		 * We are going to process the first group of presorted data.
		 * Initialize support structures for cmpSortSkipCols - already
		 * sorted columns.
		 */
		prepareSkipCols(node);

		/*
		 * Pass all the columns to tuplesort.  We pass to tuple sort groups
		 * of at least MIN_GROUP_SIZE size.  Thus, these groups doesn't
		 * necessary have equal value of the first column.  We unlikely will
		 * have huge groups with incremental sort.  Therefore usage of
		 * abbreviated keys would be likely a waste of time.
		 */
		tuplesortstate = tuplesort_begin_heap(
									tupDesc,
									plannode->sort.numCols,
									plannode->sort.sortColIdx,
									plannode->sort.sortOperators,
									plannode->sort.collations,
									plannode->sort.nullsFirst,
									work_mem,
									false,
									true);
		node->tuplesortstate = (void *) tuplesortstate;
		node->groupsCount++;
	}
	else
	{
		/* Next group of presorted data */
		tuplesort_reset((Tuplesortstate *) node->tuplesortstate);
		node->groupsCount++;
	}

	/* Calculate remaining bound for bounded sort */
	if (node->bounded)
		tuplesort_set_bound(tuplesortstate, node->bound - node->bound_Done);

	/* Put saved tuple to tuplesort if any */
	if (!TupIsNull(node->sampleSlot))
	{
		tuplesort_puttupleslot(tuplesortstate, node->sampleSlot);
		ExecClearTuple(node->sampleSlot);
		nTuples++;
	}

	/*
	 * Put next group of tuples where skipCols sort values are equal to
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

		/* Put next group of presorted data to the tuplesort */
		if (nTuples < MIN_GROUP_SIZE)
		{
			tuplesort_puttupleslot(tuplesortstate, slot);

			/* Save last tuple in minimal group */
			if (nTuples == MIN_GROUP_SIZE - 1)
				ExecCopySlot(node->sampleSlot, slot);
			nTuples++;
		}
		else
		{
			/* Interate while skip cols are same as in saved tuple */
			bool	cmp;
			cmp = cmpSortSkipCols(node, node->sampleSlot, slot);

			if (cmp)
			{
				tuplesort_puttupleslot(tuplesortstate, slot);
				nTuples++;
			}
			else
			{
				ExecCopySlot(node->sampleSlot, slot);
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
		node->shared_info->sinfo[ParallelWorkerNumber].groupsCount =
															node->groupsCount;
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
	incrsortstate->sampleSlot = NULL;
	incrsortstate->bound_Done = 0;
	incrsortstate->groupsCount = 0;
	incrsortstate->skipKeys = NULL;

	/*
	 * Miscellaneous initialization
	 *
	 * Sort nodes don't initialize their ExprContexts because they never call
	 * ExecQual or ExecProject.
	 */

	/*
	 * tuple table initialization
	 *
	 * sort nodes only return scan tuples from their sorted relation.
	 */
	ExecInitResultTupleSlot(estate, &incrsortstate->ss.ps);
	ExecInitScanTupleSlot(estate, &incrsortstate->ss);

	/*
	 * initialize child nodes
	 *
	 * We shield the child node from the need to support REWIND, BACKWARD, or
	 * MARK/RESTORE.
	 */
	eflags &= ~(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

	outerPlanState(incrsortstate) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * initialize tuple type.  no need to initialize projection info because
	 * this node doesn't do projections.
	 */
	ExecAssignResultTypeFromTL(&incrsortstate->ss.ps);
	ExecAssignScanTypeFromOuterPlan(&incrsortstate->ss);
	incrsortstate->ss.ps.ps_ProjInfo = NULL;

	/* make standalone slot to store previous tuple from outer node */
	incrsortstate->sampleSlot = MakeSingleTupleTableSlot(
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
	ExecDropSingleTupleTableSlot(node->sampleSlot);

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
 *		ExecSortReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecIncrementalSortReInitializeDSM(IncrementalSortState *node, ParallelContext *pcxt)
{
	/* If there's any instrumentation space, clear it for next time */
	if (node->shared_info != NULL)
	{
		memset(node->shared_info->sinfo, 0,
			   node->shared_info->num_workers * sizeof(IncrementalSortInfo));
	}
}

/* ----------------------------------------------------------------
 *		ExecSortInitializeWorker
 *
 *		Attach worker to DSM space for sort statistics.
 * ----------------------------------------------------------------
 */
void
ExecIncrementalSortInitializeWorker(IncrementalSortState *node, shm_toc *toc)
{
	node->shared_info =
		shm_toc_lookup(toc, node->ss.ps.plan->plan_node_id, true);
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
