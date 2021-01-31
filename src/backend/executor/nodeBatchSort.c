#include "postgres.h"

#include "common/hashfn.h"
#include "executor/executor.h"
#include "executor/nodeBatchSort.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "storage/barrier.h"
#include "utils/builtins.h"
#include "utils/tuplesort.h"
#include "utils/typcache.h"

typedef struct ParallelBatchSort
{
	Barrier				barrier;		/* data build barrier */
	pg_atomic_uint32	attached;		/* how many worker attached */
	pg_atomic_uint32	cur_batch;		/* current not final sort batch number */
	Size				tuplesort_size;	/* MAXIMUM_ALIGNOF*n */
}ParallelBatchSort;

#define PARALLEL_BATCH_SORT_SIZE		MAXALIGN(sizeof(ParallelBatchSort))
#define PARALLEL_BATCH_SORT_SHARED(p,n)	\
	(Sharedsort*)(((char*)(p)) + PARALLEL_BATCH_SORT_SIZE + (p)->tuplesort_size * (n))

#define BUILD_BATCH_DONE	1

/*
 * if we can scan next batch, then return true and open it,
 * else return false
 */
static bool
ExecNextParallelBatchSort(BatchSortState *state)
{
	ParallelBatchSort  *parallel = state->parallel;
	BatchSort		   *plan = castNode(BatchSort, state->ps.plan);
	SortCoordinateData	coord;
	uint32				cur_batch;
	Assert(parallel != NULL);

	/* do we opened other batch? */
	if (state->curBatch >= 0 &&
		state->curBatch < plan->numBatches &&
		state->batches[state->curBatch] != NULL)
	{
		/* close it */
		tuplesort_end(state->batches[state->curBatch]);
		state->batches[state->curBatch] = NULL;
	}

	/* get next batch number and set next number in shared memory */
	cur_batch = pg_atomic_fetch_add_u32(&parallel->cur_batch, 1);
	if (cur_batch >= plan->numBatches)
	{
		/* out of batch range, no more batch */
		state->curBatch = plan->numBatches;
		return false;
	}

	/* open tuplesort as leader */
	Assert(state->batches[cur_batch] == NULL);
	state->curBatch = cur_batch;
	coord.isWorker = false;		/* we are leader */
	coord.nParticipants = pg_atomic_read_u32(&parallel->attached);
	coord.sharedsort = PARALLEL_BATCH_SORT_SHARED(parallel, cur_batch);
	state->batches[cur_batch] = tuplesort_begin_heap(ExecGetResultType(outerPlanState(state)),
													 plan->sort.numCols,
													 plan->sort.sortColIdx,
													 plan->sort.sortOperators,
													 plan->sort.collations,
													 plan->sort.nullsFirst,
													 work_mem,
													 &coord,
													 false);
	tuplesort_performsort(state->batches[cur_batch]);
	return true;
}

/*
 * return a empty TupleTableSlot
 */
static TupleTableSlot *
ExecEmptyBatchSort(PlanState *pstate)
{
	return ExecClearTuple(pstate->ps_ResultTupleSlot);
}

/*
 * scan tuple slot from tuplesort,
 * all batch must be sort completed(called function ExecBatchSortPrepare)
 */
static TupleTableSlot *
ExecBatchSort(PlanState *pstate)
{
	TupleTableSlot *slot = pstate->ps_ResultTupleSlot;
	BatchSortState *state = castNode(BatchSortState, pstate);
	Assert(state->sort_Done);

re_get_:
	/*
	 * Get tuple from current tuplesort
	 */
	if (likely(tuplesort_gettupleslot(state->batches[state->curBatch],
									  true,
									  false,
									  slot,
									  NULL)))
		return slot;

	/*
	 * Is all batches scanned?
	 */
	if (state->curBatch < castNode(BatchSort, pstate->plan)->numBatches-1)
	{
		/* Try next batch */
		if (state->parallel)
		{
			if (ExecNextParallelBatchSort(state) == false)
			{
				/* No nore batches */
				ExecSetExecProcNode(pstate, ExecEmptyBatchSort);
				return ExecClearTuple(slot);
			}
			/*
			 * OK, we can scan next batch,
			 * function ExecNextParallelBatchSort changed curBatch
			 */
		}else
		{
			state->curBatch++;
		}
		goto re_get_;
	}

	/*
	 * no more batches, return empty TupleTableSlot
	 * tuplesort_gettupleslot cleaned slot
	 */
	Assert(TupIsNull(slot));
	return slot;
}

/*
 * Sorts tuples from the outer subree of the node using tuplesort
 */
static TupleTableSlot *
ExecBatchSortPrepare(PlanState *pstate)
{
	BatchSort		   *node = castNode(BatchSort, pstate->plan);
	BatchSortState	   *state = castNode(BatchSortState, pstate);
	PlanState		   *outerNode = outerPlanState(pstate);
	TupleTableSlot	   *slot;
	ListCell		   *lc;
	ParallelBatchSort  *parallel = state->parallel;
	SortCoordinateData	coord;
	FunctionCallInfo	fcinfo;
	uint32				hash;
	int					i;
	AttrNumber			maxAttr;
	Assert(state->sort_Done == false ||
		   (parallel && BarrierPhase(&parallel->barrier) >= BUILD_BATCH_DONE));
	Assert(list_length(state->groupFuns) == node->numGroupCols);

	/* Is parallel mode */
	if (parallel)
	{
		/* Attach to barrier and test current state */
		if (BarrierAttach(&parallel->barrier) >= BUILD_BATCH_DONE)
		{
			/*
			 * all tuples sorted by other workers
			 * we don't need scan tuple from outer subtree
			 * and can not change batches state(too late)
			 */
			goto build_already_done_;
		}
		/* let other workers known we are attached */
		pg_atomic_add_fetch_u32(&parallel->attached, 1);
	}

	/* Make all tuplesort */
	for (i=node->numBatches;i>0;)
	{
		--i;
		if (parallel)
		{
			coord.isWorker = true;		/* We are not leader */
			coord.nParticipants = -1;
			coord.sharedsort = PARALLEL_BATCH_SORT_SHARED(parallel, i);
		}
		state->batches[i] = tuplesort_begin_heap(ExecGetResultType(outerNode),
												 node->sort.numCols,
												 node->sort.sortColIdx,
												 node->sort.sortOperators,
												 node->sort.collations,
												 node->sort.nullsFirst,
												 work_mem / node->numBatches,
												 parallel ? &coord : NULL,
												 false);
	}

	/*
	 * Get max using attribute number,
	 * we don't want call slot_getsomeattrs() more than once
	 */
	maxAttr = 0;
	for (i=node->numGroupCols;i>0;)
	{
		if (maxAttr < node->grpColIdx[--i])
			maxAttr = node->grpColIdx[i];
	}
	for (i=node->sort.numCols;i>0;)
	{
		if (maxAttr < node->sort.sortColIdx[--i])
			maxAttr = node->sort.sortColIdx[i];
	}
	Assert(maxAttr > 0);

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();
		slot = ExecProcNode(outerNode);
		if (TupIsNull(slot))
			break;

		/* Get all we using datums from tuple */
		slot_getsomeattrs(slot, maxAttr);

		/* Get hash value */
		hash = 0;
		i = 0;
		foreach(lc, state->groupFuns)
		{
			AttrNumber att = node->grpColIdx[i++]-1;
			if (slot->tts_isnull[att] == false)
			{
				fcinfo = lfirst(lc);
				fcinfo->args[0].value = slot->tts_values[att];
				hash = hash_combine(hash, DatumGetUInt32(FunctionCallInvoke(fcinfo)));
				Assert(fcinfo->isnull == false);
			}
		}

		/* Put tuple to differed batch using hash value */
		tuplesort_puttupleslot(state->batches[hash%node->numBatches], slot);
	}

	/* Finish all tuplesort put */
	for (i=node->numBatches;i>0;)
	{
		--i;
		tuplesort_performsort(state->batches[i]);
		if (parallel)
		{
			/*
			 * In parallel mode we need reopen tuplesort as leader
			 * and almost impossible get all batches,
			 * so we close it for free resource
			 */
			tuplesort_end(state->batches[i]);
			state->batches[i] = NULL;
		}
	}

	if (parallel)
	{
		/* Wait other workers finish sort */
		BarrierArriveAndWait(&parallel->barrier, WAIT_EVENT_BATCH_SORT_BUILD);
		Assert(BarrierPhase(&parallel->barrier) >= BUILD_BATCH_DONE);
	}

build_already_done_:
	/* All tuplesort already done */
	state->sort_Done = true;

	/*
	 * Change execute function.
	 * Separating the preparation and scanning data functions helps to improve performance,
	 * because we don't have to judge whether the data is ready every time
	 */
	if (parallel)
	{
		/* we don't need barrier any more */
		BarrierDetach(&parallel->barrier);

		if (ExecNextParallelBatchSort(state))
			ExecSetExecProcNode(pstate, ExecBatchSort);	/* got a batch, we can scan it */
		else
			ExecSetExecProcNode(pstate, ExecEmptyBatchSort);	/* no more batch, so no more tuple */
	}else
	{
		/* scan tuplesort from first batch */
		state->curBatch = 0;
		ExecSetExecProcNode(pstate, ExecBatchSort);
	}

	/* return first tuple or empty */
	return (*pstate->ExecProcNodeReal)(pstate);
}

/*
 * Create the run-time state information for the batch sort node
 */
BatchSortState *
ExecInitBatchSort(BatchSort *node, EState *estate, int eflags)
{
	BatchSortState *state;
	TypeCacheEntry *typentry;
	TupleDesc		desc;
	int				i;

	state = makeNode(BatchSortState);
	state->ps.plan = (Plan*) node;
	state->ps.state = estate;
	state->ps.ExecProcNode = ExecBatchSortPrepare;

	state->sort_Done = false;
	state->batches = palloc0(node->numBatches * sizeof(Tuplesortstate*));

	outerPlanState(state) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * Initialize return slot and type. No need to initialize projection info
	 * because this node doesn't do projections.
	 */
	ExecInitResultTupleSlotTL(&state->ps, &TTSOpsMinimalTuple);
	state->ps.ps_ProjInfo = NULL;

	/*
	 * create hash function call info data
	 */
	Assert(node->numGroupCols > 0);
	desc = ExecGetResultType(outerPlanState(state));
	for (i=0;i<node->numGroupCols;++i)
	{
		FmgrInfo			   *flinfo;
		FunctionCallInfo		fcinfo;
		Form_pg_attribute		attr = TupleDescAttr(desc, node->grpColIdx[i]-1);
		typentry = lookup_type_cache(attr->atttypid, TYPECACHE_HASH_PROC);
		if (!OidIsValid(typentry->hash_proc))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("could not identify an extended hash function for type %s",
							format_type_be(attr->atttypid))));

		/* Alloc memory for hash function call info data */
		flinfo = palloc0(sizeof(*flinfo));
		fcinfo = palloc0(SizeForFunctionCallInfo(1));	/* hash function only have one argument */
		fmgr_info(typentry->hash_proc, flinfo);
		InitFunctionCallInfoData(*fcinfo, flinfo, 1, attr->attcollation, NULL, NULL);
		fcinfo->args[0].isnull = false;	/* first argument always not null */

		/* Save call info data to run-time state */
		state->groupFuns = lappend(state->groupFuns, fcinfo);
	}

	return state;
}

/*
 * Free tuplesort resources
 * and set "all tuplesort already done" as false
 */
static void
CleanBatchSort(BatchSortState *node)
{
	int i;

	ExecClearTuple(node->ps.ps_ResultTupleSlot);
	if (node->sort_Done)
	{
		for (i=castNode(BatchSort, node->ps.plan)->numBatches;i>0;)
		{
			if (node->batches[--i] != NULL)
			{
				tuplesort_end(node->batches[i]);
				node->batches[i] = NULL;
			}
		}
		node->sort_Done = false;
	}
}

void
ExecEndBatchSort(BatchSortState *node)
{
	ExecClearTuple(node->ps.ps_ResultTupleSlot);
	CleanBatchSort(node);
	ExecEndNode(outerPlanState(node));
}

void
ExecReScanBatchSort(BatchSortState *node)
{
	int					i;
	Bitmapset		   *outerParams;
	ParallelBatchSort  *parallel;
	BatchSort		   *plan;

	/* If we haven't sorted yet, just return. */
	if (node->sort_Done == false)
		return;

	parallel = node->parallel;
	outerParams = outerPlanState(node)->chgParam;
	plan = castNode(BatchSort, node->ps.plan);

	/* must drop pointer to sort result tuple */
	ExecClearTuple(node->ps.ps_ResultTupleSlot);

	if (bms_is_empty(outerParams) ||
		/*
		 * In parallel mod in parallel mode don't need rescan outer
		 * when it only have gather param change,
		 * because we cached all tuple from it and we can read in any worker
		 */
		(parallel != NULL &&
		 plan->gather_param >= 0 &&
		 bms_membership(outerParams) == BMS_SINGLETON &&
		 bms_is_member(plan->gather_param, outerParams)))
	{
		if (parallel)
		{
			/* restart batches */
			pg_atomic_write_u32(&parallel->cur_batch, 0);

			/*
			 * We want call ExecNextParallelBatchSort() function when
			 * parent call ExecProcNode(), not now. Because it is possible that
			 * other workers will finish processing other batches very quickly,
			 * and we did occupy a batch and we have been in no hurry to deal with it.
			 * So we don't want occupy a batch for now.
			 */
			ExecSetExecProcNode(&node->ps, ExecBatchSortPrepare);
		}else
		{
			/* restart batches */
			node->curBatch = 0;

			/* rescan tuplesort */
			i = plan->numBatches;
			while (i>0)
				tuplesort_rescan(node->batches[--i]);
			ExecSetExecProcNode(&node->ps, ExecBatchSort);
		}
	}else
	{
		/* clean opend tuplesort */
		CleanBatchSort(node);

		if (parallel)
		{
			/* reset tuplesort */
			i = plan->numBatches;
			while (i>0)
			{
				--i;
				tuplesort_reset_shared(PARALLEL_BATCH_SORT_SHARED(parallel, i));
			}

			/* reset shared memory */
			BarrierInit(&parallel->barrier, 0);
			pg_atomic_write_u32(&parallel->attached, 0);
			pg_atomic_write_u32(&parallel->cur_batch, 0);
		}
		ExecSetExecProcNode(&node->ps, ExecBatchSortPrepare);
		node->sort_Done = false;
	}
}

void
ExecShutdownBatchSort(BatchSortState *node)
{
	CleanBatchSort(node);
}

/* ----------------------------------------------------------------
 *						Parallel Query Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecBatchSortEstimate
 *
 *		Estimate space required to propagate sort statistics.
 * ----------------------------------------------------------------
 */
void
ExecBatchSortEstimate(BatchSortState *node, ParallelContext *pcxt)
{
	Size size = mul_size(MAXALIGN(tuplesort_estimate_shared(pcxt->nworkers+1)),
						 castNode(BatchSort, node->ps.plan)->numBatches);
	size = add_size(size, PARALLEL_BATCH_SORT_SIZE);

	shm_toc_estimate_chunk(&pcxt->estimator, size);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/* Initialize shared memory for batch sort */
static void
InitializeBatchSortParallel(ParallelBatchSort *parallel,
							int num_batches,
							int num_workers,
							dsm_segment *seg)
{
	int i;
	BarrierInit(&parallel->barrier, 0);
	pg_atomic_init_u32(&parallel->attached, 0);
	pg_atomic_init_u32(&parallel->cur_batch, 0);
	for (i=0;i<num_batches;++i)
	{
		tuplesort_initialize_shared(PARALLEL_BATCH_SORT_SHARED(parallel, i),
									num_workers,
									seg);
	}
}

void
ExecBatchSortInitializeDSM(BatchSortState *node, ParallelContext *pcxt)
{
	ParallelBatchSort  *parallel;
	BatchSort		   *plan = castNode(BatchSort, node->ps.plan);
	Size				tuplesort_size = MAXALIGN(tuplesort_estimate_shared(pcxt->nworkers+1));
	Size				size = mul_size(tuplesort_size, plan->numBatches);
	size = add_size(PARALLEL_BATCH_SORT_SIZE, size);

	node->parallel = parallel = shm_toc_allocate(pcxt->toc, size);
	parallel->tuplesort_size = tuplesort_size;
	InitializeBatchSortParallel(parallel, plan->numBatches, pcxt->nworkers+1, pcxt->seg);
	shm_toc_insert(pcxt->toc, plan->sort.plan.plan_node_id, parallel);
}

void
ExecBatchSortReInitializeDSM(BatchSortState *node, ParallelContext *pcxt)
{
	pg_atomic_write_u32(&node->parallel->cur_batch, 0);
}

void
ExecBatchSortInitializeWorker(BatchSortState *node, ParallelWorkerContext *pwcxt)
{
	uint32				i;
	BatchSort		   *plan = castNode(BatchSort, node->ps.plan);
	ParallelBatchSort  *parallel = shm_toc_lookup(pwcxt->toc,
												  plan->sort.plan.plan_node_id,
												  false);
	node->parallel = parallel;
	for (i=0;i<plan->numBatches;++i)
	{
		tuplesort_attach_shared(PARALLEL_BATCH_SORT_SHARED(parallel, i),
								pwcxt->seg);
	}
}
