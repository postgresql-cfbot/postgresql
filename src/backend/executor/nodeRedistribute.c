
#include "postgres.h"

#include "common/hashfn.h"
#include "executor/executor.h"
#include "executor/nodeRedistribute.h"
#include "miscadmin.h"
#include "storage/barrier.h"
#include "storage/shm_mq.h"
#include "utils/sharedtuplestore.h"
#include "utils/typcache.h"
#include "utils/wait_event.h"

#define INVALID_PART_NUMBER			0xffffffff

/*
 * local scan status
 */
#define	STATUS_LOCALLY_END			0x01	/* outer returned NULL slot */
#define STATUS_QUERY_STS_END		0x02	/* all tuples on STS scanned */
#define STATUS_READY_STS_READ		0x04	/* STS ready to scan */

/*
 * all workers working status
 */
#define PRW_WAIT_WORKER_NUMBER		0	/* waiting leader write worker number to shared memory */
#define PRW_WRITING					1	/* writing tuple(s) to STS and MQ */
#define PRW_END_STS					2	/* all attached worker called sts_end_write(ready for read) */

#if 1
#define REDISTRIBUTE_DEBUG(...)	ereport(LOG, errbacktrace(), errmsg(__VA_ARGS__))
#else
#define REDISTRIBUTE_DEBUG(...)	((void)0)
#endif

#define SharedRedistributeOffset(shmem_, offset_)	\
	(AssertMacro((shmem_)->offset_ >= offsetof(RedistributeShmem, padding)), \
	 ((char*)(shmem_)) + (shmem_)->offset_)

typedef struct RedistributeWriteInfo
{
	SharedTuplestoreAccessor   *sta;
	shm_mq_handle			   *mqwrite;
}RedistributeWriteInfo;

typedef struct RedistributeShmem
{
	Barrier				part_barrier;				/* see PRW_XXX */
	pg_atomic_uint32	final_worker_num;			/* how many workers final start successed */
	pg_atomic_uint32	next_idle_part;				/* next idle part index */
	uint32				plan_worker_num;			/* how many workers plan to start */
	Size				sts_shmem_offset;			/* all SharedTuplestore offset (start with this struct) */
	Size				sts_shmem_size;				/* each SharedTuplestore size in bytes */
	Size				mq_shmem_offset;			/* all shm_mq offset (start with this struct) */
	Size				mq_shmem_size;				/* each shm_mq size in bytes */
	Size				mq_shmem_part_size;			/* equal plan_worker_num * mq_shmem_size */
	Size				instrument_shmem_offset;	/* RedistributeInstrumentations offset (start with this struct), 0 for not alloc */
	SharedFileSet		fileset;					/* using by SharedTuplestore */

	char				padding[FLEXIBLE_ARRAY_MEMBER];
}RedistributeShmem;

typedef struct RedistributeHashInfo
{
	FmgrInfo	flinfo;
	union
	{
		FunctionCallInfoBaseData	fcinfo;
		char	fcinfo_data[SizeForFunctionCallInfo(1)];
	};
	AttrNumber	attindex;
}RedistributeHashInfo;

int	redistribute_query_size = 0;	/* GUC variable */

#define WaitMyLatch(wait_event_info)	\
	WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0, wait_event_info)

/*
 * This function should not be call.
 * When resource not ready, just report an error,
 * else some function will call ExecSetExecProcNode using different execute function
 */
static TupleTableSlot *
ExecRedistributeError(PlanState *pstate)
{
	ereport(ERROR,
			errcode(ERRCODE_INTERNAL_ERROR),
			errmsg("DSM not initialized for Redistribute plan"));

	return NULL;	/* keep compiler quiet */
}

static TupleTableSlot *
ExecCopyOuterSlot(TupleTableSlot *result, TupleTableSlot *outer)
{
	int		natts = result->tts_tupleDescriptor->natts;
	Assert(natts == outer->tts_tupleDescriptor->natts);

	if (result->tts_ops == outer->tts_ops)
		return outer;

	/* copy outer slot to result slot and return */
	ExecClearTuple(result);
	slot_getsomeattrs(outer, natts);
	memcpy(result->tts_values, outer->tts_values, sizeof(outer->tts_values[0]) * natts);
	memcpy(result->tts_isnull, outer->tts_isnull, sizeof(outer->tts_isnull[0]) * natts);
	return ExecStoreVirtualTuple(result);
}

/*
 * ExecRedistributeDirect
 * this function for no any one parallel work start successed
 */
static TupleTableSlot *
ExecRedistributeDirect(PlanState *pstate)
{
	TupleTableSlot *outer_slot = ExecProcNode(outerPlanState(pstate));
	if (unlikely(TupIsNull(outer_slot)))
		return ExecClearTuple(pstate->ps_ResultTupleSlot);

	return ExecCopyOuterSlot(pstate->ps_ResultTupleSlot, outer_slot);
}

static shm_mq*
GetRedistributeSharedMemoryQueue(RedistributeShmem *shmem, uint32 part, uint32 worknum)
{
	/* get shm_mq start address */
	char *addr = SharedRedistributeOffset(shmem, mq_shmem_offset);

	/* get part start address */
	addr += shmem->mq_shmem_part_size * part;

	/* get offset of part */
	addr += shmem->mq_shmem_size * worknum;

	return (shm_mq*)addr;
}

static void
ExecInitRedistributeWriter(RedistributeState *node, uint32 my_work_num)
{
	MemoryContext		oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(node));
	RedistributeShmem  *shmem = node->shmem;
	uint32				nworkers = pg_atomic_read_u32(&shmem->final_worker_num);
	uint32				mq_offset = shmem->sts_shmem_size + shmem->mq_shmem_part_size;
	uint32				i;

	for (i=0;i<nworkers;++i)
	{
		RedistributeWriteInfo  *writer;
		char				   *addr;
		shm_mq				   *mq;
		if (unlikely(i == my_work_num))
			continue;

		addr = SharedRedistributeOffset(shmem, sts_shmem_offset);
		addr += shmem->sts_shmem_size * i;
		writer = &node->writer[i];
		writer->sta = sts_attach((SharedTuplestore*)addr, my_work_num, &shmem->fileset);

		REDISTRIBUTE_DEBUG("writer sts_attach(%p)=%p mq_attach %p part %u index %u",
						   addr, writer->sta, addr + mq_offset, my_work_num, i);

		mq = GetRedistributeSharedMemoryQueue(shmem, i, my_work_num);
		shm_mq_set_sender(mq, MyProc);
		writer->mqwrite = shm_mq_attach(mq, NULL, NULL);
	}

	MemoryContextSwitchTo(oldcontext);
}

static void
ExecInitRedistributeReader(RedistributeState *node)
{
	MemoryContext		oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(node));
	RedistributeShmem  *shmem = node->shmem;
	char			   *start;
	uint32				i;
	uint32				nworkers = pg_atomic_read_u32(&shmem->final_worker_num);
	Assert(node->current_part < nworkers);

	/* attach SharedTuplestore */
	start = SharedRedistributeOffset(shmem, sts_shmem_offset);
	start += node->current_part * shmem->sts_shmem_size;
	node->sta = sts_attach((SharedTuplestore*)start,
						   node->current_part,
						   &shmem->fileset);
	REDISTRIBUTE_DEBUG("reader sts_attach(%p)=%p part %u", start, node->sta, node->current_part);

	/* attach shm_mq */
	node->nextreader = 0;
	node->nreaders = 0;
	for (i=0;i<nworkers;++i)
	{
		shm_mq *mq;
		if (unlikely(i == node->current_part))
			continue;

		mq = GetRedistributeSharedMemoryQueue(shmem, node->current_part, i);
		REDISTRIBUTE_DEBUG("reader mq_attach %p part %u", mq, node->current_part);
		shm_mq_set_receiver(mq, MyProc);
		node->mqreader[node->nreaders] = shm_mq_attach(mq, NULL, NULL);
		++(node->nreaders);
	}

	MemoryContextSwitchTo(oldcontext);
}

static bool
ExecNextRedistributePart(RedistributeState *node)
{
	RedistributeShmem  *shmem = node->shmem;
	uint32				count_part = pg_atomic_read_u32(&shmem->final_worker_num);
	uint32				idle_part = pg_atomic_read_u32(&shmem->next_idle_part);

	while (idle_part < count_part)
	{
		if (pg_atomic_compare_exchange_u32(&shmem->next_idle_part,
										   &idle_part,
										   idle_part + 1))
		{
			node->current_part = idle_part;
			if (node->instrument)
				++(node->instrument->parts_got);
			return true;
		}
	}

	node->current_part = INVALID_PART_NUMBER;
	return false;
}

/*
 * initialize redistribute hash function info list
 * and alloc reader and writer array
 */
static void
InitializeRedistributeExecute(RedistributeState *node)
{
	AttrNumber				maxattr;
	int						i;
	TypeCacheEntry		   *typeCache;
	RedistributeHashInfo   *info;
	Form_pg_attribute		attr;
	TupleDesc				desc = ExecGetResultType(outerPlanState(node));
	List				   *list = NIL;
	Redistribute		   *plan = castNode(Redistribute, node->ps.plan);
	RedistributeShmem	   *shmem = node->shmem;
	MemoryContext			oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(node));

	/* initialize hash functions */
	if (node->hash_funcs == NIL)
	{
		maxattr = 0;
		for (i=0;i<plan->numCols;++i)
		{
			info = palloc0(sizeof(*info));

			info->attindex = plan->hashColIdx[i]-1;
			attr = TupleDescAttr(desc, info->attindex);

			typeCache = lookup_type_cache(attr->atttypid, TYPECACHE_HASH_PROC);

			fmgr_info(typeCache->hash_proc, &info->flinfo);
			InitFunctionCallInfoData(info->fcinfo, &info->flinfo, 1, attr->attcollation, NULL, NULL);
			info->fcinfo.args[0].isnull = false;
			list = lappend(list, info);

			if (plan->hashColIdx[i] > maxattr)
				maxattr = plan->hashColIdx[i];
		}
		node->hash_funcs = list;
		node->hash_max_attr = maxattr;
	}

	/* alloc reader and writer array */
	Assert(pg_atomic_read_u32(&shmem->final_worker_num) > 1);
	Assert(shmem->plan_worker_num >= pg_atomic_read_u32(&shmem->final_worker_num));
	if (node->mqreader == NULL)
		node->mqreader = palloc0(sizeof(node->mqreader[0]) * shmem->plan_worker_num);
	if (node->writer == NULL)
	{
		node->writer = palloc0(sizeof(node->writer[0]) * shmem->plan_worker_num);
		node->nwriter = shmem->plan_worker_num;
	}

	MemoryContextSwitchTo(oldcontext);
}

static uint32
RedistributeGetHash(RedistributeState *node, TupleTableSlot *input)
{
	ListCell			   *lc;
	RedistributeHashInfo   *info;
	Datum					datum;
	uint32					hash = 0;

	/* get all values we need */
	slot_getsomeattrs(input, node->hash_max_attr);

	foreach (lc, node->hash_funcs)
	{
		info = lfirst(lc);

		/* skip NULL */
		if (input->tts_isnull[info->attindex])
			continue;

		info->fcinfo.args[0].value = input->tts_values[info->attindex];
		datum = FunctionCallInvoke(&info->fcinfo);
		if (unlikely(info->fcinfo.isnull))
			elog(ERROR, "hash function %u returned NULL", info->flinfo.fn_oid);

		hash = hash_combine(hash, DatumGetUInt32(datum));
	}

	return hash;
}

/* like function gather_readnext */
static MinimalTuple
GetTupleFromMQ(RedistributeState *node)
{
	shm_mq_handle  *mq;
	void		   *data;
	Size			nbytes;
	shm_mq_result	mq_result;
	int				nvisited = 0;

	for (;;)
	{
		/* Check for async events, particularly messages from workers. */
		CHECK_FOR_INTERRUPTS();

		Assert(node->nextreader < node->nreaders);
		mq = node->mqreader[node->nextreader];

		mq_result = shm_mq_receive(mq, &nbytes, &data, true);
retest_:
		if (unlikely(mq_result == SHM_MQ_DETACHED))
		{
			shm_mq_detach(mq);
			node->mqreader[node->nextreader] = NULL;
			--(node->nreaders);
			if (node->nreaders == 0)
				return NULL;

			memmove(&node->mqreader[node->nextreader],
					&node->mqreader[node->nextreader + 1],
					sizeof(node->mqreader[0]) * (node->nreaders - node->nextreader));
			if (node->nextreader >= node->nreaders)
				node->nextreader = 0;
		}
		else if (mq_result == SHM_MQ_WOULD_BLOCK)
		{
			/*
			 * When SharedTuplestore ready to scan and MQ sender not attached
			 * then it will no longer attach, and we can assume that the queue has ended
			 */
			if (node->status_flags & STATUS_READY_STS_READ &&
				shm_mq_receiver_get_sender(mq) == NULL)
			{
				mq_result = SHM_MQ_DETACHED;
				goto retest_;
			}

			/*
			 * Advance nextreader pointer in round-robin fashion.  Note that we
			 * only reach this code if we weren't able to get a tuple from the
			 * current worker.  We used to advance the nextreader pointer after
			 * every tuple, but it turns out to be much more efficient to keep
			 * reading from the same queue until that would require blocking.
			 */
			node->nextreader++;
			if (node->nextreader >= node->nreaders)
				node->nextreader = 0;

			/* Have we visited every (surviving) shm_mq_handle? */
			nvisited++;
			if (nvisited >= node->nreaders)
			{
				return NULL;
			}
		}else
		{
			Assert(mq_result == SHM_MQ_SUCCESS);
			Assert(((MinimalTuple)data)->t_len == nbytes);
			return (MinimalTuple)data;
		}
	}
}

static void
PutSlotToQuery(RedistributeWriteInfo *writer, TupleTableSlot *slot,
			   RedistributeInstrumentation *instrument)
{
	MinimalTuple	mtup;
	shm_mq_result	mq_result;
	bool			shouldFree;

	mtup = ExecFetchSlotMinimalTuple(slot, &shouldFree);
	mq_result = shm_mq_send_once(writer->mqwrite, mtup->t_len, mtup);
	switch (mq_result)
	{
	case SHM_MQ_SUCCESS:
		/* nothing todo */
		break;
	case SHM_MQ_WOULD_BLOCK:
		sts_puttuple(writer->sta, NULL, mtup);
		if (instrument)
			++(instrument->disk_rows);
		break;
	case SHM_MQ_DETACHED:
		/* other worker should report an error */
		break;
	}

	if (shouldFree)
		heap_free_minimal_tuple(mtup);
}

static void
ExecRedistributeEndWrite(RedistributeState *node)
{
	RedistributeWriteInfo *writer;
	uint32	i = node->nwriter;

	while (i>0)
	{
		--i;
		writer = &node->writer[i];
		if (writer->sta)
		{
			int64	size;
			REDISTRIBUTE_DEBUG("writer sts_close(%p) part %u", writer->sta, i);
			size = sts_close(writer->sta);
			writer->sta = NULL;
			if (node->instrument)
				node->instrument->disk_used += size;
		}
		if (writer->mqwrite != NULL)
		{
			shm_mq_detach(writer->mqwrite);
			writer->mqwrite = NULL;
		}
	}
}

static TupleTableSlot *
ExecRedistributeReadOnly(PlanState *pstate)
{
	MinimalTuple		mtup;
	RedistributeState  *node = castNode(RedistributeState, pstate);

loop_:
	Assert(node->status_flags & STATUS_LOCALLY_END);

	/* try read tuple from MQ and STS */
	while (node->nreaders > 0)
	{
		/* try read tuple from MQ */
		if ((mtup = GetTupleFromMQ(node)) != NULL)
			return ExecStoreMinimalTuple(mtup, pstate->ps_ResultTupleSlot, false);

		/*
		 * GetTupleFromMQ maybe change nreaders,
		 * so test it again.
		 */
		if (unlikely(node->nreaders == 0))
			break;

		/* test for all attached worker called sts_end_write */
		if ((node->status_flags & STATUS_READY_STS_READ) == 0)
		{
			Barrier *part_barrier = &node->shmem->part_barrier;
			int part_phase = BarrierAttach(part_barrier);
			BarrierDetach(part_barrier);

			if (part_phase >= PRW_END_STS)
			{
				sts_begin_parallel_scan(node->sta);
				node->status_flags |= STATUS_READY_STS_READ;
				SetLatch(MyLatch);
			}
		}

		/* try read tuple from STS */
		if ((node->status_flags & (STATUS_READY_STS_READ|STATUS_QUERY_STS_END)) == STATUS_READY_STS_READ)
		{
			mtup = sts_parallel_scan_next(node->sta, NULL);
			if (mtup != NULL)
				return ExecStoreMinimalTuple(mtup, pstate->ps_ResultTupleSlot, false);
			REDISTRIBUTE_DEBUG("reader sts_close(%p)", node->sta);
			sts_close(node->sta);
			node->sta = NULL;
			node->status_flags |= STATUS_QUERY_STS_END;
		}

		/* wait latch */
		WaitMyLatch(WAIT_EVENT_MQ_RECEIVE);
		ResetLatch(MyLatch);
	}

	if ((node->status_flags & STATUS_READY_STS_READ) == 0)
	{
		/* wait for all worker call sts_end_write */
		Barrier *part_barrier = &node->shmem->part_barrier;
		BarrierAttach(part_barrier);
		while (BarrierPhase(part_barrier) < PRW_END_STS)
			BarrierArriveAndWait(part_barrier, WAIT_EVENT_REDISTRIBUTE_SHARED_TUPLESTORE);
		BarrierDetach(part_barrier);

		sts_begin_parallel_scan(node->sta);
		node->status_flags |= STATUS_READY_STS_READ;
	}

	if ((node->status_flags & STATUS_QUERY_STS_END) == 0)
	{
		mtup = sts_parallel_scan_next(node->sta, NULL);
		if (mtup != NULL)
			return ExecStoreMinimalTuple(mtup, pstate->ps_ResultTupleSlot, false);
		REDISTRIBUTE_DEBUG("reader sts_close(%p)", node->sta);
		sts_close(node->sta);
		node->sta = NULL;
		node->status_flags |= STATUS_QUERY_STS_END;
	}

	/* try read next part */
	if (ExecNextRedistributePart(node))
	{
		ExecInitRedistributeReader(node);
		sts_begin_parallel_scan(node->sta);
		node->status_flags &= ~STATUS_QUERY_STS_END;
		goto loop_;
	}

	return ExecClearTuple(pstate->ps_ResultTupleSlot);
}

/*
 * fetch tuples from outer and other workers
 */
static TupleTableSlot *
ExecRedistributeMixed(PlanState *pstate)
{
	uint32				count_part;
	uint32				hash;
	uint32				mod;
	MinimalTuple		mtup;
	TupleTableSlot	   *slot;
	RedistributeState  *node = castNode(RedistributeState, pstate);

	/* first try read tuple from MQ */
	if (node->nreaders > 0)
	{
		mtup = GetTupleFromMQ(node);
		if (mtup != NULL)
			return ExecStoreMinimalTuple(mtup, pstate->ps_ResultTupleSlot, false);
	}

	count_part = pg_atomic_read_u32(&node->shmem->final_worker_num);
	while ((node->status_flags & STATUS_LOCALLY_END) == 0)
	{
		slot = ExecProcNode(outerPlanState(pstate));
		if (TupIsNull(slot))
		{
			Assert(BarrierPhase(&node->shmem->part_barrier) == PRW_WRITING);
			ExecRedistributeEndWrite(node);
			BarrierArriveAndDetach(&node->shmem->part_barrier);
			node->status_flags |= STATUS_LOCALLY_END;

			break;
		}

		hash = RedistributeGetHash(node, slot);
		Assert(node->current_part != INVALID_PART_NUMBER);
		Assert(node->current_part < count_part);
		mod = hash % count_part;
		if (mod == node->current_part)
			return ExecCopyOuterSlot(pstate->ps_ResultTupleSlot, slot);

		PutSlotToQuery(&node->writer[mod], slot, node->instrument);

		/* try read tuple from MQ again */
		if (node->nreaders > 0 &&
			(mtup = GetTupleFromMQ(node)) != NULL)
		{
			return ExecStoreMinimalTuple(mtup, pstate->ps_ResultTupleSlot, false);
		}
	}

	/* no more tuples from outer, change to only read tuples from other workers */
	ExecSetExecProcNode(pstate, ExecRedistributeReadOnly);
	return ExecRedistributeReadOnly(pstate);
}

/*
 * Now, we know how many parallel work start succcessed
 */
static TupleTableSlot *
ExecRedistributeReady(PlanState *pstate)
{
	RedistributeState  *node = castNode(RedistributeState, pstate);
	RedistributeShmem  *shmem = node->shmem;
	int					part_phase;

	/* attach instrument if exist */
	if (shmem->instrument_shmem_offset > 0)
	{
		int		index = ParallelWorkerNumber + 1;
		Assert(index < shmem->plan_worker_num);
		node->instrument = (RedistributeInstrumentation*)SharedRedistributeOffset(shmem, instrument_shmem_offset);
		node->instrument = &node->instrument[index];
	}

	/* wait leader change final_worker_num */
	part_phase = BarrierAttach(&shmem->part_barrier);
	while (part_phase <= PRW_WAIT_WORKER_NUMBER)
	{
		/* only leader can change final_worker_num */
		BarrierArriveAndWait(&shmem->part_barrier, WAIT_EVENT_REDISTRIBUTE_PARALLEL_START);
		part_phase = BarrierPhase(&shmem->part_barrier);
	}

	/* get next part for read and write */
	if (ExecNextRedistributePart(node) == false)
	{
		Assert(part_phase >= PRW_END_STS);
		BarrierDetach(&shmem->part_barrier);
		return ExecClearTuple(pstate->ps_ResultTupleSlot);
	}

	/* intialize execute and read resource */
	InitializeRedistributeExecute(node);
	ExecInitRedistributeReader(node);

	if (part_phase >= PRW_END_STS)
	{
		/*
		 * all outer's tuple fetched, don't need fetch anymore
		 */
		node->status_flags |= STATUS_LOCALLY_END;
		BarrierDetach(&shmem->part_barrier);
		ExecSetExecProcNode(pstate, ExecRedistributeReadOnly);
	}else
	{
		ExecInitRedistributeWriter(node, node->current_part);
		ExecSetExecProcNode(pstate, ExecRedistributeMixed);
	}

	Assert(part_phase != PRW_WRITING ||
		   node->current_part != INVALID_PART_NUMBER);

	return (*pstate->ExecProcNodeReal)(pstate);
}

RedistributeState *
ExecInitRedistribute(Redistribute *node, EState *estate, int eflags)
{
	RedistributeState *state;

	Assert(outerPlan(node) != NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state struct
	 */
	state = makeNode(RedistributeState);
	state->ps.plan = (Plan*)node;
	state->ps.state = estate;
	/*
	 * ExecRedistributeInitializeDSM and ExecRedistributeInitializeWorker
	 * will change execute function
	 */
	state->ps.ExecProcNode = ExecRedistributeError;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &state->ps);

	/*
	 * initialize child nodes
	 */
	outerPlanState(state) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * tuple table and result type initialization
	 */
	ExecInitResultTupleSlotTL(&state->ps, &TTSOpsMinimalTuple);

	return state;
}

/* release parallel work resource */
static void
ExecEndRedistributeInternal(RedistributeState *node)
{
	uint32	i;

	if (node->writer != NULL &&
		node->shmem != NULL)
		ExecRedistributeEndWrite(node);

	if (node->nreaders > 0)
	{
		for (i=0;i<node->nreaders;++i)
			shm_mq_detach(node->mqreader[i]);
		node->nreaders = node->nextreader = 0;
	}

	if (node->sta != NULL)
	{
		sts_close(node->sta);
		node->sta = NULL;
	}
}

void
ExecEndRedistribute(RedistributeState *node)
{
	ExecEndRedistributeInternal(node);
	ExecEndNode(outerPlanState(node));
}

void
ExecReScanRedistribute(RedistributeState *node)
{
	ExecEndRedistributeInternal(node);
	ExecReScan(outerPlanState(node));
}

/* parallel scan support */
static Size
ComputeRedistributeShmemSize(RedistributeShmem *shmem, int instrument)
{
	Size	size;
	uint32	nworks = shmem->plan_worker_num;
	Assert(nworks > 1);

	/* RedistributeShmem size */
	size = MAXALIGN(offsetof(RedistributeShmem, padding));

	/* SharedTuplestore size */
	Assert(size == MAXALIGN(size));
	shmem->sts_shmem_offset = size;
	shmem->sts_shmem_size = MAXALIGN(sts_estimate(nworks));
	size = add_size(size, mul_size(shmem->sts_shmem_size, nworks));

	/* shm_mq size */
	Assert(redistribute_query_size > 0);
	Assert(size == MAXALIGN(size));
	shmem->mq_shmem_offset = size;
	shmem->mq_shmem_size = ((Size)(redistribute_query_size)) * 1024;
	shmem->mq_shmem_part_size = mul_size(shmem->mq_shmem_size, nworks);
	size = add_size(size, mul_size(shmem->mq_shmem_part_size, nworks));

	/* instrument size if need */
	if (instrument)
	{
		Assert(size == MAXALIGN(size));
		shmem->instrument_shmem_offset = size;
		size = add_size(size, sizeof(RedistributeInstrumentation) * nworks);
	}else
	{
		shmem->instrument_shmem_offset = 0;
	}

	return size;
}

void
ExecRedistributeEstimate(RedistributeState *node, ParallelContext *pcxt)
{
	RedistributeShmem tmp;
	tmp.plan_worker_num = pcxt->nworkers + 1;

	shm_toc_estimate_chunk(&pcxt->estimator,
						   ComputeRedistributeShmemSize(&tmp, node->ps.state->es_instrument));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

static void
InitializeRedistributeSharedMemory(RedistributeShmem *shmem,
								   dsm_segment *seg)
{
	uint32	i,j;
	char	name[MAXPGPATH];
	char   *addr;
	Assert(shmem->plan_worker_num > 1);

	/* base init */
	BarrierInit(&shmem->part_barrier, 0);
	BarrierAttach(&shmem->part_barrier);	/* let other worker wait */
	pg_atomic_init_u32(&shmem->final_worker_num, 0);
	pg_atomic_init_u32(&shmem->next_idle_part, 0);
	if (seg != NULL)	/* not reinitialize */
		SharedFileSetInit(&shmem->fileset, seg);

	/* initialize all SharedTuplestore */
	Assert(shmem->sts_shmem_offset >= offsetof(RedistributeShmem, padding) &&
		   shmem->sts_shmem_size > 0);
	addr = SharedRedistributeOffset(shmem, sts_shmem_offset);
	for (i=0;i<shmem->plan_worker_num;++i)
	{
		AssertPointerAlignment(addr, MAXIMUM_ALIGNOF);
		snprintf(name, sizeof(name), "rd%d", i);
		sts_create((SharedTuplestore*)addr,
				   shmem->plan_worker_num,
				   0,
				   SHARED_TUPLESTORE_SINGLE_PASS,
				   name);
		addr += shmem->sts_shmem_size;
	}

	/* initialize shm_mq */
	Assert(shmem->mq_shmem_offset >= offsetof(RedistributeShmem, padding) &&
		   shmem->mq_shmem_size > 0);
	addr = SharedRedistributeOffset(shmem, mq_shmem_offset);
	for (i=0;i<shmem->plan_worker_num;++i)
	{
		/* initialize message query for each worker */
		for (j=0;j<shmem->plan_worker_num;++j)
		{
			AssertPointerAlignment(addr, MAXIMUM_ALIGNOF);
			shm_mq_create(addr, shmem->mq_shmem_size);
			addr += shmem->mq_shmem_size;
		}
	}

	/* initialize instrument if alloced */
	if (shmem->instrument_shmem_offset > 0)
	{
		Assert(shmem->instrument_shmem_offset >= offsetof(RedistributeShmem, padding));
		addr = SharedRedistributeOffset(shmem, instrument_shmem_offset);
		AssertPointerAlignment(addr, MAXIMUM_ALIGNOF);

		MemSet(addr, 0, sizeof(RedistributeInstrumentation) * shmem->plan_worker_num);
	}
}

void
ExecRedistributeInitializeDSM(RedistributeState *node, ParallelContext *pcxt)
{
	RedistributeShmem tmp;

	tmp.plan_worker_num = pcxt->nworkers + 1;
	node->shmem = shm_toc_allocate(pcxt->toc,
								   ComputeRedistributeShmemSize(&tmp, node->ps.state->es_instrument));
	*node->shmem = tmp;
	shm_toc_insert(pcxt->toc,
				   node->ps.plan->plan_node_id,
				   node->shmem);

	InitializeRedistributeSharedMemory(node->shmem,
									   pcxt->seg);
}

void
ExecRedistributeReInitializeDSM(RedistributeState *node, ParallelContext *pcxt)
{
	RedistributeShmem *shmem = node->shmem;
	if (shmem)
	{
		SharedFileSetDeleteAll(&shmem->fileset);
		InitializeRedistributeSharedMemory(shmem, NULL);
	}
	ExecSetExecProcNode(&node->ps, ExecRedistributeError);
}

/*
 * find shared memory and change execute proc function
 */
void
ExecRedistributeInitializeWorker(RedistributeState *node, ParallelWorkerContext *pwcxt)
{
	node->shmem = shm_toc_lookup(pwcxt->toc,
								 node->ps.plan->plan_node_id,
								 false);
	ExecSetExecProcNode(&node->ps, ExecRedistributeReady);
}

/*
 * Call after all parallel workers startup, make sure got parallel
 * workers startup success count number(ParallelContext::nworkers_launched).
 */
void
ExecRedistributeParallelLaunched(RedistributeState *node, ParallelContext *pcxt)
{
	RedistributeShmem *shmem;
	if (pcxt->nworkers_launched > 0)
	{
		/*
		 * All workers should be waiting leader change final_worker_num,
		 * Leader already attached part_barrier at initialize DSM
		 */
		shmem = node->shmem;
		Assert(BarrierPhase(&shmem->part_barrier) == PRW_WAIT_WORKER_NUMBER);
		pg_atomic_write_u32(&shmem->final_worker_num, pcxt->nworkers_launched + 1);
		BarrierArriveAndDetach(&shmem->part_barrier);

		ExecSetExecProcNode(&node->ps, ExecRedistributeReady);
	}
	else
	{
		/*
		 * have no any parallel work start success,
		 * just return tuples from outer
		 */
		ExecSetExecProcNode(&node->ps, ExecRedistributeDirect);
	}
}

/* ----------------------------------------------------------------
 *		ExecRedistributeRetrieveInstrumentation
 *
 *		Transfer redistribute statistics from DSM to private memory.
 * ----------------------------------------------------------------
 */
void
ExecRedistributeRetrieveInstrumentation(RedistributeState *node)
{
	Size					size;
	SharedRedistributeInfo *info;
	RedistributeShmem	   *shmem = node->shmem;

	if (shmem == NULL ||
		shmem->instrument_shmem_offset == 0)
		return;

	size = shmem->plan_worker_num * sizeof(RedistributeInstrumentation);
	info = palloc(size + offsetof(SharedRedistributeInfo, sinstrument));
	memcpy(&info->sinstrument[0],
		   SharedRedistributeOffset(shmem, instrument_shmem_offset),
		   size);
	info->num_workers = shmem->plan_worker_num;
	node->shared_instrument = info;
}
