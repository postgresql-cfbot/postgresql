//
// Created by John Morris on 8/18/23.
//


#include "postgres.h"
#include "storage/s_lock.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/shm_toc.h"
#include "tcop/tcopprot.h"
#include "storage/spin.h"
#include "storage/procarray.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "worker_pool.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "utils/memtrack.h"  /* for debug() */
#include "utils/wait_event_types.h"
#include "storage/barrier.h"


#define WORKER_POOL_MAGIC 0x7843732

typedef struct WorkerPoolStartup WorkerPoolStartup;


struct WorkerPool
{
	dsm_segment *seg;
	shm_toc     *toc;
	int         nWorkers;
	MemQue      *inQ;
	MemQue      *outQ;
	BackgroundWorkerHandle **handle;
	WorkerPoolStartup *hdr;
};


/*
 * This structure is stored in the dynamic shared memory segment at toc(0).
 * We use it to assign worker numbers to each worker.
 */
struct WorkerPoolStartup
{
	uint32           nWorkers;
	pg_atomic_uint32 nextWorker;
	Barrier          barrier[1];
};

/* Forward references */
static int64 estimateDsmSize(int nWorkers, int inSize, int outSize);
static void cleanupWorkers(dsm_segment *seg, Datum arg);
static MemQue attachToQueue(dsm_segment *seg, shm_toc *toc, int workerIdx, int queueIdx, bool isSender);

/*
 * Create a new pool of backend workers.
 */
WorkerPool *createWorkerPool(int nWorkers, int inSize, int outSize, char *libName, char *procName)
{
	WorkerPool *pool;
	int64  dsmSize;
	MemoryContext oldcontext;
	BackgroundWorker worker;
	pid_t pid;

	debug("createWorkerPool: nWorkers=%d, inSize=%d, outSize=%d\n", nWorkers, inSize, outSize);

	/*
	 * We need the worker pool objects to allocated in CurTransactionContext
	 * rather than ExprContext; otherwise, they'll be destroyed before the on_dsm_detach
	 * hooks run.
	 */
	oldcontext = MemoryContextSwitchTo(CurTransactionContext);

	/* Create local worker pool object and allocate arrays for the headers and queues. */
	pool = MemoryContextAlloc(TopTransactionContext, sizeof(WorkerPool));
	pool->nWorkers = nWorkers;
	pool->inQ = MemoryContextAlloc(TopTransactionContext, sizeof(MemQue) * nWorkers);
	pool->outQ = MemoryContextAlloc(TopTransactionContext, sizeof(MemQue) * nWorkers);
	pool->handle = MemoryContextAlloc(TopTransactionContext, sizeof(BackgroundWorkerHandle *) * nWorkers);

	/* Estimate the size of the shared memory and allocate shared memory */
	dsmSize = estimateDsmSize(nWorkers, inSize, outSize);
	pool->seg = dsm_create(dsmSize, 0);

	/* Create table of contents in dsm so we can access contents */
	pool->toc = shm_toc_create(WORKER_POOL_MAGIC, dsm_segment_address(pool->seg), dsmSize);

	/* Set up the startup header as region 0 */
	pool->hdr = shm_toc_allocate(pool->toc, sizeof(WorkerPoolStartup));
	shm_toc_insert(pool->toc, 0, pool->hdr);
	pool->hdr->nWorkers = nWorkers;
	pg_atomic_init_u32(&pool->hdr->nextWorker, 0);
	BarrierInit(pool->hdr->barrier, nWorkers+1);

	/* Create memory queues for each worker */
	for (int w = 0; w < nWorkers; w++)
	{
		shm_mq *mqIn, *mqOut;

		/* Allocate the "In" queue */
		mqIn = shm_toc_allocate(pool->toc, shm_mq_minimum_size + inSize);
		shm_toc_insert(pool->toc, 1 + 2 * w, mqIn);
		mqIn = shm_mq_create(mqIn, shm_mq_minimum_size + inSize);
		shm_mq_set_sender(mqIn, MyProc);

		/* Allocate the "Out" queue */
		mqOut = shm_toc_allocate(pool->toc, shm_mq_minimum_size + outSize);
		shm_toc_insert(pool->toc, 2 + 2 * w, mqOut);
		mqOut = shm_mq_create(mqOut, shm_mq_minimum_size + outSize);
		shm_mq_set_receiver(mqOut, MyProc);
	}

	/*
	 * Arrange to kill all the workers if we abort before all workers are
	 * finished hooking themselves up to the dynamic shared memory segment.
	 *
	 * If we die after all the workers have finished hooking themselves up to
	 * the dynamic shared memory segment, we'll mark the two queues to which
	 * we're directly connected as detached, and the worker(s) connected to
	 * those queues will exit, marking any other queues to which they are
	 * connected as detached.  This will cause any as-yet-unaware workers
	 * connected to those queues to exit in their turn, and so on, until
	 * everybody exits.
	 *
	 * But suppose the workers which are supposed to connect to the queues to
	 * which we're directly attached exit due to some error before theynin
	 * actually attach the queues.  The remaining workers will have no way of
	 * knowing this.  From their perspective, they're still waiting for those
	 * workers to start, when in fact they've already died.
	 */
	debug("configuring on_detach:  seg=%p  fn=%p  pool=%ld\n", pool->seg, cleanupWorkers, PointerGetDatum(pool));
	on_dsm_detach(pool->seg, cleanupWorkers, PointerGetDatum(pool));


	/* Configure a prototypical worker. */
	worker = (BackgroundWorker)
	{
		.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_CLASS_PARALLEL,
		.bgw_start_time = BgWorkerStart_ConsistentState,
		.bgw_restart_time = BGW_NEVER_RESTART,
		.bgw_notify_pid = MyProcPid,
		.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(pool->seg)),
	};
	strlcpy(worker.bgw_library_name, libName, sizeof(worker.bgw_library_name));
	strlcpy(worker.bgw_function_name, procName, sizeof(worker.bgw_function_name));
	snprintf(worker.bgw_type, sizeof(worker.bgw_type), "%s worker", libName);
	snprintf(worker.bgw_name, sizeof(worker.bgw_name), "%s/%s worker for [%d]", libName, procName, MyProcPid);
	debug("lib=%s proc=%s .bgw_main_arg=%d\n", libName, procName, DatumGetInt32(worker.bgw_main_arg));

	/* Do for each worker */
	for (int w = 0; w < nWorkers; w++)
	{
		/* Create the worker */
		if (!RegisterDynamicBackgroundWorker(&worker, &pool->handle[w]))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						errmsg("could not register background process"),
						errhint("You may need to increase max_worker_processes.")));

		/* Attach the worker's memory queues */
		pool->inQ[w] = attachToQueue(pool->seg, pool->toc, w, 0, true);
		pool->outQ[w] = attachToQueue(pool->seg, pool->toc, w, 1, false);
	}

	/*
	 * Wait for workers to become ready.
	 * We could just wait on the barrier, but if a worker fails to reach
	 * the barrier, we would end up waiting forever.
	 */
	for (int w = 0; w < nWorkers; w++)
		if (WaitForBackgroundWorkerStartup(pool->handle[w], &pid) != BGWH_STARTED)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						errmsg("could not start background process"),
						errhint("You may need to increase max_worker_processes.")));

	/* Wait for workers to attach to the shared memory segment */
	debug("Leader waiting on barrier\n");
	BarrierArriveAndWait(pool->hdr->barrier, 0);

	/*
	 * Once we reach this point, all workers are ready.  We no longer need to
	 * kill them if we die; they'll die on their own as the message queues
	 * shut down.  For now, kill workers on detach anyway, just to be safe.
	 */
	//cancel_on_dsm_detach(pool->seg, cleanupWorkers, PointerGetDatum(pool));

	/* Resume using the original memory context */
	MemoryContextSwitchTo(oldcontext);

	return pool;
}


shm_mq_result sendToWorker(WorkerPool *pool, int workerIdx, void *msg, Size len)
{
	shm_mq_result result;
	debug("workerIdx=%d, len=%zd\n", workerIdx, len);

	result = shm_mq_send(pool->inQ[workerIdx], len, msg, true, true);

	return result;
}

shm_mq_result recvFromWorker(WorkerPool *pool, int workerIdx, void**msg, Size *len)
{
	shm_mq_result result;

	debug("waiting workerIdx=%d\n", workerIdx);
	result = shm_mq_receive(pool->outQ[workerIdx], len, msg, false);
	debug("received result=%d workerIdx=%d, len=%zd\n", result, workerIdx, *len);

	return result;
}

void freeWorkerPool(WorkerPool *pool)
{
	/* Only free the pool once. (possibly reentrant) */
    dsm_segment *seg = pool->seg;
	pool->seg = NULL;
	debug("pool=%p seg=%p\n", pool, seg);
	if (seg == NULL)
		return;

	/* Terminate the background workers */
	cleanupWorkers(pool->seg, PointerGetDatum(pool));

	/* Detach from the message queues */
	for (int w = 0; w < pool->nWorkers; w++)
	{
		shm_mq_detach(pool->inQ[w]);
		shm_mq_detach(pool->outQ[w]);
	}

    /* Detach and destroy the shared memory segment (if we haven't already) */
	dsm_detach(seg);

	/* Free the pool object */
	pfree(pool->inQ);
	pfree(pool->outQ);
	pfree(pool->handle);
	pfree(pool);
}


/* Wrapper to remove workers when detaching from dsm */
static void cleanupWorkers(dsm_segment *seg, Datum arg)
{
	WorkerPool *pool = (WorkerPool *)DatumGetPointer(arg);
	debug("seg=%p pool=%p\n", seg, pool);
	for (int w = 0; w < pool->nWorkers; w++)
		TerminateBackgroundWorker(pool->handle[w]);
	for (int w = 0; w < pool->nWorkers; w++)
		WaitForBackgroundWorkerShutdown(pool->handle[w]);
}


/*
 * Estimate how much shared memory we need for thw pool of workers.
 *
 * Because the TOC machinery may choose to insert padding of oddly-sized
 * requests, we must estimate each chunk separately.
 *
 * We need one key to register the location of the header, and we need
 * nworkers * 2 keys to track the locations of the message queues.
 */
static
int64 estimateDsmSize(int nWorkers, int inSize, int outSize)
{
	shm_toc_estimator e[1];

	shm_toc_initialize_estimator(e);

	shm_toc_estimate_keys(e, 1 + 2 * nWorkers);

	shm_toc_estimate_chunk(e, sizeof(WorkerPoolStartup));

	for (int w = 0; w < nWorkers; w++)
	{
		shm_toc_estimate_chunk(e, shm_mq_minimum_size + inSize);
		shm_toc_estimate_chunk(e, shm_mq_minimum_size + outSize);
	}

	return shm_toc_estimate(e);
}



/*
 * ----------------------------------------------------------------
 *	Worker process code.
 * ----------------------------------------------------------------
 */

static dsm_segment *seg;
static shm_toc *toc;

static int myWorkerNumber;
static MemQue inQ;
static MemQue outQ;
static WorkerPoolStartup *hdr;

shm_mq_result  workerRecv(void **msg, Size *msgSize)
{
	debug("MyWorkerNumber=%d\n", myWorkerNumber);
	return shm_mq_receive(inQ, msgSize, msg, false);
}

shm_mq_result  workerSend(void *msg, Size msgSize)
{
	debug("MyWorkerNumber=%d msgSize=%zd\n", myWorkerNumber, msgSize);
	return shm_mq_send(outQ, msgSize, msg, false, true);
}


/*
 * This must be called from the worker process's main function.
 */
void workerInit(Datum arg)
{
	dsm_handle handle;

	/* We are passed the dsm handle of the worker pool */
	handle = DatumGetInt32(arg);
	debug("handle=%d\n", handle);

	/*
	* Establish signal handlers.
	*
	* We want CHECK_FOR_INTERRUPTS() to kill off this worker process just as
	* it would a normal user backend.  To make that happen, we use die().
	*/
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/*
	 * Connect to the dynamic shared memory segment.
	 *
	 * The backend that registered this worker passed us the ID of a shared
	 * memory segment to which we must attach for further instructions.  Once
	 * we've mapped the segment in our address space, attach to the table of
	 * contents so we can locate the various data structures we'll need to
	 * find within the segment.
	 *
	 * Note: at this point, we have not created any ResourceOwner in this
	 * process.  This will result in our DSM mapping surviving until process
	 * exit, which is fine.  If there were a ResourceOwner, it would acquire
	 * ownership of the mapping, but we have no need for that.
	 */
	seg = dsm_attach(handle);
	if (seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("unable to map dynamic shared memory segment")));
	toc = shm_toc_attach(WORKER_POOL_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("bad magic number in dynamic shared memory segment")));

	/* Attach to the startup header and get our worker idx */
	hdr = shm_toc_lookup(toc, 0, false);
	myWorkerNumber = pg_atomic_fetch_add_u32(&hdr->nextWorker, 1);
	debug("myWorkerNumber=%d\n", myWorkerNumber);
	if (myWorkerNumber >= hdr->nWorkers)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("too many workers created in pool. Max=%d", hdr->nWorkers)));


    /* Attach to the in and out message queues. */
	inQ = attachToQueue(seg,  toc, myWorkerNumber, 0, false);
	outQ = attachToQueue(seg, toc, myWorkerNumber, 1, true);

	/* Wait for everybody else to become ready */
	debug("Waiting on barrier\n");
	BarrierArriveAndWait(hdr->barrier, 0);
	debug("Barrier passed\n");
}


void workerExit(int code)
{
	debug("code=%d\n", code);
	/*
    * We're done.  For cleanliness, explicitly detach from the shared memory
    * segment (that would happen anyway during process exit, though).
    */
	//dsm_detach(seg);
	proc_exit(code);
}


static MemQue attachToQueue(dsm_segment *seg, shm_toc *toc, int workerIdx, int queueIdx, bool isSender)
{
	MemQue que;
	shm_mq *mq;

	/* Attach to the appropriate message queues. */
	mq = shm_toc_lookup(toc, 1 + 2 * workerIdx + queueIdx, false);

	/* Make note whether we are sending or receiving */
	if (isSender)
		shm_mq_set_sender(mq, MyProc);
	else
		shm_mq_set_receiver(mq, MyProc);

	/* Attach to the queue */
	que = shm_mq_attach(mq, seg, NULL);

	debug("workerIdx=%d, queueIdx=%d  handle=%p\n", workerIdx, queueIdx, que);
	return que;
}
