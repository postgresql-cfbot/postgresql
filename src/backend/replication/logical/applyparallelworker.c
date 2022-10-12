/*-------------------------------------------------------------------------
 * applyparallelworker.c
 *     Support routines for applying xact by parallel apply worker
 *
 * Copyright (c) 2016-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/applyparallelworker.c
 *
 * This file contains routines that are intended to support setting up, using,
 * and tearing down a ParallelApplyWorkerInfo.
 *
 * Refer to the comments in the file header of logical/worker.c to see more
 * information about parallel apply workers.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "libpq/pqformat.h"
#include "libpq/pqmq.h"
#include "mb/pg_wchar.h"
#include "pgstat.h"
#include "postmaster/interrupt.h"
#include "replication/logicallauncher.h"
#include "replication/logicalworker.h"
#include "replication/origin.h"
#include "replication/walreceiver.h"
#include "replication/worker_internal.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "tcop/tcopprot.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/syscache.h"

#define PG_LOGICAL_APPLY_SHM_MAGIC 0x787ca067

/*
 * DSM keys for parallel apply worker.  Unlike other parallel execution code,
 * since we don't need to worry about DSM keys conflicting with plan_node_id we
 * can use small integers.
 */
#define PARALLEL_APPLY_KEY_SHARED		1
#define PARALLEL_APPLY_KEY_MQ			2
#define PARALLEL_APPLY_KEY_ERROR_QUEUE	3

/* Queue size of DSM, 16 MB for now. */
#define DSM_QUEUE_SIZE	(16 * 1024 * 1024)

/*
 * Error queue size of DSM. It is desirable to make it large enough that a
 * typical ErrorResponse can be sent without blocking.  That way, a worker that
 * errors out can write the whole message into the queue and terminate without
 * waiting for the user backend.
 */
#define DSM_ERROR_QUEUE_SIZE			(16 * 1024)

/*
 * There are three fields in each message received by the parallel apply
 * worker: start_lsn, end_lsn and send_time. Because we have updated these
 * statistics in the leader apply worker, we can ignore these fields in the
 * parallel apply worker (see function LogicalRepApplyLoop).
 */
#define SIZE_STATS_MESSAGE (2 * sizeof(XLogRecPtr) + sizeof(TimestampTz))

/*
 * Hash table entry to map xid to the parallel apply worker state.
 */
typedef struct ParallelApplyWorkerEntry
{
	TransactionId xid;			/* Hash key -- must be first */
	ParallelApplyWorkerInfo *winfo;
} ParallelApplyWorkerEntry;

/* Parallel apply workers hash table (initialized on first use). */
static HTAB *ParallelApplyWorkersHash = NULL;

/*
 * A list to maintain the active parallel apply workers. The information for
 * the new worker is added to the list after successfully launching it. The
 * list entry is removed at the end of the transaction if there are already
 * enough workers in the worker pool. For more information about the worker
 * pool, see comments atop worker.c. We also remove the entry from the list if
 * the worker is exited due to some error.
 */
static List *ParallelApplyWorkersList = NIL;

/*
 * Information shared between leader apply worker and parallel apply worker.
 */
ParallelApplyWorkerShared *MyParallelShared = NULL;

/*
 * Is there a message pending in parallel apply worker which we need to
 * receive?
 */
volatile sig_atomic_t ParallelApplyMessagePending = false;

/*
 * Cache the parallel apply worker information required for applying the
 * current streaming transaction. It is used to save the cost of searching the
 * hash table when applying the changes between STREAM_START and STREAM_STOP.
 */
ParallelApplyWorkerInfo *stream_apply_worker = NULL;

/* A list to maintain subtransactions, if any. */
List	   *subxactlist = NIL;

static bool parallel_apply_can_start(TransactionId xid);
static bool parallel_apply_setup_dsm(ParallelApplyWorkerInfo *winfo);
static ParallelApplyWorkerInfo *parallel_apply_setup_worker(void);
static bool parallel_apply_get_in_xact(ParallelApplyWorkerShared *wshared);
static void parallel_apply_free_worker_info(ParallelApplyWorkerInfo *winfo);

/*
 * Returns true if it is OK to start a parallel apply worker, false otherwise.
 */
static bool
parallel_apply_can_start(TransactionId xid)
{
	if (!TransactionIdIsValid(xid))
		return false;

	/*
	 * Don't start a new parallel apply worker if the subscription is not using
	 * parallel streaming mode, or if the publisher does not support parallel
	 * apply.
	 */
	if (!MyLogicalRepWorker->parallel_apply)
		return false;

	/* Only leader apply workers can start parallel apply workers. */
	if (am_parallel_apply_worker())
		return false;

	/*
	 * Don't start a new parallel worker if user has set skiplsn as it's
	 * possible that user want to skip the streaming transaction. For
	 * streaming transaction, we need to spill the transaction to disk so that
	 * we can get the last LSN of the transaction to judge whether to skip
	 * before starting to apply the change.
	 */
	if (!XLogRecPtrIsInvalid(MySubscription->skiplsn))
		return false;

	/*
	 * Don't use parallel apply workers for retries, because it is possible
	 * that the last time we tried to apply a transaction using a parallel
	 * apply worker the checks failed (see function
	 * parallel_apply_relation_check).
	 */
	if (MySubscription->retry)
	{
		elog(DEBUG1, "parallel apply workers are not used for retries");
		return false;
	}

	/*
	 * For streaming transactions that are being applied using a parallel
	 * apply worker, we cannot decide whether to apply the change for a
	 * relation that is not in the READY state (see
	 * should_apply_changes_for_rel) as we won't know remote_final_lsn by that
	 * time. So, we don't start the new parallel apply worker in this case.
	 */
	if (!AllTablesyncsReady())
		return false;

	return true;
}

/*
 * Start a parallel apply worker that will be used for the specified xid.
 *
 * If a parallel apply worker is found but not in use then re-use it, otherwise
 * start a fresh one. Cache the worker information in ParallelApplyWorkersHash
 * keyed by the specified xid.
 */
void
parallel_apply_start_worker(TransactionId xid)
{
	bool		found;
	ListCell   *lc;
	ParallelApplyWorkerInfo *winfo = NULL;
	ParallelApplyWorkerEntry *entry;

	if (!parallel_apply_can_start(xid))
		return;

	/* First time through, initialize apply workers hashtable. */
	if (ParallelApplyWorkersHash == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(TransactionId);
		ctl.entrysize = sizeof(ParallelApplyWorkerEntry);
		ctl.hcxt = ApplyContext;

		ParallelApplyWorkersHash = hash_create("logical apply workers hash",
											   16, &ctl,
											   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	/* Try to get a free parallel apply worker. */
	foreach(lc, ParallelApplyWorkersList)
	{
		ParallelApplyWorkerInfo *tmp_winfo;

		tmp_winfo = (ParallelApplyWorkerInfo *) lfirst(lc);

		if (tmp_winfo->error_mq_handle == NULL)
		{
			/*
			 * Release the worker information and try next one if the parallel
			 * apply worker exited cleanly.
			 */
			ParallelApplyWorkersList = foreach_delete_current(ParallelApplyWorkersList, lc);
			shm_mq_detach(tmp_winfo->mq_handle);
			dsm_detach(tmp_winfo->dsm_seg);
			pfree(tmp_winfo);
		}
		else if (!tmp_winfo->in_use)
		{
			/* Found a worker that has not been assigned a transaction. */
			winfo = tmp_winfo;
			break;
		}
	}

	if (winfo == NULL)
	{
		/* Try to start a new parallel apply worker. */
		winfo = parallel_apply_setup_worker();

		if (winfo == NULL)
			return;
	}

	/* Create entry for requested transaction. */
	entry = hash_search(ParallelApplyWorkersHash, &xid, HASH_ENTER, &found);
	if (found)
		elog(ERROR, "hash table corrupted");

	/*
	 * Set the in_parallel_apply_xact flag in the leader instead of the
	 * parallel apply worker to avoid the race condition where the leader has
	 * already started waiting for the parallel apply worker to finish
	 * processing the transaction while the child process has not yet
	 * processed the first STREAM_START and has not set the
	 * in_parallel_apply_xact to true.
	 */
	parallel_apply_set_in_xact(winfo->shared, true);

	winfo->in_use = true;
	entry->winfo = winfo;
	entry->xid = xid;
}

/*
 * Find the assigned worker for the given transaction, if any.
 */
ParallelApplyWorkerInfo *
parallel_apply_find_worker(TransactionId xid)
{
	bool		found;
	ParallelApplyWorkerEntry *entry = NULL;

	if (!TransactionIdIsValid(xid))
		return NULL;

	if (ParallelApplyWorkersHash == NULL)
		return NULL;

	/* Return the cached parallel apply worker if valid. */
	if (stream_apply_worker != NULL)
		return stream_apply_worker;

	/*
	 * Find entry for requested transaction.
	 */
	entry = hash_search(ParallelApplyWorkersHash, &xid, HASH_FIND, &found);
	if (found)
	{
		/*
		 * We can't proceed if the parallel streaming worker has already
		 * exited.
		 */
		if (entry->winfo->error_mq_handle == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("lost connection to parallel apply worker")));

		Assert(parallel_apply_get_in_xact(entry->winfo->shared));
		Assert(entry->winfo->in_use);

		return entry->winfo;
	}

	return NULL;
}

/*
 * Remove the parallel apply worker entry from the hash table. And stop the
 * worker if there are enough workers in the pool. For more information about
 * the worker pool, see comments atop worker.c.
 */
void
parallel_apply_free_worker(ParallelApplyWorkerInfo *winfo, TransactionId xid)
{
	int			napplyworkers;

	Assert(!am_parallel_apply_worker());
	Assert(!parallel_apply_get_in_xact(winfo->shared));

	if (!hash_search(ParallelApplyWorkersHash, &xid, HASH_REMOVE, NULL))
		elog(ERROR, "hash table corrupted");

	LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
	napplyworkers = logicalrep_parallel_apply_worker_count(MyLogicalRepWorker->subid);
	LWLockRelease(LogicalRepWorkerLock);

	winfo->in_use = false;

	/* Are there enough workers in the pool? */
	if (napplyworkers > (max_parallel_apply_workers_per_subscription / 2))
	{
		int		slot_no;
		uint16	generation;

		/*
		 * Detach the error queue before terminating the parallel apply worker
		 * to prevent the leader apply worker from receiving the worker
		 * termination message which will cause the leader to exit.
		 *
		 * Although some error messages may be lost in rare scenarios, but
		 * since the parallel apply worker has finished processing the
		 * transaction, and error messages may be lost even if we detach the
		 * error queue after terminating the process. So it should be ok.
		 */
		shm_mq_detach(winfo->error_mq_handle);
		winfo->error_mq_handle = NULL;

		SpinLockAcquire(&winfo->shared->mutex);
		slot_no = winfo->shared->logicalrep_worker_slot_no;
		generation = winfo->shared->logicalrep_worker_generation;
		SpinLockRelease(&winfo->shared->mutex);

		logicalrep_worker_stop_by_slot(slot_no, generation);

		ParallelApplyWorkersList = list_delete_ptr(ParallelApplyWorkersList,
												   winfo);

		parallel_apply_free_worker_info(winfo);
	}
}

/* Free the parallel apply worker information. */
static void
parallel_apply_free_worker_info(ParallelApplyWorkerInfo *winfo)
{
	Assert(winfo);

	if (winfo->mq_handle != NULL)
		shm_mq_detach(winfo->mq_handle);

	if (winfo->error_mq_handle != NULL)
		shm_mq_detach(winfo->error_mq_handle);

	if (winfo->dsm_seg != NULL)
		dsm_detach(winfo->dsm_seg);

	pfree(winfo);
}

/* Parallel apply worker main loop. */
static void
LogicalParallelApplyLoop(shm_mq_handle *mqh)
{
	shm_mq_result shmq_res;
	PGPROC	   *registrant;
	ErrorContextCallback errcallback;

	registrant = BackendPidGetProc(MyBgworkerEntry->bgw_notify_pid);
	SetLatch(&registrant->procLatch);

	/*
	 * Init the ApplyMessageContext which we clean up after each replication
	 * protocol message.
	 */
	ApplyMessageContext = AllocSetContextCreate(ApplyContext,
												"ApplyMessageContext",
												ALLOCSET_DEFAULT_SIZES);

	/*
	 * Push apply error context callback. Fields will be filled while applying
	 * a change.
	 */
	errcallback.callback = apply_error_callback;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	for (;;)
	{
		void	   *data;
		Size		len;
		int			c;
		StringInfoData s;
		MemoryContext oldctx;

		CHECK_FOR_INTERRUPTS();

		/* Ensure we are reading the data into our memory context. */
		oldctx = MemoryContextSwitchTo(ApplyMessageContext);

		shmq_res = shm_mq_receive(mqh, &len, &data, false);

		if (shmq_res != SHM_MQ_SUCCESS)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("lost connection to the leader apply worker")));

		if (len == 0)
			break;

		s.cursor = 0;
		s.maxlen = -1;
		s.data = (char *) data;
		s.len = len;

		/*
		 * The first byte of message for additional communication between
		 * leader apply worker and parallel apply workers can only be 'w'.
		 */
		c = pq_getmsgbyte(&s);
		if (c != 'w')
			elog(ERROR, "unexpected message \"%c\"", c);

		/*
		 * Ignore statistics fields that have been updated by the leader apply
		 * worker.
		 */
		s.cursor += SIZE_STATS_MESSAGE;

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		apply_dispatch(&s);

		MemoryContextSwitchTo(oldctx);
		MemoryContextReset(ApplyMessageContext);
	}

	/* Pop the error context stack. */
	error_context_stack = errcallback.previous;

	/* Signal main process that we are done. */
	SetLatch(&registrant->procLatch);
}

/*
 * Make sure the leader apply worker tries to read from our error queue one more
 * time. This guards against the case where we exit uncleanly without sending
 * an ErrorResponse, for example because some code calls proc_exit directly.
 */
static void
parallel_apply_shutdown(int code, Datum arg)
{
	SendProcSignal(MyLogicalRepWorker->apply_leader_pid,
				   PROCSIG_PARALLEL_APPLY_MESSAGE,
				   InvalidBackendId);

	dsm_detach((dsm_segment *) DatumGetPointer(arg));
}

/*
 * Parallel apply worker entry point.
 */
void
ParallelApplyWorkerMain(Datum main_arg)
{
	ParallelApplyWorkerShared *shared;
	dsm_handle	handle;
	dsm_segment *seg;
	shm_toc    *toc;
	shm_mq	   *mq;
	shm_mq_handle *mqh;
	shm_mq_handle *error_mqh;
	int			worker_slot = DatumGetInt32(main_arg);
	char		originname[NAMEDATALEN];

	/* Setup signal handling. */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/*
	 * Attach to the dynamic shared memory segment for the parallel apply, and
	 * find its table of contents.
	 *
	 * Like parallel query, we don't need resource owner by this time. See
	 * ParallelWorkerMain.
	 */
	memcpy(&handle, MyBgworkerEntry->bgw_extra, sizeof(dsm_handle));
	seg = dsm_attach(handle);
	if (seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("unable to map dynamic shared memory segment")));

	toc = shm_toc_attach(PG_LOGICAL_APPLY_SHM_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bad magic number in dynamic shared memory segment")));

	before_shmem_exit(parallel_apply_shutdown, PointerGetDatum(seg));

	/* Look up the shared information. */
	shared = shm_toc_lookup(toc, PARALLEL_APPLY_KEY_SHARED, false);
	MyParallelShared = shared;

	/*
	 * Attach to the message queue.
	 */
	mq = shm_toc_lookup(toc, PARALLEL_APPLY_KEY_MQ, false);
	shm_mq_set_receiver(mq, MyProc);
	mqh = shm_mq_attach(mq, seg, NULL);

	/*
	 * Primary initialization is complete. Now, we can attach to our slot.
	 * This is to ensure that the leader apply worker does not write data to
	 * the uninitialized memory queue.
	 */
	logicalrep_worker_attach(worker_slot);

	SpinLockAcquire(&MyParallelShared->mutex);
	MyParallelShared->logicalrep_worker_generation = MyLogicalRepWorker->generation;
	MyParallelShared->logicalrep_worker_slot_no = worker_slot;
	SpinLockRelease(&MyParallelShared->mutex);

	/*
	 * Attach to the error queue.
	 */
	mq = shm_toc_lookup(toc, PARALLEL_APPLY_KEY_ERROR_QUEUE, false);
	shm_mq_set_sender(mq, MyProc);
	error_mqh = shm_mq_attach(mq, seg, NULL);

	pq_redirect_to_shm_mq(seg, error_mqh);
	pq_set_parallel_leader(MyLogicalRepWorker->apply_leader_pid,
						   InvalidBackendId);

	MyLogicalRepWorker->last_send_time = MyLogicalRepWorker->last_recv_time =
		MyLogicalRepWorker->reply_time = 0;

	InitializeApplyWorker();

	/*
	 * Setup callback for syscache so that we know when something changes in
	 * the subscription relation state.
	 */
	CacheRegisterSyscacheCallback(SUBSCRIPTIONRELMAP,
								  invalidate_syncing_table_states,
								  (Datum) 0);

	/*
	 * Allocate the origin name in a long-lived context for error context
	 * message.
	 */
	ReplicationOriginNameForLogicalRep(MySubscription->oid, InvalidOid,
									   originname, sizeof(originname));
	apply_error_callback_arg.origin_name = MemoryContextStrdup(ApplyContext,
															   originname);

	LogicalParallelApplyLoop(mqh);

	proc_exit(0);
}

/*
 * Handle receipt of an interrupt indicating a parallel apply worker message.
 *
 * Note: this is called within a signal handler!  All we can do is set a flag
 * that will cause the next CHECK_FOR_INTERRUPTS() to invoke
 * HandleParallelApplyMessages().
 */
void
HandleParallelApplyMessageInterrupt(void)
{
	InterruptPending = true;
	ParallelApplyMessagePending = true;
	SetLatch(MyLatch);
}

/*
 * Handle a single protocol message received from a single parallel apply
 * worker.
 */
static void
HandleParallelApplyMessage(ParallelApplyWorkerInfo *winfo, StringInfo msg)
{
	char		msgtype;

	msgtype = pq_getmsgbyte(msg);

	switch (msgtype)
	{
		case 'E':				/* ErrorResponse */
			{
				ErrorData	edata;
				ErrorContextCallback *save_error_context_stack;

				/* Parse ErrorResponse. */
				pq_parse_errornotice(msg, &edata);

				/* Death of a worker isn't enough justification for suicide. */
				edata.elevel = Min(edata.elevel, ERROR);

				/*
				 * If desired, add a context line to show that this is a
				 * message propagated from a parallel apply worker. Otherwise,
				 * it can sometimes be confusing to understand what actually
				 * happened.
				 */
				if (edata.context)
					edata.context = psprintf("%s\n%s", edata.context,
											 _("parallel apply worker"));
				else
					edata.context = pstrdup(_("parallel apply worker"));

				/*
				 * Context beyond that should use the error context callbacks
				 * that were in effect in LogicalRepApplyLoop().
				 */
				save_error_context_stack = error_context_stack;
				error_context_stack = apply_error_context_stack;

				ThrowErrorData(&edata);

				/* Should not reach here after rethrowing an error. */
				error_context_stack = save_error_context_stack;

				break;
			}

		case 'X':				/* Terminate, indicating clean exit */
			{
				shm_mq_detach(winfo->error_mq_handle);
				winfo->error_mq_handle = NULL;
				break;
			}

			/*
			 * Don't need to do anything about NoticeResponse and
			 * NotifyResponse as the logical replication worker doesn't need
			 * to send messages to the client.
			 */
		case 'N':
		case 'A':
			break;
		default:
			{
				elog(ERROR, "unrecognized message type received from parallel apply worker: %c (message length %d bytes)",
					 msgtype, msg->len);
			}
	}
}

/*
 * Handle any queued protocol messages received from parallel apply workers.
 */
void
HandleParallelApplyMessages(void)
{
	ListCell   *lc;
	MemoryContext oldcontext;

	static MemoryContext hpam_context = NULL;

	/*
	 * This is invoked from ProcessInterrupts(), and since some of the
	 * functions it calls contain CHECK_FOR_INTERRUPTS(), there is a potential
	 * for recursive calls if more signals are received while this runs.  It's
	 * unclear that recursive entry would be safe, and it doesn't seem useful
	 * even if it is safe, so let's block interrupts until done.
	 */
	HOLD_INTERRUPTS();

	/*
	 * Moreover, CurrentMemoryContext might be pointing almost anywhere.  We
	 * don't want to risk leaking data into long-lived contexts, so let's do
	 * our work here in a private context that we can reset on each use.
	 */
	if (hpam_context == NULL)	/* first time through? */
		hpam_context = AllocSetContextCreate(TopMemoryContext,
											"HandleParallelApplyMessages",
											ALLOCSET_DEFAULT_SIZES);
	else
		MemoryContextReset(hpam_context);

	oldcontext = MemoryContextSwitchTo(hpam_context);

	ParallelApplyMessagePending = false;

	foreach(lc, ParallelApplyWorkersList)
	{
		shm_mq_result res;
		Size		nbytes;
		void	   *data;
		ParallelApplyWorkerInfo *winfo = (ParallelApplyWorkerInfo *) lfirst(lc);

		/* Skip if worker has exited */
		if (winfo->error_mq_handle == NULL)
			continue;

		res = shm_mq_receive(winfo->error_mq_handle, &nbytes, &data, true);

		if (res == SHM_MQ_WOULD_BLOCK)
			break;
		else if (res == SHM_MQ_SUCCESS)
		{
			StringInfoData msg;

			initStringInfo(&msg);
			appendBinaryStringInfo(&msg, data, nbytes);
			HandleParallelApplyMessage(winfo, &msg);
			pfree(msg.data);
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("lost connection to the parallel apply worker")));
	}

	MemoryContextSwitchTo(oldcontext);

	/* Might as well clear the context on our way out */
	MemoryContextReset(hpam_context);

	RESUME_INTERRUPTS();
}

/*
 * Set up a dynamic shared memory segment.
 *
 * We set up a control region that contains a fixed-size worker info
 * (ParallelApplyWorkerShared), a message queue, and an error queue.
 *
 * Returns true on success, false on failure.
 */
static bool
parallel_apply_setup_dsm(ParallelApplyWorkerInfo *winfo)
{
	shm_toc_estimator e;
	Size		segsize;
	dsm_segment *seg;
	shm_toc    *toc;
	ParallelApplyWorkerShared *shared;
	shm_mq	   *mq;
	Size		queue_size = DSM_QUEUE_SIZE;
	Size		error_queue_size = DSM_ERROR_QUEUE_SIZE;

	/*
	 * Estimate how much shared memory we need.
	 *
	 * Because the TOC machinery may choose to insert padding of oddly-sized
	 * requests, we must estimate each chunk separately.
	 *
	 * We need one key to register the location of the header, and two other
	 * keys to track the locations of the message queue and the error message
	 * queue.
	 */
	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, sizeof(ParallelApplyWorkerShared));
	shm_toc_estimate_chunk(&e, queue_size);
	shm_toc_estimate_chunk(&e, error_queue_size);

	shm_toc_estimate_keys(&e, 3);
	segsize = shm_toc_estimate(&e);

	/* Create the shared memory segment and establish a table of contents. */
	seg = dsm_create(shm_toc_estimate(&e), 0);
	if (seg == NULL)
		return false;

	toc = shm_toc_create(PG_LOGICAL_APPLY_SHM_MAGIC, dsm_segment_address(seg),
						 segsize);

	/* Set up the header region. */
	shared = shm_toc_allocate(toc, sizeof(ParallelApplyWorkerShared));
	SpinLockInit(&shared->mutex);

	shared->in_parallel_apply_xact = false;

	shm_toc_insert(toc, PARALLEL_APPLY_KEY_SHARED, shared);

	/* Set up message queue for the worker. */
	mq = shm_mq_create(shm_toc_allocate(toc, queue_size), queue_size);
	shm_toc_insert(toc, PARALLEL_APPLY_KEY_MQ, mq);
	shm_mq_set_sender(mq, MyProc);

	/* Attach the queue. */
	winfo->mq_handle = shm_mq_attach(mq, seg, NULL);

	/* Set up error queue for the worker. */
	mq = shm_mq_create(shm_toc_allocate(toc, error_queue_size),
					   error_queue_size);
	shm_toc_insert(toc, PARALLEL_APPLY_KEY_ERROR_QUEUE, mq);
	shm_mq_set_receiver(mq, MyProc);

	/* Attach the queue. */
	winfo->error_mq_handle = shm_mq_attach(mq, seg, NULL);

	/* Return results to caller. */
	winfo->dsm_seg = seg;
	winfo->shared = shared;

	return true;
}

/*
 * Start parallel apply worker process and allocate shared memory for it.
 */
static ParallelApplyWorkerInfo *
parallel_apply_setup_worker(void)
{
	MemoryContext oldcontext;
	bool		launched;
	ParallelApplyWorkerInfo *winfo;

	oldcontext = MemoryContextSwitchTo(ApplyContext);

	winfo = (ParallelApplyWorkerInfo *) palloc0(sizeof(ParallelApplyWorkerInfo));

	/* Setup shared memory. */
	if (!parallel_apply_setup_dsm(winfo))
	{
		MemoryContextSwitchTo(oldcontext);
		pfree(winfo);

		return NULL;
	}

	launched = logicalrep_worker_launch(MyLogicalRepWorker->dbid,
										MySubscription->oid,
										MySubscription->name,
										MyLogicalRepWorker->userid,
										InvalidOid,
										dsm_segment_handle(winfo->dsm_seg));

	if (launched)
	{
		ParallelApplyWorkersList = lappend(ParallelApplyWorkersList, winfo);
	}
	else
	{
		parallel_apply_free_worker_info(winfo);

		winfo = NULL;
	}

	MemoryContextSwitchTo(oldcontext);

	return winfo;
}

/*
 * Send the data to the specified parallel apply worker via shared-memory queue.
 */
void
parallel_apply_send_data(ParallelApplyWorkerInfo *winfo, Size nbytes,
						 const void *data)
{
	shm_mq_result result;

	result = shm_mq_send(winfo->mq_handle, nbytes, data, false, true);

	if (result != SHM_MQ_SUCCESS)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not send data to shared-memory queue")));
}

/*
 * Wait until the parallel apply worker processed the transaction finish command.
 */
void
parallel_apply_wait_for_xact_finish(ParallelApplyWorkerInfo *winfo)
{
	for (;;)
	{
		/*
		 * Stop if the parallel apply worker has processed the transaction
		 * finish command.
		 */
		if (!parallel_apply_get_in_xact(winfo->shared))
			break;

		/* If any workers have died, we have failed. */
		if (winfo->error_mq_handle == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("lost connection to parallel apply worker")));

		/* Wait to be signalled. */
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 1000L,
						 WAIT_EVENT_LOGICAL_PARALLEL_APPLY_STATE_CHANGE);

		/* Reset the latch so we don't spin. */
		ResetLatch(MyLatch);

		/* An interrupt may have occurred while we were waiting. */
		CHECK_FOR_INTERRUPTS();
	}
}

/*
 * Set the in_parallel_apply_xact flag for the given parallel apply worker.
 */
void
parallel_apply_set_in_xact(ParallelApplyWorkerShared *wshared,
						   bool in_xact)
{
	SpinLockAcquire(&wshared->mutex);
	wshared->in_parallel_apply_xact = in_xact;
	SpinLockRelease(&wshared->mutex);
}

/*
 * Get the in_parallel_apply_xact flag for the given parallel apply worker.
 */
static bool
parallel_apply_get_in_xact(ParallelApplyWorkerShared *wshared)
{
	bool		in_xact;

	SpinLockAcquire(&wshared->mutex);
	in_xact = wshared->in_parallel_apply_xact;
	SpinLockRelease(&wshared->mutex);

	return in_xact;
}

/*
 * Form the savepoint name for the streaming transaction.
 *
 * Return the name in the supplied buffer.
 */
static void
parallel_apply_savepoint_name(Oid suboid, TransactionId xid,
							  char *spname, Size szsp)
{
	snprintf(spname, szsp, "pg_sp_%u_%u", suboid, xid);
}

/*
 * Define a savepoint for a subxact in parallel apply worker if needed.
 *
 * The parallel apply worker can figure out if a new subtransaction was
 * started by checking if the new change arrived with a different xid. In that
 * case define a named savepoint, so that we are able to rollback to it
 * if required.
 */
void
parallel_apply_start_subtrans(TransactionId current_xid, TransactionId top_xid)
{
	if (current_xid != top_xid &&
		!list_member_xid(subxactlist, current_xid))
	{
		MemoryContext oldctx;
		char		spname[NAMEDATALEN];

		parallel_apply_savepoint_name(MySubscription->oid, current_xid,
									  spname, sizeof(spname));

		elog(DEBUG1, "defining savepoint %s in parallel apply worker", spname);

		/* We must be in transaction block to define the SAVEPOINT. */
		if (!IsTransactionBlock())
		{
			BeginTransactionBlock();
			CommitTransactionCommand();
		}

		DefineSavepoint(spname);

		/*
		 * CommitTransactionCommand is needed to start a subtransaction after
		 * issuing a SAVEPOINT inside a transaction block (see
		 * StartSubTransaction()).
		 */
		CommitTransactionCommand();

		oldctx = MemoryContextSwitchTo(ApplyContext);
		subxactlist = lappend_xid(subxactlist, current_xid);
		MemoryContextSwitchTo(oldctx);
	}
}

/*
 * Handle STREAM ABORT message when the transaction was applied in a parallel
 * apply worker.
 */
void
parallel_apply_stream_abort(LogicalRepStreamAbortData *abort_data)
{
	TransactionId xid = abort_data->xid;
	TransactionId subxid = abort_data->subxid;

	/*
	 * Update origin state so we can restart streaming from correct position
	 * in case of crash.
	 */
	replorigin_session_origin_lsn = abort_data->abort_lsn;
	replorigin_session_origin_timestamp = abort_data->abort_time;

	/*
	 * If the two XIDs are the same, it's in fact abort of toplevel xact, so
	 * just free the subxactlist.
	 */
	if (subxid == xid)
	{
		parallel_apply_replorigin_setup();

		AbortCurrentTransaction();

		if (IsTransactionBlock())
		{
			EndTransactionBlock(false);
			CommitTransactionCommand();
		}

		parallel_apply_replorigin_reset();

		pgstat_report_activity(STATE_IDLE, NULL);

		list_free(subxactlist);
		subxactlist = NIL;
	}
	else
	{
		/*
		 * OK, so it's a subxact. Rollback to the savepoint.
		 *
		 * We also need to read the subxactlist, determine the offset tracked
		 * for the subxact, and truncate the list.
		 */
		int			i;
		bool		found = false;
		char		spname[MAXPGPATH];

		parallel_apply_savepoint_name(MySubscription->oid, subxid, spname,
									  sizeof(spname));

		elog(DEBUG1, "rolling back to savepoint %s in parallel apply worker", spname);

		for (i = list_length(subxactlist) - 1; i >= 0; i--)
		{
			TransactionId xid_tmp = lfirst_xid(list_nth_cell(subxactlist, i));

			if (xid_tmp == subxid)
			{
				found = true;
				break;
			}
		}

		if (found)
		{
			RollbackToSavepoint(spname);
			CommitTransactionCommand();
			subxactlist = list_truncate(subxactlist, i + 1);
		}

		pgstat_report_activity(STATE_IDLEINTRANSACTION, NULL);
	}

	parallel_apply_set_in_xact(MyParallelShared, false);
}

/* Setup replication origin tracking. */
void
parallel_apply_replorigin_setup(void)
{
	RepOriginId originid;
	char		originname[NAMEDATALEN];
	bool		started_tx = false;

	/* This function might be called inside or outside of transaction. */
	if (!IsTransactionState())
	{
		StartTransactionCommand();
		started_tx = true;
	}

	ReplicationOriginNameForLogicalRep(MySubscription->oid, InvalidOid,
									   originname, sizeof(originname));
	originid = replorigin_by_name(originname, false);
	replorigin_session_setup(originid);
	replorigin_session_origin = originid;

	if (started_tx)
		CommitTransactionCommand();
}

/* Reset replication origin tracking. */
void
parallel_apply_replorigin_reset(void)
{
	replorigin_session_reset();

	replorigin_session_origin = InvalidRepOriginId;
	replorigin_session_origin_lsn = InvalidXLogRecPtr;
	replorigin_session_origin_timestamp = 0;
}

/*
 * Check if changes on this relation can be applied using a parallel apply
 * worker.
 *
 * Although the commit order is maintained by only allowing one process to
 * commit at a time, the access order to the relation has changed. This could
 * cause unexpected problems when applying transactions using a parallel
 * apply worker if the unique column on the replicated table is
 * inconsistent with the publisher-side, or if the relation contains
 * non-immutable functions.
 */
void
parallel_apply_relation_check(LogicalRepRelMapEntry *rel)
{
	/* Skip if not the parallel apply worker */
	if (!am_parallel_apply_worker())
		return;

	/*
	 * Partition table checks are done later in function
	 * apply_handle_tuple_routing.
	 */
	if (rel->localrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		return;

	if (rel->parallel_apply_safety == PARALLEL_APPLY_SAFETY_UNKNOWN)
		logicalrep_rel_mark_parallel_apply(rel);

	if (rel->parallel_apply_safety == PARALLEL_APPLY_UNSAFE)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot replicate target relation \"%s.%s\" using "
						"subscription parameter %s",
						rel->remoterel.nspname, rel->remoterel.relname,
						"streaming = parallel"),
				 errdetail("The unique column on subscriber is not the unique "
						   "column on publisher or there is at least one "
						   "non-immutable function.")));
}
