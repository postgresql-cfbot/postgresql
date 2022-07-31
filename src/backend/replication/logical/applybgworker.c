/*-------------------------------------------------------------------------
 * applybgworker.c
 *     Support routines for applying xact by apply background worker
 *
 * Copyright (c) 2016-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/applybgworker.c
 *
 * This file contains routines that are intended to support setting up, using,
 * and tearing down a ApplyBgworkerState.
 *
 * Refer to the comments in file header of logical/worker.c to see more
 * information about apply background worker.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "libpq/pqformat.h"
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

#define PG_LOGICAL_APPLY_SHM_MAGIC 0x79fb2447 // TODO Consider change

/*
 * DSM keys for apply background worker.  Unlike other parallel execution code,
 * since we don't need to worry about DSM keys conflicting with plan_node_id we
 * can use small integers.
 */
#define APPLY_BGWORKER_KEY_SHARED	1
#define APPLY_BGWORKER_KEY_MQ		2

/* Queue size of DSM, 16 MB for now. */
#define DSM_QUEUE_SIZE	160000000

/*
 * There are three fields in message: start_lsn, end_lsn and send_time. Because
 * we have updated these statistics in apply worker, we could ignore these
 * fields in apply background worker. (see function LogicalRepApplyLoop)
 */
#define IGNORE_SIZE_IN_MESSAGE (3 * sizeof(uint64))

/*
 * Entry for a hash table we use to map from xid to our apply background worker
 * state.
 */
typedef struct ApplyBgworkerEntry
{
	TransactionId xid;
	ApplyBgworkerState *wstate;
} ApplyBgworkerEntry;

/* Apply background workers hash table (initialized on first use) */
static HTAB *ApplyWorkersHash = NULL;
static List *ApplyWorkersFreeList = NIL;
static List *ApplyWorkersList = NIL;

/*
 * Information shared between main apply worker and apply background worker.
 */
volatile ApplyBgworkerShared *MyParallelShared = NULL;

List	   *subxactlist = NIL;

static bool apply_bgworker_can_start(TransactionId xid);
static ApplyBgworkerState *apply_bgworker_setup(void);
static void apply_bgworker_setup_dsm(ApplyBgworkerState *wstate);

/*
 * Check if starting a new apply background worker is allowed.
 */
static bool
apply_bgworker_can_start(TransactionId xid)
{
	if (!TransactionIdIsValid(xid))
		return false;

	/*
	 * Don't start a new background worker if not in streaming parallel mode.
	 */
	if (MySubscription->stream != SUBSTREAM_PARALLEL)
		return false;

	/*
	 * Don't start a new background worker if user has set skiplsn as it's
	 * possible that user want to skip the streaming transaction. For
	 * streaming transaction, we need to spill the transaction to disk so that
	 * we can get the last LSN of the transaction to judge whether to skip
	 * before starting to apply the change.
	 */
	if (!XLogRecPtrIsInvalid(MySubscription->skiplsn))
		return false;

	/*
	 * Don't use apply background workers for retries, because it is possible
	 * that the last time we tried to apply a transaction using an apply
	 * background worker the checks failed (see function
	 * apply_bgworker_relation_check).
	 */
	if (MySubscription->retry)
	{
		elog(DEBUG1, "apply background workers are not used for retries");
		return false;
	}

	/*
	 * For streaming transactions that are being applied in apply background
	 * worker, we cannot decide whether to apply the change for a relation
	 * that is not in the READY state (see should_apply_changes_for_rel) as we
	 * won't know remote_final_lsn by that time. So, we don't start new apply
	 * background worker in this case.
	 */
	if (!AllTablesyncsReady())
		return false;

	return true;
}

/*
 * Try to start an apply background worker and, if successful, cache it in
 * ApplyWorkersHash keyed by the specified xid.
 */
ApplyBgworkerState *
apply_bgworker_start(TransactionId xid)
{
	bool		found;
	int			server_version;
	ApplyBgworkerState *wstate;
	ApplyBgworkerEntry *entry = NULL;

	if (!apply_bgworker_can_start(xid))
		return NULL;

	/* First time through, initialize apply workers hashtable */
	if (ApplyWorkersHash == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(TransactionId);
		ctl.entrysize = sizeof(ApplyBgworkerEntry);
		ctl.hcxt = ApplyContext;

		ApplyWorkersHash = hash_create("logical apply workers hash", 8, &ctl,
									   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	/*
	 * Now, we try to get an apply background worker. If there is at least one
	 * worker in the free list, then take one. Otherwise, we try to start a
	 * new apply background worker.
	 */
	if (list_length(ApplyWorkersFreeList) > 0)
	{
		wstate = (ApplyBgworkerState *) llast(ApplyWorkersFreeList);
		ApplyWorkersFreeList = list_delete_last(ApplyWorkersFreeList);
		Assert(wstate->shared->status == APPLY_BGWORKER_FINISHED);
	}
	else
	{
		wstate = apply_bgworker_setup();

		if (wstate == NULL)
			return NULL;
	}

	/*
	 * Create entry for requested transaction.
	 */
	entry = hash_search(ApplyWorkersHash, &xid, HASH_ENTER, &found);
	if (found)
		elog(ERROR, "hash table corrupted");

	/* Fill up the hash entry */
	wstate->shared->status = APPLY_BGWORKER_BUSY;

	server_version = walrcv_server_version(LogRepWorkerWalRcvConn);
	wstate->shared->server_version =
		server_version >= 160000 ? LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM :
		server_version >= 150000 ? LOGICALREP_PROTO_TWOPHASE_VERSION_NUM :
		server_version >= 140000 ? LOGICALREP_PROTO_STREAM_VERSION_NUM :
		LOGICALREP_PROTO_VERSION_NUM;

	wstate->shared->stream_xid = xid;
	entry->wstate = wstate;
	entry->xid = xid;

	return wstate;
}

/*
 * Try to look up worker inside ApplyWorkersHash for requested xid.
 */
ApplyBgworkerState *
apply_bgworker_find(TransactionId xid)
{
	bool		found;
	ApplyBgworkerEntry *entry = NULL;

	if (!TransactionIdIsValid(xid))
		return NULL;

	if (ApplyWorkersHash == NULL)
		return NULL;

	/*
	 * Find entry for requested transaction.
	 */
	entry = hash_search(ApplyWorkersHash, &xid, HASH_FIND, &found);
	if (found)
	{
		char status = entry->wstate->shared->status;

		/* If any workers (or the postmaster) have died, we have failed. */
		if (status == APPLY_BGWORKER_EXIT)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("background worker %u failed to apply transaction %u",
							entry->wstate->shared->n,
							entry->wstate->shared->stream_xid)));

		Assert(status == APPLY_BGWORKER_BUSY);

		return entry->wstate;
	}
	else
		return NULL;
}

/*
 * Add the worker to the free list and remove the entry from the hash table.
 */
void
apply_bgworker_free(ApplyBgworkerState *wstate)
{
	MemoryContext oldctx;
	TransactionId xid = wstate->shared->stream_xid;

	Assert(wstate->shared->status == APPLY_BGWORKER_FINISHED);

	oldctx = MemoryContextSwitchTo(ApplyContext);

	hash_search(ApplyWorkersHash, &xid, HASH_REMOVE, NULL);

	elog(DEBUG1, "adding finished apply worker #%u for xid %u to the free list",
		 wstate->shared->n, wstate->shared->stream_xid);

	ApplyWorkersFreeList = lappend(ApplyWorkersFreeList, wstate);

	MemoryContextSwitchTo(oldctx);
}

/* Apply Background Worker main loop */
static void
LogicalApplyBgwLoop(shm_mq_handle *mqh, volatile ApplyBgworkerShared *shared)
{
	shm_mq_result shmq_res;
	PGPROC	   *registrant;
	ErrorContextCallback errcallback;

	registrant = BackendPidGetProc(MyBgworkerEntry->bgw_notify_pid);
	SetLatch(&registrant->procLatch);

	/*
	 * Push apply error context callback. Fields will be filled applying a
	 * change.
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
					 errmsg("lost connection to the main apply worker")));

		if (len == 0)
			break;

		s.cursor = 0;
		s.maxlen = -1;
		s.data = (char *) data;
		s.len = len;

		/*
		 * We use first byte of message for additional communication between
		 * main Logical replication worker and apply background workers, so if
		 * it differs from 'w', then process it first.
		 */
		c = pq_getmsgbyte(&s);
		switch (c)
		{
			/* End message of streaming chunk */
			case LOGICAL_REP_MSG_STREAM_STOP:
				elog(DEBUG1, "[Apply BGW #%u] ended processing streaming chunk,"
					 "waiting on shm_mq_receive", shared->n);

				in_streamed_transaction = false;
				pgstat_report_activity(STATE_IDLEINTRANSACTION, NULL);
				continue;
			case 'w':
				break;
			default:
				elog(ERROR, "[Apply BGW #%u] unexpected message \"%c\"",
					 shared->n, c);
				break;
		}

		/* Ignore statistics fields that have been updated. */
		s.cursor += IGNORE_SIZE_IN_MESSAGE;

		apply_dispatch(&s);

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		MemoryContextSwitchTo(oldctx);
		MemoryContextReset(ApplyMessageContext);
	}

	MemoryContextSwitchTo(TopMemoryContext);
	MemoryContextReset(ApplyContext);

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;

	elog(DEBUG1, "[Apply BGW #%u] exiting", shared->n);

	/* Signal main process that we are done. */
	SetLatch(&registrant->procLatch);
}

/*
 * Set the exit status so that the main apply worker can realize we have
 * shutdown.
 */
static void
apply_bgworker_shutdown(int code, Datum arg)
{
	SpinLockAcquire(&MyParallelShared->mutex);
	MyParallelShared->status = APPLY_BGWORKER_EXIT;
	SpinLockRelease(&MyParallelShared->mutex);

	dsm_detach((dsm_segment *) DatumGetPointer(arg));
}

/*
 * Apply Background Worker entry point
 */
void
ApplyBgworkerMain(Datum main_arg)
{
	volatile ApplyBgworkerShared *shared;

	dsm_handle	handle;
	dsm_segment *seg;
	shm_toc    *toc;
	shm_mq	   *mq;
	shm_mq_handle *mqh;
	MemoryContext oldcontext;
	RepOriginId originid;
	int			worker_slot = DatumGetInt32(main_arg);
	char		originname[NAMEDATALEN];

	MemoryContextSwitchTo(TopMemoryContext);

	/* Init the memory context for the apply background worker to work in. */
	ApplyContext = AllocSetContextCreate(TopMemoryContext,
										 "ApplyContext",
										 ALLOCSET_DEFAULT_SIZES);

	/*
	 * Init the ApplyMessageContext which we clean up after each replication
	 * protocol message.
	 */
	ApplyMessageContext = AllocSetContextCreate(ApplyContext,
												"ApplyMessageContext",
												ALLOCSET_DEFAULT_SIZES);

	/* Setup signal handling */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/*
	 * Connect to the dynamic shared memory segment.
	 *
	 * The backend that registered this worker passed us the ID of a shared
	 * memory segment to which we must attach for further instructions.  In
	 * order to attach to dynamic shared memory, we need a resource owner.
	 * Once we've mapped the segment in our address space, attach to the table
	 * of contents so we can locate the various data structures we'll need to
	 * find within the segment.
	 */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "Logical apply worker");
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

	before_shmem_exit(apply_bgworker_shutdown, PointerGetDatum(seg));

	/* Look up the shared information. */
	shared = shm_toc_lookup(toc, APPLY_BGWORKER_KEY_SHARED, false);
	MyParallelShared = shared;

	/*
	 * Attach to the message queue.
	 */
	mq = shm_toc_lookup(toc, APPLY_BGWORKER_KEY_MQ, false);
	shm_mq_set_receiver(mq, MyProc);
	mqh = shm_mq_attach(mq, seg, NULL);

	/* Run as replica session replication role. */
	SetConfigOption("session_replication_role", "replica",
					PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Now, we have initialized DSM. Attach to slot.
	 */
	logicalrep_worker_attach(worker_slot);

	/* Connect to our database. */
	BackgroundWorkerInitializeConnectionByOid(MyLogicalRepWorker->dbid,
											  MyLogicalRepWorker->userid,
											  0);

	/*
	 * Set always-secure search path, so malicious users can't redirect user
	 * code (e.g. pg_index.indexprs).
	 */
	SetConfigOption("search_path", "", PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Set the client encoding to the database encoding, since that is what
	 * the leader will expect.
	 */
	SetClientEncoding(GetDatabaseEncoding());

	stream_xid = shared->stream_xid;

	StartTransactionCommand();
	oldcontext = MemoryContextSwitchTo(ApplyContext);

	MySubscription = GetSubscription(MyLogicalRepWorker->subid, true);
	if (!MySubscription)
	{
		ereport(LOG,
				(errmsg("logical replication apply worker for subscription %u will not "
						"start because the subscription was removed during startup",
						MyLogicalRepWorker->subid)));
		proc_exit(0);
	}

	MySubscriptionValid = true;
	MemoryContextSwitchTo(oldcontext);

	/* Setup synchronous commit according to the user's wishes */
	SetConfigOption("synchronous_commit", MySubscription->synccommit,
					PGC_BACKEND, PGC_S_OVERRIDE);

	/* Keep us informed about subscription changes. */
	CacheRegisterSyscacheCallback(SUBSCRIPTIONOID,
								  subscription_change_cb,
								  (Datum) 0);

	CommitTransactionCommand();

	/* Setup replication origin tracking. */
	StartTransactionCommand();
	snprintf(originname, sizeof(originname), "pg_%u", MySubscription->oid);
	originid = replorigin_by_name(originname, true);
	if (!OidIsValid(originid))
		originid = replorigin_create(originname);

	/*
	 * The apply background worker doesn't need to monopolize this replication
	 * origin which was already acquired by its leader process.
	 */
	replorigin_session_setup(originid, false);
	replorigin_session_origin = originid;
	CommitTransactionCommand();

	/*
	 * Allocate the origin name in long-lived context for error context
	 * message.
	 */
	apply_error_callback_arg.origin_name = MemoryContextStrdup(ApplyContext,
															   originname);

	elog(DEBUG1, "[Apply BGW #%u] started", shared->n);

	LogicalApplyBgwLoop(mqh, shared);

	/*
	 * We're done.  Explicitly detach the shared memory segment so that we
	 * don't get a resource leak warning at commit time.  This will fire any
	 * on_dsm_detach callbacks we've registered, as well.  Once that's done,
	 * we can go ahead and exit.
	 */
	dsm_detach(seg);
	proc_exit(0);
}

/*
 * Set up a dynamic shared memory segment.
 *
 * We set up a control region that contains a ApplyBgworkerShared,
 * plus one region per message queue. There are as many message queues as
 * the number of workers.
 */
static void
apply_bgworker_setup_dsm(ApplyBgworkerState *wstate)
{
	shm_toc_estimator e;
	Size		segsize;
	dsm_segment *seg;
	shm_toc    *toc;
	ApplyBgworkerShared *shared;
	shm_mq	   *mq;
	int64		queue_size = DSM_QUEUE_SIZE;
	int			server_version;

	/*
	 * Estimate how much shared memory we need.
	 *
	 * Because the TOC machinery may choose to insert padding of oddly-sized
	 * requests, we must estimate each chunk separately.
	 *
	 * We need one key to register the location of the header, and we need
	 * another key to track the location of the message queue.
	 */
	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, sizeof(ApplyBgworkerShared));
	shm_toc_estimate_chunk(&e, (Size) queue_size);

	shm_toc_estimate_keys(&e, 1 + 1);
	segsize = shm_toc_estimate(&e);

	/* Create the shared memory segment and establish a table of contents. */
	seg = dsm_create(shm_toc_estimate(&e), 0);
	toc = shm_toc_create(PG_LOGICAL_APPLY_SHM_MAGIC, dsm_segment_address(seg),
						 segsize);

	/* Set up the header region. */
	shared = shm_toc_allocate(toc, sizeof(ApplyBgworkerShared));
	SpinLockInit(&shared->mutex);
	shared->status = APPLY_BGWORKER_BUSY;

	server_version = walrcv_server_version(LogRepWorkerWalRcvConn);
	shared->server_version =
		server_version >= 160000 ? LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM :
		server_version >= 150000 ? LOGICALREP_PROTO_TWOPHASE_VERSION_NUM :
		server_version >= 140000 ? LOGICALREP_PROTO_STREAM_VERSION_NUM :
		LOGICALREP_PROTO_VERSION_NUM;

	shared->stream_xid = stream_xid;
	shared->n = list_length(ApplyWorkersList) + 1;

	shm_toc_insert(toc, APPLY_BGWORKER_KEY_SHARED, shared);

	/* Set up message queue for the worker. */
	mq = shm_mq_create(shm_toc_allocate(toc, (Size) queue_size),
					   (Size) queue_size);
	shm_toc_insert(toc, APPLY_BGWORKER_KEY_MQ, mq);
	shm_mq_set_sender(mq, MyProc);

	/* Attach the queue. */
	wstate->mq_handle = shm_mq_attach(mq, seg, NULL);

	/* Return results to caller. */
	wstate->dsm_seg = seg;
	wstate->shared = shared;
}

/*
 * Start apply background worker process and allocate shared memory for it.
 */
static ApplyBgworkerState *
apply_bgworker_setup(void)
{
	MemoryContext oldcontext;
	bool		launched;
	ApplyBgworkerState *wstate;
	int			napplyworkers;

	elog(DEBUG1, "setting up apply worker #%u", list_length(ApplyWorkersList) + 1);

	/* Check if there are free worker slot(s) */
	LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
	napplyworkers = logicalrep_apply_bgworker_count(MyLogicalRepWorker->subid);
	LWLockRelease(LogicalRepWorkerLock);
	if (napplyworkers >= max_apply_bgworkers_per_subscription)
		return NULL;

	oldcontext = MemoryContextSwitchTo(ApplyContext);

	wstate = (ApplyBgworkerState *) palloc0(sizeof(ApplyBgworkerState));

	/* Setup shared memory */
	apply_bgworker_setup_dsm(wstate);

	launched = logicalrep_worker_launch(MyLogicalRepWorker->dbid,
										MySubscription->oid,
										MySubscription->name,
										MyLogicalRepWorker->userid,
										InvalidOid,
										dsm_segment_handle(wstate->dsm_seg));

	if (launched)
		ApplyWorkersList = lappend(ApplyWorkersList, wstate);
	else
	{
		dsm_detach(wstate->dsm_seg);
		wstate->dsm_seg = NULL;

		pfree(wstate);
		wstate = NULL;
	}

	MemoryContextSwitchTo(oldcontext);

	return wstate;
}

/*
 * Send the data to the specified apply background worker via shared-memory queue.
 */
void
apply_bgworker_send_data(ApplyBgworkerState *wstate, Size nbytes, const void *data)
{
	shm_mq_result result;

	result = shm_mq_send(wstate->mq_handle, nbytes, data, false, true);

	if (result != SHM_MQ_SUCCESS)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not send tuples to shared-memory queue")));
}

/*
 * Wait until the status of apply background worker reaches the
 * 'wait_for_status'
 */
void
apply_bgworker_wait_for(ApplyBgworkerState *wstate,
						ApplyBgworkerStatus wait_for_status)
{
	for (;;)
	{
		char		status;

		SpinLockAcquire(&wstate->shared->mutex);
		status = wstate->shared->status;
		SpinLockRelease(&wstate->shared->mutex);

		/* Done if already in correct status. */
		if (status == wait_for_status)
			break;

		/* If any workers (or the postmaster) have died, we have failed. */
		if (status == APPLY_BGWORKER_EXIT)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("background worker %u failed to apply transaction %u",
							wstate->shared->n, wstate->shared->stream_xid)));

		/* Wait to be signalled. */
		WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0,
				  WAIT_EVENT_LOGICAL_APPLY_WORKER_STATE_CHANGE);

		/* Reset the latch so we don't spin. */
		ResetLatch(MyLatch);

		/* An interrupt may have occurred while we were waiting. */
		CHECK_FOR_INTERRUPTS();
	}
}

/*
 * Check the status of workers and report an error if any apply background
 * worker has exited unexpectedly.
 */
void
apply_bgworker_check_status(void)
{
	ListCell   *lc;

	if (am_apply_bgworker() || MySubscription->stream != SUBSTREAM_PARALLEL)
		return;

	foreach(lc, ApplyWorkersList)
	{
		ApplyBgworkerState *wstate = (ApplyBgworkerState *) lfirst(lc);

		/*
		 * We don't lock here as in the worst case we will just detect the
		 * failure of worker a bit later.
		 */
		if (wstate->shared->status == APPLY_BGWORKER_EXIT)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("background worker %u exited unexpectedly",
							wstate->shared->n)));
	}

	/*
	 * Exit if any relation is not in the READY state and if any worker is
	 * handling the streaming transaction at the same time. Because for
	 * streaming transactions that is being applied in apply background
	 * worker, we cannot decide whether to apply the change for a relation
	 * that is not in the READY state (see should_apply_changes_for_rel) as we
	 * won't know remote_final_lsn by that time.
	 */
	if (list_length(ApplyWorkersFreeList) != list_length(ApplyWorkersList) &&
		!AllTablesyncsReady())
	{
		ereport(LOG,
				(errmsg("logical replication apply workers for subscription \"%s\" will restart",
						MySubscription->name),
				 errdetail("Cannot handle streamed replication transaction by apply "
						   "background workers until all tables are synchronized")));

		proc_exit(0);
	}
}

/* Set the apply background worker status */
void
apply_bgworker_set_status(ApplyBgworkerStatus status)
{
	if (!am_apply_bgworker())
		return;

	elog(DEBUG1, "[Apply BGW #%u] set status to %d", MyParallelShared->n, status);

	SpinLockAcquire(&MyParallelShared->mutex);
	MyParallelShared->status = status;
	SpinLockRelease(&MyParallelShared->mutex);
}

/*
 * Define a savepoint for a subxact in apply background worker if needed.
 *
 * Inside apply background worker we can figure out that new subtransaction was
 * started if new change arrived with different xid. In that case we can define
 * named savepoint, so that we were able to commit/rollback it separately
 * later.
 * Special case is if the first change comes from subtransaction, then
 * we check that current_xid differs from stream_xid.
 */
void
apply_bgworker_subxact_info_add(TransactionId current_xid)
{
	if (current_xid != stream_xid &&
		!list_member_int(subxactlist, (int) current_xid))
	{
		MemoryContext oldctx;
		char		spname[MAXPGPATH];

		snprintf(spname, MAXPGPATH, "savepoint_for_xid_%u", current_xid);

		elog(DEBUG1, "[Apply BGW #%u] defining savepoint %s",
			 MyParallelShared->n, spname);

		DefineSavepoint(spname);
		CommitTransactionCommand();

		oldctx = MemoryContextSwitchTo(ApplyContext);
		subxactlist = lappend_int(subxactlist, (int) current_xid);
		MemoryContextSwitchTo(oldctx);
	}
}

/*
 * Check if changes on this relation can be applied by an apply background
 * worker.
 *
 * Although the commit order is maintained only allowing one process to commit
 * at a time, the access order to the relation has changed. This could cause
 * unexpected problems if the unique column on the replicated table is
 * inconsistent with the publisher-side or contains non-immutable functions
 * when applying transactions in the apply background worker.
 */
void
apply_bgworker_relation_check(LogicalRepRelMapEntry *rel)
{
	/* Skip check if not an apply background worker. */
	if (!am_apply_bgworker())
		return;

	/*
	 * Partition table checks are done later in function
	 * apply_handle_tuple_routing.
	 */
	if (rel->localrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		return;

	/*
	 * Return if changes on this relation can be applied by an apply background
	 * worker.
	 */
	if (rel->parallel_apply == PARALLEL_APPLY_SAFE)
		return;

	/* We are in error mode and should give user correct error. */
	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("cannot replicate target relation \"%s.%s\" using "
					"subscription parameter streaming=parallel",
					rel->remoterel.nspname, rel->remoterel.relname),
			 errdetail("The unique column on subscriber is not the unique "
					   "column on publisher or there is at least one "
					   "non-immutable function.")));
}
