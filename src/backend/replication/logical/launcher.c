/*-------------------------------------------------------------------------
 * launcher.c
 *	   PostgreSQL logical replication worker launcher process
 *
 * Copyright (c) 2016-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/launcher.c
 *
 * NOTES
 *	  This module contains the logical replication worker launcher which
 *	  uses the background worker infrastructure to start the logical
 *	  replication workers for every enabled subscription.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_subscription_rel.h"
#include "funcapi.h"
#include "lib/dshash.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/fork_process.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "replication/logicallauncher.h"
#include "replication/logicalworker.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "replication/worker_internal.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/timeout.h"

/* max sleep time between cycles (3min) */
#define DEFAULT_NAPTIME_PER_CYCLE 180000L

/* GUC variables */
int			max_logical_replication_workers = 4;
int			max_sync_workers_per_subscription = 2;
int			max_parallel_apply_workers_per_subscription = 2;
int			max_slotsync_workers = 2;

/*
 * The local variables to store the current values of slot-sync related GUCs
 * before each ConfigReload.
 */
static char *PrimaryConnInfoPreReload = NULL;
static char *PrimarySlotNamePreReload = NULL;
static char *SyncSlotNamesPreReload = NULL;

/*
 * Initial allocation size for dbids array for each SlotSyncWorker in dynamic
 * shared memory.
 */
#define DB_PER_WORKER_ALLOC_INIT 100

/*
 * Once initially allocated size is exhausted for dbids array, it is extended by
 * DB_PER_WORKER_ALLOC_EXTRA size.
 */
#define DB_PER_WORKER_ALLOC_EXTRA 100

SlotSyncWorker *MySlotSyncWorker = NULL;

LogicalRepWorker *MyLogicalRepWorker = NULL;

typedef struct LogicalRepCtxStruct
{
	/* Supervisor process. */
	pid_t		launcher_pid;

	/* Hash table holding last start times of subscriptions' apply workers. */
	dsa_handle	last_start_dsa;
	dshash_table_handle last_start_dsh;

	/* Background workers. */
	SlotSyncWorker *ss_workers; /* slot-sync workers */
	LogicalRepWorker workers[FLEXIBLE_ARRAY_MEMBER];
} LogicalRepCtxStruct;

static LogicalRepCtxStruct *LogicalRepCtx;

/* an entry in the last-start-times shared hash table */
typedef struct LauncherLastStartTimesEntry
{
	Oid			subid;			/* OID of logrep subscription (hash key) */
	TimestampTz last_start_time;	/* last time its apply worker was started */
} LauncherLastStartTimesEntry;

/* parameters for the last-start-times shared hash table */
static const dshash_parameters dsh_params = {
	sizeof(Oid),
	sizeof(LauncherLastStartTimesEntry),
	dshash_memcmp,
	dshash_memhash,
	LWTRANCHE_LAUNCHER_HASH
};

static dsa_area *last_start_times_dsa = NULL;
static dshash_table *last_start_times = NULL;

static bool on_commit_launcher_wakeup = false;


static void ApplyLauncherWakeup(void);
static void logicalrep_launcher_onexit(int code, Datum arg);
static void logicalrep_worker_onexit(int code, Datum arg);
static void logicalrep_worker_detach(void);
static void logicalrep_worker_cleanup(LogicalRepWorker *worker);
static void slotsync_worker_cleanup(SlotSyncWorker *worker);
static int	logicalrep_pa_worker_count(Oid subid);
static void logicalrep_launcher_attach_dshmem(void);
static void ApplyLauncherSetWorkerStartTime(Oid subid, TimestampTz start_time);
static TimestampTz ApplyLauncherGetWorkerStartTime(Oid subid);


/*
 * Load the list of subscriptions.
 *
 * Only the fields interesting for worker start/stop functions are filled for
 * each subscription.
 */
static List *
get_subscription_list(void)
{
	List	   *res = NIL;
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tup;
	MemoryContext resultcxt;

	/* This is the context that we will allocate our output data in */
	resultcxt = CurrentMemoryContext;

	/*
	 * Start a transaction so we can access pg_database, and get a snapshot.
	 * We don't have a use for the snapshot itself, but we're interested in
	 * the secondary effect that it sets RecentGlobalXmin.  (This is critical
	 * for anything that reads heap pages, because HOT may decide to prune
	 * them even if the process doesn't attempt to modify any tuples.)
	 *
	 * FIXME: This comment is inaccurate / the code buggy. A snapshot that is
	 * not pushed/active does not reliably prevent HOT pruning (->xmin could
	 * e.g. be cleared when cache invalidations are processed).
	 */
	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	rel = table_open(SubscriptionRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_subscription subform = (Form_pg_subscription) GETSTRUCT(tup);
		Subscription *sub;
		MemoryContext oldcxt;

		/*
		 * Allocate our results in the caller's context, not the
		 * transaction's. We do this inside the loop, and restore the original
		 * context at the end, so that leaky things like heap_getnext() are
		 * not called in a potentially long-lived context.
		 */
		oldcxt = MemoryContextSwitchTo(resultcxt);

		sub = (Subscription *) palloc0(sizeof(Subscription));
		sub->oid = subform->oid;
		sub->dbid = subform->subdbid;
		sub->owner = subform->subowner;
		sub->enabled = subform->subenabled;
		sub->name = pstrdup(NameStr(subform->subname));
		/* We don't fill fields we are not interested in. */

		res = lappend(res, sub);
		MemoryContextSwitchTo(oldcxt);
	}

	table_endscan(scan);
	table_close(rel, AccessShareLock);

	CommitTransactionCommand();

	return res;
}

/*
 * This is common code for logical workers and slotsync workers.
 *
 * Wait for a background worker to start up and attach to the shmem context.
 *
 * This is only needed for cleaning up the shared memory in case the worker
 * fails to attach.
 *
 * Returns whether the attach was successful.
 */
static bool
WaitForReplicationWorkerAttach(LogicalWorkerHeader *worker,
							   uint16 generation,
							   BackgroundWorkerHandle *handle,
							   LWLock *lock)
{
	BgwHandleStatus status;
	int			rc;
	bool		is_slotsync_worker = (lock == SlotSyncWorkerLock) ? true : false;

	for (;;)
	{
		pid_t		pid;

		CHECK_FOR_INTERRUPTS();

		LWLockAcquire(lock, LW_SHARED);

		/* Worker either died or has started. Return false if died. */
		if (!worker->in_use || worker->proc)
		{
			LWLockRelease(lock);
			return worker->in_use;
		}

		LWLockRelease(lock);

		/* Check if worker has died before attaching, and clean up after it. */
		status = GetBackgroundWorkerPid(handle, &pid);

		if (status == BGWH_STOPPED)
		{
			LWLockAcquire(lock, LW_EXCLUSIVE);
			/* Ensure that this was indeed the worker we waited for. */
			if (generation == worker->generation)
			{
				if (is_slotsync_worker)
					slotsync_worker_cleanup((SlotSyncWorker *) worker);
				else
					logicalrep_worker_cleanup((LogicalRepWorker *) worker);
			}
			LWLockRelease(lock);
			return false;
		}

		/*
		 * We need timeout because we generally don't get notified via latch
		 * about the worker attach.  But we don't expect to have to wait long.
		 */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   10L, WAIT_EVENT_BGWORKER_STARTUP);

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}
	}
}

/*
 * Walks the workers array and searches for one that matches given
 * subscription id and relid.
 *
 * We are only interested in the leader apply worker or table sync worker.
 */
LogicalRepWorker *
logicalrep_worker_find(Oid subid, Oid relid, bool only_running)
{
	int			i;
	LogicalRepWorker *res = NULL;

	Assert(LWLockHeldByMe(LogicalRepWorkerLock));

	/* Search for attached worker for a given subscription id. */
	for (i = 0; i < max_logical_replication_workers; i++)
	{
		LogicalRepWorker *w = &LogicalRepCtx->workers[i];

		/* Skip parallel apply workers. */
		if (isParallelApplyWorker(w))
			continue;

		if (w->hdr.in_use && w->subid == subid && w->relid == relid &&
			(!only_running || w->hdr.proc))
		{
			res = w;
			break;
		}
	}

	return res;
}

/*
 * Similar to logicalrep_worker_find(), but returns a list of all workers for
 * the subscription, instead of just one.
 */
List *
logicalrep_workers_find(Oid subid, bool only_running)
{
	int			i;
	List	   *res = NIL;

	Assert(LWLockHeldByMe(LogicalRepWorkerLock));

	/* Search for attached worker for a given subscription id. */
	for (i = 0; i < max_logical_replication_workers; i++)
	{
		LogicalRepWorker *w = &LogicalRepCtx->workers[i];

		if (w->hdr.in_use && w->subid == subid &&
			(!only_running || w->hdr.proc))
			res = lappend(res, w);
	}

	return res;
}

/*
 * Start new logical replication background worker, if possible.
 *
 * Returns true on success, false on failure.
 */
bool
logicalrep_worker_launch(LogicalRepWorkerType wtype,
						 Oid dbid, Oid subid, const char *subname, Oid userid,
						 Oid relid, dsm_handle subworker_dsm)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *bgw_handle;
	uint16		generation;
	int			i;
	int			slot = 0;
	LogicalRepWorker *worker = NULL;
	int			nsyncworkers;
	int			nparallelapplyworkers;
	TimestampTz now;
	bool		is_tablesync_worker = (wtype == WORKERTYPE_TABLESYNC);
	bool		is_parallel_apply_worker = (wtype == WORKERTYPE_PARALLEL_APPLY);

	/*----------
	 * Sanity checks:
	 * - must be valid worker type
	 * - tablesync workers are only ones to have relid
	 * - parallel apply worker is the only kind of subworker
	 */
	Assert(wtype != WORKERTYPE_UNKNOWN);
	Assert(is_tablesync_worker == OidIsValid(relid));
	Assert(is_parallel_apply_worker == (subworker_dsm != DSM_HANDLE_INVALID));

	ereport(DEBUG1,
			(errmsg_internal("starting logical replication worker for subscription \"%s\"",
							 subname)));

	/* Report this after the initial starting message for consistency. */
	if (max_replication_slots == 0)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("cannot start logical replication workers when max_replication_slots = 0")));

	/*
	 * We need to do the modification of the shared memory under lock so that
	 * we have consistent view.
	 */
	LWLockAcquire(LogicalRepWorkerLock, LW_EXCLUSIVE);

retry:
	/* Find unused worker slot. */
	for (i = 0; i < max_logical_replication_workers; i++)
	{
		LogicalRepWorker *w = &LogicalRepCtx->workers[i];

		if (!w->hdr.in_use)
		{
			worker = w;
			slot = i;
			break;
		}
	}

	nsyncworkers = logicalrep_sync_worker_count(subid);

	now = GetCurrentTimestamp();

	/*
	 * If we didn't find a free slot, try to do garbage collection.  The
	 * reason we do this is because if some worker failed to start up and its
	 * parent has crashed while waiting, the in_use state was never cleared.
	 */
	if (worker == NULL || nsyncworkers >= max_sync_workers_per_subscription)
	{
		bool		did_cleanup = false;

		for (i = 0; i < max_logical_replication_workers; i++)
		{
			LogicalRepWorker *w = &LogicalRepCtx->workers[i];

			/*
			 * If the worker was marked in use but didn't manage to attach in
			 * time, clean it up.
			 */
			if (w->hdr.in_use && !w->hdr.proc &&
				TimestampDifferenceExceeds(w->hdr.launch_time, now,
										   wal_receiver_timeout))
			{
				elog(WARNING,
					 "logical replication worker for subscription %u took too long to start; canceled",
					 w->subid);

				logicalrep_worker_cleanup(w);
				did_cleanup = true;
			}
		}

		if (did_cleanup)
			goto retry;
	}

	/*
	 * We don't allow to invoke more sync workers once we have reached the
	 * sync worker limit per subscription. So, just return silently as we
	 * might get here because of an otherwise harmless race condition.
	 */
	if (is_tablesync_worker && nsyncworkers >= max_sync_workers_per_subscription)
	{
		LWLockRelease(LogicalRepWorkerLock);
		return false;
	}

	nparallelapplyworkers = logicalrep_pa_worker_count(subid);

	/*
	 * Return false if the number of parallel apply workers reached the limit
	 * per subscription.
	 */
	if (is_parallel_apply_worker &&
		nparallelapplyworkers >= max_parallel_apply_workers_per_subscription)
	{
		LWLockRelease(LogicalRepWorkerLock);
		return false;
	}

	/*
	 * However if there are no more free worker slots, inform user about it
	 * before exiting.
	 */
	if (worker == NULL)
	{
		LWLockRelease(LogicalRepWorkerLock);
		ereport(WARNING,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("out of logical replication worker slots"),
				 errhint("You might need to increase %s.", "max_logical_replication_workers")));
		return false;
	}

	/* Prepare the worker slot. */
	worker->type = wtype;
	worker->hdr.launch_time = now;
	worker->hdr.in_use = true;
	worker->hdr.generation++;
	worker->hdr.proc = NULL;
	worker->dbid = dbid;
	worker->userid = userid;
	worker->subid = subid;
	worker->relid = relid;
	worker->relstate = SUBREL_STATE_UNKNOWN;
	worker->relstate_lsn = InvalidXLogRecPtr;
	worker->stream_fileset = NULL;
	worker->leader_pid = is_parallel_apply_worker ? MyProcPid : InvalidPid;
	worker->parallel_apply = is_parallel_apply_worker;
	worker->last_lsn = InvalidXLogRecPtr;
	TIMESTAMP_NOBEGIN(worker->last_send_time);
	TIMESTAMP_NOBEGIN(worker->last_recv_time);
	worker->reply_lsn = InvalidXLogRecPtr;
	TIMESTAMP_NOBEGIN(worker->reply_time);

	/* Before releasing lock, remember generation for future identification. */
	generation = worker->hdr.generation;

	LWLockRelease(LogicalRepWorkerLock);

	/* Register the new dynamic worker. */
	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");

	switch (worker->type)
	{
		case WORKERTYPE_APPLY:
			snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ApplyWorkerMain");
			snprintf(bgw.bgw_name, BGW_MAXLEN,
					 "logical replication apply worker for subscription %u",
					 subid);
			snprintf(bgw.bgw_type, BGW_MAXLEN, "logical replication apply worker");
			break;

		case WORKERTYPE_PARALLEL_APPLY:
			snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ParallelApplyWorkerMain");
			snprintf(bgw.bgw_name, BGW_MAXLEN,
					 "logical replication parallel apply worker for subscription %u",
					 subid);
			snprintf(bgw.bgw_type, BGW_MAXLEN, "logical replication parallel worker");

			memcpy(bgw.bgw_extra, &subworker_dsm, sizeof(dsm_handle));
			break;

		case WORKERTYPE_TABLESYNC:
			snprintf(bgw.bgw_function_name, BGW_MAXLEN, "TablesyncWorkerMain");
			snprintf(bgw.bgw_name, BGW_MAXLEN,
					 "logical replication tablesync worker for subscription %u sync %u",
					 subid,
					 relid);
			snprintf(bgw.bgw_type, BGW_MAXLEN, "logical replication tablesync worker");
			break;

		case WORKERTYPE_UNKNOWN:
			/* Should never happen. */
			elog(ERROR, "unknown worker type");
	}

	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_notify_pid = MyProcPid;
	bgw.bgw_main_arg = Int32GetDatum(slot);

	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
	{
		/* Failed to start worker, so clean up the worker slot. */
		LWLockAcquire(LogicalRepWorkerLock, LW_EXCLUSIVE);
		Assert(generation == worker->hdr.generation);
		logicalrep_worker_cleanup(worker);
		LWLockRelease(LogicalRepWorkerLock);

		ereport(WARNING,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("out of background worker slots"),
				 errhint("You might need to increase %s.", "max_worker_processes")));
		return false;
	}

	/* Now wait until it attaches. */
	return WaitForReplicationWorkerAttach((LogicalWorkerHeader *) worker,
										  generation,
										  bgw_handle,
										  LogicalRepWorkerLock);
}

/*
 * Internal function to stop the worker and wait until it detaches from the
 * slot. It is used for both logical rep workers and slot-sync workers.
 */
static void
logicalrep_worker_stop_internal(LogicalWorkerHeader *worker, int signo,
								LWLock *lock)
{
	uint16		generation;

	Assert(LWLockHeldByMeInMode(lock, LW_SHARED));

	/*
	 * Remember which generation was our worker so we can check if what we see
	 * is still the same one.
	 */
	generation = worker->generation;

	/*
	 * If we found a worker but it does not have proc set then it is still
	 * starting up; wait for it to finish starting and then kill it.
	 */
	while (worker->in_use && !worker->proc)
	{
		int			rc;

		LWLockRelease(lock);

		/* Wait a bit --- we don't expect to have to wait long. */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   10L, WAIT_EVENT_BGWORKER_STARTUP);

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}

		/* Recheck worker status. */
		LWLockAcquire(lock, LW_SHARED);

		/*
		 * Check whether the worker slot is no longer used, which would mean
		 * that the worker has exited, or whether the worker generation is
		 * different, meaning that a different worker has taken the slot.
		 */
		if (!worker->in_use || worker->generation != generation)
			return;

		/* Worker has assigned proc, so it has started. */
		if (worker->proc)
			break;
	}

	/* Now terminate the worker ... */
	kill(worker->proc->pid, signo);

	/* ... and wait for it to die. */
	for (;;)
	{
		int			rc;

		/* is it gone? */
		if (!worker->proc || worker->generation != generation)
			break;

		LWLockRelease(lock);

		/* Wait a bit --- we don't expect to have to wait long. */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   10L, WAIT_EVENT_BGWORKER_SHUTDOWN);

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}

		LWLockAcquire(lock, LW_SHARED);
	}
}

/*
 * Stop the logical replication worker for subid/relid, if any.
 */
void
logicalrep_worker_stop(Oid subid, Oid relid)
{
	LogicalRepWorker *worker;

	LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);

	worker = logicalrep_worker_find(subid, relid, false);

	if (worker)
	{
		Assert(!isParallelApplyWorker(worker));
		logicalrep_worker_stop_internal((LogicalWorkerHeader *) worker,
										SIGTERM,
										LogicalRepWorkerLock);
	}

	LWLockRelease(LogicalRepWorkerLock);
}

/*
 * Stop the given logical replication parallel apply worker.
 *
 * Node that the function sends SIGINT instead of SIGTERM to the parallel apply
 * worker so that the worker exits cleanly.
 */
void
logicalrep_pa_worker_stop(ParallelApplyWorkerInfo *winfo)
{
	int			slot_no;
	uint16		generation;
	LogicalRepWorker *worker;

	SpinLockAcquire(&winfo->shared->mutex);
	generation = winfo->shared->logicalrep_worker_generation;
	slot_no = winfo->shared->logicalrep_worker_slot_no;
	SpinLockRelease(&winfo->shared->mutex);

	Assert(slot_no >= 0 && slot_no < max_logical_replication_workers);

	/*
	 * Detach from the error_mq_handle for the parallel apply worker before
	 * stopping it. This prevents the leader apply worker from trying to
	 * receive the message from the error queue that might already be detached
	 * by the parallel apply worker.
	 */
	if (winfo->error_mq_handle)
	{
		shm_mq_detach(winfo->error_mq_handle);
		winfo->error_mq_handle = NULL;
	}

	LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);

	worker = &LogicalRepCtx->workers[slot_no];
	Assert(isParallelApplyWorker(worker));

	/*
	 * Only stop the worker if the generation matches and the worker is alive.
	 */
	if (worker->hdr.generation == generation && worker->hdr.proc)
		logicalrep_worker_stop_internal((LogicalWorkerHeader *) worker,
										SIGINT,
										LogicalRepWorkerLock);

	LWLockRelease(LogicalRepWorkerLock);
}

/*
 * Wake up (using latch) any logical replication worker for specified sub/rel.
 */
void
logicalrep_worker_wakeup(Oid subid, Oid relid)
{
	LogicalRepWorker *worker;

	LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);

	worker = logicalrep_worker_find(subid, relid, true);

	if (worker)
		logicalrep_worker_wakeup_ptr(worker);

	LWLockRelease(LogicalRepWorkerLock);
}

/*
 * Wake up (using latch) the specified logical replication worker.
 *
 * Caller must hold lock, else worker->hdr.proc could change under us.
 */
void
logicalrep_worker_wakeup_ptr(LogicalRepWorker *worker)
{
	Assert(LWLockHeldByMe(LogicalRepWorkerLock));

	SetLatch(&worker->hdr.proc->procLatch);
}

/*
 * Attach to a slot.
 */
void
logicalrep_worker_attach(int slot)
{
	/* Block concurrent access. */
	LWLockAcquire(LogicalRepWorkerLock, LW_EXCLUSIVE);

	Assert(slot >= 0 && slot < max_logical_replication_workers);
	MyLogicalRepWorker = &LogicalRepCtx->workers[slot];

	if (!MyLogicalRepWorker->hdr.in_use)
	{
		LWLockRelease(LogicalRepWorkerLock);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("logical replication worker slot %d is empty, cannot attach",
						slot)));
	}

	if (MyLogicalRepWorker->hdr.proc)
	{
		LWLockRelease(LogicalRepWorkerLock);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("logical replication worker slot %d is already used by "
						"another worker, cannot attach", slot)));
	}

	MyLogicalRepWorker->hdr.proc = MyProc;
	before_shmem_exit(logicalrep_worker_onexit, (Datum) 0);

	LWLockRelease(LogicalRepWorkerLock);
}

/*
 * Stop the parallel apply workers if any, and detach the leader apply worker
 * (cleans up the worker info).
 */
static void
logicalrep_worker_detach(void)
{
	/* Stop the parallel apply workers. */
	if (am_leader_apply_worker())
	{
		List	   *workers;
		ListCell   *lc;

		/*
		 * Detach from the error_mq_handle for all parallel apply workers
		 * before terminating them. This prevents the leader apply worker from
		 * receiving the worker termination message and sending it to logs
		 * when the same is already done by the parallel worker.
		 */
		pa_detach_all_error_mq();

		LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);

		workers = logicalrep_workers_find(MyLogicalRepWorker->subid, true);
		foreach(lc, workers)
		{
			LogicalRepWorker *w = (LogicalRepWorker *) lfirst(lc);

			if (isParallelApplyWorker(w))
				logicalrep_worker_stop_internal((LogicalWorkerHeader *) w,
												SIGTERM,
												LogicalRepWorkerLock);
		}

		LWLockRelease(LogicalRepWorkerLock);
	}

	/* Block concurrent access. */
	LWLockAcquire(LogicalRepWorkerLock, LW_EXCLUSIVE);

	logicalrep_worker_cleanup(MyLogicalRepWorker);

	LWLockRelease(LogicalRepWorkerLock);
}

/*
 * Clean up worker info.
 */
static void
logicalrep_worker_cleanup(LogicalRepWorker *worker)
{
	Assert(LWLockHeldByMeInMode(LogicalRepWorkerLock, LW_EXCLUSIVE));

	worker->type = WORKERTYPE_UNKNOWN;
	worker->hdr.in_use = false;
	worker->hdr.proc = NULL;
	worker->userid = InvalidOid;
	worker->dbid = InvalidOid;
	worker->subid = InvalidOid;
	worker->relid = InvalidOid;
	worker->leader_pid = InvalidPid;
	worker->parallel_apply = false;
}

/*
 * Cleanup function for logical replication launcher.
 *
 * Called on logical replication launcher exit.
 */
static void
logicalrep_launcher_onexit(int code, Datum arg)
{
	LogicalRepCtx->launcher_pid = 0;
}

/*
 * Cleanup function.
 *
 * Called on logical replication worker exit.
 */
static void
logicalrep_worker_onexit(int code, Datum arg)
{
	/* Disconnect gracefully from the remote side. */
	if (LogRepWorkerWalRcvConn)
		walrcv_disconnect(LogRepWorkerWalRcvConn);

	logicalrep_worker_detach();

	/* Cleanup fileset used for streaming transactions. */
	if (MyLogicalRepWorker->stream_fileset != NULL)
		FileSetDeleteAll(MyLogicalRepWorker->stream_fileset);

	/*
	 * Session level locks may be acquired outside of a transaction in
	 * parallel apply mode and will not be released when the worker
	 * terminates, so manually release all locks before the worker exits.
	 *
	 * The locks will be acquired once the worker is initialized.
	 */
	if (!InitializingApplyWorker)
		LockReleaseAll(DEFAULT_LOCKMETHOD, true);

	ApplyLauncherWakeup();
}

/*
 * Count the number of registered (not necessarily running) sync workers
 * for a subscription.
 */
int
logicalrep_sync_worker_count(Oid subid)
{
	int			i;
	int			res = 0;

	Assert(LWLockHeldByMe(LogicalRepWorkerLock));

	/* Search for attached worker for a given subscription id. */
	for (i = 0; i < max_logical_replication_workers; i++)
	{
		LogicalRepWorker *w = &LogicalRepCtx->workers[i];

		if (isTablesyncWorker(w) && w->subid == subid)
			res++;
	}

	return res;
}

/*
 * Count the number of registered (but not necessarily running) parallel apply
 * workers for a subscription.
 */
static int
logicalrep_pa_worker_count(Oid subid)
{
	int			i;
	int			res = 0;

	Assert(LWLockHeldByMe(LogicalRepWorkerLock));

	/*
	 * Scan all attached parallel apply workers, only counting those which
	 * have the given subscription id.
	 */
	for (i = 0; i < max_logical_replication_workers; i++)
	{
		LogicalRepWorker *w = &LogicalRepCtx->workers[i];

		if (isParallelApplyWorker(w) && w->subid == subid)
			res++;
	}

	return res;
}

/*
 * ApplyLauncherShmemSize
 *		Compute space needed for replication launcher shared memory
 */
Size
ApplyLauncherShmemSize(void)
{
	Size		size;

	/*
	 * Need the fixed struct and the array of LogicalRepWorker.
	 */
	size = sizeof(LogicalRepCtxStruct);
	size = MAXALIGN(size);
	size = add_size(size, mul_size(max_logical_replication_workers,
								   sizeof(LogicalRepWorker)));
	return size;
}

/*
 * ApplyLauncherRegister
 *		Register a background worker running the logical replication launcher.
 */
void
ApplyLauncherRegister(void)
{
	BackgroundWorker bgw;

	if (max_logical_replication_workers == 0)
		return;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;

	/*
	 * The launcher now takes care of launching both logical apply workers and
	 * logical slot-sync workers. Thus to cater to the requirements of both,
	 * start it as soon as a consistent state is reached. This will help
	 * slot-sync workers to start timely on a physical standby while on a
	 * non-standby server, it holds same meaning as that of
	 * BgWorkerStart_RecoveryFinished.
	 */
	bgw.bgw_start_time = BgWorkerStart_ConsistentState;
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ApplyLauncherMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "logical replication launcher");
	snprintf(bgw.bgw_type, BGW_MAXLEN,
			 "logical replication launcher");
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
}

/*
 * ApplyLauncherShmemInit
 *		Allocate and initialize replication launcher shared memory
 */
void
ApplyLauncherShmemInit(void)
{
	bool		found;
	Size		ssw_size;

	LogicalRepCtx = (LogicalRepCtxStruct *)
		ShmemInitStruct("Logical Replication Launcher Data",
						ApplyLauncherShmemSize(),
						&found);

	if (!found)
	{
		int			slot;

		memset(LogicalRepCtx, 0, ApplyLauncherShmemSize());

		LogicalRepCtx->last_start_dsa = DSA_HANDLE_INVALID;
		LogicalRepCtx->last_start_dsh = DSHASH_HANDLE_INVALID;

		/* Initialize memory and spin locks for each worker slot. */
		for (slot = 0; slot < max_logical_replication_workers; slot++)
		{
			LogicalRepWorker *worker = &LogicalRepCtx->workers[slot];

			memset(worker, 0, sizeof(LogicalRepWorker));
			SpinLockInit(&worker->relmutex);
		}
	}

	/* Allocate shared-memory for slot-sync workers pool now */
	ssw_size = mul_size(max_slotsync_workers, sizeof(SlotSyncWorker));
	LogicalRepCtx->ss_workers = (SlotSyncWorker *)
		ShmemInitStruct("Replication slot-sync workers", ssw_size, &found);

	if (!found)
		memset(LogicalRepCtx->ss_workers, 0, ssw_size);
}

/*
 * Initialize or attach to the dynamic shared hash table that stores the
 * last-start times, if not already done.
 * This must be called before accessing the table.
 */
static void
logicalrep_launcher_attach_dshmem(void)
{
	MemoryContext oldcontext;

	/* Quick exit if we already did this. */
	if (LogicalRepCtx->last_start_dsh != DSHASH_HANDLE_INVALID &&
		last_start_times != NULL)
		return;

	/* Otherwise, use a lock to ensure only one process creates the table. */
	LWLockAcquire(LogicalRepWorkerLock, LW_EXCLUSIVE);

	/* Be sure any local memory allocated by DSA routines is persistent. */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	if (LogicalRepCtx->last_start_dsh == DSHASH_HANDLE_INVALID)
	{
		/* Initialize dynamic shared hash table for last-start times. */
		last_start_times_dsa = dsa_create(LWTRANCHE_LAUNCHER_DSA);
		dsa_pin(last_start_times_dsa);
		dsa_pin_mapping(last_start_times_dsa);
		last_start_times = dshash_create(last_start_times_dsa, &dsh_params, 0);

		/* Store handles in shared memory for other backends to use. */
		LogicalRepCtx->last_start_dsa = dsa_get_handle(last_start_times_dsa);
		LogicalRepCtx->last_start_dsh = dshash_get_hash_table_handle(last_start_times);
	}
	else if (!last_start_times)
	{
		/* Attach to existing dynamic shared hash table. */
		last_start_times_dsa = dsa_attach(LogicalRepCtx->last_start_dsa);
		dsa_pin_mapping(last_start_times_dsa);
		last_start_times = dshash_attach(last_start_times_dsa, &dsh_params,
										 LogicalRepCtx->last_start_dsh, 0);
	}

	MemoryContextSwitchTo(oldcontext);
	LWLockRelease(LogicalRepWorkerLock);
}

/*
 * Set the last-start time for the subscription.
 */
static void
ApplyLauncherSetWorkerStartTime(Oid subid, TimestampTz start_time)
{
	LauncherLastStartTimesEntry *entry;
	bool		found;

	logicalrep_launcher_attach_dshmem();

	entry = dshash_find_or_insert(last_start_times, &subid, &found);
	entry->last_start_time = start_time;
	dshash_release_lock(last_start_times, entry);
}

/*
 * Return the last-start time for the subscription, or 0 if there isn't one.
 */
static TimestampTz
ApplyLauncherGetWorkerStartTime(Oid subid)
{
	LauncherLastStartTimesEntry *entry;
	TimestampTz ret;

	logicalrep_launcher_attach_dshmem();

	entry = dshash_find(last_start_times, &subid, false);
	if (entry == NULL)
		return 0;

	ret = entry->last_start_time;
	dshash_release_lock(last_start_times, entry);

	return ret;
}

/*
 * Remove the last-start-time entry for the subscription, if one exists.
 *
 * This has two use-cases: to remove the entry related to a subscription
 * that's been deleted or disabled (just to avoid leaking shared memory),
 * and to allow immediate restart of an apply worker that has exited
 * due to subscription parameter changes.
 */
void
ApplyLauncherForgetWorkerStartTime(Oid subid)
{
	logicalrep_launcher_attach_dshmem();

	(void) dshash_delete_key(last_start_times, &subid);
}

/*
 * Wakeup the launcher on commit if requested.
 */
void
AtEOXact_ApplyLauncher(bool isCommit)
{
	if (isCommit)
	{
		if (on_commit_launcher_wakeup)
			ApplyLauncherWakeup();
	}

	on_commit_launcher_wakeup = false;
}

/*
 * Request wakeup of the launcher on commit of the transaction.
 *
 * This is used to send launcher signal to stop sleeping and process the
 * subscriptions when current transaction commits. Should be used when new
 * tuple was added to the pg_subscription catalog.
*/
void
ApplyLauncherWakeupAtCommit(void)
{
	if (!on_commit_launcher_wakeup)
		on_commit_launcher_wakeup = true;
}

static void
ApplyLauncherWakeup(void)
{
	if (LogicalRepCtx->launcher_pid != 0)
		kill(LogicalRepCtx->launcher_pid, SIGUSR1);
}

/*
 * Clean up slot-sync worker info.
 */
static void
slotsync_worker_cleanup(SlotSyncWorker *worker)
{
	Assert(LWLockHeldByMeInMode(SlotSyncWorkerLock, LW_EXCLUSIVE));

	worker->hdr.in_use = false;
	worker->hdr.proc = NULL;
	worker->slot = -1;

	if (DsaPointerIsValid(worker->dbids_dp))
	{
		dsa_free(worker->dbids_dsa, worker->dbids_dp);
		worker->dbids_dp = InvalidDsaPointer;
	}

	if (worker->dbids_dsa)
	{
		dsa_detach(worker->dbids_dsa);
		worker->dbids_dsa = NULL;
	}

	worker->dbcount = 0;

	MemSet(NameStr(worker->monitoring_info.slot_name), 0, NAMEDATALEN);
	worker->monitoring_info.confirmed_lsn = 0;
	worker->monitoring_info.last_update_time = 0;
}

/*
 * Attach Slot-sync worker to worker-slot assigned by launcher.
 */
void
slotsync_worker_attach(int slot)
{
	/* Block concurrent access. */
	LWLockAcquire(SlotSyncWorkerLock, LW_EXCLUSIVE);

	Assert(slot >= 0 && slot < max_slotsync_workers);
	MySlotSyncWorker = &LogicalRepCtx->ss_workers[slot];
	MySlotSyncWorker->slot = slot;

	if (!MySlotSyncWorker->hdr.in_use)
	{
		LWLockRelease(SlotSyncWorkerLock);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("replication slot-sync worker slot %d is "
						"empty, cannot attach", slot)));
	}

	if (MySlotSyncWorker->hdr.proc)
	{
		LWLockRelease(SlotSyncWorkerLock);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("replication slot-sync worker slot %d is "
						"already used by another worker, cannot attach", slot)));
	}

	MySlotSyncWorker->hdr.proc = MyProc;

	LWLockRelease(SlotSyncWorkerLock);
}

/*
 * Slot-Sync worker find.
 *
 * Walks the slot-sync workers pool and searches for one that matches given
 * dbid. Since one worker can manage multiple dbs, so it walks the db array in
 * each worker to find the match.
 */
static SlotSyncWorker *
slotsync_worker_find(Oid dbid)
{
	int			i;
	SlotSyncWorker *res = NULL;
	Oid		   *dbids;

	Assert(LWLockHeldByMeInMode(SlotSyncWorkerLock, LW_SHARED));

	/* Search for attached worker for a given dbid */
	for (i = 0; i < max_slotsync_workers; i++)
	{
		SlotSyncWorker *w = &LogicalRepCtx->ss_workers[i];
		int			cnt;

		if (!w->hdr.in_use)
			continue;

		dbids = (Oid *) dsa_get_address(w->dbids_dsa, w->dbids_dp);
		for (cnt = 0; cnt < w->dbcount; cnt++)
		{
			Oid			wdbid = dbids[cnt];

			if (wdbid == dbid)
			{
				res = w;
				break;
			}
		}

		/* If worker is found, break the outer loop */
		if (res)
			break;
	}

	return res;
}

/*
 * Setup DSA for slot-sync worker.
 *
 * DSA is needed for dbids array. Since max number of dbs a worker can manage
 * is not known, so initially fixed size to hold DB_PER_WORKER_ALLOC_INIT
 * dbs is allocated. If this size is exhausted, it can be extended using
 * dsa free and allocate routines.
 */
static dsa_handle
slotsync_dsa_setup(SlotSyncWorker *worker, int alloc_db_count)
{
	dsa_area   *dbids_dsa;
	dsa_pointer dbids_dp;
	dsa_handle	dbids_dsa_handle;
	MemoryContext oldcontext;

	/* Be sure any memory allocated by DSA routines is persistent. */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	dbids_dsa = dsa_create(LWTRANCHE_SLOTSYNC_DSA);
	dsa_pin(dbids_dsa);
	dsa_pin_mapping(dbids_dsa);

	dbids_dp = dsa_allocate0(dbids_dsa, alloc_db_count * sizeof(Oid));

	/* Set-up worker */
	worker->dbcount = 0;
	worker->dbids_dsa = dbids_dsa;
	worker->dbids_dp = dbids_dp;

	/* Get the handle. This is the one which can be passed to worker processes */
	dbids_dsa_handle = dsa_get_handle(dbids_dsa);

	ereport(DEBUG1,
			(errmsg("allocated dsa for slot-sync worker for dbcount: %d",
					alloc_db_count)));

	MemoryContextSwitchTo(oldcontext);

	return dbids_dsa_handle;
}

/*
 * Slot-sync worker launch or reuse
 *
 * Start new slot-sync background worker from the pool of available workers
 * going by max_slotsync_workers count. If the worker pool is exhausted,
 * reuse the existing worker with minimum number of dbs. The idea is to
 * always distribute the dbs equally among launched workers.
 * If initially allocated dbids array is exhausted for the selected worker,
 * reallocate the dbids array with increased size and copy the existing
 * dbids to it and assign the new one as well.
 *
 * Returns true on success, false on failure.
 */
static bool
slotsync_worker_launch_or_reuse(Oid dbid)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *bgw_handle;
	uint16		generation;
	SlotSyncWorker *worker = NULL;
	uint32		mindbcnt = 0;
	uint32		alloc_count = 0;
	uint32		copied_dbcnt = 0;
	Oid		   *copied_dbids = NULL;
	int			worker_slot = -1;
	dsa_handle	handle;
	Oid		   *dbids;
	int			i;
	bool		attach;

	Assert(OidIsValid(dbid));

	/*
	 * We need to do the modification of the shared memory under lock so that
	 * we have consistent view.
	 */
	LWLockAcquire(SlotSyncWorkerLock, LW_EXCLUSIVE);

	/* Find unused worker slot. */
	for (i = 0; i < max_slotsync_workers; i++)
	{
		SlotSyncWorker *w = &LogicalRepCtx->ss_workers[i];

		if (!w->hdr.in_use)
		{
			worker = w;
			worker_slot = i;
			break;
		}
	}

	/*
	 * If all the workers are currently in use. Find the one with minimum
	 * number of dbs and use that.
	 */
	if (!worker)
	{
		for (i = 0; i < max_slotsync_workers; i++)
		{
			SlotSyncWorker *w = &LogicalRepCtx->ss_workers[i];

			if (i == 0)
			{
				mindbcnt = w->dbcount;
				worker = w;
				worker_slot = i;
			}
			else if (w->dbcount < mindbcnt)
			{
				mindbcnt = w->dbcount;
				worker = w;
				worker_slot = i;
			}
		}
	}

	/*
	 * If worker is being reused, and there is vacancy in dbids array, just
	 * update dbids array and dbcount and we are done. But if dbids array is
	 * exhausted, reallocate dbids using dsa and copy the old dbids and assign
	 * the new one as well.
	 */
	if (worker->hdr.in_use)
	{
		dbids = (Oid *) dsa_get_address(worker->dbids_dsa, worker->dbids_dp);

		if (worker->dbcount < DB_PER_WORKER_ALLOC_INIT)
		{
			dbids[worker->dbcount++] = dbid;
		}
		else
		{
			MemoryContext oldcontext;

			/* Be sure any memory allocated by DSA routines is persistent. */
			oldcontext = MemoryContextSwitchTo(TopMemoryContext);

			/* Remember the old dbids before we reallocate dsa. */
			copied_dbcnt = worker->dbcount;
			copied_dbids = (Oid *) palloc0(worker->dbcount * sizeof(Oid));
			memcpy(copied_dbids, dbids, worker->dbcount * sizeof(Oid));

			alloc_count = copied_dbcnt + DB_PER_WORKER_ALLOC_EXTRA;

			/* Free the existing dbids and allocate new with increased size */
			if (DsaPointerIsValid(worker->dbids_dp))
				dsa_free(worker->dbids_dsa, worker->dbids_dp);

			worker->dbids_dp = dsa_allocate0(worker->dbids_dsa,
											 alloc_count * sizeof(Oid));

			dbids = (Oid *) dsa_get_address(worker->dbids_dsa, worker->dbids_dp);

			/* Copy the existing dbids */
			worker->dbcount = copied_dbcnt;
			memcpy(dbids, copied_dbids, copied_dbcnt * sizeof(Oid));

			/* Assign new dbid */
			dbids[worker->dbcount++] = dbid;

			MemoryContextSwitchTo(oldcontext);
		}

		LWLockRelease(SlotSyncWorkerLock);

		ereport(LOG,
				(errmsg("Added database %d to replication slot-sync "
						"worker %d; dbcount now: %d",
						dbid, worker_slot, worker->dbcount)));
		return true;
	}

	/* Prepare the new worker. */
	worker->hdr.launch_time = GetCurrentTimestamp();
	worker->hdr.in_use = true;

	/*
	 * 'proc' and 'slot' will be assigned in ReplSlotSyncWorkerMain when we
	 * attach this worker to a particular worker-pool slot
	 */
	worker->hdr.proc = NULL;
	worker->slot = -1;

	/* TODO: do we really need 'generation', analyse more here */
	worker->hdr.generation++;

	/* Initial DSA setup for dbids array to hold DB_PER_WORKER_ALLOC_INIT dbs */
	handle = slotsync_dsa_setup(worker, DB_PER_WORKER_ALLOC_INIT);
	dbids = (Oid *) dsa_get_address(worker->dbids_dsa, worker->dbids_dp);

	dbids[worker->dbcount++] = dbid;

	/* Before releasing lock, remember generation for future identification. */
	generation = worker->hdr.generation;

	LWLockRelease(SlotSyncWorkerLock);

	/* Register the new dynamic worker. */
	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_ConsistentState;
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");

	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ReplSlotSyncWorkerMain");

	Assert(worker_slot >= 0);
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "replication slot-sync worker %d", worker_slot);

	snprintf(bgw.bgw_type, BGW_MAXLEN, "slot-sync worker");

	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_notify_pid = MyProcPid;
	bgw.bgw_main_arg = Int32GetDatum(worker_slot);

	memcpy(bgw.bgw_extra, &handle, sizeof(dsa_handle));

	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
	{
		/* Failed to start worker, so clean up the worker slot. */
		LWLockAcquire(SlotSyncWorkerLock, LW_EXCLUSIVE);
		Assert(generation == worker->hdr.generation);
		slotsync_worker_cleanup(worker);
		LWLockRelease(SlotSyncWorkerLock);

		ereport(WARNING,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("out of background worker slots"),
				 errhint("You might need to increase %s.", "max_worker_processes")));
		return false;
	}

	/* Now wait until it attaches. */
	attach = WaitForReplicationWorkerAttach((LogicalWorkerHeader *) worker,
											generation,
											bgw_handle,
											SlotSyncWorkerLock);
	if (!attach)
		ereport(WARNING,
				(errmsg("Replication slot-sync worker failed to attach to "
						"worker-pool slot %d", worker_slot)));

	/* Attach is done, now safe to log that the worker is managing dbid */
	if (attach)
		ereport(LOG,
				(errmsg("Added database %d to replication slot-sync "
						"worker %d; dbcount now: %d",
						dbid, worker_slot, worker->dbcount)));
	return attach;
}

/*
 * Internal function to stop the slot-sync worker and wait until it detaches
 * from the slot-sync worker-pool slot.
 */
static void
slotsync_worker_stop_internal(SlotSyncWorker *worker)
{
	int			slot = worker->slot;

	LWLockAcquire(SlotSyncWorkerLock, LW_SHARED);
	ereport(LOG,
			(errmsg("Stopping replication slot-sync worker %d",
					slot)));
	logicalrep_worker_stop_internal((LogicalWorkerHeader *) worker,
									SIGINT,
									SlotSyncWorkerLock);
	LWLockRelease(SlotSyncWorkerLock);

	LWLockAcquire(SlotSyncWorkerLock, LW_EXCLUSIVE);
	slotsync_worker_cleanup(worker);
	LWLockRelease(SlotSyncWorkerLock);
}

/*
 * Stop all the slot-sync workers in use.
 */
static void
slotsync_workers_stop()
{
	int			widx;

	for (widx = 0; widx < max_slotsync_workers; widx++)
	{
		SlotSyncWorker *worker = &LogicalRepCtx->ss_workers[widx];

		if (worker->hdr.in_use)
			slotsync_worker_stop_internal(worker);
	}
}


/*
 * Slot-sync workers remove obsolete DBs from db-list
 *
 * If the DBIds fetched from the primary are lesser than the ones being managed
 * by slot-sync workers, remove extra dbs from worker's db-list. This may happen
 * if some slots are removed on primary but 'synchronize_slot_names' has not
 * been changed yet.
 */
static void
slotsync_remove_obsolete_dbs(List *remote_dbs)
{
	ListCell   *lc;
	Oid		   *dbids;
	int			widx;
	int			dbidx;
	int			i;

	LWLockAcquire(SlotSyncWorkerLock, LW_EXCLUSIVE);

	/* Traverse slot-sync-workers to validate the DBs */
	for (widx = 0; widx < max_slotsync_workers; widx++)
	{
		SlotSyncWorker *worker = &LogicalRepCtx->ss_workers[widx];

		if (!worker->hdr.in_use)
			continue;

		dbids = (Oid *) dsa_get_address(worker->dbids_dsa, worker->dbids_dp);

		for (dbidx = 0; dbidx < worker->dbcount;)
		{
			Oid			wdbid = dbids[dbidx];
			bool		found = false;

			/* Check if current DB is still present in remote-db-list */
			foreach(lc, remote_dbs)
			{
				WalRcvRepSlotDbData *slot_db_data = lfirst(lc);

				if (slot_db_data->database == wdbid)
				{
					found = true;
					break;
				}
			}

			/* If not found, then delete this db from worker's db-list */
			if (!found)
			{
				for (i = dbidx; i < worker->dbcount; i++)
				{
					/* Shift the DBs and get rid of wdbid */
					if (i < (worker->dbcount - 1))
						dbids[i] = dbids[i + 1];
				}

				worker->dbcount--;

				ereport(LOG,
						(errmsg("Removed database %d from replication slot-sync "
								"worker %d; dbcount now: %d",
								wdbid, worker->slot, worker->dbcount)));
			}

			/* Else move to next db-position */
			else
			{
				dbidx++;
			}
		}
	}

	LWLockRelease(SlotSyncWorkerLock);

	/* If dbcount for any worker has become 0, shut it down */
	for (widx = 0; widx < max_slotsync_workers; widx++)
	{
		SlotSyncWorker *worker = &LogicalRepCtx->ss_workers[widx];

		if (worker->hdr.in_use && !worker->dbcount)
			slotsync_worker_stop_internal(worker);
	}
}

/*
 * Connect to primary server for slotsync purpose and return the connection
 * info. Disconnect previous connection if provided in wrconn_prev.
 */
static WalReceiverConn *
primary_connect(WalReceiverConn *wrconn_prev)
{
	WalReceiverConn *wrconn = NULL;
	char	   *err;
	char	   *dbname;

	if (wrconn_prev)
		walrcv_disconnect(wrconn_prev);

	if (!RecoveryInProgress())
		return NULL;

	if (max_slotsync_workers == 0)
		return NULL;

	if (strcmp(synchronize_slot_names, "") == 0)
		return NULL;

	/* The primary_slot_name is not set */
	if (!WalRcv || WalRcv->slotname[0] == '\0')
	{
		ereport(WARNING,
				errmsg("Skipping slots synchronization as primary_slot_name "
					   "is not set."));
		return NULL;
	}

	/* The hot_standby_feedback must be ON for slot-sync to work */
	if (!hot_standby_feedback)
	{
		ereport(WARNING,
				errmsg("Skipping slots synchronization as hot_standby_feedback "
					   "is off."));
		return NULL;
	}

	/* The dbname must be specified in primary_conninfo for slot-sync to work */
	dbname = walrcv_get_dbname_from_conninfo(PrimaryConnInfo);
	if (dbname == NULL)
	{
		ereport(WARNING,
				errmsg("Skipping slots synchronization as dbname is not "
					   "specified in primary_conninfo."));
		return NULL;
	}

	wrconn = walrcv_connect(PrimaryConnInfo, false, false,
							"Logical Replication Launcher", &err);
	if (!wrconn)
		ereport(ERROR,
				(errmsg("could not connect to the primary server: %s", err)));

	return wrconn;
}

/*
 * Save current slot-sync configurations.
 *
 * This function is invoked prior to each config-reload on receiving SIGHUP.
 */
static void
SaveCurrentSlotSyncConfigs()
{
	PrimaryConnInfoPreReload = pstrdup(PrimaryConnInfo);
	PrimarySlotNamePreReload = pstrdup(WalRcv->slotname);
	SyncSlotNamesPreReload = pstrdup(synchronize_slot_names);
}

/*
 * Returns true if any of the slot-sync configurations changed.
 */
static bool
SlotSyncConfigsChanged()
{
	if (strcmp(PrimaryConnInfoPreReload, PrimaryConnInfo) != 0)
		return true;

	if (strcmp(PrimarySlotNamePreReload, WalRcv->slotname) != 0)
		return true;

	if (strcmp(SyncSlotNamesPreReload, synchronize_slot_names) != 0)
		return true;

	/*
	 * If we have reached this stage, it means original value of
	 * hot_standby_feedback was 'true', so consider it changed if 'false' now.
	 */
	if (!hot_standby_feedback)
		return true;

	return false;
}

/*
 * Start slot-sync background workers.
 *
 * It connects to primary, get the list of DBIDs for slots configured in
 * synchronize_slot_names. It then launces the slot-sync workers as per
 * max_slotsync_workers and then assign the DBs equally to the workers
 * launched.
 */
static void
ApplyLauncherStartSlotSync(long *wait_time, WalReceiverConn *wrconn)
{
	List	   *slots_dbs;
	ListCell   *lc;
	MemoryContext tmpctx;
	MemoryContext oldctx;

	/* If connection is NULL due to lack of correct configurations, return */
	if (!wrconn)
		return;

	/* Use temporary context for the slot list and worker info. */
	tmpctx = AllocSetContextCreate(TopMemoryContext,
								   "Logical Replication Launcher slot-sync ctx",
								   ALLOCSET_DEFAULT_SIZES);
	oldctx = MemoryContextSwitchTo(tmpctx);

	slots_dbs = walrcv_get_dbinfo_for_logical_slots(wrconn,
													synchronize_slot_names);

	slotsync_remove_obsolete_dbs(slots_dbs);

	foreach(lc, slots_dbs)
	{
		WalRcvRepSlotDbData *slot_db_data = lfirst(lc);
		SlotSyncWorker *w;
		TimestampTz last_launch_tried;
		TimestampTz now;
		long		elapsed;

		if (!OidIsValid(slot_db_data->database))
			continue;

		LWLockAcquire(SlotSyncWorkerLock, LW_SHARED);
		w = slotsync_worker_find(slot_db_data->database);
		LWLockRelease(SlotSyncWorkerLock);

		if (w != NULL)
			continue;			/* worker is running already */

		/*
		 * If the worker is eligible to start now, launch it. Otherwise,
		 * adjust wait_time so that we'll wake up as soon as it can be
		 * started.
		 *
		 * Each apply worker can only be restarted once per
		 * wal_retrieve_retry_interval, so that errors do not cause us to
		 * repeatedly restart the worker as fast as possible.
		 */
		last_launch_tried = slot_db_data->last_launch_time;
		now = GetCurrentTimestamp();
		if (last_launch_tried == 0 ||
			(elapsed = TimestampDifferenceMilliseconds(last_launch_tried, now)) >=
			wal_retrieve_retry_interval)
		{
			slot_db_data->last_launch_time = now;

			slotsync_worker_launch_or_reuse(slot_db_data->database);
		}
		else
		{
			*wait_time = Min(*wait_time,
							 wal_retrieve_retry_interval - elapsed);
		}
	}

	/* Switch back to original memory context. */
	MemoryContextSwitchTo(oldctx);
	/* Clean the temporary memory. */
	MemoryContextDelete(tmpctx);
}

/*
 * Start logical replication apply workers for enabled subscriptions.
 */
static void
ApplyLauncherStartSubs(long *wait_time)
{
	List	   *sublist;
	ListCell   *lc;
	MemoryContext subctx;
	MemoryContext oldctx;

	/* Use temporary context to avoid leaking memory across cycles. */
	subctx = AllocSetContextCreate(TopMemoryContext,
								   "Logical Replication Launcher sublist",
								   ALLOCSET_DEFAULT_SIZES);
	oldctx = MemoryContextSwitchTo(subctx);

	/* Start any missing workers for enabled subscriptions. */
	sublist = get_subscription_list();
	foreach(lc, sublist)
	{
		Subscription *sub = (Subscription *) lfirst(lc);
		LogicalRepWorker *w;
		TimestampTz last_start;
		TimestampTz now;
		long		elapsed;

		if (!sub->enabled)
			continue;

		LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
		w = logicalrep_worker_find(sub->oid, InvalidOid, false);
		LWLockRelease(LogicalRepWorkerLock);

		if (w != NULL)
			continue;			/* worker is running already */

		/*
		 * If the worker is eligible to start now, launch it.  Otherwise,
		 * adjust wait_time so that we'll wake up as soon as it can be
		 * started.
		 *
		 * Each subscription's apply worker can only be restarted once per
		 * wal_retrieve_retry_interval, so that errors do not cause us to
		 * repeatedly restart the worker as fast as possible.  In cases where
		 * a restart is expected (e.g., subscription parameter changes),
		 * another process should remove the last-start entry for the
		 * subscription so that the worker can be restarted without waiting
		 * for wal_retrieve_retry_interval to elapse.
		 */
		last_start = ApplyLauncherGetWorkerStartTime(sub->oid);
		now = GetCurrentTimestamp();
		if (last_start == 0 ||
			(elapsed = TimestampDifferenceMilliseconds(last_start, now)) >= wal_retrieve_retry_interval)
		{
			ApplyLauncherSetWorkerStartTime(sub->oid, now);
			logicalrep_worker_launch(WORKERTYPE_APPLY,
									 sub->dbid, sub->oid, sub->name,
									 sub->owner, InvalidOid,
									 DSM_HANDLE_INVALID);
		}
		else
		{
			*wait_time = Min(*wait_time,
							 wal_retrieve_retry_interval - elapsed);
		}
	}

	/* Switch back to original memory context. */
	MemoryContextSwitchTo(oldctx);
	/* Clean the temporary memory. */
	MemoryContextDelete(subctx);
}

/*
 * Main loop for the apply launcher process.
 */
void
ApplyLauncherMain(Datum main_arg)
{
	WalReceiverConn *wrconn = NULL;

	ereport(DEBUG1,
			(errmsg_internal("logical replication launcher started")));

	before_shmem_exit(logicalrep_launcher_onexit, (Datum) 0);

	Assert(LogicalRepCtx->launcher_pid == 0);
	LogicalRepCtx->launcher_pid = MyProcPid;

	/* Establish signal handlers. */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/*
	 * Establish connection to nailed catalogs (we only ever access
	 * pg_subscription).
	 */
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	load_file("libpqwalreceiver", false);

	wrconn = primary_connect(NULL);

	/* Enter main loop */
	for (;;)
	{
		int			rc;
		long		wait_time = DEFAULT_NAPTIME_PER_CYCLE;

		CHECK_FOR_INTERRUPTS();

		if (!RecoveryInProgress())
			ApplyLauncherStartSubs(&wait_time);
		else
			ApplyLauncherStartSlotSync(&wait_time, wrconn);

		/* Wait for more work. */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   wait_time,
					   WAIT_EVENT_LOGICAL_LAUNCHER_MAIN);

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}

		if (ConfigReloadPending)
		{
			bool		ssConfigChanged = false;

			SaveCurrentSlotSyncConfigs();

			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);

			/*
			 * Stop the slot-sync workers if any of the related GUCs changed.
			 * These will be relaunched as per the new values during next
			 * sync-cycle.
			 */
			ssConfigChanged = SlotSyncConfigsChanged();
			if (ssConfigChanged)
				slotsync_workers_stop();

			/* Reconnect in case primary_conninfo has changed */
			wrconn = primary_connect(wrconn);
		}
	}

	/* Not reachable */
}

/*
 * Is current process the logical replication launcher?
 */
bool
IsLogicalLauncher(void)
{
	return LogicalRepCtx->launcher_pid == MyProcPid;
}

/*
 * Return the pid of the leader apply worker if the given pid is the pid of a
 * parallel apply worker, otherwise, return InvalidPid.
 */
pid_t
GetLeaderApplyWorkerPid(pid_t pid)
{
	int			leader_pid = InvalidPid;
	int			i;

	LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);

	for (i = 0; i < max_logical_replication_workers; i++)
	{
		LogicalRepWorker *w = &LogicalRepCtx->workers[i];

		if (isParallelApplyWorker(w) && w->hdr.proc &&
			pid == w->hdr.proc->pid)
		{
			leader_pid = w->leader_pid;
			break;
		}
	}

	LWLockRelease(LogicalRepWorkerLock);

	return leader_pid;
}

/*
 * Returns state of the subscriptions.
 */
Datum
pg_stat_get_subscription(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_SUBSCRIPTION_COLS	10
	Oid			subid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	int			i;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	/* Make sure we get consistent view of the workers. */
	LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);

	for (i = 0; i < max_logical_replication_workers; i++)
	{
		/* for each row */
		Datum		values[PG_STAT_GET_SUBSCRIPTION_COLS] = {0};
		bool		nulls[PG_STAT_GET_SUBSCRIPTION_COLS] = {0};
		int			worker_pid;
		LogicalRepWorker worker;

		memcpy(&worker, &LogicalRepCtx->workers[i],
			   sizeof(LogicalRepWorker));
		if (!worker.hdr.proc || !IsBackendPid(worker.hdr.proc->pid))
			continue;

		if (OidIsValid(subid) && worker.subid != subid)
			continue;

		worker_pid = worker.hdr.proc->pid;

		values[0] = ObjectIdGetDatum(worker.subid);
		if (isTablesyncWorker(&worker))
			values[1] = ObjectIdGetDatum(worker.relid);
		else
			nulls[1] = true;
		values[2] = Int32GetDatum(worker_pid);

		if (isParallelApplyWorker(&worker))
			values[3] = Int32GetDatum(worker.leader_pid);
		else
			nulls[3] = true;

		if (XLogRecPtrIsInvalid(worker.last_lsn))
			nulls[4] = true;
		else
			values[4] = LSNGetDatum(worker.last_lsn);
		if (worker.last_send_time == 0)
			nulls[5] = true;
		else
			values[5] = TimestampTzGetDatum(worker.last_send_time);
		if (worker.last_recv_time == 0)
			nulls[6] = true;
		else
			values[6] = TimestampTzGetDatum(worker.last_recv_time);
		if (XLogRecPtrIsInvalid(worker.reply_lsn))
			nulls[7] = true;
		else
			values[7] = LSNGetDatum(worker.reply_lsn);
		if (worker.reply_time == 0)
			nulls[8] = true;
		else
			values[8] = TimestampTzGetDatum(worker.reply_time);

		switch (worker.type)
		{
			case WORKERTYPE_APPLY:
				values[9] = CStringGetTextDatum("apply");
				break;
			case WORKERTYPE_PARALLEL_APPLY:
				values[9] = CStringGetTextDatum("parallel apply");
				break;
			case WORKERTYPE_TABLESYNC:
				values[9] = CStringGetTextDatum("table synchronization");
				break;
			case WORKERTYPE_UNKNOWN:
				/* Should never happen. */
				elog(ERROR, "unknown worker type");
		}

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);

		/*
		 * If only a single subscription was requested, and we found it,
		 * break.
		 */
		if (OidIsValid(subid))
			break;
	}

	LWLockRelease(LogicalRepWorkerLock);

	return (Datum) 0;
}
