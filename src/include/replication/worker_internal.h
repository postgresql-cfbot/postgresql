/*-------------------------------------------------------------------------
 *
 * worker_internal.h
 *	  Internal headers shared by logical replication workers.
 *
 * Portions Copyright (c) 2016-2022, PostgreSQL Global Development Group
 *
 * src/include/replication/worker_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef WORKER_INTERNAL_H
#define WORKER_INTERNAL_H

#include <signal.h>

#include "access/xlogdefs.h"
#include "catalog/pg_subscription.h"
#include "datatype/timestamp.h"
#include "miscadmin.h"
#include "replication/logicalrelation.h"
#include "storage/fileset.h"
#include "storage/lock.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "storage/spin.h"


typedef struct LogicalRepWorker
{
	/* Time at which this worker was launched. */
	TimestampTz launch_time;

	/* Indicates if this slot is used or free. */
	bool		in_use;

	/* Increased every time the slot is taken by new worker. */
	uint16		generation;

	/* Pointer to proc array. NULL if not running. */
	PGPROC	   *proc;

	/* Database id to connect to. */
	Oid			dbid;

	/* User to use for connection (will be same as owner of subscription). */
	Oid			userid;

	/* Subscription id for the worker. */
	Oid			subid;

	/* Used for initial table synchronization. */
	Oid			relid;
	char		relstate;
	XLogRecPtr	relstate_lsn;
	slock_t		relmutex;

	/*
	 * Used to create the changes and subxact files for the streaming
	 * transactions.  Upon the arrival of the first streaming transaction, the
	 * fileset will be initialized, and it will be deleted when the worker
	 * exits.  Under this, separate buffiles would be created for each
	 * transaction which will be deleted after the transaction is finished.
	 */
	FileSet    *stream_fileset;

	/*
	 * PID of leader apply worker if this slot is used for a parallel apply
	 * worker, InvalidPid otherwise.
	 */
	pid_t		apply_leader_pid;

	/* Indicates whether apply can be performed in parallel. */
	bool		parallel_apply;

	/* Stats. */
	XLogRecPtr	last_lsn;
	TimestampTz last_send_time;
	TimestampTz last_recv_time;
	XLogRecPtr	reply_lsn;
	TimestampTz reply_time;
} LogicalRepWorker;

/* Struct for saving and restoring apply errcontext information */
typedef struct ApplyErrorCallbackArg
{
	LogicalRepMsgType command;	/* 0 if invalid */
	LogicalRepRelMapEntry *rel;

	/* Remote node information */
	int			remote_attnum;	/* -1 if invalid */
	TransactionId remote_xid;
	XLogRecPtr	finish_lsn;
	char	   *origin_name;
} ApplyErrorCallbackArg;

/*
 * Struct for sharing information between leader apply worker and parallel
 * apply workers.
 */
typedef struct ParallelApplyWorkerShared
{
	slock_t		mutex;

	/*
	 * Flag used to ensure commit ordering.
	 *
	 * The parallel apply worker will set it to false after handling the
	 * transaction finish commands while the apply leader will wait for it to
	 * become false before proceeding in transaction finish commands (e.g.
	 * STREAM_COMMIT/STREAM_ABORT/STREAM_PREPARE).
	 */
	bool		in_parallel_apply_xact;

	/* Information from the corresponding LogicalRepWorker slot. */
	uint16		logicalrep_worker_generation;

	int			logicalrep_worker_slot_no;
} ParallelApplyWorkerShared;

/*
 * Information which is used to manage the parallel apply worker.
 */
typedef struct ParallelApplyWorkerInfo
{
	shm_mq_handle *mq_handle;

	/*
	 * The queue used to transfer messages from the parallel apply worker to
	 * the leader apply worker. NULL if the parallel apply worker exited
	 * cleanly.
	 */
	shm_mq_handle *error_mq_handle;

	dsm_segment *dsm_seg;

	/*
	 * True if the worker is being used to process a parallel apply
	 * transaction. False indicates this worker is available for re-use.
	 */
	bool		in_use;

	ParallelApplyWorkerShared *shared;
} ParallelApplyWorkerInfo;

/* Main memory context for apply worker. Permanent during worker lifetime. */
extern PGDLLIMPORT MemoryContext ApplyContext;

extern PGDLLIMPORT MemoryContext ApplyMessageContext;

extern PGDLLIMPORT ErrorContextCallback *apply_error_context_stack;
extern PGDLLIMPORT ApplyErrorCallbackArg apply_error_callback_arg;

extern PGDLLIMPORT bool MySubscriptionValid;

extern PGDLLIMPORT ParallelApplyWorkerShared *MyParallelShared;

extern PGDLLIMPORT List *subxactlist;

/* libpqreceiver connection */
extern PGDLLIMPORT struct WalReceiverConn *LogRepWorkerWalRcvConn;

/* Worker and subscription objects. */
extern PGDLLIMPORT Subscription *MySubscription;
extern PGDLLIMPORT LogicalRepWorker *MyLogicalRepWorker;

extern PGDLLIMPORT bool in_remote_transaction;
extern PGDLLIMPORT ParallelApplyWorkerInfo *stream_apply_worker;

extern void logicalrep_worker_attach(int slot);
extern LogicalRepWorker *logicalrep_worker_find(Oid subid, Oid relid,
												bool only_running);
extern List *logicalrep_workers_find(Oid subid, bool only_running);
extern bool logicalrep_worker_launch(Oid dbid, Oid subid, const char *subname,
									 Oid userid, Oid relid,
									 dsm_handle subworker_dsm);
extern void logicalrep_worker_stop(Oid subid, Oid relid);
extern void logicalrep_worker_stop_by_slot(int slot_no, uint16 generation);
extern void logicalrep_worker_wakeup(Oid subid, Oid relid);
extern void logicalrep_worker_wakeup_ptr(LogicalRepWorker *worker);

extern int	logicalrep_sync_worker_count(Oid subid);
extern int	logicalrep_parallel_apply_worker_count(Oid subid);

extern void ReplicationOriginNameForLogicalRep(Oid suboid, Oid relid,
											   char *originname, Size szoriginname);
extern char *LogicalRepSyncTableStart(XLogRecPtr *origin_startpos);

extern bool AllTablesyncsReady(void);
extern void UpdateTwoPhaseState(Oid suboid, char new_state);

extern void process_syncing_tables(XLogRecPtr current_lsn);
extern void invalidate_syncing_table_states(Datum arg, int cacheid,
											uint32 hashvalue);

extern void apply_dispatch(StringInfo s);

extern void InitializeApplyWorker(void);

/* Function for apply error callback */
extern void apply_error_callback(void *arg);

/* Parallel apply worker setup and interactions */
extern void parallel_apply_start_worker(TransactionId xid);
extern ParallelApplyWorkerInfo *parallel_apply_find_worker(TransactionId xid);
extern void parallel_apply_set_in_xact(ParallelApplyWorkerShared *wshared,
									   bool in_xact);
extern void parallel_apply_free_worker(ParallelApplyWorkerInfo *winfo,
									   TransactionId xid);
extern void parallel_apply_wait_for_xact_finish(ParallelApplyWorkerInfo *winfo);
extern void parallel_apply_send_data(ParallelApplyWorkerInfo *winfo, Size nbytes,
									 const void *data);

extern void parallel_apply_start_subtrans(TransactionId current_xid, TransactionId top_xid);
extern void parallel_apply_stream_abort(LogicalRepStreamAbortData *abort_data);
extern void parallel_apply_replorigin_setup(void);
extern void parallel_apply_replorigin_reset(void);

extern void parallel_apply_relation_check(LogicalRepRelMapEntry *rel);

#define isParallelApplyWorker(worker) ((worker)->apply_leader_pid != InvalidPid)

static inline bool
am_tablesync_worker(void)
{
	return OidIsValid(MyLogicalRepWorker->relid);
}

static inline bool
am_leader_apply_worker(void)
{
	return (!OidIsValid(MyLogicalRepWorker->relid) &&
			!isParallelApplyWorker(MyLogicalRepWorker));
}

static inline bool
am_parallel_apply_worker(void)
{
	return isParallelApplyWorker(MyLogicalRepWorker);
}

#endif							/* WORKER_INTERNAL_H */
