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

	/* Indicates if this slot is used for an apply background worker. */
	bool		subworker;

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
 * Status of apply background worker.
 */
typedef enum ApplyBgworkerStatus
{
	APPLY_BGWORKER_BUSY = 0,		/* assigned to a transaction */
	APPLY_BGWORKER_FINISHED,		/* transaction is completed */
	APPLY_BGWORKER_EXIT				/* exit */
} ApplyBgworkerStatus;

/*
 * Struct for sharing information between apply main and apply background
 * workers.
 */
typedef struct ApplyBgworkerShared
{
	slock_t	mutex;

	/* Status of apply background worker. */
	ApplyBgworkerStatus	status;

	/* server version of publisher. */
	uint32	server_version;

	TransactionId	stream_xid;
	uint32	n;	/* id of apply background worker */
} ApplyBgworkerShared;

/*
 * Struct for maintaining an apply background worker.
 */
typedef struct ApplyBgworkerState
{
	shm_mq_handle			*mq_handle;
	dsm_segment				*dsm_seg;
	ApplyBgworkerShared volatile	*shared;
} ApplyBgworkerState;

/* Main memory context for apply worker. Permanent during worker lifetime. */
extern PGDLLIMPORT MemoryContext ApplyContext;
extern PGDLLIMPORT MemoryContext ApplyMessageContext;

extern PGDLLIMPORT ApplyErrorCallbackArg apply_error_callback_arg;

extern PGDLLIMPORT bool MySubscriptionValid;

extern PGDLLIMPORT volatile ApplyBgworkerShared *MyParallelShared;

extern PGDLLIMPORT List *subxactlist;

/* libpqreceiver connection */
extern PGDLLIMPORT struct WalReceiverConn *LogRepWorkerWalRcvConn;

/* Worker and subscription objects. */
extern PGDLLIMPORT Subscription *MySubscription;
extern PGDLLIMPORT LogicalRepWorker *MyLogicalRepWorker;

extern PGDLLIMPORT bool in_remote_transaction;
extern PGDLLIMPORT bool in_streamed_transaction;
extern PGDLLIMPORT TransactionId stream_xid;

extern void logicalrep_worker_attach(int slot);
extern LogicalRepWorker *logicalrep_worker_find(Oid subid, Oid relid,
												bool only_running);
extern List *logicalrep_workers_find(Oid subid, bool only_running);
extern bool logicalrep_worker_launch(Oid dbid, Oid subid, const char *subname,
									 Oid userid, Oid relid,
									 dsm_handle subworker_dsm);
extern void logicalrep_worker_stop(Oid subid, Oid relid);
extern void logicalrep_worker_wakeup(Oid subid, Oid relid);
extern void logicalrep_worker_wakeup_ptr(LogicalRepWorker *worker);

extern int	logicalrep_sync_worker_count(Oid subid);
extern int	logicalrep_apply_bgworker_count(Oid subid);

extern void ReplicationOriginNameForTablesync(Oid suboid, Oid relid,
											  char *originname, int szorgname);
extern char *LogicalRepSyncTableStart(XLogRecPtr *origin_startpos);

extern bool AllTablesyncsReady(void);
extern void UpdateTwoPhaseState(Oid suboid, char new_state);

extern void process_syncing_tables(XLogRecPtr current_lsn);
extern void invalidate_syncing_table_states(Datum arg, int cacheid,
											uint32 hashvalue);

extern void UpdateWorkerStats(XLogRecPtr last_lsn, TimestampTz send_time,
							  bool reply);

extern void apply_dispatch(StringInfo s);

/* Function for apply error callback */
extern void apply_error_callback(void *arg);

extern void subscription_change_cb(Datum arg, int cacheid, uint32 hashvalue);

/* Apply background worker setup and interactions */
extern ApplyBgworkerState *apply_bgworker_start(TransactionId xid);
extern ApplyBgworkerState *apply_bgworker_find(TransactionId xid);
extern void apply_bgworker_wait_for(ApplyBgworkerState *wstate,
									ApplyBgworkerStatus wait_for_status);
extern void apply_bgworker_send_data(ApplyBgworkerState *wstate, Size nbytes,
									 const void *data);
extern void apply_bgworker_free(ApplyBgworkerState *wstate);
extern void apply_bgworker_check_status(void);
extern void apply_bgworker_set_status(ApplyBgworkerStatus status);
extern void apply_bgworker_subxact_info_add(TransactionId current_xid);
extern void apply_bgworker_relation_check(LogicalRepRelMapEntry *rel);

static inline bool
am_tablesync_worker(void)
{
	return OidIsValid(MyLogicalRepWorker->relid);
}

static inline bool
am_apply_bgworker(void)
{
	return MyLogicalRepWorker->subworker;
}

#endif							/* WORKER_INTERNAL_H */
