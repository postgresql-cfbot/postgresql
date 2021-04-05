/* ----------
 *	pgstat.h
 *
 *	Definitions for the PostgreSQL activity statistics facility.
 *
 *	Copyright (c) 2001-2021, PostgreSQL Global Development Group
 *
 *	src/include/pgstat.h
 * ----------
 */
#ifndef PGSTAT_H
#define PGSTAT_H

#include "datatype/timestamp.h"
#include "executor/instrument.h"
#include "portability/instr_time.h"
#include "postmaster/pgarch.h" /* for MAX_XFN_CHARS */
#include "utils/backend_progress.h" /* for backward compatibility */
#include "utils/backend_status.h" /* for backward compatibility */
#include "utils/relcache.h"
#include "utils/wait_event.h" /* for backward compatibility */


/* ----------
 * Paths for the statistics files (relative to installation's $PGDATA).
 * ----------
 */
#define PGSTAT_STAT_PERMANENT_DIRECTORY		"pg_stat"
#define PGSTAT_STAT_PERMANENT_FILENAME		"pg_stat/saved_stats"
#define PGSTAT_STAT_PERMANENT_TMPFILE		"pg_stat/saved_stats.tmp"

/* Default directory to store temporary statistics data in */
#define PG_STAT_TMP_DIR		"pg_stat_tmp"

/* The types of statistics entries */
typedef enum PgStatKind
{
	/* stats with a variable number of entries */
	PGSTAT_KIND_DB,				/* database-wide statistics */
	PGSTAT_KIND_TABLE,			/* per-table statistics */
	PGSTAT_KIND_FUNCTION,		/* per-function statistics */

	/* stats with a constant number of entries */
	PGSTAT_KIND_ARCHIVER,
	PGSTAT_KIND_BGWRITER,
	PGSTAT_KIND_CHECKPOINTER,
	PGSTAT_KIND_REPLSLOT,
	PGSTAT_KIND_SLRU,
	PGSTAT_KIND_WAL,
} PgStatKind;
#define PGSTAT_KIND_LAST PGSTAT_KIND_WAL

/* Values for track_functions GUC variable --- order is significant! */
typedef enum TrackFunctionsLevel
{
	TRACK_FUNC_OFF,
	TRACK_FUNC_PL,
	TRACK_FUNC_ALL
}			TrackFunctionsLevel;

typedef enum PgStatsFetchConsistency
{
	STATS_FETCH_CONSISTENCY_NONE,
	STATS_FETCH_CONSISTENCY_CACHE,
	STATS_FETCH_CONSISTENCY_SNAPSHOT,
} PgStatsFetchConsistency;

/* Values to track the cause of session termination */
typedef enum SessionEndType
{
	DISCONNECT_NOT_YET,			/* still active */
	DISCONNECT_NORMAL,
	DISCONNECT_CLIENT_EOF,
	DISCONNECT_FATAL,
	DISCONNECT_KILLED
} SessionEndType;

/* ----------
 * The data type used for counters.
 * ----------
 */
typedef int64 PgStat_Counter;

/* Possible targets for resetting cluster-wide shared values */
typedef enum PgStat_Shared_Reset_Target
{
	RESET_ARCHIVER,
	RESET_BGWRITER,
	RESET_WAL
} PgStat_Shared_Reset_Target;

/* Possible object types for resetting single counters */
typedef enum PgStat_Single_Reset_Type
{
	RESET_TABLE,
	RESET_FUNCTION
} PgStat_Single_Reset_Type;


/* ------------------------------------------------------------
 * Structures kept in backend local memory while accumulating counts
 * ------------------------------------------------------------
 */

/* ----------
 * PgStat_TableCounts			The actual per-table counts kept by a backend
 *
 * This struct should contain only actual event counters, because we memcmp
 * it against zeroes to detect whether there are any stats updates to apply.
 * It is a component of PgStat_TableStatus (within-backend state).
 *
 * Note: for a table, tuples_returned is the number of tuples successfully
 * fetched by heap_getnext, while tuples_fetched is the number of tuples
 * successfully fetched by heap_fetch under the control of bitmap indexscans.
 * For an index, tuples_returned is the number of index entries returned by
 * the index AM, while tuples_fetched is the number of tuples successfully
 * fetched by heap_fetch under the control of simple indexscans for this index.
 *
 * tuples_inserted/updated/deleted/hot_updated count attempted actions,
 * regardless of whether the transaction committed.  delta_live_tuples,
 * delta_dead_tuples, and changed_tuples are set depending on commit or abort.
 * Note that delta_live_tuples and delta_dead_tuples can be negative!
 * ----------
 */
typedef struct PgStat_TableCounts
{
	PgStat_Counter t_numscans;

	PgStat_Counter t_tuples_returned;
	PgStat_Counter t_tuples_fetched;

	PgStat_Counter t_tuples_inserted;
	PgStat_Counter t_tuples_updated;
	PgStat_Counter t_tuples_deleted;
	PgStat_Counter t_tuples_hot_updated;
	bool		t_truncdropped;

	PgStat_Counter t_delta_live_tuples;
	PgStat_Counter t_delta_dead_tuples;
	PgStat_Counter t_changed_tuples;

	PgStat_Counter t_blocks_fetched;
	PgStat_Counter t_blocks_hit;
} PgStat_TableCounts;

/* ----------
 * PgStat_TableStatus			Per-table status within a backend
 *
 * Many of the event counters are nontransactional, ie, we count events
 * in committed and aborted transactions alike.  For these, we just count
 * directly in the PgStat_TableStatus.  However, delta_live_tuples,
 * delta_dead_tuples, and changed_tuples must be derived from event counts
 * with awareness of whether the transaction or subtransaction committed or
 * aborted.  Hence, we also keep a stack of per-(sub)transaction status
 * records for every table modified in the current transaction.  At commit
 * or abort, we propagate tuples_inserted/updated/deleted up to the
 * parent subtransaction level, or out to the parent PgStat_TableStatus,
 * as appropriate.
 * ----------
 */
typedef struct PgStat_TableStatus
{
	struct PgStat_TableXactStatus *trans;	/* lowest subxact's counts */
	PgStat_TableCounts t_counts;	/* event counts to be sent */
	Relation	relation;			/* rel that is using this entry */
} PgStat_TableStatus;

/* ----------
 * PgStat_TableXactStatus		Per-table, per-subtransaction status
 * ----------
 */
typedef struct PgStat_TableXactStatus
{
	PgStat_Counter tuples_inserted; /* tuples inserted in (sub)xact */
	PgStat_Counter tuples_updated;	/* tuples updated in (sub)xact */
	PgStat_Counter tuples_deleted;	/* tuples deleted in (sub)xact */
	bool		truncdropped;		/* relation truncated/dropped in this
									 * (sub)xact */
	/* tuples i/u/d prior to truncate/drop */
	PgStat_Counter inserted_pre_truncdrop;
	PgStat_Counter updated_pre_truncdrop;
	PgStat_Counter deleted_pre_truncdrop;
	int			nest_level;		/* subtransaction nest level */
	/* links to other structs for same relation: */
	struct PgStat_TableXactStatus *upper;	/* next higher subxact if any */
	PgStat_TableStatus *parent; /* per-table status */
	/* structs of same subxact level are linked here: */
	struct PgStat_TableXactStatus *next;	/* next of same subxact */
} PgStat_TableXactStatus;


/* ----------
 * PgStat_FunctionCounts	The actual per-function counts kept by a backend
 *
 * This struct should contain only actual event counters, because we memcmp
 * it against zeroes to detect whether there are any counts to write.
 *
 * Note that the time counters are in instr_time format here.  We convert to
 * microseconds in PgStat_Counter format when updating the shared statistics.
 * ----------
 */
typedef struct PgStat_FunctionCounts
{
	PgStat_Counter f_numcalls;
	instr_time	f_total_time;
	instr_time	f_self_time;
} PgStat_FunctionCounts;

/* ----------
 * PgStat_BackendFunctionEntry	Entry in backend's per-function hash table
 * ----------
 */
typedef struct PgStat_BackendFunctionEntry
{
	PgStat_FunctionCounts f_counts;
} PgStat_BackendFunctionEntry;

/*
 * Working state needed to accumulate per-function-call timing statistics.
 */
typedef struct PgStat_FunctionCallUsage
{
	/* Link to function's hashtable entry (must still be there at exit!) */
	/* NULL means we are not tracking the current function call */
	PgStat_FunctionCounts *fs;
	/* Total time previously charged to function, as of function start */
	instr_time	save_f_total_time;
	/* Backend-wide total time as of function start */
	instr_time	save_total;
	/* system clock as of function start */
	instr_time	f_start;
} PgStat_FunctionCallUsage;


/* ------------------------------------------------------------
 * Activity statistics data structures on file and shared memory follow
 *
 * PGSTAT_FILE_FORMAT_ID should be changed whenever any of these
 * data structures change.
 * ------------------------------------------------------------
 */

#define PGSTAT_FILE_FORMAT_ID	0x01A5BCA1

/* ----------
 * PgStat_StatDBEntry			The per database statistics
 * ----------
 */
typedef struct PgStat_StatDBEntry
{
	PgStat_Counter n_xact_commit;
	PgStat_Counter n_xact_rollback;
	PgStat_Counter n_blocks_fetched;
	PgStat_Counter n_blocks_hit;
	PgStat_Counter n_tuples_returned;
	PgStat_Counter n_tuples_fetched;
	PgStat_Counter n_tuples_inserted;
	PgStat_Counter n_tuples_updated;
	PgStat_Counter n_tuples_deleted;
	TimestampTz last_autovac_time;
	PgStat_Counter n_conflict_tablespace;
	PgStat_Counter n_conflict_lock;
	PgStat_Counter n_conflict_snapshot;
	PgStat_Counter n_conflict_bufferpin;
	PgStat_Counter n_conflict_startup_deadlock;
	PgStat_Counter n_temp_files;
	PgStat_Counter n_temp_bytes;
	PgStat_Counter n_deadlocks;
	PgStat_Counter n_checksum_failures;
	TimestampTz last_checksum_failure;
	PgStat_Counter n_block_read_time;	/* times in microseconds */
	PgStat_Counter n_block_write_time;
	PgStat_Counter n_sessions;
	PgStat_Counter total_session_time;
	PgStat_Counter total_active_time;
	PgStat_Counter total_idle_in_xact_time;
	PgStat_Counter n_sessions_abandoned;
	PgStat_Counter n_sessions_fatal;
	PgStat_Counter n_sessions_killed;

	TimestampTz stat_reset_timestamp;
} PgStat_StatDBEntry;


/* ----------
 * PgStat_StatTabEntry			The per table (or index) statistics
 * ----------
 */
typedef struct PgStat_StatTabEntry
{
	PgStat_Counter numscans;

	PgStat_Counter tuples_returned;
	PgStat_Counter tuples_fetched;

	PgStat_Counter tuples_inserted;
	PgStat_Counter tuples_updated;
	PgStat_Counter tuples_deleted;
	PgStat_Counter tuples_hot_updated;

	PgStat_Counter n_live_tuples;
	PgStat_Counter n_dead_tuples;
	PgStat_Counter changes_since_analyze;
	PgStat_Counter inserts_since_vacuum;

	PgStat_Counter blocks_fetched;
	PgStat_Counter blocks_hit;

	TimestampTz vacuum_timestamp;	/* user initiated vacuum */
	PgStat_Counter vacuum_count;
	TimestampTz autovac_vacuum_timestamp;	/* autovacuum initiated */
	PgStat_Counter autovac_vacuum_count;
	TimestampTz analyze_timestamp;	/* user initiated */
	PgStat_Counter analyze_count;
	TimestampTz autovac_analyze_timestamp;	/* autovacuum initiated */
	PgStat_Counter autovac_analyze_count;
} PgStat_StatTabEntry;


/* ----------
 * PgStat_StatFuncEntry			The per function statistics
 * ----------
 */
typedef struct PgStat_StatFuncEntry
{
	PgStat_Counter f_numcalls;

	PgStat_Counter f_total_time;	/* times in microseconds */
	PgStat_Counter f_self_time;
} PgStat_StatFuncEntry;


typedef struct PgStat_ArchiverStats
{
	PgStat_Counter archived_count;	/* archival successes */
	char		last_archived_wal[MAX_XFN_CHARS + 1];	/* last WAL file
														 * archived */
	TimestampTz last_archived_timestamp;	/* last archival success time */
	PgStat_Counter failed_count;	/* failed archival attempts */
	char		last_failed_wal[MAX_XFN_CHARS + 1]; /* WAL file involved in
													 * last failure */
	TimestampTz last_failed_timestamp;	/* last archival failure time */
	TimestampTz stat_reset_timestamp;
} PgStat_ArchiverStats;

typedef struct PgStat_BgWriterStats
{
	PgStat_Counter buf_written_clean;
	PgStat_Counter maxwritten_clean;
	PgStat_Counter buf_alloc;
	TimestampTz stat_reset_timestamp;
} PgStat_BgWriterStats;

typedef struct PgStat_CheckpointerStats
{
	PgStat_Counter timed_checkpoints;
	PgStat_Counter requested_checkpoints;
	PgStat_Counter checkpoint_write_time;	/* times in milliseconds */
	PgStat_Counter checkpoint_sync_time;
	PgStat_Counter buf_written_checkpoints;
	PgStat_Counter buf_written_backend;
	PgStat_Counter buf_fsync_backend;
} PgStat_CheckpointerStats;

typedef struct PgStat_ReplSlotStats
{
	/*
	 * AFIXME: This index needs to be removed. See note in pgstat_read_statsfile()
	 */
	uint32		index;
	char		slotname[NAMEDATALEN];
	PgStat_Counter spill_txns;
	PgStat_Counter spill_count;
	PgStat_Counter spill_bytes;
	PgStat_Counter stream_txns;
	PgStat_Counter stream_count;
	PgStat_Counter stream_bytes;
	TimestampTz stat_reset_timestamp;
} PgStat_ReplSlotStats;

typedef struct PgStat_SLRUStats
{
	PgStat_Counter blocks_zeroed;
	PgStat_Counter blocks_hit;
	PgStat_Counter blocks_read;
	PgStat_Counter blocks_written;
	PgStat_Counter blocks_exists;
	PgStat_Counter flush;
	PgStat_Counter truncate;
	TimestampTz stat_reset_timestamp;
} PgStat_SLRUStats;

typedef struct PgStat_WalStats
{
	/* AFIXME: I don't think the use of WalUsage here is a good idea */
	WalUsage	   wal_usage;
	PgStat_Counter wal_buffers_full;
	PgStat_Counter wal_write;
	PgStat_Counter wal_sync;
	PgStat_Counter wal_write_time;
	PgStat_Counter wal_sync_time;
	TimestampTz stat_reset_timestamp;
} PgStat_WalStats;


/* ----------
 * WAL logging integration
 * ----------
 */

/*
 * A transactionally dropped statistics entry.
 */
typedef struct PgStat_DroppedStatsItem
{
	PgStatKind	kind;
	Oid			dboid;
	Oid			objoid;
} PgStat_DroppedStatsItem;


/* ----------
 * pgstat.c - statistics infrastructure
 * ----------
 */

/* functions called from postmaster */
extern Size StatsShmemSize(void);
extern void StatsShmemInit(void);

/* Functions called during server startup / shutdown */
extern void pgstat_restore_stats(void);
extern void pgstat_discard_stats(void);
extern void pgstat_before_server_shutdown(int code, Datum arg);

/* Functions for backend initialization */
extern void pgstat_initialize(void);

/* transactional integration */
extern void AtEOXact_PgStat(bool isCommit, bool parallel);
extern void AtEOSubXact_PgStat(bool isCommit, int nestDepth);
extern void AtPrepare_PgStat(void);
extern void PostPrepare_PgStat(void);
extern void pgstat_twophase_postcommit(TransactionId xid, uint16 info,
									   void *recdata, uint32 len);
extern void pgstat_twophase_postabort(TransactionId xid, uint16 info,
									  void *recdata, uint32 len);
extern void pgstat_clear_snapshot(void);
extern int pgstat_pending_stats_drops(bool isCommit, struct PgStat_DroppedStatsItem **items);
extern void pgstat_perform_drops(int ndrops, struct PgStat_DroppedStatsItem *items, bool is_redo);

/* Functions called from backends */
extern long pgstat_report_stat(bool force);
extern void pgstat_force_next_flush(void);
extern void pgstat_vacuum_stat(void);
extern void pgstat_reset_counters(void);
extern void pgstat_reset_single_counter(Oid objectid, PgStat_Single_Reset_Type type);
extern TimestampTz pgstat_get_stat_snapshot_timestamp(bool *have_snapshot);

/* GUC parameters */
extern PGDLLIMPORT bool pgstat_track_counts;
extern PGDLLIMPORT int pgstat_track_functions;
extern PGDLLIMPORT int pgstat_fetch_consistency;
extern char *pgstat_stat_directory;

/* No longer used, but will be removed with GUC */
extern char *pgstat_stat_tmpname;
extern char *pgstat_stat_filename;


/* ----------
 * pgstat_database.c - database statistics
 * ----------
 */

extern void pgstat_drop_database(Oid databaseid);
extern void pgstat_report_recovery_conflict(int reason);
extern void pgstat_report_deadlock(void);
extern void pgstat_report_checksum_failures_in_db(Oid dboid, int failurecount);
extern void pgstat_report_checksum_failure(void);

#define pgstat_count_buffer_read_time(n)							\
	(pgStatBlockReadTime += (n))
#define pgstat_count_buffer_write_time(n)							\
	(pgStatBlockWriteTime += (n))
#define pgstat_count_conn_active_time(n)							\
	(pgStatActiveTime += (n))
#define pgstat_count_conn_txn_idle_time(n)							\
	(pgStatTransactionIdleTime += (n))

extern PgStat_StatDBEntry *pgstat_fetch_stat_dbentry(Oid dbid);

/*
 * Updated by pgstat_count_buffer_*_time macros
 */
extern PgStat_Counter pgStatBlockReadTime;
extern PgStat_Counter pgStatBlockWriteTime;

/*
 * Updated by pgstat_count_conn_*_time macros, called by
 * pgstat_report_activity().
 */
extern PgStat_Counter pgStatActiveTime;
extern PgStat_Counter pgStatTransactionIdleTime;

/*
 * Updated by the traffic cop and in errfinish()
 */
extern SessionEndType pgStatSessionEndCause;


/* ----------
 * pgstat_function.c - function statistics
 * ----------
 */

extern void pgstat_create_function(Oid proid);
extern void pgstat_drop_function(Oid proid);

struct FunctionCallInfoBaseData;
extern void pgstat_init_function_usage(struct FunctionCallInfoBaseData *fcinfo,
									   PgStat_FunctionCallUsage *fcu);
extern void pgstat_end_function_usage(PgStat_FunctionCallUsage *fcu,
									  bool finalize);

extern PgStat_StatFuncEntry *pgstat_fetch_stat_funcentry(Oid funcid);
extern PgStat_BackendFunctionEntry *find_funcstat_entry(Oid func_id);


/* ----------
 * pgstat_global.c - system wide statistics
 * ----------
 */

extern void pgstat_reset_shared_counters(const char *);

/* archiver stats */
extern void pgstat_report_archiver(const char *xlog, bool failed);
extern PgStat_ArchiverStats *pgstat_fetch_stat_archiver(void);

/* bgwriter stats */
extern void pgstat_report_bgwriter(void);
extern PgStat_BgWriterStats *pgstat_fetch_stat_bgwriter(void);

/* checkpointer stats */
extern void pgstat_report_checkpointer(void);
extern PgStat_CheckpointerStats *pgstat_fetch_stat_checkpointer(void);

/* replication slot stats */
extern void pgstat_reset_replslot_counter(const char *name);
extern void pgstat_report_replslot(uint32 index, const char *slotname,
								   PgStat_Counter spilltxns, PgStat_Counter spillcount,
								   PgStat_Counter spillbytes, PgStat_Counter streamtxns,
								   PgStat_Counter streamcount, PgStat_Counter streambytes);
extern void pgstat_report_replslot_drop(uint32 index, const char *slotname);
extern PgStat_ReplSlotStats *pgstat_fetch_replslot(int *nslots_p);

/* slru stats */
extern void pgstat_reset_slru_counter(const char *);
extern void pgstat_count_slru_page_zeroed(int slru_idx);
extern void pgstat_count_slru_page_hit(int slru_idx);
extern void pgstat_count_slru_page_read(int slru_idx);
extern void pgstat_count_slru_page_written(int slru_idx);
extern void pgstat_count_slru_page_exists(int slru_idx);
extern void pgstat_count_slru_flush(int slru_idx);
extern void pgstat_count_slru_truncate(int slru_idx);
extern const char *pgstat_slru_name(int slru_idx);
extern int	pgstat_slru_index(const char *name);
extern PgStat_SLRUStats *pgstat_fetch_slru(void);

/* wal stats */
extern void pgstat_report_wal(bool force);
extern PgStat_WalStats *pgstat_fetch_stat_wal(void);

/*
 * BgWriter statistics counters are updated directly by bgwriter and bufmgr
 */
extern PgStat_BgWriterStats PendingBgWriterStats;

/*
 * Checkpointer statistics counters are updated directly by checkpointer and
 * bufmgr.
 */
extern PgStat_CheckpointerStats PendingCheckpointerStats;

/*
 * WAL statistics counter is updated by backends and background processes
 */
extern PgStat_WalStats WalStats;


/* ----------
 * pgstat_relations.c - relation (tables, indexes) statistics
 * ----------
 */

extern void pgstat_create_relation(Relation rel);
extern void pgstat_drop_relation(Relation rel);
extern void pgstat_copy_relation_stats(Relation dstrel, Relation srcrel);

extern void pgstat_relation_init(Relation rel);
extern void pgstat_relation_assoc(Relation rel);
extern void pgstat_relation_delink(Relation rel);

extern void pgstat_report_autovac(Oid dboid);
extern void pgstat_report_vacuum(Oid tableoid, bool shared,
								 PgStat_Counter livetuples, PgStat_Counter deadtuples);
extern void pgstat_report_analyze(Relation rel,
								  PgStat_Counter livetuples, PgStat_Counter deadtuples,
								  bool resetcounter);

/*
 * AFIXME: pgstat_enabled should instead be tri-valued (disabled,
 * needs-initialization, initialized). Then we don't need to check two
 * separate variables.
 *
 */
#define pgstat_relation_should_count(rel)							\
	(likely((rel)->pgstat_info != NULL) ? true :					\
	 ((rel)->pgstat_enabled ? pgstat_relation_assoc(rel), true : false))

/* nontransactional event counts are simple enough to inline */

#define pgstat_count_heap_scan(rel)									\
	do {															\
		if (pgstat_relation_should_count(rel))						\
			(rel)->pgstat_info->t_counts.t_numscans++;				\
	} while (0)
#define pgstat_count_heap_getnext(rel)								\
	do {															\
		if (pgstat_relation_should_count(rel))						\
			(rel)->pgstat_info->t_counts.t_tuples_returned++;		\
	} while (0)
#define pgstat_count_heap_fetch(rel)								\
	do {															\
		if (pgstat_relation_should_count(rel))						\
			(rel)->pgstat_info->t_counts.t_tuples_fetched++;		\
	} while (0)
#define pgstat_count_index_scan(rel)								\
	do {															\
		if (pgstat_relation_should_count(rel))						\
			(rel)->pgstat_info->t_counts.t_numscans++;				\
	} while (0)
#define pgstat_count_index_tuples(rel, n)							\
	do {															\
		if (pgstat_relation_should_count(rel))						\
			(rel)->pgstat_info->t_counts.t_tuples_returned += (n);	\
	} while (0)
#define pgstat_count_buffer_read(rel)								\
	do {															\
		if (pgstat_relation_should_count(rel))						\
			(rel)->pgstat_info->t_counts.t_blocks_fetched++;		\
	} while (0)
#define pgstat_count_buffer_hit(rel)								\
	do {															\
		if (pgstat_relation_should_count(rel))						\
			(rel)->pgstat_info->t_counts.t_blocks_hit++;			\
	} while (0)

extern void pgstat_count_heap_insert(Relation rel, PgStat_Counter n);
extern void pgstat_count_heap_update(Relation rel, bool hot);
extern void pgstat_count_heap_delete(Relation rel);
extern void pgstat_count_truncate(Relation rel);
extern void pgstat_update_heap_dead_tuples(Relation rel, int delta);

extern PgStat_StatTabEntry *pgstat_fetch_stat_tabentry(Oid relid);
extern PgStat_StatTabEntry *pgstat_fetch_stat_tabentry_extended(bool shared,
																Oid relid);
extern PgStat_TableStatus *find_tabstat_entry(Oid rel_id);

#endif							/* PGSTAT_H */
