/* ----------
 *	pgstat.h
 *
 *	Definitions for the PostgreSQL statistics collector daemon.
 *
 *	Copyright (c) 2001-2018, PostgreSQL Global Development Group
 *
 *	src/include/pgstat.h
 * ----------
 */
#ifndef PGSTAT_H
#define PGSTAT_H

#include "datatype/timestamp.h"
#include "fmgr.h"
#include "lib/dshash.h"
#include "libpq/pqcomm.h"
#include "port/atomics.h"
#include "portability/instr_time.h"
#include "postmaster/pgarch.h"
#include "storage/proc.h"
#include "utils/hsearch.h"
#include "utils/relcache.h"


/* ----------
 * Paths for the statistics files (relative to installation's $PGDATA).
 * ----------
 */
#define PGSTAT_STAT_PERMANENT_DIRECTORY		"pg_stat"
#define PGSTAT_STAT_PERMANENT_FILENAME		"pg_stat/global.stat"
#define PGSTAT_STAT_PERMANENT_TMPFILE		"pg_stat/global.tmp"

/* Values for track_functions GUC variable --- order is significant! */
typedef enum TrackFunctionsLevel
{
	TRACK_FUNC_OFF,
	TRACK_FUNC_PL,
	TRACK_FUNC_ALL
}			TrackFunctionsLevel;

/* ----------
 * The data type used for counters.
 * ----------
 */
typedef int64 PgStat_Counter;

/* ----------
 * PgStat_TableCounts			The actual per-table counts kept by a backend
 *
 * This struct should contain only actual event counters, because we memcmp
 * it against zeroes to detect whether there are any counts to transmit.
 * It is a component of PgStat_TableStatus (within-backend state) and
 * PgStat_TableEntry (the transmitted message format).
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
	bool		t_truncated;

	PgStat_Counter t_delta_live_tuples;
	PgStat_Counter t_delta_dead_tuples;
	PgStat_Counter t_changed_tuples;

	PgStat_Counter t_blocks_fetched;
	PgStat_Counter t_blocks_hit;
} PgStat_TableCounts;

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
	Oid			t_id;			/* table's OID */
	bool		t_shared;		/* is it a shared catalog? */
	struct PgStat_TableXactStatus *trans;	/* lowest subxact's counts */
	PgStat_TableCounts t_counts;	/* event counts to be sent */
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
	bool		truncated;		/* relation truncated in this (sub)xact */
	PgStat_Counter inserted_pre_trunc;	/* tuples inserted prior to truncate */
	PgStat_Counter updated_pre_trunc;	/* tuples updated prior to truncate */
	PgStat_Counter deleted_pre_trunc;	/* tuples deleted prior to truncate */
	int			nest_level;		/* subtransaction nest level */
	/* links to other structs for same relation: */
	struct PgStat_TableXactStatus *upper;	/* next higher subxact if any */
	PgStat_TableStatus *parent; /* per-table status */
	/* structs of same subxact level are linked here: */
	struct PgStat_TableXactStatus *next;	/* next of same subxact */
} PgStat_TableXactStatus;


/* ----------
 * PgStat_BgWriter			bgwriter statistics
 * ----------
 */
typedef struct PgStat_BgWriter
{
	PgStat_Counter timed_checkpoints;
	PgStat_Counter requested_checkpoints;
	PgStat_Counter buf_written_checkpoints;
	PgStat_Counter buf_written_clean;
	PgStat_Counter maxwritten_clean;
	PgStat_Counter buf_written_backend;
	PgStat_Counter buf_fsync_backend;
	PgStat_Counter buf_alloc;
	PgStat_Counter checkpoint_write_time; /* times in milliseconds */
	PgStat_Counter checkpoint_sync_time;
} PgStat_BgWriter;

/* ----------
 * PgStat_FunctionCounts	The actual per-function counts kept by a backend
 *
 * This struct should contain only actual event counters, because we memcmp
 * it against zeroes to detect whether there are any counts to transmit.
 *
 * Note that the time counters are in instr_time format here.  We convert to
 * microseconds in PgStat_Counter format when transmitting to the collector.
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
	Oid			f_id;
	PgStat_FunctionCounts f_counts;
} PgStat_BackendFunctionEntry;

/* ----------
 * PgStat_FunctionEntry			Per-function info in a MsgFuncstat
 * ----------
 */
typedef struct PgStat_FunctionEntry
{
	Oid			f_id;
	PgStat_Counter f_numcalls;
	PgStat_Counter f_total_time;	/* times in microseconds */
	PgStat_Counter f_self_time;
} PgStat_FunctionEntry;

/* ------------------------------------------------------------
 * Statistic collector data structures follow
 *
 * PGSTAT_FILE_FORMAT_ID should be changed whenever any of these
 * data structures change.
 * ------------------------------------------------------------
 */

#define PGSTAT_FILE_FORMAT_ID	0x01A5BC9D

/* ----------
 * PgStat_StatDBEntry			The collector's data per database
 * ----------
 */
typedef struct PgStat_StatDBEntry
{
	Oid			databaseid;
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
	PgStat_Counter n_block_read_time;	/* times in microseconds */
	PgStat_Counter n_block_write_time;

	TimestampTz stat_reset_timestamp;
	TimestampTz stats_timestamp;	/* time of db stats file update */

	/*
	 * tables and functions must be last in the struct, because we don't write
	 * the handles and pointers out to the stats file.
	 */
	dshash_table_handle tables;
	dshash_table_handle functions;
	/* for snapshot tables */
	HTAB *snapshot_tables;
	HTAB *snapshot_functions;
} PgStat_StatDBEntry;


/* ----------
 * PgStat_StatTabEntry			The collector's data per table (or index)
 * ----------
 */
typedef struct PgStat_StatTabEntry
{
	Oid			tableid;

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
 * PgStat_StatFuncEntry			The collector's data per function
 * ----------
 */
typedef struct PgStat_StatFuncEntry
{
	Oid			functionid;

	PgStat_Counter f_numcalls;

	PgStat_Counter f_total_time;	/* times in microseconds */
	PgStat_Counter f_self_time;
} PgStat_StatFuncEntry;


/*
 * Archiver statistics kept in the stats collector
 */
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

/*
 * Global statistics kept in the stats collector
 */
typedef struct PgStat_GlobalStats
{
	TimestampTz stats_timestamp;	/* time of stats file update */
	PgStat_Counter timed_checkpoints;
	PgStat_Counter requested_checkpoints;
	PgStat_Counter checkpoint_write_time;	/* times in milliseconds */
	PgStat_Counter checkpoint_sync_time;
	PgStat_Counter buf_written_checkpoints;
	PgStat_Counter buf_written_clean;
	PgStat_Counter maxwritten_clean;
	PgStat_Counter buf_written_backend;
	PgStat_Counter buf_fsync_backend;
	PgStat_Counter buf_alloc;
	TimestampTz stat_reset_timestamp;
} PgStat_GlobalStats;


/* ----------
 * Backend types
 * ----------
 */
typedef enum BackendType
{
	B_AUTOVAC_LAUNCHER,
	B_AUTOVAC_WORKER,
	B_BACKEND,
	B_BG_WORKER,
	B_BG_WRITER,
	B_CHECKPOINTER,
	B_STARTUP,
	B_WAL_RECEIVER,
	B_WAL_SENDER,
	B_WAL_WRITER,
	B_STATS_COLLECTOR
} BackendType;


/* ----------
 * Backend states
 * ----------
 */
typedef enum BackendState
{
	STATE_UNDEFINED,
	STATE_IDLE,
	STATE_RUNNING,
	STATE_IDLEINTRANSACTION,
	STATE_FASTPATH,
	STATE_IDLEINTRANSACTION_ABORTED,
	STATE_DISABLED
} BackendState;


/* ----------
 * Wait Classes
 * ----------
 */
#define PG_WAIT_LWLOCK				0x01000000U
#define PG_WAIT_LOCK				0x03000000U
#define PG_WAIT_BUFFER_PIN			0x04000000U
#define PG_WAIT_ACTIVITY			0x05000000U
#define PG_WAIT_CLIENT				0x06000000U
#define PG_WAIT_EXTENSION			0x07000000U
#define PG_WAIT_IPC					0x08000000U
#define PG_WAIT_TIMEOUT				0x09000000U
#define PG_WAIT_IO					0x0A000000U

/* ----------
 * Wait Events - Activity
 *
 * Use this category when a process is waiting because it has no work to do,
 * unless the "Client" or "Timeout" category describes the situation better.
 * Typically, this should only be used for background processes.
 * ----------
 */
typedef enum
{
	WAIT_EVENT_ARCHIVER_MAIN = PG_WAIT_ACTIVITY,
	WAIT_EVENT_AUTOVACUUM_MAIN,
	WAIT_EVENT_BGWRITER_HIBERNATE,
	WAIT_EVENT_BGWRITER_MAIN,
	WAIT_EVENT_CHECKPOINTER_MAIN,
	WAIT_EVENT_LOGICAL_LAUNCHER_MAIN,
	WAIT_EVENT_LOGICAL_APPLY_MAIN,
	WAIT_EVENT_PGSTAT_MAIN,
	WAIT_EVENT_RECOVERY_WAL_ALL,
	WAIT_EVENT_RECOVERY_WAL_STREAM,
	WAIT_EVENT_SYSLOGGER_MAIN,
	WAIT_EVENT_WAL_RECEIVER_MAIN,
	WAIT_EVENT_WAL_SENDER_MAIN,
	WAIT_EVENT_WAL_WRITER_MAIN
} WaitEventActivity;

/* ----------
 * Wait Events - Client
 *
 * Use this category when a process is waiting to send data to or receive data
 * from the frontend process to which it is connected.  This is never used for
 * a background process, which has no client connection.
 * ----------
 */
typedef enum
{
	WAIT_EVENT_CLIENT_READ = PG_WAIT_CLIENT,
	WAIT_EVENT_CLIENT_WRITE,
	WAIT_EVENT_LIBPQWALRECEIVER_CONNECT,
	WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE,
	WAIT_EVENT_SSL_OPEN_SERVER,
	WAIT_EVENT_WAL_RECEIVER_WAIT_START,
	WAIT_EVENT_WAL_SENDER_WAIT_WAL,
	WAIT_EVENT_WAL_SENDER_WRITE_DATA
} WaitEventClient;

/* ----------
 * Wait Events - IPC
 *
 * Use this category when a process cannot complete the work it is doing because
 * it is waiting for a notification from another process.
 * ----------
 */
typedef enum
{
	WAIT_EVENT_BGWORKER_SHUTDOWN = PG_WAIT_IPC,
	WAIT_EVENT_BGWORKER_STARTUP,
	WAIT_EVENT_BTREE_PAGE,
	WAIT_EVENT_EXECUTE_GATHER,
	WAIT_EVENT_HASH_BATCH_ALLOCATING,
	WAIT_EVENT_HASH_BATCH_ELECTING,
	WAIT_EVENT_HASH_BATCH_LOADING,
	WAIT_EVENT_HASH_BUILD_ALLOCATING,
	WAIT_EVENT_HASH_BUILD_ELECTING,
	WAIT_EVENT_HASH_BUILD_HASHING_INNER,
	WAIT_EVENT_HASH_BUILD_HASHING_OUTER,
	WAIT_EVENT_HASH_GROW_BATCHES_ELECTING,
	WAIT_EVENT_HASH_GROW_BATCHES_FINISHING,
	WAIT_EVENT_HASH_GROW_BATCHES_REPARTITIONING,
	WAIT_EVENT_HASH_GROW_BATCHES_ALLOCATING,
	WAIT_EVENT_HASH_GROW_BATCHES_DECIDING,
	WAIT_EVENT_HASH_GROW_BUCKETS_ELECTING,
	WAIT_EVENT_HASH_GROW_BUCKETS_REINSERTING,
	WAIT_EVENT_HASH_GROW_BUCKETS_ALLOCATING,
	WAIT_EVENT_LOGICAL_SYNC_DATA,
	WAIT_EVENT_LOGICAL_SYNC_STATE_CHANGE,
	WAIT_EVENT_MQ_INTERNAL,
	WAIT_EVENT_MQ_PUT_MESSAGE,
	WAIT_EVENT_MQ_RECEIVE,
	WAIT_EVENT_MQ_SEND,
	WAIT_EVENT_PARALLEL_FINISH,
	WAIT_EVENT_PARALLEL_BITMAP_SCAN,
	WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN,
	WAIT_EVENT_PROCARRAY_GROUP_UPDATE,
	WAIT_EVENT_CLOG_GROUP_UPDATE,
	WAIT_EVENT_REPLICATION_ORIGIN_DROP,
	WAIT_EVENT_REPLICATION_SLOT_DROP,
	WAIT_EVENT_SAFE_SNAPSHOT,
	WAIT_EVENT_SYNC_REP
} WaitEventIPC;

/* ----------
 * Wait Events - Timeout
 *
 * Use this category when a process is waiting for a timeout to expire.
 * ----------
 */
typedef enum
{
	WAIT_EVENT_BASE_BACKUP_THROTTLE = PG_WAIT_TIMEOUT,
	WAIT_EVENT_PG_SLEEP,
	WAIT_EVENT_RECOVERY_APPLY_DELAY
} WaitEventTimeout;

/* ----------
 * Wait Events - IO
 *
 * Use this category when a process is waiting for a IO.
 * ----------
 */
typedef enum
{
	WAIT_EVENT_BUFFILE_READ = PG_WAIT_IO,
	WAIT_EVENT_BUFFILE_WRITE,
	WAIT_EVENT_CONTROL_FILE_READ,
	WAIT_EVENT_CONTROL_FILE_SYNC,
	WAIT_EVENT_CONTROL_FILE_SYNC_UPDATE,
	WAIT_EVENT_CONTROL_FILE_WRITE,
	WAIT_EVENT_CONTROL_FILE_WRITE_UPDATE,
	WAIT_EVENT_COPY_FILE_READ,
	WAIT_EVENT_COPY_FILE_WRITE,
	WAIT_EVENT_DATA_FILE_EXTEND,
	WAIT_EVENT_DATA_FILE_FLUSH,
	WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC,
	WAIT_EVENT_DATA_FILE_PREFETCH,
	WAIT_EVENT_DATA_FILE_READ,
	WAIT_EVENT_DATA_FILE_SYNC,
	WAIT_EVENT_DATA_FILE_TRUNCATE,
	WAIT_EVENT_DATA_FILE_WRITE,
	WAIT_EVENT_DSM_FILL_ZERO_WRITE,
	WAIT_EVENT_LOCK_FILE_ADDTODATADIR_READ,
	WAIT_EVENT_LOCK_FILE_ADDTODATADIR_SYNC,
	WAIT_EVENT_LOCK_FILE_ADDTODATADIR_WRITE,
	WAIT_EVENT_LOCK_FILE_CREATE_READ,
	WAIT_EVENT_LOCK_FILE_CREATE_SYNC,
	WAIT_EVENT_LOCK_FILE_CREATE_WRITE,
	WAIT_EVENT_LOCK_FILE_RECHECKDATADIR_READ,
	WAIT_EVENT_LOGICAL_REWRITE_CHECKPOINT_SYNC,
	WAIT_EVENT_LOGICAL_REWRITE_MAPPING_SYNC,
	WAIT_EVENT_LOGICAL_REWRITE_MAPPING_WRITE,
	WAIT_EVENT_LOGICAL_REWRITE_SYNC,
	WAIT_EVENT_LOGICAL_REWRITE_TRUNCATE,
	WAIT_EVENT_LOGICAL_REWRITE_WRITE,
	WAIT_EVENT_RELATION_MAP_READ,
	WAIT_EVENT_RELATION_MAP_SYNC,
	WAIT_EVENT_RELATION_MAP_WRITE,
	WAIT_EVENT_REORDER_BUFFER_READ,
	WAIT_EVENT_REORDER_BUFFER_WRITE,
	WAIT_EVENT_REORDER_LOGICAL_MAPPING_READ,
	WAIT_EVENT_REPLICATION_SLOT_READ,
	WAIT_EVENT_REPLICATION_SLOT_RESTORE_SYNC,
	WAIT_EVENT_REPLICATION_SLOT_SYNC,
	WAIT_EVENT_REPLICATION_SLOT_WRITE,
	WAIT_EVENT_SLRU_FLUSH_SYNC,
	WAIT_EVENT_SLRU_READ,
	WAIT_EVENT_SLRU_SYNC,
	WAIT_EVENT_SLRU_WRITE,
	WAIT_EVENT_SNAPBUILD_READ,
	WAIT_EVENT_SNAPBUILD_SYNC,
	WAIT_EVENT_SNAPBUILD_WRITE,
	WAIT_EVENT_TIMELINE_HISTORY_FILE_SYNC,
	WAIT_EVENT_TIMELINE_HISTORY_FILE_WRITE,
	WAIT_EVENT_TIMELINE_HISTORY_READ,
	WAIT_EVENT_TIMELINE_HISTORY_SYNC,
	WAIT_EVENT_TIMELINE_HISTORY_WRITE,
	WAIT_EVENT_TWOPHASE_FILE_READ,
	WAIT_EVENT_TWOPHASE_FILE_SYNC,
	WAIT_EVENT_TWOPHASE_FILE_WRITE,
	WAIT_EVENT_WALSENDER_TIMELINE_HISTORY_READ,
	WAIT_EVENT_WAL_BOOTSTRAP_SYNC,
	WAIT_EVENT_WAL_BOOTSTRAP_WRITE,
	WAIT_EVENT_WAL_COPY_READ,
	WAIT_EVENT_WAL_COPY_SYNC,
	WAIT_EVENT_WAL_COPY_WRITE,
	WAIT_EVENT_WAL_INIT_SYNC,
	WAIT_EVENT_WAL_INIT_WRITE,
	WAIT_EVENT_WAL_READ,
	WAIT_EVENT_WAL_SYNC,
	WAIT_EVENT_WAL_SYNC_METHOD_ASSIGN,
	WAIT_EVENT_WAL_WRITE
} WaitEventIO;

/* ----------
 * Command type for progress reporting purposes
 * ----------
 */
typedef enum ProgressCommandType
{
	PROGRESS_COMMAND_INVALID,
	PROGRESS_COMMAND_VACUUM
} ProgressCommandType;

#define PGSTAT_NUM_PROGRESS_PARAM	10

/* ----------
 * Shared-memory data structures
 * ----------
 */


/*
 * PgBackendSSLStatus
 *
 * For each backend, we keep the SSL status in a separate struct, that
 * is only filled in if SSL is enabled.
 */
typedef struct PgBackendSSLStatus
{
	/* Information about SSL connection */
	int			ssl_bits;
	bool		ssl_compression;
	char		ssl_version[NAMEDATALEN];	/* MUST be null-terminated */
	char		ssl_cipher[NAMEDATALEN];	/* MUST be null-terminated */
	char		ssl_clientdn[NAMEDATALEN];	/* MUST be null-terminated */
} PgBackendSSLStatus;


/* ----------
 * PgBackendStatus
 *
 * Each live backend maintains a PgBackendStatus struct in shared memory
 * showing its current activity.  (The structs are allocated according to
 * BackendId, but that is not critical.)  Note that the collector process
 * has no involvement in, or even access to, these structs.
 *
 * Each auxiliary process also maintains a PgBackendStatus struct in shared
 * memory.
 * ----------
 */
typedef struct PgBackendStatus
{
	/*
	 * To avoid locking overhead, we use the following protocol: a backend
	 * increments st_changecount before modifying its entry, and again after
	 * finishing a modification.  A would-be reader should note the value of
	 * st_changecount, copy the entry into private memory, then check
	 * st_changecount again.  If the value hasn't changed, and if it's even,
	 * the copy is valid; otherwise start over.  This makes updates cheap
	 * while reads are potentially expensive, but that's the tradeoff we want.
	 *
	 * The above protocol needs the memory barriers to ensure that the
	 * apparent order of execution is as it desires. Otherwise, for example,
	 * the CPU might rearrange the code so that st_changecount is incremented
	 * twice before the modification on a machine with weak memory ordering.
	 * This surprising result can lead to bugs.
	 */
	int			st_changecount;

	/* The entry is valid iff st_procpid > 0, unused if st_procpid == 0 */
	int			st_procpid;

	/* Type of backends */
	BackendType st_backendType;

	/* Times when current backend, transaction, and activity started */
	TimestampTz st_proc_start_timestamp;
	TimestampTz st_xact_start_timestamp;
	TimestampTz st_activity_start_timestamp;
	TimestampTz st_state_start_timestamp;

	/* Database OID, owning user's OID, connection client address */
	Oid			st_databaseid;
	Oid			st_userid;
	SockAddr	st_clientaddr;
	char	   *st_clienthostname;	/* MUST be null-terminated */

	/* Information about SSL connection */
	bool		st_ssl;
	PgBackendSSLStatus *st_sslstatus;

	/* current state */
	BackendState st_state;

	/* application name; MUST be null-terminated */
	char	   *st_appname;

	/*
	 * Current command string; MUST be null-terminated. Note that this string
	 * possibly is truncated in the middle of a multi-byte character. As
	 * activity strings are stored more frequently than read, that allows to
	 * move the cost of correct truncation to the display side. Use
	 * pgstat_clip_activity() to truncate correctly.
	 */
	char	   *st_activity_raw;

	/*
	 * Command progress reporting.  Any command which wishes can advertise
	 * that it is running by setting st_progress_command,
	 * st_progress_command_target, and st_progress_param[].
	 * st_progress_command_target should be the OID of the relation which the
	 * command targets (we assume there's just one, as this is meant for
	 * utility commands), but the meaning of each element in the
	 * st_progress_param array is command-specific.
	 */
	ProgressCommandType st_progress_command;
	Oid			st_progress_command_target;
	int64		st_progress_param[PGSTAT_NUM_PROGRESS_PARAM];
} PgBackendStatus;

/*
 * Macros to load and store st_changecount with the memory barriers.
 *
 * pgstat_increment_changecount_before() and
 * pgstat_increment_changecount_after() need to be called before and after
 * PgBackendStatus entries are modified, respectively. This makes sure that
 * st_changecount is incremented around the modification.
 *
 * Also pgstat_save_changecount_before() and pgstat_save_changecount_after()
 * need to be called before and after PgBackendStatus entries are copied into
 * private memory, respectively.
 */
#define pgstat_increment_changecount_before(beentry)	\
	do {	\
		beentry->st_changecount++;	\
		pg_write_barrier(); \
	} while (0)

#define pgstat_increment_changecount_after(beentry) \
	do {	\
		pg_write_barrier(); \
		beentry->st_changecount++;	\
		Assert((beentry->st_changecount & 1) == 0); \
	} while (0)

#define pgstat_save_changecount_before(beentry, save_changecount)	\
	do {	\
		save_changecount = beentry->st_changecount; \
		pg_read_barrier();	\
	} while (0)

#define pgstat_save_changecount_after(beentry, save_changecount)	\
	do {	\
		pg_read_barrier();	\
		save_changecount = beentry->st_changecount; \
	} while (0)

/* ----------
 * LocalPgBackendStatus
 *
 * When we build the backend status array, we use LocalPgBackendStatus to be
 * able to add new values to the struct when needed without adding new fields
 * to the shared memory. It contains the backend status as a first member.
 * ----------
 */
typedef struct LocalPgBackendStatus
{
	/*
	 * Local version of the backend status entry.
	 */
	PgBackendStatus backendStatus;

	/*
	 * The xid of the current transaction if available, InvalidTransactionId
	 * if not.
	 */
	TransactionId backend_xid;

	/*
	 * The xmin of the current session if available, InvalidTransactionId if
	 * not.
	 */
	TransactionId backend_xmin;
} LocalPgBackendStatus;

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


/* ----------
 * GUC parameters
 * ----------
 */
extern bool pgstat_track_activities;
extern bool pgstat_track_counts;
extern int	pgstat_track_functions;
extern PGDLLIMPORT int pgstat_track_activity_query_size;
extern char *pgstat_stat_directory;
extern char *pgstat_stat_tmpname;
extern char *pgstat_stat_filename;

/*
 * BgWriter statistics counters are updated directly by bgwriter and bufmgr
 */
extern PgStat_BgWriter BgWriterStats;

/*
 * Updated by pgstat_count_buffer_*_time macros
 */
extern PgStat_Counter pgStatBlockReadTime;
extern PgStat_Counter pgStatBlockWriteTime;

/* ----------
 * Functions called from postmaster
 * ----------
 */
extern Size BackendStatusShmemSize(void);
extern void CreateSharedBackendStatus(void);

extern void pgstat_init(void);
extern int	pgstat_start(void);
extern void pgstat_reset_all(void);
extern void allow_immediate_pgstat_restart(void);

/* ----------
 * Functions called from backends
 * ----------
 */
extern void pgstat_report_stat(bool force);
extern void pgstat_vacuum_stat(void);
extern void pgstat_drop_database(Oid databaseid);

extern void pgstat_clear_snapshot(void);
extern void pgstat_reset_counters(void);
extern void pgstat_reset_shared_counters(const char *);
extern void pgstat_reset_single_counter(Oid objectid, PgStat_Single_Reset_Type type);

extern void pgstat_report_autovac(Oid dboid);
extern void pgstat_report_vacuum(Oid tableoid, bool shared,
					 PgStat_Counter livetuples, PgStat_Counter deadtuples);
extern void pgstat_report_analyze(Relation rel,
					  PgStat_Counter livetuples, PgStat_Counter deadtuples,
					  bool resetcounter);

extern void pgstat_report_recovery_conflict(int reason);
extern void pgstat_report_deadlock(bool pending);

extern void pgstat_initialize(void);
extern void pgstat_bestart(void);

extern void pgstat_report_activity(BackendState state, const char *cmd_str);
extern void pgstat_report_tempfile(size_t filesize);
extern void pgstat_report_appname(const char *appname);
extern void pgstat_report_xact_timestamp(TimestampTz tstamp);
extern const char *pgstat_get_wait_event(uint32 wait_event_info);
extern const char *pgstat_get_wait_event_type(uint32 wait_event_info);
extern const char *pgstat_get_backend_current_activity(int pid, bool checkUser);
extern const char *pgstat_get_crashed_backend_activity(int pid, char *buffer,
									int buflen);
extern const char *pgstat_get_backend_desc(BackendType backendType);

extern void pgstat_progress_start_command(ProgressCommandType cmdtype,
							  Oid relid);
extern void pgstat_progress_update_param(int index, int64 val);
extern void pgstat_progress_update_multi_param(int nparam, const int *index,
								   const int64 *val);
extern void pgstat_progress_end_command(void);

extern PgStat_TableStatus *find_tabstat_entry(Oid rel_id);
extern PgStat_BackendFunctionEntry *find_funcstat_entry(Oid func_id);

extern void pgstat_initstats(Relation rel);

extern char *pgstat_clip_activity(const char *raw_activity);
extern PgStat_StatDBEntry *backend_get_db_entry(Oid dbid, bool oneshot);
extern HTAB *backend_snapshot_all_db_entries(void);
extern PgStat_StatTabEntry *backend_get_tab_entry(PgStat_StatDBEntry *dbent, Oid relid, bool oneshot);

/* ----------
 * pgstat_report_wait_start() -
 *
 *	Called from places where server process needs to wait.  This is called
 *	to report wait event information.  The wait information is stored
 *	as 4-bytes where first byte represents the wait event class (type of
 *	wait, for different types of wait, refer WaitClass) and the next
 *	3-bytes represent the actual wait event.  Currently 2-bytes are used
 *	for wait event which is sufficient for current usage, 1-byte is
 *	reserved for future usage.
 *
 * NB: this *must* be able to survive being called before MyProc has been
 * initialized.
 * ----------
 */
static inline void
pgstat_report_wait_start(uint32 wait_event_info)
{
	volatile PGPROC *proc = MyProc;

	if (!pgstat_track_activities || !proc)
		return;

	/*
	 * Since this is a four-byte field which is always read and written as
	 * four-bytes, updates are atomic.
	 */
	proc->wait_event_info = wait_event_info;
}

/* ----------
 * pgstat_report_wait_end() -
 *
 *	Called to report end of a wait.
 *
 * NB: this *must* be able to survive being called before MyProc has been
 * initialized.
 * ----------
 */
static inline void
pgstat_report_wait_end(void)
{
	volatile PGPROC *proc = MyProc;

	if (!pgstat_track_activities || !proc)
		return;

	/*
	 * Since this is a four-byte field which is always read and written as
	 * four-bytes, updates are atomic.
	 */
	proc->wait_event_info = 0;
}

/* nontransactional event counts are simple enough to inline */

#define pgstat_count_heap_scan(rel)									\
	do {															\
		if ((rel)->pgstat_info != NULL)								\
			(rel)->pgstat_info->t_counts.t_numscans++;				\
	} while (0)
#define pgstat_count_heap_getnext(rel)								\
	do {															\
		if ((rel)->pgstat_info != NULL)								\
			(rel)->pgstat_info->t_counts.t_tuples_returned++;		\
	} while (0)
#define pgstat_count_heap_fetch(rel)								\
	do {															\
		if ((rel)->pgstat_info != NULL)								\
			(rel)->pgstat_info->t_counts.t_tuples_fetched++;		\
	} while (0)
#define pgstat_count_index_scan(rel)								\
	do {															\
		if ((rel)->pgstat_info != NULL)								\
			(rel)->pgstat_info->t_counts.t_numscans++;				\
	} while (0)
#define pgstat_count_index_tuples(rel, n)							\
	do {															\
		if ((rel)->pgstat_info != NULL)								\
			(rel)->pgstat_info->t_counts.t_tuples_returned += (n);	\
	} while (0)
#define pgstat_count_buffer_read(rel)								\
	do {															\
		if ((rel)->pgstat_info != NULL)								\
			(rel)->pgstat_info->t_counts.t_blocks_fetched++;		\
	} while (0)
#define pgstat_count_buffer_hit(rel)								\
	do {															\
		if ((rel)->pgstat_info != NULL)								\
			(rel)->pgstat_info->t_counts.t_blocks_hit++;			\
	} while (0)
#define pgstat_count_buffer_read_time(n)							\
	(pgStatBlockReadTime += (n))
#define pgstat_count_buffer_write_time(n)							\
	(pgStatBlockWriteTime += (n))

extern void pgstat_count_heap_insert(Relation rel, PgStat_Counter n);
extern void pgstat_count_heap_update(Relation rel, bool hot);
extern void pgstat_count_heap_delete(Relation rel);
extern void pgstat_count_truncate(Relation rel);
extern void pgstat_update_heap_dead_tuples(Relation rel, int delta);

extern void pgstat_init_function_usage(FunctionCallInfoData *fcinfo,
						   PgStat_FunctionCallUsage *fcu);
extern void pgstat_end_function_usage(PgStat_FunctionCallUsage *fcu,
						  bool finalize);

extern void AtEOXact_PgStat(bool isCommit);
extern void AtEOSubXact_PgStat(bool isCommit, int nestDepth);

extern void AtPrepare_PgStat(void);
extern void PostPrepare_PgStat(void);

extern void pgstat_twophase_postcommit(TransactionId xid, uint16 info,
						   void *recdata, uint32 len);
extern void pgstat_twophase_postabort(TransactionId xid, uint16 info,
						  void *recdata, uint32 len);

extern void pgstat_send_archiver(const char *xlog, bool failed);
extern void pgstat_send_bgwriter(void);

/* ----------
 * Support functions for the SQL-callable functions to
 * generate the pgstat* views.
 * ----------
 */
extern PgStat_StatDBEntry *pgstat_fetch_stat_dbentry(Oid dbid);
extern PgStat_StatTabEntry *pgstat_fetch_stat_tabentry(Oid relid);
extern PgBackendStatus *pgstat_fetch_stat_beentry(int beid);
extern LocalPgBackendStatus *pgstat_fetch_stat_local_beentry(int beid);
extern PgStat_StatFuncEntry *pgstat_fetch_stat_funcentry(Oid funcid);
extern int	pgstat_fetch_stat_numbackends(void);
extern PgStat_ArchiverStats *pgstat_fetch_stat_archiver(void);
extern PgStat_GlobalStats *pgstat_fetch_global(void);

/* Main loop */
extern void PgstatCollectorMain(void) pg_attribute_noreturn();

extern Size StatsShmemSize(void);
extern void StatsShmemInit(void);

extern void pgstat_cleanup_pending_stat(void);

#endif							/* PGSTAT_H */
