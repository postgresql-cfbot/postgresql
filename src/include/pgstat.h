/* ----------
 *	pgstat.h
 *
 *	Definitions for the PostgreSQL activity statistics facility.
 *
 *	Copyright (c) 2001-2020, PostgreSQL Global Development Group
 *
 *	src/include/pgstat.h
 * ----------
 */
#ifndef PGSTAT_H
#define PGSTAT_H

#include "datatype/timestamp.h"
#include "libpq/pqcomm.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "lib/dshash.h"
#include "portability/instr_time.h"
#include "postmaster/pgarch.h"
#include "storage/proc.h"
#include "storage/lwlock.h"
#include "utils/hsearch.h"
#include "utils/relcache.h"


/* ----------
 * Paths for the statistics files (relative to installation's $PGDATA).
 * ----------
 */
#define PGSTAT_STAT_PERMANENT_DIRECTORY		"pg_stat"
#define PGSTAT_STAT_PERMANENT_FILENAME		"pg_stat/global.stat"
#define PGSTAT_STAT_PERMANENT_TMPFILE		"pg_stat/global.tmp"

/*
 * This used to be the directory to store temporary statistics data in but is
 * no longer used. Defined here for backward compatibility.
 */
#define PG_STAT_TMP_DIR		"pg_stat_tmp"

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
 * it against zeroes to detect whether there are any counts to write.
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
	bool		t_truncated;

	PgStat_Counter t_delta_live_tuples;
	PgStat_Counter t_delta_dead_tuples;
	PgStat_Counter t_changed_tuples;
	bool		reset_changed_tuples;

	PgStat_Counter t_blocks_fetched;
	PgStat_Counter t_blocks_hit;

	PgStat_Counter vacuum_count;
	PgStat_Counter autovac_vacuum_count;
	PgStat_Counter analyze_count;
	PgStat_Counter autovac_analyze_count;
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
	TimestampTz vacuum_timestamp;	/* user initiated vacuum */
	TimestampTz autovac_vacuum_timestamp;	/* autovacuum initiated */
	TimestampTz analyze_timestamp;	/* user initiated */
	TimestampTz autovac_analyze_timestamp;	/* autovacuum initiated */
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
	PgStat_Counter checkpoint_write_time;	/* times in milliseconds */
	PgStat_Counter checkpoint_sync_time;
}			PgStat_BgWriter;

/* ----------
 * PgStat_FunctionCounts	The actual per-function counts kept by a backend
 *
 * This struct should contain only actual event counters, because we memcmp
 * it against zeroes to detect whether there are any counts to write.
 *
 * Note that the time counters are in instr_time format here.  We convert to
 * microseconds in PgStat_Counter format when writing to shared statsitics.
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
 * Activity statistics data structures on file and shared memory follow
 *
 * PGSTAT_FILE_FORMAT_ID should be changed whenever any of these
 * data structures change.
 * ------------------------------------------------------------
 */

#define PGSTAT_FILE_FORMAT_ID	0x01A5BC9D


typedef struct PgStat_StatDBCounts
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
	PgStat_Counter n_conflict_tablespace;
	PgStat_Counter n_conflict_lock;
	PgStat_Counter n_conflict_snapshot;
	PgStat_Counter n_conflict_bufferpin;
	PgStat_Counter n_conflict_startup_deadlock;
	PgStat_Counter n_temp_files;
	PgStat_Counter n_temp_bytes;
	PgStat_Counter n_deadlocks;
	PgStat_Counter n_checksum_failures;
	PgStat_Counter n_block_read_time;	/* times in microseconds */
	PgStat_Counter n_block_write_time;
} PgStat_StatDBCounts;

/* ----------
 * PgStat_StatEntryHead			common header struct for PgStat_Stat*Entry
 * ----------
 */
typedef struct PgStat_StatEntryHeader
{
	LWLock		lock;
} PgStat_StatEntryHeader;

/* ----------
 * PgStat_StatDBEntry			The statistics per database
 * ----------
 */
typedef struct PgStat_StatDBEntry
{
	PgStat_StatEntryHeader header;	/* must be the first member, 
									   used only on shared memory  */
	TimestampTz last_autovac_time;
	TimestampTz last_checksum_failure;
	TimestampTz stat_reset_timestamp;
	TimestampTz stats_timestamp;	/* time of db stats update */

	PgStat_StatDBCounts counts;

	/*
	 * The followings must be last in the struct, because we don't write them
	 * out to the stats file.
	 */
	dshash_table_handle tables; /* current gen tables hash */
	dshash_table_handle functions;	/* current gen functions hash */
} PgStat_StatDBEntry;


/*
 * SLRU statistics kept in the stats collector
 */
typedef struct PgStat_StatSLRUEntry
{
	PgStat_Counter blocks_zeroed;
	PgStat_Counter blocks_hit;
	PgStat_Counter blocks_read;
	PgStat_Counter blocks_written;
	PgStat_Counter blocks_exists;
	PgStat_Counter flush;
	PgStat_Counter truncate;
	TimestampTz stat_reset_timestamp;
} PgStat_StatSLRUEntry;


/* ----------
 * PgStat_HashMountInfo  dshash mount information
 * ----------
 */
typedef struct PgStat_HashMountInfo
{
	HTAB	   *snapshot_tables;	/* table entry snapshot */
	HTAB	   *snapshot_functions; /* function entry snapshot */
	dshash_table *dshash_tables;	/* attached tables dshash */
	dshash_table *dshash_functions; /* attached functions dshash */
} PgStat_HashMountInfo;

/* ----------
 * PgStat_StatTabEntry			The statistics per table (or index)
 * ----------
 */
typedef struct PgStat_StatTabEntry
{
	PgStat_StatEntryHeader header;	/* must be the first member, 
									   used only on shared memory  */
	TimestampTz vacuum_timestamp;	/* user initiated vacuum */
	TimestampTz autovac_vacuum_timestamp;	/* autovacuum initiated */
	TimestampTz analyze_timestamp;	/* user initiated */
	TimestampTz autovac_analyze_timestamp;	/* autovacuum initiated */

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

	PgStat_Counter vacuum_count;
	PgStat_Counter autovac_vacuum_count;
	PgStat_Counter analyze_count;
	PgStat_Counter autovac_analyze_count;
} PgStat_StatTabEntry;


/* ----------
 * PgStat_StatFuncEntry			per function stats data
 * ----------
 */
typedef struct PgStat_StatFuncEntry
{
	PgStat_StatEntryHeader header;	/* must be the first member, 
									   used only on shared memory  */
	PgStat_Counter f_numcalls;

	PgStat_Counter f_total_time;	/* times in microseconds */
	PgStat_Counter f_self_time;
} PgStat_StatFuncEntry;

/*
 * Archiver statistics kept in the shared stats
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
 * Global statistics kept in the shared stats
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
	WAIT_EVENT_LOGICAL_APPLY_MAIN,
	WAIT_EVENT_LOGICAL_LAUNCHER_MAIN,
	WAIT_EVENT_READING_STATS_FILE,
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
	WAIT_EVENT_GSS_OPEN_SERVER,
	WAIT_EVENT_LIBPQWALRECEIVER_CONNECT,
	WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE,
	WAIT_EVENT_SSL_OPEN_SERVER,
	WAIT_EVENT_WAL_RECEIVER_WAIT_START,
	WAIT_EVENT_WAL_SENDER_WAIT_WAL,
	WAIT_EVENT_WAL_SENDER_WRITE_DATA,
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
	WAIT_EVENT_BACKUP_WAIT_WAL_ARCHIVE = PG_WAIT_IPC,
	WAIT_EVENT_BGWORKER_SHUTDOWN,
	WAIT_EVENT_BGWORKER_STARTUP,
	WAIT_EVENT_BTREE_PAGE,
	WAIT_EVENT_CHECKPOINT_DONE,
	WAIT_EVENT_CHECKPOINT_START,
	WAIT_EVENT_EXECUTE_GATHER,
	WAIT_EVENT_HASH_BATCH_ALLOCATE,
	WAIT_EVENT_HASH_BATCH_ELECT,
	WAIT_EVENT_HASH_BATCH_LOAD,
	WAIT_EVENT_HASH_BUILD_ALLOCATE,
	WAIT_EVENT_HASH_BUILD_ELECT,
	WAIT_EVENT_HASH_BUILD_HASH_INNER,
	WAIT_EVENT_HASH_BUILD_HASH_OUTER,
	WAIT_EVENT_HASH_GROW_BATCHES_ALLOCATE,
	WAIT_EVENT_HASH_GROW_BATCHES_DECIDE,
	WAIT_EVENT_HASH_GROW_BATCHES_ELECT,
	WAIT_EVENT_HASH_GROW_BATCHES_FINISH,
	WAIT_EVENT_HASH_GROW_BATCHES_REPARTITION,
	WAIT_EVENT_HASH_GROW_BUCKETS_ALLOCATE,
	WAIT_EVENT_HASH_GROW_BUCKETS_ELECT,
	WAIT_EVENT_HASH_GROW_BUCKETS_REINSERT,
	WAIT_EVENT_LOGICAL_SYNC_DATA,
	WAIT_EVENT_LOGICAL_SYNC_STATE_CHANGE,
	WAIT_EVENT_MQ_INTERNAL,
	WAIT_EVENT_MQ_PUT_MESSAGE,
	WAIT_EVENT_MQ_RECEIVE,
	WAIT_EVENT_MQ_SEND,
	WAIT_EVENT_PARALLEL_BITMAP_SCAN,
	WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN,
	WAIT_EVENT_PARALLEL_FINISH,
	WAIT_EVENT_PROCARRAY_GROUP_UPDATE,
	WAIT_EVENT_PROC_SIGNAL_BARRIER,
	WAIT_EVENT_PROMOTE,
	WAIT_EVENT_RECOVERY_CONFLICT_SNAPSHOT,
	WAIT_EVENT_RECOVERY_CONFLICT_TABLESPACE,
	WAIT_EVENT_RECOVERY_PAUSE,
	WAIT_EVENT_REPLICATION_ORIGIN_DROP,
	WAIT_EVENT_REPLICATION_SLOT_DROP,
	WAIT_EVENT_SAFE_SNAPSHOT,
	WAIT_EVENT_SYNC_REP,
	WAIT_EVENT_XACT_GROUP_UPDATE
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
	WAIT_EVENT_RECOVERY_APPLY_DELAY,
	WAIT_EVENT_RECOVERY_RETRIEVE_RETRY_INTERVAL,
	WAIT_EVENT_VACUUM_DELAY
} WaitEventTimeout;

/* ----------
 * Wait Events - IO
 *
 * Use this category when a process is waiting for a IO.
 * ----------
 */
typedef enum
{
	WAIT_EVENT_BASEBACKUP_READ = PG_WAIT_IO,
	WAIT_EVENT_BUFFILE_READ,
	WAIT_EVENT_BUFFILE_WRITE,
	WAIT_EVENT_BUFFILE_TRUNCATE,
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
	WAIT_EVENT_WAL_WRITE,
	WAIT_EVENT_LOGICAL_CHANGES_READ,
	WAIT_EVENT_LOGICAL_CHANGES_WRITE,
	WAIT_EVENT_LOGICAL_SUBXACT_READ,
	WAIT_EVENT_LOGICAL_SUBXACT_WRITE
} WaitEventIO;

/* ----------
 * Command type for progress reporting purposes
 * ----------
 */
typedef enum ProgressCommandType
{
	PROGRESS_COMMAND_INVALID,
	PROGRESS_COMMAND_VACUUM,
	PROGRESS_COMMAND_ANALYZE,
	PROGRESS_COMMAND_CLUSTER,
	PROGRESS_COMMAND_CREATE_INDEX,
	PROGRESS_COMMAND_BASEBACKUP
} ProgressCommandType;

#define PGSTAT_NUM_PROGRESS_PARAM	20

/* ----------
 * Shared-memory data structures
 * ----------
 */


/*
 * PgBackendSSLStatus
 *
 * For each backend, we keep the SSL status in a separate struct, that
 * is only filled in if SSL is enabled.
 *
 * All char arrays must be null-terminated.
 */
typedef struct PgBackendSSLStatus
{
	/* Information about SSL connection */
	int			ssl_bits;
	bool		ssl_compression;
	char		ssl_version[NAMEDATALEN];
	char		ssl_cipher[NAMEDATALEN];
	char		ssl_client_dn[NAMEDATALEN];

	/*
	 * serial number is max "20 octets" per RFC 5280, so this size should be
	 * fine
	 */
	char		ssl_client_serial[NAMEDATALEN];

	char		ssl_issuer_dn[NAMEDATALEN];
} PgBackendSSLStatus;

/*
 * PgBackendGSSStatus
 *
 * For each backend, we keep the GSS status in a separate struct, that
 * is only filled in if GSS is enabled.
 *
 * All char arrays must be null-terminated.
 */
typedef struct PgBackendGSSStatus
{
	/* Information about GSSAPI connection */
	char		gss_princ[NAMEDATALEN]; /* GSSAPI Principal used to auth */
	bool		gss_auth;		/* If GSSAPI authentication was used */
	bool		gss_enc;		/* If encryption is being used */

} PgBackendGSSStatus;


/* ----------
 * PgBackendStatus
 *
 * Each live backend maintains a PgBackendStatus struct in shared memory
 * showing its current activity.  (The structs are allocated according to
 * BackendId, but that is not critical.)  Note that the stats-writing functions
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
	 * The above protocol needs memory barriers to ensure that the apparent
	 * order of execution is as it desires.  Otherwise, for example, the CPU
	 * might rearrange the code so that st_changecount is incremented twice
	 * before the modification on a machine with weak memory ordering.  Hence,
	 * use the macros defined below for manipulating st_changecount, rather
	 * than touching it directly.
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

	/* Information about GSSAPI connection */
	bool		st_gss;
	PgBackendGSSStatus *st_gssstatus;

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
 * Macros to load and store st_changecount with appropriate memory barriers.
 *
 * Use PGSTAT_BEGIN_WRITE_ACTIVITY() before, and PGSTAT_END_WRITE_ACTIVITY()
 * after, modifying the current process's PgBackendStatus data.  Note that,
 * since there is no mechanism for cleaning up st_changecount after an error,
 * THESE MACROS FORM A CRITICAL SECTION.  Any error between them will be
 * promoted to PANIC, causing a database restart to clean up shared memory!
 * Hence, keep the critical section as short and straight-line as possible.
 * Aside from being safer, that minimizes the window in which readers will
 * have to loop.
 *
 * Reader logic should follow this sketch:
 *
 *		for (;;)
 *		{
 *			int before_ct, after_ct;
 *
 *			pgstat_begin_read_activity(beentry, before_ct);
 *			... copy beentry data to local memory ...
 *			pgstat_end_read_activity(beentry, after_ct);
 *			if (pgstat_read_activity_complete(before_ct, after_ct))
 *				break;
 *			CHECK_FOR_INTERRUPTS();
 *		}
 *
 * For extra safety, we generally use volatile beentry pointers, although
 * the memory barriers should theoretically be sufficient.
 */
#define PGSTAT_BEGIN_WRITE_ACTIVITY(beentry) \
	do { \
		START_CRIT_SECTION(); \
		(beentry)->st_changecount++; \
		pg_write_barrier(); \
	} while (0)

#define PGSTAT_END_WRITE_ACTIVITY(beentry) \
	do { \
		pg_write_barrier(); \
		(beentry)->st_changecount++; \
		Assert(((beentry)->st_changecount & 1) == 0); \
		END_CRIT_SECTION(); \
	} while (0)

#define pgstat_begin_read_activity(beentry, before_changecount) \
	do { \
		(before_changecount) = (beentry)->st_changecount; \
		pg_read_barrier(); \
	} while (0)

#define pgstat_end_read_activity(beentry, after_changecount) \
	do { \
		pg_read_barrier(); \
		(after_changecount) = (beentry)->st_changecount; \
	} while (0)

#define pgstat_read_activity_complete(before_changecount, after_changecount) \
	((before_changecount) == (after_changecount) && \
	 ((before_changecount) & 1) == 0)


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
extern PGDLLIMPORT bool pgstat_track_activities;
extern PGDLLIMPORT bool pgstat_track_counts;
extern PGDLLIMPORT int pgstat_track_functions;
extern PGDLLIMPORT int pgstat_track_activity_query_size;
extern char *pgstat_stat_directory;

/* No longer used, but will be removed with GUC */
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

extern Size StatsShmemSize(void);
extern void StatsShmemInit(void);

extern void pgstat_reset_all(void);

/* File input/output functions  */
extern void pgstat_read_statsfiles(void);
extern void pgstat_write_statsfiles(void);

/* ----------
 * Functions called from backends
 * ----------
 */
extern long pgstat_report_stat(bool force);
extern void pgstat_vacuum_stat(void);
extern void pgstat_drop_database(Oid databaseid);

extern void pgstat_clear_snapshot(void);
extern void pgstat_reset_counters(void);
extern void pgstat_reset_shared_counters(const char *target);
extern void pgstat_reset_single_counter(Oid objectid, PgStat_Single_Reset_Type type);
extern void pgstat_reset_slru_counter(const char *);

extern void pgstat_report_autovac(Oid dboid);
extern void pgstat_report_vacuum(Oid tableoid, bool shared,
								 PgStat_Counter livetuples, PgStat_Counter deadtuples);
extern void pgstat_report_analyze(Relation rel,
								  PgStat_Counter livetuples, PgStat_Counter deadtuples,
								  bool resetcounter);

extern void pgstat_report_recovery_conflict(int reason);
extern void pgstat_report_deadlock(void);
extern void pgstat_report_checksum_failures_in_db(Oid dboid, int failurecount);
extern void pgstat_report_checksum_failure(void);

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

struct FunctionCallInfoBaseData;
extern void pgstat_init_function_usage(struct FunctionCallInfoBaseData *fcinfo,
									   PgStat_FunctionCallUsage *fcu);
extern void pgstat_end_function_usage(PgStat_FunctionCallUsage *fcu,
									  bool finalize);

extern void AtEOXact_PgStat(bool isCommit, bool parallel);
extern void AtEOSubXact_PgStat(bool isCommit, int nestDepth);

extern void AtPrepare_PgStat(void);
extern void PostPrepare_PgStat(void);

extern void pgstat_twophase_postcommit(TransactionId xid, uint16 info,
									   void *recdata, uint32 len);
extern void pgstat_twophase_postabort(TransactionId xid, uint16 info,
									  void *recdata, uint32 len);

extern void pgstat_report_archiver(const char *xlog, bool failed);
extern void pgstat_report_bgwriter(void);

/* ----------
 * Support functions for the SQL-callable functions to
 * generate the pgstat* views.
 * ----------
 */
extern PgStat_StatDBEntry *pgstat_fetch_stat_dbentry(Oid dbid);
extern PgStat_StatTabEntry *pgstat_fetch_stat_tabentry(Oid relid);
extern PgStat_StatTabEntry *pgstat_fetch_stat_tabentry_snapshot(bool shared,
																Oid relid);
extern void pgstat_copy_index_counters(Oid relid, PgStat_TableStatus *dst);
extern PgBackendStatus *pgstat_fetch_stat_beentry(int beid);
extern LocalPgBackendStatus *pgstat_fetch_stat_local_beentry(int beid);
extern PgStat_StatFuncEntry *pgstat_fetch_stat_funcentry(Oid funcid);
extern int	pgstat_fetch_stat_numbackends(void);
extern PgStat_ArchiverStats *pgstat_fetch_stat_archiver(void);
extern PgStat_GlobalStats *pgstat_fetch_global(void);
extern PgStat_StatSLRUEntry *pgstat_fetch_slru(void);

extern void pgstat_count_slru_page_zeroed(int slru_idx);
extern void pgstat_count_slru_page_hit(int slru_idx);
extern void pgstat_count_slru_page_read(int slru_idx);
extern void pgstat_count_slru_page_written(int slru_idx);
extern void pgstat_count_slru_page_exists(int slru_idx);
extern void pgstat_count_slru_flush(int slru_idx);
extern void pgstat_count_slru_truncate(int slru_idx);
extern const char *pgstat_slru_name(int slru_idx);
extern int	pgstat_slru_index(const char *name);
extern void pgstat_clear_snapshot(void);

#endif							/* PGSTAT_H */
