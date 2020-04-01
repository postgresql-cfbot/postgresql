/* ----------
 * pgstat.c
 *
 *	Activity Statistics facility.
 *
 *  Collects activity statistics, e.g. per-table access statistics, of
 *  all backends in shared memory. The activity numbers are first stored
 *  locally in each process, then flushed to shared memory at commit
 *  time or by idle-timeout.
 *
 * To avoid congestion on the shared memory, shared stats is updated no more
 * often than once per PGSTAT_MIN_INTERVAL (1000ms). If some local numbers
 * remain unflushed for lock failure, retry with intervals that is initially
 * PGSTAT_RETRY_MIN_INTERVAL (250ms) then doubled at every retry. Finally we
 * force update after PGSTAT_MAX_INTERVAL (10000ms) since the first trial.
 *
 *  The first process that uses activity statistics facility creates the area
 *  then load the stored stats file if any, and the last process at shutdown
 *  writes the shared stats to the file then destroy the area before exit.
 *
 *	Copyright (c) 2001-2020, PostgreSQL Global Development Group
 *
 *	src/backend/postmaster/pgstat.c
 * ----------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "catalog/pg_proc.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/fork_process.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "replication/walsender.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "utils/ascii.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/probes.h"
#include "utils/snapmgr.h"

/* ----------
 * Timer definitions.
 * ----------
 */
#define PGSTAT_MIN_INTERVAL			1000	/* Minimum interval of stats data
											 * updates; in milliseconds. */

#define PGSTAT_RETRY_MIN_INTERVAL	250 /* Initial retry interval after
										 * PGSTAT_MIN_INTERVAL */

#define PGSTAT_MAX_INTERVAL			10000	/* Longest interval of stats data
											 * updates */

/* ----------
 * The initial size hints for the hash tables used in the activity statistics.
 * ----------
 */
#define PGSTAT_TABLE_HASH_SIZE	512
#define PGSTAT_FUNCTION_HASH_SIZE	512


/* ----------
 * Total number of backends including auxiliary
 *
 * We reserve a slot for each possible BackendId, plus one for each
 * possible auxiliary process type.  (This scheme assumes there is not
 * more than one of any auxiliary process type at a time.) MaxBackends
 * includes autovacuum workers and background workers as well.
 * ----------
 */
#define NumBackendStatSlots (MaxBackends + NUM_AUXPROCTYPES)

/* ----------
 * GUC parameters
 * ----------
 */
bool		pgstat_track_activities = false;
bool		pgstat_track_counts = false;
int			pgstat_track_functions = TRACK_FUNC_OFF;
int			pgstat_track_activity_query_size = 1024;

/*
 * This used to be a GUC variable and is no longer used in this file, but left
 * alone just for backward compatibility for extensions, having the default
 * value.
 */
char	   *pgstat_stat_directory = PG_STAT_TMP_DIR;

/*
 * Shared stats bootstrap information, protected by StatsLock.
 *
 * refcount is used to know whether a process going to detach shared stats is
 * the last process or not. The last process writes out the stats files.
 */
typedef struct StatsShmemStruct
{
	dsa_handle	stats_dsa_handle;	/* handle for stats data area */
	dshash_table_handle hash_handle;	/* shared dbstat hash */
	dsa_pointer global_stats;	/* DSA pointer to global stats */
	dsa_pointer archiver_stats; /* Ditto for archiver stats */
	int			refcount;		/* # of processes that is attaching the shared
								 * stats memory */
}			StatsShmemStruct;

/* BgWriter global statistics counters */
PgStat_BgWriter BgWriterStats = {0};

/* backend-lifetime storages */
static StatsShmemStruct * StatsShmem = NULL;
static dsa_area *area = NULL;

/*
 * Enums and types to define shared statistics structure.
 *
 * Statistics entries for each object is stored in individual DSA-allocated
 * memory. Every entry is pointed from the dshash pgStatSharedHash via
 * dsa_pointer. The structure makes object-stats entries not moved by dshash
 * resizing, and allows the dshash can release lock sooner on stats
 * updates. Also it reduces interfering among write-locks on each stat entry by
 * not relying on partition lock of dshash. PgStatLocalHashEntry is the
 * local-stats equivalent of PgStatHashEntry for shared stat entries.
 *
 * Each stat entry is enveloped by the type PgStatEnvelope, which stores common
 * attribute of all kind of statistics and a LWLock lock object.
 *
 * Shared stats are stored as:
 *
 * dshash pgStatSharedHash
 *    -> PgStatHashEntry				(dshash entry)
 *      (dsa_pointer)-> PgStatEnvelope	(dsa memory block)
 *
 * Local stats are stored as:
 *
 * dshash pgStatLocalHash
 *    -> PgStatLocalHashEntry			(dynahash entry)
 *      (direct pointer)-> PgStatEnvelope (palloc'ed memory)
 */

/* The types of statistics entries. */
typedef enum PgStatTypes
{
	PGSTAT_TYPE_ALL,			/* Not a type, for the parameters of
								 * pgstat_collect_stat_entries */
	PGSTAT_TYPE_DB,				/* database-wide statistics */
	PGSTAT_TYPE_TABLE,			/* per-table statistics */
	PGSTAT_TYPE_FUNCTION,		/* per-function statistics */
}			PgStatTypes;

/*
 * entry size lookup table of shared statistics entries corresponding to
 * PgStatTypes
 */
static size_t pgstat_entsize[] =
{
	0,							/* PGSTAT_TYPE_ALL: not an entry */
	sizeof(PgStat_StatDBEntry), /* PGSTAT_TYPE_DB */
	sizeof(PgStat_StatTabEntry),	/* PGSTAT_TYPE_TABLE */
	sizeof(PgStat_StatFuncEntry)	/* PGSTAT_TYPE_FUNCTION */
};

/* Ditto for local statistics entries */
static size_t pgstat_localentsize[] =
{
	0,							/* PGSTAT_TYPE_ALL: not an entry */
	sizeof(PgStat_StatDBEntry), /* PGSTAT_TYPE_DB */
	sizeof(PgStat_TableStatus), /* PGSTAT_TYPE_TABLE */
	sizeof(PgStat_BackendFunctionEntry) /* PGSTAT_TYPE_FUNCTION */
};

/* struct for shared statistics hash entry key. */
typedef struct PgStatHashEntryKey
{
	PgStatTypes type;			/* statistics entry type */
	Oid			databaseid;		/* database ID. InvalidOid for shared objects. */
	Oid			objectid;		/* object OID */
}			PgStatHashEntryKey;

/*
 * Stats numbers that are waiting for flushing out to shared stats are held in
 * pgStatLocalHash,
 */
typedef struct PgStatHashEntry
{
	PgStatHashEntryKey key;		/* hash key */
	dsa_pointer env;			/* pointer to shared stats envelope in DSA */
}			PgStatHashEntry;

/* struct for shared statistics entry pointed from shared hash entry. */
typedef struct PgStatEnvelope
{
	PgStatTypes type;			/* statistics entry type */
	Oid			databaseid;		/* databaseid */
	Oid			objectid;		/* objectid */
	size_t		len;			/* length of body, fixed per type. */
	LWLock		lock;			/* lightweight lock to protect body */
	int			body[FLEXIBLE_ARRAY_MEMBER];	/* statistics body */
}			PgStatEnvelope;

#define PgStatEnvelopeSize(bodylen) \
	(offsetof(PgStatEnvelope, body) + (bodylen))

/* struct for shared statistics local hash entry. */
typedef struct PgStatLocalHashEntry
{
	PgStatHashEntryKey key;		/* hash key */
	PgStatEnvelope *env;		/* pointer to stats envelope in heap */
}			PgStatLocalHashEntry;

/*
 * Snapshot is stats entry that is locally copied to offset stable values for a
 * transaction.
 */
typedef struct PgStatSnapshot
{
	PgStatHashEntryKey key;
	bool		negative;
	int			body[FLEXIBLE_ARRAY_MEMBER];	/* statistics body */
}			PgStatSnapshot;

#define PgStatSnapshotSize(bodylen)				\
	(offsetof(PgStatSnapshot, body) + (bodylen))

/* parameter for shared hashes */
static const dshash_parameters dsh_rootparams = {
	sizeof(PgStatHashEntryKey),
	sizeof(PgStatHashEntry),
	dshash_memcmp,
	dshash_memhash,
	LWTRANCHE_STATS
};

/* The shared hash to index activity stats entries. */
static dshash_table *pgStatSharedHash = NULL;

/* Local stats numbers are stored here */
static HTAB *pgStatLocalHash = NULL;

#define HAVE_ANY_PENDING_STATS()						\
	(pgStatLocalHash != NULL ||							\
	 pgStatXactCommit != 0 || pgStatXactRollback != 0)

/*
 * Tuple insertion/deletion counts for an open transaction can't be propagated
 * into PgStat_TableStatus counters until we know if it is going to commit
 * or abort.  Hence, we keep these counts in per-subxact structs that live
 * in TopTransactionContext.  This data structure is designed on the assumption
 * that subxacts won't usually modify very many tables.
 */
typedef struct PgStat_SubXactStatus
{
	int			nest_level;		/* subtransaction nest level */
	struct PgStat_SubXactStatus *prev;	/* higher-level subxact if any */
	PgStat_TableXactStatus *first;	/* head of list for this subxact */
} PgStat_SubXactStatus;

static PgStat_SubXactStatus *pgStatXactStack = NULL;

static int	pgStatXactCommit = 0;
static int	pgStatXactRollback = 0;
PgStat_Counter pgStatBlockReadTime = 0;
PgStat_Counter pgStatBlockWriteTime = 0;

/* Record that's written to 2PC state file when pgstat state is persisted */
typedef struct TwoPhasePgStatRecord
{
	PgStat_Counter tuples_inserted; /* tuples inserted in xact */
	PgStat_Counter tuples_updated;	/* tuples updated in xact */
	PgStat_Counter tuples_deleted;	/* tuples deleted in xact */
	PgStat_Counter inserted_pre_trunc;	/* tuples inserted prior to truncate */
	PgStat_Counter updated_pre_trunc;	/* tuples updated prior to truncate */
	PgStat_Counter deleted_pre_trunc;	/* tuples deleted prior to truncate */
	Oid			t_id;			/* table's OID */
	bool		t_shared;		/* is it a shared catalog? */
	bool		t_truncated;	/* was the relation truncated? */
} TwoPhasePgStatRecord;

/* Variables for backend status snapshot */
static MemoryContext pgStatLocalContext = NULL;
static MemoryContext pgStatSnapshotContext = NULL;
static HTAB *pgStatSnapshotHash = NULL;

/* Status for backends including auxiliary */
static LocalPgBackendStatus *localBackendStatusTable = NULL;

/* Total number of backends including auxiliary */
static int	localNumBackends = 0;

/*
 * Cluster wide statistics.
 *
 * Contains statistics that are collected not per database nor per table
 * basis.  shared_* points to shared memory and snapshot_* are backend
 * snapshots.
 */
static bool global_snapshot_is_valid = false;
static PgStat_ArchiverStats *shared_archiverStats;
static PgStat_ArchiverStats snapshot_archiverStats;
static PgStat_GlobalStats *shared_globalStats;
static PgStat_GlobalStats snapshot_globalStats;

/*
 * Total time charged to functions so far in the current backend.
 * We use this to help separate "self" and "other" time charges.
 * (We assume this initializes to zero.)
 */
static instr_time total_func_time;

/*
 * Newly created shared stats entries needs to be initialized before the other
 * processes get access it. get_stat_entry() calls it for the purpose.
 */
typedef void (*entry_initializer) (PgStatEnvelope * env);

/* ----------
 * Local function forward declarations
 * ----------
 */
static void pgstat_beshutdown_hook(int code, Datum arg);

static PgStatEnvelope * get_stat_entry(PgStatTypes type, Oid dbid, Oid objid,
									   bool nowait,
									   entry_initializer initfunc, bool *found);
static PgStatEnvelope * *collect_stat_entries(PgStatTypes type, Oid dbid);
static void create_missing_dbentries(void);
static void pgstat_write_database_stats(PgStat_StatDBEntry *dbentry);
static void pgstat_read_db_statsfile(PgStat_StatDBEntry *dbentry);

static bool flush_tabstat(PgStatEnvelope * env, bool nowait);
static bool flush_funcstat(PgStatEnvelope * env, bool nowait);
static bool flush_dbstat(PgStatEnvelope * env, bool nowait);

static void init_dbentry(PgStatEnvelope * env);
static void init_funcentry(PgStatEnvelope * env);
static void init_tabentry(PgStatEnvelope * env);

static bool delete_stat_entry(PgStatTypes type, Oid dbid, Oid objid,
							  bool nowait);
static PgStatEnvelope * get_local_stat_entry(PgStatTypes type, Oid dbid, Oid objid,
											 bool create, bool *found);
static PgStat_StatDBEntry *get_local_dbstat_entry(Oid dbid);
static PgStat_TableStatus *get_local_tabstat_entry(Oid rel_id, bool isshared);

static PgStat_SubXactStatus *get_tabstat_stack_level(int nest_level);
static void add_tabstat_xact_level(PgStat_TableStatus *pgstat_info, int nest_level);
static void pgstat_snapshot_global_stats(void);

static void pgstat_read_current_status(void);
static const char *pgstat_get_wait_activity(WaitEventActivity w);
static const char *pgstat_get_wait_client(WaitEventClient w);
static const char *pgstat_get_wait_ipc(WaitEventIPC w);
static const char *pgstat_get_wait_timeout(WaitEventTimeout w);
static const char *pgstat_get_wait_io(WaitEventIO w);

/* ------------------------------------------------------------
 * Public functions called from postmaster follow
 * ------------------------------------------------------------
 */

/*
 * StatsShmemSize
 *		Compute shared memory space needed for activity statistic
 */
Size
StatsShmemSize(void)
{
	return sizeof(StatsShmemStruct);
}

/*
 * StatsShmemInit - initialize during shared-memory creation
 */
void
StatsShmemInit(void)
{
	bool		found;

	StatsShmem = (StatsShmemStruct *)
		ShmemInitStruct("Stats area", StatsShmemSize(),
						&found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);

		StatsShmem->stats_dsa_handle = DSM_HANDLE_INVALID;
	}
}

/* ----------
 * pgstat_setup_memcxt() -
 *
 *	Create pgStatLocalContext and pgStatSnapshotContext, if not already done.
 * ----------
 */
static void
pgstat_setup_memcxt(void)
{
	if (!pgStatLocalContext)
		pgStatLocalContext =
			AllocSetContextCreate(TopMemoryContext,
								  "Backend statistics snapshot",
								  ALLOCSET_SMALL_SIZES);

	if (!pgStatSnapshotContext)
		pgStatSnapshotContext =
			AllocSetContextCreate(TopMemoryContext,
								  "Database statistics snapshot",
								  ALLOCSET_SMALL_SIZES);
}


/* ----------
 * attach_shared_stats() -
 *
 *	Attach shared or create stats memory. If we are the first process to use
 *	activity stats system, read saved statistics files if any.
 * ---------
 */
static void
attach_shared_stats(void)
{
	MemoryContext oldcontext;

	/*
	 * Don't use dsm under postmaster, or when not tracking counts.
	 */
	if (!pgstat_track_counts || !IsUnderPostmaster)
		return;

	pgstat_setup_memcxt();

	if (area)
		return;

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	LWLockAcquire(StatsLock, LW_EXCLUSIVE);

	/*
	 * The last process is responsible to write out stats files at exit.
	 * Maintain refcount so that processes going to exit can find whether it
	 * is the last or not.
	 */
	if (StatsShmem->refcount > 0)
		StatsShmem->refcount++;
	else
	{
		/* We're the first process to attach the shared stats memory */
		Assert(StatsShmem->stats_dsa_handle == DSM_HANDLE_INVALID);

		/* Initialize shared memory area */
		area = dsa_create(LWTRANCHE_STATS);
		pgStatSharedHash = dshash_create(area, &dsh_rootparams, 0);

		StatsShmem->stats_dsa_handle = dsa_get_handle(area);
		StatsShmem->global_stats =
			dsa_allocate0(area, sizeof(PgStat_GlobalStats));
		StatsShmem->archiver_stats =
			dsa_allocate0(area, sizeof(PgStat_ArchiverStats));
		StatsShmem->hash_handle = dshash_get_hash_table_handle(pgStatSharedHash);

		shared_globalStats = (PgStat_GlobalStats *)
			dsa_get_address(area, StatsShmem->global_stats);
		shared_archiverStats = (PgStat_ArchiverStats *)
			dsa_get_address(area, StatsShmem->archiver_stats);

		/* Load saved data if any. */
		pgstat_read_statsfiles();

		StatsShmem->refcount = 1;
	}

	LWLockRelease(StatsLock);

	/*
	 * If we're not the first process, attach existing shared stats area
	 * outside the StatsLock section.
	 */
	if (!area)
	{
		/* Attach shared area. */
		area = dsa_attach(StatsShmem->stats_dsa_handle);
		pgStatSharedHash = dshash_attach(area, &dsh_rootparams,
										 StatsShmem->hash_handle, 0);

		/* Setup local variables */
		pgStatLocalHash = NULL;
		shared_globalStats = (PgStat_GlobalStats *)
			dsa_get_address(area, StatsShmem->global_stats);
		shared_archiverStats = (PgStat_ArchiverStats *)
			dsa_get_address(area, StatsShmem->archiver_stats);
	}

	MemoryContextSwitchTo(oldcontext);

	/* don't detach automatically */
	dsa_pin_mapping(area);
	global_snapshot_is_valid = false;
}

/* ----------
 * detach_shared_stats() -
 *
 *	Detach shared stats. Write out to file if we're the last process and told
 *	to do so.
 * ----------
 */
static void
detach_shared_stats(bool write_stats)
{
	/* immediately return if useless */
	if (!area || !IsUnderPostmaster)
		return;

	LWLockAcquire(StatsLock, LW_EXCLUSIVE);

	if (--StatsShmem->refcount < 1)
	{
		/*
		 * The process is the last one that is attaching the shared stats
		 * memory. Write out the stats files if requested.
		 */
		if (write_stats)
			pgstat_write_statsfiles();

		/* No one is using the area. */
		StatsShmem->stats_dsa_handle = DSM_HANDLE_INVALID;
	}

	LWLockRelease(StatsLock);

	/*
	 * Detach the area. Automatically destroyed when the last process detached
	 * it.
	 */
	dsa_detach(area);

	area = NULL;
	pgStatSharedHash = NULL;
	shared_globalStats = NULL;
	shared_archiverStats = NULL;
	pgStatLocalHash = NULL;
	global_snapshot_is_valid = false;
}

/*
 * pgstat_reset_all() -
 *
 * Remove the stats file.  This is currently used only if WAL recovery is
 * needed after a crash.
 */
void
pgstat_reset_all(void)
{
	/* standalone server doesn't use shared stats */
	if (!IsUnderPostmaster)
		return;

	/* we must have shared stats attached */
	Assert(StatsShmem->stats_dsa_handle != DSM_HANDLE_INVALID);

	/* Startup must be the only user of shared stats */
	Assert(StatsShmem->refcount == 1);

	/*
	 * We could directly remove files and recreate the shared memory area. But
	 * just discard  then create for simplicity.
	 */
	detach_shared_stats(false); /* Don't write files. */
	attach_shared_stats();
}

/* ------------------------------------------------------------
 * Public functions used by backends follow
 *------------------------------------------------------------
 */

/* ----------
 * pgstat_report_stat() -
 *
 *	Must be called by processes that performs DML: tcop/postgres.c, logical
 *	receiver processes, SPI worker, etc. to apply the so far collected
 *	per-table and function usage statistics to the shared statistics hashes.
 *
 *	Updates are applied not more frequent than the interval of
 *	PGSTAT_MIN_INTERVAL milliseconds. They are also postponed on lock
 *	failure if force is false and there's no pending updates longer than
 *	PGSTAT_MAX_INTERVAL milliseconds. Postponed updates are retried in
 *	succeeding calls of this function.
 *
 *	Returns the time until the next timing when updates are applied in
 *	milliseconds if there are no updates held for more than
 *	PGSTAT_MIN_INTERVAL milliseconds.
 *
 *	Note that this is called only out of a transaction, so it is fine to use
 *	transaction stop time as an approximation of current time.
 *	----------
 */
long
pgstat_report_stat(bool force)
{
	static TimestampTz next_flush = 0;
	static TimestampTz pending_since = 0;
	static long retry_interval = 0;
	TimestampTz now;
	bool		nowait = !force;	/* Don't use force ever after */
	HASH_SEQ_STATUS scan;
	PgStatLocalHashEntry *lent;
	PgStatLocalHashEntry **dbentlist;
	int			dbentlistlen = 8;
	int			ndbentries = 0;
	int			remains = 0;
	int			i;

	/* Don't expend a clock check if nothing to do */
	if (area == NULL || !HAVE_ANY_PENDING_STATS())
		return 0;

	dbentlist = palloc(sizeof(PgStatLocalHashEntry *) * dbentlistlen);

	now = GetCurrentTransactionStopTimestamp();

	if (nowait)
	{
		/*
		 * Don't flush stats too frequently.  Return the time to the next
		 * flush.
		 */
		if (now < next_flush)
		{
			/* Record the epoch time if retrying. */
			if (pending_since == 0)
				pending_since = now;

			return (next_flush - now) / 1000;
		}

		/* But, don't keep pending updates longer than PGSTAT_MAX_INTERVAL. */

		if (pending_since > 0 &&
			TimestampDifferenceExceeds(pending_since, now, PGSTAT_MAX_INTERVAL))
			nowait = false;
	}

	/*
	 * flush_tabstat applies some of stats numbers of flushed entries into
	 * local database stats. So flush-out database stats later.
	 */
	if (pgStatLocalHash)
	{
		/* Step 1: flush out other than database stats */
		hash_seq_init(&scan, pgStatLocalHash);
		while ((lent = (PgStatLocalHashEntry *) hash_seq_search(&scan)) != NULL)
		{
			bool		remove = false;

			switch (lent->env->type)
			{
				case PGSTAT_TYPE_DB:
					if (ndbentries >= dbentlistlen)
					{
						dbentlistlen *= 2;
						dbentlist = repalloc(dbentlist,
											 sizeof(PgStatLocalHashEntry *) *
											 dbentlistlen);
					}
					dbentlist[ndbentries++] = lent;
					break;
				case PGSTAT_TYPE_TABLE:
					if (flush_tabstat(lent->env, nowait))
						remove = true;
					break;
				case PGSTAT_TYPE_FUNCTION:
					if (flush_funcstat(lent->env, nowait))
						remove = true;
					break;
				default:
					Assert(false);
			}

			if (!remove)
			{
				remains++;
				continue;
			}

			/* Remove the successfully flushed entry */
			pfree(lent->env);
			hash_search(pgStatLocalHash, &lent->key, HASH_REMOVE, NULL);
		}

		/* Step 2: flush out database stats */
		for (i = 0; i < ndbentries; i++)
		{
			PgStatLocalHashEntry *lent = dbentlist[i];

			if (flush_dbstat(lent->env, nowait))
			{
				remains--;
				/* Remove the successfully flushed entry */
				pfree(lent->env);
				hash_search(pgStatLocalHash, &lent->key, HASH_REMOVE, NULL);
			}
		}
		pfree(dbentlist);

		if (remains <= 0)
		{
			hash_destroy(pgStatLocalHash);
			pgStatLocalHash = NULL;
		}
	}

	/* Publish the last flush time */
	LWLockAcquire(StatsLock, LW_EXCLUSIVE);
	if (shared_globalStats->stats_timestamp < now)
		shared_globalStats->stats_timestamp = now;
	LWLockRelease(StatsLock);

	/*
	 * If we have pending local stats, let the caller know the retry interval.
	 */
	if (HAVE_ANY_PENDING_STATS())
	{
		/* Retain the epoch time */
		if (pending_since == 0)
			pending_since = now;

		/* The interval is doubled at every retry. */
		if (retry_interval == 0)
			retry_interval = PGSTAT_RETRY_MIN_INTERVAL * 1000;
		else
			retry_interval = retry_interval * 2;

		/*
		 * Determine the next retry interval so as not to get shorter than the
		 * previous interval.
		 */
		if (!TimestampDifferenceExceeds(pending_since,
										now + 2 * retry_interval,
										PGSTAT_MAX_INTERVAL))
			next_flush = now + retry_interval;
		else
		{
			next_flush = pending_since + PGSTAT_MAX_INTERVAL * 1000;
			retry_interval = next_flush - now;
		}

		return retry_interval / 1000;
	}

	/* Set the next time to update stats */
	next_flush = now + PGSTAT_MIN_INTERVAL * 1000;
	retry_interval = 0;
	pending_since = 0;

	return 0;
}

/*
 * flush_tabstat - flush out a local table stats entry
 *
 * Some of the stats numbers are copied to local database stats entry after
 * successful flush-out.
 *
 * If nowait is true, this function returns false on lock failure. Otherwise
 * this function always returns true.
 *
 * Returns true if the entry is successfully flushed out.
 */
static bool
flush_tabstat(PgStatEnvelope * lenv, bool nowait)
{
	static const PgStat_TableCounts all_zeroes;
	Oid			dboid;			/* database OID of the table */
	PgStat_TableStatus *lstats; /* local stats entry  */
	PgStatEnvelope *shenv;		/* shared stats envelope */
	PgStat_StatTabEntry *shtabstats;	/* table entry of shared stats */
	PgStat_StatDBEntry *ldbstats;	/* local database entry */
	bool		found;

	Assert(lenv->type == PGSTAT_TYPE_TABLE);

	lstats = (PgStat_TableStatus *) &lenv->body;
	dboid = lstats->t_shared ? InvalidOid : MyDatabaseId;

	/*
	 * Ignore entries that didn't accumulate any actual counts, such as
	 * indexes that were opened by the planner but not used.
	 */
	if (memcmp(&lstats->t_counts, &all_zeroes,
			   sizeof(PgStat_TableCounts)) == 0)
		return true;

	/* find shared table stats entry corresponding to the local entry */
	shenv = get_stat_entry(PGSTAT_TYPE_TABLE, dboid, lstats->t_id,
						   nowait, init_tabentry, &found);

	/* skip if dshash failed to acquire lock */
	if (shenv == NULL)
		return false;

	/* retrieve the shared table stats entry from the envelope */
	shtabstats = (PgStat_StatTabEntry *) &shenv->body;

	/* lock the shared entry to protect the content, skip if failed */
	if (!nowait)
		LWLockAcquire(&shenv->lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(&shenv->lock, LW_EXCLUSIVE))
		return false;

	/* add the values to the shared entry. */
	shtabstats->numscans += lstats->t_counts.t_numscans;
	shtabstats->tuples_returned += lstats->t_counts.t_tuples_returned;
	shtabstats->tuples_fetched += lstats->t_counts.t_tuples_fetched;
	shtabstats->tuples_inserted += lstats->t_counts.t_tuples_inserted;
	shtabstats->tuples_updated += lstats->t_counts.t_tuples_updated;
	shtabstats->tuples_deleted += lstats->t_counts.t_tuples_deleted;
	shtabstats->tuples_hot_updated += lstats->t_counts.t_tuples_hot_updated;

	/*
	 * If table was truncated or vacuum/analyze has ran, first reset the
	 * live/dead counters.
	 */
	if (lstats->t_counts.t_truncated ||
		lstats->t_counts.vacuum_count > 0 ||
		lstats->t_counts.analyze_count > 0 ||
		lstats->t_counts.autovac_vacuum_count > 0 ||
		lstats->t_counts.autovac_analyze_count > 0)
	{
		shtabstats->n_live_tuples = 0;
		shtabstats->n_dead_tuples = 0;
	}

	/* clear the change counter if requested */
	if (lstats->t_counts.reset_changed_tuples)
		shtabstats->changes_since_analyze = 0;

	shtabstats->n_live_tuples += lstats->t_counts.t_delta_live_tuples;
	shtabstats->n_dead_tuples += lstats->t_counts.t_delta_dead_tuples;
	shtabstats->changes_since_analyze += lstats->t_counts.t_changed_tuples;
	shtabstats->blocks_fetched += lstats->t_counts.t_blocks_fetched;
	shtabstats->blocks_hit += lstats->t_counts.t_blocks_hit;

	/*
	 * Update vacuum/analyze timestamp and counters, so that the values won't
	 * goes back.
	 */
	if (shtabstats->vacuum_timestamp < lstats->vacuum_timestamp)
		shtabstats->vacuum_timestamp = lstats->vacuum_timestamp;
	shtabstats->vacuum_count += lstats->t_counts.vacuum_count;

	if (shtabstats->autovac_vacuum_timestamp < lstats->autovac_vacuum_timestamp)
		shtabstats->autovac_vacuum_timestamp = lstats->autovac_vacuum_timestamp;
	shtabstats->autovac_vacuum_count += lstats->t_counts.autovac_vacuum_count;

	if (shtabstats->analyze_timestamp < lstats->analyze_timestamp)
		shtabstats->analyze_timestamp = lstats->analyze_timestamp;
	shtabstats->analyze_count += lstats->t_counts.analyze_count;

	if (shtabstats->autovac_analyze_timestamp < lstats->autovac_analyze_timestamp)
		shtabstats->autovac_analyze_timestamp = lstats->autovac_analyze_timestamp;
	shtabstats->autovac_analyze_count += lstats->t_counts.autovac_analyze_count;

	/* Clamp n_live_tuples in case of negative delta_live_tuples */
	shtabstats->n_live_tuples = Max(shtabstats->n_live_tuples, 0);
	/* Likewise for n_dead_tuples */
	shtabstats->n_dead_tuples = Max(shtabstats->n_dead_tuples, 0);

	LWLockRelease(&shenv->lock);

	/* The entry is successfully flushed so the same to add to database stats */
	ldbstats = get_local_dbstat_entry(dboid);
	ldbstats->counts.n_tuples_returned += lstats->t_counts.t_tuples_returned;
	ldbstats->counts.n_tuples_fetched += lstats->t_counts.t_tuples_fetched;
	ldbstats->counts.n_tuples_inserted += lstats->t_counts.t_tuples_inserted;
	ldbstats->counts.n_tuples_updated += lstats->t_counts.t_tuples_updated;
	ldbstats->counts.n_tuples_deleted += lstats->t_counts.t_tuples_deleted;
	ldbstats->counts.n_blocks_fetched += lstats->t_counts.t_blocks_fetched;
	ldbstats->counts.n_blocks_hit += lstats->t_counts.t_blocks_hit;

	return true;
}

/* ----------
 * init_tabentry() -
 *
 * initializes table stats entry
 * This is also used as initialization callback for get_stat_entry.
 * ----------
 */
static void
init_tabentry(PgStatEnvelope * env)
{
	PgStat_StatTabEntry *tabent = (PgStat_StatTabEntry *) &env->body;

	/*
	 * If it's a new table entry, initialize counters to the values we just
	 * got.
	 */
	Assert(env->type == PGSTAT_TYPE_TABLE);
	tabent->tableid = env->objectid;
	tabent->numscans = 0;
	tabent->tuples_returned = 0;
	tabent->tuples_fetched = 0;
	tabent->tuples_inserted = 0;
	tabent->tuples_updated = 0;
	tabent->tuples_deleted = 0;
	tabent->tuples_hot_updated = 0;
	tabent->n_live_tuples = 0;
	tabent->n_dead_tuples = 0;
	tabent->changes_since_analyze = 0;
	tabent->blocks_fetched = 0;
	tabent->blocks_hit = 0;

	tabent->vacuum_timestamp = 0;
	tabent->vacuum_count = 0;
	tabent->autovac_vacuum_timestamp = 0;
	tabent->autovac_vacuum_count = 0;
	tabent->analyze_timestamp = 0;
	tabent->analyze_count = 0;
	tabent->autovac_analyze_timestamp = 0;
	tabent->autovac_analyze_count = 0;
}


/*
 * flush_funcstat - flush out a local function stats entry
 *
 * If nowait is true, this function returns false on lock failure. Otherwise
 * this function always returns true.
 *
 * Returns true if the entry is successfully flushed out.
 */
static bool
flush_funcstat(PgStatEnvelope * env, bool nowait)
{
	/* we assume this inits to all zeroes: */
	static const PgStat_FunctionCounts all_zeroes;
	PgStat_BackendFunctionEntry *localent;	/* local stats entry */
	PgStatEnvelope *shenv;		/* shared stats envelope */
	PgStat_StatFuncEntry *sharedent = NULL; /* shared stats entry */
	bool		found;

	Assert(env->type == PGSTAT_TYPE_FUNCTION);
	localent = (PgStat_BackendFunctionEntry *) &env->body;

	/* Skip it if no counts accumulated for it so far */
	if (memcmp(&localent->f_counts, &all_zeroes,
			   sizeof(PgStat_FunctionCounts)) == 0)
		return true;

	/* find shared table stats entry corresponding to the local entry */
	shenv = get_stat_entry(PGSTAT_TYPE_FUNCTION, MyDatabaseId, localent->f_id,
						   nowait, init_funcentry, &found);
	/* skip if dshash failed to acquire lock */
	if (sharedent == NULL)
		return false;			/* failed to acquire lock, skip */

	/* retrieve the shared table stats entry from the envelope */
	sharedent = (PgStat_StatFuncEntry *) &shenv->body;

	/* lock the shared entry to protect the content, skip if failed */
	if (!nowait)
		LWLockAcquire(&shenv->lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(&shenv->lock, LW_EXCLUSIVE))
		return false;			/* failed to acquire lock, skip */

	sharedent->f_numcalls += localent->f_counts.f_numcalls;
	sharedent->f_total_time +=
		INSTR_TIME_GET_MICROSEC(localent->f_counts.f_total_time);
	sharedent->f_self_time +=
		INSTR_TIME_GET_MICROSEC(localent->f_counts.f_self_time);

	LWLockRelease(&shenv->lock);

	return true;
}


/* ----------
 * init_funcentry() -
 *
 * initializes function stats entry
 * This is also used as initialization callback for get_stat_entry.
 * ----------
 */
static void
init_funcentry(PgStatEnvelope * env)
{
	PgStat_StatFuncEntry *shstat = (PgStat_StatFuncEntry *) &env->body;

	Assert(env->type == PGSTAT_TYPE_FUNCTION);
	shstat->functionid = env->objectid;
	shstat->f_numcalls = 0;
	shstat->f_total_time = 0;
	shstat->f_self_time = 0;
}


/*
 * flush_dbstat - flush out a local database stats entry
 *
 * If nowait is true, this function returns false on lock failure. Otherwise
 * this function always returns true.
 *
 * Returns true if the entry is successfully flushed out.
 */
static bool
flush_dbstat(PgStatEnvelope * env, bool nowait)
{
	PgStat_StatDBEntry *localent;
	PgStatEnvelope *shenv;
	PgStat_StatDBEntry *sharedent;

	Assert(env->type == PGSTAT_TYPE_DB);

	localent = (PgStat_StatDBEntry *) &env->body;

	/* find shared database stats entry corresponding to the local entry */
	shenv = get_stat_entry(PGSTAT_TYPE_DB, localent->databaseid, InvalidOid,
						   nowait, init_dbentry, NULL);

	/* skip if dshash failed to acquire lock */
	if (!shenv)
		return false;

	/* retrieve the shared stats entry from the envelope */
	sharedent = (PgStat_StatDBEntry *) &shenv->body;

	/* lock the shared entry to protect the content, skip if failed */
	if (!nowait)
		LWLockAcquire(&shenv->lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(&shenv->lock, LW_EXCLUSIVE))
		return false;

	sharedent->counts.n_tuples_returned += localent->counts.n_tuples_returned;
	sharedent->counts.n_tuples_fetched += localent->counts.n_tuples_fetched;
	sharedent->counts.n_tuples_inserted += localent->counts.n_tuples_inserted;
	sharedent->counts.n_tuples_updated += localent->counts.n_tuples_updated;
	sharedent->counts.n_tuples_deleted += localent->counts.n_tuples_deleted;
	sharedent->counts.n_blocks_fetched += localent->counts.n_blocks_fetched;
	sharedent->counts.n_blocks_hit += localent->counts.n_blocks_hit;

	sharedent->counts.n_deadlocks += localent->counts.n_deadlocks;
	sharedent->counts.n_temp_bytes += localent->counts.n_temp_bytes;
	sharedent->counts.n_temp_files += localent->counts.n_temp_files;
	sharedent->counts.n_checksum_failures += localent->counts.n_checksum_failures;

	/*
	 * Accumulate xact commit/rollback and I/O timings to stats entry of the
	 * current database.
	 */
	if (OidIsValid(localent->databaseid))
	{
		sharedent->counts.n_xact_commit += pgStatXactCommit;
		sharedent->counts.n_xact_rollback += pgStatXactRollback;
		sharedent->counts.n_block_read_time += pgStatBlockReadTime;
		sharedent->counts.n_block_write_time += pgStatBlockWriteTime;
		pgStatXactCommit = 0;
		pgStatXactRollback = 0;
		pgStatBlockReadTime = 0;
		pgStatBlockWriteTime = 0;
	}
	else
	{
		sharedent->counts.n_xact_commit = 0;
		sharedent->counts.n_xact_rollback = 0;
		sharedent->counts.n_block_read_time = 0;
		sharedent->counts.n_block_write_time = 0;
	}

	LWLockRelease(&shenv->lock);

	return true;
}


/* ----------
 * init_dbentry() -
 *
 * initializes database stats entry
 * This is also used as initialization callback for get_stat_entry.
 * ----------
 */
static void
init_dbentry(PgStatEnvelope * env)
{
	PgStat_StatDBEntry *dbentry = (PgStat_StatDBEntry *) &env->body;

	Assert(env->type == PGSTAT_TYPE_DB);
	dbentry->databaseid = env->databaseid;
	dbentry->last_autovac_time = 0;
	dbentry->last_checksum_failure = 0;
	dbentry->stat_reset_timestamp = 0;
	dbentry->stats_timestamp = 0;
	/* initialize the new shared entry */
	MemSet(&dbentry->counts, 0, sizeof(PgStat_StatDBCounts));
}


/*
 * Create the filename for a DB stat file; filename is output parameter points
 * to a character buffer of length len.
 */
static void
get_dbstat_filename(bool tempname, Oid databaseid, char *filename, int len)
{
	int			printed;

	/* NB -- pgstat_reset_remove_files knows about the pattern this uses */
	printed = snprintf(filename, len, "%s/db_%u.%s",
					   PGSTAT_STAT_PERMANENT_DIRECTORY,
					   databaseid,
					   tempname ? "tmp" : "stat");
	if (printed >= len)
		elog(ERROR, "overlength pgstat path");
}


/* ----------
 * collect_stat_entries() -
 *
 *	Collect the shared statistics entries specified by type and dbid. Returns a
 *  list of pointer to shared statistics in palloc'ed memory. If type is
 *  PGSTAT_TYPE_ALL, all types of statistics of the database is collected. If
 *  type is PGSTAT_TYPE_DB, the parameter dbid is ignored and collect all
 *  PGSTAT_TYPE_DB entries.
 * ----------
 */
static PgStatEnvelope * *collect_stat_entries(PgStatTypes type, Oid dbid)
{
	dshash_seq_status hstat;
	PgStatHashEntry *p;
	int			listlen = 16;
	PgStatEnvelope **envlist = palloc(sizeof(PgStatEnvelope * *) * listlen);
	int			n = 0;

	dshash_seq_init(&hstat, pgStatSharedHash, false);
	while ((p = dshash_seq_next(&hstat)) != NULL)
	{
		if ((type != PGSTAT_TYPE_ALL && p->key.type != type) ||
			(type != PGSTAT_TYPE_DB && p->key.databaseid != dbid))
			continue;

		if (n >= listlen - 1)
		{
			listlen *= 2;
			envlist = repalloc(envlist, listlen * sizeof(PgStatEnvelope * *));
		}
		envlist[n++] = dsa_get_address(area, p->env);
	}
	dshash_seq_term(&hstat);

	envlist[n] = NULL;

	return envlist;
}


/* ----------
 * collect_oids() -
 *
 *	Collect the OIDs of all objects listed in the specified system catalog
 *	into a temporary hash table.  Caller should hash_destroy the result
 *	when done with it.  (However, we make the table in CurrentMemoryContext
 *	so that it will be freed properly in event of an error.)
 * ----------
 */
static HTAB *
collect_oids(Oid catalogid, AttrNumber anum_oid)
{
	HTAB	   *htab;
	HASHCTL		hash_ctl;
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tup;
	Snapshot	snapshot;

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(Oid);
	hash_ctl.hcxt = CurrentMemoryContext;
	htab = hash_create("Temporary table of OIDs",
					   PGSTAT_TABLE_HASH_SIZE,
					   &hash_ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	rel = table_open(catalogid, AccessShareLock);
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scan = table_beginscan(rel, snapshot, 0, NULL);
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Oid			thisoid;
		bool		isnull;

		thisoid = heap_getattr(tup, anum_oid, RelationGetDescr(rel), &isnull);
		Assert(!isnull);

		CHECK_FOR_INTERRUPTS();

		(void) hash_search(htab, (void *) &thisoid, HASH_ENTER, NULL);
	}
	table_endscan(scan);
	UnregisterSnapshot(snapshot);
	table_close(rel, AccessShareLock);

	return htab;
}


/* ----------
 * pgstat_vacuum_stat() -
 *
 *  Delete shared stat entries that are not in system catalogs.
 *
 *  To avoid holding exclusive lock on dshash for a long time, the process is
 *  performed in three steps.
 *
 *   1: Collect existent oids of every kind of object.
 *   2: Collect victim entries by scanning with shared lock.
 *   3: Try removing every nominated entry without waiting for lock.
 *
 *  As the consequence of the last step, some entries may be left alone due to
 *  lock failure, but as explained by the comment of pgstat_vacuum_stat, they
 *  will be deleted by later vacuums.
 * ----------
 */
void
pgstat_vacuum_stat(void)
{
	HTAB	   *dbids;			/* database ids */
	HTAB	   *relids;			/* relation ids in the current database */
	HTAB	   *funcids;		/* function ids in the current database */
	PgStatEnvelope **victims;	/* victim entry list */
	int			arraylen = 0;	/* storage size of the above */
	int			nvictims = 0;	/* # of entries of the above */
	dshash_seq_status dshstat;
	PgStatHashEntry *ent;
	int			i;

	/* we don't collect stats under standalone mode */
	if (!IsUnderPostmaster)
		return;

	/* collect oids of existent objects */
	dbids = collect_oids(DatabaseRelationId, Anum_pg_database_oid);
	relids = collect_oids(RelationRelationId, Anum_pg_class_oid);
	funcids = collect_oids(ProcedureRelationId, Anum_pg_proc_oid);

	/* collect victims from shared stats */
	arraylen = 16;
	victims = palloc(sizeof(PgStatEnvelope * *) * arraylen);
	nvictims = 0;

	dshash_seq_init(&dshstat, pgStatSharedHash, false);

	while ((ent = dshash_seq_next(&dshstat)) != NULL)
	{
		HTAB	   *oidtab;
		Oid		   *key;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Don't drop entries for other than database objects not of the
		 * current database.
		 */
		if (ent->key.type != PGSTAT_TYPE_DB &&
			ent->key.databaseid != MyDatabaseId)
			continue;

		switch (ent->key.type)
		{
			case PGSTAT_TYPE_DB:
				/* don't remove database entry for shared tables */
				if (ent->key.databaseid == 0)
					continue;
				oidtab = dbids;
				key = &ent->key.databaseid;
				break;

			case PGSTAT_TYPE_TABLE:
				oidtab = relids;
				key = &ent->key.objectid;
				break;

			case PGSTAT_TYPE_FUNCTION:
				oidtab = funcids;
				key = &ent->key.objectid;
				break;
			case PGSTAT_TYPE_ALL:
				Assert(false);
				break;
		}

		/* Skip existent objects. */
		if (hash_search(oidtab, key, HASH_FIND, NULL) != NULL)
			continue;

		/* extend the list if needed */
		if (nvictims >= arraylen)
		{
			arraylen *= 2;
			victims = repalloc(victims, sizeof(PgStatEnvelope * *) * arraylen);
		}

		victims[nvictims++] = dsa_get_address(area, ent->env);
	}
	dshash_seq_term(&dshstat);
	hash_destroy(dbids);
	hash_destroy(relids);
	hash_destroy(funcids);

	/* Now try removing the victim entries */
	for (i = 0; i < nvictims; i++)
	{
		PgStatEnvelope *p = victims[i];

		delete_stat_entry(p->type, p->databaseid, p->objectid, true);
	}
}


/* ----------
 * delete_stat_entry() -
 *
 *  Deletes the specified entry from shared stats hash.
 *
 *  Returns true when successfully deleted.
 * ----------
 */
static bool
delete_stat_entry(PgStatTypes type, Oid dbid, Oid objid, bool nowait)
{
	PgStatHashEntryKey key;
	PgStatHashEntry *ent;

	key.type = type;
	key.databaseid = dbid;
	key.objectid = objid;
	ent = dshash_find_extended(pgStatSharedHash, &key,
							   true, nowait, false, NULL);

	if (!ent)
		return false;			/* lock failed or not found */

	/* The entry is exclusively locked, so we can free the chunk first. */
	dsa_free(area, ent->env);
	dshash_delete_entry(pgStatSharedHash, ent);

	return true;
}


/* ----------
 * pgstat_drop_database() -
 *
 *	Remove entry for the database that we just dropped.
 *
 *  Some entries might be left alone due to lock failure or some stats are
 *	flushed after this but we will still clean the dead DB eventually via
 *	future invocations of pgstat_vacuum_stat().
 *	----------
 */
void
pgstat_drop_database(Oid databaseid)
{
	PgStatEnvelope **envlist;
	PgStatEnvelope **p;

	Assert(OidIsValid(databaseid));

	if (!IsUnderPostmaster || !pgStatSharedHash)
		return;

	envlist = collect_stat_entries(PGSTAT_TYPE_ALL, MyDatabaseId);

	for (p = envlist; *p != NULL; p++)
		delete_stat_entry((*p)->type, (*p)->databaseid, (*p)->objectid, true);

	pfree(envlist);
}


/* ----------
 * pgstat_reset_counters() -
 *
 *	Reset counters for our database.
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_counters(void)
{
	PgStatEnvelope **envlist;
	PgStatEnvelope **p;

	/* Lookup the entries of the current database in the stats hash. */
	envlist = collect_stat_entries(PGSTAT_TYPE_ALL, MyDatabaseId);
	for (p = envlist; *p != NULL; p++)
	{
		PgStatEnvelope *env = *p;
		PgStat_StatDBEntry *dbstat;

		LWLockAcquire(&env->lock, LW_EXCLUSIVE);

		switch (env->type)
		{
			case PGSTAT_TYPE_TABLE:
				init_tabentry(env);
				break;

			case PGSTAT_TYPE_FUNCTION:
				init_funcentry(env);
				break;

			case PGSTAT_TYPE_DB:
				init_dbentry(env);
				dbstat = (PgStat_StatDBEntry *) &env->body;
				dbstat->stat_reset_timestamp = GetCurrentTimestamp();
				break;
			default:
				Assert(false);
		}

		LWLockRelease(&env->lock);
	}

	pfree(envlist);
}

/* ----------
 * pgstat_reset_shared_counters() -
 *
 *	Reset cluster-wide shared counters.
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_shared_counters(const char *target)
{
	/* Reset the archiver statistics for the cluster. */
	if (strcmp(target, "archiver") == 0)
	{
		TimestampTz now = GetCurrentTimestamp();

		LWLockAcquire(StatsLock, LW_EXCLUSIVE);
		MemSet(shared_archiverStats, 0, sizeof(*shared_archiverStats));
		shared_archiverStats->stat_reset_timestamp = now;
		LWLockRelease(StatsLock);
	}
	/* Reset the bgwriter statistics for the cluster. */
	else if (strcmp(target, "bgwriter") == 0)
	{
		TimestampTz now = GetCurrentTimestamp();

		LWLockAcquire(StatsLock, LW_EXCLUSIVE);
		MemSet(shared_globalStats, 0, sizeof(*shared_globalStats));
		shared_globalStats->stat_reset_timestamp = now;
		LWLockRelease(StatsLock);
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unrecognized reset target: \"%s\"", target),
				 errhint("Target must be \"archiver\" or \"bgwriter\".")));
}

/* ----------
 * pgstat_reset_single_counter() -
 *
 *	Reset a single counter.
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_single_counter(Oid objoid, PgStat_Single_Reset_Type type)
{
	PgStatEnvelope *env;
	PgStat_StatDBEntry *dbentry;
	PgStatTypes stattype;
	TimestampTz ts;

	env = get_stat_entry(PGSTAT_TYPE_DB, MyDatabaseId, InvalidOid,
						 false, NULL, NULL);
	Assert(env);

	/* Set the reset timestamp for the whole database */
	dbentry = (PgStat_StatDBEntry *) &env->body;
	ts = GetCurrentTimestamp();
	LWLockAcquire(&env->lock, LW_EXCLUSIVE);
	dbentry->stat_reset_timestamp = ts;
	LWLockRelease(&env->lock);

	/* Remove object if it exists, ignore if not */
	switch (type)
	{
		case RESET_TABLE:
			stattype = PGSTAT_TYPE_TABLE;
			break;
		case RESET_FUNCTION:
			stattype = PGSTAT_TYPE_FUNCTION;
	}

	env = get_stat_entry(stattype, MyDatabaseId, objoid, false, NULL, NULL);
	LWLockAcquire(&env->lock, LW_EXCLUSIVE);
	if (env->type == PGSTAT_TYPE_TABLE)
		init_tabentry(env);
	else
	{
		Assert(env->type == PGSTAT_TYPE_FUNCTION);
		init_funcentry(env);
	}
	LWLockRelease(&env->lock);
}

/* ----------
 * pgstat_report_autovac() -
 *
 *	Called from autovacuum.c to report startup of an autovacuum process.
 *	We are called before InitPostgres is done, so can't rely on MyDatabaseId;
 *	the db OID must be passed in, instead.
 * ----------
 */
void
pgstat_report_autovac(Oid dboid)
{
	PgStat_StatDBEntry *dbentry;
	TimestampTz ts;

	/* return if activity stats is not active */
	if (!area)
		return;

	ts = GetCurrentTimestamp();

	/*
	 * Store the last autovacuum time in the database's hash table entry.
	 */
	dbentry = get_local_dbstat_entry(dboid);
	dbentry->last_autovac_time = ts;
}


/* ---------
 * pgstat_report_vacuum() -
 *
 *	Report about the table we just vacuumed.
 * ---------
 */
void
pgstat_report_vacuum(Oid tableoid, bool shared,
					 PgStat_Counter livetuples, PgStat_Counter deadtuples)
{
	PgStat_TableStatus *tabentry;
	TimestampTz ts;

	/* return if we are not collecting stats */
	if (!area)
		return;

	/* Store the data in the table's hash table entry. */
	ts = GetCurrentTimestamp();
	tabentry = get_local_tabstat_entry(tableoid, shared);

	tabentry->t_counts.t_delta_live_tuples = livetuples;
	tabentry->t_counts.t_delta_dead_tuples = deadtuples;

	if (IsAutoVacuumWorkerProcess())
	{
		tabentry->autovac_vacuum_timestamp = ts;
		tabentry->t_counts.autovac_vacuum_count++;
	}
	else
	{
		tabentry->vacuum_timestamp = ts;
		tabentry->t_counts.vacuum_count++;
	}
}

/* --------
 * pgstat_report_analyze() -
 *
 *	Report about the table we just analyzed.
 *
 * Caller must provide new live- and dead-tuples estimates, as well as a
 * flag indicating whether to reset the changes_since_analyze counter.
 * --------
 */
void
pgstat_report_analyze(Relation rel,
					  PgStat_Counter livetuples, PgStat_Counter deadtuples,
					  bool resetcounter)
{
	PgStat_TableStatus *tabentry;

	/* return if we are not collecting stats */
	if (!area)
		return;

	/*
	 * Unlike VACUUM, ANALYZE might be running inside a transaction that has
	 * already inserted and/or deleted rows in the target table. ANALYZE will
	 * have counted such rows as live or dead respectively. Because we will
	 * report our counts of such rows at transaction end, we should subtract
	 * off these counts from what is already written to shared stats now, else
	 * they'll be double-counted after commit.  (This approach also ensures
	 * that the shared stats ends up with the right numbers if we abort
	 * instead of committing.)
	 */
	if (rel->pgstat_info != NULL)
	{
		PgStat_TableXactStatus *trans;

		for (trans = rel->pgstat_info->trans; trans; trans = trans->upper)
		{
			livetuples -= trans->tuples_inserted - trans->tuples_deleted;
			deadtuples -= trans->tuples_updated + trans->tuples_deleted;
		}
		/* count stuff inserted by already-aborted subxacts, too */
		deadtuples -= rel->pgstat_info->t_counts.t_delta_dead_tuples;
		/* Since ANALYZE's counts are estimates, we could have underflowed */
		livetuples = Max(livetuples, 0);
		deadtuples = Max(deadtuples, 0);
	}

	/* Store the data in the table's hash table entry. */
	tabentry = get_local_tabstat_entry(RelationGetRelid(rel),
									   rel->rd_rel->relisshared);

	tabentry->t_counts.t_delta_live_tuples = livetuples;
	tabentry->t_counts.t_delta_dead_tuples = deadtuples;

	/*
	 * If commanded, reset changes_since_analyze to zero.  This forgets any
	 * changes that were committed while the ANALYZE was in progress, but we
	 * have no good way to estimate how many of those there were.
	 */
	if (resetcounter)
		tabentry->t_counts.reset_changed_tuples = true;

	if (IsAutoVacuumWorkerProcess())
	{
		tabentry->autovac_analyze_timestamp = GetCurrentTimestamp();
		tabentry->t_counts.autovac_analyze_count++;
	}
	else
	{
		tabentry->analyze_timestamp = GetCurrentTimestamp();
		tabentry->t_counts.analyze_count++;
	}
}

/* --------
 * pgstat_report_recovery_conflict() -
 *
 *	Report a Hot Standby recovery conflict.
 * --------
 */
void
pgstat_report_recovery_conflict(int reason)
{
	PgStat_StatDBEntry *dbent;

	/* return if we are not collecting stats */
	if (!area)
		return;

	dbent = get_local_dbstat_entry(MyDatabaseId);

	switch (reason)
	{
		case PROCSIG_RECOVERY_CONFLICT_DATABASE:

			/*
			 * Since we drop the information about the database as soon as it
			 * replicates, there is no point in counting these conflicts.
			 */
			break;
		case PROCSIG_RECOVERY_CONFLICT_TABLESPACE:
			dbent->counts.n_conflict_tablespace++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_LOCK:
			dbent->counts.n_conflict_lock++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_SNAPSHOT:
			dbent->counts.n_conflict_snapshot++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_BUFFERPIN:
			dbent->counts.n_conflict_bufferpin++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK:
			dbent->counts.n_conflict_startup_deadlock++;
			break;
	}
}

/* --------
 * pgstat_report_deadlock() -
 *
 *	Report a deadlock detected.
 * --------
 */
void
pgstat_report_deadlock(void)
{
	PgStat_StatDBEntry *dbent;

	/* return if we are not collecting stats */
	if (!area)
		return;

	dbent = get_local_dbstat_entry(MyDatabaseId);
	dbent->counts.n_deadlocks++;
}

/* --------
 * pgstat_report_checksum_failure() -
 *
 *	Reports about a checksum failure.
 * --------
 */
void
pgstat_report_checksum_failure(void)
{
	PgStat_StatDBEntry *dbent;

	/* return if we are not collecting stats */
	if (!area)
		return;

	dbent = get_local_dbstat_entry(MyDatabaseId);
	dbent->counts.n_checksum_failures++;
}

/* --------
 * pgstat_report_tempfile() -
 *
 *	Report a temporary file.
 * --------
 */
void
pgstat_report_tempfile(size_t filesize)
{
	PgStat_StatDBEntry *dbent;

	/* return if we are not collecting stats */
	if (!area)
		return;

	if (filesize == 0)			/* Is there a case where filesize is really 0? */
		return;

	dbent = get_local_dbstat_entry(MyDatabaseId);
	dbent->counts.n_temp_bytes += filesize; /* needs check overflow */
	dbent->counts.n_temp_files++;
}


/* --------
 * pgstat_report_checksum_failures_in_db(dboid, failure_count) -
 *
 *	Reports about one or more checksum failures.
 * --------
 */
void
pgstat_report_checksum_failures_in_db(Oid dboid, int failurecount)
{
	PgStat_StatDBEntry *dbentry;

	/* return if we are not active */
	if (!area)
		return;

	dbentry = get_local_dbstat_entry(dboid);

	/* add accumulated count to the parameter */
	dbentry->counts.n_checksum_failures += failurecount;
}

/* ----------
 * pgstat_init_function_usage() -
 *
 *  Initialize function call usage data.
 *  Called by the executor before invoking a function.
 * ----------
 */
void
pgstat_init_function_usage(FunctionCallInfo fcinfo,
						   PgStat_FunctionCallUsage *fcu)
{
	PgStatEnvelope *env;
	PgStat_BackendFunctionEntry *htabent;
	bool		found;

	if (pgstat_track_functions <= fcinfo->flinfo->fn_stats)
	{
		/* stats not wanted */
		fcu->fs = NULL;
		return;
	}

	env = get_local_stat_entry(PGSTAT_TYPE_FUNCTION, MyDatabaseId,
							   fcinfo->flinfo->fn_oid, true, &found);
	htabent = (PgStat_BackendFunctionEntry *) &env->body;

	if (!found)
		MemSet(&htabent->f_counts, 0, sizeof(PgStat_FunctionCounts));

	htabent->f_id = fcinfo->flinfo->fn_oid;

	fcu->fs = &htabent->f_counts;

	/* save stats for this function, later used to compensate for recursion */
	fcu->save_f_total_time = htabent->f_counts.f_total_time;

	/* save current backend-wide total time */
	fcu->save_total = total_func_time;

	/* get clock time as of function start */
	INSTR_TIME_SET_CURRENT(fcu->f_start);
}

/* ----------
 * find_funcstat_entry() -
 *
 *  find any existing PgStat_BackendFunctionEntry entry for specified function
 *
 *  If no entry, return NULL, not creating a new one.
 * ----------
 */
PgStat_BackendFunctionEntry *
find_funcstat_entry(Oid func_id)
{
	PgStatEnvelope *env;

	env = get_local_stat_entry(PGSTAT_TYPE_FUNCTION, MyDatabaseId,
							   func_id, false, NULL);
	if (!env)
		return NULL;

	return (PgStat_BackendFunctionEntry *) &env->body;
}

/* ----------
 * pgstat_end_function_usage() -
 *
 *  Calculate function call usage and update stat counters.
 *  Called by the executor after invoking a function.
 *
 *  In the case of a set-returning function that runs in value-per-call mode,
 *  we will see multiple pgstat_init_function_usage/pgstat_end_function_usage
 *  calls for what the user considers a single call of the function.  The
 *  finalize flag should be TRUE on the last call.
 * ----------
 */
void
pgstat_end_function_usage(PgStat_FunctionCallUsage *fcu, bool finalize)
{
	PgStat_FunctionCounts *fs = fcu->fs;
	instr_time	f_total;
	instr_time	f_others;
	instr_time	f_self;

	/* stats not wanted? */
	if (fs == NULL)
		return;

	/* total elapsed time in this function call */
	INSTR_TIME_SET_CURRENT(f_total);
	INSTR_TIME_SUBTRACT(f_total, fcu->f_start);

	/* self usage: elapsed minus anything already charged to other calls */
	f_others = total_func_time;
	INSTR_TIME_SUBTRACT(f_others, fcu->save_total);
	f_self = f_total;
	INSTR_TIME_SUBTRACT(f_self, f_others);

	/* update backend-wide total time */
	INSTR_TIME_ADD(total_func_time, f_self);

	/*
	 * Compute the new f_total_time as the total elapsed time added to the
	 * pre-call value of f_total_time.  This is necessary to avoid
	 * double-counting any time taken by recursive calls of myself.  (We do
	 * not need any similar kluge for self time, since that already excludes
	 * any recursive calls.)
	 */
	INSTR_TIME_ADD(f_total, fcu->save_f_total_time);

	/* update counters in function stats table */
	if (finalize)
		fs->f_numcalls++;
	fs->f_total_time = f_total;
	INSTR_TIME_ADD(fs->f_self_time, f_self);
}


/* ----------
 * pgstat_initstats() -
 *
 *	Initialize a relcache entry to count access statistics.
 *	Called whenever a relation is opened.
 *
 *	We assume that a relcache entry's pgstat_info field is zeroed by
 *	relcache.c when the relcache entry is made; thereafter it is long-lived
 *	data.
 * ----------
 */
void
pgstat_initstats(Relation rel)
{
	Oid			rel_id = rel->rd_id;
	char		relkind = rel->rd_rel->relkind;

	/* We only count stats for things that have storage */
	if (!(relkind == RELKIND_RELATION ||
		  relkind == RELKIND_MATVIEW ||
		  relkind == RELKIND_INDEX ||
		  relkind == RELKIND_TOASTVALUE ||
		  relkind == RELKIND_SEQUENCE))
	{
		rel->pgstat_info = NULL;
		return;
	}

	/* return if we are not collecting stats */
	if (!area)
	{
		/* We're not counting at all */
		rel->pgstat_info = NULL;
		return;
	}

	/*
	 * If we already set up this relation in the current transaction, nothing
	 * to do.
	 */
	if (rel->pgstat_info != NULL &&
		rel->pgstat_info->t_id == rel_id)
		return;

	/* Else find or make the PgStat_TableStatus entry, and update link */
	rel->pgstat_info = get_local_tabstat_entry(rel_id, rel->rd_rel->relisshared);
}


/* ----------
 * get_local_stat_entry() -
 *
 *  Returns local stats entry for the type, dbid and objid.
 *  If create is true, new entry is created if not yet.  found must be non-null
 *  in the case.
 *
 *
 *  The caller is responsible to initialize body part of the returned envelope.
 * ----------
 */
static PgStatEnvelope *
get_local_stat_entry(PgStatTypes type, Oid dbid, Oid objid,
					 bool create, bool *found)
{
	PgStatHashEntryKey key;
	PgStatLocalHashEntry *entry;

	if (pgStatLocalHash == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(PgStatHashEntryKey);
		ctl.entrysize = sizeof(PgStatLocalHashEntry);

		pgStatLocalHash = hash_create("Local stat entries",
									  PGSTAT_TABLE_HASH_SIZE,
									  &ctl,
									  HASH_ELEM | HASH_BLOBS);
	}

	/* Find an entry or create a new one. */
	key.type = type;
	key.databaseid = dbid;
	key.objectid = objid;
	entry = hash_search(pgStatLocalHash, &key,
						create ? HASH_ENTER : HASH_FIND, found);

	if (!create && !entry)
		return NULL;

	if (create && !*found)
	{
		int			len = pgstat_localentsize[type];

		entry->env = MemoryContextAlloc(CacheMemoryContext,
										PgStatEnvelopeSize(len));
		entry->env->type = type;
		entry->env->len = len;
	}

	return entry->env;
}

/* ----------
 * get_local_dbstat_entry() -
 *
 *  Find or create a local PgStat_StatDBEntry entry for dbid.  New entry is
 *  created and initialized if not exists.
 */
static PgStat_StatDBEntry *
get_local_dbstat_entry(Oid dbid)
{
	PgStatEnvelope *env;
	PgStat_StatDBEntry *dbentry;
	bool		found;

	/*
	 * Find an entry or create a new one.
	 */
	env = get_local_stat_entry(PGSTAT_TYPE_DB, dbid, InvalidOid,
							   true, &found);
	dbentry = (PgStat_StatDBEntry *) &env->body;

	if (!found)
	{
		dbentry->databaseid = dbid;
		dbentry->last_autovac_time = 0;
		dbentry->last_checksum_failure = 0;
		dbentry->stat_reset_timestamp = 0;
		dbentry->stats_timestamp = 0;
		MemSet(&dbentry->counts, 0, sizeof(PgStat_StatDBCounts));
	}

	return dbentry;
}


/* ----------
 * get_local_tabstat_entry() -
 *  Find or create a PgStat_TableStatus entry for rel. New entry is created and
 *  initialized if not exists.
 * ----------
 */
static PgStat_TableStatus *
get_local_tabstat_entry(Oid rel_id, bool isshared)
{
	PgStatEnvelope *env;
	PgStat_TableStatus *tabentry;
	bool		found;

	env = get_local_stat_entry(PGSTAT_TYPE_TABLE,
							   isshared ? InvalidOid : MyDatabaseId,
							   rel_id, true, &found);

	tabentry = (PgStat_TableStatus *) &env->body;

	if (!found)
	{
		tabentry->t_id = rel_id;
		tabentry->t_shared = isshared;
		tabentry->trans = NULL;
		MemSet(&tabentry->t_counts, 0, sizeof(PgStat_TableCounts));
		tabentry->vacuum_timestamp = 0;
		tabentry->autovac_vacuum_timestamp = 0;
		tabentry->analyze_timestamp = 0;
		tabentry->autovac_analyze_timestamp = 0;
	}

	return tabentry;
}


/*
 * find_tabstat_entry - find any existing PgStat_TableStatus entry for rel
 *
 *  Find any existing PgStat_TableStatus entry for rel from the current
 *  database then from shared tables.
 *
 *  If no entry, return NULL, don't create a new one
 * ----------
 */
PgStat_TableStatus *
find_tabstat_entry(Oid rel_id)
{
	PgStatEnvelope *env;

	env = get_local_stat_entry(PGSTAT_TYPE_TABLE, MyDatabaseId, rel_id,
							   false, NULL);
	if (!env)
		env = get_local_stat_entry(PGSTAT_TYPE_TABLE, InvalidOid, rel_id,
								   false, NULL);
	if (env)
		return (PgStat_TableStatus *) &env->body;

	return NULL;
}

/*
 * get_tabstat_stack_level - add a new (sub)transaction stack entry if needed
 */
static PgStat_SubXactStatus *
get_tabstat_stack_level(int nest_level)
{
	PgStat_SubXactStatus *xact_state;

	xact_state = pgStatXactStack;
	if (xact_state == NULL || xact_state->nest_level != nest_level)
	{
		xact_state = (PgStat_SubXactStatus *)
			MemoryContextAlloc(TopTransactionContext,
							   sizeof(PgStat_SubXactStatus));
		xact_state->nest_level = nest_level;
		xact_state->prev = pgStatXactStack;
		xact_state->first = NULL;
		pgStatXactStack = xact_state;
	}
	return xact_state;
}

/*
 * add_tabstat_xact_level - add a new (sub)transaction state record
 */
static void
add_tabstat_xact_level(PgStat_TableStatus *pgstat_info, int nest_level)
{
	PgStat_SubXactStatus *xact_state;
	PgStat_TableXactStatus *trans;

	/*
	 * If this is the first rel to be modified at the current nest level, we
	 * first have to push a transaction stack entry.
	 */
	xact_state = get_tabstat_stack_level(nest_level);

	/* Now make a per-table stack entry */
	trans = (PgStat_TableXactStatus *)
		MemoryContextAllocZero(TopTransactionContext,
							   sizeof(PgStat_TableXactStatus));
	trans->nest_level = nest_level;
	trans->upper = pgstat_info->trans;
	trans->parent = pgstat_info;
	trans->next = xact_state->first;
	xact_state->first = trans;
	pgstat_info->trans = trans;
}

/*
 * pgstat_count_heap_insert - count a tuple insertion of n tuples
 */
void
pgstat_count_heap_insert(Relation rel, PgStat_Counter n)
{
	PgStat_TableStatus *pgstat_info = rel->pgstat_info;

	if (pgstat_info != NULL)
	{
		/* We have to log the effect at the proper transactional level */
		int			nest_level = GetCurrentTransactionNestLevel();

		if (pgstat_info->trans == NULL ||
			pgstat_info->trans->nest_level != nest_level)
			add_tabstat_xact_level(pgstat_info, nest_level);

		pgstat_info->trans->tuples_inserted += n;
	}
}

/*
 * pgstat_count_heap_update - count a tuple update
 */
void
pgstat_count_heap_update(Relation rel, bool hot)
{
	PgStat_TableStatus *pgstat_info = rel->pgstat_info;

	if (pgstat_info != NULL)
	{
		/* We have to log the effect at the proper transactional level */
		int			nest_level = GetCurrentTransactionNestLevel();

		if (pgstat_info->trans == NULL ||
			pgstat_info->trans->nest_level != nest_level)
			add_tabstat_xact_level(pgstat_info, nest_level);

		pgstat_info->trans->tuples_updated++;

		/* t_tuples_hot_updated is nontransactional, so just advance it */
		if (hot)
			pgstat_info->t_counts.t_tuples_hot_updated++;
	}
}

/*
 * pgstat_count_heap_delete - count a tuple deletion
 */
void
pgstat_count_heap_delete(Relation rel)
{
	PgStat_TableStatus *pgstat_info = rel->pgstat_info;

	if (pgstat_info != NULL)
	{
		/* We have to log the effect at the proper transactional level */
		int			nest_level = GetCurrentTransactionNestLevel();

		if (pgstat_info->trans == NULL ||
			pgstat_info->trans->nest_level != nest_level)
			add_tabstat_xact_level(pgstat_info, nest_level);

		pgstat_info->trans->tuples_deleted++;
	}
}

/*
 * pgstat_truncate_save_counters
 *
 * Whenever a table is truncated, we save its i/u/d counters so that they can
 * be cleared, and if the (sub)xact that executed the truncate later aborts,
 * the counters can be restored to the saved (pre-truncate) values.  Note we do
 * this on the first truncate in any particular subxact level only.
 */
static void
pgstat_truncate_save_counters(PgStat_TableXactStatus *trans)
{
	if (!trans->truncated)
	{
		trans->inserted_pre_trunc = trans->tuples_inserted;
		trans->updated_pre_trunc = trans->tuples_updated;
		trans->deleted_pre_trunc = trans->tuples_deleted;
		trans->truncated = true;
	}
}

/*
 * pgstat_truncate_restore_counters - restore counters when a truncate aborts
 */
static void
pgstat_truncate_restore_counters(PgStat_TableXactStatus *trans)
{
	if (trans->truncated)
	{
		trans->tuples_inserted = trans->inserted_pre_trunc;
		trans->tuples_updated = trans->updated_pre_trunc;
		trans->tuples_deleted = trans->deleted_pre_trunc;
	}
}

/*
 * pgstat_count_truncate - update tuple counters due to truncate
 */
void
pgstat_count_truncate(Relation rel)
{
	PgStat_TableStatus *pgstat_info = rel->pgstat_info;

	if (pgstat_info != NULL)
	{
		/* We have to log the effect at the proper transactional level */
		int			nest_level = GetCurrentTransactionNestLevel();

		if (pgstat_info->trans == NULL ||
			pgstat_info->trans->nest_level != nest_level)
			add_tabstat_xact_level(pgstat_info, nest_level);

		pgstat_truncate_save_counters(pgstat_info->trans);
		pgstat_info->trans->tuples_inserted = 0;
		pgstat_info->trans->tuples_updated = 0;
		pgstat_info->trans->tuples_deleted = 0;
	}
}

/*
 * pgstat_update_heap_dead_tuples - update dead-tuples count
 *
 * The semantics of this are that we are reporting the nontransactional
 * recovery of "delta" dead tuples; so t_delta_dead_tuples decreases
 * rather than increasing, and the change goes straight into the per-table
 * counter, not into transactional state.
 */
void
pgstat_update_heap_dead_tuples(Relation rel, int delta)
{
	PgStat_TableStatus *pgstat_info = rel->pgstat_info;

	if (pgstat_info != NULL)
		pgstat_info->t_counts.t_delta_dead_tuples -= delta;
}


/* ----------
 * AtEOXact_PgStat
 *
 *	Called from access/transam/xact.c at top-level transaction commit/abort.
 * ----------
 */
void
AtEOXact_PgStat(bool isCommit, bool parallel)
{
	PgStat_SubXactStatus *xact_state;

	/* Don't count parallel worker transaction stats */
	if (!parallel)
	{
		/*
		 * Count transaction commit or abort.  (We use counters, not just
		 * bools, in case the reporting message isn't sent right away.)
		 */
		if (isCommit)
			pgStatXactCommit++;
		else
			pgStatXactRollback++;
	}

	/*
	 * Transfer transactional insert/update counts into the base tabstat
	 * entries.  We don't bother to free any of the transactional state, since
	 * it's all in TopTransactionContext and will go away anyway.
	 */
	xact_state = pgStatXactStack;
	if (xact_state != NULL)
	{
		PgStat_TableXactStatus *trans;

		Assert(xact_state->nest_level == 1);
		Assert(xact_state->prev == NULL);
		for (trans = xact_state->first; trans != NULL; trans = trans->next)
		{
			PgStat_TableStatus *tabstat;

			Assert(trans->nest_level == 1);
			Assert(trans->upper == NULL);
			tabstat = trans->parent;
			Assert(tabstat->trans == trans);
			/* restore pre-truncate stats (if any) in case of aborted xact */
			if (!isCommit)
				pgstat_truncate_restore_counters(trans);
			/* count attempted actions regardless of commit/abort */
			tabstat->t_counts.t_tuples_inserted += trans->tuples_inserted;
			tabstat->t_counts.t_tuples_updated += trans->tuples_updated;
			tabstat->t_counts.t_tuples_deleted += trans->tuples_deleted;
			if (isCommit)
			{
				tabstat->t_counts.t_truncated = trans->truncated;
				if (trans->truncated)
				{
					/* forget live/dead stats seen by backend thus far */
					tabstat->t_counts.t_delta_live_tuples = 0;
					tabstat->t_counts.t_delta_dead_tuples = 0;
				}
				/* insert adds a live tuple, delete removes one */
				tabstat->t_counts.t_delta_live_tuples +=
					trans->tuples_inserted - trans->tuples_deleted;
				/* update and delete each create a dead tuple */
				tabstat->t_counts.t_delta_dead_tuples +=
					trans->tuples_updated + trans->tuples_deleted;
				/* insert, update, delete each count as one change event */
				tabstat->t_counts.t_changed_tuples +=
					trans->tuples_inserted + trans->tuples_updated +
					trans->tuples_deleted;
			}
			else
			{
				/* inserted tuples are dead, deleted tuples are unaffected */
				tabstat->t_counts.t_delta_dead_tuples +=
					trans->tuples_inserted + trans->tuples_updated;
				/* an aborted xact generates no changed_tuple events */
			}
			tabstat->trans = NULL;
		}
	}
	pgStatXactStack = NULL;

	/* Make sure any stats snapshot is thrown away */
	pgstat_clear_snapshot();
}

/* ----------
 * AtEOSubXact_PgStat
 *
 *	Called from access/transam/xact.c at subtransaction commit/abort.
 * ----------
 */
void
AtEOSubXact_PgStat(bool isCommit, int nestDepth)
{
	PgStat_SubXactStatus *xact_state;

	/*
	 * Transfer transactional insert/update counts into the next higher
	 * subtransaction state.
	 */
	xact_state = pgStatXactStack;
	if (xact_state != NULL &&
		xact_state->nest_level >= nestDepth)
	{
		PgStat_TableXactStatus *trans;
		PgStat_TableXactStatus *next_trans;

		/* delink xact_state from stack immediately to simplify reuse case */
		pgStatXactStack = xact_state->prev;

		for (trans = xact_state->first; trans != NULL; trans = next_trans)
		{
			PgStat_TableStatus *tabstat;

			next_trans = trans->next;
			Assert(trans->nest_level == nestDepth);
			tabstat = trans->parent;
			Assert(tabstat->trans == trans);
			if (isCommit)
			{
				if (trans->upper && trans->upper->nest_level == nestDepth - 1)
				{
					if (trans->truncated)
					{
						/* propagate the truncate status one level up */
						pgstat_truncate_save_counters(trans->upper);
						/* replace upper xact stats with ours */
						trans->upper->tuples_inserted = trans->tuples_inserted;
						trans->upper->tuples_updated = trans->tuples_updated;
						trans->upper->tuples_deleted = trans->tuples_deleted;
					}
					else
					{
						trans->upper->tuples_inserted += trans->tuples_inserted;
						trans->upper->tuples_updated += trans->tuples_updated;
						trans->upper->tuples_deleted += trans->tuples_deleted;
					}
					tabstat->trans = trans->upper;
					pfree(trans);
				}
				else
				{
					/*
					 * When there isn't an immediate parent state, we can just
					 * reuse the record instead of going through a
					 * palloc/pfree pushup (this works since it's all in
					 * TopTransactionContext anyway).  We have to re-link it
					 * into the parent level, though, and that might mean
					 * pushing a new entry into the pgStatXactStack.
					 */
					PgStat_SubXactStatus *upper_xact_state;

					upper_xact_state = get_tabstat_stack_level(nestDepth - 1);
					trans->next = upper_xact_state->first;
					upper_xact_state->first = trans;
					trans->nest_level = nestDepth - 1;
				}
			}
			else
			{
				/*
				 * On abort, update top-level tabstat counts, then forget the
				 * subtransaction
				 */

				/* first restore values obliterated by truncate */
				pgstat_truncate_restore_counters(trans);
				/* count attempted actions regardless of commit/abort */
				tabstat->t_counts.t_tuples_inserted += trans->tuples_inserted;
				tabstat->t_counts.t_tuples_updated += trans->tuples_updated;
				tabstat->t_counts.t_tuples_deleted += trans->tuples_deleted;
				/* inserted tuples are dead, deleted tuples are unaffected */
				tabstat->t_counts.t_delta_dead_tuples +=
					trans->tuples_inserted + trans->tuples_updated;
				tabstat->trans = trans->upper;
				pfree(trans);
			}
		}
		pfree(xact_state);
	}
}


/*
 * AtPrepare_PgStat
 *		Save the transactional stats state at 2PC transaction prepare.
 *
 * In this phase we just generate 2PC records for all the pending
 * transaction-dependent stats work.
 */
void
AtPrepare_PgStat(void)
{
	PgStat_SubXactStatus *xact_state;

	xact_state = pgStatXactStack;
	if (xact_state != NULL)
	{
		PgStat_TableXactStatus *trans;

		Assert(xact_state->nest_level == 1);
		Assert(xact_state->prev == NULL);
		for (trans = xact_state->first; trans != NULL; trans = trans->next)
		{
			PgStat_TableStatus *tabstat;
			TwoPhasePgStatRecord record;

			Assert(trans->nest_level == 1);
			Assert(trans->upper == NULL);
			tabstat = trans->parent;
			Assert(tabstat->trans == trans);

			record.tuples_inserted = trans->tuples_inserted;
			record.tuples_updated = trans->tuples_updated;
			record.tuples_deleted = trans->tuples_deleted;
			record.inserted_pre_trunc = trans->inserted_pre_trunc;
			record.updated_pre_trunc = trans->updated_pre_trunc;
			record.deleted_pre_trunc = trans->deleted_pre_trunc;
			record.t_id = tabstat->t_id;
			record.t_shared = tabstat->t_shared;
			record.t_truncated = trans->truncated;

			RegisterTwoPhaseRecord(TWOPHASE_RM_PGSTAT_ID, 0,
								   &record, sizeof(TwoPhasePgStatRecord));
		}
	}
}

/*
 * PostPrepare_PgStat
 *		Clean up after successful PREPARE.
 *
 * All we need do here is unlink the transaction stats state from the
 * nontransactional state.  The nontransactional action counts will be
 * reported to the activity stats facility immediately, while the effects on
 * live and dead tuple counts are preserved in the 2PC state file.
 *
 * Note: AtEOXact_PgStat is not called during PREPARE.
 */
void
PostPrepare_PgStat(void)
{
	PgStat_SubXactStatus *xact_state;

	/*
	 * We don't bother to free any of the transactional state, since it's all
	 * in TopTransactionContext and will go away anyway.
	 */
	xact_state = pgStatXactStack;
	if (xact_state != NULL)
	{
		PgStat_TableXactStatus *trans;

		for (trans = xact_state->first; trans != NULL; trans = trans->next)
		{
			PgStat_TableStatus *tabstat;

			tabstat = trans->parent;
			tabstat->trans = NULL;
		}
	}
	pgStatXactStack = NULL;

	/* Make sure any stats snapshot is thrown away */
	pgstat_clear_snapshot();
}

/*
 * 2PC processing routine for COMMIT PREPARED case.
 *
 * Load the saved counts into our local pgstats state.
 */
void
pgstat_twophase_postcommit(TransactionId xid, uint16 info,
						   void *recdata, uint32 len)
{
	TwoPhasePgStatRecord *rec = (TwoPhasePgStatRecord *) recdata;
	PgStat_TableStatus *pgstat_info;

	/* Find or create a tabstat entry for the rel */
	pgstat_info = get_local_tabstat_entry(rec->t_id, rec->t_shared);

	/* Same math as in AtEOXact_PgStat, commit case */
	pgstat_info->t_counts.t_tuples_inserted += rec->tuples_inserted;
	pgstat_info->t_counts.t_tuples_updated += rec->tuples_updated;
	pgstat_info->t_counts.t_tuples_deleted += rec->tuples_deleted;
	pgstat_info->t_counts.t_truncated = rec->t_truncated;
	if (rec->t_truncated)
	{
		/* forget live/dead stats seen by backend thus far */
		pgstat_info->t_counts.t_delta_live_tuples = 0;
		pgstat_info->t_counts.t_delta_dead_tuples = 0;
	}
	pgstat_info->t_counts.t_delta_live_tuples +=
		rec->tuples_inserted - rec->tuples_deleted;
	pgstat_info->t_counts.t_delta_dead_tuples +=
		rec->tuples_updated + rec->tuples_deleted;
	pgstat_info->t_counts.t_changed_tuples +=
		rec->tuples_inserted + rec->tuples_updated +
		rec->tuples_deleted;
}

/*
 * 2PC processing routine for ROLLBACK PREPARED case.
 *
 * Load the saved counts into our local pgstats state, but treat them
 * as aborted.
 */
void
pgstat_twophase_postabort(TransactionId xid, uint16 info,
						  void *recdata, uint32 len)
{
	TwoPhasePgStatRecord *rec = (TwoPhasePgStatRecord *) recdata;
	PgStat_TableStatus *pgstat_info;

	/* Find or create a tabstat entry for the rel */
	pgstat_info = get_local_tabstat_entry(rec->t_id, rec->t_shared);

	/* Same math as in AtEOXact_PgStat, abort case */
	if (rec->t_truncated)
	{
		rec->tuples_inserted = rec->inserted_pre_trunc;
		rec->tuples_updated = rec->updated_pre_trunc;
		rec->tuples_deleted = rec->deleted_pre_trunc;
	}
	pgstat_info->t_counts.t_tuples_inserted += rec->tuples_inserted;
	pgstat_info->t_counts.t_tuples_updated += rec->tuples_updated;
	pgstat_info->t_counts.t_tuples_deleted += rec->tuples_deleted;
	pgstat_info->t_counts.t_delta_dead_tuples +=
		rec->tuples_inserted + rec->tuples_updated;
}


/* ----------
 * snapshot_statentry() -
 *
 *  Common routine for functions pgstat_fetch_stat_*entry()
 *
 *  Returns the pointer to the snapshot of the shared entry for the key or NULL
 *  if not found. Returned snapshots are stable during the current transaction
 *  or until pgstat_clear_snapshot() is called.
 *
 *  Created snapshots are stored in pgStatSnapshotHash.
 */
static void *
snapshot_statentry(const PgStatTypes type, const Oid dbid, const Oid objid)
{
	PgStatSnapshot *snap = NULL;
	bool		found;
	PgStatHashEntryKey key;
	size_t		statentsize = pgstat_entsize[type];

	Assert(type != PGSTAT_TYPE_ALL);

	/*
	 * Create new hash, with rather arbitrary initial number of entries since
	 * we don't know how this hash will grow.
	 */
	if (!pgStatSnapshotHash)
	{
		HASHCTL		ctl;

		/*
		 * Create the hash in the stats context
		 *
		 * The entry is prepended by common header part represented by
		 * PgStatSnapshot.
		 */

		ctl.keysize = sizeof(PgStatHashEntryKey);
		ctl.entrysize = PgStatSnapshotSize(statentsize);
		ctl.hcxt = pgStatSnapshotContext;
		pgStatSnapshotHash = hash_create("pgstat snapshot hash", 32, &ctl,
										 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	/* Find a snapshot  */
	key.type = type;
	key.databaseid = dbid;
	key.objectid = objid;

	snap = hash_search(pgStatSnapshotHash, &key, HASH_ENTER, &found);

	/*
	 * Refer shared hash if not found in the snapshot hash.
	 *
	 * In transaction state, it is obvious that we should create a snapshot
	 * entriy for consistency. If we are not, we return an up-to-date entry.
	 * Having said that, we need a snapshot since shared stats entry can be
	 * modified anytime. We share the same snapshot entry for the purpose.
	 */
	if (!found || !IsTransactionState())
	{
		PgStatEnvelope *shenv;

		shenv = get_stat_entry(type, dbid, objid, true, NULL, NULL);

		if (shenv)
			memcpy(&snap->body, &shenv->body, statentsize);

		snap->negative = !shenv;
	}

	if (snap->negative)
		return NULL;

	return &snap->body;
}


/* ----------
 * pgstat_fetch_stat_dbentry() -
 *
 *	Find database stats entry on backends. The returned entries are cached
 *	until transaction end or pgstat_clear_snapshot() is called.
 * ----------
 */
PgStat_StatDBEntry *
pgstat_fetch_stat_dbentry(Oid dbid)
{
	/* should be called from backends */
	Assert(IsUnderPostmaster);

	/* If not done for this transaction, take a snapshot of global stats */
	pgstat_snapshot_global_stats();

	/* caller doesn't have a business with snapshot-local members */
	return (PgStat_StatDBEntry *)
		snapshot_statentry(PGSTAT_TYPE_DB, dbid, InvalidOid);
}

/* ----------
 * pgstat_fetch_stat_tabentry() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	the activity statistics for one table or NULL. NULL doesn't mean
 *	that the table doesn't exist, it is just not yet known by the
 *	activity statistics facilities, so the caller is better off to
 *	report ZERO instead.
 * ----------
 */
PgStat_StatTabEntry *
pgstat_fetch_stat_tabentry(Oid relid)
{
	PgStat_StatTabEntry *tabentry;

	tabentry = pgstat_fetch_stat_tabentry_snapshot(false, relid);
	if (tabentry != NULL)
		return tabentry;

	/*
	 * If we didn't find it, maybe it's a shared table.
	 */
	tabentry = pgstat_fetch_stat_tabentry_snapshot(true, relid);
	return tabentry;
}


/* ----------
 * pgstat_fetch_stat_tabentry_snapshot() -
 *
 *	Find table stats entry on backends in dbent. The returned entry is cached
 *	until transaction end or pgstat_clear_snapshot() is called.
 */
PgStat_StatTabEntry *
pgstat_fetch_stat_tabentry_snapshot(bool shared, Oid reloid)
{
	Oid			dboid = (shared ? InvalidOid : MyDatabaseId);

	/* should be called from backends */
	Assert(IsUnderPostmaster);

	return (PgStat_StatTabEntry *)
		snapshot_statentry(PGSTAT_TYPE_TABLE, dboid, reloid);
}


/* ----------
 * pgstat_copy_index_counters() -
 *
 *	Support function for index swapping. Copy a portion of the counters of the
 *	relation to specified place.
 * ----------
 */
void
pgstat_copy_index_counters(Oid relid, PgStat_TableStatus *dst)
{
	PgStat_StatTabEntry *tabentry;

	/* No point fetching tabentry when dst is NULL */
	if (!dst)
		return;

	tabentry = pgstat_fetch_stat_tabentry(relid);

	if (!tabentry)
		return;

	dst->t_counts.t_numscans = tabentry->numscans;
	dst->t_counts.t_tuples_returned = tabentry->tuples_returned;
	dst->t_counts.t_tuples_fetched = tabentry->tuples_fetched;
	dst->t_counts.t_blocks_fetched = tabentry->blocks_fetched;
	dst->t_counts.t_blocks_hit = tabentry->blocks_hit;
}


/* ----------
 * pgstat_fetch_stat_funcentry() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	the collected statistics for one function or NULL.
 * ----------
 */
PgStat_StatFuncEntry *
pgstat_fetch_stat_funcentry(Oid func_id)
{
	/* should be called from backends */
	Assert(IsUnderPostmaster);

	return (PgStat_StatFuncEntry *)
		snapshot_statentry(PGSTAT_TYPE_FUNCTION, MyDatabaseId, func_id);
}

/*
 * pgstat_snapshot_global_stats() -
 *
 * Makes a snapshot of global stats if not done yet.  They will be kept until
 * subsequent call of pgstat_clear_snapshot() or the end of the current
 * memory context (typically TopTransactionContext).
 * ----------
 */
static void
pgstat_snapshot_global_stats(void)
{
	MemoryContext oldcontext;

	attach_shared_stats();

	/* Nothing to do if already done */
	if (global_snapshot_is_valid)
		return;

	oldcontext = MemoryContextSwitchTo(pgStatSnapshotContext);

	LWLockAcquire(StatsLock, LW_SHARED);
	memcpy(&snapshot_globalStats, shared_globalStats,
		   sizeof(PgStat_GlobalStats));

	memcpy(&snapshot_archiverStats, shared_archiverStats,
		   sizeof(PgStat_ArchiverStats));
	LWLockRelease(StatsLock);

	global_snapshot_is_valid = true;

	MemoryContextSwitchTo(oldcontext);

	return;
}

/* ----------
 * pgstat_fetch_stat_beentry() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	our local copy of the current-activity entry for one backend.
 *
 *	NB: caller is responsible for a check if the user is permitted to see
 *	this info (especially the querystring).
 * ----------
 */
PgBackendStatus *
pgstat_fetch_stat_beentry(int beid)
{
	pgstat_read_current_status();

	if (beid < 1 || beid > localNumBackends)
		return NULL;

	return &localBackendStatusTable[beid - 1].backendStatus;
}


/* ----------
 * pgstat_fetch_stat_local_beentry() -
 *
 *	Like pgstat_fetch_stat_beentry() but with locally computed additions (like
 *	xid and xmin values of the backend)
 *
 *	NB: caller is responsible for a check if the user is permitted to see
 *	this info (especially the querystring).
 * ----------
 */
LocalPgBackendStatus *
pgstat_fetch_stat_local_beentry(int beid)
{
	pgstat_read_current_status();

	if (beid < 1 || beid > localNumBackends)
		return NULL;

	return &localBackendStatusTable[beid - 1];
}


/* ----------
 * pgstat_fetch_stat_numbackends() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	the maximum current backend id.
 * ----------
 */
int
pgstat_fetch_stat_numbackends(void)
{
	pgstat_read_current_status();

	return localNumBackends;
}

/*
 * ---------
 * pgstat_fetch_stat_archiver() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	a pointer to the archiver statistics struct.
 * ---------
 */
PgStat_ArchiverStats *
pgstat_fetch_stat_archiver(void)
{
	/* If not done for this transaction, take a stats snapshot */
	pgstat_snapshot_global_stats();

	return &snapshot_archiverStats;
}


/*
 * ---------
 * pgstat_fetch_global() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	a pointer to the global statistics struct.
 * ---------
 */
PgStat_GlobalStats *
pgstat_fetch_global(void)
{
	/* If not done for this transaction, take a stats snapshot */
	pgstat_snapshot_global_stats();

	return &snapshot_globalStats;
}


/* ------------------------------------------------------------
 * Functions for management of the shared-memory PgBackendStatus array
 * ------------------------------------------------------------
 */

static PgBackendStatus *BackendStatusArray = NULL;
static PgBackendStatus *MyBEEntry = NULL;
static char *BackendAppnameBuffer = NULL;
static char *BackendClientHostnameBuffer = NULL;
static char *BackendActivityBuffer = NULL;
static Size BackendActivityBufferSize = 0;
#ifdef USE_SSL
static PgBackendSSLStatus *BackendSslStatusBuffer = NULL;
#endif
#ifdef ENABLE_GSS
static PgBackendGSSStatus *BackendGssStatusBuffer = NULL;
#endif


/*
 * Report shared-memory space needed by CreateSharedBackendStatus.
 */
Size
BackendStatusShmemSize(void)
{
	Size		size;

	/* BackendStatusArray: */
	size = mul_size(sizeof(PgBackendStatus), NumBackendStatSlots);
	/* BackendAppnameBuffer: */
	size = add_size(size,
					mul_size(NAMEDATALEN, NumBackendStatSlots));
	/* BackendClientHostnameBuffer: */
	size = add_size(size,
					mul_size(NAMEDATALEN, NumBackendStatSlots));
	/* BackendActivityBuffer: */
	size = add_size(size,
					mul_size(pgstat_track_activity_query_size, NumBackendStatSlots));
#ifdef USE_SSL
	/* BackendSslStatusBuffer: */
	size = add_size(size,
					mul_size(sizeof(PgBackendSSLStatus), NumBackendStatSlots));
#endif
	return size;
}

/*
 * Initialize the shared status array and several string buffers
 * during postmaster startup.
 */
void
CreateSharedBackendStatus(void)
{
	Size		size;
	bool		found;
	int			i;
	char	   *buffer;

	/* Create or attach to the shared array */
	size = mul_size(sizeof(PgBackendStatus), NumBackendStatSlots);
	BackendStatusArray = (PgBackendStatus *)
		ShmemInitStruct("Backend Status Array", size, &found);

	if (!found)
	{
		/*
		 * We're the first - initialize.
		 */
		MemSet(BackendStatusArray, 0, size);
	}

	/* Create or attach to the shared appname buffer */
	size = mul_size(NAMEDATALEN, NumBackendStatSlots);
	BackendAppnameBuffer = (char *)
		ShmemInitStruct("Backend Application Name Buffer", size, &found);

	if (!found)
	{
		MemSet(BackendAppnameBuffer, 0, size);

		/* Initialize st_appname pointers. */
		buffer = BackendAppnameBuffer;
		for (i = 0; i < NumBackendStatSlots; i++)
		{
			BackendStatusArray[i].st_appname = buffer;
			buffer += NAMEDATALEN;
		}
	}

	/* Create or attach to the shared client hostname buffer */
	size = mul_size(NAMEDATALEN, NumBackendStatSlots);
	BackendClientHostnameBuffer = (char *)
		ShmemInitStruct("Backend Client Host Name Buffer", size, &found);

	if (!found)
	{
		MemSet(BackendClientHostnameBuffer, 0, size);

		/* Initialize st_clienthostname pointers. */
		buffer = BackendClientHostnameBuffer;
		for (i = 0; i < NumBackendStatSlots; i++)
		{
			BackendStatusArray[i].st_clienthostname = buffer;
			buffer += NAMEDATALEN;
		}
	}

	/* Create or attach to the shared activity buffer */
	BackendActivityBufferSize = mul_size(pgstat_track_activity_query_size,
										 NumBackendStatSlots);
	BackendActivityBuffer = (char *)
		ShmemInitStruct("Backend Activity Buffer",
						BackendActivityBufferSize,
						&found);

	if (!found)
	{
		MemSet(BackendActivityBuffer, 0, BackendActivityBufferSize);

		/* Initialize st_activity pointers. */
		buffer = BackendActivityBuffer;
		for (i = 0; i < NumBackendStatSlots; i++)
		{
			BackendStatusArray[i].st_activity_raw = buffer;
			buffer += pgstat_track_activity_query_size;
		}
	}

#ifdef USE_SSL
	/* Create or attach to the shared SSL status buffer */
	size = mul_size(sizeof(PgBackendSSLStatus), NumBackendStatSlots);
	BackendSslStatusBuffer = (PgBackendSSLStatus *)
		ShmemInitStruct("Backend SSL Status Buffer", size, &found);

	if (!found)
	{
		PgBackendSSLStatus *ptr;

		MemSet(BackendSslStatusBuffer, 0, size);

		/* Initialize st_sslstatus pointers. */
		ptr = BackendSslStatusBuffer;
		for (i = 0; i < NumBackendStatSlots; i++)
		{
			BackendStatusArray[i].st_sslstatus = ptr;
			ptr++;
		}
	}
#endif

#ifdef ENABLE_GSS
	/* Create or attach to the shared GSSAPI status buffer */
	size = mul_size(sizeof(PgBackendGSSStatus), NumBackendStatSlots);
	BackendGssStatusBuffer = (PgBackendGSSStatus *)
		ShmemInitStruct("Backend GSS Status Buffer", size, &found);

	if (!found)
	{
		PgBackendGSSStatus *ptr;

		MemSet(BackendGssStatusBuffer, 0, size);

		/* Initialize st_gssstatus pointers. */
		ptr = BackendGssStatusBuffer;
		for (i = 0; i < NumBackendStatSlots; i++)
		{
			BackendStatusArray[i].st_gssstatus = ptr;
			ptr++;
		}
	}
#endif
}


/* ----------
 * pgstat_initialize() -
 *
 *	Initialize pgstats state, and set up our on-proc-exit hook.
 *	Called from InitPostgres and AuxiliaryProcessMain. For auxiliary process,
 *	MyBackendId is invalid. Otherwise, MyBackendId must be set,
 *	but we must not have started any transaction yet (since the
 *	exit hook must run after the last transaction exit).
 *	NOTE: MyDatabaseId isn't set yet; so the shutdown hook has to be careful.
 * ----------
 */
void
pgstat_initialize(void)
{
	/* Initialize MyBEEntry */
	if (MyBackendId != InvalidBackendId)
	{
		Assert(MyBackendId >= 1 && MyBackendId <= MaxBackends);
		MyBEEntry = &BackendStatusArray[MyBackendId - 1];
	}
	else
	{
		/* Must be an auxiliary process */
		Assert(MyAuxProcType != NotAnAuxProcess);

		/*
		 * Assign the MyBEEntry for an auxiliary process.  Since it doesn't
		 * have a BackendId, the slot is statically allocated based on the
		 * auxiliary process type (MyAuxProcType).  Backends use slots indexed
		 * in the range from 1 to MaxBackends (inclusive), so we use
		 * MaxBackends + AuxBackendType + 1 as the index of the slot for an
		 * auxiliary process.
		 */
		MyBEEntry = &BackendStatusArray[MaxBackends + MyAuxProcType];
	}

	/* need to be called before dsm shutdown */
	before_shmem_exit(pgstat_beshutdown_hook, 0);
}

/* ----------
 * pgstat_bestart() -
 *
 *	Initialize this backend's entry in the PgBackendStatus array.
 *	Called from InitPostgres.
 *
 *	Apart from auxiliary processes, MyBackendId, MyDatabaseId,
 *	session userid, and application_name must be set for a
 *	backend (hence, this cannot be combined with pgstat_initialize).
 *	Note also that we must be inside a transaction if this isn't an aux
 *	process, as we may need to do encoding conversion on some strings.
 * ----------
 */
void
pgstat_bestart(void)
{
	volatile PgBackendStatus *vbeentry = MyBEEntry;
	PgBackendStatus lbeentry;
#ifdef USE_SSL
	PgBackendSSLStatus lsslstatus;
#endif
#ifdef ENABLE_GSS
	PgBackendGSSStatus lgssstatus;
#endif

	/* pgstats state must be initialized from pgstat_initialize() */
	Assert(vbeentry != NULL);

	/*
	 * To minimize the time spent modifying the PgBackendStatus entry, and
	 * avoid risk of errors inside the critical section, we first copy the
	 * shared-memory struct to a local variable, then modify the data in the
	 * local variable, then copy the local variable back to shared memory.
	 * Only the last step has to be inside the critical section.
	 *
	 * Most of the data we copy from shared memory is just going to be
	 * overwritten, but the struct's not so large that it's worth the
	 * maintenance hassle to copy only the needful fields.
	 */
	memcpy(&lbeentry,
		   unvolatize(PgBackendStatus *, vbeentry),
		   sizeof(PgBackendStatus));

	/* These structs can just start from zeroes each time, though */
#ifdef USE_SSL
	memset(&lsslstatus, 0, sizeof(lsslstatus));
#endif
#ifdef ENABLE_GSS
	memset(&lgssstatus, 0, sizeof(lgssstatus));
#endif

	/*
	 * Now fill in all the fields of lbeentry, except for strings that are
	 * out-of-line data.  Those have to be handled separately, below.
	 */
	lbeentry.st_procpid = MyProcPid;
	lbeentry.st_backendType = MyBackendType;
	lbeentry.st_proc_start_timestamp = MyStartTimestamp;
	lbeentry.st_activity_start_timestamp = 0;
	lbeentry.st_state_start_timestamp = 0;
	lbeentry.st_xact_start_timestamp = 0;
	lbeentry.st_databaseid = MyDatabaseId;

	/* We have userid for client-backends, wal-sender and bgworker processes */
	if (lbeentry.st_backendType == B_BACKEND
		|| lbeentry.st_backendType == B_WAL_SENDER
		|| lbeentry.st_backendType == B_BG_WORKER)
		lbeentry.st_userid = GetSessionUserId();
	else
		lbeentry.st_userid = InvalidOid;

	/*
	 * We may not have a MyProcPort (eg, if this is the autovacuum process).
	 * If so, use all-zeroes client address, which is dealt with specially in
	 * pg_stat_get_backend_client_addr and pg_stat_get_backend_client_port.
	 */
	if (MyProcPort)
		memcpy(&lbeentry.st_clientaddr, &MyProcPort->raddr,
			   sizeof(lbeentry.st_clientaddr));
	else
		MemSet(&lbeentry.st_clientaddr, 0, sizeof(lbeentry.st_clientaddr));

#ifdef USE_SSL
	if (MyProcPort && MyProcPort->ssl != NULL)
	{
		lbeentry.st_ssl = true;
		lsslstatus.ssl_bits = be_tls_get_cipher_bits(MyProcPort);
		lsslstatus.ssl_compression = be_tls_get_compression(MyProcPort);
		strlcpy(lsslstatus.ssl_version, be_tls_get_version(MyProcPort), NAMEDATALEN);
		strlcpy(lsslstatus.ssl_cipher, be_tls_get_cipher(MyProcPort), NAMEDATALEN);
		be_tls_get_peer_subject_name(MyProcPort, lsslstatus.ssl_client_dn, NAMEDATALEN);
		be_tls_get_peer_serial(MyProcPort, lsslstatus.ssl_client_serial, NAMEDATALEN);
		be_tls_get_peer_issuer_name(MyProcPort, lsslstatus.ssl_issuer_dn, NAMEDATALEN);
	}
	else
	{
		lbeentry.st_ssl = false;
	}
#else
	lbeentry.st_ssl = false;
#endif

#ifdef ENABLE_GSS
	if (MyProcPort && MyProcPort->gss != NULL)
	{
		lbeentry.st_gss = true;
		lgssstatus.gss_auth = be_gssapi_get_auth(MyProcPort);
		lgssstatus.gss_enc = be_gssapi_get_enc(MyProcPort);

		if (lgssstatus.gss_auth)
			strlcpy(lgssstatus.gss_princ, be_gssapi_get_princ(MyProcPort), NAMEDATALEN);
	}
	else
	{
		lbeentry.st_gss = false;
	}
#else
	lbeentry.st_gss = false;
#endif

	lbeentry.st_state = STATE_UNDEFINED;
	lbeentry.st_progress_command = PROGRESS_COMMAND_INVALID;
	lbeentry.st_progress_command_target = InvalidOid;

	/*
	 * we don't zero st_progress_param here to save cycles; nobody should
	 * examine it until st_progress_command has been set to something other
	 * than PROGRESS_COMMAND_INVALID
	 */

	/*
	 * We're ready to enter the critical section that fills the shared-memory
	 * status entry.  We follow the protocol of bumping st_changecount before
	 * and after; and make sure it's even afterwards.  We use a volatile
	 * pointer here to ensure the compiler doesn't try to get cute.
	 */
	PGSTAT_BEGIN_WRITE_ACTIVITY(vbeentry);

	/* make sure we'll memcpy the same st_changecount back */
	lbeentry.st_changecount = vbeentry->st_changecount;

	memcpy(unvolatize(PgBackendStatus *, vbeentry),
		   &lbeentry,
		   sizeof(PgBackendStatus));

	/*
	 * We can write the out-of-line strings and structs using the pointers
	 * that are in lbeentry; this saves some de-volatilizing messiness.
	 */
	lbeentry.st_appname[0] = '\0';
	if (MyProcPort && MyProcPort->remote_hostname)
		strlcpy(lbeentry.st_clienthostname, MyProcPort->remote_hostname,
				NAMEDATALEN);
	else
		lbeentry.st_clienthostname[0] = '\0';
	lbeentry.st_activity_raw[0] = '\0';
	/* Also make sure the last byte in each string area is always 0 */
	lbeentry.st_appname[NAMEDATALEN - 1] = '\0';
	lbeentry.st_clienthostname[NAMEDATALEN - 1] = '\0';
	lbeentry.st_activity_raw[pgstat_track_activity_query_size - 1] = '\0';

#ifdef USE_SSL
	memcpy(lbeentry.st_sslstatus, &lsslstatus, sizeof(PgBackendSSLStatus));
#endif
#ifdef ENABLE_GSS
	memcpy(lbeentry.st_gssstatus, &lgssstatus, sizeof(PgBackendGSSStatus));
#endif

	PGSTAT_END_WRITE_ACTIVITY(vbeentry);

	/* Update app name to current GUC setting */
	if (application_name)
		pgstat_report_appname(application_name);

	/* attach shared database stats area */
	attach_shared_stats();
}

/*
 * Shut down a single backend's statistics reporting at process exit.
 *
 * Flush any remaining statistics counts out to shared stats.
 * Without this, operations triggered during backend exit (such as
 * temp table deletions) won't be counted.
 *
 * Lastly, clear out our entry in the PgBackendStatus array.
 */
static void
pgstat_beshutdown_hook(int code, Datum arg)
{
	volatile PgBackendStatus *beentry = MyBEEntry;

	/*
	 * If we got as far as discovering our own database ID, we can report what
	 * we did to the shares stats.  Otherwise, we'd be sending an invalid
	 * database ID, so forget it.  (This means that accesses to pg_database
	 * during failed backend starts might never get counted.)
	 */
	if (OidIsValid(MyDatabaseId))
		pgstat_report_stat(true);

	/*
	 * Clear my status entry, following the protocol of bumping st_changecount
	 * before and after.  We use a volatile pointer here to ensure the
	 * compiler doesn't try to get cute.
	 */
	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

	beentry->st_procpid = 0;	/* mark invalid */

	PGSTAT_END_WRITE_ACTIVITY(beentry);

	detach_shared_stats(true);
}


/* ----------
 * pgstat_report_activity() -
 *
 *	Called from tcop/postgres.c to report what the backend is actually doing
 *	(but note cmd_str can be NULL for certain cases).
 *
 * All updates of the status entry follow the protocol of bumping
 * st_changecount before and after.  We use a volatile pointer here to
 * ensure the compiler doesn't try to get cute.
 * ----------
 */
void
pgstat_report_activity(BackendState state, const char *cmd_str)
{
	volatile PgBackendStatus *beentry = MyBEEntry;
	TimestampTz start_timestamp;
	TimestampTz current_timestamp;
	int			len = 0;

	TRACE_POSTGRESQL_STATEMENT_STATUS(cmd_str);

	if (!beentry)
		return;

	if (!pgstat_track_activities)
	{
		if (beentry->st_state != STATE_DISABLED)
		{
			volatile PGPROC *proc = MyProc;

			/*
			 * track_activities is disabled, but we last reported a
			 * non-disabled state.  As our final update, change the state and
			 * clear fields we will not be updating anymore.
			 */
			PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
			beentry->st_state = STATE_DISABLED;
			beentry->st_state_start_timestamp = 0;
			beentry->st_activity_raw[0] = '\0';
			beentry->st_activity_start_timestamp = 0;
			/* st_xact_start_timestamp and wait_event_info are also disabled */
			beentry->st_xact_start_timestamp = 0;
			proc->wait_event_info = 0;
			PGSTAT_END_WRITE_ACTIVITY(beentry);
		}
		return;
	}

	/*
	 * To minimize the time spent modifying the entry, and avoid risk of
	 * errors inside the critical section, fetch all the needed data first.
	 */
	start_timestamp = GetCurrentStatementStartTimestamp();
	if (cmd_str != NULL)
	{
		/*
		 * Compute length of to-be-stored string unaware of multi-byte
		 * characters. For speed reasons that'll get corrected on read, rather
		 * than computed every write.
		 */
		len = Min(strlen(cmd_str), pgstat_track_activity_query_size - 1);
	}
	current_timestamp = GetCurrentTimestamp();

	/*
	 * Now update the status entry
	 */
	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

	beentry->st_state = state;
	beentry->st_state_start_timestamp = current_timestamp;

	if (cmd_str != NULL)
	{
		memcpy((char *) beentry->st_activity_raw, cmd_str, len);
		beentry->st_activity_raw[len] = '\0';
		beentry->st_activity_start_timestamp = start_timestamp;
	}

	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/*-----------
 * pgstat_progress_start_command() -
 *
 * Set st_progress_command (and st_progress_command_target) in own backend
 * entry.  Also, zero-initialize st_progress_param array.
 *-----------
 */
void
pgstat_progress_start_command(ProgressCommandType cmdtype, Oid relid)
{
	volatile PgBackendStatus *beentry = MyBEEntry;

	if (!beentry || !pgstat_track_activities)
		return;

	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
	beentry->st_progress_command = cmdtype;
	beentry->st_progress_command_target = relid;
	MemSet(&beentry->st_progress_param, 0, sizeof(beentry->st_progress_param));
	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/*-----------
 * pgstat_progress_update_param() -
 *
 * Update index'th member in st_progress_param[] of own backend entry.
 *-----------
 */
void
pgstat_progress_update_param(int index, int64 val)
{
	volatile PgBackendStatus *beentry = MyBEEntry;

	Assert(index >= 0 && index < PGSTAT_NUM_PROGRESS_PARAM);

	if (!beentry || !pgstat_track_activities)
		return;

	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
	beentry->st_progress_param[index] = val;
	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/*-----------
 * pgstat_progress_update_multi_param() -
 *
 * Update multiple members in st_progress_param[] of own backend entry.
 * This is atomic; readers won't see intermediate states.
 *-----------
 */
void
pgstat_progress_update_multi_param(int nparam, const int *index,
								   const int64 *val)
{
	volatile PgBackendStatus *beentry = MyBEEntry;
	int			i;

	if (!beentry || !pgstat_track_activities || nparam == 0)
		return;

	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

	for (i = 0; i < nparam; ++i)
	{
		Assert(index[i] >= 0 && index[i] < PGSTAT_NUM_PROGRESS_PARAM);

		beentry->st_progress_param[index[i]] = val[i];
	}

	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/*-----------
 * pgstat_progress_end_command() -
 *
 * Reset st_progress_command (and st_progress_command_target) in own backend
 * entry.  This signals the end of the command.
 *-----------
 */
void
pgstat_progress_end_command(void)
{
	volatile PgBackendStatus *beentry = MyBEEntry;

	if (!beentry || !pgstat_track_activities)
		return;

	if (beentry->st_progress_command == PROGRESS_COMMAND_INVALID)
		return;

	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
	beentry->st_progress_command = PROGRESS_COMMAND_INVALID;
	beentry->st_progress_command_target = InvalidOid;
	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/* ----------
 * pgstat_report_appname() -
 *
 *	Called to update our application name.
 * ----------
 */
void
pgstat_report_appname(const char *appname)
{
	volatile PgBackendStatus *beentry = MyBEEntry;
	int			len;

	if (!beentry)
		return;

	/* This should be unnecessary if GUC did its job, but be safe */
	len = pg_mbcliplen(appname, strlen(appname), NAMEDATALEN - 1);

	/*
	 * Update my status entry, following the protocol of bumping
	 * st_changecount before and after.  We use a volatile pointer here to
	 * ensure the compiler doesn't try to get cute.
	 */
	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

	memcpy((char *) beentry->st_appname, appname, len);
	beentry->st_appname[len] = '\0';

	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/*
 * Report current transaction start timestamp as the specified value.
 * Zero means there is no active transaction.
 */
void
pgstat_report_xact_timestamp(TimestampTz tstamp)
{
	volatile PgBackendStatus *beentry = MyBEEntry;

	if (!pgstat_track_activities || !beentry)
		return;

	/*
	 * Update my status entry, following the protocol of bumping
	 * st_changecount before and after.  We use a volatile pointer here to
	 * ensure the compiler doesn't try to get cute.
	 */
	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

	beentry->st_xact_start_timestamp = tstamp;

	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/* ----------
 * pgstat_read_current_status() -
 *
 *	Copy the current contents of the PgBackendStatus array to local memory,
 *	if not already done in this transaction.
 * ----------
 */
static void
pgstat_read_current_status(void)
{
	volatile PgBackendStatus *beentry;
	LocalPgBackendStatus *localtable;
	LocalPgBackendStatus *localentry;
	char	   *localappname,
			   *localclienthostname,
			   *localactivity;
#ifdef USE_SSL
	PgBackendSSLStatus *localsslstatus;
#endif
#ifdef ENABLE_GSS
	PgBackendGSSStatus *localgssstatus;
#endif
	int			i;

	Assert(IsUnderPostmaster);

	if (localBackendStatusTable)
		return;					/* already done */

	pgstat_setup_memcxt();

	/*
	 * Allocate storage for local copy of state data.  We can presume that
	 * none of these requests overflow size_t, because we already calculated
	 * the same values using mul_size during shmem setup.  However, with
	 * probably-silly values of pgstat_track_activity_query_size and
	 * max_connections, the localactivity buffer could exceed 1GB, so use
	 * "huge" allocation for that one.
	 */
	localtable = (LocalPgBackendStatus *)
		MemoryContextAlloc(pgStatLocalContext,
						   sizeof(LocalPgBackendStatus) * NumBackendStatSlots);
	localappname = (char *)
		MemoryContextAlloc(pgStatLocalContext,
						   NAMEDATALEN * NumBackendStatSlots);
	localclienthostname = (char *)
		MemoryContextAlloc(pgStatLocalContext,
						   NAMEDATALEN * NumBackendStatSlots);
	localactivity = (char *)
		MemoryContextAllocHuge(pgStatLocalContext,
							   pgstat_track_activity_query_size * NumBackendStatSlots);
#ifdef USE_SSL
	localsslstatus = (PgBackendSSLStatus *)
		MemoryContextAlloc(pgStatLocalContext,
						   sizeof(PgBackendSSLStatus) * NumBackendStatSlots);
#endif
#ifdef ENABLE_GSS
	localgssstatus = (PgBackendGSSStatus *)
		MemoryContextAlloc(pgStatLocalContext,
						   sizeof(PgBackendGSSStatus) * NumBackendStatSlots);
#endif

	localNumBackends = 0;

	beentry = BackendStatusArray;
	localentry = localtable;
	for (i = 1; i <= NumBackendStatSlots; i++)
	{
		/*
		 * Follow the protocol of retrying if st_changecount changes while we
		 * copy the entry, or if it's odd.  (The check for odd is needed to
		 * cover the case where we are able to completely copy the entry while
		 * the source backend is between increment steps.)	We use a volatile
		 * pointer here to ensure the compiler doesn't try to get cute.
		 */
		for (;;)
		{
			int			before_changecount;
			int			after_changecount;

			pgstat_begin_read_activity(beentry, before_changecount);

			localentry->backendStatus.st_procpid = beentry->st_procpid;
			/* Skip all the data-copying work if entry is not in use */
			if (localentry->backendStatus.st_procpid > 0)
			{
				memcpy(&localentry->backendStatus, unvolatize(PgBackendStatus *, beentry), sizeof(PgBackendStatus));

				/*
				 * For each PgBackendStatus field that is a pointer, copy the
				 * pointed-to data, then adjust the local copy of the pointer
				 * field to point at the local copy of the data.
				 *
				 * strcpy is safe even if the string is modified concurrently,
				 * because there's always a \0 at the end of the buffer.
				 */
				strcpy(localappname, (char *) beentry->st_appname);
				localentry->backendStatus.st_appname = localappname;
				strcpy(localclienthostname, (char *) beentry->st_clienthostname);
				localentry->backendStatus.st_clienthostname = localclienthostname;
				strcpy(localactivity, (char *) beentry->st_activity_raw);
				localentry->backendStatus.st_activity_raw = localactivity;
#ifdef USE_SSL
				if (beentry->st_ssl)
				{
					memcpy(localsslstatus, beentry->st_sslstatus, sizeof(PgBackendSSLStatus));
					localentry->backendStatus.st_sslstatus = localsslstatus;
				}
#endif
#ifdef ENABLE_GSS
				if (beentry->st_gss)
				{
					memcpy(localgssstatus, beentry->st_gssstatus, sizeof(PgBackendGSSStatus));
					localentry->backendStatus.st_gssstatus = localgssstatus;
				}
#endif
			}

			pgstat_end_read_activity(beentry, after_changecount);

			if (pgstat_read_activity_complete(before_changecount,
											  after_changecount))
				break;

			/* Make sure we can break out of loop if stuck... */
			CHECK_FOR_INTERRUPTS();
		}

		beentry++;
		/* Only valid entries get included into the local array */
		if (localentry->backendStatus.st_procpid > 0)
		{
			BackendIdGetTransactionIds(i,
									   &localentry->backend_xid,
									   &localentry->backend_xmin);

			localentry++;
			localappname += NAMEDATALEN;
			localclienthostname += NAMEDATALEN;
			localactivity += pgstat_track_activity_query_size;
#ifdef USE_SSL
			localsslstatus++;
#endif
#ifdef ENABLE_GSS
			localgssstatus++;
#endif
			localNumBackends++;
		}
	}

	/* Set the pointer only after completion of a valid table */
	localBackendStatusTable = localtable;
}

/* ----------
 * pgstat_get_wait_event_type() -
 *
 *	Return a string representing the current wait event type, backend is
 *	waiting on.
 */
const char *
pgstat_get_wait_event_type(uint32 wait_event_info)
{
	uint32		classId;
	const char *event_type;

	/* report process as not waiting. */
	if (wait_event_info == 0)
		return NULL;

	classId = wait_event_info & 0xFF000000;

	switch (classId)
	{
		case PG_WAIT_LWLOCK:
			event_type = "LWLock";
			break;
		case PG_WAIT_LOCK:
			event_type = "Lock";
			break;
		case PG_WAIT_BUFFER_PIN:
			event_type = "BufferPin";
			break;
		case PG_WAIT_ACTIVITY:
			event_type = "Activity";
			break;
		case PG_WAIT_CLIENT:
			event_type = "Client";
			break;
		case PG_WAIT_EXTENSION:
			event_type = "Extension";
			break;
		case PG_WAIT_IPC:
			event_type = "IPC";
			break;
		case PG_WAIT_TIMEOUT:
			event_type = "Timeout";
			break;
		case PG_WAIT_IO:
			event_type = "IO";
			break;
		default:
			event_type = "???";
			break;
	}

	return event_type;
}

/* ----------
 * pgstat_get_wait_event() -
 *
 *	Return a string representing the current wait event, backend is
 *	waiting on.
 */
const char *
pgstat_get_wait_event(uint32 wait_event_info)
{
	uint32		classId;
	uint16		eventId;
	const char *event_name;

	/* report process as not waiting. */
	if (wait_event_info == 0)
		return NULL;

	classId = wait_event_info & 0xFF000000;
	eventId = wait_event_info & 0x0000FFFF;

	switch (classId)
	{
		case PG_WAIT_LWLOCK:
			event_name = GetLWLockIdentifier(classId, eventId);
			break;
		case PG_WAIT_LOCK:
			event_name = GetLockNameFromTagType(eventId);
			break;
		case PG_WAIT_BUFFER_PIN:
			event_name = "BufferPin";
			break;
		case PG_WAIT_ACTIVITY:
			{
				WaitEventActivity w = (WaitEventActivity) wait_event_info;

				event_name = pgstat_get_wait_activity(w);
				break;
			}
		case PG_WAIT_CLIENT:
			{
				WaitEventClient w = (WaitEventClient) wait_event_info;

				event_name = pgstat_get_wait_client(w);
				break;
			}
		case PG_WAIT_EXTENSION:
			event_name = "Extension";
			break;
		case PG_WAIT_IPC:
			{
				WaitEventIPC w = (WaitEventIPC) wait_event_info;

				event_name = pgstat_get_wait_ipc(w);
				break;
			}
		case PG_WAIT_TIMEOUT:
			{
				WaitEventTimeout w = (WaitEventTimeout) wait_event_info;

				event_name = pgstat_get_wait_timeout(w);
				break;
			}
		case PG_WAIT_IO:
			{
				WaitEventIO w = (WaitEventIO) wait_event_info;

				event_name = pgstat_get_wait_io(w);
				break;
			}
		default:
			event_name = "unknown wait event";
			break;
	}

	return event_name;
}

/* ----------
 * pgstat_get_wait_activity() -
 *
 * Convert WaitEventActivity to string.
 * ----------
 */
static const char *
pgstat_get_wait_activity(WaitEventActivity w)
{
	const char *event_name = "unknown wait event";

	switch (w)
	{
		case WAIT_EVENT_ARCHIVER_MAIN:
			event_name = "ArchiverMain";
			break;
		case WAIT_EVENT_AUTOVACUUM_MAIN:
			event_name = "AutoVacuumMain";
			break;
		case WAIT_EVENT_BGWRITER_HIBERNATE:
			event_name = "BgWriterHibernate";
			break;
		case WAIT_EVENT_BGWRITER_MAIN:
			event_name = "BgWriterMain";
			break;
		case WAIT_EVENT_CHECKPOINTER_MAIN:
			event_name = "CheckpointerMain";
			break;
		case WAIT_EVENT_LOGICAL_APPLY_MAIN:
			event_name = "LogicalApplyMain";
			break;
		case WAIT_EVENT_LOGICAL_LAUNCHER_MAIN:
			event_name = "LogicalLauncherMain";
			break;
		case WAIT_EVENT_RECOVERY_WAL_STREAM:
			event_name = "RecoveryWalStream";
			break;
		case WAIT_EVENT_SYSLOGGER_MAIN:
			event_name = "SysLoggerMain";
			break;
		case WAIT_EVENT_WAL_RECEIVER_MAIN:
			event_name = "WalReceiverMain";
			break;
		case WAIT_EVENT_WAL_SENDER_MAIN:
			event_name = "WalSenderMain";
			break;
		case WAIT_EVENT_WAL_WRITER_MAIN:
			event_name = "WalWriterMain";
			break;
			/* no default case, so that compiler will warn */
	}

	return event_name;
}

/* ----------
 * pgstat_get_wait_client() -
 *
 * Convert WaitEventClient to string.
 * ----------
 */
static const char *
pgstat_get_wait_client(WaitEventClient w)
{
	const char *event_name = "unknown wait event";

	switch (w)
	{
		case WAIT_EVENT_CLIENT_READ:
			event_name = "ClientRead";
			break;
		case WAIT_EVENT_CLIENT_WRITE:
			event_name = "ClientWrite";
			break;
		case WAIT_EVENT_GSS_OPEN_SERVER:
			event_name = "GSSOpenServer";
			break;
		case WAIT_EVENT_LIBPQWALRECEIVER_CONNECT:
			event_name = "LibPQWalReceiverConnect";
			break;
		case WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE:
			event_name = "LibPQWalReceiverReceive";
			break;
		case WAIT_EVENT_SSL_OPEN_SERVER:
			event_name = "SSLOpenServer";
			break;
		case WAIT_EVENT_WAL_RECEIVER_WAIT_START:
			event_name = "WalReceiverWaitStart";
			break;
		case WAIT_EVENT_WAL_SENDER_WAIT_WAL:
			event_name = "WalSenderWaitForWAL";
			break;
		case WAIT_EVENT_WAL_SENDER_WRITE_DATA:
			event_name = "WalSenderWriteData";
			break;
			/* no default case, so that compiler will warn */
	}

	return event_name;
}

/* ----------
 * pgstat_get_wait_ipc() -
 *
 * Convert WaitEventIPC to string.
 * ----------
 */
static const char *
pgstat_get_wait_ipc(WaitEventIPC w)
{
	const char *event_name = "unknown wait event";

	switch (w)
	{
		case WAIT_EVENT_BACKUP_WAIT_WAL_ARCHIVE:
			event_name = "BackupWaitWalArchive";
			break;
		case WAIT_EVENT_BGWORKER_SHUTDOWN:
			event_name = "BgWorkerShutdown";
			break;
		case WAIT_EVENT_BGWORKER_STARTUP:
			event_name = "BgWorkerStartup";
			break;
		case WAIT_EVENT_BTREE_PAGE:
			event_name = "BtreePage";
			break;
		case WAIT_EVENT_CHECKPOINT_DONE:
			event_name = "CheckpointDone";
			break;
		case WAIT_EVENT_CHECKPOINT_START:
			event_name = "CheckpointStart";
			break;
		case WAIT_EVENT_CLOG_GROUP_UPDATE:
			event_name = "ClogGroupUpdate";
			break;
		case WAIT_EVENT_EXECUTE_GATHER:
			event_name = "ExecuteGather";
			break;
		case WAIT_EVENT_HASH_BATCH_ALLOCATING:
			event_name = "Hash/Batch/Allocating";
			break;
		case WAIT_EVENT_HASH_BATCH_ELECTING:
			event_name = "Hash/Batch/Electing";
			break;
		case WAIT_EVENT_HASH_BATCH_LOADING:
			event_name = "Hash/Batch/Loading";
			break;
		case WAIT_EVENT_HASH_BUILD_ALLOCATING:
			event_name = "Hash/Build/Allocating";
			break;
		case WAIT_EVENT_HASH_BUILD_ELECTING:
			event_name = "Hash/Build/Electing";
			break;
		case WAIT_EVENT_HASH_BUILD_HASHING_INNER:
			event_name = "Hash/Build/HashingInner";
			break;
		case WAIT_EVENT_HASH_BUILD_HASHING_OUTER:
			event_name = "Hash/Build/HashingOuter";
			break;
		case WAIT_EVENT_HASH_GROW_BATCHES_ALLOCATING:
			event_name = "Hash/GrowBatches/Allocating";
			break;
		case WAIT_EVENT_HASH_GROW_BATCHES_DECIDING:
			event_name = "Hash/GrowBatches/Deciding";
			break;
		case WAIT_EVENT_HASH_GROW_BATCHES_ELECTING:
			event_name = "Hash/GrowBatches/Electing";
			break;
		case WAIT_EVENT_HASH_GROW_BATCHES_FINISHING:
			event_name = "Hash/GrowBatches/Finishing";
			break;
		case WAIT_EVENT_HASH_GROW_BATCHES_REPARTITIONING:
			event_name = "Hash/GrowBatches/Repartitioning";
			break;
		case WAIT_EVENT_HASH_GROW_BUCKETS_ALLOCATING:
			event_name = "Hash/GrowBuckets/Allocating";
			break;
		case WAIT_EVENT_HASH_GROW_BUCKETS_ELECTING:
			event_name = "Hash/GrowBuckets/Electing";
			break;
		case WAIT_EVENT_HASH_GROW_BUCKETS_REINSERTING:
			event_name = "Hash/GrowBuckets/Reinserting";
			break;
		case WAIT_EVENT_LOGICAL_SYNC_DATA:
			event_name = "LogicalSyncData";
			break;
		case WAIT_EVENT_LOGICAL_SYNC_STATE_CHANGE:
			event_name = "LogicalSyncStateChange";
			break;
		case WAIT_EVENT_MQ_INTERNAL:
			event_name = "MessageQueueInternal";
			break;
		case WAIT_EVENT_MQ_PUT_MESSAGE:
			event_name = "MessageQueuePutMessage";
			break;
		case WAIT_EVENT_MQ_RECEIVE:
			event_name = "MessageQueueReceive";
			break;
		case WAIT_EVENT_MQ_SEND:
			event_name = "MessageQueueSend";
			break;
		case WAIT_EVENT_PARALLEL_BITMAP_SCAN:
			event_name = "ParallelBitmapScan";
			break;
		case WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN:
			event_name = "ParallelCreateIndexScan";
			break;
		case WAIT_EVENT_PARALLEL_FINISH:
			event_name = "ParallelFinish";
			break;
		case WAIT_EVENT_PROCARRAY_GROUP_UPDATE:
			event_name = "ProcArrayGroupUpdate";
			break;
		case WAIT_EVENT_PROMOTE:
			event_name = "Promote";
			break;
		case WAIT_EVENT_RECOVERY_PAUSE:
			event_name = "RecoveryPause";
			break;
		case WAIT_EVENT_REPLICATION_ORIGIN_DROP:
			event_name = "ReplicationOriginDrop";
			break;
		case WAIT_EVENT_REPLICATION_SLOT_DROP:
			event_name = "ReplicationSlotDrop";
			break;
		case WAIT_EVENT_SAFE_SNAPSHOT:
			event_name = "SafeSnapshot";
			break;
		case WAIT_EVENT_SYNC_REP:
			event_name = "SyncRep";
			break;
			/* no default case, so that compiler will warn */
	}

	return event_name;
}

/* ----------
 * pgstat_get_wait_timeout() -
 *
 * Convert WaitEventTimeout to string.
 * ----------
 */
static const char *
pgstat_get_wait_timeout(WaitEventTimeout w)
{
	const char *event_name = "unknown wait event";

	switch (w)
	{
		case WAIT_EVENT_BASE_BACKUP_THROTTLE:
			event_name = "BaseBackupThrottle";
			break;
		case WAIT_EVENT_PG_SLEEP:
			event_name = "PgSleep";
			break;
		case WAIT_EVENT_RECOVERY_APPLY_DELAY:
			event_name = "RecoveryApplyDelay";
			break;
		case WAIT_EVENT_RECOVERY_RETRIEVE_RETRY_INTERVAL:
			event_name = "RecoveryRetrieveRetryInterval";
			break;
		case WAIT_EVENT_VACUUM_DELAY:
			event_name = "VacuumDelay";
			break;
			/* no default case, so that compiler will warn */
	}

	return event_name;
}

/* ----------
 * pgstat_get_wait_io() -
 *
 * Convert WaitEventIO to string.
 * ----------
 */
static const char *
pgstat_get_wait_io(WaitEventIO w)
{
	const char *event_name = "unknown wait event";

	switch (w)
	{
		case WAIT_EVENT_BUFFILE_READ:
			event_name = "BufFileRead";
			break;
		case WAIT_EVENT_BUFFILE_WRITE:
			event_name = "BufFileWrite";
			break;
		case WAIT_EVENT_CONTROL_FILE_READ:
			event_name = "ControlFileRead";
			break;
		case WAIT_EVENT_CONTROL_FILE_SYNC:
			event_name = "ControlFileSync";
			break;
		case WAIT_EVENT_CONTROL_FILE_SYNC_UPDATE:
			event_name = "ControlFileSyncUpdate";
			break;
		case WAIT_EVENT_CONTROL_FILE_WRITE:
			event_name = "ControlFileWrite";
			break;
		case WAIT_EVENT_CONTROL_FILE_WRITE_UPDATE:
			event_name = "ControlFileWriteUpdate";
			break;
		case WAIT_EVENT_COPY_FILE_READ:
			event_name = "CopyFileRead";
			break;
		case WAIT_EVENT_COPY_FILE_WRITE:
			event_name = "CopyFileWrite";
			break;
		case WAIT_EVENT_DATA_FILE_EXTEND:
			event_name = "DataFileExtend";
			break;
		case WAIT_EVENT_DATA_FILE_FLUSH:
			event_name = "DataFileFlush";
			break;
		case WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC:
			event_name = "DataFileImmediateSync";
			break;
		case WAIT_EVENT_DATA_FILE_PREFETCH:
			event_name = "DataFilePrefetch";
			break;
		case WAIT_EVENT_DATA_FILE_READ:
			event_name = "DataFileRead";
			break;
		case WAIT_EVENT_DATA_FILE_SYNC:
			event_name = "DataFileSync";
			break;
		case WAIT_EVENT_DATA_FILE_TRUNCATE:
			event_name = "DataFileTruncate";
			break;
		case WAIT_EVENT_DATA_FILE_WRITE:
			event_name = "DataFileWrite";
			break;
		case WAIT_EVENT_DSM_FILL_ZERO_WRITE:
			event_name = "DSMFillZeroWrite";
			break;
		case WAIT_EVENT_LOCK_FILE_ADDTODATADIR_READ:
			event_name = "LockFileAddToDataDirRead";
			break;
		case WAIT_EVENT_LOCK_FILE_ADDTODATADIR_SYNC:
			event_name = "LockFileAddToDataDirSync";
			break;
		case WAIT_EVENT_LOCK_FILE_ADDTODATADIR_WRITE:
			event_name = "LockFileAddToDataDirWrite";
			break;
		case WAIT_EVENT_LOCK_FILE_CREATE_READ:
			event_name = "LockFileCreateRead";
			break;
		case WAIT_EVENT_LOCK_FILE_CREATE_SYNC:
			event_name = "LockFileCreateSync";
			break;
		case WAIT_EVENT_LOCK_FILE_CREATE_WRITE:
			event_name = "LockFileCreateWrite";
			break;
		case WAIT_EVENT_LOCK_FILE_RECHECKDATADIR_READ:
			event_name = "LockFileReCheckDataDirRead";
			break;
		case WAIT_EVENT_LOGICAL_REWRITE_CHECKPOINT_SYNC:
			event_name = "LogicalRewriteCheckpointSync";
			break;
		case WAIT_EVENT_LOGICAL_REWRITE_MAPPING_SYNC:
			event_name = "LogicalRewriteMappingSync";
			break;
		case WAIT_EVENT_LOGICAL_REWRITE_MAPPING_WRITE:
			event_name = "LogicalRewriteMappingWrite";
			break;
		case WAIT_EVENT_LOGICAL_REWRITE_SYNC:
			event_name = "LogicalRewriteSync";
			break;
		case WAIT_EVENT_LOGICAL_REWRITE_TRUNCATE:
			event_name = "LogicalRewriteTruncate";
			break;
		case WAIT_EVENT_LOGICAL_REWRITE_WRITE:
			event_name = "LogicalRewriteWrite";
			break;
		case WAIT_EVENT_PROC_SIGNAL_BARRIER:
			event_name = "ProcSignalBarrier";
			break;
		case WAIT_EVENT_RELATION_MAP_READ:
			event_name = "RelationMapRead";
			break;
		case WAIT_EVENT_RELATION_MAP_SYNC:
			event_name = "RelationMapSync";
			break;
		case WAIT_EVENT_RELATION_MAP_WRITE:
			event_name = "RelationMapWrite";
			break;
		case WAIT_EVENT_REORDER_BUFFER_READ:
			event_name = "ReorderBufferRead";
			break;
		case WAIT_EVENT_REORDER_BUFFER_WRITE:
			event_name = "ReorderBufferWrite";
			break;
		case WAIT_EVENT_REORDER_LOGICAL_MAPPING_READ:
			event_name = "ReorderLogicalMappingRead";
			break;
		case WAIT_EVENT_REPLICATION_SLOT_READ:
			event_name = "ReplicationSlotRead";
			break;
		case WAIT_EVENT_REPLICATION_SLOT_RESTORE_SYNC:
			event_name = "ReplicationSlotRestoreSync";
			break;
		case WAIT_EVENT_REPLICATION_SLOT_SYNC:
			event_name = "ReplicationSlotSync";
			break;
		case WAIT_EVENT_REPLICATION_SLOT_WRITE:
			event_name = "ReplicationSlotWrite";
			break;
		case WAIT_EVENT_SLRU_FLUSH_SYNC:
			event_name = "SLRUFlushSync";
			break;
		case WAIT_EVENT_SLRU_READ:
			event_name = "SLRURead";
			break;
		case WAIT_EVENT_SLRU_SYNC:
			event_name = "SLRUSync";
			break;
		case WAIT_EVENT_SLRU_WRITE:
			event_name = "SLRUWrite";
			break;
		case WAIT_EVENT_SNAPBUILD_READ:
			event_name = "SnapbuildRead";
			break;
		case WAIT_EVENT_SNAPBUILD_SYNC:
			event_name = "SnapbuildSync";
			break;
		case WAIT_EVENT_SNAPBUILD_WRITE:
			event_name = "SnapbuildWrite";
			break;
		case WAIT_EVENT_TIMELINE_HISTORY_FILE_SYNC:
			event_name = "TimelineHistoryFileSync";
			break;
		case WAIT_EVENT_TIMELINE_HISTORY_FILE_WRITE:
			event_name = "TimelineHistoryFileWrite";
			break;
		case WAIT_EVENT_TIMELINE_HISTORY_READ:
			event_name = "TimelineHistoryRead";
			break;
		case WAIT_EVENT_TIMELINE_HISTORY_SYNC:
			event_name = "TimelineHistorySync";
			break;
		case WAIT_EVENT_TIMELINE_HISTORY_WRITE:
			event_name = "TimelineHistoryWrite";
			break;
		case WAIT_EVENT_TWOPHASE_FILE_READ:
			event_name = "TwophaseFileRead";
			break;
		case WAIT_EVENT_TWOPHASE_FILE_SYNC:
			event_name = "TwophaseFileSync";
			break;
		case WAIT_EVENT_TWOPHASE_FILE_WRITE:
			event_name = "TwophaseFileWrite";
			break;
		case WAIT_EVENT_WALSENDER_TIMELINE_HISTORY_READ:
			event_name = "WALSenderTimelineHistoryRead";
			break;
		case WAIT_EVENT_WAL_BOOTSTRAP_SYNC:
			event_name = "WALBootstrapSync";
			break;
		case WAIT_EVENT_WAL_BOOTSTRAP_WRITE:
			event_name = "WALBootstrapWrite";
			break;
		case WAIT_EVENT_WAL_COPY_READ:
			event_name = "WALCopyRead";
			break;
		case WAIT_EVENT_WAL_COPY_SYNC:
			event_name = "WALCopySync";
			break;
		case WAIT_EVENT_WAL_COPY_WRITE:
			event_name = "WALCopyWrite";
			break;
		case WAIT_EVENT_WAL_INIT_SYNC:
			event_name = "WALInitSync";
			break;
		case WAIT_EVENT_WAL_INIT_WRITE:
			event_name = "WALInitWrite";
			break;
		case WAIT_EVENT_WAL_READ:
			event_name = "WALRead";
			break;
		case WAIT_EVENT_WAL_SYNC:
			event_name = "WALSync";
			break;
		case WAIT_EVENT_WAL_SYNC_METHOD_ASSIGN:
			event_name = "WALSyncMethodAssign";
			break;
		case WAIT_EVENT_WAL_WRITE:
			event_name = "WALWrite";
			break;

			/* no default case, so that compiler will warn */
	}

	return event_name;
}


/* ----------
 * pgstat_get_backend_current_activity() -
 *
 *	Return a string representing the current activity of the backend with
 *	the specified PID.  This looks directly at the BackendStatusArray,
 *	and so will provide current information regardless of the age of our
 *	transaction's snapshot of the status array.
 *
 *	It is the caller's responsibility to invoke this only for backends whose
 *	state is expected to remain stable while the result is in use.  The
 *	only current use is in deadlock reporting, where we can expect that
 *	the target backend is blocked on a lock.  (There are corner cases
 *	where the target's wait could get aborted while we are looking at it,
 *	but the very worst consequence is to return a pointer to a string
 *	that's been changed, so we won't worry too much.)
 *
 *	Note: return strings for special cases match pg_stat_get_backend_activity.
 * ----------
 */
const char *
pgstat_get_backend_current_activity(int pid, bool checkUser)
{
	PgBackendStatus *beentry;
	int			i;

	beentry = BackendStatusArray;
	for (i = 1; i <= MaxBackends; i++)
	{
		/*
		 * Although we expect the target backend's entry to be stable, that
		 * doesn't imply that anyone else's is.  To avoid identifying the
		 * wrong backend, while we check for a match to the desired PID we
		 * must follow the protocol of retrying if st_changecount changes
		 * while we examine the entry, or if it's odd.  (This might be
		 * unnecessary, since fetching or storing an int is almost certainly
		 * atomic, but let's play it safe.)  We use a volatile pointer here to
		 * ensure the compiler doesn't try to get cute.
		 */
		volatile PgBackendStatus *vbeentry = beentry;
		bool		found;

		for (;;)
		{
			int			before_changecount;
			int			after_changecount;

			pgstat_begin_read_activity(vbeentry, before_changecount);

			found = (vbeentry->st_procpid == pid);

			pgstat_end_read_activity(vbeentry, after_changecount);

			if (pgstat_read_activity_complete(before_changecount,
											  after_changecount))
				break;

			/* Make sure we can break out of loop if stuck... */
			CHECK_FOR_INTERRUPTS();
		}

		if (found)
		{
			/* Now it is safe to use the non-volatile pointer */
			if (checkUser && !superuser() && beentry->st_userid != GetUserId())
				return "<insufficient privilege>";
			else if (*(beentry->st_activity_raw) == '\0')
				return "<command string not enabled>";
			else
			{
				/* this'll leak a bit of memory, but that seems acceptable */
				return pgstat_clip_activity(beentry->st_activity_raw);
			}
		}

		beentry++;
	}

	/* If we get here, caller is in error ... */
	return "<backend information not available>";
}

/* ----------
 * pgstat_get_crashed_backend_activity() -
 *
 *	Return a string representing the current activity of the backend with
 *	the specified PID.  Like the function above, but reads shared memory with
 *	the expectation that it may be corrupt.  On success, copy the string
 *	into the "buffer" argument and return that pointer.  On failure,
 *	return NULL.
 *
 *	This function is only intended to be used by the postmaster to report the
 *	query that crashed a backend.  In particular, no attempt is made to
 *	follow the correct concurrency protocol when accessing the
 *	BackendStatusArray.  But that's OK, in the worst case we'll return a
 *	corrupted message.  We also must take care not to trip on ereport(ERROR).
 * ----------
 */
const char *
pgstat_get_crashed_backend_activity(int pid, char *buffer, int buflen)
{
	volatile PgBackendStatus *beentry;
	int			i;

	beentry = BackendStatusArray;

	/*
	 * We probably shouldn't get here before shared memory has been set up,
	 * but be safe.
	 */
	if (beentry == NULL || BackendActivityBuffer == NULL)
		return NULL;

	for (i = 1; i <= MaxBackends; i++)
	{
		if (beentry->st_procpid == pid)
		{
			/* Read pointer just once, so it can't change after validation */
			const char *activity = beentry->st_activity_raw;
			const char *activity_last;

			/*
			 * We mustn't access activity string before we verify that it
			 * falls within the BackendActivityBuffer. To make sure that the
			 * entire string including its ending is contained within the
			 * buffer, subtract one activity length from the buffer size.
			 */
			activity_last = BackendActivityBuffer + BackendActivityBufferSize
				- pgstat_track_activity_query_size;

			if (activity < BackendActivityBuffer ||
				activity > activity_last)
				return NULL;

			/* If no string available, no point in a report */
			if (activity[0] == '\0')
				return NULL;

			/*
			 * Copy only ASCII-safe characters so we don't run into encoding
			 * problems when reporting the message; and be sure not to run off
			 * the end of memory.  As only ASCII characters are reported, it
			 * doesn't seem necessary to perform multibyte aware clipping.
			 */
			ascii_safe_strlcpy(buffer, activity,
							   Min(buflen, pgstat_track_activity_query_size));

			return buffer;
		}

		beentry++;
	}

	/* PID not found */
	return NULL;
}

/* ------------------------------------------------------------
 * Local support functions follow
 * ------------------------------------------------------------
 */


/* ----------
 * pgstat_report_archiver() -
 *
 *		Report archiver statistics
 * ----------
 */
void
pgstat_report_archiver(const char *xlog, bool failed)
{
	TimestampTz now = GetCurrentTimestamp();

	if (failed)
	{
		/* Failed archival attempt */
		LWLockAcquire(StatsLock, LW_EXCLUSIVE);
		++shared_archiverStats->failed_count;
		memcpy(shared_archiverStats->last_failed_wal, xlog,
			   sizeof(shared_archiverStats->last_failed_wal));
		shared_archiverStats->last_failed_timestamp = now;
		LWLockRelease(StatsLock);
	}
	else
	{
		/* Successful archival operation */
		LWLockAcquire(StatsLock, LW_EXCLUSIVE);
		++shared_archiverStats->archived_count;
		memcpy(shared_archiverStats->last_archived_wal, xlog,
			   sizeof(shared_archiverStats->last_archived_wal));
		shared_archiverStats->last_archived_timestamp = now;
		LWLockRelease(StatsLock);
	}
}

/* ----------
 * pgstat_report_bgwriter() -
 *
 *		Report bgwriter statistics
 * ----------
 */
void
pgstat_report_bgwriter(void)
{
	/* We assume this initializes to zeroes */
	static const PgStat_BgWriter all_zeroes;

	PgStat_BgWriter *l = &BgWriterStats;

	/*
	 * This function can be called even if nothing at all has happened. In
	 * this case, avoid taking lock for a completely empty stats.
	 */
	if (memcmp(&BgWriterStats, &all_zeroes, sizeof(PgStat_BgWriter)) == 0)
		return;

	LWLockAcquire(StatsLock, LW_EXCLUSIVE);
	shared_globalStats->timed_checkpoints += l->timed_checkpoints;
	shared_globalStats->requested_checkpoints += l->requested_checkpoints;
	shared_globalStats->checkpoint_write_time += l->checkpoint_write_time;
	shared_globalStats->checkpoint_sync_time += l->checkpoint_sync_time;
	shared_globalStats->buf_written_checkpoints += l->buf_written_checkpoints;
	shared_globalStats->buf_written_clean += l->buf_written_clean;
	shared_globalStats->maxwritten_clean += l->maxwritten_clean;
	shared_globalStats->buf_written_backend += l->buf_written_backend;
	shared_globalStats->buf_fsync_backend += l->buf_fsync_backend;
	shared_globalStats->buf_alloc += l->buf_alloc;
	LWLockRelease(StatsLock);

	/*
	 * Clear out the statistics buffer, so it can be re-used.
	 */
	MemSet(&BgWriterStats, 0, sizeof(BgWriterStats));
}


/* ----------
 * pgstat_write_statsfiles() -
 *		Write the global statistics file, as well as DB files.
 * ----------
 */
void
pgstat_write_statsfiles(void)
{
	FILE	   *fpout;
	int32		format_id;
	const char *tmpfile = PGSTAT_STAT_PERMANENT_TMPFILE;
	const char *statfile = PGSTAT_STAT_PERMANENT_FILENAME;
	int			rc;
	PgStatEnvelope **envlist;
	PgStatEnvelope **penv;

	/* stats is not initialized yet. just return. */
	if (StatsShmem->stats_dsa_handle == DSM_HANDLE_INVALID)
		return;

	elog(DEBUG2, "writing stats file \"%s\"", statfile);

	create_missing_dbentries();

	/*
	 * Open the statistics temp file to write out the current values.
	 */
	fpout = AllocateFile(tmpfile, PG_BINARY_W);
	if (fpout == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not open temporary statistics file \"%s\": %m",
						tmpfile)));
		return;
	}

	/*
	 * Set the timestamp of the stats file.
	 */
	shared_globalStats->stats_timestamp = GetCurrentTimestamp();

	/*
	 * Write the file header --- currently just a format ID.
	 */
	format_id = PGSTAT_FILE_FORMAT_ID;
	rc = fwrite(&format_id, sizeof(format_id), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Write global stats struct
	 */
	rc = fwrite(shared_globalStats, sizeof(*shared_globalStats), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Write archiver stats struct
	 */
	rc = fwrite(shared_archiverStats, sizeof(*shared_archiverStats), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Walk through the database table.
	 */
	envlist = collect_stat_entries(PGSTAT_TYPE_DB, InvalidOid);
	for (penv = envlist; *penv != NULL; penv++)
	{
		PgStat_StatDBEntry *dbentry = (PgStat_StatDBEntry *) &(*penv)->body;

		/*
		 * Write out the table and function stats for this DB into the
		 * appropriate per-DB stat file, if required.
		 */
		/* Make DB's timestamp consistent with the global stats */
		dbentry->stats_timestamp = shared_globalStats->stats_timestamp;

		pgstat_write_database_stats(dbentry);

		/*
		 * Write out the DB entry. We don't write the tables or functions
		 * pointers, since they're of no use to any other process.
		 */
		fputc('D', fpout);
		rc = fwrite(dbentry, offsetof(PgStat_StatDBEntry, tables), 1, fpout);
		(void) rc;				/* we'll check for error with ferror */
	}

	pfree(envlist);

	/*
	 * No more output to be done. Close the temp file and replace the old
	 * pgstat.stat with it.  The ferror() check replaces testing for error
	 * after each individual fputc or fwrite above.
	 */
	fputc('E', fpout);

	if (ferror(fpout))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write temporary statistics file \"%s\": %m",
						tmpfile)));
		FreeFile(fpout);
		unlink(tmpfile);
	}
	else if (FreeFile(fpout) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not close temporary statistics file \"%s\": %m",
						tmpfile)));
		unlink(tmpfile);
	}
	else if (rename(tmpfile, statfile) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename temporary statistics file \"%s\" to \"%s\": %m",
						tmpfile, statfile)));
		unlink(tmpfile);
	}
}


/* ----------
 * pgstat_write_database_stats() -
 *  Write the stat file for a single database.
 * ----------
 */
static void
pgstat_write_database_stats(PgStat_StatDBEntry *dbentry)
{
	PgStatEnvelope **envlist;
	PgStatEnvelope **penv;
	FILE	   *fpout;
	int32		format_id;
	Oid			dbid = dbentry->databaseid;
	int			rc;
	char		tmpfile[MAXPGPATH];
	char		statfile[MAXPGPATH];

	get_dbstat_filename(true, dbid, tmpfile, MAXPGPATH);
	get_dbstat_filename(false, dbid, statfile, MAXPGPATH);

	elog(DEBUG2, "writing stats file \"%s\"", statfile);

	/*
	 * Open the statistics temp file to write out the current values.
	 */
	fpout = AllocateFile(tmpfile, PG_BINARY_W);
	if (fpout == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not open temporary statistics file \"%s\": %m",
						tmpfile)));
		return;
	}

	/*
	 * Write the file header --- currently just a format ID.
	 */
	format_id = PGSTAT_FILE_FORMAT_ID;
	rc = fwrite(&format_id, sizeof(format_id), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Walk through the database's access stats per table.
	 */
	envlist = collect_stat_entries(PGSTAT_TYPE_TABLE, dbentry->databaseid);
	for (penv = envlist; *penv != NULL; penv++)
	{
		PgStat_StatTabEntry *tabentry = (PgStat_StatTabEntry *) &(*penv)->body;

		fputc('T', fpout);
		rc = fwrite(tabentry, sizeof(PgStat_StatTabEntry), 1, fpout);
		(void) rc;				/* we'll check for error with ferror */
	}
	pfree(envlist);

	/*
	 * Walk through the database's function stats table.
	 */
	envlist = collect_stat_entries(PGSTAT_TYPE_FUNCTION, dbentry->databaseid);
	for (penv = envlist; *penv != NULL; penv++)
	{
		PgStat_StatFuncEntry *funcentry =
		(PgStat_StatFuncEntry *) &(*penv)->body;

		fputc('F', fpout);
		rc = fwrite(funcentry, sizeof(PgStat_StatFuncEntry), 1, fpout);
		(void) rc;				/* we'll check for error with ferror */
	}
	pfree(envlist);

	/*
	 * No more output to be done. Close the temp file and replace the old
	 * pgstat.stat with it.  The ferror() check replaces testing for error
	 * after each individual fputc or fwrite above.
	 */
	fputc('E', fpout);

	if (ferror(fpout))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write temporary statistics file \"%s\": %m",
						tmpfile)));
		FreeFile(fpout);
		unlink(tmpfile);
	}
	else if (FreeFile(fpout) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not close temporary statistics file \"%s\": %m",
						tmpfile)));
		unlink(tmpfile);
	}
	else if (rename(tmpfile, statfile) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename temporary statistics file \"%s\" to \"%s\": %m",
						tmpfile, statfile)));
		unlink(tmpfile);
	}
}

/* ----------
 * create_missing_dbentries() -
 *
 *  There may be the case where database entry is missing for the database
 *  where object stats are recorded. This function creates such missing
 *  dbentries so that so that all stats entries can be written out to files.
 * ----------
 */
static void
create_missing_dbentries(void)
{
	dshash_seq_status hstat;
	PgStatHashEntry *p;
	HTAB	   *oidhash;
	HASHCTL		ctl;
	HASH_SEQ_STATUS scan;
	Oid		   *poid;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(Oid);
	ctl.hcxt = CurrentMemoryContext;
	oidhash = hash_create("Temporary table of OIDs",
						  PGSTAT_TABLE_HASH_SIZE,
						  &ctl,
						  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* Collect OID from the shared stats hash */
	dshash_seq_init(&hstat, pgStatSharedHash, false);
	while ((p = dshash_seq_next(&hstat)) != NULL)
		hash_search(oidhash, &p->key.databaseid, HASH_ENTER, NULL);
	dshash_seq_term(&hstat);

	/* Create missing database entries if not exists. */
	hash_seq_init(&scan, oidhash);
	while ((poid = (Oid *) hash_seq_search(&scan)) != NULL)
		(void) get_stat_entry(PGSTAT_TYPE_DB, *poid, InvalidOid,
							  false, init_dbentry, NULL);

	hash_destroy(oidhash);
}


/* ----------
 * get_stat_entry() -
 *
 *	get shared stats entry for specified type, dbid and objid.
 *  If nowait is true, returns NULL on lock failure.
 *
 *  If initfunc is not NULL, new entry is created if not yet and the function
 *  is called with the new envelope. If found is not NULL, it is set to true if
 *  existing entry is found or false if not.
 * ----------
 */
static PgStatEnvelope *
get_stat_entry(PgStatTypes type, Oid dbid, Oid objid,
			   bool nowait, entry_initializer initfunc, bool *found)
{
	bool		create = (initfunc != NULL);
	PgStatHashEntry *shent;
	PgStatEnvelope *shenv = NULL;
	PgStatHashEntryKey key;
	bool		myfound;

	Assert(type != PGSTAT_TYPE_ALL);

	key.type = type;
	key.databaseid = dbid;
	key.objectid = objid;
	shent = dshash_find_extended(pgStatSharedHash, &key,
								 create, nowait, create, &myfound);
	if (shent)
	{
		if (create && !myfound)
		{
			/* Create new stats envelope. */
			size_t		envsize = PgStatEnvelopeSize(pgstat_entsize[type]);
			dsa_pointer chunk = dsa_allocate0(area, envsize);

			shenv = dsa_get_address(area, chunk);
			shenv->type = type;
			shenv->databaseid = dbid;
			shenv->objectid = objid;
			shenv->len = pgstat_entsize[type];
			LWLockInitialize(&shenv->lock, LWTRANCHE_STATS);

			/*
			 * The lock on dshsh is released just after. Call initializer
			 * callback before it is exposed to other process.
			 */
			if (initfunc)
				initfunc(shenv);

			/* Link the new entry from the hash entry. */
			shent->env = chunk;
		}
		else
			shenv = dsa_get_address(area, shent->env);

		dshash_release_lock(pgStatSharedHash, shent);
	}

	if (found)
		*found = myfound;

	return shenv;
}


/* ----------
 * pgstat_read_statsfiles() -
 *
 *	Reads in existing activity statistics files into the shared stats hash.
 *
 * ----------
 */
void
pgstat_read_statsfiles(void)
{
	PgStatEnvelope *env;
	PgStat_StatDBEntry *dbentry;
	PgStat_StatDBEntry dbbuf;
	FILE	   *fpin;
	int32		format_id;
	bool		found;
	const char *statfile = PGSTAT_STAT_PERMANENT_FILENAME;

	/* shouldn't be called from postmaster */
	Assert(IsUnderPostmaster);

	elog(DEBUG2, "reading stats file \"%s\"", statfile);

	/*
	 * Set the current timestamp (will be kept only in case we can't load an
	 * existing statsfile).
	 */
	shared_globalStats->stat_reset_timestamp = GetCurrentTimestamp();
	shared_archiverStats->stat_reset_timestamp =
		shared_globalStats->stat_reset_timestamp;

	/*
	 * Try to open the stats file. If it doesn't exist, the backends simply
	 * returns zero for anything and the activity statistics simply starts
	 * from scratch with empty counters.
	 *
	 * ENOENT is a possibility if the activity statistics is not running or
	 * has not yet written the stats file the first time.  Any other failure
	 * condition is suspicious.
	 */
	if ((fpin = AllocateFile(statfile, PG_BINARY_R)) == NULL)
	{
		if (errno != ENOENT)
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not open statistics file \"%s\": %m",
							statfile)));
		return;
	}

	/*
	 * Verify it's of the expected format.
	 */
	if (fread(&format_id, 1, sizeof(format_id), fpin) != sizeof(format_id) ||
		format_id != PGSTAT_FILE_FORMAT_ID)
	{
		ereport(LOG,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		goto done;
	}

	/*
	 * Read global stats struct
	 */
	if (fread(shared_globalStats, 1, sizeof(*shared_globalStats), fpin) !=
		sizeof(*shared_globalStats))
	{
		ereport(LOG,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		MemSet(shared_globalStats, 0, sizeof(*shared_globalStats));
		goto done;
	}

	/*
	 * Read archiver stats struct
	 */
	if (fread(shared_archiverStats, 1, sizeof(*shared_archiverStats), fpin) !=
		sizeof(*shared_archiverStats))
	{
		ereport(LOG,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		MemSet(shared_archiverStats, 0, sizeof(*shared_archiverStats));
		goto done;
	}

	/*
	 * We found an existing activity statistics file. Read it and put all the
	 * hash table entries into place.
	 */
	for (;;)
	{
		switch (fgetc(fpin))
		{
				/*
				 * 'D'	A PgStat_StatDBEntry struct describing a database
				 * follows.
				 */
			case 'D':
				if (fread(&dbbuf, 1, offsetof(PgStat_StatDBEntry, tables),
						  fpin) != offsetof(PgStat_StatDBEntry, tables))
				{
					ereport(LOG,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				/*
				 * Add to the DB hash
				 */

				env = get_stat_entry(PGSTAT_TYPE_DB, dbbuf.databaseid,
									 InvalidOid,
									 false, init_dbentry, &found);
				dbentry = (PgStat_StatDBEntry *) &env->body;

				/* don't allow duplicate dbentries */
				if (found)
				{
					ereport(LOG,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				memcpy(dbentry, &dbbuf,
					   offsetof(PgStat_StatDBEntry, tables));

				/* Read the data from the database-specific file. */
				pgstat_read_db_statsfile(dbentry);
				break;

			case 'E':
				goto done;

			default:
				ereport(LOG,
						(errmsg("corrupted statistics file \"%s\"",
								statfile)));
				goto done;
		}
	}

done:
	FreeFile(fpin);

	elog(DEBUG2, "removing permanent stats file \"%s\"", statfile);
	unlink(statfile);

	return;
}


/* ----------
 * pgstat_read_db_statsfile() -
 *
 *	Reads in the at-rest statistics file and create shared statistics
 *	tables. The file is removed after reading.
 * ----------
 */
static void
pgstat_read_db_statsfile(PgStat_StatDBEntry *dbentry)
{
	PgStatEnvelope *env;
	PgStat_StatTabEntry *tabentry;
	PgStat_StatTabEntry tabbuf;
	PgStat_StatFuncEntry funcbuf;
	PgStat_StatFuncEntry *funcentry;
	dshash_table *tabhash = NULL;
	dshash_table *funchash = NULL;
	FILE	   *fpin;
	int32		format_id;
	bool		found;
	char		statfile[MAXPGPATH];

	get_dbstat_filename(false, dbentry->databaseid, statfile, MAXPGPATH);

	/*
	 * Try to open the stats file. If it doesn't exist, the backends simply
	 * returns zero for anything and the activity statistics simply starts
	 * from scratch with empty counters.
	 *
	 * ENOENT is a possibility if the activity statistics is not running or
	 * has not yet written the stats file the first time.  Any other failure
	 * condition is suspicious.
	 */
	if ((fpin = AllocateFile(statfile, PG_BINARY_R)) == NULL)
	{
		if (errno != ENOENT)
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not open statistics file \"%s\": %m",
							statfile)));
		return;
	}

	/*
	 * Verify it's of the expected format.
	 */
	if (fread(&format_id, 1, sizeof(format_id), fpin) != sizeof(format_id) ||
		format_id != PGSTAT_FILE_FORMAT_ID)
	{
		ereport(LOG,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		goto done;
	}

	/*
	 * We found an existing activity statistics file. Read it and put all the
	 * hash table entries into place.
	 */
	for (;;)
	{
		switch (fgetc(fpin))
		{
				/*
				 * 'T'	A PgStat_StatTabEntry follows.
				 */
			case 'T':
				if (fread(&tabbuf, 1, sizeof(PgStat_StatTabEntry),
						  fpin) != sizeof(PgStat_StatTabEntry))
				{
					ereport(LOG,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				env = get_stat_entry(PGSTAT_TYPE_TABLE,
									 dbentry->databaseid, tabbuf.tableid,
									 false, init_tabentry, &found);
				tabentry = (PgStat_StatTabEntry *) &env->body;

				/* don't allow duplicate entries */
				if (found)
				{
					ereport(LOG,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				memcpy(tabentry, &tabbuf, sizeof(tabbuf));
				break;

				/*
				 * 'F'	A PgStat_StatFuncEntry follows.
				 */
			case 'F':
				if (fread(&funcbuf, 1, sizeof(PgStat_StatFuncEntry),
						  fpin) != sizeof(PgStat_StatFuncEntry))
				{
					ereport(LOG,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				env = get_stat_entry(PGSTAT_TYPE_TABLE, dbentry->databaseid,
									 funcbuf.functionid,
									 false, init_funcentry, &found);
				funcentry = (PgStat_StatFuncEntry *) &env->body;

				if (found)
				{
					ereport(LOG,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				memcpy(funcentry, &funcbuf, sizeof(funcbuf));
				break;

				/*
				 * 'E'	The EOF marker of a complete stats file.
				 */
			case 'E':
				goto done;

			default:
				ereport(LOG,
						(errmsg("corrupted statistics file \"%s\"",
								statfile)));
				goto done;
		}
	}

done:
	if (tabhash)
		dshash_detach(tabhash);
	if (funchash)
		dshash_detach(funchash);

	FreeFile(fpin);

	elog(DEBUG2, "removing permanent stats file \"%s\"", statfile);
	unlink(statfile);
}


/* ----------
 * pgstat_clear_snapshot() -
 *
 *	Discard any data collected in the current transaction.  Any subsequent
 *	request will cause new snapshots to be read.
 *
 *	This is also invoked during transaction commit or abort to discard
 *	the no-longer-wanted snapshot.
 * ----------
 */
void
pgstat_clear_snapshot(void)
{
	/* Release memory, if any was allocated */
	if (pgStatLocalContext)
	{
		MemoryContextDelete(pgStatLocalContext);

		/* Reset variables */
		pgStatLocalContext = NULL;
		localBackendStatusTable = NULL;
		localNumBackends = 0;
	}

	if (pgStatSnapshotContext)
	{
		MemoryContextReset(pgStatSnapshotContext);

		/* Reset variables that pointed to the context */
		global_snapshot_is_valid = false;
		pgStatSnapshotHash = NULL;
	}
}

/*
 * Convert a potentially unsafely truncated activity string (see
 * PgBackendStatus.st_activity_raw's documentation) into a correctly truncated
 * one.
 *
 * The returned string is allocated in the caller's memory context and may be
 * freed.
 */
char *
pgstat_clip_activity(const char *raw_activity)
{
	char	   *activity;
	int			rawlen;
	int			cliplen;

	/*
	 * Some callers, like pgstat_get_backend_current_activity(), do not
	 * guarantee that the buffer isn't concurrently modified. We try to take
	 * care that the buffer is always terminated by a NUL byte regardless, but
	 * let's still be paranoid about the string's length. In those cases the
	 * underlying buffer is guaranteed to be pgstat_track_activity_query_size
	 * large.
	 */
	activity = pnstrdup(raw_activity, pgstat_track_activity_query_size - 1);

	/* now double-guaranteed to be NUL terminated */
	rawlen = strlen(activity);

	/*
	 * All supported server-encodings make it possible to determine the length
	 * of a multi-byte character from its first byte (this is not the case for
	 * client encodings, see GB18030). As st_activity is always stored using
	 * server encoding, this allows us to perform multi-byte aware truncation,
	 * even if the string earlier was truncated in the middle of a multi-byte
	 * character.
	 */
	cliplen = pg_mbcliplen(activity, rawlen,
						   pgstat_track_activity_query_size - 1);

	activity[cliplen] = '\0';

	return activity;
}
