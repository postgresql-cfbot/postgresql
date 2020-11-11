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
 * often than once per PGSTAT_MIN_INTERVAL (10000ms). If some local numbers
 * remain unflushed for lock failure, retry with intervals that is initially
 * PGSTAT_RETRY_MIN_INTERVAL (1000ms) then doubled at every retry. Finally we
 * force update after PGSTAT_MAX_INTERVAL (60000ms) since the first trial.
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
#include "common/hashfn.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/fork_process.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "replication/slot.h"
#include "replication/walsender.h"
#include "storage/condition_variable.h"
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
#include "utils/timestamp.h"

/* ----------
 * Timer definitions.
 * ----------
 */
#define PGSTAT_MIN_INTERVAL			10000	/* Minimum interval of stats data
											 * updates; in milliseconds. */

#define PGSTAT_RETRY_MIN_INTERVAL	1000 /* Initial retry interval after
										  * PGSTAT_MIN_INTERVAL */

#define PGSTAT_MAX_INTERVAL			60000	/* Longest interval of stats data
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
 * List of SLRU names that we keep stats for.  There is no central registry of
 * SLRUs, so we use this fixed list instead.  The "other" entry is used for
 * all SLRUs without an explicit entry (e.g. SLRUs in extensions).
 */
static const char *const slru_names[] = {
	"CommitTs",
	"MultiXactMember",
	"MultiXactOffset",
	"Notify",
	"Serial",
	"Subtrans",
	"Xact",
	"other"						/* has to be last */
};

#define SLRU_NUM_ELEMENTS	lengthof(slru_names)

/* struct for shared SLRU stats */
typedef struct PgStatSharedSLRUStats
{
	PgStat_SLRUStats		entry[SLRU_NUM_ELEMENTS];
	LWLock					lock;
	pg_atomic_uint32		changecount;
} PgStatSharedSLRUStats;

StaticAssertDecl(sizeof(TimestampTz) == sizeof(pg_atomic_uint64),
				 "size of pg_atomic_uint64 doesn't match TimestampTz");

typedef struct StatsShmemStruct
{
	dsa_handle	stats_dsa_handle;	/* handle for stats data area */
	dshash_table_handle hash_handle;	/* shared dbstat hash */
	int			refcount;			/* # of processes that is attaching the
									 * shared stats memory */
	/* Global stats structs */
	PgStat_Archiver			archiver_stats;
	pg_atomic_uint32		archiver_changecount;
	PgStat_BgWriter			bgwriter_stats;
	pg_atomic_uint32		bgwriter_changecount;
	PgStat_CheckPointer		checkpointer_stats;
	pg_atomic_uint32		checkpointer_changecount;
	PgStat_Wal				wal_stats;
	LWLock					wal_stats_lock;
	PgStatSharedSLRUStats	slru_stats;
	pg_atomic_uint32		slru_changecount;
	pg_atomic_uint64		stats_timestamp;

	/* Reset offsets, protected by StatsLock */
	PgStat_Archiver			archiver_reset_offset;
	PgStat_BgWriter			bgwriter_reset_offset;
	PgStat_CheckPointer		checkpointer_reset_offset;

	/* file read/write protection */
	bool					attach_holdoff;
	ConditionVariable		holdoff_cv;

	pg_atomic_uint64		gc_count; /* # of entries deleted. not
											* protected by StatsLock */
} StatsShmemStruct;

/* BgWriter global statistics counters */
PgStat_BgWriter BgWriterStats = {0};

/* CheckPointer global statistics counters */
PgStat_CheckPointer CheckPointerStats = {0};

/* WAL global statistics counters */
PgStat_Wal WalStats = {0} ;

/*
 * XXXX: always try to flush WAL stats. We don't want to manipulate another
 * counter during XLogInsert so we don't have an effecient short cut to know
 * whether any counter gets incremented.
 */
static inline bool
walstats_pending(void)
{
	static const PgStat_BgWriter all_zeroes;

	return memcmp(&BgWriterStats, &all_zeroes, sizeof(PgStat_BgWriter)) != 0;
}

/*
 * SLRU statistics counts waiting to be written to the shared activity
 * statistics.  We assume this variable inits to zeroes.  Entries are
 * one-to-one with slru_names[].
 * Changes of SLRU counters are reported within critical sections so we use
 * static memory in order to avoid memory allocation.
 */
static PgStat_SLRUStats local_SLRUStats[SLRU_NUM_ELEMENTS];
static bool 	have_slrustats = false;

/* ----------
 * Local data
 * ----------
 */
/* backend-lifetime storages */
static StatsShmemStruct * StatsShmem = NULL;
static dsa_area *area = NULL;

/*
 * Types to define shared statistics structure.
 *
 * Per-object statistics are stored in a "shared stats", corresponding struct
 * that has a header part common among all object types in DSA-allocated
 * memory. All shared stats are pointed from a dshash via a dsa_pointer. This
 * structure make the shared stats immovable against dshash resizing, allows a
 * backend point to shared stats entries via a native pointer and allows
 * locking at stats-entry level. The per-entry locking reduces lock contention
 * compared to partition lock of dshash. A backend accumulates stats numbers in
 * a stats entry in the local memory space then flushes the numbers to shared
 * stats entries at basically transaction end.
 *
 * Each stat entry type has a fixed member PgStat_HashEntryHeader as the first
 * element.
 *
 * Shared stats are stored as:
 *
 * dshash pgStatSharedHash
 *    -> PgStatHashEntry					(dshash entry)
 *      (dsa_pointer)-> PgStat_Stat*Entry	(dsa memory block)
 *
 * Shared stats entries are directly pointed from pgstat_localhash hash:
 *
 * pgstat_localhash pgStatEntHash
 *    -> PgStatLocalHashEntry                (equivalent of PgStatHashEntry)
 *      (native pointer)-> PgStat_Stat*Entry (dsa memory block)
 *
 * Local stats that are waiting for being flushed to share stats are stored as:
 *
 * pgstat_localhash pgStatLocalHash
 *    -> PgStatLocalHashEntry			     (local hash entry)
 *      (native pointer)-> PgStat_Stat*Entry/TableStatus (palloc'ed memory)
 */

/* The types of statistics entries */
typedef enum PgStatTypes
{
	PGSTAT_TYPE_DB,			/* database-wide statistics */
	PGSTAT_TYPE_TABLE,		/* per-table statistics */
	PGSTAT_TYPE_FUNCTION,	/* per-function statistics */
	PGSTAT_TYPE_REPLSLOT	/* per-replication-slot statistics */
} PgStatTypes;

/*
 * entry body size lookup table of shared statistics entries corresponding to
 * PgStatTypes
 */
static const size_t pgstat_sharedentsize[] =
{
	sizeof(PgStat_StatDBEntry), 	/* PGSTAT_TYPE_DB */
	sizeof(PgStat_StatTabEntry),	/* PGSTAT_TYPE_TABLE */
	sizeof(PgStat_StatFuncEntry),	/* PGSTAT_TYPE_FUNCTION */
	sizeof(PgStat_ReplSlot)			/* PGSTAT_TYPE_REPLSLOT */
};

/* Ditto for local statistics entries */
static const size_t pgstat_localentsize[] =
{
	sizeof(PgStat_StatDBEntry),			/* PGSTAT_TYPE_DB */
	sizeof(PgStat_TableStatus),			/* PGSTAT_TYPE_TABLE */
	sizeof(PgStat_BackendFunctionEntry),/* PGSTAT_TYPE_FUNCTION */
	sizeof(PgStat_ReplSlot)				/* PGSTAT_TYPE_REPLSLOT */
};

/*
 * We shoud avoid overwriting header part of a shared entry. Use these macros
 * to know what portion of the struct to be written or read. PSTAT_SHENT_BODY
 * returns a bit smaller address than the actual address of the next member but
 * that doesn't matter.
 */
#define PGSTAT_SHENT_BODY(e) (((char *)(e)) + sizeof(PgStat_StatEntryHeader))
#define PGSTAT_SHENT_BODY_LEN(t) \
	(pgstat_sharedentsize[t] - sizeof(PgStat_StatEntryHeader))

/* struct for shared statistics hash entry key. */
typedef struct PgStatHashKey
{
	PgStatTypes type;		/* statistics entry type */
	Oid			databaseid;	/* database ID. InvalidOid for shared objects. */
	Oid			objectid;	/* object ID, either table or function. */
} PgStatHashKey;

/* struct for shared statistics hash entry */
typedef struct PgStatHashEntry
{
	PgStatHashKey	key; /* hash key */
	dsa_pointer		body;/* pointer to shared stats in PgStat_StatEntryHeader */
} PgStatHashEntry;

/* struct for shared statistics local hash entry. */
typedef struct PgStatLocalHashEntry
{
	PgStatHashKey			key;	/* hash key */
	char					status;	/* for simplehash use */
	PgStat_StatEntryHeader *body;	/* pointer to stats body in local heap */
	dsa_pointer				dsapointer; /* dsa pointer of body */
} PgStatLocalHashEntry;

/* parameter for the shared hash */
static const dshash_parameters dsh_params = {
	sizeof(PgStatHashKey),
	sizeof(PgStatHashEntry),
	dshash_memcmp,
	dshash_memhash,
	LWTRANCHE_STATS
};

/* define hashtable for local hashes */
#define SH_PREFIX pgstat_localhash
#define SH_ELEMENT_TYPE PgStatLocalHashEntry
#define SH_KEY_TYPE PgStatHashKey
#define SH_KEY key
#define SH_HASH_KEY(tb, key) \
	hash_bytes((unsigned char *)&key, sizeof(PgStatHashKey))
#define SH_EQUAL(tb, a, b) (memcmp(&a, &b, sizeof(PgStatHashKey)) == 0)
#define SH_SCOPE static inline
#define SH_DEFINE
#define SH_DECLARE
#include "lib/simplehash.h"

/* The shared hash to index activity stats entries. */
static dshash_table *pgStatSharedHash = NULL;

/*
 * The local cache to index shared stats entries.
 *
 * This is a local hash to store native pointers to shared hash
 * entries. pgStatEntHashAge is copied from StatsShmem->gc_count at creation
 * and garbage collection.
 */
static pgstat_localhash_hash *pgStatEntHash = NULL;
static int	 pgStatEntHashAge = 0;		/* cache age of pgStatEntHash */

/* Local stats numbers are stored here. */
static pgstat_localhash_hash *pgStatLocalHash = NULL;

/* entry type for oid hash */
typedef struct pgstat_oident
{
	Oid oid;
	char status;
} pgstat_oident;

/* Define hashtable for OID hashes. */
StaticAssertDecl(sizeof(Oid) == 4, "oid is not compatible with uint32");
#define SH_PREFIX pgstat_oid
#define SH_ELEMENT_TYPE pgstat_oident
#define SH_KEY_TYPE Oid
#define SH_KEY oid
#define SH_HASH_KEY(tb, key) hash_bytes_uint32(key)
#define SH_EQUAL(tb, a, b) (a == b)
#define SH_SCOPE static inline
#define SH_DEFINE
#define SH_DECLARE
#include "lib/simplehash.h"

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

/* Status for backends including auxiliary */
static LocalPgBackendStatus *localBackendStatusTable = NULL;

/* Total number of backends including auxiliary */
static int	localNumBackends = 0;

/*
 * Make our own memory context to make it easy to track memory usage.
 */
MemoryContext	pgStatCacheContext = NULL;

/*
 * Total time charged to functions so far in the current backend.
 * We use this to help separate "self" and "other" time charges.
 * (We assume this initializes to zero.)
 */
static instr_time total_func_time;

/* Simple caching feature for pgstatfuncs */
static PgStatHashKey	stathashkey_zero = {0};
static PgStatHashKey		cached_dbent_key = {0};
static PgStat_StatDBEntry	cached_dbent;
static PgStatHashKey		cached_tabent_key = {0};
static PgStat_StatTabEntry	cached_tabent;
static PgStatHashKey		cached_funcent_key = {0};
static PgStat_StatFuncEntry	cached_funcent;

static PgStat_Archiver		cached_archiverstats;
static bool					cached_archiverstats_is_valid = false;
static PgStat_BgWriter		cached_bgwriterstats;
static bool					cached_bgwriterstats_is_valid = false;
static PgStat_CheckPointer	cached_checkpointerstats;
static bool					cached_checkpointerstats_is_valid = false;
static PgStat_Wal			cached_walstats;
static bool					cached_walstats_is_valid = false;
static PgStat_SLRUStats		cached_slrustats;
static bool					cached_slrustats_is_valid = false;
static PgStat_ReplSlot	   *cached_replslotstats = NULL;
static int					n_cached_replslotstats = -1;

/* ----------
 * Local function forward declarations
 * ----------
 */
static void pgstat_beshutdown_hook(int code, Datum arg);

static PgStat_StatDBEntry *get_local_dbstat_entry(Oid dbid);
static PgStat_TableStatus *get_local_tabstat_entry(Oid rel_id, bool isshared);

static void pgstat_write_statsfile(void);

static void pgstat_read_statsfile(void);
static void pgstat_read_current_status(void);

static PgStat_StatEntryHeader * get_stat_entry(PgStatTypes type, Oid dbid,
											   Oid objid, bool nowait,
											   bool create, bool *found);

static bool flush_tabstat(PgStatLocalHashEntry *ent, bool nowait);
static bool flush_funcstat(PgStatLocalHashEntry *ent, bool nowait);
static bool flush_dbstat(PgStatLocalHashEntry *ent, bool nowait);
static bool flush_walstat(bool nowait);
static bool flush_slrustat(bool nowait);
static void delete_current_stats_entry(dshash_seq_status *hstat);
static PgStat_StatEntryHeader * get_local_stat_entry(PgStatTypes type, Oid dbid,
													 Oid objid, bool create,
													 bool *found);

static pgstat_oid_hash *collect_oids(Oid catalogid, AttrNumber anum_oid);

static void pgstat_setup_memcxt(void);

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
		ShmemInitStruct("Stats area", StatsShmemSize(),	&found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);

		StatsShmem->stats_dsa_handle = DSM_HANDLE_INVALID;
		ConditionVariableInit(&StatsShmem->holdoff_cv);
		pg_atomic_init_u32(&StatsShmem->archiver_changecount, 0);
		pg_atomic_init_u32(&StatsShmem->bgwriter_changecount, 0);
		pg_atomic_init_u32(&StatsShmem->checkpointer_changecount, 0);

		pg_atomic_init_u64(&StatsShmem->gc_count, 0);

		LWLockInitialize(&StatsShmem->wal_stats_lock, LWTRANCHE_STATS);
	}
}

/* ----------
 * allow_next_attacher() -
 *
 *  Let other processes to go ahead attaching the shared stats area.
 * ----------
 */
static void
allow_next_attacher(void)
{
	bool triggerd = false;

	LWLockAcquire(StatsLock, LW_EXCLUSIVE);
	if (StatsShmem->attach_holdoff)
	{
		StatsShmem->attach_holdoff = false;
		triggerd = true;
	}
	LWLockRelease(StatsLock);

	if (triggerd)
		ConditionVariableBroadcast(&StatsShmem->holdoff_cv);
}

/* ----------
 * attach_shared_stats() -
 *
 *	Attach shared or create stats memory. If we are the first process to use
 *	activity stats system, read the saved statistics file if any.
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

	/* stats shared memory persists for the backend lifetime */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * The first attacher backend may still reading the stats file, or the last
	 * detacher may writing it. Wait for the work to finish.
	 */
	ConditionVariablePrepareToSleep(&StatsShmem->holdoff_cv);
	for (;;)
	{
		bool hold_off;

		LWLockAcquire(StatsLock, LW_SHARED);
		hold_off = StatsShmem->attach_holdoff;
		LWLockRelease(StatsLock);

		if (!hold_off)
			break;

		ConditionVariableTimedSleep(&StatsShmem->holdoff_cv, 10,
									WAIT_EVENT_READING_STATS_FILE);
	}
	ConditionVariableCancelSleep();

	LWLockAcquire(StatsLock, LW_EXCLUSIVE);

	/*
	 * The last process is responsible to write out stats files at exit.
	 * Maintain refcount so that a process going to exit can find whether it is
	 * the last one or not.
	 */
	if (StatsShmem->refcount > 0)
		StatsShmem->refcount++;
	else
	{
		/* We're the first process to attach the shared stats memory */
		Assert(StatsShmem->stats_dsa_handle == DSM_HANDLE_INVALID);

		/* Initialize shared memory area */
		area = dsa_create(LWTRANCHE_STATS);
		pgStatSharedHash = dshash_create(area, &dsh_params, 0);

		StatsShmem->stats_dsa_handle = dsa_get_handle(area);
		StatsShmem->hash_handle = dshash_get_hash_table_handle(pgStatSharedHash);
		LWLockInitialize(&StatsShmem->slru_stats.lock, LWTRANCHE_STATS);
		pg_atomic_init_u32(&StatsShmem->slru_stats.changecount, 0);

		/* Block the next attacher for a while, see the comment above. */
		StatsShmem->attach_holdoff = true;

		StatsShmem->refcount = 1;
	}

	LWLockRelease(StatsLock);

	if (area)
	{
		/*
		 * We're the first attacher process, read stats file while blocking
		 * successors.
		 */
		Assert(StatsShmem->attach_holdoff);
		pgstat_read_statsfile();
		allow_next_attacher();
	}
	else
	{
		/* We're not the first one, attach existing shared area. */
		area = dsa_attach(StatsShmem->stats_dsa_handle);
		pgStatSharedHash = dshash_attach(area, &dsh_params,
										 StatsShmem->hash_handle, 0);

	}

	Assert(StatsShmem->stats_dsa_handle != DSM_HANDLE_INVALID);

	MemoryContextSwitchTo(oldcontext);

	/* don't detach automatically */
	dsa_pin_mapping(area);
}

/* ----------
 * cleanup_dropped_stats_entries() -
 *              Clean up shared stats entries no longer used.
 *
 *  Shared stats entries for dropped objects may be left referenced. Clean up
 *  our reference and drop the shared entry if needed.
 * ----------
 */
static void
cleanup_dropped_stats_entries(void)
{
	pgstat_localhash_iterator i;
	PgStatLocalHashEntry   *ent;

	if (pgStatEntHash == NULL)
		return;

	pgstat_localhash_start_iterate(pgStatEntHash, &i);
	while ((ent = pgstat_localhash_iterate(pgStatEntHash, &i))
		   != NULL)
	{
		/*
		 * Free the shared memory chunk for the entry if we were the last
		 * referrer to a dropped entry.
		 */
		if (pg_atomic_sub_fetch_u32(&ent->body->refcount, 1) < 1 &&
			ent->body->dropped)
			dsa_free(area, ent->dsapointer);
	}

	/*
	 * This function is expected to be called during backend exit. So we don't
	 * bother destroying pgStatEntHash.
	 */
	pgStatEntHash = NULL;
}

/* ----------
 * detach_shared_stats() -
 *
 *	Detach shared stats. Write out to file if we're the last process and told
 *	to do so.
 * ----------
 */
static void
detach_shared_stats(bool write_file)
{
	bool is_last_detacher = 0;

	/* immediately return if useless */
	if (!area || !IsUnderPostmaster)
		return;

	/* We shouldn't leave a reference to shared stats. */
	Assert(pgStatEntHash == NULL);

	/*
	 * If we are the last detacher, hold off the next attacher (if possible)
	 * until we finish writing stats file.
	 */
	LWLockAcquire(StatsLock, LW_EXCLUSIVE);
	if (--StatsShmem->refcount == 0)
	{
		StatsShmem->attach_holdoff = true;
		is_last_detacher = true;
	}
	LWLockRelease(StatsLock);

	if (is_last_detacher)
	{
		if (write_file)
			pgstat_write_statsfile();

		StatsShmem->stats_dsa_handle = DSM_HANDLE_INVALID;
		/* allow the next attacher, if any */
		allow_next_attacher();
	}

	/*
	 * Detach the area. It is automatically destroyed when the last process
	 * detached it.
	 */
	dsa_detach(area);

	area = NULL;
	pgStatSharedHash = NULL;

	/* We are going to exit. Don't bother destroying local hashes. */
	pgStatLocalHash = NULL;
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
	detach_shared_stats(false);
	attach_shared_stats();
}


/*
 * fetch_lock_statentry - common helper function to fetch and lock a stats
 * entry for flush_tabstat, flush_funcstat and flush_dbstat.
 */
static PgStat_StatEntryHeader *
fetch_lock_statentry(PgStatTypes type, Oid dboid, Oid objid, bool nowait)
{
	PgStat_StatEntryHeader *header;

	/* find shared table stats entry corresponding to the local entry */
	header = (PgStat_StatEntryHeader *)
		get_stat_entry(type, dboid, objid, nowait, true, NULL);

	/* skip if dshash failed to acquire lock */
	if (header == NULL)
		return false;

	/* lock the shared entry to protect the content, skip if failed */
	if (!nowait)
		LWLockAcquire(&header->lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(&header->lock, LW_EXCLUSIVE))
		return false;

	return header;
}


/* ----------
 * get_stat_entry() -
 *
 *	get shared stats entry for specified type, dbid and objid.
 *  If nowait is true, returns NULL on lock failure.
 *
 *  If initfunc is not NULL, new entry is created if not yet and the function
 *  is called with the new base entry. If found is not NULL, it is set to true
 *  if existing entry is found or false if not.
 *  ----------
 */
static PgStat_StatEntryHeader *
get_stat_entry(PgStatTypes type, Oid dbid, Oid objid, bool nowait, bool create,
			   bool *found)
{
	PgStatHashEntry		   *shhashent;
	PgStatLocalHashEntry   *lohashent;
	PgStat_StatEntryHeader *shheader = NULL;
	PgStatHashKey			key;
	bool					shfound;

	key.type		= type;
	key.databaseid 	= dbid;
	key.objectid	= objid;

	if (pgStatEntHash)
	{
		uint64 currage;

		/*
		 * pgStatEntHashAge increments quite slowly than the time the following
		 * loop takes so this is expected to iterate no more than twice.
		 */
		while (unlikely
			   (pgStatEntHashAge !=
				(currage = pg_atomic_read_u64(&StatsShmem->gc_count))))
		{
			pgstat_localhash_iterator i;

			/*
			 * Some entries have been dropped. Invalidate cache pointer to
			 * them.
			 */
			pgstat_localhash_start_iterate(pgStatEntHash, &i);
			while ((lohashent = pgstat_localhash_iterate(pgStatEntHash, &i))
				   != NULL)
			{
				PgStat_StatEntryHeader *header = lohashent->body;
				if (header->dropped)
				{
					pgstat_localhash_delete(pgStatEntHash, key);

					if (pg_atomic_sub_fetch_u32(&header->refcount, 1) < 1)
					{
						/*
						 * We're the last referrer to this entry, drop the
						 * shared entry.
						 */
						dsa_free(area, lohashent->dsapointer);
					}
				}
			}

			pgStatEntHashAge = currage;
		}

		lohashent = pgstat_localhash_lookup(pgStatEntHash, key);

		if (lohashent)
		{
			if (found)
				*found = true;
			return lohashent->body;
		}
	}

	shhashent = dshash_find_extended(pgStatSharedHash, &key,
									 create, nowait, create, &shfound);
	if (shhashent)
	{
		if (create && !shfound)
		{
			/* Create new stats entry. */
			dsa_pointer chunk = dsa_allocate0(area,
											  pgstat_sharedentsize[type]);

			shheader = dsa_get_address(area, chunk);
			LWLockInitialize(&shheader->lock, LWTRANCHE_STATS);
			pg_atomic_init_u32(&shheader->refcount, 0);

			/* Link the new entry from the hash entry. */
			shhashent->body = chunk;
		}
		else
			shheader = dsa_get_address(area, shhashent->body);

		/*
		 * We expose this shared entry now.  You might think that the entry can
		 * be removed by a concurrent backend, but since we are creating an
		 * stats entry, the object actually exists and used in the upper
		 * layer. Such an object cannot be dropped until the first vacuum after
		 * the current transaction ends.
		 */
		dshash_release_lock(pgStatSharedHash, shhashent);

		/* register to local hash if possible */
		if (pgStatEntHash || pgStatCacheContext)
		{
			bool					lofound;

			if (pgStatEntHash == NULL)
			{
				pgStatEntHash =
					pgstat_localhash_create(pgStatCacheContext,
										PGSTAT_TABLE_HASH_SIZE, NULL);
				pgStatEntHashAge =
					pg_atomic_read_u64(&StatsShmem->gc_count);
			}

			lohashent =
				pgstat_localhash_insert(pgStatEntHash, key, &lofound);

			Assert(!lofound);
			lohashent->body = shheader;
			lohashent->dsapointer = shhashent->body;

			pg_atomic_add_fetch_u32(&shheader->refcount, 1);
		}
	}

	if (found)
		*found = shfound;

	return shheader;
}

/*
 * flush_walstat - flush out a local WAL stats entries
 *
 * If nowait is true, this function returns false on lock failure. Otherwise
 * this function always returns true.
 *
 * Returns true if all local WAL stats are successfully flushed out.
 */
static bool
flush_walstat(bool nowait)
{
	/* We assume this initializes to zeroes */
	PgStat_Wal *s = &StatsShmem->wal_stats;
	PgStat_Wal *l = &WalStats;

	/*
	 * This function can be called even if nothing at all has happened. In
	 * this case, avoid taking lock for a completely empty stats.
	 */
	if (walstats_pending())
		return true;

	/* lock the shared entry to protect the content, skip if failed */
	if (!nowait)
		LWLockAcquire(&StatsShmem->wal_stats_lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(&StatsShmem->wal_stats_lock,
									   LW_EXCLUSIVE))
		return false;			/* failed to acquire lock, skip */

	s->wal_buffers_full += l->wal_buffers_full;
	LWLockRelease(&StatsShmem->wal_stats_lock);

	/*
	 * Clear out the statistics buffer, so it can be re-used.
	 */
	MemSet(&WalStats, 0, sizeof(WalStats));

	return true;
}

/*
 * flush_slrustat - flush out a local SLRU stats entries
 *
 * If nowait is true, this function returns false on lock failure. Otherwise
 * this function always returns true. Writer processes are mutually excluded
 * using LWLock, but readers are expected to use change-count protocol to avoid
 * interference with writers.
 *
 * Returns true if all local SLRU stats are successfully flushed out.
 */
static bool
flush_slrustat(bool nowait)
{
	uint32	assert_changecount PG_USED_FOR_ASSERTS_ONLY;
	int i;

	if (!have_slrustats)
		return true;

	/* lock the shared entry to protect the content, skip if failed */
	if (!nowait)
		LWLockAcquire(&StatsShmem->slru_stats.lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(&StatsShmem->slru_stats.lock,
									   LW_EXCLUSIVE))
		return false;			/* failed to acquire lock, skip */

	assert_changecount =
		pg_atomic_fetch_add_u32(&StatsShmem->slru_stats.changecount, 1);
	Assert((assert_changecount & 1) == 0);

	for (i = 0 ; i < SLRU_NUM_ELEMENTS ; i++)
	{
		PgStat_SLRUStats *sharedent = &StatsShmem->slru_stats.entry[i];
		PgStat_SLRUStats *localent = &local_SLRUStats[i];

		sharedent->blocks_zeroed += localent->blocks_zeroed;
		sharedent->blocks_hit += localent->blocks_hit;
		sharedent->blocks_read += localent->blocks_read;
		sharedent->blocks_written += localent->blocks_written;
		sharedent->blocks_exists += localent->blocks_exists;
		sharedent->flush += localent->flush;
		sharedent->truncate += localent->truncate;
	}

	/* done, clear the local entry */
	MemSet(local_SLRUStats, 0,
		   sizeof(PgStat_SLRUStats) * SLRU_NUM_ELEMENTS);

	pg_atomic_add_fetch_u32(&StatsShmem->slru_stats.changecount, 1);
	LWLockRelease(&StatsShmem->slru_stats.lock);

	have_slrustats = false;

	return true;
}

/* ----------
 * delete_current_stats_entry()
 *
 *  Deletes the given shared entry from shared stats hash. The entry must be
 *  exclusively locked.
 * ----------
 */
static void
delete_current_stats_entry(dshash_seq_status *hstat)
{
	dsa_pointer				pdsa;
	PgStat_StatEntryHeader *header;
	PgStatHashEntry *ent;

	ent = dshash_get_current(hstat);
	pdsa = ent->body;
	header = dsa_get_address(area, pdsa);

	/* No one find this entry ever after. */
	dshash_delete_current(hstat);

	/*
	 * Let the referrers drop the entry if any.  Refcount won't be decremented
	 * until "dropped" is set true and StatsShmem->gc_count is incremented
	 * later. So we can check refcount to set dropped without holding a
	 * lock. If no one is referring this entry, free it immediately.
	 */

	if (pg_atomic_read_u32(&header->refcount) > 0)
		header->dropped = true;
	else
		dsa_free(area, pdsa);

	return;
}

/* ----------
 * get_local_stat_entry() -
 *
 *  Returns local stats entry for the type, dbid and objid.
 *  If create is true, new entry is created if not yet.  found must be non-null
 *  in the case.
 *
 *
 *  The caller is responsible to initialize the statsbody part of the returned
 *  memory.
 * ----------
 */
static PgStat_StatEntryHeader *
get_local_stat_entry(PgStatTypes type, Oid dbid, Oid objid,
					 bool create, bool *found)
{
	PgStatHashKey key;
	PgStatLocalHashEntry *entry;

	if (pgStatLocalHash == NULL)
		pgStatLocalHash = pgstat_localhash_create(pgStatCacheContext,
												  PGSTAT_TABLE_HASH_SIZE, NULL);

	/* Find an entry or create a new one. */
	key.type = type;
	key.databaseid = dbid;
	key.objectid = objid;
	if (create)
		entry = pgstat_localhash_insert(pgStatLocalHash, key, found);
	else
		entry = pgstat_localhash_lookup(pgStatLocalHash, key);

	if (!create && !entry)
		return NULL;

	if (create && !*found)
		entry->body = MemoryContextAllocZero(TopMemoryContext,
											 pgstat_localentsize[type]);

	return entry->body;
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
	static TimestampTz	next_flush = 0;
	static TimestampTz	pending_since = 0;
	static long			retry_interval = 0;
	TimestampTz now;
	bool		nowait;
	int			i;
	uint64		oldval;

	/* Return if not active */
	if (area == NULL)
		return 0;

	/*
	 * We need a database entry if the following stats exists.
	 */
	if (pgStatXactCommit > 0 || pgStatXactRollback > 0 ||
		pgStatBlockReadTime > 0 || pgStatBlockWriteTime > 0)
		get_local_dbstat_entry(MyDatabaseId);

	/* Don't expend a clock check if nothing to do */
	if (pgStatLocalHash == NULL && have_slrustats && !walstats_pending())
		return 0;

	now = GetCurrentTransactionStopTimestamp();

	if (!force)
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
			force = true;
	}

	/* don't wait for lock acquisition when !force */
	nowait = !force;

	if (pgStatLocalHash)
	{
		int			remains = 0;
		pgstat_localhash_iterator i;
		List				   *dbentlist = NIL;
		ListCell			   *lc;
		PgStatLocalHashEntry   *lent;

		/* Step 1: flush out other than database stats */
		pgstat_localhash_start_iterate(pgStatLocalHash, &i);
		while ((lent = pgstat_localhash_iterate(pgStatLocalHash, &i)) != NULL)
		{
			bool		remove = false;

			switch (lent->key.type)
			{
				case PGSTAT_TYPE_DB:
					/*
					 * flush_tabstat applies some of stats numbers of flushed
					 * entries into local database stats. Just remember the
					 * database entries for now then flush-out them later.
					 */
					dbentlist = lappend(dbentlist, lent);
					break;
				case PGSTAT_TYPE_TABLE:
					if (flush_tabstat(lent, nowait))
						remove = true;
					break;
				case PGSTAT_TYPE_FUNCTION:
					if (flush_funcstat(lent, nowait))
						remove = true;
					break;
				case PGSTAT_TYPE_REPLSLOT:
					/* We don't have that kind of local entry */
					Assert(false);
			}

			if (!remove)
			{
				remains++;
				continue;
			}

			/* Remove the successfully flushed entry */
			pfree(lent->body);
			lent->body = NULL;
			pgstat_localhash_delete(pgStatLocalHash, lent->key);
		}

		/* Step 2: flush out database stats */
		foreach(lc, dbentlist)
		{
			PgStatLocalHashEntry *lent = (PgStatLocalHashEntry *) lfirst(lc);

			if (flush_dbstat(lent, nowait))
			{
				remains--;
				/* Remove the successfully flushed entry */
				pfree(lent->body);
				lent->body = NULL;
				pgstat_localhash_delete(pgStatLocalHash, lent->key);
			}
		}
		list_free(dbentlist);
		dbentlist = NULL;

		if (remains <= 0)
		{
			pgstat_localhash_destroy(pgStatLocalHash);
			pgStatLocalHash = NULL;
		}
	}

	/* flush wal stats */
	flush_walstat(nowait);

	/* flush SLRU stats */
	flush_slrustat(nowait);

	/*
	 * Publish the time of the last flush, but we don't notify the change of
	 * the timestamp itself. Readers will get sufficiently recent timestamp.
	 * If we failed to update the value, concurrent processes should have
	 * updated it to sufficiently recent time.
	 *
	 * XXX: The loop might be unnecessary for the reason above.
	 */
	oldval = pg_atomic_read_u64(&StatsShmem->stats_timestamp);

	for (i = 0 ; i < 10 ; i++)
	{
		if (oldval >= now ||
			pg_atomic_compare_exchange_u64(&StatsShmem->stats_timestamp,
										   &oldval, (uint64) now))
			break;
	}

	/*
	 * Some of the local stats may have not been flushed due to lock
	 * contention.  If we have such pending local stats here, let the caller
	 * know the retry interval.
	 */
	if (pgStatLocalHash != NULL || have_slrustats || walstats_pending())
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
flush_tabstat(PgStatLocalHashEntry *ent, bool nowait)
{
	static const PgStat_TableCounts all_zeroes;
	Oid					dboid;			/* database OID of the table */
	PgStat_TableStatus *lstats;			/* local stats entry  */
	PgStat_StatTabEntry *shtabstats;	/* table entry of shared stats */
	PgStat_StatDBEntry *ldbstats;		/* local database entry */

	Assert(ent->key.type == PGSTAT_TYPE_TABLE);
	lstats = (PgStat_TableStatus *) ent->body;
	dboid = ent->key.databaseid;

	/*
	 * Ignore entries that didn't accumulate any actual counts, such as
	 * indexes that were opened by the planner but not used.
	 */
	if (memcmp(&lstats->t_counts, &all_zeroes,
			   sizeof(PgStat_TableCounts)) == 0)
	{
		/* This local entry is going to be dropped, delink from relcache. */
		pgstat_delinkstats(lstats->relation);
		return true;
	}

	/* find shared table stats entry corresponding to the local entry */
	shtabstats = (PgStat_StatTabEntry *)
		fetch_lock_statentry(PGSTAT_TYPE_TABLE, dboid, ent->key.objectid,
							 nowait);

	if (shtabstats == NULL)
		return false;			/* failed to acquire lock, skip */

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
	if (lstats->t_counts.t_truncated)
	{
		shtabstats->n_live_tuples = 0;
		shtabstats->n_dead_tuples = 0;
	}

	shtabstats->n_live_tuples += lstats->t_counts.t_delta_live_tuples;
	shtabstats->n_dead_tuples += lstats->t_counts.t_delta_dead_tuples;
	shtabstats->changes_since_analyze += lstats->t_counts.t_changed_tuples;
	shtabstats->inserts_since_vacuum += lstats->t_counts.t_tuples_inserted;
	shtabstats->blocks_fetched += lstats->t_counts.t_blocks_fetched;
	shtabstats->blocks_hit += lstats->t_counts.t_blocks_hit;

	/* Clamp n_live_tuples in case of negative delta_live_tuples */
	shtabstats->n_live_tuples = Max(shtabstats->n_live_tuples, 0);
	/* Likewise for n_dead_tuples */
	shtabstats->n_dead_tuples = Max(shtabstats->n_dead_tuples, 0);

	LWLockRelease(&shtabstats->header.lock);

	/* The entry is successfully flushed so the same to add to database stats */
	ldbstats = get_local_dbstat_entry(dboid);
	ldbstats->counts.n_tuples_returned += lstats->t_counts.t_tuples_returned;
	ldbstats->counts.n_tuples_fetched += lstats->t_counts.t_tuples_fetched;
	ldbstats->counts.n_tuples_inserted += lstats->t_counts.t_tuples_inserted;
	ldbstats->counts.n_tuples_updated += lstats->t_counts.t_tuples_updated;
	ldbstats->counts.n_tuples_deleted += lstats->t_counts.t_tuples_deleted;
	ldbstats->counts.n_blocks_fetched += lstats->t_counts.t_blocks_fetched;
	ldbstats->counts.n_blocks_hit += lstats->t_counts.t_blocks_hit;

	/* This local entry is going to be dropped, delink from relcache. */
	pgstat_delinkstats(lstats->relation);

	return true;
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
flush_funcstat(PgStatLocalHashEntry *ent, bool nowait)
{
	PgStat_BackendFunctionEntry *localent;	/* local stats entry */
	PgStat_StatFuncEntry *shfuncent = NULL; /* shared stats entry */

	Assert(ent->key.type == PGSTAT_TYPE_FUNCTION);
	localent = (PgStat_BackendFunctionEntry *) ent->body;

	/* localent always has non-zero content */

	/* find shared table stats entry corresponding to the local entry */
	shfuncent = (PgStat_StatFuncEntry *)
		fetch_lock_statentry(PGSTAT_TYPE_FUNCTION, MyDatabaseId,
							 ent->key.objectid, nowait);
	if (shfuncent == NULL)
		return false;			/* failed to acquire lock, skip */

	shfuncent->f_numcalls += localent->f_counts.f_numcalls;
	shfuncent->f_total_time +=
		INSTR_TIME_GET_MICROSEC(localent->f_counts.f_total_time);
	shfuncent->f_self_time +=
		INSTR_TIME_GET_MICROSEC(localent->f_counts.f_self_time);

	LWLockRelease(&shfuncent->header.lock);

	return true;
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
flush_dbstat(PgStatLocalHashEntry *ent, bool nowait)
{
	PgStat_StatDBEntry *localent;
	PgStat_StatDBEntry *sharedent;

	Assert(ent->key.type == PGSTAT_TYPE_DB);

	localent = (PgStat_StatDBEntry *) &ent->body;

	/* find shared database stats entry corresponding to the local entry */
	sharedent = (PgStat_StatDBEntry *)
		fetch_lock_statentry(PGSTAT_TYPE_DB, ent->key.databaseid, InvalidOid,
							 nowait);

	if (!sharedent)
		return false;			/* failed to acquire lock, skip */

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
	if (OidIsValid(ent->key.databaseid))
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

	LWLockRelease(&sharedent->header.lock);

	return true;
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
	pgstat_oid_hash	   *dbids;	/* database ids */
	pgstat_oid_hash	   *relids;	/* relation ids in the current database */
	pgstat_oid_hash	   *funcids;/* function ids in the current database */
	int			nvictims = 0;	/* # of entries of the above */
	dshash_seq_status dshstat;
	PgStatHashEntry *ent;

	/* we don't collect stats under standalone mode */
	if (!IsUnderPostmaster)
		return;

	/* collect oids of existent objects */
	dbids = collect_oids(DatabaseRelationId, Anum_pg_database_oid);
	relids = collect_oids(RelationRelationId, Anum_pg_class_oid);
	funcids = collect_oids(ProcedureRelationId, Anum_pg_proc_oid);

	nvictims = 0;

	/* some of the dshash entries are to be removed, take exclusive lock. */
	dshash_seq_init(&dshstat, pgStatSharedHash, true);
	while ((ent = dshash_seq_next(&dshstat)) != NULL)
	{
		pgstat_oid_hash *oidhash;
		Oid		   key;

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
				oidhash = dbids;
				key = ent->key.databaseid;
				break;

			case PGSTAT_TYPE_TABLE:
				oidhash = relids;
				key = ent->key.objectid;
				break;

			case PGSTAT_TYPE_FUNCTION:
				oidhash = funcids;
				key = ent->key.objectid;
				break;

			case PGSTAT_TYPE_REPLSLOT:
				/*
				 * We don't bother vacuumming this kind of entries because the
				 * number of entries is quite small and entries are likely to
				 * be reused soon.
				 */
				continue;
		}

		/* Skip existent objects. */
		if (pgstat_oid_lookup(oidhash, key) != NULL)
			continue;

		/* drop this etnry */
		delete_current_stats_entry(&dshstat);
		nvictims++;
	}
	dshash_seq_term(&dshstat);
	pgstat_oid_destroy(dbids);
	pgstat_oid_destroy(relids);
	pgstat_oid_destroy(funcids);

	if (nvictims > 0)
		pg_atomic_add_fetch_u64(&StatsShmem->gc_count, 1);
}

/* ----------
 * collect_oids() -
 *
 *	Collect the OIDs of all objects listed in the specified system catalog
 *	into a temporary hash table.  Caller should pgsstat_oid_destroy the result
 *	when done with it.  (However, we make the table in CurrentMemoryContext
 *	so that it will be freed properly in event of an error.)
 * ----------
 */
static pgstat_oid_hash *
collect_oids(Oid catalogid, AttrNumber anum_oid)
{
	pgstat_oid_hash *rethash;
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tup;
	Snapshot	snapshot;

	rethash = pgstat_oid_create(CurrentMemoryContext,
								PGSTAT_TABLE_HASH_SIZE, NULL);

	rel = table_open(catalogid, AccessShareLock);
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scan = table_beginscan(rel, snapshot, 0, NULL);
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Oid			thisoid;
		bool		isnull;
		bool		found;

		thisoid = heap_getattr(tup, anum_oid, RelationGetDescr(rel), &isnull);
		Assert(!isnull);

		CHECK_FOR_INTERRUPTS();

		pgstat_oid_insert(rethash, thisoid, &found);
	}
	table_endscan(scan);
	UnregisterSnapshot(snapshot);
	table_close(rel, AccessShareLock);

	return rethash;
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
	dshash_seq_status hstat;
	PgStatHashEntry *p;

	Assert(OidIsValid(databaseid));

	if (!IsUnderPostmaster || !pgStatSharedHash)
		return;

	/* some of the dshash entries are to be removed, take exclusive lock. */
	dshash_seq_init(&hstat, pgStatSharedHash, true);
	while ((p = dshash_seq_next(&hstat)) != NULL)
	{
		if (p->key.databaseid == MyDatabaseId)
			delete_current_stats_entry(&hstat);
	}
	dshash_seq_term(&hstat);

	/* Let readers run a garbage collection of local hashes */
	pg_atomic_add_fetch_u64(&StatsShmem->gc_count, 1);
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
	dshash_seq_status hstat;
	PgStatHashEntry *p;

	if (!IsUnderPostmaster || !pgStatSharedHash)
		return;

	/* dshash entry is not modified, take shared lock */
	dshash_seq_init(&hstat, pgStatSharedHash, false);
	while ((p = dshash_seq_next(&hstat)) != NULL)
	{
		PgStat_StatEntryHeader *header;

		if (p->key.databaseid != MyDatabaseId)
			continue;

		header = dsa_get_address(area, p->body);

		LWLockAcquire(&header->lock, LW_EXCLUSIVE);
		memset(PGSTAT_SHENT_BODY(header), 0,
			   PGSTAT_SHENT_BODY_LEN(p->key.type));

		if (p->key.type == PGSTAT_TYPE_DB)
		{
			PgStat_StatDBEntry *dbstat = (PgStat_StatDBEntry *) header;
			dbstat->stat_reset_timestamp = GetCurrentTimestamp();
		}
		LWLockRelease(&header->lock);
	}
	dshash_seq_term(&hstat);

	/* Invalidate the simple cache keys */
	cached_dbent_key = stathashkey_zero;
	cached_tabent_key = stathashkey_zero;
	cached_funcent_key = stathashkey_zero;
}

/*
 * pgstat_copy_global_stats - helper function for functions
 *           pgstat_fetch_stat_*() and pgstat_reset_shared_counters().
 *
 * Copies out the specified memory area following change-count protocol.
 */
static inline void
pgstat_copy_global_stats(void *dst, void*src, size_t len,
						 pg_atomic_uint32 *count)
{
	int before_changecount;
	int after_changecount;

	after_changecount =	pg_atomic_read_u32(count);

	do
	{
		before_changecount = after_changecount;
		memcpy(dst, src, len);
		after_changecount = pg_atomic_read_u32(count);
	} while ((before_changecount & 1) == 1 ||
			 after_changecount != before_changecount);
}

/* ----------
 * pgstat_reset_shared_counters() -
 *
 *	Reset cluster-wide shared counters.
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 *
 *  We don't scribble on shared stats while resetting to avoid locking on
 *  shared stats struct. Instead, just record the current counters in another
 *  shared struct, which is protected by StatsLock. See
 *  pgstat_fetch_stat_(archiver|bgwriter|checkpointer) for the reader side.
 * ----------
 */
void
pgstat_reset_shared_counters(const char *target)
{
	TimestampTz now = GetCurrentTimestamp();
	PgStat_Shared_Reset_Target t;

	if (strcmp(target, "archiver") == 0)
		t = RESET_ARCHIVER;
	else if (strcmp(target, "bgwriter") == 0)
		t = RESET_BGWRITER;
	else if (strcmp(target, "wal") == 0)
		t = RESET_WAL;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unrecognized reset target: \"%s\"", target),
				 errhint("Target must be \"archiver\", \"bgwriter\" or \"wal\".")));

	/* Reset statistics for the cluster. */
	LWLockAcquire(StatsLock, LW_EXCLUSIVE);

	switch (t)
	{
		case RESET_ARCHIVER:
			pgstat_copy_global_stats(&StatsShmem->archiver_reset_offset,
									 &StatsShmem->archiver_stats,
									 sizeof(PgStat_Archiver),
									 &StatsShmem->archiver_changecount);
			StatsShmem->archiver_reset_offset.stat_reset_timestamp = now;
			cached_archiverstats_is_valid = false;
			break;

		case RESET_BGWRITER:
			pgstat_copy_global_stats(&StatsShmem->bgwriter_reset_offset,
									 &StatsShmem->bgwriter_stats,
									 sizeof(PgStat_BgWriter),
									 &StatsShmem->bgwriter_changecount);
			pgstat_copy_global_stats(&StatsShmem->checkpointer_reset_offset,
									 &StatsShmem->checkpointer_stats,
									 sizeof(PgStat_CheckPointer),
									 &StatsShmem->checkpointer_changecount);
			StatsShmem->bgwriter_reset_offset.stat_reset_timestamp = now;
			cached_bgwriterstats_is_valid = false;
			cached_checkpointerstats_is_valid = false;
			break;

		case RESET_WAL:
			/*
			 * Differntly from the two above, WAL statistics has many writer
			 * processes and protected by wal_stats_lock.
			 */
			LWLockAcquire(&StatsShmem->wal_stats_lock, LW_EXCLUSIVE);
			MemSet(&StatsShmem->wal_stats, 0, sizeof(PgStat_Wal));
			StatsShmem->wal_stats.stat_reset_timestamp = now;
			LWLockRelease(&StatsShmem->wal_stats_lock);
			cached_walstats_is_valid = false;
			break;
	}

	LWLockRelease(StatsLock);
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
	PgStat_StatEntryHeader *header;
	PgStat_StatDBEntry *dbentry;
	PgStatTypes stattype;
	TimestampTz ts;

	dbentry = (PgStat_StatDBEntry *)
		get_stat_entry(PGSTAT_TYPE_DB, MyDatabaseId, InvalidOid, false, false,
					   NULL);
	Assert(dbentry);

	/* Set the reset timestamp for the whole database */
	ts = GetCurrentTimestamp();
	LWLockAcquire(&dbentry->header.lock, LW_EXCLUSIVE);
	dbentry->stat_reset_timestamp = ts;
	LWLockRelease(&dbentry->header.lock);

	/* Remove object if it exists, ignore if not */
	switch (type)
	{
		case RESET_TABLE:
			stattype = PGSTAT_TYPE_TABLE;
			break;
		case RESET_FUNCTION:
			stattype = PGSTAT_TYPE_FUNCTION;
	}

	header = get_stat_entry(stattype, MyDatabaseId, objoid, false, false, NULL);

	LWLockAcquire(&header->lock, LW_EXCLUSIVE);
	memset(PGSTAT_SHENT_BODY(header), 0, PGSTAT_SHENT_BODY_LEN(stattype));
	LWLockRelease(&header->lock);
}

/* ----------
 * pgstat_reset_slru_counter() -
 *
 *	Tell the statistics collector to reset a single SLRU counter, or all
 *	SLRU counters (when name is null).
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_slru_counter(const char *name)
{
	int i;
	TimestampTz	ts = GetCurrentTimestamp();
	uint32	assert_changecount;PG_USED_FOR_ASSERTS_ONLY;

	if (name)
	{
		i = pgstat_slru_index(name);
		LWLockAcquire(&StatsShmem->slru_stats.lock, LW_EXCLUSIVE);
		assert_changecount =
			pg_atomic_fetch_add_u32(&StatsShmem->slru_changecount, 1);
		Assert((assert_changecount & 1) == 0);
		MemSet(&StatsShmem->slru_stats.entry[i], 0,
			   sizeof(PgStat_SLRUStats));
		StatsShmem->slru_stats.entry[i].stat_reset_timestamp = ts;
		pg_atomic_add_fetch_u32(&StatsShmem->slru_changecount, 1);
		LWLockRelease(&StatsShmem->slru_stats.lock);
	}
	else
	{
		LWLockAcquire(&StatsShmem->slru_stats.lock, LW_EXCLUSIVE);
		assert_changecount =
			pg_atomic_fetch_add_u32(&StatsShmem->slru_changecount, 1);
		Assert((assert_changecount & 1) == 0);
		for (i = 0 ; i < SLRU_NUM_ELEMENTS; i++)
		{
			MemSet(&StatsShmem->slru_stats.entry[i], 0,
				   sizeof(PgStat_SLRUStats));
			StatsShmem->slru_stats.entry[i].stat_reset_timestamp = ts;
		}
		pg_atomic_add_fetch_u32(&StatsShmem->slru_changecount, 1);
		LWLockRelease(&StatsShmem->slru_stats.lock);
	}

	cached_slrustats_is_valid = false;
}

/* ----------
 * pgstat_reset_replslot_counter() -
 *
 *	Tell the statistics collector to reset a single replication slot
 *	counter, or all replication slots counters (when name is null).
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_replslot_counter(const char *name)
{
	int			startidx;
	int			endidx;
	int			i;
	TimestampTz	ts;

	if (!IsUnderPostmaster || !pgStatSharedHash)
		return;

	if (name)
	{
		ReplicationSlot *slot;
			
		/* Check if the slot exits with the given name. */
		LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
		slot = SearchNamedReplicationSlot(name);
		LWLockRelease(ReplicationSlotControlLock);

		if (!slot)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("replication slot \"%s\" does not exist",
							name)));

		/*
		 * Nothing to do for physical slots as we collect stats only for
		 * logical slots.
		 */
		if (SlotIsPhysical(slot))
			return;

		/* reset this one entry */
		startidx = endidx = slot - ReplicationSlotCtl->replication_slots;
	}
	else
	{
		/* reset all existent entries */
		startidx = 0;
		endidx = max_replication_slots - 1;
	}

	ts = GetCurrentTimestamp();
	for (i = startidx ; i <= endidx ; i++)
	{
		PgStat_ReplSlot *shent;

		shent = (PgStat_ReplSlot *)
			get_stat_entry(PGSTAT_TYPE_REPLSLOT,
						   MyDatabaseId, i, false, false, NULL);

		/* Skip non-existent entries */
		if (!shent)
			continue;

		LWLockAcquire(&shent->header.lock, LW_EXCLUSIVE);
		memset(&shent->spill_txns, 0,
			   offsetof(PgStat_ReplSlot, stat_reset_timestamp) -
			   offsetof(PgStat_ReplSlot, spill_txns));
		shent->stat_reset_timestamp = ts;
		LWLockRelease(&shent->header.lock);
	}
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

	/*
	 * End-of-vacuum is reported instantly. Report the start the same way for
	 * consistency. Vacuum doesn't run frequently and is a long-lasting
	 * operation so it doesn't matter if we get blocked here a little.
	 */
	dbentry = (PgStat_StatDBEntry *)
		get_stat_entry(PGSTAT_TYPE_DB, dboid, InvalidOid, false, true, NULL);

	ts = GetCurrentTimestamp();;
	LWLockAcquire(&dbentry->header.lock, LW_EXCLUSIVE);
	dbentry->last_autovac_time = ts;
	LWLockRelease(&dbentry->header.lock);
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
	PgStat_StatTabEntry *tabentry;
	Oid			dboid = (shared ? InvalidOid : MyDatabaseId);
	TimestampTz ts;

	/* return if we are not collecting stats */
	if (!area)
		return;

	/* Store the data in the table's hash table entry. */
	ts = GetCurrentTimestamp();

	/*
	 * Differently from ordinary operations, maintenance commands take longer
	 * time and getting blocked at the end of work doesn't matter. Furthermore,
	 * this can prevent the stats updates made by the transactions that ends
	 * after this vacuum from being canceled by a delayed vacuum report.
	 * Update shared stats entry directly for the above reasons.
	 */
	tabentry = (PgStat_StatTabEntry *)
		get_stat_entry(PGSTAT_TYPE_TABLE, dboid, tableoid, false, true, NULL);

	LWLockAcquire(&tabentry->header.lock, LW_EXCLUSIVE);
	tabentry->n_live_tuples = livetuples;
	tabentry->n_dead_tuples = deadtuples;

	/*
	 * It is quite possible that a non-aggressive VACUUM ended up skipping
	 * various pages, however, we'll zero the insert counter here regardless.
	 * It's currently used only to track when we need to perform an "insert"
	 * autovacuum, which are mainly intended to freeze newly inserted tuples.
	 * Zeroing this may just mean we'll not try to vacuum the table again
	 * until enough tuples have been inserted to trigger another insert
	 * autovacuum.  An anti-wraparound autovacuum will catch any persistent
	 * stragglers.
	 */
	tabentry->inserts_since_vacuum = 0;

	if (IsAutoVacuumWorkerProcess())
	{
		tabentry->autovac_vacuum_timestamp = ts;
		tabentry->autovac_vacuum_count++;
	}
	else
	{
		tabentry->vacuum_timestamp = ts;
		tabentry->vacuum_count++;
	}

	LWLockRelease(&tabentry->header.lock);
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
	PgStat_StatTabEntry *tabentry;
	Oid		dboid = (rel->rd_rel->relisshared ? InvalidOid : MyDatabaseId);

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

	/*
	 * Differently from ordinary operations, maintenance commands take longer
	 * time and getting blocked at the end of work doesn't matter. Furthermore,
	 * this can prevent the stats updates made by the transactions that ends
	 * after this analyze from being canceled by a delayed analyze report.
	 * Update shared stats entry directly for the above reasons.
	 */
	tabentry = (PgStat_StatTabEntry *)
		get_stat_entry(PGSTAT_TYPE_TABLE, dboid, RelationGetRelid(rel),
					   false, true, NULL);

	LWLockAcquire(&tabentry->header.lock, LW_EXCLUSIVE);
	tabentry->n_live_tuples = livetuples;
	tabentry->n_dead_tuples = deadtuples;

	/*
	 * If commanded, reset changes_since_analyze to zero.  This forgets any
	 * changes that were committed while the ANALYZE was in progress, but we
	 * have no good way to estimate how many of those there were.
	 */
	if (resetcounter)
		tabentry->changes_since_analyze = 0;

	if (IsAutoVacuumWorkerProcess())
	{
		tabentry->autovac_analyze_timestamp = GetCurrentTimestamp();
		tabentry->autovac_analyze_count++;
	}
	else
	{
		tabentry->analyze_timestamp = GetCurrentTimestamp();
		tabentry->analyze_count++;
	}
	LWLockRelease(&tabentry->header.lock);
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

/* ----------
 * pgstat_report_replslot() -
 *
 *	Report replication slot activity.
 * ----------
 */
void
pgstat_report_replslot(const char *slotname,
					   int spilltxns, int spillcount, int spillbytes,
					   int streamtxns, int streamcount, int streambytes)
{
	PgStat_ReplSlot *shent;
	int				 i;
	bool			 found;

	if (!area)
		return;

	for (i = 0 ; i < max_replication_slots ; i++)
	{
		if (strcmp(NameStr(ReplicationSlotCtl->replication_slots[i].data.name),
				   slotname) == 0)
			break;

	}

	/*
	 * the slot should have been removed. just ignore it.  We create the entry
	 * for the slot with this name next time.
	 */
	if (i == max_replication_slots)
		return;

	shent = (PgStat_ReplSlot *)
		get_stat_entry(PGSTAT_TYPE_REPLSLOT,
					   MyDatabaseId, i, false, true, &found);

	/* Clear the counters and reset dropped when we reuse it */
	LWLockAcquire(&shent->header.lock, LW_EXCLUSIVE);
	if (shent->header.dropped || !found)
	{
		memset(&shent->spill_txns, 0,
			   sizeof(PgStat_ReplSlot) - offsetof(PgStat_ReplSlot, spill_txns));
		strlcpy(shent->slotname, slotname, NAMEDATALEN);
		shent->header.dropped = false;
	}

	shent->spill_txns += spilltxns;
	shent->spill_count += spillcount;
	shent->spill_bytes += spillbytes;
	shent->stream_txns += streamtxns;
	shent->stream_count += streamcount;
	shent->stream_bytes += streambytes;
	LWLockRelease(&shent->header.lock);
}

/* ----------
 * pgstat_report_replslot_drop() -
 *
 *	Tell the collector about dropping the replication slot.
 * ----------
 */
void
pgstat_report_replslot_drop(const char *slotname)
{
	int i;
	PgStat_ReplSlot *shent;

	Assert(area);
	if (!area)
		return;

	for (i = 0 ; i < max_replication_slots ; i++)
	{
		if (strcmp(NameStr(ReplicationSlotCtl->replication_slots[i].data.name),
				   slotname) == 0)
			break;

	}

	/*  XXX: maybe the slot has been removed. just ignore it. */
	if (i == max_replication_slots)
		return;
	
	shent = (PgStat_ReplSlot *)
		get_stat_entry(PGSTAT_TYPE_REPLSLOT,
					   MyDatabaseId, i, false, false, NULL);

	if (shent && !shent->header.dropped)
	{
		LWLockAcquire(&shent->header.lock, LW_EXCLUSIVE);
		shent->header.dropped = true;
		LWLockRelease(&shent->header.lock);
	}
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
	PgStat_BackendFunctionEntry *htabent;
	bool		found;

	if (pgstat_track_functions <= fcinfo->flinfo->fn_stats)
	{
		/* stats not wanted */
		fcu->fs = NULL;
		return;
	}

	htabent = (PgStat_BackendFunctionEntry *)
		get_local_stat_entry(PGSTAT_TYPE_FUNCTION, MyDatabaseId,
							 fcinfo->flinfo->fn_oid, true, &found);

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
	PgStat_BackendFunctionEntry *ent;

	ent = (PgStat_BackendFunctionEntry *)
		get_local_stat_entry(PGSTAT_TYPE_FUNCTION, MyDatabaseId,
							 func_id, false, NULL);

	return ent;
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
	if (!RELKIND_HAS_STORAGE(relkind))
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
	if (rel->pgstat_info != NULL)
		return;

	/* Else find or make the PgStat_TableStatus entry, and update link */
	rel->pgstat_info = get_local_tabstat_entry(rel_id, rel->rd_rel->relisshared);
	/* mark this relation as the owner */

	/* don't allow link a stats to multiple relcache entries */
	Assert (rel->pgstat_info->relation == NULL);
	rel->pgstat_info->relation = rel;
}

/*
 * pgstat_delinkstats() -
 *
 *  Break the mutual link between a relcache entry and a local stats entry.
 *  This must be called always when one end of the link is removed.
 */
void
pgstat_delinkstats(Relation rel)
{
	/* remove the link to stats info if any */
	if (rel && rel->pgstat_info)
	{
		/* ilnk sanity check */
		Assert (rel->pgstat_info->relation == rel);
		rel->pgstat_info->relation = NULL;
		rel->pgstat_info = NULL;
	}
}

/*
 * find_tabstat_entry - find any existing PgStat_TableStatus entry for rel
 *
 *  Find any existing PgStat_TableStatus entry for rel_id in the current
 *  database. If not found, try finding from shared tables.
 *
 *  If no entry found, return NULL, don't create a new one
 * ----------
 */
PgStat_TableStatus *
find_tabstat_entry(Oid rel_id)
{
	PgStat_TableStatus *ent;

	ent = (PgStat_TableStatus *)
		get_local_stat_entry(PGSTAT_TYPE_TABLE, MyDatabaseId, rel_id,
							 false, NULL);
	if (!ent)
		ent = (PgStat_TableStatus *)
			get_local_stat_entry(PGSTAT_TYPE_TABLE, InvalidOid, rel_id,
								 false, NULL);

	return ent;
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
 * pgstat_fetch_stat_dbentry() -
 *
 *	Find database stats entry on backends in a palloc'ed memory.
 *
 *  The returned entry is stored in static memory so the content is valid until
 *	the next call of the same function for the different database.
 * ----------
 */
PgStat_StatDBEntry *
pgstat_fetch_stat_dbentry(Oid dbid)
{
	PgStat_StatDBEntry *shent;

	/* should be called from backends */
	Assert(IsUnderPostmaster);

	/* the simple cache doesn't work properly for InvalidOid */
	Assert(dbid != InvalidOid);

	/* Return cached result if it is valid. */
	if (cached_dbent_key.databaseid == dbid)
		return &cached_dbent;

	shent = (PgStat_StatDBEntry *)
		get_stat_entry(PGSTAT_TYPE_DB, dbid, InvalidOid, true, false, NULL);

	if (!shent)
		return NULL;

	LWLockAcquire(&shent->header.lock, LW_SHARED);
	memcpy(&cached_dbent, shent, sizeof(PgStat_StatDBEntry));
	LWLockRelease(&shent->header.lock);

	/* remember the key for the cached entry */
	cached_dbent_key.databaseid = dbid;

	return &cached_dbent;
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

	tabentry = pgstat_fetch_stat_tabentry_extended(false, relid);
	if (tabentry != NULL)
		return tabentry;

	/*
	 * If we didn't find it, maybe it's a shared table.
	 */
	tabentry = pgstat_fetch_stat_tabentry_extended(true, relid);
	return tabentry;
}


/* ----------
 * pgstat_fetch_stat_tabentry_extended() -
 *
 *	Find table stats entry on backends in dbent. The returned entry is stored
 *	in static memory so the content is valid until the next call of the same
 *	function for the different table.
 */
PgStat_StatTabEntry *
pgstat_fetch_stat_tabentry_extended(bool shared, Oid reloid)
{
	PgStat_StatTabEntry *shent;
	Oid			dboid = (shared ? InvalidOid : MyDatabaseId);

	/* should be called from backends */
	Assert(IsUnderPostmaster);

	/* the simple cache doesn't work properly for the InvalidOid */
	Assert(reloid != InvalidOid);

	/* Return cached result if it is valid. */
	if (cached_tabent_key.databaseid == dboid &&
		cached_tabent_key.objectid == reloid)
		return &cached_tabent;

	shent = (PgStat_StatTabEntry *)
		get_stat_entry(PGSTAT_TYPE_TABLE, dboid, reloid, true, false, NULL);

	if (!shent)
		return NULL;

	LWLockAcquire(&shent->header.lock, LW_SHARED);
	memcpy(&cached_tabent, shent, sizeof(PgStat_StatTabEntry));
	LWLockRelease(&shent->header.lock);

	/* remember the key for the cached entry */
	cached_tabent_key.databaseid = dboid;
	cached_tabent_key.objectid = reloid;

	return &cached_tabent;
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
 *
 *  The returned entry is stored in static memory so the content is valid until
 *	the next call of the same function for the different function id.
 * ----------
 */
PgStat_StatFuncEntry *
pgstat_fetch_stat_funcentry(Oid func_id)
{
	PgStat_StatFuncEntry *shent;
	Oid	dboid = MyDatabaseId;

	/* should be called from backends */
	Assert(IsUnderPostmaster);

	/* the simple cache doesn't work properly for the InvalidOid */
	Assert(func_id != InvalidOid);

	/* Return cached result if it is valid. */
	if (cached_funcent_key.databaseid == dboid &&
		cached_funcent_key.objectid == func_id)
		return &cached_funcent;

	shent = (PgStat_StatFuncEntry *)
		get_stat_entry(PGSTAT_TYPE_FUNCTION, dboid, func_id, true, false,
					   NULL);

	if (!shent)
		return NULL;

	LWLockAcquire(&shent->header.lock, LW_SHARED);
	memcpy(&cached_funcent, shent, sizeof(PgStat_StatFuncEntry));
	LWLockRelease(&shent->header.lock);

	/* remember the key for the cached entry */
	cached_funcent_key.databaseid = dboid;
	cached_funcent_key.objectid = func_id;

	return &cached_funcent;
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
 * pgstat_get_stat_timestamp() -
 *
 *  Returns the last update timstamp of global staticstics.
 */
TimestampTz
pgstat_get_stat_timestamp(void)
{
	return (TimestampTz) pg_atomic_read_u64(&StatsShmem->stats_timestamp);
}

/*
 * ---------
 * pgstat_fetch_stat_archiver() -
 *
 *	Support function for the SQL-callable pgstat* functions.  The returned
 *  entry is stored in static memory so the content is valid until the next
 *  call.
 * ---------
 */
PgStat_Archiver *
pgstat_fetch_stat_archiver(void)
{
	PgStat_Archiver reset;
	PgStat_Archiver *reset_shared = &StatsShmem->archiver_reset_offset;
	PgStat_Archiver *shared = &StatsShmem->archiver_stats;
	PgStat_Archiver *cached = &cached_archiverstats;

	pgstat_copy_global_stats(cached, shared, sizeof(PgStat_Archiver),
							 &StatsShmem->archiver_changecount);

	LWLockAcquire(StatsLock, LW_SHARED);
	memcpy(&reset, reset_shared, sizeof(PgStat_Archiver));
	LWLockRelease(StatsLock);

	/* compensate by reset offsets */
	if (cached->archived_count == reset.archived_count)
	{
		cached->last_archived_wal[0] = 0;
		cached->last_archived_timestamp = 0;
	}
	cached->archived_count -= reset.archived_count;

	if (cached->failed_count == reset.failed_count)
	{
		cached->last_failed_wal[0] = 0;
		cached->last_failed_timestamp = 0;
	}
	cached->failed_count -= reset.failed_count;

	cached->stat_reset_timestamp = reset.stat_reset_timestamp;

	cached_archiverstats_is_valid = true;

	return &cached_archiverstats;
}


/*
 * ---------
 * pgstat_fetch_stat_bgwriter() -
 *
 *	Support function for the SQL-callable pgstat* functions.  The returned
 *  entry is stored in static memory so the content is valid until the next
 *  call.
 * ---------
 */
PgStat_BgWriter *
pgstat_fetch_stat_bgwriter(void)
{
	PgStat_BgWriter reset;
	PgStat_BgWriter *reset_shared = &StatsShmem->bgwriter_reset_offset;
	PgStat_BgWriter *shared = &StatsShmem->bgwriter_stats;
	PgStat_BgWriter *cached = &cached_bgwriterstats;

	pgstat_copy_global_stats(cached, shared, sizeof(PgStat_BgWriter),
							 &StatsShmem->bgwriter_changecount);

	LWLockAcquire(StatsLock, LW_SHARED);
	memcpy(&reset, reset_shared, sizeof(PgStat_BgWriter));
	LWLockRelease(StatsLock);

	/* compensate by reset offsets */
	cached->buf_written_clean -= reset.buf_written_clean;
	cached->maxwritten_clean  -= reset.maxwritten_clean;
	cached->buf_alloc		  -= reset.buf_alloc;
	cached->stat_reset_timestamp = reset.stat_reset_timestamp;

	cached_bgwriterstats_is_valid = true;

	return &cached_bgwriterstats;
}

/*
 * ---------
 * pgstat_fetch_stat_checkpinter() -
 *
 *	Support function for the SQL-callable pgstat* functions.  The returned
 *  entry is stored in static memory so the content is valid until the next
 *  call.
 * ---------
 */
PgStat_CheckPointer *
pgstat_fetch_stat_checkpointer(void)
{
	PgStat_CheckPointer reset;
	PgStat_CheckPointer *reset_shared = &StatsShmem->checkpointer_reset_offset;
	PgStat_CheckPointer *shared = &StatsShmem->checkpointer_stats;
	PgStat_CheckPointer *cached = &cached_checkpointerstats;

	pgstat_copy_global_stats(cached, shared, sizeof(PgStat_CheckPointer),
							 &StatsShmem->checkpointer_changecount);

	LWLockAcquire(StatsLock, LW_SHARED);
	memcpy(&reset, reset_shared, sizeof(PgStat_CheckPointer));
	LWLockRelease(StatsLock);

	/* compensate by reset offsets */
	cached->timed_checkpoints       -= reset.timed_checkpoints;
	cached->requested_checkpoints   -= reset.requested_checkpoints;
	cached->buf_written_checkpoints -= reset.buf_written_checkpoints;
	cached->buf_written_backend     -= reset.buf_written_backend;
	cached->buf_fsync_backend       -= reset.buf_fsync_backend;
	cached->checkpoint_write_time   -= reset.checkpoint_write_time;
	cached->checkpoint_sync_time    -= reset.checkpoint_sync_time;

	cached_checkpointerstats_is_valid = true;

	return &cached_checkpointerstats;
}

/*
 * ---------
 * pgstat_fetch_stat_wal() -
 *
 *	Support function for the SQL-callable pgstat* functions. The returned entry
 *  is stored in static memory so the content is valid until the next
 *  call.
 * ---------
 */
PgStat_Wal *
pgstat_fetch_stat_wal(void)
{
	if (!cached_walstats_is_valid)
	{
		LWLockAcquire(StatsLock, LW_SHARED);
		memcpy(&cached_walstats, &StatsShmem->wal_stats, sizeof(PgStat_Wal));
		LWLockRelease(StatsLock);
	}

	cached_walstats_is_valid = true;

	return &cached_walstats;
}

/*
 * ---------
 * pgstat_fetch_slru() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	a pointer to the slru statistics struct.
 * ---------
 */
PgStat_SLRUStats *
pgstat_fetch_slru(void)
{
	size_t size = sizeof(PgStat_SLRUStats) * SLRU_NUM_ELEMENTS;

	for (;;)
	{
		uint32 before_count;
		uint32 after_count;

		pg_read_barrier();
		before_count = pg_atomic_read_u32(&StatsShmem->slru_changecount);
		memcpy(&cached_slrustats, &StatsShmem->slru_stats, size);
		after_count = pg_atomic_read_u32(&StatsShmem->slru_changecount);

		if (before_count == after_count && (before_count & 1) == 0)
			break;

		CHECK_FOR_INTERRUPTS();
	}

	cached_slrustats_is_valid = true;

	return &cached_slrustats;
}

/*
 * ---------
 * pgstat_fetch_replslot() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	a pointer to the replication slot statistics struct and sets the
 *	number of entries in nslots_p.
 * ---------
 */
PgStat_ReplSlot *
pgstat_fetch_replslot(int *nslots_p)
{

	if (cached_replslotstats == NULL)
	{
		cached_replslotstats = (PgStat_ReplSlot *)
			MemoryContextAlloc(pgStatCacheContext,
							   sizeof(PgStat_ReplSlot) * max_replication_slots);
	}

	if (n_cached_replslotstats < 0)
	{
		int n = 0;
		int i;

		for (i = 0 ; i < max_replication_slots ; i++)
		{
			PgStat_ReplSlot *shent = (PgStat_ReplSlot *)
				get_stat_entry(PGSTAT_TYPE_REPLSLOT, MyDatabaseId, i,
							   false, false, NULL);
			if (shent && !shent->header.dropped)
			{
				memcpy(cached_replslotstats[n++].slotname,
					   shent->slotname,
					   sizeof(PgStat_ReplSlot) -
					   offsetof(PgStat_ReplSlot, slotname));
			}
		}

		n_cached_replslotstats = n;
	}

	*nslots_p = n_cached_replslotstats;
	return cached_replslotstats;
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
	if (MyProcPort && MyProcPort->ssl_in_use)
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
	 * We need to clean up temporary slots before detaching shared statistics
	 * so that the statistics for temporary slots are properly removed.
	 */
	if (MyReplicationSlot != NULL)
		ReplicationSlotRelease();

	ReplicationSlotCleanup();

	/*
	 * Clear my status entry, following the protocol of bumping st_changecount
	 * before and after.  We use a volatile pointer here to ensure the
	 * compiler doesn't try to get cute.
	 */
	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

	beentry->st_procpid = 0;	/* mark invalid */

	PGSTAT_END_WRITE_ACTIVITY(beentry);

	cleanup_dropped_stats_entries();

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
		case WAIT_EVENT_READING_STATS_FILE:
			event_name = "ReadingStatsFile";
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
		case WAIT_EVENT_EXECUTE_GATHER:
			event_name = "ExecuteGather";
			break;
		case WAIT_EVENT_HASH_BATCH_ALLOCATE:
			event_name = "HashBatchAllocate";
			break;
		case WAIT_EVENT_HASH_BATCH_ELECT:
			event_name = "HashBatchElect";
			break;
		case WAIT_EVENT_HASH_BATCH_LOAD:
			event_name = "HashBatchLoad";
			break;
		case WAIT_EVENT_HASH_BUILD_ALLOCATE:
			event_name = "HashBuildAllocate";
			break;
		case WAIT_EVENT_HASH_BUILD_ELECT:
			event_name = "HashBuildElect";
			break;
		case WAIT_EVENT_HASH_BUILD_HASH_INNER:
			event_name = "HashBuildHashInner";
			break;
		case WAIT_EVENT_HASH_BUILD_HASH_OUTER:
			event_name = "HashBuildHashOuter";
			break;
		case WAIT_EVENT_HASH_GROW_BATCHES_ALLOCATE:
			event_name = "HashGrowBatchesAllocate";
			break;
		case WAIT_EVENT_HASH_GROW_BATCHES_DECIDE:
			event_name = "HashGrowBatchesDecide";
			break;
		case WAIT_EVENT_HASH_GROW_BATCHES_ELECT:
			event_name = "HashGrowBatchesElect";
			break;
		case WAIT_EVENT_HASH_GROW_BATCHES_FINISH:
			event_name = "HashGrowBatchesFinish";
			break;
		case WAIT_EVENT_HASH_GROW_BATCHES_REPARTITION:
			event_name = "HashGrowBatchesRepartition";
			break;
		case WAIT_EVENT_HASH_GROW_BUCKETS_ALLOCATE:
			event_name = "HashGrowBucketsAllocate";
			break;
		case WAIT_EVENT_HASH_GROW_BUCKETS_ELECT:
			event_name = "HashGrowBucketsElect";
			break;
		case WAIT_EVENT_HASH_GROW_BUCKETS_REINSERT:
			event_name = "HashGrowBucketsReinsert";
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
		case WAIT_EVENT_PROC_SIGNAL_BARRIER:
			event_name = "ProcSignalBarrier";
			break;
		case WAIT_EVENT_PROMOTE:
			event_name = "Promote";
			break;
		case WAIT_EVENT_RECOVERY_CONFLICT_SNAPSHOT:
			event_name = "RecoveryConflictSnapshot";
			break;
		case WAIT_EVENT_RECOVERY_CONFLICT_TABLESPACE:
			event_name = "RecoveryConflictTablespace";
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
		case WAIT_EVENT_XACT_GROUP_UPDATE:
			event_name = "XactGroupUpdate";
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
		case WAIT_EVENT_BASEBACKUP_READ:
			event_name = "BaseBackupRead";
			break;
		case WAIT_EVENT_BUFFILE_READ:
			event_name = "BufFileRead";
			break;
		case WAIT_EVENT_BUFFILE_WRITE:
			event_name = "BufFileWrite";
			break;
		case WAIT_EVENT_BUFFILE_TRUNCATE:
			event_name = "BufFileTruncate";
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
		case WAIT_EVENT_LOGICAL_CHANGES_READ:
			event_name = "LogicalChangesRead";
			break;
		case WAIT_EVENT_LOGICAL_CHANGES_WRITE:
			event_name = "LogicalChangesWrite";
			break;
		case WAIT_EVENT_LOGICAL_SUBXACT_READ:
			event_name = "LogicalSubxactRead";
			break;
		case WAIT_EVENT_LOGICAL_SUBXACT_WRITE:
			event_name = "LogicalSubxactWrite";
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
	uint32	before_count PG_USED_FOR_ASSERTS_ONLY;
	uint32	after_count PG_USED_FOR_ASSERTS_ONLY;


	START_CRIT_SECTION();
	before_count =
		pg_atomic_fetch_add_u32(&StatsShmem->archiver_changecount, 1);
	Assert((before_count & 1) == 0);

	if (failed)
	{
		++StatsShmem->archiver_stats.failed_count;
		memcpy(&StatsShmem->archiver_stats.last_failed_wal, xlog,
			   sizeof(StatsShmem->archiver_stats.last_failed_wal));
		StatsShmem->archiver_stats.last_failed_timestamp = now;
	}
	else
	{
		++StatsShmem->archiver_stats.archived_count;
		memcpy(&StatsShmem->archiver_stats.last_archived_wal, xlog,
			   sizeof(StatsShmem->archiver_stats.last_archived_wal));
		StatsShmem->archiver_stats.last_archived_timestamp = now;
	}

	after_count =
		pg_atomic_fetch_add_u32(&StatsShmem->archiver_changecount, 1);
	Assert(after_count == before_count + 1);
	END_CRIT_SECTION();
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
	static const PgStat_BgWriter all_zeroes;
	PgStat_BgWriter *s = &StatsShmem->bgwriter_stats;
	PgStat_BgWriter *l = &BgWriterStats;
	uint32	before_count PG_USED_FOR_ASSERTS_ONLY;
	uint32	after_count PG_USED_FOR_ASSERTS_ONLY;

	/*
	 * This function can be called even if nothing at all has happened. In
	 * this case, avoid taking lock for a completely empty stats.
	 */
	if (memcmp(&BgWriterStats, &all_zeroes, sizeof(PgStat_BgWriter)) == 0)
		return;

	START_CRIT_SECTION();
	before_count =
		pg_atomic_fetch_add_u32(&StatsShmem->bgwriter_changecount, 1);
	Assert((before_count & 1) == 0);

	s->buf_written_clean += l->buf_written_clean;
	s->maxwritten_clean += l->maxwritten_clean;
	s->buf_alloc += l->buf_alloc;

	after_count =
		pg_atomic_fetch_add_u32(&StatsShmem->bgwriter_changecount, 1);
	Assert(after_count == before_count + 1);
	END_CRIT_SECTION();

	/*
	 * Clear out the statistics buffer, so it can be re-used.
	 */
	MemSet(&BgWriterStats, 0, sizeof(BgWriterStats));
}

/* ----------
 * pgstat_report_checkpointer() -
 *
 *		Report checkpointer statistics
 * ----------
 */
void
pgstat_report_checkpointer(void)
{
	/* We assume this initializes to zeroes */
	static const PgStat_CheckPointer all_zeroes;
	PgStat_CheckPointer *s = &StatsShmem->checkpointer_stats;
	PgStat_CheckPointer *l = &CheckPointerStats;
	uint32	before_count PG_USED_FOR_ASSERTS_ONLY;
	uint32	after_count PG_USED_FOR_ASSERTS_ONLY;

	/*
	 * This function can be called even if nothing at all has happened. In
	 * this case, avoid taking lock for a completely empty stats.
	 */
	if (memcmp(&BgWriterStats, &all_zeroes, sizeof(PgStat_CheckPointer)) == 0)
		return;

	START_CRIT_SECTION();
	before_count =
		pg_atomic_fetch_add_u32(&StatsShmem->checkpointer_changecount, 1);
	Assert((before_count & 1) == 0);

	s->timed_checkpoints += l->timed_checkpoints;
	s->requested_checkpoints += l->requested_checkpoints;
	s->checkpoint_write_time += l->checkpoint_write_time;
	s->checkpoint_sync_time += l->checkpoint_sync_time;
	s->buf_written_checkpoints += l->buf_written_checkpoints;
	s->buf_written_backend += l->buf_written_backend;
	s->buf_fsync_backend += l->buf_fsync_backend;

	after_count =
		pg_atomic_fetch_add_u32(&StatsShmem->checkpointer_changecount, 1);
	Assert(after_count == before_count + 1);
	END_CRIT_SECTION();
	/*
	 * Clear out the statistics buffer, so it can be re-used.
	 */
	MemSet(&BgWriterStats, 0, sizeof(BgWriterStats));
}

/* ----------
 * pgstat_report_wal() -
 *
 *		Report WAL statistics
 * ----------
 */
void
pgstat_report_wal(void)
{
	flush_walstat(false);
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
	PgStat_StatDBEntry *dbentry;
	bool		found;

	/*
	 * Find an entry or create a new one.
	 */
	dbentry = (PgStat_StatDBEntry *)
		get_local_stat_entry(PGSTAT_TYPE_DB, dbid, InvalidOid,
							 true, &found);

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
	PgStat_TableStatus *tabentry;
	bool		found;

	tabentry = (PgStat_TableStatus *)
		get_local_stat_entry(PGSTAT_TYPE_TABLE,
							 isshared ? InvalidOid : MyDatabaseId,
							 rel_id, true, &found);

	return tabentry;
}

/* ----------
 * pgstat_write_statsfile() -
 *		Write the global statistics file, as well as DB files.
 *
 * This function is called in the last process that is accessing the shared
 * stats so locking is not required.
 * ----------
 */
static void
pgstat_write_statsfile(void)
{
	FILE	   *fpout;
	int32		format_id;
	const char *tmpfile = PGSTAT_STAT_PERMANENT_TMPFILE;
	const char *statfile = PGSTAT_STAT_PERMANENT_FILENAME;
	int			rc;
	dshash_seq_status hstat;
	PgStatHashEntry *ps;

	/* stats is not initialized yet. just return. */
	if (StatsShmem->stats_dsa_handle == DSM_HANDLE_INVALID)
		return;

	/* this is the last process that is accesing the shared stats */
#ifdef USE_ASSERT_CHECKING
	LWLockAcquire(StatsLock, LW_SHARED);
	Assert(StatsShmem->refcount == 0);
	LWLockRelease(StatsLock);
#endif

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
	 * Set the timestamp of the stats file.
	 */
	pg_atomic_write_u64(&StatsShmem->stats_timestamp, GetCurrentTimestamp());

	/*
	 * Write the file header --- currently just a format ID.
	 */
	format_id = PGSTAT_FILE_FORMAT_ID;
	rc = fwrite(&format_id, sizeof(format_id), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Write bgwriter global stats struct
	 */
	rc = fwrite(&StatsShmem->bgwriter_stats, sizeof(PgStat_BgWriter), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Write checkpointer global stats struct
	 */
	rc = fwrite(&StatsShmem->checkpointer_stats, sizeof(PgStat_CheckPointer), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Write archiver global stats struct
	 */
	rc = fwrite(&StatsShmem->archiver_stats, sizeof(PgStat_Archiver), 1,
				fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Write WAL global stats struct
	 */
	rc = fwrite(&StatsShmem->wal_stats, sizeof(PgStat_Wal), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Write SLRU stats struct
	 */
	rc = fwrite(&StatsShmem->slru_stats, sizeof(PgStatSharedSLRUStats), 1,
				fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Walk through the stats entry
	 */
	dshash_seq_init(&hstat, pgStatSharedHash, false);
	while ((ps = dshash_seq_next(&hstat)) != NULL)
	{
		PgStat_StatEntryHeader *shent;
		size_t					len;

		CHECK_FOR_INTERRUPTS();

		shent = (PgStat_StatEntryHeader *)dsa_get_address(area, ps->body);

		/* we may have some "dropped" entries not yet removed, skip them */
		if (shent->dropped)
			continue;

		/* Make DB's timestamp consistent with the global stats */
		if (ps->key.type == PGSTAT_TYPE_DB)
		{
			PgStat_StatDBEntry *dbentry = (PgStat_StatDBEntry *) shent;

			dbentry->stats_timestamp =
				(TimestampTz) pg_atomic_read_u64(&StatsShmem->stats_timestamp);
		}

		fputc('S', fpout);
		rc = fwrite(&ps->key, sizeof(PgStatHashKey), 1, fpout);

		/* Write except the header part of the etnry */
		len = PGSTAT_SHENT_BODY_LEN(ps->key.type);
		rc = fwrite(PGSTAT_SHENT_BODY(shent), len, 1, fpout);
		(void) rc;				/* we'll check for error with ferror */
	}
	dshash_seq_term(&hstat);

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
 * pgstat_read_statsfile() -
 *
 *	Reads in existing activity statistics file into the shared stats hash.
 *
 * This function is called in the only process that is accessing the shared
 * stats so locking is not required.
 * ----------
 */
static void
pgstat_read_statsfile(void)
{
	FILE	   *fpin;
	int32		format_id;
	bool		found;
	const char *statfile = PGSTAT_STAT_PERMANENT_FILENAME;
	char		tag;

	/* shouldn't be called from postmaster */
	Assert(IsUnderPostmaster);

	/* this is the only process that is accesing the shared stats */
#ifdef USE_ASSERT_CHECKING
	LWLockAcquire(StatsLock, LW_SHARED);
	Assert(StatsShmem->refcount == 1);
	LWLockRelease(StatsLock);
#endif

	elog(DEBUG2, "reading stats file \"%s\"", statfile);

	/*
	 * Set the current timestamp (will be kept only in case we can't load an
	 * existing statsfile).
	 */
	StatsShmem->bgwriter_stats.stat_reset_timestamp = GetCurrentTimestamp();
	StatsShmem->archiver_stats.stat_reset_timestamp =
		StatsShmem->bgwriter_stats.stat_reset_timestamp;

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
	 * Read bgwiter stats struct
	 */
	if (fread(&StatsShmem->bgwriter_stats, 1, sizeof(PgStat_BgWriter), fpin) !=
		sizeof(PgStat_BgWriter))
	{
		ereport(LOG,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		MemSet(&StatsShmem->bgwriter_stats, 0, sizeof(PgStat_BgWriter));
		goto done;
	}

	/*
	 * Read checkpointer stats struct
	 */
	if (fread(&StatsShmem->checkpointer_stats, 1, sizeof(PgStat_CheckPointer), fpin) !=
		sizeof(PgStat_CheckPointer))
	{
		ereport(LOG,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		MemSet(&StatsShmem->checkpointer_stats, 0, sizeof(PgStat_CheckPointer));
		goto done;
	}

	/*
	 * Read archiver stats struct
	 */
	if (fread(&StatsShmem->archiver_stats, 1, sizeof(PgStat_Archiver),
			  fpin) != sizeof(PgStat_Archiver))
	{
		ereport(LOG,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		MemSet(&StatsShmem->archiver_stats, 0, sizeof(PgStat_Archiver));
		goto done;
	}

	/*
	 * Read WAL stats struct
	 */
	if (fread(&StatsShmem->wal_stats, 1, sizeof(PgStat_Wal), fpin)
		!= sizeof(PgStat_Wal))
	{
		ereport(LOG,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		MemSet(&StatsShmem->wal_stats, 0, sizeof(PgStat_Wal));
		goto done;
	}

	/*
	 * Read SLRU stats struct
	 */
	if (fread(&StatsShmem->slru_stats, 1, sizeof(PgStatSharedSLRUStats),
			  fpin) != sizeof(PgStatSharedSLRUStats))
	{
		ereport(LOG,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		goto done;
	}

	/*
	 * We found an existing activity statistics file. Read it and put all the
	 * hash table entries into place.
	 */
	while ((tag = fgetc(fpin)) == 'S')
	{
		PgStatHashKey		key;
		PgStat_StatEntryHeader *p;
		size_t				len;

		CHECK_FOR_INTERRUPTS();

		if (fread(&key, 1, sizeof(key), fpin) != sizeof(key))
		{
			ereport(LOG,
					(errmsg("corrupted statistics file \"%s\"", statfile)));
			goto done;
		}

		p = get_stat_entry(key.type, key.databaseid, key.objectid,
							  false, true, &found);

		/* don't allow duplicate entries */
		if (found)
		{
			ereport(LOG,
					(errmsg("corrupted statistics file \"%s\"",
							statfile)));
			goto done;
		}

		/* Avoid overwriting header part */
		len = PGSTAT_SHENT_BODY_LEN(key.type);

		if (fread(PGSTAT_SHENT_BODY(p), 1, len, fpin) != len)
		{
			ereport(LOG,
					(errmsg("corrupted statistics file \"%s\"",	statfile)));
			goto done;
		}
	}

	if (tag !=  'E')
	{
		ereport(LOG,
				(errmsg("corrupted statistics file \"%s\"",
						statfile)));
		goto done;
	}

done:
	FreeFile(fpin);

	elog(DEBUG2, "removing permanent stats file \"%s\"", statfile);
	unlink(statfile);

	return;
}

/* ----------
 * pgstat_setup_memcxt() -
 *
 *	Create pgStatLocalContext if not already done.
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

	if (!pgStatCacheContext)
		pgStatCacheContext =
			AllocSetContextCreate(CacheMemoryContext,
								  "Activity statistics",
								  ALLOCSET_SMALL_SIZES);
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

	/* Invalidate the simple cache keys */
	cached_dbent_key = stathashkey_zero;
	cached_tabent_key = stathashkey_zero;
	cached_funcent_key = stathashkey_zero;
	cached_archiverstats_is_valid = false;
	cached_bgwriterstats_is_valid = false;
	cached_checkpointerstats_is_valid = false;
	cached_walstats_is_valid = false;
	cached_slrustats_is_valid = false;
	n_cached_replslotstats = -1;
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

/*
 * pgstat_slru_index
 *
 * Determine index of entry for a SLRU with a given name. If there's no exact
 * match, returns index of the last "other" entry used for SLRUs defined in
 * external projects.
 */
int
pgstat_slru_index(const char *name)
{
	int			i;

	for (i = 0; i < SLRU_NUM_ELEMENTS; i++)
	{
		if (strcmp(slru_names[i], name) == 0)
			return i;
	}

	/* return index of the last entry (which is the "other" one) */
	return (SLRU_NUM_ELEMENTS - 1);
}

/*
 * pgstat_slru_name
 *
 * Returns SLRU name for an index. The index may be above SLRU_NUM_ELEMENTS,
 * in which case this returns NULL. This allows writing code that does not
 * know the number of entries in advance.
 */
const char *
pgstat_slru_name(int slru_idx)
{
	if (slru_idx < 0 || slru_idx >= SLRU_NUM_ELEMENTS)
		return NULL;

	return slru_names[slru_idx];
}

/*
 * slru_entry
 *
 * Returns pointer to entry with counters for given SLRU (based on the name
 * stored in SlruCtl as lwlock tranche name).
 */
static PgStat_SLRUStats *
slru_entry(int slru_idx)
{
	/*
	 * The postmaster should never register any SLRU statistics counts; if it
	 * did, the counts would be duplicated into child processes via fork().
	 */
	Assert(IsUnderPostmaster || !IsPostmasterEnvironment);

	Assert((slru_idx >= 0) && (slru_idx < SLRU_NUM_ELEMENTS));

	return &local_SLRUStats[slru_idx];
}

/*
 * SLRU statistics count accumulation functions --- called from slru.c
 */

void
pgstat_count_slru_page_zeroed(int slru_idx)
{
	slru_entry(slru_idx)->blocks_zeroed += 1;
	have_slrustats = true;
}

void
pgstat_count_slru_page_hit(int slru_idx)
{
	slru_entry(slru_idx)->blocks_hit += 1;
	have_slrustats = true;
}

void
pgstat_count_slru_page_exists(int slru_idx)
{
	slru_entry(slru_idx)->blocks_exists += 1;
	have_slrustats = true;
}

void
pgstat_count_slru_page_read(int slru_idx)
{
	slru_entry(slru_idx)->blocks_read += 1;
	have_slrustats = true;
}

void
pgstat_count_slru_page_written(int slru_idx)
{
	slru_entry(slru_idx)->blocks_written += 1;
	have_slrustats = true;
}

void
pgstat_count_slru_flush(int slru_idx)
{
	slru_entry(slru_idx)->flush += 1;
	have_slrustats = true;
}

void
pgstat_count_slru_truncate(int slru_idx)
{
	slru_entry(slru_idx)->truncate += 1;
	have_slrustats = true;
}
