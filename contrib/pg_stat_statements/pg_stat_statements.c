/*-------------------------------------------------------------------------
 *
 * pg_stat_statements.c
 *		Track statement planning and execution times as well as resource
 *		usage across a whole database cluster.
 *
 * Execution costs are totaled for each distinct source query, and kept in
 * shared memory as a custom pgstat kind (own_hash=true), giving this module
 * its own dedicated dshash table.  Query text is stored in a separate DSA
 * area (via DSM registry).
 *
 * Normalization of query constants is performed by the core query jumble
 * machinery (compute_query_id).  This module stores a "representative"
 * query string with constants replaced by parameter symbols ($n).
 *
 * Entry metadata (key, query text pointer, usage counter, timestamps) and
 * counters live together in the PgStatShared_Pgss body.  Backends accumulate
 * stats locally via the pending/flush infrastructure, flushing to shared
 * memory without contending on entry-level locks on the hot path.  All
 * locking (per-entry LWLock, dshash partition locks) is provided by the
 * core pgstat infrastructure.
 *
 * Locking strategy for entry creation:
 *
 * The hot path (existing entries) is lock-free: the backend holds a
 * cached local reference to the entry (or acquires one via a dshash
 * lookup on first access), then pending stats are accumulated in
 * backend-local memory without any external lock.
 *
 * New entries require an exclusive LWLock (pgss_shared->lock) to serialize
 * creation against concurrent eviction.  This lock is only taken when the
 * entry does not already exist (cache miss), which is rare in steady state.
 *
 * Eviction:
 *
 * When the live entry count reaches pgss_max, eviction is triggered
 * under the exclusive lock.  At most one eviction pass runs per
 * PGSS_EVICT_INTERVAL_MS (globally across all backends) to prevent
 * lock convoy effects under
 * high-churn workloads.  Between passes, backends that would need to
 * create a new entry simply skip recording.
 *
 * Each eviction pass scans all entries, sorts by last_access timestamp
 * (LRU, least recently used first), and drops the bottom 1/N (where
 * N = PGSS_EVICT_RATIO).  A grace period (PGSS_EVICT_GRACE_MS)
 * protects entries accessed recently from eviction, even if they rank among
 * the coldest.  This prevents a churn cycle where the same active
 * entries are evicted and immediately re-created, consuming eviction
 * capacity without admitting genuinely new queries.  Only entries that
 * are truly stale (not accessed recently) are evicted, which means they
 * are unlikely to be pinned by backends and can be freed immediately.
 *
 * Eviction decisions use a live entry count (incremented on creation,
 * decremented on eviction) rather than the physical dshash size, which
 * includes dropped-but-pinned entries awaiting GC.  This ensures
 * headroom is available immediately after eviction without waiting for
 * backends to release their refs.
 *
 * To avoid runaway growth of the dshash when long-running transactions
 * hold refs and prevent GC from freeing evicted entries, creation is
 * skipped unconditionally once the physical entry count reaches 2x
 * pgss_max.
 *
 * Copyright (c) 2008-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_stat_statements/pg_stat_statements.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/htup_details.h"
#include "access/parallel.h"
#include "catalog/pg_authid.h"
#include "common/hashfn.h"
#include "executor/instrument.h"
#include "funcapi.h"
#include "jit/jit.h"
#include "lib/dshash.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "nodes/queryjumble.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "storage/dsm_registry.h"
#include "tcop/utility.h"
#include "access/xact.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/dsa.h"
#include "utils/guc.h"
#include "utils/numeric.h"
#include "utils/pgstat_internal.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"

PG_MODULE_MAGIC_EXT(
					.name = "pg_stat_statements",
					.version = PG_VERSION
);

/* Custom pgstat kind ID */
#define PGSTAT_KIND_PGSS	25

/* Fraction of pgss_max to evict per pass (1/N) */
#define PGSS_EVICT_RATIO		10

/*
 * Minimum interval (ms) between eviction passes.  Limits lock hold time
 * under high-churn workloads to at most one scan+sort per this interval.
 */
#define PGSS_EVICT_INTERVAL_MS	1000

/*
 * Grace period (ms) protecting recently-accessed entries from eviction.
 * Entries whose last_access is within this window are skipped even if they
 * rank among the coldest.  Without this, entries that are still being
 * executed get evicted and immediately re-created, wasting eviction
 * capacity without making room for genuinely new queries.
 */
#define PGSS_EVICT_GRACE_MS		10000

/*
 * Extension version number, for supporting older extension versions' objects
 */
typedef enum pgssVersion
{
	PGSS_V1_0 = 0,
	PGSS_V1_1,
	PGSS_V1_2,
	PGSS_V1_3,
	PGSS_V1_8,
	PGSS_V1_9,
	PGSS_V1_10,
	PGSS_V1_11,
	PGSS_V1_12,
	PGSS_V1_13,
} pgssVersion;
typedef enum pgssStoreKind
{
	PGSS_INVALID = -1,

	/*
	 * PGSS_PLAN and PGSS_EXEC must be respectively 0 and 1 as they're used to
	 * reference the underlying values in the arrays in the Counters struct,
	 * and this order is required in pg_stat_statements_internal().
	 */
	PGSS_PLAN = 0,
	PGSS_EXEC,
} pgssStoreKind;

#define PGSS_NUMKIND (PGSS_EXEC + 1)

/*
 * Hashtable key that defines the identity of a tracked statement.
 * We separate queries by user and by database even if they are otherwise
 * identical.
 */
typedef struct pgssHashKey
{
	Oid			userid;			/* user OID */
	Oid			dbid;			/* database OID */
	int64		queryid;		/* query identifier */
	bool		toplevel;		/* query executed at top level */
} pgssHashKey;

/*
 * The actual stats counters kept within the custom pgstat kind.
 */
typedef struct pgssCounters
{
	int64		calls[PGSS_NUMKIND];	/* # of times planned/executed */
	double		total_time[PGSS_NUMKIND];	/* total planning/execution time,
											 * in msec */
	double		min_time[PGSS_NUMKIND]; /* minimum planning/execution time in
										 * msec since min/max reset */
	double		max_time[PGSS_NUMKIND]; /* maximum planning/execution time in
										 * msec since min/max reset */
	double		mean_time[PGSS_NUMKIND];	/* mean planning/execution time in
											 * msec */
	double		sum_var_time[PGSS_NUMKIND]; /* sum of variances in
											 * planning/execution time in msec */
	int64		rows;			/* total # of retrieved or affected rows */
	int64		shared_blks_hit;	/* # of shared buffer hits */
	int64		shared_blks_read;	/* # of shared disk blocks read */
	int64		shared_blks_dirtied;	/* # of shared disk blocks dirtied */
	int64		shared_blks_written;	/* # of shared disk blocks written */
	int64		local_blks_hit; /* # of local buffer hits */
	int64		local_blks_read;	/* # of local disk blocks read */
	int64		local_blks_dirtied; /* # of local disk blocks dirtied */
	int64		local_blks_written; /* # of local disk blocks written */
	int64		temp_blks_read; /* # of temp blocks read */
	int64		temp_blks_written;	/* # of temp blocks written */
	double		shared_blk_read_time;	/* time spent reading shared blocks,
										 * in msec */
	double		shared_blk_write_time;	/* time spent writing shared blocks,
										 * in msec */
	double		local_blk_read_time;	/* time spent reading local blocks, in
										 * msec */
	double		local_blk_write_time;	/* time spent writing local blocks, in
										 * msec */
	double		temp_blk_read_time; /* time spent reading temp blocks, in msec */
	double		temp_blk_write_time;	/* time spent writing temp blocks, in
										 * msec */
	double		usage;			/* usage factor */
	int64		wal_records;	/* # of WAL records generated */
	int64		wal_fpi;		/* # of WAL full page images generated */
	uint64		wal_bytes;		/* total amount of WAL generated in bytes */
	int64		wal_buffers_full;	/* # of times the WAL buffers became full */
	int64		jit_functions;	/* total number of JIT functions emitted */
	double		jit_generation_time;	/* total time to generate jit code */
	int64		jit_inlining_count; /* number of times inlining time has been
									 * > 0 */
	double		jit_deform_time;	/* total time to deform tuples in jit code */
	int64		jit_deform_count;	/* number of times deform time has been >
									 * 0 */

	double		jit_inlining_time;	/* total time to inline jit code */
	int64		jit_optimization_count; /* number of times optimization time
										 * has been > 0 */
	double		jit_optimization_time;	/* total time to optimize jit code */
	int64		jit_emission_count; /* number of times emission time has been
									 * > 0 */
	double		jit_emission_time;	/* total time to emit jit code */
	int64		parallel_workers_to_launch; /* # of parallel workers planned
											 * to be launched */
	int64		parallel_workers_launched;	/* # of parallel workers actually
											 * launched */
	int64		generic_plan_calls; /* number of calls using a generic plan */
	int64		custom_plan_calls;	/* number of calls using a custom plan */
} pgssCounters;

/*
 * Shared pgstat entry  - stored in the per-kind dshash (own_hash).
 * Contains both the stats counters and the entry metadata that was
 * previously stored in a separate dshash registry.
 */
typedef struct PgStatShared_Pgss
{
	PgStatShared_Common header;
	pgssHashKey key;
	dsa_pointer query_text;		/* DSA pointer to query text */
	int			query_len;		/* # of valid bytes in query string, or -1 */
	int			encoding;		/* query text encoding */
	TimestampTz stats_since;	/* timestamp of entry allocation */
	TimestampTz minmax_stats_since; /* timestamp of last min/max values reset */
	pg_atomic_uint32 usage;		/* hotness: decremented by sweep, incremented
								 * on access */
	pg_atomic_uint64 last_access;	/* statement start timestamp of last
									 * access, for LRU eviction */
	pgssCounters counters;
} PgStatShared_Pgss;

/*
 * Global shared state stored in DSM segment
 */
typedef struct pgssSharedState
{
	LWLock		lock;			/* protects entry creation and eviction */
	pg_atomic_uint64 dealloc;	/* total # of entries evicted */
	pg_atomic_uint64 skipped_entries;	/* # of entries skipped due to
										 * throttle */
	pg_atomic_uint64 stats_reset;	/* timestamp with all stats reset */

	/*
	 * Live entry count: tracks non-dropped entries.  We need this separate
	 * from pgstat_get_entry_count() because the core counter includes
	 * dropped-but-pinned entries still held by backend references.
	 */
	pg_atomic_uint64 live_entries;

	/* Timestamp of last eviction pass; limits to once per second globally */
	pg_atomic_uint64 last_eviction;
} pgssSharedState;

/* Backend-local pending entry */
typedef struct PgStat_PgssPending
{
	pgssHashKey key;
	bool		last_access_updated;
	pgssCounters counters;
} PgStat_PgssPending;


/*---- Local variables ----*/

/* Global shared state */
static pgssSharedState *pgss_shared = NULL;

/* Query text dsa area */
static dsa_area *pgss_qtext_dsa = NULL;

/* Per-kind pgstat dshash for this kind */
static dshash_table *pgss_hash = NULL;

/* Current nesting depth of planner/ExecutorRun/ProcessUtility calls */
static int	nesting_level = 0;

/* Saved hook values */
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

/*---- GUC variables ----*/

typedef enum
{
	PGSS_TRACK_NONE,			/* track no statements */
	PGSS_TRACK_TOP,				/* only top level statements */
	PGSS_TRACK_ALL,				/* all statements, including nested ones */
}			PGSSTrackLevel;

static const struct config_enum_entry track_options[] =
{
	{"none", PGSS_TRACK_NONE, false},
	{"top", PGSS_TRACK_TOP, false},
	{"all", PGSS_TRACK_ALL, false},
	{NULL, 0, false}
};

static int	pgss_max = 5000;	/* max # statements to track */
static int	pgss_track = PGSS_TRACK_TOP;	/* tracking level */
static bool pgss_track_utility = true;	/* whether to track utility commands */
static bool pgss_track_planning = false;	/* whether to track planning
											 * duration */
static bool pgss_save = true;	/* whether to save stats across shutdown XXX:
								 * this does not prevent stats being saved to
								 * the core stats file, should it? */
static int	pgss_query_text_memory = 4096;	/* in KB XXX: Should default be
											 * lower? */

#define pgss_enabled(level) \
	(!IsParallelWorker() && \
	(pgss_track == PGSS_TRACK_ALL || \
	(pgss_track == PGSS_TRACK_TOP && (level) == 0)))

/*---- Function declarations ----*/

PG_FUNCTION_INFO_V1(pg_stat_statements_reset);
PG_FUNCTION_INFO_V1(pg_stat_statements_reset_1_7);
PG_FUNCTION_INFO_V1(pg_stat_statements_reset_1_11);
PG_FUNCTION_INFO_V1(pg_stat_statements_1_2);
PG_FUNCTION_INFO_V1(pg_stat_statements_1_3);
PG_FUNCTION_INFO_V1(pg_stat_statements_1_8);
PG_FUNCTION_INFO_V1(pg_stat_statements_1_9);
PG_FUNCTION_INFO_V1(pg_stat_statements_1_10);
PG_FUNCTION_INFO_V1(pg_stat_statements_1_11);
PG_FUNCTION_INFO_V1(pg_stat_statements_1_12);
PG_FUNCTION_INFO_V1(pg_stat_statements_1_13);
PG_FUNCTION_INFO_V1(pg_stat_statements);
PG_FUNCTION_INFO_V1(pg_stat_statements_info);

static void pgss_post_parse_analyze(ParseState *pstate, Query *query,
									const JumbleState *jstate);
static PlannedStmt *pgss_planner(Query *parse,
								 const char *query_string,
								 int cursorOptions,
								 ParamListInfo boundParams,
								 ExplainState *es);
static void pgss_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pgss_ExecutorRun(QueryDesc *queryDesc,
							 ScanDirection direction,
							 uint64 count);
static void pgss_ExecutorFinish(QueryDesc *queryDesc);
static void pgss_ExecutorEnd(QueryDesc *queryDesc);
static void pgss_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
								bool readOnlyTree,
								ProcessUtilityContext context, ParamListInfo params,
								QueryEnvironment *queryEnv,
								DestReceiver *dest, QueryCompletion *qc);
static void pgss_store(const char *query, int64 queryId,
					   int query_location, int query_len,
					   pgssStoreKind kind,
					   double total_time, uint64 rows,
					   const BufferUsage *bufusage,
					   const WalUsage *walusage,
					   const struct JitInstrumentation *jitusage,
					   const JumbleState *jstate,
					   int parallel_workers_to_launch,
					   int parallel_workers_launched,
					   PlannedStmtOrigin planOrigin);
static void pgss_assign_query_text_memory(int newval, void *extra);
static void pgss_attach_shmem(void);
static TimestampTz entry_reset(Oid userid, Oid dbid, int64 queryid, bool minmax_only);
static inline uint64 pgss_hash_key(pgssHashKey *key);
static char *generate_normalized_query(const JumbleState *jstate,
									   const char *query,
									   int query_loc, int *query_len_p);

static void pg_stat_statements_internal(FunctionCallInfo fcinfo,
										pgssVersion api_version,
										bool showtext);

static bool pgss_flush_pending_cb(PgStat_EntryRef *entry_ref, bool nowait);
static void pgss_to_serialized_data(const PgStat_HashKey *key,
									const PgStatShared_Common *header,
									FILE *statfile);
static bool pgss_from_serialized_data(const PgStat_HashKey *key,
									  PgStatShared_Common *header,
									  FILE *statfile);

/*--------------------------------------------------------------------------
 * Custom pgstat kind definition
 *--------------------------------------------------------------------------
 */

static const PgStat_KindInfo pgss_kind_info = {
	.name = "pg_stat_statements",
	.fixed_amount = false,
	.write_to_file = true,
	.track_entry_count = true,
	.accessed_across_databases = true,
	.own_hash = true,
	.shared_size = sizeof(PgStatShared_Pgss),
	.shared_data_off = offsetof(PgStatShared_Pgss, counters),
	.shared_data_len = sizeof(pgssCounters),
	.pending_size = sizeof(PgStat_PgssPending),
	.init_backend_cb = pgss_attach_shmem,
	.flush_pending_cb = pgss_flush_pending_cb,
	.to_serialized_data = pgss_to_serialized_data,
	.from_serialized_data = pgss_from_serialized_data,
};

static inline uint64
pgss_hash_key(pgssHashKey *key)
{
	return hash_bytes_extended((const unsigned char *) key,
							   sizeof(pgssHashKey), 0);
}

static void
pgss_init_shmem(void *ptr, void *arg)
{
	pgssSharedState *state = (pgssSharedState *) ptr;

	LWLockInitialize(&state->lock, LWLockNewTrancheId("pg_stat_statements"));
	pg_atomic_init_u64(&state->dealloc, 0);
	pg_atomic_init_u64(&state->skipped_entries, 0);
	pg_atomic_init_u64(&state->stats_reset, (uint64) GetCurrentTimestamp());
	pg_atomic_init_u64(&state->live_entries, 0);
	pg_atomic_init_u64(&state->last_eviction, 0);
}

static void
pgss_assign_query_text_memory(int newval, void *extra)
{
	if (pgss_qtext_dsa)
		dsa_set_size_limit(pgss_qtext_dsa, (size_t) newval * 1024);
}

static void
pgss_attach_shmem(void)
{
	bool		found;

	if (pgss_shared != NULL)
		return;
	if (!pgstat_get_kind_info(PGSTAT_KIND_PGSS))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_stat_statements must be loaded via shared_preload_libraries")));

	pgss_shared = GetNamedDSMSegment("pg_stat_statements_state",
									 sizeof(pgssSharedState),
									 pgss_init_shmem,
									 &found, NULL);

	if (pgss_qtext_dsa == NULL)
	{
		pgss_qtext_dsa = GetNamedDSA("pg_stat_statements_qtext", &found);
		dsa_set_size_limit(pgss_qtext_dsa, (size_t) pgss_query_text_memory * 1024);
	}

	if (pgss_hash == NULL)
		pgss_hash = pgstat_get_hash_for_kind(PGSTAT_KIND_PGSS);
}

/*
 * Module load callback
 */
void
_PG_init(void)
{
	/*
	 * In order to register our custom pgstat kind, we have to be loaded via
	 * shared_preload_libraries.  If not, fall out without hooking into any of
	 * the main system.  (We don't throw error here because it seems useful to
	 * allow the pg_stat_statements functions to be created even when the
	 * module isn't active.  The functions must protect themselves against
	 * being called then, however.)
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	/*
	 * Inform the postmaster that we want to enable query_id calculation if
	 * compute_query_id is set to auto.
	 */
	EnableQueryId();

	/* Register custom pgstat kind */
	pgstat_register_kind(PGSTAT_KIND_PGSS, &pgss_kind_info);

	/* Define GUCs */
	DefineCustomIntVariable("pg_stat_statements.max",
							"Sets the maximum number of statements tracked by pg_stat_statements.",
							NULL,
							&pgss_max,
							5000,
							100,
							INT_MAX / 2,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomEnumVariable("pg_stat_statements.track",
							 "Selects which statements are tracked by pg_stat_statements.",
							 NULL,
							 &pgss_track,
							 PGSS_TRACK_TOP,
							 track_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_stat_statements.track_utility",
							 "Selects whether utility commands are tracked by pg_stat_statements.",
							 NULL,
							 &pgss_track_utility,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_stat_statements.track_planning",
							 "Selects whether planning duration is tracked by pg_stat_statements.",
							 NULL,
							 &pgss_track_planning,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_stat_statements.save",
							 "Save pg_stat_statements statistics across server shutdowns.",
							 NULL,
							 &pgss_save,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("pg_stat_statements.query_text_memory",
							"Sets the memory limit for query text storage.",
							NULL,
							&pgss_query_text_memory,
							4096,
							256,
							MAX_KILOBYTES,
							PGC_SIGHUP,
							GUC_UNIT_KB,
							NULL,
							pgss_assign_query_text_memory,
							NULL);

	MarkGUCPrefixReserved("pg_stat_statements");

	/*
	 * Install hooks.
	 */
	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = pgss_post_parse_analyze;
	prev_planner_hook = planner_hook;
	planner_hook = pgss_planner;
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pgss_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pgss_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pgss_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pgss_ExecutorEnd;
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = pgss_ProcessUtility;
}

/*--------------------------------------------------------------------------
 * pgstat flush helpers: merge per-kind timing counters into shared memory
 *--------------------------------------------------------------------------
 */
static void
pgss_flush_kind(pgssCounters *shared, pgssCounters *pending, pgssStoreKind kind)
{
	int64		n_a,
				n_b;
	double		delta;

	n_a = shared->calls[kind];
	n_b = pending->calls[kind];

	shared->calls[kind] += n_b;
	shared->total_time[kind] += pending->total_time[kind];

	if (n_a == 0)
	{
		shared->min_time[kind] = pending->min_time[kind];
		shared->max_time[kind] = pending->max_time[kind];
		shared->mean_time[kind] = pending->mean_time[kind];
		shared->sum_var_time[kind] = pending->sum_var_time[kind];
	}
	else
	{
		if (pending->min_time[kind] < shared->min_time[kind])
			shared->min_time[kind] = pending->min_time[kind];
		if (pending->max_time[kind] > shared->max_time[kind])
			shared->max_time[kind] = pending->max_time[kind];

		/*
		 * Chan's parallel variance algorithm: combine two sets of (count,
		 * mean, sum_of_squared_deviations). See
		 * <http://www.johndcook.com/blog/standard_deviation/>
		 */
		delta = pending->mean_time[kind] - shared->mean_time[kind];
		shared->sum_var_time[kind] +=
			pending->sum_var_time[kind] +
			delta * delta * (double) n_a * (double) n_b / (double) (n_a + n_b);
		shared->mean_time[kind] =
			shared->total_time[kind] / shared->calls[kind];
	}
}

static bool
pgss_flush_pending_cb(PgStat_EntryRef *entry_ref, bool nowait)
{
	PgStat_PgssPending *pending;
	PgStatShared_Pgss *shared;

	pending = (PgStat_PgssPending *) entry_ref->pending;
	shared = (PgStatShared_Pgss *) entry_ref->shared_stats;

	if (!pgstat_lock_entry(entry_ref, nowait))
		return false;

	shared->key = pending->key;

	pgss_flush_kind(&shared->counters, &pending->counters, PGSS_EXEC);

	if (pgss_track_planning && pending->counters.calls[PGSS_PLAN] > 0)
		pgss_flush_kind(&shared->counters, &pending->counters, PGSS_PLAN);

	shared->counters.rows += pending->counters.rows;
	shared->counters.shared_blks_hit += pending->counters.shared_blks_hit;
	shared->counters.shared_blks_read += pending->counters.shared_blks_read;
	shared->counters.shared_blks_dirtied += pending->counters.shared_blks_dirtied;
	shared->counters.shared_blks_written += pending->counters.shared_blks_written;
	shared->counters.local_blks_hit += pending->counters.local_blks_hit;
	shared->counters.local_blks_read += pending->counters.local_blks_read;
	shared->counters.local_blks_dirtied += pending->counters.local_blks_dirtied;
	shared->counters.local_blks_written += pending->counters.local_blks_written;
	shared->counters.temp_blks_read += pending->counters.temp_blks_read;
	shared->counters.temp_blks_written += pending->counters.temp_blks_written;
	shared->counters.shared_blk_read_time += pending->counters.shared_blk_read_time;
	shared->counters.shared_blk_write_time += pending->counters.shared_blk_write_time;
	shared->counters.local_blk_read_time += pending->counters.local_blk_read_time;
	shared->counters.local_blk_write_time += pending->counters.local_blk_write_time;
	shared->counters.temp_blk_read_time += pending->counters.temp_blk_read_time;
	shared->counters.temp_blk_write_time += pending->counters.temp_blk_write_time;
	shared->counters.wal_records += pending->counters.wal_records;
	shared->counters.wal_fpi += pending->counters.wal_fpi;
	shared->counters.wal_bytes += pending->counters.wal_bytes;
	shared->counters.wal_buffers_full += pending->counters.wal_buffers_full;
	shared->counters.jit_functions += pending->counters.jit_functions;
	shared->counters.jit_generation_time += pending->counters.jit_generation_time;
	shared->counters.jit_inlining_count += pending->counters.jit_inlining_count;
	shared->counters.jit_inlining_time += pending->counters.jit_inlining_time;
	shared->counters.jit_optimization_count += pending->counters.jit_optimization_count;
	shared->counters.jit_optimization_time += pending->counters.jit_optimization_time;
	shared->counters.jit_emission_count += pending->counters.jit_emission_count;
	shared->counters.jit_emission_time += pending->counters.jit_emission_time;
	shared->counters.jit_deform_count += pending->counters.jit_deform_count;
	shared->counters.jit_deform_time += pending->counters.jit_deform_time;
	shared->counters.parallel_workers_to_launch += pending->counters.parallel_workers_to_launch;
	shared->counters.parallel_workers_launched += pending->counters.parallel_workers_launched;
	shared->counters.generic_plan_calls += pending->counters.generic_plan_calls;
	shared->counters.custom_plan_calls += pending->counters.custom_plan_calls;

	if (pending->last_access_updated)
		pg_atomic_write_u64(&shared->last_access,
							(uint64) GetCurrentStatementStartTimestamp());

	pgstat_unlock_entry(entry_ref);

	return true;
}

/*
 * Serialize entry metadata + query text alongside each pgstat entry.
 * On restart, from_serialized_data reconstructs both.
 */
static void
pgss_to_serialized_data(const PgStat_HashKey *key,
						const PgStatShared_Common *header,
						FILE *statfile)
{
	PgStatShared_Pgss *shpgss = (PgStatShared_Pgss *) header;
	bool		found = pgss_save;
	char	   *qtext = NULL;
	int			qtext_len = 0;

	pgstat_write_chunk_s(statfile, &found);

	if (!pgss_save)
		return;

	pgss_attach_shmem();

	pgstat_write_chunk_s(statfile, &shpgss->encoding);
	pgstat_write_chunk_s(statfile, &shpgss->stats_since);
	pgstat_write_chunk_s(statfile, &shpgss->minmax_stats_since);

	/* Write query text */
	if (DsaPointerIsValid(shpgss->query_text) && shpgss->query_len >= 0)
		qtext = dsa_get_address(pgss_qtext_dsa, shpgss->query_text);

	if (qtext)
	{
		qtext_len = shpgss->query_len;
		pgstat_write_chunk_s(statfile, &qtext_len);
		pgstat_write_chunk(statfile, qtext, qtext_len + 1);
	}
	else
	{
		qtext_len = -1;
		pgstat_write_chunk_s(statfile, &qtext_len);
	}
}

/*
 * Deserialize auxiliary data: restore metadata fields and query text.
 */
static bool
pgss_from_serialized_data(const PgStat_HashKey *key,
						  PgStatShared_Common *header,
						  FILE *statfile)
{
	PgStatShared_Pgss *shpgss = (PgStatShared_Pgss *) header;
	bool		had_entry;
	int			qtext_len;

	if (!pgstat_read_chunk_s(statfile, &had_entry))
		return false;

	if (!had_entry)
	{
		pgstat_drop_entry(PGSTAT_KIND_PGSS, key->dboid, key->objid, false);
		pg_atomic_sub_fetch_u64(&pgss_shared->live_entries, 1);
		return true;
	}

	if (!pgstat_read_chunk_s(statfile, &shpgss->encoding))
		return false;
	if (!pgstat_read_chunk_s(statfile, &shpgss->stats_since))
		return false;
	if (!pgstat_read_chunk_s(statfile, &shpgss->minmax_stats_since))
		return false;
	if (!pgstat_read_chunk_s(statfile, &qtext_len))
		return false;

	pgss_attach_shmem();

	if (qtext_len >= 0)
	{
		char	   *qtext;
		dsa_pointer dp;

		qtext = palloc(qtext_len + 1);
		if (!pgstat_read_chunk(statfile, qtext, qtext_len + 1))
		{
			pfree(qtext);
			return false;
		}

		dp = dsa_allocate_extended(pgss_qtext_dsa, qtext_len + 1, DSA_ALLOC_NO_OOM);
		if (DsaPointerIsValid(dp))
		{
			memcpy(dsa_get_address(pgss_qtext_dsa, dp), qtext, qtext_len + 1);
			shpgss->query_text = dp;
			shpgss->query_len = qtext_len;
		}
		else
		{
			shpgss->query_text = InvalidDsaPointer;
			shpgss->query_len = -1;
		}

		pfree(qtext);
	}
	else
	{
		shpgss->query_text = InvalidDsaPointer;
		shpgss->query_len = -1;
	}

	return true;
}


/*
 * Store query text in the DSA area and update the entry's text pointer.
 * On allocation failure, sets query_text to InvalidDsaPointer.
 */
static void
pgss_store_query_text(PgStatShared_Pgss *entry, const char *query, int query_len,
					  int encoding)
{
	dsa_pointer dp;

	dp = dsa_allocate_extended(pgss_qtext_dsa, query_len + 1, DSA_ALLOC_NO_OOM);
	if (DsaPointerIsValid(dp))
	{
		char	   *dst = dsa_get_address(pgss_qtext_dsa, dp);

		memcpy(dst, query, query_len);
		dst[query_len] = '\0';
		entry->query_text = dp;
		entry->query_len = query_len;
		entry->encoding = encoding;
	}
	else
	{
		entry->query_text = InvalidDsaPointer;
		entry->query_len = -1;
		entry->encoding = encoding;
	}
}

/*--------------------------------------------------------------------------
 * pgss_evict: Evict the least-recently-used entries.
 *
 * Called with pgss_shared->lock held exclusively.  Scans all entries,
 * sorts by last_access timestamp (oldest first), and drops the bottom 5%.
 *--------------------------------------------------------------------------
 */
typedef struct pgssEvictEntry
{
	Oid			dbid;
	uint64		objid;
	TimestampTz last_access;
	dsa_pointer text_ptr;
}			pgssEvictEntry;

static int
pgss_evict_entry_cmp(const void *a, const void *b)
{
	TimestampTz ta = ((const pgssEvictEntry *) a)->last_access;
	TimestampTz tb = ((const pgssEvictEntry *) b)->last_access;

	if (ta < tb)
		return -1;
	if (ta > tb)
		return 1;
	return 0;
}

static void
pgss_evict(void)
{
	dshash_seq_status hstat;
	PgStatShared_HashEntry *p;
	pgssEvictEntry *entries;
	int			nentries = 0;
	int			live = (int) pg_atomic_read_u64(&pgss_shared->live_entries);
	int			nevict;
	int			not_freed = 0;
	instr_time	start,
				duration;

	INSTR_TIME_SET_CURRENT(start);

	entries = palloc(live * sizeof(pgssEvictEntry));

	dshash_seq_init(&hstat, pgss_hash, false);
	while ((p = dshash_seq_next(&hstat)) != NULL)
	{
		PgStatShared_Pgss *shared;

		if (p->dropped)
			continue;

		shared = (PgStatShared_Pgss *) dsa_get_address(pgStatLocal.dsa, p->body);

		entries[nentries].dbid = p->key.dboid;
		entries[nentries].objid = p->key.objid;
		entries[nentries].last_access = (TimestampTz) pg_atomic_read_u64(&shared->last_access);
		entries[nentries].text_ptr = shared->query_text;
		nentries++;
	}
	dshash_seq_term(&hstat);

	if (nentries == 0)
	{
		pfree(entries);
		return;
	}

	qsort(entries, nentries, sizeof(pgssEvictEntry), pgss_evict_entry_cmp);

	nevict = Max(nentries / PGSS_EVICT_RATIO, 1);

	/* Don't evict entries accessed within the grace period */
	for (int i = 0; i < nevict; i++)
	{
		if (!TimestampDifferenceExceeds(entries[i].last_access,
										GetCurrentTimestamp(),
										PGSS_EVICT_GRACE_MS))
		{
			nevict = i;
			break;
		}
	}

	for (int i = 0; i < nevict; i++)
	{
		PgStat_EntryRef *victim_ref;
		dsa_pointer text_ptr = InvalidDsaPointer;
		bool		freed;

		/*
		 * Look up the entry to safely clear its query_text pointer under the
		 * entry-level lock before freeing.  This prevents concurrent readers
		 * from following a dangling DSA pointer.
		 */
		victim_ref = pgstat_get_entry_ref(PGSTAT_KIND_PGSS, entries[i].dbid,
										  entries[i].objid, false, NULL);
		if (victim_ref != NULL)
		{
			PgStatShared_Pgss *shared;

			shared = (PgStatShared_Pgss *) victim_ref->shared_stats;
			pgstat_lock_entry(victim_ref, false);
			text_ptr = shared->query_text;
			shared->query_text = InvalidDsaPointer;
			pgstat_unlock_entry(victim_ref);
		}

		freed = pgstat_drop_entry(PGSTAT_KIND_PGSS, entries[i].dbid,
								  entries[i].objid, true);

		if (DsaPointerIsValid(text_ptr))
			dsa_free(pgss_qtext_dsa, text_ptr);

		if (!freed)
			not_freed++;
		pg_atomic_sub_fetch_u64(&pgss_shared->live_entries, 1);
	}

	pg_atomic_fetch_add_u64(&pgss_shared->dealloc, 1);

	if (not_freed > 0)
		pgstat_request_entry_refs_gc();

	pfree(entries);

	INSTR_TIME_SET_CURRENT(duration);
	INSTR_TIME_SUBTRACT(duration, start);
}

/*
 * pgss_maybe_evict: Run eviction if needed.  Returns true if creation
 * can proceed, false if the caller should skip recording.
 *
 * Must be called with pgss_shared->lock held exclusively.
 */
static bool
pgss_maybe_evict(void)
{
	int64		live = (int64) pg_atomic_read_u64(&pgss_shared->live_entries);

	/* Safety valve: dshash overflow from pinned entries (see above) */
	if (pgstat_get_entry_count(PGSTAT_KIND_PGSS) >= pgss_max * 2)
	{
		pg_atomic_fetch_add_u64(&pgss_shared->skipped_entries, 1);
		return false;
	}

	if (live >= pgss_max)
	{
		TimestampTz now = GetCurrentStatementStartTimestamp();
		TimestampTz last = (TimestampTz) pg_atomic_read_u64(&pgss_shared->last_eviction);

		if (last == 0 || TimestampDifferenceExceeds(last, now,
													PGSS_EVICT_INTERVAL_MS))
		{
			pg_atomic_write_u64(&pgss_shared->last_eviction, (uint64) now);
			pgss_evict();
		}
		else
		{
			pg_atomic_fetch_add_u64(&pgss_shared->skipped_entries, 1);
			return false;
		}
	}

	return true;
}

/*--------------------------------------------------------------------------
 * pgss_store: Record statistics for one statement execution.
 *
 * Lock-free lookup for existing entries.  If the entry is new, takes an
 * exclusive lock to serialize creation, evicting if at capacity.
 *--------------------------------------------------------------------------
 */
static void
pgss_store(const char *query, int64 queryId,
		   int query_location, int query_len,
		   pgssStoreKind kind,
		   double total_time, uint64 rows,
		   const BufferUsage *bufusage,
		   const WalUsage *walusage,
		   const struct JitInstrumentation *jitusage,
		   const JumbleState *jstate,
		   int parallel_workers_to_launch,
		   int parallel_workers_launched,
		   PlannedStmtOrigin planOrigin)
{
	pgssHashKey key;

	uint64		objid;
	PgStat_EntryRef *entry_ref;
	PgStat_PgssPending *pending;
	PgStatShared_Pgss *shared;
	char	   *norm_query = NULL;
	int			encoding = GetDatabaseEncoding();
	bool		created_entry = false;

	Assert(query != NULL);

	if (queryId == INT64CONST(0))
		return;

	memset(&key, 0, sizeof(pgssHashKey));
	key.userid = GetUserId();
	key.dbid = MyDatabaseId;
	key.queryid = queryId;
	key.toplevel = (nesting_level == 0);

	pgss_attach_shmem();

	objid = pgss_hash_key(&key);

	/* Fast path: check if the entry already exists (lock-free lookup) */
	entry_ref = pgstat_get_entry_ref(PGSTAT_KIND_PGSS, key.dbid, objid,
									 false, NULL);

	if (entry_ref == NULL)
	{
		/*
		 * Entry doesn't exist.  Serialize creation under exclusive lock,
		 * evicting if needed.
		 */
		LWLockAcquire(&pgss_shared->lock, LW_EXCLUSIVE);

		if (!pgss_maybe_evict())
		{
			LWLockRelease(&pgss_shared->lock);
			return;
		}

		entry_ref = pgstat_prep_pending_entry(PGSTAT_KIND_PGSS, key.dbid,
											  objid, &created_entry);

		LWLockRelease(&pgss_shared->lock);
	}
	else
		entry_ref = pgstat_prep_pending_entry(PGSTAT_KIND_PGSS, key.dbid,
											  objid, &created_entry);

	shared = (PgStatShared_Pgss *) entry_ref->shared_stats;

	/* New entry, or existing entry whose text was freed during eviction */
	if (created_entry || !DsaPointerIsValid(shared->query_text))
	{
		pgstat_lock_entry(entry_ref, false);

		if (created_entry)
		{
			pg_atomic_fetch_add_u64(&pgss_shared->live_entries, 1);
			shared->key = key;
			shared->stats_since = GetCurrentTimestamp();
			pg_atomic_init_u32(&shared->usage, 1);
			pg_atomic_init_u64(&shared->last_access,
							   (uint64) GetCurrentStatementStartTimestamp());
			shared->minmax_stats_since = shared->stats_since;
			shared->encoding = encoding;
		}

		if (!DsaPointerIsValid(shared->query_text))
		{
			query = CleanQuerytext(query, &query_location, &query_len);
			if (jstate && jstate->clocations_count > 0)
				norm_query = generate_normalized_query(jstate, query,
													   query_location,
													   &query_len);

			pgss_store_query_text(shared, norm_query ? norm_query : query,
								  query_len, encoding);
		}
		pgstat_unlock_entry(entry_ref);
	}

	if (norm_query)
		pfree(norm_query);

	if (!jstate)
	{
		Assert(kind == PGSS_PLAN || kind == PGSS_EXEC);

		pending = (PgStat_PgssPending *) entry_ref->pending;
		pending->key = key;

		pending->counters.calls[kind]++;
		pending->last_access_updated = true;
		pending->counters.total_time[kind] += total_time;

		if (pending->counters.calls[kind] == 1)
		{
			pending->counters.min_time[kind] = total_time;
			pending->counters.max_time[kind] = total_time;
			pending->counters.mean_time[kind] = total_time;
		}
		else
		{
			/*
			 * Welford's online algorithm for accumulating mean and sum of
			 * squared deviations. See
			 * <http://www.johndcook.com/blog/standard_deviation/>
			 */
			double		old_mean = pending->counters.mean_time[kind];

			pending->counters.mean_time[kind] +=
				(total_time - old_mean) / pending->counters.calls[kind];
			pending->counters.sum_var_time[kind] +=
				(total_time - old_mean) * (total_time - pending->counters.mean_time[kind]);

			if (pending->counters.min_time[kind] > total_time)
				pending->counters.min_time[kind] = total_time;
			if (pending->counters.max_time[kind] < total_time)
				pending->counters.max_time[kind] = total_time;
		}

		pending->counters.rows += rows;

		if (bufusage)
		{
			pending->counters.shared_blks_hit += bufusage->shared_blks_hit;
			pending->counters.shared_blks_read += bufusage->shared_blks_read;
			pending->counters.shared_blks_dirtied += bufusage->shared_blks_dirtied;
			pending->counters.shared_blks_written += bufusage->shared_blks_written;
			pending->counters.local_blks_hit += bufusage->local_blks_hit;
			pending->counters.local_blks_read += bufusage->local_blks_read;
			pending->counters.local_blks_dirtied += bufusage->local_blks_dirtied;
			pending->counters.local_blks_written += bufusage->local_blks_written;
			pending->counters.temp_blks_read += bufusage->temp_blks_read;
			pending->counters.temp_blks_written += bufusage->temp_blks_written;
			pending->counters.shared_blk_read_time += INSTR_TIME_GET_MILLISEC(bufusage->shared_blk_read_time);
			pending->counters.shared_blk_write_time += INSTR_TIME_GET_MILLISEC(bufusage->shared_blk_write_time);
			pending->counters.local_blk_read_time += INSTR_TIME_GET_MILLISEC(bufusage->local_blk_read_time);
			pending->counters.local_blk_write_time += INSTR_TIME_GET_MILLISEC(bufusage->local_blk_write_time);
			pending->counters.temp_blk_read_time += INSTR_TIME_GET_MILLISEC(bufusage->temp_blk_read_time);
			pending->counters.temp_blk_write_time += INSTR_TIME_GET_MILLISEC(bufusage->temp_blk_write_time);
		}

		if (walusage)
		{
			pending->counters.wal_records += walusage->wal_records;
			pending->counters.wal_fpi += walusage->wal_fpi;
			pending->counters.wal_bytes += walusage->wal_bytes;
			pending->counters.wal_buffers_full += walusage->wal_buffers_full;
		}

		if (jitusage)
		{
			pending->counters.jit_functions += jitusage->created_functions;
			pending->counters.jit_generation_time += INSTR_TIME_GET_MILLISEC(jitusage->generation_counter);

			if (INSTR_TIME_GET_MILLISEC(jitusage->deform_counter))
				pending->counters.jit_deform_count++;
			pending->counters.jit_deform_time += INSTR_TIME_GET_MILLISEC(jitusage->deform_counter);

			if (INSTR_TIME_GET_MILLISEC(jitusage->inlining_counter))
				pending->counters.jit_inlining_count++;
			pending->counters.jit_inlining_time += INSTR_TIME_GET_MILLISEC(jitusage->inlining_counter);

			if (INSTR_TIME_GET_MILLISEC(jitusage->optimization_counter))
				pending->counters.jit_optimization_count++;
			pending->counters.jit_optimization_time += INSTR_TIME_GET_MILLISEC(jitusage->optimization_counter);

			if (INSTR_TIME_GET_MILLISEC(jitusage->emission_counter))
				pending->counters.jit_emission_count++;
			pending->counters.jit_emission_time += INSTR_TIME_GET_MILLISEC(jitusage->emission_counter);
		}

		pending->counters.parallel_workers_to_launch += parallel_workers_to_launch;
		pending->counters.parallel_workers_launched += parallel_workers_launched;

		if (planOrigin == PLAN_STMT_CACHE_GENERIC)
			pending->counters.generic_plan_calls++;
		else if (planOrigin == PLAN_STMT_CACHE_CUSTOM)
			pending->counters.custom_plan_calls++;
	}

}

/*--------------------------------------------------------------------------
 * Hook implementations
 *--------------------------------------------------------------------------
 */

static void
pgss_post_parse_analyze(ParseState *pstate, Query *query,
						const JumbleState *jstate)
{
	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query, jstate);

	if (!pgss_enabled(nesting_level))
		return;

	/*
	 * Clear queryId for EXECUTE so stats accumulate under the PREPARE's
	 * queryId instead.
	 */
	if (query->utilityStmt)
	{
		if (pgss_track_utility && IsA(query->utilityStmt, ExecuteStmt))
		{
			query->queryId = INT64CONST(0);
			return;
		}
	}

	/*
	 * If query jumbling were able to identify any ignorable constants, we
	 * immediately create a hash table entry for the query, so that we can
	 * record the normalized form of the query string.  If there were no such
	 * constants, the normalized string would be the same as the query text
	 * anyway, so there's no need for an early entry.
	 */
	if (jstate && jstate->clocations_count > 0)
		pgss_store(pstate->p_sourcetext,
				   query->queryId,
				   query->stmt_location,
				   query->stmt_len,
				   PGSS_INVALID,
				   0,
				   0,
				   NULL,
				   NULL,
				   NULL,
				   jstate,
				   0,
				   0,
				   PLAN_STMT_UNKNOWN);
}

static PlannedStmt *
pgss_planner(Query *parse,
			 const char *query_string,
			 int cursorOptions,
			 ParamListInfo boundParams,
			 ExplainState *es)
{
	PlannedStmt *result;

	/*
	 * We can't process the query if no query_string is provided, as
	 * pgss_store needs it.  We also ignore query without queryid, as it would
	 * be treated as a utility statement, which may not be the case.
	 */
	if (pgss_enabled(nesting_level)
		&& pgss_track_planning && query_string
		&& parse->queryId != INT64CONST(0))
	{
		instr_time	start;
		instr_time	duration;
		BufferUsage bufusage_start,
					bufusage;
		WalUsage	walusage_start,
					walusage;

		bufusage_start = pgBufferUsage;
		walusage_start = pgWalUsage;
		INSTR_TIME_SET_CURRENT(start);

		nesting_level++;
		PG_TRY();
		{
			if (prev_planner_hook)
				result = prev_planner_hook(parse, query_string, cursorOptions,
										   boundParams, es);
			else
				result = standard_planner(parse, query_string, cursorOptions,
										  boundParams, es);
		}
		PG_FINALLY();
		{
			nesting_level--;
		}
		PG_END_TRY();

		INSTR_TIME_SET_CURRENT(duration);
		INSTR_TIME_SUBTRACT(duration, start);

		memset(&bufusage, 0, sizeof(BufferUsage));
		BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);

		memset(&walusage, 0, sizeof(WalUsage));
		WalUsageAccumDiff(&walusage, &pgWalUsage, &walusage_start);

		pgss_store(query_string,
				   parse->queryId,
				   parse->stmt_location,
				   parse->stmt_len,
				   PGSS_PLAN,
				   INSTR_TIME_GET_MILLISEC(duration),
				   0,
				   &bufusage,
				   &walusage,
				   NULL,
				   NULL,
				   0, 0,
				   result->planOrigin);
	}
	else
	{
		nesting_level++;
		PG_TRY();
		{
			if (prev_planner_hook)
				result = prev_planner_hook(parse, query_string, cursorOptions,
										   boundParams, es);
			else
				result = standard_planner(parse, query_string, cursorOptions,
										  boundParams, es);
		}
		PG_FINALLY();
		{
			nesting_level--;
		}
		PG_END_TRY();
	}

	return result;
}

static void
pgss_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (pgss_enabled(nesting_level) &&
		queryDesc->plannedstmt->queryId != INT64CONST(0))
	{
		queryDesc->query_instr_options |= INSTRUMENT_ALL;
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

static void
pgss_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count);
		else
			standard_ExecutorRun(queryDesc, direction, count);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();
}

static void
pgss_ExecutorFinish(QueryDesc *queryDesc)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();
}

static void
pgss_ExecutorEnd(QueryDesc *queryDesc)
{
	int64		queryId = queryDesc->plannedstmt->queryId;

	if (queryId != INT64CONST(0) && queryDesc->query_instr &&
		pgss_enabled(nesting_level))
	{
		pgss_store(queryDesc->sourceText,
				   queryId,
				   queryDesc->plannedstmt->stmt_location,
				   queryDesc->plannedstmt->stmt_len,
				   PGSS_EXEC,
				   INSTR_TIME_GET_MILLISEC(queryDesc->query_instr->total),
				   queryDesc->estate->es_total_processed,
				   &queryDesc->query_instr->bufusage,
				   &queryDesc->query_instr->walusage,
				   queryDesc->estate->es_jit ? &queryDesc->estate->es_jit->instr : NULL,
				   NULL,
				   queryDesc->estate->es_parallel_workers_to_launch,
				   queryDesc->estate->es_parallel_workers_launched,
				   queryDesc->plannedstmt->planOrigin);
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

static void
pgss_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
					bool readOnlyTree,
					ProcessUtilityContext context,
					ParamListInfo params, QueryEnvironment *queryEnv,
					DestReceiver *dest, QueryCompletion *qc)
{
	Node	   *parsetree = pstmt->utilityStmt;
	int64		saved_queryId = pstmt->queryId;
	int			saved_stmt_location = pstmt->stmt_location;
	int			saved_stmt_len = pstmt->stmt_len;
	PlannedStmtOrigin saved_planOrigin = pstmt->planOrigin;
	bool		enabled = pgss_track_utility && pgss_enabled(nesting_level);

	if (enabled)
		pstmt->queryId = INT64CONST(0);

	if (enabled &&
		!IsA(parsetree, ExecuteStmt) &&
		!IsA(parsetree, PrepareStmt))
	{
		instr_time	start;
		instr_time	duration;
		uint64		rows;
		BufferUsage bufusage_start,
					bufusage;
		WalUsage	walusage_start,
					walusage;

		bufusage_start = pgBufferUsage;
		walusage_start = pgWalUsage;
		INSTR_TIME_SET_CURRENT(start);

		nesting_level++;
		PG_TRY();
		{
			if (prev_ProcessUtility)
				prev_ProcessUtility(pstmt, queryString, readOnlyTree,
									context, params, queryEnv,
									dest, qc);
			else
				standard_ProcessUtility(pstmt, queryString, readOnlyTree,
										context, params, queryEnv,
										dest, qc);
		}
		PG_FINALLY();
		{
			nesting_level--;
		}
		PG_END_TRY();

		pstmt = NULL;

		INSTR_TIME_SET_CURRENT(duration);
		INSTR_TIME_SUBTRACT(duration, start);

		rows = (qc && (qc->commandTag == CMDTAG_COPY ||
					   qc->commandTag == CMDTAG_FETCH ||
					   qc->commandTag == CMDTAG_SELECT ||
					   qc->commandTag == CMDTAG_REFRESH_MATERIALIZED_VIEW)) ?
			qc->nprocessed : 0;

		memset(&bufusage, 0, sizeof(BufferUsage));
		BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);

		memset(&walusage, 0, sizeof(WalUsage));
		WalUsageAccumDiff(&walusage, &pgWalUsage, &walusage_start);

		pgss_store(queryString,
				   saved_queryId,
				   saved_stmt_location,
				   saved_stmt_len,
				   PGSS_EXEC,
				   INSTR_TIME_GET_MILLISEC(duration),
				   rows,
				   &bufusage,
				   &walusage,
				   NULL,
				   NULL,
				   0, 0,
				   saved_planOrigin);
	}
	else
	{
		bool		bump_level =
			!IsA(parsetree, ExecuteStmt) &&
			!IsA(parsetree, PrepareStmt);

		if (bump_level)
			nesting_level++;
		PG_TRY();
		{
			if (prev_ProcessUtility)
				prev_ProcessUtility(pstmt, queryString, readOnlyTree,
									context, params, queryEnv,
									dest, qc);
			else
				standard_ProcessUtility(pstmt, queryString, readOnlyTree,
										context, params, queryEnv,
										dest, qc);
		}
		PG_FINALLY();
		{
			if (bump_level)
				nesting_level--;
		}
		PG_END_TRY();
	}
}

/*--------------------------------------------------------------------------
 * SQL-callable functions
 *--------------------------------------------------------------------------
 */

/* Number of output arguments (columns) for various API versions */
#define PG_STAT_STATEMENTS_COLS_V1_0	14
#define PG_STAT_STATEMENTS_COLS_V1_1	18
#define PG_STAT_STATEMENTS_COLS_V1_2	19
#define PG_STAT_STATEMENTS_COLS_V1_3	23
#define PG_STAT_STATEMENTS_COLS_V1_8	32
#define PG_STAT_STATEMENTS_COLS_V1_9	33
#define PG_STAT_STATEMENTS_COLS_V1_10	43
#define PG_STAT_STATEMENTS_COLS_V1_11	49
#define PG_STAT_STATEMENTS_COLS_V1_12	52
#define PG_STAT_STATEMENTS_COLS_V1_13	54
#define PG_STAT_STATEMENTS_COLS			54	/* maximum of above */

/*
 * Reset statement statistics.
 */
Datum
pg_stat_statements_reset(PG_FUNCTION_ARGS)
{
	entry_reset(0, 0, 0, false);

	PG_RETURN_VOID();
}

/*
 * Reset statement statistics corresponding to userid, dbid, and queryid.
 */
Datum
pg_stat_statements_reset_1_7(PG_FUNCTION_ARGS)
{
	Oid			userid;
	Oid			dbid;
	int64		queryid;

	userid = PG_GETARG_OID(0);
	dbid = PG_GETARG_OID(1);
	queryid = PG_GETARG_INT64(2);

	entry_reset(userid, dbid, queryid, false);

	PG_RETURN_VOID();
}

Datum
pg_stat_statements_reset_1_11(PG_FUNCTION_ARGS)
{
	Oid			userid;
	Oid			dbid;
	int64		queryid;
	bool		minmax_only;

	userid = PG_GETARG_OID(0);
	dbid = PG_GETARG_OID(1);
	queryid = PG_GETARG_INT64(2);
	minmax_only = PG_GETARG_BOOL(3);

	PG_RETURN_TIMESTAMPTZ(entry_reset(userid, dbid, queryid, minmax_only));
}

Datum
pg_stat_statements_1_2(PG_FUNCTION_ARGS)
{
	bool		showtext = PG_GETARG_BOOL(0);

	pg_stat_statements_internal(fcinfo, PGSS_V1_2, showtext);
	return (Datum) 0;
}

Datum
pg_stat_statements_1_3(PG_FUNCTION_ARGS)
{
	bool		showtext = PG_GETARG_BOOL(0);

	pg_stat_statements_internal(fcinfo, PGSS_V1_3, showtext);
	return (Datum) 0;
}

Datum
pg_stat_statements_1_8(PG_FUNCTION_ARGS)
{
	bool		showtext = PG_GETARG_BOOL(0);

	pg_stat_statements_internal(fcinfo, PGSS_V1_8, showtext);
	return (Datum) 0;
}

Datum
pg_stat_statements_1_9(PG_FUNCTION_ARGS)
{
	bool		showtext = PG_GETARG_BOOL(0);

	pg_stat_statements_internal(fcinfo, PGSS_V1_9, showtext);
	return (Datum) 0;
}

Datum
pg_stat_statements_1_10(PG_FUNCTION_ARGS)
{
	bool		showtext = PG_GETARG_BOOL(0);

	pg_stat_statements_internal(fcinfo, PGSS_V1_10, showtext);
	return (Datum) 0;
}

Datum
pg_stat_statements_1_11(PG_FUNCTION_ARGS)
{
	bool		showtext = PG_GETARG_BOOL(0);

	pg_stat_statements_internal(fcinfo, PGSS_V1_11, showtext);
	return (Datum) 0;
}

Datum
pg_stat_statements_1_12(PG_FUNCTION_ARGS)
{
	bool		showtext = PG_GETARG_BOOL(0);

	pg_stat_statements_internal(fcinfo, PGSS_V1_12, showtext);
	return (Datum) 0;
}

Datum
pg_stat_statements_1_13(PG_FUNCTION_ARGS)
{
	bool		showtext = PG_GETARG_BOOL(0);

	pg_stat_statements_internal(fcinfo, PGSS_V1_13, showtext);
	return (Datum) 0;
}

/*
 * Legacy entry point for pg_stat_statements() API versions 1.0 and 1.1.
 */
Datum
pg_stat_statements(PG_FUNCTION_ARGS)
{
	/* If it's really API 1.1, we'll figure that out below */
	pg_stat_statements_internal(fcinfo, PGSS_V1_0, true);

	return (Datum) 0;
}

/*
 * pg_stat_statements_internal
 *
 * Scan the per-kind pgstat dshash for all entries, reading counters and
 * metadata directly from the shared body.
 */
static void
pg_stat_statements_internal(FunctionCallInfo fcinfo,
							pgssVersion api_version,
							bool showtext)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	dshash_seq_status hstat;
	PgStatShared_HashEntry *p;
	Oid			userid = GetUserId();
	bool		is_allowed_role;

	is_allowed_role = has_privs_of_role(userid, ROLE_PG_READ_ALL_STATS);

	pgss_attach_shmem();

	/* Flush pending stats so we can read up-to-date counters */
	pgstat_report_stat(true);

	InitMaterializedSRF(fcinfo, 0);

	/*
	 * Check we have the expected number of output arguments.
	 */
	switch (rsinfo->setDesc->natts)
	{
		case PG_STAT_STATEMENTS_COLS_V1_0:
			if (api_version != PGSS_V1_0)
				elog(ERROR, "incorrect number of output arguments");
			break;
		case PG_STAT_STATEMENTS_COLS_V1_1:
			/* pg_stat_statements() should have told us 1.0 */
			if (api_version != PGSS_V1_0)
				elog(ERROR, "incorrect number of output arguments");
			api_version = PGSS_V1_1;
			break;
		case PG_STAT_STATEMENTS_COLS_V1_2:
			if (api_version != PGSS_V1_2)
				elog(ERROR, "incorrect number of output arguments");
			break;
		case PG_STAT_STATEMENTS_COLS_V1_3:
			if (api_version != PGSS_V1_3)
				elog(ERROR, "incorrect number of output arguments");
			break;
		case PG_STAT_STATEMENTS_COLS_V1_8:
			if (api_version != PGSS_V1_8)
				elog(ERROR, "incorrect number of output arguments");
			break;
		case PG_STAT_STATEMENTS_COLS_V1_9:
			if (api_version != PGSS_V1_9)
				elog(ERROR, "incorrect number of output arguments");
			break;
		case PG_STAT_STATEMENTS_COLS_V1_10:
			if (api_version != PGSS_V1_10)
				elog(ERROR, "incorrect number of output arguments");
			break;
		case PG_STAT_STATEMENTS_COLS_V1_11:
			if (api_version != PGSS_V1_11)
				elog(ERROR, "incorrect number of output arguments");
			break;
		case PG_STAT_STATEMENTS_COLS_V1_12:
			if (api_version != PGSS_V1_12)
				elog(ERROR, "incorrect number of output arguments");
			break;
		case PG_STAT_STATEMENTS_COLS_V1_13:
			if (api_version != PGSS_V1_13)
				elog(ERROR, "incorrect number of output arguments");
			break;
		default:
			elog(ERROR, "incorrect number of output arguments");
	}

	dshash_seq_init(&hstat, pgss_hash, false);
	while ((p = dshash_seq_next(&hstat)) != NULL)
	{
		Datum		values[PG_STAT_STATEMENTS_COLS];
		bool		nulls[PG_STAT_STATEMENTS_COLS];
		int			i = 0;
		PgStatShared_Pgss *shared;
		pgssCounters tmp;
		double		stddev;

		if (p->dropped)
			continue;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		shared = (PgStatShared_Pgss *) dsa_get_address(pgStatLocal.dsa, p->body);

		LWLockAcquire(&shared->header.lock, LW_SHARED);
		tmp = shared->counters;
		LWLockRelease(&shared->header.lock);

		if (tmp.calls[PGSS_EXEC] == 0 && tmp.calls[PGSS_PLAN] == 0)
			continue;

		values[i++] = ObjectIdGetDatum(shared->key.userid);
		values[i++] = ObjectIdGetDatum(shared->key.dbid);
		if (api_version >= PGSS_V1_9)
			values[i++] = BoolGetDatum(shared->key.toplevel);

		if (is_allowed_role || shared->key.userid == userid)
		{
			if (api_version >= PGSS_V1_2)
				values[i++] = Int64GetDatumFast(shared->key.queryid);

			if (showtext)
			{
				if (DsaPointerIsValid(shared->query_text) && shared->query_len >= 0)
				{
					char	   *qstr = dsa_get_address(pgss_qtext_dsa, shared->query_text);
					char	   *enc = pg_any_to_server(qstr, shared->query_len, shared->encoding);

					values[i++] = CStringGetTextDatum(enc);
					if (enc != qstr)
						pfree(enc);
				}
				else
					nulls[i++] = true;
			}
			else
				nulls[i++] = true;
		}
		else
		{
			if (api_version >= PGSS_V1_2)
				nulls[i++] = true;

			if (showtext)
				values[i++] = CStringGetTextDatum("<insufficient privilege>");
			else
				nulls[i++] = true;
		}

		/* Note: PGSS_PLAN is 0, PGSS_EXEC is 1 */
		for (int kind = 0; kind < PGSS_NUMKIND; kind++)
		{
			if (kind == PGSS_EXEC || api_version >= PGSS_V1_8)
			{
				values[i++] = Int64GetDatumFast(tmp.calls[kind]);
				values[i++] = Float8GetDatumFast(tmp.total_time[kind]);
			}

			if ((kind == PGSS_EXEC && api_version >= PGSS_V1_3) ||
				api_version >= PGSS_V1_8)
			{
				values[i++] = Float8GetDatumFast(tmp.min_time[kind]);
				values[i++] = Float8GetDatumFast(tmp.max_time[kind]);
				values[i++] = Float8GetDatumFast(tmp.mean_time[kind]);

				if (tmp.calls[kind] > 1)
					stddev = sqrt(tmp.sum_var_time[kind] / tmp.calls[kind]);
				else
					stddev = 0.0;
				values[i++] = Float8GetDatumFast(stddev);
			}
		}

		values[i++] = Int64GetDatumFast(tmp.rows);
		values[i++] = Int64GetDatumFast(tmp.shared_blks_hit);
		values[i++] = Int64GetDatumFast(tmp.shared_blks_read);
		if (api_version >= PGSS_V1_1)
			values[i++] = Int64GetDatumFast(tmp.shared_blks_dirtied);
		values[i++] = Int64GetDatumFast(tmp.shared_blks_written);
		values[i++] = Int64GetDatumFast(tmp.local_blks_hit);
		values[i++] = Int64GetDatumFast(tmp.local_blks_read);
		if (api_version >= PGSS_V1_1)
			values[i++] = Int64GetDatumFast(tmp.local_blks_dirtied);
		values[i++] = Int64GetDatumFast(tmp.local_blks_written);
		values[i++] = Int64GetDatumFast(tmp.temp_blks_read);
		values[i++] = Int64GetDatumFast(tmp.temp_blks_written);
		if (api_version >= PGSS_V1_1)
		{
			values[i++] = Float8GetDatumFast(tmp.shared_blk_read_time);
			values[i++] = Float8GetDatumFast(tmp.shared_blk_write_time);
		}
		if (api_version >= PGSS_V1_11)
		{
			values[i++] = Float8GetDatumFast(tmp.local_blk_read_time);
			values[i++] = Float8GetDatumFast(tmp.local_blk_write_time);
		}
		if (api_version >= PGSS_V1_10)
		{
			values[i++] = Float8GetDatumFast(tmp.temp_blk_read_time);
			values[i++] = Float8GetDatumFast(tmp.temp_blk_write_time);
		}
		if (api_version >= PGSS_V1_8)
		{
			char		buf[256];
			Datum		wal_bytes;

			values[i++] = Int64GetDatumFast(tmp.wal_records);
			values[i++] = Int64GetDatumFast(tmp.wal_fpi);

			snprintf(buf, sizeof buf, UINT64_FORMAT, tmp.wal_bytes);
			wal_bytes = DirectFunctionCall3(numeric_in,
											CStringGetDatum(buf),
											ObjectIdGetDatum(0),
											Int32GetDatum(-1));
			values[i++] = wal_bytes;
		}
		if (api_version >= PGSS_V1_12)
			values[i++] = Int64GetDatumFast(tmp.wal_buffers_full);
		if (api_version >= PGSS_V1_10)
		{
			values[i++] = Int64GetDatumFast(tmp.jit_functions);
			values[i++] = Float8GetDatumFast(tmp.jit_generation_time);
			values[i++] = Int64GetDatumFast(tmp.jit_inlining_count);
			values[i++] = Float8GetDatumFast(tmp.jit_inlining_time);
			values[i++] = Int64GetDatumFast(tmp.jit_optimization_count);
			values[i++] = Float8GetDatumFast(tmp.jit_optimization_time);
			values[i++] = Int64GetDatumFast(tmp.jit_emission_count);
			values[i++] = Float8GetDatumFast(tmp.jit_emission_time);
		}
		if (api_version >= PGSS_V1_11)
		{
			values[i++] = Int64GetDatumFast(tmp.jit_deform_count);
			values[i++] = Float8GetDatumFast(tmp.jit_deform_time);
		}
		if (api_version >= PGSS_V1_12)
		{
			values[i++] = Int64GetDatumFast(tmp.parallel_workers_to_launch);
			values[i++] = Int64GetDatumFast(tmp.parallel_workers_launched);
		}
		if (api_version >= PGSS_V1_13)
		{
			values[i++] = Int64GetDatumFast(tmp.generic_plan_calls);
			values[i++] = Int64GetDatumFast(tmp.custom_plan_calls);
		}
		if (api_version >= PGSS_V1_11)
		{
			values[i++] = TimestampTzGetDatum(shared->stats_since);
			values[i++] = TimestampTzGetDatum(shared->minmax_stats_since);
		}

		Assert(i == (api_version == PGSS_V1_0 ? PG_STAT_STATEMENTS_COLS_V1_0 :
					 api_version == PGSS_V1_1 ? PG_STAT_STATEMENTS_COLS_V1_1 :
					 api_version == PGSS_V1_2 ? PG_STAT_STATEMENTS_COLS_V1_2 :
					 api_version == PGSS_V1_3 ? PG_STAT_STATEMENTS_COLS_V1_3 :
					 api_version == PGSS_V1_8 ? PG_STAT_STATEMENTS_COLS_V1_8 :
					 api_version == PGSS_V1_9 ? PG_STAT_STATEMENTS_COLS_V1_9 :
					 api_version == PGSS_V1_10 ? PG_STAT_STATEMENTS_COLS_V1_10 :
					 api_version == PGSS_V1_11 ? PG_STAT_STATEMENTS_COLS_V1_11 :
					 api_version == PGSS_V1_12 ? PG_STAT_STATEMENTS_COLS_V1_12 :
					 api_version == PGSS_V1_13 ? PG_STAT_STATEMENTS_COLS_V1_13 :
					 -1 /* fail if you forget to update this assert */ ));

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	dshash_seq_term(&hstat);
}

/* Number of output arguments (columns) for pg_stat_statements_info */
#define PG_STAT_STATEMENTS_INFO_COLS	5

/*
 * Return statistics of pg_stat_statements.
 */
Datum
pg_stat_statements_info(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[PG_STAT_STATEMENTS_INFO_COLS] = {0};
	bool		nulls[PG_STAT_STATEMENTS_INFO_COLS] = {0};

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	pgss_attach_shmem();

	values[0] = Int64GetDatum((int64) pg_atomic_read_u64(&pgss_shared->dealloc));
	values[1] = TimestampTzGetDatum((TimestampTz) pg_atomic_read_u64(&pgss_shared->stats_reset));
	values[2] = Int64GetDatum((int64) pg_atomic_read_u64(&pgss_shared->skipped_entries));
	values[3] = Int64GetDatum((int64) dsa_get_total_size(pgss_qtext_dsa));
	values[4] = Int64GetDatum((int64) pgstat_get_entry_count(PGSTAT_KIND_PGSS));

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

static TimestampTz
entry_reset(Oid userid, Oid dbid, int64 queryid, bool minmax_only)
{
	dshash_seq_status hstat;
	PgStatShared_HashEntry *p;
	TimestampTz stats_reset;

	pgss_attach_shmem();

	stats_reset = GetCurrentTimestamp();

	if (minmax_only)
	{
		dshash_seq_init(&hstat, pgss_hash, false);
		while ((p = dshash_seq_next(&hstat)) != NULL)
		{
			PgStatShared_Pgss *shared;

			if (p->dropped)
				continue;

			shared = (PgStatShared_Pgss *) dsa_get_address(pgStatLocal.dsa, p->body);

			if ((!userid || shared->key.userid == userid) &&
				(!dbid || shared->key.dbid == dbid) &&
				(!queryid || shared->key.queryid == queryid))
			{
				LWLockAcquire(&shared->header.lock, LW_EXCLUSIVE);

				shared->minmax_stats_since = stats_reset;

				for (int kind = 0; kind < PGSS_NUMKIND; kind++)
				{
					shared->counters.min_time[kind] = 0;
					shared->counters.max_time[kind] = 0;
					shared->counters.mean_time[kind] = 0;
					shared->counters.sum_var_time[kind] = 0;
				}

				LWLockRelease(&shared->header.lock);
			}
		}
		dshash_seq_term(&hstat);

		return stats_reset;
	}

	if (userid != 0 && dbid != 0 && queryid != INT64CONST(0))
	{
		pgssHashKey key;
		uint64		objid;

		memset(&key, 0, sizeof(pgssHashKey));
		key.userid = userid;
		key.dbid = dbid;
		key.queryid = queryid;

		key.toplevel = false;
		objid = pgss_hash_key(&key);
		pgstat_drop_entry(PGSTAT_KIND_PGSS, key.dbid, objid, true);

		key.toplevel = true;
		objid = pgss_hash_key(&key);
		pgstat_drop_entry(PGSTAT_KIND_PGSS, key.dbid, objid, true);

		pgstat_request_entry_refs_gc();
	}
	else
	{
		dshash_seq_init(&hstat, pgss_hash, true);
		while ((p = dshash_seq_next(&hstat)) != NULL)
		{
			PgStatShared_Pgss *shared;

			if (p->dropped)
				continue;

			shared = (PgStatShared_Pgss *) dsa_get_address(pgStatLocal.dsa, p->body);

			if ((!userid || shared->key.userid == userid) &&
				(!dbid || shared->key.dbid == dbid) &&
				(!queryid || shared->key.queryid == queryid))
			{
				if (DsaPointerIsValid(shared->query_text))
				{
					dsa_free(pgss_qtext_dsa, shared->query_text);
					shared->query_text = InvalidDsaPointer;
				}

				pgstat_drop_current(p, &hstat);
				pg_atomic_sub_fetch_u64(&pgss_shared->live_entries, 1);
			}
		}
		dshash_seq_term(&hstat);

		pgstat_request_entry_refs_gc();
	}
	/* If this was a full reset (no filters), reset global statistics */
	if (!userid && !dbid && !queryid)
	{
		pg_atomic_write_u64(&pgss_shared->dealloc, 0);
		pg_atomic_write_u64(&pgss_shared->skipped_entries, 0);
		pg_atomic_write_u64(&pgss_shared->live_entries, 0);
		pg_atomic_write_u64(&pgss_shared->stats_reset, (uint64) stats_reset);
	}

	return stats_reset;
}

/*
 * Generate a normalized version of the query string that will be used to
 * represent all similar queries.
 *
 * Note that the normalized representation may well vary depending on
 * just which "equivalent" query is used to create the hashtable entry.
 * We assume this is OK.
 *
 * If query_loc > 0, then "query" has been advanced by that much compared to
 * the original string start, so we need to translate the provided locations
 * to compensate.  (This lets us avoid re-scanning statements before the one
 * of interest, so it's worth doing.)
 *
 * *query_len_p contains the input string length, and is updated with
 * the result string length on exit.  The resulting string might be longer
 * or shorter depending on what happens with replacement of constants.
 *
 * Returns a palloc'd string.
 */
static char *
generate_normalized_query(const JumbleState *jstate, const char *query,
						  int query_loc, int *query_len_p)
{
	char	   *norm_query;
	int			query_len = *query_len_p;
	int			norm_query_buflen,	/* Space allowed for norm_query */
				len_to_wrt,		/* Length (in bytes) to write */
				quer_loc = 0,	/* Source query byte location */
				n_quer_loc = 0, /* Normalized query byte location */
				last_off = 0,	/* Offset from start for previous tok */
				last_tok_len = 0;	/* Length (in bytes) of that tok */
	int			num_constants_replaced = 0;
	LocationLen *locs = NULL;

	/*
	 * Determine constants' lengths (core system only gives us locations), and
	 * return a sorted copy of jstate's LocationLen data with lengths filled
	 * in.
	 */
	locs = ComputeConstantLengths(jstate, query, query_loc);

	/*
	 * Allow for $n symbols to be longer than the constants they replace.
	 * Constants must take at least one byte in text form, while a $n symbol
	 * certainly isn't more than 11 bytes, even if n reaches INT_MAX.  We
	 * could refine that limit based on the max value of n for the current
	 * query, but it hardly seems worth any extra effort to do so.
	 */
	norm_query_buflen = query_len + jstate->clocations_count * 10;

	/* Allocate result buffer */
	norm_query = palloc(norm_query_buflen + 1);

	for (int i = 0; i < jstate->clocations_count; i++)
	{
		int			off,		/* Offset from start for cur tok */
					tok_len;	/* Length (in bytes) of that tok */

		/*
		 * If we have an external param at this location, but no lists are
		 * being squashed across the query, then we skip here; this will make
		 * us print the characters found in the original query that represent
		 * the parameter in the next iteration (or after the loop is done),
		 * which is a bit odd but seems to work okay in most cases.
		 */
		if (locs[i].extern_param && !jstate->has_squashed_lists)
			continue;

		off = locs[i].location;

		/* Adjust recorded location if we're dealing with partial string */
		off -= query_loc;

		tok_len = locs[i].length;

		if (tok_len < 0)
			continue;			/* ignore any duplicates */

		/* Copy next chunk (what precedes the next constant) */
		len_to_wrt = off - last_off;
		len_to_wrt -= last_tok_len;
		Assert(len_to_wrt >= 0);
		memcpy(norm_query + n_quer_loc, query + quer_loc, len_to_wrt);
		n_quer_loc += len_to_wrt;

		/*
		 * And insert a param symbol in place of the constant token; and, if
		 * we have a squashable list, insert a placeholder comment starting
		 * from the list's second value.
		 */
		n_quer_loc += sprintf(norm_query + n_quer_loc, "$%d%s",
							  num_constants_replaced + 1 + jstate->highest_extern_param_id,
							  locs[i].squashed ? " /*, ... */" : "");
		num_constants_replaced++;

		/* move forward */
		quer_loc = off + tok_len;
		last_off = off;
		last_tok_len = tok_len;
	}

	/* Clean up, if needed */
	if (locs)
		pfree(locs);

	/*
	 * We've copied up until the last ignorable constant.  Copy over the
	 * remaining bytes of the original query string.
	 */
	len_to_wrt = query_len - quer_loc;

	Assert(len_to_wrt >= 0);
	memcpy(norm_query + n_quer_loc, query + quer_loc, len_to_wrt);
	n_quer_loc += len_to_wrt;

	Assert(n_quer_loc <= norm_query_buflen);
	norm_query[n_quer_loc] = '\0';

	*query_len_p = n_quer_loc;
	return norm_query;
}
