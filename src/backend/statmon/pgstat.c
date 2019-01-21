/* ----------
 * pgstat.c
 *
 *	Statistics collector facility.
 *
 *	Statistics data is stored in dynamic shared memory. Every backends
 *	updates and read it individually.
 *
 *	Copyright (c) 2001-2019, PostgreSQL Global Development Group
 *
 *	src/backend/statmon/pgstat.c
 * ----------
 */
#include "postgres.h"

#include <unistd.h>

#include "pgstat.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "bestatus.h"
#include "catalog/pg_database.h"
#include "catalog/pg_proc.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/procsignal.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

/* ----------
 * Timer definitions.
 * ----------
 */
#define PGSTAT_STAT_MIN_INTERVAL	500 /* Minimum time between stats data
										 * updates; in milliseconds. */

#define PGSTAT_STAT_MAX_INTERVAL   1000 /* Maximum time between stats data
										 * updates; in milliseconds. */

/* ----------
 * The initial size hints for the hash tables used in the collector.
 * ----------
 */
#define PGSTAT_TAB_HASH_SIZE	512
#define PGSTAT_FUNCTION_HASH_SIZE	512

/*
 * Operation mode of pgstat_get_db_entry.
 */
#define PGSTAT_FETCH_SHARED	0
#define PGSTAT_FETCH_EXCLUSIVE	1
#define	PGSTAT_FETCH_NOWAIT 2

typedef enum
{
	PGSTAT_ENTRY_NOT_FOUND,
	PGSTAT_ENTRY_FOUND,
	PGSTAT_ENTRY_LOCK_FAILED
} pg_stat_table_result_status;

/* ----------
 * GUC parameters
 * ----------
 */
bool		pgstat_track_counts = false;
int			pgstat_track_functions = TRACK_FUNC_OFF;

/*
 * This was a GUC parameter and no longer used in this file. But left alone
 * just for backward comptibility for extensions, having the default value.
 */
char	   *pgstat_stat_directory = PG_STAT_TMP_DIR;

/* Shared stats bootstrap infomation */
typedef struct StatsShmemStruct {
	dsa_handle stats_dsa_handle;
	dshash_table_handle db_stats_handle;
	dsa_pointer	global_stats;
	dsa_pointer	archiver_stats;
} StatsShmemStruct;


/*
 * BgWriter global statistics counters (unused in other processes).
 * Stored directly in a stats message structure so it can be sent
 * without needing to copy things around.  We assume this inits to zeroes.
 */
PgStat_BgWriter BgWriterStats;

/* ----------
 * Local data
 * ----------
 */
static StatsShmemStruct * StatsShmem = NULL;
static dsa_area *area = NULL;
static dshash_table *db_stats;
static HTAB *snapshot_db_stats;
static MemoryContext stats_cxt;

/*
 *  report withholding facility.
 *
 *  some report items are withholded if required lock is not acquired
 *  immediately.
 */
static bool pgstat_pending_recoveryconflict = false;
static bool pgstat_pending_deadlock = false;
static bool pgstat_pending_tempfile = false;

static MemoryContext pgStatLocalContext = NULL;

/* dshash parameter for each type of table */
static const dshash_parameters dsh_dbparams = {
	sizeof(Oid),
	sizeof(PgStat_StatDBEntry),
	dshash_memcmp,
	dshash_memhash,
	LWTRANCHE_STATS_DB
};
static const dshash_parameters dsh_tblparams = {
	sizeof(Oid),
	sizeof(PgStat_StatTabEntry),
	dshash_memcmp,
	dshash_memhash,
	LWTRANCHE_STATS_FUNC_TABLE
};
static const dshash_parameters dsh_funcparams = {
	sizeof(Oid),
	sizeof(PgStat_StatFuncEntry),
	dshash_memcmp,
	dshash_memhash,
	LWTRANCHE_STATS_FUNC_TABLE
};

/*
 * Structures in which backends store per-table info that's waiting to be
 * sent to the collector.
 *
 * NOTE: once allocated, TabStatusArray structures are never moved or deleted
 * for the life of the backend.  Also, we zero out the t_id fields of the
 * contained PgStat_TableStatus structs whenever they are not actively in use.
 * This allows relcache pgstat_info pointers to be treated as long-lived data,
 * avoiding repeated searches in pgstat_initstats() when a relation is
 * repeatedly opened during a transaction.
 */
#define TABSTAT_QUANTUM		100 /* we alloc this many at a time */

typedef struct TabStatusArray
{
	struct TabStatusArray *tsa_next;	/* link to next array, if any */
	int			tsa_used;		/* # entries currently used */
	PgStat_TableStatus tsa_entries[TABSTAT_QUANTUM];	/* per-table data */
} TabStatusArray;

static TabStatusArray *pgStatTabList = NULL;

/*
 * pgStatTabHash entry: map from relation OID to PgStat_TableStatus pointer
 */
typedef struct TabStatHashEntry
{
	Oid			t_id;
	PgStat_TableStatus *tsa_entry;
} TabStatHashEntry;

/*
 * Hash table for O(1) t_id -> tsa_entry lookup
 */
static HTAB *pgStatTabHash = NULL;
static HTAB *pgStatPendingTabHash = NULL;

/*
 * Backends store per-function info that's waiting to be sent to the collector
 * in this hash table (indexed by function OID).
 */
static HTAB *pgStatFunctions = NULL;
static HTAB *pgStatPendingFunctions = NULL;

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

typedef struct
{
	dshash_table *tabhash;
	PgStat_StatDBEntry *dbentry;
} pgstat_apply_tabstat_context;

/*
 * Info about current "snapshot" of stats file
 */
static HTAB *pgStatDBHash = NULL;

/*
 * Cluster wide statistics.
 * Contains statistics that are not collected per database or per table.
 * shared_* are the statistics maintained by pgstats and snapshot_* are the
 * snapshots taken on backends.
 */
static PgStat_ArchiverStats *shared_archiverStats;
static PgStat_ArchiverStats *snapshot_archiverStats;
static PgStat_GlobalStats *shared_globalStats;
static PgStat_GlobalStats *snapshot_globalStats;

/*
 * Total time charged to functions so far in the current backend.
 * We use this to help separate "self" and "other" time charges.
 * (We assume this initializes to zero.)
 */
static instr_time total_func_time;


/* ----------
 * Local function forward declarations
 * ----------
 */
/* functions used in backends */
static void pgstat_beshutdown_hook(int code, Datum arg);

static PgStat_StatDBEntry *pgstat_get_db_entry(Oid databaseid, int op,
									pg_stat_table_result_status *status);
static PgStat_StatTabEntry *pgstat_get_tab_entry(dshash_table *table, Oid tableoid, bool create);
static void pgstat_write_db_statsfile(PgStat_StatDBEntry *dbentry);
static void pgstat_read_db_statsfile(Oid databaseid, dshash_table *tabhash, dshash_table *funchash);

/* functions used in backends */
static bool backend_snapshot_global_stats(void);
static PgStat_StatFuncEntry *backend_get_func_etnry(PgStat_StatDBEntry *dbent, Oid funcid, bool oneshot);

static void pgstat_postmaster_shutdown(int code, Datum arg);
static void pgstat_apply_pending_tabstats(bool shared, bool force,
							   pgstat_apply_tabstat_context *cxt);
static bool pgstat_apply_tabstat(pgstat_apply_tabstat_context *cxt,
								 PgStat_TableStatus *entry, bool nowait);
static void pgstat_merge_tabentry(PgStat_TableStatus *deststat,
										  PgStat_TableStatus *srcstat,
										  bool init);
static void pgstat_update_funcstats(bool force, PgStat_StatDBEntry *dbentry);
static void pgstat_reset_all_counters(void);
static void pgstat_cleanup_recovery_conflict(PgStat_StatDBEntry *dbentry);
static void pgstat_cleanup_deadlock(PgStat_StatDBEntry *dbentry);
static void pgstat_cleanup_tempfile(PgStat_StatDBEntry *dbentry);

static inline void pgstat_merge_backendstats_to_funcentry(
	PgStat_StatFuncEntry *dest, PgStat_BackendFunctionEntry *src, bool init);
static inline void pgstat_merge_funcentry(
	PgStat_StatFuncEntry *dest, PgStat_StatFuncEntry *src, bool init);

static HTAB *pgstat_collect_oids(Oid catalogid, AttrNumber anum_oid);
static void reset_dbentry_counters(PgStat_StatDBEntry *dbentry);
static PgStat_TableStatus *get_tabstat_entry(Oid rel_id, bool isshared);

static void pgstat_setup_memcxt(void);

static bool pgstat_update_tabentry(dshash_table *tabhash,
								   PgStat_TableStatus *stat, bool nowait);
static void pgstat_update_dbentry(PgStat_StatDBEntry *dbentry,
								  PgStat_TableStatus *stat);

/* ------------------------------------------------------------
 * Public functions called from postmaster follow
 * ------------------------------------------------------------
 */


void
pgstat_initialize(void)
{
	/* Set up a process-exit hook to clean up */
	before_shmem_exit(pgstat_beshutdown_hook, 0);
}

/*
 * subroutine for pgstat_reset_all
 */
static void
pgstat_reset_remove_files(const char *directory)
{
	DIR		   *dir;
	struct dirent *entry;
	char		fname[MAXPGPATH * 2];

	dir = AllocateDir(directory);
	while ((entry = ReadDir(dir, directory)) != NULL)
	{
		int			nchars;
		Oid			tmp_oid;

		/*
		 * Skip directory entries that don't match the file names we write.
		 * See get_dbstat_filename for the database-specific pattern.
		 */
		if (strncmp(entry->d_name, "global.", 7) == 0)
			nchars = 7;
		else
		{
			nchars = 0;
			(void) sscanf(entry->d_name, "db_%u.%n",
						  &tmp_oid, &nchars);
			if (nchars <= 0)
				continue;
			/* %u allows leading whitespace, so reject that */
			if (strchr("0123456789", entry->d_name[3]) == NULL)
				continue;
		}

		if (strcmp(entry->d_name + nchars, "tmp") != 0 &&
			strcmp(entry->d_name + nchars, "stat") != 0)
			continue;

		snprintf(fname, sizeof(fname), "%s/%s", directory,
				 entry->d_name);
		unlink(fname);
	}
	FreeDir(dir);
}

/*
 * pgstat_reset_all() -
 *
 * Remove the stats files and on-memory counters.  This is currently used only
 * if WAL recovery is needed after a crash.
 */
void
pgstat_reset_all(void)
{
	pgstat_reset_remove_files(PGSTAT_STAT_PERMANENT_DIRECTORY);
	pgstat_reset_all_counters();
}

/* ----------
 * pgstat_create_shared_stats() -
 *
 *	create shared stats memory
 * ----------
 */
static void
pgstat_create_shared_stats(void)
{
	MemoryContext oldcontext;

	Assert(StatsShmem->stats_dsa_handle == DSM_HANDLE_INVALID);

	/* lives for the lifetime of the process */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	area = dsa_create(LWTRANCHE_STATS_DSA);
	dsa_pin_mapping(area);

	db_stats = dshash_create(area, &dsh_dbparams, 0);

	/* create shared area and write bootstrap information */
	StatsShmem->stats_dsa_handle = dsa_get_handle(area);
	StatsShmem->global_stats =
		dsa_allocate0(area, sizeof(PgStat_GlobalStats));
	StatsShmem->archiver_stats =
		dsa_allocate0(area, sizeof(PgStat_ArchiverStats));
	StatsShmem->db_stats_handle =
		dshash_get_hash_table_handle(db_stats);

	/* connect to the memory */
	snapshot_db_stats = NULL;
	shared_globalStats = (PgStat_GlobalStats *)
		dsa_get_address(area, StatsShmem->global_stats);
	shared_archiverStats = (PgStat_ArchiverStats *)
		dsa_get_address(area, StatsShmem->archiver_stats);
	MemoryContextSwitchTo(oldcontext);
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
 *  This requires taking some locks on the shared statistics hashes and some
 *  of updates may be withholded on lock failure. Pending updates are
 *  retried in later call of this function and finally cleaned up by calling
 *  this function with force = true or PGSTAT_STAT_MAX_INTERVAL milliseconds
 *  was elapsed since last cleanup. On the other hand updates by regular
 *  backends happen with the interval not shorter than
 *  PGSTAT_STAT_MIN_INTERVAL when force = false.
 *
 *  Returns time in milliseconds until the next update time.
 *
 *	Note that this is called only when not within a transaction, so it is fair
 *	to use transaction stop time as an approximation of current time.
 *	----------
 */
long
pgstat_update_stat(bool force)
{
	/* we assume this inits to all zeroes: */
	static TimestampTz last_report = 0;
	static TimestampTz oldest_pending = 0;
	TimestampTz now;
	TabStatusArray *tsa;
	pgstat_apply_tabstat_context cxt;
	bool		other_pending_stats = false;
	long elapsed;
	long secs;
	int	 usecs;

	if (pgstat_pending_recoveryconflict ||
		pgstat_pending_deadlock ||
		pgstat_pending_tempfile ||
		pgStatPendingFunctions)
		other_pending_stats = true;

	/* Don't expend a clock check if nothing to do */
	if (!other_pending_stats && !pgStatPendingTabHash &&
		(pgStatTabList == NULL || pgStatTabList->tsa_used == 0) &&
		pgStatXactCommit == 0 && pgStatXactRollback == 0)
		return 0;

	now = GetCurrentTransactionStopTimestamp();

	if (!force)
	{
		/*
		 * Don't update shared stats unless it's been at least
		 * PGSTAT_STAT_MIN_INTERVAL msec since we last updated one.
		 * Returns time to wait in the case.
		 */
		TimestampDifference(last_report, now, &secs, &usecs);
		elapsed = secs * 1000 + usecs /1000;

		if(elapsed < PGSTAT_STAT_MIN_INTERVAL)
		{
			/* we know we have some statistics */
			if (oldest_pending == 0)
				oldest_pending = now;

			return PGSTAT_STAT_MIN_INTERVAL - elapsed;
		}


		/*
		 * Don't keep pending stats for longer than PGSTAT_STAT_MAX_INTERVAL.
		 */
		if (oldest_pending > 0)
		{
			TimestampDifference(oldest_pending, now, &secs, &usecs);
			elapsed = secs * 1000 + usecs /1000;

			if(elapsed > PGSTAT_STAT_MAX_INTERVAL)
				force = true;
		}
	}

	last_report = now;

	/* setup stats update context*/
	cxt.dbentry = NULL;
	cxt.tabhash = NULL;

	/* Forecibly update other stats if any. */
	if (other_pending_stats)
	{
		cxt.dbentry =
			pgstat_get_db_entry(MyDatabaseId, PGSTAT_FETCH_EXCLUSIVE, NULL);

		/* clean up pending statistics if any */
		if (pgStatPendingFunctions)
			pgstat_update_funcstats(true, cxt.dbentry);
		if (pgstat_pending_recoveryconflict)
			pgstat_cleanup_recovery_conflict(cxt.dbentry);
		if (pgstat_pending_deadlock)
			pgstat_cleanup_deadlock(cxt.dbentry);
		if (pgstat_pending_tempfile)
			pgstat_cleanup_tempfile(cxt.dbentry);
	}

	/*
	 * Destroy pgStatTabHash before we start invalidating PgStat_TableEntry
	 * entries it points to.  (Should we fail partway through the loop below,
	 * it's okay to have removed the hashtable already --- the only
	 * consequence is we'd get multiple entries for the same table in the
	 * pgStatTabList, and that's safe.)
	 */
	if (pgStatTabHash)
		hash_destroy(pgStatTabHash);
	pgStatTabHash = NULL;

	/*
	 * XX: We cannot lock two dshash entries at once. Since we must keep lock
	 * while tables stats are being updated we have no choice other than
	 * separating jobs for shared table stats and that of egular tables.
	 * Looping over the array twice isapparently ineffcient and more efficient
	 * way is expected.
	 */

	/* The first call of the followings uses dbentry obtained above if any.*/
	pgstat_apply_pending_tabstats(false, force, &cxt);
	pgstat_apply_pending_tabstats(true, force, &cxt);

	/* zero out TableStatus structs after use */
	for (tsa = pgStatTabList; tsa != NULL; tsa = tsa->tsa_next)
	{
		MemSet(tsa->tsa_entries, 0,
			   tsa->tsa_used * sizeof(PgStat_TableStatus));
		tsa->tsa_used = 0;
	}

	/* record oldest pending update time */
	if (pgStatPendingTabHash == NULL)
		oldest_pending = 0;
	else if (oldest_pending == 0)
		oldest_pending = now;

	return 0;
}

/*
 * Subroutine for pgstat_update_stat.
 *
 * Appies table stats in table status array merging with pending stats if any.
 * If force is true waits until required locks to be acquired. Elsewise stats
 * merged stats as pending sats and it will be processed in the next chance.
 */
static void
pgstat_apply_pending_tabstats(bool shared, bool force,
							  pgstat_apply_tabstat_context *cxt)
{
	static const PgStat_TableCounts all_zeroes;
	TabStatusArray *tsa;
	int i;

	for (tsa = pgStatTabList; tsa != NULL; tsa = tsa->tsa_next)
	{
		for (i = 0; i < tsa->tsa_used; i++)
		{
			PgStat_TableStatus *entry = &tsa->tsa_entries[i];
			PgStat_TableStatus *pentry = NULL;

			/* Shouldn't have any pending transaction-dependent counts */
			Assert(entry->trans == NULL);

			/*
			 * Ignore entries that didn't accumulate any actual counts, such
			 * as indexes that were opened by the planner but not used.
			 */
			if (memcmp(&entry->t_counts, &all_zeroes,
					   sizeof(PgStat_TableCounts)) == 0)
				continue;

			/* Skip if this entry is not match the request */
			if (entry->t_shared != shared)
				continue;

			/* if pending update exists, it should be applied along with */
			if (pgStatPendingTabHash != NULL)
			{
				pentry = hash_search(pgStatPendingTabHash,
									 (void *) entry, HASH_FIND, NULL);

				if (pentry)
				{
					/* merge new update into pending updates */
					pgstat_merge_tabentry(pentry, entry, false);
					entry = pentry;
				}
			}

			/* try to apply the merged stats */
			if (pgstat_apply_tabstat(cxt, entry, !force))
			{
				/* succeeded. remove it if it was pending stats */
				if (pentry && entry != pentry)
					hash_search(pgStatPendingTabHash,
								(void *) pentry, HASH_REMOVE, NULL);
			}
			else if (!pentry)
			{
				/* failed and there was no pending entry, create new one. */
				bool found;

				if (pgStatPendingTabHash == NULL)
				{
					HASHCTL		ctl;

					memset(&ctl, 0, sizeof(ctl));
					ctl.keysize = sizeof(Oid);
					ctl.entrysize = sizeof(PgStat_TableStatus);
					pgStatPendingTabHash =
						hash_create("pgstat pending table stats hash",
									TABSTAT_QUANTUM,
									&ctl,
									HASH_ELEM | HASH_BLOBS);
				}

				pentry = hash_search(pgStatPendingTabHash,
									 (void *) entry, HASH_ENTER, &found);
				Assert (!found);

				*pentry = *entry;
			}
		}
	}

	/* if any pending stats exists, try to clean it up */
	if (pgStatPendingTabHash != NULL)
	{
		HASH_SEQ_STATUS pstat;
		PgStat_TableStatus *pentry;

		hash_seq_init(&pstat, pgStatPendingTabHash);
		while((pentry = (PgStat_TableStatus *) hash_seq_search(&pstat)) != NULL)
		{
			/* Skip if this entry is not match the request */
			if (pentry->t_shared != shared)
				continue;

			/* apply pending entry and remove on success */
			if (pgstat_apply_tabstat(cxt, pentry, !force))
				hash_search(pgStatPendingTabHash,
							(void *) pentry, HASH_REMOVE, NULL);
		}

		/* destroy the hash if no entry is left */
		if (hash_get_num_entries(pgStatPendingTabHash) == 0)
		{
			hash_destroy(pgStatPendingTabHash);
			pgStatPendingTabHash = NULL;
		}
	}

	if (cxt->tabhash)
		dshash_detach(cxt->tabhash);
	if (cxt->dbentry)
		dshash_release_lock(db_stats, cxt->dbentry);
	cxt->tabhash = NULL;
	cxt->dbentry = NULL;
}


/*
 * pgstat_apply_tabstat: update shared stats entry using given entry
 *
 * If nowait is true, just returns false on lock failure.  Dshashes for table
 * and function stats are kept attached and stored in ctx. The caller must
 * detach them after use.
 */
bool
pgstat_apply_tabstat(pgstat_apply_tabstat_context *cxt,
					 PgStat_TableStatus *entry, bool nowait)
{
	Oid dboid = entry->t_shared ? InvalidOid : MyDatabaseId;
	int		table_mode = PGSTAT_FETCH_EXCLUSIVE;
	bool updated = false;

	if (nowait)
		table_mode |= PGSTAT_FETCH_NOWAIT;

	/*
	 * We need to keep lock on dbentries for regular tables to avoid race
	 * condition with drop database. So we hold it in the context variable. We
	 * don't need that for shared tables.
	 */
	if (!cxt->dbentry)
		cxt->dbentry = pgstat_get_db_entry(dboid, table_mode, NULL);

	/* we cannot acquire lock, just return */
	if (!cxt->dbentry)
		return false;

	/* attach shared stats table if not yet */
	if (!cxt->tabhash)
	{
		/* apply database stats  */
		if (!entry->t_shared)
		{
			/* Update database-wide stats  */
			cxt->dbentry->n_xact_commit += pgStatXactCommit;
			cxt->dbentry->n_xact_rollback += pgStatXactRollback;
			cxt->dbentry->n_block_read_time += pgStatBlockReadTime;
			cxt->dbentry->n_block_write_time += pgStatBlockWriteTime;
			pgStatXactCommit = 0;
			pgStatXactRollback = 0;
			pgStatBlockReadTime = 0;
			pgStatBlockWriteTime = 0;
		}

		cxt->tabhash =
			dshash_attach(area, &dsh_tblparams, cxt->dbentry->tables, 0);
	}

	/*
	 * If we have access to the required data, try update table stats first.
	 * Update database stats only if the first step suceeded.
	 */
	if (pgstat_update_tabentry(cxt->tabhash, entry, nowait))
	{
		pgstat_update_dbentry(cxt->dbentry, entry);
		updated = true;
	}

	return updated;
}

/*
 * pgstat_merge_tabentry: subroutine for pgstat_update_stat
 *
 * Merge srcstat into deststat. Existing value in deststat is cleard if
 * init is true.
 */
static void
pgstat_merge_tabentry(PgStat_TableStatus *deststat,
					  PgStat_TableStatus *srcstat,
					  bool init)
{
	Assert (deststat != srcstat);

	if (init)
		deststat->t_counts = srcstat->t_counts;
	else
	{
		PgStat_TableCounts *dest = &deststat->t_counts;
		PgStat_TableCounts *src = &srcstat->t_counts;

		dest->t_numscans += src->t_numscans;
		dest->t_tuples_returned += src->t_tuples_returned;
		dest->t_tuples_fetched += src->t_tuples_fetched;
		dest->t_tuples_inserted += src->t_tuples_inserted;
		dest->t_tuples_updated += src->t_tuples_updated;
		dest->t_tuples_deleted += src->t_tuples_deleted;
		dest->t_tuples_hot_updated += src->t_tuples_hot_updated;
		dest->t_truncated |= src->t_truncated;

		/* If table was truncated, first reset the live/dead counters */
		if (src->t_truncated)
		{
			dest->t_delta_live_tuples = 0;
			dest->t_delta_dead_tuples = 0;
		}
		dest->t_delta_live_tuples += src->t_delta_live_tuples;
		dest->t_delta_dead_tuples += src->t_delta_dead_tuples;
		dest->t_changed_tuples += src->t_changed_tuples;
		dest->t_blocks_fetched += src->t_blocks_fetched;
		dest->t_blocks_hit += src->t_blocks_hit;
	}
}

/*
 * pgstat_update_funcstats: subroutine for pgstat_update_stat
 *
 *  updates a function stat
 */
static void
pgstat_update_funcstats(bool force, PgStat_StatDBEntry *dbentry)
{
	/* we assume this inits to all zeroes: */
	static const PgStat_FunctionCounts all_zeroes;
	pg_stat_table_result_status status = 0;
	dshash_table *funchash;
	bool		  nowait = !force;
	bool		  release_db = false;
	int			  table_op = PGSTAT_FETCH_EXCLUSIVE;

	if (pgStatFunctions == NULL && pgStatPendingFunctions == NULL)
		return;

	if (nowait)
		table_op += PGSTAT_FETCH_NOWAIT;

	/* find the shared function stats table */
	if (!dbentry)
	{
		dbentry = pgstat_get_db_entry(MyDatabaseId, table_op, &status);
		release_db = true;
	}

	/* lock failure, return. */
	if (status == PGSTAT_ENTRY_LOCK_FAILED)
		return;

	/* create hash if not yet */
	if (dbentry->functions == DSM_HANDLE_INVALID)
	{
		funchash = dshash_create(area, &dsh_funcparams, 0);
		dbentry->functions = dshash_get_hash_table_handle(funchash);
	}
	else
		funchash = dshash_attach(area, &dsh_funcparams, dbentry->functions, 0);

	/*
	 * First, we empty the transaction stats. Just move numbers to pending
	 * stats if any. Elsewise try to directly update the shared stats but
	 * create a new pending entry on lock failure.
	 */
	if (pgStatFunctions)
	{
		HASH_SEQ_STATUS fstat;
		PgStat_BackendFunctionEntry *bestat;

		hash_seq_init(&fstat, pgStatFunctions);
		while ((bestat = (PgStat_BackendFunctionEntry *) hash_seq_search(&fstat)) != NULL)
		{
			bool found;
			bool init = false;
			PgStat_StatFuncEntry *funcent = NULL;

			/* Skip it if no counts accumulated since last time */
			if (memcmp(&bestat->f_counts, &all_zeroes,
					   sizeof(PgStat_FunctionCounts)) == 0)
				continue;

			/* find pending entry */
			if (pgStatPendingFunctions)
				funcent = (PgStat_StatFuncEntry *)
					hash_search(pgStatPendingFunctions,
								(void *) &(bestat->f_id), HASH_FIND, NULL);

			if (!funcent)
			{
				/* pending entry not found, find shared stats entry */
				funcent = (PgStat_StatFuncEntry *)
					dshash_find_or_insert_extended(funchash,
												   (void *) &(bestat->f_id),
												   &found, nowait);
				if (funcent)
					init = !found;
				else
				{
					/* no shared stats entry. create a new pending one */
					funcent = (PgStat_StatFuncEntry *)
						hash_search(pgStatPendingFunctions,
									(void *) &(bestat->f_id), HASH_ENTER, NULL);
					init = true;
				}
			}
			Assert (funcent != NULL);

			pgstat_merge_backendstats_to_funcentry(funcent, bestat, init);

			/* reset used counts */
			MemSet(&bestat->f_counts, 0, sizeof(PgStat_FunctionCounts));
		}
	}

	/* Second, apply pending stats numbers to shared table */
	if (pgStatPendingFunctions)
	{
		HASH_SEQ_STATUS fstat;
		PgStat_StatFuncEntry *pendent;

		hash_seq_init(&fstat, pgStatPendingFunctions);
		while ((pendent = (PgStat_StatFuncEntry *) hash_seq_search(&fstat)) != NULL)
		{
			PgStat_StatFuncEntry *funcent;
			bool found;

			funcent = (PgStat_StatFuncEntry *)
				dshash_find_or_insert_extended(funchash,
											   (void *) &(pendent->functionid),
											   &found, nowait);
			if (funcent)
			{
				pgstat_merge_funcentry(pendent, funcent, !found);
				hash_search(pgStatPendingFunctions,
							(void *) &(pendent->functionid), HASH_REMOVE, NULL);
			}
		}

		/* destroy the hsah if no entry remains */
		if (hash_get_num_entries(pgStatPendingFunctions) == 0)
		{
			hash_destroy(pgStatPendingFunctions);
			pgStatPendingFunctions = NULL;
		}
	}

	if (release_db)
		dshash_release_lock(db_stats, dbentry);
}

/*
 * pgstat_merge_backendstats_to_funcentry: subroutine for
 *											 pgstat_update_funcstats
 *
 * Merges BackendFunctionEntry into StatFuncEntry
 */
static inline void
pgstat_merge_backendstats_to_funcentry(PgStat_StatFuncEntry *dest,
									   PgStat_BackendFunctionEntry *src,
									   bool init)
{
	if (init)
	{
		/*
		 * If it's a new function entry, initialize counters to the values
		 * we just got.
		 */
		dest->f_numcalls = src->f_counts.f_numcalls;
		dest->f_total_time =
			INSTR_TIME_GET_MICROSEC(src->f_counts.f_total_time);
		dest->f_self_time =
			INSTR_TIME_GET_MICROSEC(src->f_counts.f_self_time);
	}
	else
	{
		/*
		 * Otherwise add the values to the existing entry.
		 */
		dest->f_numcalls += src->f_counts.f_numcalls;
		dest->f_total_time +=
			INSTR_TIME_GET_MICROSEC(src->f_counts.f_total_time);
		dest->f_self_time +=
			INSTR_TIME_GET_MICROSEC(src->f_counts.f_self_time);
	}
}

/*
 * pgstat_merge_funcentry: subroutine for pgstat_update_funcstats
 *
 * Merges two StatFuncEntrys
 */
static inline void
pgstat_merge_funcentry(PgStat_StatFuncEntry *dest, PgStat_StatFuncEntry *src,
					   bool init)
{
	if (init)
	{
		/*
		 * If it's a new function entry, initialize counters to the values
		 * we just got.
		 */
		dest->f_numcalls = src->f_numcalls;
		dest->f_total_time = src->f_total_time;
		dest->f_self_time = src->f_self_time;
	}
	else
	{
		/*
		 * Otherwise add the values to the existing entry.
		 */
		dest->f_numcalls += src->f_numcalls;
		dest->f_total_time += src->f_total_time;
		dest->f_self_time += src->f_self_time;
	}
}



/* ----------
 * pgstat_vacuum_stat() -
 *
 *	Remove objects he can get rid of.
 * ----------
 */
void
pgstat_vacuum_stat(void)
{
	HTAB	   *oidtab;
	dshash_table *dshtable;
	dshash_seq_status dshstat;
	PgStat_StatDBEntry *dbentry;
	PgStat_StatTabEntry *tabentry;
	PgStat_StatFuncEntry *funcentry;

	/* we don't collect statistics under standalone mode */
	if (!IsUnderPostmaster)
		return;

	/* If not done for this transaction, take a snapshot of stats */
	if (!backend_snapshot_global_stats())
		return;

	/*
	 * Read pg_database and make a list of OIDs of all existing databases
	 */
	oidtab = pgstat_collect_oids(DatabaseRelationId, Anum_pg_database_oid);

	/*
	 * Search the database hash table for dead databases and drop them
	 * from the hash.
	 */

	dshash_seq_init(&dshstat, db_stats, false, true);
	while ((dbentry = (PgStat_StatDBEntry *) dshash_seq_next(&dshstat)) != NULL)
	{
		Oid			dbid = dbentry->databaseid;

		CHECK_FOR_INTERRUPTS();

		/* the DB entry for shared tables (with InvalidOid) is never dropped */
		if (OidIsValid(dbid) &&
			hash_search(oidtab, (void *) &dbid, HASH_FIND, NULL) == NULL)
			pgstat_drop_database(dbid);
	}

	/* Clean up */
	hash_destroy(oidtab);

	/*
	 * Lookup our own database entry; if not found, nothing more to do.
	 */
	dbentry = pgstat_get_db_entry(MyDatabaseId, PGSTAT_FETCH_EXCLUSIVE, NULL);
	if (!dbentry)
		return;

	/*
	 * Similarly to above, make a list of all known relations in this DB.
	 */
	oidtab = pgstat_collect_oids(RelationRelationId, Anum_pg_class_oid);

	/*
	 * Check for all tables listed in stats hashtable if they still exist.
	 * Stats cache is useless here so directly search the shared hash.
	 */
	dshtable = dshash_attach(area, &dsh_tblparams, dbentry->tables, 0);
	dshash_seq_init(&dshstat, dshtable, false, true);
	while ((tabentry = (PgStat_StatTabEntry *) dshash_seq_next(&dshstat)) != NULL)
	{
		Oid			tabid = tabentry->tableid;

		CHECK_FOR_INTERRUPTS();

		if (hash_search(oidtab, (void *) &tabid, HASH_FIND, NULL) != NULL)
			continue;

		/* Not there, so purge this table */
		dshash_delete_entry(dshtable, tabentry);
	}
	dshash_detach(dshtable);

	/* Clean up */
	hash_destroy(oidtab);

	/*
	 * Now repeat the above steps for functions.  However, we needn't bother
	 * in the common case where no function stats are being collected.
	 */
	if (dbentry->functions != DSM_HANDLE_INVALID)
	{
		dshtable = dshash_attach(area, &dsh_funcparams, dbentry->functions, 0);
		oidtab = pgstat_collect_oids(ProcedureRelationId, Anum_pg_proc_oid);

		dshash_seq_init(&dshstat, dshtable, false, true);
		while ((funcentry = (PgStat_StatFuncEntry *) dshash_seq_next(&dshstat)) != NULL)
		{
			Oid			funcid = funcentry->functionid;

			CHECK_FOR_INTERRUPTS();

			if (hash_search(oidtab, (void *) &funcid, HASH_FIND, NULL) != NULL)
				continue;

			/* Not there, so remove this function */
			dshash_delete_entry(dshtable, funcentry);
		}

		hash_destroy(oidtab);

		dshash_detach(dshtable);
	}
	dshash_release_lock(db_stats, dbentry);
}


/*
 * pgstat_collect_oids() -
 *
 *	Collect the OIDs of all objects listed in the specified system catalog
 *	into a temporary hash table.  Caller should hash_destroy the result after
 *	use.  (However, we make the table in CurrentMemoryContext so that it will
 *	be freed properly in event of an error.)
 */
static HTAB *
pgstat_collect_oids(Oid catalogid, AttrNumber anum_oid)
{
	HTAB	   *htab;
	HASHCTL		hash_ctl;
	Relation	rel;
	HeapScanDesc scan;
	HeapTuple	tup;
	Snapshot	snapshot;

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(Oid);
	hash_ctl.hcxt = CurrentMemoryContext;
	htab = hash_create("Temporary table of OIDs",
					   PGSTAT_TAB_HASH_SIZE,
					   &hash_ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	rel = heap_open(catalogid, AccessShareLock);
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scan = heap_beginscan(rel, snapshot, 0, NULL);
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Oid			thisoid;
		bool		isnull;

		thisoid = heap_getattr(tup, anum_oid, RelationGetDescr(rel), &isnull);
		Assert(!isnull);

		CHECK_FOR_INTERRUPTS();

		(void) hash_search(htab, (void *) &thisoid, HASH_ENTER, NULL);
	}
	heap_endscan(scan);
	UnregisterSnapshot(snapshot);
	heap_close(rel, AccessShareLock);

	return htab;
}


/* ----------
 * pgstat_drop_database() -
 *
 *	Remove entry for the database that we just dropped.
 *
 *  If some stats update happens after this, this entry will re-created but
 *	we will still clean the dead DB eventually via future invocations of
 *	pgstat_vacuum_stat().
 * ----------
 */

void
pgstat_drop_database(Oid databaseid)
{
	PgStat_StatDBEntry *dbentry;

	Assert (OidIsValid(databaseid));
	Assert(db_stats);

	/*
	 * Lookup the database in the hashtable with exclusive lock.
	 */
	dbentry = pgstat_get_db_entry(databaseid, PGSTAT_FETCH_EXCLUSIVE, NULL);

	/*
	 * If found, remove it (along with the db statfile).
	 */
	if (dbentry)
	{
		if (dbentry->tables != DSM_HANDLE_INVALID)
		{
			dshash_table *tbl =
				dshash_attach(area, &dsh_tblparams, dbentry->tables, 0);
			dshash_destroy(tbl);
		}
		if (dbentry->functions != DSM_HANDLE_INVALID)
		{
			dshash_table *tbl =
				dshash_attach(area, &dsh_funcparams, dbentry->functions, 0);
			dshash_destroy(tbl);
		}

		dshash_delete_entry(db_stats, (void *)dbentry);
	}
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
	PgStat_StatDBEntry		   *dbentry;
	pg_stat_table_result_status status;

	Assert(db_stats);

	/*
	 * Lookup the database in the hashtable.  Nothing to do if not there.
	 */
	dbentry = pgstat_get_db_entry(MyDatabaseId, PGSTAT_FETCH_EXCLUSIVE, &status);

	if (!dbentry)
		return;

	/*
	 * We simply throw away all the database's table entries by recreating a
	 * new hash table for them.
	 */
	if (dbentry->tables != DSM_HANDLE_INVALID)
	{
		dshash_table *t =
			dshash_attach(area, &dsh_tblparams, dbentry->tables, 0);
		dshash_destroy(t);
		dbentry->tables = DSM_HANDLE_INVALID;
	}
	if (dbentry->functions != DSM_HANDLE_INVALID)
	{
		dshash_table *t =
			dshash_attach(area, &dsh_funcparams, dbentry->functions, 0);
		dshash_destroy(t);
		dbentry->functions = DSM_HANDLE_INVALID;
	}

	/*
	 * Reset database-level stats, too.  This creates empty hash tables for
	 * tables and functions.
	 */
	reset_dbentry_counters(dbentry);

	dshash_release_lock(db_stats, dbentry);
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
	Assert(db_stats);

	/* Reset the archiver statistics for the cluster. */
	if (strcmp(target, "archiver") == 0)
	{
		LWLockAcquire(StatsLock, LW_EXCLUSIVE);

		memset(shared_archiverStats, 0, sizeof(*shared_archiverStats));
		shared_archiverStats->stat_reset_timestamp = GetCurrentTimestamp();
	}
	else if (strcmp(target, "bgwriter") == 0)
	{
		LWLockAcquire(StatsLock, LW_EXCLUSIVE);

		/* Reset the global background writer statistics for the cluster. */
		memset(shared_globalStats, 0, sizeof(*shared_globalStats));
		shared_globalStats->stat_reset_timestamp = GetCurrentTimestamp();
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unrecognized reset target: \"%s\"", target),
				 errhint("Target must be \"archiver\" or \"bgwriter\".")));
	
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
	PgStat_StatDBEntry *dbentry;
	

	Assert(db_stats);

	dbentry = pgstat_get_db_entry(MyDatabaseId, PGSTAT_FETCH_EXCLUSIVE, NULL);

	if (!dbentry)
		return;

	/* Set the reset timestamp for the whole database */
	dbentry->stat_reset_timestamp = GetCurrentTimestamp();

	/* Remove object if it exists, ignore it if not */
	if (type == RESET_TABLE)
	{
		dshash_table *t =
			dshash_attach(area, &dsh_tblparams, dbentry->tables, 0);
		dshash_delete_key(t, (void *) &objoid);
	}

	if (type == RESET_FUNCTION && dbentry->functions != DSM_HANDLE_INVALID)
	{
		dshash_table *t =
			dshash_attach(area, &dsh_funcparams, dbentry->functions, 0);
		dshash_delete_key(t, (void *) &objoid);
	}

	dshash_release_lock(db_stats, dbentry);
}

/*
 * pgstat_reset_all_counters: subroutine for pgstat_reset_all
 *
 * clear all counters on shared memory
 */
static void
pgstat_reset_all_counters(void)
{
	dshash_seq_status dshstat;
	PgStat_StatDBEntry		   *dbentry;

	Assert (db_stats);

	LWLockAcquire(StatsLock, LW_EXCLUSIVE);
	dshash_seq_init(&dshstat, db_stats, false, true);
	while ((dbentry = (PgStat_StatDBEntry *) dshash_seq_next(&dshstat)) != NULL)
	{
		/*
		 * We simply throw away all the database's table hashes
		 */
		if (dbentry->tables != DSM_HANDLE_INVALID)
		{
			dshash_table *t =
				dshash_attach(area, &dsh_tblparams, dbentry->tables, 0);
			dshash_destroy(t);
			dbentry->tables = DSM_HANDLE_INVALID;
		}
		if (dbentry->functions != DSM_HANDLE_INVALID)
		{
			dshash_table *t =
				dshash_attach(area, &dsh_funcparams, dbentry->functions, 0);
			dshash_destroy(t);
			dbentry->functions = DSM_HANDLE_INVALID;
		}

		/*
		 * Reset database-level stats, too.  This creates empty hash tables
		 * for tables and functions.
		 */
		reset_dbentry_counters(dbentry);
		dshash_release_lock(db_stats, dbentry);

	}

	/*
	 * Reset global counters
	 */
	memset(shared_globalStats, 0, sizeof(*shared_globalStats));
	memset(shared_archiverStats, 0, sizeof(*shared_archiverStats));
	shared_globalStats->stat_reset_timestamp =
		shared_archiverStats->stat_reset_timestamp = GetCurrentTimestamp();
	
	LWLockRelease(StatsLock);
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

	Assert(db_stats);

	if (!pgstat_track_counts || !IsUnderPostmaster)
		return;

	/*
	 * Store the last autovacuum time in the database's hashtable entry.
	 */
	dbentry = pgstat_get_db_entry(dboid, PGSTAT_FETCH_EXCLUSIVE, NULL);

	dbentry->last_autovac_time = GetCurrentTimestamp();

	dshash_release_lock(db_stats, dbentry);
}


/* ---------
 * pgstat_report_vacuum() -
 *
 *	Repot about the table we just vacuumed.
 * ---------
 */
void
pgstat_report_vacuum(Oid tableoid, bool shared,
					 PgStat_Counter livetuples, PgStat_Counter deadtuples)
{
	Oid					dboid;
	PgStat_StatDBEntry *dbentry;
	PgStat_StatTabEntry *tabentry;
	dshash_table *table;

	Assert(db_stats);

	if (!pgstat_track_counts || !IsUnderPostmaster)
		return;

	dboid = shared ? InvalidOid : MyDatabaseId;

	/*
	 * Store the data in the table's hashtable entry.
	 */
	dbentry = pgstat_get_db_entry(dboid, PGSTAT_FETCH_EXCLUSIVE, NULL);
	table = dshash_attach(area, &dsh_tblparams, dbentry->tables, 0);
	tabentry = pgstat_get_tab_entry(table, tableoid, true);

	tabentry->n_live_tuples = livetuples;
	tabentry->n_dead_tuples = deadtuples;

	if (IsAutoVacuumWorkerProcess())
	{
		tabentry->autovac_vacuum_timestamp = GetCurrentTimestamp();
		tabentry->autovac_vacuum_count++;
	}
	else
	{
		tabentry->vacuum_timestamp = GetCurrentTimestamp();
		tabentry->vacuum_count++;
	}
	dshash_release_lock(table, tabentry);
	dshash_detach(table);
	dshash_release_lock(db_stats, dbentry);
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
	Oid					dboid;
	PgStat_StatDBEntry *dbentry;
	PgStat_StatTabEntry *tabentry;
	dshash_table *table;

	Assert(db_stats);

	if (!pgstat_track_counts || !IsUnderPostmaster)
		return;

	/*
	 * Unlike VACUUM, ANALYZE might be running inside a transaction that has
	 * already inserted and/or deleted rows in the target table. ANALYZE will
	 * have counted such rows as live or dead respectively. Because we will
	 * report our counts of such rows at transaction end, we should subtract
	 * off these counts from what we send to the collector now, else they'll
	 * be double-counted after commit.  (This approach also ensures that the
	 * collector ends up with the right numbers if we abort instead of
	 * committing.)
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

	dboid = rel->rd_rel->relisshared ? InvalidOid : MyDatabaseId;

	/*
	 * Store the data in the table's hashtable entry.
	 */
	dbentry = pgstat_get_db_entry(dboid, PGSTAT_FETCH_EXCLUSIVE, NULL);

	table = dshash_attach(area, &dsh_tblparams, dbentry->tables, 0);
	tabentry = pgstat_get_tab_entry(table, RelationGetRelid(rel), true);

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
	dshash_release_lock(table, tabentry);
	dshash_detach(table);
	dshash_release_lock(db_stats, dbentry);
}

/* --------
 * pgstat_report_recovery_conflict() -
 *
 *	Report a Hot Standby recovery conflict.
 * --------
 */
static int pending_conflict_tablespace = 0;
static int pending_conflict_lock = 0;
static int pending_conflict_snapshot = 0;
static int pending_conflict_bufferpin = 0;
static int pending_conflict_startup_deadlock = 0;

void
pgstat_report_recovery_conflict(int reason)
{
	PgStat_StatDBEntry *dbentry;
	pg_stat_table_result_status status;

	Assert(db_stats);

	if (!pgstat_track_counts || !IsUnderPostmaster)
		return;

	pgstat_pending_recoveryconflict = true;

	switch (reason)
	{
		case PROCSIG_RECOVERY_CONFLICT_DATABASE:

			/*
			 * Since we drop the information about the database as soon as it
			 * replicates, there is no point in counting these conflicts.
			 */
			break;
		case PROCSIG_RECOVERY_CONFLICT_TABLESPACE:
			pending_conflict_tablespace++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_LOCK:
			pending_conflict_lock++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_SNAPSHOT:
			pending_conflict_snapshot++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_BUFFERPIN:
			pending_conflict_bufferpin++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK:
			pending_conflict_startup_deadlock++;
			break;
	}

	dbentry = pgstat_get_db_entry(MyDatabaseId,
								  PGSTAT_FETCH_EXCLUSIVE | PGSTAT_FETCH_NOWAIT,
								  &status);

	if (status == PGSTAT_ENTRY_LOCK_FAILED)
		return;

	pgstat_cleanup_recovery_conflict(dbentry);

	dshash_release_lock(db_stats, dbentry);
}

/*
 * clean up function for pending recovery conflicts
 */
static void
pgstat_cleanup_recovery_conflict(PgStat_StatDBEntry *dbentry)
{
	dbentry->n_conflict_tablespace	+= pending_conflict_tablespace;
	dbentry->n_conflict_lock 		+= pending_conflict_lock;
	dbentry->n_conflict_snapshot	+= pending_conflict_snapshot;
	dbentry->n_conflict_bufferpin	+= pending_conflict_bufferpin;
	dbentry->n_conflict_startup_deadlock += pending_conflict_startup_deadlock;

	pending_conflict_tablespace = 0;
	pending_conflict_lock = 0;
	pending_conflict_snapshot = 0;
	pending_conflict_bufferpin = 0;
	pending_conflict_startup_deadlock = 0;
	
	pgstat_pending_recoveryconflict = false;
}

/* --------
 * pgstat_report_deadlock() -
 *
 *	Report a deadlock detected.
 * --------
 */
static int pending_deadlocks = 0;

void
pgstat_report_deadlock(void)
{
	PgStat_StatDBEntry *dbentry;
	pg_stat_table_result_status status;

	Assert(db_stats);

	if (!pgstat_track_counts || !IsUnderPostmaster)
		return;

	pending_deadlocks++;
	pgstat_pending_deadlock = true;

	dbentry = pgstat_get_db_entry(MyDatabaseId,
								  PGSTAT_FETCH_EXCLUSIVE | PGSTAT_FETCH_NOWAIT,
								  &status);

	if (status == PGSTAT_ENTRY_LOCK_FAILED)
		return;

	pgstat_cleanup_deadlock(dbentry);

	dshash_release_lock(db_stats, dbentry);
}

/*
 * clean up function for pending dead locks
 */
static void
pgstat_cleanup_deadlock(PgStat_StatDBEntry *dbentry)
{
	dbentry->n_deadlocks += pending_deadlocks;
	pending_deadlocks = 0;
	pgstat_pending_deadlock = false;
}

/* --------
 * pgstat_report_tempfile() -
 *
 *	Report a temporary file.
 * --------
 */
static size_t pending_filesize = 0;
static size_t pending_files = 0;

void
pgstat_report_tempfile(size_t filesize)
{
	PgStat_StatDBEntry *dbentry;
	pg_stat_table_result_status status;

	Assert(db_stats);

	if (!pgstat_track_counts || !IsUnderPostmaster)
		return;

	if (filesize > 0) /* Is't there a case where filesize is really 0? */
	{
		pgstat_pending_tempfile = true;
		pending_filesize += filesize; /* needs check overflow */
		pending_files++;
	}

	if (!pgstat_pending_tempfile)
		return;

	dbentry = pgstat_get_db_entry(MyDatabaseId,
								  PGSTAT_FETCH_EXCLUSIVE | PGSTAT_FETCH_NOWAIT,
								  &status);

	if (status == PGSTAT_ENTRY_LOCK_FAILED)
		return;

	pgstat_cleanup_tempfile(dbentry);

	dshash_release_lock(db_stats, dbentry);
}

/*
 * clean up function for temporary files
 */
static void
pgstat_cleanup_tempfile(PgStat_StatDBEntry *dbentry)
{

	dbentry->n_temp_bytes += pending_filesize;
	dbentry->n_temp_files += pending_files;
	pending_filesize = 0;
	pending_files = 0;
	pgstat_pending_tempfile = false;

}

/*
 * Initialize function call usage data.
 * Called by the executor before invoking a function.
 */
void
pgstat_init_function_usage(FunctionCallInfoData *fcinfo,
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

	if (!pgStatFunctions)
	{
		/* First time through - initialize function stat table */
		HASHCTL		hash_ctl;

		memset(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(Oid);
		hash_ctl.entrysize = sizeof(PgStat_BackendFunctionEntry);
		pgStatFunctions = hash_create("Function stat entries",
									  PGSTAT_FUNCTION_HASH_SIZE,
									  &hash_ctl,
									  HASH_ELEM | HASH_BLOBS);
	}

	/* Get the stats entry for this function, create if necessary */
	htabent = hash_search(pgStatFunctions, &fcinfo->flinfo->fn_oid,
						  HASH_ENTER, &found);
	if (!found)
		MemSet(&htabent->f_counts, 0, sizeof(PgStat_FunctionCounts));

	fcu->fs = &htabent->f_counts;

	/* save stats for this function, later used to compensate for recursion */
	fcu->save_f_total_time = htabent->f_counts.f_total_time;

	/* save current backend-wide total time */
	fcu->save_total = total_func_time;

	/* get clock time as of function start */
	INSTR_TIME_SET_CURRENT(fcu->f_start);
}

/*
 * find_funcstat_entry - find any existing PgStat_BackendFunctionEntry entry
 *		for specified function
 *
 * If no entry, return NULL, don't create a new one
 */
PgStat_BackendFunctionEntry *
find_funcstat_entry(Oid func_id)
{
	if (pgStatFunctions == NULL)
		return NULL;

	return (PgStat_BackendFunctionEntry *) hash_search(pgStatFunctions,
													   (void *) &func_id,
													   HASH_FIND, NULL);
}

/*
 * Calculate function call usage and update stat counters.
 * Called by the executor after invoking a function.
 *
 * In the case of a set-returning function that runs in value-per-call mode,
 * we will see multiple pgstat_init_function_usage/pgstat_end_function_usage
 * calls for what the user considers a single call of the function.  The
 * finalize flag should be TRUE on the last call.
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
 *	data.  We can avoid repeated searches of the TabStatus arrays when the
 *	same relation is touched repeatedly within a transaction.
 * ----------
 */
void
pgstat_initstats(Relation rel)
{
	Oid			rel_id = rel->rd_id;
	char		relkind = rel->rd_rel->relkind;

	Assert(db_stats);

	if (!pgstat_track_counts || !IsUnderPostmaster)
	{
		/* We're not counting at all */
		rel->pgstat_info = NULL;
		return;
	}

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

	/*
	 * If we already set up this relation in the current transaction, nothing
	 * to do.
	 */
	if (rel->pgstat_info != NULL &&
		rel->pgstat_info->t_id == rel_id)
		return;

	/* Else find or make the PgStat_TableStatus entry, and update link */
	rel->pgstat_info = get_tabstat_entry(rel_id, rel->rd_rel->relisshared);
}

/*
 * get_tabstat_entry - find or create a PgStat_TableStatus entry for rel
 */
static PgStat_TableStatus *
get_tabstat_entry(Oid rel_id, bool isshared)
{
	TabStatHashEntry *hash_entry;
	PgStat_TableStatus *entry;
	TabStatusArray *tsa;
	bool		found;

	/*
	 * Create hash table if we don't have it already.
	 */
	if (pgStatTabHash == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(TabStatHashEntry);

		pgStatTabHash = hash_create("pgstat TabStatusArray lookup hash table",
									TABSTAT_QUANTUM,
									&ctl,
									HASH_ELEM | HASH_BLOBS);
	}

	/*
	 * Find an entry or create a new one.
	 */
	hash_entry = hash_search(pgStatTabHash, &rel_id, HASH_ENTER, &found);
	if (!found)
	{
		/* initialize new entry with null pointer */
		hash_entry->tsa_entry = NULL;
	}

	/*
	 * If entry is already valid, we're done.
	 */
	if (hash_entry->tsa_entry)
		return hash_entry->tsa_entry;

	/*
	 * Locate the first pgStatTabList entry with free space, making a new list
	 * entry if needed.  Note that we could get an OOM failure here, but if so
	 * we have left the hashtable and the list in a consistent state.
	 */
	if (pgStatTabList == NULL)
	{
		/* Set up first pgStatTabList entry */
		pgStatTabList = (TabStatusArray *)
			MemoryContextAllocZero(TopMemoryContext,
								   sizeof(TabStatusArray));
	}

	tsa = pgStatTabList;
	while (tsa->tsa_used >= TABSTAT_QUANTUM)
	{
		if (tsa->tsa_next == NULL)
			tsa->tsa_next = (TabStatusArray *)
				MemoryContextAllocZero(TopMemoryContext,
									   sizeof(TabStatusArray));
		tsa = tsa->tsa_next;
	}

	/*
	 * Allocate a PgStat_TableStatus entry within this list entry.  We assume
	 * the entry was already zeroed, either at creation or after last use.
	 */
	entry = &tsa->tsa_entries[tsa->tsa_used++];
	entry->t_id = rel_id;
	entry->t_shared = isshared;

	/*
	 * Now we can fill the entry in pgStatTabHash.
	 */
	hash_entry->tsa_entry = entry;

	return entry;
}

/*
 * find_tabstat_entry - find any existing PgStat_TableStatus entry for rel
 *
 * If no entry, return NULL, don't create a new one
 *
 * Note: if we got an error in the most recent execution of pgstat_report_stat,
 * it's possible that an entry exists but there's no hashtable entry for it.
 * That's okay, we'll treat this case as "doesn't exist".
 */
PgStat_TableStatus *
find_tabstat_entry(Oid rel_id)
{
	TabStatHashEntry *hash_entry;

	/* If hashtable doesn't exist, there are no entries at all */
	if (!pgStatTabHash)
		return NULL;

	hash_entry = hash_search(pgStatTabHash, &rel_id, HASH_FIND, NULL);
	if (!hash_entry)
		return NULL;

	/* Note that this step could also return NULL, but that's correct */
	return hash_entry->tsa_entry;
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
AtEOXact_PgStat(bool isCommit)
{
	PgStat_SubXactStatus *xact_state;

	/*
	 * Count transaction commit or abort.  (We use counters, not just bools,
	 * in case the reporting message isn't sent right away.)
	 */
	if (isCommit)
		pgStatXactCommit++;
	else
		pgStatXactRollback++;

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
 * reported to the stats collector immediately, while the effects on live
 * and dead tuple counts are preserved in the 2PC state file.
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
	pgstat_info = get_tabstat_entry(rec->t_id, rec->t_shared);

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
	pgstat_info = get_tabstat_entry(rec->t_id, rec->t_shared);

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
 * pgstat_fetch_stat_tabentry() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	the collected statistics for one table or NULL. NULL doesn't mean
 *	that the table doesn't exist, it is just not yet known by the
 *	collector, so the caller is better off to report ZERO instead.
 * ----------
 */
PgStat_StatTabEntry *
pgstat_fetch_stat_tabentry(Oid relid)
{
	PgStat_StatDBEntry *dbentry;
	PgStat_StatTabEntry *tabentry;

	/* Lookup our database, then look in its table hash table. */
	dbentry = pgstat_fetch_stat_dbentry(MyDatabaseId, false);
	if (dbentry == NULL)
		return NULL;

	tabentry = backend_get_tab_entry(dbentry, relid, false);
	if (tabentry != NULL)
		return tabentry;

	/*
	 * If we didn't find it, maybe it's a shared table.
	 */
	dbentry = pgstat_fetch_stat_dbentry(InvalidOid, false);
	if (dbentry == NULL)
		return NULL;

	tabentry = backend_get_tab_entry(dbentry, relid, false);
	if (tabentry != NULL)
		return tabentry;

	return NULL;
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
	PgStat_StatDBEntry *dbentry;
	PgStat_StatFuncEntry *funcentry = NULL;

	/* Lookup our database, then find the requested function */
	dbentry = pgstat_get_db_entry(MyDatabaseId, PGSTAT_FETCH_SHARED, NULL);
	if (dbentry == NULL)
		return NULL;

	funcentry = backend_get_func_etnry(dbentry, func_id, false);

	dshash_release_lock(db_stats, dbentry);
	return funcentry;
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
	if (!backend_snapshot_global_stats())
		return NULL;

	return snapshot_archiverStats;
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
	if (!backend_snapshot_global_stats())
		return NULL;

	return snapshot_globalStats;
}

/*
 * Shut down a single backend's statistics reporting at process exit.
 *
 * Flush any remaining statistics counts out to the collector.
 * Without this, operations triggered during backend exit (such as
 * temp table deletions) won't be counted.
 *
 * Lastly, clear out our entry in the PgBackendStatus array.
 */
static void
pgstat_beshutdown_hook(int code, Datum arg)
{
	/*
	 * If we got as far as discovering our own database ID, we can report what
	 * we did to the collector.  Otherwise, we'd be sending an invalid
	 * database ID, so forget it.  (This means that accesses to pg_database
	 * during failed backend starts might never get counted.)
	 */
	if (OidIsValid(MyDatabaseId))
		pgstat_update_stat(true);
}


/* ------------------------------------------------------------
 * Local support functions follow
 * ------------------------------------------------------------
 */

/* ----------
 * pgstat_update_archiver() -
 *
 *	Update the stats data about the WAL file that we successfully archived or
 *	failed to archive.
 * ----------
 */
void
pgstat_update_archiver(const char *xlog, bool failed)
{
	if (failed)
	{
		/* Failed archival attempt */
		++shared_archiverStats->failed_count;
		memcpy(shared_archiverStats->last_failed_wal, xlog,
			   sizeof(shared_archiverStats->last_failed_wal));
		shared_archiverStats->last_failed_timestamp = GetCurrentTimestamp();
	}
	else
	{
		/* Successful archival operation */
		++shared_archiverStats->archived_count;
		memcpy(shared_archiverStats->last_archived_wal, xlog,
			   sizeof(shared_archiverStats->last_archived_wal));
		shared_archiverStats->last_archived_timestamp = GetCurrentTimestamp();
	}
}

/* ----------
 * pgstat_update_bgwriter() -
 *
 *		Update bgwriter statistics
 * ----------
 */
void
pgstat_update_bgwriter(void)
{
	/* We assume this initializes to zeroes */
	static const PgStat_BgWriter all_zeroes;

	PgStat_BgWriter *s = &BgWriterStats;

	/*
	 * This function can be called even if nothing at all has happened. In
	 * this case, avoid sending a completely empty message to the stats
	 * collector.
	 */
	if (memcmp(&BgWriterStats, &all_zeroes, sizeof(PgStat_BgWriter)) == 0)
		return;

	LWLockAcquire(StatsLock, LW_EXCLUSIVE);
	shared_globalStats->timed_checkpoints += s->timed_checkpoints;
	shared_globalStats->requested_checkpoints += s->requested_checkpoints;
	shared_globalStats->checkpoint_write_time += s->checkpoint_write_time;
	shared_globalStats->checkpoint_sync_time += s->checkpoint_sync_time;
	shared_globalStats->buf_written_checkpoints += s->buf_written_checkpoints;
	shared_globalStats->buf_written_clean += s->buf_written_clean;
	shared_globalStats->maxwritten_clean += s->maxwritten_clean;
	shared_globalStats->buf_written_backend += s->buf_written_backend;
	shared_globalStats->buf_fsync_backend += s->buf_fsync_backend;
	shared_globalStats->buf_alloc += s->buf_alloc;
	LWLockRelease(StatsLock);

	/*
	 * Clear out the statistics buffer, so it can be re-used.
	 */
	MemSet(&BgWriterStats, 0, sizeof(BgWriterStats));
}

/*
 * Subroutine to reset stats in a shared database entry
 *
 * Tables and functions hashes are initialized to empty.
 */
static void
reset_dbentry_counters(PgStat_StatDBEntry *dbentry)
{
	dshash_table *tbl;

	dbentry->n_xact_commit = 0;
	dbentry->n_xact_rollback = 0;
	dbentry->n_blocks_fetched = 0;
	dbentry->n_blocks_hit = 0;
	dbentry->n_tuples_returned = 0;
	dbentry->n_tuples_fetched = 0;
	dbentry->n_tuples_inserted = 0;
	dbentry->n_tuples_updated = 0;
	dbentry->n_tuples_deleted = 0;
	dbentry->last_autovac_time = 0;
	dbentry->n_conflict_tablespace = 0;
	dbentry->n_conflict_lock = 0;
	dbentry->n_conflict_snapshot = 0;
	dbentry->n_conflict_bufferpin = 0;
	dbentry->n_conflict_startup_deadlock = 0;
	dbentry->n_temp_files = 0;
	dbentry->n_temp_bytes = 0;
	dbentry->n_deadlocks = 0;
	dbentry->n_block_read_time = 0;
	dbentry->n_block_write_time = 0;

	dbentry->stat_reset_timestamp = GetCurrentTimestamp();
	dbentry->stats_timestamp = 0;


	Assert(dbentry->tables == DSM_HANDLE_INVALID);
	tbl = dshash_create(area, &dsh_tblparams, 0);
	dbentry->tables = dshash_get_hash_table_handle(tbl);
	dshash_detach(tbl);

	Assert(dbentry->functions == DSM_HANDLE_INVALID);
	/* we create function hash as needed */

	dbentry->snapshot_tables = NULL;
	dbentry->snapshot_functions = NULL;
}

/*
 * Lookup the hash table entry for the specified database. If no hash
 * table entry exists, initialize it, if the create parameter is true.
 * Else, return NULL.
 */
static PgStat_StatDBEntry *
pgstat_get_db_entry(Oid databaseid, int op,	pg_stat_table_result_status *status)
{
	PgStat_StatDBEntry *result;
	bool		nowait = ((op & PGSTAT_FETCH_NOWAIT) != 0);
	bool		lock_acquired = true;
	bool		found = true;

	if (!IsUnderPostmaster)
		return NULL;

	/* Lookup or create the hash table entry for this database */
	if (op & PGSTAT_FETCH_EXCLUSIVE)
	{
		result = (PgStat_StatDBEntry *)
			dshash_find_or_insert_extended(db_stats, &databaseid,
										   &found, nowait);
		if (result == NULL)
			lock_acquired = false;
		else if (!found)
		{
			/*
			 * If not found, initialize the new one.  This creates empty hash
			 * tables for tables and functions, too.
			 */
			reset_dbentry_counters(result);
		}
	}
	else
	{
		result = (PgStat_StatDBEntry *)
			dshash_find_extended(db_stats, &databaseid, true, nowait,
								 &lock_acquired);
		if (result == NULL)
			found = false;
	}

	/* Set return status if requested */
	if (status)
	{
		if (!lock_acquired)
		{
			Assert(nowait);
			*status = PGSTAT_ENTRY_LOCK_FAILED;
		}
		else if (!found)
			*status = PGSTAT_ENTRY_NOT_FOUND;
		else
			*status = PGSTAT_ENTRY_FOUND;
	}

	return result;
}

/*
 * Lookup the hash table entry for the specified table. If no hash
 * table entry exists, initialize it, if the create parameter is true.
 * Else, return NULL.
 */
static PgStat_StatTabEntry *
pgstat_get_tab_entry(dshash_table *table, Oid tableoid, bool create)
{
	PgStat_StatTabEntry *result;
	bool		found;

	/* Lookup or create the hash table entry for this table */
	if (create)
		result = (PgStat_StatTabEntry *)
			dshash_find_or_insert(table, &tableoid, &found);
	else
		result = (PgStat_StatTabEntry *) dshash_find(table, &tableoid, false);

	if (!create && !found)
		return NULL;

	/* If not found, initialize the new one. */
	if (!found)
	{
		result->numscans = 0;
		result->tuples_returned = 0;
		result->tuples_fetched = 0;
		result->tuples_inserted = 0;
		result->tuples_updated = 0;
		result->tuples_deleted = 0;
		result->tuples_hot_updated = 0;
		result->n_live_tuples = 0;
		result->n_dead_tuples = 0;
		result->changes_since_analyze = 0;
		result->blocks_fetched = 0;
		result->blocks_hit = 0;
		result->vacuum_timestamp = 0;
		result->vacuum_count = 0;
		result->autovac_vacuum_timestamp = 0;
		result->autovac_vacuum_count = 0;
		result->analyze_timestamp = 0;
		result->analyze_count = 0;
		result->autovac_analyze_timestamp = 0;
		result->autovac_analyze_count = 0;
	}

	return result;
}


/* ----------
 * pgstat_write_statsfiles() -
 *		Write the global statistics file, as well as DB files.
 * ----------
 */
void
pgstat_write_statsfiles(void)
{
	dshash_seq_status hstat;
	PgStat_StatDBEntry *dbentry;
	FILE	   *fpout;
	int32		format_id;
	const char *tmpfile = PGSTAT_STAT_PERMANENT_TMPFILE;
	const char *statfile = PGSTAT_STAT_PERMANENT_FILENAME;
	int			rc;

	/* should be called from postmaster  */
	Assert(!IsUnderPostmaster);

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
	dshash_seq_init(&hstat, db_stats, false, false);
	while ((dbentry = (PgStat_StatDBEntry *) dshash_seq_next(&hstat)) != NULL)
	{
		/*
		 * Write out the table and function stats for this DB into the
		 * appropriate per-DB stat file, if required.
		 */
		/* Make DB's timestamp consistent with the global stats */
		dbentry->stats_timestamp = shared_globalStats->stats_timestamp;

		pgstat_write_db_statsfile(dbentry);

		/*
		 * Write out the DB entry. We don't write the tables or functions
		 * pointers, since they're of no use to any other process.
		 */
		fputc('D', fpout);
		rc = fwrite(dbentry, offsetof(PgStat_StatDBEntry, tables), 1, fpout);
		(void) rc;				/* we'll check for error with ferror */
	}

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

/*
 * return the filename for a DB stat file; filename is the output buffer,
 * of length len.
 */
static void
get_dbstat_filename(bool tempname, Oid databaseid,
					char *filename, int len)
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
 * pgstat_write_db_statsfile() -
 *		Write the stat file for a single database.
 *
 *	If writing to the permanent file (happens when the collector is
 *	shutting down only), remove the temporary file so that backends
 *	starting up under a new postmaster can't read the old data before
 *	the new collector is ready.
 * ----------
 */
static void
pgstat_write_db_statsfile(PgStat_StatDBEntry *dbentry)
{
	dshash_seq_status tstat;
	dshash_seq_status fstat;
	PgStat_StatTabEntry *tabentry;
	PgStat_StatFuncEntry *funcentry;
	FILE	   *fpout;
	int32		format_id;
	Oid			dbid = dbentry->databaseid;
	int			rc;
	char		tmpfile[MAXPGPATH];
	char		statfile[MAXPGPATH];
	dshash_table *tbl;

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
	tbl = dshash_attach(area, &dsh_tblparams, dbentry->tables, 0);
	dshash_seq_init(&tstat, tbl, false, false);
	while ((tabentry = (PgStat_StatTabEntry *) dshash_seq_next(&tstat)) != NULL)
	{
		fputc('T', fpout);
		rc = fwrite(tabentry, sizeof(PgStat_StatTabEntry), 1, fpout);
		(void) rc;				/* we'll check for error with ferror */
	}
	dshash_detach(tbl);

	/*
	 * Walk through the database's function stats table.
	 */
	if (dbentry->functions != DSM_HANDLE_INVALID)
	{
		tbl = dshash_attach(area, &dsh_funcparams, dbentry->functions, 0);
		dshash_seq_init(&fstat, tbl, false, false);
		while ((funcentry = (PgStat_StatFuncEntry *) dshash_seq_next(&fstat)) != NULL)
		{
			fputc('F', fpout);
			rc = fwrite(funcentry, sizeof(PgStat_StatFuncEntry), 1, fpout);
			(void) rc;				/* we'll check for error with ferror */
		}
		dshash_detach(tbl);
	}

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
 * pgstat_read_statsfiles() -
 *
 *	Reads in some existing statistics collector files into the shared stats
 *	hash.
 *
 * ----------
 */
void
pgstat_read_statsfiles(void)
{
	PgStat_StatDBEntry *dbentry;
	PgStat_StatDBEntry dbbuf;
	FILE	   *fpin;
	int32		format_id;
	bool		found;
	const char *statfile = PGSTAT_STAT_PERMANENT_FILENAME;
	dshash_table *tblstats = NULL;
	dshash_table *funcstats = NULL;

	/* should be called from postmaster  */
	Assert(!IsUnderPostmaster);

	/*
	 * The tables will live in pgStatLocalContext.
	 */
	pgstat_setup_memcxt();

	/*
	 * Create the DB hashtable and global stas area
	 */
	/* Hold lock so that no other process looks empty stats */
	LWLockAcquire(StatsLock, LW_EXCLUSIVE);
	pgstat_create_shared_stats();

	/*
	 * Set the current timestamp (will be kept only in case we can't load an
	 * existing statsfile).
	 */
	shared_globalStats->stat_reset_timestamp = GetCurrentTimestamp();
	shared_archiverStats->stat_reset_timestamp = shared_globalStats->stat_reset_timestamp;

	/*
	 * Try to open the stats file. If it doesn't exist, the backends simply
	 * return zero for anything and the collector simply starts from scratch
	 * with empty counters.
	 *
	 * ENOENT is a possibility if the stats collector is not running or has
	 * not yet written the stats file the first time.  Any other failure
	 * condition is suspicious.
	 */
	if ((fpin = AllocateFile(statfile, PG_BINARY_R)) == NULL)
	{
		if (errno != ENOENT)
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not open statistics file \"%s\": %m",
							statfile)));
		LWLockRelease(StatsLock);
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
		memset(shared_globalStats, 0, sizeof(*shared_globalStats));
		goto done;
	}

	/*
	 * In the collector, disregard the timestamp we read from the permanent
	 * stats file; we should be willing to write a temp stats file immediately
	 * upon the first request from any backend.  This only matters if the old
	 * file's timestamp is less than PGSTAT_STAT_INTERVAL ago, but that's not
	 * an unusual scenario.
	 */
	shared_globalStats->stats_timestamp = 0;

	/*
	 * Read archiver stats struct
	 */
	if (fread(shared_archiverStats, 1, sizeof(*shared_archiverStats), fpin) !=
		sizeof(*shared_archiverStats))
	{
		ereport(LOG,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		memset(shared_archiverStats, 0, sizeof(*shared_archiverStats));
		goto done;
	}

	/*
	 * We found an existing collector stats file. Read it and put all the
	 * hashtable entries into place.
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
				dbentry = (PgStat_StatDBEntry *)
					dshash_find_or_insert(db_stats, (void *) &dbbuf.databaseid,
										  &found);
				if (found)
				{
					dshash_release_lock(db_stats, dbentry);
					ereport(LOG,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				memcpy(dbentry, &dbbuf, sizeof(PgStat_StatDBEntry));
				dbentry->tables = DSM_HANDLE_INVALID;
				dbentry->functions = DSM_HANDLE_INVALID;
				dbentry->snapshot_tables = NULL;
				dbentry->snapshot_functions = NULL;

				/*
				 * In the collector, disregard the timestamp we read from the
				 * permanent stats file; we should be willing to write a temp
				 * stats file immediately upon the first request from any
				 * backend.
				 */
				dbentry->stats_timestamp = 0;

				/*
				 * If requested, read the data from the database-specific
				 * file.  Otherwise we just leave the hashtables empty.
				 */
				tblstats = dshash_create(area, &dsh_tblparams, 0);
				dbentry->tables = dshash_get_hash_table_handle(tblstats);
				/* we don't create function hash at the present */
				dshash_release_lock(db_stats, dbentry);
				pgstat_read_db_statsfile(dbentry->databaseid,
										 tblstats, funcstats);
				dshash_detach(tblstats);
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
	LWLockRelease(StatsLock);
	FreeFile(fpin);

	elog(DEBUG2, "removing permanent stats file \"%s\"", statfile);
	unlink(statfile);

	return;
}


Size
StatsShmemSize(void)
{
	return sizeof(StatsShmemStruct);
}

void
StatsShmemInit(void)
{
	bool	found;

	StatsShmem = (StatsShmemStruct *)
		ShmemInitStruct("Stats area", StatsShmemSize(),
						&found);
	if (!IsUnderPostmaster)
	{
		Assert(!found);

		StatsShmem->stats_dsa_handle = DSM_HANDLE_INVALID;

		/* Load saved data if any */
		pgstat_read_statsfiles();

		/* need to be called before dsm shutodwn */
		before_shmem_exit(pgstat_postmaster_shutdown, (Datum) 0);
	}
}

static void
pgstat_postmaster_shutdown(int code, Datum arg)
{
	/* we trash the stats on crash */
	if (code == 0)
		pgstat_write_statsfiles();
}

/* ----------
 * pgstat_read_db_statsfile() -
 *
 *	Reads in the permanent statistics collector file and create shared
 *	statistics tables. The file is removed afer reading.
 * ----------
 */
static void
pgstat_read_db_statsfile(Oid databaseid,
						 dshash_table *tabhash, dshash_table *funchash)
{
	PgStat_StatTabEntry *tabentry;
	PgStat_StatTabEntry tabbuf;
	PgStat_StatFuncEntry funcbuf;
	PgStat_StatFuncEntry *funcentry;
	FILE	   *fpin;
	int32		format_id;
	bool		found;
	char		statfile[MAXPGPATH];

	/* should be called from postmaster  */
	Assert(!IsUnderPostmaster);

	get_dbstat_filename(false, databaseid, statfile, MAXPGPATH);

	/*
	 * Try to open the stats file. If it doesn't exist, the backends simply
	 * return zero for anything and the collector simply starts from scratch
	 * with empty counters.
	 *
	 * ENOENT is a possibility if the stats collector is not running or has
	 * not yet written the stats file the first time.  Any other failure
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
	 * We found an existing collector stats file. Read it and put all the
	 * hashtable entries into place.
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

				/*
				 * Skip if table data not wanted.
				 */
				if (tabhash == NULL)
					break;

				tabentry = (PgStat_StatTabEntry *)
					dshash_find_or_insert(tabhash,
										  (void *) &tabbuf.tableid, &found);

				if (found)
				{
					dshash_release_lock(tabhash, tabentry);
					ereport(LOG,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				memcpy(tabentry, &tabbuf, sizeof(tabbuf));
				dshash_release_lock(tabhash, tabentry);
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

				/*
				 * Skip if function data not wanted.
				 */
				if (funchash == NULL)
					break;

				funcentry = (PgStat_StatFuncEntry *)
					dshash_find_or_insert(funchash,
										  (void *) &funcbuf.functionid, &found);

				if (found)
				{
					ereport(LOG,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				memcpy(funcentry, &funcbuf, sizeof(funcbuf));
				dshash_release_lock(funchash, funcentry);
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
	FreeFile(fpin);

	elog(DEBUG2, "removing permanent stats file \"%s\"", statfile);
	unlink(statfile);
}

/* ----------
 * backend_clean_snapshot_callback() -
 *
 *	This is usually called with arg = NULL when the memory context where the
 *  current snapshot has been taken. Don't bother releasing memory in the
 *  case.
 * ----------
 */
static void
backend_clean_snapshot_callback(void *arg)
{
	if (arg != NULL)
	{
		/* explicitly called, so explicitly free resources */
		if (snapshot_globalStats)
			pfree(snapshot_globalStats);

		if (snapshot_archiverStats)
			pfree(snapshot_archiverStats);

		if (snapshot_db_stats)
		{
			HASH_SEQ_STATUS seq;
			PgStat_StatDBEntry *dbent;

			hash_seq_init(&seq, snapshot_db_stats);
			while ((dbent = hash_seq_search(&seq)) != NULL)
			{
				if (dbent->snapshot_tables)
					hash_destroy(dbent->snapshot_tables);
				if (dbent->snapshot_functions)
					hash_destroy(dbent->snapshot_functions);
			}
			hash_destroy(snapshot_db_stats);
		}
	}

	/* mark as the resource are not allocated */
	snapshot_globalStats = NULL;
	snapshot_archiverStats = NULL;
	snapshot_db_stats = NULL;
}

/*
 * create_local_stats_hash() -
 *
 * Creates a dynahash used for table/function stats cache.
 */
static HTAB *
create_local_stats_hash(const char *name, size_t keysize, size_t entrysize,
						int nentries)
{
	HTAB *result;
	HASHCTL ctl;

	/* Create the hash in the stats context */
	ctl.keysize		= keysize;
	ctl.entrysize	= entrysize;
	ctl.hcxt		= stats_cxt;
	result = hash_create(name, nentries, &ctl,
						 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	return result;
}

/*
 * snapshot_statentry() - Find an entriy from source dshash.
 *
 * Returns the entry for key or NULL if not found. If dest is not null, uses
 * *dest as local cache, which is created in the same shape with the given
 * dshash when *dest is NULL. In both cases the result is cached in the hash
 * and the same entry is returned to subsequent calls for the same key.
 * 
 * Otherwise returned entry is a copy that is palloc'ed in the current memory
 * context. Its content may differ for every request.
 *
 * If dshash is NULL, temporaralily attaches dsh_handle instead.
 */
static void *
snapshot_statentry(HTAB **dest, const char *hashname,
				   dshash_table *dshash, dshash_table_handle dsh_handle,
				   const dshash_parameters *dsh_params, Oid key)
{
	void *lentry = NULL;
	size_t keysize = dsh_params->key_size;
	size_t entrysize = dsh_params->entry_size;

	if (dest)
	{
		/* caches the result entry */
		bool found;

		/*
		 * Create new hash with arbitrary initial entries since we don't know
		 * how this hash will grow.
		 */
		if (!*dest)
		{
			Assert(hashname);
			*dest = create_local_stats_hash(hashname, keysize, entrysize, 32);
		}

		lentry = hash_search(*dest, &key, HASH_ENTER, &found);
		if (!found)
		{
			dshash_table *t = dshash;
			void *sentry;

			if (!t)
				t = dshash_attach(area, dsh_params, dsh_handle, NULL);

			sentry = dshash_find(t, &key, false);

			/*
			 * We expect that the stats for specified database exists in most
			 * cases.
			 */
			if (!sentry)
			{
				hash_search(*dest, &key, HASH_REMOVE, NULL);
				if (!dshash)
					dshash_detach(t);
				return NULL;
			}
			memcpy(lentry, sentry, entrysize);
			dshash_release_lock(t, sentry);

			if (!dshash)
				dshash_detach(t);
		}
	}
	else
	{
		/*
		 * The caller don't want caching. Just make a copy of the entry then
		 * return.
		 */
		dshash_table *t = dshash;
		void *sentry;

		if (!t)
			t = dshash_attach(area, dsh_params, dsh_handle, NULL);

		sentry = dshash_find(t, &key, false);
		if (sentry)
		{
			lentry = palloc(entrysize);
			memcpy(lentry, sentry, entrysize);
			dshash_release_lock(t, sentry);
		}

		if (!dshash)
			dshash_detach(t);
	}

	return lentry;
}

/*
 * backend_snapshot_global_stats() -
 *
 * Makes a local copy of global stats if not already done.  They will be kept
 * until pgstat_clear_snapshot() is called or the end of the current memory
 * context (typically TopTransactionContext).  Returns false if the shared
 * stats is not created yet.
 */
static bool
backend_snapshot_global_stats(void)
{
	MemoryContext oldcontext = CurrentMemoryContext;
	MemoryContextCallback *mcxt_cb;

	/* Nothing to do if already done */
	if (snapshot_globalStats)
		return true;

	Assert(snapshot_archiverStats == NULL);

	/*
	 * The snapshot lives within the current top transaction if any, or the
	 * current memory context liftime otherwise.
	 */
	if (IsTransactionState())
		oldcontext = MemoryContextSwitchTo(TopTransactionContext);

	/* Remember for stats memory allocation later */
	stats_cxt = CurrentMemoryContext;

	/* global stats can be just copied  */
	snapshot_globalStats = palloc(sizeof(PgStat_GlobalStats));
	memcpy(snapshot_globalStats, shared_globalStats,
		   sizeof(PgStat_GlobalStats));

	snapshot_archiverStats = palloc(sizeof(PgStat_ArchiverStats));
	memcpy(snapshot_archiverStats, shared_archiverStats,
		   sizeof(PgStat_ArchiverStats));

	/* set the timestamp of this snapshot */
	snapshot_globalStats->stats_timestamp = GetCurrentTimestamp();

	/* register callback to clear snapshot */
	mcxt_cb = (MemoryContextCallback *)palloc(sizeof(MemoryContextCallback));
	mcxt_cb->func = backend_clean_snapshot_callback;
	mcxt_cb->arg = NULL;
	MemoryContextRegisterResetCallback(CurrentMemoryContext, mcxt_cb);
	MemoryContextSwitchTo(oldcontext);

	return true;
}

/* ----------
 * pgstat_fetch_stat_dbentry() -
 *
 *	Find database stats entry on backends. The returned entries are cached
 *	until transaction end. If onshot is true, they are not cached and returned
 *	in a palloc'ed memory.
 */
PgStat_StatDBEntry *
pgstat_fetch_stat_dbentry(Oid dbid, bool oneshot)
{
	/* take a local snapshot if we don't have one */
	char *hashname = "local database stats hash";
	PgStat_StatDBEntry *dbentry;

	/* should be called from backends  */
	Assert(IsUnderPostmaster);

	/* If not done for this transaction, take a snapshot of global stats */
	if (!backend_snapshot_global_stats())
		return NULL;

	dbentry = snapshot_statentry(oneshot ? NULL : &snapshot_db_stats,
								 hashname, db_stats, 0, &dsh_dbparams,
								 dbid);
	
	return dbentry;
}

/* ----------
 * backend_get_tab_entry() -
 *
 *	Find table stats entry on backends. The returned entries are cached until
 *	transaction end. If onshot is true, they are not cached and returned in a
 *	palloc'ed memory.
 */
PgStat_StatTabEntry *
backend_get_tab_entry(PgStat_StatDBEntry *dbent, Oid reloid, bool oneshot)
{
	/* take a local snapshot if we don't have one */
	char *hashname = "local table stats hash";

	/* should be called from backends  */
	Assert(IsUnderPostmaster);

	return snapshot_statentry(oneshot ? NULL : &dbent->snapshot_tables,
							  hashname, NULL, dbent->tables, &dsh_tblparams,
							  reloid);
}

/* ----------
 * backend_get_func_entry() -
 *
 *	Find function stats entry on backends. The returned entries are cached
 *	until transaction end. If onshot is true, they are not cached and returned
 *	in a palloc'ed memory.
 */
static PgStat_StatFuncEntry *
backend_get_func_etnry(PgStat_StatDBEntry *dbent, Oid funcid, bool oneshot)
{
	char *hashname = "local table stats hash";

	/* should be called from backends  */
	Assert(IsUnderPostmaster);

	if (dbent->functions == DSM_HANDLE_INVALID)
		return NULL;

	return snapshot_statentry(oneshot ? NULL : &dbent->snapshot_tables,
							  hashname, NULL, dbent->functions, &dsh_funcparams,
							  funcid);
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
	int param = 0;	/* only the address is significant */

	pgstat_bestatus_clear_snapshot();

	/* Release memory, if any was allocated */
	if (pgStatLocalContext)
		MemoryContextDelete(pgStatLocalContext);

	/* Reset variables */
	pgStatLocalContext = NULL;
	pgStatDBHash = NULL;

	/*
	 * the parameter inform the function that it is not called from
	 * MemoryContextCallback
	 */
	backend_clean_snapshot_callback(&param);
}


static bool
pgstat_update_tabentry(dshash_table *tabhash, PgStat_TableStatus *stat,
					   bool nowait)
{
	PgStat_StatTabEntry *tabentry;
	bool	found;

	if (tabhash == NULL)
		return false;

	tabentry = (PgStat_StatTabEntry *)
		dshash_find_or_insert_extended(tabhash, (void *) &(stat->t_id),
									   &found, nowait);

	/* failed to acquire lock */
	if (tabentry == NULL)
		return false;

	if (!found)
	{
		/*
		 * If it's a new table entry, initialize counters to the values we
		 * just got.
		 */
		tabentry->numscans = stat->t_counts.t_numscans;
		tabentry->tuples_returned = stat->t_counts.t_tuples_returned;
		tabentry->tuples_fetched = stat->t_counts.t_tuples_fetched;
		tabentry->tuples_inserted = stat->t_counts.t_tuples_inserted;
		tabentry->tuples_updated = stat->t_counts.t_tuples_updated;
		tabentry->tuples_deleted = stat->t_counts.t_tuples_deleted;
		tabentry->tuples_hot_updated = stat->t_counts.t_tuples_hot_updated;
		tabentry->n_live_tuples = stat->t_counts.t_delta_live_tuples;
		tabentry->n_dead_tuples = stat->t_counts.t_delta_dead_tuples;
		tabentry->changes_since_analyze = stat->t_counts.t_changed_tuples;
		tabentry->blocks_fetched = stat->t_counts.t_blocks_fetched;
		tabentry->blocks_hit = stat->t_counts.t_blocks_hit;

		tabentry->vacuum_timestamp = 0;
		tabentry->vacuum_count = 0;
		tabentry->autovac_vacuum_timestamp = 0;
		tabentry->autovac_vacuum_count = 0;
		tabentry->analyze_timestamp = 0;
		tabentry->analyze_count = 0;
		tabentry->autovac_analyze_timestamp = 0;
		tabentry->autovac_analyze_count = 0;
	}
	else
	{
		/*
		 * Otherwise add the values to the existing entry.
		 */
		tabentry->numscans += stat->t_counts.t_numscans;
		tabentry->tuples_returned += stat->t_counts.t_tuples_returned;
		tabentry->tuples_fetched += stat->t_counts.t_tuples_fetched;
		tabentry->tuples_inserted += stat->t_counts.t_tuples_inserted;
		tabentry->tuples_updated += stat->t_counts.t_tuples_updated;
		tabentry->tuples_deleted += stat->t_counts.t_tuples_deleted;
		tabentry->tuples_hot_updated += stat->t_counts.t_tuples_hot_updated;
		/* If table was truncated, first reset the live/dead counters */
		if (stat->t_counts.t_truncated)
		{
			tabentry->n_live_tuples = 0;
			tabentry->n_dead_tuples = 0;
		}
		tabentry->n_live_tuples += stat->t_counts.t_delta_live_tuples;
		tabentry->n_dead_tuples += stat->t_counts.t_delta_dead_tuples;
		tabentry->changes_since_analyze += stat->t_counts.t_changed_tuples;
		tabentry->blocks_fetched += stat->t_counts.t_blocks_fetched;
		tabentry->blocks_hit += stat->t_counts.t_blocks_hit;
	}

	/* Clamp n_live_tuples in case of negative delta_live_tuples */
	tabentry->n_live_tuples = Max(tabentry->n_live_tuples, 0);
	/* Likewise for n_dead_tuples */
	tabentry->n_dead_tuples = Max(tabentry->n_dead_tuples, 0);
	
	dshash_release_lock(tabhash, tabentry);

	return true;
}

static void
pgstat_update_dbentry(PgStat_StatDBEntry *dbentry, PgStat_TableStatus *stat)
{
	/*
	 * Add per-table stats to the per-database entry, too.
	 */
	dbentry->n_tuples_returned += stat->t_counts.t_tuples_returned;
	dbentry->n_tuples_fetched += stat->t_counts.t_tuples_fetched;
	dbentry->n_tuples_inserted += stat->t_counts.t_tuples_inserted;
	dbentry->n_tuples_updated += stat->t_counts.t_tuples_updated;
	dbentry->n_tuples_deleted += stat->t_counts.t_tuples_deleted;
	dbentry->n_blocks_fetched += stat->t_counts.t_blocks_fetched;
	dbentry->n_blocks_hit += stat->t_counts.t_blocks_hit;
}

/* ----------
 * pgstat_setup_memcxt() -
 *
 *	Create pgStatLocalContext, if not already done.
 * ----------
 */
static void
pgstat_setup_memcxt(void)
{
	if (!pgStatLocalContext)
		pgStatLocalContext = AllocSetContextCreate(TopMemoryContext,
												   "Statistics snapshot",
												   ALLOCSET_SMALL_SIZES);
}

