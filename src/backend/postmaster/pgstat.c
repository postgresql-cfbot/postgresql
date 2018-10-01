/* ----------
 * pgstat.c
 *
 *	All the statistics collector stuff hacked up in one big, ugly file.
 *
 *	TODO:	- Separate collector, postmaster and backend stuff
 *			  into different files.
 *
 *			- Add some automatic call for pgstat vacuuming.
 *
 *			- Add a pgstat config column to pg_database, so this
 *			  entire thing can be enabled/disabled on a per db basis.
 *
 *	Copyright (c) 2001-2018, PostgreSQL Global Development Group
 *
 *	src/backend/postmaster/pgstat.c
 * ----------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/param.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <time.h>
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#include "pgstat.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "catalog/pg_proc.h"
#include "common/ip.h"
#include "libpq/libpq.h"
#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "postmaster/autovacuum.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "replication/walsender.h"
#include "storage/backendid.h"
#include "storage/dsm.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/pg_shmem.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "utils/ascii.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"


/* ----------
 * Timer definitions.
 * ----------
 */
#define PGSTAT_STAT_INTERVAL	500 /* Minimum time between stats file
									 * updates; in milliseconds. */

#define PGSTAT_RESTART_INTERVAL 60	/* How often to attempt to restart a
									 * failed statistics collector; in
									 * seconds. */

/* Minimum receive buffer size for the collector's socket. */
#define PGSTAT_MIN_RCVBUF		(100 * 1024)


/* ----------
 * The initial size hints for the hash tables used in the collector.
 * ----------
 */
#define PGSTAT_TAB_HASH_SIZE	512
#define PGSTAT_FUNCTION_HASH_SIZE	512


#define PGSTAT_MAX_QUEUE_LEN	100

/*
 * Operation mode of pgstat_get_db_entry.
 */
#define PGSTAT_TABLE_READ	0
#define PGSTAT_TABLE_WRITE	1
#define PGSTAT_TABLE_CREATE 2
#define	PGSTAT_TABLE_NOWAIT 4

typedef enum
{
	PGSTAT_TABLE_NOT_FOUND,
	PGSTAT_TABLE_FOUND,
	PGSTAT_TABLE_LOCK_FAILED
} pg_stat_table_result_status;

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
 * BgWriter global statistics counters (unused in other processes).
 * Stored directly in a stats message structure so it can be sent
 * without needing to copy things around.  We assume this inits to zeroes.
 */
PgStat_BgWriter BgWriterStats;

/* ----------
 * Local data
 * ----------
 */
static time_t last_pgstat_start_time;

static bool pgStatRunningInCollector = false;

/* Shared stats bootstrap infomation */
typedef struct StatsShmemStruct {
	dsa_handle stats_dsa_handle;
	dshash_table_handle db_stats_handle;
	dsa_pointer	global_stats;
	dsa_pointer	archiver_stats;
} StatsShmemStruct;

static StatsShmemStruct * StatsShmem = NULL;
static dsa_area *area = NULL;
static dshash_table *db_stats;
static HTAB *snapshot_db_stats;
static MemoryContext stats_cxt;

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

/*
 * Backends store per-function info that's waiting to be sent to the collector
 * in this hash table (indexed by function OID).
 */
static HTAB *pgStatFunctions = NULL;

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

/*
 * Info about current "snapshot" of stats file
 */
static MemoryContext pgStatLocalContext = NULL;
static HTAB *pgStatDBHash = NULL;

/* Status for backends including auxiliary */
static LocalPgBackendStatus *localBackendStatusTable = NULL;

/* Total number of backends including auxiliary */
static int	localNumBackends = 0;

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
 * List of OIDs of databases we need to write out.  If an entry is InvalidOid,
 * it means to write only the shared-catalog stats ("DB 0"); otherwise, we
 * will write both that DB's data and the shared stats.
 */
static List *pending_write_requests = NIL;

/* Signal handler flags */
static volatile bool need_exit = false;
static volatile bool got_SIGHUP = false;
static volatile bool got_SIGTERM = false;

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
#ifdef EXEC_BACKEND
static pid_t pgstat_forkexec(void);
#endif

/* functions used in stats collector */
static void pgstat_shutdown_handler(SIGNAL_ARGS);
static void pgstat_quickdie_handler(SIGNAL_ARGS);
static void pgstat_beshutdown_hook(int code, Datum arg);
static void pgstat_sighup_handler(SIGNAL_ARGS);

static PgStat_StatDBEntry *pgstat_get_db_entry(Oid databaseid, int op,
									pg_stat_table_result_status *status);
static PgStat_StatTabEntry *pgstat_get_tab_entry(dshash_table *table, Oid tableoid, bool create);
static void pgstat_write_statsfiles(void);
static void pgstat_write_db_statsfile(PgStat_StatDBEntry *dbentry);
static void pgstat_read_statsfiles(void);
static void pgstat_read_db_statsfile(Oid databaseid, dshash_table *tabhash, dshash_table *funchash);

/* functions used in backends */
static bool backend_snapshot_global_stats(void);
static PgStat_StatFuncEntry *backend_get_func_etnry(PgStat_StatDBEntry *dbent, Oid funcid, bool oneshot);
static void pgstat_read_current_status(void);

static void pgstat_send_funcstats(void);
static HTAB *pgstat_collect_oids(Oid catalogid);
static void reset_dbentry_counters(PgStat_StatDBEntry *dbentry);
static PgStat_TableStatus *get_tabstat_entry(Oid rel_id, bool isshared);

static void pgstat_setup_memcxt(void);

static const char *pgstat_get_wait_activity(WaitEventActivity w);
static const char *pgstat_get_wait_client(WaitEventClient w);
static const char *pgstat_get_wait_ipc(WaitEventIPC w);
static const char *pgstat_get_wait_timeout(WaitEventTimeout w);
static const char *pgstat_get_wait_io(WaitEventIO w);

static dshash_table *pgstat_update_dbentry(Oid dboid);
static bool pgstat_update_tabentry(dshash_table *tabhash,
								   PgStat_TableStatus *stat);
static bool pgstat_update_funcentry(dshash_table *funchash,
									PgStat_BackendFunctionEntry *stat);
static bool pgstat_tabpurge(Oid dboid, Oid taboid);
static bool pgstat_funcpurge(Oid dboid, Oid funcoid);

/* ------------------------------------------------------------
 * Public functions called from postmaster follow
 * ------------------------------------------------------------
 */

/* ----------
 * pgstat_init() -
 *
 *	Called from postmaster at startup. Create the resources required
 *	by the statistics collector process.  If unable to do so, do not
 *	fail --- better to let the postmaster start with stats collection
 *	disabled.
 * ----------
 */
void
pgstat_init(void)
{
	return;
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
 * Remove the stats files.  This is currently used only if WAL
 * recovery is needed after a crash.
 */
void
pgstat_reset_all(void)
{
	pgstat_reset_remove_files(PGSTAT_STAT_PERMANENT_DIRECTORY);
}

void
allow_immediate_pgstat_restart(void)
{
	last_pgstat_start_time = 0;
}

/* ----------
 * pgstat_attach_shared_stats() -
 *
 *	attach existing shared stats memory
 * ----------
 */
static bool
pgstat_attach_shared_stats(void)
{
	MemoryContext oldcontext;

	LWLockAcquire(StatsLock, LW_EXCLUSIVE);
	if (StatsShmem->stats_dsa_handle == DSM_HANDLE_INVALID || area != NULL)
	{
		LWLockRelease(StatsLock);
		return area != NULL;
	}

	/* top level varialbles. lives for the lifetime of the process */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	area = dsa_attach(StatsShmem->stats_dsa_handle);
	dsa_pin_mapping(area);
	db_stats = dshash_attach(area, &dsh_dbparams,
							 StatsShmem->db_stats_handle, 0);
	snapshot_db_stats = NULL;
	shared_globalStats = (PgStat_GlobalStats *)
		dsa_get_address(area, StatsShmem->global_stats);
	shared_archiverStats =	(PgStat_ArchiverStats *)
		dsa_get_address(area, StatsShmem->archiver_stats);
	MemoryContextSwitchTo(oldcontext);
	LWLockRelease(StatsLock);

	return true;
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

	LWLockAcquire(StatsLock, LW_EXCLUSIVE);
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
	LWLockRelease(StatsLock);
}


/* ------------------------------------------------------------
 * Public functions used by backends follow
 *------------------------------------------------------------
 */
static bool pgstat_pending_tabstats = false;
static bool pgstat_pending_funcstats = false;
static bool pgstat_pending_vacstats = false;
static bool pgstat_pending_dropdb = false;
static bool pgstat_pending_resetcounter = false;
static bool pgstat_pending_resetsharedcounter = false;
static bool pgstat_pending_resetsinglecounter = false;
static bool pgstat_pending_autovac = false;
static bool pgstat_pending_vacuum = false;
static bool pgstat_pending_analyze = false;
static bool pgstat_pending_recoveryconflict = false;
static bool pgstat_pending_deadlock = false;
static bool pgstat_pending_tempfile = false;

void
pgstat_cleanup_pending_stat(void)
{
	if (pgstat_pending_tabstats)
		pgstat_report_stat(true);
	if (pgstat_pending_funcstats)
		pgstat_send_funcstats();
	if (pgstat_pending_vacstats)
		pgstat_vacuum_stat();
	if (pgstat_pending_dropdb)
		pgstat_drop_database(InvalidOid);
	if (pgstat_pending_resetcounter)
		pgstat_reset_counters();
	if (pgstat_pending_resetsharedcounter)
		pgstat_reset_shared_counters(NULL);
	if (pgstat_pending_resetsinglecounter)
		pgstat_reset_single_counter(InvalidOid, 0);
	if (pgstat_pending_autovac)
		pgstat_report_autovac(InvalidOid);
	if (pgstat_pending_vacuum)
		pgstat_report_vacuum(InvalidOid, false, 0, 0);
	if (pgstat_pending_analyze)
		pgstat_report_analyze(NULL, 0, 0, false);
	if (pgstat_pending_recoveryconflict)
		pgstat_report_recovery_conflict(-1);
	if (pgstat_pending_deadlock)
		pgstat_report_deadlock(true);
	if (pgstat_pending_tempfile)
		pgstat_report_tempfile(0);
}

/* ----------
 * pgstat_report_stat() -
 *
 *	Must be called by processes that performs DML: tcop/postgres.c, logical
 *	receiver processes, SPI worker, etc. to send the so far collected
 *	per-table and function usage statistics to the collector.  Note that this
 *	is called only when not within a transaction, so it is fair to use
 *	transaction stop time as an approximation of current time.
 * ----------
 */
void
pgstat_report_stat(bool force)
{
	/* we assume this inits to all zeroes: */
	static const PgStat_TableCounts all_zeroes;
	static TimestampTz last_report = 0;
	TimestampTz now;
	TabStatusArray *tsa;
	int			i;
	dshash_table *shared_tabhash = NULL;
	dshash_table *regular_tabhash = NULL;

	/* Don't expend a clock check if nothing to do */
	if (!pgstat_pending_tabstats &&
		(pgStatTabList == NULL || pgStatTabList->tsa_used == 0) &&
		pgStatXactCommit == 0 && pgStatXactRollback == 0 &&
		!pgstat_pending_funcstats)
		return;

	/*
	 * Don't update shared stats aunless it's been at least
	 * PGSTAT_STAT_INTERVAL msec since we last sent one, or the caller wants
	 * to force stats out.
	 */
	now = GetCurrentTransactionStopTimestamp();
	if (!force &&
		!TimestampDifferenceExceeds(last_report, now, PGSTAT_STAT_INTERVAL))
		return;
	last_report = now;

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

	pgstat_pending_tabstats = false;
	for (tsa = pgStatTabList; tsa != NULL; tsa = tsa->tsa_next)
	{
		int move_skipped_to = 0;

		for (i = 0; i < tsa->tsa_used; i++)
		{
			PgStat_TableStatus *entry = &tsa->tsa_entries[i];
			dshash_table *tabhash;

			/* Shouldn't have any pending transaction-dependent counts */
			Assert(entry->trans == NULL);

			/*
			 * Ignore entries that didn't accumulate any actual counts, such
			 * as indexes that were opened by the planner but not used.
			 */
			if (memcmp(&entry->t_counts, &all_zeroes,
					   sizeof(PgStat_TableCounts)) != 0)
			{
				/*
				 * OK, insert data into the appropriate message, and send if
				 * full.
				 */
				if (entry->t_shared)
				{
					if (!shared_tabhash)
						shared_tabhash = pgstat_update_dbentry(InvalidOid);
					tabhash = shared_tabhash;
				}
				else
				{
					if (!regular_tabhash)
						regular_tabhash = pgstat_update_dbentry(MyDatabaseId);
					tabhash = regular_tabhash;
				}

				/*
				 * If this entry failed to be processed, leave this entry for
				 * the next turn. The enties should be in head-filled manner.
				 */
				if (!pgstat_update_tabentry(tabhash, entry))
				{
					if (move_skipped_to < i)
						memmove(&tsa->tsa_entries[move_skipped_to],
								&tsa->tsa_entries[i],
								sizeof(PgStat_TableStatus));
					move_skipped_to++;
				}
			}
		}

		/* notify unapplied items are exists  */
		if (move_skipped_to > 0)
			pgstat_pending_tabstats = true;

		tsa->tsa_used = move_skipped_to;
		/* zero out TableStatus structs after use */
		MemSet(&tsa->tsa_entries[tsa->tsa_used], 0,
			   (TABSTAT_QUANTUM - tsa->tsa_used) * sizeof(PgStat_TableStatus));
	}

	/* Now, send function statistics */
	pgstat_send_funcstats();
}

/*
 * Subroutine for pgstat_report_stat: populate and send a function stat message
 */
static void
pgstat_send_funcstats(void)
{
	/* we assume this inits to all zeroes: */
	static const PgStat_FunctionCounts all_zeroes;

	PgStat_BackendFunctionEntry *entry;
	HASH_SEQ_STATUS fstat;
	PgStat_StatDBEntry *dbentry;
	pg_stat_table_result_status status;
	dshash_table *funchash;

	if (pgStatFunctions == NULL)
		return;

	pgstat_pending_funcstats = false;

	dbentry = pgstat_get_db_entry(MyDatabaseId,
								  PGSTAT_TABLE_WRITE
								  | PGSTAT_TABLE_CREATE
								  | PGSTAT_TABLE_NOWAIT,
								  &status);

	if (status == PGSTAT_TABLE_LOCK_FAILED)
		return;

	funchash = dshash_attach(area, &dsh_funcparams, dbentry->functions, 0);

	hash_seq_init(&fstat, pgStatFunctions);
	while ((entry = (PgStat_BackendFunctionEntry *) hash_seq_search(&fstat)) != NULL)
	{
		/* Skip it if no counts accumulated since last time */
		if (memcmp(&entry->f_counts, &all_zeroes,
				   sizeof(PgStat_FunctionCounts)) == 0)
			continue;

		if (pgstat_update_funcentry(funchash, entry))
		{
			/* reset the entry's counts */
			MemSet(&entry->f_counts, 0, sizeof(PgStat_FunctionCounts));
		}
		else
			pgstat_pending_funcstats = true;
	}
}


/* ----------
 * pgstat_vacuum_stat() -
 *
 *	Will tell the collector about objects he can get rid of.
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
	pg_stat_table_result_status status;
	bool		no_pending_stats;

	/* If not done for this transaction, take a snapshot of stats */
	if (!backend_snapshot_global_stats())
		return;

	/*
	 * Read pg_database and make a list of OIDs of all existing databases
	 */
	oidtab = pgstat_collect_oids(DatabaseRelationId);

	/*
	 * Search the database hash table for dead databases and tell the
	 * collector to drop them.
	 */

	dshash_seq_init(&dshstat, db_stats, true);
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

	pgstat_pending_vacstats = true;

	/*
	 * Lookup our own database entry; if not found, nothing more to do.
	 */
	dbentry = pgstat_get_db_entry(MyDatabaseId,
								  PGSTAT_TABLE_WRITE | PGSTAT_TABLE_NOWAIT,
								  &status);
	if (status == PGSTAT_TABLE_LOCK_FAILED)
		return;
	
	/*
	 * Similarly to above, make a list of all known relations in this DB.
	 */
	oidtab = pgstat_collect_oids(RelationRelationId);

	/*
	 * Check for all tables listed in stats hashtable if they still exist.
	 * Stats cache is useless here so directly search the shared hash.
	 */
	dshtable = dshash_attach(area, &dsh_tblparams, dbentry->tables, 0);
	dshash_seq_init(&dshstat, dshtable, false);
	no_pending_stats = true;
	while ((tabentry = (PgStat_StatTabEntry *) dshash_seq_next(&dshstat)) != NULL)
	{
		Oid			tabid = tabentry->tableid;

		CHECK_FOR_INTERRUPTS();

		if (hash_search(oidtab, (void *) &tabid, HASH_FIND, NULL) != NULL)
			continue;

		/* Not there, so purge this table */
		if (!pgstat_tabpurge(MyDatabaseId, tabid))
			no_pending_stats = false;
	}
	dshash_detach(dshtable);
	/* Clean up */
	hash_destroy(oidtab);

	/*
	 * Now repeat the above steps for functions.  However, we needn't bother
	 * in the common case where no function stats are being collected.
	 */
	dshtable = dshash_attach(area, &dsh_funcparams, dbentry->functions, 0);
	if (dshash_get_num_entries(dshtable) > 0)
	{
		oidtab = pgstat_collect_oids(ProcedureRelationId);

		dshash_seq_init(&dshstat, dshtable, false);
		while ((funcentry = (PgStat_StatFuncEntry *) dshash_seq_next(&dshstat)) != NULL)
		{
			Oid			funcid = funcentry->functionid;

			CHECK_FOR_INTERRUPTS();

			if (hash_search(oidtab, (void *) &funcid, HASH_FIND, NULL) != NULL)
				continue;

			/* Not there, so move this function */
			if (!pgstat_funcpurge(MyDatabaseId, funcid))
				no_pending_stats = false;
		}
		hash_destroy(oidtab);
	}
	dshash_detach(dshtable);

	if (no_pending_stats)
		pgstat_pending_vacstats = false;
}


/* ----------
 * pgstat_collect_oids() -
 *
 *	Collect the OIDs of all objects listed in the specified system catalog
 *	into a temporary hash table.  Caller should hash_destroy the result
 *	when done with it.  (However, we make the table in CurrentMemoryContext
 *	so that it will be freed properly in event of an error.)
 * ----------
 */
static HTAB *
pgstat_collect_oids(Oid catalogid)
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
		Oid			thisoid = HeapTupleGetOid(tup);

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
 *	Tell the collector that we just dropped a database.
 *	(If the message gets lost, we will still clean the dead DB eventually
 *	via future invocations of pgstat_vacuum_stat().)
 * ----------
 */
void
pgstat_drop_database(Oid databaseid)
{
	static List *pending_dbid = NIL;
	List *left_dbid = NIL;
	PgStat_StatDBEntry *dbentry;
	pg_stat_table_result_status status;
	ListCell *lc;

	if (OidIsValid(databaseid))
		pending_dbid = lappend_oid(pending_dbid, databaseid);
	pgstat_pending_dropdb = true;

	if (db_stats == NULL)
		return;

	foreach (lc, pending_dbid)
	{
		Oid dbid = lfirst_oid(lc);

		/*
		 * Lookup the database in the hashtable.
		 */
		dbentry = pgstat_get_db_entry(dbid,
									  PGSTAT_TABLE_WRITE | PGSTAT_TABLE_NOWAIT,
									  &status);

		/* skip on lock failure */
		if (status == PGSTAT_TABLE_LOCK_FAILED)
		{
			left_dbid = lappend_oid(left_dbid, dbid);
			continue;
		}

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

	list_free(pending_dbid);
	pending_dbid = left_dbid;

	/*  we're done if no pending database ids */
	if (list_length(pending_dbid) == 0)
		pgstat_pending_dropdb = false;
}


/* ----------
 * pgstat_reset_counters() -
 *
 *	Tell the statistics collector to reset counters for our database.
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_counters(void)
{
	PgStat_StatDBEntry *dbentry;
	pg_stat_table_result_status status;

	pgstat_pending_resetcounter = true;

	if (db_stats == NULL)
		return;

	/*
	 * Lookup the database in the hashtable.  Nothing to do if not there.
	 */
	dbentry = pgstat_get_db_entry(MyDatabaseId,
								  PGSTAT_TABLE_WRITE | PGSTAT_TABLE_NOWAIT,
								  &status);

	if (status == PGSTAT_TABLE_LOCK_FAILED)
		return;

	if (!dbentry)
	{
		pgstat_pending_resetcounter = false;
		return;
	}

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

	/*  we're done */
	pgstat_pending_resetcounter = false;
}

/* ----------
 * pgstat_reset_shared_counters() -
 *
 *	Tell the statistics collector to reset cluster-wide shared counters.
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_shared_counters(const char *target)
{
	static bool archiver_pending = false;
	static bool bgwriter_pending = false;
	bool	have_lock = false;

	if (strcmp(target, "archiver") == 0)
	{
		archiver_pending = true;
	}
	else if (strcmp(target, "bgwriter") == 0)
	{
		bgwriter_pending = true;
	}
	else if (target != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unrecognized reset target: \"%s\"", target),
				 errhint("Target must be \"archiver\" or \"bgwriter\".")));

	pgstat_pending_resetsharedcounter = true;

	if (db_stats == NULL)
		return;

	/* Reset the archiver statistics for the cluster. */
	if (archiver_pending && LWLockConditionalAcquire(StatsLock, LW_EXCLUSIVE))
	{
		memset(&shared_archiverStats, 0, sizeof(shared_archiverStats));
		shared_archiverStats->stat_reset_timestamp = GetCurrentTimestamp();
		archiver_pending = false;
		have_lock = true;
	}

	if (bgwriter_pending &&
		(have_lock || LWLockConditionalAcquire(StatsLock, LW_EXCLUSIVE)))
	{
		/* Reset the global background writer statistics for the cluster. */
		memset(&shared_globalStats, 0, sizeof(shared_globalStats));
		shared_globalStats->stat_reset_timestamp = GetCurrentTimestamp();
		bgwriter_pending = false;
		have_lock = true;
	}

	if (have_lock)
		LWLockRelease(StatsLock);

	/* notify any pending update  */
	pgstat_pending_resetsharedcounter =	(archiver_pending || bgwriter_pending);
}

/* ----------
 * pgstat_reset_single_counter() -
 *
 *	Tell the statistics collector to reset a single counter.
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_single_counter(Oid objoid, PgStat_Single_Reset_Type type)
{
	PgStat_StatDBEntry *dbentry;

	/* Don't defer */

	if (db_stats == NULL)
		return;

	dbentry = pgstat_get_db_entry(MyDatabaseId, PGSTAT_TABLE_WRITE, NULL);

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

	if (type == RESET_FUNCTION)
	{
		dshash_table *t =
			dshash_attach(area, &dsh_funcparams, dbentry->functions, 0);
		dshash_delete_key(t, (void *) &objoid);
	}

	dshash_release_lock(db_stats, dbentry);
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

	/* Don't defer */

	if (db_stats == NULL)
		return;

	/*
	 * Store the last autovacuum time in the database's hashtable entry.
	 */
	dbentry = pgstat_get_db_entry(dboid,
								  PGSTAT_TABLE_WRITE | PGSTAT_TABLE_CREATE,
								  NULL);

	dbentry->last_autovac_time = GetCurrentTimestamp();

	dshash_release_lock(db_stats, dbentry);
}


/* ---------
 * pgstat_report_vacuum() -
 *
 *	Tell the collector about the table we just vacuumed.
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

	/* Don't defer */

	if (db_stats == NULL || !pgstat_track_counts)
		return;

	dboid = shared ? InvalidOid : MyDatabaseId;

	/*
	 * Store the data in the table's hashtable entry.
	 */
	dbentry = pgstat_get_db_entry(dboid,
								  PGSTAT_TABLE_WRITE | PGSTAT_TABLE_CREATE,
								  NULL);
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
 *	Tell the collector about the table we just analyzed.
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

	/* Don't defer */

	if (db_stats == NULL || !pgstat_track_counts)
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
	dbentry = pgstat_get_db_entry(dboid,
								  PGSTAT_TABLE_WRITE | PGSTAT_TABLE_CREATE,
								  NULL);

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
 *	Tell the collector about a Hot Standby recovery conflict.
 * --------
 */
void
pgstat_report_recovery_conflict(int reason)
{
	static int pending_conflict_tablespace = 0;
	static int pending_conflict_lock = 0;
	static int pending_conflict_snapshot = 0;
	static int pending_conflict_bufferpin = 0;
	static int pending_conflict_startup_deadlock = 0;
	PgStat_StatDBEntry *dbentry;
	pg_stat_table_result_status status;

	if (db_stats == NULL || !pgstat_track_counts)
		return;

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
	pgstat_pending_recoveryconflict = true;

	dbentry = pgstat_get_db_entry(MyDatabaseId,
								  PGSTAT_TABLE_WRITE | PGSTAT_TABLE_CREATE |
								  PGSTAT_TABLE_NOWAIT,
								  &status);

	if (status == PGSTAT_TABLE_LOCK_FAILED)
		return;

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

	dshash_release_lock(db_stats, dbentry);
	
	pgstat_pending_recoveryconflict = false;
}

/* --------
 * pgstat_report_deadlock() -
 *
 *	Tell the collector about a deadlock detected.
 * --------
 */
void
pgstat_report_deadlock(bool pending)
{
	static int pending_deadlocks = 0;
	PgStat_StatDBEntry *dbentry;
	pg_stat_table_result_status status;

	if (db_stats == NULL || !pgstat_track_counts)
		return;

	pending_deadlocks++;
	pgstat_pending_deadlock = true;

	dbentry = pgstat_get_db_entry(MyDatabaseId,
								  PGSTAT_TABLE_WRITE | PGSTAT_TABLE_CREATE |
								  PGSTAT_TABLE_NOWAIT,
								  &status);

	if (status == PGSTAT_TABLE_LOCK_FAILED)
		return;

	dbentry->n_deadlocks += pending_deadlocks;
	pending_deadlocks = 0;
	pgstat_pending_deadlock = false;

	dshash_release_lock(db_stats, dbentry);
}

/* --------
 * pgstat_report_tempfile() -
 *
 *	Tell the collector about a temporary file.
 * --------
 */
void
pgstat_report_tempfile(size_t filesize)
{
	static size_t pending_filesize = 0;
	static size_t pending_files = 0;
	PgStat_StatDBEntry *dbentry;
	pg_stat_table_result_status status;

	if (filesize > 0) /* Is't there a case where filesize is really 0? */
	{
		pending_filesize += filesize; /* needs check overflow */
		pending_files++;
	}
	pgstat_pending_tempfile = true;

	if (db_stats == NULL || !pgstat_track_counts)
		return;

	dbentry = pgstat_get_db_entry(MyDatabaseId,
								  PGSTAT_TABLE_WRITE | PGSTAT_TABLE_CREATE |
								  PGSTAT_TABLE_NOWAIT,
								  &status);

	if (status == PGSTAT_TABLE_LOCK_FAILED)
		return;

	dbentry->n_temp_bytes += pending_filesize;
	dbentry->n_temp_files += pending_files;
	pending_filesize = 0;
	pending_files = 0;
	pgstat_pending_tempfile = false;

	dshash_release_lock(db_stats, dbentry);
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

	/* indicate that we have something to send */
	pgstat_pending_funcstats = true;
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
	MemoryContext oldcontext;

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

	/* Attached shared memory lives for the process lifetime */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	while (!pgstat_attach_shared_stats())
		sleep(1);

	MemoryContextSwitchTo(oldcontext);

	if (db_stats == NULL || !pgstat_track_counts)
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
 * pgstat_fetch_stat_dbentry() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	the collected statistics for one database or NULL. NULL doesn't mean
 *	that the database doesn't exist, it is just not yet known by the
 *	collector, so the caller is better off to report ZERO instead.
 * ----------
 */
PgStat_StatDBEntry *
pgstat_fetch_stat_dbentry(Oid dbid)
{
	PgStat_StatDBEntry *dbentry;

	dbentry = backend_get_db_entry(dbid, false);
	return dbentry;
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
	dbentry = backend_get_db_entry(MyDatabaseId, false);
	if (dbentry == NULL)
		return NULL;

	tabentry = backend_get_tab_entry(dbentry, relid, false);
	if (tabentry != NULL)
		return tabentry;

	/*
	 * If we didn't find it, maybe it's a shared table.
	 */
	dbentry = backend_get_db_entry(InvalidOid, false);
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
	dbentry = pgstat_get_db_entry(MyDatabaseId, PGSTAT_TABLE_READ, NULL);
	if (dbentry == NULL)
		return NULL;

	funcentry = backend_get_func_etnry(dbentry, func_id, false);
	if (funcentry == NULL)
		return NULL;

	return funcentry;
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

	/* Set up a process-exit hook to clean up */
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
 * ----------
 */
void
pgstat_bestart(void)
{
	TimestampTz proc_start_timestamp;
	SockAddr	clientaddr;
	volatile PgBackendStatus *beentry;

	/*
	 * To minimize the time spent modifying the PgBackendStatus entry, fetch
	 * all the needed data first.
	 *
	 * If we have a MyProcPort, use its session start time (for consistency,
	 * and to save a kernel call).
	 */
	if (MyProcPort)
		proc_start_timestamp = MyProcPort->SessionStartTime;
	else
		proc_start_timestamp = GetCurrentTimestamp();

	/*
	 * We may not have a MyProcPort (eg, if this is the autovacuum process).
	 * If so, use all-zeroes client address, which is dealt with specially in
	 * pg_stat_get_backend_client_addr and pg_stat_get_backend_client_port.
	 */
	if (MyProcPort)
		memcpy(&clientaddr, &MyProcPort->raddr, sizeof(clientaddr));
	else
		MemSet(&clientaddr, 0, sizeof(clientaddr));

	/*
	 * Initialize my status entry, following the protocol of bumping
	 * st_changecount before and after; and make sure it's even afterwards. We
	 * use a volatile pointer here to ensure the compiler doesn't try to get
	 * cute.
	 */
	beentry = MyBEEntry;

	/* pgstats state must be initialized from pgstat_initialize() */
	Assert(beentry != NULL);

	if (MyBackendId != InvalidBackendId)
	{
		if (IsAutoVacuumLauncherProcess())
		{
			/* Autovacuum Launcher */
			beentry->st_backendType = B_AUTOVAC_LAUNCHER;
		}
		else if (IsAutoVacuumWorkerProcess())
		{
			/* Autovacuum Worker */
			beentry->st_backendType = B_AUTOVAC_WORKER;
		}
		else if (am_walsender)
		{
			/* Wal sender */
			beentry->st_backendType = B_WAL_SENDER;
		}
		else if (IsBackgroundWorker)
		{
			/* bgworker */
			beentry->st_backendType = B_BG_WORKER;
		}
		else
		{
			/* client-backend */
			beentry->st_backendType = B_BACKEND;
		}
	}
	else
	{
		/* Must be an auxiliary process */
		Assert(MyAuxProcType != NotAnAuxProcess);
		switch (MyAuxProcType)
		{
			case StartupProcess:
				beentry->st_backendType = B_STARTUP;
				break;
			case BgWriterProcess:
				beentry->st_backendType = B_BG_WRITER;
				break;
			case CheckpointerProcess:
				beentry->st_backendType = B_CHECKPOINTER;
				break;
			case WalWriterProcess:
				beentry->st_backendType = B_WAL_WRITER;
				break;
			case WalReceiverProcess:
				beentry->st_backendType = B_WAL_RECEIVER;
				break;
			case StatsCollectorProcess:
				beentry->st_backendType = B_STATS_COLLECTOR;
				break;
			default:
				elog(FATAL, "unrecognized process type: %d",
					 (int) MyAuxProcType);
				proc_exit(1);
		}
	}

	do
	{
		pgstat_increment_changecount_before(beentry);
	} while ((beentry->st_changecount & 1) == 0);

	beentry->st_procpid = MyProcPid;
	beentry->st_proc_start_timestamp = proc_start_timestamp;
	beentry->st_activity_start_timestamp = 0;
	beentry->st_state_start_timestamp = 0;
	beentry->st_xact_start_timestamp = 0;
	beentry->st_databaseid = MyDatabaseId;

	/* We have userid for client-backends, wal-sender and bgworker processes */
	if (beentry->st_backendType == B_BACKEND
		|| beentry->st_backendType == B_WAL_SENDER
		|| beentry->st_backendType == B_BG_WORKER)
		beentry->st_userid = GetSessionUserId();
	else
		beentry->st_userid = InvalidOid;

	beentry->st_clientaddr = clientaddr;
	if (MyProcPort && MyProcPort->remote_hostname)
		strlcpy(beentry->st_clienthostname, MyProcPort->remote_hostname,
				NAMEDATALEN);
	else
		beentry->st_clienthostname[0] = '\0';
#ifdef USE_SSL
	if (MyProcPort && MyProcPort->ssl != NULL)
	{
		beentry->st_ssl = true;
		beentry->st_sslstatus->ssl_bits = be_tls_get_cipher_bits(MyProcPort);
		beentry->st_sslstatus->ssl_compression = be_tls_get_compression(MyProcPort);
		strlcpy(beentry->st_sslstatus->ssl_version, be_tls_get_version(MyProcPort), NAMEDATALEN);
		strlcpy(beentry->st_sslstatus->ssl_cipher, be_tls_get_cipher(MyProcPort), NAMEDATALEN);
		be_tls_get_peerdn_name(MyProcPort, beentry->st_sslstatus->ssl_clientdn, NAMEDATALEN);
	}
	else
	{
		beentry->st_ssl = false;
	}
#else
	beentry->st_ssl = false;
#endif
	beentry->st_state = STATE_UNDEFINED;
	beentry->st_appname[0] = '\0';
	beentry->st_activity_raw[0] = '\0';
	/* Also make sure the last byte in each string area is always 0 */
	beentry->st_clienthostname[NAMEDATALEN - 1] = '\0';
	beentry->st_appname[NAMEDATALEN - 1] = '\0';
	beentry->st_activity_raw[pgstat_track_activity_query_size - 1] = '\0';
	beentry->st_progress_command = PROGRESS_COMMAND_INVALID;
	beentry->st_progress_command_target = InvalidOid;

	/*
	 * we don't zero st_progress_param here to save cycles; nobody should
	 * examine it until st_progress_command has been set to something other
	 * than PROGRESS_COMMAND_INVALID
	 */

	pgstat_increment_changecount_after(beentry);

	/* Update app name to current GUC setting */
	if (application_name)
		pgstat_report_appname(application_name);
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
	volatile PgBackendStatus *beentry = MyBEEntry;

	/*
	 * If we got as far as discovering our own database ID, we can report what
	 * we did to the collector.  Otherwise, we'd be sending an invalid
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
	pgstat_increment_changecount_before(beentry);

	beentry->st_procpid = 0;	/* mark invalid */

	pgstat_increment_changecount_after(beentry);
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
			pgstat_increment_changecount_before(beentry);
			beentry->st_state = STATE_DISABLED;
			beentry->st_state_start_timestamp = 0;
			beentry->st_activity_raw[0] = '\0';
			beentry->st_activity_start_timestamp = 0;
			/* st_xact_start_timestamp and wait_event_info are also disabled */
			beentry->st_xact_start_timestamp = 0;
			proc->wait_event_info = 0;
			pgstat_increment_changecount_after(beentry);
		}
		return;
	}

	/*
	 * To minimize the time spent modifying the entry, fetch all the needed
	 * data first.
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
	pgstat_increment_changecount_before(beentry);

	beentry->st_state = state;
	beentry->st_state_start_timestamp = current_timestamp;

	if (cmd_str != NULL)
	{
		memcpy((char *) beentry->st_activity_raw, cmd_str, len);
		beentry->st_activity_raw[len] = '\0';
		beentry->st_activity_start_timestamp = start_timestamp;
	}

	pgstat_increment_changecount_after(beentry);
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

	pgstat_increment_changecount_before(beentry);
	beentry->st_progress_command = cmdtype;
	beentry->st_progress_command_target = relid;
	MemSet(&beentry->st_progress_param, 0, sizeof(beentry->st_progress_param));
	pgstat_increment_changecount_after(beentry);
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

	pgstat_increment_changecount_before(beentry);
	beentry->st_progress_param[index] = val;
	pgstat_increment_changecount_after(beentry);
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

	pgstat_increment_changecount_before(beentry);

	for (i = 0; i < nparam; ++i)
	{
		Assert(index[i] >= 0 && index[i] < PGSTAT_NUM_PROGRESS_PARAM);

		beentry->st_progress_param[index[i]] = val[i];
	}

	pgstat_increment_changecount_after(beentry);
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

	if (!beentry)
		return;
	if (!pgstat_track_activities
		&& beentry->st_progress_command == PROGRESS_COMMAND_INVALID)
		return;

	pgstat_increment_changecount_before(beentry);
	beentry->st_progress_command = PROGRESS_COMMAND_INVALID;
	beentry->st_progress_command_target = InvalidOid;
	pgstat_increment_changecount_after(beentry);
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
	pgstat_increment_changecount_before(beentry);

	memcpy((char *) beentry->st_appname, appname, len);
	beentry->st_appname[len] = '\0';

	pgstat_increment_changecount_after(beentry);
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
	pgstat_increment_changecount_before(beentry);
	beentry->st_xact_start_timestamp = tstamp;
	pgstat_increment_changecount_after(beentry);
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
	int			i;

	Assert(!pgStatRunningInCollector);
	if (localBackendStatusTable)
		return;					/* already done */

	pgstat_setup_memcxt();

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
		MemoryContextAlloc(pgStatLocalContext,
						   pgstat_track_activity_query_size * NumBackendStatSlots);
#ifdef USE_SSL
	localsslstatus = (PgBackendSSLStatus *)
		MemoryContextAlloc(pgStatLocalContext,
						   sizeof(PgBackendSSLStatus) * NumBackendStatSlots);
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

			pgstat_save_changecount_before(beentry, before_changecount);

			localentry->backendStatus.st_procpid = beentry->st_procpid;
			if (localentry->backendStatus.st_procpid > 0)
			{
				memcpy(&localentry->backendStatus, (char *) beentry, sizeof(PgBackendStatus));

				/*
				 * strcpy is safe even if the string is modified concurrently,
				 * because there's always a \0 at the end of the buffer.
				 */
				strcpy(localappname, (char *) beentry->st_appname);
				localentry->backendStatus.st_appname = localappname;
				strcpy(localclienthostname, (char *) beentry->st_clienthostname);
				localentry->backendStatus.st_clienthostname = localclienthostname;
				strcpy(localactivity, (char *) beentry->st_activity_raw);
				localentry->backendStatus.st_activity_raw = localactivity;
				localentry->backendStatus.st_ssl = beentry->st_ssl;
#ifdef USE_SSL
				if (beentry->st_ssl)
				{
					memcpy(localsslstatus, beentry->st_sslstatus, sizeof(PgBackendSSLStatus));
					localentry->backendStatus.st_sslstatus = localsslstatus;
				}
#endif
			}

			pgstat_save_changecount_after(beentry, after_changecount);
			if (before_changecount == after_changecount &&
				(before_changecount & 1) == 0)
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
		case WAIT_EVENT_LOGICAL_LAUNCHER_MAIN:
			event_name = "LogicalLauncherMain";
			break;
		case WAIT_EVENT_LOGICAL_APPLY_MAIN:
			event_name = "LogicalApplyMain";
			break;
		case WAIT_EVENT_PGSTAT_MAIN:
			event_name = "PgStatMain";
			break;
		case WAIT_EVENT_RECOVERY_WAL_ALL:
			event_name = "RecoveryWalAll";
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
		case WAIT_EVENT_BGWORKER_SHUTDOWN:
			event_name = "BgWorkerShutdown";
			break;
		case WAIT_EVENT_BGWORKER_STARTUP:
			event_name = "BgWorkerStartup";
			break;
		case WAIT_EVENT_BTREE_PAGE:
			event_name = "BtreePage";
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
		case WAIT_EVENT_PARALLEL_FINISH:
			event_name = "ParallelFinish";
			break;
		case WAIT_EVENT_PARALLEL_BITMAP_SCAN:
			event_name = "ParallelBitmapScan";
			break;
		case WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN:
			event_name = "ParallelCreateIndexScan";
			break;
		case WAIT_EVENT_PROCARRAY_GROUP_UPDATE:
			event_name = "ProcArrayGroupUpdate";
			break;
		case WAIT_EVENT_CLOG_GROUP_UPDATE:
			event_name = "ClogGroupUpdate";
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
			event_name = "LockFileCreateWRITE";
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

			pgstat_save_changecount_before(vbeentry, before_changecount);

			found = (vbeentry->st_procpid == pid);

			pgstat_save_changecount_after(vbeentry, after_changecount);

			if (before_changecount == after_changecount &&
				(before_changecount & 1) == 0)
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

const char *
pgstat_get_backend_desc(BackendType backendType)
{
	const char *backendDesc = "unknown process type";

	switch (backendType)
	{
		case B_AUTOVAC_LAUNCHER:
			backendDesc = "autovacuum launcher";
			break;
		case B_AUTOVAC_WORKER:
			backendDesc = "autovacuum worker";
			break;
		case B_BACKEND:
			backendDesc = "client backend";
			break;
		case B_BG_WORKER:
			backendDesc = "background worker";
			break;
		case B_BG_WRITER:
			backendDesc = "background writer";
			break;
		case B_CHECKPOINTER:
			backendDesc = "checkpointer";
			break;
		case B_STARTUP:
			backendDesc = "startup";
			break;
		case B_WAL_RECEIVER:
			backendDesc = "walreceiver";
			break;
		case B_WAL_SENDER:
			backendDesc = "walsender";
			break;
		case B_WAL_WRITER:
			backendDesc = "walwriter";
			break;
		case B_STATS_COLLECTOR:
			backendDesc = "stats collector";
			break;
	}

	return backendDesc;
}

/* ------------------------------------------------------------
 * Local support functions follow
 * ------------------------------------------------------------
 */

/* ----------
 * pgstat_send_archiver() -
 *
 *	Tell the collector about the WAL file that we successfully
 *	archived or failed to archive.
 * ----------
 */
void
pgstat_send_archiver(const char *xlog, bool failed)
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
 * pgstat_send_bgwriter() -
 *
 *		Send bgwriter statistics to the collector
 * ----------
 */
void
pgstat_send_bgwriter(void)
{
	/* We assume this initializes to zeroes */
	static const PgStat_BgWriter all_zeroes;

	PgStat_BgWriter *s = &BgWriterStats;
	MemoryContext oldcontext;

	/*
	 * This function can be called even if nothing at all has happened. In
	 * this case, avoid sending a completely empty message to the stats
	 * collector.
	 */
	if (memcmp(&BgWriterStats, &all_zeroes, sizeof(PgStat_BgWriter)) == 0)
		return;

	/* Attached shared memory lives for the process lifetime */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	while (!pgstat_attach_shared_stats())
		sleep(1);

	MemoryContextSwitchTo(oldcontext);

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

	/*
	 * Clear out the statistics buffer, so it can be re-used.
	 */
	MemSet(&BgWriterStats, 0, sizeof(BgWriterStats));
}


/* ----------
 * PgstatCollectorMain() -
 *
 *	Start up the statistics collector process.  This is the body of the
 *	postmaster child process.
 *
 *	The argc/argv parameters are valid only in EXEC_BACKEND case.
 * ----------
 */
void
PgstatCollectorMain(void)
{
	int			wr;

	/*
	 * Ignore all signals usually bound to some action in the postmaster,
	 * except SIGHUP and SIGQUIT.  Note we don't need a SIGUSR1 handler to
	 * support latch operations, because we only use a local latch.
	 */
	pqsignal(SIGHUP, pgstat_sighup_handler);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, pgstat_shutdown_handler);
	pqsignal(SIGQUIT, pgstat_quickdie_handler);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, SIG_IGN);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);

	PG_SETMASK(&UnBlockSig);

	/*
	 * Read in existing stats files or initialize the stats to zero.
	 */
	pgStatRunningInCollector = true;
	pgstat_read_statsfiles();

	for (;;)
	{
		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		/*
		 * Quit if we get SIGQUIT from the postmaster.
		 */
		if (got_SIGTERM)
			break;

		/*
		 * Reload configuration if we got SIGHUP from the postmaster.
		 */
		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
		
		wr = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1L,
					   WAIT_EVENT_PGSTAT_MAIN);

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (wr & WL_POSTMASTER_DEATH)
			break;
	}
		
	/*
	 * Save the final stats to reuse at next startup.
	 */
	pgstat_write_statsfiles();

	exit(0);
}


/* SIGQUIT signal handler for collector process */
static void
pgstat_quickdie_handler(SIGNAL_ARGS)
{
	PG_SETMASK(&BlockSig);

	/*
	 * We DO NOT want to run proc_exit() callbacks -- we're here because
	 * shared memory may be corrupted, so we don't want to try to clean up our
	 * transaction.  Just nail the windows shut and get out of town.  Now that
	 * there's an atexit callback to prevent third-party code from breaking
	 * things by calling exit() directly, we have to reset the callbacks
	 * explicitly to make this work as intended.
	 */
	on_exit_reset();

	/*
	 * Note we do exit(2) not exit(0).  This is to force the postmaster into a
	 * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
	 * backend.  This is necessary precisely because we don't clean up our
	 * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
	 * should ensure the postmaster sees this as a crash, too, but no harm in
	 * being doubly sure.)
	 */
	exit(2);
}

/* SIGHUP handler for collector process */
static void
pgstat_sighup_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGHUP = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

static void
pgstat_shutdown_handler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGTERM = true;

	SetLatch(MyLatch);

	errno = save_errno;
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


	tbl = dshash_create(area, &dsh_tblparams, 0);
	dbentry->tables = dshash_get_hash_table_handle(tbl);
	dshash_detach(tbl);

	tbl = dshash_create(area, &dsh_funcparams, 0);
	dbentry->functions = dshash_get_hash_table_handle(tbl);
	dshash_detach(tbl);

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
	bool		nowait = ((op & PGSTAT_TABLE_NOWAIT) != 0);
	bool		lock_acquired = true;
	bool		found = true;
	MemoryContext oldcontext;

	/* XXXXXXX */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	if (!pgstat_attach_shared_stats())
	{
		MemoryContextSwitchTo(oldcontext);
		return false;
	}
	MemoryContextSwitchTo(oldcontext);

	/* Lookup or create the hash table entry for this database */
	if (op & PGSTAT_TABLE_CREATE)
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
			dshash_find_extended(db_stats, &databaseid,
								 &lock_acquired, true, nowait);
		if (result == NULL)
			found = false;
	}

	/* Set return status if requested */
	if (status)
	{
		if (!lock_acquired)
		{
			Assert(nowait);
			*status = PGSTAT_TABLE_LOCK_FAILED;
		}
		else if (!found)
			*status = PGSTAT_TABLE_NOT_FOUND;
		else
			*status = PGSTAT_TABLE_FOUND;
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
 *		Write the global statistics file, as well as requested DB files.
 *
 *	When 'allDbs' is false, only the requested databases (listed in
 *	pending_write_requests) will be written; otherwise, all databases
 *	will be written.
 * ----------
 */
static void
pgstat_write_statsfiles(void)
{
	dshash_seq_status hstat;
	PgStat_StatDBEntry *dbentry;
	FILE	   *fpout;
	int32		format_id;
	const char *tmpfile = PGSTAT_STAT_PERMANENT_TMPFILE;
	const char *statfile = PGSTAT_STAT_PERMANENT_FILENAME;
	int			rc;

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
	rc = fwrite(shared_globalStats, sizeof(shared_globalStats), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Write archiver stats struct
	 */
	rc = fwrite(shared_archiverStats, sizeof(shared_archiverStats), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Walk through the database table.
	 */
	dshash_seq_init(&hstat, db_stats, false);
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

	/*
	 * Now throw away the list of requests.  Note that requests sent after we
	 * started the write are still waiting on the network socket.
	 */
	list_free(pending_write_requests);
	pending_write_requests = NIL;
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
	dshash_seq_init(&tstat, tbl, false);
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
	tbl = dshash_attach(area, &dsh_funcparams, dbentry->functions, 0);
	dshash_seq_init(&fstat, tbl, false);
	while ((funcentry = (PgStat_StatFuncEntry *) dshash_seq_next(&fstat)) != NULL)
	{
		fputc('F', fpout);
		rc = fwrite(funcentry, sizeof(PgStat_StatFuncEntry), 1, fpout);
		(void) rc;				/* we'll check for error with ferror */
	}
	dshash_detach(tbl);

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
static void
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

	Assert(pgStatRunningInCollector);
	/*
	 * The tables will live in pgStatLocalContext.
	 */
	pgstat_setup_memcxt();

	/*
	 * Create the DB hashtable and global stas area
	 */
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
			ereport(pgStatRunningInCollector ? LOG : WARNING,
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
		ereport(pgStatRunningInCollector ? LOG : WARNING,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		goto done;
	}

	/*
	 * Read global stats struct
	 */
	if (fread(shared_globalStats, 1, sizeof(shared_globalStats), fpin) != sizeof(shared_globalStats))
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
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
	if (fread(shared_archiverStats, 1, sizeof(shared_archiverStats), fpin) != sizeof(shared_archiverStats))
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
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
					ereport(pgStatRunningInCollector ? LOG : WARNING,
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
					ereport(pgStatRunningInCollector ? LOG : WARNING,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				memcpy(dbentry, &dbbuf, sizeof(PgStat_StatDBEntry));
				dbentry->tables = DSM_HANDLE_INVALID;
				dbentry->functions = DSM_HANDLE_INVALID;

				/*
				 * In the collector, disregard the timestamp we read from the
				 * permanent stats file; we should be willing to write a temp
				 * stats file immediately upon the first request from any
				 * backend.
				 */
				Assert(pgStatRunningInCollector);
				dbentry->stats_timestamp = 0;

				/*
				 * If requested, read the data from the database-specific
				 * file.  Otherwise we just leave the hashtables empty.
				 */
				tblstats = dshash_create(area, &dsh_tblparams, 0);
				dbentry->tables = dshash_get_hash_table_handle(tblstats);
				funcstats = dshash_create(area, &dsh_funcparams, 0);
				dbentry->functions =
					dshash_get_hash_table_handle(funcstats);
				dshash_release_lock(db_stats, dbentry);
				pgstat_read_db_statsfile(dbentry->databaseid,
										 tblstats, funcstats);
				dshash_detach(tblstats);
				dshash_detach(funcstats);
				break;

			case 'E':
				goto done;

			default:
				ereport(pgStatRunningInCollector ? LOG : WARNING,
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
	}
	else
		Assert(found);
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

	Assert(pgStatRunningInCollector);
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
			ereport(pgStatRunningInCollector ? LOG : WARNING,
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
		ereport(pgStatRunningInCollector ? LOG : WARNING,
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
					ereport(pgStatRunningInCollector ? LOG : WARNING,
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
					ereport(pgStatRunningInCollector ? LOG : WARNING,
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
					ereport(pgStatRunningInCollector ? LOG : WARNING,
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
					ereport(pgStatRunningInCollector ? LOG : WARNING,
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
				ereport(pgStatRunningInCollector ? LOG : WARNING,
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
 * snapshot_statentry_all() - Take a snapshot of all shared stats entries
 *
 * Returns a local hash contains all entries in the shared stats.
 *
 * The given dshash is used if any. Elsewise temporarily attach dsh_handle.
 */
static HTAB *
snapshot_statentry_all(const char *hashname,
					   dshash_table *dshash, dshash_table_handle dsh_handle,
					   const dshash_parameters *dsh_params)
{
	dshash_table *t;
	dshash_seq_status s;
	size_t keysize = dsh_params->key_size;
	size_t entrysize = dsh_params->entry_size;
	void *ps;
	int num_entries;
	HTAB *dest;

	t = dshash ? dshash : dshash_attach(area, dsh_params, dsh_handle, NULL);

	/*
	 * No need to create new hash if no entry exists. The number can change
	 * after this, but dynahash can store extra entries in the case.
	 */
	num_entries = dshash_get_num_entries(t);
	if (num_entries == 0)
	{
		dshash_detach(t);
		return NULL;
	}

	Assert(hashname);
	dest = create_local_stats_hash(hashname,
									keysize, entrysize, num_entries);

	dshash_seq_init(&s, t, true);
	while ((ps = dshash_seq_next(&s)) != NULL)
	{
		bool found;
		void *pd = hash_search(dest, ps, HASH_ENTER, &found);
		Assert(!found);
		memcpy(pd, ps, entrysize);
		/* dshash_seq_next releases entry lock automatically */
	}

	if (!dshash)
		dshash_detach(t);

	return dest;
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
	MemoryContext oldcontext;
	MemoryContextCallback *mcxt_cb;

	/* Nothing to do if already done */
	if (snapshot_globalStats)
		return true;

	Assert(!pgStatRunningInCollector);

	/* Attached shared memory lives for the process lifetime */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	if (!pgstat_attach_shared_stats())
	{
		MemoryContextSwitchTo(oldcontext);
		return false;
	}
	MemoryContextSwitchTo(oldcontext);

	Assert(snapshot_archiverStats == NULL);

	/*
	 * The snapshot lives within the current top transaction if any, or the
	 * current memory context liftime otherwise.
	 */
	if (IsTransactionState())
		MemoryContextSwitchTo(TopTransactionContext);

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
 * backend_get_db_entry() -
 *
 *	Find database stats entry on backends. The returned entries are cached
 *	until transaction end. If onshot is true, they are not cached and returned
 *	in a palloc'ed memory.
 */
PgStat_StatDBEntry *
backend_get_db_entry(Oid dbid, bool oneshot)
{
	/* take a local snapshot if we don't have one */
	char *hashname = "local database stats hash";

	/* If not done for this transaction, take a snapshot of global stats */
	if (!backend_snapshot_global_stats())
		return NULL;

	return snapshot_statentry(oneshot ? NULL : &snapshot_db_stats,
							  hashname, db_stats, 0, &dsh_dbparams,
							  dbid);
}

/* ----------
 * backend_snapshot_all_db_entries() -
 *
 *	Take a snapshot of all databsae stats at once into returned hash.
 */
HTAB *
backend_snapshot_all_db_entries(void)
{
	/* take a local snapshot if we don't have one */
	char *hashname = "local database stats hash";

	/* If not done for this transaction, take a snapshot of global stats */
	if (!backend_snapshot_global_stats())
		return NULL;

	return snapshot_statentry_all(hashname, db_stats, 0, &dsh_dbparams);
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
	return snapshot_statentry(oneshot ? NULL : &dbent->snapshot_tables,
							  hashname, NULL, dbent->functions, &dsh_funcparams,
							  funcid);
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

	/* Release memory, if any was allocated */
	if (pgStatLocalContext)
		MemoryContextDelete(pgStatLocalContext);

	/* Reset variables */
	pgStatLocalContext = NULL;
	pgStatDBHash = NULL;
	localBackendStatusTable = NULL;
	localNumBackends = 0;

	/*
	 * the parameter inform the function that it is not called from
	 * MemoryContextCallback
	 */
	backend_clean_snapshot_callback(&param);
}


/* ----------
 * pgstat_recv_tabstat() -
 *
 *	Count what the backend has done.
 * ----------
 */
static dshash_table *
pgstat_update_dbentry(Oid dboid)
{
	PgStat_StatDBEntry *dbentry;
	pg_stat_table_result_status status;
	dshash_table *tabhash;

	dbentry = pgstat_get_db_entry(dboid,
								  PGSTAT_TABLE_WRITE
								  | PGSTAT_TABLE_CREATE
								  | PGSTAT_TABLE_NOWAIT,
								  &status);
	
	/* return if lock failed */
	if (status == PGSTAT_TABLE_LOCK_FAILED)
		return NULL;

	tabhash = dshash_attach(area, &dsh_tblparams, dbentry->tables, 0);
	
	if (OidIsValid(dboid))
	{
		/*
		 * Update database-wide stats.
		 */
		dbentry->n_xact_commit += pgStatXactCommit;
		dbentry->n_xact_rollback += pgStatXactRollback;
		dbentry->n_block_read_time += pgStatBlockReadTime;
		dbentry->n_block_write_time += pgStatBlockWriteTime;
	}

	pgStatXactCommit = 0;
	pgStatXactRollback = 0;
	pgStatBlockReadTime = 0;
	pgStatBlockWriteTime = 0;

	dshash_release_lock(db_stats, dbentry);

	return tabhash;
}

static bool
pgstat_update_tabentry(dshash_table *tabhash, PgStat_TableStatus *stat)
{
	PgStat_StatTabEntry *tabentry;
	bool	found;

	if (tabhash == NULL)
		return false;

	tabentry = (PgStat_StatTabEntry *)
		dshash_find_or_insert_extended(tabhash, (void *) &(stat->t_id),
									   &found, true);

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


/* ----------
 * pgstat_recv_tabpurge() -
 *
 *	Arrange for dead table removal.
 * ----------
 */
static bool
pgstat_tabpurge(Oid dboid, Oid taboid)
{
	dshash_table *tbl;
	PgStat_StatDBEntry *dbentry;

	/* wait for lock */
	dbentry = pgstat_get_db_entry(dboid, PGSTAT_TABLE_WRITE, NULL);

	/*
	 * No need to purge if we don't even know the database.
	 */
	if (!dbentry || dbentry->tables == DSM_HANDLE_INVALID)
	{
		if (dbentry)
			dshash_release_lock(db_stats, dbentry);
		return true;
	}

	tbl = dshash_attach(area, &dsh_tblparams, dbentry->tables, 0);

	/* Remove from hashtable if present; we don't care if it's not. */
	(void) dshash_delete_key(tbl, (void *) &taboid);

	dshash_release_lock(db_stats, dbentry);

	return true;
}


/* ----------
 * pgstat_funcstat() -
 *
 *	Count what the backend has done.
 * ----------
 */
static bool
pgstat_update_funcentry(dshash_table *funchash,
						PgStat_BackendFunctionEntry *stat)
{
	PgStat_StatFuncEntry *funcentry;
	bool		found;

	funcentry = (PgStat_StatFuncEntry *)
		dshash_find_or_insert_extended(funchash, (void *) &(stat->f_id),
									   &found, true);

	if (funcentry == NULL)
		return false;

	if (!found)
	{
		/*
		 * If it's a new function entry, initialize counters to the values
		 * we just got.
		 */
		funcentry->f_numcalls = stat->f_counts.f_numcalls;
		funcentry->f_total_time =
			INSTR_TIME_GET_MICROSEC(stat->f_counts.f_total_time);
		funcentry->f_self_time =
			INSTR_TIME_GET_MICROSEC(stat->f_counts.f_self_time);
	}
	else
	{
		/*
		 * Otherwise add the values to the existing entry.
		 */
		funcentry->f_numcalls += stat->f_counts.f_numcalls;
		funcentry->f_total_time +=
			INSTR_TIME_GET_MICROSEC(stat->f_counts.f_total_time);
		funcentry->f_self_time +=
			INSTR_TIME_GET_MICROSEC(stat->f_counts.f_self_time);
	}

	dshash_release_lock(funchash, funcentry);

	return true;
}

/* ----------
 * pgstat_recv_funcpurge() -
 *
 *	Arrange for dead function removal.
 * ----------
 */
static bool
pgstat_funcpurge(Oid dboid, Oid funcoid)
{
	dshash_table *t;
	PgStat_StatDBEntry *dbentry;

	/* wait for lock */
	dbentry = pgstat_get_db_entry(dboid, PGSTAT_TABLE_WRITE, NULL);

	/*
	 * No need to purge if we don't even know the database.
	 */
	if (!dbentry || dbentry->functions == DSM_HANDLE_INVALID)
		return true;

	t = dshash_attach(area, &dsh_funcparams, dbentry->functions, 0);

	/* Remove from hashtable if present; we don't care if it's not. */
	dshash_delete_key(t, (void *) &funcoid);
	dshash_detach(t);

	dshash_release_lock(db_stats, dbentry);

	return true;
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
