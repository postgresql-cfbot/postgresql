/*-------------------------------------------------------------------------
 *
 * checksumhelper.c
 *	  Background worker to walk the database and write checksums to pages
 *
 * When enabling data checksums on a database at initdb time, no extra process
 * is required as each page is checksummed, and verified, at accesses.  When
 * enabling checksums on an already running cluster, which was not initialized
 * with checksums, this helper worker will ensure that all pages are
 * checksummed before verification of the checksums is turned on.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/checksumhelper.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "commands/vacuum.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"
#include "postmaster/checksumhelper.h"
#include "storage/bufmgr.h"
#include "storage/checksum.h"
#include "storage/lmgr.h"
#include "storage/ipc.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/ps_status.h"

/*
 * Maximum number of times to try enabling checksums in a specific database
 * before giving up.
 */
#define MAX_ATTEMPTS 4

typedef struct ChecksumHelperShmemStruct
{
	pg_atomic_flag launcher_started;
	bool		success;
	bool		process_shared_catalogs;
	/* Parameter values set on start */
	int			cost_delay;
	int			cost_limit;
}			ChecksumHelperShmemStruct;

/* Shared memory segment for checksum helper */
static ChecksumHelperShmemStruct * ChecksumHelperShmem;

/* Bookkeeping for work to do */
typedef struct ChecksumHelperDatabase
{
	Oid			dboid;
	char	   *dbname;
	int			attempts;
	bool		success;
}			ChecksumHelperDatabase;

typedef struct ChecksumHelperRelation
{
	Oid			reloid;
	char		relkind;
	bool		success;
}			ChecksumHelperRelation;

/* Prototypes */
static List *BuildDatabaseList(void);
static List *BuildRelationList(bool include_shared);
static bool ProcessDatabase(ChecksumHelperDatabase * db);

/*
 * Main entry point for checksum helper launcher process.
 */
bool
StartChecksumHelperLauncher(int cost_delay, int cost_limit)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *bgw_handle;
	HeapTuple	tup;
	Relation	rel;
	HeapScanDesc scan;

	/*
	 * Check that all databases allow connections.  This will be re-checked
	 * when we build the list of databases to work on, the point of duplicating
	 * this is to catch any databases we won't be able to open while we can
	 * still send an error message to the client.
	 */
	rel = heap_open(DatabaseRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdb = (Form_pg_database) GETSTRUCT(tup);
		if (!pgdb->datallowconn)
			ereport(ERROR,
					(errmsg("database \"%s\" does not allow connections",
							NameStr(pgdb->datname)),
					 errhint("Allow connections using ALTER DATABASE and try again.")));
	}

	heap_endscan(scan);
	heap_close(rel, AccessShareLock);

	if (!pg_atomic_test_set_flag(&ChecksumHelperShmem->launcher_started))
	{
		/* Failed to set means somebody else started */
		ereport(ERROR,
				(errmsg("could not start checksum helper: already running")));
	}

	ChecksumHelperShmem->cost_delay = cost_delay;
	ChecksumHelperShmem->cost_limit = cost_limit;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ChecksumHelperLauncherMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "checksum helper launcher");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "checksum helper launcher");
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_notify_pid = MyProcPid;
	bgw.bgw_main_arg = (Datum) 0;

	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
	{
		pg_atomic_clear_flag(&ChecksumHelperShmem->launcher_started);
		return false;
	}

	return true;
}

void
ShutdownChecksumHelperIfRunning(void)
{
	if (pg_atomic_unlocked_test_flag(&ChecksumHelperShmem->launcher_started))
	{
		/* Launcher not started, so nothing to shut down */
		return;
	}

	ereport(ERROR,
			(errmsg("checksum helper is currently running, cannot disable checksums"),
			 errhint("Restart the cluster or wait for the worker to finish.")));
}

/*
 * Enable checksums in a single relation/fork.
 * XXX: must hold a lock on the relation preventing it from being truncated?
 */
static bool
ProcessSingleRelationFork(Relation reln, ForkNumber forkNum, BufferAccessStrategy strategy)
{
	BlockNumber numblocks = RelationGetNumberOfBlocksInFork(reln, forkNum);
	BlockNumber b;

	for (b = 0; b < numblocks; b++)
	{
		Buffer		buf = ReadBufferExtended(reln, forkNum, b, RBM_NORMAL, strategy);

		/* Need to get an exclusive lock before we can flag as dirty */
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		/*
		 * Mark the buffer as dirty and force a full page write.  We have to
		 * re-write the page to wal even if the checksum hasn't changed,
		 * because if there is a replica it might have a slightly different
		 * version of the page with an invalid checksum, caused by unlogged
		 * changes (e.g. hintbits) on the master happening while checksums were
		 * off. This can happen if there was a valid checksum on the page at
		 * one point in the past, so only when checksums are first on, then
		 * off, and then turned on again.
		 */
		START_CRIT_SECTION();
		MarkBufferDirty(buf);
		log_newpage_buffer(buf, false);
		END_CRIT_SECTION();

		UnlockReleaseBuffer(buf);

		vacuum_delay_point();
	}

	return true;
}

static bool
ProcessSingleRelationByOid(Oid relationId, BufferAccessStrategy strategy)
{
	Relation	rel;
	ForkNumber	fnum;

	StartTransactionCommand();

	elog(DEBUG2, "Checksumhelper starting to process relation %d", relationId);
	rel = relation_open(relationId, AccessShareLock);
	RelationOpenSmgr(rel);

	for (fnum = 0; fnum <= MAX_FORKNUM; fnum++)
	{
		if (smgrexists(rel->rd_smgr, fnum))
			ProcessSingleRelationFork(rel, fnum, strategy);
	}
	relation_close(rel, AccessShareLock);
	elog(DEBUG2, "Checksumhelper done with relation %d", relationId);

	CommitTransactionCommand();

	return true;
}

/*
 * ProcessDatabase
 *		Enable checksums in a single database.
 *
 * We do this by launching a dynamic background worker into this database, and
 * waiting for it to finish.  We have to do this in a separate worker, since
 * each process can only be connected to one database during its lifetime.
 */
static bool
ProcessDatabase(ChecksumHelperDatabase * db)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *bgw_handle;
	BgwHandleStatus status;
	pid_t		pid;

	ChecksumHelperShmem->success = false;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ChecksumHelperWorkerMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "checksum helper worker");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "checksum helper worker");
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_notify_pid = MyProcPid;
	bgw.bgw_main_arg = ObjectIdGetDatum(db->dboid);

	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
	{
		ereport(LOG,
				(errmsg("failed to start worker for checksum helper in \"%s\"",
				 db->dbname)));
		return false;
	}

	status = WaitForBackgroundWorkerStartup(bgw_handle, &pid);
	if (status != BGWH_STARTED)
	{
		ereport(LOG,
				(errmsg("failed to wait for worker startup for checksum helper in \"%s\"",
				 db->dbname)));
		return false;
	}

	ereport(DEBUG1,
			(errmsg("started background worker for checksums in \"%s\"",
			 db->dbname)));

	status = WaitForBackgroundWorkerShutdown(bgw_handle);
	if (status != BGWH_STOPPED)
	{
		ereport(LOG,
				(errmsg("failed to wait for worker shutdown for checksum helper in \"%s\"",
				 db->dbname)));
		return false;
	}

	ereport(DEBUG1,
			(errmsg("background worker for checksums in \"%s\" completed",
			 db->dbname)));

	return ChecksumHelperShmem->success;
}

static void
launcher_exit(int code, Datum arg)
{
	pg_atomic_clear_flag(&ChecksumHelperShmem->launcher_started);
}

void
ChecksumHelperLauncherMain(Datum arg)
{
	List	   *DatabaseList;

	on_shmem_exit(launcher_exit, 0);

	ereport(DEBUG1,
			(errmsg("checksumhelper launcher started")));

	pqsignal(SIGTERM, die);

	BackgroundWorkerUnblockSignals();

	init_ps_display(pgstat_get_backend_desc(B_CHECKSUMHELPER_LAUNCHER), "", "", "");

	/*
	 * Initialize a connection to shared catalogs only.
	 */
	BackgroundWorkerInitializeConnection(NULL, NULL);

	/*
	 * Set up so first run processes shared catalogs, but not once in every db.
	 */
	ChecksumHelperShmem->process_shared_catalogs = true;

	/*
	 * Create a database list.  We don't need to concern ourselves with
	 * rebuilding this list during runtime since any new created database will
	 * be running with checksums turned on from the start.
	 */
	DatabaseList = BuildDatabaseList();

	/*
	 * If there are no databases at all to checksum, we can exit immediately
	 * as there is no work to do.
	 */
	if (DatabaseList == NIL || list_length(DatabaseList) == 0)
		return;

	while (true)
	{
		List	   *remaining = NIL;
		ListCell   *lc,
				   *lc2;
		List	   *CurrentDatabases = NIL;

		foreach(lc, DatabaseList)
		{
			ChecksumHelperDatabase *db = (ChecksumHelperDatabase *) lfirst(lc);

			if (ProcessDatabase(db))
			{
				pfree(db->dbname);
				pfree(db);

				if (ChecksumHelperShmem->process_shared_catalogs)

					/*
					 * Now that one database has completed shared catalogs, we
					 * don't have to process them again.
					 */
					ChecksumHelperShmem->process_shared_catalogs = false;
			}
			else
			{
				/*
				 * Put failed databases on the remaining list.
				 */
				remaining = lappend(remaining, db);
			}
		}
		list_free(DatabaseList);

		DatabaseList = remaining;
		remaining = NIL;

		/*
		 * DatabaseList now has all databases not yet processed. This can be
		 * because they failed for some reason, or because the database was
		 * dropped between us getting the database list and trying to process
		 * it. Get a fresh list of databases to detect the second case where
		 * the database was dropped before we had started processing it.
		 * Any database that still exists but where enabling checksums failed,
		 * is retried for a limited number of times before giving up. Any
		 * database that remains in failed state after the retries expire will
		 * fail the entire operation.
		 */
		CurrentDatabases = BuildDatabaseList();

		foreach(lc, DatabaseList)
		{
			ChecksumHelperDatabase *db = (ChecksumHelperDatabase *) lfirst(lc);
			bool		found = false;

			foreach(lc2, CurrentDatabases)
			{
				ChecksumHelperDatabase *db2 = (ChecksumHelperDatabase *) lfirst(lc2);

				if (db->dboid == db2->dboid)
				{
					/* Database still exists, time to give up? */
					if (++db->attempts > MAX_ATTEMPTS)
					{
						/* Disable checksums on cluster, because we failed */
						SetDataChecksumsOff();

						ereport(ERROR,
								(errmsg("failed to enable checksums in \"%s\", giving up.",
										db->dbname)));
					}
					else
						/* Try again with this db */
						remaining = lappend(remaining, db);
					found = true;
					break;
				}
			}
			if (!found)
			{
				ereport(LOG,
						(errmsg("database \"%s\" has been dropped, skipping",
								db->dbname)));
				pfree(db->dbname);
				pfree(db);
			}
		}

		/* Free the extra list of databases */
		foreach(lc, CurrentDatabases)
		{
			ChecksumHelperDatabase *db = (ChecksumHelperDatabase *) lfirst(lc);

			pfree(db->dbname);
			pfree(db);
		}
		list_free(CurrentDatabases);

		/* All databases processed yet? */
		if (remaining == NIL || list_length(remaining) == 0)
			break;

		DatabaseList = remaining;
	}

	/*
	 * Force a checkpoint to get everything out to disk.
	 */
	RequestCheckpoint(CHECKPOINT_FORCE | CHECKPOINT_WAIT | CHECKPOINT_IMMEDIATE);

	/*
	 * Everything has been processed, so flag checksums enabled.
	 */
	SetDataChecksumsOn();

	ereport(LOG,
			(errmsg("checksums enabled, checksumhelper launcher shutting down")));
}

/*
 * ChecksumHelperShmemSize
 *		Compute required space for checksumhelper-related shared memory
 */
Size
ChecksumHelperShmemSize(void)
{
	Size		size;

	size = sizeof(ChecksumHelperShmemStruct);
	size = MAXALIGN(size);

	return size;
}

/*
 * ChecksumHelperShmemInit
 *		Allocate and initialize checksumhelper-related shared memory
 */
void
ChecksumHelperShmemInit(void)
{
	bool		found;

	ChecksumHelperShmem = (ChecksumHelperShmemStruct *)
		ShmemInitStruct("ChecksumHelper Data",
						ChecksumHelperShmemSize(),
						&found);

	pg_atomic_init_flag(&ChecksumHelperShmem->launcher_started);
}

/*
 * BuildDatabaseList
 *		Compile a list of all currently available databases in the cluster
 *
 * This is intended to create the worklist for the workers to go through, and
 * as we are only concerned with already existing databases we need to ever
 * rebuild this list, which simplifies the coding.
 */
static List *
BuildDatabaseList(void)
{
	List	   *DatabaseList = NIL;
	Relation	rel;
	HeapScanDesc scan;
	HeapTuple	tup;
	MemoryContext ctx = CurrentMemoryContext;
	MemoryContext oldctx;

	StartTransactionCommand();

	rel = heap_open(DatabaseRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdb = (Form_pg_database) GETSTRUCT(tup);
		ChecksumHelperDatabase *db;

		if (!pgdb->datallowconn)
			ereport(ERROR,
					(errmsg("database \"%s\" does not allow connections",
							NameStr(pgdb->datname)),
					 errhint("Allow connections using ALTER DATABASE and try again.")));

		oldctx = MemoryContextSwitchTo(ctx);

		db = (ChecksumHelperDatabase *) palloc(sizeof(ChecksumHelperDatabase));

		db->dboid = HeapTupleGetOid(tup);
		db->dbname = pstrdup(NameStr(pgdb->datname));

		DatabaseList = lappend(DatabaseList, db);

		MemoryContextSwitchTo(oldctx);
	}

	heap_endscan(scan);
	heap_close(rel, AccessShareLock);

	CommitTransactionCommand();

	return DatabaseList;
}

/*
 * BuildRelationList
 *		Compile a list of all relations in the database
 *
 * If shared is true, both shared relations and local ones are returned, else
 * all non-shared relations are returned.
 */
static List *
BuildRelationList(bool include_shared)
{
	List	   *RelationList = NIL;
	Relation	rel;
	HeapScanDesc scan;
	HeapTuple	tup;
	MemoryContext ctx = CurrentMemoryContext;
	MemoryContext oldctx;

	StartTransactionCommand();

	rel = heap_open(RelationRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_class pgc = (Form_pg_class) GETSTRUCT(tup);
		ChecksumHelperRelation *relentry;

		if (pgc->relisshared && !include_shared)
			continue;

		/*
		 * Foreign tables have by definition no local storage that can be
		 * checksummed, so skip.
		 */
		if (pgc->relkind == RELKIND_FOREIGN_TABLE)
			continue;

		oldctx = MemoryContextSwitchTo(ctx);
		relentry = (ChecksumHelperRelation *) palloc(sizeof(ChecksumHelperRelation));

		relentry->reloid = HeapTupleGetOid(tup);
		relentry->relkind = pgc->relkind;

		RelationList = lappend(RelationList, relentry);

		MemoryContextSwitchTo(oldctx);
	}

	heap_endscan(scan);
	heap_close(rel, AccessShareLock);

	CommitTransactionCommand();

	return RelationList;
}

/*
 * Main function for enabling checksums in a single database
 */
void
ChecksumHelperWorkerMain(Datum arg)
{
	Oid			dboid = DatumGetObjectId(arg);
	List	   *RelationList = NIL;
	ListCell   *lc;
	BufferAccessStrategy strategy;

	pqsignal(SIGTERM, die);

	BackgroundWorkerUnblockSignals();

	init_ps_display(pgstat_get_backend_desc(B_CHECKSUMHELPER_WORKER), "", "", "");

	ereport(DEBUG1,
			(errmsg("checksum worker starting for database oid %d", dboid)));

	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid);

	/*
	 * Enable vacuum cost delay, if any.
	 */
	VacuumCostDelay = ChecksumHelperShmem->cost_delay;
	VacuumCostLimit = ChecksumHelperShmem->cost_limit;
	VacuumCostActive = (VacuumCostDelay > 0);
	VacuumCostBalance = 0;
	VacuumPageHit = 0;
	VacuumPageMiss = 0;
	VacuumPageDirty = 0;

	/*
	 * Create and set the vacuum strategy as our buffer strategy.
	 */
	strategy = GetAccessStrategy(BAS_VACUUM);

	RelationList = BuildRelationList(ChecksumHelperShmem->process_shared_catalogs);
	foreach(lc, RelationList)
	{
		ChecksumHelperRelation *rel = (ChecksumHelperRelation *) lfirst(lc);

		if (!ProcessSingleRelationByOid(rel->reloid, strategy))
		{
			ereport(ERROR,
					(errmsg("failed to process table with oid %d", rel->reloid)));
		}
	}
	list_free_deep(RelationList);

	ChecksumHelperShmem->success = true;

	ereport(DEBUG1,
			(errmsg("checksum worker completed in database oid %d", dboid)));
}
