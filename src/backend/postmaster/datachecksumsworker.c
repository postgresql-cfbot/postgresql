/*-------------------------------------------------------------------------
 *
 * datachecksumsworker.c
 *	  Background worker for enabling or disabling data checksums online
 *
 * When enabling data checksums on a database at initdb time or with
 * pg_checksums, no extra process is required as each page is checksummed, and
 * verified, when accessed.  When enabling checksums on an already running
 * cluster, which was not initialized with checksums, this worker will ensure
 * that all pages are checksummed before verification of the checksums is
 * turned on. In the case of disabling checksums, the state transition is
 * recorded in the catalog and controlfile, no changes are performed
 * on the data pages or in the catalog.
 *
 * Checksums can be either enabled or disabled clusterwide, with on/off being
 * the endstate for data_checkums.
 *
 * Enabling checksums
 * ------------------
 * When enabling checkums in an online cluster, data_checksums will be set to
 * inprogress which signals that write operations MUST compute and write
 * the checksum on the data page, but during reading the checksum SHALL NOT be
 * verified. This ensures that all objects created during checksumming will
 * have checksums set, but no reads will fail due to incorrect checksum. The
 * DataChecksumsWorker will compile a list of databases which exists at the
 * start of checksumming, and all of these which havent been dropped during
 * the processing MUST have been processed successfully in order for checksums
 * to be enabled. Any new relation created during processing will see the
 * inprogress state and will automatically be checksummed as well as have its
 * state recorded in the catalog to avoid the datachecksumworker having to
 * process it when already checksummed.
 *
 * For each database, all relations which have storage are read and every data
 * page is marked dirty to force a write with the checksum, this will generate
 * a lot of WAL as the entire database is read and written. Once all datapages
 * in a relation have been written, pg_class.relhaschecksums is set to true to
 * indicate that the relation is done.
 *
 * If the processing is interrupted by a cluster restart, it will be restarted
 * from where it left off given that pg_class.relhaschecksums track state of
 * processed relations and the inprogress state will ensure all new writes
 * performed with checksums. Each database will be reprocessed, but relations
 * where pg_class.relhaschecksums is true are skipped.
 *
 * In case checksums have been enabled and later disabled, when re-enabling
 * pg_class.relhaschecksums will be reset to false before entering inprogress
 * mode to ensure that all relations are re-processed.
 *
 *
 * Disabling checksums
 * -------------------
 * Disabling checksums is done as an immediate operation as it only updates
 * the controlfile and accompanying local state in the backends. No changes
 * to pg_class.relhaschecksums is performed as it only tracks state during
 * enabling.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/datachecksumsworker.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "commands/vacuum.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"
#include "postmaster/datachecksumsworker.h"
#include "storage/bufmgr.h"
#include "storage/checksum.h"
#include "storage/lmgr.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/ps_status.h"
#include "utils/syscache.h"

#define DATACHECKSUMSWORKER_MAX_DB_RETRIES 5

typedef enum
{
	DATACHECKSUMSWORKER_SUCCESSFUL = 0,
	DATACHECKSUMSWORKER_ABORTED,
	DATACHECKSUMSWORKER_FAILED,
	DATACHECKSUMSWORKER_RETRYDB,
}			DatachecksumsWorkerResult;

typedef struct DatachecksumsWorkerShmemStruct
{
	/*
	 * Access to launcher_started and abort must be protected by
	 * DatachecksumsWorkerLock.
	 */
	bool		launcher_started;
	bool		abort;

	/*
	 * Access to other members can be done without a lock, as while they are
	 * in shared memory, they are never concurrently accessed. When a worker
	 * is running, the launcher is only waiting for that worker to finish.
	 */
	DatachecksumsWorkerResult success;
	bool		process_shared_catalogs;
	/* Parameter values set on start */
	int			cost_delay;
	int			cost_limit;
	DataChecksumOperation	operation;
}			DatachecksumsWorkerShmemStruct;

/* Shared memory segment for datachecksumsworker */
static DatachecksumsWorkerShmemStruct * DatachecksumsWorkerShmem;

/* Bookkeeping for work to do */
typedef struct DatachecksumsWorkerDatabase
{
	Oid			dboid;
	char	   *dbname;
}			DatachecksumsWorkerDatabase;

typedef struct DatachecksumsWorkerRelation
{
	Oid			reloid;
	char		relkind;
}			DatachecksumsWorkerRelation;

typedef struct DatachecksumsWorkerResultEntry
{
	Oid						dboid;
	DatachecksumsWorkerResult	result;
	int						retries;
}			DatachecksumsWorkerResultEntry;


/* Prototypes */
static List *BuildDatabaseList(void);
static List *BuildRelationList(bool include_shared);
static List *BuildTempTableList(void);
static DatachecksumsWorkerResult ProcessDatabase(DatachecksumsWorkerDatabase * db);
static bool ProcessAllDatabases(bool already_connected);
static void launcher_cancel_handler(SIGNAL_ARGS);
static void SetRelHasChecksums(Oid relOid);

bool
DataChecksumWorkerStarted(void)
{
	bool		started = false;

	LWLockAcquire(DatachecksumsWorkerLock, LW_EXCLUSIVE);
	if (DatachecksumsWorkerShmem->launcher_started && !DatachecksumsWorkerShmem->abort)
		started = true;
	LWLockRelease(DatachecksumsWorkerLock);

	return started;
}

/*
 * Main entry point for datachecksumsworker launcher process.
 */
void
StartDatachecksumsWorkerLauncher(DataChecksumOperation op,
								 int cost_delay, int cost_limit)
{
	BackgroundWorker		bgw;
	BackgroundWorkerHandle *bgw_handle;

	/*
	 * This can be hit during a short window during which the worker is
	 * shutting down. Once done the worker will clear the abort flag and
	 * re-processing can be performed.
	 */
	LWLockAcquire(DatachecksumsWorkerLock, LW_EXCLUSIVE);
	if (DatachecksumsWorkerShmem->abort)
	{
		LWLockRelease(DatachecksumsWorkerLock);
		ereport(ERROR,
				(errmsg("data checksums worker has been aborted")));
	}

	if (DatachecksumsWorkerShmem->launcher_started)
	{
		/* Failed to set means somebody else started */
		LWLockRelease(DatachecksumsWorkerLock);
		ereport(NOTICE,
				(errmsg("data checksums worker is already running")));
		return;
	}

	/* Whether to enable or disable checksums */
	DatachecksumsWorkerShmem->operation = op;

	/* Backoff parameters to throttle the load during enabling */
	DatachecksumsWorkerShmem->cost_delay = cost_delay;
	DatachecksumsWorkerShmem->cost_limit = cost_limit;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "DatachecksumsWorkerLauncherMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "datachecksumsworker launcher");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "datachecksumsworker launcher");
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_notify_pid = MyProcPid;
	bgw.bgw_main_arg = (Datum) 0;

	DatachecksumsWorkerShmem->launcher_started = true;
	LWLockRelease(DatachecksumsWorkerLock);

	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
	{
		LWLockAcquire(DatachecksumsWorkerLock, LW_EXCLUSIVE);
		DatachecksumsWorkerShmem->launcher_started = false;
		LWLockRelease(DatachecksumsWorkerLock);
		ereport(ERROR,
				(errmsg("failed to start background worker to process data checksums")));
	}
}

/*
 * ShutdownDatachecksumsWorkerIfRunning
 *		Request shutdown of the datachecksumworker
 *
 * This does not turn off processing immediately, it signals the checksum
 * process to end when done with the current block.
 */
void
ShutdownDatachecksumsWorkerIfRunning(void)
{
	LWLockAcquire(DatachecksumsWorkerLock, LW_EXCLUSIVE);

	/* If the launcher isn't started, there is nothing to shut down */
	if (DatachecksumsWorkerShmem->launcher_started)
		DatachecksumsWorkerShmem->abort = true;

	LWLockRelease(DatachecksumsWorkerLock);
}

/*
 * ProcessSingleRelationFork
 *		Enable checksums in a single relation/fork.
 *
 * Returns true if successful, and false if *aborted*. On error, an actual
 * error is raised in the lower levels.
 */
static bool
ProcessSingleRelationFork(Relation reln, ForkNumber forkNum, BufferAccessStrategy strategy)
{
	BlockNumber numblocks = RelationGetNumberOfBlocksInFork(reln, forkNum);
	BlockNumber b;
	char		activity[NAMEDATALEN * 2 + 128];

	for (b = 0; b < numblocks; b++)
	{
		Buffer		buf = ReadBufferExtended(reln, forkNum, b, RBM_NORMAL, strategy);

		/*
		 * Report to pgstat every 100 blocks to keep from overwhelming the
		 * activity reporting with close to identical reports.
		 */
		if ((b % 100) == 0)
		{
			snprintf(activity, sizeof(activity) - 1, "processing: %s.%s (%s block %d/%d)",
					 get_namespace_name(RelationGetNamespace(reln)), RelationGetRelationName(reln),
					 forkNames[forkNum], b, numblocks);
			pgstat_report_activity(STATE_RUNNING, activity);
		}

		/* Need to get an exclusive lock before we can flag as dirty */
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		/*
		 * Mark the buffer as dirty and force a full page write.  We have to
		 * re-write the page to WAL even if the checksum hasn't changed,
		 * because if there is a replica it might have a slightly different
		 * version of the page with an invalid checksum, caused by unlogged
		 * changes (e.g. hintbits) on the master happening while checksums
		 * were off. This can happen if there was a valid checksum on the page
		 * at one point in the past, so only when checksums are first on, then
		 * off, and then turned on again.
		 */
		START_CRIT_SECTION();
		MarkBufferDirty(buf);
		log_newpage_buffer(buf, false);
		END_CRIT_SECTION();

		UnlockReleaseBuffer(buf);

		/*
		 * This is the only place where we check if we are asked to abort, the
		 * abortion will bubble up from here. It's safe to check this without
		 * a lock, because if we miss it being set, we will try again soon.
		 */
		if (DatachecksumsWorkerShmem->abort)
			return false;

		vacuum_delay_point();
	}

	return true;
}

/*
 * ProcessSingleRelationByOid
 *		Process a single relation based on oid.
 *
 * Returns true if successful, and false if *aborted*. On error, an actual
 * error is raised in the lower levels.
 */
static bool
ProcessSingleRelationByOid(Oid relationId, BufferAccessStrategy strategy)
{
	Relation	rel;
	ForkNumber	fnum;
	bool		aborted = false;

	StartTransactionCommand();

	elog(DEBUG2,
		 "background worker \"datachecksumsworker\" starting to process relation %u",
		 relationId);

	rel = try_relation_open(relationId, AccessShareLock);
	if (rel == NULL)
	{
		/*
		 * Relation no longer exist. We don't consider this an error since
		 * there are no pages in it that need checksums, and thus return true.
		 */
		elog(DEBUG1,
			 "background worker \"datachecksumsworker\" skipping relation %u as it no longer exists",
			 relationId);
		CommitTransactionCommand();
		pgstat_report_activity(STATE_IDLE, NULL);
		return true;
	}
	RelationOpenSmgr(rel);

	for (fnum = 0; fnum <= MAX_FORKNUM; fnum++)
	{
		if (smgrexists(rel->rd_smgr, fnum))
		{
			if (!ProcessSingleRelationFork(rel, fnum, strategy))
			{
				aborted = true;
				break;
			}
		}
	}
	relation_close(rel, AccessShareLock);
	elog(DEBUG2,
		 "background worker \"datachecksumsworker\" done with relation %u: %s",
		 relationId, (aborted ? "aborted" : "finished"));

	if (!aborted)
		SetRelHasChecksums(relationId);

	CommitTransactionCommand();

	pgstat_report_activity(STATE_IDLE, NULL);

	return !aborted;
}

static void
SetRelHasChecksums(Oid relOid)
{
	Relation		rel;
	Form_pg_class	pg_class_tuple;
	HeapTuple		tuple;

	rel = table_open(RelationRelationId, RowExclusiveLock);

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relOid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relOid);

	pg_class_tuple = (Form_pg_class) GETSTRUCT(tuple);
	pg_class_tuple->relhaschecksums = true;

	CatalogTupleUpdate(rel, &tuple->t_self, tuple);

	ReleaseSysCache(tuple);

	table_close(rel, RowExclusiveLock);
}

/*
 * ProcessDatabase
 *		Enable checksums in a single database.
 *
 * We do this by launching a dynamic background worker into this database, and
 * waiting for it to finish.  We have to do this in a separate worker, since
 * each process can only be connected to one database during its lifetime.
 */
static DatachecksumsWorkerResult
ProcessDatabase(DatachecksumsWorkerDatabase * db)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *bgw_handle;
	BgwHandleStatus status;
	pid_t		pid;
	char		activity[NAMEDATALEN + 64];

	DatachecksumsWorkerShmem->success = DATACHECKSUMSWORKER_FAILED;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "postgres");
	if (DatachecksumsWorkerShmem->operation == ENABLE_CHECKSUMS)
		snprintf(bgw.bgw_function_name, BGW_MAXLEN, "DatachecksumsWorkerMain");
	else if (DatachecksumsWorkerShmem->operation == RESET_STATE)
		snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ResetDataChecksumStateInDatabase");
	else
		elog(ERROR, "invalid datachecksumworker operation requested: %d",
			 DatachecksumsWorkerShmem->operation);
	snprintf(bgw.bgw_name, BGW_MAXLEN, "datachecksumsworker worker");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "datachecksumsworker worker");
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_notify_pid = MyProcPid;
	bgw.bgw_main_arg = ObjectIdGetDatum(db->dboid);

	/*
	 * If there are no worker slots available, make sure we retry processing
	 * this database. This will make the datachecksumsworker move on to the next
	 * database and quite likely fail with the same problem. TODO: Maybe we
	 * need a backoff to avoid running through all the databases here in short
	 * order.
	 */
	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
	{
		ereport(WARNING,
				(errmsg("failed to start worker for enabling checksums in \"%s\", retrying",
						db->dbname),
				 errhint("The max_worker_processes setting might be too low.")));
		return DATACHECKSUMSWORKER_RETRYDB;
	}

	status = WaitForBackgroundWorkerStartup(bgw_handle, &pid);
	if (status == BGWH_STOPPED)
	{
		ereport(WARNING,
				(errmsg("could not start background worker for enabling checksums in \"%s\"",
						db->dbname),
				 errhint("More details on the error might be found in the server log.")));
		return DATACHECKSUMSWORKER_FAILED;
	}

	/*
	 * If the postmaster crashed we cannot end up with a processed database
	 * so we have no alternative other than exiting. When enabling checksums
	 * we won't at this time have changed the pg_control version to enabled
	 * so when the cluster comes back up processing will habe to be resumed.
	 * When disabling, the pg_control version will be set to off before this
	 * so when the cluster comes up checksums will be off as expected. In the
	 * latter case we might have stale relhaschecksums flags in pg_class which
	 * need to be handled in some way. TODO
	 */
	if (status == BGWH_POSTMASTER_DIED)
		ereport(FATAL,
				(errmsg("cannot enable checksums without the postmaster process"),
				 errhint("Restart the database and restart the checksumming process by calling pg_enable_data_checksums().")));

	Assert(status == BGWH_STARTED);
	ereport(DEBUG1,
			(errmsg("started background worker \"datachecksumsworker\" in database \"%s\"",
					db->dbname)));

	snprintf(activity, sizeof(activity) - 1,
			 "Waiting for worker in database %s (pid %d)", db->dbname, pid);
	pgstat_report_activity(STATE_RUNNING, activity);

	status = WaitForBackgroundWorkerShutdown(bgw_handle);
	if (status == BGWH_POSTMASTER_DIED)
		ereport(FATAL,
				(errmsg("postmaster exited during checksum processing in \"%s\"",
						db->dbname),
				 errhint("Restart the database and restart the checksumming process by calling pg_enable_data_checksums().")));

	if (DatachecksumsWorkerShmem->success == DATACHECKSUMSWORKER_ABORTED)
		ereport(LOG,
				(errmsg("background worker for enabling checksums was aborted during processing in \"%s\"",
						db->dbname)));

	ereport(DEBUG1,
			(errmsg("background worker \"datachecksumsworker\" in \"%s\" completed",
					db->dbname)));

	pgstat_report_activity(STATE_IDLE, NULL);

	return DatachecksumsWorkerShmem->success;
}

static void
launcher_exit(int code, Datum arg)
{
	LWLockAcquire(DatachecksumsWorkerLock, LW_EXCLUSIVE);
	DatachecksumsWorkerShmem->abort = false;
	DatachecksumsWorkerShmem->launcher_started = false;
	LWLockRelease(DatachecksumsWorkerLock);
}

static void
launcher_cancel_handler(SIGNAL_ARGS)
{
	LWLockAcquire(DatachecksumsWorkerLock, LW_EXCLUSIVE);
	DatachecksumsWorkerShmem->abort = true;
	LWLockRelease(DatachecksumsWorkerLock);
}

static void
WaitForAllTransactionsToFinish(void)
{
	TransactionId	waitforxid;
	bool			aborted = false;

	LWLockAcquire(XidGenLock, LW_SHARED);
	waitforxid = XidFromFullTransactionId(ShmemVariableCache->nextFullXid);
	LWLockRelease(XidGenLock);

	while (!aborted)
	{
		TransactionId	oldestxid = GetOldestActiveTransactionId();

		if (TransactionIdPrecedes(oldestxid, waitforxid))
		{
			char		activity[64];
			int			rc;

			/* Oldest running xid is older than us, so wait */
			snprintf(activity,
					 sizeof(activity),
					 "Waiting for current transactions to finish (waiting for %u)",
					 waitforxid);
			pgstat_report_activity(STATE_RUNNING, activity);

			/* Retry every 5 seconds */
			ResetLatch(MyLatch);
			rc = WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   5000,
						   WAIT_EVENT_CHECKSUM_ENABLE_STARTCONDITION);

			LWLockAcquire(DatachecksumsWorkerLock, LW_EXCLUSIVE);

			/*
			 * If the postmaster died we wont be able to enable checksums
			 * clusterwide so abort and hope to continue when restarted.
			 */
			if (rc & WL_POSTMASTER_DEATH)
				DatachecksumsWorkerShmem->abort = true;
			aborted = DatachecksumsWorkerShmem->abort;

			LWLockRelease(DatachecksumsWorkerLock);
		}
		else
		{
			pgstat_report_activity(STATE_IDLE, NULL);
			return;
		}
	}
}

void
DatachecksumsWorkerLauncherMain(Datum arg)
{
	bool connected = false;

	on_shmem_exit(launcher_exit, 0);

	ereport(DEBUG1,
			(errmsg("background worker \"datachecksumsworker\" launcher started")));

	pqsignal(SIGTERM, die);
	pqsignal(SIGINT, launcher_cancel_handler);

	BackgroundWorkerUnblockSignals();

	MyBackendType = B_DATACHECKSUMSWORKER_LAUNCHER;
	init_ps_display(NULL);

	if (DatachecksumsWorkerShmem->operation == RESET_STATE)
	{
		if (!ProcessAllDatabases(connected))
		{
			/*
			 * Before we error out make sure we clear state since this may
			 * otherwise render the worker stuck without possibility of a
			 * restart.
			 */
			LWLockAcquire(DatachecksumsWorkerLock, LW_EXCLUSIVE);
			DatachecksumsWorkerShmem->launcher_started = false;
			DatachecksumsWorkerShmem->abort = false;
			LWLockRelease(DatachecksumsWorkerLock);
			ereport(ERROR,
					(errmsg("unable to finish processing")));
		}

		connected = true;
		SetDataChecksumsOnInProgress();

		LWLockAcquire(DatachecksumsWorkerLock, LW_EXCLUSIVE);
		DatachecksumsWorkerShmem->operation = ENABLE_CHECKSUMS;
		LWLockRelease(DatachecksumsWorkerLock);
	}

	/*
	 * Prepare for datachecksumworker shutdown, once we signal that checksums
	 * are enabled we want the worker to be done and exited to avoid races
	 * with immediate disabling/enabling.
	 */
	LWLockAcquire(DatachecksumsWorkerLock, LW_EXCLUSIVE);
	DatachecksumsWorkerShmem->abort = false;
	DatachecksumsWorkerShmem->launcher_started = false;
	LWLockRelease(DatachecksumsWorkerLock);

	/*
	 * If processing succeeds for ENABLE_CHECKSUMS, then everything has been
	 * processed so set checksums as enabled clusterwide
	 */
	if (ProcessAllDatabases(connected))
	{
		SetDataChecksumsOn();
		ereport(LOG,
			(errmsg("checksums enabled clusterwide")));
	}
}

static bool
ProcessAllDatabases(bool already_connected)
{
	List	   *DatabaseList;
	HTAB	   *ProcessedDatabases = NULL;
	ListCell   *lc;
	HASHCTL		hash_ctl;
	bool		found_failed = false;

	/* Initialize a hash tracking all processed databases */
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(DatachecksumsWorkerResultEntry);
	ProcessedDatabases = hash_create("Processed databases",
									 64,
									 &hash_ctl,
									 HASH_ELEM | HASH_BLOBS);

	/*
	 * Initialize a connection to shared catalogs only.
	 */
	if (!already_connected)
		BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	/*
	 * Set up so first run processes shared catalogs, but not once in every
	 * db.
	 */
	DatachecksumsWorkerShmem->process_shared_catalogs = true;

	while (true)
	{
		int			processed_databases = 0;

		/*
		 * Get a list of all databases to process. This may include databases
		 * that were created during our runtime.
		 *
		 * Since a database can be created as a copy of any other database
		 * (which may not have existed in our last run), we have to repeat
		 * this loop until no new databases show up in the list. Since we wait
		 * for all pre-existing transactions finish, this way we can be
		 * certain that there are no databases left without checksums.
		 */
		DatabaseList = BuildDatabaseList();

		foreach(lc, DatabaseList)
		{
			DatachecksumsWorkerDatabase *db = (DatachecksumsWorkerDatabase *) lfirst(lc);
			DatachecksumsWorkerResult result;
			DatachecksumsWorkerResultEntry *entry;
			bool			found;

			elog(DEBUG1, "Starting processing of database %s with oid %u", db->dbname, db->dboid);

			entry = (DatachecksumsWorkerResultEntry *) hash_search(ProcessedDatabases, &db->dboid,
								HASH_FIND, NULL);

			if (entry)
			{
				if (entry->result == DATACHECKSUMSWORKER_RETRYDB)
				{
					/*
					 * Limit the number of retries to avoid infinite looping
					 * in case there simply wont be enough workers in the
					 * cluster to finish this operation.
					 */
					if (entry->retries > DATACHECKSUMSWORKER_MAX_DB_RETRIES)
						entry->result = DATACHECKSUMSWORKER_FAILED;
				}

				/* Skip if this database has been processed already */
				if (entry->result != DATACHECKSUMSWORKER_RETRYDB)
				{
					pfree(db->dbname);
					pfree(db);
					continue;
				}
			}

			result = ProcessDatabase(db);
			processed_databases++;

			if (result == DATACHECKSUMSWORKER_SUCCESSFUL)
			{
				/*
				 * If one database has completed shared catalogs, we don't
				 * have to process them again.
				 */
				if (DatachecksumsWorkerShmem->process_shared_catalogs)
					DatachecksumsWorkerShmem->process_shared_catalogs = false;
			}
			else if (result == DATACHECKSUMSWORKER_ABORTED)
				/* Abort flag set, so exit the whole process */
				return false;

			entry = hash_search(ProcessedDatabases, &db->dboid, HASH_ENTER, &found);
			entry->dboid = db->dboid;
			entry->result = result;
			if (!found)
				entry->retries = 0;
			else
				entry->retries++;

			pfree(db->dbname);
			pfree(db);
		}

		elog(DEBUG1,
			 "completed one pass over all databases for checksum enabling, %i databases processed",
			 processed_databases);

		list_free(DatabaseList);

		/*
		 * If no databases were processed in this run of the loop, we have now
		 * finished all databases and no concurrently created ones can exist.
		 */
		if (processed_databases == 0)
			break;
	}

	/*
	 * ProcessedDatabases now has all databases and the results of their
	 * processing. Failure to enable checksums for a database can be because
	 * they actually failed for some reason, or because the database was
	 * dropped between us getting the database list and trying to process it.
	 * Get a fresh list of databases to detect the second case where the
	 * database was dropped before we had started processing it. If a database
	 * still exists, but enabling checksums failed then we fail the entire
	 * checksumming process and exit with an error.
	 */
	DatabaseList = BuildDatabaseList();

	foreach(lc, DatabaseList)
	{
		DatachecksumsWorkerDatabase *db = (DatachecksumsWorkerDatabase *) lfirst(lc);
		DatachecksumsWorkerResult *entry;
		bool		found;

		entry = hash_search(ProcessedDatabases, (void *) &db->dboid,
							HASH_FIND, &found);

		/*
		 * We are only interested in the databases where the failed database
		 * still exist.
		 */
		if (found && *entry == DATACHECKSUMSWORKER_FAILED)
		{
			ereport(WARNING,
					(errmsg("failed to enable checksums in \"%s\"",
							db->dbname)));
			found_failed = found;
			continue;
		}
	}

	if (found_failed)
	{
		/* Disable checksums on cluster, because we failed */
		SetDataChecksumsOff();
		ereport(ERROR,
				(errmsg("checksums failed to get enabled in all databases, aborting"),
				 errhint("The server log might have more information on the error.")));
	}

	/*
	 * Force a checkpoint to get everything out to disk. TODO: we probably
	 * don't want to use a CHECKPOINT_IMMEDIATE here but it's very convenient
	 * for testing until the patch is fully baked, as it may otherwise make
	 * tests take a lot longer.
	 */
	RequestCheckpoint(CHECKPOINT_FORCE | CHECKPOINT_WAIT | CHECKPOINT_IMMEDIATE);

	return true;
}

/*
 * DatachecksumsWorkerShmemSize
 *		Compute required space for datachecksumsworker-related shared memory
 */
Size
DatachecksumsWorkerShmemSize(void)
{
	Size		size;

	size = sizeof(DatachecksumsWorkerShmemStruct);
	size = MAXALIGN(size);

	return size;
}

/*
 * DatachecksumsWorkerShmemInit
 *		Allocate and initialize datachecksumsworker-related shared memory
 */
void
DatachecksumsWorkerShmemInit(void)
{
	bool		found;

	DatachecksumsWorkerShmem = (DatachecksumsWorkerShmemStruct *)
		ShmemInitStruct("DatachecksumsWorker Data",
						DatachecksumsWorkerShmemSize(),
						&found);

	if (!found)
	{
		MemSet(DatachecksumsWorkerShmem, 0, DatachecksumsWorkerShmemSize());

		/*
		 * Even if this is a redundant assignment, we want to be explicit about
		 * our intent for readability, since we want to be able to query this
		 * state in case of restartability.
		 */
		DatachecksumsWorkerShmem->launcher_started = false;
	}
}

/*
 * BuildDatabaseList
 *		Compile a list of all currently available databases in the cluster
 *
 * This creates the list of databases for the datachecksumsworker workers to add
 * checksums to.
 */
static List *
BuildDatabaseList(void)
{
	List	   *DatabaseList = NIL;
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tup;
	MemoryContext ctx = CurrentMemoryContext;
	MemoryContext oldctx;

	StartTransactionCommand();

	rel = table_open(DatabaseRelationId, AccessShareLock);

	/*
	 * Before we do this, wait for all pending transactions to finish. This
	 * will ensure there are no concurrently running CREATE DATABASE, which
	 * could cause us to miss the creation of a database that was copied
	 * without checksums.
	 */
	WaitForAllTransactionsToFinish();

	scan = table_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdb = (Form_pg_database) GETSTRUCT(tup);
		DatachecksumsWorkerDatabase *db;

		oldctx = MemoryContextSwitchTo(ctx);

		db = (DatachecksumsWorkerDatabase *) palloc(sizeof(DatachecksumsWorkerDatabase));

		db->dboid = pgdb->oid;
		db->dbname = pstrdup(NameStr(pgdb->datname));

		DatabaseList = lappend(DatabaseList, db);

		MemoryContextSwitchTo(oldctx);
	}

	table_endscan(scan);
	table_close(rel, AccessShareLock);

	CommitTransactionCommand();

	return DatabaseList;
}

/*
 * BuildRelationList
 *		Compile a list of all relations in the database
 *
 * If shared is true, both shared relations and local ones are returned, else
 * all non-shared relations are returned. Temp tables are not included.
 */
static List *
BuildRelationList(bool include_shared)
{
	List	   *RelationList = NIL;
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tup;
	MemoryContext ctx = CurrentMemoryContext;
	MemoryContext oldctx;

	StartTransactionCommand();

	rel = table_open(RelationRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_class pgc = (Form_pg_class) GETSTRUCT(tup);
		DatachecksumsWorkerRelation *relentry;

		if (!RELKIND_HAS_STORAGE(pgc->relkind) ||
			pgc->relpersistence == RELPERSISTENCE_TEMP)
			continue;

		if (pgc->relhaschecksums)
			continue;

		if (pgc->relisshared && !include_shared)
			continue;

		oldctx = MemoryContextSwitchTo(ctx);
		relentry = (DatachecksumsWorkerRelation *) palloc(sizeof(DatachecksumsWorkerRelation));

		relentry->reloid = pgc->oid;
		relentry->relkind = pgc->relkind;

		RelationList = lappend(RelationList, relentry);

		MemoryContextSwitchTo(oldctx);
	}

	table_endscan(scan);
	table_close(rel, AccessShareLock);

	CommitTransactionCommand();

	return RelationList;
}

/*
 * BuildTempTableList
 *		Compile a list of all temporary tables in database
 *
 * Returns a List of oids.
 */
static List *
BuildTempTableList(void)
{
	List	   *RelationList = NIL;
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tup;
	MemoryContext ctx = CurrentMemoryContext;
	MemoryContext oldctx;

	StartTransactionCommand();

	rel = table_open(RelationRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_class pgc = (Form_pg_class) GETSTRUCT(tup);

		if (pgc->relpersistence != RELPERSISTENCE_TEMP)
			continue;

		oldctx = MemoryContextSwitchTo(ctx);
		RelationList = lappend_oid(RelationList, pgc->oid);
		MemoryContextSwitchTo(oldctx);
	}

	table_endscan(scan);
	table_close(rel, AccessShareLock);

	CommitTransactionCommand();

	return RelationList;
}

void
ResetDataChecksumStateInDatabase(Datum arg)
{
	Relation		rel;
	HeapTuple		tuple;
	Oid				dboid = DatumGetObjectId(arg);
	TableScanDesc	scan;
	Form_pg_class	pgc;

	pqsignal(SIGTERM, die);

	BackgroundWorkerUnblockSignals();

	MyBackendType = B_DATACHECKSUMSWORKER_WORKER;
	init_ps_display(NULL);

	ereport(DEBUG1,
			(errmsg("background worker \"datachecksumsworker\" starting for database oid %d to reset state",
					dboid)));

	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid, BGWORKER_BYPASS_ALLOWCONN);

	StartTransactionCommand();

	rel = table_open(RelationRelationId, RowExclusiveLock);
	scan = table_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection)))
	{
		tuple = heap_copytuple(tuple);
		pgc = (Form_pg_class) GETSTRUCT(tuple);

		if (pgc->relhaschecksums)
		{
			pgc->relhaschecksums = false;
			CatalogTupleUpdate(rel, &tuple->t_self, tuple);
		}

		heap_freetuple(tuple);
	}

	table_endscan(scan);
	table_close(rel, RowExclusiveLock);

	CommitTransactionCommand();

	DatachecksumsWorkerShmem->success = DATACHECKSUMSWORKER_SUCCESSFUL;
	ereport(DEBUG1,
			(errmsg("background worker \"datachecksumsworker\" completed resetting state in database oid %d",
					dboid)));
}

/*
 * Main function for enabling checksums in a single database
 */
void
DatachecksumsWorkerMain(Datum arg)
{
	Oid			dboid = DatumGetObjectId(arg);
	List	   *RelationList = NIL;
	List	   *InitialTempTableList = NIL;
	ListCell   *lc;
	BufferAccessStrategy strategy;
	bool		aborted = false;

	pqsignal(SIGTERM, die);

	BackgroundWorkerUnblockSignals();

	MyBackendType = B_DATACHECKSUMSWORKER_WORKER;
	init_ps_display(NULL);

	ereport(DEBUG1,
			(errmsg("background worker \"datachecksumsworker\" starting for database oid %d",
					dboid)));

	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid, BGWORKER_BYPASS_ALLOWCONN);

	/*
	 * Get a list of all temp tables present as we start in this database. We
	 * need to wait until they are all gone until we are done, since we cannot
	 * access those files and modify them.
	 */
	InitialTempTableList = BuildTempTableList();

	/*
	 * Enable vacuum cost delay, if any.
	 */
	VacuumCostDelay = DatachecksumsWorkerShmem->cost_delay;
	VacuumCostLimit = DatachecksumsWorkerShmem->cost_limit;
	VacuumCostActive = (VacuumCostDelay > 0);
	VacuumCostBalance = 0;
	VacuumPageHit = 0;
	VacuumPageMiss = 0;
	VacuumPageDirty = 0;

	/*
	 * Create and set the vacuum strategy as our buffer strategy.
	 */
	strategy = GetAccessStrategy(BAS_VACUUM);

	RelationList = BuildRelationList(DatachecksumsWorkerShmem->process_shared_catalogs);
	foreach(lc, RelationList)
	{
		DatachecksumsWorkerRelation *rel = (DatachecksumsWorkerRelation *) lfirst(lc);

		if (!ProcessSingleRelationByOid(rel->reloid, strategy))
		{
			aborted = true;
			break;
		}
	}
	list_free_deep(RelationList);

	if (aborted)
	{
		DatachecksumsWorkerShmem->success = DATACHECKSUMSWORKER_ABORTED;
		ereport(DEBUG1,
				(errmsg("background worker \"datachecksumsworker\" aborted in database oid %d",
						dboid)));
		return;
	}

	/*
	 * Wait for all temp tables that existed when we started to go away. This
	 * is necessary since we cannot "reach" them to enable checksums. Any temp
	 * tables created after we started will already have checksums in them
	 * (due to the inprogress state), so no need to wait for those.
	 */
	while (!aborted)
	{
		List	   *CurrentTempTables;
		ListCell   *lc;
		int			numleft;
		char		activity[64];
		int			rc;

		CurrentTempTables = BuildTempTableList();
		numleft = 0;
		foreach(lc, InitialTempTableList)
		{
			if (list_member_oid(CurrentTempTables, lfirst_oid(lc)))
				numleft++;
		}
		list_free(CurrentTempTables);

		if (numleft == 0)
			break;

		/* At least one temp table left to wait for */
		snprintf(activity, sizeof(activity), "Waiting for %d temp tables to be removed", numleft);
		pgstat_report_activity(STATE_RUNNING, activity);

		/* Retry every 5 seconds */
		ResetLatch(MyLatch);
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   5000,
					   WAIT_EVENT_CHECKSUM_ENABLE_FINISHCONDITION);

		LWLockAcquire(DatachecksumsWorkerLock, LW_EXCLUSIVE);

		/*
		 * If the postmaster died we wont be able to enable checksums
		 * clusterwide so abort and hope to continue when restarted.
		 */
		if (rc & WL_POSTMASTER_DEATH)
			DatachecksumsWorkerShmem->abort = true;
		aborted = DatachecksumsWorkerShmem->abort;

		LWLockRelease(DatachecksumsWorkerLock);
	}

	list_free(InitialTempTableList);

	DatachecksumsWorkerShmem->success = DATACHECKSUMSWORKER_SUCCESSFUL;
	ereport(DEBUG1,
			(errmsg("background worker \"datachecksumsworker\" completed in database oid %d",
					dboid)));
}
