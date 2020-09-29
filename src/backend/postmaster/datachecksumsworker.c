/*-------------------------------------------------------------------------
 *
 * datachecksumsworker.c
 *	  Background worker for enabling or disabling data checksums online
 *
 * When enabling data checksums on a database at initdb time or with
 * pg_checksums, no extra process is required as each page is checksummed, and
 * verified, when accessed.  When enabling checksums on an already running
 * cluster, which does not run with checksums enabled, this worker will ensure
 * that all pages are checksummed before verification of the checksums is
 * turned on. In the case of disabling checksums, the state transition is
 * recorded in the catalog and control file, and no changes are performed
 * on the data pages or in the catalog.
 *
 * Checksums can be either enabled or disabled cluster-wide, with on/off being
 * the end state for data_checkums.
 *
 * Enabling checksums
 * ------------------
 * When enabling checkums in an online cluster, data_checksums will be set to
 * "inprogress-on" which signals that write operations MUST compute and write
 * the checksum on the data page, but during reading the checksum SHALL NOT be
 * verified. This ensures that all objects created during checksumming will
 * have checksums set, but no reads will fail due to incorrect checksum. The
 * DataChecksumsWorker will compile a list of databases which exist at the
 * start of checksumming, and all of these which haven't been dropped during
 * the processing MUST have been processed successfully in order for checksums
 * to be enabled. Any new relation created during processing will see the
 * in-progress state and will automatically be checksummed as well as have its
 * state recorded in the catalog to avoid the datachecksumsworker having to
 * process it when already checksummed.
 *
 * For each database, all relations which have storage are read and every data
 * page is marked dirty to force a write with the checksum. This will generate
 * a lot of WAL as the entire database is read and written. Once all datapages
 * in a relation have been written, pg_class.relhaschecksums is set to true to
 * indicate that the relation is done.
 *
 * If the processing is interrupted by a cluster restart, it will be restarted
 * from where it left off given that pg_class.relhaschecksums track state of
 * processed relations and the in-progress state will ensure all new writes
 * performed with checksums. Each database will be reprocessed, but relations
 * where pg_class.relhaschecksums is true are skipped.
 *
 * If data checksums are enabled, then disabled, and then re-enabled, every
 * relation's pg_class.relhaschecksums field will be reset to false before
 * entering the in-progress mode.
 *
 *
 * Disabling checksums
 * -------------------
 * When disabling checksums, data_checksums will be set to "inprogress-off"
 * which signals that checksums are written but no longer verified. This ensure
 * that backends which have yet to move from the "on" state will still be able
 * to process data checksum validation. During "inprogress-off", the catalog
 * state pg_class.relhaschecksums is cleared for all relations.
 *
 *
 * Synchronization and Correctness
 * -------------------------------
 * The processes involved in enabling, or disabling, data checksums in an
 * online cluster must be properly synchronized with the normal backends
 * serving concurrent queries to ensure correctness. Correctness is defined
 * as the following:
 *
 *		- Backends SHALL NOT violate local datachecksum state
 *		- Data checksums SHALL NOT be considered enabled cluster-wide until all
 *		  currently connected backends have the local state "enabled"
 *
 * There are two levels of synchronization required for enabling data checksums
 * in an online cluster: (i) changing state in the active backends ("on",
 * "off", "inprogress-on" and "inprogress-off"), and (ii) ensuring no
 * incompatible objects and processes are left in a database when workers end.
 * The former deals with cluster-wide agreement on data checksum state and the
 * latter with ensuring that any concurrent activity cannot break the data
 * checksum contract during processing.
 *
 * Synchronizing the state change is done with procsignal barriers, where the
 * backend updating the global state in the controlfile will wait for all other
 * backends to absorb the barrier before WAL logging. Barrier absorption will
 * happen during interrupt processing, which means that connected backends will
 * change state at different times.
 *
 *   When Enabling Data Checksums
 *	 ----------------------------
 *	 A process which fails to observe data checksums being enabled can induce
 *	 two types of errors: failing to write the checksum when modifying the page
 *	 and failing to validate the data checksum on the page when reading it.
 *
 *   When the DataChecksumsWorker has finished writing checksums on all pages
 *   and enable data checksums cluster-wide, there are three sets of backends:
 *
 *   Bg: Backend updating the global state and emitting the procsignalbarrier
 *   Bd: Backends on "off" state
 *   Be: Backends in "on" state
 *   Bi: Backends in "inprogress-on" state
 *
 *   Backends transition from the Bd state to Be like so: Bd -> Bi -> Be
 *
 *   Backends in Bi and Be will write checksums when modifying a page, but only
 *   backends in Be will verify the checksum during reading. The Bg backend is
 *   blocked waiting for all backends in Bi to process interrupts and move to
 *   Be. Any backend starting will observe the global state being "on" and will
 *   thus automatically belong to Be.  Checksums are enabled cluster-wide when
 *   Bi is an empty set. All sets are compatible while still operating based on
 *   their local state.
 *
 *	 When Disabling Data Checksums
 *	 -----------------------------
 *	 A process which fails to observe data checksums being disabled can induce
 *	 two types of errors: writing the checksum when modifying the page and
 *	 validating a data checksum which is no longer correct due to modifications
 *	 to the page.
 *
 *   Bg: Backend updating the global state and emitting the procsignalbarrier
 *   Bd: Backands in "off" state
 *   Be: Backends in "on" state
 *   Bi: Backends in "inprogress-off" state
 *
 *   Backends transition from the Be state to Bd like so: Be -> Bi -> Bd
 *
 *   The goal is to transition all backends to Bd making the others empty sets.
 *   Backends in Bi writes data checksums, but don't validate them, such that
 *   backends still in Be can continue to validate pages until the barrier has
 *   been absorbed such that they are in Bi. Once all backends are in Bi, the
 *   barrier to transition to "off" can be raised and all backends can safely
 *   stop writing data checksums as no backend is enforcing data checksum
 *   validation.
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
	 * Variables for the worker to signal the launcher, or subsequent workers
	 * in other databases. As there is only a single worker, and the launcher
	 * won't read these until the worker exits, they can be accessed without
	 * the need for a lock. If multiple workers are supported then this will
	 * have to be revisited.
	 */
	DatachecksumsWorkerResult success;
	bool		process_shared_catalogs;

	/*
	 * The below members are set when the launcher starts, and are only
	 * accessed read-only by the single worker. Thus, we can access these
	 * without a lock. If multiple workers, or dynamic cost parameters, are
	 * supported at some point then this would need to be revisited.
	 */
	int			cost_delay;
	int			cost_limit;
	DataChecksumOperation operation;
}			DatachecksumsWorkerShmemStruct;

/* Shared memory segment for datachecksumsworker */
static DatachecksumsWorkerShmemStruct * DatachecksumsWorkerShmem;

/* Bookkeeping for work to do */
typedef struct DatachecksumsWorkerDatabase
{
	Oid			dboid;
	char	   *dbname;
}			DatachecksumsWorkerDatabase;

typedef struct DatachecksumsWorkerResultEntry
{
	Oid			dboid;
	DatachecksumsWorkerResult result;
	int			retries;
}			DatachecksumsWorkerResultEntry;


/* Prototypes */
static List *BuildDatabaseList(void);
static List *BuildRelationList(bool temp_relations, bool include_shared);
static DatachecksumsWorkerResult ProcessDatabase(DatachecksumsWorkerDatabase * db);
static bool ProcessAllDatabases(bool already_connected);
static bool ProcessSingleRelationFork(Relation reln, ForkNumber forkNum, BufferAccessStrategy strategy);
static void launcher_cancel_handler(SIGNAL_ARGS);
static void SetRelHasChecksums(Oid relOid);
static void WaitForAllTransactionsToFinish(void);

/*
 * DataChecksumsWorkerStarted
 *			Informational function to query the state of the worker
 */
bool
DataChecksumsWorkerStarted(void)
{
	bool		started;

	LWLockAcquire(DatachecksumsWorkerLock, LW_EXCLUSIVE);
	started = DatachecksumsWorkerShmem->launcher_started && !DatachecksumsWorkerShmem->abort;
	LWLockRelease(DatachecksumsWorkerLock);

	return started;
}

/*
 * StartDataChecksumsWorkerLauncher
 * 		Main entry point for datachecksumsworker launcher process.
 */
void
StartDatachecksumsWorkerLauncher(DataChecksumOperation op,
								 int cost_delay, int cost_limit)
{
	BackgroundWorker bgw;
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

	/* Whether to enable or disable data checksums */
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
 *		Request shutdown of the datachecksumsworker
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
 *		Enable data checksums in a single relation/fork.
 *
 * Returns true if successful, and false if *aborted*. On error, an actual
 * error is raised in the lower levels.
 */
static bool
ProcessSingleRelationFork(Relation reln, ForkNumber forkNum, BufferAccessStrategy strategy)
{
	BlockNumber numblocks = RelationGetNumberOfBlocksInFork(reln, forkNum);
	BlockNumber blknum;
	char		activity[NAMEDATALEN * 2 + 128];
	char	   *relns;

	relns = get_namespace_name(RelationGetNamespace(reln));

	if (!relns)
		return false;

	/*
	 * We are looping over the blocks which existed at the time of process
	 * start, which is safe since new blocks are created with checksums set
	 * already due to the state being "inprogress-on".
	 */
	for (blknum = 0; blknum < numblocks; blknum++)
	{
		Buffer		buf = ReadBufferExtended(reln, forkNum, blknum, RBM_NORMAL, strategy);

		/*
		 * Report to pgstat every 100 blocks to keep from overwhelming the
		 * activity reporting with close to identical reports.
		 */
		if ((blknum % 100) == 0)
		{
			snprintf(activity, sizeof(activity) - 1, "processing: %s.%s (%s block %d/%d)",
					 relns, RelationGetRelationName(reln),
					 forkNames[forkNum], blknum, numblocks);
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
		 * off, and then turned on again. Iff wal_level is set to "minimal",
		 * this could be avoided iff the checksum is calculated to be correct.
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

	pfree(relns);
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
		 "adding data checksums to relation with OID %u",
		 relationId);

	rel = try_relation_open(relationId, AccessShareLock);
	if (rel == NULL)
	{
		/*
		 * Relation no longer exists. We don't consider this an error since
		 * there are no pages in it that need data checksums, and thus return
		 * true. The worker operates off a list of relations generated at the
		 * start of processing, so relations being dropped in the meantime is
		 * to be expected.
		 */
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
		 "data checksum processing done for relation with OID %u: %s",
		 relationId, (aborted ? "aborted" : "finished"));

	if (!aborted)
		SetRelHasChecksums(relationId);

	CommitTransactionCommand();

	pgstat_report_activity(STATE_IDLE, NULL);

	return !aborted;
}

/*
 * SetRelHasChecksums
 *
 * Sets the pg_class.relhaschecksums flag for the relation specified by relOid
 * to true. The corresponding function for clearing state is
 * ResetDataChecksumsStateInDatabase which operate on all relations in a
 * database.
 */
static void
SetRelHasChecksums(Oid relOid)
{
	Relation	rel;
	Form_pg_class pg_class_tuple;
	HeapTuple	tuple;

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
 *		Enable data checksums in a single database.
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
	else if (DatachecksumsWorkerShmem->operation == RESET_STATE ||
			 DatachecksumsWorkerShmem->operation == RESET_STATE_AND_ENABLE_CHECKSUMS)
		snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ResetDataChecksumsStateInDatabase");
	else
		elog(ERROR, "invalid datachecksumsworker operation requested: %d",
			 DatachecksumsWorkerShmem->operation);
	snprintf(bgw.bgw_name, BGW_MAXLEN, "datachecksumsworker worker");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "datachecksumsworker worker");
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_notify_pid = MyProcPid;
	bgw.bgw_main_arg = ObjectIdGetDatum(db->dboid);

	/*
	 * If there are no worker slots available, make sure we retry processing
	 * this database. This will make the datachecksumsworker move on to the
	 * next database and quite likely fail with the same problem. TODO: Maybe
	 * we need a backoff to avoid running through all the databases here in
	 * short order.
	 */
	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
	{
		ereport(WARNING,
				(errmsg("failed to start worker for enabling data checksums in database \"%s\", retrying",
						db->dbname),
				 errhint("The max_worker_processes setting might be too low.")));
		return DATACHECKSUMSWORKER_RETRYDB;
	}

	status = WaitForBackgroundWorkerStartup(bgw_handle, &pid);
	if (status == BGWH_STOPPED)
	{
		ereport(WARNING,
				(errmsg("could not start background worker for enabling data checksums in database \"%s\"",
						db->dbname),
				 errhint("More details on the error might be found in the server log.")));
		return DATACHECKSUMSWORKER_FAILED;
	}

	/*
	 * If the postmaster crashed we cannot end up with a processed database so
	 * we have no alternative other than exiting. When enabling checksums we
	 * won't at this time have changed the pg_control version to enabled so
	 * when the cluster comes back up processing will have to be resumed. When
	 * disabling, the pg_control version will be set to off before this so
	 * when the cluster comes up checksums will be off as expected. In the
	 * latter case we might have stale relhaschecksums flags in pg_class which
	 * need to be handled in some way. TODO
	 */
	if (status == BGWH_POSTMASTER_DIED)
		ereport(FATAL,
				(errmsg("cannot enable data checksums without the postmaster process"),
				 errhint("Restart the database and restart data checksum processing by calling pg_enable_data_checksums().")));

	Assert(status == BGWH_STARTED);
	ereport(DEBUG1,
			(errmsg("initiating data checksum processing in database \"%s\"",
					db->dbname)));

	snprintf(activity, sizeof(activity) - 1,
			 "Waiting for worker in database %s (pid %d)", db->dbname, pid);
	pgstat_report_activity(STATE_RUNNING, activity);

	status = WaitForBackgroundWorkerShutdown(bgw_handle);
	if (status == BGWH_POSTMASTER_DIED)
		ereport(FATAL,
				(errmsg("postmaster exited during data checksum processing in \"%s\"",
						db->dbname),
				 errhint("Restart the database and restart data checksum processing by calling pg_enable_data_checksums().")));

	if (DatachecksumsWorkerShmem->success == DATACHECKSUMSWORKER_ABORTED)
		ereport(LOG,
				(errmsg("data checksums processing was aborted in database \"%s\"",
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

/*
 * WaitForAllTransactionsToFinish
 *		Blocks awaiting all current transactions to finish
 *
 * Returns when all transactions which are active at the call of the function
 * have ended, or if the postmaster dies while waiting. If the postmaster dies
 * the abort flag will be set to indicate that the caller of this shouldn't
 * proceed.
 */
static void
WaitForAllTransactionsToFinish(void)
{
	TransactionId waitforxid;
	bool		aborted = false;

	LWLockAcquire(XidGenLock, LW_SHARED);
	waitforxid = XidFromFullTransactionId(ShmemVariableCache->nextXid);
	LWLockRelease(XidGenLock);

	while (!aborted)
	{
		TransactionId oldestxid = GetOldestActiveTransactionId();

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
			 * cluster-wide so abort and hope to continue when restarted.
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

/*
 * DatachecksumsWorkerLauncherMain
 *
 * Main function for launching dynamic background workers for processing data
 * checksums in databases. This function has the bgworker management, with
 * ProcessAllDatabases being responsible for looping over the databases and
 * initiating processing.
 */
void
DatachecksumsWorkerLauncherMain(Datum arg)
{
	bool		connected = false;

	on_shmem_exit(launcher_exit, 0);

	ereport(DEBUG1,
			(errmsg("background worker \"datachecksumsworker\" launcher started")));

	pqsignal(SIGTERM, die);
	pqsignal(SIGINT, launcher_cancel_handler);

	BackgroundWorkerUnblockSignals();

	MyBackendType = B_DATACHECKSUMSWORKER_LAUNCHER;
	init_ps_display(NULL);

	/*
	 * Reset catalog state for checksum tracking, either to ensure that it's
	 * cleared before enabling checksums or as part of disabling checksums.
	 */
	if (DatachecksumsWorkerShmem->operation == RESET_STATE ||
		DatachecksumsWorkerShmem->operation == RESET_STATE_AND_ENABLE_CHECKSUMS)
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

		/*
		 * If checksums should be enabled as the next step, transition to the
		 * ENABLE_CHECKSUMS state to keep processing in the next stage.
		 */
		if (DatachecksumsWorkerShmem->operation == RESET_STATE_AND_ENABLE_CHECKSUMS)
		{
			SetDataChecksumsOnInProgress();

			LWLockAcquire(DatachecksumsWorkerLock, LW_EXCLUSIVE);
			DatachecksumsWorkerShmem->operation = ENABLE_CHECKSUMS;
			LWLockRelease(DatachecksumsWorkerLock);
		}
	}

	/*
	 * Prepare for datachecksumsworker shutdown, once we signal that checksums
	 * are enabled we want the worker to be done and exited to avoid races
	 * with immediate disabling/enabling.
	 */
	LWLockAcquire(DatachecksumsWorkerLock, LW_EXCLUSIVE);
	DatachecksumsWorkerShmem->abort = false;
	DatachecksumsWorkerShmem->launcher_started = false;
	LWLockRelease(DatachecksumsWorkerLock);

	if (DatachecksumsWorkerShmem->operation == ENABLE_CHECKSUMS)
	{
		/*
		 * If processing succeeds for ENABLE_CHECKSUMS, then everything has been
		 * processed so set checksums as enabled cluster-wide
		 */
		if (ProcessAllDatabases(connected))
		{
			SetDataChecksumsOn();
			ereport(LOG,
					(errmsg("checksums enabled cluster-wide")));
		}
	}
}

/*
 * ProcessAllDatabases
 *		Compute the list of all databases and process checksums in each
 *
 * This will repeatedly generate a list of databases to process for either
 * enabling checksums or resetting the checksum catalog tracking. Until no
 * new databases are found, this will loop around computing a new list and
 * comparing it to the already seen ones.
 */
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
			bool		found;

			elog(DEBUG1,
				 "starting processing of database %s with oid %u",
				 db->dbname, db->dboid);

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
			{
				/* Abort flag set, so exit the whole process */
				return false;
			}

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
			 "%i databases processed for data checksum enabling, %s",
			 processed_databases,
			 (processed_databases ? "process with restart" : "process completed"));

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
		 * still exists.
		 */
		if (found && *entry == DATACHECKSUMSWORKER_FAILED)
		{
			ereport(WARNING,
					(errmsg("failed to enable data checksums in \"%s\"",
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
		 * Even if this is a redundant assignment, we want to be explicit
		 * about our intent for readability, since we want to be able to query
		 * this state in case of restartability.
		 */
		DatachecksumsWorkerShmem->launcher_started = false;
	}
}

/*
 * BuildDatabaseList
 *		Compile a list of all currently available databases in the cluster
 *
 * This creates the list of databases for the datachecksumsworker workers to
 * add checksums to.
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
 *		Compile a list of relations in the database
 *
 * Returns a list of OIDs for the request relation types. If temp_relations
 * is True then only temporary relations are returned. If temp_relations is
 * False then non-temporary relations which have data checksums are returned.
 * If include_shared is True then shared relations are included as well in a
 * non-temporary list. include_shared has no relevance when bulding a list of
 * temporary relations.
 */
static List *
BuildRelationList(bool temp_relations, bool include_shared)
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

		/*
		 * Only include temporary relations when asked for a temp relation
		 * list.
		 */
		if (pgc->relpersistence == RELPERSISTENCE_TEMP)
		{
			if (!temp_relations)
				continue;
		}
		else
		{
			if (!RELKIND_HAS_STORAGE(pgc->relkind))
				continue;

			if (pgc->relhaschecksums)
				continue;

			if (pgc->relisshared && !include_shared)
				continue;
		}

		oldctx = MemoryContextSwitchTo(ctx);
		RelationList = lappend_oid(RelationList, pgc->oid);
		MemoryContextSwitchTo(oldctx);
	}

	table_endscan(scan);
	table_close(rel, AccessShareLock);

	CommitTransactionCommand();

	return RelationList;
}

/*
 * ResetDataChecksumsStateInDatabase
 *		Main worker function for clearing checksums state in the catalog
 *
 * Resets the pg_class.relhaschecksums flag to false for all entries in the
 * current database. This is required to be performed before adding checksums
 * to a running cluster in order to track the state of the processing.
 */
void
ResetDataChecksumsStateInDatabase(Datum arg)
{
	Relation	rel;
	HeapTuple	tuple;
	Oid			dboid = DatumGetObjectId(arg);
	TableScanDesc scan;
	Form_pg_class pgc;

	pqsignal(SIGTERM, die);

	BackgroundWorkerUnblockSignals();

	MyBackendType = B_DATACHECKSUMSWORKER_WORKER;
	init_ps_display(NULL);

	ereport(DEBUG1,
			(errmsg("resetting catalog state for data checksums in database with OID %u",
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
}

/*
 * DatachecksumsWorkerMain
 *
 * Main function for enabling checksums in a single database, This is the
 * function set as the bgw_function_name in the dynamic background worker
 * process initated for each database by the worker launcher. After enabling
 * data checksums in each applicable relation in the database, it will wait for
 * all temporary relations that were present when the function started to
 * disappear before returning. This is required since we cannot rewrite
 * existing temporary relations with data checksums.
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
			(errmsg("starting data checksum processing in database with OID %u",
					dboid)));

	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid,
											  BGWORKER_BYPASS_ALLOWCONN);

	/*
	 * Get a list of all temp tables present as we start in this database. We
	 * need to wait until they are all gone until we are done, since we cannot
	 * access these relations and modify them.
	 */
	InitialTempTableList = BuildRelationList(true, false);

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

	RelationList = BuildRelationList(false,
									 DatachecksumsWorkerShmem->process_shared_catalogs);
	foreach(lc, RelationList)
	{
		Oid reloid = lfirst_oid(lc);

		if (!ProcessSingleRelationByOid(reloid, strategy))
		{
			aborted = true;
			break;
		}
	}
	list_free(RelationList);

	if (aborted)
	{
		DatachecksumsWorkerShmem->success = DATACHECKSUMSWORKER_ABORTED;
		ereport(DEBUG1,
				(errmsg("data checksum processing aborted in database OID %u",
						dboid)));
		return;
	}

	/*
	 * Wait for all temp tables that existed when we started to go away. This
	 * is necessary since we cannot "reach" them to enable checksums. Any temp
	 * tables created after we started will already have checksums in them
	 * (due to the "inprogress-on" state), so no need to wait for those.
	 */
	while (!aborted)
	{
		List	   *CurrentTempTables;
		ListCell   *lc;
		int			numleft;
		char		activity[64];
		int			rc;

		CurrentTempTables = BuildRelationList(true, false);
		numleft = 0;
		foreach(lc, InitialTempTableList)
		{
			if (list_member_oid(CurrentTempTables, lfirst_oid(lc)))
				numleft++;
		}
		list_free(CurrentTempTables);

		if (numleft == 0)
			break;

		/* At least one temp table is left to wait for */
		snprintf(activity,
				 sizeof(activity),
				 "Waiting for %d temp tables to be removed", numleft);
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
		 * cluster-wide so abort and hope to continue when restarted.
		 */
		if (rc & WL_POSTMASTER_DEATH)
			DatachecksumsWorkerShmem->abort = true;
		aborted = DatachecksumsWorkerShmem->abort;

		LWLockRelease(DatachecksumsWorkerLock);
	}

	list_free(InitialTempTableList);

	DatachecksumsWorkerShmem->success = DATACHECKSUMSWORKER_SUCCESSFUL;
	ereport(DEBUG1,
			(errmsg("data checksum processing completed in database with OID %u",
					dboid)));
}
