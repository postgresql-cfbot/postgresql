/*-------------------------------------------------------------------------
 *
 * xlogrestore.c
 *	  Infrastructure for parallel restore commands execution
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/xlogrestore.c
 *
 *-------------------------------------------------------------------------
 */

#include "access/xlogrestore.h"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <unistd.h>
#include <sys/stat.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogarchive.h"
#include "access/xlogdefs.h"
#include "access/xlogutils.h"
#include "common/archive.h"
#include "common/file_perm.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port.h"
#include "port/atomics.h"
#include "postmaster/bgworker.h"
#include "postmaster/startup.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "storage/shmem.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "tcop/tcopprot.h"
#include "utils/timestamp.h"
#include "utils/memutils.h"

/*
 * The max number of WAL files to prefetch from archive.
 */
int			wal_max_prefetch_amount;

/*
 * Number of background workers to run on postmaster startup for retrieving
 * WAL files from archive. Zero value of this variable turns off prefetching of
 * WAL files from archive.
 */
int			wal_prefetch_workers;

/*
 * Data for restore_command bgworker.
 */
typedef struct RestoreSlot
{
	/*
	 * The handle corresponding to a running bgworker process.
	 */
	BackgroundWorkerHandle *bgwhandle;

	/*
	 * The latch used for signaling that bgworker can continue downloading of
	 * WAL files since a number of already pre-fetched	WAL files dropped below
	 * the limit imposed by the GUC parameter wal_max_prefetch_amount.
	 */
	Latch		continuePrefetching;

	/*
	 * The latch to notify an invoker that a bgworker process has been
	 * successfully run.
	 */
	Latch		workerReady;

	/*
	 * This flag is set by bgworker process if it was started and run
	 * successfully.
	 */
	bool		workerStarted;
} RestoreSlot;

typedef struct PrefetchedFile
{
	/*
	 * The name of the archive file %f.
	 */
	char		xlogfname[MAXFNAMELEN];
} PrefetchedFile;

/*
 * Type of values stored in hash table RestoreDataStruct->hashtab
 */
typedef struct PrefetchedFileEntry
{
	PrefetchedFile key;

	/*
	 * True if a file with a name equals to the key does exist on a file
	 * system, else false.
	 */
	bool		file_exist;

	/*
	 * True if a file with a name equals to the key has been already processed
	 * during recovery procedure.
	 */
	bool		file_was_processed;
} PrefetchedFileEntry;

typedef struct RestoreDataStruct
{
	/*
	 * The lock to guard against concurrent modification of structure's
	 * members from parallel running threads.
	 */
	slock_t		lock;

	/*
	 * The latch to support for producer/consumer pattern. Producers are
	 * bgworker processes pre-fetching WAL files from archive, Consumer is a
	 * recovery process who waiting until the next required WAL file be
	 * downloaded from archive to continue database recovering. This latch is
	 * used for notifying the consumer that a new file was retrieved by one of
	 * running producers.
	 */
	Latch		fileAvailable;

	/*
	 * Hash table to trace what WAL files have been pre-fetched.
	 */
	HTAB	   *hashtab;

	/*
	 * The name of the last recovery point file %r.
	 */
	char		pointfname[MAXFNAMELEN];

	/*
	 * TLI and an initial segment number from which to start a database
	 * recovery
	 */
	XLogSegNo	restartSegNo;
	TimeLineID	restartTli;

	/*
	 * Number of pre-fetched WAL files.
	 */
	int			nprefetched;

	/*
	 * Data for background workers.
	 */
	RestoreSlot slots[FLEXIBLE_ARRAY_MEMBER];

} RestoreDataStruct;

static RestoreDataStruct *RestoreData = NULL;

typedef enum WALPrefetchingState_e
{
	WALPrefetchingIsInactive,
	WALPrefetchingIsActive,
	WALPrefetchingShutdown
} WALPrefetchingState_e;

static WALPrefetchingState_e WALPrefetchingState = WALPrefetchingIsInactive;

static void XLogFilePathPrefetch(char *path, const char *xlogfname);
static bool FilePathExists(const char *xlogpath);
static void StartWALPrefetchWorkers(const char *xlogfname);
static void ShutdownWALPrefetchWorkers(int last_process_idx);
static bool WaitUntilFileRetrieved(const char *xlogfname,
								   bool *wal_file_processed);

/*
 * Calculate a size of shared memory used for storing bgworker slots.
 */
Size
RestoreCommandShmemSize(void)
{
	Size		size;

	size = sizeof(RestoreDataStruct);
	size = MAXALIGN(size);
	size = add_size(size, mul_size(wal_prefetch_workers, sizeof(RestoreSlot)));
	return size;
}

#define PREFETCH_DIR XLOGDIR "/" PG_TEMP_FILES_DIR

/*
 * Create a temporary directory to store prepfetched files
 * and initialize a shared memory used for storing bgworker slots.
 */
void
RestoreCommandShmemInit(void)
{
	bool		found;

	RestoreData = (RestoreDataStruct *)
		ShmemInitStruct("Restore Command Workers Data",
						RestoreCommandShmemSize(),
						&found);

	if (!found)
	{
		int			i;
		HASHCTL		hash_ctl;

		memset(RestoreData, 0, RestoreCommandShmemSize());

		SpinLockInit(&RestoreData->lock);

		InitSharedLatch(&RestoreData->fileAvailable);

		/* Create the hash table */
		memset(&hash_ctl, 0, sizeof(hash_ctl));

		hash_ctl.keysize = sizeof(PrefetchedFile);
		hash_ctl.entrysize = sizeof(PrefetchedFileEntry);

		RestoreData->hashtab = ShmemInitHash("Pre-fetched WAL files",
											 wal_max_prefetch_amount,
											 wal_max_prefetch_amount,
											 &hash_ctl,
											 HASH_ELEM);

		/*
		 * Initialize memory for each worker slot.
		 */
		for (i = 0; i < wal_prefetch_workers; ++i)
		{
			RestoreSlot *slot = &RestoreData->slots[i];

			memset(slot, 0, sizeof(RestoreSlot));
			InitSharedLatch(&slot->continuePrefetching);
			InitSharedLatch(&slot->workerReady);
		}

		/* Create or clear temporary wals. */
		PathNameCreateTemporaryDir(XLOGDIR, PREFETCH_DIR);
		RemovePgTempFilesInDir(PREFETCH_DIR, true, true);
	}
}

/*
 * Iterate along bgworkers slots and notify everyone bgworker process
 * waiting on the continuePrefetching Latch to resume retrieving of WAL files
 * from archive.
 */
static void
ResumePrefetching()
{
	unsigned	i;

	for (i = 0; i < wal_prefetch_workers; ++i)
	{
		SetLatch(&RestoreData->slots[i].continuePrefetching);
	}
}

/*
 * This function is counterpart of RestoreArchivedFile function with xlogs
 * pre-fetching.
 *
 * On success the requested WAL file has been retrieved from archive.
 * Invocation of this function also initiates loading of WAL files that
 * will be required later. For this goal several brworker processes
 * are started and perform loading of WAL files. A name of file to start
 * loading is assigned to every background worker process together with
 * a delta value that will be applied to a segment number of a WAL file just
 * received in order to calculate a next file name to pre-load.
 *
 * A number of background workers started for WAL files loading is determined
 * by the new GUC parameter wal_prefetch_workers. A number of WAL files to
 * prefetch is limited by the new GUC parameter wal_max_prefetch_amount.
 * If wal_max_prefetch_amount has value 0 no background worker processes
 * are started and WAL files preloading is not performed. In this case regular
 * (in one by one manner) loading of WAL files is performed.
 *
 * Input:
 *		path - the path to a WAL file retrieved from archive
 *		xlogfname - a name of WAL file to retrieve from archive
 *		recovername - the directory name where a retrieved WAL file
 *					  has to be placed
 *		expectedSize - expected size of the requested WAL file
 *		cleanupEnabled - true if start recovering from last restart point,
 *						 false if start recovering from the very outset.
 *	Return:
 *		true on success, false on error
 */
bool
RestoreCommandXLog(char *path, const char *xlogfname, const char *recovername,
				   const off_t expectedSize, bool cleanupEnabled)
{
	char		xlogpath[MAXPGPATH];
	bool		prefetchedFileNotFound,
				wal_file_already_processed;
	int			nprefetched;
	PrefetchedFileEntry *foundEntry;

	/*
	 * Synchronous mode.
	 */
	if (wal_max_prefetch_amount < 1)
		goto fallback;

	/*
	 * Ignore restore_command when not in archive recovery (meaning we are in
	 * crash recovery).
	 */
	if (!ArchiveRecoveryRequested)
		goto fallback;

	/*
	 * In standby mode, restore_command might not be supplied.
	 */
	if (recoveryRestoreCommand == NULL ||
		strcmp(recoveryRestoreCommand, "") == 0)
		goto fallback;

	/*
	 * Create the last restart point file name.
	 */
	XLogFileNameLastPoint(RestoreData->pointfname, cleanupEnabled);

	/*
	 * Run WAL pre-fetching processes if they haven't been started yet.
	 */
	StartWALPrefetchWorkers(xlogfname);

	/*
	 * We shouldn't need anything earlier than the last restart point.
	 */
	Assert(strcmp(RestoreData->pointfname, xlogfname) <= 0);

	/*
	 * Make prefetched path for file.
	 */
	XLogFilePathPrefetch(xlogpath, xlogfname);

	/*
	 * Wait until file be retrieved from archive.
	 */
	if (!WaitUntilFileRetrieved(xlogfname, &wal_file_already_processed))
	{
		/*
		 * WaitUntilFileRetrieved() returns false in case there is no more WAL
		 * files to retrieve.
		 */
		snprintf(path, MAXPGPATH, XLOGDIR "/%s", xlogfname);
		return false;
	}

	if (wal_file_already_processed)
		return false;

	/*
	 * Make sure the file is really there now and has the correct size.
	 */
	if (!FileValidateSize(xlogpath, expectedSize, xlogfname,
						  &prefetchedFileNotFound))
	{
		if (prefetchedFileNotFound)
			snprintf(path, MAXPGPATH, XLOGDIR "/%s", xlogfname);
		else
			/* Remove artifacts. */
			FileUnlink(xlogpath);

		return false;
	}

	/*
	 * Move file to target path.
	 */
	snprintf(path, MAXPGPATH, XLOGDIR "/%s", recovername);
	durable_rename(xlogpath, path, ERROR);

	/*
	 * Decrease by one a number of prefetched files and wake up any of
	 * pre-fetching processes suspended on this latch.
	 */
	SpinLockAcquire(&RestoreData->lock);

	Assert(RestoreData->nprefetched > 0);

	nprefetched = RestoreData->nprefetched;
	RestoreData->nprefetched = RestoreData->nprefetched - 1;

	/*
	 * Check whether a number of already prefetched files greater or equal the
	 * limit wal_max_prefetch_amount and whether this number dropped below the
	 * limit after its decrement.
	 */
	if (nprefetched >= wal_max_prefetch_amount &&
		RestoreData->nprefetched < wal_max_prefetch_amount)

		/*
		 * The value of RestoreData->nprefetched dropped below the
		 * wal_max_prefetch_amount limit, signal background processes to
		 * continue prefetching of WAL files from archive.
		 */
		ResumePrefetching();

	foundEntry =
	(PrefetchedFileEntry *) hash_search(RestoreData->hashtab, xlogfname,
										HASH_FIND, NULL);

	foundEntry->file_was_processed = true;
	SpinLockRelease(&RestoreData->lock);

	/*
	 * Log message like in RestoreArchivedFile.
	 */
	ereport(LOG,
			(errmsg("restored log file \"%s\" from archive",
					xlogfname)));
	return true;

fallback:

	/*
	 * On any errors - try default implementation
	 */
	return RestoreArchivedFile(path, xlogfname, recovername, expectedSize,
							   cleanupEnabled);
}

/*
 * Waiting until a file with the name specified by the parameter xlogfname
 * be received from archive and written to file system.
 *
 * Input:
 *		xlogfname - a name of file to wait for delivering from archive
 * Return:
 *		false in case there is no more file in archive to retrieve, else true
 */
static bool
WaitUntilFileRetrieved(const char *xlogfname, bool *wal_file_processed)
{
	bool		found;

	do
	{
		PrefetchedFileEntry *foundEntry;

		SpinLockAcquire(&RestoreData->lock);

		/*
		 * Check whether the file name does exist in the hash table. If it
		 * does then restore_command was executed on behalf of this file name
		 * and a file was probably copied to a destination directory. The
		 * actual presence of the file in the destination directory is
		 * determined by the the data member file_exist of the structure
		 * PrefetchedFileEntry.
		 */
		foundEntry =
			(PrefetchedFileEntry *) hash_search(RestoreData->hashtab, xlogfname,
												HASH_FIND, NULL);

		if (foundEntry != NULL)
		{
			/*
			 * The data member file_exist of the structure PrefetchedFileEntry
			 * has the false value if restore_command was executed but the
			 * file wasn't copied to a destination directory by some reason,
			 * e.g. since no more file exist in archive.
			 */
			found = foundEntry->file_exist;
			*wal_file_processed = foundEntry->file_was_processed;

			SpinLockRelease(&RestoreData->lock);
			break;
		}
		SpinLockRelease(&RestoreData->lock);

		/*
		 * There is no an entry in hash table corresponding to a name
		 * specified by the parameter xlogfname. Wait on the latch
		 * RestoreData->fileAvailable located in the shared memory until a
		 * file be retrieved from archive. bgworker processes run for
		 * delivering WAL files from archive will trigger this latch every
		 * time a new WAL file be delivered.
		 */
		(void) WaitLatch(&RestoreData->fileAvailable,
						 WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
						 0, PG_WAIT_EXTENSION);
		ResetLatch(&RestoreData->fileAvailable);
		CHECK_FOR_INTERRUPTS();
	}
	while (true);

	return found;
}

/*
 * Insert a file name into the hash table and wake up a thread that is waiting
 * until file retrieved from archive.
 *
 * Input:
 *		xlogfname - name of pre-fetched file.
 */
static void
SignalFileDelivered(const char *xlogfname)
{
	PrefetchedFileEntry newFileEntry;
	PrefetchedFileEntry *insertedElement;

#ifdef USE_ASSERT_CHECKING
	bool		found = false;
#endif

	strcpy(newFileEntry.key.xlogfname, xlogfname);

	SpinLockAcquire(&RestoreData->lock);

	/*
	 * Add the new file name to the hash of file names that have been already
	 * delivered from archive. Out of memory error is reported by ereport, so
	 * it is not required to check the return value of hash_search().
	 */
#ifdef USE_ASSERT_CHECKING
	insertedElement = hash_search(RestoreData->hashtab, &newFileEntry,
								  HASH_ENTER, &found);

	/*
	 * An entry with such a key mustn't exist in the hash table at the tim of
	 * insertion. If it does something really wrong happened.
	 */
	Assert(!found);
#else
	insertedElement = hash_search(RestoreData->hashtab, &newFileEntry,
								  HASH_ENTER, NULL);
#endif

	insertedElement->file_exist = true;
	insertedElement->file_was_processed = false;

	/*
	 * Increase by one a number of pre-fetched files
	 */
	RestoreData->nprefetched = RestoreData->nprefetched + 1;

	/*
	 * Wake up the thread that executing RestoreCommandXLog() to continue
	 * processing of WAL files.
	 */
	SetLatch(&RestoreData->fileAvailable);

	SpinLockRelease(&RestoreData->lock);
}

/*
 * Mark that the specified WAL file doesn't exist in archive and wake up
 * a thread that is waiting in RestoreCommandXLog() to finish WAL file
 * processing.
 */
static void
SignalFileNotExist(const char *xlogfname)
{
	PrefetchedFileEntry *insertedElement;
	PrefetchedFileEntry newFileEntry;
#ifdef USE_ASSERT_CHECKING
	bool		found = false;
#endif

	strcpy(newFileEntry.key.xlogfname, xlogfname);

	SpinLockAcquire(&RestoreData->lock);

	/*
	 * Add the new file name to the hash of file names that have been already
	 * delivered from archive. Out of memory error is reported by ereport, so
	 * it is not required to check the return value of hash_search().
	 */
#ifdef USE_ASSERT_CHECKING
	insertedElement = hash_search(RestoreData->hashtab, &newFileEntry,
								  HASH_ENTER, &found);

	/*
	 * We tried to add the new file name and discovered that such file does
	 * already exist. It seems something wrong happens.
	 */
	Assert(!found);
	insertedElement->file_exist = false;
	insertedElement->file_was_processed = false;

	Assert(insertedElement->file_exist == false &&
		   strcmp(insertedElement->key.xlogfname, xlogfname) == 0);
#else
	insertedElement = hash_search(RestoreData->hashtab, &newFileEntry,
								  HASH_ENTER, NULL);
	insertedElement->file_exist = false;
	insertedElement->file_was_processed = false;
#endif

	/*
	 * Wake up the thread executing RestoreCommandXLog() to finish WAL files
	 * processing since no more files left in archive.
	 */
	SetLatch(&RestoreData->fileAvailable);

	SpinLockRelease(&RestoreData->lock);
}

/*
 * Check whether a limit imposed by the GUC parameter wal_max_prefetch_amount
 * has been exceeded on prefetching of WAL files and suspend further
 * downloading of WAL files until a notification be received to resume it.
 * Input:
 *			bgwid - an index of bgworker process that has to suspend prefetching of
 *			WAL files.
 */
static void
SuspendPrefetchingIfRequired(uint16 bgwid)
{
	while (RestoreData->nprefetched >= wal_max_prefetch_amount)
	{
		/*
		 * If a number of already pre-fetched WAL files exceeds a limit
		 * imposed by the GUC parameter 'wal_max_prefetch_amount', suspend
		 * execution until some of already retrieved WAL files be processed
		 * and a number of pre-fetched files dropped below this limit.
		 */
		(void) WaitLatch(&RestoreData->slots[bgwid].continuePrefetching,
						 WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
						 0, PG_WAIT_EXTENSION);
		ResetLatch(&RestoreData->slots[bgwid].continuePrefetching);
		CHECK_FOR_INTERRUPTS();
	}
}

/*
 * The main entry point for bgworker.
 * Input:
 *		main_arg -	id value assigned to each pre-fetching bgworker process.
 *				This value is used both as an index in array of active bgworker
 *				processes and for calculating the number of the first segment
 *				from that to start WAL files pre-fetching by corresponding
 *				bgworker process.
 *
 * This function returns the control flow if a file that currently
 * being processed is not found, meaning that all files were already delivered
 * from archive and the requested file is one that was never stored
 * in the archive.
 */
void
WALPrefetchWorkerMain(Datum main_arg)
{
	int			rc;
	char		xlogpath[MAXPGPATH];
	char		xlogfnext[MAXFNAMELEN];
	unsigned	increment;
	XLogSegNo	nextSegNo;
	uint16		bgwid;

	/* Establish signal handlers. */
	pqsignal(SIGTERM, die);
	/* We're now ready to receive signals. */
	BackgroundWorkerUnblockSignals();

	/* Get RestoreSlot */
	bgwid = DatumGetUInt16(main_arg);

	nextSegNo = RestoreData->restartSegNo + bgwid;
	increment = wal_prefetch_workers;

	OwnLatch(&RestoreData->slots[bgwid].continuePrefetching);

	/*
	 * Notify invoker that the WAL files prefetching worker has just been
	 * successfully started.
	 */
	RestoreData->slots[bgwid].workerStarted = true;
	SetLatch(&RestoreData->slots[bgwid].workerReady);

	while (true)
	{
		SuspendPrefetchingIfRequired(bgwid);

		XLogFileName(xlogfnext, RestoreData->restartTli, nextSegNo,
					 wal_segment_size);

		/* Prepare path. */
		XLogFilePathPrefetch(xlogpath, xlogfnext);

		/*
		 * Make sure there is no such file in a directory for prefetched
		 * files.
		 */
		FileUnlink(xlogpath);

		/* Prepare and execute the restore command. */
		if ((rc = DoRestore(xlogpath, xlogfnext, RestoreData->pointfname)))
		{
			FileUnlink(xlogpath);

			if (wait_result_is_any_signal(rc, true))
				proc_exit(1);

			if (!FilePathExists(xlogpath))
				SignalFileNotExist(xlogfnext);
			else

				/*
				 * Although execution of external program specified by the GUC
				 * parameter 'restore_command' can failed since there is no
				 * such file in archive, a file with this name can exist in
				 * prefetch directory since it left from the last server start
				 * up. If it's true put the file name into the hash table and
				 * wake up the thread that is waiting in RestoreCommandXLog()
				 * to continue WAL file processing.
				 */
				SignalFileDelivered(xlogfnext);

			ereport(INFO,
					(errmsg("could not restore file \"%s\" from archive: %s",
							xlogfnext, wait_result_to_str(rc))));

			break;
		}
		CHECK_FOR_INTERRUPTS();

		/*
		 * Check that file has been really written to file system and if it
		 * does then wake up the thread that is waiting in
		 * RestoreCommandXLog() to continue WAL file processing.
		 */
		if (FilePathExists(xlogpath))
		{
			SignalFileDelivered(xlogfnext);

			ereport(INFO, errmsg("The file %s was retrieved to \"%s\"",
								 xlogfnext, xlogpath));
		}
		else
		{
			/*
			 * DoRestore() finished with success that means invocation of
			 * system() API function completed without error. On the other
			 * hand, the requested file is not found. That means something
			 * wrong happened with script run by the API function system(),
			 * e.g. the script doens't do really something useful, or may be
			 * it put a file to a wrong destination. Anyway, it is time to
			 * exit and give a chance to system administrator to fix the
			 * issue.
			 */
			SignalFileNotExist(xlogfnext);

			ereport(INFO, errmsg("The file %s is not found", xlogfnext));
			break;
		}

		nextSegNo = nextSegNo + increment;
	}
	proc_exit(0);
}

/*
 * Setup and spawn bgworker to prefetch WAL files from archive.
 *
 * Input:
 *		bgwid - sequence number of bgworker process we are going to spawn
 *
 * Returns true on success and false on failure.
 */
static bool
SpawnWALPrefetchWorker(uint16 bgwid)
{
	BackgroundWorker bgw;
	RestoreSlot *slot = &RestoreData->slots[bgwid];

	memset(&bgw, 0, sizeof(bgw));
	snprintf(bgw.bgw_name, sizeof(bgw.bgw_name), "WAL prefetching worker #%d",
			 bgwid);

	/*
	 * Length of the string literal "Restore Command Worker" is less than size
	 * of a buffer referenced by the data member bgw.bgw_type (the size is
	 * limited by the constant BGW_MAXLEN that currently has value 96).
	 * Therefore we can use function strcpy() instead of strncpy/strlcpy to
	 * copy the string literal into the buffer bgw.bgw_type. The same is true
	 * for other two string literals "postgres" and "RestoreCommandWorkerMain"
	 * and their corresponding destination buffers referenced by the data
	 * members bgw.bgw_library_name, bgw.bgw_function_name. To guards against
	 * further possible change of limit represented by the constant BGW_MAXLEN
	 * the asserts have been inserted before invoking of the function strcpy()
	 * as a sanity check. In case some of these asserts be fired it means that
	 * some really drastic change was done in the core source code that should
	 * be carefully studied.
	 */
	Assert(sizeof(bgw.bgw_type) >= sizeof("WAL files pre-fetching Worker"));
	Assert(sizeof(bgw.bgw_library_name) >= sizeof("postgres"));
	Assert(sizeof(bgw.bgw_function_name) >= sizeof("WALPrefetchWorkerMain"));

	strcpy(bgw.bgw_type, "WAL files pre-fetching Worker");
	strcpy(bgw.bgw_library_name, "postgres");
	strcpy(bgw.bgw_function_name, "WALPrefetchWorkerMain");

	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;

	/*
	 * BgWorkerStart_PostmasterStart for PM_RECOVERY, PM_STARTUP
	 * BgWorkerStart_ConsistentState for PM_HOT_STANDBY
	 */
	bgw.bgw_start_time = HotStandbyActive() ? BgWorkerStart_ConsistentState :
		BgWorkerStart_PostmasterStart;

	bgw.bgw_restart_time = BGW_NEVER_RESTART;

	/*
	 * The value of bgw.bgw_main_arg is passed as an argument to the function
	 * WALPrefetchWorkerMain()
	 */
	bgw.bgw_main_arg = UInt16GetDatum(bgwid);
	bgw.bgw_notify_pid = MyProcPid;

	return RegisterDynamicBackgroundWorker(&bgw, &slot->bgwhandle);
}

/*
 * Terminate bgworker process whose slot addressed by specified index and
 * free memory allocated for the slot.
 *
 * Input:
 *		slot_idx -	index of a slot in the array RestoreData->slot[] that
 *					contains data about bgworker process.
 */
static void
ShutdownWALPrefetchWorker(uint16 slot_idx)
{
	RestoreSlot *slot = &RestoreData->slots[slot_idx];

	if (slot->bgwhandle != NULL)
	{
		TerminateBackgroundWorker(slot->bgwhandle);
		pfree(slot->bgwhandle);
		slot->bgwhandle = NULL;
	}
}

/*
 * Stop every WAL prefetching process started from the last spawned one
 * specified by the parameter failed_process_idx. This function is called
 * either on postmaster shutdown or on postmaster starting up in case some of
 * WAL prefetching workers failed to start.
 *
 * Input:
 *		failed_process_idx - sequence number (starting from 0) of a bgworker
 *							 process that failed to start.
 */
static void
ShutdownWALPrefetchWorkers(int last_process_idx)
{
	while (last_process_idx > 0)
		ShutdownWALPrefetchWorker(--last_process_idx);

	WALPrefetchingState = WALPrefetchingShutdown;
}

/*
 * Start bgworker processes for retrieving WAL files from archive in
 * pre-fetching mode. Wait until all spawned processes be run. A number of
 * bgworker processes to spawn is determined by the GUC parameter
 * wal_prefetch_workers.
 *
 * Input:
 *		xlogfname - the name of a WAL file from which to start recovery
 *
 * Throw error if any of WAL files pre-fetching workers fail to start.
 */
static void
StartWALPrefetchWorkers(const char *xlogfname)
{
	int			i;

	if (WALPrefetchingState != WALPrefetchingIsInactive)
		return;

	XLogFromFileName(xlogfname, &RestoreData->restartTli,
					 &RestoreData->restartSegNo, wal_segment_size);

	for (i = 0; i < wal_prefetch_workers; ++i)
		OwnLatch(&RestoreData->slots[i].workerReady);

	OwnLatch(&RestoreData->fileAvailable);

	for (i = 0; i < wal_prefetch_workers; ++i)
	{
		if (!SpawnWALPrefetchWorker(i))
		{
			ShutdownWALPrefetchWorkers(i);
			ereport(FATAL,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("could not run background process for WAL files "
							"pre-fetching")));
		}

	}

	/*
	 * Wait until all spawned workers will be successfully started.
	 */
	for (i = 0; i < wal_prefetch_workers; ++i)
	{
		while (!RestoreData->slots[i].workerStarted)
		{
			(void) WaitLatch(&RestoreData->slots[i].workerReady,
							 WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
							 0, PG_WAIT_EXTENSION);
			ResetLatch(&RestoreData->slots[i].workerReady);
		}
	}

	WALPrefetchingState = WALPrefetchingIsActive;
}

/*
 * Get a path to the WAL file pre-fetched from archive.
 */
static void
XLogFilePathPrefetch(char *path, const char *xlogfname)
{
	snprintf(path, MAXPGPATH, PREFETCH_DIR "/%s", xlogfname);
}

/*
 * Check that the path does exist.
 * Return:
 *		true if file does exist, else false.
 * Throw error on failure.
 */
static bool
FilePathExists(const char *xlogpath)
{
	struct stat statbuf;

	if (stat(xlogpath, &statbuf) == 0)
		return true;

	if (errno != ENOENT)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m",
						xlogpath)));

	return false;
}
