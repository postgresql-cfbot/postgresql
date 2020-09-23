/*-------------------------------------------------------------------------
 *
 * xlogrestore.c
 *	  Infrastructure for parallel executing restore commands
 *
 * Copyright (c) 2014-2020, PostgreSQL Global Development Group
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
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "tcop/tcopprot.h"
#include "utils/timestamp.h"
#include "utils/memutils.h"

#define PREFETCH_DIR XLOGDIR "/" PG_TEMP_FILES_DIR
#define PREFETCH_RATIO (16)

int			max_restore_command_workers;

/*
 * Data for restore_command bgworker.
 */
typedef struct RestoreSlot
{
	slock_t		spin;

	/*
	 * The name of the archive file %f.
	 */
	char		xlogfname[MAXFNAMELEN];

	/*
	 * The name of the last recovery point file %r.
	 */
	char		pointfname[MAXFNAMELEN];

	/*
	 * The restore_command was called.
	 */
	bool		bgwdone;

	/*
	 * restore_command exit code
	 */
	int			bgwrc;

	/*
	 * Used to check that this slot is still linked with the bgworker.
	 */
	uint64		bgwid;

	/*
	 * bgworker start time.
	 */
	TimestampTz bgwtime;

	/*
	 * The pointer is valid for the launcher only.
	 */
	BackgroundWorkerHandle *bgwhandle;
}			RestoreSlot;

typedef struct RestoreDataStruct
{
	/*
	 * Counter for bgworkers identification.
	 */
	uint64		counter;

	/*
	 * Number of prefetched files. Required to limit the number of prefetched
	 * files (max_restore_command_workers*PREFETCH_RATIO). Replaces direct
	 * counting of files in a PREFETCH_DIR.
	 */
	pg_atomic_uint32 nprefetched;

	/*
	 * Data for background workers.
	 */
	RestoreSlot slots[FLEXIBLE_ARRAY_MEMBER];
}			RestoreDataStruct;

RestoreDataStruct *RestoreData = NULL;

static bool RestoreCommandPrefetch(char *, const char *, const char *);
static void RestoreCommandPredict(const char *);
static int	RestoreCommandGarbage(void);
static bool RestoreSlotSpawn(RestoreSlot *);
static void RestoreSlotWait(RestoreSlot *);
static void RestoreSlotReset(RestoreSlot *);
static void RestoreSlotSetup(RestoreSlot *, const char *, const char *);
static RestoreSlot * RestoreSlotEmplace(bool);
static RestoreSlot * RestoreSlotFind(const char *);
static RestoreSlot * RestoreSlotPredict(const char *, const char *);
static void XLogFileNamePredict(char *, const char *);
static void XLogFilePathPrefetch(char *, const char *);
static bool FilePathExists(const char *);

/*
 * Calculate a size of shared memory used for storing bgworker slots.
 */
Size
RestoreCommandShmemSize(void)
{
	Size		size;

	size = sizeof(RestoreDataStruct);
	size = MAXALIGN(size);
	size = add_size(size, mul_size(max_restore_command_workers,
								   sizeof(RestoreSlot)));
	return size;

}

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

		/* nprefetched is also set to 0 by this */
		memset(RestoreData, 0, RestoreCommandShmemSize());

		/*
		 * Initialize memory and spin locks for each worker slot.
		 */
		for (i = 0; i < max_restore_command_workers; ++i)
		{
			RestoreSlot *slot = &RestoreData->slots[i];

			memset(slot, 0, sizeof(RestoreSlot));
			SpinLockInit(&slot->spin);

		}

		/* Create or clear temporary wals. */
		PathNameCreateTemporaryDir(XLOGDIR, PREFETCH_DIR);
		RemovePgTempFilesInDir(PREFETCH_DIR, true, true);
		/* before_shmem_exit(pg_on_exit_callback function, Datum arg) */
	}
}

/*
 * This function is analogue of RestoreArchivedFile function with xlogs
 * prefetching.
 *
 * If successful, subsequent WAL files will be predicted and
 * bgworkers processes be run to restore the predicted files.
 *
 * The number of predicted files will be limited to the number of free slots.
 *
 * If not successful, then fallback RestoreArchivedFile will be called.
 */
bool
RestoreCommandXLog(char *path, const char *xlogfname, const char *recovername,
				   const off_t expectedSize, bool cleanupEnabled)
{

	char		pointfname[MAXFNAMELEN];
	char		xlogpath[MAXPGPATH];
#ifdef USE_ASSERT_CHECKING
	uint32		new_val;
#endif

	/*
	 * Synchronous mode.
	 */
	if (max_restore_command_workers < 1)
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
	XLogFileNameLastPoint(pointfname, cleanupEnabled);

	/*
	 * We shouldn't need anything earlier than the last restart point.
	 */
	Assert(strcmp(pointfname, xlogfname) <= 0);

	/*
	 * If the restore failed, try fallback result.
	 */
	if (!RestoreCommandPrefetch(xlogpath, xlogfname, pointfname))
		goto fallback;

	/*
	 * Make sure the file is really there now and has the correct size.
	 */
	if (!FileValidateSize(xlogpath, expectedSize, xlogfname, NULL))
	{
		/* Remove artifacts. */
		FileUnlink(xlogpath);
		goto fallback;
	}

	/*
	 * Move file to target path.
	 */
	snprintf(path, MAXPGPATH, XLOGDIR "/%s", recovername);
	durable_rename(xlogpath, path, ERROR);

#ifdef USE_ASSERT_CHECKING
	new_val = pg_atomic_sub_fetch_u32(&RestoreData->nprefetched, 1);

	/*
	 * new_val expected to be >= 0. The assert below checks that
	 * RestoreData->nprefetched is not wrapped around 0 after atomic decrement.
	 */
	Assert(new_val != UINT_MAX);
#else
	pg_atomic_sub_fetch_u32(&RestoreData->nprefetched, 1);
#endif

	/*
	 * Log message like in RestoreArchivedFile.
	 */
	ereport(LOG,
			(errmsg("restored log file \"%s\" from archive",
					xlogfname)));

	/*
	 * Remove obsolete slots.
	 */
	if (RestoreCommandGarbage() > 0)
	{
		/*
		 * Predict next logs and spawn bgworkers.
		 */
		RestoreCommandPredict(xlogfname);
	}

	return true;

fallback:

	/*
	 * On any errors - try default implementation
	 */
	return RestoreArchivedFile(path, xlogfname, recovername, expectedSize,
							   cleanupEnabled);
}

/*
 * Attempt to retrieve the specified file from off-line archival storage
 * to PREFDIR directory.
 *
 * Fill "path" with its complete path.
 *
 * Return true if command was executed successfully and file exists, or the
 * file is found in the PREFDIR directory.
 */
static bool
RestoreCommandPrefetch(char *xlogpath, const char *xlogfname,
					   const char *pointfname)
{
	RestoreSlot *slot;
	bool		bgwdone;
	int			rc;

	/*
	 * Make prefetched path for file.
	 */
	XLogFilePathPrefetch(xlogpath, xlogfname);

	/*
	 * Check if file already on bgworker pool.
	 */
	if (!(slot = RestoreSlotFind(xlogfname)))
	{
		/*
		 * Check if file already on prefetch dir.
		 */
		if (FilePathExists(xlogpath))
			return true;

		/*
		 * Emplace a new slot and spawn bgworker.
		 */
		slot = RestoreSlotEmplace(true);
		Assert(slot);

		/*
		 * When the function RestoreSlotEmplace is invoked with the 'force'
		 * argument having true value this function calls the function
		 * RestoreSlotReset(). The function RestoreSlotReset() terminates active
		 * bgworker process if there is a bgwhandle associated with the slot.
		 * In result, when RestoreSlotEmplace(true) returns a control flow,
		 * the process that executes RestoreCommandWorkerMain() has been already
		 * finished or being finished. Anyway, it is safe to reset slot's data
		 * used from RestoreCommandWorkerMain() without first taking a lock
		 * on the spin lock slot->spin.
		 */
		RestoreSlotSetup(slot, xlogfname, pointfname);

		if (!RestoreSlotSpawn(slot))
			ereport(FATAL,
					(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
					 errmsg("out of background worker slots"),
					 errhint("You might need to increase max_worker_processes.")
					));

	}

	RestoreSlotWait(slot);

	bgwdone = slot->bgwdone;
	rc = slot->bgwrc;

	RestoreSlotReset(slot);

	/*
	 * bgworker didn't execute restore_command
	 */
	if (!bgwdone)
		return false;

	/*
	 * bgworker failed
	 */
	if (rc)
	{
		ereport(wait_result_is_any_signal(rc, true) ? FATAL : DEBUG2,
				(errmsg("could not restore file \"%s\" from archive: %s",
						xlogfname, wait_result_to_str(rc))));
	}

	return FilePathExists(xlogpath);
}

/*
 * Predict next logs and spawn bgworkers while possible.
 */
static void
RestoreCommandPredict(const char *xlogfname)
{
	char		pointfname[MAXFNAMELEN];
	const char	*xlogfnext;
	RestoreSlot	*slot;
	int			spawn_limit;

	XLogFileNameLastPoint(pointfname, false);
	xlogfnext = xlogfname;

	spawn_limit = PREFETCH_RATIO * max_restore_command_workers -
		pg_atomic_read_u32(&RestoreData->nprefetched);

	while (spawn_limit-- > 0)
	{
		if (!(slot = RestoreSlotPredict(xlogfnext, pointfname)))
			break;

		if (!RestoreSlotSpawn(slot))
		{
			ereport(WARNING,
					(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
					 errmsg("out of background worker slots"),
					 errhint("You might need to increase max_worker_processes.")
					));
			break;
		}

		ereport(DEBUG1,
				(errmsg("predicted log file \"%s\"",
						slot->xlogfname)));

		xlogfnext = slot->xlogfname;
	}
}

/*
 * Cleaning garbage on pool.
 *
 * Reset the slots linked to completed bgworkers.
 *
 * Return free slots count.
 */
static int
RestoreCommandGarbage(void)
{
	int			capacity;
	int			i;

	for (i = 0, capacity = 0; i < max_restore_command_workers; ++i)
	{
		RestoreSlot *slot = &RestoreData->slots[i];

		SpinLockAcquire(&slot->spin);
		if (slot->bgwdone)
		{
			int			bgwrc = slot->bgwrc;

			/*
			 * RestoreSlotReset() terminates a process if it does exist and
			 * releases a memory pointed by slot->bgwhandle. RestoreSlotReset()
			 * must be called before invoking of ereport() since the latter
			 * can result in termination of current process. In this case,
			 * bgworker process be orphaned and memory referenced by the
			 * slot->bgwhandle data member wouldn't be released. Although this
			 * memory will be released anyway on process termination, some
			 * memory sanitizer tools could produce error report for such memory
			 * chunks.
			 */
			RestoreSlotReset(slot);

			if (bgwrc)
			{
				ereport(wait_result_is_any_signal(bgwrc, true) ? FATAL :
																 DEBUG2,
						(errmsg("restore_command error: %s",
								wait_result_to_str(bgwrc))));
			}

			capacity++;
		}
		else if (slot->bgwhandle == NULL)
			/*
			 * Slots that are not assigned to any bgworker should be also
			 * counted.
			 */
			capacity++;

		SpinLockRelease(&slot->spin);
	}

	return capacity;
}

/*
 * The main entry point for bgworker.
 */
void
RestoreCommandWorkerMain(Datum main_arg)
{
	int			rc;
	RestoreSlot *slot;
	uint64		bgwid;
	bool		linked;
	char		xlogfname[MAXFNAMELEN];
	char		pointfname[MAXFNAMELEN];
	char		xlogpath[MAXPGPATH];

	/* Establish signal handlers. */
	pqsignal(SIGTERM, die);
	/* We're now ready to receive signals. */
	BackgroundWorkerUnblockSignals();

	/* Get bgwid */
	memcpy(&bgwid, MyBgworkerEntry->bgw_extra, sizeof(bgwid));

	/* Get RestoreSlot */
	slot = &RestoreData->slots[DatumGetInt16(main_arg)];
	SpinLockAcquire(&slot->spin);
	if ((linked = (slot->bgwid == bgwid)))
	{
		strlcpy(xlogfname, slot->xlogfname, sizeof(xlogfname));
		strlcpy(pointfname, slot->pointfname, sizeof(pointfname));
	}
	SpinLockRelease(&slot->spin);

	if (!linked)
		ereport(FATAL,
				(errmsg("slot " UINT64_FORMAT " is unlinked during a restore",
						bgwid)));

	/* Prepare path. */
	XLogFilePathPrefetch(xlogpath, xlogfname);

	/* Make sure there is no existing file named recovername. */
	FileUnlink(xlogpath);

	/* Prepare and execute the restore command. */
	if ((rc = DoRestore(xlogpath, xlogfname, pointfname)))
		FileUnlink(xlogpath);

	CHECK_FOR_INTERRUPTS();

	/* Keep the results. */
	SpinLockAcquire(&slot->spin);
	/*
	 * Retesting of the condition 'slot->bgwid == bgwid' is required to
	 * guard against reusing of a slot inside RestoreCommandPrefetch function
	 * when RestoreSlotEmplace function called with argument value equals true.
	*/
	if ((linked = (slot->bgwid == bgwid)))
	{
		slot->bgwdone = true;
		slot->bgwrc = rc;

		if (FilePathExists(xlogpath))
			pg_atomic_add_fetch_u32(&RestoreData->nprefetched, 1);

	}
	SpinLockRelease(&slot->spin);

	/* If slot was unlinked - delete restored file. */
	if (!linked)
	{
		FileUnlink(xlogpath);

		ereport(FATAL,
				(errmsg("slot %s is unlinked during a restore",
						xlogfname)));
	}
	else
		ereport(DEBUG2,
				(errmsg_internal("slot %s done %d \"%s\"",
								 xlogfname, rc, xlogpath)));

	proc_exit(0);
}


/*
 * Setup and spawn bgworker.
 * Link it to the slot by bgwhandle.
 */
static bool
RestoreSlotSpawn(RestoreSlot *slot)
{
	BackgroundWorker bgw;

	memset(&bgw, 0, sizeof(bgw));
	snprintf(bgw.bgw_name, sizeof(bgw.bgw_name), "restore %s", slot->xlogfname);

	/*
	 * Length of the string literal "Restore Command Worker" is less than
	 * size of a buffer referenced by the data member bgw.bgw_type (the size is
	 * limited by the constant BGW_MAXLEN that currently has value 96).
	 * Therefore we can use function strcpy() instead of strncpy/strlcpy to copy
	 * the string literal into the buffer bgw.bgw_type. The same is true for
	 * other two string literals "postgres" and "RestoreCommandWorkerMain" and
	 * their corresponding destination buffers referenced by the data members
	 * bgw.bgw_library_name, bgw.bgw_function_name.
	 * To guards against further possible change of limit represented by the
	 * constant BGW_MAXLEN the asserts have been inserted before invoking of
	 * the function strcpy() as a sanity check. In case some of these asserts be
	 * fired it means that some really drastic change was done in the core
	 * source code that should be carefully studied.
	 */
	Assert(sizeof(bgw.bgw_type) >= sizeof("Restore Command Worker"));
	Assert(sizeof(bgw.bgw_library_name) >= sizeof("postgres"));
	Assert(sizeof(bgw.bgw_function_name) >= sizeof("RestoreCommandWorkerMain"));

	strcpy(bgw.bgw_type, "Restore Command Worker");
	strcpy(bgw.bgw_library_name, "postgres");
	strcpy(bgw.bgw_function_name, "RestoreCommandWorkerMain");

	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;

	/*
	 * BgWorkerStart_PostmasterStart for PM_RECOVERY, PM_STARTUP
	 * BgWorkerStart_ConsistentState for PM_HOT_STANDBY
	 */
	bgw.bgw_start_time = HotStandbyActive() ? BgWorkerStart_ConsistentState :
		BgWorkerStart_PostmasterStart;

	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_main_arg = Int16GetDatum(slot - RestoreData->slots);
	memcpy(bgw.bgw_extra, &slot->bgwid, sizeof(slot->bgwid));
	bgw.bgw_notify_pid = MyProcPid;
	return RegisterDynamicBackgroundWorker(&bgw, &slot->bgwhandle);
}

/*
 * Wait linked bgworker to shutdown.
 */
static void
RestoreSlotWait(RestoreSlot *slot)
{
	BgwHandleStatus status;
	pid_t		pid;

	/* is linked slot by bgworker */
	if (slot->bgwhandle == NULL)
		return;

	/* WaitForBackgroundWorkerShutdown  */
	for (;;)
	{
		status = GetBackgroundWorkerPid(slot->bgwhandle, &pid);
		if (status == BGWH_STOPPED)
			break;

		WaitLatch(MyLatch,
				  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				  8,
				  PG_WAIT_EXTENSION);

		ResetLatch(MyLatch);
		HandleStartupProcInterrupts();
	}

	pfree(slot->bgwhandle);

	slot->bgwhandle = NULL;
}

/*
 * Reset slot params and terminate linked bgworker if exists.
 */
static void
RestoreSlotReset(RestoreSlot *slot)
{
	slot->xlogfname[0] = '\0';
	slot->pointfname[0] = '\0';
	slot->bgwdone = false;
	slot->bgwrc = 0;
	slot->bgwid = 0;
	slot->bgwtime = 0;
	if (slot->bgwhandle)
	{
		/* If there's a bgworker tied up, stop it. */
		TerminateBackgroundWorker(slot->bgwhandle);

		/* Release memory. */
		pfree(slot->bgwhandle);
		slot->bgwhandle = NULL;
	}
}

/*
 *  Configure slot options.
 */
static void
RestoreSlotSetup(RestoreSlot * slot, const char *xlogfname,
				 const char *pointfname)
{
	strlcpy(slot->xlogfname, xlogfname, sizeof(slot->xlogfname));
	strlcpy(slot->pointfname, pointfname, sizeof(slot->pointfname));
	slot->bgwid = RestoreData->counter++;
	slot->bgwtime = GetCurrentTimestamp();
}

/*
 * Get a free slot.
 *
 * Return NULL if no free slots.
 *
 * If force param is TRUE and there are no free slots,
 * then return the earliest slot.
 */
static RestoreSlot *
RestoreSlotEmplace(bool force)
{
	int			i;
	RestoreSlot *reslot;

	reslot = NULL;
	for (i = 0; i < max_restore_command_workers; ++i)
	{
		RestoreSlot *slot = &RestoreData->slots[i];

		if (!slot->bgwhandle)
		{
			reslot = slot;
			break;
		}

		if (reslot == NULL || slot->bgwtime < reslot->bgwtime)
			reslot = slot;
	}

	if (!reslot)
		return NULL;

	/* Do not use linked slots in unforced mode. */
	if (!force && reslot->bgwhandle)
		return NULL;

	/* Reset slot (unlink if linked).  */
	SpinLockAcquire(&reslot->spin);
	RestoreSlotReset(reslot);
	SpinLockRelease(&reslot->spin);

	return reslot;
}

/*
 * Find a slot on pool by WAL name.
 */
static RestoreSlot *
RestoreSlotFind(const char *xlogfname)
{
	int			i;

	for (i = 0; i < max_restore_command_workers; ++i)
	{
		RestoreSlot *slot = &RestoreData->slots[i];

		if (!strcmp(slot->xlogfname, xlogfname))
			return slot;
	}

	return NULL;
}

/*
 * Predict the next WAL name and allocate a slot for it.
 * Return NULL if no slots are available.
 */
static RestoreSlot *
RestoreSlotPredict(const char *xlogfname, const char *pointfname)
{
	char		xlogfnext[MAXFNAMELEN];
	char		xlogpath[MAXPGPATH];

	strlcpy(xlogfnext, xlogfname, sizeof(xlogfnext));

	for (;;)
	{
		RestoreSlot *slot;

		/* already in pool */
		XLogFileNamePredict(xlogfnext, xlogfnext);

		if (RestoreSlotFind(xlogfnext))
			continue;

		/* already restored */
		XLogFilePathPrefetch(xlogpath, xlogfnext);

		if (FilePathExists(xlogpath))
			continue;

		if (!(slot = RestoreSlotEmplace(false)))
			break;

		RestoreSlotSetup(slot, xlogfnext, pointfname);
		return slot;
	}

	return NULL;
}

/*
 * Predict the name of the next WAL file "xlognext",
 * based on the specified "xlogfname".
 */
static void
XLogFileNamePredict(char *xlogfnext, const char *xlogfname)
{
	uint32		tli;
	XLogSegNo	segno;

	XLogFromFileName(xlogfname, &tli, &segno, wal_segment_size);
	XLogFileName(xlogfnext, tli, segno + 1, wal_segment_size);
}

static void
XLogFilePathPrefetch(char *path, const char *xlogfname)
{
	snprintf(path, MAXPGPATH, PREFETCH_DIR "/%s", xlogfname);
}

/*
 * Check that the path does exist.
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
