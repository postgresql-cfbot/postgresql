/*-------------------------------------------------------------------------
 *
 * xlogarchive.c
 *		Functions for archiving WAL files and restoring from the archive.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/xlogarchive.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogarchive.h"
#include "common/archive.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/startup.h"
#include "postmaster/pgarch.h"
#include "replication/walsender.h"
#include "restore/restore_module.h"
#include "restore/shell_restore.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "utils/memutils.h"

char *restoreLibrary = "";
const RestoreModuleCallbacks *RestoreCallbacks = NULL;
RestoreModuleState *restore_module_state;

/*
 * Attempt to retrieve the specified file from off-line archival storage.
 * If successful, fill "path" with its complete path (note that this will be
 * a temp file name that doesn't follow the normal naming convention), and
 * return true.
 *
 * If not successful, fill "path" with the name of the normal on-line file
 * (which may or may not actually exist, but we'll try to use it), and return
 * false.
 *
 * For fixed-size files, the caller may pass the expected size as an
 * additional crosscheck on successful recovery.  If the file size is not
 * known, set expectedSize = 0.
 *
 * When 'cleanupEnabled' is false, refrain from deleting any old WAL segments
 * in the archive. This is used when fetching the initial checkpoint record,
 * when we are not yet sure how far back we need the WAL.
 */
bool
RestoreArchivedFile(char *path, const char *xlogfname,
					const char *recovername, off_t expectedSize,
					bool cleanupEnabled, ArchiveType archive_type)
{
	char		xlogpath[MAXPGPATH];
	char		lastRestartPointFname[MAXPGPATH];
	bool		ret = false;
	struct stat stat_buf;
	XLogSegNo	restartSegNo;
	XLogRecPtr	restartRedoPtr;
	TimeLineID	restartTli;

	/*
	 * Ignore restore_command when not in archive recovery (meaning we are in
	 * crash recovery).
	 */
	if (!ArchiveRecoveryRequested)
		goto not_available;

	/* In standby mode, restore_command might not be supplied */
	switch (archive_type)
	{
		case ARCHIVE_TYPE_WAL_SEGMENT:
			if (!restore_wal_segment_configured())
				goto not_available;
			break;
		case ARCHIVE_TYPE_TIMELINE_HISTORY:
			if (!restore_timeline_history_configured())
				goto not_available;
			break;
		case ARCHIVE_TYPE_TIMELINE_HISTORY_EXISTS:
			if (!timeline_history_exists_configured())
				goto not_available;
			break;
	}

	/*
	 * When doing archive recovery, we always prefer an archived log file even
	 * if a file of the same name exists in XLOGDIR.  The reason is that the
	 * file in XLOGDIR could be an old, un-filled or partly-filled version
	 * that was copied and restored as part of backing up $PGDATA.
	 *
	 * We could try to optimize this slightly by checking the local copy
	 * lastchange timestamp against the archived copy, but we have no API to
	 * do this, nor can we guarantee that the lastchange timestamp was
	 * preserved correctly when we copied to archive. Our aim is robustness,
	 * so we elect not to do this.
	 *
	 * If we cannot obtain the log file from the archive, however, we will try
	 * to use the XLOGDIR file if it exists.  This is so that we can make use
	 * of log segments that weren't yet transferred to the archive.
	 *
	 * Notice that we don't actually overwrite any files when we copy back
	 * from archive because the restore_command may inadvertently restore
	 * inappropriate xlogs, or they may be corrupt, so we may wish to fallback
	 * to the segments remaining in current XLOGDIR later. The
	 * copy-from-archive filename is always the same, ensuring that we don't
	 * run out of disk space on long recoveries.
	 */
	snprintf(xlogpath, MAXPGPATH, XLOGDIR "/%s", recovername);

	/*
	 * Make sure there is no existing file named recovername.
	 */
	if (stat(xlogpath, &stat_buf) != 0)
	{
		if (errno != ENOENT)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m",
							xlogpath)));
	}
	else
	{
		if (unlink(xlogpath) != 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not remove file \"%s\": %m",
							xlogpath)));
	}

	/*
	 * Calculate the archive file cutoff point for use during log shipping
	 * replication. All files earlier than this point can be deleted from the
	 * archive, though there is no requirement to do so.
	 *
	 * If cleanup is not enabled, initialise this with the filename of
	 * InvalidXLogRecPtr, which will prevent the deletion of any WAL files
	 * from the archive because of the alphabetic sorting property of WAL
	 * filenames.
	 *
	 * Once we have successfully located the redo pointer of the checkpoint
	 * from which we start recovery we never request a file prior to the redo
	 * pointer of the last restartpoint. When redo begins we know that we have
	 * successfully located it, so there is no need for additional status
	 * flags to signify the point when we can begin deleting WAL files from
	 * the archive.
	 */
	if (cleanupEnabled)
	{
		GetOldestRestartPoint(&restartRedoPtr, &restartTli);
		XLByteToSeg(restartRedoPtr, restartSegNo, wal_segment_size);
		XLogFileName(lastRestartPointFname, restartTli, restartSegNo,
					 wal_segment_size);
		/* we shouldn't need anything earlier than last restart point */
		Assert(strcmp(lastRestartPointFname, xlogfname) <= 0);
	}
	else
		XLogFileName(lastRestartPointFname, 0, 0, wal_segment_size);

	/*
	 * To ensure we are responsive to server shutdown, check for shutdown
	 * requests before and after restoring a file.  If there is one, we exit
	 * right away.
	 */
	HandleStartupProcShutdownRequests();

	/*
	 * Copy xlog from archival storage to XLOGDIR
	 */
	switch (archive_type)
	{
		case ARCHIVE_TYPE_WAL_SEGMENT:
			ret = RestoreCallbacks->restore_wal_segment_cb(restore_module_state,
														   xlogfname, xlogpath,
														   lastRestartPointFname);
			break;
		case ARCHIVE_TYPE_TIMELINE_HISTORY:
			ret = RestoreCallbacks->restore_timeline_history_cb(restore_module_state,
																xlogfname, xlogpath);
			break;
		case ARCHIVE_TYPE_TIMELINE_HISTORY_EXISTS:
			ret = RestoreCallbacks->timeline_history_exists_cb(restore_module_state,
															   xlogfname, xlogpath);
			break;
	}

	HandleStartupProcShutdownRequests();

	if (ret)
	{
		/*
		 * Some restore functions might not copy the file (e.g., checking
		 * whether a timeline history file exists), but they can set a flag to
		 * tell us if they do.  We only need to verify file existence if this
		 * flag is enabled.
		 */
		if (archive_type == ARCHIVE_TYPE_TIMELINE_HISTORY_EXISTS &&
			!restore_module_state->timeline_history_exists_cb_copies)
			return true;

		/*
		 * command apparently succeeded, but let's make sure the file is
		 * really there now and has the correct size.
		 */
		if (stat(xlogpath, &stat_buf) == 0)
		{
			if (expectedSize > 0 && stat_buf.st_size != expectedSize)
			{
				int			elevel;

				/*
				 * If we find a partial file in standby mode, we assume it's
				 * because it's just being copied to the archive, and keep
				 * trying.
				 *
				 * Otherwise treat a wrong-sized file as FATAL to ensure the
				 * DBA would notice it, but is that too strong? We could try
				 * to plow ahead with a local copy of the file ... but the
				 * problem is that there probably isn't one, and we'd
				 * incorrectly conclude we've reached the end of WAL and we're
				 * done recovering ...
				 */
				if (StandbyMode && stat_buf.st_size < expectedSize)
					elevel = DEBUG1;
				else
					elevel = FATAL;
				ereport(elevel,
						(errmsg("archive file \"%s\" has wrong size: %lld instead of %lld",
								xlogfname,
								(long long int) stat_buf.st_size,
								(long long int) expectedSize)));
				return false;
			}
			else
			{
				ereport(LOG,
						(errmsg("restored log file \"%s\" from archive",
								xlogfname)));
				strcpy(path, xlogpath);
				return true;
			}
		}
		else
		{
			/* stat failed */
			int			elevel = (errno == ENOENT) ? LOG : FATAL;

			ereport(elevel,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m", xlogpath),
					 errdetail("restore_command returned a zero exit status, but stat() failed.")));
		}
	}

not_available:

	/*
	 * if an archived file is not available, there might still be a version of
	 * this file in XLOGDIR, so return that as the filename to open.
	 *
	 * In many recovery scenarios we expect this to fail also, but if so that
	 * just means we've reached the end of WAL.
	 */
	snprintf(path, MAXPGPATH, XLOGDIR "/%s", xlogfname);
	return false;
}

/*
 * A file was restored from the archive under a temporary filename (path),
 * and now we want to keep it. Rename it under the permanent filename in
 * pg_wal (xlogfname), replacing any existing file with the same name.
 */
void
KeepFileRestoredFromArchive(const char *path, const char *xlogfname)
{
	char		xlogfpath[MAXPGPATH];
	bool		reload = false;
	struct stat statbuf;

	snprintf(xlogfpath, MAXPGPATH, XLOGDIR "/%s", xlogfname);

	if (stat(xlogfpath, &statbuf) == 0)
	{
		char		oldpath[MAXPGPATH];

#ifdef WIN32
		static unsigned int deletedcounter = 1;

		/*
		 * On Windows, if another process (e.g a walsender process) holds the
		 * file open in FILE_SHARE_DELETE mode, unlink will succeed, but the
		 * file will still show up in directory listing until the last handle
		 * is closed, and we cannot rename the new file in its place until
		 * that. To avoid that problem, rename the old file to a temporary
		 * name first. Use a counter to create a unique filename, because the
		 * same file might be restored from the archive multiple times, and a
		 * walsender could still be holding onto an old deleted version of it.
		 */
		snprintf(oldpath, MAXPGPATH, "%s.deleted%u",
				 xlogfpath, deletedcounter++);
		if (rename(xlogfpath, oldpath) != 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not rename file \"%s\" to \"%s\": %m",
							xlogfpath, oldpath)));
		}
#else
		/* same-size buffers, so this never truncates */
		strlcpy(oldpath, xlogfpath, MAXPGPATH);
#endif
		if (unlink(oldpath) != 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not remove file \"%s\": %m",
							xlogfpath)));
		reload = true;
	}

	durable_rename(path, xlogfpath, ERROR);

	/*
	 * Create .done file forcibly to prevent the restored segment from being
	 * archived again later.
	 */
	if (XLogArchiveMode != ARCHIVE_MODE_ALWAYS)
		XLogArchiveForceDone(xlogfname);
	else
		XLogArchiveNotify(xlogfname);

	/*
	 * If the existing file was replaced, since walsenders might have it open,
	 * request them to reload a currently-open segment. This is only required
	 * for WAL segments, walsenders don't hold other files open, but there's
	 * no harm in doing this too often, and we don't know what kind of a file
	 * we're dealing with here.
	 */
	if (reload)
		WalSndRqstFileReload();

	/*
	 * Signal walsender that new WAL has arrived. Again, this isn't necessary
	 * if we restored something other than a WAL segment, but it does no harm
	 * either.
	 */
	WalSndWakeup(true, false);
}

/*
 * XLogArchiveNotify
 *
 * Create an archive notification file
 *
 * The name of the notification file is the message that will be picked up
 * by the archiver, e.g. we write 0000000100000001000000C6.ready
 * and the archiver then knows to archive XLOGDIR/0000000100000001000000C6,
 * then when complete, rename it to 0000000100000001000000C6.done
 */
void
XLogArchiveNotify(const char *xlog)
{
	char		archiveStatusPath[MAXPGPATH];
	FILE	   *fd;

	/* insert an otherwise empty file called <XLOG>.ready */
	StatusFilePath(archiveStatusPath, xlog, ".ready");
	fd = AllocateFile(archiveStatusPath, "w");
	if (fd == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not create archive status file \"%s\": %m",
						archiveStatusPath)));
		return;
	}
	if (FreeFile(fd))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write archive status file \"%s\": %m",
						archiveStatusPath)));
		return;
	}

	/*
	 * Timeline history files are given the highest archival priority to lower
	 * the chance that a promoted standby will choose a timeline that is
	 * already in use.  However, the archiver ordinarily tries to gather
	 * multiple files to archive from each scan of the archive_status
	 * directory, which means that newly created timeline history files could
	 * be left unarchived for a while.  To ensure that the archiver picks up
	 * timeline history files as soon as possible, we force the archiver to
	 * scan the archive_status directory the next time it looks for a file to
	 * archive.
	 */
	if (IsTLHistoryFileName(xlog))
		PgArchForceDirScan();

	/* Notify archiver that it's got something to do */
	if (IsUnderPostmaster)
		PgArchWakeup();
}

/*
 * Convenience routine to notify using segment number representation of filename
 */
void
XLogArchiveNotifySeg(XLogSegNo segno, TimeLineID tli)
{
	char		xlog[MAXFNAMELEN];

	Assert(tli != 0);

	XLogFileName(xlog, tli, segno, wal_segment_size);
	XLogArchiveNotify(xlog);
}

/*
 * XLogArchiveForceDone
 *
 * Emit notification forcibly that an XLOG segment file has been successfully
 * archived, by creating <XLOG>.done regardless of whether <XLOG>.ready
 * exists or not.
 */
void
XLogArchiveForceDone(const char *xlog)
{
	char		archiveReady[MAXPGPATH];
	char		archiveDone[MAXPGPATH];
	struct stat stat_buf;
	FILE	   *fd;

	/* Exit if already known done */
	StatusFilePath(archiveDone, xlog, ".done");
	if (stat(archiveDone, &stat_buf) == 0)
		return;

	/* If .ready exists, rename it to .done */
	StatusFilePath(archiveReady, xlog, ".ready");
	if (stat(archiveReady, &stat_buf) == 0)
	{
		(void) durable_rename(archiveReady, archiveDone, WARNING);
		return;
	}

	/* insert an otherwise empty file called <XLOG>.done */
	fd = AllocateFile(archiveDone, "w");
	if (fd == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not create archive status file \"%s\": %m",
						archiveDone)));
		return;
	}
	if (FreeFile(fd))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write archive status file \"%s\": %m",
						archiveDone)));
		return;
	}
}

/*
 * XLogArchiveCheckDone
 *
 * This is called when we are ready to delete or recycle an old XLOG segment
 * file or backup history file.  If it is okay to delete it then return true.
 * If it is not time to delete it, make sure a .ready file exists, and return
 * false.
 *
 * If <XLOG>.done exists, then return true; else if <XLOG>.ready exists,
 * then return false; else create <XLOG>.ready and return false.
 *
 * The reason we do things this way is so that if the original attempt to
 * create <XLOG>.ready fails, we'll retry during subsequent checkpoints.
 */
bool
XLogArchiveCheckDone(const char *xlog)
{
	char		archiveStatusPath[MAXPGPATH];
	struct stat stat_buf;

	/* The file is always deletable if archive_mode is "off". */
	if (!XLogArchivingActive())
		return true;

	/*
	 * During archive recovery, the file is deletable if archive_mode is not
	 * "always".
	 */
	if (!XLogArchivingAlways() &&
		GetRecoveryState() == RECOVERY_STATE_ARCHIVE)
		return true;

	/*
	 * At this point of the logic, note that we are either a primary with
	 * archive_mode set to "on" or "always", or a standby with archive_mode
	 * set to "always".
	 */

	/* First check for .done --- this means archiver is done with it */
	StatusFilePath(archiveStatusPath, xlog, ".done");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return true;

	/* check for .ready --- this means archiver is still busy with it */
	StatusFilePath(archiveStatusPath, xlog, ".ready");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return false;

	/* Race condition --- maybe archiver just finished, so recheck */
	StatusFilePath(archiveStatusPath, xlog, ".done");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return true;

	/* Retry creation of the .ready file */
	XLogArchiveNotify(xlog);
	return false;
}

/*
 * XLogArchiveIsBusy
 *
 * Check to see if an XLOG segment file is still unarchived.
 * This is almost but not quite the inverse of XLogArchiveCheckDone: in
 * the first place we aren't chartered to recreate the .ready file, and
 * in the second place we should consider that if the file is already gone
 * then it's not busy.  (This check is needed to handle the race condition
 * that a checkpoint already deleted the no-longer-needed file.)
 */
bool
XLogArchiveIsBusy(const char *xlog)
{
	char		archiveStatusPath[MAXPGPATH];
	struct stat stat_buf;

	/* First check for .done --- this means archiver is done with it */
	StatusFilePath(archiveStatusPath, xlog, ".done");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return false;

	/* check for .ready --- this means archiver is still busy with it */
	StatusFilePath(archiveStatusPath, xlog, ".ready");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return true;

	/* Race condition --- maybe archiver just finished, so recheck */
	StatusFilePath(archiveStatusPath, xlog, ".done");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return false;

	/*
	 * Check to see if the WAL file has been removed by checkpoint, which
	 * implies it has already been archived, and explains why we can't see a
	 * status file for it.
	 */
	snprintf(archiveStatusPath, MAXPGPATH, XLOGDIR "/%s", xlog);
	if (stat(archiveStatusPath, &stat_buf) != 0 &&
		errno == ENOENT)
		return false;

	return true;
}

/*
 * XLogArchiveIsReadyOrDone
 *
 * Check to see if an XLOG segment file has a .ready or .done file.
 * This is similar to XLogArchiveIsBusy(), but returns true if the file
 * is already archived or is about to be archived.
 *
 * This is currently only used at recovery.  During normal operation this
 * would be racy: the file might get removed or marked with .ready as we're
 * checking it, or immediately after we return.
 */
bool
XLogArchiveIsReadyOrDone(const char *xlog)
{
	char		archiveStatusPath[MAXPGPATH];
	struct stat stat_buf;

	/* First check for .done --- this means archiver is done with it */
	StatusFilePath(archiveStatusPath, xlog, ".done");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return true;

	/* check for .ready --- this means archiver is still busy with it */
	StatusFilePath(archiveStatusPath, xlog, ".ready");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return true;

	/* Race condition --- maybe archiver just finished, so recheck */
	StatusFilePath(archiveStatusPath, xlog, ".done");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return true;

	return false;
}

/*
 * XLogArchiveIsReady
 *
 * Check to see if an XLOG segment file has an archive notification (.ready)
 * file.
 */
bool
XLogArchiveIsReady(const char *xlog)
{
	char		archiveStatusPath[MAXPGPATH];
	struct stat stat_buf;

	StatusFilePath(archiveStatusPath, xlog, ".ready");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return true;

	return false;
}

/*
 * XLogArchiveCleanup
 *
 * Cleanup archive notification file(s) for a particular xlog segment
 */
void
XLogArchiveCleanup(const char *xlog)
{
	char		archiveStatusPath[MAXPGPATH];

	/* Remove the .done file */
	StatusFilePath(archiveStatusPath, xlog, ".done");
	unlink(archiveStatusPath);
	/* should we complain about failure? */

	/* Remove the .ready file if present --- normally it shouldn't be */
	StatusFilePath(archiveStatusPath, xlog, ".ready");
	unlink(archiveStatusPath);
	/* should we complain about failure? */
}

/*
 * Loads all the restore callbacks into our global RestoreCallbacks.  The
 * caller is responsible for validating the combination of library/command
 * parameters that are set (e.g., restore_command and restore_library cannot
 * both be set).
 */
void
LoadRestoreCallbacks(void)
{
	RestoreModuleInit init;

	/*
	 * If the shell command is enabled, use our special initialization
	 * function.  Otherwise, load the library and call its
	 * _PG_restore_module_init().
	 */
	if (restoreLibrary[0] == '\0')
		init = shell_restore_init;
	else
		init = (RestoreModuleInit)
				load_external_function(restoreLibrary, "_PG_restore_module_init",
									   false, NULL);

	if (init == NULL)
		ereport(ERROR,
				(errmsg("restore modules have to define the symbol "
						"_PG_restore_module_init")));

	RestoreCallbacks = (*init) ();

	/* restore state should be freed before calling this function */
	Assert(restore_module_state == NULL);
	restore_module_state = (RestoreModuleState *)
							MemoryContextAllocZero(TopMemoryContext,
												   sizeof(RestoreModuleState));

	if (RestoreCallbacks->startup_cb != NULL)
		RestoreCallbacks->startup_cb(restore_module_state);
}

/*
 * Call the shutdown callback of the loaded restore module, if defined.  Also,
 * free the restore module state if it was allocated.
 *
 * Processes that load restore libraries should register this via
 * before_shmem_exit().
 */
void
call_restore_module_shutdown_cb(int code, Datum arg)
{
	if (RestoreCallbacks != NULL &&
		RestoreCallbacks->shutdown_cb != NULL &&
		restore_module_state != NULL)
		RestoreCallbacks->shutdown_cb(restore_module_state);

	if (restore_module_state != NULL)
	{
		pfree(restore_module_state);
		restore_module_state = NULL;
	}
}
