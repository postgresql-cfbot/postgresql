/*-------------------------------------------------------------------------
 *
 * xlogbackup.c
 *		Internal routines for base backups.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      src/backend/access/transam/xlogbackup.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <time.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogarchive.h"
#include "access/xlogbackup.h"
#include "access/xloginsert.h"
#include "backup/basebackup.h"
#include "catalog/pg_control.h"
#include "common/file_utils.h"
#include "miscadmin.h"
#include "postmaster/bgwriter.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "utils/wait_event.h"

/*
 * Session status of running backup, used for sanity checks in SQL-callable
 * functions to start and stop backups.
 */
static SessionBackupState sessionBackupState = SESSION_BACKUP_NONE;

static void CleanupBackupHistory(void);

/*
 * Build contents for backup_label or backup history file.
 *
 * When ishistoryfile is true, it creates the contents for a backup history
 * file, otherwise it creates contents for a backup_label file.
 *
 * Returns the result generated as a palloc'd string.
 */
char *
build_backup_content(BackupState *state, bool ishistoryfile)
{
	char		startstrbuf[128];
	char		startxlogfile[MAXFNAMELEN]; /* backup start WAL file */
	XLogSegNo	startsegno;
	StringInfo	result = makeStringInfo();
	char	   *data;

	Assert(state != NULL);

	/* Use the log timezone here, not the session timezone */
	pg_strftime(startstrbuf, sizeof(startstrbuf), "%Y-%m-%d %H:%M:%S %Z",
				pg_localtime(&state->starttime, log_timezone));

	XLByteToSeg(state->startpoint, startsegno, wal_segment_size);
	XLogFileName(startxlogfile, state->starttli, startsegno, wal_segment_size);
	appendStringInfo(result, "START WAL LOCATION: %X/%X (file %s)\n",
					 LSN_FORMAT_ARGS(state->startpoint), startxlogfile);

	if (ishistoryfile)
	{
		char		stopxlogfile[MAXFNAMELEN];	/* backup stop WAL file */
		XLogSegNo	stopsegno;

		XLByteToSeg(state->stoppoint, stopsegno, wal_segment_size);
		XLogFileName(stopxlogfile, state->stoptli, stopsegno, wal_segment_size);
		appendStringInfo(result, "STOP WAL LOCATION: %X/%X (file %s)\n",
						 LSN_FORMAT_ARGS(state->stoppoint), stopxlogfile);
	}

	appendStringInfo(result, "CHECKPOINT LOCATION: %X/%X\n",
					 LSN_FORMAT_ARGS(state->checkpointloc));
	appendStringInfo(result, "BACKUP METHOD: streamed\n");
	appendStringInfo(result, "BACKUP FROM: %s\n",
					 state->started_in_recovery ? "standby" : "primary");
	appendStringInfo(result, "START TIME: %s\n", startstrbuf);
	appendStringInfo(result, "LABEL: %s\n", state->name);
	appendStringInfo(result, "START TIMELINE: %u\n", state->starttli);

	if (ishistoryfile)
	{
		char		stopstrfbuf[128];

		/* Use the log timezone here, not the session timezone */
		pg_strftime(stopstrfbuf, sizeof(stopstrfbuf), "%Y-%m-%d %H:%M:%S %Z",
					pg_localtime(&state->stoptime, log_timezone));

		appendStringInfo(result, "STOP TIME: %s\n", stopstrfbuf);
		appendStringInfo(result, "STOP TIMELINE: %u\n", state->stoptli);
	}

	data = result->data;
	pfree(result);

	return data;
}

/*
 * Remove previous backup history files.  This also retries creation of
 * .ready files for any backup history files for which XLogArchiveNotify
 * failed earlier.
 */
static void
CleanupBackupHistory(void)
{
	DIR		   *xldir;
	struct dirent *xlde;
	char		path[MAXPGPATH + sizeof(XLOGDIR)];

	xldir = AllocateDir(XLOGDIR);

	while ((xlde = ReadDir(xldir, XLOGDIR)) != NULL)
	{
		if (IsBackupHistoryFileName(xlde->d_name))
		{
			if (XLogArchiveCheckDone(xlde->d_name))
			{
				elog(DEBUG2, "removing WAL backup history file \"%s\"",
					 xlde->d_name);
				snprintf(path, sizeof(path), XLOGDIR "/%s", xlde->d_name);
				unlink(path);
				XLogArchiveCleanup(xlde->d_name);
			}
		}
	}

	FreeDir(xldir);
}

/*
 * do_pg_backup_start is the workhorse of the user-visible pg_backup_start()
 * function. It creates the necessary starting checkpoint and constructs the
 * backup state and tablespace map.
 *
 * Input parameters are "state" (the backup state), "fast" (if true, we do
 * the checkpoint in immediate mode to make it faster), and "tablespaces"
 * (if non-NULL, indicates a list of tablespaceinfo structs describing the
 * cluster's tablespaces.).
 *
 * The tablespace map contents are appended to passed-in parameter
 * tablespace_map and the caller is responsible for including it in the backup
 * archive as 'tablespace_map'. The tablespace_map file is required mainly for
 * tar format in windows as native windows utilities are not able to create
 * symlinks while extracting files from tar. However for consistency and
 * platform-independence, we do it the same way everywhere.
 *
 * It fills in "state" with the information required for the backup, such
 * as the minimum WAL location that must be present to restore from this
 * backup (starttli) and the corresponding timeline ID (starttli).
 *
 * Every successfully started backup must be stopped by calling
 * do_pg_backup_stop() or do_pg_abort_backup(). There can be many
 * backups active at the same time.
 *
 * It is the responsibility of the caller of this function to verify the
 * permissions of the calling user!
 */
void
do_pg_backup_start(const char *backupidstr, bool fast, List **tablespaces,
				   BackupState *state, StringInfo tblspcmapfile)
{
	bool		backup_started_in_recovery;

	Assert(state != NULL);
	backup_started_in_recovery = RecoveryInProgress();

	/*
	 * During recovery, we don't need to check WAL level. Because, if WAL
	 * level is not sufficient, it's impossible to get here during recovery.
	 */
	if (!backup_started_in_recovery && !XLogIsNeeded())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL level not sufficient for making an online backup"),
				 errhint("wal_level must be set to \"replica\" or \"logical\" at server start.")));

	if (strlen(backupidstr) > MAXPGPATH)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("backup label too long (max %d bytes)",
						MAXPGPATH)));

	memcpy(state->name, backupidstr, strlen(backupidstr));

	/*
	 * Mark backup active in shared memory.  We must do full-page WAL writes
	 * during an on-line backup even if not doing so at other times, because
	 * it's quite possible for the backup dump to obtain a "torn" (partially
	 * written) copy of a database page if it reads the page concurrently with
	 * our write to the same page.  This can be fixed as long as the first
	 * write to the page in the WAL sequence is a full-page write. Hence, we
	 * increment runningBackups then force a CHECKPOINT, to ensure there are
	 * no dirty pages in shared memory that might get dumped while the backup
	 * is in progress without having a corresponding WAL record.  (Once the
	 * backup is complete, we need not force full-page writes anymore, since
	 * we expect that any pages not modified during the backup interval must
	 * have been correctly captured by the backup.)
	 *
	 * Note that forcing full-page writes has no effect during an online
	 * backup from the standby.
	 *
	 * We must hold all the insertion locks to change the value of
	 * runningBackups, to ensure adequate interlocking against
	 * XLogInsertRecord().
	 */
	XLogBackupSetRunning();

	/*
	 * Ensure we decrement runningBackups if we fail below. NB -- for this to
	 * work correctly, it is critical that sessionBackupState is only updated
	 * after this block is over.
	 */
	PG_ENSURE_ERROR_CLEANUP(do_pg_abort_backup, DatumGetBool(true));
	{
		bool		gotUniqueStartpoint = false;
		DIR		   *tblspcdir;
		struct dirent *de;
		tablespaceinfo *ti;
		int			datadirpathlen;

		/*
		 * Force an XLOG file switch before the checkpoint, to ensure that the
		 * WAL segment the checkpoint is written to doesn't contain pages with
		 * old timeline IDs.  That would otherwise happen if you called
		 * pg_backup_start() right after restoring from a PITR archive: the
		 * first WAL segment containing the startup checkpoint has pages in
		 * the beginning with the old timeline ID.  That can cause trouble at
		 * recovery: we won't have a history file covering the old timeline if
		 * pg_wal directory was not included in the base backup and the WAL
		 * archive was cleared too before starting the backup.
		 *
		 * This also ensures that we have emitted a WAL page header that has
		 * XLP_BKP_REMOVABLE off before we emit the checkpoint record.
		 * Therefore, if a WAL archiver (such as pglesslog) is trying to
		 * compress out removable backup blocks, it won't remove any that
		 * occur after this point.
		 *
		 * During recovery, we skip forcing XLOG file switch, which means that
		 * the backup taken during recovery is not available for the special
		 * recovery case described above.
		 */
		if (!backup_started_in_recovery)
			RequestXLogSwitch(false);

		do
		{
			bool		checkpointfpw;

			/*
			 * Force a CHECKPOINT.  Aside from being necessary to prevent torn
			 * page problems, this guarantees that two successive backup runs
			 * will have different checkpoint positions and hence different
			 * history file names, even if nothing happened in between.
			 *
			 * During recovery, establish a restartpoint if possible. We use
			 * the last restartpoint as the backup starting checkpoint. This
			 * means that two successive backup runs can have same checkpoint
			 * positions.
			 *
			 * Since the fact that we are executing do_pg_backup_start()
			 * during recovery means that checkpointer is running, we can use
			 * RequestCheckpoint() to establish a restartpoint.
			 *
			 * We use CHECKPOINT_IMMEDIATE only if requested by user (via
			 * passing fast = true).  Otherwise this can take awhile.
			 */
			RequestCheckpoint(CHECKPOINT_FORCE | CHECKPOINT_WAIT |
							  (fast ? CHECKPOINT_IMMEDIATE : 0));

			/*
			 * Now we need to fetch the checkpoint record location, and also
			 * its REDO pointer.  The oldest point in WAL that would be needed
			 * to restore starting from the checkpoint is precisely the REDO
			 * pointer.
			 */
			GetCheckpointLocation(&state->checkpointloc, &state->startpoint,
								  &state->starttli, &checkpointfpw);

			if (backup_started_in_recovery)
			{
				XLogRecPtr	recptr;

				/*
				 * Check to see if all WAL replayed during online backup
				 * (i.e., since last restartpoint used as backup starting
				 * checkpoint) contain full-page writes.
				 */
				recptr = XLogGetLastFPWDisableRecptr();

				if (!checkpointfpw || state->startpoint <= recptr)
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							 errmsg("WAL generated with full_page_writes=off was replayed "
									"since last restartpoint"),
							 errhint("This means that the backup being taken on the standby "
									 "is corrupt and should not be used. "
									 "Enable full_page_writes and run CHECKPOINT on the primary, "
									 "and then try an online backup again.")));

				/*
				 * During recovery, since we don't use the end-of-backup WAL
				 * record and don't write the backup history file, the
				 * starting WAL location doesn't need to be unique. This means
				 * that two base backups started at the same time might use
				 * the same checkpoint as starting locations.
				 */
				gotUniqueStartpoint = true;
			}

			/*
			 * If two base backups are started at the same time (in WAL sender
			 * processes), we need to make sure that they use different
			 * checkpoints as starting locations, because we use the starting
			 * WAL location as a unique identifier for the base backup in the
			 * end-of-backup WAL record and when we write the backup history
			 * file. Perhaps it would be better generate a separate unique ID
			 * for each backup instead of forcing another checkpoint, but
			 * taking a checkpoint right after another is not that expensive
			 * either because only few buffers have been dirtied yet.
			 */
			gotUniqueStartpoint = XLogBackupSetLastStart(state->startpoint);
		} while (!gotUniqueStartpoint);

		/*
		 * Construct tablespace_map file.
		 */
		datadirpathlen = strlen(DataDir);

		/* Collect information about all tablespaces */
		tblspcdir = AllocateDir("pg_tblspc");
		while ((de = ReadDir(tblspcdir, "pg_tblspc")) != NULL)
		{
			char		fullpath[MAXPGPATH + 10];
			char		linkpath[MAXPGPATH];
			char	   *relpath = NULL;
			int			rllen;
			StringInfoData escapedpath;
			char	   *s;

			/* Skip anything that doesn't look like a tablespace */
			if (strspn(de->d_name, "0123456789") != strlen(de->d_name))
				continue;

			snprintf(fullpath, sizeof(fullpath), "pg_tblspc/%s", de->d_name);

			/*
			 * Skip anything that isn't a symlink/junction.  For testing only,
			 * we sometimes use allow_in_place_tablespaces to create
			 * directories directly under pg_tblspc, which would fail below.
			 */
			if (get_dirent_type(fullpath, de, false, ERROR) != PGFILETYPE_LNK)
				continue;

			rllen = readlink(fullpath, linkpath, sizeof(linkpath));
			if (rllen < 0)
			{
				ereport(WARNING,
						(errmsg("could not read symbolic link \"%s\": %m",
								fullpath)));
				continue;
			}
			else if (rllen >= sizeof(linkpath))
			{
				ereport(WARNING,
						(errmsg("symbolic link \"%s\" target is too long",
								fullpath)));
				continue;
			}
			linkpath[rllen] = '\0';

			/*
			 * Build a backslash-escaped version of the link path to include
			 * in the tablespace map file.
			 */
			initStringInfo(&escapedpath);
			for (s = linkpath; *s; s++)
			{
				if (*s == '\n' || *s == '\r' || *s == '\\')
					appendStringInfoChar(&escapedpath, '\\');
				appendStringInfoChar(&escapedpath, *s);
			}

			/*
			 * Relpath holds the relative path of the tablespace directory
			 * when it's located within PGDATA, or NULL if it's located
			 * elsewhere.
			 */
			if (rllen > datadirpathlen &&
				strncmp(linkpath, DataDir, datadirpathlen) == 0 &&
				IS_DIR_SEP(linkpath[datadirpathlen]))
				relpath = linkpath + datadirpathlen + 1;

			ti = palloc(sizeof(tablespaceinfo));
			ti->oid = pstrdup(de->d_name);
			ti->path = pstrdup(linkpath);
			ti->rpath = relpath ? pstrdup(relpath) : NULL;
			ti->size = -1;

			if (tablespaces)
				*tablespaces = lappend(*tablespaces, ti);

			appendStringInfo(tblspcmapfile, "%s %s\n",
							 ti->oid, escapedpath.data);

			pfree(escapedpath.data);
		}
		FreeDir(tblspcdir);

		state->starttime = (pg_time_t) time(NULL);
	}
	PG_END_ENSURE_ERROR_CLEANUP(do_pg_abort_backup, DatumGetBool(true));

	state->started_in_recovery = backup_started_in_recovery;

	/*
	 * Mark that the start phase has correctly finished for the backup.
	 */
	sessionBackupState = SESSION_BACKUP_RUNNING;
}

/*
 * Utility routine to fetch the session-level status of a backup running.
 */
SessionBackupState
get_backup_status(void)
{
	return sessionBackupState;
}

/*
 * Utility routine to reset the session-level status of a backup running.
 */
void
reset_backup_status(void)
{
	sessionBackupState = SESSION_BACKUP_NONE;
}

/*
 * do_pg_backup_stop
 *
 * Utility function called at the end of an online backup.  It creates history
 * file (if required), resets sessionBackupState and so on.  It can optionally
 * wait for WAL segments to be archived.
 *
 * "state" is filled with the information necessary to restore from this
 * backup with its stop LSN (stoppoint), its timeline ID (stoptli), etc.
 *
 * It is the responsibility of the caller of this function to verify the
 * permissions of the calling user!
 */
void
do_pg_backup_stop(BackupState *state, bool waitforarchive)
{
	bool		backup_stopped_in_recovery = false;
	char		histfilepath[MAXPGPATH];
	char		lastxlogfilename[MAXFNAMELEN];
	char		histfilename[MAXFNAMELEN];
	XLogSegNo	_logSegNo;
	FILE	   *fp;
	int			seconds_before_warning;
	int			waits = 0;
	bool		reported_waiting = false;

	Assert(state != NULL);

	backup_stopped_in_recovery = RecoveryInProgress();

	/*
	 * During recovery, we don't need to check WAL level. Because, if WAL
	 * level is not sufficient, it's impossible to get here during recovery.
	 */
	if (!backup_stopped_in_recovery && !XLogIsNeeded())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL level not sufficient for making an online backup"),
				 errhint("wal_level must be set to \"replica\" or \"logical\" at server start.")));

	/*
	 * OK to reset backup counter and session-level lock.
	 *
	 * Note that CHECK_FOR_INTERRUPTS() must not occur while updating them,
	 * otherwise they can be updated inconsistently, which might cause
	 * do_pg_abort_backup() to fail.
	 *
	 * It is expected that each do_pg_backup_start() call is matched by
	 * exactly one do_pg_backup_stop() call.
	 */
	XLogBackupResetRunning();

	/*
	 * If we are taking an online backup from the standby, we confirm that the
	 * standby has not been promoted during the backup.
	 */
	if (state->started_in_recovery && !backup_stopped_in_recovery)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("the standby was promoted during online backup"),
				 errhint("This means that the backup being taken is corrupt "
						 "and should not be used. "
						 "Try taking another online backup.")));

	/*
	 * During recovery, we don't write an end-of-backup record. We assume that
	 * pg_control was backed up last and its minimum recovery point can be
	 * available as the backup end location. Since we don't have an
	 * end-of-backup record, we use the pg_control value to check whether
	 * we've reached the end of backup when starting recovery from this
	 * backup. We have no way of checking if pg_control wasn't backed up last
	 * however.
	 *
	 * We don't force a switch to new WAL file but it is still possible to
	 * wait for all the required files to be archived if waitforarchive is
	 * true. This is okay if we use the backup to start a standby and fetch
	 * the missing WAL using streaming replication. But in the case of an
	 * archive recovery, a user should set waitforarchive to true and wait for
	 * them to be archived to ensure that all the required files are
	 * available.
	 *
	 * We return the current minimum recovery point as the backup end
	 * location. Note that it can be greater than the exact backup end
	 * location if the minimum recovery point is updated after the backup of
	 * pg_control. This is harmless for current uses.
	 *
	 * XXX currently a backup history file is for informational and debug
	 * purposes only. It's not essential for an online backup. Furthermore,
	 * even if it's created, it will not be archived during recovery because
	 * an archiver is not invoked. So it doesn't seem worthwhile to write a
	 * backup history file during recovery.
	 */
	if (backup_stopped_in_recovery)
	{
		XLogRecPtr	recptr;

		/*
		 * Check to see if all WAL replayed during online backup contain
		 * full-page writes.
		 */
		recptr = XLogGetLastFPWDisableRecptr();

		if (state->startpoint <= recptr)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("WAL generated with full_page_writes=off was replayed "
							"during online backup"),
					 errhint("This means that the backup being taken on the standby "
							 "is corrupt and should not be used. "
							 "Enable full_page_writes and run CHECKPOINT on the primary, "
							 "and then try an online backup again.")));


		XLogGetMinRecoveryPoint(&state->stoppoint, &state->stoptli);
	}
	else
	{
		char	   *history_file;

		/*
		 * Write the backup-end xlog record
		 */
		XLogBeginInsert();
		XLogRegisterData((char *) (&state->startpoint),
						 sizeof(state->startpoint));
		state->stoppoint = XLogInsert(RM_XLOG_ID, XLOG_BACKUP_END);

		/*
		 * Given that we're not in recovery, InsertTimeLineID is set and can't
		 * change, so we can read it without a lock.
		 */
		state->stoptli = GetWALInsertionTimeLine();

		/*
		 * Force a switch to a new xlog segment file, so that the backup is
		 * valid as soon as archiver moves out the current segment file.
		 */
		RequestXLogSwitch(false);

		state->stoptime = (pg_time_t) time(NULL);

		/*
		 * Write the backup history file
		 */
		XLByteToSeg(state->startpoint, _logSegNo, wal_segment_size);
		BackupHistoryFilePath(histfilepath, state->stoptli, _logSegNo,
							  state->startpoint, wal_segment_size);
		fp = AllocateFile(histfilepath, "w");
		if (!fp)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create file \"%s\": %m",
							histfilepath)));

		/* Build and save the contents of the backup history file */
		history_file = build_backup_content(state, true);
		fprintf(fp, "%s", history_file);
		pfree(history_file);

		if (fflush(fp) || ferror(fp) || FreeFile(fp))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m",
							histfilepath)));

		/*
		 * Clean out any no-longer-needed history files.  As a side effect,
		 * this will post a .ready file for the newly created history file,
		 * notifying the archiver that history file may be archived
		 * immediately.
		 */
		CleanupBackupHistory();
	}

	/*
	 * If archiving is enabled, wait for all the required WAL files to be
	 * archived before returning. If archiving isn't enabled, the required WAL
	 * needs to be transported via streaming replication (hopefully with
	 * wal_keep_size set high enough), or some more exotic mechanism like
	 * polling and copying files from pg_wal with script. We have no knowledge
	 * of those mechanisms, so it's up to the user to ensure that he gets all
	 * the required WAL.
	 *
	 * We wait until both the last WAL file filled during backup and the
	 * history file have been archived, and assume that the alphabetic sorting
	 * property of the WAL files ensures any earlier WAL files are safely
	 * archived as well.
	 *
	 * We wait forever, since archive_command is supposed to work and we
	 * assume the admin wanted his backup to work completely. If you don't
	 * wish to wait, then either waitforarchive should be passed in as false,
	 * or you can set statement_timeout.  Also, some notices are issued to
	 * clue in anyone who might be doing this interactively.
	 */

	if (waitforarchive &&
		((!backup_stopped_in_recovery && XLogArchivingActive()) ||
		 (backup_stopped_in_recovery && XLogArchivingAlways())))
	{
		XLByteToPrevSeg(state->stoppoint, _logSegNo, wal_segment_size);
		XLogFileName(lastxlogfilename, state->stoptli, _logSegNo,
					 wal_segment_size);

		XLByteToSeg(state->startpoint, _logSegNo, wal_segment_size);
		BackupHistoryFileName(histfilename, state->stoptli, _logSegNo,
							  state->startpoint, wal_segment_size);

		seconds_before_warning = 60;
		waits = 0;

		while (XLogArchiveIsBusy(lastxlogfilename) ||
			   XLogArchiveIsBusy(histfilename))
		{
			CHECK_FOR_INTERRUPTS();

			if (!reported_waiting && waits > 5)
			{
				ereport(NOTICE,
						(errmsg("base backup done, waiting for required WAL segments to be archived")));
				reported_waiting = true;
			}

			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 1000L,
							 WAIT_EVENT_BACKUP_WAIT_WAL_ARCHIVE);
			ResetLatch(MyLatch);

			if (++waits >= seconds_before_warning)
			{
				seconds_before_warning *= 2;	/* This wraps in >10 years... */
				ereport(WARNING,
						(errmsg("still waiting for all required WAL segments to be archived (%d seconds elapsed)",
								waits),
						 errhint("Check that your archive_command is executing properly.  "
								 "You can safely cancel this backup, "
								 "but the database backup will not be usable without all the WAL segments.")));
			}
		}

		ereport(NOTICE,
				(errmsg("all required WAL segments have been archived")));
	}
	else if (waitforarchive)
		ereport(NOTICE,
				(errmsg("WAL archiving is not enabled; you must ensure that all required WAL segments are copied through other means to complete the backup")));
}

/*
 * do_pg_abort_backup: abort a running backup
 *
 * This does just the most basic steps of do_pg_backup_stop(), by taking the
 * system out of backup mode, thus making it a lot more safe to call from
 * an error handler.
 *
 * 'arg' indicates that it's being called during backup setup; so
 * sessionBackupState has not been modified yet, but runningBackups has
 * already been incremented.  When it's false, then it's invoked as a
 * before_shmem_exit handler, and therefore we must not change state
 * unless sessionBackupState indicates that a backup is actually running.
 *
 * NB: This gets used as a PG_ENSURE_ERROR_CLEANUP callback and
 * before_shmem_exit handler, hence the odd-looking signature.
 */
void
do_pg_abort_backup(int code, Datum arg)
{
	bool		during_backup_start = DatumGetBool(arg);

	/* If called during backup start, there shouldn't be one already running */
	Assert(!during_backup_start || sessionBackupState == SESSION_BACKUP_NONE);

	if (during_backup_start || sessionBackupState != SESSION_BACKUP_NONE)
	{
		XLogBackupResetRunning();

		if (!during_backup_start)
			ereport(WARNING,
					errmsg("aborting backup due to backend exiting before pg_backup_stop was called"));
	}
}

/*
 * Register a handler that will warn about unterminated backups at end of
 * session, unless this has already been done.
 */
void
register_persistent_abort_backup_handler(void)
{
	static bool already_done = false;

	if (already_done)
		return;
	before_shmem_exit(do_pg_abort_backup, DatumGetBool(false));
	already_done = true;
}
