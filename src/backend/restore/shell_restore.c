/*-------------------------------------------------------------------------
 *
 * shell_restore.c
 *
 * These restore functions use a user-specified shell command (e.g., the
 * restore_command GUC).  It is used as the default, but other modules may
 * define their own restore logic.
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/restore/shell_restore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>

#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "access/xlog_internal.h"
#include "common/archive.h"
#include "common/percentrepl.h"
#include "postmaster/startup.h"
#include "restore/restore_module.h"
#include "restore/shell_restore.h"
#include "storage/ipc.h"
#include "utils/wait_event.h"

static void shell_restore_startup(RestoreModuleState *state);
static bool shell_restore_file_configured(RestoreModuleState *state);
static bool shell_restore_file(const char *file, const char *path,
							   const char *lastRestartPointFileName);
static bool shell_restore_wal_segment(RestoreModuleState *state,
									  const char *file, const char *path,
									  const char *lastRestartPointFileName);
static bool shell_restore_timeline_history(RestoreModuleState *state,
										   const char *file, const char *path);
static bool shell_archive_cleanup_configured(RestoreModuleState *state);
static void shell_archive_cleanup(RestoreModuleState *state,
								  const char *lastRestartPointFileName);
static bool shell_recovery_end_configured(RestoreModuleState *state);
static void shell_recovery_end(RestoreModuleState *state,
							   const char *lastRestartPointFileName);
static void ExecuteRecoveryCommand(const char *command,
								   const char *commandName,
								   bool failOnSignal,
								   uint32 wait_event_info,
								   const char *lastRestartPointFileName);

static const RestoreModuleCallbacks shell_restore_callbacks = {
	.startup_cb = shell_restore_startup,
	.restore_wal_segment_configured_cb = shell_restore_file_configured,
	.restore_wal_segment_cb = shell_restore_wal_segment,
	.restore_timeline_history_configured_cb = shell_restore_file_configured,
	.restore_timeline_history_cb = shell_restore_timeline_history,
	.timeline_history_exists_configured_cb = shell_restore_file_configured,
	.timeline_history_exists_cb = shell_restore_timeline_history,
	.archive_cleanup_configured_cb = shell_archive_cleanup_configured,
	.archive_cleanup_cb = shell_archive_cleanup,
	.recovery_end_configured_cb = shell_recovery_end_configured,
	.recovery_end_cb = shell_recovery_end,
	.shutdown_cb = NULL
};

/*
 * shell_restore_init
 *
 * Returns the callbacks for restoring via shell.
 */
const RestoreModuleCallbacks *
shell_restore_init(void)
{
	return &shell_restore_callbacks;
}

/*
 * Indicates that our timeline_history_exists_cb only attempts to retrieve the
 * file, so the caller should verify the file exists afterwards.
 */
static void
shell_restore_startup(RestoreModuleState *state)
{
	state->timeline_history_exists_cb_copies = true;
}

/*
 * Attempt to execute a shell-based restore command to retrieve a WAL segment.
 *
 * Returns true if the command has succeeded, false otherwise.
 */
static bool
shell_restore_wal_segment(RestoreModuleState *state,
						  const char *file, const char *path,
						  const char *lastRestartPointFileName)
{
	return shell_restore_file(file, path, lastRestartPointFileName);
}

/*
 * Attempt to execute a shell-based restore command to retrieve a timeline
 * history file.
 *
 * Returns true if the command has succeeded, false otherwise.
 */
static bool
shell_restore_timeline_history(RestoreModuleState *state,
							   const char *file, const char *path)
{
	char		lastRestartPointFname[MAXPGPATH];

	XLogFileName(lastRestartPointFname, 0, 0L, wal_segment_size);
	return shell_restore_file(file, path, lastRestartPointFname);
}

/*
 * Check whether restore_command is supplied.
 */
static bool
shell_restore_file_configured(RestoreModuleState *state)
{
	return recoveryRestoreCommand[0] != '\0';
}

/*
 * Attempt to execute a shell-based restore command to retrieve a file.
 *
 * Returns true if the command has succeeded, false otherwise.
 */
static bool
shell_restore_file(const char *file, const char *path,
				   const char *lastRestartPointFileName)
{
	char	   *cmd;
	int			rc;

	/* Build the restore command to execute */
	cmd = BuildRestoreCommand(recoveryRestoreCommand, path, file,
							  lastRestartPointFileName);

	ereport(DEBUG3,
			(errmsg_internal("executing restore command \"%s\"", cmd)));

	fflush(NULL);
	pgstat_report_wait_start(WAIT_EVENT_RESTORE_COMMAND);

	/*
	 * Check signals before restore command and reset afterwards.
	 * PreRestoreCommand() informs the SIGTERM handler for the startup process
	 * that it should proc_exit() right away.  This is done for the duration of
	 * the system() call because there isn't a good way to break out while it
	 * is executing.  Since we might call proc_exit() in a signal handler, it
	 * is best to put any additional logic before or after the
	 * PreRestoreCommand()/PostRestoreCommand() section.
	 */
	PreRestoreCommand();

	/*
	 * Copy xlog from archival storage to XLOGDIR
	 */
	rc = system(cmd);

	PostRestoreCommand();

	pgstat_report_wait_end();
	pfree(cmd);

	/*
	 * Remember, we rollforward UNTIL the restore fails so failure here is
	 * just part of the process... that makes it difficult to determine
	 * whether the restore failed because there isn't an archive to restore,
	 * or because the administrator has specified the restore program
	 * incorrectly.  We have to assume the former.
	 *
	 * However, if the failure was due to any sort of signal, it's best to
	 * punt and abort recovery.  (If we "return false" here, upper levels will
	 * assume that recovery is complete and start up the database!) It's
	 * essential to abort on child SIGINT and SIGQUIT, because per spec
	 * system() ignores SIGINT and SIGQUIT while waiting; if we see one of
	 * those it's a good bet we should have gotten it too.
	 *
	 * On SIGTERM, assume we have received a fast shutdown request, and exit
	 * cleanly. It's pure chance whether we receive the SIGTERM first, or the
	 * child process. If we receive it first, the signal handler will call
	 * proc_exit, otherwise we do it here. If we or the child process received
	 * SIGTERM for any other reason than a fast shutdown request, postmaster
	 * will perform an immediate shutdown when it sees us exiting
	 * unexpectedly.
	 *
	 * We treat hard shell errors such as "command not found" as fatal, too.
	 */
	if (rc != 0)
	{
		if (wait_result_is_signal(rc, SIGTERM))
			proc_exit(1);

		ereport(wait_result_is_any_signal(rc, true) ? FATAL : DEBUG2,
				(errmsg("could not restore file \"%s\" from archive: %s",
						file, wait_result_to_str(rc))));
	}

	return (rc == 0);
}

/*
 * Check whether archive_cleanup_command is supplied.
 */
static bool
shell_archive_cleanup_configured(RestoreModuleState *state)
{
	return archiveCleanupCommand[0] != '\0';
}

/*
 * Attempt to execute a shell-based archive cleanup command.
 */
static void
shell_archive_cleanup(RestoreModuleState *state,
					  const char *lastRestartPointFileName)
{
	ExecuteRecoveryCommand(archiveCleanupCommand, "archive_cleanup_command",
						   false, WAIT_EVENT_ARCHIVE_CLEANUP_COMMAND,
						   lastRestartPointFileName);
}

/*
 * Check whether recovery_end_command is supplied.
 */
static bool
shell_recovery_end_configured(RestoreModuleState *state)
{
	return recoveryEndCommand[0] != '\0';
}

/*
 * Attempt to execute a shell-based end-of-recovery command.
 */
static void
shell_recovery_end(RestoreModuleState *state,
				   const char *lastRestartPointFileName)
{
	ExecuteRecoveryCommand(recoveryEndCommand, "recovery_end_command", true,
						   WAIT_EVENT_RECOVERY_END_COMMAND,
						   lastRestartPointFileName);
}

/*
 * Attempt to execute an external shell command during recovery.
 *
 * 'command' is the shell command to be executed, 'commandName' is a
 * human-readable name describing the command emitted in the logs. If
 * 'failOnSignal' is true and the command is killed by a signal, a FATAL
 * error is thrown. Otherwise a WARNING is emitted.
 *
 * This is currently used for recovery_end_command and archive_cleanup_command.
 */
static void
ExecuteRecoveryCommand(const char *command, const char *commandName,
					   bool failOnSignal, uint32 wait_event_info,
					   const char *lastRestartPointFileName)
{
	char	   *xlogRecoveryCmd;
	int			rc;

	Assert(command && commandName);

	/*
	 * construct the command to be executed
	 */
	xlogRecoveryCmd = replace_percent_placeholders(command, commandName, "r",
												   lastRestartPointFileName);

	ereport(DEBUG3,
			(errmsg_internal("executing %s \"%s\"", commandName, command)));

	/*
	 * execute the constructed command
	 */
	fflush(NULL);
	pgstat_report_wait_start(wait_event_info);
	rc = system(xlogRecoveryCmd);
	pgstat_report_wait_end();

	pfree(xlogRecoveryCmd);

	if (rc != 0)
	{
		/*
		 * If the failure was due to any sort of signal, it's best to punt and
		 * abort recovery.  See comments in shell_restore().
		 */
		ereport((failOnSignal && wait_result_is_any_signal(rc, true)) ? FATAL : WARNING,
		/*------
		   translator: First %s represents a postgresql.conf parameter name like
		  "recovery_end_command", the 2nd is the value of that parameter, the
		  third an already translated error message. */
				(errmsg("%s \"%s\": %s", commandName,
						command, wait_result_to_str(rc))));
	}
}
