/*-------------------------------------------------------------------------
 *
 * shell_archive.c
 *
 * This archiving function uses a user-specified shell command (the
 * archive_command GUC) to copy write-ahead log files.  It is used as the
 * default, but other modules may define their own custom archiving logic.
 *
 * Copyright (c) 2022-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/shell_archive.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/wait.h>

#include "access/xlog.h"
#include "common/percentrepl.h"
#include "pgstat.h"
#include "postmaster/archive_module.h"

static bool shell_archive_configured(void *arg);
static bool shell_archive_file(const char *file, const char *path, void *arg);
static void shell_archive_shutdown(void *arg);

static const ArchiveModuleCallbacks shell_archive_callbacks = {
	.check_configured_cb = shell_archive_configured,
	.archive_file_cb = shell_archive_file,
	.shutdown_cb = shell_archive_shutdown
};

const ArchiveModuleCallbacks *
shell_archive_init(void **arg)
{
	AssertVariableIsOfType(&shell_archive_init, ArchiveModuleInit);

	return &shell_archive_callbacks;
}

static bool
shell_archive_configured(void *arg)
{
	return XLogArchiveCommand[0] != '\0';
}

static bool
shell_archive_file(const char *file, const char *path, void *arg)
{
	char	   *xlogarchcmd;
	char	   *nativePath = NULL;
	int			rc;

	if (path)
	{
		nativePath = pstrdup(path);
		make_native_path(nativePath);
	}

	xlogarchcmd = replace_percent_placeholders(XLogArchiveCommand, "archive_command", "fp", file, nativePath);

	if (nativePath)
		pfree(nativePath);

	ereport(DEBUG3,
			(errmsg_internal("executing archive command \"%s\"",
							 xlogarchcmd)));

	fflush(NULL);
	pgstat_report_wait_start(WAIT_EVENT_ARCHIVE_COMMAND);
	rc = system(xlogarchcmd);
	pgstat_report_wait_end();

	if (rc != 0)
	{
		/*
		 * If either the shell itself, or a called command, died on a signal,
		 * abort the archiver.  We do this because system() ignores SIGINT and
		 * SIGQUIT while waiting; so a signal is very likely something that
		 * should have interrupted us too.  Also die if the shell got a hard
		 * "command not found" type of error.  If we overreact it's no big
		 * deal, the postmaster will just start the archiver again.
		 */
		int			lev = wait_result_is_any_signal(rc, true) ? FATAL : LOG;

		if (WIFEXITED(rc))
		{
			ereport(lev,
					(errmsg("archive command failed with exit code %d",
							WEXITSTATUS(rc)),
					 errdetail("The failed archive command was: %s",
							   xlogarchcmd)));
		}
		else if (WIFSIGNALED(rc))
		{
#if defined(WIN32)
			ereport(lev,
					(errmsg("archive command was terminated by exception 0x%X",
							WTERMSIG(rc)),
					 errhint("See C include file \"ntstatus.h\" for a description of the hexadecimal value."),
					 errdetail("The failed archive command was: %s",
							   xlogarchcmd)));
#else
			ereport(lev,
					(errmsg("archive command was terminated by signal %d: %s",
							WTERMSIG(rc), pg_strsignal(WTERMSIG(rc))),
					 errdetail("The failed archive command was: %s",
							   xlogarchcmd)));
#endif
		}
		else
		{
			ereport(lev,
					(errmsg("archive command exited with unrecognized status %d",
							rc),
					 errdetail("The failed archive command was: %s",
							   xlogarchcmd)));
		}

		return false;
	}

	pfree(xlogarchcmd);

	elog(DEBUG1, "archived write-ahead log file \"%s\"", file);
	return true;
}

static void
shell_archive_shutdown(void *arg)
{
	elog(DEBUG1, "archiver process shutting down");
}
