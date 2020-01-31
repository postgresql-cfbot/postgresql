/*-------------------------------------------------------------------------
 *
 * fe_archive.c
 *	  Routines to access WAL archive from frontend
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/common/fe_archive.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <unistd.h>
#include <sys/stat.h>

#include "access/xlog_internal.h"
#include "common/archive.h"
#include "common/fe_archive.h"
#include "common/logging.h"


/* logging support */
#define pg_fatal(...) do { pg_log_fatal(__VA_ARGS__); exit(1); } while(0)

/*
 * Attempt to retrieve the specified file from off-line archival storage.
 * If successful return a file descriptor of the restored WAL file, else
 * return -1.
 *
 * For fixed-size files, the caller may pass the expected size as an
 * additional crosscheck on successful recovery.  If the file size is not
 * known, set expectedSize = 0.
 */
int
RestoreArchivedWALFile(const char *path, const char *xlogfname,
					   off_t expectedSize, const char *restoreCommand)
{
	char		xlogpath[MAXPGPATH],
				xlogRestoreCmd[MAXPGPATH];
	int			rc,
				xlogfd;
	struct stat stat_buf;

	snprintf(xlogpath, MAXPGPATH, "%s/" XLOGDIR "/%s", path, xlogfname);

	rc = ConstructRestoreCommand(restoreCommand, xlogpath, xlogfname, NULL, xlogRestoreCmd);

	if (rc < 0)
		pg_fatal("restore_command with %%r alias cannot be used.");

	/*
	 * Execute restore_command, which should copy the missing WAL file from
	 * archival storage.
	 */
	rc = system(xlogRestoreCmd);

	if (rc == 0)
	{
		/*
		 * Command apparently succeeded, but let's make sure the file is
		 * really there now and has the correct size.
		 */
		if (stat(xlogpath, &stat_buf) == 0)
		{
			if (expectedSize > 0 && stat_buf.st_size != expectedSize)
			{
				pg_log_error("archive file \"%s\" has wrong size: %lu instead of %lu, %s",
							 xlogfname, (unsigned long) stat_buf.st_size,
							 (unsigned long) expectedSize, strerror(errno));
			}
			else
			{
				xlogfd = open(xlogpath, O_RDONLY | PG_BINARY, 0);

				if (xlogfd < 0)
					pg_log_error("could not open file \"%s\" restored from archive: %s\n",
								 xlogpath, strerror(errno));
				else
					return xlogfd;
			}
		}
		else
		{
			/* Stat failed */
			pg_log_error("could not stat file \"%s\" restored from archive: %s",
						 xlogpath, strerror(errno));
		}
	}

	/*
	 * If the failure was due to any sort of signal, then it will be
	 * misleading to return message 'could not restore file...' and propagate
	 * result to the upper levels. We should exit right now.
	 */
	if (wait_result_is_any_signal(rc, false))
		pg_fatal("restore_command failed due to the signal: %s",
				 wait_result_to_str(rc));

	pg_log_error("could not restore file \"%s\" from archive\n",
				 xlogfname);
	return -1;
}
