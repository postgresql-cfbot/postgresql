/*-------------------------------------------------------------------------
 *
 * controldata_utils.c
 *		Common code for control data file output.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/controldata_utils.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>

#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "common/controldata_utils.h"
#include "common/file_perm.h"
#ifdef FRONTEND
#include "common/logging.h"
#endif
#include "port/pg_crc32c.h"

#ifndef FRONTEND
#include "pgstat.h"
#include "storage/fd.h"
#endif

/*
 * Lock the control file until closed.  This is used for the benefit of
 * frontend/external programs that want to take an atomic copy of the control
 * file, when though it might be written to concurrently by the server.
 * On some systems including Windows NTFS and Linux ext4, that might otherwise
 * cause a torn read.
 *
 * (The server also reads and writes the control file directly sometimes, not
 * through these routines; that's OK, because it uses ControlFileLock, or no
 * locking when it expects no concurrent writes.)
 */
static int
lock_controlfile(int fd, bool exclusive)
{
#ifdef WIN32
	OVERLAPPED	overlapped = {.Offset = PG_CONTROL_FILE_SIZE};
	HANDLE		handle;

	handle = (HANDLE) _get_osfhandle(fd);
	if (handle == INVALID_HANDLE_VALUE)
	{
		errno = EBADF;
		return -1;
	}

	/*
	 * On Windows, we lock the first byte past the end of the control file (see
	 * overlapped.Offset).  This means that we shouldn't cause errors in
	 * external tools that just want to read the file, but we can block other
	 * processes using this routine or that know about this protocol.  This
	 * provides an approximation of Unix's "advisory" locking.
	 */
	if (!LockFileEx(handle,
					exclusive ? LOCKFILE_EXCLUSIVE_LOCK : 0,
					0,
					1,
					0,
					&overlapped))
	{
		_dosmaperr(GetLastError());
		return -1;
	}

	return 0;
#else
	struct flock lock;
	int			rc;

	memset(&lock, 0, sizeof(lock));
	lock.l_type = exclusive ? F_WRLCK : F_RDLCK;
	lock.l_start = 0;
	lock.l_whence = SEEK_SET;
	lock.l_len = 0;
	lock.l_pid = -1;

	do
	{
		rc = fcntl(fd, F_SETLKW, &lock);
	}
	while (rc < 0 && errno == EINTR);

	return rc;
#endif
}

#ifdef WIN32
/*
 * Release lock acquire with lock_controlfile().  On POSIX systems, we don't
 * bother making an extra system call to release the lock, since it'll be
 * released on close anyway.  On Windows, explicit release is recommended by
 * the documentation to make sure it is done synchronously.
 */
static int
unlock_controlfile(int fd)
{
	OVERLAPPED	overlapped = {.Offset = PG_CONTROL_FILE_SIZE};
	HANDLE		handle;

	handle = (HANDLE) _get_osfhandle(fd);
	if (handle == INVALID_HANDLE_VALUE)
	{
		errno = EBADF;
		return -1;
	}

	/* Unlock first byte. */
	if (!UnlockFileEx(handle, 0, 1, 0, &overlapped))
	{
		_dosmaperr(GetLastError());
		return -1;
	}

	return 0;
}
#endif

/*
 * get_controlfile()
 *
 * Get controlfile values.  The result is returned as a palloc'd copy of the
 * control file data.
 *
 * crc_ok_p can be used by the caller to see whether the CRC of the control
 * file data is correct.
 */
ControlFileData *
get_controlfile(const char *DataDir, bool *crc_ok_p)
{
	ControlFileData *ControlFile;
	int			fd;
	char		ControlFilePath[MAXPGPATH];
	pg_crc32c	crc;
	int			r;

	Assert(crc_ok_p);

	ControlFile = palloc_object(ControlFileData);
	snprintf(ControlFilePath, MAXPGPATH, "%s/global/pg_control", DataDir);

#ifndef FRONTEND
	if ((fd = OpenTransientFile(ControlFilePath, O_RDONLY | PG_BINARY)) == -1)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\" for reading: %m",
						ControlFilePath)));
#else
	if ((fd = open(ControlFilePath, O_RDONLY | PG_BINARY, 0)) == -1)
		pg_fatal("could not open file \"%s\" for reading: %m",
				 ControlFilePath);
#endif

	/* Make sure we can read the file atomically. */
	if (lock_controlfile(fd, false) < 0)
	{
#ifndef FRONTEND
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not lock file \"%s\" for reading: %m",
						ControlFilePath)));
#else
		pg_fatal("could not lock file \"%s\" for reading: %m",
				 ControlFilePath);
#endif
	}

	r = read(fd, ControlFile, sizeof(ControlFileData));
	if (r != sizeof(ControlFileData))
	{
		if (r < 0)
#ifndef FRONTEND
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", ControlFilePath)));
#else
			pg_fatal("could not read file \"%s\": %m", ControlFilePath);
#endif
		else
#ifndef FRONTEND
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read file \"%s\": read %d of %zu",
							ControlFilePath, r, sizeof(ControlFileData))));
#else
			pg_fatal("could not read file \"%s\": read %d of %zu",
					 ControlFilePath, r, sizeof(ControlFileData));
#endif
	}

#ifdef WIN32
	unlock_controlfile(fd);
#endif

#ifndef FRONTEND
	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m",
						ControlFilePath)));
#else
	if (close(fd) != 0)
		pg_fatal("could not close file \"%s\": %m", ControlFilePath);
#endif

	/* Check the CRC. */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc,
				(char *) ControlFile,
				offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);

	*crc_ok_p = EQ_CRC32C(crc, ControlFile->crc);

	/* Make sure the control file is valid byte order. */
	if (ControlFile->pg_control_version % 65536 == 0 &&
		ControlFile->pg_control_version / 65536 != 0)
#ifndef FRONTEND
		elog(ERROR, _("byte ordering mismatch"));
#else
		pg_log_warning("possible byte ordering mismatch\n"
					   "The byte ordering used to store the pg_control file might not match the one\n"
					   "used by this program.  In that case the results below would be incorrect, and\n"
					   "the PostgreSQL installation would be incompatible with this data directory.");
#endif

	return ControlFile;
}

/*
 * update_controlfile()
 *
 * Update controlfile values with the contents given by caller.  The
 * contents to write are included in "ControlFile".  "sync_op" can be set
 * to 0 or a synchronization method UPDATE_CONTROLFILE_XXX.  In frontend code,
 * only 0 and non-zero (fsync) are meaningful.
 * Note that it is up to the caller to properly lock ControlFileLock when
 * calling this routine in the backend.
 */
void
update_controlfile(const char *DataDir,
				   ControlFileData *ControlFile,
				   int sync_op)
{
	int			fd;
	char		buffer[PG_CONTROL_FILE_SIZE];
	char		ControlFilePath[MAXPGPATH];
	int			open_flag;

	switch (sync_op)
	{
#ifndef FRONTEND
#ifdef O_SYNC
		case UPDATE_CONTROLFILE_O_SYNC:
			open_flag = O_SYNC;
			break;
#endif
#ifdef O_DSYNC
		case UPDATE_CONTROLFILE_O_DSYNC:
			open_flag = O_DSYNC;
			break;
#endif
#endif
		default:
			open_flag = 0;
	}

	/* Update timestamp  */
	ControlFile->time = (pg_time_t) time(NULL);

	/* Recalculate CRC of control file */
	INIT_CRC32C(ControlFile->crc);
	COMP_CRC32C(ControlFile->crc,
				(char *) ControlFile,
				offsetof(ControlFileData, crc));
	FIN_CRC32C(ControlFile->crc);

	/*
	 * Write out PG_CONTROL_FILE_SIZE bytes into pg_control by zero-padding
	 * the excess over sizeof(ControlFileData), to avoid premature EOF related
	 * errors when reading it.
	 */
	memset(buffer, 0, PG_CONTROL_FILE_SIZE);
	memcpy(buffer, ControlFile, sizeof(ControlFileData));

	snprintf(ControlFilePath, sizeof(ControlFilePath), "%s/%s", DataDir, XLOG_CONTROL_FILE);

#ifndef FRONTEND

	/*
	 * All errors issue a PANIC, so no need to use OpenTransientFile() and to
	 * worry about file descriptor leaks.
	 */
	if ((fd = BasicOpenFile(ControlFilePath, O_RDWR | PG_BINARY | open_flag)) < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						ControlFilePath)));
#else
	if ((fd = open(ControlFilePath, O_WRONLY | PG_BINARY | open_flag,
				   pg_file_create_mode)) == -1)
		pg_fatal("could not open file \"%s\": %m", ControlFilePath);
#endif

	/*
	 * Make sure that any concurrent reader (including frontend programs) can
	 * read the file atomically.  Note that this refers to atomicity of
	 * concurrent reads and writes.  For our assumption of atomicity under
	 * power failure, see PG_CONTROL_MAX_SAFE_SIZE.
	 */
	if (lock_controlfile(fd, true) < 0)
	{
#ifndef FRONTEND
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not lock file \"%s\" for writing: %m",
						ControlFilePath)));
#else
		pg_fatal("could not lock file \"%s\" for writing: %m",
				 ControlFilePath);
#endif
	}

	errno = 0;
#ifndef FRONTEND
	pgstat_report_wait_start(WAIT_EVENT_CONTROL_FILE_WRITE_UPDATE);
#endif
	if (write(fd, buffer, PG_CONTROL_FILE_SIZE) != PG_CONTROL_FILE_SIZE)
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;

#ifndef FRONTEND
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m",
						ControlFilePath)));
#else
		pg_fatal("could not write file \"%s\": %m", ControlFilePath);
#endif
	}
#ifndef FRONTEND
	pgstat_report_wait_end();
#endif

	if (sync_op != 0)
	{
#ifndef FRONTEND
		pgstat_report_wait_start(WAIT_EVENT_CONTROL_FILE_SYNC_UPDATE);
		if (sync_op == UPDATE_CONTROLFILE_FDATASYNC)
		{
			if (fdatasync(fd) != 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not fdatasync file \"%s\": %m",
								ControlFilePath)));
		}
		else if (sync_op == UPDATE_CONTROLFILE_FSYNC)
		{
			if (pg_fsync(fd) != 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not fdatasync file \"%s\": %m",
								ControlFilePath)));
		}
		pgstat_report_wait_end();
#else
		/* In frontend code, non-zero sync_op gets you plain fsync(). */
		if (fsync(fd) != 0)
			pg_fatal("could not fsync file \"%s\": %m", ControlFilePath);
#endif
	}

#ifdef WIN32
	unlock_controlfile(fd);
#endif

	if (close(fd) != 0)
	{
#ifndef FRONTEND
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m",
						ControlFilePath)));
#else
		pg_fatal("could not close file \"%s\": %m", ControlFilePath);
#endif
	}
}
