/*-------------------------------------------------------------------------
 *
 * copydir.c
 *	  copies a directory
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	While "xcopy /e /i /q" works fine for copying directories, on Windows XP
 *	it requires a Window handle which prevents it from working when invoked
 *	as a service.
 *
 * IDENTIFICATION
 *	  src/backend/storage/file/copydir.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "miscadmin.h"
#include "pgstat.h"
#include "storage/buffile.h"
#include "storage/copydir.h"
#include "storage/fd.h"

/*
 * copydir: copy a directory
 *
 * If recurse is false, subdirectories are ignored.  Anything that's not
 * a directory or a regular file is ignored.
 */
void
copydir(char *fromdir, char *todir, bool recurse)
{
	DIR		   *xldir;
	struct dirent *xlde;
	char		fromfile[MAXPGPATH * 2];
	char		tofile[MAXPGPATH * 2];

	if (MakePGDirectory(todir) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\": %m", todir)));

	xldir = AllocateDir(fromdir);

	while ((xlde = ReadDir(xldir, fromdir)) != NULL)
	{
		struct stat fst;

		/* If we got a cancel signal during the copy of the directory, quit */
		CHECK_FOR_INTERRUPTS();

		if (strcmp(xlde->d_name, ".") == 0 ||
			strcmp(xlde->d_name, "..") == 0)
			continue;

		snprintf(fromfile, sizeof(fromfile), "%s/%s", fromdir, xlde->d_name);
		snprintf(tofile, sizeof(tofile), "%s/%s", todir, xlde->d_name);

		if (lstat(fromfile, &fst) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m", fromfile)));

		if (S_ISDIR(fst.st_mode))
		{
			/* recurse to handle subdirectories */
			if (recurse)
				copydir(fromfile, tofile, true);
		}
		else if (S_ISREG(fst.st_mode))
			copy_file(fromfile, tofile);
	}
	FreeDir(xldir);

	/*
	 * Be paranoid here and fsync all files to ensure the copy is really done.
	 * But if fsync is disabled, we're done.
	 */
	if (!enableFsync)
		return;

	xldir = AllocateDir(todir);

	while ((xlde = ReadDir(xldir, todir)) != NULL)
	{
		struct stat fst;

		if (strcmp(xlde->d_name, ".") == 0 ||
			strcmp(xlde->d_name, "..") == 0)
			continue;

		snprintf(tofile, sizeof(tofile), "%s/%s", todir, xlde->d_name);

		/*
		 * We don't need to sync subdirectories here since the recursive
		 * copydir will do it before it returns
		 */
		if (lstat(tofile, &fst) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m", tofile)));

		if (S_ISREG(fst.st_mode))
			fsync_fname(tofile, false);
	}
	FreeDir(xldir);

	/*
	 * It's important to fsync the destination directory itself as individual
	 * file fsyncs don't guarantee that the directory entry for the file is
	 * synced. Recent versions of ext4 have made the window much wider but
	 * it's been true for ext3 and other filesystems in the past.
	 */
	fsync_fname(todir, true);
}

/*
 * copy one file
 */
void
copy_file(char *fromfile, char *tofile)
{
	BufFileCopyFDArg src,
				dst;

	/* Size of copy buffer (read and write requests) */
#define COPY_BUF_SIZE (8 * BLCKSZ)

	/*
	 * Size of data flush requests.  It seems beneficial on most platforms to
	 * do this every 1MB or so.  But macOS, at least with early releases of
	 * APFS, is really unfriendly to small mmap/msync requests, so there do it
	 * only every 32MB.
	 */
#if defined(__darwin__)
#define FLUSH_DISTANCE (32 * 1024 * 1024)
#else
#define FLUSH_DISTANCE (1024 * 1024)
#endif

	/*
	 * Open the files
	 */
	src.path = fromfile;
	src.fd = OpenTransientFile(fromfile, O_RDONLY | PG_BINARY);
	if (src.fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", fromfile)));
	src.wait_event_info = WAIT_EVENT_COPY_FILE_READ;

	dst.path = tofile;
	dst.fd = OpenTransientFile(tofile, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
	if (dst.fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", tofile)));
	dst.wait_event_info = WAIT_EVENT_COPY_FILE_WRITE;

	/*
	 * Do the data copying.
	 */
	BufFileCopyFD(&dst, &src, COPY_BUF_SIZE, 0, FLUSH_DISTANCE, false);

	if (CloseTransientFile(dst.fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", tofile)));

	if (CloseTransientFile(src.fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", fromfile)));
}
