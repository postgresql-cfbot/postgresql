/*-------------------------------------------------------------------------
 *
 * copydir.c
 *	  copies a directory
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
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

#include "storage/copydir.h"
#include "storage/encryption.h"
#include "storage/fd.h"
#include "storage/reinit.h"
#include "miscadmin.h"
#include "pgstat.h"

/*
 * copydir: copy a directory
 *
 * RelFileNode values must specify tablespace and database oids for source
 * and target to support re-encryption if necessary. relNode value in provided
 * structs will be clobbered.
 */
void
copydir(char *fromdir, char *todir, RelFileNode *fromNode, RelFileNode *toNode)
{
	DIR		   *xldir;
	struct dirent *xlde;
	char		fromfile[MAXPGPATH * 2];
	char		tofile[MAXPGPATH * 2];

	Assert(!data_encrypted || (fromNode != NULL && toNode != NULL));

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

		if (S_ISREG(fst.st_mode))
		{
			int			oidchars;
			ForkNumber	forkNum;
			int			segment;

			/*
			 * For encrypted databases we need to reencrypt files with new
			 * tweaks.
			 */
			if (data_encrypted &&
				parse_filename_for_nontemp_relation(xlde->d_name,
													&oidchars, &forkNum, &segment))
			{
				char		oidbuf[OIDCHARS + 1];

				memcpy(oidbuf, xlde->d_name, oidchars);
				oidbuf[oidchars] = '\0';

				/* We scribble over the provided RelFileNodes here */
				fromNode->relNode = toNode->relNode = atol(oidbuf);
				copy_file(fromfile, tofile, fromNode, toNode, forkNum, forkNum, segment);
			}
			else
				copy_file(fromfile, tofile, NULL, NULL, 0, 0, 0);
		}
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
 * copy one file. If decryption and reencryption may be needed specify
 * relfilenodes for source and target.
 */
void
copy_file(char *fromfile, char *tofile, RelFileNode *fromNode,
		  RelFileNode *toNode, ForkNumber fromForkNum, ForkNumber toForkNum,
		  int segment)
{
	char	   *buffer;
	int			srcfd;
	int			dstfd;
	int			nbytes;
	int			bytesread;
	off_t		offset;
	off_t		flush_offset;
	BlockNumber blockNum = segment * RELSEG_SIZE;

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

	/* Use palloc to ensure we get a maxaligned buffer */
	buffer = palloc(COPY_BUF_SIZE);

	/*
	 * Open the files
	 */
	srcfd = OpenTransientFile(fromfile, O_RDONLY | PG_BINARY);
	if (srcfd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", fromfile)));

	dstfd = OpenTransientFile(tofile, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
	if (dstfd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", tofile)));

	/*
	 * Do the data copying.
	 */
	flush_offset = 0;
	for (offset = 0;; offset += nbytes)
	{
		/* If we got a cancel signal during the copy of the file, quit */
		CHECK_FOR_INTERRUPTS();

		/*
		 * We fsync the files later, but during the copy, flush them every so
		 * often to avoid spamming the cache and hopefully get the kernel to
		 * start writing them out before the fsync comes.
		 */
		if (offset - flush_offset >= FLUSH_DISTANCE)
		{
			pg_flush_data(dstfd, flush_offset, offset - flush_offset);
			flush_offset = offset;
		}

		/*
		 * Try to read as much as we fit in the buffer so we can deal with
		 * complete blocks if we need to reencrypt.
		 */
		nbytes = 0;
		while (nbytes < COPY_BUF_SIZE)
		{
			pgstat_report_wait_start(WAIT_EVENT_COPY_FILE_READ);
			bytesread = read(srcfd, buffer + nbytes, COPY_BUF_SIZE - nbytes);
			pgstat_report_wait_end();
			if (bytesread < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read file \"%s\": %m", fromfile)));
			nbytes += bytesread;
			if (bytesread == 0)
				break;
		}
		if (nbytes == 0)
			break;

		/*
		 * If the database is encrypted we need to decrypt the data here and
		 * reencrypt it to adjust the tweak values of blocks.
		 */
		if (data_encrypted)
		{
			if (fromNode != NULL)
			{
				Assert(toNode != NULL);

				/*
				 * There's no reason not to have whole number of pages read
				 * Computation of the number of blocks below relies on this
				 * fact.
				 */
				Assert(nbytes % BLCKSZ == 0);

				blockNum = ReencryptBlock(buffer, nbytes / BLCKSZ,
										  fromNode, toNode, fromForkNum,
										  toForkNum, blockNum);
			}
		}

		errno = 0;
		pgstat_report_wait_start(WAIT_EVENT_COPY_FILE_WRITE);
		if ((int) write(dstfd, buffer, nbytes) != nbytes)
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
				errno = ENOSPC;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m", tofile)));
		}
		pgstat_report_wait_end();
	}

	if (offset > flush_offset)
		pg_flush_data(dstfd, flush_offset, offset - flush_offset);

	if (CloseTransientFile(dstfd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", tofile)));

	if (CloseTransientFile(srcfd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", fromfile)));

	pfree(buffer);
}
