/*
 * Copy entire files.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/pg_combinebackup/copy_file.h
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#ifdef HAVE_COPYFILE_H
#include <copyfile.h>
#endif
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/file_perm.h"
#include "common/logging.h"
#include "copy_file.h"

static void copy_file_blocks(const char *src, const char *dst,
							 pg_checksum_context *checksum_ctx);

static void copy_file_clone(const char *src, const char *dst);

static void copy_file_by_range(const char *src, const char *dst);

#ifdef WIN32
static void copy_file_copyfile(const char *src, const char *dst);
#endif

/*
 * Copy a regular file, optionally computing a checksum, and emitting
 * appropriate debug messages. But if we're in dry-run mode, then just emit
 * the messages and don't copy anything.
 */
void
copy_file(const char *src, const char *dst,
		  pg_checksum_context *checksum_ctx, bool dry_run,
		  CopyMode copy_mode)
{
	char   *strategy_name = NULL;

	/*
	 * In dry-run mode, we don't actually copy anything, nor do we read any
	 * data from the source file, but we do verify that we can open it.
	 */
	if (dry_run)
	{
		int			fd;

		if ((fd = open(src, O_RDONLY | PG_BINARY, 0)) < 0)
			pg_fatal("could not open \"%s\": %m", src);
		if (close(fd) < 0)
			pg_fatal("could not close \"%s\": %m", src);
	}

	/*
	 *
	 * If we need to compute a checksum, but the user perhaps requested
	 * a special copy method that does not support this, fallback to the
	 * default block-by-block copy. We don't want to fail if just one of
	 * many files requires checksum, etc.
	 *
	 * If we don't need to compute a checksum, then we can use any special
	 * operating system primitives that we know about to copy the file; this
	 * may be quicker than a naive block copy. We only do this for WIN32.
	 * On other operating systems the user has to explicitly specify one of
	 * the available primitives - there may be multiple, we don't know which
	 * are reliable/preferred.
	 */
	if (checksum_ctx->type != CHECKSUM_TYPE_NONE)
	{
		/* fallback to block-by-block copy */
		copy_mode = COPY_MODE_COPY;
	}
#ifdef WIN32
	else
	{
		copy_mode = COPY_MODE_COPYFILE;
	}
#endif

	/* Determine the name of the copy strategy for use in log messages. */
	switch (copy_mode)
	{
		case COPY_MODE_CLONE:
			strategy_name = "clone";
			break;
		case COPY_MODE_COPY:
			/* leave NULL for simple block-by-block copy */
			break;
		case COPY_MODE_COPY_FILE_RANGE:
			strategy_name = "copy_file_range";
			break;
#ifdef WIN32
		case COPY_MODE_COPYFILE:
			strategy_name = "CopyFile";
			break;
#endif
	}

	if (dry_run)
	{
		if (strategy_name)
			pg_log_debug("would copy \"%s\" to \"%s\" using strategy %s",
						 src, dst, strategy_name);
		else
			pg_log_debug("would copy \"%s\" to \"%s\"",
						 src, dst);
	}
	else
	{

		if (strategy_name)
			pg_log_debug("copying \"%s\" to \"%s\" using strategy %s",
						 src, dst, strategy_name);
		else
			pg_log_debug("copying \"%s\" to \"%s\"",
						 src, dst);

		switch (copy_mode)
		{
			case COPY_MODE_CLONE:
				copy_file_clone(src, dst);
				break;
			case COPY_MODE_COPY:
				copy_file_blocks(src, dst, checksum_ctx);
				break;
			case COPY_MODE_COPY_FILE_RANGE:
				copy_file_by_range(src, dst);
				break;
#ifdef WIN32
			case COPY_MODE_COPYFILE:
				copy_file_copyfile(src, dst);
				break;
#endif
		}
	}
}

/*
 * Copy a file block by block, and optionally compute a checksum as we go.
 */
static void
copy_file_blocks(const char *src, const char *dst,
				 pg_checksum_context *checksum_ctx)
{
	int			src_fd;
	int			dest_fd;
	uint8	   *buffer;
	const int	buffer_size = 50 * BLCKSZ;
	ssize_t		rb;
	unsigned	offset = 0;

	if ((src_fd = open(src, O_RDONLY | PG_BINARY, 0)) < 0)
		pg_fatal("could not open file \"%s\": %m", src);

	if ((dest_fd = open(dst, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY,
						pg_file_create_mode)) < 0)
		pg_fatal("could not open file \"%s\": %m", dst);

	buffer = pg_malloc(buffer_size);

	while ((rb = read(src_fd, buffer, buffer_size)) > 0)
	{
		ssize_t		wb;

		if ((wb = write(dest_fd, buffer, rb)) != rb)
		{
			if (wb < 0)
				pg_fatal("could not write file \"%s\": %m", dst);
			else
				pg_fatal("could not write file \"%s\": wrote only %d of %d bytes at offset %u",
						 dst, (int) wb, (int) rb, offset);
		}

		if (pg_checksum_update(checksum_ctx, buffer, rb) < 0)
			pg_fatal("could not update checksum of file \"%s\"", dst);

		offset += rb;
	}

	if (rb < 0)
		pg_fatal("could not read file \"%s\": %m", dst);

	pg_free(buffer);
	close(src_fd);
	close(dest_fd);
}

static void
copy_file_clone(const char *src, const char *dest)
{
#if defined(HAVE_COPYFILE) && defined(COPYFILE_CLONE_FORCE)
	if (copyfile(src, dest, NULL, COPYFILE_CLONE_FORCE) < 0)
		pg_fatal("error while cloning file \"%s\" to \"%s\": %m", src, dest);
#elif defined(__linux__) && defined(FICLONE)
	{
		if ((src_fd = open(src, O_RDONLY | PG_BINARY, 0)) < 0)
			pg_fatal("could not open file \"%s\": %m", src);

		if ((dest_fd = open(dest, O_RDWR | O_CREAT | O_EXCL | PG_BINARY,
							pg_file_create_mode)) < 0)
			pg_fatal("could not create file \"%s\": %m", dest);

		if (ioctl(dest_fd, FICLONE, src_fd) < 0)
		{
			int			save_errno = errno;

			unlink(dest);

			pg_fatal("error while cloning file \"%s\" to \"%s\": %s",
					 src, dest);
		}
	}
#else
	pg_fatal("file cloning not supported on this platform");
#endif
}

static void
copy_file_by_range(const char *src, const char *dest)
{
#if defined(HAVE_COPY_FILE_RANGE)
	int			src_fd;
	int			dest_fd;
	ssize_t		nbytes;

	if ((src_fd = open(src, O_RDONLY | PG_BINARY, 0)) < 0)
		pg_fatal("could not open file \"%s\": %m", src);

	if ((dest_fd = open(dest, O_RDWR | O_CREAT | O_EXCL | PG_BINARY,
						pg_file_create_mode)) < 0)
		pg_fatal("could not create file \"%s\": %m", dest);

	do
	{
		nbytes = copy_file_range(src_fd, NULL, dest_fd, NULL, SSIZE_MAX, 0);
		if (nbytes < 0)
			pg_fatal("error while copying file range from \"%s\" to \"%s\": %m",
					 src, dest);
	} while (nbytes > 0);

	close(src_fd);
	close(dest_fd);
#else
	pg_fatal("copy_file_range not supported on this platform");
#endif
}

/* XXX maybe this should do the check internally, same as the other functions? */
#ifdef WIN32
static void
copy_file_copyfile(const char *src, const char *dst)
{
	if (CopyFile(src, dst, true) == 0)
	{
		_dosmaperr(GetLastError());
		pg_fatal("could not copy \"%s\" to \"%s\": %m", src, dst);
	}
}
#endif							/* WIN32 */
