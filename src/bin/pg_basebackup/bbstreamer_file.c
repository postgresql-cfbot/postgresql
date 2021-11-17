/*-------------------------------------------------------------------------
 *
 * bbstreamer_file.c
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_basebackup/bbstreamer_file.c
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#ifdef HAVE_LIBZ
#include <zlib.h>
#endif

#include <unistd.h>

#include "bbstreamer.h"
#include "common/logging.h"
#include "common/file_perm.h"
#include "common/string.h"

typedef struct bbstreamer_plain_writer
{
	bbstreamer	base;
	char	   *pathname;
	FILE	   *file;
	bool		should_close_file;
} bbstreamer_plain_writer;

#ifdef HAVE_LIBZ
typedef struct bbstreamer_gzip_writer
{
	bbstreamer	base;
	char	   *pathname;
	gzFile		gzfile;
}			bbstreamer_gzip_writer;
#endif

typedef struct bbstreamer_extractor
{
	bbstreamer	base;
	char	   *basepath;
	const char *(*link_map) (const char *);
	void		(*report_output_file) (const char *);
	char		filename[MAXPGPATH];
	FILE	   *file;
}			bbstreamer_extractor;

static void bbstreamer_plain_writer_content(bbstreamer *streamer,
											bbstreamer_member *member,
											const char *data, int len,
											bbstreamer_archive_context context);
static void bbstreamer_plain_writer_finalize(bbstreamer *streamer);
static void bbstreamer_plain_writer_free(bbstreamer *streamer);

const bbstreamer_ops bbstreamer_plain_writer_ops = {
	.content = bbstreamer_plain_writer_content,
	.finalize = bbstreamer_plain_writer_finalize,
	.free = bbstreamer_plain_writer_free
};

#ifdef HAVE_LIBZ
static void bbstreamer_gzip_writer_content(bbstreamer *streamer,
										   bbstreamer_member *member,
										   const char *data, int len,
										   bbstreamer_archive_context context);
static void bbstreamer_gzip_writer_finalize(bbstreamer *streamer);
static void bbstreamer_gzip_writer_free(bbstreamer *streamer);
static const char *get_gz_error(gzFile gzf);

const bbstreamer_ops bbstreamer_gzip_writer_ops = {
	.content = bbstreamer_gzip_writer_content,
	.finalize = bbstreamer_gzip_writer_finalize,
	.free = bbstreamer_gzip_writer_free
};
#endif

static void bbstreamer_extractor_content(bbstreamer *streamer,
										 bbstreamer_member *member,
										 const char *data, int len,
										 bbstreamer_archive_context context);
static void bbstreamer_extractor_finalize(bbstreamer *streamer);
static void bbstreamer_extractor_free(bbstreamer *streamer);
static void extract_directory(const char *filename, mode_t mode);
static void extract_link(const char *filename, const char *linktarget);
static FILE *create_file_for_extract(const char *filename, mode_t mode);

const bbstreamer_ops bbstreamer_extractor_ops = {
	.content = bbstreamer_extractor_content,
	.finalize = bbstreamer_extractor_finalize,
	.free = bbstreamer_extractor_free
};

/*
 * Create a bbstreamer that just writes data to a file.
 *
 * The caller must specify a pathname and may specify a file. The pathname is
 * used for error-reporting purposes either way. If file is NULL, the pathname
 * also identifies the file to which the data should be written: it is opened
 * for writing and closed when done. If file is not NULL, the data is written
 * there.
 */
bbstreamer *
bbstreamer_plain_writer_new(char *pathname, FILE *file)
{
	bbstreamer_plain_writer *streamer;

	streamer = palloc0(sizeof(bbstreamer_plain_writer));
	*((const bbstreamer_ops **) &streamer->base.bbs_ops) =
		&bbstreamer_plain_writer_ops;

	streamer->pathname = pstrdup(pathname);
	streamer->file = file;

	if (file == NULL)
	{
		streamer->file = fopen(pathname, "wb");
		if (streamer->file == NULL)
		{
			pg_log_error("could not create file \"%s\": %m", pathname);
			exit(1);
		}
		streamer->should_close_file = true;
	}

	return &streamer->base;
}

/*
 * Write archive content to file.
 */
static void
bbstreamer_plain_writer_content(bbstreamer *streamer,
								bbstreamer_member *member, const char *data,
								int len, bbstreamer_archive_context context)
{
	bbstreamer_plain_writer *mystreamer;

	mystreamer = (bbstreamer_plain_writer *) streamer;

	if (len == 0)
		return;

	errno = 0;
	if (fwrite(data, len, 1, mystreamer->file) != 1)
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		pg_log_error("could not write to file \"%s\": %m",
					 mystreamer->pathname);
		exit(1);
	}
}

/*
 * End-of-archive processing when writing to a plain file consists of closing
 * the file if we opened it, but not if the caller provided it.
 */
static void
bbstreamer_plain_writer_finalize(bbstreamer *streamer)
{
	bbstreamer_plain_writer *mystreamer;

	mystreamer = (bbstreamer_plain_writer *) streamer;

	if (mystreamer->should_close_file && fclose(mystreamer->file) != 0)
	{
		pg_log_error("could not close file \"%s\": %m",
					 mystreamer->pathname);
		exit(1);
	}

	mystreamer->file = NULL;
	mystreamer->should_close_file = false;
}

/*
 * Free memory associated with this bbstreamer.
 */
static void
bbstreamer_plain_writer_free(bbstreamer *streamer)
{
	bbstreamer_plain_writer *mystreamer;

	mystreamer = (bbstreamer_plain_writer *) streamer;

	Assert(!mystreamer->should_close_file);
	Assert(mystreamer->base.bbs_next == NULL);

	pfree(mystreamer->pathname);
	pfree(mystreamer);
}

/*
 * Create a bbstreamer that just compresses data using gzip, and then writes
 * it to a file.
 *
 * As in the case of bbstreamer_plain_writer_new, pathname is always used
 * for error reporting purposes; if file is NULL, it is also the opened and
 * closed so that the data may be written there.
 */
bbstreamer *
bbstreamer_gzip_writer_new(char *pathname, FILE *file, int compresslevel)
{
#ifdef HAVE_LIBZ
	bbstreamer_gzip_writer *streamer;

	streamer = palloc0(sizeof(bbstreamer_gzip_writer));
	*((const bbstreamer_ops **) &streamer->base.bbs_ops) =
		&bbstreamer_gzip_writer_ops;

	streamer->pathname = pstrdup(pathname);

	if (file == NULL)
	{
		streamer->gzfile = gzopen(pathname, "wb");
		if (streamer->gzfile == NULL)
		{
			pg_log_error("could not create compressed file \"%s\": %m",
						 pathname);
			exit(1);
		}
	}
	else
	{
		int			fd = dup(fileno(file));

		if (fd < 0)
		{
			pg_log_error("could not duplicate stdout: %m");
			exit(1);
		}

		streamer->gzfile = gzdopen(fd, "wb");
		if (streamer->gzfile == NULL)
		{
			pg_log_error("could not open output file: %m");
			exit(1);
		}
	}

	if (gzsetparams(streamer->gzfile, compresslevel,
					Z_DEFAULT_STRATEGY) != Z_OK)
	{
		pg_log_error("could not set compression level %d: %s",
					 compresslevel, get_gz_error(streamer->gzfile));
		exit(1);
	}

	return &streamer->base;
#else
	pg_log_error("this build does not support compression");
	exit(1);
#endif
}

#ifdef HAVE_LIBZ
/*
 * Write archive content to gzip file.
 */
static void
bbstreamer_gzip_writer_content(bbstreamer *streamer,
							   bbstreamer_member *member, const char *data,
							   int len, bbstreamer_archive_context context)
{
	bbstreamer_gzip_writer *mystreamer;

	mystreamer = (bbstreamer_gzip_writer *) streamer;

	if (len == 0)
		return;

	errno = 0;
	if (gzwrite(mystreamer->gzfile, data, len) != len)
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		pg_log_error("could not write to compressed file \"%s\": %s",
					 mystreamer->pathname, get_gz_error(mystreamer->gzfile));
		exit(1);
	}
}

/*
 * End-of-archive processing when writing to a gzip file consists of just
 * calling gzclose.
 *
 * It makes no difference whether we opened the file or the caller did it,
 * because libz provides no way of avoiding a close on the underling file
 * handle. Notice, however, that bbstreamer_gzip_writer_new() uses dup() to
 * work around this issue, so that the behavior from the caller's viewpoint
 * is the same as for bbstreamer_plain_writer.
 */
static void
bbstreamer_gzip_writer_finalize(bbstreamer *streamer)
{
	bbstreamer_gzip_writer *mystreamer;

	mystreamer = (bbstreamer_gzip_writer *) streamer;

	errno = 0;					/* in case gzclose() doesn't set it */
	if (gzclose(mystreamer->gzfile) != 0)
	{
		pg_log_error("could not close compressed file \"%s\": %m",
					 mystreamer->pathname);
		exit(1);
	}

	mystreamer->gzfile = NULL;
}

/*
 * Free memory associated with this bbstreamer.
 */
static void
bbstreamer_gzip_writer_free(bbstreamer *streamer)
{
	bbstreamer_gzip_writer *mystreamer;

	mystreamer = (bbstreamer_gzip_writer *) streamer;

	Assert(mystreamer->base.bbs_next == NULL);
	Assert(mystreamer->gzfile == NULL);

	pfree(mystreamer->pathname);
	pfree(mystreamer);
}

/*
 * Helper function for libz error reporting.
 */
static const char *
get_gz_error(gzFile gzf)
{
	int			errnum;
	const char *errmsg;

	errmsg = gzerror(gzf, &errnum);
	if (errnum == Z_ERRNO)
		return strerror(errno);
	else
		return errmsg;
}
#endif

/*
 * Create a bbstreamer that extracts an archive.
 *
 * All pathnames in the archive are interpreted relative to basepath.
 *
 * Unlike e.g. bbstreamer_plain_writer_new() we can't do anything useful here
 * with untyped chunks; we need typed chunks which follow the rules described
 * in bbstreamer.h. Assuming we have that, we don't need to worry about the
 * original archive format; it's enough to just look at the member information
 * provided and write to the corresponding file.
 *
 * 'link_map' is a function that will be applied to the target of any
 * symbolic link, and which should return a replacement pathname to be used
 * in its place.  If NULL, the symbolic link target is used without
 * modification.
 *
 * 'report_output_file' is a function that will be called each time we open a
 * new output file. The pathname to that file is passed as an argument. If
 * NULL, the call is skipped.
 */
bbstreamer *
bbstreamer_extractor_new(const char *basepath,
						 const char *(*link_map) (const char *),
						 void (*report_output_file) (const char *))
{
	bbstreamer_extractor *streamer;

	streamer = palloc0(sizeof(bbstreamer_extractor));
	*((const bbstreamer_ops **) &streamer->base.bbs_ops) =
		&bbstreamer_extractor_ops;
	streamer->basepath = pstrdup(basepath);
	streamer->link_map = link_map;
	streamer->report_output_file = report_output_file;

	return &streamer->base;
}

/*
 * Extract archive contents to the filesystem.
 */
static void
bbstreamer_extractor_content(bbstreamer *streamer, bbstreamer_member *member,
							 const char *data, int len,
							 bbstreamer_archive_context context)
{
	bbstreamer_extractor *mystreamer = (bbstreamer_extractor *) streamer;
	int			fnamelen;

	Assert(member != NULL || context == BBSTREAMER_ARCHIVE_TRAILER);
	Assert(context != BBSTREAMER_UNKNOWN);

	switch (context)
	{
		case BBSTREAMER_MEMBER_HEADER:
			Assert(mystreamer->file == NULL);

			/* Prepend basepath. */
			snprintf(mystreamer->filename, sizeof(mystreamer->filename),
					 "%s/%s", mystreamer->basepath, member->pathname);

			/* Remove any trailing slash. */
			fnamelen = strlen(mystreamer->filename);
			if (mystreamer->filename[fnamelen - 1] == '/')
				mystreamer->filename[fnamelen - 1] = '\0';

			/* Dispatch based on file type. */
			if (member->is_directory)
				extract_directory(mystreamer->filename, member->mode);
			else if (member->is_link)
			{
				const char *linktarget = member->linktarget;

				if (mystreamer->link_map)
					linktarget = mystreamer->link_map(linktarget);
				extract_link(mystreamer->filename, linktarget);
			}
			else
				mystreamer->file =
					create_file_for_extract(mystreamer->filename,
											member->mode);

			/* Report output file change. */
			if (mystreamer->report_output_file)
				mystreamer->report_output_file(mystreamer->filename);
			break;

		case BBSTREAMER_MEMBER_CONTENTS:
			if (mystreamer->file == NULL)
				break;

			errno = 0;
			if (len > 0 && fwrite(data, len, 1, mystreamer->file) != 1)
			{
				/* if write didn't set errno, assume problem is no disk space */
				if (errno == 0)
					errno = ENOSPC;
				pg_log_error("could not write to file \"%s\": %m",
							 mystreamer->filename);
				exit(1);
			}
			break;

		case BBSTREAMER_MEMBER_TRAILER:
			if (mystreamer->file == NULL)
				break;
			fclose(mystreamer->file);
			mystreamer->file = NULL;
			break;

		case BBSTREAMER_ARCHIVE_TRAILER:
			break;

		default:
			/* Shouldn't happen. */
			pg_log_error("unexpected state while extracting archive");
			exit(1);
	}
}

/*
 * Create a directory.
 */
static void
extract_directory(const char *filename, mode_t mode)
{
	if (mkdir(filename, pg_dir_create_mode) != 0)
	{
		/*
		 * When streaming WAL, pg_wal (or pg_xlog for pre-9.6 clusters) will
		 * have been created by the wal receiver process. Also, when the WAL
		 * directory location was specified, pg_wal (or pg_xlog) has already
		 * been created as a symbolic link before starting the actual backup.
		 * So just ignore creation failures on related directories.
		 */
		if (!((pg_str_endswith(filename, "/pg_wal") ||
			   pg_str_endswith(filename, "/pg_xlog") ||
			   pg_str_endswith(filename, "/archive_status")) &&
			  errno == EEXIST))
		{
			pg_log_error("could not create directory \"%s\": %m",
						 filename);
			exit(1);
		}
	}

#ifndef WIN32
	if (chmod(filename, mode))
	{
		pg_log_error("could not set permissions on directory \"%s\": %m",
					 filename);
		exit(1);
	}
#endif
}

/*
 * Create a symbolic link.
 *
 * It's most likely a link in pg_tblspc directory, to the location of a
 * tablespace. Apply any tablespace mapping given on the command line
 * (--tablespace-mapping). (We blindly apply the mapping without checking that
 * the link really is inside pg_tblspc. We don't expect there to be other
 * symlinks in a data directory, but if there are, you can call it an
 * undocumented feature that you can map them too.)
 */
static void
extract_link(const char *filename, const char *linktarget)
{
	if (symlink(linktarget, filename) != 0)
	{
		pg_log_error("could not create symbolic link from \"%s\" to \"%s\": %m",
					 filename, linktarget);
		exit(1);
	}
}

/*
 * Create a regular file.
 *
 * Return the resulting handle so we can write the content to the file.
 */
static FILE *
create_file_for_extract(const char *filename, mode_t mode)
{
	FILE	   *file;

	file = fopen(filename, "wb");
	if (file == NULL)
	{
		pg_log_error("could not create file \"%s\": %m", filename);
		exit(1);
	}

#ifndef WIN32
	if (chmod(filename, mode))
	{
		pg_log_error("could not set permissions on file \"%s\": %m",
					 filename);
		exit(1);
	}
#endif

	return file;
}

/*
 * End-of-stream processing for extracting an archive.
 *
 * There's nothing to do here but sanity checking.
 */
static void
bbstreamer_extractor_finalize(bbstreamer *streamer)
{
	bbstreamer_extractor *mystreamer PG_USED_FOR_ASSERTS_ONLY
		= (bbstreamer_extractor *) streamer;

	Assert(mystreamer->file == NULL);
}

/*
 * Free memory.
 */
static void
bbstreamer_extractor_free(bbstreamer *streamer)
{
	bbstreamer_extractor *mystreamer = (bbstreamer_extractor *) streamer;

	pfree(mystreamer->basepath);
	pfree(mystreamer);
}
