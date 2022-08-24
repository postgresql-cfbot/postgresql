/*-------------------------------------------------------------------------
 *
 * compress_io.c
 *	 Routines for archivers to write an uncompressed or compressed data
 *	 stream.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This file includes two APIs for dealing with compressed data. The first
 * provides more flexibility, using callbacks to read/write data from the
 * underlying stream. The second API is a wrapper around fopen/gzopen and
 * friends, providing an interface similar to those, but abstracts away
 * the possible compression. Both APIs use libz for the compression, but
 * the second API uses gzip headers, so the resulting files can be easily
 * manipulated with the gzip utility.
 *
 * Compressor API
 * --------------
 *
 *	The interface for writing to an archive consists of three functions:
 *	AllocateCompressor, WriteDataToArchive and EndCompressor. First you call
 *	AllocateCompressor, then write all the data by calling WriteDataToArchive
 *	as many times as needed, and finally EndCompressor. WriteDataToArchive
 *	and EndCompressor will call the WriteFunc that was provided to
 *	AllocateCompressor for each chunk of compressed data.
 *
 *	The interface for reading an archive consists of just one function:
 *	ReadDataFromArchive. ReadDataFromArchive reads the whole compressed input
 *	stream, by repeatedly calling the given ReadFunc. ReadFunc returns the
 *	compressed data chunk at a time, and ReadDataFromArchive decompresses it
 *	and passes the decompressed data to ahwrite(), until ReadFunc returns 0
 *	to signal EOF.
 *
 *	The interface is the same for compressed and uncompressed streams.
 *
 * Compressed stream API
 * ----------------------
 *
 *	The compressed stream API is a wrapper around the C standard fopen() and
 *	libz's gzopen() APIs and custom LZ4 calls which provide similar
 *	functionality. It allows you to use the same functions for compressed and
 *	uncompressed streams. cfopen_read() first tries to open the file with given
 *	name, and if it fails, it tries to open the same file with the .gz suffix,
 *	failing that it tries to open the same file with the .lz4 suffix.
 *	cfopen_write() opens a file for writing, an extra argument specifies the
 *	method to use should the file be compressed, and adds the appropriate
 *	suffix, .gz or .lz4, to the filename if so. This allows you to easily handle
 *	both compressed and uncompressed files.
 *
 * IDENTIFICATION
 *	   src/bin/pg_dump/compress_io.c
 *
 *-------------------------------------------------------------------------
 */
#include <sys/stat.h>
#include <unistd.h>
#include "postgres_fe.h"

#include "compress_io.h"
#include "compress_gzip.h"
#include "compress_lz4.h"
#include "pg_backup_utils.h"

/*----------------------
 * Compressor API
 *----------------------
 */

/* Private routines that support uncompressed data I/O */
static void
ReadDataFromArchiveNone(ArchiveHandle *AH, CompressorState *cs)
{
	size_t		cnt;
	char	   *buf;
	size_t		buflen;

	buf = pg_malloc(ZLIB_OUT_SIZE);
	buflen = ZLIB_OUT_SIZE;

	while ((cnt = cs->readF(AH, &buf, &buflen)))
	{
		ahwrite(buf, 1, cnt, AH);
	}

	free(buf);
}

static void
WriteDataToArchiveNone(ArchiveHandle *AH, CompressorState *cs,
					   const void *data, size_t dLen)
{
	cs->writeF(AH, data, dLen);
}

static void
EndCompressorNone(ArchiveHandle *AH, CompressorState *cs)
{
	/* no op */
}

static void
InitCompressorNone(CompressorState *cs)
{
	cs->readData = ReadDataFromArchiveNone;
	cs->writeData = WriteDataToArchiveNone;
	cs->end = EndCompressorNone;
}

/* Public interface routines */

/* Allocate a new compressor */
CompressorState *
AllocateCompressor(const pg_compress_specification compress_spec,
				   ReadFunc readF, WriteFunc writeF)
{
	CompressorState *cs;

	cs = (CompressorState *) pg_malloc0(sizeof(CompressorState));
	cs->readF = readF;
	cs->writeF = writeF;

	switch (compress_spec.algorithm)
	{
		case PG_COMPRESSION_NONE:
			InitCompressorNone(cs);
			break;
		case PG_COMPRESSION_GZIP:
			InitCompressorGzip(cs, compress_spec.level);
			break;
		case PG_COMPRESSION_LZ4:
			InitCompressorLZ4(cs, compress_spec.level);
			break;
		default:
			pg_fatal("invalid compression method");
			break;
	}

	return cs;
}

/*
 * Terminate compression library context and flush its buffers.
 */
void
EndCompressor(ArchiveHandle *AH, CompressorState *cs)
{
	cs->end(AH, cs);
	pg_free(cs);
}

/*----------------------
 * Compressed stream API
 *----------------------
 */

static int
hasSuffix(const char *filename, const char *suffix)
{
	int			filenamelen = strlen(filename);
	int			suffixlen = strlen(suffix);

	if (filenamelen < suffixlen)
		return 0;

	return memcmp(&filename[filenamelen - suffixlen],
				  suffix,
				  suffixlen) == 0;
}

/* free() without changing errno; useful in several places below */
static void
free_keep_errno(void *p)
{
	int			save_errno = errno;

	free(p);
	errno = save_errno;
}

/*
 * Compression None implementation
 */

static size_t
_read(void *ptr, size_t size, CompressFileHandle * CFH)
{
	FILE	   *fp = (FILE *) CFH->private;
	size_t		ret;

	if (size == 0)
		return 0;

	ret = fread(ptr, 1, size, fp);
	if (ret != size && !feof(fp))
		pg_fatal("could not read from input file: %s",
				 strerror(errno));

	return ret;
}

static size_t
_write(const void *ptr, size_t size, CompressFileHandle * CFH)
{
	return fwrite(ptr, 1, size, (FILE *) CFH->private);
}

static const char *
_get_error(CompressFileHandle * CFH)
{
	return strerror(errno);
}

static char *
_gets(char *ptr, int size, CompressFileHandle * CFH)
{
	return fgets(ptr, size, (FILE *) CFH->private);
}

static int
_getc(CompressFileHandle * CFH)
{
	FILE	   *fp = (FILE *) CFH->private;
	int			ret;

	ret = fgetc(fp);
	if (ret == EOF)
	{
		if (!feof(fp))
			pg_fatal("could not read from input file: %s", strerror(errno));
		else
			pg_fatal("could not read from input file: end of file");
	}

	return ret;
}

static int
_close(CompressFileHandle * CFH)
{
	FILE	   *fp = (FILE *) CFH->private;
	int			ret = 0;

	CFH->private = NULL;

	if (fp)
		ret = fclose(fp);

	return ret;
}

static int
_eof(CompressFileHandle * CFH)
{
	return feof((FILE *) CFH->private);
}

static int
_open(const char *path, int fd, const char *mode, CompressFileHandle * CFH)
{
	Assert(CFH->private == NULL);

	if (fd >= 0)
		CFH->private = fdopen(dup(fd), mode);
	else
		CFH->private = fopen(path, mode);

	if (CFH->private == NULL)
		return 1;

	return 0;
}

static int
_open_write(const char *path, const char *mode, CompressFileHandle * CFH)
{
	Assert(CFH->private == NULL);

	CFH->private = fopen(path, mode);
	if (CFH->private == NULL)
		return 1;

	return 0;
}

static void
InitCompressNone(CompressFileHandle * CFH)
{
	CFH->open = _open;
	CFH->open_write = _open_write;
	CFH->read = _read;
	CFH->write = _write;
	CFH->gets = _gets;
	CFH->getc = _getc;
	CFH->close = _close;
	CFH->eof = _eof;
	CFH->get_error = _get_error;

	CFH->private = NULL;
}

/*
 * Public interface
 */
CompressFileHandle *
InitCompressFileHandle(const pg_compress_specification compress_spec)
{
	CompressFileHandle *CFH;

	CFH = pg_malloc0(sizeof(CompressFileHandle));

	switch (compress_spec.algorithm)
	{
		case PG_COMPRESSION_NONE:
			InitCompressNone(CFH);
			break;
		case PG_COMPRESSION_GZIP:
			InitCompressGzip(CFH, compress_spec.level);
			break;
		case PG_COMPRESSION_LZ4:
			InitCompressLZ4(CFH, compress_spec.level);
			break;
		default:
			pg_fatal("invalid compression method");
			break;
	}

	return CFH;
}

/*
 * Open a file for reading. 'path' is the file to open, and 'mode' should
 * be either "r" or "rb".
 *
 * If the file at 'path' does not exist, we append the "{.gz,.lz4}" suffix (i
 * 'path' doesn't already have it) and try again. So if you pass "foo" as
 * 'path', this will open either "foo" or "foo.gz" or "foo.lz4", trying in that
 * order.
 *
 * On failure, return NULL with an error code in errno.
 */
CompressFileHandle *
InitDiscoverCompressFileHandle(const char *path, const char *mode)
{
	CompressFileHandle *CFH = NULL;
	struct stat st;
	char	   *fname;
	pg_compress_specification compress_spec = {0};

	compress_spec.algorithm = PG_COMPRESSION_NONE;

	Assert(strcmp(mode, "r") == 0 || strcmp(mode, "rb") == 0);

	fname = strdup(path);

	if (hasSuffix(fname, ".gz"))
		compress_spec.algorithm = PG_COMPRESSION_GZIP;
	else
	{
		bool		exists;

		exists = (stat(path, &st) == 0);
		/* avoid unused warning if it is not build with compression */
		if (exists)
			compress_spec.algorithm = PG_COMPRESSION_NONE;
#ifdef HAVE_LIBZ
		if (!exists)
		{
			free_keep_errno(fname);
			fname = psprintf("%s.gz", path);
			exists = (stat(fname, &st) == 0);

			if (exists)
				compress_spec.algorithm = PG_COMPRESSION_GZIP;
		}
#endif
#ifdef USE_LZ4
		if (!exists)
		{
			free_keep_errno(fname);
			fname = psprintf("%s.lz4", path);
			exists = (stat(fname, &st) == 0);

			if (exists)
				compress_spec.algorithm = PG_COMPRESSION_LZ4;
		}
#endif
	}

	CFH = InitCompressFileHandle(compress_spec);
	if (CFH->open(fname, -1, mode, CFH))
	{
		free_keep_errno(CFH);
		CFH = NULL;
	}
	free_keep_errno(fname);

	return CFH;
}

int
DestroyCompressFileHandle(CompressFileHandle * CFH)
{
	int			ret = 0;

	if (CFH->private)
		ret = CFH->close(CFH);

	free_keep_errno(CFH);

	return ret;
}
