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
#include "postgres_fe.h"

#include "compress_io.h"
#include "pg_backup_utils.h"

#ifdef HAVE_LIBLZ4
#include "lz4.h"
#include "lz4frame.h"

#define LZ4_OUT_SIZE	(4 * 1024)
#define LZ4_IN_SIZE		(16 * 1024)
#endif

/*----------------------
 * Compressor API
 *----------------------
 */

/* typedef appears in compress_io.h */
struct CompressorState
{
	CompressionMethod compressionMethod;
	WriteFunc	writeF;

#ifdef HAVE_LIBZ
	z_streamp	zp;
#endif
	void	   *outbuf;
	size_t		outsize;
};

/* Routines that support zlib compressed data I/O */
#ifdef HAVE_LIBZ
static void InitCompressorZlib(CompressorState *cs, int level);
static void DeflateCompressorZlib(ArchiveHandle *AH, CompressorState *cs,
								  bool flush);
static void ReadDataFromArchiveZlib(ArchiveHandle *AH, ReadFunc readF);
static void WriteDataToArchiveZlib(ArchiveHandle *AH, CompressorState *cs,
								   const char *data, size_t dLen);
static void EndCompressorZlib(ArchiveHandle *AH, CompressorState *cs);
#endif

/* Routines that support LZ4 compressed data I/O */
#ifdef HAVE_LIBLZ4
static void InitCompressorLZ4(CompressorState *cs);
static void ReadDataFromArchiveLZ4(ArchiveHandle *AH, ReadFunc readF);
static void WriteDataToArchiveLZ4(ArchiveHandle *AH, CompressorState *cs,
								  const char *data, size_t dLen);
static void EndCompressorLZ4(ArchiveHandle *AH, CompressorState *cs);
#endif

/* Routines that support uncompressed data I/O */
static void ReadDataFromArchiveNone(ArchiveHandle *AH, ReadFunc readF);
static void WriteDataToArchiveNone(ArchiveHandle *AH, CompressorState *cs,
								   const char *data, size_t dLen);

/* Public interface routines */

/* Allocate a new compressor */
CompressorState *
AllocateCompressor(CompressionMethod compressionMethod,
				   int compressionLevel, WriteFunc writeF)
{
	CompressorState *cs;

#ifndef HAVE_LIBZ
	if (compressionMethod == COMPRESSION_GZIP)
		fatal("not built with zlib support");
#endif
#ifndef HAVE_LIBLZ4
	if (compressionMethod == COMPRESSION_LZ4)
		fatal("not built with LZ4 support");
#endif


	cs = (CompressorState *) pg_malloc0(sizeof(CompressorState));
	cs->writeF = writeF;
	cs->compressionMethod = compressionMethod;

	/*
	 * Perform compression algorithm specific initialization.
	 */
#ifdef HAVE_LIBZ
	if (compressionMethod == COMPRESSION_GZIP)
		InitCompressorZlib(cs, compressionLevel);
#endif
#ifdef HAVE_LIBLZ4
	if (compressionMethod == COMPRESSION_LZ4)
		InitCompressorLZ4(cs);
#endif

	return cs;
}

/*
 * Read all compressed data from the input stream (via readF) and print it
 * out with ahwrite().
 */
void
ReadDataFromArchive(ArchiveHandle *AH, CompressionMethod compressionMethod,
					int compressionLevel, ReadFunc readF)
{
	switch (compressionMethod)
	{
		case COMPRESSION_NONE:
			ReadDataFromArchiveNone(AH, readF);
			break;
		case COMPRESSION_GZIP:
#ifdef HAVE_LIBZ
			ReadDataFromArchiveZlib(AH, readF);
#else
			fatal("not built with zlib support");
#endif
			break;
		case COMPRESSION_LZ4:
#ifdef HAVE_LIBLZ4
			ReadDataFromArchiveLZ4(AH, readF);
#else
			fatal("not built with lz4 support");
#endif
			break;
		default:
			fatal("invalid compression method");
			break;
	}
}

/*
 * Compress and write data to the output stream (via writeF).
 */
void
WriteDataToArchive(ArchiveHandle *AH, CompressorState *cs,
				   const void *data, size_t dLen)
{
	switch (cs->compressionMethod)
	{
		case COMPRESSION_GZIP:
#ifdef HAVE_LIBZ
			WriteDataToArchiveZlib(AH, cs, data, dLen);
#else
			fatal("not built with zlib support");
#endif
			break;
		case COMPRESSION_LZ4:
#ifdef HAVE_LIBLZ4
			WriteDataToArchiveLZ4(AH, cs, data, dLen);
#else
			fatal("not built with lz4 support");
#endif
			break;
		case COMPRESSION_NONE:
			WriteDataToArchiveNone(AH, cs, data, dLen);
			break;
		default:
			fatal("invalid compression method");
			break;
	}
}

/*
 * Terminate compression library context and flush its buffers.
 */
void
EndCompressor(ArchiveHandle *AH, CompressorState *cs)
{
	switch (cs->compressionMethod)
	{
		case COMPRESSION_GZIP:
#ifdef HAVE_LIBZ
			EndCompressorZlib(AH, cs);
#else
			fatal("not built with zlib support");
#endif
			break;
		case COMPRESSION_LZ4:
#ifdef HAVE_LIBLZ4
			EndCompressorLZ4(AH, cs);
#else
			fatal("not built with lz4 support");
#endif
			break;
		case COMPRESSION_NONE:
			free(cs);
			break;

		default:
			fatal("invalid compression method");
			break;
	}
}

/* Private routines, specific to each compression method. */

#ifdef HAVE_LIBZ
/*
 * Functions for zlib compressed output.
 */

static void
InitCompressorZlib(CompressorState *cs, int level)
{
	z_streamp	zp;

	zp = cs->zp = (z_streamp) pg_malloc(sizeof(z_stream));
	zp->zalloc = Z_NULL;
	zp->zfree = Z_NULL;
	zp->opaque = Z_NULL;

	/*
	 * outsize is the buffer size we tell zlib it can output to.  We
	 * actually allocate one extra byte because some routines want to append a
	 * trailing zero byte to the zlib output.
	 */
	cs->outbuf = pg_malloc(ZLIB_OUT_SIZE + 1);
	cs->outsize = ZLIB_OUT_SIZE;

	if (deflateInit(zp, level) != Z_OK)
		fatal("could not initialize compression library: %s",
			  zp->msg);

	/* Just be paranoid - maybe End is called after Start, with no Write */
	zp->next_out = cs->outbuf;
	zp->avail_out = cs->outsize;
}

static void
EndCompressorZlib(ArchiveHandle *AH, CompressorState *cs)
{
	z_streamp	zp = cs->zp;

	zp->next_in = NULL;
	zp->avail_in = 0;

	/* Flush any remaining data from zlib buffer */
	DeflateCompressorZlib(AH, cs, true);

	if (deflateEnd(zp) != Z_OK)
		fatal("could not close compression stream: %s", zp->msg);

	free(cs->outbuf);
	free(cs->zp);
}

static void
DeflateCompressorZlib(ArchiveHandle *AH, CompressorState *cs, bool flush)
{
	z_streamp	zp = cs->zp;
	void	   *out = cs->outbuf;
	int			res = Z_OK;

	while (cs->zp->avail_in != 0 || flush)
	{
		res = deflate(zp, flush ? Z_FINISH : Z_NO_FLUSH);
		if (res == Z_STREAM_ERROR)
			fatal("could not compress data: %s", zp->msg);
		if ((flush && (zp->avail_out < cs->outsize))
			|| (zp->avail_out == 0)
			|| (zp->avail_in != 0)
			)
		{
			/*
			 * Extra paranoia: avoid zero-length chunks, since a zero length
			 * chunk is the EOF marker in the custom format. This should never
			 * happen but...
			 */
			if (zp->avail_out < cs->outsize)
			{
				/*
				 * Any write function should do its own error checking but to
				 * make sure we do a check here as well...
				 */
				size_t		len = cs->outsize - zp->avail_out;

				cs->writeF(AH, (char *)out, len);
			}
			zp->next_out = out;
			zp->avail_out = cs->outsize;
		}

		if (res == Z_STREAM_END)
			break;
	}
}

static void
WriteDataToArchiveZlib(ArchiveHandle *AH, CompressorState *cs,
					   const char *data, size_t dLen)
{
	cs->zp->next_in = (void *) unconstify(char *, data);
	cs->zp->avail_in = dLen;
	DeflateCompressorZlib(AH, cs, false);
}

static void
ReadDataFromArchiveZlib(ArchiveHandle *AH, ReadFunc readF)
{
	z_streamp	zp;
	char	   *out;
	int			res = Z_OK;
	size_t		cnt;
	char	   *buf;
	size_t		buflen;

	zp = (z_streamp) pg_malloc(sizeof(z_stream));
	zp->zalloc = Z_NULL;
	zp->zfree = Z_NULL;
	zp->opaque = Z_NULL;

	buf = pg_malloc(ZLIB_IN_SIZE);
	buflen = ZLIB_IN_SIZE;

	out = pg_malloc(ZLIB_OUT_SIZE + 1);

	if (inflateInit(zp) != Z_OK)
		fatal("could not initialize compression library: %s",
			  zp->msg);

	/* no minimal chunk size for zlib */
	while ((cnt = readF(AH, &buf, &buflen)))
	{
		zp->next_in = (void *) buf;
		zp->avail_in = cnt;

		while (zp->avail_in > 0)
		{
			zp->next_out = (void *) out;
			zp->avail_out = ZLIB_OUT_SIZE;

			res = inflate(zp, 0);
			if (res != Z_OK && res != Z_STREAM_END)
				fatal("could not uncompress data: %s", zp->msg);

			out[ZLIB_OUT_SIZE - zp->avail_out] = '\0';
			ahwrite(out, 1, ZLIB_OUT_SIZE - zp->avail_out, AH);
		}
	}

	zp->next_in = NULL;
	zp->avail_in = 0;
	while (res != Z_STREAM_END)
	{
		zp->next_out = (void *) out;
		zp->avail_out = ZLIB_OUT_SIZE;
		res = inflate(zp, 0);
		if (res != Z_OK && res != Z_STREAM_END)
			fatal("could not uncompress data: %s", zp->msg);

		out[ZLIB_OUT_SIZE - zp->avail_out] = '\0';
		ahwrite(out, 1, ZLIB_OUT_SIZE - zp->avail_out, AH);
	}

	if (inflateEnd(zp) != Z_OK)
		fatal("could not close compression library: %s", zp->msg);

	free(buf);
	free(out);
	free(zp);
}
#endif							/* HAVE_LIBZ */

#ifdef HAVE_LIBLZ4
static void
InitCompressorLZ4(CompressorState *cs)
{
	/* Will be lazy init'd */
	cs->outbuf = NULL;
	cs->outsize = 0;
}

static void
EndCompressorLZ4(ArchiveHandle *AH, CompressorState *cs)
{
	pg_free(cs->outbuf);

	cs->outbuf = NULL;
	cs->outsize = 0;
}

static void
ReadDataFromArchiveLZ4(ArchiveHandle *AH, ReadFunc readF)
{
	LZ4_streamDecode_t lz4StreamDecode;
	char	   *buf;
	char	   *decbuf;
	size_t		buflen;
	size_t		cnt;

	buflen = (4 * 1024) + 1;
	buf = pg_malloc(buflen);
	decbuf = pg_malloc(buflen);

	LZ4_setStreamDecode(&lz4StreamDecode, NULL, 0);

	while ((cnt = readF(AH, &buf, &buflen)))
	{
		int		decBytes = LZ4_decompress_safe_continue(&lz4StreamDecode,
														buf, decbuf,
														cnt, buflen);

		ahwrite(decbuf, 1, decBytes, AH);
	}
}

static void
WriteDataToArchiveLZ4(ArchiveHandle *AH, CompressorState *cs,
					  const char *data, size_t dLen)
{
	size_t		compressed;
	size_t		requiredsize = LZ4_compressBound(dLen);

	if (requiredsize > cs->outsize)
	{
		cs->outbuf = pg_realloc(cs->outbuf, requiredsize);
		cs->outsize = requiredsize;
	}

	compressed = LZ4_compress_default(data, cs->outbuf,
									  dLen, cs->outsize);

	if (compressed <= 0)
		fatal("failed to LZ4 compress data");

	cs->writeF(AH, cs->outbuf, compressed);
}
#endif							/* HAVE_LIBLZ4 */

/*
 * Functions for uncompressed output.
 */

static void
ReadDataFromArchiveNone(ArchiveHandle *AH, ReadFunc readF)
{
	size_t		cnt;
	char	   *buf;
	size_t		buflen;

	buf = pg_malloc(ZLIB_OUT_SIZE);
	buflen = ZLIB_OUT_SIZE;

	while ((cnt = readF(AH, &buf, &buflen)))
	{
		ahwrite(buf, 1, cnt, AH);
	}

	free(buf);
}

static void
WriteDataToArchiveNone(ArchiveHandle *AH, CompressorState *cs,
					   const char *data, size_t dLen)
{
	cs->writeF(AH, data, dLen);
}


/*----------------------
 * Compressed stream API
 *----------------------
 */

#ifdef HAVE_LIBLZ4
/*
 * State needed for LZ4 (de)compression using the cfp API.
 */
typedef struct LZ4File
{
	FILE	*fp;

	LZ4F_preferences_t			prefs;

	LZ4F_compressionContext_t	ctx;
	LZ4F_decompressionContext_t	dtx;

	bool	inited;
	bool	compressing;

	size_t	buflen;
	char   *buffer;

	size_t  overflowalloclen;
	size_t	overflowlen;
	char   *overflowbuf;

	size_t	errcode;
} LZ4File;
#endif

/*
 * cfp represents an open stream, wrapping the underlying FILE, gzFile
 * pointer, or LZ4File pointer. This is opaque to the callers.
 */
struct cfp
{
	CompressionMethod compressionMethod;
	void	   *fp;
};

static int	hasSuffix(const char *filename, const char *suffix);

/* free() without changing errno; useful in several places below */
static void
free_keep_errno(void *p)
{
	int			save_errno = errno;

	free(p);
	errno = save_errno;
}

#ifdef HAVE_LIBLZ4
/*
 * LZ4 equivalent to feof() or gzeof(). The end of file
 * is reached if there is no decompressed output in the
 * overflow buffer and the end of the file is reached.
 */
static int
LZ4File_eof(LZ4File *fs)
{
	return fs->overflowlen == 0 && feof(fs->fp);
}

static const char *
LZ4File_error(LZ4File *fs)
{
	const char *errmsg;

	if (LZ4F_isError(fs->errcode))
		errmsg = LZ4F_getErrorName(fs->errcode);
	else
		errmsg = strerror(errno);

	return errmsg;
}

/*
 * Prepare an already alloc'ed LZ4File struct for subsequent calls.
 *
 * It creates the nessary contexts for the operations. When compressing,
 * it additionally writes the LZ4 header in the output stream.
 */
static int
LZ4File_init(LZ4File *fs, int size, bool compressing)
{
	size_t	status;

	if (fs->inited)
		return 0;

	fs->compressing = compressing;
	fs->inited = true;

	if (fs->compressing)
	{
		fs->buflen = LZ4F_compressBound(LZ4_IN_SIZE, &fs->prefs);
		if (fs->buflen < LZ4F_HEADER_SIZE_MAX)
			fs->buflen = LZ4F_HEADER_SIZE_MAX;

		status = LZ4F_createCompressionContext(&fs->ctx, LZ4F_VERSION);
		if (LZ4F_isError(status))
		{
			fs->errcode = status;
			return 1;
		}

		fs->buffer = pg_malloc(fs->buflen);
		status = LZ4F_compressBegin(fs->ctx, fs->buffer, fs->buflen, &fs->prefs);

		if (LZ4F_isError(status))
		{
			fs->errcode = status;
			return 1;
		}

		if (fwrite(fs->buffer, 1, status, fs->fp) != status)
		{
			errno = errno ? : ENOSPC;
			return 1;
		}
	}
	else
	{
		status = LZ4F_createDecompressionContext(&fs->dtx, LZ4F_VERSION);
		if (LZ4F_isError(status))
		{
			fs->errcode = status;
			return 1;
		}

		fs->buflen = size > LZ4_OUT_SIZE ? size : LZ4_OUT_SIZE;
		fs->buffer = pg_malloc(fs->buflen);

		fs->overflowalloclen = fs->buflen;
		fs->overflowbuf = pg_malloc(fs->overflowalloclen);
		fs->overflowlen = 0;
	}

	return 0;
}

/*
 * Read already decompressed content from the overflow buffer into 'ptr' up to
 * 'size' bytes, if available. If the eol_flag is set, then stop at the first
 * occurance of the new line char prior to 'size' bytes.
 *
 * Any unread content in the overflow buffer, is moved to the beginning.
 */
static int
LZ4File_read_overflow(LZ4File *fs, void *ptr, int size, bool eol_flag)
{
	char   *p;
	int		readlen = 0;

	if (fs->overflowlen == 0)
		return 0;

	if (fs->overflowlen >= size)
		readlen = size;
	else
		readlen = fs->overflowlen;

	if (eol_flag && (p = memchr(fs->overflowbuf, '\n', readlen)))
		readlen = p - fs->overflowbuf + 1; /* Include the line terminating char */

	memcpy(ptr, fs->overflowbuf, readlen);
	fs->overflowlen -= readlen;

	if (fs->overflowlen > 0)
		memmove(fs->overflowbuf, fs->overflowbuf + readlen, fs->overflowlen);

	return readlen;
}

/*
 * The workhorse for reading decompressed content out of an LZ4 compressed
 * stream.
 *
 * It will read up to 'ptrsize' decompressed content, or up to the new line char
 * if found first when the eol_flag is set. It is possible that the decompressed
 * output generated by reading any compressed input via the LZ4F API, exceeds
 * 'ptrsize'. Any exceeding decompressed content is stored at an overflow
 * buffer within LZ4File. Of course, when the function is called, it will first
 * try to consume any decompressed content already present in the overflow
 * buffer, before decompressing new content.
 */
static int
LZ4File_read(LZ4File *fs, void *ptr, int ptrsize, bool eol_flag)
{
	size_t	dsize = 0;
	size_t  rsize;
	size_t	size = ptrsize;
	bool	eol_found = false;

	void *readbuf;

	/* Lazy init */
	if (!fs->inited && LZ4File_init(fs, size, false /* decompressing */))
		return -1;

	/* Verfiy that there is enough space in the outbuf */
	if (size > fs->buflen)
	{
		fs->buflen = size;
		fs->buffer = pg_realloc(fs->buffer, size);
	}

	/* use already decompressed content if available */
	dsize = LZ4File_read_overflow(fs, ptr, size, eol_flag);
	if (dsize == size || (eol_flag && memchr(ptr, '\n', dsize)))
		return dsize;

	readbuf = pg_malloc(size);

	do
	{
		char   *rp;
		char   *rend;

		rsize = fread(readbuf, 1, size, fs->fp);
		if (rsize < size && !feof(fs->fp))
			return -1;

		rp = (char *)readbuf;
		rend = (char *)readbuf + rsize;

		while (rp < rend)
		{
			size_t	status;
			size_t	outlen = fs->buflen;
			size_t	read_remain = rend - rp;

			memset(fs->buffer, 0, outlen);
			status = LZ4F_decompress(fs->dtx, fs->buffer, &outlen,
									 rp, &read_remain, NULL);
			if (LZ4F_isError(status))
			{
				fs->errcode = status;
				return -1;
			}

			rp += read_remain;

			/*
			 * fill in what space is available in ptr
			 * if the eol flag is set, either skip if one already found or fill up to EOL
			 * if present in the outbuf
			 */
			if (outlen > 0 && dsize < size && eol_found == false)
			{
				char   *p;
				size_t	lib = (eol_flag == 0) ? size - dsize : size -1 -dsize;
				size_t	len = outlen < lib ? outlen : lib;

				if (eol_flag == true && (p = memchr(fs->buffer, '\n', outlen)) &&
					(size_t)(p - fs->buffer + 1) <= len)
				{
					len = p - fs->buffer + 1;
					eol_found = true;
				}

				memcpy((char *)ptr + dsize, fs->buffer, len);
				dsize += len;

				/* move what did not fit, if any, at the begining of the buf */
				if (len < outlen)
					memmove(fs->buffer, fs->buffer + len, outlen - len);
				outlen -= len;
			}

			/* if there is available output, save it */
			if (outlen > 0)
			{
				while (fs->overflowlen + outlen > fs->overflowalloclen)
				{
					fs->overflowalloclen *= 2;
					fs->overflowbuf = pg_realloc(fs->overflowbuf, fs->overflowalloclen);
				}

				memcpy(fs->overflowbuf + fs->overflowlen, fs->buffer, outlen);
				fs->overflowlen += outlen;
			}
		}
	} while (rsize == size && dsize < size && eol_found == 0);

	pg_free(readbuf);

	return (int)dsize;
}

/*
 * Compress size bytes from ptr and write them to the stream.
 */
static int
LZ4File_write(LZ4File *fs, const void *ptr, int size)
{
	size_t	status;
	int		remaining = size;

	if (!fs->inited && LZ4File_init(fs, size, true))
		return -1;

	while (remaining > 0)
	{
		int		chunk = remaining < LZ4_IN_SIZE ? remaining : LZ4_IN_SIZE;
		remaining -= chunk;

		status = LZ4F_compressUpdate(fs->ctx, fs->buffer, fs->buflen,
									 ptr, chunk, NULL);
		if (LZ4F_isError(status))
		{
			fs->errcode = status;
			return -1;
		}

		if (fwrite(fs->buffer, 1, status, fs->fp) != status)
		{
			errno = errno ? : ENOSPC;
			return 1;
		}
	}

	return size;
}

/*
 * fgetc() and gzgetc() equivalent implementation for LZ4 compressed files.
 */
static int
LZ4File_getc(LZ4File *fs)
{
	unsigned char c;

	if (LZ4File_read(fs, &c, 1, false) != 1)
		return EOF;

	return c;
}

/*
 * fgets() and gzgets() equivalent implementation for LZ4 compressed files.
 */
static char *
LZ4File_gets(LZ4File *fs, char *buf, int len)
{
	size_t	dsize;

	dsize = LZ4File_read(fs, buf, len, true);
	if (dsize < 0)
		fatal("failed to read from archive %s", LZ4File_error(fs));

	/* Done reading */
	if (dsize == 0)
		return NULL;

	return buf;
}

/*
 * Finalize (de)compression of a stream. When compressing it will write any
 * remaining content and/or generated footer from the LZ4 API.
 */
static int
LZ4File_close(LZ4File *fs)
{
	FILE	*fp;
	size_t	status;
	int		ret;

	fp = fs->fp;
	if (fs->inited)
	{
		if (fs->compressing)
		{
			status = LZ4F_compressEnd(fs->ctx, fs->buffer, fs->buflen, NULL);
			if (LZ4F_isError(status))
				fatal("failed to end compression: %s", LZ4F_getErrorName(status));
			else if ((ret = fwrite(fs->buffer, 1, status, fs->fp)) != status)
			{
				errno = errno ? : ENOSPC;
				WRITE_ERROR_EXIT;
			}

			status = LZ4F_freeCompressionContext(fs->ctx);
			if (LZ4F_isError(status))
				fatal("failed to end compression: %s", LZ4F_getErrorName(status));
		}
		else
		{
			status = LZ4F_freeDecompressionContext(fs->dtx);
			if (LZ4F_isError(status))
				fatal("failed to end decompression: %s", LZ4F_getErrorName(status));
			pg_free(fs->overflowbuf);
		}

		pg_free(fs->buffer);
	}

	pg_free(fs);

	return fclose(fp);
}
#endif

/*
 * Open a file for reading. 'path' is the file to open, and 'mode' should
 * be either "r" or "rb".
 *
 * If the file at 'path' does not exist, we append the "{.gz,.lz4}" suffix (if
 * 'path' doesn't already have it) and try again. So if you pass "foo" as 'path',
 * this will open either "foo" or "foo.gz" or "foo.lz4", trying in that order.
 *
 * On failure, return NULL with an error code in errno.
 *
 */
cfp *
cfopen_read(const char *path, const char *mode)
{
	cfp		   *fp = NULL;

	if (hasSuffix(path, ".gz"))
		fp = cfopen(path, mode, COMPRESSION_GZIP, 0);
	else if (hasSuffix(path, ".lz4"))
		fp = cfopen(path, mode, COMPRESSION_LZ4, 0);
	else
	{
		fp = cfopen(path, mode, COMPRESSION_NONE, 0);
#ifdef HAVE_LIBZ
		if (fp == NULL)
		{
			char	   *fname;

			fname = psprintf("%s.gz", path);
			fp = cfopen(fname, mode, COMPRESSION_GZIP, 0);
			free_keep_errno(fname);
		}
#endif
#ifdef HAVE_LIBLZ4
		if (fp == NULL)
		{
			char	   *fname;

			fname = psprintf("%s.lz4", path);
			fp = cfopen(fname, mode, COMPRESSION_LZ4, 0);
			free_keep_errno(fname);
		}
#endif
	}

	return fp;
}

/*
 * Open a file for writing. 'path' indicates the path name, and 'mode' must
 * be a filemode as accepted by fopen() and gzopen() that indicates writing
 * ("w", "wb", "a", or "ab").
 *
 * When 'compressionMethod' indicates gzip, a gzip compressed stream is opened,
 * and 'compressionLevel' is used. The ".gz" suffix is automatically added to
 * 'path' in that case. The same applies when 'compressionMethod' indicates lz4,
 * but then the ".lz4" suffix is added instead.
 *
 * It is the caller's responsibility to verify that the requested
 * 'compressionMethod' is supported by the build.
 *
 * On failure, return NULL with an error code in errno.
 */
cfp *
cfopen_write(const char *path, const char *mode,
			 CompressionMethod compressionMethod,
			 int compressionLevel)
{
	cfp		   *fp = NULL;

	switch (compressionMethod)
	{
		case COMPRESSION_NONE:
			fp = cfopen(path, mode, compressionMethod, 0);
			break;
		case COMPRESSION_GZIP:
#ifdef HAVE_LIBZ
			{
				char	   *fname;

				fname = psprintf("%s.gz", path);
				fp = cfopen(fname, mode, compressionMethod, compressionLevel);
				free_keep_errno(fname);
			}
#else
			fatal("not built with zlib support");
#endif
			break;
		case COMPRESSION_LZ4:
#ifdef HAVE_LIBLZ4
			{
				char	   *fname;

				fname = psprintf("%s.lz4", path);
				fp = cfopen(fname, mode, compressionMethod, compressionLevel);
				free_keep_errno(fname);
			}
#else
			fatal("not built with LZ4 support");
#endif
			break;
		default:
			fatal("invalid compression method");
			break;
	}

	return fp;
}

/*
 * This is the workhorse for cfopen() or cfdopen(). It opens file 'path' or
 * associates a stream 'fd', if 'fd' is a valid descriptor, in 'mode'. The
 * descriptor is not dup'ed and it is the caller's responsibility to do so.
 * The caller must verify that the 'compressionMethod' is supported by the
 * current build.
 *
 * On failure, return NULL with an error code in errno.
 */
static cfp *
cfopen_internal(const char *path, int fd, const char *mode,
				CompressionMethod compressionMethod, int compressionLevel)
{
	cfp		   *fp = pg_malloc0(sizeof(cfp));

	fp->compressionMethod = compressionMethod;

	switch (compressionMethod)
	{
		case COMPRESSION_NONE:
			if (fd >= 0)
				fp->fp = fdopen(fd, mode);
			else
				fp->fp = fopen(path, mode);
			if (fp->fp == NULL)
			{
				free_keep_errno(fp);
				fp = NULL;
			}

			break;
		case COMPRESSION_GZIP:
#ifdef HAVE_LIBZ
			if (compressionLevel != Z_DEFAULT_COMPRESSION)
			{
				/*
				 * user has specified a compression level, so tell zlib to use
				 * it
				 */
				char		mode_compression[32];

				snprintf(mode_compression, sizeof(mode_compression), "%s%d",
						 mode, compressionLevel);
				if (fd >= 0)
					fp->fp = gzdopen(fd, mode_compression);
				else
					fp->fp = gzopen(path, mode_compression);
			}
			else
			{
				/* don't specify a level, just use the zlib default */
				if (fd >= 0)
					fp->fp = gzdopen(fd, mode);
				else
					fp->fp = gzopen(path, mode);
			}

			if (fp->fp == NULL)
			{
				free_keep_errno(fp);
				fp = NULL;
			}
#else
			fatal("not built with zlib support");
#endif
			break;
		case COMPRESSION_LZ4:
#ifdef HAVE_LIBLZ4
			{
				LZ4File *lz4fp = pg_malloc0(sizeof(*lz4fp));
				if (fd >= 0)
					lz4fp->fp = fdopen(fd, mode);
				else
					lz4fp->fp = fopen(path, mode);
				if (lz4fp->fp == NULL)
				{
					free_keep_errno(lz4fp);
					fp = NULL;
				}
				if (compressionLevel >= 0)
					lz4fp->prefs.compressionLevel = compressionLevel;
				fp->fp = lz4fp;
			}
#else
			fatal("not built with LZ4 support");
#endif
			break;
		default:
			fatal("invalid compression method");
			break;
	}

	return fp;
}

cfp *
cfopen(const char *path, const char *mode,
	   CompressionMethod compressionMethod,
	   int compressionLevel)
{
	return cfopen_internal(path, -1, mode, compressionMethod, compressionLevel);
}

cfp *
cfdopen(int fd, const char *mode,
		CompressionMethod compressionMethod,
		int compressionLevel)
{
	return cfopen_internal(NULL, fd, mode, compressionMethod, compressionLevel);
}

int
cfread(void *ptr, int size, cfp *fp)
{
	int			ret;

	if (size == 0)
		return 0;

	switch (fp->compressionMethod)
	{
		case COMPRESSION_NONE:
			ret = fread(ptr, 1, size, fp->fp);
			if (ret != size && !feof(fp->fp))
				READ_ERROR_EXIT(fp->fp);

			break;
		case COMPRESSION_GZIP:
#ifdef HAVE_LIBZ
			ret = gzread(fp->fp, ptr, size);
			if (ret != size && !gzeof(fp->fp))
			{
				int			errnum;
				const char *errmsg = gzerror(fp->fp, &errnum);

				fatal("could not read from input file: %s",
					  errnum == Z_ERRNO ? strerror(errno) : errmsg);
			}
#else
			fatal("not built with zlib support");
#endif
			break;
		case COMPRESSION_LZ4:
#ifdef HAVE_LIBLZ4
			ret = LZ4File_read(fp->fp, ptr, size, false);
			if (ret != size && !LZ4File_eof(fp->fp))
				fatal("could not read from input file: %s",
					  LZ4File_error(fp->fp));
#else
			fatal("not built with LZ4 support");
#endif
			break;
		default:
			fatal("invalid compression method");
			break;
	}

	return ret;
}

int
cfwrite(const void *ptr, int size, cfp *fp)
{
	int			ret = 0;

	switch (fp->compressionMethod)
	{
		case COMPRESSION_NONE:
			ret = fwrite(ptr, 1, size, fp->fp);
			break;
		case COMPRESSION_GZIP:
#ifdef HAVE_LIBZ
			ret = gzwrite(fp->fp, ptr, size);
#else
			fatal("not built with zlib support");
#endif
			break;
		case COMPRESSION_LZ4:
#ifdef HAVE_LIBLZ4
			ret = LZ4File_write(fp->fp, ptr, size);
#else
			fatal("not built with LZ4 support");
#endif
			break;
		default:
			fatal("invalid compression method");
			break;
	}

	return ret;
}

int
cfgetc(cfp *fp)
{
	int			ret;

	switch (fp->compressionMethod)
	{
		case COMPRESSION_NONE:
			ret = fgetc(fp->fp);
			if (ret == EOF)
				READ_ERROR_EXIT(fp->fp);

			break;
		case COMPRESSION_GZIP:
#ifdef HAVE_LIBZ
			ret = gzgetc((gzFile)fp->fp);
			if (ret == EOF)
			{
				if (!gzeof(fp->fp))
					fatal("could not read from input file: %s", strerror(errno));
				else
					fatal("could not read from input file: end of file");
			}
#else
			fatal("not built with zlib support");
#endif
			break;
		case COMPRESSION_LZ4:
#ifdef HAVE_LIBLZ4
			ret = LZ4File_getc(fp->fp);
			if (ret == EOF)
			{
				if (!LZ4File_eof(fp->fp))
					fatal("could not read from input file: %s", strerror(errno));
				else
					fatal("could not read from input file: end of file");
			}
#else
			fatal("not built with LZ4 support");
#endif
			break;
		default:
			fatal("invalid compression method");
			break;
	}

	return ret;
}

char *
cfgets(cfp *fp, char *buf, int len)
{
	char	   *ret;

	switch (fp->compressionMethod)
	{
		case COMPRESSION_NONE:
			ret = fgets(buf, len, fp->fp);
			break;
		case COMPRESSION_GZIP:
#ifdef HAVE_LIBZ
			ret = gzgets(fp->fp, buf, len);
#else
			fatal("not built with zlib support");
#endif
			break;
		case COMPRESSION_LZ4:
#ifdef HAVE_LIBLZ4
			ret = LZ4File_gets(fp->fp, buf, len);
#else
			fatal("not built with LZ4 support");
#endif
			break;
		default:
			fatal("invalid compression method");
			break;
	}

	return ret;
}

int
cfclose(cfp *fp)
{
	int			ret;

	if (fp == NULL)
	{
		errno = EBADF;
		return EOF;
	}

	switch (fp->compressionMethod)
	{
		case COMPRESSION_NONE:
			ret = fclose(fp->fp);
			fp->fp = NULL;

			break;
		case COMPRESSION_GZIP:
#ifdef HAVE_LIBZ
			ret = gzclose(fp->fp);
			fp->fp = NULL;
#else
			fatal("not built with zlib support");
#endif
			break;
		case COMPRESSION_LZ4:
#ifdef HAVE_LIBLZ4
			ret = LZ4File_close(fp->fp);
			fp->fp = NULL;
#else
			fatal("not built with LZ4 support");
#endif
			break;
		default:
			fatal("invalid compression method");
			break;
	}

	free_keep_errno(fp);

	return ret;
}

int
cfeof(cfp *fp)
{
	int			ret;

	switch (fp->compressionMethod)
	{
		case COMPRESSION_NONE:
			ret = feof(fp->fp);

			break;
		case COMPRESSION_GZIP:
#ifdef HAVE_LIBZ
			ret = gzeof(fp->fp);
#else
			fatal("not built with zlib support");
#endif
			break;
		case COMPRESSION_LZ4:
#ifdef HAVE_LIBLZ4
			ret = LZ4File_eof(fp->fp);
#else
			fatal("not built with LZ4 support");
#endif
			break;
		default:
			fatal("invalid compression method");
			break;
	}

	return ret;
}

const char *
get_cfp_error(cfp *fp)
{
	const char *errmsg = NULL;

	switch(fp->compressionMethod)
	{
		case COMPRESSION_NONE:
			errmsg = strerror(errno);

			break;
		case COMPRESSION_GZIP:
#ifdef HAVE_LIBZ
			{
				int			errnum;
				errmsg = gzerror(fp->fp, &errnum);

				if (errnum == Z_ERRNO)
					errmsg = strerror(errno);
			}
#else
			fatal("not built with zlib support");
#endif
			break;
		case COMPRESSION_LZ4:
#ifdef HAVE_LIBLZ4
			errmsg = LZ4File_error(fp->fp);
#else
			fatal("not built with LZ4 support");
#endif
			break;
		default:
			fatal("invalid compression method");
			break;
	}

	return errmsg;
}

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
