/*-------------------------------------------------------------------------
 *
 * compress_io.c
 *	 Routines for archivers to write an uncompressed or compressed data
 *	 stream.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This file includes two APIs for dealing with compressed data. The first
 * provides more flexibility, using callbacks to read/write data from the
 * underlying stream. The second API is a wrapper around fopen/gzopen and
 * friends, providing an interface similar to those, but abstracts away
 * the possible compression. Both APIs use libz for the compression, but
 * the second API uses gzip headers, so the resulting files can be easily
 * manipulated with the gzip utility. XXX
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
 *	compressed data one chunk at a time, and ReadDataFromArchive decompresses it
 *	and passes the decompressed data to ahwrite(), until ReadFunc returns 0
 *	to signal EOF.
 *
 *	The interface is the same for compressed and uncompressed streams.
 *
 * Compressed stream API
 * ----------------------
 *
 *	The compressed stream API is a wrapper around the C standard fopen() and
 *	libz's gzopen() APIs. It allows you to use the same functions for
 *	compressed and uncompressed streams. cfopen_read() first tries to open
 *	the file with given name, and if it fails, it tries to open the same
 *	file with a compressed suffix. cfopen_write() opens a file for writing, an
 *	extra argument specifies if the file should be compressed, and adds the
 *	compressed suffix to the filename if so. This allows you to easily handle both
 *	compressed and uncompressed files.
 *
 * IDENTIFICATION
 *	   src/bin/pg_dump/compress_io.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "compress_io.h"
#include "pg_backup_utils.h"

const struct compressLibs
compresslibs[] = {
	{ COMPR_ALG_NONE, "no", "", 0, },
	{ COMPR_ALG_NONE, "none", "", 0, }, /* Alternate name */

// #ifdef HAVE_LIBZ?
	{ COMPR_ALG_LIBZ, "libz", ".gz", Z_DEFAULT_COMPRESSION },
	{ COMPR_ALG_LIBZ, "zlib", ".gz", Z_DEFAULT_COMPRESSION }, /* Alternate name */

#ifdef HAVE_LIBZSTD
	/*
	 * ZSTD doesen't have a #define for it, but 0 means "the current default".
	 * Note that ZSTD_CLEVEL_DEFAULT is currently defined to 3.
	 *
	 * Block size should be ZSTD_DStreamOutSize(), but needs to be
	 * constant, so use ZSTD_BLOCKSIZE_MAX (128kB)
	 */
	{ COMPR_ALG_ZSTD, "zst",  ".zst", 0 },
	{ COMPR_ALG_ZSTD, "zstd", ".zst", 0 }, /* Alternate name */
#endif /* HAVE_LIBZSTD */

	{ 0, NULL, } /* sentinel */
};

/*----------------------
 * Compressor API
 *----------------------
 */

/* typedef appears in compress_io.h */
struct CompressorState
{
	CompressionAlgorithm comprAlg;
	WriteFunc	writeF;

	union {
#ifdef HAVE_LIBZ
		struct {
			z_streamp	zp;
			char	   *zlibOut;
			size_t		zlibOutSize;
		} zlib;
#endif

#ifdef HAVE_LIBZSTD
		/* This is used for compression but not decompression */
		struct {
			// XXX: use one separate ZSTD_CStream per thread: disable on windows ?
			ZSTD_CStream	*cstream;
			ZSTD_outBuffer	output;
			ZSTD_inBuffer	input;
		} zstd;
#endif
	} u;
};

/* Routines that support zlib compressed data I/O */
#ifdef HAVE_LIBZ
static void InitCompressorZlib(CompressorState *cs, Compress *compress);
static void DeflateCompressorZlib(ArchiveHandle *AH, CompressorState *cs,
								  bool flush);
static void ReadDataFromArchiveZlib(ArchiveHandle *AH, ReadFunc readF);
static void WriteDataToArchiveZlib(ArchiveHandle *AH, CompressorState *cs,
								   const char *data, size_t dLen);
static void EndCompressorZlib(ArchiveHandle *AH, CompressorState *cs);
#endif

#ifdef HAVE_LIBZSTD
static ZSTD_CStream *ZstdCStreamParams(Compress *compress);
static void InitCompressorZstd(CompressorState *cs, Compress *compress);
static void EndCompressorZstd(ArchiveHandle *AH, CompressorState *cs);
static void WriteDataToArchiveZstd(ArchiveHandle *AH, CompressorState *cs,
					   const char *data, size_t dLen);
static void ReadDataFromArchiveZstd(ArchiveHandle *AH, ReadFunc readF);
#endif

/* Routines that support uncompressed data I/O */
static void ReadDataFromArchiveNone(ArchiveHandle *AH, ReadFunc readF);
static void WriteDataToArchiveNone(ArchiveHandle *AH, CompressorState *cs,
								   const char *data, size_t dLen);

/* Public interface routines */

/* Allocate a new compressor */
CompressorState *
AllocateCompressor(Compress *compression, WriteFunc writeF)
{
	CompressorState *cs;

	cs = (CompressorState *) pg_malloc0(sizeof(CompressorState));
	cs->writeF = writeF;
	cs->comprAlg = compression->alg;

	/*
	 * Perform compression algorithm specific initialization.
	 */
	Assert (compression->alg != COMPR_ALG_DEFAULT);
	switch (compression->alg)
	{
#ifdef HAVE_LIBZ
	case COMPR_ALG_LIBZ:
		InitCompressorZlib(cs, compression);
		break;
#endif

#ifdef HAVE_LIBZSTD
	case COMPR_ALG_ZSTD:
		InitCompressorZstd(cs, compression);
		break;
#endif

	case COMPR_ALG_NONE:
		/* Do nothing */
		break;
	default:
		/* Should not happen */
		fatal("requested compression not available in this installation");
	}

	return cs;
}

/*
 * Read all compressed data from the input stream (via readF) and print it
 * out with ahwrite().
 */
void
ReadDataFromArchive(ArchiveHandle *AH, ReadFunc readF)
{
	switch (AH->compression.alg)
	{
	case COMPR_ALG_NONE:
		ReadDataFromArchiveNone(AH, readF);
		break;
#ifdef HAVE_LIBZ
	case COMPR_ALG_LIBZ:
		ReadDataFromArchiveZlib(AH, readF);
		break;
#endif

#ifdef HAVE_LIBZSTD
	case COMPR_ALG_ZSTD:
		ReadDataFromArchiveZstd(AH, readF);
		break;
#endif

	default:
		/* Should not happen */
		fatal("requested compression not available in this installation");
	}
}

/*
 * Compress and write data to the output stream (via writeF).
 */
void
WriteDataToArchive(ArchiveHandle *AH, CompressorState *cs,
				   const void *data, size_t dLen)
{
	switch (cs->comprAlg)
	{
#ifdef HAVE_LIBZ
		case COMPR_ALG_LIBZ:
			WriteDataToArchiveZlib(AH, cs, data, dLen);
			break;
#endif

#ifdef HAVE_LIBZSTD
		case COMPR_ALG_ZSTD:
			WriteDataToArchiveZstd(AH, cs, data, dLen);
			break;
#endif
		case COMPR_ALG_NONE:
			WriteDataToArchiveNone(AH, cs, data, dLen);
			break;

		default:
			/* Should not happen */
			fatal("requested compression not available in this installation");
	}
}

/*
 * Terminate compression library context and flush its buffers.
 */
void
EndCompressor(ArchiveHandle *AH, CompressorState *cs)
{
#ifdef HAVE_LIBZ
	if (cs->comprAlg == COMPR_ALG_LIBZ)
		EndCompressorZlib(AH, cs);
#endif

#ifdef HAVE_LIBZSTD
	if (cs->comprAlg == COMPR_ALG_ZSTD)
		EndCompressorZstd(AH, cs);
#endif

	free(cs);
}

/* Private routines, specific to each compression method. */

#ifdef HAVE_LIBZSTD

static void ZSTD_CCtx_setParam_or_die(ZSTD_CStream *cstream,
		ZSTD_cParameter param, int value)

{
	size_t res;
	res = ZSTD_CCtx_setParameter(cstream, param, value);
	if (ZSTD_isError(res))
		fatal("could not set compression parameter: %s",
				ZSTD_getErrorName(res));
}

/* Return a compression stream with parameters set per argument */
static ZSTD_CStream*
ZstdCStreamParams(Compress *compress)
{
	ZSTD_CStream *cstream;
	cstream = ZSTD_createCStream();
	if (cstream == NULL)
		fatal("could not initialize compression library");

	if (compress->level != 0) // XXX: ZSTD_CLEVEL_DEFAULT
	{
		size_t res;
		res = ZSTD_CCtx_setParameter(cstream,
				ZSTD_c_compressionLevel, compress->level);
		if (ZSTD_isError(res))
			fatal("could not set compression level: %s",
					ZSTD_getErrorName(res));
	}

	if (compress->zstd.longdistance) // XXX: ternary
		ZSTD_CCtx_setParam_or_die(cstream,
				ZSTD_c_enableLongDistanceMatching,
				compress->zstd.longdistance);

	if (compress->zstd.checksum)
		ZSTD_CCtx_setParam_or_die(cstream, ZSTD_c_checksumFlag,
				compress->zstd.checksum);

// not supported in my library ?
	if (compress->zstd.threads)
		ZSTD_CCtx_setParam_or_die(cstream, ZSTD_c_nbWorkers,
				compress->zstd.threads);

#if 0
	/* Still marked as experimental */
	if (compress->zstd.rsyncable)
		ZSTD_CCtx_setParam_or_die(cstream, ZSTD_c_rsyncable, 1);
#endif

	return cstream;
}

static void
InitCompressorZstd(CompressorState *cs, Compress *compress)
{
	cs->u.zstd.cstream = ZstdCStreamParams(compress);
	/* XXX: initialize safely like the corresponding zlib "paranoia" */
	cs->u.zstd.output.size = ZSTD_CStreamOutSize();
	cs->u.zstd.output.dst = pg_malloc(cs->u.zstd.output.size);
	cs->u.zstd.output.pos = 0;
}

static void
EndCompressorZstd(ArchiveHandle *AH, CompressorState *cs)
{
	ZSTD_outBuffer	*output = &cs->u.zstd.output;

	for (;;)
	{
		size_t res;

		res = ZSTD_compressStream2(cs->u.zstd.cstream, output,
				&cs->u.zstd.input, ZSTD_e_end);

		if (output->pos > 0)
			cs->writeF(AH, output->dst, output->pos);

		if (res == 0)
			break;

		if (ZSTD_isError(res))
			fatal("could not close compression stream: %s",
					ZSTD_getErrorName(res));
	}

	// XXX: retval
	ZSTD_freeCStream(cs->u.zstd.cstream);
}

static void
WriteDataToArchiveZstd(ArchiveHandle *AH, CompressorState *cs,
					   const char *data, size_t dLen)
{
	ZSTD_inBuffer	*input = &cs->u.zstd.input;
	ZSTD_outBuffer	*output = &cs->u.zstd.output;

	input->src = (void *) unconstify(char *, data);
	input->size = dLen;
	input->pos = 0;

	while (input->pos != input->size)
	{
		size_t		res;

		res = ZSTD_compressStream2(cs->u.zstd.cstream, output,
				input, ZSTD_e_continue);

		if (output->pos == output->size ||
				input->pos != input->size)
		{
			/*
			 * Extra paranoia: avoid zero-length chunks, since a zero length
			 * chunk is the EOF marker in the custom format. This should never
			 * happen but...
			 */
			if (output->pos > 0)
				cs->writeF(AH, output->dst, output->pos);

			output->pos = 0;
		}

		if (ZSTD_isError(res))
			fatal("could not compress data: %s", ZSTD_getErrorName(res));
	}
}

/* Read data from a compressed zstd archive */
static void
ReadDataFromArchiveZstd(ArchiveHandle *AH, ReadFunc readF)
{
	ZSTD_DStream	*dstream;
	ZSTD_outBuffer	output;
	ZSTD_inBuffer	input;
	size_t			res;
	size_t			input_size;

	dstream = ZSTD_createDStream();
	if (dstream == NULL)
		fatal("could not initialize compression library");

	input_size = ZSTD_DStreamInSize();
	input.src = pg_malloc(input_size);

	output.size = ZSTD_DStreamOutSize();
	output.dst = pg_malloc(output.size);

	/* read compressed data */
	for (;;)
	{
		size_t			cnt;

		input.size = input_size; // XXX: the buffer can grow, we shouldn't keep resetting it to the original value..
		cnt = readF(AH, (char **)unconstify(void **, &input.src), &input.size);
		input.pos = 0;
		input.size = cnt;

		if (cnt == 0)
			break;

		while (input.pos < input.size)
		{
			/* decompress */
			output.pos = 0;
			res = ZSTD_decompressStream(dstream, &output, &input);

			if (ZSTD_isError(res))
				fatal("could not decompress data: %s", ZSTD_getErrorName(res));

			/* write to output handle */
			((char *)output.dst)[output.pos] = '\0';
			ahwrite(output.dst, 1, output.pos, AH);
			// if (res == 0)
				// break;
		}
	}

	pg_free(unconstify(void *, input.src));
	pg_free(output.dst);
}

#endif		/* HAVE_LIBZSTD */

#ifdef HAVE_LIBZ
/*
 * Functions for zlib compressed output.
 */

static void
InitCompressorZlib(CompressorState *cs, Compress *compress)
{
	z_streamp	zp;

	zp = cs->u.zlib.zp = (z_streamp) pg_malloc(sizeof(z_stream));
	zp->zalloc = Z_NULL;
	zp->zfree = Z_NULL;
	zp->opaque = Z_NULL;

	/*
	 * zlibOutSize is the buffer size we tell zlib it can output to.  We
	 * actually allocate one extra byte because some routines want to append a
	 * trailing zero byte to the zlib output.
	 */
	cs->u.zlib.zlibOut = (char *) pg_malloc(ZLIB_OUT_SIZE + 1);
	cs->u.zlib.zlibOutSize = ZLIB_OUT_SIZE;

	if (deflateInit(zp, compress->level) != Z_OK)
		fatal("could not initialize compression library: %s",
			  zp->msg);

	/* Just be paranoid - maybe End is called after Start, with no Write */
	zp->next_out = (void *) cs->u.zlib.zlibOut;
	zp->avail_out = cs->u.zlib.zlibOutSize;
}

static void
EndCompressorZlib(ArchiveHandle *AH, CompressorState *cs)
{
	z_streamp	zp = cs->u.zlib.zp;

	zp->next_in = NULL;
	zp->avail_in = 0;

	/* Flush any remaining data from zlib buffer */
	DeflateCompressorZlib(AH, cs, true);

	if (deflateEnd(zp) != Z_OK)
		fatal("could not close compression stream: %s", zp->msg);

	free(cs->u.zlib.zlibOut);
	free(cs->u.zlib.zp);
}

static void
DeflateCompressorZlib(ArchiveHandle *AH, CompressorState *cs, bool flush)
{
	z_streamp	zp = cs->u.zlib.zp;
	char	   *out = cs->u.zlib.zlibOut;
	int			res = Z_OK;

	while (cs->u.zlib.zp->avail_in != 0 || flush)
	{
		res = deflate(zp, flush ? Z_FINISH : Z_NO_FLUSH);
		if (res == Z_STREAM_ERROR)
			fatal("could not compress data: %s", zp->msg);
		if ((flush && (zp->avail_out < cs->u.zlib.zlibOutSize))
			|| (zp->avail_out == 0)
			|| (zp->avail_in != 0)
			)
		{
			/*
			 * Extra paranoia: avoid zero-length chunks, since a zero length
			 * chunk is the EOF marker in the custom format. This should never
			 * happen but...
			 */
			if (zp->avail_out < cs->u.zlib.zlibOutSize)
			{
				/*
				 * Any write function should do its own error checking but to
				 * make sure we do a check here as well...
				 */
				size_t		len = cs->u.zlib.zlibOutSize - zp->avail_out;

				cs->writeF(AH, out, len);
			}
			zp->next_out = (void *) out;
			zp->avail_out = cs->u.zlib.zlibOutSize;
		}

		if (res == Z_STREAM_END)
			break;
	}
}

static void
WriteDataToArchiveZlib(ArchiveHandle *AH, CompressorState *cs,
					   const char *data, size_t dLen)
{
	cs->u.zlib.zp->next_in = (void *) unconstify(char *, data);
	cs->u.zlib.zp->avail_in = dLen;
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

/*
 * cfp represents an open stream, wrapping the underlying compressed or
 * uncompressed file object.  This is opaque to the callers.
 */
struct cfp
{
	CompressionAlgorithm alg;

	union {
		FILE	   *fp;

#ifdef HAVE_LIBZ
		gzFile		gzfp;
#endif

#ifdef HAVE_LIBZSTD
		struct {
			/* This is a normal file to which we read/write compressed data */
			FILE			*fp;
			// XXX: use one separate ZSTD_CStream per thread: disable on windows ?
			ZSTD_CStream	*cstream;
			ZSTD_DStream	*dstream;
			ZSTD_outBuffer	output;
			ZSTD_inBuffer	input;
		} zstd;
#endif
	} u;
};

static int	hasSuffix(const char *filename);

/* free() without changing errno; useful in several places below */
static void
free_keep_errno(void *p)
{
	int			save_errno = errno;

	free(p);
	errno = save_errno;
}

/*
 * Open a file for reading. 'path' is the file to open, and 'mode' should
 * be either "r" or "rb".
 *
 * If the file at 'path' does not exist, we search with compressed suffix (if 'path'
 * doesn't already have one) and try again.
 *
 * On failure, return NULL with an error code in errno.
 */
cfp *
cfopen_read(const char *path, const char *mode, Compress *compression)
{
	cfp		   *fp;

	if (hasSuffix(path))
		fp = cfopen(path, mode, compression);
	else
	{
		fp = cfopen(path, mode, compression);
		if (fp == NULL)
		{
			char	   *fname;
			const char *suffix = compress_suffix(compression);

			fname = psprintf("%s%s", path, suffix);
			fp = cfopen(fname, mode, compression);
			free_keep_errno(fname);
		}
	}
	return fp;
}

/*
 * Open a file for writing. 'path' indicates the path name, and 'mode' must
 * be a filemode as accepted by fopen() and gzopen() that indicates writing
 * ("w", "wb", "a", or "ab").
 *
 * Use compression if specified.
 * The appropriate suffix is automatically added to 'path' in that case.
 *
 * On failure, return NULL with an error code in errno.
 */
cfp *
cfopen_write(const char *path, const char *mode, Compress *compression)
{
	cfp		   *fp;

	if (compression->alg == COMPR_ALG_NONE)
		fp = cfopen(path, mode, compression);
	else
	{
		char	   *fname;
		const char *suffix = compress_suffix(compression);

		fname = psprintf("%s%s", path, suffix);
		fp = cfopen(fname, mode, compression);
		free_keep_errno(fname);
	}
	return fp;
}

/*
 * Opens file 'path' in 'mode'. If 'alg' is COMPR_ALG_ZLIB, the file
 * is opened with libz gzopen(), otherwise with plain fopen().
 *
 * On failure, return NULL with an error code in errno.
 */
cfp *
cfopen(const char *path, const char *mode, Compress *compression)
{
	cfp		   *fp = pg_malloc0(sizeof(cfp));

	fp->alg = compression->alg;

	switch (compression->alg)
	{
#ifdef HAVE_LIBZ
	case COMPR_ALG_LIBZ:
		if (compression->level != Z_DEFAULT_COMPRESSION)
		{
			/* user has specified a compression level, so tell zlib to use it */
			char		mode_compression[32];

			snprintf(mode_compression, sizeof(mode_compression), "%s%d",
					 mode, compression->level);
			fp->u.gzfp = gzopen(path, mode_compression);
		}
		else
		{
			/* don't specify a level, just use the zlib default */
			fp->u.gzfp = gzopen(path, mode);
		}

		if (fp->u.gzfp == NULL)
		{
			free_keep_errno(fp);
			fp = NULL;
		}
		return fp;
#endif

#ifdef HAVE_LIBZSTD
	case COMPR_ALG_ZSTD:
		fp->u.zstd.fp = fopen(path, mode);
		if (fp->u.zstd.fp == NULL)
		{
			free_keep_errno(fp);
			fp = NULL;
		}
		else if (mode[0] == 'w' || mode[0] == 'a' ||
			strchr(mode, '+') != NULL)
		{
			fp->u.zstd.output.size = ZSTD_CStreamOutSize();
			fp->u.zstd.output.dst = pg_malloc0(fp->u.zstd.output.size);
			fp->u.zstd.cstream = ZstdCStreamParams(compression);
		}
		else if (strchr(mode, 'r'))
		{
			fp->u.zstd.input.src = pg_malloc0(ZSTD_DStreamInSize());
			fp->u.zstd.dstream = ZSTD_createDStream();
			if (fp->u.zstd.dstream == NULL)
				fatal("could not initialize compression library");
		} // XXX else: bad mode
		return fp;
#endif

	case COMPR_ALG_NONE:
		fp->u.fp = fopen(path, mode);
		if (fp->u.fp == NULL)
		{
			free_keep_errno(fp);
			fp = NULL;
		}
		return fp;

	default:
		/* Should not happen */
		fatal("requested compression not available in this installation");
	}
}

/*
 * Open a file descriptor, with specified compression.
 * Returns an opaque cfp object.
 */
cfp *
cfdopen(int fd, const char *mode, Compress *compression)
{
	cfp		   *fp = pg_malloc0(sizeof(cfp));

	fp->alg = compression->alg;

	switch (compression->alg)
	{
#ifdef HAVE_LIBZ
	case COMPR_ALG_LIBZ:
		if (compression->level != Z_DEFAULT_COMPRESSION)
		{
			/* user has specified a compression level, so tell zlib to use it */
			char		mode_compression[32];

			snprintf(mode_compression, sizeof(mode_compression), "%s%d",
					 mode, compression->level);
			fp->u.gzfp = gzdopen(fd, mode_compression);
		}
		else
		{
			/* don't specify a level, just use the zlib default */
			fp->u.gzfp = gzdopen(fd, mode);
		}

		if (fp->u.gzfp == NULL)
		{
			free_keep_errno(fp);
			fp = NULL;
		}
		return fp;
#endif

#ifdef HAVE_LIBZSTD
	case COMPR_ALG_ZSTD:
		fp->u.zstd.fp = fdopen(fd, mode);
		if (fp->u.zstd.fp == NULL)
		{
			free_keep_errno(fp);
			fp = NULL;
		}
		else if (mode[0] == 'w' || mode[0] == 'a' ||
			strchr(mode, '+') != NULL)
		{
			fp->u.zstd.output.size = ZSTD_CStreamOutSize();
			fp->u.zstd.output.dst = pg_malloc0(fp->u.zstd.output.size);
			fp->u.zstd.cstream = ZstdCStreamParams(compression);
		}
		else if (strchr(mode, 'r'))
		{
			fp->u.zstd.input.src = pg_malloc0(ZSTD_DStreamInSize());
			fp->u.zstd.dstream = ZSTD_createDStream();
			if (fp->u.zstd.dstream == NULL)
				fatal("could not initialize compression library");
		} // XXX else: bad mode
		return fp;
#endif

	case COMPR_ALG_NONE:
		fp->u.fp = fdopen(fd, mode);
		if (fp->u.fp == NULL)
		{
			free_keep_errno(fp);
			fp = NULL;
		}
		else
			setvbuf(fp->u.fp, NULL, _IONBF, 0);
		return fp;

	default:
		/* Should not happen */
		fatal("requested compression not available in this installation");
	}
}

int
cfread(void *ptr, int size, cfp *fp)
{
	int			ret;

	if (size == 0)
		return 0;

#ifdef HAVE_LIBZ
	if (fp->alg == COMPR_ALG_LIBZ)
	{
		ret = gzread(fp->u.gzfp, ptr, size);
		if (ret != size && !gzeof(fp->u.gzfp))
		{
			int			errnum;
			const char *errmsg = gzerror(fp->u.gzfp, &errnum);

			fatal("could not read from input file: %s",
				  errnum == Z_ERRNO ? strerror(errno) : errmsg);
		}
		return ret;
	}
#endif

#ifdef HAVE_LIBZSTD
	if (fp->alg == COMPR_ALG_ZSTD)
	{
		ZSTD_outBuffer	*output = &fp->u.zstd.output;
		ZSTD_inBuffer	*input = &fp->u.zstd.input;
		size_t			input_size = ZSTD_DStreamInSize();
		/* input_size is the allocated size */
		size_t			res, cnt;

		output->size = size;
		output->dst = ptr;
		output->pos = 0;

		for (;;)
		{
			Assert(input->pos <= input->size);
			Assert(input->size <= input_size);

			/* If the input is completely consumed, start back at the beginning */
			if (input->pos == input->size)
			{
				/* input->size is size produced by "fread" */
				input->size = 0;
				/* input->pos is position consumed by decompress */
				input->pos = 0;
			}

			/* read compressed data if we must produce more input */
			if (input->pos == input->size)
			{
				cnt = fread(unconstify(void *, input->src), 1, input_size, fp->u.zstd.fp);
				input->size = cnt;

				/* If we have no input to consume, we're done */
				if (cnt == 0)
					break;
			}

			Assert(cnt >= 0);
			Assert(input->size <= input_size);

			/* Now consume as much as possible */
			for ( ; input->pos < input->size; )
			{
				/* decompress */
				res = ZSTD_decompressStream(fp->u.zstd.dstream, output, input);
				if (res == 0)
					break; /* End of frame */
				if (output->pos == output->size)
					break; /* No more room for output */
				if (ZSTD_isError(res))
					fatal("could not decompress data: %s", ZSTD_getErrorName(res));
			}

			if (output->pos == output->size)
				break; /* We read all the data that fits */
		}

		return output->pos;
	}
#endif

	ret = fread(ptr, 1, size, fp->u.fp);
	if (ret != size && !feof(fp->u.fp))
		READ_ERROR_EXIT(fp->u.fp);
	return ret;
}

int
cfwrite(const void *ptr, int size, cfp *fp)
{
#ifdef HAVE_LIBZ
	if (fp->alg == COMPR_ALG_LIBZ)
		return gzwrite(fp->u.gzfp, ptr, size);
#endif

#ifdef HAVE_LIBZSTD
	if (fp->alg == COMPR_ALG_ZSTD)
	{
		size_t      res, cnt;
		ZSTD_outBuffer	*output = &fp->u.zstd.output;
		ZSTD_inBuffer	*input = &fp->u.zstd.input;

		input->src = ptr;
		input->size = size;
		input->pos = 0;

		/* Consume all input, and flush later */
		while (input->pos != input->size)
		{
			output->pos = 0;
			res = ZSTD_compressStream2(fp->u.zstd.cstream, output, input, ZSTD_e_continue);
			if (ZSTD_isError(res))
				fatal("could not compress data: %s", ZSTD_getErrorName(res));

			cnt = fwrite(output->dst, 1, output->pos, fp->u.zstd.fp);
			if (cnt != output->pos)
				fatal("could not write data: %s", strerror(errno));
		}

		return size;
	}
#endif

	return fwrite(ptr, 1, size, fp->u.fp);
}

int
cfgetc(cfp *fp)
{
	int			ret;

#ifdef HAVE_LIBZ
	if (fp->alg == COMPR_ALG_LIBZ)
	{
		ret = gzgetc(fp->u.gzfp);
		if (ret == EOF)
		{
			if (!gzeof(fp->u.gzfp))
				fatal("could not read from input file: %s", strerror(errno));
			else
				fatal("could not read from input file: end of file");
		}
		return ret;
	}
#endif

#ifdef HAVE_LIBZSTD
	if (fp->alg == COMPR_ALG_ZSTD)
	{
		if (cfread(&ret, 1, fp) != 1)
		{
			if (feof(fp->u.zstd.fp))
				fatal("could not read from input file: end of file");
			else
				fatal("could not read from input file: %s", strerror(errno));
		}
		return ret;
	}
#endif

	ret = fgetc(fp->u.fp);
	if (ret == EOF)
		READ_ERROR_EXIT(fp->u.fp);
	return ret;
}

char *
cfgets(cfp *fp, char *buf, int len)
{
#ifdef HAVE_LIBZ
	if (fp->alg == COMPR_ALG_LIBZ)
		return gzgets(fp->u.gzfp, buf, len);
#endif

#ifdef HAVE_LIBZSTD
	if (fp->alg == COMPR_ALG_ZSTD)
	{
		/*
		 * Read one byte at a time until newline or EOF.
		 * This is only used to read the list of blobs, and the I/O is
		 * buffered anyway.
		 */
		int i, res;
		for (i = 0; i < len - 1; ++i)
		{
			res = cfread(&buf[i], 1, fp);
			if (res != 1)
				break;
			if (buf[i] == '\n')
			{
				++i;
				break;
			}
		}
		buf[i] = '\0';
		return i > 0 ? buf : 0;
	}
#endif

	return fgets(buf, len, fp->u.fp);
}

/* Close the given compressed or uncompressed stream; return 0 on success. */
int
cfclose(cfp *fp)
{
	int			result;

	if (fp == NULL)
	{
		errno = EBADF;
		return EOF;
	}
#ifdef HAVE_LIBZ
	if (fp->alg == COMPR_ALG_LIBZ)
	{
		result = gzclose(fp->u.gzfp);
		fp->u.gzfp = NULL;
		return result;
	}
#endif

#ifdef HAVE_LIBZSTD
	if (fp->alg == COMPR_ALG_ZSTD)
	{
		ZSTD_outBuffer	*output = &fp->u.zstd.output;
		ZSTD_inBuffer	*input = &fp->u.zstd.input;
		size_t res, cnt;

		if (fp->u.zstd.cstream)
		{
			for (;;)
			{
				output->pos = 0;
				res = ZSTD_compressStream2(fp->u.zstd.cstream, output, input, ZSTD_e_end);
				if (ZSTD_isError(res))
					fatal("could not compress data: %s", ZSTD_getErrorName(res));
				cnt = fwrite(output->dst, 1, output->pos, fp->u.zstd.fp);
				if (cnt != output->pos)
					fatal("could not write data: %s", strerror(errno));
				if (res == 0)
					break;
			}

			ZSTD_freeCStream(fp->u.zstd.cstream);
			pg_free(fp->u.zstd.output.dst);
		}

		if (fp->u.zstd.dstream)
		{
			ZSTD_freeDStream(fp->u.zstd.dstream);
			pg_free(unconstify(void *, fp->u.zstd.input.src));
		}

		result = fclose(fp->u.zstd.fp);
		fp->u.zstd.fp = NULL;
		return result;
	}
#endif

	result = fclose(fp->u.fp);
	fp->u.fp = NULL;
	free_keep_errno(fp);
	return result;
}

int
cfeof(cfp *fp)
{
#ifdef HAVE_LIBZ
	if (fp->alg == COMPR_ALG_LIBZ)
		return gzeof(fp->u.gzfp);
#endif

#ifdef HAVE_LIBZSTD
	if (fp->alg == COMPR_ALG_ZSTD)
		return feof(fp->u.zstd.fp);
#endif

	return feof(fp->u.fp);
}

const char *
get_cfp_error(cfp *fp)
{
#ifdef HAVE_LIBZ
	if (fp->alg == COMPR_ALG_LIBZ)
	{
		int			errnum;
		const char *errmsg = gzerror(fp->u.gzfp, &errnum);

		if (errnum != Z_ERRNO)
			return errmsg;
	}
#endif
	return strerror(errno);
}

/* Return true iff the filename has a known compression suffix */
static int
hasSuffix(const char *filename)
{
	for (int i = 0; compresslibs[i].name != NULL; ++i)
	{
		const char	*suffix = compresslibs[i].suffix;
		int			filenamelen = strlen(filename);
		int			suffixlen = strlen(suffix);

		/* COMPR_ALG_NONE has an empty "suffix", which doesn't count */
		if (suffixlen == 0)
			continue;

		if (filenamelen < suffixlen)
			continue;

		if (memcmp(&filename[filenamelen - suffixlen],
					  suffix, suffixlen) == 0)
			return true;
	}

	return false;
}

/*
 * Return a string for the given AH's compression.
 * The string is statically allocated.
 */
const char *
compress_suffix(Compress *compression)
{
	for (int i = 0; compresslibs[i].name != NULL; ++i)
	{
		if (compression->alg != compresslibs[i].alg)
			continue;

		return compresslibs[i].suffix;
	}

	return "";
}
