/*-------------------------------------------------------------------------
 *
 * z_stream.c
 *	  Functions implementing streaming compression algorithms
 *
 * Copyright (c) 2018-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/z_stream.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"
#include "pg_config.h"
#include "common/z_stream.h"
#include "utils/elog.h"

typedef struct
{
	/*
	 * Id of compression algorithm.
	 */
	pg_compress_algorithm algorithm;

	/*
	 * Create new compression stream. level: compression level
	 */
	void	   *(*create_compressor) (int level);

	/*
	 * Create new decompression stream.
	 */
	void	   *(*create_decompressor) ();

	/*
	 * Decompress up to "src_size" compressed bytes from *src and write up to
	 * "dst_size" raw (decompressed) bytes to *dst. Number of decompressed
	 * bytes written to *dst is stored in *dst_processed. Number of compressed
	 * bytes read from *src is stored in *src_processed.
	 *
	 * Return codes: ZS_OK if no errors were encountered during decompression
	 * attempt. This return code does not guarantee that *src_processed > 0 or
	 * *dst_processed > 0.
	 *
	 * ZS_STREAM_END if encountered end of compressed data stream.
	 *
	 * ZS_DECOMPRESS_ERROR if encountered an error during decompression
	 * attempt.
	 */
	int			(*decompress) (void *ds, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed);

	/*
	 * Returns true if there is some data left in internal decompression
	 * buffers
	 */
	bool		(*decompress_buffered_data) (void *ds);

	/*
	 * Compress up to "src_size" raw (non-compressed) bytes from *src and
	 * write up to "dst_size" compressed bytes to *dst. Number of compressed
	 * bytes written to *dst is stored in *dst_processed. Number of
	 * non-compressed bytes read from *src is stored in *src_processed.
	 *
	 * Return codes: ZS_OK if no errors were encountered during compression
	 * attempt. This return code does not guarantee that *src_processed > 0 or
	 * *dst_processed > 0.
	 *
	 * ZS_COMPRESS_ERROR if encountered an error during compression attempt.
	 */
	int			(*compress) (void *cs, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed);

	/*
	 * Returns true if there is some data left in internal compression buffers
	 */
	bool		(*compress_buffered_data) (void *ds);

	/*
	 * Free compression stream created by create_compressor function.
	 */
	void		(*free_compressor) (void *cs);

	/*
	 * Free decompression stream created by create_decompressor function.
	 */
	void		(*free_decompressor) (void *ds);

	/*
	 * Get compressor error message.
	 */
	char const *(*compress_error) (void *cs);

	/*
	 * Get decompressor error message.
	 */
	char const *(*decompress_error) (void *ds);

	int			(*end_compression) (void *cs, void *dst, size_t dst_size, size_t *dst_processed);
}			ZAlgorithm;

struct ZStream
{
	ZAlgorithm const *algorithm;
	void	   *stream;
};

#ifndef FRONTEND
#include "utils/palloc.h"
#include "utils/memutils.h"
#define ALLOC(size) MemoryContextAlloc(TopMemoryContext, size)
#define FREE(size) pfree(size)
#else
#define ALLOC(size) malloc(size)
#define FREE(size) free(size)
#endif

#if HAVE_LIBZSTD

#include <stdlib.h>
#include <zstd.h>

/*
 * Maximum allowed back-reference distance, expressed as power of 2.
 * This setting controls max compressor/decompressor window size.
 * More details https://github.com/facebook/zstd/blob/v1.4.7/lib/zstd.h#L536
 */
#define ZSTD_WINDOWLOG_LIMIT 23 /* set max window size to 8MB */


typedef struct ZS_ZSTD_CStream
{
	ZSTD_CStream *stream;
	bool		has_buffered_data;
	char const *error;			/* error message */
}			ZS_ZSTD_CStream;

typedef struct ZS_ZSTD_DStream
{
	ZSTD_DStream *stream;
	bool		has_buffered_data;
	char const *error;			/* error message */
}			ZS_ZSTD_DStream;

static void *
zstd_create_compressor(int level)
{
	size_t		rc;
	ZS_ZSTD_CStream *c_stream = (ZS_ZSTD_CStream *) ALLOC(sizeof(ZS_ZSTD_CStream));

	c_stream->stream = ZSTD_createCStream();
	c_stream->has_buffered_data = false;
	rc = ZSTD_initCStream(c_stream->stream, level);
	if (ZSTD_isError(rc))
	{
		ZSTD_freeCStream(c_stream->stream);
		FREE(c_stream);
		return NULL;
	}
#if ZSTD_VERSION_MAJOR > 1 || ZSTD_VERSION_MINOR > 3
	ZSTD_CCtx_setParameter(c_stream->stream, ZSTD_c_windowLog, ZSTD_WINDOWLOG_LIMIT);
#endif
	c_stream->error = NULL;
	return c_stream;
}

static void *
zstd_create_decompressor()
{
	size_t		rc;
	ZS_ZSTD_DStream *d_stream = (ZS_ZSTD_DStream *) ALLOC(sizeof(ZS_ZSTD_DStream));

	d_stream->stream = ZSTD_createDStream();
	d_stream->has_buffered_data = false;
	rc = ZSTD_initDStream(d_stream->stream);
	if (ZSTD_isError(rc))
	{
		ZSTD_freeDStream(d_stream->stream);
		FREE(d_stream);
		return NULL;
	}
#if ZSTD_VERSION_MAJOR > 1 || ZSTD_VERSION_MINOR > 3
	ZSTD_DCtx_setParameter(d_stream->stream, ZSTD_d_windowLogMax, ZSTD_WINDOWLOG_LIMIT);
#endif
	d_stream->error = NULL;
	return d_stream;
}

static int
zstd_decompress(void *d_stream, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed)
{
	ZS_ZSTD_DStream *ds = (ZS_ZSTD_DStream *) d_stream;
	ZSTD_inBuffer in;
	ZSTD_outBuffer out;
	size_t		rc;

	in.src = src;
	in.pos = 0;
	in.size = src_size;

	out.dst = dst;
	out.pos = 0;
	out.size = dst_size;

	rc = ZSTD_decompressStream(ds->stream, &out, &in);

	*src_processed = in.pos;
	*dst_processed = out.pos;
	if (ZSTD_isError(rc))
	{
		ds->error = ZSTD_getErrorName(rc);
		return ZS_DECOMPRESS_ERROR;
	}

	if (rc == 0)
	{
		return ZS_STREAM_END;
	}

	/*
	 * if `output.pos == output.size`, there might be some data left within
	 * internal buffers
	 */
	ds->has_buffered_data = out.pos == out.size;

	return ZS_OK;
}

static bool
zstd_decompress_buffered_data(void *d_stream)
{
	ZS_ZSTD_DStream *ds = (ZS_ZSTD_DStream *) d_stream;

	return ds->has_buffered_data;
}

static int
zstd_compress(void *c_stream, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed)
{
	ZS_ZSTD_CStream *cs = (ZS_ZSTD_CStream *) c_stream;
	ZSTD_inBuffer in;
	ZSTD_outBuffer out;

	in.src = src;
	in.pos = 0;
	in.size = src_size;

	out.dst = dst;
	out.pos = 0;
	out.size = dst_size;

	if (in.pos < src_size)		/* Has something to compress in input buffer */
	{
		size_t		rc = ZSTD_compressStream(cs->stream, &out, &in);

		*dst_processed = out.pos;
		*src_processed = in.pos;
		if (ZSTD_isError(rc))
		{
			cs->error = ZSTD_getErrorName(rc);
			return ZS_COMPRESS_ERROR;
		}
	}

	if (in.pos == src_size)		/* All data is compressed: flush internal zstd
								 * buffer */
	{
		size_t		tx_not_flushed = ZSTD_flushStream(cs->stream, &out);

		*dst_processed = out.pos;
		cs->has_buffered_data = tx_not_flushed > 0;
	}
	else
	{
		cs->has_buffered_data = false;
	}

	return ZS_OK;
}

static bool
zstd_compress_buffered_data(void *c_stream)
{
	ZS_ZSTD_CStream *cs = (ZS_ZSTD_CStream *) c_stream;

	return cs->has_buffered_data;
}

static int
zstd_end(void *c_stream, void *dst, size_t dst_size, size_t *dst_processed)
{
	size_t		tx_not_flushed;
	ZS_ZSTD_CStream *cs = (ZS_ZSTD_CStream *) c_stream;
	ZSTD_outBuffer output;

	output.dst = dst;
	output.pos = 0;
	output.size = dst_size;

	do
	{
		tx_not_flushed = ZSTD_endStream(cs->stream, &output);
	} while ((tx_not_flushed > 0) && (output.pos < output.size));

	*dst_processed = output.pos;

	cs->has_buffered_data = tx_not_flushed > 0;
	return ZS_OK;
}

static void
zstd_free_compressor(void *c_stream)
{
	ZS_ZSTD_CStream *cs = (ZS_ZSTD_CStream *) c_stream;

	if (cs != NULL)
	{
		ZSTD_freeCStream(cs->stream);
		FREE(cs);
	}
}

static void
zstd_free_decompressor(void *d_stream)
{
	ZS_ZSTD_DStream *ds = (ZS_ZSTD_DStream *) d_stream;

	if (ds != NULL)
	{
		ZSTD_freeDStream(ds->stream);
		FREE(ds);
	}
}

static char const *
zstd_compress_error(void *c_stream)
{
	ZS_ZSTD_CStream *cs = (ZS_ZSTD_CStream *) c_stream;

	return cs->error;
}

static char const *
zstd_decompress_error(void *d_stream)
{
	ZS_ZSTD_DStream *ds = (ZS_ZSTD_DStream *) d_stream;

	return ds->error;
}

static ZAlgorithm const zstd_algorithm = {
	.algorithm = PG_COMPRESSION_ZSTD,
	.create_compressor = zstd_create_compressor,
	.create_decompressor = zstd_create_decompressor,
	.decompress = zstd_decompress,
	.decompress_buffered_data = zstd_decompress_buffered_data,
	.compress = zstd_compress,
	.compress_buffered_data = zstd_compress_buffered_data,
	.free_compressor = zstd_free_compressor,
	.free_decompressor = zstd_free_decompressor,
	.compress_error = zstd_compress_error,
	.decompress_error = zstd_decompress_error,
	.end_compression = zstd_end
};

#endif

#if HAVE_LIBZ

#include <stdlib.h>
#include <zlib.h>


static void *
zlib_create_compressor(int level)
{
	int			rc;
	z_stream   *c_stream = (z_stream *) ALLOC(sizeof(z_stream));

	memset(c_stream, 0, sizeof(*c_stream));
	rc = deflateInit(c_stream, level);
	if (rc != Z_OK)
	{
		FREE(c_stream);
		return NULL;
	}
	return c_stream;
}

static void *
zlib_create_decompressor()
{
	int			rc;
	z_stream   *d_stream = (z_stream *) ALLOC(sizeof(z_stream));

	memset(d_stream, 0, sizeof(*d_stream));
	rc = inflateInit(d_stream);
	if (rc != Z_OK)
	{
		FREE(d_stream);
		return NULL;
	}
	return d_stream;
}

static int
zlib_decompress(void *d_stream, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed)
{
	z_stream   *ds = (z_stream *) d_stream;
	int			rc;

	ds->next_in = (Bytef *) src;
	ds->avail_in = src_size;
	ds->next_out = (Bytef *) dst;
	ds->avail_out = dst_size;

	rc = inflate(ds, Z_SYNC_FLUSH);
	*src_processed = src_size - ds->avail_in;
	*dst_processed = dst_size - ds->avail_out;

	if (rc == Z_STREAM_END)
	{
		return ZS_STREAM_END;
	}
	if (rc != Z_OK && rc != Z_BUF_ERROR)
	{
		return ZS_DECOMPRESS_ERROR;
	}

	return ZS_OK;
}

static bool
zlib_decompress_buffered_data(void *d_stream)
{
	z_stream   *ds = (z_stream *) d_stream;
	unsigned	deflate_pending = 0;

	return deflatePending(ds, &deflate_pending, Z_NULL) > 0;
}

static int
zlib_compress(void *c_stream, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed)
{
	z_stream   *cs = (z_stream *) c_stream;
	int			rc PG_USED_FOR_ASSERTS_ONLY;

	cs->next_out = (Bytef *) dst;
	cs->avail_out = dst_size;
	cs->next_in = (Bytef *) src;
	cs->avail_in = src_size;

	rc = deflate(cs, Z_SYNC_FLUSH);
	Assert(rc == Z_OK);
	*dst_processed = dst_size - cs->avail_out;
	*src_processed = src_size - cs->avail_in;

	return ZS_OK;
}

static bool
zlib_compress_buffered_data(void *c_stream)
{
	return false;
}

static int
zlib_end(void *c_stream, void *dst, size_t dst_size, size_t *dst_processed)
{
	z_stream   *cs = (z_stream *) c_stream;
	int			rc;

	cs->next_out = (Bytef *) dst;
	cs->avail_out = dst_size;
	cs->next_in = NULL;
	cs->avail_in = 0;

	rc = deflate(cs, Z_STREAM_END);
	Assert(rc == Z_OK || rc == Z_STREAM_END);
	*dst_processed = dst_size - cs->avail_out;
	if (rc == Z_STREAM_END)
	{
		return ZS_OK;
	}

	return rc;
}

static void
zlib_free_compressor(void *c_stream)
{
	z_stream   *cs = (z_stream *) c_stream;

	if (cs != NULL)
	{
		deflateEnd(cs);
		FREE(cs);
	}
}

static void
zlib_free_decompressor(void *d_stream)
{
	z_stream   *ds = (z_stream *) d_stream;

	if (ds != NULL)
	{
		inflateEnd(ds);
		FREE(ds);
	}
}

static char const *
zlib_error(void *stream)
{
	z_stream   *zs = (z_stream *) stream;

	return zs->msg;
}

/* as with elsewhere in postgres, gzip really means zlib */
static ZAlgorithm const zlib_algorithm = {
	.algorithm = PG_COMPRESSION_GZIP,
	.create_compressor = zlib_create_compressor,
	.create_decompressor = zlib_create_decompressor,
	.decompress = zlib_decompress,
	.decompress_buffered_data = zlib_decompress_buffered_data,
	.compress = zlib_compress,
	.compress_buffered_data = zlib_compress_buffered_data,
	.free_compressor = zlib_free_compressor,
	.free_decompressor = zlib_free_decompressor,
	.compress_error = zlib_error,
	.decompress_error = zlib_error,
	.end_compression = zlib_end
};

#endif

#if USE_LZ4
#include <lz4.h>

#define MESSAGE_MAX_BYTES 64 * 1024
#define RING_BUFFER_BYTES (LZ4_DECODER_RING_BUFFER_SIZE(MESSAGE_MAX_BYTES))

typedef struct ZS_LZ4_CStream
{
	LZ4_stream_t *stream;
	int			level;
	size_t		buf_pos;
	char	   *last_error;
	char		buf[RING_BUFFER_BYTES];
}			ZS_LZ4_CStream;

typedef struct ZS_LZ4_DStream
{
	LZ4_streamDecode_t *stream;
	size_t		buf_pos;
	size_t		read_pos;
	char	   *last_error;
	char		buf[RING_BUFFER_BYTES];
}			ZS_LZ4_DStream;

static void *
lz4_create_compressor(int level)
{
	ZS_LZ4_CStream *c_stream = (ZS_LZ4_CStream *) ALLOC(sizeof(ZS_LZ4_CStream));

	if (c_stream == NULL)
	{
		return NULL;
	}
	c_stream->stream = LZ4_createStream();
	c_stream->level = level;
	c_stream->buf_pos = 0;
	if (c_stream->stream == NULL)
	{
		FREE(c_stream);
		return NULL;
	}
	return c_stream;
}

static void *
lz4_create_decompressor()
{
	ZS_LZ4_DStream *d_stream = (ZS_LZ4_DStream *) ALLOC(sizeof(ZS_LZ4_DStream));

	if (d_stream == NULL)
	{
		return NULL;
	}

	d_stream->stream = LZ4_createStreamDecode();
	d_stream->buf_pos = 0;
	d_stream->read_pos = 0;
	if (d_stream->stream == NULL)
	{
		FREE(d_stream);
		return NULL;
	}

	return d_stream;
}
char		last_error[256];

static int
lz4_decompress(void *d_stream, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed)
{
	ZS_LZ4_DStream *ds = (ZS_LZ4_DStream *) d_stream;
	size_t		copyable;
	char	   *decPtr;
	int			decBytes;

	if (ds->read_pos < ds->buf_pos)
	{
		decPtr = &ds->buf[ds->read_pos];
		copyable = Min(ds->buf_pos - ds->read_pos, dst_size);
		memcpy(dst, decPtr, copyable);	/* read msg length */
		*dst_processed = copyable;
		*src_processed = 0;
		ds->read_pos += copyable;

		if (ds->read_pos == ds->buf_pos && RING_BUFFER_BYTES - ds->buf_pos < MESSAGE_MAX_BYTES)
		{
			ds->buf_pos = 0;
			ds->read_pos = 0;
		}
		return ZS_OK;
	}
	decPtr = &ds->buf[ds->buf_pos];

	decBytes = LZ4_decompress_safe_continue(ds->stream, src, decPtr, (int) src_size, RING_BUFFER_BYTES - ds->buf_pos);
	if (decBytes < 0)
	{
		sprintf(last_error, "LZ4 decompression failed (src_size %ld, dst_size %ld, error: %d)", src_size, RING_BUFFER_BYTES - ds->buf_pos, decBytes);
		ds->last_error = last_error;
#ifndef FRONTEND
		elog(ERROR, "%s", ds->last_error);
#else
		return ZS_DECOMPRESS_ERROR;
#endif
	}

	copyable = Min(decBytes, dst_size);
	memcpy(dst, decPtr, copyable);

	*dst_processed = copyable;
	*src_processed = src_size;

	ds->buf_pos += decBytes;
	ds->read_pos += copyable;
	/* only reset the ring buffer after the internal buffer is drained */
	if (copyable == decBytes && RING_BUFFER_BYTES - ds->buf_pos < MESSAGE_MAX_BYTES)
	{
		ds->buf_pos = 0;
		ds->read_pos = 0;
	}

	return ZS_OK;
}

static bool
lz4_decompress_buffered_data(void *d_stream)
{
	ZS_LZ4_DStream *ds = (ZS_LZ4_DStream *) d_stream;

	return ds->read_pos < ds->buf_pos;
}

static int
lz4_compress(void *c_stream, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed)
{
	ZS_LZ4_CStream *cs = (ZS_LZ4_CStream *) c_stream;
	int			cmpBytes;

	src_size = Min(MESSAGE_MAX_BYTES, src_size);

	memcpy((char *) (cs->buf) + cs->buf_pos, src, src_size);	/* write msg length */

	if (dst_size < LZ4_compressBound(src_size))
	{
		cs->last_error = "LZ4 compression failed: buffer not big enough";
#ifndef FRONTEND
		elog(ERROR, "%s", cs->last_error);
#else
		return ZS_COMPRESS_ERROR;
#endif
	}

	cmpBytes = LZ4_compress_fast_continue(cs->stream, (char *) (cs->buf) + cs->buf_pos, dst, (int) src_size, (int) dst_size, cs->level);

	if (cmpBytes < 0 || cmpBytes > MESSAGE_MAX_BYTES)
	{
		cs->last_error = "LZ4 compression failed";
#ifndef FRONTEND
		elog(ERROR, "%s", cs->last_error);
#else
		return ZS_COMPRESS_ERROR;
#endif
	}

	*dst_processed = cmpBytes;
	*src_processed = src_size;

	cs->buf_pos += src_size;
	if (cs->buf_pos >= RING_BUFFER_BYTES - MESSAGE_MAX_BYTES)
	{
		cs->buf_pos = 0;
	}
	return ZS_OK;
}

static bool
lz4_compress_buffered_data(void *d_stream)
{
	return false;
}


static int
lz4_end(void *c_stream, void *dst, size_t dst_size, size_t *dst_processed)
{
	*dst_processed = 0;
	return ZS_OK;
}

static void
lz4_free_compressor(void *c_stream)
{
	ZS_LZ4_CStream *cs = (ZS_LZ4_CStream *) c_stream;

	if (cs != NULL)
	{
		if (cs->stream != NULL)
		{
			LZ4_freeStream(cs->stream);
		}
		FREE(cs);
	}
}

static void
lz4_free_decompressor(void *d_stream)
{
	ZS_LZ4_DStream *ds = (ZS_LZ4_DStream *) d_stream;

	if (ds != NULL)
	{
		if (ds->stream != NULL)
		{
			LZ4_freeStreamDecode(ds->stream);
		}
		FREE(ds);
	}
}

static char const *
lz4_compress_error(void *stream)
{
	ZS_LZ4_CStream *cs = (ZS_LZ4_CStream *) stream;

	/* lz4 doesn't have any explicit API to get the error names */
	return cs->last_error;
}

static char const *
lz4_decompress_error(void *stream)
{
	ZS_LZ4_DStream *ds = (ZS_LZ4_DStream *) stream;

	/* lz4 doesn't have any explicit API to get the error names */
	return ds->last_error;
}

static ZAlgorithm const lz4_algorithm = {
	.algorithm = PG_COMPRESSION_LZ4,
	.create_compressor = lz4_create_compressor,
	.create_decompressor = lz4_create_decompressor,
	.decompress = lz4_decompress,
	.decompress_buffered_data = lz4_decompress_buffered_data,
	.compress = lz4_compress,
	.compress_buffered_data = lz4_compress_buffered_data,
	.free_compressor = lz4_free_compressor,
	.free_decompressor = lz4_free_decompressor,
	.compress_error = lz4_compress_error,
	.decompress_error = lz4_decompress_error,
	.end_compression = lz4_end
};

#endif

static const ZAlgorithm *
zs_find_algorithm(pg_compress_algorithm algorithm)
{
#if HAVE_LIBZ
	if (algorithm == PG_COMPRESSION_GZIP)
	{
		return &zlib_algorithm;
	}
#endif
#if USE_LZ4
	if (algorithm == PG_COMPRESSION_LZ4)
	{
		return &lz4_algorithm;
	}
#endif
#if HAVE_LIBZSTD
	if (algorithm == PG_COMPRESSION_ZSTD)
	{
		return &zstd_algorithm;
	}
#endif
	return NULL;
}

static int
zs_init_compressor(ZStream * zs, pg_compress_specification *spec)
{
	const		ZAlgorithm *algorithm = zs_find_algorithm(spec->algorithm);

	if (algorithm == NULL)
	{
		return -1;
	}
	zs->algorithm = algorithm;
	zs->stream = zs->algorithm->create_compressor(spec->level);
	if (zs->stream == NULL)
	{
		return -1;
	}
	return 0;
}

static int
zs_init_decompressor(ZStream * zs, pg_compress_specification *spec)
{
	const		ZAlgorithm *algorithm = zs_find_algorithm(spec->algorithm);

	if (algorithm == NULL)
	{
		return -1;
	}
	zs->algorithm = algorithm;
	zs->stream = zs->algorithm->create_decompressor();
	if (zs->stream == NULL)
	{
		return -1;
	}
	return 0;
}

ZStream *
zs_create_compressor(pg_compress_specification *spec)
{
	ZStream    *zs = (ZStream *) ALLOC(sizeof(ZStream));

	if (zs_init_compressor(zs, spec))
	{
		FREE(zs);
		return NULL;
	}

	return zs;
}

ZStream *
zs_create_decompressor(pg_compress_specification *spec)
{
	ZStream    *zs = (ZStream *) ALLOC(sizeof(ZStream));

	if (zs_init_decompressor(zs, spec))
	{
		FREE(zs);
		return NULL;
	}

	return zs;
}

int
zs_read(ZStream * zs, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed)
{
	int			rc;

	*src_processed = 0;
	*dst_processed = 0;

	rc = zs->algorithm->decompress(zs->stream,
								   src, src_size, src_processed,
								   dst, dst_size, dst_processed);

	if (rc == ZS_OK || rc == ZS_INCOMPLETE_SRC || rc == ZS_STREAM_END)
	{
		return rc;
	}

	return ZS_DECOMPRESS_ERROR;
}

int
zs_write(ZStream * zs, void const *buf, size_t size, size_t *processed, void *dst, size_t dst_size, size_t *dst_processed)
{
	int			rc;

	*processed = 0;
	*dst_processed = 0;

	rc = zs->algorithm->compress(zs->stream,
								 buf, size, processed,
								 dst, dst_size, dst_processed);

	if (rc != ZS_OK)
	{
		return ZS_COMPRESS_ERROR;
	}

	return rc;
}

void
zs_compressor_free(ZStream * zs)
{
	if (zs == NULL)
	{
		return;
	}

	if (zs->stream)
	{
		zs->algorithm->free_compressor(zs->stream);
	}

	FREE(zs);
}

void
zs_decompressor_free(ZStream * zs)
{
	if (zs == NULL)
	{
		return;
	}

	if (zs->stream)
	{
		zs->algorithm->free_decompressor(zs->stream);
	}

	FREE(zs);
}

int
zs_end_compression(ZStream * zs, void *dst, size_t dst_size, size_t *dst_processed)
{
	int			rc;

	*dst_processed = 0;

	rc = zs->algorithm->end_compression(zs->stream, dst, dst_size, dst_processed);

	if (rc != ZS_OK)
	{
		return ZS_COMPRESS_ERROR;
	}

	return rc;
}

char const *
zs_compress_error(ZStream * zs)
{
	return zs->algorithm->compress_error(zs->stream);
}

char const *
zs_decompress_error(ZStream * zs)
{
	return zs->algorithm->decompress_error(zs->stream);
}

bool
zs_compress_buffered(ZStream * zs)
{
	return zs ? zs->algorithm->compress_buffered_data(zs->stream) : false;
}

bool
zs_decompress_buffered(ZStream * zs)
{
	return zs ? zs->algorithm->decompress_buffered_data(zs->stream) : false;
}

pg_compress_algorithm
zs_algorithm(ZStream * zs)
{
	return zs ? zs->algorithm->algorithm : PG_COMPRESSION_NONE;
}
