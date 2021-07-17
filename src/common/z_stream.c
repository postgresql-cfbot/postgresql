#include "c.h"
#include "pg_config.h"
#include "common/z_stream.h"

/*
 * Functions implementing streaming compression algorithm
 */
typedef struct
{
	/*
	 * Name of compression algorithm.
	 */
	char const *(*name) (void);

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
	 * ZS_DATA_PENDING means that there might be some data left within
	 * decompressor internal buffers.
	 *
	 * ZS_STREAM_END if encountered end of compressed data stream.
	 *
	 * ZS_DECOMPRESS_ERROR if encountered an error during decompression
	 * attempt.
	 */
	ssize_t		(*decompress) (void *ds, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed);

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
	 * ZS_DATA_PENDING means that there might be some data left within
	 * compressor internal buffers.
	 *
	 * ZS_COMPRESS_ERROR if encountered an error during compression attempt.
	 */
	ssize_t		(*compress) (void *cs, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed);

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

	ssize_t		(*end_compression) (void *cs, void *dst, size_t dst_size, size_t *dst_processed);
}			ZAlgorithm;

struct ZStream
{
	ZAlgorithm const *c_algorithm;
	void	   *c_stream;

	ZAlgorithm const *d_algorithm;
	void	   *d_stream;

	bool		rx_not_flushed;
	bool		tx_not_flushed;
};

#if HAVE_LIBZSTD

#include <stdlib.h>
#include <zstd.h>

/*
 * Maximum allowed back-reference distance, expressed as power of 2.
 * This setting controls max compressor/decompressor window size.
 * More details https://github.com/facebook/zstd/blob/v1.4.7/lib/zstd.h#L536
 */
#define ZSTD_WINDOWLOG_LIMIT 23 /* set max window size to 8MB */


typedef struct ZPQ_ZSTD_CStream
{
	ZSTD_CStream *stream;
	char const *error;			/* error message */
}			ZPQ_ZSTD_CStream;

typedef struct ZPQ_ZSTD_DStream
{
	ZSTD_DStream *stream;
	char const *error;			/* error message */
}			ZPQ_ZSTD_DStream;

static void *
zstd_create_compressor(int level)
{
	ZPQ_ZSTD_CStream *c_stream = (ZPQ_ZSTD_CStream *) malloc(sizeof(ZPQ_ZSTD_CStream));

	c_stream->stream = ZSTD_createCStream();
	ZSTD_initCStream(c_stream->stream, level);
#if ZSTD_VERSION_MAJOR > 1 || ZSTD_VERSION_MINOR > 3
	ZSTD_CCtx_setParameter(c_stream->stream, ZSTD_c_windowLog, ZSTD_WINDOWLOG_LIMIT);
#endif
	c_stream->error = NULL;
	return c_stream;
}

static void *
zstd_create_decompressor()
{
	ZPQ_ZSTD_DStream *d_stream = (ZPQ_ZSTD_DStream *) malloc(sizeof(ZPQ_ZSTD_DStream));

	d_stream->stream = ZSTD_createDStream();
	ZSTD_initDStream(d_stream->stream);
#if ZSTD_VERSION_MAJOR > 1 || ZSTD_VERSION_MINOR > 3
	ZSTD_DCtx_setParameter(d_stream->stream, ZSTD_d_windowLogMax, ZSTD_WINDOWLOG_LIMIT);
#endif
	d_stream->error = NULL;
	return d_stream;
}

static ssize_t
zstd_decompress(void *d_stream, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed)
{
	ZPQ_ZSTD_DStream *ds = (ZPQ_ZSTD_DStream *) d_stream;
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

	if (out.pos == out.size)
	{
		/*
		 * if `output.pos == output.size`, there might be some data left
		 * within internal buffers
		 */
		return ZS_DATA_PENDING;
	}
	return ZS_OK;
}

static ssize_t
zstd_compress(void *c_stream, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed)
{
	ZPQ_ZSTD_CStream *cs = (ZPQ_ZSTD_CStream *) c_stream;
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
		if (tx_not_flushed > 0)
		{
			return ZS_DATA_PENDING;
		}
	}

	return ZS_OK;
}

static ssize_t
zstd_end(void *c_stream, void *dst, size_t dst_size, size_t *dst_processed)
{
	size_t		tx_not_flushed;
	ZPQ_ZSTD_CStream *cs = (ZPQ_ZSTD_CStream *) c_stream;
	ZSTD_outBuffer output;

	output.dst = dst;
	output.pos = 0;
	output.size = dst_size;

	do
	{
		tx_not_flushed = ZSTD_endStream(cs->stream, &output);
	} while ((tx_not_flushed > 0) && (output.pos < output.size));

	*dst_processed = output.pos;

	if (tx_not_flushed > 0)
	{
		return ZS_DATA_PENDING;
	}
	return ZS_OK;
}

static void
zstd_free_compressor(void *c_stream)
{
	ZPQ_ZSTD_CStream *cs = (ZPQ_ZSTD_CStream *) c_stream;

	if (cs != NULL)
	{
		ZSTD_freeCStream(cs->stream);
		free(cs);
	}
}

static void
zstd_free_decompressor(void *d_stream)
{
	ZPQ_ZSTD_DStream *ds = (ZPQ_ZSTD_DStream *) d_stream;

	if (ds != NULL)
	{
		ZSTD_freeDStream(ds->stream);
		free(ds);
	}
}

static char const *
zstd_compress_error(void *c_stream)
{
	ZPQ_ZSTD_CStream *cs = (ZPQ_ZSTD_CStream *) c_stream;

	return cs->error;
}

static char const *
zstd_decompress_error(void *d_stream)
{
	ZPQ_ZSTD_DStream *ds = (ZPQ_ZSTD_DStream *) d_stream;

	return ds->error;
}

static char const *
zstd_name(void)
{
	return "zstd";
}

#endif

#if HAVE_LIBZ

#include <stdlib.h>
#include <zlib.h>


static void *
zlib_create_compressor(int level)
{
	int			rc;
	z_stream   *c_stream = (z_stream *) malloc(sizeof(z_stream));

	memset(c_stream, 0, sizeof(*c_stream));
	rc = deflateInit(c_stream, level);
	if (rc != Z_OK)
	{
		free(c_stream);
		return NULL;
	}
	return c_stream;
}

static void *
zlib_create_decompressor()
{
	int			rc;
	z_stream   *d_stream = (z_stream *) malloc(sizeof(z_stream));

	memset(d_stream, 0, sizeof(*d_stream));
	rc = inflateInit(d_stream);
	if (rc != Z_OK)
	{
		free(d_stream);
		return NULL;
	}
	return d_stream;
}

static ssize_t
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

static ssize_t
zlib_compress(void *c_stream, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed)
{
	z_stream   *cs = (z_stream *) c_stream;
	int			rc;
	unsigned	deflate_pending = 0;


	cs->next_out = (Bytef *) dst;
	cs->avail_out = dst_size;
	cs->next_in = (Bytef *) src;
	cs->avail_in = src_size;

	rc = deflate(cs, Z_SYNC_FLUSH);
	Assert(rc == Z_OK);
	*dst_processed = dst_size - cs->avail_out;
	*src_processed = src_size - cs->avail_in;

	deflatePending(cs, &deflate_pending, Z_NULL);	/* check if any data left
													 * in deflate buffer */
	if (deflate_pending > 0)
	{
		return ZS_DATA_PENDING;
	}
	return ZS_OK;
}


static ssize_t
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

	return ZS_DATA_PENDING;
}

static void
zlib_free_compressor(void *c_stream)
{
	z_stream   *cs = (z_stream *) c_stream;

	if (cs != NULL)
	{
		deflateEnd(cs);
		free(cs);
	}
}

static void
zlib_free_decompressor(void *d_stream)
{
	z_stream   *ds = (z_stream *) d_stream;

	if (ds != NULL)
	{
		inflateEnd(ds);
		free(ds);
	}
}

static char const *
zlib_error(void *stream)
{
	z_stream   *zs = (z_stream *) stream;

	return zs->msg;
}

static char const *
zlib_name(void)
{
	return "zlib";
}

#endif

static char const *
no_compression_name(void)
{
	return NULL;
}

/*
 * Array with all supported compression algorithms.
 */
static ZAlgorithm const zpq_algorithms[] =
{
#if HAVE_LIBZSTD
	{zstd_name, zstd_create_compressor, zstd_create_decompressor, zstd_decompress, zstd_compress, zstd_free_compressor, zstd_free_decompressor, zstd_compress_error, zstd_decompress_error, zstd_end},
#endif
#if HAVE_LIBZ
	{zlib_name, zlib_create_compressor, zlib_create_decompressor, zlib_decompress, zlib_compress, zlib_free_compressor, zlib_free_decompressor, zlib_error, zlib_error, zlib_end},
#endif
	{no_compression_name}
};

static ssize_t
zpq_init_compressor(ZStream * zs, int c_alg_impl, int c_level)
{
	zs->c_algorithm = &zpq_algorithms[c_alg_impl];
	zs->c_stream = zpq_algorithms[c_alg_impl].create_compressor(c_level);
	if (zs->c_stream == NULL)
	{
		return -1;
	}
	return 0;
}

static ssize_t
zpq_init_decompressor(ZStream * zs, int d_alg_impl)
{
	zs->d_algorithm = &zpq_algorithms[d_alg_impl];
	zs->d_stream = zpq_algorithms[d_alg_impl].create_decompressor();
	if (zs->d_stream == NULL)
	{
		return -1;
	}
	return 0;
}

/*
 * Index of used compression algorithm in zpq_algorithms array.
 */
ZStream *
zs_create(int c_alg_impl, int c_level, int d_alg_impl)
{
	ZStream    *zs = (ZStream *) malloc(sizeof(ZStream));

	zs->tx_not_flushed = false;
	zs->rx_not_flushed = false;

	if (zpq_init_compressor(zs, c_alg_impl, c_level) || zpq_init_decompressor(zs, d_alg_impl))
	{
		free(zs);
		return NULL;
	}

	return zs;
}

ssize_t
zs_read(ZStream * zs, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed)
{
	ssize_t		rc;
	*src_processed = 0;
	*dst_processed = 0;

	rc = zs->d_algorithm->decompress(zs->d_stream,
									 src, src_size, src_processed,
									 dst, dst_size, dst_processed);

	zs->rx_not_flushed = false;
	if (rc == ZS_DATA_PENDING)
	{
		zs->rx_not_flushed = true;
		return ZS_OK;
	}

	if (rc != ZS_OK && rc != ZS_STREAM_END)
	{
		return ZS_DECOMPRESS_ERROR;
	}

	return rc;
}

ssize_t
zs_write(ZStream * zs, void const *buf, size_t size, size_t *processed, void *dst, size_t dst_size, size_t *dst_processed)
{
	ssize_t		rc;
	*processed = 0;
	*dst_processed = 0;

	rc = zs->c_algorithm->compress(zs->c_stream,
								   buf, size, processed,
								   dst, dst_size, dst_processed);

	zs->tx_not_flushed = false;
	if (rc == ZS_DATA_PENDING)
	{
		zs->tx_not_flushed = true;
		return ZS_OK;
	}
	if (rc != ZS_OK)
	{
		return ZS_COMPRESS_ERROR;
	}

	return rc;
}

void
zs_free(ZStream * zs)
{
	if (zs)
	{
		if (zs->c_stream)
		{
			zs->c_algorithm->free_compressor(zs->c_stream);
		}
		if (zs->d_stream)
		{
			zs->d_algorithm->free_decompressor(zs->d_stream);
		}
		free(zs);
	}
}

ssize_t
zs_end(ZStream * zs, void *dst, size_t dst_size, size_t *dst_processed)
{
	ssize_t		rc;
	*dst_processed = 0;

	rc = zs->c_algorithm->end_compression(zs->c_stream, dst, dst_size, dst_processed);

	zs->tx_not_flushed = false;
	if (rc == ZS_DATA_PENDING)
	{
		zs->tx_not_flushed = true;
		return ZS_OK;
	}
	if (rc != ZS_OK)
	{
		return ZS_COMPRESS_ERROR;
	}

	return rc;
}

char const *
zs_compress_error(ZStream * zs)
{
	return zs->c_algorithm->compress_error(zs->c_stream);
}

char const *
zs_decompress_error(ZStream * zs)
{
	return zs->d_algorithm->decompress_error(zs->d_stream);
}

bool
zs_buffered_rx(ZStream * zs)
{
	return zs ? zs->rx_not_flushed : 0;
}

bool
zs_buffered_tx(ZStream * zs)
{
	return zs ? zs->tx_not_flushed : 0;
}

/*
 * Get list of the supported algorithms.
 */
char	  **
zs_get_supported_algorithms(void)
{
	size_t		n_algorithms = sizeof(zpq_algorithms) / sizeof(*zpq_algorithms);
	char	  **algorithm_names = malloc(n_algorithms * sizeof(char *));

	for (size_t i = 0; i < n_algorithms; i++)
	{
		algorithm_names[i] = (char *) zpq_algorithms[i].name();
	}

	return algorithm_names;
}

char const *
zs_compress_algorithm_name(ZStream * zs)
{
	return zs ? zs->c_algorithm->name() : NULL;
}

char const *
zs_decompress_algorithm_name(ZStream * zs)
{
	return zs ? zs->d_algorithm->name() : NULL;
}
