#include "postgres_fe.h"
#include "common/zpq_stream.h"
#include "c.h"
#include "pg_config.h"

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
	 * Create new compression stream.
	 * level: compression level
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
	 * Return codes: ZPQ_OK if no errors were encountered during decompression
	 * attempt. This return code does not guarantee that *src_processed > 0 or
	 * *dst_processed > 0.
	 *
	 * ZPQ_DATA_PENDING means that there might be some data left within
	 * decompressor internal buffers.
	 *
	 * ZPQ_STREAM_END if encountered end of compressed data stream.
	 *
	 * ZPQ_DECOMPRESS_ERROR if encountered an error during decompression
	 * attempt.
	 */
	ssize_t		(*decompress) (void *ds, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed);

	/*
	 * Compress up to "src_size" raw (non-compressed) bytes from *src and
	 * write up to "dst_size" compressed bytes to *dst. Number of compressed
	 * bytes written to *dst is stored in *dst_processed. Number of
	 * non-compressed bytes read from *src is stored in *src_processed.
	 *
	 * Return codes: ZPQ_OK if no errors were encountered during compression
	 * attempt. This return code does not guarantee that *src_processed > 0 or
	 * *dst_processed > 0.
	 *
	 * ZPQ_DATA_PENDING means that there might be some data left within
	 * compressor internal buffers.
	 *
	 * ZPQ_COMPRESS_ERROR if encountered an error during compression attempt.
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
}			ZpqAlgorithm;


#define ZPQ_BUFFER_SIZE       8192	/* We have to flush stream after each
									 * protocol command and command is mostly
									 * limited by record length, which in turn
									 * is usually less than page size (except
									 * TOAST)
									 */

struct ZpqStream
{
	ZpqAlgorithm const *c_algorithm;
	void	   *c_stream;

	ZpqAlgorithm const *d_algorithm;
	void	   *d_stream;

	char		tx_buf[ZPQ_BUFFER_SIZE];
	size_t		tx_pos;
	size_t		tx_size;

	char		rx_buf[ZPQ_BUFFER_SIZE];
	size_t		rx_pos;
	size_t		rx_size;

	zpq_tx_func tx_func;
	zpq_rx_func rx_func;
	void	   *arg;

	size_t		tx_total;
	size_t		tx_total_raw;
	size_t		rx_total;
	size_t		rx_total_raw;

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
		return ZPQ_DECOMPRESS_ERROR;
	}

	if (out.pos == out.size)
	{
		/*
		 * if `output.pos == output.size`, there might be some data left
		 * within internal buffers
		 */
		return ZPQ_DATA_PENDING;
	}
	return ZPQ_OK;
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
			return ZPQ_COMPRESS_ERROR;
		}
	}

	if (in.pos == src_size)		/* All data is compressed: flush internal zstd
								 * buffer */
	{
		size_t		tx_not_flushed = ZSTD_flushStream(cs->stream, &out);

		*dst_processed = out.pos;
		if (tx_not_flushed > 0)
		{
			return ZPQ_DATA_PENDING;
		}
	}

	return ZPQ_OK;
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
		return ZPQ_STREAM_END;
	}
	if (rc != Z_OK && rc != Z_BUF_ERROR)
	{
		return ZPQ_DECOMPRESS_ERROR;
	}

	return ZPQ_OK;
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
		return ZPQ_DATA_PENDING;
	}
	return ZPQ_OK;
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
static ZpqAlgorithm const zpq_algorithms[] =
{
#if HAVE_LIBZSTD
	{zstd_name, zstd_create_compressor, zstd_create_decompressor, zstd_decompress, zstd_compress, zstd_free_compressor, zstd_free_decompressor, zstd_compress_error, zstd_decompress_error},
#endif
#if HAVE_LIBZ
	{zlib_name, zlib_create_compressor, zlib_create_decompressor, zlib_decompress, zlib_compress, zlib_free_compressor, zlib_free_decompressor, zlib_error, zlib_error},
#endif
	{no_compression_name}
};

/*
 * Index of used compression algorithm in zpq_algorithms array.
 */
ZpqStream *
zpq_create(int c_alg_impl, int c_level, int d_alg_impl, zpq_tx_func tx_func, zpq_rx_func rx_func, void *arg, char *rx_data, size_t rx_data_size)
{
	ZpqStream  *zs = (ZpqStream *) malloc(sizeof(ZpqStream));

	zs->tx_pos = 0;
	zs->tx_size = 0;
	zs->rx_pos = 0;
	zs->rx_size = 0;
	zs->tx_func = tx_func;
	zs->rx_func = rx_func;
	zs->arg = arg;
	zs->tx_total = 0;
	zs->tx_total_raw = 0;
	zs->rx_total = 0;
	zs->rx_total_raw = 0;
	zs->tx_not_flushed = false;
	zs->rx_not_flushed = false;

	zs->rx_size = rx_data_size;
	Assert(rx_data_size < ZPQ_BUFFER_SIZE);
	memcpy(zs->rx_buf, rx_data, rx_data_size);

	zs->c_algorithm = &zpq_algorithms[c_alg_impl];
	zs->c_stream = zpq_algorithms[c_alg_impl].create_compressor(c_level);
	if (zs->c_stream == NULL)
	{
		free(zs);
		return NULL;
	}
	zs->d_algorithm = &zpq_algorithms[d_alg_impl];
	zs->d_stream = zpq_algorithms[d_alg_impl].create_decompressor();
	if (zs->d_stream == NULL)
	{
		free(zs);
		return NULL;
	}

	return zs;
}

ssize_t
zpq_read(ZpqStream * zs, void *buf, size_t size)
{
	size_t		buf_pos = 0;
	size_t		rx_processed;
	size_t		buf_processed;
	ssize_t		rc;

	while (buf_pos == 0)
	{							/* Read until some data fetched */
		if (zs->rx_pos == zs->rx_size)
		{
			zs->rx_pos = zs->rx_size = 0;	/* Reset rx buffer */
		}

		if (zs->rx_pos == zs->rx_size && !zs->rx_not_flushed)
		{
			ssize_t		rc = zs->rx_func(zs->arg, (char *) zs->rx_buf + zs->rx_size, ZPQ_BUFFER_SIZE - zs->rx_size);

			if (rc > 0)			/* read fetches some data */
			{
				zs->rx_size += rc;
				zs->rx_total += rc;
			}
			else				/* read failed */
			{
				return rc;
			}
		}

		Assert(zs->rx_pos <= zs->rx_size);
		rx_processed = 0;
		buf_processed = 0;
		rc = zs->d_algorithm->decompress(zs->d_stream,
										 (char *) zs->rx_buf + zs->rx_pos, zs->rx_size - zs->rx_pos, &rx_processed,
										 buf, size, &buf_processed);

		zs->rx_pos += rx_processed;
		zs->rx_total_raw += rx_processed;
		buf_pos += buf_processed;
		zs->rx_not_flushed = false;
		if (rc == ZPQ_STREAM_END)
		{
			break;
		}
		if (rc == ZPQ_DATA_PENDING)
		{
			zs->rx_not_flushed = true;
			continue;
		}
		if (rc != ZPQ_OK)
		{
			return ZPQ_DECOMPRESS_ERROR;
		}
	}
	return buf_pos;
}

ssize_t
zpq_write(ZpqStream * zs, void const *buf, size_t size, size_t *processed)
{
	size_t		buf_pos = 0;

	do
	{
		if (zs->tx_pos == zs->tx_size)	/* Have nothing to send */
		{
			size_t		tx_processed = 0;
			size_t		buf_processed = 0;
			ssize_t		rc;

			zs->tx_pos = zs->tx_size = 0;	/* Reset pointer to the beginning of buffer */

			rc = zs->c_algorithm->compress(zs->c_stream,
										   (char *) buf + buf_pos, size - buf_pos, &buf_processed,
										   (char *) zs->tx_buf + zs->tx_size, ZPQ_BUFFER_SIZE - zs->tx_size, &tx_processed);

			zs->tx_size += tx_processed;
			buf_pos += buf_processed;
			zs->tx_total_raw += buf_processed;
			zs->tx_not_flushed = false;

			if (rc == ZPQ_DATA_PENDING)
			{
				zs->tx_not_flushed = true;
				continue;
			}
			if (rc != ZPQ_OK)
			{
				*processed = buf_pos;
				return ZPQ_COMPRESS_ERROR;
			}
		}
		while (zs->tx_pos < zs->tx_size)
		{
			ssize_t		rc = zs->tx_func(zs->arg, (char *) zs->tx_buf + zs->tx_pos, zs->tx_size - zs->tx_pos);

			if (rc > 0)
			{
				zs->tx_pos += rc;
				zs->tx_total += rc;
			}
			else
			{
				*processed = buf_pos;
				return rc;
			}
		}

		/*
		 * repeat sending while there is some data in input or internal
		 * compression buffer
		 */
	} while (buf_pos < size || zs->tx_not_flushed);

	return buf_pos;
}

void
zpq_free(ZpqStream * zs)
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

char const *
zpq_compress_error(ZpqStream * zs)
{
	return zs->c_algorithm->compress_error(zs->c_stream);
}

char const *
zpq_decompress_error(ZpqStream * zs)
{
	return zs->d_algorithm->decompress_error(zs->d_stream);
}

size_t
zpq_buffered_rx(ZpqStream * zs)
{
	return zs ? zs->rx_not_flushed || (zs->rx_size - zs->rx_pos) : 0;
}

size_t
zpq_buffered_tx(ZpqStream * zs)
{
	return zs ? zs->tx_not_flushed || (zs->tx_size - zs->tx_pos) : 0;
}

/*
 * Get list of the supported algorithms.
 */
char	  **
zpq_get_supported_algorithms(void)
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
zpq_compress_algorithm_name(ZpqStream * zs)
{
	return zs ? zs->c_algorithm->name() : NULL;
}

char const *
zpq_decompress_algorithm_name(ZpqStream * zs)
{
	return zs ? zs->d_algorithm->name() : NULL;
}
