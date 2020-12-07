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
	 * Returns name of compression algorithm.
	 */
	char const* (*name)(void);

	/*
	 * Create compression stream with using rx/tx function for fetching/sending compressed data.
	 * level: compression level
	 * tx_func: function for writing compressed data in underlying stream
	 * rx_func: function for receiving compressed data from underlying stream
	 * arg: context passed to the function
     * rx_data: received data (compressed data already fetched from input stream)
	 * rx_data_size: size of data fetched from input stream
	 */
	ZpqStream* (*create)(int level, zpq_tx_func tx_func, zpq_rx_func rx_func, void *arg, char* rx_data, size_t rx_data_size);

	/*
	 * Read up to "size" raw (decompressed) bytes.
	 * Returns number of decompressed bytes or error code.
	 * Error code is either ZPQ_DECOMPRESS_ERROR either error code returned by the rx function.
	 */
	ssize_t (*read)(ZpqStream *zs, void *buf, size_t size);

	/*
	 * Write up to "size" raw (decompressed) bytes.
	 * Returns number of written raw bytes or error code returned by tx function.
	 * In the last case amount of written raw bytes is stored in *processed.
	 */
	ssize_t (*write)(ZpqStream *zs, void const *buf, size_t size, size_t *processed);

	/*
	 * Free stream created by create function.
	 */
	void    (*free)(ZpqStream *zs);

	/*
	 * Get error message.
	 */
	char const* (*error)(ZpqStream *zs);

	/*
	 * Returns amount of data in internal tx decompression buffer.
	 */
	size_t  (*buffered_tx)(ZpqStream *zs);

	/*
	 * Returns amount of data in internal rx compression buffer.
	 */
	size_t  (*buffered_rx)(ZpqStream *zs);
} ZpqAlgorithm;

struct ZpqStream
{
	ZpqAlgorithm const* algorithm;
};

#if HAVE_LIBZSTD

#include <stdlib.h>
#include <zstd.h>

#define ZSTD_BUFFER_SIZE (8*1024)

typedef struct ZstdStream
{
	ZpqStream      common;
	ZSTD_CStream*  tx_stream;
	ZSTD_DStream*  rx_stream;
	ZSTD_outBuffer tx;
	ZSTD_inBuffer  rx;
	size_t         tx_not_flushed; /* Amount of data in internal zstd buffer */
	size_t         tx_buffered;    /* Data which is consumed by ztd_write but not yet sent */
	size_t         rx_buffered;    /* Data which is needed for ztd_read */
	zpq_tx_func    tx_func;
	zpq_rx_func    rx_func;
	void*          arg;
	char const*    rx_error;    /* Decompress error message */
	size_t         tx_total;
	size_t         tx_total_raw;
	size_t         rx_total;
	size_t         rx_total_raw;
	char           tx_buf[ZSTD_BUFFER_SIZE];
	char           rx_buf[ZSTD_BUFFER_SIZE];
} ZstdStream;

static ZpqStream*
zstd_create(int level, zpq_tx_func tx_func, zpq_rx_func rx_func, void *arg, char* rx_data, size_t rx_data_size)
{
	ZstdStream* zs = (ZstdStream*)malloc(sizeof(ZstdStream));

	zs->tx_stream = ZSTD_createCStream();
	ZSTD_initCStream(zs->tx_stream, level);
	zs->rx_stream = ZSTD_createDStream();
	ZSTD_initDStream(zs->rx_stream);
	zs->tx.dst = zs->tx_buf;
	zs->tx.pos = 0;
	zs->tx.size = ZSTD_BUFFER_SIZE;
	zs->rx.src = zs->rx_buf;
	zs->rx.pos = 0;
	zs->rx.size = 0;
	zs->rx_func = rx_func;
	zs->tx_func = tx_func;
	zs->tx_buffered = 0;
	zs->rx_buffered = 0;
	zs->tx_not_flushed = 0;
	zs->rx_error = NULL;
	zs->arg = arg;
	zs->tx_total = zs->tx_total_raw = 0;
	zs->rx_total = zs->rx_total_raw = 0;
	zs->rx.size = rx_data_size;
	Assert(rx_data_size < ZSTD_BUFFER_SIZE);
	memcpy(zs->rx_buf, rx_data, rx_data_size);

	return (ZpqStream*)zs;
}

static ssize_t
zstd_read(ZpqStream *zstream, void *buf, size_t size)
{
	ZstdStream* zs = (ZstdStream*)zstream;
	ssize_t rc;
	ZSTD_outBuffer out;
	out.dst = buf;
	out.pos = 0;
	out.size = size;

	while (1)
	{
		if (zs->rx.pos != zs->rx.size || zs->rx_buffered == 0)
		{
			rc = ZSTD_decompressStream(zs->rx_stream, &out, &zs->rx);
			if (ZSTD_isError(rc))
			{
				zs->rx_error = ZSTD_getErrorName(rc);
				return ZPQ_DECOMPRESS_ERROR;
			}
			/* Return result if we fill requested amount of bytes or read operation was performed */
			if (out.pos != 0)
			{
				zs->rx_total_raw += out.pos;
				zs->rx_buffered = 0;
				return out.pos;
			}
			zs->rx_buffered = rc;
			if (zs->rx.pos == zs->rx.size)
			{
				zs->rx.pos = zs->rx.size = 0; /* Reset rx buffer */
			}
		}
		rc = zs->rx_func(zs->arg, (char*)zs->rx.src + zs->rx.size, ZSTD_BUFFER_SIZE - zs->rx.size);
		if (rc > 0) /* read fetches some data */
		{
			zs->rx.size += rc;
			zs->rx_total += rc;
		}
		else /* read failed */
		{
			zs->rx_total_raw += out.pos;
			return rc;
		}
	}
}

static ssize_t
zstd_write(ZpqStream *zstream, void const *buf, size_t size, size_t *processed)
{
	ZstdStream* zs = (ZstdStream*)zstream;
	ssize_t rc;
	ZSTD_inBuffer in_buf;
	in_buf.src = buf;
	in_buf.pos = 0;
	in_buf.size = size;

	do
	{
		if (zs->tx.pos == 0) /* Compress buffer is empty */
		{
			zs->tx.dst = zs->tx_buf; /* Reset pointer to the beginning of buffer */

			if (in_buf.pos < size) /* Has something to compress in input buffer */
				ZSTD_compressStream(zs->tx_stream, &zs->tx, &in_buf);

			if (in_buf.pos == size) /* All data is compressed: flushed internal zstd buffer */
			{
				zs->tx_not_flushed = ZSTD_flushStream(zs->tx_stream, &zs->tx);
			}
		}
		rc = zs->tx_func(zs->arg, zs->tx.dst, zs->tx.pos);
		if (rc > 0)
		{
			zs->tx.pos -= rc;
			zs->tx.dst = (char*)zs->tx.dst + rc;
			zs->tx_total += rc;
		}
		else
		{
			*processed = in_buf.pos;
			zs->tx_buffered = zs->tx.pos;
			zs->tx_total_raw += in_buf.pos;
			return rc;
		}
    /* repeat sending while there is some data in input or internal zstd buffer */
    } while (in_buf.pos < size || zs->tx_not_flushed);

	zs->tx_total_raw += in_buf.pos;
	zs->tx_buffered = zs->tx.pos;
	return in_buf.pos;
}

static void
zstd_free(ZpqStream *zstream)
{
	ZstdStream* zs = (ZstdStream*)zstream;
	if (zs != NULL)
	{
		ZSTD_freeCStream(zs->tx_stream);
		ZSTD_freeDStream(zs->rx_stream);
		free(zs);
	}
}

static char const*
zstd_error(ZpqStream *zstream)
{
	ZstdStream* zs = (ZstdStream*)zstream;
	return zs->rx_error;
}

static size_t
zstd_buffered_tx(ZpqStream *zstream)
{
	ZstdStream* zs = (ZstdStream*)zstream;
	return zs != NULL ? zs->tx_buffered + zs->tx_not_flushed : 0;
}

static size_t
zstd_buffered_rx(ZpqStream *zstream)
{
	ZstdStream* zs = (ZstdStream*)zstream;
	return zs != NULL ? zs->rx.size - zs->rx.pos : 0;
}

static char const*
zstd_name(void)
{
	return "zstd";
}

#endif

#if HAVE_LIBZ

#include <stdlib.h>
#include <zlib.h>

#define ZLIB_BUFFER_SIZE       8192 /* We have to flush stream after each protocol command
									 * and command is mostly limited by record length,
									 * which in turn usually less than page size (except TOAST)
									 */

typedef struct ZlibStream
{
	ZpqStream      common;

	z_stream tx;
	z_stream rx;

	zpq_tx_func    tx_func;
	zpq_rx_func    rx_func;
	void*          arg;
    unsigned       tx_deflate_pending;
	size_t         tx_buffered;

	Bytef          tx_buf[ZLIB_BUFFER_SIZE];
	Bytef          rx_buf[ZLIB_BUFFER_SIZE];
} ZlibStream;

static ZpqStream*
zlib_create(int level, zpq_tx_func tx_func, zpq_rx_func rx_func, void *arg, char* rx_data, size_t rx_data_size)
{
	int rc;
	ZlibStream* zs = (ZlibStream*)malloc(sizeof(ZlibStream));
	memset(&zs->tx, 0, sizeof(zs->tx));
	zs->tx.next_out = zs->tx_buf;
	zs->tx.avail_out = ZLIB_BUFFER_SIZE;
	zs->tx_buffered = 0;
	rc = deflateInit(&zs->tx, level);
	if (rc != Z_OK)
	{
		free(zs);
		return NULL;
	}
	Assert(zs->tx.next_out == zs->tx_buf && zs->tx.avail_out == ZLIB_BUFFER_SIZE);

	memset(&zs->rx, 0, sizeof(zs->tx));
	zs->rx.next_in = zs->rx_buf;
	zs->rx.avail_in = ZLIB_BUFFER_SIZE;
	zs->tx_deflate_pending = 0;
	rc = inflateInit(&zs->rx);
	if (rc != Z_OK)
	{
		free(zs);
		return NULL;
	}
	Assert(zs->rx.next_in == zs->rx_buf && zs->rx.avail_in == ZLIB_BUFFER_SIZE);

	zs->rx.avail_in = rx_data_size;
	Assert(rx_data_size < ZLIB_BUFFER_SIZE);
	memcpy(zs->rx_buf, rx_data, rx_data_size);

	zs->rx_func = rx_func;
	zs->tx_func = tx_func;
	zs->arg = arg;

	return (ZpqStream*)zs;
}

static ssize_t
zlib_read(ZpqStream *zstream, void *buf, size_t size)
{
	ZlibStream* zs = (ZlibStream*)zstream;
	int rc;
	zs->rx.next_out = (Bytef *)buf;
	zs->rx.avail_out = size;

	while (1)
	{
		if (zs->rx.avail_in != 0) /* If there is some data in receiver buffer, then decompress it */
		{
			rc = inflate(&zs->rx, Z_SYNC_FLUSH);
			if (rc != Z_OK && rc != Z_BUF_ERROR)
			{
				return ZPQ_DECOMPRESS_ERROR;
			}
			if (zs->rx.avail_out != size)
			{
				return size - zs->rx.avail_out;
			}
			if (zs->rx.avail_in == 0)
			{
				zs->rx.next_in = zs->rx_buf;
			}
		}
		else
		{
			zs->rx.next_in = zs->rx_buf;
		}
		rc = zs->rx_func(zs->arg, zs->rx.next_in + zs->rx.avail_in, zs->rx_buf + ZLIB_BUFFER_SIZE - zs->rx.next_in - zs->rx.avail_in);
		if (rc > 0)
		{
			zs->rx.avail_in += rc;
		}
		else
		{
			return rc;
		}
	}
}

static ssize_t
zlib_write(ZpqStream *zstream, void const *buf, size_t size, size_t *processed)
{
	ZlibStream* zs = (ZlibStream*)zstream;
    int rc;
	zs->tx.next_in = (Bytef *)buf;
	zs->tx.avail_in = size;
	do
	{
		if (zs->tx.avail_out == ZLIB_BUFFER_SIZE) /* Compress buffer is empty */
		{
			zs->tx.next_out = zs->tx_buf; /* Reset pointer to the  beginning of buffer */

			if (zs->tx.avail_in != 0 || (zs->tx_deflate_pending > 0)) /* Has something in input or deflate buffer */
			{
				rc = deflate(&zs->tx, Z_SYNC_FLUSH);
				Assert(rc == Z_OK);
                deflatePending(&zs->tx, &zs->tx_deflate_pending, Z_NULL); /* check if any data left in deflate buffer */
				zs->tx.next_out = zs->tx_buf; /* Reset pointer to the  beginning of buffer */
			}
		}
		rc = zs->tx_func(zs->arg, zs->tx.next_out, ZLIB_BUFFER_SIZE - zs->tx.avail_out);
		if (rc > 0)
		{
			zs->tx.next_out += rc;
			zs->tx.avail_out += rc;
		}
		else
		{
			*processed = size - zs->tx.avail_in;
			zs->tx_buffered = ZLIB_BUFFER_SIZE - zs->tx.avail_out;
			return rc;
		}
    /* repeat sending while there is some data in input or deflate buffer */
	} while (zs->tx.avail_in != 0 || zs->tx_deflate_pending > 0);

	zs->tx_buffered = ZLIB_BUFFER_SIZE - zs->tx.avail_out;

	return size - zs->tx.avail_in;
}

static void
zlib_free(ZpqStream *zstream)
{
	ZlibStream* zs = (ZlibStream*)zstream;
	if (zs != NULL)
	{
		inflateEnd(&zs->rx);
		deflateEnd(&zs->tx);
		free(zs);
	}
}

static char const*
zlib_error(ZpqStream *zstream)
{
	ZlibStream* zs = (ZlibStream*)zstream;
	return zs->rx.msg;
}

static size_t
zlib_buffered_tx(ZpqStream *zstream)
{
	ZlibStream* zs = (ZlibStream*)zstream;
	return zs != NULL ? zs->tx_buffered + zs->tx_deflate_pending : 0;
}

static size_t
zlib_buffered_rx(ZpqStream *zstream)
{
	ZlibStream* zs = (ZlibStream*)zstream;
	return zs != NULL ? zs->rx.avail_in : 0;
}

static char const*
zlib_name(void)
{
	return "zlib";
}

#endif

static char const*
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
	{zstd_name, zstd_create, zstd_read, zstd_write, zstd_free, zstd_error, zstd_buffered_tx, zstd_buffered_rx},
#endif
#if HAVE_LIBZ
	{zlib_name, zlib_create, zlib_read, zlib_write, zlib_free, zlib_error, zlib_buffered_tx, zlib_buffered_rx},
#endif
	{no_compression_name}
};

/*
 * Index of used compression algorithm in zpq_algorithms array.
 */
ZpqStream*
zpq_create(int algorithm_impl, int level, zpq_tx_func tx_func, zpq_rx_func rx_func, void *arg, char* rx_data, size_t rx_data_size)
{
	ZpqStream* stream = zpq_algorithms[algorithm_impl].create(level, tx_func, rx_func, arg, rx_data, rx_data_size);
	if (stream)
		stream->algorithm = &zpq_algorithms[algorithm_impl];
	return stream;
}

ssize_t
zpq_read(ZpqStream *zs, void *buf, size_t size)
{
	return zs->algorithm->read(zs, buf, size);
}

ssize_t
zpq_write(ZpqStream *zs, void const *buf, size_t size, size_t* processed)
{
	return zs->algorithm->write(zs, buf, size, processed);
}

void
zpq_free(ZpqStream *zs)
{
	if (zs)
		zs->algorithm->free(zs);
}

char const*
zpq_error(ZpqStream *zs)
{
	return zs->algorithm->error(zs);
}


size_t
zpq_buffered_rx(ZpqStream *zs)
{
	return zs ? zs->algorithm->buffered_rx(zs) : 0;
}

size_t
zpq_buffered_tx(ZpqStream *zs)
{
	return zs ? zs->algorithm->buffered_tx(zs) : 0;
}

/*
 * Get list of the supported algorithms.
 */
char**
zpq_get_supported_algorithms(void)
{
	size_t n_algorithms = sizeof(zpq_algorithms)/sizeof(*zpq_algorithms);
	char** algorithm_names = malloc(n_algorithms*sizeof(char*));

	for (size_t i = 0; i < n_algorithms; i++)
	{
		algorithm_names[i] = (char*)zpq_algorithms[i].name();
	}

	return algorithm_names;
}

char const*
zpq_algorithm_name(ZpqStream *zs)
{
	return zs ? zs->algorithm->name() : NULL;
}
