/*-------------------------------------------------------------------------
 *
 * zpq_stream.c
 *	  IO stream layer applying ZStream compression to libpq
 *
 * Copyright (c) 2018-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/zpq_stream.c
 *
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif
#include <unistd.h>
#include <math.h>

#include "common/zpq_stream.h"

#include <common/io_stream.h>
#include <libpq/protocol.h>

#include "pg_config.h"
#include "port/pg_bswap.h"

/* log warnings on backend */
#ifndef FRONTEND
#define pg_log_warning(...) elog(WARNING, __VA_ARGS__)
#else
#define pg_log_warning(...) (void)0
#endif

#ifndef FRONTEND
#include "utils/memutils.h"
#define ALLOC(size) MemoryContextAlloc(TopMemoryContext, size)
#define STRDUP(str) pstrdup(str)
#define FREE(size) pfree(size)
#else
#define ALLOC(size) malloc(size)
#define STRDUP(str) strdup(str)
#define FREE(size) free(size)
#endif

/* ZpqBuffer size, in bytes */
#define ZPQ_BUFFER_SIZE 8192000

#define ZPQ_COMPRESS_THRESHOLD 60

/* startup messages have no type field and therefore have a null first byte */
#define MESSAGE_TYPE_OFFSET(msg_type) (msg_type == '\0' ? 0 : 1)

typedef struct ZpqBuffer ZpqBuffer;


/* ZpqBuffer used as RX/TX buffer in ZpqStream */
struct ZpqBuffer
{
	char		buf[ZPQ_BUFFER_SIZE];
	size_t		size;			/* current size of buf */
	size_t		pos;			/* current position in buf, in range [0, size] */
};

/*
 * Write up to "src_size" raw (decompressed) bytes.
 * Returns number of written raw bytes or error code.
 * Error code is either ZPQ_COMPRESS_ERROR or error code returned by the tx function.
 * In the last case number of bytes written is stored in *src_processed.
 */
static int	zpq_write(IoStreamLayer * layer, ZpqStream * zpq, void const *src, size_t src_size, size_t *bytes_written);

/*
 * Read up to "dst_size" raw (decompressed) bytes.
 * Returns number of decompressed bytes or error code.
 * Error code is either ZPQ_DECOMPRESS_ERROR or error code returned by the rx function.
 */
static ssize_t zpq_read(IoStreamLayer * layer, ZpqStream * zpq, void *dst, size_t dst_size, bool buffered_only);

/*
 * Return true if non-flushed data left in internal rx decompression buffer.
 */
static bool zpq_buffered_rx(ZpqStream * zpq);

/*
 * Return true if non-flushed data left in internal tx compression buffer.
 */
static bool zpq_buffered_tx(ZpqStream * zpq);

/*
 * Clear inflight messages and tracked state
 */
static void zpq_reset_write_state(ZpqStream * zpq);

/*
 * Free stream created by zs_create function.
 */
static void zpq_free(ZpqStream * zpq);

IoStreamProcessor zpq_processor = {
	.read = (io_stream_read_func) zpq_read,
	.write = (io_stream_write_func) zpq_write,
	.buffered_read_data = (io_stream_predicate) zpq_buffered_rx,
	.buffered_write_data = (io_stream_predicate) zpq_buffered_tx,
	.reset_write_state = (io_stream_consumer) zpq_reset_write_state,
	.destroy = (io_stream_consumer) zpq_free
};

static inline void
zpq_buf_init(ZpqBuffer * zb)
{
	zb->size = 0;
	zb->pos = 0;
}

static inline size_t
zpq_buf_left(ZpqBuffer * zb)
{
	Assert(zb->buf);
	return ZPQ_BUFFER_SIZE - zb->size;
}

static inline size_t
zpq_buf_unread(ZpqBuffer * zb)
{
	return zb->size - zb->pos;
}

static inline char *
zpq_buf_size(ZpqBuffer * zb)
{
	return (char *) (zb->buf) + zb->size;
}

static inline char *
zpq_buf_pos(ZpqBuffer * zb)
{
	return (char *) (zb->buf) + zb->pos;
}

static inline void
zpq_buf_size_advance(ZpqBuffer * zb, size_t value)
{
	zb->size += value;
}

static inline void
zpq_buf_pos_advance(ZpqBuffer * zb, size_t value)
{
	zb->pos += value;
}

static inline void
zpq_buf_reuse(ZpqBuffer * zb)
{
	size_t		unread = zpq_buf_unread(zb);

	if (unread > 5)				/* can read message header, don't do anything */
		return;
	if (unread == 0)
	{
		zb->size = 0;
		zb->pos = 0;
		return;
	}
	memmove(zb->buf, zb->buf + zb->pos, unread);
	zb->size = unread;
	zb->pos = 0;
}

struct ZpqStream
{
	ZStream    *c_stream;		/* underlying compression stream */
	ZStream    *d_stream;		/* underlying decompression stream */

	bool		is_compressing; /* current compression state */

	bool		is_decompressing;	/* current decompression state */
	bool		reading_compressed_header;	/* compression header processing
											 * incomplete */
	size_t		rx_msg_bytes_left;	/* number of bytes left to process without
									 * changing the decompression state */
	size_t		tx_msg_bytes_left;	/* number of bytes left to process without
									 * changing the compression state */

	ZpqBuffer	rx_in;			/* buffer for unprocessed data read from next
								 * stream layer */
	ZpqBuffer	tx_in;			/* buffer for unprocessed data consumed by
								 * zpq_write */
	ZpqBuffer	tx_out;			/* buffer for processed data waiting for send
								 * to next stream layer */

	pg_compress_specification *compressors; /* compressors array holds the
											 * available compressors to use
											 * for compression/decompression */
	size_t		n_compressors;	/* size of the compressors array */
	pg_compress_algorithm compress_algs[COMPRESSION_ALGORITHM_COUNT - 1];	/* array of compression
																			 * algorithms supported
																			 * by the reciever of
																			 * the stream */
	pg_compress_algorithm compress_alg; /* active compression algorithm */
	bool		restart_compression;	/* restart the compression stream
										 * after a client request */
	bool		first_compressed_message;	/* track if we have sent the new
											 * compression algorithm */
	pg_compress_algorithm decompress_alg;	/* active decompression algorithm */
};

/*
 * Choose the algorithm to use for the message of msg_type with msg_len.
 * Returns a pg_compress_algorithm with a registered compressor, or PG_COMPRESSION_NONE if no compressor is appropriate
 */
static inline pg_compress_algorithm
zpq_choose_algorithm(ZpqStream * zpq, char msg_type, uint32 msg_len)
{
	/*
	 * in theory we could choose the algorithm based on the message type
	 * and/or more complex heuristics, but at least for now we will just use
	 * the first available algorithm (which defaults to none if compression
	 * has not yet been enabled) for message types that would most obviously
	 * benefit from compression
	 */
	/* force enable for testing */
	if (true || (msg_len >= ZPQ_COMPRESS_THRESHOLD && (msg_type == PqMsg_CopyData || msg_type == PqMsg_DataRow || msg_type == PqMsg_Query)))
	{
		return zpq->compress_algs[0];
	}
	return PG_COMPRESSION_NONE;
}

static inline bool
zpq_should_compress(ZpqStream * zpq, char msg_type, uint32 msg_len)
{
	return zpq_choose_algorithm(zpq, msg_type, msg_len) != PG_COMPRESSION_NONE;
}

static inline pg_compress_specification *
zpq_find_compressor(ZpqStream * zpq, pg_compress_algorithm algorithm)
{
	int			i;

	for (i = 0; i < zpq->n_compressors; i++)
	{
		if (algorithm == zpq->compressors[i].algorithm)
			return &zpq->compressors[i];
	}
	return NULL;
}

static inline bool
zpq_is_compressed_msg(char msg_type)
{
	return msg_type == PqMsg_CompressedMessage;
}

ZpqStream *
zpq_create(pg_compress_specification *compressors, size_t n_compressors, IoStream * stream)
{
	ZpqStream  *zpq;

	/* zpqStream needs at least one compressor */
	if (n_compressors == 0 || compressors == NULL)
	{
		return NULL;
	}
	zpq = (ZpqStream *) ALLOC(sizeof(ZpqStream));
	/* almost all fields should default to 0/NULL/PG_COMPRESSION_NONE */
	memset(zpq, 0, sizeof(ZpqStream));
	zpq->compressors = compressors;
	zpq->n_compressors = n_compressors;
	zpq_buf_init(&zpq->tx_in);
	zpq_buf_init(&zpq->rx_in);
	zpq_buf_init(&zpq->tx_out);

	io_stream_add_layer(stream, &zpq_processor, zpq);

	return zpq;
}

void
zpq_enable_compression(ZpqStream * zpq, pg_compress_algorithm *algorithms, size_t n_algorithms)
{
	Assert(n_algorithms < COMPRESSION_ALGORITHM_COUNT);

	for (int i = 0; i < COMPRESSION_ALGORITHM_COUNT - 1; i++)
	{
		if (i < n_algorithms)
		{
			zpq->compress_algs[i] = algorithms[i];
		}
		else
		{
			zpq->compress_algs[i] = PG_COMPRESSION_NONE;
		}
	}
	zpq->restart_compression = true;
}

/* Compress up to src_size bytes from *src into CompressedData and write it to the tx buffer.
 * Returns ZS_OK on success, ZS_COMPRESS_ERROR if encountered a compression error. */
static inline int
zpq_write_compressed_message(ZpqStream * zpq, char const *src, size_t src_size, size_t *src_processed)
{
	size_t		compressed_len;
	ssize_t		rc;
	uint32		size;
	uint8		algorithm;

	/* check if have enough space */
	if (zpq_buf_left(&zpq->tx_out) <= 6)
	{
		/* too little space for CompressedData, abort */
		*src_processed = 0;
		return ZS_OK;
	}

	compressed_len = 0;
	rc = zs_write(zpq->c_stream, src, src_size, src_processed,
				  zpq_buf_size(&zpq->tx_out) + 6, zpq_buf_left(&zpq->tx_out) - 6, &compressed_len);

	if (compressed_len > 0)
	{
		/* write CompressedData type */
		*zpq_buf_size(&zpq->tx_out) = PqMsg_CompressedMessage;
		size = pg_hton32(compressed_len + 5);

		memcpy(zpq_buf_size(&zpq->tx_out) + 1, &size, sizeof(uint32));	/* write msg length */
		compressed_len += 6;	/* append header length to compressed data
								 * length */
		algorithm = zpq->first_compressed_message ? zpq->compress_alg : 0;
		zpq->first_compressed_message = false;
		memcpy(zpq_buf_size(&zpq->tx_out) + 5, &algorithm, sizeof(uint8));	/* write msg algorithm */
	}

	zpq_buf_size_advance(&zpq->tx_out, compressed_len);
	return rc;
}

/* Copy the data directly from *src to the tx buffer */
static void
zpq_write_uncompressed(ZpqStream * zpq, char const *src, size_t src_size, size_t *src_processed)
{
	src_size = Min(zpq_buf_left(&zpq->tx_out), src_size);
	memcpy(zpq_buf_size(&zpq->tx_out), src, src_size);

	zpq_buf_size_advance(&zpq->tx_out, src_size);
	*src_processed = src_size;
}

/* Determine if should compress the next message and change the current compression state */
static int
zpq_toggle_compression(ZpqStream * zpq, char msg_type, uint32 msg_len)
{
	pg_compress_algorithm new_compress_alg = zpq_choose_algorithm(zpq, msg_type, msg_len);
	pg_compress_specification *spec;
	bool		should_compress = new_compress_alg != PG_COMPRESSION_NONE;

	if (should_compress)
	{
		/*
		 * if the new compressor does not match the current one, change out
		 * the underlying z_stream
		 */
		if (zpq->compress_alg != new_compress_alg || zpq->restart_compression)
		{
			zpq->restart_compression = false;
			zpq->first_compressed_message = true;
			zs_compressor_free(zpq->c_stream);
			spec = zpq_find_compressor(zpq, new_compress_alg);
			if (spec == NULL)
			{
				return ZPQ_FATAL_ERROR;
			}
			zpq->c_stream = zs_create_compressor(spec);
			if (zpq->c_stream == NULL)
			{
				return ZPQ_FATAL_ERROR;
			}
			zpq->compress_alg = new_compress_alg;
		}
	}

	zpq->is_compressing = should_compress;
	zpq->tx_msg_bytes_left = msg_len + MESSAGE_TYPE_OFFSET(msg_type);
	return 0;
}

/*
 * Internal write function. Reads the data from *src buffer,
 * determines the postgres messages type and length.
 * If message matches the compression criteria, it wraps the message into
 * CompressedData. Otherwise, leaves the message unchanged.
 * If *src data ends with incomplete message header, this function is not
 * going to read this message header.
 * Returns 0 on success or error code
 * Number of bytes written is stored in *processed.
 */
static int
zpq_write_internal(ZpqStream * zpq, void const *src, size_t src_size, size_t *processed)
{
	size_t		src_pos = 0;
	ssize_t		rc;

	do
	{
		/*
		 * try to read ahead the next message types and increase
		 * tx_msg_bytes_left, if possible
		 */
		while (zpq->tx_msg_bytes_left > 0 && src_size - src_pos >= zpq->tx_msg_bytes_left + 5)
		{
			char		msg_type = *((char *) src + src_pos + zpq->tx_msg_bytes_left);
			uint32		msg_len;

			memcpy(&msg_len, (char *) src + src_pos + zpq->tx_msg_bytes_left + MESSAGE_TYPE_OFFSET(msg_type), 4);
			msg_len = pg_ntoh32(msg_len);
			if (zpq_should_compress(zpq, msg_type, msg_len) != zpq->is_compressing)
			{
				/*
				 * cannot proceed further, encountered compression toggle
				 * point
				 */
				break;
			}
			zpq->tx_msg_bytes_left += msg_len + MESSAGE_TYPE_OFFSET(msg_type);
		}

		/*
		 * Write CompressedData if currently is compressing or have some
		 * buffered data left in underlying compression stream
		 */
		if (zs_compress_buffered(zpq->c_stream) || (zpq->is_compressing && zpq->tx_msg_bytes_left > 0))
		{
			size_t		buf_processed = 0;
			size_t		to_compress = Min(zpq->tx_msg_bytes_left, src_size - src_pos);

			rc = zpq_write_compressed_message(zpq, (char *) src + src_pos, to_compress, &buf_processed);
			src_pos += buf_processed;
			zpq->tx_msg_bytes_left -= buf_processed;

			if (rc != ZS_OK)
			{
				*processed = src_pos;
				return rc;
			}
		}

		/*
		 * If not going to compress the data from *src, just write it
		 * uncompressed.
		 */
		else if (zpq->tx_msg_bytes_left > 0)
		{						/* determine next message type */
			size_t		copy_len = Min(src_size - src_pos, zpq->tx_msg_bytes_left);
			size_t		copy_processed = 0;

			zpq_write_uncompressed(zpq, (char *) src + src_pos, copy_len, &copy_processed);
			src_pos += copy_processed;
			zpq->tx_msg_bytes_left -= copy_processed;
		}

		/*
		 * Reached the compression toggle point, fetch next message header to
		 * determine compression state.
		 */
		else
		{
			char		msg_type;
			uint32		msg_len;

			if (src_size - src_pos < 5)
			{
				/*
				 * must return here because we can't continue without full
				 * message header
				 */
				*processed = src_pos;
				return 0;
			}

			msg_type = *((char *) src + src_pos);
			memcpy(&msg_len, (char *) src + src_pos + MESSAGE_TYPE_OFFSET(msg_type), 4);
			msg_len = pg_ntoh32(msg_len);
			rc = zpq_toggle_compression(zpq, msg_type, msg_len);
			if (rc)
			{
				*processed = src_pos;
				return rc;
			}
		}

		/*
		 * repeat sending while there is some data in input or internal
		 * compression buffer
		 */
	} while (src_pos < src_size && zpq_buf_left(&zpq->tx_out) > 6);

	*processed = src_pos;
	return 0;
}

int
zpq_write(IoStreamLayer * self, ZpqStream * zpq, void const *src, size_t src_size, size_t *bytes_written)
{
	size_t		src_pos = 0;
	ssize_t		rc;

	/* try to process as much data as possible before calling the tx_func */
	while (zpq_buf_left(&zpq->tx_out) > 6)
	{
		size_t		copy_len = Min(zpq_buf_left(&zpq->tx_in), src_size - src_pos);
		size_t		processed;

		memcpy(zpq_buf_size(&zpq->tx_in), (char *) src + src_pos, copy_len);
		zpq_buf_size_advance(&zpq->tx_in, copy_len);
		src_pos += copy_len;

		if (zpq_buf_unread(&zpq->tx_in) == 0 && !zs_compress_buffered(zpq->c_stream))
		{
			break;
		}

		processed = 0;

		rc = zpq_write_internal(zpq, zpq_buf_pos(&zpq->tx_in), zpq_buf_unread(&zpq->tx_in), &processed);
		zpq_buf_pos_advance(&zpq->tx_in, processed);
		zpq_buf_reuse(&zpq->tx_in);
		if (rc < 0)
		{
			*bytes_written = src_pos;
			return rc;
		}
		if (processed == 0)
		{
			break;
		}
	}
	*bytes_written = src_pos;

	/*
	 * call the tx_func if have any bytes to send
	 */
	while (zpq_buf_unread(&zpq->tx_out))
	{
		size_t		count;

		rc = io_stream_next_write(self, zpq_buf_pos(&zpq->tx_out), zpq_buf_unread(&zpq->tx_out), &count);
		if (!rc && count > 0)
		{
			zpq_buf_pos_advance(&zpq->tx_out, count);
		}
		else
		{
			zpq_buf_reuse(&zpq->tx_out);
			return rc;
		}
	}

	zpq_buf_reuse(&zpq->tx_out);
	return 0;
}

/* Decompress bytes from RX buffer and write up to dst_len of uncompressed data to *dst.
 * Returns:
 * ZS_OK on success,
 * ZS_STREAM_END if reached end of compressed chunk
 * ZS_DECOMPRESS_ERROR if encountered a decompression error */
static inline ssize_t
zpq_read_compressed_message(ZpqStream * zpq, char *dst, size_t dst_len, size_t *dst_processed)
{
	size_t		rx_processed = 0;
	ssize_t		rc;
	size_t		read_len = Min(zpq->rx_msg_bytes_left, zpq_buf_unread(&zpq->rx_in));

	Assert(read_len == zpq->rx_msg_bytes_left);
	rc = zs_read(zpq->d_stream, zpq_buf_pos(&zpq->rx_in), read_len, &rx_processed,
				 dst, dst_len, dst_processed);

	zpq_buf_pos_advance(&zpq->rx_in, rx_processed);
	zpq->rx_msg_bytes_left -= rx_processed;
	return rc;
}

/* Copy up to dst_len bytes from rx buffer to *dst.
 * Returns amount of bytes copied. */
static inline size_t
zpq_read_uncompressed(ZpqStream * zpq, char *dst, size_t dst_len)
{
	size_t		copy_len;

	Assert(zpq_buf_unread(&zpq->rx_in) > 0);
	copy_len = Min(zpq->rx_msg_bytes_left, Min(zpq_buf_unread(&zpq->rx_in), dst_len));

	memcpy(dst, zpq_buf_pos(&zpq->rx_in), copy_len);

	zpq_buf_pos_advance(&zpq->rx_in, copy_len);
	zpq->rx_msg_bytes_left -= copy_len;
	return copy_len;
}

/* Determine if should decompress the next message and
 * change the current decompression state */
static inline void
zpq_toggle_decompression(ZpqStream * zpq)
{
	uint32		msg_len;
	char		msg_type = *zpq_buf_pos(&zpq->rx_in);

	memcpy(&msg_len, zpq_buf_pos(&zpq->rx_in) + MESSAGE_TYPE_OFFSET(msg_type), 4);
	msg_len = pg_ntoh32(msg_len);

	zpq->is_decompressing = zpq_is_compressed_msg(msg_type);
	zpq->rx_msg_bytes_left = msg_len + MESSAGE_TYPE_OFFSET(msg_type);

	if (zpq->is_decompressing)
	{
		/* compressed message header is no longer needed, just skip it */
		zpq_buf_pos_advance(&zpq->rx_in, 5);
		zpq->rx_msg_bytes_left -= 5;
		zpq->reading_compressed_header = true;
	}
}

static inline ssize_t
zpq_process_switch(ZpqStream * zpq)
{
	pg_compress_algorithm algorithm;
	pg_compress_specification *spec;

	if (zpq_buf_unread(&zpq->rx_in) < 1)
	{
		return 0;
	}

	algorithm = *zpq_buf_pos(&zpq->rx_in);

	zpq_buf_pos_advance(&zpq->rx_in, 1);
	zpq->reading_compressed_header = false;
	zpq->rx_msg_bytes_left -= 1;

	/*
	 * if the message specifies an algorithm, restart the decompressor with
	 * the new algorithm (can be the same as the previous one)
	 */
	if (algorithm > 0)
	{
		zs_decompressor_free(zpq->d_stream);
		spec = zpq_find_compressor(zpq, algorithm);
		if (spec == NULL)
		{
			return ZPQ_FATAL_ERROR;
		}
		zpq->d_stream = zs_create_decompressor(spec);
		if (zpq->d_stream == NULL)
		{
			return ZPQ_FATAL_ERROR;
		}
		zpq->decompress_alg = algorithm;
	}

	return 0;
}

ssize_t
zpq_read(IoStreamLayer * self, ZpqStream * zpq, void *dst, size_t dst_size, bool buffered_only)
{
	size_t		dst_pos = 0;
	size_t		dst_processed = 0;
	ssize_t		rc;

	/* Read until some data fetched */
	while (dst_pos == 0)
	{
		zpq_buf_reuse(&zpq->rx_in);

		if (!zpq_buffered_rx(zpq) || (zpq->is_decompressing && zpq_buf_unread(&zpq->rx_in) < zpq->rx_msg_bytes_left))
		{
			rc = io_stream_next_read(self, zpq_buf_size(&zpq->rx_in), zpq_buf_left(&zpq->rx_in), buffered_only);
			if (rc > 0)			/* read fetches some data */
			{
				zpq_buf_size_advance(&zpq->rx_in, rc);
			}
			else if (rc == 0)
			{
				/* got no more data; return what we have */
				return dst_pos;
			}
			else				/* read failed */
			{
				return rc;
			}
		}

		/*
		 * try to read ahead the next message types and increase
		 * rx_msg_bytes_left, if possible (ONLY UNCOMPRESSED MESSAGES)
		 */
		while (!zpq->is_decompressing && zpq->rx_msg_bytes_left > 0 && (zpq_buf_unread(&zpq->rx_in) >= zpq->rx_msg_bytes_left + 5))
		{
			char		msg_type;
			uint32		msg_len;

			msg_type = *(zpq_buf_pos(&zpq->rx_in) + zpq->rx_msg_bytes_left);
			if (zpq_is_compressed_msg(msg_type))
			{
				/*
				 * cannot proceed further, encountered compression toggle
				 * point
				 */
				break;
			}

			memcpy(&msg_len, zpq_buf_pos(&zpq->rx_in) + zpq->rx_msg_bytes_left + MESSAGE_TYPE_OFFSET(msg_type), 4);
			zpq->rx_msg_bytes_left += pg_ntoh32(msg_len) + MESSAGE_TYPE_OFFSET(msg_type);
		}


		/*
		 * If we are in the middle of reading a message, keep reading it until
		 * we reach the end at which point we need to check if we should
		 * toggle compression
		 */
		if (zpq->rx_msg_bytes_left > 0 || zs_decompress_buffered(zpq->d_stream))
		{
			dst_processed = 0;
			if (zpq->is_decompressing || zs_decompress_buffered(zpq->d_stream))
			{
				if (zpq->reading_compressed_header)
				{
					zpq_process_switch(zpq);
				}
				if (!zs_decompress_buffered(zpq->d_stream) && zpq_buf_unread(&zpq->rx_in) < zpq->rx_msg_bytes_left)
				{
					/*
					 * prefer to read only the fully compressed messages or
					 * read if some data is buffered
					 */
					continue;
				}
				rc = zpq_read_compressed_message(zpq, dst, dst_size - dst_pos, &dst_processed);
				dst_pos += dst_processed;
				if (rc == ZS_STREAM_END)
				{
					continue;
				}
				if (rc != ZS_OK)
				{
					return rc;
				}
			}
			else
				dst_pos += zpq_read_uncompressed(zpq, dst, dst_size - dst_pos);
		}
		else if (zpq_buf_unread(&zpq->rx_in) >= 5)
			zpq_toggle_decompression(zpq);
	}
	return dst_pos;
}

bool
zpq_buffered_rx(ZpqStream * zpq)
{
	return zpq ? zpq_buf_unread(&zpq->rx_in) >= 5 || (zpq_buf_unread(&zpq->rx_in) > 0 && zpq->rx_msg_bytes_left > 0) ||
		zs_decompress_buffered(zpq->d_stream) : 0;
}

bool
zpq_buffered_tx(ZpqStream * zpq)
{
	return zpq ? zpq_buf_unread(&zpq->tx_in) >= 5 || (zpq_buf_unread(&zpq->tx_in) > 0 && zpq->tx_msg_bytes_left > 0) || zpq_buf_unread(&zpq->tx_out) > 0 ||
		zs_compress_buffered(zpq->c_stream) : 0;
}

void
zpq_reset_write_state(ZpqStream * zpq)
{
	/*
	 * We need to flush where we are in the msg to ensure we keep our reads
	 * correctly aligned
	 */
	zpq->tx_msg_bytes_left = 0;
	zpq->tx_in.pos = 0;
	zpq->tx_in.size = 0;
	zpq->tx_out.pos = 0;
	zpq->tx_out.size = 0;

	/*
	 * Disable compression for the rest of the life of this connection since
	 * any existing state will be stale, and we only call this after a failure
	 * at which point the connection will be terminated shortly anyways
	 */
	zpq->is_compressing = false;
	for (int i = 0; i < COMPRESSION_ALGORITHM_COUNT - 1; i++)
	{
		zpq->compress_algs[i] = PG_COMPRESSION_NONE;
	}
}

void
zpq_free(ZpqStream * zpq)
{
	if (zpq)
	{
		if (zpq->c_stream)
		{
			zs_compressor_free(zpq->c_stream);
		}
		if (zpq->d_stream)
		{
			zs_decompressor_free(zpq->d_stream);
		}
		FREE(zpq);
	}
}

char const *
zpq_compress_error(ZpqStream * zpq)
{
	return zs_compress_error(zpq->c_stream);
}

char const *
zpq_decompress_error(ZpqStream * zpq)
{
	return zs_decompress_error(zpq->d_stream);
}

pg_compress_algorithm
zpq_compress_algorithm(ZpqStream * zpq)
{
	return zs_algorithm(zpq->c_stream);
}

pg_compress_algorithm
zpq_decompress_algorithm(ZpqStream * zpq)
{
	return zs_algorithm(zpq->d_stream);
}

char *
zpq_algorithms(ZpqStream * zpq)
{
	return zpq_serialize_compressors(zpq->compressors, zpq->n_compressors);
}

int
zpq_parse_compression_setting(const char *val, pg_compress_specification *compressors, size_t *n_compressors)
{
	int			i;

	*n_compressors = 0;
	memset(compressors, 0, sizeof(pg_compress_specification) * COMPRESSION_ALGORITHM_COUNT);

	if (pg_strcasecmp(val, "true") == 0 ||
		pg_strcasecmp(val, "yes") == 0 ||
		pg_strcasecmp(val, "on") == 0 ||
		pg_strcasecmp(val, "1") == 0)
	{
		int			j = 0;

		/*
		 * return all available compressors
		 */
		for (i = 1; i < COMPRESSION_ALGORITHM_COUNT; i++)
		{
			if (supported_compression_algorithm(i))
				*n_compressors += 1;
		}

		for (i = 1; i < COMPRESSION_ALGORITHM_COUNT; i++)
		{
			if (supported_compression_algorithm(i))
			{
				parse_compress_specification(i, NULL, &compressors[j], 0);
				j += 1;
			}
		}
		return 1;
	}

	if (*val == 0 ||
		pg_strcasecmp(val, "false") == 0 ||
		pg_strcasecmp(val, "no") == 0 ||
		pg_strcasecmp(val, "off") == 0 ||
		pg_strcasecmp(val, "0") == 0)
	{
		/* Compression is disabled */
		return 0;
	}

	return zpq_deserialize_compressors(val, compressors, n_compressors) ? 1 : -1;
}

bool
zpq_deserialize_compressors(char const *c_string, pg_compress_specification *compressors, size_t *n_compressors)
{
	int			selected_alg_mask = 0;	/* bitmask of already selected
										 * algorithms to avoid duplicates in
										 * compressors */
	char	   *c_string_dup = STRDUP(c_string);	/* following parsing can
													 * modify the string */
	char	   *p = c_string_dup;

	*n_compressors = 0;
	memset(compressors, 0, sizeof(pg_compress_specification) * COMPRESSION_ALGORITHM_COUNT);

	while (*p != '\0')
	{
		char	   *sep = strchr(p, ';');
		char	   *col;
		char	   *error_detail;
		pg_compress_algorithm algorithm;
		pg_compress_specification *spec = &compressors[*n_compressors];

		if (sep != NULL)
			*sep = '\0';

		col = strchr(p, ':');
		if (col != NULL)
		{
			*col = '\0';
		}
		if (!parse_compress_algorithm(p, &algorithm))
		{
			pg_log_warning("invalid compression algorithm %s", p);
			goto error;
		}

		if (supported_compression_algorithm(algorithm))
		{
			parse_compress_specification(algorithm, col == NULL ? NULL : col + 1, spec, PG_COMPRESSION_OPTION_COMPRESS | PG_COMPRESSION_OPTION_DECOMPRESS);
			error_detail = validate_compress_specification(spec);
			if (error_detail)
			{
				pg_log_warning("invalid compression specification: %s", error_detail);
				goto error;
			}

			if (algorithm == PG_COMPRESSION_NONE)
			{
				pg_log_warning("algorithm none is not valid for protocol compression");
				goto error;
			}

			if (selected_alg_mask & (1 << algorithm))
			{
				/* duplicates are not allowed */
				pg_log_warning("duplicate algorithm %s in compressors string %s", get_compress_algorithm_name(algorithm), c_string);
				goto error;
			}

			*n_compressors += 1;
			selected_alg_mask |= 1 << algorithm;
		}
		else
		{
			/*
			 * Intentionally do not return an error, as we must support
			 * clients/servers parsing algorithms they don't suppport mixed
			 * with ones they do support
			 */
			pg_log_warning("this build does not support compression with %s",
						   get_compress_algorithm_name(algorithm));
		}

		if (sep)
			p = sep + 1;
		else
			break;
	}

	FREE(c_string_dup);
	return true;

error:
	FREE(c_string_dup);
	*n_compressors = 0;
	return false;
}

char *
zpq_serialize_compressors(pg_compress_specification const *compressors, size_t n_compressors)
{
	char	   *res;
	char	   *p;
	size_t		i;
	size_t		total_len = 0;

	if (n_compressors == 0)
	{
		return NULL;
	}

	for (i = 0; i < n_compressors; i++)
	{
		/*
		 * single entry looks like "alg_name:compression_specification," so +2
		 * is for ":" and ";" symbols (or trailing null)
		 */
		total_len += strlen(get_compress_algorithm_name(compressors[i].algorithm)) + serialize_compress_specification(&compressors[i], NULL, 0) + 2;
	}

	res = p = ALLOC(total_len);

	for (i = 0; i < n_compressors; i++)
	{
		p += sprintf(p, "%s:", get_compress_algorithm_name(compressors[i].algorithm));
		p += serialize_compress_specification(&compressors[i], p, total_len - (p - res));
		if (i < n_compressors - 1)
			*p++ = ';';
	}
	return res;
}
