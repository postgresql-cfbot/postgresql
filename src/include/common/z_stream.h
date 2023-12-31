/*-------------------------------------------------------------------------
 *
 * z_stream.h
 *	  Streaming compression algorithms
 *
 * Copyright (c) 2018-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/z_stream.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef Z_STREAM_H
#define Z_STREAM_H

#include <stdlib.h>

#include "compression.h"

#define ZS_OK (0)
#define ZS_IO_ERROR (-1)
#define ZS_DECOMPRESS_ERROR (-2)
#define ZS_COMPRESS_ERROR (-3)
#define ZS_STREAM_END (-4)
#define ZS_INCOMPLETE_SRC (-5)	/* cannot decompress unless full src message
								 * is fetched */

struct ZStream;
typedef struct ZStream ZStream;

#endif

/*
 * Create compression stream for sending compressed data.
 * spec: specifications of chosen decompression algorithm
 */
extern ZStream * zs_create_compressor(pg_compress_specification *spec);

/*
 * Create decompression stream for reading compressed data.
 * spec: specifications of chosen decompression algorithm
 */
extern ZStream * zs_create_decompressor(pg_compress_specification *spec);

/*
 * Read up to "size" raw (decompressed) bytes.
 * Returns ZS_OK on success or error code.
 * Stores bytes read from src in src_processed, bytes written to dst in dst_process.
 */
extern int	zs_read(ZStream * zs, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed);

/*
 * Write up to "size" raw (decompressed) bytes.
 * Returns number of written raw bytes or error code.
 * Returns ZS_OK on success or error code.
 * Stores bytes read from buf in processed, bytes written to dst in dst_process
 */
extern int	zs_write(ZStream * zs, void const *buf, size_t size, size_t *processed, void *dst, size_t dst_size, size_t *dst_processed);

/*
 * Returns if there is buffered raw data remaining in the stream to compress
 */
extern bool zs_compress_buffered(ZStream * zs);

/*
 * Returns if there is buffered decompressed data remaining in the stream to read
 */
extern bool zs_decompress_buffered(ZStream * zs);

/*
 * Get decompressor error message.
 */
extern char const *zs_decompress_error(ZStream * zs);

/*
 * Get compressor error message.
 */
extern char const *zs_compress_error(ZStream * zs);

/*
 * End the compression stream.
 */
extern int	zs_end_compression(ZStream * zs, void *dst, size_t dst_size, size_t *dst_processed);

/*
 * Free stream created by zs_create_compressor function.
 */
extern void zs_compressor_free(ZStream * zs);

/*
 * Free stream created by zs_create_decompressor function.
 */
extern void zs_decompressor_free(ZStream * zs);

/*
 * Get the descriptor of chosen algorithm.
 */
extern pg_compress_algorithm zs_algorithm(ZStream * zs);
