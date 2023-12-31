/*-------------------------------------------------------------------------
 *
 * zpq_stream.h
 *	  IO stream layer applying ZStream compression to libpq
 *
 * Copyright (c) 2018-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/zpq_stream.h
 *
 *-------------------------------------------------------------------------
 */
#include "io_stream.h"
#include "z_stream.h"

#ifndef ZPQ_STREAM_H
#define ZPQ_STREAM_H

#define ZPQ_FATAL_ERROR (-7)
struct ZpqStream;
typedef struct ZpqStream ZpqStream;

#endif

/*
 * Create compression stream with rx/tx function for reading/sending compressed data.
 * io_stream: IO Stream to layer on top of
 * rx_data: received data (compressed data already fetched from input stream)
 * rx_data_size: size of data fetched from input stream
 * The returned ZpqStream can only be destroyed by destoing the IoStream with io_stream_destroy.
 */
extern ZpqStream * zpq_create(pg_compress_specification *compressors, size_t n_compressors, IoStream * stream);

/*
 * Start compressing applicable outgoing data once the connection is sufficiently set up
 */
extern void zpq_enable_compression(ZpqStream * zpq, pg_compress_algorithm *algorithms, size_t n_algorithms);

/*
 * Get decompressor error message.
 */
extern char const *zpq_decompress_error(ZpqStream * zpq);

/*
 * Get compressor error message.
 */
extern char const *zpq_compress_error(ZpqStream * zpq);

/*
 * Get the name of the current compression algorithm.
 */
extern pg_compress_algorithm zpq_compress_algorithm(ZpqStream * zpq);

/*
 * Get the name of the current decompression algorithm.
 */
extern pg_compress_algorithm zpq_decompress_algorithm(ZpqStream * zpq);

/*
 * Parse the compression setting.
 * Compressors must be an array of length COMPRESSION_ALGORITHM_COUNT
 * Returns:
 * - 1 if the compression setting is valid
 * - 0 if the compression setting is valid but disabled
 * - -1 if the compression setting is invalid
 * It also populates the compressors array with the recognized compressors. Size of the array is stored in n_compressors.
 * If no supported compressors recognized or if compression is disabled, then n_compressors is set to 0.
 */
extern int
			zpq_parse_compression_setting(const char *val, pg_compress_specification *compressors, size_t *n_compressors);

/* Serialize the compressors array to string so it can be transmitted to the other side during the compression startup.
 * For example, for array of two compressors (zstd, level 1), (zlib, level 2) resulting string would look like "zstd:1;zlib:2".
 * Returns the resulting string.
 */
extern char
		   *zpq_serialize_compressors(pg_compress_specification const *compressors, size_t n_compressors);

/* Deserialize the compressors string received during the compression setup to a compressors array.
 * Compressors must be an array of length COMPRESSION_ALGORITHM_COUNT
 * Returns:
 * - true if the compressors string is successfully parsed
 * - false otherwise
 * It also populates the compressors array with the recognized compressors. Size of the array is stored in n_compressors.
 * If no supported compressors are recognized or c_string is empty, then n_compressors is set to 0.
 */
bool
			zpq_deserialize_compressors(char const *c_string, pg_compress_specification *compressors, size_t *n_compressors);

/* Returns the currently enabled compression algorithms using zpq_serialize_compressors */
char	   *zpq_algorithms(ZpqStream * zpq);
