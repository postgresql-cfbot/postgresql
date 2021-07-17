/*
 * z_stream.h
 *     Streaming compression
 */


#ifndef Z_STREAM_H
#define Z_STREAM_H

#include <stdlib.h>

#define ZS_OK (0)
#define ZS_IO_ERROR (-1)
#define ZS_DECOMPRESS_ERROR (-2)
#define ZS_COMPRESS_ERROR (-3)
#define ZS_STREAM_END (-4)
#define ZS_DATA_PENDING (-5)

struct ZStream;
typedef struct ZStream ZStream;

#endif

/*
 * Create compression stream with rx/tx function for reading/sending compressed data.
 * c_alg_impl: index of chosen compression algorithm
 * c_level: compression c_level
 * d_alg_impl: index of chosen decompression algorithm
 */
extern ZStream * zs_create(int c_alg_impl, int c_level, int d_alg_impl);

/*
 * Read up to "size" raw (decompressed) bytes.
 * Returns number of decompressed bytes or error code.
 * Error code is either ZS_DECOMPRESS_ERROR or error code returned by the rx function.
 */
extern ssize_t zs_read(ZStream * zs, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed);

/*
 * Write up to "size" raw (decompressed) bytes.
 * Returns number of written raw bytes or error code.
 * Error code is either ZS_COMPRESS_ERROR or error code returned by the tx function.
 * In the last case number of bytes written is stored in *processed.
 */
extern ssize_t zs_write(ZStream * zs, void const *buf, size_t size, size_t *processed, void *dst, size_t dst_size, size_t *dst_processed);

/*
 * Get decompressor error message.
 */
extern char const *zs_decompress_error(ZStream * zs);

/*
 * Get compressor error message.
 */
extern char const *zs_compress_error(ZStream * zs);

/*
 * Return true if non-flushed data might left in internal rx decompression buffer.
 */
extern bool zs_buffered_rx(ZStream * zs);

/*
 * Return true if non-flushed data might left in internal tx compression buffer.
 */
extern bool zs_buffered_tx(ZStream * zs);

/*
 * End the compression stream.
 */
extern ssize_t zs_end(ZStream * zs, void *dst, size_t dst_size, size_t *dst_processed);

/*
 * Free stream created by zs_create function.
 */
extern void zs_free(ZStream * zs);

/*
 * Get the name of chosen compression algorithm.
 */
extern char const *zs_compress_algorithm_name(ZStream * zs);

/*
 * Get the name of chosen decompression algorithm.
 */
extern char const *zs_decompress_algorithm_name(ZStream * zs);

/*
  Returns zero terminated array with compression algorithms names
*/
extern char **zs_get_supported_algorithms(void);
