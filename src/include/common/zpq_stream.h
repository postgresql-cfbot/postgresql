/*
 * zpq_stream.h
 *     Streaiming compression for libpq
 */

#ifndef ZPQ_STREAM_H
#define ZPQ_STREAM_H

#include <stdlib.h>

#define ZPQ_IO_ERROR (-1)
#define ZPQ_DECOMPRESS_ERROR (-2)

#define ZPQ_DEFAULT_COMPRESSION_LEVEL (1)

struct ZpqStream;
typedef struct ZpqStream ZpqStream;

typedef ssize_t(*zpq_tx_func)(void* arg, void const* data, size_t size);
typedef ssize_t(*zpq_rx_func)(void* arg, void* data, size_t size);

ZpqStream* zpq_create(int impl, int level, zpq_tx_func tx_func, zpq_rx_func rx_func, void* arg, char* rx_data, size_t rx_data_size);
ssize_t zpq_read(ZpqStream* zs, void* buf, size_t size);
ssize_t zpq_write(ZpqStream* zs, void const* buf, size_t size, size_t* processed);
char const* zpq_error(ZpqStream* zs);
size_t zpq_buffered_rx(ZpqStream* zs);
size_t zpq_buffered_tx(ZpqStream* zs);
void zpq_free(ZpqStream* zs);
char const* zpq_algorithm_name(ZpqStream* zs);

/*
  Returns zero terminated array with compression algorithms names
*/
char** zpq_get_supported_algorithms(void);

#endif
