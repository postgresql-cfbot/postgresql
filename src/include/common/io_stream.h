/*-------------------------------------------------------------------------
 *
 * io_stream.c
 *	  defintions for managing layers of streaming IO.
 *	  In general the base layer will work with raw sockets, and then
 *    additional layers will add features such as encryption and
 *    compression.
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/include/common/io_stream.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef IO_STREAM_H
#define IO_STREAM_H

/* Opaque structs should only be interacted with through corresponding functions*/
typedef struct IoStreamLayer IoStreamLayer;
typedef struct IoStream IoStream;

typedef ssize_t (*io_stream_read_func) (IoStreamLayer * self, void *context, void *data, size_t size, bool buffered_only);
typedef int (*io_stream_write_func) (IoStreamLayer * self, void *context, void const *data, size_t size, size_t *bytes_written);
typedef bool (*io_stream_predicate) (void *context);
typedef void (*io_stream_consumer) (void *context);

typedef struct IoStreamProcessor
{
	/*
	 * Required Should call io_stream_next_read with self either directly or
	 * indirectly to recieve bytes from the underlying layer of the stream
	 */
	io_stream_read_func read;

	/*
	 * Required Should call io_stream_next_write with self either directly or
	 * indirectly to write bytes to the underlying layer of the stream
	 */
	io_stream_write_func write;

	/*
	 * Optional Return true if this layer is holding buffered readable data
	 */
	io_stream_predicate buffered_read_data;

	/*
	 * Optional Return true if this layer is holding buffered writable data
	 */
	io_stream_predicate buffered_write_data;

	/*
	 * Optional Will be called if data being sent to the stream has been reset
	 * early (e.g. due to a transmission error). Only necessary if the
	 * processor inspects the messages of the stream and tracks related state
	 */
	io_stream_consumer reset_write_state;

	/*
	 * Optional will be called as part of io_stream_destroy when cleaning up
	 * the stream
	 */
	io_stream_consumer destroy;
}			IoStreamProcessor;

/*
 * Allocate a new IoStream and return the address to the caller. IoStreams should always be destroyed with
 * the corresponding io_stream_destroy function
 */
extern IoStream * io_stream_create(void);
/*
 * Adds new processors to the IO stream
 *
 * processorDescription contains collection of function pointers for this layer
 * context (optional) pointer with context to be used in reader/writer
 *
 * Returns the newly created processor
 * */
extern IoStreamLayer * io_stream_add_layer(IoStream * stream, IoStreamProcessor * processorDescription, void *context);
/*
 * Destroy an IoStream, freeing all associated memory
 */
extern void io_stream_destroy(IoStream * stream);

/*
 * Read data from the stream
 *
 * Reads at most size bytes into the buffer pointed to by data, and returns
 * the number of bytes read. If buffered_only is true, then only data that
 * was stored in an in-process buffer will be returned and this function will
 * not block. In that case, a return value of 0 simply means there was no
 * buffered data available and does not mean the stream has reached EOF.
 */
extern ssize_t io_stream_read(IoStream * stream, void *data, size_t size, bool buffered_only);

/*
 * Write data to the stream
 *
 * Writes at most size bytes from the buffer pointed to by data, and returns
 * true on success, false on failure, with the specific error in errno. The
 * count of bytes written from data will be stored in bytes_written and may
 * be non-zero even on failure
 */
extern int	io_stream_write(IoStream * stream, void const *data, size_t size, size_t *bytes_written);

/*
 * Check if there is data buffered in memory waiting to be read (e.g. if
 * compression is in use and more uncompressed data was read than fit into
 * the provided buffer)
 */
extern bool io_stream_buffered_read_data(IoStream * stream);

/*
 * Check if there is data buffered in memory waiting to be written to the underlying backing store
 */
extern bool io_stream_buffered_write_data(IoStream * stream);

/*
 * Resets any state in the processors about currently in-flight messages
 * Should be called if message transmission is aborted for any reason
 */
extern void io_stream_reset_write_state(IoStream * stream);

/*
 * Read data from the next layer of the stream
 * (to be used by io_stream_read_func)
 *
 * Reads at most size bytes into the buffer pointed to by data, and returns
 * the number of bytes read
 */
extern ssize_t io_stream_next_read(IoStreamLayer * layer, void *data, size_t size, bool buffered_only);

/*
 * Write data to the next layer of the stream
 * (to be used by io_stream_write_func)
 *
 * Writes at most size bytes from the buffer pointed to by data, and returns
 * true on success, false on failure, with the specific error in errno. The
 * count of bytes written from data will be stored in bytes_written and may
 * be non-zero even on failure
 */
extern int	io_stream_next_write(IoStreamLayer * layer, void const *data, size_t size, size_t *bytes_written);

#endif							/* //IO_STREAM_H */
