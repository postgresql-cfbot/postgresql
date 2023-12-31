/*-------------------------------------------------------------------------
 *
 * io_stream.c
 *	  functions related to managing layers of streaming IO.
 *	  In general the base layer will work with raw sockets, and then
 *    additional layers will add features such as encryption and
 *    compression.
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/io_stream.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include <common/io_stream.h>

#ifndef FRONTEND
#include "utils/memutils.h"
#define ALLOC(size) MemoryContextAlloc(TopMemoryContext, size)
#define FREE(size) pfree(size)
#else
#define ALLOC(size) malloc(size)
#define FREE(size) free(size)
#endif

struct IoStreamLayer
{
	IoStreamProcessor *processor;
	void	   *context;
	IoStreamLayer *next;
};

struct IoStream
{
	IoStreamLayer *layer;
};

IoStream *
io_stream_create(void)
{
	IoStream   *ret = ALLOC(sizeof(IoStream));

	ret->layer = NULL;
	return ret;
}

void
io_stream_destroy(IoStream * arg)
{
	IoStreamLayer *layer;

	if (arg == NULL)
		return;

	layer = arg->layer;
	while (layer != NULL)
	{
		IoStreamLayer *next = layer->next;

		if (layer->processor->destroy != NULL)
			layer->processor->destroy(layer->context);
		FREE(layer);
		layer = next;
	}
	FREE(arg);
}

IoStreamLayer *
io_stream_add_layer(IoStream * stream, IoStreamProcessor * processor, void *context)
{
	IoStreamLayer *layer = ALLOC(sizeof(IoStreamLayer));

	layer->processor = processor;
	layer->context = context;
	layer->next = stream->layer;
	stream->layer = layer;
	return layer;
}

ssize_t
io_stream_read(IoStream * stream, void *data, size_t size, bool buffered_only)
{
	if (stream->layer == NULL)
		return -1;

	return stream->layer->processor->read(stream->layer, stream->layer->context, data, size, buffered_only);
}

int
io_stream_write(IoStream * stream, void const *data, size_t size, size_t *bytes_written)
{
	if (stream->layer == NULL)
		return -1;

	return stream->layer->processor->write(stream->layer, stream->layer->context, data, size, bytes_written);
}

bool
io_stream_buffered_read_data(IoStream * stream)
{
	IoStreamLayer *layer;

	for (layer = stream->layer; layer != NULL; layer = layer->next)
	{
		if (layer->processor->buffered_read_data != NULL && layer->processor->buffered_read_data(layer->context))
			return true;
	}
	return false;
}

bool
io_stream_buffered_write_data(IoStream * stream)
{
	IoStreamLayer *layer;

	for (layer = stream->layer; layer != NULL; layer = layer->next)
	{
		if (layer->processor->buffered_write_data != NULL && layer->processor->buffered_write_data(layer->context))
			return true;
	}
	return false;
}

void
io_stream_reset_write_state(IoStream * stream)
{

	IoStreamLayer *layer;

	for (layer = stream->layer; layer != NULL; layer = layer->next)
	{
		if (layer->processor->reset_write_state != NULL)
			layer->processor->reset_write_state(layer->context);
	}
}

ssize_t
io_stream_next_read(IoStreamLayer * layer, void *data, size_t size, bool buffered_only)
{
	if (layer->next == NULL)
		return -1;

	return layer->next->processor->read(layer->next, layer->next->context, data, size, buffered_only);
}

int
io_stream_next_write(IoStreamLayer * layer, void const *data, size_t size, size_t *bytes_written)
{
	if (layer->next == NULL)
		return -1;

	return layer->next->processor->write(layer->next, layer->next->context, data, size, bytes_written);
}
