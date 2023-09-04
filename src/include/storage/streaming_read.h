#ifndef STREAMING_READ_H
#define STREAMING_READ_H

#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/smgr.h"

/*
 * For most sequential access, callers can user this size to build full sized
 * reads without pinning many extra buffers.
 */
#define PG_STREAMING_READ_DEFAULT_MAX_IOS MAX_BUFFERS_PER_TRANSFER

struct PgStreamingRead;
typedef struct PgStreamingRead PgStreamingRead;

typedef bool (*PgStreamingReadBufferDetermineNextCB) (PgStreamingRead *pgsr,
													  uintptr_t pgsr_private,
													  void *per_io_private,
													  BufferManagerRelation *bmr,
													  ForkNumber *forkNum,
													  BlockNumber *blockNum,
													  ReadBufferMode *mode);

extern PgStreamingRead *pg_streaming_read_buffer_alloc(int max_ios,
													   size_t per_io_private_size,
													   uintptr_t pgsr_private,
													   BufferAccessStrategy strategy,
													   PgStreamingReadBufferDetermineNextCB determine_next_cb);
extern void pg_streaming_read_prefetch(PgStreamingRead *pgsr);
extern Buffer pg_streaming_read_buffer_get_next(PgStreamingRead *pgsr, void **per_io_private);
extern void pg_streaming_read_reset(PgStreamingRead *pgsr);
extern void pg_streaming_read_free(PgStreamingRead *pgsr);

extern int pg_streaming_read_ios(PgStreamingRead *pgsr);
extern int pg_streaming_read_pins(PgStreamingRead *pgsr);

#endif
