#ifndef STREAMING_READ_H
#define STREAMING_READ_H

#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/smgr.h"

/* Default tuning, reasonable for many users. */
#define PGSR_FLAG_DEFAULT 0x00

/*
 * I/O streams that are performing maintenance work on behalf of potentially
 * many users.
 */
#define PGSR_FLAG_MAINTENANCE 0x01

/*
 * We usually avoid issuing prefetch advice automatically when sequential
 * access is detected, but this flag explicitly disables it, for cases that
 * might not be correctly detected.  Explicit advice is known to perform worse
 * than letting the kernel (at least Linux) detect sequential access.
 */
#define PGSR_FLAG_SEQUENTIAL 0x02

/*
 * We usually ramp up from smaller reads to larger ones, to support users who
 * don't know if it's worth reading lots of buffers yet.  This flag disables
 * that, declaring ahead of time that we'll be reading all available buffers.
 */
#define PGSR_FLAG_FULL 0x04

struct PgStreamingRead;
typedef struct PgStreamingRead PgStreamingRead;

/* Callback that returns the next block number to read. */
typedef BlockNumber (*PgStreamingReadBufferCB) (PgStreamingRead *pgsr,
												void *pgsr_private,
												void *per_buffer_private);

extern PgStreamingRead *pg_streaming_read_buffer_alloc(int flags,
													   void *pgsr_private,
													   size_t per_buffer_private_size,
													   BufferAccessStrategy strategy,
													   BufferManagerRelation bmr,
													   ForkNumber forknum,
													   PgStreamingReadBufferCB next_block_cb);

extern void pg_streaming_read_prefetch(PgStreamingRead *pgsr);
extern Buffer pg_streaming_read_buffer_get_next(PgStreamingRead *pgsr, void **per_buffer_private);
extern void pg_streaming_read_free(PgStreamingRead *pgsr);

#endif
