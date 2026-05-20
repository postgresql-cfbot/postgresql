/*-------------------------------------------------------------------------
 *
 * dynamic_shared_buffers.h
 *	  Dynamic shared buffer (DSB) coordination state and helpers.
 *
 * This header collects the neon-specific machinery that lets shared_buffers
 * grow and shrink at runtime.
 *
 * See pgxn/neon/README.md ("Dynamic shared buffer") for the full design and
 * the resize protocol.
 *
 * src/include/storage/dynamic_shared_buffers.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DYNAMIC_SHARED_BUFFERS_H
#define DYNAMIC_SHARED_BUFFERS_H

#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/spin.h"

/*
 * Minimum allowed value of the shared_buffers GUC, and the smallest size that
 * pg_resize_shared_buffers() can shrink to.
 */
#define MIN_SHARED_BUFFERS	16

/*
 * DynamicSharedBuffersControl is shared between backends and helps to
 * coordinate shared buffer pool resize. See pgxn/neon/README.md
 * ("Dynamic shared buffer") for the full design and protocol.
 */
typedef struct
{
	/*
	 * When a resize is in progress, `resize_in_progress` is set and
	 * `coordinator_pid` is the PID of the backend performing the resize.
	 */
	bool		resize_in_progress;
	pid_t		coordinator_pid;
	slock_t		coordinator_lock;	/* protects the two fields above */

	pg_atomic_uint32 lowNBuffers;	/* low water mark: backends allocate
									 * buffers from [0, lowNBuffers). */
	pg_atomic_uint32 highNBuffers;	/* high water mark: buffer descriptor
									 * memory in [0, highNBuffers) is
									 * allocated and initialized. */
	LWLock		AccessNBuffersLock; /* Backends hold this in shared mode while
									 * iterating the buffer pool up to
									 * highNBuffers; the resize coordinator
									 * acquires it exclusively to mutate the
									 * buffer pool memory and publish a new
									 * highNBuffers. */
} DynamicSharedBuffersControl;

extern PGDLLIMPORT DynamicSharedBuffersControl *DSBCtrl;

extern PGDLLIMPORT int NBuffersGUC;

extern bool IsProcSignalInitialized(void);

/*
 * GetHighNBuffers returns the high water mark.
 */
static inline int
GetHighNBuffers(void)
{
	if (DSBCtrl == NULL)
		return NBuffersGUC;
	return pg_atomic_read_u32(&DSBCtrl->highNBuffers);
}

/*
 * GetLowNBuffers returns the low water mark.
 */
static inline int
GetLowNBuffers(void)
{
	if (DSBCtrl == NULL)
		return NBuffersGUC;
	/*
	 * This must only be called from a process that is subscribed to the
	 * SHBUF_RESIZE ProcSignal barrier (i.e. one that has finished
	 * ProcSignalInit) -- otherwise a concurrent
	 * shrink could free the buffer memory in [new_low, old_low) without
	 * waiting for us.
	 */
	Assert(IsProcSignalInitialized());
	return pg_atomic_read_u32(&DSBCtrl->lowNBuffers);
}

extern void DSBControlInit(void);

/*
 * Try to claim coordinator status for a buffer-pool resize. Returns true if
 * we became the coordinator (caller must eventually call
 * ReleaseResizeCoordinator()), false if a resize was already in progress.
 */
extern bool ClaimResizeCoordinator(void);
extern void ReleaseResizeCoordinator(void);

#endif							/* DYNAMIC_SHARED_BUFFERS_H */
