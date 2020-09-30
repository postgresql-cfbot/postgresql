/*-------------------------------------------------------------------------
 *
 * xlogprefetch.h
 *		Declarations for the recovery prefetching module.
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/access/xlogprefetch.h
 *-------------------------------------------------------------------------
 */
#ifndef XLOGPREFETCH_H
#define XLOGPREFETCH_H

#include "access/xlogdefs.h"

/* GUCs */
extern bool recovery_prefetch;
extern bool recovery_prefetch_fpw;

struct XLogPrefetcher;
typedef struct XLogPrefetcher XLogPrefetcher;

extern int XLogPrefetchReconfigureCount;

typedef struct XLogPrefetchState
{
	XLogReaderState *reader;
	XLogPrefetcher *prefetcher;
	int			reconfigure_count;
} XLogPrefetchState;

extern size_t XLogPrefetchShmemSize(void);
extern void XLogPrefetchShmemInit(void);

extern void XLogPrefetchReconfigure(void);
extern void XLogPrefetchRequestResetStats(void);

extern void XLogPrefetchBegin(XLogPrefetchState *state, XLogReaderState *reader);
extern void XLogPrefetchEnd(XLogPrefetchState *state);

/* Functions exposed only for the use of XLogPrefetch(). */
extern XLogPrefetcher *XLogPrefetcherAllocate(XLogReaderState *reader);
extern void XLogPrefetcherFree(XLogPrefetcher *prefetcher);
extern void XLogPrefetcherReadAhead(XLogPrefetcher *prefetch,
									XLogRecPtr replaying_lsn);

/*
 * Tell the prefetching module that we are now replaying a given LSN, so that
 * it can decide how far ahead to read in the WAL, if configured.
 */
static inline void
XLogPrefetch(XLogPrefetchState *state, XLogRecPtr replaying_lsn)
{
	/*
	 * Handle any configuration changes.  Rather than trying to deal with
	 * various parameter changes, we just tear down and set up a new
	 * prefetcher if anything we depend on changes.
	 */
	if (unlikely(state->reconfigure_count != XLogPrefetchReconfigureCount))
	{
		/* If we had a prefetcher, tear it down. */
		if (state->prefetcher)
		{
			XLogPrefetcherFree(state->prefetcher);
			state->prefetcher = NULL;
		}
		/* If we want a prefetcher, set it up. */
		if (recovery_prefetch > 0)
			state->prefetcher = XLogPrefetcherAllocate(state->reader);
		state->reconfigure_count = XLogPrefetchReconfigureCount;
	}

	if (state->prefetcher)
		XLogPrefetcherReadAhead(state->prefetcher, replaying_lsn);
}

#endif
