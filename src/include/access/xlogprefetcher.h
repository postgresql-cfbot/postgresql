/*-------------------------------------------------------------------------
 *
 * xlogprefetcher.h
 *		Declarations for the recovery prefetching module.
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/access/xlogprefetcher.h
 *-------------------------------------------------------------------------
 */
#ifndef XLOGPREFETCHER_H
#define XLOGPREFETCHER_H

#include "access/xlogdefs.h"

/* GUCs */
extern bool recovery_prefetch;

struct XLogPrefetcher;
typedef struct XLogPrefetcher XLogPrefetcher;


extern void XLogPrefetchReconfigure(void);

extern size_t XLogPrefetchShmemSize(void);
extern void XLogPrefetchShmemInit(void);

extern void XLogPrefetchRequestResetStats(void);

extern XLogPrefetcher *XLogPrefetcherAllocate(XLogReaderState *reader);
extern void XLogPrefetcherFree(XLogPrefetcher *prefetcher);

extern XLogReaderState *XLogPrefetcherReader(XLogPrefetcher *prefetcher);

extern void XLogPrefetcherBeginRead(XLogPrefetcher *prefetcher,
									XLogRecPtr recPtr);

extern XLogRecord *XLogPrefetcherReadRecord(XLogPrefetcher *prefetcher,
											char **errmsg);

#endif
