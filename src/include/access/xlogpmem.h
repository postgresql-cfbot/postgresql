/*
 * xlogpmem.h
 *
 * Definitions for PMEM-mapped WAL buffers.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/xlogpmem.h
 */
#ifndef XLOGPMEM_H
#define XLOGPMEM_H

#include "postgres.h"

#include "c.h"					/* Size */
#include "access/xlogdefs.h"	/* XLogRecPtr, XLogSegNo */

#ifdef USE_LIBPMEM

/* Prototypes */
extern XLogSegNo PmemXLogEnsurePrevMapped(XLogRecPtr ptr, TimeLineID tli);
extern char *PmemXLogGetBufferPages(void);
extern void PmemXLogFlush(XLogRecPtr start, XLogRecPtr end);
extern void PmemXLogSync(void);

#else /* USE_LIBPMEM */

#include <stdlib.h> /* abort */

static inline XLogSegNo
PmemXLogEnsurePrevMapped(XLogRecPtr ptr, TimeLineID tli)
{
	abort();
	return 0;
}

static inline char *
PmemXLogGetBufferPages(void)
{
	abort();
	return NULL;
}

static inline void
PmemXLogFlush(XLogRecPtr start, XLogRecPtr end)
{
	abort();
}

static inline void
PmemXLogSync(void)
{
	abort();
}

#endif /* USE_LIBPMEM */

#endif							/* XLOGPMEM_H */
