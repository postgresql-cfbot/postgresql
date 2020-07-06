/*
 * nv_xlog_buffer.h
 *
 * PostgreSQL non-volatile WAL buffer
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/nv_xlog_buffer.h
 */
#ifndef NV_XLOG_BUFFER_H
#define NV_XLOG_BUFFER_H

extern void *MapNonVolatileXLogBuffer(const char *fname, Size fsize);
extern void	UnmapNonVolatileXLogBuffer(void *addr, Size fsize);

#ifdef USE_NVWAL
#include <libpmem.h>

#define nv_memset_persist	pmem_memset_persist
#define nv_memcpy_nodrain	pmem_memcpy_nodrain
#define nv_flush			pmem_flush
#define nv_drain			pmem_drain
#define nv_persist			pmem_persist

#else
void *
MapNonVolatileXLogBuffer(const char *fname, Size fsize)
{
	return NULL;
}

void
UnmapNonVolatileXLogBuffer(void *addr, Size fsize)
{
	return;
}

static inline void *
nv_memset_persist(void *pmemdest, int c, size_t len)
{
	return NULL;
}

static inline void *
nv_memcpy_nodrain(void *pmemdest, const void *src,
				  size_t len)
{
	return NULL;
}

static inline void
nv_flush(void *pmemdest, size_t len)
{
	return;
}

static inline void
nv_drain(void)
{
	return;
}

static inline void
nv_persist(const void *addr, size_t len)
{
	return;
}

#endif							/* USE_NVWAL */
#endif							/* NV_XLOG_BUFFER_H */
