/*-------------------------------------------------------------------------
 *
 * wal_allocator.h
 *	  Exports from postmaster/wal_allocator.c.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * src/include/postmaster/wal_allocator.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _WAL_ALLOCATOR_H
#define _WAL_ALLOCATOR_H

/* GUC options */
extern int  wal_alloc_max_size_mb;

extern void WalAllocatorMain(void) pg_attribute_noreturn();
extern bool WalPreallocationEnabled(void);

#endif							/* _WAL_ALLOCATOR_H */
