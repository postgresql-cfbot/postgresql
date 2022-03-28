/*-------------------------------------------------------------------------
 *
 * directmgr.h
 *	  POSTGRES unbuffered IO manager definitions.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/directmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DIRECTMGR_H
#define DIRECTMGR_H

#include "access/xlogdefs.h"
#include "common/relpath.h"
#include "storage/block.h"
#include "storage/bufpage.h"
#include "storage/smgr.h"

typedef struct UnBufferedWriteState
{
	/*
	 * When writing WAL-logged relation data outside of shared buffers, there
	 * is a risk of a concurrent CHECKPOINT moving the redo pointer past the
	 * data's associated WAL entries. To avoid this, callers in this situation
	 * must fsync the pages they have written themselves. This is necessary
	 * only if the relation is WAL-logged or in special cases such as the init
	 * fork of an unlogged index.
	 *
	 * These callers can optionally use the following optimization: attempt to
	 * use the sync request queue and fall back to fsync'ing the pages
	 * themselves if the Redo pointer moves between the start and finish of
	 * their write. In order to do this, they must set do_optimization to true
	 * so that the redo pointer is saved before the write begins.
	 */
	bool do_wal;
	bool do_optimization;
	XLogRecPtr redo;
} UnBufferedWriteState;
/*
 * prototypes for functions in directmgr.c
 */
extern void
unbuffered_prep(UnBufferedWriteState *wstate, bool do_wal, bool do_optimization);
extern void
unbuffered_extend(UnBufferedWriteState *wstate, SMgrRelation smgrrel,
		ForkNumber forknum, BlockNumber blocknum, Page page, bool empty);
extern void
unbuffered_extend_range(UnBufferedWriteState *wstate, SMgrRelation smgrrel,
		ForkNumber forknum, int num_pages, BlockNumber *blocknums, Page *pages,
		bool empty, XLogRecPtr custom_lsn);
extern void
unbuffered_write(UnBufferedWriteState *wstate, SMgrRelation smgrrel, ForkNumber
		forknum, BlockNumber blocknum, Page page);
extern void
unbuffered_finish(UnBufferedWriteState *wstate, SMgrRelation smgrrel,
		ForkNumber forknum);

#endif							/* DIRECTMGR_H */
