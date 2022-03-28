/*-------------------------------------------------------------------------
 *
 * directmgr.c
 *	  routines for managing unbuffered IO
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/direct/directmgr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"


#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "access/xloginsert.h"
#include "storage/directmgr.h"

void
unbuffered_prep(UnBufferedWriteState *wstate, bool do_wal, bool
		do_optimization)
{
	/*
	 * There is no valid fsync optimization if no WAL is being written anyway
	 */
	Assert(!do_optimization || (do_optimization && do_wal));

	wstate->do_wal = do_wal;
	wstate->do_optimization = do_optimization;
	wstate->redo = do_optimization ? GetRedoRecPtr() : InvalidXLogRecPtr;
}

void
unbuffered_extend(UnBufferedWriteState *wstate, SMgrRelation
		smgrrel, ForkNumber forknum, BlockNumber blocknum, Page page, bool
		empty)
{
	/*
	 * Don't checksum empty pages
	 */
	if (!empty)
		PageSetChecksumInplace(page, blocknum);

	smgrextend(smgrrel, forknum, blocknum, (char *) page, true);

	/*
	 * Don't WAL-log empty pages
	 */
	if (!empty && wstate->do_wal)
		log_newpage(&(smgrrel)->smgr_rnode.node, forknum,
					blocknum, page, true);
}

void
unbuffered_extend_range(UnBufferedWriteState *wstate, SMgrRelation smgrrel,
		ForkNumber forknum, int num_pages, BlockNumber *blocknums, Page *pages,
		bool empty, XLogRecPtr custom_lsn)
{
	for (int i = 0; i < num_pages; i++)
	{
		Page		page = pages[i];
		BlockNumber blkno = blocknums[i];

		if (!XLogRecPtrIsInvalid(custom_lsn))
			PageSetLSN(page, custom_lsn);

		if (!empty)
			PageSetChecksumInplace(page, blkno);

		smgrextend(smgrrel, forknum, blkno, (char *) page, true);
	}

	if (!empty && wstate->do_wal)
		log_newpages(&(smgrrel)->smgr_rnode.node, forknum, num_pages,
				blocknums, pages, true);
}

void
unbuffered_write(UnBufferedWriteState *wstate, SMgrRelation smgrrel, ForkNumber
		forknum, BlockNumber blocknum, Page page)
{
	PageSetChecksumInplace(page, blocknum);

	smgrwrite(smgrrel, forknum, blocknum, (char *) page, true);
}

/*
 * When writing data outside shared buffers, a concurrent CHECKPOINT can move
 * the redo pointer past our WAL entries and won't flush our data to disk. If
 * the database crashes before the data makes it to disk, our WAL won't be
 * replayed and the data will be lost.
 * Thus, if a CHECKPOINT begins between unbuffered_prep() and
 * unbuffered_finish(), the backend must fsync the data itself.
 */
void
unbuffered_finish(UnBufferedWriteState *wstate, SMgrRelation smgrrel,
		ForkNumber forknum)
{
	if (!wstate->do_wal)
		return;

	if (wstate->do_optimization && !RedoRecPtrChanged(wstate->redo))
		return;

	smgrimmedsync(smgrrel, forknum);
}
