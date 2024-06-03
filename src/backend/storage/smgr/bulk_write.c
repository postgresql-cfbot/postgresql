/*-------------------------------------------------------------------------
 *
 * bulk_write.c
 *	  Efficiently and reliably populate a new relation
 *
 * The assumption is that no other backends access the relation while we are
 * loading it, so we can take some shortcuts.  Do not mix operations through
 * the regular buffer manager and the bulk loading interface!
 *
 * We bypass the buffer manager to avoid the locking overhead, and call
 * smgrwritev() directly.  A downside is that the pages will need to be
 * re-read into shared buffers on first use after the build finishes.  That's
 * usually a good tradeoff for large relations, and for small relations, the
 * overhead isn't very significant compared to creating the relation in the
 * first place.
 *
 * The pages are WAL-logged if needed.  To save on WAL header overhead, we
 * WAL-log several pages in one record.
 *
 * One tricky point is that because we bypass the buffer manager, we need to
 * register the relation for fsyncing at the next checkpoint ourselves, and
 * make sure that the relation is correctly fsync'd by us or the checkpointer
 * even if a checkpoint happens concurrently.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/bulk_write.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xloginsert.h"
#include "access/xlogrecord.h"
#include "lib/ilist.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/bulk_write.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "utils/rel.h"

#define MAX_PENDING_WRITES XLR_MAX_BLOCK_ID

static const PGIOAlignedBlock zero_buffer = {{0}};	/* worth BLCKSZ */

typedef union BufferSlot
{
	PGIOAlignedBlock buffer;
	dlist_node	freelist_node;
} BufferSlot;

typedef struct PendingWrite
{
	BufferSlot *slot;
	BlockNumber blkno;
	bool		page_std;
} PendingWrite;

/*
 * Bulk writer state for one relation fork.
 */
struct BulkWriteState
{
	/*
	 * This must come first so that it is correctly aligned for I/O.  We have
	 * one extra slot for the write that has been allocated but not yet
	 * submitted to smgr_bulk_write() yet.
	 */
	BufferSlot	buffer_slots[MAX_PENDING_WRITES + 1];
	dlist_head	buffer_slots_freelist;

	/* Information about the target relation we're writing */
	SMgrRelation smgr;
	ForkNumber	forknum;
	bool		use_wal;

	/* We keep several writes queued, and WAL-log them in batches */
	int			npending;
	PendingWrite pending_writes[MAX_PENDING_WRITES];

	/* Current size of the relation */
	BlockNumber pages_written;

	/* The RedoRecPtr at the time that the bulk operation started */
	XLogRecPtr	start_RedoRecPtr;
};

static void smgr_bulk_flush(BulkWriteState *bulkstate);

/*
 * Start a bulk write operation on a relation fork.
 */
BulkWriteState *
smgr_bulk_start_rel(Relation rel, ForkNumber forknum)
{
	return smgr_bulk_start_smgr(RelationGetSmgr(rel),
								forknum,
								RelationNeedsWAL(rel) || forknum == INIT_FORKNUM);
}

/*
 * Start a bulk write operation on a relation fork.
 *
 * This is like smgr_bulk_start_rel, but can be used without a relcache entry.
 */
BulkWriteState *
smgr_bulk_start_smgr(SMgrRelation smgr, ForkNumber forknum, bool use_wal)
{
	BulkWriteState *state;

	state = palloc_aligned(sizeof(BulkWriteState), PG_IO_ALIGN_SIZE, 0);
	state->smgr = smgr;
	state->forknum = forknum;
	state->use_wal = use_wal;

	state->npending = 0;
	state->pages_written = 0;

	state->start_RedoRecPtr = GetRedoRecPtr();

	/* Set up the free-list of buffers. */
	dlist_init(&state->buffer_slots_freelist);
	for (int i = 0; i < lengthof(state->buffer_slots); ++i)
		dlist_push_tail(&state->buffer_slots_freelist,
						&state->buffer_slots[i].freelist_node);

	return state;
}

/*
 * Finish bulk write operation.
 *
 * This WAL-logs and flushes any remaining pending writes to disk, and fsyncs
 * the relation if needed.
 */
void
smgr_bulk_finish(BulkWriteState *bulkstate)
{
	/* WAL-log and flush any remaining pages */
	smgr_bulk_flush(bulkstate);

	/*
	 * When we wrote out the pages, we passed skipFsync=true to avoid the
	 * overhead of registering all the writes with the checkpointer.  Register
	 * the whole relation now.
	 *
	 * There is one hole in that idea: If a checkpoint occurred while we were
	 * writing the pages, it already missed fsyncing the pages we had written
	 * before the checkpoint started.  A crash later on would replay the WAL
	 * starting from the checkpoint, therefore it wouldn't replay our earlier
	 * WAL records.  So if a checkpoint started after the bulk write, fsync
	 * the files now.
	 */
	if (!SmgrIsTemp(bulkstate->smgr))
	{
		/*
		 * Prevent a checkpoint from starting between the GetRedoRecPtr() and
		 * smgrregistersync() calls.
		 */
		Assert((MyProc->delayChkptFlags & DELAY_CHKPT_START) == 0);
		MyProc->delayChkptFlags |= DELAY_CHKPT_START;

		if (bulkstate->start_RedoRecPtr != GetRedoRecPtr())
		{
			/*
			 * A checkpoint occurred and it didn't know about our writes, so
			 * fsync() the relation ourselves.
			 */
			MyProc->delayChkptFlags &= ~DELAY_CHKPT_START;
			smgrimmedsync(bulkstate->smgr, bulkstate->forknum);
			elog(DEBUG1, "flushed relation because a checkpoint occurred concurrently");
		}
		else
		{
			smgrregistersync(bulkstate->smgr, bulkstate->forknum);
			MyProc->delayChkptFlags &= ~DELAY_CHKPT_START;
		}
	}
}

static int
buffer_cmp(const void *a, const void *b)
{
	const PendingWrite *bufa = (const PendingWrite *) a;
	const PendingWrite *bufb = (const PendingWrite *) b;

	/* We should not see duplicated writes for the same block */
	Assert(bufa->blkno != bufb->blkno);
	if (bufa->blkno > bufb->blkno)
		return 1;
	else
		return -1;
}

/*
 * Finish all the pending writes.
 */
static void
smgr_bulk_flush(BulkWriteState *bulkstate)
{
	int			npending = bulkstate->npending;
	PendingWrite *pending_writes = bulkstate->pending_writes;

	if (npending == 0)
		return;

	if (npending > 1)
		qsort(pending_writes, npending, sizeof(PendingWrite), buffer_cmp);

	if (bulkstate->use_wal)
	{
		BlockNumber blknos[MAX_PENDING_WRITES];
		Page		pages[MAX_PENDING_WRITES];
		bool		page_std = true;

		for (int i = 0; i < npending; i++)
		{
			blknos[i] = pending_writes[i].blkno;
			pages[i] = pending_writes[i].slot->buffer.data;

			/*
			 * If any of the pages use !page_std, we log them all as such.
			 * That's a bit wasteful, but in practice, a mix of standard and
			 * non-standard page layout is rare.  None of the built-in AMs do
			 * that.
			 */
			if (!pending_writes[i].page_std)
				page_std = false;
		}
		log_newpages(&bulkstate->smgr->smgr_rlocator.locator, bulkstate->forknum,
					 npending, blknos, pages, page_std);
	}

	for (int i = 0; i < npending; i++)
	{
		Page		page;
		const void *pages[MAX_IO_COMBINE_LIMIT];
		BlockNumber blkno;
		int			nblocks;
		int			max_nblocks;

		/* Prepare to write the first block. */
		blkno = pending_writes[i].blkno;
		page = pending_writes[i].slot->buffer.data;
		PageSetChecksumInplace(page, blkno);
		pages[0] = page;
		nblocks = 1;

		/*
		 * If we have to write pages nonsequentially, fill in the space with
		 * zeroes until we come back and overwrite.  This is not logically
		 * necessary on standard Unix filesystems (unwritten space will read
		 * as zeroes anyway), but it should help to avoid fragmentation.  The
		 * dummy pages aren't WAL-logged though.
		 */
		while (blkno > bulkstate->pages_written)
		{
			/* don't set checksum for all-zero page */
			smgrextend(bulkstate->smgr, bulkstate->forknum,
					   bulkstate->pages_written++,
					   &zero_buffer,
					   true);
		}

		if (blkno < bulkstate->pages_written)
		{
			/*
			 * We're overwriting.  Clamp at the existing size, because we
			 * can't mix writing and extending in a single operation.
			 */
			max_nblocks = Min(io_combine_limit,
							  bulkstate->pages_written - blkno);
		}
		else
		{
			/* We're extending. */
			Assert(blkno == bulkstate->pages_written);
			max_nblocks = io_combine_limit;
		}

		/* Collect as many consecutive blocks as we can. */
		while (i + 1 < npending &&
			   pending_writes[i + 1].blkno == blkno + nblocks &&
			   nblocks < max_nblocks)
		{
			page = pending_writes[++i].slot->buffer.data;
			PageSetChecksumInplace(page, pending_writes[i].blkno);
			pages[nblocks++] = page;
		}

		/* Extend or overwrite. */
		if (blkno == bulkstate->pages_written)
		{
			smgrwritev(bulkstate->smgr, bulkstate->forknum, blkno,
					   pages, nblocks,
					   SMGR_WRITE_SKIP_FSYNC | SMGR_WRITE_EXTEND);
			bulkstate->pages_written += nblocks;
		}
		else
		{
			Assert(blkno + nblocks <= bulkstate->pages_written);
			smgrwritev(bulkstate->smgr, bulkstate->forknum, blkno,
					   pages, nblocks,
					   SMGR_WRITE_SKIP_FSYNC);
		}

		/*
		 * Maintain FIFO ordering in the free list, so that users who write
		 * blocks in sequential order tend to get sequential chunks of buffer
		 * memory, which may be slight more efficient for vectored writes.
		 */
		for (int j = i - nblocks + 1; j <= i; ++j)
			dlist_push_tail(&bulkstate->buffer_slots_freelist,
							&pending_writes[j].slot->freelist_node);
	}

	bulkstate->npending = 0;
}

/*
 * Queue write of 'buf'.
 *
 * NB: this takes ownership of 'buf'!
 *
 * You are only allowed to write a given block once as part of one bulk write
 * operation.
 */
void
smgr_bulk_write(BulkWriteState *bulkstate, BlockNumber blocknum, BulkWriteBuffer buf, bool page_std)
{
	PendingWrite *w;

	w = &bulkstate->pending_writes[bulkstate->npending++];
	w->slot = (BufferSlot *) buf;
	w->blkno = blocknum;
	w->page_std = page_std;

	if (bulkstate->npending == MAX_PENDING_WRITES ||
		bulkstate->npending == io_combine_limit)
		smgr_bulk_flush(bulkstate);
}

/*
 * Allocate a new buffer which can later be written with smgr_bulk_write().
 *
 * There is no function to free the buffer.  When you pass it to
 * smgr_bulk_write(), it takes ownership and frees it when it's no longer
 * needed.
 */
BulkWriteBuffer
smgr_bulk_get_buf(BulkWriteState *bulkstate)
{
	BufferSlot *slot;

	if (dlist_is_empty(&bulkstate->buffer_slots_freelist))
	{
		smgr_bulk_flush(bulkstate);
		if (dlist_is_empty(&bulkstate->buffer_slots_freelist))
			elog(ERROR, "too many bulk write buffers used but not yet written");
	}

	slot = dlist_head_element(BufferSlot, freelist_node, &bulkstate->buffer_slots_freelist);
	dlist_pop_head_node(&bulkstate->buffer_slots_freelist);

	return &slot->buffer;
}
