/*-------------------------------------------------------------------------
 *
 * seqlocalxlog.c
 *	  WAL replay logic for local sequence access manager
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *        src/backend/access/sequence/seqlocalxlog.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/bufmask.h"
#include "access/seqlocalam.h"
#include "access/xlogutils.h"
#include "storage/block.h"

/*
 * Mask a Sequence page before performing consistency checks on it.
 */
void
seq_local_mask(char *page, BlockNumber blkno)
{
	mask_page_lsn_and_checksum(page);

	mask_unused_space(page);
}

void
seq_local_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	Buffer		buffer;
	Page		page;
	Page		localpage;
	char	   *item;
	Size		itemsz;
	xl_seq_local_rec *xlrec = (xl_seq_local_rec *) XLogRecGetData(record);
	seq_local_magic *sm;

	if (info != XLOG_SEQ_LOCAL_LOG)
		elog(PANIC, "seq_redo: unknown op code %u", info);

	buffer = XLogInitBufferForRedo(record, 0);
	page = (Page) BufferGetPage(buffer);

	/*
	 * We always reinit the page.  However, since this WAL record type is also
	 * used for updating sequences, it's possible that a hot-standby backend
	 * is examining the page concurrently; so we mustn't transiently trash the
	 * buffer.  The solution is to build the correct new page contents in
	 * local workspace and then memcpy into the buffer.  Then only bytes that
	 * are supposed to change will change, even transiently. We must palloc
	 * the local page for alignment reasons.
	 */
	localpage = (Page) palloc(BufferGetPageSize(buffer));

	PageInit(localpage, BufferGetPageSize(buffer), sizeof(seq_local_magic));
	sm = (seq_local_magic *) PageGetSpecialPointer(localpage);
	sm->magic = SEQ_LOCAL_MAGIC;

	item = (char *) xlrec + sizeof(xl_seq_local_rec);
	itemsz = XLogRecGetDataLen(record) - sizeof(xl_seq_local_rec);

	if (PageAddItem(localpage, (Item) item, itemsz,
					FirstOffsetNumber, false, false) == InvalidOffsetNumber)
		elog(PANIC, "seq_local_redo: failed to add item to page");

	PageSetLSN(localpage, lsn);

	memcpy(page, localpage, BufferGetPageSize(buffer));
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);

	pfree(localpage);
}
