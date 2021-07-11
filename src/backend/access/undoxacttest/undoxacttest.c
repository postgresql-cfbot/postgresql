#include "postgres.h"

#include "access/undorecordset.h"
#include "access/undoxacttest.h"
#include "access/xactundo.h"
#include "access/xlogutils.h"
#include "catalog/pg_control.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/rel.h"

#define UNDOXACT_TEST	0

int64
undoxacttest_log_execute_mod(Relation rel, Buffer buf, int64 *counter, int64 mod,
							 bool is_undo)
{
	XactUndoContext undo_context;
	xu_undoxactest_mod undo_rec;
	XLogRecPtr	recptr;
	int64		oldval;
	int64		newval;

	/* build undo record */
	if (!is_undo)
	{
		UndoRecData rdata;

		/* AFIXME: API needs to be changed so serialization happens at a later */
		/* stage. */
		undo_rec.reloid = RelationGetRelid(rel);
		undo_rec.mod = mod;

		rdata.len = sizeof(undo_rec);
		rdata.data = (char *) &undo_rec;
		rdata.next = NULL;

		PrepareXactUndoData(&undo_context,
							rel->rd_rel->relpersistence,
							GetUndoDataSize(&rdata));
		SerializeUndoData(&undo_context.data, RM_UNDOXACTTEST_ID,
						  UNDOXACT_TEST, &rdata);
	}

	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	START_CRIT_SECTION();

	/* perform the modification */
	oldval = *counter;
	*counter += mod;
	newval = *counter;

	MarkBufferDirty(buf);

	if (RelationNeedsWAL(rel) || !is_undo)
		XLogBeginInsert();

	if (RelationNeedsWAL(rel))
		XLogRegisterBuffer(0, buf, REGBUF_STANDARD | REGBUF_KEEP_DATA);

	if (!is_undo)
		InsertXactUndoData(&undo_context, 1);

	if (RelationNeedsWAL(rel))
	{
		xl_undoxacttest_mod xlrec = {.newval = newval,
			.debug_mod = mod,
			.debug_oldval = oldval,
		.reloid = RelationGetRelid(rel)};
		uint8		info = XLOG_UNDOXACTTEST_MOD;

		XLogRegisterData((char *) &xlrec, sizeof(xlrec));

		recptr = XLogInsert(RM_UNDOXACTTEST_ID, info);

		if (!is_undo)
			SetXactUndoPageLSNs(&undo_context, recptr);
	}
	else if (!is_undo)
	{
		char		data[1];

		/*
		 * Insert a dummy record to which we can attach the undo-record-set
		 * metadata (e.g. adjustment of the page insertion point).
		 */
		XLogRegisterData(data, 1);
		recptr = XLogInsert(RM_XLOG_ID, XLOG_NOOP);
		SetXactUndoPageLSNs(&undo_context, recptr);
	}

	END_CRIT_SECTION();

	if (!is_undo)
		CleanupXactUndoInsertion(&undo_context);

	return oldval;
}

static void
undoxacttest_redo_mod(XLogReaderState *record)
{
	Buffer		buf;
	xl_undoxacttest_mod *xlrec = (xl_undoxacttest_mod *) XLogRecGetData(record);

	if (XLogReadBufferForRedo(record, 0, &buf) == BLK_NEEDS_REDO)
	{
		Page		page;
		XLogRecPtr	lsn = record->EndRecPtr;
		ItemId		lp = NULL;
		HeapTupleHeader htup;
		char	   *tupdata;
		bytea	   *data;
		int64	   *pagevalue;

		page = BufferGetPage(buf);

		lp = PageGetItemId(page, 1);
		if (PageGetMaxOffsetNumber(page) != 1 || !ItemIdIsNormal(lp))
			elog(PANIC, "invalid lp");

		htup = (HeapTupleHeader) PageGetItem(page, lp);

		tupdata = (char *) htup + htup->t_hoff;

		if (VARSIZE_ANY_EXHDR(tupdata) != 100)
			elog(PANIC, "unexpected size");

		data = (bytea *) VARDATA_ANY(tupdata);
		pagevalue = ((int64 *) &data[0]);

		elog(LOG, "current page value is: " INT64_FORMAT
			 ", w/ debug_oldval: " INT64_FORMAT
			 ", setting to: " INT64_FORMAT
			 ", for modification: " INT64_FORMAT,
			 *pagevalue, xlrec->debug_oldval,
			 xlrec->newval, xlrec->debug_mod);

		*pagevalue = xlrec->newval;

		PageSetLSN(page, lsn);
		MarkBufferDirty(buf);
	}

	if (BufferIsValid(buf))
		UnlockReleaseBuffer(buf);

	{
		xu_undoxactest_mod undo_rec;

		/* reconstruct undo record */
		undo_rec.reloid = xlrec->reloid;
		undo_rec.mod = xlrec->debug_mod;

		XactUndoReplay(record, RM_UNDOXACTTEST_ID, UNDOXACT_TEST, &undo_rec,
					   sizeof(undo_rec));
	}
}

void
undoxacttest_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_UNDOXACTTEST_MOD:
			undoxacttest_redo_mod(record);
			break;
		default:
			elog(PANIC, "undoxacttest_redo: unknown op code %u", info);
	}
}

void
undoxacttest_undo(const WrittenUndoNode *record)
{
	const xu_undoxactest_mod *uxt_r;

	/*
	 * The end of transaction or subtransaction is not interesting for us.
	 * Should we process the records in batches?
	 */
	if (record == NULL)
		return;

	uxt_r = (const xu_undoxactest_mod *) record->n.data;

	elog(DEBUG1, "called for record of type %d, length %zu at %lu: %ld",
		 record->n.type, record->n.length, record->location,
		 uxt_r->mod);

	undoxacttest_undo_mod(uxt_r);
}
