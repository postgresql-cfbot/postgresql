/*--------------------------------------------------------------------------
 *
 * test_undorecord.c
 *		Throw-away test code for undo records.
 *
 * Copyright (c) 2013-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_undorecord.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/undoinsert.h"
#include "access/undolog.h"
#include "access/undorecord.h"
#include "access/xlog_internal.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "storage/bufmgr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_undo_insert);
PG_FUNCTION_INFO_V1(dump_undo_records);

Datum
test_undo_insert(PG_FUNCTION_ARGS)
{
	UnpackedUndoRecord	uur;
	char	pages[2 * BLCKSZ];
	Page	p1 = (Page) &pages;
	Page	p2 = (Page) &pages[BLCKSZ];
	int		aw = 0;

	memset(&uur, 0, sizeof(UnpackedUndoRecord));
	uur.uur_type = 0xaa;
	uur.uur_prevlen = 0xbbbb;
	uur.uur_relfilenode = 0xdeadbeef;
	uur.uur_tsid = PG_GETARG_INT32(0);
	uur.uur_fork = PG_GETARG_INT32(1);
	uur.uur_blkprev = 0x0123456789abcdef;
	uur.uur_block = PG_GETARG_INT32(2);
	uur.uur_offset = 0xcc;

	if (!PG_ARGISNULL(3))
	{
		bytea *varlena = PG_GETARG_BYTEA_PP(3);
		uur.uur_payload.len = VARSIZE_ANY_EXHDR(varlena);
		uur.uur_payload.data = VARDATA_ANY(varlena);
	}

	if (!PG_ARGISNULL(4))
	{
		bytea *varlena = PG_GETARG_BYTEA_PP(4);
		uur.uur_tuple.len = VARSIZE_ANY_EXHDR(varlena);
		uur.uur_tuple.data = VARDATA_ANY(varlena);
	}

	memset(pages, 0, 2 * BLCKSZ);
	if (!InsertUndoRecord(&uur, p1, PG_GETARG_INT32(5), &aw, false) &&
		!InsertUndoRecord(&uur, p2, MAXALIGN(sizeof(PageHeaderData)), &aw, false))
		elog(NOTICE, "couldn't fit in 2 pages?!");

	PG_RETURN_BYTEA_P(cstring_to_text_with_len(pages, sizeof pages));
}

Datum
dump_undo_records(PG_FUNCTION_ARGS)
{
	int logno = PG_GETARG_INT32(0);
	UndoRecPtr record_ptr;
	UndoRecPtr oldest_record_ptr;
	UndoLogControl *log;
	bool empty;

	log = UndoLogGet(logno, false);

	/* Prevent any data from being discarded while we look at it. */
	LWLockAcquire(&log->discard_lock, LW_SHARED);

	/*
	 * We need a consistent snapshot of the range of records and the length
	 * of the final record.
	 */
	LWLockAcquire(&log->mutex, LW_SHARED);
	empty = log->meta.insert == log->meta.discard;
	record_ptr = MakeUndoRecPtr(log->logno,
								log->meta.insert - log->meta.prevlen);
	oldest_record_ptr = MakeUndoRecPtr(log->logno, log->meta.discard);
	LWLockRelease(&log->mutex);

	/* Now walk back record-by-record dumping description data. */
	if (!empty)
	{
		UnpackedUndoRecord *record = palloc0(sizeof(UnpackedUndoRecord));
		RelFileNode rnode;
		StringInfoData buffer;

		UndoRecPtrAssignRelFileNode(rnode, record_ptr);
		initStringInfo(&buffer);
		while (record_ptr >= oldest_record_ptr)
		{
			resetStringInfo(&buffer);
			record = UndoGetOneRecord(record, record_ptr, rnode,
									  log->meta.persistence);
			if (RmgrTable[record->uur_rmid].rm_undo_desc)
				RmgrTable[record->uur_rmid].rm_undo_desc(&buffer, record);
			else
				appendStringInfoString(&buffer, "<no undo desc function>");
			elog(NOTICE, UndoRecPtrFormat ": %s: %s",
				 record_ptr,
				 RmgrTable[record->uur_rmid].rm_name,
				 buffer.data);
			if (BufferIsValid(record->uur_buffer))
			{
				ReleaseBuffer(record->uur_buffer);
				record->uur_buffer = InvalidBuffer;
			}
			if (record->uur_prevlen == 0)
				break;
			record_ptr -= record->uur_prevlen;
		}
		pfree(buffer.data);
	}

	LWLockRelease(&log->discard_lock);

	PG_RETURN_VOID();
}
