/*--------------------------------------------------------------------------
 *
 * read_wal_from_buffers.c
 *		Test module to read WAL from WAL buffers.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	src/test/modules/read_wal_from_buffers/read_wal_from_buffers.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlogrecovery.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"

PG_MODULE_MAGIC;

static int	read_from_wal_buffers(XLogReaderState *state, XLogRecPtr targetPagePtr,
								  int reqLen, XLogRecPtr targetRecPtr,
								  char *cur_page);

static XLogRecord *ReadNextXLogRecord(XLogReaderState *xlogreader);
static void GetWALRecordInfo(XLogReaderState *record, Datum *values,
							 bool *nulls, uint32 ncols);
static void GetWALRecordsInfo(FunctionCallInfo fcinfo,
							  XLogRecPtr start_lsn,
							  XLogRecPtr end_lsn);

/*
 * SQL function to read WAL from WAL buffers. Returns number of bytes read.
 */
PG_FUNCTION_INFO_V1(read_wal_from_buffers);
Datum
read_wal_from_buffers(PG_FUNCTION_ARGS)
{
	XLogRecPtr	startptr = PG_GETARG_LSN(0);
	int32		count = PG_GETARG_INT32(1);
	Size		read;
	char	   *data = palloc0(count);
	XLogRecPtr	upto = startptr + count;
	XLogRecPtr	insert_pos = GetXLogInsertRecPtr();
	TimeLineID	tli = GetWALInsertionTimeLine();

	/*
	 * The requested WAL may be very recent, so wait for any in-progress WAL
	 * insertions to WAL buffers to finish.
	 */
	if (upto > insert_pos)
	{
		XLogRecPtr	writtenUpto = WaitXLogInsertionsToFinish(upto);

		upto = Min(upto, writtenUpto);
		count = upto - startptr;
	}

	read = WALReadFromBuffers(data, startptr, count, tli);

	pfree(data);

	PG_RETURN_INT32(read);
}

/*
 * XLogReaderRoutine->page_read callback for reading WAL from WAL buffers.
 */
static int
read_from_wal_buffers(XLogReaderState *state, XLogRecPtr targetPagePtr,
					  int reqLen, XLogRecPtr targetRecPtr,
					  char *cur_page)
{
	XLogRecPtr	read_upto,
				loc;
	TimeLineID	tli = GetWALInsertionTimeLine();
	Size		count;
	Size		read = 0;

	loc = targetPagePtr + reqLen;

	/* Loop waiting for xlog to be available if necessary */
	while (1)
	{
		read_upto = GetXLogInsertRecPtr();

		if (loc <= read_upto)
			break;

		WaitXLogInsertionsToFinish(loc);

		CHECK_FOR_INTERRUPTS();
		pg_usleep(1000L);
	}

	if (targetPagePtr + XLOG_BLCKSZ <= read_upto)
	{
		/*
		 * more than one block available; read only that block, have caller
		 * come back if they need more.
		 */
		count = XLOG_BLCKSZ;
	}
	else if (targetPagePtr + reqLen > read_upto)
	{
		/* not enough data there */
		return -1;
	}
	else
	{
		/* enough bytes available to satisfy the request */
		count = read_upto - targetPagePtr;
	}

	/* read WAL from WAL buffers */
	read = WALReadFromBuffers(cur_page, targetPagePtr, count, tli);

	if (read != count)
		ereport(ERROR,
				errmsg("could not read fully from WAL buffers; expected %lu, read %lu",
					   count, read));

	return count;
}

/*
 * Get info of all WAL records between start LSN and end LSN.
 *
 * This function and its helpers below are similar to pg_walinspect's
 * pg_get_wal_records_info() except that it will get info of WAL records
 * available in WAL buffers.
 */
PG_FUNCTION_INFO_V1(get_wal_records_info_from_buffers);
Datum
get_wal_records_info_from_buffers(PG_FUNCTION_ARGS)
{
	XLogRecPtr	start_lsn = PG_GETARG_LSN(0);
	XLogRecPtr	end_lsn = PG_GETARG_LSN(1);

	/*
	 * Validate start and end LSNs coming from the function inputs.
	 *
	 * Reading WAL below the first page of the first segments isn't allowed.
	 * This is a bootstrap WAL page and the page_read callback fails to read
	 * it.
	 */
	if (start_lsn < XLOG_BLCKSZ)
		ereport(ERROR,
				(errmsg("could not read WAL at LSN %X/%X",
						LSN_FORMAT_ARGS(start_lsn))));

	if (start_lsn > end_lsn)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("WAL start LSN must be less than end LSN")));

	GetWALRecordsInfo(fcinfo, start_lsn, end_lsn);

	PG_RETURN_VOID();
}

/*
 * Read next WAL record.
 */
static XLogRecord *
ReadNextXLogRecord(XLogReaderState *xlogreader)
{
	XLogRecord *record;
	char	   *errormsg;

	record = XLogReadRecord(xlogreader, &errormsg);

	if (record == NULL)
	{
		if (errormsg)
			ereport(ERROR,
					errmsg("could not read WAL at %X/%X: %s",
						   LSN_FORMAT_ARGS(xlogreader->EndRecPtr), errormsg));
		else
			ereport(ERROR,
					errmsg("could not read WAL at %X/%X",
						   LSN_FORMAT_ARGS(xlogreader->EndRecPtr)));
	}

	return record;
}

/*
 * Output values that make up a row describing caller's WAL record.
 */
static void
GetWALRecordInfo(XLogReaderState *record, Datum *values,
				 bool *nulls, uint32 ncols)
{
	const char *record_type;
	RmgrData	desc;
	uint32		fpi_len = 0;
	StringInfoData rec_desc;
	StringInfoData rec_blk_ref;
	int			i = 0;

	desc = GetRmgr(XLogRecGetRmid(record));
	record_type = desc.rm_identify(XLogRecGetInfo(record));

	if (record_type == NULL)
		record_type = psprintf("UNKNOWN (%x)", XLogRecGetInfo(record) & ~XLR_INFO_MASK);

	initStringInfo(&rec_desc);
	desc.rm_desc(&rec_desc, record);

	if (XLogRecHasAnyBlockRefs(record))
	{
		initStringInfo(&rec_blk_ref);
		XLogRecGetBlockRefInfo(record, false, true, &rec_blk_ref, &fpi_len);
	}

	values[i++] = LSNGetDatum(record->ReadRecPtr);
	values[i++] = LSNGetDatum(record->EndRecPtr);
	values[i++] = LSNGetDatum(XLogRecGetPrev(record));
	values[i++] = TransactionIdGetDatum(XLogRecGetXid(record));
	values[i++] = CStringGetTextDatum(desc.rm_name);
	values[i++] = CStringGetTextDatum(record_type);
	values[i++] = UInt32GetDatum(XLogRecGetTotalLen(record));
	values[i++] = UInt32GetDatum(XLogRecGetDataLen(record));
	values[i++] = UInt32GetDatum(fpi_len);

	if (rec_desc.len > 0)
		values[i++] = CStringGetTextDatum(rec_desc.data);
	else
		nulls[i++] = true;

	if (XLogRecHasAnyBlockRefs(record))
		values[i++] = CStringGetTextDatum(rec_blk_ref.data);
	else
		nulls[i++] = true;

	Assert(i == ncols);
}

/*
 * Get info of all WAL records between start LSN and end LSN.
 */
static void
GetWALRecordsInfo(FunctionCallInfo fcinfo, XLogRecPtr start_lsn,
				  XLogRecPtr end_lsn)
{
#define GET_WAL_RECORDS_INFO_FROM_BUFFERS_COLS 11
	XLogReaderState *xlogreader;
	XLogRecPtr	first_valid_record;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext old_cxt;
	MemoryContext tmp_cxt;

	Assert(start_lsn <= end_lsn);

	InitMaterializedSRF(fcinfo, 0);

	xlogreader = XLogReaderAllocate(wal_segment_size, NULL,
									XL_ROUTINE(.page_read = &read_from_wal_buffers,
											   .segment_open = NULL,
											   .segment_close = NULL),
									NULL);

	if (xlogreader == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	/* first find a valid recptr to start from */
	first_valid_record = XLogFindNextRecord(xlogreader, start_lsn);

	if (XLogRecPtrIsInvalid(first_valid_record))
	{
		ereport(LOG,
				(errmsg("could not find a valid record after %X/%X",
						LSN_FORMAT_ARGS(start_lsn))));

		return;
	}

	tmp_cxt = AllocSetContextCreate(CurrentMemoryContext,
									"GetWALRecordsInfo temporary cxt",
									ALLOCSET_DEFAULT_SIZES);

	while (ReadNextXLogRecord(xlogreader) &&
		   xlogreader->EndRecPtr <= end_lsn)
	{
		Datum		values[GET_WAL_RECORDS_INFO_FROM_BUFFERS_COLS] = {0};
		bool		nulls[GET_WAL_RECORDS_INFO_FROM_BUFFERS_COLS] = {0};

		/* Use the tmp context so we can clean up after each tuple is done */
		old_cxt = MemoryContextSwitchTo(tmp_cxt);

		GetWALRecordInfo(xlogreader, values, nulls,
						 GET_WAL_RECORDS_INFO_FROM_BUFFERS_COLS);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);

		/* clean up and switch back */
		MemoryContextSwitchTo(old_cxt);
		MemoryContextReset(tmp_cxt);

		CHECK_FOR_INTERRUPTS();
	}

	MemoryContextDelete(tmp_cxt);
	XLogReaderFree(xlogreader);

#undef GET_WAL_RECORDS_INFO_FROM_BUFFERS_COLS
}
