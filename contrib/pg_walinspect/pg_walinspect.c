/*-------------------------------------------------------------------------
 *
 * pg_walinspect.c
 *		  Functions to inspect contents of PostgreSQL Write-Ahead Log
 *
 * Portions Copyright (c) 2012-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/pg_walinspect/pg_walinspect.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"

PG_MODULE_MAGIC;

typedef struct Stats
{
	uint64		count;
	uint64		rec_len;
	uint64		fpi_len;
} Stats;

#define MAX_XLINFO_TYPES 16

typedef struct XLogRecStats
{
	uint64		count;
	Stats		rmgr_stats[RM_NEXT_ID];
	Stats		record_stats[RM_NEXT_ID][MAX_XLINFO_TYPES];
}			XLogRecStats;

extern void _PG_init(void);
extern void _PG_fini(void);

PG_FUNCTION_INFO_V1(pg_get_raw_wal_record);
PG_FUNCTION_INFO_V1(pg_get_first_valid_wal_record_lsn);
PG_FUNCTION_INFO_V1(pg_verify_raw_wal_record);
PG_FUNCTION_INFO_V1(pg_get_wal_record_info);
PG_FUNCTION_INFO_V1(pg_get_wal_record_info_2);
PG_FUNCTION_INFO_V1(pg_get_wal_stats);

static XLogReaderState *InitXLogReaderState(XLogRecPtr lsn,
											XLogRecPtr *first_record,
											bool warning);
static XLogRecord *ReadNextXLogRecord(XLogReaderState *xlogreader,
									  XLogRecPtr first_record);
static void GetXLogRecordInfo(XLogReaderState *record, XLogRecPtr lsn,
							  Datum *values, bool *nulls);
static void StoreXLogRecordStats(XLogRecStats * stats, XLogReaderState *record);
static void GetXLogSummaryStats(XLogRecStats * stats, Tuplestorestate *tupstore,
								TupleDesc tupdesc, Datum *values, bool *nulls);
static void FillXLogStatsRow(const char *name, uint64 n, uint64 total_count,
							 uint64 rec_len, uint64 total_rec_len, uint64 fpi_len,
							 uint64 total_fpi_len, uint64 tot_len, uint64 total_len,
							 Datum *values, bool *nulls);
static void GetWalStatsInternal(FunctionCallInfo fcinfo, XLogRecPtr start_lsn,
								XLogRecPtr end_lsn);

/*
 * Module load callback.
 */
void
_PG_init(void)
{
	/* Define custom GUCs and install hooks here, if any. */

	/*
	 * Have EmitWarningsOnPlaceholders("pg_walinspect"); if custom GUCs are
	 * defined.
	 */

}

/*
 * Module unload callback.
 */
void
_PG_fini(void)
{
	/* Uninstall hooks, if any. */
}

/*
 * Intialize WAL reader and identify the first valid LSN.
 */
static XLogReaderState *
InitXLogReaderState(XLogRecPtr lsn, XLogRecPtr *first_record, bool warning)
{
	XLogReaderState *xlogreader;

	/*
	 * Reading WAL below the first page of the first sgements isn't allowed.
	 * This is a bootstrap WAL page and the page_read callback fails to read
	 * it.
	 */
	if (lsn < XLOG_BLCKSZ)
		ereport(ERROR,
				(errmsg("could not read WAL at %X/%X",
						LSN_FORMAT_ARGS(lsn))));

	xlogreader = XLogReaderAllocate(wal_segment_size, NULL,
									XL_ROUTINE(.page_read = &read_local_xlog_page,
											   .segment_open = &wal_segment_open,
											   .segment_close = &wal_segment_close),
									NULL);

	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	/* First find a valid recptr to start from. */
	*first_record = XLogFindNextRecord(xlogreader, lsn);

	if (XLogRecPtrIsInvalid(*first_record))
		ereport(ERROR,
				(errmsg("could not find a valid record after %X/%X",
						LSN_FORMAT_ARGS(lsn))));

	/*
	 * Display a message that we're skipping data if the given lsn wasn't a
	 * pointer to the start of a record and also wasn't a pointer to the
	 * beginning of a segment (e.g. we were used in file mode).
	 */
	if (warning && *first_record != lsn &&
		XLogSegmentOffset(lsn, wal_segment_size) != 0)
		ereport(WARNING,
				(errmsg_plural("first record is after %X/%X, at %X/%X, skipping over %u byte",
							   "first record is after %X/%X, at %X/%X, skipping over %u bytes",
							   (*first_record - lsn),
							   LSN_FORMAT_ARGS(lsn),
							   LSN_FORMAT_ARGS(*first_record),
							   (uint32) (*first_record - lsn))));

	return xlogreader;
}

/*
 * Read the WAL next record
 */
static XLogRecord *
ReadNextXLogRecord(XLogReaderState *xlogreader, XLogRecPtr first_record)
{
	char	   *errormsg;
	XLogRecord *record;

	record = XLogReadRecord(xlogreader, &errormsg);

	if (!record)
	{
		if (errormsg)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read WAL at %X/%X: %s",
							LSN_FORMAT_ARGS(first_record), errormsg)));
		else
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read WAL at %X/%X",
							LSN_FORMAT_ARGS(first_record))));
	}

	return record;
}

/*
 * Get the raw WAL record. This function will wait for the required WAL at LSN.
 */
Datum
pg_get_raw_wal_record(PG_FUNCTION_ARGS)
{
#define PG_GET_WAL_RECORD_COLS 2
	XLogRecPtr	lsn;
	XLogRecord *record;
	XLogRecPtr	first_record;
	XLogReaderState *xlogreader;
	bytea	   *raw_record;
	uint32		rec_len;
	char	   *raw_record_data;
	TupleDesc	tupdesc;
	Datum		result;
	HeapTuple	tuple;
	Datum		values[PG_GET_WAL_RECORD_COLS];
	bool		nulls[PG_GET_WAL_RECORD_COLS];

	lsn = PG_GETARG_LSN(0);

	if (XLogRecPtrIsInvalid(lsn))
		PG_RETURN_NULL();

	/* Build a tuple descriptor for our result type. */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	xlogreader = InitXLogReaderState(lsn, &first_record, true);

	record = ReadNextXLogRecord(xlogreader, first_record);

	Assert(record);

	rec_len = XLogRecGetTotalLen(xlogreader);

	Assert(rec_len > 0);

	raw_record = (bytea *) palloc(rec_len + VARHDRSZ);
	SET_VARSIZE(raw_record, rec_len + VARHDRSZ);
	raw_record_data = VARDATA(raw_record);

	memcpy(raw_record_data, record, rec_len);

	/*
	 * XXX: Check the authenticity of the WAL record here? Although it might
	 * have been done by XLogReadRecord, after memcpy, just check for
	 * raw_record_data correctness.
	 */

	XLogReaderFree(xlogreader);

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, 0, sizeof(nulls));

	values[0] = LSNGetDatum(first_record);
	values[1] = PointerGetDatum(raw_record);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
#undef PG_GET_WAL_RECORD_COLS
}

/*
 * Get the raw WAL record. This function will wait for the required WAL at LSN.
 */
Datum
pg_get_first_valid_wal_record_lsn(PG_FUNCTION_ARGS)
{
	XLogRecPtr	lsn;
	XLogRecPtr	first_record;
	XLogReaderState *xlogreader;

	lsn = PG_GETARG_LSN(0);

	if (XLogRecPtrIsInvalid(lsn))
		PG_RETURN_NULL();

	xlogreader = InitXLogReaderState(lsn, &first_record, true);

	XLogReaderFree(xlogreader);

	PG_RETURN_LSN(first_record);
}

/*
 * Verify the authenticity of the given raw WAL record.
 */
Datum
pg_verify_raw_wal_record(PG_FUNCTION_ARGS)
{
	bytea	   *raw_record;
	XLogRecord *record;
	bool		valid;

	raw_record = PG_GETARG_BYTEA_PP(0);
	record = (XLogRecord *) VARDATA_ANY(raw_record);
	valid = IsXLogRecordValid(record);

	PG_RETURN_BOOL(valid);
}

/*
 * Calculate the size of a record, split into !FPI and FPI parts.
 */
static void
GetXLogRecordLen(XLogReaderState *record, uint32 *rec_len, uint32 *fpi_len)
{
	int			block_id;

	/*
	 * Calculate the amount of FPI data in the record.
	 *
	 * XXX: We peek into xlogreader's private decoded backup blocks for the
	 * bimg_len indicating the length of FPI data. It doesn't seem worth it to
	 * add an accessor macro for this.
	 */
	*fpi_len = 0;
	for (block_id = 0; block_id <= record->max_block_id; block_id++)
	{
		if (XLogRecHasBlockImage(record, block_id))
			*fpi_len += record->blocks[block_id].bimg_len;
	}

	/*
	 * Calculate the length of the record as the total length - the length of
	 * all the block images.
	 */
	*rec_len = XLogRecGetTotalLen(record) - *fpi_len;
}

/*
 * Get the WAL record info.
 */
static void
GetXLogRecordInfo(XLogReaderState *record, XLogRecPtr lsn,
				  Datum *values, bool *nulls)
{
	const char *id;
	const RmgrData *desc;
	uint32		rec_len;
	uint32		fpi_len;
	RelFileNode rnode;
	ForkNumber	forknum;
	BlockNumber blk;
	int			block_id;
	StringInfoData rec_desc;
	StringInfoData rec_blk_ref;
	StringInfoData temp;
	bytea	   *data;
	char	   *main_data;
	uint32		main_data_len;

	desc = &RmgrTable[XLogRecGetRmid(record)];

	GetXLogRecordLen(record, &rec_len, &fpi_len);

	values[0] = LSNGetDatum(lsn);
	values[1] = LSNGetDatum(XLogRecGetPrev(record));
	values[2] = TransactionIdGetDatum(XLogRecGetXid(record));
	values[3] = CStringGetTextDatum(desc->rm_name);
	values[4] = UInt32GetDatum(rec_len);
	values[5] = UInt32GetDatum(XLogRecGetTotalLen(record));

	initStringInfo(&rec_desc);

	id = desc->rm_identify(XLogRecGetInfo(record));

	if (id == NULL)
		appendStringInfo(&rec_desc, "desc: UNKNOWN (%x) ", XLogRecGetInfo(record) & ~XLR_INFO_MASK);
	else
		appendStringInfo(&rec_desc, "desc: %s ", id);

	initStringInfo(&temp);

	desc->rm_desc(&temp, record);
	appendStringInfo(&rec_desc, "%s", temp.data);

	values[6] = CStringGetTextDatum(rec_desc.data);

	pfree(temp.data);

	initStringInfo(&rec_blk_ref);

	/* Block references (detailed format). */
	for (block_id = 0; block_id <= record->max_block_id; block_id++)
	{
		if (!XLogRecHasBlockRef(record, block_id))
			continue;

		XLogRecGetBlockTag(record, block_id, &rnode, &forknum, &blk);
		appendStringInfo(&rec_blk_ref, "blkref #%u: rel %u/%u/%u fork %s blk %u",
						 block_id, rnode.spcNode, rnode.dbNode,
						 rnode.relNode, get_forkname(forknum), blk);

		if (XLogRecHasBlockImage(record, block_id))
		{
			uint8		bimg_info = record->blocks[block_id].bimg_info;

			if (BKPIMAGE_COMPRESSED(bimg_info))
			{
				const char *method;

				if ((bimg_info & BKPIMAGE_COMPRESS_PGLZ) != 0)
					method = "pglz";
				else if ((bimg_info & BKPIMAGE_COMPRESS_LZ4) != 0)
					method = "lz4";
				else
					method = "unknown";

				appendStringInfo(&rec_blk_ref, " (FPW%s); hole: offset: %u, length: %u, "
								 "compression saved: %u, method: %s",
								 XLogRecBlockImageApply(record, block_id) ?
								 "" : " for WAL verification",
								 record->blocks[block_id].hole_offset,
								 record->blocks[block_id].hole_length,
								 BLCKSZ -
								 record->blocks[block_id].hole_length -
								 record->blocks[block_id].bimg_len,
								 method);
			}
			else
			{
				appendStringInfo(&rec_blk_ref, " (FPW%s); hole: offset: %u, length: %u",
								 XLogRecBlockImageApply(record, block_id) ?
								 "" : " for WAL verification",
								 record->blocks[block_id].hole_offset,
								 record->blocks[block_id].hole_length);
			}
		}
	}

	values[7] = CStringGetTextDatum(rec_blk_ref.data);

	main_data_len = XLogRecGetDataLen(record);

	data = (bytea *) palloc(main_data_len + VARHDRSZ);
	SET_VARSIZE(data, main_data_len + VARHDRSZ);
	main_data = VARDATA(data);

	memcpy(main_data, XLogRecGetData(record), main_data_len);

	values[8] = PointerGetDatum(data);
	values[9] = UInt32GetDatum(main_data_len);
}

/*
 * Get the WAL record info and data.
 */
Datum
pg_get_wal_record_info(PG_FUNCTION_ARGS)
{
#define PG_GET_WAL_RECORD_INFO_COLS 10
	XLogRecPtr	lsn;
	XLogRecPtr	first_record;
	XLogReaderState *xlogreader;
	TupleDesc	tupdesc;
	Datum		result;
	HeapTuple	tuple;
	Datum		values[PG_GET_WAL_RECORD_INFO_COLS];
	bool		nulls[PG_GET_WAL_RECORD_INFO_COLS];

	lsn = PG_GETARG_LSN(0);

	if (XLogRecPtrIsInvalid(lsn))
		PG_RETURN_NULL();

	/* Build a tuple descriptor for our result type. */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	xlogreader = InitXLogReaderState(lsn, &first_record, true);

	(void) ReadNextXLogRecord(xlogreader, first_record);

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, 0, sizeof(nulls));

	GetXLogRecordInfo(xlogreader, first_record, values, nulls);

	XLogReaderFree(xlogreader);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
#undef PG_GET_WAL_RECORD_INFO_COLS
}

/*
 * Get the WAL record info and data between start LSN and end LSN.
 */
Datum
pg_get_wal_record_info_2(PG_FUNCTION_ARGS)
{
#define PG_GET_WAL_RECORD_INFO_COLS 10
	XLogRecPtr	start_lsn;
	XLogRecPtr	end_lsn;
	XLogRecPtr	first_record;
	XLogReaderState *xlogreader;
	ReturnSetInfo *rsinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	Datum		values[PG_GET_WAL_RECORD_INFO_COLS];
	bool		nulls[PG_GET_WAL_RECORD_INFO_COLS];

	start_lsn = PG_GETARG_LSN(0);
	end_lsn = PG_GETARG_LSN(1);

	if (XLogRecPtrIsInvalid(start_lsn) || XLogRecPtrIsInvalid(end_lsn))
		PG_RETURN_NULL();

	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	/* Check to see if caller supports us returning a tuplestore. */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type. */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Build tuplestore to hold the result rows. */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	xlogreader = InitXLogReaderState(start_lsn, &first_record, true);

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, 0, sizeof(nulls));

	for (;;)
	{
		(void) ReadNextXLogRecord(xlogreader, first_record);

		if (xlogreader->currRecPtr >= end_lsn)
			break;

		GetXLogRecordInfo(xlogreader, xlogreader->currRecPtr, values, nulls);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	XLogReaderFree(xlogreader);

	/* Clean up and return the tuplestore. */
	tuplestore_donestoring(tupstore);

	PG_RETURN_VOID();
#undef PG_GET_WAL_RECORD_INFO_COLS
}

/*
 * Store per-rmgr and per-record statistics for a given record.
 */
static void
StoreXLogRecordStats(XLogRecStats * stats, XLogReaderState *record)
{
	RmgrId		rmid;
	uint8		recid;
	uint32		rec_len;
	uint32		fpi_len;

	stats->count++;

	rmid = XLogRecGetRmid(record);

	GetXLogRecordLen(record, &rec_len, &fpi_len);

	/* Update per-rmgr statistics. */
	stats->rmgr_stats[rmid].count++;
	stats->rmgr_stats[rmid].rec_len += rec_len;
	stats->rmgr_stats[rmid].fpi_len += fpi_len;

	/*
	 * Update per-record statistics, where the record is identified by a
	 * combination of the RmgrId and the four bits of the xl_info field that
	 * are the rmgr's domain (resulting in sixteen possible entries per
	 * RmgrId).
	 */
	recid = XLogRecGetInfo(record) >> 4;

	/*
	 * XACT records need to be handled differently. Those records use the
	 * first bit of those four bits for an optional flag variable and the
	 * following three bits for the opcode. We filter opcode out of xl_info
	 * and use it as the identifier of the record.
	 */
	if (rmid == RM_XACT_ID)
		recid &= 0x07;

	stats->record_stats[rmid][recid].count++;
	stats->record_stats[rmid][recid].rec_len += rec_len;
	stats->record_stats[rmid][recid].fpi_len += fpi_len;
}

/*
 * Display a single row of record counts and sizes for an rmgr or record.
 */
static void
FillXLogStatsRow(const char *name,
				 uint64 n, uint64 total_count,
				 uint64 rec_len, uint64 total_rec_len,
				 uint64 fpi_len, uint64 total_fpi_len,
				 uint64 tot_len, uint64 total_len,
				 Datum *values, bool *nulls)
{
	double		n_pct;
	double		rec_len_pct;
	double		fpi_len_pct;
	double		tot_len_pct;

	n_pct = 0;
	if (total_count != 0)
		n_pct = 100 * (double) n / total_count;

	rec_len_pct = 0;
	if (total_rec_len != 0)
		rec_len_pct = 100 * (double) rec_len / total_rec_len;

	fpi_len_pct = 0;
	if (total_fpi_len != 0)
		fpi_len_pct = 100 * (double) fpi_len / total_fpi_len;

	tot_len_pct = 0;
	if (total_len != 0)
		tot_len_pct = 100 * (double) tot_len / total_len;

	values[0] = CStringGetTextDatum(name);
	values[1] = Int64GetDatum(n);
	values[2] = Float4GetDatum(n_pct);
	values[3] = Int64GetDatum(rec_len);
	values[4] = Float4GetDatum(rec_len_pct);
	values[5] = Int64GetDatum(fpi_len);
	values[6] = Float4GetDatum(fpi_len_pct);
	values[7] = Int64GetDatum(tot_len);
	values[8] = Float4GetDatum(tot_len_pct);
}

/*
 * Get summary statistics about the records seen so far.
 */
static void
GetXLogSummaryStats(XLogRecStats * stats, Tuplestorestate *tupstore,
					TupleDesc tupdesc, Datum *values, bool *nulls)
{
	int			ri;
	uint64		total_count = 0;
	uint64		total_rec_len = 0;
	uint64		total_fpi_len = 0;
	uint64		total_len = 0;

	/*
	 * Each row shows its percentages of the total, so make a first pass to
	 * calculate column totals.
	 */
	for (ri = 0; ri < RM_NEXT_ID; ri++)
	{
		total_count += stats->rmgr_stats[ri].count;
		total_rec_len += stats->rmgr_stats[ri].rec_len;
		total_fpi_len += stats->rmgr_stats[ri].fpi_len;
	}
	total_len = total_rec_len + total_fpi_len;

	for (ri = 0; ri < RM_NEXT_ID; ri++)
	{
		uint64		count;
		uint64		rec_len;
		uint64		fpi_len;
		uint64		tot_len;
		const RmgrData *desc = &RmgrTable[ri];

		count = stats->rmgr_stats[ri].count;
		rec_len = stats->rmgr_stats[ri].rec_len;
		fpi_len = stats->rmgr_stats[ri].fpi_len;
		tot_len = rec_len + fpi_len;

		FillXLogStatsRow(desc->rm_name, count, total_count, rec_len,
						 total_rec_len, fpi_len, total_fpi_len, tot_len,
						 total_len, values, nulls);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
}

/*
 * Get the WAL stats between start LSN and end LSN.
 */
static void
GetWalStatsInternal(FunctionCallInfo fcinfo, XLogRecPtr start_lsn,
					XLogRecPtr end_lsn)
{
#define PG_GET_WAL_STATS_COLS 9
	XLogRecPtr	first_record;
	XLogReaderState *xlogreader;
	XLogRecStats stats;
	ReturnSetInfo *rsinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	Datum		values[PG_GET_WAL_STATS_COLS];
	bool		nulls[PG_GET_WAL_STATS_COLS];

	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	/* Check to see if caller supports us returning a tuplestore. */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type. */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Build tuplestore to hold the result rows. */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	xlogreader = InitXLogReaderState(start_lsn, &first_record, true);

	MemSet(&stats, 0, sizeof(stats));

	for (;;)
	{
		(void) ReadNextXLogRecord(xlogreader, first_record);

		if (xlogreader->currRecPtr >= end_lsn)
			break;

		StoreXLogRecordStats(&stats, xlogreader);
	}

	XLogReaderFree(xlogreader);

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, 0, sizeof(nulls));

	GetXLogSummaryStats(&stats, tupstore, tupdesc, values, nulls);

	/* Clean up and return the tuplestore. */
	tuplestore_donestoring(tupstore);
#undef PG_GET_WAL_STATS_COLS
}

/*
 * Get the WAL stats between start LSN and end LSN.
 */
Datum
pg_get_wal_stats(PG_FUNCTION_ARGS)
{
	XLogRecPtr	start_lsn;
	XLogRecPtr	end_lsn;

	start_lsn = PG_GETARG_LSN(0);
	end_lsn = PG_GETARG_LSN(1);

	if (XLogRecPtrIsInvalid(start_lsn) || XLogRecPtrIsInvalid(end_lsn))
		PG_RETURN_NULL();

	GetWalStatsInternal(fcinfo, start_lsn, end_lsn);

	PG_RETURN_VOID();
}
