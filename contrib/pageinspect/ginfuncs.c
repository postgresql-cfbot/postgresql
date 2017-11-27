/*
 * ginfuncs.c
 *		Functions to investigate the content of GIN indexes
 *
 * Source code is based on Gevel module available at
 * (http://www.sai.msu.su/~megera/oddmuse/index.cgi/Gevel)
 * Originally developed by:
 *  - Oleg Bartunov <oleg@sai.msu.su>, Moscow, Moscow University, Russia
 *  - Teodor Sigaev <teodor@sigaev.ru>, Moscow, Moscow University, Russia
 *
 * Copyright (c) 2014-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/pageinspect/ginfuncs.c
 */

#include "postgres.h"

#include "pageinspect.h"

#include "access/gin.h"
#include "access/gin_private.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "tsearch/ts_utils.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/regproc.h"
#include "utils/lsyscache.h"
#include "utils/varlena.h"


#define DatumGetItemPointer(X)	 ((ItemPointer) DatumGetPointer(X))
#define ItemPointerGetDatum(X)	 PointerGetDatum(X)


typedef struct GinStatState
{
	Relation	index;
	GinState	ginstate;
	OffsetNumber attnum;

	Buffer		buffer;
	OffsetNumber offset;
	Datum		curval;			/* Key */
	GinNullCategory category;
	Datum		dvalues[2];		/* Represents tuple 0 - key; 1 - # of TIDs */
	bool		nulls[2];
}			GinStatState;

typedef struct gin_leafpage_items_state
{
	TupleDesc	tupd;
	GinPostingList *seg;
	GinPostingList *lastseg;
} gin_leafpage_items_state;

PG_FUNCTION_INFO_V1(gin_metapage_info);
PG_FUNCTION_INFO_V1(gin_page_opaque_info);
PG_FUNCTION_INFO_V1(gin_leafpage_items);
PG_FUNCTION_INFO_V1(gin_value_count);
PG_FUNCTION_INFO_V1(gin_count_estimate);
PG_FUNCTION_INFO_V1(gin_stats);

/*
 * process_tuple
 *		Retrieves the number of TIDs in a tuple.
 */
static void
process_tuple(FuncCallContext *funcctx, GinStatState *st, IndexTuple itup)
{
	MemoryContext oldcontext;
	Datum		key;

	oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

	key = gintuple_get_key(&st->ginstate, itup, &st->category);

	if (st->category != GIN_CAT_NORM_KEY)
		st->curval = (Datum) 0;
	else
		st->curval = datumCopy(
							   key,
							   TupleDescAttr(st->index->rd_att, st->attnum)->attbyval,
							   TupleDescAttr(st->index->rd_att, st->attnum)->attlen);
	MemoryContextSwitchTo(oldcontext);

	st->dvalues[0] = st->curval;

	/* do not distinguish various null categories */
	st->nulls[0] = (st->category != GIN_CAT_NORM_KEY);

	if (GinIsPostingTree(itup))
	{
		BlockNumber rootblkno = GinGetPostingTree(itup);
		GinBtreeData btree;
		GinBtreeStack *stack;
		ItemPointerData minItem;
		int			nlist;
		ItemPointer list;
		Page		page;
		uint32		predictNumber;

		LockBuffer(st->buffer, GIN_UNLOCK);
		stack = ginScanBeginPostingTree(&btree, st->index, rootblkno, NULL);
		page = BufferGetPage(stack->buffer);
		ItemPointerSetMin(&minItem);
		list = GinDataLeafPageGetItems(page, &nlist, minItem);
		pfree(list);
		predictNumber = stack->predictNumber;
		st->dvalues[1] = Int32GetDatum(predictNumber * nlist);

		LockBuffer(stack->buffer, GIN_UNLOCK);
		freeGinBtreeStack(stack);
	}
	else
	{
		st->dvalues[1] = Int32GetDatum(GinGetNPosting(itup));
		LockBuffer(st->buffer, GIN_UNLOCK);
	}
}

static bool
move_right_if_needed(GinStatState *st)
{
	Page		page = BufferGetPage(st->buffer);

	if (st->offset > PageGetMaxOffsetNumber(page))
	{
		/*
		 * We scanned the whole page, so we should take right page
		 */
		BlockNumber blkno = GinPageGetOpaque(page)->rightlink;

		if (GinPageRightMost(page))
			return false;		/* no more page */

		LockBuffer(st->buffer, GIN_UNLOCK);
		st->buffer = ReleaseAndReadBuffer(st->buffer, st->index, blkno);
		LockBuffer(st->buffer, GIN_SHARE);
		st->offset = FirstOffsetNumber;
	}

	return true;
}

/*
 * refind_position
 *		Refinds a previous position, on return it has correctly
 *		set offset and buffer is locked.
 */
static bool
refind_position(GinStatState *st)
{
	Page		page;

	/* find left if needed (it causes only for first search) */
	for (;;)
	{
		IndexTuple	itup;
		BlockNumber blkno;

		LockBuffer(st->buffer, GIN_SHARE);

		page = BufferGetPage(st->buffer);
		if (GinPageIsLeaf(page))
			break;

		itup = (IndexTuple) PageGetItem(page,
										PageGetItemId(page, FirstOffsetNumber));
		blkno = GinItemPointerGetBlockNumber(&(itup)->t_tid);

		LockBuffer(st->buffer, GIN_UNLOCK);
		st->buffer = ReleaseAndReadBuffer(st->buffer, st->index, blkno);
	}

	if (st->offset == InvalidOffsetNumber)
	{
		return (PageGetMaxOffsetNumber(page) >= FirstOffsetNumber);
	}

	for (;;)
	{
		int			cmp;
		GinNullCategory category;
		Datum		datum;
		IndexTuple	itup;

		if (move_right_if_needed(st) == false)
			return false;

		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, st->offset));
		datum = gintuple_get_key(&st->ginstate, itup, &category);
		cmp = ginCompareAttEntries(&st->ginstate,
								   st->attnum + 1, st->curval, st->category,
								   gintuple_get_attrnum(&st->ginstate, itup),
								   datum, category);
		if (cmp == 0)
		{
			if (st->curval && !TupleDescAttr(st->index->rd_att, st->attnum)->attbyval)
				pfree((void *) st->curval);
			return true;
		}

		st->offset++;
	}

	return false;
}

static Relation
gin_index_open(RangeVar *relvar)
{
	Oid			relOid = RangeVarGetRelid(relvar, NoLock, false);
	Relation	rel = index_open(relOid, AccessShareLock);

	if (rel->rd_index == NULL)
		elog(ERROR, "relation %s.%s is not an index",
			 get_namespace_name(RelationGetNamespace(rel)),
			 RelationGetRelationName(rel)
			);

	if (rel->rd_rel->relam != GIN_AM_OID)
		elog(ERROR, "index %s.%s has wrong type",
			 get_namespace_name(RelationGetNamespace(rel)),
			 RelationGetRelationName(rel)
			);
	return rel;
}

static void
gin_setup_firstcall(FuncCallContext *funcctx, text *name, int attnum)
{
	MemoryContext oldcontext;
	GinStatState *st;
	char	   *relname = text_to_cstring(name);
	TupleDesc	tupdesc;

	oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

	st = (GinStatState *) palloc(sizeof(GinStatState));
	memset(st, 0, sizeof(GinStatState));
	st->index = gin_index_open(makeRangeVarFromNameList(stringToQualifiedNameList(relname)));
	initGinState(&st->ginstate, st->index);

	if (attnum < 0 || attnum >= st->index->rd_att->natts)
		elog(ERROR, "invalid attribute number");
	st->attnum = attnum;

	funcctx->user_fctx = (void *) st;

	tupdesc = CreateTemplateTupleDesc(2, false);
	TupleDescInitEntry(tupdesc, 1, "value",
					   TupleDescAttr(st->index->rd_att, st->attnum)->atttypid,
					   TupleDescAttr(st->index->rd_att, st->attnum)->atttypmod,
					   TupleDescAttr(st->index->rd_att, st->attnum)->attndims);
	TupleDescInitEntry(tupdesc, 2, "nrow", INT4OID, -1, 0);

	st->nulls[0] = false;
	st->nulls[1] = false;

	funcctx->slot = TupleDescGetSlot(tupdesc);
	funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);

	MemoryContextSwitchTo(oldcontext);
	pfree(relname);

	st->offset = InvalidOffsetNumber;
	st->buffer = ReadBuffer(st->index, GIN_ROOT_BLKNO);
}

/*
 * gin_metapage_info
 */
Datum
gin_metapage_info(PG_FUNCTION_ARGS)
{
	bytea	   *raw_page = PG_GETARG_BYTEA_P(0);
	TupleDesc	tupdesc;
	Page		page;
	GinPageOpaque opaq;
	GinMetaPageData *metadata;
	HeapTuple	resultTuple;
	Datum		values[10];
	bool		nulls[10];

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use raw page functions"))));

	page = get_page_from_raw(raw_page);

	opaq = (GinPageOpaque) PageGetSpecialPointer(page);
	if (opaq->flags != GIN_META)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("input page is not a GIN metapage"),
				 errdetail("Flags %04X, expected %04X",
						   opaq->flags, GIN_META)));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	metadata = GinPageGetMeta(page);

	memset(nulls, 0, sizeof(nulls));

	values[0] = Int64GetDatum(metadata->head);
	values[1] = Int64GetDatum(metadata->tail);
	values[2] = Int32GetDatum(metadata->tailFreeSize);
	values[3] = Int64GetDatum(metadata->nPendingPages);
	values[4] = Int64GetDatum(metadata->nPendingHeapTuples);

	/* statistics, updated by VACUUM */
	values[5] = Int64GetDatum(metadata->nTotalPages);
	values[6] = Int64GetDatum(metadata->nEntryPages);
	values[7] = Int64GetDatum(metadata->nDataPages);
	values[8] = Int64GetDatum(metadata->nEntries);

	values[9] = Int32GetDatum(metadata->ginVersion);

	/* Build and return the result tuple. */
	resultTuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(resultTuple);
}

/*
 * gin_page_opaque_info
 */
Datum
gin_page_opaque_info(PG_FUNCTION_ARGS)
{
	bytea	   *raw_page = PG_GETARG_BYTEA_P(0);
	TupleDesc	tupdesc;
	Page		page;
	GinPageOpaque opaq;
	HeapTuple	resultTuple;
	Datum		values[3];
	bool		nulls[3];
	Datum		flags[16];
	int			nflags = 0;
	uint16		flagbits;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use raw page functions"))));

	page = get_page_from_raw(raw_page);

	opaq = (GinPageOpaque) PageGetSpecialPointer(page);

	/* Build a tuple descriptor for the result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Convert the flags bitmask to an array of human-readable names */
	flagbits = opaq->flags;
	if (flagbits & GIN_DATA)
		flags[nflags++] = CStringGetTextDatum("data");
	if (flagbits & GIN_LEAF)
		flags[nflags++] = CStringGetTextDatum("leaf");
	if (flagbits & GIN_DELETED)
		flags[nflags++] = CStringGetTextDatum("deleted");
	if (flagbits & GIN_META)
		flags[nflags++] = CStringGetTextDatum("meta");
	if (flagbits & GIN_LIST)
		flags[nflags++] = CStringGetTextDatum("list");
	if (flagbits & GIN_LIST_FULLROW)
		flags[nflags++] = CStringGetTextDatum("list_fullrow");
	if (flagbits & GIN_INCOMPLETE_SPLIT)
		flags[nflags++] = CStringGetTextDatum("incomplete_split");
	if (flagbits & GIN_COMPRESSED)
		flags[nflags++] = CStringGetTextDatum("compressed");
	flagbits &= ~(GIN_DATA | GIN_LEAF | GIN_DELETED | GIN_META | GIN_LIST |
				  GIN_LIST_FULLROW | GIN_INCOMPLETE_SPLIT | GIN_COMPRESSED);
	if (flagbits)
	{
		/* any flags we don't recognize are printed in hex */
		flags[nflags++] = DirectFunctionCall1(to_hex32,
											  Int32GetDatum(flagbits));
	}

	memset(nulls, 0, sizeof(nulls));

	values[0] = Int64GetDatum(opaq->rightlink);
	values[1] = Int32GetDatum(opaq->maxoff);
	values[2] = PointerGetDatum(construct_array(flags, nflags,
												TEXTOID, -1, false, 'i'));

	/* Build and return the result tuple. */
	resultTuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(resultTuple);
}

/*
 * gin_leafpage_items
 */
Datum
gin_leafpage_items(PG_FUNCTION_ARGS)
{
	bytea	   *raw_page = PG_GETARG_BYTEA_P(0);
	FuncCallContext *fctx;
	gin_leafpage_items_state *inter_call_data;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use raw page functions"))));

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext mctx;
		Page		page;
		GinPageOpaque opaq;

		fctx = SRF_FIRSTCALL_INIT();
		mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		page = get_page_from_raw(raw_page);

		if (PageGetSpecialSize(page) != MAXALIGN(sizeof(GinPageOpaqueData)))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("input page is not a valid GIN data leaf page"),
					 errdetail("Special size %d, expected %d",
							   (int) PageGetSpecialSize(page),
							   (int) MAXALIGN(sizeof(GinPageOpaqueData)))));

		opaq = (GinPageOpaque) PageGetSpecialPointer(page);
		if (opaq->flags != (GIN_DATA | GIN_LEAF | GIN_COMPRESSED))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("input page is not a compressed GIN data leaf page"),
					 errdetail("Flags %04X, expected %04X",
							   opaq->flags,
							   (GIN_DATA | GIN_LEAF | GIN_COMPRESSED))));

		inter_call_data = palloc(sizeof(gin_leafpage_items_state));

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		inter_call_data->tupd = tupdesc;

		inter_call_data->seg = GinDataLeafPageGetPostingList(page);
		inter_call_data->lastseg = (GinPostingList *)
			(((char *) inter_call_data->seg) +
			 GinDataLeafPageGetPostingListSize(page));

		fctx->user_fctx = inter_call_data;

		MemoryContextSwitchTo(mctx);
	}

	fctx = SRF_PERCALL_SETUP();
	inter_call_data = fctx->user_fctx;

	if (inter_call_data->seg != inter_call_data->lastseg)
	{
		GinPostingList *cur = inter_call_data->seg;
		HeapTuple	resultTuple;
		Datum		result;
		Datum		values[3];
		bool		nulls[3];
		int			ndecoded,
					i;
		ItemPointer tids;
		Datum	   *tids_datum;

		memset(nulls, 0, sizeof(nulls));

		values[0] = ItemPointerGetDatum(&cur->first);
		values[1] = UInt16GetDatum(cur->nbytes);

		/* build an array of decoded item pointers */
		tids = ginPostingListDecode(cur, &ndecoded);
		tids_datum = (Datum *) palloc(ndecoded * sizeof(Datum));
		for (i = 0; i < ndecoded; i++)
			tids_datum[i] = ItemPointerGetDatum(&tids[i]);
		values[2] = PointerGetDatum(construct_array(tids_datum,
													ndecoded,
													TIDOID,
													sizeof(ItemPointerData),
													false, 's'));
		pfree(tids_datum);
		pfree(tids);

		/* build and return the result tuple */
		resultTuple = heap_form_tuple(inter_call_data->tupd, values, nulls);
		result = HeapTupleGetDatum(resultTuple);

		inter_call_data->seg = GinNextPostingListSegment(cur);

		SRF_RETURN_NEXT(fctx, result);
	}
	else
	{
		SRF_RETURN_DONE(fctx);
	}
}

/*
 * gin_value_count
 *		Prints estimated counts for each indexed value.
 */
Datum
gin_value_count(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	GinStatState *st;
	Datum		result = (Datum) 0;
	IndexTuple	ituple;
	HeapTuple	htuple;
	Page		page;

	if (SRF_IS_FIRSTCALL())
	{
		text	   *name = PG_GETARG_TEXT_P(0);

		funcctx = SRF_FIRSTCALL_INIT();
		gin_setup_firstcall(funcctx, name, (PG_NARGS() == 2) ?
							PG_GETARG_INT32(1) : 0);
		PG_FREE_IF_COPY(name, 0);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (GinStatState *) (funcctx->user_fctx);

	if (refind_position(st) == false)
	{
		UnlockReleaseBuffer(st->buffer);
		index_close(st->index, AccessShareLock);

		SRF_RETURN_DONE(funcctx);
	}

	for (;;)
	{
		st->offset++;

		if (move_right_if_needed(st) == false)
		{
			UnlockReleaseBuffer(st->buffer);
			index_close(st->index, AccessShareLock);

			SRF_RETURN_DONE(funcctx);
		}

		page = BufferGetPage(st->buffer);
		ituple = (IndexTuple) PageGetItem(page,
										  PageGetItemId(page, st->offset));

		if (st->attnum + 1 == gintuple_get_attrnum(&st->ginstate, ituple))
			break;
	}

	process_tuple(funcctx, st, ituple);

	htuple = heap_form_tuple(funcctx->attinmeta->tupdesc,
							 st->dvalues,
							 st->nulls);
	result = TupleGetDatum(funcctx->slot, htuple);

	SRF_RETURN_NEXT(funcctx, result);
}

/*
 * gin_count_estimate
 *		Outputs number of indexed rows matching query. It doesn't touch heap at
 *		all.
 *
 * Usage:
 *		SELECT gin_count_estimate('index_name', 'query');
 */
Datum
gin_count_estimate(PG_FUNCTION_ARGS)
{
	text	   *name = PG_GETARG_TEXT_P(0);
	Relation	index;
	IndexScanDesc scan;
	int64		count = 0;
	char	   *relname = text_to_cstring(name);
	ScanKeyData key;
	TIDBitmap  *bitmap = tbm_create(work_mem * 1024L, NULL);

	index = gin_index_open(makeRangeVarFromNameList(stringToQualifiedNameList(relname)));

	if (index->rd_opcintype[0] != TSVECTOROID)
	{
		index_close(index, AccessShareLock);
		elog(ERROR, "column type is not a tsvector");
	}

	key.sk_flags = 0;
	key.sk_attno = 1;
	key.sk_strategy = TSearchStrategyNumber;
	key.sk_subtype = 0;
	key.sk_argument = PG_GETARG_DATUM(1);

	fmgr_info(F_TS_MATCH_VQ, &key.sk_func);

	scan = index_beginscan_bitmap(index, SnapshotSelf, 1);
	index_rescan(scan, &key, 1, NULL, 0);

	count = index_getbitmap(scan, bitmap);
	tbm_free(bitmap);

	pfree(relname);

	index_endscan(scan);
	index_close(index, AccessShareLock);

	PG_RETURN_INT64(count);
}

/*
 * gin_stat
 *		Prints various stats about index internals.
 *
 * Usage:
 *		SELECT * FROM gin_stat('gin_idx') as t(value int, nrow int);
 */
Datum
gin_stats(PG_FUNCTION_ARGS)
{
	text	   *name = PG_GETARG_TEXT_P(0);
	RangeVar   *relvar;
	Relation	index;
	BlockNumber blkno;
	char		res[1024];
	uint32		totalPages,
				deletedPages = 0,
				emptyDataPages = 0,
				entryPages = 0,
				dataPages = 0,
				dataInnerPages = 0,
				dataLeafPages = 0,
				entryInnerPages = 0,
				entryLeafPages = 0;

	uint64		dataInnerFreeSpace = 0,
				dataLeafFreeSpace = 0,
				dataInnerTuplesCount = 0,
				dataLeafIptrsCount = 0,
				entryInnerFreeSpace = 0,
				entryLeafFreeSpace = 0,
				entryInnerTuplesCount = 0,
				entryLeafTuplesCount = 0,
				entryPostingSize = 0,
				entryPostingCount = 0,
				entryAttrSize = 0;

	relvar = makeRangeVarFromNameList(textToQualifiedNameList(name));
	index = relation_openrv(relvar, AccessExclusiveLock);

	if (index->rd_rel->relkind != RELKIND_INDEX ||
		index->rd_rel->relam != GIN_AM_OID)
		elog(ERROR, "relation \"%s\" is not a GIN index",
			 RelationGetRelationName(index));

	totalPages = RelationGetNumberOfBlocks(index);

	for (blkno = GIN_ROOT_BLKNO; blkno < totalPages; blkno++)
	{
		Buffer		buffer;
		Page		page;
		PageHeader	header;

		buffer = ReadBuffer(index, blkno);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buffer);
		header = (PageHeader) page;

		if (GinPageIsDeleted(page))
		{
			deletedPages++;
		}
		else if (GinPageIsData(page))
		{
			dataPages++;
			if (GinPageIsLeaf(page))
			{
				ItemPointerData minItem,
						   *ptr;
				int			nlist;


				dataLeafPages++;
				dataLeafFreeSpace += header->pd_upper - header->pd_lower;
				ItemPointerSetMin(&minItem);

				ptr = GinDataLeafPageGetItems(page, &nlist, minItem);

				if (ptr)
				{
					pfree(ptr);
					dataLeafIptrsCount += nlist;
				}
				else
					emptyDataPages++;
			}
			else
			{
				dataInnerPages++;
				dataInnerFreeSpace += header->pd_upper - header->pd_lower;
				dataInnerTuplesCount += GinPageGetOpaque(page)->maxoff;
			}
		}
		else
		{
			IndexTuple	itup;
			OffsetNumber i,
						maxoff;

			maxoff = PageGetMaxOffsetNumber(page);

			entryPages++;
			if (GinPageIsLeaf(page))
			{
				entryLeafPages++;
				entryLeafFreeSpace += header->pd_upper - header->pd_lower;
				entryLeafTuplesCount += maxoff;
			}
			else
			{
				entryInnerPages++;
				entryInnerFreeSpace += header->pd_upper - header->pd_lower;
				entryInnerTuplesCount += maxoff;
			}

			for (i = 1; i <= maxoff; i++)
			{
				itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, i));

				if (GinPageIsLeaf(page))
				{
					GinPostingList *list = (GinPostingList *) GinGetPosting(itup);

					entryPostingCount += GinGetNPosting(itup);
					entryPostingSize += SizeOfGinPostingList(list);
					entryAttrSize += GinGetPostingOffset(itup) -
						IndexInfoFindDataOffset((itup)->t_info);
				}
				else
				{
					entryAttrSize += IndexTupleSize(itup) -
						IndexInfoFindDataOffset((itup)->t_info);
				}
			}
		}

		UnlockReleaseBuffer(buffer);
	}

	index_close(index, AccessExclusiveLock);
	totalPages--;

	snprintf(res, sizeof(res),
			 "totalPages:            %u\n"
			 "deletedPages:          %u\n"
			 "emptyDataPages:        %u\n"
			 "dataPages:             %u\n"
			 "dataInnerPages:        %u\n"
			 "dataLeafPages:         %u\n"
			 "dataInnerFreeSpace:    " INT64_FORMAT "\n"
			 "dataLeafFreeSpace:     " INT64_FORMAT "\n"
			 "dataInnerTuplesCount:  " INT64_FORMAT "\n"
			 "dataLeafIptrsCount:    " INT64_FORMAT "\n"
			 "entryPages:            %u\n"
			 "entryInnerPages:       %u\n"
			 "entryLeafPages:        %u\n"
			 "entryInnerFreeSpace:   " INT64_FORMAT "\n"
			 "entryLeafFreeSpace:    " INT64_FORMAT "\n"
			 "entryInnerTuplesCount: " INT64_FORMAT "\n"
			 "entryLeafTuplesCount:  " INT64_FORMAT "\n"
			 "entryPostingSize:      " INT64_FORMAT "\n"
			 "entryPostingCount:     " INT64_FORMAT "\n"
			 "entryAttrSize:         " INT64_FORMAT "\n"
			 ,
			 totalPages,
			 deletedPages,
			 emptyDataPages,
			 dataPages,
			 dataInnerPages,
			 dataLeafPages,
			 dataInnerFreeSpace,
			 dataLeafFreeSpace,
			 dataInnerTuplesCount,
			 dataLeafIptrsCount,
			 entryPages,
			 entryInnerPages,
			 entryLeafPages,
			 entryInnerFreeSpace,
			 entryLeafFreeSpace,
			 entryInnerTuplesCount,
			 entryLeafTuplesCount,
			 entryPostingSize,
			 entryPostingCount,
			 entryAttrSize
		);

	PG_RETURN_TEXT_P(CStringGetTextDatum(res));
}
