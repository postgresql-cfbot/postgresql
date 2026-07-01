/*-------------------------------------------------------------------------
 *
 * verify_brin.c
 *	  Functions to check postgresql brin indexes for corruption
 *
 * Copyright (c) 2016-2026, PostgreSQL Global Development Group
 *
 *	  contrib/amcheck/verify_brin.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/brin.h"
#include "access/brin_page.h"
#include "access/brin_revmap.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "catalog/index.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_operator.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "verify_common.h"

PG_FUNCTION_INFO_V1(brin_index_check);

typedef struct BrinCheckState
{

	/* Check arguments */

	bool		regularpagescheck;
	bool		heapallindexed;
	ArrayType  *consistent_oper_names;

	/* BRIN check common fields */

	Relation	idxrel;
	Relation	heaprel;
	BrinDesc   *bdesc;
	int			natts;
	BlockNumber pagesPerRange;

	/* Index structure check fields */

	BufferAccessStrategy checkstrategy;
	BlockNumber idxnblocks;
	BlockNumber heapnblocks;
	BlockNumber lastRevmapPage;
	/* Current range blkno */
	BlockNumber rangeBlkno;
	/* Current revmap item */
	BlockNumber revmapBlk;
	Buffer		revmapbuf;
	Page		revmappage;
	uint32		revmapidx;
	/* Current index tuple */
	BlockNumber regpageBlk;
	Buffer		regpagebuf;
	Page		regpage;
	OffsetNumber regpageoffset;

	/* Heap all indexed check fields */

	BrinRevmap *revmap;
	Buffer		buf;
	FmgrInfo   *consistentFn;
	/* Scan keys for regular values */
	ScanKey    *nonnull_sk;
	/* Scan keys for null values */
	ScanKey    *isnull_sk;
	double		range_cnt;
	/* first block of the next range */
	BlockNumber nextrangeBlk;

	/*
	 * checkable_range shows if current range could be checked and dtup
	 * contains valid index tuple for the range. It could be false if the
	 * current range is not summarized, or it's placeholder, or it's just a
	 * beginning of the check
	 */
	bool		checkable_range;
	BrinMemTuple *dtup;
	MemoryContext rangeCtx;
	MemoryContext heaptupleCtx;
} BrinCheckState;

static void brin_check(Relation idxrel, Relation heaprel, void *callback_state, bool readonly);

static void check_brin_index_structure(BrinCheckState *state);

static void check_meta(BrinCheckState *state);

static void check_revmap(BrinCheckState *state);

static void check_revmap_item(BrinCheckState *state);

static void check_index_tuple(BrinCheckState *state, BrinTuple *tuple, ItemId lp);

static void check_regular_pages(BrinCheckState *state);

static bool revmap_points_to_index_tuple(BrinCheckState *state);

static ItemId PageGetItemIdCareful(BrinCheckState *state);

static void check_heap_all_indexed(BrinCheckState *state);

static void prepare_nonnull_scan_keys(BrinCheckState *state);

static void brin_check_callback(Relation index,
								ItemPointer tid,
								Datum *values,
								bool *isnull,
								bool tupleIsAlive,
								void *brstate);

static void check_heap_tuple(BrinCheckState *state, const Datum *values, const bool *nulls, ItemPointer tid);

static ScanKey prepare_nonnull_scan_key(const BrinCheckState *state, AttrNumber attno, String *opname);

static ScanKey prepare_isnull_scan_key(AttrNumber attno);

static void brin_check_ereport(BrinCheckState *state, const char *fmt);

static void revmap_item_ereport(BrinCheckState *state, const char *fmt);

static void index_tuple_ereport(BrinCheckState *state, const char *fmt);

static void index_tuple_only_ereport(BrinCheckState *state, const char *fmt);

static void heap_all_indexed_ereport(const BrinCheckState *state, const ItemPointerData *tid, const char *message);

Datum
brin_index_check(PG_FUNCTION_ARGS)
{
	Oid			indrelid = PG_GETARG_OID(0);
	BrinCheckState *state = palloc0(sizeof(BrinCheckState));

	state->regularpagescheck = PG_GETARG_BOOL(1);
	state->heapallindexed = PG_GETARG_BOOL(2);
	state->consistent_oper_names = PG_GETARG_ARRAYTYPE_P(3);

	amcheck_lock_relation_and_check(indrelid,
									BRIN_AM_OID,
									brin_check,
									ShareUpdateExclusiveLock,
									state);

	PG_RETURN_VOID();
}

/*
 * Main check function
 */
static void
brin_check(Relation idxrel, Relation heaprel, void *callback_state, bool readonly)
{
	BrinCheckState *state = (BrinCheckState *) callback_state;

	/* Initialize check common fields */
	state->idxrel = idxrel;
	state->heaprel = heaprel;
	state->bdesc = brin_build_desc(idxrel);
	state->natts = state->bdesc->bd_tupdesc->natts;

	/* Do some preparations and checks for heapallindexed */
	if (state->heapallindexed)
	{
		/*
		 * Check if we are OK with indcheckxmin, and unregister snapshot as we
		 * don't need it further
		 */
		Snapshot	snapshot = RegisterSnapshot(GetTransactionSnapshot());

		check_indcheckxmin(state->idxrel, snapshot);
		UnregisterSnapshot(snapshot);

		/*
		 * If there are some problems with scan keys generation or operator
		 * name array is invalid we want to fail fast. So do it here.
		 */
		prepare_nonnull_scan_keys(state);
	}

	check_brin_index_structure(state);

	if (state->heapallindexed)
	{
		check_heap_all_indexed(state);
	}

	brin_free_desc(state->bdesc);
}

/*
 * Check that index has expected structure
 *
 *  Some check expectations:
 * - we hold ShareUpdateExclusiveLock, so revmap could not be extended (i.e. no evacuation) while check as well as
 *   all regular pages should stay regular and ranges could not be summarized and desummarized.
 *   Nevertheless, concurrent updates could lead to new regular page allocations
 *   and moving of index tuples.
 * - if revmap pointer is valid there should be valid index tuple it points to.
 * - there are no orphan index tuples (if there is an index tuple, the revmap item points to this tuple also must exist)
 * - it's possible to encounter placeholder tuples (as a result of crash)
 * - it's possible to encounter new pages instead of regular (as a result of crash)
 * - it's possible to encounter pages with evacuation bit (as a result of crash)
 *
 */
static void
check_brin_index_structure(BrinCheckState *state)
{
	/* Index structure check fields initialization */
	state->checkstrategy = GetAccessStrategy(BAS_BULKREAD);

	check_meta(state);

	/* Check revmap first, blocks: [1, lastRevmapPage] */
	check_revmap(state);

	if (state->regularpagescheck)
	{
		/* Check regular pages, blocks: [lastRevmapPage + 1, idxnblocks] */
		check_regular_pages(state);
	}

}

/* Meta page check and save some data for the further check */
static void
check_meta(BrinCheckState *state)
{
	Buffer		metabuf;
	Page		metapage;
	BrinMetaPageData *metadata;

	/* Meta page check */
	metabuf = ReadBufferExtended(state->idxrel, MAIN_FORKNUM, BRIN_METAPAGE_BLKNO, RBM_NORMAL,
								 state->checkstrategy);
	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
	metapage = BufferGetPage(metabuf);
	metadata = (BrinMetaPageData *) PageGetContents(metapage);
	state->idxnblocks = RelationGetNumberOfBlocks(state->idxrel);

	if (!BRIN_IS_META_PAGE(metapage) ||
		metadata->brinMagic != BRIN_META_MAGIC ||
		metadata->brinVersion != BRIN_CURRENT_VERSION ||
		metadata->pagesPerRange < 1 || metadata->pagesPerRange > BRIN_MAX_PAGES_PER_RANGE ||
		metadata->lastRevmapPage <= BRIN_METAPAGE_BLKNO || metadata->lastRevmapPage >= state->idxnblocks)
	{
		brin_check_ereport(state, "metapage is corrupted");
	}

	state->lastRevmapPage = metadata->lastRevmapPage;
	state->pagesPerRange = metadata->pagesPerRange;
	UnlockReleaseBuffer(metabuf);
}

/*
 * This is a main part of the brin index structure check.
 * We walk revmap page by page from the beginning and check every revmap item and
 * every index tuple pointed from the revmap.
 */
static void
check_revmap(BrinCheckState *state)
{
	Relation	idxrel = state->idxrel;
	BlockNumber lastRevmapPage = state->lastRevmapPage;
	ReadStream *stream;
	int			stream_flags;
	ReadStreamBlockNumberCB stream_cb;
	BlockRangeReadStreamPrivate stream_data;

	state->rangeBlkno = 0;
	state->regpagebuf = InvalidBuffer;
	state->heapnblocks = RelationGetNumberOfBlocks(state->heaprel);

	/*
	 * Prepare stream data for revmap walk. It is safe to use batchmode as
	 * block_range_read_stream_cb takes no locks.
	 */
	stream_flags = READ_STREAM_SEQUENTIAL | READ_STREAM_USE_BATCHING;
	/* First revmap page is right after meta page */
	stream_data.current_blocknum = BRIN_METAPAGE_BLKNO + 1;
	stream_data.last_exclusive = lastRevmapPage + 1;

	stream_cb = block_range_read_stream_cb;
	stream = read_stream_begin_relation(stream_flags,
										GetAccessStrategy(BAS_BULKREAD),
										idxrel,
										MAIN_FORKNUM,
										stream_cb,
										&stream_data,
										0);

	/* Walk each revmap page */
	while ((state->revmapbuf = read_stream_next_buffer(stream, NULL)) != InvalidBuffer)
	{
		state->revmapBlk = BufferGetBlockNumber(state->revmapbuf);
		LockBuffer(state->revmapbuf, BUFFER_LOCK_SHARE);
		state->revmappage = BufferGetPage(state->revmapbuf);

		/*
		 * Pages with block numbers in [1, lastRevmapPage] should be revmap
		 * pages
		 */
		if (!BRIN_IS_REVMAP_PAGE(state->revmappage))
		{
			brin_check_ereport(state, psprintf("revmap page is expected at block %u, last revmap page %u",
											   state->revmapBlk,
											   lastRevmapPage));
		}
		LockBuffer(state->revmapbuf, BUFFER_LOCK_UNLOCK);

		/* Walk and check all brin tuples from the current revmap page */
		state->revmapidx = 0;
		while (state->revmapidx < REVMAP_PAGE_MAXITEMS)
		{
			CHECK_FOR_INTERRUPTS();

			/* Check revmap item */
			check_revmap_item(state);

			state->rangeBlkno += state->pagesPerRange;
			state->revmapidx++;
		}

		elog(DEBUG3, "Complete revmap page check: %d", state->revmapBlk);

		ReleaseBuffer(state->revmapbuf);
	}

	read_stream_end(stream);

	if (BufferIsValid(state->regpagebuf))
	{
		ReleaseBuffer(state->regpagebuf);
	}
}

/*
 * Check revmap item.
 *
 * We check revmap item pointer itself and if it is ok we check the index tuple it points to.
 *
 * To avoid deadlock we need to unlock revmap page before locking regular page,
 * so when we get the lock on the regular page our index tuple pointer may no longer be relevant.
 * So for some checks before reporting an error we need to make sure that our pointer is still relevant and if it's not - retry.
 */
static void
check_revmap_item(BrinCheckState *state)
{
	ItemPointerData *revmaptids;
	RevmapContents *contents;
	ItemPointerData *iptr;
	ItemId		lp;
	BrinTuple  *tup;
	Relation	idxrel = state->idxrel;

	/* Loop to retry revmap item check if there was a concurrent update. */
	for (;;)
	{
		LockBuffer(state->revmapbuf, BUFFER_LOCK_SHARE);

		contents = (RevmapContents *) PageGetContents(BufferGetPage(state->revmapbuf));
		revmaptids = contents->rm_tids;
		/* Pointer for the range with start at state->rangeBlkno */
		iptr = revmaptids + state->revmapidx;

		/* At first check revmap item pointer */

		/*
		 * Tuple pointer is invalid means range isn't summarized, just move
		 * further
		 */
		if (!ItemPointerIsValid(iptr))
		{
			elog(DEBUG3, "Range %u is not summarized", state->rangeBlkno);
			LockBuffer(state->revmapbuf, BUFFER_LOCK_UNLOCK);
			break;
		}

		/*
		 * Pointer is valid, it should points to index tuple for the range
		 * with blkno rangeBlkno. Remember it and unlock revmap page to avoid
		 * deadlock
		 */
		state->regpageBlk = ItemPointerGetBlockNumber(iptr);
		state->regpageoffset = ItemPointerGetOffsetNumber(iptr);

		LockBuffer(state->revmapbuf, BUFFER_LOCK_UNLOCK);

		/*
		 * Check if the regpage block number is greater than the relation
		 * size. To avoid fetching the number of blocks for each tuple, use
		 * cached value first
		 */
		if (state->regpageBlk >= state->idxnblocks)
		{
			/*
			 * Regular pages may have been added, so refresh idxnblocks and
			 * recheck
			 */
			state->idxnblocks = RelationGetNumberOfBlocks(idxrel);
			if (state->regpageBlk >= state->idxnblocks)
			{
				revmap_item_ereport(state,
									psprintf("revmap item points to a non existing block %u, index max block %u",
											 state->regpageBlk,
											 state->idxnblocks - 1));
			}
		}

		/*
		 * To avoid some pin/unpin cycles we cache last used regular page.
		 * Check if we need different regular page and fetch it.
		 */
		if (!BufferIsValid(state->regpagebuf) || BufferGetBlockNumber(state->regpagebuf) != state->regpageBlk)
		{
			if (BufferIsValid(state->regpagebuf))
			{
				ReleaseBuffer(state->regpagebuf);
			}
			state->regpagebuf = ReadBufferExtended(idxrel, MAIN_FORKNUM, state->regpageBlk, RBM_NORMAL,
												   state->checkstrategy);
		}

		LockBuffer(state->regpagebuf, BUFFER_LOCK_SHARE);
		state->regpage = BufferGetPage(state->regpagebuf);

		/* Revmap should always point to a regular page */
		if (!BRIN_IS_REGULAR_PAGE(state->regpage))
		{
			revmap_item_ereport(state,
								psprintf("revmap item points to the page which is not regular (blkno: %u)",
										 state->regpageBlk));

		}

		/* Check item offset is valid */
		if (state->regpageoffset > PageGetMaxOffsetNumber(state->regpage))
		{

			/* If concurrent update moved our tuple we need to retry */
			if (!revmap_points_to_index_tuple(state))
			{
				LockBuffer(state->regpagebuf, BUFFER_LOCK_UNLOCK);
				continue;
			}

			revmap_item_ereport(state,
								psprintf("revmap item offset number %u is greater than regular page %u max offset %u",
										 state->regpageoffset,
										 state->regpageBlk,
										 PageGetMaxOffsetNumber(state->regpage)));
		}

		elog(DEBUG3, "Process range: %u, iptr: (%u,%u)", state->rangeBlkno, state->regpageBlk, state->regpageoffset);

		/*
		 * Revmap pointer is OK. It points to existing regular page, offset
		 * also is ok. Let's check index tuple it points to.
		 */

		lp = PageGetItemIdCareful(state);

		/* Revmap should point to NORMAL tuples only */
		if (!ItemIdIsUsed(lp))
		{

			/* If concurrent update moved our tuple we need to retry */
			if (!revmap_points_to_index_tuple(state))
			{
				LockBuffer(state->regpagebuf, BUFFER_LOCK_UNLOCK);
				continue;
			}

			index_tuple_ereport(state, "revmap item points to unused index tuple");
		}

		tup = (BrinTuple *) PageGetItem(state->regpage, lp);

		/* Check if range block number is as expected */
		if (tup->bt_blkno != state->rangeBlkno)
		{

			/* If concurrent update moved our tuple we need to retry */
			if (!revmap_points_to_index_tuple(state))
			{
				LockBuffer(state->regpagebuf, BUFFER_LOCK_UNLOCK);
				continue;
			}

			index_tuple_ereport(state, psprintf("index tuple has invalid blkno %u", tup->bt_blkno));
		}

		/*
		 * If the range is beyond the table size - the range must be empty.
		 * It's valid situation for empty table now.
		 */
		if (state->rangeBlkno >= state->heapnblocks)
		{
			if (!BrinTupleIsEmptyRange(tup))
			{
				index_tuple_ereport(state,
									psprintf("the range is beyond the table size, "
											 "but is not marked as empty, table size: %u blocks",
											 state->heapnblocks));
			}
		}

		/* Check index tuple itself */
		check_index_tuple(state, tup, lp);

		LockBuffer(state->regpagebuf, BUFFER_LOCK_UNLOCK);
		break;
	}
}

/*
 * Check that index tuple has expected structure.
 *
 * This function follows the logic performed by brin_deform_tuple().
 * After this check is complete we are sure that brin_deform_tuple can process it.
 *
 * In case of empty range check that for all attributes allnulls are true, hasnulls are false and
 * there is no data. All core opclasses expect allnulls is true for empty range.
 */
static void
check_index_tuple(BrinCheckState *state, BrinTuple *tuple, ItemId lp)
{

	char	   *tp;				/* tuple data */
	uint16		off;
	uint8	   *nullbits;
	TupleDesc	disktdesc;
	int			stored;
	bool		empty_range = BrinTupleIsEmptyRange(tuple);
	bool		hasnullbitmap = BrinTupleHasNulls(tuple);
	uint8		hoff = BrinTupleDataOffset(tuple);
	uint16		tuplen = ItemIdGetLength(lp);

	/* Check that header length is not greater than tuple length */
	if (hoff > tuplen)
	{
		index_tuple_ereport(state, psprintf("index tuple header length %u is greater than tuple len %u", hoff, tuplen));
	}

	/* If tuple has null bitmap - initialize it */
	if (hasnullbitmap)
	{
		nullbits = (uint8 *) ((char *) tuple + SizeOfBrinTuple);
	}
	else
	{
		nullbits = NULL;
	}

	/* Empty range index tuple checks */
	if (empty_range)
	{
		/* Empty range tuple should have null bitmap */
		if (!hasnullbitmap)
		{
			index_tuple_ereport(state, "empty range index tuple doesn't have null bitmap");
		}

		Assert(nullbits != NULL);

		/* Check every attribute has allnulls is true and hasnulls is false */
		for (int attindex = 0; attindex < state->natts; ++attindex)
		{

			/* Attribute allnulls should be true for empty range */
			if (att_isnull(attindex, nullbits))
			{
				index_tuple_ereport(state,
									psprintf("empty range index tuple attribute %d with allnulls is false",
											 attindex));
			}

			/* Attribute hasnulls should be false for empty range */
			if (!att_isnull(state->natts + attindex, nullbits))
			{
				index_tuple_ereport(state,
									psprintf("empty range index tuple attribute %d with hasnulls is true",
											 attindex));
			}
		}

		/* We are done with empty range tuple */
		return;
	}

	/*
	 * Range is marked as not empty so we can have some data in the tuple.
	 * Walk all attributes and checks that all stored values fit into the
	 * tuple
	 */

	tp = (char *) tuple + BrinTupleDataOffset(tuple);
	stored = 0;
	off = 0;

	disktdesc = brin_tuple_tupdesc(state->bdesc);

	for (int attindex = 0; attindex < state->natts; ++attindex)
	{
		BrinOpcInfo *opclass = state->bdesc->bd_info[attindex];

		/*
		 * if allnulls is set we have no data for this attribute, move to the
		 * next
		 */
		if (hasnullbitmap && !att_isnull(attindex, nullbits))
		{
			stored += opclass->oi_nstored;
			continue;
		}

		/* Walk all stored values for the current attribute */
		for (int datumno = 0; datumno < opclass->oi_nstored; datumno++)
		{
			CompactAttribute *thisatt = TupleDescCompactAttr(disktdesc, stored);

			if (thisatt->attlen == -1)
			{
				off = att_pointer_alignby(off,
										  thisatt->attalignby,
										  -1,
										  tp + off);
			}
			else
			{
				off = att_nominal_alignby(off, thisatt->attalignby);
			}

			/* Check that we are still in the tuple */
			if (hoff + off > tuplen)
			{
				index_tuple_ereport(state,
									psprintf("attribute %u stored value %u with length %d "
											 "starts at offset %u beyond total tuple length %u",
											 attindex, datumno, thisatt->attlen, off, tuplen));
			}

			off = att_addlength_pointer(off, thisatt->attlen, tp + off);

			/* Check that we are still in the tuple */
			if (hoff + off > tuplen)
			{
				index_tuple_ereport(state,
									psprintf("attribute %u stored value %u with length %d "
											 "ends at offset %u beyond total tuple length %u",
											 attindex, datumno, thisatt->attlen, off, tuplen));
			}
			stored++;
		}

	}

}

/*
 * At the moment we should have been already check that every index
 * tuple in the regular pages has valid structure and range blkno
 * (because every normal index tuple must have pointer in the revmap and
 * we followed every such pointer in check_revmap). So here we just want
 * to do some additional checks to be sure that there is nothing wrong
 * with the regular pages [lastRevmapPage + 1, indexnblocks]:
 *   - there is a pointer in revmap to each NORMAL index tuple
 *     (no orphans index tuples)
 *   - all pages have expected type (REGULAR). We can encounter new pages as
 *     result of crash, so we just skip such pages.
 */
static void
check_regular_pages(BrinCheckState *state)
{
	ReadStream *stream;
	int			stream_flags;
	ReadStreamBlockNumberCB stream_cb;
	BlockRangeReadStreamPrivate stream_data;

	/* reset state */
	state->revmapBlk = InvalidBlockNumber;
	state->revmapbuf = InvalidBuffer;
	state->revmapidx = -1;
	state->regpageBlk = InvalidBlockNumber;
	state->regpagebuf = InvalidBuffer;
	state->regpageoffset = InvalidOffsetNumber;
	state->idxnblocks = RelationGetNumberOfBlocks(state->idxrel);

	/*
	 * Prepare stream data for regular pages walk. It is safe to use batchmode
	 * as block_range_read_stream_cb takes no locks.
	 */
	stream_flags = READ_STREAM_SEQUENTIAL | READ_STREAM_USE_BATCHING | READ_STREAM_FULL;
	/* First regular page is right after the last revmap page */
	stream_data.current_blocknum = state->lastRevmapPage + 1;
	stream_data.last_exclusive = state->idxnblocks;

	stream_cb = block_range_read_stream_cb;
	stream = read_stream_begin_relation(stream_flags,
										GetAccessStrategy(BAS_BULKREAD),
										state->idxrel,
										MAIN_FORKNUM,
										stream_cb,
										&stream_data,
										0);

	while ((state->regpagebuf = read_stream_next_buffer(stream, NULL)) != InvalidBuffer)
	{
		OffsetNumber maxoff;

		state->regpageBlk = BufferGetBlockNumber(state->regpagebuf);
		LockBuffer(state->regpagebuf, BUFFER_LOCK_SHARE);
		state->regpage = BufferGetPage(state->regpagebuf);

		/* Skip new pages */
		if (PageIsNew(state->regpage))
		{
			UnlockReleaseBuffer(state->regpagebuf);
			continue;
		}

		if (!BRIN_IS_REGULAR_PAGE(state->regpage))
		{
			brin_check_ereport(state, psprintf("expected new or regular page at block %u", state->regpageBlk));
		}

		/* Check that all NORMAL index tuples within the page are not orphans */
		maxoff = PageGetMaxOffsetNumber(state->regpage);
		for (state->regpageoffset = FirstOffsetNumber; state->regpageoffset <= maxoff; state->regpageoffset++)
		{
			ItemId		lp;

			CHECK_FOR_INTERRUPTS();

			lp = PageGetItemIdCareful(state);

			if (ItemIdIsUsed(lp))
			{
				BrinTuple  *tup;
				BlockNumber revmapBlk;

				tup = (BrinTuple *) PageGetItem(state->regpage, lp);

				/* Get revmap block number for index tuple blkno */
				revmapBlk = ((tup->bt_blkno / state->pagesPerRange) / REVMAP_PAGE_MAXITEMS) + 1;
				if (revmapBlk > state->lastRevmapPage)
				{
					index_tuple_only_ereport(state, psprintf("no revmap page for the index tuple with blkno %u",
															 tup->bt_blkno));
				}

				/* Fetch another revmap page if needed */
				if (state->revmapBlk != revmapBlk)
				{
					if (BlockNumberIsValid(state->revmapBlk))
					{
						ReleaseBuffer(state->revmapbuf);
					}
					state->revmapBlk = revmapBlk;
					state->revmapbuf = ReadBufferExtended(state->idxrel, MAIN_FORKNUM, state->revmapBlk, RBM_NORMAL,
														  state->checkstrategy);
				}

				state->revmapidx = (tup->bt_blkno / state->pagesPerRange) % REVMAP_PAGE_MAXITEMS;
				state->rangeBlkno = tup->bt_blkno;

				/* check that revmap item points to index tuple */
				if (!revmap_points_to_index_tuple(state))
				{
					index_tuple_ereport(state, psprintf("revmap doesn't point to index tuple"));
				}

			}
		}

		UnlockReleaseBuffer(state->regpagebuf);
	}

	read_stream_end(stream);

	if (state->revmapbuf != InvalidBuffer)
	{
		ReleaseBuffer(state->revmapbuf);
	}
}

/*
 * Check if the revmap item points to the index tuple (regpageBlk, regpageoffset).
 * We have locked reg page, and lock revmap page here.
 * It's a valid lock ordering, so no deadlock is possible.
 */
static bool
revmap_points_to_index_tuple(BrinCheckState *state)
{
	ItemPointerData *revmaptids;
	RevmapContents *contents;
	ItemPointerData *tid;
	bool		points;

	LockBuffer(state->revmapbuf, BUFFER_LOCK_SHARE);
	contents = (RevmapContents *) PageGetContents(BufferGetPage(state->revmapbuf));
	revmaptids = contents->rm_tids;
	tid = revmaptids + state->revmapidx;

	points = ItemPointerGetBlockNumberNoCheck(tid) == state->regpageBlk &&
		ItemPointerGetOffsetNumberNoCheck(tid) == state->regpageoffset;

	LockBuffer(state->revmapbuf, BUFFER_LOCK_UNLOCK);
	return points;
}

/*
 * PageGetItemId() wrapper that validates returned line pointer.
 *
 * itemId in brin index could be UNUSED or NORMAL.
 */
static ItemId
PageGetItemIdCareful(BrinCheckState *state)
{
	Page		page = state->regpage;
	OffsetNumber offset = state->regpageoffset;
	ItemId		itemid = PageGetItemId(page, offset);

	if (ItemIdGetOffset(itemid) + ItemIdGetLength(itemid) >
		BLCKSZ - MAXALIGN(sizeof(BrinSpecialSpace)))
		index_tuple_ereport(state,
							psprintf("line pointer points past end of tuple space in index. "
									 "lp_off=%u, lp_len=%u lp_flags=%u",
									 ItemIdGetOffset(itemid),
									 ItemIdGetLength(itemid),
									 ItemIdGetFlags(itemid)
									 )
			);

	/* Verify that line pointer is LP_NORMAL or LP_UNUSED */
	if (!((ItemIdIsNormal(itemid) && ItemIdHasStorage(itemid)) ||
		  (!ItemIdIsUsed(itemid) && !ItemIdHasStorage(itemid))))
	{
		index_tuple_ereport(state,
							psprintf("invalid line pointer storage in index. "
									 "lp_off=%u, lp_len=%u lp_flags=%u",
									 ItemIdGetOffset(itemid),
									 ItemIdGetLength(itemid),
									 ItemIdGetFlags(itemid)
									 ));
	}

	return itemid;
}

/*
 * Check that every heap tuple are consistent with the index.
 *
 * Here we generate ScanKey for every heap tuple and test it against
 * appropriate range using consistentFn (for ScanKey generation logic look 'prepare_nonnull_scan_keys')
 *
 * Also, we check that fields 'empty_range', 'all_nulls' and 'has_nulls'
 * are not too "narrow" for each range, which means:
 * 1) has_nulls = false, but we see null value (only for oi_regular_nulls is true)
 * 2) all_nulls = true, but we see nonnull value.
 * 3) empty_range = true, but we see tuple within the range.
 *
 * We use allowSync = false, because this way
 * we process full ranges one by one from the first range.
 * It's not necessary, but makes the code simpler and this way
 * we need to fetch every index tuple only once.
 */
static void
check_heap_all_indexed(BrinCheckState *state)
{
	Relation	idxrel = state->idxrel;
	Relation	heaprel = state->heaprel;
	double		reltuples;
	IndexInfo  *indexInfo;

	/* heap all indexed check fields initialization */

	state->revmap = brinRevmapInitialize(idxrel, &state->pagesPerRange);
	state->dtup = brin_new_memtuple(state->bdesc);
	state->checkable_range = false;
	state->consistentFn = palloc0_array(FmgrInfo, state->natts);
	state->range_cnt = 0;
	/* next range is the first range in the beginning */
	state->nextrangeBlk = 0;
	state->isnull_sk = palloc0_array(ScanKey, state->natts);
	state->rangeCtx = AllocSetContextCreate(CurrentMemoryContext,
											"brin check range context",
											ALLOCSET_DEFAULT_SIZES);
	state->heaptupleCtx = AllocSetContextCreate(CurrentMemoryContext,
												"brin check tuple context",
												ALLOCSET_DEFAULT_SIZES);

	/*
	 * Prepare "is_null" scan keys and consistent fn for each attribute.
	 * "non-null" scan keys are already generated.
	 */
	for (AttrNumber attno = 1; attno <= state->natts; attno++)
	{
		FmgrInfo   *tmp;

		tmp = index_getprocinfo(idxrel, attno, BRIN_PROCNUM_CONSISTENT);
		fmgr_info_copy(&state->consistentFn[attno - 1], tmp, CurrentMemoryContext);

		state->isnull_sk[attno - 1] = prepare_isnull_scan_key(attno);
	}

	indexInfo = BuildIndexInfo(idxrel);

	/*
	 * Use snapshot to check only those tuples that are guaranteed to be
	 * indexed already. Using SnapshotAny would make it more difficult to say
	 * if there is a corruption or checked tuple just haven't been indexed
	 * yet. Also, we want to support CIC indexes.
	 */
	indexInfo->ii_Concurrent = true;
	reltuples = table_index_build_scan(heaprel, idxrel, indexInfo, false, true,
									   brin_check_callback, (void *) state, NULL);

	elog(DEBUG3, "ranges were checked: %f", state->range_cnt);
	elog(DEBUG3, "scan total tuples: %f", reltuples);

	if (state->buf != InvalidBuffer)
		ReleaseBuffer(state->buf);

	brinRevmapTerminate(state->revmap);
	MemoryContextDelete(state->rangeCtx);
	MemoryContextDelete(state->heaptupleCtx);
}

/*
 * Generate scan keys for every index attribute.
 *
 * ConsistentFn requires ScanKey, so we need to generate ScanKey for every
 * attribute somehow. We want ScanKey that would result in TRUE for every heap
 * tuple within the range when we use its indexed value as sk_argument.
 * To generate such a ScanKey we need to define the right operand type and the strategy number.
 * Right operand type is a type of data that index is built on, so it's 'opcintype'.
 * There is no strategy number that we can always use,
 * because every opclass defines its own set of operators it supports and strategy number
 * for the same operator can differ from opclass to opclass.
 * So to get strategy number we look up an operator that gives us desired behavior
 * and which both operand types are 'opcintype' and then retrieve the strategy number for it.
 * Most of the time we can use '='. We let user define operator name in case opclass doesn't
 * support '=' operator. Also, if such operator doesn't exist, we can't proceed with the check.
 *
 * If operator name array is empty use "=" operator for every attribute.
 */
static void
prepare_nonnull_scan_keys(BrinCheckState *state)
{
	Oid			element_type = ARR_ELEMTYPE(state->consistent_oper_names);
	int16		typlen;
	bool		typbyval;
	char		typalign;
	Datum	   *values;
	bool	   *elem_nulls;
	int			num_elems;

	get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);
	deconstruct_array(state->consistent_oper_names, element_type, typlen, typbyval, typalign,
					  &values, &elem_nulls, &num_elems);

	/*
	 * If we have some input, check that number of operators in the input is
	 * relevant to the index
	 */
	if (num_elems > 0 && num_elems != state->natts)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("number of operator names in input (%u) "
						"doesn't match index attributes number (%u)",
						num_elems, state->natts)));
	}

	/* Generate scan key for every index attribute */
	state->nonnull_sk = palloc0_array(ScanKey, state->natts);

	for (AttrNumber attno = 1; attno <= state->natts; attno++)
	{
		String	   *operatorName;

		if (num_elems > 0)
		{

			if (elem_nulls[attno - 1])
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("operator name must not be NULL")));
			}

			operatorName = makeString(TextDatumGetCString(values[attno - 1]));
		}
		else
		{

			/* Use '=' as default operator */
			operatorName = makeString("=");
		}

		state->nonnull_sk[attno - 1] = prepare_nonnull_scan_key(state, attno, operatorName);
		pfree(operatorName);
	}
}

/*
 * Prepare ScanKey for index attribute.
 *
 * Generated once, and will be reused for all heap tuples.
 * Argument field will be filled for every heap tuple before
 * consistent function invocation, so leave it NULL for a while.
 */
static ScanKey
prepare_nonnull_scan_key(const BrinCheckState *state, AttrNumber attno, String *opname)
{
	ScanKey		scanKey;
	Oid			opOid;
	Oid			opFamilyOid;
	bool		defined;
	StrategyNumber strategy;
	RegProcedure opRegProc;
	List	   *operNameList;
	int			attindex = attno - 1;
	Form_pg_attribute attr = TupleDescAttr(state->bdesc->bd_tupdesc, attindex);
	Oid			type = state->idxrel->rd_opcintype[attindex];

	opFamilyOid = state->idxrel->rd_opfamily[attindex];
	operNameList = list_make1(opname);
	opOid = OperatorLookup(operNameList, type, type, &defined);

	if (opOid == InvalidOid)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("there is no operator %s for type \"%s\"",
						opname->sval, format_type_be(type))));
	}

	strategy = get_op_opfamily_strategy(opOid, opFamilyOid);

	if (strategy == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("operator %s is not a member of operator family \"%s\"",
						opname->sval,
						get_opfamily_name(opFamilyOid, false))));
	}

	opRegProc = get_opcode(opOid);
	scanKey = palloc0(sizeof(ScanKeyData));
	ScanKeyEntryInitialize(
						   scanKey,
						   0,
						   attno,
						   strategy,
						   type,
						   attr->attcollation,
						   opRegProc,
						   (Datum) NULL
		);
	pfree(operNameList);

	return scanKey;
}

static ScanKey
prepare_isnull_scan_key(AttrNumber attno)
{
	ScanKey		scanKey;

	scanKey = palloc0(sizeof(ScanKeyData));
	ScanKeyEntryInitialize(scanKey,
						   SK_ISNULL | SK_SEARCHNULL,
						   attno,
						   InvalidStrategy,
						   InvalidOid,
						   InvalidOid,
						   InvalidOid,
						   (Datum) 0);
	return scanKey;
}

/*
 * We walk from the first range (blkno = 0) to the last as the scan proceed.
 * For every heap tuple we check if we are done with the current range, and we need to move further
 * to the current heap tuple's range. While moving to the next range we check that it's not empty (because
 * we have at least one tuple for this range).
 * Every heap tuple are checked to be consistent with the range it belongs to.
 * In case of unsummarized ranges and placeholders we skip all checks.
 *
 * While moving, we may jump over some ranges,
 * but it's okay because we would not be able to check them anyway.
 * We also can't say whether skipped ranges should be marked as empty or not,
 * since it's possible that there were some tuples before that are now deleted.
 *
 */
static void
brin_check_callback(Relation index, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *brstate)
{
	BrinCheckState *state;
	BlockNumber heapblk;

	state = (BrinCheckState *) brstate;
	heapblk = ItemPointerGetBlockNumber(tid);

	/* If we went beyond the current range let's fetch new range */
	if (heapblk >= state->nextrangeBlk)
	{
		BrinTuple  *tup;
		BrinTuple  *tupcopy = NULL;
		MemoryContext oldCtx;
		OffsetNumber off;
		Size		size;
		Size		btupsz = 0;

		MemoryContextReset(state->rangeCtx);
		oldCtx = MemoryContextSwitchTo(state->rangeCtx);

		state->range_cnt++;

		/* Move to the range that contains current heap tuple */
		tup = brinGetTupleForHeapBlock(state->revmap, heapblk, &state->buf,
									   &off, &size, BUFFER_LOCK_SHARE);

		if (tup)
		{
			tupcopy = brin_copy_tuple(tup, size, tupcopy, &btupsz);
			LockBuffer(state->buf, BUFFER_LOCK_UNLOCK);
			state->dtup = brin_deform_tuple(state->bdesc, tupcopy, state->dtup);

			/* We can't check placeholder ranges */
			state->checkable_range = !state->dtup->bt_placeholder;
		}
		else
		{
			/* We can't check unsummarized ranges. */
			state->checkable_range = false;
		}

		/*
		 * Update nextrangeBlk so we know when we are done with the current
		 * range
		 */
		state->nextrangeBlk = (heapblk / state->pagesPerRange + 1) * state->pagesPerRange;

		MemoryContextSwitchTo(oldCtx);

		/* Range must not be empty */
		if (state->checkable_range && state->dtup->bt_empty_range)
		{
			heap_all_indexed_ereport(state, tid, "range is marked as empty but contains qualified live tuples");
		}

	}

	/* Check tuple is consistent with the index */
	if (state->checkable_range)
	{
		check_heap_tuple(state, values, isnull, tid);
	}

}

/*
 * We check hasnulls flags for null values and oi_regular_nulls = true,
 * check allnulls is false for all nonnull values not matter oi_regular_nulls is set or not,
 * For all other cases we call consistentFn with appropriate scanKey:
 * - for oi_regular_nulls = false and null values we use 'isNull' scanKey,
 * - for nonnull values we use 'nonnull' scanKey
 */
static void
check_heap_tuple(BrinCheckState *state, const Datum *values, const bool *nulls, ItemPointer tid)
{
	int			attindex;
	BrinMemTuple *dtup = state->dtup;
	BrinDesc   *bdesc = state->bdesc;
	MemoryContext oldCtx;

	Assert(state->checkable_range);

	MemoryContextReset(state->heaptupleCtx);
	oldCtx = MemoryContextSwitchTo(state->heaptupleCtx);

	/* check every index attribute */
	for (attindex = 0; attindex < state->natts; attindex++)
	{
		BrinValues *bval;
		Datum		consistentFnResult;
		bool		consistent;
		ScanKey		scanKey;
		bool		oi_regular_nulls = bdesc->bd_info[attindex]->oi_regular_nulls;

		bval = &dtup->bt_columns[attindex];

		if (nulls[attindex])
		{
			/*
			 * Use hasnulls flag for oi_regular_nulls is true. Otherwise,
			 * delegate check to consistentFn
			 */
			if (oi_regular_nulls)
			{
				/* We have null value, so hasnulls or allnulls must be true */
				if (!(bval->bv_hasnulls || bval->bv_allnulls))
				{
					heap_all_indexed_ereport(state, tid,
											 "range hasnulls and allnulls are false, but contains a null value");
				}
				continue;
			}

			/*
			 * In case of null and oi_regular_nulls = false we use isNull
			 * scanKey for invocation of consistentFn
			 */
			scanKey = state->isnull_sk[attindex];
		}
		else
		{
			/* We have a nonnull value, so allnulls should be false */
			if (bval->bv_allnulls)
			{
				heap_all_indexed_ereport(state, tid, "range allnulls is true, but contains nonnull value");
			}

			/* use nonnull scan key */
			scanKey = state->nonnull_sk[attindex];
			scanKey->sk_argument = values[attindex];
		}

		/* If oi_regular_nulls = true we should never get there with null */
		Assert(!oi_regular_nulls || !nulls[attindex]);

		if (state->consistentFn[attindex].fn_nargs >= 4)
		{
			consistentFnResult = FunctionCall4Coll(&state->consistentFn[attindex],
												   state->idxrel->rd_indcollation[attindex],
												   PointerGetDatum(state->bdesc),
												   PointerGetDatum(bval),
												   PointerGetDatum(&scanKey),
												   Int32GetDatum(1)
				);
		}
		else
		{
			consistentFnResult = FunctionCall3Coll(&state->consistentFn[attindex],
												   state->idxrel->rd_indcollation[attindex],
												   PointerGetDatum(state->bdesc),
												   PointerGetDatum(bval),
												   PointerGetDatum(scanKey)
				);
		}

		consistent = DatumGetBool(consistentFnResult);

		if (!consistent)
		{
			heap_all_indexed_ereport(state, tid, "heap tuple inconsistent with index");
		}

	}

	MemoryContextSwitchTo(oldCtx);
}

/* Report without any additional info */
static void
brin_check_ereport(BrinCheckState *state, const char *fmt)
{
	ereport(ERROR,
			(errcode(ERRCODE_INDEX_CORRUPTED),
			 errmsg("index %s is corrupted - %s", RelationGetRelationName(state->idxrel), fmt)));
}

/* Report with range blkno, revmap item info, index tuple info */
static void
index_tuple_ereport(BrinCheckState *state, const char *fmt)
{
	Assert(state->rangeBlkno != InvalidBlockNumber);
	Assert(state->revmapBlk != InvalidBlockNumber);
	Assert(state->revmapidx < REVMAP_PAGE_MAXITEMS);
	Assert(state->regpageBlk != InvalidBlockNumber);
	Assert(state->regpageoffset != InvalidOffsetNumber);

	ereport(ERROR,
			(errcode(ERRCODE_INDEX_CORRUPTED),
			 errmsg("index %s is corrupted - %s. Range blkno: %u, revmap item: (%u,%u), index tuple: (%u,%u)",
					RelationGetRelationName(state->idxrel),
					fmt,
					state->rangeBlkno,
					state->revmapBlk,
					state->revmapidx,
					state->regpageBlk,
					state->regpageoffset)));
}

/* Report with index tuple info */
static void
index_tuple_only_ereport(BrinCheckState *state, const char *fmt)
{
	Assert(state->regpageBlk != InvalidBlockNumber);
	Assert(state->regpageoffset != InvalidOffsetNumber);

	ereport(ERROR,
			(errcode(ERRCODE_INDEX_CORRUPTED),
			 errmsg("index %s is corrupted - %s. Index tuple: (%u,%u)",
					RelationGetRelationName(state->idxrel),
					fmt,
					state->regpageBlk,
					state->regpageoffset)));
}

/* Report with range blkno, revmap item info */
static void
revmap_item_ereport(BrinCheckState *state, const char *fmt)
{
	Assert(state->rangeBlkno != InvalidBlockNumber);
	Assert(state->revmapBlk != InvalidBlockNumber);
	Assert(state->revmapidx < REVMAP_PAGE_MAXITEMS);

	ereport(ERROR,
			(errcode(ERRCODE_INDEX_CORRUPTED),
			 errmsg("index %s is corrupted - %s. Range blkno: %u, revmap item: (%u,%u).",
					RelationGetRelationName(state->idxrel),
					fmt,
					state->rangeBlkno,
					state->revmapBlk,
					state->revmapidx)));
}

/* Report with range blkno, heap tuple info */
static void
heap_all_indexed_ereport(const BrinCheckState *state, const ItemPointerData *tid, const char *message)
{
	Assert(state->rangeBlkno != InvalidBlockNumber);

	ereport(ERROR,
			(errcode(ERRCODE_INDEX_CORRUPTED),
			 errmsg("index %s is not consistent with the heap - %s. Range blkno: %u, heap tid (%u,%u)",
					RelationGetRelationName(state->idxrel),
					message,
					state->dtup->bt_blkno,
					ItemPointerGetBlockNumber(tid),
					ItemPointerGetOffsetNumber(tid))));
}
