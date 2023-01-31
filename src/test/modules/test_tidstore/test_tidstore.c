/*--------------------------------------------------------------------------
 *
 * test_tidstore.c
 *		Test TidStore data structure.
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_tidstore/test_tidstore.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/tidstore.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/block.h"
#include "storage/itemptr.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

#define TEST_TIDSTORE_MAX_BYTES (2 * 1024 * 1024L) /* 2MB */

PG_FUNCTION_INFO_V1(test_tidstore);

static void
check_tid(TidStore *ts, BlockNumber blkno, OffsetNumber off, bool expect)
{
	ItemPointerData tid;
	bool found;

	ItemPointerSet(&tid, blkno, off);

	found = tidstore_lookup_tid(ts, &tid);

	if (found != expect)
		elog(ERROR, "lookup TID (%u, %u) returned %d, expected %d",
			 blkno, off, found, expect);
}

static void
test_basic(int max_offset)
{
#define TEST_TIDSTORE_NUM_BLOCKS	5
#define TEST_TIDSTORE_NUM_OFFSETS	5

	TidStore *ts;
	TidStoreIter *iter;
	TidStoreIterResult *iter_result;
	BlockNumber	blks[TEST_TIDSTORE_NUM_BLOCKS] = {
		0, MaxBlockNumber, MaxBlockNumber - 1, 1, MaxBlockNumber / 2,
	};
	BlockNumber	blks_sorted[TEST_TIDSTORE_NUM_BLOCKS] = {
		0, 1, MaxBlockNumber / 2, MaxBlockNumber - 1, MaxBlockNumber
	};
	OffsetNumber offs[TEST_TIDSTORE_NUM_OFFSETS];
	int blk_idx;

	/* prepare the offset array */
	offs[0] = FirstOffsetNumber;
	offs[1] = FirstOffsetNumber + 1;
	offs[2] = max_offset / 2;
	offs[3] = max_offset - 1;
	offs[4] = max_offset;

	ts = tidstore_create(TEST_TIDSTORE_MAX_BYTES, max_offset, NULL);

	/* add tids */
	for (int i = 0; i < TEST_TIDSTORE_NUM_BLOCKS; i++)
		tidstore_add_tids(ts, blks[i], offs, TEST_TIDSTORE_NUM_OFFSETS);

	/* lookup test */
	for (OffsetNumber off = FirstOffsetNumber ; off < max_offset; off++)
	{
		bool expect = false;
		for (int i = 0; i < TEST_TIDSTORE_NUM_OFFSETS; i++)
		{
			if (offs[i] == off)
			{
				expect = true;
				break;
			}
		}

		check_tid(ts, 0, off, expect);
		check_tid(ts, 2, off, false);
		check_tid(ts, MaxBlockNumber - 2, off, false);
		check_tid(ts, MaxBlockNumber, off, expect);
	}

	/* test the number of tids */
	if (tidstore_num_tids(ts) != (TEST_TIDSTORE_NUM_BLOCKS * TEST_TIDSTORE_NUM_OFFSETS))
		elog(ERROR, "tidstore_num_tids returned " UINT64_FORMAT ", expected %d",
			 tidstore_num_tids(ts),
			 TEST_TIDSTORE_NUM_BLOCKS * TEST_TIDSTORE_NUM_OFFSETS);

	/* iteration test */
	iter = tidstore_begin_iterate(ts);
	blk_idx = 0;
	while ((iter_result = tidstore_iterate_next(iter)) != NULL)
	{
		/* check the returned block number */
		if (blks_sorted[blk_idx] != iter_result->blkno)
			elog(ERROR, "tidstore_iterate_next returned block number %u, expected %u",
				 iter_result->blkno, blks_sorted[blk_idx]);

		/* check the returned offset numbers */
		if (TEST_TIDSTORE_NUM_OFFSETS != iter_result->num_offsets)
			elog(ERROR, "tidstore_iterate_next returned %u offsets, expected %u",
				 iter_result->num_offsets, TEST_TIDSTORE_NUM_OFFSETS);

		for (int i = 0; i < iter_result->num_offsets; i++)
		{
			if (offs[i] != iter_result->offsets[i])
				elog(ERROR, "tidstore_iterate_next returned offset number %u on block %u, expected %u",
					 iter_result->offsets[i], iter_result->blkno, offs[i]);
		}

		blk_idx++;
	}

	if (blk_idx != TEST_TIDSTORE_NUM_BLOCKS)
		elog(ERROR, "tidstore_iterate_next returned %d blocks, expected %d",
			 blk_idx, TEST_TIDSTORE_NUM_BLOCKS);

	/* remove all tids */
	tidstore_reset(ts);

	/* test the number of tids */
	if (tidstore_num_tids(ts) != 0)
		elog(ERROR, "tidstore_num_tids on empty store returned non-zero");

	/* lookup test for empty store */
	for (OffsetNumber off = FirstOffsetNumber ; off < MaxHeapTuplesPerPage;
		 off++)
	{
		check_tid(ts, 0, off, false);
		check_tid(ts, 2, off, false);
		check_tid(ts, MaxBlockNumber - 2, off, false);
		check_tid(ts, MaxBlockNumber, off, false);
	}

	tidstore_destroy(ts);
}

static void
test_empty(void)
{
	TidStore *ts;
	TidStoreIter *iter;
	ItemPointerData tid;

	elog(NOTICE, "testing empty tidstore");

	ts = tidstore_create(TEST_TIDSTORE_MAX_BYTES, MaxHeapTuplesPerPage, NULL);

	ItemPointerSet(&tid, 0, FirstOffsetNumber);
	if (tidstore_lookup_tid(ts, &tid))
		elog(ERROR, "tidstore_lookup_tid for (0,1) on empty store returned true");

	ItemPointerSet(&tid, MaxBlockNumber, MaxOffsetNumber);
	if (tidstore_lookup_tid(ts, &tid))
		elog(ERROR, "tidstore_lookup_tid for (%u,%u) on empty store returned true",
			 MaxBlockNumber, MaxOffsetNumber);

	if (tidstore_num_tids(ts) != 0)
		elog(ERROR, "tidstore_num_entries on empty store returned non-zero");

	if (tidstore_is_full(ts))
		elog(ERROR, "tidstore_is_full on empty store returned true");

	iter = tidstore_begin_iterate(ts);

	if (tidstore_iterate_next(iter) != NULL)
		elog(ERROR, "tidstore_iterate_next on empty store returned TIDs");

	tidstore_end_iterate(iter);

	tidstore_destroy(ts);
}

Datum
test_tidstore(PG_FUNCTION_ARGS)
{
	test_empty();

	elog(NOTICE, "testing basic operations");
	test_basic(MaxHeapTuplesPerPage);
	test_basic(10);

	PG_RETURN_VOID();
}
