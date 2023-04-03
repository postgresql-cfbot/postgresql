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
#include "storage/lwlock.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

/* #define TEST_SHARED_TIDSTORE 1 */

#define TEST_TIDSTORE_MAX_BYTES (2 * 1024 * 1024L) /* 2MB */

PG_FUNCTION_INFO_V1(test_tidstore);

static void
check_tid(TidStore *ts, BlockNumber blkno, OffsetNumber off, bool expect)
{
	ItemPointerData tid;
	bool found;

	ItemPointerSet(&tid, blkno, off);

	found = TidStoreIsMember(ts, &tid);

	if (found != expect)
		elog(ERROR, "TidStoreIsMember for TID (%u, %u) returned %d, expected %d",
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

#ifdef TEST_SHARED_TIDSTORE
	int tranche_id = LWLockNewTrancheId();
	dsa_area *dsa;

	LWLockRegisterTranche(tranche_id, "test_tidstore");
	dsa = dsa_create(tranche_id);

	ts = TidStoreCreate(TEST_TIDSTORE_MAX_BYTES, max_offset, dsa);
#else
	ts = TidStoreCreate(TEST_TIDSTORE_MAX_BYTES, max_offset, NULL);
#endif

	/* prepare the offset array */
	offs[0] = FirstOffsetNumber;
	offs[1] = FirstOffsetNumber + 1;
	offs[2] = max_offset / 2;
	offs[3] = max_offset - 1;
	offs[4] = max_offset;

	/* add tids */
	for (int i = 0; i < TEST_TIDSTORE_NUM_BLOCKS; i++)
		TidStoreSetBlockOffsets(ts, blks[i], offs, TEST_TIDSTORE_NUM_OFFSETS);

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
	if (TidStoreNumTids(ts) != (TEST_TIDSTORE_NUM_BLOCKS * TEST_TIDSTORE_NUM_OFFSETS))
		elog(ERROR, "TidStoreNumTids returned " UINT64_FORMAT ", expected %d",
			 TidStoreNumTids(ts),
			 TEST_TIDSTORE_NUM_BLOCKS * TEST_TIDSTORE_NUM_OFFSETS);

	/* iteration test */
	iter = TidStoreBeginIterate(ts);
	blk_idx = 0;
	while ((iter_result = TidStoreIterateNext(iter)) != NULL)
	{
		/* check the returned block number */
		if (blks_sorted[blk_idx] != iter_result->blkno)
			elog(ERROR, "TidStoreIterateNext returned block number %u, expected %u",
				 iter_result->blkno, blks_sorted[blk_idx]);

		/* check the returned offset numbers */
		if (TEST_TIDSTORE_NUM_OFFSETS != iter_result->num_offsets)
			elog(ERROR, "TidStoreIterateNext %u offsets, expected %u",
				 iter_result->num_offsets, TEST_TIDSTORE_NUM_OFFSETS);

		for (int i = 0; i < iter_result->num_offsets; i++)
		{
			if (offs[i] != iter_result->offsets[i])
				elog(ERROR, "TidStoreIterateNext offset number %u on block %u, expected %u",
					 iter_result->offsets[i], iter_result->blkno, offs[i]);
		}

		blk_idx++;
	}

	if (blk_idx != TEST_TIDSTORE_NUM_BLOCKS)
		elog(ERROR, "TidStoreIterateNext returned %d blocks, expected %d",
			 blk_idx, TEST_TIDSTORE_NUM_BLOCKS);

	/* remove all tids */
	TidStoreReset(ts);

	/* test the number of tids */
	if (TidStoreNumTids(ts) != 0)
		elog(ERROR, "TidStoreNumTids on empty store returned non-zero");

	/* lookup test for empty store */
	for (OffsetNumber off = FirstOffsetNumber ; off < MaxHeapTuplesPerPage;
		 off++)
	{
		check_tid(ts, 0, off, false);
		check_tid(ts, 2, off, false);
		check_tid(ts, MaxBlockNumber - 2, off, false);
		check_tid(ts, MaxBlockNumber, off, false);
	}

	TidStoreDestroy(ts);

#ifdef TEST_SHARED_TIDSTORE
	dsa_detach(dsa);
#endif
}

static void
test_empty(void)
{
	TidStore *ts;
	TidStoreIter *iter;
	ItemPointerData tid;

#ifdef TEST_SHARED_TIDSTORE
	int tranche_id = LWLockNewTrancheId();
	dsa_area *dsa;

	LWLockRegisterTranche(tranche_id, "test_tidstore");
	dsa = dsa_create(tranche_id);

	ts = TidStoreCreate(TEST_TIDSTORE_MAX_BYTES, MaxHeapTuplesPerPage, dsa);
#else
	ts = TidStoreCreate(TEST_TIDSTORE_MAX_BYTES, MaxHeapTuplesPerPage, NULL);
#endif

	elog(NOTICE, "testing empty tidstore");

	ItemPointerSet(&tid, 0, FirstOffsetNumber);
	if (TidStoreIsMember(ts, &tid))
		elog(ERROR, "TidStoreIsMember for TID (%u,%u) on empty store returned true",
			 0, FirstOffsetNumber);

	ItemPointerSet(&tid, MaxBlockNumber, MaxOffsetNumber);
	if (TidStoreIsMember(ts, &tid))
		elog(ERROR, "TidStoreIsMember for TID (%u,%u) on empty store returned true",
			 MaxBlockNumber, MaxOffsetNumber);

	if (TidStoreNumTids(ts) != 0)
		elog(ERROR, "TidStoreNumTids on empty store returned non-zero");

	if (TidStoreIsFull(ts))
		elog(ERROR, "TidStoreIsFull on empty store returned true");

	iter = TidStoreBeginIterate(ts);

	if (TidStoreIterateNext(iter) != NULL)
		elog(ERROR, "TidStoreIterateNext on empty store returned TIDs");

	TidStoreEndIterate(iter);

	TidStoreDestroy(ts);

#ifdef TEST_SHARED_TIDSTORE
	dsa_detach(dsa);
#endif
}

Datum
test_tidstore(PG_FUNCTION_ARGS)
{
	test_empty();

	elog(NOTICE, "testing basic operations");
	test_basic(MaxHeapTuplesPerPage);
	test_basic(10);
	test_basic(MaxHeapTuplesPerPage * 2);

	PG_RETURN_VOID();
}
