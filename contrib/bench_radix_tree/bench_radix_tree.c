/*-------------------------------------------------------------------------
 *
 * bench_radix_tree.c
 *
 * Copyright (c) 2016-2022, PostgreSQL Global Development Group
 *
 *	  contrib/bench_radix_tree/bench_radix_tree.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/pg_prng.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/radixtree.h"
#include "miscadmin.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

#define TIDS_PER_BLOCK_FOR_LOAD		30
#define TIDS_PER_BLOCK_FOR_LOOKUP	50

PG_FUNCTION_INFO_V1(bench_seq_search);
PG_FUNCTION_INFO_V1(bench_shuffle_search);
PG_FUNCTION_INFO_V1(bench_load_random_int);

static radix_tree *rt = NULL;
static ItemPointer itemptrs = NULL;

static uint64
tid_to_key_off(ItemPointer tid, uint32 *off)
{
	uint32 upper;
	uint32 shift = pg_ceil_log2_32(MaxHeapTuplesPerPage);
	int64 tid_i;

	Assert(ItemPointerGetOffsetNumber(tid) < MaxHeapTuplesPerPage);

	tid_i = ItemPointerGetOffsetNumber(tid);
	tid_i |= ItemPointerGetBlockNumber(tid) << shift;

	/* log(sizeof(uint64) * BITS_PER_BYTE, 2) = log(64, 2) = 6 */
	*off = tid_i & ((1 << 6) - 1);
	upper = tid_i >> 6;
	Assert(*off < (sizeof(uint64) * BITS_PER_BYTE));

	Assert(*off < 64);

	return upper;
}

static int
shuffle_randrange(pg_prng_state *state, int lower, int upper)
{
	return (int) floor(pg_prng_double(state) * ((upper-lower)+0.999999)) + lower;
}

/* Naive Fisher-Yates implementation*/
static void
shuffle_itemptrs(ItemPointer itemptr, uint64 nitems)
{
	/* reproducability */
	pg_prng_state state;

	pg_prng_seed(&state, 0);

	for (int i = 0; i < nitems - 1; i++)
	{
		int j = shuffle_randrange(&state, i, nitems - 1);
		ItemPointerData t = itemptrs[j];

		itemptrs[j] = itemptrs[i];
		itemptrs[i] = t;
	}
}

static ItemPointer
generate_tids(BlockNumber minblk, BlockNumber maxblk, int ntids_per_blk, uint64 *ntids_p)
{
	ItemPointer	tids;
	uint64	maxitems;
	uint64	ntids = 0;

	maxitems = (maxblk - minblk + 1) * ntids_per_blk;
	tids = MemoryContextAllocHuge(TopTransactionContext,
								  sizeof(ItemPointerData) * maxitems);

	for (BlockNumber blk = minblk; blk < maxblk; blk++)
	{
		for (OffsetNumber off = FirstOffsetNumber;
			 off <= ntids_per_blk; off++)
		{
			CHECK_FOR_INTERRUPTS();

			ItemPointerSetBlockNumber(&(tids[ntids]), blk);
			ItemPointerSetOffsetNumber(&(tids[ntids]), off);

			ntids++;
		}
	}

	*ntids_p = ntids;
	return tids;
}

static int
vac_cmp_itemptr(const void *left, const void *right)
{
	BlockNumber lblk,
				rblk;
	OffsetNumber loff,
				roff;

	lblk = ItemPointerGetBlockNumber((ItemPointer) left);
	rblk = ItemPointerGetBlockNumber((ItemPointer) right);

	if (lblk < rblk)
		return -1;
	if (lblk > rblk)
		return 1;

	loff = ItemPointerGetOffsetNumber((ItemPointer) left);
	roff = ItemPointerGetOffsetNumber((ItemPointer) right);

	if (loff < roff)
		return -1;
	if (loff > roff)
		return 1;

	return 0;
}

static Datum
bench_search(FunctionCallInfo fcinfo, bool shuffle)
{
	BlockNumber minblk = PG_GETARG_INT32(0);
	BlockNumber maxblk = PG_GETARG_INT32(1);
	uint64	ntids;
	uint64	key;
	uint64	last_key = PG_UINT64_MAX;;
	uint64	val = 0;
	ItemPointer tids;
	TupleDesc	tupdesc;
	TimestampTz	start_time,	end_time;
	long	secs;
	int		usecs;
	int64	rt_load_ms, rt_search_ms, ar_load_ms, ar_search_ms;
	Datum	values[7];
	bool	nulls[7];

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tids = generate_tids(minblk, maxblk, TIDS_PER_BLOCK_FOR_LOAD, &ntids);

	/* measure the load time of the radix tree */
	rt = rt_create(CurrentMemoryContext);
	start_time = GetCurrentTimestamp();
	for (int i = 0; i < ntids; i++)
	{
		ItemPointer tid = &(tids[i]);
		uint32	off;

		CHECK_FOR_INTERRUPTS();

		key = tid_to_key_off(tid, &off);

		if (last_key != PG_UINT64_MAX && last_key != key)
		{
			rt_set(rt, last_key, val);
			val = 0;
		}

		last_key = key;
		val |= (uint64) 1 << off;
	}
	if (last_key != PG_UINT64_MAX)
		rt_set(rt, last_key, val);

	end_time = GetCurrentTimestamp();
	TimestampDifference(start_time, end_time, &secs, &usecs);
	rt_load_ms = secs * 1000 + usecs / 1000;

	/* measure the load time of the array */
	itemptrs = MemoryContextAllocHuge(CurrentMemoryContext,
									  sizeof(ItemPointerData) * ntids);
	start_time = GetCurrentTimestamp();
	for (int i = 0; i < ntids; i++)
	{
		ItemPointerSetBlockNumber(&(itemptrs[i]),
								  ItemPointerGetBlockNumber(&(tids[i])));
		ItemPointerSetOffsetNumber(&(itemptrs[i]),
								   ItemPointerGetOffsetNumber(&(tids[i])));
	}
	end_time = GetCurrentTimestamp();
	TimestampDifference(start_time, end_time, &secs, &usecs);
	ar_load_ms = secs * 1000 + usecs / 1000;

	if (shuffle)
		shuffle_itemptrs(tids, ntids);

	/* meaure the serach time of the radix tree */
	start_time = GetCurrentTimestamp();
	for (int i = 0; i < ntids; i++)
	{
		ItemPointer tid = &(tids[i]);
		uint64	key, val;
		uint32	off;

		CHECK_FOR_INTERRUPTS();

		key = tid_to_key_off(tid, &off);

		rt_search(rt, key, &val);
	}
	end_time = GetCurrentTimestamp();
	TimestampDifference(start_time, end_time, &secs, &usecs);
	rt_search_ms = secs * 1000 + usecs / 1000;

	/* next, measure the serach time of the array */
	start_time = GetCurrentTimestamp();
	for (int i = 0; i < ntids; i++)
	{
		ItemPointer tid = &(tids[i]);

		bsearch((void *) tid,
				(void *) itemptrs,
				ntids,
				sizeof(ItemPointerData),
				vac_cmp_itemptr);
	}
	end_time = GetCurrentTimestamp();
	TimestampDifference(start_time, end_time, &secs, &usecs);
	ar_search_ms = secs * 1000 + usecs / 1000;

	MemSet(nulls, false, sizeof(nulls));
	values[0] = Int64GetDatum(rt_num_entries(rt));
	values[1] = Int64GetDatum(rt_memory_usage(rt));
	values[2] = Int64GetDatum(sizeof(ItemPointerData) * ntids);
	values[3] = Int64GetDatum(rt_load_ms);
	values[4] = Int64GetDatum(ar_load_ms);
	values[5] = Int64GetDatum(rt_search_ms);
	values[6] = Int64GetDatum(ar_search_ms);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

Datum
bench_seq_search(PG_FUNCTION_ARGS)
{
	return bench_search(fcinfo, false);
}

Datum
bench_shuffle_search(PG_FUNCTION_ARGS)
{
	return bench_search(fcinfo, true);
}

Datum
bench_load_random_int(PG_FUNCTION_ARGS)
{
	uint64	cnt = (uint64) PG_GETARG_INT64(0);
	radix_tree *rt;
	pg_prng_state state;
	TupleDesc	tupdesc;
	TimestampTz	start_time,	end_time;
	long	secs;
	int		usecs;
	int64	load_time_ms;
	Datum	values[2];
	bool	nulls[2];

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	pg_prng_seed(&state, 0);
	rt = rt_create(CurrentMemoryContext);

	start_time = GetCurrentTimestamp();
	for (uint64 i = 0; i < cnt; i++)
	{
		uint64 key = pg_prng_uint64(&state);

		rt_set(rt, key, key);
	}
	end_time = GetCurrentTimestamp();

	TimestampDifference(start_time, end_time, &secs, &usecs);
	load_time_ms = secs * 1000 + usecs / 1000;

	MemSet(nulls, false, sizeof(nulls));
	values[0] = Int64GetDatum(rt_memory_usage(rt));
	values[1] = Int64GetDatum(load_time_ms);

	rt_free(rt);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}
