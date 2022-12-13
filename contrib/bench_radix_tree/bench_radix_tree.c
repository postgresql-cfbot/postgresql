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
#include <math.h>
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

/* run benchmark also for binary-search case? */
/* #define MEASURE_BINARY_SEARCH 1 */

#define TIDS_PER_BLOCK_FOR_LOAD		30
#define TIDS_PER_BLOCK_FOR_LOOKUP	50

PG_FUNCTION_INFO_V1(bench_seq_search);
PG_FUNCTION_INFO_V1(bench_shuffle_search);
PG_FUNCTION_INFO_V1(bench_load_random_int);
PG_FUNCTION_INFO_V1(bench_fixed_height_search);
PG_FUNCTION_INFO_V1(bench_search_random_nodes);
PG_FUNCTION_INFO_V1(bench_node128_load);

static uint64
tid_to_key_off(ItemPointer tid, uint32 *off)
{
	uint64		upper;
	uint32		shift = pg_ceil_log2_32(MaxHeapTuplesPerPage);
	int64		tid_i;

	Assert(ItemPointerGetOffsetNumber(tid) < MaxHeapTuplesPerPage);

	tid_i = ItemPointerGetOffsetNumber(tid);
	tid_i |= (uint64) ItemPointerGetBlockNumber(tid) << shift;

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
	return (int) floor(pg_prng_double(state) * ((upper - lower) + 0.999999)) + lower;
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
		int			j = shuffle_randrange(&state, i, nitems - 1);
		ItemPointerData t = itemptr[j];

		itemptr[j] = itemptr[i];
		itemptr[i] = t;
	}
}

static ItemPointer
generate_tids(BlockNumber minblk, BlockNumber maxblk, int ntids_per_blk, uint64 *ntids_p,
			  bool random_block)
{
	ItemPointer tids;
	uint64		maxitems;
	uint64		ntids = 0;
	pg_prng_state state;

	maxitems = (maxblk - minblk + 1) * ntids_per_blk;
	tids = MemoryContextAllocHuge(TopTransactionContext,
								  sizeof(ItemPointerData) * maxitems);

	if (random_block)
		pg_prng_seed(&state, 0x9E3779B185EBCA87);

	for (BlockNumber blk = minblk; blk < maxblk; blk++)
	{
		if (random_block && !pg_prng_bool(&state))
			continue;

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

#ifdef MEASURE_BINARY_SEARCH
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
#endif

static Datum
bench_search(FunctionCallInfo fcinfo, bool shuffle)
{
	BlockNumber minblk = PG_GETARG_INT32(0);
	BlockNumber maxblk = PG_GETARG_INT32(1);
	bool		random_block = PG_GETARG_BOOL(2);
	radix_tree *rt = NULL;
	bool		shared = PG_GETARG_BOOL(3);
	dsa_area   *dsa = NULL;
	uint64		ntids;
	uint64		key;
	uint64		last_key = PG_UINT64_MAX;
	uint64		val = 0;
	ItemPointer tids;
	TupleDesc	tupdesc;
	TimestampTz start_time,
				end_time;
	long		secs;
	int			usecs;
	int64		rt_load_ms,
				rt_search_ms;
	Datum		values[7];
	bool		nulls[7];

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tids = generate_tids(minblk, maxblk, TIDS_PER_BLOCK_FOR_LOAD, &ntids, random_block);

	/* measure the load time of the radix tree */
	if (shared)
		dsa = dsa_create(LWLockNewTrancheId());
	rt = rt_create(CurrentMemoryContext, dsa);

	/* measure the load time of the radix tree */
	start_time = GetCurrentTimestamp();
	for (int i = 0; i < ntids; i++)
	{
		ItemPointer tid = &(tids[i]);
		uint32		off;

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

	rt_stats(rt);

	if (shuffle)
		shuffle_itemptrs(tids, ntids);

	elog(NOTICE, "sleeping for 2 seconds...");
	pg_usleep(2 * 1000000L);

	/* meaure the serach time of the radix tree */
	start_time = GetCurrentTimestamp();
	for (int i = 0; i < ntids; i++)
	{
		ItemPointer tid = &(tids[i]);
		uint32		off;
		volatile bool ret;		/* prevent calling rt_search from being
								 * optimized out */

		CHECK_FOR_INTERRUPTS();

		key = tid_to_key_off(tid, &off);

		ret = rt_search(rt, key, &val);
		(void) ret;
	}
	end_time = GetCurrentTimestamp();
	TimestampDifference(start_time, end_time, &secs, &usecs);
	rt_search_ms = secs * 1000 + usecs / 1000;

	MemSet(nulls, false, sizeof(nulls));
	values[0] = Int64GetDatum(rt_num_entries(rt));
	values[1] = Int64GetDatum(rt_memory_usage(rt));
	values[2] = Int64GetDatum(sizeof(ItemPointerData) * ntids);
	values[3] = Int64GetDatum(rt_load_ms);
	nulls[4] = true;			/* ar_load_ms */
	values[5] = Int64GetDatum(rt_search_ms);
	nulls[6] = true;			/* ar_search_ms */

#ifdef MEASURE_BINARY_SEARCH
	{
		ItemPointer itemptrs = NULL;

		int64		ar_load_ms,
					ar_search_ms;

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

		/* next, measure the serach time of the array */
		start_time = GetCurrentTimestamp();
		for (int i = 0; i < ntids; i++)
		{
			ItemPointer tid = &(tids[i]);
			volatile bool ret;	/* prevent calling bsearch from being
								 * optimized out */

			CHECK_FOR_INTERRUPTS();

			ret = bsearch((void *) tid,
						  (void *) itemptrs,
						  ntids,
						  sizeof(ItemPointerData),
						  vac_cmp_itemptr);
			(void) ret;
		}
		end_time = GetCurrentTimestamp();
		TimestampDifference(start_time, end_time, &secs, &usecs);
		ar_search_ms = secs * 1000 + usecs / 1000;

		/* set the result */
		nulls[4] = false;
		values[4] = Int64GetDatum(ar_load_ms);
		nulls[6] = false;
		values[6] = Int64GetDatum(ar_search_ms);
	}
#endif

	rt_free(rt);
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
	uint64		cnt = (uint64) PG_GETARG_INT64(0);
	radix_tree *rt;
	pg_prng_state state;
	TupleDesc	tupdesc;
	TimestampTz start_time,
				end_time;
	long		secs;
	int			usecs;
	int64		load_time_ms;
	Datum		values[2];
	bool		nulls[2];

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	pg_prng_seed(&state, 0);
	rt = rt_create(CurrentMemoryContext, NULL);

	start_time = GetCurrentTimestamp();
	for (uint64 i = 0; i < cnt; i++)
	{
		uint64		key = pg_prng_uint64(&state);

		rt_set(rt, key, key);
	}
	end_time = GetCurrentTimestamp();

	TimestampDifference(start_time, end_time, &secs, &usecs);
	load_time_ms = secs * 1000 + usecs / 1000;

	rt_stats(rt);

	MemSet(nulls, false, sizeof(nulls));
	values[0] = Int64GetDatum(rt_memory_usage(rt));
	values[1] = Int64GetDatum(load_time_ms);

	rt_free(rt);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/* copy of splitmix64() */
static uint64
hash64(uint64 x)
{
	x ^= x >> 30;
	x *= UINT64CONST(0xbf58476d1ce4e5b9);
	x ^= x >> 27;
	x *= UINT64CONST(0x94d049bb133111eb);
	x ^= x >> 31;
	return x;
}

/* attempts to have a relatively even population of node kinds */
Datum
bench_search_random_nodes(PG_FUNCTION_ARGS)
{
	uint64		cnt = (uint64) PG_GETARG_INT64(0);
	radix_tree *rt;
	TupleDesc	tupdesc;
	TimestampTz start_time,
				end_time;
	long		secs;
	int			usecs;
	int64		search_time_ms;
	Datum		values[2] = {0};
	bool		nulls[2] = {0};
	/* from trial and error */
	uint64 filter = (((uint64) 0x7F<<32) | (0x07<<24) | (0xFF<<16) | 0xFF);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	if (!PG_ARGISNULL(1))
	{
		char		*filter_str = text_to_cstring(PG_GETARG_TEXT_P(1));

		if (sscanf(filter_str, "0x%lX", &filter) == 0)
			elog(ERROR, "invalid filter string %s", filter_str);
	}
	elog(NOTICE, "bench with filter 0x%lX", filter);

	rt = rt_create(CurrentMemoryContext, NULL);

	for (uint64 i = 0; i < cnt; i++)
	{
		const uint64 hash = hash64(i);
		const uint64 key = hash & filter;

		rt_set(rt, key, key);
	}

	elog(NOTICE, "sleeping for 2 seconds...");
	pg_usleep(2 * 1000000L);

	start_time = GetCurrentTimestamp();
	for (uint64 i = 0; i < cnt; i++)
	{
		const uint64 hash = hash64(i);
		const uint64 key = hash & filter;
		uint64		val;
		volatile bool ret;		/* prevent calling rt_search from being
								 * optimized out */

		CHECK_FOR_INTERRUPTS();

		ret = rt_search(rt, key, &val);
		(void) ret;
	}
	end_time = GetCurrentTimestamp();

	TimestampDifference(start_time, end_time, &secs, &usecs);
	search_time_ms = secs * 1000 + usecs / 1000;

	rt_stats(rt);

	values[0] = Int64GetDatum(rt_memory_usage(rt));
	values[1] = Int64GetDatum(search_time_ms);

	rt_free(rt);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

Datum
bench_fixed_height_search(PG_FUNCTION_ARGS)
{
	int			fanout = PG_GETARG_INT32(0);
	radix_tree *rt;
	TupleDesc	tupdesc;
	TimestampTz start_time,
				end_time;
	long		secs;
	int			usecs;
	int64		rt_load_ms,
				rt_search_ms;
	Datum		values[5];
	bool		nulls[5];

	/* test boundary between vector and iteration */
	const int	n_keys = 5 * 16 * 16 * 16 * 16;
	uint64		r,
				h,
				i,
				j,
				k;
	int			key_id;

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	rt = rt_create(CurrentMemoryContext, NULL);

	start_time = GetCurrentTimestamp();

	key_id = 0;

	/*
	 * lower nodes have limited fanout, the top is only limited by
	 * bits-per-byte
	 */
	for (r = 1;; r++)
	{
		for (h = 1; h <= fanout; h++)
		{
			for (i = 1; i <= fanout; i++)
			{
				for (j = 1; j <= fanout; j++)
				{
					for (k = 1; k <= fanout; k++)
					{
						uint64		key;

						key = (r << 32) | (h << 24) | (i << 16) | (j << 8) | (k);

						CHECK_FOR_INTERRUPTS();

						key_id++;
						if (key_id > n_keys)
							goto finish_set;

						rt_set(rt, key, key_id);
					}
				}
			}
		}
	}
finish_set:
	end_time = GetCurrentTimestamp();
	TimestampDifference(start_time, end_time, &secs, &usecs);
	rt_load_ms = secs * 1000 + usecs / 1000;

	rt_stats(rt);

	/* meaure the search time of the radix tree */
	start_time = GetCurrentTimestamp();

	key_id = 0;
	for (r = 1;; r++)
	{
		for (h = 1; h <= fanout; h++)
		{
			for (i = 1; i <= fanout; i++)
			{
				for (j = 1; j <= fanout; j++)
				{
					for (k = 1; k <= fanout; k++)
					{
						uint64		key,
									val;

						key = (r << 32) | (h << 24) | (i << 16) | (j << 8) | (k);

						CHECK_FOR_INTERRUPTS();

						key_id++;
						if (key_id > n_keys)
							goto finish_search;

						rt_search(rt, key, &val);
					}
				}
			}
		}
	}
finish_search:
	end_time = GetCurrentTimestamp();
	TimestampDifference(start_time, end_time, &secs, &usecs);
	rt_search_ms = secs * 1000 + usecs / 1000;

	MemSet(nulls, false, sizeof(nulls));
	values[0] = Int32GetDatum(fanout);
	values[1] = Int64GetDatum(rt_num_entries(rt));
	values[2] = Int64GetDatum(rt_memory_usage(rt));
	values[3] = Int64GetDatum(rt_load_ms);
	values[4] = Int64GetDatum(rt_search_ms);

	rt_free(rt);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

Datum
bench_node128_load(PG_FUNCTION_ARGS)
{
	int			fanout = PG_GETARG_INT32(0);
	radix_tree *rt;
	TupleDesc	tupdesc;
	TimestampTz start_time,
				end_time;
	long		secs;
	int			usecs;
	int64		rt_sparseload_ms;
	Datum		values[5];
	bool		nulls[5];

	uint64		r,
				h;
	int			key_id;

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	rt = rt_create(CurrentMemoryContext, NULL);

	key_id = 0;

	for (r = 1; r <= fanout; r++)
	{
		for (h = 1; h <= fanout; h++)
		{
			uint64		key;

			key = (r << 8) | (h);

			key_id++;
			rt_set(rt, key, key_id);
		}
	}

	rt_stats(rt);

	/* measure sparse deletion and re-loading */
	start_time = GetCurrentTimestamp();

	for (int t = 0; t<10000; t++)
	{
		/* delete one key in each leaf */
		for (r = 1; r <= fanout; r++)
		{
			uint64		key;

			key = (r << 8) | (fanout);

			rt_delete(rt, key);
		}

		/* add them all back */
		key_id = 0;
		for (r = 1; r <= fanout; r++)
		{
			uint64		key;

			key = (r << 8) | (fanout);

			key_id++;
			rt_set(rt, key, key_id);
		}
	}
	end_time = GetCurrentTimestamp();
	TimestampDifference(start_time, end_time, &secs, &usecs);
	rt_sparseload_ms = secs * 1000 + usecs / 1000;

	MemSet(nulls, false, sizeof(nulls));
	values[0] = Int32GetDatum(fanout);
	values[1] = Int64GetDatum(rt_num_entries(rt));
	values[2] = Int64GetDatum(rt_memory_usage(rt));
	values[3] = Int64GetDatum(rt_sparseload_ms);

	rt_free(rt);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}
