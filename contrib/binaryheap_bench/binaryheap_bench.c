/*-------------------------------------------------------------------------
 *
 * binaryheap_bench.c
 *
 * Copyright (c) 2016-2024, PostgreSQL Global Development Group
 *
 *	  contrib/binaryheap_bench/binaryheap_bench.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "funcapi.h"
#include "miscadmin.h"

/*
 * This benchmark tool uses three binary heap implementations.
 *
 * "binaryheap" is the current binaryheap implementation in PostgreSQL. That
 * is, it internally has a hash table to track each node index within the
 * node array.
 *
 * "xx_binaryheap" is based on "binaryheap" but remove the hash table.
 * Instead, it has each element have its index with in the node array. The
 * element's index is updated by the callback function, xx_binaryheap_update_index_fn
 * specified when xx_binaryheap_allocate().
 *
 * "old_binaryheap" is the binaryheap implementation before the "indexed" binary
 * heap changes are made. It neither has a hash table internally nor tracks nodes'
 * indexes.
 */
#include "lib/binaryheap.h"
#include "xx_binaryheap.h"
#include "old_binaryheap.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(bench_load);
PG_FUNCTION_INFO_V1(bench_sift_down);

typedef struct test_elem
{
	int64		key;
	int			index; /* used only for xx_binaryheap */
} test_elem;

/* comparator for max-heap */
static int
test_elem_cmp(Datum a, Datum b, void *arg)
{
	test_elem *e1 = (test_elem *) DatumGetPointer(a);
	test_elem *e2 = (test_elem *) DatumGetPointer(b);

	if (e1->key < e2->key)
		return -1;
	else if (e1->key > e2->key)
		return 1;
	return 0;
}

static void
test_update_index(Datum a, int new_element_index)
{
	test_elem *e = (test_elem *) DatumGetPointer(a);
	e->index = new_element_index;
}

Datum
bench_load(PG_FUNCTION_ARGS)
{
	bool	indexed = PG_GETARG_BOOL(0);
	int64	cnt = PG_GETARG_INT64(1);
	test_elem	*values;
	binaryheap	*heap;
	xx_binaryheap *xx_heap;
	old_binaryheap *old_heap;
	TupleDesc	tupdesc;
	TimestampTz start_time, end_time;
	long		secs;
	int			usecs;
	int64		load_ms, xx_load_ms, old_load_ms;
	Datum	vals[4];
	bool	nulls[4];

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* generate test data */
	values = (test_elem *) palloc(sizeof(test_elem) * cnt);
	for (int64 i = 0; i < cnt; i++)
		values[i].key = i;

	heap = binaryheap_allocate(cnt, test_elem_cmp, indexed, NULL);
	xx_heap = xx_binaryheap_allocate(cnt, test_elem_cmp, NULL, test_update_index);
	old_heap = old_binaryheap_allocate(cnt, test_elem_cmp, NULL);

	/* measure load time of binaryheap */
	start_time = GetCurrentTimestamp();
	for (int64 i = 0; i < cnt; i++)
		binaryheap_add(heap, PointerGetDatum(&(values[i])));
	end_time = GetCurrentTimestamp();
	TimestampDifference(start_time, end_time, &secs, &usecs);
	load_ms = secs * 1000 + usecs / 1000;

	/* measure load time of binaryheap */
	start_time = GetCurrentTimestamp();
	for (int64 i = 0; i < cnt; i++)
		xx_binaryheap_add(xx_heap, PointerGetDatum(&(values[i])));
	end_time = GetCurrentTimestamp();
	TimestampDifference(start_time, end_time, &secs, &usecs);
	xx_load_ms = secs * 1000 + usecs / 1000;

	/* measure load time of old_binaryheap */
	start_time = GetCurrentTimestamp();
	for (int64 i = 0; i < cnt; i++)
		old_binaryheap_add(old_heap, PointerGetDatum(&(values[i])));
	end_time = GetCurrentTimestamp();
	TimestampDifference(start_time, end_time, &secs, &usecs);
	old_load_ms = secs * 1000 + usecs / 1000;

	MemSet(nulls, false, sizeof(nulls));
	vals[0] = Int64GetDatum(cnt);
	vals[1] = Int64GetDatum(load_ms);
	vals[2] = Int64GetDatum(xx_load_ms);
	vals[3] = Int64GetDatum(old_load_ms);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, vals, nulls)));
}

Datum
bench_sift_down(PG_FUNCTION_ARGS)
{
	int64	cnt = PG_GETARG_INT64(0);
	test_elem	*values;
	binaryheap	*heap;
	xx_binaryheap *xx_heap;
	TupleDesc	tupdesc;
	TimestampTz start_time, end_time;
	long		secs;
	int			usecs;
	int64		sift_ms, xx_sift_ms;
	Datum	vals[3];
	bool	nulls[3];
	test_elem * e;
	int64	old_key;

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* generate test data */
	values = (test_elem *) palloc(sizeof(test_elem) * cnt);
	for (int64 i = 0; i < cnt; i++)
		values[i].key = i;

	heap = binaryheap_allocate(cnt, test_elem_cmp, true, NULL);
	xx_heap = xx_binaryheap_allocate(cnt, test_elem_cmp, NULL, test_update_index);

	/*
	 * test for binaryheap.
	 *
	 * 1. load the test data.
	 * 2. measure the time of sifting down the top node while decreasing the key
	 */
	for (int64 i = 0; i < cnt; i++)
		binaryheap_add(heap, PointerGetDatum(&(values[i])));
	e = (test_elem *) DatumGetPointer(binaryheap_first(heap));
	old_key = e->key;
	start_time = GetCurrentTimestamp();
	for (int64 i = 0; i < cnt; i++)
	{
		e->key--;
		binaryheap_update_down(heap, PointerGetDatum(e));
	}
	end_time = GetCurrentTimestamp();
	TimestampDifference(start_time, end_time, &secs, &usecs);
	sift_ms = secs * 1000 + usecs / 1000;

	/* restore the old key */
	e->key = old_key;

	/*
	 * test for xx_binaryheap.
	 *
	 * 1. load the test data.
	 * 2. measure the time of sifting down the top node while decreasing the key
	 */
	for (int64 i = 0; i < cnt; i++)
		xx_binaryheap_add(xx_heap, PointerGetDatum(&(values[i])));
	e = (test_elem *) DatumGetPointer(xx_binaryheap_first(xx_heap));
	start_time = GetCurrentTimestamp();
	for (int64 i = 0; i < cnt; i++)
	{
		e->key--;
		xx_binaryheap_update_down(xx_heap, e->index);
	}
	end_time = GetCurrentTimestamp();
	TimestampDifference(start_time, end_time, &secs, &usecs);
	xx_sift_ms = secs * 1000 + usecs / 1000;

	MemSet(nulls, false, sizeof(nulls));
	vals[0] = Int64GetDatum(cnt);
	vals[1] = Int64GetDatum(sift_ms);
	vals[2] = Int64GetDatum(xx_sift_ms);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, vals, nulls)));
}
