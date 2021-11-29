/*-------------------------------------------------------------------------
 *
 * generation_bench.c
 *
 * helper functions to benchmark generation context with different workloads
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/time.h>
#include <x86intrin.h>

#include "funcapi.h"
#include "miscadmin.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(generation_bench_random);
PG_FUNCTION_INFO_V1(generation_bench_fifo);
PG_FUNCTION_INFO_V1(generation_bench_lifo);

typedef struct Chunk {
	int		random;
	void   *ptr;
} Chunk;

static int
chunk_index_cmp(const void *a, const void *b)
{
	Chunk *ca = (Chunk *) a;
	Chunk *cb = (Chunk *) b;

	if (ca->random < cb->random)
		return -1;
	else if (ca->random > cb->random)
		return 1;

	return 0;
}

#ifdef RDTSC
// optional wrapper if you don't want to just use __rdtsc() everywhere
static inline
unsigned long long read_time(void)
{
	// _mm_lfence();  // optionally wait for earlier insns to retire before reading the clock
	return __rdtsc();
	// _mm_lfence();  // optionally block later instructions until rdtsc retires
}
#else
static inline
unsigned long long read_time(void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);

	return (unsigned long long) tv.tv_sec * 1000000L + tv.tv_usec;
}
#endif

Datum
generation_bench_random(PG_FUNCTION_ARGS)
{
	MemoryContext	cxt,
					oldcxt;
	Chunk		   *chunks;
	int64			i, j;
	int64			nallocs = PG_GETARG_INT64(0);
	int64			blockSize = PG_GETARG_INT64(1);
	int64			minChunkSize = PG_GETARG_INT64(2);
	int64			maxChunkSize = PG_GETARG_INT64(3);

	int				nloops = PG_GETARG_INT32(4);
	int				free_cnt = PG_GETARG_INT32(5);
	int				alloc_cnt = PG_GETARG_INT32(6);

	unsigned long long	start_time,
						end_time;

	int64			alloc_time = 0,
					free_time = 0;
	int64			mem_allocated;

	TupleDesc		tupdesc;
	Datum			result;
	HeapTuple		tuple;
	Datum			values[9];
	bool			nulls[9];

	int				maxchunks;

	maxchunks = nallocs + nloops * Max(0, alloc_cnt - free_cnt);

	cxt = GenerationContextCreate(CurrentMemoryContext, "generation_bench", blockSize, blockSize, 1024L * 1024L);

	chunks = (Chunk *) palloc(maxchunks * sizeof(Chunk));

	/* allocate the chunks in random order */
	oldcxt = MemoryContextSwitchTo(cxt);

	start_time = read_time();

	for (i = 0; i < nallocs; i++)
	{
		int chunkSize = minChunkSize + random() % (maxChunkSize - minChunkSize);

		chunks[i].ptr = palloc(chunkSize);
	}

	end_time = read_time();

	alloc_time += (end_time - start_time);

	MemoryContextSwitchTo(oldcxt);

	mem_allocated = MemoryContextMemAllocated(cxt, true);

	/* do the requested number of free/alloc loops */
	for (j = 0; j < nloops; j++)
	{
		CHECK_FOR_INTERRUPTS();

		/* randomize the indexes */
		for (i = 0; i < nallocs; i++)
			chunks[i].random = random();

		qsort(chunks, nallocs, sizeof(Chunk), chunk_index_cmp);

		oldcxt = MemoryContextSwitchTo(cxt);

		start_time = read_time();

		/* free the first free_cnt chunks */
		for (i = 0; i < Min(nallocs, free_cnt); i++)
			pfree(chunks[i].ptr);

		end_time = read_time();

		nallocs -= Min(nallocs, free_cnt);

		free_time += (end_time - start_time);

		memmove(chunks, &chunks[free_cnt], nallocs * sizeof(Chunk));


		/* allocate alloc_cnt chunks at the end */
		start_time = read_time();

		/* free the first free_cnt chunks */
		for (i = 0; i < alloc_cnt; i++)
		{
			int chunkSize = minChunkSize + random() % (maxChunkSize - minChunkSize);

			chunks[nallocs + i].ptr = palloc(chunkSize);
		}

		end_time = read_time();

		nallocs += alloc_cnt;

		alloc_time += (end_time - start_time);

		MemoryContextSwitchTo(oldcxt);

		mem_allocated = Max(mem_allocated, MemoryContextMemAllocated(cxt, true));
	}

	/* release the chunks in random order */
	for (i = 0; i < nallocs; i++)
		chunks[i].random = random();

	qsort(chunks, nallocs, sizeof(Chunk), chunk_index_cmp);

	start_time = read_time();

	for (i = 0; i < nallocs; i++)
		pfree(chunks[i].ptr);

	end_time = read_time();

	free_time += (end_time - start_time);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	values[0] = Int64GetDatum(mem_allocated);
	values[1] = Int64GetDatum(alloc_time);
	values[2] = Int64GetDatum(free_time);

	memset(nulls, 0, sizeof(nulls));

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

Datum
generation_bench_fifo(PG_FUNCTION_ARGS)
{
	MemoryContext	cxt,
					oldcxt;
	Chunk		   *chunks;
	int64			i, j;
	int64			nallocs = PG_GETARG_INT64(0);
	int64			blockSize = PG_GETARG_INT64(1);
	int64			minChunkSize = PG_GETARG_INT64(2);
	int64			maxChunkSize = PG_GETARG_INT64(3);

	int				nloops = PG_GETARG_INT32(4);
	int				free_cnt = PG_GETARG_INT32(5);
	int				alloc_cnt = PG_GETARG_INT32(6);

	unsigned long long	start_time,
						end_time;

	int64			alloc_time = 0,
					free_time = 0;
	int64			mem_allocated;

	TupleDesc		tupdesc;
	Datum			result;
	HeapTuple		tuple;
	Datum			values[9];
	bool			nulls[9];

	int				maxchunks;

	maxchunks = nallocs + nloops * Max(0, alloc_cnt - free_cnt);

	cxt = GenerationContextCreate(CurrentMemoryContext, "generation_bench", blockSize, blockSize, 1024L * 1024L);

	chunks = (Chunk *) palloc(maxchunks * sizeof(Chunk));

	oldcxt = MemoryContextSwitchTo(cxt);

	start_time = read_time();

	for (i = 0; i < nallocs; i++)
	{
		int chunkSize = minChunkSize + random() % (maxChunkSize - minChunkSize);

		chunks[i].ptr = palloc(chunkSize);
	}

	end_time = read_time();

	alloc_time += (end_time - start_time);

	MemoryContextSwitchTo(oldcxt);

	mem_allocated = MemoryContextMemAllocated(cxt, true);


	/* do the requested number of free/alloc loops */
	for (j = 0; j < nloops; j++)
	{
		CHECK_FOR_INTERRUPTS();

		oldcxt = MemoryContextSwitchTo(cxt);

		start_time = read_time();

		/* free the first free_cnt chunks */
		for (i = 0; i < Min(nallocs, free_cnt); i++)
			pfree(chunks[i].ptr);

		end_time = read_time();

		nallocs -= Min(nallocs, free_cnt);

		free_time += (end_time - start_time);

		memmove(chunks, &chunks[free_cnt], nallocs * sizeof(Chunk));

		/* allocate alloc_cnt chunks at the end */
		start_time = read_time();

		/* free the first free_cnt chunks */
		for (i = 0; i < alloc_cnt; i++)
		{
			int chunkSize = minChunkSize + random() % (maxChunkSize - minChunkSize);

			chunks[nallocs + i].ptr = palloc(chunkSize);
		}

		end_time = read_time();

		nallocs += alloc_cnt;

		alloc_time += (end_time - start_time);

		MemoryContextSwitchTo(oldcxt);

		mem_allocated = Max(mem_allocated, MemoryContextMemAllocated(cxt, true));
	}

	start_time = read_time();

	for (i = 0; i < nallocs; i++)
		pfree(chunks[i].ptr);

	end_time = read_time();

	free_time += (end_time - start_time);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	values[0] = Int64GetDatum(mem_allocated);
	values[1] = Int64GetDatum(alloc_time);
	values[2] = Int64GetDatum(free_time);

	memset(nulls, 0, sizeof(nulls));

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

Datum
generation_bench_lifo(PG_FUNCTION_ARGS)
{
	MemoryContext	cxt,
					oldcxt;
	Chunk		  *chunks;
	int64			i, j;
	int64			nallocs = PG_GETARG_INT64(0);
	int64			blockSize = PG_GETARG_INT64(1);
	int64			minChunkSize = PG_GETARG_INT64(2);
	int64			maxChunkSize = PG_GETARG_INT64(3);

	int				nloops = PG_GETARG_INT32(4);
	int				free_cnt = PG_GETARG_INT32(5);
	int				alloc_cnt = PG_GETARG_INT32(6);

	unsigned long long	start_time,
						end_time;

	int64			alloc_time = 0,
					free_time = 0;
	int64			mem_allocated;

	TupleDesc		tupdesc;
	Datum			result;
	HeapTuple		tuple;
	Datum			values[9];
	bool			nulls[9];

	int				maxchunks;

	maxchunks = nallocs + nloops * Max(0, alloc_cnt - free_cnt);

	cxt = GenerationContextCreate(CurrentMemoryContext, "generation_bench", blockSize, blockSize, 1024L * 1024L);

	chunks = (Chunk *) palloc(maxchunks * sizeof(Chunk));

	oldcxt = MemoryContextSwitchTo(cxt);

	/* palloc benchmark */
	start_time = read_time();

	for (i = 0; i < nallocs; i++)
	{
		int chunkSize = minChunkSize + random() % (maxChunkSize - minChunkSize);

		chunks[i].ptr = palloc(chunkSize);
	}

	end_time = read_time();

	alloc_time += (end_time - start_time);

	MemoryContextSwitchTo(oldcxt);

	mem_allocated = MemoryContextMemAllocated(cxt, true);


	/* do the requested number of free/alloc loops */
	for (j = 0; j < nloops; j++)
	{
		CHECK_FOR_INTERRUPTS();

		oldcxt = MemoryContextSwitchTo(cxt);

		start_time = read_time();

		/* free the first free_cnt chunks */
		for (i = 1; i <= Min(nallocs, free_cnt); i++)
			pfree(chunks[nallocs - i].ptr);

		end_time = read_time();

		nallocs -= Min(nallocs, free_cnt);

		free_time += (end_time - start_time);

		/* allocate alloc_cnt chunks at the end */
		start_time = read_time();

		/* free the first free_cnt chunks */
		for (i = 0; i < alloc_cnt; i++)
		{
			int chunkSize = minChunkSize + random() % (maxChunkSize - minChunkSize);

			chunks[nallocs + i].ptr = palloc(chunkSize);
		}

		end_time = read_time();

		nallocs += alloc_cnt;

		alloc_time += (end_time - start_time);

		MemoryContextSwitchTo(oldcxt);

		mem_allocated = Max(mem_allocated, MemoryContextMemAllocated(cxt, true));
	}


	start_time = read_time();

	for (i = (nallocs - 1); i >= 0; i--)
		pfree(chunks[i].ptr);

	end_time = read_time();

	free_time += (end_time - start_time);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	values[0] = Int64GetDatum(mem_allocated);
	values[1] = Int64GetDatum(alloc_time);
	values[2] = Int64GetDatum(free_time);

	memset(nulls, 0, sizeof(nulls));

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	MemoryContextDelete(cxt);

	PG_RETURN_DATUM(result);
}
