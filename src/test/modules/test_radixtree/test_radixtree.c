/*--------------------------------------------------------------------------
 *
 * test_radixtree.c
 *		Test radixtree set data structure.
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_radixtree/test_radixtree.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/pg_prng.h"
#include "fmgr.h"
#include "lib/radixtree.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "storage/block.h"
#include "storage/itemptr.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#define UINT64_HEX_FORMAT "%" INT64_MODIFIER "X"

/*
 * If you enable this, the "pattern" tests will print information about
 * how long populating, probing, and iterating the test set takes, and
 * how much memory the test set consumed.  That can be used as
 * micro-benchmark of various operations and input patterns (you might
 * want to increase the number of values used in each of the test, if
 * you do that, to reduce noise).
 *
 * The information is printed to the server's stderr, mostly because
 * that's where MemoryContextStats() output goes.
 */
static const bool rt_test_stats = false;

/* The maximum number of entries each node type can have */
static int rt_node_max_entries[] = {
	4,		/* RT_NODE_KIND_4 */
	16,		/* RT_NODE_KIND_16 */
	32,		/* RT_NODE_KIND_32 */
	128,	/* RT_NODE_KIND_128 */
	256		/* RT_NODE_KIND_256 */
};

/*
 * A struct to define a pattern of integers, for use with the test_pattern()
 * function.
 */
typedef struct
{
	char	   *test_name;		/* short name of the test, for humans */
	char	   *pattern_str;	/* a bit pattern */
	uint64		spacing;		/* pattern repeats at this interval */
	uint64		num_values;		/* number of integers to set in total */
} test_spec;

/* Test patterns borrowed from test_integerset.c */
static const test_spec test_specs[] = {
	{
		"all ones", "1111111111",
		10, 1000000
	},
	{
		"alternating bits", "0101010101",
		10, 1000000
	},
	{
		"clusters of ten", "1111111111",
		10000, 1000000
	},
	{
		"clusters of hundred",
		"1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111",
		10000, 10000000
	},
	{
		"one-every-64k", "1",
		65536, 1000000
	},
	{
		"sparse", "100000000000000000000000000000001",
		10000000, 1000000
	},
	{
		"single values, distance > 2^32", "1",
		UINT64CONST(10000000000), 100000
	},
	{
		"clusters, distance > 2^32", "10101010",
		UINT64CONST(10000000000), 1000000
	},
	{
		"clusters, distance > 2^60", "10101010",
		UINT64CONST(2000000000000000000),
		23						/* can't be much higher than this, or we
								 * overflow uint64 */
	}
};

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_radixtree);

static void
test_empty(void)
{
	radix_tree *radixtree;
	uint64 dummy;

	radixtree = rt_create(CurrentMemoryContext);

	if (rt_search(radixtree, 0, &dummy))
		elog(ERROR, "rt_search on empty tree returned true");

	if (rt_search(radixtree, 1, &dummy))
		elog(ERROR, "rt_search on empty tree returned true");

	if (rt_search(radixtree, PG_UINT64_MAX, &dummy))
		elog(ERROR, "rt_search on empty tree returned true");

	if (rt_num_entries(radixtree) != 0)
		elog(ERROR, "rt_num_entries on empty tree return non-zero");

	rt_free(radixtree);
}

/*
 * Check if keys from start to end with the shift exist in the tree.
 */
static void
check_search_on_node(radix_tree *radixtree, uint8 shift, int start, int end)
{
	for (int i = start; i < end; i++)
	{
		uint64 key = ((uint64) i << shift);
		uint64 val;

		if (!rt_search(radixtree, key, &val))
			elog(ERROR, "key 0x" UINT64_HEX_FORMAT " is not found on node-%d",
				 key, end);
		if (val != key)
		{
			rt_dump(radixtree);
			elog(ERROR, "rt_search with key 0x" UINT64_HEX_FORMAT " returns 0x" UINT64_HEX_FORMAT ", expected 0x" UINT64_HEX_FORMAT,
				 key, val, key);
		}
	}
}

static void
test_node_types_insert(radix_tree *radixtree, uint8 shift)
{
	uint64 num_entries;

	for (int i = 0; i < 256; i++)
	{
		uint64 key = ((uint64) i << shift);
		bool found;

		found = rt_set(radixtree, key, key);

		if (found)
			elog(ERROR, "newly inserted key 0x" UINT64_HEX_FORMAT " found", key);

		for (int j = 0; j < lengthof(rt_node_max_entries); j++)
		{
			/*
			 * After filling all slots in each node type, check if the values are
			 * stored properly.
			 */
			if (i == (rt_node_max_entries[j] - 1))
			{
				check_search_on_node(radixtree, shift,
									 (j == 0) ? 0 : rt_node_max_entries[j - 1],
									 rt_node_max_entries[j]);
				break;
			}
		}
	}

	num_entries = rt_num_entries(radixtree);

	if (num_entries != 256)
		elog(ERROR,
			 "rt_num_entries returned" UINT64_FORMAT ", expected " UINT64_FORMAT,
			 num_entries, UINT64CONST(256));
}

static void
test_node_types_delete(radix_tree *radixtree, uint8 shift)
{
	uint64 num_entries;

	for (int i = 0; i < 256; i++)
	{
		uint64	key = ((uint64) i << shift);
		bool	found;

		found = rt_delete(radixtree, key);

		if (!found)
			elog(ERROR, "inserted key 0x" UINT64_HEX_FORMAT " is not found", key);
	}

	num_entries = rt_num_entries(radixtree);

	/* The tree must be empty */
	if (num_entries != 0)
		elog(ERROR,
			 "rt_num_entries returned" UINT64_FORMAT ", expected " UINT64_FORMAT,
			 num_entries, UINT64CONST(256));
}

/*
 * Test for inserting and deleting key-value pairs to each node type at the given shift
 * level.
 */
static void
test_node_types(uint8 shift)
{
	radix_tree *radixtree;

	elog(NOTICE, "testing radix tree node types with shift \"%d\"", shift);

	radixtree = rt_create(CurrentMemoryContext);

	/*
	 * Insert and search entries for every node type at the 'shift' level,
	 * then delete all entries to make it empty, and insert and search
	 * entries again.
	 */
	test_node_types_insert(radixtree, shift);
	test_node_types_delete(radixtree, shift);
	test_node_types_insert(radixtree, shift);

	rt_free(radixtree);
}

/*
 * Test with a repeating pattern, defined by the 'spec'.
 */
static void
test_pattern(const test_spec *spec)
{
	radix_tree *radixtree;
	rt_iter *iter;
	MemoryContext radixtree_ctx;
	TimestampTz starttime;
	TimestampTz endtime;
	uint64		n;
	uint64		last_int;
	uint64		ndeleted;
	uint64		nbefore;
	uint64		nafter;
	int			patternlen;
	uint64	   *pattern_values;
	uint64		pattern_num_values;

	elog(NOTICE, "testing radix tree with pattern \"%s\"", spec->test_name);
	if (rt_test_stats)
		fprintf(stderr, "-----\ntesting radix tree with pattern \"%s\"\n", spec->test_name);

	/* Pre-process the pattern, creating an array of integers from it. */
	patternlen = strlen(spec->pattern_str);
	pattern_values = palloc(patternlen * sizeof(uint64));
	pattern_num_values = 0;
	for (int i = 0; i < patternlen; i++)
	{
		if (spec->pattern_str[i] == '1')
			pattern_values[pattern_num_values++] = i;
	}

	/*
	 * Allocate the radix tree.
	 *
	 * Allocate it in a separate memory context, so that we can print its
	 * memory usage easily.
	 */
	radixtree_ctx = AllocSetContextCreate(CurrentMemoryContext,
										  "radixtree test",
										  ALLOCSET_SMALL_SIZES);
	MemoryContextSetIdentifier(radixtree_ctx, spec->test_name);
	radixtree = rt_create(radixtree_ctx);

	/*
	 * Add values to the set.
	 */
	starttime = GetCurrentTimestamp();

	n = 0;
	last_int = 0;
	while (n < spec->num_values)
	{
		uint64		x = 0;

		for (int i = 0; i < pattern_num_values && n < spec->num_values; i++)
		{
			bool found;

			x = last_int + pattern_values[i];

			found = rt_set(radixtree, x, x);

			if (found)
				elog(ERROR, "newly inserted key 0x" UINT64_HEX_FORMAT " found", x);

			n++;
		}
		last_int += spec->spacing;
	}

	endtime = GetCurrentTimestamp();

	if (rt_test_stats)
		fprintf(stderr, "added " UINT64_FORMAT " values in %d ms\n",
				spec->num_values, (int) (endtime - starttime) / 1000);

	/*
	 * Print stats on the amount of memory used.
	 *
	 * We print the usage reported by rt_memory_usage(), as well as the
	 * stats from the memory context.  They should be in the same ballpark,
	 * but it's hard to automate testing that, so if you're making changes to
	 * the implementation, just observe that manually.
	 */
	if (rt_test_stats)
	{
		uint64		mem_usage;

		/*
		 * Also print memory usage as reported by rt_memory_usage().  It
		 * should be in the same ballpark as the usage reported by
		 * MemoryContextStats().
		 */
		mem_usage = rt_memory_usage(radixtree);
		fprintf(stderr, "rt_memory_usage() reported " UINT64_FORMAT " (%0.2f bytes / integer)\n",
				mem_usage, (double) mem_usage / spec->num_values);

		MemoryContextStats(radixtree_ctx);
	}

	/* Check that rt_num_entries works */
	n = rt_num_entries(radixtree);
	if (n != spec->num_values)
		elog(ERROR, "rt_num_entries returned " UINT64_FORMAT ", expected " UINT64_FORMAT, n, spec->num_values);

	/*
	 * Test random-access probes with rt_search()
	 */
	starttime = GetCurrentTimestamp();

	for (n = 0; n < 100000; n++)
	{
		bool		found;
		bool		expected;
		uint64		x;
		uint64		v;

		/*
		 * Pick next value to probe at random.  We limit the probes to the
		 * last integer that we added to the set, plus an arbitrary constant
		 * (1000).  There's no point in probing the whole 0 - 2^64 range, if
		 * only a small part of the integer space is used.  We would very
		 * rarely hit values that are actually in the set.
		 */
		x = pg_prng_uint64_range(&pg_global_prng_state, 0, last_int + 1000);

		/* Do we expect this value to be present in the set? */
		if (x >= last_int)
			expected = false;
		else
		{
			uint64		idx = x % spec->spacing;

			if (idx >= patternlen)
				expected = false;
			else if (spec->pattern_str[idx] == '1')
				expected = true;
			else
				expected = false;
		}

		/* Is it present according to rt_search() ? */
		found = rt_search(radixtree, x, &v);

		if (found != expected)
			elog(ERROR, "mismatch at 0x" UINT64_HEX_FORMAT ": %d vs %d", x, found, expected);
		if (found && (v != x))
			elog(ERROR, "found 0x" UINT64_HEX_FORMAT ", expected 0x" UINT64_HEX_FORMAT,
				 v, x);
	}
	endtime = GetCurrentTimestamp();
	if (rt_test_stats)
		fprintf(stderr, "probed " UINT64_FORMAT " values in %d ms\n",
				n, (int) (endtime - starttime) / 1000);

	/*
	 * Test iterator
	 */
	starttime = GetCurrentTimestamp();

	iter = rt_begin_iterate(radixtree);
	n = 0;
	last_int = 0;
	while (n < spec->num_values)
	{
		for (int i = 0; i < pattern_num_values && n < spec->num_values; i++)
		{
			uint64		expected = last_int + pattern_values[i];
			uint64		x;
			uint64		val;

			if (!rt_iterate_next(iter, &x, &val))
				break;

			if (x != expected)
				elog(ERROR,
					 "iterate returned wrong key; got 0x" UINT64_HEX_FORMAT ", expected 0x" UINT64_HEX_FORMAT " at %d",
					 x, expected, i);
			if (val != expected)
				elog(ERROR,
					 "iterate returned wrong value; got 0x" UINT64_HEX_FORMAT ", expected 0x" UINT64_HEX_FORMAT " at %d", x, expected, i);
			n++;
		}
		last_int += spec->spacing;
	}
	endtime = GetCurrentTimestamp();
	if (rt_test_stats)
		fprintf(stderr, "iterated " UINT64_FORMAT " values in %d ms\n",
				n, (int) (endtime - starttime) / 1000);

	if (n < spec->num_values)
		elog(ERROR, "iterator stopped short after " UINT64_FORMAT " entries, expected " UINT64_FORMAT, n, spec->num_values);
	if (n > spec->num_values)
		elog(ERROR, "iterator returned " UINT64_FORMAT " entries, " UINT64_FORMAT " was expected", n, spec->num_values);

	/*
	 * Test random-access probes with rt_delete()
	 */
	starttime = GetCurrentTimestamp();

	nbefore = rt_num_entries(radixtree);
	ndeleted = 0;
	for (n = 0; n < 100000; n++)
	{
		bool		found;
		uint64		x;
		uint64		v;

		/*
		 * Pick next value to probe at random.  We limit the probes to the
		 * last integer that we added to the set, plus an arbitrary constant
		 * (1000).  There's no point in probing the whole 0 - 2^64 range, if
		 * only a small part of the integer space is used.  We would very
		 * rarely hit values that are actually in the set.
		 */
		x = pg_prng_uint64_range(&pg_global_prng_state, 0, last_int + 1000);

		/* Is it present according to rt_search() ? */
		found = rt_search(radixtree, x, &v);

		if (!found)
			continue;

		/* If the key is found, delete it and check again */
		if (!rt_delete(radixtree, x))
			elog(ERROR, "could not delete key 0x" UINT64_HEX_FORMAT, x);
		if (rt_search(radixtree, x, &v))
			elog(ERROR, "found deleted key 0x" UINT64_HEX_FORMAT, x);
		if (rt_delete(radixtree, x))
			elog(ERROR, "deleted already-deleted key 0x" UINT64_HEX_FORMAT, x);

		ndeleted++;
	}
	endtime = GetCurrentTimestamp();
	if (rt_test_stats)
		fprintf(stderr, "deleted " UINT64_FORMAT " values in %d ms\n",
				ndeleted, (int) (endtime - starttime) / 1000);

	nafter = rt_num_entries(radixtree);

	/* Check that rt_num_entries works */
	if ((nbefore - ndeleted) != nafter)
		elog(ERROR, "rt_num_entries returned " UINT64_FORMAT ", expected " UINT64_FORMAT "after " UINT64_FORMAT " deletion",
			 nafter, (nbefore - ndeleted), ndeleted);

	MemoryContextDelete(radixtree_ctx);
}

Datum
test_radixtree(PG_FUNCTION_ARGS)
{
	test_empty();

	for (int shift = 0; shift <= (64 - 8); shift += 8)
		test_node_types(shift);

	/* Test different test patterns, with lots of entries */
	for (int i = 0; i < lengthof(test_specs); i++)
		test_pattern(&test_specs[i]);

	PG_RETURN_VOID();
}
