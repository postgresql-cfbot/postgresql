/*--------------------------------------------------------------------------
 *
 * test_radixtree.c
 *		Test radixtree set data structure.
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_radixtree/test_radixtree.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/pg_prng.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "storage/block.h"
#include "storage/itemptr.h"
#include "storage/lwlock.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

/* #define TEST_SHARED_RT */

#define UINT64_HEX_FORMAT "%" INT64_MODIFIER "X"

/* Convenient macros to test results */
#define EXPECT_TRUE(expr)	\
	do { \
		if (!(expr)) \
			elog(ERROR, \
				 "%s was unexpectedly false in file \"%s\" line %u", \
				 #expr, __FILE__, __LINE__); \
	} while (0)

#define EXPECT_FALSE(expr)	\
	do { \
		if (expr) \
			elog(ERROR, \
				 "%s was unexpectedly true in file \"%s\" line %u", \
				 #expr, __FILE__, __LINE__); \
	} while (0)

#define EXPECT_EQ_U64(result_expr, expected_expr)	\
	do { \
		uint64		_result = (result_expr); \
		uint64		_expected = (expected_expr); \
		if (_result != _expected) \
			elog(ERROR, \
				 "%s yielded " UINT64_HEX_FORMAT ", expected " UINT64_HEX_FORMAT " (%s) in file \"%s\" line %u", \
				 #result_expr, _result, _expected, #expected_expr, __FILE__, __LINE__); \
	} while (0)

/*
 * The tests pass with uint32, but build with warnings because the string
 * format expects uint64.
 */
typedef uint64 TestValueType;

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

/*
 * The number of keys big enough to grow nodes into each size class.
 */
static int	rt_node_class_fanouts[] = {
	1,		/* RT_CLASS_4 */
	15,		/* RT_CLASS_16_LO */
	30, 	/* RT_CLASS_16_HI */
	100,	/* RT_CLASS_48 */
	255		/* RT_CLASS_256 */
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
}			test_spec;

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
		10000, 1000000
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

/* define the radix tree implementation to test */
#define RT_PREFIX rt
#define RT_SCOPE static pg_noinline
#define RT_DECLARE
#define RT_DEFINE
#define RT_USE_DELETE
#define RT_VALUE_TYPE TestValueType
#ifdef TEST_SHARED_RT
#define RT_SHMEM
#endif
#define RT_DEBUG
#include "lib/radixtree.h"


/*
 * Return the number of keys in the radix tree.
 */
static uint64
rt_num_entries(rt_radix_tree *tree)
{
	return tree->ctl->num_keys;
}

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_radixtree);

static void
test_empty(void)
{
	rt_radix_tree *radixtree;
	rt_iter		*iter;
	uint64		key;

#ifdef TEST_SHARED_RT
	int			tranche_id = LWLockNewTrancheId();
	dsa_area   *dsa;

	LWLockRegisterTranche(tranche_id, "test_radix_tree");
	dsa = dsa_create(tranche_id);

	radixtree = rt_create(CurrentMemoryContext, work_mem, dsa, tranche_id);
#else
	radixtree = rt_create(CurrentMemoryContext, work_mem);
#endif

	/* Should not find anything on an empty tree */
	EXPECT_TRUE(rt_find(radixtree, 0) == NULL);
	EXPECT_TRUE(rt_find(radixtree, 1) == NULL);
	EXPECT_TRUE(rt_find(radixtree, PG_UINT64_MAX) == NULL);
	EXPECT_FALSE(rt_delete(radixtree, 0));
	EXPECT_TRUE(rt_num_entries(radixtree) == 0);

	/* The iteration on an empty tree should not return anything */
	iter = rt_begin_iterate(radixtree);
	EXPECT_TRUE(rt_iterate_next(iter, &key) == NULL);
	rt_end_iterate(iter);

	rt_free(radixtree);

#ifdef TEST_SHARED_RT
	dsa_detach(dsa);
#endif
}

static void
test_basic(int children, int height, bool reverse)
{
	rt_radix_tree	*radixtree;
	rt_iter    *iter;
	uint64 *keys;
	int shift = height * 8;

#ifdef TEST_SHARED_RT
	int			tranche_id = LWLockNewTrancheId();
	dsa_area   *dsa;

	LWLockRegisterTranche(tranche_id, "test_radix_tree");
	dsa = dsa_create(tranche_id);
#endif

	elog(NOTICE, "testing node %3d with height %d and %s keys",
		children, height, reverse ? "descending" : " ascending");

#ifdef TEST_SHARED_RT
	radixtree = rt_create(CurrentMemoryContext, work_mem, dsa, tranche_id);
#else
	radixtree = rt_create(CurrentMemoryContext, work_mem);
#endif

	keys = palloc(sizeof(uint64) * children);
	for (int i = 0; i < children; i++)
			keys[i] = (uint64) i << shift;

	/*
	 * Insert keys. Since the tree is empty yet, rt_set should return
	 * false.
	 */
	if (reverse)
	{
		for (int i = children - 1; i >= 0; i--)
			EXPECT_FALSE(rt_set(radixtree, keys[i], (TestValueType*) &keys[i]));
	}
	else
	{
		for (int i = 0; i < children; i++)
			EXPECT_FALSE(rt_set(radixtree, keys[i], (TestValueType*) &keys[i]));
	}

	rt_stats(radixtree);

	/* look up keys */
	for (int i = 0; i < children; i++)
	{
		TestValueType *value;

		value = rt_find(radixtree, keys[i]);

		/* Test rt_find returns the expected value */
		EXPECT_TRUE(value != NULL);
		EXPECT_EQ_U64(*value, (TestValueType) keys[i]);
	}

	/* update keys */
	for (int i = 0; i < children; i++)
	{
		TestValueType update = keys[i] + 1;

		/* rt_set should find the key and update it */
		EXPECT_TRUE(rt_set(radixtree, keys[i], (TestValueType*) &update));
	}

	/* repeat deleting and inserting keys */
	for (int i = 0; i < children; i++)
	{
		EXPECT_TRUE(rt_delete(radixtree, keys[i]));
		EXPECT_FALSE(rt_set(radixtree, keys[i], (TestValueType*) &keys[i]));
	}

	/* look up keys after deleting and re-inserting */
	for (int i = 0; i < children; i++)
	{
		TestValueType *value;

		value = rt_find(radixtree, keys[i]);

		/* Test rt_find returns the expected value */
		EXPECT_TRUE(value != NULL);
		EXPECT_EQ_U64(*value, (TestValueType) keys[i]);
	}

	/* iterate over the tree */
	iter = rt_begin_iterate(radixtree);

	for (int i = 0; i < children; i++)
	{
		uint64		expected = keys[i];
		uint64		iterkey;
		TestValueType		*iterval;

		iterval = rt_iterate_next(iter, &iterkey);

		/* Test if the iteration returns the expected keys and values */
		EXPECT_TRUE(iterval != NULL);
		EXPECT_EQ_U64(iterkey, expected);
		EXPECT_EQ_U64(*iterval, expected);
	}

	rt_end_iterate(iter);


	/* delete all keys again */
	for (int i = 0; i < children; i++)
		EXPECT_TRUE(rt_delete(radixtree, keys[i]));

	/* test if all keys are deleted */
	for (int i = 0; i < children; i++)
		EXPECT_TRUE(rt_find(radixtree, keys[i]) == NULL);

	rt_stats(radixtree);

	pfree(keys);
	rt_free(radixtree);
#ifdef TEST_SHARED_RT
	dsa_detach(dsa);
#endif
}

/*
 * Check if keys from start to end with the shift exist in the tree.
 */
static void
check_search_on_node(rt_radix_tree *radixtree, uint8 shift, int start, int end)
{
	for (int i = start; i <= end; i++)
	{
		uint64		key = ((uint64) i << shift);
		TestValueType		*val;

		val = rt_find(radixtree, key);

		EXPECT_TRUE(val != NULL);
		EXPECT_EQ_U64(*val, (TestValueType) key);
	}
}

/*
 * Insert 256 key-value pairs, and check if keys are properly inserted on each
 * node class.
 */
/* Test keys [0, 256) */
#define NODE_TYPE_TEST_KEY_MIN 0
#define NODE_TYPE_TEST_KEY_MAX 256
static void
test_node_types_insert_asc(rt_radix_tree *radixtree, uint8 shift)
{
	int node_class_idx = 0;
	uint64 key_checked = 0;

	for (int i = NODE_TYPE_TEST_KEY_MIN; i < NODE_TYPE_TEST_KEY_MAX; i++)
	{
		uint64		key = ((uint64) i << shift);

		/* rt_set should not find the existing key */
		EXPECT_FALSE(rt_set(radixtree, key, (TestValueType *) &key));

		/*
		 * After filling all slots in each node type, check if the values
		 * are stored properly.
		 */
		if (i == rt_node_class_fanouts[node_class_idx])
		{
			check_search_on_node(radixtree, shift, key_checked, i);
			key_checked = i;
			node_class_idx++;
		}
	}

	EXPECT_EQ_U64(rt_num_entries(radixtree), 256);
}

/*
 * Similar to test_node_types_insert_asc(), but inserts keys in descending order.
 */
static void
test_node_types_insert_desc(rt_radix_tree *radixtree, uint8 shift)
{
	int node_class_idx = 0;
	uint64 key_checked = NODE_TYPE_TEST_KEY_MAX - 1;

	for (int i = NODE_TYPE_TEST_KEY_MAX - 1; i >= NODE_TYPE_TEST_KEY_MIN; i--)
	{
		uint64		key = ((uint64) i << shift);

		EXPECT_FALSE(rt_set(radixtree, key, (TestValueType *) &key));

		if ((i + 1) == rt_node_class_fanouts[node_class_idx])
		{
			check_search_on_node(radixtree, shift, i, key_checked);
			key_checked = i;
			node_class_idx++;
		}
	}

	EXPECT_EQ_U64(rt_num_entries(radixtree), 256);
}

static void pg_attribute_unused()
test_node_types_delete(rt_radix_tree *radixtree, uint8 shift)
{
	for (int i = NODE_TYPE_TEST_KEY_MIN; i < NODE_TYPE_TEST_KEY_MAX; i++)
	{
		uint64		key = ((uint64) i << shift);

		EXPECT_TRUE(rt_delete(radixtree, key));
	}

	/* The tree must be empty */
	EXPECT_EQ_U64(rt_num_entries(radixtree), 0);
}

/*
 * Test for inserting and deleting key-value pairs to each node type at the given shift
 * level.
 */
static void pg_attribute_unused()
test_node_types(uint8 shift)
{
	rt_radix_tree *radixtree;

#ifdef TEST_SHARED_RT
	int			tranche_id = LWLockNewTrancheId();
	dsa_area   *dsa;

	LWLockRegisterTranche(tranche_id, "test_radix_tree");
	dsa = dsa_create(tranche_id);
#endif

	elog(NOTICE, "testing radix tree node types with shift \"%d\"", shift);

#ifdef TEST_SHARED_RT
	radixtree = rt_create(CurrentMemoryContext, work_mem, dsa, tranche_id);
#else
	radixtree = rt_create(CurrentMemoryContext, work_mem);
#endif

	/*
	 * Insert and search entries for every node type at the 'shift' level,
	 * then delete all entries to make it empty, and insert and search entries
	 * again.
	 */
	test_node_types_insert_asc(radixtree, shift);
	test_node_types_delete(radixtree, shift);
	test_node_types_insert_desc(radixtree, shift);

	rt_free(radixtree);
#ifdef TEST_SHARED_RT
	dsa_detach(dsa);
#endif
}

/*
 * Test with a repeating pattern, defined by the 'spec'.
 */
static void
test_pattern(const test_spec * spec)
{
	rt_radix_tree *radixtree;
	rt_iter    *iter;
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
#ifdef TEST_SHARED_RT
	int			tranche_id = LWLockNewTrancheId();
	dsa_area   *dsa;

	LWLockRegisterTranche(tranche_id, "test_radix_tree");
	dsa = dsa_create(tranche_id);
#endif

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

#ifdef TEST_SHARED_RT
	radixtree = rt_create(radixtree_ctx, work_mem, dsa, tranche_id);
#else
	radixtree = rt_create(radixtree_ctx, work_mem);
#endif


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
			x = last_int + pattern_values[i];

			EXPECT_FALSE(rt_set(radixtree, x, (TestValueType*) &x));
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
	 * We print the usage reported by rt_memory_usage(), as well as the stats
	 * from the memory context.  They should be in the same ballpark, but it's
	 * hard to automate testing that, so if you're making changes to the
	 * implementation, just observe that manually.
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
	EXPECT_EQ_U64(rt_num_entries(radixtree), spec->num_values);

	/*
	 * Test random-access probes with rt_search()
	 */
	starttime = GetCurrentTimestamp();

	for (n = 0; n < 100000; n++)
	{
		bool		found;
		bool		expected;
		uint64		x;
		TestValueType		*v;

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
		v = rt_find(radixtree, x);
		found = (v != NULL);

		EXPECT_TRUE(found == expected);
		if (found)
			EXPECT_EQ_U64(*v, (TestValueType) x);
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
			TestValueType		*val;

			val = rt_iterate_next(iter, &x);
			if (val == NULL)
				break;

			EXPECT_EQ_U64(x, expected);
			EXPECT_EQ_U64(*val, (TestValueType) expected);

			n++;
		}
		last_int += spec->spacing;
	}
	endtime = GetCurrentTimestamp();
	if (rt_test_stats)
		fprintf(stderr, "iterated " UINT64_FORMAT " values in %d ms\n",
				n, (int) (endtime - starttime) / 1000);

	rt_end_iterate(iter);

	/* iterator returned the expected number of entries */
	EXPECT_EQ_U64(n, spec->num_values);

	/*
	 * Test random-access probes with rt_delete()
	 */
	starttime = GetCurrentTimestamp();

	nbefore = rt_num_entries(radixtree);
	ndeleted = 0;
	for (n = 0; n < 1; n++)
	{
		uint64		x;
		TestValueType		*v;

		/*
		 * Pick next value to probe at random.  We limit the probes to the
		 * last integer that we added to the set, plus an arbitrary constant
		 * (1000).  There's no point in probing the whole 0 - 2^64 range, if
		 * only a small part of the integer space is used.  We would very
		 * rarely hit values that are actually in the set.
		 */
		x = pg_prng_uint64_range(&pg_global_prng_state, 0, last_int + 1000);

		/* Is it present according to rt_find() ? */
		v = rt_find(radixtree, x);

		if (!v)
			continue;

		/* If the key is found, delete it and check again */
		EXPECT_TRUE(rt_delete(radixtree, x));
		EXPECT_TRUE(rt_find(radixtree, x) == NULL);
		EXPECT_FALSE(rt_delete(radixtree, x));

		ndeleted++;
	}
	endtime = GetCurrentTimestamp();
	if (rt_test_stats)
		fprintf(stderr, "deleted " UINT64_FORMAT " values in %d ms\n",
				ndeleted, (int) (endtime - starttime) / 1000);

	nafter = rt_num_entries(radixtree);

	/* Check that rt_num_entries works */
	EXPECT_EQ_U64(nbefore - ndeleted, nafter);

	rt_free(radixtree);
	MemoryContextDelete(radixtree_ctx);
#ifdef TEST_SHARED_RT
	dsa_detach(dsa);
#endif
}

Datum
test_radixtree(PG_FUNCTION_ARGS)
{
	test_empty();

	for (int i = 0; i < lengthof(rt_node_class_fanouts); i++)
	{
		test_basic(rt_node_class_fanouts[i], 0, false);
		test_basic(rt_node_class_fanouts[i], 0, true);
		test_basic(rt_node_class_fanouts[i], 1, false);
		test_basic(rt_node_class_fanouts[i], 1, true);
	}

	for (int shift = 0; shift <= (64 - 8); shift += 8)
		test_node_types(shift);

	/* Test different test patterns, with lots of entries */
	for (int i = 0; i < lengthof(test_specs); i++)
		test_pattern(&test_specs[i]);

	PG_RETURN_VOID();
}
