/*-------------------------------------------------------------------------
 *
 * tidstore.c
 *		TID (ItemPointer) storage implementation.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/common/tidstore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tidstore.h"
#include "lib/radixtree.h"
#include "utils/dsa.h"
#include "utils/memutils.h"
#include "miscadmin.h"

#define XXX_DEBUG_TID_STORE 1

/* XXX: should be configurable for non-heap AMs */
#define TIDSTORE_OFFSET_NBITS 11	/* pg_ceil_log2_32(MaxHeapTuplesPerPage) */

#define TIDSTORE_VALUE_NBITS 6	/* log(sizeof(uint64) * BITS_PER_BYTE, 2) */

/* Get block number from the key */
#define KEY_GET_BLKNO(key) \
	((BlockNumber) ((key) >> (TIDSTORE_OFFSET_NBITS - TIDSTORE_VALUE_NBITS)))

struct TIDStore
{
	/* main storage for TID */
	radix_tree	*tree;

	/* # of tids in TIDStore */
	int	num_tids;

	/* DSA area and handle for shared TIDStore */
	rt_handle	handle;
	dsa_area	*area;

#if defined(USE_ASSERT_CHECKING) && defined(XXX_DEBUG_TID_STORE)
	ItemPointer	itemptrs;
	uint64	nitems;
#endif
};

static void tidstore_iter_collect_tids(TIDStoreIter *iter, uint64 key, uint64 val);
static inline uint64 tid_to_key_off(ItemPointer tid, uint32 *off);

#if defined(USE_ASSERT_CHECKING) && defined(XXX_DEBUG_TID_STORE)
/*
 * Comparator routines for use with qsort() and bsearch().
 */
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

static void
verify_iter_tids(TIDStoreIter *iter)
{
	uint64 index = iter->prev_index;

	if (iter->ts->itemptrs == NULL)
		return;

	Assert(index <= iter->ts->nitems);

	for (int i = 0; i < iter->num_offsets; i++)
	{
		ItemPointerData tid;

		ItemPointerSetBlockNumber(&tid, iter->blkno);
		ItemPointerSetOffsetNumber(&tid, iter->offsets[i]);

		Assert(ItemPointerEquals(&iter->ts->itemptrs[index++], &tid));
	}

	iter->prev_index = iter->itemptrs_index;
}

static void
dump_itemptrs(TIDStore *ts)
{
	StringInfoData buf;

	if (ts->itemptrs == NULL)
		return;

	initStringInfo(&buf);
	for (int i = 0; i < ts->nitems; i++)
	{
		appendStringInfo(&buf, "(%d,%d) ",
						 ItemPointerGetBlockNumber(&(ts->itemptrs[i])),
						 ItemPointerGetOffsetNumber(&(ts->itemptrs[i])));
	}
	elog(WARNING, "--- dump (" UINT64_FORMAT " items) ---", ts->nitems);
	elog(WARNING, "%s\n", buf.data);
}

#endif

/*
 * Create a TIDStore. The returned object is allocated in backend-local memory.
 * The radix tree for storage is allocated in DSA area is 'area' is non-NULL.
 */
TIDStore *
tidstore_create(dsa_area *area)
{
	TIDStore	*ts;

	ts = palloc0(sizeof(TIDStore));

	ts->tree = rt_create(CurrentMemoryContext, area);
	ts->area = area;

	if (area != NULL)
		ts->handle = rt_get_handle(ts->tree);

#if defined(USE_ASSERT_CHECKING) && defined(XXX_DEBUG_TID_STORE)
#define MAXDEADITEMS(avail_mem) \
	(avail_mem / sizeof(ItemPointerData))

	if (area == NULL)
	{
		ts->itemptrs = (ItemPointer) palloc0(sizeof(ItemPointerData) *
											 MAXDEADITEMS(maintenance_work_mem * 1024));
		ts->nitems = 0;
	}
#endif

	return ts;
}

/* Attach to the shared TIDStore using a handle */
TIDStore *
tidstore_attach(dsa_area *area, rt_handle handle)
{
	TIDStore *ts;

	Assert(area != NULL);
	Assert(DsaPointerIsValid(handle));

	ts = palloc0(sizeof(TIDStore));
	ts->tree = rt_attach(area, handle);

	return ts;
}

/*
 * Detach from a TIDStore. This detaches from radix tree and frees the
 * backend-local resources.
 */
void
tidstore_detach(TIDStore *ts)
{
	rt_detach(ts->tree);
	pfree(ts);
}

void
tidstore_free(TIDStore *ts)
{
#if defined(USE_ASSERT_CHECKING) && defined(XXX_DEBUG_TID_STORE)
	if (ts->itemptrs)
		pfree(ts->itemptrs);
#endif

	rt_free(ts->tree);
	pfree(ts);
}

void
tidstore_reset(TIDStore *ts)
{
	dsa_area *area = ts->area;

	/* Reset the statistics */
	ts->num_tids = 0;

	/* Recreate radix tree storage */
	rt_free(ts->tree);
	ts->tree = rt_create(CurrentMemoryContext, area);

#if defined(USE_ASSERT_CHECKING) && defined(XXX_DEBUG_TID_STORE)
	ts->nitems = 0;
#endif
}

/* Add TIDs to TIDStore */
void
tidstore_add_tids(TIDStore *ts, BlockNumber blkno, OffsetNumber *offsets,
				  int num_offsets)
{
	uint64 last_key = PG_UINT64_MAX;
	uint64 key;
	uint64 val = 0;
	ItemPointerData tid;

	ItemPointerSetBlockNumber(&tid, blkno);

	for (int i = 0; i < num_offsets; i++)
	{
		uint32	off;

		ItemPointerSetOffsetNumber(&tid, offsets[i]);

		key = tid_to_key_off(&tid, &off);

		if (last_key != PG_UINT64_MAX && last_key != key)
		{
			rt_set(ts->tree, last_key, val);
			val = 0;
		}

		last_key = key;
		val |= UINT64CONST(1) << off;
		ts->num_tids++;

#if defined(USE_ASSERT_CHECKING) && defined(XXX_DEBUG_TID_STORE)
		if (ts->itemptrs)
		{
			ItemPointerSetBlockNumber(&(ts->itemptrs[ts->nitems]), blkno);
			ItemPointerSetOffsetNumber(&(ts->itemptrs[ts->nitems]), offsets[i]);
			ts->nitems++;
		}
#endif
	}

	if (last_key != PG_UINT64_MAX)
	{
		rt_set(ts->tree, last_key, val);
		val = 0;
	}

#if defined(USE_ASSERT_CHECKING) && defined(XXX_DEBUG_TID_STORE)
	if (ts->itemptrs)
		Assert(ts->nitems == ts->num_tids);
#endif
}

/* Return true if the given TID is present in TIDStore */
bool
tidstore_lookup_tid(TIDStore *ts, ItemPointer tid)
{
	uint64 key;
	uint64 val;
	uint32 off;
	bool found;
#if defined(USE_ASSERT_CHECKING) && defined(XXX_DEBUG_TID_STORE)
	bool found_assert;
#endif

	key = tid_to_key_off(tid, &off);

	found = rt_search(ts->tree, key, &val);

#if defined(USE_ASSERT_CHECKING) && defined(XXX_DEBUG_TID_STORE)
	if (ts->itemptrs)
		found_assert = bsearch((void *) tid,
							   (void *) ts->itemptrs,
							   ts->nitems,
							   sizeof(ItemPointerData),
							   vac_cmp_itemptr) != NULL;
#endif

	if (!found)
	{
#if defined(USE_ASSERT_CHECKING) && defined(XXX_DEBUG_TID_STORE)
		if (ts->itemptrs)
			Assert(!found_assert);
#endif
		return false;
	}

	found = (val & (UINT64CONST(1) << off)) != 0;

#if defined(USE_ASSERT_CHECKING) && defined(XXX_DEBUG_TID_STORE)

	if (ts->itemptrs && found != found_assert)
	{
		elog(WARNING, "tid (%d,%d)\n",
				ItemPointerGetBlockNumber(tid),
				ItemPointerGetOffsetNumber(tid));
		dump_itemptrs(ts);
	}

	if (ts->itemptrs)
		Assert(found == found_assert);

#endif
	return found;
}

TIDStoreIter *
tidstore_begin_iterate(TIDStore *ts)
{
	TIDStoreIter *iter;

	iter = palloc0(sizeof(TIDStoreIter));
	iter->ts = ts;
	iter->tree_iter = rt_begin_iterate(ts->tree);
	iter->blkno = InvalidBlockNumber;

#if defined(USE_ASSERT_CHECKING) && defined(XXX_DEBUG_TID_STORE)
	iter->itemptrs_index = 0;
#endif

	return iter;
}

bool
tidstore_iterate_next(TIDStoreIter *iter)
{
	uint64 key;
	uint64 val;

	if (iter->finished)
		return false;

	if (BlockNumberIsValid(iter->blkno))
	{
		iter->num_offsets = 0;
		tidstore_iter_collect_tids(iter, iter->next_key, iter->next_val);
	}

	while (rt_iterate_next(iter->tree_iter, &key, &val))
	{
		BlockNumber blkno;

		blkno = KEY_GET_BLKNO(key);

		if (BlockNumberIsValid(iter->blkno) && iter->blkno != blkno)
		{
			/*
			 * Remember the key-value pair for the next block for the
			 * next iteration.
			 */
			iter->next_key = key;
			iter->next_val = val;

#if defined(USE_ASSERT_CHECKING) && defined(XXX_DEBUG_TID_STORE)
			verify_iter_tids(iter);
#endif
			return true;
		}

		/* Collect tids extracted from the key-value pair */
		tidstore_iter_collect_tids(iter, key, val);
	}

#if defined(USE_ASSERT_CHECKING) && defined(XXX_DEBUG_TID_STORE)
	verify_iter_tids(iter);
#endif

	iter->finished = true;
	return true;
}

uint64
tidstore_num_tids(TIDStore *ts)
{
	return ts->num_tids;
}

uint64
tidstore_memory_usage(TIDStore *ts)
{
	return (uint64) sizeof(TIDStore) + rt_memory_usage(ts->tree);
}

/*
 * Get a handle that can be used by other processes to attach to this TIDStore
 */
tidstore_handle
tidstore_get_handle(TIDStore *ts)
{
	return rt_get_handle(ts->tree);
}

/* Extract TIDs from key-value pair */
static void
tidstore_iter_collect_tids(TIDStoreIter *iter, uint64 key, uint64 val)
{
	for (int i = 0; i < sizeof(uint64) * BITS_PER_BYTE; i++)
	{
		uint64	tid_i;
		OffsetNumber	off;

		if ((val & (UINT64CONST(1) << i)) == 0)
			continue;

		tid_i = key << TIDSTORE_VALUE_NBITS;
		tid_i |= i;

		off = tid_i & ((UINT64CONST(1) << TIDSTORE_OFFSET_NBITS) - 1);
		iter->offsets[iter->num_offsets++] = off;

#if defined(USE_ASSERT_CHECKING) && defined(XXX_DEBUG_TID_STORE)
		iter->itemptrs_index++;
#endif
	}

	iter->blkno = KEY_GET_BLKNO(key);
}

/* Encode a TID to key and val */
static inline uint64
tid_to_key_off(ItemPointer tid, uint32 *off)
{
	uint64 upper;
	uint64 tid_i;

	tid_i = ItemPointerGetOffsetNumber(tid);
	tid_i |= (uint64) ItemPointerGetBlockNumber(tid) << TIDSTORE_OFFSET_NBITS;

	*off = tid_i & ((1 << TIDSTORE_VALUE_NBITS) - 1);
	upper = tid_i >> TIDSTORE_VALUE_NBITS;
	Assert(*off < (sizeof(uint64) * BITS_PER_BYTE));

	return upper;
}
