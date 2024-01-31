/*-------------------------------------------------------------------------
 *
 * tidstore.c
 *		Tid (ItemPointerData) storage implementation.
 *
 * TidStore is a in-memory data structure to store tids (ItemPointerData).
 * Internally it uses a radix tree as the storage for tids. The key is the
 * BlockNumber and the value is a bitmap of offsets, BlocktableEntry.
 *
 * TidStore can be shared among parallel worker processes by passing DSA area
 * to TidStoreCreate(). Other backends can attach to the shared TidStore by
 * TidStoreAttach().
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/common/tidstore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tidstore.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "port/pg_bitutils.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"
#include "utils/memutils.h"

/* Enable TidStore debugging if USE_ASSERT_CHECKING */
#ifdef USE_ASSERT_CHECKING
#define TIDSTORE_DEBUG
#include "catalog/index.h"
#endif

#define WORDNUM(x)	((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x)	((x) % BITS_PER_BITMAPWORD)

/* number of active words for a page: */
#define WORDS_PER_PAGE(n) ((n) / BITS_PER_BITMAPWORD + 1)

typedef struct BlocktableEntry
{
	uint16		nwords;
	bitmapword	words[FLEXIBLE_ARRAY_MEMBER];
} BlocktableEntry;
#define MaxBlocktableEntrySize \
	offsetof(BlocktableEntry, words) + \
		(sizeof(bitmapword) * WORDS_PER_PAGE(MaxOffsetNumber))

#define RT_PREFIX local_rt
#define RT_SCOPE static
#define RT_DECLARE
#define RT_DEFINE
#define RT_VALUE_TYPE BlocktableEntry
#define RT_VARLEN_VALUE
#include "lib/radixtree.h"

#define RT_PREFIX shared_rt
#define RT_SHMEM
#define RT_SCOPE static
#define RT_DECLARE
#define RT_DEFINE
#define RT_VALUE_TYPE BlocktableEntry
#define RT_VARLEN_VALUE
#include "lib/radixtree.h"

/* Per-backend state for a TidStore */
struct TidStore
{
	/* MemoryContext where the TidStore is allocated */
	MemoryContext	context;

	/* Storage for Tids. Use either one depending on TidStoreIsShared() */
	union
	{
		local_rt_radix_tree *local;
		shared_rt_radix_tree *shared;
	} tree;

	/* DSA area for TidStore if used */
	dsa_area	*area;

#ifdef TIDSTORE_DEBUG
	ItemPointerData	*tids;
	int64		max_tids;
	int64		num_tids;
	bool		tids_unordered;
#endif
};
#define TidStoreIsShared(ts) ((ts)->area != NULL)

/* Iterator for TidStore */
typedef struct TidStoreIter
{
	TidStore	*ts;

	/* iterator of radix tree. Use either one depending on TidStoreIsShared() */
	union
	{
		shared_rt_iter	*shared;
		local_rt_iter	*local;
	} tree_iter;


#ifdef TIDSTORE_DEBUG
	/* iterator index for the ts->tids array */
	int64		tids_idx;
#endif

	/*
	 * output for the caller. Must be last because variable-size.
	 */
	TidStoreIterResult output;
} TidStoreIter;

static void tidstore_iter_extract_tids(TidStoreIter *iter, BlocktableEntry *page);

/* debug functions available only when TIDSTORE_DEBUG */
#ifdef TIDSTORE_DEBUG
static void ts_debug_set_block_offsets(TidStore *ts, BlockNumber blkno,
									   OffsetNumber *offsets, int num_offsets);
static void ts_debug_iter_check_tids(TidStoreIter *iter);
static bool ts_debug_is_member(TidStore *ts, ItemPointer tid);
static int itemptr_cmp(const void *left, const void *right);
#endif

/*
 * Create a TidStore. The returned object is allocated in backend-local memory.
 * The radix tree for storage is allocated in DSA area if 'area' is non-NULL.
 */
TidStore *
TidStoreCreate(size_t max_bytes, dsa_area *area)
{
	TidStore	*ts;

	ts = palloc0(sizeof(TidStore));
	ts->context = CurrentMemoryContext;

	if (area != NULL)
	{
		ts->tree.shared = shared_rt_create(ts->context, max_bytes, area,
										   LWTRANCHE_SHARED_TIDSTORE);
		ts->area = area;
	}
	else
	{
		ts->tree.local = local_rt_create(ts->context, max_bytes);
	}

#ifdef TIDSTORE_DEBUG
	{
		int64		max_tids = max_bytes / sizeof(ItemPointerData);

		ts->tids = palloc(sizeof(ItemPointerData) * max_tids);
		ts->max_tids = max_tids;
		ts->num_tids = 0;
		ts->tids_unordered = false;
	}
#endif

	return ts;
}

/*
 * Attach to the shared TidStore using a handle. The returned object is
 * allocated in backend-local memory using the CurrentMemoryContext.
 */
TidStore *
TidStoreAttach(dsa_area *area, TidStoreHandle handle)
{
	TidStore *ts;
	dsa_pointer	rt_dp = handle;

	Assert(area != NULL);
	Assert(DsaPointerIsValid(rt_dp));

	/* create per-backend state */
	ts = palloc0(sizeof(TidStore));

	/* Find the shared the shared radix tree */
	ts->tree.shared = shared_rt_attach(area, rt_dp);
	ts->area = area;

	return ts;
}

/*
 * Detach from a TidStore. This detaches from radix tree and frees the
 * backend-local resources. The radix tree will continue to exist until
 * it is either explicitly destroyed, or the area that backs it is returned
 * to the operating system.
 */
void
TidStoreDetach(TidStore *ts)
{
	Assert(TidStoreIsShared(ts));

	shared_rt_detach(ts->tree.shared);

	pfree(ts);
}

/*
 * Lock support functions.
 *
 * We can use the radix tree's lock for shared tidstore as the data we
 * need to protect is only the shared radix tree.
 */
void
TidStoreLockExclusive(TidStore *ts)
{
	if (TidStoreIsShared(ts))
		shared_rt_lock_exclusive(ts->tree.shared);
}

void
TidStoreLockShare(TidStore *ts)
{
	if (TidStoreIsShared(ts))
		shared_rt_lock_share(ts->tree.shared);
}

void
TidStoreUnlock(TidStore *ts)
{
	if (TidStoreIsShared(ts))
		shared_rt_unlock(ts->tree.shared);
}

/*
 * Destroy a TidStore, returning all memory.
 *
 * Note that the caller must be certain that no other backend will attempt to
 * access the TidStore before calling this function. Other backend must
 * explicitly call TidStoreDetach() to free up backend-local memory associated
 * with the TidStore. The backend that calls TidStoreDestroy() must not call
 * TidStoreDetach().
 */
void
TidStoreDestroy(TidStore *ts)
{
	/* Destroy underlying radix tree */
	if (TidStoreIsShared(ts))
		shared_rt_free(ts->tree.shared);
	else
		local_rt_free(ts->tree.local);

#ifdef TIDSTORE_DEBUG
	if (!TidStoreIsShared(ts))
		pfree(ts->tids);
#endif

	pfree(ts);
}

/*
 * Support routine for TidStoreSetBlockOffsets(). Set the offset bitmapword
 * to the right location and zero out any gaps between (wordnum, new_wordnum).
 */
static inline void
set_offset_bitmap_at(BlocktableEntry *page, bitmapword word, int wordnum,
					 int new_wordnum)
{
	Assert(new_wordnum >= wordnum);

	/* write out offset bitmap for this page */
	page->words[wordnum] = word;

	/* Zero out any gaps up to the current word */
	for (int empty_idx = wordnum + 1; empty_idx < new_wordnum; empty_idx++)
		page->words[empty_idx] = 0;
}

/*
 * Set the given tids on the blkno to TidStore.
 *
 * NB: the offset numbers in offsets must be sorted in ascending order.
 */
void
TidStoreSetBlockOffsets(TidStore *ts, BlockNumber blkno, OffsetNumber *offsets,
						int num_offsets)
{
	char	data[MaxBlocktableEntrySize];
	BlocktableEntry *page = (BlocktableEntry *) data;
	bitmapword word = 0;
	int		prev_wordnum;
	int		wordnum;
	int		bitnum;
	size_t page_len;
	bool	found PG_USED_FOR_ASSERTS_ONLY;

	Assert(num_offsets > 0);

	wordnum = prev_wordnum = 0;
	for (int i = 0; i < num_offsets; i++)
	{
		OffsetNumber off = offsets[i];

		/* safety check to ensure we don't overrun bit array bounds */
		if (!OffsetNumberIsValid(off))
			elog(ERROR, "tuple offset out of range: %u", off);

		/* Page is exact, so set bit for individual tuple */
		wordnum = WORDNUM(off);
		bitnum = BITNUM(off);

		if (wordnum > prev_wordnum)
		{
			/* write out offset bitmap for this page */
			set_offset_bitmap_at(page, word, prev_wordnum, wordnum);

			word = 0;
			prev_wordnum = wordnum;
		}

		word |= ((bitmapword) 1 << bitnum);
	}

	/* write out the final offset bitmap */
	set_offset_bitmap_at(page, word, prev_wordnum, wordnum);

	page->nwords = WORDS_PER_PAGE(offsets[num_offsets - 1]);

	page_len = offsetof(BlocktableEntry, words) +
		sizeof(bitmapword) * page->nwords;

	if (TidStoreIsShared(ts))
		found = shared_rt_set(ts->tree.shared, blkno, (BlocktableEntry *) page,
							  page_len);
	else
		found = local_rt_set(ts->tree.local, blkno, (BlocktableEntry *) page,
							 page_len);

	Assert(!found);

#ifdef TIDSTORE_DEBUG
	if (!TidStoreIsShared(ts))
	{
		/* Insert tids into the tid array too */
		ts_debug_set_block_offsets(ts, blkno, offsets, num_offsets);
	}
#endif
}

/* Return true if the given tid is present in the TidStore */
bool
TidStoreIsMember(TidStore *ts, ItemPointer tid)
{
	int wordnum;
	int bitnum;
	BlocktableEntry *page;
	BlockNumber blk = ItemPointerGetBlockNumber(tid);
	OffsetNumber off = ItemPointerGetOffsetNumber(tid);
	bool ret;

	if (TidStoreIsShared(ts))
		page = shared_rt_find(ts->tree.shared, blk);
	else
		page = local_rt_find(ts->tree.local, blk);

	/* no entry for the blk */
	if (page == NULL)
		return false;

	wordnum = WORDNUM(off);
	bitnum = BITNUM(off);

	/* no bitmap for the off */
	if (wordnum >= page->nwords)
		return false;

	ret = (page->words[wordnum] & ((bitmapword) 1 << bitnum)) != 0;

#ifdef TIDSTORE_DEBUG
	if (!TidStoreIsShared(ts))
	{
		bool ret_debug = ts_debug_is_member(ts, tid);;
		Assert(ret == ret_debug);
	}
#endif
	return ret;
}

/*
 * Prepare to iterate through a TidStore. Since the radix tree is locked during
 * the iteration, so TidStoreEndIterate() needs to be called when finished.
 *
 * The TidStoreIter struct is created in the caller's memory context.
 *
 * Concurrent updates during the iteration will be blocked when inserting a
 * key-value to the radix tree.
 */
TidStoreIter *
TidStoreBeginIterate(TidStore *ts)
{
	TidStoreIter *iter;

	iter = palloc0(sizeof(TidStoreIter));
	iter->ts = ts;

	/*
	 * We start with an array large enough to contain at least the offsets
	 * from one completely full bitmap element.
	 */
	iter->output.max_offset = 2 * BITS_PER_BITMAPWORD;
	iter->output.offsets = palloc(sizeof(OffsetNumber) * iter->output.max_offset);

	if (TidStoreIsShared(ts))
		iter->tree_iter.shared = shared_rt_begin_iterate(ts->tree.shared);
	else
		iter->tree_iter.local = local_rt_begin_iterate(ts->tree.local);

#ifdef TIDSTORE_DEBUG
	if (!TidStoreIsShared(ts))
		iter->tids_idx = 0;
#endif

	return iter;
}


/*
 * Scan the TidStore and return a pointer to TidStoreIterResult that has tids
 * in one block. We return the block numbers in ascending order and the offset
 * numbers in each result is also sorted in ascending order.
 */
TidStoreIterResult *
TidStoreIterateNext(TidStoreIter *iter)
{
	uint64 key;
	BlocktableEntry *page;
	TidStoreIterResult *result = &(iter->output);

	if (TidStoreIsShared(iter->ts))
		page =  shared_rt_iterate_next(iter->tree_iter.shared, &key);
	else
		page = local_rt_iterate_next(iter->tree_iter.local, &key);

	if (page == NULL)
		return NULL;

	/* Collect tids extracted from the key-value pair */
	result->num_offsets = 0;

	tidstore_iter_extract_tids(iter, page);
	result->blkno = key;

#ifdef TIDSTORE_DEBUG
	if (!TidStoreIsShared(iter->ts))
		ts_debug_iter_check_tids(iter);
#endif

	return result;
}

/*
 * Finish an iteration over TidStore. This needs to be called after finishing
 * or when existing an iteration.
 */
void
TidStoreEndIterate(TidStoreIter *iter)
{
	if (TidStoreIsShared(iter->ts))
		shared_rt_end_iterate(iter->tree_iter.shared);
	else
		local_rt_end_iterate(iter->tree_iter.local);

	pfree(iter);
}

/* Return the memory usage of TidStore */
size_t
TidStoreMemoryUsage(TidStore *ts)
{
	if (TidStoreIsShared(ts))
		return shared_rt_memory_usage(ts->tree.shared);
	else
		return local_rt_memory_usage(ts->tree.local);
}

TidStoreHandle
TidStoreGetHandle(TidStore *ts)
{
	Assert(TidStoreIsShared(ts));

	return (TidStoreHandle) shared_rt_get_handle(ts->tree.shared);
}

/* Extract tids from the given key-value pair */
static void
tidstore_iter_extract_tids(TidStoreIter *iter, BlocktableEntry *page)
{
	TidStoreIterResult *result = (&iter->output);
	int			wordnum;

	for (wordnum = 0; wordnum < page->nwords; wordnum++)
	{
		bitmapword	w = page->words[wordnum];

		/* Make sure there is enough space to add offsets */
		if ((result->num_offsets + BITS_PER_BITMAPWORD) > result->max_offset)
		{
			result->max_offset *= 2;
			result->offsets = repalloc(result->offsets,
									   sizeof(OffsetNumber) * result->max_offset);
		}

		while (w != 0)
		{
			/* get pos of rightmost bit */
			int bitnum = bmw_rightmost_one_pos(w);
			int			off = wordnum * BITS_PER_BITMAPWORD + bitnum;

			result->offsets[result->num_offsets++] = off;

			/* unset the rightmost bit */
			w &= w - 1;
		}
	}
}

#ifdef TIDSTORE_DEBUG
/* Comparator routines for ItemPointer */
static int
itemptr_cmp(const void *left, const void *right)
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

/* Insert tids to the tid array for debugging */
static void
ts_debug_set_block_offsets(TidStore *ts, BlockNumber blkno, OffsetNumber *offsets,
						   int num_offsets)
{
	if (ts->num_tids > 0 &&
		blkno < ItemPointerGetBlockNumber(&(ts->tids[ts->num_tids - 1])))
	{
		/* The array will be sorted at ts_debug_is_member() */
		ts->tids_unordered = true;
	}

	for (int i = 0; i < num_offsets; i++)
	{
		ItemPointer tid;
		int idx = ts->num_tids + i;

		/* Enlarge the tid array if necessary */
		if (idx >= ts->max_tids)
		{
			ts->max_tids *= 2;
			ts->tids = repalloc(ts->tids, sizeof(ItemPointerData) * ts->max_tids);
		}

		tid = &(ts->tids[idx]);

		ItemPointerSetBlockNumber(tid, blkno);
		ItemPointerSetOffsetNumber(tid, offsets[i]);
	}

	ts->num_tids += num_offsets;
}

/* Return true if the given tid is present in the tid array */
static bool
ts_debug_is_member(TidStore *ts, ItemPointer tid)
{
	int64	litem,
		ritem,
		item;
	ItemPointer res;

	if (ts->num_tids == 0)
		return false;

	/* Make sure the tid array is sorted */
	if (ts->tids_unordered)
	{
		qsort(ts->tids, ts->num_tids, sizeof(ItemPointerData), itemptr_cmp);
		ts->tids_unordered = false;
	}

	litem = itemptr_encode(&ts->tids[0]);
	ritem = itemptr_encode(&ts->tids[ts->num_tids - 1]);
	item = itemptr_encode(tid);

	/*
	 * Doing a simple bound check before bsearch() is useful to avoid the
	 * extra cost of bsearch(), especially if dead items on the heap are
	 * concentrated in a certain range.	Since this function is called for
	 * every index tuple, it pays to be really fast.
	 */
	if (item < litem || item > ritem)
		return false;

	res = bsearch(tid, ts->tids, ts->num_tids, sizeof(ItemPointerData),
				  itemptr_cmp);

	return (res != NULL);
}

/* Verify if the iterator output matches the tids in the array for debugging */
static void
ts_debug_iter_check_tids(TidStoreIter *iter)
{
	BlockNumber blkno = iter->output.blkno;

	for (int i = 0; i < iter->output.num_offsets; i++)
	{
		ItemPointer tid = &(iter->ts->tids[iter->tids_idx + i]);

		Assert((iter->tids_idx + i) < iter->ts->max_tids);
		Assert(ItemPointerGetBlockNumber(tid) == blkno);
		Assert(ItemPointerGetOffsetNumber(tid) == iter->output.offsets[i]);
	}

	iter->tids_idx += iter->output.num_offsets;
}
#endif
