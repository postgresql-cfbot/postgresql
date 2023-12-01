/*-------------------------------------------------------------------------
 *
 * tidstore.c
 *		Tid (ItemPointerData) storage implementation.
 *
 * TidStore is a in-memory data structure to store tids (ItemPointerData).
 * Internally, a tid is encoded as a pair of 64-bit key and 64-bit value,
 * and stored in the radix tree.
 *
 * TidStore can be shared among parallel worker processes by passing DSA area
 * to TidStoreCreate(). Other backends can attach to the shared TidStore by
 * TidStoreAttach().
 *
 * Regarding the concurrency support, we use a single LWLock for the TidStore.
 * The TidStore is exclusively locked when inserting encoded tids to the
 * radix tree or when resetting itself. When searching on the TidStore or
 * doing the iteration, it is not locked but the underlying radix tree is
 * locked in shared mode.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/common/tidstore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/tidstore.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "port/pg_bitutils.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"
#include "utils/memutils.h"


#define MAX_TUPLES_PER_PAGE  MaxHeapTuplesPerPage

#define WORDNUM(x)	((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x)	((x) % BITS_PER_BITMAPWORD)

/* number of active words for a page: */
#define WORDS_PER_PAGE	(MAX_TUPLES_PER_PAGE / BITS_PER_BITMAPWORD + 1)

typedef struct BlocktableEntry
{
	bitmapword	words[WORDS_PER_PAGE];
} BlocktableEntry;


/* A magic value used to identify our TidStores. */
#define TIDSTORE_MAGIC 0x826f6a10

#define RT_PREFIX local_rt
#define RT_SCOPE static
#define RT_DECLARE
#define RT_DEFINE
#define RT_VALUE_TYPE BlocktableEntry
#include "lib/radixtree.h"

#define RT_PREFIX shared_rt
#define RT_SHMEM
#define RT_SCOPE static
#define RT_DECLARE
#define RT_DEFINE
#define RT_VALUE_TYPE BlocktableEntry
#include "lib/radixtree.h"

/* The control object for a TidStore */
typedef struct TidStoreControl
{
	/* the number of tids in the store */
	int64	num_tids;

	/* These values are never changed after creation */
	size_t	max_bytes;		/* the maximum bytes a TidStore can use */
	int		max_offset;		/* the maximum offset number */

	/* The below fields are used only in shared case */

	uint32	magic;
	LWLock	lock;

	/* handles for TidStore and radix tree */
	TidStoreHandle		handle;
	shared_rt_handle	tree_handle;
} TidStoreControl;

/* Per-backend state for a TidStore */
struct TidStore
{
	/*
	 * Control object. This is allocated in DSA area 'area' in the shared
	 * case, otherwise in backend-local memory.
	 */
	TidStoreControl *control;

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

	/* we returned all tids? */
	bool		finished;

	/*
	 * output for the caller. Must be last because variable-size.
	 */
	TidStoreIterResult output;
} TidStoreIter;

static void tidstore_iter_extract_tids(TidStoreIter *iter, BlocktableEntry *page);

/*
 * Create a TidStore. The returned object is allocated in backend-local memory.
 * The radix tree for storage is allocated in DSA area is 'area' is non-NULL.
 */
TidStore *
TidStoreCreate(size_t max_bytes, int max_off, dsa_area *area)
{
	TidStore	*ts;

	ts = palloc0(sizeof(TidStore));
	ts->context = CurrentMemoryContext;

	/*
	 * Create the radix tree for the main storage.
	 *
	 * Memory consumption depends on the number of stored tids, but also on the
	 * distribution of them, how the radix tree stores, and the memory management
	 * that backed the radix tree. The maximum bytes that a TidStore can
	 * use is specified by the max_bytes in TidStoreCreate(). We want the total
	 * amount of memory consumption by a TidStore not to exceed the max_bytes.
	 *
	 * In local TidStore cases, the radix tree uses slab allocators for each kind
	 * of node class. The most memory consuming case while adding Tids associated
	 * with one page (i.e. during TidStoreSetBlockOffsets()) is that we allocate a new
	 * slab block for a new radix tree node, which is approximately 70kB. Therefore,
	 * we deduct 70kB from the max_bytes.
	 *
	 * In shared cases, DSA allocates the memory segments big enough to follow
	 * a geometric series that approximately doubles the total DSA size (see
	 * make_new_segment() in dsa.c). We simulated the how DSA increases segment
	 * size and the simulation revealed, the 75% threshold for the maximum bytes
	 * perfectly works in case where the max_bytes is a power-of-2, and the 60%
	 * threshold works for other cases.
	 */
	if (area != NULL)
	{
		dsa_pointer dp;
		float ratio = ((max_bytes & (max_bytes - 1)) == 0) ? 0.75 : 0.6;

		ts->tree.shared = shared_rt_create(ts->context, area,
										   LWTRANCHE_SHARED_TIDSTORE);

		dp = dsa_allocate0(area, sizeof(TidStoreControl));
		ts->control = (TidStoreControl *) dsa_get_address(area, dp);
		ts->control->max_bytes = (size_t) (max_bytes * ratio);
		ts->area = area;

		ts->control->magic = TIDSTORE_MAGIC;
		LWLockInitialize(&ts->control->lock, LWTRANCHE_SHARED_TIDSTORE);
		ts->control->handle = dp;
		ts->control->tree_handle = shared_rt_get_handle(ts->tree.shared);
	}
	else
	{
		ts->tree.local = local_rt_create(ts->context);

		ts->control = (TidStoreControl *) palloc0(sizeof(TidStoreControl));
		ts->control->max_bytes = max_bytes - (70 * 1024);
	}

	ts->control->max_offset = max_off;

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
	dsa_pointer control;

	Assert(area != NULL);
	Assert(DsaPointerIsValid(handle));

	/* create per-backend state */
	ts = palloc0(sizeof(TidStore));

	/* Find the control object in shared memory */
	control = handle;

	/* Set up the TidStore */
	ts->control = (TidStoreControl *) dsa_get_address(area, control);
	Assert(ts->control->magic == TIDSTORE_MAGIC);

	ts->tree.shared = shared_rt_attach(area, ts->control->tree_handle);
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
	Assert(TidStoreIsShared(ts) && ts->control->magic == TIDSTORE_MAGIC);

	shared_rt_detach(ts->tree.shared);
	pfree(ts);
}

/*
 * Destroy a TidStore, returning all memory.
 *
 * TODO: The caller must be certain that no other backend will attempt to
 * access the TidStore before calling this function. Other backend must
 * explicitly call TidStoreDetach() to free up backend-local memory associated
 * with the TidStore. The backend that calls TidStoreDestroy() must not call
 * TidStoreDetach().
 */
void
TidStoreDestroy(TidStore *ts)
{
	if (TidStoreIsShared(ts))
	{
		Assert(ts->control->magic == TIDSTORE_MAGIC);

		/*
		 * Vandalize the control block to help catch programming error where
		 * other backends access the memory formerly occupied by this radix
		 * tree.
		 */
		ts->control->magic = 0;
		dsa_free(ts->area, ts->control->handle);
		shared_rt_free(ts->tree.shared);
	}
	else
	{
		pfree(ts->control);
		local_rt_free(ts->tree.local);
	}

	pfree(ts);
}

/*
 * Forget all collected Tids. It's similar to TidStoreDestroy() but we don't free
 * entire TidStore but recreate only the radix tree storage.
 */
void
TidStoreReset(TidStore *ts)
{
	if (TidStoreIsShared(ts))
	{
		Assert(ts->control->magic == TIDSTORE_MAGIC);

		LWLockAcquire(&ts->control->lock, LW_EXCLUSIVE);

		/*
		 * Free the radix tree and return allocated DSA segments to
		 * the operating system.
		 */
		shared_rt_free(ts->tree.shared);
		dsa_trim(ts->area);

		/* Recreate the radix tree */
		ts->tree.shared = shared_rt_create(ts->context, ts->area,
										   LWTRANCHE_SHARED_TIDSTORE);

		/* update the radix tree handle as we recreated it */
		ts->control->tree_handle = shared_rt_get_handle(ts->tree.shared);

		/* Reset the statistics */
		ts->control->num_tids = 0;

		LWLockRelease(&ts->control->lock);
	}
	else
	{
		local_rt_free(ts->tree.local);
		ts->tree.local = local_rt_create(ts->context);

		/* Reset the statistics */
		ts->control->num_tids = 0;
	}
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
	BlocktableEntry *page;
	bool found;

	Assert(!TidStoreIsShared(ts) || ts->control->magic == TIDSTORE_MAGIC);

	if (TidStoreIsShared(ts))
		LWLockAcquire(&ts->control->lock, LW_EXCLUSIVE);

	if (TidStoreIsShared(ts))
		page = shared_rt_get(ts->tree.shared, blkno, &found);
	else
		page = local_rt_get(ts->tree.local, blkno, &found);

	for (int i = 0; i < num_offsets; i++)
	{
		int			wordnum,
					bitnum;
		OffsetNumber off = offsets[i];

		/* safety check to ensure we don't overrun bit array bounds */
		if (off < 1 || off > MAX_TUPLES_PER_PAGE)
			elog(ERROR, "tuple offset out of range: %u", off);

		/* Page is exact, so set bit for individual tuple */
		wordnum = WORDNUM(off);
		bitnum = BITNUM(off);

		/* WIP: slow, since it writes to memory for every bit */
		page->words[wordnum] |= ((bitmapword) 1 << bitnum);
	}

	/* update statistics */
	ts->control->num_tids += num_offsets;

	if (TidStoreIsShared(ts))
		LWLockRelease(&ts->control->lock);
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

	if (TidStoreIsShared(ts))
		page = shared_rt_find(ts->tree.shared, blk);
	else
		page = local_rt_find(ts->tree.local, blk);

	if (page == NULL)
		return false;

	wordnum = WORDNUM(off);
	bitnum = BITNUM(off);
	return (page->words[wordnum] & ((bitmapword) 1 << bitnum)) != 0;
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

	Assert(!TidStoreIsShared(ts) || ts->control->magic == TIDSTORE_MAGIC);

	iter = palloc0(sizeof(TidStoreIter) +
				   sizeof(OffsetNumber) * ts->control->max_offset);
	iter->ts = ts;

	if (TidStoreIsShared(ts))
		iter->tree_iter.shared = shared_rt_begin_iterate(ts->tree.shared);
	else
		iter->tree_iter.local = local_rt_begin_iterate(ts->tree.local);

	/* If the TidStore is empty, there is no business */
	if (TidStoreNumTids(ts) == 0)
		iter->finished = true;

	return iter;
}

static inline BlocktableEntry *
tidstore_iter_kv(TidStoreIter *iter, uint64 *key)
{
	if (TidStoreIsShared(iter->ts))
		return shared_rt_iterate_next_ptr(iter->tree_iter.shared, key);

	return local_rt_iterate_next_ptr(iter->tree_iter.local, key);
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
	//BlockNumber blkno;
	TidStoreIterResult *result = &(iter->output);

	if (iter->finished)
		return NULL;

	page = tidstore_iter_kv(iter, &key);

	/* XXX if we didn't return above, can we even reach here? */
	if (page == NULL)
		return NULL;

			/* Collect tids extracted from the key-value pair */
			result->num_offsets = 0;
			tidstore_iter_extract_tids(iter, page);
			result->blkno = key;

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

/* Return the number of tids we collected so far */
int64
TidStoreNumTids(TidStore *ts)
{
	int64 num_tids;

	Assert(!TidStoreIsShared(ts) || ts->control->magic == TIDSTORE_MAGIC);

	if (!TidStoreIsShared(ts))
		return ts->control->num_tids;

	LWLockAcquire(&ts->control->lock, LW_SHARED);
	num_tids = ts->control->num_tids;
	LWLockRelease(&ts->control->lock);

	return num_tids;
}

/* Return true if the current memory usage of TidStore exceeds the limit */
bool
TidStoreIsFull(TidStore *ts)
{
	Assert(!TidStoreIsShared(ts) || ts->control->magic == TIDSTORE_MAGIC);

	return (TidStoreMemoryUsage(ts) > ts->control->max_bytes);
}

/* Return the maximum memory TidStore can use */
size_t
TidStoreMaxMemory(TidStore *ts)
{
	Assert(!TidStoreIsShared(ts) || ts->control->magic == TIDSTORE_MAGIC);

	return ts->control->max_bytes;
}

/* Return the memory usage of TidStore */
size_t
TidStoreMemoryUsage(TidStore *ts)
{
	Assert(!TidStoreIsShared(ts) || ts->control->magic == TIDSTORE_MAGIC);

	/*
	 * In the shared case, TidStoreControl and radix_tree are backed by the
	 * same DSA area and rt_memory_usage() returns the value including both.
	 * So we don't need to add the size of TidStoreControl separately.
	 */
	if (TidStoreIsShared(ts))
		return sizeof(TidStore) + shared_rt_memory_usage(ts->tree.shared);

	return sizeof(TidStore) + sizeof(TidStore) + local_rt_memory_usage(ts->tree.local);
}

/*
 * Get a handle that can be used by other processes to attach to this TidStore
 */
TidStoreHandle
TidStoreGetHandle(TidStore *ts)
{
	Assert(TidStoreIsShared(ts) && ts->control->magic == TIDSTORE_MAGIC);

	return ts->control->handle;
}

/* Extract tids from the given key-value pair */
static void
tidstore_iter_extract_tids(TidStoreIter *iter, BlocktableEntry *page)
{
	TidStoreIterResult *result = (&iter->output);

	int			wordnum;

	for (wordnum = 0; wordnum < WORDS_PER_PAGE; wordnum++)
	{
		bitmapword	w = page->words[wordnum];

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
