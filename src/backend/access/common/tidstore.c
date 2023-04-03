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

#include "access/tidstore.h"
#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"
#include "utils/memutils.h"

/*
 * For encoding purposes, a tid is represented as a pair of 64-bit key and
 * 64-bit value.
 *
 * First, we construct a 64-bit unsigned integer by combining the block
 * number and the offset number. The number of bits used for the offset number
 * is specified by max_off in TidStoreCreate(). We are frugal with the bits,
 * because smaller keys could help keeping the radix tree shallow.
 *
 * For example, a tid of heap on a 8kB block uses the lowest 9 bits for
 * the offset number and uses the next 32 bits for the block number. 9 bits
 * are enough for the offset number, because MaxHeapTuplesPerPage < 2^9
 * on 8kB blocks. That is, only 41 bits are used:
 *
 * uuuuuuuY YYYYYYYY YYYYYYYY YYYYYYYY YYYYYYYX XXXXXXXX
 *
 * X = bits used for offset number
 * Y = bits used for block number
 * u = unused bit
 * (high on the left, low on the right)
 *
 * Then, 64-bit value is the bitmap representation of the lowest 6 bits
 * (LOWER_OFFSET_NBITS) of the integer, and 64-bit key consists of the
 * upper 3 bits of the offset number and the block number, 35 bits in
 * total:
 *
 * uuuuuuuY YYYYYYYY YYYYYYYY YYYYYYYY YYYYYYYX XXXXXXXX
 *                                                |----| value
 *        |--------------------------------------| key
 *
 * The maximum height of the radix tree is 5 in this case.
 *
 * If the number of bits required for offset numbers fits in LOWER_OFFSET_NBITS,
 * 64-bit value is the bitmap representation of the offset number, and the
 * 64-bit key is the block number.
 */
typedef uint64 tidkey;
typedef uint64 offsetbm;
#define LOWER_OFFSET_NBITS	6	/* log(sizeof(offsetbm), 2) */
#define LOWER_OFFSET_MASK	((1 << LOWER_OFFSET_NBITS) - 1)

/* A magic value used to identify our TidStore. */
#define TIDSTORE_MAGIC 0x826f6a10

#define RT_PREFIX local_rt
#define RT_SCOPE static
#define RT_DECLARE
#define RT_DEFINE
#define RT_MEASURE_MEMORY_USAGE
#define RT_VALUE_TYPE tidkey
#include "lib/radixtree.h"

#define RT_PREFIX shared_rt
#define RT_SHMEM
#define RT_SCOPE static
#define RT_DECLARE
#define RT_DEFINE
#define RT_MEASURE_MEMORY_USAGE
#define RT_VALUE_TYPE tidkey
#include "lib/radixtree.h"

/* The control object for a TidStore */
typedef struct TidStoreControl
{
	/* the number of tids in the store */
	int64	num_tids;

	/* These values are never changed after creation */
	size_t	max_bytes;		/* the maximum bytes a TidStore can use */
	int		max_off;		/* the maximum offset number */
	int		max_off_nbits;	/* the number of bits required for offset
							 * numbers */
	int		upper_off_nbits;	/* the number of bits of offset numbers
								 * used in a key */

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

	/* save for the next iteration */
	tidkey		next_tidkey;
	offsetbm	next_off_bitmap;

	/*
	 * output for the caller. Must be last because variable-size.
	 */
	TidStoreIterResult output;
} TidStoreIter;

static void iter_decode_key_off(TidStoreIter *iter, tidkey key, offsetbm off_bitmap);
static inline BlockNumber key_get_blkno(TidStore *ts, tidkey key);
static inline tidkey encode_blk_off(TidStore *ts, BlockNumber block,
									OffsetNumber offset, offsetbm *off_bit);
static inline tidkey encode_tid(TidStore *ts, ItemPointer tid, offsetbm *off_bit);

/*
 * Create a TidStore. The returned object is allocated in backend-local memory.
 * The radix tree for storage is allocated in DSA area is 'area' is non-NULL.
 */
TidStore *
TidStoreCreate(size_t max_bytes, int max_off, dsa_area *area)
{
	TidStore	*ts;

	ts = palloc0(sizeof(TidStore));

	if (area != NULL)
	{
		dsa_pointer dp;

		ts->tree.shared = shared_rt_create(CurrentMemoryContext, area,
										   LWTRANCHE_SHARED_TIDSTORE);

		dp = dsa_allocate0(area, sizeof(TidStoreControl));
		ts->control = (TidStoreControl *) dsa_get_address(area, dp);
		ts->area = area;

		ts->control->magic = TIDSTORE_MAGIC;
		LWLockInitialize(&ts->control->lock, LWTRANCHE_SHARED_TIDSTORE);
		ts->control->handle = dp;
		ts->control->tree_handle = shared_rt_get_handle(ts->tree.shared);
	}
	else
	{
		ts->tree.local = local_rt_create(CurrentMemoryContext);
		ts->control = (TidStoreControl *) palloc0(sizeof(TidStoreControl));
	}

	/*
	 * max_bytes is forced to be at least 64KB, the current minimum valid value
	 * for the work_mem GUC.
	 */
	ts->control->max_bytes = Max(64 * 1024L, max_bytes);

	ts->control->max_off = max_off;
	ts->control->max_off_nbits = pg_ceil_log2_32(max_off);

	if (ts->control->max_off_nbits < LOWER_OFFSET_NBITS)
		ts->control->max_off_nbits = LOWER_OFFSET_NBITS;

	ts->control->upper_off_nbits =
		ts->control->max_off_nbits - LOWER_OFFSET_NBITS;

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

		/* Recreate the radix tree */
		shared_rt_free(ts->tree.shared);
		ts->tree.shared = shared_rt_create(CurrentMemoryContext, ts->area,
										   LWTRANCHE_SHARED_TIDSTORE);

		/* update the radix tree handle as we recreated it */
		ts->control->tree_handle = shared_rt_get_handle(ts->tree.shared);

		/* Reset the statistics */
		ts->control->num_tids = 0;

		LWLockRelease(&ts->control->lock);
	}
	else
	{
		/* Recreate the radix tree */
		local_rt_free(ts->tree.local);
		ts->tree.local = local_rt_create(CurrentMemoryContext);

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
	offsetbm	*bitmaps;
	tidkey		key;
	tidkey		prev_key;
	offsetbm	off_bitmap = 0;
	int idx;
	const tidkey key_base = ((uint64) blkno) << ts->control->upper_off_nbits;
	const int nkeys = UINT64CONST(1) << ts->control->upper_off_nbits;

	Assert(!TidStoreIsShared(ts) || ts->control->magic == TIDSTORE_MAGIC);

	bitmaps = palloc(sizeof(offsetbm) * nkeys);
	key = prev_key = key_base;

	for (int i = 0; i < num_offsets; i++)
	{
		offsetbm	off_bit;

		/* encode the tid to a key and partial offset */
		key = encode_blk_off(ts, blkno, offsets[i], &off_bit);

		/* make sure we scanned the line pointer array in order */
		Assert(key >= prev_key);

		if (key > prev_key)
		{
			idx = prev_key - key_base;
			Assert(idx >= 0 && idx < nkeys);

			/* write out offset bitmap for this key */
			bitmaps[idx] = off_bitmap;

			/* zero out any gaps up to the current key */
			for (int empty_idx = idx + 1; empty_idx < key - key_base; empty_idx++)
				bitmaps[empty_idx] = 0;

			/* reset for current key -- the current offset will be handled below */
			off_bitmap = 0;
			prev_key = key;
		}

		off_bitmap |= off_bit;
	}

	/* save the final index for later */
	idx = key - key_base;
	/* write out last offset bitmap */
	bitmaps[idx] = off_bitmap;

	if (TidStoreIsShared(ts))
		LWLockAcquire(&ts->control->lock, LW_EXCLUSIVE);

	/* insert the calculated key-values to the tree */
	for (int i = 0; i <= idx; i++)
	{
		if (bitmaps[i])
		{
			key = key_base + i;

			if (TidStoreIsShared(ts))
				shared_rt_set(ts->tree.shared, key, &bitmaps[i]);
			else
				local_rt_set(ts->tree.local, key, &bitmaps[i]);
		}
	}

	/* update statistics */
	ts->control->num_tids += num_offsets;

	if (TidStoreIsShared(ts))
		LWLockRelease(&ts->control->lock);

	pfree(bitmaps);
}

/* Return true if the given tid is present in the TidStore */
bool
TidStoreIsMember(TidStore *ts, ItemPointer tid)
{
	tidkey key;
	offsetbm off_bitmap = 0;
	offsetbm off_bit;
	bool found;

	key = encode_tid(ts, tid, &off_bit);

	if (TidStoreIsShared(ts))
		found = shared_rt_search(ts->tree.shared, key, &off_bitmap);
	else
		found = local_rt_search(ts->tree.local, key, &off_bitmap);

	if (!found)
		return false;

	return (off_bitmap & off_bit) != 0;
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
				   sizeof(OffsetNumber) * ts->control->max_off);
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

static inline bool
tidstore_iter(TidStoreIter *iter, tidkey *key, offsetbm *off_bitmap)
{
	if (TidStoreIsShared(iter->ts))
		return shared_rt_iterate_next(iter->tree_iter.shared, key, off_bitmap);

	return local_rt_iterate_next(iter->tree_iter.local, key, off_bitmap);
}

/*
 * Scan the TidStore and return a pointer to TidStoreIterResult that has tids
 * in one block. We return the block numbers in ascending order and the offset
 * numbers in each result is also sorted in ascending order.
 */
TidStoreIterResult *
TidStoreIterateNext(TidStoreIter *iter)
{
	tidkey key;
	offsetbm off_bitmap = 0;
	TidStoreIterResult *output = &(iter->output);

	if (iter->finished)
		return NULL;

	/* Initialize the outputs */
	output->blkno = InvalidBlockNumber;
	output->num_offsets = 0;

	/*
	 * Decode the key and offset bitmap that are collected in the previous
	 * time, if exists.
	 */
	if (iter->next_off_bitmap > 0)
		iter_decode_key_off(iter, iter->next_tidkey, iter->next_off_bitmap);

	while (tidstore_iter(iter, &key, &off_bitmap))
	{
		BlockNumber blkno = key_get_blkno(iter->ts, key);

		if (BlockNumberIsValid(output->blkno) && output->blkno != blkno)
		{
			/*
			 * We got tids for a different block. We return the collected
			 * tids so far, and remember the key-value for the next
			 * iteration.
			 */
			iter->next_tidkey = key;
			iter->next_off_bitmap = off_bitmap;
			return output;
		}

		/* Collect tids decoded from the key and offset bitmap */
		iter_decode_key_off(iter, key, off_bitmap);
	}

	iter->finished = true;
	return output;
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

/*
 * Decode the key and offset bitmap to tids and store them to the iteration
 * result.
 */
static void
iter_decode_key_off(TidStoreIter *iter, tidkey key, offsetbm off_bitmap)
{
	TidStoreIterResult *output = (&iter->output);

	while (off_bitmap)
	{
		uint64	compressed_tid;
		OffsetNumber	off;

		compressed_tid = key << LOWER_OFFSET_NBITS;
		compressed_tid |= pg_rightmost_one_pos64(off_bitmap);

		off = compressed_tid & ((UINT64CONST(1) << iter->ts->control->max_off_nbits) - 1);

		Assert(output->num_offsets < iter->ts->control->max_off);
		output->offsets[output->num_offsets++] = off;

		/* unset the rightmost bit */
		off_bitmap &= ~pg_rightmost_one64(off_bitmap);
	}

	output->blkno = key_get_blkno(iter->ts, key);
}

/* Get block number from the given key */
static inline BlockNumber
key_get_blkno(TidStore *ts, tidkey key)
{
	return (BlockNumber) (key >> ts->control->upper_off_nbits);
}

/* Encode a tid to key and partial offset */
static inline tidkey
encode_tid(TidStore *ts, ItemPointer tid, offsetbm *off_bit)
{
	OffsetNumber offset = ItemPointerGetOffsetNumber(tid);
	BlockNumber block = ItemPointerGetBlockNumber(tid);

	return encode_blk_off(ts, block, offset, off_bit);
}

/* encode a block and offset to a key and partial offset */
static inline tidkey
encode_blk_off(TidStore *ts, BlockNumber block, OffsetNumber offset,
			   offsetbm *off_bit)
{
	tidkey key;
	uint64 compressed_tid;
	uint32 off_lower;

	off_lower = offset & LOWER_OFFSET_MASK;
	Assert(off_lower < (sizeof(offsetbm) * BITS_PER_BYTE));

	*off_bit = UINT64CONST(1) << off_lower;
	compressed_tid = offset | ((uint64) block << ts->control->max_off_nbits);
	key = compressed_tid >> LOWER_OFFSET_NBITS;

	return key;
}
