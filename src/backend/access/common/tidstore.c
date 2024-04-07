/*-------------------------------------------------------------------------
 *
 * tidstore.c
 *		TID (ItemPointerData) storage implementation.
 *
 * TidStore is a in-memory data structure to store TIDs (ItemPointerData).
 * Internally it uses a radix tree as the storage for TIDs. The key is the
 * BlockNumber and the value is a bitmap of offsets, BlocktableEntry.
 *
 * TidStore can be shared among parallel worker processes by using
 * TidStoreCreateShared(). Other backends can attach to the shared TidStore
 * by TidStoreAttach().
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
#include "storage/lwlock.h"
#include "utils/dsa.h"


#define WORDNUM(x)	((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x)	((x) % BITS_PER_BITMAPWORD)

/* number of active words for a page: */
#define WORDS_PER_PAGE(n) ((n) / BITS_PER_BITMAPWORD + 1)

/*
 * This is named similarly to PagetableEntry in tidbitmap.c
 * because the two have a similar function.
 */
typedef struct BlocktableEntry
{
	uint16		nwords;
	bitmapword	words[FLEXIBLE_ARRAY_MEMBER];
} BlocktableEntry;
#define MaxBlocktableEntrySize \
	offsetof(BlocktableEntry, words) + \
		(sizeof(bitmapword) * WORDS_PER_PAGE(MaxOffsetNumber))

#define RT_PREFIX local_ts
#define RT_SCOPE static
#define RT_DECLARE
#define RT_DEFINE
#define RT_VALUE_TYPE BlocktableEntry
#define RT_VARLEN_VALUE_SIZE(page) \
	(offsetof(BlocktableEntry, words) + \
	sizeof(bitmapword) * (page)->nwords)
#include "lib/radixtree.h"

#define RT_PREFIX shared_ts
#define RT_SHMEM
#define RT_SCOPE static
#define RT_DECLARE
#define RT_DEFINE
#define RT_VALUE_TYPE BlocktableEntry
#define RT_VARLEN_VALUE_SIZE(page) \
	(offsetof(BlocktableEntry, words) + \
	sizeof(bitmapword) * (page)->nwords)
#include "lib/radixtree.h"

/* Per-backend state for a TidStore */
struct TidStore
{
	/* MemoryContext where the TidStore is allocated */
	MemoryContext context;

	/* MemoryContext that the radix tree uses */
	MemoryContext rt_context;

	/* Storage for TIDs. Use either one depending on TidStoreIsShared() */
	union
	{
		local_ts_radix_tree *local;
		shared_ts_radix_tree *shared;
	}			tree;

	/* DSA area for TidStore if using shared memory */
	dsa_area   *area;
};
#define TidStoreIsShared(ts) ((ts)->area != NULL)

/* Iterator for TidStore */
struct TidStoreIter
{
	TidStore   *ts;

	/* iterator of radix tree. Use either one depending on TidStoreIsShared() */
	union
	{
		shared_ts_iter *shared;
		local_ts_iter *local;
	}			tree_iter;

	/* output for the caller */
	TidStoreIterResult output;
};

static void tidstore_iter_extract_tids(TidStoreIter *iter, BlockNumber blkno,
									   BlocktableEntry *page);

/*
 * Create a TidStore. The TidStore will live in the memory context that is
 * CurrentMemoryContext at the time of this call. The TID storage, backed
 * by a radix tree, will live in its child memory context, rt_context.
 *
 * "max_bytes" is not an internally-enforced limit; it is used only as a
 * hint to cap the memory block size of the memory context for TID storage.
 * This reduces space wastage due to over-allocation. If the caller wants to
 * monitor memory usage, it must compare its limit with the value reported
 * by TidStoreMemoryUsage().
 */
TidStore *
TidStoreCreateLocal(size_t max_bytes)
{
	TidStore   *ts;
	size_t		initBlockSize = ALLOCSET_DEFAULT_INITSIZE;
	size_t		minContextSize = ALLOCSET_DEFAULT_MINSIZE;
	size_t		maxBlockSize = ALLOCSET_DEFAULT_MAXSIZE;

	ts = palloc0(sizeof(TidStore));
	ts->context = CurrentMemoryContext;

	/* choose the maxBlockSize to be no larger than 1/16 of max_bytes */
	while (16 * maxBlockSize > max_bytes)
		maxBlockSize >>= 1;

	if (maxBlockSize < ALLOCSET_DEFAULT_INITSIZE)
		maxBlockSize = ALLOCSET_DEFAULT_INITSIZE;

	/* Create a memory context for the TID storage */
	ts->rt_context = AllocSetContextCreate(CurrentMemoryContext,
										   "TID storage",
										   minContextSize,
										   initBlockSize,
										   maxBlockSize);

	ts->tree.local = local_ts_create(ts->rt_context);

	return ts;
}

/*
 * Similar to TidStoreCreateLocal() but create a shared TidStore on a
 * DSA area. The TID storage will live in the DSA area, and the memory
 * context rt_context will have only meta data of the radix tree.
 *
 * The returned object is allocated in backend-local memory.
 */
TidStore *
TidStoreCreateShared(size_t max_bytes, int tranche_id)
{
	TidStore   *ts;
	dsa_area   *area;
	size_t		dsa_init_size = DSA_DEFAULT_INIT_SEGMENT_SIZE;
	size_t		dsa_max_size = DSA_MAX_SEGMENT_SIZE;

	ts = palloc0(sizeof(TidStore));
	ts->context = CurrentMemoryContext;

	ts->rt_context = AllocSetContextCreate(CurrentMemoryContext,
										   "TID storage meta data",
										   ALLOCSET_SMALL_SIZES);

	/*
	 * Choose the initial and maximum DSA segment sizes to be no longer than
	 * 1/8 of max_bytes.
	 */
	while (8 * dsa_max_size > max_bytes)
		dsa_max_size >>= 1;

	if (dsa_max_size < DSA_MIN_SEGMENT_SIZE)
		dsa_max_size = DSA_MIN_SEGMENT_SIZE;

	if (dsa_init_size > dsa_max_size)
		dsa_init_size = dsa_max_size;

	area = dsa_create_ext(tranche_id, dsa_init_size, dsa_max_size);
	ts->tree.shared = shared_ts_create(ts->rt_context, area,
									   tranche_id);
	ts->area = area;

	return ts;
}

/*
 * Attach to the shared TidStore. 'area_handle' is the DSA handle where
 * the TidStore is created. 'handle' is the dsa_pointer returned by
 * TidStoreGetHandle(). The returned object is allocated in backend-local
 * memory using the CurrentMemoryContext.
 */
TidStore *
TidStoreAttach(dsa_handle area_handle, dsa_pointer handle)
{
	TidStore   *ts;
	dsa_area   *area;

	Assert(area_handle != DSA_HANDLE_INVALID);
	Assert(DsaPointerIsValid(handle));

	/* create per-backend state */
	ts = palloc0(sizeof(TidStore));

	area = dsa_attach(area_handle);

	/* Find the shared the shared radix tree */
	ts->tree.shared = shared_ts_attach(area, handle);
	ts->area = area;

	return ts;
}

/*
 * Detach from a TidStore. This also detaches from radix tree and frees
 * the backend-local resources.
 */
void
TidStoreDetach(TidStore *ts)
{
	Assert(TidStoreIsShared(ts));

	shared_ts_detach(ts->tree.shared);
	dsa_detach(ts->area);

	pfree(ts);
}

/*
 * Lock support functions.
 *
 * We can use the radix tree's lock for shared TidStore as the data we
 * need to protect is only the shared radix tree.
 */

void
TidStoreLockExclusive(TidStore *ts)
{
	if (TidStoreIsShared(ts))
		shared_ts_lock_exclusive(ts->tree.shared);
}

void
TidStoreLockShare(TidStore *ts)
{
	if (TidStoreIsShared(ts))
		shared_ts_lock_share(ts->tree.shared);
}

void
TidStoreUnlock(TidStore *ts)
{
	if (TidStoreIsShared(ts))
		shared_ts_unlock(ts->tree.shared);
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
	{
		shared_ts_free(ts->tree.shared);

		dsa_detach(ts->area);
	}
	else
		local_ts_free(ts->tree.local);

	MemoryContextDelete(ts->rt_context);

	pfree(ts);
}

/*
 * Create or replace an entry for the given block and array of offsets.
 *
 * NB: This function is designed and optimized for vacuum's heap scanning
 * phase, so has some limitations:
 *
 * - The offset numbers "offsets" must be sorted in ascending order.
 * - If the block number already exists, the entry will be replaced --
 *	 there is no way to add or remove offsets from an entry.
 */
void
TidStoreSetBlockOffsets(TidStore *ts, BlockNumber blkno, OffsetNumber *offsets,
						int num_offsets)
{
	union
	{
		char		data[MaxBlocktableEntrySize];
		BlocktableEntry force_align_entry;
	} data;
	BlocktableEntry *page = (BlocktableEntry *) data.data;
	bitmapword	word;
	int			wordnum;
	int			next_word_threshold;
	int			idx = 0;

	Assert(num_offsets > 0);

	/* Check if the given offset numbers are ordered */
	for (int i = 1; i < num_offsets; i++)
		Assert(offsets[i] > offsets[i - 1]);

	for (wordnum = 0, next_word_threshold = BITS_PER_BITMAPWORD;
		 wordnum <= WORDNUM(offsets[num_offsets - 1]);
		 wordnum++, next_word_threshold += BITS_PER_BITMAPWORD)
	{
		word = 0;

		while (idx < num_offsets)
		{
			OffsetNumber off = offsets[idx];

			/* safety check to ensure we don't overrun bit array bounds */
			if (!OffsetNumberIsValid(off))
				elog(ERROR, "tuple offset out of range: %u", off);

			if (off >= next_word_threshold)
				break;

			word |= ((bitmapword) 1 << BITNUM(off));
			idx++;
		}

		/* write out offset bitmap for this wordnum */
		page->words[wordnum] = word;
	}

	page->nwords = wordnum;
	Assert(page->nwords == WORDS_PER_PAGE(offsets[num_offsets - 1]));

	if (TidStoreIsShared(ts))
		shared_ts_set(ts->tree.shared, blkno, page);
	else
		local_ts_set(ts->tree.local, blkno, page);
}

/* Return true if the given TID is present in the TidStore */
bool
TidStoreIsMember(TidStore *ts, ItemPointer tid)
{
	int			wordnum;
	int			bitnum;
	BlocktableEntry *page;
	BlockNumber blk = ItemPointerGetBlockNumber(tid);
	OffsetNumber off = ItemPointerGetOffsetNumber(tid);

	if (TidStoreIsShared(ts))
		page = shared_ts_find(ts->tree.shared, blk);
	else
		page = local_ts_find(ts->tree.local, blk);

	/* no entry for the blk */
	if (page == NULL)
		return false;

	wordnum = WORDNUM(off);
	bitnum = BITNUM(off);

	/* no bitmap for the off */
	if (wordnum >= page->nwords)
		return false;

	return (page->words[wordnum] & ((bitmapword) 1 << bitnum)) != 0;
}

/*
 * Prepare to iterate through a TidStore.
 *
 * The TidStoreIter struct is created in the caller's memory context, and it
 * will be freed in TidStoreEndIterate.
 *
 * The caller is responsible for locking TidStore until the iteration is
 * finished.
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
		iter->tree_iter.shared = shared_ts_begin_iterate(ts->tree.shared);
	else
		iter->tree_iter.local = local_ts_begin_iterate(ts->tree.local);

	return iter;
}


/*
 * Scan the TidStore and return the TIDs of the next block. The offsets in
 * each iteration result are ordered, as are the block numbers over all
 * iterations.
 */
TidStoreIterResult *
TidStoreIterateNext(TidStoreIter *iter)
{
	uint64		key;
	BlocktableEntry *page;

	if (TidStoreIsShared(iter->ts))
		page = shared_ts_iterate_next(iter->tree_iter.shared, &key);
	else
		page = local_ts_iterate_next(iter->tree_iter.local, &key);

	if (page == NULL)
		return NULL;

	/* Collect TIDs from the key-value pair */
	tidstore_iter_extract_tids(iter, (BlockNumber) key, page);

	return &(iter->output);
}

/*
 * Finish the iteration on TidStore.
 *
 * The caller is responsible for releasing any locks.
 */
void
TidStoreEndIterate(TidStoreIter *iter)
{
	if (TidStoreIsShared(iter->ts))
		shared_ts_end_iterate(iter->tree_iter.shared);
	else
		local_ts_end_iterate(iter->tree_iter.local);

	pfree(iter->output.offsets);
	pfree(iter);
}

/*
 * Return the memory usage of TidStore.
 */
size_t
TidStoreMemoryUsage(TidStore *ts)
{
	if (TidStoreIsShared(ts))
		return shared_ts_memory_usage(ts->tree.shared);
	else
		return local_ts_memory_usage(ts->tree.local);
}

/*
 * Return the DSA area where the TidStore lives.
 */
dsa_area *
TidStoreGetDSA(TidStore *ts)
{
	Assert(TidStoreIsShared(ts));

	return ts->area;
}

dsa_pointer
TidStoreGetHandle(TidStore *ts)
{
	Assert(TidStoreIsShared(ts));

	return (dsa_pointer) shared_ts_get_handle(ts->tree.shared);
}

/* Extract TIDs from the given key-value pair */
static void
tidstore_iter_extract_tids(TidStoreIter *iter, BlockNumber blkno,
						   BlocktableEntry *page)
{
	TidStoreIterResult *result = (&iter->output);
	int			wordnum;

	result->num_offsets = 0;
	result->blkno = blkno;

	for (wordnum = 0; wordnum < page->nwords; wordnum++)
	{
		bitmapword	w = page->words[wordnum];
		int			off = wordnum * BITS_PER_BITMAPWORD;

		/* Make sure there is enough space to add offsets */
		if ((result->num_offsets + BITS_PER_BITMAPWORD) > result->max_offset)
		{
			result->max_offset *= 2;
			result->offsets = repalloc(result->offsets,
									   sizeof(OffsetNumber) * result->max_offset);
		}

		while (w != 0)
		{
			if (w & 1)
				result->offsets[result->num_offsets++] = (OffsetNumber) off;
			off++;
			w >>= 1;
		}
	}
}
