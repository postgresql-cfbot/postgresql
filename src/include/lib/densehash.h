/*
 * densehash.h
 *
 *	  A hashtable implementation which can be included into .c files to
 *	  provide a fast hash table implementation specific to the given type.
 *
 *	  DH_ELEMENT_TYPE defines the data type that the hashtable stores.  These
 *	  are allocated DH_ITEMS_PER_SEGMENT at a time and stored inside a
 *	  DH_SEGMENT.  Each DH_SEGMENT is allocated on demand only when there are
 *	  no free slots to store another DH_ELEMENT_TYPE in an existing segment.
 *	  After items are removed from the hash table, the next inserted item's
 *	  data will be stored in the earliest free item in the earliest segment
 *	  with a free slot.  This helps keep the actual data compact, or "dense"
 *	  even when the bucket array has become large.
 *
 *	  The bucket array is an array of DH_BUCKET and is dynamically allocated
 *	  and may grow as more items are added to the table.  The DH_BUCKET type
 *	  is very narrow and stores just 2 uint32 values.  One of these is the
 *	  hash value and the other is the index into the segments which are used
 *	  to directly look up the stored DH_ELEMENT_TYPE type.
 *
 *	  During inserts, hash table collisions are dealt with using linear
 *	  probing, this means that instead of doing something like chaining with a
 *	  linked list, we use the first free bucket which comes after the optimal
 *	  bucket.  This is much more CPU cache efficient than traversing a linked
 *	  list.  When we're unable to use the most optimal bucket, we may also
 *	  move the contents of subsequent buckets around so that we keep items as
 *	  close to their most optimal position as possible.  This prevents
 *	  excessively long linear probes during lookups.
 *
 *	  During hash table deletes, we must attempt to move the contents of
 *	  buckets that are not in their optimal position up to either their
 *	  optimal position, or as close as we can get to it.  During lookups, this
 *	  means that we can stop searching for a non-existing item as soon as we
 *	  find an empty bucket.
 *
 *	  Empty buckets are denoted by their 'index' field being set to
 *	  DH_UNUSED_BUCKET_INDEX.  This is done rather than adding a special field
 *	  so that we can keep the DH_BUCKET type as narrow as possible.
 *	  Conveniently sizeof(DH_BUCKET) is 8, which allows 8 of these to fit on a
 *	  single 64-byte cache line. It's important to keep this type as narrow as
 *	  possible so that we can perform hash lookups by hitting as few
 *	  cache lines as possible.
 *
 *	  The implementation here is similar to simplehash.h but has the following
 *	  benefits:
 *
 *	  - Pointers to elements are stable and are not moved around like they are
 *		in simplehash.h
 *	  - Sequential scans of the hash table remain very fast even when the
 *		table is sparsely populated.
 *	  - Both simplehash.h and densehash.h may move items around during inserts
 *		and deletes.  If DH_ELEMENT_TYPE is large, since simplehash.h stores
 *		the data in the hash bucket, these operations may become expensive in
 *		simplehash.h.  In densehash.h these remain fairly cheap as the bucket
 *		is always 8 bytes wide due to the hash entry being stored in the
 *		DH_SEGMENT.
 *
 * If none of the above points are important for the given use case then,
 * please consider using simplehash.h instead.
 *
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/lib/densehash.h
 *
 */

#include "port/pg_bitutils.h"

/* helpers */
#define DH_MAKE_PREFIX(a) CppConcat(a,_)
#define DH_MAKE_NAME(name) DH_MAKE_NAME_(DH_MAKE_PREFIX(DH_PREFIX),name)
#define DH_MAKE_NAME_(a,b) CppConcat(a,b)

/* type declarations */
#define DH_TYPE DH_MAKE_NAME(hash)
#define DH_BUCKET DH_MAKE_NAME(bucket)
#define DH_SEGMENT DH_MAKE_NAME(segment)
#define DH_ITERATOR DH_MAKE_NAME(iterator)

/* function declarations */
#define DH_CREATE DH_MAKE_NAME(create)
#define DH_DESTROY DH_MAKE_NAME(destroy)
#define DH_RESET DH_MAKE_NAME(reset)
#define DH_INSERT DH_MAKE_NAME(insert)
#define DH_INSERT_HASH DH_MAKE_NAME(insert_hash)
#define DH_DELETE DH_MAKE_NAME(delete)
#define DH_LOOKUP DH_MAKE_NAME(lookup)
#define DH_LOOKUP_HASH DH_MAKE_NAME(lookup_hash)
#define DH_GROW DH_MAKE_NAME(grow)
#define DH_START_ITERATE DH_MAKE_NAME(start_iterate)
#define DH_ITERATE DH_MAKE_NAME(iterate)

/* internal helper functions (no externally visible prototypes) */
#define DH_NEXT_ONEBIT DH_MAKE_NAME(next_onebit)
#define DH_NEXT_ZEROBIT DH_MAKE_NAME(next_zerobit)
#define DH_INDEX_TO_ELEMENT DH_MAKE_NAME(index_to_element)
#define DH_MARK_SEGMENT_ITEM_USED DH_MAKE_NAME(mark_segment_item_used)
#define DH_MARK_SEGMENT_ITEM_UNUSED DH_MAKE_NAME(mark_segment_item_unused)
#define DH_GET_NEXT_UNUSED_ENTRY DH_MAKE_NAME(get_next_unused_entry)
#define DH_REMOVE_ENTRY DH_MAKE_NAME(remove_entry)
#define DH_SET_BUCKET_IN_USE DH_MAKE_NAME(set_bucket_in_use)
#define DH_SET_BUCKET_EMPTY DH_MAKE_NAME(set_bucket_empty)
#define DH_IS_BUCKET_IN_USE DH_MAKE_NAME(is_bucket_in_use)
#define DH_COMPUTE_PARAMETERS DH_MAKE_NAME(compute_parameters)
#define DH_NEXT DH_MAKE_NAME(next)
#define DH_PREV DH_MAKE_NAME(prev)
#define DH_DISTANCE_FROM_OPTIMAL DH_MAKE_NAME(distance)
#define DH_INITIAL_BUCKET DH_MAKE_NAME(initial_bucket)
#define DH_INSERT_HASH_INTERNAL DH_MAKE_NAME(insert_hash_internal)
#define DH_LOOKUP_HASH_INTERNAL DH_MAKE_NAME(lookup_hash_internal)

/*
 * When allocating memory to store instances of DH_ELEMENT_TYPE, how many
 * should we allocate at once?  This must be a power of 2 and at least
 * DH_BITS_PER_WORD.
 */
#ifndef DH_ITEMS_PER_SEGMENT
#define DH_ITEMS_PER_SEGMENT	256
#endif

/* A special index to set DH_BUCKET->index to when it's not in use */
#define DH_UNUSED_BUCKET_INDEX	PG_UINT32_MAX

/*
 * Macros for translating a bucket's index into the segment index and another
 * to determine the item number within the segment.
 */
#define DH_INDEX_SEGMENT(i)	(i) / DH_ITEMS_PER_SEGMENT
#define DH_INDEX_ITEM(i)	(i) % DH_ITEMS_PER_SEGMENT

 /*
  * How many elements do we need in the bitmap array to store a bit for each
  * of DH_ITEMS_PER_SEGMENT.  Keep the word size native to the processor.
  */
#if SIZEOF_VOID_P >= 8

#define DH_BITS_PER_WORD		64
#define DH_BITMAP_WORD			uint64
#define DH_RIGHTMOST_ONE_POS(x) pg_rightmost_one_pos64(x)

#else

#define DH_BITS_PER_WORD		32
#define DH_BITMAP_WORD			uint32
#define DH_RIGHTMOST_ONE_POS(x) pg_rightmost_one_pos32(x)

#endif

/* Sanity check on DH_ITEMS_PER_SEGMENT setting */
#if DH_ITEMS_PER_SEGMENT < DH_BITS_PER_WORD
#error "DH_ITEMS_PER_SEGMENT must be >= than DH_BITS_PER_WORD"
#endif

/* Ensure DH_ITEMS_PER_SEGMENT is a power of 2 */
#if DH_ITEMS_PER_SEGMENT & (DH_ITEMS_PER_SEGMENT - 1) != 0
#error "DH_ITEMS_PER_SEGMENT must be a power of 2"
#endif

#define DH_BITMAP_WORDS			(DH_ITEMS_PER_SEGMENT / DH_BITS_PER_WORD)
#define DH_WORDNUM(x)			((x) / DH_BITS_PER_WORD)
#define DH_BITNUM(x)			((x) % DH_BITS_PER_WORD)

/* generate forward declarations necessary to use the hash table */
#ifdef DH_DECLARE

typedef struct DH_BUCKET
{
	uint32		hashvalue;		/* Hash value for this bucket */
	uint32		index;			/* Index to the actual data */
}			DH_BUCKET;

typedef struct DH_SEGMENT
{
	uint32		nitems;			/* Number of items stored */
	DH_BITMAP_WORD used_items[DH_BITMAP_WORDS]; /* A 1-bit for each used item
												 * in the items array */
	DH_ELEMENT_TYPE items[DH_ITEMS_PER_SEGMENT];	/* the actual data */
}			DH_SEGMENT;

/* type definitions */

/*
 * DH_TYPE
 *		Hash table metadata type
 */
typedef struct DH_TYPE
{
	/*
	 * Size of bucket array.  Note that the maximum number of elements is
	 * lower (DH_MAX_FILLFACTOR)
	 */
	uint32		size;

	/* mask for bucket and size calculations, based on size */
	uint32		sizemask;

	/* the number of elements stored */
	uint32		members;

	/* boundary after which to grow hashtable */
	uint32		grow_threshold;

	/* how many elements are there in the segments array */
	uint32		nsegments;

	/* the number of elements in the used_segments array */
	uint32		used_segment_words;

	/*
	 * The first segment we should search in for an empty slot.  This will be
	 * the first segment that DH_GET_NEXT_UNUSED_ENTRY will search in when
	 * looking for an unused entry.  We'll increase the value of this when we
	 * fill a segment and we'll lower it down when we delete an item from a
	 * segment lower than this value.
	 */
	uint32		first_free_segment;

	/* dynamically allocated array of hash buckets */
	DH_BUCKET  *buckets;

	/* an array of segment pointers to store data */
	DH_SEGMENT **segments;

	/*
	 * A bitmap of non-empty segments.  A 1-bit denotes that the corresponding
	 * segment is non-empty.
	 */
	DH_BITMAP_WORD *used_segments;

#ifdef DH_HAVE_PRIVATE_DATA
	/* user defined data, useful for callbacks */
	void	   *private_data;
#endif
}			DH_TYPE;

/*
 * DH_ITERATOR
 *		Used when looping over the contents of the hash table.
 */
typedef struct DH_ITERATOR
{
	int32		cursegidx;		/* current segment. -1 means not started */
	int32		curitemidx;		/* current item within cursegidx, -1 means not
								 * started */
	uint32		found_members;	/* number of items visitied so far in the loop */
	uint32		total_members;	/* number of items that existed at the start
								 * iteration. */
}			DH_ITERATOR;

/* externally visible function prototypes */

#ifdef DH_HAVE_PRIVATE_DATA
/* <prefix>_hash <prefix>_create(uint32 nbuckets, void *private_data) */
DH_SCOPE	DH_TYPE *DH_CREATE(uint32 nbuckets, void *private_data);
#else
/* <prefix>_hash <prefix>_create(uint32 nbuckets) */
DH_SCOPE	DH_TYPE *DH_CREATE(uint32 nbuckets);
#endif

/* void <prefix>_destroy(<prefix>_hash *tb) */
DH_SCOPE void DH_DESTROY(DH_TYPE * tb);

/* void <prefix>_reset(<prefix>_hash *tb) */
DH_SCOPE void DH_RESET(DH_TYPE * tb);

/* void <prefix>_grow(<prefix>_hash *tb) */
DH_SCOPE void DH_GROW(DH_TYPE * tb, uint32 newsize);

/* <element> *<prefix>_insert(<prefix>_hash *tb, <key> key, bool *found) */
DH_SCOPE	DH_ELEMENT_TYPE *DH_INSERT(DH_TYPE * tb, DH_KEY_TYPE key,
									   bool *found);

/*
 * <element> *<prefix>_insert_hash(<prefix>_hash *tb, <key> key, uint32 hash,
 * 								   bool *found)
 */
DH_SCOPE	DH_ELEMENT_TYPE *DH_INSERT_HASH(DH_TYPE * tb, DH_KEY_TYPE key,
											uint32 hash, bool *found);

/* <element> *<prefix>_lookup(<prefix>_hash *tb, <key> key) */
DH_SCOPE	DH_ELEMENT_TYPE *DH_LOOKUP(DH_TYPE * tb, DH_KEY_TYPE key);

/* <element> *<prefix>_lookup_hash(<prefix>_hash *tb, <key> key, uint32 hash) */
DH_SCOPE	DH_ELEMENT_TYPE *DH_LOOKUP_HASH(DH_TYPE * tb, DH_KEY_TYPE key,
											uint32 hash);

/* bool <prefix>_delete(<prefix>_hash *tb, <key> key) */
DH_SCOPE bool DH_DELETE(DH_TYPE * tb, DH_KEY_TYPE key);

/* void <prefix>_start_iterate(<prefix>_hash *tb, <prefix>_iterator *iter) */
DH_SCOPE void DH_START_ITERATE(DH_TYPE * tb, DH_ITERATOR * iter);

/* <element> *<prefix>_iterate(<prefix>_hash *tb, <prefix>_iterator *iter) */
DH_SCOPE	DH_ELEMENT_TYPE *DH_ITERATE(DH_TYPE * tb, DH_ITERATOR * iter);

#endif							/* DH_DECLARE */

/* generate implementation of the hash table */
#ifdef DH_DEFINE

/*
 * The maximum size for the hash table.  This must be a power of 2.  We cannot
 * make this PG_UINT32_MAX + 1 because we use DH_UNUSED_BUCKET_INDEX denote an
 * empty bucket.  Doing so would mean we could accidentally set a used
 * bucket's index to DH_UNUSED_BUCKET_INDEX.
 */
#define DH_MAX_SIZE ((uint32) PG_INT32_MAX + 1)

/* normal fillfactor, unless already close to maximum */
#ifndef DH_FILLFACTOR
#define DH_FILLFACTOR (0.9)
#endif
/* increase fillfactor if we otherwise would error out */
#define DH_MAX_FILLFACTOR (0.98)
/* grow if actual and optimal location bigger than */
#ifndef DH_GROW_MAX_DIB
#define DH_GROW_MAX_DIB 25
#endif
/*
 * Grow if more than this number of buckets needs to be moved when inserting.
 */
#ifndef DH_GROW_MAX_MOVE
#define DH_GROW_MAX_MOVE 150
#endif
#ifndef DH_GROW_MIN_FILLFACTOR
/* but do not grow due to DH_GROW_MAX_* if below */
#define DH_GROW_MIN_FILLFACTOR 0.1
#endif

/*
 * Wrap the following definitions in include guards, to avoid multiple
 * definition errors if this header is included more than once.  The rest of
 * the file deliberately has no include guards, because it can be included
 * with different parameters to define functions and types with non-colliding
 * names.
 */
#ifndef DENSEHASH_H
#define DENSEHASH_H

#ifdef FRONTEND
#define gh_error(...) pg_log_error(__VA_ARGS__)
#define gh_log(...) pg_log_info(__VA_ARGS__)
#else
#define gh_error(...) elog(ERROR, __VA_ARGS__)
#define gh_log(...) elog(LOG, __VA_ARGS__)
#endif

#endif							/* DENSEHASH_H */

/*
 * Gets the position of the first 1-bit which comes after 'prevbit' in the
 * 'words' array.  'nwords' is the size of the 'words' array.
 */
static inline int32
DH_NEXT_ONEBIT(DH_BITMAP_WORD * words, uint32 nwords, int32 prevbit)
{
	uint32		wordnum;

	prevbit++;

	wordnum = DH_WORDNUM(prevbit);
	if (wordnum < nwords)
	{
		DH_BITMAP_WORD mask = (~(DH_BITMAP_WORD) 0) << DH_BITNUM(prevbit);
		DH_BITMAP_WORD word = words[wordnum] & mask;

		if (word != 0)
			return wordnum * DH_BITS_PER_WORD + DH_RIGHTMOST_ONE_POS(word);

		for (++wordnum; wordnum < nwords; wordnum++)
		{
			word = words[wordnum];

			if (word != 0)
			{
				int32		result = wordnum * DH_BITS_PER_WORD;

				result += DH_RIGHTMOST_ONE_POS(word);
				return result;
			}
		}
	}
	return -1;
}

/*
 * Gets the position of the first 0-bit which comes after 'prevbit' in the
 * 'words' array.  'nwords' is the size of the 'words' array.
 *
 * This is similar to DH_NEXT_ONEBIT but flips the bits before operating on
 * each DH_BITMAP_WORD.
 */
static inline int32
DH_NEXT_ZEROBIT(DH_BITMAP_WORD * words, uint32 nwords, int32 prevbit)
{
	uint32		wordnum;

	prevbit++;

	wordnum = DH_WORDNUM(prevbit);
	if (wordnum < nwords)
	{
		DH_BITMAP_WORD mask = (~(DH_BITMAP_WORD) 0) << DH_BITNUM(prevbit);
		DH_BITMAP_WORD word = ~(words[wordnum] & mask); /* flip bits */

		if (word != 0)
			return wordnum * DH_BITS_PER_WORD + DH_RIGHTMOST_ONE_POS(word);

		for (++wordnum; wordnum < nwords; wordnum++)
		{
			word = ~words[wordnum]; /* flip bits */

			if (word != 0)
			{
				int32		result = wordnum * DH_BITS_PER_WORD;

				result += DH_RIGHTMOST_ONE_POS(word);
				return result;
			}
		}
	}
	return -1;
}

/*
 * Finds the hash table entry for a given DH_BUCKET's 'index'.
 */
static inline DH_ELEMENT_TYPE *
DH_INDEX_TO_ELEMENT(DH_TYPE * tb, uint32 index)
{
	DH_SEGMENT *seg;
	uint32		segidx;
	uint32		item;

	segidx = DH_INDEX_SEGMENT(index);
	item = DH_INDEX_ITEM(index);

	Assert(segidx < tb->nsegments);

	seg = tb->segments[segidx];

	Assert(seg != NULL);

	/* ensure this segment is marked as used */
	Assert(seg->used_items[DH_WORDNUM(item)] & (((DH_BITMAP_WORD) 1) << DH_BITNUM(item)));

	return &seg->items[item];
}

static inline void
DH_MARK_SEGMENT_ITEM_USED(DH_TYPE * tb, DH_SEGMENT * seg, uint32 segidx,
						  uint32 segitem)
{
	uint32		word = DH_WORDNUM(segitem);
	uint32		bit = DH_BITNUM(segitem);

	/* ensure this item is not marked as used */
	Assert((seg->used_items[word] & (((DH_BITMAP_WORD) 1) << bit)) == 0);

	/* switch on the used bit */
	seg->used_items[word] |= (((DH_BITMAP_WORD) 1) << bit);

	/* if the segment was previously empty then mark it as used */
	if (seg->nitems == 0)
	{
		word = DH_WORDNUM(segidx);
		bit = DH_BITNUM(segidx);

		/* switch on the used bit for this segment */
		tb->used_segments[word] |= (((DH_BITMAP_WORD) 1) << bit);
	}
	seg->nitems++;
}

static inline void
DH_MARK_SEGMENT_ITEM_UNUSED(DH_TYPE * tb, DH_SEGMENT * seg, uint32 segidx,
							uint32 segitem)
{
	uint32		word = DH_WORDNUM(segitem);
	uint32		bit = DH_BITNUM(segitem);

	/* ensure this item is marked as used */
	Assert((seg->used_items[word] & (((DH_BITMAP_WORD) 1) << bit)) != 0);

	/* switch off the used bit */
	seg->used_items[word] &= ~(((DH_BITMAP_WORD) 1) << bit);

	/* when removing the last item mark the segment as unused */
	if (seg->nitems == 1)
	{
		word = DH_WORDNUM(segidx);
		bit = DH_BITNUM(segidx);

		/* switch off the used bit for this segment */
		tb->used_segments[word] &= ~(((DH_BITMAP_WORD) 1) << bit);
	}

	seg->nitems--;
}

/*
 * Returns the first unused entry from the first non-full segment and set
 * *index to the index of the returned entry.
 */
static inline DH_ELEMENT_TYPE *
DH_GET_NEXT_UNUSED_ENTRY(DH_TYPE * tb, uint32 *index)
{
	DH_SEGMENT *seg;
	uint32		segidx = tb->first_free_segment;
	uint32		itemidx;

	seg = tb->segments[segidx];

	/* find the first segment with an unused item */
	while (seg != NULL && seg->nitems == DH_ITEMS_PER_SEGMENT)
		seg = tb->segments[++segidx];

	tb->first_free_segment = segidx;

	/* allocate the segment if it's not already */
	if (seg == NULL)
	{
		seg = DH_ALLOCATE(sizeof(DH_SEGMENT));
		tb->segments[segidx] = seg;

		seg->nitems = 0;
		memset(seg->used_items, 0, sizeof(seg->used_items));
		/* no need to zero the items array */

		/* use the first slot in this segment */
		itemidx = 0;
	}
	else
	{
		/* find the first unused item in this segment */
		itemidx = DH_NEXT_ZEROBIT(seg->used_items, DH_BITMAP_WORDS, -1);
		Assert(itemidx >= 0);
	}

	/* this is a good spot to ensure nitems matches the bits in used_items */
	Assert(seg->nitems == pg_popcount((const char *) seg->used_items, DH_ITEMS_PER_SEGMENT / 8));

	DH_MARK_SEGMENT_ITEM_USED(tb, seg, segidx, itemidx);

	*index = segidx * DH_ITEMS_PER_SEGMENT + itemidx;
	return &seg->items[itemidx];

}

/*
 * Remove the entry denoted by 'index' from its segment.
 */
static inline void
DH_REMOVE_ENTRY(DH_TYPE * tb, uint32 index)
{
	DH_SEGMENT *seg;
	uint32		segidx = DH_INDEX_SEGMENT(index);
	uint32		item = DH_INDEX_ITEM(index);

	Assert(segidx < tb->nsegments);
	seg = tb->segments[segidx];
	Assert(seg != NULL);

	DH_MARK_SEGMENT_ITEM_UNUSED(tb, seg, segidx, item);

	/*
	 * Lower the first free segment index to point to this segment so that the
	 * next insert will store in this segment.  If it's already set to a lower
	 * segment number then don't adjust as we want to consume slots from the
	 * earliest segment first.
	 */
	if (tb->first_free_segment > segidx)
		tb->first_free_segment = segidx;
}

/*
 * Set 'bucket' as in use by 'index'.
 */
static inline void
DH_SET_BUCKET_IN_USE(DH_BUCKET * bucket, uint32 index)
{
	bucket->index = index;
}

/*
 * Mark 'bucket' as unused.
 */
static inline void
DH_SET_BUCKET_EMPTY(DH_BUCKET * bucket)
{
	bucket->index = DH_UNUSED_BUCKET_INDEX;
}

/*
 * Return true if 'bucket' is in use.
 */
static inline bool
DH_IS_BUCKET_IN_USE(DH_BUCKET * bucket)
{
	return bucket->index != DH_UNUSED_BUCKET_INDEX;
}

 /*
  * Compute sizing parameters for hashtable.  Called when creating and growing
  * the hashtable.
  */
static inline void
DH_COMPUTE_PARAMETERS(DH_TYPE * tb, uint32 newsize)
{
	uint32		size;

	/*
	 * Ensure the bucket array size has not exceeded DH_MAX_SIZE or wrapped
	 * back to zero.
	 */
	if (newsize == 0 || newsize > DH_MAX_SIZE)
		gh_error("hash table too large");

	/*
	 * Ensure we don't build a table that can't store an entire single segment
	 * worth of data.
	 */
	size = Max(newsize, DH_ITEMS_PER_SEGMENT);

	/* round up size to the next power of 2 */
	size = pg_nextpower2_32(size);

	/* now set size */
	tb->size = size;
	tb->sizemask = tb->size - 1;

	/* calculate how many segments we'll need to store 'size' items */
	tb->nsegments = pg_nextpower2_32(size / DH_ITEMS_PER_SEGMENT);

	/*
	 * Calculate the number of bitmap words needed to store a bit for each
	 * segment.
	 */
	tb->used_segment_words = (tb->nsegments + DH_BITS_PER_WORD - 1) / DH_BITS_PER_WORD;

	/*
	 * Compute the next threshold at which we need to grow the hash table
	 * again.
	 */
	if (tb->size == DH_MAX_SIZE)
		tb->grow_threshold = (uint32) (((double) tb->size) * DH_MAX_FILLFACTOR);
	else
		tb->grow_threshold = (uint32) (((double) tb->size) * DH_FILLFACTOR);
}

/* return the optimal bucket for the hash */
static inline uint32
DH_INITIAL_BUCKET(DH_TYPE * tb, uint32 hash)
{
	return hash & tb->sizemask;
}

/* return the next bucket after the current, handling wraparound */
static inline uint32
DH_NEXT(DH_TYPE * tb, uint32 curelem, uint32 startelem)
{
	curelem = (curelem + 1) & tb->sizemask;

	Assert(curelem != startelem);

	return curelem;
}

/* return the bucket before the current, handling wraparound */
static inline uint32
DH_PREV(DH_TYPE * tb, uint32 curelem, uint32 startelem)
{
	curelem = (curelem - 1) & tb->sizemask;

	Assert(curelem != startelem);

	return curelem;
}

/* return the distance between a bucket and its optimal position */
static inline uint32
DH_DISTANCE_FROM_OPTIMAL(DH_TYPE * tb, uint32 optimal, uint32 bucket)
{
	if (optimal <= bucket)
		return bucket - optimal;
	else
		return (tb->size + bucket) - optimal;
}

/*
 * Create a hash table with 'nbuckets' buckets.
 */
DH_SCOPE	DH_TYPE *
#ifdef DH_HAVE_PRIVATE_DATA
DH_CREATE(uint32 nbuckets, void *private_data)
#else
DH_CREATE(uint32 nbuckets)
#endif
{
	DH_TYPE    *tb;
	uint32		size;
	uint32		i;

	tb = DH_ALLOCATE_ZERO(sizeof(DH_TYPE));

#ifdef DH_HAVE_PRIVATE_DATA
	tb->private_data = private_data;
#endif

	/* increase nelements by fillfactor, want to store nelements elements */
	size = (uint32) Min((double) DH_MAX_SIZE, ((double) nbuckets) / DH_FILLFACTOR);

	DH_COMPUTE_PARAMETERS(tb, size);

	tb->buckets = DH_ALLOCATE(sizeof(DH_BUCKET) * tb->size);

	/* ensure all the buckets are set to empty */
	for (i = 0; i < tb->size; i++)
		DH_SET_BUCKET_EMPTY(&tb->buckets[i]);

	tb->segments = DH_ALLOCATE_ZERO(sizeof(DH_SEGMENT *) * tb->nsegments);
	tb->used_segments = DH_ALLOCATE_ZERO(sizeof(DH_BITMAP_WORD) * tb->used_segment_words);
	return tb;
}

/* destroy a previously created hash table */
DH_SCOPE void
DH_DESTROY(DH_TYPE * tb)
{
	DH_FREE(tb->buckets);

	/* Free each segment one by one */
	for (uint32 n = 0; n < tb->nsegments; n++)
	{
		if (tb->segments[n] != NULL)
			DH_FREE(tb->segments[n]);
	}

	DH_FREE(tb->segments);
	DH_FREE(tb->used_segments);

	pfree(tb);
}

/* reset the contents of a previously created hash table */
DH_SCOPE void
DH_RESET(DH_TYPE * tb)
{
	int32		i = -1;
	uint32		x;

	/* reset each used segment one by one */
	while ((i = DH_NEXT_ONEBIT(tb->used_segments, tb->used_segment_words,
							   i)) >= 0)
	{
		DH_SEGMENT *seg = tb->segments[i];

		Assert(seg != NULL);

		seg->nitems = 0;
		memset(seg->used_items, 0, sizeof(seg->used_items));
	}

	/* empty every bucket */
	for (x = 0; x < tb->size; x++)
		DH_SET_BUCKET_EMPTY(&tb->buckets[x]);

	/* zero the used segment bits */
	memset(tb->used_segments, 0, sizeof(DH_BITMAP_WORD) * tb->used_segment_words);

	/* and mark the table as having zero members */
	tb->members = 0;

	/* ensure we start putting any new items in the first segment */
	tb->first_free_segment = 0;
}

/*
 * Grow a hash table to at least 'newsize' buckets.
 *
 * Usually this will automatically be called by insertions/deletions, when
 * necessary. But resizing to the exact input size can be advantageous
 * performance-wise, when known at some point.
 */
DH_SCOPE void
DH_GROW(DH_TYPE * tb, uint32 newsize)
{
	uint32		oldsize = tb->size;
	uint32		oldnsegments = tb->nsegments;
	uint32		oldusedsegmentwords = tb->used_segment_words;
	DH_BUCKET  *oldbuckets = tb->buckets;
	DH_SEGMENT **oldsegments = tb->segments;
	DH_BITMAP_WORD *oldusedsegments = tb->used_segments;
	DH_BUCKET  *newbuckets;
	uint32		i;
	uint32		startelem = 0;
	uint32		copyelem;

	Assert(oldsize == pg_nextpower2_32(oldsize));

	/* compute parameters for new table */
	DH_COMPUTE_PARAMETERS(tb, newsize);

	tb->buckets = DH_ALLOCATE(sizeof(DH_ELEMENT_TYPE) * tb->size);

	/* Ensure all the buckets are set to empty */
	for (i = 0; i < tb->size; i++)
		DH_SET_BUCKET_EMPTY(&tb->buckets[i]);

	newbuckets = tb->buckets;

	/*
	 * Copy buckets from the old buckets to newbuckets. We theoretically could
	 * use DH_INSERT here, to avoid code duplication, but that's more general
	 * than we need. We neither want tb->members increased, nor do we need to
	 * do deal with deleted elements, nor do we need to compare keys. So a
	 * special-cased implementation is a lot faster.  Resizing can be time
	 * consuming and frequent, that's worthwhile to optimize.
	 *
	 * To be able to simply move buckets over, we have to start not at the
	 * first bucket (i.e oldbuckets[0]), but find the first bucket that's
	 * either empty or is occupied by an entry at its optimal position. Such a
	 * bucket has to exist in any table with a load factor under 1, as not all
	 * buckets are occupied, i.e. there always has to be an empty bucket.  By
	 * starting at such a bucket we can move the entries to the larger table,
	 * without having to deal with conflicts.
	 */

	/* search for the first element in the hash that's not wrapped around */
	for (i = 0; i < oldsize; i++)
	{
		DH_BUCKET  *oldbucket = &oldbuckets[i];
		uint32		hash;
		uint32		optimal;

		if (!DH_IS_BUCKET_IN_USE(oldbucket))
		{
			startelem = i;
			break;
		}

		hash = oldbucket->hashvalue;
		optimal = DH_INITIAL_BUCKET(tb, hash);

		if (optimal == i)
		{
			startelem = i;
			break;
		}
	}

	/* and copy all elements in the old table */
	copyelem = startelem;
	for (i = 0; i < oldsize; i++)
	{
		DH_BUCKET  *oldbucket = &oldbuckets[copyelem];

		if (DH_IS_BUCKET_IN_USE(oldbucket))
		{
			uint32		hash;
			uint32		startelem;
			uint32		curelem;
			DH_BUCKET  *newbucket;

			hash = oldbucket->hashvalue;
			startelem = DH_INITIAL_BUCKET(tb, hash);
			curelem = startelem;

			/* find empty element to put data into */
			for (;;)
			{
				newbucket = &newbuckets[curelem];

				if (!DH_IS_BUCKET_IN_USE(newbucket))
					break;

				curelem = DH_NEXT(tb, curelem, startelem);
			}

			/* copy entry to new slot */
			memcpy(newbucket, oldbucket, sizeof(DH_BUCKET));
		}

		/* can't use DH_NEXT here, would use new size */
		copyelem++;
		if (copyelem >= oldsize)
			copyelem = 0;
	}

	DH_FREE(oldbuckets);

	/*
	 * Enlarge the segment array so we can store enough segments for the new
	 * hash table capacity.
	 */
	tb->segments = DH_ALLOCATE(sizeof(DH_SEGMENT *) * tb->nsegments);
	memcpy(tb->segments, oldsegments, sizeof(DH_SEGMENT *) * oldnsegments);
	/* zero the newly extended part of the array */
	memset(&tb->segments[oldnsegments], 0, sizeof(DH_SEGMENT *) *
		   (tb->nsegments - oldnsegments));
	DH_FREE(oldsegments);

	/*
	 * The majority of tables will only ever need one bitmap word to store
	 * used segments, so we only bother to reallocate the used_segments array
	 * if the number of bitmap words has actually changed.
	 */
	if (tb->used_segment_words != oldusedsegmentwords)
	{
		tb->used_segments = DH_ALLOCATE(sizeof(DH_BITMAP_WORD) *
										tb->used_segment_words);
		memcpy(tb->used_segments, oldusedsegments, sizeof(DH_BITMAP_WORD) *
			   oldusedsegmentwords);
		memset(&tb->used_segments[oldusedsegmentwords], 0,
			   sizeof(DH_BITMAP_WORD) * (tb->used_segment_words -
										 oldusedsegmentwords));

		DH_FREE(oldusedsegments);
	}
}

/*
 * This is a separate static inline function, so it can be reliably be inlined
 * into its wrapper functions even if DH_SCOPE is extern.
 */
static inline DH_ELEMENT_TYPE *
DH_INSERT_HASH_INTERNAL(DH_TYPE * tb, DH_KEY_TYPE key, uint32 hash, bool *found)
{
	uint32		startelem;
	uint32		curelem;
	DH_BUCKET  *buckets;
	uint32		insertdist;

restart:
	insertdist = 0;

	/*
	 * To avoid doing the grow check inside the loop, we do the grow check
	 * regardless of if the key is present.  This also lets us avoid having to
	 * re-find our position in the hashtable after resizing.
	 *
	 * Note that this also reached when resizing the table due to
	 * DH_GROW_MAX_DIB / DH_GROW_MAX_MOVE.
	 */
	if (unlikely(tb->members >= tb->grow_threshold))
	{
		/* this may wrap back to 0 when we're already at DH_MAX_SIZE */
		DH_GROW(tb, tb->size * 2);
	}

	/* perform the insert starting the bucket search at optimal location */
	buckets = tb->buckets;
	startelem = DH_INITIAL_BUCKET(tb, hash);
	curelem = startelem;
	for (;;)
	{
		DH_BUCKET  *bucket = &buckets[curelem];
		DH_ELEMENT_TYPE *entry;
		uint32		curdist;
		uint32		curhash;
		uint32		curoptimal;

		/* any empty bucket can directly be used */
		if (!DH_IS_BUCKET_IN_USE(bucket))
		{
			uint32		index;

			/* and add the new entry */
			tb->members++;

			entry = DH_GET_NEXT_UNUSED_ENTRY(tb, &index);
			entry->DH_KEY = key;
			bucket->hashvalue = hash;
			DH_SET_BUCKET_IN_USE(bucket, index);
			*found = false;
			return entry;
		}

		curhash = bucket->hashvalue;

		if (curhash == hash)
		{
			/*
			 * The hash value matches so we just need to ensure the key
			 * matches too.  To do that, we need to lookup the entry in the
			 * segments using the index stored in the bucket.
			 */
			entry = DH_INDEX_TO_ELEMENT(tb, bucket->index);

			/* if we find a match, we're done */
			if (DH_EQUAL(tb, key, entry->DH_KEY))
			{
				Assert(DH_IS_BUCKET_IN_USE(bucket));
				*found = true;
				return entry;
			}
		}

		/*
		 * For non-empty, non-matching buckets we have to decide whether to
		 * skip over or move the colliding entry.  When the colliding
		 * element's distance to its optimal position is smaller than the
		 * to-be-inserted entry's, we shift the colliding entry (and its
		 * followers) one bucket closer to their optimal position.
		 */
		curoptimal = DH_INITIAL_BUCKET(tb, curhash);
		curdist = DH_DISTANCE_FROM_OPTIMAL(tb, curoptimal, curelem);

		if (insertdist > curdist)
		{
			DH_ELEMENT_TYPE *entry;
			DH_BUCKET  *lastbucket = bucket;
			uint32		emptyelem = curelem;
			uint32		moveelem;
			int32		emptydist = 0;
			uint32		index;

			/* find next empty bucket */
			for (;;)
			{
				DH_BUCKET  *emptybucket;

				emptyelem = DH_NEXT(tb, emptyelem, startelem);
				emptybucket = &buckets[emptyelem];

				if (!DH_IS_BUCKET_IN_USE(emptybucket))
				{
					lastbucket = emptybucket;
					break;
				}

				/*
				 * To avoid negative consequences from overly imbalanced
				 * hashtables, grow the hashtable if collisions would require
				 * us to move a lot of entries.  The most likely cause of such
				 * imbalance is filling a (currently) small table, from a
				 * currently big one, in hashtable order.  Don't grow if the
				 * hashtable would be too empty, to prevent quick space
				 * explosion for some weird edge cases.
				 */
				if (unlikely(++emptydist > DH_GROW_MAX_MOVE) &&
					((double) tb->members / tb->size) >= DH_GROW_MIN_FILLFACTOR)
				{
					tb->grow_threshold = 0;
					goto restart;
				}
			}

			/* shift forward, starting at last occupied element */

			/*
			 * TODO: This could be optimized to be one memcpy in many cases,
			 * excepting wrapping around at the end of ->data. Hasn't shown up
			 * in profiles so far though.
			 */
			moveelem = emptyelem;
			while (moveelem != curelem)
			{
				DH_BUCKET  *movebucket;

				moveelem = DH_PREV(tb, moveelem, startelem);
				movebucket = &buckets[moveelem];

				memcpy(lastbucket, movebucket, sizeof(DH_BUCKET));
				lastbucket = movebucket;
			}

			/* and add the new entry */
			tb->members++;

			entry = DH_GET_NEXT_UNUSED_ENTRY(tb, &index);
			entry->DH_KEY = key;
			bucket->hashvalue = hash;
			DH_SET_BUCKET_IN_USE(bucket, index);
			*found = false;
			return entry;
		}

		curelem = DH_NEXT(tb, curelem, startelem);
		insertdist++;

		/*
		 * To avoid negative consequences from overly imbalanced hashtables,
		 * grow the hashtable if collisions lead to large runs. The most
		 * likely cause of such imbalance is filling a (currently) small
		 * table, from a currently big one, in hashtable order.  Don't grow if
		 * the hashtable would be too empty, to prevent quick space explosion
		 * for some weird edge cases.
		 */
		if (unlikely(insertdist > DH_GROW_MAX_DIB) &&
			((double) tb->members / tb->size) >= DH_GROW_MIN_FILLFACTOR)
		{
			tb->grow_threshold = 0;
			goto restart;
		}
	}
}

/*
 * Insert the key into the hashtable, set *found to true if the key already
 * exists, false otherwise. Returns the hashtable entry in either case.
 */
DH_SCOPE	DH_ELEMENT_TYPE *
DH_INSERT(DH_TYPE * tb, DH_KEY_TYPE key, bool *found)
{
	uint32		hash = DH_HASH_KEY(tb, key);

	return DH_INSERT_HASH_INTERNAL(tb, key, hash, found);
}

/*
 * Insert the key into the hashtable using an already-calculated hash. Set
 * *found to true if the key already exists, false otherwise. Returns the
 * hashtable entry in either case.
 */
DH_SCOPE	DH_ELEMENT_TYPE *
DH_INSERT_HASH(DH_TYPE * tb, DH_KEY_TYPE key, uint32 hash, bool *found)
{
	return DH_INSERT_HASH_INTERNAL(tb, key, hash, found);
}

/*
 * This is a separate static inline function, so it can be reliably be inlined
 * into its wrapper functions even if DH_SCOPE is extern.
 */
static inline DH_ELEMENT_TYPE *
DH_LOOKUP_HASH_INTERNAL(DH_TYPE * tb, DH_KEY_TYPE key, uint32 hash)
{
	const uint32 startelem = DH_INITIAL_BUCKET(tb, hash);
	uint32		curelem = startelem;

	for (;;)
	{
		DH_BUCKET  *bucket = &tb->buckets[curelem];

		if (!DH_IS_BUCKET_IN_USE(bucket))
			return NULL;

		if (bucket->hashvalue == hash)
		{
			DH_ELEMENT_TYPE *entry;

			/*
			 * The hash value matches so we just need to ensure the key
			 * matches too.  To do that, we need to lookup the entry in the
			 * segments using the index stored in the bucket.
			 */
			entry = DH_INDEX_TO_ELEMENT(tb, bucket->index);

			/* if we find a match, we're done */
			if (DH_EQUAL(tb, key, entry->DH_KEY))
				return entry;
		}

		/*
		 * TODO: we could stop search based on distance. If the current
		 * buckets's distance-from-optimal is smaller than what we've skipped
		 * already, the entry doesn't exist.
		 */

		curelem = DH_NEXT(tb, curelem, startelem);
	}
}

/*
 * Lookup an entry in the hash table.  Returns NULL if key not present.
 */
DH_SCOPE	DH_ELEMENT_TYPE *
DH_LOOKUP(DH_TYPE * tb, DH_KEY_TYPE key)
{
	uint32		hash = DH_HASH_KEY(tb, key);

	return DH_LOOKUP_HASH_INTERNAL(tb, key, hash);
}

/*
 * Lookup an entry in the hash table using an already-calculated hash.
 *
 * Returns NULL if key not present.
 */
DH_SCOPE	DH_ELEMENT_TYPE *
DH_LOOKUP_HASH(DH_TYPE * tb, DH_KEY_TYPE key, uint32 hash)
{
	return DH_LOOKUP_HASH_INTERNAL(tb, key, hash);
}

/*
 * Delete an entry from hash table by key.  Returns whether to-be-deleted key
 * was present.
 */
DH_SCOPE bool
DH_DELETE(DH_TYPE * tb, DH_KEY_TYPE key)
{
	uint32		hash = DH_HASH_KEY(tb, key);
	uint32		startelem = DH_INITIAL_BUCKET(tb, hash);
	uint32		curelem = startelem;

	for (;;)
	{
		DH_BUCKET  *bucket = &tb->buckets[curelem];

		if (!DH_IS_BUCKET_IN_USE(bucket))
			return false;

		if (bucket->hashvalue == hash)
		{
			DH_ELEMENT_TYPE *entry;

			entry = DH_INDEX_TO_ELEMENT(tb, bucket->index);

			if (DH_EQUAL(tb, key, entry->DH_KEY))
			{
				DH_BUCKET  *lastbucket = bucket;

				/* mark the entry as unused */
				DH_REMOVE_ENTRY(tb, bucket->index);
				/* and mark the bucket unused */
				DH_SET_BUCKET_EMPTY(bucket);

				tb->members--;

				/*
				 * Backward shift following buckets till either an empty
				 * bucket or a bucket at its optimal position is encountered.
				 *
				 * While that sounds expensive, the average chain length is
				 * short, and deletions would otherwise require tombstones.
				 */
				for (;;)
				{
					DH_BUCKET  *curbucket;
					uint32		curhash;
					uint32		curoptimal;

					curelem = DH_NEXT(tb, curelem, startelem);
					curbucket = &tb->buckets[curelem];

					if (!DH_IS_BUCKET_IN_USE(curbucket))
						break;

					curhash = curbucket->hashvalue;
					curoptimal = DH_INITIAL_BUCKET(tb, curhash);

					/* current is at optimal position, done */
					if (curoptimal == curelem)
					{
						DH_SET_BUCKET_EMPTY(lastbucket);
						break;
					}

					/* shift */
					memcpy(lastbucket, curbucket, sizeof(DH_BUCKET));
					DH_SET_BUCKET_EMPTY(curbucket);

					lastbucket = curbucket;
				}

				return true;
			}
		}
		/* TODO: return false; if the distance is too big */

		curelem = DH_NEXT(tb, curelem, startelem);
	}
}

/*
 * Initialize iterator.
 */
DH_SCOPE void
DH_START_ITERATE(DH_TYPE * tb, DH_ITERATOR * iter)
{
	iter->cursegidx = -1;
	iter->curitemidx = -1;
	iter->found_members = 0;
	iter->total_members = tb->members;
}

/*
 * Iterate over all entries in the hashtable. Return the next occupied entry,
 * or NULL if there are no more entries.
 *
 * During iteration the only current entry in the hash table and any entry
 * which was previously visited in the loop may be deleted.  Deletion of items
 * not yet visited is prohibited as are insertions of new entries.
 */
DH_SCOPE	DH_ELEMENT_TYPE *
DH_ITERATE(DH_TYPE * tb, DH_ITERATOR * iter)
{
	/*
	 * Bail if we've already visited all members.  This check allows us to
	 * exit quickly in cases where the table is large but it only contains a
	 * small number of records.  This also means that inserts into the table
	 * are not possible during iteration.  If that is done then we may not
	 * visit all items in the table.  Rather than ever removing this check to
	 * allow table insertions during iteration, we should add another iterator
	 * where insertions are safe.
	 */
	if (iter->found_members == iter->total_members)
		return NULL;

	for (;;)
	{
		DH_SEGMENT *seg;

		/* need a new segment? */
		if (iter->curitemidx == -1)
		{
			iter->cursegidx = DH_NEXT_ONEBIT(tb->used_segments,
											 tb->used_segment_words,
											 iter->cursegidx);

			/* no more segments with items? We're done */
			if (iter->cursegidx == -1)
				return NULL;
		}

		seg = tb->segments[iter->cursegidx];

		/* if the segment has items then it certainly shouldn't be NULL */
		Assert(seg != NULL);

		/*
		 * Advance to the next used item in this segment.  For full segments
		 * we bypass the bitmap and just skip to the next item, otherwise we
		 * consult the bitmap to find the next used item.
		 */
		if (seg->nitems == DH_ITEMS_PER_SEGMENT)
		{
			if (iter->curitemidx == DH_ITEMS_PER_SEGMENT - 1)
				iter->curitemidx = -1;
			else
			{
				iter->curitemidx++;
				iter->found_members++;
				return &seg->items[iter->curitemidx];
			}
		}
		else
		{
			iter->curitemidx = DH_NEXT_ONEBIT(seg->used_items,
											  DH_BITMAP_WORDS,
											  iter->curitemidx);

			if (iter->curitemidx >= 0)
			{
				iter->found_members++;
				return &seg->items[iter->curitemidx];
			}
		}

		/*
		 * DH_NEXT_ONEBIT returns -1 when there are no more bits.  We just
		 * loop again to fetch the next segment.
		 */
	}
}

#endif							/* DH_DEFINE */

/* undefine external parameters, so next hash table can be defined */
#undef DH_PREFIX
#undef DH_KEY_TYPE
#undef DH_KEY
#undef DH_ELEMENT_TYPE
#undef DH_HASH_KEY
#undef DH_SCOPE
#undef DH_DECLARE
#undef DH_DEFINE
#undef DH_EQUAL
#undef DH_ALLOCATE
#undef DH_ALLOCATE_ZERO
#undef DH_FREE

/* undefine locally declared macros */
#undef DH_MAKE_PREFIX
#undef DH_MAKE_NAME
#undef DH_MAKE_NAME_
#undef DH_ITEMS_PER_SEGMENT
#undef DH_UNUSED_BUCKET_INDEX
#undef DH_INDEX_SEGMENT
#undef DH_INDEX_ITEM
#undef DH_BITS_PER_WORD
#undef DH_BITMAP_WORD
#undef DH_RIGHTMOST_ONE_POS
#undef DH_BITMAP_WORDS
#undef DH_WORDNUM
#undef DH_BITNUM
#undef DH_RAW_ALLOCATOR
#undef DH_MAX_SIZE
#undef DH_FILLFACTOR
#undef DH_MAX_FILLFACTOR
#undef DH_GROW_MAX_DIB
#undef DH_GROW_MAX_MOVE
#undef DH_GROW_MIN_FILLFACTOR

/* types */
#undef DH_TYPE
#undef DH_BUCKET
#undef DH_SEGMENT
#undef DH_ITERATOR

/* external function names */
#undef DH_CREATE
#undef DH_DESTROY
#undef DH_RESET
#undef DH_INSERT
#undef DH_INSERT_HASH
#undef DH_DELETE
#undef DH_LOOKUP
#undef DH_LOOKUP_HASH
#undef DH_GROW
#undef DH_START_ITERATE
#undef DH_ITERATE

/* internal function names */
#undef DH_NEXT_ONEBIT
#undef DH_NEXT_ZEROBIT
#undef DH_INDEX_TO_ELEMENT
#undef DH_MARK_SEGMENT_ITEM_USED
#undef DH_MARK_SEGMENT_ITEM_UNUSED
#undef DH_GET_NEXT_UNUSED_ENTRY
#undef DH_REMOVE_ENTRY
#undef DH_SET_BUCKET_IN_USE
#undef DH_SET_BUCKET_EMPTY
#undef DH_IS_BUCKET_IN_USE
#undef DH_COMPUTE_PARAMETERS
#undef DH_NEXT
#undef DH_PREV
#undef DH_DISTANCE_FROM_OPTIMAL
#undef DH_INITIAL_BUCKET
#undef DH_INSERT_HASH_INTERNAL
#undef DH_LOOKUP_HASH_INTERNAL
