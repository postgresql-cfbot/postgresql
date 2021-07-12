/*
 * generichash.h
 *
 *	  A hashtable implementation which can be included into .c files to
 *	  provide a fast hash table implementation specific to the given type.
 *
 *	  GH_ELEMENT_TYPE defines the data type that the hashtable stores.  Each
 *	  instance of GH_ELEMENT_TYPE which is stored in the hash table is done so
 *	  inside a GH_SEGMENT.  These GH_SEGMENTs are allocated on demand and
 *	  store GH_ITEMS_PER_SEGMENT each.  After items are removed from the hash
 *	  table, the next inserted item's data will be stored in the earliest free
 *	  item in the earliest free segment.  This helps keep the actual data
 *	  compact even when the bucket array has become large.
 *
 *	  The bucket array is an array of GH_BUCKET and is dynamically allocated
 *	  and may grow as more items are added to the table.  The GH_BUCKET type
 *	  is very narrow and stores just 2 uint32 values.  One of these is the
 *	  hash value and the other is the index into the segments which are used
 *	  to directly look up the stored GH_ELEMENT_TYPE type.
 *
 *	  During inserts, hash table collisions are dealt with using linear
 *	  probing, this means that instead of doing something like chaining with a
 *	  linked list, we use the first free bucket which comes after the optimal
 *	  bucket.  This is much more CPU cache efficient than traversing a linked
 *	  list.  When we're unable to use the most optimal bucket, we may also
 *	  move the contexts of subsequent buckets around so that we keep items as
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
 *	  GH_UNUSED_BUCKET_INDEX.  This is done rather than adding a special field
 *	  so that we can keep the GH_BUCKET type as narrow as possible.
 *	  Conveniently sizeof(GH_BUCKET) is 8, which allows 8 of these to fit on a
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
 *	  - Moving the contents of buckets around during inserts and deletes is
 *		generally cheaper here due to GH_BUCKET being very narrow.
 *
 * If none of the above points are important for the given use case then,
 * please consider using simplehash.h instead.
 *
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/lib/generichash.h
 *
 */

#include "port/pg_bitutils.h"

/* helpers */
#define GH_MAKE_PREFIX(a) CppConcat(a,_)
#define GH_MAKE_NAME(name) GH_MAKE_NAME_(GH_MAKE_PREFIX(GH_PREFIX),name)
#define GH_MAKE_NAME_(a,b) CppConcat(a,b)

/* type declarations */
#define GH_TYPE GH_MAKE_NAME(hash)
#define GH_BUCKET GH_MAKE_NAME(bucket)
#define GH_SEGMENT GH_MAKE_NAME(segment)
#define GH_ITERATOR GH_MAKE_NAME(iterator)

/* function declarations */
#define GH_CREATE GH_MAKE_NAME(create)
#define GH_DESTROY GH_MAKE_NAME(destroy)
#define GH_RESET GH_MAKE_NAME(reset)
#define GH_INSERT GH_MAKE_NAME(insert)
#define GH_INSERT_HASH GH_MAKE_NAME(insert_hash)
#define GH_DELETE GH_MAKE_NAME(delete)
#define GH_LOOKUP GH_MAKE_NAME(lookup)
#define GH_LOOKUP_HASH GH_MAKE_NAME(lookup_hash)
#define GH_GROW GH_MAKE_NAME(grow)
#define GH_START_ITERATE GH_MAKE_NAME(start_iterate)
#define GH_ITERATE GH_MAKE_NAME(iterate)

/* internal helper functions (no externally visible prototypes) */
#define GH_NEXT_ONEBIT GH_MAKE_NAME(next_onebit)
#define GH_NEXT_ZEROBIT GH_MAKE_NAME(next_zerobit)
#define GH_INDEX_TO_ELEMENT GH_MAKE_NAME(index_to_element)
#define GH_MARK_SEGMENT_ITEM_USED GH_MAKE_NAME(mark_segment_item_used)
#define GH_MARK_SEGMENT_ITEM_UNUSED GH_MAKE_NAME(mark_segment_item_unused)
#define GH_GET_NEXT_UNUSED_ENTRY GH_MAKE_NAME(get_next_unused_entry)
#define GH_REMOVE_ENTRY GH_MAKE_NAME(remove_entry)
#define GH_SET_BUCKET_IN_USE GH_MAKE_NAME(set_bucket_in_use)
#define GH_SET_BUCKET_EMPTY GH_MAKE_NAME(set_bucket_empty)
#define GH_IS_BUCKET_IN_USE GH_MAKE_NAME(is_bucket_in_use)
#define GH_COMPUTE_PARAMETERS GH_MAKE_NAME(compute_parameters)
#define GH_NEXT GH_MAKE_NAME(next)
#define GH_PREV GH_MAKE_NAME(prev)
#define GH_DISTANCE_FROM_OPTIMAL GH_MAKE_NAME(distance)
#define GH_INITIAL_BUCKET GH_MAKE_NAME(initial_bucket)
#define GH_INSERT_HASH_INTERNAL GH_MAKE_NAME(insert_hash_internal)
#define GH_LOOKUP_HASH_INTERNAL GH_MAKE_NAME(lookup_hash_internal)

/*
 * When allocating memory to store instances of GH_ELEMENT_TYPE, how many
 * should we allocate at once?  This must be a power of 2 and at least
 * GH_BITS_PER_WORD.
 */
#ifndef GH_ITEMS_PER_SEGMENT
#define GH_ITEMS_PER_SEGMENT	256
#endif

/* A special index to set GH_BUCKET->index to when it's not in use */
#define GH_UNUSED_BUCKET_INDEX	PG_UINT32_MAX

/*
 * Macros for translating a bucket's index into the segment and another to
 * determine the item number within the segment.
 */
#define GH_INDEX_SEGMENT(i)	(i) / GH_ITEMS_PER_SEGMENT
#define GH_INDEX_ITEM(i)	(i) % GH_ITEMS_PER_SEGMENT

 /*
  * How many elements do we need in the bitmap array to store a bit for each
  * of GH_ITEMS_PER_SEGMENT.  Keep the word size native to the processor.
  */
#if SIZEOF_VOID_P >= 8

#define GH_BITS_PER_WORD		64
#define GH_BITMAP_WORD			uint64
#define GH_RIGHTMOST_ONE_POS(x) pg_rightmost_one_pos64(x)

#else

#define GH_BITS_PER_WORD		32
#define GH_BITMAP_WORD			uint32
#define GH_RIGHTMOST_ONE_POS(x) pg_rightmost_one_pos32(x)

#endif

/* Sanity check on GH_ITEMS_PER_SEGMENT setting */
#if GH_ITEMS_PER_SEGMENT < GH_BITS_PER_WORD
#error "GH_ITEMS_PER_SEGMENT must be >= than GH_BITS_PER_WORD"
#endif

/* Ensure GH_ITEMS_PER_SEGMENT is a power of 2 */
#if GH_ITEMS_PER_SEGMENT & (GH_ITEMS_PER_SEGMENT - 1) != 0
#error "GH_ITEMS_PER_SEGMENT must be a power of 2"
#endif

#define GH_BITMAP_WORDS			(GH_ITEMS_PER_SEGMENT / GH_BITS_PER_WORD)
#define GH_WORDNUM(x)			((x) / GH_BITS_PER_WORD)
#define GH_BITNUM(x)			((x) % GH_BITS_PER_WORD)

/* generate forward declarations necessary to use the hash table */
#ifdef GH_DECLARE

typedef struct GH_BUCKET
{
	uint32		hashvalue;		/* Hash value for this bucket */
	uint32		index;			/* Index to the actual data */
}			GH_BUCKET;

typedef struct GH_SEGMENT
{
	uint32		nitems;			/* Number of items stored */
	GH_BITMAP_WORD used_items[GH_BITMAP_WORDS]; /* A 1-bit for each used item
												 * in the items array */
	GH_ELEMENT_TYPE items[GH_ITEMS_PER_SEGMENT];	/* the actual data */
}			GH_SEGMENT;

/* type definitions */

/*
 * GH_TYPE
 *		Hash table metadata type
 */
typedef struct GH_TYPE
{
	/*
	 * Size of bucket array.  Note that the maximum number of elements is
	 * lower (GH_MAX_FILLFACTOR)
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
	 * the first segment that GH_GET_NEXT_UNUSED_ENTRY will search in when
	 * looking for an unused entry.  We'll increase the value of this when we
	 * fill a segment and we'll lower it down when we delete an item from a
	 * segment lower than this value.
	 */
	uint32		first_free_segment;

	/* dynamically allocated array of hash buckets */
	GH_BUCKET  *buckets;

	/* an array of segment pointers to store data */
	GH_SEGMENT **segments;

	/*
	 * A bitmap of non-empty segments.  A 1-bit denotes that the corresponding
	 * segment is non-empty.
	 */
	GH_BITMAP_WORD *used_segments;

#ifdef GH_HAVE_PRIVATE_DATA
	/* user defined data, useful for callbacks */
	void	   *private_data;
#endif
}			GH_TYPE;

/*
 * GH_ITERATOR
 *		Used when looping over the contents of the hash table.
 */
typedef struct GH_ITERATOR
{
	int32		cursegidx;		/* current segment. -1 means not started */
	int32		curitemidx;		/* current item within cursegidx, -1 means not
								 * started */
	uint32		found_members;	/* number of items visitied so far in the loop */
	uint32		total_members;	/* number of items that existed at the start
								 * iteration. */
}			GH_ITERATOR;

/* externally visible function prototypes */

#ifdef GH_HAVE_PRIVATE_DATA
/* <prefix>_hash <prefix>_create(uint32 nbuckets, void *private_data) */
GH_SCOPE	GH_TYPE *GH_CREATE(uint32 nbuckets, void *private_data);
#else
/* <prefix>_hash <prefix>_create(uint32 nbuckets) */
GH_SCOPE	GH_TYPE *GH_CREATE(uint32 nbuckets);
#endif

/* void <prefix>_destroy(<prefix>_hash *tb) */
GH_SCOPE void GH_DESTROY(GH_TYPE * tb);

/* void <prefix>_reset(<prefix>_hash *tb) */
GH_SCOPE void GH_RESET(GH_TYPE * tb);

/* void <prefix>_grow(<prefix>_hash *tb) */
GH_SCOPE void GH_GROW(GH_TYPE * tb, uint32 newsize);

/* <element> *<prefix>_insert(<prefix>_hash *tb, <key> key, bool *found) */
GH_SCOPE	GH_ELEMENT_TYPE *GH_INSERT(GH_TYPE * tb, GH_KEY_TYPE key,
									   bool *found);

/*
 * <element> *<prefix>_insert_hash(<prefix>_hash *tb, <key> key, uint32 hash,
 * 								   bool *found)
 */
GH_SCOPE	GH_ELEMENT_TYPE *GH_INSERT_HASH(GH_TYPE * tb, GH_KEY_TYPE key,
											uint32 hash, bool *found);

/* <element> *<prefix>_lookup(<prefix>_hash *tb, <key> key) */
GH_SCOPE	GH_ELEMENT_TYPE *GH_LOOKUP(GH_TYPE * tb, GH_KEY_TYPE key);

/* <element> *<prefix>_lookup_hash(<prefix>_hash *tb, <key> key, uint32 hash) */
GH_SCOPE	GH_ELEMENT_TYPE *GH_LOOKUP_HASH(GH_TYPE * tb, GH_KEY_TYPE key,
											uint32 hash);

/* bool <prefix>_delete(<prefix>_hash *tb, <key> key) */
GH_SCOPE bool GH_DELETE(GH_TYPE * tb, GH_KEY_TYPE key);

/* void <prefix>_start_iterate(<prefix>_hash *tb, <prefix>_iterator *iter) */
GH_SCOPE void GH_START_ITERATE(GH_TYPE * tb, GH_ITERATOR * iter);

/* <element> *<prefix>_iterate(<prefix>_hash *tb, <prefix>_iterator *iter) */
GH_SCOPE	GH_ELEMENT_TYPE *GH_ITERATE(GH_TYPE * tb, GH_ITERATOR * iter);

#endif							/* GH_DECLARE */

/* generate implementation of the hash table */
#ifdef GH_DEFINE

/*
 * The maximum size for the hash table.  This must be a power of 2.  We cannot
 * make this PG_UINT32_MAX + 1 because we use GH_UNUSED_BUCKET_INDEX denote an
 * empty bucket.  Doing so would mean we could accidentally set a used
 * bucket's index to GH_UNUSED_BUCKET_INDEX.
 */
#define GH_MAX_SIZE ((uint32) PG_INT32_MAX + 1)

/* normal fillfactor, unless already close to maximum */
#ifndef GH_FILLFACTOR
#define GH_FILLFACTOR (0.9)
#endif
/* increase fillfactor if we otherwise would error out */
#define GH_MAX_FILLFACTOR (0.98)
/* grow if actual and optimal location bigger than */
#ifndef GH_GROW_MAX_DIB
#define GH_GROW_MAX_DIB 25
#endif
/*
 * Grow if more than this number of buckets needs to be moved when inserting.
 */
#ifndef GH_GROW_MAX_MOVE
#define GH_GROW_MAX_MOVE 150
#endif
#ifndef GH_GROW_MIN_FILLFACTOR
/* but do not grow due to GH_GROW_MAX_* if below */
#define GH_GROW_MIN_FILLFACTOR 0.1
#endif

/*
 * Wrap the following definitions in include guards, to avoid multiple
 * definition errors if this header is included more than once.  The rest of
 * the file deliberately has no include guards, because it can be included
 * with different parameters to define functions and types with non-colliding
 * names.
 */
#ifndef GENERICHASH_H
#define GENERICHASH_H

#ifdef FRONTEND
#define gh_error(...) pg_log_error(__VA_ARGS__)
#define gh_log(...) pg_log_info(__VA_ARGS__)
#else
#define gh_error(...) elog(ERROR, __VA_ARGS__)
#define gh_log(...) elog(LOG, __VA_ARGS__)
#endif

#endif							/* GENERICHASH_H */

/*
 * Gets the position of the first 1-bit which comes after 'prevbit' in the
 * 'words' array.  'nwords' is the size of the 'words' array.
 */
static inline int32
GH_NEXT_ONEBIT(GH_BITMAP_WORD * words, uint32 nwords, int32 prevbit)
{
	uint32		wordnum;

	prevbit++;

	wordnum = GH_WORDNUM(prevbit);
	if (wordnum < nwords)
	{
		GH_BITMAP_WORD mask = (~(GH_BITMAP_WORD) 0) << GH_BITNUM(prevbit);
		GH_BITMAP_WORD word = words[wordnum] & mask;

		if (word != 0)
			return wordnum * GH_BITS_PER_WORD + GH_RIGHTMOST_ONE_POS(word);

		for (++wordnum; wordnum < nwords; wordnum++)
		{
			word = words[wordnum];

			if (word != 0)
			{
				int32		result = wordnum * GH_BITS_PER_WORD;

				result += GH_RIGHTMOST_ONE_POS(word);
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
 * This is similar to GH_NEXT_ONEBIT but flips the bits before operating on
 * each GH_BITMAP_WORD.
 */
static inline int32
GH_NEXT_ZEROBIT(GH_BITMAP_WORD * words, uint32 nwords, int32 prevbit)
{
	uint32		wordnum;

	prevbit++;

	wordnum = GH_WORDNUM(prevbit);
	if (wordnum < nwords)
	{
		GH_BITMAP_WORD mask = (~(GH_BITMAP_WORD) 0) << GH_BITNUM(prevbit);
		GH_BITMAP_WORD word = ~(words[wordnum] & mask); /* flip bits */

		if (word != 0)
			return wordnum * GH_BITS_PER_WORD + GH_RIGHTMOST_ONE_POS(word);

		for (++wordnum; wordnum < nwords; wordnum++)
		{
			word = ~words[wordnum]; /* flip bits */

			if (word != 0)
			{
				int32		result = wordnum * GH_BITS_PER_WORD;

				result += GH_RIGHTMOST_ONE_POS(word);
				return result;
			}
		}
	}
	return -1;
}

/*
 * Finds the hash table entry for a given GH_BUCKET's 'index'.
 */
static inline GH_ELEMENT_TYPE *
GH_INDEX_TO_ELEMENT(GH_TYPE * tb, uint32 index)
{
	GH_SEGMENT *seg;
	uint32		segidx;
	uint32		item;

	segidx = GH_INDEX_SEGMENT(index);
	item = GH_INDEX_ITEM(index);

	Assert(segidx < tb->nsegments);

	seg = tb->segments[segidx];

	Assert(seg != NULL);

	/* ensure this segment is marked as used */
	Assert(seg->used_items[GH_WORDNUM(item)] & (((GH_BITMAP_WORD) 1) << GH_BITNUM(item)));

	return &seg->items[item];
}

static inline void
GH_MARK_SEGMENT_ITEM_USED(GH_TYPE * tb, GH_SEGMENT * seg, uint32 segidx,
						  uint32 segitem)
{
	uint32		word = GH_WORDNUM(segitem);
	uint32		bit = GH_BITNUM(segitem);

	/* ensure this item is not marked as used */
	Assert((seg->used_items[word] & (((GH_BITMAP_WORD) 1) << bit)) == 0);

	/* switch on the used bit */
	seg->used_items[word] |= (((GH_BITMAP_WORD) 1) << bit);

	/* if the segment was previously empty then mark it as used */
	if (seg->nitems == 0)
	{
		word = GH_WORDNUM(segidx);
		bit = GH_BITNUM(segidx);

		/* switch on the used bit for this segment */
		tb->used_segments[word] |= (((GH_BITMAP_WORD) 1) << bit);
	}
	seg->nitems++;
}

static inline void
GH_MARK_SEGMENT_ITEM_UNUSED(GH_TYPE * tb, GH_SEGMENT * seg, uint32 segidx,
							uint32 segitem)
{
	uint32		word = GH_WORDNUM(segitem);
	uint32		bit = GH_BITNUM(segitem);

	/* ensure this item is marked as used */
	Assert((seg->used_items[word] & (((GH_BITMAP_WORD) 1) << bit)) != 0);

	/* switch off the used bit */
	seg->used_items[word] &= ~(((GH_BITMAP_WORD) 1) << bit);

	/* when removing the last item mark the segment as unused */
	if (seg->nitems == 1)
	{
		word = GH_WORDNUM(segidx);
		bit = GH_BITNUM(segidx);

		/* switch off the used bit for this segment */
		tb->used_segments[word] &= ~(((GH_BITMAP_WORD) 1) << bit);
	}

	seg->nitems--;
}

/*
 * Returns the first unused entry from the first non-full segment and set
 * *index to the index of the returned entry.
 */
static inline GH_ELEMENT_TYPE *
GH_GET_NEXT_UNUSED_ENTRY(GH_TYPE * tb, uint32 *index)
{
	GH_SEGMENT *seg;
	uint32		segidx = tb->first_free_segment;
	uint32		itemidx;

	seg = tb->segments[segidx];

	/* find the first segment with an unused item */
	while (seg != NULL && seg->nitems == GH_ITEMS_PER_SEGMENT)
		seg = tb->segments[++segidx];

	tb->first_free_segment = segidx;

	/* allocate the segment if it's not already */
	if (seg == NULL)
	{
		seg = GH_ALLOCATE(sizeof(GH_SEGMENT));
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
		itemidx = GH_NEXT_ZEROBIT(seg->used_items, GH_BITMAP_WORDS, -1);
		Assert(itemidx >= 0);
	}

	/* this is a good spot to ensure nitems matches the bits in used_items */
	Assert(seg->nitems == pg_popcount((const char *) seg->used_items, GH_ITEMS_PER_SEGMENT / 8));

	GH_MARK_SEGMENT_ITEM_USED(tb, seg, segidx, itemidx);

	*index = segidx * GH_ITEMS_PER_SEGMENT + itemidx;
	return &seg->items[itemidx];

}

/*
 * Remove the entry denoted by 'index' from its segment.
 */
static inline void
GH_REMOVE_ENTRY(GH_TYPE * tb, uint32 index)
{
	GH_SEGMENT *seg;
	uint32		segidx = GH_INDEX_SEGMENT(index);
	uint32		item = GH_INDEX_ITEM(index);

	Assert(segidx < tb->nsegments);
	seg = tb->segments[segidx];
	Assert(seg != NULL);

	GH_MARK_SEGMENT_ITEM_UNUSED(tb, seg, segidx, item);

	/*
	 * Lower the first free segment index to point to this segment so that the
	 * next insert will store in this segment.  If it's already pointing to an
	 * earlier segment, then leave it be.
	 */
	if (tb->first_free_segment > segidx)
		tb->first_free_segment = segidx;
}

/*
 * Set 'bucket' as in use by 'index'.
 */
static inline void
GH_SET_BUCKET_IN_USE(GH_BUCKET * bucket, uint32 index)
{
	bucket->index = index;
}

/*
 * Mark 'bucket' as unused.
 */
static inline void
GH_SET_BUCKET_EMPTY(GH_BUCKET * bucket)
{
	bucket->index = GH_UNUSED_BUCKET_INDEX;
}

/*
 * Return true if 'bucket' is in use.
 */
static inline bool
GH_IS_BUCKET_IN_USE(GH_BUCKET * bucket)
{
	return bucket->index != GH_UNUSED_BUCKET_INDEX;
}

 /*
  * Compute sizing parameters for hashtable.  Called when creating and growing
  * the hashtable.
  */
static inline void
GH_COMPUTE_PARAMETERS(GH_TYPE * tb, uint32 newsize)
{
	uint32		size;

	/*
	 * Ensure the bucket array size has not exceeded GH_MAX_SIZE or wrapped
	 * back to zero.
	 */
	if (newsize == 0 || newsize > GH_MAX_SIZE)
		gh_error("hash table too large");

	/*
	 * Ensure we don't build a table that can't store an entire single segment
	 * worth of data.
	 */
	size = Max(newsize, GH_ITEMS_PER_SEGMENT);

	/* round up size to the next power of 2 */
	size = pg_nextpower2_32(size);

	/* now set size */
	tb->size = size;
	tb->sizemask = tb->size - 1;

	/* calculate how many segments we'll need to store 'size' items */
	tb->nsegments = pg_nextpower2_32(size / GH_ITEMS_PER_SEGMENT);

	/*
	 * Calculate the number of bitmap words needed to store a bit for each
	 * segment.
	 */
	tb->used_segment_words = (tb->nsegments + GH_BITS_PER_WORD - 1) / GH_BITS_PER_WORD;

	/*
	 * Compute the next threshold at which we need to grow the hash table
	 * again.
	 */
	if (tb->size == GH_MAX_SIZE)
		tb->grow_threshold = (uint32) (((double) tb->size) * GH_MAX_FILLFACTOR);
	else
		tb->grow_threshold = (uint32) (((double) tb->size) * GH_FILLFACTOR);
}

/* return the optimal bucket for the hash */
static inline uint32
GH_INITIAL_BUCKET(GH_TYPE * tb, uint32 hash)
{
	return hash & tb->sizemask;
}

/* return the next bucket after the current, handling wraparound */
static inline uint32
GH_NEXT(GH_TYPE * tb, uint32 curelem, uint32 startelem)
{
	curelem = (curelem + 1) & tb->sizemask;

	Assert(curelem != startelem);

	return curelem;
}

/* return the bucket before the current, handling wraparound */
static inline uint32
GH_PREV(GH_TYPE * tb, uint32 curelem, uint32 startelem)
{
	curelem = (curelem - 1) & tb->sizemask;

	Assert(curelem != startelem);

	return curelem;
}

/* return the distance between a bucket and its optimal position */
static inline uint32
GH_DISTANCE_FROM_OPTIMAL(GH_TYPE * tb, uint32 optimal, uint32 bucket)
{
	if (optimal <= bucket)
		return bucket - optimal;
	else
		return (tb->size + bucket) - optimal;
}

/*
 * Create a hash table with 'nbuckets' buckets.
 */
GH_SCOPE	GH_TYPE *
#ifdef GH_HAVE_PRIVATE_DATA
GH_CREATE(uint32 nbuckets, void *private_data)
#else
GH_CREATE(uint32 nbuckets)
#endif
{
	GH_TYPE    *tb;
	uint32		size;
	uint32		i;

	tb = GH_ALLOCATE_ZERO(sizeof(GH_TYPE));

#ifdef GH_HAVE_PRIVATE_DATA
	tb->private_data = private_data;
#endif

	/* increase nelements by fillfactor, want to store nelements elements */
	size = (uint32) Min((double) GH_MAX_SIZE, ((double) nbuckets) / GH_FILLFACTOR);

	GH_COMPUTE_PARAMETERS(tb, size);

	tb->buckets = GH_ALLOCATE(sizeof(GH_BUCKET) * tb->size);

	/* ensure all the buckets are set to empty */
	for (i = 0; i < tb->size; i++)
		GH_SET_BUCKET_EMPTY(&tb->buckets[i]);

	tb->segments = GH_ALLOCATE_ZERO(sizeof(GH_SEGMENT *) * tb->nsegments);
	tb->used_segments = GH_ALLOCATE_ZERO(sizeof(GH_BITMAP_WORD) * tb->used_segment_words);
	return tb;
}

/* destroy a previously created hash table */
GH_SCOPE void
GH_DESTROY(GH_TYPE * tb)
{
	GH_FREE(tb->buckets);

	/* Free each segment one by one */
	for (uint32 n = 0; n < tb->nsegments; n++)
	{
		if (tb->segments[n] != NULL)
			GH_FREE(tb->segments[n]);
	}

	GH_FREE(tb->segments);
	GH_FREE(tb->used_segments);

	pfree(tb);
}

/* reset the contents of a previously created hash table */
GH_SCOPE void
GH_RESET(GH_TYPE * tb)
{
	int32		i = -1;
	uint32		x;

	/* reset each used segment one by one */
	while ((i = GH_NEXT_ONEBIT(tb->used_segments, tb->used_segment_words,
							   i)) >= 0)
	{
		GH_SEGMENT *seg = tb->segments[i];

		Assert(seg != NULL);

		seg->nitems = 0;
		memset(seg->used_items, 0, sizeof(seg->used_items));
	}

	/* empty every bucket */
	for (x = 0; x < tb->size; x++)
		GH_SET_BUCKET_EMPTY(&tb->buckets[x]);

	/* zero the used segment bits */
	memset(tb->used_segments, 0, sizeof(GH_BITMAP_WORD) * tb->used_segment_words);

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
GH_SCOPE void
GH_GROW(GH_TYPE * tb, uint32 newsize)
{
	uint32		oldsize = tb->size;
	uint32		oldnsegments = tb->nsegments;
	uint32		oldusedsegmentwords = tb->used_segment_words;
	GH_BUCKET  *oldbuckets = tb->buckets;
	GH_SEGMENT **oldsegments = tb->segments;
	GH_BITMAP_WORD *oldusedsegments = tb->used_segments;
	GH_BUCKET  *newbuckets;
	uint32		i;
	uint32		startelem = 0;
	uint32		copyelem;

	Assert(oldsize == pg_nextpower2_32(oldsize));

	/* compute parameters for new table */
	GH_COMPUTE_PARAMETERS(tb, newsize);

	tb->buckets = GH_ALLOCATE(sizeof(GH_ELEMENT_TYPE) * tb->size);

	/* Ensure all the buckets are set to empty */
	for (i = 0; i < tb->size; i++)
		GH_SET_BUCKET_EMPTY(&tb->buckets[i]);

	newbuckets = tb->buckets;

	/*
	 * Copy buckets from the old buckets to newbuckets. We theoretically could
	 * use GH_INSERT here, to avoid code duplication, but that's more general
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
		GH_BUCKET  *oldbucket = &oldbuckets[i];
		uint32		hash;
		uint32		optimal;

		if (!GH_IS_BUCKET_IN_USE(oldbucket))
		{
			startelem = i;
			break;
		}

		hash = oldbucket->hashvalue;
		optimal = GH_INITIAL_BUCKET(tb, hash);

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
		GH_BUCKET  *oldbucket = &oldbuckets[copyelem];

		if (GH_IS_BUCKET_IN_USE(oldbucket))
		{
			uint32		hash;
			uint32		startelem;
			uint32		curelem;
			GH_BUCKET  *newbucket;

			hash = oldbucket->hashvalue;
			startelem = GH_INITIAL_BUCKET(tb, hash);
			curelem = startelem;

			/* find empty element to put data into */
			for (;;)
			{
				newbucket = &newbuckets[curelem];

				if (!GH_IS_BUCKET_IN_USE(newbucket))
					break;

				curelem = GH_NEXT(tb, curelem, startelem);
			}

			/* copy entry to new slot */
			memcpy(newbucket, oldbucket, sizeof(GH_BUCKET));
		}

		/* can't use GH_NEXT here, would use new size */
		copyelem++;
		if (copyelem >= oldsize)
			copyelem = 0;
	}

	GH_FREE(oldbuckets);

	/*
	 * Enlarge the segment array so we can store enough segments for the new
	 * hash table capacity.
	 */
	tb->segments = GH_ALLOCATE(sizeof(GH_SEGMENT *) * tb->nsegments);
	memcpy(tb->segments, oldsegments, sizeof(GH_SEGMENT *) * oldnsegments);
	/* zero the newly extended part of the array */
	memset(&tb->segments[oldnsegments], 0, sizeof(GH_SEGMENT *) *
		   (tb->nsegments - oldnsegments));
	GH_FREE(oldsegments);

	/*
	 * The majority of tables will only ever need one bitmap word to store
	 * used segments, so we only bother to reallocate the used_segments array
	 * if the number of bitmap words has actually changed.
	 */
	if (tb->used_segment_words != oldusedsegmentwords)
	{
		tb->used_segments = GH_ALLOCATE(sizeof(GH_BITMAP_WORD) *
										tb->used_segment_words);
		memcpy(tb->used_segments, oldusedsegments, sizeof(GH_BITMAP_WORD) *
			   oldusedsegmentwords);
		memset(&tb->used_segments[oldusedsegmentwords], 0,
			   sizeof(GH_BITMAP_WORD) * (tb->used_segment_words -
										 oldusedsegmentwords));

		GH_FREE(oldusedsegments);
	}
}

/*
 * This is a separate static inline function, so it can be reliably be inlined
 * into its wrapper functions even if GH_SCOPE is extern.
 */
static inline GH_ELEMENT_TYPE *
GH_INSERT_HASH_INTERNAL(GH_TYPE * tb, GH_KEY_TYPE key, uint32 hash, bool *found)
{
	uint32		startelem;
	uint32		curelem;
	GH_BUCKET  *buckets;
	uint32		insertdist;

restart:
	insertdist = 0;

	/*
	 * To avoid doing the grow check inside the loop, we do the grow check
	 * regardless of if the key is present.  This also lets us avoid having to
	 * re-find our position in the hashtable after resizing.
	 *
	 * Note that this also reached when resizing the table due to
	 * GH_GROW_MAX_DIB / GH_GROW_MAX_MOVE.
	 */
	if (unlikely(tb->members >= tb->grow_threshold))
	{
		/* this may wrap back to 0 when we're already at GH_MAX_SIZE */
		GH_GROW(tb, tb->size * 2);
	}

	/* perform the insert starting the bucket search at optimal location */
	buckets = tb->buckets;
	startelem = GH_INITIAL_BUCKET(tb, hash);
	curelem = startelem;
	for (;;)
	{
		GH_BUCKET  *bucket = &buckets[curelem];
		GH_ELEMENT_TYPE *entry;
		uint32		curdist;
		uint32		curhash;
		uint32		curoptimal;

		/* any empty bucket can directly be used */
		if (!GH_IS_BUCKET_IN_USE(bucket))
		{
			uint32		index;

			/* and add the new entry */
			tb->members++;

			entry = GH_GET_NEXT_UNUSED_ENTRY(tb, &index);
			entry->GH_KEY = key;
			bucket->hashvalue = hash;
			GH_SET_BUCKET_IN_USE(bucket, index);
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
			entry = GH_INDEX_TO_ELEMENT(tb, bucket->index);

			/* if we find a match, we're done */
			if (GH_EQUAL(tb, key, entry->GH_KEY))
			{
				Assert(GH_IS_BUCKET_IN_USE(bucket));
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
		curoptimal = GH_INITIAL_BUCKET(tb, curhash);
		curdist = GH_DISTANCE_FROM_OPTIMAL(tb, curoptimal, curelem);

		if (insertdist > curdist)
		{
			GH_ELEMENT_TYPE *entry;
			GH_BUCKET  *lastbucket = bucket;
			uint32		emptyelem = curelem;
			uint32		moveelem;
			int32		emptydist = 0;
			uint32		index;

			/* find next empty bucket */
			for (;;)
			{
				GH_BUCKET  *emptybucket;

				emptyelem = GH_NEXT(tb, emptyelem, startelem);
				emptybucket = &buckets[emptyelem];

				if (!GH_IS_BUCKET_IN_USE(emptybucket))
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
				if (unlikely(++emptydist > GH_GROW_MAX_MOVE) &&
					((double) tb->members / tb->size) >= GH_GROW_MIN_FILLFACTOR)
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
				GH_BUCKET  *movebucket;

				moveelem = GH_PREV(tb, moveelem, startelem);
				movebucket = &buckets[moveelem];

				memcpy(lastbucket, movebucket, sizeof(GH_BUCKET));
				lastbucket = movebucket;
			}

			/* and add the new entry */
			tb->members++;

			entry = GH_GET_NEXT_UNUSED_ENTRY(tb, &index);
			entry->GH_KEY = key;
			bucket->hashvalue = hash;
			GH_SET_BUCKET_IN_USE(bucket, index);
			*found = false;
			return entry;
		}

		curelem = GH_NEXT(tb, curelem, startelem);
		insertdist++;

		/*
		 * To avoid negative consequences from overly imbalanced hashtables,
		 * grow the hashtable if collisions lead to large runs. The most
		 * likely cause of such imbalance is filling a (currently) small
		 * table, from a currently big one, in hashtable order.  Don't grow if
		 * the hashtable would be too empty, to prevent quick space explosion
		 * for some weird edge cases.
		 */
		if (unlikely(insertdist > GH_GROW_MAX_DIB) &&
			((double) tb->members / tb->size) >= GH_GROW_MIN_FILLFACTOR)
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
GH_SCOPE	GH_ELEMENT_TYPE *
GH_INSERT(GH_TYPE * tb, GH_KEY_TYPE key, bool *found)
{
	uint32		hash = GH_HASH_KEY(tb, key);

	return GH_INSERT_HASH_INTERNAL(tb, key, hash, found);
}

/*
 * Insert the key into the hashtable using an already-calculated hash. Set
 * *found to true if the key already exists, false otherwise. Returns the
 * hashtable entry in either case.
 */
GH_SCOPE	GH_ELEMENT_TYPE *
GH_INSERT_HASH(GH_TYPE * tb, GH_KEY_TYPE key, uint32 hash, bool *found)
{
	return GH_INSERT_HASH_INTERNAL(tb, key, hash, found);
}

/*
 * This is a separate static inline function, so it can be reliably be inlined
 * into its wrapper functions even if GH_SCOPE is extern.
 */
static inline GH_ELEMENT_TYPE *
GH_LOOKUP_HASH_INTERNAL(GH_TYPE * tb, GH_KEY_TYPE key, uint32 hash)
{
	const uint32 startelem = GH_INITIAL_BUCKET(tb, hash);
	uint32		curelem = startelem;

	for (;;)
	{
		GH_BUCKET  *bucket = &tb->buckets[curelem];

		if (!GH_IS_BUCKET_IN_USE(bucket))
			return NULL;

		if (bucket->hashvalue == hash)
		{
			GH_ELEMENT_TYPE *entry;

			/*
			 * The hash value matches so we just need to ensure the key
			 * matches too.  To do that, we need to lookup the entry in the
			 * segments using the index stored in the bucket.
			 */
			entry = GH_INDEX_TO_ELEMENT(tb, bucket->index);

			/* if we find a match, we're done */
			if (GH_EQUAL(tb, key, entry->GH_KEY))
				return entry;
		}

		/*
		 * TODO: we could stop search based on distance. If the current
		 * buckets's distance-from-optimal is smaller than what we've skipped
		 * already, the entry doesn't exist.
		 */

		curelem = GH_NEXT(tb, curelem, startelem);
	}
}

/*
 * Lookup an entry in the hash table.  Returns NULL if key not present.
 */
GH_SCOPE	GH_ELEMENT_TYPE *
GH_LOOKUP(GH_TYPE * tb, GH_KEY_TYPE key)
{
	uint32		hash = GH_HASH_KEY(tb, key);

	return GH_LOOKUP_HASH_INTERNAL(tb, key, hash);
}

/*
 * Lookup an entry in the hash table using an already-calculated hash.
 *
 * Returns NULL if key not present.
 */
GH_SCOPE	GH_ELEMENT_TYPE *
GH_LOOKUP_HASH(GH_TYPE * tb, GH_KEY_TYPE key, uint32 hash)
{
	return GH_LOOKUP_HASH_INTERNAL(tb, key, hash);
}

/*
 * Delete an entry from hash table by key.  Returns whether to-be-deleted key
 * was present.
 */
GH_SCOPE bool
GH_DELETE(GH_TYPE * tb, GH_KEY_TYPE key)
{
	uint32		hash = GH_HASH_KEY(tb, key);
	uint32		startelem = GH_INITIAL_BUCKET(tb, hash);
	uint32		curelem = startelem;

	for (;;)
	{
		GH_BUCKET  *bucket = &tb->buckets[curelem];

		if (!GH_IS_BUCKET_IN_USE(bucket))
			return false;

		if (bucket->hashvalue == hash)
		{
			GH_ELEMENT_TYPE *entry;

			entry = GH_INDEX_TO_ELEMENT(tb, bucket->index);

			if (GH_EQUAL(tb, key, entry->GH_KEY))
			{
				GH_BUCKET  *lastbucket = bucket;

				/* mark the entry as unused */
				GH_REMOVE_ENTRY(tb, bucket->index);
				/* and mark the bucket unused */
				GH_SET_BUCKET_EMPTY(bucket);

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
					GH_BUCKET  *curbucket;
					uint32		curhash;
					uint32		curoptimal;

					curelem = GH_NEXT(tb, curelem, startelem);
					curbucket = &tb->buckets[curelem];

					if (!GH_IS_BUCKET_IN_USE(curbucket))
						break;

					curhash = curbucket->hashvalue;
					curoptimal = GH_INITIAL_BUCKET(tb, curhash);

					/* current is at optimal position, done */
					if (curoptimal == curelem)
					{
						GH_SET_BUCKET_EMPTY(lastbucket);
						break;
					}

					/* shift */
					memcpy(lastbucket, curbucket, sizeof(GH_BUCKET));
					GH_SET_BUCKET_EMPTY(curbucket);

					lastbucket = curbucket;
				}

				return true;
			}
		}
		/* TODO: return false; if the distance is too big */

		curelem = GH_NEXT(tb, curelem, startelem);
	}
}

/*
 * Initialize iterator.
 */
GH_SCOPE void
GH_START_ITERATE(GH_TYPE * tb, GH_ITERATOR * iter)
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
GH_SCOPE	GH_ELEMENT_TYPE *
GH_ITERATE(GH_TYPE * tb, GH_ITERATOR * iter)
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
		GH_SEGMENT *seg;

		/* need a new segment? */
		if (iter->curitemidx == -1)
		{
			iter->cursegidx = GH_NEXT_ONEBIT(tb->used_segments,
											 tb->used_segment_words,
											 iter->cursegidx);

			/* no more segments with items? We're done */
			if (iter->cursegidx == -1)
				return NULL;
		}

		seg = tb->segments[iter->cursegidx];

		/* if the segment has items then it certainly shouldn't be NULL */
		Assert(seg != NULL);
		/* advance to the next used item in this segment */
		iter->curitemidx = GH_NEXT_ONEBIT(seg->used_items, GH_BITMAP_WORDS,
										  iter->curitemidx);
		if (iter->curitemidx >= 0)
		{
			iter->found_members++;
			return &seg->items[iter->curitemidx];
		}

		/*
		 * GH_NEXT_ONEBIT returns -1 when there are no more bits.  We just
		 * loop again to fetch the next segment.
		 */
	}
}

#endif							/* GH_DEFINE */

/* undefine external parameters, so next hash table can be defined */
#undef GH_PREFIX
#undef GH_KEY_TYPE
#undef GH_KEY
#undef GH_ELEMENT_TYPE
#undef GH_HASH_KEY
#undef GH_SCOPE
#undef GH_DECLARE
#undef GH_DEFINE
#undef GH_EQUAL
#undef GH_ALLOCATE
#undef GH_ALLOCATE_ZERO
#undef GH_FREE

/* undefine locally declared macros */
#undef GH_MAKE_PREFIX
#undef GH_MAKE_NAME
#undef GH_MAKE_NAME_
#undef GH_ITEMS_PER_SEGMENT
#undef GH_UNUSED_BUCKET_INDEX
#undef GH_INDEX_SEGMENT
#undef GH_INDEX_ITEM
#undef GH_BITS_PER_WORD
#undef GH_BITMAP_WORD
#undef GH_RIGHTMOST_ONE_POS
#undef GH_BITMAP_WORDS
#undef GH_WORDNUM
#undef GH_BITNUM
#undef GH_RAW_ALLOCATOR
#undef GH_MAX_SIZE
#undef GH_FILLFACTOR
#undef GH_MAX_FILLFACTOR
#undef GH_GROW_MAX_DIB
#undef GH_GROW_MAX_MOVE
#undef GH_GROW_MIN_FILLFACTOR

/* types */
#undef GH_TYPE
#undef GH_BUCKET
#undef GH_SEGMENT
#undef GH_ITERATOR

/* external function names */
#undef GH_CREATE
#undef GH_DESTROY
#undef GH_RESET
#undef GH_INSERT
#undef GH_INSERT_HASH
#undef GH_DELETE
#undef GH_LOOKUP
#undef GH_LOOKUP_HASH
#undef GH_GROW
#undef GH_START_ITERATE
#undef GH_ITERATE

/* internal function names */
#undef GH_NEXT_ONEBIT
#undef GH_NEXT_ZEROBIT
#undef GH_INDEX_TO_ELEMENT
#undef GH_MARK_SEGMENT_ITEM_USED
#undef GH_MARK_SEGMENT_ITEM_UNUSED
#undef GH_GET_NEXT_UNUSED_ENTRY
#undef GH_REMOVE_ENTRY
#undef GH_SET_BUCKET_IN_USE
#undef GH_SET_BUCKET_EMPTY
#undef GH_IS_BUCKET_IN_USE
#undef GH_COMPUTE_PARAMETERS
#undef GH_NEXT
#undef GH_PREV
#undef GH_DISTANCE_FROM_OPTIMAL
#undef GH_INITIAL_BUCKET
#undef GH_INSERT_HASH_INTERNAL
#undef GH_LOOKUP_HASH_INTERNAL
