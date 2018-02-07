/*
 * brin_bloom.c
 *		Implementation of Bloom opclass for BRIN
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/brin/brin_bloom.c
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/brin_internal.h"
#include "access/brin_tuple.h"
#include "access/hash.h"
#include "access/stratnum.h"
#include "catalog/pg_type.h"
#include "catalog/pg_amop.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include <math.h>

#define BloomEqualStrategyNumber	1

/*
 * Additional SQL level support functions
 *
 * Procedure numbers must not use values reserved for BRIN itself; see
 * brin_internal.h.
 */
#define		BLOOM_MAX_PROCNUMS		4	/* maximum support procs we need */
#define		PROCNUM_HASH			11	/* required */

/*
 * Subtract this from procnum to obtain index in BloomOpaque arrays
 * (Must be equal to minimum of private procnums).
 */
#define		PROCNUM_BASE			11


#define		BLOOM_PHASE_SORTED		1
#define		BLOOM_PHASE_HASH		2

/* how many hashes to accumulate before hashing */
#define		BLOOM_MAX_UNSORTED		32
#define		BLOOM_GROW_BYTES		32
#define		BLOOM_NDISTINCT			1000	/* number of distinct values */
#define		BLOOM_ERROR_RATE		0.05	/* 2% false positive rate */

/*
 * Bloom Filter
 *
 * Represents a bloom filter, built on hashes of the indexed values. That is,
 * we compute a uint32 hash of the value, and then store this hash into the
 * bloom filter (and compute additional hashes on it).
 *
 * We use an optimisation that initially we store the uint32 values directly,
 * without the extra hashing step. And only later filling the bitmap space,
 * we switch to the regular bloom filter mode.
 *
 * PHASE_SORTED
 *
 * Initially we copy the uint32 hash into the bitmap, regularly sorting the
 * hash values for fast lookup (we keep at most BLOOM_MAX_UNSORTED unsorted
 * values).
 *
 * The idea is that if we only see very few distinct values, we can store
 * them in less space compared to the (sparse) bloom filter bitmap. It also
 * stores them exactly, although that's not a big advantage as almost-empty
 * bloom filter has false positive rate close to zero anyway.
 *
 * PHASE_HASH
 *
 * Once we fill the bitmap space in the sorted phase, we switch to the hash
 * phase, where we actually use the bloom filter. We treat the uint32 hashes
 * as input values, and hash them again with different seeds (to get the k
 * hash functions needed for bloom filter).
 *
 *
 * XXX Maybe instead of explicitly limiting the number of unsorted values
 * by BLOOM_MAX_UNSORTED, we should cap them by (filter size / 4B), i.e.
 * allow up to the whole filter size.
 *
 * XXX Perhaps we could save a few bytes by using different data types, but
 * considering the size of the bitmap, the difference is negligible.
 *
 * XXX We could also implement "sparse" bloom filters, keeping only the
 * bytes that are not entirely 0. That might make the "sorted" phase
 * mostly unnecessary.
 *
 * XXX We can also watch the number of bits set in the bloom filter, and
 * then stop using it (and not store the bitmap, to save space) when the
 * false positive rate gets too high.
 */
typedef struct BloomFilter
{
	/* varlena header (do not touch directly!) */
	int32	vl_len_;

	/* global bloom filter parameters */
	uint32	phase;		/* phase (initially SORTED, then HASH) */
	uint32	nhashes;	/* number of hash functions */
	uint32	nbits;		/* number of bits in the bitmap (optimal) */
	uint32	nbits_set;	/* number of bits set to 1 */

	/* fields used only in the EXACT phase */
	uint32	nvalues;	/* number of hashes stored (sorted + extra) */
	uint32	nsorted;	/* number of uint32 hashes in sorted part */

	/* bitmap of the bloom filter */
	char 	bitmap[FLEXIBLE_ARRAY_MEMBER];

} BloomFilter;

/*
 * bloom_init
 * 		Initialize the Bloom Filter, allocate all the memory.
 *
 * The filter is initialized with optimal size for ndistinct expected
 * values, and requested false positive rate. The filter is stored as
 * varlena.
 */
static BloomFilter *
bloom_init(int ndistinct, double false_positive_rate)
{
	Size			len;
	BloomFilter	   *filter;

	/* https://en.wikipedia.org/wiki/Bloom_filter */
	int m;	/* number of bits */
	int k;	/* number of hash functions */

	Assert((ndistinct > 0) && (ndistinct < 10000));	/* 10k is mostly arbitrary limit */
	Assert((false_positive_rate > 0) && (false_positive_rate < 1.0));

	m = ceil((ndistinct * log(false_positive_rate)) / log(1.0 / (pow(2.0, log(2.0)))));

	/* round m to whole bytes */
	m = ((m + 7) / 8) * 8;

	k = round(log(2.0) * m / ndistinct);

	/*
	 * Allocate the bloom filter with a minimum size 64B (about 40B in the
	 * bitmap part). We require space at least for the header.
	 */
	len = Max(offsetof(BloomFilter, bitmap), 64);

	filter = (BloomFilter *) palloc0(len);

	filter->phase = BLOOM_PHASE_SORTED;
	filter->nhashes = k;
	filter->nbits = m;

	SET_VARSIZE(filter, len);

	return filter;
}

/* simple uint32 comparator, for pg_qsort and bsearch */
static int
cmp_uint32(const void *a, const void *b)
{
	uint32 *ia = (uint32 *) a;
	uint32 *ib = (uint32 *) b;

	if (*ia == *ib)
		return 0;
	else if (*ia < *ib)
		return -1;
	else
		return 1;
}

/*
 * bloom_compact
 *		Compact the filter during the 'sorted' phase.
 *
 * We sort the uint32 hashes and remove duplicates, for two main reasons.
 * Firstly, to keep most of the data sorted for bsearch lookups. Secondly,
 * we try to save space by removing the duplicates, allowing us to stay
 * in the sorted phase a bit longer.
 *
 * We currently don't repalloc the bitmap, i.e. we don't free the memory
 * here - in the worst case we waste space for up to 32 unsorted hashes
 * (if all of them are already in the sorted part), so about 128B. We can
 * either reduce the number of unsorted items (e.g. to 8 hashes, which
 * would mean 32B), or start doing the repalloc.
 *
 * XXX Actually, we don't need to do repalloc - we just need to set the
 * varlena header length!
 */
static void
bloom_compact(BloomFilter *filter)
{
	int		i,
			nvalues;
	uint32 *values;

	/* never call compact on filters in HASH phase */
	Assert(filter->phase == BLOOM_PHASE_SORTED);

	/* no chance to compact anything */
	if (filter->nvalues == filter->nsorted)
		return;

	values = (uint32 *) palloc(filter->nvalues * sizeof(uint32));

	/* copy the data, then reset the bitmap */
	memcpy(values, filter->bitmap, filter->nvalues * sizeof(uint32));
	memset(filter->bitmap, 0, filter->nvalues * sizeof(uint32));

	/* FIXME optimization: sort only the unsorted part, then merge */
	pg_qsort(values, filter->nvalues, sizeof(uint32), cmp_uint32);

	nvalues = 1;
	for (i = 1; i < filter->nvalues; i++)
	{
		/* if a new value, keep it */
		if (values[i] != values[i-1])
		{
			values[nvalues] = values[i];
			nvalues++;
		}
	}

	filter->nvalues = nvalues;
	filter->nsorted = nvalues;

	memcpy(filter->bitmap, values, nvalues * sizeof(uint32));

	pfree(values);
}

static BloomFilter *
bloom_switch_to_hashing(BloomFilter *filter);

/*
 * bloom_add_value
 * 		Add value to the bloom filter.
 */
static BloomFilter *
bloom_add_value(BloomFilter *filter, uint32 value, bool *updated)
{
	int		i;
	uint32	big_h, h, d;

	/* assume 'not updated' by default */
	Assert(filter);

	/* if we're in the sorted phase, we store the hashes directly */
	if (filter->phase == BLOOM_PHASE_SORTED)
	{
		/* how many uint32 hashes can we fit into the bitmap */
		int maxvalues = filter->nbits / (8 * sizeof(uint32));

		/* do not overflow the bitmap space or number of unsorted items */
		Assert(filter->nvalues <= maxvalues);
		Assert(filter->nvalues - filter->nsorted <= BLOOM_MAX_UNSORTED);

		/*
		 * In this branch we always update the filter - we either add the
		 * hash to the unsorted part, or switch the filter to hashing.
		 */
		if (updated)
			*updated = true;

		/*
		 * If the array is full, or if we reached the limit on unsorted
		 * items, try to compact the filter first, before attempting to
		 * add the new value.
		 */
		if ((filter->nvalues == maxvalues) ||
			(filter->nvalues - filter->nsorted == BLOOM_MAX_UNSORTED))
				bloom_compact(filter);

		/*
		 * Can we squeeze one more uint32 hash into the bitmap? Also make
		 * sure there's enough space in the bytea value first.
		 */
		if (filter->nvalues < maxvalues)
		{
			Size len = VARSIZE_ANY(filter);
			Size need = offsetof(BloomFilter,bitmap) + (filter->nvalues+1) * sizeof(uint32);

			if (len < need)
			{
				/*
				 * We don't double the size here, as in the first place we care about
				 * reducing storage requirements, and the doubling happens automatically
				 * in memory contexts anyway.
				 *
				 * XXX Zero the newly allocated part. Maybe not really needed?
				 */
				filter = (BloomFilter *) repalloc(filter, len + BLOOM_GROW_BYTES);
				memset((char *)filter + len, 0, BLOOM_GROW_BYTES);
				SET_VARSIZE(filter, len + BLOOM_GROW_BYTES);
			}

			/* copy the data into the bitmap */
			memcpy(&filter->bitmap[filter->nvalues * sizeof(uint32)],
				   &value, sizeof(uint32));

			filter->nvalues++;

			/* we're done */
			return filter;
		}

		/* can't add any more exact hashes, so switch to hashing */
		filter = bloom_switch_to_hashing(filter);
	}

	/* we better be in the hashing phase */
	Assert(filter->phase == BLOOM_PHASE_HASH);

    /* compute the hashes, used for the bloom filter */
    big_h = ((uint32)DatumGetInt64(hash_uint32(value)));

    h = big_h % filter->nbits;
    d = big_h % (filter->nbits - 1); 

	/* compute the requested number of hashes */
	for (i = 0; i < filter->nhashes; i++)
	{
		int byte = (h / 8);
		int bit  = (h % 8);

		/* if the bit is not set, set it and remember we did that */
		if (! (filter->bitmap[byte] & (0x01 << bit)))
		{
			filter->bitmap[byte] |= (0x01 << bit);
			filter->nbits_set++;
			if (updated)
				*updated = true;
		}

		/* next bit */
        h += d++;
        if (h >= filter->nbits) h -= filter->nbits;
        if (d == filter->nbits) d = 0; 
	}

	return filter;
}

/*
 * bloom_switch_to_hashing
 * 		Switch the bloom filter from sorted to hashing mode.
 */
static BloomFilter *
bloom_switch_to_hashing(BloomFilter *filter)
{
	int		i;
	uint32 *values;
	Size			len;
	BloomFilter	   *newfilter;

	/*
	 * The new filter is allocated with all the memory, directly into
	 * the HASH phase.
	 */
	len = offsetof(BloomFilter, bitmap) + (filter->nbits / 8);

	newfilter = (BloomFilter *) palloc0(len);

	newfilter->nhashes = filter->nhashes;
	newfilter->nbits = filter->nbits;
	newfilter->phase = BLOOM_PHASE_HASH;

	SET_VARSIZE(newfilter, len);

	values = (uint32 *) filter->bitmap;

	for (i = 0; i < filter->nvalues; i++)
		/* ignore the return value here, re don't repalloc in hashing mode */
		bloom_add_value(newfilter, values[i], NULL);

	/* free the original filter, return the newly allocated one */
	pfree(filter);

	return newfilter;
}

/*
 * bloom_contains_value
 * 		Check if the bloom filter contains a particular value.
 */
static bool
bloom_contains_value(BloomFilter *filter, uint32 value)
{
	int		i;
	uint32	big_h, h, d;

	Assert(filter);

	/* in sorted mode we simply search the two arrays (sorted, unsorted) */
	if (filter->phase == BLOOM_PHASE_SORTED)
	{
		int i;
		uint32 *values = (uint32 *)filter->bitmap;

		/* first search through the sorted part */
		if ((filter->nsorted > 0) &&
			(bsearch(&value, values, filter->nsorted, sizeof(uint32), cmp_uint32) != NULL))
			return true;

		/* now search through the unsorted part - linear search */
		for (i = filter->nsorted; i < filter->nvalues; i++)
			if (value == values[i])
				return true;

		/* nothing found */
		return false;
	}

	/* now the regular hashing mode */
	Assert(filter->phase == BLOOM_PHASE_HASH);

    big_h = ((uint32)DatumGetInt64(hash_uint32(value)));

    h = big_h % filter->nbits;
    d = big_h % (filter->nbits - 1); 

	/* compute the requested number of hashes */
	for (i = 0; i < filter->nhashes; i++)
	{
		int byte = (h / 8);
		int bit  = (h % 8);

		/* if the bit is not set, the value is not there */
		if (! (filter->bitmap[byte] & (0x01 << bit)))
			return false;

		/* next bit */
        h += d++;
        if (h >= filter->nbits) h -= filter->nbits;
        if (d == filter->nbits) d = 0; 
	}

	/* all hashes found in bloom filter */
	return true;
}

typedef struct BloomOpaque
{
	/*
	 * XXX At this point we only need a single proc (to compute the hash),
	 * but let's keep the array just like inclusion and minman opclasses,
	 * for consistency. We may need additional procs in the future.
	 */
	FmgrInfo	extra_procinfos[BLOOM_MAX_PROCNUMS];
	bool		extra_proc_missing[BLOOM_MAX_PROCNUMS];
} BloomOpaque;

static FmgrInfo *bloom_get_procinfo(BrinDesc *bdesc, uint16 attno,
					   uint16 procnum);


Datum
brin_bloom_opcinfo(PG_FUNCTION_ARGS)
{
	BrinOpcInfo *result;

	/*
	 * opaque->strategy_procinfos is initialized lazily; here it is set to
	 * all-uninitialized by palloc0 which sets fn_oid to InvalidOid.
	 * 
	 * bloom indexes only store a the filter as a single BYTEA column
	 */

	result = palloc0(MAXALIGN(SizeofBrinOpcInfo(1)) +
					 sizeof(BloomOpaque));
	result->oi_nstored = 1;
	result->oi_opaque = (BloomOpaque *)
		MAXALIGN((char *) result + SizeofBrinOpcInfo(1));
	result->oi_typcache[0] = lookup_type_cache(BYTEAOID, 0);

	PG_RETURN_POINTER(result);
}

/*
 * Examine the given index tuple (which contains partial status of a certain
 * page range) by comparing it to the given value that comes from another heap
 * tuple.  If the new value is outside the bloom filter specified by the
 * existing tuple values, update the index tuple and return true.  Otherwise,
 * return false and do not modify in this case.
 */
Datum
brin_bloom_add_value(PG_FUNCTION_ARGS)
{
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *column = (BrinValues *) PG_GETARG_POINTER(1);
	Datum		newval = PG_GETARG_DATUM(2);
	bool		isnull = PG_GETARG_DATUM(3);
	Oid			colloid = PG_GET_COLLATION();
	FmgrInfo   *hashFn;
	uint32		hashValue;
	bool		updated = false;
	AttrNumber	attno;
	BloomFilter *filter;

	/*
	 * If the new value is null, we record that we saw it if it's the first
	 * one; otherwise, there's nothing to do.
	 */
	if (isnull)
	{
		if (column->bv_hasnulls)
			PG_RETURN_BOOL(false);

		column->bv_hasnulls = true;
		PG_RETURN_BOOL(true);
	}

	attno = column->bv_attno;

	/*
	 * If this is the first non-null value, we need to initialize the bloom
	 * filter. Otherwise just extract the existing bloom filter from BrinValues.
	 */
	if (column->bv_allnulls)
	{
		filter = bloom_init(BLOOM_NDISTINCT, BLOOM_ERROR_RATE);
		column->bv_values[0] = PointerGetDatum(filter);
		column->bv_allnulls = false;
		updated = true;
	}
	else
		filter = (BloomFilter *) PG_DETOAST_DATUM(column->bv_values[0]);

	/*
	 * Compute the hash of the new value, using the supplied hash function,
	 * and then add the hash value to the bloom filter.
	 */
	hashFn = bloom_get_procinfo(bdesc, attno, PROCNUM_HASH);

	hashValue = DatumGetUInt32(FunctionCall1Coll(hashFn, colloid, newval));

	filter = bloom_add_value(filter, hashValue, &updated);

	column->bv_values[0] = PointerGetDatum(filter);

	PG_RETURN_BOOL(updated);
}

/*
 * Given an index tuple corresponding to a certain page range and a scan key,
 * return whether the scan key is consistent with the index tuple's bloom
 * filter.  Return true if so, false otherwise.
 */
Datum
brin_bloom_consistent(PG_FUNCTION_ARGS)
{
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *column = (BrinValues *) PG_GETARG_POINTER(1);
	ScanKey	   *keys = (ScanKey *) PG_GETARG_POINTER(2);
	int			nkeys = PG_GETARG_INT32(3);
	Oid			colloid = PG_GET_COLLATION();
	AttrNumber	attno;
	Datum		value;
	Datum		matches;
	FmgrInfo   *finfo;
	uint32		hashValue;
	BloomFilter *filter;
	int			keyno;

	filter = (BloomFilter *) PG_DETOAST_DATUM(column->bv_values[0]);

	Assert(filter);

	matches = true;

	for (keyno = 0; keyno < nkeys; keyno++)
	{
		ScanKey	key = keys[keyno];

		/* NULL keys are handled and filtered-out in bringetbitmap */
		Assert(!(key->sk_flags & SK_ISNULL));

		attno = key->sk_attno;
		value = key->sk_argument;

		switch (key->sk_strategy)
		{
			case BloomEqualStrategyNumber:

				/*
				 * In the equality case (WHERE col = someval), we want to return
				 * the current page range if the minimum value in the range <=
				 * scan key, and the maximum value >= scan key.
				 */
				finfo = bloom_get_procinfo(bdesc, attno, PROCNUM_HASH);

				hashValue = DatumGetUInt32(FunctionCall1Coll(finfo, colloid, value));
				matches &= bloom_contains_value(filter, hashValue);

				break;
			default:
				/* shouldn't happen */
				elog(ERROR, "invalid strategy number %d", key->sk_strategy);
				matches = 0;
				break;
		}

		if (!matches)
			break;
	}

	PG_RETURN_DATUM(matches);
}

/*
 * Given two BrinValues, update the first of them as a union of the summary
 * values contained in both.  The second one is untouched.
 *
 * XXX We assume the bloom filters have the same parameters fow now. In the
 * future we should have 'can union' function, to decide if we can combine
 * two particular bloom filters.
 */
Datum
brin_bloom_union(PG_FUNCTION_ARGS)
{
	BrinValues *col_a = (BrinValues *) PG_GETARG_POINTER(1);
	BrinValues *col_b = (BrinValues *) PG_GETARG_POINTER(2);
	BloomFilter *filter_a;
	BloomFilter *filter_b;

	Assert(col_a->bv_attno == col_b->bv_attno);

	/* Adjust "hasnulls" */
	if (!col_a->bv_hasnulls && col_b->bv_hasnulls)
		col_a->bv_hasnulls = true;

	/* If there are no values in B, there's nothing left to do */
	if (col_b->bv_allnulls)
		PG_RETURN_VOID();

	/*
	 * Adjust "allnulls".  If A doesn't have values, just copy the values from
	 * B into A, and we're done.  We cannot run the operators in this case,
	 * because values in A might contain garbage.  Note we already established
	 * that B contains values.
	 */
	if (col_a->bv_allnulls)
	{
		col_a->bv_allnulls = false;
		col_a->bv_values[0] = datumCopy(col_b->bv_values[0], false, -1);
		PG_RETURN_VOID();
	}

	filter_a = (BloomFilter *) PG_DETOAST_DATUM(col_a->bv_values[0]);
	filter_b = (BloomFilter *) PG_DETOAST_DATUM(col_b->bv_values[0]);

	/* make sure neither of the bloom filters is NULL */
	Assert(filter_a && filter_b);
	Assert(filter_a->nbits == filter_b->nbits);

	/*
	 * Merging of the filters depends on the phase of both filters.
	 */
	if (filter_b->phase == BLOOM_PHASE_SORTED)
	{
		/*
		 * Simply read all items from 'b' and add them to 'a' (the phase of
		 * 'a' does not really matter).
		 */
		int		i;
		uint32 *values = (uint32 *) filter_b->bitmap;

		for (i = 0; i < filter_b->nvalues; i++)
			filter_a = bloom_add_value(filter_a, values[i], NULL);

		col_a->bv_values[0] = PointerGetDatum(filter_a);
	}
	else if (filter_a->phase == BLOOM_PHASE_SORTED)
	{
		/*
		 * 'b' hashed, 'a' sorted - copy 'b' into 'a' and then add all values
		 * from 'a' into the new copy.
		 */
		int		i;
		BloomFilter *filter_c;
		uint32 *values = (uint32 *) filter_a->bitmap;

		filter_c = (BloomFilter *) PG_DETOAST_DATUM(datumCopy(PointerGetDatum(filter_b), false, -1));

		for (i = 0; i < filter_a->nvalues; i++)
			filter_c = bloom_add_value(filter_c, values[i], NULL);

		col_a->bv_values[0] = PointerGetDatum(filter_c);
	}
	else if (filter_a->phase == BLOOM_PHASE_HASH)
	{
		/*
		 * 'b' hashed, 'a' hashed - merge the bitmaps by OR
		 */
		int		i;
		int		nbytes = (filter_a->nbits + 7) / 8;

		for (i = 0; i < nbytes; i++)
			filter_a->bitmap[i] |= filter_b->bitmap[i];
	}

	PG_RETURN_VOID();
}

/*
 * Cache and return inclusion opclass support procedure
 *
 * Return the procedure corresponding to the given function support number
 * or null if it is not exists.
 */
static FmgrInfo *
bloom_get_procinfo(BrinDesc *bdesc, uint16 attno, uint16 procnum)
{
	BloomOpaque *opaque;
	uint16		basenum = procnum - PROCNUM_BASE;

	/*
	 * We cache these in the opaque struct, to avoid repetitive syscache
	 * lookups.
	 */
	opaque = (BloomOpaque *) bdesc->bd_info[attno - 1]->oi_opaque;

	/*
	 * If we already searched for this proc and didn't find it, don't bother
	 * searching again.
	 */
	if (opaque->extra_proc_missing[basenum])
		return NULL;

	if (opaque->extra_procinfos[basenum].fn_oid == InvalidOid)
	{
		if (RegProcedureIsValid(index_getprocid(bdesc->bd_index, attno,
												procnum)))
		{
			fmgr_info_copy(&opaque->extra_procinfos[basenum],
						   index_getprocinfo(bdesc->bd_index, attno, procnum),
						   bdesc->bd_context);
		}
		else
		{
			opaque->extra_proc_missing[basenum] = true;
			return NULL;
		}
	}

	return &opaque->extra_procinfos[basenum];
}
