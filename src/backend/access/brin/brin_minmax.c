/*
 * brin_minmax.c
 *		Implementation of Min/Max opclass for BRIN
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/brin/brin_minmax.c
 */
#include "postgres.h"

#include "access/brin.h"
#include "access/brin_internal.h"
#include "access/brin_revmap.h"
#include "access/brin_tuple.h"
#include "access/genam.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#ifdef DEBUG_BRIN_STATS
bool debug_brin_stats = false;
bool debug_brin_cross_check = false;
#endif

typedef struct MinmaxOpaque
{
	Oid			cached_subtype;
	FmgrInfo	strategy_procinfos[BTMaxStrategyNumber];
} MinmaxOpaque;

static FmgrInfo *minmax_get_strategy_procinfo(BrinDesc *bdesc, uint16 attno,
											  Oid subtype, uint16 strategynum);


Datum
brin_minmax_opcinfo(PG_FUNCTION_ARGS)
{
	Oid			typoid = PG_GETARG_OID(0);
	BrinOpcInfo *result;

	/*
	 * opaque->strategy_procinfos is initialized lazily; here it is set to
	 * all-uninitialized by palloc0 which sets fn_oid to InvalidOid.
	 */

	result = palloc0(MAXALIGN(SizeofBrinOpcInfo(2)) +
					 sizeof(MinmaxOpaque));
	result->oi_nstored = 2;
	result->oi_regular_nulls = true;
	result->oi_opaque = (MinmaxOpaque *)
		MAXALIGN((char *) result + SizeofBrinOpcInfo(2));
	result->oi_typcache[0] = result->oi_typcache[1] =
		lookup_type_cache(typoid, 0);

	PG_RETURN_POINTER(result);
}

/*
 * Examine the given index tuple (which contains partial status of a certain
 * page range) by comparing it to the given value that comes from another heap
 * tuple.  If the new value is outside the min/max range specified by the
 * existing tuple values, update the index tuple and return true.  Otherwise,
 * return false and do not modify in this case.
 */
Datum
brin_minmax_add_value(PG_FUNCTION_ARGS)
{
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *column = (BrinValues *) PG_GETARG_POINTER(1);
	Datum		newval = PG_GETARG_DATUM(2);
	bool		isnull PG_USED_FOR_ASSERTS_ONLY = PG_GETARG_DATUM(3);
	Oid			colloid = PG_GET_COLLATION();
	FmgrInfo   *cmpFn;
	Datum		compar;
	bool		updated = false;
	Form_pg_attribute attr;
	AttrNumber	attno;

	Assert(!isnull);

	attno = column->bv_attno;
	attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);

	/*
	 * If the recorded value is null, store the new value (which we know to be
	 * not null) as both minimum and maximum, and we're done.
	 */
	if (column->bv_allnulls)
	{
		column->bv_values[0] = datumCopy(newval, attr->attbyval, attr->attlen);
		column->bv_values[1] = datumCopy(newval, attr->attbyval, attr->attlen);
		column->bv_allnulls = false;
		PG_RETURN_BOOL(true);
	}

	/*
	 * Otherwise, need to compare the new value with the existing boundaries
	 * and update them accordingly.  First check if it's less than the
	 * existing minimum.
	 */
	cmpFn = minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
										 BTLessStrategyNumber);
	compar = FunctionCall2Coll(cmpFn, colloid, newval, column->bv_values[0]);
	if (DatumGetBool(compar))
	{
		if (!attr->attbyval)
			pfree(DatumGetPointer(column->bv_values[0]));
		column->bv_values[0] = datumCopy(newval, attr->attbyval, attr->attlen);
		updated = true;
	}

	/*
	 * And now compare it to the existing maximum.
	 */
	cmpFn = minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
										 BTGreaterStrategyNumber);
	compar = FunctionCall2Coll(cmpFn, colloid, newval, column->bv_values[1]);
	if (DatumGetBool(compar))
	{
		if (!attr->attbyval)
			pfree(DatumGetPointer(column->bv_values[1]));
		column->bv_values[1] = datumCopy(newval, attr->attbyval, attr->attlen);
		updated = true;
	}

	PG_RETURN_BOOL(updated);
}

/*
 * Given an index tuple corresponding to a certain page range and a scan key,
 * return whether the scan key is consistent with the index tuple's min/max
 * values.  Return true if so, false otherwise.
 *
 * We're no longer dealing with NULL keys in the consistent function, that is
 * now handled by the AM code. That means we should not get any all-NULL ranges
 * either, because those can't be consistent with regular (not [IS] NULL) keys.
 */
Datum
brin_minmax_consistent(PG_FUNCTION_ARGS)
{
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *column = (BrinValues *) PG_GETARG_POINTER(1);
	ScanKey		key = (ScanKey) PG_GETARG_POINTER(2);
	Oid			colloid = PG_GET_COLLATION(),
				subtype;
	AttrNumber	attno;
	Datum		value;
	Datum		matches;
	FmgrInfo   *finfo;

	/* This opclass uses the old signature with only three arguments. */
	Assert(PG_NARGS() == 3);

	/* Should not be dealing with all-NULL ranges. */
	Assert(!column->bv_allnulls);

	attno = key->sk_attno;
	subtype = key->sk_subtype;
	value = key->sk_argument;
	switch (key->sk_strategy)
	{
		case BTLessStrategyNumber:
		case BTLessEqualStrategyNumber:
			finfo = minmax_get_strategy_procinfo(bdesc, attno, subtype,
												 key->sk_strategy);
			matches = FunctionCall2Coll(finfo, colloid, column->bv_values[0],
										value);
			break;
		case BTEqualStrategyNumber:

			/*
			 * In the equality case (WHERE col = someval), we want to return
			 * the current page range if the minimum value in the range <=
			 * scan key, and the maximum value >= scan key.
			 */
			finfo = minmax_get_strategy_procinfo(bdesc, attno, subtype,
												 BTLessEqualStrategyNumber);
			matches = FunctionCall2Coll(finfo, colloid, column->bv_values[0],
										value);
			if (!DatumGetBool(matches))
				break;
			/* max() >= scankey */
			finfo = minmax_get_strategy_procinfo(bdesc, attno, subtype,
												 BTGreaterEqualStrategyNumber);
			matches = FunctionCall2Coll(finfo, colloid, column->bv_values[1],
										value);
			break;
		case BTGreaterEqualStrategyNumber:
		case BTGreaterStrategyNumber:
			finfo = minmax_get_strategy_procinfo(bdesc, attno, subtype,
												 key->sk_strategy);
			matches = FunctionCall2Coll(finfo, colloid, column->bv_values[1],
										value);
			break;
		default:
			/* shouldn't happen */
			elog(ERROR, "invalid strategy number %d", key->sk_strategy);
			matches = 0;
			break;
	}

	PG_RETURN_DATUM(matches);
}

/*
 * Given two BrinValues, update the first of them as a union of the summary
 * values contained in both.  The second one is untouched.
 */
Datum
brin_minmax_union(PG_FUNCTION_ARGS)
{
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *col_a = (BrinValues *) PG_GETARG_POINTER(1);
	BrinValues *col_b = (BrinValues *) PG_GETARG_POINTER(2);
	Oid			colloid = PG_GET_COLLATION();
	AttrNumber	attno;
	Form_pg_attribute attr;
	FmgrInfo   *finfo;
	bool		needsadj;

	Assert(col_a->bv_attno == col_b->bv_attno);
	Assert(!col_a->bv_allnulls && !col_b->bv_allnulls);

	attno = col_a->bv_attno;
	attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);

	/* Adjust minimum, if B's min is less than A's min */
	finfo = minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
										 BTLessStrategyNumber);
	needsadj = FunctionCall2Coll(finfo, colloid, col_b->bv_values[0],
								 col_a->bv_values[0]);
	if (needsadj)
	{
		if (!attr->attbyval)
			pfree(DatumGetPointer(col_a->bv_values[0]));
		col_a->bv_values[0] = datumCopy(col_b->bv_values[0],
										attr->attbyval, attr->attlen);
	}

	/* Adjust maximum, if B's max is greater than A's max */
	finfo = minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
										 BTGreaterStrategyNumber);
	needsadj = FunctionCall2Coll(finfo, colloid, col_b->bv_values[1],
								 col_a->bv_values[1]);
	if (needsadj)
	{
		if (!attr->attbyval)
			pfree(DatumGetPointer(col_a->bv_values[1]));
		col_a->bv_values[1] = datumCopy(col_b->bv_values[1],
										attr->attbyval, attr->attlen);
	}

	PG_RETURN_VOID();
}

/* FIXME copy of a private struct from brin.c */
typedef struct BrinOpaque
{
	BlockNumber bo_pagesPerRange;
	BrinRevmap *bo_rmAccess;
	BrinDesc   *bo_bdesc;
} BrinOpaque;

/*
 * Compare ranges by minval (collation and operator are taken from the extra
 * argument, which is expected to be TypeCacheEntry).
 */
static int
range_minval_cmp(const void *a, const void *b, void *arg)
{
	BrinRange *ra = *(BrinRange **) a;
	BrinRange *rb = *(BrinRange **) b;
	TypeCacheEntry *typentry = (TypeCacheEntry *) arg;
	FmgrInfo   *cmpfunc = &typentry->cmp_proc_finfo;
	Datum	c;
	int		r;

	c = FunctionCall2Coll(cmpfunc, typentry->typcollation,
						  ra->min_value, rb->min_value);
	r = DatumGetInt32(c);

	if (r != 0)
		return r;

	if (ra->blkno_start < rb->blkno_start)
		return -1;
	else
		return 1;
}

/*
 * Compare ranges by maxval (collation and operator are taken from the extra
 * argument, which is expected to be TypeCacheEntry).
 */
static int
range_maxval_cmp(const void *a, const void *b, void *arg)
{
	BrinRange *ra = *(BrinRange **) a;
	BrinRange *rb = *(BrinRange **) b;
	TypeCacheEntry *typentry = (TypeCacheEntry *) arg;
	FmgrInfo   *cmpfunc = &typentry->cmp_proc_finfo;
	Datum	c;
	int		r;

	c = FunctionCall2Coll(cmpfunc, typentry->typcollation,
						  ra->max_value, rb->max_value);
	r = DatumGetInt32(c);

	if (r != 0)
		return r;

	if (ra->blkno_start < rb->blkno_start)
		return -1;
	else
		return 1;
}

/* compare values using an operator from typcache */
static int
range_values_cmp(const void *a, const void *b, void *arg)
{
	Datum	da = * (Datum *) a;
	Datum	db = * (Datum *) b;
	TypeCacheEntry *typentry = (TypeCacheEntry *) arg;
	FmgrInfo   *cmpfunc = &typentry->cmp_proc_finfo;
	Datum	c;

	c = FunctionCall2Coll(cmpfunc, typentry->typcollation,
						  da, db);
	return DatumGetInt32(c);
}

#ifdef DEBUG_BRIN_STATS
/*
 * maxval_start
 *		Determine first index so that (maxvalue >= value).
 *
 * The array of ranges is expected to be sorted by maxvalue, so this is the first
 * range that can possibly intersect with range having "value" as minval.
 */
static int
maxval_start(BrinRange **ranges, int nranges, Datum value, TypeCacheEntry *typcache)
{
	int		start = 0,
			end = (nranges - 1);

	// everything matches
	if (range_values_cmp(&value, &ranges[start]->max_value, typcache) <= 0)
		return 0;

	// no matches
	if (range_values_cmp(&value, &ranges[end]->max_value, typcache) > 0)
		return nranges;

	while ((end - start) > 0)
	{
		int	 midpoint;
		int	 r;

		midpoint = start + (end - start) / 2;

		r = range_values_cmp(&value, &ranges[midpoint]->max_value, typcache);

		if (r <= 0)
			end = midpoint;
		else
			start = (midpoint + 1);
	}

	Assert(ranges[start]->max_value >= value);
	Assert(ranges[start-1]->max_value < value);

	return start;
}
#endif

/*
 * minval_end
 *		Determine first index so that (minval > value).
 *
 * The array of ranges is expected to be sorted by minvalue, so this is the first
 * range that can't possibly intersect with a range having "value" as maxval.
 */
static int
minval_end(BrinRange **ranges, int nranges, Datum value, TypeCacheEntry *typcache)
{
	int		start = 0,
			end = (nranges - 1);

	// everything matches
	if (range_values_cmp(&value, &ranges[end]->min_value, typcache) >= 0)
		return nranges;

	// no matches
	if (range_values_cmp(&value, &ranges[start]->min_value, typcache) < 0)
		return 0;

	while ((end - start) > 0)
	{
		int midpoint;
		int r;

		midpoint = start + (end - start) / 2;

		r = range_values_cmp(&value, &ranges[midpoint]->min_value, typcache);

		if (r >= 0)
			start = midpoint + 1;
		else
			end = midpoint;
	}

	Assert(range_values_cmp(&ranges[start]->min_value, &value, typcache) > 0);
	Assert(range_values_cmp(&ranges[start-1]->min_value, &value, typcache) <= 0);

	return start;
}


/*
 * lower_bound
 *		Determine first index so that (values[index] >= value).
 *
 * The array of values is sorted, and this returns the first value that
 * exceeds (or is equal) to the minvalue.
 */
static int
lower_bound(Datum *values, int nvalues, Datum minvalue, TypeCacheEntry *typcache)
{
	int		start = 0,
			end = (nvalues - 1);

	/* all values exceed minvalue - return the first element */
	if (range_values_cmp(&minvalue, &values[start], typcache) <= 0)
		return 0;

	/* nothing matches - return the element after the last one */
	if (range_values_cmp(&minvalue, &values[end], typcache) > 0)
		return nvalues;

	/*
	 * Now we know the lower boundary is somewhere in the array (and we know
	 * it's not the first element, because that's covered by the first check
	 * above). So do a binary search.
	 */
	while ((end - start) > 0)
	{
		int	midpoint;
		int	r;

		midpoint = start + (end - start) / 2;

		r = range_values_cmp(&minvalue, &values[midpoint], typcache);

		if (r <= 0)	/* minvalue >= midpoint */
			end = midpoint;
		else		/* midpoint < minvalue */
			start = (midpoint + 1);
	}

	Assert(range_values_cmp(&minvalue, &values[start], typcache) <= 0);
	Assert(range_values_cmp(&minvalue, &values[start-1], typcache) > 0);

	return start;
}

/*
 * upper_bound
 *		Determine last index so that (values[index] <= maxvalue).
 *
 * The array of values is sorted, and this returns the last value that
 * does not exceed (or is equal) to the maxvalue.
 */
static int
upper_bound(Datum *values, int nvalues, Datum maxvalue, TypeCacheEntry *typcache)
{
	int		start = 0,
			end = (nvalues - 1);

	/* everything matches, return the last element */
	if (range_values_cmp(&values[end], &maxvalue, typcache) <= 0)
		return (nvalues - 1);

	/* nothing matches, return the element before the first one */
	if (range_values_cmp(&values[start], &maxvalue, typcache) > 0)
		return -1;

	/*
	 * Now we know the lower boundary is somewhere in the array (and we know
	 * it's not the last element, because that's covered by the first check
	 * above). So do a binary search.
	 */
	while ((end - start) > 0)
	{
		int midpoint;
		int r;

		midpoint = start + (end - start) / 2;

		/* Ensure we always move (it might be equal to start due to rounding). */
		midpoint = Max(start+1, midpoint);

		r = range_values_cmp(&values[midpoint], &maxvalue, typcache);

		if (r <= 0)			/* value <= maxvalue */
			start = midpoint;
		else				/* value > maxvalue */
			end = midpoint - 1;
	}

	Assert(range_values_cmp(&values[start], &maxvalue, typcache) <= 0);
	Assert(range_values_cmp(&values[start+1], &maxvalue, typcache) > 0);

	return start;
}

/*
 * brin_minmax_count_overlaps
 *		Calculate number of overlaps.
 *
 * This uses the minranges to quickly eliminate ranges that can't possibly
 * intersect. We simply walk minranges until minval > current maxval, and
 * we're done.
 *
 * Unlike brin_minmax_count_overlaps2, this does not have issues with wide
 * ranges, so this is what we should use.
 */
static void
brin_minmax_count_overlaps(BrinRange **minranges, int nranges,
						   TypeCacheEntry *typcache, BrinMinmaxStats *stats)
{
	int64	noverlaps;

#ifdef DEBUG_BRIN_STATS
	TimestampTz		start_ts;

	if (debug_brin_stats)
		start_ts = GetCurrentTimestamp();
#endif

	noverlaps = 0;
	for (int i = 0; i < nranges; i++)
	{
		Datum	maxval = minranges[i]->max_value;

		/*
		 * Determine index of the first range with (minval > current maxval)
		 * by binary search. We know all other ranges can't overlap the
		 * current one. We simply subtract indexes to count ranges.
		 */
		int		idx = minval_end(minranges, nranges, maxval, typcache);

		/* -1 because we don't count the range as intersecting with itself */
		noverlaps += (idx - i - 1);
	}

	/*
	 * We only count 1/2 the ranges (minval > current minval), so the total
	 * number of overlaps is twice what we counted.
	 */
	noverlaps *= 2;

#ifdef DEBUG_BRIN_STATS
	if (debug_brin_stats)
	{
		elog(WARNING, "----- brin_minmax_count_overlaps -----");
		elog(WARNING, "noverlaps = %ld", noverlaps);
		elog(WARNING, "duration = %ld", TimestampDifferenceMilliseconds(start_ts,
										GetCurrentTimestamp()));
	}
#endif

	stats->avg_overlaps = (double) noverlaps / nranges;
}

/*
 * brin_minmax_match_tuples_to_ranges
 *		Match tuples to ranges, count average number of ranges per tuple.
 *
 * Alternative to brin_minmax_match_tuples_to_ranges2, leveraging ordering
 * of values, not ranges.
 *
 * XXX This seems like the optimal way to do this.
 */
static void
brin_minmax_match_tuples_to_ranges(BrinRanges *ranges,
								   int numrows, HeapTuple *rows,
								   int nvalues, Datum *values,
								   TypeCacheEntry *typcache,
								   BrinMinmaxStats *stats)
{
	int64	nmatches = 0;
	int64	nmatches_unique = 0;
	int64	nvalues_unique = 0;

	int64  *unique = (int64 *) palloc0(sizeof(int64) * nvalues);

#ifdef DEBUG_BRIN_STATS
	TimestampTz		start_ts;

	if (debug_brin_stats)
		start_ts = GetCurrentTimestamp();
#endif

	/*
	 * Build running count of unique values. We know there are unique[i]
	 * unique values in values array up to index "i".
	 */
	unique[0] = 1;
	for (int i = 1; i < nvalues; i++)
	{
		if (range_values_cmp(&values[i-1], &values[i], typcache) == 0)
			unique[i] = unique[i-1];
		else
			unique[i] = unique[i-1] + 1;
	}

	nvalues_unique = unique[nvalues-1];

	/*
	 * Walk the ranges, for each range determine the first/last mapping
	 * value. Use the "unique" array to count the unique values.
	 */
	for (int i = 0; i < ranges->nranges; i++)
	{
		int		start,
				end,
				nvalues_match,
				nunique_match;

		CHECK_FOR_INTERRUPTS();

		start = lower_bound(values, nvalues, ranges->ranges[i].min_value, typcache);
		end = upper_bound(values, nvalues, ranges->ranges[i].max_value, typcache);

		/* if nothing matches (e.g. end=0), skip this range */
		if (end <= start)
			continue;

		nvalues_match = (end - start + 1);
		nunique_match = (unique[end] - unique[start] + 1);

		Assert((nvalues_match >= 1) && (nvalues_match <= nvalues));
		Assert((nunique_match >= 1) && (nunique_match <= unique[nvalues-1]));

		nmatches += nvalues_match;
		nmatches_unique += nunique_match;
	}

	Assert(nmatches >= 0);
	Assert(nmatches_unique >= 0);

#ifdef DEBUG_BRIN_STATS
	if (debug_brin_stats)
	{
		elog(WARNING, "----- brin_minmax_match_tuples_to_ranges -----");
		elog(WARNING, "nmatches = %ld %f", nmatches, (double) nmatches / numrows);
		elog(WARNING, "nmatches unique = %ld %ld %f", nmatches_unique, nvalues_unique,
			(double) nmatches_unique / nvalues_unique);
		elog(WARNING, "duration = %ld", TimestampDifferenceMilliseconds(start_ts,
									GetCurrentTimestamp()));
	}
#endif

	stats->avg_matches = (double) nmatches / numrows;
	stats->avg_matches_unique = (double) nmatches_unique / nvalues_unique;
}

#ifdef DEBUG_BRIN_STATS
/*
 * Simple histogram, with bins tracking value and two overlap counts.
 *
 * XXX Maybe we should have two separate histograms, one for all counts and
 * another one for "unique" values.
 *
 * XXX Serialize the histogram. There might be a data set where we have very
 * many distinct buckets (values having very different number of matching
 * ranges) - not sure if there's some sort of upper limit (but hard to say for
 * other opclasses, like bloom). And we don't want arbitrarily large histogram,
 * to keep the statistics fairly small, I guess. So we'd need to pick a subset,
 * merge buckets with "similar" counts, or approximate it somehow. For now we
 * don't serialize it, because we don't use the histogram.
 */
typedef struct histogram_bin_t
{
	int64	value;
	int64	count;
} histogram_bin_t;

typedef struct histogram_t
{
	int				nbins;
	int				nbins_max;
	histogram_bin_t	bins[FLEXIBLE_ARRAY_MEMBER];
} histogram_t;

#define HISTOGRAM_BINS_START 32

/* allocate histogram with default number of bins */
static histogram_t *
histogram_init(void)
{
	histogram_t *hist;

	hist = (histogram_t *) palloc0(offsetof(histogram_t, bins) +
								   sizeof(histogram_bin_t) * HISTOGRAM_BINS_START);
	hist->nbins_max = HISTOGRAM_BINS_START;

	return hist;
}

/*
 * histogram_add
 *			 Add a hit for a particular value to the histogram.
 *
 * XXX We don't sort the bins, so just do binary sort. For large number of values
 * this might be an issue, for small number of values a linear search is fine.
 */
static histogram_t *
histogram_add(histogram_t *hist, int value)
{
	bool	found = false;
	histogram_bin_t *bin;

	for (int i = 0; i < hist->nbins; i++)
	{
		if (hist->bins[i].value == value)
		{
			bin = &hist->bins[i];
			found = true;
		}
	}

	if (!found)
	{
		if (hist->nbins == hist->nbins_max)
		{
			int		nbins = (2 * hist->nbins_max);

			hist = repalloc(hist, offsetof(histogram_t, bins) +
							sizeof(histogram_bin_t) * nbins);
			hist->nbins_max = nbins;
		}

		Assert(hist->nbins < hist->nbins_max);

		bin = &hist->bins[hist->nbins++];
		bin->value = value;
		bin->count = 0;
	}

	bin->count += 1;

	Assert(bin->value == value);
	Assert(bin->count >= 0);

	return hist;
}

/* used to sort histogram bins by value */
static int
histogram_bin_cmp(const void *a, const void *b)
{
	histogram_bin_t *ba = (histogram_bin_t *) a;
	histogram_bin_t *bb = (histogram_bin_t *) b;

	if (ba->value < bb->value)
		return -1;

	if (bb->value < ba->value)
		return 1;

	return 0;
}

static void
histogram_print(histogram_t *hist)
{
	return;

	elog(WARNING, "----- histogram -----");
	for (int i = 0; i < hist->nbins; i++)
	{
		elog(WARNING, "bin %d value %ld count %ld",
				i, hist->bins[i].value, hist->bins[i].count);
	}
}

/*
 * brin_minmax_match_tuples_to_ranges2
 *		Match tuples to ranges, count average number of ranges per tuple.
 *
 * Match sample tuples to the ranges, so that we can count how many ranges
 * a value matches on average. This might seem redundant to the number of
 * overlaps, because the value is ~avg_overlaps/2.
 *
 * Imagine ranges arranged in "shifted" uniformly by 1/overlaps, e.g. with 3
 * overlaps [0,100], [33,133], [66, 166] and so on. A random value will hit
 * only half of there ranges, thus 1/2. This can be extended to randomly
 * overlapping ranges.
 *
 * However, we may not be able to count overlaps for some opclasses (e.g. for
 * bloom ranges), in which case we have at least this.
 *
 * This simply walks the values, and determines matching ranges by looking
 * for lower/upper bound in ranges ordered by minval/maxval.
 *
 * XXX The other question is what to do about duplicate values. If we have a
 * very frequent value in the sample, it's likely in many places/ranges. Which
 * will skew the average, because it'll be added repeatedly. So we also count
 * avg_ranges for unique values.
 *
 * XXX The relationship that (average_matches ~ average_overlaps/2) only
 * works for minmax opclass, and can't be extended to minmax-multi. The
 * overlaps can only consider the two extreme values (essentially treating
 * the summary as a single minmax range), because that's what brinsort
 * needs. But the minmax-multi range may have "gaps" (kinda the whole point
 * of these opclasses), which affects matching tuples to ranges.
 *
 * XXX This also builds histograms of the number of matches, both for the
 * raw and unique values. At the moment we don't do anything with the
 * results, though (except for printing those).
 */
static void
brin_minmax_match_tuples_to_ranges2(BrinRanges *ranges,
									BrinRange **minranges, BrinRange **maxranges,
									int numrows, HeapTuple *rows,
									int nvalues, Datum *values,
									TypeCacheEntry *typcache,
									BrinMinmaxStats *stats)
{
	int64	nmatches = 0;
	int64	nmatches_unique = 0;
	int64	nmatches_value = 0;
	int64	nvalues_unique = 0;

	histogram_t	   *hist = histogram_init();
	histogram_t	   *hist_unique = histogram_init();
	TimestampTz		start_ts = GetCurrentTimestamp();

	for (int i = 0; i < nvalues; i++)
	{
		int		start;
		int		end;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Same value as preceding, so just use the preceding count.
		 * We don't increment the unique counters, because this is
		 * a duplicate.
		 */
		if ((i > 0) && (range_values_cmp(&values[i-1], &values[i], typcache) == 0))
		{
			nmatches += nmatches_value;
			hist = histogram_add(hist, nmatches_value);
			continue;
		}

		nmatches_value = 0;

		start = maxval_start(maxranges, ranges->nranges, values[i], typcache);
		end = minval_end(minranges, ranges->nranges, values[i], typcache);

		for (int j = start; j < ranges->nranges; j++)
		{
			if (maxranges[j]->min_index >= end)
				continue;

			if (maxranges[j]->min_index_lowest >= end)
				break;

			nmatches_value++;
		}

		hist = histogram_add(hist, nmatches_value);
		hist_unique = histogram_add(hist_unique, nmatches_value);

		nmatches += nmatches_value;
		nmatches_unique += nmatches_value;
		nvalues_unique++;
	}

	if (debug_brin_stats)
	{
		elog(WARNING, "----- brin_minmax_match_tuples_to_ranges2 -----");
		elog(WARNING, "nmatches = %ld %f", nmatches, (double) nmatches / numrows);
		elog(WARNING, "nmatches unique = %ld %ld %f",
			 nmatches_unique, nvalues_unique, (double) nmatches_unique / nvalues_unique);
		elog(WARNING, "duration = %ld", TimestampDifferenceMilliseconds(start_ts,
										GetCurrentTimestamp()));
	}

	if (stats->avg_matches != (double) nmatches / numrows)
		elog(ERROR, "brin_minmax_match_tuples_to_ranges2: avg_matches mismatch %f != %f",
			 stats->avg_matches, (double) nmatches / numrows);

	if (stats->avg_matches_unique != (double) nmatches_unique / nvalues_unique)
		elog(ERROR, "brin_minmax_match_tuples_to_ranges2: avg_matches_unique mismatch %f != %f",
			 stats->avg_matches_unique, (double) nmatches_unique / nvalues_unique);

	pg_qsort(hist->bins, hist->nbins, sizeof(histogram_bin_t), histogram_bin_cmp);
	pg_qsort(hist_unique->bins, hist_unique->nbins, sizeof(histogram_bin_t), histogram_bin_cmp);

	histogram_print(hist);
	histogram_print(hist_unique);

	pfree(hist);
	pfree(hist_unique);
}

/*
 * brin_minmax_match_tuples_to_ranges_bruteforce
 *		Match tuples to ranges, count average number of ranges per tuple.
 *
 * Bruteforce approach, used mostly for cross-checking.
 */
static void
brin_minmax_match_tuples_to_ranges_bruteforce(BrinRanges *ranges,
											  int numrows, HeapTuple *rows,
											  int nvalues, Datum *values,
											  TypeCacheEntry *typcache,
											  BrinMinmaxStats *stats)
{
	int64	nmatches = 0;
	int64	nmatches_unique = 0;
	int64	nvalues_unique = 0;

	TimestampTz		start_ts = GetCurrentTimestamp();

	for (int i = 0; i < nvalues; i++)
	{
		bool	is_unique;
		int64	nmatches_value = 0;

		CHECK_FOR_INTERRUPTS();

		/* is this a new value? */
		is_unique = ((i == 0) || (range_values_cmp(&values[i-1], &values[i], typcache) != 0));

		/* count unique values */
		nvalues_unique += (is_unique) ? 1 : 0;

		for (int j = 0; j < ranges->nranges; j++)
		{
			if (range_values_cmp(&values[i], &ranges->ranges[j].min_value, typcache) < 0)
				continue;

			if (range_values_cmp(&values[i], &ranges->ranges[j].max_value, typcache) > 0)
				continue;

			nmatches_value++;
		}

		nmatches += nmatches_value;
		nmatches_unique += (is_unique) ? nmatches_value : 0;
	}

	if (debug_brin_stats)
	{
		elog(WARNING, "----- brin_minmax_match_tuples_to_ranges_bruteforce -----");
		elog(WARNING, "nmatches = %ld %f", nmatches, (double) nmatches / numrows);
		elog(WARNING, "nmatches unique = %ld %ld %f", nmatches_unique, nvalues_unique,
			 (double) nmatches_unique / nvalues_unique);
		elog(WARNING, "duration = %ld", TimestampDifferenceMilliseconds(start_ts,
										GetCurrentTimestamp()));
	}

	if (stats->avg_matches != (double) nmatches / numrows)
		elog(ERROR, "brin_minmax_match_tuples_to_ranges_bruteforce: avg_matches mismatch %f != %f",
			 stats->avg_matches, (double) nmatches / numrows);

	if (stats->avg_matches_unique != (double) nmatches_unique / nvalues_unique)
		elog(ERROR, "brin_minmax_match_tuples_to_ranges_bruteforce: avg_matches_unique mismatch %f != %f",
			 stats->avg_matches_unique, (double) nmatches_unique / nvalues_unique);
}
#endif

/*
 * brin_minmax_value_stats
 *		Calculate statistics about minval/maxval values.
 *
 * We calculate the number of distinct values, and also correlation with respect
 * to blkno_start. We don't calculate the regular correlation coefficient, because
 * our goal is to estimate how sequential the accesses are. The regular correlation
 * would produce 0 for cyclical data sets like mod(i,1000000), but it may be quite
 * sequantial access. Maybe it should be called differently, not correlation?
 *
 * XXX Maybe this should calculate minval vs. maxval correlation too?
 *
 * XXX I don't know how important the sequentiality is - BRIN generally uses 1MB
 * page ranges, which is pretty sequential and the one random seek in between is
 * likely going to be negligible. Maybe for small page ranges it'll matter, though.
 */
static void
brin_minmax_value_stats(BrinRange **minranges, BrinRange **maxranges,
						int nranges, TypeCacheEntry *typcache,
						BrinMinmaxStats *stats)
{
	/* */
	int64	minval_ndist = 1,
			maxval_ndist = 1,
			minval_corr = 0,
			maxval_corr = 0;

#ifdef DEBUG_BRIN_STATS
	TimestampTz		start_ts;

	if (debug_brin_stats)
		start_ts = GetCurrentTimestamp();
#endif

	for (int i = 1; i < nranges; i++)
	{
		if (range_values_cmp(&minranges[i-1]->min_value, &minranges[i]->min_value, typcache) != 0)
			minval_ndist++;

		if (range_values_cmp(&maxranges[i-1]->max_value, &maxranges[i]->max_value, typcache) != 0)
			maxval_ndist++;

		/* is it immediately sequential? */
		if (minranges[i-1]->blkno_end + 1 == minranges[i]->blkno_start)
			minval_corr++;

		/* is it immediately sequential? */
		if (maxranges[i-1]->blkno_end + 1 == maxranges[i]->blkno_start)
			maxval_corr++;
	}

	stats->minval_ndistinct = minval_ndist;
	stats->maxval_ndistinct = maxval_ndist;

	stats->minval_correlation = (double) minval_corr / nranges;
	stats->maxval_correlation = (double) maxval_corr / nranges;

#ifdef DEBUG_BRIN_STATS
	if (debug_brin_stats)
	{
		elog(WARNING, "----- brin_minmax_value_stats -----");
		elog(WARNING, "minval ndistinct " INT64_FORMAT " correlation %f",
			 stats->minval_ndistinct, stats->minval_correlation);
		elog(WARNING, "maxval ndistinct " INT64_FORMAT " correlation %f",
			 stats->maxval_ndistinct, stats->maxval_correlation);
		elog(WARNING, "duration = %ld", TimestampDifferenceMilliseconds(start_ts,
										GetCurrentTimestamp()));
	}
#endif
}

/*
 * brin_minmax_increment_stats
 *		Calculate the increment size for minval/maxval steps.
 *
 * Calculates the minval/maxval increment size, i.e. number of rows that need
 * to be added to the sort. This serves as an input to calculation of a good
 * watermark step.
 */
static void
brin_minmax_increment_stats(BrinRange **minranges, BrinRange **maxranges,
							int nranges, Datum *values, int nvalues,
							TypeCacheEntry *typcache, BrinMinmaxStats *stats)
{
	/* */
	int64	minval_ndist = 1,
			maxval_ndist = 1;

	double	sum_minval = 0,
			sum_maxval = 0,
			max_minval = 0,
			max_maxval = 0;

#ifdef DEBUG_BRIN_STATS
	TimestampTz		start_ts;

	if (debug_brin_stats)
		start_ts = GetCurrentTimestamp();
#endif

	for (int i = 1; i < nranges; i++)
	{
		if (range_values_cmp(&minranges[i-1]->min_value, &minranges[i]->min_value, typcache) != 0)
		{
			double	p;
			int		start = upper_bound(values, nvalues, minranges[i-1]->min_value, typcache);
			int		end = upper_bound(values, nvalues, minranges[i]->min_value, typcache);

			/*
			 * Maybe there are no matching rows, but we still need to count
			 * this as distinct minval (even though the sample increase is 0).
			 */
			minval_ndist++;

			Assert(end >= start);

			/* no sample rows match this, so skip */
			if (end == start)
				continue;

			p = (double) (end - start) / nvalues;

			max_minval = Max(max_minval, p);
			sum_minval += p;
		}

		if (range_values_cmp(&maxranges[i-1]->max_value, &maxranges[i]->max_value, typcache) != 0)
		{
			double	p;
			int		start = upper_bound(values, nvalues, maxranges[i-1]->max_value, typcache);
			int		end = upper_bound(values, nvalues, maxranges[i]->max_value, typcache);

			/*
			 * Maybe there are no matching rows, but we still need to count
			 * this as distinct maxval (even though the sample increase is 0).
			 */
			maxval_ndist++;

			Assert(end >= start);

			/* no sample rows match this, so skip */
			if (end == start)
				continue;

			p = (double) (end - start) / nvalues;

			max_maxval = Max(max_maxval, p);
			sum_maxval += p;
		}
	}

#ifdef DEBUG_BRIN_STATS
	if (debug_brin_stats)
	{
		elog(WARNING, "----- brin_minmax_increment_stats -----");
		elog(WARNING, "minval ndistinct %ld sum %f max %f avg %f",
			 minval_ndist, sum_minval, max_minval, sum_minval / minval_ndist);
		elog(WARNING, "maxval ndistinct %ld sum %f max %f avg %f",
			 maxval_ndist, sum_maxval, max_maxval, sum_maxval / maxval_ndist);
		elog(WARNING, "duration = %ld", TimestampDifferenceMilliseconds(start_ts,
										GetCurrentTimestamp()));
	}
#endif

	stats->minval_increment_avg = (sum_minval / minval_ndist);
	stats->minval_increment_max = max_minval;

	stats->maxval_increment_avg = (sum_maxval / maxval_ndist);
	stats->maxval_increment_max = max_maxval;
}

#ifdef DEBUG_BRIN_STATS
/*
 * brin_minmax_count_overlaps2
 *		Calculate number of overlaps.
 *
 * This uses the minranges/maxranges to quickly eliminate ranges that can't
 * possibly intersect.
 *
 * XXX Seems rather complicated and works poorly for wide ranges (with outlier
 * values), brin_minmax_count_overlaps is likely better.
 */
static void
brin_minmax_count_overlaps2(BrinRanges *ranges,
						   BrinRange **minranges, BrinRange **maxranges,
						   TypeCacheEntry *typcache, BrinMinmaxStats *stats)
{
	int64			noverlaps;
	TimestampTz		start_ts = GetCurrentTimestamp();

	/*
	 * Walk the ranges ordered by max_values, see how many ranges overlap.
	 *
	 * Once we get to a state where (min_value > current.max_value) for
	 * all future ranges, we know none of them can overlap and we can
	 * terminate. This is what min_index_lowest is for.
	 *
	 * XXX If there are very wide ranges (with outlier min/max values),
	 * the min_index_lowest is going to be pretty useless, because the
	 * range will be sorted at the very end by max_value, but will have
	 * very low min_index, so this won't work.
	 *
	 * XXX We could collect a more elaborate stuff, like for example a
	 * histogram of number of overlaps, or maximum number of overlaps.
	 * So we'd have average, but then also an info if there are some
	 * ranges with very many overlaps.
	 */
	noverlaps = 0;
	for (int i = 0; i < ranges->nranges; i++)
	{
		int			idx = (i + 1);
		BrinRange *ra = maxranges[i];
		uint64		min_index = ra->min_index;

		CHECK_FOR_INTERRUPTS();

#ifdef NOT_USED
		/*
		 * XXX Not needed, we can just count "future" ranges and then
		 * we just multiply by 2.
		 */

		/*
		 * What's the first range that might overlap with this one?
		 * needs to have maxval > current.minval.
		 */
		while (idx > 0)
		{
			BrinRange *rb = maxranges[idx - 1];

			/* the range is before the current one, so can't intersect */
			if (range_values_cmp(&rb->max_value, &ra->min_value, typcache) < 0)
				break;

			idx--;
		}
#endif

		/*
		 * Find the first min_index that is higher than the max_value,
		 * so that we can compare that instead of the values in the
		 * next loop. There should be fewer value comparisons than in
		 * the next loop, so we'll save on function calls.
		 */
		while (min_index < ranges->nranges)
		{
			if (range_values_cmp(&minranges[min_index]->min_value,
								 &ra->max_value, typcache) > 0)
				break;

			min_index++;
		}

		/*
		 * Walk the following ranges (ordered by max_value), and check
		 * if it overlaps. If it matches, we look at the next one. If
		 * not, we check if there can be more ranges.
		 */
		for (int j = idx; j < ranges->nranges; j++)
		{
			BrinRange *rb = maxranges[j];

			/* the range overlaps - just continue with the next one */
			// if (range_values_cmp(&rb->min_value, &ra->max_value, typcache) <= 0)
			if (rb->min_index < min_index)
			{
				noverlaps++;
				continue;
			}

			/*
			 * Are there any future ranges that might overlap? We can
			 * check the min_index_lowest to decide quickly.
			 */
			 if (rb->min_index_lowest >= min_index)
					break;
		}
	}

	/*
	 * We only count intersect for "following" ranges when ordered by maxval,
	 * so we only see 1/2 the overlaps. So double the result.
	 */
	noverlaps *= 2;

	if (debug_brin_stats)
	{
		elog(WARNING, "----- brin_minmax_count_overlaps2 -----");
		elog(WARNING, "noverlaps = %ld", noverlaps);
		elog(WARNING, "duration = %ld", TimestampDifferenceMilliseconds(start_ts,
										GetCurrentTimestamp()));
	}

	if (stats->avg_overlaps != (double) noverlaps / ranges->nranges)
		elog(ERROR, "brin_minmax_count_overlaps2: mismatch %f != %f",
			 stats->avg_overlaps, (double) noverlaps / ranges->nranges);
}

/*
 * brin_minmax_count_overlaps_bruteforce
 *		Calculate number of overlaps by brute force.
 *
 * Actually compares every range to every other range. Quite expensive, used
 * primarily to cross-check the other algorithms.
 */
static void
brin_minmax_count_overlaps_bruteforce(BrinRanges *ranges,
									  TypeCacheEntry *typcache,
									  BrinMinmaxStats *stats)
{
	int64			noverlaps;
	TimestampTz		start_ts = GetCurrentTimestamp();

	/*
	 * Brute force calculation of overlapping ranges, comparing each
	 * range to every other range - bound to be pretty expensive, as
	 * it's pretty much O(N^2). Kept mostly for easy cross-check with
	 * the preceding "optimized" code.
	 */
	noverlaps = 0;
	for (int i = 0; i < ranges->nranges; i++)
	{
		BrinRange *ra = &ranges->ranges[i];

		for (int j = 0; j < ranges->nranges; j++)
		{
			BrinRange *rb = &ranges->ranges[j];

			CHECK_FOR_INTERRUPTS();

			if (i == j)
				continue;

			if (range_values_cmp(&ra->max_value, &rb->min_value, typcache) < 0)
				continue;

			if (range_values_cmp(&rb->max_value, &ra->min_value, typcache) < 0)
				continue;

#if 0
			elog(DEBUG1, "[%ld,%ld] overlaps [%ld,%ld]",
				 ra->min_value, ra->max_value,
				 rb->min_value, rb->max_value);
#endif

			noverlaps++;
		}
	}

	if (debug_brin_stats)
	{
		elog(WARNING, "----- brin_minmax_count_overlaps_bruteforce -----");
		elog(WARNING, "noverlaps = %ld", noverlaps);
		elog(WARNING, "duration = %ld", TimestampDifferenceMilliseconds(start_ts,
										GetCurrentTimestamp()));
	}

	if (stats->avg_overlaps != (double) noverlaps / ranges->nranges)
		elog(ERROR, "brin_minmax_count_overlaps2: mismatch %f != %f",
			 stats->avg_overlaps, (double) noverlaps / ranges->nranges);
}
#endif

/*
 * brin_minmax_stats
 *		Calculate custom statistics for a BRIN minmax index.
 *
 * At the moment this calculates:
 *
 *  - number of summarized/not-summarized and all/has nulls ranges
 *  - average number of overlaps for a range
 *  - average number of rows matching a range
 *  - number of distinct minval/maxval values
 *
 * There are multiple ways to calculate some of the metrics, so to allow
 * cross-checking during development it's possible to run and compare all.
 * To do that, define STATS_CROSS_CHECK. There's also STATS_DEBUG define
 * that simply prints the calculated results.
 *
 * XXX This could also calculate correlation of the range minval, so that
 * we can estimate how much random I/O will happen during the BrinSort.
 * And perhaps we should also sort the ranges by (minval,block_start) to
 * make this as sequential as possible?
 *
 * XXX Another interesting statistics might be the number of ranges with
 * the same minval (or number of distinct minval values), because that's
 * essentially what we need to estimate how many ranges will be read in
 * one brinsort step. In fact, knowing the number of distinct minval
 * values tells us the number of BrinSort loops.
 *
 * XXX We might also calculate a histogram of minval/maxval values.
 *
 * XXX I wonder if we could track for each range track probabilities:
 *
 * - P1 = P(v <= minval)
 * - P2 = P(x <= Max(maxval)) for Max(maxval) over preceding ranges
 *
 * That would allow us to estimate how many ranges we'll have to read to produce
 * a particular number of rows, because we need the first probability to exceed
 * the requested number of rows (fraction of the table):
 *
 *     (limit rows / reltuples) <= P(v <= minval)
 *
 * and then the second probability would say how many rows we'll process (either
 * sort or spill). And inversely for the DESC ordering.
 *
 * The difference between P1 for two ranges is how much we'd have to sort
 * if we moved the watermark between the ranges (first minval to second one).
 * The (P2 - P1) for the new watermark range measures the number of rows in
 * the tuplestore. We'll need to aggregate this, though, we can't keep the
 * whole data - probably average/median/max for the differences would be nice.
 * Might be tricky for different watermark step values, though.
 *
 * This would also allow estimating how many rows will spill from each range,
 * because we have an estimate how many rows match a range on average, and
 * we can compare it to the difference between P1.
 *
 * One issue is we don't have actual tuples from the ranges, so we can't
 * measure exactly how many rows would we add. But we can match the sample
 * and at least estimate the the probability difference.
 *
 * Actually - we do know the tuples *are* in those ranges, because if we
 * assume the tuple is in some other range, that range would have to have
 * a minimal/maximal value so that the value is consistent. Which means
 * the range has to be between those ranges. Of course, this only estimates
 * the rows we'd going to add to the tuplesort - there might be more rows
 * we read and spill to tuplestore, but that's something we can estimate
 * using average tuples per range.
 */
Datum
brin_minmax_stats(PG_FUNCTION_ARGS)
{
	Relation		heapRel = (Relation) PG_GETARG_POINTER(0);
	Relation		indexRel = (Relation) PG_GETARG_POINTER(1);
	AttrNumber		attnum = PG_GETARG_INT16(2);	/* index attnum */
	AttrNumber		heap_attnum = PG_GETARG_INT16(3);
	Expr		   *expr = (Expr *) PG_GETARG_POINTER(4);
	HeapTuple	   *rows = (HeapTuple *) PG_GETARG_POINTER(5);
	int				numrows = PG_GETARG_INT32(6);

	BrinOpaque *opaque;
	BlockNumber nblocks;
	BlockNumber	nranges;
	BlockNumber	heapBlk;
	BrinMemTuple *dtup;
	BrinTuple  *btup = NULL;
	Size		btupsz = 0;
	Buffer		buf = InvalidBuffer;
	BrinRanges  *ranges;
	BlockNumber	pagesPerRange;
	BrinDesc	   *bdesc;
	BrinMinmaxStats *stats;
	Form_pg_attribute attr;

	Oid				typoid;
	TypeCacheEntry *typcache;
	BrinRange	  **minranges,
				  **maxranges;
	int64			prev_min_index;

	/* expression stats */
	EState	   *estate;
	ExprContext *econtext;
	ExprState  *exprstate;
	TupleTableSlot *slot;

	/* attnum or expression has to be supplied */
	Assert(AttributeNumberIsValid(heap_attnum) || (expr != NULL));

	/* but not both of them at the same time */
	Assert(!(AttributeNumberIsValid(heap_attnum) && (expr != NULL)));

	/*
	 * Mostly what brinbeginscan does to initialize BrinOpaque, except that
	 * we use active snapshot instead of the scan snapshot.
	 */
	opaque = palloc_object(BrinOpaque);
	opaque->bo_rmAccess = brinRevmapInitialize(indexRel,
											   &opaque->bo_pagesPerRange,
											   GetActiveSnapshot());
	opaque->bo_bdesc = brin_build_desc(indexRel);

	bdesc = opaque->bo_bdesc;
	pagesPerRange = opaque->bo_pagesPerRange;

	/* make sure the provided attnum is valid */
	Assert((attnum > 0) && (attnum <= bdesc->bd_tupdesc->natts));

	/* attribute information */
	attr = TupleDescAttr(bdesc->bd_tupdesc, attnum - 1);

	/*
	 * We need to know the size of the table so that we know how long to iterate
	 * on the revmap (and to pre-allocate the arrays).
	 */
	nblocks = RelationGetNumberOfBlocks(heapRel);

	/*
	 * How many ranges can there be? We simply look at the number of pages,
	 * divide it by the pages_per_range.
	 *
	 * XXX We need to be careful not to overflow nranges, so we just divide
	 * and then maybe add 1 for partial ranges.
	 */
	nranges = (nblocks / pagesPerRange);
	if (nblocks % pagesPerRange != 0)
		nranges += 1;

	/* allocate for space, and also for the alternative ordering */
	ranges = palloc0(offsetof(BrinRanges, ranges) + nranges * sizeof(BrinRange));
	ranges->nranges = 0;

	/* allocate an initial in-memory tuple, out of the per-range memcxt */
	dtup = brin_new_memtuple(bdesc);

	/* result stats */
	stats = palloc0(sizeof(BrinMinmaxStats));
	SET_VARSIZE(stats, sizeof(BrinMinmaxStats));

	/*
	 * Now scan the revmap.  We start by querying for heap page 0,
	 * incrementing by the number of pages per range; this gives us a full
	 * view of the table.
	 *
	 * XXX We count the ranges, and count the special types (not summarized,
	 * all-null and has-null). The regular ranges are accumulated into an
	 * array, so that we can calculate additional statistics (overlaps, hits
	 * for sample tuples, etc).
	 *
	 * XXX This needs rethinking to make it work with large indexes with more
	 * ranges than we can fit into memory (work_mem/maintenance_work_mem).
	 */
	for (heapBlk = 0; heapBlk < nblocks; heapBlk += pagesPerRange)
	{
		bool		gottuple = false;
		BrinTuple  *tup;
		OffsetNumber off;
		Size		size;

		stats->n_ranges++;

		CHECK_FOR_INTERRUPTS();

		tup = brinGetTupleForHeapBlock(opaque->bo_rmAccess, heapBlk, &buf,
									   &off, &size, BUFFER_LOCK_SHARE,
									   GetActiveSnapshot());
		if (tup)
		{
			gottuple = true;
			btup = brin_copy_tuple(tup, size, btup, &btupsz);
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		}

		/* Ranges with no indexed tuple are ignored for overlap analysis. */
		if (!gottuple)
		{
			continue;
		}
		else
		{
			dtup = brin_deform_tuple(bdesc, btup, dtup);
			if (dtup->bt_placeholder)
			{
				/* Placeholders can be ignored too, as if not summarized. */
				continue;
			}
			else
			{
				BrinValues *bval;

				bval = &dtup->bt_columns[attnum - 1];

				/* OK this range is summarized */
				stats->n_summarized++;

				if (bval->bv_allnulls)
					stats->n_all_nulls++;

				if (bval->bv_hasnulls)
					stats->n_has_nulls++;

				if (!bval->bv_allnulls)
				{
					BrinRange  *range;

					range = &ranges->ranges[ranges->nranges++];

					range->blkno_start = heapBlk;
					range->blkno_end = heapBlk + (pagesPerRange - 1);

					range->min_value = datumCopy(bval->bv_values[0],
												 attr->attbyval, attr->attlen);
					range->max_value = datumCopy(bval->bv_values[1],
												 attr->attbyval, attr->attlen);
				}
			}
		}
	}

	if (buf != InvalidBuffer)
		ReleaseBuffer(buf);

	/* if we have no regular ranges, we're done */
	if (ranges->nranges == 0)
		goto cleanup;

	/*
	 * Build auxiliary info to optimize the calculation.
	 *
	 * We have ranges in the blocknum order, but that is not very useful when
	 * calculating which ranges interstect - we could cross-check every range
	 * against every other range, but that's O(N^2) and thus may get extremely
	 * expensive pretty quick).
	 *
	 * To make that cheaper, we'll build two orderings, allowing us to quickly
	 * eliminate ranges that can't possibly overlap:
	 *
	 * - minranges = ranges ordered by min_value
	 * - maxranges = ranges ordered by max_value
	 *
	 * To count intersections, we'll then walk maxranges (i.e. ranges ordered
	 * by maxval), and for each following range we'll check if it overlaps.
	 * If yes, we'll proceed to the next one, until we find a range that does
	 * not overlap. But there might be a later page overlapping - but we can
	 * use a min_index_lowest tracking the minimum min_index for "future"
	 * ranges to quickly decide if there are such ranges. If there are none,
	 * we can terminate (and proceed to the next maxranges element), else we
	 * have to process additional ranges.
	 *
	 * Note: This only counts overlaps with ranges with max_value higher than
	 * the current one - we want to count all, but the overlaps with preceding
	 * ranges have already been counted when processing those preceding ranges.
	 * That is, we'll end up with counting each overlap just for one of those
	 * ranges, so we get only 1/2 the count.
	 *
	 * Note: We don't count the range as overlapping with itself. This needs
	 * to be considered later, when applying the statistics.
	 *
	 *
	 * XXX This will not work for very many ranges - we can have up to 2^32 of
	 * them, so allocating a ~32B struct for each would need a lot of memory.
	 * Not sure what to do about that, perhaps we could sample a couple ranges
	 * and do some calculations based on that? That is, we could process all
	 * ranges up to some number (say, statistics_target * 300, as for rows), and
	 * then sample ranges for larger tables. Then sort the sampled ranges, and
	 * walk through all ranges once, comparing them to the sample and counting
	 * overlaps (having them sorted should allow making this quite efficient,
	 * I think - following algorithm similar to the one implemented here).
	 */

	/* info about ordering for the data type */
	typoid = get_atttype(RelationGetRelid(indexRel), attnum);
	typcache = lookup_type_cache(typoid, TYPECACHE_CMP_PROC_FINFO);

	/* shouldn't happen, I think - we use this to build the index */
	Assert(OidIsValid(typcache->cmp_proc_finfo.fn_oid));

	minranges = (BrinRange **) palloc0(ranges->nranges * sizeof(BrinRanges *));
	maxranges = (BrinRange **) palloc0(ranges->nranges * sizeof(BrinRanges *));

	/*
	 * Build and sort the ranges min_value / max_value (just pointers
	 * to the main array). Then go and assign the min_index to each
	 * range, and finally walk the maxranges array backwards and track
	 * the min_index_lowest as minimum of "future" indexes.
	 */
	for (int i = 0; i < ranges->nranges; i++)
	{
		minranges[i] = &ranges->ranges[i];
		maxranges[i] = &ranges->ranges[i];
	}

	qsort_arg(minranges, ranges->nranges, sizeof(BrinRange *),
			  range_minval_cmp, typcache);

	qsort_arg(maxranges, ranges->nranges, sizeof(BrinRange *),
			  range_maxval_cmp, typcache);

	/*
	 * Update the min_index for each range. If the values are equal, be sure to
	 * pick the lowest index with that min_value.
	 */
	minranges[0]->min_index = 0;
	for (int i = 1; i < ranges->nranges; i++)
	{
		if (range_values_cmp(&minranges[i]->min_value, &minranges[i-1]->min_value, typcache) == 0)
			minranges[i]->min_index = minranges[i-1]->min_index;
		else
			minranges[i]->min_index = i;
	}

	/*
	 * Walk the maxranges backward and assign the min_index_lowest as
	 * a running minimum.
	 */
	prev_min_index = ranges->nranges;
	for (int i = (ranges->nranges - 1); i >= 0; i--)
	{
		maxranges[i]->min_index_lowest = Min(maxranges[i]->min_index,
											 prev_min_index);
		prev_min_index = maxranges[i]->min_index_lowest;
	}

	/* calculate average number of overlapping ranges for any range */
	brin_minmax_count_overlaps(minranges, ranges->nranges, typcache, stats);

#ifdef DEBUG_BRIN_STATS
	if (debug_brin_cross_check)
	{
		brin_minmax_count_overlaps2(ranges, minranges, maxranges, typcache, stats);
		brin_minmax_count_overlaps_bruteforce(ranges, typcache, stats);
	}
#endif

	/* calculate minval/maxval stats (distinct values and correlation) */
	brin_minmax_value_stats(minranges, maxranges,
							ranges->nranges, typcache, stats);

	/*
	 * If processing expression, prepare context to evaluate it.
	 *
	 * XXX cleanup / refactoring needed
	 */
	if (expr)
	{
		estate = CreateExecutorState();
		econtext = GetPerTupleExprContext(estate);

		/* Need a slot to hold the current heap tuple, too */
		slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRel),
										&TTSOpsHeapTuple);

		/* Arrange for econtext's scan tuple to be the tuple under test */
		econtext->ecxt_scantuple = slot;

		exprstate = ExecPrepareExpr(expr, estate);
	}

	/* match tuples to ranges */
	{
		int		nvalues = 0;
		Datum  *values = (Datum *) palloc0(numrows * sizeof(Datum));

		TupleDesc	tdesc = RelationGetDescr(heapRel);

		for (int i = 0; i < numrows; i++)
		{
			bool	isnull;
			Datum	value;

			if (!expr)
				value = heap_getattr(rows[i], heap_attnum, tdesc, &isnull);
			else
			{
				/*
				 * Reset the per-tuple context each time, to reclaim any cruft
				 * left behind by evaluating the predicate or index expressions.
				 */
				ResetExprContext(econtext);

				/* Set up for predicate or expression evaluation */
				ExecStoreHeapTuple(rows[i], slot, false);

				value = ExecEvalExpr(exprstate,
									 GetPerTupleExprContext(estate),
									 &isnull);
			}

			if (!isnull)
				values[nvalues++] = value;
		}

		qsort_arg(values, nvalues, sizeof(Datum), range_values_cmp, typcache);

		/* optimized algorithm */
		brin_minmax_match_tuples_to_ranges(ranges,
										   numrows, rows, nvalues, values,
										   typcache, stats);

#ifdef DEBUG_BRIN_STATS
		if (debug_brin_cross_check)
		{
			brin_minmax_match_tuples_to_ranges2(ranges, minranges, maxranges,
												numrows, rows, nvalues, values,
												typcache, stats);

			brin_minmax_match_tuples_to_ranges_bruteforce(ranges,
														  numrows, rows,
														  nvalues, values,
														  typcache, stats);
		}
#endif

		brin_minmax_increment_stats(minranges, maxranges, ranges->nranges,
									values, nvalues, typcache, stats);
	}

	/* XXX cleanup / refactoring needed */
	if (expr)
	{
		ExecDropSingleTupleTableSlot(slot);
		FreeExecutorState(estate);
	}

	/*
	 * Possibly quite large, so release explicitly and don't rely
	 * on the memory context to discard this.
	 */
	pfree(minranges);
	pfree(maxranges);

cleanup:
	/* possibly quite large, so release explicitly */
	pfree(ranges);

	/* free the BrinOpaque, just like brinendscan() would */
	brinRevmapTerminate(opaque->bo_rmAccess);
	brin_free_desc(opaque->bo_bdesc);

	PG_RETURN_POINTER(stats);
}

/*
 * brin_minmax_range_tupdesc
 *		Create a tuple descriptor to store BrinRange data.
 */
static TupleDesc
brin_minmax_range_tupdesc(BrinDesc *brdesc, AttrNumber attnum)
{
	TupleDesc	tupdesc;
	AttrNumber	attno = 1;

	/* expect minimum and maximum */
	Assert(brdesc->bd_info[attnum - 1]->oi_nstored == 2);

	tupdesc = CreateTemplateTupleDesc(7);

	/* blkno_start */
	TupleDescInitEntry(tupdesc, attno++, NULL, INT4OID, -1, 0);

	/* blkno_end (could be calculated as blkno_start + pages_per_range) */
	TupleDescInitEntry(tupdesc, attno++, NULL, INT4OID, -1, 0);

	/* has_nulls */
	TupleDescInitEntry(tupdesc, attno++, NULL, BOOLOID, -1, 0);

	/* all_nulls */
	TupleDescInitEntry(tupdesc, attno++, NULL, BOOLOID, -1, 0);

	/* not_summarized */
	TupleDescInitEntry(tupdesc, attno++, NULL, BOOLOID, -1, 0);

	/* min_value */
	TupleDescInitEntry(tupdesc, attno++, NULL,
					   brdesc->bd_info[attnum - 1]->oi_typcache[0]->type_id,
								   -1, 0);

	/* max_value */
	TupleDescInitEntry(tupdesc, attno++, NULL,
					   brdesc->bd_info[attnum - 1]->oi_typcache[0]->type_id,
								   -1, 0);

	return tupdesc;
}

/*
 * brin_minmax_scan_init
 *		Prepare the BrinRangeScanDesc including the sorting info etc.
 *
 * We want to have the ranges in roughly this order
 *
 * - not-summarized
 * - summarized, non-null values
 * - summarized, all-nulls
 *
 * We do it this way, because the not-summarized ranges need to be
 * scanned always (both to produce NULL and non-NULL values), and
 * we need to read all of them into the tuplesort before producing
 * anything. So placing them at the beginning is reasonable.
 *
 * The all-nulls ranges are placed last, because when processing
 * NULLs we need to scan everything anyway (some of the ranges might
 * have has_nulls=true). But for non-NULL values we can abort once
 * we hit the first all-nulls range.
 *
 * The regular ranges are sorted by blkno_start, to make it maybe
 * a bit more sequential (but this only helps if there are ranges
 * with the same minval).
 */
static BrinRangeScanDesc *
brin_minmax_scan_init(BrinDesc *bdesc, Oid collation, AttrNumber attnum, bool asc)
{
	BrinRangeScanDesc  *scan;

	/* sort by (not_summarized, minval, blkno_start, all_nulls) */
	AttrNumber			keys[4];
	Oid					collations[4];
	bool				nullsFirst[4];
	Oid					operators[4];
	Oid					typid;
	TypeCacheEntry	   *typcache;

	/* we expect to have min/max value for each range, same type for both */
	Assert(bdesc->bd_info[attnum - 1]->oi_nstored == 2);
	Assert(bdesc->bd_info[attnum - 1]->oi_typcache[0]->type_id ==
		   bdesc->bd_info[attnum - 1]->oi_typcache[1]->type_id);

	scan = (BrinRangeScanDesc *) palloc0(sizeof(BrinRangeScanDesc));

	/* build tuple descriptor for range data */
	scan->tdesc = brin_minmax_range_tupdesc(bdesc, attnum);

	/* initialize ordering info */
	keys[0] = 5;				/* not_summarized */
	keys[1] = 4;				/* all_nulls */
	keys[2] = (asc) ? 6 : 7;	/* min_value (asc) or max_value (desc) */
	keys[3] = 1;				/* blkno_start */

	collations[0] = InvalidOid;	/* FIXME */
	collations[1] = InvalidOid;	/* FIXME */
	collations[2] = collation;	/* FIXME */
	collations[3] = InvalidOid;	/* FIXME */

	/* unrelated to the ordering desired by the user */
	nullsFirst[0] = false;
	nullsFirst[1] = false;
	nullsFirst[2] = false;
	nullsFirst[3] = false;

	/* lookup sort operator for the boolean type (used for not_summarized) */
	typcache = lookup_type_cache(BOOLOID, TYPECACHE_GT_OPR);
	operators[0] = typcache->gt_opr;

	/* lookup sort operator for the boolean type (used for all_nulls) */
	typcache = lookup_type_cache(BOOLOID, TYPECACHE_LT_OPR);
	operators[1] = typcache->lt_opr;

	/* lookup sort operator for the min/max type */
	typid = bdesc->bd_info[attnum - 1]->oi_typcache[0]->type_id;
	typcache = lookup_type_cache(typid, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
	operators[2] = (asc) ? typcache->lt_opr : typcache->gt_opr;

	/* lookup sort operator for the bigint type (used for blkno_start) */
	typcache = lookup_type_cache(INT4OID, TYPECACHE_LT_OPR);
	operators[3] = typcache->lt_opr;

	/*
	 * XXX better to keep this small enough to fit into L2/L3, large values
	 * of work_mem may easily make this slower.
	 */
	scan->ranges = tuplesort_begin_heap(scan->tdesc,
										4, /* nkeys */
										keys,
										operators,
										collations,
										nullsFirst,
										work_mem,
										NULL,
										TUPLESORT_RANDOMACCESS);

	scan->slot = MakeSingleTupleTableSlot(scan->tdesc,
										  &TTSOpsMinimalTuple);

	return scan;
}

/*
 * brin_minmax_scan_add_tuple
 *		Form and store a tuple representing the BRIN range to the tuplestore.
 */
static void
brin_minmax_scan_add_tuple(BrinRangeScanDesc *scan, TupleTableSlot *slot,
						   BlockNumber block_start, BlockNumber block_end,
						   bool has_nulls, bool all_nulls, bool not_summarized,
						   Datum min_value, Datum max_value)
{
	ExecClearTuple(slot);

	memset(slot->tts_isnull, false, 7 * sizeof(bool));

	slot->tts_values[0] = UInt32GetDatum(block_start);
	slot->tts_values[1] = UInt32GetDatum(block_end);
	slot->tts_values[2] = BoolGetDatum(has_nulls);
	slot->tts_values[3] = BoolGetDatum(all_nulls);
	slot->tts_values[4] = BoolGetDatum(not_summarized);
	slot->tts_values[5] = min_value;
	slot->tts_values[6] = max_value;

	if (all_nulls || not_summarized)
	{
		slot->tts_isnull[5] = true;
		slot->tts_isnull[6] = true;
	}

	ExecStoreVirtualTuple(slot);

	tuplesort_puttupleslot(scan->ranges, slot);

	scan->nranges++;
}

#ifdef BRIN_SORT_DEBUG
/*
 * brin_minmax_scan_next
 *		Return the next BRIN range information from the tuplestore.
 *
 * Returns NULL when there are no more ranges.
 */
static BrinRange *
brin_minmax_scan_next(BrinRangeScanDesc *scan)
{
	if (tuplesort_gettupleslot(scan->ranges, true, false, scan->slot, NULL))
	{
		bool		isnull;
		BrinRange  *range = (BrinRange *) palloc(sizeof(BrinRange));

		range->blkno_start = slot_getattr(scan->slot, 1, &isnull);
		range->blkno_end = slot_getattr(scan->slot, 2, &isnull);
		range->has_nulls = slot_getattr(scan->slot, 3, &isnull);
		range->all_nulls = slot_getattr(scan->slot, 4, &isnull);
		range->not_summarized = slot_getattr(scan->slot, 5, &isnull);
		range->min_value = slot_getattr(scan->slot, 6, &isnull);
		range->max_value = slot_getattr(scan->slot, 7, &isnull);

		return range;
	}

	return NULL;
}

/*
 * brin_minmax_scan_dump
 *		Print info about all page ranges stored in the tuplestore.
 */
static void
brin_minmax_scan_dump(BrinRangeScanDesc *scan)
{
	BrinRange *range;

	if (!message_level_is_interesting(WARNING))
		return;

	elog(WARNING, "===== dumping =====");
	while ((range = brin_minmax_scan_next(scan)) != NULL)
	{
		elog(WARNING, "[%u %u] has_nulls %d all_nulls %d not_summarized %d values [%ld %ld]",
			 range->blkno_start, range->blkno_end,
			 range->has_nulls, range->all_nulls, range->not_summarized,
			 range->min_value, range->max_value);

		pfree(range);
	}

	/* reset the tuplestore, so that we can start scanning again */
	tuplesort_rescan(scan->ranges);
}
#endif

static void
brin_minmax_scan_finalize(BrinRangeScanDesc *scan)
{
	tuplesort_performsort(scan->ranges);
}

/*
 * brin_minmax_ranges
 *		Load the BRIN ranges and sort them.
 */
Datum
brin_minmax_ranges(PG_FUNCTION_ARGS)
{
	IndexScanDesc	scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	AttrNumber		attnum = PG_GETARG_INT16(1);
	bool			asc = PG_GETARG_BOOL(2);
	Oid				colloid = PG_GET_COLLATION();
	BrinOpaque *opaque;
	Relation	indexRel;
	Relation	heapRel;
	BlockNumber nblocks;
	BlockNumber	heapBlk;
	Oid			heapOid;
	BrinMemTuple *dtup;
	BrinTuple  *btup = NULL;
	Size		btupsz = 0;
	Buffer		buf = InvalidBuffer;
	BlockNumber	pagesPerRange;
	BrinDesc	   *bdesc;
	BrinRangeScanDesc *brscan;
	TupleTableSlot *slot;

	/*
	 * Determine how many BRIN ranges could there be, allocate space and read
	 * all the min/max values.
	 */
	opaque = (BrinOpaque *) scan->opaque;
	bdesc = opaque->bo_bdesc;
	pagesPerRange = opaque->bo_pagesPerRange;

	indexRel = bdesc->bd_index;

	/* make sure the provided attnum is valid */
	Assert((attnum > 0) && (attnum <= bdesc->bd_tupdesc->natts));

	/*
	 * We need to know the size of the table so that we know how long to iterate
	 * on the revmap (and to pre-allocate the arrays).
	 */
	heapOid = IndexGetRelation(RelationGetRelid(indexRel), false);
	heapRel = table_open(heapOid, AccessShareLock);
	nblocks = RelationGetNumberOfBlocks(heapRel);
	table_close(heapRel, AccessShareLock);

	/* allocate an initial in-memory tuple, out of the per-range memcxt */
	dtup = brin_new_memtuple(bdesc);

	/* initialize the scan describing scan of ranges sorted by minval */
	brscan = brin_minmax_scan_init(bdesc, colloid, attnum, asc);

	slot = MakeSingleTupleTableSlot(brscan->tdesc, &TTSOpsVirtual);

	/*
	 * Now scan the revmap.  We start by querying for heap page 0,
	 * incrementing by the number of pages per range; this gives us a full
	 * view of the table.
	 *
	 * XXX The sort may be quite expensive, e.g. for small BRIN ranges. Maybe
	 * we could optimize this somehow? For example, we know the not-summarized
	 * ranges are always going to be first, and all-null ranges last, so maybe
	 * we could stash those somewhere, and not sort them? But there are likely
	 * only very few such ranges, in most cases. Moreover, how would we then
	 * prepend/append those ranges to the sorted ones? Probably would have to
	 * store them in a tuplestore, or something.
	 *
	 * XXX Seems that having large work_mem can be quite detrimental, because
	 * then it overflows L2/L3 caches, making the sort much slower.
	 *
	 * XXX If there are other indexes, would be great to filter the ranges, so
	 * that we only sort the interesting ones - reduces the number of ranges,
	 * makes the sort faster.
	 *
	 * XXX Another option is making this incremental - e.g. only ask for the
	 * first 1000 ranges, using a top-N sort. And then if it's not enough we
	 * could request another chunk. But the second request would have to be
	 * rather unlikely (because quite expensive), and the top-N sort does not
	 * seem all that faster (as long as we don't overflow L2/L3).
	 */
	for (heapBlk = 0; heapBlk < nblocks; heapBlk += pagesPerRange)
	{
		bool		gottuple = false;
		BrinTuple  *tup;
		OffsetNumber off;
		Size		size;

		CHECK_FOR_INTERRUPTS();

		tup = brinGetTupleForHeapBlock(opaque->bo_rmAccess, heapBlk, &buf,
									   &off, &size, BUFFER_LOCK_SHARE,
									   scan->xs_snapshot);
		if (tup)
		{
			gottuple = true;
			btup = brin_copy_tuple(tup, size, btup, &btupsz);
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		}

		/*
		 * Ranges with no indexed tuple may contain anything.
		 */
		if (!gottuple)
		{
			brin_minmax_scan_add_tuple(brscan, slot,
									   heapBlk, heapBlk + (pagesPerRange - 1),
									   false, false, true, 0, 0);
		}
		else
		{
			dtup = brin_deform_tuple(bdesc, btup, dtup);
			if (dtup->bt_placeholder)
			{
				/*
				 * Placeholder tuples are treated as if not summarized.
				 *
				 * XXX Is this correct?
				 */
				brin_minmax_scan_add_tuple(brscan, slot,
										   heapBlk, heapBlk + (pagesPerRange - 1),
										   false, false, true, 0, 0);
			}
			else
			{
				BrinValues *bval;

				bval = &dtup->bt_columns[attnum - 1];

				brin_minmax_scan_add_tuple(brscan, slot,
										   heapBlk, heapBlk + (pagesPerRange - 1),
										   bval->bv_hasnulls, bval->bv_allnulls, false,
										   bval->bv_values[0], bval->bv_values[1]);
			}
		}
	}

	ExecDropSingleTupleTableSlot(slot);

	if (buf != InvalidBuffer)
		ReleaseBuffer(buf);

	/* do the sort and any necessary post-processing */
	brin_minmax_scan_finalize(brscan);

#ifdef BRIN_SORT_DEBUG
	brin_minmax_scan_dump(brscan);
#endif

	PG_RETURN_POINTER(brscan);
}

/*
 * Cache and return the procedure for the given strategy.
 *
 * Note: this function mirrors inclusion_get_strategy_procinfo; see notes
 * there.  If changes are made here, see that function too.
 */
static FmgrInfo *
minmax_get_strategy_procinfo(BrinDesc *bdesc, uint16 attno, Oid subtype,
							 uint16 strategynum)
{
	MinmaxOpaque *opaque;

	Assert(strategynum >= 1 &&
		   strategynum <= BTMaxStrategyNumber);

	opaque = (MinmaxOpaque *) bdesc->bd_info[attno - 1]->oi_opaque;

	/*
	 * We cache the procedures for the previous subtype in the opaque struct,
	 * to avoid repetitive syscache lookups.  If the subtype changed,
	 * invalidate all the cached entries.
	 */
	if (opaque->cached_subtype != subtype)
	{
		uint16		i;

		for (i = 1; i <= BTMaxStrategyNumber; i++)
			opaque->strategy_procinfos[i - 1].fn_oid = InvalidOid;
		opaque->cached_subtype = subtype;
	}

	if (opaque->strategy_procinfos[strategynum - 1].fn_oid == InvalidOid)
	{
		Form_pg_attribute attr;
		HeapTuple	tuple;
		Oid			opfamily,
					oprid;

		opfamily = bdesc->bd_index->rd_opfamily[attno - 1];
		attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);
		tuple = SearchSysCache4(AMOPSTRATEGY, ObjectIdGetDatum(opfamily),
								ObjectIdGetDatum(attr->atttypid),
								ObjectIdGetDatum(subtype),
								Int16GetDatum(strategynum));

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
				 strategynum, attr->atttypid, subtype, opfamily);

		oprid = DatumGetObjectId(SysCacheGetAttrNotNull(AMOPSTRATEGY, tuple,
														Anum_pg_amop_amopopr));
		ReleaseSysCache(tuple);
		Assert(RegProcedureIsValid(oprid));

		fmgr_info_cxt(get_opcode(oprid),
					  &opaque->strategy_procinfos[strategynum - 1],
					  bdesc->bd_context);
	}

	return &opaque->strategy_procinfos[strategynum - 1];
}
