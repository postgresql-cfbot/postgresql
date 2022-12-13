/*
 * brin_minmax.c
 *		Implementation of Min/Max opclass for BRIN
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
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
#include "catalog/pg_amop.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

typedef struct MinmaxOpaque
{
	Oid			cached_subtype;
	FmgrInfo	strategy_procinfos[BTMaxStrategyNumber];
} MinmaxOpaque;

static FmgrInfo *minmax_get_strategy_procinfo(BrinDesc *bdesc, uint16 attno,
											  Oid subtype, uint16 strategynum);

#define STATS_CROSS_CHECK

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
typedef struct c
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

	c = FunctionCall2Coll(cmpfunc, typentry->typcollation,
						  ra->min_value, rb->min_value);
	return DatumGetInt32(c);
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

	c = FunctionCall2Coll(cmpfunc, typentry->typcollation,
						  ra->max_value, rb->max_value);
	return DatumGetInt32(c);
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
		int	midpoint;
		int	r;

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

	Assert(ranges[start]->min_value > value);
	Assert(ranges[start-1]->min_value <= value);

	return start;
}


/*
 * lower_bound
 *		Determine first index so that (values[index] >= value).
 *
 * The array of ranges is expected to be sorted by maxvalue, so this is the first
 * range that can possibly intersect with range having "value" as minval.
 */
static int
lower_bound(Datum *values, int nvalues, Datum value, TypeCacheEntry *typcache)
{
	int		start = 0,
			end = (nvalues - 1);

	// everything matches
	if (range_values_cmp(&value, &values[start], typcache) <= 0)
		return 0;

	// no matches
	if (range_values_cmp(&value, &values[end], typcache) > 0)
		return nvalues;

	while ((end - start) > 0)
	{
		int	midpoint;
		int	r;

		midpoint = start + (end - start) / 2;

		r = range_values_cmp(&value, &values[midpoint], typcache);

		if (r <= 0)
			end = midpoint;
		else
			start = (midpoint + 1);
	}

	Assert(values[start] >= value);
	Assert(values[start-1] < value);

	return start;
}

/*
 * upper_bound
 *		Determine first index so that (values[index] > value).
 *
 * The array of ranges is expected to be sorted by minvalue, so this is the first
 * range that can't possibly intersect with a range having "value" as maxval.
 */
static int
upper_bound(Datum *values, int nvalues, Datum value, TypeCacheEntry *typcache)
{
	int		start = 0,
			end = (nvalues - 1);

	// everything matches
	if (range_values_cmp(&value, &values[end], typcache) >= 0)
		return nvalues;

	// no matches
	if (range_values_cmp(&value, &values[start], typcache) < 0)
		return 0;

	while ((end - start) > 0)
	{
		int midpoint;
		int r;

		midpoint = start + (end - start) / 2;

		r = range_values_cmp(&value, &values[midpoint], typcache);

		if (r >= 0)
			start = midpoint + 1;
		else
			end = midpoint;
	}

	Assert(values[start] > value);
	Assert(values[start-1] <= value);

	return start;
}

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
	int		value;
	int		count;
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
 *		Add a hit for a particular value to the histogram.
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
		elog(WARNING, "bin %d value %d count %d",
			 i, hist->bins[i].value, hist->bins[i].count);
	}
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
static int
brin_minmax_count_overlaps(BrinRange **minranges, int nranges, TypeCacheEntry *typcache)
{
	int noverlaps;

#ifdef STATS_CROSS_CHECK
	TimestampTz		start_ts = GetCurrentTimestamp();
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

#ifdef STATS_CROSS_CHECK
	elog(WARNING, "----- brin_minmax_count_overlaps -----");
	elog(WARNING, "noverlaps = %d", noverlaps);
	elog(WARNING, "duration = %ld", TimestampDifferenceMilliseconds(start_ts,
									GetCurrentTimestamp()));
#endif

	return noverlaps;
}

#ifdef STATS_CROSS_CHECK
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
static int
brin_minmax_count_overlaps2(BrinRanges *ranges,
						   BrinRange **minranges, BrinRange **maxranges,
						   TypeCacheEntry *typcache)
{
	int noverlaps;

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
		int			idx = i+1;
		BrinRange *ra = maxranges[i];
		uint64		min_index = ra->min_index;

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

	elog(WARNING, "----- brin_minmax_count_overlaps2 -----");
	elog(WARNING, "noverlaps = %d", noverlaps);
	elog(WARNING, "duration = %ld", TimestampDifferenceMilliseconds(start_ts,
									GetCurrentTimestamp()));

	return noverlaps;
}

/*
 * brin_minmax_count_overlaps_bruteforce
 *		Calculate number of overlaps by brute force.
 *
 * Actually compares every range to every other range. Quite expensive, used
 * primarily to cross-check the other algorithms. 
 */
static int
brin_minmax_count_overlaps_bruteforce(BrinRanges *ranges, TypeCacheEntry *typcache)
{
	int noverlaps;

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

			if (i == j)
				continue;

			if (range_values_cmp(&ra->max_value, &rb->min_value, typcache) < 0)
				continue;

			if (range_values_cmp(&rb->max_value, &ra->min_value, typcache) < 0)
				continue;

			elog(DEBUG1, "[%ld,%ld] overlaps [%ld,%ld]",
				 ra->min_value, ra->max_value,
				 rb->min_value, rb->max_value);

			noverlaps++;
		}
	}

	elog(WARNING, "----- brin_minmax_count_overlaps_bruteforce -----");
	elog(WARNING, "noverlaps = %d", noverlaps);
	elog(WARNING, "duration = %ld", TimestampDifferenceMilliseconds(start_ts,
									GetCurrentTimestamp()));

	return noverlaps;
}
#endif

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
								   int *res_nmatches,
								   int *res_nmatches_unique,
								   int *res_nvalues_unique)
{
	int		nmatches = 0;
	int		nmatches_unique = 0;
	int		nvalues_unique = 0;
	int		nmatches_value = 0;

	int	   *unique = (int *) palloc0(sizeof(int) * nvalues);

#ifdef STATS_CROSS_CHECK
	TimestampTz		start_ts = GetCurrentTimestamp();
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
		int		start;
		int		end;

		start = lower_bound(values, nvalues, ranges->ranges[i].min_value, typcache);
		end = upper_bound(values, nvalues, ranges->ranges[i].max_value, typcache);

		Assert(end > start);

		nmatches_value = (end - start);
		nmatches_unique += (unique[end-1] - unique[start] + 1);

		nmatches += nmatches_value;
	}

#ifdef STATS_CROSS_CHECK
	elog(WARNING, "----- brin_minmax_match_tuples_to_ranges -----");
	elog(WARNING, "nmatches = %d %f", nmatches, (double) nmatches / numrows);
	elog(WARNING, "nmatches unique = %d %d %f", nmatches_unique, nvalues_unique,
		 (double) nmatches_unique / nvalues_unique);
	elog(WARNING, "duration = %ld", TimestampDifferenceMilliseconds(start_ts,
									GetCurrentTimestamp()));
#endif

	*res_nmatches = nmatches;
	*res_nmatches_unique = nmatches_unique;
	*res_nvalues_unique = nvalues_unique;
}

#ifdef STATS_CROSS_CHECK
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
								    int *res_nmatches,
								    int *res_nmatches_unique,
								    int *res_nvalues_unique)
{
	int		nmatches = 0;
	int		nmatches_unique = 0;
	int		nvalues_unique = 0;
	histogram_t *hist = histogram_init();
	histogram_t *hist_unique = histogram_init();
	int		nmatches_value = 0;

	TimestampTz		start_ts = GetCurrentTimestamp();

	for (int i = 0; i < nvalues; i++)
	{
		int		start;
		int		end;

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

	elog(WARNING, "----- brin_minmax_match_tuples_to_ranges2 -----");
	elog(WARNING, "nmatches = %d %f", nmatches, (double) nmatches / numrows);
	elog(WARNING, "nmatches unique = %d %d %f",
		 nmatches_unique, nvalues_unique, (double) nmatches_unique / nvalues_unique);
	elog(WARNING, "duration = %ld", TimestampDifferenceMilliseconds(start_ts,
									GetCurrentTimestamp()));

	pg_qsort(hist->bins, hist->nbins, sizeof(histogram_bin_t), histogram_bin_cmp);
	pg_qsort(hist_unique->bins, hist_unique->nbins, sizeof(histogram_bin_t), histogram_bin_cmp);

	histogram_print(hist);
	histogram_print(hist_unique);

	pfree(hist);
	pfree(hist_unique);

	*res_nmatches = nmatches;
	*res_nmatches_unique = nmatches_unique;
	*res_nvalues_unique = nvalues_unique;
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
											  int *res_nmatches,
											  int *res_nmatches_unique,
											  int *res_nvalues_unique)
{
	int nmatches = 0;
	int nmatches_unique = 0;
	int nvalues_unique = 0;

	TimestampTz		start_ts = GetCurrentTimestamp();

	for (int i = 0; i < nvalues; i++)
	{
		bool	is_unique;
		int		nmatches_value = 0;

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

	elog(WARNING, "----- brin_minmax_match_tuples_to_ranges_bruteforce -----");
	elog(WARNING, "nmatches = %d %f", nmatches, (double) nmatches / numrows);
	elog(WARNING, "nmatches unique = %d %d %f", nmatches_unique, nvalues_unique,
		 (double) nmatches_unique / nvalues_unique);
	elog(WARNING, "duration = %ld", TimestampDifferenceMilliseconds(start_ts,
									GetCurrentTimestamp()));

	*res_nmatches = nmatches;
	*res_nmatches_unique = nmatches_unique;
	*res_nvalues_unique = nvalues_unique;
}
#endif

/*
 * brin_minmax_stats
 *		Calculate custom statistics for a BRIN minmax index.
 */
Datum
brin_minmax_stats(PG_FUNCTION_ARGS)
{
	Relation		heapRel = (Relation) PG_GETARG_POINTER(0);
	Relation		indexRel = (Relation) PG_GETARG_POINTER(1);
	AttrNumber		attnum = PG_GETARG_INT16(2);
	AttrNumber		heap_attnum = PG_GETARG_INT16(3);
	HeapTuple	   *rows = (HeapTuple *) PG_GETARG_POINTER(4);
	int				numrows = PG_GETARG_INT32(5);

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

	Oid				typoid;
	TypeCacheEntry *typcache;
	BrinRange	  **minranges,
				  **maxranges;
	int64			noverlaps;
	int64			prev_min_index;

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

					range->min_value = bval->bv_values[0];
					range->max_value = bval->bv_values[1];
				}
			}
		}
	}

	if (buf != InvalidBuffer)
		ReleaseBuffer(buf);

	elog(WARNING, "extracted ranges %d from BRIN index", ranges->nranges);

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

	noverlaps = brin_minmax_count_overlaps(minranges, ranges->nranges, typcache);

	/* calculate average number of overlapping ranges for any range */
	stats->avg_overlaps = (double) noverlaps / ranges->nranges;

#ifdef STATS_CROSS_CHECK
	brin_minmax_count_overlaps2(ranges, minranges, maxranges, typcache);
	brin_minmax_count_overlaps_bruteforce(ranges, typcache);
#endif

	/* match tuples to ranges */
	{
		int		nvalues = 0;
		int		nmatches,
				nmatches_unique,
				nvalues_unique;

		Datum  *values = (Datum *) palloc0(numrows * sizeof(Datum));

		TupleDesc	tdesc = RelationGetDescr(heapRel);

		for (int i = 0; i < numrows; i++)
		{
			bool	isnull;
			Datum	value;

			value = heap_getattr(rows[i], heap_attnum, tdesc, &isnull);
			if (!isnull)
				values[nvalues++] = value;
		}

		qsort_arg(values, nvalues, sizeof(Datum), range_values_cmp, typcache);

		/* optimized algorithm */
		brin_minmax_match_tuples_to_ranges(ranges,
										   numrows, rows, nvalues, values,
										   typcache,
										   &nmatches,
										   &nmatches_unique,
										   &nvalues_unique);

		stats->avg_matches = (double) nmatches / numrows;
		stats->avg_matches_unique = (double) nmatches_unique / nvalues_unique;

#ifdef STATS_CROSS_CHECK
		brin_minmax_match_tuples_to_ranges2(ranges, minranges, maxranges,
										    numrows, rows, nvalues, values,
										    typcache,
										    &nmatches,
										    &nmatches_unique,
										    &nvalues_unique);

		brin_minmax_match_tuples_to_ranges_bruteforce(ranges,
													  numrows, rows,
													  nvalues, values,
													  typcache,
													  &nmatches,
													  &nmatches_unique,
													  &nvalues_unique);
#endif
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
		bool		isNull;

		opfamily = bdesc->bd_index->rd_opfamily[attno - 1];
		attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);
		tuple = SearchSysCache4(AMOPSTRATEGY, ObjectIdGetDatum(opfamily),
								ObjectIdGetDatum(attr->atttypid),
								ObjectIdGetDatum(subtype),
								Int16GetDatum(strategynum));

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
				 strategynum, attr->atttypid, subtype, opfamily);

		oprid = DatumGetObjectId(SysCacheGetAttr(AMOPSTRATEGY, tuple,
												 Anum_pg_amop_amopopr, &isNull));
		ReleaseSysCache(tuple);
		Assert(!isNull && RegProcedureIsValid(oprid));

		fmgr_info_cxt(get_opcode(oprid),
					  &opaque->strategy_procinfos[strategynum - 1],
					  bdesc->bd_context);
	}

	return &opaque->strategy_procinfos[strategynum - 1];
}
