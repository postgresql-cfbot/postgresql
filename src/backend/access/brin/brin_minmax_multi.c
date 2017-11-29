/*
 * brin_minmax_multi.c
 *		Implementation of Multi Min/Max opclass for BRIN
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * Implements a variant of minmax opclass, where the summary is composed of
 * multiple smaller intervals. This allows us to handle outliers, which
 * usually makes the simple minmax opclass inefficient.
 *
 * Consider for example page range with simple minmax interval [1000,2000],
 * and assume a new row gets inserted into the range with value 1000000.
 * Due to that the interval gets [1000,1000000]. I.e. the minmax interval
 * got 1000x wider and won't be useful to eliminate scan keys between 2001
 * and 1000000.
 *
 * With multi-minmax opclass, we may have [1000,2000] interval initially,
 * but after adding the new row we start tracking it as two interval:
 *
 *   [1000,2000] and [1000000,1000000]
 *
 * This allow us to still eliminate the page range when the scan keys hit
 * the gap between 2000 and 1000000, making it useful in cases when the
 * simple minmax opclass gets inefficient.
 *
 * The number of intervals tracked per page range is somewhat flexible.
 * What is restricted is the number of values per page range, and the limit
 * is currently 64 (see MINMAX_MAX_VALUES). Collapsed intervals (with equal
 * minimum and maximum value) are stored as a single value, while regular
 * intervals require two values.
 *
 * When the number of values gets too high (by adding new values to the
 * summary), we merge some of the intervals to free space for more values.
 * This is done in greedy way - we simply pick the two closest intervals,
 * merge them, and repeat this until the number of values to store gets
 * sufficiently low (below MINMAX_MAX_VALUES/2, but that's mostly arbitrary
 * value and may be changed easily).
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/brin/brin_minmax_multi.c
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/brin_internal.h"
#include "access/brin_tuple.h"
#include "access/stratnum.h"
#include "catalog/pg_type.h"
#include "catalog/pg_amop.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"


typedef struct MinmaxMultiOpaque
{
	Oid			cached_subtype;
	FmgrInfo	strategy_procinfos[BTMaxStrategyNumber];
} MinmaxMultiOpaque;

#define		MINMAX_MAX_VALUES	64

/*
 * The summary of multi-minmax indexes is represented by a list of intervals,
 * followed by recently added values (which may be considered to be collapsed
 * ranges).
 *
 * The 'values' array stores the intervals first (two values for each one),
 * sorted by the minimum value. Then there are nvalues "exact" values (nsort
 * of which are known to be sorted).
 *
 * This allows us to quickly add new values, and check if a value (or a scan
 * key) is already consistent with the range.
 */
typedef struct MinmaxMultiRanges
{
	/* varlena header (do not touch directly!) */
	int32	vl_len_;

	/* maxvalues >= (2*nranges + nvalues) */
	int		maxvalues;	/* maximum number of values in the buffer */
	int		nranges;	/* number of ranges in the array (stored) */
	int		nvalues;	/* number of values in the data array (all) */
	int		nsorted;	/* number of already-sorted values */

	/* values stored for this range - either raw values, or ranges */
	Datum 	values[FLEXIBLE_ARRAY_MEMBER];

} MinmaxMultiRanges;

static FmgrInfo *minmax_multi_get_strategy_procinfo(BrinDesc *bdesc, uint16 attno,
							 Oid subtype, uint16 strategynum);


/*
 * minmax_multi_init
 * 		Initialize the range list, allocate all the memory.
 */
static MinmaxMultiRanges *
minmax_multi_init(int maxvalues)
{
	Size				len;
	MinmaxMultiRanges  *ranges;

	Assert(maxvalues > 0);
	Assert(maxvalues <= 1024);	/* arbitrary limit */

	/*
	 * Allocate the range list with space for the max number of values.
	 */
	len = offsetof(MinmaxMultiRanges, values) + maxvalues * sizeof(Datum);

	ranges = (MinmaxMultiRanges *) palloc0(len);

	ranges->maxvalues = maxvalues;

	SET_VARSIZE(ranges, len);

	return ranges;
}

typedef struct compare_context
{
	FmgrInfo   *cmpFn;
	Oid			colloid;
} compare_context;

/* Used to represent ranges during merging. */
typedef struct DatumRange {
	Datum	minval;		/* lower boundary */
	Datum	maxval;		/* upper boundary */
	bool	collapsed;	/* true if minval==maxval */
} DatumRange;

/*
 * Comparator for qsort, extracting the cmp procedure from the context.
 */
static int
compare_values(const void *a, const void *b, void *arg)
{
	Datum va = *(Datum *)a;
	Datum vb = *(Datum *)b;
	Datum r;

	compare_context *cxt = (compare_context *)arg;

	r = FunctionCall2Coll(cxt->cmpFn, cxt->colloid, va, vb);

	if (DatumGetBool(r))
		return -1;

	r = FunctionCall2Coll(cxt->cmpFn, cxt->colloid, vb, va);

	if (DatumGetBool(r))
		return 1;

	return 0;
}

/*
 * Compare the ranges during merging - first by minimum, then by maximum.
 */
static int
compare_ranges(const void *a, const void *b, void *arg)
{
	DatumRange *ra = (DatumRange *)a;
	DatumRange *rb = (DatumRange *)b;
	Datum r;

	compare_context *cxt = (compare_context *)arg;

	/* first compare minvals */
	r = FunctionCall2Coll(cxt->cmpFn, cxt->colloid, ra->minval, rb->minval);

	if (DatumGetBool(r))
		return -1;

	r = FunctionCall2Coll(cxt->cmpFn, cxt->colloid, rb->minval, ra->minval);

	if (DatumGetBool(r))
		return 1;

	/* first compare maxvals */
	r = FunctionCall2Coll(cxt->cmpFn, cxt->colloid, ra->maxval, rb->maxval);

	if (DatumGetBool(r))
		return -1;

	r = FunctionCall2Coll(cxt->cmpFn, cxt->colloid, rb->maxval, ra->maxval);

	if (DatumGetBool(r))
		return 1;

	return 0;
}

/*
 * minmax_multi_contains_value
 * 		See if the new value is already contained in the range list.
 *
 * We first inspect the list of intervals. We use a small trick - we check
 * the value against min/max of the whole range (min of the first interval,
 * max of the last one) first, and only inspect the individual intervals if
 * this passes.
 *
 * If the value matches none of the intervals, we check the exact values.
 * We simply loop through them and invoke equality operator on them.
 *
 * XXX This might benefit from the fact that both the intervals and exact
 * values are sorted - we might do bsearch or something. Currently that
 * does not make much difference (there are only ~32 intervals), but if
 * this gets increased and/or the comparator function is more expensive,
 * it might be a huge win.
 */
static bool
minmax_multi_contains_value(BrinDesc *bdesc, Oid colloid,
							AttrNumber attno, Oid typid,
							MinmaxMultiRanges *ranges, Datum newval)
{
	int			i;
	FmgrInfo   *cmpFn;

	/*
	 * First inspect the ranges, if there are any. We first check the whole
	 * range, and only when there's still a chance of getting a match we
	 * inspect the individual ranges.
	 */
	if (ranges->nranges > 0)
	{
		Datum	compar;
		bool	match = true;

		Datum	minvalue = ranges->values[0];
		Datum	maxvalue = ranges->values[2*ranges->nranges - 1];

		/*
		 * Otherwise, need to compare the new value with boundaries of all
		 * the ranges. First check if it's less than the absolute minimum,
		 * which is the first value in the array.
		 */
		cmpFn = minmax_multi_get_strategy_procinfo(bdesc, attno, typid,
											 BTLessStrategyNumber);
		compar = FunctionCall2Coll(cmpFn, colloid, newval, minvalue);

		/* smaller than the smallest value in the range list */
		if (DatumGetBool(compar))
			match = false;

		/*
		 * And now compare it to the existing maximum (last value in the
		 * data array). But only if we haven't already ruled out a possible
		 * match in the minvalue check.
		 */
		if (match)
		{
			cmpFn = minmax_multi_get_strategy_procinfo(bdesc, attno, typid,
												BTGreaterStrategyNumber);
			compar = FunctionCall2Coll(cmpFn, colloid, newval, maxvalue);

			if (DatumGetBool(compar))
				match = false;
		}

		/*
		 * So it's in the general range, but is it actually covered by any
		 * of the ranges? Repeat the check for each range.
		 */
		for (i = 0; i < ranges->nranges && match; i++)
		{
			/* copy the min/max values from the ranges */
			minvalue = ranges->values[2*i];
			maxvalue = ranges->values[2*i+1];

			/*
			 * Otherwise, need to compare the new value with boundaries of all
			 * the ranges. First check if it's less than the absolute minimum,
			 * which is the first value in the array.
			 */
			cmpFn = minmax_multi_get_strategy_procinfo(bdesc, attno, typid,
												 BTLessStrategyNumber);
			compar = FunctionCall2Coll(cmpFn, colloid, newval, minvalue);

			/* smaller than the smallest value in this range */
			if (DatumGetBool(compar))
				continue;

			cmpFn = minmax_multi_get_strategy_procinfo(bdesc, attno, typid,
												 BTGreaterStrategyNumber);
			compar = FunctionCall2Coll(cmpFn, colloid, newval, maxvalue);

			/* larger than the largest value in this range */
			if (DatumGetBool(compar))
				continue;

			/* hey, we found a matching row */
			return true;
		}
	}

	/*
	 * so we're done with the ranges, now let's inspect the exact values
	 *
	 * TODO optimization by using that the first values are sorted
	 */
	for (i = 2*ranges->nranges; i < 2*ranges->nranges + ranges->nvalues; i++)
	{
		Datum compar;

		cmpFn = minmax_multi_get_strategy_procinfo(bdesc, attno, typid,
											 BTEqualStrategyNumber);

		compar = FunctionCall2Coll(cmpFn, colloid, newval, ranges->values[i]);

		/* found an exact match */
		if (DatumGetBool(compar))
			return true;
	}

	/* the value is not covered by this BRIN tuple */
	return false;
}

/*
 * merge_sort
 *	  Merge intervals and exact values into a sorted array of DatumRange.
 *
 * We merge intervals from 'ranges' and exact values into ranges_tmp. During
 * the merge we expand the values into "collapsed" ranges (with minimum and
 * maximum being the same).
 */
static int
merge_sort(FmgrInfo *cmp, Oid colloid, DatumRange *ranges_tmp, int nranges,
		   MinmaxMultiRanges *ranges, Datum *values, int nvalues)
{
	int	range_idx;
	int	value_idx;
	int	out_idx ;

	/* initialize at the start of all the arrays */
	range_idx = value_idx = out_idx = 0;

	while (range_idx < ranges->nranges && value_idx < nvalues)
	{
		Datum compar;

		compar = FunctionCall2Coll(cmp, colloid,
								   ranges->values[2*range_idx],
								   values[value_idx]);

		/* not equal, we have to store it */
		if (DatumGetBool(compar))
		{
			ranges_tmp[out_idx].minval = ranges->values[2*range_idx];
			ranges_tmp[out_idx].maxval = ranges->values[2*range_idx+1];
			ranges_tmp[out_idx].collapsed = false;
			range_idx++;
		}
		else
		{
			ranges_tmp[out_idx].minval = values[value_idx];
			ranges_tmp[out_idx].maxval = values[value_idx];
			ranges_tmp[out_idx].collapsed = true;
			value_idx++;
		}

		out_idx++;
	}

	/* exactly one of the sources is fully consumed at this point */
	Assert((range_idx == ranges->nranges) || (value_idx == nvalues));
	Assert((range_idx <  ranges->nranges) || (value_idx  < nvalues));

	/* remaining ranges */
	while (range_idx < ranges->nranges)
	{
		ranges_tmp[out_idx].minval = ranges->values[2*range_idx];
		ranges_tmp[out_idx].maxval = ranges->values[2*range_idx+1];
		ranges_tmp[out_idx].collapsed = false;
		out_idx++;
		range_idx++;
	}

	/* remaining values */
	while (value_idx < nvalues)
	{
		ranges_tmp[out_idx].minval = values[value_idx];
		ranges_tmp[out_idx].maxval = values[value_idx];
		ranges_tmp[out_idx].collapsed = true;
		out_idx++;
		value_idx++;
	}

	/* now all inputs are fully consumed */
	Assert((range_idx == ranges->nranges) && (value_idx == nvalues));

	return out_idx;
}

/*
 * merge_sort
 *	  Merge values from two sorted arrays.
 *
 * values_a and values_b are merged into values
 */
static int
merge_sort_2(FmgrInfo *cmp, Oid colloid, Datum *values, int nvalues,
			 Datum *values_a, int nvalues_a, Datum *values_b, int nvalues_b)
{
	int	a_idx;
	int	b_idx;
	int	out_idx ;

	Assert(nvalues == nvalues_a + nvalues_b);

	/* initialize at the start of all the arrays */
	a_idx = b_idx = out_idx = 0;

	while (a_idx < nvalues_a && b_idx < nvalues_b)
	{
		Datum compar;

		compar = FunctionCall2Coll(cmp, colloid, values_a[a_idx], values_b[b_idx]);

		/* not equal, we have to store it */
		if (DatumGetBool(compar))
			values[out_idx++] = values_a[a_idx++];
		else
			values[out_idx++] = values_b[b_idx++];
	}

	/* exactly one of the sources is fully consumed at this point */
	Assert((a_idx == nvalues_a) || (b_idx == nvalues_b));
	Assert((a_idx <  nvalues_a) || (b_idx  < nvalues_b));

	/* remaining values from the first array */
	while (a_idx < nvalues_a)
		values[out_idx++] = values_a[a_idx++];

	/* remaining values from the second array */
	while (b_idx < nvalues_b)
		values[out_idx++] = values_b[b_idx++];

	/* now all inputs are fully consumed */
	Assert((a_idx == nvalues_a) && (b_idx == nvalues_b));

	Assert(nvalues == out_idx);

	return out_idx;
}

/*
 * compute_distance
 *	  Compute distance between two values.
 *
 * Currently this assumes the two values are float8-based, we'll need to
 * provide this as part of the opclass.
 *
 * XXX Due to how it's executed on a sorted array, this can assume (a>b).
 */
static double
compute_distance(Datum a, Datum b)
{
	double da = DatumGetFloat8(a);
	double db = DatumGetFloat8(b);

	return (da-db);
}

/*
 * minmax_multi_add_value
 * 		Add the new value to the multi-minmax range.
 */
static void
minmax_multi_add_value(BrinDesc *bdesc, Oid colloid,
					   AttrNumber attno, Form_pg_attribute attr,
					   MinmaxMultiRanges *ranges, Datum newval)
{
	int			i;

	/* context for sorting */
	compare_context cxt;

	Assert(ranges->maxvalues >= 2*ranges->nranges + ranges->nvalues);

	/*
	 * If there's space in the values array, copy it in and we're done.
	 *
	 * If we get duplicates, it doesn't matter as we'll deduplicate the
	 * values later.
	 */
	if (ranges->maxvalues > 2*ranges->nranges + ranges->nvalues)
	{
		ranges->values[2*ranges->nranges + ranges->nvalues] = newval;
		ranges->nvalues++;
		return;
	}

	/*
	 * There's not enough space, so try deduplicating the values array,
	 * including the new value.
	 *
	 * XXX maybe try deduplicating using memcmp first, instead of using
	 * the (possibly) fairly complex/expensive comparator.
	 *
	 * XXX The if is somewhat unnecessary, because nvalues is always >= 0
	 * so we do this always.
	 */
	if (ranges->nvalues >= 0)
	{
		FmgrInfo   *cmpFn;
		int			nvalues = ranges->nvalues + 1;	/* space for newval */
		Datum	   *values = palloc0(sizeof(Datum) * nvalues);

		/* we only need to sort this many values (using qsort) */
		int			nvalues_qsort = (ranges->nvalues - ranges->nsorted) + 1;
		Datum	   *values_qsort = palloc0(sizeof(Datum) * nvalues_qsort);

		int			idx;
		DatumRange *ranges_tmp;
		int			nranges;
		int			count;

		/* sort the values */
		cxt.colloid = colloid;
		cxt.cmpFn = minmax_multi_get_strategy_procinfo(bdesc, attno, attr->atttypid,
												 BTLessStrategyNumber);

		/* put the new value at the end */
		values_qsort[nvalues_qsort-1] = newval;

		/*
		 * Do the qsort, but only when actually necessary (there are unsorted
		 * values in the values array). Copy the unsorted values first.
		 */
		if (nvalues_qsort > 1)
		{
			memcpy(values_qsort,
				   &ranges->values[2*ranges->nranges + ranges->nsorted - 1],
				   (ranges->nvalues - ranges->nsorted) * sizeof(Datum));

			qsort_arg(values_qsort, nvalues_qsort, sizeof(Datum),
					  compare_values, (void *) &cxt);
		}

		/* now do the merge sort of ranges->values and values_qsort */
		merge_sort_2(cxt.cmpFn, colloid, values, nvalues,
					 ranges->values + 2*ranges->nranges, ranges->nsorted,
					 values_qsort, nvalues_qsort);

		/* lookup equality procedure for duplicate detection */
		cmpFn = minmax_multi_get_strategy_procinfo(bdesc, attno, attr->atttypid,
												   BTEqualStrategyNumber);

		/* keep the first value and build the array of distinct values */
		idx = 1;
		for (i = 1; i < nvalues; i++)
		{
			Datum compar;

			/* is this a new value (different from the previous one)? */
			compar = FunctionCall2Coll(cmpFn, colloid, values[i-1], values[i]);

			/* not equal, we have to store it */
			if (!DatumGetBool(compar))
				values[idx++] = values[i];
		}

		/*
		 * Have we managed to reduce the number of values? If yes, we can just
		 * copy it back into the minmax range and we're done.
		 */
		if (idx < nvalues)
		{
			memcpy(ranges->values + 2*ranges->nranges, values, idx * sizeof(Datum));
			ranges->nvalues = idx;
			pfree(values);
			return;
		}

		Assert(idx == nvalues);

		/*
		 * Nope, that didn't work, we have to merge some of the ranges. To do
		 * that we'll turn the values to "collapsed" ranges (min==max), and
		 * then merge a bunch of "closest" ranges to cut the space requirements
		 * in half.
		 *
		 * XXX Do a merge sort, instead of just using qsort.
		 */
		nranges = (ranges->nranges + nvalues);
		ranges_tmp = palloc0(sizeof(DatumRange) * nranges);

		/* equality for duplicate detection */
		cmpFn = minmax_multi_get_strategy_procinfo(bdesc, attno, attr->atttypid,
												   BTLessStrategyNumber);

		idx = merge_sort(cmpFn, colloid, ranges_tmp, nranges, ranges, values, nvalues);

		Assert(idx == nranges);

		/* Now combine as many ranges until the number of values to store
		 * gets to half of MINMAX_MAX_VALUES. The collapsed ranges will be
		 * stored as a single value.
		 */
		count = ranges->nranges * 2 + nvalues;

		while (count > MINMAX_MAX_VALUES/2)
		{
			int		minidx = 0;

			double	mindistance
				= compute_distance(ranges_tmp[1].minval, ranges_tmp[0].maxval);

			/* pick the two closest ranges */
			for (i = 1; i < (nranges-1); i++)
			{
				double	distance
					= compute_distance(ranges_tmp[i+1].minval, ranges_tmp[i].maxval);

				if (distance < mindistance)
				{
					mindistance = distance;
					minidx = i;
				}
			}

			/*
			 * Update the count of Datum values we need to store, depending
			 * on what type of ranges we are going to merge.
			 *
			 * 2 - both ranges are 'regular'
			 * 1 - regular + collapsed
			 * 0 - both collapsed
			 */
			if (!ranges_tmp[minidx].collapsed && !ranges_tmp[minidx+1].collapsed)	/* both regular */
				count -= 2;
			else if (!ranges_tmp[minidx].collapsed || !ranges_tmp[minidx+1].collapsed) /* one regular */
				count -= 1;

			/*
			 * combine the two selected ranges, the new range is definiely
			 * not collapsed
			 */
			ranges_tmp[minidx].maxval = ranges_tmp[minidx+1].maxval;
			ranges_tmp[minidx].collapsed = false;

			for (i = minidx+1; i < nranges-1; i++)
				ranges_tmp[i] = ranges_tmp[i+1];

			nranges--;

			/*
			 * we can never get zero values to store
			 *
			 * XXX Actually we should never get below (MINMAX_MAX_VALUES/2 - 1)
			 * values or so.
			 */
			Assert(count > 0);
		}

		/* first copy in the regular ranges */
		ranges->nranges = 0;
		for (i = 0; i < nranges; i++)
		{
			if (!ranges_tmp[i].collapsed)
			{
				ranges->values[2*ranges->nranges    ] = ranges_tmp[i].minval;
				ranges->values[2*ranges->nranges + 1] = ranges_tmp[i].maxval;
				ranges->nranges++;
			}
		}

		/* now copy in the collapsed ones */
		ranges->nvalues = 0;
		for (i = 0; i < nranges; i++)
		{
			if (ranges_tmp[i].collapsed)
			{
				ranges->values[2*ranges->nranges + ranges->nvalues] = ranges_tmp[i].minval;
				ranges->nvalues++;
			}
		}

		/* all the exact values are sorted */
		ranges->nsorted = ranges->nsorted;

		pfree(ranges_tmp);
		pfree(values);
	}
}


Datum
brin_minmax_multi_opcinfo(PG_FUNCTION_ARGS)
{
	BrinOpcInfo *result;

	/*
	 * opaque->strategy_procinfos is initialized lazily; here it is set to
	 * all-uninitialized by palloc0 which sets fn_oid to InvalidOid.
	 */

	result = palloc0(MAXALIGN(SizeofBrinOpcInfo(1)) +
					 sizeof(MinmaxMultiOpaque));
	result->oi_nstored = 1;
	result->oi_opaque = (MinmaxMultiOpaque *)
		MAXALIGN((char *) result + SizeofBrinOpcInfo(1));
	result->oi_typcache[0] = lookup_type_cache(BYTEAOID, 0);

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
brin_minmax_multi_add_value(PG_FUNCTION_ARGS)
{
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *column = (BrinValues *) PG_GETARG_POINTER(1);
	Datum		newval = PG_GETARG_DATUM(2);
	bool		isnull = PG_GETARG_DATUM(3);
	Oid			colloid = PG_GET_COLLATION();
	bool		updated = false;
	Form_pg_attribute attr;
	AttrNumber	attno;
	MinmaxMultiRanges *ranges;

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
	attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);

	/*
	 * If this is the first non-null value, we need to initialize the range
	 * list. Otherwise just extract the existing range list from BrinValues.
	 */
	if (column->bv_allnulls)
	{
		ranges = minmax_multi_init(MINMAX_MAX_VALUES);
		column->bv_values[0] = PointerGetDatum(ranges);
		column->bv_allnulls = false;
		updated = true;
	}
	else
		ranges = (MinmaxMultiRanges *) PG_DETOAST_DATUM(column->bv_values[0]);

	/*
	 * If the new value is already covered by the existing values (or ranges)
	 * in the BRIN tuple, we're done. We can't really exit when we just
	 * created the ranges.
	 */
	if (minmax_multi_contains_value(bdesc, colloid, attno, attr->atttypid, ranges, newval))
		PG_RETURN_BOOL(updated);

	/* */
	minmax_multi_add_value(bdesc, colloid, attno, attr, ranges, newval);

	PG_RETURN_BOOL(true);
}

/*
 * Given an index tuple corresponding to a certain page range and a scan key,
 * return whether the scan key is consistent with the index tuple's min/max
 * values.  Return true if so, false otherwise.
 */
Datum
brin_minmax_multi_consistent(PG_FUNCTION_ARGS)
{
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *column = (BrinValues *) PG_GETARG_POINTER(1);
	ScanKey	   *keys = (ScanKey *) PG_GETARG_POINTER(2);
	int			nkeys = PG_GETARG_INT32(3);
	Oid			colloid = PG_GET_COLLATION(),
				subtype;
	AttrNumber	attno;
	Datum		value;
	FmgrInfo   *finfo;
	MinmaxMultiRanges	*ranges;
	int			keyno;
	int			rangeno;
	int			i;

	ranges = (MinmaxMultiRanges *) PG_DETOAST_DATUM(column->bv_values[0]);

	/* inspect the ranges, and for each one evaluate the scan keys */
	for (rangeno = 0; rangeno < ranges->nranges; rangeno++)
	{
		Datum	minval = ranges->values[2*rangeno];
		Datum	maxval = ranges->values[2*rangeno+1];

		/* assume the range is matching, and we'll try to prove otherwise */
		bool	matching = true;

		for (keyno = 0; keyno < nkeys; keyno++)
		{
			Datum	matches;
			ScanKey	key = keys[keyno];

			/* NULL keys are handled and filtered-out in bringetbitmap */
			Assert(!(key->sk_flags & SK_ISNULL));

			attno = key->sk_attno;
			subtype = key->sk_subtype;
			value = key->sk_argument;
			switch (key->sk_strategy)
			{
				case BTLessStrategyNumber:
				case BTLessEqualStrategyNumber:
					finfo = minmax_multi_get_strategy_procinfo(bdesc, attno, subtype,
															   key->sk_strategy);
					/* first value from the array */
					matches = FunctionCall2Coll(finfo, colloid, minval, value);
					break;

				case BTEqualStrategyNumber:
				{
					Datum		compar;
					FmgrInfo   *cmpFn;

					/* by default this range does not match */
					matches = false;

					/*
					 * Otherwise, need to compare the new value with boundaries of all
					 * the ranges. First check if it's less than the absolute minimum,
					 * which is the first value in the array.
					 */
					cmpFn = minmax_multi_get_strategy_procinfo(bdesc, attno, subtype,
															   BTLessStrategyNumber);
					compar = FunctionCall2Coll(cmpFn, colloid, value, minval);

					/* smaller than the smallest value in this range */
					if (DatumGetBool(compar))
						break;

					cmpFn = minmax_multi_get_strategy_procinfo(bdesc, attno, subtype,
															   BTGreaterStrategyNumber);
					compar = FunctionCall2Coll(cmpFn, colloid, value, maxval);

					/* larger than the largest value in this range */
					if (DatumGetBool(compar))
						break;

					/* haven't managed to eliminate this range, so consider it matching */
					matches = true;

					break;
				}
				case BTGreaterEqualStrategyNumber:
				case BTGreaterStrategyNumber:
					finfo = minmax_multi_get_strategy_procinfo(bdesc, attno, subtype,
															   key->sk_strategy);
					/* last value from the array */
					matches = FunctionCall2Coll(finfo, colloid, maxval, value);
					break;

				default:
					/* shouldn't happen */
					elog(ERROR, "invalid strategy number %d", key->sk_strategy);
					matches = 0;
					break;
			}

			/* the range has to match all the scan keys */
			matching &= DatumGetBool(matches);

			/* once we find a non-matching key, we're done */
			if (! matching)
				break;
		}

		/* have we found a range matching all scan keys? if yes, we're
		 * done */
		if (matching)
			PG_RETURN_DATUM(BoolGetDatum(true));
	}

	/* and now inspect the values */
	for (i = 0; i < ranges->nvalues; i++)
	{
		Datum	val = ranges->values[2*ranges->nranges + i];

		/* assume the range is matching, and we'll try to prove otherwise */
		bool	matching = true;

		for (keyno = 0; keyno < nkeys; keyno++)
		{
			Datum	matches;
			ScanKey	key = keys[keyno];

			/* we've already dealt with NULL keys at the beginning */
			if (key->sk_flags & SK_ISNULL)
				continue;

			attno = key->sk_attno;
			subtype = key->sk_subtype;
			value = key->sk_argument;
			switch (key->sk_strategy)
			{
				case BTLessStrategyNumber:
				case BTLessEqualStrategyNumber:
				case BTEqualStrategyNumber:
				case BTGreaterEqualStrategyNumber:
				case BTGreaterStrategyNumber:
				
					finfo = minmax_multi_get_strategy_procinfo(bdesc, attno, subtype,
															   key->sk_strategy);
					matches = FunctionCall2Coll(finfo, colloid, value, val);
					break;

				default:
					/* shouldn't happen */
					elog(ERROR, "invalid strategy number %d", key->sk_strategy);
					matches = 0;
					break;
			}

			/* the range has to match all the scan keys */
			matching &= DatumGetBool(matches);

			/* once we find a non-matching key, we're done */
			if (! matching)
				break;
		}

		/* have we found a range matching all scan keys? if yes, we're
		 * done */
		if (matching)
			PG_RETURN_DATUM(BoolGetDatum(true));
	}

	PG_RETURN_DATUM(BoolGetDatum(false));
}

/*
 * Given two BrinValues, update the first of them as a union of the summary
 * values contained in both.  The second one is untouched.
 */
Datum
brin_minmax_multi_union(PG_FUNCTION_ARGS)
{
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *col_a = (BrinValues *) PG_GETARG_POINTER(1);
	BrinValues *col_b = (BrinValues *) PG_GETARG_POINTER(2);
	Oid			colloid = PG_GET_COLLATION();
	MinmaxMultiRanges *ranges_a;
	MinmaxMultiRanges *ranges_b;
	DatumRange		  *ranges_tmp;
	AttrNumber	attno;
	Form_pg_attribute attr;
	int			nranges;
	int			i;
	int			idx;
	int			count;

	/* context for sorting */
	compare_context cxt;

	Assert(col_a->bv_attno == col_b->bv_attno);

	/* Adjust "hasnulls" */
	if (!col_a->bv_hasnulls && col_b->bv_hasnulls)
		col_a->bv_hasnulls = true;

	/* If there are no values in B, there's nothing left to do */
	if (col_b->bv_allnulls)
		PG_RETURN_VOID();

	attno = col_a->bv_attno;
	attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);

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

	ranges_a = (MinmaxMultiRanges *) PG_DETOAST_DATUM(col_a->bv_values[0]);
	ranges_b = (MinmaxMultiRanges *) PG_DETOAST_DATUM(col_b->bv_values[0]);

	/* make neither of the ranges is NULL */
	Assert(ranges_a && ranges_b);

	nranges = (ranges_a->nranges + ranges_a->nvalues) +
			  (ranges_b->nranges + ranges_b->nvalues);

	ranges_tmp = (DatumRange *)palloc0(nranges * sizeof(DatumRange));

	idx = 0;

	for (i = 0; i < ranges_a->nranges; i++)
	{
		ranges_tmp[idx].minval = ranges_a->values[2*i];
		ranges_tmp[idx].maxval = ranges_a->values[2*i+1];
		ranges_tmp[idx].collapsed = false;
		idx++;
	}

	for (i = 0; i < ranges_a->nvalues; i++)
	{
		ranges_tmp[idx].minval = ranges_a->values[2*ranges_a->nranges + i];
		ranges_tmp[idx].maxval = ranges_a->values[2*ranges_a->nranges + i];
		ranges_tmp[idx].collapsed = true;
		idx++;
	}

	for (i = 0; i < ranges_b->nranges; i++)
	{
		ranges_tmp[idx].minval = ranges_b->values[2*i];
		ranges_tmp[idx].maxval = ranges_b->values[2*i+1];
		ranges_tmp[idx].collapsed = false;
		idx++;
	}

	for (i = 0; i < ranges_b->nvalues; i++)
	{
		ranges_tmp[idx].minval = ranges_b->values[2*ranges_b->nranges + i];
		ranges_tmp[idx].maxval = ranges_b->values[2*ranges_b->nranges + i];
		ranges_tmp[idx].collapsed = true;
		idx++;
	}

	Assert(idx == nranges);

	/* sort the values */
	cxt.colloid = colloid;
	cxt.cmpFn = minmax_multi_get_strategy_procinfo(bdesc, attno, attr->atttypid,
												   BTLessStrategyNumber);

	qsort_arg(ranges_tmp, nranges, sizeof(DatumRange), compare_ranges, (void *) &cxt);

	/* for each range, see if it overlaps with the subsequent ones */
	while (true)
	{
		int		j;
		bool	merged = false;

		for (i = 0; i < (nranges-1); i++)
		{
			Datum	r;

			/*
			 * comparing [?,maxval] vs. [minval,?], so those two ranges overlap
			 * if minval < maxval
			 */
			r = FunctionCall2Coll(cxt.cmpFn, colloid,
								  ranges_tmp[i].maxval, ranges_tmp[i+1].minval);

			if (DatumGetBool(r))
				continue;

			/*
			 * So 'i' and 'i+1' overlap, but we don't know if 'i+1' is contained
			 * in 'i', or if they only partially overlap. So compare upper bounds
			 * and use the larger one.
			 */
			r = FunctionCall2Coll(cxt.cmpFn, colloid,
								  ranges_tmp[i].maxval, ranges_tmp[i+1].maxval);

			if (DatumGetBool(r))
				ranges_tmp[i].maxval = ranges_tmp[i+1].maxval;

			/* in any case, the range certainly is no longer collapsed */
			ranges_tmp[i].collapsed = false;

			/*
			 * now get rid of the i+1 range entirely by shifting all subsequent
			 * ranges by 1
			 */
			for (j = (i+1); j < (nranges-1); j++)
				ranges_tmp[j] = ranges_tmp[j+1];

			/* decrease the number of ranges, and we're done */
			nranges = nranges - 1;
			merged = true;
		}

		/* if we haven't merged anything this round, we're done */
		if (!merged)
			break;
	}

#ifdef USE_ASSERT_CHECKING
	/* check that the ranges are valid */
	for (i = 0; i < nranges; i++)
	{
		Datum r;
		FmgrInfo *eq;
		FmgrInfo *lt;

		eq = minmax_multi_get_strategy_procinfo(bdesc, attno, attr->atttypid,
												BTEqualStrategyNumber);

		lt = minmax_multi_get_strategy_procinfo(bdesc, attno, attr->atttypid,
												BTLessStrategyNumber);

		if (ranges_tmp[i].collapsed)	/* collapsed: minval == maxval */
		{
			r = FunctionCall2Coll(eq, colloid,
								  ranges_tmp[i].minval, ranges_tmp[i].maxval);
			Assert(DatumGetBool(r));
		}
		else	/* non-collapsed: minval < maxval */
		{
			r = FunctionCall2Coll(lt, colloid,
								  ranges_tmp[i].minval, ranges_tmp[i].maxval);
			Assert(DatumGetBool(r));
		}
	}

	/* ranges are non-overlapping and correctly sorted */
	for (i = 0; i < nranges-1; i++)
	{
		Datum r;
		FmgrInfo *lt;

		lt = minmax_multi_get_strategy_procinfo(bdesc, attno, attr->atttypid,
												BTLessStrategyNumber);

		r = FunctionCall2Coll(lt, colloid,
							  ranges_tmp[i].maxval, ranges_tmp[i+1].minval);
		Assert(DatumGetBool(r));
	}
#endif

	/*
	 * See how many values would be needed to store the current ranges, and if
	 * needed combine as many off them to get below half of MINMAX_MAX_VALUES.
	 * The collapsed ranges will be stored as a single value.
	 */
	count = 0;

	for (i = 0; i < nranges; i++)
		count += ranges_tmp[i].collapsed ? 1 : 2;

	while (count > MINMAX_MAX_VALUES/2)
	{
		int		minidx = 0;
		double	mindistance = DatumGetFloat8(ranges_tmp[1].minval) - DatumGetFloat8(ranges_tmp[0].maxval);

		/* pick the two closest ranges */
		for (i = 1; i < (nranges-1); i++)
		{
			double	distance = DatumGetFloat8(ranges_tmp[i+1].minval) - DatumGetFloat8(ranges_tmp[i-1].maxval);
			if (distance < mindistance)
			{
				mindistance = distance;
				minidx = i;
			}
		}

		/*
		 * Update the count of Datum values we need to store, depending
		 * on what type of ranges we merged.
		 *
		 * 2 - when both ranges are 'regular'
		 * 1 - when regular + collapsed
		 * 0 - when both collapsed
		 */
		if (!ranges_tmp[minidx].collapsed && !ranges_tmp[minidx+1].collapsed)	/* both regular */
			count -= 2;
		else if (!ranges_tmp[minidx].collapsed || !ranges_tmp[minidx+1].collapsed) /* one regular */
			count -= 1;

		/*
		 * combine the two selected ranges, the new range is definiely
		 * not collapsed
		 */
		ranges_tmp[minidx].maxval = ranges_tmp[minidx+1].maxval;
		ranges_tmp[minidx].collapsed = false;

		for (i = minidx+1; i < nranges-1; i++)
			ranges_tmp[i] = ranges_tmp[i+1];

		nranges--;

		/*
		 * we can never get zero values
		 *
		 * XXX Actually we should never get below (MINMAX_MAX_VALUES/2 - 1)
		 * values or so.
		 */
		Assert(count > 0);
	}

	/* first copy in the regular ranges */
	ranges_a->nranges = 0;
	for (i = 0; i < nranges; i++)
	{
		if (!ranges_tmp[i].collapsed)
		{
			ranges_a->values[2*ranges_a->nranges    ] = ranges_tmp[i].minval;
			ranges_a->values[2*ranges_a->nranges + 1] = ranges_tmp[i].maxval;
			ranges_a->nranges++;
		}
	}

	/* now copy in the collapsed ones */
	ranges_a->nvalues = 0;
	for (i = 0; i < nranges; i++)
	{
		if (ranges_tmp[i].collapsed)
		{
			ranges_a->values[2*ranges_a->nranges + ranges_a->nvalues] = ranges_tmp[i].minval;
			ranges_a->nvalues++;
		}
	}

	/* all the exact values are sorted */
	ranges_a->nsorted = ranges_a->nsorted;

	pfree(ranges_tmp);

	PG_RETURN_VOID();
}

/*
 * Cache and return the procedure for the given strategy.
 *
 * Note: this function mirrors inclusion_get_strategy_procinfo; see notes
 * there.  If changes are made here, see that function too.
 */
static FmgrInfo *
minmax_multi_get_strategy_procinfo(BrinDesc *bdesc, uint16 attno, Oid subtype,
							 uint16 strategynum)
{
	MinmaxMultiOpaque *opaque;

	Assert(strategynum >= 1 &&
		   strategynum <= BTMaxStrategyNumber);

	opaque = (MinmaxMultiOpaque *) bdesc->bd_info[attno - 1]->oi_opaque;

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
