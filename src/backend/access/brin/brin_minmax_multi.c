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
 * is currently 64 (see values_per_range reloption). Collapsed intervals
 * (with equal minimum and maximum value) are stored as a single value,
 * while regular intervals require two values.
 *
 * When the number of values gets too high (by adding new values to the
 * summary), we merge some of the intervals to free space for more values.
 * This is done in a greedy way - we simply pick the two closest intervals,
 * merge them, and repeat this until the number of values to store gets
 * sufficiently low (below 75% of maximum values), but that is mostly
 * arbitrary threshold and may be changed easily).
 *
 * To pick the closest intervals we use the "distance" support procedure,
 * which measures space between two ranges (i.e. length of an interval).
 * The computed value may be an approximation - in the worst case we will
 * merge two ranges that are slightly less optimal at that step, but the
 * index should still produce correct results.
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/brin/brin_minmax_multi.c
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/brin.h"
#include "access/brin_internal.h"
#include "access/brin_tuple.h"
#include "access/stratnum.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "catalog/pg_amop.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datum.h"
#include "utils/inet.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/uuid.h"

/*
 * Additional SQL level support functions
 *
 * Procedure numbers must not use values reserved for BRIN itself; see
 * brin_internal.h.
 */
#define		MINMAX_MAX_PROCNUMS		1	/* maximum support procs we need */
#define		PROCNUM_DISTANCE		11	/* required, distance between values*/

/*
 * Subtract this from procnum to obtain index in MinmaxMultiOpaque arrays
 * (Must be equal to minimum of private procnums).
 */
#define		PROCNUM_BASE			11


typedef struct MinmaxMultiOpaque
{
	FmgrInfo	extra_procinfos[MINMAX_MAX_PROCNUMS];
	bool		extra_proc_missing[MINMAX_MAX_PROCNUMS];
	Oid			cached_subtype;
	FmgrInfo	strategy_procinfos[BTMaxStrategyNumber];
} MinmaxMultiOpaque;


/*
 * The summary of multi-minmax indexes has two representations - Ranges for
 * convenient processing, and SerializedRanges for storage in bytea value.
 *
 * The Ranges struct stores the boundary values in a single array, but we
 * treat regular and single-point ranges differently to save space. For
 * regular ranges (with different boundary values) we have to store both
 * values, while for "single-point ranges" we only need to save one value.
 *
 * The 'values' array stores boundary values for regular ranges first (there
 * are 2*nranges values to store), and then the nvalues boundary values for
 * single-point ranges. That is, we have (2*nranges + nvalues) boundary
 * values in the array.
 *
 * +---------------------------------+-------------------------------+
 * | ranges (sorted pairs of values) | sorted values (single points) |
 * +---------------------------------+-------------------------------+
 *
 * This allows us to quickly add new values, and store outliers without
 * making the other ranges very wide.
 *
 * We never store more than maxvalues values (as set by values_per_range
 * reloption). If needed we merge some of the ranges.
 *
 * To minimize palloc overhead, we always allocate the full array with
 * space for maxvalues elements. This should be fine as long as the
 * maxvalues is reasonably small (64 seems fine), which is the case
 * thanks to values_per_range reloption being limited to 256.
 */
typedef struct Ranges
{
	/* (2*nranges + nvalues) <= maxvalues */
	int		nranges;	/* number of ranges in the array (stored) */
	int		nvalues;	/* number of values in the data array (all) */
	int		maxvalues;	/* maximum number of values (reloption) */

	/* values stored for this range - either raw values, or ranges */
	Datum	values[FLEXIBLE_ARRAY_MEMBER];
} Ranges;

/*
 * On-disk the summary is stored as a bytea value, represented by the
 * SerializedRanges structure. It has a 4B varlena header, so can treated
 * as varlena directly.
 *
 * See range_serialize/range_deserialize methods for serialization details.
 */
typedef struct SerializedRanges
{
	/* varlena header (do not touch directly!) */
	int32	vl_len_;

	/* (2*nranges + nvalues) <= maxvalues */
	int		nranges;	/* number of ranges in the array (stored) */
	int		nvalues;	/* number of values in the data array (all) */
	int		maxvalues;	/* maximum number of values (reloption) */

	/* contains the actual data */
	char	data[FLEXIBLE_ARRAY_MEMBER];
} SerializedRanges;

static SerializedRanges *range_serialize(Ranges *range,
				AttrNumber attno, Form_pg_attribute attr);

static Ranges *range_deserialize(SerializedRanges *range,
				AttrNumber attno, Form_pg_attribute attr);

/* Cache for support and strategy procesures. */

static FmgrInfo *minmax_multi_get_procinfo(BrinDesc *bdesc, uint16 attno,
					   uint16 procnum);

static FmgrInfo *minmax_multi_get_strategy_procinfo(BrinDesc *bdesc,
					   uint16 attno, Oid subtype, uint16 strategynum);


/*
 * minmax_multi_init
 * 		Initialize the deserialized range list, allocate all the memory.
 *
 * This is only in-memory representation of the ranges, so we allocate
 * everything enough space for the maximum number of values (so as not
 * to have to do repallocs as the ranges grow).
 */
static Ranges *
minmax_multi_init(int maxvalues)
{
	Size				len;
	Ranges *			ranges;

	Assert(maxvalues > 0);

	len = offsetof(Ranges, values);	/* fixed header */
	len += maxvalues * sizeof(Datum); /* Datum values */

	ranges = (Ranges *) palloc0(len);

	ranges->maxvalues = maxvalues;

	return ranges;
}

/*
 * range_serialize
 *	  Serialize the in-memory representation into a compact varlena value.
 *
 * Simply copy the header and then also the individual values, as stored
 * in the in-memory value array.
 */
static SerializedRanges *
range_serialize(Ranges *range, AttrNumber attno, Form_pg_attribute attr)
{
	Size	len;
	int		nvalues;
	SerializedRanges *serialized;

	int		i;
	char   *ptr;

	/* simple sanity checks */
	Assert(range->nranges >= 0);
	Assert(range->nvalues >= 0);
	Assert(range->maxvalues > 0);

	/* see how many Datum values we actually have */
	nvalues = 2*range->nranges + range->nvalues;

	Assert(2*range->nranges + range->nvalues <= range->maxvalues);

	/* header is always needed */
	len = offsetof(SerializedRanges,data);

	/*
	 * The space needed depends on data type - for fixed-length data types
	 * (by-value and some by-reference) it's pretty simple, just multiply
	 * (attlen * nvalues) and we're done. For variable-length by-reference
	 * types we need to actually walk all the values and sum the lengths.
	 */
	if (attr->attlen == -1)	/* varlena */
	{
		int i;
		for (i = 0; i < nvalues; i++)
		{
			len += VARSIZE_ANY(range->values[i]);
		}
	}
	else if (attr->attlen == -2)	/* cstring */
	{
		int i;
		for (i = 0; i < nvalues; i++)
		{
			/* don't forget to include the null terminator ;-) */
			len += strlen(DatumGetPointer(range->values[i])) + 1;
		}
	}
	else /* fixed-length types (even by-reference) */
	{
		Assert(attr->attlen > 0);
		len += nvalues * attr->attlen;
	}

	/*
	 * Allocate the serialized object, copy the basic information. The
	 * serialized object is a varlena, so update the header.
	 */
	serialized = (SerializedRanges *) palloc0(len);
	SET_VARSIZE(serialized, len);

	serialized->nranges = range->nranges;
	serialized->nvalues = range->nvalues;
	serialized->maxvalues = range->maxvalues;

	/*
	 * And now copy also the boundary values (like the length calculation
	 * this depends on the particular data type).
	 */
	ptr = serialized->data;	/* start of the serialized data */

	for (i = 0; i < nvalues; i++)
	{
		if (attr->attbyval)	/* simple by-value data types */
		{
			memcpy(ptr, &range->values[i], attr->attlen);
			ptr += attr->attlen;
		}
		else if (attr->attlen > 0)	/* fixed-length by-ref types */
		{
			memcpy(ptr, DatumGetPointer(range->values[i]), attr->attlen);
			ptr += attr->attlen;
		}
		else if (attr->attlen == -1)	/* varlena */
		{
			int tmp = VARSIZE_ANY(DatumGetPointer(range->values[i]));
			memcpy(ptr, DatumGetPointer(range->values[i]), tmp);
			ptr += tmp;
		}
		else if (attr->attlen == -2)	/* cstring */
		{
			int tmp = strlen(DatumGetPointer(range->values[i])) + 1;
			memcpy(ptr, DatumGetPointer(range->values[i]), tmp);
			ptr += tmp;
		}

		/* make sure we haven't overflown the buffer end */
		Assert(ptr <= ((char *)serialized + len));
	}

		/* exact size */
	Assert(ptr == ((char *)serialized + len));

	return serialized;
}

/*
 * range_deserialize
 *	  Serialize the in-memory representation into a compact varlena value.
 *
 * Simply copy the header and then also the individual values, as stored
 * in the in-memory value array.
 */
static Ranges *
range_deserialize(SerializedRanges *serialized,
				AttrNumber attno, Form_pg_attribute attr)
{
	int		i,
			nvalues;
	char   *ptr;

	Ranges *range;

	Assert(serialized->nranges >= 0);
	Assert(serialized->nvalues >= 0);
	Assert(serialized->maxvalues > 0);

	nvalues = 2*serialized->nranges + serialized->nvalues;

	Assert(nvalues <= serialized->maxvalues);

	range = minmax_multi_init(serialized->maxvalues);

	/* copy the header info */
	range->nranges = serialized->nranges;
	range->nvalues = serialized->nvalues;
	range->maxvalues = serialized->maxvalues;

	/*
	 * And now deconstruct the values into Datum array. We don't need
	 * to copy the values and will instead just point the values to the
	 * serialized varlena value (assuming it will be kept around).
	 */
	ptr = serialized->data;

	for (i = 0; i < nvalues; i++)
	{
		if (attr->attbyval)	/* simple by-value data types */
		{
			memcpy(&range->values[i], ptr, attr->attlen);
			ptr += attr->attlen;
		}
		else if (attr->attlen > 0)	/* fixed-length by-ref types */
		{
			/* no copy, just set the value to the pointer */
			range->values[i] = PointerGetDatum(ptr);
			ptr += attr->attlen;
		}
		else if (attr->attlen == -1)	/* varlena */
		{
			range->values[i] = PointerGetDatum(ptr);
			ptr += VARSIZE_ANY(DatumGetPointer(range->values[i]));
		}
		else if (attr->attlen == -2)	/* cstring */
		{
			range->values[i] = PointerGetDatum(ptr);
			ptr += strlen(DatumGetPointer(range->values[i])) + 1;
		}

		/* make sure we haven't overflown the buffer end */
		Assert(ptr <= ((char *)serialized + VARSIZE_ANY(serialized)));
	}

	/* should have consumed the whole input value exactly */
	Assert(ptr == ((char *)serialized + VARSIZE_ANY(serialized)));

	/* return the deserialized value */
	return range;
}

typedef struct compare_context
{
	FmgrInfo   *cmpFn;
	Oid			colloid;
} compare_context;

/*
 * Used to represent ranges expanded during merging and combining (to
 * reduce number of boundary values to store).
 *
 * XXX CombineRange name seems a bit weird. Consider renaming, perhaps to
 * something ExpandedRange or so.
 */
typedef struct CombineRange
{
	Datum	minval;		/* lower boundary */
	Datum	maxval;		/* upper boundary */
	bool	collapsed;	/* true if minval==maxval */
} CombineRange;

/*
 * compare_combine_ranges
 *	  Compare the combine ranges - first by minimum, then by maximum.
 *
 * We do guarantee that ranges in a single Range object do not overlap,
 * so it may seem strange that we don't order just by minimum. But when
 * merging two Ranges (which happens in the union function), the ranges
 * may in fact overlap. So we do compare both.
 */
static int
compare_combine_ranges(const void *a, const void *b, void *arg)
{
	CombineRange *ra = (CombineRange *)a;
	CombineRange *rb = (CombineRange *)b;
	Datum r;

	compare_context *cxt = (compare_context *)arg;

	/* first compare minvals */
	r = FunctionCall2Coll(cxt->cmpFn, cxt->colloid, ra->minval, rb->minval);

	if (DatumGetBool(r))
		return -1;

	r = FunctionCall2Coll(cxt->cmpFn, cxt->colloid, rb->minval, ra->minval);

	if (DatumGetBool(r))
		return 1;

	/* then compare maxvals */
	r = FunctionCall2Coll(cxt->cmpFn, cxt->colloid, ra->maxval, rb->maxval);

	if (DatumGetBool(r))
		return -1;

	r = FunctionCall2Coll(cxt->cmpFn, cxt->colloid, rb->maxval, ra->maxval);

	if (DatumGetBool(r))
		return 1;

	return 0;
}

/*
 * range_contains_value
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
range_contains_value(BrinDesc *bdesc, Oid colloid,
							AttrNumber attno, Form_pg_attribute attr,
							Ranges *ranges, Datum newval)
{
	int			i;
	FmgrInfo   *cmpFn;
	Oid			typid = attr->atttypid;

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
		 *
		 * XXX We simply walk the ranges sequentially, but maybe we could
		 * further leverage the ordering and non-overlap and use bsearch to
		 * speed this up a bit.
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
	 * We're done with the ranges, now let's inspect the exact values.
	 *
	 * XXX Again, we do sequentially search the values - consider leveraging
	 * the ordering of values to improve performance.
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
 * insert_value
 *	  Adds a new value into the single-point part, while maintaining ordering.
 *
 * The function inserts the new value to the right place in the single-point
 * part of the range. It assumes there's enough free space, and then does
 * essentially an insert-sort.
 *
 * XXX Assumes the 'values' array has space for (nvalues+1) entries, and that
 * only the first nvalues are used.
 */
static void
insert_value(FmgrInfo *cmp, Oid colloid, Datum *values, int nvalues,
			 Datum newvalue)
{
	int	i;
	Datum	lt;

	/* If there are no values yet, store the new one and we're done. */
	if (!nvalues)
	{
		values[0] = newvalue;
		return;
	}

	/*
	 * A common case is that the new value is entirely out of the existing
	 * range, i.e. it's either smaller or larger than all previous values.
	 * So we check and handle this case first - first we check the larger
	 * case, because in that case we can just append the value to the end
	 * of the array and we're done.
	 */

	/* Is it greater than all existing values in the array? */
	lt = FunctionCall2Coll(cmp, colloid, values[nvalues-1], newvalue);
	if (DatumGetBool(lt))
	{
		/* just copy it in-place and we're done */
		values[nvalues] = newvalue;
		return;
	}

	/*
	 * OK, I lied a bit - we won't check the smaller case explicitly, but
	 * we'll just compare the value to all existing values in the array.
	 * But we happen to start with the smallest value, so we're actually
	 * doing the check anyway.
	 *
	 * XXX We do walk the values sequentially. Perhaps we could/should be
	 * smarter and do some sort of bisection, to improve performance?
	 */
	for (i = 0; i < nvalues; i++)
	{
		lt = FunctionCall2Coll(cmp, colloid, newvalue, values[i]);
		if (DatumGetBool(lt))
		{
			/*
			 * Move values to make space for the new entry, which should go
			 * to index 'i'. Entries 0 ... (i-1) should stay where they are.
			 */
			memmove(&values[i+1], &values[i], (nvalues-i) * sizeof(Datum));
			values[i] = newvalue;
			return;
		}
	}

	/* We should never really get here. */
	Assert(false);
}

/*
 * Check that the order of the array values is correct, using the cmp
 * function (which should be BTLessStrategyNumber).
 */
static void
AssertArrayOrder(FmgrInfo *cmp, Oid colloid, Datum *values, int nvalues)
{
#ifdef USE_ASSERT_CHECKING
	int	i;
	Datum lt;

	for (i = 0; i < (nvalues-1); i++)
	{
		lt = FunctionCall2Coll(cmp, colloid, values[i], values[i+1]);
		Assert(DatumGetBool(lt));
	}
#endif
}

/*
 * Check ordering of boundary values in both parts of the ranges, using
 * the cmp function (which should be BTLessStrategyNumber).
 *
 * XXX We might also check that the ranges and points do not overlap. But
 * that will break this check sooner or later anyway.
 */
static void
AssertRangeOrdering(FmgrInfo *cmpFn, Oid colloid, Ranges *ranges)
{
#ifdef USE_ASSERT_CHECKING
	/* first the ranges (there are 2*nranges boundary values) */
	AssertArrayOrder(cmpFn, colloid, ranges->values, 2*ranges->nranges);

	/* then the single-point ranges (with nvalues boundar values ) */
	AssertArrayOrder(cmpFn, colloid, &ranges->values[2*ranges->nranges],
					 ranges->nvalues);
#endif
}

/*
 * Check that the expanded ranges (built when reducing the number of ranges
 * by combining some of them) are correctly sorted and do not overlap.
 */
static void
AssertValidCombineRanges(BrinDesc *bdesc, Oid colloid, AttrNumber attno,
						 Form_pg_attribute attr, CombineRange *ranges,
						 int nranges)
{
#ifdef USE_ASSERT_CHECKING
	int i;
	FmgrInfo *eq;
	FmgrInfo *lt;

	eq = minmax_multi_get_strategy_procinfo(bdesc, attno, attr->atttypid,
											BTEqualStrategyNumber);

	lt = minmax_multi_get_strategy_procinfo(bdesc, attno, attr->atttypid,
											BTLessStrategyNumber);

	/*
	 * Each range independently should be valid, i.e. that for the boundary
	 * values (lower <= upper).
	 */
	for (i = 0; i < nranges; i++)
	{
		Datum r;
		Datum minval = ranges[i].minval;
		Datum maxval = ranges[i].maxval;

		if (ranges[i].collapsed)	/* collapsed: minval == maxval */
			r = FunctionCall2Coll(eq, colloid, minval, maxval);
		else						/* non-collapsed: minval < maxval */
			r = FunctionCall2Coll(lt, colloid, minval, maxval);

		Assert(DatumGetBool(r));
	}

	/*
	 * And the ranges should be ordered and must nor overlap, i.e.
	 * upper < lower for boundaries of consecutive ranges.
	 */
	for (i = 0; i < nranges-1; i++)
	{
		Datum r;
		Datum maxval = ranges[i].maxval;
		Datum minval = ranges[i+1].minval;

		r = FunctionCall2Coll(lt, colloid, maxval, minval);

		Assert(DatumGetBool(r));
	}
#endif
}

/*
 * Expand ranges from Ranges into CombineRange array. This expects the
 * cranges to be pre-allocated and sufficiently large (there needs to be
 * at least (nranges + nvalues) slots).
 */
static void
fill_combine_ranges(CombineRange *cranges, int ncranges, Ranges *ranges)
{
	int idx;
	int i;

	idx = 0;
	for (i = 0; i < ranges->nranges; i++)
	{
		cranges[idx].minval = ranges->values[2*i];
		cranges[idx].maxval = ranges->values[2*i+1];
		cranges[idx].collapsed = false;
		idx++;

		Assert(idx <= ncranges);
	}

	for (i = 0; i < ranges->nvalues; i++)
	{
		cranges[idx].minval = ranges->values[2*ranges->nranges + i];
		cranges[idx].maxval = ranges->values[2*ranges->nranges + i];
		cranges[idx].collapsed = true;
		idx++;

		Assert(idx <= ncranges);
	}

	Assert(idx == ncranges);

	return;
}

/*
 * Sort combine ranges using qsort (with BTLessStrategyNumber function).
 */
static void
sort_combine_ranges(FmgrInfo *cmp, Oid colloid,
					CombineRange *cranges, int ncranges)
{
	compare_context cxt;

	/* sort the values */
	cxt.colloid = colloid;
	cxt.cmpFn = cmp;

	qsort_arg(cranges, ncranges, sizeof(CombineRange),
			  compare_combine_ranges, (void *) &cxt);
}

/*
 * When combining multiple Range values (in union function), some of the
 * ranges may overlap. We simply merge the overlapping ranges to fix that.
 *
 * XXX This assumes the combine ranges were previously sorted (by minval
 * and then maxval). We leverage this when detecting overlap.
 */
static int
merge_combine_ranges(FmgrInfo *cmp, Oid colloid,
					 CombineRange *cranges, int ncranges)
{
	int		idx;

	/* TODO: add assert checking the ordering of input ranges */

	/* try merging ranges (idx) and (idx+1) if they overlap */
	idx = 0;
	while (idx < (ncranges-1))
	{
		Datum	r;

		/*
		 * comparing [?,maxval] vs. [minval,?] - the ranges overlap
		 * if (minval < maxval)
		 */
		r = FunctionCall2Coll(cmp, colloid,
							  cranges[idx].maxval,
							  cranges[idx+1].minval);

		/*
		 * Nope, maxval < minval, so no overlap. And we know the ranges
		 * are ordered, so there are no more overlaps, because all the
		 * remaining ranges have greater or equal minval.
		 */
		if (DatumGetBool(r))
		{
			/* proceed to the next range */
			idx += 1;
			continue;
		}

		/*
		 * So ranges 'idx' and 'idx+1' do overlap, but we don't know if
		 * 'idx+1' is contained in 'idx', or if they only overlap only
		 * partially. So compare the upper bounds and keep the larger one.
		 */
		r = FunctionCall2Coll(cmp, colloid,
							  cranges[idx].maxval,
							  cranges[idx+1].maxval);

		if (DatumGetBool(r))
			cranges[idx].maxval = cranges[idx+1].maxval;

		/*
		 * The range certainly is no longer collapsed (irrespectedly of
		 * the previous state).
		 */
		cranges[idx].collapsed = false;

		/*
		 * Now get rid of the (idx+1) range entirely by shifting the
		 * remaining ranges by 1. There are ncranges elements, and we
		 * need to move elements from (idx+2). That means the number
		 * of elements to move is [ncranges - (idx+2)].
		 */
		memmove(&cranges[idx+1], &cranges[idx+2],
				(ncranges - (idx + 2)) * sizeof(CombineRange));

		/*
		 * Decrease the number of ranges, and repeat (with the same range,
		 * as it might overlap with additional ranges thanks to the merge).
		 */
		ncranges--;
	}

	/* TODO: add assert checking the ordering etc. of produced ranges */

	return ncranges;
}

/*
 * Represents a distance between two ranges (identified by index into
 * an array of combine ranges).
 */
typedef struct DistanceValue
{
	int		index;
	double	value;
} DistanceValue;

/*
 * Simple comparator for distance values, comparing the double value.
 */
static int
compare_distances(const void *a, const void *b)
{
	DistanceValue *da = (DistanceValue *)a;
	DistanceValue *db = (DistanceValue *)b;

	if (da->value < db->value)
		return -1;
	else if (da->value > db->value)
		return 1;

	return 0;
}

/*
 * Given an array of combine ranges, compute distance of the gaps betwen
 * the ranges - for ncranges there are (ncranges-1) gaps.
 *
 * We simply call the "distance" function to compute the (min-max) for pairs
 * of consecutive ganges. The function may be fairly expensive, so we do that
 * just once (and then use it to pick as many ranges to merge as possible).
 *
 * See reduce_combine_ranges for details.
 */
static DistanceValue *
build_distances(FmgrInfo *distanceFn, Oid colloid,
				CombineRange *cranges, int ncranges)
{
	int i;
	DistanceValue *distances;

	Assert(ncranges >= 2);

	distances = (DistanceValue *) palloc0(sizeof(DistanceValue) * (ncranges - 1));

	/*
	 * Walk though the ranges once and compute distance between the ranges
	 * so that we can sort them once.
	 */
	for (i = 0; i < (ncranges-1); i++)
	{
		Datum a1, a2, r;

		a1 = cranges[i].maxval;
		a2 = cranges[i+1].minval;

		/* compute length of the empty gap (distance between max/min) */
		r = FunctionCall2Coll(distanceFn, colloid, a1, a2);

		/* remember the index */
		distances[i].index = i;
		distances[i].value = DatumGetFloat8(r);
	}

	/* sort the distances in ascending order */
	pg_qsort(distances, (ncranges-1), sizeof(DistanceValue), compare_distances);

	return distances;
}

/*
 * Builds combine ranges for the existing ranges (and single-point ranges),
 * and also the new value (which did not fit into the array).
 *
 * XXX We do perform qsort on all the values, but we could also leverage
 * the fact that the input data is already sorted and do merge sort.
 */
static CombineRange *
build_combine_ranges(FmgrInfo *cmp, Oid colloid, Ranges *ranges,
					 Datum newvalue, int *nranges)
{
	int				ncranges;
	CombineRange   *cranges;

	/* now do the actual merge sort */
	ncranges = ranges->nranges + ranges->nvalues + 1;
	cranges = (CombineRange *) palloc0(ncranges * sizeof(CombineRange));
	*nranges = ncranges;

	/* put the new value at the beginning */
	cranges[0].minval = newvalue;
	cranges[0].maxval = newvalue;
	cranges[0].collapsed = true;

	/* then the regular and collapsed ranges */
	fill_combine_ranges(&cranges[1], ncranges-1, ranges);

	/* and sort the ranges */
	sort_combine_ranges(cmp, colloid, cranges, ncranges);

	return cranges;
}

/*
 * Counts bondary values needed to store the ranges. Each single-point
 * range is stored using a single value, each regular range needs two.
 */
static int
count_values(CombineRange *cranges, int ncranges)
{
	int	i;
	int	count;

	count = 0;
	for (i = 0; i < ncranges; i++)
	{
		if (cranges[i].collapsed)
			count += 1;
		else
			count += 2;
	}

	return count;
}

/*
 * Combines ranges until the number of boundary values drops below 75%
 * of the capacity (as set by values_per_range reloption).
 *
 * XXX The ranges to merge are selected solely using the distance. But
 * that may not be the best strategy, for example when multiple gaps
 * are of equal (or very similar) length.
 *
 * Consider for example points 1, 2, 3, .., 64, which have gaps of the
 * same length 1 of course. In that case we tend to pick the first
 * gap of that length, which leads to this:
 *
 *    step 1:  [1, 2], 3, 4, 5, .., 64
 *    step 2:  [1, 3], 4, 5,    .., 64
 *    step 3:  [1, 4], 5,       .., 64
 *    ...
 *
 * So in the end we'll have one "large" range and multiple small points.
 * That may be fine, but it seems a bit strange and non-optimal. Maybe
 * we should consider other things when picking ranges to merge - e.g.
 * length of the ranges? Or perhaps randomize the choice of ranges, with
 * probability inversely proportional to the distance (the gap lengths
 * may be very close, but not exactly the same).
 */
static int
reduce_combine_ranges(CombineRange *cranges, int ncranges,
					  DistanceValue *distances, int max_values)
{
	int i;
	int	ndistances = (ncranges - 1);

	/*
	 * We have one fewer 'gaps' than the ranges. We'll be decrementing
	 * the number of combine ranges (reduction is the primary goal of
	 * this function), so we must use a separate value.
	 */
	for (i = 0; i < ndistances; i++)
	{
		int j;
		int shortest;

		if (count_values(cranges, ncranges) <= max_values * 0.75)
			break;

		shortest = distances[i].index;

		/*
		 * The index must be still valid with respect to the current size
		 * of cranges array (and it always points to the first range, so
		 * never to the last one - hence the -1 in the condition).
		 */
		Assert(shortest < (ncranges - 1));

		/*
		 * Move the values to join the two selected ranges. The new range is
		 * definiely not collapsed but a regular range.
		 */
		cranges[shortest].maxval = cranges[shortest+1].maxval;
		cranges[shortest].collapsed = false;

		/* shuffle the subsequent combine ranges */
		memmove(&cranges[shortest+1], &cranges[shortest+2],
				(ncranges - shortest - 2) * sizeof(CombineRange));

		/* also, shuffle all higher indexes (we've just moved the ranges) */
		for (j = i; j < ndistances; j++)
		{
			if (distances[j].index > shortest)
				distances[j].index--;
		}

		ncranges--;

		Assert(ncranges > 0);
	}

	return ncranges;
}

/*
 * Store the boundary values from CombineRanges back into Range (using
 * only the minimal number of values needed).
 */
static void
store_combine_ranges(Ranges *ranges, CombineRange *cranges, int ncranges)
{
	int	i;
	int	idx = 0;

	/* first copy in the regular ranges */
	ranges->nranges = 0;
	for (i = 0; i < ncranges; i++)
	{
		if (!cranges[i].collapsed)
		{
			ranges->values[idx++] = cranges[i].minval;
			ranges->values[idx++] = cranges[i].maxval;
			ranges->nranges++;
		}
	}

	/* now copy in the collapsed ones */
	ranges->nvalues = 0;
	for (i = 0; i < ncranges; i++)
	{
		if (cranges[i].collapsed)
		{
			ranges->values[idx++] = cranges[i].minval;
			ranges->nvalues++;
		}
	}
}

/*
 * range_add_value
 * 		Add the new value to the multi-minmax range.
 */
static bool
range_add_value(BrinDesc *bdesc, Oid colloid,
				AttrNumber attno, Form_pg_attribute attr,
				Ranges *ranges, Datum newval)
{
	FmgrInfo   *cmpFn,
			   *distanceFn;

	/* combine ranges */
	CombineRange   *cranges;
	int				ncranges;
	DistanceValue  *distances;

	MemoryContext	ctx;
	MemoryContext	oldctx;

	Assert(2*ranges->nranges + ranges->nvalues <= ranges->maxvalues);

	/*
	 * Bail out if the value already is covered by the range.
	 *
	 * We could also add values until we hit values_per_range, and then
	 * do the deduplication in a batch, hoping for better efficiency. But
	 * that would mean we actually modify the range every time, which means
	 * having to serialize the value, which does palloc, walks the values,
	 * copies them, etc. Not exactly cheap.
	 *
	 * So instead we do the check, which should be fairly cheap - assuming
	 * the comparator function is not very expensive.
	 *
	 * This also implies means the values array can't contain duplicities.
	 */
	if (range_contains_value(bdesc, colloid, attno, attr, ranges, newval))
		return false;

	/*
	 * If there's space in the values array, copy it in and we're done.
	 *
	 * We do want to keep the values sorted (to speed up searches), so we
	 * do a simple insertion sort. We could do something more elaborate,
	 * e.g. by sorting the values only now and then, but for small counts
	 * (e.g. when maxvalues is 64) this should be fine.
	 */
	if (2*ranges->nranges + ranges->nvalues < ranges->maxvalues)
	{
		FmgrInfo   *cmpFn;
		Datum	   *values;

		cmpFn = minmax_multi_get_strategy_procinfo(bdesc, attno, attr->atttypid,
												   BTLessStrategyNumber);

		/* beginning of the 'single value' part (for convenience) */
		values = &ranges->values[2*ranges->nranges];

		insert_value(cmpFn, colloid, values, ranges->nvalues, newval);

		ranges->nvalues++;

		/*
		 * Check we haven't broken the ordering of boundary values (checks
		 * both parts, but that doesn't hurt).
		 */
		AssertRangeOrdering(cmpFn, colloid, ranges);

		/* yep, we've modified the range */
		return true;
	}

	/*
	 * Damn - the new value is not in the range yet, but we don't have space
	 * to just insert it. So we need to combine some of the existing ranges,
	 * to reduce the number of values we need to store (joining two intervals
	 * reduces the number of boundaries to store by 2).
	 *
	 * To do that we first construct an array of CombineRange items - each
	 * combine range tracks if it's a regular range or collapsed range, where
	 * "collapsed" means "single point."
	 *
	 * Existing ranges (we have ranges->nranges of them) map to combine ranges
	 * directly, while single points (ranges->nvalues of them) have to be
	 * expanded. We neet the combine ranges to be sorted, and we do that by
	 * performing a merge sort of ranges, values and new value.
	 */

	/* we'll certainly need the comparator, so just look it up now */
	cmpFn = minmax_multi_get_strategy_procinfo(bdesc, attno, attr->atttypid,
											   BTLessStrategyNumber);

	/* Check the ordering invariants are not violated (for both parts). */
	AssertArrayOrder(cmpFn, colloid, ranges->values, ranges->nranges*2);
	AssertArrayOrder(cmpFn, colloid, &ranges->values[ranges->nranges*2],
					 ranges->nvalues);

	/* and we'll also need the 'distance' procedure */
	distanceFn = minmax_multi_get_procinfo(bdesc, attno, PROCNUM_DISTANCE);

	/*
	 * The distanceFn calls (which may internally call e.g. numeric_le) may
	 * allocate quite a bit of memory, and we must not leak it. Otherwise
	 * we'd have problems e.g. when building indexes. So we create a local
	 * memory context and make sure we free the memory before leaving this
	 * function (not after every call).
	 */
	ctx = AllocSetContextCreate(CurrentMemoryContext,
								"minmax-multi context",
								ALLOCSET_DEFAULT_SIZES);

	oldctx = MemoryContextSwitchTo(ctx);

	/* OK build the combine ranges */
	cranges = build_combine_ranges(cmpFn, colloid, ranges, newval, &ncranges);

	/* build array of gap distances and sort them in ascending order */
	distances = build_distances(distanceFn, colloid, cranges, ncranges);

	/*
	 * Combine ranges until we release at least 25% of the space. This
	 * threshold is somewhat arbitrary, perhaps needs tuning. We must not
	 * use too low or high value.
	 */
	ncranges = reduce_combine_ranges(cranges, ncranges, distances,
									 ranges->maxvalues);

	Assert(count_values(cranges, ncranges) <= ranges->maxvalues * 0.75);

	/* decompose the combine ranges into regular ranges and single values */
	store_combine_ranges(ranges, cranges, ncranges);

	MemoryContextSwitchTo(oldctx);
	MemoryContextDelete(ctx);

	/* Check the ordering invariants are not violated (for both parts). */
	AssertArrayOrder(cmpFn, colloid, ranges->values, ranges->nranges*2);
	AssertArrayOrder(cmpFn, colloid, &ranges->values[ranges->nranges*2],
					 ranges->nvalues);

	return true;
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
 * Compute distance between two float4 values (plain subtraction).
 */
Datum
brin_minmax_multi_distance_float4(PG_FUNCTION_ARGS)
{
	float a1 = PG_GETARG_FLOAT4(0);
	float a2 = PG_GETARG_FLOAT4(1);

	/*
	 * We know the values are range boundaries, but the range may be
	 * collapsed (i.e. single points), with equal values.
	 */
	Assert(a1 <= a2);

	PG_RETURN_FLOAT8((double) a2 - (double) a1);
}

/*
 * Compute distance between two float8 values (plain subtraction).
 */
Datum
brin_minmax_multi_distance_float8(PG_FUNCTION_ARGS)
{
	double a1 = PG_GETARG_FLOAT8(0);
	double a2 = PG_GETARG_FLOAT8(1);

	/*
	 * We know the values are range boundaries, but the range may be
	 * collapsed (i.e. single points), with equal values.
	 */
	Assert(a1 <= a2);

	PG_RETURN_FLOAT8(a2 - a1);
}

/*
 * Compute distance between two int2 values (plain subtraction).
 */
Datum
brin_minmax_multi_distance_int2(PG_FUNCTION_ARGS)
{
	int16	a1 = PG_GETARG_INT16(0);
	int16	a2 = PG_GETARG_INT16(1);

	/*
	 * We know the values are range boundaries, but the range may be
	 * collapsed (i.e. single points), with equal values.
	 */
	Assert(a1 <= a2);

	PG_RETURN_FLOAT8((double) a2 - (double) a1);
}

/*
 * Compute distance between two int4 values (plain subtraction).
 */
Datum
brin_minmax_multi_distance_int4(PG_FUNCTION_ARGS)
{
	int32	a1 = PG_GETARG_INT32(0);
	int32	a2 = PG_GETARG_INT32(1);

	/*
	 * We know the values are range boundaries, but the range may be
	 * collapsed (i.e. single points), with equal values.
	 */
	Assert(a1 <= a2);

	PG_RETURN_FLOAT8((double) a2 - (double) a1);
}

/*
 * Compute distance between two int8 values (plain subtraction).
 */
Datum
brin_minmax_multi_distance_int8(PG_FUNCTION_ARGS)
{
	int64	a1 = PG_GETARG_INT64(0);
	int64	a2 = PG_GETARG_INT64(1);

	/*
	 * We know the values are range boundaries, but the range may be
	 * collapsed (i.e. single points), with equal values.
	 */
	Assert(a1 <= a2);

	PG_RETURN_FLOAT8((double) a2 - (double) a1);
}

/*
 * Compute distance between two tid values (by mapping them to float8
 * and then subtracting them).
 */
Datum
brin_minmax_multi_distance_tid(PG_FUNCTION_ARGS)
{
	double	da1,
			da2;

	ItemPointer	pa1 = (ItemPointer) PG_GETARG_DATUM(0);
	ItemPointer	pa2 = (ItemPointer) PG_GETARG_DATUM(1);

	/*
	 * We know the values are range boundaries, but the range may be
	 * collapsed (i.e. single points), with equal values.
	 */
	Assert(ItemPointerCompare(pa1, pa2) <= 0);

	da1 = ItemPointerGetBlockNumber(pa1) * MaxHeapTuplesPerPage +
		  ItemPointerGetOffsetNumber(pa1);

	da2 = ItemPointerGetBlockNumber(pa2) * MaxHeapTuplesPerPage +
		  ItemPointerGetOffsetNumber(pa2);

	PG_RETURN_FLOAT8(da2 - da1);
}

/*
 * Comutes distance between two numeric values (plain subtraction).
 */
Datum
brin_minmax_multi_distance_numeric(PG_FUNCTION_ARGS)
{
	Datum	d;
	Datum	a1 = PG_GETARG_DATUM(0);
	Datum	a2 = PG_GETARG_DATUM(1);

	/*
	 * We know the values are range boundaries, but the range may be
	 * collapsed (i.e. single points), with equal values.
	 */
	Assert(DatumGetBool(DirectFunctionCall2(numeric_le, a1, a2)));

	d = DirectFunctionCall2(numeric_sub, a2, a1);	/* a2 - a1 */

	PG_RETURN_FLOAT8(DirectFunctionCall1(numeric_float8, d));
}

/*
 * Computes approximate distance between two UUID values.
 *
 * XXX We do not need a perfectly accurate value, so we approximate the
 * deltas (which would have to be 128-bit integers) with a 64-bit float.
 * The small inaccuracies do not matter in practice, in the worst case
 * we'll decide to merge ranges that are not the closest ones.
 */
Datum
brin_minmax_multi_distance_uuid(PG_FUNCTION_ARGS)
{
	int		i;
	double	delta = 0;

	Datum	a1 = PG_GETARG_DATUM(0);
	Datum	a2 = PG_GETARG_DATUM(1);

	pg_uuid_t  *u1 = DatumGetUUIDP(a1);
	pg_uuid_t  *u2 = DatumGetUUIDP(a2);

	/*
	 * We know the values are range boundaries, but the range may be
	 * collapsed (i.e. single points), with equal values.
	 */
	Assert(DatumGetBool(DirectFunctionCall2(uuid_le, a1, a2)));

	/* compute approximate delta as a double precision value */
	for (i = UUID_LEN-1; i >= 0; i--)
	{
		delta += (int) u2->data[i] - (int) u1->data[i];
		delta /= 256;
	}

	Assert(delta >= 0);

	PG_RETURN_FLOAT8(delta);
}

/*
 * Computes approximate distance between two time (without tz) values.
 *
 * TimeADT is just an int64, so we simply subtract the values directly.
 */
Datum
brin_minmax_multi_distance_time(PG_FUNCTION_ARGS)
{
	double	delta = 0;

	TimeADT	ta = PG_GETARG_TIMEADT(0);
	TimeADT	tb = PG_GETARG_TIMEADT(1);

	delta = (tb - ta);

	Assert(delta >= 0);

	PG_RETURN_FLOAT8(delta);
}

/*
 * Computes approximate distance between two timetz values.
 *
 * Simply subtracts the TimeADT (int64) values embedded in TimeTzADT.
 *
 * XXX Does this need to consider the time zones?
 */
Datum
brin_minmax_multi_distance_timetz(PG_FUNCTION_ARGS)
{
	double	delta = 0;

	TimeTzADT  *ta = PG_GETARG_TIMETZADT_P(0);
	TimeTzADT  *tb = PG_GETARG_TIMETZADT_P(1);

	delta = tb->time - ta->time;

	Assert(delta >= 0);

	PG_RETURN_FLOAT8(delta);
}

/*
 * Computes distance between two interval values.
 *
 * Intervals are internally just 'time' values, so use the same approach
 * as for in brin_minmax_multi_distance_time.
 *
 * XXX Do we actually need two separate functions, then?
 */
Datum
brin_minmax_multi_distance_interval(PG_FUNCTION_ARGS)
{
	double	delta = 0;

	TimeADT		ia = PG_GETARG_TIMEADT(0);
	TimeADT		ib = PG_GETARG_TIMEADT(1);

	delta = (ib - ia);

	Assert(delta >= 0);

	PG_RETURN_FLOAT8(delta);
}

/*
 * Compute distance between two pg_lsn values.
 *
 * LSN is just an int64 encoding position in the stream, so just subtract
 * those int64 values directly.
 */
Datum
brin_minmax_multi_distance_pg_lsn(PG_FUNCTION_ARGS)
{
	double	delta = 0;

	XLogRecPtr	lsna = PG_GETARG_LSN(0);
	XLogRecPtr	lsnb = PG_GETARG_LSN(1);

	delta = (lsnb - lsna);

	Assert(delta >= 0);

	PG_RETURN_FLOAT8(delta);
}

/*
 * Compute distance between two macaddr values.
 *
 * mac addresses are treated as 6 unsigned chars, so do the same thing we
 * already do for UUID values.
 */
Datum
brin_minmax_multi_distance_macaddr(PG_FUNCTION_ARGS)
{
	double	delta;

	macaddr    *a = PG_GETARG_MACADDR_P(0);
	macaddr    *b = PG_GETARG_MACADDR_P(1);

	delta = ((double)b->f - (double)a->f);
	delta /= 256;

	delta += ((double)b->e - (double)a->e);
	delta /= 256;

	delta += ((double)b->d - (double)a->d);
	delta /= 256;

	delta += ((double)b->c - (double)a->c);
	delta /= 256;

	delta += ((double)b->b - (double)a->b);
	delta /= 256;

	delta += ((double)b->a - (double)a->a);
	delta /= 256;

	Assert(delta >= 0);

	PG_RETURN_FLOAT8(delta);
}

/*
 * Compute distance between two macaddr8 values.
 *
 * macaddr8 addresses are 8 unsigned chars, so do the same thing we
 * already do for UUID values.
 */
Datum
brin_minmax_multi_distance_macaddr8(PG_FUNCTION_ARGS)
{
	double	delta;

	macaddr8   *a = PG_GETARG_MACADDR8_P(0);
	macaddr8   *b = PG_GETARG_MACADDR8_P(1);

	delta = ((double)b->h - (double)a->h);
	delta /= 256;

	delta += ((double)b->g - (double)a->g);
	delta /= 256;

	delta += ((double)b->f - (double)a->f);
	delta /= 256;

	delta += ((double)b->e - (double)a->e);
	delta /= 256;

	delta += ((double)b->d - (double)a->d);
	delta /= 256;

	delta += ((double)b->c - (double)a->c);
	delta /= 256;

	delta += ((double)b->b - (double)a->b);
	delta /= 256;

	delta += ((double)b->a - (double)a->a);
	delta /= 256;

	Assert(delta >= 0);

	PG_RETURN_FLOAT8(delta);
}

/*
 * Compute distance between two inet values.
 *
 * The distance is defined as difference between 32-bit/128-bit values,
 * depending on the IP version. The distance is computed by subtracting
 * the bytes and normalizing it to [0,1] range for each IP family.
 * Addresses from difference families are consider to be in maximum
 * distance, which is 1.0.
 *
 * XXX Does this need to consider the mask (bits)? For now it's ignored.
 */
Datum
brin_minmax_multi_distance_inet(PG_FUNCTION_ARGS)
{
	double			delta;
	int				i;
	int				len;
	unsigned char  *addra,
				   *addrb;

	inet	   *ipa = PG_GETARG_INET_PP(0);
	inet	   *ipb = PG_GETARG_INET_PP(1);

	/*
	 * If the addresses are from different families, consider them to be
	 * in maximal possible distance (which is 1.0).
	 */
	if (ip_family(ipa) != ip_family(ipb))
		return 1.0;

	/* ipv4 or ipv6 */
	if (ip_family(ipa) == PGSQL_AF_INET)
		len = 4;
	else
		len = 16; /* NS_IN6ADDRSZ */

	addra = ip_addr(ipa);
	addrb = ip_addr(ipb);

	delta = 0;
	for (i = len-1; i >= 0; i--)
	{
		delta += (double)addrb[i] - (double)addra[i];
		delta /= 256;
	}

	Assert((delta >= 0) && (delta <= 1));

	PG_RETURN_FLOAT8(delta);
}

static int
brin_minmax_multi_get_values(BrinDesc *bdesc)
{
	return BrinGetValuesPerRange(bdesc->bd_index);
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
	bool		modified = false;
	Form_pg_attribute attr;
	AttrNumber	attno;
	Ranges *ranges;
	SerializedRanges *serialized = NULL;

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
		ranges = minmax_multi_init(brin_minmax_multi_get_values(bdesc));
		column->bv_allnulls = false;
		modified = true;
	}
	else
	{
		serialized = (SerializedRanges *) PG_DETOAST_DATUM(column->bv_values[0]);
		ranges = range_deserialize(serialized, attno, attr);
	}

	/*
	 * Try to add the new value to the range. We need to update the modified
	 * flag, so that we serialize the correct value.
	 */
	modified |= range_add_value(bdesc, colloid, attno, attr, ranges, newval);

	if (modified)
	{
		SerializedRanges *s = range_serialize(ranges, attno, attr);
		column->bv_values[0] = PointerGetDatum(s);

		/*
		 * XXX pfree must happen after range_serialize, because the Ranges value
		 * may reference the original serialized value.
		 */
		if (serialized)
			pfree(serialized);
	}

	pfree(ranges);

	PG_RETURN_BOOL(modified);
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
	SerializedRanges *serialized;
	Ranges		*ranges;
	int			keyno;
	int			rangeno;
	int			i;
	Form_pg_attribute attr;

	attno = column->bv_attno;
	attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);

	serialized = (SerializedRanges *) PG_DETOAST_DATUM(column->bv_values[0]);
	ranges = range_deserialize(serialized, attno, attr);

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
					matches = FunctionCall2Coll(finfo, colloid, val, value);
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
	SerializedRanges   *serialized_a;
	SerializedRanges   *serialized_b;
	Ranges	   *ranges_a;
	Ranges	   *ranges_b;
	AttrNumber	attno;
	Form_pg_attribute attr;
	CombineRange *cranges;
	int			ncranges;
	FmgrInfo   *cmpFn,
			   *distanceFn;
	DistanceValue  *distances;
	MemoryContext	ctx;
	MemoryContext	oldctx;

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

	serialized_a = (SerializedRanges *) PG_DETOAST_DATUM(col_a->bv_values[0]);
	serialized_b = (SerializedRanges *) PG_DETOAST_DATUM(col_b->bv_values[0]);

	ranges_a = range_deserialize(serialized_a, attno, attr);
	ranges_b = range_deserialize(serialized_b, attno, attr);

	/* make sure neither of the ranges is NULL */
	Assert(ranges_a && ranges_b);

	ncranges = (ranges_a->nranges + ranges_a->nvalues) +
			   (ranges_b->nranges + ranges_b->nvalues);

	/*
	 * The distanceFn calls (which may internally call e.g. numeric_le) may
	 * allocate quite a bit of memory, and we must not leak it. Otherwise
	 * we'd have problems e.g. when building indexes. So we create a local
	 * memory context and make sure we free the memory before leaving this
	 * function (not after every call).
	 */
	ctx = AllocSetContextCreate(CurrentMemoryContext,
								"minmax-multi context",
								ALLOCSET_DEFAULT_SIZES);

	oldctx = MemoryContextSwitchTo(ctx);

	/* allocate and fill */
	cranges = (CombineRange *)palloc0(ncranges * sizeof(CombineRange));

	/* fill the combine ranges with entries for the first range */
	fill_combine_ranges(cranges, ranges_a->nranges + ranges_a->nvalues,
						ranges_a);

	/* and now add combine ranges for the second range */
	fill_combine_ranges(&cranges[ranges_a->nranges + ranges_a->nvalues],
						ranges_b->nranges + ranges_b->nvalues,
						ranges_b);

	cmpFn = minmax_multi_get_strategy_procinfo(bdesc, attno, attr->atttypid,
											 BTLessStrategyNumber);

	/* sort the combine ranges */
	sort_combine_ranges(cmpFn, colloid, cranges, ncranges);

	/*
	 * We've merged two different lists of ranges, so some of them may be
	 * overlapping. So walk through them and merge them.
	 */
	ncranges = merge_combine_ranges(cmpFn, colloid, cranges, ncranges);

	/* check that the combine ranges are correct (no overlaps, ordering) */
	AssertValidCombineRanges(bdesc, colloid, attno, attr, cranges, ncranges);

	/* build array of gap distances and sort them in ascending order */
	distanceFn = minmax_multi_get_procinfo(bdesc, attno, PROCNUM_DISTANCE);
	distances = build_distances(distanceFn, colloid, cranges, ncranges);

	/*
	 * See how many values would be needed to store the current ranges, and if
	 * needed combine as many off them to get below the maxvalues threshold.
	 * The collapsed ranges will be stored as a single value.
	 *
	 * XXX The maxvalues may be different, so perhaps use Max?
	 */
	ncranges = reduce_combine_ranges(cranges, ncranges, distances,
									 ranges_a->maxvalues);

	/* update the first range summary */
	store_combine_ranges(ranges_a, cranges, ncranges);

	MemoryContextSwitchTo(oldctx);
	MemoryContextDelete(ctx);

	/* cleanup and update the serialized value */
	pfree(serialized_a);
	col_a->bv_values[0] = PointerGetDatum(range_serialize(ranges_a, attno, attr));

	PG_RETURN_VOID();
}

/*
 * Cache and return minmax multi opclass support procedure
 *
 * Return the procedure corresponding to the given function support number
 * or null if it is not exists.
 */
static FmgrInfo *
minmax_multi_get_procinfo(BrinDesc *bdesc, uint16 attno, uint16 procnum)
{
	MinmaxMultiOpaque *opaque;
	uint16		basenum = procnum - PROCNUM_BASE;

	/*
	 * We cache these in the opaque struct, to avoid repetitive syscache
	 * lookups.
	 */
	opaque = (MinmaxMultiOpaque *) bdesc->bd_info[attno - 1]->oi_opaque;

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

/*
 * Cache and return the procedure for the given strategy.
 *
 * Note: this function mirrors minmax_multi_get_strategy_procinfo; see notes
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
