/*-------------------------------------------------------------------------
 *
 * mcv.c
 *	  POSTGRES multivariate MCV lists
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/statistics/mcv.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/htup_details.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_statistic_ext.h"
#include "fmgr.h"
#include "funcapi.h"
#include "optimizer/clauses.h"
#include "statistics/extended_stats_internal.h"
#include "statistics/statistics.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

/*
 * Computes size of a serialized MCV item, depending on the number of
 * dimensions (columns) the statistic is defined on. The datum values are
 * stored in a separate array (deduplicated, to minimize the size), and
 * so the serialized items only store uint16 indexes into that array.
 *
 * Each serialized item store (in this order):
 *
 * - indexes to values	  (ndim * sizeof(uint16))
 * - null flags			  (ndim * sizeof(bool))
 * - frequency			  (sizeof(double))
 * - base_frequency		  (sizeof(double))
 *
 * So in total each MCV item requires this many bytes:
 *
 *	 ndim * (sizeof(uint16) + sizeof(bool)) + 2 * sizeof(double)
 */
#define ITEM_SIZE(ndims)	\
	((ndims) * (sizeof(uint16) + sizeof(bool)) + 2 * sizeof(double))

/*
 * Macros for convenient access to parts of a serialized MCV item.
 */
#define ITEM_INDEXES(item)			((uint16*)item)
#define ITEM_NULLS(item,ndims)		((bool*)(ITEM_INDEXES(item) + ndims))
#define ITEM_FREQUENCY(item,ndims)	((double*)(ITEM_NULLS(item,ndims) + ndims))
#define ITEM_BASE_FREQUENCY(item,ndims)	((double*)(ITEM_FREQUENCY(item,ndims) + 1))


static MultiSortSupport build_mss(VacAttrStats **stats, Bitmapset *attrs);

static SortItem *build_distinct_groups(int numrows, SortItem *items,
					  MultiSortSupport mss, int *ndistinct);

static int count_distinct_groups(int numrows, SortItem *items,
					  MultiSortSupport mss);

/*
 * get_mincount_for_mcv_list
 * 		Determine the minimum number of times a value needs to appear in
 * 		the sample for it to be included in the MCV list.
 *
 * We want to keep only values that appear sufficiently often in the
 * sample that it is reasonable to extrapolate their sample frequencies to
 * the entire table.  We do this by placing an upper bound on the relative
 * standard error of the sample frequency, so that any estimates the
 * planner generates from the MCV statistics can be expected to be
 * reasonably accurate.
 *
 * Since we are sampling without replacement, the sample frequency of a
 * particular value is described by a hypergeometric distribution.  A
 * common rule of thumb when estimating errors in this situation is to
 * require at least 10 instances of the value in the sample, in which case
 * the distribution can be approximated by a normal distribution, and
 * standard error analysis techniques can be applied.  Given a sample size
 * of n, a population size of N, and a sample frequency of p=cnt/n, the
 * standard error of the proportion p is given by
 *		SE = sqrt(p*(1-p)/n) * sqrt((N-n)/(N-1))
 * where the second term is the finite population correction.  To get
 * reasonably accurate planner estimates, we impose an upper bound on the
 * relative standard error of 20% -- i.e., SE/p < 0.2.  This 20% relative
 * error bound is fairly arbitrary, but has been found empirically to work
 * well.  Rearranging this formula gives a lower bound on the number of
 * instances of the value seen:
 *		cnt > n*(N-n) / (N-n+0.04*n*(N-1))
 * This bound is at most 25, and approaches 0 as n approaches 0 or N. The
 * case where n approaches 0 cannot happen in practice, since the sample
 * size is at least 300.  The case where n approaches N corresponds to
 * sampling the whole the table, in which case it is reasonable to keep
 * the whole MCV list (have no lower bound), so it makes sense to apply
 * this formula for all inputs, even though the above derivation is
 * technically only valid when the right hand side is at least around 10.
 *
 * An alternative way to look at this formula is as follows -- assume that
 * the number of instances of the value seen scales up to the entire
 * table, so that the population count is K=N*cnt/n. Then the distribution
 * in the sample is a hypergeometric distribution parameterised by N, n
 * and K, and the bound above is mathematically equivalent to demanding
 * that the standard deviation of that distribution is less than 20% of
 * its mean.  Thus the relative errors in any planner estimates produced
 * from the MCV statistics are likely to be not too large.
 *----------
 */
static double
get_mincount_for_mcv_list(int samplerows, double totalrows)
{
	double		n = samplerows;
	double		N = totalrows;
	double		numer,
				denom;

	numer = n * (N - n);
	denom = N - n + 0.04 * n * (N - 1);

	/* Guard against division by zero (possible if n = N = 1) */
	if (denom == 0)
		return 0;

	return numer / denom;
}

/*
 * Builds MCV list from the set of sampled rows.
 *
 * The algorithm is quite simple:
 *
 *	   (1) sort the data (default collation, '<' for the data type)
 *
 *	   (2) count distinct groups, decide how many to keep
 *
 *	   (3) build the MCV list using the threshold determined in (2)
 *
 *	   (4) remove rows represented by the MCV from the sample
 *
 */
MCVList *
statext_mcv_build(int numrows, HeapTuple *rows, Bitmapset *attrs,
				  VacAttrStats **stats, HeapTuple **rows_filtered,
				  int *numrows_filtered, double totalrows)
{
	int			i,
				j,
				k;
	int			numattrs = bms_num_members(attrs);
	int			ngroups;
	int			nitems;
	int		   *mcv_counts;
	int			numrows_mcv;

	int		   *attnums = build_attnums_array(attrs);

	MCVList    *mcvlist = NULL;

	/* comparator for all the columns */
	MultiSortSupport mss = build_mss(stats, attrs);

	/* sort the rows */
	SortItem   *items = build_sorted_items(numrows, rows, stats[0]->tupDesc,
										   mss, numattrs, attnums);

	/* transform the sorted rows into groups (sorted by frequency) */
	SortItem   *groups = build_distinct_groups(numrows, items, mss, &ngroups);

	/* Either we have both pointers or none of them. */
	Assert((rows_filtered && numrows_filtered) || (!rows_filtered && !numrows_filtered));

	/*
	 * Maximum number of MCV items to store, based on the attribute with the
	 * largest stats target (and the number of groups we have available).
	 */
	nitems = stats[0]->attr->attstattarget;
	for (i = 1; i < numattrs; i++)
	{
		if (stats[i]->attr->attstattarget > nitems)
			nitems = stats[i]->attr->attstattarget;
	}
	if (nitems > ngroups)
		nitems = ngroups;

	/*
	 * Decide how many items to keep in the MCV list. We simply use the same
	 * algorithm as for per-column MCV lists, to keep it consistent.
	 *
	 * One difference is that we do not have a multi-column stanullfrac, and
	 * we simply treat it as a special item in the MCV list (if it makes it).
	 * We could compute and store it, of course, but we may have statistics
	 * on more than two columns, so we'd probably want to store this for
	 * various combinations of columns - for K columns that'd be 2^K values.
	 * So we instead store those as items of the multi-column MCV list (if
	 * common enough).
	 *
	 * XXX Conceptually this is similar to the NULL-buckets of histograms.
	 */
	mcv_counts = (int *) palloc(sizeof(int) * nitems);

	for (i = 0; i < nitems; i++)
		mcv_counts[i] = groups[i].count;

	/*
	 * If we can fit all the items onto the MCV list, do that. Otherwise
	 * use get_mincount_for_mcv_list to decide which items to keep in the
	 * MCV list, based on the number of occurences in the sample.
	 *
	 * XXX We don't use the same criteria as for single-column MCV lists
	 * (which is what analyze_mcv_list does), because that may exclude
	 * items that are close to "average" frequency. But it does not say
	 * whether the frequency is close to base frequency or not. We also
	 * need to consider unexpectedly uncommon items (compared to base
	 * frequency), and the single-column algorithm ignores that.
	 */
	if (ngroups > nitems)
	{
		double mincount;

		mincount = get_mincount_for_mcv_list(numrows, totalrows);

		for (i = 0; i < nitems; i++)
		{
			if (mcv_counts[i] < mincount)
			{
				nitems = i;
				break;
			}
		}
	}

	/* number of rows represented by MCV items */
	numrows_mcv = 0;

	/*
	 * At this point we know the number of items for the MCV list. There might
	 * be none (for uniform distribution with many groups), and in that case
	 * there will be no MCV list. Otherwise construct the MCV list.
	 */
	if (nitems > 0)
	{
		/*
		 * Allocate the MCV list structure, set the global parameters.
		 */
		mcvlist = (MCVList *) palloc0(sizeof(MCVList));

		mcvlist->magic = STATS_MCV_MAGIC;
		mcvlist->type = STATS_MCV_TYPE_BASIC;
		mcvlist->ndimensions = numattrs;
		mcvlist->nitems = nitems;

		/* store info about data type OIDs */
		i = 0;
		j = -1;
		while ((j = bms_next_member(attrs, j)) >= 0)
		{
			VacAttrStats *colstat = stats[i];

			mcvlist->types[i] = colstat->attrtypid;
			i++;
		}

		/*
		 * Preallocate Datum/isnull arrays (not as a single chunk, as we will
		 * pass the result outside and thus it needs to be easy to pfree().
		 *
		 * XXX On second thought, we're the only ones dealing with MCV lists,
		 * so we might allocate everything as a single chunk to reduce palloc
		 * overhead (chunk headers, etc.) without significant risk. Not sure
		 * it's worth it, though, as we're not re-building stats very often.
		 */
		mcvlist->items = (MCVItem **) palloc(sizeof(MCVItem *) * nitems);

		for (i = 0; i < nitems; i++)
		{
			mcvlist->items[i] = (MCVItem *) palloc(sizeof(MCVItem));
			mcvlist->items[i]->values = (Datum *) palloc(sizeof(Datum) * numattrs);
			mcvlist->items[i]->isnull = (bool *) palloc(sizeof(bool) * numattrs);
		}

		/* Copy the first chunk of groups into the result. */
		for (i = 0; i < nitems; i++)
		{
			/* just pointer to the proper place in the list */
			MCVItem    *item = mcvlist->items[i];

			/* copy values from the _previous_ group (last item of) */
			memcpy(item->values, groups[i].values, sizeof(Datum) * numattrs);
			memcpy(item->isnull, groups[i].isnull, sizeof(bool) * numattrs);

			/* groups should be sorted by frequency in descending order */
			Assert((i == 0) || (groups[i - 1].count >= groups[i].count));

			/* group frequency */
			item->frequency = (double) groups[i].count / numrows;

			/* base frequency, if the attributes were independent */
			item->base_frequency = 1.0;
			for (j = 0; j < numattrs; j++)
			{
				int			count = 0;

				for (k = 0; k < ngroups; k++)
				{
					if (multi_sort_compare_dim(j, &groups[i], &groups[k], mss) == 0)
						count += groups[k].count;
				}

				item->base_frequency *= (double) count / numrows;
			}

			/* update the number of sampled rows represented by the MCV list */
			numrows_mcv += groups[i].count;
		}
	}

	/* Assume we're not returning any filtered rows by default. */
	if (numrows_filtered)
		*numrows_filtered = 0;

	if (rows_filtered)
		*rows_filtered = NULL;

	/*
	 * Produce an array with only tuples not covered by the MCV list. This is
	 * needed when building MCV+histogram pair, where MCV covers the most
	 * common combinations and histogram covers the remaining part.
	 *
	 * We will first sort the groups by the keys (not by count) and then use
	 * binary search in the group array to check which rows are covered by the
	 * MCV items.
	 *
	 * Do not modify the array in place, as there may be additional stats on
	 * the table and we need to keep the original array for them.
	 *
	 * We only do this when requested by passing non-NULL rows_filtered, and
	 * when there are rows not covered by the MCV list (that is, when
	 * numrows_mcv < numrows), or also (nitems < ngroups).
	 */
	if (rows_filtered && numrows_filtered && (nitems < ngroups))
	{
		int			i,
					j;

		/* used to build the filtered array of tuples */
		HeapTuple  *filtered;
		int			nfiltered;

		/* used for the searches */
		SortItem	key;

		/* We do know how many rows we expect (total - MCV rows). */
		nfiltered = (numrows - numrows_mcv);
		filtered = (HeapTuple *) palloc(nfiltered * sizeof(HeapTuple));

		/* wfill this with data from the rows */
		key.values = (Datum *) palloc0(numattrs * sizeof(Datum));
		key.isnull = (bool *) palloc0(numattrs * sizeof(bool));

		/*
		 * Sort the groups for bsearch_r (but only the items that actually
		 * made it to the MCV list).
		 */
		qsort_arg((void *) groups, nitems, sizeof(SortItem),
				  multi_sort_compare, mss);

		/* walk through the tuples, compare the values to MCV items */
		nfiltered = 0;
		for (i = 0; i < numrows; i++)
		{
			/* collect the key values from the row */
			for (j = 0; j < numattrs; j++)
				key.values[j]
					= heap_getattr(rows[i], attnums[j],
								   stats[j]->tupDesc, &key.isnull[j]);

			/* if not included in the MCV list, keep it in the array */
			if (bsearch_arg(&key, groups, nitems, sizeof(SortItem),
							multi_sort_compare, mss) == NULL)
				filtered[nfiltered++] = rows[i];

			/* do not overflow the array */
			Assert(nfiltered <= (numrows - numrows_mcv));
		}

		/* expect to get the right number of remaining rows exactly */
		Assert(nfiltered + numrows_mcv == numrows);

		/* pass the filtered tuples up */
		*numrows_filtered = nfiltered;
		*rows_filtered = filtered;

		/* free all the data used here */
		pfree(key.values);
		pfree(key.isnull);
	}

	pfree(items);
	pfree(groups);
	pfree(mcv_counts);

	return mcvlist;
}

/*
 * build_mss
 *	build MultiSortSupport for the attributes passed in attrs
 */
static MultiSortSupport
build_mss(VacAttrStats **stats, Bitmapset *attrs)
{
	int			i,
				j;
	int			numattrs = bms_num_members(attrs);

	/* Sort by multiple columns (using array of SortSupport) */
	MultiSortSupport mss = multi_sort_init(numattrs);

	/* prepare the sort functions for all the attributes */
	i = 0;
	j = -1;
	while ((j = bms_next_member(attrs, j)) >= 0)
	{
		VacAttrStats *colstat = stats[i];
		TypeCacheEntry *type;

		type = lookup_type_cache(colstat->attrtypid, TYPECACHE_LT_OPR);
		if (type->lt_opr == InvalidOid) /* shouldn't happen */
			elog(ERROR, "cache lookup failed for ordering operator for type %u",
				 colstat->attrtypid);

		multi_sort_add_dimension(mss, i, type->lt_opr, type->typcollation);
		i++;
	}

	return mss;
}

/*
 * count_distinct_groups
 *	count distinct combinations of SortItems in the array
 *
 * The array is assumed to be sorted according to the MultiSortSupport.
 */
static int
count_distinct_groups(int numrows, SortItem *items, MultiSortSupport mss)
{
	int			i;
	int			ndistinct;

	ndistinct = 1;
	for (i = 1; i < numrows; i++)
	{
		/* make sure the array really is sorted */
		Assert(multi_sort_compare(&items[i], &items[i - 1], mss) >= 0);

		if (multi_sort_compare(&items[i], &items[i - 1], mss) != 0)
			ndistinct += 1;
	}

	return ndistinct;
}

/*
 * compare_sort_item_count
 *	comparator for sorting items by count (frequencies) in descending order
 */
static int
compare_sort_item_count(const void *a, const void *b)
{
	SortItem   *ia = (SortItem *) a;
	SortItem   *ib = (SortItem *) b;

	if (ia->count == ib->count)
		return 0;
	else if (ia->count > ib->count)
		return -1;

	return 1;
}

/*
 * build_distinct_groups
 *	build array of SortItems for distinct groups and counts matching items
 *
 * The input array is assumed to be sorted
 */
static SortItem *
build_distinct_groups(int numrows, SortItem *items, MultiSortSupport mss,
					  int *ndistinct)
{
	int			i,
				j;
	int			ngroups = count_distinct_groups(numrows, items, mss);

	SortItem   *groups = (SortItem *) palloc(ngroups * sizeof(SortItem));

	j = 0;
	groups[0] = items[0];
	groups[0].count = 1;

	for (i = 1; i < numrows; i++)
	{
		/* Assume sorted in ascending order. */
		Assert(multi_sort_compare(&items[i], &items[i - 1], mss) >= 0);

		/* New distinct group detected. */
		if (multi_sort_compare(&items[i], &items[i - 1], mss) != 0)
			groups[++j] = items[i];

		groups[j].count++;
	}

	/* ensure we filled the expected number of distinct groups */
	Assert(j + 1 == ngroups);

	/* Sort the distinct groups by frequency (in descending order). */
	pg_qsort((void *) groups, ngroups, sizeof(SortItem),
			 compare_sort_item_count);

	*ndistinct = ngroups;
	return groups;
}


/*
 * statext_mcv_load
 *		Load the MCV list for the indicated pg_statistic_ext tuple
 */
MCVList *
statext_mcv_load(Oid mvoid)
{
	bool		isnull = false;
	Datum		mcvlist;
	HeapTuple	htup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(mvoid));

	if (!HeapTupleIsValid(htup))
		elog(ERROR, "cache lookup failed for statistics object %u", mvoid);

	mcvlist = SysCacheGetAttr(STATEXTOID, htup,
							  Anum_pg_statistic_ext_stxmcv, &isnull);

	ReleaseSysCache(htup);

	if (isnull)
		return NULL;

	return statext_mcv_deserialize(DatumGetByteaP(mcvlist));
}


/*
 * Serialize MCV list into a bytea value.
 *
 * The basic algorithm is simple:
 *
 * (1) perform deduplication (for each attribute separately)
 *	   (a) collect all (non-NULL) attribute values from all MCV items
 *	   (b) sort the data (using 'lt' from VacAttrStats)
 *	   (c) remove duplicate values from the array
 *
 * (2) serialize the arrays into a bytea value
 *
 * (3) process all MCV list items
 *	   (a) replace values with indexes into the arrays
 *
 * Each attribute has to be processed separately, as we may be mixing different
 * datatypes, with different sort operators, etc.
 *
 * We use uint16 values for the indexes in step (3), as we currently don't allow
 * more than 8k MCV items anyway, although that's mostly arbitrary limit. We might
 * increase this to 65k and still fit into uint16. Furthermore, this limit is on
 * the number of distinct values per column, and we usually have few of those
 * (and various combinations of them for the those MCV list). So uint16 seems fine.
 *
 * We don't really expect the serialization to save as much space as for
 * histograms, as we are not doing any bucket splits (which is the source
 * of high redundancy in histograms).
 *
 * TODO: Consider packing boolean flags (NULL) for each item into a single char
 * (or a longer type) instead of using an array of bool items.
 */
bytea *
statext_mcv_serialize(MCVList * mcvlist, VacAttrStats **stats)
{
	int			i;
	int			dim;
	int			ndims = mcvlist->ndimensions;
	int			itemsize = ITEM_SIZE(ndims);

	SortSupport ssup;
	DimensionInfo *info;

	Size		total_length;

	/* allocate the item just once */
	char	   *item = palloc0(itemsize);

	/* serialized items (indexes into arrays, etc.) */
	bytea	   *output;
	char	   *data = NULL;

	/* values per dimension (and number of non-NULL values) */
	Datum	  **values = (Datum **) palloc0(sizeof(Datum *) * ndims);
	int		   *counts = (int *) palloc0(sizeof(int) * ndims);

	/*
	 * We'll include some rudimentary information about the attributes (type
	 * length, etc.), so that we don't have to look them up while
	 * deserializing the MCV list.
	 *
	 * XXX Maybe this is not a great idea? Or maybe we should actually copy
	 * more fields, e.g. typeid, which would allow us to display the MCV list
	 * using only the serialized representation (currently we have to fetch
	 * this info from the relation).
	 */
	info = (DimensionInfo *) palloc0(sizeof(DimensionInfo) * ndims);

	/* sort support data for all attributes included in the MCV list */
	ssup = (SortSupport) palloc0(sizeof(SortSupportData) * ndims);

	/* collect and deduplicate values for each dimension (attribute) */
	for (dim = 0; dim < ndims; dim++)
	{
		int			ndistinct;
		TypeCacheEntry *typentry;

		/*
		 * Lookup the LT operator (can't get it from stats extra_data, as we
		 * don't know how to interpret that - scalar vs. array etc.).
		 */
		typentry = lookup_type_cache(stats[dim]->attrtypid, TYPECACHE_LT_OPR);

		/* copy important info about the data type (length, by-value) */
		info[dim].typlen = stats[dim]->attrtype->typlen;
		info[dim].typbyval = stats[dim]->attrtype->typbyval;

		/* allocate space for values in the attribute and collect them */
		values[dim] = (Datum *) palloc0(sizeof(Datum) * mcvlist->nitems);

		for (i = 0; i < mcvlist->nitems; i++)
		{
			/* skip NULL values - we don't need to deduplicate those */
			if (mcvlist->items[i]->isnull[dim])
				continue;

			values[dim][counts[dim]] = mcvlist->items[i]->values[dim];
			counts[dim] += 1;
		}

		/* if there are just NULL values in this dimension, we're done */
		if (counts[dim] == 0)
			continue;

		/* sort and deduplicate the data */
		ssup[dim].ssup_cxt = CurrentMemoryContext;
		ssup[dim].ssup_collation = DEFAULT_COLLATION_OID;
		ssup[dim].ssup_nulls_first = false;

		PrepareSortSupportFromOrderingOp(typentry->lt_opr, &ssup[dim]);

		qsort_arg(values[dim], counts[dim], sizeof(Datum),
				  compare_scalars_simple, &ssup[dim]);

		/*
		 * Walk through the array and eliminate duplicate values, but keep the
		 * ordering (so that we can do bsearch later). We know there's at
		 * least one item as (counts[dim] != 0), so we can skip the first
		 * element.
		 */
		ndistinct = 1;			/* number of distinct values */
		for (i = 1; i < counts[dim]; i++)
		{
			/* expect sorted array */
			Assert(compare_datums_simple(values[dim][i - 1], values[dim][i], &ssup[dim]) <= 0);

			/* if the value is the same as the previous one, we can skip it */
			if (!compare_datums_simple(values[dim][i - 1], values[dim][i], &ssup[dim]))
				continue;

			values[dim][ndistinct] = values[dim][i];
			ndistinct += 1;
		}

		/* we must not exceed PG_UINT16_MAX, as we use uint16 indexes */
		Assert(ndistinct <= PG_UINT16_MAX);

		/*
		 * Store additional info about the attribute - number of deduplicated
		 * values, and also size of the serialized data. For fixed-length data
		 * types this is trivial to compute, for varwidth types we need to
		 * actually walk the array and sum the sizes.
		 */
		info[dim].nvalues = ndistinct;

		if (info[dim].typlen > 0)	/* fixed-length data types */
			info[dim].nbytes = info[dim].nvalues * info[dim].typlen;
		else if (info[dim].typlen == -1)	/* varlena */
		{
			info[dim].nbytes = 0;
			for (i = 0; i < info[dim].nvalues; i++)
				info[dim].nbytes += VARSIZE_ANY(values[dim][i]);
		}
		else if (info[dim].typlen == -2)	/* cstring */
		{
			info[dim].nbytes = 0;
			for (i = 0; i < info[dim].nvalues; i++)
				info[dim].nbytes += strlen(DatumGetCString(values[dim][i]));
		}

		/* we know (count>0) so there must be some data */
		Assert(info[dim].nbytes > 0);
	}

	/*
	 * Now we can finally compute how much space we'll actually need for the
	 * whole serialized MCV list, as it contains these fields:
	 *
	 * - length (4B) for varlena - magic (4B) - type (4B) - ndimensions (4B) -
	 * nitems (4B) - info (ndim * sizeof(DimensionInfo) - arrays of values for
	 * each dimension - serialized items (nitems * itemsize)
	 *
	 * So the 'header' size is 20B + ndim * sizeof(DimensionInfo) and then we
	 * will place all the data (values + indexes). We'll however use offsetof
	 * and sizeof to compute sizes of the structs.
	 */
	total_length = (sizeof(int32) + offsetof(MCVList, items)
					+ (ndims * sizeof(DimensionInfo))
					+ mcvlist->nitems * itemsize);

	/* add space for the arrays of deduplicated values */
	for (i = 0; i < ndims; i++)
		total_length += info[i].nbytes;

	/*
	 * Enforce arbitrary limit of 1MB on the size of the serialized MCV list.
	 * This is meant as a protection against someone building MCV list on long
	 * values (e.g. text documents).
	 *
	 * XXX Should we enforce arbitrary limits like this one? Maybe it's not
	 * even necessary, as long values are usually unique and so won't make it
	 * into the MCV list in the first place. In the end, we have a 1GB limit
	 * on bytea values.
	 */
	if (total_length > (1024 * 1024))
		elog(ERROR, "serialized MCV list exceeds 1MB (%zu)", total_length);

	/* allocate space for the serialized MCV list, set header fields */
	output = (bytea *) palloc0(total_length);
	SET_VARSIZE(output, total_length);

	/* 'data' points to the current position in the output buffer */
	data = VARDATA(output);

	/* MCV list header (number of items, ...) */
	memcpy(data, mcvlist, offsetof(MCVList, items));
	data += offsetof(MCVList, items);

	/* information about the attributes */
	memcpy(data, info, sizeof(DimensionInfo) * ndims);
	data += sizeof(DimensionInfo) * ndims;

	/* Copy the deduplicated values for all attributes to the output. */
	for (dim = 0; dim < ndims; dim++)
	{
#ifdef USE_ASSERT_CHECKING
		/* remember the starting point for Asserts later */
		char	   *tmp = data;
#endif
		for (i = 0; i < info[dim].nvalues; i++)
		{
			Datum		v = values[dim][i];

			if (info[dim].typbyval) /* passed by value */
			{
				memcpy(data, &v, info[dim].typlen);
				data += info[dim].typlen;
			}
			else if (info[dim].typlen > 0)	/* pased by reference */
			{
				memcpy(data, DatumGetPointer(v), info[dim].typlen);
				data += info[dim].typlen;
			}
			else if (info[dim].typlen == -1)	/* varlena */
			{
				memcpy(data, DatumGetPointer(v), VARSIZE_ANY(v));
				data += VARSIZE_ANY(v);
			}
			else if (info[dim].typlen == -2)	/* cstring */
			{
				memcpy(data, DatumGetCString(v), strlen(DatumGetCString(v)) + 1);
				data += strlen(DatumGetCString(v)) + 1; /* terminator */
			}

			/* no underflows or overflows */
			Assert((data > tmp) && ((data - tmp) <= info[dim].nbytes));
		}

		/*
		 * check we got exactly the amount of data we expected for this
		 * dimension
		 */
		Assert((data - tmp) == info[dim].nbytes);
	}

	/* Serialize the items, with uint16 indexes instead of the values. */
	for (i = 0; i < mcvlist->nitems; i++)
	{
		MCVItem    *mcvitem = mcvlist->items[i];

		/* don't write beyond the allocated space */
		Assert(data <= (char *) output + total_length - itemsize);

		/* reset the item (we only allocate it once and reuse it) */
		memset(item, 0, itemsize);

		for (dim = 0; dim < ndims; dim++)
		{
			Datum	   *value;

			/* do the lookup only for non-NULL values */
			if (mcvlist->items[i]->isnull[dim])
				continue;

			value = (Datum *) bsearch_arg(&mcvitem->values[dim], values[dim],
									  info[dim].nvalues, sizeof(Datum),
									  compare_scalars_simple, &ssup[dim]);

			Assert(value != NULL);	/* serialization or deduplication error */

			/* compute index within the array */
			ITEM_INDEXES(item)[dim] = (value - values[dim]);

			/* check the index is within expected bounds */
			Assert(ITEM_INDEXES(item)[dim] >= 0);
			Assert(ITEM_INDEXES(item)[dim] < info[dim].nvalues);
		}

		/* copy NULL and frequency flags into the item */
		memcpy(ITEM_NULLS(item, ndims), mcvitem->isnull, sizeof(bool) * ndims);
		memcpy(ITEM_FREQUENCY(item, ndims), &mcvitem->frequency, sizeof(double));
		memcpy(ITEM_BASE_FREQUENCY(item, ndims), &mcvitem->base_frequency, sizeof(double));

		/* copy the serialized item into the array */
		memcpy(data, item, itemsize);

		data += itemsize;
	}

	/* at this point we expect to match the total_length exactly */
	Assert((data - (char *) output) == total_length);

	pfree(item);
	pfree(values);
	pfree(counts);

	return output;
}

/*
 * Reads serialized MCV list into MCVList structure.
 *
 * Unlike with histograms, we deserialize the MCV list fully (i.e. we don't
 * keep the deduplicated arrays and pointers into them), as we don't expect
 * there to be a lot of duplicate values. But perhaps that's not true and we
 * should keep the MCV in serialized form too.
 *
 * XXX See how much memory we could save by keeping the deduplicated version
 * (both for typical and corner cases with few distinct values but many items).
 */
MCVList *
statext_mcv_deserialize(bytea *data)
{
	int			dim,
				i;
	Size		expected_size;
	MCVList    *mcvlist;
	char	   *tmp;

	int			ndims,
				nitems,
				itemsize;
	DimensionInfo *info = NULL;
	Datum	  **values = NULL;

	/* local allocation buffer (used only for deserialization) */
	int			bufflen;
	char	   *buff;
	char	   *ptr;

	/* buffer used for the result */
	int			rbufflen;
	char	   *rbuff;
	char	   *rptr;

	if (data == NULL)
		return NULL;

	/*
	 * We can't possibly deserialize a MCV list if there's not even a complete
	 * header.
	 */
	if (VARSIZE_ANY_EXHDR(data) < offsetof(MCVList, items))
		elog(ERROR, "invalid MCV Size %ld (expected at least %ld)",
			 VARSIZE_ANY_EXHDR(data), offsetof(MCVList, items));

	/* read the MCV list header */
	mcvlist = (MCVList *) palloc0(sizeof(MCVList));

	/* initialize pointer to the data part (skip the varlena header) */
	tmp = VARDATA_ANY(data);

	/* get the header and perform further sanity checks */
	memcpy(mcvlist, tmp, offsetof(MCVList, items));
	tmp += offsetof(MCVList, items);

	if (mcvlist->magic != STATS_MCV_MAGIC)
		elog(ERROR, "invalid MCV magic %d (expected %d)",
			 mcvlist->magic, STATS_MCV_MAGIC);

	if (mcvlist->type != STATS_MCV_TYPE_BASIC)
		elog(ERROR, "invalid MCV type %d (expected %d)",
			 mcvlist->type, STATS_MCV_TYPE_BASIC);

	if (mcvlist->ndimensions == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid zero-length dimension array in MCVList")));
	else if (mcvlist->ndimensions > STATS_MAX_DIMENSIONS)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid length (%d) dimension array in MCVList",
						mcvlist->ndimensions)));

	if (mcvlist->nitems == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid zero-length item array in MCVList")));
	else if (mcvlist->nitems > STATS_MCVLIST_MAX_ITEMS)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid length (%d) item array in MCVList",
						mcvlist->nitems)));

	nitems = mcvlist->nitems;
	ndims = mcvlist->ndimensions;
	itemsize = ITEM_SIZE(ndims);

	/*
	 * Check amount of data including DimensionInfo for all dimensions and
	 * also the serialized items (including uint16 indexes). Also, walk
	 * through the dimension information and add it to the sum.
	 */
	expected_size = offsetof(MCVList, items) +
		ndims * sizeof(DimensionInfo) +
		(nitems * itemsize);

	/*
	 * Check that we have at least the dimension and info records, along with
	 * the items. We don't know the size of the serialized values yet. We need
	 * to do this check first, before accessing the dimension info.
	 */
	if (VARSIZE_ANY_EXHDR(data) < expected_size)
		elog(ERROR, "invalid MCV size %ld (expected %zu)",
			 VARSIZE_ANY_EXHDR(data), expected_size);

	/* Now it's safe to access the dimension info. */
	info = (DimensionInfo *) (tmp);
	tmp += ndims * sizeof(DimensionInfo);

	/* account for the value arrays */
	for (dim = 0; dim < ndims; dim++)
	{
		/*
		 * XXX I wonder if we can/should rely on asserts here. Maybe those
		 * checks should be done every time?
		 */
		Assert(info[dim].nvalues >= 0);
		Assert(info[dim].nbytes >= 0);

		expected_size += info[dim].nbytes;
	}

	/*
	 * Now we know the total expected MCV size, including all the pieces
	 * (header, dimension info. items and deduplicated data). So do the final
	 * check on size.
	 */
	if (VARSIZE_ANY_EXHDR(data) != expected_size)
		elog(ERROR, "invalid MCV size %ld (expected %ld)",
			 VARSIZE_ANY_EXHDR(data), expected_size);

	/*
	 * Allocate one large chunk of memory for the intermediate data, needed
	 * only for deserializing the MCV list (and allocate densely to minimize
	 * the palloc overhead).
	 *
	 * Let's see how much space we'll actually need, and also include space
	 * for the array with pointers.
	 *
	 * We need an array of Datum pointers values for each dimension, so that
	 * we can easily translate the uint16 indexes. We also need a top-level
	 * array of pointers to those per-dimension arrays.
	 *
	 * For byval types with size matching sizeof(Datum) we can reuse the
	 * serialized array directly.
	 */
	bufflen = sizeof(Datum **) * ndims; /* space for top-level pointers */

	for (dim = 0; dim < ndims; dim++)
	{
		/* for full-size byval types, we reuse the serialized value */
		if (!(info[dim].typbyval && info[dim].typlen == sizeof(Datum)))
			bufflen += (sizeof(Datum) * info[dim].nvalues);
	}

	buff = palloc0(bufflen);
	ptr = buff;

	values = (Datum **) buff;
	ptr += (sizeof(Datum *) * ndims);

	/*
	 * XXX This uses pointers to the original data array (the types not passed
	 * by value), so when someone frees the memory, e.g. by doing something
	 * like this:
	 *
	 *	  bytea * data = ... fetch the data from catalog ...
	 *
	 *	  MCVList mcvlist = deserialize_mcv_list(data);
	 *
	 *	  pfree(data);
	 *
	 * then 'mcvlist' references the freed memory. Should copy the pieces.
	 */
	for (dim = 0; dim < ndims; dim++)
	{
#ifdef USE_ASSERT_CHECKING
		/* remember where data for this dimension starts */
		char	   *start = tmp;
#endif
		if (info[dim].typbyval)
		{
			/* passed by value / size matches Datum - just reuse the array */
			if (info[dim].typlen == sizeof(Datum))
			{
				values[dim] = (Datum *) tmp;
				tmp += info[dim].nbytes;

				/* no overflow of input array */
				Assert(tmp <= start + info[dim].nbytes);
			}
			else
			{
				values[dim] = (Datum *) ptr;
				ptr += (sizeof(Datum) * info[dim].nvalues);

				for (i = 0; i < info[dim].nvalues; i++)
				{
					/* just point into the array */
					memcpy(&values[dim][i], tmp, info[dim].typlen);
					tmp += info[dim].typlen;

					/* no overflow of input array */
					Assert(tmp <= start + info[dim].nbytes);
				}
			}
		}
		else
		{
			/* all the other types need a chunk of the buffer */
			values[dim] = (Datum *) ptr;
			ptr += (sizeof(Datum) * info[dim].nvalues);

			/* passed by reference, but fixed length (name, tid, ...) */
			if (info[dim].typlen > 0)
			{
				for (i = 0; i < info[dim].nvalues; i++)
				{
					/* just point into the array */
					values[dim][i] = PointerGetDatum(tmp);
					tmp += info[dim].typlen;

					/* no overflow of input array */
					Assert(tmp <= start + info[dim].nbytes);
				}
			}
			else if (info[dim].typlen == -1)
			{
				/* varlena */
				for (i = 0; i < info[dim].nvalues; i++)
				{
					/* just point into the array */
					values[dim][i] = PointerGetDatum(tmp);
					tmp += VARSIZE_ANY(tmp);

					/* no overflow of input array */
					Assert(tmp <= start + info[dim].nbytes);
				}
			}
			else if (info[dim].typlen == -2)
			{
				/* cstring */
				for (i = 0; i < info[dim].nvalues; i++)
				{
					/* just point into the array */
					values[dim][i] = PointerGetDatum(tmp);
					tmp += (strlen(tmp) + 1);	/* don't forget the \0 */

					/* no overflow of input array */
					Assert(tmp <= start + info[dim].nbytes);
				}
			}
		}

		/* check we consumed the serialized data for this dimension exactly */
		Assert((tmp - start) == info[dim].nbytes);
	}

	/* we should have exhausted the buffer exactly */
	Assert((ptr - buff) == bufflen);

	/* allocate space for all the MCV items in a single piece */
	rbufflen = (sizeof(MCVItem *) + sizeof(MCVItem) +
				sizeof(Datum) * ndims + sizeof(bool) * ndims) * nitems;

	rbuff = palloc0(rbufflen);
	rptr = rbuff;

	mcvlist->items = (MCVItem * *) rbuff;
	rptr += (sizeof(MCVItem *) * nitems);

	/* deserialize the MCV items and translate the indexes to Datums */
	for (i = 0; i < nitems; i++)
	{
		uint16	   *indexes = NULL;
		MCVItem    *item = (MCVItem *) rptr;

		rptr += (sizeof(MCVItem));

		item->values = (Datum *) rptr;
		rptr += (sizeof(Datum) * ndims);

		item->isnull = (bool *) rptr;
		rptr += (sizeof(bool) * ndims);

		/* just point to the right place */
		indexes = ITEM_INDEXES(tmp);

		memcpy(item->isnull, ITEM_NULLS(tmp, ndims), sizeof(bool) * ndims);
		memcpy(&item->frequency, ITEM_FREQUENCY(tmp, ndims), sizeof(double));
		memcpy(&item->base_frequency, ITEM_BASE_FREQUENCY(tmp, ndims), sizeof(double));

		/* translate the values */
		for (dim = 0; dim < ndims; dim++)
			if (!item->isnull[dim])
				item->values[dim] = values[dim][indexes[dim]];

		mcvlist->items[i] = item;

		tmp += ITEM_SIZE(ndims);

		/* check we're not overflowing the input */
		Assert(tmp <= (char *) data + VARSIZE_ANY(data));
	}

	/* check that we processed all the data */
	Assert(tmp == (char *) data + VARSIZE_ANY(data));

	/* release the temporary buffer */
	pfree(buff);

	return mcvlist;
}

/*
 * SRF with details about buckets of a histogram:
 *
 * - item ID (0...nitems)
 * - values (string array)
 * - nulls only (boolean array)
 * - frequency (double precision)
 * - base_frequency (double precision)
 *
 * The input is the OID of the statistics, and there are no rows returned if
 * the statistics contains no histogram.
 */
PG_FUNCTION_INFO_V1(pg_stats_ext_mcvlist_items);

Datum
pg_stats_ext_mcvlist_items(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	int			call_cntr;
	int			max_calls;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		MCVList    *mcvlist;
		TupleDesc	tupdesc;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		mcvlist = statext_mcv_deserialize(PG_GETARG_BYTEA_P(0));

		funcctx->user_fctx = mcvlist;

		/* total number of tuples to be returned */
		funcctx->max_calls = 0;
		if (funcctx->user_fctx != NULL)
			funcctx->max_calls = mcvlist->nitems;

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;

	if (call_cntr < max_calls)	/* do when there is more left to send */
	{
		Datum	   *values;
		bool	   *nulls;
		HeapTuple	tuple;
		Datum		result;
		TupleDesc	tupdesc;

		StringInfoData	itemValues;
		StringInfoData	itemNulls;

		int			i;

		Oid		   *outfuncs;
		FmgrInfo   *fmgrinfo;

		MCVList    *mcvlist;
		MCVItem    *item;

		mcvlist = (MCVList *) funcctx->user_fctx;

		Assert(call_cntr < mcvlist->nitems);

		item = mcvlist->items[call_cntr];

		/*
		 * Prepare a values array for building the returned tuple. This should
		 * be an array of C strings which will be processed later by the type
		 * input functions.
		 */
		values = (Datum *) palloc(5 * sizeof(Datum));
		nulls = (bool *) palloc(5 * sizeof(bool));

		/* no NULL values */
		memset(nulls, 0, 5 * sizeof(bool));

		outfuncs = (Oid *) palloc0(sizeof(Oid) * mcvlist->ndimensions);
		fmgrinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * mcvlist->ndimensions);

		for (i = 0; i < mcvlist->ndimensions; i++)
		{
			bool		isvarlena;

			getTypeOutputInfo(mcvlist->types[i], &outfuncs[i], &isvarlena);

			fmgr_info(outfuncs[i], &fmgrinfo[i]);
		}

		values[0] = Int32GetDatum(call_cntr);	/* item ID */

		/* build the arrays of values / nulls */
		initStringInfo(&itemValues);
		initStringInfo(&itemNulls);

		appendStringInfoString(&itemValues, "{");
		appendStringInfoString(&itemNulls, "{");

		for (i = 0; i < mcvlist->ndimensions; i++)
		{
			Datum		val,
						valout;

			if (i == 0)
			{
				appendStringInfoString(&itemValues, ", ");
				appendStringInfoString(&itemNulls, ", ");
			}

			if (item->isnull[i])
				valout = CStringGetDatum("NULL");
			else
			{
				val = item->values[i];
				valout = FunctionCall1(&fmgrinfo[i], val);
			}

			appendStringInfoString(&itemValues, DatumGetCString(valout));
			appendStringInfoString(&itemNulls, item->isnull[i] ? "t" : "f");
		}

		values[1] = CStringGetDatum(itemValues.data);
		values[2] = CStringGetDatum(itemNulls.data);

		values[3] = Float8GetDatum(item->frequency);		/* frequency */
		values[4] = Float8GetDatum(item->base_frequency);	/* base frequency */

		/* get tuple descriptor (we've checked it's correct on first call) */
		get_call_result_type(fcinfo, NULL, &tupdesc);

		/* build a tuple */
		tuple = heap_form_tuple(tupdesc, values, nulls);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(tuple);

		/* clean up (this is not really necessary) */
		pfree(itemValues.data);
		pfree(itemNulls.data);

		pfree(values);
		pfree(nulls);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else						/* do when there is no more left */
	{
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * pg_mcv_list_in		- input routine for type pg_mcv_list.
 *
 * pg_mcv_list is real enough to be a table column, but it has no operations
 * of its own, and disallows input too
 */
Datum
pg_mcv_list_in(PG_FUNCTION_ARGS)
{
	/*
	 * pg_mcv_list stores the data in binary form and parsing text input is
	 * not needed, so disallow this.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "pg_mcv_list")));

	PG_RETURN_VOID();			/* keep compiler quiet */
}


/*
 * pg_mcv_list_out		- output routine for type PG_MCV_LIST.
 *
 * MCV lists are serialized into a bytea value, so we simply call byteaout()
 * to serialize the value into text. But it'd be nice to serialize that into
 * a meaningful representation (e.g. for inspection by people).
 *
 * XXX This should probably return something meaningful, similar to what
 * pg_dependencies_out does. Not sure how to deal with the deduplicated
 * values, though - do we want to expand that or not?
 */
Datum
pg_mcv_list_out(PG_FUNCTION_ARGS)
{
	return byteaout(fcinfo);
}

/*
 * pg_mcv_list_recv		- binary input routine for type pg_mcv_list.
 */
Datum
pg_mcv_list_recv(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "pg_mcv_list")));

	PG_RETURN_VOID();			/* keep compiler quiet */
}

/*
 * pg_mcv_list_send		- binary output routine for type pg_mcv_list.
 *
 * MCV lists are serialized in a bytea value (although the type is named
 * differently), so let's just send that.
 */
Datum
pg_mcv_list_send(PG_FUNCTION_ARGS)
{
	return byteasend(fcinfo);
}

/*
 * mcv_get_match_bitmap
 *	Evaluate clauses using the MCV list, and update the match bitmap.
 *
 * A match bitmap keeps match/mismatch status for each MCV item, and we
 * update it based on additional clauses. We also use it to skip items
 * that can't possibly match (e.g. item marked as "mismatch" can't change
 * to "match" when evaluating AND clause list).
 *
 * The function also returns a flag indicating whether there was an
 * equality condition for all attributes, the minimum frequency in the MCV
 * list, and a total MCV frequency (sum of frequencies for all items).
 *
 * XXX Currently the match bitmap uses a char for each MCV item, which is
 * somewhat wasteful as we could do with just a single bit, thus reducing
 * the size to ~1/8. It would also allow us to combine bitmaps simply using
 * & and |, which should be faster than min/max. The bitmaps are fairly
 * small, though (as we cap the MCV list size to 8k items).
 */
static char *
mcv_get_match_bitmap(PlannerInfo *root, List *clauses,
					  Bitmapset *keys, MCVList * mcvlist, bool is_or)
{
	int			i;
	ListCell   *l;
	char	   *matches;

	/* The bitmap may be partially built. */
	Assert(clauses != NIL);
	Assert(list_length(clauses) >= 1);
	Assert(mcvlist != NULL);
	Assert(mcvlist->nitems > 0);
	Assert(mcvlist->nitems <= STATS_MCVLIST_MAX_ITEMS);

	matches = palloc(sizeof(char) * mcvlist->nitems);
	memset(matches, (is_or) ? STATS_MATCH_NONE : STATS_MATCH_FULL,
		   sizeof(char) * mcvlist->nitems);

	/*
	 * Loop through the list of clauses, and for each of them evaluate all the
	 * MCV items not yet eliminated by the preceding clauses.
	 */
	foreach(l, clauses)
	{
		Node	   *clause = (Node *) lfirst(l);

		/* if it's a RestrictInfo, then extract the clause */
		if (IsA(clause, RestrictInfo))
			clause = (Node *) ((RestrictInfo *) clause)->clause;

		/*
		 * Handle the various types of clauses - OpClause, NullTest and
		 * AND/OR/NOT
		 */
		if (is_opclause(clause))
		{
			OpExpr	   *expr = (OpExpr *) clause;
			bool		varonleft = true;
			bool		ok;
			FmgrInfo	opproc;

			/* get procedure computing operator selectivity */
			RegProcedure oprrest = get_oprrest(expr->opno);

			fmgr_info(get_opcode(expr->opno), &opproc);

			ok = (NumRelids(clause) == 1) &&
				(is_pseudo_constant_clause(lsecond(expr->args)) ||
				 (varonleft = false,
				  is_pseudo_constant_clause(linitial(expr->args))));

			if (ok)
			{
				TypeCacheEntry *typecache;
				FmgrInfo	gtproc;
				Var		   *var;
				Const	   *cst;
				bool		isgt;
				int			idx;

				/* extract the var and const from the expression */
				var = (varonleft) ? linitial(expr->args) : lsecond(expr->args);
				cst = (varonleft) ? lsecond(expr->args) : linitial(expr->args);
				isgt = (!varonleft);

				/* match the attribute to a dimension of the statistic */
				idx = bms_member_index(keys, var->varattno);

				/* get information about the >= procedure */
				typecache = lookup_type_cache(var->vartype, TYPECACHE_GT_OPR);
				fmgr_info(get_opcode(typecache->gt_opr), &gtproc);

				/*
				 * Walk through the MCV items and evaluate the current clause.
				 * We can skip items that were already ruled out, and
				 * terminate if there are no remaining MCV items that might
				 * possibly match.
				 */
				for (i = 0; i < mcvlist->nitems; i++)
				{
					bool		mismatch = false;
					MCVItem    *item = mcvlist->items[i];

					/*
					 * For AND-lists, we can also mark NULL items as 'no
					 * match' (and then skip them). For OR-lists this is not
					 * possible.
					 */
					if ((!is_or) && item->isnull[idx])
						matches[i] = STATS_MATCH_NONE;

					/* skip MCV items that were already ruled out */
					if ((!is_or) && (matches[i] == STATS_MATCH_NONE))
						continue;
					else if (is_or && (matches[i] == STATS_MATCH_FULL))
						continue;

					switch (oprrest)
					{
						case F_EQSEL:
						case F_NEQSEL:

							/*
							 * We don't care about isgt in equality, because
							 * it does not matter whether it's (var op const)
							 * or (const op var).
							 */
							mismatch = !DatumGetBool(FunctionCall2Coll(&opproc,
																	   DEFAULT_COLLATION_OID,
																	   cst->constvalue,
																	   item->values[idx]));

							break;

						case F_SCALARLTSEL: /* column < constant */
						case F_SCALARLESEL: /* column <= constant */
						case F_SCALARGTSEL: /* column > constant */
						case F_SCALARGESEL: /* column >= constant */

							/*
							 * First check whether the constant is below the
							 * lower boundary (in that case we can skip the
							 * bucket, because there's no overlap).
							 */
							if (isgt)
								mismatch = !DatumGetBool(FunctionCall2Coll(&opproc,
																		   DEFAULT_COLLATION_OID,
																		   cst->constvalue,
																		   item->values[idx]));
							else
								mismatch = !DatumGetBool(FunctionCall2Coll(&opproc,
																		   DEFAULT_COLLATION_OID,
																		   item->values[idx],
																		   cst->constvalue));

							break;
					}

					/*
					 * XXX The conditions on matches[i] are not needed, as we
					 * skip MCV items that can't become true/false, depending
					 * on the current flag. See beginning of the loop over MCV
					 * items.
					 */

					if ((is_or) && (!mismatch))
					{
						/* OR - was MATCH_NONE, but will be MATCH_FULL */
						matches[i] = STATS_MATCH_FULL;
						continue;
					}
					else if ((!is_or) && mismatch)
					{
						/* AND - was MATC_FULL, but will be MATCH_NONE */
						matches[i] = STATS_MATCH_NONE;
						continue;
					}

				}
			}
		}
		else if (IsA(clause, NullTest))
		{
			NullTest   *expr = (NullTest *) clause;
			Var		   *var = (Var *) (expr->arg);

			/* match the attribute to a dimension of the statistic */
			int			idx = bms_member_index(keys, var->varattno);

			/*
			 * Walk through the MCV items and evaluate the current clause. We
			 * can skip items that were already ruled out, and terminate if
			 * there are no remaining MCV items that might possibly match.
			 */
			for (i = 0; i < mcvlist->nitems; i++)
			{
				char		match = STATS_MATCH_NONE;	/* assume mismatch */
				MCVItem    *item = mcvlist->items[i];

				/* if the clause mismatches the MCV item, set it as MATCH_NONE */
				switch (expr->nulltesttype)
				{
					case IS_NULL:
						match = (item->isnull[idx]) ? STATS_MATCH_FULL : match;
						break;

					case IS_NOT_NULL:
						match = (!item->isnull[idx]) ? STATS_MATCH_FULL : match;
						break;
				}

				/* now, update the match bitmap, depending on OR/AND type */
				if (is_or)
					matches[i] = Max(matches[i], match);
				else
					matches[i] = Min(matches[i], match);
			}
		}
		else if (or_clause(clause) || and_clause(clause))
		{
			/* AND/OR clause, with all subclauses being compatible */

			int			i;
			BoolExpr   *bool_clause = ((BoolExpr *) clause);
			List	   *bool_clauses = bool_clause->args;

			/* match/mismatch bitmap for each MCV item */
			char	   *bool_matches = NULL;

			Assert(bool_clauses != NIL);
			Assert(list_length(bool_clauses) >= 2);

			/* build the match bitmap for the OR-clauses */
			bool_matches = mcv_get_match_bitmap(root, bool_clauses, keys,
												mcvlist, or_clause(clause));

			/*
			 * Merge the bitmap produced by mcv_get_match_bitmap into the
			 * current one. We need to consider if we're evaluating AND or OR
			 * condition when merging the results.
			 */
			for (i = 0; i < mcvlist->nitems; i++)
			{
				/* Is this OR or AND clause? */
				if (is_or)
					matches[i] = Max(matches[i], bool_matches[i]);
				else
					matches[i] = Min(matches[i], bool_matches[i]);
			}

			pfree(bool_matches);
		}
		else if (not_clause(clause))
		{
			/* NOT clause, with all subclauses compatible */

			int			i;
			BoolExpr   *not_clause = ((BoolExpr *) clause);
			List	   *not_args = not_clause->args;

			/* match/mismatch bitmap for each MCV item */
			char	   *not_matches = NULL;

			Assert(not_args != NIL);
			Assert(list_length(not_args) == 1);

			/* build the match bitmap for the NOT-clause */
			not_matches = mcv_get_match_bitmap(root, not_args, keys,
											   mcvlist, false);

			/*
			 * Merge the bitmap produced by mcv_get_match_bitmap into the
			 * current one.
			 */
			for (i = 0; i < mcvlist->nitems; i++)
			{
				/*
				 * When handling a NOT clause, we need to invert the result
				 * before merging it into the global result.
				 */
				if (not_matches[i] == STATS_MATCH_NONE)
					not_matches[i] = STATS_MATCH_FULL;
				else
					not_matches[i] = STATS_MATCH_NONE;

				/* Is this OR or AND clause? */
				if (is_or)
					matches[i] = Max(matches[i], not_matches[i]);
				else
					matches[i] = Min(matches[i], not_matches[i]);
			}

			pfree(not_matches);
		}
		else if (IsA(clause, Var))
		{
			/* Var (has to be a boolean Var, possibly from below NOT) */

			Var		   *var = (Var *) (clause);

			/* match the attribute to a dimension of the statistic */
			int			idx = bms_member_index(keys, var->varattno);

			Assert(var->vartype == BOOLOID);

			/*
			 * Walk through the MCV items and evaluate the current clause. We
			 * can skip items that were already ruled out, and terminate if
			 * there are no remaining MCV items that might possibly match.
			 */
			for (i = 0; i < mcvlist->nitems; i++)
			{
				MCVItem    *item = mcvlist->items[i];
				bool		match = STATS_MATCH_NONE;

				/* if the item is NULL, it's a mismatch */
				if (!item->isnull[idx] && DatumGetBool(item->values[idx]))
					match = STATS_MATCH_FULL;

				/* now, update the match bitmap, depending on OR/AND type */
				if (is_or)
					matches[i] = Max(matches[i], match);
				else
					matches[i] = Min(matches[i], match);
			}
		}
		else
		{
			elog(ERROR, "unknown clause type: %d", clause->type);
		}
	}

	return matches;
}


/*
 * mcv_clauselist_selectivity
 *		Return the selectivity estimate of clauses using MCV list.
 *
 * It also produces two interesting selectivities - total selectivity of
 * all the MCV items combined, and selectivity of the least frequent item
 * in the list.
 */
Selectivity
mcv_clauselist_selectivity(PlannerInfo *root, StatisticExtInfo *stat,
						   List *clauses, int varRelid,
						   JoinType jointype, SpecialJoinInfo *sjinfo,
						   RelOptInfo *rel,
						   Selectivity *basesel, Selectivity *totalsel)
{
	int			i;
	MCVList    *mcv;
	Selectivity s = 0.0;

	/* match/mismatch bitmap for each MCV item */
	char	   *matches = NULL;

	/* load the MCV list stored in the statistics object */
	mcv = statext_mcv_load(stat->statOid);

	/* build a match bitmap for the clauses */
	matches = mcv_get_match_bitmap(root, clauses, stat->keys, mcv, false);

	/* sum frequencies for all the matching MCV items */
	*basesel = 0.0;
	*totalsel = 0.0;
	for (i = 0; i < mcv->nitems; i++)
	{
		*totalsel += mcv->items[i]->frequency;

		if (matches[i] != STATS_MATCH_NONE)
		{
			/* XXX Shouldn't the basesel be outside the if condition? */
			*basesel += mcv->items[i]->base_frequency;
			s += mcv->items[i]->frequency;
		}
	}

	return s;
}
