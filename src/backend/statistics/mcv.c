/*-------------------------------------------------------------------------
 *
 * mcv.c
 *	  POSTGRES multivariate MCV lists
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/statistics/mcv.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

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
 * dimentions (columns) the statistic is defined on. The datum values are
 * stored in a separate array (deduplicated, to minimize the size), and
 * so the serialized items only store uint16 indexes into that array.
 *
 * Each serialized item store (in this order):
 *
 * - indexes to values	  (ndim * sizeof(uint16))
 * - null flags			  (ndim * sizeof(bool))
 * - frequency			  (sizeof(double))
 *
 * So in total each MCV item requires this many bytes:
 *
 *	 ndim * (sizeof(uint16) + sizeof(bool)) + sizeof(double)
 */
#define ITEM_SIZE(ndims)	\
	(ndims * (sizeof(uint16) + sizeof(bool)) + sizeof(double))

/*
 * Macros for convenient access to parts of a serialized MCV item.
 */
#define ITEM_INDEXES(item)			((uint16*)item)
#define ITEM_NULLS(item,ndims)		((bool*)(ITEM_INDEXES(item) + ndims))
#define ITEM_FREQUENCY(item,ndims)	((double*)(ITEM_NULLS(item,ndims) + ndims))


static MultiSortSupport build_mss(VacAttrStats **stats, Bitmapset *attrs);

static SortItem *build_distinct_groups(int numrows, SortItem *items,
					  MultiSortSupport mss, int *ndistinct);

static int count_distinct_groups(int numrows, SortItem *items,
					  MultiSortSupport mss);

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
 * FIXME: Single-dimensional MCV is sorted by frequency (descending). We
 * should do that too, because when walking through the list we want to
 * check the most frequent items first.
 *
 * TODO: We're using Datum (8B), even for data types (e.g. int4 or float4).
 * Maybe we could save some space here, but the bytea compression should
 * handle it just fine.
 *
 * TODO: This probably should not use the ndistinct directly (as computed from
 * the table, but rather estimate the number of distinct values in the
 * table), no?
 */
MCVList *
statext_mcv_build(int numrows, HeapTuple *rows, Bitmapset *attrs,
				 VacAttrStats **stats, HeapTuple **rows_filtered,
				 int *numrows_filtered)
{
	int			i;
	int			numattrs = bms_num_members(attrs);
	int			ndistinct = 0;
	int			mcv_threshold = 0;
	int			numrows_mcv;	/* rows covered by the MCV items */
	int			nitems = 0;

	int		   *attnums = build_attnums(attrs);

	MCVList	   *mcvlist = NULL;

	/* comparator for all the columns */
	MultiSortSupport mss = build_mss(stats, attrs);

	/* sort the rows */
	SortItem   *items = build_sorted_items(numrows, rows, stats[0]->tupDesc,
										   mss, numattrs, attnums);

	/* transform the sorted rows into groups (sorted by frequency) */
	SortItem   *groups = build_distinct_groups(numrows, items, mss, &ndistinct);

	/* Either we have both pointers or none of them. */
	Assert((rows_filtered && numrows_filtered) || (!rows_filtered && !numrows_filtered));

	/*
	 * Determine the minimum size of a group to be eligible for MCV list, and
	 * check how many groups actually pass that threshold. We use 1.25x the
	 * avarage group size, just like for regular per-column statistics.
	 *
	 * XXX We also use a minimum number of 4 rows for mcv_threshold, not sure
	 * if that's what per-column statistics do too?
	 *
	 * But if we can fit all the distinct values in the MCV list (i.e. if
	 * there are less distinct groups than STATS_MCVLIST_MAX_ITEMS), we'll
	 * require only 2 rows per group.
	 *
	 * XXX Maybe this part (requiring 2 rows per group) is not very reliable?
	 * Perhaps we should instead estimate the number of groups the way we
	 * estimate ndistinct (after all, that's what MCV items are), and base our
	 * decision on that?
	 */
	mcv_threshold = 1.25 * numrows / ndistinct;
	mcv_threshold = (mcv_threshold < 4) ? 4 : mcv_threshold;

	if (ndistinct <= STATS_MCVLIST_MAX_ITEMS)
		mcv_threshold = 2;

	/* Walk through the groups and stop once we fall below the threshold. */
	nitems = 0;
	numrows_mcv = 0;
	for (i = 0; i < ndistinct; i++)
	{
		if (groups[i].count < mcv_threshold)
			break;

		numrows_mcv += groups[i].count;
		nitems++;
	}

	/* The MCV can't possibly cover more rows than we sampled. */
	Assert(numrows_mcv <= numrows);

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

		/*
		 * Preallocate Datum/isnull arrays (not as a single chunk, as we will
		 * pass the result outside and thus it needs to be easy to pfree().
		 *
		 * XXX On second thought, we're the only ones dealing with MCV lists,
		 * so we might allocate everything as a single chunk without any risk.
		 * Not sure it's worth it, though.
		 */
		mcvlist->items = (MCVItem **) palloc0(sizeof(MCVItem *) * nitems);

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
			MCVItem	   *item = mcvlist->items[i];

			/* copy values from the _previous_ group (last item of) */
			memcpy(item->values, groups[i].values, sizeof(Datum) * numattrs);
			memcpy(item->isnull, groups[i].isnull, sizeof(bool) * numattrs);

			/* make sure basic assumptions on group size are correct */
			Assert(groups[i].count >= mcv_threshold);
			Assert(groups[i].count <= numrows);

			/* groups should be sorted by frequency in descending order */
			Assert((i == 0) || (groups[i-1].count >= groups[i].count));

			/* and finally the group frequency */
			item->frequency = (double) groups[i].count / numrows;
		}

		/* make sure the loops are consistent */
		Assert(nitems == mcvlist->nitems);
	}

	/* Assume we're not returning any filtered rows by default. */
	if (numrows_filtered)
		*numrows_filtered = 0;

	if (rows_filtered)
		*rows_filtered = NULL;

	/*
	 * Produce an array with only tuples not covered by the MCV list. This
	 * is needed when building MCV+histogram pair, where MCV covers the most
	 * common combinations and histogram covers the remaining part.
	 *
	 * We will first sort the groups by the keys (not by count) and then use
	 * binary search in the group array to check which rows are covered by
	 * the MCV items.
	 *
	 * Do not modify the array in place, as there may be additional stats on
	 * the table and we need to keep the original array for them.
	 *
	 * We only do this when requested by passing non-NULL rows_filtered,
	 * and when there are rows not covered by the MCV list (that is, when
	 * numrows_mcv < numrows), or also (nitems < ndistinct).
	 */
	if (rows_filtered && numrows_filtered && (nitems < ndistinct))
	{
		int		i,
				j;

		/* used to build the filtered array of tuples */
		HeapTuple  *filtered;
		int			nfiltered;

		/* used for the searches */
		SortItem        key;

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

	return mcvlist;
}

/*
 * build_mss
 *	build MultiSortSupport for the attributes passed in attrs
 */
static MultiSortSupport
build_mss(VacAttrStats **stats, Bitmapset *attrs)
{
	int			i, j;
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

		multi_sort_add_dimension(mss, i, type->lt_opr);
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
		if (multi_sort_compare(&items[i], &items[i - 1], mss) != 0)
			ndistinct += 1;

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

	SortItem   *groups = (SortItem *) palloc0(ngroups * sizeof(SortItem));

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
statext_mcv_serialize(MCVList *mcvlist, VacAttrStats **stats)
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
		StdAnalyzeData *tmp = (StdAnalyzeData *) stats[dim]->extra_data;

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

		PrepareSortSupportFromOrderingOp(tmp->ltopr, &ssup[dim]);

		qsort_arg(values[dim], counts[dim], sizeof(Datum),
				  compare_scalars_simple, &ssup[dim]);

		/*
		 * Walk through the array and eliminate duplicate values, but keep the
		 * ordering (so that we can do bsearch later). We know there's at least
		 * one item as (counts[dim] != 0), so we can skip the first element.
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

		/* we must not exceed UINT16_MAX, as we use uint16 indexes */
		Assert(ndistinct <= UINT16_MAX);

		/*
		 * Store additional info about the attribute - number of deduplicated
		 * values, and also size of the serialized data. For fixed-length data
		 * types this is trivial to compute, for varwidth types we need to
		 * actually walk the array and sum the sizes.
		 */
		info[dim].nvalues = ndistinct;

		if (info[dim].typlen > 0) /* fixed-length data types */
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
				info[dim].nbytes += strlen(DatumGetPointer(values[dim][i]));
		}

		/* we know (count>0) so there must be some data */
		Assert(info[dim].nbytes > 0);
	}

	/*
	 * Now we can finally compute how much space we'll actually need for the
	 * whole serialized MCV list, as it contains these fields:
	 *
	 * - length (4B) for varlena
	 * - magic (4B)
	 * - type (4B)
	 * - ndimensions (4B)
	 * - nitems (4B)
	 * - info (ndim * sizeof(DimensionInfo)
	 * - arrays of values for each dimension
	 * - serialized items (nitems * itemsize)
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
		elog(ERROR, "serialized MCV list exceeds 1MB (%ld)", total_length);

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

			if (info[dim].typbyval)		/* passed by value */
			{
				memcpy(data, &v, info[dim].typlen);
				data += info[dim].typlen;
			}
			else if (info[dim].typlen > 0)		/* pased by reference */
			{
				memcpy(data, DatumGetPointer(v), info[dim].typlen);
				data += info[dim].typlen;
			}
			else if (info[dim].typlen == -1)		/* varlena */
			{
				memcpy(data, DatumGetPointer(v), VARSIZE_ANY(v));
				data += VARSIZE_ANY(v);
			}
			else if (info[dim].typlen == -2)		/* cstring */
			{
				memcpy(data, DatumGetPointer(v), strlen(DatumGetPointer(v)) + 1);
				data += strlen(DatumGetPointer(v)) + 1; /* terminator */
			}
		}

		/* check we got exactly the amount of data we expected for this dimension */
		Assert((data - tmp) == info[dim].nbytes);
	}

	/* finally serialize the items, with uint16 indexes instead of the values */
	for (i = 0; i < mcvlist->nitems; i++)
	{
		MCVItem	   *mcvitem = mcvlist->items[i];

		/* don't write beyond the allocated space */
		Assert(data <= (char *) output + total_length - itemsize);

		/* reset the item (we only allocate it once and reuse it) */
		memset(item, 0, itemsize);

		for (dim = 0; dim < ndims; dim++)
		{
			Datum	   *v = NULL;

			/* do the lookup only for non-NULL values */
			if (mcvlist->items[i]->isnull[dim])
				continue;

			v = (Datum *) bsearch_arg(&mcvitem->values[dim], values[dim],
									  info[dim].nvalues, sizeof(Datum),
									  compare_scalars_simple, &ssup[dim]);

			Assert(v != NULL);	/* serialization or deduplication error */

			/* compute index within the array */
			ITEM_INDEXES(item)[dim] = (v - values[dim]);

			/* check the index is within expected bounds */
			Assert(ITEM_INDEXES(item)[dim] >= 0);
			Assert(ITEM_INDEXES(item)[dim] < info[dim].nvalues);
		}

		/* copy NULL and frequency flags into the item */
		memcpy(ITEM_NULLS(item, ndims), mcvitem->isnull, sizeof(bool) * ndims);
		memcpy(ITEM_FREQUENCY(item, ndims), &mcvitem->frequency, sizeof(double));

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
 * there bo be a lot of duplicate values. But perhaps that's not true and we
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
	MCVList	   *mcvlist;
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
	 * We can't possibly deserialize a MCV list if there's not even a
	 * complete header.
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
		elog(ERROR, "invalid MCV magic %d (expected %dd)",
			 mcvlist->magic, STATS_MCV_MAGIC);

	if (mcvlist->type != STATS_MCV_TYPE_BASIC)
		elog(ERROR, "invalid MCV type %d (expected %dd)",
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
	 * Check that we have at least the dimension and info records, along
	 * with the items. We don't know the size of the serialized values yet.
	 * We need to do this check first, before accessing the dimension info.
	 */
	if (VARSIZE_ANY_EXHDR(data) < expected_size)
		elog(ERROR, "invalid MCV size %ld (expected %ld)",
			 VARSIZE_ANY_EXHDR(data), expected_size);

	/* Now it's safe to access the dimention info. */
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
	 * Nowe we know the total expected MCV size, including all the pieces
	 * (header, dimension info. items and deduplicated data). So do the
	 * final check on size.
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
	bufflen = sizeof(Datum **) * ndims;	/* space for top-level pointers */

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
	 *	bytea * data = ... fetch the data from catalog ...
	 *	MCVList mcvlist = deserialize_mcv_list(data);
	 *	pfree(data);
	 *
	 * then 'mcvlist' references the freed memory. Should copy the pieces.
	 */
	for (dim = 0; dim < ndims; dim++)
	{
#ifdef USE_ASSERT_CHECKING
		/* remember where data for this dimension starts */
		char *start = tmp;
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

			/* pased by reference, but fixed length (name, tid, ...) */
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
	rbufflen = (sizeof(MCVItem*) + sizeof(MCVItem) +
				sizeof(Datum) * ndims + sizeof(bool) * ndims) * nitems;

	rbuff = palloc0(rbufflen);
	rptr = rbuff;

	mcvlist->items = (MCVItem **) rbuff;
	rptr += (sizeof(MCVItem *) * nitems);

	/* deserialize the MCV items and translate the indexes to Datums */
	for (i = 0; i < nitems; i++)
	{
		uint16	   *indexes = NULL;
		MCVItem	   *item = (MCVItem *) rptr;

		rptr += (sizeof(MCVItem));

		item->values = (Datum *) rptr;
		rptr += (sizeof(Datum) * ndims);

		item->isnull = (bool *) rptr;
		rptr += (sizeof(bool) * ndims);

		/* just point to the right place */
		indexes = ITEM_INDEXES(tmp);

		memcpy(item->isnull, ITEM_NULLS(tmp, ndims), sizeof(bool) * ndims);
		memcpy(&item->frequency, ITEM_FREQUENCY(tmp, ndims), sizeof(double));

#ifdef ASSERT_CHECKING
		/*
		 * XXX This seems rather useless, considering the 'indexes' array is
		 * defined as (uint16*).
		 */
		for (dim = 0; dim < ndims; dim++)
			Assert(indexes[dim] <= UINT16_MAX);
#endif

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
	TupleDesc	tupdesc;
	AttInMetadata *attinmeta;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		MCVList	   *mcvlist;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		mcvlist = statext_mcv_load(PG_GETARG_OID(0));

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

		/* build metadata needed later to produce tuples from raw C-strings */
		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	attinmeta = funcctx->attinmeta;

	if (call_cntr < max_calls)	/* do when there is more left to send */
	{
		char	  **values;
		HeapTuple	tuple;
		Datum		result;
		int2vector *stakeys;
		Oid			relid;

		char	   *buff = palloc0(1024);
		char	   *format;

		int			i;

		Oid		   *outfuncs;
		FmgrInfo   *fmgrinfo;

		MCVList	   *mcvlist;
		MCVItem	   *item;

		mcvlist = (MCVList *) funcctx->user_fctx;

		Assert(call_cntr < mcvlist->nitems);

		item = mcvlist->items[call_cntr];

		stakeys = find_ext_attnums(PG_GETARG_OID(0), &relid);

		/*
		 * Prepare a values array for building the returned tuple. This should
		 * be an array of C strings which will be processed later by the type
		 * input functions.
		 */
		values = (char **) palloc(4 * sizeof(char *));

		values[0] = (char *) palloc(64 * sizeof(char));

		/* arrays */
		values[1] = (char *) palloc0(1024 * sizeof(char));
		values[2] = (char *) palloc0(1024 * sizeof(char));

		/* frequency */
		values[3] = (char *) palloc(64 * sizeof(char));

		outfuncs = (Oid *) palloc0(sizeof(Oid) * mcvlist->ndimensions);
		fmgrinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * mcvlist->ndimensions);

		for (i = 0; i < mcvlist->ndimensions; i++)
		{
			bool		isvarlena;

			getTypeOutputInfo(get_atttype(relid, stakeys->values[i]),
							  &outfuncs[i], &isvarlena);

			fmgr_info(outfuncs[i], &fmgrinfo[i]);
		}

		snprintf(values[0], 64, "%d", call_cntr);		/* item ID */

		for (i = 0; i < mcvlist->ndimensions; i++)
		{
			Datum		val,
						valout;

			format = "%s, %s";
			if (i == 0)
				format = "{%s%s";
			else if (i == mcvlist->ndimensions - 1)
				format = "%s, %s}";

			if (item->isnull[i])
				valout = CStringGetDatum("NULL");
			else
			{
				val = item->values[i];
				valout = FunctionCall1(&fmgrinfo[i], val);
			}

			snprintf(buff, 1024, format, values[1], DatumGetPointer(valout));
			strncpy(values[1], buff, 1023);
			buff[0] = '\0';

			snprintf(buff, 1024, format, values[2], item->isnull[i] ? "t" : "f");
			strncpy(values[2], buff, 1023);
			buff[0] = '\0';
		}

		snprintf(values[3], 64, "%f", item->frequency); /* frequency */

		/* build a tuple */
		tuple = BuildTupleFromCStrings(attinmeta, values);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(tuple);

		/* clean up (this is not really necessary) */
		pfree(values[0]);
		pfree(values[1]);
		pfree(values[2]);
		pfree(values[3]);

		pfree(values);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else	/* do when there is no more left */
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
 * mcv_update_match_bitmap
 *	Evaluate clauses using the MCV list, and update the match bitmap.
 *
 * A match bitmap keeps match/mismatch status for each MCV item, and we
 * update it based on additional clauses. We also use it to skip items
 * that can't possibly match (e.g. item marked as "mismatch" can't change
 * to "match" when evaluating AND clause list).
 *
 * The function returns the number of items currently marked as 'match', and
 * it also returns two additional pieces of information - a flag indicating
 * whether there was an equality condition for all attributes, and the
 * minimum frequency in the MCV list.
 *
 * XXX Currently the match bitmap uses a char for each MCV item, which is
 * somewhat wasteful as we could do with just a single bit, thus reducing
 * the size to ~1/8. It would also allow us to combine bitmaps simply using
 * & and |, which should be faster than min/max. The bitmaps are fairly
 * small, though (as we cap the MCV list size to 8k items).
 */
static int
mcv_update_match_bitmap(PlannerInfo *root, List *clauses,
						Bitmapset *keys, MCVList *mcvlist,
						int nmatches, char *matches,
						Selectivity *lowsel, bool *fullmatch,
						bool is_or)
{
	int			i;
	ListCell   *l;

	Bitmapset  *eqmatches = NULL;		/* attributes with equality matches */

	/* The bitmap may be partially built. */
	Assert(nmatches >= 0);
	Assert(nmatches <= mcvlist->nitems);
	Assert(clauses != NIL);
	Assert(list_length(clauses) >= 1);
	Assert(mcvlist != NULL);
	Assert(mcvlist->nitems > 0);

	/*
	 * Handle cases where either all MCV items are marked as mismatch (AND),
	 * or match (OR). In those cases additional clauses can't possibly change
	 * match status of any items, so don't waste time by trying.
	 */
	if (((nmatches == 0) && (!is_or)) ||			/* AND-ed clauses */
		((nmatches == mcvlist->nitems) && is_or))	/* OR-ed clauses */
		return nmatches;

	/*
	 * Find the lowest frequency in the MCV list. The MCV list is sorted by
	 * frequency in descending order, so simply get frequency of the the last
	 * MCV item.
	 */
	*lowsel = mcvlist->items[mcvlist->nitems-1]->frequency;

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
		 * Check it still makes sense to continue evaluating the clauses on the
		 * MCV list, just like we did at the very beginning.
		 */
		if (((nmatches == 0) && (!is_or)) ||
			((nmatches == mcvlist->nitems) && is_or))
			break;

		/* Handle the various types of clauses - OpClause, NullTest and AND/OR/NOT */
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

				FmgrInfo	gtproc;
				Var		   *var = (varonleft) ? linitial(expr->args) : lsecond(expr->args);
				Const	   *cst = (varonleft) ? lsecond(expr->args) : linitial(expr->args);
				bool		isgt = (!varonleft);

				TypeCacheEntry *typecache
				= lookup_type_cache(var->vartype, TYPECACHE_GT_OPR);

				/* FIXME proper matching attribute to dimension */
				int			idx = bms_member_index(keys, var->varattno);

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
					MCVItem	   *item = mcvlist->items[i];

					/*
					 * If there are no more matches (AND) or no remaining
					 * unmatched items (OR), we can stop processing this
					 * clause.
					 */
					if (((nmatches == 0) && (!is_or)) ||
						((nmatches == mcvlist->nitems) && is_or))
						break;

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

							/*
							 * We don't care about isgt in equality, because
							 * it does not matter whether it's (var = const)
							 * or (const = var).
							 */
							mismatch = !DatumGetBool(FunctionCall2Coll(&opproc,
													   DEFAULT_COLLATION_OID,
															 cst->constvalue,
														 item->values[idx]));

							if (!mismatch)
								eqmatches = bms_add_member(eqmatches, idx);

							break;

						case F_SCALARLTSEL:		/* column < constant */
						case F_SCALARGTSEL:		/* column > constant */

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

					if ((is_or) && (matches[i] == STATS_MATCH_NONE) && (!mismatch))
					{
						/* OR - was MATCH_NONE, but will be MATCH_FULL */
						matches[i] = STATS_MATCH_FULL;
						++nmatches;
						continue;
					}
					else if ((!is_or) && (matches[i] == STATS_MATCH_FULL) && mismatch)
					{
						/* AND - was MATC_FULL, but will be MATCH_NONE */
						matches[i] = STATS_MATCH_NONE;
						--nmatches;
						continue;
					}

				}
			}
		}
		else if (IsA(clause, NullTest))
		{
			NullTest   *expr = (NullTest *) clause;
			Var		   *var = (Var *) (expr->arg);

			/* FIXME proper matching attribute to dimension */
			int			idx = bms_member_index(keys, var->varattno);

			/*
			 * Walk through the MCV items and evaluate the current clause. We
			 * can skip items that were already ruled out, and terminate if
			 * there are no remaining MCV items that might possibly match.
			 */
			for (i = 0; i < mcvlist->nitems; i++)
			{
				MCVItem	   *item = mcvlist->items[i];

				/*
				 * if there are no more matches, we can stop processing this
				 * clause
				 */
				if (nmatches == 0)
					break;

				/* skip MCV items that were already ruled out */
				if (matches[i] == STATS_MATCH_NONE)
					continue;

				/* if the clause mismatches the MCV item, set it as MATCH_NONE */
				if (((expr->nulltesttype == IS_NULL) && (!item->isnull[idx])) ||
				((expr->nulltesttype == IS_NOT_NULL) && (item->isnull[idx])))
				{
					matches[i] = STATS_MATCH_NONE;
					--nmatches;
				}
			}
		}
		else if (or_clause(clause) || and_clause(clause))
		{
			/*
			 * AND/OR clause, with all clauses compatible with the selected MV
			 * stat
			 */

			int			i;
			BoolExpr   *orclause = ((BoolExpr *) clause);
			List	   *orclauses = orclause->args;

			/* match/mismatch bitmap for each MCV item */
			int			or_nmatches = 0;
			char	   *or_matches = NULL;

			Assert(orclauses != NIL);
			Assert(list_length(orclauses) >= 2);

			/* number of matching MCV items */
			or_nmatches = mcvlist->nitems;

			/* by default none of the MCV items matches the clauses */
			or_matches = palloc0(sizeof(char) * or_nmatches);

			if (or_clause(clause))
			{
				/* OR clauses assume nothing matches, initially */
				memset(or_matches, STATS_MATCH_NONE, sizeof(char) * or_nmatches);
				or_nmatches = 0;
			}
			else
			{
				/* AND clauses assume nothing matches, initially */
				memset(or_matches, STATS_MATCH_FULL, sizeof(char) * or_nmatches);
			}

			/* build the match bitmap for the OR-clauses */
			or_nmatches = mcv_update_match_bitmap(root, orclauses, keys,
										mcvlist, or_nmatches, or_matches,
										lowsel, fullmatch, or_clause(clause));

			/* merge the bitmap into the existing one */
			for (i = 0; i < mcvlist->nitems; i++)
			{
				/*
				 * Merge the result into the bitmap (Min for AND, Max for OR).
				 *
				 * FIXME this does not decrease the number of matches
				 */
				UPDATE_RESULT(matches[i], or_matches[i], is_or);
			}

			pfree(or_matches);

		}
		else
		{
			elog(ERROR, "unknown clause type: %d", clause->type);
		}
	}

	/*
	 * If all the columns were matched by equality, it's a full match. In this
	 * case there can be just a single MCV item, matching the clause (if there
	 * were two, both would match the other one).
	 */
	*fullmatch = (bms_num_members(eqmatches) == mcvlist->ndimensions);

	/* free the allocated pieces */
	if (eqmatches)
		pfree(eqmatches);

	return nmatches;
}

/*
 * mcv_clauselist_selectivity
 *		Return the estimated selectivity of the given clauses using MCV list
 *		statistics, or 1.0 if no useful MCV list statistic exists.
 */
Selectivity
mcv_clauselist_selectivity(PlannerInfo *root, StatisticExtInfo *stat,
						   List *clauses, int varRelid,
						   JoinType jointype, SpecialJoinInfo *sjinfo,
						   RelOptInfo *rel,
						   bool *fullmatch, Selectivity *lowsel)
{
	int			i;
	MCVList	   *mcv;
	Selectivity	s = 0.0;

	/* match/mismatch bitmap for each MCV item */
	char	   *matches = NULL;
	int			nmatches = 0;

	/* load the MCV list stored in the statistics object */
	mcv = statext_mcv_load(stat->statOid);

	/* by default all the MCV items match the clauses fully */
	matches = palloc0(sizeof(char) * mcv->nitems);
	memset(matches, STATS_MATCH_FULL, sizeof(char) * mcv->nitems);

	/* number of matching MCV items */
	nmatches = mcv->nitems;

	nmatches = mcv_update_match_bitmap(root, clauses,
									   stat->keys, mcv,
									   nmatches, matches,
									   lowsel, fullmatch, false);

	/* sum frequencies for all the matching MCV items */
	for (i = 0; i < mcv->nitems; i++)
	{
		if (matches[i] != STATS_MATCH_NONE)
			s += mcv->items[i]->frequency;
	}

	return s;
}
