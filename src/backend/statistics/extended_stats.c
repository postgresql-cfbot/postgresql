/*-------------------------------------------------------------------------
 *
 * extended_stats.c
 *	  POSTGRES extended statistics
 *
 * Generic code supporting statistics objects created via CREATE STATISTICS.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/statistics/extended_stats.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_statistic_ext.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "postmaster/autovacuum.h"
#include "statistics/extended_stats_internal.h"
#include "statistics/statistics.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * Used internally to refer to an individual statistics object, i.e.,
 * a pg_statistic_ext entry.
 */
typedef struct StatExtEntry
{
	Oid			statOid;		/* OID of pg_statistic_ext entry */
	char	   *schema;			/* statistics object's schema */
	char	   *name;			/* statistics object's name */
	Bitmapset  *columns;		/* attribute numbers covered by the object */
	List	   *types;			/* 'char' list of enabled statistic kinds */
} StatExtEntry;


static List *fetch_statentries_for_relation(Relation pg_statext, Oid relid);
static VacAttrStats **lookup_var_attr_stats(Relation rel, Bitmapset *attrs,
					  int nvacatts, VacAttrStats **vacatts);
static void statext_store(Relation pg_stext, Oid relid,
			  MVNDistinct *ndistinct, MVDependencies *dependencies,
			  MCVList *mcvlist, MVHistogram *histogram, VacAttrStats **stats);


/*
 * Compute requested extended stats, using the rows sampled for the plain
 * (single-column) stats.
 *
 * This fetches a list of stats types from pg_statistic_ext, computes the
 * requested stats, and serializes them back into the catalog.
 */
void
BuildRelationExtStatistics(Relation onerel, double totalrows,
						   int numrows, HeapTuple *rows,
						   int natts, VacAttrStats **vacattrstats)
{
	Relation	pg_stext;
	ListCell   *lc;
	List	   *stats;
	MemoryContext cxt;
	MemoryContext oldcxt;

	cxt = AllocSetContextCreate(CurrentMemoryContext, "stats ext",
								ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(cxt);

	pg_stext = heap_open(StatisticExtRelationId, RowExclusiveLock);
	stats = fetch_statentries_for_relation(pg_stext, RelationGetRelid(onerel));

	foreach(lc, stats)
	{
		StatExtEntry *stat = (StatExtEntry *) lfirst(lc);
		MVNDistinct *ndistinct = NULL;
		MVDependencies *dependencies = NULL;
		MVHistogram *histogram = NULL;
		MCVList	   *mcv = NULL;
		VacAttrStats **stats;
		ListCell   *lc2;

		bool		build_mcv = false;
		bool		build_histogram = false;

		/*
		 * Check if we can build these stats based on the column analyzed. If
		 * not, report this fact (except in autovacuum) and move on.
		 */
		stats = lookup_var_attr_stats(onerel, stat->columns,
									  natts, vacattrstats);
		if (!stats && !IsAutoVacuumWorkerProcess())
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("statistics object \"%s.%s\" could not be computed for relation \"%s.%s\"",
							stat->schema, stat->name,
							get_namespace_name(onerel->rd_rel->relnamespace),
							RelationGetRelationName(onerel)),
					 errtable(onerel)));
			continue;
		}

		/* check allowed number of dimensions */
		Assert(bms_num_members(stat->columns) >= 2 &&
			   bms_num_members(stat->columns) <= STATS_MAX_DIMENSIONS);

		/* compute statistic of each requested type */
		foreach(lc2, stat->types)
		{
			char		t = (char) lfirst_int(lc2);

			if (t == STATS_EXT_NDISTINCT)
				ndistinct = statext_ndistinct_build(totalrows, numrows, rows,
													stat->columns, stats);
			else if (t == STATS_EXT_DEPENDENCIES)
				dependencies = statext_dependencies_build(numrows, rows,
														  stat->columns, stats);
			else if (t == STATS_EXT_MCV)
				build_mcv = true;
			else if (t == STATS_EXT_HISTOGRAM)
				build_histogram = true;
		}

		/*
		 * If asked to build both MCV and histogram, first build the MCV part
		 * and then histogram on the remaining rows.
		 */
		if (build_mcv && build_histogram)
		{
			HeapTuple  *rows_filtered = NULL;
			int			numrows_filtered;

			mcv = statext_mcv_build(numrows, rows, stat->columns, stats,
									&rows_filtered, &numrows_filtered);

			/* Only build the histogram when there are rows not covered by MCV. */
			if (rows_filtered)
			{
				Assert(numrows_filtered > 0);

				histogram = statext_histogram_build(numrows_filtered, rows_filtered,
													stat->columns, stats, numrows);

				/* free this immediately, as we may be building many stats */
				pfree(rows_filtered);
			}
		}
		else if (build_mcv)
			mcv = statext_mcv_build(numrows, rows, stat->columns, stats,
									NULL, NULL);
		else if (build_histogram)
			histogram = statext_histogram_build(numrows, rows, stat->columns,
												stats, numrows);

		/* store the statistics in the catalog */
		statext_store(pg_stext, stat->statOid, ndistinct, dependencies, mcv,
					  histogram, stats);
	}

	heap_close(pg_stext, RowExclusiveLock);

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(cxt);
}

/*
 * statext_is_kind_built
 *		Is this stat kind built in the given pg_statistic_ext tuple?
 */
bool
statext_is_kind_built(HeapTuple htup, char type)
{
	AttrNumber	attnum;

	switch (type)
	{
		case STATS_EXT_NDISTINCT:
			attnum = Anum_pg_statistic_ext_stxndistinct;
			break;

		case STATS_EXT_DEPENDENCIES:
			attnum = Anum_pg_statistic_ext_stxdependencies;
			break;

		case STATS_EXT_MCV:
			attnum = Anum_pg_statistic_ext_stxmcv;
			break;

		case STATS_EXT_HISTOGRAM:
			attnum = Anum_pg_statistic_ext_stxhistogram;
			break;

		default:
			elog(ERROR, "unexpected statistics type requested: %d", type);
	}

	return !heap_attisnull(htup, attnum);
}

/*
 * Return a list (of StatExtEntry) of statistics objects for the given relation.
 */
static List *
fetch_statentries_for_relation(Relation pg_statext, Oid relid)
{
	SysScanDesc scan;
	ScanKeyData skey;
	HeapTuple	htup;
	List	   *result = NIL;

	/*
	 * Prepare to scan pg_statistic_ext for entries having stxrelid = this
	 * rel.
	 */
	ScanKeyInit(&skey,
				Anum_pg_statistic_ext_stxrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(pg_statext, StatisticExtRelidIndexId, true,
							  NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(scan)))
	{
		StatExtEntry *entry;
		Datum		datum;
		bool		isnull;
		int			i;
		ArrayType  *arr;
		char	   *enabled;
		Form_pg_statistic_ext staForm;

		entry = palloc0(sizeof(StatExtEntry));
		entry->statOid = HeapTupleGetOid(htup);
		staForm = (Form_pg_statistic_ext) GETSTRUCT(htup);
		entry->schema = get_namespace_name(staForm->stxnamespace);
		entry->name = pstrdup(NameStr(staForm->stxname));
		for (i = 0; i < staForm->stxkeys.dim1; i++)
		{
			entry->columns = bms_add_member(entry->columns,
											staForm->stxkeys.values[i]);
		}

		/* decode the stxkind char array into a list of chars */
		datum = SysCacheGetAttr(STATEXTOID, htup,
								Anum_pg_statistic_ext_stxkind, &isnull);
		Assert(!isnull);
		arr = DatumGetArrayTypeP(datum);
		if (ARR_NDIM(arr) != 1 ||
			ARR_HASNULL(arr) ||
			ARR_ELEMTYPE(arr) != CHAROID)
			elog(ERROR, "stxkind is not a 1-D char array");
		enabled = (char *) ARR_DATA_PTR(arr);
		for (i = 0; i < ARR_DIMS(arr)[0]; i++)
		{
			Assert((enabled[i] == STATS_EXT_NDISTINCT) ||
				   (enabled[i] == STATS_EXT_DEPENDENCIES) ||
				   (enabled[i] == STATS_EXT_MCV) ||
				   (enabled[i] == STATS_EXT_HISTOGRAM));
			entry->types = lappend_int(entry->types, (int) enabled[i]);
		}

		result = lappend(result, entry);
	}

	systable_endscan(scan);

	return result;
}

/*
 * Using 'vacatts' of size 'nvacatts' as input data, return a newly built
 * VacAttrStats array which includes only the items corresponding to
 * attributes indicated by 'stxkeys'. If we don't have all of the per column
 * stats available to compute the extended stats, then we return NULL to indicate
 * to the caller that the stats should not be built.
 */
static VacAttrStats **
lookup_var_attr_stats(Relation rel, Bitmapset *attrs,
					  int nvacatts, VacAttrStats **vacatts)
{
	int			i = 0;
	int			x = -1;
	VacAttrStats **stats;

	stats = (VacAttrStats **)
		palloc(bms_num_members(attrs) * sizeof(VacAttrStats *));

	/* lookup VacAttrStats info for the requested columns (same attnum) */
	while ((x = bms_next_member(attrs, x)) >= 0)
	{
		int			j;

		stats[i] = NULL;
		for (j = 0; j < nvacatts; j++)
		{
			if (x == vacatts[j]->tupattnum)
			{
				stats[i] = vacatts[j];
				break;
			}
		}

		if (!stats[i])
		{
			/*
			 * Looks like stats were not gathered for one of the columns
			 * required. We'll be unable to build the extended stats without
			 * this column.
			 */
			pfree(stats);
			return NULL;
		}

		/*
		 * Sanity check that the column is not dropped - stats should have
		 * been removed in this case.
		 */
		Assert(!stats[i]->attr->attisdropped);

		i++;
	}

	return stats;
}

/*
 * Find attnums of MV stats using the mvoid.
 */
int2vector *
find_ext_attnums(Oid mvoid, Oid *relid)
{
	ArrayType  *arr;
	Datum       adatum;
	bool        isnull;
	HeapTuple   htup;
	int2vector *keys;

	/* Prepare to scan pg_statistic_ext for entries having indrelid = this rel. */
	htup = SearchSysCache1(STATEXTOID,
						   ObjectIdGetDatum(mvoid));

	/* XXX syscache contains OIDs of deleted stats (not invalidated) */
	if (!HeapTupleIsValid(htup))
		return NULL;

	/* starelid */
	adatum = SysCacheGetAttr(STATEXTOID, htup,
							 Anum_pg_statistic_ext_stxrelid, &isnull);
	Assert(!isnull);

	*relid = DatumGetObjectId(adatum);

	/* stakeys */
	adatum = SysCacheGetAttr(STATEXTOID, htup,
							 Anum_pg_statistic_ext_stxkeys, &isnull);
	Assert(!isnull);

	arr = DatumGetArrayTypeP(adatum);

	keys = buildint2vector((int16 *) ARR_DATA_PTR(arr),
						   ARR_DIMS(arr)[0]);
	ReleaseSysCache(htup);

	/*
	 * TODO maybe save the list into relcache, as in RelationGetIndexList
	 * (which was used as an inspiration of this one)?.
	 */

	return keys;
}

/*
 * statext_store
 *	Serializes the statistics and stores them into the pg_statistic_ext tuple.
 */
static void
statext_store(Relation pg_stext, Oid statOid,
			  MVNDistinct *ndistinct, MVDependencies *dependencies,
			  MCVList *mcv, MVHistogram *histogram, VacAttrStats **stats)
{
	HeapTuple	stup,
				oldtup;
	Datum		values[Natts_pg_statistic_ext];
	bool		nulls[Natts_pg_statistic_ext];
	bool		replaces[Natts_pg_statistic_ext];

	memset(nulls, 1, Natts_pg_statistic_ext * sizeof(bool));
	memset(replaces, 0, Natts_pg_statistic_ext * sizeof(bool));
	memset(values, 0, Natts_pg_statistic_ext * sizeof(Datum));

	/*
	 * Construct a new pg_statistic_ext tuple, replacing the calculated stats.
	 */
	if (ndistinct != NULL)
	{
		bytea	   *data = statext_ndistinct_serialize(ndistinct);

		nulls[Anum_pg_statistic_ext_stxndistinct - 1] = (data == NULL);
		values[Anum_pg_statistic_ext_stxndistinct - 1] = PointerGetDatum(data);
	}

	if (dependencies != NULL)
	{
		bytea	   *data = statext_dependencies_serialize(dependencies);

		nulls[Anum_pg_statistic_ext_stxdependencies - 1] = (data == NULL);
		values[Anum_pg_statistic_ext_stxdependencies - 1] = PointerGetDatum(data);
	}

	if (mcv != NULL)
	{
		bytea	   *data = statext_mcv_serialize(mcv, stats);

		nulls[Anum_pg_statistic_ext_stxmcv - 1] = (data == NULL);
		values[Anum_pg_statistic_ext_stxmcv - 1] = PointerGetDatum(data);
	}

	if (histogram != NULL)
	{
		bytea	   *data = statext_histogram_serialize(histogram, stats);

		nulls[Anum_pg_statistic_ext_stxhistogram - 1] = (data == NULL);
		values[Anum_pg_statistic_ext_stxhistogram - 1] = PointerGetDatum(data);
	}

	/* always replace the value (either by bytea or NULL) */
	replaces[Anum_pg_statistic_ext_stxndistinct - 1] = true;
	replaces[Anum_pg_statistic_ext_stxdependencies - 1] = true;
	replaces[Anum_pg_statistic_ext_stxmcv - 1] = true;
	replaces[Anum_pg_statistic_ext_stxhistogram - 1] = true;

	/* there should already be a pg_statistic_ext tuple */
	oldtup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(statOid));
	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "cache lookup failed for statistics object %u", statOid);

	/* replace it */
	stup = heap_modify_tuple(oldtup,
							 RelationGetDescr(pg_stext),
							 values,
							 nulls,
							 replaces);
	ReleaseSysCache(oldtup);
	CatalogTupleUpdate(pg_stext, &stup->t_self, stup);

	heap_freetuple(stup);
}

/* initialize multi-dimensional sort */
MultiSortSupport
multi_sort_init(int ndims)
{
	MultiSortSupport mss;

	Assert(ndims >= 2);

	mss = (MultiSortSupport) palloc0(offsetof(MultiSortSupportData, ssup)
									 + sizeof(SortSupportData) * ndims);

	mss->ndims = ndims;

	return mss;
}

/*
 * Prepare sort support info using the given sort operator
 * at the position 'sortdim'
 */
void
multi_sort_add_dimension(MultiSortSupport mss, int sortdim, Oid oper)
{
	SortSupport ssup = &mss->ssup[sortdim];

	ssup->ssup_cxt = CurrentMemoryContext;
	ssup->ssup_collation = DEFAULT_COLLATION_OID;
	ssup->ssup_nulls_first = false;
	ssup->ssup_cxt = CurrentMemoryContext;

	PrepareSortSupportFromOrderingOp(oper, ssup);
}

/* compare all the dimensions in the selected order */
int
multi_sort_compare(const void *a, const void *b, void *arg)
{
	MultiSortSupport mss = (MultiSortSupport) arg;
	SortItem   *ia = (SortItem *) a;
	SortItem   *ib = (SortItem *) b;
	int			i;

	for (i = 0; i < mss->ndims; i++)
	{
		int			compare;

		compare = ApplySortComparator(ia->values[i], ia->isnull[i],
									  ib->values[i], ib->isnull[i],
									  &mss->ssup[i]);

		if (compare != 0)
			return compare;
	}

	/* equal by default */
	return 0;
}

/* compare selected dimension */
int
multi_sort_compare_dim(int dim, const SortItem *a, const SortItem *b,
					   MultiSortSupport mss)
{
	return ApplySortComparator(a->values[dim], a->isnull[dim],
							   b->values[dim], b->isnull[dim],
							   &mss->ssup[dim]);
}

int
multi_sort_compare_dims(int start, int end,
						const SortItem *a, const SortItem *b,
						MultiSortSupport mss)
{
	int			dim;

	for (dim = start; dim <= end; dim++)
	{
		int			r = ApplySortComparator(a->values[dim], a->isnull[dim],
											b->values[dim], b->isnull[dim],
											&mss->ssup[dim]);

		if (r != 0)
			return r;
	}

	return 0;
}

int
compare_scalars_simple(const void *a, const void *b, void *arg)
{
	return compare_datums_simple(*(Datum *) a,
								 *(Datum *) b,
								 (SortSupport) arg);
}

/*
 * qsort_arg comparator for sorting data when partitioning a MV bucket
 */
int
compare_scalars_partition(const void *a, const void *b, void *arg)
{
	Datum		da = ((ScalarItem *) a)->value;
	Datum		db = ((ScalarItem *) b)->value;
	SortSupport ssup = (SortSupport) arg;

	return ApplySortComparator(da, false, db, false, ssup);
}

int
compare_datums_simple(Datum a, Datum b, SortSupport ssup)
{
	return ApplySortComparator(a, false, b, false, ssup);
}

/* simple counterpart to qsort_arg */
void *
bsearch_arg(const void *key, const void *base, size_t nmemb, size_t size,
			int (*compar) (const void *, const void *, void *),
			void *arg)
{
	size_t		l,
				u,
				idx;
	const void *p;
	int			comparison;

	l = 0;
	u = nmemb;
	while (l < u)
	{
		idx = (l + u) / 2;
		p = (void *) (((const char *) base) + (idx * size));
		comparison = (*compar) (key, p, arg);

		if (comparison < 0)
			u = idx;
		else if (comparison > 0)
			l = idx + 1;
		else
			return (void *) p;
	}

	return NULL;
}

int *
build_attnums(Bitmapset *attrs)
{
	int		i,
			j;
	int		numattrs = bms_num_members(attrs);
	int	   *attnums;

	/* build attnums from the bitmapset */
	attnums = (int*)palloc(sizeof(int) * numattrs);
	i = 0;
	j = -1;
	while ((j = bms_next_member(attrs, j)) >= 0)
		attnums[i++] = j;

	return attnums;
}

/* build_sorted_items
 * 	build sorted array of SortItem with values from rows
 *
 * XXX All the memory is allocated in a single chunk, so that the caller
 * can simply pfree the return value to release all of it.
 */
SortItem *
build_sorted_items(int numrows, HeapTuple *rows, TupleDesc tdesc,
				   MultiSortSupport mss, int numattrs, int *attnums)
{
	int			i,
				j,
				len;
	int			nvalues = numrows * numattrs;

	/*
	 * We won't allocate the arrays for each item independenly, but in one
	 * large chunk and then just set the pointers. This allows the caller
	 * to simply pfree the return value to release all the memory.
	 */
	SortItem   *items;
	Datum	   *values;
	bool	   *isnull;
	char	   *ptr;

	/* Compute the total amount of memory we need (both items and values). */
	len = numrows * sizeof(SortItem) + nvalues * (sizeof(Datum) + sizeof(bool));

	/* Allocate the memory and split it into the pieces. */
	ptr = palloc0(len);

	/* items to sort */
	items = (SortItem *) ptr;
	ptr += numrows * sizeof(SortItem);

	/* values and null flags */
	values = (Datum *) ptr;
	ptr += nvalues * sizeof(Datum);

	isnull = (bool *) ptr;
	ptr += nvalues * sizeof(bool);

	/* make sure we consumed the whole buffer exactly */
	Assert((ptr - (char *) items) == len);

	/* fix the pointers to Datum and bool arrays */
	for (i = 0; i < numrows; i++)
	{
		items[i].values = &values[i * numattrs];
		items[i].isnull = &isnull[i * numattrs];

		/* load the values/null flags from sample rows */
		for (j = 0; j < numattrs; j++)
		{
			items[i].values[j] = heap_getattr(rows[i],
											  attnums[j], /* attnum */
											  tdesc,
											  &items[i].isnull[j]);		/* isnull */
		}
	}

	/* do the sort, using the multi-sort */
	qsort_arg((void *) items, numrows, sizeof(SortItem),
			  multi_sort_compare, mss);

	return items;
}

/*
 * has_stats_of_kind
 *		Check whether the list contains statistic of a given kind (at least
 * one of those specified statistics types).
 */
bool
has_stats_of_kind(List *stats, int requiredkinds)
{
	ListCell   *l;

	foreach(l, stats)
	{
		StatisticExtInfo *stat = (StatisticExtInfo *) lfirst(l);

		if (stat->kinds & requiredkinds)
			return true;
	}

	return false;
}

/*
 * choose_best_statistics
 *		Look for and return statistics with the specified 'requiredkind' which
 *		have keys that match at least two of the given attnums.  Return NULL if
 *		there's no match.
 *
 * The current selection criteria is very simple - we choose the statistics
 * object referencing the most of the requested attributes, breaking ties
 * in favor of objects with fewer keys overall.
 *
 * XXX if multiple statistics objects tie on both criteria, then which object
 * is chosen depends on the order that they appear in the stats list. Perhaps
 * further tiebreakers are needed.
 */
StatisticExtInfo *
choose_best_statistics(List *stats, Bitmapset *attnums, int requiredkinds)
{
	ListCell   *lc;
	StatisticExtInfo *best_match = NULL;
	int			best_num_matched = 2;	/* goal #1: maximize */
	int			best_match_keys = (STATS_MAX_DIMENSIONS + 1);	/* goal #2: minimize */

	foreach(lc, stats)
	{
		StatisticExtInfo *info = (StatisticExtInfo *) lfirst(lc);
		int			num_matched;
		int			numkeys;
		Bitmapset  *matched;

		/* skip statistics that do not match any of the requested types */
		if ((info->kinds & requiredkinds) == 0)
			continue;

		/* determine how many attributes of these stats can be matched to */
		matched = bms_intersect(attnums, info->keys);
		num_matched = bms_num_members(matched);
		bms_free(matched);

		/*
		 * save the actual number of keys in the stats so that we can choose
		 * the narrowest stats with the most matching keys.
		 */
		numkeys = bms_num_members(info->keys);

		/*
		 * Use this object when it increases the number of matched clauses or
		 * when it matches the same number of attributes but these stats have
		 * fewer keys than any previous match.
		 */
		if (num_matched > best_num_matched ||
			(num_matched == best_num_matched && numkeys < best_match_keys))
		{
			best_match = info;
			best_num_matched = num_matched;
			best_match_keys = numkeys;
		}
	}

	return best_match;
}

int
bms_member_index(Bitmapset *keys, AttrNumber varattno)
{
	int	i, j;

	i = -1;
	j = 0;
	while (((i = bms_next_member(keys, i)) >= 0) && (i < varattno))
		j += 1;

	return j;
}

/*
 * statext_is_compatible_clause_internal
 *	Does the heavy lifting of actually inspecting the clauses for
 * statext_is_compatible_clause.
 */
static bool
statext_is_compatible_clause_internal(Node *clause, Index relid, Bitmapset **attnums)
{
	/* We only support plain Vars for now */
	if (IsA(clause, Var))
	{
		Var *var = (Var *) clause;

		/* Ensure var is from the correct relation */
		if (var->varno != relid)
			return false;

		/* we also better ensure the Var is from the current level */
		if (var->varlevelsup > 0)
			return false;

		/* Also skip system attributes (we don't allow stats on those). */
		if (!AttrNumberIsForUserDefinedAttr(var->varattno))
			return false;

		*attnums = bms_add_member(*attnums, var->varattno);

		return true;
	}

	/* Var = Const */
	if (is_opclause(clause))
	{
		OpExpr	   *expr = (OpExpr *) clause;
		Var		   *var;
		bool		varonleft = true;
		bool		ok;

		/* Only expressions with two arguments are considered compatible. */
		if (list_length(expr->args) != 2)
			return false;

		/* see if it actually has the right */
		ok = (NumRelids((Node *) expr) == 1) &&
			(is_pseudo_constant_clause(lsecond(expr->args)) ||
			 (varonleft = false,
			  is_pseudo_constant_clause(linitial(expr->args))));

		/* unsupported structure (two variables or so) */
		if (!ok)
			return false;

		/*
		 * If it's not one of the supported operators ("=", "<", ">", etc.),
		 * just ignore the clause, as it's not compatible with MCV lists.
		 *
		 * This uses the function for estimating selectivity, not the operator
		 * directly (a bit awkward, but well ...).
		 */
		if ((get_oprrest(expr->opno) != F_EQSEL) &&
			(get_oprrest(expr->opno) != F_SCALARLTSEL) &&
			(get_oprrest(expr->opno) != F_SCALARGTSEL))
			return false;

		var = (varonleft) ? linitial(expr->args) : lsecond(expr->args);

		return statext_is_compatible_clause_internal((Node *)var, relid, attnums);
	}

	/* NOT clause, clause AND/OR clause */
	if (or_clause(clause) ||
		and_clause(clause) ||
		not_clause(clause))
	{
		/*
		 * AND/OR/NOT-clauses are supported if all sub-clauses are supported
		 *
		 * TODO: We might support mixed case, where some of the clauses are
		 * supported and some are not, and treat all supported subclauses as a
		 * single clause, compute it's selectivity using mv stats, and compute
		 * the total selectivity using the current algorithm.
		 *
		 * TODO: For RestrictInfo above an OR-clause, we might use the
		 * orclause with nested RestrictInfo - we won't have to call
		 * pull_varnos() for each clause, saving time.
		 */
		BoolExpr   *expr = (BoolExpr *) clause;
		ListCell   *lc;
		Bitmapset  *clause_attnums = NULL;

		foreach(lc, expr->args)
		{
			/*
			 * Had we found incompatible clause in the arguments, treat the
			 * whole clause as incompatible.
			 */
			if (!statext_is_compatible_clause_internal((Node *) lfirst(lc),
												   relid, &clause_attnums))
				return false;
		}

		/*
		 * Otherwise the clause is compatible, and we need to merge the
		 * attnums into the main bitmapset.
		 */
		*attnums = bms_join(*attnums, clause_attnums);

		return true;
	}

	/* Var IS NULL */
	if (IsA(clause, NullTest))
	{
		NullTest   *nt = (NullTest *) clause;

		/*
		 * Only simple (Var IS NULL) expressions supported for now. Maybe we
		 * could use examine_variable to fix this?
		 */
		if (!IsA(nt->arg, Var))
			return false;

		return statext_is_compatible_clause_internal((Node *) (nt->arg), relid, attnums);
	}

	return false;
}

/*
 * statext_is_compatible_clause
 *		Determines if the clause is compatible with MCV lists and histograms
 *
 * Only OpExprs with two arguments using an equality operator are supported.
 * When returning True attnum is set to the attribute number of the Var within
 * the supported clause.
 *
 * Currently we only support Var = Const, or Const = Var. It may be possible
 * to expand on this later.
 */
static bool
statext_is_compatible_clause(Node *clause, Index relid, Bitmapset **attnums)
{
	RestrictInfo *rinfo = (RestrictInfo *) clause;

	if (!IsA(rinfo, RestrictInfo))
		return false;

	/* Pseudoconstants are not really interesting here. */
	if (rinfo->pseudoconstant)
		return false;

	/* clauses referencing multiple varnos are incompatible */
	if (bms_membership(rinfo->clause_relids) != BMS_SINGLETON)
		return false;

	return statext_is_compatible_clause_internal((Node *)rinfo->clause,
												 relid, attnums);
}

Selectivity
statext_clauselist_selectivity(PlannerInfo *root, List *clauses, int varRelid,
							   JoinType jointype, SpecialJoinInfo *sjinfo,
							   RelOptInfo *rel, Bitmapset **estimatedclauses)
{
	ListCell   *l;
	Bitmapset  *clauses_attnums = NULL;
	Bitmapset **list_attnums;
	int			listidx;
	StatisticExtInfo *stat;
	List	   *stat_clauses;

	/* selectivities for MCV and histogram part */
	Selectivity	s1 = 0.0,
				s2 = 0.0;

	/* we're interested in MCV lists and/or histograms */
	int			types = (STATS_EXT_INFO_MCV | STATS_EXT_INFO_HISTOGRAM);

	/* additional information for MCV matching */
	bool		fullmatch = false;
	Selectivity	mcv_lowsel = 1.0;

	/* check if there's any stats that might be useful for us. */
	if (!has_stats_of_kind(rel->statlist, types))
		return (Selectivity)1.0;

	list_attnums = (Bitmapset **) palloc(sizeof(Bitmapset *) *
										 list_length(clauses));

	/*
	 * Pre-process the clauses list to extract the attnums seen in each item.
	 * We need to determine if there's any clauses which will be useful for
	 * dependency selectivity estimations. Along the way we'll record all of
	 * the attnums for each clause in a list which we'll reference later so we
	 * don't need to repeat the same work again. We'll also keep track of all
	 * attnums seen.
	 *
	 * FIXME Should skip already estimated clauses (using the estimatedclauses
	 * bitmap).
	 */
	listidx = 0;
	foreach(l, clauses)
	{
		Node	   *clause = (Node *) lfirst(l);
		Bitmapset  *attnums = NULL;

		if (statext_is_compatible_clause(clause, rel->relid, &attnums))
		{
			list_attnums[listidx] = attnums;
			clauses_attnums = bms_add_members(clauses_attnums, attnums);
		}
		else
			list_attnums[listidx] = NULL;

		listidx++;
	}

	/* We need at least two attributes for MCV lists. */
	if (bms_num_members(clauses_attnums) < 2)
		return 1.0;

	/* find the best suited statistics object for these attnums */
	stat = choose_best_statistics(rel->statlist, clauses_attnums, types);

	/* if no matching stats could be found then we've nothing to do */
	if (!stat)
		return (Selectivity)1.0;

	/* now filter the clauses to be estimated using the selected MCV */
	stat_clauses = NIL;

	listidx = 0;
	foreach (l, clauses)
	{
		/*
		 * If the clause is compatible with the selected statistics,
		 * mark it as estimated and add it to the list to estimate.
		 */
		if ((list_attnums[listidx] != NULL) &&
			(bms_is_subset(list_attnums[listidx], stat->keys)))
		{
			stat_clauses = lappend(stat_clauses, (Node *)lfirst(l));
			*estimatedclauses = bms_add_member(*estimatedclauses, listidx);
		}

		listidx++;
	}

	/*
	 * Evaluate the MCV selectivity. See if we got a full match and the
	 * minimal selectivity.
	 */
	if (stat->kinds & STATS_EXT_INFO_MCV)
		s1 = mcv_clauselist_selectivity(root, stat, clauses, varRelid,
										jointype, sjinfo, rel,
										&fullmatch, &mcv_lowsel);

	/*
	 * If we got a full equality match on the MCV list, we're done (and the
	 * estimate is likely pretty good).
	 */
	if (fullmatch && (s1 > 0.0))
		return s1;

	/*
	 * If it's a full match (equalities on all columns) but we haven't
	 * found it in the MCV, then we limit the selectivity by frequency
	 * of the last MCV item. Otherwise reset it to 1.0.
	 */
	if (!fullmatch)
		mcv_lowsel = 1.0;

	/* Now estimate the selectivity from a histogram. */
	if (stat->kinds & STATS_EXT_INFO_HISTOGRAM)
		s2 = histogram_clauselist_selectivity(root, stat, clauses, varRelid,
											  jointype, sjinfo, rel);

	return Min(s1 + s2, mcv_lowsel);
}
