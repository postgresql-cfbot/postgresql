/*-------------------------------------------------------------------------
 * statistics.c
 *
 *	  POSTGRES statistics import
 *
 * Code supporting the direct importation of relation statistics, similar to
 * what is done by the ANALYZE command.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *       src/backend/statistics/statistics.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "catalog/pg_database.h"
#include "catalog/pg_operator.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "statistics/statistics.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rangetypes.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

/*
 * A role has privileges to vacuum or analyze the relation if any of the
 * following are true:
 *   - the role owns the current database and the relation is not shared
 *   - the role has the MAINTAIN privilege on the relation
 *
 */
static bool
canModifyRelation(Oid relid, Form_pg_class reltuple)
{
	return ((object_ownercheck(DatabaseRelationId, MyDatabaseId, GetUserId())
			 && !reltuple->relisshared) ||
			pg_class_aclcheck(relid, GetUserId(), ACL_MAINTAIN) == ACLCHECK_OK);
}

/*
 * Set statistics for a given pg_class entry.
 *
 * This does an in-place (i.e. non-transactional) update of pg_class, just as
 * is done in ANALYZE.
 *
 */
Datum
pg_set_relation_stats(PG_FUNCTION_ARGS)
{
	enum
	{
		P_RELATION = 0,			/* oid */
		P_RELPAGES,				/* int */
		P_RELTUPLES,			/* float4 */
		P_RELALLVISIBLE,		/* int */
		P_NUM_PARAMS
	};

	const char *param_names[] = {
		"relation",
		"relpages",
		"reltuples",
		"relallvisible"
	};

	Oid			relid;
	Relation	rel;
	HeapTuple	ctup;
	Form_pg_class pgcform;
	float4		reltuples;
	int			relpages;
	int			relallvisible;

	/* Any NULL parameter is an error */
	for (int i = P_RELATION; i < P_NUM_PARAMS; i++)
		if (PG_ARGISNULL(i))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s cannot be NULL", param_names[i])));

	relid = PG_GETARG_OID(P_RELATION);

	/*
	 * Open the relation, getting ShareUpdateExclusiveLock to ensure that no
	 * other stat-setting operation can run on it concurrently.
	 */
	rel = table_open(RelationRelationId, ShareUpdateExclusiveLock);

	ctup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(ctup))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("pg_class entry for relid %u not found", relid)));

	pgcform = (Form_pg_class) GETSTRUCT(ctup);

	if (!canModifyRelation(relid, pgcform))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for relation %s",
						RelationGetRelationName(rel))));


	relpages = PG_GETARG_INT32(P_RELPAGES);
	reltuples = PG_GETARG_FLOAT4(P_RELTUPLES);
	relallvisible = PG_GETARG_INT32(P_RELALLVISIBLE);

	/* Only update pg_class if there is a meaningful change */
	if ((pgcform->reltuples != reltuples)
		|| (pgcform->relpages != relpages)
		|| (pgcform->relallvisible != relallvisible))
	{
		pgcform->relpages = PG_GETARG_INT32(P_RELPAGES);
		pgcform->reltuples = PG_GETARG_FLOAT4(P_RELTUPLES);
		pgcform->relallvisible = PG_GETARG_INT32(P_RELALLVISIBLE);

		heap_inplace_update(rel, ctup);
	}

	table_close(rel, NoLock);

	PG_RETURN_VOID();
}

/*
 * Perform the cast of text to some array type
 */
static Datum
cast_stavalue(FmgrInfo *finfo, Datum d, Oid typid, int32 typmod)
{
	char	   *s = TextDatumGetCString(d);
	Datum		out = FunctionCall3(finfo, CStringGetDatum(s),
									ObjectIdGetDatum(typid),
									Int32GetDatum(typmod));

	pfree(s);

	return out;
}

/*
 * Convenience routine to handle a common pattern where two function
 * parameters must either both be NULL or both NOT NULL.
 */
static bool
has_arg_pair(FunctionCallInfo fcinfo, const char **pnames, int p1, int p2)
{
	/* if on param is NULL and the other NOT NULL, report an error */
	if (PG_ARGISNULL(p1) != PG_ARGISNULL(p2))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s cannot be NULL when %s is NOT NULL",
						pnames[(PG_ARGISNULL(p1)) ? p1 : p2],
						pnames[(PG_ARGISNULL(p1)) ? p2 : p1])));

	return (!PG_ARGISNULL(p1));
}

/*
 * Test if the type is a scalar for MCELM purposes
 */
static bool
type_is_scalar(Oid typid)
{
	HeapTuple	tp;
	bool		result = false;

	tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);

		result = (!OidIsValid(typtup->typanalyze));
		ReleaseSysCache(tp);
	}
	return result;
}

static int
value_array_len(ExpandedArrayHeader *arr, const char *name)
{
	if (arr->ndims != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s cannot be a multidimensional array", name)));

	return arr->dims[0];
}

/*
 * Convenience routine to encapsulate all of the steps needed for any
 * value array.
 */
static int
value_not_null_array_len(ExpandedArrayHeader *arr, const char *name)
{
	const int	nelems = value_array_len(arr, name);

	if (nelems > 0)
	{
		deconstruct_expanded_array(arr);

		/* if there's a nulls array, all values must be false */
		if (arr->dnulls != NULL)
			for (int i = 0; i < nelems; i++)
				if (arr->dnulls[i])
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("%s array cannot contain NULL values", name)));
	}

	return nelems;
}

/*
 * Import statistics for a given relation attribute.
 *
 * This will insert/replace a row in pg_statistic for the given relation and
 * attribute name.
 *
 * The function takes input parameters that correspond to columns in the view
 * pg_stats.
 *
 * Of those, the columns attname, inherited, null_frac, avg_width, and
 * n_distinct all correspond to NOT NULL columns in pg_statistic. These
 * parameters have no default value and passing NULL to them will result
 * in an error.
 *
 * If there is no attribute with a matching attname in the relation, the
 * function will raise an error. Likewise for setting inherited statistics
 * on a table that is not partitioned.
 *
 * The remaining parameters all belong to a specific stakind. Some stakinds
 * have multiple parameters, and in those cases both parameters must be
 * NOT NULL or both NULL, otherwise an error will be raised.
 *
 * Omitting a parameter or explicitly passing NULL means that that particular
 * stakind is not associated with the attribute.
 *
 * Parameters that are NOT NULL will be inspected for consistency checks,
 * any of which can raise an error.
 *
 * Parameters corresponding to ANYARRAY columns are instead passed in as text
 * values, which is a valid input string for an array of the type or basetype
 * of the attribute. Any error generated by the array_in() function will in
 * turn fail the function.
 *
 * XXX this is a very looooong function, maybe split that into smaller parts
 * that do different parts (e.g. checks vs. actual update).
 */
Datum
pg_set_attribute_stats(PG_FUNCTION_ARGS)
{
	/* XXX this would really deserve some comment that the order matters,
	 * and so on. It took me ages to realize it's also the index of the
	 * attribute in function arguments. */
	enum
	{
		P_RELATION = 0,			/* oid */
		P_ATTNAME,				/* name */
		P_INHERITED,			/* bool */
		P_NULL_FRAC,			/* float4 */
		P_AVG_WIDTH,			/* int32 */
		P_N_DISTINCT,			/* float4 */
		P_MC_VALS,				/* text, null */
		P_MC_FREQS,				/* float4[], null */
		P_HIST_BOUNDS,			/* text, null */
		P_CORRELATION,			/* float4, null */
		P_MC_ELEMS,				/* text, null */
		P_MC_ELEM_FREQS,		/* float4[], null */
		P_ELEM_COUNT_HIST,		/* float4[], null */
		P_RANGE_LENGTH_HIST,	/* text, null */
		P_RANGE_EMPTY_FRAC,		/* float4, null */
		P_RANGE_BOUNDS_HIST,	/* text, null */
		P_NUM_PARAMS
	};

	/* names of columns that cannot be null */
	/* XXX surely many of those can be NULL? say, MCV or elem fields */
	const char *param_names[] = {
		"relation",
		"attname",
		"inherited",
		"null_frac",
		"avg_width",
		"n_distinct",
		"most_common_vals",
		"most_common_freqs",
		"histogram_bounds",
		"correlation",
		"most_common_elems",
		"most_common_elem_freqs",
		"elem_count_histogram",
		"range_length_histogram",
		"range_empty_frac",
		"range_bounds_histogram"
	};

	Oid			relid;
	Name		attname;
	bool		inherited;
	Relation	rel;
	HeapTuple	ctup;
	HeapTuple	atup;
	Form_pg_class pgcform;

	TypeCacheEntry *typcache;
	const int	operator_flags = TYPECACHE_LT_OPR | TYPECACHE_EQ_OPR;

	Oid			typid;
	int32		typmod;
	Oid			typcoll;
	Oid			eqopr;
	Oid			ltopr;
	Oid			basetypid;
	Oid			baseeqopr;
	Oid			baseltopr;

	const float4 frac_min = 0.0;
	const float4 frac_max = 1.0;
	float4		null_frac;
	const int	avg_width_min = 0;
	int			avg_width;
	const float4 n_distinct_min = -1.0;
	float4		n_distinct;

	Datum		values[Natts_pg_statistic] = {0};
	bool		nulls[Natts_pg_statistic] = {false};

	Relation	sd;
	HeapTuple	oldtup;
	CatalogIndexState indstate;
	HeapTuple	stup;
	Form_pg_attribute attr;

	FmgrInfo	finfo;

	bool		has_mcv;
	bool		has_mc_elems;
	bool		has_rl_hist;
	int			stakind_count;

	int			k = 0;

	/*
	 * A null in a required parameter is an error.
	 *
	 * XXX How do we know P_N_DISTINCT is the last non-null argument?
	 */
	for (int i = P_RELATION; i <= P_N_DISTINCT; i++)
		if (PG_ARGISNULL(i))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s cannot be NULL", param_names[i])));

	/*
	 * Check all parameter pairs up front.
	 */
	has_mcv = has_arg_pair(fcinfo, param_names,
						   P_MC_VALS, P_MC_FREQS);
	has_mc_elems = has_arg_pair(fcinfo, param_names,
								P_MC_ELEMS, P_MC_ELEM_FREQS);
	has_rl_hist = has_arg_pair(fcinfo, param_names,
							   P_RANGE_LENGTH_HIST, P_RANGE_EMPTY_FRAC);

	/*
	 * If a caller specifies more stakind-stats than we have slots to store
	 * them, raise an error.
	 *
	 * XXX is it a good idea to treat bool as 0/1 int?
	 */
	stakind_count = (int) has_mcv + (int) has_mc_elems + (int) has_rl_hist +
		(int) !PG_ARGISNULL(P_HIST_BOUNDS) +
		(int) !PG_ARGISNULL(P_CORRELATION) +
		(int) !PG_ARGISNULL(P_ELEM_COUNT_HIST) +
		(int) !PG_ARGISNULL(P_RANGE_BOUNDS_HIST);

	if (stakind_count > STATISTIC_NUM_SLOTS)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("imported statistics must have a maximum of %d slots "
						"but %d given",
						STATISTIC_NUM_SLOTS, stakind_count)));

	relid = PG_GETARG_OID(P_RELATION);

	rel = relation_open(relid, ShareUpdateExclusiveLock);

	/* Test existence of Relation */
	ctup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(ctup))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	pgcform = (Form_pg_class) GETSTRUCT(ctup);

	if (!canModifyRelation(relid, pgcform))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for relation %s",
						RelationGetRelationName(rel))));

	ReleaseSysCache(ctup);

	/*
	 * Test existence of attribute
	 */
	attname = PG_GETARG_NAME(P_ATTNAME);
	atup = SearchSysCache2(ATTNAME,
						   ObjectIdGetDatum(relid),
						   NameGetDatum(attname));

	/* Attribute not found nowhere to import the stats to */
	if (!HeapTupleIsValid(atup))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("Relation %s has no attname %s",
						RelationGetRelationName(rel),
						NameStr(*attname))));

	attr = (Form_pg_attribute) GETSTRUCT(atup);
	if (attr->attisdropped)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("Relation %s attname %s is dropped",
						RelationGetRelationName(rel),
						NameStr(*attname))));

	/* Test inherited */
	inherited = PG_GETARG_BOOL(P_INHERITED);
	if (inherited &&
		(rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE) &&
		(rel->rd_rel->relkind != RELKIND_PARTITIONED_INDEX))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("Relation %s is not partitioned, cannot accept inherited stats",
						RelationGetRelationName(rel))));

	/*
	 * Fetch datatype information, this is needed to derive the proper staopN
	 * and stacollN values.
	 *
	 * If this relation is an index and that index has expressions in it, and
	 * the attnum specified is known to be an expression, then we must walk
	 * the list attributes up to the specified attnum to get the right
	 * expression.
	 */
	if ((rel->rd_rel->relkind == RELKIND_INDEX
		 || (rel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX))
		&& (rel->rd_indexprs != NIL)
		&& (rel->rd_index->indkey.values[attr->attnum - 1] == 0))
	{
		ListCell   *indexpr_item = list_head(rel->rd_indexprs);
		Node	   *expr;

		for (int i = 0; i < attr->attnum - 1; i++)
			if (rel->rd_index->indkey.values[i] == 0)
				indexpr_item = lnext(rel->rd_indexprs, indexpr_item);

		if (indexpr_item == NULL)	/* shouldn't happen */
			elog(ERROR, "too few entries in indexprs list");

		expr = (Node *) lfirst(indexpr_item);

		typid = exprType(expr);
		typmod = exprTypmod(expr);

		/*
		 * If a collation has been specified for the index column, use that in
		 * preference to anything else; but if not, fall back to whatever we
		 * can get from the expression.
		 */
		if (OidIsValid(attr->attcollation))
			typcoll = attr->attcollation;
		else
			typcoll = exprCollation(expr);
	}
	else
	{
		typid = attr->atttypid;
		typmod = attr->atttypmod;
		typcoll = attr->attcollation;
	}

	/* if it's a multirange, step down to the range type */
	if (type_is_multirange(typid))
		typid = get_multirange_range(typid);

	typcache = lookup_type_cache(typid, operator_flags);
	ltopr = typcache->lt_opr;
	eqopr = typcache->eq_opr;

	/*
	 * if it's a range type, swap the subtype for the base type, otherwise get
	 * the base element type
	 */
	if (type_is_range(typid))
		basetypid = get_range_subtype(typid);
	else
		basetypid = get_base_element_type(typid);

	if (basetypid == InvalidOid)
	{
		/* type is its own base type */
		basetypid = typid;
		baseltopr = ltopr;
		baseeqopr = eqopr;
	}
	else
	{
		TypeCacheEntry *bentry = lookup_type_cache(basetypid, operator_flags);

		baseltopr = bentry->lt_opr;
		baseeqopr = bentry->eq_opr;
	}

	/* P_HIST_BOUNDS and P_CORRELATION must have a < operator */
	if (baseltopr == InvalidOid)
		for (int i = P_HIST_BOUNDS; i <= P_CORRELATION; i++)
			if (!PG_ARGISNULL(i))
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("Relation %s attname %s cannot "
								"have stats of type %s",
								RelationGetRelationName(rel),
								NameStr(*attname),
								param_names[i])));

	/* Scalar types can't have P_MC_ELEMS, P_MC_ELEM_FREQS, P_ELEM_COUNT_HIST */
	/* TODO any other types we can exclude? */
	if (type_is_scalar(typid))
		for (int i = P_MC_ELEMS; i <= P_ELEM_COUNT_HIST; i++)
			if (!PG_ARGISNULL(i))
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("Relation %s attname %s is a scalar type, "
								"cannot have stats of type %s",
								RelationGetRelationName(rel),
								NameStr(*attname),
								param_names[i])));

	/* Only range types can have P_RANGE_x */
	if ((!type_is_range(typid)) && (!type_is_multirange(typid)))
		for (int i = P_RANGE_LENGTH_HIST; i <= P_RANGE_BOUNDS_HIST; i++)
			if (!PG_ARGISNULL(i))
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("Relation %s attname %s is not a range type, "
								"cannot have stats of type %s",
								RelationGetRelationName(rel),
								NameStr(*attname),
								param_names[i])));

	/*
	 * Statistical parameters that must pass data validity tests
	 */
	null_frac = PG_GETARG_FLOAT4(P_NULL_FRAC);
	if ((null_frac < frac_min) || (null_frac > frac_max))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s %f is out of range %.1f to %.1f",
						param_names[P_NULL_FRAC], null_frac,
						frac_min, frac_max)));

	avg_width = PG_GETARG_INT32(P_AVG_WIDTH);
	if (avg_width < avg_width_min)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s %d must be >= %d",
						param_names[P_AVG_WIDTH], avg_width, avg_width_min)));

	n_distinct = PG_GETARG_FLOAT4(P_N_DISTINCT);
	if (n_distinct < n_distinct_min)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s %f must be >= %.1f",
						param_names[P_N_DISTINCT], n_distinct,
						n_distinct_min)));

	values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(attr->attnum);
	values[Anum_pg_statistic_stainherit - 1] = PG_GETARG_DATUM(P_INHERITED);
	values[Anum_pg_statistic_stanullfrac - 1] = PG_GETARG_DATUM(P_NULL_FRAC);
	values[Anum_pg_statistic_stawidth - 1] = PG_GETARG_DATUM(P_AVG_WIDTH);
	values[Anum_pg_statistic_stadistinct - 1] = PG_GETARG_DATUM(P_N_DISTINCT);

	fmgr_info(F_ARRAY_IN, &finfo);

	/* MC_VALS && MC_FREQS => STATISTIC_KIND_MCV */
	if (has_mcv)
	{
		const char *freqsname = param_names[P_MC_FREQS];
		const char *valsname = param_names[P_MC_VALS];
		Datum		freqs = PG_GETARG_DATUM(P_MC_FREQS);
		Datum		vals = cast_stavalue(&finfo, PG_GETARG_DATUM(P_MC_VALS),
										 basetypid, typmod);

		ExpandedArrayHeader *freqsarr = DatumGetExpandedArray(freqs);
		ExpandedArrayHeader *valsarr = DatumGetExpandedArray(vals);

		int			nvals = value_array_len(valsarr, valsname);
		int			nfreqs = value_not_null_array_len(freqsarr, freqsname);

		if (nfreqs != nvals)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s has %d elements, but %s has %d elements, "
							"but they must be equal",
							freqsname, nfreqs,
							valsname, nvals)));

		/*
		 * check that freqs sum to <= 1.0 or some number slightly higer to
		 * allow for compounded rounding errors.
		 */


		if (nfreqs >= 1)
		{
			const float4 freqsummax = 1.1;

			float4		prev = DatumGetFloat4(freqsarr->dvalues[0]);
			float4		freqsum = prev;

			for (int i = 1; i < nfreqs; i++)
			{
				float4		f = DatumGetFloat4(freqsarr->dvalues[i]);

				if (f > prev)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("%s array values must be in descending "
									"order, but %f > %f",
									freqsname, f, prev)));

				freqsum += f;
				prev = f;
			}

			if (freqsum > freqsummax)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("The sum of elements in %s must not exceed "
								"%.2f but is %f",
								freqsname, freqsummax, freqsum)));
		}

		values[Anum_pg_statistic_stakind1 - 1 + k] =
			Int16GetDatum(STATISTIC_KIND_MCV);
		values[Anum_pg_statistic_staop1 - 1 + k] = ObjectIdGetDatum(eqopr);
		values[Anum_pg_statistic_stacoll1 - 1 + k] = ObjectIdGetDatum(typcoll);
		values[Anum_pg_statistic_stanumbers1 - 1 + k] = freqs;
		values[Anum_pg_statistic_stavalues1 - 1 + k] = vals;

		k++;
	}


	/* HIST_BOUNDS => STATISTIC_KIND_HISTOGRAM */
	if (!PG_ARGISNULL(P_HIST_BOUNDS))
	{
		const char *statname = param_names[P_HIST_BOUNDS];
		Datum		strvalue = PG_GETARG_DATUM(P_HIST_BOUNDS);
		Datum		stavalues = cast_stavalue(&finfo, strvalue, basetypid, typmod);

		ExpandedArrayHeader *arr = DatumGetExpandedArray(stavalues);
		SortSupportData ssupd;

		int			nelems = value_not_null_array_len(arr, statname);



		memset(&ssupd, 0, sizeof(ssupd));
		ssupd.ssup_cxt = CurrentMemoryContext;
		ssupd.ssup_collation = typcoll;
		ssupd.ssup_nulls_first = false;
		ssupd.abbreviate = false;

		PrepareSortSupportFromOrderingOp(baseltopr, &ssupd);

		/*
		 * This is a histogram, which means that the values must be in
		 * monotonically non-decreasing order. If we every find a case where
		 * [n] > [n+1], raise an error.
		 */
		for (int i = 1; i < nelems; i++)
		{
			Datum		a = arr->dvalues[i - 1];
			Datum		b = arr->dvalues[i];

			if (ssupd.comparator(a, b, &ssupd) > 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("%s values must be in ascending order %s",
								statname, TextDatumGetCString(strvalue))));

		}

		values[Anum_pg_statistic_stakind1 - 1 + k] =
			Int16GetDatum(STATISTIC_KIND_HISTOGRAM);
		values[Anum_pg_statistic_staop1 - 1 + k] = ObjectIdGetDatum(ltopr);
		values[Anum_pg_statistic_stacoll1 - 1 + k] = ObjectIdGetDatum(typcoll);

		nulls[Anum_pg_statistic_stanumbers1 - 1 + k] = true;
		values[Anum_pg_statistic_stavalues1 - 1 + k] = stavalues;

		k++;
	}

	/* CORRELATION => STATISTIC_KIND_CORRELATION */
	if (!PG_ARGISNULL(P_CORRELATION))
	{
		const char *statname = param_names[P_CORRELATION];
		Datum		elem = PG_GETARG_DATUM(P_CORRELATION);
		Datum		elems[] = {elem};
		ArrayType  *arry = construct_array_builtin(elems, 1, FLOAT4OID);

		const float4 corr_min = -1.0;
		const float4 corr_max = 1.0;
		float4		corr = PG_GETARG_FLOAT4(P_CORRELATION);

		if ((corr < corr_min) || (corr > corr_max))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s %f is out of range %.1f to %.1f",
							statname, corr, corr_min, corr_max)));

		values[Anum_pg_statistic_stakind1 - 1 + k] =
			Int16GetDatum(STATISTIC_KIND_CORRELATION);
		values[Anum_pg_statistic_staop1 - 1 + k] = ObjectIdGetDatum(ltopr);
		values[Anum_pg_statistic_stacoll1 - 1 + k] = ObjectIdGetDatum(typcoll);
		values[Anum_pg_statistic_stanumbers1 - 1 + k] = PointerGetDatum(arry);
		nulls[Anum_pg_statistic_stavalues1 - 1 + k] = true;

		k++;
	}

	/* MC_ELEMS && MC_ELEM_FREQS => STATISTIC_KIND_MCELEM */
	if (has_mc_elems)
	{
		const char *elemsname = param_names[P_MC_ELEMS];
		const char *freqsname = param_names[P_MC_ELEM_FREQS];
		Datum		freqs = PG_GETARG_DATUM(P_MC_ELEM_FREQS);
		Datum		vals = cast_stavalue(&finfo, PG_GETARG_DATUM(P_MC_ELEMS),
										 basetypid, typmod);

		ExpandedArrayHeader *freqsarr = DatumGetExpandedArray(freqs);
		ExpandedArrayHeader *valsarr = DatumGetExpandedArray(vals);

		int			nfreqs = value_not_null_array_len(freqsarr, freqsname);
		int			nvals = value_not_null_array_len(valsarr, elemsname);

		/*
		 * The mcelem freqs array has either 2 or 3 additional values: the min
		 * frequency, the max frequency, the optional null frequency.
		 */
		int			nfreqsmin = nvals + 2;
		int			nfreqsmax = nvals + 3;

		float4		freqlowbound;
		float4		freqhighbound;

		if (nfreqs > 0)
		{
			if ((nfreqs < nfreqsmin) || (nfreqs > nfreqsmax))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("%s has %d elements, but must have between "
								"%d and %d because %s has %d elements",
								freqsname, nfreqs, nfreqsmin, nfreqsmax,
								elemsname, nvals)));

			/*
			 * the freqlowbound and freqhighbound must themselves be valid
			 * percentages
			 */

			/* first freq element past the length of the values is the min */
			freqlowbound = DatumGetFloat4(freqsarr->dvalues[nvals]);
			if ((freqlowbound < frac_min) || (freqlowbound > frac_max))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("%s %s frequency %f is out of "
								"range %.1f to %.1f",
								freqsname, "minimum", freqlowbound,
								frac_min, frac_max)));

			/* second freq element past the length of the values is the max */
			freqhighbound = DatumGetFloat4(freqsarr->dvalues[nvals + 1]);
			if ((freqhighbound < frac_min) || (freqhighbound > frac_max))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("%s %s frequency %f is out of "
								"range %.1f to %.1f",
								freqsname, "maximum", freqhighbound,
								frac_min, frac_max)));

			/* low bound must be < high bound */
			if (freqlowbound > freqhighbound)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("%s frequency low bound %f cannot be greater "
								"than high bound %f",
								freqsname, freqlowbound, freqhighbound)));

			/*
			 * third freq element past the length of the values is the null
			 * frac
			 */
			if (nfreqs == nvals + 3)
			{
				float4		freqnullpct;

				freqnullpct = DatumGetFloat4(freqsarr->dvalues[nvals + 2]);

				if ((freqnullpct < frac_min) || (freqnullpct > frac_max))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("%s %s frequency %f is out of "
									"range %.1f to %.1f",
									freqsname, "null", freqnullpct,
									frac_min, frac_max)));
			}

			/*
			 * All the freqs that match up to a val must bet between low/high
			 * bounds (which is never less strict than frac_min/frac_max) and
			 * must be in monotonically non-increasing order.
			 *
			 * Also, these frequencies do not sum to a number <= 1.0 as is the
			 * case with MC_FREQS.
			 */
			if (nvals > 1)
			{
				float4		prev = DatumGetFloat4(freqsarr->dvalues[0]);

				for (int i = 1; i < nvals; i++)
				{
					float4		f = DatumGetFloat4(freqsarr->dvalues[i]);

					if ((f < freqlowbound) || (f > freqhighbound))
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("%s frequency %f is out of range "
										"%f to %f",
										freqsname, f,
										freqlowbound, freqhighbound)));
					if (f > prev)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("%s array values must be in "
										"descending order, but %f > %f",
										freqsname, f, prev)));

					prev = f;
				}
			}
		}

		values[Anum_pg_statistic_stakind1 - 1 + k] =
			Int16GetDatum(STATISTIC_KIND_MCELEM);
		values[Anum_pg_statistic_staop1 - 1 + k] = ObjectIdGetDatum(baseeqopr);
		values[Anum_pg_statistic_stacoll1 - 1 + k] = ObjectIdGetDatum(typcoll);
		values[Anum_pg_statistic_stanumbers1 - 1 + k] =
			PG_GETARG_DATUM(P_MC_ELEM_FREQS);
		values[Anum_pg_statistic_stavalues1 - 1 + k] =
			cast_stavalue(&finfo, PG_GETARG_DATUM(P_MC_ELEMS),
						  basetypid, typmod);

		k++;
	}

	/* ELEM_COUNT_HIST => STATISTIC_KIND_DECHIST */
	if (!PG_ARGISNULL(P_ELEM_COUNT_HIST))
	{
		const char *statname = param_names[P_ELEM_COUNT_HIST];
		Datum		stanumbers = PG_GETARG_DATUM(P_ELEM_COUNT_HIST);

		ExpandedArrayHeader *arr = DatumGetExpandedArray(stanumbers);

		const float4 last_min = 0.0;

		int			nelems = value_not_null_array_len(arr, statname);
		float4		last;

		/* Last element must be >= 0 */
		last = DatumGetFloat4(arr->dvalues[nelems - 1]);
		if (last < last_min)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s has last element %f < %.1f",
							statname, last, last_min)));

		/* all other elements must be monotonically nondecreasing */
		if (nelems > 1)
		{
			float4		prev = DatumGetFloat4(arr->dvalues[0]);

			for (int i = 1; i < nelems - 1; i++)
			{
				float4		f = DatumGetFloat4(arr->dvalues[i]);

				if (f < prev)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("%s array values must be in ascending "
									"order, but %f > %f",
									statname, prev, f)));

				prev = f;
			}
		}

		values[Anum_pg_statistic_stakind1 - 1 + k] =
			Int16GetDatum(STATISTIC_KIND_DECHIST);
		values[Anum_pg_statistic_staop1 - 1 + k] = ObjectIdGetDatum(baseeqopr);
		values[Anum_pg_statistic_stacoll1 - 1 + k] = ObjectIdGetDatum(typcoll);

		values[Anum_pg_statistic_stanumbers1 - 1 + k] = stanumbers;
		nulls[Anum_pg_statistic_stavalues1 - 1 + k] = true;

		k++;
	}

	/*
	 * RANGE_BOUNDS_HIST => STATISTIC_KIND_BOUNDS_HISTOGRAM
	 *
	 * This stakind appears before STATISTIC_KIND_BOUNDS_HISTOGRAM even though
	 * it is numerically greater, and all other stakinds appear in numerical
	 * order. We duplicate this quirk to make before/after tests of
	 * pg_statistic records easier.
	 *
	 */
	if (!PG_ARGISNULL(P_RANGE_BOUNDS_HIST))
	{
		const char *statname = param_names[P_RANGE_BOUNDS_HIST];
		Datum		strvalue = PG_GETARG_DATUM(P_RANGE_BOUNDS_HIST);
		Datum		stavalues = cast_stavalue(&finfo, strvalue, typid, typmod);

		ExpandedArrayHeader *arr = DatumGetExpandedArray(stavalues);

		int			nelems = value_not_null_array_len(arr, statname);

		/*
		 * The values in this array are range types, but in fact it's using
		 * range types to have two parallel arrays with inclusive/exclusive
		 * bounds, and those two arrays must each be monotonically
		 * nondecreasing. So basically we want to test that: lower_bound(N) <=
		 * lower_bound(N+1) and upper_bound(N) <= upper_bound(N+1)
		 */
		if (nelems > 1)
		{
			RangeType  *prevrange = DatumGetRangeTypeP(arr->dvalues[0]);
			RangeBound	prevlower;
			RangeBound	prevupper;
			bool		empty;

			range_deserialize(typcache, prevrange, &prevlower,
							  &prevupper, &empty);


			for (int i = 1; i < nelems; i++)
			{
				RangeType  *range = DatumGetRangeTypeP(arr->dvalues[i]);
				RangeBound	lower;
				RangeBound	upper;

				range_deserialize(typcache, range, &lower, &upper, &empty);

				if (range_cmp_bounds(typcache, &prevlower, &lower) == 1)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("%s array %s bounds must be in ascending order",
									statname, "lower")));

				if (range_cmp_bounds(typcache, &prevupper, &upper) == 1)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("%s array %s bounds must be in ascending order",
									statname, "upper")));

				memcpy(&lower, &prevlower, sizeof(RangeBound));
				memcpy(&upper, &prevupper, sizeof(RangeBound));
			}
		}

		values[Anum_pg_statistic_stakind1 - 1 + k] =
			Int16GetDatum(STATISTIC_KIND_BOUNDS_HISTOGRAM);
		values[Anum_pg_statistic_staop1 - 1 + k] = ObjectIdGetDatum(InvalidOid);
		values[Anum_pg_statistic_stacoll1 - 1 + k] = ObjectIdGetDatum(InvalidOid);
		nulls[Anum_pg_statistic_stanumbers1 - 1 + k] = true;
		values[Anum_pg_statistic_stavalues1 - 1 + k] = stavalues;

		k++;
	}

	/*
	 * P_RANGE_LENGTH_HIST && P_RANGE_EMPTY_FRAC =>
	 * STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM
	 */
	if (has_rl_hist)
	{
		const char *histname = param_names[P_RANGE_LENGTH_HIST];
		const char *fracname = param_names[P_RANGE_EMPTY_FRAC];
		Datum		elem = PG_GETARG_DATUM(P_RANGE_EMPTY_FRAC);
		Datum		rlhist = PG_GETARG_DATUM(P_RANGE_LENGTH_HIST);
		Datum		elems[] = {elem};
		ArrayType  *arry = construct_array_builtin(elems, 1, FLOAT4OID);
		float4		frac = DatumGetFloat4(elem);
		Datum		stavalue;

		ExpandedArrayHeader *arr;
		int			nelems;

		if ((frac < frac_min) || (frac > frac_max))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s %f is out of range %.1f to %.1f",
							fracname, frac, frac_min, frac_max)));

		/*
		 * RANGE_LENGTH_HIST is stored in an anyarray, but it is known to be
		 * of type float8[]. It is also a histogram, so it must be in
		 * monotonically nondecreasing order.
		 */
		stavalue = cast_stavalue(&finfo, rlhist, FLOAT8OID, typmod);

		arr = DatumGetExpandedArray(stavalue);
		nelems = value_not_null_array_len(arr, histname);

		if (nelems > 1)
		{
			float8		prev = DatumGetFloat8(arr->dvalues[0]);

			for (int i = 1; i < nelems; i++)
			{
				float8		f = DatumGetFloat8(arr->dvalues[i]);

				if (f < prev)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("%s array values must be in ascending "
									"order, but %f > %f",
									histname, prev, f)));

				prev = f;
			}
		}

		values[Anum_pg_statistic_stakind1 - 1 + k] =
			Int16GetDatum(STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM);
		values[Anum_pg_statistic_staop1 - 1 + k] = ObjectIdGetDatum(Float8LessOperator);
		values[Anum_pg_statistic_stacoll1 - 1 + k] = ObjectIdGetDatum(InvalidOid);
		values[Anum_pg_statistic_stanumbers1 - 1 + k] = PointerGetDatum(arry);
		values[Anum_pg_statistic_stavalues1 - 1 + k] = stavalue;
		k++;
	}

	/* fill in all remaining slots */
	for (; k < STATISTIC_NUM_SLOTS; k++)
	{
		values[Anum_pg_statistic_stakind1 - 1 + k] = Int16GetDatum(0);
		values[Anum_pg_statistic_staop1 - 1 + k] = ObjectIdGetDatum(InvalidOid);
		values[Anum_pg_statistic_stacoll1 - 1 + k] = ObjectIdGetDatum(InvalidOid);
		nulls[Anum_pg_statistic_stanumbers1 - 1 + k] = true;
		nulls[Anum_pg_statistic_stavalues1 - 1 + k] = true;
	}

	/* Is there already a pg_statistic tuple for this attribute? */
	oldtup = SearchSysCache3(STATRELATTINH,
							 ObjectIdGetDatum(relid),
							 Int16GetDatum(attr->attnum),
							 PG_GETARG_DATUM(P_INHERITED));

	sd = table_open(StatisticRelationId, RowExclusiveLock);
	indstate = CatalogOpenIndexes(sd);

	if (HeapTupleIsValid(oldtup))
	{
		/* Yes, replace it */
		bool		replaces[Natts_pg_statistic];

		for (int i = 0; i < Natts_pg_statistic; i++)
			replaces[i] = true;

		stup = heap_modify_tuple(oldtup, RelationGetDescr(sd),
								 values, nulls, replaces);
		ReleaseSysCache(oldtup);
		CatalogTupleUpdateWithInfo(sd, &stup->t_self, stup, indstate);
	}
	else
	{
		/* No, insert new tuple */
		stup = heap_form_tuple(RelationGetDescr(sd), values, nulls);
		CatalogTupleInsertWithInfo(sd, stup, indstate);
	}

	heap_freetuple(stup);
	CatalogCloseIndexes(indstate);
	table_close(sd, NoLock);
	relation_close(rel, NoLock);
	ReleaseSysCache(atup);
	PG_RETURN_VOID();
}
