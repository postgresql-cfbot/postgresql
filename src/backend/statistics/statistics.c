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
#include "catalog/indexing.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "funcapi.h"
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
 * A role has privileges to set statistics on the relation if any of the
 * following are true:
 *   - the role owns the current database and the relation is not shared
 *   - the role has the MAINTAIN privilege on the relation
 *
 */
static bool
can_modify_relation(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	Form_pg_class reltuple = rel->rd_rel;

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
	Oid			relid = PG_GETARG_OID(0);
	int			version = PG_GETARG_INT32(1);

	/* indexes of where we found required stats */
	int			i_relpages = 0;
	int			i_reltuples = 0;
	int			i_relallvisible = 0;

	/* build argument values to build the object */
	Datum	   *args;
	bool	   *nulls;			/* placeholder, because strict */
	Oid		   *types;
	int			nargs;

	/* Minimum version supported */
	if (version <= 90200)
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Cannot export statistics prior to version 9.2")));

	nargs = extract_variadic_args(fcinfo, 2, true, &args, &types, &nulls);

	/* if the pairs aren't pairs, something is malformed */
	if (nargs % 2 == 1)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("stats parameters must be in name-value pairs")));
		PG_RETURN_BOOL(false);
	}

	/* loop through args, matching params to their arg indexes */
	for (int i = 0; i < nargs; i += 2)
	{
		char	   *statname;
		int			argidx = i + 1;

		if (types[i] != TEXTOID)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("stat names must be of type text")));
			continue;
		}
		statname = TextDatumGetCString(args[i]);

		/*
		 * match each named parameter to the index of the value that follows
		 * it
		 */
		if (strcmp(statname, "relpages") == 0)
			i_relpages = argidx;
		else if (strcmp(statname, "reltuples") == 0)
			i_reltuples = argidx;
		else if (strcmp(statname, "relallvisible") == 0)
			i_relallvisible = argidx;
		else
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unknown stat naame '%s', skipping", statname)));

		pfree(statname);
	}

	/*
	 * Ensure that we got all required parameters, and they are of the correct
	 * type.
	 */
	if (i_relpages == 0)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("required parameter %s not set", "relpages")));
		PG_RETURN_BOOL(false);
	}
	else if (types[i_relpages] != INT4OID)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("relpages must be of type integer")));
		PG_RETURN_BOOL(false);
	}
	else if (i_reltuples == 0)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("required parameter %s not set", "reltuples")));
		PG_RETURN_BOOL(false);
	}
	else if (types[i_reltuples] != FLOAT4OID)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("reltuples must be of type real")));
		PG_RETURN_BOOL(false);
	}
	else if (types[i_relallvisible] != INT4OID)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("required parameter %s not set", "relallvisible")));
		PG_RETURN_BOOL(false);
	}
	else if (i_relallvisible == -1)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("relallvisible must be of type integer")));
		PG_RETURN_BOOL(false);
	}
	else
	{
		/*
		 * Open the relation, getting ShareUpdateExclusiveLock to ensure that
		 * no other stat-setting operation can run on it concurrently.
		 */
		Relation	rel;
		HeapTuple	ctup;
		Form_pg_class pgcform;
		int			relpages;
		float4		reltuples;
		int			relallvisible;

		rel = table_open(RelationRelationId, ShareUpdateExclusiveLock);

		if (!can_modify_relation(rel))
		{
			table_close(rel, NoLock);
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("must be owner to modify relation stats")));
			PG_RETURN_BOOL(false);
		}

		relpages = DatumGetInt32(args[i_relpages]);
		reltuples = DatumGetFloat4(args[i_reltuples]);
		relallvisible = DatumGetInt32(args[i_relallvisible]);

		ctup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
		if (!HeapTupleIsValid(ctup))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_IN_USE),
					 errmsg("pg_class entry for relid %u not found", relid)));

		pgcform = (Form_pg_class) GETSTRUCT(ctup);
		/* Only update pg_class if there is a meaningful change */
		if ((pgcform->reltuples != reltuples)
			|| (pgcform->relpages != relpages)
			|| (pgcform->relallvisible != relallvisible))
		{
			pgcform->relpages = relpages;
			pgcform->reltuples = reltuples;
			pgcform->relallvisible = relallvisible;

			heap_inplace_update(rel, ctup);
		}

		table_close(rel, NoLock);
		PG_RETURN_BOOL(true);
	}

	PG_RETURN_BOOL(false);
}

/*
 * Test if the type is a scalar for MCELEM purposes
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

/*
 * If this relation is an index and that index has expressions in it, and
 * the attnum specified is known to be an expression, then we must walk
 * the list attributes up to the specified attnum to get the right
 * expression.
 */
static Node *
get_attr_expr(Relation rel, int attnum)
{
	if ((rel->rd_rel->relkind == RELKIND_INDEX
		 || (rel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX))
		&& (rel->rd_indexprs != NIL)
		&& (rel->rd_index->indkey.values[attnum - 1] == 0))
	{
		ListCell   *indexpr_item = list_head(rel->rd_indexprs);

		for (int i = 0; i < attnum - 1; i++)
			if (rel->rd_index->indkey.values[i] == 0)
				indexpr_item = lnext(rel->rd_indexprs, indexpr_item);

		if (indexpr_item == NULL)	/* shouldn't happen */
			elog(ERROR, "too few entries in indexprs list");

		return (Node *) lfirst(indexpr_item);
	}
	return NULL;
}

/*
 * Fetch datatype information, this is needed to derive the proper staopN
 * and stacollN values.
 *
 */
static TypeCacheEntry *
get_attr_stat_type(Relation rel, Name attname,
				   int16 *attnum, int32 *typmod, Oid *typcoll)
{
	Oid			relid = RelationGetRelid(rel);
	Form_pg_attribute attr;
	HeapTuple	atup;
	Oid			typid;
	Node	   *expr;

	atup = SearchSysCache2(ATTNAME, ObjectIdGetDatum(relid),
						   NameGetDatum(attname));

	/* Attribute not found */
	if (!HeapTupleIsValid(atup))
	{
		ereport(WARNING,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("Relation %s has no attname %s",
						RelationGetRelationName(rel),
						NameStr(*attname))));
		return NULL;
	}

	attr = (Form_pg_attribute) GETSTRUCT(atup);
	if (attr->attisdropped)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("Relation %s attname %s is dropped",
						RelationGetRelationName(rel),
						NameStr(*attname))));
		return NULL;
	}
	*attnum = attr->attnum;

	expr = get_attr_expr(rel, attr->attnum);

	if (expr == NULL)
	{
		/* regular attribute */
		typid = attr->atttypid;
		*typmod = attr->atttypmod;
		*typcoll = attr->attcollation;
	}
	else
	{
		typid = exprType(expr);
		*typmod = exprTypmod(expr);

		/*
		 * If a collation has been specified for the index column, use that in
		 * preference to anything else; but if not, fall back to whatever we
		 * can get from the expression.
		 */
		if (OidIsValid(attr->attcollation))
			*typcoll = attr->attcollation;
		else
			*typcoll = exprCollation(expr);
	}
	ReleaseSysCache(atup);

	/* if it's a multirange, step down to the range type */
	if (type_is_multirange(typid))
		typid = get_multirange_range(typid);

	return lookup_type_cache(typid,
							 TYPECACHE_LT_OPR | TYPECACHE_EQ_OPR);
}

/*
 * Perform the cast of a known TextDatum into the type specified.
 *
 * If no errors are found, ok is set to true. Otherwise, set ok
 * to false, capture the error found, and re-throw at warning level.
 */
static Datum
cast_stavalues(FmgrInfo *flinfo, Datum d, Oid typid, int32 typmod, bool *ok)
{
	LOCAL_FCINFO(fcinfo, 8);
	char	   *s;
	Datum		result;
	ErrorSaveContext escontext = {T_ErrorSaveContext};

	escontext.details_wanted = true;

	s = TextDatumGetCString(d);

	InitFunctionCallInfoData(*fcinfo, flinfo, 3, InvalidOid,
							 (Node *) &escontext, NULL);

	fcinfo->args[0].value = CStringGetDatum(s);
	fcinfo->args[0].isnull = false;
	fcinfo->args[1].value = ObjectIdGetDatum(typid);
	fcinfo->args[1].isnull = false;
	fcinfo->args[2].value = Int32GetDatum(typmod);
	fcinfo->args[2].isnull = false;

	result = FunctionCallInvoke(fcinfo);

	if (SOFT_ERROR_OCCURRED(&escontext))
	{
		escontext.error_data->elevel = WARNING;
		ThrowErrorData(escontext.error_data);
		*ok = false;
	}
	else
		*ok = true;

	pfree(s);

	return result;
}


/*
 * Check array for any NULLs, and optionally for one-dimensionality.
 *
 * Report any failures as warnings.
 */
static bool
array_check(Datum datum, int one_dim, const char *statname)
{
	ArrayType  *arr = DatumGetArrayTypeP(datum);
	int16		elmlen;
	char		elmalign;
	bool		elembyval;
	Datum	   *values;
	bool	   *nulls;
	int			nelems;

	if (one_dim && (arr->ndim != 1))
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s cannot be a multidimensional array", statname)));
		return false;
	}

	get_typlenbyvalalign(ARR_ELEMTYPE(arr), &elmlen, &elembyval, &elmalign);

	deconstruct_array(arr, ARR_ELEMTYPE(arr), elmlen, elembyval, elmalign,
					  &values, &nulls, &nelems);

	for (int i = 0; i < nelems; i++)
		if (nulls[i])
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s array cannot contain NULL values", statname)));
			return false;
		}
	return true;
}

/*
 * Update the pg_statistic record.
 */
static void
update_pg_statistic(Datum values[], bool nulls[])
{
	Relation	sd = table_open(StatisticRelationId, RowExclusiveLock);
	CatalogIndexState indstate = CatalogOpenIndexes(sd);
	HeapTuple	oldtup;
	HeapTuple	stup;

	/* Is there already a pg_statistic tuple for this attribute? */
	oldtup = SearchSysCache3(STATRELATTINH,
							 values[Anum_pg_statistic_starelid - 1],
							 values[Anum_pg_statistic_staattnum - 1],
							 values[Anum_pg_statistic_stainherit - 1]);

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
}

/*
 * Import statistics for a given relation attribute.
 *
 * This will insert/replace a row in pg_statistic for the given combinarion
 * of relation, attribute, and inherited flag.
 *
 * The version parameter is for future use in the events as future versions
 * may change them meaning of certain parameters.
 *
 * The variadic parameters all represent name-value pairs, with the names
 * corresponding to attributes in pg_stats. Unkown names will generate a
 * warning.
 *
 * Parameters null_frac, avg_width, and n_distinct are required because
 * those attributes have no default value in pg_statistic.
 *
 * The remaining parameters all belong to a specific stakind, and all are
 * optional. Some stakinds have multiple parameters, and in those cases
 * both parameters must be specified if one of them is, otherwise a
 * warning is generated but the rest of the stats may still be imported.
 *
 * If there is no attribute with a matching attname in the relation, the
 * function will raise a warning and return false.
 *
 * Parameters corresponding to ANYARRAY columns are instead passed in as text
 * values, which is a valid input string for an array of the type or element
 * type of the attribute.
 */
Datum
pg_set_attribute_stats(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Name		attname = PG_GETARG_NAME(1);
	bool		inherited = PG_GETARG_BOOL(2);
	int			version = PG_GETARG_INT32(3);

	/* build argument values to build the object */
	Datum	   *args;
	bool	   *argnulls;		/* placeholder, because strict */
	Oid		   *types;
	int			nargs = extract_variadic_args(fcinfo, 4, true,
											  &args, &types, &argnulls);

	Datum		values[Natts_pg_statistic] = {0};
	bool		nulls[Natts_pg_statistic] = {false};

	/*
	 * argument indexes for each known statistic
	 *
	 * 0 = not found, -1 = error, n > 0 = found
	 */

	/* parameters that are required get indexes */
	int			i_null_frac = 0;
	int			i_avg_width = 0;
	int			i_n_distinct = 0;

	/* stakind stats are optional */
	int			i_mc_vals = 0;
	int			i_mc_freqs = 0;
	int			i_hist_bounds = 0;
	int			i_correlation = 0;
	int			i_mc_elems = 0;
	int			i_mc_elem_freqs = 0;
	int			i_elem_count_hist = 0;
	int			i_range_length_hist = 0;
	int			i_range_empty_frac = 0;
	int			i_range_bounds_hist = 0;

	Relation	rel;

	TypeCacheEntry *typcache;
	TypeCacheEntry *elemtypcache = NULL;

	int16		attnum;
	int32		typmod;
	Oid			typcoll;

	FmgrInfo	finfo;

	int			stakind_count;

	int			k = 0;

	if (version <= 90200)
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Cannot export statistics prior to version 9.2")));

	/* if the pairs aren't pairs, something is malformed */
	if (nargs % 2 == 1)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("stats parameters must be in name-value pairs")));
		PG_RETURN_BOOL(false);
	}

	for (int i = 0; i < nargs; i += 2)
	{
		char	   *statname;
		int			argidx = i + 1;

		if (types[i] != TEXTOID)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("stat names must be of type text")));
			break;
		}
		statname = TextDatumGetCString(args[i]);

		if (strcmp(statname, "null_frac") == 0)
			i_null_frac = argidx;
		else if (strcmp(statname, "avg_width") == 0)
			i_avg_width = argidx;
		else if (strcmp(statname, "n_distinct") == 0)
			i_n_distinct = argidx;
		else if (strcmp(statname, "most_common_vals") == 0)
			i_mc_vals = argidx;
		else if (strcmp(statname, "most_common_freqs") == 0)
			i_mc_freqs = argidx;
		else if (strcmp(statname, "histogram_bounds") == 0)
			i_hist_bounds = argidx;
		else if (strcmp(statname, "correlation") == 0)
			i_correlation = argidx;
		else if (strcmp(statname, "most_common_elems") == 0)
			i_mc_elems = argidx;
		else if (strcmp(statname, "most_common_elem_freqs") == 0)
			i_mc_elem_freqs = argidx;
		else if (strcmp(statname, "elem_count_histogram") == 0)
			i_elem_count_hist = argidx;
		else if (strcmp(statname, "range_length_histogram") == 0)
			i_range_length_hist = argidx;
		else if (strcmp(statname, "range_empty_frac") == 0)
			i_range_empty_frac = argidx;
		else if (strcmp(statname, "range_bounds_histogram") == 0)
			i_range_bounds_hist = argidx;
		else
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unknown stat naame '%s', skipping", statname)));

		pfree(statname);
	}

	/* check all required parameters */
	if (i_null_frac > 0)
	{
		if (types[i_null_frac] != FLOAT4OID)
		{
			/* required param, not recoverable */
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s must be of type %s", "null_frac",
							"real")));
			PG_RETURN_BOOL(false);
		}
	}
	else
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("required parameter %s not set", "null_frac")));
		PG_RETURN_BOOL(false);
	}

	if (i_avg_width > 0)
	{
		if (types[i_avg_width] != INT4OID)
		{
			/* required param, not recoverable */
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s must be of type %s", "avg_width",
							"integer")));
			PG_RETURN_BOOL(false);
		}
	}
	else
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("required parameter %s not set", "avg_width")));
		PG_RETURN_BOOL(false);
	}

	if (i_n_distinct > 0)
	{
		if (types[i_n_distinct] != FLOAT4OID)
		{
			/* required param, not recoverable */
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s must be of type %s", "n_distinct",
							"real")));
			PG_RETURN_BOOL(false);
		}
	}
	else
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("required parameter %s not set", "n_distinct")));
		PG_RETURN_BOOL(false);
	}

	/*
	 * Look for pair mismatches, if found warn and disable.
	 */
	if ((i_mc_vals == 0) != (i_mc_freqs == 0))
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s cannot be present if %s is missing",
						(i_mc_vals == 0) ? "most_common_freqs" :
						"most_common_vals",
						(i_mc_vals == 0) ? "most_common_vals" :
						"most_common_freqs")));
		i_mc_vals = -1;
		i_mc_freqs = -1;
	}

	if ((i_mc_elems == 0) != (i_mc_elem_freqs == 0))
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s cannot be present if %s is missing",
						(i_mc_elems == 0) ?
						"most_common_elem_freqs" :
						"most_common_elems",
						(i_mc_elems == 0) ?
						"most_common_elems" :
						"most_common_elem_freqs")));
		i_mc_elems = -1;
		i_mc_elem_freqs = -1;
	}

	if ((i_range_length_hist == 0) != (i_range_empty_frac == 0))
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s cannot be present if %s is missing",
						(i_range_length_hist == 0) ?
						"range_empty_frac" :
						"range_length_histogram",
						(i_range_length_hist == 0) ?
						"range_length_histogram" :
						"range_empty_frac")));
		i_range_length_hist = -1;
		i_range_empty_frac = -1;
	}

	rel = relation_open(relid, ShareUpdateExclusiveLock);

	if (!can_modify_relation(rel))
	{
		relation_close(rel, NoLock);
		ereport(WARNING,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for relation %s",
						RelationGetRelationName(rel))));
		PG_RETURN_BOOL(false);
	}

	if ((!inherited) &&
		((rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) ||
		 (rel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX)))
	{
		relation_close(rel, NoLock);
		ereport(WARNING,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("%s is partitioned, can only accepted inherted stats",
						RelationGetRelationName(rel))));
		PG_RETURN_BOOL(false);
	}

	/*
	 * Many of the values that are set for a particular stakind are entirely
	 * derived from the attribute itself, or it's expression.
	 */
	typcache = get_attr_stat_type(rel, attname, &attnum, &typmod, &typcoll);
	if (typcache == NULL)
	{
		relation_close(rel, NoLock);
		PG_RETURN_BOOL(false);
	}

	/*
	 * Derive element type if we have stat kinds that need it.
	 *
	 * This duplicates some type-specific logic found in various typanalyze
	 * functions which are called from vacuum's examine_attribute().
	 */
	if ((i_mc_elems > 0) || (i_elem_count_hist > 0))
	{
		Oid			elemtypid;

		if (typcache->type_id == TSVECTOROID)
		{
			/*
			 * tsvector elems always have a text oid type and default
			 * collation
			 */
			elemtypid = TEXTOID;
			typcoll = DEFAULT_COLLATION_OID;
		}
		else if (typcache->typtype == TYPTYPE_RANGE)
			elemtypid = get_range_subtype(typcache->type_id);
		else
			elemtypid = get_base_element_type(typcache->type_id);

		/* not finding a basetype means we already had it */
		if (elemtypid == InvalidOid)
			elemtypid = typcache->type_id;

		/* The stats need the eq_opr, any validation would need the lt_opr */
		elemtypcache = lookup_type_cache(elemtypid, TYPECACHE_EQ_OPR);

		if (elemtypcache == NULL)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s cannot accept %s stats, ignored",
							NameStr(*attname),
							(i_mc_elems > 0) ?
							"most_common_elems" :
							"elem_count_histogram")));
			i_mc_elems = -1;
			i_elem_count_hist = -1;
		}
	}

	/*
	 * histogram_bounds and correlation must have a type < operator. WARN and
	 * skip if this attribute doesn't have one.
	 */
	if (typcache->lt_opr == InvalidOid)
	{
		if (i_hist_bounds > 0)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s cannot accept %s stats, ignored",
							NameStr(*attname),
							"histogram_bounds")));
			i_hist_bounds = -1;
		}
		if (i_correlation > 0)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s cannot accept %s stats, ignored",
							NameStr(*attname),
							"correlation")));
			i_correlation = -1;
		}
	}

	/*
	 * Scalar types can't have most_common_elems, most_common_elem_freqs,
	 * elem_count_histogram. WARN and skip.
	 */
	if (type_is_scalar(typcache->type_id))
	{
		if (i_mc_elems > 0)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s cannot accept %s stats, ignored",
							NameStr(*attname),
							"most_common_elems")));
			i_mc_elems = -1;
		}
		if (i_mc_elem_freqs > 0)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s cannot accept %s stats, ignored",
							NameStr(*attname),
							"most_common_elem_freqs")));
			i_mc_elem_freqs = -1;
		}
		if (i_elem_count_hist > 0)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s cannot accept %s stats, ignored",
							NameStr(*attname),
							"elem_count_histogram")));
			i_elem_count_hist = -1;
		}
	}

	/*
	 * Only range types can have range_length_histogram, range_empty_frac, and
	 * range_bounds_histogram. WARN and skip
	 */
	if ((typcache->typtype != TYPTYPE_MULTIRANGE) &&
		(typcache->typtype != TYPTYPE_RANGE))
	{
		if (i_range_length_hist > 0)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s cannot accept %s stats, ignored",
							NameStr(*attname),
							"range_length_histogram")));
			i_range_length_hist = -1;
		}
		if (i_range_empty_frac > 0)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s cannot accept %s stats, ignored",
							NameStr(*attname),
							"range_empty_frac")));
			i_range_empty_frac = -1;
		}
		if (i_range_bounds_hist > 0)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s cannot accept %s stats, ignored",
							NameStr(*attname),
							"range_bounds_histogram")));
			i_range_bounds_hist = -1;
		}
	}

	/*
	 * count the number of stakinds we still want to set, paired params count
	 * as one. The count cannot exceed STATISTIC_NUM_SLOTS.
	 */
	stakind_count = (int) (i_mc_vals > 0) +
		(int) (i_mc_elems > 0) +
		(int) (i_range_length_hist > 0) +
		(int) (i_hist_bounds > 0) +
		(int) (i_correlation > 0) +
		(int) (i_elem_count_hist > 0) +
		(int) (i_range_bounds_hist > 0);

	if (stakind_count > STATISTIC_NUM_SLOTS)
	{
		/*
		 * This really shouldn't happen, as most datatypes exclude at least
		 * one of these types of stats.
		 */
		relation_close(rel, NoLock);
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("imported statistics must have a maximum of %d slots "
						"but %d given",
						STATISTIC_NUM_SLOTS, stakind_count)));
		PG_RETURN_BOOL(false);
	}

	fmgr_info(F_ARRAY_IN, &finfo);

	values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(attnum);
	values[Anum_pg_statistic_stainherit - 1] = BoolGetDatum(inherited);
	values[Anum_pg_statistic_stanullfrac - 1] = args[i_null_frac];
	values[Anum_pg_statistic_stawidth - 1] = args[i_avg_width];
	values[Anum_pg_statistic_stadistinct - 1] = args[i_n_distinct];

	/*
	 * STATISTIC_KIND_MCV
	 *
	 * most_common_freqs: real[]
	 *
	 * most_common_vals : ANYARRAY::text
	 */
	if (i_mc_vals > 0)
	{
		Oid			numberstype = types[i_mc_freqs];
		Oid			valuestype = types[i_mc_vals];
		Datum		stanumbers = args[i_mc_freqs];
		Datum		strvalue = args[i_mc_vals];

		if (get_element_type(numberstype) != FLOAT4OID)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s must be type %s, ignored",
							"most_common_freqs", "real[]")));
		}
		else if (valuestype != TEXTOID)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s must be type %s, ignored",
							"most_common_vals", "text")));
		}
		else
		{
			Datum		stakind = Int16GetDatum(STATISTIC_KIND_MCV);
			Datum		staop = ObjectIdGetDatum(typcache->eq_opr);
			Datum		stacoll = ObjectIdGetDatum(typcoll);
			Datum		stavalues;
			bool		converted = false;

			stavalues = cast_stavalues(&finfo, strvalue, typcache->type_id,
									   typmod, &converted);

			if (converted &&
				array_check(stavalues, false, "most_common_vals") &&
				array_check(stanumbers, true, "most_common_freqs"))
			{
				values[Anum_pg_statistic_stakind1 - 1 + k] = stakind;
				values[Anum_pg_statistic_staop1 - 1 + k] = staop;
				values[Anum_pg_statistic_stacoll1 - 1 + k] = stacoll;
				values[Anum_pg_statistic_stanumbers1 - 1 + k] = stanumbers;
				values[Anum_pg_statistic_stavalues1 - 1 + k] = stavalues;

				k++;
			}
		}
	}

	/*
	 * STATISTIC_KIND_HISTOGRAM
	 *
	 * histogram_bounds: ANYARRAY::text
	 */
	if (i_hist_bounds > 0)
	{
		Oid			valuestype = types[i_hist_bounds];
		Datum		strvalue = args[i_hist_bounds];

		if (valuestype != TEXTOID)
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s must be type %s, ignored",
							"histogram_bounds", "text")));
		else
		{
			Datum		stakind = Int16GetDatum(STATISTIC_KIND_HISTOGRAM);
			Datum		staop = ObjectIdGetDatum(typcache->lt_opr);
			Datum		stacoll = ObjectIdGetDatum(typcoll);
			Datum		stavalues;
			bool		converted = false;

			stavalues = cast_stavalues(&finfo, strvalue, typcache->type_id,
									   typmod, &converted);

			if (converted && array_check(stavalues, false, "histogram_bounds"))
			{
				values[Anum_pg_statistic_stakind1 - 1 + k] = stakind;
				values[Anum_pg_statistic_staop1 - 1 + k] = staop;
				values[Anum_pg_statistic_stacoll1 - 1 + k] = stacoll;
				nulls[Anum_pg_statistic_stanumbers1 - 1 + k] = true;
				values[Anum_pg_statistic_stavalues1 - 1 + k] = stavalues;

				k++;
			}
		}
	}

	/*
	 * STATISTIC_KIND_CORRELATION
	 *
	 * correlation: real
	 */
	if (i_correlation > 0)
	{
		if (types[i_correlation] != FLOAT4OID)
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s must be of type %s, ignored",
							"correlation", "real")));
		else
		{
			Datum		stakind = Int16GetDatum(STATISTIC_KIND_CORRELATION);
			Datum		staop = ObjectIdGetDatum(typcache->lt_opr);
			Datum		stacoll = ObjectIdGetDatum(typcoll);
			Datum		elems[] = {args[i_correlation]};
			ArrayType  *arry = construct_array_builtin(elems, 1, FLOAT4OID);
			Datum		stanumbers = PointerGetDatum(arry);

			values[Anum_pg_statistic_stakind1 - 1 + k] = stakind;
			values[Anum_pg_statistic_staop1 - 1 + k] = staop;
			values[Anum_pg_statistic_stacoll1 - 1 + k] = stacoll;
			values[Anum_pg_statistic_stanumbers1 - 1 + k] = stanumbers;
			nulls[Anum_pg_statistic_stavalues1 - 1 + k] = true;

			k++;
		}
	}

	/*
	 * STATISTIC_KIND_MCELEM
	 *
	 * most_common_elem_freqs: real[]
	 *
	 * most_common_elems     : ANYARRAY::text
	 */
	if (i_mc_elems > 0)
	{
		Oid			numberstype = types[i_mc_elem_freqs];
		Oid			valuestype = types[i_mc_elems];
		Datum		stanumbers = args[i_mc_elem_freqs];

		if (get_element_type(numberstype) != FLOAT4OID)
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s must be type %s, ignored",
							"most_common_elem_freqs", "real[]")));
		else if (valuestype != TEXTOID)
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s must be type %s, ignored",
							"most_common_elems", "text")));
		else
		{
			Datum		stakind = Int16GetDatum(STATISTIC_KIND_MCELEM);
			Datum		staop = ObjectIdGetDatum(elemtypcache->eq_opr);
			Datum		stacoll = ObjectIdGetDatum(typcoll);
			Datum		strvalue = args[i_mc_elems];
			bool		converted = false;
			Datum		stavalues;

			stavalues = cast_stavalues(&finfo, strvalue, elemtypcache->type_id,
									   typmod, &converted);

			if (converted &&
				array_check(stavalues, false, "most_common_elems") &&
				array_check(stanumbers, true, "most_common_elem_freqs"))
			{
				values[Anum_pg_statistic_stakind1 - 1 + k] = stakind;
				values[Anum_pg_statistic_staop1 - 1 + k] = staop;
				values[Anum_pg_statistic_stacoll1 - 1 + k] = stacoll;
				values[Anum_pg_statistic_stanumbers1 - 1 + k] = stanumbers;
				values[Anum_pg_statistic_stavalues1 - 1 + k] = stavalues;

				k++;
			}
		}
	}

	/*
	 * STATISTIC_KIND_DECHIST
	 *
	 * elem_count_histogram:	real[]
	 */
	if (i_elem_count_hist > 0)
	{
		Oid			numberstype = types[i_elem_count_hist];

		if (get_element_type(numberstype) != FLOAT4OID)
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s must be type %s, ignored",
							"elem_count_histogram", "real[]")));
		else
		{
			Datum		stakind = Int16GetDatum(STATISTIC_KIND_DECHIST);
			Datum		staop = ObjectIdGetDatum(elemtypcache->eq_opr);
			Datum		stacoll = ObjectIdGetDatum(typcoll);
			Datum		stanumbers = args[i_elem_count_hist];

			if (array_check(stanumbers, true, "elem_count_histogram"))
			{
				values[Anum_pg_statistic_stakind1 - 1 + k] = stakind;
				values[Anum_pg_statistic_staop1 - 1 + k] = staop;
				values[Anum_pg_statistic_stacoll1 - 1 + k] = stacoll;
				values[Anum_pg_statistic_stanumbers1 - 1 + k] = stanumbers;
				nulls[Anum_pg_statistic_stavalues1 - 1 + k] = true;

				k++;
			}
		}
	}

	/*
	 * STATISTIC_KIND_BOUNDS_HISTOGRAM
	 *
	 * range_bounds_histogram: ANYARRAY::text
	 *
	 * This stakind appears before STATISTIC_KIND_BOUNDS_HISTOGRAM even though
	 * it is numerically greater, and all other stakinds appear in numerical
	 * order. We duplicate this quirk to make before/after tests of
	 * pg_statistic records easier.
	 */
	if (i_range_bounds_hist > 0)
	{
		Oid			valuestype = types[i_range_bounds_hist];

		if (valuestype != TEXTOID)
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s must be type %s, ignored",
							"range_bounds_histogram", "text")));
		else
		{

			Datum		stakind = Int16GetDatum(STATISTIC_KIND_BOUNDS_HISTOGRAM);
			Datum		staop = ObjectIdGetDatum(InvalidOid);
			Datum		stacoll = ObjectIdGetDatum(InvalidOid);
			Datum		strvalue = args[i_range_bounds_hist];
			bool		converted = false;
			Datum		stavalues;

			stavalues = cast_stavalues(&finfo, strvalue, typcache->type_id,
									   typmod, &converted);

			if (converted &&
				array_check(stavalues, false, "range_bounds_histogram"))
			{
				values[Anum_pg_statistic_stakind1 - 1 + k] = stakind;
				values[Anum_pg_statistic_staop1 - 1 + k] = staop;
				values[Anum_pg_statistic_stacoll1 - 1 + k] = stacoll;
				nulls[Anum_pg_statistic_stanumbers1 - 1 + k] = true;
				values[Anum_pg_statistic_stavalues1 - 1 + k] = stavalues;

				k++;
			}
		}
	}

	/*
	 * STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM
	 *
	 * range_empty_frac: real
	 *
	 * range_length_histogram:  double precision[]::text
	 */
	if (i_range_length_hist > 0)
	{
		Oid			numberstype = types[i_range_empty_frac];
		Oid			valuestype = types[i_range_length_hist];

		if (numberstype != FLOAT4OID)
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s must be type %s, ignored",
							"range_empty_frac", "real")));
		else if (valuestype != TEXTOID)
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s must be type %s, ignored",
							"range_length_histogram", "text")));
		else
		{
			Datum		stakind = Int16GetDatum(STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM);
			Datum		staop = ObjectIdGetDatum(Float8LessOperator);
			Datum		stacoll = ObjectIdGetDatum(InvalidOid);

			/* The anyarray is always a float8[] for this stakind */
			Datum		elem = args[i_range_empty_frac];
			Datum		elems[] = {elem};
			ArrayType  *arry = construct_array_builtin(elems, 1, FLOAT4OID);
			Datum		stanumbers = PointerGetDatum(arry);
			Datum		strvalue = args[i_range_length_hist];
			bool		converted = false;
			Datum		stavalues;

			stavalues = cast_stavalues(&finfo, strvalue, FLOAT8OID, 0, &converted);

			if (converted &&
				array_check(stavalues, false, "range_length_histogram"))
			{
				values[Anum_pg_statistic_stakind1 - 1 + k] = stakind;
				values[Anum_pg_statistic_staop1 - 1 + k] = staop;
				values[Anum_pg_statistic_stacoll1 - 1 + k] = stacoll;
				values[Anum_pg_statistic_stanumbers1 - 1 + k] = stanumbers;
				values[Anum_pg_statistic_stavalues1 - 1 + k] = stavalues;

				k++;
			}
		}
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

	update_pg_statistic(values, nulls);

	relation_close(rel, NoLock);
	PG_RETURN_BOOL(true);
}
