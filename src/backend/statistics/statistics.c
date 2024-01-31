/*-------------------------------------------------------------------------
 *
 * statistics.c
 *
 * IDENTIFICATION
 *	  src/backend/statistics/statistics.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "catalog/indexing.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "catalog/pg_operator.h"
#include "fmgr.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/datum.h" /* REMOVE */
#include "utils/float.h"
#include "utils/fmgroids.h"
#include "utils/jsonb.h"
#include "utils/numeric.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "statistics/statistics.h"


static int16
decode_stakind_string(char *s);

static
void import_pg_statistic(Relation rel, bool inh, JsonbContainer *cont);

static
void import_stakinds(const VacAttrStats *stat, JsonbContainer *cont,
					 bool inh, int16 kindenums[], Datum kindvalues[],
					 bool kindnulls[], bool kindreplaces[], Datum opvalues[],
					 bool opnulls[], bool opreplaces[], Datum collvalues[],
					 bool collnulls[], bool collreplaces[]);

static
void import_stanumbers(const VacAttrStats *stat, JsonbContainer *cont,
					   Datum kindvalues[], bool kindnulls[],
					   bool kindreplaces[]);

static
void import_stavalues(const VacAttrStats *stat, JsonbContainer *cont,
					  int16 kindenums[], Datum valvalues[],
					  bool valnulls[], bool valreplaces[]);


/*
 * Import staistic from:
 *   root->"regular"
 *   and
 *   root->"inherited"
 *
 * Container format is:
 *
 * {
 * 	 "colname1": { ...per column stats... },
 * 	 "colname2": { ...per column stats... },
 *   ...
 * }
 *
 */
static
void import_pg_statistic(Relation rel, bool inh, JsonbContainer *cont)
{
	TupleDesc   		tupdesc = RelationGetDescr(rel);
	Oid					relid = RelationGetRelid(rel);
	int					natts = tupdesc->natts;
	CatalogIndexState	indstate = NULL;
	Relation			sd;
	int					i;
	bool				has_index_exprs = false;
	ListCell		   *indexpr_item = NULL;

	if (cont == NULL)
		return;

	sd = table_open(StatisticRelationId, RowExclusiveLock);

	/*
	 * If this relation is an index and that index has expressions in
	 * it, then we will need to keep the list of remaining expressions
	 * aligned with the attributes as we iterate over them, whether or
	 * not those attributes have statistics to import.
	 */
	if ((rel->rd_rel->relkind == RELKIND_INDEX
			|| (rel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX))
		 && (rel->rd_indexprs != NIL))
	{
		has_index_exprs = true;
		indexpr_item = list_head(rel->rd_indexprs);
	}

	for (i = 0; i < natts; i++)
	{

		Form_pg_attribute	att;
		char			   *name;
		JsonbContainer	   *attrcont;
		VacAttrStats	   *stat;
		Node			   *index_expr = NULL;

		att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped)
			continue;

		if (has_index_exprs && (rel->rd_index->indkey.values[i] == 0))
		{
			if (indexpr_item == NULL)   /* shouldn't happen */
				elog(ERROR, "too few entries in indexprs list");

			index_expr = (Node *) lfirst(indexpr_item);
			indexpr_item = lnext(rel->rd_indexprs, indexpr_item);
		}

		stat = examine_rel_attribute(att, rel, index_expr);

		name = NameStr(att->attname);

		attrcont = key_lookup_object(cont, name);

		if (attrcont != NULL)
		{
			Datum		values[Natts_pg_statistic] = { 0 };
			bool		nulls[Natts_pg_statistic] = { false };
			bool		replaces[Natts_pg_statistic] = { false };
			HeapTuple	stup,
						oldtup;

			import_attribute(relid, stat, attrcont, inh, values, nulls, replaces);

			/* Is there already a pg_statistic tuple for this attribute? */
			oldtup = SearchSysCache3(STATRELATTINH,
									 ObjectIdGetDatum(RelationGetRelid(rel)),
									 Int16GetDatum(att->attnum),
									 BoolGetDatum(inh));

			/* Open index information when we know we need it */
			if (indstate == NULL)
				indstate = CatalogOpenIndexes(sd);

			if (HeapTupleIsValid(oldtup))
			{
				/* Yes, replace it */
				stup = heap_modify_tuple(oldtup,
										 RelationGetDescr(sd),
										 values,
										 nulls,
										 replaces);
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
		}
		/* DEBUG pfree(stat); */
	}

	if (indstate != NULL)
		CatalogCloseIndexes(indstate);
	table_close(sd, RowExclusiveLock);
}

/*
 * Import statitics for one attribute
 *
 */
void
import_attribute(Oid relid, const VacAttrStats *stat,
				 JsonbContainer *cont, bool inh,
				 Datum values[], bool nulls[], bool replaces[])
{
	JsonbContainer *arraycont;
	char		   *s;
	int16			kindenums[STATISTIC_NUM_SLOTS] = {0};

	Assert(cont != NULL);

	values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(stat->tupattnum);
	values[Anum_pg_statistic_stainherit - 1] = BoolGetDatum(inh);

	s = key_lookup_cstring(cont, "stanullfrac");
	if (s != NULL)
	{
		float4 f = float4in_internal(s, NULL, "real", s, NULL);
		pfree(s);
		values[Anum_pg_statistic_stanullfrac - 1] = Float4GetDatum(f);
		replaces[Anum_pg_statistic_stanullfrac - 1] = true;
	}

	s = key_lookup_cstring(cont, "stawidth");
	if (s != NULL)
	{
		int32 d = pg_strtoint32(s);
		pfree(s);
		values[Anum_pg_statistic_stawidth - 1] = Int32GetDatum(d);
		replaces[Anum_pg_statistic_stawidth - 1] = true;
	}

	s = key_lookup_cstring(cont, "stadistinct");
	if (s != NULL)
	{
		float4 f = float4in_internal(s, NULL, "real", s, NULL);
		pfree(s);
		values[Anum_pg_statistic_stadistinct - 1] = Float4GetDatum(f);
		replaces[Anum_pg_statistic_stadistinct - 1] = true;
	}

	arraycont = key_lookup_array(cont, "stakinds");
	import_stakinds(stat, arraycont, inh, kindenums,
					&values[Anum_pg_statistic_stakind1 - 1],
					&nulls[Anum_pg_statistic_stakind1 - 1],
					&replaces[Anum_pg_statistic_stakind1 - 1],
					&values[Anum_pg_statistic_staop1 - 1],
					&nulls[Anum_pg_statistic_staop1 - 1],
					&replaces[Anum_pg_statistic_staop1 - 1],
					&values[Anum_pg_statistic_stacoll1 - 1],
					&nulls[Anum_pg_statistic_stacoll1 - 1],
					&replaces[Anum_pg_statistic_stacoll1 - 1]);

	arraycont = key_lookup_array(cont, "stanumbers");
	import_stanumbers(stat, arraycont,
					  &values[Anum_pg_statistic_stanumbers1 - 1],
					  &nulls[Anum_pg_statistic_stanumbers1 - 1],
					  &replaces[Anum_pg_statistic_stanumbers1 - 1]);

	arraycont = key_lookup_array(cont, "stavalues");
	import_stavalues(stat, arraycont, kindenums,
					 &values[Anum_pg_statistic_stavalues1 - 1],
					 &nulls[Anum_pg_statistic_stavalues1 - 1],
					 &replaces[Anum_pg_statistic_stavalues1 - 1]);
}

/*
 * import stakinds values from json, the values of which determine
 * the staop and stacoll values to use as well.
 */
static
void import_stakinds(const VacAttrStats *stat, JsonbContainer *cont,
					 bool inh, int16 kindenums[], Datum kindvalues[],
					 bool kindnulls[], bool kindreplaces[], Datum opvalues[],
					 bool opnulls[], bool opreplaces[], Datum collvalues[],
					 bool collnulls[], bool collreplaces[])
{
	int k;
	int numkinds = 0;

	if (cont != NULL)
	{
		TypeCacheEntry *typentry = lookup_type_cache(stat->attrtypid,
													TYPECACHE_LT_OPR | TYPECACHE_EQ_OPR);
		Datum	lt_opr = ObjectIdGetDatum(typentry->lt_opr);
		Datum	eq_opr = ObjectIdGetDatum(typentry->eq_opr);

		numkinds = JsonContainerSize(cont);

		if (numkinds > STATISTIC_NUM_SLOTS)
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("invalid format: number of stakinds %d is greater than available slots %d",
						numkinds, STATISTIC_NUM_SLOTS)));

		for (k = 0; k < numkinds; k++)
		{
			JsonbValue *j = getIthJsonbValueFromContainer(cont, k);
			int16		kind;
			char	   *s;

			if (j == NULL || (j->type != jbvString))
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("invalid format: stakind elements must be strings")));

			s = JsonbStringValueToCString(j);
			kind = decode_stakind_string(s);
			pfree(s);
			pfree(j);

			kindenums[k] = kind;
			kindvalues[k] = Int16GetDatum(kind);
			kindreplaces[k] = true;

			switch(kind)
			{
				case STATISTIC_KIND_MCV:
					opvalues[k] = eq_opr;
					opreplaces[k] = true;
					collvalues[k] = ObjectIdGetDatum(stat->attrcollid);
					collreplaces[k] = true;
					break;

				case STATISTIC_KIND_HISTOGRAM:
				case STATISTIC_KIND_CORRELATION:
					opvalues[k] = lt_opr;
					opreplaces[k] = true;
					collvalues[k] = ObjectIdGetDatum(stat->attrcollid);
					collreplaces[k] = true;
					break;

				case STATISTIC_KIND_MCELEM:
				case STATISTIC_KIND_DECHIST:
					opvalues[k] = ObjectIdGetDatum(TextEqualOperator);
					opreplaces[k] = true;
					collvalues[k] = ObjectIdGetDatum(DEFAULT_COLLATION_OID);
					collreplaces[k] = true;
					break;

				case STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM:
					opvalues[k] = ObjectIdGetDatum(Float8LessOperator);
					opreplaces[k] = true;
					collvalues[k] = ObjectIdGetDatum(InvalidOid);
					collreplaces[k] = true;
					break;

				case STATISTIC_KIND_BOUNDS_HISTOGRAM:
				default:
					opvalues[k] = ObjectIdGetDatum(InvalidOid);
					opreplaces[k] = true;
					collvalues[k] = ObjectIdGetDatum(InvalidOid);
					collreplaces[k] = true;
					break;
			}
		}
	}

	/* fill out empty slots, but do not replace */
	for (k = numkinds; k < STATISTIC_NUM_SLOTS; k++)
	{
		kindvalues[k] = Int16GetDatum(0);
		opvalues[k] = ObjectIdGetDatum(InvalidOid);
		collvalues[k] = ObjectIdGetDatum(InvalidOid);
	}
}

static
void import_stanumbers(const VacAttrStats *stat, JsonbContainer *cont,
					   Datum numvalues[], bool numnulls[],
					   bool numreplaces[])
{
	int numnumbers = 0;
	int k;

	if (cont != NULL)
	{
		FmgrInfo	finfo;

		numnumbers = JsonContainerSize(cont);

		if (numnumbers > STATISTIC_NUM_SLOTS)
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("invalid format: number of stanumbers arrays (%d) is greater than available slots %d",
						numnumbers, STATISTIC_NUM_SLOTS)));

		fmgr_info(F_ARRAY_IN, &finfo);

		for (k = 0; k < numnumbers; k++)
		{
			JsonbValue *j = getIthJsonbValueFromContainer(cont, k);

			if (j == NULL)
			{
				numvalues[k] = (Datum) 0;
				numnulls[k] = true;
				continue;
			}

			if (j->type == jbvNull)
			{
				numvalues[k] = (Datum) 0;
				numnulls[k] = true;
				pfree(j);
				continue;
			}

			if (j->type == jbvString)
			{
				char *s = JsonbStringValueToCString(j);

				numvalues[k] = FunctionCall3(&finfo, CStringGetDatum(s),
											 ObjectIdGetDatum(FLOAT4OID),
											 Int32GetDatum(0));
				numreplaces[k] = true;
				pfree(s);
				pfree(j);
				continue;
			}
			else
				ereport(ERROR,
				  (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				   errmsg("invalid statistics format, stanumbers elements "
						  "must be a string that is castable to an array of floats")));

		}
	}

	/* fill out empty slots, but do not replace */
	for (k = numnumbers; k < STATISTIC_NUM_SLOTS; k++)
	{
		numvalues[k] = (Datum) 0;
		numnulls[k] = true;
	}
}

static
void import_stavalues(const VacAttrStats *stat, JsonbContainer *cont,
					  int16 kindenums[], Datum valvalues[],
					  bool valnulls[], bool valreplaces[])
{
	int numvals = 0;
	int k;

	if (cont != NULL)
	{
		FmgrInfo	finfo;

		fmgr_info(F_ARRAY_IN, &finfo);
		numvals = JsonContainerSize(cont);

		if (numvals > STATISTIC_NUM_SLOTS)
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("invalid format: number of stavalues %d is greater than available slots %d",
						numvals, STATISTIC_NUM_SLOTS)));

		for (k = 0; k < numvals; k++)
		{
			JsonbValue *j = getIthJsonbValueFromContainer(cont, k);

			if (j == NULL)
			{
				valvalues[k] = (Datum) 0;
				valnulls[k] = true;
				continue;
			}

			if (j->type == jbvNull)
			{
				valvalues[k] = (Datum) 0;
				valnulls[k] = true;
				pfree(j);
				continue;
			}

			if (j->type == jbvString)
			{
				char   *s = JsonbStringValueToCString(j);
				Oid		typoid = stat->statypid[k];
				int32	typmod = 0;

				/*
				 * MCELEM stat arrays are of the same type as the
				 * array base element type.
				 */
				if (kindenums[k] == STATISTIC_KIND_MCELEM)
				{
					TypeCacheEntry *typentry = lookup_type_cache(typoid, 0);
					if (IsTrueArrayType(typentry))
						typoid = typentry->typelem;
				}
				valvalues[k] = FunctionCall3(&finfo, CStringGetDatum(s),
											 ObjectIdGetDatum(typoid),
											 Int32GetDatum(typmod));
				valreplaces[k] = true;
				pfree(s);
				pfree(j);
			}
			else
				ereport(ERROR,
				  (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				   errmsg("invalid statistics format, stavalues elements must "
						  "be a string that is castable to an array of the "
						  "column type")));

		}
	}

	/* fill out empty slots, but do not replace */
	for (k = numvals; k < STATISTIC_NUM_SLOTS; k++)
	{
		valvalues[k] = (Datum) 0;
		valnulls[k] = true;
	}
}

/*
 * Get a JsonbValue from a JsonbContainer and ensure that it is a string,
 * and return the cstring.
 */
char *key_lookup_cstring(JsonbContainer *cont, const char *key)
{
	JsonbValue	j;

	if (!getKeyJsonValueFromContainer(cont, key, strlen(key), &j))
		return NULL;

	if (j.type != jbvString)
		ereport(ERROR,
		  (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		   errmsg("invalid statistics format, %s must be a string but is type %s",
				  key, JsonbTypeName(&j))));

	return JsonbStringValueToCString(&j);
}

/*
 * Get a JsonbContainer from a JsonbContainer and ensure that it is a object
 */
JsonbContainer *key_lookup_object(JsonbContainer *cont, const char *key)
{
	JsonbValue		j;

	if (!getKeyJsonValueFromContainer(cont, key, strlen(key), &j))
		return NULL;

	if (j.type == jbvNull)
		return NULL;

	if (j.type != jbvBinary)
		ereport(ERROR,
		  (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		   errmsg("invalid statistics format, %s must be an object or null but is type %s",
				  key, JsonbTypeName(&j))));

	if (!JsonContainerIsObject(j.val.binary.data))
		ereport(ERROR,
		  (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		   errmsg("invalid statistics format, %s must be an object or null but is type %s",
				  key, JsonbContainerTypeName(j.val.binary.data))));

	return j.val.binary.data;
}

/*
 * Get a JsonbContainer from a JsonbContainer and ensure that it is an array
 */
JsonbContainer *key_lookup_array(JsonbContainer *cont, const char *key)
{
	JsonbValue	j;

	if (!getKeyJsonValueFromContainer(cont, key, strlen(key), &j))
		return NULL;

	if (j.type == jbvNull)
		return NULL;

	if (j.type != jbvBinary)
		ereport(ERROR,
		  (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		   errmsg("invalid statistics format, %s must be an array or null but is type %s",
				  key, JsonbTypeName(&j))));

	if (!JsonContainerIsArray(j.val.binary.data))
		ereport(ERROR,
		  (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		   errmsg("invalid statistics format, %s must be an array or null but is type %s",
				  key, JsonbContainerTypeName(j.val.binary.data))));

	return j.val.binary.data;
}

/*
 * Convert the STATISTICS_KIND strings defined in pg_statistic_export
 * back to their defined enum values.
 */
static int16
decode_stakind_string(char *s)
{
	if (strcmp(s,"MCV") == 0)
		return STATISTIC_KIND_MCV;
	if (strcmp(s,"HISTOGRAM") == 0)
		return STATISTIC_KIND_HISTOGRAM;
	if (strcmp(s,"CORRELATION") == 0)
		return STATISTIC_KIND_CORRELATION;
	if (strcmp(s,"MCELEM") == 0)
		return STATISTIC_KIND_MCELEM;
	if (strcmp(s,"DECHIST") == 0)
		return STATISTIC_KIND_DECHIST;
	if (strcmp(s,"RANGE_LENGTH_HISTOGRAM") == 0)
		return STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM;
	if (strcmp(s,"BOUNDS_HISTOGRAM") == 0)
		return STATISTIC_KIND_BOUNDS_HISTOGRAM;
	if (strcmp(s,"TRIVIAL") != 0)
		ereport(ERROR,
		  (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		   errmsg("unknown statistics kind: %s", s)));

	return 0;
}

/*
 * examine_rel_attribute -- pre-analysis of a single column
 *
 * Determine whether the column is analyzable; if so, create and initialize
 * a VacAttrStats struct for it.  If not, return NULL.
 *
 * If index_expr isn't NULL, then we're trying to import an expression index,
 * and index_expr is the expression tree representing the column's data.
 */
VacAttrStats *
examine_rel_attribute(Form_pg_attribute attr, Relation onerel, Node *index_expr)
{
	HeapTuple		typtuple;
	int				i;
	bool			ok;
	VacAttrStats   *stats;

	/* Never analyze dropped columns */
	if (attr->attisdropped)
		return NULL;

	/* Don't analyze column if user has specified not to */
	if (attr->attstattarget == 0)
		return NULL;

	/*
	 * Create the VacAttrStats struct.
	 */
	stats = (VacAttrStats *) palloc0(sizeof(VacAttrStats));
	stats->attstattarget = attr->attstattarget;

	/*
	 * When analyzing an expression index, believe the expression tree's type
	 * not the column datatype --- the latter might be the opckeytype storage
	 * type of the opclass, which is not interesting for our purposes.  (Note:
	 * if we did anything with non-expression index columns, we'd need to
	 * figure out where to get the correct type info from, but for now that's
	 * not a problem.)	It's not clear whether anyone will care about the
	 * typmod, but we store that too just in case.
	 */
	if (index_expr)
	{
		stats->attrtypid = exprType(index_expr);
		stats->attrtypmod = exprTypmod(index_expr);

		/*
		 * If a collation has been specified for the index column, use that in
		 * preference to anything else; but if not, fall back to whatever we
		 * can get from the expression.
		 */
		if (OidIsValid(onerel->rd_indcollation[attr->attnum - 1]))
			stats->attrcollid = onerel->rd_indcollation[attr->attnum - 1];
		else
			stats->attrcollid = exprCollation(index_expr);
	}
	else
	{
		stats->attrtypid = attr->atttypid;
		stats->attrtypmod = attr->atttypmod;
		stats->attrcollid = attr->attcollation;
	}

	typtuple = SearchSysCacheCopy1(TYPEOID,
								   ObjectIdGetDatum(stats->attrtypid));
	if (!HeapTupleIsValid(typtuple))
		elog(ERROR, "cache lookup failed for type %u", stats->attrtypid);
	stats->attrtype = (Form_pg_type) GETSTRUCT(typtuple);
	stats->anl_context = NULL; /*DEBUG CurrentMemoryContext; */
	stats->tupattnum = attr->attnum;

	/*
	 * The fields describing the stats->stavalues[n] element types default to
	 * the type of the data being analyzed, but the type-specific typanalyze
	 * function can change them if it wants to store something else.
	 */
	for (i = 0; i < STATISTIC_NUM_SLOTS; i++)
	{
		stats->statypid[i] = stats->attrtypid;
		stats->statyplen[i] = stats->attrtype->typlen;
		stats->statypbyval[i] = stats->attrtype->typbyval;
		stats->statypalign[i] = stats->attrtype->typalign;
	}

	/*
	 * Call the type-specific typanalyze function.  If none is specified, use
	 * std_typanalyze().
	 */
	if (OidIsValid(stats->attrtype->typanalyze))
		ok = DatumGetBool(OidFunctionCall1(stats->attrtype->typanalyze,
										   PointerGetDatum(stats)));
	else
		ok = std_typanalyze(stats);

	if (!ok || stats->compute_stats == NULL || stats->minrows <= 0)
	{
		heap_freetuple(typtuple);
		pfree(stats);
		return NULL;
	}

	return stats;
}

/*
 * Import statistics (pg_statistic) into a relation
 */
Datum
pg_import_rel_stats(PG_FUNCTION_ARGS)
{
	Oid			relid;
	int32		stats_version_num;
	Jsonb	   *jb;
	Relation	rel;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
		  (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		   errmsg("relation cannot be NULL")));
	relid = PG_GETARG_OID(0);

	if (PG_ARGISNULL(1))
		ereport(ERROR,
		  (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		   errmsg("server_version_number cannot be NULL")));
	stats_version_num = PG_GETARG_INT32(1);

	if (stats_version_num < 80000)
		ereport(ERROR,
		  (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		   errmsg("invalid statistics version: %d is earlier than earliest supported version",
				  stats_version_num)));

	if (PG_ARGISNULL(4))
		jb = NULL;
	else
	{
		jb = PG_GETARG_JSONB_P(4);
		if (!JB_ROOT_IS_OBJECT(jb))
			ereport(ERROR,
			  (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			   errmsg("columns must be jsonb object at root")));
	}

	rel = relation_open(relid, ShareUpdateExclusiveLock);

	/*
	 * Apply statistical updates, if any, to copied tuple.
	 *
	 * Format is:
	 * {
	 *   "regular": { "columns": ..., "extended": ...},
	 *   "inherited": { "columns": ..., "extended": ...}
	 * }
	 *
	 */
	if (jb != NULL)
	{
		JsonbContainer	   *cont;

		cont = key_lookup_object(&jb->root, "regular");
		import_pg_statistic(rel, false, cont);

		if (rel->rd_rel->relhassubclass)
		{
			cont = key_lookup_object(&jb->root, "inherited");
			import_pg_statistic(rel, true, cont);
		}
	}

	/* only modify pg_class row if changes are to be made */
	if ( ! PG_ARGISNULL(2) || ! PG_ARGISNULL(3) )
	{
		Relation		pg_class_rel;
		HeapTuple		ctup;
		Form_pg_class	pgcform;

		/*
		 * Open the relation, getting ShareUpdateExclusiveLock to ensure that no
		 * other stat-setting operation can run on it concurrently.
		 */
		pg_class_rel = table_open(RelationRelationId, ShareUpdateExclusiveLock);

		/* leave if relation could not be opened or locked */
		if (!pg_class_rel)
			PG_RETURN_BOOL(false);

		ctup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
		if (!HeapTupleIsValid(ctup))
			elog(ERROR, "pg_class entry for relid %u vanished during statistics import",
				 relid);
		pgcform = (Form_pg_class) GETSTRUCT(ctup);

		/* leave un-set values alone */
		if (! PG_ARGISNULL(2))
			pgcform->reltuples = PG_GETARG_FLOAT4(2);
		if (! PG_ARGISNULL(3))
			pgcform->relpages = PG_GETARG_INT32(3);

		heap_inplace_update(pg_class_rel, ctup);
		table_close(pg_class_rel, ShareUpdateExclusiveLock);
	}

	/* relation_close(onerel, ShareUpdateExclusiveLock); */
	relation_close(rel, NoLock);

	PG_RETURN_BOOL(true);
}
