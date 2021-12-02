/*-------------------------------------------------------------------------
 *
 * extended_stats.c
 *	  POSTGRES extended statistics
 *
 * Generic code supporting statistics objects created via CREATE STATISTICS.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/statistics/extended_stats.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/detoast.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_statistic_ext_data.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "commands/progress.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "rewrite/rewriteManip.h"
#include "statistics/extended_stats_internal.h"
#include "statistics/statistics.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/attoptcache.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

/*
 * To avoid consuming too much memory during analysis and/or too much space
 * in the resulting pg_statistic rows, we ignore varlena datums that are wider
 * than WIDTH_THRESHOLD (after detoasting!).  This is legitimate for MCV
 * and distinct-value calculations since a wide value is unlikely to be
 * duplicated at all, much less be a most-common value.  For the same reason,
 * ignoring wide values will not affect our estimates of histogram bin
 * boundaries very much.
 */
#define WIDTH_THRESHOLD  1024

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
	List	   *types;			/* 'char' list of enabled statistics kinds */
	int			stattarget;		/* statistics target (-1 for default) */
	List	   *exprs;			/* expressions */
} StatExtEntry;


static List *fetch_statentries_for_relation(Relation pg_statext, Oid relid);
static VacAttrStats **lookup_var_attr_stats(Relation rel, Bitmapset *attrs, List *exprs,
											int nvacatts, VacAttrStats **vacatts);
static void statext_store(Oid statOid,
						  MVNDistinct *ndistinct, MVDependencies *dependencies,
						  MCVList *mcv, Datum exprs, VacAttrStats **stats);
static int	statext_compute_stattarget(int stattarget,
									   int natts, VacAttrStats **stats);

/* Information needed to analyze a single simple expression. */
typedef struct AnlExprData
{
	Node	   *expr;			/* expression to analyze */
	VacAttrStats *vacattrstat;	/* statistics attrs to analyze */
} AnlExprData;

static void compute_expr_stats(Relation onerel, double totalrows,
							   AnlExprData *exprdata, int nexprs,
							   HeapTuple *rows, int numrows);
static Datum serialize_expr_stats(AnlExprData *exprdata, int nexprs);
static Datum expr_fetch_func(VacAttrStatsP stats, int rownum, bool *isNull);
static AnlExprData *build_expr_data(List *exprs, int stattarget);

static StatsBuildData *make_build_data(Relation onerel, StatExtEntry *stat,
									   int numrows, HeapTuple *rows,
									   VacAttrStats **stats, int stattarget);

/*
 * Runtime samples used to estimate scans and joins.
 */
typedef struct Sample
{
	int			nrows;
	int			maxrows;

	Bitmapset  *attnums;
	List	   *exprs;

	/*
	 * We don't keep the original heap tuples, we extract and keep just the
	 * interesting attibutes to save space (hopefully).
	 *
	 * XXX We might deduplicate the values, which would save a lot of memory
	 * for data with a lot of repetitions. Which is quite common.
	 */
	Datum	   *values;
	bool	   *isnull;
} Sample;

static Sample *statext_collect_sample(PlannerInfo *root,
									  StatisticExtInfo *stat);

static Sample *statext_collect_correlated_sample(PlannerInfo *root,
												 StatisticExtInfo *stat,
												 StatisticExtInfo *stat2,
												 Sample *sample,
												 List *clauses);

static Selectivity sample_clauselist_selectivity(PlannerInfo *root,
												 StatisticExtInfo *stat, Sample *sample,
												 List *clauses, int varRelid,
												 JoinType jointype, SpecialJoinInfo *sjinfo,
												 RelOptInfo *rel, bool is_or);

static bool stat_covers_expressions(StatisticExtInfo *stat, List *exprs,
									Bitmapset **expr_idxs);

static List *statext_sample_get_conditions(PlannerInfo *root,
										   RelOptInfo *rel,
										   StatisticExtInfo *info);

static bool *statext_sample_eval_conditions(PlannerInfo *root, RelOptInfo *rel,
											StatisticExtInfo *stat, Sample *sample,
											Selectivity *sel);

/* various (mostly developer-oriented) GUCs */
bool enable_sample_estimates_scan = true;
bool enable_sample_estimates_join = true;
bool enable_sample_join_correlate = true;

/* 1% might be a bit too much, perhaps we should cap it to statistics target? */
double estimate_sample_rate = 0.01;

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
	List	   *statslist;
	MemoryContext cxt;
	MemoryContext oldcxt;
	int64		ext_cnt;

	/* Do nothing if there are no columns to analyze. */
	if (!natts)
		return;

	/* the list of stats has to be allocated outside the memory context */
	pg_stext = table_open(StatisticExtRelationId, RowExclusiveLock);
	statslist = fetch_statentries_for_relation(pg_stext, RelationGetRelid(onerel));

	/* memory context for building each statistics object */
	cxt = AllocSetContextCreate(CurrentMemoryContext,
								"BuildRelationExtStatistics",
								ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(cxt);

	/* report this phase */
	if (statslist != NIL)
	{
		const int	index[] = {
			PROGRESS_ANALYZE_PHASE,
			PROGRESS_ANALYZE_EXT_STATS_TOTAL
		};
		const int64 val[] = {
			PROGRESS_ANALYZE_PHASE_COMPUTE_EXT_STATS,
			list_length(statslist)
		};

		pgstat_progress_update_multi_param(2, index, val);
	}

	ext_cnt = 0;
	foreach(lc, statslist)
	{
		StatExtEntry *stat = (StatExtEntry *) lfirst(lc);
		MVNDistinct *ndistinct = NULL;
		MVDependencies *dependencies = NULL;
		MCVList    *mcv = NULL;
		Datum		exprstats = (Datum) 0;
		VacAttrStats **stats;
		ListCell   *lc2;
		int			stattarget;
		StatsBuildData *data;

		/*
		 * Check if we can build these stats based on the column analyzed. If
		 * not, report this fact (except in autovacuum) and move on.
		 */
		stats = lookup_var_attr_stats(onerel, stat->columns, stat->exprs,
									  natts, vacattrstats);
		if (!stats)
		{
			if (!IsAutoVacuumWorkerProcess())
				ereport(WARNING,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("statistics object \"%s.%s\" could not be computed for relation \"%s.%s\"",
								stat->schema, stat->name,
								get_namespace_name(onerel->rd_rel->relnamespace),
								RelationGetRelationName(onerel)),
						 errtable(onerel)));
			continue;
		}

		/* compute statistics target for this statistics object */
		stattarget = statext_compute_stattarget(stat->stattarget,
												bms_num_members(stat->columns),
												stats);

		/*
		 * Don't rebuild statistics objects with statistics target set to 0
		 * (we just leave the existing values around, just like we do for
		 * regular per-column statistics).
		 */
		if (stattarget == 0)
			continue;

		/* evaluate expressions (if the statistics object has any) */
		data = make_build_data(onerel, stat, numrows, rows, stats, stattarget);

		/* compute statistic of each requested type */
		foreach(lc2, stat->types)
		{
			char		t = (char) lfirst_int(lc2);

			if (t == STATS_EXT_NDISTINCT)
				ndistinct = statext_ndistinct_build(totalrows, data);
			else if (t == STATS_EXT_DEPENDENCIES)
				dependencies = statext_dependencies_build(data);
			else if (t == STATS_EXT_MCV)
				mcv = statext_mcv_build(data, totalrows, stattarget);
			else if (t == STATS_EXT_EXPRESSIONS)
			{
				AnlExprData *exprdata;
				int			nexprs;

				/* should not happen, thanks to checks when defining stats */
				if (!stat->exprs)
					elog(ERROR, "requested expression stats, but there are no expressions");

				exprdata = build_expr_data(stat->exprs, stattarget);
				nexprs = list_length(stat->exprs);

				compute_expr_stats(onerel, totalrows,
								   exprdata, nexprs,
								   rows, numrows);

				exprstats = serialize_expr_stats(exprdata, nexprs);
			}
		}

		/* store the statistics in the catalog */
		statext_store(stat->statOid, ndistinct, dependencies, mcv, exprstats, stats);

		/* for reporting progress */
		pgstat_progress_update_param(PROGRESS_ANALYZE_EXT_STATS_COMPUTED,
									 ++ext_cnt);

		/* free the data used for building this statistics object */
		MemoryContextReset(cxt);
	}

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(cxt);

	list_free(statslist);

	table_close(pg_stext, RowExclusiveLock);
}

/*
 * ComputeExtStatisticsRows
 *		Compute number of rows required by extended statistics on a table.
 *
 * Computes number of rows we need to sample to build extended statistics on a
 * table. This only looks at statistics we can actually build - for example
 * when analyzing only some of the columns, this will skip statistics objects
 * that would require additional columns.
 *
 * See statext_compute_stattarget for details about how we compute the
 * statistics target for a statistics object (from the object target,
 * attribute targets and default statistics target).
 */
int
ComputeExtStatisticsRows(Relation onerel,
						 int natts, VacAttrStats **vacattrstats)
{
	Relation	pg_stext;
	ListCell   *lc;
	List	   *lstats;
	MemoryContext cxt;
	MemoryContext oldcxt;
	int			result = 0;

	/* If there are no columns to analyze, just return 0. */
	if (!natts)
		return 0;

	cxt = AllocSetContextCreate(CurrentMemoryContext,
								"ComputeExtStatisticsRows",
								ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(cxt);

	pg_stext = table_open(StatisticExtRelationId, RowExclusiveLock);
	lstats = fetch_statentries_for_relation(pg_stext, RelationGetRelid(onerel));

	foreach(lc, lstats)
	{
		StatExtEntry *stat = (StatExtEntry *) lfirst(lc);
		int			stattarget;
		VacAttrStats **stats;
		int			nattrs = bms_num_members(stat->columns);

		/*
		 * Check if we can build this statistics object based on the columns
		 * analyzed. If not, ignore it (don't report anything, we'll do that
		 * during the actual build BuildRelationExtStatistics).
		 */
		stats = lookup_var_attr_stats(onerel, stat->columns, stat->exprs,
									  natts, vacattrstats);

		if (!stats)
			continue;

		/*
		 * Compute statistics target, based on what's set for the statistic
		 * object itself, and for its attributes.
		 */
		stattarget = statext_compute_stattarget(stat->stattarget,
												nattrs, stats);

		/* Use the largest value for all statistics objects. */
		if (stattarget > result)
			result = stattarget;
	}

	table_close(pg_stext, RowExclusiveLock);

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(cxt);

	/* compute sample size based on the statistics target */
	return (300 * result);
}

/*
 * statext_compute_stattarget
 *		compute statistics target for an extended statistic
 *
 * When computing target for extended statistics objects, we consider three
 * places where the target may be set - the statistics object itself,
 * attributes the statistics object is defined on, and then the default
 * statistics target.
 *
 * First we look at what's set for the statistics object itself, using the
 * ALTER STATISTICS ... SET STATISTICS command. If we find a valid value
 * there (i.e. not -1) we're done. Otherwise we look at targets set for any
 * of the attributes the statistic is defined on, and if there are columns
 * with defined target, we use the maximum value. We do this mostly for
 * backwards compatibility, because this is what we did before having
 * statistics target for extended statistics.
 *
 * And finally, if we still don't have a statistics target, we use the value
 * set in default_statistics_target.
 */
static int
statext_compute_stattarget(int stattarget, int nattrs, VacAttrStats **stats)
{
	int			i;

	/*
	 * If there's statistics target set for the statistics object, use it. It
	 * may be set to 0 which disables building of that statistic.
	 */
	if (stattarget >= 0)
		return stattarget;

	/*
	 * The target for the statistics object is set to -1, in which case we
	 * look at the maximum target set for any of the attributes the object is
	 * defined on.
	 */
	for (i = 0; i < nattrs; i++)
	{
		/* keep the maximum statistics target */
		if (stats[i]->attr->attstattarget > stattarget)
			stattarget = stats[i]->attr->attstattarget;
	}

	/*
	 * If the value is still negative (so neither the statistics object nor
	 * any of the columns have custom statistics target set), use the global
	 * default target.
	 */
	if (stattarget < 0)
		stattarget = default_statistics_target;

	/* As this point we should have a valid statistics target. */
	Assert((stattarget >= 0) && (stattarget <= 10000));

	return stattarget;
}

/*
 * statext_is_kind_built
 *		Is this stat kind built in the given pg_statistic_ext_data tuple?
 */
bool
statext_is_kind_built(HeapTuple htup, char type)
{
	AttrNumber	attnum;

	switch (type)
	{
		case STATS_EXT_NDISTINCT:
			attnum = Anum_pg_statistic_ext_data_stxdndistinct;
			break;

		case STATS_EXT_DEPENDENCIES:
			attnum = Anum_pg_statistic_ext_data_stxddependencies;
			break;

		case STATS_EXT_MCV:
			attnum = Anum_pg_statistic_ext_data_stxdmcv;
			break;

		case STATS_EXT_EXPRESSIONS:
			attnum = Anum_pg_statistic_ext_data_stxdexpr;
			break;

		default:
			elog(ERROR, "unexpected statistics type requested: %d", type);
	}

	return !heap_attisnull(htup, attnum, NULL);
}

/*
 * statext_is_kind_enabled
 *		Is this stat kind enabled in the given pg_statistic_ext tuple?
 */
bool
statext_is_kind_enabled(HeapTuple htup, char type)
{
	int			i;
	ArrayType  *arr;
	char	   *enabled;

	Datum		datum;
	bool		isnull;

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
			   (enabled[i] == STATS_EXT_EXPRESSIONS) ||
			   (enabled[i] == STATS_EXT_SAMPLE));

		if (enabled[i] == type)
			return true;
	}

	return false;
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
		List	   *exprs = NIL;

		entry = palloc0(sizeof(StatExtEntry));
		staForm = (Form_pg_statistic_ext) GETSTRUCT(htup);
		entry->statOid = staForm->oid;
		entry->schema = get_namespace_name(staForm->stxnamespace);
		entry->name = pstrdup(NameStr(staForm->stxname));
		entry->stattarget = staForm->stxstattarget;
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
				   (enabled[i] == STATS_EXT_EXPRESSIONS) ||
				   (enabled[i] == STATS_EXT_SAMPLE));
			entry->types = lappend_int(entry->types, (int) enabled[i]);
		}

		/* decode expression (if any) */
		datum = SysCacheGetAttr(STATEXTOID, htup,
								Anum_pg_statistic_ext_stxexprs, &isnull);

		if (!isnull)
		{
			char	   *exprsString;

			exprsString = TextDatumGetCString(datum);
			exprs = (List *) stringToNode(exprsString);

			pfree(exprsString);

			/*
			 * Run the expressions through eval_const_expressions. This is not
			 * just an optimization, but is necessary, because the planner
			 * will be comparing them to similarly-processed qual clauses, and
			 * may fail to detect valid matches without this.  We must not use
			 * canonicalize_qual, however, since these aren't qual
			 * expressions.
			 */
			exprs = (List *) eval_const_expressions(NULL, (Node *) exprs);

			/* May as well fix opfuncids too */
			fix_opfuncids((Node *) exprs);
		}

		entry->exprs = exprs;

		result = lappend(result, entry);
	}

	systable_endscan(scan);

	return result;
}

/*
 * examine_attribute -- pre-analysis of a single column
 *
 * Determine whether the column is analyzable; if so, create and initialize
 * a VacAttrStats struct for it.  If not, return NULL.
 */
static VacAttrStats *
examine_attribute(Node *expr)
{
	HeapTuple	typtuple;
	VacAttrStats *stats;
	int			i;
	bool		ok;

	/*
	 * Create the VacAttrStats struct.  Note that we only have a copy of the
	 * fixed fields of the pg_attribute tuple.
	 */
	stats = (VacAttrStats *) palloc0(sizeof(VacAttrStats));

	/* fake the attribute */
	stats->attr = (Form_pg_attribute) palloc0(ATTRIBUTE_FIXED_PART_SIZE);
	stats->attr->attstattarget = -1;

	/*
	 * When analyzing an expression, believe the expression tree's type not
	 * the column datatype --- the latter might be the opckeytype storage type
	 * of the opclass, which is not interesting for our purposes.  (Note: if
	 * we did anything with non-expression statistics columns, we'd need to
	 * figure out where to get the correct type info from, but for now that's
	 * not a problem.)	It's not clear whether anyone will care about the
	 * typmod, but we store that too just in case.
	 */
	stats->attrtypid = exprType(expr);
	stats->attrtypmod = exprTypmod(expr);
	stats->attrcollid = exprCollation(expr);

	typtuple = SearchSysCacheCopy1(TYPEOID,
								   ObjectIdGetDatum(stats->attrtypid));
	if (!HeapTupleIsValid(typtuple))
		elog(ERROR, "cache lookup failed for type %u", stats->attrtypid);
	stats->attrtype = (Form_pg_type) GETSTRUCT(typtuple);

	/*
	 * We don't actually analyze individual attributes, so no need to set the
	 * memory context.
	 */
	stats->anl_context = NULL;
	stats->tupattnum = InvalidAttrNumber;

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
		pfree(stats->attr);
		pfree(stats);
		return NULL;
	}

	return stats;
}

/*
 * examine_expression -- pre-analysis of a single expression
 *
 * Determine whether the expression is analyzable; if so, create and initialize
 * a VacAttrStats struct for it.  If not, return NULL.
 */
static VacAttrStats *
examine_expression(Node *expr, int stattarget)
{
	HeapTuple	typtuple;
	VacAttrStats *stats;
	int			i;
	bool		ok;

	Assert(expr != NULL);

	/*
	 * Create the VacAttrStats struct.
	 */
	stats = (VacAttrStats *) palloc0(sizeof(VacAttrStats));

	/*
	 * When analyzing an expression, believe the expression tree's type.
	 */
	stats->attrtypid = exprType(expr);
	stats->attrtypmod = exprTypmod(expr);

	/*
	 * We don't allow collation to be specified in CREATE STATISTICS, so we
	 * have to use the collation specified for the expression. It's possible
	 * to specify the collation in the expression "(col COLLATE "en_US")" in
	 * which case exprCollation() does the right thing.
	 */
	stats->attrcollid = exprCollation(expr);

	/*
	 * We don't have any pg_attribute for expressions, so let's fake something
	 * reasonable into attstattarget, which is the only thing std_typanalyze
	 * needs.
	 */
	stats->attr = (Form_pg_attribute) palloc(ATTRIBUTE_FIXED_PART_SIZE);

	/*
	 * We can't have statistics target specified for the expression, so we
	 * could use either the default_statistics_target, or the target computed
	 * for the extended statistics. The second option seems more reasonable.
	 */
	stats->attr->attstattarget = stattarget;

	/* initialize some basic fields */
	stats->attr->attrelid = InvalidOid;
	stats->attr->attnum = InvalidAttrNumber;
	stats->attr->atttypid = stats->attrtypid;

	typtuple = SearchSysCacheCopy1(TYPEOID,
								   ObjectIdGetDatum(stats->attrtypid));
	if (!HeapTupleIsValid(typtuple))
		elog(ERROR, "cache lookup failed for type %u", stats->attrtypid);

	stats->attrtype = (Form_pg_type) GETSTRUCT(typtuple);
	stats->anl_context = CurrentMemoryContext;	/* XXX should be using
												 * something else? */
	stats->tupattnum = InvalidAttrNumber;

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
 * Using 'vacatts' of size 'nvacatts' as input data, return a newly-built
 * VacAttrStats array which includes only the items corresponding to
 * attributes indicated by 'attrs'.  If we don't have all of the per-column
 * stats available to compute the extended stats, then we return NULL to
 * indicate to the caller that the stats should not be built.
 */
static VacAttrStats **
lookup_var_attr_stats(Relation rel, Bitmapset *attrs, List *exprs,
					  int nvacatts, VacAttrStats **vacatts)
{
	int			i = 0;
	int			x = -1;
	int			natts;
	VacAttrStats **stats;
	ListCell   *lc;

	natts = bms_num_members(attrs) + list_length(exprs);

	stats = (VacAttrStats **) palloc(natts * sizeof(VacAttrStats *));

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

	/* also add info for expressions */
	foreach(lc, exprs)
	{
		Node	   *expr = (Node *) lfirst(lc);

		stats[i] = examine_attribute(expr);

		/*
		 * XXX We need tuple descriptor later, and we just grab it from
		 * stats[0]->tupDesc (see e.g. statext_mcv_build). But as coded
		 * examine_attribute does not set that, so just grab it from the first
		 * vacatts element.
		 */
		stats[i]->tupDesc = vacatts[0]->tupDesc;

		i++;
	}

	return stats;
}

/*
 * statext_store
 *	Serializes the statistics and stores them into the pg_statistic_ext_data
 *	tuple.
 */
static void
statext_store(Oid statOid,
			  MVNDistinct *ndistinct, MVDependencies *dependencies,
			  MCVList *mcv, Datum exprs, VacAttrStats **stats)
{
	Relation	pg_stextdata;
	HeapTuple	stup,
				oldtup;
	Datum		values[Natts_pg_statistic_ext_data];
	bool		nulls[Natts_pg_statistic_ext_data];
	bool		replaces[Natts_pg_statistic_ext_data];

	pg_stextdata = table_open(StatisticExtDataRelationId, RowExclusiveLock);

	memset(nulls, true, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));
	memset(values, 0, sizeof(values));

	/*
	 * Construct a new pg_statistic_ext_data tuple, replacing the calculated
	 * stats.
	 */
	if (ndistinct != NULL)
	{
		bytea	   *data = statext_ndistinct_serialize(ndistinct);

		nulls[Anum_pg_statistic_ext_data_stxdndistinct - 1] = (data == NULL);
		values[Anum_pg_statistic_ext_data_stxdndistinct - 1] = PointerGetDatum(data);
	}

	if (dependencies != NULL)
	{
		bytea	   *data = statext_dependencies_serialize(dependencies);

		nulls[Anum_pg_statistic_ext_data_stxddependencies - 1] = (data == NULL);
		values[Anum_pg_statistic_ext_data_stxddependencies - 1] = PointerGetDatum(data);
	}
	if (mcv != NULL)
	{
		bytea	   *data = statext_mcv_serialize(mcv, stats);

		nulls[Anum_pg_statistic_ext_data_stxdmcv - 1] = (data == NULL);
		values[Anum_pg_statistic_ext_data_stxdmcv - 1] = PointerGetDatum(data);
	}
	if (exprs != (Datum) 0)
	{
		nulls[Anum_pg_statistic_ext_data_stxdexpr - 1] = false;
		values[Anum_pg_statistic_ext_data_stxdexpr - 1] = exprs;
	}

	/* always replace the value (either by bytea or NULL) */
	replaces[Anum_pg_statistic_ext_data_stxdndistinct - 1] = true;
	replaces[Anum_pg_statistic_ext_data_stxddependencies - 1] = true;
	replaces[Anum_pg_statistic_ext_data_stxdmcv - 1] = true;
	replaces[Anum_pg_statistic_ext_data_stxdexpr - 1] = true;

	/* there should already be a pg_statistic_ext_data tuple */
	oldtup = SearchSysCache1(STATEXTDATASTXOID, ObjectIdGetDatum(statOid));
	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "cache lookup failed for statistics object %u", statOid);

	/* replace it */
	stup = heap_modify_tuple(oldtup,
							 RelationGetDescr(pg_stextdata),
							 values,
							 nulls,
							 replaces);
	ReleaseSysCache(oldtup);
	CatalogTupleUpdate(pg_stextdata, &stup->t_self, stup);

	heap_freetuple(stup);

	table_close(pg_stextdata, RowExclusiveLock);
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
 * Prepare sort support info using the given sort operator and collation
 * at the position 'sortdim'
 */
void
multi_sort_add_dimension(MultiSortSupport mss, int sortdim,
						 Oid oper, Oid collation)
{
	SortSupport ssup = &mss->ssup[sortdim];

	ssup->ssup_cxt = CurrentMemoryContext;
	ssup->ssup_collation = collation;
	ssup->ssup_nulls_first = false;

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

int
compare_datums_simple(Datum a, Datum b, SortSupport ssup)
{
	return ApplySortComparator(a, false, b, false, ssup);
}

/*
 * build_attnums_array
 *		Transforms a bitmap into an array of AttrNumber values.
 *
 * This is used for extended statistics only, so all the attribute must be
 * user-defined. That means offsetting by FirstLowInvalidHeapAttributeNumber
 * is not necessary here (and when querying the bitmap).
 */
AttrNumber *
build_attnums_array(Bitmapset *attrs, int nexprs, int *numattrs)
{
	int			i,
				j;
	AttrNumber *attnums;
	int			num = bms_num_members(attrs);

	if (numattrs)
		*numattrs = num;

	/* build attnums from the bitmapset */
	attnums = (AttrNumber *) palloc(sizeof(AttrNumber) * num);
	i = 0;
	j = -1;
	while ((j = bms_next_member(attrs, j)) >= 0)
	{
		int			attnum = (j - nexprs);

		/*
		 * Make sure the bitmap contains only user-defined attributes. As
		 * bitmaps can't contain negative values, this can be violated in two
		 * ways. Firstly, the bitmap might contain 0 as a member, and secondly
		 * the integer value might be larger than MaxAttrNumber.
		 */
		Assert(AttributeNumberIsValid(attnum));
		Assert(attnum <= MaxAttrNumber);
		Assert(attnum >= (-nexprs));

		attnums[i++] = (AttrNumber) attnum;

		/* protect against overflows */
		Assert(i <= num);
	}

	return attnums;
}

/*
 * build_sorted_items
 *		build a sorted array of SortItem with values from rows
 *
 * Note: All the memory is allocated in a single chunk, so that the caller
 * can simply pfree the return value to release all of it.
 */
SortItem *
build_sorted_items(StatsBuildData *data, int *nitems,
				   MultiSortSupport mss,
				   int numattrs, AttrNumber *attnums)
{
	int			i,
				j,
				len,
				nrows;
	int			nvalues = data->numrows * numattrs;

	SortItem   *items;
	Datum	   *values;
	bool	   *isnull;
	char	   *ptr;
	int		   *typlen;

	/* Compute the total amount of memory we need (both items and values). */
	len = data->numrows * sizeof(SortItem) + nvalues * (sizeof(Datum) + sizeof(bool));

	/* Allocate the memory and split it into the pieces. */
	ptr = palloc0(len);

	/* items to sort */
	items = (SortItem *) ptr;
	ptr += data->numrows * sizeof(SortItem);

	/* values and null flags */
	values = (Datum *) ptr;
	ptr += nvalues * sizeof(Datum);

	isnull = (bool *) ptr;
	ptr += nvalues * sizeof(bool);

	/* make sure we consumed the whole buffer exactly */
	Assert((ptr - (char *) items) == len);

	/* fix the pointers to Datum and bool arrays */
	nrows = 0;
	for (i = 0; i < data->numrows; i++)
	{
		items[nrows].values = &values[nrows * numattrs];
		items[nrows].isnull = &isnull[nrows * numattrs];

		nrows++;
	}

	/* build a local cache of typlen for all attributes */
	typlen = (int *) palloc(sizeof(int) * data->nattnums);
	for (i = 0; i < data->nattnums; i++)
		typlen[i] = get_typlen(data->stats[i]->attrtypid);

	nrows = 0;
	for (i = 0; i < data->numrows; i++)
	{
		bool		toowide = false;

		/* load the values/null flags from sample rows */
		for (j = 0; j < numattrs; j++)
		{
			Datum		value;
			bool		isnull;
			int			attlen;
			AttrNumber	attnum = attnums[j];

			int			idx;

			/* match attnum to the pre-calculated data */
			for (idx = 0; idx < data->nattnums; idx++)
			{
				if (attnum == data->attnums[idx])
					break;
			}

			Assert(idx < data->nattnums);

			value = data->values[idx][i];
			isnull = data->nulls[idx][i];
			attlen = typlen[idx];

			/*
			 * If this is a varlena value, check if it's too wide and if yes
			 * then skip the whole item. Otherwise detoast the value.
			 *
			 * XXX It may happen that we've already detoasted some preceding
			 * values for the current item. We don't bother to cleanup those
			 * on the assumption that those are small (below WIDTH_THRESHOLD)
			 * and will be discarded at the end of analyze.
			 */
			if ((!isnull) && (attlen == -1))
			{
				if (toast_raw_datum_size(value) > WIDTH_THRESHOLD)
				{
					toowide = true;
					break;
				}

				value = PointerGetDatum(PG_DETOAST_DATUM(value));
			}

			items[nrows].values[j] = value;
			items[nrows].isnull[j] = isnull;
		}

		if (toowide)
			continue;

		nrows++;
	}

	/* store the actual number of items (ignoring the too-wide ones) */
	*nitems = nrows;

	/* all items were too wide */
	if (nrows == 0)
	{
		/* everything is allocated as a single chunk */
		pfree(items);
		return NULL;
	}

	/* do the sort, using the multi-sort */
	qsort_arg((void *) items, nrows, sizeof(SortItem),
			  multi_sort_compare, mss);

	return items;
}

/*
 * has_stats_of_kind
 *		Check whether the list contains statistic of a given kind
 */
bool
has_stats_of_kind(List *stats, char requiredkind)
{
	ListCell   *l;

	foreach(l, stats)
	{
		StatisticExtInfo *stat = (StatisticExtInfo *) lfirst(l);

		if (stat->kind == requiredkind)
			return true;
	}

	return false;
}

/*
 * find_matching_sample
 *		Search for a sample statistics covering all the attributes.
 *
 * Both attnums and expressions in the join clause are required to be covered
 * by the statistics. Additional restrictions on base relations are considered
 * as extra conditions - but those are not required to be covered, we only use
 * them to pick "better" sample.
 *
 * So for example with a query
 *
 * t1 JOIN t2 ON (t1.a = t2.a AND t1.b = t2.b) AND t1.c = 10 AND t2.d < 100
 *
 * any statistics (on either side of the join) covering (a,b) will be eligible,
 * but those covering the columns in WHERE clauses will be seen as better.
 *
 * XXX The requirement that all the attributes need to be covered might be
 * too strong. Maybe we could relax it a bit, and search for stats (on both
 * sides of the join) with the largest overlap. But we don't really expect
 * many candidate samples, so this simple approach seems sufficient for now.
 */
StatisticExtInfo *
find_matching_sample(PlannerInfo *root, RelOptInfo *rel,
					 Bitmapset *attnums, List *exprs)
{
	ListCell   *l;
	StatisticExtInfo *sample = NULL;
	List *stats = rel->statlist;

	foreach(l, stats)
	{
		StatisticExtInfo *stat = (StatisticExtInfo *) lfirst(l);
		List *conditions1 = NIL,
			 *conditions2 = NIL;

		/* We only care about samples statistics here. */
		if (stat->kind != STATS_EXT_SAMPLE)
			continue;

		/*
		 * Ignore stats not covering all the attributes/expressions.
		 *
		 * XXX Maybe we shouldn't be so strict and consider only partial
		 * matches for join clauses too?
		 */
		if (!bms_is_subset(attnums, stat->keys) ||
			!stat_covers_expressions(stat, exprs, NULL))
			continue;

		/* If there's no matching sample yet, keep it. */
		if (!sample)
		{
			sample = stat;
			continue;
		}

		/*
		 * OK, we have two candidate statistics and we need to pick. We'll
		 * use two simple heuristics: We prefer smaller statistics (fewer
		 * columns), on the assumption that a smaller statistics probably
		 * represents a larger fraction of the data (fewer combinations
		 * with higher counts). But we also like if the statistics covers
		 * some additional conditions at the baserel level, because this
		 * may affect the data distribition. Of course, those two metrics
		 * are contradictory - smaller stats are less likely to cover as
		 * many conditions as a larger one.
		 *
		 * XXX For now we simply prefer smaller statistics, but maybe it
		 * should be the other way around.
		 */
		if (bms_num_members(sample->keys) + list_length(sample->exprs) >
			bms_num_members(stat->keys) + list_length(stat->exprs))
		{
			sample = stat;
			continue;
		}

		/*
		 * Now inspect the base restrictinfo conditions too. We need to be
		 * more careful because we didn't check which of those clauses are
		 * compatible, so we need to run statext_is_compatible_clause.
		 *
		 * XXX This should be moved before the previous check, probably. This
		 * way a "smaller" statistics will be preferred, no matter if that
		 * means some conditions will be unusable.
		 */
		conditions1 = statext_sample_get_conditions(root, rel, stat);
		conditions2 = statext_sample_get_conditions(root, rel, sample);

		/* if the new statistics covers more conditions, use it */
		if (list_length(conditions2) > list_length(conditions1))
			sample = stat;
	}

	return sample;
}

/*
 * stat_find_expression
 *		Search for an expression in statistics object's list of expressions.
 *
 * Returns the index of the expression in the statistics object's list of
 * expressions, or -1 if not found.
 */
static int
stat_find_expression(StatisticExtInfo *stat, Node *expr)
{
	ListCell   *lc;
	int			idx;

	idx = 0;
	foreach(lc, stat->exprs)
	{
		Node	   *stat_expr = (Node *) lfirst(lc);

		if (equal(stat_expr, expr))
			return idx;
		idx++;
	}

	/* Expression not found */
	return -1;
}

/*
 * stat_covers_expressions
 * 		Test whether a statistics object covers all expressions in a list.
 *
 * Returns true if all expressions are covered.  If expr_idxs is non-NULL, it
 * is populated with the indexes of the expressions found.
 */
static bool
stat_covers_expressions(StatisticExtInfo *stat, List *exprs,
						Bitmapset **expr_idxs)
{
	ListCell   *lc;

	foreach(lc, exprs)
	{
		Node	   *expr = (Node *) lfirst(lc);
		int			expr_idx;

		expr_idx = stat_find_expression(stat, expr);
		if (expr_idx == -1)
			return false;

		if (expr_idxs != NULL)
			*expr_idxs = bms_add_member(*expr_idxs, expr_idx);
	}

	/* If we reach here, all expressions are covered */
	return true;
}

/*
 * choose_best_statistics
 *		Look for and return statistics with the specified 'requiredkind' which
 *		have keys that match at least two of the given attnums.  Return NULL if
 *		there's no match.
 *
 * The current selection criteria is very simple - we choose the statistics
 * object referencing the most attributes in covered (and still unestimated
 * clauses), breaking ties in favor of objects with fewer keys overall.
 *
 * The clause_attnums is an array of bitmaps, storing attnums for individual
 * clauses. A NULL element means the clause is either incompatible or already
 * estimated.
 *
 * XXX If multiple statistics objects tie on both criteria, then which object
 * is chosen depends on the order that they appear in the stats list. Perhaps
 * further tiebreakers are needed.
 */
StatisticExtInfo *
choose_best_statistics(List *stats, char requiredkind,
					   Bitmapset **clause_attnums, List **clause_exprs,
					   int nclauses)
{
	ListCell   *lc;
	StatisticExtInfo *best_match = NULL;
	int			best_num_matched = 2;	/* goal #1: maximize */
	int			best_match_keys = (STATS_MAX_DIMENSIONS + 1);	/* goal #2: minimize */

	foreach(lc, stats)
	{
		int			i;
		StatisticExtInfo *info = (StatisticExtInfo *) lfirst(lc);
		Bitmapset  *matched_attnums = NULL;
		Bitmapset  *matched_exprs = NULL;
		int			num_matched;
		int			numkeys;

		/* skip statistics that are not of the correct type */
		if (info->kind != requiredkind)
			continue;

		/*
		 * Collect attributes and expressions in remaining (unestimated)
		 * clauses fully covered by this statistic object.
		 *
		 * We know already estimated clauses have both clause_attnums and
		 * clause_exprs set to NULL. We leave the pointers NULL if already
		 * estimated, or we reset them to NULL after estimating the clause.
		 */
		for (i = 0; i < nclauses; i++)
		{
			Bitmapset  *expr_idxs = NULL;

			/* ignore incompatible/estimated clauses */
			if (!clause_attnums[i] && !clause_exprs[i])
				continue;

			/* ignore clauses that are not covered by this object */
			if (!bms_is_subset(clause_attnums[i], info->keys) ||
				!stat_covers_expressions(info, clause_exprs[i], &expr_idxs))
				continue;

			/* record attnums and indexes of expressions covered */
			matched_attnums = bms_add_members(matched_attnums, clause_attnums[i]);
			matched_exprs = bms_add_members(matched_exprs, expr_idxs);
		}

		num_matched = bms_num_members(matched_attnums) + bms_num_members(matched_exprs);

		bms_free(matched_attnums);
		bms_free(matched_exprs);

		/*
		 * save the actual number of keys in the stats so that we can choose
		 * the narrowest stats with the most matching keys.
		 */
		numkeys = bms_num_members(info->keys) + list_length(info->exprs);

		/*
		 * Use this object when it increases the number of matched attributes
		 * and expressions or when it matches the same number of attributes
		 * and expressions but these stats have fewer keys than any previous
		 * match.
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

/*
 * statext_is_compatible_clause_internal
 *		Determines if the clause is compatible with MCV lists.
 *
 * Does the heavy lifting of actually inspecting the clauses for
 * statext_is_compatible_clause. It needs to be split like this because
 * of recursion.  The attnums bitmap is an input/output parameter collecting
 * attribute numbers from all compatible clauses (recursively).
 */
static bool
statext_is_compatible_clause_internal(PlannerInfo *root, Node *clause,
									  Index relid, Bitmapset **attnums,
									  List **exprs)
{
	/* Look inside any binary-compatible relabeling (as in examine_variable) */
	if (IsA(clause, RelabelType))
		clause = (Node *) ((RelabelType *) clause)->arg;

	/* plain Var references (boolean Vars or recursive checks) */
	if (IsA(clause, Var))
	{
		Var		   *var = (Var *) clause;

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

	/* (Var/Expr op Const) or (Const op Var/Expr) */
	if (is_opclause(clause))
	{
		RangeTblEntry *rte = root->simple_rte_array[relid];
		OpExpr	   *expr = (OpExpr *) clause;
		Node	   *clause_expr;

		/* Only expressions with two arguments are considered compatible. */
		if (list_length(expr->args) != 2)
			return false;

		/* Check if the expression has the right shape */
		if (!examine_opclause_args(expr->args, &clause_expr, NULL, NULL))
			return false;

		/*
		 * If it's not one of the supported operators ("=", "<", ">", etc.),
		 * just ignore the clause, as it's not compatible with MCV lists.
		 *
		 * This uses the function for estimating selectivity, not the operator
		 * directly (a bit awkward, but well ...).
		 */
		switch (get_oprrest(expr->opno))
		{
			case F_EQSEL:
			case F_NEQSEL:
			case F_SCALARLTSEL:
			case F_SCALARLESEL:
			case F_SCALARGTSEL:
			case F_SCALARGESEL:
				/* supported, will continue with inspection of the Var/Expr */
				break;

			default:
				/* other estimators are considered unknown/unsupported */
				return false;
		}

		/*
		 * If there are any securityQuals on the RTE from security barrier
		 * views or RLS policies, then the user may not have access to all the
		 * table's data, and we must check that the operator is leak-proof.
		 *
		 * If the operator is leaky, then we must ignore this clause for the
		 * purposes of estimating with MCV lists, otherwise the operator might
		 * reveal values from the MCV list that the user doesn't have
		 * permission to see.
		 */
		if (rte->securityQuals != NIL &&
			!get_func_leakproof(get_opcode(expr->opno)))
			return false;

		/* Check (Var op Const) or (Const op Var) clauses by recursing. */
		if (IsA(clause_expr, Var))
			return statext_is_compatible_clause_internal(root, clause_expr,
														 relid, attnums, exprs);

		/* Otherwise we have (Expr op Const) or (Const op Expr). */
		*exprs = lappend(*exprs, clause_expr);
		return true;
	}

	/* Var/Expr IN Array */
	if (IsA(clause, ScalarArrayOpExpr))
	{
		RangeTblEntry *rte = root->simple_rte_array[relid];
		ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) clause;
		Node	   *clause_expr;

		/* Only expressions with two arguments are considered compatible. */
		if (list_length(expr->args) != 2)
			return false;

		/* Check if the expression has the right shape (one Var, one Const) */
		if (!examine_opclause_args(expr->args, &clause_expr, NULL, NULL))
			return false;

		/*
		 * If it's not one of the supported operators ("=", "<", ">", etc.),
		 * just ignore the clause, as it's not compatible with MCV lists.
		 *
		 * This uses the function for estimating selectivity, not the operator
		 * directly (a bit awkward, but well ...).
		 */
		switch (get_oprrest(expr->opno))
		{
			case F_EQSEL:
			case F_NEQSEL:
			case F_SCALARLTSEL:
			case F_SCALARLESEL:
			case F_SCALARGTSEL:
			case F_SCALARGESEL:
				/* supported, will continue with inspection of the Var/Expr */
				break;

			default:
				/* other estimators are considered unknown/unsupported */
				return false;
		}

		/*
		 * If there are any securityQuals on the RTE from security barrier
		 * views or RLS policies, then the user may not have access to all the
		 * table's data, and we must check that the operator is leak-proof.
		 *
		 * If the operator is leaky, then we must ignore this clause for the
		 * purposes of estimating with MCV lists, otherwise the operator might
		 * reveal values from the MCV list that the user doesn't have
		 * permission to see.
		 */
		if (rte->securityQuals != NIL &&
			!get_func_leakproof(get_opcode(expr->opno)))
			return false;

		/* Check Var IN Array clauses by recursing. */
		if (IsA(clause_expr, Var))
			return statext_is_compatible_clause_internal(root, clause_expr,
														 relid, attnums, exprs);

		/* Otherwise we have Expr IN Array. */
		*exprs = lappend(*exprs, clause_expr);
		return true;
	}

	/* AND/OR/NOT clause */
	if (is_andclause(clause) ||
		is_orclause(clause) ||
		is_notclause(clause))
	{
		/*
		 * AND/OR/NOT-clauses are supported if all sub-clauses are supported
		 *
		 * Perhaps we could improve this by handling mixed cases, when some of
		 * the clauses are supported and some are not. Selectivity for the
		 * supported subclauses would be computed using extended statistics,
		 * and the remaining clauses would be estimated using the traditional
		 * algorithm (product of selectivities).
		 *
		 * It however seems overly complex, and in a way we already do that
		 * because if we reject the whole clause as unsupported here, it will
		 * be eventually passed to clauselist_selectivity() which does exactly
		 * this (split into supported/unsupported clauses etc).
		 */
		BoolExpr   *expr = (BoolExpr *) clause;
		ListCell   *lc;

		foreach(lc, expr->args)
		{
			/*
			 * Had we found incompatible clause in the arguments, treat the
			 * whole clause as incompatible.
			 */
			if (!statext_is_compatible_clause_internal(root,
													   (Node *) lfirst(lc),
													   relid, attnums, exprs))
				return false;
		}

		return true;
	}

	/* Var/Expr IS NULL */
	if (IsA(clause, NullTest))
	{
		NullTest   *nt = (NullTest *) clause;

		/* Check Var IS NULL clauses by recursing. */
		if (IsA(nt->arg, Var))
			return statext_is_compatible_clause_internal(root, (Node *) (nt->arg),
														 relid, attnums, exprs);

		/* Otherwise we have Expr IS NULL. */
		*exprs = lappend(*exprs, nt->arg);
		return true;
	}

	/*
	 * Treat any other expressions as bare expressions to be matched against
	 * expressions in statistics objects.
	 */
	*exprs = lappend(*exprs, clause);
	return true;
}

/*
 * statext_is_compatible_clause
 *		Determines if the clause is compatible with MCV lists.
 *
 * Currently, we only support the following types of clauses:
 *
 * (a) OpExprs of the form (Var/Expr op Const), or (Const op Var/Expr), where
 * the op is one of ("=", "<", ">", ">=", "<=")
 *
 * (b) (Var/Expr IS [NOT] NULL)
 *
 * (c) combinations using AND/OR/NOT
 *
 * (d) ScalarArrayOpExprs of the form (Var/Expr op ANY (array)) or (Var/Expr
 * op ALL (array))
 *
 * In the future, the range of supported clauses may be expanded to more
 * complex cases, for example (Var op Var).
 */
static bool
statext_is_compatible_clause(PlannerInfo *root, Node *clause, Index relid,
							 Bitmapset **attnums, List **exprs)
{
	RangeTblEntry *rte = root->simple_rte_array[relid];
	RestrictInfo *rinfo = (RestrictInfo *) clause;
	int			clause_relid;
	Oid			userid;

	/*
	 * Special-case handling for bare BoolExpr AND clauses, because the
	 * restrictinfo machinery doesn't build RestrictInfos on top of AND
	 * clauses.
	 */
	if (is_andclause(clause))
	{
		BoolExpr   *expr = (BoolExpr *) clause;
		ListCell   *lc;

		/*
		 * Check that each sub-clause is compatible.  We expect these to be
		 * RestrictInfos.
		 */
		foreach(lc, expr->args)
		{
			if (!statext_is_compatible_clause(root, (Node *) lfirst(lc),
											  relid, attnums, exprs))
				return false;
		}

		return true;
	}

	/* Otherwise it must be a RestrictInfo. */
	if (!IsA(rinfo, RestrictInfo))
		return false;

	/* Pseudoconstants are not really interesting here. */
	if (rinfo->pseudoconstant)
		return false;

	/* Clauses referencing other varnos are incompatible. */
	if (!bms_get_singleton_member(rinfo->clause_relids, &clause_relid) ||
		clause_relid != relid)
		return false;

	/* Check the clause and determine what attributes it references. */
	if (!statext_is_compatible_clause_internal(root, (Node *) rinfo->clause,
											   relid, attnums, exprs))
		return false;

	/*
	 * Check that the user has permission to read all required attributes. Use
	 * checkAsUser if it's set, in case we're accessing the table via a view.
	 */
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	if (pg_class_aclcheck(rte->relid, userid, ACL_SELECT) != ACLCHECK_OK)
	{
		Bitmapset  *clause_attnums = NULL;

		/* Don't have table privilege, must check individual columns */
		if (*exprs != NIL)
		{
			pull_varattnos((Node *) exprs, relid, &clause_attnums);
			clause_attnums = bms_add_members(clause_attnums, *attnums);
		}
		else
			clause_attnums = *attnums;

		if (bms_is_member(InvalidAttrNumber, clause_attnums))
		{
			/* Have a whole-row reference, must have access to all columns */
			if (pg_attribute_aclcheck_all(rte->relid, userid, ACL_SELECT,
										  ACLMASK_ALL) != ACLCHECK_OK)
				return false;
		}
		else
		{
			/* Check the columns referenced by the clause */
			int			attnum = -1;

			while ((attnum = bms_next_member(clause_attnums, attnum)) >= 0)
			{
				if (pg_attribute_aclcheck(rte->relid, attnum, userid,
										  ACL_SELECT) != ACLCHECK_OK)
					return false;
			}
		}
	}

	/* If we reach here, the clause is OK */
	return true;
}

/*
 * statext_sample_clauselist_selectivity
 *		Estimate clauses using the best multi-column statistics sample.
 *
 * Applies available extended (multi-column) statistics on a table. There may
 * be multiple applicable statistics (with respect to the clauses), in which
 * case we use greedy approach. In each round we select the best statistic on
 * a table (measured by the number of attributes extracted from the clauses
 * and covered by it), and compute the selectivity for the supplied clauses.
 * We repeat this process with the remaining clauses (if any), until none of
 * the available statistics can be used.
 *
 * This is similar to statext_mcv_clauselist_selectivity, but it only considers
 * statistics with run-time samples, not MCV lists. We try to apply it before
 * MCV lists, and the remaining clauses are estimated by MCVs.
 *
 * 'estimatedclauses' is an input/output parameter.  We set bits for the
 * 0-based 'clauses' indexes we estimate for and also skip clause items that
 * already have a bit set.
 *
 * XXX In principle, samples might cover much wider range of clauses - almost
 * any condition can be evaluated on the sample (as long as all the input vars
 * are in the sample), and then we can use that. For the MCV this would not
 * work well as the function might move values from the non-MCV part to the
 * MCV, but that's impossible to calculate.  But for now we ignore this and
 * just use statext_is_compatible_clause, we can relax this later.
 */
static Selectivity
statext_sample_clauselist_selectivity(PlannerInfo *root, List *clauses, int varRelid,
									  JoinType jointype, SpecialJoinInfo *sjinfo,
									  RelOptInfo *rel, Bitmapset **estimatedclauses,
									  bool is_or)
{
	ListCell   *l;
	Bitmapset **list_attnums;	/* attnums extracted from the clause */
	List	  **list_exprs;		/* expressions matched to any statistic */
	int			listidx;
	Selectivity sel = (is_or) ? 0.0 : 1.0;
	Sample	   *sample;

	/* check if there's any stats that might be useful for us. */
	if (!has_stats_of_kind(rel->statlist, STATS_EXT_SAMPLE) ||
		!enable_sample_estimates_scan)
		return sel;

	list_attnums = (Bitmapset **) palloc(sizeof(Bitmapset *) *
										 list_length(clauses));

	/* expressions extracted from complex expressions */
	list_exprs = (List **) palloc(sizeof(Node *) * list_length(clauses));

	/*
	 * Pre-process the clauses list to extract the attnums and expressions
	 * seen in each item.  We need to determine if there are any clauses which
	 * will be useful for selectivity estimations with extended stats.  Along
	 * the way we'll record all of the attnums and expressions for each clause
	 * in lists which we'll reference later so we don't need to repeat the
	 * same work again.
	 *
	 * We also skip clauses that we already estimated using different types of
	 * statistics (we treat them as incompatible).
	 */
	listidx = 0;
	foreach(l, clauses)
	{
		Node	   *clause = (Node *) lfirst(l);
		Bitmapset  *attnums = NULL;
		List	   *exprs = NIL;

		if (!bms_is_member(listidx, *estimatedclauses) &&
			statext_is_compatible_clause(root, clause, rel->relid, &attnums, &exprs))
		{
			list_attnums[listidx] = attnums;
			list_exprs[listidx] = exprs;
		}
		else
		{
			list_attnums[listidx] = NULL;
			list_exprs[listidx] = NIL;
		}

		listidx++;
	}

	/* apply as many extended statistics as possible */
	while (true)
	{
		StatisticExtInfo *stat;
		List	   *stat_clauses;
		Bitmapset  *simple_clauses;

		/* find the best suited statistics object for these attnums */
		stat = choose_best_statistics(rel->statlist, STATS_EXT_SAMPLE,
									  list_attnums, list_exprs,
									  list_length(clauses));

		/*
		 * if no (additional) matching stats could be found then we've nothing
		 * to do
		 */
		if (!stat)
			break;

		/*
		 * XXX should be done later, after determining which attnums and exprs
		 * need to be sampled.
		 */
		sample = statext_collect_sample(root, stat);

		/* Ensure choose_best_statistics produced an expected stats type. */
		Assert(stat->kind == STATS_EXT_SAMPLE);

		/* now filter the clauses to be estimated using the selected stat */
		stat_clauses = NIL;

		/* record which clauses are simple (single column or expression) */
		simple_clauses = NULL;

		listidx = -1;
		foreach(l, clauses)
		{
			/* Increment the index before we decide if to skip the clause. */
			listidx++;

			/*
			 * Ignore clauses from which we did not extract any attnums or
			 * expressions (this needs to be consistent with what we do in
			 * choose_best_statistics).
			 *
			 * This also eliminates already estimated clauses - both those
			 * estimated before and during applying extended statistics.
			 *
			 * XXX This check is needed because both bms_is_subset and
			 * stat_covers_expressions return true for empty attnums and
			 * expressions.
			 */
			if (!list_attnums[listidx] && !list_exprs[listidx])
				continue;

			/*
			 * The clause was not estimated yet, and we've extracted either
			 * attnums of expressions from it. Ignore it if it's not fully
			 * covered by the chosen statistics.
			 *
			 * We need to check both attributes and expressions, and reject if
			 * either is not covered.
			 */
			if (!bms_is_subset(list_attnums[listidx], stat->keys) ||
				!stat_covers_expressions(stat, list_exprs[listidx], NULL))
				continue;

			/*
			 * Now we know the clause is compatible (we have either attnums or
			 * expressions extracted from it), and was not estimated yet.
			 */

			/* record simple clauses (single column or expression) */
			if ((list_attnums[listidx] == NULL &&
				 list_length(list_exprs[listidx]) == 1) ||
				(list_exprs[listidx] == NIL &&
				 bms_membership(list_attnums[listidx]) == BMS_SINGLETON))
				simple_clauses = bms_add_member(simple_clauses,
												list_length(stat_clauses));

			/* add clause to list and mark it as estimated */
			stat_clauses = lappend(stat_clauses, (Node *) lfirst(l));
			*estimatedclauses = bms_add_member(*estimatedclauses, listidx);

			/*
			 * Reset the pointers, so that choose_best_statistics knows this
			 * clause was estimated and does not consider it again.
			 */
			bms_free(list_attnums[listidx]);
			list_attnums[listidx] = NULL;

			list_free(list_exprs[listidx]);
			list_exprs[listidx] = NULL;
		}

		if (is_or)
		{
			Selectivity	stat_sel;

			stat_sel = sample_clauselist_selectivity(root, stat, sample, stat_clauses,
													 varRelid, jointype, sjinfo,
													 rel, true);

			/*
			 * Factor the result for this statistics object into the overall
			 * result.  We treat the results from each separate statistics
			 * object as independent of one another.
			 */
			sel = sel + stat_sel - sel * stat_sel;
		}
		else					/* Implicitly-ANDed list of clauses */
		{
			/*
			 * Multi-column estimate using the sample. Just facto it right
			 * into the result.
			 */
			sel *= sample_clauselist_selectivity(root, stat, sample, stat_clauses,
												 varRelid, jointype, sjinfo,
												 rel, false);
		}
	}

	return sel;
}

/*
 * statext_mcv_clauselist_selectivity
 *		Estimate clauses using the best multi-column statistics.
 *
 * Applies available extended (multi-column) statistics on a table. There may
 * be multiple applicable statistics (with respect to the clauses), in which
 * case we use greedy approach. In each round we select the best statistic on
 * a table (measured by the number of attributes extracted from the clauses
 * and covered by it), and compute the selectivity for the supplied clauses.
 * We repeat this process with the remaining clauses (if any), until none of
 * the available statistics can be used.
 *
 * One of the main challenges with using MCV lists is how to extrapolate the
 * estimate to the data not covered by the MCV list. To do that, we compute
 * not only the "MCV selectivity" (selectivities for MCV items matching the
 * supplied clauses), but also the following related selectivities:
 *
 * - simple selectivity:  Computed without extended statistics, i.e. as if the
 * columns/clauses were independent.
 *
 * - base selectivity:  Similar to simple selectivity, but is computed using
 * the extended statistic by adding up the base frequencies (that we compute
 * and store for each MCV item) of matching MCV items.
 *
 * - total selectivity: Selectivity covered by the whole MCV list.
 *
 * These are passed to mcv_combine_selectivities() which combines them to
 * produce a selectivity estimate that makes use of both per-column statistics
 * and the multi-column MCV statistics.
 *
 * 'estimatedclauses' is an input/output parameter.  We set bits for the
 * 0-based 'clauses' indexes we estimate for and also skip clause items that
 * already have a bit set.
 */
static Selectivity
statext_mcv_clauselist_selectivity(PlannerInfo *root, List *clauses, int varRelid,
								   JoinType jointype, SpecialJoinInfo *sjinfo,
								   RelOptInfo *rel, Bitmapset **estimatedclauses,
								   bool is_or)
{
	ListCell   *l;
	Bitmapset **list_attnums;	/* attnums extracted from the clause */
	List	  **list_exprs;		/* expressions matched to any statistic */
	int			listidx;
	Selectivity sel = (is_or) ? 0.0 : 1.0;

	/* check if there's any stats that might be useful for us. */
	if (!has_stats_of_kind(rel->statlist, STATS_EXT_MCV))
		return sel;

	list_attnums = (Bitmapset **) palloc(sizeof(Bitmapset *) *
										 list_length(clauses));

	/* expressions extracted from complex expressions */
	list_exprs = (List **) palloc(sizeof(Node *) * list_length(clauses));

	/*
	 * Pre-process the clauses list to extract the attnums and expressions
	 * seen in each item.  We need to determine if there are any clauses which
	 * will be useful for selectivity estimations with extended stats.  Along
	 * the way we'll record all of the attnums and expressions for each clause
	 * in lists which we'll reference later so we don't need to repeat the
	 * same work again.
	 *
	 * We also skip clauses that we already estimated using different types of
	 * statistics (we treat them as incompatible).
	 */
	listidx = 0;
	foreach(l, clauses)
	{
		Node	   *clause = (Node *) lfirst(l);
		Bitmapset  *attnums = NULL;
		List	   *exprs = NIL;

		if (!bms_is_member(listidx, *estimatedclauses) &&
			statext_is_compatible_clause(root, clause, rel->relid, &attnums, &exprs))
		{
			list_attnums[listidx] = attnums;
			list_exprs[listidx] = exprs;
		}
		else
		{
			list_attnums[listidx] = NULL;
			list_exprs[listidx] = NIL;
		}

		listidx++;
	}

	/* apply as many extended statistics as possible */
	while (true)
	{
		StatisticExtInfo *stat;
		List	   *stat_clauses;
		Bitmapset  *simple_clauses;

		/* find the best suited statistics object for these attnums */
		stat = choose_best_statistics(rel->statlist, STATS_EXT_MCV,
									  list_attnums, list_exprs,
									  list_length(clauses));

		/*
		 * if no (additional) matching stats could be found then we've nothing
		 * to do
		 */
		if (!stat)
			break;

		/* Ensure choose_best_statistics produced an expected stats type. */
		Assert(stat->kind == STATS_EXT_MCV);

		/* now filter the clauses to be estimated using the selected MCV */
		stat_clauses = NIL;

		/* record which clauses are simple (single column or expression) */
		simple_clauses = NULL;

		listidx = -1;
		foreach(l, clauses)
		{
			/* Increment the index before we decide if to skip the clause. */
			listidx++;

			/*
			 * Ignore clauses from which we did not extract any attnums or
			 * expressions (this needs to be consistent with what we do in
			 * choose_best_statistics).
			 *
			 * This also eliminates already estimated clauses - both those
			 * estimated before and during applying extended statistics.
			 *
			 * XXX This check is needed because both bms_is_subset and
			 * stat_covers_expressions return true for empty attnums and
			 * expressions.
			 */
			if (!list_attnums[listidx] && !list_exprs[listidx])
				continue;

			/*
			 * The clause was not estimated yet, and we've extracted either
			 * attnums or expressions from it. Ignore it if it's not fully
			 * covered by the chosen statistics object.
			 *
			 * We need to check both attributes and expressions, and reject if
			 * either is not covered.
			 */
			if (!bms_is_subset(list_attnums[listidx], stat->keys) ||
				!stat_covers_expressions(stat, list_exprs[listidx], NULL))
				continue;

			/*
			 * Now we know the clause is compatible (we have either attnums or
			 * expressions extracted from it), and was not estimated yet.
			 */

			/* record simple clauses (single column or expression) */
			if ((list_attnums[listidx] == NULL &&
				 list_length(list_exprs[listidx]) == 1) ||
				(list_exprs[listidx] == NIL &&
				 bms_membership(list_attnums[listidx]) == BMS_SINGLETON))
				simple_clauses = bms_add_member(simple_clauses,
												list_length(stat_clauses));

			/* add clause to list and mark it as estimated */
			stat_clauses = lappend(stat_clauses, (Node *) lfirst(l));
			*estimatedclauses = bms_add_member(*estimatedclauses, listidx);

			/*
			 * Reset the pointers, so that choose_best_statistics knows this
			 * clause was estimated and does not consider it again.
			 */
			bms_free(list_attnums[listidx]);
			list_attnums[listidx] = NULL;

			list_free(list_exprs[listidx]);
			list_exprs[listidx] = NULL;
		}

		if (is_or)
		{
			bool	   *or_matches = NULL;
			Selectivity simple_or_sel = 0.0,
						stat_sel = 0.0;
			MCVList    *mcv_list;

			/* Load the MCV list stored in the statistics object */
			mcv_list = statext_mcv_load(stat->statOid);

			/*
			 * Compute the selectivity of the ORed list of clauses covered by
			 * this statistics object by estimating each in turn and combining
			 * them using the formula P(A OR B) = P(A) + P(B) - P(A AND B).
			 * This allows us to use the multivariate MCV stats to better
			 * estimate the individual terms and their overlap.
			 *
			 * Each time we iterate this formula, the clause "A" above is
			 * equal to all the clauses processed so far, combined with "OR".
			 */
			listidx = 0;
			foreach(l, stat_clauses)
			{
				Node	   *clause = (Node *) lfirst(l);
				Selectivity simple_sel,
							overlap_simple_sel,
							mcv_sel,
							mcv_basesel,
							overlap_mcvsel,
							overlap_basesel,
							mcv_totalsel,
							clause_sel,
							overlap_sel;

				/*
				 * "Simple" selectivity of the next clause and its overlap
				 * with any of the previous clauses.  These are our initial
				 * estimates of P(B) and P(A AND B), assuming independence of
				 * columns/clauses.
				 */
				simple_sel = clause_selectivity_ext(root, clause, varRelid,
													jointype, sjinfo, false);

				overlap_simple_sel = simple_or_sel * simple_sel;

				/*
				 * New "simple" selectivity of all clauses seen so far,
				 * assuming independence.
				 */
				simple_or_sel += simple_sel - overlap_simple_sel;
				CLAMP_PROBABILITY(simple_or_sel);

				/*
				 * Multi-column estimate of this clause using MCV statistics,
				 * along with base and total selectivities, and corresponding
				 * selectivities for the overlap term P(A AND B).
				 */
				mcv_sel = mcv_clause_selectivity_or(root, stat, mcv_list,
													clause, &or_matches,
													&mcv_basesel,
													&overlap_mcvsel,
													&overlap_basesel,
													&mcv_totalsel);

				/*
				 * Combine the simple and multi-column estimates.
				 *
				 * If this clause is a simple single-column clause, then we
				 * just use the simple selectivity estimate for it, since the
				 * multi-column statistics are unlikely to improve on that
				 * (and in fact could make it worse).  For the overlap, we
				 * always make use of the multi-column statistics.
				 */
				if (bms_is_member(listidx, simple_clauses))
					clause_sel = simple_sel;
				else
					clause_sel = mcv_combine_selectivities(simple_sel,
														   mcv_sel,
														   mcv_basesel,
														   mcv_totalsel);

				overlap_sel = mcv_combine_selectivities(overlap_simple_sel,
														overlap_mcvsel,
														overlap_basesel,
														mcv_totalsel);

				/* Factor these into the result for this statistics object */
				stat_sel += clause_sel - overlap_sel;
				CLAMP_PROBABILITY(stat_sel);

				listidx++;
			}

			/*
			 * Factor the result for this statistics object into the overall
			 * result.  We treat the results from each separate statistics
			 * object as independent of one another.
			 */
			sel = sel + stat_sel - sel * stat_sel;
		}
		else					/* Implicitly-ANDed list of clauses */
		{
			Selectivity simple_sel,
						mcv_sel,
						mcv_basesel,
						mcv_totalsel,
						stat_sel;

			/*
			 * "Simple" selectivity, i.e. without any extended statistics,
			 * essentially assuming independence of the columns/clauses.
			 */
			simple_sel = clauselist_selectivity_ext(root, stat_clauses,
													varRelid, jointype,
													sjinfo, false);

			/*
			 * Multi-column estimate using MCV statistics, along with base and
			 * total selectivities.
			 */
			mcv_sel = mcv_clauselist_selectivity(root, stat, stat_clauses,
												 varRelid, jointype, sjinfo,
												 rel, &mcv_basesel,
												 &mcv_totalsel);

			/* Combine the simple and multi-column estimates. */
			stat_sel = mcv_combine_selectivities(simple_sel,
												 mcv_sel,
												 mcv_basesel,
												 mcv_totalsel);

			/* Factor this into the overall result */
			sel *= stat_sel;
		}
	}

	return sel;
}

/*
 * statext_clauselist_selectivity
 *		Estimate clauses using the best multi-column statistics.
 */
Selectivity
statext_clauselist_selectivity(PlannerInfo *root, List *clauses, int varRelid,
							   JoinType jointype, SpecialJoinInfo *sjinfo,
							   RelOptInfo *rel, Bitmapset **estimatedclauses,
							   bool is_or)
{
	Selectivity sel;
	Selectivity sel2;

	/* First, try estimating clauses using a multivariate sample. */
	sel = statext_sample_clauselist_selectivity(root, clauses, varRelid, jointype,
												sjinfo, rel, estimatedclauses, is_or);

	/* Then try estimating the remaining clauses using a multivariate MCV list. */
	sel2 = statext_mcv_clauselist_selectivity(root, clauses, varRelid, jointype,
											 sjinfo, rel, estimatedclauses, is_or);

	/*
	 * Functional dependencies only work for clauses connected by AND, so for
	 * OR clauses we're done.
	 *
	 * If it's OR, combine them using the usual (s1 + s2 - s1 * s2).
	 */
	if (is_or)
		return (sel + sel2 - sel * sel2);

	/*
	 * Otherwise continue with functional dependencies, but first combine the results
	 * using the usual product formula (assuming independence).
	 */
	sel *= sel2;

	/*
	 * Then, apply functional dependencies on the remaining clauses by calling
	 * dependencies_clauselist_selectivity.  Pass 'estimatedclauses' so the
	 * function can properly skip clauses already estimated above.
	 *
	 * The reasoning for applying dependencies last is that the more complex
	 * stats can track more complex correlations between the attributes, and
	 * so may be considered more reliable.
	 *
	 * For example, MCV list can give us an exact selectivity for values in
	 * two columns, while functional dependencies can only provide information
	 * about the overall strength of the dependency.
	 */
	sel *= dependencies_clauselist_selectivity(root, clauses, varRelid,
											   jointype, sjinfo, rel,
											   estimatedclauses);

	return sel;
}

/*
 * examine_opclause_args
 *		Split an operator expression's arguments into Expr and Const parts.
 *
 * Attempts to match the arguments to either (Expr op Const) or (Const op
 * Expr), possibly with a RelabelType on top. When the expression matches this
 * form, returns true, otherwise returns false.
 *
 * Optionally returns pointers to the extracted Expr/Const nodes, when passed
 * non-null pointers (exprp, cstp and expronleftp). The expronleftp flag
 * specifies on which side of the operator we found the expression node.
 */
bool
examine_opclause_args(List *args, Node **exprp, Const **cstp,
					  bool *expronleftp)
{
	Node	   *expr;
	Const	   *cst;
	bool		expronleft;
	Node	   *leftop,
			   *rightop;

	/* enforced by statext_is_compatible_clause_internal */
	Assert(list_length(args) == 2);

	leftop = linitial(args);
	rightop = lsecond(args);

	/* strip RelabelType from either side of the expression */
	if (IsA(leftop, RelabelType))
		leftop = (Node *) ((RelabelType *) leftop)->arg;

	if (IsA(rightop, RelabelType))
		rightop = (Node *) ((RelabelType *) rightop)->arg;

	if (IsA(rightop, Const))
	{
		expr = (Node *) leftop;
		cst = (Const *) rightop;
		expronleft = true;
	}
	else if (IsA(leftop, Const))
	{
		expr = (Node *) rightop;
		cst = (Const *) leftop;
		expronleft = false;
	}
	else
		return false;

	/* return pointers to the extracted parts if requested */
	if (exprp)
		*exprp = expr;

	if (cstp)
		*cstp = cst;

	if (expronleftp)
		*expronleftp = expronleft;

	return true;
}


/*
 * Compute statistics about expressions of a relation.
 */
static void
compute_expr_stats(Relation onerel, double totalrows,
				   AnlExprData *exprdata, int nexprs,
				   HeapTuple *rows, int numrows)
{
	MemoryContext expr_context,
				old_context;
	int			ind,
				i;

	expr_context = AllocSetContextCreate(CurrentMemoryContext,
										 "Analyze Expression",
										 ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(expr_context);

	for (ind = 0; ind < nexprs; ind++)
	{
		AnlExprData *thisdata = &exprdata[ind];
		VacAttrStats *stats = thisdata->vacattrstat;
		Node	   *expr = thisdata->expr;
		TupleTableSlot *slot;
		EState	   *estate;
		ExprContext *econtext;
		Datum	   *exprvals;
		bool	   *exprnulls;
		ExprState  *exprstate;
		int			tcnt;

		/* Are we still in the main context? */
		Assert(CurrentMemoryContext == expr_context);

		/*
		 * Need an EState for evaluation of expressions.  Create it in the
		 * per-expression context to be sure it gets cleaned up at the bottom
		 * of the loop.
		 */
		estate = CreateExecutorState();
		econtext = GetPerTupleExprContext(estate);

		/* Set up expression evaluation state */
		exprstate = ExecPrepareExpr((Expr *) expr, estate);

		/* Need a slot to hold the current heap tuple, too */
		slot = MakeSingleTupleTableSlot(RelationGetDescr(onerel),
										&TTSOpsHeapTuple);

		/* Arrange for econtext's scan tuple to be the tuple under test */
		econtext->ecxt_scantuple = slot;

		/* Compute and save expression values */
		exprvals = (Datum *) palloc(numrows * sizeof(Datum));
		exprnulls = (bool *) palloc(numrows * sizeof(bool));

		tcnt = 0;
		for (i = 0; i < numrows; i++)
		{
			Datum		datum;
			bool		isnull;

			/*
			 * Reset the per-tuple context each time, to reclaim any cruft
			 * left behind by evaluating the statistics expressions.
			 */
			ResetExprContext(econtext);

			/* Set up for expression evaluation */
			ExecStoreHeapTuple(rows[i], slot, false);

			/*
			 * Evaluate the expression. We do this in the per-tuple context so
			 * as not to leak memory, and then copy the result into the
			 * context created at the beginning of this function.
			 */
			datum = ExecEvalExprSwitchContext(exprstate,
											  GetPerTupleExprContext(estate),
											  &isnull);
			if (isnull)
			{
				exprvals[tcnt] = (Datum) 0;
				exprnulls[tcnt] = true;
			}
			else
			{
				/* Make sure we copy the data into the context. */
				Assert(CurrentMemoryContext == expr_context);

				exprvals[tcnt] = datumCopy(datum,
										   stats->attrtype->typbyval,
										   stats->attrtype->typlen);
				exprnulls[tcnt] = false;
			}

			tcnt++;
		}

		/*
		 * Now we can compute the statistics for the expression columns.
		 *
		 * XXX Unlike compute_index_stats we don't need to switch and reset
		 * memory contexts here, because we're only computing stats for a
		 * single expression (and not iterating over many indexes), so we just
		 * do it in expr_context. Note that compute_stats copies the result
		 * into stats->anl_context, so it does not disappear.
		 */
		if (tcnt > 0)
		{
			AttributeOpts *aopt =
			get_attribute_options(stats->attr->attrelid,
								  stats->attr->attnum);

			stats->exprvals = exprvals;
			stats->exprnulls = exprnulls;
			stats->rowstride = 1;
			stats->compute_stats(stats,
								 expr_fetch_func,
								 tcnt,
								 tcnt);

			/*
			 * If the n_distinct option is specified, it overrides the above
			 * computation.
			 */
			if (aopt != NULL && aopt->n_distinct != 0.0)
				stats->stadistinct = aopt->n_distinct;
		}

		/* And clean up */
		MemoryContextSwitchTo(expr_context);

		ExecDropSingleTupleTableSlot(slot);
		FreeExecutorState(estate);
		MemoryContextResetAndDeleteChildren(expr_context);
	}

	MemoryContextSwitchTo(old_context);
	MemoryContextDelete(expr_context);
}


/*
 * Fetch function for analyzing statistics object expressions.
 *
 * We have not bothered to construct tuples from the data, instead the data
 * is just in Datum arrays.
 */
static Datum
expr_fetch_func(VacAttrStatsP stats, int rownum, bool *isNull)
{
	int			i;

	/* exprvals and exprnulls are already offset for proper column */
	i = rownum * stats->rowstride;
	*isNull = stats->exprnulls[i];
	return stats->exprvals[i];
}

/*
 * Build analyze data for a list of expressions. As this is not tied
 * directly to a relation (table or index), we have to fake some of
 * the fields in examine_expression().
 */
static AnlExprData *
build_expr_data(List *exprs, int stattarget)
{
	int			idx;
	int			nexprs = list_length(exprs);
	AnlExprData *exprdata;
	ListCell   *lc;

	exprdata = (AnlExprData *) palloc0(nexprs * sizeof(AnlExprData));

	idx = 0;
	foreach(lc, exprs)
	{
		Node	   *expr = (Node *) lfirst(lc);
		AnlExprData *thisdata = &exprdata[idx];

		thisdata->expr = expr;
		thisdata->vacattrstat = examine_expression(expr, stattarget);
		idx++;
	}

	return exprdata;
}

/* form an array of pg_statistic rows (per update_attstats) */
static Datum
serialize_expr_stats(AnlExprData *exprdata, int nexprs)
{
	int			exprno;
	Oid			typOid;
	Relation	sd;

	ArrayBuildState *astate = NULL;

	sd = table_open(StatisticRelationId, RowExclusiveLock);

	/* lookup OID of composite type for pg_statistic */
	typOid = get_rel_type_id(StatisticRelationId);
	if (!OidIsValid(typOid))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation \"%s\" does not have a composite type",
						"pg_statistic")));

	for (exprno = 0; exprno < nexprs; exprno++)
	{
		int			i,
					k;
		VacAttrStats *stats = exprdata[exprno].vacattrstat;

		Datum		values[Natts_pg_statistic];
		bool		nulls[Natts_pg_statistic];
		HeapTuple	stup;

		if (!stats->stats_valid)
		{
			astate = accumArrayResult(astate,
									  (Datum) 0,
									  true,
									  typOid,
									  CurrentMemoryContext);
			continue;
		}

		/*
		 * Construct a new pg_statistic tuple
		 */
		for (i = 0; i < Natts_pg_statistic; ++i)
		{
			nulls[i] = false;
		}

		values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(InvalidOid);
		values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(InvalidAttrNumber);
		values[Anum_pg_statistic_stainherit - 1] = BoolGetDatum(false);
		values[Anum_pg_statistic_stanullfrac - 1] = Float4GetDatum(stats->stanullfrac);
		values[Anum_pg_statistic_stawidth - 1] = Int32GetDatum(stats->stawidth);
		values[Anum_pg_statistic_stadistinct - 1] = Float4GetDatum(stats->stadistinct);
		i = Anum_pg_statistic_stakind1 - 1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			values[i++] = Int16GetDatum(stats->stakind[k]); /* stakindN */
		}
		i = Anum_pg_statistic_staop1 - 1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			values[i++] = ObjectIdGetDatum(stats->staop[k]);	/* staopN */
		}
		i = Anum_pg_statistic_stacoll1 - 1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			values[i++] = ObjectIdGetDatum(stats->stacoll[k]);	/* stacollN */
		}
		i = Anum_pg_statistic_stanumbers1 - 1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			int			nnum = stats->numnumbers[k];

			if (nnum > 0)
			{
				int			n;
				Datum	   *numdatums = (Datum *) palloc(nnum * sizeof(Datum));
				ArrayType  *arry;

				for (n = 0; n < nnum; n++)
					numdatums[n] = Float4GetDatum(stats->stanumbers[k][n]);
				/* XXX knows more than it should about type float4: */
				arry = construct_array(numdatums, nnum,
									   FLOAT4OID,
									   sizeof(float4), true, TYPALIGN_INT);
				values[i++] = PointerGetDatum(arry);	/* stanumbersN */
			}
			else
			{
				nulls[i] = true;
				values[i++] = (Datum) 0;
			}
		}
		i = Anum_pg_statistic_stavalues1 - 1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			if (stats->numvalues[k] > 0)
			{
				ArrayType  *arry;

				arry = construct_array(stats->stavalues[k],
									   stats->numvalues[k],
									   stats->statypid[k],
									   stats->statyplen[k],
									   stats->statypbyval[k],
									   stats->statypalign[k]);
				values[i++] = PointerGetDatum(arry);	/* stavaluesN */
			}
			else
			{
				nulls[i] = true;
				values[i++] = (Datum) 0;
			}
		}

		stup = heap_form_tuple(RelationGetDescr(sd), values, nulls);

		astate = accumArrayResult(astate,
								  heap_copy_tuple_as_datum(stup, RelationGetDescr(sd)),
								  false,
								  typOid,
								  CurrentMemoryContext);
	}

	table_close(sd, RowExclusiveLock);

	return makeArrayResult(astate, CurrentMemoryContext);
}

/*
 * Loads pg_statistic record from expression statistics for expression
 * identified by the supplied index.
 */
HeapTuple
statext_expressions_load(Oid stxoid, int idx)
{
	bool		isnull;
	Datum		value;
	HeapTuple	htup;
	ExpandedArrayHeader *eah;
	HeapTupleHeader td;
	HeapTupleData tmptup;
	HeapTuple	tup;

	htup = SearchSysCache1(STATEXTDATASTXOID, ObjectIdGetDatum(stxoid));
	if (!HeapTupleIsValid(htup))
		elog(ERROR, "cache lookup failed for statistics object %u", stxoid);

	value = SysCacheGetAttr(STATEXTDATASTXOID, htup,
							Anum_pg_statistic_ext_data_stxdexpr, &isnull);
	if (isnull)
		elog(ERROR,
			 "requested statistics kind \"%c\" is not yet built for statistics object %u",
			 STATS_EXT_DEPENDENCIES, stxoid);

	eah = DatumGetExpandedArray(value);

	deconstruct_expanded_array(eah);

	td = DatumGetHeapTupleHeader(eah->dvalues[idx]);

	/* Build a temporary HeapTuple control structure */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
	ItemPointerSetInvalid(&(tmptup.t_self));
	tmptup.t_tableOid = InvalidOid;
	tmptup.t_data = td;

	tup = heap_copytuple(&tmptup);

	ReleaseSysCache(htup);

	return tup;
}

/*
 * Evaluate the expressions, so that we can use the results to build
 * all the requested statistics types. This matters especially for
 * expensive expressions, of course.
 */
static StatsBuildData *
make_build_data(Relation rel, StatExtEntry *stat, int numrows, HeapTuple *rows,
				VacAttrStats **stats, int stattarget)
{
	/* evaluated expressions */
	StatsBuildData *result;
	char	   *ptr;
	Size		len;

	int			i;
	int			k;
	int			idx;
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	List	   *exprstates = NIL;
	int			nkeys = bms_num_members(stat->columns) + list_length(stat->exprs);
	ListCell   *lc;

	/* allocate everything as a single chunk, so we can free it easily */
	len = MAXALIGN(sizeof(StatsBuildData));
	len += MAXALIGN(sizeof(AttrNumber) * nkeys);	/* attnums */
	len += MAXALIGN(sizeof(VacAttrStats *) * nkeys);	/* stats */

	/* values */
	len += MAXALIGN(sizeof(Datum *) * nkeys);
	len += nkeys * MAXALIGN(sizeof(Datum) * numrows);

	/* nulls */
	len += MAXALIGN(sizeof(bool *) * nkeys);
	len += nkeys * MAXALIGN(sizeof(bool) * numrows);

	ptr = palloc(len);

	/* set the pointers */
	result = (StatsBuildData *) ptr;
	ptr += MAXALIGN(sizeof(StatsBuildData));

	/* attnums */
	result->attnums = (AttrNumber *) ptr;
	ptr += MAXALIGN(sizeof(AttrNumber) * nkeys);

	/* stats */
	result->stats = (VacAttrStats **) ptr;
	ptr += MAXALIGN(sizeof(VacAttrStats *) * nkeys);

	/* values */
	result->values = (Datum **) ptr;
	ptr += MAXALIGN(sizeof(Datum *) * nkeys);

	/* nulls */
	result->nulls = (bool **) ptr;
	ptr += MAXALIGN(sizeof(bool *) * nkeys);

	for (i = 0; i < nkeys; i++)
	{
		result->values[i] = (Datum *) ptr;
		ptr += MAXALIGN(sizeof(Datum) * numrows);

		result->nulls[i] = (bool *) ptr;
		ptr += MAXALIGN(sizeof(bool) * numrows);
	}

	Assert((ptr - (char *) result) == len);

	/* we have it allocated, so let's fill the values */
	result->nattnums = nkeys;
	result->numrows = numrows;

	/* fill the attribute info - first attributes, then expressions */
	idx = 0;
	k = -1;
	while ((k = bms_next_member(stat->columns, k)) >= 0)
	{
		result->attnums[idx] = k;
		result->stats[idx] = stats[idx];

		idx++;
	}

	k = -1;
	foreach(lc, stat->exprs)
	{
		Node	   *expr = (Node *) lfirst(lc);

		result->attnums[idx] = k;
		result->stats[idx] = examine_expression(expr, stattarget);

		idx++;
		k--;
	}

	/* first extract values for all the regular attributes */
	for (i = 0; i < numrows; i++)
	{
		idx = 0;
		k = -1;
		while ((k = bms_next_member(stat->columns, k)) >= 0)
		{
			result->values[idx][i] = heap_getattr(rows[i], k,
												  result->stats[idx]->tupDesc,
												  &result->nulls[idx][i]);

			idx++;
		}
	}

	/* Need an EState for evaluation expressions. */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);

	/* Need a slot to hold the current heap tuple, too */
	slot = MakeSingleTupleTableSlot(RelationGetDescr(rel),
									&TTSOpsHeapTuple);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/* Set up expression evaluation state */
	exprstates = ExecPrepareExprList(stat->exprs, estate);

	for (i = 0; i < numrows; i++)
	{
		/*
		 * Reset the per-tuple context each time, to reclaim any cruft left
		 * behind by evaluating the statistics object expressions.
		 */
		ResetExprContext(econtext);

		/* Set up for expression evaluation */
		ExecStoreHeapTuple(rows[i], slot, false);

		idx = bms_num_members(stat->columns);
		foreach(lc, exprstates)
		{
			Datum		datum;
			bool		isnull;
			ExprState  *exprstate = (ExprState *) lfirst(lc);

			/*
			 * XXX This probably leaks memory. Maybe we should use
			 * ExecEvalExprSwitchContext but then we need to copy the result
			 * somewhere else.
			 */
			datum = ExecEvalExpr(exprstate,
								 GetPerTupleExprContext(estate),
								 &isnull);
			if (isnull)
			{
				result->values[idx][i] = (Datum) 0;
				result->nulls[idx][i] = true;
			}
			else
			{
				result->values[idx][i] = (Datum) datum;
				result->nulls[idx][i] = false;
			}

			idx++;
		}
	}

	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);

	return result;
}

/*
 * sample_alloc
 *		allocate a sample with space for maxrows (we'll resize if needed)
 */
static Sample *
sample_alloc(Bitmapset *attrs, List *exprs, int maxrows)
{
	Sample *sample;
	int		nattributes = bms_num_members(attrs) + list_length(exprs);

	/* basic struct */
	sample = palloc0(sizeof(Sample));

	/* XXX should we copy this? */
	sample->attnums = attrs;
	sample->exprs = exprs;
	sample->maxrows = maxrows;

	sample->values = palloc(sizeof(Datum) * nattributes * maxrows);
	sample->isnull = palloc(sizeof(bool) * nattributes * maxrows);

	return sample;
}

/*
 * sample_free
 *		free the row sample
 */
static void
sample_free(Sample *sample)
{
	/* XXX maybe free the attnums / exprs */
	pfree(sample->values);
	pfree(sample->isnull);
	pfree(sample);
}

/*
 * sample_add_tuple
 *		add tuple to the random sample, resize if needed
 *
 * We extract the attnums from the heap tuples, and keep simple array of Datum
 * values and bool isnull flags.
 */
static void
sample_add_tuple(Sample *sample, TupleDesc tdesc, HeapTuple htup)
{
	int	attno;
	int	nattrs = bms_num_members(sample->attnums) + list_length(sample->exprs);

	int	idx = sample->nrows * nattrs;

	if (sample->maxrows == sample->nrows)
	{
		sample->maxrows *= 2;
		sample->values = repalloc(sample->values, sample->maxrows * nattrs * sizeof(Datum));
		sample->isnull = repalloc(sample->isnull, sample->maxrows * nattrs * sizeof(bool));
	}

	for (attno = 1; attno <= nattrs; attno++)
	{
		if (!htup)
		{
			/* if no tuple, treat it as all NULL values (won't match any conditions) */
			sample->values[idx] = (Datum) 0;
			sample->isnull[idx] = true;
		}
		else
			sample->values[idx] = heap_getattr(htup, attno, tdesc,
											   &sample->isnull[idx]);
		idx++;
	}

	sample->nrows++;
}

/*
 * sample_calculate_size
 *		calculate the sampling rate for tablesample
 *
 * We look at statistics target for the statistics object, or the default one if
 * not set, and we use that as a fraction for tablesample. We also combine that
 * with estimate_sample_rate GUC.
 *
 * XXX Maybe we should look at per-attribute targets too.
 */
static double
sample_calculate_size(StatisticExtInfo *stat, double ntuples, double *nrows)
{
	int			stattarget;
	HeapTuple	htup;
	Datum		datum;
	bool		isnull;
	double		sample_rate;

	/* determine stattarget, if any */
	htup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(stat->statOid));
	if (!HeapTupleIsValid(htup))
		elog(ERROR, "cache lookup failed for statistics object %u", stat->statOid);

	/* Determine which statistics types exist */
	datum = SysCacheGetAttr(STATEXTOID, htup,
							Anum_pg_statistic_ext_stxstattarget, &isnull);

	Assert(!isnull);

	stattarget = DatumGetInt32(datum);
	if (stattarget == -1)
		stattarget = default_statistics_target;

	ReleaseSysCache(htup);

	*nrows = Min(300.0 * stattarget, ntuples);

	/* use the GUC of stattarget, whatever gives higher sample rate */
	sample_rate = Max(estimate_sample_rate, *nrows / ntuples);

	*nrows = ntuples * sample_rate;

	return sample_rate;
}

/*
 * statext_collect_sample
 *		build a random sample for the relation
 *
 * XXX We sample all attributes and expresions the statistics is defined on, but
 * we should sample only what's referenced in the query (both as a join clause
 * and base restrictions).
 *
 * XXX This uses the regular TABLESAMPLE through SPI, as that's the simplest way.
 * Good enough for PoC, but maybe there's a better way to do this.
 */
static Sample *
statext_collect_sample(PlannerInfo *root, StatisticExtInfo *stat)
{
	int		ret;
	int		i, k;
	uint64	proc;
	StringInfoData str;
	bool	first;
	RangeTblEntry *rte = planner_rt_fetch(stat->rel->relid, root);
	Oid		relid = rte->relid;
	ListCell *lc;
	bool	reset;

	double	sample_rows;
	double	sample_rate = sample_calculate_size(stat, stat->rel->tuples,
												&sample_rows);

	Sample *sample = sample_alloc(stat->keys, stat->exprs, sample_rows);

	SPITupleTable  *spi_tuptable;
	TupleDesc		spi_tupdesc;

	List	   *context;
	initStringInfo(&str);

	/* internal error */
	if ((ret = SPI_connect()) < 0)
		elog(ERROR, "statext_collect_sample: SPI_connect returned %d", ret);

	appendStringInfoString(&str, "SELECT ");

	first = true;
	k = -1;

	while ((k = bms_next_member(stat->keys, k)) >= 0)
	{
		AttrNumber attnum = k;

		if (!first)
			appendStringInfo(&str, ", ");

		appendStringInfo(&str, "%s", get_attname(relid, attnum, false));

		first = false;
	}

	context = deparse_context_for(get_rel_name(relid), relid);

	foreach(lc, stat->exprs)
	{
		Node   *expr = (Node *) lfirst(lc);
		char   *expr_str;

		/* tweak the varno */
		if (stat->rel->relid != 1)
		{
			expr = copyObject(expr);
			ChangeVarNodes(expr, stat->rel->relid, 1, 0);
		}

		expr_str = deparse_expression(expr, context, false, false);

		if (!first)
			appendStringInfo(&str, ", ");

		appendStringInfo(&str, "%s", expr_str);

		first = false;
	}

	/*
	 * XXX Maybe for joins this could also include the additional non-join
	 * conditions, derived from the baserestrictinfos. That would make the
	 * sample smaller, and we would not need to bother with evaluating the
	 * conditions later. Both would make it more efficient. It may make the
	 * sample less useful for other queries (not an issue for samples built
	 * at run-time for each query, but if retained it'd be a problem).
	 */
	appendStringInfo(&str, " FROM %s.%s TABLESAMPLE BERNOULLI (%f)",
					 quote_identifier(get_namespace_name(get_rel_namespace(relid))),
					 quote_identifier(get_rel_name(relid)), 100 * sample_rate);

	/*
	 * disable sampling for this sampling query
	 *
	 * The query is simple enough and should always use the unique index, so
	 * there's very little risk of poor query plans. Moreover, there might be
	 * a risk of infinite cycles (having to sample when collecting a sample).
	 */
	reset = enable_sample_estimates_scan;
	enable_sample_estimates_scan = false;

	ret = SPI_execute(str.data, true, 0);
	proc = SPI_processed;

	/* If no qualifying tuples, fall out early */
	if (ret != SPI_OK_SELECT || proc == 0)
	{
		SPI_finish();
		return NULL;
	}

	spi_tuptable = SPI_tuptable;
	spi_tupdesc = spi_tuptable->tupdesc;

	for (i = 0; i < proc; i++)
		sample_add_tuple(sample, spi_tupdesc, spi_tuptable->vals[i]);

	elog(WARNING, "statext_collect_sample: sampled %ld rows", proc);

	SPI_finish();

	enable_sample_estimates_scan = reset;

	return sample;
}

/*
 * statext_collect_correlated_sample
 *		build a correlated random sample for the relation
 *
 * Given an existing sample on A, find matching rows in relation B. This requires
 * a primary key on B, but could be relaxed to a unique index or even any index.
 * With non-unique indexes we may need to sample the rows somehow.
 *
 * This works best for cases with star/snowflake schema. It's inspired by papers
 *
 * Cardinality Estimation Done Right: Index-Based Join Sampling - Viktor Leis and
 *   B. Radke and Andrey Gubichev and A. Kemper and Thomas Neumann, CIDR 2017
 *
 * CS2: A New Database Synopsis for Query Estimation - Feng Yu, Southern Illinois
 *   University; Wen-Chi Hou, Southern Illinois University; Cheng    Luo, Coppin
 *   State University; Dunren Che, Southern Illinois University; Mengxia Zhu,
 *   Southern Illinois University
 *
 * XXX The optimizer already uses the foreign key info to improve estimates, but
 * that does not consider cross-table correlations or additional conditions on
 * the tables. So this should probably take precedence, before considering the
 * foreign keys.
 */
static Sample *
statext_collect_correlated_sample(PlannerInfo *root, StatisticExtInfo *stat,
								  StatisticExtInfo *stat2, Sample *sample2, List *clauses)
{
	int		ret;
	int		i, k;
	uint64	proc;
	StringInfoData str;
	bool	first;
	RangeTblEntry *rte = planner_rt_fetch(stat->rel->relid, root);
	Oid		relid = rte->relid;
	ListCell *lc;

	/*
	 * Use the same number of rows as the source sample (we'll lookup by
	 * primary key).
	 */
	Sample *sample = sample_alloc(stat->keys, stat->exprs, sample2->nrows);

	SPIPlanPtr		pplan;
	SPITupleTable  *spi_tuptable;
	TupleDesc		spi_tupdesc;

	List	   *context;

	initStringInfo(&str);

	appendStringInfoString(&str, "SELECT ");

	first = true;
	k = -1;

	while ((k = bms_next_member(stat->keys, k)) >= 0)
	{
		AttrNumber attnum = k;

		if (!first)
			appendStringInfo(&str, ", ");

		appendStringInfo(&str, "%s", get_attname(relid, attnum, false));

		first = false;
	}

	context = deparse_context_for(get_rel_name(relid), relid);

	foreach(lc, stat->exprs)
	{
		Node   *expr = (Node *) lfirst(lc);
		char   *expr_str;

		/* tweak the varno */
		if (stat->rel->relid != 1)
		{
			expr = copyObject(expr);
			ChangeVarNodes(expr, stat->rel->relid, 1, 0);
		}

		expr_str = deparse_expression(expr, context, false, false);

		if (!first)
			appendStringInfo(&str, ", ");

		appendStringInfo(&str, "%s", expr_str);

		first = false;
	}

	appendStringInfo(&str, " FROM %s.%s",
					 quote_identifier(get_namespace_name(get_rel_namespace(relid))),
					 quote_identifier(get_rel_name(relid)));

	/*
	 * Extract information from the join conditions - which attributes to add to
	 * the SQL query, which values to use from the existing sample, etc.
	*/
	{
		ListCell *lc;
		int j;

		int			idx = 0;
		int			natts = list_length(clauses);
		AttrNumber *attnums1 = (AttrNumber *) palloc(sizeof(AttrNumber) * natts);
		AttrNumber *attnums2 = (AttrNumber *) palloc(sizeof(AttrNumber) * natts);
		Oid		   *atttypes = (Oid *) palloc(sizeof(Oid) * natts);
		int		   *attmap = (int *) palloc(sizeof(int) * natts);
		bool		reset;

		Datum  *values = palloc(sizeof(Datum) * natts);
		char   *nulls = palloc(sizeof(char) * natts);

		/* expects trivial */
		foreach (lc, clauses)
		{
			Node *clause = (Node *) lfirst(lc);
			Bitmapset *attnums = NULL;

			pull_varattnos(clause, stat->rel->relid, &attnums);

			attnums1[idx] = bms_singleton_member(attnums) + FirstLowInvalidHeapAttributeNumber;

			bms_free(attnums);
			attnums = NULL;

			pull_varattnos(clause, stat2->rel->relid, &attnums);

			attnums2[idx] = bms_singleton_member(attnums) + FirstLowInvalidHeapAttributeNumber;

			bms_free(attnums);
			attnums = NULL;

			atttypes[idx] = get_atttype(relid, attnums1[idx]);

			/* which index in the other sample */
			attmap[idx] = bms_member_index(stat2->keys, attnums2[idx]);

			idx++;
		}

		Assert(idx == natts);

		appendStringInfoString(&str, " WHERE ");

		for (i = 0; i < natts; i++)
		{
			if (i > 0)
				appendStringInfoString(&str, " AND ");

			appendStringInfo(&str, " %s = $%d ", get_attname(relid, attnums1[i], false), (i+1));
		}

		/*
		 * XXX Maybe for joins this could also include the additional non-join
		 * conditions, derived from the baserestrictinfos. That would make the
		 * sample smaller, and we would not need to bother with evaluating the
		 * conditions later. Both would make it more efficient. It may make the
		 * sample less useful for other queries (not an issue for samples built
		 * at run-time for each query, but if retained it'd be a problem).
		 */

		elog(WARNING, "SQL: %s", str.data);

		/* internal error */
		if ((ret = SPI_connect()) < 0)
			elog(ERROR, "statext_collect_sample: SPI_connect returned %d", ret);

		/*
		 * disable sampling for this sampling query
		 *
		 * The query is simple enough and should always use the unique index, so
		 * there's very little risk of poor query plans. Moreover, there might be
		 * a risk of infinite cycles (having to sample when collecting a sample).
		 */
		reset = enable_sample_estimates_scan;
		enable_sample_estimates_scan = false;

		/* prepare the lookup statement */
		pplan = SPI_prepare(str.data, natts, atttypes);

		/*
		 * Lookup the values for all rows in the existing sample.
		 *
		 * XXX We might sort te sample2 so that it's easy to identify values
		 * that are equal, and skip the index lookup in that case.
		 */
		for (j = 0; j < sample2->nrows; j++)
		{
			int	ret;
			int	natts2 = bms_num_members(stat2->keys) + list_length(stat2->exprs);

			/* build parameters for the prepared statement */
			for (i = 0; i < natts; i++)
			{
				int	idx = j * natts2 + attmap[i];

				values[i] = sample2->values[idx];

				if (sample2->isnull[idx])
					nulls[i] = 'n';
				else
					nulls[i] = ' ';
			}

			/* run the prepared statement */
			ret = SPI_execp(pplan, values, nulls, natts);
			proc = SPI_processed;

			/* results of the lookup */
			spi_tuptable = SPI_tuptable;
			spi_tupdesc = spi_tuptable->tupdesc;

			/*
			 * We could relax this to work with any indexes, in which case this
			 * will fail (and we'll need to sample the returned rows somehow).
			 */
			Assert(proc <= 1);

			/* If no qualifying tuples, add "empty" tuple */
			if (ret != SPI_OK_SELECT || proc == 0)
				sample_add_tuple(sample, spi_tupdesc, NULL);
			else
				sample_add_tuple(sample, spi_tupdesc, spi_tuptable->vals[0]);

			SPI_freetuptable(spi_tuptable);
		}

		/* restore the value */
		enable_sample_estimates_scan = reset;
	}

	SPI_finish();

	SPI_freeplan(pplan);

	elog(WARNING, "statext_collect_correlated_sample: sampled %d rows", sample->nrows);

	return sample;
}

/*
 * match the attribute/expression to a dimension of the statistic
 *
 * Match the attribute/expression to statistics dimension. Optionally
 * determine the collation.
 */
static int
sample_match_expression(Node *expr, Bitmapset *keys, List *exprs, Oid *collid)
{
	int			idx = -1;

	if (IsA(expr, Var))
	{
		/* simple Var, so just lookup using varattno */
		Var		   *var = (Var *) expr;

		if (collid)
			*collid = var->varcollid;

		idx = bms_member_index(keys, var->varattno);

		/* make sure the index is valid */
		Assert((idx >= 0) && (idx <= bms_num_members(keys)));
	}
	else
	{
		ListCell   *lc;

		/* expressions are stored after the simple columns */
		idx = bms_num_members(keys);

		if (collid)
			*collid = exprCollation(expr);

		/* expression - lookup in stats expressions */
		foreach(lc, exprs)
		{
			Node	   *stat_expr = (Node *) lfirst(lc);

			if (equal(expr, stat_expr))
				break;

			idx++;
		}

		/* make sure the index is valid */
		Assert((idx >= bms_num_members(keys)) &&
			   (idx <= bms_num_members(keys) + list_length(exprs)));
	}

	Assert((idx >= 0) && (idx < bms_num_members(keys) + list_length(exprs)));

	return idx;
}

#define RESULT_MERGE(value, is_or, match) \
	((is_or) ? ((value) || (match)) : ((value) && (match)))

#define RESULT_IS_FINAL(value, is_or)	((is_or) ? (value) : (!(value)))

static bool *
sample_get_match_bitmap(PlannerInfo *root, List *clauses,
						Bitmapset *keys, List *exprs,
						Sample *sample, bool is_or)
{
	ListCell *l;
	bool   *matches = palloc(sample->nrows * sizeof(bool));
	int		nattrs = bms_num_members(keys) + list_length(exprs);

	memset(matches, (is_or) ? false : true,
		   sizeof(bool) * sample->nrows);

	/*
	 * Loop through the list of clauses, and for each of them evaluate all
	 * the sampled rows not yet eliminated by the preceding clauses.
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
			FmgrInfo	opproc;

			/* valid only after examine_opclause_args returns true */
			Node	   *clause_expr;
			Const	   *cst;
			bool		expronleft;
			int			idx;
			Oid			collid;
			int			i;

			fmgr_info(get_opcode(expr->opno), &opproc);

			/* extract the var/expr and const from the expression */
			if (!examine_opclause_args(expr->args, &clause_expr, &cst, &expronleft))
				elog(ERROR, "incompatible clause");

			/* match the attribute/expression to a dimension of the statistic */
			idx = sample_match_expression(clause_expr, keys, exprs, &collid);

			Assert((idx >= 0) && (idx < bms_num_members(keys) + list_length(exprs)));

			/*
			 * Walk through the sampled rows and evaluate the current clause. We
			 * can skip items that were already ruled out, and terminate if
			 * there are no remaining MCV items that might possibly match.
			 */
			for (i = 0; i < sample->nrows; i++)
			{
				bool		match = true;
				Datum	   *values = &sample->values[i * nattrs];
				bool	   *isnull = &sample->isnull[i * nattrs];

				Assert(idx >= 0);

				/*
				 * When the sampled row or the Const value is NULL we can treat
				 * this as a mismatch. We must not call the operator because
				 * of strictness.
				 */
				if (isnull[idx] || cst->constisnull)
				{
					matches[i] = RESULT_MERGE(matches[i], is_or, false);
					continue;
				}

				/*
				 * Skip sampled rows that can't change result in the bitmap. Once
				 * the value gets false for AND-lists, or true for OR-lists, we
				 * don't need to look at more clauses.
				 */
				if (RESULT_IS_FINAL(matches[i], is_or))
					continue;

				/*
				 * First check whether the constant is below the lower
				 * boundary (in that case we can skip the bucket, because
				 * there's no overlap).
				 *
				 * We don't store collations used to build the statistics, but
				 * we can use the collation for the attribute itself, as
				 * stored in varcollid. We do reset the statistics after a
				 * type change (including collation change), so this is OK.
				 * For expressions, we use the collation extracted from the
				 * expression itself.
				 */
				if (expronleft)
					match = DatumGetBool(FunctionCall2Coll(&opproc,
														   collid,
														   values[idx],
														   cst->constvalue));
				else
					match = DatumGetBool(FunctionCall2Coll(&opproc,
														   collid,
														   cst->constvalue,
														   values[idx]));

				/* update the match bitmap with the result */
				matches[i] = RESULT_MERGE(matches[i], is_or, match);
			}
		}
		else if (IsA(clause, ScalarArrayOpExpr))
		{
			ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) clause;
			FmgrInfo	opproc;

			/* valid only after examine_opclause_args returns true */
			Node	   *clause_expr;
			Const	   *cst;
			bool		expronleft;
			Oid			collid;
			int			idx,
						i;

			/* array evaluation */
			ArrayType  *arrayval;
			int16		elmlen;
			bool		elmbyval;
			char		elmalign;
			int			num_elems;
			Datum	   *elem_values;
			bool	   *elem_nulls;

			fmgr_info(get_opcode(expr->opno), &opproc);

			/* extract the var/expr and const from the expression */
			if (!examine_opclause_args(expr->args, &clause_expr, &cst, &expronleft))
				elog(ERROR, "incompatible clause");

			/* ScalarArrayOpExpr has the Var always on the left */
			Assert(expronleft);

			/* XXX what if (cst->constisnull == NULL)? */
			if (!cst->constisnull)
			{
				arrayval = DatumGetArrayTypeP(cst->constvalue);
				get_typlenbyvalalign(ARR_ELEMTYPE(arrayval),
									 &elmlen, &elmbyval, &elmalign);
				deconstruct_array(arrayval,
								  ARR_ELEMTYPE(arrayval),
								  elmlen, elmbyval, elmalign,
								  &elem_values, &elem_nulls, &num_elems);
			}

			/* match the attribute/expression to a dimension of the statistic */
			idx = sample_match_expression(clause_expr, keys, exprs, &collid);

			/*
			 * Walk through the sample and evaluate the current clause. We
			 * can skip items that were already ruled out, and terminate if
			 * there are no remaining MCV items that might possibly match.
			 */
			for (i = 0; i < sample->nrows; i++)
			{
				int			j;
				bool		match = (expr->useOr ? false : true);
				Datum	   *values = &sample->values[i * nattrs];
				bool	   *isnull = &sample->isnull[i * nattrs];

				/*
				 * When the MCV item or the Const value is NULL we can treat
				 * this as a mismatch. We must not call the operator because
				 * of strictness.
				 */
				if (isnull[idx] || cst->constisnull)
				{
					matches[i] = RESULT_MERGE(matches[i], is_or, false);
					continue;
				}

				/*
				 * Skip MCV items that can't change result in the bitmap. Once
				 * the value gets false for AND-lists, or true for OR-lists,
				 * we don't need to look at more clauses.
				 */
				if (RESULT_IS_FINAL(matches[i], is_or))
					continue;

				for (j = 0; j < num_elems; j++)
				{
					Datum		elem_value = elem_values[j];
					bool		elem_isnull = elem_nulls[j];
					bool		elem_match;

					/* NULL values always evaluate as not matching. */
					if (elem_isnull)
					{
						match = RESULT_MERGE(match, expr->useOr, false);
						continue;
					}

					/*
					 * Stop evaluating the array elements once we reach a
					 * matching value that can't change - ALL() is the same as
					 * AND-list, ANY() is the same as OR-list.
					 */
					if (RESULT_IS_FINAL(match, expr->useOr))
						break;

					elem_match = DatumGetBool(FunctionCall2Coll(&opproc,
																collid,
																values[idx],
																elem_value));

					match = RESULT_MERGE(match, expr->useOr, elem_match);
				}

				/* update the match bitmap with the result */
				matches[i] = RESULT_MERGE(matches[i], is_or, match);
			}
		}
		else if (IsA(clause, NullTest))
		{
			int			i;
			NullTest   *expr = (NullTest *) clause;
			Node	   *clause_expr = (Node *) (expr->arg);

			/* match the attribute/expression to a dimension of the statistic */
			int			idx = sample_match_expression(clause_expr, keys, exprs, NULL);

			/*
			 * Walk through the sample and evaluate the current clause. We
			 * can skip items that were already ruled out, and terminate if
			 * there are no remaining MCV items that might possibly match.
			 */
			for (i = 0; i < sample->nrows; i++)
			{
				bool	match = false;	/* assume mismatch */
				bool   *isnull = &sample->isnull[i * nattrs];


				/* if the clause mismatches the MCV item, update the bitmap */
				switch (expr->nulltesttype)
				{
					case IS_NULL:
						match = (isnull[idx]) ? true : match;
						break;

					case IS_NOT_NULL:
						match = (!isnull[idx]) ? true : match;
						break;
				}

				/* now, update the match bitmap, depending on OR/AND type */
				matches[i] = RESULT_MERGE(matches[i], is_or, match);
			}
		}
		else if (is_orclause(clause) || is_andclause(clause))
		{
			/* AND/OR clause, with all subclauses being compatible */

			int			i;
			BoolExpr   *bool_clause = ((BoolExpr *) clause);
			List	   *bool_clauses = bool_clause->args;

			/* match/mismatch bitmap for each MCV item */
			bool	   *bool_matches = NULL;

			Assert(bool_clauses != NIL);
			Assert(list_length(bool_clauses) >= 2);

			/* build the match bitmap for the OR-clauses */
			bool_matches = sample_get_match_bitmap(root, bool_clauses, keys, exprs,
												   sample, is_orclause(clause));

			/*
			 * Merge the bitmap produced by mcv_get_match_bitmap into the
			 * current one. We need to consider if we're evaluating AND or OR
			 * condition when merging the results.
			 */
			for (i = 0; i < sample->nrows; i++)
				matches[i] = RESULT_MERGE(matches[i], is_or, bool_matches[i]);

			pfree(bool_matches);
		}
		else if (is_notclause(clause))
		{
			/* NOT clause, with all subclauses compatible */

			int			i;
			BoolExpr   *not_clause = ((BoolExpr *) clause);
			List	   *not_args = not_clause->args;

			/* match/mismatch bitmap for each MCV item */
			bool	   *not_matches = NULL;

			Assert(not_args != NIL);
			Assert(list_length(not_args) == 1);

			/* build the match bitmap for the NOT-clause */
			not_matches = sample_get_match_bitmap(root, not_args, keys, exprs,
												  sample, false);

			/*
			 * Merge the bitmap produced by mcv_get_match_bitmap into the
			 * current one. We're handling a NOT clause, so invert the result
			 * before merging it into the global bitmap.
			 */
			for (i = 0; i < sample->nrows; i++)
				matches[i] = RESULT_MERGE(matches[i], is_or, !not_matches[i]);

			pfree(not_matches);
		}
		else if (IsA(clause, Var))
		{
			/* Var (has to be a boolean Var, possibly from below NOT) */
			Var		   *var = (Var *) (clause);
			int			i;

			/* match the attribute to a dimension of the statistic */
			int			idx = bms_member_index(keys, var->varattno);

			Assert(var->vartype == BOOLOID);

			/*
			 * Walk through the MCV items and evaluate the current clause. We
			 * can skip items that were already ruled out, and terminate if
			 * there are no remaining MCV items that might possibly match.
			 */
			for (i = 0; i < sample->nrows; i++)
			{
				Datum	   *values = &sample->values[i * nattrs];
				bool	   *isnull = &sample->isnull[i * nattrs];
				bool		match = false;

				/* if the item is NULL, it's a mismatch */
				if (!isnull[idx] && DatumGetBool(values[idx]))
					match = true;

				/* update the result bitmap */
				matches[i] = RESULT_MERGE(matches[i], is_or, match);
			}
		}
		else
			elog(ERROR, "unknown clause type: %d", clause->type);
	}

	return matches;
}

static Selectivity
sample_clauselist_selectivity(PlannerInfo *root, StatisticExtInfo *stat,
							  Sample *sample, List *clauses, int varRelid,
							  JoinType jointype, SpecialJoinInfo *sjinfo,
							  RelOptInfo *rel, bool is_or)
{
	int			i;
	int			matched;

	/* match/mismatch bitmap for each sampled row */
	bool	   *matches = NULL;

	/* build a match bitmap for the clauses */
	matches = sample_get_match_bitmap(root, clauses,
									  sample->attnums, sample->exprs,
									  sample, is_or);

	/* sum frequencies for all the matching sampled rows */
	matched = 0;
	for (i = 0; i < sample->nrows; i++)
	{
		if (matches[i] != false)
			matched++;
	}

	return matched / (double) sample->nrows;
}

/*
 * statext_sample_get_conditions
 *		Get conditions on base relations, to be used as conditions for joins.
 *
 * When estimating joins using extended statistics, we can apply conditions
 * from base relations as conditions. This peeks at the baserestrictinfo
 * list for a relation and extracts those that are compatible with extended
 * statistics.
 *
 * XXX Requiring statext_is_compatible_clause for samples is a bit bogus, as
 * samples can be used to estimate almost any expression. So we should relax
 * this in the future, probably.
 */
static List *
statext_sample_get_conditions(PlannerInfo *root, RelOptInfo *rel,
						   StatisticExtInfo *info)
{
	ListCell   *lc;
	List	   *conditions = NIL;

	/* extract conditions that may be applied to the MCV list */
	foreach (lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
		Bitmapset *indexes = NULL;
		Bitmapset *attnums = NULL;
		List *exprs = NIL;

		/* clause has to be supported by sample in general */
		if (!statext_is_compatible_clause(root, (Node *) rinfo, rel->relid,
										  &attnums, &exprs))
			continue;

		/*
		 * clause is compatible in general, but is it actually covered
		 * by this partiular statistics object?
		 */
		if (!bms_is_subset(attnums, info->keys) ||
			!stat_covers_expressions(info, exprs, &indexes))
			continue;

		conditions = lappend(conditions, rinfo->clause);
	}

	return conditions;
}

/*
 * statext_mcv_eval_conditions
 *		Evaluate a list of conditions on the sample.
 *
 * This returns a match bitmap for the conditions, which can be used later
 * to restrict just the "interesting" part of the sample. Also returns
 * the selectivity of the conditions, or 1.0 if there are no conditions.
 */
static bool *
statext_sample_eval_conditions(PlannerInfo *root, RelOptInfo *rel,
							   StatisticExtInfo *stat, Sample *sample,
							   Selectivity *sel)
{
	List   *conditions;

	/* everything matches by default */
	*sel = 1.0;

	/*
	 * XXX We've already evaluated this before, when picking the statistics
	 * object. Maybe we should stash it somewhere, so that we don't have to
	 * evaluate it again.
	 */
	conditions = statext_sample_get_conditions(root, rel, stat);

	/* If no conditions, we're done. */
	if (!conditions)
		return NULL;

	/* what's the selectivity of the conditions alone? */
	*sel = clauselist_selectivity(root, conditions, rel->relid, 0, NULL);

	return sample_get_match_bitmap(root, conditions, stat->keys, stat->exprs,
								   sample, false);
}

static bool
statext_can_use_correlated_sample(PlannerInfo *root, RelOptInfo *rel, List *clauses)
{
	RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
	Oid		relid = rte->relid;
	Oid		constraintOid;
	Bitmapset *pkattnos = NULL;
	Bitmapset *attnos = NULL;

	pkattnos = get_primary_key_attnos(relid, true, &constraintOid);

	if (!OidIsValid(constraintOid))
		return false;

	pull_varattnos((Node *) clauses, rel->relid, &attnos);

	if (!bms_equal(attnos, pkattnos))
		return false;

	return true;
}

/*
 * statext_compare_samples
 *		Calculte join selectivity using extended statistics, similarly to
 *		eqjoinsel_inner.
 *
 * Considers restrictions on base relations too, essentially computing
 * a conditional probability
 *
 *	P(join clauses | baserestrictinfos on either side)
 *
 * Compared to eqjoinsel_inner there's a couple problems. With per-column
 * MCV lists it's obvious that the number of distinct values not covered
 * by the MCV is (ndistinct - size(MCV)). With multi-column MCVs it's not
 * that simple, particularly when the conditions are on a subset of the
 * MCV and NULLs are involved. E.g. with MCV (a,b,c) and conditions on
 * (a,b), it's not clear if the number of (a,b) combinations not covered
 * by the MCV is
 *
 * (ndistinct(a,b) - ndistinct_mcv(a,b))
 *
 * where ndistinct_mcv(a,b) is the number of distinct (a,b) combinations
 * included in the MCV list. These combinations may be present in the rest
 * of the data (outside MCV), just with some extra values in "c". So in
 * principle there may be between
 *
 * (ndistinct(a,b) - ndistinct_mcv(a,b)) and ndistinct(a,b)
 *
 * distinct values in the rest of the data. So we need to pick something
 * in between, there's no way to calculate this accurately.
 */
static Selectivity
statext_compare_samples(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2,
						StatisticExtInfo *stat1, StatisticExtInfo *stat2,
						List *clauses)
{
	Sample *sample1;
	Sample *sample2;
	int		i, j;
	Selectivity s = 0;

	/* items eliminated by conditions (if any) */
	bool   *conditions1 = NULL,
		   *conditions2 = NULL;

	double	conditions1_sel = 1.0,
			conditions2_sel = 1.0;

	bool   *matches1 = NULL,
		   *matches2 = NULL;

	double	matchfreq1,
			unmatchfreq1,
			matchfreq2,
			unmatchfreq2,
			freq1,
			freq2;

	int64	nmatches = 0,
			nmatches1 = 0,
			nunmatches1 = 0,
			nmatches2 = 0,
			nunmatches2 = 0;
	int64	total = 0;

	bool	correlated = false;

	/*
	 * XXX Extract attnums / expressions and sample just those.
	 *
	 * XXX The other thing we could do is sampling just the rows
	 * matching the conditions, which would make the sample a bit
	 * smaller.
	 *
	 * XXX And finally we can do GROUP BY to combine duplicate values,
	 * which should make the sample yet a bit smaller and cheaper to
	 * process. It'll be a bit like a large MCV, with each row having a
	 * frequency. This can be done easily in SPI, however that means the
	 * query will be dependent on the planner (it'll pick the grouping
	 * algorithm).
	 *
	 * XXX We could also build a count-min sketch instead of the sample.
	 * We can't restrict the count-min sketch with the conditions, but
	 * we can apply the conditions *before* the sketch gets built. Not
	 * sure if that can affect the query plan (e.g. by using index for
	 * the conditions).
	 */
	if (enable_sample_join_correlate &&
		statext_can_use_correlated_sample(root, rel1, clauses))
	{
		sample2 = statext_collect_sample(root, stat2);
		sample1 = statext_collect_correlated_sample(root, stat1, stat2, sample2, clauses);
		correlated = true;
	}
	else if (enable_sample_join_correlate &&
			 statext_can_use_correlated_sample(root, rel2, clauses))
	{
		sample1 = statext_collect_sample(root, stat1);
		sample2 = statext_collect_correlated_sample(root, stat2, stat1, sample1, clauses);
		correlated = true;
	}
	else
	{
		sample1 = statext_collect_sample(root, stat1);
		sample2 = statext_collect_sample(root, stat2);
	}

	/* should only get here with samples on both sides */
	Assert(sample1 && sample2);

	matches1 = (bool *) palloc0(sizeof(bool) * sample1->nrows);
	matches2 = (bool *) palloc0(sizeof(bool) * sample2->nrows);

	/* apply baserestrictinfo conditions on the MCV lists */

	conditions1 = statext_sample_eval_conditions(root, rel1, stat1, sample1,
												 &conditions1_sel);

	conditions2 = statext_sample_eval_conditions(root, rel2, stat2, sample2,
												 &conditions2_sel);

	/*
	 * Match items from the two samples.
	 *
	 * We don't know if the matches are 1:1 - we may have overlap on only
	 * a subset of attributes, e.g. (a,b,c) vs. (b,c,d), so there may be
	 * multiple matches.
	 */
	for (i = 0; i < sample1->nrows; i++)
	{
		int	start = 0;
		int	end = sample2->nrows;

		/* skip items eliminated by restrictions on rel1 */
		if (conditions1 && !conditions1[i])
			continue;

		CHECK_FOR_INTERRUPTS();

		if (correlated)
		{
			start = i;
			end = i+1;
		}

		/* find matches in the second MCV list */
		for (j = start; j < end; j++)
		{
			ListCell   *lc;
			bool		items_match = true;

			/* skip items eliminated by restrictions on rel2 */
			if (conditions2 && !conditions2[j])
				continue;

			foreach (lc, clauses)
			{
				Node *clause = (Node *) lfirst(lc);
				Bitmapset  *atts1 = NULL;
				Bitmapset  *atts2 = NULL;
				Datum		value1, value2;
				int			index1, index2;
				AttrNumber	attnum1;
				AttrNumber	attnum2;
				bool		match;
				int			natts1 = bms_num_members(sample1->attnums) + list_length(sample1->exprs);
				int			natts2 = bms_num_members(sample2->attnums) + list_length(sample2->exprs);

				FmgrInfo	opproc;
				OpExpr	   *expr = (OpExpr *) clause;

				Assert(is_opclause(clause));

				fmgr_info(get_opcode(expr->opno), &opproc);

				/* determine the columns in each statistics object */

				/* FIXME make this work with expressions too */
				pull_varattnos(clause, rel1->relid, &atts1);
				attnum1 = bms_singleton_member(atts1) + FirstLowInvalidHeapAttributeNumber;
				index1 = bms_member_index(stat1->keys, attnum1);

				pull_varattnos(clause, rel2->relid, &atts2);
				attnum2 = bms_singleton_member(atts2) + FirstLowInvalidHeapAttributeNumber;
				index2 = bms_member_index(stat2->keys, attnum2);

				/* translate attr indexes to index in the sample arrays */
				index1 = i * natts1 + index1;
				index2 = j * natts2 + index2;

				bms_free(atts1);
				bms_free(atts2);

				/* if either value is null, we're done */
				if (sample1->isnull[index1] || sample2->isnull[index2])
					match = false;
				else
				{
					value1 = sample1->values[index1];
					value2 = sample2->values[index2];

					/*
					 * FIXME Might have issues with order of parameters, but for
					 * same-type equality that should not matter.
					 * */
					match = DatumGetBool(FunctionCall2Coll(&opproc,
														   InvalidOid,
														   value1, value2));
				}

				items_match &= match;

				if (!items_match)
					break;
			}

			if (items_match)
			{
				matches1[i] = matches2[j] = true;
				nmatches += 1;
			}
		}
	}

	if (correlated)
		total = (int64) Min(sample1->nrows, stat1->rel->tuples) * (int64) Min(sample2->nrows, stat2->rel->tuples);
	else
		total = (int64) sample1->nrows * (int64) sample2->nrows;

	s = nmatches / (double) total;

	elog(WARNING, "statext_compare_samples nmatches %ld cartesian %ld s %f", nmatches, total, s);

	/*
	 * XXX The "unmatched" frequencies are probably not needed, as the
	 * sample should cover the whole data set (unlike a MCV list).
	 *
	 * XXX We should however look at the number of distinct groups in
	 * the matched columns/expressions, which should help us to calculate
	 * selectivity for the whole cross join. One possible extreme is we
	 * saw all the distinct values, and that all the groups grow about
	 * proportionally (and the join as ~square of that). Or maybe there
	 * are many other distinct groups, in which case the join grows
	 * slowly (closer to linear).
	 */

	for (i = 0; i < sample1->nrows; i++)
	{
		if (conditions1 && !conditions1[i])
			continue;

		if (matches1[i])
			nmatches1++;
		else
			nunmatches1++;
	}

	matchfreq1 = nmatches1 / (double) sample1->nrows;
	unmatchfreq1 = nunmatches1 / (double) sample1->nrows;
	freq1 = 1.0;

	for (i = 0; i < sample2->nrows; i++)
	{
		if (conditions2 && !conditions2[i])
			continue;

		if (matches2[i])
			nmatches2++;
		else
			nunmatches2++;
	}

	matchfreq2 = nmatches2 / (double) sample2->nrows;
	unmatchfreq2 = nunmatches2 / (double) sample2->nrows;
	freq2 = 1.0;

	/*
	 * correction for sample parts eliminated by the conditions
	 *
	 * FIXME for "impossible" conditions this does /0, which results in
	 * NaN and other weird stuff.
	 */
	if ((matchfreq1 + unmatchfreq1) > 0 && (matchfreq2 + unmatchfreq2) > 0)
		s = s * freq1 * freq2 / (matchfreq1 + unmatchfreq1) / (matchfreq2 + unmatchfreq2);
	else
		s = 0.0;

	elog(WARNING, "statext_compare_samples corrected %f", s);

	sample_free(sample1);
	sample_free(sample2);

	return s;
}

/*
 * statext_is_supported_join_clause
 *		Check if a join clause may be estimated using extended stats.
 *
 * Determines if this is a join clause of the form (Expr op Expr) which
 * may be estimated using extended statistics. Each side must reference
 * just one relation for now.
 */
static bool
statext_is_supported_join_clause(PlannerInfo *root, Node *clause,
								 int varRelid, SpecialJoinInfo *sjinfo)
{
	Oid	oprsel;
	RestrictInfo   *rinfo;
	OpExpr		   *opclause;
	ListCell	   *lc;

	/*
	 * evaluation as a restriction clause, either at scan node or forced
	 *
	 * XXX See treat_as_join_clause.
	 */
	if ((varRelid != 0) || (sjinfo == NULL))
		return false;

	/* XXX Can we rely on always getting RestrictInfo here? */
	if (!IsA(clause, RestrictInfo))
		return false;

	/* strip the RestrictInfo */
	rinfo = (RestrictInfo *) clause;
	clause = (Node *) rinfo->clause;

	/* is it referencing multiple relations? */
	if (bms_membership(rinfo->clause_relids) != BMS_MULTIPLE)
		return false;

	/* we only support simple operator clauses for now */
	if (!is_opclause(clause))
		return false;

	opclause = (OpExpr *) clause;

	/* for now we only support estimating equijoins */
	oprsel = get_oprjoin(opclause->opno);

	if (oprsel != F_EQJOINSEL)
		return false;

	/*
	 * Make sure we're not mixing vars from multiple relations on the same
	 * side, like
	 *
	 *   (t1.a + t2.a) = (t1.b + t2.b)
	 *
	 * which is still technically an opclause, but we can't match it to
	 * extended statistics in a simple way.
	 *
	 * XXX This also means we require rinfo->clause_relids to have 2 rels.
	 *
	 * XXX Also check it's not expression on system attributes, which we
	 * don't allow in extended statistics.
	 *
	 * XXX Although maybe we could allow cases that combine expressions
	 * from both relations on either side? Like (t1.a + t2.b = t1.c - t2.d)
	 * or something like that. We could do "cartesian product" of the MCV
	 * stats and restrict it using this condition.
	 */
	foreach (lc, opclause->args)
	{
		Bitmapset *varnos = NULL;
		Node *expr = (Node *) lfirst(lc);

		varnos = pull_varnos(root, expr);

		/*
		 * No argument should reference more than just one relation.
		 *
		 * This effectively means each side references just two relations.
		 * If there's no relation on one side, it's a Const, and the other
		 * side has to be either Const or Expr with a single rel. In which
		 * case it can't be a join clause.
		 */
		if (bms_num_members(varnos) > 1)
			return false;

		/*
		 * XXX Maybe check that both relations have extended statistics
		 * (no point in considering the clause as useful without it). But
		 * we'll do that check later anyway, so keep this cheap.
		 */
	}

	return true;
}

/*
 * statext_try_join_estimates
 *		Checks if it's worth considering extended stats on join estimates.
 *
 * This is supposed to be a quick/cheap check to decide whether to expend
 * more effort on applying extended statistics to join clauses.
 */
bool
statext_try_join_estimates(PlannerInfo *root, List *clauses, int varRelid,
						   JoinType jointype, SpecialJoinInfo *sjinfo,
						   Bitmapset *estimatedclauses)
{
	int			listidx;
	int			k;
	ListCell   *lc;
	Bitmapset  *relids = NULL;

	/*
	 * XXX Not having these values means treat_as_join_clause returns false,
	 * so we're not supposed to handle join clauses here. So just bail out.
	 */
	if ((varRelid != 0) || (sjinfo == NULL))
		return false;

	listidx = -1;
	foreach (lc, clauses)
	{
		Node *clause = (Node *) lfirst(lc);
		RestrictInfo *rinfo;
		listidx++;

		/* skip estimated clauses */
		if (bms_is_member(listidx, estimatedclauses))
			continue;

		/*
		 * Skip clauses that are not join clauses or that we don't know
		 * how to handle estimate using extended statistics.
		 */
		if (!statext_is_supported_join_clause(root, clause, varRelid, sjinfo))
			continue;

		/*
		 * Collect relids from all usable clauses.
		 *
		 * XXX We're guaranteed to have RestrictInfo thanks to the checks
		 * in statext_is_supported_join_clause.
		 */
		rinfo = (RestrictInfo *) clause;
		relids = bms_union(relids, rinfo->clause_relids);
	}

	/* no join clauses found, don't try applying extended stats */
	if (bms_num_members(relids) == 0)
		return false;

	/*
	 * We expect either 0 or >= 2 relids, a case with 1 relid in join clauses
	 * should be impossible. And we just ruled out 0, so there are at least 2.
	 */
	Assert(bms_num_members(relids) >= 2);

	/*
	 * Check that at least some of the rels referenced by the clauses have
	 * extended stats.
	 *
	 * XXX Maybe we should check how many rels have stats, and cross-check
	 * how compatible they are (e.g. that both have MCVs, etc.). Also,
	 * maybe this should cross-check the exact pairs of rels with a join
	 * clause between them? OTOH this is supposed to be a cheap check, so
	 * maybe better leave that for later.
	 *
	 * XXX We could also check if there are enough parameters in each rel
	 * to consider extended stats. If there's just a single attribute, it's
	 * probably better to use just regular statistics. OTOH we can also
	 * consider restriction clauses from baserestrictinfo and use them
	 * to calculate conditional probabilities.
	 */
	k = -1;
	while ((k = bms_next_member(relids, k)) >= 0)
	{
		RelOptInfo *rel = find_base_rel(root, k);
		if (rel->statlist)
			return true;
	}

	return false;
}

/*
 * Information about a join between two relations. It tracks relations being
 * joined and the join clauses.
 */
typedef struct JoinPairInfo
{
	Bitmapset  *rels;
	List	   *clauses;
} JoinPairInfo;

/*
 * statext_build_join_pairs
 *		Extract pairs of joined rels with join clauses for each pair.
 *
 * Walks the remaining (not yet estimated) clauses, and splits them into
 * lists for each pair of joined relations. Returns NULL if there are no
 * suitable join pairs that might be estimated using extended stats.
 *
 * XXX It's possible there are join clauses, but the clauses are not
 * supported by the extended stats machinery (we only support opclauses
 * with F_EQJOINSEL selectivity function at the moment). But for samples
 * it should be possible to support almost anything, although maybe not
 * with the efficient correlated sampling.
 *
 * XXX This should also order the join pairs in a way that supports the
 * correlated sampling, so for example in a snowflake join
 *
 * F -> D1 -> D2
 *
 * we should join F->D1 first, then D1->D2. This way we can enrich the
 * first sample. For star schema this probably does not matter too much.
 */
static JoinPairInfo *
statext_build_join_pairs(PlannerInfo *root, List *clauses, int varRelid,
						 JoinType jointype, SpecialJoinInfo *sjinfo,
						 Bitmapset *estimatedclauses, int *npairs)
{
	int				cnt;
	int				listidx;
	JoinPairInfo   *info;
	ListCell	   *lc;

	/*
	 * Assume each clause is for a different pair of relations (some of them
	 * might be already estimated, but meh - there shouldn't be too many of
	 * them and it's cheaper than repalloc.
	 */
	info = (JoinPairInfo *) palloc0(sizeof(JoinPairInfo) * list_length(clauses));
	cnt = 0;

	listidx = -1;
	foreach(lc, clauses)
	{
		int				i;
		bool			found;
		Node		   *clause = (Node *) lfirst(lc);
		RestrictInfo   *rinfo;

		listidx++;

		/* skip already estimated clauses */
		if (bms_is_member(listidx, estimatedclauses))
			continue;

		/*
		 * Make sure the clause is a join clause of a supported shape (at
		 * the moment we support just (Expr op Expr) clauses with each
		 * side referencing just a single relation.
		 */
		if (!statext_is_supported_join_clause(root, clause, varRelid, sjinfo))
			continue;

		/* statext_is_supported_join_clause guarantees RestrictInfo */
		rinfo = (RestrictInfo *) clause;
		clause = (Node *) rinfo->clause;

		/* search for a matching join pair */
		found = false;
		for (i = 0; i < cnt; i++)
		{
			if (bms_is_subset(rinfo->clause_relids, info[i].rels))
			{
				info[i].clauses = lappend(info[i].clauses, clause);
				found = true;
				break;
			}
		}

		if (!found)
		{
			info[cnt].rels = rinfo->clause_relids;
			info[cnt].clauses = lappend(info[cnt].clauses, clause);
			cnt++;
		}
	}

	if (cnt == 0)
		return NULL;

	*npairs = cnt;
	return info;
}

/*
 * extract_relation_info
 *		Extract information about a relation in a join pair.
 *
 * The relation is identified by index (generally 0 or 1), and picks extended
 * statistics covering matching the join clauses and baserel restrictions.
 *
 * XXX Can we have cases with indexes above 1? Probably for clauses mixing
 * vars from 3 relations, but we're rejecting those.
 */
static RelOptInfo *
extract_relation_info(PlannerInfo *root, JoinPairInfo *info, int index,
					  StatisticExtInfo **stat)
{
	int	k;
	int	relid;
	RelOptInfo *rel;
	ListCell *lc;

	Bitmapset  *attnums = NULL;
	List	   *exprs = NIL;

	k = -1;
	while (index >= 0)
	{
		k = bms_next_member(info->rels, k);
		if (k < 0)
			elog(ERROR, "failed to extract relid");

		relid = k;
		index--;
	}

	rel = find_base_rel(root, relid);

	/*
	 * Walk the clauses for this join pair, and extract expressions about
	 * the relation identified by index / relid. For simple Vars we extract
	 * the attnum. Otherwise we keep the whole expression.
	 */
	foreach (lc, info->clauses)
	{
		ListCell *lc2;
		Node *clause = (Node *) lfirst(lc);
		OpExpr *opclause = (OpExpr *) clause;

		/* only opclauses supported for now */
		Assert(is_opclause(clause));

		foreach (lc2, opclause->args)
		{
			Node *arg = (Node *) lfirst(lc2);
			Bitmapset *varnos = NULL;

			/* plain Var references (boolean Vars or recursive checks) */
			if (IsA(arg, Var))
			{
				Var		   *var = (Var *) arg;

				/* Ignore vars from other relations. */
				if (var->varno != relid)
					continue;

				/* we also better ensure the Var is from the current level */
				if (var->varlevelsup > 0)
					continue;

				/* Also skip system attributes (we don't allow stats on those). */
				if (!AttrNumberIsForUserDefinedAttr(var->varattno))
					elog(ERROR, "unexpected system attribute");

				attnums = bms_add_member(attnums, var->varattno);

				/* Done, process the next argument. */
				continue;
			}

			/*
			 * OK, it's a more complex expression, so check if it matches
			 * the relid and maybe keep it as a whole. It should be
			 * compatible because we already checked it when building the
			 * join pairs.
			 */
			varnos = pull_varnos(root, arg);

			if (relid == bms_singleton_member(varnos))
				exprs = lappend(exprs, arg);
		}
	}

	*stat = find_matching_sample(root, rel, attnums, exprs);

	return rel;
}

/*
 * statext_clauselist_join_selectivity
 *		Use extended stats to estimate join clauses.
 *
 * XXX In principle, we should not restrict this to cases with multiple
 * join clauses - we should consider dependencies with conditions at the
 * base relations, i.e. calculate P(join clause | base restrictions).
 * But currently that does not happen, because clauselist_selectivity_ext
 * treats a single clause as a special case (and we don't apply extended
 * statistics in that case yet).
 */
Selectivity
statext_clauselist_join_selectivity(PlannerInfo *root, List *clauses, int varRelid,
									JoinType jointype, SpecialJoinInfo *sjinfo,
									Bitmapset **estimatedclauses)
{
	int			i;
	int			listidx;
	Selectivity	s = 1.0;

	JoinPairInfo *info;
	int				ninfo;

	if (!clauses || !enable_sample_estimates_join)
		return 1.0;

	/* extract pairs of joined relations from the list of clauses */
	info = statext_build_join_pairs(root, clauses, varRelid, jointype, sjinfo,
									*estimatedclauses, &ninfo);

	/* no useful join pairs */
	if (!info)
		return 1.0;

	/*
	 * Process the join pairs, try to find a matching MCV on each side.
	 *
	 * XXX The basic principle is quite similar to eqjoinsel_inner, i.e.
	 * we try to find a MCV on both sides of the join, and use it to get
	 * better join estimate. It's a bit more complicated, because there
	 * might be multiple MCV lists, we also need ndistinct estimate, and
	 * there may be interesting baserestrictions too.
	 *
	 * XXX At the moment we only handle the case with matching MCVs on
	 * both sides, but it'd be good to also handle case with just ndistinct
	 * statistics improving ndistinct estimates.
	 *
	 * XXX Perhaps it'd be good to also handle case with one side only
	 * having "regular" statistics (e.g. MCV), especially in cases with
	 * no conditions on that side of the join (where we can't use the
	 * extended MCV to calculate conditional probability).
	 */
	for (i = 0; i < ninfo; i++)
	{
		RelOptInfo *rel1;
		RelOptInfo *rel2;
		StatisticExtInfo *stat1;
		StatisticExtInfo *stat2;

		ListCell *lc;

		/* extract info about the first relation */
		rel1 = extract_relation_info(root, &info[i], 0, &stat1);

		/* extract info about the second relation */
		rel2 = extract_relation_info(root, &info[i], 1, &stat2);

		/* XXX only handling case with MCV on both sides for now */
		if (!stat1 || !stat2)
			continue;

		s *= statext_compare_samples(root, rel1, rel2, stat1, stat2, info[i].clauses);

		/*
		 * Now mark all the clauses for this join pair as estimated.
		 *
		 * XXX Maybe track the indexes in JoinPairInfo, so that we can
		 * simply union the two bitmaps, without the extra matching.
		 */
		foreach (lc, info->clauses)
		{
			Node *clause = (Node *) lfirst(lc);
			ListCell *lc2;

			listidx = -1;
			foreach (lc2, clauses)
			{
				Node *clause2 = (Node *) lfirst(lc2);
				listidx++;

				Assert(IsA(clause2, RestrictInfo));

				clause2 = (Node *) ((RestrictInfo *) clause2)->clause;

				if (equal(clause, clause2))
				{
					*estimatedclauses = bms_add_member(*estimatedclauses, listidx);
					break;
				}
			}
		}
	}

	return s;
}
