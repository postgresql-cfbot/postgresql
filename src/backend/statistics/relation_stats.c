/*-------------------------------------------------------------------------
 * relation_stats.c
 *
 *	  PostgreSQL relation statistics manipulation
 *
 * Code supporting the direct import of relation statistics, similar to
 * what is done by the ANALYZE command.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *       src/backend/statistics/relation_stats.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_temp_class.h"
#include "nodes/makefuncs.h"
#include "statistics/stat_utils.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


/*
 * Positional argument numbers, names, and types for
 * relation_statistics_update().
 */

enum relation_stats_argnum
{
	RELSCHEMA_ARG = 0,
	RELNAME_ARG,
	RELPAGES_ARG,
	RELTUPLES_ARG,
	RELALLVISIBLE_ARG,
	RELALLFROZEN_ARG,
	NUM_RELATION_STATS_ARGS
};

static struct StatsArgInfo relarginfo[] =
{
	[RELSCHEMA_ARG] = {"schemaname", TEXTOID},
	[RELNAME_ARG] = {"relname", TEXTOID},
	[RELPAGES_ARG] = {"relpages", INT4OID},
	[RELTUPLES_ARG] = {"reltuples", FLOAT4OID},
	[RELALLVISIBLE_ARG] = {"relallvisible", INT4OID},
	[RELALLFROZEN_ARG] = {"relallfrozen", INT4OID},
	[NUM_RELATION_STATS_ARGS] = {0}
};

static bool relation_statistics_update(FunctionCallInfo fcinfo);

/*
 * Internal function for modifying statistics for a relation.
 */
static bool
relation_statistics_update(FunctionCallInfo fcinfo)
{
	bool		result = true;
	char	   *nspname;
	char	   *relname;
	Oid			reloid;
	Relation	crel;
	BlockNumber relpages = 0;
	bool		update_relpages = false;
	float		reltuples = 0;
	bool		update_reltuples = false;
	BlockNumber relallvisible = 0;
	bool		update_relallvisible = false;
	BlockNumber relallfrozen = 0;
	bool		update_relallfrozen = false;
	HeapTuple	ctup;
	Form_pg_class pgcform;
	Relation	rel;
	HeapTuple	temp_ctup;
	Form_pg_temp_class temp_pgcform;
	bool		dirty;
	bool		temp_dirty;
	Oid			locked_table = InvalidOid;

	stats_check_required_arg(fcinfo, relarginfo, RELSCHEMA_ARG);
	stats_check_required_arg(fcinfo, relarginfo, RELNAME_ARG);

	nspname = TextDatumGetCString(PG_GETARG_DATUM(RELSCHEMA_ARG));
	relname = TextDatumGetCString(PG_GETARG_DATUM(RELNAME_ARG));

	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is in progress"),
				 errhint("Statistics cannot be modified during recovery.")));

	reloid = RangeVarGetRelidExtended(makeRangeVar(nspname, relname, -1),
									  ShareUpdateExclusiveLock, 0,
									  RangeVarCallbackForStats, &locked_table);

	if (!PG_ARGISNULL(RELPAGES_ARG))
	{
		relpages = PG_GETARG_UINT32(RELPAGES_ARG);
		update_relpages = true;
	}

	if (!PG_ARGISNULL(RELTUPLES_ARG))
	{
		reltuples = PG_GETARG_FLOAT4(RELTUPLES_ARG);
		if (reltuples < -1.0)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("argument \"%s\" must not be less than -1.0", "reltuples")));
			result = false;
		}
		else
			update_reltuples = true;
	}

	if (!PG_ARGISNULL(RELALLVISIBLE_ARG))
	{
		relallvisible = PG_GETARG_UINT32(RELALLVISIBLE_ARG);
		update_relallvisible = true;
	}

	if (!PG_ARGISNULL(RELALLFROZEN_ARG))
	{
		relallfrozen = PG_GETARG_UINT32(RELALLFROZEN_ARG);
		update_relallfrozen = true;
	}

	/*
	 * Take RowExclusiveLock on pg_class, consistent with
	 * vac_update_relstats().
	 */
	crel = table_open(RelationRelationId, RowExclusiveLock);

	ctup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(reloid));
	if (!HeapTupleIsValid(ctup))
		elog(ERROR, "pg_class entry for relid %u not found", reloid);

	pgcform = (Form_pg_class) GETSTRUCT(ctup);

	/*
	 * For a global temporary table, need to update the pg_temp_class tuple
	 * instead.  Force it into existence by opening the relation.
	 */
	if (pgcform->relpersistence == RELPERSISTENCE_GLOBAL_TEMP)
	{
		rel = relation_open(reloid, AccessShareLock);
		relation_close(rel, AccessShareLock);

		temp_ctup = GetPgTempClassTuple(reloid);
		if (!HeapTupleIsValid(temp_ctup))
			elog(ERROR, "pg_temp_class entry for relid %u not found", reloid);

		temp_pgcform = (Form_pg_temp_class) GETSTRUCT(temp_ctup);
	}
	else
	{
		temp_ctup = NULL;
		temp_pgcform = NULL;
	}

	dirty = false;
	temp_dirty = false;

	if (update_relpages)
		SetEffective_relpages(pgcform, temp_pgcform, (int32) relpages,
							  &dirty, &temp_dirty);

	if (update_reltuples)
		SetEffective_reltuples(pgcform, temp_pgcform, (float4) reltuples,
							   &dirty, &temp_dirty);

	if (update_relallvisible)
		SetEffective_relallvisible(pgcform, temp_pgcform, (int32) relallvisible,
								   &dirty, &temp_dirty);

	if (update_relallfrozen)
		SetEffective_relallfrozen(pgcform, temp_pgcform, (int32) relallfrozen,
								  &dirty, &temp_dirty);

	if (dirty)
		CatalogTupleUpdate(crel, &ctup->t_self, ctup);

	if (HeapTupleIsValid(temp_ctup))
	{
		if (temp_dirty)
			UpdatePgTempClassTuple(reloid, temp_ctup);

		heap_freetuple(temp_ctup);
	}

	heap_freetuple(ctup);

	/* release the lock, consistent with vac_update_relstats() */
	table_close(crel, RowExclusiveLock);

	CommandCounterIncrement();

	return result;
}

/*
 * Clear statistics for a given pg_class entry; that is, set back to initial
 * stats for a newly-created table.
 */
Datum
pg_clear_relation_stats(PG_FUNCTION_ARGS)
{
	LOCAL_FCINFO(newfcinfo, 6);

	InitFunctionCallInfoData(*newfcinfo, NULL, 6, InvalidOid, NULL, NULL);

	newfcinfo->args[0].value = PG_GETARG_DATUM(0);
	newfcinfo->args[0].isnull = PG_ARGISNULL(0);
	newfcinfo->args[1].value = PG_GETARG_DATUM(1);
	newfcinfo->args[1].isnull = PG_ARGISNULL(1);
	newfcinfo->args[2].value = UInt32GetDatum(0);
	newfcinfo->args[2].isnull = false;
	newfcinfo->args[3].value = Float4GetDatum(-1.0);
	newfcinfo->args[3].isnull = false;
	newfcinfo->args[4].value = UInt32GetDatum(0);
	newfcinfo->args[4].isnull = false;
	newfcinfo->args[5].value = UInt32GetDatum(0);
	newfcinfo->args[5].isnull = false;

	relation_statistics_update(newfcinfo);
	PG_RETURN_VOID();
}

Datum
pg_restore_relation_stats(PG_FUNCTION_ARGS)
{
	LOCAL_FCINFO(positional_fcinfo, NUM_RELATION_STATS_ARGS);
	bool		result = true;

	InitFunctionCallInfoData(*positional_fcinfo, NULL,
							 NUM_RELATION_STATS_ARGS,
							 InvalidOid, NULL, NULL);

	if (!stats_fill_fcinfo_from_arg_pairs(fcinfo, positional_fcinfo,
										  relarginfo))
		result = false;

	if (!relation_statistics_update(positional_fcinfo))
		result = false;

	PG_RETURN_BOOL(result);
}
