/*-------------------------------------------------------------------------
 *
 * pg_period.c
 *	  routines to support manipulation of the pg_period relation
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_period.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/pg_period.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"


/*
 * Delete a single period record.
 */
void
RemovePeriodById(Oid periodId)
{
	Relation	pg_period;
	HeapTuple	tup;

	pg_period = table_open(PeriodRelationId, RowExclusiveLock);

	tup = SearchSysCache1(PERIODOID, ObjectIdGetDatum(periodId));
	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for period %u", periodId);

	/* Fry the period itself */
	CatalogTupleDelete(pg_period, &tup->t_self);

	/* Clean up */
	ReleaseSysCache(tup);
	table_close(pg_period, RowExclusiveLock);
}

/*
 * get_relation_period_oid
 *		Find a period on the specified relation with the specified name.
 *		Returns period's OID.
 */
Oid
get_relation_period_oid(Oid relid, const char *pername, bool missing_ok)
{
	Relation	pg_period;
	HeapTuple	tuple;
	SysScanDesc scan;
	ScanKeyData skey[2];
	Oid			perOid = InvalidOid;

	/* Fetch the period tuple from pg_period. */
	pg_period = table_open(PeriodRelationId, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_pg_period_perrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	ScanKeyInit(&skey[1],
				Anum_pg_period_pername,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(pername));

	scan = systable_beginscan(pg_period, PeriodRelidNameIndexId, true,
							  NULL, 2, skey);

	/* There can be at most one matching row */
	if (HeapTupleIsValid(tuple = systable_getnext(scan)))
		perOid = ((Form_pg_period) GETSTRUCT(tuple))->oid;

	systable_endscan(scan);

	/* If no such period exists, complain */
	if (!OidIsValid(perOid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("period \"%s\" for table \"%s\" does not exist",
						pername, get_rel_name(relid))));

	table_close(pg_period, AccessShareLock);

	return perOid;
}

/*
 * get_period_attnos
 *		Get the attno of the GENERATED rangetype column
 *		for all PERIODs in this table.
 */
extern Bitmapset
*get_period_attnos(Oid relid)
{
	Bitmapset *attnos = NULL;
	Relation	pg_period;
	HeapTuple	tuple;
	SysScanDesc scan;
	ScanKeyData skey[1];

	pg_period = table_open(PeriodRelationId, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_pg_period_perrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(pg_period, PeriodRelidNameIndexId, true,
							  NULL, 1, skey);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_period period = (Form_pg_period) GETSTRUCT(tuple);

		attnos = bms_add_member(attnos, period->perrange);
	}

	systable_endscan(scan);
	table_close(pg_period, AccessShareLock);

	return attnos;
}
