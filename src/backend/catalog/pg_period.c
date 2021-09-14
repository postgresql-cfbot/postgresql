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
	ScanKeyData skey[1];
	Oid			perOid = InvalidOid;

	/*
	 * Fetch the period tuple from pg_period.  There may be more than
	 * one match, because periods are not required to have unique names;
	 * if so, error out.
	 */
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

		if (strcmp(NameStr(period->pername), pername) == 0)
		{
			if (OidIsValid(perOid))
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_OBJECT),
						 errmsg("table \"%s\" has multiple periods named \"%s\"",
								get_rel_name(relid), pername)));
			perOid = period->oid;
		}
	}

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
