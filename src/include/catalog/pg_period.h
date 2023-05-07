/*-------------------------------------------------------------------------
 *
 * pg_period.h
 *	  definition of the "period" system catalog (pg_period)
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * src/include/catalog/pg_period.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PERIOD_H
#define PG_PERIOD_H

#include "catalog/genbki.h"
#include "catalog/pg_period_d.h"

/* ----------------
 *		pg_period definition.  cpp turns this into
 *		typedef struct FormData_pg_period
 * ----------------
 */
CATALOG(pg_period,8000,PeriodRelationId)
{
	Oid			oid;			/* OID of the period */
	NameData	pername;		/* name of period */
	Oid			perrelid;		/* OID of relation containing this period */
	int16		perstart;		/* column for start value */
	int16		perend;			/* column for end value */
	Oid			perrngtype;		/* OID of the range type for this period */
	Oid			perconstraint;	/* OID of (start < end) constraint */
} FormData_pg_period;

/* ----------------
 *		Form_pg_period corresponds to a pointer to a tuple with
 *		the format of pg_period relation.
 * ----------------
 */
typedef FormData_pg_period *Form_pg_period;

DECLARE_UNIQUE_INDEX_PKEY(pg_period_oid_index, 8001, PeriodObjectIndexId, on pg_period using btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_period_perrelid_pername_index, 8002, PeriodRelidNameIndexId, on pg_period using btree(perrelid oid_ops, pername name_ops));

extern void RemovePeriodById(Oid periodId);

extern Oid get_relation_period_oid(Oid relid, const char *pername, bool missing_ok);

#endif							/* PG_PERIOD_H */
