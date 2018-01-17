/*-------------------------------------------------------------------------
 *
 * pg_range.h
 *	  definition of the system "range" relation (pg_range)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_range.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_RANGE_H
#define PG_RANGE_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_range definition.  cpp turns this into
 *		typedef struct FormData_pg_range
 * ----------------
 */
#define RangeRelationId 3541

CATALOG(pg_range,3541) BKI_WITHOUT_OIDS
{
	regtype		rngtypid;		/* OID of owning range type */
	regtype		rngsubtype;		/* OID of range's element type (subtype) */
	Oid			rngcollation;	/* collation for this range type, or 0 */
	Oid			rngsubopc;		/* subtype's btree opclass */
	regproc		rngcanonical;	/* canonicalize range, or 0 */
	regproc		rngsubdiff;		/* subtype difference as a float8, or 0 */
} FormData_pg_range;

DECLARE_UNIQUE_INDEX(pg_range_rngtypid_index, 3542, on pg_range using btree(rngtypid oid_ops));
#define RangeTypidIndexId					3542

/* ----------------
 *		Form_pg_range corresponds to a pointer to a tuple with
 *		the format of pg_range relation.
 * ----------------
 */
typedef FormData_pg_range *Form_pg_range;

/* ----------------
 *		compiler constants for pg_range
 * ----------------
 */
#define Natts_pg_range					6
#define Anum_pg_range_rngtypid			1
#define Anum_pg_range_rngsubtype		2
#define Anum_pg_range_rngcollation		3
#define Anum_pg_range_rngsubopc			4
#define Anum_pg_range_rngcanonical		5
#define Anum_pg_range_rngsubdiff		6

/*
 * prototypes for functions in pg_range.c
 */

extern void RangeCreate(Oid rangeTypeOid, Oid rangeSubType, Oid rangeCollation,
			Oid rangeSubOpclass, RegProcedure rangeCanonical,
			RegProcedure rangeSubDiff);
extern void RangeDelete(Oid rangeTypeOid);

#endif							/* PG_RANGE_H */
