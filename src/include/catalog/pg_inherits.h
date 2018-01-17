/*-------------------------------------------------------------------------
 *
 * pg_inherits.h
 *	  definition of the system "inherits" relation (pg_inherits)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_inherits.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_INHERITS_H
#define PG_INHERITS_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_inherits definition.  cpp turns this into
 *		typedef struct FormData_pg_inherits
 * ----------------
 */
#define InheritsRelationId	2611

CATALOG(pg_inherits,2611) BKI_WITHOUT_OIDS
{
	Oid			inhrelid;
	Oid			inhparent;
	int32		inhseqno;
} FormData_pg_inherits;

DECLARE_UNIQUE_INDEX(pg_inherits_relid_seqno_index, 2680, on pg_inherits using btree(inhrelid oid_ops, inhseqno int4_ops));
#define InheritsRelidSeqnoIndexId  2680
DECLARE_INDEX(pg_inherits_parent_index, 2187, on pg_inherits using btree(inhparent oid_ops));
#define InheritsParentIndexId  2187

/* ----------------
 *		Form_pg_inherits corresponds to a pointer to a tuple with
 *		the format of pg_inherits relation.
 * ----------------
 */
typedef FormData_pg_inherits *Form_pg_inherits;

/* ----------------
 *		compiler constants for pg_inherits
 * ----------------
 */
#define Natts_pg_inherits				3
#define Anum_pg_inherits_inhrelid		1
#define Anum_pg_inherits_inhparent		2
#define Anum_pg_inherits_inhseqno		3

#endif							/* PG_INHERITS_H */
