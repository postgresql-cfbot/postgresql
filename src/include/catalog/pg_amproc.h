/*-------------------------------------------------------------------------
 *
 * pg_amproc.h
 *	  definition of the system "amproc" relation (pg_amproc)
 *	  along with the relation's initial contents.
 *
 * The amproc table identifies support procedures associated with index
 * operator families and classes.  These procedures can't be listed in pg_amop
 * since they are not the implementation of any indexable operator.
 *
 * The primary key for this table is <amprocfamily, amproclefttype,
 * amprocrighttype, amprocnum>.  The "default" support functions for a
 * particular opclass within the family are those with amproclefttype =
 * amprocrighttype = opclass's opcintype.  These are the ones loaded into the
 * relcache for an index and typically used for internal index operations.
 * Other support functions are typically used to handle cross-type indexable
 * operators with oprleft/oprright matching the entry's amproclefttype and
 * amprocrighttype. The exact behavior depends on the index AM, however, and
 * some don't pay attention to non-default functions at all.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_amproc.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_AMPROC_H
#define PG_AMPROC_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_amproc definition.  cpp turns this into
 *		typedef struct FormData_pg_amproc
 * ----------------
 */
#define AccessMethodProcedureRelationId  2603

CATALOG(pg_amproc,2603)
{
	/* the index opfamily this entry is for */
	regopf		amprocfamily BKI_ABBREV(opf);

	/* procedure's left input data type */
	regtype		amproclefttype BKI_ABBREV(lt);

	/* procedure's right input data type */
	regtype		amprocrighttype BKI_ABBREV(rt);

	/* support procedure index */
	int16		amprocnum BKI_ABBREV(num);

	/* OID of the proc */
	regproc		amproc;
} FormData_pg_amproc;

DECLARE_UNIQUE_INDEX(pg_amproc_fam_proc_index, 2655, on pg_amproc using btree(amprocfamily oid_ops, amproclefttype oid_ops, amprocrighttype oid_ops, amprocnum int2_ops));
#define AccessMethodProcedureIndexId  2655
DECLARE_UNIQUE_INDEX(pg_amproc_oid_index, 2757, on pg_amproc using btree(oid oid_ops));
#define AccessMethodProcedureOidIndexId  2757

/* ----------------
 *		Form_pg_amproc corresponds to a pointer to a tuple with
 *		the format of pg_amproc relation.
 * ----------------
 */
typedef FormData_pg_amproc *Form_pg_amproc;

/* ----------------
 *		compiler constants for pg_amproc
 * ----------------
 */
#define Natts_pg_amproc					5
#define Anum_pg_amproc_amprocfamily		1
#define Anum_pg_amproc_amproclefttype	2
#define Anum_pg_amproc_amprocrighttype	3
#define Anum_pg_amproc_amprocnum		4
#define Anum_pg_amproc_amproc			5

#endif							/* PG_AMPROC_H */
