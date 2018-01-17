/*-------------------------------------------------------------------------
 *
 * pg_opfamily.h
 *	  definition of the system "opfamily" relation (pg_opfamily)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_opfamily.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_OPFAMILY_H
#define PG_OPFAMILY_H

#include "catalog/genbki.h"
#include "catalog/oid_symbols.h"

/* ----------------
 *		pg_opfamily definition. cpp turns this into
 *		typedef struct FormData_pg_opfamily
 * ----------------
 */
#define OperatorFamilyRelationId  2753

CATALOG(pg_opfamily,2753)
{
	regam		opfmethod;		/* index access method opfamily is for */
	NameData	opfname;		/* name of this opfamily */

	/* namespace of this opfamily */
	Oid			opfnamespace BKI_DEFAULT(PGNSP);

	/* opfamily owner */
	Oid			opfowner BKI_DEFAULT(PGUID);
} FormData_pg_opfamily;

DECLARE_UNIQUE_INDEX(pg_opfamily_am_name_nsp_index, 2754, on pg_opfamily using btree(opfmethod oid_ops, opfname name_ops, opfnamespace oid_ops));
#define OpfamilyAmNameNspIndexId  2754
DECLARE_UNIQUE_INDEX(pg_opfamily_oid_index, 2755, on pg_opfamily using btree(oid oid_ops));
#define OpfamilyOidIndexId	2755

/* ----------------
 *		Form_pg_opfamily corresponds to a pointer to a tuple with
 *		the format of pg_opfamily relation.
 * ----------------
 */
typedef FormData_pg_opfamily *Form_pg_opfamily;

/* ----------------
 *		compiler constants for pg_opfamily
 * ----------------
 */
#define Natts_pg_opfamily				4
#define Anum_pg_opfamily_opfmethod		1
#define Anum_pg_opfamily_opfname		2
#define Anum_pg_opfamily_opfnamespace	3
#define Anum_pg_opfamily_opfowner		4

#endif							/* PG_OPFAMILY_H */
