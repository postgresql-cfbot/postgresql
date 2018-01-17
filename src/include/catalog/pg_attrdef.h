/*-------------------------------------------------------------------------
 *
 * pg_attrdef.h
 *	  definition of the system "attribute defaults" relation (pg_attrdef)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_attrdef.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ATTRDEF_H
#define PG_ATTRDEF_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_attrdef definition.  cpp turns this into
 *		typedef struct FormData_pg_attrdef
 * ----------------
 */
#define AttrDefaultRelationId  2604

CATALOG(pg_attrdef,2604)
{
	Oid			adrelid;		/* OID of table containing attribute */
	int16		adnum;			/* attnum of attribute */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	pg_node_tree adbin;			/* nodeToString representation of default */
	text		adsrc;			/* human-readable representation of default */
#endif
} FormData_pg_attrdef;

DECLARE_TOAST(pg_attrdef, 2830, 2831);
DECLARE_UNIQUE_INDEX(pg_attrdef_adrelid_adnum_index, 2656, on pg_attrdef using btree(adrelid oid_ops, adnum int2_ops));
#define AttrDefaultIndexId	2656
DECLARE_UNIQUE_INDEX(pg_attrdef_oid_index, 2657, on pg_attrdef using btree(oid oid_ops));
#define AttrDefaultOidIndexId  2657


/* ----------------
 *		Form_pg_attrdef corresponds to a pointer to a tuple with
 *		the format of pg_attrdef relation.
 * ----------------
 */
typedef FormData_pg_attrdef *Form_pg_attrdef;

/* ----------------
 *		compiler constants for pg_attrdef
 * ----------------
 */
#define Natts_pg_attrdef				4
#define Anum_pg_attrdef_adrelid			1
#define Anum_pg_attrdef_adnum			2
#define Anum_pg_attrdef_adbin			3
#define Anum_pg_attrdef_adsrc			4

#endif							/* PG_ATTRDEF_H */
