/*-------------------------------------------------------------------------
 *
 * pg_extension.h
 *	  definition of the system "extension" relation (pg_extension)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_extension.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_EXTENSION_H
#define PG_EXTENSION_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_extension definition.  cpp turns this into
 *		typedef struct FormData_pg_extension
 * ----------------
 */
#define ExtensionRelationId 3079

CATALOG(pg_extension,3079)
{
	NameData	extname;		/* extension name */
	Oid			extowner;		/* extension owner */
	Oid			extnamespace;	/* namespace of contained objects */
	bool		extrelocatable; /* if true, allow ALTER EXTENSION SET SCHEMA */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	/* extversion may never be null, but the others can be. */
	text		extversion BKI_FORCE_NOT_NULL;	/* extension version name */
	Oid			extconfig[1];	/* dumpable configuration tables */
	text		extcondition[1];	/* WHERE clauses for config tables */
#endif
} FormData_pg_extension;

DECLARE_UNIQUE_INDEX(pg_extension_oid_index, 3080, on pg_extension using btree(oid oid_ops));
#define ExtensionOidIndexId 3080
DECLARE_UNIQUE_INDEX(pg_extension_name_index, 3081, on pg_extension using btree(extname name_ops));
#define ExtensionNameIndexId 3081

/* ----------------
 *		Form_pg_extension corresponds to a pointer to a tuple with
 *		the format of pg_extension relation.
 * ----------------
 */
typedef FormData_pg_extension *Form_pg_extension;

/* ----------------
 *		compiler constants for pg_extension
 * ----------------
 */

#define Natts_pg_extension					7
#define Anum_pg_extension_extname			1
#define Anum_pg_extension_extowner			2
#define Anum_pg_extension_extnamespace		3
#define Anum_pg_extension_extrelocatable	4
#define Anum_pg_extension_extversion		5
#define Anum_pg_extension_extconfig			6
#define Anum_pg_extension_extcondition		7

#endif							/* PG_EXTENSION_H */
