/*-------------------------------------------------------------------------
 *
 * pg_shdescription.h
 *	  definition of the system "shared description" relation
 *	  (pg_shdescription)
 *
 * NOTE: an object is identified by the OID of the row that primarily
 * defines the object, plus the OID of the table that that row appears in.
 * For example, a database is identified by the OID of its pg_database row
 * plus the pg_class OID of table pg_database.  This allows unique
 * identification of objects without assuming that OIDs are unique
 * across tables.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_shdescription.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_SHDESCRIPTION_H
#define PG_SHDESCRIPTION_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_shdescription definition.    cpp turns this into
 *		typedef struct FormData_pg_shdescription
 * ----------------
 */
#define SharedDescriptionRelationId  2396

CATALOG(pg_shdescription,2396) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
	Oid			objoid;			/* OID of object itself */
	Oid			classoid;		/* OID of table containing object */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		description BKI_FORCE_NOT_NULL; /* description of object */
#endif
} FormData_pg_shdescription;

DECLARE_TOAST(pg_shdescription, 2846, 2847);
#define PgShdescriptionToastTable 2846
#define PgShdescriptionToastIndex 2847
DECLARE_UNIQUE_INDEX(pg_shdescription_o_c_index, 2397, on pg_shdescription using btree(objoid oid_ops, classoid oid_ops));
#define SharedDescriptionObjIndexId 2397

/* ----------------
 *		Form_pg_shdescription corresponds to a pointer to a tuple with
 *		the format of pg_shdescription relation.
 * ----------------
 */
typedef FormData_pg_shdescription * Form_pg_shdescription;

/* ----------------
 *		compiler constants for pg_shdescription
 * ----------------
 */
#define Natts_pg_shdescription			3
#define Anum_pg_shdescription_objoid		1
#define Anum_pg_shdescription_classoid	2
#define Anum_pg_shdescription_description 3

/*
 *	Because the contents of this table are taken from the *.dat files
 *	of other catalogs, pg_shdescription does not have its own *.dat file.
 *	The initial contents are assembled by genbki.pl and loaded during initdb.
 */

#endif							/* PG_SHDESCRIPTION_H */
