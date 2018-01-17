/*-------------------------------------------------------------------------
 *
 * pg_authid.h
 *	  definition of the system "authorization identifier" relation (pg_authid)
 *	  along with the relation's initial contents.
 *
 *	  pg_shadow and pg_group are now publicly accessible views on pg_authid.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_authid.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_AUTHID_H
#define PG_AUTHID_H

#include "catalog/genbki.h"
#include "catalog/oid_symbols.h"

/*
 * The CATALOG definition has to refer to the type of rolvaliduntil as
 * "timestamptz" (lower case) so that bootstrap mode recognizes it.  But
 * the C header files define this type as TimestampTz.  Since the field is
 * potentially-null and therefore can't be accessed directly from C code,
 * there is no particular need for the C struct definition to show the
 * field type as TimestampTz --- instead we just make it int.
 */
#define timestamptz int


/* ----------------
 *		pg_authid definition.  cpp turns this into
 *		typedef struct FormData_pg_authid
 * ----------------
 */
#define AuthIdRelationId	1260
#define AuthIdRelation_Rowtype_Id	2842

CATALOG(pg_authid,1260) BKI_SHARED_RELATION BKI_ROWTYPE_OID(2842) BKI_SCHEMA_MACRO
{
	/* name of role */
	NameData	rolname;

	/* read this field via superuser() only! */
	bool		rolsuper BKI_DEFAULT(f);

	/* inherit privileges from other roles? */
	bool		rolinherit BKI_DEFAULT(t);

	/* allowed to create more roles? */
	bool		rolcreaterole BKI_DEFAULT(f);

	/* allowed to create databases? */
	bool		rolcreatedb BKI_DEFAULT(f);

	/* allowed to log in as session user? */
	bool		rolcanlogin BKI_DEFAULT(f);

	/* role used for streaming replication */
	bool		rolreplication BKI_DEFAULT(f);

	/* bypasses row level security? */
	bool		rolbypassrls BKI_DEFAULT(f);

	/* max connections allowed (-1=no limit) */
	int32		rolconnlimit BKI_DEFAULT(-1);

	/* remaining fields may be null; use heap_getattr to read them! */
#ifdef CATALOG_VARLEN			/* variable-length fields start here */

	/* password, if any */
	text		rolpassword BKI_DEFAULT(_null_);

	/* password expiration time, if any */
	timestamptz rolvaliduntil BKI_DEFAULT(_null_);
#endif
} FormData_pg_authid;

#undef timestamptz

DECLARE_UNIQUE_INDEX(pg_authid_rolname_index, 2676, on pg_authid using btree(rolname name_ops));
#define AuthIdRolnameIndexId	2676
DECLARE_UNIQUE_INDEX(pg_authid_oid_index, 2677, on pg_authid using btree(oid oid_ops));
#define AuthIdOidIndexId	2677

/* ----------------
 *		Form_pg_authid corresponds to a pointer to a tuple with
 *		the format of pg_authid relation.
 * ----------------
 */
typedef FormData_pg_authid *Form_pg_authid;

/* ----------------
 *		compiler constants for pg_authid
 * ----------------
 */
#define Natts_pg_authid					11
#define Anum_pg_authid_rolname			1
#define Anum_pg_authid_rolsuper			2
#define Anum_pg_authid_rolinherit		3
#define Anum_pg_authid_rolcreaterole	4
#define Anum_pg_authid_rolcreatedb		5
#define Anum_pg_authid_rolcanlogin		6
#define Anum_pg_authid_rolreplication	7
#define Anum_pg_authid_rolbypassrls		8
#define Anum_pg_authid_rolconnlimit		9
#define Anum_pg_authid_rolpassword		10
#define Anum_pg_authid_rolvaliduntil	11

#endif							/* PG_AUTHID_H */
