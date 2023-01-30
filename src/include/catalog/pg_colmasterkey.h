/*-------------------------------------------------------------------------
 *
 * pg_colmasterkey.h
 *	  definition of the "column master key" system catalog
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_colmasterkey.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_COLMASTERKEY_H
#define PG_COLMASTERKEY_H

#include "catalog/genbki.h"
#include "catalog/pg_colmasterkey_d.h"

/* ----------------
 *		pg_colmasterkey definition. cpp turns this into
 *		typedef struct FormData_pg_colmasterkey
 * ----------------
 */
CATALOG(pg_colmasterkey,8233,ColumnMasterKeyRelationId)
{
	Oid			oid;
	NameData	cmkname;
	Oid			cmknamespace BKI_DEFAULT(pg_catalog) BKI_LOOKUP(pg_namespace);
	Oid			cmkowner BKI_LOOKUP(pg_authid);
#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		cmkrealm BKI_FORCE_NOT_NULL;
#endif
} FormData_pg_colmasterkey;

typedef FormData_pg_colmasterkey *Form_pg_colmasterkey;

DECLARE_TOAST(pg_colmasterkey, 8235, 8236);

DECLARE_UNIQUE_INDEX_PKEY(pg_colmasterkey_oid_index, 8239, ColumnMasterKeyOidIndexId, on pg_colmasterkey using btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_colmasterkey_cmkname_nsp_index, 8241, ColumnMasterKeyNameNspIndexId, on pg_colmasterkey using btree(cmkname name_ops, cmknamespace oid_ops));

#endif
