/*-------------------------------------------------------------------------
 *
 * pg_colenckey.h
 *	  definition of the "column encryption key" system catalog
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_colenkey.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_COLENCKEY_H
#define PG_COLENCKEY_H

#include "catalog/genbki.h"
#include "catalog/pg_colenckey_d.h"

/* ----------------
 *		pg_colenckey definition. cpp turns this into
 *		typedef struct FormData_pg_colenckey
 * ----------------
 */
CATALOG(pg_colenckey,8234,ColumnEncKeyRelationId)
{
	Oid			oid;
	NameData	cekname;
	Oid			ceknamespace BKI_DEFAULT(pg_catalog) BKI_LOOKUP(pg_namespace);
	Oid			cekowner BKI_LOOKUP(pg_authid);
#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	aclitem		cekacl[1] BKI_DEFAULT(_null_);
#endif
} FormData_pg_colenckey;

typedef FormData_pg_colenckey *Form_pg_colenckey;

DECLARE_TOAST(pg_colenckey, 8263, 8264);

DECLARE_UNIQUE_INDEX_PKEY(pg_colenckey_oid_index, 8240, ColumnEncKeyOidIndexId, on pg_colenckey using btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_colenckey_cekname_nsp_index, 8242, ColumnEncKeyNameNspIndexId, on pg_colenckey using btree(cekname name_ops, ceknamespace oid_ops));

#endif
