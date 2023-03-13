/*-------------------------------------------------------------------------
 *
 * pg_colenckeydata.h
 *	  definition of the "column encryption key data" system catalog
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_colenkeydata.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_COLENCKEYDATA_H
#define PG_COLENCKEYDATA_H

#include "catalog/genbki.h"
#include "catalog/pg_colenckeydata_d.h"

/* ----------------
 *		pg_colenckeydata definition. cpp turns this into
 *		typedef struct FormData_pg_colenckeydata
 * ----------------
 */
CATALOG(pg_colenckeydata,8250,ColumnEncKeyDataRelationId)
{
	Oid			oid;
	Oid			ckdcekid BKI_LOOKUP(pg_colenckey);
	Oid			ckdcmkid BKI_LOOKUP(pg_colmasterkey);
	int32		ckdcmkalg;		/* PG_CMK_* values */
#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	bytea		ckdencval BKI_FORCE_NOT_NULL;
#endif
} FormData_pg_colenckeydata;

typedef FormData_pg_colenckeydata *Form_pg_colenckeydata;

DECLARE_TOAST(pg_colenckeydata, 8237, 8238);

DECLARE_UNIQUE_INDEX_PKEY(pg_colenckeydata_oid_index, 8251, ColumnEncKeyDataOidIndexId, on pg_colenckeydata using btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_colenckeydata_ckdcekid_ckdcmkid_index, 8252, ColumnEncKeyCekidCmkidIndexId, on pg_colenckeydata using btree(ckdcekid oid_ops, ckdcmkid oid_ops));

#endif
