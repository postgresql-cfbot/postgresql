/*-------------------------------------------------------------------------
 *
 * pg_colenckey.h
 *	  definition of the "column encryption key" system catalog
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
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
	Oid			cekowner BKI_LOOKUP(pg_authid);
} FormData_pg_colenckey;

typedef FormData_pg_colenckey *Form_pg_colenckey;

DECLARE_UNIQUE_INDEX_PKEY(pg_colenckey_oid_index, 8240, ColumnEncKeyOidIndexId, on pg_colenckey using btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_colenckey_cekname_index, 8242, ColumnEncKeyNameIndexId, on pg_colenckey using btree(cekname name_ops));

/*
 * Constants for CMK and CEK algorithms.  Note that these are part of the
 * protocol.  For clarity, the assigned numbers are not reused between CMKs
 * and CEKs, but that is not technically required.  In either case, don't
 * assign zero, so that that can be used as an invalid value.
 */

#define PG_CMK_RSAES_OAEP_SHA_1			1
#define PG_CMK_RSAES_OAEP_SHA_256		2

#define PG_CEK_AEAD_AES_128_CBC_HMAC_SHA_256	130
#define PG_CEK_AEAD_AES_192_CBC_HMAC_SHA_384	131
#define PG_CEK_AEAD_AES_256_CBC_HMAC_SHA_384	132
#define PG_CEK_AEAD_AES_256_CBC_HMAC_SHA_512	133

#endif
