/*-------------------------------------------------------------------------
 *
 * pg_transform.h
 *
 * Copyright (c) 2012-2018, PostgreSQL Global Development Group
 *
 * src/include/catalog/pg_transform.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_TRANSFORM_H
#define PG_TRANSFORM_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_transform definition.  cpp turns this into
 *		typedef struct FormData_pg_transform
 * ----------------
 */
#define TransformRelationId 3576

CATALOG(pg_transform,3576)
{
	Oid			trftype;
	Oid			trflang;
	regproc		trffromsql;
	regproc		trftosql;
} FormData_pg_transform;

DECLARE_UNIQUE_INDEX(pg_transform_oid_index, 3574, on pg_transform using btree(oid oid_ops));
#define TransformOidIndexId 3574
DECLARE_UNIQUE_INDEX(pg_transform_type_lang_index, 3575, on pg_transform using btree(trftype oid_ops, trflang oid_ops));
#define TransformTypeLangIndexId  3575

typedef FormData_pg_transform *Form_pg_transform;

/* ----------------
 *		compiler constants for pg_transform
 * ----------------
 */
#define Natts_pg_transform			4
#define Anum_pg_transform_trftype	1
#define Anum_pg_transform_trflang	2
#define Anum_pg_transform_trffromsql	3
#define Anum_pg_transform_trftosql	4

#endif							/* PG_TRANSFORM_H */
