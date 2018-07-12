/*-------------------------------------------------------------------------
 *
 * pg_attr_compression.h
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_attr_compression.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ATTR_COMPRESSION_H
#define PG_ATTR_COMPRESSION_H

#include "catalog/genbki.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_attr_compression_d.h"

/* ----------------
 *		pg_attr_compression definition.  cpp turns this into
 *		typedef struct FormData_pg_attr_compression
 * ----------------
 */
CATALOG(pg_attr_compression,4001,AttrCompressionRelationId) BKI_WITHOUT_OIDS
{
	Oid			acoid;						/* attribute compression oid */
	NameData	acname;						/* name of compression AM */
	Oid			acrelid BKI_DEFAULT(0);		/* attribute relation */
	int16		acattnum BKI_DEFAULT(0);	/* attribute number in the relation */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		acoptions[1] BKI_DEFAULT(_null_);	/* specific options from WITH */
#endif
} FormData_pg_attr_compression;

/* ----------------
 *		Form_pg_attr_compresssion corresponds to a pointer to a tuple with
 *		the format of pg_attr_compression relation.
 * ----------------
 */
typedef FormData_pg_attr_compression *Form_pg_attr_compression;

#ifdef EXPOSE_TO_CLIENT_CODE
/* builtin attribute compression Oids */
#define PGLZ_AC_OID (PGLZ_COMPRESSION_AM_OID)
#define ZLIB_AC_OID (ZLIB_COMPRESSION_AM_OID)
#endif

#endif							/* PG_ATTR_COMPRESSION_H */
