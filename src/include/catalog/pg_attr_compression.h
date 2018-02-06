/*-------------------------------------------------------------------------
 *
 * pg_attr_compression.h
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_attr_compression.h
 *
 * NOTES
 *		the genbki.pl script reads this file and generates .bki
 *		information from the DATA() statements.
 *
 *		XXX do NOT break up DATA() statements into multiple lines!
 *			the scripts are not as smart as you might think...
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ATTR_COMPRESSION_H
#define PG_ATTR_COMPRESSION_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_attr_compression definition.  cpp turns this into
 *		typedef struct FormData_pg_attr_compression
 * ----------------
 */
#define AttrCompressionRelationId		3420

CATALOG(pg_attr_compression,3420) BKI_WITHOUT_OIDS
{
	Oid			acoid;			/* attribute compression oid */
	NameData	acname;			/* name of compression AM */
	Oid			acrelid;		/* attribute relation */
	int16		acattnum;		/* attribute number in the relation */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		acoptions[1];	/* specific options from WITH */
#endif
} FormData_pg_attr_compression;

/* ----------------
 *		Form_pg_attr_compresssion corresponds to a pointer to a tuple with
 *		the format of pg_attr_compression relation.
 * ----------------
 */
typedef FormData_pg_attr_compression * Form_pg_attr_compression;

/* ----------------
 *		compiler constants for pg_attr_compression
 * ----------------
 */
#define Natts_pg_attr_compression			5
#define Anum_pg_attr_compression_acoid		1
#define Anum_pg_attr_compression_acname		2
#define Anum_pg_attr_compression_acrelid	3
#define Anum_pg_attr_compression_acattnum	4
#define Anum_pg_attr_compression_acoptions	5

/*
 * Predefined compression options for builtin compression access methods
 * It is safe to use Oids that equal to Oids of access methods, since
 * we're not using system Oids.
 *
 * Note that predefined options should have 0 in acrelid and acattnum.
 */

DATA(insert ( 3422 pglz 0 0 _null_ ));
#define PGLZ_AC_OID 3422

#endif							/* PG_ATTR_COMPRESSION_H */
