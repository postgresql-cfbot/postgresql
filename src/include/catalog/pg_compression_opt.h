/*-------------------------------------------------------------------------
 *
 * pg_compression_opt.h
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_compression_opt.h
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
#ifndef PG_COMPRESSION_OPT_H
#define PG_COMPRESSION_OPT_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_compression_opt definition.  cpp turns this into
 *		typedef struct FormData_pg_compression_opt
 * ----------------
 */
#define CompressionOptRelationId	3420

CATALOG(pg_compression_opt,3420) BKI_WITHOUT_OIDS
{
	Oid			cmoptoid;		/* compression options oid */
	NameData	cmname;			/* name of compression method */
	regproc		cmhandler;		/* compression handler */
	text		cmoptions[1];	/* specific options from WITH */
} FormData_pg_compression_opt;

/* ----------------
 *		Form_pg_compression_opt corresponds to a pointer to a tuple with
 *		the format of pg_compression_opt relation.
 * ----------------
 */
typedef FormData_pg_compression_opt * Form_pg_compression_opt;

/* ----------------
 *		compiler constants for pg_compression_opt
 * ----------------
 */
#define Natts_pg_compression_opt			4
#define Anum_pg_compression_opt_cmoptoid	1
#define Anum_pg_compression_opt_cmname		2
#define Anum_pg_compression_opt_cmhandler	3
#define Anum_pg_compression_opt_cmoptions	4

#endif							/* PG_COMPRESSION_OPT_H */
