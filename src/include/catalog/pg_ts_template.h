/*-------------------------------------------------------------------------
 *
 * pg_ts_template.h
 *	definition of dictionary templates for tsearch
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_ts_template.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_TS_TEMPLATE_H
#define PG_TS_TEMPLATE_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_ts_template definition.  cpp turns this into
 *		typedef struct FormData_pg_ts_template
 * ----------------
 */
#define TSTemplateRelationId	3764

CATALOG(pg_ts_template,3764)
{
	NameData	tmplname;		/* template name */
	Oid			tmplnamespace;	/* name space */
	regproc		tmplinit;		/* initialization method of dict (may be 0) */
	regproc		tmpllexize;		/* base method of dictionary */
} FormData_pg_ts_template;

DECLARE_UNIQUE_INDEX(pg_ts_template_tmplname_index, 3766, on pg_ts_template using btree(tmplname name_ops, tmplnamespace oid_ops));
#define TSTemplateNameNspIndexId	3766
DECLARE_UNIQUE_INDEX(pg_ts_template_oid_index, 3767, on pg_ts_template using btree(oid oid_ops));
#define TSTemplateOidIndexId	3767

typedef FormData_pg_ts_template *Form_pg_ts_template;

/* ----------------
 *		compiler constants for pg_ts_template
 * ----------------
 */
#define Natts_pg_ts_template				4
#define Anum_pg_ts_template_tmplname		1
#define Anum_pg_ts_template_tmplnamespace	2
#define Anum_pg_ts_template_tmplinit		3
#define Anum_pg_ts_template_tmpllexize		4

#endif							/* PG_TS_TEMPLATE_H */
