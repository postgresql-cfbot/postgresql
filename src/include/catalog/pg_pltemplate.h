/*-------------------------------------------------------------------------
 *
 * pg_pltemplate.h
 *	  definition of the system "PL template" relation (pg_pltemplate)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_pltemplate.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PLTEMPLATE_H
#define PG_PLTEMPLATE_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_pltemplate definition.  cpp turns this into
 *		typedef struct FormData_pg_pltemplate
 * ----------------
 */
#define PLTemplateRelationId	1136

CATALOG(pg_pltemplate,1136) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
	NameData	tmplname;		/* name of PL */
	bool		tmpltrusted;	/* PL is trusted? */
	bool		tmpldbacreate;	/* PL is installable by db owner? */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		tmplhandler BKI_FORCE_NOT_NULL; /* name of call handler
												 * function */
	text		tmplinline;		/* name of anonymous-block handler, or NULL */
	text		tmplvalidator;	/* name of validator function, or NULL */
	text		tmpllibrary BKI_FORCE_NOT_NULL; /* path of shared library */
	aclitem		tmplacl[1];		/* access privileges for template */
#endif
} FormData_pg_pltemplate;

DECLARE_UNIQUE_INDEX(pg_pltemplate_name_index, 1137, on pg_pltemplate using btree(tmplname name_ops));
#define PLTemplateNameIndexId  1137

/* ----------------
 *		Form_pg_pltemplate corresponds to a pointer to a row with
 *		the format of pg_pltemplate relation.
 * ----------------
 */
typedef FormData_pg_pltemplate *Form_pg_pltemplate;

/* ----------------
 *		compiler constants for pg_pltemplate
 * ----------------
 */
#define Natts_pg_pltemplate					8
#define Anum_pg_pltemplate_tmplname			1
#define Anum_pg_pltemplate_tmpltrusted		2
#define Anum_pg_pltemplate_tmpldbacreate	3
#define Anum_pg_pltemplate_tmplhandler		4
#define Anum_pg_pltemplate_tmplinline		5
#define Anum_pg_pltemplate_tmplvalidator	6
#define Anum_pg_pltemplate_tmpllibrary		7
#define Anum_pg_pltemplate_tmplacl			8

#endif							/* PG_PLTEMPLATE_H */
