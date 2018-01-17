/*-------------------------------------------------------------------------
 *
 * pg_language.h
 *	  definition of the system "language" relation (pg_language)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_language.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LANGUAGE_H
#define PG_LANGUAGE_H

#include "catalog/genbki.h"
#include "catalog/oid_symbols.h"

/* ----------------
 *		pg_language definition.  cpp turns this into
 *		typedef struct FormData_pg_language
 * ----------------
 */
#define LanguageRelationId	2612

CATALOG(pg_language,2612)
{
	NameData	lanname;		/* Language name */
	Oid			lanowner;		/* Language's owner */
	bool		lanispl;		/* Is a procedural language */
	bool		lanpltrusted;	/* PL is trusted */
	Oid			lanplcallfoid;	/* Call handler for PL */
	Oid			laninline;		/* Optional anonymous-block handler function */
	Oid			lanvalidator;	/* Optional validation function */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	aclitem		lanacl[1];		/* Access privileges */
#endif
} FormData_pg_language;

DECLARE_UNIQUE_INDEX(pg_language_name_index, 2681, on pg_language using btree(lanname name_ops));
#define LanguageNameIndexId  2681
DECLARE_UNIQUE_INDEX(pg_language_oid_index, 2682, on pg_language using btree(oid oid_ops));
#define LanguageOidIndexId	2682

/* ----------------
 *		Form_pg_language corresponds to a pointer to a tuple with
 *		the format of pg_language relation.
 * ----------------
 */
typedef FormData_pg_language *Form_pg_language;

/* ----------------
 *		compiler constants for pg_language
 * ----------------
 */
#define Natts_pg_language				8
#define Anum_pg_language_lanname		1
#define Anum_pg_language_lanowner		2
#define Anum_pg_language_lanispl		3
#define Anum_pg_language_lanpltrusted	4
#define Anum_pg_language_lanplcallfoid	5
#define Anum_pg_language_laninline		6
#define Anum_pg_language_lanvalidator	7
#define Anum_pg_language_lanacl			8

#endif							/* PG_LANGUAGE_H */
