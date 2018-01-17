/*-------------------------------------------------------------------------
 *
 * pg_ts_dict.h
 *	definition of dictionaries for tsearch
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_ts_dict.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_TS_DICT_H
#define PG_TS_DICT_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_ts_dict definition.  cpp turns this into
 *		typedef struct FormData_pg_ts_dict
 * ----------------
 */
#define TSDictionaryRelationId	3600

CATALOG(pg_ts_dict,3600)
{
	NameData	dictname;		/* dictionary name */
	Oid			dictnamespace;	/* name space */
	Oid			dictowner;		/* owner */
	Oid			dicttemplate;	/* dictionary's template */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		dictinitoption; /* options passed to dict_init() */
#endif
} FormData_pg_ts_dict;

DECLARE_UNIQUE_INDEX(pg_ts_dict_dictname_index, 3604, on pg_ts_dict using btree(dictname name_ops, dictnamespace oid_ops));
#define TSDictionaryNameNspIndexId	3604
DECLARE_UNIQUE_INDEX(pg_ts_dict_oid_index, 3605, on pg_ts_dict using btree(oid oid_ops));
#define TSDictionaryOidIndexId	3605

typedef FormData_pg_ts_dict *Form_pg_ts_dict;

/* ----------------
 *		compiler constants for pg_ts_dict
 * ----------------
 */
#define Natts_pg_ts_dict				5
#define Anum_pg_ts_dict_dictname		1
#define Anum_pg_ts_dict_dictnamespace	2
#define Anum_pg_ts_dict_dictowner		3
#define Anum_pg_ts_dict_dicttemplate	4
#define Anum_pg_ts_dict_dictinitoption	5

#endif							/* PG_TS_DICT_H */
