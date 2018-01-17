/*-------------------------------------------------------------------------
 *
 * pg_ts_parser.h
 *	definition of parsers for tsearch
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_ts_parser.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_TS_PARSER_H
#define PG_TS_PARSER_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_ts_parser definition.  cpp turns this into
 *		typedef struct FormData_pg_ts_parser
 * ----------------
 */
#define TSParserRelationId	3601

CATALOG(pg_ts_parser,3601)
{
	NameData	prsname;		/* parser's name */
	Oid			prsnamespace;	/* name space */
	regproc		prsstart;		/* init parsing session */
	regproc		prstoken;		/* return next token */
	regproc		prsend;			/* finalize parsing session */
	regproc		prsheadline;	/* return data for headline creation */
	regproc		prslextype;		/* return descriptions of lexeme's types */
} FormData_pg_ts_parser;

DECLARE_UNIQUE_INDEX(pg_ts_parser_prsname_index, 3606, on pg_ts_parser using btree(prsname name_ops, prsnamespace oid_ops));
#define TSParserNameNspIndexId	3606
DECLARE_UNIQUE_INDEX(pg_ts_parser_oid_index, 3607, on pg_ts_parser using btree(oid oid_ops));
#define TSParserOidIndexId	3607

typedef FormData_pg_ts_parser *Form_pg_ts_parser;

/* ----------------
 *		compiler constants for pg_ts_parser
 * ----------------
 */
#define Natts_pg_ts_parser					7
#define Anum_pg_ts_parser_prsname			1
#define Anum_pg_ts_parser_prsnamespace		2
#define Anum_pg_ts_parser_prsstart			3
#define Anum_pg_ts_parser_prstoken			4
#define Anum_pg_ts_parser_prsend			5
#define Anum_pg_ts_parser_prsheadline		6
#define Anum_pg_ts_parser_prslextype		7

#endif							/* PG_TS_PARSER_H */
