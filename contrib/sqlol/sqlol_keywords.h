/*-------------------------------------------------------------------------
 *
 * sqlol_keywords.h
 *	  lexical token lookup for key words in PostgreSQL
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/sqlol/sqlol_keywords.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SQLOL_KEYWORDS_H
#define SQLOL_KEYWORDS_H

/* Keyword categories --- should match lists in gram.y */
#define UNRESERVED_KEYWORD		0
#define COL_NAME_KEYWORD		1
#define TYPE_FUNC_NAME_KEYWORD	2
#define RESERVED_KEYWORD		3


typedef struct sqlol_ScanKeyword
{
	const char *name;			/* in lower case */
	int16		value;			/* grammar's token code */
	int16		category;		/* see codes above */
} sqlol_ScanKeyword;

extern PGDLLIMPORT const sqlol_ScanKeyword sqlol_ScanKeywords[];
extern PGDLLIMPORT const int sqlol_NumScanKeywords;

extern const sqlol_ScanKeyword *sqlol_ScanKeywordLookup(const char *text,
				  const sqlol_ScanKeyword *keywords,
				  int num_keywords);

#endif							/* SQLOL_KEYWORDS_H */
