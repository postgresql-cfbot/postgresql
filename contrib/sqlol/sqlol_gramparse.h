/*-------------------------------------------------------------------------
 *
 * sqlol_gramparse.h
 *		Shared definitions for the "raw" parser (flex and bison phases only)
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/sqlol/sqlol_gramparse.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SQLOL_GRAMPARSE_H
#define SQLOL_GRAMPARSE_H

#include "nodes/parsenodes.h"
#include "sqlol_scanner.h"

/*
 * NB: include gram.h only AFTER including scanner.h, because scanner.h
 * is what #defines YYLTYPE.
 */
#include "sqlol_gram.h"

/*
 * The YY_EXTRA data that a flex scanner allows us to pass around.  Private
 * state needed for raw parsing/lexing goes here.
 */
typedef struct sqlol_base_yy_extra_type
{
	/*
	 * Fields used by the core scanner.
	 */
	sqlol_yy_extra_type sqlol_yy_extra;

	/*
	 * State variables that belong to the grammar.
	 */
	List	   *parsetree;		/* final parse result is delivered here */
} sqlol_base_yy_extra_type;

/*
 * In principle we should use yyget_extra() to fetch the yyextra field
 * from a yyscanner struct.  However, flex always puts that field first,
 * and this is sufficiently performance-critical to make it seem worth
 * cheating a bit to use an inline macro.
 */
#define pg_yyget_extra(yyscanner) (*((sqlol_base_yy_extra_type **) (yyscanner)))


/* from parser.c */
extern int	sqlol_base_yylex(YYSTYPE *lvalp, YYLTYPE *llocp,
					   sqlol_yyscan_t yyscanner);

/* from gram.y */
extern void sqlol_parser_init(sqlol_base_yy_extra_type *yyext);
extern int	sqlol_baseyyparse(sqlol_yyscan_t yyscanner);

#endif							/* SQLOL_GRAMPARSE_H */
