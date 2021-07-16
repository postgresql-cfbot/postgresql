/*-------------------------------------------------------------------------
 *
 * sqlol.c
 *
 *
 * Copyright (c) 2008-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/sqlol/sqlol.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "tcop/tcopprot.h"

#include "sqlol_gramparse.h"
#include "sqlol_keywords.h"

PG_MODULE_MAGIC;


/* Saved hook values in case of unload */
static parser_hook_type prev_parser_hook = NULL;

void		_PG_init(void);
void		_PG_fini(void);

static List *sqlol_parser_hook(const char *str, RawParseMode mode, int offset,
							   bool *error);


/*
 * Module load callback
 */
void
_PG_init(void)
{
	/* Install hooks. */
	prev_parser_hook = parser_hook;
	parser_hook = sqlol_parser_hook;
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hooks. */
	parser_hook = prev_parser_hook;
}

/*
 * sqlol_parser_hook: parse our grammar
 */
static List *
sqlol_parser_hook(const char *str, RawParseMode mode, int offset, bool *error)
{
	sqlol_yyscan_t yyscanner;
	sqlol_base_yy_extra_type yyextra;
	int			yyresult;

	if (mode != RAW_PARSE_DEFAULT && mode != RAW_PARSE_SINGLE_QUERY)
	{
		if (prev_parser_hook)
			return (*prev_parser_hook) (str, mode, offset, error);

		*error = true;
		return NIL;
	}

	/* initialize the flex scanner */
	yyscanner = sqlol_scanner_init(str, &yyextra.sqlol_yy_extra,
							 sqlol_ScanKeywords, sqlol_NumScanKeywords,
							 offset);

	/* initialize the bison parser */
	sqlol_parser_init(&yyextra);

	/* Parse! */
	yyresult = sqlol_base_yyparse(yyscanner);

	/* Clean up (release memory) */
	sqlol_scanner_finish(yyscanner);

	/*
	 * Invalid statement, fallback on previous parser_hook if any or
	 * raw_parser()
	 */
	if (yyresult)
	{
		if (prev_parser_hook)
			return (*prev_parser_hook) (str, mode, offset, error);

		*error = true;
		return NIL;
	}

	return yyextra.parsetree;
}

int
sqlol_base_yylex(YYSTYPE *lvalp, YYLTYPE *llocp, sqlol_yyscan_t yyscanner)
{
	int			cur_token;

	cur_token = sqlol_yylex(&(lvalp->sqlol_yystype), llocp, yyscanner);

	return cur_token;
}
