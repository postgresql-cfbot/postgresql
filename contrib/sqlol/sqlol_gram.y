%{

/*#define YYDEBUG 1*/
/*-------------------------------------------------------------------------
 *
 * sqlol_gram.y
 *	  sqlol BISON rules/actions
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  contrib/sqlol/sqlol_gram.y
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/namespace.h"
#include "nodes/makefuncs.h"
#include "catalog/pg_class_d.h"

#include "sqlol_gramparse.h"

/*
 * Location tracking support --- simpler than bison's default, since we only
 * want to track the start position not the end position of each nonterminal.
 */
#define YYLLOC_DEFAULT(Current, Rhs, N) \
	do { \
		if ((N) > 0) \
			(Current) = (Rhs)[1]; \
		else \
			(Current) = (-1); \
	} while (0)

/*
 * The above macro assigns -1 (unknown) as the parse location of any
 * nonterminal that was reduced from an empty rule, or whose leftmost
 * component was reduced from an empty rule.  This is problematic
 * for nonterminals defined like
 *		OptFooList: / * EMPTY * / { ... } | OptFooList Foo { ... } ;
 * because we'll set -1 as the location during the first reduction and then
 * copy it during each subsequent reduction, leaving us with -1 for the
 * location even when the list is not empty.  To fix that, do this in the
 * action for the nonempty rule(s):
 *		if (@$ < 0) @$ = @2;
 * (Although we have many nonterminals that follow this pattern, we only
 * bother with fixing @$ like this when the nonterminal's parse location
 * is actually referenced in some rule.)
 *
 * A cleaner answer would be to make YYLLOC_DEFAULT scan all the Rhs
 * locations until it's found one that's not -1.  Then we'd get a correct
 * location for any nonterminal that isn't entirely empty.  But this way
 * would add overhead to every rule reduction, and so far there's not been
 * a compelling reason to pay that overhead.
 */

/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.  Note this only works with
 * bison >= 2.0.  However, in bison 1.875 the default is to use alloca()
 * if possible, so there's not really much problem anyhow, at least if
 * you're building with gcc.
 */
#define YYMALLOC palloc
#define YYFREE   pfree


#define parser_yyerror(msg)  sqlol_scanner_yyerror(msg, yyscanner)
#define parser_errposition(pos)  sqlol_scanner_errposition(pos, yyscanner)

static void sqlol_base_yyerror(YYLTYPE *yylloc, sqlol_yyscan_t yyscanner,
						 const char *msg);
static RawStmt *makeRawStmt(Node *stmt, int stmt_location);
static void updateRawStmtEnd(RawStmt *rs, int end_location);
static Node *makeColumnRef(char *colname, List *indirection,
						   int location, sqlol_yyscan_t yyscanner);
static void check_qualified_name(List *names, sqlol_yyscan_t yyscanner);
static List *check_indirection(List *indirection, sqlol_yyscan_t yyscanner);

%}

%pure-parser
%expect 0
%name-prefix="sqlol_base_yy"
%locations

%parse-param {sqlol_yyscan_t yyscanner}
%lex-param   {sqlol_yyscan_t yyscanner}

%union
{
	sqlol_YYSTYPE		sqlol_yystype;
	/* these fields must match sqlol_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;

	List				*list;
	Node				*node;
	RangeVar			*range;
	ResTarget			*target;
}

%type <node>	stmt toplevel_stmt GimmehStmt MaekStmt simple_gimmeh columnref
				indirection_el

%type <list>	parse_toplevel rawstmt gimmeh_list indirection

%type <range>	qualified_name

%type <str>		ColId ColLabel attr_name

%type <target>	gimmeh_el

/*
 * Non-keyword token types.  These are hard-wired into the "flex" lexer.
 * They must be listed first so that their numeric codes do not depend on
 * the set of keywords.  PL/pgSQL depends on this so that it can share the
 * same lexer.  If you add/change tokens here, fix PL/pgSQL to match!
 *
 */
%token <str>	IDENT FCONST SCONST Op

/*
 * If you want to make any keyword changes, update the keyword table in
 * src/include/parser/kwlist.h and add new keywords to the appropriate one
 * of the reserved-or-not-so-reserved keyword lists, below; search
 * this file for "Keyword category lists".
 */

/* ordinary key words in alphabetical order */
%token <keyword> A GIMMEH HAI HAS I KTHXBYE MAEK

%%

/*
 *	The target production for the whole parse.
 */
parse_toplevel:
			rawstmt
			{
				pg_yyget_extra(yyscanner)->parsetree = $1;

				YYACCEPT;
			}
		;

/*
 * At top level, we wrap each stmt with a RawStmt node carrying start location
 * and length of the stmt's text.  Notice that the start loc/len are driven
 * entirely from semicolon locations (@2).  It would seem natural to use
 * @1 or @3 to get the true start location of a stmt, but that doesn't work
 * for statements that can start with empty nonterminals (opt_with_clause is
 * the main offender here); as noted in the comments for YYLLOC_DEFAULT,
 * we'd get -1 for the location in such cases.
 * We also take care to discard empty statements entirely.
 */
rawstmt:	toplevel_stmt KTHXBYE
				{
					RawStmt *raw = makeRawStmt($1, 0);
					updateRawStmtEnd(raw, @2 + 7);
					$$ = list_make1(raw);
				}
		;

/*
 * toplevel_stmt includes BEGIN and END.  stmt does not include them, because
 * those words have different meanings in function bodys.
 */
toplevel_stmt:
			HAI FCONST stmt { $$ = $3; }
		;

stmt:
			GimmehStmt
			| MaekStmt
		;

/*****************************************************************************
 *
 * GIMMEH statement
 *
 *****************************************************************************/

GimmehStmt:
			simple_gimmeh						{ $$ = $1; }
		;

simple_gimmeh:
			I HAS A qualified_name GIMMEH gimmeh_list
				{
					SelectStmt *n = makeNode(SelectStmt);
					n->targetList = $6;
					n->fromClause = list_make1($4);
					$$ = (Node *)n;
				}
		;

gimmeh_list:
		   gimmeh_el							{ $$ = list_make1($1); }
		   | gimmeh_list ',' gimmeh_el			{ $$ = lappend($1, $3); }

gimmeh_el:
		 columnref
			{
				$$ = makeNode(ResTarget);
				$$->name = NULL;
				$$->indirection = NIL;
				$$->val = (Node *)$1;
				$$->location = @1;
			}

MaekStmt:
		MAEK GimmehStmt A qualified_name
			{
				ViewStmt *n = makeNode(ViewStmt);
				n->view = $4;
				n->view->relpersistence = RELPERSISTENCE_PERMANENT;
				n->aliases = NIL;
				n->query = $2;
				n->replace = false;
				n->options = NIL;
				n->withCheckOption = false;
				$$ = (Node *) n;
			}

qualified_name:
			ColId
				{
					$$ = makeRangeVar(NULL, $1, @1);
				}
			| ColId indirection
				{
					check_qualified_name($2, yyscanner);
					$$ = makeRangeVar(NULL, NULL, @1);
					switch (list_length($2))
					{
						case 1:
							$$->catalogname = NULL;
							$$->schemaname = $1;
							$$->relname = strVal(linitial($2));
							break;
						case 2:
							$$->catalogname = $1;
							$$->schemaname = strVal(linitial($2));
							$$->relname = strVal(lsecond($2));
							break;
						default:
							/*
							 * It's ok to error out here as at this point we
							 * already parsed a "HAI FCONST" preamble, and no
							 * other grammar is likely to accept a command
							 * starting with that, so there's no point trying
							 * to fall back on the other grammars.
							 */
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("improper qualified name (too many dotted names): %s",
											NameListToString(lcons(makeString($1), $2))),
									 parser_errposition(@1)));
							break;
					}
				}
		;

columnref:	ColId
				{
					$$ = makeColumnRef($1, NIL, @1, yyscanner);
				}
			| ColId indirection
				{
					$$ = makeColumnRef($1, $2, @1, yyscanner);
				}
		;

ColId:		IDENT									{ $$ = $1; }

indirection:
			indirection_el							{ $$ = list_make1($1); }
			| indirection indirection_el			{ $$ = lappend($1, $2); }
		;

indirection_el:
			'.' attr_name
				{
					$$ = (Node *) makeString($2);
				}
		;

attr_name:	ColLabel								{ $$ = $1; };

ColLabel:	IDENT									{ $$ = $1; }

%%

/*
 * The signature of this function is required by bison.  However, we
 * ignore the passed yylloc and instead use the last token position
 * available from the scanner.
 */
static void
sqlol_base_yyerror(YYLTYPE *yylloc, sqlol_yyscan_t yyscanner, const char *msg)
{
	parser_yyerror(msg);
}

static RawStmt *
makeRawStmt(Node *stmt, int stmt_location)
{
	RawStmt    *rs = makeNode(RawStmt);

	rs->stmt = stmt;
	rs->stmt_location = stmt_location;
	rs->stmt_len = 0;			/* might get changed later */
	return rs;
}

/* Adjust a RawStmt to reflect that it doesn't run to the end of the string */
static void
updateRawStmtEnd(RawStmt *rs, int end_location)
{
	/*
	 * If we already set the length, don't change it.  This is for situations
	 * like "select foo ;; select bar" where the same statement will be last
	 * in the string for more than one semicolon.
	 */
	if (rs->stmt_len > 0)
		return;

	/* OK, update length of RawStmt */
	rs->stmt_len = end_location - rs->stmt_location;
}

static Node *
makeColumnRef(char *colname, List *indirection,
			  int location, sqlol_yyscan_t yyscanner)
{
	/*
	 * Generate a ColumnRef node, with an A_Indirection node added if there
	 * is any subscripting in the specified indirection list.  However,
	 * any field selection at the start of the indirection list must be
	 * transposed into the "fields" part of the ColumnRef node.
	 */
	ColumnRef  *c = makeNode(ColumnRef);
	int		nfields = 0;
	ListCell *l;

	c->location = location;
	foreach(l, indirection)
	{
		if (IsA(lfirst(l), A_Indices))
		{
			A_Indirection *i = makeNode(A_Indirection);

			if (nfields == 0)
			{
				/* easy case - all indirection goes to A_Indirection */
				c->fields = list_make1(makeString(colname));
				i->indirection = check_indirection(indirection, yyscanner);
			}
			else
			{
				/* got to split the list in two */
				i->indirection = check_indirection(list_copy_tail(indirection,
																  nfields),
												   yyscanner);
				indirection = list_truncate(indirection, nfields);
				c->fields = lcons(makeString(colname), indirection);
			}
			i->arg = (Node *) c;
			return (Node *) i;
		}
		else if (IsA(lfirst(l), A_Star))
		{
			/* We only allow '*' at the end of a ColumnRef */
			if (lnext(indirection, l) != NULL)
				parser_yyerror("improper use of \"*\"");
		}
		nfields++;
	}
	/* No subscripting, so all indirection gets added to field list */
	c->fields = lcons(makeString(colname), indirection);
	return (Node *) c;
}

/* check_qualified_name --- check the result of qualified_name production
 *
 * It's easiest to let the grammar production for qualified_name allow
 * subscripts and '*', which we then must reject here.
 */
static void
check_qualified_name(List *names, sqlol_yyscan_t yyscanner)
{
	ListCell   *i;

	foreach(i, names)
	{
		if (!IsA(lfirst(i), String))
			parser_yyerror("syntax error");
	}
}

/* check_indirection --- check the result of indirection production
 *
 * We only allow '*' at the end of the list, but it's hard to enforce that
 * in the grammar, so do it here.
 */
static List *
check_indirection(List *indirection, sqlol_yyscan_t yyscanner)
{
	ListCell *l;

	foreach(l, indirection)
	{
		if (IsA(lfirst(l), A_Star))
		{
			if (lnext(indirection, l) != NULL)
				parser_yyerror("improper use of \"*\"");
		}
	}
	return indirection;
}

/* sqlol_parser_init()
 * Initialize to parse one query string
 */
void
sqlol_parser_init(sqlol_base_yy_extra_type *yyext)
{
	yyext->parsetree = NIL;		/* in case grammar forgets to set it */
}
