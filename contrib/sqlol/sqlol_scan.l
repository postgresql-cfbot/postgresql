%top{
/*-------------------------------------------------------------------------
 *
 * sqlol_scan.l
 *	  lexical scanner for sqlol
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/sqlol/sqlol_scan.l
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/string.h"
#include "sqlol_gramparse.h"
#include "parser/scansup.h"
#include "mb/pg_wchar.h"

#include "sqlol_keywords.h"
}

%{

/* LCOV_EXCL_START */

/* Avoid exit() on fatal scanner errors (a bit ugly -- see yy_fatal_error) */
#undef fprintf
#define fprintf(file, fmt, msg)  fprintf_to_ereport(fmt, msg)

static void
fprintf_to_ereport(const char *fmt, const char *msg)
{
	ereport(ERROR, (errmsg_internal("%s", msg)));
}


/*
 * Set the type of YYSTYPE.
 */
#define YYSTYPE sqlol_YYSTYPE

/*
 * Set the type of yyextra.  All state variables used by the scanner should
 * be in yyextra, *not* statically allocated.
 */
#define YY_EXTRA_TYPE sqlol_yy_extra_type *

/*
 * Each call to yylex must set yylloc to the location of the found token
 * (expressed as a byte offset from the start of the input text).
 * When we parse a token that requires multiple lexer rules to process,
 * this should be done in the first such rule, else yylloc will point
 * into the middle of the token.
 */
#define SET_YYLLOC()  (*(yylloc) = yytext - yyextra->scanbuf)

/*
 * Advance yylloc by the given number of bytes.
 */
#define ADVANCE_YYLLOC(delta)  ( *(yylloc) += (delta) )

/*
 * Sometimes, we do want yylloc to point into the middle of a token; this is
 * useful for instance to throw an error about an escape sequence within a
 * string literal.  But if we find no error there, we want to revert yylloc
 * to the token start, so that that's the location reported to the parser.
 * Use PUSH_YYLLOC/POP_YYLLOC to save/restore yylloc around such code.
 * (Currently the implied "stack" is just one location, but someday we might
 * need to nest these.)
 */
#define PUSH_YYLLOC()	(yyextra->save_yylloc = *(yylloc))
#define POP_YYLLOC()	(*(yylloc) = yyextra->save_yylloc)

#define startlit()	( yyextra->literallen = 0 )
static void addlit(char *ytext, int yleng, sqlol_yyscan_t yyscanner);
static void addlitchar(unsigned char ychar, sqlol_yyscan_t yyscanner);
static char *litbufdup(sqlol_yyscan_t yyscanner);

#define yyerror(msg)  sqlol_scanner_yyerror(msg, yyscanner)

#define lexer_errposition()  sqlol_scanner_errposition(*(yylloc), yyscanner)

/*
 * Work around a bug in flex 2.5.35: it emits a couple of functions that
 * it forgets to emit declarations for.  Since we use -Wmissing-prototypes,
 * this would cause warnings.  Providing our own declarations should be
 * harmless even when the bug gets fixed.
 */
extern int	sqlol_yyget_column(yyscan_t yyscanner);
extern void sqlol_yyset_column(int column_no, yyscan_t yyscanner);

%}

%option reentrant
%option bison-bridge
%option bison-locations
%option 8bit
%option never-interactive
%option nodefault
%option noinput
%option nounput
%option noyywrap
%option noyyalloc
%option noyyrealloc
%option noyyfree
%option warn
%option prefix="sqlol_yy"

/*
 * OK, here is a short description of lex/flex rules behavior.
 * The longest pattern which matches an input string is always chosen.
 * For equal-length patterns, the first occurring in the rules list is chosen.
 * INITIAL is the starting state, to which all non-conditional rules apply.
 * Exclusive states change parsing rules while the state is active.  When in
 * an exclusive state, only those rules defined for that state apply.
 *
 * We use exclusive states for quoted strings, extended comments,
 * and to eliminate parsing troubles for numeric strings.
 * Exclusive states:
 *  <xd> delimited identifiers (double-quoted identifiers)
 *  <xq> standard quoted strings
 *  <xqs> quote stop (detect continued strings)
 *
 * Remember to add an <<EOF>> case whenever you add a new exclusive state!
 * The default one is probably not the right thing.
 */

%x xd
%x xq
%x xqs

/*
 * In order to make the world safe for Windows and Mac clients as well as
 * Unix ones, we accept either \n or \r as a newline.  A DOS-style \r\n
 * sequence will be seen as two successive newlines, but that doesn't cause
 * any problems.  Comments that start with -- and extend to the next
 * newline are treated as equivalent to a single whitespace character.
 *
 * NOTE a fine point: if there is no newline following --, we will absorb
 * everything to the end of the input as a comment.  This is correct.  Older
 * versions of Postgres failed to recognize -- as a comment if the input
 * did not end with a newline.
 *
 * XXX perhaps \f (formfeed) should be treated as a newline as well?
 *
 * XXX if you change the set of whitespace characters, fix scanner_isspace()
 * to agree.
 */

space			[ \t\n\r\f]
horiz_space		[ \t\f]
newline			[\n\r]
non_newline		[^\n\r]

comment			("--"{non_newline}*)

whitespace		({space}+|{comment})

/*
 * SQL requires at least one newline in the whitespace separating
 * string literals that are to be concatenated.  Silly, but who are we
 * to argue?  Note that {whitespace_with_newline} should not have * after
 * it, whereas {whitespace} should generally have a * after it...
 */

special_whitespace		({space}+|{comment}{newline})
horiz_whitespace		({horiz_space}|{comment})
whitespace_with_newline	({horiz_whitespace}*{newline}{special_whitespace}*)

quote			'
/* If we see {quote} then {quotecontinue}, the quoted string continues */
quotecontinue	{whitespace_with_newline}{quote}

/*
 * {quotecontinuefail} is needed to avoid lexer backup when we fail to match
 * {quotecontinue}.  It might seem that this could just be {whitespace}*,
 * but if there's a dash after {whitespace_with_newline}, it must be consumed
 * to see if there's another dash --- which would start a {comment} and thus
 * allow continuation of the {quotecontinue} token.
 */
quotecontinuefail	{whitespace}*"-"?

/* Extended quote
 * xqdouble implements embedded quote, ''''
 */
xqstart			{quote}
xqdouble		{quote}{quote}
xqinside		[^']+

/* Double quote
 * Allows embedded spaces and other special characters into identifiers.
 */
dquote			\"
xdstart			{dquote}
xdstop			{dquote}
xddouble		{dquote}{dquote}
xdinside		[^"]+

digit			[0-9]
ident_start		[A-Za-z\200-\377_]
ident_cont		[A-Za-z\200-\377_0-9\$]

identifier		{ident_start}{ident_cont}*

decimal			(({digit}+)|({digit}*\.{digit}+)|({digit}+\.{digit}*))

other			.

%%

{whitespace}	{
					/* ignore */
				}


{xqstart}		{
					yyextra->saw_non_ascii = false;
					SET_YYLLOC();
					BEGIN(xq);
					startlit();
}
<xq>{quote} {
					/*
					 * When we are scanning a quoted string and see an end
					 * quote, we must look ahead for a possible continuation.
					 * If we don't see one, we know the end quote was in fact
					 * the end of the string.  To reduce the lexer table size,
					 * we use a single "xqs" state to do the lookahead for all
					 * types of strings.
					 */
					yyextra->state_before_str_stop = YYSTATE;
					BEGIN(xqs);
				}
<xqs>{quotecontinue} {
					/*
					 * Found a quote continuation, so return to the in-quote
					 * state and continue scanning the literal.  Nothing is
					 * added to the literal's contents.
					 */
					BEGIN(yyextra->state_before_str_stop);
				}
<xqs>{quotecontinuefail} |
<xqs>{other} |
<xqs><<EOF>>	{
					/*
					 * Failed to see a quote continuation.  Throw back
					 * everything after the end quote, and handle the string
					 * according to the state we were in previously.
					 */
					yyless(0);
					BEGIN(INITIAL);

					switch (yyextra->state_before_str_stop)
					{
						case xq:
							/*
							 * Check that the data remains valid, if it might
							 * have been made invalid by unescaping any chars.
							 */
							if (yyextra->saw_non_ascii)
								pg_verifymbstr(yyextra->literalbuf,
										yyextra->literallen,
										false);
							yylval->str = litbufdup(yyscanner);
							return SCONST;
						default:
							yyerror("unhandled previous state in xqs");
					}
				}

<xq>{xqdouble} {
					addlitchar('\'', yyscanner);
				}
<xq>{xqinside}  {
					addlit(yytext, yyleng, yyscanner);
				}
<xq><<EOF>>		{ yyerror("unterminated quoted string"); }


{xdstart}		{
					SET_YYLLOC();
					BEGIN(xd);
					startlit();
				}
<xd>{xdstop}	{
					char	   *ident;

					BEGIN(INITIAL);
					if (yyextra->literallen == 0)
						yyerror("zero-length delimited identifier");
					ident = litbufdup(yyscanner);
					if (yyextra->literallen >= NAMEDATALEN)
						truncate_identifier(ident, yyextra->literallen, true);
					yylval->str = ident;
					return IDENT;
				}
<xd>{xddouble}	{
					addlitchar('"', yyscanner);
				}
<xd>{xdinside}	{
					addlit(yytext, yyleng, yyscanner);
				}
<xd><<EOF>>		{ yyerror("unterminated quoted identifier"); }

{decimal}		{
					SET_YYLLOC();
					yylval->str = pstrdup(yytext);
					return FCONST;
				}

{identifier}	{
					const sqlol_ScanKeyword *keyword;
					char	   *ident;

					SET_YYLLOC();

					/* Is it a keyword? */
					keyword = sqlol_ScanKeywordLookup(yytext,
													yyextra->keywords,
													yyextra->num_keywords);
					if (keyword != NULL)
					{
						yylval->keyword = keyword->name;
						return keyword->value;
					}

					/*
					 * No.  Convert the identifier to lower case, and truncate
					 * if necessary.
					 */
					ident = downcase_truncate_identifier(yytext, yyleng, true);
					yylval->str = ident;
					return IDENT;
				}

{other}			{
					SET_YYLLOC();
					return yytext[0];
				}

<<EOF>>			{
					SET_YYLLOC();
					yyterminate();
				}

%%

/* LCOV_EXCL_STOP */

/*
 * Arrange access to yyextra for subroutines of the main yylex() function.
 * We expect each subroutine to have a yyscanner parameter.  Rather than
 * use the yyget_xxx functions, which might or might not get inlined by the
 * compiler, we cheat just a bit and cast yyscanner to the right type.
 */
#undef yyextra
#define yyextra  (((struct yyguts_t *) yyscanner)->yyextra_r)

/* Likewise for a couple of other things we need. */
#undef yylloc
#define yylloc	(((struct yyguts_t *) yyscanner)->yylloc_r)
#undef yyleng
#define yyleng	(((struct yyguts_t *) yyscanner)->yyleng_r)


/*
 * scanner_errposition
 *		Report a lexer or grammar error cursor position, if possible.
 *
 * This is expected to be used within an ereport() call.  The return value
 * is a dummy (always 0, in fact).
 *
 * Note that this can only be used for messages emitted during raw parsing
 * (essentially, sqlol_scan.l, sqlol_parser.c, sqlol_and gram.y), since it
 * requires the yyscanner struct to still be available.
 */
int
sqlol_scanner_errposition(int location, sqlol_yyscan_t yyscanner)
{
	int			pos;

	if (location < 0)
		return 0;				/* no-op if location is unknown */

	/* Convert byte offset to character number */
	pos = pg_mbstrlen_with_len(yyextra->scanbuf, location) + 1;
	/* And pass it to the ereport mechanism */
	return errposition(pos);
}

/*
 * scanner_yyerror
 *		Report a lexer or grammar error.
 *
 * Just ignore as we'll fallback to raw_parser().
 */
void
sqlol_scanner_yyerror(const char *message, sqlol_yyscan_t yyscanner)
{
	return;
}


/*
 * Called before any actual parsing is done
 */
sqlol_yyscan_t
sqlol_scanner_init(const char *str,
			 sqlol_yy_extra_type *yyext,
			 const sqlol_ScanKeyword *keywords,
			 int num_keywords,
			 int offset)
{
	YY_BUFFER_STATE state;
	Size		slen = strlen(str);
	yyscan_t	scanner;

	if (yylex_init(&scanner) != 0)
		elog(ERROR, "yylex_init() failed: %m");

	sqlol_yyset_extra(yyext, scanner);

	yyext->keywords = keywords;
	yyext->num_keywords = num_keywords;

	/*
	 * Make a scan buffer with special termination needed by flex.
	 */
	yyext->scanbuf = (char *) palloc(slen + 2);
	yyext->scanbuflen = slen;
	memcpy(yyext->scanbuf, str, slen);
	yyext->scanbuf[slen] = yyext->scanbuf[slen + 1] = YY_END_OF_BUFFER_CHAR;
	state = yy_scan_buffer(yyext->scanbuf, slen + 2, scanner);

	/* initialize literal buffer to a reasonable but expansible size */
	yyext->literalalloc = 1024;
	yyext->literalbuf = (char *) palloc(yyext->literalalloc);
	yyext->literallen = 0;

	/*
	 * Adjust the offset in the input string.  This is required in single-query
	 * mode, as we need to register the same token locations as we would have
	 * in normal mode with multi-statement query string.
	 */
	state->yy_buf_pos += offset;

	return scanner;
}


/*
 * Called after parsing is done to clean up after scanner_init()
 */
void
sqlol_scanner_finish(sqlol_yyscan_t yyscanner)
{
	/*
	 * We don't bother to call yylex_destroy(), because all it would do is
	 * pfree a small amount of control storage.  It's cheaper to leak the
	 * storage until the parsing context is destroyed.  The amount of space
	 * involved is usually negligible compared to the output parse tree
	 * anyway.
	 *
	 * We do bother to pfree the scanbuf and literal buffer, but only if they
	 * represent a nontrivial amount of space.  The 8K cutoff is arbitrary.
	 */
	if (yyextra->scanbuflen >= 8192)
		pfree(yyextra->scanbuf);
	if (yyextra->literalalloc >= 8192)
		pfree(yyextra->literalbuf);
}


static void
addlit(char *ytext, int yleng, sqlol_yyscan_t yyscanner)
{
	/* enlarge buffer if needed */
	if ((yyextra->literallen + yleng) >= yyextra->literalalloc)
	{
		do
		{
			yyextra->literalalloc *= 2;
		} while ((yyextra->literallen + yleng) >= yyextra->literalalloc);
		yyextra->literalbuf = (char *) repalloc(yyextra->literalbuf,
												yyextra->literalalloc);
	}
	/* append new data */
	memcpy(yyextra->literalbuf + yyextra->literallen, ytext, yleng);
	yyextra->literallen += yleng;
}


static void
addlitchar(unsigned char ychar, sqlol_yyscan_t yyscanner)
{
	/* enlarge buffer if needed */
	if ((yyextra->literallen + 1) >= yyextra->literalalloc)
	{
		yyextra->literalalloc *= 2;
		yyextra->literalbuf = (char *) repalloc(yyextra->literalbuf,
												yyextra->literalalloc);
	}
	/* append new data */
	yyextra->literalbuf[yyextra->literallen] = ychar;
	yyextra->literallen += 1;
}


/*
 * Create a palloc'd copy of literalbuf, adding a trailing null.
 */
static char *
litbufdup(sqlol_yyscan_t yyscanner)
{
	int			llen = yyextra->literallen;
	char	   *new;

	new = palloc(llen + 1);
	memcpy(new, yyextra->literalbuf, llen);
	new[llen] = '\0';
	return new;
}

/*
 * Interface functions to make flex use palloc() instead of malloc().
 * It'd be better to make these static, but flex insists otherwise.
 */

void *
sqlol_yyalloc(yy_size_t bytes, sqlol_yyscan_t yyscanner)
{
	return palloc(bytes);
}

void *
sqlol_yyrealloc(void *ptr, yy_size_t bytes, sqlol_yyscan_t yyscanner)
{
	if (ptr)
		return repalloc(ptr, bytes);
	else
		return palloc(bytes);
}

void
sqlol_yyfree(void *ptr, sqlol_yyscan_t yyscanner)
{
	if (ptr)
		pfree(ptr);
}
