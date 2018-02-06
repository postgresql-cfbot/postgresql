/*-------------------------------------------------------------------------
 *
 * xpath_parser.c
 *	  XML XPath parser.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/utils/adt/xpath_parser.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/xpath_parser.h"

/*
 * All PostgreSQL XML related functionality is based on libxml2 library, and
 * XPath support is not an exception.  However, libxml2 doesn't support
 * default namespace for XPath expressions. Because there are not any API
 * how to transform or access to parsed XPath expression we have to parse
 * XPath here.
 *
 * Those functionalities are implemented with a simple XPath parser/
 * preprocessor.  This XPath parser transforms a XPath expression to another
 * XPath expression that can be used by libxml2 XPath evaluation. It doesn't
 * replace libxml2 XPath parser or libxml2 XPath expression evaluation.
 */

#ifdef USE_LIBXML

/*
 * We need to work with XPath expression tokens.  When expression starting with
 * nodename, then we can use prefix.  When default namespace is defined, then we
 * should to enhance any nodename and attribute without namespace by default
 * namespace.
 */

typedef enum
{
	XPATH_TOKEN_NONE,
	XPATH_TOKEN_NAME,
	XPATH_TOKEN_STRING,
	XPATH_TOKEN_NUMBER,
	XPATH_TOKEN_COLON,
	XPATH_TOKEN_DCOLON,
	XPATH_TOKEN_OTHER
}	XPathTokenType;

typedef struct XPathTokenInfo
{
	XPathTokenType ttype;
	const char	   *start;
	int			length;
}	XPathTokenInfo;

typedef struct ParserData
{
	const char	   *str;
	const char	   *cur;
	XPathTokenInfo buffer;
	bool		buffer_is_empty;
}	XPathParserData;

/* Any high-bit-set character is OK (might be part of a multibyte char) */
#define IS_NODENAME_FIRSTCHAR(c)	 ((c) == '_' || \
								 ((c) >= 'A' && (c) <= 'Z') || \
								 ((c) >= 'a' && (c) <= 'z') || \
								 (IS_HIGHBIT_SET(c)))

#define IS_NODENAME_CHAR(c)		(IS_NODENAME_FIRSTCHAR(c) || (c) == '-' || (c) == '.' || \
								 ((c) >= '0' && (c) <= '9'))

#define TOKEN_IS_EMPTY(t)		((t).ttype == XPATH_TOKEN_NONE)

/*
 * Returns next char after last char of token - XPath lexer
 */
static const char *
getXPathToken(const char *str, XPathTokenInfo * ti)
{
	/* skip initial spaces */
	while (*str == ' ')
		str++;

	if (*str != '\0')
	{
		char		c = *str;

		ti->start = str++;

		if (c >= '0' && c <= '9')
		{
			while (*str >= '0' && *str <= '9')
				str++;
			if (*str == '.')
			{
				str++;
				while (*str >= '0' && *str <= '9')
					str++;
			}
			ti->ttype = XPATH_TOKEN_NUMBER;
		}
		else if (IS_NODENAME_FIRSTCHAR(c))
		{
			while (IS_NODENAME_CHAR(*str))
				str++;

			ti->ttype = XPATH_TOKEN_NAME;
		}
		else if (c == '"')
		{
			while (*str != '\0')
				if (*str++ == '"')
					break;

			ti->ttype = XPATH_TOKEN_STRING;
		}
		else if (c == '\'')
		{
			while (*str != '\0')
				if (*str++ == '\'')
					break;

			ti->ttype = XPATH_TOKEN_STRING;
		}
		else if (c == ':')
		{
			/* look ahead to detect a double-colon */
			if (*str == ':')
			{
				ti->ttype = XPATH_TOKEN_DCOLON;
				str++;
			}
			else
				ti->ttype = XPATH_TOKEN_COLON;
		}
		else
			ti->ttype = XPATH_TOKEN_OTHER;

		ti->length = str - ti->start;
	}
	else
	{
		ti->start = NULL;
		ti->length = 0;

		ti->ttype = XPATH_TOKEN_NONE;
	}

	return str;
}

/*
 * reset XPath parser stack
 */
static void
initXPathParser(XPathParserData * parser, const char *str)
{
	parser->str = str;
	parser->cur = str;
	parser->buffer_is_empty = true;
}

/*
 * Returns token from stack or read token
 */
static void
nextXPathToken(XPathParserData * parser, XPathTokenInfo * ti)
{
	if (!parser->buffer_is_empty)
	{
		memcpy(ti, &parser->buffer, sizeof(XPathTokenInfo));
		parser->buffer_is_empty = true;
	}
	else
		parser->cur = getXPathToken(parser->cur, ti);
}

/*
 * Push token to stack
 */
static void
pushXPathToken(XPathParserData * parser, XPathTokenInfo * ti)
{
	if (!parser->buffer_is_empty)
		elog(ERROR, "internal error");

	memcpy(&parser->buffer, ti, sizeof(XPathTokenInfo));
	parser->buffer_is_empty = false;
	ti->ttype = XPATH_TOKEN_NONE;
}

/*
 * Write token to output string
 */
static void
writeXPathToken(StringInfo str, XPathTokenInfo * ti)
{
	Assert(ti->ttype != XPATH_TOKEN_NONE);

	if (ti->ttype != XPATH_TOKEN_OTHER)
		appendBinaryStringInfo(str, ti->start, ti->length);
	else
		appendStringInfoChar(str, *ti->start);

	ti->ttype = XPATH_TOKEN_NONE;
}

/*
 * This is main part of XPath transformation. It can be called recursivly,
 * when XPath expression contains predicates.
 */
static void
_transformXPath(StringInfo str, XPathParserData * parser,
				bool inside_predicate,
				char *def_namespace_name)
{
	XPathTokenInfo t1,
				t2;
	bool		tagname_needs_defnsp;
	bool		token_is_tagattrib = false;

	nextXPathToken(parser, &t1);

	while (t1.ttype != XPATH_TOKEN_NONE)
	{
		switch (t1.ttype)
		{
			case XPATH_TOKEN_NUMBER:
			case XPATH_TOKEN_STRING:
			case XPATH_TOKEN_COLON:
			case XPATH_TOKEN_DCOLON:
				/* write without any changes */
				writeXPathToken(str, &t1);
				/* process fresh token */
				nextXPathToken(parser, &t1);
				break;

			case XPATH_TOKEN_NAME:
				{
					/*
					 * Inside predicate ignore keywords (literal operators)
					 * "and" "or" "div" and "mod".
					 */
					if (inside_predicate)
					{
						if ((strncmp(t1.start, "and", 3) == 0 && t1.length == 3) ||
						 (strncmp(t1.start, "or", 2) == 0 && t1.length == 2) ||
						 (strncmp(t1.start, "div", 3) == 0 && t1.length == 3) ||
						 (strncmp(t1.start, "mod", 3) == 0 && t1.length == 3))
						{
							token_is_tagattrib = false;

							/* keyword */
							writeXPathToken(str, &t1);
							/* process fresh token */
							nextXPathToken(parser, &t1);
							break;
						}
					}

					tagname_needs_defnsp = true;

					nextXPathToken(parser, &t2);
					if (t2.ttype == XPATH_TOKEN_COLON)
					{
						/* t1 is a quilified node name. no need to add default one. */
						tagname_needs_defnsp = false;

						/* namespace name */
						writeXPathToken(str, &t1);
						/* colon */
						writeXPathToken(str, &t2);
						/* get node name */
						nextXPathToken(parser, &t1);
					}
					else if (t2.ttype == XPATH_TOKEN_DCOLON)
					{
						/* t1 is an axis name. write out as it is */
						if (strncmp(t1.start, "attribute", 9) == 0 && t1.length == 9)
							token_is_tagattrib = true;

						/* axis name */
						writeXPathToken(str, &t1);
						/* double colon */
						writeXPathToken(str, &t2);

						/*
						 * The next token may be qualified tag name, process
						 * it as a fresh token.
						 */
						nextXPathToken(parser, &t1);
						break;
					}
					else if (t2.ttype == XPATH_TOKEN_OTHER)
					{
						/* function name doesn't require namespace */
						if (*t2.start == '(')
							tagname_needs_defnsp = false;
						else
							pushXPathToken(parser, &t2);
					}

					if (tagname_needs_defnsp && !token_is_tagattrib)
						appendStringInfo(str, "%s:", def_namespace_name);

					token_is_tagattrib = false;

					/* write maybe-tagname if not consumed yet */
					if (!TOKEN_IS_EMPTY(t1))
						writeXPathToken(str, &t1);

					/* output t2 if not consumed yet */
					if (!TOKEN_IS_EMPTY(t2))
						writeXPathToken(str, &t2);

					nextXPathToken(parser, &t1);
				}
				break;

			case XPATH_TOKEN_OTHER:
				{
					char		c = *t1.start;

					writeXPathToken(str, &t1);

					if (c == '[')
						_transformXPath(str, parser, true, def_namespace_name);
					else
					{
						if (c == ']' && inside_predicate)
						{
							return;
						}
						else if (c == '@')
						{
							nextXPathToken(parser, &t1);
							if (t1.ttype == XPATH_TOKEN_NAME)
								token_is_tagattrib = true;

							pushXPathToken(parser, &t1);
						}
					}
					nextXPathToken(parser, &t1);
				}
				break;

			case XPATH_TOKEN_NONE:
				elog(ERROR, "should not be here");
		}
	}
}

void
transformXPath(StringInfo str, const char *xpath,
			   char *def_namespace_name)
{
	XPathParserData parser;

	Assert(def_namespace_name != NULL);

	initStringInfo(str);
	initXPathParser(&parser, xpath);
	_transformXPath(str, &parser, false, def_namespace_name);

	elog(DEBUG1, "apply default namespace \"%s\"", str->data);
}

#endif
