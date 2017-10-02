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
	XPATH_TOKEN_OTHER
}	XPathTokenType;

typedef struct XPathTokenInfo
{
	XPathTokenType ttype;
	char	   *start;
	int			length;
}	XPathTokenInfo;

#define TOKEN_STACK_SIZE		10

typedef struct ParserData
{
	char	   *str;
	char	   *cur;
	XPathTokenInfo stack[TOKEN_STACK_SIZE];
	int			stack_length;
}	XPathParserData;

/* Any high-bit-set character is OK (might be part of a multibyte char) */
#define NODENAME_FIRSTCHAR(c)	 ((c) == '_' || (c) == '-' || \
								 ((c) >= 'A' && (c) <= 'Z') || \
								 ((c) >= 'a' && (c) <= 'z') || \
								 (IS_HIGHBIT_SET(c)))

#define IS_NODENAME_CHAR(c)		(NODENAME_FIRSTCHAR(c) || (c) == '.' || \
								 ((c) >= '0' && (c) <= '9'))


/*
 * Returns next char after last char of token - XPath lexer
 */
static char *
getXPathToken(char *str, XPathTokenInfo * ti)
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
		else if (NODENAME_FIRSTCHAR(c))
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
initXPathParser(XPathParserData * parser, char *str)
{
	parser->str = str;
	parser->cur = str;
	parser->stack_length = 0;
}

/*
 * Returns token from stack or read token
 */
static void
nextXPathToken(XPathParserData * parser, XPathTokenInfo * ti)
{
	if (parser->stack_length > 0)
		memcpy(ti, &parser->stack[--parser->stack_length],
			   sizeof(XPathTokenInfo));
	else
		parser->cur = getXPathToken(parser->cur, ti);
}

/*
 * Push token to stack
 */
static void
pushXPathToken(XPathParserData * parser, XPathTokenInfo * ti)
{
	if (parser->stack_length == TOKEN_STACK_SIZE)
		elog(ERROR, "internal error");
	memcpy(&parser->stack[parser->stack_length++], ti,
		   sizeof(XPathTokenInfo));
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
	bool		last_token_is_name = false;

	nextXPathToken(parser, &t1);

	while (t1.ttype != XPATH_TOKEN_NONE)
	{
		switch (t1.ttype)
		{
			case XPATH_TOKEN_NUMBER:
			case XPATH_TOKEN_STRING:
				last_token_is_name = false;
				writeXPathToken(str, &t1);
				nextXPathToken(parser, &t1);
				break;

			case XPATH_TOKEN_NAME:
				{
					bool		is_qual_name = false;

					/* inside predicate ignore keywords "and" "or" */
					if (inside_predicate)
					{
						if ((strncmp(t1.start, "and", 3) == 0 && t1.length == 3) ||
						 (strncmp(t1.start, "or", 2) == 0 && t1.length == 2))
						{
							writeXPathToken(str, &t1);
							nextXPathToken(parser, &t1);
							break;
						}
					}

					last_token_is_name = true;
					nextXPathToken(parser, &t2);
					if (t2.ttype == XPATH_TOKEN_OTHER)
					{
						if (*t2.start == '(')
							last_token_is_name = false;
						else if (*t2.start == ':')
							is_qual_name = true;
					}

					if (last_token_is_name && !is_qual_name && def_namespace_name != NULL)
						appendStringInfo(str, "%s:", def_namespace_name);

					writeXPathToken(str, &t1);

					if (is_qual_name)
					{
						writeXPathToken(str, &t2);
						nextXPathToken(parser, &t1);
						if (t1.ttype == XPATH_TOKEN_NAME)
							writeXPathToken(str, &t1);
						else
							pushXPathToken(parser, &t1);
					}
					else
						pushXPathToken(parser, &t2);

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
						last_token_is_name = false;

						if (c == ']' && inside_predicate)
							return;

						else if (c == '@')
						{
							nextXPathToken(parser, &t1);
							if (t1.ttype == XPATH_TOKEN_NAME)
							{
								bool		is_qual_name = false;

								/*
								 * A default namespace declaration applies to all
								 * unprefixed element names within its scope. Default
								 * namespace declarations do not apply directly to
								 * attribute names; the interpretation of unprefixed
								 * attributes is determined by the element on which
								 * they appear.
								 */
								nextXPathToken(parser, &t2);
								if (t2.ttype == XPATH_TOKEN_OTHER && *t2.start == ':')
									is_qual_name = true;

								writeXPathToken(str, &t1);
								if (is_qual_name)
								{
									writeXPathToken(str, &t2);
									nextXPathToken(parser, &t1);
									if (t1.ttype == XPATH_TOKEN_NAME)
										writeXPathToken(str, &t1);
									else
										pushXPathToken(parser, &t1);
								}
								else
									pushXPathToken(parser, &t2);
							}
							else
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
transformXPath(StringInfo str, char *xpath,
			   char *def_namespace_name)
{
	XPathParserData parser;

	initStringInfo(str);
	initXPathParser(&parser, xpath);
	_transformXPath(str, &parser, false, def_namespace_name);
}

#endif
