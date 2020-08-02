/*
 * in/out function for ltree and lquery
 * Teodor Sigaev <teodor@stack.net>
 * contrib/ltree/ltree_io.c
 */
#include "postgres.h"

#include <ctype.h>

#include "crc32.h"
#include "libpq/pqformat.h"
#include "ltree.h"
#include "utils/memutils.h"


typedef struct
{
	const char *start;
	int			len;			/* length in bytes */
	int			flag;
	int			wlen;			/* length in characters */
} nodeitem;

#define LTPRS_WAITNAME	0
#define LTPRS_WAITDELIM 1

static void finish_nodeitem(nodeitem *lptr, const char *ptr, int len, int wlen,
							int escapes, bool is_lquery, int pos);


/*
 * Calculating the number of literals in the string to be parsed.
 *
 * For ltree, returns a number of not escaped delimiters (dots).  If pORs is
 * not NULL, calculates the number of alternate templates (used in lquery
 * parsing).  The function can return more levels than is really necessesary,
 * it will be corrected during the real parsing process.
 */
static void
count_parts_ors(const char *ptr, int *plevels, int *pORs)
{
	bool		quote = false;
	bool		escaping = false;

	while (*ptr)
	{
		if (escaping)
			escaping = false;
		else if (t_iseq(ptr, '\\'))
			escaping = true;
		else if (quote)
		{
			if (t_iseq(ptr, '"'))
				quote = false;
		}
		else
		{
			if (t_iseq(ptr, '"'))
				quote = true;
			else if (t_iseq(ptr, '.'))
				(*plevels)++;
			else if (t_iseq(ptr, '|') && pORs != NULL)
				(*pORs)++;
		}

		ptr += pg_mblen(ptr);
	}

	(*plevels)++;
	if (pORs != NULL)
		(*pORs)++;
}

/*
 * Char-by-char copying from "src" to "dst" representation removing escaping.
 * Total amount of copied bytes is "len".
 */
void
copy_unescaped(char *dst, const char *src, int len)
{
	const char *dst_end = dst + len;
	bool		escaping = false;

	while (dst < dst_end && *src)
	{
		int			charlen;

		if (t_iseq(src, '\\') && !escaping)
		{
			escaping = true;
			src++;
			continue;
		}

		charlen = pg_mblen(src);

		if (dst + charlen > dst_end)
			elog(ERROR, "internal error during splitting levels");

		memcpy(dst, src, charlen);
		src += charlen;
		dst += charlen;
		escaping = false;
	}

	if (dst != dst_end)
		elog(ERROR, "internal error during splitting levels");
}

static bool
is_quoted_char(const char *ptr)
{
	return !(t_isalpha(ptr) || t_isdigit(ptr) || t_iseq(ptr, '_'));
}

static bool
is_escaped_char(const char *ptr)
{
	return t_iseq(ptr, '"') || t_iseq(ptr, '\\');
}

/*
 * Function calculating extra bytes needed for quoting/escaping of special
 * characters.
 *
 * If there are no special characters, return 0.
 * If there are any special symbol, we need initial and final quote, return 2.
 * If there are any quotes or backslashes, we need to escape all of them and
 * also initial and final quote, so return 2 + number of quotes/backslashes.
 */
int
extra_bytes_for_escaping(const char *start, const int len)
{
	const char *ptr = start;
	const char *end = start + len;
	int			escapes = 0;
	bool		quotes = false;

	if (len == 0)
		return 2;

	while (ptr < end && *ptr)
	{
		if (is_escaped_char(ptr))
			escapes++;
		else if (is_quoted_char(ptr))
			quotes = true;

		ptr += pg_mblen(ptr);
	}

	if (ptr > end)
		elog(ERROR, "internal error during merging levels");

	return (escapes > 0) ? escapes + 2 : quotes ? 2 : 0;
}

/*
 * Copy "src" to "dst" escaping backslashes and quotes.
 *
 * Return number of escaped characters.
 */
static int
copy_escaped(char *dst, const char *src, int len)
{
	const char *src_end = src + len;
	int			escapes = 0;

	while (src < src_end && *src)
	{
		int			charlen = pg_mblen(src);

		if (is_escaped_char(src))
		{
			*dst++ = '\\';
			escapes++;
		}

		if (src + charlen > src_end)
			elog(ERROR, "internal error during merging levels");

		memcpy(dst, src, charlen);
		src += charlen;
		dst += charlen;
	}

	return escapes;
}

/*
 * Copy "src" to "dst" possibly adding surrounding quotes and escaping
 * backslashes and internal quotes.
 *
 * "extra_bytes" is a value calculated by extra_bytes_for_escaping().
 */
void
copy_level(char *dst, const char *src, int len, int extra_bytes)
{
	if (extra_bytes == 0)	/* no quotes and escaping */
		memcpy(dst, src, len);
	else if (extra_bytes == 2)	/* only quotes, no escaping */
	{
		*dst = '"';
		memcpy(dst + 1, src, len);
		dst[len + 1] = '"';
	}
	else	/* quotes and escaping */
	{
		*dst = '"';
		copy_escaped(dst + 1, src, len);
		dst[len + extra_bytes - 1] = '"';
	}
}

/*
 * Read next token from input string "str".
 *
 * Output parameteres:
 *   "len" - token length in bytes.
 *   "wlen" - token length in characters.
 *   "escaped_count" - number of escaped characters in LTREE_TOK_LABEL token.
 */
ltree_token
ltree_get_token(const char *str, const char *datatype_name, int pos,
				int *len, int *wlen, int *escaped_count)
{
	const char *ptr = str;
	int			charlen;
	bool		quoted = false;
	bool		escaped = false;

	*escaped_count = 0;
	*len = 0;
	*wlen = 0;

	if (!*ptr)
		return LTREE_TOK_END;

	charlen = pg_mblen(ptr);

	if (t_isspace(ptr))
	{
		++*wlen;
		ptr += charlen;

		while (*ptr && t_isspace(ptr))
		{
			ptr += pg_mblen(ptr);
			++*wlen;
		}

		*len = ptr - str;
		return LTREE_TOK_SPACE;
	}

	if (charlen == 1 && strchr(".*!|&@%{}(),", *ptr))
	{
		*wlen = *len = 1;

		if (t_iseq(ptr, '.'))
			return LTREE_TOK_DOT;
		else if (t_iseq(ptr, '*'))
			return LTREE_TOK_ASTERISK;
		else if (t_iseq(ptr, '!'))
			return LTREE_TOK_NOT;
		else if (t_iseq(ptr, '|'))
			return LTREE_TOK_OR;
		else if (t_iseq(ptr, '&'))
			return LTREE_TOK_AND;
		else if (t_iseq(ptr, '@'))
			return LTREE_TOK_AT;
		else if (t_iseq(ptr, '%'))
			return LTREE_TOK_PERCENT;
		else if (t_iseq(ptr, ','))
			return LTREE_TOK_COMMA;
		else if (t_iseq(ptr, '{'))
			return LTREE_TOK_LBRACE;
		else if (t_iseq(ptr, '}'))
			return LTREE_TOK_RBRACE;
		else if (t_iseq(ptr, '('))
			return LTREE_TOK_LPAREN;
		else if (t_iseq(ptr, ')'))
			return LTREE_TOK_RPAREN;
		else
			elog(ERROR, "invalid special character");
	}
	else if (t_iseq(ptr, '\\'))
		escaped = true;
	else if (t_iseq(ptr, '"'))
		quoted = true;
	else if (is_quoted_char(ptr))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("%s syntax error at character %d", datatype_name, pos),
				 errdetail("Unexpected character")));

	for (ptr += charlen, ++*wlen; *ptr; ptr += charlen, ++*wlen)
	{
		charlen = pg_mblen(ptr);

		if (escaped)
		{
			++*escaped_count;
			escaped = false;
		}
		else if (t_iseq(ptr, '\\'))
			escaped = true;
		else if (quoted)
		{
			if (t_iseq(ptr, '"'))
			{
				quoted = false;
				ptr += charlen;
				++*wlen;
				break;
			}
		}
		else if (is_quoted_char(ptr))
			break;
	}

	if (quoted)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("%s syntax error at character %d",
						datatype_name, pos),
				 errdetail("Unclosed quote")));

	if (escaped)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("%s syntax error at character %d",
						datatype_name, pos + *wlen - 1),
				 errdetail("Unclosed escape sequence")));

	*len = ptr - str;

	return LTREE_TOK_LABEL;
}

/*
 * expects a null terminated string
 * returns an ltree
 */
static ltree *
parse_ltree(const char *buf)
{
	const char *ptr;
	nodeitem   *list,
			   *lptr;
	int			levels = 0,
				totallen = 0;
	int			state = LTPRS_WAITNAME;
	ltree	   *result;
	ltree_level *curlevel;
	int			len;
	int			wlen;
	int			pos = 1;		/* character position for error messages */

#define UNCHAR ereport(ERROR, \
					   errcode(ERRCODE_SYNTAX_ERROR), \
					   errmsg("ltree syntax error at character %d", \
							  pos))

	ptr = buf;
	count_parts_ors(ptr, &levels, NULL);

	if (levels > LTREE_MAX_LEVELS)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("number of ltree labels (%d) exceeds the maximum allowed (%d)",
						levels, LTREE_MAX_LEVELS)));
	list = lptr = (nodeitem *) palloc(sizeof(nodeitem) * (levels));

	/*
	 * This block calculates single nodes' settings
	 */
	for (ptr = buf; *ptr; ptr += len, pos += wlen)
	{
		int			escaped_count;
		ltree_token tok = ltree_get_token(ptr, "ltree", pos,
										  &len, &wlen, &escaped_count);

		if (tok == LTREE_TOK_SPACE)
			continue;

		switch (state)
		{
			case LTPRS_WAITNAME:
				if (tok != LTREE_TOK_LABEL)
					UNCHAR;

				finish_nodeitem(lptr, ptr, len, wlen, escaped_count, false, pos);
				totallen += MAXALIGN(lptr->len + LEVEL_HDRSIZE);
				lptr++;

				state = LTPRS_WAITDELIM;
				break;
			case LTPRS_WAITDELIM:
				if (tok != LTREE_TOK_DOT)
					UNCHAR;

				state = LTPRS_WAITNAME;
				break;
			default:
				elog(ERROR, "internal error in ltree parser");
		}
	}

	if (state == LTPRS_WAITNAME && lptr != list)	/* Empty string */
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("ltree syntax error"),
				 errdetail("Unexpected end of input.")));

	result = (ltree *) palloc0(LTREE_HDRSIZE + totallen);
	SET_VARSIZE(result, LTREE_HDRSIZE + totallen);
	result->numlevel = lptr - list;
	curlevel = LTREE_FIRST(result);
	lptr = list;
	while (lptr - list < result->numlevel)
	{
		curlevel->len = (uint16) lptr->len;

		if (lptr->len > 0)
			copy_unescaped(curlevel->name, lptr->start, lptr->len);

		curlevel = LEVEL_NEXT(curlevel);
		lptr++;
	}

	pfree(list);
	return result;

#undef UNCHAR
}

/*
 * expects an ltree
 * returns a null terminated string
 */
static char *
deparse_ltree(const ltree *in)
{
	char	   *buf,
			   *end,
			   *ptr;
	int			i;
	ltree_level *curlevel;
	Size		allocated = VARSIZE(in);

	ptr = buf = (char *) palloc(allocated);
	end = buf + allocated;
	curlevel = LTREE_FIRST(in);

	for (i = 0; i < in->numlevel; i++)
	{
		int			extra_bytes = extra_bytes_for_escaping(curlevel->name,
														   curlevel->len);
		int			level_len = curlevel->len + extra_bytes;

		if (ptr + level_len + 1 >= end)
		{
			char	   *old_buf = buf;

			allocated += (level_len + 1) * 2;
			buf = repalloc(buf, allocated);
			ptr = buf + (ptr - old_buf);
		}

		if (i != 0)
		{
			*ptr = '.';
			ptr++;
		}

		copy_level(ptr, curlevel->name, curlevel->len, extra_bytes);
		ptr += level_len;

		curlevel = LEVEL_NEXT(curlevel);
	}

	*ptr = '\0';
	return buf;
}

/*
 * Basic ltree I/O functions
 */
PG_FUNCTION_INFO_V1(ltree_in);
Datum
ltree_in(PG_FUNCTION_ARGS)
{
	char	   *buf = (char *) PG_GETARG_POINTER(0);

	PG_RETURN_POINTER(parse_ltree(buf));
}

PG_FUNCTION_INFO_V1(ltree_out);
Datum
ltree_out(PG_FUNCTION_ARGS)
{
	ltree	   *in = PG_GETARG_LTREE_P(0);

	PG_RETURN_POINTER(deparse_ltree(in));
}

/*
 * ltree type send function
 *
 * The type is sent as text in binary mode, so this is almost the same
 * as the output function, but it's prefixed with a version number so we
 * can change the binary format sent in future if necessary. For now,
 * only version 1 is supported.
 */
PG_FUNCTION_INFO_V1(ltree_send);
Datum
ltree_send(PG_FUNCTION_ARGS)
{
	ltree	   *in = PG_GETARG_LTREE_P(0);
	StringInfoData buf;
	int			version = 1;
	char	   *res = deparse_ltree(in);

	pq_begintypsend(&buf);
	pq_sendint8(&buf, version);
	pq_sendtext(&buf, res, strlen(res));
	pfree(res);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * ltree type recv function
 *
 * The type is sent as text in binary mode, so this is almost the same
 * as the input function, but it's prefixed with a version number so we
 * can change the binary format sent in future if necessary. For now,
 * only version 1 is supported.
 */
PG_FUNCTION_INFO_V1(ltree_recv);
Datum
ltree_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	int			version = pq_getmsgint(buf, 1);
	char	   *str;
	int			nbytes;
	ltree	   *res;

	if (version != 1)
		elog(ERROR, "unsupported ltree version number %d", version);

	str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
	res = parse_ltree(str);
	pfree(str);

	PG_RETURN_POINTER(res);
}


#define LQPRS_WAITLEVEL 0
#define LQPRS_WAITDELIM 1
#define LQPRS_WAITOPEN	2
#define LQPRS_WAITFNUM	3
#define LQPRS_WAITSNUM	4
#define LQPRS_WAITND	5
#define LQPRS_WAITCLOSE 6
#define LQPRS_WAITEND	7
#define LQPRS_WAITVAR	8


#define GETVAR(x) ( *((nodeitem**)LQL_FIRST(x)) )
#define ITEMSIZE	MAXALIGN(LQL_HDRSIZE+sizeof(nodeitem*))
#define NEXTLEV(x) ( (lquery_level*)( ((char*)(x)) + ITEMSIZE) )

/*
 * expects a null terminated string
 * returns an lquery
 */
static lquery *
parse_lquery(const char *buf)
{
	const char *ptr;
	int			levels = 0,
				totallen = 0,
				numOR = 0;
	int			state = LQPRS_WAITLEVEL;
	lquery	   *result;
	nodeitem   *lptr = NULL;
	lquery_level *cur,
			   *curqlevel,
			   *tmpql;
	lquery_variant *lrptr = NULL;
	bool		hasnot = false;
	bool		wasbad = false;
	int			real_levels = 0;
	int			pos = 1;		/* character position for error messages */
	int			wlen;			/* token length in characters */
	int			len;			/* token length in bytes */

#define UNCHAR ereport(ERROR, \
					   errcode(ERRCODE_SYNTAX_ERROR), \
					   errmsg("lquery syntax error at character %d", \
							  pos))

	count_parts_ors(buf, &levels, &numOR);

	if (levels > LQUERY_MAX_LEVELS)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("number of lquery items (%d) exceeds the maximum allowed (%d)",
						levels, LQUERY_MAX_LEVELS)));
	curqlevel = tmpql = (lquery_level *) palloc0(ITEMSIZE * levels);

	for (ptr = buf; *ptr; ptr += len, pos += wlen)
	{
		int			escaped_count;
		ltree_token tok = ltree_get_token(ptr, "lquery", pos,
										  &len, &wlen, &escaped_count);

		switch (state)
		{
			case LQPRS_WAITLEVEL:
				if (tok == LTREE_TOK_SPACE)
					break;

				if (tok == LTREE_TOK_NOT)
				{
					if (curqlevel->flag & LQL_NOT)	/* '!!' is disallowed */
						UNCHAR;

					curqlevel->flag |= LQL_NOT;
					hasnot = true;
					break;
				}

				real_levels++;

				GETVAR(curqlevel) = lptr = (nodeitem *) palloc0(sizeof(nodeitem) * numOR);

				if (tok == LTREE_TOK_LABEL)
				{
					curqlevel->numvar = 1;
					finish_nodeitem(lptr, ptr, len, wlen, escaped_count, true, pos);
					state = LQPRS_WAITDELIM;
				}
				else if (tok == LTREE_TOK_ASTERISK)
				{
					if (curqlevel->flag & LQL_NOT)	/* '!*' is meaningless */
						UNCHAR;

					lptr->start = ptr;

					curqlevel->low = 0;
					curqlevel->high = LTREE_MAX_LEVELS;

					state = LQPRS_WAITOPEN;
				}
				else
					UNCHAR;

				break;
			case LQPRS_WAITVAR:
				if (tok == LTREE_TOK_SPACE)
					break;

				if (tok != LTREE_TOK_LABEL)
					UNCHAR;

				curqlevel->numvar++;

				lptr++;
				finish_nodeitem(lptr, ptr, len, wlen, escaped_count, true, pos);

				state = LQPRS_WAITDELIM;
				break;
			case LQPRS_WAITDELIM:
				if (tok == LTREE_TOK_SPACE)
					break;
				else if (tok == LTREE_TOK_AT)
				{
					lptr->flag |= LVAR_INCASE;
					curqlevel->flag |= LVAR_INCASE;
				}
				else if (tok == LTREE_TOK_ASTERISK)
				{
					lptr->flag |= LVAR_ANYEND;
					curqlevel->flag |= LVAR_ANYEND;
				}
				else if (tok == LTREE_TOK_PERCENT)
				{
					lptr->flag |= LVAR_SUBLEXEME;
					curqlevel->flag |= LVAR_SUBLEXEME;
				}
				else if (tok == LTREE_TOK_OR)
				{
					state = LQPRS_WAITVAR;
				}
				else if (tok == LTREE_TOK_LBRACE)
				{
					curqlevel->flag |= LQL_COUNT;
					state = LQPRS_WAITFNUM;
				}
				else if (tok == LTREE_TOK_DOT)
				{
					state = LQPRS_WAITLEVEL;
					curqlevel = NEXTLEV(curqlevel);
				}
				else
					UNCHAR;
				break;
			case LQPRS_WAITOPEN:
				if (tok == LTREE_TOK_SPACE)
					break;
				else if (tok == LTREE_TOK_LBRACE)
					state = LQPRS_WAITFNUM;
				else if (tok == LTREE_TOK_DOT)
				{
					/* We only get here for '*', so these are correct defaults */
					curqlevel->low = 0;
					curqlevel->high = LTREE_MAX_LEVELS;
					curqlevel = NEXTLEV(curqlevel);
					state = LQPRS_WAITLEVEL;
				}
				else
					UNCHAR;
				break;
			case LQPRS_WAITFNUM:
				if (tok == LTREE_TOK_COMMA)
					state = LQPRS_WAITSNUM;
				else if (t_isdigit(ptr))
				{
					int			low = atoi(ptr);

					if (low < 0 || low > LTREE_MAX_LEVELS)
						ereport(ERROR,
								(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
								 errmsg("lquery syntax error"),
								 errdetail("Low limit (%d) exceeds the maximum allowed (%d), at character %d.",
										   low, LTREE_MAX_LEVELS, pos)));

					curqlevel->low = (uint16) low;
					len = wlen = 1;
					state = LQPRS_WAITND;
				}
				else
					UNCHAR;
				break;
			case LQPRS_WAITSNUM:
				if (t_isdigit(ptr))
				{
					int			high = atoi(ptr);
					
					if (high < 0 || high > LTREE_MAX_LEVELS)
						ereport(ERROR,
								(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
								 errmsg("lquery syntax error"),
								 errdetail("High limit (%d) exceeds the maximum allowed (%d), at character %d.",
										   high, LTREE_MAX_LEVELS, pos)));
					else if (curqlevel->low > high)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("lquery syntax error"),
								 errdetail("Low limit (%d) is greater than high limit (%d), at character %d.",
										   curqlevel->low, high, pos)));

					curqlevel->high = (uint16) high;
					len = wlen = 1;
					state = LQPRS_WAITCLOSE;
				}
				else if (tok == LTREE_TOK_RBRACE)
				{
					curqlevel->high = LTREE_MAX_LEVELS;
					state = LQPRS_WAITEND;
				}
				else
					UNCHAR;
				break;
			case LQPRS_WAITCLOSE:
				if (tok == LTREE_TOK_RBRACE)
					state = LQPRS_WAITEND;
				else if (!t_isdigit(ptr))
					UNCHAR;
				break;
			case LQPRS_WAITND:
				if (tok == LTREE_TOK_RBRACE)
				{
					curqlevel->high = curqlevel->low;
					state = LQPRS_WAITEND;
				}
				else if (tok == LTREE_TOK_COMMA)
					state = LQPRS_WAITSNUM;
				else if (!t_isdigit(ptr))
					UNCHAR;
				break;
			case LQPRS_WAITEND:
				if (tok == LTREE_TOK_DOT)
				{
					state = LQPRS_WAITLEVEL;
					curqlevel = NEXTLEV(curqlevel);
				}
				else
					UNCHAR;
				break;
			default:
				elog(ERROR, "internal error in lquery parser");
		}
	}

	if (state != LQPRS_WAITDELIM &&
		state != LQPRS_WAITOPEN &&
		state != LQPRS_WAITEND)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("lquery syntax error"),
				 errdetail("Unexpected end of input.")));

	curqlevel = tmpql;
	totallen = LQUERY_HDRSIZE;
	while ((char *) curqlevel - (char *) tmpql < levels * ITEMSIZE)
	{
		totallen += LQL_HDRSIZE;
		if (curqlevel->numvar)
		{
			lptr = GETVAR(curqlevel);
			while (lptr - GETVAR(curqlevel) < curqlevel->numvar)
			{
				totallen += MAXALIGN(LVAR_HDRSIZE + lptr->len);
				lptr++;
			}
		}
		curqlevel = NEXTLEV(curqlevel);
	}

	result = (lquery *) palloc0(totallen);
	SET_VARSIZE(result, totallen);
	result->numlevel = real_levels;
	result->firstgood = 0;
	result->flag = 0;
	if (hasnot)
		result->flag |= LQUERY_HASNOT;
	cur = LQUERY_FIRST(result);
	curqlevel = tmpql;
	while ((char *) curqlevel - (char *) tmpql < levels * ITEMSIZE)
	{
		memcpy(cur, curqlevel, LQL_HDRSIZE);
		cur->totallen = LQL_HDRSIZE;
		if (curqlevel->numvar)
		{
			lrptr = LQL_FIRST(cur);
			lptr = GETVAR(curqlevel);
			while (lptr - GETVAR(curqlevel) < curqlevel->numvar)
			{
				cur->totallen += MAXALIGN(LVAR_HDRSIZE + lptr->len);
				lrptr->len = lptr->len;
				lrptr->flag = lptr->flag;
				copy_unescaped(lrptr->name, lptr->start, lptr->len);
				lrptr->val = ltree_crc32_sz(lrptr->name, lptr->len);
				lptr++;
				lrptr = LVAR_NEXT(lrptr);
			}
			pfree(GETVAR(curqlevel));
			if (cur->numvar > 1 || cur->flag != 0)
			{
				/* Not a simple match */
				wasbad = true;
			}
			else if (wasbad == false)
			{
				/* count leading simple matches */
				(result->firstgood)++;
			}
		}
		else
		{
			/* '*', so this isn't a simple match */
			wasbad = true;
		}
		curqlevel = NEXTLEV(curqlevel);
		cur = LQL_NEXT(cur);
	}

	pfree(tmpql);
	return result;

#undef UNCHAR
}

/*
 * Close out parsing an ltree or lquery nodeitem:
 * compute the correct length, and complain if it's not OK
 *
 * "len"     - length of label in bytes
 * "wlen"    - length of label in wide characters
 * "escapes" - number of escaped characters in label
 * "pos"     - position of label in the input string in wide characters
 */
static void
finish_nodeitem(nodeitem *lptr, const char *ptr, int len, int wlen, int escapes,
				bool is_lquery, int pos)
{
	lptr->start = ptr;

	/*
	 * Exclude escape symbols from the length, because the labels stored
	 * unescaped.
	 */
	lptr->len = len - escapes;
	lptr->wlen = wlen - escapes;

	/*
	 * If it is a quoted label, then we have to move start a byte ahead and
	 * exclude beginning and final quotes from the label itself.
	 */
	if (t_iseq(lptr->start, '"'))
	{
		lptr->start++;
		lptr->len -= 2;
		lptr->wlen -= 2;
	}

	/* Complain if it's empty or too long */
	if (lptr->len <= 0 || lptr->wlen <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 is_lquery ?
				 errmsg("lquery syntax error at character %d", pos) :
				 errmsg("ltree syntax error at character %d", pos),
				 errdetail("Empty labels are not allowed.")));
	if (lptr->wlen > LTREE_LABEL_MAX_CHARS)
		ereport(ERROR,
				(errcode(ERRCODE_NAME_TOO_LONG),
				 errmsg("label string is too long"),
				 errdetail("Label length is %d, must be at most %d, at character %d.",
						   lptr->wlen, LTREE_LABEL_MAX_CHARS, pos)));
}

/*
 * expects an lquery
 * returns a null terminated string
 */
static char *
deparse_lquery(const lquery *in)
{
	char	   *buf,
			   *ptr;
	int			i,
				j,
				var_num = 0,
				totallen = 1;
	int		   *var_extra_bytes;	/* per-var extra bytes for escaping */
	lquery_level *curqlevel;
	lquery_variant *curtlevel;

	curqlevel = LQUERY_FIRST(in);
	for (i = 0; i < in->numlevel; i++)
	{
		totallen++;
		var_num += curqlevel->numvar;

		if (curqlevel->numvar)
		{
			totallen += 1 + (curqlevel->numvar * 4) + curqlevel->totallen;
			if (curqlevel->flag & LQL_COUNT)
				totallen += 2 * 11 + 3;
		}
		else
			totallen += 2 * 11 + 4;		/* length of "*{%d,%d}" */

		curqlevel = LQL_NEXT(curqlevel);
	}

	/* count extra bytes needed for escaping */
	var_extra_bytes = palloc(sizeof(*var_extra_bytes) * var_num);

	var_num = 0;
	curqlevel = LQUERY_FIRST(in);

	for (i = 0; i < in->numlevel; i++)
	{
		if (curqlevel->numvar)
		{
			curtlevel = LQL_FIRST(curqlevel);

			for (j = 0; j < curqlevel->numvar; j++, var_num++)
			{
				int			extra_bytes =
					extra_bytes_for_escaping(curtlevel->name, curtlevel->len);

				var_extra_bytes[var_num] = extra_bytes;
				totallen += extra_bytes;

				curtlevel = LVAR_NEXT(curtlevel);
			}
		}

		curqlevel = LQL_NEXT(curqlevel);
	}

	ptr = buf = (char *) palloc(totallen);
	var_num = 0;
	curqlevel = LQUERY_FIRST(in);
	for (i = 0; i < in->numlevel; i++)
	{
		if (i != 0)
		{
			*ptr = '.';
			ptr++;
		}
		if (curqlevel->numvar)
		{
			if (curqlevel->flag & LQL_NOT)
			{
				*ptr = '!';
				ptr++;
			}
			curtlevel = LQL_FIRST(curqlevel);
			for (j = 0; j < curqlevel->numvar; j++, var_num++)
			{
				int			extra_bytes = var_extra_bytes[var_num];

				if (j != 0)
				{
					*ptr = '|';
					ptr++;
				}

				Assert(ptr + curtlevel->len + extra_bytes < buf + totallen);
				copy_level(ptr, curtlevel->name, curtlevel->len, extra_bytes);
				ptr += curtlevel->len + extra_bytes;

				if ((curtlevel->flag & LVAR_SUBLEXEME))
				{
					*ptr = '%';
					ptr++;
				}
				if ((curtlevel->flag & LVAR_INCASE))
				{
					*ptr = '@';
					ptr++;
				}
				if ((curtlevel->flag & LVAR_ANYEND))
				{
					*ptr = '*';
					ptr++;
				}
				curtlevel = LVAR_NEXT(curtlevel);
			}
		}
		else
		{
			*ptr = '*';
			ptr++;
		}

		if ((curqlevel->flag & LQL_COUNT) || curqlevel->numvar == 0)
		{
			if (curqlevel->low == curqlevel->high)
			{
				sprintf(ptr, "{%d}", curqlevel->low);
			}
			else if (curqlevel->low == 0)
			{
				if (curqlevel->high == LTREE_MAX_LEVELS)
				{
					if (curqlevel->numvar == 0)
					{
						/* This is default for '*', so print nothing */
						*ptr = '\0';
					}
					else
						sprintf(ptr, "{,}");
				}
				else
					sprintf(ptr, "{,%d}", curqlevel->high);
			}
			else if (curqlevel->high == LTREE_MAX_LEVELS)
			{
				sprintf(ptr, "{%d,}", curqlevel->low);
			}
			else
				sprintf(ptr, "{%d,%d}", curqlevel->low, curqlevel->high);
			ptr = strchr(ptr, '\0');
		}

		curqlevel = LQL_NEXT(curqlevel);
	}

	pfree(var_extra_bytes);

	*ptr = '\0';
	return buf;
}

/*
 * Basic lquery I/O functions
 */
PG_FUNCTION_INFO_V1(lquery_in);
Datum
lquery_in(PG_FUNCTION_ARGS)
{
	char	   *buf = (char *) PG_GETARG_POINTER(0);

	PG_RETURN_POINTER(parse_lquery(buf));
}

PG_FUNCTION_INFO_V1(lquery_out);
Datum
lquery_out(PG_FUNCTION_ARGS)
{
	lquery	   *in = PG_GETARG_LQUERY_P(0);

	PG_RETURN_POINTER(deparse_lquery(in));
}

/*
 * lquery type send function
 *
 * The type is sent as text in binary mode, so this is almost the same
 * as the output function, but it's prefixed with a version number so we
 * can change the binary format sent in future if necessary. For now,
 * only version 1 is supported.
 */
PG_FUNCTION_INFO_V1(lquery_send);
Datum
lquery_send(PG_FUNCTION_ARGS)
{
	lquery	   *in = PG_GETARG_LQUERY_P(0);
	StringInfoData buf;
	int			version = 1;
	char	   *res = deparse_lquery(in);

	pq_begintypsend(&buf);
	pq_sendint8(&buf, version);
	pq_sendtext(&buf, res, strlen(res));
	pfree(res);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * lquery type recv function
 *
 * The type is sent as text in binary mode, so this is almost the same
 * as the input function, but it's prefixed with a version number so we
 * can change the binary format sent in future if necessary. For now,
 * only version 1 is supported.
 */
PG_FUNCTION_INFO_V1(lquery_recv);
Datum
lquery_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	int			version = pq_getmsgint(buf, 1);
	char	   *str;
	int			nbytes;
	lquery	   *res;

	if (version != 1)
		elog(ERROR, "unsupported lquery version number %d", version);

	str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
	res = parse_lquery(str);
	pfree(str);

	PG_RETURN_POINTER(res);
}
