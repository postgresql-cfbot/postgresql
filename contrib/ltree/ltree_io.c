/*
 * in/out function for ltree and lquery
 * Teodor Sigaev <teodor@stack.net>
 * contrib/ltree/ltree_io.c
 */
#include "postgres.h"

#include <ctype.h>

#include "ltree.h"
#include "utils/memutils.h"
#include "crc32.h"

PG_FUNCTION_INFO_V1(ltree_in);
PG_FUNCTION_INFO_V1(ltree_out);
PG_FUNCTION_INFO_V1(lquery_in);
PG_FUNCTION_INFO_V1(lquery_out);


#define UNCHAR ereport(ERROR, \
					   (errcode(ERRCODE_SYNTAX_ERROR), \
						errmsg("syntax error at position %d", \
						pos)));


typedef struct
{
	char	   *start;
	int			len;			/* length in bytes */
	int			flag;
	int			wlen;			/* length in characters */
} nodeitem;

#define LTPRS_WAITNAME	0
#define LTPRS_WAITDELIM 1
#define LTPRS_WAITESCAPED 2
#define LTPRS_WAITDELIMSTRICT 3

static void
count_parts_ors(const char *ptr, int *plevels, int *pORs)
{
	int			escape_mode = 0;
	int			charlen;

	while (*ptr)
	{
		charlen = pg_mblen(ptr);

		if (escape_mode == 1)
			escape_mode = 0;
		else if (charlen == 1)
		{
			if (t_iseq(ptr, '\\'))
				escape_mode = 1;
			else if (t_iseq(ptr, '.'))
				(*plevels)++;
			else if (t_iseq(ptr, '|') && pORs != NULL)
				(*pORs)++;
		}

		ptr += charlen;
	}

	(*plevels)++;
	if (pORs != NULL)
		(*pORs)++;
}

/*
 * Char-by-char copying from src to dst representation removing escaping \\
 * Total amount of copied bytes is len
 */
static void
copy_unescaped(char *dst, const char *src, int len)
{
	uint16		copied = 0;
	int			charlen;
	bool		escaping = false;

	while (*src && copied < len)
	{
		charlen = pg_mblen(src);
		if ((charlen == 1) && t_iseq(src, '\\') && escaping == 0)
		{
			escaping = 1;
			src++;
			continue;
		};

		if (copied + charlen > len)
			elog(ERROR, "internal error during splitting levels");

		memcpy(dst, src, charlen);
		src += charlen;
		dst += charlen;
		copied += charlen;
		escaping = 0;
	}

	if (copied != len)
		elog(ERROR, "internal error during splitting levels");
}

/*
 * Function calculating bytes to escape
 * to_escape is an array of "special" 1-byte symbols
 * Behvaiour:
 * If there is no "special" symbols, return 0
 * If there are any special symbol, we need initial and final quote, so return 2
 * If there are any quotes, we need to escape all of them and also initial and final quote, so
 * return 2 + number of quotes
 */
int
bytes_to_escape(const char *start, const int len, const char *to_escape)
{
	uint16		copied = 0;
	int			charlen;
	int			escapes = 0;
	int			quotes = 0;
	const char *buf = start;

	if (len == 0)
		return 2;

	while (*start && copied < len)
	{
		charlen = pg_mblen(buf);
		if ((charlen == 1) && strchr(to_escape, *buf))
		{
			escapes++;
		}
		else if ((charlen == 1) && t_iseq(buf, '"'))
		{
			quotes++;
		}

		if (copied + charlen > len)
			elog(ERROR, "internal error during merging levels");

		buf += charlen;
		copied += charlen;
	}

	return (quotes > 0) ? quotes + 2 :
		(escapes > 0) ? 2 : 0;
}

static int
copy_escaped(char *dst, const char *src, int len)
{
	uint16		copied = 0;
	int			charlen;
	int			escapes = 0;
	char	   *buf = dst;

	while (*src && copied < len)
	{
		charlen = pg_mblen(src);
		if ((charlen == 1) && t_iseq(src, '"'))
		{
			*buf = '\\';
			buf++;
			escapes++;
		};

		if (copied + charlen > len)
			elog(ERROR, "internal error during merging levels");

		memcpy(buf, src, charlen);
		src += charlen;
		buf += charlen;
		copied += charlen;
	}
	return escapes;
}

void
copy_level(char *dst, const char *src, int len, int extra_bytes)
{
	if (extra_bytes == 0)
		memcpy(dst, src, len);
	else if (extra_bytes == 2)
	{
		*dst = '"';
		memcpy(dst + 1, src, len);
		dst[len + 1] = '"';
	}
	else
	{
		*dst = '"';
		copy_escaped(dst + 1, src, len);
		dst[len + extra_bytes - 1] = '"';
	}
}

static void
real_nodeitem_len(nodeitem *lptr, const char *ptr, int escapes, int tail_space_bytes, int tail_space_symbols)
{
	lptr->len = ptr - lptr->start - escapes -
		((lptr->flag & LVAR_SUBLEXEME) ? 1 : 0) -
		((lptr->flag & LVAR_INCASE) ? 1 : 0) -
		((lptr->flag & LVAR_ANYEND) ? 1 : 0) - tail_space_bytes;
	lptr->wlen -= tail_space_symbols;
}

/*
 * If we have a part beginning with quote,
 * we must be sure it is finished with quote either.
 * After that we moving start of the part a byte ahead
 * and excluding beginning and final quotes from the part itself.
 * */
static void
adjust_quoted_nodeitem(nodeitem *lptr)
{
	lptr->start++;
	lptr->len -= 2;
	lptr->wlen -= 2;
}

static void
check_level_length(const nodeitem *lptr, int pos)
{
	if (lptr->len < 0)
		elog(ERROR, "internal error: invalid level length");

	if (lptr->wlen <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("name of level is empty"),
				 errdetail("Name length is 0 in position %d.",
						   pos)));

	if (lptr->wlen > 255)
		ereport(ERROR,
				(errcode(ERRCODE_NAME_TOO_LONG),
				 errmsg("name of level is too long"),
				 errdetail("Name length is %d, must "
						   "be < 256, in position %d.",
						   lptr->wlen, pos)));
}

Datum
ltree_in(PG_FUNCTION_ARGS)
{
	char	   *buf = (char *) PG_GETARG_POINTER(0);
	char	   *ptr;
	nodeitem   *list,
			   *lptr;
	int			levels = 0,
				totallen = 0;
	int			state = LTPRS_WAITNAME;
	ltree	   *result;
	ltree_level *curlevel;
	int			charlen;

	/* Position in strings, in symbols. */
	int			pos = 0;
	int			escaped_count = 0;
	int			tail_space_bytes = 0;
	int			tail_space_symbols = 0;

	ptr = buf;
	count_parts_ors(ptr, &levels, NULL);

	if (levels > MaxAllocSize / sizeof(nodeitem))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("number of levels (%d) exceeds the maximum allowed (%d)",
						levels, (int) (MaxAllocSize / sizeof(nodeitem)))));
	list = lptr = (nodeitem *) palloc(sizeof(nodeitem) * (levels));

	/*
	 * This block calculates single nodes' settings
	 */
	ptr = buf;
	while (*ptr)
	{
		charlen = pg_mblen(ptr);
		if (state == LTPRS_WAITNAME)
		{
			if (t_isspace(ptr))
			{
				ptr += charlen;
				pos++;
				continue;
			}
			state = LTPRS_WAITDELIM;
			lptr->start = ptr;
			lptr->wlen = 0;
			lptr->flag = 0;
			escaped_count = 0;

			if (charlen == 1)
			{
				if (t_iseq(ptr, '.'))
				{
					UNCHAR;
				}
				else if (t_iseq(ptr, '\\'))
					state = LTPRS_WAITESCAPED;
				else if (t_iseq(ptr, '"'))
					lptr->flag |= LVAR_QUOTEDPART;
			}
		}
		else if (state == LTPRS_WAITESCAPED)
		{
			state = LTPRS_WAITDELIM;
			escaped_count++;
		}
		else if (state == LTPRS_WAITDELIM)
		{
			if (charlen == 1)
			{
				if (t_iseq(ptr, '.') && !(lptr->flag & LVAR_QUOTEDPART))
				{
					real_nodeitem_len(lptr, ptr, escaped_count, tail_space_bytes, tail_space_symbols);
					check_level_length(lptr, pos);

					totallen += MAXALIGN(lptr->len + LEVEL_HDRSIZE);
					lptr++;
					state = LTPRS_WAITNAME;
				}
				else if (t_iseq(ptr, '\\'))
				{
					state = LTPRS_WAITESCAPED;
				}
				else if (t_iseq(ptr, '"'))
				{
					if (lptr->flag & LVAR_QUOTEDPART)
					{
						lptr->flag &= ~LVAR_QUOTEDPART;
						state = LTPRS_WAITDELIMSTRICT;
					}
					else		/* Unescaped quote is forbidden */
						UNCHAR;
				}
			}

			if (t_isspace(ptr))
			{
				tail_space_symbols++;
				tail_space_bytes += charlen;
			}
			else
			{
				tail_space_symbols = 0;
				tail_space_bytes = 0;
			}
		}
		else if (state == LTPRS_WAITDELIMSTRICT)
		{
			if (t_isspace(ptr))
			{
				ptr += charlen;
				pos++;
				tail_space_bytes += charlen;
				tail_space_symbols = 1;
				continue;
			}

			if (!(charlen == 1 && t_iseq(ptr, '.')))
				UNCHAR;

			real_nodeitem_len(lptr, ptr, escaped_count, tail_space_bytes, tail_space_symbols);

			adjust_quoted_nodeitem(lptr);
			check_level_length(lptr, pos);

			totallen += MAXALIGN(lptr->len + LEVEL_HDRSIZE);
			lptr++;
			state = LTPRS_WAITNAME;
		}
		else
			/* internal error */
			elog(ERROR, "internal error in parser");
		ptr += charlen;
		if (state == LTPRS_WAITDELIM || state == LTPRS_WAITDELIMSTRICT)
			lptr->wlen++;
		pos++;
	}

	if (state == LTPRS_WAITDELIM || state == LTPRS_WAITDELIMSTRICT)
	{
		if (lptr->flag & LVAR_QUOTEDPART)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("syntax error"),
					 errdetail("Unexpected end of line.")));

		real_nodeitem_len(lptr, ptr, escaped_count, tail_space_bytes, tail_space_symbols);

		if (state == LTPRS_WAITDELIMSTRICT)
			adjust_quoted_nodeitem(lptr);

		check_level_length(lptr, pos);

		totallen += MAXALIGN(lptr->len + LEVEL_HDRSIZE);
		lptr++;
	}
	else if (!(state == LTPRS_WAITNAME && lptr == list))	/* Empty string */
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("syntax error"),
				 errdetail("Unexpected end of line.")));

	result = (ltree *) palloc0(LTREE_HDRSIZE + totallen);
	SET_VARSIZE(result, LTREE_HDRSIZE + totallen);
	result->numlevel = lptr - list;
	curlevel = LTREE_FIRST(result);
	lptr = list;
	while (lptr - list < result->numlevel)
	{
		curlevel->len = (uint16) lptr->len;
		if (lptr->len > 0)
		{
			copy_unescaped(curlevel->name, lptr->start, lptr->len);
		}
		curlevel = LEVEL_NEXT(curlevel);
		lptr++;
	}

	pfree(list);
	PG_RETURN_POINTER(result);
}

Datum
ltree_out(PG_FUNCTION_ARGS)
{
	ltree	   *in = PG_GETARG_LTREE_P(0);
	char	   *buf,
			   *ptr;
	int			i;
	ltree_level *curlevel;
	Size		allocated = VARSIZE(in);
	Size		filled = 0;

	ptr = buf = (char *) palloc(allocated);
	curlevel = LTREE_FIRST(in);
	for (i = 0; i < in->numlevel; i++)
	{
		if (i != 0)
		{
			*ptr = '.';
			ptr++;
			filled++;
		}
		if (curlevel->len >= 0)
		{
			int			extra_bytes = bytes_to_escape(curlevel->name, curlevel->len, "\\ .");

			if (filled + extra_bytes + curlevel->len >= allocated)
			{
				buf = repalloc(buf, allocated + (extra_bytes + curlevel->len) * 2);
				allocated += (extra_bytes + curlevel->len) * 2;
				ptr = buf + filled;
			}

			copy_level(ptr, curlevel->name, curlevel->len, extra_bytes);
			ptr += curlevel->len + extra_bytes;
		}
		curlevel = LEVEL_NEXT(curlevel);
	}

	*ptr = '\0';
	PG_FREE_IF_COPY(in, 0);

	PG_RETURN_POINTER(buf);
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
#define LQPRS_WAITESCAPED 9
#define LQPRS_WAITDELIMSTRICT 10


#define GETVAR(x) ( *((nodeitem**)LQL_FIRST(x)) )
#define ITEMSIZE	MAXALIGN(LQL_HDRSIZE+sizeof(nodeitem*))
#define NEXTLEV(x) ( (lquery_level*)( ((char*)(x)) + ITEMSIZE) )

Datum
lquery_in(PG_FUNCTION_ARGS)
{
	char	   *buf = (char *) PG_GETARG_POINTER(0);
	char	   *ptr;
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
	int			charlen;
	int			pos = 0;
	int			escaped_count = 0;
	int			real_levels = 0;
	int			tail_space_bytes = 0;
	int			tail_space_symbols = 0;

	ptr = buf;
	count_parts_ors(ptr, &levels, &numOR);

	if (levels > MaxAllocSize / ITEMSIZE)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("number of levels (%d) exceeds the maximum allowed (%d)",
						levels, (int) (MaxAllocSize / ITEMSIZE))));
	curqlevel = tmpql = (lquery_level *) palloc0(ITEMSIZE * levels);
	ptr = buf;
	while (*ptr)
	{
		charlen = pg_mblen(ptr);

		if (state == LQPRS_WAITLEVEL)
		{
			if (t_isspace(ptr))
			{
				ptr += charlen;
				pos++;
				continue;
			}

			escaped_count = 0;
			real_levels++;
			if (charlen == 1)
			{
				if (t_iseq(ptr, '!'))
				{
					GETVAR(curqlevel) = lptr = (nodeitem *) palloc0(sizeof(nodeitem) * numOR);
					lptr->start = ptr + 1;
					state = LQPRS_WAITDELIM;
					curqlevel->numvar = 1;
					curqlevel->flag |= LQL_NOT;
					hasnot = true;
				}
				else if (t_iseq(ptr, '*'))
					state = LQPRS_WAITOPEN;
				else if (t_iseq(ptr, '\\'))
				{
					GETVAR(curqlevel) = lptr = (nodeitem *) palloc0(sizeof(nodeitem) * numOR);
					lptr->start = ptr;
					curqlevel->numvar = 1;
					state = LQPRS_WAITESCAPED;
				}
				else if (strchr(".|@%{}", *ptr))
				{
					UNCHAR;
				}
				else
				{
					GETVAR(curqlevel) = lptr = (nodeitem *) palloc0(sizeof(nodeitem) * numOR);
					lptr->start = ptr;
					state = LQPRS_WAITDELIM;
					curqlevel->numvar = 1;
					if (t_iseq(ptr, '"'))
					{
						lptr->flag |= LVAR_QUOTEDPART;
					}
				}
			}
			else
			{
				GETVAR(curqlevel) = lptr = (nodeitem *) palloc0(sizeof(nodeitem) * numOR);
				lptr->start = ptr;
				state = LQPRS_WAITDELIM;
				curqlevel->numvar = 1;
			}
		}
		else if (state == LQPRS_WAITVAR)
		{
			if (t_isspace(ptr))
			{
				ptr += charlen;
				pos++;
				continue;
			}

			escaped_count = 0;
			lptr++;
			lptr->start = ptr;
			curqlevel->numvar++;
			if (t_iseq(ptr, '.') || t_iseq(ptr, '|'))
				UNCHAR;

			state = (t_iseq(ptr, '\\')) ? LQPRS_WAITESCAPED : LQPRS_WAITDELIM;
			if (t_iseq(ptr, '"'))
				lptr->flag |= LVAR_QUOTEDPART;
		}
		else if (state == LQPRS_WAITDELIM || state == LQPRS_WAITDELIMSTRICT)
		{
			if (charlen == 1 && t_iseq(ptr, '"'))
			{
				/* We are here if variant begins with ! */
				if (lptr->start == ptr)
					lptr->flag |= LVAR_QUOTEDPART;
				else if (state == LQPRS_WAITDELIMSTRICT)
				{
					UNCHAR;
				}
				else if (lptr->flag & LVAR_QUOTEDPART)
				{
					lptr->flag &= ~LVAR_QUOTEDPART;
					state = LQPRS_WAITDELIMSTRICT;
				}
				else
					UNCHAR;
			}
			else if ((lptr->flag & LVAR_QUOTEDPART) == 0)
			{
				if (charlen == 1 && t_iseq(ptr, '@'))
				{
					if (lptr->start == ptr)
						UNCHAR;
					lptr->flag |= LVAR_INCASE;
					curqlevel->flag |= LVAR_INCASE;
				}
				else if (charlen == 1 && t_iseq(ptr, '*'))
				{
					if (lptr->start == ptr)
						UNCHAR;
					lptr->flag |= LVAR_ANYEND;
					curqlevel->flag |= LVAR_ANYEND;
				}
				else if (charlen == 1 && t_iseq(ptr, '%'))
				{
					if (lptr->start == ptr)
						UNCHAR;
					lptr->flag |= LVAR_SUBLEXEME;
					curqlevel->flag |= LVAR_SUBLEXEME;
				}
				else if (charlen == 1 && t_iseq(ptr, '|'))
				{
					real_nodeitem_len(lptr, ptr, escaped_count, tail_space_bytes, tail_space_symbols);

					if (state == LQPRS_WAITDELIMSTRICT)
						adjust_quoted_nodeitem(lptr);

					check_level_length(lptr, pos);
					state = LQPRS_WAITVAR;
				}
				else if (charlen == 1 && t_iseq(ptr, '.'))
				{
					real_nodeitem_len(lptr, ptr, escaped_count, tail_space_bytes, tail_space_symbols);

					if (state == LQPRS_WAITDELIMSTRICT)
						adjust_quoted_nodeitem(lptr);

					check_level_length(lptr, pos);

					state = LQPRS_WAITLEVEL;
					curqlevel = NEXTLEV(curqlevel);
				}
				else if (charlen == 1 && t_iseq(ptr, '\\'))
				{
					if (state == LQPRS_WAITDELIMSTRICT)
						UNCHAR;
					state = LQPRS_WAITESCAPED;
				}
				else
				{
					if (charlen == 1 && strchr("!{}", *ptr))
						UNCHAR;
					if (state == LQPRS_WAITDELIMSTRICT)
					{
						if (t_isspace(ptr))
						{
							ptr += charlen;
							pos++;
							tail_space_bytes += charlen;
							tail_space_symbols = 1;
							continue;
						}

						UNCHAR;
					}
					if (lptr->flag & ~LVAR_QUOTEDPART)
						UNCHAR;
				}
			}
			else if (charlen == 1 && t_iseq(ptr, '\\'))
			{
				if (state == LQPRS_WAITDELIMSTRICT)
					UNCHAR;
				if (lptr->flag & ~LVAR_QUOTEDPART)
					UNCHAR;
				state = LQPRS_WAITESCAPED;
			}
			else
			{
				if (state == LQPRS_WAITDELIMSTRICT)
				{
					if (t_isspace(ptr))
					{
						ptr += charlen;
						pos++;
						tail_space_bytes += charlen;
						tail_space_symbols = 1;
						continue;
					}

					UNCHAR;
				}
				if (lptr->flag & ~LVAR_QUOTEDPART)
					UNCHAR;
			}

			if (t_isspace(ptr))
			{
				tail_space_symbols++;
				tail_space_bytes += charlen;
			}
			else
			{
				tail_space_symbols = 0;
				tail_space_bytes = 0;
			}
		}
		else if (state == LQPRS_WAITOPEN)
		{
			if (charlen == 1 && t_iseq(ptr, '{'))
				state = LQPRS_WAITFNUM;
			else if (charlen == 1 && t_iseq(ptr, '.'))
			{
				curqlevel->low = 0;
				curqlevel->high = 0xffff;
				curqlevel = NEXTLEV(curqlevel);
				state = LQPRS_WAITLEVEL;
			}
			else
				UNCHAR;
		}
		else if (state == LQPRS_WAITFNUM)
		{
			if (charlen == 1 && t_iseq(ptr, ','))
				state = LQPRS_WAITSNUM;
			else if (t_isdigit(ptr))
			{
				curqlevel->low = atoi(ptr);
				state = LQPRS_WAITND;
			}
			else
				UNCHAR;
		}
		else if (state == LQPRS_WAITSNUM)
		{
			if (t_isdigit(ptr))
			{
				curqlevel->high = atoi(ptr);
				state = LQPRS_WAITCLOSE;
			}
			else if (charlen == 1 && t_iseq(ptr, '}'))
			{
				curqlevel->high = 0xffff;
				state = LQPRS_WAITEND;
			}
			else
				UNCHAR;
		}
		else if (state == LQPRS_WAITCLOSE)
		{
			if (charlen == 1 && t_iseq(ptr, '}'))
				state = LQPRS_WAITEND;
			else if (!t_isdigit(ptr))
				UNCHAR;
		}
		else if (state == LQPRS_WAITND)
		{
			if (charlen == 1 && t_iseq(ptr, '}'))
			{
				curqlevel->high = curqlevel->low;
				state = LQPRS_WAITEND;
			}
			else if (charlen == 1 && t_iseq(ptr, ','))
				state = LQPRS_WAITSNUM;
			else if (!t_isdigit(ptr))
				UNCHAR;
		}
		else if (state == LQPRS_WAITEND)
		{
			if (charlen == 1 && (t_iseq(ptr, '.') || t_iseq(ptr, '|')))
			{
				state = LQPRS_WAITLEVEL;
				curqlevel = NEXTLEV(curqlevel);
			}
			else
				UNCHAR;
		}
		else if (state == LQPRS_WAITESCAPED)
		{
			state = LQPRS_WAITDELIM;
			escaped_count++;
		}
		else
			/* internal error */
			elog(ERROR, "internal error in parser");

		ptr += charlen;
		if (state == LQPRS_WAITDELIM || state == LQPRS_WAITDELIMSTRICT)
			lptr->wlen++;
		pos++;
	}

	if (lptr->flag & LVAR_QUOTEDPART)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("syntax error"),
				 errdetail("Unexpected end of line.")));
	}
	else if (state == LQPRS_WAITDELIM || state == LQPRS_WAITDELIMSTRICT)
	{
		if (lptr->start == ptr)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("syntax error"),
					 errdetail("Unexpected end of line.")));

		real_nodeitem_len(lptr, ptr, escaped_count, tail_space_bytes, tail_space_symbols);

		if (state == LQPRS_WAITDELIMSTRICT)
			adjust_quoted_nodeitem(lptr);

		check_level_length(lptr, pos);
	}
	else if (state == LQPRS_WAITOPEN)
		curqlevel->high = 0xffff;
	else if (state != LQPRS_WAITEND)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("syntax error"),
				 errdetail("Unexpected end of line.")));
	else if (state == LQPRS_WAITESCAPED)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("syntax error"),
				 errdetail("Unexpected end of line.")));


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
		else if (curqlevel->low > curqlevel->high)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("syntax error"),
					 errdetail("Low limit(%d) is greater than upper(%d).",
							   curqlevel->low, curqlevel->high)));

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
				wasbad = true;
			else if (wasbad == false)
				(result->firstgood)++;
		}
		else
			wasbad = true;
		curqlevel = NEXTLEV(curqlevel);
		cur = LQL_NEXT(cur);
	}

	pfree(tmpql);
	PG_RETURN_POINTER(result);
}

Datum
lquery_out(PG_FUNCTION_ARGS)
{
	lquery	   *in = PG_GETARG_LQUERY_P(0);
	char	   *buf,
			   *ptr;
	int			i,
				j,
				totallen = 1,
				filled = 0;
	lquery_level *curqlevel;
	lquery_variant *curtlevel;

	curqlevel = LQUERY_FIRST(in);
	for (i = 0; i < in->numlevel; i++)
	{
		totallen++;
		if (curqlevel->numvar)
			totallen += 1 + (curqlevel->numvar * 4) + curqlevel->totallen;
		else
			totallen += 2 * 11 + 4;
		curqlevel = LQL_NEXT(curqlevel);
	}

	ptr = buf = (char *) palloc(totallen);
	curqlevel = LQUERY_FIRST(in);
	for (i = 0; i < in->numlevel; i++)
	{
		if (i != 0)
		{
			*ptr = '.';
			ptr++;
			filled++;
		}
		if (curqlevel->numvar)
		{
			if (curqlevel->flag & LQL_NOT)
			{
				*ptr = '!';
				ptr++;
				filled++;
			}
			curtlevel = LQL_FIRST(curqlevel);
			for (j = 0; j < curqlevel->numvar; j++)
			{
				int			extra_bytes = bytes_to_escape(curtlevel->name, curtlevel->len, ". \\|!*@%{}");

				if (j != 0)
				{
					*ptr = '|';
					ptr++;
					filled++;
				}
				if (filled + extra_bytes + curtlevel->len >= totallen)
				{
					buf = repalloc(buf, totallen + (extra_bytes + curtlevel->len) * 2);
					totallen += (extra_bytes + curtlevel->len) * 2;
					ptr = buf + filled;
				}

				copy_level(ptr, curtlevel->name, curtlevel->len, extra_bytes);
				ptr += curtlevel->len + extra_bytes;

				if ((curtlevel->flag & LVAR_SUBLEXEME))
				{
					*ptr = '%';
					ptr++;
					filled++;
				}
				if ((curtlevel->flag & LVAR_INCASE))
				{
					*ptr = '@';
					ptr++;
					filled++;
				}
				if ((curtlevel->flag & LVAR_ANYEND))
				{
					*ptr = '*';
					ptr++;
					filled++;
				}
				curtlevel = LVAR_NEXT(curtlevel);
			}
		}
		else
		{
			if (curqlevel->low == curqlevel->high)
			{
				sprintf(ptr, "*{%d}", curqlevel->low);
			}
			else if (curqlevel->low == 0)
			{
				if (curqlevel->high == 0xffff)
				{
					*ptr = '*';
					*(ptr + 1) = '\0';
				}
				else
					sprintf(ptr, "*{,%d}", curqlevel->high);
			}
			else if (curqlevel->high == 0xffff)
			{
				sprintf(ptr, "*{%d,}", curqlevel->low);
			}
			else
				sprintf(ptr, "*{%d,%d}", curqlevel->low, curqlevel->high);
			ptr = strchr(ptr, '\0');
			filled = ptr - buf;
		}

		curqlevel = LQL_NEXT(curqlevel);
	}

	*ptr = '\0';
	PG_FREE_IF_COPY(in, 0);

	PG_RETURN_POINTER(buf);
}
