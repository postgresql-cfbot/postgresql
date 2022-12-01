/*-------------------------------------------------------------------------
 *
 * Implementation of simple filter file parser
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/pg_dump/filter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "common/fe_memutils.h"
#include "common/logging.h"
#include "common/string.h"
#include "filter.h"
#include "lib/stringinfo.h"
#include "pqexpbuffer.h"

#define		is_keyword_str(cstr, str, bytes) \
	((strlen(cstr) == (bytes)) && (pg_strncasecmp((cstr), (str), (bytes)) == 0))

/*
 * Following routines are called from pg_dump, pg_dumpall and pg_restore.
 * Unfortunately, the implementation of exit_nicely in pg_dump and pg_restore is
 * different from the one in pg_dumpall, so instead of calling exit_nicely we
 * have to return some error flag (in this case NULL), and exit_nicelly will be
 * executed from caller's routine.
 */

/*
 * Opens filter's file and initialize fstate structure.
 * Returns true on success.
 */
bool
filter_init(FilterStateData *fstate, const char *filename)
{
	fstate->filename = filename;
	fstate->lineno = 0;
	initStringInfo(&fstate->linebuff);

	if (strcmp(filename, "-") != 0)
	{
		fstate->fp = fopen(filename, "r");
		if (!fstate->fp)
		{
			pg_log_error("could not open filter file \"%s\": %m", filename);
			return false;
		}
	}
	else
		fstate->fp = stdin;

	fstate->is_error = false;

	return true;
}

/*
 * Release allocated resources for the given filter.
 */
void
filter_free(FilterStateData *fstate)
{
	free(fstate->linebuff.data);
	fstate->linebuff.data = NULL;

	if (fstate->fp && fstate->fp != stdin)
	{
		if (fclose(fstate->fp) != 0)
			pg_log_error("could not close filter file \"%s\": %m", fstate->filename);

		fstate->fp = NULL;
	}
}

/*
 * Translate FilterObjectType enum to string. It is designed for formatting
 * of error message in log_unsupported_filter_object_type routine.
 */
static const char *
filter_object_type_name(FilterObjectType fot)
{
	switch (fot)
	{
		case FILTER_OBJECT_TYPE_NONE:
			return "comment or empty line";
		case FILTER_OBJECT_TYPE_TABLE_DATA:
			return "table data";
		case FILTER_OBJECT_TYPE_DATABASE:
			return "database";
		case FILTER_OBJECT_TYPE_FOREIGN_DATA:
			return "foreign data";
		case FILTER_OBJECT_TYPE_FUNCTION:
			return "function";
		case FILTER_OBJECT_TYPE_INDEX:
			return "index";
		case FILTER_OBJECT_TYPE_SCHEMA:
			return "schema";
		case FILTER_OBJECT_TYPE_TABLE:
			return "table";
		case FILTER_OBJECT_TYPE_TRIGGER:
			return "trigger";
	}

	/* should never get here */
	pg_unreachable();
}

/*
 * Emit error message "invalid format in filter file ..."
 *
 * This is mostly a convenience routine to avoid duplicating file closing code
 * in multiple callsites.
 */
void
log_invalid_filter_format(FilterStateData *fstate, char *message)
{
	if (fstate->fp != stdin)
	{
		pg_log_error("invalid format in filter file \"%s\" on line %d: %s",
					 fstate->filename,
					 fstate->lineno,
					 message);
	}
	else
		pg_log_error("invalid format in filter on line %d: %s",
					 fstate->lineno,
					 message);

	fstate->is_error = true;
}

/*
 * Emit error message "The application doesn't support filter for object type ..."
 *
 * This is mostly a convenience routine to avoid duplicating file closing code
 * in multiple callsites.
 */
void
log_unsupported_filter_object_type(FilterStateData *fstate,
									const char *appname,
									FilterObjectType fot)
{
	PQExpBuffer str = createPQExpBuffer();

	printfPQExpBuffer(str,
					  "\"%s\" doesn't support filter for object type \"%s\".",
					  appname,
					  filter_object_type_name(fot));

	log_invalid_filter_format(fstate, str->data);
}

/*
 * filter_get_keyword - read the next filter keyword from buffer
 *
 * Search for keywords (limited to ascii alphabetic characters) in
 * the passed in line buffer. Returns NULL when the buffer is empty or first
 * char is not alpha. The char '_' is allowed too (exclude first position).
 * The length of the found keyword is returned in the size parameter.
 */
static const char *
filter_get_keyword(const char **line, int *size)
{
	const char *ptr = *line;
	const char *result = NULL;

	/* Set returnlength preemptively in case no keyword is found */
	*size = 0;

	/* Skip initial whitespace */
	while (isspace(*ptr))
		ptr++;

	if (isalpha(*ptr))
	{
		result = ptr++;

		while (isalpha(*ptr) || *ptr == '_')
			ptr++;

		*size = ptr - result;
	}

	*line = ptr;

	return result;
}

/*
 * read_quoted_pattern - read quoted possibly multi lined string.
 *
 * Returns pointer to next char after ending double quotes or NULL on error.
 */
static const char *
read_quoted_string(FilterStateData *fstate,
					const char *str,
					PQExpBuffer pattern)
{
	appendPQExpBufferChar(pattern, '"');
	str++;

	while (1)
	{
		/*
		 * We can ignore \r or \n chars because the string is read by
		 * pg_get_line_buf, so these chars should be just trailing chars.
		 */
		if (*str == '\r' || *str == '\n')
		{
			str++;
			continue;
		}

		if (*str == '\0')
		{
			Assert(fstate->linebuff.data);

			if (!pg_get_line_buf(fstate->fp, &fstate->linebuff))
			{
				if (ferror(fstate->fp))
				{
					pg_log_error("could not read from filter file \"%s\": %m",
								 fstate->filename);
					fstate->is_error = true;
				}
				else
					log_invalid_filter_format(fstate, "unexpected end of file");

				return NULL;
			}

			str = fstate->linebuff.data;

			appendPQExpBufferChar(pattern, '\n');
			fstate->lineno++;
		}

		if (*str == '"')
		{
			appendPQExpBufferChar(pattern, '"');
			str++;

			if (*str == '"')
			{
				appendPQExpBufferChar(pattern, '"');
				str++;
			}
			else
				break;
		}
		else if (*str == '\\')
		{
			str++;
			if (*str == 'n')
				appendPQExpBufferChar(pattern, '\n');
			else if (*str == '\\')
				appendPQExpBufferChar(pattern, '\\');

			str++;
		}
		else
			appendPQExpBufferChar(pattern, *str++);
	}

	return str;
}

/*
 * read_pattern - reads on object pattern from input
 *
 * This function will parse any valid identifier (quoted or not, qualified or
 * not), which can also includes the full signature for routines.
 * Note that this function takes special care to sanitize the detected
 * identifier (removing extraneous whitespaces or other unnecessary
 * characters).  This is necessary as most backup/restore filtering functions
 * only recognize identifiers if they are written exactly way as they are
 * regenerated.
 * Returns a pointer to next character after the found identifier, or NULL on
 * error.
 */
static const char *
read_pattern(FilterStateData *fstate, const char *str, PQExpBuffer pattern)
{
	bool	skip_space = true;
	bool	found_space = false;

	/* Skip initial whitespace */
	while (isspace(*str))
		str++;

	if (*str == '\0')
	{
		log_invalid_filter_format(fstate, "missing object name pattern");
		return NULL;
	}

	while (*str && *str != '#')
	{
		while (*str && !isspace(*str) && !strchr("#,.()\"", *str))
		{
			/*
			 * Append space only when it is allowed, and when it was found
			 * in original string.
			 */
			if (!skip_space && found_space)
			{
				appendPQExpBufferChar(pattern, ' ');
				skip_space = true;
			}

			appendPQExpBufferChar(pattern, *str++);
		}

		skip_space = false;

		if (*str == '"')
		{
			if (found_space)
				appendPQExpBufferChar(pattern, ' ');

			str = read_quoted_string(fstate, str, pattern);
			if (!str)
				return NULL;
		}
		else if (*str == ',')
		{
			appendPQExpBufferStr(pattern, ", ");
			skip_space = true;
			str++;
		}
		else if (*str && strchr(".()", *str))
		{
			appendPQExpBufferChar(pattern, *str++);
			skip_space = true;
		}

		found_space = false;

		/* skip ending whitespaces */
		while (isspace(*str))
		{
			found_space = true;
			str++;
		}
	}

	return str;
}

/*
 * filter_read_item - Read command/type/pattern triplet from a filter file
 *
 * This will parse one filter item from the filter file, and while it is a
 * row based format a pattern may span more than one line due to how object
 * names can be constructed.  The expected format of the filter file is:
 *
 * <command> <object_type> <pattern>
 *
 * command can be "include" or "exclude"
 * object_type can one of: "table", "schema", "foreign_data", "table_data",
 * "database", "function", "trigger" or "index"
 * pattern can be any possibly-quoted and possibly-qualified identifier.  It
 * follows the same rules as other object include and exclude functions so it
 * can also use wildcards.
 *
 * Returns true when one filter item was successfully read and parsed.  When
 * object name contains \n chars, then more than one line from input file can
 * be processed.  Returns false when the filter file reaches EOF. In case of
 * error, the function will emit an appropriate error message before returning
 * false.
 */
bool
filter_read_item(FilterStateData *fstate,
				 bool *is_include,
				 char **objname,
				 FilterObjectType *objtype)
{
	Assert(!fstate->is_error);

	if (pg_get_line_buf(fstate->fp, &fstate->linebuff))
	{
		const char *str = fstate->linebuff.data;
		const char *keyword;
		int			size;
		PQExpBufferData pattern;

		fstate->lineno++;

		/* Skip initial white spaces */
		while (isspace(*str))
			str++;

		/*
		 * Skip empty lines or lines where the first non-whitespace character
		 * is a hash indicating a comment.
		 */
		if (*str != '\0' && *str != '#')
		{
			/*
			 * First we expect sequence of two keywords, {include|exclude}
			 * followed by the object type to operate on.
			 */
			keyword = filter_get_keyword(&str, &size);
			if (!keyword)
			{
				log_invalid_filter_format(fstate,
										   "no filter command found (expected \"include\" or \"exclude\")");
				return false;
			}

			if (is_keyword_str("include", keyword, size))
				*is_include = true;
			else if (is_keyword_str("exclude", keyword, size))
				*is_include = false;
			else
			{
				log_invalid_filter_format(fstate,
										  "invalid filter command (expected \"include\" or \"exclude\")");
				return false;
			}

			keyword = filter_get_keyword(&str, &size);
			if (!keyword)
			{
				log_invalid_filter_format(fstate, "missing filter object type");
				return false;
			}

			if (is_keyword_str("table_data", keyword, size))
				*objtype = FILTER_OBJECT_TYPE_TABLE_DATA;
			else if (is_keyword_str("database", keyword, size))
				*objtype = FILTER_OBJECT_TYPE_DATABASE;
			else if (is_keyword_str("foreign_data",keyword, size))
				*objtype = FILTER_OBJECT_TYPE_FOREIGN_DATA;
			else if (is_keyword_str("function", keyword, size))
				*objtype = FILTER_OBJECT_TYPE_FUNCTION;
			else if (is_keyword_str("index", keyword, size))
				*objtype = FILTER_OBJECT_TYPE_INDEX;
			else if (is_keyword_str("schema", keyword, size))
				*objtype = FILTER_OBJECT_TYPE_SCHEMA;
			else if (is_keyword_str("table", keyword, size))
				*objtype = FILTER_OBJECT_TYPE_TABLE;
			else if (is_keyword_str("trigger", keyword, size))
				*objtype = FILTER_OBJECT_TYPE_TRIGGER;
			else
			{
				PQExpBuffer str = createPQExpBuffer();

				printfPQExpBuffer(str, "unsupported filter object type: \"%.*s\"", size, keyword);
				log_invalid_filter_format(fstate, str->data);
				return false;
			}

			initPQExpBuffer(&pattern);

			str = read_pattern(fstate, str, &pattern);
			if (!str)
				return false;

			*objname = pattern.data;
		}
		else
		{
			*objname = NULL;
			*objtype = FILTER_OBJECT_TYPE_NONE;
		}

		return true;
	}

	if (ferror(fstate->fp))
	{
		pg_log_error("could not read from filter file \"%s\": %m", fstate->filename);
		fstate->is_error = true;
	}

	return false;
}
