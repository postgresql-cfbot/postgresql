#include "pg_tracing_query_process.h"
#include "pg_tracing.h"

#include <sys/stat.h>
#include <unistd.h>
#include "parser/scanner.h"
#include "nodes/extensible.h"

/*
 * Extract traceparent parameters from SQLCommenter
 *
 * We're expecting the query to start with a SQLComment containing the
 * traceparent parameter
 * "/\*traceparent='00-00000000000000000000000000000009-0000000000000005-01'*\/
 * SELECT 1;"
 * Traceparent has the following format: version-traceid-parentid-sampled
 * We also accept SQLCommenter as a parameter. In this case, we don't have
 * the start and end of comments.
 *
 * if is_parameter is true, the SQLComment is passed as a query parameter and
 * won't have the comment start and end.
 */
void
extract_trace_context_from_query(pgTracingTraceContext * trace_context, const char *sqlcomment_str, bool is_parameter)
{
	const char *expected_start = "/*";
	const char *traceparent;
	const char *end_sqlcomment;
	char	   *endptr;

	if (!is_parameter)
	{
		/*
		 * Look for the start of a comment We're expecting the comment to be
		 * at the very start of the query
		 */
		for (size_t i = 0; i < strlen(expected_start); i++)
		{
			if (sqlcomment_str[i] != expected_start[i])
				return;
		}

		/*
		 * Check that we have an end
		 */
		end_sqlcomment = strstr(sqlcomment_str, "*/");
		if (end_sqlcomment == NULL)
			return;
	}
	else
		end_sqlcomment = sqlcomment_str + strlen(sqlcomment_str);

	/*
	 * Locate traceparent parameter and make sure it has the expected size
	 * "traceparent" + "=" + '' -> 13 characters
	 * 00-00000000000000000000000000000009-0000000000000005-01 -> 55
	 * characters
	 */
	traceparent = strstr(sqlcomment_str, "traceparent='");
	if (traceparent == NULL || traceparent > end_sqlcomment || end_sqlcomment - traceparent < 55 + 13)
		return;

	/*
	 * Move to the start of the traceparent values
	 */
	traceparent = traceparent + 13;

	/*
	 * Check that '-' are at the expected places
	 */
	if (traceparent[2] != '-' || traceparent[35] != '-' || traceparent[52] != '-')
		return;

	/*
	 * Parse traceparent parameters
	 */
	errno = 0;
	trace_context->trace_id = strtol(&traceparent[3], &endptr, 16);
	if (endptr != traceparent + 35 || errno)
		return;
	trace_context->parent_id = strtol(&traceparent[36], &endptr, 16);
	if (endptr != traceparent + 52 || errno)
		return;
	trace_context->sampled = strtol(&traceparent[53], &endptr, 16);
	if (endptr != traceparent + 55 || errno)

		/*
		 * Just to be sure, reset sampled on error
		 */
		trace_context->sampled = 0;
}


/*
 * comp_location: comparator for qsorting LocationLen structs by location
 */
static int
comp_location(const void *a, const void *b)
{
	int			l = ((const LocationLen *) a)->location;
	int			r = ((const LocationLen *) b)->location;

	if (l < r)
		return -1;
	else if (l > r)
		return +1;
	else
		return 0;
}


/*
 * Normalise query and fill param_str.
 * Normalised query will separate tokens with a single space and
 * parameters are replaced by $1, $2...
 * Parameters are put in the param_str wich will contain all parameters values
 * using the format: "$1 = 0, $2 = 'v'"
 */
const char *
normalise_query_parameters(const JumbleState *jstate, const char *query,
						   int query_loc, int *query_len_p, char **param_str, int *param_len)
{
	char	   *norm_query;
	int			query_len = *query_len_p;
	int			norm_query_buflen,	/* Space allowed for norm_query */
				n_quer_loc = 0;
	LocationLen *locs;
	core_yyscan_t yyscanner;
	core_yy_extra_type yyextra;
	core_YYSTYPE yylval;
	YYLTYPE		yylloc;
	int			current_loc = 0;
	StringInfoData buf;

	initStringInfo(&buf);

	if (query_loc == -1)
	{
		/* If query location is unknown, distrust query_len as well */
		query_loc = 0;
		query_len = strlen(query);
	}
	else
	{
		/* Length of 0 (or -1) means "rest of string" */
		if (query_len <= 0)
			query_len = strlen(query);
		else
			Assert(query_len <= strlen(query));
	}

	norm_query_buflen = query_len + jstate->clocations_count * 10;
	Assert(norm_query_buflen > 0);
	locs = jstate->clocations;

	/* Allocate result buffer */
	norm_query = palloc(norm_query_buflen + 1);

	if (jstate->clocations_count > 1)
		qsort(jstate->clocations, jstate->clocations_count,
			  sizeof(LocationLen), comp_location);

	/* initialize the flex scanner --- should match raw_parser() */
	yyscanner = scanner_init(query + query_loc,
							 &yyextra,
							 &ScanKeywords,
							 ScanKeywordTokens);

	for (;;)
	{
		int			loc = locs[current_loc].location;
		int			tok;

		loc -= query_loc;

		tok = core_yylex(&yylval, &yylloc, yyscanner);

		/*
		 * We should not hit end-of-string, but if we do, behave sanely
		 */
		if (tok == 0)
			break;				/* out of inner for-loop */
		if (yylloc > query_len)
			break;

		/*
		 * We should find the token position exactly, but if we somehow run
		 * past it, work with that.
		 */
		if (current_loc < jstate->clocations_count && yylloc >= loc)
		{
			appendStringInfo(&buf,
							 "%s$%d = ",
							 current_loc > 0 ? ", " : "",
							 current_loc + 1);
			if (query[loc] == '-')
			{
				/*
				 * It's a negative value - this is the one and only case where
				 * we replace more than a single token.
				 *
				 * Do not compensate for the core system's special-case
				 * adjustment of location to that of the leading '-' operator
				 * in the event of a negative constant.  It is also useful for
				 * our purposes to start from the minus symbol.  In this way,
				 * queries like "select * from foo where bar = 1" and "select *
				 * from foo where bar = -2" will have identical normalized
				 * query strings.
				 */
				appendStringInfoChar(&buf, '-');
				tok = core_yylex(&yylval, &yylloc, yyscanner);
				if (tok == 0)
					break;		/* out of inner for-loop */
			}
			if (yylloc > 0 && yyextra.scanbuf[yylloc - 1] == ' ' && n_quer_loc > 0)
			{
				norm_query[n_quer_loc++] = ' ';
			}

			/*
			 * Append the current parameter $x in the normalised query
			 */
			n_quer_loc += sprintf(norm_query + n_quer_loc, "$%d",
								  current_loc + 1 + jstate->highest_extern_param_id);

			appendStringInfoString(&buf, yyextra.scanbuf + yylloc);

			current_loc++;
		}
		else
		{
			int			to_copy;

			if (yylloc > 0 && yyextra.scanbuf[yylloc - 1] == ' ' && n_quer_loc > 0)
			{
				norm_query[n_quer_loc++] = ' ';
			}
			to_copy = strlen(yyextra.scanbuf + yylloc);
			Assert(n_quer_loc + to_copy < norm_query_buflen + 1);
			memcpy(norm_query + n_quer_loc, yyextra.scanbuf + yylloc, to_copy);
			n_quer_loc += to_copy;
		}
	}
	scanner_finish(yyscanner);

	*query_len_p = n_quer_loc;
	norm_query[n_quer_loc] = '\0';
	*param_str = buf.data;
	*param_len = buf.len;
	return norm_query;
}

/*
 * Normalise query: tokens will be separated by a single space
 */
const char *
normalise_query(const char *query, int query_loc, int *query_len_p)
{
	char	   *norm_query;
	int			query_len = *query_len_p;
	int			norm_query_buflen = query_len;
	int			n_quer_loc = 0;
	core_yyscan_t yyscanner;
	core_yy_extra_type yyextra;
	core_YYSTYPE yylval;
	YYLTYPE		yylloc;

	/* Allocate result buffer */
	norm_query = palloc(norm_query_buflen + 2);

	/* initialize the flex scanner --- should match raw_parser() */
	yyscanner = scanner_init(query + query_loc, &yyextra, &ScanKeywords, ScanKeywordTokens);
	for (;;)
	{
		int			tok;
		int			to_copy;

		tok = core_yylex(&yylval, &yylloc, yyscanner);

		if (tok == 0)
			break;				/* out of inner for-loop */
		if (yylloc > query_len)
			break;

		if (yylloc > 0 && yyextra.scanbuf[yylloc - 1] == ' ' && n_quer_loc > 0)
		{
			norm_query[n_quer_loc++] = ' ';
		}
		to_copy = strlen(yyextra.scanbuf + yylloc);
		Assert(n_quer_loc + to_copy < norm_query_buflen + 2);
		memcpy(norm_query + n_quer_loc, yyextra.scanbuf + yylloc, to_copy);
		n_quer_loc += to_copy;
	}
	scanner_finish(yyscanner);

	*query_len_p = n_quer_loc;
	norm_query[n_quer_loc] = '\0';
	return norm_query;
}

/*
 * Store text in the pg_tracing stat file
 */
bool
text_store_file(pgTracingSharedState * pg_tracing, const char *text, int text_len,
				Size *query_offset)
{
	Size		off;
	int			fd;

	{
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;

		SpinLockAcquire(&s->mutex);
		off = s->extent;
		s->extent += text_len + 1;
		s->n_writers++;
		SpinLockRelease(&s->mutex);
	}

	/*
	 * Don't allow the file to grow larger than what qtext_load_file can
	 * (theoretically) handle.  This has been seen to be reachable on 32-bit
	 * platforms.
	 */
	if (unlikely(text_len >= MaxAllocHugeSize - off))
	{
		errno = EFBIG;			/* not quite right, but it'll do */
		fd = -1;
		goto error;
	}

	/* Now write the data into the successfully-reserved part of the file */
	fd = OpenTransientFile(PG_TRACING_TEXT_FILE, O_RDWR | O_CREAT | PG_BINARY);
	if (fd < 0)
		goto error;

	if (pg_pwrite(fd, text, text_len, off) != text_len)
		goto error;
	if (pg_pwrite(fd, "\0", 1, off + text_len) != 1)
		goto error;

	CloseTransientFile(fd);

	/* Mark our write complete */
	{
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;

		SpinLockAcquire(&s->mutex);
		s->n_writers--;
		SpinLockRelease(&s->mutex);
	}

	/*
	 * Set offset once write was succesful
	 */
	*query_offset = off;

	return true;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not write file \"%s\": %m",
					PG_TRACING_TEXT_FILE)));

	if (fd >= 0)
		CloseTransientFile(fd);

	/* Mark our write complete */
	{
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;

		SpinLockAcquire(&s->mutex);
		s->n_writers--;
		SpinLockRelease(&s->mutex);
	}

	return false;
}

/*
 * Read the external query text file into a malloc'd buffer.
 *
 * Returns NULL (without throwing an error) if unable to read, eg file not
 * there or insufficient memory.
 *
 * On success, the buffer size is also returned into *buffer_size.
 *
 * This can be called without any lock on pgss->lock, but in that case the
 * caller is responsible for verifying that the result is sane.
 */
const char *
qtext_load_file(Size *buffer_size)
{
	char	   *buf;
	int			fd;
	struct stat stat;
	Size		nread;

	fd = OpenTransientFile(PG_TRACING_TEXT_FILE, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		if (errno != ENOENT)
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m",
							PG_TRACING_TEXT_FILE)));
		return NULL;
	}

	/* Get file length */
	if (fstat(fd, &stat))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m",
						PG_TRACING_TEXT_FILE)));
		CloseTransientFile(fd);
		return NULL;
	}

	/* Allocate buffer; beware that off_t might be wider than size_t */
	if (stat.st_size <= MaxAllocHugeSize)
		buf = (char *) malloc(stat.st_size);
	else
		buf = NULL;
	if (buf == NULL)
	{
		ereport(LOG,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Could not allocate enough memory to read file \"%s\".",
						   PG_TRACING_TEXT_FILE)));
		CloseTransientFile(fd);
		return NULL;
	}

	/*
	 * OK, slurp in the file.  Windows fails if we try to read more than
	 * INT_MAX bytes at once, and other platforms might not like that either,
	 * so read a very large file in 1GB segments.
	 */
	nread = 0;
	while (nread < stat.st_size)
	{
		int			toread = Min(1024 * 1024 * 1024, stat.st_size - nread);

		/*
		 * If we get a short read and errno doesn't get set, the reason is
		 * probably that garbage collection truncated the file since we did
		 * the fstat(), so we don't log a complaint --- but we don't return
		 * the data, either, since it's most likely corrupt due to concurrent
		 * writes from garbage collection.
		 */
		errno = 0;
		if (read(fd, buf + nread, toread) != toread)
		{
			if (errno)
				ereport(LOG,
						(errcode_for_file_access(),
						 errmsg("could not read file \"%s\": %m",
								PG_TRACING_TEXT_FILE)));
			free(buf);
			CloseTransientFile(fd);
			return NULL;
		}
		nread += toread;
	}

	if (CloseTransientFile(fd) != 0)
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", PG_TRACING_TEXT_FILE)));

	*buffer_size = nread;
	return buf;
}
