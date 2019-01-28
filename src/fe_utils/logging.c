/*-------------------------------------------------------------------------
 * Logging framework for frontend programs
 *
 * Copyright (c) 2018, PostgreSQL Global Development Group
 *
 * src/fe_utils/logging.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <unistd.h>

#include "fe_utils/logging.h"

static const char *progname;

enum pg_log_level __pg_log_level;
static int log_flags;
void (*log_pre_callback)(void);
void (*log_locus_callback)(const char **, uint64 *);
static bool log_color;

const char *sgr_error = "01;31";
const char *sgr_warning = "01;35";
const char *sgr_locus = "01";

#define ANSI_ESCAPE_FMT "\x1b[%sm"
#define ANSI_ESCAPE_RESET "\x1b[0m"

void
pg_logging_init(const char *argv0)
{
	const char *pg_color_env = getenv("PG_COLOR");

	progname = get_progname(argv0);
	__pg_log_level = PG_LOG_INFO;

	if (pg_color_env)
	{
		if (strcmp(pg_color_env, "always") == 0 ||
			(strcmp(pg_color_env, "auto") == 0 && isatty(fileno(stderr))))
			log_color = true;
	}
}

void
pg_logging_config(int new_flags)
{
	log_flags = new_flags;
}

void
pg_logging_set_level(enum pg_log_level new_level)
{
	__pg_log_level = new_level;
}

void
pg_logging_set_pre_callback(void (*cb)(void))
{
	log_pre_callback = cb;
}

void
pg_logging_set_locus_callback(void (*cb)(const char **filename, uint64 *lineno))
{
	log_locus_callback = cb;
}

void
pg_log_generic(enum pg_log_level level, const char * pg_restrict fmt, ...)
{
	int			save_errno = errno;
	const char *filename = NULL;
	uint64		lineno = 0;
	va_list		ap;
	size_t		required_len;
	char	   *buf;

	Assert(progname);
	Assert(level);
	Assert(fmt);
	Assert(fmt[strlen(fmt) - 1] != '\n');

	fflush(stdout);
	if (log_pre_callback)
		log_pre_callback();

	if (log_locus_callback)
		log_locus_callback(&filename, &lineno);

	fmt = _(fmt);

	if (log_color)
		fprintf(stderr, ANSI_ESCAPE_FMT, sgr_locus);
	if (!(log_flags & PG_LOG_FLAG_TERSE))
		fprintf(stderr, "%s:", progname);
	if (filename)
	{
		fprintf(stderr, "%s:", filename);
		if (lineno > 0)
			fprintf(stderr, UINT64_FORMAT ":", lineno);
	}
	if (!(log_flags & PG_LOG_FLAG_TERSE) || filename)
		fprintf(stderr, " ");
	if (log_color)
		fprintf(stderr, ANSI_ESCAPE_RESET);

	if (!(log_flags & PG_LOG_FLAG_TERSE))
	switch (level)
	{
		case PG_LOG_FATAL:
			if (log_color)
				fprintf(stderr, ANSI_ESCAPE_FMT, sgr_error);
			fprintf(stderr, _("fatal: "));
			if (log_color)
				fprintf(stderr, ANSI_ESCAPE_RESET);
			break;
		case PG_LOG_ERROR:
			if (log_color)
				fprintf(stderr, ANSI_ESCAPE_FMT, sgr_error);
			fprintf(stderr, _("error: "));
			if (log_color)
				fprintf(stderr, ANSI_ESCAPE_RESET);
			break;
		case PG_LOG_WARNING:
			if (log_color)
				fprintf(stderr, ANSI_ESCAPE_FMT, sgr_warning);
			fprintf(stderr, _("warning: "));
			if (log_color)
				fprintf(stderr, ANSI_ESCAPE_RESET);
			break;
		default:
			break;
	}

	errno = save_errno;

	va_start(ap, fmt);
	required_len = vsnprintf(NULL, 0, fmt, ap) + 1;
	va_end(ap);

	buf = pg_malloc_extended(required_len, MCXT_ALLOC_NO_OOM);

	if (!buf)
	{
		/* memory trouble, just print what we can and get out of here */
		va_start(ap, fmt);
		vfprintf(stderr, fmt, ap);
		va_end(ap);
		return;
	}

	va_start(ap, fmt);
	vsnprintf(buf, required_len, fmt, ap);
	va_end(ap);

	/* strip one newline, for PQerrorMessage() */
	if (buf[required_len - 2] == '\n')
		buf[required_len - 2] = '\0';

	fprintf(stderr, "%s\n", buf);
	free(buf);
}
