/*-------------------------------------------------------------------------
 *
 * test_parser.c
 *	  test canonicalize_path
 *
 * Copyright (c) 2007-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/test_path/test_path.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

/*
 * functions
 */
PG_FUNCTION_INFO_V1(test_canonicalize_path);
PG_FUNCTION_INFO_V1(test_platform);

Datum
test_canonicalize_path(PG_FUNCTION_ARGS)
{
	char	   *path = strdup(text_to_cstring(PG_GETARG_TEXT_PP(0)));

	if (!path)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));

	canonicalize_path(path);

	PG_RETURN_TEXT_P(cstring_to_text(path));
}

Datum
test_platform(PG_FUNCTION_ARGS)
{
	char *platform;

#ifdef WIN32
	platform = strdup("Win32");
#else
	platform = strdup("Linux");
#endif

	PG_RETURN_TEXT_P(cstring_to_text(platform));
}