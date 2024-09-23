/*-------------------------------------------------------------------------
 *
 * version.c
 *	 Returns the PostgreSQL version string
 *
 * Copyright (c) 1998-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *
 * src/backend/utils/adt/version.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/builtins.h"
#include "jit/jit.h"


Datum
pgsql_version(PG_FUNCTION_ARGS)
{
	bool jit_available = false;
	const char *jit_version = jit_get_version(&jit_available);

	/* Add jit provides's version string if available. */
	if (jit_available)
	{
		PG_RETURN_TEXT_P(cstring_to_text(psprintf("%s, %s", PG_VERSION_STR,
												  jit_version)));
	}
	else
	{
		PG_RETURN_TEXT_P(cstring_to_text(PG_VERSION_STR));
	}
}
