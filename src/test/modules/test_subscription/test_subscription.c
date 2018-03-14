/*--------------------------------------------------------------------------
 *
 * test_subscription.c
 *		Code for testing logical replication subscriptions.
 *
 * Copyright (c) 2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_subscription/test_subscription.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include <utils/builtins.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(dummytext_in);
PG_FUNCTION_INFO_V1(dummytext_out);

/* Dummy input of data type function */
Datum
dummytext_in(PG_FUNCTION_ARGS)
{
	char	*inputText = PG_GETARG_CSTRING(0);

	if (inputText)
		elog(LOG, "intput text: \"%s\"", inputText);

	PG_RETURN_TEXT_P(cstring_to_text(inputText));
}

/* Dummy output of data type function */
Datum
dummytext_out(PG_FUNCTION_ARGS)
{
	Datum	txt = PG_GETARG_DATUM(0);

	PG_RETURN_CSTRING(TextDatumGetCString(txt));
}
