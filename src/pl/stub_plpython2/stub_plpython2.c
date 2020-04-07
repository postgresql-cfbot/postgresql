/*
 * PL/Python stub for Python 2, in an environment that has only Python 3
 *
 * Our strategy is to pass through "plpythonu" functions to Python 3,
 * but throw a not-implemented error for "plpython2u".
 *
 * Pass-through is implemented by using dfmgr.c to look up the appropriate
 * function in plpython3.so, rather than trying to resolve the reference
 * directly.  This greatly simplifies building this as an independent
 * shared library, and it ensures that we can't somehow pull in a different
 * version of plpython3 (and thence libpython) than would get loaded for
 * a plpython3u function.
 *
 * src/pl/stub_plpython2/stub_plpython2.c
 */

#include "postgres.h"

#include "fmgr.h"

#define PLPYTHON_LIBNAME "$libdir/plpython3"

/*
 * exported functions
 */

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(plpython_validator);
PG_FUNCTION_INFO_V1(plpython_call_handler);
PG_FUNCTION_INFO_V1(plpython_inline_handler);

PG_FUNCTION_INFO_V1(plpython2_validator);
PG_FUNCTION_INFO_V1(plpython2_call_handler);
PG_FUNCTION_INFO_V1(plpython2_inline_handler);


Datum
plpython_validator(PG_FUNCTION_ARGS)
{
	static PGFunction plpython3_validator = NULL;

	if (plpython3_validator == NULL)
		plpython3_validator =
			load_external_function(PLPYTHON_LIBNAME,
								   "plpython3_validator",
								   true, NULL);

	return (*plpython3_validator) (fcinfo);
}

Datum
plpython_call_handler(PG_FUNCTION_ARGS)
{
	static PGFunction plpython3_call_handler = NULL;

	if (plpython3_call_handler == NULL)
		plpython3_call_handler =
			load_external_function(PLPYTHON_LIBNAME,
								   "plpython3_call_handler",
								   true, NULL);

	return (*plpython3_call_handler) (fcinfo);
}

Datum
plpython_inline_handler(PG_FUNCTION_ARGS)
{
	static PGFunction plpython3_inline_handler = NULL;

	if (plpython3_inline_handler == NULL)
		plpython3_inline_handler =
			load_external_function(PLPYTHON_LIBNAME,
								   "plpython3_inline_handler",
								   true, NULL);

	return (*plpython3_inline_handler) (fcinfo);
}

Datum
plpython2_validator(PG_FUNCTION_ARGS)
{
	/* It seems more convenient to do nothing here than throw an error. */
	PG_RETURN_VOID();
}

Datum
plpython2_call_handler(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("Python 2 is no longer supported"),
			 errhint("Convert the function to use plpython3u.")));
	PG_RETURN_NULL();			/* keep compiler quiet */
}

Datum
plpython2_inline_handler(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("Python 2 is no longer supported"),
			 errhint("Convert the DO block to use plpython3u.")));
	PG_RETURN_NULL();			/* keep compiler quiet */
}
