/*-------------------------------------------------------------------------
 *
 * authfuncs.c
 *	  Functions that assist with authentication management
 *
 * Portions Copyright (c) 2022, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/authfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq/scram.h"
#include "utils/builtins.h"

/*
 * Build a SCRAM secret that can be used for SCRAM-SHA-256 authentication.
 *
 * This function can take three arguments:
 *
 * - password: a plaintext password. This argument is required. If none of the
 *             other arguments is set, the function short circuits to use a
 *             SCRAM secret generation function that relies on defaults.
 * - salt_str_enc: a base64 encoded salt. If this is not provided, a salt using
 *                 the defaults is generated.
 * - iterations: the number of iterations to hash the password. If set to 0
 *               or less, the default number of iterations is used.
 */
Datum
scram_build_secret_sha256(PG_FUNCTION_ARGS)
{
	const char	  *password;
  char 		*salt_str = NULL;
  int	     salt_str_len = -1;
	int		iterations = 0;
	char	   *secret;

	if (PG_ARGISNULL(0))
	{
		ereport(ERROR,
			(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
			 errmsg("password must not be null")));
  }

  password = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (!PG_ARGISNULL(1))
	{
		salt_str = text_to_cstring((text *) PG_GETARG_BYTEA_PP(1));
		salt_str_len = strlen(salt_str);
	}

	if (!PG_ARGISNULL(2))
		iterations = PG_GETARG_INT32(2);

	secret = pg_be_scram_build_secret(password, salt_str, salt_str_len,
		iterations);

	Assert(secret != NULL);

	/*
   * convert the SCRAM secret to text which matches the type for
   * pg_authid.rolpassword
   */
	PG_RETURN_TEXT_P(cstring_to_text(secret));
}
