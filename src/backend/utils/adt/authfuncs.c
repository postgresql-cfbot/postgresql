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

#define SCRAM_BUILD_SECRET_HASH_STR_LEN	6

void		parse_cryptohash_type(const char *hash_type_str,
								  pg_cryptohash_type *hash_type);

/*
 * Build a SCRAM secret that can be used for SCRAM-SHA-256 authentication.
 *
 * This function can take four arguments:
 *
 * - hash_type_str: the type of hash to use when building the SCRAM secret.
 *              Currently only "sha256" is supported.
 * - password: a plaintext password. This argument is required. If none of the
 *             other arguments is set, the function short circuits to use a
 *             SCRAM secret generation function that relies on defaults.
 * - salt_str_enc: a base64 encoded salt. If this is not provided, a salt using
 *                 the defaults is generated.
 * - iterations: the number of iterations to hash the password. If set to 0
 *               or less, the default number of iterations is used.
 */
Datum
scram_build_secret_str(PG_FUNCTION_ARGS)
{
	const char *hash_type_str;
	pg_cryptohash_type hash_type;
	const char *password;
	char	   *salt_str = NULL;
	int			salt_str_len = -1;
	int			iterations = 0;
	char	   *secret;

	if (PG_ARGISNULL(0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("hash type must not be null")));
	}

	hash_type_str = text_to_cstring(PG_GETARG_TEXT_PP(0));
	parse_cryptohash_type(hash_type_str, &hash_type);

	if (PG_ARGISNULL(1))
	{
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("password must not be null")));
	}

	password = text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (!PG_ARGISNULL(2))
	{
		salt_str = text_to_cstring((text *) PG_GETARG_BYTEA_PP(2));
		salt_str_len = strlen(salt_str);
	}

	if (!PG_ARGISNULL(3))
		iterations = PG_GETARG_INT32(3);

	secret = pg_be_scram_build_secret(password, salt_str, salt_str_len,
									  iterations, hash_type);

	Assert(secret != NULL);

	/*
	 * convert the SCRAM secret to text which matches the type for
	 * pg_authid.rolpassword
	 */
	PG_RETURN_TEXT_P(cstring_to_text(secret));
}

/*
 * If "hash_type_str" is a valid cryptohash type that can be used for SCRAM
 * authetnication, set the value to "hash_type". Otherwise, return an
 * unsupported error.
 */
void
parse_cryptohash_type(const char *hash_type_str, pg_cryptohash_type *hash_type)
{
	pg_cryptohash_type result_type;

	if (pg_strncasecmp(hash_type_str, "sha256", SCRAM_BUILD_SECRET_HASH_STR_LEN) == 0)
		result_type = PG_SHA256;
	else
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("hash not supported: \"%s\"", hash_type_str),
				 errmsg("supported hashes are \"sha256\"")));

	*hash_type = result_type;
}
