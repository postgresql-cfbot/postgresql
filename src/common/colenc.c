/*-------------------------------------------------------------------------
 *
 * colenc.c
 *
 * Shared code for column encryption algorithms.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/common/colenc.c
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/colenc.h"

int
get_cmkalg_num(const char *name)
{
	if (strcmp(name, "unspecified") == 0)
		return PG_CMK_UNSPECIFIED;
	else if (strcmp(name, "RSAES_OAEP_SHA_1") == 0)
		return PG_CMK_RSAES_OAEP_SHA_1;
	else if (strcmp(name, "RSAES_OAEP_SHA_256") == 0)
		return PG_CMK_RSAES_OAEP_SHA_256;
	else
		return 0;
}

const char *
get_cmkalg_name(int num)
{
	switch (num)
	{
		case PG_CMK_UNSPECIFIED:
			return "unspecified";
		case PG_CMK_RSAES_OAEP_SHA_1:
			return "RSAES_OAEP_SHA_1";
		case PG_CMK_RSAES_OAEP_SHA_256:
			return "RSAES_OAEP_SHA_256";
	}

	return NULL;
}

/*
 * JSON Web Algorithms (JWA) names (RFC 7518)
 *
 * This is useful for some key management systems that use these names
 * natively.
 */
const char *
get_cmkalg_jwa_name(int num)
{
	switch (num)
	{
		case PG_CMK_UNSPECIFIED:
			return NULL;
		case PG_CMK_RSAES_OAEP_SHA_1:
			return "RSA-OAEP";
		case PG_CMK_RSAES_OAEP_SHA_256:
			return "RSA-OAEP-256";
	}

	return NULL;
}

int
get_cekalg_num(const char *name)
{
	if (strcmp(name, "AEAD_AES_128_CBC_HMAC_SHA_256") == 0)
		return PG_CEK_AEAD_AES_128_CBC_HMAC_SHA_256;
	else if (strcmp(name, "AEAD_AES_192_CBC_HMAC_SHA_384") == 0)
		return PG_CEK_AEAD_AES_192_CBC_HMAC_SHA_384;
	else if (strcmp(name, "AEAD_AES_256_CBC_HMAC_SHA_384") == 0)
		return PG_CEK_AEAD_AES_256_CBC_HMAC_SHA_384;
	else if (strcmp(name, "AEAD_AES_256_CBC_HMAC_SHA_512") == 0)
		return PG_CEK_AEAD_AES_256_CBC_HMAC_SHA_512;
	else
		return 0;
}

const char *
get_cekalg_name(int num)
{
	switch (num)
	{
		case PG_CEK_AEAD_AES_128_CBC_HMAC_SHA_256:
			return "AEAD_AES_128_CBC_HMAC_SHA_256";
		case PG_CEK_AEAD_AES_192_CBC_HMAC_SHA_384:
			return "AEAD_AES_192_CBC_HMAC_SHA_384";
		case PG_CEK_AEAD_AES_256_CBC_HMAC_SHA_384:
			return "AEAD_AES_256_CBC_HMAC_SHA_384";
		case PG_CEK_AEAD_AES_256_CBC_HMAC_SHA_512:
			return "AEAD_AES_256_CBC_HMAC_SHA_512";
	}

	return NULL;
}
