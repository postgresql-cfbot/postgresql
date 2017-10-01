/*-------------------------------------------------------------------------
 *
 * sha2_gnutlsl.c
 *	  Set of wrapper routines on top of GnuTLS to support SHA-224
 *	  SHA-256, SHA-384 and SHA-512 functions.
 *
 * This should only be used if code is compiled with GnuTLS support.
 *
 * Portions Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/common/sha2_gnutls.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/sha2.h"

/* Interface routines for SHA-256 */
void
pg_sha256_init(pg_sha256_ctx *ctx)
{
	gnutls_hash_init(ctx, GNUTLS_DIG_SHA256);
}

void
pg_sha256_update(pg_sha256_ctx *ctx, const uint8 *data, size_t len)
{
	gnutls_hash(*ctx, data, len);
}

void
pg_sha256_final(pg_sha256_ctx *ctx, uint8 *dest)
{
	gnutls_hash_deinit(*ctx, dest);
}

/* Interface routines for SHA-512 */
void
pg_sha512_init(pg_sha512_ctx *ctx)
{
	gnutls_hash_init(ctx, GNUTLS_DIG_SHA512);
}

void
pg_sha512_update(pg_sha512_ctx *ctx, const uint8 *data, size_t len)
{
	gnutls_hash(*ctx, data, len);
}

void
pg_sha512_final(pg_sha512_ctx *ctx, uint8 *dest)
{
	gnutls_hash_deinit(*ctx, dest);
}

/* Interface routines for SHA-384 */
void
pg_sha384_init(pg_sha384_ctx *ctx)
{
	gnutls_hash_init(ctx, GNUTLS_DIG_SHA384);
}

void
pg_sha384_update(pg_sha384_ctx *ctx, const uint8 *data, size_t len)
{
	gnutls_hash(*ctx, data, len);
}

void
pg_sha384_final(pg_sha384_ctx *ctx, uint8 *dest)
{
	gnutls_hash_deinit(*ctx, dest);
}

/* Interface routines for SHA-224 */
void
pg_sha224_init(pg_sha224_ctx *ctx)
{
	gnutls_hash_init(ctx, GNUTLS_DIG_SHA224);
}

void
pg_sha224_update(pg_sha224_ctx *ctx, const uint8 *data, size_t len)
{
	gnutls_hash(*ctx, data, len);
}

void
pg_sha224_final(pg_sha224_ctx *ctx, uint8 *dest)
{
	gnutls_hash_deinit(*ctx, dest);
}
