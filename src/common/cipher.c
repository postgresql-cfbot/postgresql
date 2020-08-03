/*-------------------------------------------------------------------------
 *
 * cipher.c
 *	  Shared frontend/backend for cryptographic functions
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/cipher.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/cipher.h"
#ifdef USE_OPENSSL
#include "common/cipher_openssl.h"
#endif

/*
 * Return a newly created cipher context.  'cipher' specifies cipher algorithm
 * by identifer like PG_CIPHER_XXX.
 */
PgCipherCtx *
pg_cipher_ctx_create(int cipher, uint8 *key, int klen)
{
	PgCipherCtx *ctx = NULL;

	if (cipher >= PG_MAX_CIPHER_ID)
		return NULL;

#ifdef USE_OPENSSL
	ctx = (PgCipherCtx *) palloc0(sizeof(PgCipherCtx));

	ctx->encctx = ossl_cipher_ctx_create(cipher, key, klen, true);
	ctx->decctx = ossl_cipher_ctx_create(cipher, key, klen, false);
#endif

	return ctx;
}

void
pg_cipher_ctx_free(PgCipherCtx *ctx)
{
#ifdef USE_OPENSSL
	ossl_cipher_ctx_free(ctx->encctx);
	ossl_cipher_ctx_free(ctx->decctx);
#endif
}

bool
pg_cipher_encrypt(PgCipherCtx *ctx, const uint8 *in, int inlen,
				  uint8 *out, int *outlen, const uint8 *iv)
{
	bool		r = false;
#ifdef USE_OPENSSL
	r = ossl_cipher_encrypt(ctx->encctx, in, inlen, out, outlen, iv);
#endif
	return r;
}

bool
pg_cipher_decrypt(PgCipherCtx *ctx, const uint8 *in, int inlen,
				  uint8 *out, int *outlen, const uint8 *iv)
{
	bool		r = false;
#ifdef USE_OPENSSL
	r = ossl_cipher_decrypt(ctx->decctx, in, inlen, out, outlen, iv);
#endif
	return r;
}

bool
pg_HMAC_SHA512(const uint8 *key, const uint8 *in, int inlen,
			   uint8 *out)
{
	bool		r = false;
#ifdef USE_OPENSSL
	r = ossl_HMAC_SHA512(key, in, inlen, out);
#endif
	return r;
}
