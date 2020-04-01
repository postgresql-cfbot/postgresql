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
#include "common/cipher_openssl.h"

void
pg_cipher_setup(void)
{
#ifdef USE_OPENSSL
	ossl_cipher_setup();
#endif
}

pg_cipher_ctx *
pg_cipher_ctx_create(void)
{
#ifdef USE_OPENSSL
	return ossl_cipher_ctx_create();
#endif
	return NULL;
}

void
pg_cipher_ctx_free(pg_cipher_ctx *ctx)
{
#ifdef USE_OPENSSL
	ossl_cipher_ctx_free(ctx);
#endif
}

bool
pg_aes256_encrypt_init(pg_cipher_ctx *ctx, uint8 *key)
{
#ifdef USE_OPENSSL
	return ossl_aes256_encrypt_init(ctx, key);
#endif
	return false;
}

bool
pg_aes256_decrypt_init(pg_cipher_ctx *ctx, uint8 *key)
{
#ifdef USE_OPENSSL
	return ossl_aes256_decrypt_init(ctx, key);
#endif
	return false;
}

bool
pg_cipher_encrypt(pg_cipher_ctx *ctx, const uint8 *input, int input_size,
				  const uint8 *iv, uint8 *dest, int *dest_size)
{
	bool		r = false;
#ifdef USE_OPENSSL
	r = ossl_cipher_encrypt(ctx, input, input_size, iv, dest, dest_size);
#endif
	return r;
}

bool
pg_cipher_decrypt(pg_cipher_ctx *ctx, const uint8 *input, int input_size,
				  const uint8 *iv, uint8 *dest, int *dest_size)
{
	bool		r = false;
#ifdef USE_OPENSSL
	r = ossl_cipher_decrypt(ctx, input, input_size, iv, dest, dest_size);
#endif
	return r;
}

bool
pg_compute_HMAC(const uint8 *key, const uint8 *data,
				int data_size, uint8 *result, int *result_size)
{
	bool		r = true;
#ifdef USE_OPENSSL
	r = ossl_compute_HMAC(key, data, data_size, result,
						  result_size);
#endif
	return r;
}
