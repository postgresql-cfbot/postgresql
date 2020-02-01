/*-------------------------------------------------------------------------
 *
 * cipher.c
 *	  Shared frontend/backend for cryptographic functions
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
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

pg_cipher_ctx *
pg_cipher_ctx_create(void)
{
#ifdef USE_OPENSSL
	return ossl_cipher_ctx_create();
#endif
	return NULL;
}

void
pg_cipher_setup(void)
{
#ifdef USE_OPENSSL
	ossl_cipher_setup();
#endif
}

bool
pg_aes256_ctr_wrap_init(pg_cipher_ctx *ctx)
{
#ifdef USE_OPENSSL
	return ossl_aes256_ctr_wrap_init(ctx);
#endif
	return false;
}

bool
pg_aes256_ctr_unwrap_init(pg_cipher_ctx *ctx)
{
#ifdef USE_OPENSSL
	return ossl_aes256_ctr_unwrap_init(ctx);
#endif
	return false;
}

bool
pg_cipher_encrypt(pg_cipher_ctx *ctx, const uint8 *key,
				   const uint8 *input, int input_size,
				   const uint8 *iv, uint8 *dest, int *dest_size)

{
	bool r = true;
#ifdef USE_OPENSSL
	r = ossl_cipher_encrypt(ctx, key, input, input_size, iv,
							dest, dest_size);
#endif
	return r;
}

bool
pg_cipher_decrypt(pg_cipher_ctx *ctx, const uint8 *key,
				   const uint8 *input, int input_size,
				   const uint8 *iv, uint8 *dest, int *dest_size)
{
	bool r = true;
#ifdef USE_OPENSSL
	r = ossl_cipher_decrypt(ctx, key, input, input_size, iv,
							dest, dest_size);
#endif
	return r;
}

bool
pg_compute_HMAC(const uint8 *key, const uint8 *data,
				 int data_size, uint8 *result, int *result_size)
{
	bool r = true;
#ifdef USE_OPENSSL
	r = ossl_compute_HMAC(key, data, data_size, result,
						  result_size);
#endif
	return r;
}
