/*-------------------------------------------------------------------------
 * cipher_openssl.c
 *		Cryptographic function using OpenSSL
 *
 * This contains the common low-level functions needed in both frontend and
 * backend, for implement the database encryption.
 *
 * Portions Copyright (c) 2017-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/cipher_openssl.c
 *
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/cipher_openssl.h"

#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/hmac.h>

pg_cipher_ctx *
ossl_cipher_ctx_create(void)
{
	return EVP_CIPHER_CTX_new();
}

bool
ossl_cipher_setup(void)
{
#ifdef HAVE_OPENSSL_INIT_CRYPTO
	/* Setup OpenSSL */
	if (!OPENSSL_init_crypto(OPENSSL_INIT_LOAD_CONFIG, NULL))
		return false;
	return true;
#endif
	return false;
}

bool
ossl_aes256_ctr_wrap_init(pg_cipher_ctx *ctx)
{
	EVP_CIPHER_CTX_set_flags(ctx, EVP_CIPHER_CTX_FLAG_WRAP_ALLOW);
	if (!EVP_EncryptInit_ex(ctx, EVP_aes_256_wrap(), NULL, NULL, NULL))
		return false;

	EVP_CIPHER_CTX_set_key_length(ctx, AES256_KEY_LEN);
	return true;
}

bool
ossl_aes256_ctr_unwrap_init(pg_cipher_ctx *ctx)
{
	EVP_CIPHER_CTX_set_flags(ctx, EVP_CIPHER_CTX_FLAG_WRAP_ALLOW);
	if (!EVP_DecryptInit_ex(ctx, EVP_aes_256_wrap(), NULL, NULL, NULL))
		return false;

	EVP_CIPHER_CTX_set_key_length(ctx, AES256_KEY_LEN);
	return true;
}

bool
ossl_cipher_encrypt(pg_cipher_ctx *ctx, const uint8 *key,
					const uint8 *input, int input_size,
					const uint8 *iv, uint8 *dest,
					int *dest_size)
{
	if (!EVP_EncryptInit_ex(ctx, NULL, NULL, key, iv))
		return false;

	return EVP_EncryptUpdate(ctx, dest, dest_size, input, input_size);
}

bool
ossl_cipher_decrypt(pg_cipher_ctx *ctx, const uint8 *key,
					const uint8 *input, int input_size,
					const uint8 *iv, uint8 *dest,
					int *dest_size)
{
	if (!EVP_DecryptInit_ex(ctx, NULL, NULL, key, iv))
		return false;

	return EVP_DecryptUpdate(ctx, dest, dest_size, input, input_size);
}

bool
ossl_compute_HMAC(const uint8 *key, const uint8 *data,
				  int data_size, uint8 *result,
				  int *result_size)
{
	return HMAC(EVP_sha256(), key, AES256_KEY_LEN, data,
				(uint32) data_size, result, (uint32 *) result_size);
}
