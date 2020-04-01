/*-------------------------------------------------------------------------
 * cipher_openssl.c
 *		Cryptographic function using OpenSSL
 *
 * This contains the common low-level functions needed in both frontend and
 * backend, for implement the database encryption.
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
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

bool
ossl_cipher_setup(void)
{
#ifdef HAVE_OPENSSL_INIT_CRYPTO
	/* Setup OpenSSL */
	if (!OPENSSL_init_crypto(OPENSSL_INIT_LOAD_CONFIG, NULL))
		return false;
#else
	OPENSSL_config(NULL);
#endif
	return false;
}

pg_cipher_ctx *
ossl_cipher_ctx_create(void)
{
	return EVP_CIPHER_CTX_new();
}

void
ossl_cipher_ctx_free(pg_cipher_ctx *ctx)
{
	return EVP_CIPHER_CTX_free(ctx);
}

bool
ossl_aes256_encrypt_init(pg_cipher_ctx *ctx, uint8 *key)
{
	if (!EVP_EncryptInit_ex(ctx, EVP_aes_256_cbc(), NULL, NULL, NULL))
		return false;
	if (!EVP_CIPHER_CTX_set_key_length(ctx, PG_AES256_KEY_LEN))
		return false;
	if (!EVP_EncryptInit_ex(ctx, NULL, NULL, key, NULL))
		return false;

	/*
	 * Always enable padding. We don't need to check the return value as
	 * EVP_CIPHER_CTX_set_padding always returns 1.
	 */
	EVP_CIPHER_CTX_set_padding(ctx, 1);

	return true;
}

bool
ossl_aes256_decrypt_init(pg_cipher_ctx *ctx, uint8 *key)
{
	if (!EVP_DecryptInit_ex(ctx, EVP_aes_256_cbc(), NULL, NULL, NULL))
		return false;
	if (!EVP_CIPHER_CTX_set_key_length(ctx, PG_AES256_KEY_LEN))
		return false;
	if (!EVP_DecryptInit_ex(ctx, NULL, NULL, key, NULL))
		return false;

	/*
	 * Always enable padding. We don't need to check the return value as
	 * EVP_CIPHER_CTX_set_padding always returns 1.
	 */
	EVP_CIPHER_CTX_set_padding(ctx, 1);

	return true;
}

bool
ossl_cipher_encrypt(pg_cipher_ctx *ctx,
					const uint8 *in, int inlen,
					const uint8 *iv, uint8 *out,
					int *outlen)
{
	int			len;
	int			enclen;

	if (!EVP_EncryptInit_ex(ctx, NULL, NULL, NULL, iv))
		return false;

	if (!EVP_EncryptUpdate(ctx, out, &len, in, inlen))
		return false;

	enclen = len;

	if (!EVP_EncryptFinal_ex(ctx, (uint8 *) ((char *) out + enclen),
							 &len))
		return false;

	*outlen = enclen + len;

	return true;
}

bool
ossl_cipher_decrypt(pg_cipher_ctx *ctx,
					const uint8 *in, int inlen,
					const uint8 *iv, uint8 *out,
					int *outlen)
{
	int			declen;
	int			len;

	if (!EVP_DecryptInit_ex(ctx, NULL, NULL, NULL, iv))
		return false;

	if (!EVP_DecryptUpdate(ctx, out, &len, in, inlen))
		return false;

	declen = len;

	if (!EVP_DecryptFinal_ex(ctx, (uint8 *) ((char *) out + declen),
							 &len))
		return false;

	*outlen = declen + len;

	return true;
}

bool
ossl_compute_HMAC(const uint8 *key, const uint8 *data,
				  int data_size, uint8 *result,
				  int *result_size)
{
	return HMAC(EVP_sha256(), key, PG_AES256_KEY_LEN, data,
				(uint32) data_size, result, (uint32 *) result_size);
}
