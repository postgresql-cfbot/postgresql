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

#include "common/sha2.h"
#include "common/cipher_openssl.h"

#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/hmac.h>

/*
 * prototype for the EVP functions that return an algorithm, e.g.
 * EVP_aes_128_cbc().
 */
typedef const EVP_CIPHER *(*ossl_EVP_cipher_func) (void);

static bool ossl_initialized = false;

static bool ossl_cipher_setup(void);
static ossl_EVP_cipher_func get_evp_aes_cbc(int klen);

static bool
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

static ossl_EVP_cipher_func
get_evp_aes_cbc(int klen)
{
	switch (klen)
	{
		case PG_AES128_KEY_LEN:
			return EVP_aes_128_cbc;
		case PG_AES192_KEY_LEN:
			return EVP_aes_192_cbc;
		case PG_AES256_KEY_LEN:
			return EVP_aes_256_cbc;
		default:
			return NULL;
	}
}

/*
 * Initialize and return an EVP_CIPHER_CTX. Return NULL if the given
 * cipher algorithm is not supported or on failure..
 */
EVP_CIPHER_CTX *
ossl_cipher_ctx_create(int cipher, uint8 *key, int klen, bool enc)
{
	EVP_CIPHER_CTX			*ctx;
	ossl_EVP_cipher_func	func;
	int	ret;

	if (!ossl_initialized)
	{
		ossl_cipher_setup();
		ossl_initialized = true;
	}

	ctx = EVP_CIPHER_CTX_new();

	switch (cipher)
	{
		case PG_CIPHER_AES_CBC:
			func = get_evp_aes_cbc(klen);
			if (!func)
				goto failed;
			break;
		default:
			goto failed;
	}


	if (enc)
		ret = EVP_EncryptInit_ex(ctx, (const EVP_CIPHER *) func(), NULL, key, NULL);
	else
		ret = EVP_DecryptInit_ex(ctx, (const EVP_CIPHER *) func(), NULL, key, NULL);

	if (!ret)
		goto failed;

	if (!EVP_CIPHER_CTX_set_key_length(ctx, PG_AES256_KEY_LEN))
		goto failed;

	/*
	 * Always enable padding. We don't need to check the return value as
	 * EVP_CIPHER_CTX_set_padding always returns 1.
	 */
	EVP_CIPHER_CTX_set_padding(ctx, 1);

	return ctx;

failed:
	EVP_CIPHER_CTX_free(ctx);
	return NULL;
}

void
ossl_cipher_ctx_free(EVP_CIPHER_CTX *ctx)
{
	return EVP_CIPHER_CTX_free(ctx);
}

bool
ossl_cipher_encrypt(EVP_CIPHER_CTX *ctx,
					const uint8 *in, int inlen,
					uint8 *out, int *outlen,
					const uint8 *iv)
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
ossl_cipher_decrypt(EVP_CIPHER_CTX *ctx,
					const uint8 *in, int inlen,
					uint8 *out, int *outlen,
					const uint8 *iv)
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
ossl_HMAC_SHA512(const uint8 *key, const uint8 *in, int inlen,
				 uint8 *out)
{
	return HMAC(EVP_sha512(), key, PG_SHA512_DIGEST_LENGTH,
				in, (uint32) inlen, out, NULL);
}
