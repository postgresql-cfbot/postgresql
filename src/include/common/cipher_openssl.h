/*-------------------------------------------------------------------------
 *
 * cipher_openssl.h
 *		Declarations for helper functions using OpenSSL
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * src/include/common/cipher_openssl.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CIPHER_OPENSSL_H
#define CIPHER_OPENSSL_H

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/cipher.h"

extern EVP_CIPHER_CTX *ossl_cipher_ctx_create(int cipher, uint8 *key, int klen,
											  bool enc);
extern void ossl_cipher_ctx_free(EVP_CIPHER_CTX *ctx);
extern bool ossl_cipher_encrypt(EVP_CIPHER_CTX *ctx,
								const uint8 *in, int inlen,
								uint8 *out, int *outlen,
								const uint8 *iv);
extern bool ossl_cipher_decrypt(EVP_CIPHER_CTX *ctx,
								const uint8 *in, int inlen,
								uint8 *out,	int *outlen,
								const uint8 *iv);
extern bool ossl_HMAC_SHA512(const uint8 *key,
							 const uint8 *in, int inlen,
							 uint8 *out);
#endif
