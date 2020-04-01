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

extern pg_cipher_ctx *ossl_cipher_ctx_create(void);
extern void ossl_cipher_ctx_free(pg_cipher_ctx *ctx);
extern bool ossl_cipher_setup(void);
extern bool ossl_aes256_encrypt_init(pg_cipher_ctx *ctx, uint8 *key);
extern bool ossl_aes256_decrypt_init(pg_cipher_ctx *ctx, uint8 *key);
extern bool ossl_cipher_encrypt(pg_cipher_ctx *ctx,
								const uint8 *in, int inlen,
								const uint8 *iv, uint8 *out,
								int *outlen);
extern bool ossl_cipher_decrypt(pg_cipher_ctx *ctx,
								const uint8 *in, int inlen,
								const uint8 *iv, uint8 *out,
								int *outlen);
extern bool ossl_compute_HMAC(const uint8 *key, const uint8 *data,
							  int data_size, uint8 *result,
							  int *result_size);
#endif
