/*-------------------------------------------------------------------------
 *
 * cipher.h
 *		Declarations for cryptographic functions
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * src/include/common/cipher.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CIPHER_H
#define CIPHER_H

#ifdef USE_OPENSSL
#include <openssl/evp.h>
#include <openssl/conf.h>
#include <openssl/err.h>
#endif

/*
 * Supported symmetric encryption algorithm. These identifiers are passed
 * to pg_cipher_ctx_create() function, and then actual encryption
 * implementations need to initialize their context of the given encryption
 * algorithm.
 */
#define PG_CIPHER_AES_CBC			0
#define PG_MAX_CIPHER_ID			1

/* AES128/192/256 various length definitions */
#define PG_AES128_KEY_LEN			(128 / 8)
#define PG_AES192_KEY_LEN			(192 / 8)
#define PG_AES256_KEY_LEN			(256 / 8)

/*
 * The encrypted data is a series of blocks of size. Initialization
 * vector(IV) is the same size of cipher block.
 */
#define PG_AES_BLOCK_SIZE			16
#define PG_AES_IV_SIZE				(PG_AES_BLOCK_SIZE)

/* HMAC key and HMAC length. We use HMAC-SHA256 */
#define PG_HMAC_SHA512_KEY_LEN		64
#define PG_HMAC_SHA512_LEN			64

#ifdef USE_OPENSSL
typedef EVP_CIPHER_CTX cipher_private_ctx;
#else
typedef void cipher_private_ctx;
#endif

/*
 * This struct has two implementation-private context for
 * encryption and decryption. The caller must create the encryption
 * context using by pg_cipher_ctx_create() and pass the context  to
 * pg_cipher_encrypt() or pg_cipher_decrypt().
 */
typedef struct PgCipherCtx
{
	cipher_private_ctx *encctx;
	cipher_private_ctx *decctx;
} PgCipherCtx;

extern PgCipherCtx *pg_cipher_ctx_create(int cipher, uint8 *key, int klen);
extern void pg_cipher_ctx_free(PgCipherCtx *ctx);
extern bool pg_cipher_encrypt(PgCipherCtx *ctx,
							  const uint8 *in, int inlen,
							  uint8 *out, int *outlen,
							  const uint8 *iv);
extern bool pg_cipher_decrypt(PgCipherCtx *ctx,
							  const uint8 *in, int inlen,
							  uint8 *out, int *outlen,
							  const uint8 *iv);
extern bool pg_HMAC_SHA512(const uint8 *key,
						   const uint8 *in, int inlen,
						   uint8 *out);

#endif							/* CIPHER_H */
