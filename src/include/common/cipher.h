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

/* Key length of AES256 */
#define PG_AES256_KEY_LEN		32

/*
 * The encrypted data is a series of blocks of size ENCRYPTION_BLOCK.
 * Initialization vector(IV) is the same size of cipher block.
 */
#define AES_BLOCK_SIZE	16
#define AES_IV_SIZE		(AES_BLOCK_SIZE)

/* HMAC key and HMAC length. We use HMAC-SHA256 */
#define PG_HMAC_SHA256_KEY_LEN		32
#define PG_HMAC_SHA256_LEN	32

#ifdef USE_OPENSSL
typedef EVP_CIPHER_CTX pg_cipher_ctx;
#else
typedef void pg_cipher_ctx;
#endif

extern pg_cipher_ctx *pg_cipher_ctx_create(void);
extern void pg_cipher_ctx_free(pg_cipher_ctx *ctx);
extern void pg_cipher_setup(void);
extern bool pg_aes256_encrypt_init(pg_cipher_ctx *ctx, uint8 *key);
extern bool pg_aes256_decrypt_init(pg_cipher_ctx *ctx, uint8 *key);
extern bool pg_cipher_encrypt(pg_cipher_ctx *ctx,
							  const uint8 *input, int input_size,
							  const uint8 *iv, uint8 *dest,
							  int *dest_size);
extern bool pg_cipher_decrypt(pg_cipher_ctx *ctx,
							  const uint8 *input, int input_size,
							  const uint8 *iv, uint8 *dest,
							  int *dest_size);
extern bool pg_compute_HMAC(const uint8 *key, const uint8 *data,
							int data_size, uint8 *result,
							int *result_size);

#endif							/* CIPHER_H */
