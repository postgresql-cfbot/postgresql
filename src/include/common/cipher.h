/*-------------------------------------------------------------------------
 *
 * cipher.h
 *		Declarations for cryptographic functions
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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

/* Key lengths for AES */
#define AES128_KEY_LEN		16
#define AES256_KEY_LEN		32

/*
 * The encrypted data is a series of blocks of size ENCRYPTION_BLOCK.
 * Initialization vector(IV) is the same size of cipher block.
 */
#define AES_BLOCK_SIZE 16
#define AES_IV_SIZE		(AES_BLOCK_SIZE)

/*
 * Key wrapping appends the initial 8 bytes value. Therefore
 * wrapped key size gets larger than original one.
 */
#define AES256_KEY_WRAP_VALUE_LEN		8
#define AES256_MAX_WRAPPED_KEY_LEN		(AES256_KEY_LEN + AES256_KEY_WRAP_VALUE_LEN)

/* Size of HMAC key is the same as the length of hash, we use SHA-256 */
#define SHA256_HMAC_KEY_LEN		32

/* SHA-256 results 256 bits HMAC */
#define SHA256_HMAC_LEN	32


#ifdef USE_OPENSSL
typedef EVP_CIPHER_CTX pg_cipher_ctx;
#else
typedef void pg_cipher_ctx;
#endif

extern pg_cipher_ctx *pg_cipher_ctx_create(void);
extern void pg_cipher_setup(void);
extern bool pg_aes256_ctr_wrap_init(pg_cipher_ctx *ctx);
extern bool pg_aes256_ctr_unwrap_init(pg_cipher_ctx *ctx);

extern bool pg_cipher_encrypt(pg_cipher_ctx *ctx, const uint8 *key,
							   const uint8 *input, int input_size,
							   const uint8 *iv, uint8 *dest,
							   int *dest_size);
extern bool pg_cipher_decrypt(pg_cipher_ctx *ctx, const uint8 *key,
							   const uint8 *input, int input_size,
							   const uint8 *iv, uint8 *dest,
							   int *dest_size);
extern bool pg_compute_HMAC(const uint8 *key, const uint8 *data,
							 int data_size, uint8 *result,
							 int *result_size);

#endif /* CIPHER_H */
