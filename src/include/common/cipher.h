/*-------------------------------------------------------------------------
 *
 * cipher.h
 *		Declarations for cryptographic functions
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * src/include/common/cipher.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CIPHER_H
#define PG_CIPHER_H

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
#define PG_CIPHER_AES_GCM			0
#define PG_CIPHER_AES_CTR			1
#define PG_CIPHER_AES_KWP			2
#define PG_CIPHER_AES_XTS			3
#define PG_MAX_CIPHER_ID			3
#define SizeOfEncryptionTag(m) encryption_methods[m].authtag_len
#define EncryptionAlgorithm(m) encryption_methods[m].algorithm
#define EncryptionKeyLength(m) encryption_methods[m].key_length
#define EncryptionBlockLength(m) (encryption_methods[m].block_length/8)

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

#ifdef USE_OPENSSL
typedef EVP_CIPHER_CTX PgCipherCtx;
#else
typedef void PgCipherCtx;
#endif

typedef Pointer EncryptionHandle;

extern PgCipherCtx *pg_cipher_ctx_create(int cipher, unsigned char *key, int klen,
										 bool enc);

extern void pg_cipher_ctx_free(PgCipherCtx *ctx);
extern bool pg_cipher_encrypt(PgCipherCtx *ctx, int cipher,
							  const unsigned char *plaintext, const int inlen,
							  unsigned char *ciphertext, int *outlen,
							  const unsigned char *iv, const int ivlen,
							  const unsigned char *aad, const int aadlen,
							  unsigned char *tag, const int taglen);
extern bool pg_cipher_encrypt_ex(PgCipherCtx *ctx, int cipher,
							  const unsigned char **plaintext, const int *inlen,
							  int nchunks,
							  unsigned char *ciphertext, int *outlen,
							  const unsigned char *iv, const int ivlen,
							  const unsigned char *aad, const int aadlen,
							  unsigned char *tag, const int taglen);
extern bool pg_cipher_decrypt(PgCipherCtx *ctx, const int cipher,
							  const unsigned char *ciphertext, const int inlen,
							  unsigned char *plaintext, int *outlen,
							  const unsigned char *iv, const int ivlen,
							  const unsigned char *aad, const int aadlen,
							  unsigned char *intag, const int taglen);


/* handle incremental encryption; returns context */
extern EncryptionHandle pg_cipher_incr_init(PgCipherCtx *ctx, const int cipher,
											const unsigned char *iv, const int ivlen);
extern bool pg_cipher_incr_add_authenticated_data(EncryptionHandle incr,
												  const unsigned char *aad,
												  const int aadlen);
extern bool pg_cipher_incr_encrypt(EncryptionHandle incr,
								   const unsigned char *plaintext,
								   const int inlen,
								   unsigned char *ciphertext, int *outlen);
extern bool pg_cipher_incr_finish(EncryptionHandle incr,
								  unsigned char *ciphertext, int *outlen,
								  unsigned char *tag, const int taglen);

extern bool pg_cipher_keywrap(PgCipherCtx *ctx,
							  const unsigned char *plaintext, const int inlen,
							  unsigned char *ciphertext, int *outlen);
extern bool pg_cipher_keyunwrap(PgCipherCtx *ctx,
								const unsigned char *ciphertext, const int inlen,
								unsigned char *plaintext, int *outlen);

extern int	pg_cipher_blocksize(PgCipherCtx *ctx);

#endif							/* PG_CIPHER_H */
