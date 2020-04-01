/*-------------------------------------------------------------------------
 *
 * kmgr_utils.h
 *		Declarations for utility function for key management
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * src/include/common/kmgr_utils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef KMGR_UTILS_H
#define KMGR_UTILS_H

#include "common/cipher.h"

/*
 * Directory where cryptographic keys reside within PGDATA. KMGR_DIR_TMP
 * is used during cluster passphrase rotation.
 */
#define KMGR_DIR			"pg_cryptokeys"
#define KMGR_TMP_DIR		"pg_cryptokeys_tmp"

/* Identifiers of internal keys */
#define KMGR_SQL_KEY_ID			0
#define KMGR_TDE_BLOCk_KEY_ID	1
#define KMGR_TDE_WAL_KEY_ID		2

#define KMGR_MAX_INTERNAL_KEYS	3

/* As of now key length supports only AES-256 key */
#define KMGR_KEY_LEN		PG_AES256_KEY_LEN

/* Key management uses HMAC-256 */
#define KMGR_HMACKEY_LEN	PG_HMAC_SHA256_KEY_LEN
#define KMGR_HMAC_LEN		PG_HMAC_SHA256_LEN

/* Allowed length of cluster passphrase */
#define KMGR_MIN_PASSPHRASE_LEN 64
#define KMGR_MAX_PASSPHRASE_LEN	1024

/* Wrapped key consists of HMAC of encrypted key, IV and encrypted key */
#define KMGR_KEY_AND_HMACKEY_LEN	(KMGR_KEY_LEN + KMGR_HMACKEY_LEN)
#define KMGR_WRAPPED_KEY_LEN \
	(KMGR_HMAC_LEN + AES_IV_SIZE + SizeOfKeyWithPadding(KMGR_KEY_AND_HMACKEY_LEN))

/*
 * Size of encrypted key size with padding. We use PKCS#7 padding,
 * described in RFC 5652.
 */
#define SizeOfKeyWithPadding(klen) \
	((int)(klen) + (AES_BLOCK_SIZE - ((int)(klen) % AES_BLOCK_SIZE)))

/*
 * Macro to compute the size of wrapped and unwrapped key.  The wrapped
 * key consists of HMAC of the encrypted data, IV and the encrypted data
 * that is the same length as the input.
 */
#define SizeOfWrappedKey(klen) \
	(KMGR_HMACKEY_LEN + AES_IV_SIZE + SizeOfKeyWithPadding((int)(klen)))
#define SizeOfUnwrappedKey(klen) \
	((int)(klen) - (KMGR_HMACKEY_LEN + AES_IV_SIZE))

#define CryptoKeyFilePath(path, dir, id) \
	snprintf((path), MAXPGPATH, "%s/%04X", (dir), (id))

/* On-disk data of cryptographic keys */
typedef struct CryptoKeyOnDisk
{
	/* The key's identifier */
	uint32		id;

	/* Wrapped key data */
	uint8		key[KMGR_WRAPPED_KEY_LEN];
} CryptoKeyOnDisk;

/* Key wrapping cipher context */
typedef struct KeyWrapCtx
{
	uint8		key[KMGR_KEY_LEN];
	uint8		hmackey[KMGR_HMACKEY_LEN];
	pg_cipher_ctx *cipher;
} KeyWrapCtx;

extern KeyWrapCtx *create_keywrap_ctx(uint8 key[KMGR_KEY_LEN],
									  uint8 hmackey[KMGR_HMACKEY_LEN],
									  bool for_wrap);
extern void free_keywrap_ctx(KeyWrapCtx *ctx);
extern void kmgr_derive_keys(char *passphrase, Size passlen,
							 uint8 key[KMGR_KEY_LEN],
							 uint8 hmackey[KMGR_HMACKEY_LEN]);
extern bool kmgr_verify_passphrase(char *passphrase, int passlen,
								   uint8 wrapped_key[KMGR_WRAPPED_KEY_LEN],
								   uint8 raw_key[KMGR_WRAPPED_KEY_LEN]);
extern bool kmgr_wrap_key(KeyWrapCtx *ctx, const uint8 *in, int inlen,
						  uint8 *out, int *outlen);
extern bool kmgr_unwrap_key(KeyWrapCtx *ctx, const uint8 *in, int inlen,
							uint8 *out, int *outlen);
extern bool kmgr_compute_HMAC(KeyWrapCtx *ctx, const uint8 *in, int inlen,
							  uint8 *out);
extern int	kmgr_run_cluster_passphrase_command(char *passphrase_command,
												char *buf, int size);
extern CryptoKeyOnDisk *kmgr_get_cryptokeys(const char *path, int *nkeys);

#endif							/* KMGR_UTILS_H */
