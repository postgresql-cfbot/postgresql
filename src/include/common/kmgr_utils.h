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

/* Current version number */
#define KMGR_VERSION 1

/*
 * Directory where cryptographic keys reside within PGDATA. KMGR_DIR_TMP
 * is used during cluster passphrase rotation.
 */
#define KMGR_DIR			"pg_cryptokeys"
#define KMGR_TMP_DIR		"pg_cryptokeys_tmp"

/*
 * Identifiers of internal keys.  When adding a new internal key, we
 * also need to add its key length to internalKeyLengths.
 */
/* #define KMGR_XXX_KEY_ID 0 */
#define KMGR_MAX_INTERNAL_KEYS	0

/* Encryption key and MAC key used for key wrapping */
#define KMGR_ENC_KEY_LEN			PG_AES256_KEY_LEN
#define KMGR_MAC_KEY_LEN			PG_HMAC_SHA512_KEY_LEN
#define KMGR_HMAC_LEN				PG_HMAC_SHA512_LEN

/* Key wrapping key consists of encryption key and mac key */
#define KMGR_KEY_LEN				(PG_AEAD_ENC_KEY_LEN + PG_AEAD_MAC_KEY_LEN)

/* Allowed length of cluster passphrase */
#define KMGR_MIN_PASSPHRASE_LEN 	64
#define KMGR_MAX_PASSPHRASE_LEN		1024

/* Maximum length of key the key manager can store */
#define KMGR_MAX_KEY_LEN			128
#define KMGR_MAX_WRAPPED_KEY_LEN	KmgrSizeOfCipherText(KMGR_MAX_KEY_LEN)

/*
 * Size of encrypted key size with padding. We use PKCS#7 padding,
 * described in RFC 5652.
 */
#define SizeOfDataWithPadding(klen) \
	((int)(klen) + (PG_AES_BLOCK_SIZE - ((int)(klen) % PG_AES_BLOCK_SIZE)))

/* Macros to compute the size of cipher text and plain text */
#define KmgrSizeOfCipherText(len) \
	(KMGR_MAC_KEY_LEN + PG_AES_IV_SIZE + SizeOfDataWithPadding((int)(len)))
#define KmgrSizeOfPlainText(klen) \
	((int)(klen) - (KMGR_MAC_KEY_LEN + PG_AES_IV_SIZE))

/* CryptoKey file name is keys id */
#define CryptoKeyFilePath(path, dir, id) \
	snprintf((path), MAXPGPATH, "%s/%04X", (dir), (id))

/*
 * Cryptographic key data structure. This structure is used for
 * both on-disk (raw key) and on-memory (wrapped key).
 */
typedef struct CryptoKey
{
	int		klen;
	uint8	key[KMGR_MAX_WRAPPED_KEY_LEN];
} CryptoKey;

/* Key wrapping cipher context */
typedef struct PgKeyWrapCtx
{
	uint8			key[KMGR_ENC_KEY_LEN];
	uint8			mackey[KMGR_MAC_KEY_LEN];
	PgCipherCtx		*cipherctx;
} PgKeyWrapCtx;

extern PgKeyWrapCtx *pg_create_keywrap_ctx(uint8 key[KMGR_ENC_KEY_LEN],
										   uint8 mackey[KMGR_MAC_KEY_LEN]);
extern void pg_free_keywrap_ctx(PgKeyWrapCtx *ctx);
extern bool kmgr_wrap_key(PgKeyWrapCtx *ctx, CryptoKey *in, CryptoKey *out);
extern bool kmgr_unwrap_key(PgKeyWrapCtx *ctx, CryptoKey *in, CryptoKey *out);



extern void kmgr_derive_keys(char *passphrase, Size passlen,
							 uint8 enckey[KMGR_ENC_KEY_LEN],
							 uint8 mackey[KMGR_MAC_KEY_LEN]);
extern bool kmgr_verify_passphrase(char *passphrase, int passlen,
								   CryptoKey *keys_in, CryptoKey *keys_out,
								   int nkey);
extern bool kmgr_wrap_key(PgKeyWrapCtx *ctx, CryptoKey *in, CryptoKey *out);
extern bool kmgr_unwrap_key(PgKeyWrapCtx *ctx, CryptoKey *in, CryptoKey *out);
extern int	kmgr_run_cluster_passphrase_command(char *passphrase_command,
												char *buf, int size);
extern CryptoKey *kmgr_get_cryptokeys(const char *path, int *nkeys);

#endif							/* KMGR_UTILS_H */
