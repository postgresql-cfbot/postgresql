/*-------------------------------------------------------------------------
 *
 * enc_openssl.c
 *	  This code handles encryption and decryption using OpenSSL
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/encryption/enc_openssl.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "storage/enc_internal.h"
#include "storage/enc_common.h"
#include "utils/memutils.h"

#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/hmac.h>
#ifdef HAVE_OPENSSL_KDF
#include <openssl/kdf.h>
#endif

/*
 * prototype for the EVP functions that return an algorithm, e.g.
 * EVP_aes_128_cbc().
 */
typedef const EVP_CIPHER *(*ossl_EVP_cipher_func) (void);

/*
 * Supported cipher function and its key size. The index of each cipher
 * is (data_encryption_cipher - 1).
 */
ossl_EVP_cipher_func cipher_func_table[] =
{
	EVP_aes_128_ctr,	/* TDE_ENCRYPTION_AES_128 */
	EVP_aes_256_ctr		/* TDE_ENCRYPTION_AES_256 */
};

typedef struct CipherCtx
{
	/* Encryption context */
	EVP_CIPHER_CTX *enc_ctx;

	/* Decryption context */
	EVP_CIPHER_CTX *dec_ctx;

	/* Key wrap context */
	EVP_CIPHER_CTX *wrap_ctx;

	/* Key unwrap context */
	EVP_CIPHER_CTX *unwrap_ctx;

	/* Key derivation context */
	EVP_PKEY_CTX   *derive_ctx;
} CipherCtx;

CipherCtx		*MyCipherCtx = NULL;
MemoryContext	EncMemoryCtx;

static void createCipherContext(void);
static EVP_CIPHER_CTX *create_ossl_encryption_ctx(ossl_EVP_cipher_func func,
												  int klen, bool isenc,
												  bool iswrap);
static EVP_PKEY_CTX *create_ossl_derive_ctx(void);
static void setup_encryption_ossl(void);
static void setup_encryption(void) ;

static void
createCipherContext(void)
{
	ossl_EVP_cipher_func cipherfunc = cipher_func_table[data_encryption_cipher - 1];
	MemoryContext old_ctx;
	CipherCtx *cctx;

	if (MyCipherCtx != NULL)
		return;

	if (EncMemoryCtx == NULL)
		EncMemoryCtx = AllocSetContextCreate(TopMemoryContext,
											 "db encryption context",
											 ALLOCSET_DEFAULT_SIZES);

	old_ctx = MemoryContextSwitchTo(EncMemoryCtx);

	cctx = (CipherCtx *) palloc(sizeof(CipherCtx));

	/* Create encryption/decryption contexts */
	cctx->enc_ctx = create_ossl_encryption_ctx(cipherfunc,
											   EncryptionKeySize,
											   true, false);
	cctx->dec_ctx = create_ossl_encryption_ctx(cipherfunc,
											   EncryptionKeySize,
											   false, false);

	/* Create key wrap/unwrap contexts */
	cctx->wrap_ctx = create_ossl_encryption_ctx(EVP_aes_256_wrap,
												32, true, true);
	cctx->unwrap_ctx = create_ossl_encryption_ctx(EVP_aes_256_wrap,
												  32, false, true);

	/* Create key derivation context */
	cctx->derive_ctx = create_ossl_derive_ctx();

	/* Set my cipher context and key size */
	MyCipherCtx = cctx;

	MemoryContextSwitchTo(old_ctx);
}

/* Create openssl's key derivation context */
static EVP_PKEY_CTX *
create_ossl_derive_ctx(void)
{
   EVP_PKEY_CTX *pctx = NULL;

#ifdef HAVE_OPENSSL_KDF
   pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_HKDF, NULL);

   if (EVP_PKEY_derive_init(pctx) <= 0)
		ereport(ERROR,
				(errmsg("openssl encountered error during initializing derive context"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

   if (EVP_PKEY_CTX_set_hkdf_md(pctx, EVP_sha256()) <= 0)
		ereport(ERROR,
				(errmsg("openssl encountered error during setting HKDF context"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));
#endif

   return pctx;
}

/* Create openssl's encryption context */
static EVP_CIPHER_CTX *
create_ossl_encryption_ctx(ossl_EVP_cipher_func func, int klen, bool isenc,
						   bool iswrap)
{
	EVP_CIPHER_CTX *ctx;
	int ret;

	/* Create new openssl cipher context */
	ctx = EVP_CIPHER_CTX_new();

	/* Enable key wrap algorithm */
	if (iswrap)
		EVP_CIPHER_CTX_set_flags(ctx, EVP_CIPHER_CTX_FLAG_WRAP_ALLOW);

	if (ctx == NULL)
		ereport(ERROR,
				(errmsg("openssl encountered error during creating context"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	if (isenc)
		ret = EVP_EncryptInit_ex(ctx, (const EVP_CIPHER *) func(), NULL,
								 NULL, NULL);
	else
		ret = EVP_DecryptInit_ex(ctx, (const EVP_CIPHER *) func(), NULL,
								 NULL, NULL);

	if (ret != 1)
			ereport(ERROR,
					(errmsg("openssl encountered error during initializing context"),
					 (errdetail("openssl error string: %s",
								ERR_error_string(ERR_get_error(), NULL)))));

	if (!EVP_CIPHER_CTX_set_key_length(ctx, klen))
		ereport(ERROR,
				(errmsg("openssl encountered error during setting key length"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	return ctx;
}

/*
 * Initialize encryption subsystem for use. Must be called before any
 * encryptable data is read from or written to data directory.
 */
static void
setup_encryption(void)
{
	setup_encryption_ossl();
	createCipherContext();
}

static void
setup_encryption_ossl(void)
{
#ifndef HAVE_OPENSSL_KDF
	/*
	 * We can initialize openssl even with openssl is 1.0.0 or older, but
	 * since AES key wrap algorithms have introduced in openssl 1.1.0
	 * we require 1.1.0 or higher version for cluster encryption.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_CONFIG_FILE_ERROR),
			 (errmsg("openssl 1.1.0 or higher is required for cluster encryption"))));
#endif

#ifdef HAVE_OPENSSL_INIT_CRYPTO
	/* Setup OpenSSL */
	OPENSSL_init_crypto(OPENSSL_INIT_LOAD_CONFIG, NULL);
#endif
}

void
ossl_encrypt_data(const char *input, char *output, int size,
				  const char *key, const char *iv)
{
	int			out_size;
	EVP_CIPHER_CTX *ctx;

	/* Ensure encryption has setup */
	if (MyCipherCtx == NULL)
		setup_encryption();

	ctx = MyCipherCtx->enc_ctx;

	if (EVP_EncryptInit_ex(ctx, NULL, NULL, (unsigned char *) key,
						   (unsigned char *) iv) != 1)
		ereport(ERROR,
				(errmsg("openssl encountered initialization error during encryption"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	if (EVP_EncryptUpdate(ctx, (unsigned char *) output,
						  &out_size, (unsigned char *) input, size) != 1)
		ereport(ERROR,
				(errmsg("openssl encountered error during encryption"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	Assert(out_size == size);
}

void
ossl_decrypt_data(const char *input, char *output, int size,
				  const char *key, const char *iv)
{
	int			out_size;
	EVP_CIPHER_CTX *ctx;

	/* Ensure encryption has setup */
	if (MyCipherCtx == NULL)
		setup_encryption();

	ctx = MyCipherCtx->dec_ctx;

	if (EVP_DecryptInit_ex(ctx, NULL, NULL, (unsigned char *) key,
						   (unsigned char *) iv) != 1)
		ereport(ERROR,
				(errmsg("openssl encountered initialization error during decryption"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	if (EVP_DecryptUpdate(ctx, (unsigned char *) output,
						  &out_size, (unsigned char *) input, size) != 1)
		ereport(ERROR,
				(errmsg("openssl encountered error during decryption"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	Assert(out_size == size);
}

void
ossl_derive_key(const unsigned char *base_key, int base_size, unsigned char *info,
				unsigned char *derived_key, Size derived_size)
{
#ifdef HAVE_OPENSSL_KDF
   EVP_PKEY_CTX *pctx;

   pctx = MyCipherCtx->derive_ctx;

   if (EVP_PKEY_CTX_set1_hkdf_key(pctx, base_key, base_size) != 1)
	   ereport(ERROR,
			   (errmsg("openssl encountered setting key error during key derivation"),
				(errdetail("openssl error string: %s",
						   ERR_error_string(ERR_get_error(), NULL)))));

   /*
	* we don't need to set salt since the input key is already present
	* as cryptographically strong.
	*/

   if (EVP_PKEY_CTX_add1_hkdf_info(pctx, (unsigned char *) info,
								   strlen((char *) info)) != 1)
	   ereport(ERROR,
			   (errmsg("openssl encountered setting info error during key derivation"),
				(errdetail("openssl error string: %s",
						   ERR_error_string(ERR_get_error(), NULL)))));

   /*
	* The 'derivedkey_size' should contain the length of the 'derivedkey'
	* buffer, if the call got successful the derived key is written to
	* 'derivedkey' and the amount of data written to 'derivedkey_size'
	*/
   if (EVP_PKEY_derive(pctx, derived_key, &derived_size) != 1)
	   ereport(ERROR,
			   (errmsg("openssl encountered error during key derivation"),
				(errdetail("openssl error string: %s",
						   ERR_error_string(ERR_get_error(), NULL)))));
#endif
}

void
ossl_compute_hmac(const unsigned char *hmac_key, int key_size,
				  unsigned char *data, int data_size, unsigned char *hmac)
{
	unsigned char *h;
	uint32			hmac_size;

	Assert(hmac != NULL);

	h = HMAC(EVP_sha256(), hmac_key, key_size, data, data_size, hmac, &hmac_size);

	if (h == NULL)
		ereport(ERROR,
				(errmsg("could not compute HMAC"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	memcpy(hmac, h, hmac_size);
}

void
ossl_wrap_key(const unsigned char *key, int key_size, unsigned char *in,
			  int in_size, unsigned char *out, int *out_size)
{
	EVP_CIPHER_CTX *ctx;

	/* Ensure encryption has setup */
	if (MyCipherCtx == NULL)
		setup_encryption();

	ctx = MyCipherCtx->wrap_ctx;

	if (EVP_EncryptInit_ex(ctx, NULL, NULL, key, NULL) != 1)
		ereport(ERROR,
				(errmsg("openssl encountered initialization error during unwrapping key"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	if (!EVP_CIPHER_CTX_set_key_length(ctx, key_size))
		ereport(ERROR,
				(errmsg("openssl encountered setting key length error during wrapping key"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	if (!EVP_EncryptUpdate(ctx, out, out_size, in, in_size))
		ereport(ERROR,
				(errmsg("openssl encountered error during wrapping key"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));
}

void
ossl_unwrap_key(const unsigned char *key, int key_size, unsigned char *in,
				int in_size, unsigned char *out, int *out_size)
{
	EVP_CIPHER_CTX *ctx;

	/* Ensure encryption has setup */
	if (MyCipherCtx == NULL)
		setup_encryption();

	ctx = MyCipherCtx->unwrap_ctx;

	if (EVP_DecryptInit_ex(ctx, NULL, NULL, key, NULL) != 1)
		ereport(ERROR,
				(errmsg("openssl encountered initialization error during unwrapping key"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	if (!EVP_CIPHER_CTX_set_key_length(ctx, key_size))
		ereport(ERROR,
				(errmsg("openssl encountered setting key length error during unwrapping key"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));

	if (EVP_DecryptUpdate(ctx, out, out_size, in, in_size) != 1)
		ereport(ERROR,
				(errmsg("openssl encountered error during unwrapping key"),
				 (errdetail("openssl error string: %s",
							ERR_error_string(ERR_get_error(), NULL)))));
}
