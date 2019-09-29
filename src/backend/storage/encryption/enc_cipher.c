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

#include "storage/enc_cipher.h"
#include "storage/enc_common.h"
#include "storage/enc_internal.h"

/* GUC parameter */
int data_encryption_cipher;
int	EncryptionKeySize;

/* Data encryption */
void
pg_encrypt(const char *input, char *output, int size,
		   const char *key, const char *iv)
{
#ifdef USE_OPENSSL
	ossl_encrypt_data(input, output, size, key, iv);
#endif
}

/* Data decryption */
void
pg_decrypt(const char *input, char *output, int size,
		   const char *key, const char *iv)
{
#ifdef USE_OPENSSL
	ossl_decrypt_data(input, output, size, key, iv);
#endif
}

/* Password based key derivation */
void
pg_derive_key_passphrase(const char *passphrase, int pass_size,
						 unsigned char *salt, int salt_size,
						 int iter_cnt, int derived_size,
						 unsigned char *derived_key)
{
#ifdef USE_OPENSSL
	ossl_derive_key_passphrase(passphrase, pass_size, salt, salt_size,
							   iter_cnt, derived_size, derived_key);
#endif
}

/* Key derivation */
void
pg_derive_key(const unsigned char *base_key, int base_size, unsigned char *info,
			  unsigned char *derived_key, Size derived_size)
{
#ifdef USE_OPENSSL
	ossl_derive_key(base_key, base_size, info, derived_key, derived_size);
#endif
}

/* Compute HMAC */
void
pg_compute_hmac(const unsigned char *hmac_key, int key_size, unsigned char *data,
				int data_size,	unsigned char *hmac)
{
#ifdef USE_OPENSSL
	ossl_compute_hmac(hmac_key, key_size, data, data_size, hmac);
#endif
}

/* Key wrap */
void
pg_wrap_key(const unsigned char *key, int key_size, unsigned char *in,
			int in_size, unsigned char *out, int *out_size)
{
#ifdef USE_OPENSSL
	ossl_wrap_key(key, key_size, in, in_size, out, out_size);
#endif
}

/* Key unwrap */
void
pg_unwrap_key(const unsigned char *key, int key_size, unsigned char *in,
			  int in_size, unsigned char *out, int *out_size)
{
#ifdef USE_OPENSSL
	ossl_unwrap_key(key, key_size, in, in_size, out, out_size);
#endif
}

/* Convert cipher name string to integer value */
int
EncryptionCipherValue(const char *name)
{
	if (strcmp(name, "aes-128") == 0)
		return TDE_ENCRYPTION_AES_128;
	else if (strcmp(name, "aes-256") == 0)
		return TDE_ENCRYPTION_AES_256;
	else
		return TDE_ENCRYPTION_OFF;
}

/* Convert integer value to cipher name string */
char *
EncryptionCipherString(int value)
{
	switch (value)
	{
		case TDE_ENCRYPTION_OFF :
			return "off";
		case TDE_ENCRYPTION_AES_128:
			return "aes-128";
		case TDE_ENCRYPTION_AES_256:
			return "aes-256";
	}

	return "unknown";
}
