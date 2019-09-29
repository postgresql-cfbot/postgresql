/*-------------------------------------------------------------------------
 *
 * enc_internal.h
 *	  This file contains internal definitions of encryption cipher
 *	  functions.
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * src/include/storage/enc_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ENC_INTERNAL_H
#define ENC_INTERNAL_H

/* enc_openssl.h */
extern void ossl_encrypt_data(const char *input, char *output, int size,
							  const char *key, const char *iv);
extern void ossl_decrypt_data(const char *input, char *output, int size,
							  const char *key, const char *iv);
extern void ossl_derive_key_passphrase(const char *passphrase, int pass_size,
									   unsigned char *salt, int salt_size,
									   int iter_cnt, int derived_size,
									   unsigned char *derived_key);
extern void ossl_derive_key(const unsigned char *base_key, int base_size,
							unsigned char *info, unsigned char *derived_key,
							Size derived_size);
extern void ossl_compute_hmac(const unsigned char *hmac_key, int key_size,
							  unsigned char *data, int data_size,
							  unsigned char *hmac);
extern void ossl_wrap_key(const unsigned char *kek, int key_size,
						  unsigned char *in, int in_size, unsigned char *out,
						  int *out_size);
extern void ossl_unwrap_key(const unsigned char *key, int key_size,
							unsigned char *in, int in_size, unsigned char *out,
							int *out_size);

#endif /* ENC_INTERNAL_H */
