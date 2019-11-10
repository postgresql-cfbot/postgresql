/*-------------------------------------------------------------------------
 *
 * enc_cipher.h
 *	  This file contains definitions for structures and externs for
 *	  functions used by data encryption.
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * src/include/storage/enc_cipher.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ENC_CIPHER_H
#define ENC_CIPHER_H

extern void pg_encrypt(const char *input, char *output, int size,
					   const char *key, const char *iv);
extern void pg_decrypt(const char *input, char *output, int size,
					   const char *key, const char *iv);
extern void pg_derive_key(const unsigned char *base_key, int base_size,
						  unsigned char *info, unsigned char *derived_key,
						  Size derived_size);
extern void pg_compute_hmac(const unsigned char *hmac_key, int key_size,
							unsigned char *data, int data_size,
							unsigned char *hmac);
extern void pg_wrap_key(const unsigned char *key, int key_size,
						unsigned char *in, int in_size, unsigned char *out,
						int *out_size);
extern void pg_unwrap_key(const unsigned char *key, int key_size,
						  unsigned char *in, int in_size, unsigned char *out,
						  int *out_size);

#endif	/* ENC_CIPHER_H */
