/*-------------------------------------------------------------------------
 *
 * enc_common.h
 *	  This file contains common definitions for cluster encryption.
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * src/include/storage/enc_common.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ENC_COMMON_H
#define ENC_COMMON_H

/* Value of data_encryption_cipher */
enum
{
	TDE_ENCRYPTION_OFF = 0,
	TDE_ENCRYPTION_AES_128,
	TDE_ENCRYPTION_AES_256
};

/* GUC parameter */
extern PGDLLIMPORT int data_encryption_cipher;

/* Encryption keys (TDEK and WDEK) size */
extern int EncryptionKeySize;

extern char *EncryptionCipherString(int value);
extern int EncryptionCipherValue(const char *name);
extern void assign_data_encryption_cipher(int new_encryption_cipher,
										  void *extra);

#endif /* ENC_COMMON_H */
