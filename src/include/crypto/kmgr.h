/*-------------------------------------------------------------------------
 *
 * kmgr.h
 *	  Key management module for transparent data encryption
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * src/include/crypto/kmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef KMGR_H
#define KMGR_H

#include "common/cipher.h"
#include "common/kmgr_utils.h"
#include "storage/relfilenode.h"
#include "storage/bufpage.h"

#define KMGR_MAX_PASSPHRASE_LEN		1024
#define KMGR_PROMPT_MSG "Enter database encryption pass phrase:"

#define DataEncryptionEnabled() \
	(data_encryption_cipher > KMGR_ENCRYPTION_OFF)

#define SizeOfWrappedDEK() \
	(EncryptionKeyLen + AES256_KEY_WRAP_VALUE_LEN)

/* GUC parameter */
extern PGDLLIMPORT int data_encryption_cipher;

/* Encryption keys (TDEK and WDEK) length */
extern int EncryptionKeyLen;

/* GUC variable */
extern char *cluster_passphrase_command;

extern WrappedEncKeyWithHmac *BootStrapKmgr(int bootstrap_data_encryption_cipher);
extern void InitializeKmgr(void);
extern const char *KmgrGetMasterEncryptionKey(void);
extern void assign_data_encryption_cipher(int new_encryption_cipher,
										  void *extra);

#endif /* KMGR_H */
