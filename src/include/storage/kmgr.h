/*-------------------------------------------------------------------------
 *
 * kmgr.h
 *	  Key management module for transparent data encryption
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * src/include/storage/kmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef KMGR_H
#define KMGR_H

#include "storage/relfilenode.h"
#include "storage/bufpage.h"

/* Size of key encryption key (KEK), which is always AES-256 key */
#define TDE_KEK_SIZE				32

/*
 * Size of HMAC key for KEK is the same as the length of hash, we use
 * SHA-256.
 */
#define TDE_KEK_HMAC_KEY_SIZE		32

/* SHA-256 results 256 bits HMAC */
#define TDE_KEK_HMAC_SIZE			32

/*
 * Size of the derived key from passphrase. It consists of KEK and HMAC key.
 */
#define TDE_KEK_DERIVED_KEY_SIZE	(TDE_KEK_SIZE + TDE_KEK_HMAC_KEY_SIZE)

/*
 * Iteration count of password based key derivation. NIST recommends that
 * minimum iteration count is 1000.
 */
#define TDE_KEK_DEVIRATION_ITER_COUNT	100000

/*
 * Size of salt for password based key derivation. NIST recommended that
 * salt size is at least 16
 */
#define TDE_KEK_DEVIRATION_SALT_SIZE	64

/* Key wrapping appends the initial 64 bit value */
#define TDE_KEY_WRAP_VALUE_SIZE		8

/* Size of master data encryption key (MDEK), which is always AES-256 key */
#define TDE_MDEK_SIZE				32

/* Wrapped key size is n+1 value */
#define TDE_MDEK_WRAPPED_SIZE		(TDE_MDEK_SIZE + TDE_KEY_WRAP_VALUE_SIZE)

#define TDE_MAX_PASSPHRASE_LEN		MAXPGPATH

/* GUC variable */
extern char *cluster_passphrase_command;

extern void BootStrapKmgr(int bootstrap_data_encryption_cipher);
extern Size KmgrShmemSize(void);
extern void KmgrShmemInit(void);
extern void InitializeKmgr(void);
extern const char *GetTableEncryptionKey(void);
extern const char *GetWALEncryptionKey(void);

#endif /* KMGR_H */
