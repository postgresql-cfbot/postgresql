/*-------------------------------------------------------------------------
 *
 * kmgr.c
 *	 Encryption key management module.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/encryption/kmgr.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/xlog.h"
#include "common/sha2.h"
#include "common/kmgr_utils.h"
#include "crypto/kmgr.h"
#include "storage/fd.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"

/*
 * Key encryption key.  This key is derived from the passphrase provided
 * by user when startup.  This variable is set during verification
 * of user given passphrase. After verification, the plain key data
 * is set to this variable.
 */
static uint8 keyEncKey[KMGR_KEK_LEN];

/*
 * Mater encryption key.  Similar to key encryption key, this
 * store the plain key data.
 */
static uint8 masterEncKey[KMGR_MAX_DEK_LEN];

/* GUC variable */
char *cluster_passphrase_command = NULL;

int data_encryption_cipher;
int EncryptionKeyLen;

/*
 * This function must be called ONCE on system install. We retrieve the KEK,
 * generate the master key.
 */
WrappedEncKeyWithHmac *
BootStrapKmgr(int bootstrap_data_encryption_cipher)
{
	WrappedEncKeyWithHmac *ret_mk;
	char passphrase[KMGR_MAX_PASSPHRASE_LEN];
	uint8 hmackey[KMGR_HMAC_KEY_LEN];
	int	passlen;

	if (bootstrap_data_encryption_cipher == KMGR_ENCRYPTION_OFF)
		return NULL;

#ifndef USE_OPENSSL
	ereport(ERROR,
			(errcode(ERRCODE_CONFIG_FILE_ERROR),
			 (errmsg("cluster encryption is not supported because OpenSSL is not supported by this build"),
			  errhint("Compile with --with-openssl to use cluster encryption."))));
#endif

	ret_mk = palloc0(sizeof(WrappedEncKeyWithHmac));

	/*
	 * Set data encryption cipher so that subsequent bootstrapping process
	 * can proceed.
	 */
	SetConfigOption("data_encryption_cipher",
					kmgr_cipher_string(bootstrap_data_encryption_cipher),
					PGC_INTERNAL, PGC_S_OVERRIDE);

	/* Get key encryption key from command */
	passlen = kmgr_run_cluster_passphrase_command(cluster_passphrase_command,
												  passphrase, KMGR_MAX_PASSPHRASE_LEN);
	if (passlen < KMGR_MIN_PASSPHRASE_LEN)
		ereport(ERROR,
				(errmsg("passphrase must be more than %d bytes",
						KMGR_MIN_PASSPHRASE_LEN)));

	/* Get key encryption key and HMAC key from passphrase */
	kmgr_derive_keys(passphrase, passlen, keyEncKey, hmackey);

	/* Generate the master encryption key */
	if (!pg_strong_random(masterEncKey, EncryptionKeyLen))
		ereport(ERROR,
				(errmsg("failed to generate cluster encryption key")));

	/* Wrap the master key by KEK */
	kmgr_wrap_key(keyEncKey, masterEncKey, EncryptionKeyLen, ret_mk->key);

	/* Compute HMAC of the master key */
	kmgr_compute_HMAC(hmackey, ret_mk->key, SizeOfWrappedDEK(),
					  ret_mk->hmac);

	/* return keys and HMACs generated during bootstrap */
	return ret_mk;
}

/*
 * Get encryption key passphrase and verify it, then get the un-wrapped
 * master encryption key. This function is called by postmaster at startup time.
 */
void
InitializeKmgr(void)
{
	WrappedEncKeyWithHmac *wrapped_mk;
	char	passphrase[KMGR_MAX_PASSPHRASE_LEN];
	int		passlen;

	if (!DataEncryptionEnabled())
		return;

	/* Get cluster passphrase */
	passlen = kmgr_run_cluster_passphrase_command(cluster_passphrase_command,
												  passphrase, KMGR_MAX_PASSPHRASE_LEN);

	/* Get two wrapped keys stored in control file */
	wrapped_mk = GetMasterEncryptionKey();

	/* Verify the correctness of given passphrase */
	if (!kmgr_verify_passphrase(passphrase, passlen, wrapped_mk,
								SizeOfWrappedDEK()))
		ereport(ERROR,
				(errmsg("cluster passphrase does not match expected passphrase")));

	kmgr_derive_keys(passphrase, passlen, keyEncKey, NULL);

	/* The passphrase is correct, unwrap the master key */
	kmgr_unwrap_key(keyEncKey, wrapped_mk->key, SizeOfWrappedDEK(),
					masterEncKey);
}

/* Return plain cluster encryption key */
const char *
KmgrGetMasterEncryptionKey(void)
{
	return DataEncryptionEnabled() ?
		pstrdup((const char *) masterEncKey) : NULL;
}

extern
void assign_data_encryption_cipher(int new_encryption_cipher,
								   void *extra)
{
	switch (new_encryption_cipher)
	{
		case KMGR_ENCRYPTION_OFF :
			EncryptionKeyLen = 0;
			break;
		case KMGR_ENCRYPTION_AES128:
			EncryptionKeyLen = 16;
			break;
		case KMGR_ENCRYPTION_AES256:
			EncryptionKeyLen = 32;
			break;
	}
}

/*
 * SQL function to rotate the cluster encryption key. This function
 * assumes that the cluster_passphrase_command is already reloaded
 * to the new value.
 */
Datum
pg_rotate_encryption_key(PG_FUNCTION_ARGS)
{
	WrappedEncKeyWithHmac new_masterkey;
	WrappedEncKeyWithHmac *cur_masterkey;
	char    passphrase[KMGR_MAX_PASSPHRASE_LEN];
	uint8   new_kek[KMGR_KEK_LEN];
	uint8   new_hmackey[KMGR_HMAC_KEY_LEN];
	int     passlen;

	passlen = kmgr_run_cluster_passphrase_command(cluster_passphrase_command,
												  passphrase,
												  KMGR_MAX_PASSPHRASE_LEN);
	if (passlen < KMGR_MIN_PASSPHRASE_LEN)
		ereport(ERROR,
				(errmsg("passphrase must be more than %d bytes",
						KMGR_MIN_PASSPHRASE_LEN)));

	kmgr_derive_keys(passphrase, passlen, new_kek, new_hmackey);

	/* Copy the current master encryption key */
	memcpy(&(new_masterkey.key), masterEncKey, EncryptionKeyLen);

	/*
	 * Wrap and compute HMAC of the master key by the new key
	 * encryption key.
	 */
	kmgr_wrap_key(new_kek, new_masterkey.key, EncryptionKeyLen,
				  new_masterkey.key);
	kmgr_compute_HMAC(new_hmackey, new_masterkey.key,
					  SizeOfWrappedDEK(), new_masterkey.hmac);

	/* Update control file */
	LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
	cur_masterkey = GetMasterEncryptionKey();
	memcpy(cur_masterkey, &new_masterkey, sizeof(WrappedEncKeyWithHmac));
	UpdateControlFile();
	LWLockRelease(ControlFileLock);

	PG_RETURN_BOOL(true);
}

Datum
pg_kmgr_wrap(PG_FUNCTION_ARGS)
{
	bytea	*data = PG_GETARG_BYTEA_PP(0);
	bytea	*res;
	unsigned datalen;
	unsigned reslen;
	bool ret;

	datalen = VARSIZE_ANY_EXHDR(data);

	if (datalen % 8 != 0)
		ereport(ERROR,
				(errmsg("input data must be multiple of 8 bytes")));

	reslen = VARHDRSZ + datalen + AES256_KEY_WRAP_VALUE_LEN;
	res = palloc(reslen);

	ret = kmgr_wrap_key(keyEncKey, (uint8 *) VARDATA_ANY(data), datalen,
						(uint8 *) VARDATA(res));
	if (!ret)
		ereport(ERROR,
				(errmsg("could not wrap the given secret")));

	SET_VARSIZE(res, reslen);
	PG_RETURN_BYTEA_P(res);
}

Datum
pg_kmgr_unwrap(PG_FUNCTION_ARGS)
{
	bytea	*data = PG_GETARG_BYTEA_PP(0);
	bytea	*res;
	unsigned datalen;
	unsigned reslen;
	bool ret;

	datalen = VARSIZE_ANY_EXHDR(data);

	if (datalen % 8 != 0)
		ereport(ERROR,
				(errmsg("input data must be multiple of 8 bytes")));

	reslen = VARHDRSZ + datalen - AES256_KEY_WRAP_VALUE_LEN;
	res = palloc(reslen);

	ret = kmgr_unwrap_key(keyEncKey, (uint8 *) VARDATA_ANY(data), datalen,
						  (uint8 *) VARDATA(res));
	if (!ret)
		ereport(ERROR,
				(errmsg("could not unwrap the given secret")));

	SET_VARSIZE(res, reslen);
	PG_RETURN_BYTEA_P(res);
}
