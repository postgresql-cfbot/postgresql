/*-------------------------------------------------------------------------
 *
 * kmgr.c
 *	 Key manager routines
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * Key manager is enabled if user requests during initdb.  We have one key
 * encryption key (KEK) and three internal keys: SQL key and two TDE keys.
 * KEK is derived from the user-provided passphrase and used for wrapping the
 * internal keys.  The SQL key is used for wrap and unwrap the user secret
 * via pg_wrap and pg_unwrap SQL functions.  The two TDE keys will be used
 * for other encryption feature such as transparent data encryption in the
 * future.  An internal key consists of encryption key and HMAC key.  Wrapping
 * process encrypts data and append HMAC of the encrypted data.  And unwrapping
 * process does the integrity check of the wrapped data and decrypt data.
 * These internal keys are wrapped by KEK and stored into each file located at
 * KMGR_DIR.
 *
 * IDENTIFICATION
 *	  src/backend/crypto/kmgr.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "common/sha2.h"
#include "common/kmgr_utils.h"
#include "crypto/kmgr.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"

/* GUC variables */
bool		key_management_enabled = false;;
char	   *cluster_passphrase_command = NULL;

static MemoryContext KmgrCtx = NULL;

/*
 * Cache of the internal keys. These are used for key rotation so that
 * we can encrypt them with the new key encryption key.
 */
static uint8 *internalKeys[KMGR_MAX_INTERNAL_KEYS];

/* Key wrap and unwrap contexts initialized with the SQL keys */
static KeyWrapCtx *WrapCtx = NULL;
static KeyWrapCtx *UnwrapCtx = NULL;

static void ShutdownKmgr(int code, Datum arg);
static void generate_keys(uint8 *buf);
static void KmgrSaveCryptoKeys(const char *dir, CryptoKeyOnDisk *keys);
static void recoverIncompleteRotation(void);

/*
 * This function must be called ONCE on system install.
 */
void
BootStrapKmgr(void)
{
	KeyWrapCtx *ctx;
	uint8		keys_raw[KMGR_MAX_INTERNAL_KEYS][KMGR_KEY_AND_HMACKEY_LEN];
	CryptoKeyOnDisk keys_disk[KMGR_MAX_INTERNAL_KEYS];
	char		passphrase[KMGR_MAX_PASSPHRASE_LEN];
	uint8		kek[KMGR_KEY_LEN];
	uint8		kekhmac[KMGR_HMACKEY_LEN];
	int			passlen;
	int			i;

	/*
	 * Requirement check. We need openssl library to enable key management
	 * because all encryption and decryption calls happen via openssl function
	 * calls.
	 */
#ifndef USE_OPENSSL
	ereport(ERROR,
			(errcode(ERRCODE_CONFIG_FILE_ERROR),
			 (errmsg("cluster encryption is not supported because OpenSSL is not supported by this build"),
			  errhint("Compile with --with-openssl to use cluster encryption."))));
#endif

	/* Get key encryption key from the passphrase command */
	passlen = kmgr_run_cluster_passphrase_command(cluster_passphrase_command,
												  passphrase, KMGR_MAX_PASSPHRASE_LEN);
	if (passlen < KMGR_MIN_PASSPHRASE_LEN)
		ereport(ERROR,
				(errmsg("passphrase must be more than %d bytes",
						KMGR_MIN_PASSPHRASE_LEN)));

	/* Get key encryption key and HMAC key from passphrase */
	kmgr_derive_keys(passphrase, passlen, kek, kekhmac);
	ctx = create_keywrap_ctx(kek, kekhmac, true);
	if (!ctx)
		elog(ERROR, "could not initialize cipher contect");

	/* Generate all internal keys */
	for (i = 0; i < KMGR_MAX_INTERNAL_KEYS; i++)
		generate_keys(keys_raw[i]);

	/* Wrap all internal keys with key encryption key */
	for (i = 0; i < KMGR_MAX_INTERNAL_KEYS; i++)
	{
		int			wrapped_keylen;

		if (!kmgr_wrap_key(ctx, keys_raw[i], KMGR_KEY_AND_HMACKEY_LEN,
						   keys_disk[i].key, &wrapped_keylen))
		{
			free_keywrap_ctx(ctx);
			elog(ERROR, "failed to wrap cluster encryption key");
		}
		Assert(wrapped_keylen == KMGR_WRAPPED_KEY_LEN);

		keys_disk[i].id = i;
	}

	/* Save the internal keys to the disk */
	KmgrSaveCryptoKeys(KMGR_DIR, keys_disk);

	free_keywrap_ctx(ctx);
}

/*
 * Get encryption key passphrase and verify it, then get the internal keys.
 * This function is called by postmaster at startup time.
 */
void
InitializeKmgr(void)
{
	MemoryContext oldctx;
	CryptoKeyOnDisk *keys_disk;
	char		passphrase[KMGR_MAX_PASSPHRASE_LEN];
	uint8	   *sql_key;
	uint8	   *sql_hmackey;
	int			passlen;
	int			i;
	int			nkeys;

	if (!key_management_enabled)
		return;

	elog(DEBUG1, "starting up key management system");

	/* Recover the failure of the last passphrase rotation if necessary */
	recoverIncompleteRotation();

	/* Get the crypto keys from the file */
	keys_disk = kmgr_get_cryptokeys(KMGR_DIR, &nkeys);

	/* Get cluster passphrase */
	passlen = kmgr_run_cluster_passphrase_command(cluster_passphrase_command,
												  passphrase, KMGR_MAX_PASSPHRASE_LEN);

	KmgrCtx = AllocSetContextCreate(TopMemoryContext,
									"Key manager context",
									ALLOCSET_DEFAULT_SIZES);
	oldctx = MemoryContextSwitchTo(KmgrCtx);

	/*
	 * Verify the correctness of given passphrase using user key and internal
	 * key.
	 */
	for (i = 0; i < KMGR_MAX_INTERNAL_KEYS; i++)
	{
		uint8		keybuf[KMGR_WRAPPED_KEY_LEN];
		uint32		keyid = keys_disk[i].id;

		internalKeys[keyid] = (uint8 *) palloc(KMGR_KEY_AND_HMACKEY_LEN);

		if (!kmgr_verify_passphrase(passphrase, passlen, keys_disk[i].key,
									keybuf))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("cluster passphrase does not match expected passphrase")));
		memcpy(internalKeys[keyid], keybuf, KMGR_KEY_AND_HMACKEY_LEN);
	}

	/*
	 * The passphrase is correct, we wet wrap and unwrap context with the user
	 * key. These context is used for pg_wrap and pg_unwrap.
	 */
	sql_key = internalKeys[KMGR_SQL_KEY_ID];
	sql_hmackey = (uint8 *) ((char *) sql_key + KMGR_KEY_LEN);
	WrapCtx = create_keywrap_ctx(sql_key, sql_hmackey, true);
	UnwrapCtx = create_keywrap_ctx(sql_key, sql_hmackey, false);

	MemoryContextSwitchTo(oldctx);
	on_shmem_exit(ShutdownKmgr, 0);
}

/*
 * This must be called once during postmaster shutdown.
 */
static void
ShutdownKmgr(int code, Datum arg)
{
	if (WrapCtx)
		free_keywrap_ctx(WrapCtx);
	if (UnwrapCtx)
		free_keywrap_ctx(UnwrapCtx);
}

/*
 * Generate pair of the encryption and HMAC key. The buf must have
 * sufficient space to store two keys.
 */
static void
generate_keys(uint8 *buf)
{
	Assert(buf != NULL);

	if (!pg_strong_random(buf, KMGR_KEY_LEN))
		elog(ERROR, "failed to generate cluster encryption key");
	if (!pg_strong_random(buf + KMGR_KEY_LEN, KMGR_HMACKEY_LEN))
		elog(ERROR, "failed to generate cluster hmac key");
}

/*
 * Save the given crypto keys to the disk. We don't need CRC check for crypto
 * keys because these keys has HMAC which is used for integrity check
 * during unwrapping.
 */
static void
KmgrSaveCryptoKeys(const char *dir, CryptoKeyOnDisk *keys)
{
	int			i;

	elog(DEBUG2, "saving all cryptographic keys");

	for (i = 0; i < KMGR_MAX_INTERNAL_KEYS; i++)
	{
		int			fd;
		char		path[MAXPGPATH];

		CryptoKeyFilePath(path, dir, keys[i].id);

		if ((fd = BasicOpenFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY)) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m",
							path)));

		errno = 0;
		pgstat_report_wait_start(WAIT_EVENT_KEY_FILE_WRITE);
		if (write(fd, &(keys[i]), sizeof(CryptoKeyOnDisk)) != sizeof(CryptoKeyOnDisk))
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
				errno = ENOSPC;

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m",
							path)));
		}
		pgstat_report_wait_end();

		pgstat_report_wait_start(WAIT_EVENT_KEY_FILE_SYNC);
		if (pg_fsync(fd) != 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m",
							path)));
		pgstat_report_wait_end();

		if (close(fd) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not close file \"%s\": %m",
							path)));
	}
}

/*
 * SQL function to wrap the given data by the user key
 */
Datum
pg_wrap(PG_FUNCTION_ARGS)
{
	text	   *data = PG_GETARG_TEXT_PP(0);
	bytea	   *res;
	int			datalen;
	int			reslen;
	int			len;

	if (!key_management_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("could not wrap key because key management is not supported")));

	datalen = VARSIZE_ANY_EXHDR(data);
	reslen = VARHDRSZ + SizeOfWrappedKey(datalen);
	res = palloc(reslen);

	if (!kmgr_wrap_key(WrapCtx, (uint8 *) VARDATA_ANY(data), datalen,
					   (uint8 *) VARDATA(res), &len))
		elog(ERROR, "could not wrap the given secret");

	SET_VARSIZE(res, reslen);

	PG_RETURN_TEXT_P(res);
}

/*
 * SQL function to unwrap the given data by the user key
 */
Datum
pg_unwrap(PG_FUNCTION_ARGS)
{
	bytea	   *data = PG_GETARG_BYTEA_PP(0);
	text	   *res;
	int			datalen;
	int			buflen;
	int			len;

	if (!key_management_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("could not wrap key because key management is not supported")));

	datalen = VARSIZE_ANY_EXHDR(data);

	/* Check if the input length is more than minimum length of wrapped key */
	if (datalen < SizeOfWrappedKey(0))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid wrapped key input")));

	buflen = VARHDRSZ + SizeOfUnwrappedKey(datalen);
	res = palloc(buflen);

	if (!kmgr_unwrap_key(UnwrapCtx, (uint8 *) VARDATA_ANY(data), datalen,
						 (uint8 *) VARDATA(res), &len))
		elog(ERROR, "could not unwrap the given secret");

	/*
	 * The size of unwrapped key can be smaller than the size estimated before
	 * unwrapping since the padding is removed during unwrapping.
	 */
	SET_VARSIZE(res, VARHDRSZ + len);

	PG_RETURN_TEXT_P(res);
}

/*
 * SQL function to rotate the cluster passphrase. This function assumes that
 * the cluster_passphrase_command is already reloaded to the new value.
 * All internal keys are wrapped by the new passphrase and saved to the disk.
 * To update all crypto keys atomically we save the newly wrapped keys to the
 * temporary directory, pg_cryptokeys_tmp, and remove the original directory,
 * pg_cryptokeys, and rename it. These operation is performed without the help
 * of WAL.  In the case of failure during rotationpg_cryptokeys directory and
 * pg_cryptokeys_tmp directory can be left in incomplete status.  We recover
 * the incomplete situation by calling to checkIncompleteRotation.
 */
Datum
pg_rotate_cluster_passphrase(PG_FUNCTION_ARGS)
{
	KeyWrapCtx *ctx;
	CryptoKeyOnDisk *newkeys;
	char		passphrase[KMGR_MAX_PASSPHRASE_LEN];
	uint8		new_kek[KMGR_KEY_LEN];
	uint8		new_hmackey[KMGR_HMACKEY_LEN];
	int			passlen;
	int			outlen;
	int			i;

	if (!key_management_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("could not rotate cluster passphrase because key management is not supported")));

	/* Recover the failure of the last passphrase rotation if necessary */
	recoverIncompleteRotation();

	passlen = kmgr_run_cluster_passphrase_command(cluster_passphrase_command,
												  passphrase,
												  KMGR_MAX_PASSPHRASE_LEN);
	if (passlen < KMGR_MIN_PASSPHRASE_LEN)
		ereport(ERROR,
				(errmsg("passphrase must be more than %d bytes",
						KMGR_MIN_PASSPHRASE_LEN)));

	/* Get new key encryption key and wrap context */
	kmgr_derive_keys(passphrase, passlen, new_kek, new_hmackey);
	ctx = create_keywrap_ctx(new_kek, new_hmackey, true);

	newkeys = palloc(sizeof(CryptoKeyOnDisk) * KMGR_MAX_INTERNAL_KEYS);

	for (i = 0; i < KMGR_MAX_INTERNAL_KEYS; i++)
	{
		if (!kmgr_wrap_key(ctx, internalKeys[i], KMGR_KEY_AND_HMACKEY_LEN,
						   newkeys[i].key, &outlen))
			elog(ERROR, "failed to wrap key");
		newkeys[i].id = i;
		Assert(outlen == KMGR_WRAPPED_KEY_LEN);
	}

	/* Create temporary directory */
	if (MakePGDirectory(KMGR_TMP_DIR) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create temporary directory \"%s\": %m",
						KMGR_TMP_DIR)));
	fsync_fname(KMGR_TMP_DIR, true);

	LWLockAcquire(KmgrFileLock, LW_EXCLUSIVE);

	/* Save the key wrapped by the new passphrase to the temporary directory */
	KmgrSaveCryptoKeys(KMGR_TMP_DIR, newkeys);

	/* Remove the original directory */
	if (!rmtree(KMGR_DIR, true))
		ereport(ERROR,
				(errmsg("could not remove directory \"%s\"",
						KMGR_DIR)));

	/* Rename to the original directory */
	if (rename(KMGR_TMP_DIR, KMGR_DIR) != 0)
		ereport(ERROR,
				(errmsg("could not rename directory \"%s\" to \"%s\": %m",
						KMGR_TMP_DIR, KMGR_DIR)));
	fsync_fname(KMGR_DIR, true);

	LWLockRelease(KmgrFileLock);

	PG_RETURN_BOOL(true);
}

/*
 * Check the last passphrase rotation was completed. If not, we decide which wrapped
 * keys will be used according to the status of temporary directory and its wrapped
 * keys.
 */
static void
recoverIncompleteRotation(void)
{
	struct stat st;
	struct stat st_tmp;
	CryptoKeyOnDisk *keys;
	int			nkeys_tmp;

	/* The cluster passphrase rotation was completed, nothing to do */
	if (stat(KMGR_TMP_DIR, &st_tmp) != 0)
		return;

	/*
	 * If there is only temporary directory, it means that the previous
	 * rotation failed after wrapping the all internal keys by the new
	 * passphrase.  Therefore we use the new cluster passphrase.
	 */
	if (stat(KMGR_DIR, &st) != 0)
	{
		ereport(DEBUG1,
				(errmsg("there is only temporary directory, use the newly wrapped keys",
						KMGR_DIR, KMGR_TMP_DIR)));

		if (rename(KMGR_TMP_DIR, KMGR_DIR) != 0)
			ereport(ERROR,
					errmsg("could not rename directory \"%s\" to \"%s\": %m",
						   KMGR_TMP_DIR, KMGR_DIR));
		ereport(LOG,
				errmsg("cryptographic keys wrapped by new passphrase command are chosen"),
				errdetail("last cluster passphrase rotation failed in the middle"));
		return;
	}

	/*
	 * In case where both the original directory and temporary directory
	 * exist, there are two possibilities: (a) the all internal keys are
	 * wrapped by the new passphrase but rotation failed before removing the
	 * original directory, or (b) the rotation failed during wrapping internal
	 * keys by the new passphrase.  In case of (a) we need to use the wrapped
	 * keys in the temporary directory as rotation is essentially completed,
	 * but in case of (b) we use the wrapped keys in the original directory.
	 *
	 * To check the possibility of (b) we validate the wrapped keys in the
	 * temporary directory by checking the number of wrapped keys.  Since the
	 * wrapped key length is smaller than one disk sector, which is 512 bytes
	 * on common hardware, saving wrapped key is atomic write. So we can
	 * ensure that the all wrapped keys are valid if the number of wrapped
	 * keys in the temporary directory is KMGR_MAX_INTERNAL_KEYS.
	 */
	keys = kmgr_get_cryptokeys(KMGR_TMP_DIR, &nkeys_tmp);

	if (nkeys_tmp == KMGR_MAX_INTERNAL_KEYS)
	{
		/*
		 * This is case (a), the all wrapped keys in temporary directory are
		 * valid. Remove the original directory and rename.
		 */
		ereport(DEBUG1,
				(errmsg("last passphrase rotation failed before renaming direcotry name, use the newly wrapped keys")));

		if (!rmtree(KMGR_DIR, true))
			ereport(ERROR,
					(errmsg("could not remove directory \"%s\"",
							KMGR_DIR)));
		if (rename(KMGR_TMP_DIR, KMGR_DIR) != 0)
			ereport(ERROR,
					errmsg("could not rename directory \"%s\" to \"%s\": %m",
						   KMGR_TMP_DIR, KMGR_DIR));

		ereport(LOG,
				errmsg("cryptographic keys wrapped by new passphrase command are chosen"),
				errdetail("last cluster passphrase rotation failed in the middle"));
	}
	else
	{
		/*
		 * This is case (b), the last passphrase rotation failed during
		 * wrapping keys. Remove the keys in the temporary directory and use
		 * keys in the original keys.
		 */
		ereport(DEBUG1,
				(errmsg("last passphrase rotation failed during wrapping keys, use the old wrapped keys")));

		if (!rmtree(KMGR_TMP_DIR, true))
			ereport(ERROR,
					(errmsg("could not remove directory \"%s\"",
							KMGR_DIR)));
		ereport(LOG,
				errmsg("cryptographic keys wrapped by old passphrase command are chosen"),
				errdetail("last cluster passphrase rotation failed in the middle"));
	}

	pfree(keys);
}
