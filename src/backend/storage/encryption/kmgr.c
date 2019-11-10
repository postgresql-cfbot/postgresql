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
#include "storage/encryption.h"
#include "storage/fd.h"
#include "storage/kmgr.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#define KMGR_PROMPT_MSG "Enter database encryption pass phrase:"

/*
 * Key encryption key. This variable is set during verification
 * of user given passphrase. After verified, the plain key data
 * is set to this variable.
 */
static keydata_t keyEncKey[TDE_KEK_SIZE];

/*
 * Relation encryption key and WAL encryption key.  Similar to
 * key encryption key, these variables store the plain key data.
 */
static keydata_t relEncKey[TDE_MAX_DEK_SIZE];
static keydata_t walEncKey[TDE_MAX_DEK_SIZE];

/* GUC variable */
char *cluster_passphrase_command = NULL;

static int run_cluster_passphrase_command(char *buf, int size);
static void get_kek_and_hmackey_from_passphrase(char *passphrase,
												Size passlen,
												keydata_t kek[TDE_KEK_SIZE],
												keydata_t hmackey[TDE_HMAC_KEY_SIZE]);
static bool verify_passphrase(char *passphrase, int passlen,
							  WrappedEncKeyWithHmac *rdek,
							  WrappedEncKeyWithHmac *wdek);

/*
 * This func must be called ONCE on system install. we retrive KEK,
 * generate RDEK and WDEK etc.
 */
KmgrBootstrapInfo *
BootStrapKmgr(int bootstrap_data_encryption_cipher)
{
	KmgrBootstrapInfo *kmgrinfo;
	char passphrase[TDE_MAX_PASSPHRASE_LEN];
	keydata_t hmackey[TDE_HMAC_KEY_SIZE];
	keydata_t *rdek_enc;
	keydata_t *wdek_enc;
	keydata_t *rdek_hmac;
	keydata_t *wdek_hmac;
	int	wrapped_keysize;
	int	len;
	int size;

	if (bootstrap_data_encryption_cipher == TDE_ENCRYPTION_OFF)
		return NULL;

#ifndef USE_OPENSSL
	ereport(ERROR,
			(errcode(ERRCODE_CONFIG_FILE_ERROR),
			 (errmsg("cluster encryption is not supported because OpenSSL is not supported by this build"),
			  errhint("Compile with --with-openssl to use cluster encryption."))));
#endif

	kmgrinfo = palloc0(sizeof(KmgrBootstrapInfo));
	rdek_enc = kmgrinfo->relEncKey.key;
	rdek_hmac = kmgrinfo->relEncKey.hmac;
	wdek_enc = kmgrinfo->walEncKey.key;
	wdek_hmac = kmgrinfo->walEncKey.hmac;

	/*
	 * Set data encryption cipher so that subsequent bootstrapping process
	 * can proceed.
	 */
	SetConfigOption("data_encryption_cipher",
					EncryptionCipherString(bootstrap_data_encryption_cipher),
					PGC_INTERNAL, PGC_S_OVERRIDE);

	/* Get key encryption key fro command */
	len = run_cluster_passphrase_command(passphrase, TDE_MAX_PASSPHRASE_LEN);

	/* Get key encryption key and HMAC key from passphrase */
	get_kek_and_hmackey_from_passphrase(passphrase, len, keyEncKey,
										hmackey);

	/*
	 * Generate relation encryption key and WAL encryption key.
	 * The generated two keys must be stored in relEncKey and
	 * walEncKey that can be used by other modules since even
	 * during bootstrapping we need to encrypt both systemcatalogs
	 * and WAL.
	 */
	if (!pg_strong_random(relEncKey, EncryptionKeySize))
		ereport(ERROR,
				(errmsg("failed to generate relation encryption key")));
	if (!pg_strong_random(walEncKey, EncryptionKeySize))
		ereport(ERROR,
				(errmsg("failed to generate WAL encryption key")));

	/* Wrap both keys by KEK */
	wrapped_keysize = EncryptionKeySize + TDE_DEK_WRAP_VALUE_SIZE;
	pg_wrap_key(keyEncKey, TDE_KEK_SIZE,
				relEncKey, EncryptionKeySize,
				rdek_enc, &size);
	if (size != wrapped_keysize)
		elog(ERROR, "wrapped relation encryption key size is invalid, got %d expected %d",
			 size, wrapped_keysize);

	pg_wrap_key(keyEncKey, TDE_KEK_SIZE,
				walEncKey, EncryptionKeySize,
				wdek_enc, &size);
	if (size != wrapped_keysize)
		elog(ERROR, "wrapped WAL encryption key size is invalid, got %d expected %d",
			 size, wrapped_keysize);

	/* Compute both HMAC */
	pg_compute_hmac(hmackey, TDE_HMAC_KEY_SIZE,
					rdek_enc, wrapped_keysize,
					rdek_hmac);
	pg_compute_hmac(hmackey, TDE_HMAC_KEY_SIZE,
					wdek_enc, wrapped_keysize,
					wdek_hmac);

	/* return keys and HMACs generated during bootstrap */
	return kmgrinfo;
}

/*
 * Run cluster_passphrase_command
 *
 * prompt will be substituted for %p.
 *
 * The result will be put in buffer buf, which is of size size.
 * The return value is the length of the actual result.
 */
static int
run_cluster_passphrase_command(char *buf, int size)
{
	StringInfoData command;
	char	   *p;
	FILE	   *fh;
	int			pclose_rc;
	size_t		len = 0;

	Assert(size > 0);
	buf[0] = '\0';

	initStringInfo(&command);

	for (p = cluster_passphrase_command; *p; p++)
	{
		if (p[0] == '%')
		{
			switch (p[1])
			{
				case 'p':
					appendStringInfoString(&command, KMGR_PROMPT_MSG);
					p++;
					break;
				case '%':
					appendStringInfoChar(&command, '%');
					p++;
					break;
				default:
					appendStringInfoChar(&command, p[0]);
			}
		}
		else
			appendStringInfoChar(&command, p[0]);
	}

	fh = OpenPipeStream(command.data, "r");
	if (fh == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not execute command \"%s\": %m",
						command.data)));

	if (!fgets(buf, size, fh))
	{
		if (ferror(fh))
		{
			pfree(command.data);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from command \"%s\": %m",
							command.data)));
		}
	}

	pclose_rc = ClosePipeStream(fh);
	if (pclose_rc == -1)
	{
		pfree(command.data);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close pipe to external command: %m")));
	}
	else if (pclose_rc != 0)
	{
		pfree(command.data);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("command \"%s\" failed",
						command.data),
				 errdetail_internal("%s", wait_result_to_str(pclose_rc))));
	}

	/* strip trailing newline */
	len = strlen(buf);
	if (len > 0 && buf[len - 1] == '\n')
		buf[--len] = '\0';

	pfree(command.data);

	return len;
}

/*
 * Get encryption key passphrase and verify it, then get the un-encrypted
 * RDEK and WDEK. This function is called by postmaster at startup time.
 */
void
InitializeKmgr(void)
{
	WrappedEncKeyWithHmac *wrapped_rdek;
	WrappedEncKeyWithHmac *wrapped_wdek;
	char passphrase[TDE_MAX_PASSPHRASE_LEN];
	int		len;
	int		wrapped_keysize;
	int		unwrapped_size;

	if (!DataEncryptionEnabled())
		return;

	/* Get cluster passphrase */
	len = run_cluster_passphrase_command(passphrase, TDE_MAX_PASSPHRASE_LEN);

	/* Get two wrapped keys stored in control file */
	wrapped_rdek = GetTDERelationEncryptionKey();
	wrapped_wdek = GetTDEWALEncryptionKey();

	wrapped_keysize = EncryptionKeySize + TDE_DEK_WRAP_VALUE_SIZE;

	/* Verify the correctness of given passphrase */
	if (!verify_passphrase(passphrase, len, wrapped_rdek, wrapped_wdek))
		ereport(ERROR,
				(errmsg("cluster passphrase does not match expected passphrase")));

	/* The passphrase is correct, unwrap both RDEK and WDEK */
	pg_unwrap_key(keyEncKey, TDE_KEK_SIZE,
				  wrapped_rdek->key, wrapped_keysize,
				  relEncKey, &unwrapped_size);
	if (unwrapped_size != EncryptionKeySize)
		elog(ERROR, "unwrapped relation encryption key size is invalid, got %d expected %d",
			 unwrapped_size, EncryptionKeySize);

	pg_unwrap_key(keyEncKey, TDE_KEK_SIZE,
				  wrapped_wdek->key, wrapped_keysize,
				  walEncKey, &unwrapped_size);
	if (unwrapped_size != EncryptionKeySize)
		elog(ERROR, "unwrapped WAL encryptoin key size is invalid, got %d expected %d",
			 unwrapped_size, EncryptionKeySize);
}

 /*
  * Hash the given passphrase and extract it into KEK and HMAC
  * key.
  */
static void
get_kek_and_hmackey_from_passphrase(char *passphrase, Size passlen,
									keydata_t kek_out[TDE_KEK_SIZE],
									keydata_t hmackey_out[TDE_HMAC_KEY_SIZE])
{
	keydata_t enckey_and_hmackey[PG_SHA512_DIGEST_LENGTH];
	pg_sha512_ctx ctx;

	pg_sha512_init(&ctx);
	pg_sha512_update(&ctx, (const uint8 *) passphrase, passlen);
	pg_sha512_final(&ctx, enckey_and_hmackey);

	/*
	 * SHA-512 results 64 bytes. We extract it into two keys for
	 * each 32 bytes: one for key encryption and another one for
	 * HMAC.
	 */
	memcpy(kek_out, enckey_and_hmackey, TDE_KEK_SIZE);
	memcpy(hmackey_out, enckey_and_hmackey + TDE_KEK_SIZE, TDE_HMAC_KEY_SIZE);
}

/*
 * Verify the correctness of the given passphrase. We compute HMACs of the
 * wrapped keys (RDEK and WDEK) using the HMAC key retrived from the user
 * provided passphrase. And then we compare it with the HMAC stored alongside
 * the controlfile. Return true if both HMACs are matched, meaning the given
 * passphrase is correct. Otherwise return false.
 */
static bool
verify_passphrase(char *passphrase, int passlen,
				  WrappedEncKeyWithHmac *rdek, WrappedEncKeyWithHmac *wdek)
{
	keydata_t user_kek[TDE_KEK_SIZE];
	keydata_t user_hmackey[TDE_HMAC_KEY_SIZE];
	keydata_t result_hmac[TDE_HMAC_SIZE];
	int	wrapped_keysize = EncryptionKeySize + TDE_DEK_WRAP_VALUE_SIZE;

	get_kek_and_hmackey_from_passphrase(passphrase, passlen,
										user_kek, user_hmackey);

	/* Verify both HMACs of RDEK and WDEK */
	pg_compute_hmac(user_hmackey, TDE_HMAC_KEY_SIZE,
					rdek->key, wrapped_keysize,
					result_hmac);
	if (memcmp(result_hmac, rdek->hmac, TDE_HMAC_SIZE) != 0)
		return false;

	pg_compute_hmac(user_hmackey, TDE_HMAC_KEY_SIZE,
					wdek->key, wrapped_keysize,
					result_hmac);
	if (memcmp(result_hmac, wdek->hmac, TDE_HMAC_SIZE) != 0)
		return false;

	/* The passphrase is verified. Save the key encryption key */
	memcpy(keyEncKey, user_kek, TDE_KEK_SIZE);

	return true;
}

/* Return plain relation encryption key */
const char *
KmgrGetRelationEncryptionKey(void)
{
	Assert(DataEncryptionEnabled());
	return (const char *) relEncKey;
}

/* Return plain WAL encryption key */
const char *
KmgrGetWALEncryptionKey(void)
{
	Assert(DataEncryptionEnabled());
	return (const char *) walEncKey;
}
