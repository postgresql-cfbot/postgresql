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
#include "storage/encryption.h"
#include "storage/fd.h"
#include "storage/kmgr.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/inval.h"
#include "utils/syscache.h"

/* Kmgr file name */
#define KMGR_FILENAME "global/pg_kmgr"

/* Info for key derivation of TDEK and WDEK */
#define TDE_TDEK_INFO	"TDEK"
#define TDE_WDEK_INFO	"WDEK"

typedef unsigned char keydata_t;

/*
 * Struct for key manager meta data written in KMGR_FILENAME. Kmgr file stores
 * information that is used for KEK derivation, verification and wrapped MDEK.
 * The file is written once when bootstrapping and is read when postmaster
 * startup.
 */
typedef struct KmgrFileData
{
	/* salt used for KEK derivation */
	keydata_t	kek_salt[TDE_KEK_DEVIRATION_SALT_SIZE];

	/* HMAC for KEK */
	keydata_t	kek_hmac[TDE_KEK_HMAC_SIZE];

	/* MDEK in encrypted state, NULL-terminated */
	keydata_t	mdek[TDE_MDEK_WRAPPED_SIZE];

	/* CRC of all above ... MUST BE LAST! */
	pg_crc32c	crc;
} KmgrFileData;

/*
 * Shared memory struct for encryption key information. All keys are stored
 * in non-encrypted state and never be modified during running. Therefore
 * processes can use them without locking.
 */
typedef struct KmgrCtlData
{
	/* Master data encryption key */
	keydata_t	mdek[TDE_MDEK_SIZE];

	/* Table data encryption key */
	keydata_t	tdek[ENC_MAX_ENCRYPTION_KEY_SIZE];

	/* WAL data encryption key */
	keydata_t	wdek[ENC_MAX_ENCRYPTION_KEY_SIZE];
} KmgrCtlData;
static KmgrCtlData	*KmgrCtl = NULL;

/* GUC variable */
char *cluster_passphrase_command = NULL;

static int run_cluster_passphrase_command(const char *prompt,
										  char *buf, int size);
static KmgrFileData *read_kmgr_file(void);
static void write_kmgr_file(KmgrFileData *filedata);
static void get_kek_and_hmackey_from_passphrase(char *passphrase, char passlen,
												keydata_t salt[TDE_KEK_DEVIRATION_SALT_SIZE],
												keydata_t kek[TDE_KEK_SIZE],
												keydata_t hmackey[TDE_KEK_HMAC_KEY_SIZE]);
static bool verify_passphrase(KmgrFileData *kmgfile,
							  char passphrase[TDE_MAX_PASSPHRASE_LEN],
							  int passlen, keydata_t kek[TDE_KEK_SIZE]);
static void derive_encryption_key(char *id, keydata_t key[ENC_MAX_ENCRYPTION_KEY_SIZE]);

/*
 * This func must be called ONCE on system install. we derive KEK,
 * generate MDEK and salt, compute hmac, write kmgr file etc.
 */
void
BootStrapKmgr(int bootstrap_data_encryption_cipher)
{
	const char *prompt = "Enter database encryption pass phrase:";
	KmgrFileData kmgrfile;
	char passphrase[TDE_MAX_PASSPHRASE_LEN];
	keydata_t kek_salt[TDE_KEK_DEVIRATION_SALT_SIZE];
	keydata_t kek_hmac[TDE_KEK_HMAC_SIZE];
	keydata_t mdek[TDE_MDEK_SIZE];
	keydata_t encmdek[TDE_MDEK_WRAPPED_SIZE];
	keydata_t kek[TDE_KEK_SIZE];
	keydata_t hmackey[TDE_KEK_HMAC_KEY_SIZE];
	int encmdek_size;
	int len;
	int ret;

#ifndef USE_OPENSSL
	ereport(ERROR,
			(errcode(ERRCODE_CONFIG_FILE_ERROR),
			 (errmsg("cluster encryption is not supported because OpenSSL is not supported by this build"),
			  errhint("Compile with --with-openssl to use cluster encryption."))));
#endif

	if (bootstrap_data_encryption_cipher == 0)
		return;

	/*
	 * Set data encryption cipher so that subsequent bootstrapping process
	 * can proceed.
	 */
	SetConfigOption("data_encryption_cipher",
					EncryptionCipherString(bootstrap_data_encryption_cipher),
					PGC_INTERNAL, PGC_S_OVERRIDE);

	 /* Get encryption key passphrase */
	len = run_cluster_passphrase_command(prompt,
										 passphrase,
										 TDE_MAX_PASSPHRASE_LEN);

	/* Generate salt for KEK derivation */
	ret = pg_strong_random(kek_salt, TDE_KEK_DEVIRATION_SALT_SIZE);
	if (!ret)
		ereport(ERROR,
				(errmsg("failed to generate random salt for key encryption key")));

	/* Get KEK and HMAC key */
	get_kek_and_hmackey_from_passphrase(passphrase, len, kek_salt, kek, hmackey);

	/* Generate salt for KEK */
	ret = pg_strong_random(mdek, TDE_MDEK_SIZE);
	if (!ret)
		ereport(ERROR,
				(errmsg("failed to generate the master encryption key")));

	/* Encrypt MDEK with KEK */
	pg_wrap_key(kek, TDE_KEK_SIZE, mdek, TDE_MDEK_SIZE, encmdek, &encmdek_size);

	if (encmdek_size != TDE_MDEK_WRAPPED_SIZE)
		elog(ERROR, "wrapped MDEK key size is invalid, got %d expected %d",
			 encmdek_size, TDE_MDEK_WRAPPED_SIZE);

	/* Compute HMAC */
	pg_compute_hmac(hmackey, TDE_KEK_HMAC_SIZE, encmdek, TDE_MDEK_WRAPPED_SIZE,
					kek_hmac);

	/* Fill out the kmgr file contents */
	memcpy(kmgrfile.kek_salt, kek_salt, TDE_KEK_DEVIRATION_SALT_SIZE);
	memcpy(kmgrfile.kek_hmac, kek_hmac, TDE_KEK_HMAC_SIZE);
	memcpy(kmgrfile.mdek, encmdek, TDE_MDEK_WRAPPED_SIZE);

	/* write kmgr file to the disk */
	write_kmgr_file(&kmgrfile);

	/* Set keys */
	memcpy(KmgrCtl->mdek, mdek, TDE_MDEK_SIZE);
	derive_encryption_key(TDE_TDEK_INFO, KmgrCtl->tdek);
	derive_encryption_key(TDE_WDEK_INFO, KmgrCtl->wdek);
}

Size
KmgrShmemSize(void)
{
	return sizeof(KmgrCtlData);
}

void
KmgrShmemInit(void)
{
	bool found;

	KmgrCtl = (KmgrCtlData *) ShmemInitStruct("KeyManagementData",
											  KmgrShmemSize(), &found);

	if (!found)
		MemSet(KmgrCtl, 0, KmgrShmemSize());
}

/*
 * Run cluster_passphrase_command
 *
 * prompt will be substituted for %p.
 *
 * The result will be put in buffer buf, which is of size size.	 The return
 * value is the length of the actual result.
 */
static int
run_cluster_passphrase_command(const char *prompt, char *buf, int size)
{
	StringInfoData command;
	char	   *p;
	FILE	   *fh;
	int			pclose_rc;
	size_t		len = 0;

	Assert(prompt);
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
					appendStringInfoString(&command, prompt);
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
 * MDEK. This function is called by postmaster at startup time.
 */
void
InitializeKmgr(void)
{
	const char *prompt = "Enter database encryption pass phrase:";
	char passphrase[TDE_MAX_PASSPHRASE_LEN];
	keydata_t kek[TDE_KEK_SIZE];
	keydata_t mdek[TDE_MDEK_SIZE];
	KmgrFileData *kmgrfile;
	int		len;
	int	mdek_size;

	if (!DataEncryptionEnabled())
		return;

	/* Get contents of kmgr file */
	kmgrfile = read_kmgr_file();

	/* Get encryption key passphrase */
	len = run_cluster_passphrase_command(prompt,
										 passphrase,
										 TDE_MAX_PASSPHRASE_LEN);

	/* Verify the given passphrase */
	if (!verify_passphrase(kmgrfile, passphrase, len, kek))
		ereport(ERROR,
				(errmsg("cluster passphrase does not match expected passphrase")));

	/* Unwrap MDEK with KEK */
	pg_unwrap_key(kek, TDE_KEK_SIZE, kmgrfile->mdek, TDE_MDEK_WRAPPED_SIZE,
				  mdek, &mdek_size);

	if (mdek_size != TDE_MDEK_SIZE)
		elog(ERROR, "unwrapped MDEK key size is invalid, got %d expected %d",
			 mdek_size, TDE_MDEK_SIZE);

	/* Set MDEK, TDEK and WDEK to the shared memory struct */
	memcpy(KmgrCtl->mdek, mdek, TDE_MDEK_SIZE);
	derive_encryption_key(TDE_TDEK_INFO, KmgrCtl->tdek);
	derive_encryption_key(TDE_WDEK_INFO, KmgrCtl->wdek);
}

/*
 * Read kmgr file, and return palloc'd file data.
 */
static KmgrFileData *
read_kmgr_file(void)
{
	KmgrFileData *kmgrfile;
	pg_crc32c	crc;
	int read_len;
	int fd;

	fd = BasicOpenFile(KMGR_FILENAME, O_RDONLY | PG_BINARY);

	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", KMGR_FILENAME)));

	/* Read data */
	kmgrfile = (KmgrFileData *) palloc(sizeof(KmgrFileData));

	pgstat_report_wait_start(WAIT_EVENT_KMGR_FILE_READ);
	if ((read_len = read(fd, kmgrfile, sizeof(KmgrFileData))) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 (errmsg("could not read from file \"%s\": %m", KMGR_FILENAME))));
	pgstat_report_wait_end();

	if (close(fd))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", KMGR_FILENAME)));

	/* Verify CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *) kmgrfile, offsetof(KmgrFileData, crc));
	FIN_CRC32C(crc);

	if (!EQ_CRC32C(crc, kmgrfile->crc))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("calculated CRC checksum does not match value stored in file \"%s\"",
						KMGR_FILENAME)));

	return kmgrfile;
}

/*
 * Write kmgr file. This function is used only when bootstrapping.
 */
static void
write_kmgr_file(KmgrFileData *filedata)
{
	int				fd;

	INIT_CRC32C(filedata->crc);
	COMP_CRC32C(filedata->crc, filedata, offsetof(KmgrFileData, crc));
	FIN_CRC32C(filedata->crc);

	fd = BasicOpenFile(KMGR_FILENAME, PG_BINARY | O_CREAT | O_RDWR);

	if (fd < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						KMGR_FILENAME)));
		return;
	}

	pgstat_report_wait_start(WAIT_EVENT_KMGR_FILE_WRITE);
	if (write(fd, filedata, sizeof(KmgrFileData)) != sizeof(KmgrFileData))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write kmgr file \"%s\": %m",
						KMGR_FILENAME)));
	pgstat_report_wait_end();

	pgstat_report_wait_start(WAIT_EVENT_KMGR_FILE_SYNC);
	if (pg_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 (errmsg("could not sync file \"%s\": %m",
						 KMGR_FILENAME))));
	pgstat_report_wait_end();

	if (close(fd))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", KMGR_FILENAME)));

}

/*
 * Derive key from passphrase and extract KEK and HMAC key from the derived key.
 */
static void
get_kek_and_hmackey_from_passphrase(char *passphrase, char passlen,
									keydata_t salt[TDE_KEK_DEVIRATION_SALT_SIZE],
									keydata_t kek[TDE_KEK_SIZE],
									keydata_t hmackey[TDE_KEK_HMAC_KEY_SIZE])
{
	keydata_t enckey_and_hmackey[TDE_KEK_DERIVED_KEY_SIZE];

	/* Derive key from passphrase, or error */
	pg_derive_key_passphrase(passphrase, passlen,
							 salt, TDE_KEK_DEVIRATION_SALT_SIZE,
							 TDE_KEK_DEVIRATION_ITER_COUNT,
							 TDE_KEK_SIZE + TDE_KEK_HMAC_KEY_SIZE,
							 enckey_and_hmackey);

	/* Extract KEK and HMAC key from the derived key */
	memcpy(kek, enckey_and_hmackey, TDE_KEK_SIZE);
	memcpy(hmackey, enckey_and_hmackey + TDE_KEK_SIZE, TDE_KEK_HMAC_KEY_SIZE);
}

/*
 * Verify correctness of the given passphrase. We compute HMAC of encrypted
 * MDEK written in the kmgr file, and compare it to the HMAC written in the kmgr
 * file. Return true and set KEK if the passphrase is verified, otherwise return
 * false.
 */
static bool
verify_passphrase(KmgrFileData *kmgrfile, char passphrase[TDE_MAX_PASSPHRASE_LEN],
				  int passlen, keydata_t kek[TDE_KEK_SIZE])
{
	keydata_t hmac[TDE_KEK_HMAC_SIZE];
	keydata_t kek_tmp[TDE_KEK_SIZE];
	keydata_t hmackey_tmp[TDE_KEK_HMAC_KEY_SIZE];

	get_kek_and_hmackey_from_passphrase(passphrase, passlen,
										kmgrfile->kek_salt, kek_tmp,
										hmackey_tmp);

	/*
	 * Compute HMAC of encrypted MDEK in kmgr file with the HMAC key derived
	 * from passphrase.
	 */
	pg_compute_hmac(hmackey_tmp, TDE_KEK_HMAC_KEY_SIZE, kmgrfile->mdek,
					TDE_MDEK_WRAPPED_SIZE, hmac);

	/* Compare two HMACs */
	if (memcmp(kmgrfile->kek_hmac, hmac, TDE_KEK_HMAC_SIZE) != 0)
		return false;

	/* user-provided passphrase is correct, set KEK */
	memcpy(kek, kek_tmp, TDE_KEK_SIZE);

	return true;
}

/* Return table data encryption key (TDEK) */
const char *
GetTableEncryptionKey(void)
{
	Assert(KmgrCtl != NULL);

	return (char *) KmgrCtl->tdek;
}

/* Return WAL data encryption key (WDEK) */
const char *
GetWALEncryptionKey(void)
{
	Assert(KmgrCtl != NULL);

	return (const char *) KmgrCtl->wdek;
}

/*
 * Derive new encryption key with info from MDEK.
 */
static void
derive_encryption_key(char *info, keydata_t key[ENC_MAX_ENCRYPTION_KEY_SIZE])
{
	char key_len = EncryptionKeySize;

	/* derive new TDEK that length is EncryptionKeySize from MDEK */
	pg_derive_key(KmgrCtl->mdek, TDE_MDEK_SIZE, (keydata_t *) info,
				  key, key_len);
}
