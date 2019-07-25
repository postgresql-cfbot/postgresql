/*-------------------------------------------------------------------------
 *
 * encryption.h
 *	  Full database encryption support
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/encryption.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ENCRYPTION_H
#define ENCRYPTION_H

#include "access/xlogdefs.h"
#include "miscadmin.h"
#include "storage/block.h"
#include "storage/relfilenode.h"
#include "port/pg_crc32c.h"

/*
 * Common error message issued when particular code path cannot be executed
 * due to absence of the OpenSSL library.
 */
#define ENCRYPTION_NOT_SUPPORTED_MSG \
	"compile postgres with --with-openssl to use encryption."

/*
 * Full database encryption key.
 *
 * The key of EVP_aes_256_cbc() cipher is 256 bits long.
 */
#define	ENCRYPTION_KEY_LENGTH	32
/* Key length in characters (two characters per hexadecimal digit) */
#define ENCRYPTION_KEY_CHARS	(ENCRYPTION_KEY_LENGTH * 2)

/*
 * Cipher used to encrypt data.
 *
 * Due to very specific requirements, the ciphers are not likely to change,
 * but we should be somewhat flexible.
 *
 * XXX If we have more than one cipher someday, have pg_controldata report the
 * cipher kind (in textual form) instead of merely saying "on".
 */
typedef enum CipherKind
{
	/* The cluster is not encrypted. */
	PG_CIPHER_NONE = 0,

	/*
	 * AES (Rijndael) in CBC mode of operation as block cipher, and in CTR
	 * mode as stream cipher. Key length is always 256 bits.
	 */
	PG_CIPHER_AES_BLOCK_CBC_256_STREAM_CTR_256
}			CipherKind;

/*
 * TODO Tune these values.
 */
#define ENCRYPTION_PWD_MIN_LENGTH	8
#define ENCRYPTION_PWD_MAX_LENGTH	16
#define ENCRYPTION_KDF_NITER		1048576
#define	ENCRYPTION_KDF_SALT_LEN		sizeof(uint64)

/* Key to encrypt / decrypt data. */
extern unsigned char encryption_key[];

#ifndef FRONTEND
/*
 * Space for the encryption key in shared memory. Backend that receives the
 * key during startup stores it here so postmaster can eventually take a local
 * copy.
 *
 * We cannot protect the "initialized" field with a spinlock because the data
 * will be accessed by postmaster, but it should not be necessary for bool
 * type. Furthermore, synchronization is not really critical here, see
 * processEncryptionKey().
 */
typedef struct ShmemEncryptionKey
{
	char	data[ENCRYPTION_KEY_LENGTH]; /* the key */
	bool	initialized;				/* received the key? */
} ShmemEncryptionKey;

/*
 * Encryption key in the shared memory.
 */
extern ShmemEncryptionKey *encryption_key_shmem;
#endif							/* FRONTEND */

/*
 * The encrypted data is a series of blocks of size
 * ENCRYPTION_BLOCK. Currently we use the EVP_aes_256_xts implementation. Make
 * sure the following constants match if adopting another algorithm.
 */
#define ENCRYPTION_BLOCK 16

#define TWEAK_SIZE 16

/* Is the cluster encrypted? */
extern PGDLLIMPORT bool data_encrypted;

/*
 * Number of bytes reserved to store encryption sample in ControlFileData.
 */
#define ENCRYPTION_SAMPLE_SIZE 16

#ifndef FRONTEND
/* Copy of the same field of ControlFileData. */
extern char encryption_verification[];
#endif							/* FRONTEND */

/* Do we have encryption_key and the encryption library initialized? */
extern bool	encryption_setup_done;

/*
 * In some cases we need a separate copy of the data because encryption
 * in-place (typically in the shared buffers) would make the data unusable for
 * backends.
 */
extern PGAlignedBlock encrypt_buf;

/*
 * The same for XLOG. This buffer spans multiple pages, in order to reduce the
 * number of syscalls when doing I/O.
 *
 * XXX Fine tune the buffer size.
 */
extern char *encrypt_buf_xlog;
#define	XLOG_ENCRYPT_BUF_PAGES	8
#define ENCRYPT_BUF_XLOG_SIZE	(XLOG_ENCRYPT_BUF_PAGES * XLOG_BLCKSZ)

#ifndef FRONTEND
extern Size EncryptionShmemSize(void);
extern void EncryptionShmemInit(void);

typedef int (*read_encryption_key_cb) (void);
extern void read_encryption_key(read_encryption_key_cb read_char);
#endif							/* FRONTEND */

extern void setup_encryption(void);
extern void sample_encryption(char *buf);
extern void encrypt_block(const char *input, char *output, Size size,
			  char *tweak, bool stream);
extern void decrypt_block(const char *input, char *output, Size size,
			  char *tweak, bool stream);
extern void encryption_error(bool fatal, char *message);

extern void XLogEncryptionTweak(char *tweak, TimeLineID timeline,
					XLogSegNo segment, uint32 offset);
extern BlockNumber ReencryptBlock(char *buffer, int blocks,
			   RelFileNode *srcNode, RelFileNode *dstNode,
			   ForkNumber srcForkNum, ForkNumber dstForkNum,
			   BlockNumber blockNum);
extern void mdtweak(char *tweak, RelFileNode *relnode, ForkNumber forknum,
		BlockNumber blocknum);

#ifndef FRONTEND
extern bool EnforceLSNUpdateForEncryption(char	*buf_contents);
extern void RestoreInvalidLSN(char	*buf_contents);
#endif	/* FRONTEND */

#endif							/* ENCRYPTION_H */
