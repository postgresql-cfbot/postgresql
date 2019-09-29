/*-------------------------------------------------------------------------
 *
 * bufenc.c
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/encryption/bufenc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "storage/bufpage.h"
#include "storage/encryption.h"
#include "storage/fd.h"
#include "storage/kmgr.h"

static char buf_encryption_iv[ENC_IV_SIZE];

static void set_buffer_encryption_iv(Page page, BlockNumber blocknum);

void
EncryptBufferBlock(BlockNumber blocknum, Page page)
{
	/*
	fprintf(stderr, "ENC offset %d, encsize %d, blkno %u: ",
			PageEncryptOffset, SizeOfPageEncryption, blocknum);
	//dp("  key", (unsigned char *) GetTableEncryptionKey(), EncryptionKeySize);
	*/

	set_buffer_encryption_iv(page, blocknum);
	//dp("  iv", (unsigned char *) buf_encryption_iv, ENC_IV_SIZE);
	pg_encrypt(page + PageEncryptOffset,
			   page + PageEncryptOffset,
			   SizeOfPageEncryption,
			   GetTableEncryptionKey(),
			   buf_encryption_iv);
}
void
DecryptBufferBlock(BlockNumber blocknum, Page page)
{
/*
	fprintf(stderr, "DEC offset %d, encsize %d, blkno %u: ",
			PageEncryptOffset, SizeOfPageEncryption, blocknum);
*/
	//dp("  key", GetTableEncryptionKey(), EncryptionKeySize);

	set_buffer_encryption_iv(page, blocknum);
	//dp("  iv", (unsigned char *) buf_encryption_iv, ENC_IV_SIZE);
	pg_decrypt(page + PageEncryptOffset,
			   page + PageEncryptOffset,
			   SizeOfPageEncryption,
			   GetTableEncryptionKey(),
			   buf_encryption_iv);
}

static void
set_buffer_encryption_iv(Page page, BlockNumber blocknum)
{
	char *p = buf_encryption_iv;

	MemSet(buf_encryption_iv, 0, ENC_IV_SIZE);

	//PageXLogRecPtr lsn = ((PageHeader) page)->pd_lsn;
	//fprintf(stderr, "  -> setting iv lsn %x/%x, blkno %u\n", lsn.xlogid, lsn.xrecoff, blocknum);

	/* page lsn (8 byte) */
	memcpy(p, &((PageHeader) page)->pd_lsn, sizeof(PageXLogRecPtr));
	p += sizeof(PageXLogRecPtr);

	/* block number (4 byte) */
	memcpy(p, &blocknum, sizeof(BlockNumber));
	p += sizeof(BlockNumber);

	/* Space for counter (4 byte) */
	memset(p, 0, ENC_BUFFER_AES_COUNTER_SIZE);
	p += ENC_BUFFER_AES_COUNTER_SIZE;

}

