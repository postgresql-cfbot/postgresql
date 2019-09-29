/*-------------------------------------------------------------------------
 *
 * walenc.c
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/encryption/walenc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlog.h"
#include "storage/encryption.h"

#ifdef NOT_USED
static char wal_encryption_iv[ENC_IV_SIZE];

static void
set_wal_encryption_iv(char *iv, XLogSegNo segment, uint32 offset)
{
	char *p = iv;
	uint32 pageno = offset / XLOG_BLCKSZ;

	Assert(iv != NULL);

	/* Space for counter (4 byte) */
	memset(p, 0, ENC_WAL_AES_COUNTER_SIZE);
	p += ENC_WAL_AES_COUNTER_SIZE;

	/* Segment number (8 byte) */
	memcpy(p, &segment, sizeof(XLogSegNo));
	p += sizeof(XLogSegNo);

	/* Page number within a WAL segment (4 byte) */
	memcpy(p, &pageno, sizeof(uint32));
}
#endif

