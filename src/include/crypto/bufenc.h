/*-------------------------------------------------------------------------
 *
 * bufenc.h
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * src/include/crypto/bufenc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BUFENC_H
#define BUFENC_H

#include "storage/bufmgr.h"
#include "crypto/kmgr.h"
#include "access/xlog_internal.h"

/* Cluster encryption encrypts only main forks */
#define PageNeedsToBeEncrypted(forknum) \
	(FileEncryptionEnabled && (forknum) == MAIN_FORKNUM)


#ifdef FRONTEND
#include "common/logging.h"
#define my_error(...) pg_fatal(__VA_ARGS__)
#define LSNForEncryption(a) InvalidXLogRecPtr
#else
extern XLogRecPtr LSNForEncryption(bool use_wal_lsn);
#define my_error(...) elog(ERROR, __VA_ARGS__)
#endif

extern void InitializeBufferEncryption(int file_encryption_method);
extern void EncryptPage(Page page, bool relation_is_permanent,
						BlockNumber blkno, RelFileNumber fileno);
extern void DecryptPage(Page page, bool relation_is_permanent,
						BlockNumber blkno, RelFileNumber fileno);
extern void EncryptXLogRecord(XLogRecord *record, XLogRecPtr address, char *dest);
extern bool DecryptXLogRecord(XLogRecord *record, XLogRecPtr address);
extern void CalculateXLogRecordAuthtag(XLogRecData *recdat, XLogRecPtr address, char *tag);
extern void StartEncryptXLogRecord(XLogRecord *record, XLogRecPtr address);
extern int EncryptXLogRecordIncremental(char *plaintext, char *encdest, int len);
extern void FinishEncryptXLogRecord(char *loc);
#endif							/* BUFENC_H */
