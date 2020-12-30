/*
 * clog.h
 *
 * PostgreSQL transaction-commit-log manager
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/clog.h
 */
#ifndef CLOG_H
#define CLOG_H

#include "access/clogdefs.h"
#include "access/xlogreader.h"
#include "storage/sync.h"
#include "lib/stringinfo.h"

typedef struct xl_clog_truncate
{
	int			pageno;
	TransactionId oldestXact;
	Oid			oldestXactDb;
} xl_clog_truncate;

extern void TransactionIdSetTreeStatus(TransactionId xid, int nsubxids,
									   TransactionId *subxids, XidStatus status, XLogRecPtr lsn);
extern XidStatus TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn,
										bool throwError);

extern Size CLOGShmemBuffers(void);
extern Size CLOGShmemSize(void);
extern void CLOGShmemInit(void);
extern void BootStrapCLOG(void);
extern void StartupCLOG(void);
extern void TrimCLOG(void);
extern void ShutdownCLOG(void);
extern void CheckPointCLOG(void);
extern void ExtendCLOG(TransactionId newestXact);
extern void TruncateCLOG(TransactionId oldestXact, Oid oldestxid_datoid);

extern int	clogsyncfiletag(const FileTag *ftag, char *path);

/* XLOG stuff */
#define CLOG_ZEROPAGE		0x00
#define CLOG_TRUNCATE		0x10

extern void clog_redo(XLogReaderState *record);
extern void clog_desc(StringInfo buf, XLogReaderState *record);
extern const char *clog_identify(uint8 info);

#endif							/* CLOG_H */
