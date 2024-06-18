#ifndef SIMPLE_UNDOLOG_H
#define SIMPLE_UNDOLOG_H

#include "access/rmgr.h"
#include "port/pg_crc32c.h"

#define SIMPLE_UNDOLOG_DIR	"pg_ulog"

typedef struct SimpleUndoLogRecord
{
	uint32		ul_tot_len;		/* total length of entire record */
	pg_crc32c	ul_crc;			/* CRC for this record */
	RmgrId		ul_rmid;		/* resource manager for this record */
	uint8		ul_info;		/* record info */
	TransactionId ul_xid;		/* transaction id */
	/* rmgr-specific data follow, no padding */
} SimpleUndoLogRecord;

extern void SimpleUndoLogWrite(RmgrId rmgr, uint8 info,
							   TransactionId xid, void *data, int len);
extern void SimpleUndoLogSetPrpared(TransactionId xid, bool prepared);
extern void AtEOXact_SimpleUndoLog(bool isCommit, TransactionId xid);
extern void UndoLogCleanup(void);

extern void AtPrepare_UndoLog(TransactionId xid);
extern void PostPrepare_UndoLog(void);
extern void undolog_twophase_recover(TransactionId xid, uint16 info,
									 void *recdata, uint32 len);
extern void undolog_twophase_postcommit(TransactionId xid, uint16 info,
										void *recdata, uint32 len);
extern void undolog_twophase_postabort(TransactionId xid, uint16 info,
									   void *recdata, uint32 len);
extern void undolog_twophase_standby_recover(TransactionId xid, uint16 info,
											 void *recdata, uint32 len);

#endif							/* SIMPLE_UNDOLOG_H */
