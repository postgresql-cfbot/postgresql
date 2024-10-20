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
	/* rmgr-specific data follow, no padding */
} SimpleUndoLogRecord;

/* State of the undo log file */
typedef enum UndoLogFileState
{
	ULOG_FILE_DEFAULT,				/* normal state */
	ULOG_FILE_PREPARED,				/* ulog file is prepared */
	ULOG_FILE_CRASH_AFTER_PREPARED	/* experienced a crash after prepared */
} UndoLogFileState;

/*
 * The high 4 bits in ul_info may be used freely by rmgr. The lower 4 bits are
 * not used for now.
 */
#define ULR_INFO_MASK			0x0F
#define ULR_RMGR_INFO_MASK		0xF0

extern void SimpleUndoLogWrite(RmgrId rmgr, uint8 info, void *data, int len);
extern void SimpleUndoLogWriteRedo(RmgrId rmgr, uint8 info, void *data, int len,
								   TransactionId xid);
extern void SimpleUndoLogSetPreparedRedo(void);
extern void AtEOXact_SimpleUndoLog(bool isCommit);
extern void AtEOSubXact_SimpleUndoLog(bool isCommit);
extern void AtPrepare_SimpleUndoLog(void);
extern void SimpleUndoLog_UndoByXid(bool isCommit, TransactionId xid,
									int nchildren, TransactionId *children);
extern void UndoLogCleanup(void);

#endif							/* SIMPLE_UNDOLOG_H */
