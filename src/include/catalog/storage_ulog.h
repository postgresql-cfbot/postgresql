/*-------------------------------------------------------------------------
 *
 * storage_ulog.h
 *	  prototypes for Undo Log support for backend/catalog/storage.c
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/storage_ulog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STORAGE_ULOG_H
#define STORAGE_ULOG_H

#include "access/simpleundolog.h"
#include "storage/relfilelocator.h"

/* ULOG gives us high 4 bits (just following xlog) */
#define ULOG_SMGR_UNCOMMITED_STORAGE	0x10

/* undo log entry for uncommitted storage files */
typedef struct ul_uncommitted_storage
{
	RelFileLocator	rlocator;
	ForkNumber		forknum;
	bool			remove;
} ul_uncommitted_storage;

/* flags for xl_smgr_truncate */
#define SMGR_TRUNCATE_HEAP		0x0001

void smgr_undo(SimpleUndoLogRecord *record, bool crash_prepared);

#define ULogRecGetData(record) ((char *)record + sizeof(SimpleUndoLogRecord))

#endif							/* STORAGE_XLOG_H */
