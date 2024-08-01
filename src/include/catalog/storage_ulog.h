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
#include "storage/smgr.h"

extern void smgr_undo(SimpleUndoLogRecord *record, UndoLogFileState prepared,
					  bool isCommit, bool cleanup);
#define ULogRecGetData(record) ((char *)record + sizeof(SimpleUndoLogRecord))

#endif							/* STORAGE_XLOG_H */
