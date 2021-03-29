/*-------------------------------------------------------------------------
 *
 * fdwxact_xlog.h
 *	  Foreign transaction XLOG definitions.
 *
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * src/include/access/fdwxact_xlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FDWXACT_XLOG_H
#define FDWXACT_XLOG_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"

/* Info types for logs related to FDW transactions */
#define XLOG_FDWXACT_INSERT	0x00
#define XLOG_FDWXACT_REMOVE	0x10

/* Maximum length of the prepared transaction id, borrowed from twophase.c */
#define FDWXACT_ID_MAX_LEN 200

/*
 * On disk file structure, also used to WAL
 */
typedef struct
{
	TransactionId xid;
	Oid		dbid;
	Oid		umid;
	Oid		serverid;
	Oid		owner;
	char	identifier[FDWXACT_ID_MAX_LEN]; /* foreign txn prepare id */
} FdwXactOnDiskData;

typedef struct xl_fdwxact_remove
{
	TransactionId xid;
	Oid		umid;
	bool	force;
} xl_fdwxact_remove;

extern void fdwxact_redo(XLogReaderState *record);
extern void fdwxact_desc(StringInfo buf, XLogReaderState *record);
extern const char *fdwxact_identify(uint8 info);

#endif							/* FDWXACT_XLOG_H */
