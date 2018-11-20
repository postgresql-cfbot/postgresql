/*-------------------------------------------------------------------------
 *
 * fdwxact_xlog.h
 *	  Foreign transaction XLOG definitions.
 *
 *
 * Portions Copyright (c) 2018, PostgreSQL Global Development Group
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
#define XLOG_FDW_XACT_INSERT	0x00
#define XLOG_FDW_XACT_REMOVE	0x10

/* Same as GIDSIZE */
#define FDW_XACT_MAX_ID_LEN 200
/*
 * On disk file structure, also used to WAL
 */
typedef struct
{
	TransactionId local_xid;
	Oid			dbid;			/* database oid where to find foreign server
								 * and user mapping */
	Oid			serverid;		/* foreign server where transaction takes
								 * place */
	Oid			userid;			/* user who initiated the foreign transaction */
	Oid			umid;
	char		fdw_xact_id[FDW_XACT_MAX_ID_LEN]; /* foreign txn prepare id */
} FdwXactOnDiskData;

typedef struct xl_fdw_xact_remove
{
	TransactionId xid;
	Oid			serverid;
	Oid			userid;
	Oid			dbid;
} xl_fdw_xact_remove;

extern void fdw_xact_redo(XLogReaderState *record);
extern void fdw_xact_desc(StringInfo buf, XLogReaderState *record);
extern const char *fdw_xact_identify(uint8 info);

#endif	/* FDWXACT_XLOG_H */
