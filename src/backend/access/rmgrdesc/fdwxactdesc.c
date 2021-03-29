/*-------------------------------------------------------------------------
 *
 * fdwxactdesc.c
 *		PostgreSQL global transaction manager for foreign server.
 *
 * This module describes the WAL records for foreign transaction manager.
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * src/backend/access/rmgrdesc/fdwxactdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/fdwxact_xlog.h"

void
fdwxact_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_FDWXACT_INSERT)
	{
		FdwXactOnDiskData *fdwxact_insert = (FdwXactOnDiskData *) rec;

		appendStringInfo(buf, "xid: %u, dbid: %u, umid: %u, serverid: %u, owner: %u, identifier: %s",
						 fdwxact_insert->xid,
						 fdwxact_insert->dbid,
						 fdwxact_insert->umid,
						 fdwxact_insert->serverid,
						 fdwxact_insert->owner,
						 fdwxact_insert->identifier);
	}
	else
	{
		xl_fdwxact_remove *fdwxact_remove = (xl_fdwxact_remove *) rec;

		appendStringInfo(buf, "xid: %u, umid: %u",
						 fdwxact_remove->xid,
						 fdwxact_remove->umid);
	}

}

const char *
fdwxact_identify(uint8 info)
{
	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_FDWXACT_INSERT:
			return "NEW FOREIGN TRANSACTION";
		case XLOG_FDWXACT_REMOVE:
			return "REMOVE FOREIGN TRANSACTION";
	}
	/* Keep compiler happy */
	return NULL;
}
