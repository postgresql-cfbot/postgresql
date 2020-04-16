/*-------------------------------------------------------------------------
 *
 * fdwxactdesc.c
 *		PostgreSQL global transaction manager for foreign server.
 *
 * This module describes the WAL records for foreign transaction manager.
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
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

		appendStringInfo(buf, "server: %u,", fdwxact_insert->serverid);
		appendStringInfo(buf, " user: %u,", fdwxact_insert->userid);
		appendStringInfo(buf, " database: %u,", fdwxact_insert->dbid);
		appendStringInfo(buf, " local xid: %u,", fdwxact_insert->local_xid);
		appendStringInfo(buf, " id: %s", fdwxact_insert->fdwxact_id);
	}
	else
	{
		xl_fdwxact_remove *fdwxact_remove = (xl_fdwxact_remove *) rec;

		appendStringInfo(buf, "server: %u,", fdwxact_remove->serverid);
		appendStringInfo(buf, " user: %u,", fdwxact_remove->userid);
		appendStringInfo(buf, " database: %u,", fdwxact_remove->dbid);
		appendStringInfo(buf, " local xid: %u", fdwxact_remove->xid);
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
