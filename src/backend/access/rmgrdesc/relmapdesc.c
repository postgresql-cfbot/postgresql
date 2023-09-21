/*-------------------------------------------------------------------------
 *
 * relmapdesc.c
 *	  rmgr descriptor routines for utils/cache/relmapper.c
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/relmapdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/relmapper.h"

void
relmap_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetRmgrInfo(record);

	if (info == XLOG_RELMAP_UPDATE)
	{
		xl_relmap_update *xlrec = (xl_relmap_update *) rec;

		appendStringInfo(buf, "database %u tablespace %u size %d",
						 xlrec->dbid, xlrec->tsid, xlrec->nbytes);
	}
}

const char *
relmap_identify(uint8 rmgrinfo)
{
	const char *id = NULL;

	switch (rmgrinfo)
	{
		case XLOG_RELMAP_UPDATE:
			id = "UPDATE";
			break;
	}

	return id;
}
