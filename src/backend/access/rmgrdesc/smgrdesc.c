/*-------------------------------------------------------------------------
 *
 * smgrdesc.c
 *	  rmgr descriptor routines for catalog/storage.c
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/smgrdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/storage_xlog.h"


void
smgr_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_SMGR_CREATE)
	{
		xl_smgr_create *xlrec = (xl_smgr_create *) rec;
		char	   *path = relpathperm(xlrec->rnode, xlrec->forkNum);

		appendStringInfoString(buf, path);
		pfree(path);
	}
	else if (info == XLOG_SMGR_TRUNCATE)
	{
		xl_smgr_truncate *xlrec = (xl_smgr_truncate *) rec;
		char	   *path = relpathperm(xlrec->rnode, MAIN_FORKNUM);

		appendStringInfo(buf, "%s to %u blocks flags %d", path,
						 xlrec->blkno, xlrec->flags);
		pfree(path);
	}
	else if (info == XLOG_SMGR_UNLINK)
	{
		xl_smgr_unlink *xlrec = (xl_smgr_unlink *) rec;
		char	   *path = relpathperm(xlrec->rnode, xlrec->forkNum);

		appendStringInfoString(buf, path);
		pfree(path);
	}
	else if (info == XLOG_SMGR_MARK)
	{
		xl_smgr_mark *xlrec = (xl_smgr_mark *) rec;
		char	   *path = GetRelationPath(xlrec->rnode.dbNode,
										   xlrec->rnode.spcNode,
										   xlrec->rnode.relNode,
										   InvalidBackendId,
										   xlrec->forkNum, xlrec->mark);
		char	   *action = "<none>";

		switch (xlrec->action)
		{
			case XLOG_SMGR_MARK_CREATE:
				action = "CREATE";
				break;
			case XLOG_SMGR_MARK_UNLINK:
				action = "DELETE";
				break;
		}

		appendStringInfo(buf, "%s %s", action, path);
		pfree(path);
	}
	else if (info == XLOG_SMGR_BUFPERSISTENCE)
	{
		xl_smgr_bufpersistence *xlrec = (xl_smgr_bufpersistence *) rec;
		char	   *path = relpathperm(xlrec->rnode, MAIN_FORKNUM);

		appendStringInfoString(buf, path);
		appendStringInfo(buf, " persistence %d", xlrec->persistence);
		pfree(path);
	}
}

const char *
smgr_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_SMGR_CREATE:
			id = "CREATE";
			break;
		case XLOG_SMGR_TRUNCATE:
			id = "TRUNCATE";
			break;
		case XLOG_SMGR_UNLINK:
			id = "UNLINK";
			break;
		case XLOG_SMGR_MARK:
			id = "MARK";
			break;
		case XLOG_SMGR_BUFPERSISTENCE:
			id = "BUFPERSISTENCE";
			break;
	}

	return id;
}
