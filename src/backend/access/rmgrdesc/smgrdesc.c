/*-------------------------------------------------------------------------
 *
 * smgrdesc.c
 *	  rmgr descriptor routines for catalog/storage.c
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
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
#include "catalog/storage_undo.h"

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
	else if (info == XLOG_SMGR_PRECREATE)
	{
		xl_smgr_precreate *xlrec = (xl_smgr_precreate *) rec;
		char	   *path = relpathperm(xlrec->rnode, 0);

		appendStringInfoString(buf, path);
		pfree(path);
	}
	else if (info == XLOG_SMGR_DROP)
	{
		xl_smgr_drop *xlrec = (xl_smgr_drop *) rec;
		char	   *path = relpathperm(xlrec->rnode, 0);

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
}

const char *
smgr_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_SMGR_PRECREATE:
			id = "PRECREATE";
			break;
		case XLOG_SMGR_CREATE:
			id = "CREATE";
			break;
		case XLOG_SMGR_DROP:
			id = "DROP";
			break;
		case XLOG_SMGR_TRUNCATE:
			id = "TRUNCATE";
			break;
	}

	return id;
}

void
smgr_undo_desc(StringInfo buf, const WrittenUndoNode *record)
{
	xu_smgr_create *undo_rec;
	RelFileNode *rnode;

	Assert(record->n.rmid == RM_SMGR_ID);
	Assert(record->n.type == UNDO_SMGR_CREATE);

	undo_rec = (xu_smgr_create *) record->n.data;
	rnode = &undo_rec->rnode;

	appendStringInfo(buf, "CREATE dbid=%u, tsid=%u, relfile=%u",
					 rnode->dbNode,
					 rnode->spcNode,
					 rnode->relNode);
}
