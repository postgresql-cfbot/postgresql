/*-------------------------------------------------------------------------
 *
 * seqdesc.c
 *	  rmgr descriptor routines for commands/sequence.c
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/seqdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/sequence.h"


void
seq_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetRmgrInfo(record);
	xl_seq_rec *xlrec = (xl_seq_rec *) rec;

	if (info == XLOG_SEQ_LOG)
		appendStringInfo(buf, "rel %u/%u/%u",
						 xlrec->locator.spcOid, xlrec->locator.dbOid,
						 xlrec->locator.relNumber);
}

const char *
seq_identify(uint8 rmgrinfo)
{
	const char *id = NULL;

	switch (rmgrinfo)
	{
		case XLOG_SEQ_LOG:
			id = "LOG";
			break;
	}

	return id;
}
