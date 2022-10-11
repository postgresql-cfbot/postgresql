/*-------------------------------------------------------------------------
 *
 * seqdesc.c
 *	  rmgr descriptor routines for commands/sequence.c
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
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
	uint8		rminfo = XLogRecGetRmInfo(record);
	xl_seq_rec *xlrec = (xl_seq_rec *) rec;

	if (rminfo == XLOG_SEQ_LOG)
		appendStringInfo(buf, "rel %u/%u/%u",
						 xlrec->locator.spcOid, xlrec->locator.dbOid,
						 xlrec->locator.relNumber);
}

const char *
seq_identify(uint8 rminfo)
{
	const char *id = NULL;

	switch (rminfo)
	{
		case XLOG_SEQ_LOG:
			id = "LOG";
			break;
	}

	return id;
}
