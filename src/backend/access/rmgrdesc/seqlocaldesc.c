/*-------------------------------------------------------------------------
 *
 * seqlocaldesc.c
 *	  rmgr descriptor routines for sequence/seqlocal.c
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/seqlocaldesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/seqlocalam.h"


void
seq_local_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	xl_seq_local_rec *xlrec = (xl_seq_local_rec *) rec;

	if (info == XLOG_SEQ_LOCAL_LOG)
		appendStringInfo(buf, "rel %u/%u/%u",
						 xlrec->locator.spcOid, xlrec->locator.dbOid,
						 xlrec->locator.relNumber);
}

const char *
seq_local_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_SEQ_LOCAL_LOG:
			id = "SEQ_LOCAL_LOG";
			break;
	}

	return id;
}
