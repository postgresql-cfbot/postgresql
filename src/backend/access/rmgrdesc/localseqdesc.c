/*-------------------------------------------------------------------------
 *
 * localseqdesc.c
 *	  rmgr descriptor routines for sequence/local.c
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/seqdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/localam.h"


void
local_seq_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	xl_local_seq_rec *xlrec = (xl_local_seq_rec *) rec;

	if (info == XLOG_LOCAL_SEQ_LOG)
		appendStringInfo(buf, "rel %u/%u/%u",
						 xlrec->locator.spcOid, xlrec->locator.dbOid,
						 xlrec->locator.relNumber);
}

const char *
local_seq_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_LOCAL_SEQ_LOG:
			id = "LOCAL_SEQ_LOG";
			break;
	}

	return id;
}
