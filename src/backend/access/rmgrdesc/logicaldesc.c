/*-------------------------------------------------------------------------
 *
 * logicaldesc.c
 *	  rmgr descriptor routines for replication/logical/logical_xlog.c
 *
 * Portions Copyright (c) 2015-2022, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/logicaldesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "replication/logical_xlog.h"

void
logical_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_LOGICAL_MESSAGE)
	{
		xl_logical_message *xlrec = (xl_logical_message *) rec;
		char	   *prefix = xlrec->message;
		char	   *message = xlrec->message + xlrec->prefix_size;
		char	   *sep = "";

		Assert(prefix[xlrec->prefix_size] != '\0');

		appendStringInfo(buf, "%s, prefix \"%s\"; payload (%zu bytes): ",
						 xlrec->transactional ? "transactional" : "non-transactional",
						 prefix, xlrec->message_size);
		/* Write message payload as a series of hex bytes */
		for (int cnt = 0; cnt < xlrec->message_size; cnt++)
		{
			appendStringInfo(buf, "%s%02X", sep, (unsigned char) message[cnt]);
			sep = " ";
		}
	}
	else if (info == XLOG_LOGICAL_INSERT)
	{

	}
	else if (info == XLOG_LOGICAL_UPDATE)
	{

	}
	else if (info == XLOG_LOGICAL_DELETE)
	{

	}
	else if (info == XLOG_LOGICAL_TRUNCATE)
	{

	}
}

const char *
logical_identify(uint8 info)
{
	switch (info)
	{
		case XLOG_LOGICAL_MESSAGE:
			return "MESSAGE";
		case XLOG_LOGICAL_INSERT:
			return "INSERT";
		case XLOG_LOGICAL_UPDATE:
			return "UPDATE";
		case XLOG_LOGICAL_DELETE:
			return "DELETE";
		case XLOG_LOGICAL_TRUNCATE:
			return "TRUNCATE";
		default:
			return NULL;
	}

	return NULL;
}
