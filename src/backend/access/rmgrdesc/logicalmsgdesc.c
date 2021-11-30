/*-------------------------------------------------------------------------
 *
 * logicalmsgdesc.c
 *	  rmgr descriptor routines for replication/logical/message.c
 *
 * Portions Copyright (c) 2015-2021, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/logicalmsgdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "replication/message.h"

void
logicalmsg_desc(StringInfo buf, XLogReaderState *record)
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
	else if (info == XLOG_LOGICAL_SEQUENCE)
	{
		xl_logical_sequence *xlrec = (xl_logical_sequence *) rec;

		appendStringInfo(buf, "rel %u/%u/%u last: %lu log_cnt: %lu is_called: %d",
						 xlrec->node.spcNode,
						 xlrec->node.dbNode,
						 xlrec->node.relNode,
						 xlrec->last, xlrec->log_cnt, xlrec->is_called);
	}
}

const char *
logicalmsg_identify(uint8 info)
{
	if ((info & ~XLR_INFO_MASK) == XLOG_LOGICAL_MESSAGE)
		return "MESSAGE";

	if ((info & ~XLR_INFO_MASK) == XLOG_LOGICAL_SEQUENCE)
		return "SEQUENCE";

	return NULL;
}
