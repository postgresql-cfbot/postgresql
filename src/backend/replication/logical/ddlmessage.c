/*-------------------------------------------------------------------------
 *
 * ddlmessage.c
 *	  Logical DDL messages.
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/ddlmessage.c
 *
 * NOTES
 *
 * Logical DDL messages allow XLOG logging of DDL command strings that
 * get passed to the logical decoding plugin. In normal XLOG processing they
 * are same as NOOP.
 *
 * Unlike generic logical messages, these DDL messages have only transactional
 * mode. Note by default DDLs in PostgreSQL are transactional.
 *
 * These messages are part of current transaction and will be sent to
 * decoding plugin using in a same way as DML operations.
 *
 * Every message carries prefix to avoid conflicts between different decoding
 * plugins. The plugin authors must take extra care to use unique prefix,
 * good options seems to be for example to use the name of the extension.
 *
 * ---------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "access/xloginsert.h"
#include "catalog/namespace.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "replication/logical.h"
#include "replication/ddlmessage.h"
#include "utils/memutils.h"

/*
 * Write logical decoding DDL message into XLog.
 */
XLogRecPtr
LogLogicalDDLMessage(const char *prefix, Oid relid, DeparsedCommandType cmdtype,
					 const char *message, size_t size)
{
	xl_logical_ddl_message xlrec;

	 /* Ensure we have a valid transaction id. */
	Assert(IsTransactionState());
	GetCurrentTransactionId();

	xlrec.dbId = MyDatabaseId;
	/* Trailing zero is critical; see logicalddlmsg_desc */
	xlrec.prefix_size = strlen(prefix) + 1;
	xlrec.message_size = size;
	xlrec.relid = relid;
	xlrec.cmdtype = cmdtype;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfLogicalDDLMessage);
	XLogRegisterData(unconstify(char *, prefix), xlrec.prefix_size);
	XLogRegisterData(unconstify(char *, message), size);

	/* Allow origin filtering */
	XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

	return XLogInsert(RM_LOGICALDDLMSG_ID, XLOG_LOGICAL_DDL_MESSAGE);
}

/*
 * Redo is basically just noop for logical decoding ddl messages.
 */
void
logicalddlmsg_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info != XLOG_LOGICAL_DDL_MESSAGE)
		elog(PANIC, "logicalddlmsg_redo: unknown op code %u", info);

	/* This is only interesting for logical decoding, see decode.c. */
}
