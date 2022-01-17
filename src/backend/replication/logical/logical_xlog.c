/*-------------------------------------------------------------------------
 *
 * logical_xlog.c
 *	  Logical xlog records.
 *
 * Copyright (c) 2013-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/logical_xlog.c
 *
 * Logical Messages
 *
 * Generic logical messages allow XLOG logging of arbitrary binary blobs that
 * get passed to the logical decoding plugin. In normal XLOG processing they
 * are same as NOOP.
 *
 * These messages can be either transactional or non-transactional.
 * Transactional messages are part of current transaction and will be sent to
 * decoding plugin using in a same way as DML operations.
 * Non-transactional messages are sent to the plugin at the time when the
 * logical decoding reads them from XLOG. This also means that transactional
 * messages won't be delivered if the transaction was rolled back but the
 * non-transactional one will always be delivered.
 *
 * Every message carries prefix to avoid conflicts between different decoding
 * plugins. The plugin authors must take extra care to use unique prefix,
 * good options seems to be for example to use the name of the extension.
 *
 * Logical Insert/Update/Delete/Truncate
 *
 * These records are intended to be used by non-heap table access methods that
 * wish to support logical decoding and replication. They are treated
 * similarly to the analogous heap records, but are not tied to physical pages
 * or other details of the heap. These records are not processed during redo,
 * so do not contribute to durability or physical replication; use generic WAL
 * records for that. Note that using both logical WAL records and generic WAL
 * records is redundant compared with the heap.
 *
 * ---------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam_xlog.h"
#include "access/xact.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "replication/logical.h"
#include "replication/logical_xlog.h"
#include "utils/memutils.h"

/*
 * Write logical decoding message into XLog.
 */
XLogRecPtr
LogLogicalMessage(const char *prefix, const char *message, size_t size,
				  bool transactional)
{
	xl_logical_message xlrec;

	/*
	 * Force xid to be allocated if we're emitting a transactional message.
	 */
	if (transactional)
	{
		Assert(IsTransactionState());
		GetCurrentTransactionId();
	}

	xlrec.dbId = MyDatabaseId;
	xlrec.transactional = transactional;
	/* trailing zero is critical; see logicalmsg_desc */
	xlrec.prefix_size = strlen(prefix) + 1;
	xlrec.message_size = size;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfLogicalMessage);
	XLogRegisterData(unconstify(char *, prefix), xlrec.prefix_size);
	XLogRegisterData(unconstify(char *, message), size);

	/* allow origin filtering */
	XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

	return XLogInsert(RM_LOGICAL_ID, XLOG_LOGICAL_MESSAGE);
}

XLogRecPtr
LogLogicalInsert(Relation relation, TupleTableSlot *slot)
{
	bool		shouldFree;
	HeapTuple	tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);
	xl_logical_insert xlrec;
	xl_heap_header xlhdr;
	XLogRecPtr recptr;

	/* force xid to be allocated */
	Assert(IsTransactionState());
	GetCurrentTransactionId();

	tuple->t_tableOid = slot->tts_tableOid;
	ItemPointerCopy(&tuple->t_self, &slot->tts_tid);

	xlrec.node = relation->rd_node;
	xlrec.datalen = tuple->t_len;
	xlrec.offnum = ItemPointerGetOffsetNumber(&tuple->t_self);
	xlrec.flags = 0;
	xlrec.flags |= XLH_INSERT_CONTAINS_NEW_TUPLE;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfLogicalInsert);

	xlhdr.t_infomask2 = tuple->t_data->t_infomask2;
	xlhdr.t_infomask = tuple->t_data->t_infomask;
	xlhdr.t_hoff = tuple->t_data->t_hoff;

	XLogRegisterData((char *) &xlhdr, SizeOfHeapHeader);
	/* PG73FORMAT: write bitmap [+ padding] [+ oid] + data */
	XLogRegisterData((char *) tuple->t_data + SizeofHeapTupleHeader,
					 tuple->t_len - SizeofHeapTupleHeader);


	/* allow origin filtering */
	XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

	recptr = XLogInsert(RM_LOGICAL_ID, XLOG_LOGICAL_INSERT);

	if (shouldFree)
		pfree(tuple);

	return recptr;
}


/*
 * Redo is basically just noop for logical decoding messages.
 */
void
logical_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_LOGICAL_MESSAGE:
		case XLOG_LOGICAL_INSERT:
		case XLOG_LOGICAL_UPDATE:
		case XLOG_LOGICAL_DELETE:
		case XLOG_LOGICAL_TRUNCATE:
			break;
		default:
			elog(PANIC, "logical_redo: unknown op code %u", info);
	}

	/* This is only interesting for logical decoding, see decode.c. */
}
