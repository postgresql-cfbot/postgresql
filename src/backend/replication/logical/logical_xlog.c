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

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "replication/logical.h"
#include "replication/logical_xlog.h"
#include "utils/memutils.h"
#include "utils/relcache.h"

static void CheckReplicaIdentity(Relation relation, TupleTableSlot *slot);

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

/*
 * Write logical insert into log.
 */
XLogRecPtr
LogLogicalInsert(Relation relation, TupleTableSlot *new_slot)
{
	bool		free_new_tuple;
	HeapTuple	new_tuple;
	xl_logical_insert xlrec;
	xl_heap_header xlhdr;
	XLogRecPtr recptr;

	if (!equalTupleDescs(relation->rd_att, new_slot->tts_tupleDescriptor))
		ereport(ERROR, (errmsg("record type must match relation type")));

	if (!RelationIsLogicallyLogged(relation))
		return InvalidXLogRecPtr;

	new_tuple = ExecFetchSlotHeapTuple(new_slot, true, &free_new_tuple);

	/* force xid to be allocated */
	Assert(IsTransactionState());
	GetCurrentTransactionId();

	new_tuple->t_tableOid = new_slot->tts_tableOid;
	ItemPointerCopy(&new_tuple->t_self, &new_slot->tts_tid);

	xlrec.node = relation->rd_node;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfLogicalInsert);

	xlhdr.t_infomask2 = new_tuple->t_data->t_infomask2;
	xlhdr.t_infomask = new_tuple->t_data->t_infomask;
	xlhdr.t_hoff = new_tuple->t_data->t_hoff;

	XLogRegisterData((char *) &xlhdr, SizeOfHeapHeader);
	XLogRegisterData((char *) new_tuple->t_data + SizeofHeapTupleHeader,
					 new_tuple->t_len - SizeofHeapTupleHeader);

	/* allow origin filtering */
	XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

	recptr = XLogInsert(RM_LOGICAL_ID, XLOG_LOGICAL_INSERT);

	if (free_new_tuple)
		pfree(new_tuple);

	return recptr;
}

/*
 * Write logical update into log.
 */
XLogRecPtr
LogLogicalUpdate(Relation relation, TupleTableSlot *old_slot,
				 TupleTableSlot *new_slot)
{
	HeapTuple	old_whole_tuple;
	HeapTuple	old_id_tuple;
	HeapTuple	new_tuple;
	bool		free_old_whole_tuple;
	bool		free_old_id_tuple;
	bool		free_new_tuple;
	xl_heap_header new_xlhdr;
	xl_logical_update xlrec;
	XLogRecPtr recptr;

	if (!equalTupleDescs(relation->rd_att, old_slot->tts_tupleDescriptor) ||
		!equalTupleDescs(relation->rd_att, new_slot->tts_tupleDescriptor))
		ereport(ERROR, (errmsg("record types must match relation type")));

	CheckReplicaIdentity(relation, old_slot);

	if (!RelationIsLogicallyLogged(relation))
		return InvalidXLogRecPtr;

	/* force xid to be allocated */
	Assert(IsTransactionState());
	GetCurrentTransactionId();

	old_whole_tuple = ExecFetchSlotHeapTuple(old_slot, true,
											 &free_old_whole_tuple);
	new_tuple = ExecFetchSlotHeapTuple(new_slot, true, &free_new_tuple);

	xlrec.node = relation->rd_node;
	xlrec.new_datalen = new_tuple->t_len - SizeofHeapTupleHeader +
		SizeOfHeapHeader;
	xlrec.flags = 0;

	old_id_tuple = ExtractReplicaIdentity(relation, old_whole_tuple, true,
										  &free_old_id_tuple);

	if (old_id_tuple != NULL)
	{
		xlrec.flags |= XLL_UPDATE_CONTAINS_OLD;
		old_id_tuple->t_tableOid = old_slot->tts_tableOid;
		ItemPointerCopy(&old_id_tuple->t_self, &old_slot->tts_tid);
	}

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfLogicalUpdate);

	new_tuple->t_tableOid = new_slot->tts_tableOid;
	ItemPointerCopy(&new_tuple->t_self, &new_slot->tts_tid);

	new_xlhdr.t_infomask2 = new_tuple->t_data->t_infomask2;
	new_xlhdr.t_infomask = new_tuple->t_data->t_infomask;
	new_xlhdr.t_hoff = new_tuple->t_data->t_hoff;

	/* write new tuple first, then old */
	XLogRegisterData((char *) &new_xlhdr, SizeOfHeapHeader);
	XLogRegisterData((char *) new_tuple->t_data + SizeofHeapTupleHeader,
					 new_tuple->t_len - SizeofHeapTupleHeader);

	if (old_id_tuple != NULL)
	{
		xl_heap_header old_xlhdr;

		old_xlhdr.t_infomask2 = old_id_tuple->t_data->t_infomask2;
		old_xlhdr.t_infomask = old_id_tuple->t_data->t_infomask;
		old_xlhdr.t_hoff = old_id_tuple->t_data->t_hoff;

		XLogRegisterData((char *) &old_xlhdr, SizeOfHeapHeader);
		XLogRegisterData((char *) old_id_tuple->t_data + SizeofHeapTupleHeader,
					 old_id_tuple->t_len - SizeofHeapTupleHeader);
	}

	/* allow origin filtering */
	XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

	recptr = XLogInsert(RM_LOGICAL_ID, XLOG_LOGICAL_UPDATE);

	if (free_old_whole_tuple)
		pfree(old_whole_tuple);
	if (free_old_id_tuple)
		pfree(old_id_tuple);
	if (free_new_tuple)
		pfree(new_tuple);

	return recptr;
}

/*
 * Write logical update into log.
 */
XLogRecPtr
LogLogicalDelete(Relation relation, TupleTableSlot *old_slot)
{
	HeapTuple		 old_whole_tuple;
	HeapTuple		 old_id_tuple;
	bool			 free_old_whole_tuple;
	bool			 free_old_id_tuple;
	xl_logical_delete xlrec;
	XLogRecPtr		 recptr;

	if (!equalTupleDescs(relation->rd_att, old_slot->tts_tupleDescriptor))
		ereport(ERROR, (errmsg("record type must match relation type")));

	CheckReplicaIdentity(relation, old_slot);

	if (!RelationIsLogicallyLogged(relation))
		return InvalidXLogRecPtr;

	/* force xid to be allocated */
	Assert(IsTransactionState());
	GetCurrentTransactionId();

	old_whole_tuple = ExecFetchSlotHeapTuple(old_slot, true,
											 &free_old_whole_tuple);

	xlrec.node = relation->rd_node;
	xlrec.flags = 0;

	old_id_tuple = ExtractReplicaIdentity(relation, old_whole_tuple, true,
										  &free_old_id_tuple);

	if (old_id_tuple != NULL)
	{
		xlrec.flags |= XLL_UPDATE_CONTAINS_OLD;
		old_id_tuple->t_tableOid = old_slot->tts_tableOid;
		ItemPointerCopy(&old_id_tuple->t_self, &old_slot->tts_tid);
	}

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfLogicalDelete);

	if (old_id_tuple != NULL)
	{
		xl_heap_header old_xlhdr;

		old_xlhdr.t_infomask2 = old_id_tuple->t_data->t_infomask2;
		old_xlhdr.t_infomask = old_id_tuple->t_data->t_infomask;
		old_xlhdr.t_hoff = old_id_tuple->t_data->t_hoff;

		XLogRegisterData((char *) &old_xlhdr, SizeOfHeapHeader);
		XLogRegisterData((char *) old_id_tuple->t_data + SizeofHeapTupleHeader,
					 old_id_tuple->t_len - SizeofHeapTupleHeader);
	}

	/* allow origin filtering */
	XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

	recptr = XLogInsert(RM_LOGICAL_ID, XLOG_LOGICAL_DELETE);

	if (free_old_whole_tuple)
		pfree(old_whole_tuple);
	if (free_old_id_tuple)
		pfree(old_id_tuple);

	return recptr;
}

/*
 * Write logical truncate into log.
 */
XLogRecPtr
LogLogicalTruncate(List *relids, bool cascade, bool restart_seqs)
{
	xl_logical_truncate	 xlrec;
	XLogRecPtr			 recptr;
	Oid					*logrelids;
	ListCell			*lc;
	int					 nrelids = 0;

	/* force xid to be allocated */
	Assert(IsTransactionState());
	GetCurrentTransactionId();

	xlrec.dbId = MyDatabaseId;
	xlrec.nrelids = list_length(relids);
	xlrec.flags = 0;
	if (cascade)
		xlrec.flags |= XLL_TRUNCATE_CASCADE;
	if (restart_seqs)
		xlrec.flags |= XLL_TRUNCATE_RESTART_SEQS;

	logrelids = palloc(list_length(relids) * sizeof(Oid));
	foreach(lc, relids)
	{
		Oid			relid = lfirst_oid(lc);
		Relation	rel	  = relation_open(relid, AccessShareLock);

		if (RelationIsLogicallyLogged(rel))
		{
			logrelids[nrelids++] = lfirst_oid(lc);
		}

		relation_close(rel, NoLock);
	}

	if (nrelids == 0)
		return InvalidXLogRecPtr;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfLogicalTruncate);
	XLogRegisterData((char *) logrelids, nrelids * sizeof(Oid));

	/* allow origin filtering */
	XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

	recptr = XLogInsert(RM_LOGICAL_ID, XLOG_LOGICAL_TRUNCATE);

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

/*
 * Check that replica identity columns are non-NULL.
 */
static void
CheckReplicaIdentity(Relation relation, TupleTableSlot *slot)
{
	/* check for NULL attributes in the replica identity */
	Bitmapset *id_attrs = RelationGetIndexAttrBitmap(
		relation, INDEX_ATTR_BITMAP_IDENTITY_KEY);
	int id_attr = (-1) * FirstLowInvalidHeapAttributeNumber;

	while ((id_attr = bms_next_member(id_attrs, id_attr)) >= 0)
	{
		AttrNumber attno = id_attr + FirstLowInvalidHeapAttributeNumber;
		if (slot_attisnull(slot, attno))
			ereport(ERROR, (errmsg("replica identity column is NULL")));
	}
}
