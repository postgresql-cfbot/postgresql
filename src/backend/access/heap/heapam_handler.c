/*-------------------------------------------------------------------------
 *
 * heapam_handler.c
 *	  heap table access method code
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/heapam_handler.c
 *
 *
 * NOTES
 *	  This file contains the heap_ routines which implement
 *	  the POSTGRES heap table access method used for all POSTGRES
 *	  relations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/relscan.h"
#include "access/rewriteheap.h"
#include "access/tableamapi.h"
#include "pgstat.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/tqual.h"


/* ----------------------------------------------------------------
 *				storage AM support routines for heapam
 * ----------------------------------------------------------------
 */

static bool
heapam_fetch(Relation relation,
			 ItemPointer tid,
			 Snapshot snapshot,
			 TableTuple * stuple,
			 Buffer *userbuf,
			 bool keep_buf,
			 Relation stats_relation)
{
	HeapTupleData tuple;

	*stuple = NULL;
	if (heap_fetch(relation, tid, snapshot, &tuple, userbuf, keep_buf, stats_relation))
	{
		*stuple = heap_copytuple(&tuple);
		return true;
	}

	return false;
}

/*
 * Insert a heap tuple from a slot, which may contain an OID and speculative
 * insertion token.
 */
static Oid
heapam_heap_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
				   int options, BulkInsertState bistate, InsertIndexTuples IndexFunc,
				   EState *estate, List *arbiterIndexes, List **recheckIndexes)
{
	Oid			oid;
	HeapTuple	tuple = NULL;

	if (slot->tts_storage)
	{
		HeapamTuple *htuple = slot->tts_storage;

		tuple = htuple->hst_heaptuple;

		if (relation->rd_rel->relhasoids)
			HeapTupleSetOid(tuple, InvalidOid);
	}
	else
	{
		/*
		 * Obtain the physical tuple to insert, building from the slot values.
		 * XXX: maybe the slot already contains a physical tuple in the right
		 * format?  In fact, if the slot isn't fully deformed, this is
		 * completely bogus ...
		 */
		tuple = heap_form_tuple(slot->tts_tupleDescriptor,
								slot->tts_values,
								slot->tts_isnull);
	}

	/* Set the OID, if the slot has one */
	if (slot->tts_tupleOid != InvalidOid)
		HeapTupleHeaderSetOid(tuple->t_data, slot->tts_tupleOid);

	/* Update the tuple with table oid */
	if (slot->tts_tableOid != InvalidOid)
		tuple->t_tableOid = slot->tts_tableOid;

	/* Set the speculative insertion token, if the slot has one */
	if ((options & HEAP_INSERT_SPECULATIVE) && slot->tts_speculativeToken)
		HeapTupleHeaderSetSpeculativeToken(tuple->t_data, slot->tts_speculativeToken);

	/* Perform the insertion, and copy the resulting ItemPointer */
	oid = heap_insert(relation, tuple, cid, options, bistate);
	ItemPointerCopy(&tuple->t_self, &slot->tts_tid);

	if (slot->tts_storage == NULL)
		ExecStoreTuple(tuple, slot, InvalidBuffer, true);

	if ((estate != NULL) && (estate->es_result_relation_info->ri_NumIndices > 0))
	{
		Assert(IndexFunc != NULL);

		if (options & HEAP_INSERT_SPECULATIVE)
		{
			bool		specConflict = false;

			*recheckIndexes = (IndexFunc) (slot, estate, true,
										   &specConflict,
										   arbiterIndexes);

			/* adjust the tuple's state accordingly */
			if (!specConflict)
				heap_finish_speculative(relation, slot);
			else
			{
				heap_abort_speculative(relation, slot);
				slot->tts_specConflict = true;
			}
		}
		else
		{
			*recheckIndexes = (IndexFunc) (slot, estate, false,
										   NULL, arbiterIndexes);
		}
	}

	return oid;
}

static HTSU_Result
heapam_heap_delete(Relation relation, ItemPointer tid, CommandId cid,
				   Snapshot crosscheck, bool wait, DeleteIndexTuples IndexFunc,
				   HeapUpdateFailureData *hufd, bool changingPart)
{
	/*
	 * Currently Deleting of index tuples are handled at vacuum, in case
	 * if the storage itself is cleaning the dead tuples by itself, it is
	 * the time to call the index tuple deletion also.
	 */
	return heap_delete(relation, tid, cid, crosscheck, wait, hufd, changingPart);
}


/*
 * Locks tuple and fetches its newest version and TID.
 *
 *	relation - table containing tuple
 *	*tid - TID of tuple to lock (rest of struct need not be valid)
 *	snapshot - snapshot indentifying required version (used for assert check only)
 *	*stuple - tuple to be returned
 *	cid - current command ID (used for visibility test, and stored into
 *		  tuple's cmax if lock is successful)
 *	mode - indicates if shared or exclusive tuple lock is desired
 *	wait_policy - what to do if tuple lock is not available
 *	flags – indicating how do we handle updated tuples
 *	*hufd - filled in failure cases
 *
 * Function result may be:
 *	HeapTupleMayBeUpdated: lock was successfully acquired
 *	HeapTupleInvisible: lock failed because tuple was never visible to us
 *	HeapTupleSelfUpdated: lock failed because tuple updated by self
 *	HeapTupleUpdated: lock failed because tuple updated by other xact
 *	HeapTupleDeleted: lock failed because tuple deleted by other xact
 *	HeapTupleWouldBlock: lock couldn't be acquired and wait_policy is skip
 *
 * In the failure cases other than HeapTupleInvisible, the routine fills
 * *hufd with the tuple's t_ctid, t_xmax (resolving a possible MultiXact,
 * if necessary), and t_cmax (the last only for HeapTupleSelfUpdated,
 * since we cannot obtain cmax from a combocid generated by another
 * transaction).
 * See comments for struct HeapUpdateFailureData for additional info.
 */
static HTSU_Result
heapam_lock_tuple(Relation relation, ItemPointer tid, Snapshot snapshot,
				TableTuple *stuple, CommandId cid, LockTupleMode mode,
				LockWaitPolicy wait_policy, uint8 flags,
				HeapUpdateFailureData *hufd)
{
	HTSU_Result		result;
	HeapTupleData	tuple;
	Buffer			buffer;

	Assert(stuple != NULL);
	*stuple = NULL;

	hufd->traversed = false;

retry:
	tuple.t_self = *tid;
	result = heap_lock_tuple(relation, &tuple, cid, mode, wait_policy,
		(flags & TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS) ? true : false,
		&buffer, hufd);

	if (result == HeapTupleUpdated &&
		(flags & TUPLE_LOCK_FLAG_FIND_LAST_VERSION))
	{
		ReleaseBuffer(buffer);
		/* Should not encounter speculative tuple on recheck */
		Assert(!HeapTupleHeaderIsSpeculative(tuple.t_data));

		if (!ItemPointerEquals(&hufd->ctid, &tuple.t_self))
		{
			SnapshotData	SnapshotDirty;
			TransactionId	priorXmax;

			/* it was updated, so look at the updated version */
			*tid = hufd->ctid;
			/* updated row should have xmin matching this xmax */
			priorXmax = hufd->xmax;

			/*
			 * fetch target tuple
			 *
			 * Loop here to deal with updated or busy tuples
			 */
			InitDirtySnapshot(SnapshotDirty);
			for (;;)
			{
				if (ItemPointerIndicatesMovedPartitions(tid))
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("tuple to be locked was already moved to another partition due to concurrent update")));


				if (heap_fetch(relation, tid, &SnapshotDirty, &tuple, &buffer, true, NULL))
				{
					/*
					 * If xmin isn't what we're expecting, the slot must have been
					 * recycled and reused for an unrelated tuple.  This implies that
					 * the latest version of the row was deleted, so we need do
					 * nothing.  (Should be safe to examine xmin without getting
					 * buffer's content lock.  We assume reading a TransactionId to be
					 * atomic, and Xmin never changes in an existing tuple, except to
					 * invalid or frozen, and neither of those can match priorXmax.)
					 */
					if (!TransactionIdEquals(HeapTupleHeaderGetXmin(tuple.t_data),
											 priorXmax))
					{
						ReleaseBuffer(buffer);
						return HeapTupleDeleted;
					}

					/* otherwise xmin should not be dirty... */
					if (TransactionIdIsValid(SnapshotDirty.xmin))
						elog(ERROR, "t_xmin is uncommitted in tuple to be updated");

					/*
					 * If tuple is being updated by other transaction then we have to
					 * wait for its commit/abort, or die trying.
					 */
					if (TransactionIdIsValid(SnapshotDirty.xmax))
					{
						ReleaseBuffer(buffer);
						switch (wait_policy)
						{
							case LockWaitBlock:
								XactLockTableWait(SnapshotDirty.xmax,
												  relation, &tuple.t_self,
												  XLTW_FetchUpdated);
								break;
							case LockWaitSkip:
								if (!ConditionalXactLockTableWait(SnapshotDirty.xmax))
									return result;	/* skip instead of waiting */
								break;
							case LockWaitError:
								if (!ConditionalXactLockTableWait(SnapshotDirty.xmax))
									ereport(ERROR,
											(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
											 errmsg("could not obtain lock on row in relation \"%s\"",
													RelationGetRelationName(relation))));
								break;
						}
						continue;		/* loop back to repeat heap_fetch */
					}

					/*
					 * If tuple was inserted by our own transaction, we have to check
					 * cmin against es_output_cid: cmin >= current CID means our
					 * command cannot see the tuple, so we should ignore it. Otherwise
					 * heap_lock_tuple() will throw an error, and so would any later
					 * attempt to update or delete the tuple.  (We need not check cmax
					 * because HeapTupleSatisfiesDirty will consider a tuple deleted
					 * by our transaction dead, regardless of cmax.) We just checked
					 * that priorXmax == xmin, so we can test that variable instead of
					 * doing HeapTupleHeaderGetXmin again.
					 */
					if (TransactionIdIsCurrentTransactionId(priorXmax) &&
						HeapTupleHeaderGetCmin(tuple.t_data) >= cid)
					{
						ReleaseBuffer(buffer);
						return result;
					}

					hufd->traversed = true;
					*tid = tuple.t_data->t_ctid;
					ReleaseBuffer(buffer);
					goto retry;
				}

				/*
				 * If the referenced slot was actually empty, the latest version of
				 * the row must have been deleted, so we need do nothing.
				 */
				if (tuple.t_data == NULL)
				{
					ReleaseBuffer(buffer);
					return HeapTupleDeleted;
				}

				/*
				 * As above, if xmin isn't what we're expecting, do nothing.
				 */
				if (!TransactionIdEquals(HeapTupleHeaderGetXmin(tuple.t_data),
										 priorXmax))
				{
					ReleaseBuffer(buffer);
					return HeapTupleDeleted;
				}

				/*
				 * If we get here, the tuple was found but failed SnapshotDirty.
				 * Assuming the xmin is either a committed xact or our own xact (as it
				 * certainly should be if we're trying to modify the tuple), this must
				 * mean that the row was updated or deleted by either a committed xact
				 * or our own xact.  If it was deleted, we can ignore it; if it was
				 * updated then chain up to the next version and repeat the whole
				 * process.
				 *
				 * As above, it should be safe to examine xmax and t_ctid without the
				 * buffer content lock, because they can't be changing.
				 */
				if (ItemPointerEquals(&tuple.t_self, &tuple.t_data->t_ctid))
				{
					/* deleted, so forget about it */
					ReleaseBuffer(buffer);
					return HeapTupleDeleted;
				}

				/* updated, so look at the updated row */
				*tid = tuple.t_data->t_ctid;
				/* updated row should have xmin matching this xmax */
				priorXmax = HeapTupleHeaderGetUpdateXid(tuple.t_data);
				ReleaseBuffer(buffer);
				/* loop back to fetch next in chain */
			}
		}
		else
		{
			/* tuple was deleted, so give up */
			return HeapTupleDeleted;
		}
	}

	Assert((flags & TUPLE_LOCK_FLAG_FIND_LAST_VERSION) ||
			HeapTupleSatisfies((TableTuple) &tuple, snapshot, InvalidBuffer));

	*stuple = heap_copytuple(&tuple);
	ReleaseBuffer(buffer);

	return result;
}


static HTSU_Result
heapam_heap_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
				   EState *estate, CommandId cid, Snapshot crosscheck,
				   bool wait, HeapUpdateFailureData *hufd, LockTupleMode *lockmode,
				   InsertIndexTuples IndexFunc, List **recheckIndexes)
{
	HeapTuple	tuple;
	HTSU_Result result;

	if (slot->tts_storage)
	{
		HeapamTuple *htuple = slot->tts_storage;

		tuple = htuple->hst_heaptuple;
	}
	else
	{
		tuple = heap_form_tuple(slot->tts_tupleDescriptor,
								slot->tts_values,
								slot->tts_isnull);
	}

	/* Set the OID, if the slot has one */
	if (slot->tts_tupleOid != InvalidOid)
		HeapTupleHeaderSetOid(tuple->t_data, slot->tts_tupleOid);

	/* Update the tuple with table oid */
	if (slot->tts_tableOid != InvalidOid)
		tuple->t_tableOid = slot->tts_tableOid;

	result = heap_update(relation, otid, tuple, cid, crosscheck, wait,
						 hufd, lockmode);
	ItemPointerCopy(&tuple->t_self, &slot->tts_tid);

	if (slot->tts_storage == NULL)
		ExecStoreTuple(tuple, slot, InvalidBuffer, true);

	/*
	 * Note: instead of having to update the old index tuples associated with
	 * the heap tuple, all we do is form and insert new index tuples. This is
	 * because UPDATEs are actually DELETEs and INSERTs, and index tuple
	 * deletion is done later by VACUUM (see notes in ExecDelete). All we do
	 * here is insert new index tuples.  -cim 9/27/89
	 */

	/*
	 * insert index entries for tuple
	 *
	 * Note: heap_update returns the tid (location) of the new tuple in the
	 * t_self field.
	 *
	 * If it's a HOT update, we mustn't insert new index entries.
	 */
	if ((result == HeapTupleMayBeUpdated) &&
		((estate != NULL) && (estate->es_result_relation_info->ri_NumIndices > 0)) &&
		(!HeapTupleIsHeapOnly(tuple)))
		*recheckIndexes = (IndexFunc) (slot, estate, false, NULL, NIL);

	return result;
}

static tuple_data
heapam_get_tuple_data(TableTuple tuple, tuple_data_flags flags)
{
	tuple_data	result;

	switch (flags)
	{
		case XMIN:
			result.xid = HeapTupleHeaderGetXmin(((HeapTuple) tuple)->t_data);
			break;
		case UPDATED_XID:
			result.xid = HeapTupleHeaderGetUpdateXid(((HeapTuple) tuple)->t_data);
			break;
		case CMIN:
			result.cid = HeapTupleHeaderGetCmin(((HeapTuple) tuple)->t_data);
			break;
		case TID:
			result.tid = ((HeapTuple) tuple)->t_self;
			break;
		case CTID:
			result.tid = ((HeapTuple) tuple)->t_data->t_ctid;
			break;
		default:
			Assert(0);
			break;
	}

	return result;
}

static TableTuple
heapam_form_tuple_by_datum(Datum data, Oid tableoid)
{
	return heap_form_tuple_by_datum(data, tableoid);
}

static ParallelHeapScanDesc
heapam_get_parallelheapscandesc(TableScanDesc sscan)
{
	HeapScanDesc scan = (HeapScanDesc) sscan;

	return scan->rs_parallel;
}

static HeapPageScanDesc
heapam_get_heappagescandesc(TableScanDesc sscan)
{
	HeapScanDesc scan = (HeapScanDesc) sscan;

	return &scan->rs_pagescan;
}

static TableTuple
heapam_fetch_tuple_from_offset(TableScanDesc sscan, BlockNumber blkno, OffsetNumber offset)
{
	HeapScanDesc scan = (HeapScanDesc) sscan;
	Page		dp;
	ItemId		lp;

	dp = (Page) BufferGetPage(scan->rs_scan.rs_cbuf);
	lp = PageGetItemId(dp, offset);
	Assert(ItemIdIsNormal(lp));

	scan->rs_ctup.t_data = (HeapTupleHeader) PageGetItem((Page) dp, lp);
	scan->rs_ctup.t_len = ItemIdGetLength(lp);
	scan->rs_ctup.t_tableOid = scan->rs_scan.rs_rd->rd_id;
	ItemPointerSet(&scan->rs_ctup.t_self, blkno, offset);

	pgstat_count_heap_fetch(scan->rs_scan.rs_rd);

	return &(scan->rs_ctup);
}

Size
heapam_storage_shmem_size()
{
	return SyncScanShmemSize();
}

void
heapam_storage_shmem_init()
{
	return SyncScanShmemInit();
}

Datum
heap_tableam_handler(PG_FUNCTION_ARGS)
{
	TableAmRoutine *amroutine = makeNode(TableAmRoutine);

	amroutine->snapshot_satisfies = HeapTupleSatisfies;

	amroutine->snapshot_satisfiesUpdate = HeapTupleSatisfiesUpdate;
	amroutine->snapshot_satisfiesVacuum = HeapTupleSatisfiesVacuum;

	amroutine->slot_storageam = slot_tableam_handler;

	amroutine->scan_begin = heap_beginscan;
	amroutine->scansetlimits = heap_setscanlimits;
	amroutine->scan_getnext = heap_getnext;
	amroutine->scan_getnextslot = heap_getnextslot;
	amroutine->scan_end = heap_endscan;
	amroutine->scan_rescan = heap_rescan;
	amroutine->scan_update_snapshot = heap_update_snapshot;
	amroutine->hot_search_buffer = heap_hot_search_buffer;
	amroutine->scan_fetch_tuple_from_offset = heapam_fetch_tuple_from_offset;

	/*
	 * The following routine needs to be provided when the storage support
	 * parallel sequential scan
	 */
	amroutine->scan_get_parallelheapscandesc = heapam_get_parallelheapscandesc;

	/*
	 * The following routine needs to be provided when the storage support
	 * BitmapHeap and Sample Scans
	 */
	amroutine->scan_get_heappagescandesc = heapam_get_heappagescandesc;
	amroutine->sync_scan_report_location = ss_report_location;

	amroutine->tuple_fetch = heapam_fetch;
	amroutine->tuple_insert = heapam_heap_insert;
	amroutine->tuple_delete = heapam_heap_delete;
	amroutine->tuple_update = heapam_heap_update;
	amroutine->tuple_lock = heapam_lock_tuple;
	amroutine->multi_insert = heap_multi_insert;

	amroutine->get_tuple_data = heapam_get_tuple_data;
	amroutine->tuple_from_datum = heapam_form_tuple_by_datum;
	amroutine->tuple_get_latest_tid = heap_get_latest_tid;
	amroutine->relation_sync = heap_sync;

	amroutine->getbulkinsertstate = GetBulkInsertState;
	amroutine->freebulkinsertstate = FreeBulkInsertState;
	amroutine->releasebulkinsertstate = ReleaseBulkInsertStatePin;

	amroutine->begin_heap_rewrite = begin_heap_rewrite;
	amroutine->end_heap_rewrite = end_heap_rewrite;
	amroutine->rewrite_heap_tuple = rewrite_heap_tuple;
	amroutine->rewrite_heap_dead_tuple = rewrite_heap_dead_tuple;

	PG_RETURN_POINTER(amroutine);
}
