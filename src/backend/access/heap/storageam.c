/*-------------------------------------------------------------------------
 *
 * storageam.c
 *	  storage access method code
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/storageam.c
 *
 *
 * NOTES
 *	  This file contains the storage_ routines which implement
 *	  the POSTGRES storage access method used for all POSTGRES
 *	  relations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/hio.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/storageam.h"
#include "access/storageamapi.h"
#include "access/tuptoaster.h"
#include "access/valid.h"
#include "access/visibilitymap.h"
#include "access/xloginsert.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/rel.h"
#include "utils/tqual.h"


/* ----------------
 *		heap_beginscan_parallel - join a parallel scan
 *
 *		Caller must hold a suitable lock on the correct relation.
 * ----------------
 */
StorageScanDesc
storage_beginscan_parallel(Relation relation, ParallelHeapScanDesc parallel_scan)
{
	Snapshot	snapshot;

	Assert(RelationGetRelid(relation) == parallel_scan->phs_relid);
	snapshot = RestoreSnapshot(parallel_scan->phs_snapshot_data);
	RegisterSnapshot(snapshot);

	return relation->rd_stamroutine->scan_begin(relation, snapshot, 0, NULL, parallel_scan,
								   true, true, true, false, false, true);
}

ParallelHeapScanDesc
storageam_get_parallelheapscandesc(StorageScanDesc sscan)
{
	return sscan->rs_rd->rd_stamroutine->scan_get_parallelheapscandesc(sscan);
}

HeapPageScanDesc
storageam_get_heappagescandesc(StorageScanDesc sscan)
{
	/*
	 * Planner should have already validated whether the current storage
	 * supports Page scans are not? This function will be called only from
	 * Bitmap Heap scan and sample scan
	 */
	Assert (sscan->rs_rd->rd_stamroutine->scan_get_heappagescandesc != NULL);

	return sscan->rs_rd->rd_stamroutine->scan_get_heappagescandesc(sscan);
}

/*
 * heap_setscanlimits - restrict range of a heapscan
 *
 * startBlk is the page to start at
 * numBlks is number of pages to scan (InvalidBlockNumber means "all")
 */
void
storage_setscanlimits(StorageScanDesc sscan, BlockNumber startBlk, BlockNumber numBlks)
{
	sscan->rs_rd->rd_stamroutine->scansetlimits(sscan, startBlk, numBlks);
}


/* ----------------
 *		heap_beginscan	- begin relation scan
 *
 * heap_beginscan is the "standard" case.
 *
 * heap_beginscan_catalog differs in setting up its own temporary snapshot.
 *
 * heap_beginscan_strat offers an extended API that lets the caller control
 * whether a nondefault buffer access strategy can be used, and whether
 * syncscan can be chosen (possibly resulting in the scan not starting from
 * block zero).  Both of these default to TRUE with plain heap_beginscan.
 *
 * heap_beginscan_bm is an alternative entry point for setting up a
 * HeapScanDesc for a bitmap heap scan.  Although that scan technology is
 * really quite unlike a standard seqscan, there is just enough commonality
 * to make it worth using the same data structure.
 *
 * heap_beginscan_sampling is an alternative entry point for setting up a
 * HeapScanDesc for a TABLESAMPLE scan.  As with bitmap scans, it's worth
 * using the same data structure although the behavior is rather different.
 * In addition to the options offered by heap_beginscan_strat, this call
 * also allows control of whether page-mode visibility checking is used.
 * ----------------
 */
StorageScanDesc
storage_beginscan(Relation relation, Snapshot snapshot,
			   int nkeys, ScanKey key)
{
	return relation->rd_stamroutine->scan_begin(relation, snapshot, nkeys, key, NULL,
								   true, true, true, false, false, false);
}

StorageScanDesc
storage_beginscan_catalog(Relation relation, int nkeys, ScanKey key)
{
	Oid			relid = RelationGetRelid(relation);
	Snapshot	snapshot = RegisterSnapshot(GetCatalogSnapshot(relid));

	return relation->rd_stamroutine->scan_begin(relation, snapshot, nkeys, key, NULL,
								   true, true, true, false, false, true);
}

StorageScanDesc
storage_beginscan_strat(Relation relation, Snapshot snapshot,
					 int nkeys, ScanKey key,
					 bool allow_strat, bool allow_sync)
{
	return relation->rd_stamroutine->scan_begin(relation, snapshot, nkeys, key, NULL,
								   allow_strat, allow_sync, true,
								   false, false, false);
}

StorageScanDesc
storage_beginscan_bm(Relation relation, Snapshot snapshot,
				  int nkeys, ScanKey key)
{
	return relation->rd_stamroutine->scan_begin(relation, snapshot, nkeys, key, NULL,
								   false, false, true, true, false, false);
}

StorageScanDesc
storage_beginscan_sampling(Relation relation, Snapshot snapshot,
						int nkeys, ScanKey key,
						bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	return relation->rd_stamroutine->scan_begin(relation, snapshot, nkeys, key, NULL,
								   allow_strat, allow_sync, allow_pagemode,
								   false, true, false);
}

/* ----------------
 *		heap_rescan		- restart a relation scan
 * ----------------
 */
void
storage_rescan(StorageScanDesc scan,
			ScanKey key)
{
	scan->rs_rd->rd_stamroutine->scan_rescan(scan, key, false, false, false, false);
}

/* ----------------
 *		heap_rescan_set_params	- restart a relation scan after changing params
 *
 * This call allows changing the buffer strategy, syncscan, and pagemode
 * options before starting a fresh scan.  Note that although the actual use
 * of syncscan might change (effectively, enabling or disabling reporting),
 * the previously selected startblock will be kept.
 * ----------------
 */
void
storage_rescan_set_params(StorageScanDesc scan, ScanKey key,
					   bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	scan->rs_rd->rd_stamroutine->scan_rescan(scan, key, true,
			allow_strat, allow_sync, (allow_pagemode && IsMVCCSnapshot(scan->rs_snapshot)));
}

/* ----------------
 *		heap_endscan	- end relation scan
 *
 *		See how to integrate with index scans.
 *		Check handling if reldesc caching.
 * ----------------
 */
void
storage_endscan(StorageScanDesc scan)
{
	scan->rs_rd->rd_stamroutine->scan_end(scan);
}


/* ----------------
 *		heap_update_snapshot
 *
 *		Update snapshot info in heap scan descriptor.
 * ----------------
 */
void
storage_update_snapshot(StorageScanDesc scan, Snapshot snapshot)
{
	scan->rs_rd->rd_stamroutine->scan_update_snapshot(scan, snapshot);
}

StorageTuple
storage_getnext(StorageScanDesc sscan, ScanDirection direction)
{
	return sscan->rs_rd->rd_stamroutine->scan_getnext(sscan, direction);
}

TupleTableSlot*
storage_getnextslot(StorageScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	return sscan->rs_rd->rd_stamroutine->scan_getnextslot(sscan, direction, slot);
}

StorageTuple
storage_fetch_tuple_from_offset(StorageScanDesc sscan, BlockNumber blkno, OffsetNumber offset)
{
	return sscan->rs_rd->rd_stamroutine->scan_fetch_tuple_from_offset(sscan, blkno, offset);
}

/*
 *	storage_fetch		- retrieve tuple with given tid
 *
 * On entry, tuple->t_self is the TID to fetch.  We pin the buffer holding
 * the tuple, fill in the remaining fields of *tuple, and check the tuple
 * against the specified snapshot.
 *
 * If successful (tuple found and passes snapshot time qual), then *userbuf
 * is set to the buffer holding the tuple and TRUE is returned.  The caller
 * must unpin the buffer when done with the tuple.
 *
 * If the tuple is not found (ie, item number references a deleted slot),
 * then tuple->t_data is set to NULL and FALSE is returned.
 *
 * If the tuple is found but fails the time qual check, then FALSE is returned
 * but tuple->t_data is left pointing to the tuple.
 *
 * keep_buf determines what is done with the buffer in the FALSE-result cases.
 * When the caller specifies keep_buf = true, we retain the pin on the buffer
 * and return it in *userbuf (so the caller must eventually unpin it); when
 * keep_buf = false, the pin is released and *userbuf is set to InvalidBuffer.
 *
 * stats_relation is the relation to charge the heap_fetch operation against
 * for statistical purposes.  (This could be the heap rel itself, an
 * associated index, or NULL to not count the fetch at all.)
 *
 * heap_fetch does not follow HOT chains: only the exact TID requested will
 * be fetched.
 *
 * It is somewhat inconsistent that we ereport() on invalid block number but
 * return false on invalid item number.  There are a couple of reasons though.
 * One is that the caller can relatively easily check the block number for
 * validity, but cannot check the item number without reading the page
 * himself.  Another is that when we are following a t_ctid link, we can be
 * reasonably confident that the page number is valid (since VACUUM shouldn't
 * truncate off the destination page without having killed the referencing
 * tuple first), but the item number might well not be good.
 */
bool
storage_fetch(Relation relation,
		   ItemPointer tid,
		   Snapshot snapshot,
		   StorageTuple *stuple,
		   Buffer *userbuf,
		   bool keep_buf,
		   Relation stats_relation)
{
	return relation->rd_stamroutine->tuple_fetch(relation, tid, snapshot, stuple,
							userbuf, keep_buf, stats_relation);
}

/*
 *	heap_hot_search_buffer	- search HOT chain for tuple satisfying snapshot
 *
 * On entry, *tid is the TID of a tuple (either a simple tuple, or the root
 * of a HOT chain), and buffer is the buffer holding this tuple.  We search
 * for the first chain member satisfying the given snapshot.  If one is
 * found, we update *tid to reference that tuple's offset number, and
 * return TRUE.  If no match, return FALSE without modifying *tid.
 *
 * heapTuple is a caller-supplied buffer.  When a match is found, we return
 * the tuple here, in addition to updating *tid.  If no match is found, the
 * contents of this buffer on return are undefined.
 *
 * If all_dead is not NULL, we check non-visible tuples to see if they are
 * globally dead; *all_dead is set TRUE if all members of the HOT chain
 * are vacuumable, FALSE if not.
 *
 * Unlike heap_fetch, the caller must already have pin and (at least) share
 * lock on the buffer; it is still pinned/locked at exit.  Also unlike
 * heap_fetch, we do not report any pgstats count; caller may do so if wanted.
 */
bool
storage_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer,
					   Snapshot snapshot, HeapTuple heapTuple,
					   bool *all_dead, bool first_call)
{
	return relation->rd_stamroutine->hot_search_buffer(tid, relation, buffer,
						snapshot, heapTuple, all_dead, first_call);
}

/*
 *	heap_hot_search		- search HOT chain for tuple satisfying snapshot
 *
 * This has the same API as heap_hot_search_buffer, except that the caller
 * does not provide the buffer containing the page, rather we access it
 * locally.
 */
bool
storage_hot_search(ItemPointer tid, Relation relation, Snapshot snapshot,
				bool *all_dead)
{
	bool		result;
	Buffer		buffer;
	HeapTupleData heapTuple;

	buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	result = relation->rd_stamroutine->hot_search_buffer(tid, relation, buffer,
						snapshot, &heapTuple, all_dead, true);
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buffer);
	return result;
}

/*
 * heap_freeze_tuple
 *		Freeze tuple in place, without WAL logging.
 *
 * Useful for callers like CLUSTER that perform their own WAL logging.
 */
bool
storage_freeze_tuple(Relation rel, HeapTupleHeader tuple, TransactionId cutoff_xid,
				  TransactionId cutoff_multi)
{
	return rel->rd_stamroutine->tuple_freeze(tuple, cutoff_xid, cutoff_multi);
}


/*
 *	storage_lock_tuple - lock a tuple in shared or exclusive mode
 *
 * Note that this acquires a buffer pin, which the caller must release.
 *
 * Input parameters:
 *	relation: relation containing tuple (caller must hold suitable lock)
 *	tuple->t_self: TID of tuple to lock (rest of struct need not be valid)
 *	cid: current command ID (used for visibility test, and stored into
 *		tuple's cmax if lock is successful)
 *	mode: indicates if shared or exclusive tuple lock is desired
 *	wait_policy: what to do if tuple lock is not available
 *	follow_updates: if true, follow the update chain to also lock descendant
 *		tuples.
 *
 * Output parameters:
 *	*tuple: all fields filled in
 *	*buffer: set to buffer holding tuple (pinned but not locked at exit)
 *	*hufd: filled in failure cases (see below)
 *
 * Function result may be:
 *	HeapTupleMayBeUpdated: lock was successfully acquired
 *	HeapTupleInvisible: lock failed because tuple was never visible to us
 *	HeapTupleSelfUpdated: lock failed because tuple updated by self
 *	HeapTupleUpdated: lock failed because tuple updated by other xact
 *	HeapTupleWouldBlock: lock couldn't be acquired and wait_policy is skip
 *
 * In the failure cases other than HeapTupleInvisible, the routine fills
 * *hufd with the tuple's t_ctid, t_xmax (resolving a possible MultiXact,
 * if necessary), and t_cmax (the last only for HeapTupleSelfUpdated,
 * since we cannot obtain cmax from a combocid generated by another
 * transaction).
 * See comments for struct HeapUpdateFailureData for additional info.
 *
 * See README.tuplock for a thorough explanation of this mechanism.
 */
HTSU_Result
storage_lock_tuple(Relation relation, ItemPointer tid, StorageTuple *stuple,
				CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy,
				bool follow_updates, Buffer *buffer, HeapUpdateFailureData *hufd)
{
	return relation->rd_stamroutine->tuple_lock(relation, tid, stuple,
								cid, mode, wait_policy,
								follow_updates, buffer, hufd);
}

/*
 * Insert a tuple from a slot into storage AM routine
 */
Oid
storage_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
				   int options, BulkInsertState bistate)
{
	return relation->rd_stamroutine->tuple_insert(relation, slot, cid,
							options, bistate);
}

/*
 * Delete a tuple from tid using storage AM routine
 */
HTSU_Result
storage_delete(Relation relation, ItemPointer tid, CommandId cid,
				   Snapshot crosscheck, bool wait,
				   HeapUpdateFailureData *hufd)
{
	return relation->rd_stamroutine->tuple_delete(relation, tid, cid,
									crosscheck, wait, hufd);
}

/*
 * update a tuple from tid using storage AM routine
 */
HTSU_Result
storage_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
				   CommandId cid, Snapshot crosscheck, bool wait,
				   HeapUpdateFailureData *hufd, LockTupleMode *lockmode)
{
	return relation->rd_stamroutine->tuple_update(relation, otid, slot, cid,
							crosscheck, wait, hufd, lockmode);
}


/*
 *	storage_multi_insert	- insert multiple tuple into a storage
 *
 * This is like heap_insert(), but inserts multiple tuples in one operation.
 * That's faster than calling heap_insert() in a loop, because when multiple
 * tuples can be inserted on a single page, we can write just a single WAL
 * record covering all of them, and only need to lock/unlock the page once.
 *
 * Note: this leaks memory into the current memory context. You can create a
 * temporary context before calling this, if that's a problem.
 */
void
storage_multi_insert(Relation relation, HeapTuple *tuples, int ntuples,
				  CommandId cid, int options, BulkInsertState bistate)
{
	relation->rd_stamroutine->multi_insert(relation, tuples, ntuples,
											cid, options, bistate);
}


/*
 *	storage_finish_speculative - mark speculative insertion as successful
 *
 * To successfully finish a speculative insertion we have to clear speculative
 * token from tuple.  To do so the t_ctid field, which will contain a
 * speculative token value, is modified in place to point to the tuple itself,
 * which is characteristic of a newly inserted ordinary tuple.
 *
 * NB: It is not ok to commit without either finishing or aborting a
 * speculative insertion.  We could treat speculative tuples of committed
 * transactions implicitly as completed, but then we would have to be prepared
 * to deal with speculative tokens on committed tuples.  That wouldn't be
 * difficult - no-one looks at the ctid field of a tuple with invalid xmax -
 * but clearing the token at completion isn't very expensive either.
 * An explicit confirmation WAL record also makes logical decoding simpler.
 */
void
storage_finish_speculative(Relation relation, TupleTableSlot *slot)
{
	relation->rd_stamroutine->speculative_finish(relation, slot);
}

/*
 *	storage_abort_speculative - kill a speculatively inserted tuple
 *
 * Marks a tuple that was speculatively inserted in the same command as dead,
 * by setting its xmin as invalid.  That makes it immediately appear as dead
 * to all transactions, including our own.  In particular, it makes
 * HeapTupleSatisfiesDirty() regard the tuple as dead, so that another backend
 * inserting a duplicate key value won't unnecessarily wait for our whole
 * transaction to finish (it'll just wait for our speculative insertion to
 * finish).
 *
 * Killing the tuple prevents "unprincipled deadlocks", which are deadlocks
 * that arise due to a mutual dependency that is not user visible.  By
 * definition, unprincipled deadlocks cannot be prevented by the user
 * reordering lock acquisition in client code, because the implementation level
 * lock acquisitions are not under the user's direct control.  If speculative
 * inserters did not take this precaution, then under high concurrency they
 * could deadlock with each other, which would not be acceptable.
 *
 * This is somewhat redundant with heap_delete, but we prefer to have a
 * dedicated routine with stripped down requirements.  Note that this is also
 * used to delete the TOAST tuples created during speculative insertion.
 *
 * This routine does not affect logical decoding as it only looks at
 * confirmation records.
 */
void
storage_abort_speculative(Relation relation, TupleTableSlot *slot)
{
	relation->rd_stamroutine->speculative_abort(relation, slot);
}

tuple_data
storage_tuple_get_data(Relation relation, StorageTuple tuple, tuple_data_flags flags)
{
	return relation->rd_stamroutine->get_tuple_data(tuple, flags);
}

bool
storage_tuple_is_heaponly(Relation relation, StorageTuple tuple)
{
	return relation->rd_stamroutine->tuple_is_heaponly(tuple);
}

StorageTuple
storage_tuple_by_datum(Relation relation, Datum data, Oid tableoid)
{
	if (relation)
		return relation->rd_stamroutine->tuple_from_datum(data, tableoid);
	else
		return heap_form_tuple_by_datum(data, tableoid);
}

void
storage_get_latest_tid(Relation relation,
					Snapshot snapshot,
					ItemPointer tid)
{
	relation->rd_stamroutine->tuple_get_latest_tid(relation, snapshot, tid);
}

/*
 *	storage_sync		- sync a heap, for use when no WAL has been written
 *
 * This forces the heap contents (including TOAST heap if any) down to disk.
 * If we skipped using WAL, and WAL is otherwise needed, we must force the
 * relation down to disk before it's safe to commit the transaction.  This
 * requires writing out any dirty buffers and then doing a forced fsync.
 *
 * Indexes are not touched.  (Currently, index operations associated with
 * the commands that use this are WAL-logged and so do not need fsync.
 * That behavior might change someday, but in any case it's likely that
 * any fsync decisions required would be per-index and hence not appropriate
 * to be done here.)
 */
void
storage_sync(Relation rel)
{
	rel->rd_stamroutine->relation_sync(rel);
}
