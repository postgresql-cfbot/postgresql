/*-------------------------------------------------------------------------
 *
 * indexam.c
 *	  general index access method routines
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/index/indexam.c
 *
 * INTERFACE ROUTINES
 *		index_open		- open an index relation by relation OID
 *		index_close		- close an index relation
 *		index_beginscan - start a scan of an index with amgettuple
 *		index_beginscan_bitmap - start a scan of an index with amgetbitmap
 *		index_rescan	- restart a scan of an index
 *		index_endscan	- end a scan
 *		index_insert	- insert an index tuple into a relation
 *		index_markpos	- mark a scan position
 *		index_restrpos	- restore a scan position
 *		index_parallelscan_estimate - estimate shared memory for parallel scan
 *		index_parallelscan_initialize - initialize parallel scan
 *		index_parallelrescan  - (re)start a parallel scan of an index
 *		index_beginscan_parallel - join parallel index scan
 *		index_getnext_tid	- get the next TID from a scan
 *		index_fetch_heap		- get the scan's next heap tuple
 *		index_getnext_slot	- get the next tuple from a scan
 *		index_getbitmap - get all tuples from a scan
 *		index_bulk_delete	- bulk deletion of index tuples
 *		index_vacuum_cleanup	- post-deletion cleanup of an index
 *		index_can_return	- does index support index-only scans?
 *		index_getprocid - get a support procedure OID
 *		index_getprocinfo - get a support procedure's lookup info
 *
 * NOTES
 *		This file contains the index_ routines which used
 *		to be a scattered collection of stuff in access/genam.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/amapi.h"
#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "catalog/index.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "common/hashfn.h"
#include "nodes/makefuncs.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"


/* ----------------------------------------------------------------
 *					macros used in index_ routines
 *
 * Note: the ReindexIsProcessingIndex() check in RELATION_CHECKS is there
 * to check that we don't try to scan or do retail insertions into an index
 * that is currently being rebuilt or pending rebuild.  This helps to catch
 * things that don't work when reindexing system catalogs.  The assertion
 * doesn't prevent the actual rebuild because we don't use RELATION_CHECKS
 * when calling the index AM's ambuild routine, and there is no reason for
 * ambuild to call its subsidiary routines through this file.
 * ----------------------------------------------------------------
 */
#define RELATION_CHECKS \
( \
	AssertMacro(RelationIsValid(indexRelation)), \
	AssertMacro(PointerIsValid(indexRelation->rd_indam)), \
	AssertMacro(!ReindexIsProcessingIndex(RelationGetRelid(indexRelation))) \
)

#define SCAN_CHECKS \
( \
	AssertMacro(IndexScanIsValid(scan)), \
	AssertMacro(RelationIsValid(scan->indexRelation)), \
	AssertMacro(PointerIsValid(scan->indexRelation->rd_indam)) \
)

#define CHECK_REL_PROCEDURE(pname) \
do { \
	if (indexRelation->rd_indam->pname == NULL) \
		elog(ERROR, "function \"%s\" is not defined for index \"%s\"", \
			 CppAsString(pname), RelationGetRelationName(indexRelation)); \
} while(0)

#define CHECK_SCAN_PROCEDURE(pname) \
do { \
	if (scan->indexRelation->rd_indam->pname == NULL) \
		elog(ERROR, "function \"%s\" is not defined for index \"%s\"", \
			 CppAsString(pname), RelationGetRelationName(scan->indexRelation)); \
} while(0)

static IndexScanDesc index_beginscan_internal(Relation indexRelation,
											  int nkeys, int norderbys, Snapshot snapshot,
											  ParallelIndexScanDesc pscan, bool temp_snap,
											  int prefetch_target, int prefetch_reset);

static void index_prefetch(IndexScanDesc scan, ItemPointer tid);


/* ----------------------------------------------------------------
 *				   index_ interface functions
 * ----------------------------------------------------------------
 */

/* ----------------
 *		index_open - open an index relation by relation OID
 *
 *		If lockmode is not "NoLock", the specified kind of lock is
 *		obtained on the index.  (Generally, NoLock should only be
 *		used if the caller knows it has some appropriate lock on the
 *		index already.)
 *
 *		An error is raised if the index does not exist.
 *
 *		This is a convenience routine adapted for indexscan use.
 *		Some callers may prefer to use relation_open directly.
 * ----------------
 */
Relation
index_open(Oid relationId, LOCKMODE lockmode)
{
	Relation	r;

	r = relation_open(relationId, lockmode);

	if (r->rd_rel->relkind != RELKIND_INDEX &&
		r->rd_rel->relkind != RELKIND_PARTITIONED_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index",
						RelationGetRelationName(r))));

	return r;
}

/* ----------------
 *		index_close - close an index relation
 *
 *		If lockmode is not "NoLock", we then release the specified lock.
 *
 *		Note that it is often sensible to hold a lock beyond index_close;
 *		in that case, the lock is released automatically at xact end.
 * ----------------
 */
void
index_close(Relation relation, LOCKMODE lockmode)
{
	LockRelId	relid = relation->rd_lockInfo.lockRelId;

	Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

	/* The relcache does the real work... */
	RelationClose(relation);

	if (lockmode != NoLock)
		UnlockRelationId(&relid, lockmode);
}

/* ----------------
 *		index_insert - insert an index tuple into a relation
 * ----------------
 */
bool
index_insert(Relation indexRelation,
			 Datum *values,
			 bool *isnull,
			 ItemPointer heap_t_ctid,
			 Relation heapRelation,
			 IndexUniqueCheck checkUnique,
			 bool indexUnchanged,
			 IndexInfo *indexInfo)
{
	RELATION_CHECKS;
	CHECK_REL_PROCEDURE(aminsert);

	if (!(indexRelation->rd_indam->ampredlocks))
		CheckForSerializableConflictIn(indexRelation,
									   (ItemPointer) NULL,
									   InvalidBlockNumber);

	return indexRelation->rd_indam->aminsert(indexRelation, values, isnull,
											 heap_t_ctid, heapRelation,
											 checkUnique, indexUnchanged,
											 indexInfo);
}

/*
 * index_beginscan - start a scan of an index with amgettuple
 *
 * Caller must be holding suitable locks on the heap and the index.
 *
 * prefetch_target determines if prefetching is requested for this index scan.
 * We need to be able to disable this for two reasons. Firstly, we don't want
 * to do prefetching for IOS (where we hope most of the heap pages won't be
 * really needed. Secondly, we must prevent infinite loop when determining
 * prefetch value for the tablespace - the get_tablespace_io_concurrency()
 * does an index scan internally, which would result in infinite loop. So we
 * simply disable prefetching in systable_beginscan().
 *
 * XXX Maybe we should do prefetching even for catalogs, but then disable it
 * when accessing TableSpaceRelationId. We still need the ability to disable
 * this and catalogs are expected to be tiny, so prefetching is unlikely to
 * make a difference.
 *
 * XXX The second reason doesn't really apply after effective_io_concurrency
 * lookup moved to caller of index_beginscan.
 */
IndexScanDesc
index_beginscan(Relation heapRelation,
				Relation indexRelation,
				Snapshot snapshot,
				int nkeys, int norderbys,
				int prefetch_target, int prefetch_reset)
{
	IndexScanDesc scan;

	Assert(snapshot != InvalidSnapshot);

	scan = index_beginscan_internal(indexRelation, nkeys, norderbys, snapshot, NULL, false,
									prefetch_target, prefetch_reset);

	/*
	 * Save additional parameters into the scandesc.  Everything else was set
	 * up by RelationGetIndexScan.
	 */
	scan->heapRelation = heapRelation;
	scan->xs_snapshot = snapshot;

	/* prepare to fetch index matches from table */
	scan->xs_heapfetch = table_index_fetch_begin(heapRelation);

	return scan;
}

/*
 * index_beginscan_bitmap - start a scan of an index with amgetbitmap
 *
 * As above, caller had better be holding some lock on the parent heap
 * relation, even though it's not explicitly mentioned here.
 */
IndexScanDesc
index_beginscan_bitmap(Relation indexRelation,
					   Snapshot snapshot,
					   int nkeys)
{
	IndexScanDesc scan;

	Assert(snapshot != InvalidSnapshot);

	scan = index_beginscan_internal(indexRelation, nkeys, 0, snapshot, NULL, false,
									0, 0); /* no prefetch */

	/*
	 * Save additional parameters into the scandesc.  Everything else was set
	 * up by RelationGetIndexScan.
	 */
	scan->xs_snapshot = snapshot;

	return scan;
}

/*
 * index_beginscan_internal --- common code for index_beginscan variants
 */
static IndexScanDesc
index_beginscan_internal(Relation indexRelation,
						 int nkeys, int norderbys, Snapshot snapshot,
						 ParallelIndexScanDesc pscan, bool temp_snap,
						 int prefetch_target, int prefetch_reset)
{
	IndexScanDesc scan;

	RELATION_CHECKS;
	CHECK_REL_PROCEDURE(ambeginscan);

	if (!(indexRelation->rd_indam->ampredlocks))
		PredicateLockRelation(indexRelation, snapshot);

	/*
	 * We hold a reference count to the relcache entry throughout the scan.
	 */
	RelationIncrementReferenceCount(indexRelation);

	/*
	 * Tell the AM to open a scan.
	 */
	scan = indexRelation->rd_indam->ambeginscan(indexRelation, nkeys, norderbys);
	/* Initialize information for parallel scan. */
	scan->parallel_scan = pscan;
	scan->xs_temp_snap = temp_snap;

	/* with prefetching enabled, initialize the necessary state */
	if (prefetch_target > 0)
	{
		IndexPrefetch prefetcher = palloc0(sizeof(IndexPrefetchData));

		prefetcher->queueIndex = 0;
		prefetcher->queueStart = 0;
		prefetcher->queueEnd = 0;

		prefetcher->prefetchTarget = 0;
		prefetcher->prefetchMaxTarget = prefetch_target;
		prefetcher->prefetchReset = prefetch_reset;

		scan->xs_prefetch = prefetcher;
	}

	return scan;
}

/* ----------------
 *		index_rescan  - (re)start a scan of an index
 *
 * During a restart, the caller may specify a new set of scankeys and/or
 * orderbykeys; but the number of keys cannot differ from what index_beginscan
 * was told.  (Later we might relax that to "must not exceed", but currently
 * the index AMs tend to assume that scan->numberOfKeys is what to believe.)
 * To restart the scan without changing keys, pass NULL for the key arrays.
 * (Of course, keys *must* be passed on the first call, unless
 * scan->numberOfKeys is zero.)
 * ----------------
 */
void
index_rescan(IndexScanDesc scan,
			 ScanKey keys, int nkeys,
			 ScanKey orderbys, int norderbys)
{
	SCAN_CHECKS;
	CHECK_SCAN_PROCEDURE(amrescan);

	Assert(nkeys == scan->numberOfKeys);
	Assert(norderbys == scan->numberOfOrderBys);

	/* Release resources (like buffer pins) from table accesses */
	if (scan->xs_heapfetch)
		table_index_fetch_reset(scan->xs_heapfetch);

	scan->kill_prior_tuple = false; /* for safety */
	scan->xs_heap_continue = false;

	scan->indexRelation->rd_indam->amrescan(scan, keys, nkeys,
											orderbys, norderbys);

	/* If we're prefetching for this index, maybe reset some of the state. */
	if (scan->xs_prefetch != NULL)
	{
		IndexPrefetch prefetcher = scan->xs_prefetch;

		prefetcher->queueStart = 0;
		prefetcher->queueEnd = 0;
		prefetcher->queueIndex = 0;
		prefetcher->prefetchDone = false;
	
		prefetcher->prefetchTarget = Min(prefetcher->prefetchTarget,
										 prefetcher->prefetchReset);
	}
}

/* ----------------
 *		index_endscan - end a scan
 * ----------------
 */
void
index_endscan(IndexScanDesc scan)
{
	SCAN_CHECKS;
	CHECK_SCAN_PROCEDURE(amendscan);

	/* Release resources (like buffer pins) from table accesses */
	if (scan->xs_heapfetch)
	{
		table_index_fetch_end(scan->xs_heapfetch);
		scan->xs_heapfetch = NULL;
	}

	/* End the AM's scan */
	scan->indexRelation->rd_indam->amendscan(scan);

	/* Release index refcount acquired by index_beginscan */
	RelationDecrementReferenceCount(scan->indexRelation);

	if (scan->xs_temp_snap)
		UnregisterSnapshot(scan->xs_snapshot);

	/* If prefetching enabled, log prefetch stats. */
	if (scan->xs_prefetch)
	{
		IndexPrefetch prefetch = scan->xs_prefetch;

		elog(LOG, "index prefetch stats: requests " UINT64_FORMAT " prefetches " UINT64_FORMAT " (%f) skip cached " UINT64_FORMAT " sequential " UINT64_FORMAT,
			 prefetch->countAll,
			 prefetch->countPrefetch,
			 prefetch->countPrefetch * 100.0 / prefetch->countAll,
			 prefetch->countSkipCached,
			 prefetch->countSkipSequential);
	}

	/* Release the scan data structure itself */
	IndexScanEnd(scan);
}

/* ----------------
 *		index_markpos  - mark a scan position
 * ----------------
 */
void
index_markpos(IndexScanDesc scan)
{
	SCAN_CHECKS;
	CHECK_SCAN_PROCEDURE(ammarkpos);

	scan->indexRelation->rd_indam->ammarkpos(scan);
}

/* ----------------
 *		index_restrpos	- restore a scan position
 *
 * NOTE: this only restores the internal scan state of the index AM.  See
 * comments for ExecRestrPos().
 *
 * NOTE: For heap, in the presence of HOT chains, mark/restore only works
 * correctly if the scan's snapshot is MVCC-safe; that ensures that there's at
 * most one returnable tuple in each HOT chain, and so restoring the prior
 * state at the granularity of the index AM is sufficient.  Since the only
 * current user of mark/restore functionality is nodeMergejoin.c, this
 * effectively means that merge-join plans only work for MVCC snapshots.  This
 * could be fixed if necessary, but for now it seems unimportant.
 * ----------------
 */
void
index_restrpos(IndexScanDesc scan)
{
	Assert(IsMVCCSnapshot(scan->xs_snapshot));

	SCAN_CHECKS;
	CHECK_SCAN_PROCEDURE(amrestrpos);

	/* release resources (like buffer pins) from table accesses */
	if (scan->xs_heapfetch)
		table_index_fetch_reset(scan->xs_heapfetch);

	scan->kill_prior_tuple = false; /* for safety */
	scan->xs_heap_continue = false;

	scan->indexRelation->rd_indam->amrestrpos(scan);
}

/*
 * index_parallelscan_estimate - estimate shared memory for parallel scan
 *
 * Currently, we don't pass any information to the AM-specific estimator,
 * so it can probably only return a constant.  In the future, we might need
 * to pass more information.
 */
Size
index_parallelscan_estimate(Relation indexRelation, Snapshot snapshot)
{
	Size		nbytes;

	Assert(snapshot != InvalidSnapshot);

	RELATION_CHECKS;

	nbytes = offsetof(ParallelIndexScanDescData, ps_snapshot_data);
	nbytes = add_size(nbytes, EstimateSnapshotSpace(snapshot));
	nbytes = MAXALIGN(nbytes);

	/*
	 * If amestimateparallelscan is not provided, assume there is no
	 * AM-specific data needed.  (It's hard to believe that could work, but
	 * it's easy enough to cater to it here.)
	 */
	if (indexRelation->rd_indam->amestimateparallelscan != NULL)
		nbytes = add_size(nbytes,
						  indexRelation->rd_indam->amestimateparallelscan());

	return nbytes;
}

/*
 * index_parallelscan_initialize - initialize parallel scan
 *
 * We initialize both the ParallelIndexScanDesc proper and the AM-specific
 * information which follows it.
 *
 * This function calls access method specific initialization routine to
 * initialize am specific information.  Call this just once in the leader
 * process; then, individual workers attach via index_beginscan_parallel.
 */
void
index_parallelscan_initialize(Relation heapRelation, Relation indexRelation,
							  Snapshot snapshot, ParallelIndexScanDesc target)
{
	Size		offset;

	Assert(snapshot != InvalidSnapshot);

	RELATION_CHECKS;

	offset = add_size(offsetof(ParallelIndexScanDescData, ps_snapshot_data),
					  EstimateSnapshotSpace(snapshot));
	offset = MAXALIGN(offset);

	target->ps_relid = RelationGetRelid(heapRelation);
	target->ps_indexid = RelationGetRelid(indexRelation);
	target->ps_offset = offset;
	SerializeSnapshot(snapshot, target->ps_snapshot_data);

	/* aminitparallelscan is optional; assume no-op if not provided by AM */
	if (indexRelation->rd_indam->aminitparallelscan != NULL)
	{
		void	   *amtarget;

		amtarget = OffsetToPointer(target, offset);
		indexRelation->rd_indam->aminitparallelscan(amtarget);
	}
}

/* ----------------
 *		index_parallelrescan  - (re)start a parallel scan of an index
 * ----------------
 */
void
index_parallelrescan(IndexScanDesc scan)
{
	SCAN_CHECKS;

	if (scan->xs_heapfetch)
		table_index_fetch_reset(scan->xs_heapfetch);

	/* amparallelrescan is optional; assume no-op if not provided by AM */
	if (scan->indexRelation->rd_indam->amparallelrescan != NULL)
		scan->indexRelation->rd_indam->amparallelrescan(scan);
}

/*
 * index_beginscan_parallel - join parallel index scan
 *
 * Caller must be holding suitable locks on the heap and the index.
 *
 * XXX See index_beginscan() for more comments on prefetch_target.
 */
IndexScanDesc
index_beginscan_parallel(Relation heaprel, Relation indexrel, int nkeys,
						 int norderbys, ParallelIndexScanDesc pscan,
						 int prefetch_target, int prefetch_reset)
{
	Snapshot	snapshot;
	IndexScanDesc scan;

	Assert(RelationGetRelid(heaprel) == pscan->ps_relid);
	snapshot = RestoreSnapshot(pscan->ps_snapshot_data);
	RegisterSnapshot(snapshot);
	scan = index_beginscan_internal(indexrel, nkeys, norderbys, snapshot,
									pscan, true, prefetch_target, prefetch_reset);

	/*
	 * Save additional parameters into the scandesc.  Everything else was set
	 * up by index_beginscan_internal.
	 */
	scan->heapRelation = heaprel;
	scan->xs_snapshot = snapshot;

	/* prepare to fetch index matches from table */
	scan->xs_heapfetch = table_index_fetch_begin(heaprel);

	return scan;
}

/* ----------------
 * index_getnext_tid - get the next TID from a scan
 *
 * The result is the next TID satisfying the scan keys,
 * or NULL if no more matching tuples exist.
 * ----------------
 */
ItemPointer
index_getnext_tid(IndexScanDesc scan, ScanDirection direction)
{
	bool		found;

	SCAN_CHECKS;
	CHECK_SCAN_PROCEDURE(amgettuple);

	/* XXX: we should assert that a snapshot is pushed or registered */
	Assert(TransactionIdIsValid(RecentXmin));

	/*
	 * The AM's amgettuple proc finds the next index entry matching the scan
	 * keys, and puts the TID into scan->xs_heaptid.  It should also set
	 * scan->xs_recheck and possibly scan->xs_itup/scan->xs_hitup, though we
	 * pay no attention to those fields here.
	 */
	found = scan->indexRelation->rd_indam->amgettuple(scan, direction);

	/* Reset kill flag immediately for safety */
	scan->kill_prior_tuple = false;
	scan->xs_heap_continue = false;

	/* If we're out of index entries, we're done */
	if (!found)
	{
		/* release resources (like buffer pins) from table accesses */
		if (scan->xs_heapfetch)
			table_index_fetch_reset(scan->xs_heapfetch);

		return NULL;
	}
	Assert(ItemPointerIsValid(&scan->xs_heaptid));

	pgstat_count_index_tuples(scan->indexRelation, 1);

	/* Return the TID of the tuple we found. */
	return &scan->xs_heaptid;
}

/* ----------------
 *		index_fetch_heap - get the scan's next heap tuple
 *
 * The result is a visible heap tuple associated with the index TID most
 * recently fetched by index_getnext_tid, or NULL if no more matching tuples
 * exist.  (There can be more than one matching tuple because of HOT chains,
 * although when using an MVCC snapshot it should be impossible for more than
 * one such tuple to exist.)
 *
 * On success, the buffer containing the heap tup is pinned (the pin will be
 * dropped in a future index_getnext_tid, index_fetch_heap or index_endscan
 * call).
 *
 * Note: caller must check scan->xs_recheck, and perform rechecking of the
 * scan keys if required.  We do not do that here because we don't have
 * enough information to do it efficiently in the general case.
 * ----------------
 */
bool
index_fetch_heap(IndexScanDesc scan, TupleTableSlot *slot)
{
	bool		all_dead = false;
	bool		found;

	found = table_index_fetch_tuple(scan->xs_heapfetch, &scan->xs_heaptid,
									scan->xs_snapshot, slot,
									&scan->xs_heap_continue, &all_dead);

	if (found)
		pgstat_count_heap_fetch(scan->indexRelation);

	/*
	 * If we scanned a whole HOT chain and found only dead tuples, tell index
	 * AM to kill its entry for that TID (this will take effect in the next
	 * amgettuple call, in index_getnext_tid).  We do not do this when in
	 * recovery because it may violate MVCC to do so.  See comments in
	 * RelationGetIndexScan().
	 */
	if (!scan->xactStartedInRecovery)
		scan->kill_prior_tuple = all_dead;

	return found;
}

/* ----------------
 *		index_getnext_slot - get the next tuple from a scan
 *
 * The result is true if a tuple satisfying the scan keys and the snapshot was
 * found, false otherwise.  The tuple is stored in the specified slot.
 *
 * On success, resources (like buffer pins) are likely to be held, and will be
 * dropped by a future index_getnext_tid, index_fetch_heap or index_endscan
 * call).
 *
 * Note: caller must check scan->xs_recheck, and perform rechecking of the
 * scan keys if required.  We do not do that here because we don't have
 * enough information to do it efficiently in the general case.
 * ----------------
 */
bool
index_getnext_slot(IndexScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
	IndexPrefetch	prefetch = scan->xs_prefetch;

	for (;;)
	{
		/* with prefetching enabled, accumulate enough TIDs into the prefetch */
		if (PREFETCH_ACTIVE(prefetch))
		{
			/* 
			 * incrementally ramp up prefetch distance
			 *
			 * XXX Intentionally done as first, so that with prefetching there's
			 * always at least one item in the queue.
			 */
			prefetch->prefetchTarget = Min(prefetch->prefetchTarget + 1,
										prefetch->prefetchMaxTarget);

			/*
			 * get more TID while there is empty space in the queue (considering
			 * current prefetch target
			 */
			while (!PREFETCH_FULL(prefetch))
			{
				ItemPointer tid;

				/* Time to fetch the next TID from the index */
				tid = index_getnext_tid(scan, direction);

				/* If we're out of index entries, we're done */
				if (tid == NULL)
				{
					prefetch->prefetchDone = true;
					break;
				}

				Assert(ItemPointerEquals(tid, &scan->xs_heaptid));

				prefetch->queueItems[PREFETCH_QUEUE_INDEX(prefetch->queueEnd)] = *tid;
				prefetch->queueEnd++;

				index_prefetch(scan, tid);
			}
		}

		if (!scan->xs_heap_continue)
		{
			if (PREFETCH_ENABLED(prefetch))
			{
				/* prefetching enabled, but reached the end and queue empty */
				if (PREFETCH_DONE(prefetch))
					break;

				scan->xs_heaptid = prefetch->queueItems[PREFETCH_QUEUE_INDEX(prefetch->queueIndex)];
				prefetch->queueIndex++;
			}
			else	/* not prefetching, just do the regular work  */
			{
				ItemPointer tid;

				/* Time to fetch the next TID from the index */
				tid = index_getnext_tid(scan, direction);

				/* If we're out of index entries, we're done */
				if (tid == NULL)
					break;

				Assert(ItemPointerEquals(tid, &scan->xs_heaptid));
			}

		}

		/*
		 * Fetch the next (or only) visible heap tuple for this index entry.
		 * If we don't find anything, loop around and grab the next TID from
		 * the index.
		 */
		Assert(ItemPointerIsValid(&scan->xs_heaptid));
		if (index_fetch_heap(scan, slot))
			return true;
	}

	return false;
}

/* ----------------
 *		index_getbitmap - get all tuples at once from an index scan
 *
 * Adds the TIDs of all heap tuples satisfying the scan keys to a bitmap.
 * Since there's no interlock between the index scan and the eventual heap
 * access, this is only safe to use with MVCC-based snapshots: the heap
 * item slot could have been replaced by a newer tuple by the time we get
 * to it.
 *
 * Returns the number of matching tuples found.  (Note: this might be only
 * approximate, so it should only be used for statistical purposes.)
 * ----------------
 */
int64
index_getbitmap(IndexScanDesc scan, TIDBitmap *bitmap)
{
	int64		ntids;

	SCAN_CHECKS;
	CHECK_SCAN_PROCEDURE(amgetbitmap);

	/* just make sure this is false... */
	scan->kill_prior_tuple = false;

	/*
	 * have the am's getbitmap proc do all the work.
	 */
	ntids = scan->indexRelation->rd_indam->amgetbitmap(scan, bitmap);

	pgstat_count_index_tuples(scan->indexRelation, ntids);

	return ntids;
}

/* ----------------
 *		index_bulk_delete - do mass deletion of index entries
 *
 *		callback routine tells whether a given main-heap tuple is
 *		to be deleted
 *
 *		return value is an optional palloc'd struct of statistics
 * ----------------
 */
IndexBulkDeleteResult *
index_bulk_delete(IndexVacuumInfo *info,
				  IndexBulkDeleteResult *istat,
				  IndexBulkDeleteCallback callback,
				  void *callback_state)
{
	Relation	indexRelation = info->index;

	RELATION_CHECKS;
	CHECK_REL_PROCEDURE(ambulkdelete);

	return indexRelation->rd_indam->ambulkdelete(info, istat,
												 callback, callback_state);
}

/* ----------------
 *		index_vacuum_cleanup - do post-deletion cleanup of an index
 *
 *		return value is an optional palloc'd struct of statistics
 * ----------------
 */
IndexBulkDeleteResult *
index_vacuum_cleanup(IndexVacuumInfo *info,
					 IndexBulkDeleteResult *istat)
{
	Relation	indexRelation = info->index;

	RELATION_CHECKS;
	CHECK_REL_PROCEDURE(amvacuumcleanup);

	return indexRelation->rd_indam->amvacuumcleanup(info, istat);
}

/* ----------------
 *		index_can_return
 *
 *		Does the index access method support index-only scans for the given
 *		column?
 * ----------------
 */
bool
index_can_return(Relation indexRelation, int attno)
{
	RELATION_CHECKS;

	/* amcanreturn is optional; assume false if not provided by AM */
	if (indexRelation->rd_indam->amcanreturn == NULL)
		return false;

	return indexRelation->rd_indam->amcanreturn(indexRelation, attno);
}

/* ----------------
 *		index_getprocid
 *
 *		Index access methods typically require support routines that are
 *		not directly the implementation of any WHERE-clause query operator
 *		and so cannot be kept in pg_amop.  Instead, such routines are kept
 *		in pg_amproc.  These registered procedure OIDs are assigned numbers
 *		according to a convention established by the access method.
 *		The general index code doesn't know anything about the routines
 *		involved; it just builds an ordered list of them for
 *		each attribute on which an index is defined.
 *
 *		As of Postgres 8.3, support routines within an operator family
 *		are further subdivided by the "left type" and "right type" of the
 *		query operator(s) that they support.  The "default" functions for a
 *		particular indexed attribute are those with both types equal to
 *		the index opclass' opcintype (note that this is subtly different
 *		from the indexed attribute's own type: it may be a binary-compatible
 *		type instead).  Only the default functions are stored in relcache
 *		entries --- access methods can use the syscache to look up non-default
 *		functions.
 *
 *		This routine returns the requested default procedure OID for a
 *		particular indexed attribute.
 * ----------------
 */
RegProcedure
index_getprocid(Relation irel,
				AttrNumber attnum,
				uint16 procnum)
{
	RegProcedure *loc;
	int			nproc;
	int			procindex;

	nproc = irel->rd_indam->amsupport;

	Assert(procnum > 0 && procnum <= (uint16) nproc);

	procindex = (nproc * (attnum - 1)) + (procnum - 1);

	loc = irel->rd_support;

	Assert(loc != NULL);

	return loc[procindex];
}

/* ----------------
 *		index_getprocinfo
 *
 *		This routine allows index AMs to keep fmgr lookup info for
 *		support procs in the relcache.  As above, only the "default"
 *		functions for any particular indexed attribute are cached.
 *
 * Note: the return value points into cached data that will be lost during
 * any relcache rebuild!  Therefore, either use the callinfo right away,
 * or save it only after having acquired some type of lock on the index rel.
 * ----------------
 */
FmgrInfo *
index_getprocinfo(Relation irel,
				  AttrNumber attnum,
				  uint16 procnum)
{
	FmgrInfo   *locinfo;
	int			nproc;
	int			optsproc;
	int			procindex;

	nproc = irel->rd_indam->amsupport;
	optsproc = irel->rd_indam->amoptsprocnum;

	Assert(procnum > 0 && procnum <= (uint16) nproc);

	procindex = (nproc * (attnum - 1)) + (procnum - 1);

	locinfo = irel->rd_supportinfo;

	Assert(locinfo != NULL);

	locinfo += procindex;

	/* Initialize the lookup info if first time through */
	if (locinfo->fn_oid == InvalidOid)
	{
		RegProcedure *loc = irel->rd_support;
		RegProcedure procId;

		Assert(loc != NULL);

		procId = loc[procindex];

		/*
		 * Complain if function was not found during IndexSupportInitialize.
		 * This should not happen unless the system tables contain bogus
		 * entries for the index opclass.  (If an AM wants to allow a support
		 * function to be optional, it can use index_getprocid.)
		 */
		if (!RegProcedureIsValid(procId))
			elog(ERROR, "missing support function %d for attribute %d of index \"%s\"",
				 procnum, attnum, RelationGetRelationName(irel));

		fmgr_info_cxt(procId, locinfo, irel->rd_indexcxt);

		if (procnum != optsproc)
		{
			/* Initialize locinfo->fn_expr with opclass options Const */
			bytea	  **attoptions = RelationGetIndexAttOptions(irel, false);
			MemoryContext oldcxt = MemoryContextSwitchTo(irel->rd_indexcxt);

			set_fn_opclass_options(locinfo, attoptions[attnum - 1]);

			MemoryContextSwitchTo(oldcxt);
		}
	}

	return locinfo;
}

/* ----------------
 *		index_store_float8_orderby_distances
 *
 *		Convert AM distance function's results (that can be inexact)
 *		to ORDER BY types and save them into xs_orderbyvals/xs_orderbynulls
 *		for a possible recheck.
 * ----------------
 */
void
index_store_float8_orderby_distances(IndexScanDesc scan, Oid *orderByTypes,
									 IndexOrderByDistance *distances,
									 bool recheckOrderBy)
{
	int			i;

	Assert(distances || !recheckOrderBy);

	scan->xs_recheckorderby = recheckOrderBy;

	for (i = 0; i < scan->numberOfOrderBys; i++)
	{
		if (orderByTypes[i] == FLOAT8OID)
		{
#ifndef USE_FLOAT8_BYVAL
			/* must free any old value to avoid memory leakage */
			if (!scan->xs_orderbynulls[i])
				pfree(DatumGetPointer(scan->xs_orderbyvals[i]));
#endif
			if (distances && !distances[i].isnull)
			{
				scan->xs_orderbyvals[i] = Float8GetDatum(distances[i].value);
				scan->xs_orderbynulls[i] = false;
			}
			else
			{
				scan->xs_orderbyvals[i] = (Datum) 0;
				scan->xs_orderbynulls[i] = true;
			}
		}
		else if (orderByTypes[i] == FLOAT4OID)
		{
			/* convert distance function's result to ORDER BY type */
			if (distances && !distances[i].isnull)
			{
				scan->xs_orderbyvals[i] = Float4GetDatum((float4) distances[i].value);
				scan->xs_orderbynulls[i] = false;
			}
			else
			{
				scan->xs_orderbyvals[i] = (Datum) 0;
				scan->xs_orderbynulls[i] = true;
			}
		}
		else
		{
			/*
			 * If the ordering operator's return value is anything else, we
			 * don't know how to convert the float8 bound calculated by the
			 * distance function to that.  The executor won't actually need
			 * the order by values we return here, if there are no lossy
			 * results, so only insist on converting if the *recheck flag is
			 * set.
			 */
			if (scan->xs_recheckorderby)
				elog(ERROR, "ORDER BY operator must return float8 or float4 if the distance function is lossy");
			scan->xs_orderbynulls[i] = true;
		}
	}
}

/* ----------------
 *      index_opclass_options
 *
 *      Parse opclass-specific options for index column.
 * ----------------
 */
bytea *
index_opclass_options(Relation indrel, AttrNumber attnum, Datum attoptions,
					  bool validate)
{
	int			amoptsprocnum = indrel->rd_indam->amoptsprocnum;
	Oid			procid = InvalidOid;
	FmgrInfo   *procinfo;
	local_relopts relopts;

	/* fetch options support procedure if specified */
	if (amoptsprocnum != 0)
		procid = index_getprocid(indrel, attnum, amoptsprocnum);

	if (!OidIsValid(procid))
	{
		Oid			opclass;
		Datum		indclassDatum;
		oidvector  *indclass;

		if (!DatumGetPointer(attoptions))
			return NULL;		/* ok, no options, no procedure */

		/*
		 * Report an error if the opclass's options-parsing procedure does not
		 * exist but the opclass options are specified.
		 */
		indclassDatum = SysCacheGetAttrNotNull(INDEXRELID, indrel->rd_indextuple,
											   Anum_pg_index_indclass);
		indclass = (oidvector *) DatumGetPointer(indclassDatum);
		opclass = indclass->values[attnum - 1];

		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("operator class %s has no options",
						generate_opclass_name(opclass))));
	}

	init_local_reloptions(&relopts, 0);

	procinfo = index_getprocinfo(indrel, attnum, amoptsprocnum);

	(void) FunctionCall1(procinfo, PointerGetDatum(&relopts));

	return build_local_reloptions(&relopts, attoptions, validate);
}

/*
 * Add the block to the tiny top-level queue (LRU), and check if the block
 * is in a sequential pattern.
 */
static bool
index_prefetch_is_sequential(IndexPrefetch prefetch, BlockNumber block)
{
	int		idx;

	/* If the queue is empty, just store the block and we're done. */
	if (prefetch->blockIndex == 0)
	{
		prefetch->blockItems[PREFETCH_BLOCK_INDEX(prefetch->blockIndex)] = block;
		prefetch->blockIndex++;
		return false;
	}

	/*
	 * Otherwise, check if it's the same as the immediately preceding block (we
	 * don't want to prefetch the same block over and over.)
	 */
	if (prefetch->blockItems[PREFETCH_BLOCK_INDEX(prefetch->blockIndex - 1)] == block)
		return true;

	/* Not the same block, so add it to the queue. */
	prefetch->blockItems[PREFETCH_BLOCK_INDEX(prefetch->blockIndex)] = block;
	prefetch->blockIndex++;

	/* check sequential patter a couple requests back */
	for (int i = 1; i < PREFETCH_SEQ_PATTERN_BLOCKS; i++)
	{
		/* not enough requests to confirm a sequential pattern */
		if (prefetch->blockIndex < i)
			return false;

		/*
		 * index of the already requested buffer (-1 because we already
		 * incremented the index when adding the block to the queue)
		 */
		idx = PREFETCH_BLOCK_INDEX(prefetch->blockIndex - i - 1);

		/*  */
		if (prefetch->blockItems[idx] != (block - i))
			return false;
	}

	return true;
}

/*
 * index_prefetch_add_cache
 *		Add a block to the cache, return true if it was recently prefetched.
 *
 * When checking a block, we need to check if it was recently prefetched,
 * where recently means within PREFETCH_CACHE_SIZE requests. This check
 * needs to be very cheap, even with fairly large caches (hundreds of
 * entries). The cache does not need to be perfect, we can accept false
 * positives/negatives, as long as the rate is reasonably low. We also
 * need to expire entries, so that only "recent" requests are remembered.
 *
 * A queue would allow expiring the requests, but checking if a block was
 * prefetched would be expensive (linear search for longer queues). Another
 * option would be a hash table, but that has issues with expiring entries
 * cheaply (which usually degrades the hash table).
 *
 * So we use a cache that is organized as multiple small LRU caches. Each
 * block is mapped to a particular LRU by hashing (so it's a bit like a
 * hash table), and each LRU is tiny (e.g. 8 entries). The LRU only keeps
 * the most recent requests (for that particular LRU).
 *
 * This allows quick searches and expiration, with false negatives (when
 * a particular LRU has too many collisions).
 *
 * For example, imagine 128 LRU caches, each with 8 entries - that's 1024
 * prefetch request in total.
 *
 * The recency is determined using a prefetch counter, incremented every
 * time we end up prefetching a block. The counter is uint64, so it should
 * not wrap (125 zebibytes, would take ~4 million years at 1GB/s).
 *
 * To check if a block was prefetched recently, we calculate hash(block),
 * and then linearly search if the tiny LRU has entry for the same block
 * and request less than PREFETCH_CACHE_SIZE ago.
 *
 * At the same time, we either update the entry (for the same block) if
 * found, or replace the oldest/empty entry.
 *
 * If the block was not recently prefetched (i.e. we want to prefetch it),
 * we increment the counter.
 */
static bool
index_prefetch_add_cache(IndexPrefetch prefetch, BlockNumber block)
{
	PrefetchCacheEntry *entry;

	/* calculate which LRU to use */
	int			lru = hash_uint32(block) % PREFETCH_LRU_COUNT;

	/* entry to (maybe) use for this block request */
	uint64		oldestRequest = PG_UINT64_MAX;
	int			oldestIndex = -1;

	/*
	 * First add the block to the (tiny) top-level LRU cache and see if it's
	 * part of a sequential pattern. In this case we just ignore the block
	 * and don't prefetch it - we expect read-ahead to do a better job.
	 *
	 * XXX Maybe we should still add the block to the later cache, in case
	 * we happen to access it later? That might help if we first scan a lot
	 * of the table sequentially, and then randomly. Not sure that's very
	 * likely with index access, though.
	 */
	if (index_prefetch_is_sequential(prefetch, block))
	{
		prefetch->countSkipSequential++;
		return true;
	}

	/* see if we already have prefetched this block (linear search of LRU) */
	for (int i = 0; i < PREFETCH_LRU_SIZE; i++)
	{
		entry = &prefetch->prefetchCache[lru * PREFETCH_LRU_SIZE + i];

		/* Is this the oldest prefetch request in this LRU? */
		if (entry->request < oldestRequest)
		{
			oldestRequest = entry->request;
			oldestIndex = i;
		}

		/* Request numbers are positive, so 0 means "unused". */
		if (entry->request == 0)
			continue;

		/* Is this entry for the same block as the current request? */
		if (entry->block == block)
		{
			bool	prefetched;

			/*
			 * Is the old request sufficiently recent? If yes, we treat the
			 * block as already prefetched.
			 *
			 * XXX We do add the cache size to the request in order not to
			 * have issues with uint64 underflows.
			 */
			prefetched = (entry->request + PREFETCH_CACHE_SIZE >= prefetch->prefetchReqNumber);

			/* Update the request number. */
			entry->request = ++prefetch->prefetchReqNumber;

			prefetch->countSkipCached += (prefetched) ? 1 : 0;

			return prefetched;
		}
	}

	/*
	 * We didn't find the block in the LRU, so store it either in an empty
	 * entry, or in the "oldest" prefetch request in this LRU.
	 */
	Assert((oldestIndex >= 0) && (oldestIndex < PREFETCH_LRU_SIZE));

	entry = &prefetch->prefetchCache[lru * PREFETCH_LRU_SIZE + oldestIndex];

	entry->block = block;
	entry->request = ++prefetch->prefetchReqNumber;

	/* not in the prefetch cache */
	return false;
}

/*
 * Do prefetching, and gradually increase the prefetch distance.
 *
 * XXX This is limited to a single index page (because that's where we get
 * currPos.items from). But index tuples are typically very small, so there
 * should be quite a bit of stuff to prefetch (especially with deduplicated
 * indexes, etc.). Does not seem worth reworking the index access to allow
 * more aggressive prefetching, it's best effort.
 *
 * XXX Some ideas how to auto-tune the prefetching, so that unnecessary
 * prefetching does not cause significant regressions (e.g. for nestloop
 * with inner index scan). We could track number of index pages visited
 * and index tuples returned, to calculate avg tuples / page, and then
 * use that to limit prefetching after switching to a new page (instead
 * of just using prefetchMaxTarget, which can get much larger).
 *
 * XXX Obviously, another option is to use the planner estimates - we know
 * how many rows we're expected to fetch (on average, assuming the estimates
 * are reasonably accurate), so why not to use that. And maybe combine it
 * with the auto-tuning based on runtime statistics, described above.
 *
 * XXX The prefetching may interfere with the patch allowing us to evaluate
 * conditions on the index tuple, in which case we may not need the heap
 * tuple. Maybe if there's such filter, we should prefetch only pages that
 * are not all-visible (and the same idea would also work for IOS), but
 * it also makes the indexing a bit "aware" of the visibility stuff (which
 * seems a bit wrong). Also, maybe we should consider the filter selectivity
 * (if the index-only filter is expected to eliminate only few rows, then
 * the vm check is pointless). Maybe this could/should be auto-tuning too,
 * i.e. we could track how many heap tuples were needed after all, and then
 * we would consider this when deciding whether to prefetch all-visible
 * pages or not (matters only for regular index scans, not IOS).
 *
 * XXX Maybe we could/should also prefetch the next index block, e.g. stored
 * in BTScanPosData.nextPage.
 */
static void
index_prefetch(IndexScanDesc scan, ItemPointer tid)
{
	IndexPrefetch	prefetch = scan->xs_prefetch;
	BlockNumber	block;

	/*
	 * No heap relation means bitmap index scan, which does prefetching at
	 * the bitmap heap scan, so no prefetch here (we can't do it anyway,
	 * without the heap)
	 *
	 * XXX But in this case we should have prefetchMaxTarget=0, because in
	 * index_bebinscan_bitmap() we disable prefetching. So maybe we should
	 * just check that.
	 */
	if (!prefetch)
		return;

	/* was it initialized correctly? */
	// Assert(prefetch->prefetchIndex != -1);

	/*
	 * If we got here, prefetching is enabled and it's a node that supports
	 * prefetching (i.e. it can't be a bitmap index scan).
	 */
	Assert(scan->heapRelation);

	prefetch->countAll++;

	block = ItemPointerGetBlockNumber(tid);

	/*
	 * Do not prefetch the same block over and over again,
	 *
	 * This happens e.g. for clustered or naturally correlated indexes
	 * (fkey to a sequence ID). It's not expensive (the block is in page
	 * cache already, so no I/O), but it's not free either.
	 */
	if (!index_prefetch_add_cache(prefetch, block))
	{
		prefetch->countPrefetch++;

		PrefetchBuffer(scan->heapRelation, MAIN_FORKNUM, block);
		pgBufferUsage.blks_prefetches++;
	}
}
