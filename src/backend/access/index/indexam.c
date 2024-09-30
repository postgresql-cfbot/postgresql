/*-------------------------------------------------------------------------
 *
 * indexam.c
 *	  general index access method routines
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
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
 *		index_batch_add		- add an item (TID, itup) to the batch
 *
 * NOTES
 *		This file contains the index_ routines which used
 *		to be a scattered collection of stuff in access/genam.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/amapi.h"
#include "access/relation.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/pg_type.h"
#include "nodes/execnodes.h"
#include "pgstat.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "utils/memutils.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"
#include "utils/spccache.h"
#include "utils/syscache.h"

/* enable reading batches / prefetching of TIDs from the index */
bool		enable_indexscan_batching = false;

/* ----------------------------------------------------------------
 *					macros used in index_ routines
 *
 * Note: the ReindexIsProcessingIndex() check in RELATION_CHECKS is there
 * to check that we don't try to scan or do retail insertions into an index
 * that is currently being rebuilt or pending rebuild.  This helps to catch
 * things that don't work when reindexing system catalogs, as well as prevent
 * user errors like index expressions that access their own tables.  The check
 * doesn't prevent the actual rebuild because we don't use RELATION_CHECKS
 * when calling the index AM's ambuild routine, and there is no reason for
 * ambuild to call its subsidiary routines through this file.
 * ----------------------------------------------------------------
 */
#define RELATION_CHECKS \
do { \
	Assert(RelationIsValid(indexRelation)); \
	Assert(PointerIsValid(indexRelation->rd_indam)); \
	if (unlikely(ReindexIsProcessingIndex(RelationGetRelid(indexRelation)))) \
		ereport(ERROR, \
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
				 errmsg("cannot access index \"%s\" while it is being reindexed", \
						RelationGetRelationName(indexRelation)))); \
} while(0)

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
											  ParallelIndexScanDesc pscan, bool temp_snap);
static inline void validate_relation_kind(Relation r);

/* index batching */
static void index_batch_init(IndexScanDesc scan);
static void index_batch_reset(IndexScanDesc scan);
static bool index_batch_getnext(IndexScanDesc scan,
								ScanDirection direction);
static ItemPointer index_batch_getnext_tid(IndexScanDesc scan,
										   ScanDirection direction);
static void index_batch_prefetch(IndexScanDesc scan,
								 ScanDirection direction);

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

	validate_relation_kind(r);

	return r;
}

/* ----------------
 *		try_index_open - open an index relation by relation OID
 *
 *		Same as index_open, except return NULL instead of failing
 *		if the relation does not exist.
 * ----------------
 */
Relation
try_index_open(Oid relationId, LOCKMODE lockmode)
{
	Relation	r;

	r = try_relation_open(relationId, lockmode);

	/* leave if index does not exist */
	if (!r)
		return NULL;

	validate_relation_kind(r);

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
 *		validate_relation_kind - check the relation's kind
 *
 *		Make sure relkind is an index or a partitioned index.
 * ----------------
 */
static inline void
validate_relation_kind(Relation r)
{
	if (r->rd_rel->relkind != RELKIND_INDEX &&
		r->rd_rel->relkind != RELKIND_PARTITIONED_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index",
						RelationGetRelationName(r))));
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

/* -------------------------
 *		index_insert_cleanup - clean up after all index inserts are done
 * -------------------------
 */
void
index_insert_cleanup(Relation indexRelation,
					 IndexInfo *indexInfo)
{
	RELATION_CHECKS;

	if (indexRelation->rd_indam->aminsertcleanup)
		indexRelation->rd_indam->aminsertcleanup(indexRelation, indexInfo);
}

/*
 * index_beginscan - start a scan of an index with amgettuple
 *
 * Caller must be holding suitable locks on the heap and the index.
 */
IndexScanDesc
index_beginscan(Relation heapRelation,
				Relation indexRelation,
				Snapshot snapshot,
				int nkeys, int norderbys,
				bool enable_batching)
{
	IndexScanDesc scan;

	Assert(snapshot != InvalidSnapshot);

	scan = index_beginscan_internal(indexRelation, nkeys, norderbys, snapshot, NULL, false);

	/*
	 * Save additional parameters into the scandesc.  Everything else was set
	 * up by RelationGetIndexScan.
	 */
	scan->heapRelation = heapRelation;
	scan->xs_snapshot = snapshot;

	/* prepare to fetch index matches from table */
	scan->xs_heapfetch = table_index_fetch_begin(heapRelation);

	/*
	 * If explicitly requested and supported by both the index AM and the
	 * plan, initialize batching info.
	 *
	 * XXX We do this after ambeginscan(), which means the AM can't init the
	 * private data in there (it doesn't even know if batching will be used at
	 * that point).
	 *
	 * XXX Maybe we should have a separate "amcanbatch" call, to let the AM
	 * decide if batching is supported depending on the scan details.
	 */
	if ((indexRelation->rd_indam->amgetbatch != NULL) &&
		enable_batching &&
		enable_indexscan_batching)
	{
		index_batch_init(scan);
	}

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

	scan = index_beginscan_internal(indexRelation, nkeys, 0, snapshot, NULL, false);

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
						 ParallelIndexScanDesc pscan, bool temp_snap)
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
	scan = indexRelation->rd_indam->ambeginscan(indexRelation, nkeys,
												norderbys);
	/* Initialize information for parallel scan. */
	scan->parallel_scan = pscan;
	scan->xs_temp_snap = temp_snap;

	/*
	 * No batching by default, so set it to NULL. Will be initialized later if
	 * batching is requested and AM supports it.
	 */
	scan->xs_batch = NULL;

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

	/*
	 * Reset the batch, to make it look empty.
	 *
	 * Done after the amrescan() call, in case the AM needs some of the batch
	 * info (e.g. to properly transfer the killed tuples).
	 *
	 * XXX This is a bit misleading, because index_batch_reset does not reset
	 * the killed tuples. So if that's the only justification, we could have
	 * done it before the call.
	 */
	index_batch_reset(scan);
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

	/*
	 * Reset the batch, to make it look empty.
	 *
	 * Done after the amrescan() call, in case the AM needs some of the batch
	 * info (e.g. to properly transfer the killed tuples).
	 *
	 * XXX This is a bit misleading, because index_batch_reset does not reset
	 * the killed tuples. So if that's the only justification, we could have
	 * done it before the call.
	 */
	index_batch_reset(scan);
}

/*
 * index_parallelscan_estimate - estimate shared memory for parallel scan
 */
Size
index_parallelscan_estimate(Relation indexRelation, int nkeys, int norderbys,
							Snapshot snapshot)
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
						  indexRelation->rd_indam->amestimateparallelscan(nkeys,
																		  norderbys));

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

	target->ps_locator = heapRelation->rd_locator;
	target->ps_indexlocator = indexRelation->rd_locator;
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
 */
IndexScanDesc
index_beginscan_parallel(Relation heaprel, Relation indexrel, int nkeys,
						 int norderbys, ParallelIndexScanDesc pscan,
						 bool enable_batching)
{
	Snapshot	snapshot;
	IndexScanDesc scan;

	Assert(RelFileLocatorEquals(heaprel->rd_locator, pscan->ps_locator));
	Assert(RelFileLocatorEquals(indexrel->rd_locator, pscan->ps_indexlocator));

	snapshot = RestoreSnapshot(pscan->ps_snapshot_data);
	RegisterSnapshot(snapshot);
	scan = index_beginscan_internal(indexrel, nkeys, norderbys, snapshot,
									pscan, true);

	/*
	 * Save additional parameters into the scandesc.  Everything else was set
	 * up by index_beginscan_internal.
	 */
	scan->heapRelation = heaprel;
	scan->xs_snapshot = snapshot;

	/* prepare to fetch index matches from table */
	scan->xs_heapfetch = table_index_fetch_begin(heaprel);

	/*
	 * If explicitly requested and supported by both the index AM and the
	 * plan, initialize batching info.
	 *
	 * XXX We do this after ambeginscan(), which means the AM can't init the
	 * private data in there (it doesn't even know if batching will be used at
	 * that point).
	 *
	 * XXX Maybe we should have a separate "amcanbatch" call, to let the AM
	 * decide if batching is supported depending on the scan details.
	 */
	if ((indexrel->rd_indam->amgetbatch != NULL) &&
		enable_batching &&
		enable_indexscan_batching)
	{
		index_batch_init(scan);
	}

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
	 * When using batching (which may be disabled for various reasons (e.g.
	 * through a GUC, the index AM not supporting it) do the old approach.
	 *
	 * XXX Maybe we should enable batching based on the plan too, so that we
	 * don't do batching when it's probably useless (e.g. semijoins or queries
	 * with LIMIT 1 etc.). But maybe the approach with slow ramp-up (starting
	 * with small batches) will handle that well enough.
	 *
	 * XXX Perhaps it'd be possible to do both in index_getnext_slot(), i.e.
	 * call either the original code without batching, or the new batching
	 * code if supported/enabled. It's not great to have duplicated code.
	 */
	if (scan->xs_batch != NULL)
	{
batch_loaded:
		/* Try getting a TID from the current batch (if we have one). */
		while (index_batch_getnext_tid(scan, direction) != NULL)
		{
			/*
			 * We've successfully loaded a TID from the batch, so issue
			 * prefetches for future TIDs if needed.
			 */
			index_batch_prefetch(scan, direction);

			return &scan->xs_heaptid;
		}

		/*
		 * We either don't have any batch yet, or we've already processed
		 * all items from the current batch. Try loading the next one.
		 *
		 * If we succeed, issue prefetches (using the current prefetch
		 * distance without ramp up), and then go back to returning the
		 * TIDs from the batch.
		 *
		 * XXX Maybe do this as a simple while/for loop without the goto.
		 */
		if (index_batch_getnext(scan, direction))
		{
			index_batch_prefetch(scan, direction);
			goto batch_loaded;
		}

		return NULL;
	}

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
	{
		if (scan->xs_batch == NULL)
		{
			scan->kill_prior_tuple = all_dead;
		}
		else if (all_dead)
		{
			/* batch case - record the killed tuple in the batch */
			if (scan->xs_batch->nKilledItems < scan->xs_batch->maxSize)
				scan->xs_batch->killedItems[scan->xs_batch->nKilledItems++]
					= scan->xs_batch->currIndex;
		}
	}

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
	for (;;)
	{
		if (!scan->xs_heap_continue)
		{
			ItemPointer tid;

			/* Time to fetch the next TID from the index */
			tid = index_getnext_tid(scan, direction);

			/* If we're out of index entries, we're done */
			if (tid == NULL)
				break;

			Assert(ItemPointerEquals(tid, &scan->xs_heaptid));
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
 * INDEX BATCHING AND PREFETCHING
 *
 * Allows reading chunks of items from an index, instead of reading them
 * one by one. This reduces the overhead of accessing index pages, and
 * also allows acting on "future" TIDs - e.g. we can prefetch heap pages
 * that will be needed, etc.
 *
 *
 * index AM contract
 * -----------------
 *
 * To support batching, the index AM needs to implement an optional callback
 * amgetbatch() which loads data into the batch (in the scan descriptor).
 *
 * The index AM also needs to ensure it can perform all optimizations for
 * all TIDs in the current batch. A good example of is the kill_prior_tuple
 * optimization - with batching, the index AM may receive the information
 * which tuples are to be killed with a delay - when loading the next
 * batch, when ending/restarting the scan, etc. The AM needs to ensure it
 * can still process such information, to keep the optimization effective.
 *
 * The AM may also need to keep pins required by the whole batch (not just
 * the last tuple), etc.
 *
 * What this means/requires is very dependent on the index AM, of course.
 * For B-Tree (and most other index AMs), batches spanning multiple leaf
 * pages would be problematic. Such batches would work for basic index
 * scans, but the kill_prior_tuple would be an issue - the AMs keep only
 * a single leaf pinned. We'd either need to keep multiple pins, or allow
 * reading older leaf pages pages (which might have been modified). Index
 * only-scans is challenging too - we keep IndexTuple pointers into the
 * leaf pages, which requires keeping those pins too.
 *
 * To solve this, we give the AM some control over batch boundaries. It is
 * up to the index AM to pick which range of index items to load into the
 * batch, and how to ensure all the optimizations are possible, keep pins,
 * and so on. The index AM may use information about the batch (in the
 * scan descriptor, maintained by indexam.c code), and may also keep some
 * private information (in the existing "opaque" scan field).
 *
 * For most index AMs the easiest way is to not load batches spanning
 * multiple leaf pages. This may impact the efficiency, especially for
 * indexes with wide index tuples, as it means batches close to the end
 * of the leaf page may be smaller.
 *
 * Note: There already is a pipeline break for prefetching - as we are
 * getting closer to the end of a batch, we can't prefetch further than
 * that, and the effective prefetch distance drops to 0.
 *
 * The alternative would be to make the index AMs more complex, to keep
 * more leaf pages pinned, etc. The current model does not prohibit the
 * index AMs from implementing that - it's entirely possible to keep the
 * additional information in the "opaque" structure (say, list of pinned
 * pages, and other necessary details).
 *
 * But that does not seem like a good trade off, as it's subject to
 * "diminishing returns" - we see significant gains initially (with even
 * small batches / prefetch distance), and as the batch grows the gains
 * get smaller and smaller. It does not seem worth the complexity of
 * pinning more pages etc. at least for the first version.
 *
 * To deal with the "prefetch pipeline break", that could be addressed by
 * allowing multiple in-fligt batches - e.g. with prefetch distance 64
 * we might have three batches of 32 items each, to prefetch far ahead.
 * But that's not what this patch does yet.
 *
 *
 * batch = sliding window
 * ----------------------
 *
 * A good way to visualize a batch is a sliding window over the array
 * of items on a leaf page. In the simplest example (forward scan with no
 * changes of direction), we slice the array into smaller chunks, and
 * then process each of those chunks.
 *
 * The batch size is adaptive - it starts small (only 8 elements) and
 * increases as we read more batches (up to 64 elements). We don't want
 * to regress cases that only need a single item (e.g. LIMIT 1 queries),
 * and loading/copying a lot of data might cause that. So we start small
 * and increase the size - that still improves cases reading a lot of
 * data from the index, without hurting small queries.
 *
 * Note: This gradual ramp up is for batch size, independent of what we
 * do for prefetch. The prefetch distance is gradually increased too, but
 * it's independent / orthogonal to the batch size. The batch size limits
 * how far ahead we can prefetch, of course.
 *
 * Note: The current limits on batch size (initial 8, maximum 64) are
 * quite arbitrary, it just seemed those values are sane. We could adjust
 * the initial size, but I don't think it'd make a fundamental difference.
 * Growing the batches faster/slower has bigger impact.
 *
 * The maximum batch size does not matter much - it's true a btree index can
 * have up to ~1300 items per 8K leaf page, but in most cases the actual
 * number is lower, perhaps ~300. That's not that far from 64.
 *
 * Each batch has a firstIndex/lastIndex to track which part of the leaf
 * page it currently represents.
 *
 *
 * kill_prior_tuples
 * -----------------
 *
 * If we decide a tuple can be killed, the batch item is marked accordingly,
 * and the flag is reset to false (so that the index AM does not do something
 * silly to a random tuple it thinks is "current").
 *
 * Then the next time the AM decides it's time to kill tuples, the AM needs
 * to look at the batch and consider the tuples marked to be killed. B-Tree
 * simply adds those TIDs to the regular "killItems" array.
 *
 *
 * mark/restore
 * ------------
 *
 * With batching, the index AM does not know the the "current" position on
 * the leaf page - we don't propagate this to the index AM while walking
 * items in the batch. To make ammarkpos() work, the index AM has to check
 * the current position in the batch, and translate it to the proper page
 * position, using the private information (about items in the batch).
 *
 * XXX This needs more work, I don't quite like how the two layers interact,
 * it seems quite wrong to look at the batch info directly.
 */

/*
 * Comprehensive check of various invariants on the index batch. Makes sure
 * the indexes are set as expected, the buffer size is within limits, and
 * so on.
 */
static void
AssertCheckBatchInfo(IndexScanDesc scan)
{
#ifdef USE_ASSERT_CHECKING
	/* all the arrays need to be allocated */
	Assert((scan->xs_batch->heaptids != NULL) &&
		   (scan->xs_batch->killedItems != NULL) &&
		   (scan->xs_batch->privateData != NULL));

	/* if IndexTuples expected, should be allocated too */
	Assert(!(scan->xs_want_itup && (scan->xs_batch->itups == NULL)));

	/* Various check on batch sizes */
	Assert((scan->xs_batch->initSize >= 0) &&
		   (scan->xs_batch->initSize <= scan->xs_batch->currSize) &&
		   (scan->xs_batch->currSize <= scan->xs_batch->maxSize) &&
		   (scan->xs_batch->maxSize <= 1024));	/* arbitrary limit */

	/* Is the number of in the batch TIDs in a valid range? */
	Assert((scan->xs_batch->nheaptids >= 0) &&
		   (scan->xs_batch->nheaptids <= scan->xs_batch->maxSize));

	/*
	 * The current item must be between -1 and nheaptids. Those two extreme
	 * values are starting points for forward/backward scans.
	 */
	Assert((scan->xs_batch->currIndex >= -1) &&
		   (scan->xs_batch->currIndex <= scan->xs_batch->nheaptids));

	/* check prefetch data */
	Assert((scan->xs_batch->prefetchTarget >= 0) &&
		   (scan->xs_batch->prefetchTarget <= scan->xs_batch->prefetchMaximum));

	Assert((scan->xs_batch->prefetchIndex >= -1) &&
		   (scan->xs_batch->prefetchIndex <= scan->xs_batch->nheaptids));

	for (int i = 0; i < scan->xs_batch->nheaptids; i++)
		Assert(ItemPointerIsValid(&scan->xs_batch->heaptids[i]));
#endif
}

/* Is the batch full (TIDs up to capacity)? */
#define	INDEX_BATCH_IS_FULL(scan)	\
	((scan)->xs_batch->nheaptids == (scan)->xs_batch->currSize)

/* Is the batch empty (no TIDs)? */
#define	INDEX_BATCH_IS_EMPTY(scan)	\
	((scan)->xs_batch->nheaptids == 0)

/*
 * Did we process all items? For forward scan it means the index points to the
 * last item, for backward scans it has to point to the first one.
 *
 * This does not cover empty batches properly, because of backward scans.
 */
#define	INDEX_BATCH_IS_PROCESSED(scan, direction)	\
	(ScanDirectionIsForward(direction) ? \
		((scan)->xs_batch->nheaptids == ((scan)->xs_batch->currIndex + 1)) : \
		((scan)->xs_batch->currIndex == 0))

/* Does the batch items in the requested direction? */
#define INDEX_BATCH_HAS_ITEMS(scan, direction) \
	(!INDEX_BATCH_IS_EMPTY(scan) && !INDEX_BATCH_IS_PROCESSED(scan, direction))


/* ----------------
 *		index_batch_getnext - get the next batch of TIDs from a scan
 *
 * Returns true if we managed to read at least some TIDs into the batch,
 * or false if there are no more TIDs in the scan. The xs_heaptids and
 * xs_nheaptids fields contain the TIDS and the number of elements.
 *
 * XXX This only loads the TIDs and resets the various batch fields to
 * fresh state. It does not set xs_heaptid/xs_itup/xs_hitup, that's the
 * responsibility of the following index_batch_getnext_tid() calls.
 * ----------------
 */
static bool
index_batch_getnext(IndexScanDesc scan, ScanDirection direction)
{
	bool		found;

	SCAN_CHECKS;
	CHECK_SCAN_PROCEDURE(amgetbatch);

	/* XXX: we should assert that a snapshot is pushed or registered */
	Assert(TransactionIdIsValid(RecentXmin));

	/* comprehensive checks of batching info */
	AssertCheckBatchInfo(scan);

	/*
	 * We never read a new batch before we run out of items in the current
	 * one. The current batch has to be either empty or we ran out of items
	 * (in the given direction).
	 */
	Assert(!INDEX_BATCH_HAS_ITEMS(scan, direction));

	/*
	 * Reset the current/prefetch positions in the batch.
	 *
	 * XXX Done before calling amgetbatch(), so that it sees the index as
	 * invalid, batch as empty, and can add items.
	 *
	 * XXX Intentionally does not reset the nheaptids, because the AM does
	 * rely on that when processing killed tuples. Maybe store the killed
	 * tuples differently?
	 */
	scan->xs_batch->currIndex = -1;
	scan->xs_batch->prefetchIndex = 0;
	scan->xs_batch->nheaptids = 0;

	/*
	 * Reset the memory context with all per-batch data, allocated by the AM.
	 * This might be tuples, or anything else the AM needs.
	 *
	 * XXX Make sure to reset the tuples, because the AM may do something with
	 * them later (e.g. release them, as getNextNearest in gist), but we may
	 * release them by the MemoryContextReset() call.
	 *
	 * This might break the AM if it relies on them pointing to the last
	 * tuple, but at least it has the chance to do the right thing by checking
	 * if the pointer is NULL.
	 */
	scan->xs_itup = NULL;
	scan->xs_hitup = NULL;

	MemoryContextReset(scan->xs_batch->ctx);

	/*
	 * The AM's amgetbatch proc loads a chunk of TIDs matching the scan keys,
	 * and puts the TIDs into scan->xs_batch->heaptids.  It should also set
	 * scan->xs_recheck and possibly
	 * scan->xs_batch->itups/scan->xs_batch->hitups, though we pay no
	 * attention to those fields here.
	 *
	 * FIXME At the moment this does nothing with hitup. Needs to be fixed?
	 */
	found = scan->indexRelation->rd_indam->amgetbatch(scan, direction);

	/* Reset kill flag immediately for safety */
	scan->kill_prior_tuple = false;
	scan->xs_heap_continue = false;

	/* If we're out of index entries, we're done */
	if (!found)
	{
		/* release resources (like buffer pins) from table accesses */
		if (scan->xs_heapfetch)
			table_index_fetch_reset(scan->xs_heapfetch);

		return false;
	}

	/* We should have a non-empty batch with items. */
	Assert(INDEX_BATCH_HAS_ITEMS(scan, direction));

	pgstat_count_index_tuples(scan->indexRelation, scan->xs_batch->nheaptids);

	/*
	 * Set the prefetch index to the first item in the loaded batch (we expect
	 * the index AM to set that).
	 *
	 * FIXME Maybe set the currIndex here, not in the index AM. It seems much
	 * more like indexam.c responsibility rather than something every index AM
	 * should be doing (in _bt_first_batch etc.).
	 *
	 * FIXME It's a bit unclear who (indexam.c or the index AM) is responsible
	 * for setting which fields. This needs clarification.
	 */
	scan->xs_batch->prefetchIndex = scan->xs_batch->currIndex;

	/*
	 * Try to increase the size of the batch. Intentionally done after the AM
	 * call, so that the new value applies to the next batch. Otherwise we
	 * would always skip the initial batch size.
	 */
	scan->xs_batch->currSize = Min(scan->xs_batch->currSize + 1,
								   scan->xs_batch->maxSize);

	/* comprehensive checks of batching info */
	AssertCheckBatchInfo(scan);

	/* Return the batch of TIDs we found. */
	return true;
}

/* ----------------
 *		index_getnext_batch_tid - get the next TID from the current batch
 *
 * Same calling convention as index_getnext_tid(), except that NULL means
 * no more items in the current batch, there may be more batches.
 *
 * XXX This only sets xs_heaptid and xs_itup (if requested). Not sure if
 * we need to do something with xs_hitup.
 *
 * FIXME Should this set xs_hitup?
 * ----------------
 */
static ItemPointer
index_batch_getnext_tid(IndexScanDesc scan, ScanDirection direction)
{
	/* comprehensive checks of batching info */
	AssertCheckBatchInfo(scan);

	/*
	 * Bail out if he batch does not have more items in the requested directio
	 * (either empty or everthing processed).
	 */
	if (!INDEX_BATCH_HAS_ITEMS(scan, direction))
		return NULL;

	/*
	 * Advance to the next batch item - we know it's not empty and there are
	 * items to process, so this is valid.
	 */
	if (ScanDirectionIsForward(direction))
		scan->xs_batch->currIndex++;
	else
		scan->xs_batch->currIndex--;

	/*
	 * Next TID from the batch, optionally also the IndexTuple/HeapTuple.
	 *
	 * XXX Not sure how to decide which of the tuples to set, seems easier to
	 * just set both, one of them will be NULL.
	 *
	 * XXX Do we need to reset the itups/htups array between batches? Doesn't
	 * seem necessary, but maybe we could get bogus data?
	 */
	scan->xs_heaptid = scan->xs_batch->heaptids[scan->xs_batch->currIndex];
	if (scan->xs_want_itup)
	{
		scan->xs_itup = scan->xs_batch->itups[scan->xs_batch->currIndex];
		scan->xs_hitup = scan->xs_batch->htups[scan->xs_batch->currIndex];
	}

	scan->xs_recheck = scan->xs_batch->recheck[scan->xs_batch->currIndex];

	/*
	 * If there are order-by clauses, point to the appropriate chunk in the
	 * arrays.
	 */
	if (scan->numberOfOrderBys > 0)
	{
		int			idx = scan->numberOfOrderBys * scan->xs_batch->currIndex;

		scan->xs_orderbyvals = &scan->xs_batch->orderbyvals[idx];
		scan->xs_orderbynulls = &scan->xs_batch->orderbynulls[idx];
	}

	/* comprehensive checks of batching info */
	AssertCheckBatchInfo(scan);

	return &scan->xs_heaptid;
}

/* ----------------
 *		index_batch_prefetch - prefetch pages for TIDs in current batch
 *
 * The prefetch distance is increased gradually, similar to what we do for
 * bitmap heap scans. We start from distance 0 (no prefetch), and then in each
 * iteration increment the distance up to prefetchMaximum.
 *
 * The prefetch distance is reset (to 0) only on rescans, not between batches.
 *
 * It's possible to provide an index_prefetch_callback callback, to affect
 * which items need to be prefetched. With prefetch_callback=NULL, all
 * items are prefetched. With the callback provided, the item is prefetched
 * iff the callback and returns true.
 *
 * The "arg" argument is used to pass a state for the plan node invoking the
 * function, and is then passed to the callback. This means the callback is
 * specific to the plan state.
 *
 * XXX the prefetchMaximum depends on effective_io_concurrency, and also on
 * tablespace options.
 *
 * XXX For accesses that change scan direction, we may do a lot of unnecessary
 * prefetching (because we will re-issue prefetches for what we recently read).
 * I'm not sure if there's a simple way to track what was already prefetched.
 * Maybe we could count how far we got (in the forward direction), keep that
 * as a watermark, and never prefetch again below it.
 *
 * XXX Maybe wrap this in ifdef USE_PREFETCH?
 * ----------------
 */
static void
index_batch_prefetch(IndexScanDesc scan, ScanDirection direction)
{
	int			prefetchStart,
				prefetchEnd;

	IndexPrefetchCallback	prefetch_callback = scan->xs_batch->prefetchCallback;
	void *arg = scan->xs_batch->prefetchArgument;

	if (ScanDirectionIsForward(direction))
	{
		/* Where should we start to prefetch? */
		prefetchStart = Max(scan->xs_batch->currIndex,
							scan->xs_batch->prefetchIndex);

		/*
		 * Where should we stop prefetching? this is the first item that we do
		 * NOT prefetch, i.e. it can be the first item after the batch.
		 */
		prefetchEnd = Min((scan->xs_batch->currIndex + 1) + scan->xs_batch->prefetchTarget,
						  scan->xs_batch->nheaptids);

		/* FIXME should calculate in a way to make this unnecessary */
		prefetchStart = Max(Min(prefetchStart, scan->xs_batch->nheaptids - 1), 0);
		prefetchEnd = Max(Min(prefetchEnd, scan->xs_batch->nheaptids - 1), 0);

		/* remember how far we prefetched / where to start the next prefetch */
		scan->xs_batch->prefetchIndex = prefetchEnd;
	}
	else
	{
		/* Where should we start to prefetch? */
		prefetchEnd = Min(scan->xs_batch->currIndex,
						  scan->xs_batch->prefetchIndex);

		/*
		 * Where should we stop prefetching? this is the first item that we do
		 * NOT prefetch, i.e. it can be the first item after the batch.
		 */
		prefetchStart = Max((scan->xs_batch->currIndex - 1) - scan->xs_batch->prefetchTarget,
							-1);

		/* FIXME should calculate in a way to make this unnecessary */
		prefetchStart = Max(Min(prefetchStart, scan->xs_batch->nheaptids - 1), 0);
		prefetchEnd = Max(Min(prefetchEnd, scan->xs_batch->nheaptids - 1), 0);

		/* remember how far we prefetched / where to start the next prefetch */
		scan->xs_batch->prefetchIndex = prefetchStart;
	}

	/*
	 * It's possible we get inverted prefetch range after a restrpos() call,
	 * because we intentionally don't reset the prefetchIndex - we don't want
	 * to prefetch pages over and over in this case. We'll do nothing in that
	 * case, except for the AssertCheckBatchInfo().
	 *
	 * FIXME I suspect this actually does not work correctly if we change the
	 * direction, because the prefetchIndex will flip between two extremes
	 * thanks to the Min/Max.
	 */

	/*
	 * Increase the prefetch distance, but not beyond prefetchMaximum. We
	 * intentionally do this after calculating start/end, so that we start
	 * actually prefetching only after the first item.
	 */
	scan->xs_batch->prefetchTarget = Min(scan->xs_batch->prefetchTarget + 1,
										 scan->xs_batch->prefetchMaximum);

	/* comprehensive checks of batching info */
	AssertCheckBatchInfo(scan);

	/* finally, do the actual prefetching */
	for (int i = prefetchStart; i < prefetchEnd; i++)
	{
		/* skip block if the provided callback says so */
		if (prefetch_callback && !prefetch_callback(scan, arg, i))
			continue;

		PrefetchBuffer(scan->heapRelation, MAIN_FORKNUM,
					   ItemPointerGetBlockNumber(&scan->xs_batch->heaptids[i]));
	}
}

/*
 * index_batch_init
 *		Initialize various fields / arrays needed by batching.
 *
 * FIXME This is a bit ad-hoc hodge podge, due to how I was adding more and
 * more pieces. Some of the fields may be not quite necessary, needs cleanup.
 */
static void
index_batch_init(IndexScanDesc scan)
{
	/* init batching info, but only if batch supported */
	Assert(scan->indexRelation->rd_indam->amgetbatch != NULL);

	scan->xs_batch = palloc0(sizeof(IndexScanBatchData));

	/*
	 * Set some reasonable batch size defaults.
	 *
	 * XXX Maybe should depend on prefetch distance, or something like that?
	 * The initSize will affect how far ahead we can prefetch.
	 */
	scan->xs_batch->maxSize = 64;
	scan->xs_batch->initSize = 8;
	scan->xs_batch->currSize = scan->xs_batch->initSize;

	/* initialize prefetching info */
	scan->xs_batch->prefetchMaximum =
		get_tablespace_io_concurrency(scan->heapRelation->rd_rel->reltablespace);
	scan->xs_batch->prefetchTarget = 0;
	scan->xs_batch->prefetchIndex = 0;

	/* */
	scan->xs_batch->currIndex = -1;

	/* Preallocate the largest allowed array of TIDs. */
	scan->xs_batch->nheaptids = 0;
	scan->xs_batch->heaptids = palloc0(sizeof(ItemPointerData) * scan->xs_batch->maxSize);

	/*
	 * XXX We can't check scan->xs_want_itup, because that's set only after
	 * the scan is initialized (and we initialize in beginscan). Maybe we
	 * could (or should) allocate lazily.
	 */
	scan->xs_batch->itups = palloc(sizeof(IndexTuple) * scan->xs_batch->maxSize);
	scan->xs_batch->htups = palloc(sizeof(HeapTuple) * scan->xs_batch->maxSize);

	scan->xs_batch->recheck = palloc(sizeof(bool) * scan->xs_batch->maxSize);

	/*
	 * XXX Maybe use a more compact bitmap? We need just one bit per element,
	 * not a bool. This is easier / more convenient to manipulate, though.
	 *
	 * XXX Maybe should allow more items thant the max batch size?
	 */
	scan->xs_batch->nKilledItems = 0;
	scan->xs_batch->killedItems = (int *) palloc0(sizeof(int) * scan->xs_batch->maxSize);

	/*
	 * XXX Maybe allocate only when actually needed? Also, shouldn't we have a
	 * memory context for the private data?
	 */
	scan->xs_batch->privateData = (Datum *) palloc0(sizeof(Datum) * scan->xs_batch->maxSize);

	if (scan->numberOfOrderBys > 0)
	{
		int			cnt = (scan->xs_batch->maxSize * scan->numberOfOrderBys);

		scan->xs_batch->orderbyvals = (Datum *) palloc0(sizeof(Datum) * cnt);
		scan->xs_batch->orderbynulls = (bool *) palloc0(sizeof(Datum) * cnt);
	}
	else
	{
		scan->xs_batch->orderbyvals = NULL;
		scan->xs_batch->orderbynulls = NULL;
	}

	scan->xs_batch->ctx = AllocSetContextCreate(CurrentMemoryContext,
												"indexscan batch context",
												ALLOCSET_DEFAULT_SIZES);

	/* comprehensive checks */
	AssertCheckBatchInfo(scan);
}

/*
 * index_batch_reset
 *		Reset the batch before reading the next chunk of data.
 *
 * FIXME Another bit in need of cleanup. The currIndex default (-1) is not quite
 * correct, because for backwards scans is wrong.
 */
static void
index_batch_reset(IndexScanDesc scan)
{
	/* bail out if batching not enabled */
	if (!scan->xs_batch)
		return;

	scan->xs_batch->nheaptids = 0;
	scan->xs_batch->prefetchIndex = 0;
	scan->xs_batch->currIndex = -1;
}

/*
 * index_batch_add
 *		Add an item to the batch.
 *
 * The item is always a TID, and then also IndexTuple if requested (for IOS).
 * Items are always added from the beginning (index 0).
 *
 * Returns true when adding the item was successful, or false when the batch
 * is full (and the item should be added to the next batch).
 */
bool
index_batch_add(IndexScanDesc scan, ItemPointerData tid, bool recheck,
				IndexTuple itup, HeapTuple htup)
{
	/* comprehensive checks on the batch info */
	AssertCheckBatchInfo(scan);

	/* don't add TIDs beyond the current batch size */
	if (INDEX_BATCH_IS_FULL(scan))
		return false;

	/*
	 * There must be space for at least one entry.
	 *
	 * XXX Seems redundant with the earlier INDEX_BATCH_IS_FULL check.
	 */
	Assert(scan->xs_batch->nheaptids < scan->xs_batch->currSize);
	Assert(scan->xs_batch->nheaptids >= 0);

	scan->xs_batch->heaptids[scan->xs_batch->nheaptids] = tid;
	scan->xs_batch->privateData[scan->xs_batch->nheaptids] = (Datum) 0;

	if (scan->xs_want_itup)
	{
		scan->xs_batch->itups[scan->xs_batch->nheaptids] = itup;
		scan->xs_batch->htups[scan->xs_batch->nheaptids] = htup;
	}

	scan->xs_batch->recheck[scan->xs_batch->nheaptids] = recheck;

	if (scan->numberOfOrderBys > 0)
	{
		int			idx = scan->xs_batch->nheaptids * scan->numberOfOrderBys;

		memcpy(&scan->xs_batch->orderbyvals[idx], scan->xs_orderbyvals, sizeof(Datum) * scan->numberOfOrderBys);
		memcpy(&scan->xs_batch->orderbynulls[idx], scan->xs_orderbynulls, sizeof(bool) * scan->numberOfOrderBys);
	}

	scan->xs_batch->nheaptids++;

	/* comprehensive checks on the batch info */
	AssertCheckBatchInfo(scan);

	return true;
}
