/*-------------------------------------------------------------------------
 *
 * pruneheap.c
 *	  heap page pruning and HOT-chain management code
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/pruneheap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/* Working data for heap_page_prune and subroutines */
typedef struct
{
	/* tuple visibility test, initialized for the relation */
	GlobalVisState *vistest;
	/* whether or not dead items can be set LP_UNUSED during pruning */
	bool		mark_unused_now;

	TransactionId new_prune_xid;	/* new prune hint value for page */
	TransactionId snapshotConflictHorizon;	/* latest xid removed */
	int			nredirected;	/* numbers of entries in arrays below */
	int			ndead;
	int			nunused;
	/* arrays that accumulate indexes of items to be changed */
	OffsetNumber redirected[MaxHeapTuplesPerPage * 2];
	OffsetNumber nowdead[MaxHeapTuplesPerPage];
	OffsetNumber nowunused[MaxHeapTuplesPerPage];

	/*
	 * 'root_items' contains offsets of all LP_REDIRECT line pointers and
	 * normal non-HOT tuples.  They can be stand-alone items or the first item
	 * in a HOT chain.  'heaponly_items' contains heap-only tuples which can
	 * only be removed as part of a HOT chain.
	 */
	int			nroot_items;
	OffsetNumber root_items[MaxHeapTuplesPerPage];
	int			nheaponly_items;
	OffsetNumber heaponly_items[MaxHeapTuplesPerPage];

	/*
	 * processed[offnum] is true if item at offnum has been processed.
	 *
	 * This needs to be MaxHeapTuplesPerPage + 1 long as FirstOffsetNumber is
	 * 1. Otherwise every access would need to subtract 1.
	 */
	bool		processed[MaxHeapTuplesPerPage + 1];

	int			ndeleted;		/* Number of tuples deleted from the page */
} PruneState;

/* Local functions */
static HTSV_Result heap_prune_satisfies_vacuum(PruneState *prstate,
											   HeapTuple tup,
											   Buffer buffer);
static void heap_prune_chain(Page page, BlockNumber blockno, OffsetNumber maxoff,
							 OffsetNumber rootoffnum, int8 *htsv, PruneState *prstate);
static void heap_prune_record_prunable(PruneState *prstate, TransactionId xid);
static void heap_prune_record_redirect(PruneState *prstate,
									   OffsetNumber offnum, OffsetNumber rdoffnum, bool was_normal);
static void heap_prune_record_dead(PruneState *prstate, OffsetNumber offnum, bool was_normal);
static void heap_prune_record_dead_or_unused(PruneState *prstate, OffsetNumber offnum, bool was_normal);
static void heap_prune_record_unused(PruneState *prstate, OffsetNumber offnum, bool was_normal);
static void heap_prune_record_unchanged(PruneState *prstate, OffsetNumber offnum);

static void page_verify_redirects(Page page);


/*
 * Optionally prune and repair fragmentation in the specified page.
 *
 * This is an opportunistic function.  It will perform housekeeping
 * only if the page heuristically looks like a candidate for pruning and we
 * can acquire buffer cleanup lock without blocking.
 *
 * Note: this is called quite often.  It's important that it fall out quickly
 * if there's not any use in pruning.
 *
 * Caller must have pin on the buffer, and must either have an exclusive lock
 * (and pass already_locked = true) or not have a lock on it.
 */
void
heap_page_prune_opt(Relation relation, Buffer buffer, bool already_locked)
{
	Page		page = BufferGetPage(buffer);
	TransactionId prune_xid;
	GlobalVisState *vistest;
	Size		minfree;

	if (relation->rd_id > 16384)
		elog(WARNING, "in heap_page_prune_opt");
	/*
	 * We can't write WAL in recovery mode, so there's no point trying to
	 * clean the page. The primary will likely issue a cleaning WAL record
	 * soon anyway, so this is no particular loss.
	 */
	if (RecoveryInProgress())
		return;

	/*
	 * First check whether there's any chance there's something to prune,
	 * determining the appropriate horizon is a waste if there's no prune_xid
	 * (i.e. no updates/deletes left potentially dead tuples around).
	 */
	prune_xid = ((PageHeader) page)->pd_prune_xid;
	if (!TransactionIdIsValid(prune_xid))
		return;

	/*
	 * Check whether prune_xid indicates that there may be dead rows that can
	 * be cleaned up.
	 */
	vistest = GlobalVisTestFor(relation);

	if (!GlobalVisTestIsRemovableXid(vistest, prune_xid))
		return;

	/*
	 * We prune when a previous UPDATE failed to find enough space on the page
	 * for a new tuple version, or when free space falls below the relation's
	 * fill-factor target (but not less than 10%).
	 *
	 * Checking free space here is questionable since we aren't holding any
	 * lock on the buffer; in the worst case we could get a bogus answer. It's
	 * unlikely to be *seriously* wrong, though, since reading either pd_lower
	 * or pd_upper is probably atomic.  Avoiding taking a lock seems more
	 * important than sometimes getting a wrong answer in what is after all
	 * just a heuristic estimate.
	 */
	minfree = RelationGetTargetPageFreeSpace(relation,
											 HEAP_DEFAULT_FILLFACTOR);
	minfree = Max(minfree, BLCKSZ / 10);

	if (PageIsFull(page) || PageGetHeapFreeSpace(page) < minfree)
	{
		/* OK, try to get exclusive buffer lock if necessary */
		if ((!already_locked && !ConditionalLockBufferForCleanup(buffer)) ||
				(already_locked && !IsBufferCleanupOK(buffer)))
			return;

		/*
		 * Now that we have buffer lock, get accurate information about the
		 * page's free space, and recheck the heuristic about whether to
		 * prune.
		 */
		if (PageIsFull(page) || PageGetHeapFreeSpace(page) < minfree)
		{
			OffsetNumber dummy_off_loc;
			PruneResult presult;

			/*
			 * For now, pass mark_unused_now as false regardless of whether or
			 * not the relation has indexes, since we cannot safely determine
			 * that during on-access pruning with the current implementation.
			 */
			heap_page_prune(relation, buffer, vistest, 0,
							&presult, PRUNE_ON_ACCESS, &dummy_off_loc);

			/*
			 * Report the number of tuples reclaimed to pgstats.  This is
			 * presult.ndeleted minus the number of newly-LP_DEAD-set items.
			 *
			 * We derive the number of dead tuples like this to avoid totally
			 * forgetting about items that were set to LP_DEAD, since they
			 * still need to be cleaned up by VACUUM.  We only want to count
			 * heap-only tuples that just became LP_UNUSED in our report,
			 * which don't.
			 *
			 * VACUUM doesn't have to compensate in the same way when it
			 * tracks ndeleted, since it will set the same LP_DEAD items to
			 * LP_UNUSED separately.
			 */
			if (presult.ndeleted > presult.nnewlpdead)
				pgstat_update_heap_dead_tuples(relation,
											   presult.ndeleted - presult.nnewlpdead);
		}

		/* And release buffer lock if we acquired it */
		if (!already_locked)
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

		/*
		 * We avoid reuse of any free space created on the page by unrelated
		 * UPDATEs/INSERTs by opting to not update the FSM at this point.  The
		 * free space should be reused by UPDATEs to *this* page.
		 */
	}
}


/*
 * Prune and repair fragmentation in the specified page.
 *
 * Caller must have pin and buffer cleanup lock on the page.  Note that we
 * don't update the FSM information for page on caller's behalf.  Caller might
 * also need to account for a reduction in the length of the line pointer
 * array following array truncation by us.
 *
 * vistest is used to distinguish whether tuples are DEAD or RECENTLY_DEAD
 * (see heap_prune_satisfies_vacuum).
 *
 * options:
 *   MARK_UNUSED_NOW indicates that dead items can be set LP_UNUSED during
 *   pruning.
 *
 * presult contains output parameters needed by callers such as the number of
 * tuples removed and the number of line pointers newly marked LP_DEAD.
 * heap_page_prune() is responsible for initializing it.
 *
 * reason indicates why the pruning is performed.  It is included in the WAL
 * record for debugging and analysis purposes, but otherwise has no effect.
 *
 * off_loc is the offset location required by the caller to use in error
 * callback.
 */
void
heap_page_prune(Relation relation, Buffer buffer,
				GlobalVisState *vistest,
				int options,
				PruneResult *presult,
				PruneReason reason,
				OffsetNumber *off_loc)
{
	Page		page = BufferGetPage(buffer);
	BlockNumber blockno = BufferGetBlockNumber(buffer);
	OffsetNumber offnum,
				maxoff;
	PruneState	prstate;
	HeapTupleData tup;

	if (relation->rd_id > 16384)
	  elog(WARNING, "in heap_page_prune");
	/*
	 * Our strategy is to scan the page and make lists of items to change,
	 * then apply the changes within a critical section.  This keeps as much
	 * logic as possible out of the critical section, and also ensures that
	 * WAL replay will work the same as the normal case.
	 *
	 * First, initialize the new pd_prune_xid value to zero (indicating no
	 * prunable tuples).  If we find any tuples which may soon become
	 * prunable, we will save the lowest relevant XID in new_prune_xid. Also
	 * initialize the rest of our working state.
	 */
	prstate.new_prune_xid = InvalidTransactionId;
	prstate.vistest = vistest;
	prstate.mark_unused_now = (options & HEAP_PAGE_PRUNE_MARK_UNUSED_NOW) != 0;
	prstate.snapshotConflictHorizon = InvalidTransactionId;
	prstate.nredirected = prstate.ndead = prstate.nunused = 0;
	prstate.ndeleted = 0;
	prstate.nroot_items = 0;
	prstate.nheaponly_items = 0;

	/*
	 * presult->htsv is not initialized here because all ntuple spots in the
	 * array will be set either to a valid HTSV_Result value or -1.
	 */
	presult->ndeleted = 0;
	presult->nnewlpdead = 0;

	maxoff = PageGetMaxOffsetNumber(page);
	tup.t_tableOid = RelationGetRelid(relation);

	/*
	 * Determine HTSV for all tuples, and queue them up for processing as HOT
	 * chain roots or as a heap-only items.
	 *
	 * Determining HTSV only once for each tuple is required for correctness,
	 * to deal with cases where running HTSV twice could result in different
	 * results.  For example, RECENTLY_DEAD can turn to DEAD if another
	 * checked item causes GlobalVisTestIsRemovableFullXid() to update the
	 * horizon, or INSERT_IN_PROGRESS can change to DEAD if the inserting
	 * transaction aborts.  VACUUM assumes that there are no normal DEAD
	 * tuples left on the page after pruning, so it needs to have the same
	 * understanding of what is DEAD and what is not.
	 *
	 * It's also good for performance. Most commonly tuples within a page are
	 * stored at decreasing offsets (while the items are stored at increasing
	 * offsets). When processing all tuples on a page this leads to reading
	 * memory at decreasing offsets within a page, with a variable stride.
	 * That's hard for CPU prefetchers to deal with. Processing the items in
	 * reverse order (and thus the tuples in increasing order) increases
	 * prefetching efficiency significantly / decreases the number of cache
	 * misses.
	 */
	for (offnum = maxoff;
		 offnum >= FirstOffsetNumber;
		 offnum = OffsetNumberPrev(offnum))
	{
		ItemId		itemid = PageGetItemId(page, offnum);
		HeapTupleHeader htup;

		/*
		 * Set the offset number so that we can display it along with any
		 * error that occurred while processing this tuple.
		 */
		*off_loc = offnum;

		prstate.processed[offnum] = false;
		presult->htsv[offnum] = -1;

		/* Nothing to do if slot doesn't contain a tuple */
		if (!ItemIdIsUsed(itemid))
		{
			heap_prune_record_unchanged(&prstate, offnum);
			continue;
		}

		if (ItemIdIsDead(itemid))
		{
			/*
			 * If the caller set mark_unused_now true, we can set dead line
			 * pointers LP_UNUSED now.
			 */
			if (unlikely(prstate.mark_unused_now))
				heap_prune_record_unused(&prstate, offnum, false);
			else
				heap_prune_record_unchanged(&prstate, offnum);
			continue;
		}

		if (ItemIdIsRedirected(itemid))
		{
			/* This is the start of a HOT chain */
			prstate.root_items[prstate.nroot_items++] = offnum;
			continue;
		}

		Assert(ItemIdIsNormal(itemid));

		/*
		 * Get the tuple's visibility status and queue it up for processing.
		 */
		htup = (HeapTupleHeader) PageGetItem(page, itemid);
		tup.t_data = htup;
		tup.t_len = ItemIdGetLength(itemid);
		ItemPointerSet(&tup.t_self, blockno, offnum);

		presult->htsv[offnum] = heap_prune_satisfies_vacuum(&prstate, &tup,
															buffer);

		if (!HeapTupleHeaderIsHeapOnly(htup))
			prstate.root_items[prstate.nroot_items++] = offnum;
		else
			prstate.heaponly_items[prstate.nheaponly_items++] = offnum;
	}

	/*
	 * Process HOT chains.
	 *
	 * We added the items to the array starting from 'maxoff', so by
	 * processing the array in reverse order, we process the items in
	 * ascending offset number order.  The order doesn't matter for
	 * correctness, but some quick micro-benchmarking suggests that this is
	 * faster.  (Earlier PostgreSQL versions, which scanned all the items on
	 * the page instead of using the root_items array, also did it in
	 * ascending offset number order.)
	 */
	for (int i = prstate.nroot_items - 1; i >= 0; i--)
	{
		offnum = prstate.root_items[i];

		/* Ignore items already processed as part of an earlier chain */
		if (prstate.processed[offnum])
			continue;

		/* see preceding loop */
		*off_loc = offnum;

		/* Process this item or chain of items */
		heap_prune_chain(page, blockno, maxoff,
						 offnum, presult->htsv, &prstate);
	}

	/*
	 * Process any heap-only tuples that were not already processed as part of
	 * a HOT chain.
	 */
	for (int i = prstate.nheaponly_items - 1; i >= 0; i--)
	{
		offnum = prstate.heaponly_items[i];

		if (prstate.processed[offnum])
			continue;

		/* see preceding loop */
		*off_loc = offnum;

		/*
		 * If the tuple is DEAD and doesn't chain to anything else, mark it
		 * unused.  (If it does chain, we can only remove it as part of
		 * pruning its chain.)
		 *
		 * We need this primarily to handle aborted HOT updates, that is,
		 * XMIN_INVALID heap-only tuples.  Those might not be linked to by any
		 * chain, since the parent tuple might be re-updated before any
		 * pruning occurs.  So we have to be able to reap them separately from
		 * chain-pruning.  (Note that HeapTupleHeaderIsHotUpdated will never
		 * return true for an XMIN_INVALID tuple, so this code will work even
		 * when there were sequential updates within the aborted transaction.)
		 */
		if (presult->htsv[offnum] == HEAPTUPLE_DEAD)
		{
			ItemId		itemid = PageGetItemId(page, offnum);
			HeapTupleHeader htup = (HeapTupleHeader) PageGetItem(page, itemid);

			if (likely(!HeapTupleHeaderIsHotUpdated(htup)))
			{
				HeapTupleHeaderAdvanceConflictHorizon(htup,
													  &prstate.snapshotConflictHorizon);
				heap_prune_record_unused(&prstate, offnum, true);
			}
			else
			{
				/*
				 * This tuple should've been processed and removed as part of
				 * a HOT chain, so something's wrong.  To preserve evidence,
				 * we don't dare to remove it.  We cannot leave behind a DEAD
				 * tuple either, because that will cause VACUUM to error out.
				 * Throwing an error with a distinct error message seems like
				 * the least bad option.
				 */
				elog(ERROR, "dead heap-only tuple (%u, %d) is not linked to from any HOT chain",
					 blockno, offnum);
			}
		}
		else
			heap_prune_record_unchanged(&prstate, offnum);
	}

	/* We should now have processed every tuple exactly once  */
#ifdef USE_ASSERT_CHECKING
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		*off_loc = offnum;

		Assert(prstate.processed[offnum]);
	}
#endif

	/* Clear the offset information once we have processed the given page. */
	*off_loc = InvalidOffsetNumber;

	/* Any error while applying the changes is critical */
	START_CRIT_SECTION();

	/* Have we found any prunable items? */
	if (prstate.nredirected > 0 || prstate.ndead > 0 || prstate.nunused > 0)
	{
		/*
		 * Apply the planned item changes, then repair page fragmentation, and
		 * update the page's hint bit about whether it has free line pointers.
		 */
		heap_page_prune_execute(buffer, false,
								prstate.redirected, prstate.nredirected,
								prstate.nowdead, prstate.ndead,
								prstate.nowunused, prstate.nunused);

		/*
		 * Update the page's pd_prune_xid field to either zero, or the lowest
		 * XID of any soon-prunable tuple.
		 */
		if (relation->rd_id > 16384)
			elog(WARNING, "setting pd_prune_xid=%u", prstate.new_prune_xid);
		((PageHeader) page)->pd_prune_xid = prstate.new_prune_xid;

		/*
		 * Also clear the "page is full" flag, since there's no point in
		 * repeating the prune/defrag process until something else happens to
		 * the page.
		 */
		PageClearFull(page);

		MarkBufferDirty(buffer);

		/*
		 * Emit a WAL XLOG_HEAP2_PRUNE_FREEZE record showing what we did
		 */
		if (RelationNeedsWAL(relation))
		{
			log_heap_prune_and_freeze(relation, buffer,
									  prstate.snapshotConflictHorizon,
									  true, reason,
									  NULL, 0,
									  prstate.redirected, prstate.nredirected,
									  prstate.nowdead, prstate.ndead,
									  prstate.nowunused, prstate.nunused);
		}
	}
	else
	{
		/*
		 * If we didn't prune anything, but have found a new value for the
		 * pd_prune_xid field, update it and mark the buffer dirty. This is
		 * treated as a non-WAL-logged hint.
		 *
		 * Also clear the "page is full" flag if it is set, since there's no
		 * point in repeating the prune/defrag process until something else
		 * happens to the page.
		 */
		if (((PageHeader) page)->pd_prune_xid != prstate.new_prune_xid ||
			PageIsFull(page))
		{
			if (relation->rd_id > 16384)
				elog(WARNING, "setting pd_prune_xid=%u", prstate.new_prune_xid);
			((PageHeader) page)->pd_prune_xid = prstate.new_prune_xid;
			PageClearFull(page);
			MarkBufferDirtyHint(buffer, true);
		}
	}

	END_CRIT_SECTION();

	/* Copy information back for caller */
	presult->nnewlpdead = prstate.ndead;
	presult->ndeleted = prstate.ndeleted;
}


/*
 * Perform visibility checks for heap pruning.
 */
static HTSV_Result
heap_prune_satisfies_vacuum(PruneState *prstate, HeapTuple tup, Buffer buffer)
{
	HTSV_Result res;
	TransactionId dead_after;

	res = HeapTupleSatisfiesVacuumHorizon(tup, buffer, &dead_after);

	if (res != HEAPTUPLE_RECENTLY_DEAD)
		return res;

	if (GlobalVisTestIsRemovableXid(prstate->vistest, dead_after))
		res = HEAPTUPLE_DEAD;

	return res;
}


/*
 * Prune specified line pointer or a HOT chain originating at line pointer.
 *
 * Tuple visibility information is provided in htsv.
 *
 * If the item is an index-referenced tuple (i.e. not a heap-only tuple),
 * the HOT chain is pruned by removing all DEAD tuples at the start of the HOT
 * chain.  We also prune any RECENTLY_DEAD tuples preceding a DEAD tuple.
 * This is OK because a RECENTLY_DEAD tuple preceding a DEAD tuple is really
 * DEAD, our visibility test is just too coarse to detect it.
 *
 * Pruning must never leave behind a DEAD tuple that still has tuple storage.
 * VACUUM isn't prepared to deal with that case.
 *
 * The root line pointer is redirected to the tuple immediately after the
 * latest DEAD tuple.  If all tuples in the chain are DEAD, the root line
 * pointer is marked LP_DEAD.  (This includes the case of a DEAD simple
 * tuple, which we treat as a chain of length 1.)
 *
 * We don't actually change the page here. We just add entries to the arrays in
 * prstate showing the changes to be made.  Items to be redirected are added
 * to the redirected[] array (two entries per redirection); items to be set to
 * LP_DEAD state are added to nowdead[]; and items to be set to LP_UNUSED
 * state are added to nowunused[].
 */
static void
heap_prune_chain(Page page, BlockNumber blockno, OffsetNumber maxoff,
				 OffsetNumber rootoffnum, int8 *htsv, PruneState *prstate)
{
	TransactionId priorXmax = InvalidTransactionId;
	ItemId		rootlp;
	OffsetNumber offnum;
	OffsetNumber chainitems[MaxHeapTuplesPerPage];

	/*
	 * After traversing the HOT chain, ndeadchain is the index in chainitems
	 * of the first live successor after the last dead item.
	 */
	int			ndeadchain = 0,
				nchain = 0;

	rootlp = PageGetItemId(page, rootoffnum);

	/* Start from the root tuple */
	offnum = rootoffnum;

	/* while not end of the chain */
	for (;;)
	{
		HeapTupleHeader htup;
		ItemId		lp;

		/* Sanity check (pure paranoia) */
		if (offnum < FirstOffsetNumber)
			break;

		/*
		 * An offset past the end of page's line pointer array is possible
		 * when the array was truncated (original item must have been unused)
		 */
		if (offnum > maxoff)
			break;

		/* If item is already processed, stop --- it must not be same chain */
		if (prstate->processed[offnum])
			break;

		lp = PageGetItemId(page, offnum);

		/*
		 * Unused item obviously isn't part of the chain. Likewise, a dead
		 * line pointer can't be part of the chain.  Both of those cases were
		 * already marked as processed.
		 */
		Assert(ItemIdIsUsed(lp));
		Assert(!ItemIdIsDead(lp));

		/*
		 * If we are looking at the redirected root line pointer, jump to the
		 * first normal tuple in the chain.  If we find a redirect somewhere
		 * else, stop --- it must not be same chain.
		 */
		if (ItemIdIsRedirected(lp))
		{
			if (nchain > 0)
				break;			/* not at start of chain */
			chainitems[nchain++] = offnum;
			offnum = ItemIdGetRedirect(rootlp);
			continue;
		}

		Assert(ItemIdIsNormal(lp));

		htup = (HeapTupleHeader) PageGetItem(page, lp);

		/*
		 * Check the tuple XMIN against prior XMAX, if any
		 */
		if (TransactionIdIsValid(priorXmax) &&
			!TransactionIdEquals(HeapTupleHeaderGetXmin(htup), priorXmax))
			break;

		/*
		 * OK, this tuple is indeed a member of the chain.
		 */
		chainitems[nchain++] = offnum;

		/*
		 * Check tuple's visibility status.
		 */
		switch (htsv_get_valid_status(htsv[offnum]))
		{
			case HEAPTUPLE_DEAD:

				/* Remember the last DEAD tuple seen */
				ndeadchain = nchain;
				HeapTupleHeaderAdvanceConflictHorizon(htup,
													  &prstate->snapshotConflictHorizon);

				/* Advance to next chain member */
				break;

			case HEAPTUPLE_RECENTLY_DEAD:

				/*
				 * This tuple may soon become DEAD.  Update the hint field so
				 * that the page is reconsidered for pruning in future.
				 *
				 * We don't need to advance the conflict horizon for
				 * RECENTLY_DEAD tuples, even if we are removing them.  This
				 * is because we only remove RECENTLY_DEAD tuples if they
				 * precede a DEAD tuple, and the DEAD tuple must have been
				 * inserted by a newer transaction than the RECENTLY_DEAD
				 * tuple by virtue of being later in the chain.  We will have
				 * advanced the conflict horizon for the DEAD tuple.
				 */
				heap_prune_record_prunable(prstate,
										   HeapTupleHeaderGetUpdateXid(htup));

				/*
				 * Advance past RECENTLY_DEAD tuples just in case there's a
				 * DEAD one after them.  We have to make sure that we don't
				 * miss any DEAD tuples, since DEAD tuples that still have
				 * tuple storage after pruning will confuse VACUUM.
				 */
				break;

			case HEAPTUPLE_DELETE_IN_PROGRESS:

				/*
				 * This tuple may soon become DEAD.  Update the hint field so
				 * that the page is reconsidered for pruning in future.
				 */
				heap_prune_record_prunable(prstate,
										   HeapTupleHeaderGetUpdateXid(htup));
				goto process_chain;

			case HEAPTUPLE_LIVE:
			case HEAPTUPLE_INSERT_IN_PROGRESS:

				/*
				 * If we wanted to optimize for aborts, we might consider
				 * marking the page prunable when we see INSERT_IN_PROGRESS.
				 * But we don't.  See related decisions about when to mark the
				 * page prunable in heapam.c.
				 */
				goto process_chain;

			default:
				elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
				goto process_chain;
		}

		/*
		 * If the tuple is not HOT-updated, then we are at the end of this
		 * HOT-update chain.
		 */
		if (!HeapTupleHeaderIsHotUpdated(htup))
			goto process_chain;

		/* HOT implies it can't have moved to different partition */
		Assert(!HeapTupleHeaderIndicatesMovedPartitions(htup));

		/*
		 * Advance to next chain member.
		 */
		Assert(ItemPointerGetBlockNumber(&htup->t_ctid) == blockno);
		offnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
		priorXmax = HeapTupleHeaderGetUpdateXid(htup);
	}

	if (ItemIdIsRedirected(rootlp) && nchain < 2)
	{
		/*
		 * We found a redirect item that doesn't point to a valid follow-on
		 * item.  This can happen if the loop in heap_page_prune caused us to
		 * visit the dead successor of a redirect item before visiting the
		 * redirect item.  We can clean up by setting the redirect item to
		 * LP_DEAD state or LP_UNUSED if the caller indicated.
		 */
		heap_prune_record_dead_or_unused(prstate, rootoffnum, false);
		return;
	}

process_chain:

	if (ndeadchain == 0)
	{
		/*
		 * No DEAD tuple was found, so the chain is entirely composed of
		 * normal, unchanged tuples.  Leave it alone.
		 */
		for (int i = 0; i < nchain; i++)
			heap_prune_record_unchanged(prstate, chainitems[i]);
	}
	else if (ndeadchain == nchain)
	{
		/*
		 * The entire chain is dead.  Mark the root line pointer LP_DEAD, and
		 * fully remove the other tuples in the chain.
		 */
		heap_prune_record_dead_or_unused(prstate, rootoffnum, ItemIdIsNormal(rootlp));
		for (int i = 1; i < nchain; i++)
			heap_prune_record_unused(prstate, chainitems[i], true);
	}
	else
	{
		/*
		 * We found a DEAD tuple in the chain.  Redirect the root line pointer
		 * to the first non-DEAD tuple, and mark as unused each intermediate
		 * item that we are able to remove from the chain.
		 */
		heap_prune_record_redirect(prstate, rootoffnum, chainitems[ndeadchain],
								   ItemIdIsNormal(rootlp));
		for (int i = 1; i < ndeadchain; i++)
			heap_prune_record_unused(prstate, chainitems[i], true);

		/* the rest of tuples in the chain are normal, unchanged tuples */
		for (int i = ndeadchain; i < nchain; i++)
			heap_prune_record_unchanged(prstate, chainitems[i]);
	}
}

/* Record lowest soon-prunable XID */
static void
heap_prune_record_prunable(PruneState *prstate, TransactionId xid)
{
	/*
	 * This should exactly match the PageSetPrunable macro.  We can't store
	 * directly into the page header yet, so we update working state.
	 */
	Assert(TransactionIdIsNormal(xid));
	if (!TransactionIdIsValid(prstate->new_prune_xid) ||
		TransactionIdPrecedes(xid, prstate->new_prune_xid))
		prstate->new_prune_xid = xid;
}

/* Record line pointer to be redirected */
static void
heap_prune_record_redirect(PruneState *prstate,
						   OffsetNumber offnum, OffsetNumber rdoffnum,
						   bool was_normal)
{
	Assert(!prstate->processed[offnum]);
	prstate->processed[offnum] = true;

	/*
	 * Do not mark the redirect target here.  It needs to be counted
	 * separately as an unchanged tuple.
	 */

	Assert(prstate->nredirected < MaxHeapTuplesPerPage);
	prstate->redirected[prstate->nredirected * 2] = offnum;
	prstate->redirected[prstate->nredirected * 2 + 1] = rdoffnum;

	prstate->nredirected++;

	/*
	 * If the root entry had been a normal tuple, we are deleting it, so count
	 * it in the result.  But changing a redirect (even to DEAD state) doesn't
	 * count.
	 */
	if (was_normal)
		prstate->ndeleted++;
}

/* Record line pointer to be marked dead */
static void
heap_prune_record_dead(PruneState *prstate, OffsetNumber offnum,
					   bool was_normal)
{
	Assert(!prstate->processed[offnum]);
	prstate->processed[offnum] = true;

	Assert(prstate->ndead < MaxHeapTuplesPerPage);
	prstate->nowdead[prstate->ndead] = offnum;
	prstate->ndead++;

	/*
	 * If the root entry had been a normal tuple, we are deleting it, so count
	 * it in the result.  But changing a redirect (even to DEAD state) doesn't
	 * count.
	 */
	if (was_normal)
		prstate->ndeleted++;
}

/*
 * Depending on whether or not the caller set mark_unused_now to true, record that a
 * line pointer should be marked LP_DEAD or LP_UNUSED. There are other cases in
 * which we will mark line pointers LP_UNUSED, but we will not mark line
 * pointers LP_DEAD if mark_unused_now is true.
 */
static void
heap_prune_record_dead_or_unused(PruneState *prstate, OffsetNumber offnum,
								 bool was_normal)
{
	/*
	 * If the caller set mark_unused_now to true, we can remove dead tuples
	 * during pruning instead of marking their line pointers dead. Set this
	 * tuple's line pointer LP_UNUSED. We hint that this option is less
	 * likely.
	 */
	if (unlikely(prstate->mark_unused_now))
		heap_prune_record_unused(prstate, offnum, was_normal);
	else
		heap_prune_record_dead(prstate, offnum, was_normal);
}

/* Record line pointer to be marked unused */
static void
heap_prune_record_unused(PruneState *prstate, OffsetNumber offnum, bool was_normal)
{
	Assert(!prstate->processed[offnum]);
	prstate->processed[offnum] = true;

	Assert(prstate->nunused < MaxHeapTuplesPerPage);
	prstate->nowunused[prstate->nunused] = offnum;
	prstate->nunused++;

	/*
	 * If the root entry had been a normal tuple, we are deleting it, so count
	 * it in the result.  But changing a redirect (even to DEAD state) doesn't
	 * count.
	 */
	if (was_normal)
		prstate->ndeleted++;
}

/* Record a line pointer that is left unchanged */
static void
heap_prune_record_unchanged(PruneState *prstate, OffsetNumber offnum)
{
	Assert(!prstate->processed[offnum]);
	prstate->processed[offnum] = true;
}

/*
 * Perform the actual page changes needed by heap_page_prune.
 *
 * If 'lp_truncate_only' is set, we are merely marking LP_DEAD line pointers
 * as unused, not redirecting or removing anything else.  The
 * PageRepairFragmentation() call is skipped in that case.
 *
 * If 'lp_truncate_only' is not set, the caller must hold a cleanup lock on
 * the buffer.  If it is set, an ordinary exclusive lock suffices.
 */
void
heap_page_prune_execute(Buffer buffer, bool lp_truncate_only,
						OffsetNumber *redirected, int nredirected,
						OffsetNumber *nowdead, int ndead,
						OffsetNumber *nowunused, int nunused)
{
	Page		page = (Page) BufferGetPage(buffer);
	OffsetNumber *offnum;
	HeapTupleHeader htup PG_USED_FOR_ASSERTS_ONLY;

	/* Shouldn't be called unless there's something to do */
	Assert(nredirected > 0 || ndead > 0 || nunused > 0);

	/* If 'lp_truncate_only', we can only remove already-dead line pointers */
	Assert(!lp_truncate_only || (nredirected == 0 && ndead == 0));

	/* Update all redirected line pointers */
	offnum = redirected;
	for (int i = 0; i < nredirected; i++)
	{
		OffsetNumber fromoff = *offnum++;
		OffsetNumber tooff = *offnum++;
		ItemId		fromlp = PageGetItemId(page, fromoff);
		ItemId		tolp PG_USED_FOR_ASSERTS_ONLY;

#ifdef USE_ASSERT_CHECKING

		/*
		 * Any existing item that we set as an LP_REDIRECT (any 'from' item)
		 * must be the first item from a HOT chain.  If the item has tuple
		 * storage then it can't be a heap-only tuple.  Otherwise we are just
		 * maintaining an existing LP_REDIRECT from an existing HOT chain that
		 * has been pruned at least once before now.
		 */
		if (!ItemIdIsRedirected(fromlp))
		{
			Assert(ItemIdHasStorage(fromlp) && ItemIdIsNormal(fromlp));

			htup = (HeapTupleHeader) PageGetItem(page, fromlp);
			Assert(!HeapTupleHeaderIsHeapOnly(htup));
		}
		else
		{
			/* We shouldn't need to redundantly set the redirect */
			Assert(ItemIdGetRedirect(fromlp) != tooff);
		}

		/*
		 * The item that we're about to set as an LP_REDIRECT (the 'from'
		 * item) will point to an existing item (the 'to' item) that is
		 * already a heap-only tuple.  There can be at most one LP_REDIRECT
		 * item per HOT chain.
		 *
		 * We need to keep around an LP_REDIRECT item (after original
		 * non-heap-only root tuple gets pruned away) so that it's always
		 * possible for VACUUM to easily figure out what TID to delete from
		 * indexes when an entire HOT chain becomes dead.  A heap-only tuple
		 * can never become LP_DEAD; an LP_REDIRECT item or a regular heap
		 * tuple can.
		 *
		 * This check may miss problems, e.g. the target of a redirect could
		 * be marked as unused subsequently. The page_verify_redirects() check
		 * below will catch such problems.
		 */
		tolp = PageGetItemId(page, tooff);
		Assert(ItemIdHasStorage(tolp) && ItemIdIsNormal(tolp));
		htup = (HeapTupleHeader) PageGetItem(page, tolp);
		Assert(HeapTupleHeaderIsHeapOnly(htup));
#endif

		ItemIdSetRedirect(fromlp, tooff);
	}

	/* Update all now-dead line pointers */
	offnum = nowdead;
	for (int i = 0; i < ndead; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);

#ifdef USE_ASSERT_CHECKING

		/*
		 * An LP_DEAD line pointer must be left behind when the original item
		 * (which is dead to everybody) could still be referenced by a TID in
		 * an index.  This should never be necessary with any individual
		 * heap-only tuple item, though. (It's not clear how much of a problem
		 * that would be, but there is no reason to allow it.)
		 */
		if (ItemIdHasStorage(lp))
		{
			Assert(ItemIdIsNormal(lp));
			htup = (HeapTupleHeader) PageGetItem(page, lp);
			Assert(!HeapTupleHeaderIsHeapOnly(htup));
		}
		else
		{
			/* Whole HOT chain becomes dead */
			Assert(ItemIdIsRedirected(lp));
		}
#endif

		ItemIdSetDead(lp);
	}

	/* Update all now-unused line pointers */
	offnum = nowunused;
	for (int i = 0; i < nunused; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);

#ifdef USE_ASSERT_CHECKING

		if (lp_truncate_only)
		{
			/* Setting LP_DEAD to LP_UNUSED in vacuum's second pass */
			Assert(ItemIdIsDead(lp) && !ItemIdHasStorage(lp));
		}
		else
		{
			/*
			 * When heap_page_prune() was called, mark_unused_now may have
			 * been passed as true, which allows would-be LP_DEAD items to be
			 * made LP_UNUSED instead.  This is only possible if the relation
			 * has no indexes.  If there are any dead items, then
			 * mark_unused_now was not true and every item being marked
			 * LP_UNUSED must refer to a heap-only tuple.
			 */
			if (ndead > 0)
			{
				Assert(ItemIdHasStorage(lp) && ItemIdIsNormal(lp));
				htup = (HeapTupleHeader) PageGetItem(page, lp);
				Assert(HeapTupleHeaderIsHeapOnly(htup));
			}
			else
				Assert(ItemIdIsUsed(lp));
		}

#endif

		ItemIdSetUnused(lp);
	}

	if (lp_truncate_only)
		PageTruncateLinePointerArray(page);
	else
	{
		/*
		 * Finally, repair any fragmentation, and update the page's hint bit
		 * about whether it has free pointers.
		 */
		PageRepairFragmentation(page);

		/*
		 * Now that the page has been modified, assert that redirect items
		 * still point to valid targets.
		 */
		page_verify_redirects(page);
	}
}


/*
 * If built with assertions, verify that all LP_REDIRECT items point to a
 * valid item.
 *
 * One way that bugs related to HOT pruning show is redirect items pointing to
 * removed tuples. It's not trivial to reliably check that marking an item
 * unused will not orphan a redirect item during heap_prune_chain() /
 * heap_page_prune_execute(), so we additionally check the whole page after
 * pruning. Without this check such bugs would typically only cause asserts
 * later, potentially well after the corruption has been introduced.
 *
 * Also check comments in heap_page_prune_execute()'s redirection loop.
 */
static void
page_verify_redirects(Page page)
{
#ifdef USE_ASSERT_CHECKING
	OffsetNumber offnum;
	OffsetNumber maxoff;

	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid = PageGetItemId(page, offnum);
		OffsetNumber targoff;
		ItemId		targitem;
		HeapTupleHeader htup;

		if (!ItemIdIsRedirected(itemid))
			continue;

		targoff = ItemIdGetRedirect(itemid);
		targitem = PageGetItemId(page, targoff);

		Assert(ItemIdIsUsed(targitem));
		Assert(ItemIdIsNormal(targitem));
		Assert(ItemIdHasStorage(targitem));
		htup = (HeapTupleHeader) PageGetItem(page, targitem);
		Assert(HeapTupleHeaderIsHeapOnly(htup));
	}
#endif
}


/*
 * For all items in this page, find their respective root line pointers.
 * If item k is part of a HOT-chain with root at item j, then we set
 * root_offsets[k - 1] = j.
 *
 * The passed-in root_offsets array must have MaxHeapTuplesPerPage entries.
 * Unused entries are filled with InvalidOffsetNumber (zero).
 *
 * The function must be called with at least share lock on the buffer, to
 * prevent concurrent prune operations.
 *
 * Note: The information collected here is valid only as long as the caller
 * holds a pin on the buffer. Once pin is released, a tuple might be pruned
 * and reused by a completely unrelated tuple.
 */
void
heap_get_root_tuples(Page page, OffsetNumber *root_offsets)
{
	OffsetNumber offnum,
				maxoff;

	MemSet(root_offsets, InvalidOffsetNumber,
		   MaxHeapTuplesPerPage * sizeof(OffsetNumber));

	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum))
	{
		ItemId		lp = PageGetItemId(page, offnum);
		HeapTupleHeader htup;
		OffsetNumber nextoffnum;
		TransactionId priorXmax;

		/* skip unused and dead items */
		if (!ItemIdIsUsed(lp) || ItemIdIsDead(lp))
			continue;

		if (ItemIdIsNormal(lp))
		{
			htup = (HeapTupleHeader) PageGetItem(page, lp);

			/*
			 * Check if this tuple is part of a HOT-chain rooted at some other
			 * tuple. If so, skip it for now; we'll process it when we find
			 * its root.
			 */
			if (HeapTupleHeaderIsHeapOnly(htup))
				continue;

			/*
			 * This is either a plain tuple or the root of a HOT-chain.
			 * Remember it in the mapping.
			 */
			root_offsets[offnum - 1] = offnum;

			/* If it's not the start of a HOT-chain, we're done with it */
			if (!HeapTupleHeaderIsHotUpdated(htup))
				continue;

			/* Set up to scan the HOT-chain */
			nextoffnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
			priorXmax = HeapTupleHeaderGetUpdateXid(htup);
		}
		else
		{
			/* Must be a redirect item. We do not set its root_offsets entry */
			Assert(ItemIdIsRedirected(lp));
			/* Set up to scan the HOT-chain */
			nextoffnum = ItemIdGetRedirect(lp);
			priorXmax = InvalidTransactionId;
		}

		/*
		 * Now follow the HOT-chain and collect other tuples in the chain.
		 *
		 * Note: Even though this is a nested loop, the complexity of the
		 * function is O(N) because a tuple in the page should be visited not
		 * more than twice, once in the outer loop and once in HOT-chain
		 * chases.
		 */
		for (;;)
		{
			/* Sanity check (pure paranoia) */
			if (offnum < FirstOffsetNumber)
				break;

			/*
			 * An offset past the end of page's line pointer array is possible
			 * when the array was truncated
			 */
			if (offnum > maxoff)
				break;

			lp = PageGetItemId(page, nextoffnum);

			/* Check for broken chains */
			if (!ItemIdIsNormal(lp))
				break;

			htup = (HeapTupleHeader) PageGetItem(page, lp);

			if (TransactionIdIsValid(priorXmax) &&
				!TransactionIdEquals(priorXmax, HeapTupleHeaderGetXmin(htup)))
				break;

			/* Remember the root line pointer for this item */
			root_offsets[nextoffnum - 1] = offnum;

			/* Advance to next chain member, if any */
			if (!HeapTupleHeaderIsHotUpdated(htup))
				break;

			/* HOT implies it can't have moved to different partition */
			Assert(!HeapTupleHeaderIndicatesMovedPartitions(htup));

			nextoffnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
			priorXmax = HeapTupleHeaderGetUpdateXid(htup);
		}
	}
}


/*
 * Compare fields that describe actions required to freeze tuple with caller's
 * open plan.  If everything matches then the frz tuple plan is equivalent to
 * caller's plan.
 */
static inline bool
heap_log_freeze_eq(xlhp_freeze_plan *plan, HeapTupleFreeze *frz)
{
	if (plan->xmax == frz->xmax &&
		plan->t_infomask2 == frz->t_infomask2 &&
		plan->t_infomask == frz->t_infomask &&
		plan->frzflags == frz->frzflags)
		return true;

	/* Caller must call heap_log_freeze_new_plan again for frz */
	return false;
}

/*
 * Comparator used to deduplicate XLOG_HEAP2_FREEZE_PAGE freeze plans
 */
static int
heap_log_freeze_cmp(const void *arg1, const void *arg2)
{
	HeapTupleFreeze *frz1 = (HeapTupleFreeze *) arg1;
	HeapTupleFreeze *frz2 = (HeapTupleFreeze *) arg2;

	if (frz1->xmax < frz2->xmax)
		return -1;
	else if (frz1->xmax > frz2->xmax)
		return 1;

	if (frz1->t_infomask2 < frz2->t_infomask2)
		return -1;
	else if (frz1->t_infomask2 > frz2->t_infomask2)
		return 1;

	if (frz1->t_infomask < frz2->t_infomask)
		return -1;
	else if (frz1->t_infomask > frz2->t_infomask)
		return 1;

	if (frz1->frzflags < frz2->frzflags)
		return -1;
	else if (frz1->frzflags > frz2->frzflags)
		return 1;

	/*
	 * heap_log_freeze_eq would consider these tuple-wise plans to be equal.
	 * (So the tuples will share a single canonical freeze plan.)
	 *
	 * We tiebreak on page offset number to keep each freeze plan's page
	 * offset number array individually sorted. (Unnecessary, but be tidy.)
	 */
	if (frz1->offset < frz2->offset)
		return -1;
	else if (frz1->offset > frz2->offset)
		return 1;

	Assert(false);
	return 0;
}

/*
 * Start new plan initialized using tuple-level actions.  At least one tuple
 * will have steps required to freeze described by caller's plan during REDO.
 */
static inline void
heap_log_freeze_new_plan(xlhp_freeze_plan *plan, HeapTupleFreeze *frz)
{
	plan->xmax = frz->xmax;
	plan->t_infomask2 = frz->t_infomask2;
	plan->t_infomask = frz->t_infomask;
	plan->frzflags = frz->frzflags;
	plan->ntuples = 1;			/* for now */
}

/*
 * Deduplicate tuple-based freeze plans so that each distinct set of
 * processing steps is only stored once in XLOG_HEAP2_FREEZE_PAGE records.
 * Called during original execution of freezing (for logged relations).
 *
 * Return value is number of plans set in *plans_out for caller.  Also writes
 * an array of offset numbers into *offsets_out output argument for caller
 * (actually there is one array per freeze plan, but that's not of immediate
 * concern to our caller).
 */
static int
heap_log_freeze_plan(HeapTupleFreeze *tuples, int ntuples,
					 xlhp_freeze_plan *plans_out,
					 OffsetNumber *offsets_out)
{
	int			nplans = 0;

	/* Sort tuple-based freeze plans in the order required to deduplicate */
	qsort(tuples, ntuples, sizeof(HeapTupleFreeze), heap_log_freeze_cmp);

	for (int i = 0; i < ntuples; i++)
	{
		HeapTupleFreeze *frz = tuples + i;

		if (i == 0)
		{
			/* New canonical freeze plan starting with first tup */
			heap_log_freeze_new_plan(plans_out, frz);
			nplans++;
		}
		else if (heap_log_freeze_eq(plans_out, frz))
		{
			/* tup matches open canonical plan -- include tup in it */
			Assert(offsets_out[i - 1] < frz->offset);
			plans_out->ntuples++;
		}
		else
		{
			/* Tup doesn't match current plan -- done with it now */
			plans_out++;

			/* New canonical freeze plan starting with this tup */
			heap_log_freeze_new_plan(plans_out, frz);
			nplans++;
		}

		/*
		 * Save page offset number in dedicated buffer in passing.
		 *
		 * REDO routine relies on the record's offset numbers array grouping
		 * offset numbers by freeze plan.  The sort order within each grouping
		 * is ascending offset number order, just to keep things tidy.
		 */
		offsets_out[i] = frz->offset;
	}

	Assert(nplans > 0 && nplans <= ntuples);

	return nplans;
}

/*
 * Write an XLOG_HEAP2_PRUNE_FREEZE WAL record
 *
 * This is used for several different page maintenance operations:
 *
 * - Page pruning, in VACUUM's 1st pass or on access: Some items are
 *   redirected, some marked dead, and some removed altogether.
 *
 * - Freezing: Items are marked as 'frozen'.
 *
 * - Vacuum, 2nd pass: Items that are already LP_DEAD are marked as unused.
 *
 * They have enough commonalities that we use a single WAL record for them
 * all.
 *
 * If replaying the record requires a cleanup lock, pass cleanup_lock = true.
 * Replaying 'redirected' or 'dead' items always requires a cleanup lock, but
 * replaying 'unused' items depends on whether they were all previously marked
 * as dead.
 *
 * Note: This function scribbles on the 'frozen' array.
 *
 * Note: This is called in a critical section, so careful what you do here.
 */
void
log_heap_prune_and_freeze(Relation relation, Buffer buffer,
						  TransactionId conflict_xid,
						  bool cleanup_lock,
						  PruneReason reason,
						  HeapTupleFreeze *frozen, int nfrozen,
						  OffsetNumber *redirected, int nredirected,
						  OffsetNumber *dead, int ndead,
						  OffsetNumber *unused, int nunused)
{
	xl_heap_prune xlrec;
	XLogRecPtr	recptr;
	uint8		info;

	/* The following local variables hold data registered in the WAL record: */
	xlhp_freeze_plan plans[MaxHeapTuplesPerPage];
	xlhp_freeze_plans freeze_plans;
	xlhp_prune_items redirect_items;
	xlhp_prune_items dead_items;
	xlhp_prune_items unused_items;
	OffsetNumber frz_offsets[MaxHeapTuplesPerPage];

	xlrec.flags = 0;

	/*
	 * Prepare data for the buffer.  The arrays are not actually in the
	 * buffer, but we pretend that they are.  When XLogInsert stores a full
	 * page image, the arrays can be omitted.
	 */
	XLogBeginInsert();
	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);
	if (nfrozen > 0)
	{
		int			nplans;

		xlrec.flags |= XLHP_HAS_FREEZE_PLANS;

		/*
		 * Prepare deduplicated representation for use in the WAL record. This
		 * destructively sorts frozen tuples array in-place.
		 */
		nplans = heap_log_freeze_plan(frozen, nfrozen, plans, frz_offsets);

		freeze_plans.nplans = nplans;
		XLogRegisterBufData(0, (char *) &freeze_plans,
							offsetof(xlhp_freeze_plans, plans));
		XLogRegisterBufData(0, (char *) plans,
							sizeof(xlhp_freeze_plan) * nplans);
	}
	if (nredirected > 0)
	{
		xlrec.flags |= XLHP_HAS_REDIRECTIONS;

		redirect_items.ntargets = nredirected;
		XLogRegisterBufData(0, (char *) &redirect_items,
							offsetof(xlhp_prune_items, data));
		XLogRegisterBufData(0, (char *) redirected,
							sizeof(OffsetNumber[2]) * nredirected);
	}
	if (ndead > 0)
	{
		xlrec.flags |= XLHP_HAS_DEAD_ITEMS;

		dead_items.ntargets = ndead;
		XLogRegisterBufData(0, (char *) &dead_items,
							offsetof(xlhp_prune_items, data));
		XLogRegisterBufData(0, (char *) dead,
							sizeof(OffsetNumber) * ndead);
	}
	if (nunused > 0)
	{
		xlrec.flags |= XLHP_HAS_NOW_UNUSED_ITEMS;

		unused_items.ntargets = nunused;
		XLogRegisterBufData(0, (char *) &unused_items,
							offsetof(xlhp_prune_items, data));
		XLogRegisterBufData(0, (char *) unused,
							sizeof(OffsetNumber) * nunused);
	}
	if (nfrozen > 0)
		XLogRegisterBufData(0, (char *) frz_offsets,
							sizeof(OffsetNumber) * nfrozen);

	/*
	 * Prepare the main xl_heap_prune record.  We already set the XLPH_HAS_*
	 * flag above.
	 */
	if (RelationIsAccessibleInLogicalDecoding(relation))
		xlrec.flags |= XLHP_IS_CATALOG_REL;
	if (TransactionIdIsValid(conflict_xid))
		xlrec.flags |= XLHP_HAS_CONFLICT_HORIZON;
	if (cleanup_lock)
		xlrec.flags |= XLHP_CLEANUP_LOCK;
	else
	{
		Assert(nredirected == 0 && ndead == 0);
		/* also, any items in 'unused' must've been LP_DEAD previously */
	}
	XLogRegisterData((char *) &xlrec, SizeOfHeapPrune);
	if (TransactionIdIsValid(conflict_xid))
		XLogRegisterData((char *) &conflict_xid, sizeof(TransactionId));

	switch (reason)
	{
		case PRUNE_ON_ACCESS:
			info = XLOG_HEAP2_PRUNE_ON_ACCESS;
			break;
		case PRUNE_VACUUM_SCAN:
			info = XLOG_HEAP2_PRUNE_VACUUM_SCAN;
			break;
		case PRUNE_VACUUM_CLEANUP:
			info = XLOG_HEAP2_PRUNE_VACUUM_CLEANUP;
			break;
		default:
			elog(ERROR, "unrecognized prune reason: %d", (int) reason);
			break;
	}
	recptr = XLogInsert(RM_HEAP2_ID, info);

	PageSetLSN(BufferGetPage(buffer), recptr);
}
