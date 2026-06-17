/*-------------------------------------------------------------------------
 *
 * heapam_indexscan.c
 *	  heap table plain index scan and index-only scan code
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/heapam_indexscan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/hot_indexed.h"
#include "access/relscan.h"
#include "miscadmin.h"
#include "storage/predicate.h"


/* ------------------------------------------------------------------------
 * Index Scan Callbacks for heap AM
 * ------------------------------------------------------------------------
 */

IndexFetchTableData *
heapam_index_fetch_begin(Relation rel, uint32 flags)
{
	IndexFetchHeapData *hscan = palloc0_object(IndexFetchHeapData);

	hscan->xs_base.rel = rel;
	hscan->xs_base.flags = flags;
	hscan->xs_cbuf = InvalidBuffer;
	hscan->xs_blk = InvalidBlockNumber;
	hscan->xs_vmbuffer = InvalidBuffer;

	/*
	 * Scratch space for the union of modified-attrs bitmaps that a HOT/SIU
	 * chain walk crosses, sized for this relation's column count.  Threaded
	 * back out through xs_hot_indexed_crossed for the index-access layer.
	 */
	hscan->xs_base.xs_hot_indexed_crossed =
		palloc0(HotIndexedBitmapBytes(RelationGetNumberOfAttributes(rel)));

	return &hscan->xs_base;
}

void
heapam_index_fetch_reset(IndexFetchTableData *scan)
{
	/*
	 * Resets are a no-op.
	 *
	 * Deliberately avoid dropping pins now held in xs_cbuf and xs_vmbuffer.
	 * This saves cycles during certain tight nested loop joins (it can avoid
	 * repeated pinning and unpinning of the same buffer across rescans).
	 */
}

void
heapam_index_fetch_end(IndexFetchTableData *scan)
{
	IndexFetchHeapData *hscan = (IndexFetchHeapData *) scan;

	/* drop pin if there's a pinned heap page */
	if (BufferIsValid(hscan->xs_cbuf))
		ReleaseBuffer(hscan->xs_cbuf);

	/* drop pin if there's a pinned visibility map page */
	if (BufferIsValid(hscan->xs_vmbuffer))
		ReleaseBuffer(hscan->xs_vmbuffer);

	if (hscan->xs_base.xs_hot_indexed_crossed != NULL)
		pfree(hscan->xs_base.xs_hot_indexed_crossed);

	pfree(hscan);
}

/*
 *	heap_hot_search_buffer	- search HOT chain for tuple satisfying snapshot
 *
 * On entry, *tid is the TID of a tuple (either a simple tuple, or the root
 * of a HOT chain), and buffer is the buffer holding this tuple.  We search
 * for the first chain member satisfying the given snapshot.  If one is
 * found, we update *tid to reference that tuple's offset number, and
 * return true.  If no match, return false without modifying *tid.
 *
 * heapTuple is a caller-supplied buffer.  When a match is found, we return
 * the tuple here, in addition to updating *tid.  If no match is found, the
 * contents of this buffer on return are undefined.
 *
 * If all_dead is not NULL, we check non-visible tuples to see if they are
 * globally dead; *all_dead is set true if all members of the HOT chain
 * are vacuumable, false if not.
 *
 * If hot_indexed_recheck is not NULL, *hot_indexed_recheck is set true iff the
 * walk crossed a HOT-selectively-updated (HOT/SIU) hop after the entry tuple
 * on the way to the returned tuple -- i.e. the arriving index entry's stored
 * key may no longer match the live tuple, so the caller must recheck it (via
 * a leaf-key comparison or a qual recheck).  The entry tuple's own producing
 * hop is excluded, so a fresh entry pointing directly at its tuple is not
 * flagged.  When no such hop was crossed, *hot_indexed_recheck is left false.
 *
 * Unlike heap_fetch, the caller must already have pin and (at least) share
 * lock on the buffer; it is still pinned/locked at exit.
 */
bool
heap_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer,
					   Snapshot snapshot, HeapTuple heapTuple,
					   bool *all_dead, bool first_call,
					   bool *hot_indexed_recheck,
					   uint8 *crossed_bitmap,
					   bool *prefix_all_dead)
{
	Page		page = BufferGetPage(buffer);
	TransactionId prev_xmax = InvalidTransactionId;
	BlockNumber blkno;
	OffsetNumber offnum;
	bool		at_chain_start;
	bool		valid;
	bool		skip;
	bool		prefix_dead;
	GlobalVisState *vistest = NULL;
	int			relnatts = RelationGetNumberOfAttributes(relation);
	int			chain_hops;
	OffsetNumber maxoff_guard;

	/* Only track prefix_dead when the caller actually wants it. */
	prefix_dead = (prefix_all_dead != NULL);

	/* If this is not the first call, previous call returned a (live!) tuple */
	if (all_dead)
		*all_dead = first_call;

	/*
	 * On the first call, clear the recheck flag and the crossed-attrs union.
	 * On subsequent calls (same chain continuing) keep whatever an earlier
	 * hop already accumulated.
	 */
	if (first_call)
	{
		if (hot_indexed_recheck)
			*hot_indexed_recheck = false;
		if (crossed_bitmap)
			memset(crossed_bitmap, 0, HotIndexedBitmapBytes(relnatts));
	}

	blkno = ItemPointerGetBlockNumber(tid);
	offnum = ItemPointerGetOffsetNumber(tid);
	at_chain_start = first_call;
	skip = !first_call;

	/* XXX: we should assert that a snapshot is pushed or registered */
	Assert(TransactionIdIsValid(RecentXmin));
	Assert(BufferGetBlockNumber(buffer) == blkno);

	/*
	 * Bound the HOT/SIU chain walk.  A corrupt page whose forward links form
	 * a cycle among valid in-range offsets (e.g. stub A -> stub B -> stub A)
	 * would otherwise spin this loop forever under a buffer share-lock, with
	 * no CHECK_FOR_INTERRUPTS to break out.  No legitimate chain visits more
	 * offsets than the page holds.
	 */
	chain_hops = 0;
	maxoff_guard = PageGetMaxOffsetNumber(page);

	/* Scan through possible multiple members of HOT-chain */
	for (;;)
	{
		ItemId		lp;

		CHECK_FOR_INTERRUPTS();
		if (chain_hops++ > maxoff_guard)
			break;			/* defend against a corrupt forward-link cycle */

		/* check for bogus TID */
		if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
			break;

		lp = PageGetItemId(page, offnum);

		/* check for unused, dead, or redirected items */
		if (!ItemIdIsNormal(lp))
		{
			/* We should only see a redirect at start of chain */
			if (ItemIdIsRedirected(lp) && at_chain_start)
			{
				/*
				 * Follow the redirect.  A collapsed dead prefix is preserved
				 * as a run of forwarding stubs, each carrying its segment's
				 * modified-attrs bitmap, ending at the first live tuple;
				 * chain collapse reclaims a dead member only when its
				 * attributes are a subset of the surviving later hops (see
				 * pruneheap.c).  So the stubs and live hops this walk crosses
				 * below contribute the complete union of every collapsed
				 * hop's modified attributes, and that union drives the
				 * overlap staleness test for the index-access layer.
				 */
				offnum = ItemIdGetRedirect(lp);
				at_chain_start = false;
				continue;
			}
			/* else must be end of chain */
			break;
		}

		/*
		 * Update heapTuple to point to the element of the HOT chain we're
		 * currently investigating. Having t_self set correctly is important
		 * because the SSI checks and the *Satisfies routine for historical
		 * MVCC snapshots need the correct tid to decide about the visibility.
		 */
		heapTuple->t_data = (HeapTupleHeader) PageGetItem(page, lp);
		heapTuple->t_len = ItemIdGetLength(lp);
		heapTuple->t_tableOid = RelationGetRelid(relation);
		ItemPointerSet(&heapTuple->t_self, blkno, offnum);

		/*
		 * A collapse-survivor stub is an LP_NORMAL item but not a real tuple:
		 * it is a freeze-safe forwarding node carrying the modified-attrs
		 * bitmap for the chain segment it represents.  Treat it like a
		 * crossed HOT/SIU hop -- arm the recheck and OR its bitmap into the
		 * crossed union (unless we arrived directly at it, in which case the
		 * arriving entry already reflects this segment's value) -- then
		 * follow its forward link.  A stub is never visible and never
		 * returned, and its forward link is a logical, not xid-continuous,
		 * edge, so reset prev_xmax to skip the chain-integrity check on the
		 * next member.
		 */
		if (HotIndexedHeaderIsStub(heapTuple->t_data))
		{
			if (!at_chain_start)
			{
				if (hot_indexed_recheck)
					*hot_indexed_recheck = true;
				if (crossed_bitmap)
				{
					int			bmnatts =
						HotIndexedTupleBitmapNatts(heapTuple->t_data);

					/*
					 * A hop's write-time natts can never legitimately exceed
					 * the relation's current natts.  On a corrupt page a
					 * stub's unbounded stashed natts could otherwise overflow
					 * crossed_bitmap, which is allocated for relnatts; clamp
					 * defensively.
					 */
					Assert(bmnatts >= 0 && bmnatts <= relnatts);
					if (bmnatts < 0 || bmnatts > relnatts)
						bmnatts = relnatts;

					HotIndexedBitmapUnion(crossed_bitmap,
										  HotIndexedGetModifiedBitmap(heapTuple->t_data,
																	  heapTuple->t_len,
																	  bmnatts),
										  bmnatts);
				}
			}
			offnum = HotIndexedStubGetForward(heapTuple->t_data);
			at_chain_start = false;
			prev_xmax = InvalidTransactionId;
			continue;
		}

		/*
		 * Shouldn't see a HEAP_ONLY tuple at chain start, unless that tuple
		 * is the target of a freshly-inserted hot-indexed index entry: then
		 * arriving directly at a heap-only HOT-indexed tuple is legal and the
		 * tuple is the canonical visible version, so we fall through and
		 * apply normal visibility checks to it.  Otherwise, treat it as a
		 * broken chain.
		 */
		if (at_chain_start && HeapTupleIsHeapOnly(heapTuple))
		{
			if ((heapTuple->t_data->t_infomask2 & HEAP_INDEXED_UPDATED) == 0)
				break;

			/*
			 * We were pointed directly at this hot-indexed tuple.  The index
			 * entry we arrived through was inserted *for* this update, so it
			 * reflects this tuple's current attribute values; its own
			 * producing hop is not a crossed hop, so it is not flagged for
			 * recheck (a fresh entry is never stale for its own index).
			 */
		}
		else if (hot_indexed_recheck != NULL &&
				 (heapTuple->t_data->t_infomask2 & HEAP_INDEXED_UPDATED) != 0)
		{
			/*
			 * A HOT/SIU hop reached by following the chain (or a redirect)
			 * from an earlier entry: this hop is crossed, so the arriving
			 * entry's stored key may no longer match the live tuple.  Set the
			 * recheck flag to tell the index-access layer to consult the
			 * crossed-attrs union; that union (accumulated below) is what
			 * decides staleness.
			 */
			*hot_indexed_recheck = true;

			/*
			 * Accumulate this hop's modified-attrs bitmap into the crossed
			 * union.  A tuple's inline bitmap records the indexed attributes
			 * that changed at the hop INTO it, which is exactly the hop we
			 * just crossed by advancing to it; ORing each crossed hop yields
			 * the indexed attributes that changed after the entry's own
			 * tuple.
			 */
			if (crossed_bitmap)
			{
				int			bmnatts =
					HotIndexedTupleBitmapNatts(heapTuple->t_data);

				/* See the comment on the stub case's crossed_bitmap use. */
				Assert(bmnatts >= 0 && bmnatts <= relnatts);
				if (bmnatts < 0 || bmnatts > relnatts)
					bmnatts = relnatts;

				HotIndexedBitmapUnion(crossed_bitmap,
									  HotIndexedGetModifiedBitmap(heapTuple->t_data,
																  heapTuple->t_len,
																  bmnatts),
									  bmnatts);
			}
		}

		/*
		 * The xmin should match the previous xmax value, else chain is
		 * broken.
		 */
		if (TransactionIdIsValid(prev_xmax) &&
			!TransactionIdEquals(prev_xmax,
								 HeapTupleHeaderGetXmin(heapTuple->t_data)))
			break;

		/*
		 * When first_call is true (and thus, skip is initially false) we'll
		 * return the first tuple we find.  But on later passes, heapTuple
		 * will initially be pointing to the tuple we returned last time.
		 * Returning it again would be incorrect (and would loop forever), so
		 * we skip it and return the next match we find.
		 */
		if (!skip)
		{
			/* If it's visible per the snapshot, we must return it */
			valid = HeapTupleSatisfiesVisibility(heapTuple, snapshot, buffer);
			HeapCheckForSerializableConflictOut(valid, relation, heapTuple,
												buffer, snapshot);

			if (valid)
			{
				ItemPointerSetOffsetNumber(tid, offnum);
				PredicateLockTID(relation, &heapTuple->t_self, snapshot,
								 HeapTupleHeaderGetXmin(heapTuple->t_data));
				if (all_dead)
					*all_dead = false;

				/*
				 * Report whether every chain member skipped before this
				 * visible tuple is dead to all transactions.  With a stale
				 * verdict this lets the caller kill the arriving leaf safely.
				 */
				if (prefix_all_dead)
					*prefix_all_dead = prefix_dead;

				return true;
			}
		}
		skip = false;

		/*
		 * If we can't see it, maybe no one else can either.  At caller
		 * request, check whether all chain members are dead to all
		 * transactions.  The same surely-dead test feeds prefix_dead, which
		 * (unlike all_dead) is not reset when a visible tuple is found, so it
		 * records whether the members skipped ahead of the returned tuple are
		 * all dead to all -- the safe-to-kill-this-leaf condition.
		 *
		 * Note: if you change the criterion here for what is "dead", fix the
		 * planner's get_actual_variable_range() function to match.
		 */
		if ((all_dead && *all_dead) || (prefix_all_dead && prefix_dead))
		{
			if (!vistest)
				vistest = GlobalVisTestFor(relation);

			if (!HeapTupleIsSurelyDead(heapTuple, vistest))
			{
				if (all_dead)
					*all_dead = false;
				prefix_dead = false;
			}
		}

		/*
		 * Check to see if HOT chain continues past this tuple; if so fetch
		 * the next offnum and loop around.
		 */
		if (HeapTupleIsHotUpdated(heapTuple))
		{
			Assert(ItemPointerGetBlockNumber(&heapTuple->t_data->t_ctid) ==
				   blkno);
			offnum = ItemPointerGetOffsetNumber(&heapTuple->t_data->t_ctid);
			at_chain_start = false;
			prev_xmax = HeapTupleHeaderGetUpdateXid(heapTuple->t_data);
		}
		else
			break;				/* end of chain */

	}

	return false;
}

bool
heapam_index_fetch_tuple(struct IndexFetchTableData *scan,
						 ItemPointer tid,
						 Snapshot snapshot,
						 TupleTableSlot *slot,
						 bool *heap_continue, bool *all_dead)
{
	IndexFetchHeapData *hscan = (IndexFetchHeapData *) scan;
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;
	bool		got_heap_tuple;

	Assert(TTS_IS_BUFFERTUPLE(slot));

	/* We can skip the buffer-switching logic if we're on the same page. */
	if (hscan->xs_blk != ItemPointerGetBlockNumber(tid))
	{
		Assert(!*heap_continue);

		/* Remember this buffer's block number for next time */
		hscan->xs_blk = ItemPointerGetBlockNumber(tid);

		if (BufferIsValid(hscan->xs_cbuf))
			ReleaseBuffer(hscan->xs_cbuf);

		hscan->xs_cbuf = ReadBuffer(hscan->xs_base.rel, hscan->xs_blk);

		/*
		 * Prune page when it is pinned for the first time
		 */
		heap_page_prune_opt(hscan->xs_base.rel, hscan->xs_cbuf,
							&hscan->xs_vmbuffer,
							hscan->xs_base.flags & SO_HINT_REL_READ_ONLY);
	}

	Assert(BufferGetBlockNumber(hscan->xs_cbuf) == hscan->xs_blk);
	Assert(hscan->xs_blk == ItemPointerGetBlockNumber(tid));

	/* Obtain share-lock on the buffer so we can examine visibility */
	LockBuffer(hscan->xs_cbuf, BUFFER_LOCK_SHARE);
	got_heap_tuple = heap_hot_search_buffer(tid,
											hscan->xs_base.rel,
											hscan->xs_cbuf,
											snapshot,
											&bslot->base.tupdata,
											all_dead,
											!*heap_continue,
											&scan->xs_hot_indexed_recheck,
											scan->xs_hot_indexed_crossed,
											&scan->xs_prefix_all_dead);
	if (!got_heap_tuple)
	{
		scan->xs_hot_indexed_recheck = false;
		scan->xs_prefix_all_dead = false;
	}
	bslot->base.tupdata.t_self = *tid;
	LockBuffer(hscan->xs_cbuf, BUFFER_LOCK_UNLOCK);

	if (got_heap_tuple)
	{
		/*
		 * Only in a non-MVCC snapshot can more than one member of the HOT
		 * chain be visible.
		 */
		*heap_continue = !IsMVCCLikeSnapshot(snapshot);

		slot->tts_tableOid = RelationGetRelid(scan->rel);
		ExecStoreBufferHeapTuple(&bslot->base.tupdata, slot, hscan->xs_cbuf);
	}
	else
	{
		/* We've reached the end of the HOT chain. */
		*heap_continue = false;
	}

	return got_heap_tuple;
}
