/*-------------------------------------------------------------------------
 *
 * nbtdedup.c
 *	  Deduplicate items in Lehman and Yao btrees for Postgres.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtdedup.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/nbtree.h"
#include "access/nbtxlog.h"
#include "miscadmin.h"
#include "utils/rel.h"


/*
 * Try to deduplicate items to free at least enough space to avoid a page
 * split.  This function should be called during insertion, only after LP_DEAD
 * items were removed by _bt_vacuum_one_page() to prevent a page split.
 * (We'll have to kill LP_DEAD items here when the page's BTP_HAS_GARBAGE hint
 * was not set, but that should be rare.)
 *
 * The strategy for !checkingunique callers is to perform as much
 * deduplication as possible to free as much space as possible now, since
 * making it harder to set LP_DEAD bits is considered an acceptable price for
 * not having to deduplicate the same page many times.  It is unlikely that
 * the items on the page will have their LP_DEAD bit set in the future, since
 * that hasn't happened before now (besides, entire posting lists can still
 * have their LP_DEAD bit set).
 *
 * The strategy for checkingunique callers is rather different, since the
 * overall goal is different.  Deduplication cooperates with and enhances
 * garbage collection, especially the LP_DEAD bit setting that takes place in
 * _bt_check_unique().  Deduplication does as little as possible while still
 * preventing a page split for caller, since it's less likely that posting
 * lists will have their LP_DEAD bit set.  Deduplication avoids creating new
 * posting lists with only two heap TIDs, and also avoids creating new posting
 * lists from an existing posting list.  Deduplication is only useful when it
 * delays a page split long enough for garbage collection to prevent the page
 * split altogether.  checkingunique deduplication can make all the difference
 * in cases where VACUUM keeps up with dead index tuples, but "recently dead"
 * index tuples are still numerous enough to cause page splits that are truly
 * unnecessary.
 *
 * Note: If newitem contains NULL values in key attributes, caller will be
 * !checkingunique even when rel is a unique index.  The page in question will
 * usually have many existing items with NULLs.
 */
void
_bt_dedup_one_page(Relation rel, Buffer buffer, Relation heapRel,
				   IndexTuple newitem, Size newitemsz, bool checkingunique)
{
	OffsetNumber offnum,
				minoff,
				maxoff;
	Page		page = BufferGetPage(buffer);
	BTPageOpaque oopaque;
	BTDedupState *state = NULL;
	int			natts = IndexRelationGetNumberOfAttributes(rel);
	OffsetNumber deletable[MaxIndexTuplesPerPage];
	bool		minimal = checkingunique;
	int			ndeletable = 0;
	Size		pagesaving = 0;
	int			count = 0;
	bool		singlevalue = false;

	oopaque = (BTPageOpaque) PageGetSpecialPointer(page);
	/* init deduplication state needed to build posting tuples */
	state = (BTDedupState *) palloc(sizeof(BTDedupState));
	state->rel = rel;

	state->maxitemsize = BTMaxItemSize(page);
	state->newitem = newitem;
	state->checkingunique = checkingunique;
	state->skippedbase = InvalidOffsetNumber;
	/* Metadata about current pending posting list */
	state->htids = NULL;
	state->nhtids = 0;
	state->nitems = 0;
	state->alltupsize = 0;
	state->overlap = false;
	/* Metadata about based tuple of current pending posting list */
	state->base = NULL;
	state->baseoff = InvalidOffsetNumber;
	state->basetupsize = 0;

	minoff = P_FIRSTDATAKEY(oopaque);
	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * Delete dead tuples if any. We cannot simply skip them in the cycle
	 * below, because it's necessary to generate special Xlog record
	 * containing such tuples to compute latestRemovedXid on a standby server
	 * later.
	 *
	 * This should not affect performance, since it only can happen in a rare
	 * situation when BTP_HAS_GARBAGE flag was not set and _bt_vacuum_one_page
	 * was not called, or _bt_vacuum_one_page didn't remove all dead items.
	 */
	for (offnum = minoff;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid = PageGetItemId(page, offnum);

		if (ItemIdIsDead(itemid))
			deletable[ndeletable++] = offnum;
	}

	if (ndeletable > 0)
	{
		/*
		 * Skip duplication in rare cases where there were LP_DEAD items
		 * encountered here when that frees sufficient space for caller to
		 * avoid a page split
		 */
		_bt_delitems_delete(rel, buffer, deletable, ndeletable, heapRel);
		if (PageGetFreeSpace(page) >= newitemsz)
		{
			pfree(state);
			return;
		}

		/* Continue with deduplication */
		minoff = P_FIRSTDATAKEY(oopaque);
		maxoff = PageGetMaxOffsetNumber(page);
	}

	/* Make sure that new page won't have garbage flag set */
	oopaque->btpo_flags &= ~BTP_HAS_GARBAGE;

	/* Passed-in newitemsz is MAXALIGNED but does not include line pointer */
	newitemsz += sizeof(ItemIdData);
	/* Conservatively size array */
	state->htids = palloc(state->maxitemsize);

	/*
	 * Determine if a "single value" strategy page split is likely to occur
	 * shortly after deduplication finishes.  It should be possible for the
	 * single value split to find a split point that packs the left half of
	 * the split BTREE_SINGLEVAL_FILLFACTOR% full.
	 */
	if (!checkingunique)
	{
		ItemId		itemid;
		IndexTuple	itup;

		itemid = PageGetItemId(page, minoff);
		itup = (IndexTuple) PageGetItem(page, itemid);

		if (_bt_keep_natts_fast(rel, newitem, itup) > natts)
		{
			itemid = PageGetItemId(page, PageGetMaxOffsetNumber(page));
			itup = (IndexTuple) PageGetItem(page, itemid);

			/*
			 * Use different strategy if future page split likely to need to
			 * use "single value" strategy
			 */
			if (_bt_keep_natts_fast(rel, newitem, itup) > natts)
				singlevalue = true;
		}
	}

	/*
	 * Iterate over tuples on the page, try to deduplicate them into posting
	 * lists and insert into new page.  NOTE: It's essential to reassess the
	 * max offset on each iteration, since it will change as items are
	 * deduplicated.
	 */
	offnum = minoff;
retry:
	while (offnum <= PageGetMaxOffsetNumber(page))
	{
		ItemId		itemid = PageGetItemId(page, offnum);
		IndexTuple	itup = (IndexTuple) PageGetItem(page, itemid);

		Assert(!ItemIdIsDead(itemid));

		if (state->nitems == 0)
		{
			/*
			 * No previous/base tuple for the data item -- use the data item
			 * as base tuple of pending posting list
			 */
			_bt_dedup_start_pending(state, itup, offnum);
		}
		else if (_bt_keep_natts_fast(rel, state->base, itup) > natts &&
				 _bt_dedup_save_htid(state, itup))
		{
			/*
			 * Tuple is equal to base tuple of pending posting list.  Heap
			 * TID(s) for itup have been saved in state.  The next iteration
			 * will also end up here if it's possible to merge the next tuple
			 * into the same pending posting list.
			 */
		}
		else
		{
			/*
			 * Tuple is not equal to pending posting list tuple, or
			 * _bt_dedup_save_htid() opted to not merge current item into
			 * pending posting list for some other reason (e.g., adding more
			 * TIDs would have caused posting list to exceed BTMaxItemSize()
			 * limit).
			 *
			 * If state contains pending posting list with more than one item,
			 * form new posting tuple, and update the page.  Otherwise, reset
			 * the state and move on.
			 */
			pagesaving += _bt_dedup_finish_pending(buffer, state,
												   RelationNeedsWAL(rel));

			count++;

			/*
			 * When caller is a checkingunique caller and we have deduplicated
			 * enough to avoid a page split, do minimal deduplication in case
			 * the remaining items are about to be marked dead within
			 * _bt_check_unique().
			 */
			if (minimal && pagesaving >= newitemsz)
				break;

			/*
			 * Consider special steps when a future page split of the leaf
			 * page is likely to occur using nbtsplitloc.c's "single value"
			 * strategy
			 */
			if (singlevalue)
			{
				/*
				 * Adjust maxitemsize so that there isn't a third and final
				 * 1/3 of a page width tuple that fills the page to capacity.
				 * The third tuple produced should be smaller than the first
				 * two by an amount equal to the free space that nbtsplitloc.c
				 * is likely to want to leave behind when the page it split.
				 * When there are 3 posting lists on the page, then we end
				 * deduplication.  Remaining tuples on the page can be
				 * deduplicated later, when they're on the new right sibling
				 * of this page, and the new sibling page needs to be split in
				 * turn.
				 *
				 * Note that it doesn't matter if there are items on the page
				 * that were already 1/3 of a page during current pass;
				 * they'll still count as the first two posting list tuples.
				 */
				if (count == 2)
				{
					Size		leftfree;

					/* This calculation needs to match nbtsplitloc.c */
					leftfree = PageGetPageSize(page) - SizeOfPageHeaderData -
						MAXALIGN(sizeof(BTPageOpaqueData));
					/* Subtract predicted size of new high key */
					leftfree -= newitemsz + MAXALIGN(sizeof(ItemPointerData));

					/*
					 * Reduce maxitemsize by an amount equal to target free
					 * space on left half of page
					 */
					state->maxitemsize -= leftfree *
						((100 - BTREE_SINGLEVAL_FILLFACTOR) / 100.0);
				}
				else if (count == 3)
					break;
			}

			/*
			 * Next iteration starts immediately after base tuple offset (this
			 * will be the next offset on the page when we didn't modify the
			 * page)
			 */
			offnum = state->baseoff;
		}

		offnum = OffsetNumberNext(offnum);
	}

	/* Handle the last item when pending posting list is not empty */
	if (state->nitems != 0)
	{
		pagesaving += _bt_dedup_finish_pending(buffer, state,
											   RelationNeedsWAL(rel));
		count++;
	}

	if (pagesaving < newitemsz && state->skippedbase != InvalidOffsetNumber)
	{
		/*
		 * Didn't free enough space for new item in first checkingunique pass.
		 * Try making a second pass over the page, this time starting from the
		 * first candidate posting list base offset that was skipped over in
		 * the first pass (only do a second pass when this actually happened).
		 *
		 * The second pass over the page may deduplicate items that were
		 * initially passed over due to concerns about limiting the
		 * effectiveness of LP_DEAD bit setting within _bt_check_unique().
		 * Note that the second pass will still stop deduplicating as soon as
		 * enough space has been freed to avoid an immediate page split.
		 */
		Assert(state->checkingunique);
		offnum = state->skippedbase;

		state->checkingunique = false;
		state->skippedbase = InvalidOffsetNumber;
		state->alltupsize = 0;
		state->nitems = 0;
		state->base = NULL;
		state->baseoff = InvalidOffsetNumber;
		state->basetupsize = 0;
		goto retry;
	}

	/* Local space accounting should agree with page accounting */
	Assert(pagesaving < newitemsz || PageGetExactFreeSpace(page) >= newitemsz);

	/* be tidy */
	pfree(state->htids);
	pfree(state);
}

/*
 * Create a new pending posting list tuple based on caller's tuple.
 *
 * Every tuple processed by the deduplication routines either becomes the base
 * tuple for a posting list, or gets its heap TID(s) accepted into a pending
 * posting list.  A tuple that starts out as the base tuple for a posting list
 * will only actually be rewritten within _bt_dedup_finish_pending() when
 * there was at least one successful call to _bt_dedup_save_htid().
 */
void
_bt_dedup_start_pending(BTDedupState *state, IndexTuple base,
						OffsetNumber baseoff)
{
	Assert(state->nhtids == 0);
	Assert(state->nitems == 0);

	/*
	 * Copy heap TIDs from new base tuple for new candidate posting list into
	 * ipd array.  Assume that we'll eventually create a new posting tuple by
	 * merging later tuples with this existing one, though we may not.
	 */
	if (!BTreeTupleIsPosting(base))
	{
		memcpy(state->htids, base, sizeof(ItemPointerData));
		state->nhtids = 1;
		/* Save size of tuple without any posting list */
		state->basetupsize = IndexTupleSize(base);
	}
	else
	{
		int			nposting;

		nposting = BTreeTupleGetNPosting(base);
		memcpy(state->htids, BTreeTupleGetPosting(base),
			   sizeof(ItemPointerData) * nposting);
		state->nhtids = nposting;
		/* Save size of tuple without any posting list */
		state->basetupsize = BTreeTupleGetPostingOffset(base);
	}

	/*
	 * Save new base tuple itself -- it'll be needed if we actually create a
	 * new posting list from new pending posting list.
	 *
	 * Must maintain size of all tuples (including line pointer overhead) to
	 * calculate space savings on page within _bt_dedup_finish_pending().
	 * Also, save number of base tuple logical tuples so that we can save
	 * cycles in the common case where an existing posting list can't or won't
	 * be merged with other tuples on the page.
	 */
	state->nitems = 1;
	state->base = base;
	state->baseoff = baseoff;
	state->alltupsize = MAXALIGN(IndexTupleSize(base)) + sizeof(ItemIdData);
	/* Also save baseoff in pending state for interval */
	state->interval.baseoff = state->baseoff;
	state->overlap = false;
	if (state->newitem)
	{
		/* Might overlap with new item -- mark it as possible if it is */
		if (BTreeTupleGetHeapTID(base) < BTreeTupleGetHeapTID(state->newitem))
			state->overlap = true;
	}
}

/*
 * Save itup heap TID(s) into pending posting list where possible.
 *
 * Returns bool indicating if the pending posting list managed by state has
 * itup's heap TID(s) saved.  When this is false, enlarging the pending
 * posting list by the required amount would exceed the maxitemsize limit, so
 * caller must finish the pending posting list tuple.  (Generally itup becomes
 * the base tuple of caller's new pending posting list).
 */
bool
_bt_dedup_save_htid(BTDedupState *state, IndexTuple itup)
{
	int			nhtids;
	ItemPointer htids;
	Size		mergedtupsz;

	if (!BTreeTupleIsPosting(itup))
	{
		nhtids = 1;
		htids = &itup->t_tid;
	}
	else
	{
		nhtids = BTreeTupleGetNPosting(itup);
		htids = BTreeTupleGetPosting(itup);
	}

	/*
	 * Don't append (have caller finish pending posting list as-is) if
	 * appending heap TID(s) from itup would put us over limit
	 */
	mergedtupsz = MAXALIGN(state->basetupsize +
						   (state->nhtids + nhtids) *
						   sizeof(ItemPointerData));

	if (mergedtupsz > state->maxitemsize)
		return false;

	/* Don't merge existing posting lists with checkingunique */
	if (state->checkingunique &&
		(BTreeTupleIsPosting(state->base) || nhtids > 1))
	{
		/* May begin here if second pass over page is required */
		if (state->skippedbase == InvalidOffsetNumber)
			state->skippedbase = state->baseoff;
		return false;
	}

	if (state->overlap)
	{
		if (BTreeTupleGetMaxHeapTID(itup) > BTreeTupleGetHeapTID(state->newitem))
		{
			/*
			 * newitem has heap TID in the range of the would-be new posting
			 * list.  Avoid an immediate posting list split for caller.
			 */
			if (_bt_keep_natts_fast(state->rel, state->newitem, itup) >
				IndexRelationGetNumberOfAttributes(state->rel))
			{
				state->newitem = NULL;	/* avoid unnecessary comparisons */
				return false;
			}
		}
	}

	/*
	 * Save heap TIDs to pending posting list tuple -- itup can be merged into
	 * pending posting list
	 */
	state->nitems++;
	memcpy(state->htids + state->nhtids, htids,
		   sizeof(ItemPointerData) * nhtids);
	state->nhtids += nhtids;
	state->alltupsize += MAXALIGN(IndexTupleSize(itup)) + sizeof(ItemIdData);

	return true;
}

/*
 * Finalize pending posting list tuple, and add it to the page.  Final tuple
 * is based on saved base tuple, and saved list of heap TIDs.
 *
 * Returns space saving from deduplicating to make a new posting list tuple.
 * Note that this includes line pointer overhead.  This is zero in the case
 * where no deduplication was possible.
 */
Size
_bt_dedup_finish_pending(Buffer buffer, BTDedupState *state, bool need_wal)
{
	Size		spacesaving = 0;
	Page		page = BufferGetPage(buffer);
	int			minimum = 2;

	Assert(state->nitems > 0);
	Assert(state->nitems <= state->nhtids);
	Assert(state->interval.baseoff == state->baseoff);

	/*
	 * Only create a posting list when at least 3 heap TIDs will appear in the
	 * checkingunique case (checkingunique strategy won't merge existing
	 * posting list tuples, so we know that the number of items here must also
	 * be the total number of heap TIDs).  Creating a new posting lists with
	 * only two heap TIDs won't even save enough space to fit another
	 * duplicate with the same key as the posting list.  This is a bad
	 * trade-off if there is a chance that the LP_DEAD bit can be set for
	 * either existing tuple by putting off deduplication.
	 *
	 * (Note that a second pass over the page can deduplicate the item if that
	 * is truly the only way to avoid a page split for checkingunique caller)
	 */
	Assert(!state->checkingunique || state->nitems == 1 ||
		   state->nhtids == state->nitems);
	if (state->checkingunique)
	{
		minimum = 3;
		/* May begin here if second pass over page is required */
		if (state->nitems == 2 && state->skippedbase == InvalidOffsetNumber)
			state->skippedbase = state->baseoff;
	}

	if (state->nitems >= minimum)
	{
		IndexTuple	final;
		Size		finalsz;
		OffsetNumber offnum;
		OffsetNumber deletable[MaxOffsetNumber];
		int			ndeletable = 0;

		/* find all tuples that will be replaced with this new posting tuple */
		for (offnum = state->baseoff;
			 offnum < state->baseoff + state->nitems;
			 offnum = OffsetNumberNext(offnum))
			deletable[ndeletable++] = offnum;

		/* Form a tuple with a posting list */
		final = _bt_form_posting(state->base, state->htids, state->nhtids);
		finalsz = IndexTupleSize(final);
		spacesaving = state->alltupsize - (finalsz + sizeof(ItemIdData));
		/* Must have saved some space */
		Assert(spacesaving > 0 && spacesaving < BLCKSZ);

		/* Save final number of items for posting list */
		state->interval.nitems = state->nitems;

		Assert(finalsz <= state->maxitemsize);
		Assert(finalsz == MAXALIGN(IndexTupleSize(final)));

		START_CRIT_SECTION();

		/* Delete items to replace */
		PageIndexMultiDelete(page, deletable, ndeletable);
		/* Insert posting tuple */
		if (PageAddItem(page, (Item) final, finalsz, state->baseoff, false,
						false) == InvalidOffsetNumber)
			elog(ERROR, "deduplication failed to add tuple to page");

		MarkBufferDirty(buffer);

		/* Log deduplicated items */
		if (need_wal)
		{
			XLogRecPtr	recptr;
			xl_btree_dedup xlrec_dedup;

			xlrec_dedup.baseoff = state->interval.baseoff;
			xlrec_dedup.nitems = state->interval.nitems;

			XLogBeginInsert();
			XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);
			XLogRegisterData((char *) &xlrec_dedup, SizeOfBtreeDedup);

			recptr = XLogInsert(RM_BTREE_ID, XLOG_BTREE_DEDUP_PAGE);

			PageSetLSN(page, recptr);
		}

		END_CRIT_SECTION();

		pfree(final);
	}

	/* Reset state for next pending posting list */
	state->nhtids = 0;
	state->nitems = 0;
	state->alltupsize = 0;

	return spacesaving;
}

/*
 * Build a posting list tuple from a "base" index tuple and a list of heap
 * TIDs for posting list.
 *
 * Caller's "htids" array must be sorted in ascending order.  Any heap TIDs
 * from caller's base tuple will not appear in returned posting list.
 *
 * If nhtids == 1, builds a non-posting tuple (posting list tuples can never
 * have a single heap TID).
 */
IndexTuple
_bt_form_posting(IndexTuple tuple, ItemPointer htids, int nhtids)
{
	uint32		keysize,
				newsize = 0;
	IndexTuple	itup;

	/* We only need key part of the tuple */
	if (BTreeTupleIsPosting(tuple))
		keysize = BTreeTupleGetPostingOffset(tuple);
	else
		keysize = IndexTupleSize(tuple);

	Assert(nhtids > 0);

	/* Add space needed for posting list */
	if (nhtids > 1)
		newsize = SHORTALIGN(keysize) + sizeof(ItemPointerData) * nhtids;
	else
		newsize = keysize;

	newsize = MAXALIGN(newsize);
	itup = palloc0(newsize);
	memcpy(itup, tuple, keysize);
	itup->t_info &= ~INDEX_SIZE_MASK;
	itup->t_info |= newsize;

	if (nhtids > 1)
	{
		/* Form posting tuple, fill posting fields */

		itup->t_info |= INDEX_ALT_TID_MASK;
		BTreeSetPostingMeta(itup, nhtids, SHORTALIGN(keysize));
		/* Copy posting list into the posting tuple */
		memcpy(BTreeTupleGetPosting(itup), htids,
			   sizeof(ItemPointerData) * nhtids);

#ifdef USE_ASSERT_CHECKING
		{
			/* Assert that htid array is sorted and has unique TIDs */
			ItemPointerData last;
			ItemPointer current;

			ItemPointerCopy(BTreeTupleGetHeapTID(itup), &last);

			for (int i = 1; i < BTreeTupleGetNPosting(itup); i++)
			{
				current = BTreeTupleGetPostingN(itup, i);
				Assert(ItemPointerCompare(current, &last) > 0);
				ItemPointerCopy(current, &last);
			}
		}
#endif
	}
	else
	{
		/* To finish building of a non-posting tuple, copy TID from htids */
		itup->t_info &= ~INDEX_ALT_TID_MASK;
		ItemPointerCopy(htids, &itup->t_tid);
	}

	return itup;
}

/*
 * Prepare for a posting list split by swapping heap TID in newitem with heap
 * TID from original posting list (the 'oposting' heap TID located at offset
 * 'postingoff').
 *
 * Returns new posting list tuple, which is palloc()'d in caller's context.
 * This is guaranteed to be the same size as 'oposting'.  Modified version of
 * newitem is what caller actually inserts inside the critical section that
 * also performs an in-place update of posting list.
 *
 * Explicit WAL-logging of newitem must use the original version of newitem in
 * order to make it possible for our nbtxlog.c callers to correctly REDO
 * original steps.  (This approach avoids any explicit WAL-logging of a
 * posting list tuple.  This is important because posting lists are often much
 * larger than plain tuples.)
 */
IndexTuple
_bt_swap_posting(IndexTuple newitem, IndexTuple oposting, int postingoff)
{
	int			nhtids;
	char	   *replacepos;
	char	   *rightpos;
	Size		nbytes;
	IndexTuple	nposting;

	nhtids = BTreeTupleGetNPosting(oposting);
	Assert(postingoff > 0 && postingoff < nhtids);

	nposting = CopyIndexTuple(oposting);
	replacepos = (char *) BTreeTupleGetPostingN(nposting, postingoff);
	rightpos = replacepos + sizeof(ItemPointerData);
	nbytes = (nhtids - postingoff - 1) * sizeof(ItemPointerData);

	/*
	 * Move item pointers in posting list to make a gap for the new item's
	 * heap TID (shift TIDs one place to the right, losing original rightmost
	 * TID)
	 */
	memmove(rightpos, replacepos, nbytes);

	/* Fill the gap with the TID of the new item */
	ItemPointerCopy(&newitem->t_tid, (ItemPointer) replacepos);

	/* Copy original posting list's rightmost TID into new item */
	ItemPointerCopy(BTreeTupleGetPostingN(oposting, nhtids - 1),
					&newitem->t_tid);
	Assert(ItemPointerCompare(BTreeTupleGetMaxHeapTID(nposting),
							  BTreeTupleGetHeapTID(newitem)) < 0);
	Assert(BTreeTupleGetNPosting(oposting) == BTreeTupleGetNPosting(nposting));

	return nposting;
}
