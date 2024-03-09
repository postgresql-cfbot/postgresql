/*-------------------------------------------------------------------------
 *
 * nbtdedup_spec.c
 *	  Index shape-specialized functions for nbtdedup.c
 *
 * NOTES
 *	  See also: access/nbtree/README section "nbtree specialization"
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtdedup_spec.c
 *
 *-------------------------------------------------------------------------
 */

#define _bt_do_singleval NBTS_FUNCTION(_bt_do_singleval)

static bool _bt_do_singleval(Relation rel, Page page, BTDedupState state,
							 OffsetNumber minoff, IndexTuple newitem);

/*
 * Perform a deduplication pass.
 *
 * The general approach taken here is to perform as much deduplication as
 * possible to free as much space as possible.  Note, however, that "single
 * value" strategy is used for !bottomupdedup callers when the page is full of
 * tuples of a single value.  Deduplication passes that apply the strategy
 * will leave behind a few untouched tuples at the end of the page, preparing
 * the page for an anticipated page split that uses nbtsplitloc.c's own single
 * value strategy.  Our high level goal is to delay merging the untouched
 * tuples until after the page splits.
 *
 * When a call to _bt_bottomupdel_pass() just took place (and failed), our
 * high level goal is to prevent a page split entirely by buying more time.
 * We still hope that a page split can be avoided altogether.  That's why
 * single value strategy is not even considered for bottomupdedup callers.
 *
 * The page will have to be split if we cannot successfully free at least
 * newitemsz (we also need space for newitem's line pointer, which isn't
 * included in caller's newitemsz).
 *
 * Note: Caller should have already deleted all existing items with their
 * LP_DEAD bits set.
 */
void
_bt_dedup_pass(Relation rel, Buffer buf, IndexTuple newitem, Size newitemsz,
			   bool bottomupdedup)
{
	OffsetNumber offnum,
				minoff,
				maxoff;
	Page		page = BufferGetPage(buf);
	BTPageOpaque opaque = BTPageGetOpaque(page);
	Page		newpage;
	BTDedupState state;
	Size		pagesaving PG_USED_FOR_ASSERTS_ONLY = 0;
	bool		singlevalstrat = false;
	int			nkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);

	/* Passed-in newitemsz is MAXALIGNED but does not include line pointer */
	newitemsz += sizeof(ItemIdData);

	/*
	 * Initialize deduplication state.
	 *
	 * It would be possible for maxpostingsize (limit on posting list tuple
	 * size) to be set to one third of the page.  However, it seems like a
	 * good idea to limit the size of posting lists to one sixth of a page.
	 * That ought to leave us with a good split point when pages full of
	 * duplicates can be split several times.
	 */
	state = (BTDedupState) palloc(sizeof(BTDedupStateData));
	state->deduplicate = true;
	state->nmaxitems = 0;
	state->maxpostingsize = Min(BTMaxItemSize(page) / 2, INDEX_SIZE_MASK);
	/* Metadata about base tuple of current pending posting list */
	state->base = NULL;
	state->baseoff = InvalidOffsetNumber;
	state->basetupsize = 0;
	/* Metadata about current pending posting list TIDs */
	state->htids = palloc(state->maxpostingsize);
	state->nhtids = 0;
	state->nitems = 0;
	/* Size of all physical tuples to be replaced by pending posting list */
	state->phystupsize = 0;
	/* nintervals should be initialized to zero */
	state->nintervals = 0;

	minoff = P_FIRSTDATAKEY(opaque);
	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * Consider applying "single value" strategy, though only if the page
	 * seems likely to be split in the near future
	 */
	if (!bottomupdedup)
		singlevalstrat = _bt_do_singleval(rel, page, state, minoff, newitem);

	/*
	 * Deduplicate items from page, and write them to newpage.
	 *
	 * Copy the original page's LSN into newpage copy.  This will become the
	 * updated version of the page.  We need this because XLogInsert will
	 * examine the LSN and possibly dump it in a page image.
	 */
	newpage = PageGetTempPageCopySpecial(page);
	PageSetLSN(newpage, PageGetLSN(page));

	/* Copy high key, if any */
	if (!P_RIGHTMOST(opaque))
	{
		ItemId		hitemid = PageGetItemId(page, P_HIKEY);
		Size		hitemsz = ItemIdGetLength(hitemid);
		IndexTuple	hitem = (IndexTuple) PageGetItem(page, hitemid);

		if (PageAddItem(newpage, (Item) hitem, hitemsz, P_HIKEY,
						false, false) == InvalidOffsetNumber)
			elog(ERROR, "deduplication failed to add highkey");
	}

	for (offnum = minoff;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid = PageGetItemId(page, offnum);
		IndexTuple	itup = (IndexTuple) PageGetItem(page, itemid);

		Assert(!ItemIdIsDead(itemid));

		if (offnum == minoff)
		{
			/*
			 * No previous/base tuple for the data item -- use the data item
			 * as base tuple of pending posting list
			 */
			_bt_dedup_start_pending(state, itup, offnum);
		}
		else if (state->deduplicate &&
				 _bt_keep_natts_fast(rel, state->base, itup) > nkeyatts &&
				 _bt_dedup_save_htid(state, itup))
		{
			/*
			 * Tuple is equal to base tuple of pending posting list.  Heap
			 * TID(s) for itup have been saved in state.
			 */
		}
		else
		{
			/*
			 * Tuple is not equal to pending posting list tuple, or
			 * _bt_dedup_save_htid() opted to not merge current item into
			 * pending posting list for some other reason (e.g., adding more
			 * TIDs would have caused posting list to exceed current
			 * maxpostingsize).
			 *
			 * If state contains pending posting list with more than one item,
			 * form new posting tuple and add it to our temp page (newpage).
			 * Else add pending interval's base tuple to the temp page as-is.
			 */
			pagesaving += _bt_dedup_finish_pending(newpage, state);

			if (singlevalstrat)
			{
				/*
				 * Single value strategy's extra steps.
				 *
				 * Lower maxpostingsize for sixth and final large posting list
				 * tuple at the point where 5 maxpostingsize-capped tuples
				 * have either been formed or observed.
				 *
				 * When a sixth maxpostingsize-capped item is formed/observed,
				 * stop merging together tuples altogether.  The few tuples
				 * that remain at the end of the page won't be merged together
				 * at all (at least not until after a future page split takes
				 * place, when this page's newly allocated right sibling page
				 * gets its first deduplication pass).
				 */
				if (state->nmaxitems == 5)
					_bt_singleval_fillfactor(page, state, newitemsz);
				else if (state->nmaxitems == 6)
				{
					state->deduplicate = false;
					singlevalstrat = false; /* won't be back here */
				}
			}

			/* itup starts new pending posting list */
			_bt_dedup_start_pending(state, itup, offnum);
		}
	}

	/* Handle the last item */
	pagesaving += _bt_dedup_finish_pending(newpage, state);

	/*
	 * If no items suitable for deduplication were found, newpage must be
	 * exactly the same as the original page, so just return from function.
	 *
	 * We could determine whether or not to proceed on the basis the space
	 * savings being sufficient to avoid an immediate page split instead.  We
	 * don't do that because there is some small value in nbtsplitloc.c always
	 * operating against a page that is fully deduplicated (apart from
	 * newitem).  Besides, most of the cost has already been paid.
	 */
	if (state->nintervals == 0)
	{
		/* cannot leak memory here */
		pfree(newpage);
		pfree(state->htids);
		pfree(state);
		return;
	}

	/*
	 * By here, it's clear that deduplication will definitely go ahead.
	 *
	 * Clear the BTP_HAS_GARBAGE page flag.  The index must be a heapkeyspace
	 * index, and as such we'll never pay attention to BTP_HAS_GARBAGE anyway.
	 * But keep things tidy.
	 */
	if (P_HAS_GARBAGE(opaque))
	{
		BTPageOpaque nopaque = BTPageGetOpaque(newpage);

		nopaque->btpo_flags &= ~BTP_HAS_GARBAGE;
	}

	START_CRIT_SECTION();

	PageRestoreTempPage(newpage, page);
	MarkBufferDirty(buf);

	/* XLOG stuff */
	if (RelationNeedsWAL(rel))
	{
		XLogRecPtr	recptr;
		xl_btree_dedup xlrec_dedup;

		xlrec_dedup.nintervals = state->nintervals;

		XLogBeginInsert();
		XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
		XLogRegisterData((char *) &xlrec_dedup, SizeOfBtreeDedup);

		/*
		 * The intervals array is not in the buffer, but pretend that it is.
		 * When XLogInsert stores the whole buffer, the array need not be
		 * stored too.
		 */
		XLogRegisterBufData(0, (char *) state->intervals,
							state->nintervals * sizeof(BTDedupInterval));

		recptr = XLogInsert(RM_BTREE_ID, XLOG_BTREE_DEDUP);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	/* Local space accounting should agree with page accounting */
	Assert(pagesaving < newitemsz || PageGetExactFreeSpace(page) >= newitemsz);

	/* cannot leak memory here */
	pfree(state->htids);
	pfree(state);
}

/*
 * Determine if page non-pivot tuples (data items) are all duplicates of the
 * same value -- if they are, deduplication's "single value" strategy should
 * be applied.  The general goal of this strategy is to ensure that
 * nbtsplitloc.c (which uses its own single value strategy) will find a useful
 * split point as further duplicates are inserted, and successive rightmost
 * page splits occur among pages that store the same duplicate value.  When
 * the page finally splits, it should end up BTREE_SINGLEVAL_FILLFACTOR% full,
 * just like it would if deduplication were disabled.
 *
 * We expect that affected workloads will require _several_ single value
 * strategy deduplication passes (over a page that only stores duplicates)
 * before the page is finally split.  The first deduplication pass should only
 * find regular non-pivot tuples.  Later deduplication passes will find
 * existing maxpostingsize-capped posting list tuples, which must be skipped
 * over.  The penultimate pass is generally the first pass that actually
 * reaches _bt_singleval_fillfactor(), and so will deliberately leave behind a
 * few untouched non-pivot tuples.  The final deduplication pass won't free
 * any space -- it will skip over everything without merging anything (it
 * retraces the steps of the penultimate pass).
 *
 * Fortunately, having several passes isn't too expensive.  Each pass (after
 * the first pass) won't spend many cycles on the large posting list tuples
 * left by previous passes.  Each pass will find a large contiguous group of
 * smaller duplicate tuples to merge together at the end of the page.
 */
static bool
_bt_do_singleval(Relation rel, Page page, BTDedupState state,
				 OffsetNumber minoff, IndexTuple newitem)
{
	int			nkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
	ItemId		itemid;
	IndexTuple	itup;

	itemid = PageGetItemId(page, minoff);
	itup = (IndexTuple) PageGetItem(page, itemid);

	if (_bt_keep_natts_fast(rel, newitem, itup) > nkeyatts)
	{
		itemid = PageGetItemId(page, PageGetMaxOffsetNumber(page));
		itup = (IndexTuple) PageGetItem(page, itemid);

		if (_bt_keep_natts_fast(rel, newitem, itup) > nkeyatts)
			return true;
	}

	return false;
}
