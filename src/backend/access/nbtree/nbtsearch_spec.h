/*
 * Specialized functions for nbtsearch.c
 */

/*
 * These functions are not exposed, so their "default" emitted form would be
 * unused and would generate warnings. Avoid unused code generation and the
 * subsequent warnings by not emitting these functions when generating the
 * code for defaults.
 */
#ifndef NBTS_SPECIALIZING_DEFAULT

static OffsetNumber NBTS_FUNCTION(_bt_binsrch)(Relation rel,
											   BTScanInsert key,
											   Buffer buf,
											   AttrNumber *highkeycmpcol);
static bool NBTS_FUNCTION(_bt_readpage)(IndexScanDesc scan, ScanDirection dir,
										OffsetNumber offnum);

/*
 *	_bt_binsrch() -- Do a binary search for a key on a particular page.
 *
 * On a leaf page, _bt_binsrch() returns the OffsetNumber of the first
 * key >= given scankey, or > scankey if nextkey is true.  (NOTE: in
 * particular, this means it is possible to return a value 1 greater than the
 * number of keys on the page, if the scankey is > all keys on the page.)
 *
 * On an internal (non-leaf) page, _bt_binsrch() returns the OffsetNumber
 * of the last key < given scankey, or last key <= given scankey if nextkey
 * is true.  (Since _bt_compare treats the first data key of such a page as
 * minus infinity, there will be at least one key < scankey, so the result
 * always points at one of the keys on the page.)  This key indicates the
 * right place to descend to be sure we find all leaf keys >= given scankey
 * (or leaf keys > given scankey when nextkey is true).
 *
 * This procedure is not responsible for walking right, it just examines
 * the given page.  _bt_binsrch() has no lock or refcount side effects
 * on the buffer.
 */
static OffsetNumber
NBTS_FUNCTION(_bt_binsrch)(Relation rel,
						   BTScanInsert key,
						   Buffer buf,
						   AttrNumber *highkeycmpcol)
{
	Page		page;
	BTPageOpaque opaque;
	OffsetNumber low,
				high;
	int32		result,
				cmpval;
	AttrNumber	highcmpcol = *highkeycmpcol,
				lowcmpcol = 1;

	page = BufferGetPage(buf);
	opaque = BTPageGetOpaque(page);

	/* Requesting nextkey semantics while using scantid seems nonsensical */
	Assert(!key->nextkey || key->scantid == NULL);
	/* scantid-set callers must use _bt_binsrch_insert() on leaf pages */
	Assert(!P_ISLEAF(opaque) || key->scantid == NULL);

	low = P_FIRSTDATAKEY(opaque);
	high = PageGetMaxOffsetNumber(page);

	/*
	 * If there are no keys on the page, return the first available slot. Note
	 * this covers two cases: the page is really empty (no keys), or it
	 * contains only a high key.  The latter case is possible after vacuuming.
	 * This can never happen on an internal page, however, since they are
	 * never empty (an internal page must have children).
	 */
	if (unlikely(high < low))
		return low;

	/*
	 * Binary search to find the first key on the page >= scan key, or first
	 * key > scankey when nextkey is true.
	 *
	 * For nextkey=false (cmpval=1), the loop invariant is: all slots before
	 * 'low' are < scan key, all slots at or after 'high' are >= scan key.
	 *
	 * For nextkey=true (cmpval=0), the loop invariant is: all slots before
	 * 'low' are <= scan key, all slots at or after 'high' are > scan key.
	 *
	 * We can fall out when high == low.
	 */
	high++;						/* establish the loop invariant for high */

	cmpval = key->nextkey ? 0 : 1;	/* select comparison value */

	while (high > low)
	{
		OffsetNumber mid = low + ((high - low) / 2);
		AttrNumber cmpcol = Min(highcmpcol, lowcmpcol);

		/* We have low <= mid < high, so mid points at a real slot */

		result = nbts_call(_bt_compare, rel, key, page, mid, &cmpcol);

		if (result >= cmpval)
		{
			low = mid + 1;
			lowcmpcol = cmpcol;
		}
		else
		{
			high = mid;
			highcmpcol = cmpcol;
		}
	}

	*highkeycmpcol = highcmpcol;

	/*
	 * At this point we have high == low, but be careful: they could point
	 * past the last slot on the page.
	 *
	 * On a leaf page, we always return the first key >= scan key (resp. >
	 * scan key), which could be the last slot + 1.
	 */
	if (P_ISLEAF(opaque))
		return low;

	/*
	 * On a non-leaf page, return the last key < scan key (resp. <= scan key).
	 * There must be one if _bt_compare() is playing by the rules.
	 */
	Assert(low > P_FIRSTDATAKEY(opaque));

	return OffsetNumberPrev(low);
}

/*
 *	_bt_readpage() -- Load data from current index page into so->currPos
 *
 * Caller must have pinned and read-locked so->currPos.buf; the buffer's state
 * is not changed here.  Also, currPos.moreLeft and moreRight must be valid;
 * they are updated as appropriate.  All other fields of so->currPos are
 * initialized from scratch here.
 *
 * We scan the current page starting at offnum and moving in the indicated
 * direction.  All items matching the scan keys are loaded into currPos.items.
 * moreLeft or moreRight (as appropriate) is cleared if _bt_checkkeys reports
 * that there can be no more matching tuples in the current scan direction.
 *
 * In the case of a parallel scan, caller must have called _bt_parallel_seize
 * prior to calling this function; this function will invoke
 * _bt_parallel_release before returning.
 *
 * Returns true if any matching items found on the page, false if none.
 */
static bool
NBTS_FUNCTION(_bt_readpage)(IndexScanDesc scan, ScanDirection dir,
							OffsetNumber offnum)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	Page		page;
	BTPageOpaque opaque;
	OffsetNumber minoff;
	OffsetNumber maxoff;
	int			itemIndex;
	bool		continuescan;
	int			indnatts;

	/*
	 * We must have the buffer pinned and locked, but the usual macro can't be
	 * used here; this function is what makes it good for currPos.
	 */
	Assert(BufferIsValid(so->currPos.buf));

	page = BufferGetPage(so->currPos.buf);
	opaque = BTPageGetOpaque(page);

	/* allow next page be processed by parallel worker */
	if (scan->parallel_scan)
	{
		if (ScanDirectionIsForward(dir))
			_bt_parallel_release(scan, opaque->btpo_next);
		else
			_bt_parallel_release(scan, BufferGetBlockNumber(so->currPos.buf));
	}

	continuescan = true;		/* default assumption */
	indnatts = IndexRelationGetNumberOfAttributes(scan->indexRelation);
	minoff = P_FIRSTDATAKEY(opaque);
	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * We note the buffer's block number so that we can release the pin later.
	 * This allows us to re-read the buffer if it is needed again for hinting.
	 */
	so->currPos.currPage = BufferGetBlockNumber(so->currPos.buf);

	/*
	 * We save the LSN of the page as we read it, so that we know whether it
	 * safe to apply LP_DEAD hints to the page later.  This allows us to drop
	 * the pin for MVCC scans, which allows vacuum to avoid blocking.
	 */
	so->currPos.lsn = BufferGetLSNAtomic(so->currPos.buf);

	/*
	 * we must save the page's right-link while scanning it; this tells us
	 * where to step right to after we're done with these items.  There is no
	 * corresponding need for the left-link, since splits always go right.
	 */
	so->currPos.nextPage = opaque->btpo_next;

	/* initialize tuple workspace to empty */
	so->currPos.nextTupleOffset = 0;

	/*
	 * Now that the current page has been made consistent, the macro should be
	 * good.
	 */
	Assert(BTScanPosIsPinned(so->currPos));

	if (ScanDirectionIsForward(dir))
	{
		/* load items[] in ascending order */
		itemIndex = 0;

		offnum = Max(offnum, minoff);

		while (offnum <= maxoff)
		{
			ItemId		iid = PageGetItemId(page, offnum);
			IndexTuple	itup;

			/*
			 * If the scan specifies not to return killed tuples, then we
			 * treat a killed tuple as not passing the qual
			 */
			if (scan->ignore_killed_tuples && ItemIdIsDead(iid))
			{
				offnum = OffsetNumberNext(offnum);
				continue;
			}

			itup = (IndexTuple) PageGetItem(page, iid);

			if (nbts_call(_bt_checkkeys, scan->indexRelation, scan, itup, indnatts, dir, &continuescan))
			{
				/* tuple passes all scan key conditions */
				if (!BTreeTupleIsPosting(itup))
				{
					/* Remember it */
					_bt_saveitem(so, itemIndex, offnum, itup);
					itemIndex++;
				}
				else
				{
					int			tupleOffset;

					/*
					 * Set up state to return posting list, and remember first
					 * TID
					 */
					tupleOffset =
						_bt_setuppostingitems(so, itemIndex, offnum,
											  BTreeTupleGetPostingN(itup, 0),
											  itup);
					itemIndex++;
					/* Remember additional TIDs */
					for (int i = 1; i < BTreeTupleGetNPosting(itup); i++)
					{
						_bt_savepostingitem(so, itemIndex, offnum,
											BTreeTupleGetPostingN(itup, i),
											tupleOffset);
						itemIndex++;
					}
				}
			}
			/* When !continuescan, there can't be any more matches, so stop */
			if (!continuescan)
				break;

			offnum = OffsetNumberNext(offnum);
		}

		/*
		 * We don't need to visit page to the right when the high key
		 * indicates that no more matches will be found there.
		 *
		 * Checking the high key like this works out more often than you might
		 * think.  Leaf page splits pick a split point between the two most
		 * dissimilar tuples (this is weighed against the need to evenly share
		 * free space).  Leaf pages with high key attribute values that can
		 * only appear on non-pivot tuples on the right sibling page are
		 * common.
		 */
		if (continuescan && !P_RIGHTMOST(opaque))
		{
			ItemId		iid = PageGetItemId(page, P_HIKEY);
			IndexTuple	itup = (IndexTuple) PageGetItem(page, iid);
			int			truncatt;

			truncatt = BTreeTupleGetNAtts(itup, scan->indexRelation);
			nbts_call(_bt_checkkeys, scan->indexRelation, scan, itup, truncatt, dir, &continuescan);
		}

		if (!continuescan)
			so->currPos.moreRight = false;

		Assert(itemIndex <= MaxTIDsPerBTreePage);
		so->currPos.firstItem = 0;
		so->currPos.lastItem = itemIndex - 1;
		so->currPos.itemIndex = 0;
	}
	else
	{
		/* load items[] in descending order */
		itemIndex = MaxTIDsPerBTreePage;

		offnum = Min(offnum, maxoff);

		while (offnum >= minoff)
		{
			ItemId		iid = PageGetItemId(page, offnum);
			IndexTuple	itup;
			bool		tuple_alive;
			bool		passes_quals;

			/*
			 * If the scan specifies not to return killed tuples, then we
			 * treat a killed tuple as not passing the qual.  Most of the
			 * time, it's a win to not bother examining the tuple's index
			 * keys, but just skip to the next tuple (previous, actually,
			 * since we're scanning backwards).  However, if this is the first
			 * tuple on the page, we do check the index keys, to prevent
			 * uselessly advancing to the page to the left.  This is similar
			 * to the high key optimization used by forward scans.
			 */
			if (scan->ignore_killed_tuples && ItemIdIsDead(iid))
			{
				Assert(offnum >= P_FIRSTDATAKEY(opaque));
				if (offnum > P_FIRSTDATAKEY(opaque))
				{
					offnum = OffsetNumberPrev(offnum);
					continue;
				}

				tuple_alive = false;
			}
			else
				tuple_alive = true;

			itup = (IndexTuple) PageGetItem(page, iid);

			passes_quals = nbts_call(_bt_checkkeys, scan->indexRelation,
									 scan, itup, indnatts, dir,
									 &continuescan);
			if (passes_quals && tuple_alive)
			{
				/* tuple passes all scan key conditions */
				if (!BTreeTupleIsPosting(itup))
				{
					/* Remember it */
					itemIndex--;
					_bt_saveitem(so, itemIndex, offnum, itup);
				}
				else
				{
					int			tupleOffset;

					/*
					 * Set up state to return posting list, and remember first
					 * TID.
					 *
					 * Note that we deliberately save/return items from
					 * posting lists in ascending heap TID order for backwards
					 * scans.  This allows _bt_killitems() to make a
					 * consistent assumption about the order of items
					 * associated with the same posting list tuple.
					 */
					itemIndex--;
					tupleOffset =
						_bt_setuppostingitems(so, itemIndex, offnum,
											  BTreeTupleGetPostingN(itup, 0),
											  itup);
					/* Remember additional TIDs */
					for (int i = 1; i < BTreeTupleGetNPosting(itup); i++)
					{
						itemIndex--;
						_bt_savepostingitem(so, itemIndex, offnum,
											BTreeTupleGetPostingN(itup, i),
											tupleOffset);
					}
				}
			}
			if (!continuescan)
			{
				/* there can't be any more matches, so stop */
				so->currPos.moreLeft = false;
				break;
			}

			offnum = OffsetNumberPrev(offnum);
		}

		Assert(itemIndex >= 0);
		so->currPos.firstItem = itemIndex;
		so->currPos.lastItem = MaxTIDsPerBTreePage - 1;
		so->currPos.itemIndex = MaxTIDsPerBTreePage - 1;
	}

	return (so->currPos.firstItem <= so->currPos.lastItem);
}

#endif /* NBTS_SPECIALIZING_DEFAULT */

/*
 *	_bt_search() -- Search the tree for a particular scankey,
 *		or more precisely for the first leaf page it could be on.
 *
 * The passed scankey is an insertion-type scankey (see nbtree/README),
 * but it can omit the rightmost column(s) of the index.
 *
 * Return value is a stack of parent-page pointers (i.e. there is no entry for
 * the leaf level/page).  *bufP is set to the address of the leaf-page buffer,
 * which is locked and pinned.  No locks are held on the parent pages,
 * however!
 *
 * If the snapshot parameter is not NULL, "old snapshot" checking will take
 * place during the descent through the tree.  This is not needed when
 * positioning for an insert or delete, so NULL is used for those cases.
 *
 * The returned buffer is locked according to access parameter.  Additionally,
 * access = BT_WRITE will allow an empty root page to be created and returned.
 * When access = BT_READ, an empty index will result in *bufP being set to
 * InvalidBuffer.  Also, in BT_WRITE mode, any incomplete splits encountered
 * during the search will be finished.
 */
BTStack
NBTS_FUNCTION(_bt_search)(Relation rel, BTScanInsert key, Buffer *bufP,
						  int access, Snapshot snapshot)
{
	BTStack		stack_in = NULL;
	int			page_access = BT_READ;
	char		tupdatabuf[BLCKSZ / 3];

	/* Get the root page to start with */
	*bufP = _bt_getroot(rel, access);

	/* If index is empty and access = BT_READ, no root page is created. */
	if (!BufferIsValid(*bufP))
		return (BTStack) NULL;

	/* Loop iterates once per level descended in the tree */
	for (;;)
	{
		Page		page;
		BTPageOpaque opaque;
		OffsetNumber offnum;
		ItemId		itemid;
		IndexTuple	itup;
		BlockNumber child;
		BTStack		new_stack;
		AttrNumber	highkeycmpcol = 1;

		/*
		 * Race -- the page we just grabbed may have split since we read its
		 * downlink in its parent page (or the metapage).  If it has, we may
		 * need to move right to its new sibling.  Do that.
		 *
		 * In write-mode, allow _bt_moveright to finish any incomplete splits
		 * along the way.  Strictly speaking, we'd only need to finish an
		 * incomplete split on the leaf page we're about to insert to, not on
		 * any of the upper levels (internal pages with incomplete splits are
		 * also taken care of in _bt_getstackbuf).  But this is a good
		 * opportunity to finish splits of internal pages too.
		 */
		*bufP = nbts_call(_bt_moveright, rel, key, *bufP,
						  (access == BT_WRITE), stack_in,
						  page_access, snapshot, &highkeycmpcol,
						  (char *) tupdatabuf);

		/* if this is a leaf page, we're done */
		page = BufferGetPage(*bufP);
		opaque = BTPageGetOpaque(page);
		if (P_ISLEAF(opaque))
			break;

		/*
		 * Find the appropriate pivot tuple on this page.  Its downlink points
		 * to the child page that we're about to descend to.
		 */
		offnum = nbts_call(_bt_binsrch, rel, key, *bufP, &highkeycmpcol);
		itemid = PageGetItemId(page, offnum);
		itup = (IndexTuple) PageGetItem(page, itemid);
		Assert(BTreeTupleIsPivot(itup) || !key->heapkeyspace);
		child = BTreeTupleGetDownLink(itup);

		if (highkeycmpcol > 1)
		{
			memcpy((char *) tupdatabuf, (char *) itup, IndexTupleSize(itup));
		}

		/*
		 * We need to save the location of the pivot tuple we chose in a new
		 * stack entry for this page/level.  If caller ends up splitting a
		 * page one level down, it usually ends up inserting a new pivot
		 * tuple/downlink immediately after the location recorded here.
		 */
		new_stack = (BTStack) palloc(sizeof(BTStackData));
		new_stack->bts_blkno = BufferGetBlockNumber(*bufP);
		new_stack->bts_offset = offnum;
		new_stack->bts_parent = stack_in;

		/*
		 * Page level 1 is lowest non-leaf page level prior to leaves.  So, if
		 * we're on the level 1 and asked to lock leaf page in write mode,
		 * then lock next page in write mode, because it must be a leaf.
		 */
		if (opaque->btpo_level == 1 && access == BT_WRITE)
			page_access = BT_WRITE;

		/* drop the read lock on the page, then acquire one on its child */
		*bufP = _bt_relandgetbuf(rel, *bufP, child, page_access);

		/* okay, all set to move down a level */
		stack_in = new_stack;
	}

	/*
	 * If we're asked to lock leaf in write mode, but didn't manage to, then
	 * relock.  This should only happen when the root page is a leaf page (and
	 * the only page in the index other than the metapage).
	 */
	if (access == BT_WRITE && page_access == BT_READ)
	{
		AttrNumber highkeycmpcol = 1;
		/* trade in our read lock for a write lock */
		_bt_unlockbuf(rel, *bufP);
		_bt_lockbuf(rel, *bufP, BT_WRITE);

		/*
		 * Race -- the leaf page may have split after we dropped the read lock
		 * but before we acquired a write lock.  If it has, we may need to
		 * move right to its new sibling.  Do that.
		 */
		*bufP = nbts_call(_bt_moveright, rel, key, *bufP, true, stack_in,
						  BT_WRITE, snapshot, &highkeycmpcol, (char *) tupdatabuf);
	}

	return stack_in;
}

/*
 *	_bt_moveright() -- move right in the btree if necessary.
 *
 * When we follow a pointer to reach a page, it is possible that
 * the page has changed in the meanwhile.  If this happens, we're
 * guaranteed that the page has "split right" -- that is, that any
 * data that appeared on the page originally is either on the page
 * or strictly to the right of it.
 *
 * This routine decides whether or not we need to move right in the
 * tree by examining the high key entry on the page.  If that entry is
 * strictly less than the scankey, or <= the scankey in the
 * key.nextkey=true case, then we followed the wrong link and we need
 * to move right.
 *
 * The passed insertion-type scankey can omit the rightmost column(s) of the
 * index. (see nbtree/README)
 *
 * When key.nextkey is false (the usual case), we are looking for the first
 * item >= key.  When key.nextkey is true, we are looking for the first item
 * strictly greater than key.
 *
 * If forupdate is true, we will attempt to finish any incomplete splits
 * that we encounter.  This is required when locking a target page for an
 * insertion, because we don't allow inserting on a page before the split
 * is completed.  'stack' is only used if forupdate is true.
 *
 * On entry, we have the buffer pinned and a lock of the type specified by
 * 'access'.  If we move right, we release the buffer and lock and acquire
 * the same on the right sibling.  Return value is the buffer we stop at.
 *
 * If the snapshot parameter is not NULL, "old snapshot" checking will take
 * place during the descent through the tree.  This is not needed when
 * positioning for an insert or delete, so NULL is used for those cases.
 */
Buffer
NBTS_FUNCTION(_bt_moveright)(Relation rel,
							 BTScanInsert key,
							 Buffer buf,
							 bool forupdate,
							 BTStack stack,
							 int access,
							 Snapshot snapshot,
							 AttrNumber *comparecol,
							 char *tupdatabuf)
{
	Page		page;
	BTPageOpaque opaque;
	int32		cmpval;

	Assert(PointerIsValid(comparecol));

	/*
	 * When nextkey = false (normal case): if the scan key that brought us to
	 * this page is > the high key stored on the page, then the page has split
	 * and we need to move right.  (pg_upgrade'd !heapkeyspace indexes could
	 * have some duplicates to the right as well as the left, but that's
	 * something that's only ever dealt with on the leaf level, after
	 * _bt_search has found an initial leaf page.)
	 *
	 * When nextkey = true: move right if the scan key is >= page's high key.
	 * (Note that key.scantid cannot be set in this case.)
	 *
	 * The page could even have split more than once, so scan as far as
	 * needed.
	 *
	 * We also have to move right if we followed a link that brought us to a
	 * dead page.
	 */
	cmpval = key->nextkey ? 0 : 1;

	for (;;)
	{
		AttrNumber cmpcol = 1;

		page = BufferGetPage(buf);
		TestForOldSnapshot(snapshot, rel, page);
		opaque = BTPageGetOpaque(page);

		if (P_RIGHTMOST(opaque))
		{
			*comparecol = cmpcol;
			break;
		}

		/*
		 * Finish any incomplete splits we encounter along the way.
		 */
		if (forupdate && P_INCOMPLETE_SPLIT(opaque))
		{
			BlockNumber blkno = BufferGetBlockNumber(buf);

			/* upgrade our lock if necessary */
			if (access == BT_READ)
			{
				_bt_unlockbuf(rel, buf);
				_bt_lockbuf(rel, buf, BT_WRITE);
			}

			if (P_INCOMPLETE_SPLIT(opaque))
				_bt_finish_split(rel, buf, stack);
			else
				_bt_relbuf(rel, buf);

			/* re-acquire the lock in the right mode, and re-check */
			buf = _bt_getbuf(rel, blkno, access);
			continue;
		}

		/*
		 * When comparecol is > 1, tupdatabuf is filled with the right seperator
		 * of the parent node. This allows us to do a binary equality check
		 * between the parent node's right seperator (which is < key) and this
		 * page's P_HIKEY. If they equal, we can reuse the result of the
		 * parent node's rightkey compare, which means we can potentially save
		 * a full key compare.
		 * 
		 * Without this, we'd on average use 3 full key compares per page before
		 * we achieve full dynamic prefix bounds, with this optimization we can
		 * skip one of those.
		 * 
		 * 3: 1 for the highkey (rightmost), and on average 2 before we move
		 * right in the binary search on the page.
		 */
		if (!P_IGNORE(opaque) && *comparecol > 1)
		{
			IndexTuple itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, P_HIKEY));
			IndexTuple buftuple = (IndexTuple) tupdatabuf;
			if (IndexTupleSize(itup) == IndexTupleSize(buftuple))
			{
				char *dataptr = (char *) itup;

				if (memcmp(dataptr + sizeof(IndexTupleData),
						   tupdatabuf + sizeof(IndexTupleData),
						   IndexTupleSize(itup) - sizeof(IndexTupleData)) == 0)
					break;
			}
		}

		if (P_IGNORE(opaque) || nbts_call(_bt_compare, rel, key, page, P_HIKEY,
										  &cmpcol) >= cmpval)
		{
			/* step right one page */
			*comparecol = 1;
			buf = _bt_relandgetbuf(rel, buf, opaque->btpo_next, access);
			continue;
		}
		else
		{
			*comparecol = cmpcol;
			break;
		}
	}

	if (P_IGNORE(opaque))
		elog(ERROR, "fell off the end of index \"%s\"",
			 RelationGetRelationName(rel));

	return buf;
}

/*
 *
 *	_bt_binsrch_insert() -- Cacheable, incremental leaf page binary search.
 *
 * Like _bt_binsrch(), but with support for caching the binary search
 * bounds.  Only used during insertion, and only on the leaf page that it
 * looks like caller will insert tuple on.  Exclusive-locked and pinned
 * leaf page is contained within insertstate.
 *
 * Caches the bounds fields in insertstate so that a subsequent call can
 * reuse the low and strict high bounds of original binary search.  Callers
 * that use these fields directly must be prepared for the case where low
 * and/or stricthigh are not on the same page (one or both exceed maxoff
 * for the page).  The case where there are no items on the page (high <
 * low) makes bounds invalid.
 *
 * Caller is responsible for invalidating bounds when it modifies the page
 * before calling here a second time, and for dealing with posting list
 * tuple matches (callers can use insertstate's postingoff field to
 * determine which existing heap TID will need to be replaced by a posting
 * list split).
 */
OffsetNumber
NBTS_FUNCTION(_bt_binsrch_insert)(Relation rel, BTInsertState insertstate,
								  AttrNumber highcmpcol)
{
	BTScanInsert key = insertstate->itup_key;
	Page		page;
	BTPageOpaque opaque;
	OffsetNumber low,
				high,
				stricthigh;
	int32		result,
				cmpval;
	AttrNumber	lowcmpcol = 1;

	page = BufferGetPage(insertstate->buf);
	opaque = BTPageGetOpaque(page);

	Assert(P_ISLEAF(opaque));
	Assert(!key->nextkey);
	Assert(insertstate->postingoff == 0);

	if (!insertstate->bounds_valid)
	{
		/* Start new binary search */
		low = P_FIRSTDATAKEY(opaque);
		high = PageGetMaxOffsetNumber(page);
	}
	else
	{
		/* Restore result of previous binary search against same page */
		low = insertstate->low;
		high = insertstate->stricthigh;
	}

	/* If there are no keys on the page, return the first available slot */
	if (unlikely(high < low))
	{
		/* Caller can't reuse bounds */
		insertstate->low = InvalidOffsetNumber;
		insertstate->stricthigh = InvalidOffsetNumber;
		insertstate->bounds_valid = false;
		return low;
	}

	/*
	 * Binary search to find the first key on the page >= scan key. (nextkey
	 * is always false when inserting).
	 *
	 * The loop invariant is: all slots before 'low' are < scan key, all slots
	 * at or after 'high' are >= scan key.  'stricthigh' is > scan key, and is
	 * maintained to save additional search effort for caller.
	 *
	 * We can fall out when high == low.
	 */
	if (!insertstate->bounds_valid)
		high++;					/* establish the loop invariant for high */
	stricthigh = high;			/* high initially strictly higher */

	cmpval = 1;					/* !nextkey comparison value */

	while (high > low)
	{
		OffsetNumber mid = low + ((high - low) / 2);
		AttrNumber	cmpcol = Min(highcmpcol, lowcmpcol);

		/* We have low <= mid < high, so mid points at a real slot */

		result = nbts_call(_bt_compare, rel, key, page, mid, &cmpcol);

		if (result >= cmpval)
		{
			low = mid + 1;
			lowcmpcol = cmpcol;
		}
		else
		{
			high = mid;
			highcmpcol = cmpcol;
			if (result != 0)
				stricthigh = high;
		}

		/*
		 * If tuple at offset located by binary search is a posting list whose
		 * TID range overlaps with caller's scantid, perform posting list
		 * binary search to set postingoff for caller.  Caller must split the
		 * posting list when postingoff is set.  This should happen
		 * infrequently.
		 */
		if (unlikely(result == 0 && key->scantid != NULL))
		{
			/*
			 * postingoff should never be set more than once per leaf page
			 * binary search.  That would mean that there are duplicate table
			 * TIDs in the index, which is never okay.  Check for that here.
			 */
			if (insertstate->postingoff != 0)
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
							errmsg_internal("table tid from new index tuple (%u,%u) cannot find insert offset between offsets %u and %u of block %u in index \"%s\"",
											ItemPointerGetBlockNumber(key->scantid),
											ItemPointerGetOffsetNumber(key->scantid),
											low, stricthigh,
											BufferGetBlockNumber(insertstate->buf),
											RelationGetRelationName(rel))));

			insertstate->postingoff = _bt_binsrch_posting(key, page, mid);
		}
	}

	/*
	 * On a leaf page, a binary search always returns the first key >= scan
	 * key (at least in !nextkey case), which could be the last slot + 1. This
	 * is also the lower bound of cached search.
	 *
	 * stricthigh may also be the last slot + 1, which prevents caller from
	 * using bounds directly, but is still useful to us if we're called a
	 * second time with cached bounds (cached low will be < stricthigh when
	 * that happens).
	 */
	insertstate->low = low;
	insertstate->stricthigh = stricthigh;
	insertstate->bounds_valid = true;

	return low;
}

/*----------
 *	_bt_compare() -- Compare insertion-type scankey to tuple on a page.
 *
 *	page/offnum: location of btree item to be compared to.
 *
 *		This routine returns:
 *			<0 if scankey < tuple at offnum;
 *			 0 if scankey == tuple at offnum;
 *			>0 if scankey > tuple at offnum.
 *
 * NULLs in the keys are treated as sortable values.  Therefore
 * "equality" does not necessarily mean that the item should be returned
 * to the caller as a matching key.  Similarly, an insertion scankey
 * with its scantid set is treated as equal to a posting tuple whose TID
 * range overlaps with their scantid.  There generally won't be a
 * matching TID in the posting tuple, which caller must handle
 * themselves (e.g., by splitting the posting list tuple).
 *
 * CRUCIAL NOTE: on a non-leaf page, the first data key is assumed to be
 * "minus infinity": this routine will always claim it is less than the
 * scankey.  The actual key value stored is explicitly truncated to 0
 * attributes (explicitly minus infinity) with version 3+ indexes, but
 * that isn't relied upon.  This allows us to implement the Lehman and
 * Yao convention that the first down-link pointer is before the first
 * key.  See backend/access/nbtree/README for details.
 *----------
 */
int32
NBTS_FUNCTION(_bt_compare)(Relation rel,
						   BTScanInsert key,
						   Page page,
						   OffsetNumber offnum,
						   AttrNumber *comparecol)
{
	TupleDesc	itupdesc = RelationGetDescr(rel);
	BTPageOpaque opaque = BTPageGetOpaque(page);
	IndexTuple	itup;
	ItemPointer heapTid;
	ScanKey		scankey;
	int			ncmpkey;
	int			ntupatts;
	int32		result;
	nbts_attiterdeclare(itup);

	Assert(_bt_check_natts(rel, key->heapkeyspace, page, offnum));
	Assert(key->keysz <= IndexRelationGetNumberOfKeyAttributes(rel));
	Assert(key->heapkeyspace || key->scantid == NULL);

	/*
	 * Force result ">" if target item is first data item on an internal page
	 * --- see NOTE above.
	 */
	if (!P_ISLEAF(opaque) && offnum == P_FIRSTDATAKEY(opaque))
		return 1;

	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offnum));
	ntupatts = BTreeTupleGetNAtts(itup, rel);

	/*
	 * The scan key is set up with the attribute number associated with each
	 * term in the key.  It is important that, if the index is multi-key, the
	 * scan contain the first k key attributes, and that they be in order.  If
	 * you think about how multi-key ordering works, you'll understand why
	 * this is.
	 *
	 * We don't test for violation of this condition here, however.  The
	 * initial setup for the index scan had better have gotten it right (see
	 * _bt_first).
	 */

	ncmpkey = Min(ntupatts, key->keysz);
	Assert(key->heapkeyspace || ncmpkey == key->keysz);
	Assert(!BTreeTupleIsPosting(itup) || key->allequalimage);

	nbts_attiterinit(itup, *comparecol, itupdesc);
	scankey = key->scankeys + ((*comparecol) - 1);

	nbts_foreachattr(*comparecol, ncmpkey)
	{
		Datum		datum;

		datum = nbts_attiter_nextattdatum(itup, itupdesc);

		/* key is NULL */
		if (scankey->sk_flags & SK_ISNULL)
		{
			if (nbts_attiter_curattisnull(itup))
				result = 0;		/* NULL "=" NULL */
			else if (scankey->sk_flags & SK_BT_NULLS_FIRST)
				result = -1;	/* NULL "<" NOT_NULL */
			else
				result = 1;		/* NULL ">" NOT_NULL */
		}
		/* key is NOT_NULL and item is NULL */
		else if (nbts_attiter_curattisnull(itup))
		{
			if (scankey->sk_flags & SK_BT_NULLS_FIRST)
				result = 1;		/* NOT_NULL ">" NULL */
			else
				result = -1;	/* NOT_NULL "<" NULL */
		}
		else
		{
			/*
			 * The sk_func needs to be passed the index value as left arg and
			 * the sk_argument as right arg (they might be of different
			 * types).  Since it is convenient for callers to think of
			 * _bt_compare as comparing the scankey to the index item, we have
			 * to flip the sign of the comparison result.  (Unless it's a DESC
			 * column, in which case we *don't* flip the sign.)
			 */
			result = DatumGetInt32(FunctionCall2Coll(&scankey->sk_func,
													 scankey->sk_collation,
													 datum,
													 scankey->sk_argument));

			if (!(scankey->sk_flags & SK_BT_DESC))
				INVERT_COMPARE_RESULT(result);
		}

		/* if the keys are unequal, return the difference */
		if (result != 0)
		{
			*comparecol = nbts_attiter_attnum;
			return result;
		}

		scankey++;
	}

	/*
	 * All tuple attributes are equal to the scan key, only later attributes
	 * could potentially not equal the scan key.
	 */
	*comparecol = ntupatts + 1;

	/*
	 * All non-truncated attributes (other than heap TID) were found to be
	 * equal.  Treat truncated attributes as minus infinity when scankey has a
	 * key attribute value that would otherwise be compared directly.
	 *
	 * Note: it doesn't matter if ntupatts includes non-key attributes;
	 * scankey won't, so explicitly excluding non-key attributes isn't
	 * necessary.
	 */
	if (key->keysz > ntupatts)
		return 1;

	/*
	 * Use the heap TID attribute and scantid to try to break the tie.  The
	 * rules are the same as any other key attribute -- only the
	 * representation differs.
	 */
	heapTid = BTreeTupleGetHeapTID(itup);
	if (key->scantid == NULL)
	{
		/*
		 * Most searches have a scankey that is considered greater than a
		 * truncated pivot tuple if and when the scankey has equal values for
		 * attributes up to and including the least significant untruncated
		 * attribute in tuple.
		 *
		 * For example, if an index has the minimum two attributes (single
		 * user key attribute, plus heap TID attribute), and a page's high key
		 * is ('foo', -inf), and scankey is ('foo', <omitted>), the search
		 * will not descend to the page to the left.  The search will descend
		 * right instead.  The truncated attribute in pivot tuple means that
		 * all non-pivot tuples on the page to the left are strictly < 'foo',
		 * so it isn't necessary to descend left.  In other words, search
		 * doesn't have to descend left because it isn't interested in a match
		 * that has a heap TID value of -inf.
		 *
		 * However, some searches (pivotsearch searches) actually require that
		 * we descend left when this happens.  -inf is treated as a possible
		 * match for omitted scankey attribute(s).  This is needed by page
		 * deletion, which must re-find leaf pages that are targets for
		 * deletion using their high keys.
		 *
		 * Note: the heap TID part of the test ensures that scankey is being
		 * compared to a pivot tuple with one or more truncated key
		 * attributes.
		 *
		 * Note: pg_upgrade'd !heapkeyspace indexes must always descend to the
		 * left here, since they have no heap TID attribute (and cannot have
		 * any -inf key values in any case, since truncation can only remove
		 * non-key attributes).  !heapkeyspace searches must always be
		 * prepared to deal with matches on both sides of the pivot once the
		 * leaf level is reached.
		 */
		if (key->heapkeyspace && !key->pivotsearch &&
			key->keysz == ntupatts && heapTid == NULL)
			return 1;

		/* All provided scankey arguments found to be equal */
		return 0;
	}

	/*
	 * Treat truncated heap TID as minus infinity, since scankey has a key
	 * attribute value (scantid) that would otherwise be compared directly
	 */
	Assert(key->keysz == IndexRelationGetNumberOfKeyAttributes(rel));
	if (heapTid == NULL)
		return 1;

	/*
	 * Scankey must be treated as equal to a posting list tuple if its scantid
	 * value falls within the range of the posting list.  In all other cases
	 * there can only be a single heap TID value, which is compared directly
	 * with scantid.
	 */
	Assert(ntupatts >= IndexRelationGetNumberOfKeyAttributes(rel));
	result = ItemPointerCompare(key->scantid, heapTid);
	if (result <= 0 || !BTreeTupleIsPosting(itup))
		return result;
	else
	{
		result = ItemPointerCompare(key->scantid,
									BTreeTupleGetMaxHeapTID(itup));
		if (result > 0)
			return 1;
	}

	return 0;
}
