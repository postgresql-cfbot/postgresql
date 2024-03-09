/*-------------------------------------------------------------------------
 *
 * nbtinsert_spec.c
 *	  Index shape-specialized functions for nbtinsert.c
 *
 * NOTES
 *	  See also: access/nbtree/README section "nbtree specialization"
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtinsert_spec.c
 *
 *-------------------------------------------------------------------------
 */

#define _bt_search_insert NBTS_FUNCTION(_bt_search_insert)
#define _bt_findinsertloc NBTS_FUNCTION(_bt_findinsertloc)

static BTStack _bt_search_insert(Relation rel, Relation heaprel,
								 BTInsertState insertstate);
static OffsetNumber _bt_findinsertloc(Relation rel,
									  BTInsertState insertstate,
									  bool checkingunique,
									  bool indexUnchanged,
									  BTStack stack,
									  Relation heapRel);


/*
 *	_bt_doinsert() -- Handle insertion of a single index tuple in the tree.
 *
 *		This routine is called by the public interface routine, btinsert.
 *		By here, itup is filled in, including the TID.
 *
 *		If checkUnique is UNIQUE_CHECK_NO or UNIQUE_CHECK_PARTIAL, this
 *		will allow duplicates.  Otherwise (UNIQUE_CHECK_YES or
 *		UNIQUE_CHECK_EXISTING) it will throw error for a duplicate.
 *		For UNIQUE_CHECK_EXISTING we merely run the duplicate check, and
 *		don't actually insert.
 *
 *		indexUnchanged executor hint indicates if itup is from an
 *		UPDATE that didn't logically change the indexed value, but
 *		must nevertheless have a new entry to point to a successor
 *		version.
 *
 *		The result value is only significant for UNIQUE_CHECK_PARTIAL:
 *		it must be true if the entry is known unique, else false.
 *		(In the current implementation we'll also return true after a
 *		successful UNIQUE_CHECK_YES or UNIQUE_CHECK_EXISTING call, but
 *		that's just a coding artifact.)
 */
bool
_bt_doinsert(Relation rel, IndexTuple itup,
			 IndexUniqueCheck checkUnique, bool indexUnchanged,
			 Relation heapRel)
{
	bool		is_unique = false;
	BTInsertStateData insertstate;
	BTScanInsert itup_key;
	BTStack		stack;
	bool		checkingunique = (checkUnique != UNIQUE_CHECK_NO);

	/* we need an insertion scan key to do our search, so build one */
	itup_key = _bt_mkscankey(rel, itup);

	if (checkingunique)
	{
		if (!itup_key->anynullkeys)
		{
			/* No (heapkeyspace) scantid until uniqueness established */
			itup_key->scantid = NULL;
		}
		else
		{
			/*
			 * Scan key for new tuple contains NULL key values.  Bypass
			 * checkingunique steps.  They are unnecessary because core code
			 * considers NULL unequal to every value, including NULL.
			 *
			 * This optimization avoids O(N^2) behavior within the
			 * _bt_findinsertloc() heapkeyspace path when a unique index has a
			 * large number of "duplicates" with NULL key values.
			 */
			checkingunique = false;
			/* Tuple is unique in the sense that core code cares about */
			Assert(checkUnique != UNIQUE_CHECK_EXISTING);
			is_unique = true;
		}
	}

	/*
	 * Fill in the BTInsertState working area, to track the current page and
	 * position within the page to insert on.
	 *
	 * Note that itemsz is passed down to lower level code that deals with
	 * inserting the item.  It must be MAXALIGN()'d.  This ensures that space
	 * accounting code consistently considers the alignment overhead that we
	 * expect PageAddItem() will add later.  (Actually, index_form_tuple() is
	 * already conservative about alignment, but we don't rely on that from
	 * this distance.  Besides, preserving the "true" tuple size in index
	 * tuple headers for the benefit of nbtsplitloc.c might happen someday.
	 * Note that heapam does not MAXALIGN() each heap tuple's lp_len field.)
	 */
	insertstate.itup = itup;
	insertstate.itemsz = MAXALIGN(IndexTupleSize(itup));
	insertstate.itup_key = itup_key;
	insertstate.bounds_valid = false;
	insertstate.buf = InvalidBuffer;
	insertstate.postingoff = 0;

search:

	/*
	 * Find and lock the leaf page that the tuple should be added to by
	 * searching from the root page.  insertstate.buf will hold a buffer that
	 * is locked in exclusive mode afterwards.
	 */
	stack = _bt_search_insert(rel, heapRel, &insertstate);

	/*
	 * checkingunique inserts are not allowed to go ahead when two tuples with
	 * equal key attribute values would be visible to new MVCC snapshots once
	 * the xact commits.  Check for conflicts in the locked page/buffer (if
	 * needed) here.
	 *
	 * It might be necessary to check a page to the right in _bt_check_unique,
	 * though that should be very rare.  In practice the first page the value
	 * could be on (with scantid omitted) is almost always also the only page
	 * that a matching tuple might be found on.  This is due to the behavior
	 * of _bt_findsplitloc with duplicate tuples -- a group of duplicates can
	 * only be allowed to cross a page boundary when there is no candidate
	 * leaf page split point that avoids it.  Also, _bt_check_unique can use
	 * the leaf page high key to determine that there will be no duplicates on
	 * the right sibling without actually visiting it (it uses the high key in
	 * cases where the new item happens to belong at the far right of the leaf
	 * page).
	 *
	 * NOTE: obviously, _bt_check_unique can only detect keys that are already
	 * in the index; so it cannot defend against concurrent insertions of the
	 * same key.  We protect against that by means of holding a write lock on
	 * the first page the value could be on, with omitted/-inf value for the
	 * implicit heap TID tiebreaker attribute.  Any other would-be inserter of
	 * the same key must acquire a write lock on the same page, so only one
	 * would-be inserter can be making the check at one time.  Furthermore,
	 * once we are past the check we hold write locks continuously until we
	 * have performed our insertion, so no later inserter can fail to see our
	 * insertion.  (This requires some care in _bt_findinsertloc.)
	 *
	 * If we must wait for another xact, we release the lock while waiting,
	 * and then must perform a new search.
	 *
	 * For a partial uniqueness check, we don't wait for the other xact. Just
	 * let the tuple in and return false for possibly non-unique, or true for
	 * definitely unique.
	 */
	if (checkingunique)
	{
		TransactionId xwait;
		uint32		speculativeToken;

		xwait = _bt_check_unique(rel, &insertstate, heapRel, checkUnique,
								 &is_unique, &speculativeToken);

		if (unlikely(TransactionIdIsValid(xwait)))
		{
			/* Have to wait for the other guy ... */
			_bt_relbuf(rel, insertstate.buf);
			insertstate.buf = InvalidBuffer;

			/*
			 * If it's a speculative insertion, wait for it to finish (ie. to
			 * go ahead with the insertion, or kill the tuple).  Otherwise
			 * wait for the transaction to finish as usual.
			 */
			if (speculativeToken)
				SpeculativeInsertionWait(xwait, speculativeToken);
			else
				XactLockTableWait(xwait, rel, &itup->t_tid, XLTW_InsertIndex);

			/* start over... */
			if (stack)
				_bt_freestack(stack);
			goto search;
		}

		/* Uniqueness is established -- restore heap tid as scantid */
		if (itup_key->heapkeyspace)
			itup_key->scantid = &itup->t_tid;
	}

	if (checkUnique != UNIQUE_CHECK_EXISTING)
	{
		OffsetNumber newitemoff;

		/*
		 * The only conflict predicate locking cares about for indexes is when
		 * an index tuple insert conflicts with an existing lock.  We don't
		 * know the actual page we're going to insert on for sure just yet in
		 * checkingunique and !heapkeyspace cases, but it's okay to use the
		 * first page the value could be on (with scantid omitted) instead.
		 */
		CheckForSerializableConflictIn(rel, NULL, BufferGetBlockNumber(insertstate.buf));

		/*
		 * Do the insertion.  Note that insertstate contains cached binary
		 * search bounds established within _bt_check_unique when insertion is
		 * checkingunique.
		 */
		newitemoff = _bt_findinsertloc(rel, &insertstate, checkingunique,
									   indexUnchanged, stack, heapRel);
		_bt_insertonpg(rel, heapRel, itup_key, insertstate.buf, InvalidBuffer,
					   stack, itup, insertstate.itemsz, newitemoff,
					   insertstate.postingoff, false);
	}
	else
	{
		/* just release the buffer */
		_bt_relbuf(rel, insertstate.buf);
	}

	/* be tidy */
	if (stack)
		_bt_freestack(stack);
	pfree(itup_key);

	return is_unique;
}

/*
 *	_bt_search_insert() -- _bt_search() wrapper for inserts
 *
 * Search the tree for a particular scankey, or more precisely for the first
 * leaf page it could be on.  Try to make use of the fastpath optimization's
 * rightmost leaf page cache before actually searching the tree from the root
 * page, though.
 *
 * Return value is a stack of parent-page pointers (though see notes about
 * fastpath optimization and page splits below).  insertstate->buf is set to
 * the address of the leaf-page buffer, which is write-locked and pinned in
 * all cases (if necessary by creating a new empty root page for caller).
 *
 * The fastpath optimization avoids most of the work of searching the tree
 * repeatedly when a single backend inserts successive new tuples on the
 * rightmost leaf page of an index.  A backend cache of the rightmost leaf
 * page is maintained within _bt_insertonpg(), and used here.  The cache is
 * invalidated here when an insert of a non-pivot tuple must take place on a
 * non-rightmost leaf page.
 *
 * The optimization helps with indexes on an auto-incremented field.  It also
 * helps with indexes on datetime columns, as well as indexes with lots of
 * NULL values.  (NULLs usually get inserted in the rightmost page for single
 * column indexes, since they usually get treated as coming after everything
 * else in the key space.  Individual NULL tuples will generally be placed on
 * the rightmost leaf page due to the influence of the heap TID column.)
 *
 * Note that we avoid applying the optimization when there is insufficient
 * space on the rightmost page to fit caller's new item.  This is necessary
 * because we'll need to return a real descent stack when a page split is
 * expected (actually, caller can cope with a leaf page split that uses a NULL
 * stack, but that's very slow and so must be avoided).  Note also that the
 * fastpath optimization acquires the lock on the page conditionally as a way
 * of reducing extra contention when there are concurrent insertions into the
 * rightmost page (we give up if we'd have to wait for the lock).  We assume
 * that it isn't useful to apply the optimization when there is contention,
 * since each per-backend cache won't stay valid for long.
 */
static BTStack
_bt_search_insert(Relation rel, Relation heaprel, BTInsertState insertstate)
{
	Assert(insertstate->buf == InvalidBuffer);
	Assert(!insertstate->bounds_valid);
	Assert(insertstate->postingoff == 0);

	if (RelationGetTargetBlock(rel) != InvalidBlockNumber)
	{
		/* Simulate a _bt_getbuf() call with conditional locking */
		insertstate->buf = ReadBuffer(rel, RelationGetTargetBlock(rel));
		if (_bt_conditionallockbuf(rel, insertstate->buf))
		{
			Page		page;
			BTPageOpaque opaque;

			_bt_checkpage(rel, insertstate->buf);
			page = BufferGetPage(insertstate->buf);
			opaque = BTPageGetOpaque(page);

			/*
			 * Check if the page is still the rightmost leaf page and has
			 * enough free space to accommodate the new tuple.  Also check
			 * that the insertion scan key is strictly greater than the first
			 * non-pivot tuple on the page.  (Note that we expect itup_key's
			 * scantid to be unset when our caller is a checkingunique
			 * inserter.)
			 */
			if (P_RIGHTMOST(opaque) &&
				P_ISLEAF(opaque) &&
				!P_IGNORE(opaque) &&
				PageGetFreeSpace(page) > insertstate->itemsz &&
				PageGetMaxOffsetNumber(page) >= P_HIKEY &&
				_bt_compare(rel, insertstate->itup_key, page, P_HIKEY) > 0)
			{
				/*
				 * Caller can use the fastpath optimization because cached
				 * block is still rightmost leaf page, which can fit caller's
				 * new tuple without splitting.  Keep block in local cache for
				 * next insert, and have caller use NULL stack.
				 *
				 * Note that _bt_insert_parent() has an assertion that catches
				 * leaf page splits that somehow follow from a fastpath insert
				 * (it should only be passed a NULL stack when it must deal
				 * with a concurrent root page split, and never because a NULL
				 * stack was returned here).
				 */
				return NULL;
			}

			/* Page unsuitable for caller, drop lock and pin */
			_bt_relbuf(rel, insertstate->buf);
		}
		else
		{
			/* Lock unavailable, drop pin */
			ReleaseBuffer(insertstate->buf);
		}

		/* Forget block, since cache doesn't appear to be useful */
		RelationSetTargetBlock(rel, InvalidBlockNumber);
	}

	/* Cannot use optimization -- descend tree, return proper descent stack */
	return _bt_search(rel, heaprel, insertstate->itup_key, &insertstate->buf,
					  BT_WRITE);
}


/*
 *	_bt_findinsertloc() -- Finds an insert location for a tuple
 *
 *		On entry, insertstate buffer contains the page the new tuple belongs
 *		on.  It is exclusive-locked and pinned by the caller.
 *
 *		If 'checkingunique' is true, the buffer on entry is the first page
 *		that contains duplicates of the new key.  If there are duplicates on
 *		multiple pages, the correct insertion position might be some page to
 *		the right, rather than the first page.  In that case, this function
 *		moves right to the correct target page.
 *
 *		(In a !heapkeyspace index, there can be multiple pages with the same
 *		high key, where the new tuple could legitimately be placed on.  In
 *		that case, the caller passes the first page containing duplicates,
 *		just like when checkingunique=true.  If that page doesn't have enough
 *		room for the new tuple, this function moves right, trying to find a
 *		legal page that does.)
 *
 *		If 'indexUnchanged' is true, this is for an UPDATE that didn't
 *		logically change the indexed value, but must nevertheless have a new
 *		entry to point to a successor version.  This hint from the executor
 *		will influence our behavior when the page might have to be split and
 *		we must consider our options.  Bottom-up index deletion can avoid
 *		pathological version-driven page splits, but we only want to go to the
 *		trouble of trying it when we already have moderate confidence that
 *		it's appropriate.  The hint should not significantly affect our
 *		behavior over time unless practically all inserts on to the leaf page
 *		get the hint.
 *
 *		On exit, insertstate buffer contains the chosen insertion page, and
 *		the offset within that page is returned.  If _bt_findinsertloc needed
 *		to move right, the lock and pin on the original page are released, and
 *		the new buffer is exclusively locked and pinned instead.
 *
 *		If insertstate contains cached binary search bounds, we will take
 *		advantage of them.  This avoids repeating comparisons that we made in
 *		_bt_check_unique() already.
 */
static OffsetNumber
_bt_findinsertloc(Relation rel,
				  BTInsertState insertstate,
				  bool checkingunique,
				  bool indexUnchanged,
				  BTStack stack,
				  Relation heapRel)
{
	BTScanInsert itup_key = insertstate->itup_key;
	Page		page = BufferGetPage(insertstate->buf);
	BTPageOpaque opaque;
	OffsetNumber newitemoff;

	opaque = BTPageGetOpaque(page);

	/* Check 1/3 of a page restriction */
	if (unlikely(insertstate->itemsz > BTMaxItemSize(page)))
		_bt_check_third_page(rel, heapRel, itup_key->heapkeyspace, page,
							 insertstate->itup);

	Assert(P_ISLEAF(opaque) && !P_INCOMPLETE_SPLIT(opaque));
	Assert(!insertstate->bounds_valid || checkingunique);
	Assert(!itup_key->heapkeyspace || itup_key->scantid != NULL);
	Assert(itup_key->heapkeyspace || itup_key->scantid == NULL);
	Assert(!itup_key->allequalimage || itup_key->heapkeyspace);

	if (itup_key->heapkeyspace)
	{
		/* Keep track of whether checkingunique duplicate seen */
		bool		uniquedup = indexUnchanged;

		/*
		 * If we're inserting into a unique index, we may have to walk right
		 * through leaf pages to find the one leaf page that we must insert on
		 * to.
		 *
		 * This is needed for checkingunique callers because a scantid was not
		 * used when we called _bt_search().  scantid can only be set after
		 * _bt_check_unique() has checked for duplicates.  The buffer
		 * initially stored in insertstate->buf has the page where the first
		 * duplicate key might be found, which isn't always the page that new
		 * tuple belongs on.  The heap TID attribute for new tuple (scantid)
		 * could force us to insert on a sibling page, though that should be
		 * very rare in practice.
		 */
		if (checkingunique)
		{
			if (insertstate->low < insertstate->stricthigh)
			{
				/* Encountered a duplicate in _bt_check_unique() */
				Assert(insertstate->bounds_valid);
				uniquedup = true;
			}

			for (;;)
			{
				/*
				 * Does the new tuple belong on this page?
				 *
				 * The earlier _bt_check_unique() call may well have
				 * established a strict upper bound on the offset for the new
				 * item.  If it's not the last item of the page (i.e. if there
				 * is at least one tuple on the page that goes after the tuple
				 * we're inserting) then we know that the tuple belongs on
				 * this page.  We can skip the high key check.
				 */
				if (insertstate->bounds_valid &&
					insertstate->low <= insertstate->stricthigh &&
					insertstate->stricthigh <= PageGetMaxOffsetNumber(page))
					break;

				/* Test '<=', not '!=', since scantid is set now */
				if (P_RIGHTMOST(opaque) ||
					_bt_compare(rel, itup_key, page, P_HIKEY) <= 0)
					break;

				_bt_stepright(rel, heapRel, insertstate, stack);
				/* Update local state after stepping right */
				page = BufferGetPage(insertstate->buf);
				opaque = BTPageGetOpaque(page);
				/* Assume duplicates (if checkingunique) */
				uniquedup = true;
			}
		}

		/*
		 * If the target page cannot fit newitem, try to avoid splitting the
		 * page on insert by performing deletion or deduplication now
		 */
		if (PageGetFreeSpace(page) < insertstate->itemsz)
			_bt_delete_or_dedup_one_page(rel, heapRel, insertstate, false,
										 checkingunique, uniquedup,
										 indexUnchanged);
	}
	else
	{
		/*----------
		 * This is a !heapkeyspace (version 2 or 3) index.  The current page
		 * is the first page that we could insert the new tuple to, but there
		 * may be other pages to the right that we could opt to use instead.
		 *
		 * If the new key is equal to one or more existing keys, we can
		 * legitimately place it anywhere in the series of equal keys.  In
		 * fact, if the new key is equal to the page's "high key" we can place
		 * it on the next page.  If it is equal to the high key, and there's
		 * not room to insert the new tuple on the current page without
		 * splitting, then we move right hoping to find more free space and
		 * avoid a split.
		 *
		 * Keep scanning right until we
		 *		(a) find a page with enough free space,
		 *		(b) reach the last page where the tuple can legally go, or
		 *		(c) get tired of searching.
		 * (c) is not flippant; it is important because if there are many
		 * pages' worth of equal keys, it's better to split one of the early
		 * pages than to scan all the way to the end of the run of equal keys
		 * on every insert.  We implement "get tired" as a random choice,
		 * since stopping after scanning a fixed number of pages wouldn't work
		 * well (we'd never reach the right-hand side of previously split
		 * pages).  The probability of moving right is set at 0.99, which may
		 * seem too high to change the behavior much, but it does an excellent
		 * job of preventing O(N^2) behavior with many equal keys.
		 *----------
		 */
		while (PageGetFreeSpace(page) < insertstate->itemsz)
		{
			/*
			 * Before considering moving right, see if we can obtain enough
			 * space by erasing LP_DEAD items
			 */
			if (P_HAS_GARBAGE(opaque))
			{
				/* Perform simple deletion */
				_bt_delete_or_dedup_one_page(rel, heapRel, insertstate, true,
											 false, false, false);

				if (PageGetFreeSpace(page) >= insertstate->itemsz)
					break;		/* OK, now we have enough space */
			}

			/*
			 * Nope, so check conditions (b) and (c) enumerated above
			 *
			 * The earlier _bt_check_unique() call may well have established a
			 * strict upper bound on the offset for the new item.  If it's not
			 * the last item of the page (i.e. if there is at least one tuple
			 * on the page that's greater than the tuple we're inserting to)
			 * then we know that the tuple belongs on this page.  We can skip
			 * the high key check.
			 */
			if (insertstate->bounds_valid &&
				insertstate->low <= insertstate->stricthigh &&
				insertstate->stricthigh <= PageGetMaxOffsetNumber(page))
				break;

			if (P_RIGHTMOST(opaque) ||
				_bt_compare(rel, itup_key, page, P_HIKEY) != 0 ||
				pg_prng_uint32(&pg_global_prng_state) <= (PG_UINT32_MAX / 100))
				break;

			_bt_stepright(rel, heapRel, insertstate, stack);
			/* Update local state after stepping right */
			page = BufferGetPage(insertstate->buf);
			opaque = BTPageGetOpaque(page);
		}
	}

	/*
	 * We should now be on the correct page.  Find the offset within the page
	 * for the new tuple. (Possibly reusing earlier search bounds.)
	 */
	Assert(P_RIGHTMOST(opaque) ||
		   _bt_compare(rel, itup_key, page, P_HIKEY) <= 0);

	newitemoff = _bt_binsrch_insert(rel, insertstate);

	if (insertstate->postingoff == -1)
	{
		/*
		 * There is an overlapping posting list tuple with its LP_DEAD bit
		 * set.  We don't want to unnecessarily unset its LP_DEAD bit while
		 * performing a posting list split, so perform simple index tuple
		 * deletion early.
		 */
		_bt_delete_or_dedup_one_page(rel, heapRel, insertstate, true,
									 false, false, false);

		/*
		 * Do new binary search.  New insert location cannot overlap with any
		 * posting list now.
		 */
		Assert(!insertstate->bounds_valid);
		insertstate->postingoff = 0;
		newitemoff = _bt_binsrch_insert(rel, insertstate);
		Assert(insertstate->postingoff == 0);
	}

	return newitemoff;
}
