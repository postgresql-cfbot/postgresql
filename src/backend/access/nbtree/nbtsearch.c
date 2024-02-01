/*-------------------------------------------------------------------------
 *
 * nbtsearch.c
 *	  Search code for postgres btrees.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtsearch.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/nbtree.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/predicate.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"


static void _bt_drop_lock_and_maybe_pin(IndexScanDesc scan, BTScanPos sp);
static int	_bt_binsrch_posting(BTScanInsert key, Page page,
								OffsetNumber offnum);
static void _bt_saveitem(BTScanOpaque so, int itemIndex,
						 OffsetNumber offnum, IndexTuple itup);
static int	_bt_setuppostingitems(BTScanOpaque so, int itemIndex,
								  OffsetNumber offnum, ItemPointer heapTid,
								  IndexTuple itup);
static inline void _bt_savepostingitem(BTScanOpaque so, int itemIndex,
									   OffsetNumber offnum,
									   ItemPointer heapTid, int tupleOffset);
static bool _bt_steppage(IndexScanDesc scan, ScanDirection dir);
static bool _bt_readnextpage(IndexScanDesc scan, BlockNumber blkno, ScanDirection dir);
static bool _bt_parallel_readpage(IndexScanDesc scan, BlockNumber blkno,
								  ScanDirection dir);
static Buffer _bt_walk_left(Relation rel, Buffer buf);
static bool _bt_endpoint(IndexScanDesc scan, ScanDirection dir);
static inline void _bt_initialize_more_data(BTScanOpaque so, ScanDirection dir);

#define NBT_FILE "../../backend/access/nbtree/nbtsearch_spec.c"
#include "access/nbtree_spec.h"

/*
 *	_bt_drop_lock_and_maybe_pin()
 *
 * Unlock the buffer; and if it is safe to release the pin, do that, too.
 * This will prevent vacuum from stalling in a blocked state trying to read a
 * page when a cursor is sitting on it.
 *
 * See nbtree/README section on making concurrent TID recycling safe.
 */
static void
_bt_drop_lock_and_maybe_pin(IndexScanDesc scan, BTScanPos sp)
{
	_bt_unlockbuf(scan->indexRelation, sp->buf);

	if (IsMVCCSnapshot(scan->xs_snapshot) &&
		RelationNeedsWAL(scan->indexRelation) &&
		!scan->xs_want_itup)
	{
		ReleaseBuffer(sp->buf);
		sp->buf = InvalidBuffer;
	}
}

/*----------
 *	_bt_binsrch_posting() -- posting list binary search.
 *
 * Helper routine for _bt_binsrch_insert().
 *
 * Returns offset into posting list where caller's scantid belongs.
 *----------
 */
static int
_bt_binsrch_posting(BTScanInsert key, Page page, OffsetNumber offnum)
{
	IndexTuple	itup;
	ItemId		itemid;
	int			low,
				high,
				mid,
				res;

	/*
	 * If this isn't a posting tuple, then the index must be corrupt (if it is
	 * an ordinary non-pivot tuple then there must be an existing tuple with a
	 * heap TID that equals inserter's new heap TID/scantid).  Defensively
	 * check that tuple is a posting list tuple whose posting list range
	 * includes caller's scantid.
	 *
	 * (This is also needed because contrib/amcheck's rootdescend option needs
	 * to be able to relocate a non-pivot tuple using _bt_binsrch_insert().)
	 */
	itemid = PageGetItemId(page, offnum);
	itup = (IndexTuple) PageGetItem(page, itemid);
	if (!BTreeTupleIsPosting(itup))
		return 0;

	Assert(key->heapkeyspace && key->allequalimage);

	/*
	 * In the event that posting list tuple has LP_DEAD bit set, indicate this
	 * to _bt_binsrch_insert() caller by returning -1, a sentinel value.  A
	 * second call to _bt_binsrch_insert() can take place when its caller has
	 * removed the dead item.
	 */
	if (ItemIdIsDead(itemid))
		return -1;

	/* "high" is past end of posting list for loop invariant */
	low = 0;
	high = BTreeTupleGetNPosting(itup);
	Assert(high >= 2);

	while (high > low)
	{
		mid = low + ((high - low) / 2);
		res = ItemPointerCompare(key->scantid,
								 BTreeTupleGetPostingN(itup, mid));

		if (res > 0)
			low = mid + 1;
		else if (res < 0)
			high = mid;
		else
			return mid;
	}

	/* Exact match not found */
	return low;
}

/*
 *	_bt_first() -- Find the first item in a scan.
 *
 *		We need to be clever about the direction of scan, the search
 *		conditions, and the tree ordering.  We find the first item (or,
 *		if backwards scan, the last item) in the tree that satisfies the
 *		qualifications in the scan key.  On success exit, the page containing
 *		the current index tuple is pinned but not locked, and data about
 *		the matching tuple(s) on the page has been loaded into so->currPos.
 *		scan->xs_ctup.t_self is set to the heap TID of the current tuple,
 *		and if requested, scan->xs_itup points to a copy of the index tuple.
 *
 * If there are no matching items in the index, we return false, with no
 * pins or locks held.
 *
 * Note that scan->keyData[], and the so->keyData[] scankey built from it,
 * are both search-type scankeys (see nbtree/README for more about this).
 * Within this routine, we build a temporary insertion-type scankey to use
 * in locating the scan start position.
 */
bool
_bt_first(IndexScanDesc scan, ScanDirection dir)
{
	Relation	rel = scan->indexRelation;
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	Buffer		buf;
	BTStack		stack;
	OffsetNumber offnum;
	StrategyNumber strat;
	BTScanInsertData inskey;
	ScanKey		startKeys[INDEX_MAX_KEYS];
	ScanKeyData notnullkeys[INDEX_MAX_KEYS];
	int			keysz = 0;
	int			i;
	bool		status;
	StrategyNumber strat_total;
	BTScanPosItem *currItem;
	BlockNumber blkno;
	nbts_prep_ctx(rel);

	Assert(!BTScanPosIsValid(so->currPos));

	pgstat_count_index_scan(rel);

	/*
	 * Examine the scan keys and eliminate any redundant keys; also mark the
	 * keys that must be matched to continue the scan.
	 */
	_bt_preprocess_keys(scan);

	/*
	 * Quit now if _bt_preprocess_keys() discovered that the scan keys can
	 * never be satisfied (eg, x == 1 AND x > 2).
	 */
	if (!so->qual_ok)
	{
		/* Notify any other workers that we're done with this scan key. */
		_bt_parallel_done(scan);
		return false;
	}

	/*
	 * For parallel scans, get the starting page from shared state. If the
	 * scan has not started, proceed to find out first leaf page in the usual
	 * way while keeping other participating processes waiting.  If the scan
	 * has already begun, use the page number from the shared structure.
	 */
	if (scan->parallel_scan != NULL)
	{
		status = _bt_parallel_seize(scan, &blkno);
		if (!status)
			return false;
		else if (blkno == P_NONE)
		{
			_bt_parallel_done(scan);
			return false;
		}
		else if (blkno != InvalidBlockNumber)
		{
			if (!_bt_parallel_readpage(scan, blkno, dir))
				return false;
			goto readcomplete;
		}
	}

	/*----------
	 * Examine the scan keys to discover where we need to start the scan.
	 *
	 * We want to identify the keys that can be used as starting boundaries;
	 * these are =, >, or >= keys for a forward scan or =, <, <= keys for
	 * a backwards scan.  We can use keys for multiple attributes so long as
	 * the prior attributes had only =, >= (resp. =, <=) keys.  Once we accept
	 * a > or < boundary or find an attribute with no boundary (which can be
	 * thought of as the same as "> -infinity"), we can't use keys for any
	 * attributes to its right, because it would break our simplistic notion
	 * of what initial positioning strategy to use.
	 *
	 * When the scan keys include cross-type operators, _bt_preprocess_keys
	 * may not be able to eliminate redundant keys; in such cases we will
	 * arbitrarily pick a usable one for each attribute.  This is correct
	 * but possibly not optimal behavior.  (For example, with keys like
	 * "x >= 4 AND x >= 5" we would elect to scan starting at x=4 when
	 * x=5 would be more efficient.)  Since the situation only arises given
	 * a poorly-worded query plus an incomplete opfamily, live with it.
	 *
	 * When both equality and inequality keys appear for a single attribute
	 * (again, only possible when cross-type operators appear), we *must*
	 * select one of the equality keys for the starting point, because
	 * _bt_checkkeys() will stop the scan as soon as an equality qual fails.
	 * For example, if we have keys like "x >= 4 AND x = 10" and we elect to
	 * start at x=4, we will fail and stop before reaching x=10.  If multiple
	 * equality quals survive preprocessing, however, it doesn't matter which
	 * one we use --- by definition, they are either redundant or
	 * contradictory.
	 *
	 * Any regular (not SK_SEARCHNULL) key implies a NOT NULL qualifier.
	 * If the index stores nulls at the end of the index we'll be starting
	 * from, and we have no boundary key for the column (which means the key
	 * we deduced NOT NULL from is an inequality key that constrains the other
	 * end of the index), then we cons up an explicit SK_SEARCHNOTNULL key to
	 * use as a boundary key.  If we didn't do this, we might find ourselves
	 * traversing a lot of null entries at the start of the scan.
	 *
	 * In this loop, row-comparison keys are treated the same as keys on their
	 * first (leftmost) columns.  We'll add on lower-order columns of the row
	 * comparison below, if possible.
	 *
	 * The selected scan keys (at most one per index column) are remembered by
	 * storing their addresses into the local startKeys[] array.
	 *----------
	 */
	strat_total = BTEqualStrategyNumber;
	if (so->numberOfKeys > 0)
	{
		AttrNumber	curattr;
		ScanKey		chosen;
		ScanKey		impliesNN;
		ScanKey		cur;

		/*
		 * chosen is the so-far-chosen key for the current attribute, if any.
		 * We don't cast the decision in stone until we reach keys for the
		 * next attribute.
		 */
		curattr = 1;
		chosen = NULL;
		/* Also remember any scankey that implies a NOT NULL constraint */
		impliesNN = NULL;

		/*
		 * Loop iterates from 0 to numberOfKeys inclusive; we use the last
		 * pass to handle after-last-key processing.  Actual exit from the
		 * loop is at one of the "break" statements below.
		 */
		for (cur = so->keyData, i = 0;; cur++, i++)
		{
			if (i >= so->numberOfKeys || cur->sk_attno != curattr)
			{
				/*
				 * Done looking at keys for curattr.  If we didn't find a
				 * usable boundary key, see if we can deduce a NOT NULL key.
				 */
				if (chosen == NULL && impliesNN != NULL &&
					((impliesNN->sk_flags & SK_BT_NULLS_FIRST) ?
					 ScanDirectionIsForward(dir) :
					 ScanDirectionIsBackward(dir)))
				{
					/* Yes, so build the key in notnullkeys[keysCount] */
					chosen = &notnullkeys[keysz];
					ScanKeyEntryInitialize(chosen,
										   (SK_SEARCHNOTNULL | SK_ISNULL |
											(impliesNN->sk_flags &
											 (SK_BT_DESC | SK_BT_NULLS_FIRST))),
										   curattr,
										   ((impliesNN->sk_flags & SK_BT_NULLS_FIRST) ?
											BTGreaterStrategyNumber :
											BTLessStrategyNumber),
										   InvalidOid,
										   InvalidOid,
										   InvalidOid,
										   (Datum) 0);
				}

				/*
				 * If we still didn't find a usable boundary key, quit; else
				 * save the boundary key pointer in startKeys.
				 */
				if (chosen == NULL)
					break;
				startKeys[keysz++] = chosen;

				/*
				 * Adjust strat_total, and quit if we have stored a > or <
				 * key.
				 */
				strat = chosen->sk_strategy;
				if (strat != BTEqualStrategyNumber)
				{
					strat_total = strat;
					if (strat == BTGreaterStrategyNumber ||
						strat == BTLessStrategyNumber)
						break;
				}

				/*
				 * Done if that was the last attribute, or if next key is not
				 * in sequence (implying no boundary key is available for the
				 * next attribute).
				 */
				if (i >= so->numberOfKeys ||
					cur->sk_attno != curattr + 1)
					break;

				/*
				 * Reset for next attr.
				 */
				curattr = cur->sk_attno;
				chosen = NULL;
				impliesNN = NULL;
			}

			/*
			 * Can we use this key as a starting boundary for this attr?
			 *
			 * If not, does it imply a NOT NULL constraint?  (Because
			 * SK_SEARCHNULL keys are always assigned BTEqualStrategyNumber,
			 * *any* inequality key works for that; we need not test.)
			 */
			switch (cur->sk_strategy)
			{
				case BTLessStrategyNumber:
				case BTLessEqualStrategyNumber:
					if (chosen == NULL)
					{
						if (ScanDirectionIsBackward(dir))
							chosen = cur;
						else
							impliesNN = cur;
					}
					break;
				case BTEqualStrategyNumber:
					/* override any non-equality choice */
					chosen = cur;
					break;
				case BTGreaterEqualStrategyNumber:
				case BTGreaterStrategyNumber:
					if (chosen == NULL)
					{
						if (ScanDirectionIsForward(dir))
							chosen = cur;
						else
							impliesNN = cur;
					}
					break;
			}
		}
	}

	/*
	 * If we found no usable boundary keys, we have to start from one end of
	 * the tree.  Walk down that edge to the first or last key, and scan from
	 * there.
	 */
	if (keysz == 0)
	{
		bool		match;

		match = _bt_endpoint(scan, dir);

		if (!match)
		{
			/* No match, so mark (parallel) scan finished */
			_bt_parallel_done(scan);
		}

		return match;
	}

	/*
	 * We want to start the scan somewhere within the index.  Set up an
	 * insertion scankey we can use to search for the boundary point we
	 * identified above.  The insertion scankey is built using the keys
	 * identified by startKeys[].  (Remaining insertion scankey fields are
	 * initialized after initial-positioning strategy is finalized.)
	 */
	Assert(keysz <= INDEX_MAX_KEYS);
	for (i = 0; i < keysz; i++)
	{
		ScanKey		cur = startKeys[i];

		Assert(cur->sk_attno == i + 1);

		if (cur->sk_flags & SK_ROW_HEADER)
		{
			/*
			 * Row comparison header: look to the first row member instead.
			 *
			 * The member scankeys are already in insertion format (ie, they
			 * have sk_func = 3-way-comparison function), but we have to watch
			 * out for nulls, which _bt_preprocess_keys didn't check. A null
			 * in the first row member makes the condition unmatchable, just
			 * like qual_ok = false.
			 */
			ScanKey		subkey = (ScanKey) DatumGetPointer(cur->sk_argument);

			Assert(subkey->sk_flags & SK_ROW_MEMBER);
			if (subkey->sk_flags & SK_ISNULL)
			{
				_bt_parallel_done(scan);
				return false;
			}
			memcpy(inskey.scankeys + i, subkey, sizeof(ScanKeyData));

			/*
			 * If the row comparison is the last positioning key we accepted,
			 * try to add additional keys from the lower-order row members.
			 * (If we accepted independent conditions on additional index
			 * columns, we use those instead --- doesn't seem worth trying to
			 * determine which is more restrictive.)  Note that this is OK
			 * even if the row comparison is of ">" or "<" type, because the
			 * condition applied to all but the last row member is effectively
			 * ">=" or "<=", and so the extra keys don't break the positioning
			 * scheme.  But, by the same token, if we aren't able to use all
			 * the row members, then the part of the row comparison that we
			 * did use has to be treated as just a ">=" or "<=" condition, and
			 * so we'd better adjust strat_total accordingly.
			 */
			if (i == keysz - 1)
			{
				bool		used_all_subkeys = false;

				Assert(!(subkey->sk_flags & SK_ROW_END));
				for (;;)
				{
					subkey++;
					Assert(subkey->sk_flags & SK_ROW_MEMBER);
					if (subkey->sk_attno != keysz + 1)
						break;	/* out-of-sequence, can't use it */
					if (subkey->sk_strategy != cur->sk_strategy)
						break;	/* wrong direction, can't use it */
					if (subkey->sk_flags & SK_ISNULL)
						break;	/* can't use null keys */
					Assert(keysz < INDEX_MAX_KEYS);
					memcpy(inskey.scankeys + keysz, subkey,
						   sizeof(ScanKeyData));
					keysz++;
					if (subkey->sk_flags & SK_ROW_END)
					{
						used_all_subkeys = true;
						break;
					}
				}
				if (!used_all_subkeys)
				{
					switch (strat_total)
					{
						case BTLessStrategyNumber:
							strat_total = BTLessEqualStrategyNumber;
							break;
						case BTGreaterStrategyNumber:
							strat_total = BTGreaterEqualStrategyNumber;
							break;
					}
				}
				break;			/* done with outer loop */
			}
		}
		else
		{
			/*
			 * Ordinary comparison key.  Transform the search-style scan key
			 * to an insertion scan key by replacing the sk_func with the
			 * appropriate btree comparison function.
			 *
			 * If scankey operator is not a cross-type comparison, we can use
			 * the cached comparison function; otherwise gotta look it up in
			 * the catalogs.  (That can't lead to infinite recursion, since no
			 * indexscan initiated by syscache lookup will use cross-data-type
			 * operators.)
			 *
			 * We support the convention that sk_subtype == InvalidOid means
			 * the opclass input type; this is a hack to simplify life for
			 * ScanKeyInit().
			 */
			if (cur->sk_subtype == rel->rd_opcintype[i] ||
				cur->sk_subtype == InvalidOid)
			{
				FmgrInfo   *procinfo;

				procinfo = index_getprocinfo(rel, cur->sk_attno, BTORDER_PROC);
				ScanKeyEntryInitializeWithInfo(inskey.scankeys + i,
											   cur->sk_flags,
											   cur->sk_attno,
											   InvalidStrategy,
											   cur->sk_subtype,
											   cur->sk_collation,
											   procinfo,
											   cur->sk_argument);
			}
			else
			{
				RegProcedure cmp_proc;

				cmp_proc = get_opfamily_proc(rel->rd_opfamily[i],
											 rel->rd_opcintype[i],
											 cur->sk_subtype,
											 BTORDER_PROC);
				if (!RegProcedureIsValid(cmp_proc))
					elog(ERROR, "missing support function %d(%u,%u) for attribute %d of index \"%s\"",
						 BTORDER_PROC, rel->rd_opcintype[i], cur->sk_subtype,
						 cur->sk_attno, RelationGetRelationName(rel));
				ScanKeyEntryInitialize(inskey.scankeys + i,
									   cur->sk_flags,
									   cur->sk_attno,
									   InvalidStrategy,
									   cur->sk_subtype,
									   cur->sk_collation,
									   cmp_proc,
									   cur->sk_argument);
			}
		}
	}

	/*----------
	 * Examine the selected initial-positioning strategy to determine exactly
	 * where we need to start the scan, and set flag variables to control the
	 * initial descent by _bt_search (and our _bt_binsrch call for the leaf
	 * page _bt_search returns).
	 *----------
	 */
	_bt_metaversion(rel, &inskey.heapkeyspace, &inskey.allequalimage);
	inskey.anynullkeys = false; /* unused */
	inskey.scantid = NULL;
	inskey.keysz = keysz;
	switch (strat_total)
	{
		case BTLessStrategyNumber:

			inskey.nextkey = false;
			inskey.backward = true;
			break;

		case BTLessEqualStrategyNumber:

			inskey.nextkey = true;
			inskey.backward = true;
			break;

		case BTEqualStrategyNumber:

			/*
			 * If a backward scan was specified, need to start with last equal
			 * item not first one.
			 */
			if (ScanDirectionIsBackward(dir))
			{
				/*
				 * This is the same as the <= strategy
				 */
				inskey.nextkey = true;
				inskey.backward = true;
			}
			else
			{
				/*
				 * This is the same as the >= strategy
				 */
				inskey.nextkey = false;
				inskey.backward = false;
			}
			break;

		case BTGreaterEqualStrategyNumber:

			/*
			 * Find first item >= scankey
			 */
			inskey.nextkey = false;
			inskey.backward = false;
			break;

		case BTGreaterStrategyNumber:

			/*
			 * Find first item > scankey
			 */
			inskey.nextkey = true;
			inskey.backward = false;
			break;

		default:
			/* can't get here, but keep compiler quiet */
			elog(ERROR, "unrecognized strat_total: %d", (int) strat_total);
			return false;
	}

	/*
	 * Use the manufactured insertion scan key to descend the tree and
	 * position ourselves on the target leaf page.
	 */
	Assert(ScanDirectionIsBackward(dir) == inskey.backward);
	stack = _bt_search(rel, NULL, &inskey, &buf, BT_READ);

	/* don't need to keep the stack around... */
	_bt_freestack(stack);

	if (!BufferIsValid(buf))
	{
		/*
		 * We only get here if the index is completely empty. Lock relation
		 * because nothing finer to lock exists.  Without a buffer lock, it's
		 * possible for another transaction to insert data between
		 * _bt_search() and PredicateLockRelation().  We have to try again
		 * after taking the relation-level predicate lock, to close a narrow
		 * window where we wouldn't scan concurrently inserted tuples, but the
		 * writer wouldn't see our predicate lock.
		 */
		if (IsolationIsSerializable())
		{
			PredicateLockRelation(rel, scan->xs_snapshot);
			stack = _bt_search(rel, NULL, &inskey, &buf, BT_READ);
			_bt_freestack(stack);
		}

		if (!BufferIsValid(buf))
		{
			/*
			 * Mark parallel scan as done, so that all the workers can finish
			 * their scan.
			 */
			_bt_parallel_done(scan);
			BTScanPosInvalidate(so->currPos);
			return false;
		}
	}

	PredicateLockPage(rel, BufferGetBlockNumber(buf), scan->xs_snapshot);

	_bt_initialize_more_data(so, dir);

	/* position to the precise item on the page */
	offnum = _bt_binsrch(rel, &inskey, buf);
	Assert(!BTScanPosIsValid(so->currPos));
	so->currPos.buf = buf;

	/*
	 * Now load data from the first page of the scan.
	 *
	 * If inskey.nextkey = false and inskey.backward = false, offnum is
	 * positioned at the first non-pivot tuple >= inskey.scankeys.
	 *
	 * If inskey.nextkey = false and inskey.backward = true, offnum is
	 * positioned at the last non-pivot tuple < inskey.scankeys.
	 *
	 * If inskey.nextkey = true and inskey.backward = false, offnum is
	 * positioned at the first non-pivot tuple > inskey.scankeys.
	 *
	 * If inskey.nextkey = true and inskey.backward = true, offnum is
	 * positioned at the last non-pivot tuple <= inskey.scankeys.
	 *
	 * It's possible that _bt_binsrch returned an offnum that is out of bounds
	 * for the page.  For example, when inskey is both < the leaf page's high
	 * key and > all of its non-pivot tuples, offnum will be "maxoff + 1".
	 */
	if (!_bt_readpage(scan, dir, offnum, true))
	{
		/*
		 * There's no actually-matching data on this page.  Try to advance to
		 * the next page.  Return false if there's no matching data at all.
		 */
		_bt_unlockbuf(scan->indexRelation, so->currPos.buf);
		if (!_bt_steppage(scan, dir))
			return false;
	}
	else
	{
		/* We have at least one item to return as scan's first item */
		_bt_drop_lock_and_maybe_pin(scan, &so->currPos);
	}

readcomplete:
	/* OK, itemIndex says what to return */
	currItem = &so->currPos.items[so->currPos.itemIndex];
	scan->xs_heaptid = currItem->heapTid;
	if (scan->xs_want_itup)
		scan->xs_itup = (IndexTuple) (so->currTuples + currItem->tupleOffset);

	return true;
}

/*
 *	_bt_next() -- Get the next item in a scan.
 *
 *		On entry, so->currPos describes the current page, which may be pinned
 *		but is not locked, and so->currPos.itemIndex identifies which item was
 *		previously returned.
 *
 *		On successful exit, scan->xs_ctup.t_self is set to the TID of the
 *		next heap tuple, and if requested, scan->xs_itup points to a copy of
 *		the index tuple.  so->currPos is updated as needed.
 *
 *		On failure exit (no more tuples), we release pin and set
 *		so->currPos.buf to InvalidBuffer.
 */
bool
_bt_next(IndexScanDesc scan, ScanDirection dir)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	BTScanPosItem *currItem;

	/*
	 * Advance to next tuple on current page; or if there's no more, try to
	 * step to the next page with data.
	 */
	if (ScanDirectionIsForward(dir))
	{
		if (++so->currPos.itemIndex > so->currPos.lastItem)
		{
			if (!_bt_steppage(scan, dir))
				return false;
		}
	}
	else
	{
		if (--so->currPos.itemIndex < so->currPos.firstItem)
		{
			if (!_bt_steppage(scan, dir))
				return false;
		}
	}

	/* OK, itemIndex says what to return */
	currItem = &so->currPos.items[so->currPos.itemIndex];
	scan->xs_heaptid = currItem->heapTid;
	if (scan->xs_want_itup)
		scan->xs_itup = (IndexTuple) (so->currTuples + currItem->tupleOffset);

	return true;
}

/* Save an index item into so->currPos.items[itemIndex] */
static void
_bt_saveitem(BTScanOpaque so, int itemIndex,
			 OffsetNumber offnum, IndexTuple itup)
{
	BTScanPosItem *currItem = &so->currPos.items[itemIndex];

	Assert(!BTreeTupleIsPivot(itup) && !BTreeTupleIsPosting(itup));

	currItem->heapTid = itup->t_tid;
	currItem->indexOffset = offnum;
	if (so->currTuples)
	{
		Size		itupsz = IndexTupleSize(itup);

		currItem->tupleOffset = so->currPos.nextTupleOffset;
		memcpy(so->currTuples + so->currPos.nextTupleOffset, itup, itupsz);
		so->currPos.nextTupleOffset += MAXALIGN(itupsz);
	}
}

/*
 * Setup state to save TIDs/items from a single posting list tuple.
 *
 * Saves an index item into so->currPos.items[itemIndex] for TID that is
 * returned to scan first.  Second or subsequent TIDs for posting list should
 * be saved by calling _bt_savepostingitem().
 *
 * Returns an offset into tuple storage space that main tuple is stored at if
 * needed.
 */
static int
_bt_setuppostingitems(BTScanOpaque so, int itemIndex, OffsetNumber offnum,
					  ItemPointer heapTid, IndexTuple itup)
{
	BTScanPosItem *currItem = &so->currPos.items[itemIndex];

	Assert(BTreeTupleIsPosting(itup));

	currItem->heapTid = *heapTid;
	currItem->indexOffset = offnum;
	if (so->currTuples)
	{
		/* Save base IndexTuple (truncate posting list) */
		IndexTuple	base;
		Size		itupsz = BTreeTupleGetPostingOffset(itup);

		itupsz = MAXALIGN(itupsz);
		currItem->tupleOffset = so->currPos.nextTupleOffset;
		base = (IndexTuple) (so->currTuples + so->currPos.nextTupleOffset);
		memcpy(base, itup, itupsz);
		/* Defensively reduce work area index tuple header size */
		base->t_info &= ~INDEX_SIZE_MASK;
		base->t_info |= itupsz;
		so->currPos.nextTupleOffset += itupsz;

		return currItem->tupleOffset;
	}

	return 0;
}

/*
 * Save an index item into so->currPos.items[itemIndex] for current posting
 * tuple.
 *
 * Assumes that _bt_setuppostingitems() has already been called for current
 * posting list tuple.  Caller passes its return value as tupleOffset.
 */
static inline void
_bt_savepostingitem(BTScanOpaque so, int itemIndex, OffsetNumber offnum,
					ItemPointer heapTid, int tupleOffset)
{
	BTScanPosItem *currItem = &so->currPos.items[itemIndex];

	currItem->heapTid = *heapTid;
	currItem->indexOffset = offnum;

	/*
	 * Have index-only scans return the same base IndexTuple for every TID
	 * that originates from the same posting list
	 */
	if (so->currTuples)
		currItem->tupleOffset = tupleOffset;
}

/*
 *	_bt_steppage() -- Step to next page containing valid data for scan
 *
 * On entry, if so->currPos.buf is valid the buffer is pinned but not locked;
 * if pinned, we'll drop the pin before moving to next page.  The buffer is
 * not locked on entry.
 *
 * For success on a scan using a non-MVCC snapshot we hold a pin, but not a
 * read lock, on that page.  If we do not hold the pin, we set so->currPos.buf
 * to InvalidBuffer.  We return true to indicate success.
 */
static bool
_bt_steppage(IndexScanDesc scan, ScanDirection dir)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	BlockNumber blkno = InvalidBlockNumber;
	bool		status;

	Assert(BTScanPosIsValid(so->currPos));

	/* Before leaving current page, deal with any killed items */
	if (so->numKilled > 0)
		_bt_killitems(scan);

	/*
	 * Before we modify currPos, make a copy of the page data if there was a
	 * mark position that needs it.
	 */
	if (so->markItemIndex >= 0)
	{
		/* bump pin on current buffer for assignment to mark buffer */
		if (BTScanPosIsPinned(so->currPos))
			IncrBufferRefCount(so->currPos.buf);
		memcpy(&so->markPos, &so->currPos,
			   offsetof(BTScanPosData, items[1]) +
			   so->currPos.lastItem * sizeof(BTScanPosItem));
		if (so->markTuples)
			memcpy(so->markTuples, so->currTuples,
				   so->currPos.nextTupleOffset);
		so->markPos.itemIndex = so->markItemIndex;
		so->markItemIndex = -1;
	}

	if (ScanDirectionIsForward(dir))
	{
		/* Walk right to the next page with data */
		if (scan->parallel_scan != NULL)
		{
			/*
			 * Seize the scan to get the next block number; if the scan has
			 * ended already, bail out.
			 */
			status = _bt_parallel_seize(scan, &blkno);
			if (!status)
			{
				/* release the previous buffer, if pinned */
				BTScanPosUnpinIfPinned(so->currPos);
				BTScanPosInvalidate(so->currPos);
				return false;
			}
		}
		else
		{
			/* Not parallel, so use the previously-saved nextPage link. */
			blkno = so->currPos.nextPage;
		}

		/* Remember we left a page with data */
		so->currPos.moreLeft = true;

		/* release the previous buffer, if pinned */
		BTScanPosUnpinIfPinned(so->currPos);
	}
	else
	{
		/* Remember we left a page with data */
		so->currPos.moreRight = true;

		if (scan->parallel_scan != NULL)
		{
			/*
			 * Seize the scan to get the current block number; if the scan has
			 * ended already, bail out.
			 */
			status = _bt_parallel_seize(scan, &blkno);
			BTScanPosUnpinIfPinned(so->currPos);
			if (!status)
			{
				BTScanPosInvalidate(so->currPos);
				return false;
			}
		}
		else
		{
			/* Not parallel, so just use our own notion of the current page */
			blkno = so->currPos.currPage;
		}
	}

	if (!_bt_readnextpage(scan, blkno, dir))
		return false;

	/* We have at least one item to return as scan's next item */
	_bt_drop_lock_and_maybe_pin(scan, &so->currPos);

	return true;
}

/*
 *	_bt_readnextpage() -- Read next page containing valid data for scan
 *
 * On success exit, so->currPos is updated to contain data from the next
 * interesting page, and we return true.  Caller must release the lock (and
 * maybe the pin) on the buffer on success exit.
 *
 * If there are no more matching records in the given direction, we drop all
 * locks and pins, set so->currPos.buf to InvalidBuffer, and return false.
 */
static bool
_bt_readnextpage(IndexScanDesc scan, BlockNumber blkno, ScanDirection dir)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	Relation	rel = scan->indexRelation;
	Page		page;
	BTPageOpaque opaque;
	bool		status;
	nbts_prep_ctx(rel);

	if (ScanDirectionIsForward(dir))
	{
		for (;;)
		{
			/*
			 * if we're at end of scan, give up and mark parallel scan as
			 * done, so that all the workers can finish their scan
			 */
			if (blkno == P_NONE || !so->currPos.moreRight)
			{
				_bt_parallel_done(scan);
				BTScanPosInvalidate(so->currPos);
				return false;
			}
			/* check for interrupts while we're not holding any buffer lock */
			CHECK_FOR_INTERRUPTS();
			/* step right one page */
			so->currPos.buf = _bt_getbuf(rel, blkno, BT_READ);
			page = BufferGetPage(so->currPos.buf);
			opaque = BTPageGetOpaque(page);
			/* check for deleted page */
			if (!P_IGNORE(opaque))
			{
				PredicateLockPage(rel, blkno, scan->xs_snapshot);
				/* see if there are any matches on this page */
				/* note that this will clear moreRight if we can stop */
				if (_bt_readpage(scan, dir, P_FIRSTDATAKEY(opaque), false))
					break;
			}
			else if (scan->parallel_scan != NULL)
			{
				/* allow next page be processed by parallel worker */
				_bt_parallel_release(scan, opaque->btpo_next);
			}

			/* nope, keep going */
			if (scan->parallel_scan != NULL)
			{
				_bt_relbuf(rel, so->currPos.buf);
				status = _bt_parallel_seize(scan, &blkno);
				if (!status)
				{
					BTScanPosInvalidate(so->currPos);
					return false;
				}
			}
			else
			{
				blkno = opaque->btpo_next;
				_bt_relbuf(rel, so->currPos.buf);
			}
		}
	}
	else
	{
		/*
		 * Should only happen in parallel cases, when some other backend
		 * advanced the scan.
		 */
		if (so->currPos.currPage != blkno)
		{
			BTScanPosUnpinIfPinned(so->currPos);
			so->currPos.currPage = blkno;
		}

		/*
		 * Walk left to the next page with data.  This is much more complex
		 * than the walk-right case because of the possibility that the page
		 * to our left splits while we are in flight to it, plus the
		 * possibility that the page we were on gets deleted after we leave
		 * it.  See nbtree/README for details.
		 *
		 * It might be possible to rearrange this code to have less overhead
		 * in pinning and locking, but that would require capturing the left
		 * sibling block number when the page is initially read, and then
		 * optimistically starting there (rather than pinning the page twice).
		 * It is not clear that this would be worth the complexity.
		 */
		if (BTScanPosIsPinned(so->currPos))
			_bt_lockbuf(rel, so->currPos.buf, BT_READ);
		else
			so->currPos.buf = _bt_getbuf(rel, so->currPos.currPage, BT_READ);

		for (;;)
		{
			/* Done if we know there are no matching keys to the left */
			if (!so->currPos.moreLeft)
			{
				_bt_relbuf(rel, so->currPos.buf);
				_bt_parallel_done(scan);
				BTScanPosInvalidate(so->currPos);
				return false;
			}

			/* Step to next physical page */
			so->currPos.buf = _bt_walk_left(rel, so->currPos.buf);

			/* if we're physically at end of index, return failure */
			if (so->currPos.buf == InvalidBuffer)
			{
				_bt_parallel_done(scan);
				BTScanPosInvalidate(so->currPos);
				return false;
			}

			/*
			 * Okay, we managed to move left to a non-deleted page. Done if
			 * it's not half-dead and contains matching tuples. Else loop back
			 * and do it all again.
			 */
			page = BufferGetPage(so->currPos.buf);
			opaque = BTPageGetOpaque(page);
			if (!P_IGNORE(opaque))
			{
				PredicateLockPage(rel, BufferGetBlockNumber(so->currPos.buf), scan->xs_snapshot);
				/* see if there are any matches on this page */
				/* note that this will clear moreLeft if we can stop */
				if (_bt_readpage(scan, dir, PageGetMaxOffsetNumber(page), false))
					break;
			}
			else if (scan->parallel_scan != NULL)
			{
				/* allow next page be processed by parallel worker */
				_bt_parallel_release(scan, BufferGetBlockNumber(so->currPos.buf));
			}

			/*
			 * For parallel scans, get the last page scanned as it is quite
			 * possible that by the time we try to seize the scan, some other
			 * worker has already advanced the scan to a different page.  We
			 * must continue based on the latest page scanned by any worker.
			 */
			if (scan->parallel_scan != NULL)
			{
				_bt_relbuf(rel, so->currPos.buf);
				status = _bt_parallel_seize(scan, &blkno);
				if (!status)
				{
					BTScanPosInvalidate(so->currPos);
					return false;
				}
				so->currPos.buf = _bt_getbuf(rel, blkno, BT_READ);
			}
		}
	}

	return true;
}

/*
 *	_bt_parallel_readpage() -- Read current page containing valid data for scan
 *
 * On success, release lock and maybe pin on buffer.  We return true to
 * indicate success.
 */
static bool
_bt_parallel_readpage(IndexScanDesc scan, BlockNumber blkno, ScanDirection dir)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;

	_bt_initialize_more_data(so, dir);

	if (!_bt_readnextpage(scan, blkno, dir))
		return false;

	/* We have at least one item to return as scan's next item */
	_bt_drop_lock_and_maybe_pin(scan, &so->currPos);

	return true;
}

/*
 * _bt_walk_left() -- step left one page, if possible
 *
 * The given buffer must be pinned and read-locked.  This will be dropped
 * before stepping left.  On return, we have pin and read lock on the
 * returned page, instead.
 *
 * Returns InvalidBuffer if there is no page to the left (no lock is held
 * in that case).
 *
 * It is possible for the returned leaf page to be half-dead; caller must
 * check that condition and step left again when required.
 */
static Buffer
_bt_walk_left(Relation rel, Buffer buf)
{
	Page		page;
	BTPageOpaque opaque;

	page = BufferGetPage(buf);
	opaque = BTPageGetOpaque(page);

	for (;;)
	{
		BlockNumber obknum;
		BlockNumber lblkno;
		BlockNumber blkno;
		int			tries;

		/* if we're at end of tree, release buf and return failure */
		if (P_LEFTMOST(opaque))
		{
			_bt_relbuf(rel, buf);
			break;
		}
		/* remember original page we are stepping left from */
		obknum = BufferGetBlockNumber(buf);
		/* step left */
		blkno = lblkno = opaque->btpo_prev;
		_bt_relbuf(rel, buf);
		/* check for interrupts while we're not holding any buffer lock */
		CHECK_FOR_INTERRUPTS();
		buf = _bt_getbuf(rel, blkno, BT_READ);
		page = BufferGetPage(buf);
		opaque = BTPageGetOpaque(page);

		/*
		 * If this isn't the page we want, walk right till we find what we
		 * want --- but go no more than four hops (an arbitrary limit). If we
		 * don't find the correct page by then, the most likely bet is that
		 * the original page got deleted and isn't in the sibling chain at all
		 * anymore, not that its left sibling got split more than four times.
		 *
		 * Note that it is correct to test P_ISDELETED not P_IGNORE here,
		 * because half-dead pages are still in the sibling chain.
		 */
		tries = 0;
		for (;;)
		{
			if (!P_ISDELETED(opaque) && opaque->btpo_next == obknum)
			{
				/* Found desired page, return it */
				return buf;
			}
			if (P_RIGHTMOST(opaque) || ++tries > 4)
				break;
			blkno = opaque->btpo_next;
			buf = _bt_relandgetbuf(rel, buf, blkno, BT_READ);
			page = BufferGetPage(buf);
			opaque = BTPageGetOpaque(page);
		}

		/* Return to the original page to see what's up */
		buf = _bt_relandgetbuf(rel, buf, obknum, BT_READ);
		page = BufferGetPage(buf);
		opaque = BTPageGetOpaque(page);
		if (P_ISDELETED(opaque))
		{
			/*
			 * It was deleted.  Move right to first nondeleted page (there
			 * must be one); that is the page that has acquired the deleted
			 * one's keyspace, so stepping left from it will take us where we
			 * want to be.
			 */
			for (;;)
			{
				if (P_RIGHTMOST(opaque))
					elog(ERROR, "fell off the end of index \"%s\"",
						 RelationGetRelationName(rel));
				blkno = opaque->btpo_next;
				buf = _bt_relandgetbuf(rel, buf, blkno, BT_READ);
				page = BufferGetPage(buf);
				opaque = BTPageGetOpaque(page);
				if (!P_ISDELETED(opaque))
					break;
			}

			/*
			 * Now return to top of loop, resetting obknum to point to this
			 * nondeleted page, and try again.
			 */
		}
		else
		{
			/*
			 * It wasn't deleted; the explanation had better be that the page
			 * to the left got split or deleted. Without this check, we'd go
			 * into an infinite loop if there's anything wrong.
			 */
			if (opaque->btpo_prev == lblkno)
				elog(ERROR, "could not find left sibling of block %u in index \"%s\"",
					 obknum, RelationGetRelationName(rel));
			/* Okay to try again with new lblkno value */
		}
	}

	return InvalidBuffer;
}

/*
 * _bt_get_endpoint() -- Find the first or last page on a given tree level
 *
 * If the index is empty, we will return InvalidBuffer; any other failure
 * condition causes ereport().  We will not return a dead page.
 *
 * The returned buffer is pinned and read-locked.
 */
Buffer
_bt_get_endpoint(Relation rel, uint32 level, bool rightmost)
{
	Buffer		buf;
	Page		page;
	BTPageOpaque opaque;
	OffsetNumber offnum;
	BlockNumber blkno;
	IndexTuple	itup;

	/*
	 * If we are looking for a leaf page, okay to descend from fast root;
	 * otherwise better descend from true root.  (There is no point in being
	 * smarter about intermediate levels.)
	 */
	if (level == 0)
		buf = _bt_getroot(rel, NULL, BT_READ);
	else
		buf = _bt_gettrueroot(rel);

	if (!BufferIsValid(buf))
		return InvalidBuffer;

	page = BufferGetPage(buf);
	opaque = BTPageGetOpaque(page);

	for (;;)
	{
		/*
		 * If we landed on a deleted page, step right to find a live page
		 * (there must be one).  Also, if we want the rightmost page, step
		 * right if needed to get to it (this could happen if the page split
		 * since we obtained a pointer to it).
		 */
		while (P_IGNORE(opaque) ||
			   (rightmost && !P_RIGHTMOST(opaque)))
		{
			blkno = opaque->btpo_next;
			if (blkno == P_NONE)
				elog(ERROR, "fell off the end of index \"%s\"",
					 RelationGetRelationName(rel));
			buf = _bt_relandgetbuf(rel, buf, blkno, BT_READ);
			page = BufferGetPage(buf);
			opaque = BTPageGetOpaque(page);
		}

		/* Done? */
		if (opaque->btpo_level == level)
			break;
		if (opaque->btpo_level < level)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg_internal("btree level %u not found in index \"%s\"",
									 level, RelationGetRelationName(rel))));

		/* Descend to leftmost or rightmost child page */
		if (rightmost)
			offnum = PageGetMaxOffsetNumber(page);
		else
			offnum = P_FIRSTDATAKEY(opaque);

		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offnum));
		blkno = BTreeTupleGetDownLink(itup);

		buf = _bt_relandgetbuf(rel, buf, blkno, BT_READ);
		page = BufferGetPage(buf);
		opaque = BTPageGetOpaque(page);
	}

	return buf;
}

/*
 *	_bt_endpoint() -- Find the first or last page in the index, and scan
 * from there to the first key satisfying all the quals.
 *
 * This is used by _bt_first() to set up a scan when we've determined
 * that the scan must start at the beginning or end of the index (for
 * a forward or backward scan respectively).  Exit conditions are the
 * same as for _bt_first().
 */
static bool
_bt_endpoint(IndexScanDesc scan, ScanDirection dir)
{
	Relation	rel = scan->indexRelation;
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	Buffer		buf;
	Page		page;
	BTPageOpaque opaque;
	OffsetNumber start;
	BTScanPosItem *currItem;
	nbts_prep_ctx(rel);

	/*
	 * Scan down to the leftmost or rightmost leaf page.  This is a simplified
	 * version of _bt_search().  We don't maintain a stack since we know we
	 * won't need it.
	 */
	buf = _bt_get_endpoint(rel, 0, ScanDirectionIsBackward(dir));

	if (!BufferIsValid(buf))
	{
		/*
		 * Empty index. Lock the whole relation, as nothing finer to lock
		 * exists.
		 */
		PredicateLockRelation(rel, scan->xs_snapshot);
		BTScanPosInvalidate(so->currPos);
		return false;
	}

	PredicateLockPage(rel, BufferGetBlockNumber(buf), scan->xs_snapshot);
	page = BufferGetPage(buf);
	opaque = BTPageGetOpaque(page);
	Assert(P_ISLEAF(opaque));

	if (ScanDirectionIsForward(dir))
	{
		/* There could be dead pages to the left, so not this: */
		/* Assert(P_LEFTMOST(opaque)); */

		start = P_FIRSTDATAKEY(opaque);
	}
	else if (ScanDirectionIsBackward(dir))
	{
		Assert(P_RIGHTMOST(opaque));

		start = PageGetMaxOffsetNumber(page);
	}
	else
	{
		elog(ERROR, "invalid scan direction: %d", (int) dir);
		start = 0;				/* keep compiler quiet */
	}

	/* remember which buffer we have pinned */
	so->currPos.buf = buf;

	_bt_initialize_more_data(so, dir);

	/*
	 * Now load data from the first page of the scan.
	 */
	if (!_bt_readpage(scan, dir, start, false))
	{
		/*
		 * There's no actually-matching data on this page.  Try to advance to
		 * the next page.  Return false if there's no matching data at all.
		 */
		_bt_unlockbuf(scan->indexRelation, so->currPos.buf);
		if (!_bt_steppage(scan, dir))
			return false;
	}
	else
	{
		/* We have at least one item to return as scan's first item */
		_bt_drop_lock_and_maybe_pin(scan, &so->currPos);
	}

	/* OK, itemIndex says what to return */
	currItem = &so->currPos.items[so->currPos.itemIndex];
	scan->xs_heaptid = currItem->heapTid;
	if (scan->xs_want_itup)
		scan->xs_itup = (IndexTuple) (so->currTuples + currItem->tupleOffset);

	return true;
}

/*
 * _bt_initialize_more_data() -- initialize moreLeft/moreRight appropriately
 * for scan direction
 */
static inline void
_bt_initialize_more_data(BTScanOpaque so, ScanDirection dir)
{
	/* initialize moreLeft/moreRight appropriately for scan direction */
	if (ScanDirectionIsForward(dir))
	{
		so->currPos.moreLeft = false;
		so->currPos.moreRight = true;
	}
	else
	{
		so->currPos.moreLeft = true;
		so->currPos.moreRight = false;
	}
	so->numKilled = 0;			/* just paranoia */
	so->markItemIndex = -1;		/* ditto */
}
