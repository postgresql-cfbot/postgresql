/*-------------------------------------------------------------------------
 *
 * nbtskip.c
 *	  Search code related to skip scan for postgres btrees.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtskip.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/nbtree.h"
#include "access/relscan.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "storage/predicate.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

static inline void _bt_update_scankey_with_tuple(BTScanInsert scankeys,
											Relation indexRel, IndexTuple itup, int numattrs);
static inline bool _bt_scankey_within_page(IndexScanDesc scan, BTScanInsert key, Buffer buf);
static inline int32 _bt_compare_until(Relation rel, BTScanInsert key, IndexTuple itup, int prefix);
static inline void
_bt_determine_next_action(IndexScanDesc scan, BTSkipCompareResult *cmp, OffsetNumber firstOffnum,
						  OffsetNumber lastOffnum, ScanDirection postfixDir, BTSkipState *nextAction);
static inline void
_bt_determine_next_action_after_skip(BTScanOpaque so, BTSkipCompareResult *cmp, ScanDirection prefixDir,
									 ScanDirection postfixDir, int skipped, BTSkipState *nextAction);
static inline void
_bt_determine_next_action_after_skip_extra(BTScanOpaque so, BTSkipCompareResult *cmp, BTSkipState *nextAction);
static inline void _bt_copy_scankey(BTScanInsert to, BTScanInsert from, int numattrs);
static inline IndexTuple _bt_get_tuple_from_offset(BTScanOpaque so, OffsetNumber curTupleOffnum);
static void _bt_skip_update_scankey_after_read(IndexScanDesc scan, IndexTuple curTuple,
											   ScanDirection prefixDir, ScanDirection postfixDir);
static void _bt_skip_update_scankey_for_prefix_skip(IndexScanDesc scan, Relation indexRel,
										int prefix, IndexTuple itup, ScanDirection prefixDir);
static bool _bt_try_in_page_skip(IndexScanDesc scan, ScanDirection prefixDir);

/*
 * returns whether we're at the end of a scan.
 * the scan position can be invalid even though we still
 * should continue the scan. this happens for example when
 * we're scanning with prefixDir!=postfixDir. when looking at the first
 * prefix, we traverse the items within the prefix from max to min.
 * if none of them match, we actually run off the start of the index,
 * meaning none of the tuples within this prefix match. the scan pos becomes
 * invalid, however, we do need to look further to the next prefix.
 * therefore, this function still returns true in this particular case.
 */
static inline bool
_bt_skip_is_valid(BTScanOpaque so, ScanDirection prefixDir, ScanDirection postfixDir)
{
	return BTScanPosIsValid(so->currPos) ||
			(!_bt_skip_is_regular_mode(prefixDir, postfixDir) &&
			 so->skipData->curPos.nextAction != SkipStateStop);
}

/* try finding the next tuple to skip to within the local tuple storage.
 * local tuple storage is filled during _bt_readpage with all matching
 * tuples on that page. if we can find the next prefix here it saves
 * us doing a scan from root.
 * Note that this optimization only works with _bt_regular_mode == true
 * If this is not the case, the local tuple workspace will always only
 * contain tuples of one specific prefix (_bt_readpage will stop at
 * the end of a prefx)
 */
static bool
_bt_try_in_page_skip(IndexScanDesc scan, ScanDirection prefixDir)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	BTScanPosItem *currItem;
	BTSkip skip = so->skipData;
	IndexTuple itup = NULL;
	bool goback;
	int low, high, starthigh, startlow;
	int32		result,
				cmpval;
	BTScanInsert key = &so->skipData->curPos.skipScanKey;

	currItem = &so->currPos.items[so->currPos.itemIndex];
	itup = (IndexTuple) (so->currTuples + currItem->tupleOffset);

	_bt_skip_update_scankey_for_prefix_skip(scan, scan->indexRelation, skip->prefix, itup, prefixDir);

	_bt_set_bsearch_flags(key->scankeys[key->keysz - 1].sk_strategy, prefixDir, &key->nextkey, &goback);

	/* Requesting nextkey semantics while using scantid seems nonsensical */
	Assert(!key->nextkey || key->scantid == NULL);
	/* scantid-set callers must use _bt_binsrch_insert() on leaf pages */

	startlow = low = ScanDirectionIsForward(prefixDir) ? so->currPos.itemIndex + 1 : so->currPos.firstItem;
	starthigh = high = ScanDirectionIsForward(prefixDir) ? so->currPos.lastItem : so->currPos.itemIndex - 1;

	/*
	 * If there are no keys on the page, return the first available slot. Note
	 * this covers two cases: the page is really empty (no keys), or it
	 * contains only a high key.  The latter case is possible after vacuuming.
	 * This can never happen on an internal page, however, since they are
	 * never empty (an internal page must have children).
	 */
	if (unlikely(high < low))
		return false;

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
		int mid = low + ((high - low) / 2);

		/* We have low <= mid < high, so mid points at a real slot */

		currItem = &so->currPos.items[mid];
		itup = (IndexTuple) (so->currTuples + currItem->tupleOffset);
		result = _bt_compare_until(scan->indexRelation, key, itup, skip->prefix);

		if (result >= cmpval)
			low = mid + 1;
		else
			high = mid;
	}

	if (high > starthigh)
		return false;

	if (goback)
	{
		low--;
		if (low < startlow)
			return false;
	}

	so->currPos.itemIndex = low;

	return true;
}

/*
 *  _bt_skip() -- Skip items that have the same prefix as the most recently
 * 				  fetched index tuple.
 *
 * in: pinned, not locked
 * out: pinned, not locked (unless end of scan, then unpinned)
 */
bool
_bt_skip(IndexScanDesc scan, ScanDirection prefixDir, ScanDirection postfixDir)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	BTScanPosItem *currItem;
	IndexTuple itup = NULL;
	OffsetNumber curTupleOffnum = InvalidOffsetNumber;
	BTSkipCompareResult cmp;
	BTSkip skip = so->skipData;
	OffsetNumber first;

	/* in page skip only works when prefixDir == postfixDir */
	if (!_bt_skip_is_regular_mode(prefixDir, postfixDir) || !_bt_try_in_page_skip(scan, prefixDir))
	{
		currItem = &so->currPos.items[so->currPos.itemIndex];
		itup = (IndexTuple) (so->currTuples + currItem->tupleOffset);

		so->skipData->curPos.nextSkipIndex = so->skipData->prefix;
		_bt_skip_once(scan, &itup, &curTupleOffnum, true, prefixDir, postfixDir);
		_bt_skip_until_match(scan, &itup, &curTupleOffnum, prefixDir, postfixDir);
		if (!_bt_skip_is_always_valid(so))
			return false;

		first = curTupleOffnum;
		_bt_readpage(scan, postfixDir, &curTupleOffnum, _bt_skip_is_regular_mode(prefixDir, postfixDir));
		if (DEBUG1 >= log_min_messages || DEBUG1 >= client_min_messages)
		{
			print_itup(BufferGetBlockNumber(so->currPos.buf), _bt_get_tuple_from_offset(so, first), NULL, scan->indexRelation,
						"first item on page compared after skip");
			print_itup(BufferGetBlockNumber(so->currPos.buf), _bt_get_tuple_from_offset(so, curTupleOffnum), NULL, scan->indexRelation,
						"last item on page compared after skip");
		}
		_bt_compare_current_item(scan, _bt_get_tuple_from_offset(so, curTupleOffnum),
								 IndexRelationGetNumberOfAttributes(scan->indexRelation),
								 postfixDir, _bt_skip_is_regular_mode(prefixDir, postfixDir), &cmp);
		_bt_determine_next_action(scan, &cmp, first, curTupleOffnum, postfixDir, &skip->curPos.nextAction);
		skip->curPos.nextDirection = prefixDir;
		skip->curPos.nextSkipIndex = cmp.prefixSkipIndex;
		_bt_skip_update_scankey_after_read(scan, _bt_get_tuple_from_offset(so, curTupleOffnum), prefixDir, postfixDir);

		_bt_drop_lock_and_maybe_pin(scan, &so->currPos);
	}

	/* prepare for the call to _bt_next, because _bt_next increments this to get to the tuple we want to be at */
	if (ScanDirectionIsForward(postfixDir))
		so->currPos.itemIndex--;
	else
		so->currPos.itemIndex++;

	return true;
}

static IndexTuple
_bt_get_tuple_from_offset(BTScanOpaque so, OffsetNumber curTupleOffnum)
{
	Page page = BufferGetPage(so->currPos.buf);
	return (IndexTuple) PageGetItem(page, PageGetItemId(page, curTupleOffnum));
}

static void
_bt_determine_next_action(IndexScanDesc scan, BTSkipCompareResult *cmp, OffsetNumber firstOffnum, OffsetNumber lastOffnum, ScanDirection postfixDir, BTSkipState *nextAction)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;

	if (cmp->fullKeySkip)
		*nextAction = SkipStateStop;
	else if (ScanDirectionIsForward(postfixDir))
	{
		OffsetNumber firstItem = firstOffnum, lastItem = lastOffnum;
		if (cmp->prefixSkip)
		{
			*nextAction = SkipStateSkip;
		}
		else
		{
			IndexTuple toCmp;
			if (so->currPos.lastItem >= so->currPos.firstItem)
				toCmp = _bt_get_tuple_from_offset(so, so->currPos.items[so->currPos.lastItem].indexOffset);
			else
				toCmp = _bt_get_tuple_from_offset(so, firstItem);
			_bt_update_scankey_with_tuple(&so->skipData->currentTupleKey,
										  scan->indexRelation, toCmp, RelationGetNumberOfAttributes(scan->indexRelation));
			if (_bt_has_extra_quals_after_skip(so->skipData, postfixDir, so->skipData->prefix) && !cmp->equal &&
					(cmp->prefixCmpResult != 0 ||
					 _bt_compare_until(scan->indexRelation, &so->skipData->currentTupleKey,
									   _bt_get_tuple_from_offset(so, lastItem), so->skipData->prefix) != 0))
				*nextAction = SkipStateSkipExtra;
			else
				*nextAction = SkipStateNext;
		}
	}
	else
	{
		OffsetNumber firstItem = lastOffnum, lastItem = firstOffnum;
		if (cmp->prefixSkip)
		{
			*nextAction = SkipStateSkip;
		}
		else
		{
			IndexTuple toCmp;
			if (so->currPos.lastItem >= so->currPos.firstItem)
				toCmp = _bt_get_tuple_from_offset(so, so->currPos.items[so->currPos.firstItem].indexOffset);
			else
				toCmp = _bt_get_tuple_from_offset(so, lastItem);
			_bt_update_scankey_with_tuple(&so->skipData->currentTupleKey,
										  scan->indexRelation, toCmp, RelationGetNumberOfAttributes(scan->indexRelation));
			if (_bt_has_extra_quals_after_skip(so->skipData, postfixDir, so->skipData->prefix) && !cmp->equal &&
					(cmp->prefixCmpResult != 0 ||
					 _bt_compare_until(scan->indexRelation, &so->skipData->currentTupleKey,
									   _bt_get_tuple_from_offset(so, firstItem), so->skipData->prefix) != 0))
				*nextAction = SkipStateSkipExtra;
			else
				*nextAction = SkipStateNext;
		}
	}
}

static inline bool
_bt_should_prefix_skip(BTSkipCompareResult *cmp)
{
	return cmp->prefixSkip || cmp->prefixCmpResult != 0;
}

static inline void
_bt_determine_next_action_after_skip(BTScanOpaque so, BTSkipCompareResult *cmp, ScanDirection prefixDir,
									 ScanDirection postfixDir, int skipped, BTSkipState *nextAction)
{
	if (!_bt_skip_is_always_valid(so) || cmp->fullKeySkip)
		*nextAction = SkipStateStop;
	else if (cmp->equal && _bt_skip_is_regular_mode(prefixDir, postfixDir))
		*nextAction = SkipStateNext;
	else if (_bt_should_prefix_skip(cmp) && _bt_skip_is_regular_mode(prefixDir, postfixDir) &&
			 ((ScanDirectionIsForward(prefixDir) && cmp->skCmpResult == -1) ||
			  (ScanDirectionIsBackward(prefixDir) && cmp->skCmpResult == 1)))
		*nextAction = SkipStateSkip;
	else if (!_bt_skip_is_regular_mode(prefixDir, postfixDir) ||
			 _bt_has_extra_quals_after_skip(so->skipData, postfixDir, skipped) ||
			 cmp->prefixCmpResult != 0)
		*nextAction = SkipStateSkipExtra;
	else
		*nextAction = SkipStateNext;
}

static inline void
_bt_determine_next_action_after_skip_extra(BTScanOpaque so, BTSkipCompareResult *cmp, BTSkipState *nextAction)
{
	if (!_bt_skip_is_always_valid(so) || cmp->fullKeySkip)
		*nextAction = SkipStateStop;
	else if (cmp->equal)
		*nextAction = SkipStateNext;
	else if (_bt_should_prefix_skip(cmp))
		*nextAction = SkipStateSkip;
	else
		*nextAction = SkipStateNext;
}

/* just a debug function that prints a scankey. will be removed for final patch */
static inline void
_print_skey(IndexScanDesc scan, BTScanInsert scanKey)
{
	Oid			typOutput;
	bool		varlenatype;
	char	   *val;
	int i;
	Relation rel = scan->indexRelation;

	for (i = 0; i < scanKey->keysz; i++)
	{
		ScanKey cur = &scanKey->scankeys[i];
		if (!IsCatalogRelation(rel))
		{
			if (!(cur->sk_flags & SK_ISNULL))
			{
				if (cur->sk_subtype != InvalidOid)
					getTypeOutputInfo(cur->sk_subtype,
									  &typOutput, &varlenatype);
				else
					getTypeOutputInfo(rel->rd_opcintype[i],
									  &typOutput, &varlenatype);
				val = OidOutputFunctionCall(typOutput, cur->sk_argument);
				if (val)
				{
					elog(DEBUG1, "%s sk attr %d val: %s (%s, %s)",
						 RelationGetRelationName(rel), i, val,
						 (cur->sk_flags & SK_BT_NULLS_FIRST) != 0 ? "NULLS FIRST" : "NULLS LAST",
						 (cur->sk_flags & SK_BT_DESC) != 0 ? "DESC" : "ASC");
					pfree(val);
				}
			}
			else
			{
				elog(DEBUG1, "%s sk attr %d val: NULL (%s, %s)",
					 RelationGetRelationName(rel), i,
					 (cur->sk_flags & SK_BT_NULLS_FIRST) != 0 ? "NULLS FIRST" : "NULLS LAST",
					 (cur->sk_flags & SK_BT_DESC) != 0 ? "DESC" : "ASC");
			}
		}
	}
}

bool
_bt_checkkeys_skip(IndexScanDesc scan, IndexTuple tuple, int tupnatts,
				   ScanDirection dir, bool *continuescan, int *prefixskipindex)
{
	BTScanOpaque 	so = (BTScanOpaque) scan->opaque;
	BTSkip skip = so->skipData;

	bool match = _bt_checkkeys(scan, tuple, tupnatts, dir, continuescan, prefixskipindex);
	int prefixCmpResult = _bt_compare_until(scan->indexRelation, &skip->curPos.skipScanKey, tuple, skip->prefix);
	if (*prefixskipindex == -1 && prefixCmpResult != 0)
	{
		*prefixskipindex = skip->prefix;
		return false;
	}
	else
	{
		bool newcont;
		_bt_checkkeys_threeway(scan, tuple, tupnatts, dir, &newcont, prefixskipindex);
		if (*prefixskipindex == -1 && prefixCmpResult != 0)
		{
			*prefixskipindex = skip->prefix;
			return false;
		}
	}
	return match;
}

/*
 * Compare a scankey with a given tuple but only the first prefix columns
 * This function returns 0 if the first 'prefix' columns are equal
 * -1 if key < itup for the first prefix columns
 * 1 if key > itup for the first prefix columns
 */
int32
_bt_compare_until(Relation rel,
			BTScanInsert key,
			IndexTuple itup,
			int prefix)
{
	TupleDesc	itupdesc = RelationGetDescr(rel);
	ScanKey		scankey;
	int			ncmpkey;

	Assert(key->keysz <= IndexRelationGetNumberOfKeyAttributes(rel));

	ncmpkey = Min(prefix, key->keysz);
	scankey = key->scankeys;
	for (int i = 1; i <= ncmpkey; i++)
	{
		Datum		datum;
		bool		isNull;
		int32		result;

		datum = index_getattr(itup, scankey->sk_attno, itupdesc, &isNull);

		/* see comments about NULLs handling in btbuild */
		if (scankey->sk_flags & SK_ISNULL)	/* key is NULL */
		{
			if (isNull)
				result = 0;		/* NULL "=" NULL */
			else if (scankey->sk_flags & SK_BT_NULLS_FIRST)
				result = -1;	/* NULL "<" NOT_NULL */
			else
				result = 1;		/* NULL ">" NOT_NULL */
		}
		else if (isNull)		/* key is NOT_NULL and item is NULL */
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
			return result;

		scankey++;
	}
	return 0;
}


/*
 * Create initial scankeys for skipping and stores them in the skipData
 * structure
 */
void
_bt_skip_create_scankeys(Relation rel, BTScanOpaque so)
{
	int keysCount;
	BTSkip skip = so->skipData;
	StrategyNumber stratTotal;
	ScanKey		keyPointers[INDEX_MAX_KEYS];
	bool goback;
	/* we need to create both forward and backward keys because the scan direction
	 * may change at any moment in scans with a cursor.
	 * we could technically delay creation of the second until first use as an optimization
	 * but that is not implemented yet.
	 */
	keysCount = _bt_choose_scan_keys(so->keyData, so->numberOfKeys, ForwardScanDirection,
									 keyPointers, skip->fwdNotNullKeys, &stratTotal, skip->prefix);
	_bt_create_insertion_scan_key(rel, ForwardScanDirection, keyPointers, keysCount,
								  &skip->fwdScanKey, &stratTotal, &goback);

	keysCount = _bt_choose_scan_keys(so->keyData, so->numberOfKeys, BackwardScanDirection,
									 keyPointers, skip->bwdNotNullKeys, &stratTotal, skip->prefix);
	_bt_create_insertion_scan_key(rel, BackwardScanDirection, keyPointers, keysCount,
								  &skip->bwdScanKey, &stratTotal, &goback);

	_bt_metaversion(rel, &skip->curPos.skipScanKey.heapkeyspace,
					&skip->curPos.skipScanKey.allequalimage);
	skip->curPos.skipScanKey.anynullkeys = false; /* unused */
	skip->curPos.skipScanKey.nextkey = false;
	skip->curPos.skipScanKey.pivotsearch = false;
	skip->curPos.skipScanKey.scantid = NULL;
	skip->curPos.skipScanKey.keysz = 0;

	/* setup scankey for the current tuple as well. it's not necessarily that
	 * we will use the data from the current tuple already,
	 * but we need the rest of the data structure to be set up correctly
	 * for when we use it to create skip->curPos.skipScanKey keys later
	 */
	_bt_mkscankey(rel, NULL, &skip->currentTupleKey);
}

/*
 * _bt_scankey_within_page() -- check if the provided scankey could be found
 * 								within a page, specified by the buffer.
 */
static inline bool
_bt_scankey_within_page(IndexScanDesc scan, BTScanInsert key,
						Buffer buf)
{
	/* @todo: optimization is still possible here to
	 * only check either the low or the high, depending on
	 * which direction *we came from* AND which direction
	 * *we are planning to scan*
	 */
	OffsetNumber low, high;
	Page page = BufferGetPage(buf);
	BTPageOpaque opaque = (BTPageOpaque) PageGetSpecialPointer(page);
	int			ans_lo, ans_hi;

	low = P_FIRSTDATAKEY(opaque);
	high = PageGetMaxOffsetNumber(page);

	if (unlikely(high < low))
		return false;

	ans_lo = _bt_compare(scan->indexRelation,
					   key, page, low);
	ans_hi = _bt_compare(scan->indexRelation,
					   key, page, high);
	if (key->nextkey)
	{
		/* sk < last && sk >= first */
		return ans_lo >= 0 && ans_hi == -1;
	}
	else
	{
		/* sk <= last && sk > first */
		return ans_lo == 1 && ans_hi <= 0;
	}
}

/* in: pinned and locked, out: pinned and locked (unless end of scan) */
static void
_bt_skip_find(IndexScanDesc scan, IndexTuple *curTuple, OffsetNumber *curTupleOffnum,
			  BTScanInsert scanKey, ScanDirection dir)
{
	BTScanOpaque 	so = (BTScanOpaque) scan->opaque;
	OffsetNumber offnum;
	BTStack stack;
	Buffer buf;
	bool goback;
	Page		page;
	BTPageOpaque opaque;
	OffsetNumber minoff;
	Relation rel = scan->indexRelation;
	bool fromroot = true;

	_bt_set_bsearch_flags(scanKey->scankeys[scanKey->keysz - 1].sk_strategy, dir, &scanKey->nextkey, &goback);

	if ((DEBUG1 >= log_min_messages || DEBUG1 >= client_min_messages) && !IsCatalogRelation(rel))
	{
		if (*curTuple != NULL)
			print_itup(BufferGetBlockNumber(so->currPos.buf), *curTuple, NULL, rel,
						"before btree search");

		elog(DEBUG1, "%s searching tree with %d keys, nextkey=%d, goback=%d",
			 RelationGetRelationName(rel), scanKey->keysz, scanKey->nextkey,
			 goback);

		_print_skey(scan, scanKey);
	}

	if (*curTupleOffnum == InvalidOffsetNumber)
	{
		BTScanPosUnpinIfPinned(so->currPos);
	}
	else
	{
		if (_bt_scankey_within_page(scan, scanKey, so->currPos.buf))
		{
			elog(DEBUG1, "sk found within current page");

			offnum = _bt_binsrch(scan->indexRelation, scanKey, so->currPos.buf);
			fromroot = false;
		}
		else
		{
			LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
			ReleaseBuffer(so->currPos.buf);
			so->currPos.buf = InvalidBuffer;
		}
	}

	/*
	 * We haven't found scan key within the current page, so let's scan from
	 * the root. Use _bt_search and _bt_binsrch to get the buffer and offset
	 * number
	 */
	if (fromroot)
	{
		stack = _bt_search(scan->indexRelation, scanKey,
						   &buf, BT_READ, scan->xs_snapshot);
		_bt_freestack(stack);
		so->currPos.buf = buf;

		offnum = _bt_binsrch(scan->indexRelation, scanKey, buf);

		/* Lock the page for SERIALIZABLE transactions */
		PredicateLockPage(scan->indexRelation, BufferGetBlockNumber(so->currPos.buf),
						  scan->xs_snapshot);
	}

	page = BufferGetPage(so->currPos.buf);
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);

	if (goback)
	{
		offnum = OffsetNumberPrev(offnum);
		minoff = P_FIRSTDATAKEY(opaque);
		if (offnum < minoff)
		{
			LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
			if (!_bt_step_back_page(scan, curTuple, curTupleOffnum))
				return;
			page = BufferGetPage(so->currPos.buf);
			opaque = (BTPageOpaque) PageGetSpecialPointer(page);
			offnum = PageGetMaxOffsetNumber(page);
		}
	}
	else if (offnum > PageGetMaxOffsetNumber(page))
	{
		BlockNumber next = opaque->btpo_next;
		LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
		if (!_bt_step_forward_page(scan, next, curTuple, curTupleOffnum))
			return;
		page = BufferGetPage(so->currPos.buf);
		opaque = (BTPageOpaque) PageGetSpecialPointer(page);
		offnum = P_FIRSTDATAKEY(opaque);
	}

	/* We know in which direction to look */
	_bt_initialize_more_data(so, dir);

	*curTupleOffnum = offnum;
	*curTuple = (IndexTuple) PageGetItem(page, PageGetItemId(page, offnum));
	so->currPos.currPage = BufferGetBlockNumber(so->currPos.buf);

	if (DEBUG1 >= log_min_messages || DEBUG1 >= client_min_messages)
		print_itup(BufferGetBlockNumber(so->currPos.buf), *curTuple, NULL, rel,
					"after btree search");
}

static inline bool
_bt_step_one_page(IndexScanDesc scan, ScanDirection dir, IndexTuple *curTuple,
				  OffsetNumber *curTupleOffnum)
{
	if (ScanDirectionIsForward(dir))
	{
		BTScanOpaque so = (BTScanOpaque) scan->opaque;
		return _bt_step_forward_page(scan, so->currPos.nextPage, curTuple, curTupleOffnum);
	}
	else
	{
		return _bt_step_back_page(scan, curTuple, curTupleOffnum);
	}
}

/* in: possibly pinned, but unlocked, out: pinned and locked */
bool
_bt_step_forward_page(IndexScanDesc scan, BlockNumber next, IndexTuple *curTuple,
					  OffsetNumber *curTupleOffnum)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	Relation rel = scan->indexRelation;
	BlockNumber blkno = next;
	Page page;
	BTPageOpaque opaque;

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
		if (so->skipData)
			memcpy(&so->skipData->markPos, &so->skipData->curPos,
				   sizeof(BTSkipPosData));
		so->markItemIndex = -1;
	}

	/* Remember we left a page with data */
	so->currPos.moreLeft = true;

	/* release the previous buffer, if pinned */
	BTScanPosUnpinIfPinned(so->currPos);

	{
		for (;;)
		{
			/*
			 * if we're at end of scan, give up and mark parallel scan as
			 * done, so that all the workers can finish their scan
			 */
			if (blkno == P_NONE)
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
			TestForOldSnapshot(scan->xs_snapshot, rel, page);
			opaque = (BTPageOpaque) PageGetSpecialPointer(page);
			/* check for deleted page */
			if (!P_IGNORE(opaque))
			{
				PredicateLockPage(rel, blkno, scan->xs_snapshot);
				*curTupleOffnum = P_FIRSTDATAKEY(opaque);
				*curTuple = _bt_get_tuple_from_offset(so, *curTupleOffnum);
				break;
			}

			blkno = opaque->btpo_next;
			_bt_relbuf(rel, so->currPos.buf);
		}
	}

	return true;
}

/* in: possibly pinned, but unlocked, out: pinned and locked */
bool
_bt_step_back_page(IndexScanDesc scan, IndexTuple *curTuple, OffsetNumber *curTupleOffnum)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;

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
		if (so->skipData)
			memcpy(&so->skipData->markPos, &so->skipData->curPos,
				   sizeof(BTSkipPosData));
		so->markPos.itemIndex = so->markItemIndex;
		so->markItemIndex = -1;
	}

	/* Remember we left a page with data */
	so->currPos.moreRight = true;

	/* Not parallel, so just use our own notion of the current page */

	{
		Relation	rel;
		Page		page;
		BTPageOpaque opaque;

		rel = scan->indexRelation;

		if (BTScanPosIsPinned(so->currPos))
			LockBuffer(so->currPos.buf, BT_READ);
		else
			so->currPos.buf = _bt_getbuf(rel, so->currPos.currPage, BT_READ);

		for (;;)
		{
			/* Step to next physical page */
			so->currPos.buf = _bt_walk_left(rel, so->currPos.buf,
											scan->xs_snapshot);

			/* if we're physically at end of index, return failure */
			if (so->currPos.buf == InvalidBuffer)
			{
				BTScanPosInvalidate(so->currPos);
				return false;
			}

			/*
			 * Okay, we managed to move left to a non-deleted page. Done if
			 * it's not half-dead and contains matching tuples. Else loop back
			 * and do it all again.
			 */
			page = BufferGetPage(so->currPos.buf);
			TestForOldSnapshot(scan->xs_snapshot, rel, page);
			opaque = (BTPageOpaque) PageGetSpecialPointer(page);
			if (!P_IGNORE(opaque))
			{
				PredicateLockPage(rel, BufferGetBlockNumber(so->currPos.buf), scan->xs_snapshot);
				*curTupleOffnum = PageGetMaxOffsetNumber(page);
				*curTuple = _bt_get_tuple_from_offset(so, *curTupleOffnum);
				break;
			}
		}
	}

	return true;
}

/* holds lock as long as curTupleOffnum != InvalidOffsetNumber */
bool
_bt_skip_find_next(IndexScanDesc scan, IndexTuple curTuple, OffsetNumber curTupleOffnum,
				   ScanDirection prefixDir, ScanDirection postfixDir)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	BTSkip skip = so->skipData;
	BTSkipCompareResult cmp;

	while (_bt_skip_is_valid(so, prefixDir, postfixDir))
	{
		bool found;
		_bt_skip_until_match(scan, &curTuple, &curTupleOffnum, prefixDir, postfixDir);

		while (_bt_skip_is_always_valid(so))
		{
			OffsetNumber first = curTupleOffnum;
			found = _bt_readpage(scan, postfixDir, &curTupleOffnum,
								 _bt_skip_is_regular_mode(prefixDir, postfixDir));
			if (DEBUG1 >= log_min_messages || DEBUG1 >= client_min_messages)
			{
				print_itup(BufferGetBlockNumber(so->currPos.buf),
						   _bt_get_tuple_from_offset(so, first), NULL, scan->indexRelation,
							"first item on page compared");
				print_itup(BufferGetBlockNumber(so->currPos.buf),
						   _bt_get_tuple_from_offset(so, curTupleOffnum), NULL, scan->indexRelation,
							"last item on page compared");
			}
			_bt_compare_current_item(scan, _bt_get_tuple_from_offset(so, curTupleOffnum),
									 IndexRelationGetNumberOfAttributes(scan->indexRelation),
									 postfixDir, _bt_skip_is_regular_mode(prefixDir, postfixDir), &cmp);
			_bt_determine_next_action(scan, &cmp, first, curTupleOffnum,
									  postfixDir, &skip->curPos.nextAction);
			skip->curPos.nextDirection = prefixDir;
			skip->curPos.nextSkipIndex = cmp.prefixSkipIndex;

			if (found)
			{
				_bt_skip_update_scankey_after_read(scan, _bt_get_tuple_from_offset(so, curTupleOffnum),
												   prefixDir, postfixDir);
				return true;
			}
			else if (skip->curPos.nextAction == SkipStateNext)
			{
				if (curTupleOffnum != InvalidOffsetNumber)
					LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
				if (!_bt_step_one_page(scan, postfixDir, &curTuple, &curTupleOffnum))
					return false;
			}
			else if (skip->curPos.nextAction == SkipStateSkip || skip->curPos.nextAction == SkipStateSkipExtra)
			{
				curTuple = _bt_get_tuple_from_offset(so, curTupleOffnum);
				_bt_skip_update_scankey_after_read(scan, curTuple, prefixDir, postfixDir);
				LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
				curTupleOffnum = InvalidOffsetNumber;
				curTuple = NULL;
				break;
			}
			else if (skip->curPos.nextAction == SkipStateStop)
			{
				LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
				BTScanPosUnpinIfPinned(so->currPos);
				BTScanPosInvalidate(so->currPos);
				return false;
			}
			else
			{
				Assert(false);
			}
		}
	}
	return false;
}

void
_bt_skip_until_match(IndexScanDesc scan, IndexTuple *curTuple, OffsetNumber *curTupleOffnum,
					 ScanDirection prefixDir, ScanDirection postfixDir)
{
	BTScanOpaque 	so = (BTScanOpaque) scan->opaque;
	BTSkip skip = so->skipData;
	while (_bt_skip_is_valid(so, prefixDir, postfixDir) &&
		   (skip->curPos.nextAction == SkipStateSkip || skip->curPos.nextAction == SkipStateSkipExtra))
	{
		_bt_skip_once(scan, curTuple, curTupleOffnum,
					  skip->curPos.nextAction == SkipStateSkip, prefixDir, postfixDir);
	}
}

void
_bt_compare_current_item(IndexScanDesc scan, IndexTuple tuple, int tupnatts, ScanDirection dir,
						 bool isRegularMode, BTSkipCompareResult* cmp)
{
	BTScanOpaque 	so = (BTScanOpaque) scan->opaque;
	BTSkip skip = so->skipData;

	if (_bt_skip_is_always_valid(so))
	{
		bool continuescan = true;

		cmp->equal = _bt_checkkeys(scan, tuple, tupnatts, dir, &continuescan, &cmp->prefixSkipIndex);
		cmp->fullKeySkip = !continuescan;
		/* prefix can be smaller than scankey due to extra quals being added
		 * therefore we need to compare both. @todo this can be optimized into one function call */
		cmp->prefixCmpResult = _bt_compare_until(scan->indexRelation, &skip->curPos.skipScanKey, tuple, skip->prefix);
		cmp->skCmpResult = _bt_compare_until(scan->indexRelation,
											 &skip->curPos.skipScanKey, tuple, skip->curPos.skipScanKey.keysz);
		if (cmp->prefixSkipIndex == -1)
		{
			cmp->prefixSkipIndex = skip->prefix;
			cmp->prefixSkip = ScanDirectionIsForward(dir) ? cmp->prefixCmpResult < 0 : cmp->prefixCmpResult > 0;
		}
		else
		{
			int newskip = -1;
			_bt_checkkeys_threeway(scan, tuple, tupnatts, dir, &continuescan, &newskip);
			if (newskip != -1)
			{
				cmp->prefixSkip = true;
				cmp->prefixSkipIndex = newskip;
			}
			else
			{
				cmp->prefixSkip = ScanDirectionIsForward(dir) ? cmp->prefixCmpResult < 0 : cmp->prefixCmpResult > 0;
				cmp->prefixSkipIndex = skip->prefix;
			}
		}

		if (DEBUG1 >= log_min_messages || DEBUG1 >= client_min_messages)
		{
			print_itup(BufferGetBlockNumber(so->currPos.buf), tuple, NULL, scan->indexRelation,
						"compare item");
			_print_skey(scan, &skip->curPos.skipScanKey);
			elog(DEBUG1, "result: eq: %d fkskip: %d pfxskip: %d prefixcmpres: %d prefixskipidx: %d", cmp->equal, cmp->fullKeySkip,
				 _bt_should_prefix_skip(cmp), cmp->prefixCmpResult, cmp->prefixSkipIndex);
		}
	}
	else
	{
		/* we cannot stop the scan if !isRegularMode - then we do need to skip to the next prefix */
		cmp->fullKeySkip = isRegularMode;
		cmp->equal = false;
		cmp->prefixCmpResult = -2;
		cmp->prefixSkip = true;
		cmp->prefixSkipIndex = skip->prefix;
		cmp->skCmpResult = -2;
	}
}

void
_bt_skip_once(IndexScanDesc scan, IndexTuple *curTuple, OffsetNumber *curTupleOffnum,
			  bool forceSkip, ScanDirection prefixDir, ScanDirection postfixDir)
{
	BTScanOpaque 	so = (BTScanOpaque) scan->opaque;
	BTSkip skip = so->skipData;
	BTSkipCompareResult cmp;
	bool doskip = forceSkip;
	int skipIndex = skip->curPos.nextSkipIndex;
	skip->curPos.nextAction = SkipStateSkipExtra;

	while (doskip)
	{
		int toskip = skipIndex;
		if (*curTuple != NULL)
		{
			if (skip->prefix <= skipIndex || !_bt_skip_is_regular_mode(prefixDir, postfixDir))
			{
				toskip = skip->prefix;
			}

			_bt_skip_update_scankey_for_prefix_skip(scan, scan->indexRelation,
													toskip, *curTuple, prefixDir);
		}

		_bt_skip_find(scan, curTuple, curTupleOffnum, &skip->curPos.skipScanKey, prefixDir);

		if (_bt_skip_is_always_valid(so))
		{
			_bt_skip_update_scankey_for_extra_skip(scan, scan->indexRelation,
												   prefixDir, prefixDir, true, *curTuple);
			_bt_compare_current_item(scan, *curTuple,
									 IndexRelationGetNumberOfAttributes(scan->indexRelation),
									 prefixDir,
									 _bt_skip_is_regular_mode(prefixDir, postfixDir), &cmp);
			skipIndex = cmp.prefixSkipIndex;
			_bt_determine_next_action_after_skip(so, &cmp, prefixDir,
												 postfixDir, toskip, &skip->curPos.nextAction);
		}
		else
		{
			skip->curPos.nextAction = SkipStateStop;
		}
		doskip = skip->curPos.nextAction == SkipStateSkip;
	}
	if (skip->curPos.nextAction != SkipStateStop && skip->curPos.nextAction != SkipStateNext)
		_bt_skip_extra_conditions(scan, curTuple, curTupleOffnum, prefixDir, postfixDir, &cmp);
}

void
_bt_skip_extra_conditions(IndexScanDesc scan, IndexTuple *curTuple, OffsetNumber *curTupleOffnum,
						  ScanDirection prefixDir, ScanDirection postfixDir, BTSkipCompareResult *cmp)
{
	BTScanOpaque 	so = (BTScanOpaque) scan->opaque;
	BTSkip skip = so->skipData;
	bool regularMode = _bt_skip_is_regular_mode(prefixDir, postfixDir);
	if (_bt_skip_is_always_valid(so))
	{
		do
		{
			if (*curTuple != NULL)
				_bt_skip_update_scankey_for_extra_skip(scan, scan->indexRelation,
													   postfixDir, prefixDir, false, *curTuple);
			_bt_skip_find(scan, curTuple, curTupleOffnum, &skip->curPos.skipScanKey, postfixDir);
			_bt_compare_current_item(scan, *curTuple,
									 IndexRelationGetNumberOfAttributes(scan->indexRelation),
									 postfixDir, _bt_skip_is_regular_mode(prefixDir, postfixDir), cmp);
		} while (regularMode && cmp->prefixCmpResult != 0 && !cmp->equal && !cmp->fullKeySkip);
		skip->curPos.nextSkipIndex = cmp->prefixSkipIndex;
	}
	_bt_determine_next_action_after_skip_extra(so, cmp, &skip->curPos.nextAction);
}

static void
_bt_skip_update_scankey_after_read(IndexScanDesc scan, IndexTuple curTuple,
								   ScanDirection prefixDir, ScanDirection postfixDir)
{
	BTScanOpaque 	so = (BTScanOpaque) scan->opaque;
	BTSkip skip = so->skipData;
	if (skip->curPos.nextAction == SkipStateSkip)
	{
		int toskip = skip->curPos.nextSkipIndex;
		if (skip->prefix <= skip->curPos.nextSkipIndex ||
				!_bt_skip_is_regular_mode(prefixDir, postfixDir))
		{
			toskip = skip->prefix;
		}

		if (_bt_skip_is_regular_mode(prefixDir, postfixDir))
			_bt_skip_update_scankey_for_prefix_skip(scan, scan->indexRelation,
													toskip, curTuple, prefixDir);
		else
			_bt_skip_update_scankey_for_prefix_skip(scan, scan->indexRelation,
													toskip, NULL, prefixDir);
	}
	else if (skip->curPos.nextAction == SkipStateSkipExtra)
	{
		_bt_skip_update_scankey_for_extra_skip(scan, scan->indexRelation,
											   postfixDir, prefixDir, false, curTuple);
	}
}

static inline int
_bt_compare_one(ScanKey scankey, Datum datum2, bool isNull2)
{
	int32		result;
	Datum datum1 = scankey->sk_argument;
	bool isNull1 = scankey->sk_flags & SK_ISNULL;
	/* see comments about NULLs handling in btbuild */
	if (isNull1)	/* key is NULL */
	{
		if (isNull2)
			result = 0;		/* NULL "=" NULL */
		else if (scankey->sk_flags & SK_BT_NULLS_FIRST)
			result = -1;	/* NULL "<" NOT_NULL */
		else
			result = 1;		/* NULL ">" NOT_NULL */
	}
	else if (isNull2)		/* key is NOT_NULL and item is NULL */
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
												 datum2,
												 datum1));

		if (!(scankey->sk_flags & SK_BT_DESC))
			INVERT_COMPARE_RESULT(result);
	}
	return result;
}

/*
 * set up new values for the existing scankeys
 * based on the current index tuple
 */
static inline void
_bt_update_scankey_with_tuple(BTScanInsert insertKey, Relation indexRel, IndexTuple itup, int numattrs)
{
	TupleDesc		itupdesc;
	int				i;
	ScanKey			scankeys = insertKey->scankeys;

	insertKey->keysz = numattrs;
	itupdesc = RelationGetDescr(indexRel);
	for (i = 0; i < numattrs; i++)
	{
		Datum datum;
		bool null;
		int flags;

		datum = index_getattr(itup, i + 1, itupdesc, &null);
		flags = (null ? SK_ISNULL : 0) |
				(indexRel->rd_indoption[i] << SK_BT_INDOPTION_SHIFT);
		scankeys[i].sk_flags = flags;
		scankeys[i].sk_argument = datum;
	}
}

/* copy the elements important to a skip from one insertion sk to another */
static inline void
_bt_copy_scankey(BTScanInsert to, BTScanInsert from, int numattrs)
{
	memcpy(to->scankeys, from->scankeys, sizeof(ScanKeyData) * (unsigned long)numattrs);
	to->nextkey = from->nextkey;
	to->keysz = numattrs;
}

/*
 * Updates the existing scankey for skipping to the next prefix
 * alwaysUsePrefix determines how many attrs the scankey will have
 * when true, it will always have skip->prefix number of attributes,
 * otherwise, the value can be less, which will be determined by the comparison
 * result with the current tuple.
 * for example, a SELECT * FROM tbl WHERE b<2, index (a,b,c) and when skipping with prefix size=2
 * if we encounter the tuple (1,3,1) - this does not match the qual b<2. however, we also know that
 * it is not useful to skip to any next qual with prefix=2 (eg. (1,4)), because that will definitely not
 * match either. However, we do want to skip to eg. (2,0). Therefore, we skip over prefix=1 in this case.
 *
 * the provided itup may be null. this happens when we don't want to use the current tuple to update
 * the scankey, but instead want to use the existing curPos.skipScanKey to fill currentTupleKey. this accounts
 * for some edge cases.
 */
static void
_bt_skip_update_scankey_for_prefix_skip(IndexScanDesc scan, Relation indexRel,
										int prefix, IndexTuple itup, ScanDirection prefixDir)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	BTSkip skip = so->skipData;
	/* we use skip->prefix is alwaysUsePrefix is set or if skip->prefix is smaller than whatever the
	 * comparison result provided, such that we never skip more than skip->prefix
	 */
	int numattrs = prefix;

	if (itup != NULL)
	{
		_bt_update_scankey_with_tuple(&skip->currentTupleKey, indexRel, itup, numattrs);
		_bt_copy_scankey(&skip->curPos.skipScanKey, &skip->currentTupleKey, numattrs);
	}
	else
	{
		skip->curPos.skipScanKey.keysz = numattrs;
		_bt_copy_scankey(&skip->currentTupleKey, &skip->curPos.skipScanKey, numattrs);
	}
	/* update strategy for last attribute as we will use this to determine the rest of the
	 * rest of the flags (goback) when doing the actual tree search
	 */
	skip->currentTupleKey.scankeys[numattrs - 1].sk_strategy =
			skip->curPos.skipScanKey.scankeys[numattrs - 1].sk_strategy =
			ScanDirectionIsForward(prefixDir) ? BTGreaterStrategyNumber : BTLessStrategyNumber;
}

/* update the scankey for skipping the 'extra' conditions, opportunities
 * that arise when we have just skipped to a new prefix and can try to skip
 * within the prefix to the right tuple by using extra quals when available
 *
 * @todo as an optimization it should be possible to optimize calls to this function
 * and to _bt_skip_update_scankey_for_prefix_skip to some more specific functions that
 * will need to do less copying of data.
 */
void
_bt_skip_update_scankey_for_extra_skip(IndexScanDesc scan, Relation indexRel, ScanDirection curDir,
									   ScanDirection prefixDir, bool prioritizeEqual, IndexTuple itup)
{
	BTScanOpaque 	so = (BTScanOpaque) scan->opaque;
	BTSkip skip = so->skipData;
	BTScanInsert toCopy;
	int i, left, lastNonTuple = skip->prefix;

	/* first make sure that currentTupleKey is correct at all times */
	_bt_skip_update_scankey_for_prefix_skip(scan, indexRel, skip->prefix, itup, prefixDir);
	/* then do the actual work to setup curPos.skipScanKey - distinguish between work that depends on overallDir
	 * (those attributes between attribute number 1 and 'prefix' inclusive)
	 * and work that depends on curDir
	 * (those attributes between attribute number 'prefix' + 1 and fwdScanKey.keysz inclusive)
	 */
	if (ScanDirectionIsForward(prefixDir))
	{
		/*
		 * if overallDir is Forward, we need to choose between fwdScanKey or
		 * currentTupleKey. we need to choose the most restrictive one -
		 * in most cases this means choosing eg. a>5 over a=2 when scanning forward,
		 * unless prioritizeEqual is set. this is done for certain special cases
		 */
		for (i = 0; i < skip->prefix; i++)
		{
			ScanKey scankey = &skip->fwdScanKey.scankeys[i];
			ScanKey scankeyItem = &skip->currentTupleKey.scankeys[i];
			if (scankey->sk_attno != 0 && (_bt_compare_one(scankey, scankeyItem->sk_argument, scankeyItem->sk_flags & SK_ISNULL) > 0
										   || (prioritizeEqual && scankey->sk_strategy == BTEqualStrategyNumber)))
			{
				memcpy(skip->curPos.skipScanKey.scankeys + i, scankey, sizeof(ScanKeyData));
				lastNonTuple = i;
			}
			else
			{
				if (lastNonTuple < i)
					break;
				memcpy(skip->curPos.skipScanKey.scankeys + i, scankeyItem, sizeof(ScanKeyData));
			}
			/* for now choose equal here - it could actually be improved a bit @todo by choosing the strategy
			 * from the scankeys, but it doesn't matter a lot
			 */
			skip->curPos.skipScanKey.scankeys[i].sk_strategy = BTEqualStrategyNumber;
		}
	}
	else
	{
		/* similar for backward but in opposite direction */
		for (i = 0; i < skip->prefix; i++)
		{
			ScanKey scankey = &skip->bwdScanKey.scankeys[i];
			ScanKey scankeyItem = &skip->currentTupleKey.scankeys[i];
			if (scankey->sk_attno != 0 && (_bt_compare_one(scankey, scankeyItem->sk_argument, scankeyItem->sk_flags & SK_ISNULL) < 0
										   || (prioritizeEqual && scankey->sk_strategy == BTEqualStrategyNumber)))
			{
				memcpy(skip->curPos.skipScanKey.scankeys + i, scankey, sizeof(ScanKeyData));
				lastNonTuple = i;
			}
			else
			{
				if (lastNonTuple < i)
					break;
				memcpy(skip->curPos.skipScanKey.scankeys + i, scankeyItem, sizeof(ScanKeyData));
			}
			skip->curPos.skipScanKey.scankeys[i].sk_strategy = BTEqualStrategyNumber;
		}
	}

	/*
	 * the remaining keys are the quals after the prefix
	 */
	if (ScanDirectionIsForward(curDir))
		toCopy = &skip->fwdScanKey;
	else
		toCopy = &skip->bwdScanKey;

	if (lastNonTuple >= skip->prefix - 1)
	{
		left = toCopy->keysz - skip->prefix;
		if (left > 0)
		{
			memcpy(skip->curPos.skipScanKey.scankeys + skip->prefix, toCopy->scankeys + i, sizeof(ScanKeyData) * (unsigned long)left);
		}
		skip->curPos.skipScanKey.keysz = toCopy->keysz;
	}
	else
	{
		skip->curPos.skipScanKey.keysz = lastNonTuple + 1;
	}
}
