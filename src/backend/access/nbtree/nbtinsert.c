/*-------------------------------------------------------------------------
 *
 * nbtinsert.c
 *	  Item insertion in Lehman and Yao btrees for Postgres.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtinsert.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/nbtxlog.h"
#include "access/transam.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/smgr.h"
#include "utils/tqual.h"

/* Minimum tree height for application of fastpath optimization */
#define BTREE_FASTPATH_MIN_LEVEL	2
/* _bt_findsplitloc limits on suffix truncation split interval */
#define MAX_LEAF_SPLIT_POINTS		9
#define MAX_INTERNAL_SPLIT_POINTS	3

typedef enum
{
	/* strategy to use for a call to FindSplitData */
	SPLIT_DEFAULT,				/* give some weight to truncation */
	SPLIT_MANY_DUPLICATES,		/* find minimally distinguishing point */
	SPLIT_SINGLE_VALUE			/* leave left page almost empty */
} SplitMode;

typedef struct
{
	/* FindSplitData candidate split */
	int			delta;			/* size delta */
	bool		newitemonleft;	/* new item on left or right of split */
	OffsetNumber firstright;	/* split point */
} SplitPoint;

typedef struct
{
	/* context data for _bt_checksplitloc */
	SplitMode	mode;			/* strategy for deciding split point */
	Size		newitemsz;		/* size of new item to be inserted */
	double		fillfactor;		/* needed for weighted splits */
	int			goodenough;
	bool		is_leaf;		/* T if splitting a leaf page */
	bool		is_weighted;	/* T if weighted (e.g. rightmost) split */
	OffsetNumber newitemoff;	/* where the new item is to be inserted */
	bool		hikeyheaptid;	/* T if high key will likely get heap TID */
	int			leftspace;		/* space available for items on left page */
	int			rightspace;		/* space available for items on right page */
	int			olddataitemstotal;	/* space taken by old items */

	int			maxsplits;		/* Maximum number of splits */
	int			nsplits;		/* Current number of splits */
	SplitPoint *splits;			/* Sorted by delta */
} FindSplitData;


static Buffer _bt_newroot(Relation rel, Buffer lbuf, Buffer rbuf);

static TransactionId _bt_check_unique(Relation rel, BTScanInsert itup_scankey,
				 IndexTuple itup, Relation heapRel, Buffer buf,
				 OffsetNumber offset, IndexUniqueCheck checkUnique,
				 bool *is_unique, uint32 *speculativeToken);
static OffsetNumber _bt_findinsertloc(Relation rel,
				  BTScanInsert itup_scankey,
				  Buffer *bufptr,
				  bool uniqueindex,
				  IndexTuple newtup,
				  BTStack stack,
				  Relation heapRel);
static bool _bt_useduplicatepage(Relation rel, Relation heapRel, Buffer buf,
					 bool *restorebinsrch, Size itemsz);
static void _bt_insertonpg(Relation rel, Buffer buf, Buffer cbuf,
			   BTStack stack,
			   IndexTuple itup,
			   OffsetNumber newitemoff,
			   bool split_only_page);
static Buffer _bt_split(Relation rel, Buffer buf, Buffer cbuf,
		  OffsetNumber firstright, OffsetNumber newitemoff, Size newitemsz,
		  IndexTuple newitem, bool newitemonleft, bool truncate);
static void _bt_insert_parent(Relation rel, Buffer buf, Buffer rbuf,
				  BTStack stack, bool is_root, bool is_only);
static OffsetNumber _bt_findsplitloc(Relation rel, Page page,
				 SplitMode mode, OffsetNumber newitemoff,
				 Size newitemsz, IndexTuple newitem, bool *newitemonleft);
static int _bt_checksplitloc(FindSplitData *state,
				  OffsetNumber firstoldonright, bool newitemonleft,
				  int dataitemstoleft, Size firstoldonrightsz);
static bool _bt_dosplitatnewitem(Relation rel, Page page,
					 OffsetNumber newitemoff, IndexTuple newitem);
static OffsetNumber _bt_bestsplitloc(Relation rel, Page page,
				 FindSplitData *state,
				 int perfectpenalty,
				 OffsetNumber newitemoff,
				 IndexTuple newitem, bool *newitemonleft);
static int  _bt_perfect_penalty(Relation rel, Page page, FindSplitData *state,
				 OffsetNumber newitemoff, IndexTuple newitem,
				 SplitMode *secondmode);
static int _bt_split_penalty(Relation rel, Page page, OffsetNumber newitemoff,
				  IndexTuple newitem, SplitPoint *split, bool is_leaf);
static bool _bt_adjacenthtid(ItemPointer lowhtid, ItemPointer highhtid);
static bool _bt_pgaddtup(Page page, Size itemsize, IndexTuple itup,
			 OffsetNumber itup_off);
static bool _bt_isequal(TupleDesc itupdesc, BTScanInsert itup_scankey,
			Page page, OffsetNumber offnum);
static void _bt_vacuum_one_page(Relation rel, Buffer buffer, Relation heapRel);

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
 *		The result value is only significant for UNIQUE_CHECK_PARTIAL:
 *		it must be true if the entry is known unique, else false.
 *		(In the current implementation we'll also return true after a
 *		successful UNIQUE_CHECK_YES or UNIQUE_CHECK_EXISTING call, but
 *		that's just a coding artifact.)
 */
bool
_bt_doinsert(Relation rel, IndexTuple itup,
			 IndexUniqueCheck checkUnique, Relation heapRel)
{
	bool		is_unique = false;
	BTScanInsert itup_scankey;
	BTStack		stack = NULL;
	Buffer		buf;
	Page		page;
	BTPageOpaque lpageop;
	bool		fastpath;

	Assert(IndexRelationGetNumberOfKeyAttributes(rel) != 0);

	/* we need an insertion scan key to do our search, so build one */
	itup_scankey = _bt_mkscankey(rel, itup, false);
top:
	/* Cannot use real heap TID in unique case -- it'll be restored later */
	if (itup_scankey->heapkeyspace && checkUnique != UNIQUE_CHECK_NO)
		itup_scankey->scantid = _bt_lowest_scantid();

	/*
	 * It's very common to have an index on an auto-incremented or
	 * monotonically increasing value. In such cases, every insertion happens
	 * towards the end of the index. We try to optimize that case by caching
	 * the right-most leaf of the index. If our cached block is still the
	 * rightmost leaf, has enough free space to accommodate a new entry and
	 * the insertion key is strictly greater than the first key in this page,
	 * then we can safely conclude that the new key will be inserted in the
	 * cached block. So we simply search within the cached block and insert
	 * the key at the appropriate location. We call it a fastpath.
	 *
	 * Testing has revealed, though, that the fastpath can result in increased
	 * contention on the exclusive-lock on the rightmost leaf page. So we
	 * conditionally check if the lock is available. If it's not available
	 * then we simply abandon the fastpath and take the regular path. This
	 * makes sense because unavailability of the lock also signals that some
	 * other backend might be concurrently inserting into the page, thus
	 * reducing our chances to finding an insertion place in this page.
	 */
	fastpath = false;
	if (RelationGetTargetBlock(rel) != InvalidBlockNumber)
	{
		Size		itemsz;

		/*
		 * Conditionally acquire exclusive lock on the buffer before doing any
		 * checks. If we don't get the lock, we simply follow slowpath. If we
		 * do get the lock, this ensures that the index state cannot change,
		 * as far as the rightmost part of the index is concerned.
		 */
		buf = ReadBuffer(rel, RelationGetTargetBlock(rel));

		if (ConditionalLockBuffer(buf))
		{
			_bt_checkpage(rel, buf);

			page = BufferGetPage(buf);

			lpageop = (BTPageOpaque) PageGetSpecialPointer(page);
			itemsz = IndexTupleSize(itup);
			itemsz = MAXALIGN(itemsz);	/* be safe, PageAddItem will do this
										 * but we need to be consistent */

			/*
			 * Check if the page is still the rightmost leaf page, has enough
			 * free space to accommodate the new tuple, and the insertion scan
			 * key is strictly greater than the first key on the page.  The
			 * itup_scankey scantid value may prevent the optimization from
			 * being applied despite being safe when it was temporarily set to
			 * a sentinel low value, though only when the page is full of
			 * duplicates.
			 */
			if (P_ISLEAF(lpageop) && P_RIGHTMOST(lpageop) &&
				!P_IGNORE(lpageop) &&
				(PageGetFreeSpace(page) > itemsz) &&
				PageGetMaxOffsetNumber(page) >= P_FIRSTDATAKEY(lpageop) &&
				_bt_compare(rel, itup_scankey, page, P_FIRSTDATAKEY(lpageop)) > 0)
			{
				/*
				 * The right-most block should never have an incomplete split.
				 * But be paranoid and check for it anyway.
				 */
				Assert(!P_INCOMPLETE_SPLIT(lpageop));
				fastpath = true;
			}
			else
			{
				_bt_relbuf(rel, buf);

				/*
				 * Something did not work out. Just forget about the cached
				 * block and follow the normal path. It might be set again if
				 * the conditions are favourable.
				 */
				RelationSetTargetBlock(rel, InvalidBlockNumber);
			}
		}
		else
		{
			ReleaseBuffer(buf);

			/*
			 * If someone's holding a lock, it's likely to change anyway, so
			 * don't try again until we get an updated rightmost leaf.
			 */
			RelationSetTargetBlock(rel, InvalidBlockNumber);
		}
	}

	if (!fastpath)
	{
		/*
		 * Find the first page containing this key.  Buffer returned by
		 * _bt_search() is locked in exclusive mode.
		 */
		stack = _bt_search(rel, itup_scankey, &buf, BT_WRITE, NULL);
	}

	/*
	 * If we're not allowing duplicates, make sure the key isn't already in
	 * the index.
	 *
	 * NOTE: obviously, _bt_check_unique can only detect keys that are already
	 * in the index; so it cannot defend against concurrent insertions of the
	 * same key.  We protect against that by means of holding a write lock on
	 * the first page the value could be on, regardless of the value of its
	 * implicit heap TID tie-breaker attribute.  Any other would-be inserter
	 * of the same key must acquire a write lock on the same page, so only one
	 * would-be inserter can be making the check at one time.  Furthermore,
	 * once we are past the check we hold write locks continuously until we
	 * have performed our insertion, so no later inserter can fail to see our
	 * insertion.  (This requires some care in _bt_findinsertloc.)
	 *
	 * If we must wait for another xact, we release the lock while waiting,
	 * and then must start over completely.
	 *
	 * For a partial uniqueness check, we don't wait for the other xact. Just
	 * let the tuple in and return false for possibly non-unique, or true for
	 * definitely unique.
	 */
	if (checkUnique != UNIQUE_CHECK_NO)
	{
		TransactionId xwait;
		uint32		speculativeToken;
		OffsetNumber offset;

		page = BufferGetPage(buf);
		lpageop = (BTPageOpaque) PageGetSpecialPointer(page);

		/*
		 * Arrange for the later _bt_findinsertloc call to _bt_binsrch to
		 * avoid repeating the work done during this initial _bt_binsrch call.
		 * Clear the _bt_lowest_scantid-supplied scantid value first, though,
		 * so that the itup_scankey-cached low and high bounds will enclose a
		 * range of offsets in the event of multiple duplicates. (Our
		 * _bt_binsrch call cannot be allowed to incorrectly enclose a single
		 * offset: the offset of the first duplicate among many on the page.)
		 */
		itup_scankey->scantid = NULL;
		itup_scankey->savebinsrch = true;
		offset = _bt_binsrch(rel, itup_scankey, buf);
		xwait = _bt_check_unique(rel, itup_scankey, itup, heapRel, buf, offset,
								 checkUnique, &is_unique, &speculativeToken);

		if (TransactionIdIsValid(xwait))
		{
			/* Have to wait for the other guy ... */
			_bt_relbuf(rel, buf);

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
			goto top;
		}

		/* Uniqueness is established -- restore heap tid as scantid */
		if (itup_scankey->heapkeyspace)
			itup_scankey->scantid = &itup->t_tid;
	}

	if (checkUnique != UNIQUE_CHECK_EXISTING)
	{
		OffsetNumber insertoff;

		/*
		 * The only conflict predicate locking cares about for indexes is when
		 * an index tuple insert conflicts with an existing lock.  Since the
		 * actual location of the insert is hard to predict because of the
		 * random search used to prevent O(N^2) performance when there are
		 * many duplicate entries, we can just use the "first valid" page.
		 * This reasoning also applies to INCLUDE indexes, whose extra
		 * attributes are not considered part of the key space.
		 */
		CheckForSerializableConflictIn(rel, NULL, buf);
		/* do the insertion, possibly on a page to the right in unique case */
		insertoff = _bt_findinsertloc(rel, itup_scankey, &buf,
									  checkUnique != UNIQUE_CHECK_NO, itup,
									  stack, heapRel);
		_bt_insertonpg(rel, buf, InvalidBuffer, stack, itup, insertoff, false);
	}
	else
	{
		/* just release the buffer */
		_bt_relbuf(rel, buf);
	}

	/* be tidy */
	if (stack)
		_bt_freestack(stack);
	pfree(itup_scankey);

	return is_unique;
}

/*
 *	_bt_check_unique() -- Check for violation of unique index constraint
 *
 * offset points to the first possible item that could conflict. It can
 * also point to end-of-page, which means that the first tuple to check
 * is the first tuple on the next page.
 *
 * Returns InvalidTransactionId if there is no conflict, else an xact ID
 * we must wait for to see if it commits a conflicting tuple.   If an actual
 * conflict is detected, no return --- just ereport().  If an xact ID is
 * returned, and the conflicting tuple still has a speculative insertion in
 * progress, *speculativeToken is set to non-zero, and the caller can wait for
 * the verdict on the insertion using SpeculativeInsertionWait().
 *
 * However, if checkUnique == UNIQUE_CHECK_PARTIAL, we always return
 * InvalidTransactionId because we don't want to wait.  In this case we
 * set *is_unique to false if there is a potential conflict, and the
 * core code must redo the uniqueness check later.
 */
static TransactionId
_bt_check_unique(Relation rel, BTScanInsert itup_scankey,
				 IndexTuple itup, Relation heapRel, Buffer buf,
				 OffsetNumber offset, IndexUniqueCheck checkUnique,
				 bool *is_unique, uint32 *speculativeToken)
{
	TupleDesc	itupdesc = RelationGetDescr(rel);
	SnapshotData SnapshotDirty;
	OffsetNumber maxoff;
	Page		page;
	BTPageOpaque opaque;
	Buffer		nbuf = InvalidBuffer;
	bool		found = false;

	/* Assume unique until we find a duplicate */
	*is_unique = true;

	/* Fast path for case where there are clearly no duplicates */
	if (itup_scankey->low >= itup_scankey->high)
		return InvalidTransactionId;

	InitDirtySnapshot(SnapshotDirty);

	page = BufferGetPage(buf);
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);
	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * Scan over all equal tuples, looking for live conflicts.
	 */
	for (;;)
	{
		ItemId		curitemid;
		IndexTuple	curitup;
		BlockNumber nblkno;

		/*
		 * make sure the offset points to an actual item before trying to
		 * examine it...
		 */
		if (offset <= maxoff)
		{
			curitemid = PageGetItemId(page, offset);

			/*
			 * We can skip items that are marked killed.
			 *
			 * Formerly, we applied _bt_isequal() before checking the kill
			 * flag, so as to fall out of the item loop as soon as possible.
			 * However, in the presence of heavy update activity an index may
			 * contain many killed items with the same key; running
			 * _bt_isequal() on each killed item gets expensive. Furthermore
			 * it is likely that the non-killed version of each key appears
			 * first, so that we didn't actually get to exit any sooner
			 * anyway. So now we just advance over killed items as quickly as
			 * we can. We only apply _bt_isequal() when we get to a non-killed
			 * item or the end of the page.
			 */
			if (!ItemIdIsDead(curitemid))
			{
				ItemPointerData htid;
				bool		all_dead;

				/*
				 * _bt_compare returns 0 for (1,NULL) and (1,NULL) - this's
				 * how we handling NULLs - and so we must not use _bt_compare
				 * in real comparison, but only for ordering/finding items on
				 * pages. - vadim 03/24/97
				 */
				if (!_bt_isequal(itupdesc, itup_scankey, page, offset))
					break;		/* we're past all the equal tuples */

				/* okay, we gotta fetch the heap tuple ... */
				curitup = (IndexTuple) PageGetItem(page, curitemid);
				htid = curitup->t_tid;

				/*
				 * If we are doing a recheck, we expect to find the tuple we
				 * are rechecking.  It's not a duplicate, but we have to keep
				 * scanning.
				 */
				if (checkUnique == UNIQUE_CHECK_EXISTING &&
					ItemPointerCompare(&htid, &itup->t_tid) == 0)
				{
					found = true;
				}

				/*
				 * We check the whole HOT-chain to see if there is any tuple
				 * that satisfies SnapshotDirty.  This is necessary because we
				 * have just a single index entry for the entire chain.
				 */
				else if (heap_hot_search(&htid, heapRel, &SnapshotDirty,
										 &all_dead))
				{
					TransactionId xwait;

					/*
					 * It is a duplicate. If we are only doing a partial
					 * check, then don't bother checking if the tuple is being
					 * updated in another transaction. Just return the fact
					 * that it is a potential conflict and leave the full
					 * check till later.
					 */
					if (checkUnique == UNIQUE_CHECK_PARTIAL)
					{
						if (nbuf != InvalidBuffer)
							_bt_relbuf(rel, nbuf);
						*is_unique = false;
						return InvalidTransactionId;
					}

					/*
					 * If this tuple is being updated by other transaction
					 * then we have to wait for its commit/abort.
					 */
					xwait = (TransactionIdIsValid(SnapshotDirty.xmin)) ?
						SnapshotDirty.xmin : SnapshotDirty.xmax;

					if (TransactionIdIsValid(xwait))
					{
						if (nbuf != InvalidBuffer)
							_bt_relbuf(rel, nbuf);
						/* Tell _bt_doinsert to wait... */
						*speculativeToken = SnapshotDirty.speculativeToken;
						return xwait;
					}

					/*
					 * Otherwise we have a definite conflict.  But before
					 * complaining, look to see if the tuple we want to insert
					 * is itself now committed dead --- if so, don't complain.
					 * This is a waste of time in normal scenarios but we must
					 * do it to support CREATE INDEX CONCURRENTLY.
					 *
					 * We must follow HOT-chains here because during
					 * concurrent index build, we insert the root TID though
					 * the actual tuple may be somewhere in the HOT-chain.
					 * While following the chain we might not stop at the
					 * exact tuple which triggered the insert, but that's OK
					 * because if we find a live tuple anywhere in this chain,
					 * we have a unique key conflict.  The other live tuple is
					 * not part of this chain because it had a different index
					 * entry.
					 */
					htid = itup->t_tid;
					if (heap_hot_search(&htid, heapRel, SnapshotSelf, NULL))
					{
						/* Normal case --- it's still live */
					}
					else
					{
						/*
						 * It's been deleted, so no error, and no need to
						 * continue searching
						 */
						break;
					}

					/*
					 * Check for a conflict-in as we would if we were going to
					 * write to this page.  We aren't actually going to write,
					 * but we want a chance to report SSI conflicts that would
					 * otherwise be masked by this unique constraint
					 * violation.
					 */
					CheckForSerializableConflictIn(rel, NULL, buf);

					/*
					 * This is a definite conflict.  Break the tuple down into
					 * datums and report the error.  But first, make sure we
					 * release the buffer locks we're holding ---
					 * BuildIndexValueDescription could make catalog accesses,
					 * which in the worst case might touch this same index and
					 * cause deadlocks.
					 */
					if (nbuf != InvalidBuffer)
						_bt_relbuf(rel, nbuf);
					_bt_relbuf(rel, buf);

					{
						Datum		values[INDEX_MAX_KEYS];
						bool		isnull[INDEX_MAX_KEYS];
						char	   *key_desc;

						index_deform_tuple(itup, RelationGetDescr(rel),
										   values, isnull);

						key_desc = BuildIndexValueDescription(rel, values,
															  isnull);

						ereport(ERROR,
								(errcode(ERRCODE_UNIQUE_VIOLATION),
								 errmsg("duplicate key value violates unique constraint \"%s\"",
										RelationGetRelationName(rel)),
								 key_desc ? errdetail("Key %s already exists.",
													  key_desc) : 0,
								 errtableconstraint(heapRel,
													RelationGetRelationName(rel))));
					}
				}
				else if (all_dead)
				{
					/*
					 * The conflicting tuple (or whole HOT chain) is dead to
					 * everyone, so we may as well mark the index entry
					 * killed.
					 */
					ItemIdMarkDead(curitemid);
					opaque->btpo_flags |= BTP_HAS_GARBAGE;

					/*
					 * Mark buffer with a dirty hint, since state is not
					 * crucial. Be sure to mark the proper buffer dirty.
					 */
					if (nbuf != InvalidBuffer)
						MarkBufferDirtyHint(nbuf, true);
					else
						MarkBufferDirtyHint(buf, true);
				}
			}
		}

		/*
		 * Advance to next tuple to continue checking.
		 */
		if (offset < maxoff)
			offset = OffsetNumberNext(offset);
		else
		{
			/*
			 * If scankey <= hikey (leaving out the heap TID attribute), we
			 * gotta check the next page too.
			 *
			 * We cannot get away with giving up without going to the next
			 * page when true key values are all == hikey, because heap TID is
			 * ignored when considering duplicates (caller is sure to not
			 * provide a scantid in scankey).  We could get away with this in
			 * a hypothetical world where unique indexes certainly never
			 * contain physical duplicates, since heap TID would never be
			 * treated as part of the keyspace --- not here, and not at any
			 * other point.
			 */
			Assert(itup_scankey->scantid == NULL);
			if (P_RIGHTMOST(opaque))
				break;
			if (_bt_compare(rel, itup_scankey, page, P_HIKEY) > 0)
				break;
			/* Advance to next non-dead page --- there must be one */
			for (;;)
			{
				nblkno = opaque->btpo_next;
				nbuf = _bt_relandgetbuf(rel, nbuf, nblkno, BT_READ);
				page = BufferGetPage(nbuf);
				opaque = (BTPageOpaque) PageGetSpecialPointer(page);
				if (!P_IGNORE(opaque))
					break;
				if (P_RIGHTMOST(opaque))
					elog(ERROR, "fell off the end of index \"%s\"",
						 RelationGetRelationName(rel));
			}
			maxoff = PageGetMaxOffsetNumber(page);
			offset = P_FIRSTDATAKEY(opaque);
		}
	}

	/*
	 * If we are doing a recheck then we should have found the tuple we are
	 * checking.  Otherwise there's something very wrong --- probably, the
	 * index is on a non-immutable expression.
	 */
	if (checkUnique == UNIQUE_CHECK_EXISTING && !found)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to re-find tuple within index \"%s\"",
						RelationGetRelationName(rel)),
				 errhint("This may be because of a non-immutable index expression."),
				 errtableconstraint(heapRel,
									RelationGetRelationName(rel))));

	if (nbuf != InvalidBuffer)
		_bt_relbuf(rel, nbuf);

	return InvalidTransactionId;
}


/*
 *	_bt_findinsertloc() -- Finds an insert location for a new tuple
 *
 *		On entry, *bufptr contains the page that the new tuple unambiguously
 *		belongs on.  This may not be quite right for callers that just called
 *		_bt_check_unique(), though, since they won't have initially searched
 *		using a scantid.  They'll have to insert into a page somewhere to the
 *		right in rare cases where there are many physical duplicates in a
 *		unique index, and their scantid directs us to some page full of
 *		duplicates to the right, where the new tuple must go.  (Actually,
 *		since !heapkeyspace pg_upgraded'd non-unique indexes never get a
 *		scantid, they too may require that we move right.  We treat them
 *		somewhat like unique indexes.)
 *
 *		_bt_check_unique() callers arrange for their insertion scan key to
 *		save the progress of the last binary search performed.  No additional
 *		binary search comparisons occur in the common case where there was no
 *		existing duplicate tuple, though we may occasionally still not be able
 *		to reuse their work for our own reasons.  Even when there are garbage
 *		duplicates, very few binary search comparisons will be performed
 *		without being strictly necessary.
 *
 *		The caller should hold an exclusive lock on *bufptr in all cases.  On
 *		exit,  bufptr points to the chosen insert location in all cases.  If
 *		we have to move right, the lock and pin on the original page will be
 *		released, and the new page returned to the caller is exclusively
 *		locked instead.  In any case, we return the offset that caller should
 *		use to insert into the buffer pointed to by bufptr on return.
 *
 *		This is also where opportunistic microvacuuming of LP_DEAD tuples
 *		occurs.  It has to happen here, since it may invalidate a
 *		_bt_check_unique() caller's cached binary search work.
 */
static OffsetNumber
_bt_findinsertloc(Relation rel,
				  BTScanInsert itup_scankey,
				  Buffer *bufptr,
				  bool uniqueindex,
				  IndexTuple newtup,
				  BTStack stack,
				  Relation heapRel)
{
	Buffer		buf = *bufptr;
	Page		page = BufferGetPage(buf);
	bool		restorebinsrch = uniqueindex;
	Size		itemsz;
	BTPageOpaque lpageop;
	OffsetNumber newitemoff;

	lpageop = (BTPageOpaque) PageGetSpecialPointer(page);

	itemsz = IndexTupleSize(newtup);
	itemsz = MAXALIGN(itemsz);	/* be safe, PageAddItem will do this but we
								 * need to be consistent */

	/* Check 1/3 of a page restriction */
	if (itemsz > BTMaxItemSize(page))
		_bt_check_third_page(rel, heapRel, page, newtup);

	/*
	 * We may have to walk right through leaf pages to find the one leaf page
	 * that we must insert on to, though only when inserting into unique
	 * indexes.  This is necessary because a scantid is not used by the
	 * insertion scan key initially in the case of unique indexes -- a scantid
	 * is only set after the absence of duplicates (whose heap tuples are not
	 * dead or recently dead) has been established by _bt_check_unique().
	 * Non-unique index insertions will break out of the loop immediately.
	 *
	 * (Actually, non-unique indexes may still need to grovel through leaf
	 * pages full of duplicates with a pg_upgrade'd !heapkeyspace index.)
	 */
	Assert(P_ISLEAF(lpageop));
	Assert(!itup_scankey->heapkeyspace || itup_scankey->scantid != NULL);
	Assert(itup_scankey->heapkeyspace || itup_scankey->scantid == NULL);
	for (;;)
	{
		Buffer		rbuf;
		BlockNumber rblkno;
		int			cmpval;

		/*
		 * No need to check high key when inserting into a non-unique index --
		 * _bt_search() already checked this when it checked if a move to the
		 * right was required.  Insertion scankey's scantid would have been
		 * filled out at the time.
		 */
		if (itup_scankey->heapkeyspace && !uniqueindex)
		{
			Assert(P_RIGHTMOST(lpageop) ||
				   _bt_compare(rel, itup_scankey, page, P_HIKEY) <= 0);
			break;
		}

		if (P_RIGHTMOST(lpageop))
			break;
		cmpval = _bt_compare(rel, itup_scankey, page, P_HIKEY);
		if (itup_scankey->heapkeyspace)
		{
			if (cmpval <= 0)
				break;
		}
		else
		{
			/*
			 * pg_upgrade'd !heapkeyspace index.
			 *
			 * May have to handle legacy case where there is a choice of which
			 * page to place new tuple on, and we must balance space
			 * utilization as best we can.
			 */
			if (cmpval != 0 || _bt_useduplicatepage(rel, heapRel, buf,
													&restorebinsrch, itemsz))
				break;
		}

		/*
		 * step right to next non-dead page
		 *
		 * must write-lock that page before releasing write lock on current
		 * page; else someone else's _bt_check_unique scan could fail to see
		 * our insertion.  write locks on intermediate dead pages won't do
		 * because we don't know when they will get de-linked from the tree.
		 * (this is more aggressive than it needs to be for non-unique
		 * !heapkeyspace indexes.)
		 */
		rbuf = InvalidBuffer;

		rblkno = lpageop->btpo_next;
		for (;;)
		{
			rbuf = _bt_relandgetbuf(rel, rbuf, rblkno, BT_WRITE);
			page = BufferGetPage(rbuf);
			lpageop = (BTPageOpaque) PageGetSpecialPointer(page);

			/*
			 * If this page was incompletely split, finish the split now. We
			 * do this while holding a lock on the left sibling, which is not
			 * good because finishing the split could be a fairly lengthy
			 * operation.  But this should happen very seldom.
			 */
			if (P_INCOMPLETE_SPLIT(lpageop))
			{
				_bt_finish_split(rel, rbuf, stack);
				rbuf = InvalidBuffer;
				continue;
			}

			if (!P_IGNORE(lpageop))
				break;
			if (P_RIGHTMOST(lpageop))
				elog(ERROR, "fell off the end of index \"%s\"",
					 RelationGetRelationName(rel));

			rblkno = lpageop->btpo_next;
		}
		_bt_relbuf(rel, buf);
		buf = rbuf;
		restorebinsrch = false;
	}

	/*
	 * Perform micro-vacuuming of the page we're about to insert tuple on if
	 * it looks like it has LP_DEAD items.  Only micro-vacuum when it might
	 * forestall a page split, though.
	 */
	if (P_HAS_GARBAGE(lpageop) && PageGetFreeSpace(page) < itemsz)
	{
		_bt_vacuum_one_page(rel, buf, heapRel);

		restorebinsrch = false;
	}

	/* _bt_check_unique() callers often avoid binary search effort */
	itup_scankey->restorebinsrch = restorebinsrch;
	newitemoff = _bt_binsrch(rel, itup_scankey, buf);
	Assert(!itup_scankey->restorebinsrch);
	/* XXX: may use too many cycles to be a simple assertion */
	Assert(!restorebinsrch ||
		   newitemoff == _bt_binsrch(rel, itup_scankey, buf));

	*bufptr = buf;
	return newitemoff;
}

/*
 *	_bt_useduplicatepage() -- Settle for this page of duplicates?
 *
 *		Prior to PostgreSQL 12/Btree version 4, heap TID was never treated
 *		as a part of the keyspace.  If there were many tuples of the same
 *		value spanning more than one page, a new tuple of that same value
 *		could legally be placed on any one of the pages.  This function
 *		handles the question of whether or not an insertion of a duplicate
 *		into a pg_upgrade'd !heapkeyspace index should insert on the page
 *		contained in buf when that's a legal choice.
 *
 *		Returns true if caller should proceed with insert on buf's page.
 *		Otherwise, caller should move to page to the right.  Caller calls
 *		here again if that next page isn't where the duplicates end, and
 *		another choice must be made.
 */
static bool
_bt_useduplicatepage(Relation rel, Relation heapRel, Buffer buf,
					 bool *restorebinsrch, Size itemsz)
{
	Page		page = BufferGetPage(buf);
	BTPageOpaque lpageop;

	lpageop = (BTPageOpaque) PageGetSpecialPointer(page);
	Assert(P_ISLEAF(lpageop));

	/* Easy case -- there is space free on this page already */
	if (PageGetFreeSpace(page) >= itemsz)
		return true;

	/*
	 * Perform micro-vacuuming of the page if it looks like it has LP_DEAD
	 * items
	 */
	if (P_HAS_GARBAGE(lpageop))
	{
		_bt_vacuum_one_page(rel, buf, heapRel);

		*restorebinsrch = false;
		if (PageGetFreeSpace(page) >= itemsz)
			return true;		/* OK, now we have enough space */
	}

	/*----------
	 * _bt_findinsertloc() may need to split the page to put the item on
	 * this page, check whether we can put the tuple somewhere to the
	 * right, instead.
	 *
	 *	_bt_findinsertloc() keeps scanning right until it:
	 *		(a) reaches the last page where the tuple can legally go
	 *	Or until we:
	 *		(b) find a page with enough free space, or
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
	return random() <= (MAX_RANDOM_VALUE / 100);
}

/*----------
 *	_bt_insertonpg() -- Insert a tuple on a particular page in the index.
 *
 *		This recursive procedure does the following things:
 *
 *			+  if necessary, splits the target page (making sure that the
 *			   split is equitable as far as post-insert free space goes).
 *			+  inserts the tuple.
 *			+  if the page was split, pops the parent stack, and finds the
 *			   right place to insert the new child pointer (by walking
 *			   right using information stored in the parent stack).
 *			+  invokes itself with the appropriate tuple for the right
 *			   child page on the parent.
 *			+  updates the metapage if a true root or fast root is split.
 *
 *		On entry, we must have the correct buffer in which to do the
 *		insertion, and the buffer must be pinned and write-locked.  On return,
 *		we will have dropped both the pin and the lock on the buffer.
 *
 *		This routine only performs retail tuple insertions.  'itup' should
 *		always be either a non-highkey leaf item, or a downlink (new high
 *		key items are created indirectly, when a page is split).  When
 *		inserting to a non-leaf page, 'cbuf' is the left-sibling of the page
 *		we're inserting the downlink for.  This function will clear the
 *		INCOMPLETE_SPLIT flag on it, and release the buffer.
 *
 *		The locking interactions in this code are critical.  You should
 *		grok Lehman and Yao's paper before making any changes.  In addition,
 *		you need to understand how we disambiguate duplicate keys in this
 *		implementation, in order to be able to find our location using
 *		L&Y "move right" operations.
 *----------
 */
static void
_bt_insertonpg(Relation rel,
			   Buffer buf,
			   Buffer cbuf,
			   BTStack stack,
			   IndexTuple itup,
			   OffsetNumber newitemoff,
			   bool split_only_page)
{
	Page		page;
	BTPageOpaque lpageop;
	OffsetNumber firstright = InvalidOffsetNumber;
	Size		itemsz;
	int			indnatts = IndexRelationGetNumberOfAttributes(rel);
	int			indnkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);

	page = BufferGetPage(buf);
	lpageop = (BTPageOpaque) PageGetSpecialPointer(page);

	/* child buffer must be given iff inserting on an internal page */
	Assert(P_ISLEAF(lpageop) == !BufferIsValid(cbuf));
	/* tuple must have appropriate number of attributes */
	Assert(BTreeTupleGetNAtts(itup, rel) > 0);
	Assert(!P_ISLEAF(lpageop) || BTreeTupleGetNAtts(itup, rel) == indnatts);
	Assert(P_ISLEAF(lpageop) || BTreeTupleGetNAtts(itup, rel) <= indnkeyatts);

	/* The caller should've finished any incomplete splits already. */
	if (P_INCOMPLETE_SPLIT(lpageop))
		elog(ERROR, "cannot insert to incompletely split page %u",
			 BufferGetBlockNumber(buf));

	itemsz = IndexTupleSize(itup);
	itemsz = MAXALIGN(itemsz);	/* be safe, PageAddItem will do this but we
								 * need to be consistent */

	/*
	 * Do we need to split the page to fit the item on it?
	 *
	 * Note: PageGetFreeSpace() subtracts sizeof(ItemIdData) from its result,
	 * so this comparison is correct even though we appear to be accounting
	 * only for the item and not for its line pointer.
	 */
	if (PageGetFreeSpace(page) < itemsz)
	{
		bool		is_root = P_ISROOT(lpageop);
		bool		is_only = P_LEFTMOST(lpageop) && P_RIGHTMOST(lpageop);
		bool		truncate;
		bool		newitemonleft;
		Buffer		rbuf;

		/*
		 * If we're here then a pagesplit is needed. We should never reach
		 * here if we're using the fastpath since we should have checked for
		 * all the required conditions, including the fact that this page has
		 * enough freespace. Note that this routine can in theory deal with
		 * the situation where a NULL stack pointer is passed (that's what
		 * would happen if the fastpath is taken), like it does during crash
		 * recovery. But that path is much slower, defeating the very purpose
		 * of the optimization.  The following assertion should protect us
		 * from any future code changes that invalidate those assumptions.
		 *
		 * Note that whenever we fail to take the fastpath, we clear the
		 * cached block. Checking for a valid cached block at this point is
		 * enough to decide whether we're in a fastpath or not.
		 */
		Assert(!(P_ISLEAF(lpageop) &&
				 BlockNumberIsValid(RelationGetTargetBlock(rel))));

		/* Choose the split point */
		firstright = _bt_findsplitloc(rel, page, SPLIT_DEFAULT,
									  newitemoff, itemsz, itup,
									  &newitemonleft);

		/*
		 * Perform truncation of the new high key for the left half of the
		 * split when splitting a leaf page.  Don't do so with version 3
		 * indexes unless the index has non-key attributes.
		 */
		truncate = P_ISLEAF(lpageop) &&
			(_bt_heapkeyspace(rel) || indnatts != indnkeyatts);
		/* split the buffer into left and right halves */
		rbuf = _bt_split(rel, buf, cbuf, firstright,
						 newitemoff, itemsz, itup, newitemonleft, truncate);
		PredicateLockPageSplit(rel,
							   BufferGetBlockNumber(buf),
							   BufferGetBlockNumber(rbuf));

		/*----------
		 * By here,
		 *
		 *		+  our target page has been split;
		 *		+  the original tuple has been inserted;
		 *		+  we have write locks on both the old (left half)
		 *		   and new (right half) buffers, after the split; and
		 *		+  we know the key we want to insert into the parent
		 *		   (it's the "high key" on the left child page).
		 *
		 * We're ready to do the parent insertion.  We need to hold onto the
		 * locks for the child pages until we locate the parent, but we can
		 * release them before doing the actual insertion (see Lehman and Yao
		 * for the reasoning).
		 *----------
		 */
		_bt_insert_parent(rel, buf, rbuf, stack, is_root, is_only);
	}
	else
	{
		Buffer		metabuf = InvalidBuffer;
		Page		metapg = NULL;
		BTMetaPageData *metad = NULL;
		OffsetNumber itup_off;
		BlockNumber itup_blkno;
		BlockNumber cachedBlock = InvalidBlockNumber;

		itup_off = newitemoff;
		itup_blkno = BufferGetBlockNumber(buf);

		/*
		 * If we are doing this insert because we split a page that was the
		 * only one on its tree level, but was not the root, it may have been
		 * the "fast root".  We need to ensure that the fast root link points
		 * at or above the current page.  We can safely acquire a lock on the
		 * metapage here --- see comments for _bt_newroot().
		 */
		if (split_only_page)
		{
			Assert(!P_ISLEAF(lpageop));

			metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
			metapg = BufferGetPage(metabuf);
			metad = BTPageGetMeta(metapg);

			if (metad->btm_fastlevel >= lpageop->btpo.level)
			{
				/* no update wanted */
				_bt_relbuf(rel, metabuf);
				metabuf = InvalidBuffer;
			}
		}

		/*
		 * Every internal page should have exactly one negative infinity item
		 * at all times.  Only _bt_split() and _bt_newroot() should add items
		 * that become negative infinity items through truncation, since
		 * they're the only routines that allocate new internal pages.  Do not
		 * allow a retail insertion of a new item at the negative infinity
		 * offset.
		 */
		if (!P_ISLEAF(lpageop) && newitemoff == P_FIRSTDATAKEY(lpageop))
			elog(ERROR, "cannot insert second negative infinity item in block %u of index \"%s\"",
				 itup_blkno, RelationGetRelationName(rel));

		/* Do the update.  No ereport(ERROR) until changes are logged */
		START_CRIT_SECTION();

		if (!_bt_pgaddtup(page, itemsz, itup, newitemoff))
			elog(PANIC, "failed to add new item to block %u in index \"%s\"",
				 itup_blkno, RelationGetRelationName(rel));

		MarkBufferDirty(buf);

		if (BufferIsValid(metabuf))
		{
			/* upgrade meta-page if needed */
			if (metad->btm_version < BTREE_META_VERSION)
				_bt_upgrademetapage(metapg);
			metad->btm_fastroot = itup_blkno;
			metad->btm_fastlevel = lpageop->btpo.level;
			MarkBufferDirty(metabuf);
		}

		/* clear INCOMPLETE_SPLIT flag on child if inserting a downlink */
		if (BufferIsValid(cbuf))
		{
			Page		cpage = BufferGetPage(cbuf);
			BTPageOpaque cpageop = (BTPageOpaque) PageGetSpecialPointer(cpage);

			Assert(P_INCOMPLETE_SPLIT(cpageop));
			cpageop->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;
			MarkBufferDirty(cbuf);
		}

		/*
		 * Cache the block information if we just inserted into the rightmost
		 * leaf page of the index and it's not the root page.  For very small
		 * index where root is also the leaf, there is no point trying for any
		 * optimization.
		 */
		if (P_RIGHTMOST(lpageop) && P_ISLEAF(lpageop) && !P_ISROOT(lpageop))
			cachedBlock = BufferGetBlockNumber(buf);

		/* XLOG stuff */
		if (RelationNeedsWAL(rel))
		{
			xl_btree_insert xlrec;
			xl_btree_metadata xlmeta;
			uint8		xlinfo;
			XLogRecPtr	recptr;

			xlrec.offnum = itup_off;

			XLogBeginInsert();
			XLogRegisterData((char *) &xlrec, SizeOfBtreeInsert);

			if (P_ISLEAF(lpageop))
				xlinfo = XLOG_BTREE_INSERT_LEAF;
			else
			{
				/*
				 * Register the left child whose INCOMPLETE_SPLIT flag was
				 * cleared.
				 */
				XLogRegisterBuffer(1, cbuf, REGBUF_STANDARD);

				xlinfo = XLOG_BTREE_INSERT_UPPER;
			}

			if (BufferIsValid(metabuf))
			{
				Assert(metad->btm_version >= BTREE_META_VERSION);
				xlmeta.version = metad->btm_root;
				xlmeta.root = metad->btm_root;
				xlmeta.level = metad->btm_level;
				xlmeta.fastroot = metad->btm_fastroot;
				xlmeta.fastlevel = metad->btm_fastlevel;
				xlmeta.oldest_btpo_xact = metad->btm_oldest_btpo_xact;
				xlmeta.last_cleanup_num_heap_tuples =
					metad->btm_last_cleanup_num_heap_tuples;

				XLogRegisterBuffer(2, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);
				XLogRegisterBufData(2, (char *) &xlmeta, sizeof(xl_btree_metadata));

				xlinfo = XLOG_BTREE_INSERT_META;
			}

			XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
			XLogRegisterBufData(0, (char *) itup, IndexTupleSize(itup));

			recptr = XLogInsert(RM_BTREE_ID, xlinfo);

			if (BufferIsValid(metabuf))
			{
				PageSetLSN(metapg, recptr);
			}
			if (BufferIsValid(cbuf))
			{
				PageSetLSN(BufferGetPage(cbuf), recptr);
			}

			PageSetLSN(page, recptr);
		}

		END_CRIT_SECTION();

		/* release buffers */
		if (BufferIsValid(metabuf))
			_bt_relbuf(rel, metabuf);
		if (BufferIsValid(cbuf))
			_bt_relbuf(rel, cbuf);
		_bt_relbuf(rel, buf);

		/*
		 * If we decided to cache the insertion target block, then set it now.
		 * But before that, check for the height of the tree and don't go for
		 * the optimization for small indexes. We defer that check to this
		 * point to ensure that we don't call _bt_getrootheight while holding
		 * lock on any other block.
		 *
		 * We do this after dropping locks on all buffers. So the information
		 * about whether the insertion block is still the rightmost block or
		 * not may have changed in between. But we will deal with that during
		 * next insert operation. No special care is required while setting
		 * it.
		 */
		if (BlockNumberIsValid(cachedBlock) &&
			_bt_getrootheight(rel) >= BTREE_FASTPATH_MIN_LEVEL)
			RelationSetTargetBlock(rel, cachedBlock);
	}
}

/*
 *	_bt_split() -- split a page in the btree.
 *
 *		On entry, buf is the page to split, and is pinned and write-locked.
 *		firstright is the item index of the first item to be moved to the
 *		new right page.  newitemoff etc. tell us about the new item that
 *		must be inserted along with the data from the old page.  truncate
 *		tells us if the new high key should undergo suffix truncation.
 *		(Version 4 pivot tuples always have an explicit representation of
 *		the number of non-truncated attributes that remain.)
 *
 *		When splitting a non-leaf page, 'cbuf' is the left-sibling of the
 *		page we're inserting the downlink for.  This function will clear the
 *		INCOMPLETE_SPLIT flag on it, and release the buffer.
 *
 *		Returns the new right sibling of buf, pinned and write-locked.
 *		The pin and lock on buf are maintained.
 */
static Buffer
_bt_split(Relation rel, Buffer buf, Buffer cbuf, OffsetNumber firstright,
		  OffsetNumber newitemoff, Size newitemsz, IndexTuple newitem,
		  bool newitemonleft, bool truncate)
{
	Buffer		rbuf;
	Page		origpage;
	Page		leftpage,
				rightpage;
	BlockNumber origpagenumber,
				rightpagenumber;
	BTPageOpaque ropaque,
				lopaque,
				oopaque;
	Buffer		sbuf = InvalidBuffer;
	Page		spage = NULL;
	BTPageOpaque sopaque = NULL;
	Size		itemsz;
	ItemId		itemid;
	IndexTuple	item;
	OffsetNumber leftoff,
				rightoff;
	OffsetNumber maxoff;
	OffsetNumber i;
	bool		isleaf;
	IndexTuple	lefthikey;

	/* Acquire a new page to split into */
	rbuf = _bt_getbuf(rel, P_NEW, BT_WRITE);

	/*
	 * origpage is the original page to be split.  leftpage is a temporary
	 * buffer that receives the left-sibling data, which will be copied back
	 * into origpage on success.  rightpage is the new page that receives the
	 * right-sibling data.  If we fail before reaching the critical section,
	 * origpage hasn't been modified and leftpage is only workspace. In
	 * principle we shouldn't need to worry about rightpage either, because it
	 * hasn't been linked into the btree page structure; but to avoid leaving
	 * possibly-confusing junk behind, we are careful to rewrite rightpage as
	 * zeroes before throwing any error.
	 */
	origpage = BufferGetPage(buf);
	leftpage = PageGetTempPage(origpage);
	rightpage = BufferGetPage(rbuf);

	origpagenumber = BufferGetBlockNumber(buf);
	rightpagenumber = BufferGetBlockNumber(rbuf);

	_bt_pageinit(leftpage, BufferGetPageSize(buf));
	/* rightpage was already initialized by _bt_getbuf */

	/*
	 * Copy the original page's LSN into leftpage, which will become the
	 * updated version of the page.  We need this because XLogInsert will
	 * examine the LSN and possibly dump it in a page image.
	 */
	PageSetLSN(leftpage, PageGetLSN(origpage));

	/* init btree private data */
	oopaque = (BTPageOpaque) PageGetSpecialPointer(origpage);
	lopaque = (BTPageOpaque) PageGetSpecialPointer(leftpage);
	ropaque = (BTPageOpaque) PageGetSpecialPointer(rightpage);

	isleaf = P_ISLEAF(oopaque);

	/* if we're splitting this page, it won't be the root when we're done */
	/* also, clear the SPLIT_END and HAS_GARBAGE flags in both pages */
	lopaque->btpo_flags = oopaque->btpo_flags;
	lopaque->btpo_flags &= ~(BTP_ROOT | BTP_SPLIT_END | BTP_HAS_GARBAGE);
	ropaque->btpo_flags = lopaque->btpo_flags;
	/* set flag in left page indicating that the right page has no downlink */
	lopaque->btpo_flags |= BTP_INCOMPLETE_SPLIT;
	lopaque->btpo_prev = oopaque->btpo_prev;
	lopaque->btpo_next = rightpagenumber;
	ropaque->btpo_prev = origpagenumber;
	ropaque->btpo_next = oopaque->btpo_next;
	lopaque->btpo.level = ropaque->btpo.level = oopaque->btpo.level;
	/* Since we already have write-lock on both pages, ok to read cycleid */
	lopaque->btpo_cycleid = _bt_vacuum_cycleid(rel);
	ropaque->btpo_cycleid = lopaque->btpo_cycleid;

	/*
	 * If the page we're splitting is not the rightmost page at its level in
	 * the tree, then the first entry on the page is the high key for the
	 * page.  We need to copy that to the right half.  Otherwise (meaning the
	 * rightmost page case), all the items on the right half will be user
	 * data.
	 */
	rightoff = P_HIKEY;

	if (!P_RIGHTMOST(oopaque))
	{
		itemid = PageGetItemId(origpage, P_HIKEY);
		itemsz = ItemIdGetLength(itemid);
		item = (IndexTuple) PageGetItem(origpage, itemid);
		Assert(BTreeTupleGetNAtts(item, rel) > 0);
		Assert(BTreeTupleGetNAtts(item, rel) <=
			   IndexRelationGetNumberOfKeyAttributes(rel));
		if (PageAddItem(rightpage, (Item) item, itemsz, rightoff,
						false, false) == InvalidOffsetNumber)
		{
			memset(rightpage, 0, BufferGetPageSize(rbuf));
			elog(ERROR, "failed to add hikey to the right sibling"
				 " while splitting block %u of index \"%s\"",
				 origpagenumber, RelationGetRelationName(rel));
		}
		rightoff = OffsetNumberNext(rightoff);
	}

	/*
	 * The "high key" for the new left page will be the first key that's going
	 * to go into the new right page, or possibly a truncated version if this
	 * is a leaf page split.  This might be either the existing data item at
	 * position firstright, or the incoming tuple.
	 *
	 * Lehman and Yao use the last left item as the new high key for the left
	 * page.  Despite appearances, the new high key is generated in a way
	 * that's consistent with their approach.  See comments above
	 * _bt_findsplitloc for an explanation.
	 */
	leftoff = P_HIKEY;
	if (!newitemonleft && newitemoff == firstright)
	{
		/* incoming tuple will become first on right page */
		itemsz = newitemsz;
		item = newitem;
	}
	else
	{
		/* existing item at firstright will become first on right page */
		itemid = PageGetItemId(origpage, firstright);
		itemsz = ItemIdGetLength(itemid);
		item = (IndexTuple) PageGetItem(origpage, itemid);
	}

	/*
	 * Truncate nondistinguishing key attributes of the high key item before
	 * inserting it on the left page.  This can only happen at the leaf level,
	 * since in general all pivot tuple values originate from leaf level high
	 * keys.  This isn't just about avoiding unnecessary work, though;
	 * truncating unneeded key suffix attributes can only be performed at the
	 * leaf level anyway.  This is because a pivot tuple in a grandparent page
	 * must guide a search not only to the correct parent page, but also to
	 * the correct leaf page.
	 */
	if (truncate)
	{
		IndexTuple	lastleft;

		/*
		 * Determine which tuple will become the last on the left page.  The
		 * last left tuple and the first right tuple enclose the split point,
		 * and are needed to determine how far truncation can go while still
		 * leaving us with a high key that distinguishes the left side from
		 * the right side.
		 */
		Assert(isleaf);
		if (newitemonleft && newitemoff == firstright)
		{
			/* incoming tuple will become last on left page */
			lastleft = newitem;
		}
		else
		{
			OffsetNumber lastleftoff;

			/* item just before firstright will become last on left page */
			lastleftoff = OffsetNumberPrev(firstright);
			Assert(lastleftoff >= P_FIRSTDATAKEY(oopaque));
			itemid = PageGetItemId(origpage, lastleftoff);
			lastleft = (IndexTuple) PageGetItem(origpage, itemid);
		}

		/*
		 * Truncate first item on the right side to create a new high key for
		 * the left side.  The high key must be strictly less than all tuples
		 * on the right side of the split, but can be equal to the last item
		 * on the left side of the split.
		 */
		Assert(lastleft != item);
		lefthikey = _bt_truncate(rel, lastleft, item, false);
		itemsz = IndexTupleSize(lefthikey);
		itemsz = MAXALIGN(itemsz);
	}
	else
		lefthikey = item;

	Assert(BTreeTupleGetNAtts(lefthikey, rel) > 0);
	Assert(BTreeTupleGetNAtts(lefthikey, rel) <=
		   IndexRelationGetNumberOfKeyAttributes(rel));
	if (PageAddItem(leftpage, (Item) lefthikey, itemsz, leftoff,
					false, false) == InvalidOffsetNumber)
	{
		memset(rightpage, 0, BufferGetPageSize(rbuf));
		elog(ERROR, "failed to add hikey to the left sibling"
			 " while splitting block %u of index \"%s\"",
			 origpagenumber, RelationGetRelationName(rel));
	}
	leftoff = OffsetNumberNext(leftoff);
	/* be tidy */
	if (lefthikey != item)
		pfree(lefthikey);

	/*
	 * Now transfer all the data items to the appropriate page.
	 *
	 * Note: we *must* insert at least the right page's items in item-number
	 * order, for the benefit of _bt_restore_page().
	 */
	maxoff = PageGetMaxOffsetNumber(origpage);

	for (i = P_FIRSTDATAKEY(oopaque); i <= maxoff; i = OffsetNumberNext(i))
	{
		itemid = PageGetItemId(origpage, i);
		itemsz = ItemIdGetLength(itemid);
		item = (IndexTuple) PageGetItem(origpage, itemid);

		/* does new item belong before this one? */
		if (i == newitemoff)
		{
			if (newitemonleft)
			{
				if (!_bt_pgaddtup(leftpage, newitemsz, newitem, leftoff))
				{
					memset(rightpage, 0, BufferGetPageSize(rbuf));
					elog(ERROR, "failed to add new item to the left sibling"
						 " while splitting block %u of index \"%s\"",
						 origpagenumber, RelationGetRelationName(rel));
				}
				leftoff = OffsetNumberNext(leftoff);
			}
			else
			{
				if (!_bt_pgaddtup(rightpage, newitemsz, newitem, rightoff))
				{
					memset(rightpage, 0, BufferGetPageSize(rbuf));
					elog(ERROR, "failed to add new item to the right sibling"
						 " while splitting block %u of index \"%s\"",
						 origpagenumber, RelationGetRelationName(rel));
				}
				rightoff = OffsetNumberNext(rightoff);
			}
		}

		/* decide which page to put it on */
		if (i < firstright)
		{
			if (!_bt_pgaddtup(leftpage, itemsz, item, leftoff))
			{
				memset(rightpage, 0, BufferGetPageSize(rbuf));
				elog(ERROR, "failed to add old item to the left sibling"
					 " while splitting block %u of index \"%s\"",
					 origpagenumber, RelationGetRelationName(rel));
			}
			leftoff = OffsetNumberNext(leftoff);
		}
		else
		{
			if (!_bt_pgaddtup(rightpage, itemsz, item, rightoff))
			{
				memset(rightpage, 0, BufferGetPageSize(rbuf));
				elog(ERROR, "failed to add old item to the right sibling"
					 " while splitting block %u of index \"%s\"",
					 origpagenumber, RelationGetRelationName(rel));
			}
			rightoff = OffsetNumberNext(rightoff);
		}
	}

	/* cope with possibility that newitem goes at the end */
	if (i <= newitemoff)
	{
		/*
		 * Can't have newitemonleft here; that would imply we were told to put
		 * *everything* on the left page, which cannot fit (if it could, we'd
		 * not be splitting the page).
		 */
		Assert(!newitemonleft);
		if (!_bt_pgaddtup(rightpage, newitemsz, newitem, rightoff))
		{
			memset(rightpage, 0, BufferGetPageSize(rbuf));
			elog(ERROR, "failed to add new item to the right sibling"
				 " while splitting block %u of index \"%s\"",
				 origpagenumber, RelationGetRelationName(rel));
		}
		rightoff = OffsetNumberNext(rightoff);
	}

	/*
	 * We have to grab the right sibling (if any) and fix the prev pointer
	 * there. We are guaranteed that this is deadlock-free since no other
	 * writer will be holding a lock on that page and trying to move left, and
	 * all readers release locks on a page before trying to fetch its
	 * neighbors.
	 */

	if (!P_RIGHTMOST(oopaque))
	{
		sbuf = _bt_getbuf(rel, oopaque->btpo_next, BT_WRITE);
		spage = BufferGetPage(sbuf);
		sopaque = (BTPageOpaque) PageGetSpecialPointer(spage);
		if (sopaque->btpo_prev != origpagenumber)
		{
			memset(rightpage, 0, BufferGetPageSize(rbuf));
			elog(ERROR, "right sibling's left-link doesn't match: "
				 "block %u links to %u instead of expected %u in index \"%s\"",
				 oopaque->btpo_next, sopaque->btpo_prev, origpagenumber,
				 RelationGetRelationName(rel));
		}

		/*
		 * Check to see if we can set the SPLIT_END flag in the right-hand
		 * split page; this can save some I/O for vacuum since it need not
		 * proceed to the right sibling.  We can set the flag if the right
		 * sibling has a different cycleid: that means it could not be part of
		 * a group of pages that were all split off from the same ancestor
		 * page.  If you're confused, imagine that page A splits to A B and
		 * then again, yielding A C B, while vacuum is in progress.  Tuples
		 * originally in A could now be in either B or C, hence vacuum must
		 * examine both pages.  But if D, our right sibling, has a different
		 * cycleid then it could not contain any tuples that were in A when
		 * the vacuum started.
		 */
		if (sopaque->btpo_cycleid != ropaque->btpo_cycleid)
			ropaque->btpo_flags |= BTP_SPLIT_END;
	}

	/*
	 * Right sibling is locked, new siblings are prepared, but original page
	 * is not updated yet.
	 *
	 * NO EREPORT(ERROR) till right sibling is updated.  We can get away with
	 * not starting the critical section till here because we haven't been
	 * scribbling on the original page yet; see comments above.
	 */
	START_CRIT_SECTION();

	/*
	 * By here, the original data page has been split into two new halves, and
	 * these are correct.  The algorithm requires that the left page never
	 * move during a split, so we copy the new left page back on top of the
	 * original.  Note that this is not a waste of time, since we also require
	 * (in the page management code) that the center of a page always be
	 * clean, and the most efficient way to guarantee this is just to compact
	 * the data by reinserting it into a new left page.  (XXX the latter
	 * comment is probably obsolete; but in any case it's good to not scribble
	 * on the original page until we enter the critical section.)
	 *
	 * We need to do this before writing the WAL record, so that XLogInsert
	 * can WAL log an image of the page if necessary.
	 */
	PageRestoreTempPage(leftpage, origpage);
	/* leftpage, lopaque must not be used below here */

	MarkBufferDirty(buf);
	MarkBufferDirty(rbuf);

	if (!P_RIGHTMOST(ropaque))
	{
		sopaque->btpo_prev = rightpagenumber;
		MarkBufferDirty(sbuf);
	}

	/*
	 * Clear INCOMPLETE_SPLIT flag on child if inserting the new item finishes
	 * a split.
	 */
	if (!isleaf)
	{
		Page		cpage = BufferGetPage(cbuf);
		BTPageOpaque cpageop = (BTPageOpaque) PageGetSpecialPointer(cpage);

		cpageop->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;
		MarkBufferDirty(cbuf);
	}

	/* XLOG stuff */
	if (RelationNeedsWAL(rel))
	{
		xl_btree_split xlrec;
		uint8		xlinfo;
		XLogRecPtr	recptr;

		xlrec.level = ropaque->btpo.level;
		xlrec.firstright = firstright;
		xlrec.newitemoff = newitemoff;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfBtreeSplit);

		XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
		XLogRegisterBuffer(1, rbuf, REGBUF_WILL_INIT);
		/* Log the right sibling, because we've changed its prev-pointer. */
		if (!P_RIGHTMOST(ropaque))
			XLogRegisterBuffer(2, sbuf, REGBUF_STANDARD);
		if (BufferIsValid(cbuf))
			XLogRegisterBuffer(3, cbuf, REGBUF_STANDARD);

		/*
		 * Log the new item, if it was inserted on the left page. (If it was
		 * put on the right page, we don't need to explicitly WAL log it
		 * because it's included with all the other items on the right page.)
		 * Show the new item as belonging to the left page buffer, so that it
		 * is not stored if XLogInsert decides it needs a full-page image of
		 * the left page.  We store the offset anyway, though, to support
		 * archive compression of these records.
		 */
		if (newitemonleft)
			XLogRegisterBufData(0, (char *) newitem, MAXALIGN(newitemsz));

		/* Log left page.  We must also log the left page's high key. */
		itemid = PageGetItemId(origpage, P_HIKEY);
		item = (IndexTuple) PageGetItem(origpage, itemid);
		XLogRegisterBufData(0, (char *) item, MAXALIGN(IndexTupleSize(item)));

		/*
		 * Log the contents of the right page in the format understood by
		 * _bt_restore_page(). We set lastrdata->buffer to InvalidBuffer,
		 * because we're going to recreate the whole page anyway, so it should
		 * never be stored by XLogInsert.
		 *
		 * Direct access to page is not good but faster - we should implement
		 * some new func in page API.  Note we only store the tuples
		 * themselves, knowing that they were inserted in item-number order
		 * and so the item pointers can be reconstructed.  See comments for
		 * _bt_restore_page().
		 */
		XLogRegisterBufData(1,
							(char *) rightpage + ((PageHeader) rightpage)->pd_upper,
							((PageHeader) rightpage)->pd_special - ((PageHeader) rightpage)->pd_upper);

		xlinfo = newitemonleft ? XLOG_BTREE_SPLIT_L : XLOG_BTREE_SPLIT_R;
		recptr = XLogInsert(RM_BTREE_ID, xlinfo);

		PageSetLSN(origpage, recptr);
		PageSetLSN(rightpage, recptr);
		if (!P_RIGHTMOST(ropaque))
		{
			PageSetLSN(spage, recptr);
		}
		if (!isleaf)
		{
			PageSetLSN(BufferGetPage(cbuf), recptr);
		}
	}

	END_CRIT_SECTION();

	/* release the old right sibling */
	if (!P_RIGHTMOST(ropaque))
		_bt_relbuf(rel, sbuf);

	/* release the child */
	if (!isleaf)
		_bt_relbuf(rel, cbuf);

	/* split's done */
	return rbuf;
}

/*
 *	_bt_findsplitloc() -- find an appropriate place to split a page.
 *
 * The idea here is to equalize the free space that will be on each split
 * page, *after accounting for the inserted tuple*.  (If we fail to account
 * for it, we might find ourselves with too little room on the page that
 * it needs to go into!)
 *
 * We also give some weight to suffix truncation in deciding a split point
 * on leaf pages.  We try to select a point where a distinguishing attribute
 * appears earlier in the new high key for the left side of the split, in
 * order to maximize the number of trailing attributes that can be truncated
 * away.  Initially, only candidate split points that imply an acceptable
 * balance of free space on each side are considered.  This is even useful
 * with pages that only have a single (non-TID) attribute, since it's
 * helpful to avoid appending an explicit heap TID attribute to the new
 * pivot tuple (high key/downlink) when it cannot actually be truncated.
 * Note that it is always assumed that caller goes on to perform truncation,
 * even with pg_upgrade'd indexes where that isn't actually the case.  There
 * is still a modest benefit to choosing a split location while weighing
 * suffix truncation: the resulting (untruncated) pivot tuples are
 * nevertheless more predictive of future space utilization.
 *
 * We do all we can to avoid having to append a heap TID in the new high
 * key.  We may have to call ourselves recursively in many duplicates mode.
 * This happens when a heap TID would otherwise be appended, but the page
 * isn't completely full of logical duplicates (there may be a few as two
 * distinct values).  Many duplicates mode has no hard requirements for
 * space utilization, though it still keeps the use of space balanced as a
 * non-binding secondary goal.  This significantly improves fan-out in
 * practice, at least with most affected workloads.
 *
 * If the page is the rightmost page on its level, we instead try to arrange
 * to leave the left split page fillfactor% full.  In this way, when we are
 * inserting successively increasing keys (consider sequences, timestamps,
 * etc) we will end up with a tree whose pages are about fillfactor% full,
 * instead of the 50% full result that we'd get without this special case.
 * This is the same as nbtsort.c produces for a newly-created tree.  Note
 * that leaf and nonleaf pages use different fillfactors.  Note also that
 * the fillfactor% is determined dynamically when _bt_dosplitatnewitem()
 * indicates that there are localized monotonically increasing insertions,
 * or monotonically decreasing (DESC order) insertions. (This can only
 * happen with the default strategy, and should be thought of as a variant
 * of the fillfactor% special case that is applied only when inserting into
 * non-rightmost pages.)
 *
 * If called recursively in single value mode, we also try to arrange to
 * leave the left split page fillfactor% full, though we arrange to use a
 * fillfactor that's even more left-heavy than the fillfactor used for
 * rightmost pages.  This greatly helps with space management in cases where
 * tuples with the same attribute values span multiple pages.  Newly
 * inserted duplicates will tend to have higher heap TID values, so we'll
 * end up splitting to the right in the manner of ascending insertions of
 * monotonically increasing values.  See nbtree/README for more information
 * about suffix truncation, and how a split point is chosen.
 *
 * We are passed the intended insert position of the new tuple, expressed as
 * the offsetnumber of the tuple it must go in front of.  (This could be
 * maxoff+1 if the tuple is to go at the end.)
 *
 * We return the index of the first existing tuple that should go on the
 * righthand page, plus a boolean indicating whether the new tuple goes on
 * the left or right page.  The bool is necessary to disambiguate the case
 * where firstright == newitemoff.
 *
 * The high key for the left page is formed using the first item on the
 * right page, which may seem to be contrary to Lehman & Yao's approach of
 * using the left page's last item as its new high key.  It isn't, though;
 * suffix truncation will leave the left page's high key equal to the last
 * item on the left page when two tuples with equal key values enclose the
 * split point.  It's convenient to always express a split point as a
 * firstright offset due to internal page splits, which leave us with a
 * right half whose first item becomes a negative infinity item through
 * truncation to 0 attributes.  In effect, internal page splits store
 * firstright's "separator" key at the end of the left page (as left's new
 * high key), and store its downlink at the start of the right page.  In
 * other words, internal page splits conceptually split in the middle of the
 * firstright tuple, not on either side of it.  Crucially, when splitting
 * either a leaf page or an internal page, the new high key will be strictly
 * less than the first item on the right page in all cases, despite the fact
 * that we start with the assumption that firstright becomes the new high
 * key.
 */
static OffsetNumber
_bt_findsplitloc(Relation rel,
				 Page page,
				 SplitMode mode,
				 OffsetNumber newitemoff,
				 Size newitemsz,
				 IndexTuple newitem,
				 bool *newitemonleft)
{
	BTPageOpaque opaque;
	OffsetNumber offnum;
	OffsetNumber maxoff;
	ItemId		itemid;
	FindSplitData state;
	int			leftspace,
				rightspace,
				olddataitemstotal,
				olddataitemstoleft,
				perfectpenalty;
	bool		goodenoughfound;
	SplitPoint	splits[MAX_LEAF_SPLIT_POINTS];
	SplitMode	secondmode;
	OffsetNumber finalfirstright;

	opaque = (BTPageOpaque) PageGetSpecialPointer(page);
	maxoff = PageGetMaxOffsetNumber(page);

	/* Total free space available on a btree page, after fixed overhead */
	leftspace = rightspace =
		PageGetPageSize(page) - SizeOfPageHeaderData -
		MAXALIGN(sizeof(BTPageOpaqueData));

	/* The right page will have the same high key as the old page */
	if (!P_RIGHTMOST(opaque))
	{
		itemid = PageGetItemId(page, P_HIKEY);
		rightspace -= (int) (MAXALIGN(ItemIdGetLength(itemid)) +
							 sizeof(ItemIdData));
	}

	/* Count up total space in data items without actually scanning 'em */
	olddataitemstotal = rightspace - (int) PageGetExactFreeSpace(page);

	/* Passed-in newitemsz is MAXALIGNED but does not include line pointer */
	state.mode = mode;
	state.newitemsz = newitemsz + sizeof(ItemIdData);
	state.hikeyheaptid = (mode == SPLIT_SINGLE_VALUE);
	state.is_leaf = P_ISLEAF(opaque);
	state.is_weighted = P_RIGHTMOST(opaque);
	if (state.is_leaf)
	{
		/*
		 * Consider split at new tuple optimization.  See
		 * _bt_dosplitatnewitem() for an explanation.
		 */
		if (state.mode == SPLIT_DEFAULT && !P_RIGHTMOST(opaque) &&
			_bt_dosplitatnewitem(rel, page, newitemoff, newitem))
		{
			/*
			 * fillfactor% is dynamically set through interpolation of the
			 * new/incoming tuple's offset position
			 */
			if (newitemoff > maxoff)
				state.fillfactor = (double) BTREE_DEFAULT_FILLFACTOR / 100.0;
			else if (newitemoff == P_FIRSTDATAKEY(opaque))
				state.fillfactor = (double) BTREE_MIN_FILLFACTOR / 100.0;
			else
				state.fillfactor =
					((double) newitemoff / (((double) maxoff + 1)));

			state.is_weighted = true;
		}
		else if (state.mode != SPLIT_SINGLE_VALUE)
		{
			/* Only used on rightmost page */
			state.fillfactor = RelationGetFillFactor(rel,
													 BTREE_DEFAULT_FILLFACTOR) / 100.0;
		}
		else
		{
			state.fillfactor = BTREE_SINGLEVAL_FILLFACTOR / 100.0;
			state.is_weighted = true;
		}
	}
	else
	{
		Assert(state.mode == SPLIT_DEFAULT);
		/* Only used on rightmost page */
		state.fillfactor = BTREE_NONLEAF_FILLFACTOR / 100.0;
	}

	/*
	 * Set limits on the split interval/number of candidate split points as
	 * appropriate.  The "Prefix B-Trees" paper refers to this as sigma l for
	 * leaf splits and sigma b for internal ("branch") splits.  It's hard to
	 * provide a theoretical justification for the size of the split interval,
	 * though it's clear that a small split interval improves space
	 * utilization.
	 *
	 * (Also set interval for case when we split a page that has many
	 * duplicates, or split a page that's entirely full of tuples of a single
	 * value.  Future locality of access is prioritized over short-term space
	 * utilization in these cases.)
	 */
	if (!state.is_leaf)
		state.maxsplits = MAX_INTERNAL_SPLIT_POINTS;
	else if (state.mode == SPLIT_DEFAULT)
		state.maxsplits = Min(Max(3, maxoff * 0.05), MAX_LEAF_SPLIT_POINTS);
	else if (state.mode == SPLIT_MANY_DUPLICATES)
		state.maxsplits = maxoff + 2;
	else
		state.maxsplits = 1;
	state.nsplits = 0;
	if (state.mode != SPLIT_MANY_DUPLICATES)
		state.splits = splits;
	else
		state.splits = palloc(sizeof(SplitPoint) * state.maxsplits);

	state.leftspace = leftspace;
	state.rightspace = rightspace;
	state.olddataitemstotal = olddataitemstotal;
	state.newitemoff = newitemoff;

	/*
	 * Finding the best possible split would require checking all the possible
	 * split points, because of the high-key and left-key special cases.
	 * That's probably more work than it's worth outside of many duplicates
	 * mode; instead, stop as soon as we find sufficiently-many "good-enough"
	 * splits, where good-enough is defined as an imbalance in free space of
	 * no more than pagesize/16 (arbitrary...) This should let us stop near
	 * the middle on most pages, instead of plowing to the end.  Many
	 * duplicates mode must consider all possible choices, and so does not use
	 * this threshold for anything.
	 */
	state.goodenough = leftspace / 16;

	/*
	 * Scan through the data items and calculate space usage for a split at
	 * each possible position.
	 */
	olddataitemstoleft = 0;
	goodenoughfound = false;

	for (offnum = P_FIRSTDATAKEY(opaque);
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		Size		itemsz;
		int			delta;

		itemid = PageGetItemId(page, offnum);
		itemsz = MAXALIGN(ItemIdGetLength(itemid)) + sizeof(ItemIdData);

		/*
		 * Will the new item go to left or right of split?
		 */
		if (offnum > newitemoff)
			delta = _bt_checksplitloc(&state, offnum, true,
									  olddataitemstoleft, itemsz);

		else if (offnum < newitemoff)
			delta = _bt_checksplitloc(&state, offnum, false,
									  olddataitemstoleft, itemsz);
		else
		{
			/* need to try it both ways! */
			(void) _bt_checksplitloc(&state, offnum, true,
									 olddataitemstoleft, itemsz);

			delta = _bt_checksplitloc(&state, offnum, false,
									  olddataitemstoleft, itemsz);
		}

		/* Record when good-enough choice found */
		if (state.nsplits > 0 && state.splits[0].delta <= state.goodenough)
			goodenoughfound = true;

		/*
		 * Abort scan once we've found a good-enough choice, and reach the
		 * point where we stop finding new good-enough choices.  Don't do this
		 * in many duplicates mode, though, since that has to be completely
		 * exhaustive.
		 */
		if (goodenoughfound && state.mode != SPLIT_MANY_DUPLICATES &&
			delta > state.goodenough)
			break;

		olddataitemstoleft += itemsz;
	}

	/*
	 * If the new item goes as the last item, check for splitting so that all
	 * the old items go to the left page and the new item goes to the right
	 * page.
	 */
	if (newitemoff > maxoff &&
		(!goodenoughfound || state.mode == SPLIT_MANY_DUPLICATES))
		_bt_checksplitloc(&state, newitemoff, false, olddataitemstotal, 0);

	/*
	 * I believe it is not possible to fail to find a feasible split, but just
	 * in case ...
	 */
	if (state.nsplits == 0)
		elog(ERROR, "could not find a feasible split point for index \"%s\"",
			 RelationGetRelationName(rel));

	/*
	 * Search among acceptable split points for the entry with the lowest
	 * penalty.  See _bt_split_penalty() for the definition of penalty.  The
	 * goal here is to increase fan-out, by choosing a split point which is
	 * amenable to being made smaller by suffix truncation, or is already
	 * small.
	 *
	 * First find lowest possible penalty among acceptable split points -- the
	 * "perfect" penalty.  This will be passed to _bt_bestsplitloc() if it
	 * determines that candidate split points are good enough to finish
	 * default mode split.  Perfect penalty saves _bt_bestsplitloc()
	 * additional work around calculating penalties.
	 */
	perfectpenalty = _bt_perfect_penalty(rel, page, &state, newitemoff,
										 newitem, &secondmode);

	/* Start second pass over page if _bt_perfect_penalty() told us to */
	if (secondmode != SPLIT_DEFAULT)
		return _bt_findsplitloc(rel, page, secondmode, newitemoff, newitemsz,
								newitem, newitemonleft);

	/*
	 * Search among acceptable split points for the entry that has the lowest
	 * penalty, and thus maximizes fan-out.  Sets *newitemonleft for us.
	 */
	finalfirstright = _bt_bestsplitloc(rel, page, &state, perfectpenalty,
									   newitemoff, newitem, newitemonleft);
	/* Be tidy */
	if (state.splits != splits)
		pfree(state.splits);

	return finalfirstright;
}

/*
 * Subroutine to analyze a particular possible split choice (ie, firstright
 * and newitemonleft settings), and record the best split so far in *state.
 *
 * firstoldonright is the offset of the first item on the original page
 * that goes to the right page, and firstoldonrightsz is the size of that
 * tuple. firstoldonright can be > max offset, which means that all the old
 * items go to the left page and only the new item goes to the right page.
 * In that case, firstoldonrightsz is not used.
 *
 * olddataitemstoleft is the total size of all old items to the left of
 * firstoldonright.
 *
 * Returns delta between space that will be left free on left and right side
 * of split.
 */
static int
_bt_checksplitloc(FindSplitData *state,
				  OffsetNumber firstoldonright,
				  bool newitemonleft,
				  int olddataitemstoleft,
				  Size firstoldonrightsz)
{
	int			leftfree,
				rightfree,
				leftleafheaptidsz;
	Size		firstrightitemsz;
	bool		newitemisfirstonright;

	/* Is the new item going to be the first item on the right page? */
	newitemisfirstonright = (firstoldonright == state->newitemoff
							 && !newitemonleft);

	if (newitemisfirstonright)
		firstrightitemsz = state->newitemsz;
	else
		firstrightitemsz = firstoldonrightsz;

	/* Account for all the old tuples */
	leftfree = state->leftspace - olddataitemstoleft;
	rightfree = state->rightspace -
		(state->olddataitemstotal - olddataitemstoleft);

	/*
	 * The first item on the right page becomes the high key of the left page;
	 * therefore it counts against left space as well as right space (we
	 * cannot assume that suffix truncation will make it any smaller).  When
	 * index has included attributes, then those attributes of left page high
	 * key will be truncated leaving that page with slightly more free space.
	 * However, that shouldn't affect our ability to find valid split
	 * location, since we err in the direction of being pessimistic about free
	 * space on the left half.  Besides, even when suffix truncation of
	 * non-TID attributes occurs, there often won't be an entire MAXALIGN()
	 * quantum in pivot space savings.
	 */
	leftfree -= firstrightitemsz;

	/*
	 * Assume that suffix truncation cannot avoid adding a heap TID to the
	 * left half's new high key when splitting at the leaf level.  Don't let
	 * this impact the balance of free space in the common case where adding a
	 * heap TID is considered very unlikely, though, since there is no reason
	 * to accept a likely-suboptimal split.
	 *
	 * When adding a heap TID seems likely, then actually factor that in to
	 * delta calculation, rather than just having it as a constraint on
	 * whether or not a split is acceptable.
	 */
	leftleafheaptidsz = 0;
	if (state->is_leaf)
	{
		if (!state->hikeyheaptid)
			leftleafheaptidsz = sizeof(ItemPointerData);
		else
			leftfree -= (int) sizeof(ItemPointerData);
	}

	/* account for the new item */
	if (newitemonleft)
		leftfree -= (int) state->newitemsz;
	else
		rightfree -= (int) state->newitemsz;

	/*
	 * If we are not on the leaf level, we will be able to discard the key
	 * data from the first item that winds up on the right page.
	 */
	if (!state->is_leaf)
		rightfree += (int) firstrightitemsz -
			(int) (MAXALIGN(sizeof(IndexTupleData)) + sizeof(ItemIdData));

	/*
	 * If feasible split point with lower delta than that of most marginal
	 * spit point so far, or we haven't run out of space for split points,
	 * remember it.
	 */
	if (leftfree - leftleafheaptidsz >= 0 && rightfree >= 0)
	{
		int			delta;

		if (state->is_weighted)
		{
			/*
			 * If splitting a rightmost page, or in single value mode, try to
			 * put (100-fillfactor)% of free space on left page. See comments
			 * for _bt_findsplitloc.
			 */
			delta = (state->fillfactor * leftfree)
				- ((1.0 - state->fillfactor) * rightfree);
		}
		else
		{
			/* Otherwise, aim for equal free space on both sides */
			delta = leftfree - rightfree;
		}

		if (delta < 0)
			delta = -delta;
		/*
		 * Optimization: Don't recognize differences among marginal split
		 * points that are unlikely to end up being used anyway.
		 *
		 * We cannot do this in many duplicates mode, because that hurts cases
		 * where there are a small number of available distinguishing split
		 * points, and consistently picking the least worst choice among them
		 * matters. (e.g., a non-unique index whose leaf pages each contain a
		 * small number of distinct values, with each value duplicated a
		 * uniform number of times.)
		 */
		if (delta > state->goodenough && state->mode != SPLIT_MANY_DUPLICATES)
			delta = state->goodenough + 1;
		if (state->nsplits < state->maxsplits ||
			delta < state->splits[state->nsplits - 1].delta)
		{
			SplitPoint	newsplit;
			int			j;

			newsplit.delta = delta;
			newsplit.newitemonleft = newitemonleft;
			newsplit.firstright = firstoldonright;

			/*
			 * Make space at the end of the state array for new candidate
			 * split point if we haven't already reached the maximum number of
			 * split points.
			 */
			if (state->nsplits < state->maxsplits)
				state->nsplits++;

			/*
			 * Replace the final item in the nsplits-wise array.  The final
			 * item is either a garbage still-uninitialized entry, or the most
			 * marginal real entry when we already have as many split points
			 * as we're willing to consider.
			 */
			for (j = state->nsplits - 1;
				 j > 0 && state->splits[j - 1].delta > newsplit.delta;
				 j--)
			{
				state->splits[j] = state->splits[j - 1];
			}
			state->splits[j] = newsplit;
		}

		return delta;
	}

	return INT_MAX;
}

/*
 * Subroutine to determine whether or not the page should be split at
 * approximately the point that the new/incoming item would have been
 * inserted.
 *
 * This routine infers two distinct cases in which splitting around the new
 * item's insertion point is likely to lead to better space utilization over
 * time:
 *
 * - Composite indexes that consist of one or more leading columns that
 *   describe some grouping, plus a trailing, monotonically increasing
 *   column.  If there happened to only be one grouping then the traditional
 *   rightmost page split default fillfactor% would be used to good effect,
 *   so it seems worth recognizing this case.  This usage pattern is
 *   prevalent in the TPC-C benchmark, and is assumed to be common in real
 *   world applications.
 *
 * - DESC-ordered insertions, including DESC-ordered single (non-heap-TID)
 *   key attribute indexes.  We don't want the performance of explicitly
 *   DESC-ordered indexes to be out of line with an equivalent ASC-ordered
 *   index.  Also, there may be organic cases where items are continually
 *   inserted in DESC order for an index with ASC sort order.
 *
 * Caller uses fillfactor% rather than using the new item offset directly
 * because it allows suffix truncation to be applied using the usual
 * criteria, which can still be helpful.  This approach is also more
 * maintainable, since restrictions on split points can be handled in the
 * usual way.
 *
 * Localized insert points are inferred here by observing that neighboring
 * heap TIDs are "adjacent".  For example, if the new item has distinct key
 * attribute values to the existing item that belongs to its immediate left,
 * and the item to its left has a heap TID whose offset is exactly one less
 * than the new item's offset, then caller is told to use its new-item-split
 * strategy.  It isn't of much consequence if this routine incorrectly
 * infers that an interesting case is taking place, provided that that
 * doesn't happen very often.  In particular, it should not be possible to
 * construct a test case where the routine consistently does the wrong
 * thing.  Since heap TID "adjacency" is such a delicate condition, and
 * since there is no reason to imagine that random insertions should ever
 * consistent leave new tuples at the first or last position on the page
 * when a split is triggered, that will never happen.
 *
 * Note that we avoid using the split-at-new fillfactor% when we'd have to
 * append a heap TID during suffix truncation.  We also insist that there
 * are no varwidth attributes or NULL attribute values in new item, since
 * that invalidates interpolating from the new item offset.  Besides,
 * varwidths generally imply the use of datatypes where ordered insertions
 * are not a naturally occurring phenomenon.
 */
static bool
_bt_dosplitatnewitem(Relation rel, Page page, OffsetNumber newitemoff,
					 IndexTuple newitem)
{
	ItemId		itemid;
	OffsetNumber maxoff;
	BTPageOpaque opaque;
	IndexTuple	tup;
	int16		nkeyatts;

	if (IndexTupleHasNulls(newitem) || IndexTupleHasVarwidths(newitem))
		return false;

	opaque = (BTPageOpaque) PageGetSpecialPointer(page);
	maxoff = PageGetMaxOffsetNumber(page);

	/* Avoid optimization entirely on pages with large items */
	if (maxoff <= 3)
		return false;

	nkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);

	/*
	 * When heap TIDs appear in DESC order, consider left-heavy split.
	 *
	 * Accept left-heavy split when new item, which will be inserted at first
	 * data offset, has adjacent TID to extant item at that position.
	 */
	if (newitemoff == P_FIRSTDATAKEY(opaque))
	{
		itemid = PageGetItemId(page, P_FIRSTDATAKEY(opaque));
		tup = (IndexTuple) PageGetItem(page, itemid);

		return _bt_adjacenthtid(&tup->t_tid, &newitem->t_tid) &&
			_bt_leave_natts_fast(rel, tup, newitem) <= nkeyatts;
	}

	/* Single key indexes only use DESC optimization */
	if (nkeyatts == 1)
		return false;

	/*
	 * When tuple heap TIDs appear in ASC order, consider right-heavy split,
	 * even though this may not be the right-most page.
	 *
	 * Accept right-heavy split when new item, which belongs after any
	 * existing page offset, has adjacent TID to extant item that's the last
	 * on the page.
	 */
	if (newitemoff > maxoff)
	{
		itemid = PageGetItemId(page, maxoff);
		tup = (IndexTuple) PageGetItem(page, itemid);

		return _bt_adjacenthtid(&tup->t_tid, &newitem->t_tid) &&
			_bt_leave_natts_fast(rel, tup, newitem) <= nkeyatts;
	}

	/*
	 * When new item is approximately in the middle of the page, look for
	 * adjacency among new item, and extant item that belongs to the left of
	 * the new item in the keyspace.
	 */
	itemid = PageGetItemId(page, OffsetNumberPrev(newitemoff));
	tup = (IndexTuple) PageGetItem(page, itemid);

	return _bt_adjacenthtid(&tup->t_tid, &newitem->t_tid) &&
		_bt_leave_natts_fast(rel, tup, newitem) <= nkeyatts;
}

/*
 * Subroutine to find the "best" split point among an array of acceptable
 * candidate split points that split without there being an excessively high
 * delta between the space left free on the left and right halves.  The "best"
 * split point is the split point with the lowest penalty, which is an
 * abstract idea whose definition varies depending on whether we're splitting
 * at the leaf level, or an internal level.  See _bt_split_penalty() for the
 * definition.
 *
 * "perfectpenalty" is assumed to be the lowest possible penalty among
 * candidate split points.  This allows us to return early without wasting
 * cycles on calculating the first differing attribute for all candidate
 * splits when that clearly cannot improve our choice.  This optimization is
 * important for several common cases, including insertion into a primary key
 * index on an auto-incremented or monotonically increasing integer column.
 *
 * We return the index of the first existing tuple that should go on the
 * righthand page, plus a boolean indicating if new item is on left of split
 * point.
 */
static OffsetNumber
_bt_bestsplitloc(Relation rel,
				 Page page,
				 FindSplitData *state,
				 int perfectpenalty,
				 OffsetNumber newitemoff,
				 IndexTuple newitem,
				 bool *newitemonleft)
{
	int			bestpenalty,
				lowsplit;

	/* No point calculating penalties in trivial cases */
	if (perfectpenalty == INT_MAX || state->nsplits == 1)
	{
		*newitemonleft = state->splits[0].newitemonleft;
		return state->splits[0].firstright;
	}

	bestpenalty = INT_MAX;
	lowsplit = 0;
	for (int i = 0; i < state->nsplits; i++)
	{
		int			penalty;

		penalty = _bt_split_penalty(rel, page, newitemoff, newitem,
									state->splits + i, state->is_leaf);

		if (penalty <= perfectpenalty)
		{
			bestpenalty = penalty;
			lowsplit = i;
			break;
		}

		if (penalty < bestpenalty)
		{
			bestpenalty = penalty;
			lowsplit = i;
		}
	}

	*newitemonleft = state->splits[lowsplit].newitemonleft;
	return state->splits[lowsplit].firstright;
}

/*
 * Subroutine to find the lowest possible penalty for any acceptable candidate
 * split point.  This may be lower than any real penalty for any of the
 * candidate split points, in which case the optimization is ineffective.
 * Split penalties are generally discrete rather than continuous, so an
 * actually-obtainable penalty is common.
 *
 * This is also a convenient point to decide to either finish splitting
 * the page using the default strategy, or, alternatively, to do a second pass
 * over page using a different strategy.  (This only happens with leaf pages.)
 */
static int
_bt_perfect_penalty(Relation rel, Page page, FindSplitData *state,
					OffsetNumber newitemoff, IndexTuple newitem,
					SplitMode *secondmode)
{
	ItemId		itemid;
	OffsetNumber center;
	IndexTuple	leftmost,
				rightmost;
	int			perfectpenalty;

	/* Assume that a second pass over page won't be required for now */
	*secondmode = SPLIT_DEFAULT;

	/*
	 * There are a much smaller number of candidate split points when
	 * splitting an internal page, so we can afford to be exhaustive.  Only
	 * give up when pivot that will be inserted into parent is as small as
	 * possible.
	 */
	if (!state->is_leaf)
		return MAXALIGN(sizeof(IndexTupleData) + 1);

	/*
	 * During a many duplicates pass over page, we settle for a "perfect"
	 * split point that merely avoids appending a heap TID in new pivot.
	 * Appending a heap TID is harmful enough to fan-out that it's worth
	 * avoiding at all costs, but it doesn't make sense to go to those lengths
	 * to also be able to truncate an extra, earlier attribute.
	 */
	if (state->mode == SPLIT_MANY_DUPLICATES)
		return IndexRelationGetNumberOfKeyAttributes(rel);
	else if (state->mode == SPLIT_SINGLE_VALUE)
		return INT_MAX;

	/*
	 * Complicated though common case -- leaf page default mode split.
	 *
	 * Iterate from the end of split array to the start, in search of the
	 * firstright-wise leftmost and rightmost entries among acceptable split
	 * points.  The split point with the lowest delta is at the start of the
	 * array.  It is deemed to be the split point whose firstright offset is
	 * at the center.  Split points with firstright offsets at both the left
	 * and right extremes among acceptable split points will be found at the
	 * end of caller's array.
	 */
	leftmost = NULL;
	rightmost = NULL;
	center = state->splits[0].firstright;

	/*
	 * Leaf split points can be thought of as points _between_ tuples on the
	 * original unsplit page image, at least if you pretend that the incoming
	 * tuple is already on the page to be split (imagine that the original
	 * unsplit page actually had enough space to fit the incoming tuple).  The
	 * rightmost tuple is the tuple that is immediately to the right of a
	 * split point that is itself rightmost.  Likewise, the leftmost tuple is
	 * the tuple to the left of the leftmost split point.  It's important that
	 * many duplicates mode has every opportunity to avoid picking a split
	 * point that requires that suffix truncation append a heap TID to new
	 * pivot tuple.
	 *
	 * When there are very few candidates, no sensible comparison can be made
	 * here, resulting in caller selecting lowest delta/the center split point
	 * by default.  Typically, leftmost and rightmost tuples will be located
	 * almost immediately.
	 */
	perfectpenalty = IndexRelationGetNumberOfKeyAttributes(rel);
	for (int j = state->nsplits - 1; j > 1; j--)
	{
		SplitPoint *split = state->splits + j;

		if (!leftmost && split->firstright <= center)
		{
			if (split->newitemonleft && newitemoff == split->firstright)
				leftmost = newitem;
			else
			{
				itemid = PageGetItemId(page,
									   OffsetNumberPrev(split->firstright));
				leftmost = (IndexTuple) PageGetItem(page, itemid);
			}
		}

		if (!rightmost && split->firstright >= center)
		{
			if (!split->newitemonleft && newitemoff == split->firstright)
				rightmost = newitem;
			else
			{
				itemid = PageGetItemId(page, split->firstright);
				rightmost = (IndexTuple) PageGetItem(page, itemid);
			}
		}

		if (leftmost && rightmost)
		{
			Assert(leftmost != rightmost);
			perfectpenalty = _bt_leave_natts_fast(rel, leftmost, rightmost);
			break;
		}
	}

	/*
	 * Work out which type of second pass caller must perform when even a
	 * "perfect" penalty fails to avoid appending a heap TID to new pivot
	 * tuple.
	 */
	if (perfectpenalty > IndexRelationGetNumberOfKeyAttributes(rel))
	{
		BTPageOpaque opaque;
		OffsetNumber maxoff;
		int			outerpenalty;

		opaque = (BTPageOpaque) PageGetSpecialPointer(page);
		maxoff = PageGetMaxOffsetNumber(page);

		if (P_FIRSTDATAKEY(opaque) == newitemoff)
			leftmost = newitem;
		else
		{
			itemid = PageGetItemId(page, P_FIRSTDATAKEY(opaque));
			leftmost = (IndexTuple) PageGetItem(page, itemid);
		}

		if (newitemoff > maxoff)
			rightmost = newitem;
		else
		{
			itemid = PageGetItemId(page, maxoff);
			rightmost = (IndexTuple) PageGetItem(page, itemid);
		}

		Assert(leftmost != rightmost);
		outerpenalty = _bt_leave_natts_fast(rel, leftmost, rightmost);

		/*
		 * If page has many duplicates but is not entirely full of duplicates,
		 * a many duplicates mode pass will be performed.  If page is entirely
		 * full of duplicates, a single value mode pass will be performed.
		 *
		 * Caller should avoid a single value mode pass when incoming tuple
		 * doesn't sort highest among items on the page, though.  Instead, we
		 * instruct caller to continue with original default mode split, since
		 * an out-of-order new duplicate item predicts further inserts towards
		 * the left/middle of the original page's keyspace.  Evenly sharing
		 * space among each half of the split avoids pathological performance.
		 */
		if (outerpenalty > IndexRelationGetNumberOfKeyAttributes(rel))
		{
			if (maxoff < newitemoff)
				*secondmode = SPLIT_SINGLE_VALUE;
			else
			{
				perfectpenalty = INT_MAX;
				*secondmode = SPLIT_DEFAULT;
			}
		}
		else
			*secondmode = SPLIT_MANY_DUPLICATES;
	}

	return perfectpenalty;
}

/*
 * Subroutine to find penalty for caller's candidate split point.
 *
 * On leaf pages, penalty is the attribute number that distinguishes each side
 * of a split.  It's the last attribute that needs to be included in new high
 * key for left page.  It can be greater than the number of key attributes in
 * cases where a heap TID needs to be appending during truncation.
 *
 * On internal pages, penalty is simply the size of the first item on the
 * right half of the split (excluding ItemId overhead) which becomes the new
 * high key for the left page.  Internal page splits always use default mode.
 */
static int
_bt_split_penalty(Relation rel, Page page, OffsetNumber newitemoff,
				  IndexTuple newitem, SplitPoint *split, bool is_leaf)
{
	ItemId		itemid;
	IndexTuple	lastleft;
	IndexTuple	firstright;

	if (split->newitemonleft && newitemoff == split->firstright)
		lastleft = newitem;
	else
	{
		itemid = PageGetItemId(page, OffsetNumberPrev(split->firstright));
		lastleft = (IndexTuple) PageGetItem(page, itemid);
	}

	if (!split->newitemonleft && newitemoff == split->firstright)
		firstright = newitem;
	else
	{
		itemid = PageGetItemId(page, split->firstright);
		firstright = (IndexTuple) PageGetItem(page, itemid);
	}

	if (!is_leaf)
		return IndexTupleSize(firstright);

	Assert(lastleft != firstright);
	return _bt_leave_natts_fast(rel, lastleft, firstright);
}

/*
 * Subroutine for determining if two heap TIDS are "adjacent".
 *
 * Adjacent means that the high TID is very likely to have been inserted into
 * heap relation immediately after the low TID, probably by the same
 * transaction, and probably not through heap_update().  This is not a
 * commutative condition.
 */
static bool
_bt_adjacenthtid(ItemPointer lowhtid, ItemPointer highhtid)
{
	BlockNumber lowblk,
				highblk;
	OffsetNumber lowoff,
				highoff;

	lowblk = ItemPointerGetBlockNumber(lowhtid);
	highblk = ItemPointerGetBlockNumber(highhtid);
	lowoff = ItemPointerGetOffsetNumber(lowhtid);
	highoff = ItemPointerGetOffsetNumber(highhtid);

	/* When heap blocks match, second offset should be one up */
	if (lowblk == highblk && OffsetNumberNext(lowoff) == highoff)
		return true;

	/* When heap block one up, second offset should be FirstOffsetNumber */
	if (lowblk + 1 == highblk && highoff == FirstOffsetNumber)
		return true;

	return false;
}

/*
 * _bt_insert_parent() -- Insert downlink into parent after a page split.
 *
 * On entry, buf and rbuf are the left and right split pages, which we
 * still hold write locks on per the L&Y algorithm.  We release the
 * write locks once we have write lock on the parent page.  (Any sooner,
 * and it'd be possible for some other process to try to split or delete
 * one of these pages, and get confused because it cannot find the downlink.)
 *
 * stack - stack showing how we got here.  May be NULL in cases that don't
 *			have to be efficient (concurrent ROOT split, WAL recovery)
 * is_root - we split the true root
 * is_only - we split a page alone on its level (might have been fast root)
 */
static void
_bt_insert_parent(Relation rel,
				  Buffer buf,
				  Buffer rbuf,
				  BTStack stack,
				  bool is_root,
				  bool is_only)
{
	/*
	 * Here we have to do something Lehman and Yao don't talk about: deal with
	 * a root split and construction of a new root.  If our stack is empty
	 * then we have just split a node on what had been the root level when we
	 * descended the tree.  If it was still the root then we perform a
	 * new-root construction.  If it *wasn't* the root anymore, search to find
	 * the next higher level that someone constructed meanwhile, and find the
	 * right place to insert as for the normal case.
	 *
	 * If we have to search for the parent level, we do so by re-descending
	 * from the root.  This is not super-efficient, but it's rare enough not
	 * to matter.
	 */
	if (is_root)
	{
		Buffer		rootbuf;

		Assert(stack == NULL);
		Assert(is_only);
		/* create a new root node and update the metapage */
		rootbuf = _bt_newroot(rel, buf, rbuf);
		/* release the split buffers */
		_bt_relbuf(rel, rootbuf);
		_bt_relbuf(rel, rbuf);
		_bt_relbuf(rel, buf);
	}
	else
	{
		BlockNumber bknum = BufferGetBlockNumber(buf);
		BlockNumber rbknum = BufferGetBlockNumber(rbuf);
		Page		page = BufferGetPage(buf);
		IndexTuple	new_item;
		BTStackData fakestack;
		IndexTuple	ritem;
		Buffer		pbuf;

		if (stack == NULL)
		{
			BTPageOpaque lpageop;

			elog(DEBUG2, "concurrent ROOT page split");
			lpageop = (BTPageOpaque) PageGetSpecialPointer(page);
			/* Find the leftmost page at the next level up */
			pbuf = _bt_get_endpoint(rel, lpageop->btpo.level + 1, false,
									NULL);
			/* Set up a phony stack entry pointing there */
			stack = &fakestack;
			stack->bts_blkno = BufferGetBlockNumber(pbuf);
			stack->bts_offset = InvalidOffsetNumber;
			stack->bts_btentry = InvalidBlockNumber;
			stack->bts_parent = NULL;
			_bt_relbuf(rel, pbuf);
		}

		/* get high key from left, a strict lower bound for new right page */
		ritem = (IndexTuple) PageGetItem(page,
										 PageGetItemId(page, P_HIKEY));

		/* form an index tuple that points at the new right page */
		new_item = CopyIndexTuple(ritem);
		BTreeInnerTupleSetDownLink(new_item, rbknum);

		/*
		 * Find the parent buffer and get the parent page.
		 *
		 * Oops - if we were moved right then we need to change stack item! We
		 * want to find parent pointing to where we are, right ?	- vadim
		 * 05/27/97
		 */
		stack->bts_btentry = bknum;
		pbuf = _bt_getstackbuf(rel, stack, BT_WRITE);

		/*
		 * Now we can unlock the right child. The left child will be unlocked
		 * by _bt_insertonpg().
		 */
		_bt_relbuf(rel, rbuf);

		/* Check for error only after writing children */
		if (pbuf == InvalidBuffer)
			elog(ERROR, "failed to re-find parent key in index \"%s\" for split pages %u/%u",
				 RelationGetRelationName(rel), bknum, rbknum);

		/* Recursively update the parent */
		_bt_insertonpg(rel, pbuf, buf, stack->bts_parent,
					   new_item, stack->bts_offset + 1,
					   is_only);

		/* be tidy */
		pfree(new_item);
	}
}

/*
 * _bt_finish_split() -- Finish an incomplete split
 *
 * A crash or other failure can leave a split incomplete.  The insertion
 * routines won't allow to insert on a page that is incompletely split.
 * Before inserting on such a page, call _bt_finish_split().
 *
 * On entry, 'lbuf' must be locked in write-mode.  On exit, it is unlocked
 * and unpinned.
 */
void
_bt_finish_split(Relation rel, Buffer lbuf, BTStack stack)
{
	Page		lpage = BufferGetPage(lbuf);
	BTPageOpaque lpageop = (BTPageOpaque) PageGetSpecialPointer(lpage);
	Buffer		rbuf;
	Page		rpage;
	BTPageOpaque rpageop;
	bool		was_root;
	bool		was_only;

	Assert(P_INCOMPLETE_SPLIT(lpageop));

	/* Lock right sibling, the one missing the downlink */
	rbuf = _bt_getbuf(rel, lpageop->btpo_next, BT_WRITE);
	rpage = BufferGetPage(rbuf);
	rpageop = (BTPageOpaque) PageGetSpecialPointer(rpage);

	/* Could this be a root split? */
	if (!stack)
	{
		Buffer		metabuf;
		Page		metapg;
		BTMetaPageData *metad;

		/* acquire lock on the metapage */
		metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
		metapg = BufferGetPage(metabuf);
		metad = BTPageGetMeta(metapg);

		was_root = (metad->btm_root == BufferGetBlockNumber(lbuf));

		_bt_relbuf(rel, metabuf);
	}
	else
		was_root = false;

	/* Was this the only page on the level before split? */
	was_only = (P_LEFTMOST(lpageop) && P_RIGHTMOST(rpageop));

	elog(DEBUG1, "finishing incomplete split of %u/%u",
		 BufferGetBlockNumber(lbuf), BufferGetBlockNumber(rbuf));

	_bt_insert_parent(rel, lbuf, rbuf, stack, was_root, was_only);
}

/*
 *	_bt_getstackbuf() -- Walk back up the tree one step, and find the item
 *						 we last looked at in the parent.
 *
 *		This is possible because we save the downlink from the parent item,
 *		which is enough to uniquely identify it.  Insertions into the parent
 *		level could cause the item to move right; deletions could cause it
 *		to move left, but not left of the page we previously found it in.
 *
 *		Adjusts bts_blkno & bts_offset if changed.
 *
 *		Returns InvalidBuffer if item not found (should not happen).
 */
Buffer
_bt_getstackbuf(Relation rel, BTStack stack, int access)
{
	BlockNumber blkno;
	OffsetNumber start;

	blkno = stack->bts_blkno;
	start = stack->bts_offset;

	for (;;)
	{
		Buffer		buf;
		Page		page;
		BTPageOpaque opaque;

		buf = _bt_getbuf(rel, blkno, access);
		page = BufferGetPage(buf);
		opaque = (BTPageOpaque) PageGetSpecialPointer(page);

		if (access == BT_WRITE && P_INCOMPLETE_SPLIT(opaque))
		{
			_bt_finish_split(rel, buf, stack->bts_parent);
			continue;
		}

		if (!P_IGNORE(opaque))
		{
			OffsetNumber offnum,
						minoff,
						maxoff;
			ItemId		itemid;
			IndexTuple	item;

			minoff = P_FIRSTDATAKEY(opaque);
			maxoff = PageGetMaxOffsetNumber(page);

			/*
			 * start = InvalidOffsetNumber means "search the whole page". We
			 * need this test anyway due to possibility that page has a high
			 * key now when it didn't before.
			 */
			if (start < minoff)
				start = minoff;

			/*
			 * Need this check too, to guard against possibility that page
			 * split since we visited it originally.
			 */
			if (start > maxoff)
				start = OffsetNumberNext(maxoff);

			/*
			 * These loops will check every item on the page --- but in an
			 * order that's attuned to the probability of where it actually
			 * is.  Scan to the right first, then to the left.
			 */
			for (offnum = start;
				 offnum <= maxoff;
				 offnum = OffsetNumberNext(offnum))
			{
				itemid = PageGetItemId(page, offnum);
				item = (IndexTuple) PageGetItem(page, itemid);

				if (BTreeInnerTupleGetDownLink(item) == stack->bts_btentry)
				{
					/* Return accurate pointer to where link is now */
					stack->bts_blkno = blkno;
					stack->bts_offset = offnum;
					return buf;
				}
			}

			for (offnum = OffsetNumberPrev(start);
				 offnum >= minoff;
				 offnum = OffsetNumberPrev(offnum))
			{
				itemid = PageGetItemId(page, offnum);
				item = (IndexTuple) PageGetItem(page, itemid);

				if (BTreeInnerTupleGetDownLink(item) == stack->bts_btentry)
				{
					/* Return accurate pointer to where link is now */
					stack->bts_blkno = blkno;
					stack->bts_offset = offnum;
					return buf;
				}
			}
		}

		/*
		 * The item we're looking for moved right at least one page.
		 */
		if (P_RIGHTMOST(opaque))
		{
			_bt_relbuf(rel, buf);
			return InvalidBuffer;
		}
		blkno = opaque->btpo_next;
		start = InvalidOffsetNumber;
		_bt_relbuf(rel, buf);
	}
}

/*
 *	_bt_newroot() -- Create a new root page for the index.
 *
 *		We've just split the old root page and need to create a new one.
 *		In order to do this, we add a new root page to the file, then lock
 *		the metadata page and update it.  This is guaranteed to be deadlock-
 *		free, because all readers release their locks on the metadata page
 *		before trying to lock the root, and all writers lock the root before
 *		trying to lock the metadata page.  We have a write lock on the old
 *		root page, so we have not introduced any cycles into the waits-for
 *		graph.
 *
 *		On entry, lbuf (the old root) and rbuf (its new peer) are write-
 *		locked. On exit, a new root page exists with entries for the
 *		two new children, metapage is updated and unlocked/unpinned.
 *		The new root buffer is returned to caller which has to unlock/unpin
 *		lbuf, rbuf & rootbuf.
 */
static Buffer
_bt_newroot(Relation rel, Buffer lbuf, Buffer rbuf)
{
	Buffer		rootbuf;
	Page		lpage,
				rootpage;
	BlockNumber lbkno,
				rbkno;
	BlockNumber rootblknum;
	BTPageOpaque rootopaque;
	BTPageOpaque lopaque;
	ItemId		itemid;
	IndexTuple	item;
	IndexTuple	left_item;
	Size		left_item_sz;
	IndexTuple	right_item;
	Size		right_item_sz;
	Buffer		metabuf;
	Page		metapg;
	BTMetaPageData *metad;

	lbkno = BufferGetBlockNumber(lbuf);
	rbkno = BufferGetBlockNumber(rbuf);
	lpage = BufferGetPage(lbuf);
	lopaque = (BTPageOpaque) PageGetSpecialPointer(lpage);

	/* get a new root page */
	rootbuf = _bt_getbuf(rel, P_NEW, BT_WRITE);
	rootpage = BufferGetPage(rootbuf);
	rootblknum = BufferGetBlockNumber(rootbuf);

	/* acquire lock on the metapage */
	metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
	metapg = BufferGetPage(metabuf);
	metad = BTPageGetMeta(metapg);

	/*
	 * Create downlink item for left page (old root).  Since this will be the
	 * first item in a non-leaf page, it implicitly has minus-infinity key
	 * value, so we need not store any actual key in it.
	 */
	left_item_sz = sizeof(IndexTupleData);
	left_item = (IndexTuple) palloc(left_item_sz);
	left_item->t_info = left_item_sz;
	BTreeInnerTupleSetDownLink(left_item, lbkno);
	BTreeTupleSetNAtts(left_item, 0);

	/*
	 * Create downlink item for right page.  The key for it is obtained from
	 * the "high key" position in the left page.
	 */
	itemid = PageGetItemId(lpage, P_HIKEY);
	right_item_sz = ItemIdGetLength(itemid);
	item = (IndexTuple) PageGetItem(lpage, itemid);
	right_item = CopyIndexTuple(item);
	BTreeInnerTupleSetDownLink(right_item, rbkno);

	/* NO EREPORT(ERROR) from here till newroot op is logged */
	START_CRIT_SECTION();

	/* upgrade metapage if needed */
	if (metad->btm_version < BTREE_META_VERSION)
		_bt_upgrademetapage(metapg);

	/* set btree special data */
	rootopaque = (BTPageOpaque) PageGetSpecialPointer(rootpage);
	rootopaque->btpo_prev = rootopaque->btpo_next = P_NONE;
	rootopaque->btpo_flags = BTP_ROOT;
	rootopaque->btpo.level =
		((BTPageOpaque) PageGetSpecialPointer(lpage))->btpo.level + 1;
	rootopaque->btpo_cycleid = 0;

	/* update metapage data */
	metad->btm_root = rootblknum;
	metad->btm_level = rootopaque->btpo.level;
	metad->btm_fastroot = rootblknum;
	metad->btm_fastlevel = rootopaque->btpo.level;

	/*
	 * Insert the left page pointer into the new root page.  The root page is
	 * the rightmost page on its level so there is no "high key" in it; the
	 * two items will go into positions P_HIKEY and P_FIRSTKEY.
	 *
	 * Note: we *must* insert the two items in item-number order, for the
	 * benefit of _bt_restore_page().
	 */
	Assert(BTreeTupleGetNAtts(left_item, rel) == 0);
	if (PageAddItem(rootpage, (Item) left_item, left_item_sz, P_HIKEY,
					false, false) == InvalidOffsetNumber)
		elog(PANIC, "failed to add leftkey to new root page"
			 " while splitting block %u of index \"%s\"",
			 BufferGetBlockNumber(lbuf), RelationGetRelationName(rel));

	/*
	 * insert the right page pointer into the new root page.
	 */
	Assert(BTreeTupleGetNAtts(right_item, rel) > 0);
	Assert(BTreeTupleGetNAtts(right_item, rel) <=
		   IndexRelationGetNumberOfKeyAttributes(rel));
	if (PageAddItem(rootpage, (Item) right_item, right_item_sz, P_FIRSTKEY,
					false, false) == InvalidOffsetNumber)
		elog(PANIC, "failed to add rightkey to new root page"
			 " while splitting block %u of index \"%s\"",
			 BufferGetBlockNumber(lbuf), RelationGetRelationName(rel));

	/* Clear the incomplete-split flag in the left child */
	Assert(P_INCOMPLETE_SPLIT(lopaque));
	lopaque->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;
	MarkBufferDirty(lbuf);

	MarkBufferDirty(rootbuf);
	MarkBufferDirty(metabuf);

	/* XLOG stuff */
	if (RelationNeedsWAL(rel))
	{
		xl_btree_newroot xlrec;
		XLogRecPtr	recptr;
		xl_btree_metadata md;

		xlrec.rootblk = rootblknum;
		xlrec.level = metad->btm_level;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfBtreeNewroot);

		XLogRegisterBuffer(0, rootbuf, REGBUF_WILL_INIT);
		XLogRegisterBuffer(1, lbuf, REGBUF_STANDARD);
		XLogRegisterBuffer(2, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);

		Assert(metad->btm_version >= BTREE_META_VERSION);
		md.version = metad->btm_version;
		md.root = rootblknum;
		md.level = metad->btm_level;
		md.fastroot = rootblknum;
		md.fastlevel = metad->btm_level;
		md.oldest_btpo_xact = metad->btm_oldest_btpo_xact;
		md.last_cleanup_num_heap_tuples = metad->btm_last_cleanup_num_heap_tuples;

		XLogRegisterBufData(2, (char *) &md, sizeof(xl_btree_metadata));

		/*
		 * Direct access to page is not good but faster - we should implement
		 * some new func in page API.
		 */
		XLogRegisterBufData(0,
							(char *) rootpage + ((PageHeader) rootpage)->pd_upper,
							((PageHeader) rootpage)->pd_special -
							((PageHeader) rootpage)->pd_upper);

		recptr = XLogInsert(RM_BTREE_ID, XLOG_BTREE_NEWROOT);

		PageSetLSN(lpage, recptr);
		PageSetLSN(rootpage, recptr);
		PageSetLSN(metapg, recptr);
	}

	END_CRIT_SECTION();

	/* done with metapage */
	_bt_relbuf(rel, metabuf);

	pfree(left_item);
	pfree(right_item);

	return rootbuf;
}

/*
 *	_bt_pgaddtup() -- add a tuple to a particular page in the index.
 *
 *		This routine adds the tuple to the page as requested.  It does
 *		not affect pin/lock status, but you'd better have a write lock
 *		and pin on the target buffer!  Don't forget to write and release
 *		the buffer afterwards, either.
 *
 *		The main difference between this routine and a bare PageAddItem call
 *		is that this code knows that the leftmost index tuple on a non-leaf
 *		btree page doesn't need to have a key.  Therefore, it strips such
 *		tuples down to just the tuple header.  CAUTION: this works ONLY if
 *		we insert the tuples in order, so that the given itup_off does
 *		represent the final position of the tuple!
 */
static bool
_bt_pgaddtup(Page page,
			 Size itemsize,
			 IndexTuple itup,
			 OffsetNumber itup_off)
{
	BTPageOpaque opaque = (BTPageOpaque) PageGetSpecialPointer(page);
	IndexTupleData trunctuple;

	if (!P_ISLEAF(opaque) && itup_off == P_FIRSTDATAKEY(opaque))
	{
		trunctuple = *itup;
		trunctuple.t_info = sizeof(IndexTupleData);
		/* Deliberately zero INDEX_ALT_TID_MASK bits */
		BTreeTupleSetNAtts(&trunctuple, 0);
		itup = &trunctuple;
		itemsize = sizeof(IndexTupleData);
	}

	if (PageAddItem(page, (Item) itup, itemsize, itup_off,
					false, false) == InvalidOffsetNumber)
		return false;

	return true;
}

/*
 * _bt_isequal - used in _bt_doinsert in check for duplicates.
 *
 * This is very similar to _bt_compare, except for NULL and negative infinity
 * handling.  Rule is simple: NOT_NULL not equal NULL, NULL not equal NULL too.
 */
static bool
_bt_isequal(TupleDesc itupdesc, BTScanInsert itup_scankey, Page page,
			OffsetNumber offnum)
{
	IndexTuple	itup;
	ScanKey		scankey;
	int			i;

	/* Better be comparing to a non-pivot item */
	Assert(P_ISLEAF((BTPageOpaque) PageGetSpecialPointer(page)));
	Assert(offnum >= P_FIRSTDATAKEY((BTPageOpaque) PageGetSpecialPointer(page)));

	scankey = itup_scankey->scankeys;
	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offnum));

	for (i = 1; i <= itup_scankey->keysz; i++)
	{
		AttrNumber	attno;
		Datum		datum;
		bool		isNull;
		int32		result;

		attno = scankey->sk_attno;
		Assert(attno == i);
		datum = index_getattr(itup, attno, itupdesc, &isNull);

		/* NULLs are never equal to anything */
		if (isNull || (scankey->sk_flags & SK_ISNULL))
			return false;

		result = DatumGetInt32(FunctionCall2Coll(&scankey->sk_func,
												 scankey->sk_collation,
												 datum,
												 scankey->sk_argument));

		if (result != 0)
			return false;

		scankey++;
	}

	/* if we get here, the keys are equal */
	return true;
}

/*
 * _bt_vacuum_one_page - vacuum just one index page.
 *
 * Try to remove LP_DEAD items from the given page.  The passed buffer
 * must be exclusive-locked, but unlike a real VACUUM, we don't need a
 * super-exclusive "cleanup" lock (see nbtree/README).
 */
static void
_bt_vacuum_one_page(Relation rel, Buffer buffer, Relation heapRel)
{
	OffsetNumber deletable[MaxOffsetNumber];
	int			ndeletable = 0;
	OffsetNumber offnum,
				minoff,
				maxoff;
	Page		page = BufferGetPage(buffer);
	BTPageOpaque opaque = (BTPageOpaque) PageGetSpecialPointer(page);

	/*
	 * Scan over all items to see which ones need to be deleted according to
	 * LP_DEAD flags.
	 */
	minoff = P_FIRSTDATAKEY(opaque);
	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = minoff;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemId = PageGetItemId(page, offnum);

		if (ItemIdIsDead(itemId))
			deletable[ndeletable++] = offnum;
	}

	if (ndeletable > 0)
		_bt_delitems_delete(rel, buffer, deletable, ndeletable, heapRel);

	/*
	 * Note: if we didn't find any LP_DEAD items, then the page's
	 * BTP_HAS_GARBAGE hint bit is falsely set.  We do not bother expending a
	 * separate write to clear it, however.  We will clear it when we split
	 * the page.
	 */
}
