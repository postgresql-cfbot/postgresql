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
#include "utils/datum.h"
#include "utils/tqual.h"

/* #define DEBUG_SPLITS */
#ifdef DEBUG_SPLITS
#include "catalog/catalog.h"
#endif

/* Minimum tree height for application of fastpath optimization */
#define BTREE_FASTPATH_MIN_LEVEL	2
#define STACK_SPLIT_POINTS			15

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
	Size		newitemsz;		/* size of new item to be inserted */
	int			fillfactor;		/* needed for weighted splits */
	bool		is_leaf;		/* T if splitting a leaf page */
	bool		is_weighted;	/* T if weighted (e.g. rightmost) split */
	OffsetNumber newitemoff;	/* where the new item is to be inserted */
	int			leftspace;		/* space available for items on left page */
	int			rightspace;		/* space available for items on right page */
	int			olddataitemstotal;	/* space taken by old items */

	int			maxsplit;		/* Maximum number of splits */
	int			nsplits;		/* Current number of splits */
	SplitPoint *splits;			/* Sorted by delta */
} FindSplitData;


static Buffer _bt_newroot(Relation rel, Buffer lbuf, Buffer rbuf);

static TransactionId _bt_check_unique(Relation rel, IndexTuple itup,
				 Relation heapRel, Buffer buf, OffsetNumber offset,
				 ScanKey itup_scankey,
				 IndexUniqueCheck checkUnique, bool *is_unique,
				 uint32 *speculativeToken);
static void _bt_findinsertloc(Relation rel,
				  Buffer *bufptr,
				  OffsetNumber *offsetptr,
				  int keysz,
				  ScanKey scankey,
				  IndexTuple newtup,
				  BTStack stack,
				  Relation heapRel);
static void _bt_insertonpg(Relation rel, Buffer buf, Buffer cbuf,
			   BTStack stack,
			   IndexTuple itup,
			   OffsetNumber newitemoff,
			   bool split_only_page);
static Buffer _bt_split(Relation rel, Buffer buf, Buffer cbuf,
		  OffsetNumber firstright, OffsetNumber newitemoff, Size newitemsz,
		  IndexTuple newitem, bool newitemonleft);
static void _bt_insert_parent(Relation rel, Buffer buf, Buffer rbuf,
				  BTStack stack, bool is_root, bool is_only);
static OffsetNumber _bt_findsplitloc(Relation rel, Page page,
				 SplitMode mode, OffsetNumber newitemoff,
				 Size newitemsz, IndexTuple newitem, bool *newitemonleft);
static int _bt_checksplitloc(FindSplitData *state,
				  OffsetNumber firstoldonright, bool newitemonleft,
				  int dataitemstoleft, Size firstoldonrightsz);
static int _bt_perfect_firstdiff(Relation rel, Page page,
					  OffsetNumber newitemoff, IndexTuple newitem,
					  int nsplits, SplitPoint *splits, SplitMode *secondmode);
static int _bt_split_firstdiff(Relation rel, Page page, OffsetNumber newitemoff,
					IndexTuple newitem, SplitPoint *split);
static int _bt_tuple_firstdiff(Relation rel, IndexTuple lastleft,
					IndexTuple firstright, bool *identical);
static bool _bt_pgaddtup(Page page, Size itemsize, IndexTuple itup,
			 OffsetNumber itup_off);
static bool _bt_isequal(TupleDesc itupdesc, Page page, OffsetNumber offnum,
			int keysz, ScanKey scankey);
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
	int			indnkeyatts;
	ScanKey		itup_scankey;
	ItemPointer itup_scantid;
	BTStack		stack = NULL;
	Buffer		buf;
	OffsetNumber offset;
	Page		page;
	BTPageOpaque lpageop;
	bool		fastpath;

	indnkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
	Assert(indnkeyatts != 0);

	/* we need an insertion scan key to do our search, so build one */
	itup_scankey = _bt_mkscankey(rel, itup);
	/* we use a heap TID with scan key if this isn't unique case */
	itup_scantid = (checkUnique == UNIQUE_CHECK_NO ? &itup->t_tid : NULL);

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
top:
	fastpath = false;
	offset = InvalidOffsetNumber;
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
			 * key is strictly greater than the first key on the page.
			 */
			if (P_ISLEAF(lpageop) && P_RIGHTMOST(lpageop) &&
				!P_IGNORE(lpageop) &&
				(PageGetFreeSpace(page) > itemsz) &&
				PageGetMaxOffsetNumber(page) >= P_FIRSTDATAKEY(lpageop) &&
				_bt_compare(rel, indnkeyatts, itup_scankey, itup_scantid,
							page, P_FIRSTDATAKEY(lpageop)) > 0)
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
		stack = _bt_search(rel, indnkeyatts, itup_scankey, itup_scantid, false,
						   &buf, BT_WRITE, NULL);
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

		page = BufferGetPage(buf);
		lpageop = (BTPageOpaque) PageGetSpecialPointer(page);
		Assert(itup_scantid == NULL);
		offset = _bt_binsrch(rel, buf, indnkeyatts, itup_scankey, NULL,
							 P_FIRSTDATAKEY(lpageop), false);
		xwait = _bt_check_unique(rel, itup, heapRel, buf, offset, itup_scankey,
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
	}

	if (checkUnique != UNIQUE_CHECK_EXISTING)
	{
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
		_bt_findinsertloc(rel, &buf, &offset, indnkeyatts, itup_scankey, itup,
						  stack, heapRel);
		_bt_insertonpg(rel, buf, InvalidBuffer, stack, itup, offset, false);
	}
	else
	{
		/* just release the buffer */
		_bt_relbuf(rel, buf);
	}

	/* be tidy */
	if (stack)
		_bt_freestack(stack);
	_bt_freeskey(itup_scankey);

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
_bt_check_unique(Relation rel, IndexTuple itup, Relation heapRel,
				 Buffer buf, OffsetNumber offset, ScanKey itup_scankey,
				 IndexUniqueCheck checkUnique, bool *is_unique,
				 uint32 *speculativeToken)
{
	TupleDesc	itupdesc = RelationGetDescr(rel);
	int			indnkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
	SnapshotData SnapshotDirty;
	OffsetNumber maxoff;
	Page		page;
	BTPageOpaque opaque;
	Buffer		nbuf = InvalidBuffer;
	bool		found = false;

	/* Assume unique until we find a duplicate */
	*is_unique = true;

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
				if (!_bt_isequal(itupdesc, page, offset, indnkeyatts, itup_scankey))
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
			/* If scankey <= hikey we gotta check the next page too */
			if (P_RIGHTMOST(opaque))
				break;
			/* _bt_isequal()'s special NULL semantics not required here */
			if (_bt_compare(rel, indnkeyatts, itup_scankey, NULL, page, P_HIKEY) > 0)
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
 *	_bt_findinsertloc() -- Finds an insert location for a tuple
 *
 *		On entry, *bufptr and *offsetptr point to the first legal position
 *		where the new tuple could be inserted if we were to treat it as having
 *		no implicit heap TID; only callers that just called _bt_check_unique()
 *		provide this hint (all other callers should set *offsetptr to
 *		InvalidOffsetNumber).  The caller should hold an exclusive lock on
 *		*bufptr in all cases.  On exit, they both point to the chosen insert
 *		location in all cases.  If _bt_findinsertloc decides to move right, the
 *		lock and pin on the original page will be released, and the new page
 *		returned to the caller is exclusively locked instead.
 *
 *		This is also where opportunistic microvacuuming of LP_DEAD tuples
 *		occurs.
 *
 *		newtup is the new tuple we're inserting, and scankey is an insertion
 *		type scan key for it.  We take a "scantid" heap TID attribute value
 *		from newtup directly.
 */
static void
_bt_findinsertloc(Relation rel,
				  Buffer *bufptr,
				  OffsetNumber *offsetptr,
				  int keysz,
				  ScanKey scankey,
				  IndexTuple newtup,
				  BTStack stack,
				  Relation heapRel)
{
	Buffer		buf = *bufptr;
	Page		page = BufferGetPage(buf);
	Size		itemsz;
	BTPageOpaque lpageop;
	bool		hintinvalidated;
	OffsetNumber newitemoff;
	OffsetNumber lowitemoff;
	OffsetNumber firstlegaloff = *offsetptr;

	lpageop = (BTPageOpaque) PageGetSpecialPointer(page);

	itemsz = IndexTupleSize(newtup);
	itemsz = MAXALIGN(itemsz);	/* be safe, PageAddItem will do this but we
								 * need to be consistent */

	/*
	 * Check whether the item can fit on a btree page at all. (Eventually, we
	 * ought to try to apply TOAST methods if not.) We actually need to be
	 * able to fit three items on every page, so restrict any one item to 1/3
	 * the per-page available space. Note that at this point, itemsz doesn't
	 * include the ItemId.
	 *
	 * NOTE: if you change this, see also the similar code in _bt_buildadd().
	 */
	if (itemsz > BTMaxItemSize(page))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("index row size %zu exceeds maximum %zu for index \"%s\"",
						itemsz, BTMaxItemSize(page),
						RelationGetRelationName(rel)),
				 errhint("Values larger than 1/3 of a buffer page cannot be indexed.\n"
						 "Consider a function index of an MD5 hash of the value, "
						 "or use full text indexing."),
				 errtableconstraint(heapRel,
									RelationGetRelationName(rel))));

	/* firstlegaloff/offsetptr hint (if any) assumed valid initially */
	hintinvalidated = false;

	/*
	 * TODO: Restore the logic for finding a page to insert on in the event of
	 * many duplicates for pre-pg_upgrade indexes.  The whole search through
	 * pages of logical duplicates to determine where to insert seems like
	 * something that has little upside, but that doesn't make it okay to
	 * ignore the performance characteristics after pg_upgrade is run, but
	 * before a REINDEX can run to bump BTREE_VERSION.
	 */
	while (true)
	{
		Buffer		rbuf;
		BlockNumber rblkno;

		if (P_RIGHTMOST(lpageop) ||
			_bt_compare(rel, keysz, scankey, &newtup->t_tid, page, P_HIKEY) <= 0)
			break;

		/*
		 * step right to next non-dead page.  this is only needed for unique
		 * indexes, and pg_upgrade'd indexes that still use BTREE_VERSION 2 or
		 * 3, where heap TID isn't considered to be a part of the keyspace.
		 *
		 * must write-lock that page before releasing write lock on current
		 * page; else someone else's _bt_check_unique scan could fail to see
		 * our insertion.  write locks on intermediate dead pages won't do
		 * because we don't know when they will get de-linked from the tree.
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
		hintinvalidated = true;
	}

	Assert(P_ISLEAF(lpageop));

	/*
	 * Perform micro-vacuuming of the page we're about to insert tuple on to
	 * if it looks like it has LP_DEAD items.
	 */
	if (P_HAS_GARBAGE(lpageop) && PageGetFreeSpace(page) < itemsz)
	{
		_bt_vacuum_one_page(rel, buf, heapRel);

		hintinvalidated = true;
	}

	/*
	 * Consider using caller's hint to avoid repeated binary search effort.
	 *
	 * Note that the hint is only provided by callers that checked uniqueness.
	 * The hint is used as a lower bound for a new binary search, since
	 * caller's original binary search won't have specified a scan tid.
	 */
	if (firstlegaloff == InvalidOffsetNumber || hintinvalidated)
		lowitemoff = P_FIRSTDATAKEY(lpageop);
	else
	{
		Assert(firstlegaloff == _bt_binsrch(rel, buf, keysz, scankey, NULL,
											P_FIRSTDATAKEY(lpageop), false));
		lowitemoff = firstlegaloff;
	}

	newitemoff = _bt_binsrch(rel, buf, keysz, scankey, &newtup->t_tid,
							 lowitemoff, false);

	*bufptr = buf;
	*offsetptr = newitemoff;
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

	page = BufferGetPage(buf);
	lpageop = (BTPageOpaque) PageGetSpecialPointer(page);

	/* child buffer must be given iff inserting on an internal page */
	Assert(P_ISLEAF(lpageop) == !BufferIsValid(cbuf));
	/* tuple must have appropriate number of attributes */
	Assert(BTreeTupleGetNAtts(itup, rel) > 0);
	Assert(!P_ISLEAF(lpageop) ||
		   BTreeTupleGetNAtts(itup, rel) ==
		   IndexRelationGetNumberOfAttributes(rel));
	Assert(P_ISLEAF(lpageop) ||
		   BTreeTupleGetNAtts(itup, rel) <=
		   IndexRelationGetNumberOfKeyAttributes(rel));

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

		/* split the buffer into left and right halves */
		rbuf = _bt_split(rel, buf, cbuf, firstright,
						 newitemoff, itemsz, itup, newitemonleft);
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
			if (metad->btm_version < BTREE_VERSION)
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
 *		must be inserted along with the data from the old page.
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
		  bool newitemonleft)
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
	 * Truncate attributes of the high key item before inserting it on the
	 * left page.  This can only happen at the leaf level, since in general
	 * all pivot tuple values originate from leaf level high keys.  This isn't
	 * just about avoiding unnecessary work, though; truncating unneeded key
	 * suffix attributes can only be performed at the leaf level anyway.  This
	 * is because a pivot tuple in a grandparent page must guide a search not
	 * only to the correct parent page, but also to the correct leaf page.
	 *
	 * Note that non-key (INCLUDE) attributes are always truncated away here.
	 * Additional key attributes are truncated away when they're not required
	 * to correctly separate the key space.
	 */
	if (isleaf)
	{
		OffsetNumber lastleftoff;
		IndexTuple	lastleft;

		/*
		 * Determine which tuple is on the left side of the split point, and
		 * generate truncated copy of the right tuple.  Truncate as
		 * aggressively as possible without generating a high key for the left
		 * side of the split (and later downlink for the right side) that
		 * fails to distinguish each side.  The new high key needs to be
		 * strictly less than all tuples on the right side of the split, but
		 * can be equal to items on the left side of the split.
		 *
		 * Handle the case where the incoming tuple is about to become the
		 * last item on the left side of the split.
		 */
		if (newitemonleft && newitemoff == firstright)
			lastleft = newitem;
		else
		{
			lastleftoff = OffsetNumberPrev(firstright);
			itemid = PageGetItemId(origpage, lastleftoff);
			lastleft = (IndexTuple) PageGetItem(origpage, itemid);
		}

		Assert(lastleft != item);
		lefthikey = _bt_suffix_truncate(rel, lastleft, item);
		itemsz = IndexTupleSize(lefthikey);
		itemsz = MAXALIGN(itemsz);
#ifdef DEBUG_SPLITS
		if (IsNormalProcessingMode() && !IsSystemRelation(rel))
		{
			TupleDesc	itupdesc = RelationGetDescr(rel);
			Datum		values[INDEX_MAX_KEYS];
			bool		isnull[INDEX_MAX_KEYS];
			char	   *lastleftstr;
			char	   *firstrightstr;
			char	   *newstr;

			index_deform_tuple(lastleft, itupdesc, values, isnull);
			lastleftstr = BuildIndexValueDescription(rel, values, isnull);
			index_deform_tuple(item, itupdesc, values, isnull);
			firstrightstr = BuildIndexValueDescription(rel, values, isnull);
			index_deform_tuple(newitem, itupdesc, values, isnull);
			newstr = BuildIndexValueDescription(rel, values, isnull);

			elog(LOG, "\"%s\" leaf block %u "
				 "last left %s first right %s "
				 "attributes truncated: %u from %u%s new item %s",
				 RelationGetRelationName(rel), BufferGetBlockNumber(buf),
				 lastleftstr, firstrightstr,
				 IndexRelationGetNumberOfKeyAttributes(rel) - BTreeTupleGetNAtts(lefthikey, rel),
				 IndexRelationGetNumberOfKeyAttributes(rel),
				 BTreeTupleGetHeapTID(lefthikey) != NULL ? " (heap TID added back)" : "",
				 newstr);
		}
#endif
	}
	else
	{
		lefthikey = item;
#ifdef DEBUG_SPLITS
		if (IsNormalProcessingMode() && !IsSystemRelation(rel))
		{
			TupleDesc	itupdesc = RelationGetDescr(rel);
			Datum		values[INDEX_MAX_KEYS];
			bool		isnull[INDEX_MAX_KEYS];
			char	   *newhighkey;
			char	   *newstr;

			index_deform_tuple(lefthikey, itupdesc, values, isnull);
			newhighkey = BuildIndexValueDescription(rel, values, isnull);
			index_deform_tuple(newitem, itupdesc, values, isnull);
			newstr = BuildIndexValueDescription(rel, values, isnull);

			elog(LOG, "\"%s\" internal block %u "
				 "new high key %s "
				 "attributes truncated: %u from %u%s new item %s",
				 RelationGetRelationName(rel), BufferGetBlockNumber(buf),
				 newhighkey,
				 IndexRelationGetNumberOfKeyAttributes(rel) - BTreeTupleGetNAtts(lefthikey, rel),
				 IndexRelationGetNumberOfKeyAttributes(rel),
				 BTreeTupleGetHeapTID(lefthikey) != NULL ? " (heap TID added back)" : "",
				 newstr);
		}
#endif
	}

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
 * away.  Generally speaking, only candidate split points that fall within
 * an acceptable space utilization range are considered.  This is even
 * useful with pages that only have a single (non-TID) attribute, since it's
 * important to avoid appending an explicit heap TID attribute to the new
 * pivot tuple (high key/downlink) when it cannot actually be truncated.
 * Avoiding appending a heap TID can be thought of as a "logical" suffix
 * truncation that "removes" the final attribute in the new high key for the
 * new left page.
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
 * Many duplicates mode may lead to slightly inferior space utilization when
 * values are spaced apart at fixed intervals, even on levels above the leaf
 * level.  Even when that happens, many duplicates mode will probably still
 * beat the generic default strategy.  Not having groups of duplicates
 * straddle two leaf pages is likely to more than make up for having sparser
 * pages, since "false sharing" of leaf blocks by index scans is avoided.  A
 * point lookup will only visit one leaf page, not two. (This kind of false
 * sharing may also have negative implications for page deletion during
 * vacuuming, and may artificially increase the number of pages subsequently
 * dirtied.)
 *
 * If the page is the rightmost page on its level, we instead try to arrange
 * to leave the left split page fillfactor% full.  In this way, when we are
 * inserting successively increasing keys (consider sequences, timestamps,
 * etc) we will end up with a tree whose pages are about fillfactor% full,
 * instead of the 50% full result that we'd get without this special case.
 * This is the same as nbtsort.c produces for a newly-created tree.  Note
 * that leaf and nonleaf pages use different fillfactors.
 *
 * If called recursively in single value mode, we also try to arrange to
 * leave the left split page fillfactor% full, though we arrange to use a
 * fillfactor that leaves the left page mostly empty and the right page
 * mostly full, rather than the other way around.  This greatly helps with
 * space management in cases where tuples with the same attribute values
 * span multiple pages.  Newly inserted duplicates will tend to have higher
 * heap TID values, so we'll end up splitting the same page again and again
 * as even more duplicates are inserted.  (The heap TID attribute has
 * descending sort order, so ascending heap TID values continually split the
 * same low page).
 *
 * We are passed the intended insert position of the new tuple, expressed as
 * the offsetnumber of the tuple it must go in front of.  (This could be
 * maxoff+1 if the tuple is to go at the end.)
 *
 * We return the index of the first existing tuple that should go on the
 * righthand page, plus a boolean indicating whether the new tuple goes on
 * the left or right page.  The bool is necessary to disambiguate the case
 * where firstright == newitemoff.
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
				goodenough,
				olddataitemstotal,
				olddataitemstoleft,
				perfectfirstdiff,
				bestfirstdiff,
				lowsplit;
	bool		goodenoughfound;
	SplitPoint	splits[STACK_SPLIT_POINTS];
	SplitMode	secondmode;
	OffsetNumber finalfirstright;

	opaque = (BTPageOpaque) PageGetSpecialPointer(page);
	maxoff = PageGetMaxOffsetNumber(page);

	/* Total free space available on a btree page, after fixed overhead */
	leftspace = rightspace =
		PageGetPageSize(page) - SizeOfPageHeaderData -
		MAXALIGN(sizeof(BTPageOpaqueData));

	/*
	 * Conservatively assume that suffix truncation cannot avoid adding a heap
	 * TID to the left half's new high key when splitting at the leaf level.
	 * Accounting for the size of the rest of the high key comes later,  since
	 * it's considered for every candidate split point.
	 */
	if (P_ISLEAF(opaque))
		leftspace -= MAXALIGN(sizeof(ItemPointerData));

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
	state.newitemsz = newitemsz + sizeof(ItemIdData);
	state.is_leaf = P_ISLEAF(opaque);
	state.is_weighted = P_RIGHTMOST(opaque) || mode == SPLIT_SINGLE_VALUE;
	if (state.is_leaf)
	{
		if (mode != SPLIT_SINGLE_VALUE)
			state.fillfactor = RelationGetFillFactor(rel,
													 BTREE_DEFAULT_FILLFACTOR);
		else
			state.fillfactor = BTREE_SINGLEVAL_FILLFACTOR;

		if (mode == SPLIT_DEFAULT)
			state.maxsplit = Min(Max(3, maxoff / 16), STACK_SPLIT_POINTS);
		else if (mode == SPLIT_MANY_DUPLICATES)
			state.maxsplit = maxoff;
		else
			state.maxsplit = 1;
	}
	else
	{
		Assert(mode == SPLIT_DEFAULT);

		state.fillfactor = BTREE_NONLEAF_FILLFACTOR;
		state.maxsplit = 1;
	}
	state.nsplits = 0;
	if (mode != SPLIT_MANY_DUPLICATES)
		state.splits = splits;
	else
		state.splits = palloc(sizeof(SplitPoint) * maxoff);
	state.leftspace = leftspace;
	state.rightspace = rightspace;
	state.olddataitemstotal = olddataitemstotal;
	state.newitemoff = newitemoff;

	/*
	 * Finding the best possible split would require checking all the possible
	 * split points, because of the high-key and left-key special cases.
	 * That's probably more work than it's worth in default mode; instead,
	 * stop as soon as we find all "good-enough" splits, where good-enough is
	 * defined as an imbalance in free space of no more than pagesize/16
	 * (arbitrary...) This should let us stop near the middle on most pages,
	 * instead of plowing to the end.  Many duplicates mode does consider
	 * all choices, while single value mode gives up as soon as it finds a
	 * good enough split point.
	 */
	goodenough = leftspace / 16;

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
			_bt_checksplitloc(&state, offnum, true,
							  olddataitemstoleft, itemsz);

			delta = _bt_checksplitloc(&state, offnum, false,
									  olddataitemstoleft, itemsz);
		}

		/*
		 * Abort default mode scan once we've found a good-enough choice, and
		 * reach the point where we stop finding new good-enough choices.
		 */
		if (state.nsplits > 0 && state.splits[0].delta <= goodenough)
			goodenoughfound = true;

		if (mode == SPLIT_DEFAULT && goodenoughfound && delta > goodenough)
			break;

		/*
		 * Single value mode does not expect to be able to truncate; might as
		 * well give up quickly once good enough value found.
		 */
		if (mode == SPLIT_SINGLE_VALUE && goodenoughfound)
			break;

		olddataitemstoleft += itemsz;
	}

	/*
	 * If the new item goes as the last item, check for splitting so that all
	 * the old items go to the left page and the new item goes to the right
	 * page.
	 */
	if (newitemoff > maxoff && !goodenoughfound)
		_bt_checksplitloc(&state, newitemoff, false, olddataitemstotal, 0);

	/*
	 * I believe it is not possible to fail to find a feasible split, but just
	 * in case ...
	 */
	if (state.nsplits == 0)
		elog(ERROR, "could not find a feasible split point for index \"%s\"",
			 RelationGetRelationName(rel));

	/*
	 * Search among acceptable split points for the entry with earliest
	 * enclosing tuple pair differing attribute values.  The general idea is
	 * to maximize the effectiveness of suffix truncation without affecting
	 * the balance of space on each side of the split very much.
	 *
	 * First find lowest possible first differing attribute among array of
	 * acceptable split points -- the "perfect" firstdiff.  This allows us to
	 * return early without wasting cycles on calculating the first differing
	 * attribute for all candidate splits when that clearly cannot improve our
	 * choice.  This optimization is important for several common cases,
	 * including insertion into a primary key index on an auto-incremented or
	 * monotonically increasing integer column.
	 *
	 * This is also the point at which we decide to either finish splitting
	 * the page using the default strategy, or, alternatively, to do a second
	 * pass over page using a different strategy.  The second pass may be in
	 * many duplicates mode, or in single value mode.
	 */
	perfectfirstdiff = 0;
	secondmode = SPLIT_DEFAULT;
	if (state.is_leaf && mode == SPLIT_DEFAULT)
		perfectfirstdiff = _bt_perfect_firstdiff(rel, page, newitemoff, newitem,
												 state.nsplits, state.splits,
												 &secondmode);

	/* newitemonleft output parameter is set recursively */
	if (secondmode != SPLIT_DEFAULT)
		return _bt_findsplitloc(rel, page, secondmode, newitemoff, newitemsz,
								newitem, newitemonleft);

	/*
	 * Now actually search among acceptable split points for the entry that
	 * allows suffix truncation to truncate away the maximum possible number
	 * of attributes.
	 */
	bestfirstdiff = INT_MAX;
	lowsplit = 0;
	for (int i = 0; i < state.nsplits; i++)
	{
		int			firstdiff;

		/* Don't waste cycles */
		if (perfectfirstdiff == INT_MAX || state.nsplits == 1)
			break;

		firstdiff = _bt_split_firstdiff(rel, page, newitemoff, newitem,
										state.splits + i);

		if (firstdiff <= perfectfirstdiff)
		{
#ifdef DEBUG_SPLITS
			elog(LOG, "\"%s\" perfect firstdiff %d returned at point %d out of %d",
				 RelationGetRelationName(rel), perfectfirstdiff, i,
				 state.nsplits);
#endif
			bestfirstdiff = firstdiff;
			lowsplit = i;
			break;
		}

		if (firstdiff < bestfirstdiff)
		{
			bestfirstdiff = firstdiff;
			lowsplit = i;
		}
	}

	*newitemonleft = state.splits[lowsplit].newitemonleft;
	finalfirstright = state.splits[lowsplit].firstright;
	/* Be tidy */
	if (state.splits != splits)
		pfree(state.splits);

#ifdef DEBUG_SPLITS
	elog(LOG, "\"%s\" split location %d out of %u",
		 RelationGetRelationName(rel), finalfirstright, maxoff);
#endif
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
				rightfree;
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
	 * therefore it counts against left space as well as right space. When
	 * index has included attributes, then those attributes of left page high
	 * key will be truncated leaving that page with slightly more free space.
	 * However, that shouldn't affect our ability to find valid split
	 * location, since we err in the direction of being pessimistic about free
	 * space on the left half.
	 *
	 * Note that we've already conservatively subtracted away overhead
	 * required for the left/new high key to have an explicit head TID, on the
	 * assumption that that cannot be avoided by suffix truncation.  (Leaf
	 * pages only.)
	 */
	leftfree -= firstrightitemsz;

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
	if (leftfree >= 0 && rightfree >= 0)
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
				- ((100 - state->fillfactor) * rightfree);
		}
		else
		{
			/* Otherwise, aim for equal free space on both sides */
			delta = leftfree - rightfree;
		}

		if (delta < 0)
			delta = -delta;
		if (state->nsplits < state->maxsplit ||
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
			if (state->nsplits < state->maxsplit)
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
 * Subroutine to find the earliest possible attribute that differs for any
 * entry within array of acceptable candidate split points.
 *
 * This may be earlier than any real firstdiff for any of the candidate split
 * points, in which case the optimization is ineffective.
 */
static int
_bt_perfect_firstdiff(Relation rel, Page page, OffsetNumber newitemoff,
					  IndexTuple newitem, int nsplits, SplitPoint *splits,
					  SplitMode *secondmode)
{
	ItemId		itemid;
	OffsetNumber center;
	IndexTuple	leftmost,
				rightmost;
	int			perfectfirstdiff;
	bool		identical;

	/* Assume that a second pass over page won't be required for now */
	*secondmode = SPLIT_DEFAULT;

	/*
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
	center = splits[0].firstright;

	/*
	 * Split points can be thought of as points _between_ tuples on the
	 * original unsplit page image, at least if you pretend that the incoming
	 * tuple is already on the page to be split (imagine that the original
	 * unsplit page actually had enough space to fit the incoming tuple).  The
	 * rightmost tuple is the tuple that is immediately to the right of a
	 * split point that is itself rightmost.  Likewise, the leftmost tuple is
	 * the tuple to the left of the leftmost split point.  This is slightly
	 * arbitrary.
	 *
	 * When there are very few candidates, no sensible comparison can be made
	 * here, resulting in caller selecting lowest delta/the center split point
	 * by default.  No great care is taken around boundary cases where the
	 * center split point has the same firstright offset as either the
	 * leftmost or rightmost split points (i.e. only newitemonleft differs).
	 * We expect to find leftmost and rightmost tuples almost immediately.
	 */
	perfectfirstdiff = INT_MAX;
	identical = false;
	for (int j = nsplits - 1; j > 1; j--)
	{
		SplitPoint *split = splits + j;

		if (!leftmost && split->firstright < center)
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

		if (!rightmost && split->firstright > center)
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
			perfectfirstdiff = _bt_tuple_firstdiff(rel, leftmost, rightmost,
												   &identical);
			break;
		}
	}

	/* Work out which type of second pass will be performed, if any */
	if (identical)
	{
		BTPageOpaque opaque;
		OffsetNumber maxoff;

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
		(void) _bt_tuple_firstdiff(rel, leftmost, rightmost, &identical);

		/*
		 * If page has many duplicates but is not entirely full of
		 * duplicates, a many duplicates mode pass will be performed.  If
		 * page is entirely full of duplicates, a single value mode pass
		 * will be performed.
		 *
		 * Caller should avoid a single value mode pass when incoming tuple
		 * doesn't sort lowest among items on the page, though.  Instead, we
		 * instruct caller to continue with original default mode split,
		 * since an out-of-order new item suggests that newer tuples have
		 * come from (non-HOT) updates, not inserts.  Evenly sharing space
		 * among each half of the split avoids pathological performance.
		 */
		if (identical)
		{
#ifndef BTREE_ASC_HEAP_TID
			if (P_FIRSTDATAKEY(opaque) == newitemoff)
#else
			if (maxoff < newitemoff)
#endif
				*secondmode = SPLIT_SINGLE_VALUE;
			else
			{
				perfectfirstdiff = INT_MAX;
				*secondmode = SPLIT_DEFAULT;
			}
		}
		else
			*secondmode = SPLIT_MANY_DUPLICATES;
	}

	return perfectfirstdiff;
}

/*
 * Subroutine to find first attribute that differs among the two tuples that
 * enclose caller's candidate split point.
 */
static int
_bt_split_firstdiff(Relation rel, Page page, OffsetNumber newitemoff,
					IndexTuple newitem, SplitPoint *split)
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

	Assert(lastleft != firstright);
	return _bt_tuple_firstdiff(rel, lastleft, firstright, NULL);
}

/*
 * Subroutine to find first attribute that differs between two tuples,
 * typically two tuples that enclose a candidate split point.  Caller may also
 * be interested in whether or not tuples are completely identical, in which
 * case an "identical" parameter is passed.
 *
 * A naive bitwise approach to datum comparisons is used to save cycles.  This
 * is inherently approximate, but works just as well as real scan key
 * comparisons in most cases, since the vast majority of types in Postgres
 * cannot be equal unless they're bitwise equal.
 *
 * Testing has shown that an approach involving treating the tuple as a
 * decomposed binary string would work almost as well as our current approach.
 * It would also be faster.  It might actually be necessary to go that way in
 * the future, if suffix truncation is made sophisticated enough to truncate
 * at a finer granularity (i.e. truncate within an attribute, rather than just
 * truncating away whole attributes).  The current approach isn't markedly
 * slower, since it works particularly well with the "perfectfirstdiff"
 * optimization (there are fewer, more expensive calls here).  It also works
 * with INCLUDE indexes (indexes with non-key attributes) without any special
 * effort.
 */
static int
_bt_tuple_firstdiff(Relation rel, IndexTuple lastleft, IndexTuple firstright,
					bool *identical)
{
	TupleDesc	itupdesc = RelationGetDescr(rel);
	int			keysz = IndexRelationGetNumberOfKeyAttributes(rel);
	int			result;

	result = 0;
	for (int attnum = 1; attnum <= keysz; attnum++)
	{
		Datum		datum1,
					datum2;
		bool		isNull1,
					isNull2;
		Form_pg_attribute att;

		datum1 = index_getattr(lastleft, attnum, itupdesc, &isNull1);
		datum2 = index_getattr(firstright, attnum, itupdesc, &isNull2);
		att = TupleDescAttr(itupdesc, attnum - 1);

		if (isNull1 != isNull2)
			break;

		if (!isNull1 &&
			!datumIsEqual(datum1, datum2, att->attbyval, att->attlen))
			break;

		result++;
	}

	/* Report if left and right tuples are identical when requested */
	if (identical)
	{
		if (result >= keysz)
			*identical = true;
		else
			*identical = false;
	}

	return result;
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

		/* get high key from left page == lower bound for new right page */
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
	if (metad->btm_version < BTREE_VERSION)
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
_bt_isequal(TupleDesc itupdesc, Page page, OffsetNumber offnum,
			int keysz, ScanKey scankey)
{
	IndexTuple	itup;
	int			i;

	/* Better be comparing to a leaf item */
	Assert(P_ISLEAF((BTPageOpaque) PageGetSpecialPointer(page)));

	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offnum));

	for (i = 1; i <= keysz; i++)
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
