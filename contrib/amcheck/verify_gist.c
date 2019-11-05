/*-------------------------------------------------------------------------
 *
 * verify_gist.c
 *		Verifies the integrity of GiST indexes based on invariants.
 *
 * Verification checks that all paths in GiST graph contain
 * consistent keys: tuples on parent pages consistently include tuples
 * from children pages. Also, verification checks graph invariants:
 * internal page must have at least one downlinks, internal page can
 * reference either only leaf pages or only internal pages.
 *
 *
 * Copyright (c) 2017-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/amcheck/verify_gist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/gist_private.h"
#include "amcheck.h"
#include "catalog/pg_am.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * GistScanItem represents one item of depth-first scan of GiST index.
 */
typedef struct GistScanItem
{
	int			depth;
	IndexTuple	parenttup;
	BlockNumber parentblk;
	XLogRecPtr	parentlsn;
	BlockNumber blkno;
	struct GistScanItem *next;
} GistScanItem;

PG_FUNCTION_INFO_V1(gist_index_parent_check);

static void gist_index_checkable(Relation rel);
static void gist_check_parent_keys_consistency(Relation rel);
static void check_index_page(Relation rel, Buffer buffer, BlockNumber blockNo);
static IndexTuple gist_refind_parent(Relation rel, BlockNumber parentblkno,
									 BlockNumber childblkno,
									 BufferAccessStrategy strategy);

/*
 * gist_index_parent_check(index regclass)
 *
 * Verify integrity of GiST index.
 *
 * Acquires AccessShareLock on heap & index relations.
 */
Datum gist_index_parent_check(PG_FUNCTION_ARGS)
{
	Oid			indrelid = PG_GETARG_OID(0);
	Relation	indrel;
	Relation	heaprel;
	LOCKMODE	lockmode = AccessShareLock;

	/* lock table and index with neccesary level */
	amcheck_lock_relation(indrelid, &indrel, &heaprel, lockmode);

	/* verify that this is GiST eligible for check */
	gist_index_checkable(indrel);

	if (amcheck_index_mainfork_expected(indrel))
		gist_check_parent_keys_consistency(indrel);

	/* Unlock index and table */
	amcheck_unlock_relation(indrelid, indrel, heaprel, lockmode);

	PG_RETURN_VOID();
}

/*
 * Check that relation is eligible for GiST verification
 */
static void
gist_index_checkable(Relation rel)
{
	if (rel->rd_rel->relkind != RELKIND_INDEX ||
		rel->rd_rel->relam != GIST_AM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only GiST indexes are supported as targets for this verification"),
				 errdetail("Relation \"%s\" is not a GiST index.",
						   RelationGetRelationName(rel))));

	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions"),
				 errdetail("Index \"%s\" is associated with temporary relation.",
						   RelationGetRelationName(rel))));

	if (!rel->rd_index->indisvalid)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot check index \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail("Index is not valid")));
}

/*
 * Main entry point for GiST check. Allocates memory context and scans through
 * GiST graph.  This function verifies that tuples of internal pages cover all
 * the key space of each tuple on leaf page.  To do this we invoke
 * gist_check_internal_page() for every internal page.
 *
 * gist_check_internal_page() in it's turn takes every tuple and tries to
 * adjust it by tuples on referenced child page.  Parent gist tuple should
 * never require any adjustments.
 */
static void
gist_check_parent_keys_consistency(Relation rel)
{
	BufferAccessStrategy strategy = GetAccessStrategy(BAS_BULKREAD);
	GistScanItem *stack;
	MemoryContext mctx;
	MemoryContext oldcontext;
	GISTSTATE  *state;
	int			leafdepth;

	mctx = AllocSetContextCreate(CurrentMemoryContext,
								 "amcheck context",
								 ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(mctx);

	state = initGISTstate(rel);

	/*
	 * We don't know the height of the tree yet, but as soon as we encounter a
	 * leaf page, we will set 'leafdepth' to its depth.
	 */
	leafdepth = -1;

	/* Start the scan at the root page */
	stack = (GistScanItem *) palloc0(sizeof(GistScanItem));
	stack->depth = 0;
	stack->parenttup = NULL;
	stack->parentblk = InvalidBlockNumber;
	stack->parentlsn = InvalidXLogRecPtr;
	stack->blkno = GIST_ROOT_BLKNO;

	while (stack)
	{
		GistScanItem *stack_next;
		Buffer		buffer;
		Page		page;
		OffsetNumber  i, maxoff;
		XLogRecPtr	lsn;

		CHECK_FOR_INTERRUPTS();

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, stack->blkno,
									RBM_NORMAL, strategy);
		LockBuffer(buffer, GIST_SHARE);
		page = (Page) BufferGetPage(buffer);
		lsn = BufferGetLSNAtomic(buffer);

		/* Do basic sanity checks on the page headers */
		check_index_page(rel, buffer, stack->blkno);

		/*
		 * It's possible that the page was split since we looked at the
		 * parent, so that we didn't missed the downlink of the right sibling
		 * when we scanned the parent.  If so, add the right sibling to the
		 * stack now.
		 */
		if (GistFollowRight(page) || stack->parentlsn < GistPageGetNSN(page))
		{
			/* split page detected, install right link to the stack */
			GistScanItem *ptr = (GistScanItem *) palloc(sizeof(GistScanItem));

			ptr->depth = stack->depth;
			ptr->parenttup = CopyIndexTuple(stack->parenttup);
			ptr->parentblk = stack->parentblk;
			ptr->parentlsn = stack->parentlsn;
			ptr->blkno = GistPageGetOpaque(page)->rightlink;
			ptr->next = stack->next;
			stack->next = ptr;
		}

		/* Check that the tree has the same height in all branches */
		if (GistPageIsLeaf(page))
		{
			if (leafdepth == -1)
				leafdepth = stack->depth;
			else if (stack->depth != leafdepth)
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("index \"%s\": internal pages traversal encountered leaf page unexpectedly on block %u",
								RelationGetRelationName(rel), stack->blkno)));
		}

		/*
		 * Check that each tuple looks valid, and is consistent with the
		 * downlink we followed when we stepped on this page.
		 */
		maxoff = PageGetMaxOffsetNumber(page);
		for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
		{
			ItemId iid = PageGetItemIdCareful(rel, stack->blkno, page, i, sizeof(GISTPageOpaqueData));
			IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);

			/*
			 * Check that it's not a leftover invalid tuple from pre-9.1 See
			 * also gistdoinsert() and gistbulkdelete() handling of such
			 * tuples. We do consider it error here.
			 */
			if (GistTupleIsInvalid(idxtuple))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("index \"%s\" contains an inner tuple marked as invalid, block %u, offset %u",
								RelationGetRelationName(rel), stack->blkno, i),
						 errdetail("This is caused by an incomplete page split at crash recovery before upgrading to PostgreSQL 9.1."),
						 errhint("Please REINDEX it.")));

			if (MAXALIGN(ItemIdGetLength(iid)) != MAXALIGN(IndexTupleSize(idxtuple)))
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("index \"%s\" has inconsistent tuple sizes, block %u, offset %u",
								RelationGetRelationName(rel), stack->blkno, i)));

			/*
			 * Check if this tuple is consistent with the downlink in the
			 * parent.
			 */
			if (stack->parenttup &&
				gistgetadjusted(rel, stack->parenttup, idxtuple, state))
			{
				/*
				 * There was a discrepancy between parent and child tuples.
				 * We need to verify it is not a result of concurrent call of
				 * gistplacetopage(). So, lock parent and try to find downlink
				 * for current page. It may be missing due to concurrent page
				 * split, this is OK.
				 */
				pfree(stack->parenttup);
				stack->parenttup = gist_refind_parent(rel, stack->parentblk,
													  stack->blkno, strategy);

				/* We found it - make a final check before failing */
				if (!stack->parenttup)
					elog(NOTICE, "Unable to find parent tuple for block %u on block %u due to concurrent split",
						 stack->blkno, stack->parentblk);
				else if (gistgetadjusted(rel, stack->parenttup, idxtuple, state))
					ereport(ERROR,
							(errcode(ERRCODE_INDEX_CORRUPTED),
							 errmsg("index \"%s\" has inconsistent records on page %u offset %u",
									RelationGetRelationName(rel), stack->blkno, i)));
				else
				{
					/*
					 * But now it is properly adjusted - nothing to do here.
					 */
				}
			}

			/* If this is an internal page, recurse into the child */
			if (!GistPageIsLeaf(page))
			{
				GistScanItem *ptr;

				ptr = (GistScanItem *) palloc(sizeof(GistScanItem));
				ptr->depth = stack->depth + 1;
				ptr->parenttup = CopyIndexTuple(idxtuple);
				ptr->parentblk = stack->blkno;
				ptr->blkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));
				ptr->parentlsn = lsn;
				ptr->next = stack->next;
				stack->next = ptr;
			}
		}

		LockBuffer(buffer, GIST_UNLOCK);
		ReleaseBuffer(buffer);

		/* Step to next item in the queue */
		stack_next = stack->next;
		if (stack->parenttup)
			pfree(stack->parenttup);
		pfree(stack);
		stack = stack_next;
	}

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(mctx);
}

static void
check_index_page(Relation rel, Buffer buffer, BlockNumber blockNo)
{
	Page		page = BufferGetPage(buffer);

	gistcheckpage(rel, buffer);

	if (GistPageGetOpaque(page)->gist_page_id != GIST_PAGE_ID)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("index \"%s\" has corrupted page %d",
						RelationGetRelationName(rel), blockNo)));

	if (GistPageIsDeleted(page))
	{
		if (!GistPageIsLeaf(page))
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("index \"%s\" has deleted internal page %d",
							RelationGetRelationName(rel), blockNo)));
		if (PageGetMaxOffsetNumber(page) > InvalidOffsetNumber)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("index \"%s\" has deleted page %d with tuples",
							RelationGetRelationName(rel), blockNo)));
	}
	else if (PageGetMaxOffsetNumber(page) > MaxIndexTuplesPerPage)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("index \"%s\" has page %d with exceeding count of tuples",
						RelationGetRelationName(rel), blockNo)));
}

/*
 * Try to re-find downlink pointing to 'blkno', in 'parentblkno'.
 *
 * If found, returns a palloc'd copy of the downlink tuple. Otherwise,
 * returns NULL.
 */
static IndexTuple
gist_refind_parent(Relation rel, BlockNumber parentblkno,
				   BlockNumber childblkno, BufferAccessStrategy strategy)
{
	Buffer		parentbuf;
	Page		parentpage;
	OffsetNumber o,
				parent_maxoff;
	IndexTuple	result = NULL;

	parentbuf = ReadBufferExtended(rel, MAIN_FORKNUM, parentblkno, RBM_NORMAL,
								   strategy);

	LockBuffer(parentbuf, GIST_SHARE);
	parentpage = BufferGetPage(parentbuf);

	if (GistPageIsLeaf(parentpage))
	{
		UnlockReleaseBuffer(parentbuf);
		return result;
	}

	parent_maxoff = PageGetMaxOffsetNumber(parentpage);
	for (o = FirstOffsetNumber; o <= parent_maxoff; o = OffsetNumberNext(o))
	{
		ItemId p_iid = PageGetItemIdCareful(rel, parentblkno, parentpage, o, sizeof(GISTPageOpaqueData));
		IndexTuple	itup = (IndexTuple) PageGetItem(parentpage, p_iid);

		if (ItemPointerGetBlockNumber(&(itup->t_tid)) == childblkno)
		{
			/* Found it! Make copy and return it */
			result = CopyIndexTuple(itup);
			break;
		}
	}

	UnlockReleaseBuffer(parentbuf);

	return result;
}
