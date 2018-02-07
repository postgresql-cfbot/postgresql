/*-------------------------------------------------------------------------
 *
 * gistvacuum.c
 *	  vacuuming routines for the postgres GiST index access method.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/gist/gistvacuum.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/gist_private.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "utils/snapmgr.h"
#include "access/xact.h"


/*
 * VACUUM cleanup: update FSM
 */
IndexBulkDeleteResult *
gistvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	Relation	rel = info->index;
	BlockNumber npages,
				blkno;
	BlockNumber totFreePages;
	bool		needLock;

	/* No-op in ANALYZE ONLY mode */
	if (info->analyze_only)
		return stats;

	/* Set up all-zero stats if gistbulkdelete wasn't called */
	if (stats == NULL)
	{
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
		/* use heap's tuple count */
		stats->num_index_tuples = info->num_heap_tuples;
		stats->estimated_count = info->estimated_count;

		/*
		 * XXX the above is wrong if index is partial.  Would it be OK to just
		 * return NULL, or is there work we must do below?
		 */
	}

	/*
	 * Need lock unless it's local to this backend.
	 */
	needLock = !RELATION_IS_LOCAL(rel);

	/* try to find deleted pages */
	if (needLock)
		LockRelationForExtension(rel, ExclusiveLock);
	npages = RelationGetNumberOfBlocks(rel);
	if (needLock)
		UnlockRelationForExtension(rel, ExclusiveLock);

	totFreePages = 0;
	for (blkno = GIST_ROOT_BLKNO + 1; blkno < npages; blkno++)
	{
		Buffer		buffer;
		Page		page;

		vacuum_delay_point();

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL,
									info->strategy);
		LockBuffer(buffer, GIST_SHARE);
		page = (Page) BufferGetPage(buffer);

		if (PageIsNew(page) || GistPageIsDeleted(page))
		{
			totFreePages++;
			RecordFreeIndexPage(rel, blkno);
		}
		UnlockReleaseBuffer(buffer);
	}

	/* Finally, vacuum the FSM */
	IndexFreeSpaceMapVacuum(info->index);

	/* return statistics */
	stats->pages_free = totFreePages;
	if (needLock)
		LockRelationForExtension(rel, ExclusiveLock);
	stats->num_pages = RelationGetNumberOfBlocks(rel);
	if (needLock)
		UnlockRelationForExtension(rel, ExclusiveLock);

	return stats;
}

typedef struct GistBDItem
{
	GistNSN		 parentlsn;
	BlockNumber  blkno;
	OffsetNumber parentoffset;
	struct GistBDItem *next;
} GistBDItem;

static void
pushStackIfSplited(Page page, GistBDItem *stack)
{
	GISTPageOpaque opaque = GistPageGetOpaque(page);

	if (stack->blkno != GIST_ROOT_BLKNO && !XLogRecPtrIsInvalid(stack->parentlsn) &&
		(GistFollowRight(page) || stack->parentlsn < GistPageGetNSN(page)) &&
		opaque->rightlink != InvalidBlockNumber /* sanity check */ )
	{
		/* split page detected, install right link to the stack */

		GistBDItem *ptr = (GistBDItem *) palloc(sizeof(GistBDItem));

		ptr->blkno = opaque->rightlink;
		ptr->parentlsn = stack->parentlsn;
		ptr->next = stack->next;
		stack->next = ptr;
	}
}

/*
 * During physical scan for every pair parent-child we can either find parent
 * first or child first. Every time we open internal page - we mark parent
 * block no for every child and set GIST_PS_HAS_PARENT. When scan will get to
 * child page, if this page turns out to be empty - we will get back by
 * parent link. If we find child first (still without parent link), we mark
 * the page as GIST_PS_EMPTY_LEAF if it is ready to be deleted. When we will
 * scan it's parent - we will pick it to rescan list.
 */
#define GIST_PS_HAS_PARENT 1
#define GIST_PS_EMPTY_LEAF 2


/* Physiscal scan item */
typedef struct GistPSItem
{
	BlockNumber  parent;
	List*        emptyLeafOffsets;
	OffsetNumber parentOffset;
	uint16_t     flags;
} GistPSItem;

/* Blocknumber of internal pages with offsets to rescan for deletion */
typedef struct GistRescanItem
{
	BlockNumber       blkno;
	List*             emptyLeafOffsets;
	struct GistRescanItem* next;
} GistRescanItem;

/* Read all pages sequentially populating array of GistPSItem */
static GistRescanItem*
gistbulkdeletephysicalcan(IndexVacuumInfo * info, IndexBulkDeleteResult * stats, IndexBulkDeleteCallback callback, void* callback_state, BlockNumber npages)
{
	Relation	     rel = info->index;
	GistRescanItem *result = NULL;
	BlockNumber      blkno;

	/* Here we will store whole graph of the index */
	GistPSItem *graph = palloc0(npages * sizeof(GistPSItem));


	for (blkno = GIST_ROOT_BLKNO; blkno < npages; blkno++)
	{
		Buffer		 buffer;
		Page		 page;
		OffsetNumber i,
					 maxoff;
		IndexTuple   idxtuple;
		ItemId	     iid;

		vacuum_delay_point();

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL,
									info->strategy);
		/*
		 * We are not going to stay here for a long time, calling recursive algorithms.
		 * Especially for an internal page. So, agressivly grab an exclusive lock.
		 */
		LockBuffer(buffer, GIST_EXCLUSIVE);
		page = (Page) BufferGetPage(buffer);

		if (PageIsNew(page) || GistPageIsDeleted(page))
		{
			UnlockReleaseBuffer(buffer);
			/* TODO: Should not we record free page here? */
			continue;
		}

		maxoff = PageGetMaxOffsetNumber(page);

		if (GistPageIsLeaf(page))
		{
			OffsetNumber todelete[MaxOffsetNumber];
			int			ntodelete = 0;

			/*
			 * Remove deletable tuples from page
			 */

			for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
			{
				iid = PageGetItemId(page, i);
				idxtuple = (IndexTuple) PageGetItem(page, iid);

				if (callback(&(idxtuple->t_tid), callback_state))
					todelete[ntodelete++] = i;
				else
					stats->num_index_tuples += 1;
			}

			stats->tuples_removed += ntodelete;

			/* We have dead tuples on the page */
			if (ntodelete)
			{
				START_CRIT_SECTION();

				MarkBufferDirty(buffer);

				PageIndexMultiDelete(page, todelete, ntodelete);
				GistMarkTuplesDeleted(page);

				if (RelationNeedsWAL(rel))
				{
					XLogRecPtr	recptr;

					recptr = gistXLogUpdate(buffer,
											todelete, ntodelete,
											NULL, 0, InvalidBuffer);
					PageSetLSN(page, recptr);
				}
				else
					PageSetLSN(page, gistGetFakeLSN(rel));

				END_CRIT_SECTION();
			}

			/* The page is completely empty */
			if (ntodelete == maxoff)
			{
				/* This page is a candidate to be deleted. Remember it's parent to rescan it later with xlock */
				if (graph[blkno].flags & GIST_PS_HAS_PARENT)
				{
					/* Go to parent and append myself */
					BlockNumber parentblockno = graph[blkno].parent;
					graph[parentblockno].emptyLeafOffsets = lappend_int(graph[parentblockno].emptyLeafOffsets, (int)graph[blkno].parentOffset);
				}
				else
				{
					/* Parent will collect me later */
					graph[blkno].flags |= GIST_PS_EMPTY_LEAF;
				}
			}
		}
		else
		{
			/* For internal pages we remember stucture of the tree */
			for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
			{
				BlockNumber childblkno;
				iid = PageGetItemId(page, i);
				idxtuple = (IndexTuple) PageGetItem(page, iid);
				childblkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));

				if (graph[childblkno].flags & GIST_PS_EMPTY_LEAF)
				{
					/* Child has been scanned earlier and is ready to be picked up */
					graph[blkno].emptyLeafOffsets = lappend_int(graph[blkno].emptyLeafOffsets, i);
				}
				else
				{
					/* Collect leaf when scan will come close */
					graph[childblkno].parent = blkno;
					graph[childblkno].parentOffset = i;
					graph[childblkno].flags |= GIST_PS_HAS_PARENT;
				}


				if (GistTupleIsInvalid(idxtuple))
					ereport(LOG,
							(errmsg("index \"%s\" contains an inner tuple marked as invalid",
									RelationGetRelationName(rel)),
							 errdetail("This is caused by an incomplete page split at crash recovery before upgrading to PostgreSQL 9.1."),
							 errhint("Please REINDEX it.")));
			}
		}
		UnlockReleaseBuffer(buffer);
	}

	/* Search for internal pages pointing to empty leafs */
	for (blkno = GIST_ROOT_BLKNO; blkno < npages; blkno++)
	{
		if (graph[blkno].emptyLeafOffsets)
		{
			GistRescanItem *next = palloc(sizeof(GistRescanItem));
			next->blkno = blkno;
			next->emptyLeafOffsets = graph[blkno].emptyLeafOffsets;
			next->next = result;
			result = next;
		}
	}

	pfree(graph);

	return result;
}

/* Logical scan descends from root to leafs in DFS search */
static GistRescanItem*
gistbulkdeletelogicalscan(IndexVacuumInfo * info, IndexBulkDeleteResult * stats, IndexBulkDeleteCallback callback, void* callback_state)
{
	Relation        rel = info->index;
	BlockNumber     recentParent = InvalidBlockNumber;
	GistBDItem     *stack,
				   *ptr;
	GistRescanItem *result = NULL;

	/* This stack is used to organize DFS */
	stack = (GistBDItem *) palloc0(sizeof(GistBDItem));
	stack->blkno = GIST_ROOT_BLKNO;

	while (stack)
	{
		Buffer		buffer;
		Page		page;
		OffsetNumber i,
					maxoff;
		IndexTuple	idxtuple;
		ItemId		iid;

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, stack->blkno,
									RBM_NORMAL, info->strategy);
		LockBuffer(buffer, GIST_SHARE);
		gistcheckpage(rel, buffer);
		page = (Page) BufferGetPage(buffer);

		if (GistPageIsLeaf(page))
		{
			OffsetNumber todelete[MaxOffsetNumber];
			int			ntodelete = 0;

			LockBuffer(buffer, GIST_UNLOCK);
			LockBuffer(buffer, GIST_EXCLUSIVE);

			page = (Page) BufferGetPage(buffer);
			if (stack->blkno == GIST_ROOT_BLKNO && !GistPageIsLeaf(page))
			{
				/* only the root can become non-leaf during relock */
				UnlockReleaseBuffer(buffer);
				/* one more check */
				continue;
			}

			/*
			 * check for split proceeded after look at parent, we should check
			 * it after relock
			 */
			pushStackIfSplited(page, stack);

			/*
			 * Remove deletable tuples from page
			 */

			maxoff = PageGetMaxOffsetNumber(page);

			for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
			{
				iid = PageGetItemId(page, i);
				idxtuple = (IndexTuple) PageGetItem(page, iid);

				if (callback(&(idxtuple->t_tid), callback_state))
					todelete[ntodelete++] = i;
				else
					stats->num_index_tuples += 1;
			}

			stats->tuples_removed += ntodelete;

			if (ntodelete)
			{
				START_CRIT_SECTION();

				MarkBufferDirty(buffer);

				PageIndexMultiDelete(page, todelete, ntodelete);
				GistMarkTuplesDeleted(page);

				if (RelationNeedsWAL(rel))
				{
					XLogRecPtr	recptr;

					recptr = gistXLogUpdate(buffer,
											todelete, ntodelete,
											NULL, 0, InvalidBuffer);
					PageSetLSN(page, recptr);
				}
				else
					PageSetLSN(page, gistGetFakeLSN(rel));

				END_CRIT_SECTION();
			}

			if (ntodelete == maxoff && recentParent!=InvalidBlockNumber)
			{
				/* This page is a candidate to be deleted. Remember it's parent to rescan it later with xlock */
				if (result == NULL || result->blkno != recentParent)
				{
					GistRescanItem *next = palloc(sizeof(GistRescanItem));
					next->blkno = recentParent;
					next->emptyLeafOffsets = NULL;
					next->next = result;
					result = next;
				}
				result->emptyLeafOffsets = lappend_int(result->emptyLeafOffsets, stack->parentoffset);
			}
		}
		else
		{
			recentParent = stack->blkno;
			/* check for split proceeded after look at parent */
			pushStackIfSplited(page, stack);

			maxoff = PageGetMaxOffsetNumber(page);

			for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
			{
				iid = PageGetItemId(page, i);
				idxtuple = (IndexTuple) PageGetItem(page, iid);

				ptr = (GistBDItem *) palloc(sizeof(GistBDItem));
				ptr->blkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));
				ptr->parentlsn = BufferGetLSNAtomic(buffer);
				ptr->next = stack->next;
				ptr->parentoffset = i;
				stack->next = ptr;

				if (GistTupleIsInvalid(idxtuple))
					ereport(LOG,
							(errmsg("index \"%s\" contains an inner tuple marked as invalid",
									RelationGetRelationName(rel)),
							 errdetail("This is caused by an incomplete page split at crash recovery before upgrading to PostgreSQL 9.1."),
							 errhint("Please REINDEX it.")));
			}
		}

		UnlockReleaseBuffer(buffer);

		ptr = stack->next;
		pfree(stack);
		stack = ptr;

		vacuum_delay_point();
	}

	return result;
}

/*
 * This function is used to sort offsets for PageIndexMultiDelete
 * When employing physical scan rescan offsets are not ordered.
 */
static int
compare_offsetnumber(const void *x, const void *y)
{
	OffsetNumber a = *((OffsetNumber *)x);
	OffsetNumber b = *((OffsetNumber *)y);
	return a - b;
}

/*
 * Bulk deletion of all index entries pointing to a set of heap tuples and
 * check invalid tuples left after upgrade.
 * The set of target tuples is specified via a callback routine that tells
 * whether any given heap tuple (identified by ItemPointer) is being deleted.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
IndexBulkDeleteResult *
gistbulkdelete(IndexVacuumInfo * info, IndexBulkDeleteResult * stats, IndexBulkDeleteCallback callback, void* callback_state)
{
	Relation		rel = info->index;
	GistRescanItem *rescan;
	BlockNumber		npages;
	bool			needLock;

	/* first time through? */
	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
	/* we'll re-count the tuples each time */
	stats->estimated_count = false;
	stats->num_index_tuples = 0;

	/*
	 * Need lock unless it's local to this backend.
	 */
	needLock = !RELATION_IS_LOCAL(rel);

	/* try to find deleted pages */
	if (needLock)
		LockRelationForExtension(rel, ExclusiveLock);
	npages = RelationGetNumberOfBlocks(rel);
	if (needLock)
		UnlockRelationForExtension(rel, ExclusiveLock);

	/* If we have enough space to contruct map of whole graph, then we can do sequential reading of all index */
	if (npages * (sizeof(GistPSItem)) > maintenance_work_mem * 1024)
	{
		rescan = gistbulkdeletelogicalscan(info, stats, callback, callback_state);
	}
	else
	{
		rescan = gistbulkdeletephysicalcan(info, stats, callback, callback_state, npages);
	}

	/* rescan inner pages that had empty child pages */
	while (rescan)
	{
		Buffer			 buffer;
		Page			 page;
		OffsetNumber 	 i,
						 maxoff;
		IndexTuple		 idxtuple;
		ItemId			 iid;
		OffsetNumber 	 todelete[MaxOffsetNumber];
		Buffer			 buftodelete[MaxOffsetNumber];
		int				 ntodelete = 0;
		ListCell  		*cell;
		GistRescanItem	*oldRescan;

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, rescan->blkno,
									RBM_NORMAL, info->strategy);
		LockBuffer(buffer, GIST_EXCLUSIVE);
		gistcheckpage(rel, buffer);
		page = (Page) BufferGetPage(buffer);

		Assert(!GistPageIsLeaf(page));

		maxoff = PageGetMaxOffsetNumber(page);

		/* Check that leafs are still empty and decide what to delete */
		foreach(cell, rescan->emptyLeafOffsets)
		{
			Buffer		leafBuffer;
			Page		leafPage;

			i = (OffsetNumber)lfirst_int(cell);
			if(i > maxoff)
			{
				continue;
			}

			iid = PageGetItemId(page, i);
			idxtuple = (IndexTuple) PageGetItem(page, iid);

			leafBuffer = ReadBufferExtended(rel, MAIN_FORKNUM, ItemPointerGetBlockNumber(&(idxtuple->t_tid)),
								RBM_NORMAL, info->strategy);
			LockBuffer(leafBuffer, GIST_EXCLUSIVE);
			gistcheckpage(rel, leafBuffer);
			leafPage = (Page) BufferGetPage(leafBuffer);
			Assert(GistPageIsLeaf(leafPage));

			if (PageGetMaxOffsetNumber(leafPage) == InvalidOffsetNumber /* Nothing left to split */
				&& !(GistFollowRight(leafPage) || GistPageGetNSN(page) < GistPageGetNSN(leafPage)) /* No follow-right */
				&& ntodelete < maxoff-1) /* We must keep at least one leaf page per each */
			{
				buftodelete[ntodelete] = leafBuffer;
				todelete[ntodelete++] = i;
			}
			else
				UnlockReleaseBuffer(leafBuffer);
		}


		if (ntodelete)
		{
			/* Drop references from internal page */
			TransactionId txid = GetCurrentTransactionId();
			START_CRIT_SECTION();

			MarkBufferDirty(buffer);
			/* Prepare possibly onurdered offsets */
			qsort(todelete, ntodelete, sizeof(OffsetNumber), compare_offsetnumber);
			PageIndexMultiDelete(page, todelete, ntodelete);

			if (RelationNeedsWAL(rel))
			{
				XLogRecPtr	recptr;

				recptr = gistXLogUpdate(buffer, todelete, ntodelete, NULL, 0, InvalidBuffer);
					PageSetLSN(page, recptr);
			}
			else
				PageSetLSN(page, gistGetFakeLSN(rel));

			/* Mark pages as deleted */
			for (i = 0; i < ntodelete; i++)
			{
				Page		leafPage = (Page)BufferGetPage(buftodelete[i]);
				PageHeader	header = (PageHeader)leafPage;

				header->pd_prune_xid = txid;

				GistPageSetDeleted(leafPage);
				MarkBufferDirty(buftodelete[i]);
				stats->pages_deleted++;

				if (RelationNeedsWAL(rel))
				{
					XLogRecPtr recptr = gistXLogSetDeleted(rel->rd_node, buftodelete[i], header->pd_prune_xid);
					PageSetLSN(leafPage, recptr);
				}
				else
					PageSetLSN(leafPage, gistGetFakeLSN(rel));

				UnlockReleaseBuffer(buftodelete[i]);
			}
			END_CRIT_SECTION();
		}

		UnlockReleaseBuffer(buffer);
		oldRescan = rescan;
		rescan = rescan->next;
		list_free(oldRescan->emptyLeafOffsets);
		pfree(oldRescan);

		vacuum_delay_point();
	}


	return stats;
}