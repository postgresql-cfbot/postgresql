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
 * Copyright (c) 2017-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/amcheck/verify_gist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/gist_private.h"
#include "access/tableam.h"
#include "amcheck.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "common/pg_prng.h"
#include "lib/bloomfilter.h"
#include "utils/memutils.h"


/*
 * GistScanItem represents one item of depth-first scan of GiST index.
 */
typedef struct GistScanItem
{
	int			depth;

	/* Referenced block number to check next */
	BlockNumber blkno;

	/*
	 * Correctess of this parent tuple will be checked against contents of referenced page.
	 * This tuple will be NULL for root block.
	 */
	IndexTuple	parenttup;

	/*
	 * LSN to hande concurrent scan of the page.
	 * It's necessary to avoid missing some subtrees from page, that was
	 * split just before we read it.
	 */
	XLogRecPtr	parentlsn;

	/*
	 * Reference to parent page for re-locking in case of found parent-child
	 * tuple discrepencies.
	 */
	BlockNumber parentblk;

	/* Pointer to a next stack item. */
	struct GistScanItem *next;
} GistScanItem;

typedef struct GistCheckState
{
	/* Bloom filter fingerprints index tuples */
	bloom_filter *filter;

	/* Debug counter	FIXME what does 'debug counter' mean?*/
	int64		heaptuplespresent;

	/* XXX nitpick: I'd move these 'generic' fields to the beginning */
	/* GiST state */
	GISTSTATE  *state;

	Snapshot	snapshot;
	Relation	rel;
	Relation	heaprel;

	/* progress reporting stuff */
	BlockNumber totalblocks;
	BlockNumber reportedblocks;
	BlockNumber scannedblocks;
	BlockNumber deltablocks;

	int leafdepth;
} GistCheckState;

PG_FUNCTION_INFO_V1(gist_index_check);

static void gist_init_heapallindexed(Relation rel, GistCheckState * result);
static void gist_check_parent_keys_consistency(Relation rel, Relation heaprel,
											   void *callback_state, bool readonly);
static void gist_check_page(GistCheckState *check_state,GistScanItem *stack,
							Page page, bool heapallindexed,
							BufferAccessStrategy strategy);
static void check_index_page(Relation rel, Buffer buffer, BlockNumber blockNo);
static IndexTuple gist_refind_parent(Relation rel, BlockNumber parentblkno,
									 BlockNumber childblkno,
									 BufferAccessStrategy strategy);
static ItemId PageGetItemIdCareful(Relation rel, BlockNumber block,
								   Page page, OffsetNumber offset);
static void gist_tuple_present_callback(Relation index, ItemPointer tid,
										Datum *values, bool *isnull,
										bool tupleIsAlive, void *checkstate);
static IndexTuple gistFormNormalizedTuple(GISTSTATE *giststate, Relation r,
			  Datum *attdata, bool *isnull, ItemPointerData tid);

/*
 * gist_index_check(index regclass)
 *		Verify integrity of GiST index.
 *
 * Acquires AccessShareLock on heap & index relations.
 */
Datum
gist_index_check(PG_FUNCTION_ARGS)
{
	Oid			indrelid = PG_GETARG_OID(0);
	bool		heapallindexed = PG_GETARG_BOOL(1);

	amcheck_lock_relation_and_check(indrelid,
									GIST_AM_OID,
									gist_check_parent_keys_consistency,
									AccessShareLock,
									&heapallindexed);

	PG_RETURN_VOID();
}

/*
 * XXX This talks about 'heapallindexed' but it initializes a snapshot too,
 * but isn't that unrelated / confusing?
 */
static void
gist_init_heapallindexed(Relation rel, GistCheckState * result)
{
	int64		total_pages;
	int64		total_elems;
	uint64		seed;

	/*
	 * Size Bloom filter based on estimated number of tuples in index. This
	 * logic is similar to B-tree, see verify_btree.c .
	 */
	total_pages = result->totalblocks;
	total_elems = Max(total_pages * (MaxOffsetNumber / 5),
					  (int64) rel->rd_rel->reltuples);
	seed = pg_prng_uint64(&pg_global_prng_state);
	result->filter = bloom_create(total_elems, maintenance_work_mem, seed);

	result->snapshot = RegisterSnapshot(GetTransactionSnapshot());

	/*
	 * GetTransactionSnapshot() always acquires a new MVCC snapshot in READ
	 * COMMITTED mode.  A new snapshot is guaranteed to have all the entries
	 * it requires in the index.
	 *
	 * We must defend against the possibility that an old xact snapshot was
	 * returned at higher isolation levels when that snapshot is not safe for
	 * index scans of the target index.  This is possible when the snapshot
	 * sees tuples that are before the index's indcheckxmin horizon.  Throwing
	 * an error here should be very rare.  It doesn't seem worth using a
	 * secondary snapshot to avoid this.
	 */
	if (IsolationUsesXactSnapshot() && rel->rd_index->indcheckxmin &&
		!TransactionIdPrecedes(HeapTupleHeaderGetXmin(rel->rd_indextuple->t_data),
							   result->snapshot->xmin))
		ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 errmsg("index \"%s\" cannot be verified using transaction snapshot",
						RelationGetRelationName(rel))));
}

/*
 * Main entry point for GiST check. Allocates memory context and scans through
 * GiST graph. This scan is performed in a depth-first search using a stack of
 * GistScanItem-s. Initially this stack contains only root block number. On
 * each iteration the top block number is replaced by referenced block numbers.
 *
 * XXX I'd move the following paragraph right before "allocates memory". It
 * describes what the function does, while the "allocates memory" is more
 * a description of "how" it is done.
 *
 * This function verifies that tuples of internal pages cover all
 * the key space of each tuple on leaf page.  To do this we invoke
 * gist_check_internal_page() for every internal page.
 *
 * gist_check_internal_page() in it's turn takes every tuple and tries to
 * adjust it by tuples on referenced child page.  Parent gist tuple should
 * never require any adjustments.
 */
static void
gist_check_parent_keys_consistency(Relation rel, Relation heaprel,
								   void *callback_state, bool readonly)
{
	BufferAccessStrategy strategy = GetAccessStrategy(BAS_BULKREAD);
	GistScanItem *stack;
	MemoryContext mctx;
	MemoryContext oldcontext;
	GISTSTATE  *state;
	bool		heapallindexed = *((bool *) callback_state);
	GistCheckState *check_state = palloc0(sizeof(GistCheckState));

	mctx = AllocSetContextCreate(CurrentMemoryContext,
								 "amcheck context",
								 ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(mctx);

	state = initGISTstate(rel);

	check_state->state = state;
	check_state->rel = rel;
	check_state->heaprel = heaprel;

	/*
	 * We don't know the height of the tree yet, but as soon as we encounter a
	 * leaf page, we will set 'leafdepth' to its depth.
	 */
	check_state->leafdepth = -1;

	check_state->totalblocks = RelationGetNumberOfBlocks(rel);
	/* report every 100 blocks or 5%, whichever is bigger */
	check_state->deltablocks = Max(check_state->totalblocks / 20, 100);

	if (heapallindexed)
		gist_init_heapallindexed(rel, check_state);

	/* Start the scan at the root page */
	stack = (GistScanItem *) palloc0(sizeof(GistScanItem));
	stack->depth = 0;
	stack->parenttup = NULL;
	stack->parentblk = InvalidBlockNumber;
	stack->parentlsn = InvalidXLogRecPtr;
	stack->blkno = GIST_ROOT_BLKNO;

	/*
	 * This GiST scan is effectively "old" VACUUM version before commit
	 * fe280694d which introduced physical order scanning.
	 */

	while (stack)
	{
		GistScanItem *stack_next;
		Buffer		buffer;
		Page		page;
		XLogRecPtr	lsn;

		CHECK_FOR_INTERRUPTS();

		/* Report progress */
		if (check_state->scannedblocks > check_state->reportedblocks +
			check_state->deltablocks)
		{
			elog(DEBUG1, "verified level %u blocks of approximately %u total",
				 check_state->scannedblocks, check_state->totalblocks);
			check_state->reportedblocks = check_state->scannedblocks;
		}
		check_state->scannedblocks++;

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

		gist_check_page(check_state, stack, page, heapallindexed, strategy);

		if (!GistPageIsLeaf(page))
		{
			OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
			for (OffsetNumber i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
			{
				/* Internal page, so recurse to the child */
				GistScanItem *ptr;
				ItemId		iid = PageGetItemIdCareful(rel, stack->blkno, page, i);
				IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);

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

	if (heapallindexed)
	{
		IndexInfo  *indexinfo = BuildIndexInfo(rel);
		TableScanDesc scan;

		scan = table_beginscan_strat(heaprel,	/* relation */
									 check_state->snapshot,	/* snapshot */
									 0, /* number of keys */
									 NULL,	/* scan key */
									 true,	/* buffer access strategy OK */
									 true); /* syncscan OK? */

		/*
		 * Scan will behave as the first scan of a CREATE INDEX CONCURRENTLY.
		 */
		indexinfo->ii_Concurrent = true;

		indexinfo->ii_Unique = false;
		indexinfo->ii_ExclusionOps = NULL;
		indexinfo->ii_ExclusionProcs = NULL;
		indexinfo->ii_ExclusionStrats = NULL;

		elog(DEBUG1, "verifying that tuples from index \"%s\" are present in \"%s\"",
			 RelationGetRelationName(rel),
			 RelationGetRelationName(heaprel));

		table_index_build_scan(heaprel, rel, indexinfo, true, false,
							   gist_tuple_present_callback, (void *) check_state, scan);

		ereport(DEBUG1,
				(errmsg_internal("finished verifying presence of " INT64_FORMAT " tuples from table \"%s\" with bitset %.2f%% set",
								 check_state->heaptuplespresent,
								 RelationGetRelationName(heaprel),
								 100.0 * bloom_prop_bits_set(check_state->filter))));

		UnregisterSnapshot(check_state->snapshot);
		bloom_free(check_state->filter);
	}

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(mctx);
	pfree(check_state);
}

static void
gist_check_page(GistCheckState *check_state, GistScanItem *stack,
				Page page, bool heapallindexed, BufferAccessStrategy strategy)
{
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

	/* Check that the tree has the same height in all branches */
	if (GistPageIsLeaf(page))
	{
		if (check_state->leafdepth == -1)
			check_state->leafdepth = stack->depth;
		else if (stack->depth != check_state->leafdepth)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
						errmsg("index \"%s\": internal pages traversal encountered leaf page unexpectedly on block %u",
							RelationGetRelationName(check_state->rel), stack->blkno)));
	}

	/*
	 * Check that each tuple looks valid, and is consistent with the
	 * downlink we followed when we stepped on this page.
	 */
	for (OffsetNumber i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
	{
		ItemId		iid = PageGetItemIdCareful(check_state->rel, stack->blkno, page, i);
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
							RelationGetRelationName(check_state->rel), stack->blkno, i),
						errdetail("This is caused by an incomplete page split at crash recovery before upgrading to PostgreSQL 9.1."),
						errhint("Please REINDEX it.")));

		if (MAXALIGN(ItemIdGetLength(iid)) != MAXALIGN(IndexTupleSize(idxtuple)))
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
						errmsg("index \"%s\" has inconsistent tuple sizes, block %u, offset %u",
							RelationGetRelationName(check_state->rel), stack->blkno, i)));

		/*
		 * Check if this tuple is consistent with the downlink in the
		 * parent.
		 */
		if (stack->parenttup &&
			gistgetadjusted(check_state->rel, stack->parenttup, idxtuple, check_state->state))
		{
			/*
			 * There was a discrepancy between parent and child tuples. We
			 * need to verify it is not a result of concurrent call of
			 * gistplacetopage(). So, lock parent and try to find downlink
			 * for current page. It may be missing due to concurrent page
			 * split, this is OK.
			 *
			 * Note that when we aquire parent tuple now we hold lock for
			 * both parent and child buffers. Thus parent tuple must
			 * include keyspace of the child.
			 */
			pfree(stack->parenttup);
			stack->parenttup = gist_refind_parent(check_state->rel, stack->parentblk,
													stack->blkno, strategy);

			/* We found it - make a final check before failing */
			if (!stack->parenttup)
				elog(NOTICE, "Unable to find parent tuple for block %u on block %u due to concurrent split",
						stack->blkno, stack->parentblk);
			else if (gistgetadjusted(check_state->rel, stack->parenttup, idxtuple, check_state->state))
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
							errmsg("index \"%s\" has inconsistent records on page %u offset %u",
								RelationGetRelationName(check_state->rel), stack->blkno, i)));
			else
			{
				/*
				 * But now it is properly adjusted - nothing to do here.
				 */
			}
		}

		if (GistPageIsLeaf(page))
		{
			if (heapallindexed)
				bloom_add_element(check_state->filter,
									(unsigned char *) idxtuple,
									IndexTupleSize(idxtuple));
		}
		else
		{
			OffsetNumber off = ItemPointerGetOffsetNumber(&(idxtuple->t_tid));
			if (off != 0xffff)
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
							errmsg("index \"%s\" has on page %u offset %u has item id not pointing to 0xffff, but %hu",
								RelationGetRelationName(check_state->rel), stack->blkno, i, off)));
		}
	}
}

/*
 * gistFormNormalizedTuple - analogue to gistFormTuple, but performs deTOASTing
 * of all included data (for covering indexes). While we do not expected
 * toasted attributes in normal index, this can happen as a result of
 * intervention into system catalog. Detoasting of key attributes is expected
 * to be done by opclass decompression methods, if indexed type might be
 * toasted.
 */
static IndexTuple
gistFormNormalizedTuple(GISTSTATE *giststate, Relation r,
						Datum *attdata, bool *isnull, ItemPointerData tid)
{
	Datum		compatt[INDEX_MAX_KEYS];
	IndexTuple	res;

	gistCompressValues(giststate, r, attdata, isnull, true, compatt);

	for (int i = 0; i < r->rd_att->natts; i++)
	{
		Form_pg_attribute att;

		att = TupleDescAttr(giststate->leafTupdesc, i);
		if (att->attbyval || att->attlen != -1 || isnull[i])
			continue;

		if (VARATT_IS_EXTERNAL(DatumGetPointer(compatt[i])))
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("external varlena datum in tuple that references heap row (%u,%u) in index \"%s\"",
							ItemPointerGetBlockNumber(&tid),
							ItemPointerGetOffsetNumber(&tid),
							RelationGetRelationName(r))));
		if (VARATT_IS_COMPRESSED(DatumGetPointer(compatt[i])))
		{
			//Datum old = compatt[i];
			/* Key attributes must never be compressed */
			if (i < IndexRelationGetNumberOfKeyAttributes(r))
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
							errmsg("compressed varlena datum in tuple key that references heap row (%u,%u) in index \"%s\"",
								ItemPointerGetBlockNumber(&tid),
								ItemPointerGetOffsetNumber(&tid),
								RelationGetRelationName(r))));

			compatt[i] = PointerGetDatum(PG_DETOAST_DATUM(compatt[i]));
			//pfree(DatumGetPointer(old)); // TODO: this fails. Why?
		}
	}

	res = index_form_tuple(giststate->leafTupdesc, compatt, isnull);

	/*
	 * The offset number on tuples on internal pages is unused. For historical
	 * reasons, it is set to 0xffff.
	 */
	ItemPointerSetOffsetNumber(&(res->t_tid), 0xffff);
	return res;
}

static void
gist_tuple_present_callback(Relation index, ItemPointer tid, Datum *values,
							bool *isnull, bool tupleIsAlive, void *checkstate)
{
	GistCheckState *state = (GistCheckState *) checkstate;
	IndexTuple	itup = gistFormNormalizedTuple(state->state, index, values, isnull, *tid);

	itup->t_tid = *tid;

	/* Probe Bloom filter -- tuple should be present */
	if (bloom_lacks_element(state->filter, (unsigned char *) itup,
							IndexTupleSize(itup)))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("heap tuple (%u,%u) from table \"%s\" lacks matching index tuple within index \"%s\"",
						ItemPointerGetBlockNumber(&(itup->t_tid)),
						ItemPointerGetOffsetNumber(&(itup->t_tid)),
						RelationGetRelationName(state->heaprel),
						RelationGetRelationName(state->rel))));

	state->heaptuplespresent++;

	pfree(itup);
}

/*
 * check_index_page - verification of basic invariants about GiST page data
 * This function does no any tuple analysis.
 */
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
gist_refind_parent(Relation rel,
				   BlockNumber parentblkno, BlockNumber childblkno,
				   BufferAccessStrategy strategy)
{
	Buffer		parentbuf;
	Page		parentpage;
	OffsetNumber parent_maxoff;
	IndexTuple	result = NULL;

	parentbuf = ReadBufferExtended(rel, MAIN_FORKNUM, parentblkno, RBM_NORMAL,
								   strategy);

	LockBuffer(parentbuf, GIST_SHARE);
	parentpage = BufferGetPage(parentbuf);

	if (GistPageIsLeaf(parentpage))
	{
		/*
		 * Currently GiST never deletes internal pages, thus they can never
		 * become leaf.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
					errmsg("index \"%s\" internal page %d became leaf",
						RelationGetRelationName(rel), parentblkno)));
	}

	parent_maxoff = PageGetMaxOffsetNumber(parentpage);
	for (OffsetNumber o = FirstOffsetNumber; o <= parent_maxoff; o = OffsetNumberNext(o))
	{
		ItemId		p_iid = PageGetItemIdCareful(rel, parentblkno, parentpage, o);
		IndexTuple	itup = (IndexTuple) PageGetItem(parentpage, p_iid);

		if (ItemPointerGetBlockNumber(&(itup->t_tid)) == childblkno)
		{
			/*
			 * Found it! Make copy and return it while both parent and child
			 * pages are locked. This guaranties that at this particular moment
			 * tuples must be coherent to each other.
			 */
			result = CopyIndexTuple(itup);
			break;
		}
	}

	UnlockReleaseBuffer(parentbuf);

	return result;
}

/*
 * XXX What does this do? Maybe it should be in core or some common file?
 * Seems to be used in verify_btree.c too.
 */
static ItemId
PageGetItemIdCareful(Relation rel, BlockNumber block, Page page,
					 OffsetNumber offset)
{
	ItemId		itemid = PageGetItemId(page, offset);

	if (ItemIdGetOffset(itemid) + ItemIdGetLength(itemid) >
		BLCKSZ - MAXALIGN(sizeof(GISTPageOpaqueData)))
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("line pointer points past end of tuple space in index \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail_internal("Index tid=(%u,%u) lp_off=%u, lp_len=%u lp_flags=%u.",
									block, offset, ItemIdGetOffset(itemid),
									ItemIdGetLength(itemid),
									ItemIdGetFlags(itemid))));

	/*
	 * Verify that line pointer isn't LP_REDIRECT or LP_UNUSED, since nbtree
	 * and gist never uses either.  Verify that line pointer has storage, too,
	 * since even LP_DEAD items should.
	 *
	 * XXX why does this reference nbtree?
	 */
	if (ItemIdIsRedirected(itemid) || !ItemIdIsUsed(itemid) ||
		ItemIdGetLength(itemid) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("invalid line pointer storage in index \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail_internal("Index tid=(%u,%u) lp_off=%u, lp_len=%u lp_flags=%u.",
									block, offset, ItemIdGetOffset(itemid),
									ItemIdGetLength(itemid),
									ItemIdGetFlags(itemid))));

	return itemid;
}
