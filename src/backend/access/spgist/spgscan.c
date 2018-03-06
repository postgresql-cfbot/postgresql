/*-------------------------------------------------------------------------
 *
 * spgscan.c
 *	  routines for scanning SP-GiST indexes
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/spgist/spgscan.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>

#include "access/genam.h"
#include "access/relscan.h"
#include "access/spgist_private.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

typedef void (*storeRes_func) (SpGistScanOpaque so, ItemPointer heapPtr,
							   Datum leafValue, bool isnull, bool recheck,
							   bool recheckDist, double *distances);

/*
 * Pairing heap comparison function for the SpGistSearchItem queue
 */
static int
pairingheap_SpGistSearchItem_cmp(const pairingheap_node *a,
								 const pairingheap_node *b, void *arg)
{
	const SpGistSearchItem *sa = (const SpGistSearchItem *) a;
	const SpGistSearchItem *sb = (const SpGistSearchItem *) b;
	IndexScanDesc scan = (IndexScanDesc) arg;
	int			i;

	if (sa->isnull)
	{
		if (!sb->isnull)
			return -1;
	}
	else if (sb->isnull)
	{
		return 1;
	}
	else
	{
		/* Order according to distance comparison */
		for (i = 0; i < scan->numberOfOrderBys; i++)
		{
			if (isnan(sa->distances[i]) && isnan(sb->distances[i]))
				continue;	/* NaN == NaN */
			if (isnan(sa->distances[i]))
				return -1;	/* NaN > number */
			if (isnan(sb->distances[i]))
				return 1;	/* number < NaN */
			if (sa->distances[i] != sb->distances[i])
				return (sa->distances[i] < sb->distances[i]) ? 1 : -1;
		}
	}

	/* Leaf items go before inner pages, to ensure a depth-first search */
	if (sa->isLeaf && !sb->isLeaf)
		return 1;
	if (!sa->isLeaf && sb->isLeaf)
		return -1;

	return 0;
}

static void
spgFreeSearchItem(SpGistScanOpaque so, SpGistSearchItem *item)
{
	if (!so->state.attLeafType.attbyval &&
		DatumGetPointer(item->value) != NULL)
		pfree(DatumGetPointer(item->value));

	if (item->traversalValue)
		pfree(item->traversalValue);

	pfree(item);
}

/*
 * Add SpGistSearchItem to queue
 *
 * Called in queue context
 */
static void
spgAddSearchItemToQueue(SpGistScanOpaque so, SpGistSearchItem *item,
						double *distances)
{
	if (!item->isnull)
		memcpy(item->distances, distances,
			   so->numberOfOrderBys * sizeof(double));

	if (so->queue)
		pairingheap_add(so->queue, &item->phNode);
	else
		so->scanStack = lcons(item, so->scanStack);
}

static void
spgAddStartItem(SpGistScanOpaque so, bool isnull)
{
	SpGistSearchItem *startEntry = (SpGistSearchItem *) palloc0(
								SizeOfSpGistSearchItem(so->numberOfOrderBys));

	ItemPointerSet(&startEntry->heap,
				   isnull ? SPGIST_NULL_BLKNO : SPGIST_ROOT_BLKNO,
				   FirstOffsetNumber);
	startEntry->isLeaf = false;
	startEntry->level = 0;
	startEntry->isnull = isnull;

	spgAddSearchItemToQueue(so, startEntry, so->zeroDistances);
}

/*
 * Initialize queue to search the root page, resetting
 * any previously active scan
 */
static void
resetSpGistScanOpaque(SpGistScanOpaque so)
{
	MemoryContext oldCtx = MemoryContextSwitchTo(so->queueCxt);

	if (so->searchNulls)
		/* Add a work item to scan the null index entries */
		spgAddStartItem(so, true);

	if (so->searchNonNulls)
		/* Add a work item to scan the non-null index entries */
		spgAddStartItem(so, false);

	MemoryContextSwitchTo(oldCtx);

	if (so->numberOfOrderBys > 0)
	{
		/* Must pfree distances to avoid memory leak */
		int			i;

		for (i = 0; i < so->nPtrs; i++)
			if (so->distances[i])
				pfree(so->distances[i]);
	}

	if (so->want_itup)
	{
		/* Must pfree reconstructed tuples to avoid memory leak */
		int			i;

		for (i = 0; i < so->nPtrs; i++)
			pfree(so->reconTups[i]);
	}
	so->iPtr = so->nPtrs = 0;
}

/*
 * Prepare scan keys in SpGistScanOpaque from caller-given scan keys
 *
 * Sets searchNulls, searchNonNulls, numberOfKeys, keyData fields of *so.
 *
 * The point here is to eliminate null-related considerations from what the
 * opclass consistent functions need to deal with.  We assume all SPGiST-
 * indexable operators are strict, so any null RHS value makes the scan
 * condition unsatisfiable.  We also pull out any IS NULL/IS NOT NULL
 * conditions; their effect is reflected into searchNulls/searchNonNulls.
 */
static void
spgPrepareScanKeys(IndexScanDesc scan)
{
	SpGistScanOpaque so = (SpGistScanOpaque) scan->opaque;
	bool		qual_ok;
	bool		haveIsNull;
	bool		haveNotNull;
	int			nkeys;
	int			i;

	so->numberOfOrderBys = scan->numberOfOrderBys;
	so->orderByData = scan->orderByData;

	if (scan->numberOfKeys <= 0)
	{
		/* If no quals, whole-index scan is required */
		so->searchNulls = true;
		so->searchNonNulls = true;
		so->numberOfKeys = 0;
		return;
	}

	/* Examine the given quals */
	qual_ok = true;
	haveIsNull = haveNotNull = false;
	nkeys = 0;
	for (i = 0; i < scan->numberOfKeys; i++)
	{
		ScanKey		skey = &scan->keyData[i];

		if (skey->sk_flags & SK_SEARCHNULL)
			haveIsNull = true;
		else if (skey->sk_flags & SK_SEARCHNOTNULL)
			haveNotNull = true;
		else if (skey->sk_flags & SK_ISNULL)
		{
			/* ordinary qual with null argument - unsatisfiable */
			qual_ok = false;
			break;
		}
		else
		{
			/* ordinary qual, propagate into so->keyData */
			so->keyData[nkeys++] = *skey;
			/* this effectively creates a not-null requirement */
			haveNotNull = true;
		}
	}

	/* IS NULL in combination with something else is unsatisfiable */
	if (haveIsNull && haveNotNull)
		qual_ok = false;

	/* Emit results */
	if (qual_ok)
	{
		so->searchNulls = haveIsNull;
		so->searchNonNulls = haveNotNull;
		so->numberOfKeys = nkeys;
	}
	else
	{
		so->searchNulls = false;
		so->searchNonNulls = false;
		so->numberOfKeys = 0;
	}
}

IndexScanDesc
spgbeginscan(Relation rel, int keysz, int orderbysz)
{
	IndexScanDesc scan;
	SpGistScanOpaque so;
	int			i;

	scan = RelationGetIndexScan(rel, keysz, orderbysz);

	so = (SpGistScanOpaque) palloc0(sizeof(SpGistScanOpaqueData));
	if (keysz > 0)
		so->keyData = (ScanKey) palloc(sizeof(ScanKeyData) * keysz);
	else
		so->keyData = NULL;
	initSpGistState(&so->state, scan->indexRelation);

	so->tempCxt = AllocSetContextCreate(CurrentMemoryContext,
										"SP-GiST search temporary context",
										ALLOCSET_DEFAULT_SIZES);

	/* Set up indexTupDesc and xs_hitupdesc in case it's an index-only scan */
	so->indexTupDesc = scan->xs_hitupdesc = RelationGetDescr(rel);

	if (scan->numberOfOrderBys > 0)
	{
		so->zeroDistances = palloc(sizeof(double) * scan->numberOfOrderBys);
		so->infDistances = palloc(sizeof(double) * scan->numberOfOrderBys);

		for (i = 0; i < scan->numberOfOrderBys; i++)
		{
			so->zeroDistances[i] = 0.0;
			so->infDistances[i] = get_float8_infinity();
		}

		scan->xs_orderbyvals = palloc0(sizeof(Datum) * scan->numberOfOrderBys);
		scan->xs_orderbynulls = palloc(sizeof(bool) * scan->numberOfOrderBys);
		memset(scan->xs_orderbynulls, true, sizeof(bool) * scan->numberOfOrderBys);
	}

	so->queueCxt = AllocSetContextCreate(CurrentMemoryContext,
										 "SP-GiST queue context",
										 ALLOCSET_DEFAULT_SIZES);

	fmgr_info_copy(&so->innerConsistentFn,
				   index_getprocinfo(rel, 1, SPGIST_INNER_CONSISTENT_PROC),
				   CurrentMemoryContext);

	fmgr_info_copy(&so->leafConsistentFn,
				   index_getprocinfo(rel, 1, SPGIST_LEAF_CONSISTENT_PROC),
				   CurrentMemoryContext);

	so->indexCollation = rel->rd_indcollation[0];

	scan->opaque = so;

	return scan;
}

void
spgrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		  ScanKey orderbys, int norderbys)
{
	SpGistScanOpaque so = (SpGistScanOpaque) scan->opaque;
	MemoryContext oldCxt;

	/* copy scankeys into local storage */
	if (scankey && scan->numberOfKeys > 0)
		memmove(scan->keyData, scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));

	if (orderbys && scan->numberOfOrderBys > 0)
	{
		int			i;

		memmove(scan->orderByData, orderbys,
				scan->numberOfOrderBys * sizeof(ScanKeyData));

		so->orderByTypes = (Oid *) palloc(sizeof(Oid) * scan->numberOfOrderBys);

		for (i = 0; i < scan->numberOfOrderBys; i++)
		{
			ScanKey		skey = &scan->orderByData[i];

			/*
			 * Look up the datatype returned by the original ordering
			 * operator. SP-GiST always uses a float8 for the distance function,
			 * but the ordering operator could be anything else.
			 *
			 * XXX: The distance function is only allowed to be lossy if the
			 * ordering operator's result type is float4 or float8.  Otherwise
			 * we don't know how to return the distance to the executor.  But
			 * we cannot check that here, as we won't know if the distance
			 * function is lossy until it returns *recheck = true for the
			 * first time.
			 */
			so->orderByTypes[i] = get_func_rettype(skey->sk_func.fn_oid);
		}
	}

	/* preprocess scankeys, set up the representation in *so */
	spgPrepareScanKeys(scan);

	so->scanStack = NIL;

	MemoryContextReset(so->queueCxt);
	oldCxt = MemoryContextSwitchTo(so->queueCxt);
	/* initialize queue only for distance-ordered scans */
	so->queue = scan->numberOfOrderBys <= 0 ? NULL :
		pairingheap_allocate(pairingheap_SpGistSearchItem_cmp, scan);
	MemoryContextSwitchTo(oldCxt);

	/* set up starting queue entries */
	resetSpGistScanOpaque(so);
}

void
spgendscan(IndexScanDesc scan)
{
	SpGistScanOpaque so = (SpGistScanOpaque) scan->opaque;

	MemoryContextDelete(so->tempCxt);
	MemoryContextDelete(so->queueCxt);

	if (scan->numberOfOrderBys > 0)
	{
		pfree(so->zeroDistances);
		pfree(so->infDistances);
	}
}

/*
 * Leaf SpGistSearchItem constructor, called in queue context
 */
static SpGistSearchItem *
spgNewHeapItem(SpGistScanOpaque so, int level, ItemPointerData heapPtr,
			   Datum leafValue, bool recheckQual, bool recheckDist, bool isnull)
{
	SpGistSearchItem *item = (SpGistSearchItem *) palloc(
								SizeOfSpGistSearchItem(so->numberOfOrderBys));

	item->level = level;
	item->heap = heapPtr;
	/* copy value to queue cxt out of tmp cxt */
	item->value = isnull ? (Datum) 0 :
		datumCopy(leafValue, so->state.attLeafType.attbyval,
				  so->state.attLeafType.attlen);
	item->traversalValue = NULL;
	item->isLeaf = true;
	item->recheckQual = recheckQual;
	item->recheckDist = recheckDist;
	item->isnull = isnull;

	return item;
}

/*
 * Test whether a leaf tuple satisfies all the scan keys
 *
 * *reportedSome is set to true if:
 *		the scan is not ordered AND the item satisfies the scankeys
 */
static bool
spgLeafTest(SpGistScanOpaque so, SpGistSearchItem *item,
			SpGistLeafTuple leafTuple, bool isnull,
			bool *reportedSome, storeRes_func storeRes)
{
	Datum		leafValue;
	double	   *distances;
	bool		result;
	bool		recheckQual;
	bool		recheckDist;

	if (isnull)
	{
		/* Should not have arrived on a nulls page unless nulls are wanted */
		Assert(so->searchNulls);
		leafValue = (Datum) 0;
		/* Assume that all distances for null entries are infinities */
		distances = so->infDistances;
		recheckQual = false;
		recheckDist = false;
		result = true;
	}
	else
	{
		spgLeafConsistentIn in;
		spgLeafConsistentOut out;

		/* use temp context for calling leaf_consistent */
		MemoryContext oldCxt = MemoryContextSwitchTo(so->tempCxt);

		in.scankeys = so->keyData;
		in.nkeys = so->numberOfKeys;
		in.orderbykeys = so->orderByData;
		in.norderbys = so->numberOfOrderBys;
		in.reconstructedValue = item->value;
		in.traversalValue = item->traversalValue;
		in.level = item->level;
		in.returnData = so->want_itup;
		in.leafDatum = SGLTDATUM(leafTuple, &so->state);

		out.leafValue = (Datum) 0;
		out.recheck = false;
		out.distances = NULL;
		out.recheckDistances = false;

		result = DatumGetBool(FunctionCall2Coll(&so->leafConsistentFn,
												so->indexCollation,
												PointerGetDatum(&in),
												PointerGetDatum(&out)));
		recheckQual = out.recheck;
		recheckDist = out.recheckDistances;
		leafValue = out.leafValue;
		distances = out.distances;

		MemoryContextSwitchTo(oldCxt);
	}

	if (result)
	{
		/* item passes the scankeys */
		if (so->numberOfOrderBys > 0)
		{
			/* the scan is ordered -> add the item to the queue */
			MemoryContext oldCxt = MemoryContextSwitchTo(so->queueCxt);
			SpGistSearchItem *heapItem = spgNewHeapItem(so, item->level,
														leafTuple->heapPtr,
														leafValue,
														recheckQual,
														recheckDist,
														isnull);

			spgAddSearchItemToQueue(so, heapItem, distances);

			MemoryContextSwitchTo(oldCxt);
		}
		else
		{
			/* non-ordered scan, so report the item right away */
			Assert(!recheckDist);
			storeRes(so, &leafTuple->heapPtr, leafValue, isnull,
					 recheckQual, false, NULL);
			*reportedSome = true;
		}
	}

	return result;
}

/* A bundle initializer for inner_consistent methods */
static void
spgInitInnerConsistentIn(spgInnerConsistentIn *in,
						 SpGistScanOpaque so,
						 SpGistSearchItem *item,
						 SpGistInnerTuple innerTuple,
						 MemoryContext traversalMemoryContext)
{
	in->scankeys = so->keyData;
	in->nkeys = so->numberOfKeys;
	in->norderbys = so->numberOfOrderBys;
	in->orderbyKeys = so->orderByData;
	in->reconstructedValue = item->value;
	in->traversalMemoryContext = traversalMemoryContext;
	in->traversalValue = item->traversalValue;
	in->level = item->level;
	in->returnData = so->want_itup;
	in->allTheSame = innerTuple->allTheSame;
	in->hasPrefix = (innerTuple->prefixSize > 0);
	in->prefixDatum = SGITDATUM(innerTuple, &so->state);
	in->nNodes = innerTuple->nNodes;
	in->nodeLabels = spgExtractNodeLabels(&so->state, innerTuple);
}

static SpGistSearchItem *
spgMakeInnerItem(SpGistScanOpaque so,
				 SpGistSearchItem *parentItem,
				 SpGistNodeTuple tuple,
				 spgInnerConsistentOut *out, int i, bool isnull)
{
	SpGistSearchItem *item = palloc(SizeOfSpGistSearchItem(so->numberOfOrderBys));

	item->heap = tuple->t_tid;
	item->level = out->levelAdds ? parentItem->level + out->levelAdds[i]
								 : parentItem->level;

	/* Must copy value out of temp context */
	item->value = out->reconstructedValues
					? datumCopy(out->reconstructedValues[i],
								so->state.attLeafType.attbyval,
								so->state.attLeafType.attlen)
					: (Datum) 0;

	/*
	 * Elements of out.traversalValues should be allocated in
	 * in.traversalMemoryContext, which is actually a long
	 * lived context of index scan.
	 */
	item->traversalValue =
			out->traversalValues ? out->traversalValues[i] : NULL;

	item->isLeaf = false;
	item->recheckQual = false;
	item->recheckDist = false;
	item->isnull = isnull;

	return item;
}

static void
spgInnerTest(SpGistScanOpaque so, SpGistSearchItem *item,
			 SpGistInnerTuple innerTuple, bool isnull)
{
	MemoryContext			oldCxt = MemoryContextSwitchTo(so->tempCxt);
	spgInnerConsistentOut	out;
	int						nNodes = innerTuple->nNodes;
	int						i;

	memset(&out, 0, sizeof(out));

	if (!isnull)
	{
		spgInnerConsistentIn in;

		spgInitInnerConsistentIn(&in, so, item, innerTuple, oldCxt);

		/* use user-defined inner consistent method */
		FunctionCall2Coll(&so->innerConsistentFn,
						  so->indexCollation,
						  PointerGetDatum(&in),
						  PointerGetDatum(&out));
	}
	else
	{
		/* force all children to be visited */
		out.nNodes = nNodes;
		out.nodeNumbers = (int *) palloc(sizeof(int) * nNodes);
		for (i = 0; i < nNodes; i++)
			out.nodeNumbers[i] = i;
	}

	/* If allTheSame, they should all or none of 'em match */
	if (innerTuple->allTheSame)
		if (out.nNodes != 0 && out.nNodes != nNodes)
			elog(ERROR, "inconsistent inner_consistent results for allTheSame inner tuple");

	if (out.nNodes)
	{
		/* collect node pointers */
		SpGistNodeTuple		node;
		SpGistNodeTuple	   *nodes = (SpGistNodeTuple *) palloc(
									sizeof(SpGistNodeTuple) * nNodes);

		SGITITERATE(innerTuple, i, node)
		{
			nodes[i] = node;
		}

		MemoryContextSwitchTo(so->queueCxt);

		for (i = 0; i < out.nNodes; i++)
		{
			int			nodeN = out.nodeNumbers[i];
			SpGistSearchItem *innerItem;

			Assert(nodeN >= 0 && nodeN < nNodes);

			node = nodes[nodeN];

			if (!ItemPointerIsValid(&node->t_tid))
				continue;

			innerItem = spgMakeInnerItem(so, item, node, &out, i, isnull);

			/* Will copy out the distances in spgAddSearchItemToQueue anyway */
			spgAddSearchItemToQueue(so, innerItem,
									out.distances ? out.distances[i]
												  : so->infDistances);
		}
	}

	MemoryContextSwitchTo(oldCxt);
}

/* Returns a next item in an (ordered) scan or null if the index is exhausted */
static SpGistSearchItem *
spgGetNextQueueItem(SpGistScanOpaque so)
{
	SpGistSearchItem *item;

	if (so->queue)
	{
		if (pairingheap_is_empty(so->queue))
			return NULL;	/* Done when both heaps are empty */

		item = (SpGistSearchItem *) pairingheap_remove_first(so->queue);
	}
	else
	{
		if (!so->scanStack)
			return NULL;	/* there are no more items to scan */

		item = (SpGistSearchItem *) linitial(so->scanStack);
		so->scanStack = list_delete_first(so->scanStack);
	}

	/* Return item; caller is responsible to pfree it */
	return item;
}

enum SpGistSpecialOffsetNumbers
{
	SpGistBreakOffsetNumber = InvalidOffsetNumber,
	SpGistRedirectOffsetNumber = MaxOffsetNumber + 1,
	SpGistErrorOffsetNumber = MaxOffsetNumber + 2
};

static OffsetNumber
spgTestLeafTuple(SpGistScanOpaque so,
				 SpGistSearchItem *item,
				 Page page, OffsetNumber offset,
				 bool isnull, bool isroot,
				 bool *reportedSome,
				 storeRes_func storeRes)
{
	SpGistLeafTuple leafTuple = (SpGistLeafTuple)
		PageGetItem(page, PageGetItemId(page, offset));

	if (leafTuple->tupstate != SPGIST_LIVE)
	{
		if (!isroot) /* all tuples on root should be live */
		{
			if (leafTuple->tupstate == SPGIST_REDIRECT)
			{
				/* redirection tuple should be first in chain */
				Assert(offset == ItemPointerGetOffsetNumber(&item->heap));
				/* transfer attention to redirect point */
				item->heap = ((SpGistDeadTuple) leafTuple)->pointer;
				Assert(ItemPointerGetBlockNumber(&item->heap) != SPGIST_METAPAGE_BLKNO);
				return SpGistRedirectOffsetNumber;
			}

			if (leafTuple->tupstate == SPGIST_DEAD)
			{
				/* dead tuple should be first in chain */
				Assert(offset == ItemPointerGetOffsetNumber(&item->heap));
				/* No live entries on this page */
				Assert(leafTuple->nextOffset == InvalidOffsetNumber);
				return SpGistBreakOffsetNumber;
			}
		}

		/* We should not arrive at a placeholder */
		elog(ERROR, "unexpected SPGiST tuple state: %d", leafTuple->tupstate);
		return SpGistErrorOffsetNumber;
	}

	Assert(ItemPointerIsValid(&leafTuple->heapPtr));

	spgLeafTest(so, item, leafTuple, isnull, reportedSome, storeRes);

	return leafTuple->nextOffset;
}

/*
 * Walk the tree and report all tuples passing the scan quals to the storeRes
 * subroutine.
 *
 * If scanWholeIndex is true, we'll do just that.  If not, we'll stop at the
 * next page boundary once we have reported at least one tuple.
 */
static void
spgWalk(Relation index, SpGistScanOpaque so, bool scanWholeIndex,
		storeRes_func storeRes, Snapshot snapshot)
{
	Buffer		buffer = InvalidBuffer;
	bool		reportedSome = false;

	while (scanWholeIndex || !reportedSome)
	{
		SpGistSearchItem *item = spgGetNextQueueItem(so);

		if (item == NULL)
			break; /* No more items in queue -> done */

redirect:
		/* Check for interrupts, just in case of infinite loop */
		CHECK_FOR_INTERRUPTS();

		if (item->isLeaf)
		{
			/* We store heap items in the queue only in case of ordered search */
			Assert(so->numberOfOrderBys > 0);
			storeRes(so, &item->heap, item->value, item->isnull,
					 item->recheckQual, item->recheckDist, item->distances);
			reportedSome = true;
		}
		else
		{
			BlockNumber		blkno  = ItemPointerGetBlockNumber(&item->heap);
			OffsetNumber	offset = ItemPointerGetOffsetNumber(&item->heap);
			Page			page;
			bool			isnull;

			if (buffer == InvalidBuffer)
			{
				buffer = ReadBuffer(index, blkno);
				LockBuffer(buffer, BUFFER_LOCK_SHARE);
			}
			else if (blkno != BufferGetBlockNumber(buffer))
			{
				UnlockReleaseBuffer(buffer);
				buffer = ReadBuffer(index, blkno);
				LockBuffer(buffer, BUFFER_LOCK_SHARE);
			}

			/* else new pointer points to the same page, no work needed */

			page = BufferGetPage(buffer);
			TestForOldSnapshot(snapshot, index, page);

			isnull = SpGistPageStoresNulls(page) ? true : false;

			if (SpGistPageIsLeaf(page))
			{
				/* Page is a leaf - that is, all it's tuples are heap items */
				OffsetNumber max = PageGetMaxOffsetNumber(page);

				if (SpGistBlockIsRoot(blkno))
				{
					/* When root is a leaf, examine all its tuples */
					for (offset = FirstOffsetNumber; offset <= max; offset++)
						(void) spgTestLeafTuple(so, item, page, offset,
												isnull, true,
												&reportedSome, storeRes);
				}
				else
				{
					/* Normal case: just examine the chain we arrived at */
					while (offset != InvalidOffsetNumber)
					{
						Assert(offset >= FirstOffsetNumber && offset <= max);
						offset = spgTestLeafTuple(so, item, page, offset,
												  isnull, false,
												  &reportedSome, storeRes);
						if (offset == SpGistRedirectOffsetNumber)
							goto redirect;
					}
				}
			}
			else	/* page is inner */
			{
				SpGistInnerTuple innerTuple = (SpGistInnerTuple)
						PageGetItem(page, PageGetItemId(page, offset));

				if (innerTuple->tupstate != SPGIST_LIVE)
				{
					if (innerTuple->tupstate == SPGIST_REDIRECT)
					{
						/* transfer attention to redirect point */
						item->heap = ((SpGistDeadTuple) innerTuple)->pointer;
						Assert(ItemPointerGetBlockNumber(&item->heap) !=
							   SPGIST_METAPAGE_BLKNO);
						goto redirect;
					}
					elog(ERROR, "unexpected SPGiST tuple state: %d",
						 innerTuple->tupstate);
				}

				spgInnerTest(so, item, innerTuple, isnull);
			}
		}

		/* done with this scan item */
		spgFreeSearchItem(so, item);
		/* clear temp context before proceeding to the next one */
		MemoryContextReset(so->tempCxt);
	}

	if (buffer != InvalidBuffer)
		UnlockReleaseBuffer(buffer);
}


/* storeRes subroutine for getbitmap case */
static void
storeBitmap(SpGistScanOpaque so, ItemPointer heapPtr,
			Datum leafValue, bool isnull, bool recheck, bool recheckDist,
			double *distances)
{
	Assert(!recheckDist && !distances);
	tbm_add_tuples(so->tbm, heapPtr, 1, recheck);
	so->ntids++;
}

int64
spggetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	SpGistScanOpaque so = (SpGistScanOpaque) scan->opaque;

	/* Copy want_itup to *so so we don't need to pass it around separately */
	so->want_itup = false;

	so->tbm = tbm;
	so->ntids = 0;

	spgWalk(scan->indexRelation, so, true, storeBitmap, scan->xs_snapshot);

	return so->ntids;
}

/* storeRes subroutine for gettuple case */
static void
storeGettuple(SpGistScanOpaque so, ItemPointer heapPtr,
			  Datum leafValue, bool isnull, bool recheck, bool recheckDist,
			  double *distances)
{
	Assert(so->nPtrs < MaxIndexTuplesPerPage);
	so->heapPtrs[so->nPtrs] = *heapPtr;
	so->recheck[so->nPtrs] = recheck;
	so->recheckDist[so->nPtrs] = recheckDist;

	if (so->numberOfOrderBys > 0)
	{
		if (isnull)
			so->distances[so->nPtrs] = NULL;
		else
		{
			Size		size = sizeof(double) * so->numberOfOrderBys;

			so->distances[so->nPtrs] = memcpy(palloc(size), distances, size);
		}
	}

	if (so->want_itup)
	{
		/*
		 * Reconstruct index data.  We have to copy the datum out of the temp
		 * context anyway, so we may as well create the tuple here.
		 */
		so->reconTups[so->nPtrs] = heap_form_tuple(so->indexTupDesc,
												   &leafValue,
												   &isnull);
	}
	so->nPtrs++;
}

bool
spggettuple(IndexScanDesc scan, ScanDirection dir)
{
	SpGistScanOpaque so = (SpGistScanOpaque) scan->opaque;

	if (dir != ForwardScanDirection)
		elog(ERROR, "SP-GiST only supports forward scan direction");

	/* Copy want_itup to *so so we don't need to pass it around separately */
	so->want_itup = scan->xs_want_itup;

	for (;;)
	{
		if (so->iPtr < so->nPtrs)
		{
			/* continuing to return reported tuples */
			scan->xs_ctup.t_self = so->heapPtrs[so->iPtr];
			scan->xs_recheck = so->recheck[so->iPtr];
			scan->xs_hitup = so->reconTups[so->iPtr];

			if (so->numberOfOrderBys > 0)
				index_store_float8_orderby_distances(scan, so->orderByTypes,
													 so->distances[so->iPtr],
													 so->recheckDist[so->iPtr]);
			so->iPtr++;
			return true;
		}

		if (so->numberOfOrderBys > 0)
		{
			/* Must pfree distances to avoid memory leak */
			int			i;

			for (i = 0; i < so->nPtrs; i++)
				if (so->distances[i])
					pfree(so->distances[i]);
		}

		if (so->want_itup)
		{
			/* Must pfree reconstructed tuples to avoid memory leak */
			int			i;

			for (i = 0; i < so->nPtrs; i++)
				pfree(so->reconTups[i]);
		}
		so->iPtr = so->nPtrs = 0;

		spgWalk(scan->indexRelation, so, false, storeGettuple,
				scan->xs_snapshot);

		if (so->nPtrs == 0)
			break;				/* must have completed scan */
	}

	return false;
}

bool
spgcanreturn(Relation index, int attno)
{
	SpGistCache *cache;

	/* We can do it if the opclass config function says so */
	cache = spgGetCache(index);

	return cache->config.canReturnData;
}
