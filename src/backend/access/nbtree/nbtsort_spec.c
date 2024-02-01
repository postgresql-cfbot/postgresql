/*-------------------------------------------------------------------------
 *
 * nbtsort_spec.c
 *	  Index shape-specialized functions for nbtsort.c
 *
 * NOTES
 *	  See also: access/nbtree/README section "nbtree specialization"
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtsort_spec.c
 *
 *-------------------------------------------------------------------------
 */

#define _bt_load NBTS_FUNCTION(_bt_load)

static void _bt_load(BTWriteState *wstate,
					 BTSpool *btspool, BTSpool *btspool2);

/*
 * Read tuples in correct sort order from tuplesort, and load them into
 * btree leaves.
 */
static void
_bt_load(BTWriteState *wstate, BTSpool *btspool, BTSpool *btspool2)
{
	BTPageState *state = NULL;
	bool		merge = (btspool2 != NULL);
	IndexTuple	itup,
				itup2 = NULL;
	bool		load1;
	TupleDesc	tupdes = RelationGetDescr(wstate->index);
	int			keysz = IndexRelationGetNumberOfKeyAttributes(wstate->index);
	SortSupport sortKeys;
	int64		tuples_done = 0;
	bool		deduplicate;

	deduplicate = wstate->inskey->allequalimage && !btspool->isunique &&
				  BTGetDeduplicateItems(wstate->index);

	if (merge)
	{
		/*
		 * Another BTSpool for dead tuples exists. Now we have to merge
		 * btspool and btspool2.
		 */

		/* the preparation of merge */
		itup = tuplesort_getindextuple(btspool->sortstate, true);
		itup2 = tuplesort_getindextuple(btspool2->sortstate, true);

		/* Prepare SortSupport data for each column */
		sortKeys = (SortSupport) palloc0(keysz * sizeof(SortSupportData));

		for (int i = 0; i < keysz; i++)
		{
			SortSupport sortKey = sortKeys + i;
			ScanKey		scanKey = wstate->inskey->scankeys + i;
			int16		strategy;

			sortKey->ssup_cxt = CurrentMemoryContext;
			sortKey->ssup_collation = scanKey->sk_collation;
			sortKey->ssup_nulls_first =
				(scanKey->sk_flags & SK_BT_NULLS_FIRST) != 0;
			sortKey->ssup_attno = scanKey->sk_attno;
			/* Abbreviation is not supported here */
			sortKey->abbreviate = false;

			Assert(sortKey->ssup_attno != 0);

			strategy = (scanKey->sk_flags & SK_BT_DESC) != 0 ?
					   BTGreaterStrategyNumber : BTLessStrategyNumber;

			PrepareSortSupportFromIndexRel(wstate->index, strategy, sortKey);
		}

		for (;;)
		{
			load1 = true;		/* load BTSpool next ? */
			if (itup2 == NULL)
			{
				if (itup == NULL)
					break;
			}
			else if (itup != NULL)
			{
				int32		compare = 0;
				nbts_attiterdeclare(itup);
				nbts_attiterdeclare(itup2);

				nbts_attiterinit(itup, 1, tupdes);
				nbts_attiterinit(itup2, 1, tupdes);

				nbts_foreachattr(1, keysz)
				{
					SortSupport entry;
					Datum		attrDatum1,
								attrDatum2;

					entry = sortKeys + nbts_attiter_attnum - 1;
					attrDatum1 = nbts_attiter_nextattdatum(itup, tupdes);
					attrDatum2 = nbts_attiter_nextattdatum(itup2, tupdes);

					compare = ApplySortComparator(attrDatum1, nbts_attiter_curattisnull(itup),
												  attrDatum2, nbts_attiter_curattisnull(itup2),
												  entry);
					if (compare > 0)
					{
						load1 = false;
						break;
					}
					else if (compare < 0)
						break;
				}

				/*
				 * If key values are equal, we sort on ItemPointer.  This is
				 * required for btree indexes, since heap TID is treated as an
				 * implicit last key attribute in order to ensure that all
				 * keys in the index are physically unique.
				 */
				if (compare == 0)
				{
					compare = ItemPointerCompare(&itup->t_tid, &itup2->t_tid);
					Assert(compare != 0);
					if (compare > 0)
						load1 = false;
				}
			}
			else
				load1 = false;

			/* When we see first tuple, create first index page */
			if (state == NULL)
				state = _bt_pagestate(wstate, 0);

			if (load1)
			{
				_bt_buildadd(wstate, state, itup, 0);
				itup = tuplesort_getindextuple(btspool->sortstate, true);
			}
			else
			{
				_bt_buildadd(wstate, state, itup2, 0);
				itup2 = tuplesort_getindextuple(btspool2->sortstate, true);
			}

			/* Report progress */
			pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE,
										 ++tuples_done);
		}
		pfree(sortKeys);
	}
	else if (deduplicate)
	{
		/* merge is unnecessary, deduplicate into posting lists */
		BTDedupState dstate;

		dstate = (BTDedupState) palloc(sizeof(BTDedupStateData));
		dstate->deduplicate = true; /* unused */
		dstate->nmaxitems = 0;	/* unused */
		dstate->maxpostingsize = 0; /* set later */
		/* Metadata about base tuple of current pending posting list */
		dstate->base = NULL;
		dstate->baseoff = InvalidOffsetNumber;	/* unused */
		dstate->basetupsize = 0;
		/* Metadata about current pending posting list TIDs */
		dstate->htids = NULL;
		dstate->nhtids = 0;
		dstate->nitems = 0;
		dstate->phystupsize = 0;	/* unused */
		dstate->nintervals = 0; /* unused */

		while ((itup = tuplesort_getindextuple(btspool->sortstate,
											   true)) != NULL)
		{
			/* When we see first tuple, create first index page */
			if (state == NULL)
			{
				state = _bt_pagestate(wstate, 0);

				/*
				 * Limit size of posting list tuples to 1/10 space we want to
				 * leave behind on the page, plus space for final item's line
				 * pointer.  This is equal to the space that we'd like to
				 * leave behind on each leaf page when fillfactor is 90,
				 * allowing us to get close to fillfactor% space utilization
				 * when there happen to be a great many duplicates.  (This
				 * makes higher leaf fillfactor settings ineffective when
				 * building indexes that have many duplicates, but packing
				 * leaf pages full with few very large tuples doesn't seem
				 * like a useful goal.)
				 */
				dstate->maxpostingsize = MAXALIGN_DOWN((BLCKSZ * 10 / 100)) -
										 sizeof(ItemIdData);
				Assert(dstate->maxpostingsize <= BTMaxItemSize(state->btps_page) &&
					   dstate->maxpostingsize <= INDEX_SIZE_MASK);
				dstate->htids = palloc(dstate->maxpostingsize);

				/* start new pending posting list with itup copy */
				_bt_dedup_start_pending(dstate, CopyIndexTuple(itup),
										InvalidOffsetNumber);
			}
			else if (_bt_keep_natts_fast(wstate->index, dstate->base,
										 itup) > keysz &&
					 _bt_dedup_save_htid(dstate, itup))
			{
				/*
				 * Tuple is equal to base tuple of pending posting list.  Heap
				 * TID from itup has been saved in state.
				 */
			}
			else
			{
				/*
				 * Tuple is not equal to pending posting list tuple, or
				 * _bt_dedup_save_htid() opted to not merge current item into
				 * pending posting list.
				 */
				_bt_sort_dedup_finish_pending(wstate, state, dstate);
				pfree(dstate->base);

				/* start new pending posting list with itup copy */
				_bt_dedup_start_pending(dstate, CopyIndexTuple(itup),
										InvalidOffsetNumber);
			}

			/* Report progress */
			pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE,
										 ++tuples_done);
		}

		if (state)
		{
			/*
			 * Handle the last item (there must be a last item when the
			 * tuplesort returned one or more tuples)
			 */
			_bt_sort_dedup_finish_pending(wstate, state, dstate);
			pfree(dstate->base);
			pfree(dstate->htids);
		}

		pfree(dstate);
	}
	else
	{
		/* merging and deduplication are both unnecessary */
		while ((itup = tuplesort_getindextuple(btspool->sortstate,
											   true)) != NULL)
		{
			/* When we see first tuple, create first index page */
			if (state == NULL)
				state = _bt_pagestate(wstate, 0);

			_bt_buildadd(wstate, state, itup, 0);

			/* Report progress */
			pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE,
										 ++tuples_done);
		}
	}

	/* Close down final pages and write the metapage */
	_bt_uppershutdown(wstate, state);

	/*
	 * When we WAL-logged index pages, we must nonetheless fsync index files.
	 * Since we're building outside shared buffers, a CHECKPOINT occurring
	 * during the build has no way to flush the previously written data to
	 * disk (indeed it won't know the index even exists).  A crash later on
	 * would replay WAL from the checkpoint, therefore it wouldn't replay our
	 * earlier WAL entries. If we do not fsync those pages here, they might
	 * still not be on disk when the crash occurs.
	 */
	if (wstate->btws_use_wal)
		smgrimmedsync(RelationGetSmgr(wstate->index), MAIN_FORKNUM);
}
