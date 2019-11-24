/*-------------------------------------------------------------------------
 *
 * ginscan.c
 *	  routines to manage scans of inverted index relations
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/gin/ginscan.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/gin_private.h"
#include "access/relscan.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "utils/rel.h"


IndexScanDesc
ginbeginscan(Relation rel, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	GinScanOpaque so;

	/* no order by operators allowed */
	Assert(norderbys == 0);

	scan = RelationGetIndexScan(rel, nkeys, norderbys);

	/* allocate private workspace */
	so = (GinScanOpaque) palloc(sizeof(GinScanOpaqueData));
	so->keys = NULL;
	so->nkeys = 0;
	so->tempCtx = AllocSetContextCreate(CurrentMemoryContext,
										"Gin scan temporary context",
										ALLOCSET_DEFAULT_SIZES);
	so->keyCtx = AllocSetContextCreate(CurrentMemoryContext,
									   "Gin scan key context",
									   ALLOCSET_DEFAULT_SIZES);
	initGinState(&so->ginstate, scan->indexRelation);

	scan->opaque = so;

	return scan;
}

/*
 * Create a new GinScanEntry, unless an equivalent one already exists,
 * in which case just return it
 */
static GinScanEntry
ginFillScanEntry(GinScanOpaque so, OffsetNumber attnum,
				 StrategyNumber strategy, int32 searchMode,
				 Datum queryKey, GinNullCategory queryCategory,
				 bool isPartialMatch, Pointer extra_data)
{
	GinState   *ginstate = &so->ginstate;
	GinScanEntry scanEntry;
	uint32		i;

	/*
	 * Look for an existing equivalent entry.
	 *
	 * Entries with non-null extra_data are never considered identical, since
	 * we can't know exactly what the opclass might be doing with that.
	 */
	if (extra_data == NULL)
	{
		for (i = 0; i < so->totalentries; i++)
		{
			GinScanEntry prevEntry = so->entries[i];

			if (prevEntry->extra_data == NULL &&
				prevEntry->isPartialMatch == isPartialMatch &&
				prevEntry->strategy == strategy &&
				prevEntry->searchMode == searchMode &&
				prevEntry->attnum == attnum &&
				ginCompareEntries(ginstate, attnum,
								  prevEntry->queryKey,
								  prevEntry->queryCategory,
								  queryKey,
								  queryCategory) == 0)
			{
				/* Successful match */
				return prevEntry;
			}
		}
	}

	/* Nope, create a new entry */
	scanEntry = (GinScanEntry) palloc(sizeof(GinScanEntryData));
	scanEntry->queryKey = queryKey;
	scanEntry->queryCategory = queryCategory;
	scanEntry->isPartialMatch = isPartialMatch;
	scanEntry->extra_data = extra_data;
	scanEntry->strategy = strategy;
	scanEntry->searchMode = searchMode;
	scanEntry->attnum = attnum;

	scanEntry->buffer = InvalidBuffer;
	ItemPointerSetMin(&scanEntry->curItem);
	scanEntry->matchBitmap = NULL;
	scanEntry->matchIterator = NULL;
	scanEntry->matchResult = NULL;
	scanEntry->list = NULL;
	scanEntry->nlist = 0;
	scanEntry->offset = InvalidOffsetNumber;
	scanEntry->isFinished = false;
	scanEntry->reduceResult = false;

	/* Add it to so's array */
	if (so->totalentries >= so->allocentries)
	{
		so->allocentries *= 2;
		so->entries = (GinScanEntry *)
			repalloc(so->entries, so->allocentries * sizeof(GinScanEntry));
	}
	so->entries[so->totalentries++] = scanEntry;

	return scanEntry;
}

/*
 * Initialize the next GinScanKey using the output from the extractQueryFn
 */
static void
ginFillScanKey(GinScanOpaque so, GinScanKey	key, bool initHiddenEntries,
			   OffsetNumber attnum, StrategyNumber strategy, int32 searchMode,
			   Datum query, uint32 nQueryValues,
			   Datum *queryValues, GinNullCategory *queryCategories,
			   bool *partial_matches, Pointer *extra_data)
{
	GinState   *ginstate = &so->ginstate;
	uint32		nUserQueryValues = nQueryValues;
	uint32		nAllocatedQueryValues = nQueryValues;
	uint32		i;

	if (key == NULL)
		key = &(so->keys[so->nkeys++]);

	/*
	 * Non-default search modes add one "hidden" entry to each key, but this
	 * entry is initialized only if requested by initHiddenEntries flag.
	 */
	if (searchMode != GIN_SEARCH_MODE_DEFAULT)
	{
		nAllocatedQueryValues++;

		if (initHiddenEntries)
			nQueryValues++;
	}

	key->nentries = nQueryValues;
	key->nuserentries = nUserQueryValues;

	key->scanEntry = (GinScanEntry *) palloc(sizeof(GinScanEntry) * nAllocatedQueryValues);
	key->entryRes = (GinTernaryValue *) palloc0(sizeof(GinTernaryValue) * nAllocatedQueryValues);

	key->query = query;
	key->queryValues = queryValues;
	key->queryCategories = queryCategories;
	key->extra_data = extra_data;
	key->strategy = strategy;
	key->searchMode = searchMode;
	key->attnum = attnum;
	key->includeNonMatching = false;
	key->recheckNonMatching = false;

	ItemPointerSetMin(&key->curItem);
	key->curItemMatches = false;
	key->recheckCurItem = false;
	key->isFinished = false;
	key->nrequired = 0;
	key->nadditional = 0;
	key->requiredEntries = NULL;
	key->additionalEntries = NULL;

	ginInitConsistentFunction(ginstate, key);

	for (i = 0; i < nQueryValues; i++)
	{
		Datum		queryKey;
		GinNullCategory queryCategory;
		bool		isPartialMatch;
		Pointer		this_extra;

		if (i < nUserQueryValues)
		{
			/* set up normal entry using extractQueryFn's outputs */
			queryKey = queryValues[i];
			queryCategory = queryCategories[i];
			isPartialMatch =
				(ginstate->canPartialMatch[attnum - 1] && partial_matches)
				? partial_matches[i] : false;
			this_extra = (extra_data) ? extra_data[i] : NULL;
		}
		else
		{
			/* set up hidden entry */
			queryKey = (Datum) 0;
			switch (searchMode)
			{
				case GIN_SEARCH_MODE_INCLUDE_EMPTY:
					queryCategory = GIN_CAT_EMPTY_ITEM;
					break;
				case GIN_SEARCH_MODE_ALL:
					queryCategory = GIN_CAT_EMPTY_QUERY;
					break;
				case GIN_SEARCH_MODE_EVERYTHING:
					queryCategory = GIN_CAT_EMPTY_QUERY;
					break;
				case GIN_SEARCH_MODE_NOT_NULL:
					queryCategory = GIN_CAT_EMPTY_QUERY;
					/* use GIN_SEARCH_MODE_ALL to skip NULLs */
					searchMode = GIN_SEARCH_MODE_ALL;
					break;
				default:
					elog(ERROR, "unexpected searchMode: %d", searchMode);
					queryCategory = 0;	/* keep compiler quiet */
					break;
			}
			isPartialMatch = false;
			this_extra = NULL;

			/*
			 * We set the strategy to a fixed value so that ginFillScanEntry
			 * can combine these entries for different scan keys.  This is
			 * safe because the strategy value in the entry struct is only
			 * used for partial-match cases.  It's OK to overwrite our local
			 * variable here because this is the last loop iteration.
			 */
			strategy = InvalidStrategy;
		}

		key->scanEntry[i] = ginFillScanEntry(so, attnum,
											 strategy, searchMode,
											 queryKey, queryCategory,
											 isPartialMatch, this_extra);
	}
}

/*
 * Release current scan keys, if any.
 */
void
ginFreeScanKeys(GinScanOpaque so)
{
	uint32		i;

	if (so->keys == NULL)
		return;

	for (i = 0; i < so->totalentries; i++)
	{
		GinScanEntry entry = so->entries[i];

		if (entry->buffer != InvalidBuffer)
			ReleaseBuffer(entry->buffer);
		if (entry->list)
			pfree(entry->list);
		if (entry->matchIterator)
			tbm_end_iterate(entry->matchIterator);
		if (entry->matchBitmap)
			tbm_free(entry->matchBitmap);
	}

	MemoryContextResetAndDeleteChildren(so->keyCtx);

	so->keys = NULL;
	so->nkeys = 0;
	so->entries = NULL;
	so->totalentries = 0;
}

void
ginNewScanKey(IndexScanDesc scan)
{
	ScanKey		scankey = scan->keyData;
	GinScanOpaque so = (GinScanOpaque) scan->opaque;
	int			i;
	bool		hasNullQuery = false;
	bool		hasAllQuery = false;
	bool		hasRegularQuery = false;
	/*
	 * GIN_FALSE - column has no search keys
	 * GIN_TRUE  - NOT NULL is implied by some non-empty search key
	 * GIN_MAYBE - NOT NULL is implied by some empty ALL key
	 */
	GinTernaryValue	colNotNull[INDEX_MAX_KEYS] = {0};
	/* Number of GIN_MAYBE values in colNotNull[]  */
	int			colNotNullCount = 0;
	MemoryContext oldCtx;

	/*
	 * Allocate all the scan key information in the key context. (If
	 * extractQuery leaks anything there, it won't be reset until the end of
	 * scan or rescan, but that's OK.)
	 */
	oldCtx = MemoryContextSwitchTo(so->keyCtx);

	/* if no scan keys provided, allocate extra EVERYTHING GinScanKey */
	so->keys = (GinScanKey)
		palloc(Max(scan->numberOfKeys, 1) * sizeof(GinScanKeyData));
	so->nkeys = 0;

	/* initialize expansible array of GinScanEntry pointers */
	so->totalentries = 0;
	so->allocentries = 32;
	so->entries = (GinScanEntry *)
		palloc(so->allocentries * sizeof(GinScanEntry));

	so->isVoidRes = false;
	so->forcedRecheck = false;

	for (i = 0; i < scan->numberOfKeys; i++)
	{
		ScanKey		skey = &scankey[i];
		Datum	   *queryValues;
		int32		nQueryValues = 0;
		bool	   *partial_matches = NULL;
		Pointer    *extra_data = NULL;
		bool	   *nullFlags = NULL;
		GinNullCategory *categories;
		int32		searchMode = GIN_SEARCH_MODE_DEFAULT;
		int			colno = skey->sk_attno - 1;

		/*
		 * We assume that GIN-indexable operators are strict, so a null query
		 * argument means an unsatisfiable query.
		 */
		if (skey->sk_flags & SK_ISNULL)
		{
			so->isVoidRes = true;
			break;
		}

		/* OK to call the extractQueryFn */
		queryValues = (Datum *)
			DatumGetPointer(FunctionCall7Coll(&so->ginstate.extractQueryFn[colno],
											  so->ginstate.supportCollation[colno],
											  skey->sk_argument,
											  PointerGetDatum(&nQueryValues),
											  UInt16GetDatum(skey->sk_strategy),
											  PointerGetDatum(&partial_matches),
											  PointerGetDatum(&extra_data),
											  PointerGetDatum(&nullFlags),
											  PointerGetDatum(&searchMode)));

		/*
		 * If bogus searchMode is returned, treat as GIN_SEARCH_MODE_ALL; note
		 * in particular we don't allow extractQueryFn to select
		 * GIN_SEARCH_MODE_EVERYTHING.
		 */
		if (searchMode < GIN_SEARCH_MODE_DEFAULT ||
			searchMode > GIN_SEARCH_MODE_ALL)
			searchMode = GIN_SEARCH_MODE_ALL;

		/* Non-default modes require the index to have placeholders */
		if (searchMode != GIN_SEARCH_MODE_DEFAULT)
			hasNullQuery = true;

		/* Special cases for queries that contain no keys */
		if (queryValues == NULL || nQueryValues <= 0)
		{
			if (searchMode == GIN_SEARCH_MODE_DEFAULT)
			{
				/* In default mode, no keys means an unsatisfiable query */
				so->isVoidRes = true;
				break;
			}

			nQueryValues = 0;	/* ensure sane value */
		}

		if (searchMode == GIN_SEARCH_MODE_ALL)
		{
			hasAllQuery = true;

			/*
			 * Increment the number of columns with NOT NULL constraints
			 * if NOT NULL is not yet implied by some non-ALL key.
			 */
			if (colNotNull[colno] == GIN_FALSE)
			{
				colNotNull[colno] = GIN_MAYBE;
				colNotNullCount++;
			}

			if (!nQueryValues)
			{
				/*
				 * The query probably matches all non-null items, but rather
				 * than scanning the index in ALL mode, we use forced rechecks
				 * to verify matches of this scankey.  This wins if there are
				 * any non-ALL scankeys; otherwise we end up adding a NOT_NULL
				 * scankey below.
				 */
				GinScanKeyData key;
				GinTernaryValue res;

				/* Check whether unconditional recheck is needed. */
				ginFillScanKey(so, &key, false, skey->sk_attno,
							   skey->sk_strategy, searchMode,
							   skey->sk_argument, 0,
							   NULL, NULL, NULL, NULL);

				res = key.triConsistentFn(&key);

				if (res == GIN_FALSE)
				{
					so->isVoidRes = true;	/* unsatisfiable query */
					break;
				}

				so->forcedRecheck |= res != GIN_TRUE;
				continue;
			}
		}
		else
		{
			hasRegularQuery = true;

			/*
			 * Current key implies that column is NOT NULL, so decrement the
			 * number of columns with NOT NULL constraints.
			 */
			if (colNotNull[colno] == GIN_MAYBE)
				colNotNullCount--;

			colNotNull[colno] = GIN_TRUE;
		}

		/*
		 * Create GinNullCategory representation.  If the extractQueryFn
		 * didn't create a nullFlags array, we assume everything is non-null.
		 * While at it, detect whether any null keys are present.
		 */
		categories = (GinNullCategory *) palloc0(nQueryValues * sizeof(GinNullCategory));
		if (nullFlags)
		{
			int32		j;

			for (j = 0; j < nQueryValues; j++)
			{
				if (nullFlags[j])
				{
					categories[j] = GIN_CAT_NULL_KEY;
					hasNullQuery = true;
				}
			}
		}

		ginFillScanKey(so, NULL,
					   /* postpone initialization of hidden ALL entry */
					   searchMode != GIN_SEARCH_MODE_ALL,
					   skey->sk_attno,
					   skey->sk_strategy, searchMode,
					   skey->sk_argument, nQueryValues,
					   queryValues, categories,
					   partial_matches, extra_data);
	}

	if (!so->isVoidRes)
	{
		/*
		 * If there are no regular scan keys, generate one EVERYTHING or
		 * several NOT_NULL scankeys to drive a full-index scan.
		 */
		if (so->nkeys == 0)
		{
			hasNullQuery = true;

			/* Initialize EVERYTHING key if there are no non-NULL columns. */
			if (!colNotNullCount)
			{
				ginFillScanKey(so, NULL, true, FirstOffsetNumber,
							   InvalidStrategy, GIN_SEARCH_MODE_EVERYTHING,
							   (Datum) 0, 0, NULL, NULL, NULL, NULL);
			}
			else
			{
				/* Initialize NOT_NULL key for each non-NULL column. */
				for (i = 0; i < scan->indexRelation->rd_att->natts; i++)
				{
					if (colNotNull[i] == GIN_MAYBE)
						ginFillScanKey(so, NULL, true, i + 1, InvalidStrategy,
									   GIN_SEARCH_MODE_NOT_NULL, (Datum) 0, 0,
									   NULL, NULL, NULL, NULL);
				}
			}
		}
		else
		{
			if (hasAllQuery)
			{
				GinScanKey	nonAllKey = NULL;
				int			nkeys = so->nkeys;
				int			i = 0;

				/*
				 * Finalize non-empty ALL keys: if we have regular keys, enable
				 * includeNonMatching mode in ALL keys, otherwise initialize
				 * previously skipped hidden ALL entries.
				 */
				for (i = 0; i < nkeys; i++)
				{
					GinScanKey	key = &so->keys[i];

					if (key->searchMode != GIN_SEARCH_MODE_ALL)
					{
						nonAllKey = key;
					}
					else if (hasRegularQuery)
					{
						memset(key->entryRes, GIN_FALSE, key->nentries);

						switch (key->triConsistentFn(key))
						{
							case GIN_TRUE:
								key->includeNonMatching = true;
								key->recheckNonMatching = false;
								break;
							case GIN_MAYBE:
								key->includeNonMatching = true;
								key->recheckNonMatching = true;
							default:
								/*
								 * Items with no matching entries are not
								 * accepted, leave the key as is.
								 */
								break;
						}
					}
					else
					{
						/* Initialize missing hidden ALL entry */
						key->scanEntry[key->nentries++] =
							ginFillScanEntry(so, key->attnum, key->strategy,
											 GIN_SEARCH_MODE_ALL, (Datum) 0,
											 GIN_CAT_EMPTY_QUERY, false, NULL);

						/*
						 * ALL entry implies NOT NULL, so update the number of
						 * NOT NULL columns.
						 */
						if (colNotNull[key->attnum - 1] == GIN_MAYBE)
						{
							colNotNull[key->attnum - 1] = GIN_TRUE;
							colNotNullCount--;
						}
					}
				}

				/* Move some non-ALL key to the beginning (see scanGetItem()) */
				if (so->keys[0].includeNonMatching)
				{
					GinScanKeyData tmp = so->keys[0];

					Assert(nonAllKey);

					so->keys[0] = *nonAllKey;
					*nonAllKey = tmp;
				}
			}

			if (colNotNullCount > 0)
			{
				/*
				 * We use recheck instead of adding NOT_NULL entries to
				 * eliminate rows with NULL columns.
				 */
				so->forcedRecheck = true;
			}
		}

		/*
		 * If the index is version 0, it may be missing null and placeholder
		 * entries, which would render searches for nulls and full-index scans
		 * unreliable.  Throw an error if so.
		 */
		if (hasNullQuery)
		{
			GinStatsData ginStats;

			ginGetStats(scan->indexRelation, &ginStats);
			if (ginStats.ginVersion < 1)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("old GIN indexes do not support whole-index scans nor searches for nulls"),
						 errhint("To fix this, do REINDEX INDEX \"%s\".",
								 RelationGetRelationName(scan->indexRelation))));
		}
	}

	MemoryContextSwitchTo(oldCtx);

	pgstat_count_index_scan(scan->indexRelation);
}

void
ginrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		  ScanKey orderbys, int norderbys)
{
	GinScanOpaque so = (GinScanOpaque) scan->opaque;

	ginFreeScanKeys(so);

	if (scankey && scan->numberOfKeys > 0)
	{
		memmove(scan->keyData, scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));
	}
}


void
ginendscan(IndexScanDesc scan)
{
	GinScanOpaque so = (GinScanOpaque) scan->opaque;

	ginFreeScanKeys(so);

	MemoryContextDelete(so->tempCtx);
	MemoryContextDelete(so->keyCtx);

	pfree(so);
}
