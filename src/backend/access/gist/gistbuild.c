/*-------------------------------------------------------------------------
 *
 * gistbuild.c
 *	  build algorithm for GiST indexes implementation.
 *
 * There are two different strategies:
 *
 * 1. Sort all input tuples, pack them into GiST leaf pages in the sorted
 *    order, and create downlinks and internal pages as we go. This builds
 *    the index from the bottom up, similar to how B-tree index build
 *    works.
 *
 * 2. Start with an empty index, and insert all tuples one by one.
 *
 * The sorted method is used if the operator classes for all columns have
 * a 'sortsupport' defined. Otherwise, we resort to the second strategy.
 *
 * The second strategy can optionally use buffers at different levels of
 * the tree to reduce I/O, see "Buffering build algorithm" in the README
 * for a more detailed explanation. It initially calls insert over and
 * over, but switches to the buffered algorithm after a certain number of
 * tuples (unless buffering mode is disabled).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/gist/gistbuild.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/genam.h"
#include "access/gist_private.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xloginsert.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "pgstat.h"
#include "optimizer/optimizer.h"
#include "storage/bufmgr.h"
#include "storage/bulk_write.h"
#include "tcop/tcopprot.h"		/* pgrminclude ignore */
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/tuplesort.h"

/* Magic numbers for parallel state sharing */
#define PARALLEL_KEY_GIST_SHARED		UINT64CONST(0xB000000000000001)
#define PARALLEL_KEY_QUERY_TEXT			UINT64CONST(0xB000000000000002)
#define PARALLEL_KEY_WAL_USAGE			UINT64CONST(0xB000000000000003)
#define PARALLEL_KEY_BUFFER_USAGE		UINT64CONST(0xB000000000000004)

/* Step of index tuples for check whether to switch to buffering build mode */
#define BUFFERING_MODE_SWITCH_CHECK_STEP 256

/*
 * Number of tuples to process in the slow way before switching to buffering
 * mode, when buffering is explicitly turned on. Also, the number of tuples
 * to process between readjusting the buffer size parameter, while in
 * buffering mode.
 */
#define BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET 4096

/*
 * Strategy used to build the index. It can change between the
 * GIST_BUFFERING_* modes on the fly, but if the Sorted method is used,
 * that needs to be decided up-front and cannot be changed afterwards.
 */
typedef enum
{
	GIST_SORTED_BUILD,			/* bottom-up build by sorting */
	GIST_BUFFERING_DISABLED,	/* in regular build mode and aren't going to
								 * switch */
	GIST_BUFFERING_AUTO,		/* in regular build mode, but will switch to
								 * buffering build mode if the index grows too
								 * big */
	GIST_BUFFERING_STATS,		/* gathering statistics of index tuple size
								 * before switching to the buffering build
								 * mode */
	GIST_BUFFERING_ACTIVE,		/* in buffering build mode */
} GistBuildMode;

/*
 * Status for index builds performed in parallel.  This is allocated in a
 * dynamic shared memory segment.
 */
typedef struct GISTShared
{
	/*
	 * These fields are not modified during the build.  They primarily exist
	 * for the benefit of worker processes that need to create state
	 * corresponding to that used by the leader.
	 *
	 * XXX nparticipants is the number or workers we expect to participage in
	 * the build, possibly including the leader process.
	 */
	Oid			heaprelid;
	Oid			indexrelid;
	bool		isconcurrent;
	int			nparticipants;

	/* Parameters determined by the leader, passed to the workers. */
	GistBuildMode buildMode;
	int			freespace;

	/*
	 * workersdonecv is used to monitor the progress of workers.  All parallel
	 * participants must indicate that they are done before leader can finish
	 * building the index.
	 */
	ConditionVariable workersdonecv;

	/*
	 * mutex protects all fields before heapdesc.
	 *
	 * These fields contain status information of interest to GIST index
	 * builds that must work just the same when an index is built in parallel.
	 */
	slock_t		mutex;

	/*
	 * Mutable state that is maintained by workers, and reported back to
	 * leader at end of the scans.
	 *
	 * nparticipantsdone is number of worker processes finished.
	 *
	 * reltuples is the total number of input heap tuples.
	 *
	 * indtuples is the total number of tuples that made it into the index.
	 */
	int			nparticipantsdone;
	double		reltuples;
	double		indtuples;

	/*
	 * ParallelTableScanDescData data follows. Can't directly embed here, as
	 * implementations of the parallel table scan desc interface might need
	 * stronger alignment.
	 */
} GISTShared;

/*
 * Return pointer to a GISTShared's parallel table scan.
 *
 * c.f. shm_toc_allocate as to why BUFFERALIGN is used, rather than just
 * MAXALIGN.
 */
#define ParallelTableScanFromGistShared(shared) \
	(ParallelTableScanDesc) ((char *) (shared) + BUFFERALIGN(sizeof(GISTShared)))

/*
 * Status for leader in parallel index build.
 */
typedef struct GISTLeader
{
	/* parallel context itself */
	ParallelContext *pcxt;

	/*
	 * nparticipants is the exact number of worker processes successfully
	 * launched, plus one leader process if it participates as a worker (only
	 * DISABLE_LEADER_PARTICIPATION builds avoid leader participating as a
	 * worker).
	 *
	 * XXX Seems a bit redundant with nparticipants in GISTShared. Although
	 * that is the expected number, this is what we actually got.
	 */
	int			nparticipants;

	/*
	 * Leader process convenience pointers to shared state (leader avoids TOC
	 * lookups).
	 *
	 * GISTShared is the shared state for entire build. snapshot is the
	 * snapshot used by the scan iff an MVCC snapshot is required.
	 */
	GISTShared *gistshared;
	Snapshot	snapshot;
	WalUsage   *walusage;
	BufferUsage *bufferusage;
} GISTLeader;

/* Working state for gistbuild and its callback */
typedef struct
{
	Relation	indexrel;
	Relation	heaprel;
	GISTSTATE  *giststate;

	Size		freespace;		/* amount of free space to leave on pages */

	GistBuildMode buildMode;

	int64		indtuples;		/* number of tuples indexed */

	/*
	 * Extra data structures used during a buffering build. 'gfbb' contains
	 * information related to managing the build buffers. 'parentMap' is a
	 * lookup table of the parent of each internal page.
	 */
	int64		indtuplesSize;	/* total size of all indexed tuples */
	GISTBuildBuffers *gfbb;
	HTAB	   *parentMap;

	/*
	 * gist_leader is only present when a parallel index build is performed,
	 * and only in the leader process. (Actually, only the leader process has
	 * a GISTBuildState.)
	 */
	bool		is_parallel;
	GISTLeader *gist_leader;

	/*
	 * Extra data structures used during a sorting build.
	 */
	Tuplesortstate *sortstate;	/* state data for tuplesort.c */

	BlockNumber pages_allocated;

	BulkWriteState *bulkstate;
} GISTBuildState;

#define GIST_SORTED_BUILD_PAGE_NUM 4

/*
 * In sorted build, we use a stack of these structs, one for each level,
 * to hold an in-memory buffer of last pages at the level.
 *
 * Sorting GiST build requires good linearization of the sort opclass. This is
 * not always the case in multidimensional data. To tackle the anomalies, we
 * buffer index tuples and apply picksplit that can be multidimension-aware.
 */
typedef struct GistSortedBuildLevelState
{
	int			current_page;
	BlockNumber last_blkno;
	struct GistSortedBuildLevelState *parent;	/* Upper level, if any */
	Page		pages[GIST_SORTED_BUILD_PAGE_NUM];
} GistSortedBuildLevelState;

/* prototypes for private functions */

static void gistSortedBuildCallback(Relation index, ItemPointer tid,
									Datum *values, bool *isnull,
									bool tupleIsAlive, void *state);
static void gist_indexsortbuild(GISTBuildState *state);
static void gist_indexsortbuild_levelstate_add(GISTBuildState *state,
											   GistSortedBuildLevelState *levelstate,
											   IndexTuple itup);
static void gist_indexsortbuild_levelstate_flush(GISTBuildState *state,
												 GistSortedBuildLevelState *levelstate);

static void gistInitBuffering(GISTBuildState *buildstate);
static int	calculatePagesPerBuffer(GISTBuildState *buildstate, int levelStep);
static void gistBuildCallback(Relation index,
							  ItemPointer tid,
							  Datum *values,
							  bool *isnull,
							  bool tupleIsAlive,
							  void *state);
static void gistBuildParallelCallback(Relation index,
									  ItemPointer tid,
									  Datum *values,
									  bool *isnull,
									  bool tupleIsAlive,
									  void *state);
static void gistBufferingBuildInsert(GISTBuildState *buildstate,
									 IndexTuple itup);
static bool gistProcessItup(GISTBuildState *buildstate, IndexTuple itup,
							BlockNumber startblkno, int startlevel);
static BlockNumber gistbufferinginserttuples(GISTBuildState *buildstate,
											 Buffer buffer, int level,
											 IndexTuple *itup, int ntup, OffsetNumber oldoffnum,
											 BlockNumber parentblk, OffsetNumber downlinkoffnum);
static Buffer gistBufferingFindCorrectParent(GISTBuildState *buildstate,
											 BlockNumber childblkno, int level,
											 BlockNumber *parentblkno,
											 OffsetNumber *downlinkoffnum);
static void gistProcessEmptyingQueue(GISTBuildState *buildstate);
static void gistEmptyAllBuffers(GISTBuildState *buildstate);
static int	gistGetMaxLevel(Relation index);

static void gistInitParentMap(GISTBuildState *buildstate);
static void gistMemorizeParent(GISTBuildState *buildstate, BlockNumber child,
							   BlockNumber parent);
static void gistMemorizeAllDownlinks(GISTBuildState *buildstate,
									 Buffer parentbuf);
static BlockNumber gistGetParent(GISTBuildState *buildstate, BlockNumber child);

/* parallel index builds */
static void _gist_begin_parallel(GISTBuildState *buildstate, Relation heap, Relation index,
								 bool isconcurrent, int request);
static void _gist_end_parallel(GISTLeader *gistleader, GISTBuildState *state);
static Size _gist_parallel_estimate_shared(Relation heap, Snapshot snapshot);
static double _gist_parallel_heapscan(GISTBuildState *buildstate);
static void _gist_leader_participate_as_worker(GISTBuildState *buildstate,
											   Relation heap, Relation index);
static void _gist_parallel_scan_and_build(GISTBuildState *buildstate,
										  GISTShared *gistshared,
										  Relation heap, Relation index,
										  int workmem, bool progress);

/*
 * Main entry point to GiST index build.
 */
IndexBuildResult *
gistbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	double		reltuples;
	GISTBuildState buildstate;
	MemoryContext oldcxt = CurrentMemoryContext;
	int			fillfactor;
	Oid			SortSupportFnOids[INDEX_MAX_KEYS];
	GiSTOptions *options = (GiSTOptions *) index->rd_options;

	/*
	 * We expect to be called exactly once for any index relation. If that's
	 * not the case, big trouble's what we have.
	 */
	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

	buildstate.indexrel = index;
	buildstate.heaprel = heap;
	buildstate.sortstate = NULL;
	buildstate.giststate = initGISTstate(index);

	/* assume serial build */
	buildstate.is_parallel = false;
	buildstate.gist_leader = NULL;

	/*
	 * Create a temporary memory context that is reset once for each tuple
	 * processed.  (Note: we don't bother to make this a child of the
	 * giststate's scanCxt, so we have to delete it separately at the end.)
	 */
	buildstate.giststate->tempCxt = createTempGistContext();

	/*
	 * Choose build strategy.  First check whether the user specified to use
	 * buffering mode.  (The use-case for that in the field is somewhat
	 * questionable perhaps, but it's important for testing purposes.)
	 */
	if (options)
	{
		if (options->buffering_mode == GIST_OPTION_BUFFERING_ON)
			buildstate.buildMode = GIST_BUFFERING_STATS;
		else if (options->buffering_mode == GIST_OPTION_BUFFERING_OFF)
			buildstate.buildMode = GIST_BUFFERING_DISABLED;
		else					/* must be "auto" */
			buildstate.buildMode = GIST_BUFFERING_AUTO;
	}
	else
	{
		buildstate.buildMode = GIST_BUFFERING_AUTO;
	}

	/*
	 * Unless buffering mode was forced, see if we can use sorting instead.
	 */
	if (buildstate.buildMode != GIST_BUFFERING_STATS)
	{
		bool		hasallsortsupports = true;
		int			keyscount = IndexRelationGetNumberOfKeyAttributes(index);

		for (int i = 0; i < keyscount; i++)
		{
			SortSupportFnOids[i] = index_getprocid(index, i + 1,
												   GIST_SORTSUPPORT_PROC);
			if (!OidIsValid(SortSupportFnOids[i]))
			{
				hasallsortsupports = false;
				break;
			}
		}
		if (hasallsortsupports)
			buildstate.buildMode = GIST_SORTED_BUILD;
	}

	/*
	 * Calculate target amount of free space to leave on pages.
	 */
	fillfactor = options ? options->fillfactor : GIST_DEFAULT_FILLFACTOR;
	buildstate.freespace = BLCKSZ * (100 - fillfactor) / 100;

	/*
	 * Build the index using the chosen strategy.
	 */
	buildstate.indtuples = 0;
	buildstate.indtuplesSize = 0;

	if (buildstate.buildMode == GIST_SORTED_BUILD)
	{
		/*
		 * Sort all data, build the index from bottom up.
		 */
		buildstate.sortstate = tuplesort_begin_index_gist(heap,
														  index,
														  maintenance_work_mem,
														  NULL,
														  TUPLESORT_NONE);

		/* Scan the table, adding all tuples to the tuplesort */
		reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
										   gistSortedBuildCallback,
										   (void *) &buildstate, NULL);

		/*
		 * Perform the sort and build index pages.
		 */
		tuplesort_performsort(buildstate.sortstate);

		gist_indexsortbuild(&buildstate);

		tuplesort_end(buildstate.sortstate);
	}
	else
	{
		/*
		 * Initialize an empty index and insert all tuples, possibly using
		 * buffers on intermediate levels.
		 */
		Buffer		buffer;
		Page		page;

		/* initialize the root page */
		buffer = gistNewBuffer(index, heap);
		Assert(BufferGetBlockNumber(buffer) == GIST_ROOT_BLKNO);
		page = BufferGetPage(buffer);

		START_CRIT_SECTION();

		GISTInitBuffer(buffer, F_LEAF);

		MarkBufferDirty(buffer);
		PageSetLSN(page, GistBuildLSN);

		UnlockReleaseBuffer(buffer);

		END_CRIT_SECTION();

		/*
		 * Attempt to launch parallel worker scan when required
		 *
		 * XXX plan_create_index_workers makes the number of workers dependent
		 * on maintenance_work_mem, requiring 32MB for each worker. That makes
		 * sense for btree, but maybe not for GIST (at least when not using
		 * buffering)? So maybe make that somehow less strict, optionally?
		 */
		if (indexInfo->ii_ParallelWorkers > 0)
			_gist_begin_parallel(&buildstate, heap,
								 index, indexInfo->ii_Concurrent,
								 indexInfo->ii_ParallelWorkers);

		/*
		 * If parallel build requested and at least one worker process was
		 * successfully launched, set up coordination state, wait for workers
		 * to complete and end the parallel build.
		 *
		 * In serial mode, simply scan the table and build the index one index
		 * tuple at a time.
		 */
		if (buildstate.gist_leader)
		{
			/* scan the relation and wait for parallel workers to finish */
			reltuples = _gist_parallel_heapscan(&buildstate);

			_gist_end_parallel(buildstate.gist_leader, &buildstate);

			/*
			 * We didn't write WAL records as we built the index, so if WAL-logging is
			 * required, write all pages to the WAL now.
			 */
			if (RelationNeedsWAL(index))
			{
				log_newpage_range(index, MAIN_FORKNUM,
								  0, RelationGetNumberOfBlocks(index),
								  true);
			}
		}
		else
		{
			/* Scan the table, inserting all the tuples to the index. */
			reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
											   gistBuildCallback,
											   (void *) &buildstate, NULL);

			/*
			 * If buffering was used, flush out all the tuples that are still
			 * in the buffers.
			 */
			if (buildstate.buildMode == GIST_BUFFERING_ACTIVE)
			{
				elog(DEBUG1, "all tuples processed, emptying buffers");
				gistEmptyAllBuffers(&buildstate);
				gistFreeBuildBuffers(buildstate.gfbb);
			}

			/*
			 * We didn't write WAL records as we built the index, so if
			 * WAL-logging is required, write all pages to the WAL now.
			 */
			if (RelationNeedsWAL(index))
			{
				log_newpage_range(index, MAIN_FORKNUM,
								  0, RelationGetNumberOfBlocks(index),
								  true);
			}

			/* okay, all heap tuples are indexed */
			MemoryContextSwitchTo(oldcxt);
			MemoryContextDelete(buildstate.giststate->tempCxt);
		}
	}

	freeGISTstate(buildstate.giststate);

	/*
	 * Return statistics
	 */
	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));

	result->heap_tuples = reltuples;
	result->index_tuples = (double) buildstate.indtuples;

	return result;
}

/*-------------------------------------------------------------------------
 * Routines for sorted build
 *-------------------------------------------------------------------------
 */

/*
 * Per-tuple callback for table_index_build_scan.
 */
static void
gistSortedBuildCallback(Relation index,
						ItemPointer tid,
						Datum *values,
						bool *isnull,
						bool tupleIsAlive,
						void *state)
{
	GISTBuildState *buildstate = (GISTBuildState *) state;
	MemoryContext oldCtx;
	Datum		compressed_values[INDEX_MAX_KEYS];

	oldCtx = MemoryContextSwitchTo(buildstate->giststate->tempCxt);

	/* Form an index tuple and point it at the heap tuple */
	gistCompressValues(buildstate->giststate, index,
					   values, isnull,
					   true, compressed_values);

	tuplesort_putindextuplevalues(buildstate->sortstate,
								  buildstate->indexrel,
								  tid,
								  compressed_values, isnull);

	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->giststate->tempCxt);

	/* Update tuple count. */
	buildstate->indtuples += 1;
}

/*
 * Build GiST index from bottom up from pre-sorted tuples.
 */
static void
gist_indexsortbuild(GISTBuildState *state)
{
	IndexTuple	itup;
	GistSortedBuildLevelState *levelstate;
	BulkWriteBuffer rootbuf;

	/* Reserve block 0 for the root page */
	state->pages_allocated = 1;

	state->bulkstate = smgr_bulk_start_rel(state->indexrel, MAIN_FORKNUM);

	/* Allocate a temporary buffer for the first leaf page batch. */
	levelstate = palloc0(sizeof(GistSortedBuildLevelState));
	levelstate->pages[0] = palloc(BLCKSZ);
	levelstate->parent = NULL;
	gistinitpage(levelstate->pages[0], F_LEAF);

	/*
	 * Fill index pages with tuples in the sorted order.
	 */
	while ((itup = tuplesort_getindextuple(state->sortstate, true)) != NULL)
	{
		gist_indexsortbuild_levelstate_add(state, levelstate, itup);
		MemoryContextReset(state->giststate->tempCxt);
	}

	/*
	 * Write out the partially full non-root pages.
	 *
	 * Keep in mind that flush can build a new root. If number of pages is > 1
	 * then new root is required.
	 */
	while (levelstate->parent != NULL || levelstate->current_page != 0)
	{
		GistSortedBuildLevelState *parent;

		gist_indexsortbuild_levelstate_flush(state, levelstate);
		parent = levelstate->parent;
		for (int i = 0; i < GIST_SORTED_BUILD_PAGE_NUM; i++)
			if (levelstate->pages[i])
				pfree(levelstate->pages[i]);
		pfree(levelstate);
		levelstate = parent;
	}

	/* Write out the root */
	PageSetLSN(levelstate->pages[0], GistBuildLSN);
	rootbuf = smgr_bulk_get_buf(state->bulkstate);
	memcpy(rootbuf, levelstate->pages[0], BLCKSZ);
	smgr_bulk_write(state->bulkstate, GIST_ROOT_BLKNO, rootbuf, true);

	pfree(levelstate);

	smgr_bulk_finish(state->bulkstate);
}

/*
 * Add tuple to a page. If the pages are full, write them out and re-initialize
 * a new page first.
 */
static void
gist_indexsortbuild_levelstate_add(GISTBuildState *state,
								   GistSortedBuildLevelState *levelstate,
								   IndexTuple itup)
{
	Size		sizeNeeded;

	/* Check if tuple can be added to the current page */
	sizeNeeded = IndexTupleSize(itup) + sizeof(ItemIdData); /* fillfactor ignored */
	if (PageGetFreeSpace(levelstate->pages[levelstate->current_page]) < sizeNeeded)
	{
		Page		newPage;
		Page		old_page = levelstate->pages[levelstate->current_page];
		uint16		old_page_flags = GistPageGetOpaque(old_page)->flags;

		if (levelstate->current_page + 1 == GIST_SORTED_BUILD_PAGE_NUM)
		{
			gist_indexsortbuild_levelstate_flush(state, levelstate);
		}
		else
			levelstate->current_page++;

		if (levelstate->pages[levelstate->current_page] == NULL)
			levelstate->pages[levelstate->current_page] = palloc0(BLCKSZ);

		newPage = levelstate->pages[levelstate->current_page];
		gistinitpage(newPage, old_page_flags);
	}

	gistfillbuffer(levelstate->pages[levelstate->current_page], &itup, 1, InvalidOffsetNumber);
}

static void
gist_indexsortbuild_levelstate_flush(GISTBuildState *state,
									 GistSortedBuildLevelState *levelstate)
{
	GistSortedBuildLevelState *parent;
	BlockNumber blkno;
	MemoryContext oldCtx;
	IndexTuple	union_tuple;
	SplitPageLayout *dist;
	IndexTuple *itvec;
	int			vect_len;
	bool		isleaf = GistPageIsLeaf(levelstate->pages[0]);

	CHECK_FOR_INTERRUPTS();

	oldCtx = MemoryContextSwitchTo(state->giststate->tempCxt);

	/* Get index tuples from first page */
	itvec = gistextractpage(levelstate->pages[0], &vect_len);
	if (levelstate->current_page > 0)
	{
		/* Append tuples from each page */
		for (int i = 1; i < levelstate->current_page + 1; i++)
		{
			int			len_local;
			IndexTuple *itvec_local = gistextractpage(levelstate->pages[i], &len_local);

			itvec = gistjoinvector(itvec, &vect_len, itvec_local, len_local);
			pfree(itvec_local);
		}

		/* Apply picksplit to list of all collected tuples */
		dist = gistSplit(state->indexrel, levelstate->pages[0], itvec, vect_len, state->giststate);
	}
	else
	{
		/* Create split layout from single page */
		dist = (SplitPageLayout *) palloc0(sizeof(SplitPageLayout));
		union_tuple = gistunion(state->indexrel, itvec, vect_len,
								state->giststate);
		dist->itup = union_tuple;
		dist->list = gistfillitupvec(itvec, vect_len, &(dist->lenlist));
		dist->block.num = vect_len;
	}

	MemoryContextSwitchTo(oldCtx);

	/* Reset page counter */
	levelstate->current_page = 0;

	/* Create pages for all partitions in split result */
	for (; dist != NULL; dist = dist->next)
	{
		char	   *data;
		BulkWriteBuffer buf;
		Page		target;

		/* check once per page */
		CHECK_FOR_INTERRUPTS();

		/* Create page and copy data */
		data = (char *) (dist->list);
		buf = smgr_bulk_get_buf(state->bulkstate);
		target = (Page) buf;
		gistinitpage(target, isleaf ? F_LEAF : 0);
		for (int i = 0; i < dist->block.num; i++)
		{
			IndexTuple	thistup = (IndexTuple) data;

			if (PageAddItem(target, (Item) data, IndexTupleSize(thistup), i + FirstOffsetNumber, false, false) == InvalidOffsetNumber)
				elog(ERROR, "failed to add item to index page in \"%s\"", RelationGetRelationName(state->indexrel));

			data += IndexTupleSize(thistup);
		}
		union_tuple = dist->itup;

		/*
		 * Set the right link to point to the previous page. This is just for
		 * debugging purposes: GiST only follows the right link if a page is
		 * split concurrently to a scan, and that cannot happen during index
		 * build.
		 *
		 * It's a bit counterintuitive that we set the right link on the new
		 * page to point to the previous page, not the other way around. But
		 * GiST pages are not ordered like B-tree pages are, so as long as the
		 * right-links form a chain through all the pages at the same level,
		 * the order doesn't matter.
		 */
		if (levelstate->last_blkno)
			GistPageGetOpaque(target)->rightlink = levelstate->last_blkno;

		/*
		 * The page is now complete. Assign a block number to it, and pass it
		 * to the bulk writer.
		 */
		blkno = state->pages_allocated++;
		PageSetLSN(target, GistBuildLSN);
		smgr_bulk_write(state->bulkstate, blkno, buf, true);
		ItemPointerSetBlockNumber(&(union_tuple->t_tid), blkno);
		levelstate->last_blkno = blkno;

		/*
		 * Insert the downlink to the parent page. If this was the root,
		 * create a new page as the parent, which becomes the new root.
		 */
		parent = levelstate->parent;
		if (parent == NULL)
		{
			parent = palloc0(sizeof(GistSortedBuildLevelState));
			parent->pages[0] = palloc(BLCKSZ);
			parent->parent = NULL;
			gistinitpage(parent->pages[0], 0);

			levelstate->parent = parent;
		}
		gist_indexsortbuild_levelstate_add(state, parent, union_tuple);
	}
}


/*-------------------------------------------------------------------------
 * Routines for non-sorted build
 *-------------------------------------------------------------------------
 */

/*
 * Attempt to switch to buffering mode.
 *
 * If there is not enough memory for buffering build, sets bufferingMode
 * to GIST_BUFFERING_DISABLED, so that we don't bother to try the switch
 * anymore. Otherwise initializes the build buffers, and sets bufferingMode to
 * GIST_BUFFERING_ACTIVE.
 */
static void
gistInitBuffering(GISTBuildState *buildstate)
{
	Relation	index = buildstate->indexrel;
	int			pagesPerBuffer;
	Size		pageFreeSpace;
	Size		itupAvgSize,
				itupMinSize;
	double		avgIndexTuplesPerPage,
				maxIndexTuplesPerPage;
	int			i;
	int			levelStep;

	/* Calc space of index page which is available for index tuples */
	pageFreeSpace = BLCKSZ - SizeOfPageHeaderData - sizeof(GISTPageOpaqueData)
		- sizeof(ItemIdData)
		- buildstate->freespace;

	/*
	 * Calculate average size of already inserted index tuples using gathered
	 * statistics.
	 */
	itupAvgSize = (double) buildstate->indtuplesSize /
		(double) buildstate->indtuples;

	/*
	 * Calculate minimal possible size of index tuple by index metadata.
	 * Minimal possible size of varlena is VARHDRSZ.
	 *
	 * XXX: that's not actually true, as a short varlen can be just 2 bytes.
	 * And we should take padding into account here.
	 */
	itupMinSize = (Size) MAXALIGN(sizeof(IndexTupleData));
	for (i = 0; i < index->rd_att->natts; i++)
	{
		if (TupleDescAttr(index->rd_att, i)->attlen < 0)
			itupMinSize += VARHDRSZ;
		else
			itupMinSize += TupleDescAttr(index->rd_att, i)->attlen;
	}

	/* Calculate average and maximal number of index tuples which fit to page */
	avgIndexTuplesPerPage = pageFreeSpace / itupAvgSize;
	maxIndexTuplesPerPage = pageFreeSpace / itupMinSize;

	/*
	 * We need to calculate two parameters for the buffering algorithm:
	 * levelStep and pagesPerBuffer.
	 *
	 * levelStep determines the size of subtree that we operate on, while
	 * emptying a buffer. A higher value is better, as you need fewer buffer
	 * emptying steps to build the index. However, if you set it too high, the
	 * subtree doesn't fit in cache anymore, and you quickly lose the benefit
	 * of the buffers.
	 *
	 * In Arge et al's paper, levelStep is chosen as logB(M/4B), where B is
	 * the number of tuples on page (ie. fanout), and M is the amount of
	 * internal memory available. Curiously, they doesn't explain *why* that
	 * setting is optimal. We calculate it by taking the highest levelStep so
	 * that a subtree still fits in cache. For a small B, our way of
	 * calculating levelStep is very close to Arge et al's formula. For a
	 * large B, our formula gives a value that is 2x higher.
	 *
	 * The average size (in pages) of a subtree of depth n can be calculated
	 * as a geometric series:
	 *
	 * B^0 + B^1 + B^2 + ... + B^n = (1 - B^(n + 1)) / (1 - B)
	 *
	 * where B is the average number of index tuples on page. The subtree is
	 * cached in the shared buffer cache and the OS cache, so we choose
	 * levelStep so that the subtree size is comfortably smaller than
	 * effective_cache_size, with a safety factor of 4.
	 *
	 * The estimate on the average number of index tuples on page is based on
	 * average tuple sizes observed before switching to buffered build, so the
	 * real subtree size can be somewhat larger. Also, it would selfish to
	 * gobble the whole cache for our index build. The safety factor of 4
	 * should account for those effects.
	 *
	 * The other limiting factor for setting levelStep is that while
	 * processing a subtree, we need to hold one page for each buffer at the
	 * next lower buffered level. The max. number of buffers needed for that
	 * is maxIndexTuplesPerPage^levelStep. This is very conservative, but
	 * hopefully maintenance_work_mem is set high enough that you're
	 * constrained by effective_cache_size rather than maintenance_work_mem.
	 *
	 * XXX: the buffer hash table consumes a fair amount of memory too per
	 * buffer, but that is not currently taken into account. That scales on
	 * the total number of buffers used, ie. the index size and on levelStep.
	 * Note that a higher levelStep *reduces* the amount of memory needed for
	 * the hash table.
	 */
	levelStep = 1;
	for (;;)
	{
		double		subtreesize;
		double		maxlowestlevelpages;

		/* size of an average subtree at this levelStep (in pages). */
		subtreesize =
			(1 - pow(avgIndexTuplesPerPage, (double) (levelStep + 1))) /
			(1 - avgIndexTuplesPerPage);

		/* max number of pages at the lowest level of a subtree */
		maxlowestlevelpages = pow(maxIndexTuplesPerPage, (double) levelStep);

		/* subtree must fit in cache (with safety factor of 4) */
		if (subtreesize > effective_cache_size / 4)
			break;

		/* each node in the lowest level of a subtree has one page in memory */
		if (maxlowestlevelpages > ((double) maintenance_work_mem * 1024) / BLCKSZ)
			break;

		/* Good, we can handle this levelStep. See if we can go one higher. */
		levelStep++;
	}

	/*
	 * We just reached an unacceptable value of levelStep in previous loop.
	 * So, decrease levelStep to get last acceptable value.
	 */
	levelStep--;

	/*
	 * If there's not enough cache or maintenance_work_mem, fall back to plain
	 * inserts.
	 */
	if (levelStep <= 0)
	{
		elog(DEBUG1, "failed to switch to buffered GiST build");
		buildstate->buildMode = GIST_BUFFERING_DISABLED;
		return;
	}

	/*
	 * The second parameter to set is pagesPerBuffer, which determines the
	 * size of each buffer. We adjust pagesPerBuffer also during the build,
	 * which is why this calculation is in a separate function.
	 */
	pagesPerBuffer = calculatePagesPerBuffer(buildstate, levelStep);

	/* Initialize GISTBuildBuffers with these parameters */
	buildstate->gfbb = gistInitBuildBuffers(pagesPerBuffer, levelStep,
											gistGetMaxLevel(index));

	gistInitParentMap(buildstate);

	buildstate->buildMode = GIST_BUFFERING_ACTIVE;

	elog(DEBUG1, "switched to buffered GiST build; level step = %d, pagesPerBuffer = %d",
		 levelStep, pagesPerBuffer);
}

/*
 * Calculate pagesPerBuffer parameter for the buffering algorithm.
 *
 * Buffer size is chosen so that assuming that tuples are distributed
 * randomly, emptying half a buffer fills on average one page in every buffer
 * at the next lower level.
 */
static int
calculatePagesPerBuffer(GISTBuildState *buildstate, int levelStep)
{
	double		pagesPerBuffer;
	double		avgIndexTuplesPerPage;
	double		itupAvgSize;
	Size		pageFreeSpace;

	/* Calc space of index page which is available for index tuples */
	pageFreeSpace = BLCKSZ - SizeOfPageHeaderData - sizeof(GISTPageOpaqueData)
		- sizeof(ItemIdData)
		- buildstate->freespace;

	/*
	 * Calculate average size of already inserted index tuples using gathered
	 * statistics.
	 */
	itupAvgSize = (double) buildstate->indtuplesSize /
		(double) buildstate->indtuples;

	avgIndexTuplesPerPage = pageFreeSpace / itupAvgSize;

	/*
	 * Recalculate required size of buffers.
	 */
	pagesPerBuffer = 2 * pow(avgIndexTuplesPerPage, levelStep);

	return (int) rint(pagesPerBuffer);
}

/*
 * Per-tuple callback for table_index_build_scan.
 */
static void
gistBuildCallback(Relation index,
				  ItemPointer tid,
				  Datum *values,
				  bool *isnull,
				  bool tupleIsAlive,
				  void *state)
{
	GISTBuildState *buildstate = (GISTBuildState *) state;
	IndexTuple	itup;
	MemoryContext oldCtx;

	oldCtx = MemoryContextSwitchTo(buildstate->giststate->tempCxt);

	/* form an index tuple and point it at the heap tuple */
	itup = gistFormTuple(buildstate->giststate, index,
						 values, isnull,
						 true);
	itup->t_tid = *tid;

	/* Update tuple count and total size. */
	buildstate->indtuples += 1;
	buildstate->indtuplesSize += IndexTupleSize(itup);

	/*
	 * XXX In buffering builds, the tempCxt is also reset down inside
	 * gistProcessEmptyingQueue().  This is not great because it risks
	 * confusion and possible use of dangling pointers (for example, itup
	 * might be already freed when control returns here).  It's generally
	 * better that a memory context be "owned" by only one function.  However,
	 * currently this isn't causing issues so it doesn't seem worth the amount
	 * of refactoring that would be needed to avoid it.
	 */
	if (buildstate->buildMode == GIST_BUFFERING_ACTIVE)
	{
		/* We have buffers, so use them. */
		gistBufferingBuildInsert(buildstate, itup);
	}
	else
	{
		/*
		 * There's no buffers (yet). Since we already have the index relation
		 * locked, we call gistdoinsert directly.
		 */
		gistdoinsert(index, itup, buildstate->freespace,
					 buildstate->giststate, buildstate->heaprel, true, false);
	}

	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->giststate->tempCxt);

	if (buildstate->buildMode == GIST_BUFFERING_ACTIVE &&
		buildstate->indtuples % BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET == 0)
	{
		/* Adjust the target buffer size now */
		buildstate->gfbb->pagesPerBuffer =
			calculatePagesPerBuffer(buildstate, buildstate->gfbb->levelStep);
	}

	/*
	 * In 'auto' mode, check if the index has grown too large to fit in cache,
	 * and switch to buffering mode if it has.
	 *
	 * To avoid excessive calls to smgrnblocks(), only check this every
	 * BUFFERING_MODE_SWITCH_CHECK_STEP index tuples.
	 *
	 * In 'stats' state, switch as soon as we have seen enough tuples to have
	 * some idea of the average tuple size.
	 */
	if ((buildstate->buildMode == GIST_BUFFERING_AUTO &&
		 buildstate->indtuples % BUFFERING_MODE_SWITCH_CHECK_STEP == 0 &&
		 effective_cache_size < smgrnblocks(RelationGetSmgr(index),
											MAIN_FORKNUM)) ||
		(buildstate->buildMode == GIST_BUFFERING_STATS &&
		 buildstate->indtuples >= BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET))
	{
		/*
		 * Index doesn't fit in effective cache anymore. Try to switch to
		 * buffering build mode.
		 */
		gistInitBuffering(buildstate);
	}
}

/*
 * Per-tuple callback for table_index_build_scan.
 *
 * XXX Almost the same as gistBuildCallback, but with is_build=false when
 * calling gistdoinsert. Otherwise we get assert failures due to workers
 * modifying the index concurrently.
 */
static void
gistBuildParallelCallback(Relation index,
						  ItemPointer tid,
						  Datum *values,
						  bool *isnull,
						  bool tupleIsAlive,
						  void *state)
{
	GISTBuildState *buildstate = (GISTBuildState *) state;
	IndexTuple	itup;
	MemoryContext oldCtx;

	oldCtx = MemoryContextSwitchTo(buildstate->giststate->tempCxt);

	/* form an index tuple and point it at the heap tuple */
	itup = gistFormTuple(buildstate->giststate, index,
						 values, isnull,
						 true);
	itup->t_tid = *tid;

	/* Update tuple count and total size. */
	buildstate->indtuples += 1;
	buildstate->indtuplesSize += IndexTupleSize(itup);

	/*
	 * There's no buffers (yet). Since we already have the index relation
	 * locked, we call gistdoinsert directly.
	 */
	gistdoinsert(index, itup, buildstate->freespace,
				 buildstate->giststate, buildstate->heaprel, true, true);

	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->giststate->tempCxt);
}

/*
 * Insert function for buffering index build.
 */
static void
gistBufferingBuildInsert(GISTBuildState *buildstate, IndexTuple itup)
{
	/* Insert the tuple to buffers. */
	gistProcessItup(buildstate, itup, 0, buildstate->gfbb->rootlevel);

	/* If we filled up (half of a) buffer, process buffer emptying. */
	gistProcessEmptyingQueue(buildstate);
}

/*
 * Process an index tuple. Runs the tuple down the tree until we reach a leaf
 * page or node buffer, and inserts the tuple there. Returns true if we have
 * to stop buffer emptying process (because one of child buffers can't take
 * index tuples anymore).
 */
static bool
gistProcessItup(GISTBuildState *buildstate, IndexTuple itup,
				BlockNumber startblkno, int startlevel)
{
	GISTSTATE  *giststate = buildstate->giststate;
	GISTBuildBuffers *gfbb = buildstate->gfbb;
	Relation	indexrel = buildstate->indexrel;
	BlockNumber childblkno;
	Buffer		buffer;
	bool		result = false;
	BlockNumber blkno;
	int			level;
	OffsetNumber downlinkoffnum = InvalidOffsetNumber;
	BlockNumber parentblkno = InvalidBlockNumber;

	CHECK_FOR_INTERRUPTS();

	/*
	 * Loop until we reach a leaf page (level == 0) or a level with buffers
	 * (not including the level we start at, because we would otherwise make
	 * no progress).
	 */
	blkno = startblkno;
	level = startlevel;
	for (;;)
	{
		ItemId		iid;
		IndexTuple	idxtuple,
					newtup;
		Page		page;
		OffsetNumber childoffnum;

		/* Have we reached a level with buffers? */
		if (LEVEL_HAS_BUFFERS(level, gfbb) && level != startlevel)
			break;

		/* Have we reached a leaf page? */
		if (level == 0)
			break;

		/*
		 * Nope. Descend down to the next level then. Choose a child to
		 * descend down to.
		 */

		buffer = ReadBuffer(indexrel, blkno);
		LockBuffer(buffer, GIST_EXCLUSIVE);

		page = (Page) BufferGetPage(buffer);
		childoffnum = gistchoose(indexrel, page, itup, giststate);
		iid = PageGetItemId(page, childoffnum);
		idxtuple = (IndexTuple) PageGetItem(page, iid);
		childblkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));

		if (level > 1)
			gistMemorizeParent(buildstate, childblkno, blkno);

		/*
		 * Check that the key representing the target child node is consistent
		 * with the key we're inserting. Update it if it's not.
		 */
		newtup = gistgetadjusted(indexrel, idxtuple, itup, giststate);
		if (newtup)
		{
			blkno = gistbufferinginserttuples(buildstate,
											  buffer,
											  level,
											  &newtup,
											  1,
											  childoffnum,
											  InvalidBlockNumber,
											  InvalidOffsetNumber);
			/* gistbufferinginserttuples() released the buffer */
		}
		else
			UnlockReleaseBuffer(buffer);

		/* Descend to the child */
		parentblkno = blkno;
		blkno = childblkno;
		downlinkoffnum = childoffnum;
		Assert(level > 0);
		level--;
	}

	if (LEVEL_HAS_BUFFERS(level, gfbb))
	{
		/*
		 * We've reached level with buffers. Place the index tuple to the
		 * buffer, and add the buffer to the emptying queue if it overflows.
		 */
		GISTNodeBuffer *childNodeBuffer;

		/* Find the buffer or create a new one */
		childNodeBuffer = gistGetNodeBuffer(gfbb, giststate, blkno, level);

		/* Add index tuple to it */
		gistPushItupToNodeBuffer(gfbb, childNodeBuffer, itup);

		if (BUFFER_OVERFLOWED(childNodeBuffer, gfbb))
			result = true;
	}
	else
	{
		/*
		 * We've reached a leaf page. Place the tuple here.
		 */
		Assert(level == 0);
		buffer = ReadBuffer(indexrel, blkno);
		LockBuffer(buffer, GIST_EXCLUSIVE);
		gistbufferinginserttuples(buildstate, buffer, level,
								  &itup, 1, InvalidOffsetNumber,
								  parentblkno, downlinkoffnum);
		/* gistbufferinginserttuples() released the buffer */
	}

	return result;
}

/*
 * Insert tuples to a given page.
 *
 * This is analogous with gistinserttuples() in the regular insertion code.
 *
 * Returns the block number of the page where the (first) new or updated tuple
 * was inserted. Usually that's the original page, but might be a sibling page
 * if the original page was split.
 *
 * Caller should hold a lock on 'buffer' on entry. This function will unlock
 * and unpin it.
 */
static BlockNumber
gistbufferinginserttuples(GISTBuildState *buildstate, Buffer buffer, int level,
						  IndexTuple *itup, int ntup, OffsetNumber oldoffnum,
						  BlockNumber parentblk, OffsetNumber downlinkoffnum)
{
	GISTBuildBuffers *gfbb = buildstate->gfbb;
	List	   *splitinfo;
	bool		is_split;
	BlockNumber placed_to_blk = InvalidBlockNumber;

	is_split = gistplacetopage(buildstate->indexrel,
							   buildstate->freespace,
							   buildstate->giststate,
							   buffer,
							   itup, ntup, oldoffnum, &placed_to_blk,
							   InvalidBuffer,
							   &splitinfo,
							   false,
							   buildstate->heaprel, true,
							   buildstate->is_parallel);

	/*
	 * If this is a root split, update the root path item kept in memory. This
	 * ensures that all path stacks are always complete, including all parent
	 * nodes up to the root. That simplifies the algorithm to re-find correct
	 * parent.
	 */
	if (is_split && BufferGetBlockNumber(buffer) == GIST_ROOT_BLKNO)
	{
		Page		page = BufferGetPage(buffer);
		OffsetNumber off;
		OffsetNumber maxoff;

		Assert(level == gfbb->rootlevel);
		gfbb->rootlevel++;

		elog(DEBUG2, "splitting GiST root page, now %d levels deep", gfbb->rootlevel);

		/*
		 * All the downlinks on the old root page are now on one of the child
		 * pages. Visit all the new child pages to memorize the parents of the
		 * grandchildren.
		 */
		if (gfbb->rootlevel > 1)
		{
			maxoff = PageGetMaxOffsetNumber(page);
			for (off = FirstOffsetNumber; off <= maxoff; off++)
			{
				ItemId		iid = PageGetItemId(page, off);
				IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);
				BlockNumber childblkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));
				Buffer		childbuf = ReadBuffer(buildstate->indexrel, childblkno);

				LockBuffer(childbuf, GIST_SHARE);
				gistMemorizeAllDownlinks(buildstate, childbuf);
				UnlockReleaseBuffer(childbuf);

				/*
				 * Also remember that the parent of the new child page is the
				 * root block.
				 */
				gistMemorizeParent(buildstate, childblkno, GIST_ROOT_BLKNO);
			}
		}
	}

	if (splitinfo)
	{
		/*
		 * Insert the downlinks to the parent. This is analogous with
		 * gistfinishsplit() in the regular insertion code, but the locking is
		 * simpler, and we have to maintain the buffers on internal nodes and
		 * the parent map.
		 */
		IndexTuple *downlinks;
		int			ndownlinks,
					i;
		Buffer		parentBuffer;
		ListCell   *lc;

		/* Parent may have changed since we memorized this path. */
		parentBuffer =
			gistBufferingFindCorrectParent(buildstate,
										   BufferGetBlockNumber(buffer),
										   level,
										   &parentblk,
										   &downlinkoffnum);

		/*
		 * If there's a buffer associated with this page, that needs to be
		 * split too. gistRelocateBuildBuffersOnSplit() will also adjust the
		 * downlinks in 'splitinfo', to make sure they're consistent not only
		 * with the tuples already on the pages, but also the tuples in the
		 * buffers that will eventually be inserted to them.
		 */
		gistRelocateBuildBuffersOnSplit(gfbb,
										buildstate->giststate,
										buildstate->indexrel,
										level,
										buffer, splitinfo);

		/* Create an array of all the downlink tuples */
		ndownlinks = list_length(splitinfo);
		downlinks = (IndexTuple *) palloc(sizeof(IndexTuple) * ndownlinks);
		i = 0;
		foreach(lc, splitinfo)
		{
			GISTPageSplitInfo *splitinfo = lfirst(lc);

			/*
			 * Remember the parent of each new child page in our parent map.
			 * This assumes that the downlinks fit on the parent page. If the
			 * parent page is split, too, when we recurse up to insert the
			 * downlinks, the recursive gistbufferinginserttuples() call will
			 * update the map again.
			 */
			if (level > 0)
				gistMemorizeParent(buildstate,
								   BufferGetBlockNumber(splitinfo->buf),
								   BufferGetBlockNumber(parentBuffer));

			/*
			 * Also update the parent map for all the downlinks that got moved
			 * to a different page. (actually this also loops through the
			 * downlinks that stayed on the original page, but it does no
			 * harm).
			 */
			if (level > 1)
				gistMemorizeAllDownlinks(buildstate, splitinfo->buf);

			/*
			 * Since there's no concurrent access, we can release the lower
			 * level buffers immediately. This includes the original page.
			 */
			UnlockReleaseBuffer(splitinfo->buf);
			downlinks[i++] = splitinfo->downlink;
		}

		/* Insert them into parent. */
		gistbufferinginserttuples(buildstate, parentBuffer, level + 1,
								  downlinks, ndownlinks, downlinkoffnum,
								  InvalidBlockNumber, InvalidOffsetNumber);

		list_free_deep(splitinfo);	/* we don't need this anymore */
	}
	else
		UnlockReleaseBuffer(buffer);

	return placed_to_blk;
}

/*
 * Find the downlink pointing to a child page.
 *
 * 'childblkno' indicates the child page to find the parent for. 'level' is
 * the level of the child. On entry, *parentblkno and *downlinkoffnum can
 * point to a location where the downlink used to be - we will check that
 * location first, and save some cycles if it hasn't moved. The function
 * returns a buffer containing the downlink, exclusively-locked, and
 * *parentblkno and *downlinkoffnum are set to the real location of the
 * downlink.
 *
 * If the child page is a leaf (level == 0), the caller must supply a correct
 * parentblkno. Otherwise we use the parent map hash table to find the parent
 * block.
 *
 * This function serves the same purpose as gistFindCorrectParent() during
 * normal index inserts, but this is simpler because we don't need to deal
 * with concurrent inserts.
 */
static Buffer
gistBufferingFindCorrectParent(GISTBuildState *buildstate,
							   BlockNumber childblkno, int level,
							   BlockNumber *parentblkno,
							   OffsetNumber *downlinkoffnum)
{
	BlockNumber parent;
	Buffer		buffer;
	Page		page;
	OffsetNumber maxoff;
	OffsetNumber off;

	if (level > 0)
		parent = gistGetParent(buildstate, childblkno);
	else
	{
		/*
		 * For a leaf page, the caller must supply a correct parent block
		 * number.
		 */
		if (*parentblkno == InvalidBlockNumber)
			elog(ERROR, "no parent buffer provided of child %u", childblkno);
		parent = *parentblkno;
	}

	buffer = ReadBuffer(buildstate->indexrel, parent);
	page = BufferGetPage(buffer);
	LockBuffer(buffer, GIST_EXCLUSIVE);
	gistcheckpage(buildstate->indexrel, buffer);
	maxoff = PageGetMaxOffsetNumber(page);

	/* Check if it was not moved */
	if (parent == *parentblkno && *parentblkno != InvalidBlockNumber &&
		*downlinkoffnum != InvalidOffsetNumber && *downlinkoffnum <= maxoff)
	{
		ItemId		iid = PageGetItemId(page, *downlinkoffnum);
		IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);

		if (ItemPointerGetBlockNumber(&(idxtuple->t_tid)) == childblkno)
		{
			/* Still there */
			return buffer;
		}
	}

	/*
	 * Downlink was not at the offset where it used to be. Scan the page to
	 * find it. During normal gist insertions, it might've moved to another
	 * page, to the right, but during a buffering build, we keep track of the
	 * parent of each page in the lookup table so we should always know what
	 * page it's on.
	 */
	for (off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off))
	{
		ItemId		iid = PageGetItemId(page, off);
		IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);

		if (ItemPointerGetBlockNumber(&(idxtuple->t_tid)) == childblkno)
		{
			/* yes!!, found it */
			*downlinkoffnum = off;
			return buffer;
		}
	}

	elog(ERROR, "failed to re-find parent for block %u", childblkno);
	return InvalidBuffer;		/* keep compiler quiet */
}

/*
 * Process buffers emptying stack. Emptying of one buffer can cause emptying
 * of other buffers. This function iterates until this cascading emptying
 * process finished, e.g. until buffers emptying stack is empty.
 */
static void
gistProcessEmptyingQueue(GISTBuildState *buildstate)
{
	GISTBuildBuffers *gfbb = buildstate->gfbb;

	/* Iterate while we have elements in buffers emptying stack. */
	while (gfbb->bufferEmptyingQueue != NIL)
	{
		GISTNodeBuffer *emptyingNodeBuffer;

		/* Get node buffer from emptying stack. */
		emptyingNodeBuffer = (GISTNodeBuffer *) linitial(gfbb->bufferEmptyingQueue);
		gfbb->bufferEmptyingQueue = list_delete_first(gfbb->bufferEmptyingQueue);
		emptyingNodeBuffer->queuedForEmptying = false;

		/*
		 * We are going to load last pages of buffers where emptying will be
		 * to. So let's unload any previously loaded buffers.
		 */
		gistUnloadNodeBuffers(gfbb);

		/*
		 * Pop tuples from the buffer and run them down to the buffers at
		 * lower level, or leaf pages. We continue until one of the lower
		 * level buffers fills up, or this buffer runs empty.
		 *
		 * In Arge et al's paper, the buffer emptying is stopped after
		 * processing 1/2 node buffer worth of tuples, to avoid overfilling
		 * any of the lower level buffers. However, it's more efficient to
		 * keep going until one of the lower level buffers actually fills up,
		 * so that's what we do. This doesn't need to be exact, if a buffer
		 * overfills by a few tuples, there's no harm done.
		 */
		while (true)
		{
			IndexTuple	itup;

			/* Get next index tuple from the buffer */
			if (!gistPopItupFromNodeBuffer(gfbb, emptyingNodeBuffer, &itup))
				break;

			/*
			 * Run it down to the underlying node buffer or leaf page.
			 *
			 * Note: it's possible that the buffer we're emptying splits as a
			 * result of this call. If that happens, our emptyingNodeBuffer
			 * points to the left half of the split. After split, it's very
			 * likely that the new left buffer is no longer over the half-full
			 * threshold, but we might as well keep flushing tuples from it
			 * until we fill a lower-level buffer.
			 */
			if (gistProcessItup(buildstate, itup, emptyingNodeBuffer->nodeBlocknum, emptyingNodeBuffer->level))
			{
				/*
				 * A lower level buffer filled up. Stop emptying this buffer,
				 * to avoid overflowing the lower level buffer.
				 */
				break;
			}

			/* Free all the memory allocated during index tuple processing */
			MemoryContextReset(buildstate->giststate->tempCxt);
		}
	}
}

/*
 * Empty all node buffers, from top to bottom. This is done at the end of
 * index build to flush all remaining tuples to the index.
 *
 * Note: This destroys the buffersOnLevels lists, so the buffers should not
 * be inserted to after this call.
 */
static void
gistEmptyAllBuffers(GISTBuildState *buildstate)
{
	GISTBuildBuffers *gfbb = buildstate->gfbb;
	MemoryContext oldCtx;
	int			i;

	oldCtx = MemoryContextSwitchTo(buildstate->giststate->tempCxt);

	/*
	 * Iterate through the levels from top to bottom.
	 */
	for (i = gfbb->buffersOnLevelsLen - 1; i >= 0; i--)
	{
		/*
		 * Empty all buffers on this level. Note that new buffers can pop up
		 * in the list during the processing, as a result of page splits, so a
		 * simple walk through the list won't work. We remove buffers from the
		 * list when we see them empty; a buffer can't become non-empty once
		 * it's been fully emptied.
		 */
		while (gfbb->buffersOnLevels[i] != NIL)
		{
			GISTNodeBuffer *nodeBuffer;

			nodeBuffer = (GISTNodeBuffer *) linitial(gfbb->buffersOnLevels[i]);

			if (nodeBuffer->blocksCount != 0)
			{
				/*
				 * Add this buffer to the emptying queue, and proceed to empty
				 * the queue.
				 */
				if (!nodeBuffer->queuedForEmptying)
				{
					MemoryContextSwitchTo(gfbb->context);
					nodeBuffer->queuedForEmptying = true;
					gfbb->bufferEmptyingQueue =
						lcons(nodeBuffer, gfbb->bufferEmptyingQueue);
					MemoryContextSwitchTo(buildstate->giststate->tempCxt);
				}
				gistProcessEmptyingQueue(buildstate);
			}
			else
				gfbb->buffersOnLevels[i] =
					list_delete_first(gfbb->buffersOnLevels[i]);
		}
		elog(DEBUG2, "emptied all buffers at level %d", i);
	}
	MemoryContextSwitchTo(oldCtx);
}

/*
 * Get the depth of the GiST index.
 */
static int
gistGetMaxLevel(Relation index)
{
	int			maxLevel;
	BlockNumber blkno;

	/*
	 * Traverse down the tree, starting from the root, until we hit the leaf
	 * level.
	 */
	maxLevel = 0;
	blkno = GIST_ROOT_BLKNO;
	while (true)
	{
		Buffer		buffer;
		Page		page;
		IndexTuple	itup;

		buffer = ReadBuffer(index, blkno);

		/*
		 * There's no concurrent access during index build, so locking is just
		 * pro forma.
		 */
		LockBuffer(buffer, GIST_SHARE);
		page = (Page) BufferGetPage(buffer);

		if (GistPageIsLeaf(page))
		{
			/* We hit the bottom, so we're done. */
			UnlockReleaseBuffer(buffer);
			break;
		}

		/*
		 * Pick the first downlink on the page, and follow it. It doesn't
		 * matter which downlink we choose, the tree has the same depth
		 * everywhere, so we just pick the first one.
		 */
		itup = (IndexTuple) PageGetItem(page,
										PageGetItemId(page, FirstOffsetNumber));
		blkno = ItemPointerGetBlockNumber(&(itup->t_tid));
		UnlockReleaseBuffer(buffer);

		/*
		 * We're going down on the tree. It means that there is yet one more
		 * level in the tree.
		 */
		maxLevel++;
	}
	return maxLevel;
}


/*
 * Routines for managing the parent map.
 *
 * Whenever a page is split, we need to insert the downlinks into the parent.
 * We need to somehow find the parent page to do that. In normal insertions,
 * we keep a stack of nodes visited when we descend the tree. However, in
 * buffering build, we can start descending the tree from any internal node,
 * when we empty a buffer by cascading tuples to its children. So we don't
 * have a full stack up to the root available at that time.
 *
 * So instead, we maintain a hash table to track the parent of every internal
 * page. We don't need to track the parents of leaf nodes, however. Whenever
 * we insert to a leaf, we've just descended down from its parent, so we know
 * its immediate parent already. This helps a lot to limit the memory used
 * by this hash table.
 *
 * Whenever an internal node is split, the parent map needs to be updated.
 * the parent of the new child page needs to be recorded, and also the
 * entries for all page whose downlinks are moved to a new page at the split
 * needs to be updated.
 *
 * We also update the parent map whenever we descend the tree. That might seem
 * unnecessary, because we maintain the map whenever a downlink is moved or
 * created, but it is needed because we switch to buffering mode after
 * creating a tree with regular index inserts. Any pages created before
 * switching to buffering mode will not be present in the parent map initially,
 * but will be added there the first time we visit them.
 */

typedef struct
{
	BlockNumber childblkno;		/* hash key */
	BlockNumber parentblkno;
} ParentMapEntry;

static void
gistInitParentMap(GISTBuildState *buildstate)
{
	HASHCTL		hashCtl;

	hashCtl.keysize = sizeof(BlockNumber);
	hashCtl.entrysize = sizeof(ParentMapEntry);
	hashCtl.hcxt = CurrentMemoryContext;
	buildstate->parentMap = hash_create("gistbuild parent map",
										1024,
										&hashCtl,
										HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static void
gistMemorizeParent(GISTBuildState *buildstate, BlockNumber child, BlockNumber parent)
{
	ParentMapEntry *entry;
	bool		found;

	entry = (ParentMapEntry *) hash_search(buildstate->parentMap,
										   &child,
										   HASH_ENTER,
										   &found);
	entry->parentblkno = parent;
}

/*
 * Scan all downlinks on a page, and memorize their parent.
 */
static void
gistMemorizeAllDownlinks(GISTBuildState *buildstate, Buffer parentbuf)
{
	OffsetNumber maxoff;
	OffsetNumber off;
	BlockNumber parentblkno = BufferGetBlockNumber(parentbuf);
	Page		page = BufferGetPage(parentbuf);

	Assert(!GistPageIsLeaf(page));

	maxoff = PageGetMaxOffsetNumber(page);
	for (off = FirstOffsetNumber; off <= maxoff; off++)
	{
		ItemId		iid = PageGetItemId(page, off);
		IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);
		BlockNumber childblkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));

		gistMemorizeParent(buildstate, childblkno, parentblkno);
	}
}

static BlockNumber
gistGetParent(GISTBuildState *buildstate, BlockNumber child)
{
	ParentMapEntry *entry;
	bool		found;

	/* Find node buffer in hash table */
	entry = (ParentMapEntry *) hash_search(buildstate->parentMap,
										   &child,
										   HASH_FIND,
										   &found);
	if (!found)
		elog(ERROR, "could not find parent of block %u in lookup table", child);

	return entry->parentblkno;
}

/*
 * Create parallel context, and launch workers for leader.
 *
 * buildstate argument should be initialized
 *
 * isconcurrent indicates if operation is CREATE INDEX CONCURRENTLY.
 *
 * request is the target number of parallel worker processes to launch.
 *
 * Sets buildstate's gistLeader, which caller must use to shut down parallel
 * mode by passing it to _gist_end_parallel() at the very end of its index
 * build.  If not even a single worker process can be launched, this is
 * never set, and caller should proceed with a serial index build.
 */
static void
_gist_begin_parallel(GISTBuildState *buildstate, Relation heap, Relation index,
					 bool isconcurrent, int request)
{
	ParallelContext *pcxt;
	int			nparticipants;
	Snapshot	snapshot;
	Size		estgistshared;
	GISTShared *gistshared;
	GISTLeader *gistleader = (GISTLeader *) palloc0(sizeof(GISTLeader));
	WalUsage   *walusage;
	BufferUsage *bufferusage;
	bool		leaderparticipates = true;
	int			querylen;

#ifdef DISABLE_LEADER_PARTICIPATION
	leaderparticipates = false;
#endif

	/*
	 * Enter parallel mode, and create context for parallel build of GIST
	 * index
	 */
	EnterParallelMode();
	Assert(request > 0);
	pcxt = CreateParallelContext("postgres", "_gist_parallel_build_main",
								 request);

	nparticipants = leaderparticipates ? request + 1 : request;

	/*
	 * Prepare for scan of the base relation.  In a normal index build, we use
	 * SnapshotAny because we must retrieve all tuples and do our own time
	 * qual checks (because we have to index RECENTLY_DEAD tuples).  In a
	 * concurrent build, we take a regular MVCC snapshot and index whatever's
	 * live according to that.
	 */
	if (!isconcurrent)
		snapshot = SnapshotAny;
	else
		snapshot = RegisterSnapshot(GetTransactionSnapshot());

	/*
	 * Estimate size for our own PARALLEL_KEY_GIST_SHARED workspace.
	 */
	estgistshared = _gist_parallel_estimate_shared(heap, snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, estgistshared);

	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/*
	 * Estimate space for WalUsage and BufferUsage -- PARALLEL_KEY_WAL_USAGE
	 * and PARALLEL_KEY_BUFFER_USAGE.
	 *
	 * If there are no extensions loaded that care, we could skip this.  We
	 * have no way of knowing whether anyone's looking at pgWalUsage or
	 * pgBufferUsage, so do it unconditionally.
	 */
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* Finally, estimate PARALLEL_KEY_QUERY_TEXT space */
	if (debug_query_string)
	{
		querylen = strlen(debug_query_string);
		shm_toc_estimate_chunk(&pcxt->estimator, querylen + 1);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}
	else
		querylen = 0;			/* keep compiler quiet */

	/* Everyone's had a chance to ask for space, so now create the DSM */
	InitializeParallelDSM(pcxt);

	/* If no DSM segment was available, back out (do serial build) */
	if (pcxt->seg == NULL)
	{
		if (IsMVCCSnapshot(snapshot))
			UnregisterSnapshot(snapshot);
		DestroyParallelContext(pcxt);
		ExitParallelMode();
		return;
	}

	/* Store shared build state, for which we reserved space */
	gistshared = (GISTShared *) shm_toc_allocate(pcxt->toc, estgistshared);
	/* Initialize immutable state */
	gistshared->heaprelid = RelationGetRelid(heap);
	gistshared->indexrelid = RelationGetRelid(index);
	gistshared->isconcurrent = isconcurrent;
	gistshared->nparticipants = nparticipants;

	/* */
	gistshared->buildMode = buildstate->buildMode;
	gistshared->freespace = buildstate->freespace;

	ConditionVariableInit(&gistshared->workersdonecv);
	SpinLockInit(&gistshared->mutex);

	/* Initialize mutable state */
	gistshared->nparticipantsdone = 0;
	gistshared->reltuples = 0.0;
	gistshared->indtuples = 0.0;

	table_parallelscan_initialize(heap,
								  ParallelTableScanFromGistShared(gistshared),
								  snapshot);

	/* Store shared state, for which we reserved space. */
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_GIST_SHARED, gistshared);

	/* Store query string for workers */
	if (debug_query_string)
	{
		char	   *sharedquery;

		sharedquery = (char *) shm_toc_allocate(pcxt->toc, querylen + 1);
		memcpy(sharedquery, debug_query_string, querylen + 1);
		shm_toc_insert(pcxt->toc, PARALLEL_KEY_QUERY_TEXT, sharedquery);
	}

	/*
	 * Allocate space for each worker's WalUsage and BufferUsage; no need to
	 * initialize.
	 */
	walusage = shm_toc_allocate(pcxt->toc,
								mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_WAL_USAGE, walusage);
	bufferusage = shm_toc_allocate(pcxt->toc,
								   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_BUFFER_USAGE, bufferusage);

	/* Launch workers, saving status for leader/caller */
	LaunchParallelWorkers(pcxt);
	gistleader->pcxt = pcxt;
	gistleader->nparticipants = pcxt->nworkers_launched;
	if (leaderparticipates)
		gistleader->nparticipants++;
	gistleader->gistshared = gistshared;
	gistleader->snapshot = snapshot;
	gistleader->walusage = walusage;
	gistleader->bufferusage = bufferusage;

	/* If no workers were successfully launched, back out (do serial build) */
	if (pcxt->nworkers_launched == 0)
	{
		_gist_end_parallel(gistleader, NULL);
		return;
	}

	/* Save leader state now that it's clear build will be parallel */
	buildstate->is_parallel = true;
	buildstate->gist_leader = gistleader;

	/* Join heap scan ourselves */
	if (leaderparticipates)
		_gist_leader_participate_as_worker(buildstate, heap, index);

	/*
	 * Caller needs to wait for all launched workers when we return.  Make
	 * sure that the failure-to-start case will not hang forever.
	 */
	WaitForParallelWorkersToAttach(pcxt);
}

/*
 * Shut down workers, destroy parallel context, and end parallel mode.
 */
static void
_gist_end_parallel(GISTLeader *gistleader, GISTBuildState *state)
{
	int			i;

	/* Shutdown worker processes */
	WaitForParallelWorkersToFinish(gistleader->pcxt);

	/*
	 * Next, accumulate WAL usage.  (This must wait for the workers to finish,
	 * or we might get incomplete data.)
	 */
	for (i = 0; i < gistleader->pcxt->nworkers_launched; i++)
		InstrAccumParallelQuery(&gistleader->bufferusage[i], &gistleader->walusage[i]);

	/* Free last reference to MVCC snapshot, if one was used */
	if (IsMVCCSnapshot(gistleader->snapshot))
		UnregisterSnapshot(gistleader->snapshot);
	DestroyParallelContext(gistleader->pcxt);
	ExitParallelMode();
}

/*
 * Within leader, wait for end of heap scan.
 *
 * When called, parallel heap scan started by _gist_begin_parallel() will
 * already be underway within worker processes (when leader participates
 * as a worker, we should end up here just as workers are finishing).
 *
 * Returns the total number of heap tuples scanned.
 *
 * FIXME Maybe needs to flush data if GIST_BUFFERING_ACTIVE, a bit like in
 * the serial build?
 */
static double
_gist_parallel_heapscan(GISTBuildState *state)
{
	GISTShared *gistshared = state->gist_leader->gistshared;
	int			nparticipants;

	nparticipants = state->gist_leader->nparticipants;
	for (;;)
	{
		SpinLockAcquire(&gistshared->mutex);
		if (gistshared->nparticipantsdone == nparticipants)
		{
			/* copy the data into leader state */
			state->indtuples = gistshared->indtuples;

			SpinLockRelease(&gistshared->mutex);
			break;
		}
		SpinLockRelease(&gistshared->mutex);

		ConditionVariableSleep(&gistshared->workersdonecv,
							   WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
	}

	ConditionVariableCancelSleep();

	return state->indtuples;
}



/*
 * Returns size of shared memory required to store state for a parallel
 * gist index build based on the snapshot its parallel scan will use.
 */
static Size
_gist_parallel_estimate_shared(Relation heap, Snapshot snapshot)
{
	/* c.f. shm_toc_allocate as to why BUFFERALIGN is used */
	return add_size(BUFFERALIGN(sizeof(GISTShared)),
					table_parallelscan_estimate(heap, snapshot));
}

/*
 * Within leader, participate as a parallel worker.
 */
static void
_gist_leader_participate_as_worker(GISTBuildState *buildstate,
								   Relation heap, Relation index)
{
	GISTLeader *gistleader = buildstate->gist_leader;
	int			workmem;

	/*
	 * Might as well use reliable figure when doling out maintenance_work_mem
	 * (when requested number of workers were not launched, this will be
	 * somewhat higher than it is for other workers).
	 */
	workmem = maintenance_work_mem / gistleader->nparticipants;

	/* Perform work common to all participants */
	_gist_parallel_scan_and_build(buildstate, gistleader->gistshared,
								  heap, index, workmem, true);
}

/*
 * Perform a worker's portion of a parallel scan and insert.
 *
 * When this returns, workers are done, and need only release resources.
 */
static void
_gist_parallel_scan_and_build(GISTBuildState *state,
							  GISTShared *gistshared,
							  Relation heap, Relation index,
							  int workmem, bool progress)
{
	TableScanDesc scan;
	double		reltuples;
	IndexInfo  *indexInfo;
	MemoryContext oldcxt = CurrentMemoryContext;

	/* Join parallel scan */
	indexInfo = BuildIndexInfo(index);
	indexInfo->ii_Concurrent = gistshared->isconcurrent;

	scan = table_beginscan_parallel(heap,
									ParallelTableScanFromGistShared(gistshared));

	reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
									   gistBuildParallelCallback, state, scan);

	/*
	 * If buffering was used, flush out all the tuples that are still in the
	 * buffers.
	 */
	if (state->buildMode == GIST_BUFFERING_ACTIVE)
	{
		elog(DEBUG1, "all tuples processed, emptying buffers");
		gistEmptyAllBuffers(state);
		gistFreeBuildBuffers(state->gfbb);
	}

	/* okay, all heap tuples are indexed */
	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(state->giststate->tempCxt);

	/* FIXME Do we need to do something else with active buffering? */

	/*
	 * Done.  Record ambuild statistics.
	 */
	SpinLockAcquire(&gistshared->mutex);
	gistshared->nparticipantsdone++;
	gistshared->reltuples += reltuples;
	gistshared->indtuples += state->indtuples;
	SpinLockRelease(&gistshared->mutex);

	/* Notify leader */
	ConditionVariableSignal(&gistshared->workersdonecv);
}

/*
 * Perform work within a launched parallel process.
 */
void
_gist_parallel_build_main(dsm_segment *seg, shm_toc *toc)
{
	char	   *sharedquery;
	GISTShared *gistshared;
	GISTBuildState buildstate;
	Relation	heapRel;
	Relation	indexRel;
	LOCKMODE	heapLockmode;
	LOCKMODE	indexLockmode;
	WalUsage   *walusage;
	BufferUsage *bufferusage;
	int			workmem;

	/*
	 * The only possible status flag that can be set to the parallel worker is
	 * PROC_IN_SAFE_IC.
	 */
	Assert((MyProc->statusFlags == 0) ||
		   (MyProc->statusFlags == PROC_IN_SAFE_IC));

	/* Set debug_query_string for individual workers first */
	sharedquery = shm_toc_lookup(toc, PARALLEL_KEY_QUERY_TEXT, true);
	debug_query_string = sharedquery;

	/* Report the query string from leader */
	pgstat_report_activity(STATE_RUNNING, debug_query_string);

	/* Look up GIST shared state */
	gistshared = shm_toc_lookup(toc, PARALLEL_KEY_GIST_SHARED, false);

	/* Open relations using lock modes known to be obtained by index.c */
	if (!gistshared->isconcurrent)
	{
		heapLockmode = ShareLock;
		indexLockmode = AccessExclusiveLock;
	}
	else
	{
		heapLockmode = ShareUpdateExclusiveLock;
		indexLockmode = RowExclusiveLock;
	}

	/* Open relations within worker */
	heapRel = table_open(gistshared->heaprelid, heapLockmode);
	indexRel = index_open(gistshared->indexrelid, indexLockmode);

	buildstate.indexrel = indexRel;
	buildstate.heaprel = heapRel;
	buildstate.sortstate = NULL;
	buildstate.giststate = initGISTstate(indexRel);

	buildstate.is_parallel = true;
	buildstate.gist_leader = NULL;

	/*
	 * Create a temporary memory context that is reset once for each tuple
	 * processed.  (Note: we don't bother to make this a child of the
	 * giststate's scanCxt, so we have to delete it separately at the end.)
	 */
	buildstate.giststate->tempCxt = createTempGistContext();

	/* FIXME */
	buildstate.buildMode = gistshared->buildMode;
	buildstate.freespace = gistshared->freespace;

	buildstate.indtuples = 0;
	buildstate.indtuplesSize = 0;

	/* Prepare to track buffer usage during parallel execution */
	InstrStartParallelQuery();

	/*
	 * Might as well use reliable figure when doling out maintenance_work_mem
	 * (when requested number of workers were not launched, this will be
	 * somewhat higher than it is for other workers).
	 */
	workmem = maintenance_work_mem / gistshared->nparticipants;

	_gist_parallel_scan_and_build(&buildstate, gistshared,
								  heapRel, indexRel, workmem, false);

	/* Report WAL/buffer usage during parallel execution */
	bufferusage = shm_toc_lookup(toc, PARALLEL_KEY_BUFFER_USAGE, false);
	walusage = shm_toc_lookup(toc, PARALLEL_KEY_WAL_USAGE, false);
	InstrEndParallelQuery(&bufferusage[ParallelWorkerNumber],
						  &walusage[ParallelWorkerNumber]);

	index_close(indexRel, indexLockmode);
	table_close(heapRel, heapLockmode);
}
