/*-------------------------------------------------------------------------
 *
 * genam.h
 *	  POSTGRES generalized index access method definitions.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/genam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GENAM_H
#define GENAM_H

#include "access/sdir.h"
#include "access/skey.h"
#include "nodes/tidbitmap.h"
#include "storage/bufmgr.h"
#include "storage/lockdefs.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

/* We don't want this file to depend on execnodes.h. */
struct IndexInfo;

/*
 * Struct for statistics returned by ambuild
 */
typedef struct IndexBuildResult
{
	double		heap_tuples;	/* # of tuples seen in parent table */
	double		index_tuples;	/* # of tuples inserted into index */
} IndexBuildResult;

/*
 * Struct for input arguments passed to ambulkdelete and amvacuumcleanup
 *
 * num_heap_tuples is accurate only when estimated_count is false;
 * otherwise it's just an estimate (currently, the estimate is the
 * prior value of the relation's pg_class.reltuples field, so it could
 * even be -1).  It will always just be an estimate during ambulkdelete.
 */
typedef struct IndexVacuumInfo
{
	Relation	index;			/* the index being vacuumed */
	Relation	heaprel;		/* the heap relation the index belongs to */
	bool		analyze_only;	/* ANALYZE (without any actual vacuum) */
	bool		report_progress;	/* emit progress.h status reports */
	bool		estimated_count;	/* num_heap_tuples is an estimate */
	int			message_level;	/* ereport level for progress messages */
	double		num_heap_tuples;	/* tuples remaining in heap */
	BufferAccessStrategy strategy;	/* access strategy for reads */
} IndexVacuumInfo;

/*
 * Struct for statistics returned by ambulkdelete and amvacuumcleanup
 *
 * This struct is normally allocated by the first ambulkdelete call and then
 * passed along through subsequent ones until amvacuumcleanup; however,
 * amvacuumcleanup must be prepared to allocate it in the case where no
 * ambulkdelete calls were made (because no tuples needed deletion).
 * Note that an index AM could choose to return a larger struct
 * of which this is just the first field; this provides a way for ambulkdelete
 * to communicate additional private data to amvacuumcleanup.
 *
 * Note: pages_newly_deleted is the number of pages in the index that were
 * deleted by the current vacuum operation.  pages_deleted and pages_free
 * refer to free space within the index file.
 *
 * Note: Some index AMs may compute num_index_tuples by reference to
 * num_heap_tuples, in which case they should copy the estimated_count field
 * from IndexVacuumInfo.
 */
typedef struct IndexBulkDeleteResult
{
	BlockNumber num_pages;		/* pages remaining in index */
	bool		estimated_count;	/* num_index_tuples is an estimate */
	double		num_index_tuples;	/* tuples remaining */
	double		tuples_removed; /* # removed during vacuum operation */
	BlockNumber pages_newly_deleted;	/* # pages marked deleted by us  */
	BlockNumber pages_deleted;	/* # pages marked deleted (could be by us) */
	BlockNumber pages_free;		/* # pages available for reuse */
} IndexBulkDeleteResult;

/* Typedef for callback function to determine if a tuple is bulk-deletable */
typedef bool (*IndexBulkDeleteCallback) (ItemPointer itemptr, void *state);

/* struct definitions appear in relscan.h */
typedef struct IndexScanDescData *IndexScanDesc;
typedef struct SysScanDescData *SysScanDesc;

typedef struct ParallelIndexScanDescData *ParallelIndexScanDesc;

/*
 * Enumeration specifying the type of uniqueness check to perform in
 * index_insert().
 *
 * UNIQUE_CHECK_YES is the traditional Postgres immediate check, possibly
 * blocking to see if a conflicting transaction commits.
 *
 * For deferrable unique constraints, UNIQUE_CHECK_PARTIAL is specified at
 * insertion time.  The index AM should test if the tuple is unique, but
 * should not throw error, block, or prevent the insertion if the tuple
 * appears not to be unique.  We'll recheck later when it is time for the
 * constraint to be enforced.  The AM must return true if the tuple is
 * known unique, false if it is possibly non-unique.  In the "true" case
 * it is safe to omit the later recheck.
 *
 * When it is time to recheck the deferred constraint, a pseudo-insertion
 * call is made with UNIQUE_CHECK_EXISTING.  The tuple is already in the
 * index in this case, so it should not be inserted again.  Rather, just
 * check for conflicting live tuples (possibly blocking).
 */
typedef enum IndexUniqueCheck
{
	UNIQUE_CHECK_NO,			/* Don't do any uniqueness checking */
	UNIQUE_CHECK_YES,			/* Enforce uniqueness at insertion time */
	UNIQUE_CHECK_PARTIAL,		/* Test uniqueness, but no error */
	UNIQUE_CHECK_EXISTING,		/* Check if existing tuple is unique */
} IndexUniqueCheck;


/* Nullable "ORDER BY col op const" distance */
typedef struct IndexOrderByDistance
{
	double		value;
	bool		isnull;
} IndexOrderByDistance;

/*
 * generalized index_ interface routines (in indexam.c)
 */

/*
 * IndexScanIsValid
 *		True iff the index scan is valid.
 */
#define IndexScanIsValid(scan) PointerIsValid(scan)

extern Relation index_open(Oid relationId, LOCKMODE lockmode);
extern void index_close(Relation relation, LOCKMODE lockmode);

extern bool index_insert(Relation indexRelation,
						 Datum *values, bool *isnull,
						 ItemPointer heap_t_ctid,
						 Relation heapRelation,
						 IndexUniqueCheck checkUnique,
						 bool indexUnchanged,
						 struct IndexInfo *indexInfo);
extern void index_insert_cleanup(Relation indexRelation,
								 struct IndexInfo *indexInfo);

extern IndexScanDesc index_beginscan(Relation heapRelation,
									 Relation indexRelation,
									 Snapshot snapshot,
									 int nkeys, int norderbys,
									 int prefetch_max);
extern IndexScanDesc index_beginscan_bitmap(Relation indexRelation,
											Snapshot snapshot,
											int nkeys);
extern void index_rescan(IndexScanDesc scan,
						 ScanKey keys, int nkeys,
						 ScanKey orderbys, int norderbys);
extern void index_endscan(IndexScanDesc scan);
extern void index_markpos(IndexScanDesc scan);
extern void index_restrpos(IndexScanDesc scan);
extern Size index_parallelscan_estimate(Relation indexRelation, Snapshot snapshot);
extern void index_parallelscan_initialize(Relation heapRelation,
										  Relation indexRelation, Snapshot snapshot,
										  ParallelIndexScanDesc target);
extern void index_parallelrescan(IndexScanDesc scan);
extern IndexScanDesc index_beginscan_parallel(Relation heaprel,
											  Relation indexrel, int nkeys, int norderbys,
											  ParallelIndexScanDesc pscan,
											  int prefetch_max);
extern ItemPointer index_getnext_tid(IndexScanDesc scan,
									 ScanDirection direction);
extern ItemPointer index_getnext_tid_vm(IndexScanDesc scan,
										ScanDirection direction,
										bool *all_visible);
struct TupleTableSlot;
extern bool index_fetch_heap(IndexScanDesc scan, struct TupleTableSlot *slot);
extern bool index_getnext_slot(IndexScanDesc scan, ScanDirection direction,
							   struct TupleTableSlot *slot);
extern int64 index_getbitmap(IndexScanDesc scan, TIDBitmap *bitmap);

extern IndexBulkDeleteResult *index_bulk_delete(IndexVacuumInfo *info,
												IndexBulkDeleteResult *istat,
												IndexBulkDeleteCallback callback,
												void *callback_state);
extern IndexBulkDeleteResult *index_vacuum_cleanup(IndexVacuumInfo *info,
												   IndexBulkDeleteResult *istat);
extern bool index_can_return(Relation indexRelation, int attno);
extern RegProcedure index_getprocid(Relation irel, AttrNumber attnum,
									uint16 procnum);
extern FmgrInfo *index_getprocinfo(Relation irel, AttrNumber attnum,
								   uint16 procnum);
extern void index_store_float8_orderby_distances(IndexScanDesc scan,
												 Oid *orderByTypes,
												 IndexOrderByDistance *distances,
												 bool recheckOrderBy);
extern bytea *index_opclass_options(Relation indrel, AttrNumber attnum,
									Datum attoptions, bool validate);


/*
 * index access method support routines (in genam.c)
 */
extern IndexScanDesc RelationGetIndexScan(Relation indexRelation,
										  int nkeys, int norderbys);
extern void IndexScanEnd(IndexScanDesc scan);
extern char *BuildIndexValueDescription(Relation indexRelation,
										const Datum *values, const bool *isnull);
extern TransactionId index_compute_xid_horizon_for_tuples(Relation irel,
														  Relation hrel,
														  Buffer ibuf,
														  OffsetNumber *itemnos,
														  int nitems);

/*
 * heap-or-index access to system catalogs (in genam.c)
 */
extern SysScanDesc systable_beginscan(Relation heapRelation,
									  Oid indexId,
									  bool indexOK,
									  Snapshot snapshot,
									  int nkeys, ScanKey key);
extern HeapTuple systable_getnext(SysScanDesc sysscan);
extern bool systable_recheck_tuple(SysScanDesc sysscan, HeapTuple tup);
extern void systable_endscan(SysScanDesc sysscan);
extern SysScanDesc systable_beginscan_ordered(Relation heapRelation,
											  Relation indexRelation,
											  Snapshot snapshot,
											  int nkeys, ScanKey key);
extern HeapTuple systable_getnext_ordered(SysScanDesc sysscan,
										  ScanDirection direction);
extern void systable_endscan_ordered(SysScanDesc sysscan);

/*
 * Cache of recently prefetched blocks, organized as a hash table of
 * small LRU caches. Doesn't need to be perfectly accurate, but we
 * aim to make false positives/negatives reasonably low.
 */
typedef struct PrefetchCacheEntry {
	BlockNumber		block;
	uint64			request;
} PrefetchCacheEntry;

/*
 * Size of the cache of recently prefetched blocks - shouldn't be too
 * small or too large. 1024 seems about right, it covers ~8MB of data.
 * It's somewhat arbitrary, there's no particular formula saying it
 * should not be higher/lower.
 *
 * The cache is structured as an array of small LRU caches, so the total
 * size needs to be a multiple of LRU size. The LRU should be tiny to
 * keep linear search cheap enough.
 *
 * XXX Maybe we could consider effective_cache_size or something?
 */
#define		PREFETCH_LRU_SIZE		8
#define		PREFETCH_LRU_COUNT		128
#define		PREFETCH_CACHE_SIZE		(PREFETCH_LRU_SIZE * PREFETCH_LRU_COUNT)

/*
 * Used to detect sequential patterns (and disable prefetching).
 */
#define		PREFETCH_QUEUE_HISTORY			8
#define		PREFETCH_SEQ_PATTERN_BLOCKS		4

typedef struct PrefetchEntry
{
	ItemPointerData		tid;
	bool				all_visible;
} PrefetchEntry;

typedef struct IndexPrefetchData
{
	/*
	 * XXX We need to disable this in some cases (e.g. when using index-only
	 * scans, we don't want to prefetch pages). Or maybe we should prefetch
	 * only pages that are not all-visible, that'd be even better.
	 */
	int			prefetchTarget;	/* how far we should be prefetching */
	int			prefetchMaxTarget;	/* maximum prefetching distance */
	int			prefetchReset;	/* reset to this distance on rescan */
	bool		prefetchDone;	/* did we get all TIDs from the index? */

	/* runtime statistics */
	uint64		countAll;		/* all prefetch requests */
	uint64		countPrefetch;	/* actual prefetches */
	uint64		countSkipSequential;
	uint64		countSkipCached;

	/* used when prefetching index-only scans */
	Buffer		vmBuffer;

	/*
	 * Queue of TIDs to prefetch.
	 *
	 * XXX Sizing for MAX_IO_CONCURRENCY may be overkill, but it seems simpler
	 * than dynamically adjusting for custom values.
	 */
	PrefetchEntry	queueItems[MAX_IO_CONCURRENCY];
	uint64			queueIndex;	/* next TID to prefetch */
	uint64			queueStart;	/* first valid TID in queue */
	uint64			queueEnd;	/* first invalid (empty) TID in queue */

	/*
	 * A couple of last prefetched blocks, used to check for certain access
	 * pattern and skip prefetching - e.g. for sequential access).
	 *
	 * XXX Separate from the main queue, because we only want to compare the
	 * block numbers, not the whole TID. In sequential access it's likely we
	 * read many items from each page, and we don't want to check many items
	 * (as that is much more expensive).
	 */
	BlockNumber		blockItems[PREFETCH_QUEUE_HISTORY];
	uint64			blockIndex;	/* index in the block (points to the first
								 * empty entry)*/

	/*
	 * Cache of recently prefetched blocks, organized as a hash table of
	 * small LRU caches.
	 */
	uint64				prefetchReqNumber;
	PrefetchCacheEntry	prefetchCache[PREFETCH_CACHE_SIZE];

} IndexPrefetchData;

#define PREFETCH_QUEUE_INDEX(a)	((a) % (MAX_IO_CONCURRENCY))
#define PREFETCH_QUEUE_EMPTY(p)	((p)->queueEnd == (p)->queueIndex)
#define PREFETCH_ENABLED(p)		((p) && ((p)->prefetchMaxTarget > 0))
#define PREFETCH_FULL(p)		((p)->queueEnd - (p)->queueIndex == (p)->prefetchTarget)
#define PREFETCH_DONE(p)		((p) && ((p)->prefetchDone && PREFETCH_QUEUE_EMPTY(p)))
#define PREFETCH_ACTIVE(p)		(PREFETCH_ENABLED(p) && !(p)->prefetchDone)
#define PREFETCH_BLOCK_INDEX(v)	((v) % PREFETCH_QUEUE_HISTORY)

#endif							/* GENAM_H */
