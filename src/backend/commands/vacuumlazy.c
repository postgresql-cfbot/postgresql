/*-------------------------------------------------------------------------
 *
 * vacuumlazy.c
 *	  Concurrent ("lazy") vacuuming.
 *
 *
 * The major space usage for LAZY VACUUM is storage for the array of dead tuple
 * TIDs.  We want to ensure we can vacuum even the very largest relations with
 * finite memory space usage.  To do that, we set upper bounds on the number of
 * tuples we will keep track of at once.
 *
 * We are willing to use at most maintenance_work_mem (or perhaps
 * autovacuum_work_mem) memory space to keep track of dead tuples.  We
 * initially allocate an array of TIDs of that size, with an upper limit that
 * depends on table size (this limit ensures we don't allocate a huge area
 * uselessly for vacuuming small tables).  If the array threatens to overflow,
 * we suspend the heap scan phase and perform a pass of index cleanup and page
 * compaction, then resume the heap scan with an empty TID array.
 *
 * If we're processing a table with no indexes, we can just vacuum each page
 * as we go; there's no need to save up multiple tuples to minimize the number
 * of index scans performed.  So we don't use maintenance_work_mem memory for
 * the TID array, just enough to hold as many heap tuples as fit on one page.
 *
 * In PostgreSQL 10, we support a parallel option for lazy vacuum. In parallel
 * lazy vacuum, multiple vacuum worker processes get blocks in parallel using
 * parallel heap scan and process them. If a table with indexes the parallel
 * vacuum workers vacuum the heap and indexes in parallel.  Also, since dead
 * tuple TIDs is shared with all vacuum processes including the leader process
 * the parallel vacuum processes have to make two synchronization points in
 * lazy vacuum processing: before starting vacuum and before clearing dead
 * tuple TIDs. In those two points the leader treats dead tuple TIDs as an
 * arbiter. The information required by parallel lazy vacuum such as the
 * statistics of table, parallel heap scan description have to be shared with
 * all vacuum processes, and table statistics are funneled by the leader
 * process after finished. However, dead tuple TIDs need to be shared only
 * when the table has indexes. For table with no indexes, each parallel worker
 * processes blocks and vacuum them independently.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/vacuumlazy.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/transam.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/storage.h"
#include "commands/dbcommands.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "portability/instr_time.h"
#include "postmaster/autovacuum.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"

//#define PLV_TIME

/*
 * Space/time tradeoff parameters: do these need to be user-tunable?
 *
 * To consider truncating the relation, we want there to be at least
 * REL_TRUNCATE_MINIMUM or (relsize / REL_TRUNCATE_FRACTION) (whichever
 * is less) potentially-freeable pages.
 */
#define REL_TRUNCATE_MINIMUM	1000
#define REL_TRUNCATE_FRACTION	16

/*
 * Timing parameters for truncate locking heuristics.
 *
 * These were not exposed as user tunable GUC values because it didn't seem
 * that the potential for improvement was great enough to merit the cost of
 * supporting them.
 */
#define VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL		20	/* ms */
#define VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL		50	/* ms */
#define VACUUM_TRUNCATE_LOCK_TIMEOUT			5000	/* ms */

/*
 * Guesstimation of number of dead tuples per page.  This is used to
 * provide an upper limit to memory allocated when vacuuming small
 * tables.
 */
#define LAZY_ALLOC_TUPLES		MaxHeapTuplesPerPage

/*
 * Before we consider skipping a page that's marked as clean in
 * visibility map, we must've seen at least this many clean pages.
 */
#define SKIP_PAGES_THRESHOLD	((BlockNumber) 32)

/*
 * Size of the prefetch window for lazy vacuum backwards truncation scan.
 * Needs to be a power of 2.
 */
#define PREFETCH_SIZE			((BlockNumber) 32)

/* DSM key for parallel lazy vacuum */
#define VACUUM_KEY_PARALLEL_SCAN	UINT64CONST(0xFFFFFFFFFFF00001)
#define VACUUM_KEY_VACUUM_STATS		UINT64CONST(0xFFFFFFFFFFF00002)
#define VACUUM_KEY_INDEX_STATS	    UINT64CONST(0xFFFFFFFFFFF00003)
#define VACUUM_KEY_DEAD_TUPLE_CTL	UINT64CONST(0xFFFFFFFFFFF00004)
#define VACUUM_KEY_DEAD_TUPLES		UINT64CONST(0xFFFFFFFFFFF00005)
#define VACUUM_KEY_PARALLEL_STATE	UINT64CONST(0xFFFFFFFFFFF00006)

/*
 * see note of lazy_scan_heap_get_nextpage about forcing scanning of
 * last page
 */
#define FORCE_CHECK_PAGE(blk) \
	(blkno == (blk - 1) && should_attempt_truncation(vacrelstats))

/* Check if given index is assigned to this parallel vacuum worker */
#define IsAssignedIndex(i, pstate) \
	(pstate == NULL || \
	 (((i) % ((LVParallelState *) (pstate))->nworkers -1 ) == ParallelWorkerNumber))

#define IsDeadTupleShared(lvstate) \
	((LVState *)(lvstate))->parallel_mode && \
	((LVState *)(lvstate))->vacrelstats->nindexes > 0

/* Vacuum worker state for parallel lazy vacuum */
#define VACSTATE_SCAN			0x1	/* heap scan phase */
#define VACSTATE_VACUUM			0x2	/* vacuuming on table and index */

/*
 * Vacuum relevant options and thresholds we need share with parallel
 * vacuum workers.
 */
typedef struct VacuumInfo
{
	int				options;	/* VACUUM options */
	bool			aggressive;	/* does each worker need to aggressive vacuum? */
	TransactionId	oldestxmin;
	TransactionId	freezelimit;
	MultiXactId		multixactcutoff;
	int				elevel;
} VacuumInfo;

/* Struct for index statistics that are used for parallel lazy vacuum */
typedef struct LVIndStats
{
	bool		updated;	/* need to be updated? */
	BlockNumber	num_pages;
	BlockNumber	num_tuples;
} LVIndStats;

/* Struct for parallel lazy vacuum state */
typedef struct LVParallelState
{
	int nworkers;			/* # of process doing vacuum */
	VacuumInfo	info;
	int	state;				/* current parallel vacuum status */
	int	finish_count;
	ConditionVariable cv;
	slock_t	mutex;
} LVParallelState;

/* Struct for control dead tuple TIDs array */
typedef struct LVDeadTupleCtl
{
	int			dt_max;	/* # slots allocated in array */
	int 		dt_count; /* # of dead tuple */

	/* Used only for parallel lazy vacuum */
	int			dt_index;
	slock_t 	mutex;
} LVDeadTupleCtl;

typedef struct LVRelStats
{
	int			nindexes; /* > 0 means two-pass strategy; = 0 means one-pass */
	/* Overall statistics about rel */
	BlockNumber old_rel_pages;	/* previous value of pg_class.relpages */
	BlockNumber rel_pages;		/* total number of pages */
	BlockNumber scanned_pages;	/* number of pages we examined */
	BlockNumber pinskipped_pages;	/* # of pages we skipped due to a pin */
	BlockNumber frozenskipped_pages;	/* # of frozen pages we skipped */
	BlockNumber tupcount_pages; /* pages whose tuples we counted */
	double		scanned_tuples; /* counts only tuples on tupcount_pages */
	double		old_rel_tuples; /* previous value of pg_class.reltuples */
	double		new_rel_tuples; /* new estimated total # of tuples */
	double		new_dead_tuples;	/* new estimated total # of dead tuples */
	double		tuples_deleted;
	int			num_index_scans;
	BlockNumber pages_removed;
	BlockNumber nonempty_pages; /* actually, last nonempty page + 1 */
	TransactionId latestRemovedXid;
	bool		lock_waiter_detected;
} LVRelStats;

/* Struct for lazy vacuum execution */
typedef struct LVState
{
	bool		parallel_mode;
	LVRelStats *vacrelstats;
	/*
	 * Used when both parallel and non-parallel lazy vacuum, but in parallel
	 * lazy vacuum and table with index, dtctl points to a dynamic shared memory
	 * and controlled by dtctl struct.
	 */
	LVDeadTupleCtl	*dtctl;
	ItemPointer	deadtuples;

	/* Used only for parallel lazy vacuum */
	ParallelContext *pcxt;
	LVParallelState *pstate;
	ParallelHeapScanDesc pscan;
	LVIndStats *indstats;
} LVState;

/*
 * Scan description data for lazy vacuum. In parallel lazy vacuum,
 * we use only heapscan instead.
 */
typedef struct LVScanDescData
{
	BlockNumber lv_cblock;					/* current scanning block number */
	BlockNumber lv_next_unskippable_block;	/* next block number we cannot skip */
	BlockNumber lv_nblocks;					/* the number blocks of relation */
	HeapScanDesc heapscan;					/* field for parallel lazy vacuum */
} LVScanDescData;
typedef struct LVScanDescData *LVScanDesc;

/* A few variables that don't seem worth passing around as parameters */
static int	elevel = -1;

static TransactionId OldestXmin;
static TransactionId FreezeLimit;
static MultiXactId MultiXactCutoff;

static BufferAccessStrategy vac_strategy;

/* nonf-export function prototypes */
static void lazy_vacuum_heap(Relation onerel, LVState *lvstate);
static bool lazy_check_needs_freeze(Buffer buf, bool *hastup);
static void lazy_vacuum_index(Relation indrel,
				  IndexBulkDeleteResult **stats,
							  LVState *lvstate);
static void lazy_cleanup_index(Relation indrel, IndexBulkDeleteResult *stats,
							   LVRelStats *vacrelstats, LVIndStats *indstat);
static int lazy_vacuum_page(Relation onerel, BlockNumber blkno, Buffer buffer,
				 int tupindex, LVState *lvstate, Buffer *vmbuffer);
static bool should_attempt_truncation(LVRelStats *vacrelstats);
static void lazy_truncate_heap(Relation onerel, LVRelStats *vacrelstats);
static BlockNumber count_nondeletable_pages(Relation onerel,
						 LVRelStats *vacrelstats);
static void lazy_space_alloc(LVState *lvstate, BlockNumber relblocks);
static void lazy_record_dead_tuple(LVState *state, ItemPointer itemptr);
static bool lazy_tid_reaped(ItemPointer itemptr, void *state);
static int	vac_cmp_itemptr(const void *left, const void *right);
static bool heap_page_is_all_visible(Relation rel, Buffer buf,
						 TransactionId *visibility_cutoff_xid, bool *all_frozen);
static void do_lazy_scan_heap(LVState *lvstate, Relation onerel, Relation *Irels,
							  int nindexes, int options, bool aggressive);
static void lazy_scan_heap(Relation rel, LVState *lvstate, VacuumOptions options,
						   bool aggressive);

/* function prototypes for parallel vacuum */
static void lazy_gather_vacuum_stats(ParallelContext *pxct,
									 LVRelStats *valrelstats);
static void lazy_estimate_dsm(ParallelContext *pcxt, LVRelStats *vacrelstats);
static void lazy_initialize_dsm(ParallelContext *pcxt, Relation onrel,
								LVState *lvstate, int options, bool aggressive);
static LVState *lazy_initialize_worker(shm_toc *toc);
static LVScanDesc lv_beginscan(Relation onerel, ParallelHeapScanDesc pscan);
static void lv_endscan(LVScanDesc lvscan);
static BlockNumber lazy_scan_get_nextpage(Relation onerel, LVState *lvstate,
											   LVScanDesc lvscan,
											   bool *all_visible_according_to_vm,
											   Buffer *vmbuffer, int options, bool aggressive);
static void lazy_prepare_vacuum(LVState *lvstate);
static void lazy_end_vacuum(LVState *lvstate);
static long lazy_get_max_dead_tuples(LVRelStats *vacrelstats);


/*
 *	lazy_vacuum_rel() -- perform LAZY VACUUM for one heap relation
 *
 *		This routine vacuums a single heap, cleans out its indexes, and
 *		updates its relpages and reltuples statistics.
 *
 *		At entry, we have already established a transaction and opened
 *		and locked the relation.
 */
void
lazy_vacuum_rel(Relation onerel, VacuumOptions options, VacuumParams *params,
				BufferAccessStrategy bstrategy)
{
	LVState		*lvstate;
	LVRelStats	*vacrelstats;
	PGRUsage	ru0;
	TimestampTz starttime = 0;
	long		secs;
	int			usecs;
	double		read_rate,
				write_rate;
	bool		aggressive;		/* should we scan all unfrozen pages? */
	bool		scanned_all_unfrozen;	/* actually scanned all such pages? */
	TransactionId xidFullScanLimit;
	MultiXactId mxactFullScanLimit;
	BlockNumber new_rel_pages;
	double		new_rel_tuples;
	BlockNumber new_rel_allvisible;
	double		new_live_tuples;
	TransactionId new_frozen_xid;
	MultiXactId new_min_multi;

	Assert(params != NULL);

	/* measure elapsed time iff autovacuum logging requires it */
	if (IsAutoVacuumWorkerProcess() && params->log_min_duration >= 0)
	{
		pg_rusage_init(&ru0);
		starttime = GetCurrentTimestamp();
	}

	if (options.flags & VACOPT_VERBOSE)
		elevel = INFO;
	else
		elevel = DEBUG2;

	pgstat_progress_start_command(PROGRESS_COMMAND_VACUUM,
								  RelationGetRelid(onerel));

	vac_strategy = bstrategy;

	vacuum_set_xid_limits(onerel,
						  params->freeze_min_age,
						  params->freeze_table_age,
						  params->multixact_freeze_min_age,
						  params->multixact_freeze_table_age,
						  &OldestXmin, &FreezeLimit, &xidFullScanLimit,
						  &MultiXactCutoff, &mxactFullScanLimit);

	/*
	 * We request an aggressive scan if the table's frozen Xid is now older
	 * than or equal to the requested Xid full-table scan limit; or if the
	 * table's minimum MultiXactId is older than or equal to the requested
	 * mxid full-table scan limit; or if DISABLE_PAGE_SKIPPING was specified.
	 */
	aggressive = TransactionIdPrecedesOrEquals(onerel->rd_rel->relfrozenxid,
											   xidFullScanLimit);
	aggressive |= MultiXactIdPrecedesOrEquals(onerel->rd_rel->relminmxid,
											  mxactFullScanLimit);
	if (options.flags & VACOPT_DISABLE_PAGE_SKIPPING)
		aggressive = true;

	lvstate = (LVState *) palloc0(sizeof(LVState));
	vacrelstats = (LVRelStats *) palloc0(sizeof(LVRelStats));
	lvstate->vacrelstats = vacrelstats;

	vacrelstats->old_rel_pages = onerel->rd_rel->relpages;
	vacrelstats->old_rel_tuples = onerel->rd_rel->reltuples;
	vacrelstats->num_index_scans = 0;
	vacrelstats->pages_removed = 0;
	vacrelstats->lock_waiter_detected = false;

	/* Do the vacuuming */
	lazy_scan_heap(onerel, lvstate, options, aggressive);

	/*
	 * Compute whether we actually scanned the all unfrozen pages. If we did,
	 * we can adjust relfrozenxid and relminmxid.
	 *
	 * NB: We need to check this before truncating the relation, because that
	 * will change ->rel_pages.
	 */
	if ((lvstate->vacrelstats->scanned_pages + vacrelstats->frozenskipped_pages)
		< vacrelstats->rel_pages)
	{
		Assert(!aggressive);
		scanned_all_unfrozen = false;
	}
	else
		scanned_all_unfrozen = true;

	/*
	 * Optionally truncate the relation.
	 */
	if (should_attempt_truncation(vacrelstats))
		lazy_truncate_heap(onerel, vacrelstats);

	/* Report that we are now doing final cleanup */
	pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
								 PROGRESS_VACUUM_PHASE_FINAL_CLEANUP);

	/* Vacuum the Free Space Map */
	FreeSpaceMapVacuum(onerel);

	/*
	 * Update statistics in pg_class.
	 *
	 * A corner case here is that if we scanned no pages at all because every
	 * page is all-visible, we should not update relpages/reltuples, because
	 * we have no new information to contribute.  In particular this keeps us
	 * from replacing relpages=reltuples=0 (which means "unknown tuple
	 * density") with nonzero relpages and reltuples=0 (which means "zero
	 * tuple density") unless there's some actual evidence for the latter.
	 *
	 * It's important that we use tupcount_pages and not scanned_pages for the
	 * check described above; scanned_pages counts pages where we could not
	 * get cleanup lock, and which were processed only for frozenxid purposes.
	 *
	 * We do update relallvisible even in the corner case, since if the table
	 * is all-visible we'd definitely like to know that.  But clamp the value
	 * to be not more than what we're setting relpages to.
	 *
	 * Also, don't change relfrozenxid/relminmxid if we skipped any pages,
	 * since then we don't know for certain that all tuples have a newer xmin.
	 */
	new_rel_pages = vacrelstats->rel_pages;
	new_rel_tuples = vacrelstats->new_rel_tuples;
	if (vacrelstats->tupcount_pages == 0 && new_rel_pages > 0)
	{
		new_rel_pages = vacrelstats->old_rel_pages;
		new_rel_tuples = vacrelstats->old_rel_tuples;
	}

	visibilitymap_count(onerel, &new_rel_allvisible, NULL);
	if (new_rel_allvisible > new_rel_pages)
		new_rel_allvisible = new_rel_pages;

	new_frozen_xid = scanned_all_unfrozen ? FreezeLimit : InvalidTransactionId;
	new_min_multi = scanned_all_unfrozen ? MultiXactCutoff : InvalidMultiXactId;

	vac_update_relstats(onerel,
						new_rel_pages,
						new_rel_tuples,
						new_rel_allvisible,
						(vacrelstats->nindexes != 0),
						new_frozen_xid,
						new_min_multi,
						false);

	/* report results to the stats collector, too */
	new_live_tuples = new_rel_tuples - vacrelstats->new_dead_tuples;
	if (new_live_tuples < 0)
		new_live_tuples = 0;	/* just in case */

	pgstat_report_vacuum(RelationGetRelid(onerel),
						 onerel->rd_rel->relisshared,
						 new_live_tuples,
						 vacrelstats->new_dead_tuples);
	pgstat_progress_end_command();

	/* and log the action if appropriate */
	if (IsAutoVacuumWorkerProcess() && params->log_min_duration >= 0)
	{
		TimestampTz endtime = GetCurrentTimestamp();

		if (params->log_min_duration == 0 ||
			TimestampDifferenceExceeds(starttime, endtime,
									   params->log_min_duration))
		{
			StringInfoData buf;

			TimestampDifference(starttime, endtime, &secs, &usecs);

			read_rate = 0;
			write_rate = 0;
			if ((secs > 0) || (usecs > 0))
			{
				read_rate = (double) BLCKSZ * VacuumPageMiss / (1024 * 1024) /
					(secs + usecs / 1000000.0);
				write_rate = (double) BLCKSZ * VacuumPageDirty / (1024 * 1024) /
					(secs + usecs / 1000000.0);
			}

			/*
			 * This is pretty messy, but we split it up so that we can skip
			 * emitting individual parts of the message when not applicable.
			 */
			initStringInfo(&buf);
			appendStringInfo(&buf, _("automatic vacuum of table \"%s.%s.%s\": index scans: %d\n"),
							 get_database_name(MyDatabaseId),
							 get_namespace_name(RelationGetNamespace(onerel)),
							 RelationGetRelationName(onerel),
							 vacrelstats->num_index_scans);
			appendStringInfo(&buf, _("pages: %u removed, %u remain, %u skipped due to pins, %u skipped frozen\n"),
							 vacrelstats->pages_removed,
							 vacrelstats->rel_pages,
							 vacrelstats->pinskipped_pages,
							 vacrelstats->frozenskipped_pages);
			appendStringInfo(&buf,
							 _("tuples: %.0f removed, %.0f remain, %.0f are dead but not yet removable, oldest xmin: %u\n"),
							 vacrelstats->tuples_deleted,
							 vacrelstats->new_rel_tuples,
							 vacrelstats->new_dead_tuples,
							 OldestXmin);
			appendStringInfo(&buf,
							 _("buffer usage: %d hits, %d misses, %d dirtied\n"),
							 VacuumPageHit,
							 VacuumPageMiss,
							 VacuumPageDirty);
			appendStringInfo(&buf, _("avg read rate: %.3f MB/s, avg write rate: %.3f MB/s\n"),
							 read_rate, write_rate);
			appendStringInfo(&buf, _("system usage: %s"), pg_rusage_show(&ru0));

			ereport(LOG,
					(errmsg_internal("%s", buf.data)));
			pfree(buf.data);
		}
	}
}

/*
 * For Hot Standby we need to know the highest transaction id that will
 * be removed by any change. VACUUM proceeds in a number of passes so
 * we need to consider how each pass operates. The first phase runs
 * heap_page_prune(), which can issue XLOG_HEAP2_CLEAN records as it
 * progresses - these will have a latestRemovedXid on each record.
 * In some cases this removes all of the tuples to be removed, though
 * often we have dead tuples with index pointers so we must remember them
 * for removal in phase 3. Index records for those rows are removed
 * in phase 2 and index blocks do not have MVCC information attached.
 * So before we can allow removal of any index tuples we need to issue
 * a WAL record containing the latestRemovedXid of rows that will be
 * removed in phase three. This allows recovery queries to block at the
 * correct place, i.e. before phase two, rather than during phase three
 * which would be after the rows have become inaccessible.
 */
static void
vacuum_log_cleanup_info(Relation rel, LVRelStats *vacrelstats)
{
	/*
	 * Skip this for relations for which no WAL is to be written, or if we're
	 * not trying to support archive recovery.
	 */
	if (!RelationNeedsWAL(rel) || !XLogIsNeeded())
		return;

	/*
	 * No need to write the record at all unless it contains a valid value
	 */
	if (TransactionIdIsValid(vacrelstats->latestRemovedXid))
		(void) log_heap_cleanup_info(rel->rd_node, vacrelstats->latestRemovedXid);
}

/*
 * If the number of workers is specified more than 0, we enter the parallel lazy
 * vacuum mode. In parallel lazy vacuum mode, we initialize a dynamic shared memory
 * and launch parallel vacuum workers. The launcher process also vacuums the table
 * after launched and then waits for the all vacuum workers to finish. After all vacuum
 * workers finished we gather the vacuum statistics of table and indexes, and update
 * them.
 */
static void
lazy_scan_heap(Relation onerel, LVState *lvstate, VacuumOptions options,
			   bool aggressive)
{
	ParallelContext	*pcxt;
	LVRelStats	*vacrelstats = lvstate->vacrelstats;
	Relation	*Irel;
	int			nindexes;

	lvstate->parallel_mode = options.nworkers > 0;

	/* Open indexes */
	vac_open_indexes(onerel, RowExclusiveLock, &nindexes, &Irel);
	vacrelstats->nindexes = nindexes;

	if (lvstate->parallel_mode)
	{
		EnterParallelMode();

		/* Create parallel context and initialize it */
		pcxt = CreateParallelContext("postgres", "LazyVacuumWorkerMain",
									 options.nworkers);
		lvstate->pcxt = pcxt;

		/* Estimate DSM size for parallel vacuum */
		lazy_estimate_dsm(pcxt, lvstate->vacrelstats);

		/* Initialize DSM for parallel vacuum */
		InitializeParallelDSM(pcxt);
		lazy_initialize_dsm(pcxt, onerel, lvstate, options.flags, aggressive);

		/* Launch workers */
		LaunchParallelWorkers(pcxt);
	}

	do_lazy_scan_heap(lvstate, onerel, Irel, nindexes, options.flags, aggressive);

	/*
	 * We can update relation statistics such as scanned page after gathered
	 * statistics from all workers. Also, in parallel mode since we cannot update
	 * index statistics at the same time the leader process have to do it.
	 *
	 * XXX : If we allows workers to update statistics tuples at the same time
	 * the updating index statistics can be done in lazy_cleanup_index().
	 */
	if (lvstate->parallel_mode)
	{
		int i;
		LVIndStats *indstats = palloc(sizeof(LVIndStats) * lvstate->vacrelstats->nindexes);

		/* Wait for workers finished vacuum */
		WaitForParallelWorkersToFinish(pcxt);

		/* Gather the result of vacuum statistics from all workers */
		lazy_gather_vacuum_stats(pcxt, vacrelstats);

		/* Now we can compute the new value for pg_class.reltuples */
		vacrelstats->new_rel_tuples = vac_estimate_reltuples(onerel, false,
															 vacrelstats->rel_pages,
															 vacrelstats->scanned_pages,
															 vacrelstats->scanned_tuples);

		/* Copy new index stats to local memory */
		memcpy(indstats, lvstate->indstats, sizeof(LVIndStats) * vacrelstats->nindexes);

		DestroyParallelContext(pcxt);
		ExitParallelMode();

		/* After exit parallel mode, update index statistics */
		for (i = 0; i < vacrelstats->nindexes; i++)
		{
			Relation	ind = Irel[i];
			LVIndStats *indstat = (LVIndStats *) &(indstats[i]);

			if (indstat->updated)
			   vac_update_relstats(ind,
								   indstat->num_pages,
								   indstat->num_tuples,
								   0,
								   false,
								   InvalidTransactionId,
								   InvalidMultiXactId,
								   false);
		}
	}

	vac_close_indexes(nindexes, Irel, RowExclusiveLock);
}

/*
 * Entry point of parallel vacuum worker.
 */
void
LazyVacuumWorkerMain(dsm_segment *seg, shm_toc *toc)
{
	LVState		*lvstate;
	Relation rel;
	Relation *indrel;
	int nindexes_worker;

	/* Look up dynamic shared memory and initialize */
	lvstate = lazy_initialize_worker(toc);

	Assert(lvstate != NULL);

	rel = relation_open(lvstate->pscan->phs_relid, ShareUpdateExclusiveLock);

	/* Open all indexes */
	vac_open_indexes(rel, RowExclusiveLock, &nindexes_worker,
					 &indrel);

	/* Do lazy vacuum */
	do_lazy_scan_heap(lvstate, rel, indrel, lvstate->vacrelstats->nindexes,
					  lvstate->pstate->info.options, lvstate->pstate->info.aggressive);

	vac_close_indexes(lvstate->vacrelstats->nindexes, indrel, RowExclusiveLock);
	heap_close(rel, ShareUpdateExclusiveLock);
}

/*
 *	do_lazy_scan_heap() -- scan an open heap relation
 *
 *		This routine prunes each page in the heap, which will among other
 *		things truncate dead tuples to dead line pointers, defragment the
 *		page, and set commit status bits (see heap_page_prune).  It also uses
 *		lists of dead tuples and pages with free space, calculates statistics
 *		on the number of live tuples in the heap, and marks pages as
 *		all-visible if appropriate.  When done, or when we run low on space for
 *		dead-tuple TIDs, invoke vacuuming of assigned indexes and call lazy_vacuum_heap
 *		to reclaim dead line pointers. In parallel vacuum, we need to synchronize
 *		at where scanning heap finished and vacuuming heap finished. The vacuum
 *		worker reached to that point first need to wait for other vacuum workers
 *		reached to the same point.
 *
 *		In parallel lazy scan, we get next page number using parallel heap scan.
 *		Since the dead tuple TIDs are shared with all vacuum workers, we have to
 *		wait for all other workers to reach to the same points where before starting
 *		reclaiming dead tuple TIDs and before clearing dead tuple TIDs information
 *		in dynamic shared memory.
 *
 *		If there are no indexes then we can reclaim line pointers on the fly;
 *		dead line pointers need only be retained until all index pointers that
 *		reference them have been killed.
 */
static void
do_lazy_scan_heap(LVState *lvstate, Relation onerel, Relation *Irel,
				  int nindexes, int options, bool aggressive)
{
	LVRelStats *vacrelstats = lvstate->vacrelstats;
	BlockNumber blkno;
	BlockNumber nblocks;
	HeapTupleData tuple;
	LVScanDesc lvscan;
	char	   *relname;
	BlockNumber empty_pages,
				vacuumed_pages;
	double		num_tuples,
				tups_vacuumed,
				nkeep,
				nunused;
	IndexBulkDeleteResult **indstats;
	int			i;
	PGRUsage	ru0;
#ifdef PLV_TIME
	PGRUsage	ru_scan;
	PGRUsage	ru_vacuum;
#endif
	Buffer		vmbuffer = InvalidBuffer;
	xl_heap_freeze_tuple *frozen;
	StringInfoData buf;
	bool		all_visible_according_to_vm = false;

	const int	initprog_index[] = {
		PROGRESS_VACUUM_PHASE,
		PROGRESS_VACUUM_TOTAL_HEAP_BLKS,
		PROGRESS_VACUUM_MAX_DEAD_TUPLES
	};
	int64		initprog_val[3];

	pg_rusage_init(&ru0);

	relname = RelationGetRelationName(onerel);
	ereport(elevel,
			(errmsg("vacuuming \"%s.%s\"",
					get_namespace_name(RelationGetNamespace(onerel)),
					relname)));

	empty_pages = vacuumed_pages = 0;
	num_tuples = tups_vacuumed = nkeep = nunused = 0;

	indstats = (IndexBulkDeleteResult **)
		palloc0(nindexes * sizeof(IndexBulkDeleteResult *));

	nblocks = RelationGetNumberOfBlocks(onerel);
	vacrelstats->rel_pages = nblocks;
	vacrelstats->scanned_pages = 0;
	vacrelstats->tupcount_pages = 0;
	vacrelstats->nonempty_pages = 0;
	vacrelstats->latestRemovedXid = InvalidTransactionId;

	lazy_space_alloc(lvstate, nblocks);
	frozen = palloc(sizeof(xl_heap_freeze_tuple) * MaxHeapTuplesPerPage);

	/* Begin heap scan for vacuum */
	lvscan = lv_beginscan(onerel, lvstate->pscan);

	/* Report that we're scanning the heap, advertising total # of blocks */
	initprog_val[0] = PROGRESS_VACUUM_PHASE_SCAN_HEAP;
	initprog_val[1] = nblocks;
	initprog_val[2] = lvstate->dtctl->dt_max;
	pgstat_progress_update_multi_param(3, initprog_index, initprog_val);

#ifdef PLV_TIME
	pg_rusage_init(&ru_scan);
#endif
	while((blkno = lazy_scan_get_nextpage(onerel, lvstate, lvscan,
										  &all_visible_according_to_vm,
										  &vmbuffer, options, aggressive)) != InvalidBlockNumber)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber offnum,
					maxoff;
		bool		tupgone,
					hastup;
		int			prev_dead_count;
		int			nfrozen;
		Size		freespace;
		bool		all_visible;
		bool		all_frozen = true;	/* provided all_visible is also true */
		bool		has_dead_tuples;
		TransactionId visibility_cutoff_xid = InvalidTransactionId;
		int			dtmax;
		int			dtcount;

		pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_SCANNED, blkno);

		vacuum_delay_point();

		/*
		 * If we are close to overrunning the available space for dead-tuple
		 * TIDs, pause and do a cycle of vacuuming before we tackle this page.
		 */
		if (IsDeadTupleShared(lvstate))
			SpinLockAcquire(&lvstate->dtctl->mutex);

		dtmax = lvstate->dtctl->dt_max;
		dtcount = lvstate->dtctl->dt_count;

		if (IsDeadTupleShared(lvstate))
			SpinLockRelease(&lvstate->dtctl->mutex);

		if (((dtmax - dtcount) < MaxHeapTuplesPerPage) && dtcount > 0)
		{
			const int	hvp_index[] = {
				PROGRESS_VACUUM_PHASE,
				PROGRESS_VACUUM_NUM_INDEX_VACUUMS
			};
			int64		hvp_val[2];

#ifdef PLV_TIME
			elog(WARNING, "%d Scan %s", ParallelWorkerNumber, pg_rusage_show(&ru_scan));
#endif
			/*
			 * Here we're about to vacuum the table and indexes actually. Before
			 * entering vacuum state, we have to wait for other vacuum worker to
			 * reach here.
			 */
			lazy_prepare_vacuum(lvstate);
#ifdef PLV_TIME
			pg_rusage_init(&ru_vacuum);
#endif

			/*
			 * Before beginning index vacuuming, we release any pin we may
			 * hold on the visibility map page.  This isn't necessary for
			 * correctness, but we do it anyway to avoid holding the pin
			 * across a lengthy, unrelated operation.
			 */
			if (BufferIsValid(vmbuffer))
			{
				ReleaseBuffer(vmbuffer);
				vmbuffer = InvalidBuffer;
			}

			/* Log cleanup info before we touch indexes */
			vacuum_log_cleanup_info(onerel, vacrelstats);

			/* Report that we are now vacuuming indexes */
			pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
										 PROGRESS_VACUUM_PHASE_VACUUM_INDEX);

			/* Remove assigned index entries */
			for (i = 0; i < nindexes; i++)
			{
				if (IsAssignedIndex(i, lvstate->pstate))
					lazy_vacuum_index(Irel[i], &indstats[i], lvstate);
			}

			/*
			 * Report that we are now vacuuming the heap.  We also increase
			 * the number of index scans here; note that by using
			 * pgstat_progress_update_multi_param we can update both
			 * parameters atomically.
			 */
			hvp_val[0] = PROGRESS_VACUUM_PHASE_VACUUM_HEAP;
			hvp_val[1] = vacrelstats->num_index_scans + 1;
			pgstat_progress_update_multi_param(2, hvp_index, hvp_val);

			/* Remove tuples from heap */
			lazy_vacuum_heap(onerel, lvstate);

#ifdef PLV_TIME
			elog(WARNING, "%d VACUUM : %s", ParallelWorkerNumber, pg_rusage_show(&ru_vacuum));
#endif
			/*
			 * Here we've done vacuum on the heap and index and we are going
			 * to begin the next round scan on heap. Wait until all vacuum worker
			 * finished vacuum. After all vacuum workers finished, forget the
			 * now-vacuumed tuples, and press on, but be careful not to reset
			 * latestRemoveXid since we want that value to be valid.
			 */
			lazy_end_vacuum(lvstate);
#ifdef PLV_TIME
			pg_rusage_init(&ru_scan);
#endif

			/* Report that we are once again scanning the heap */
			pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
										 PROGRESS_VACUUM_PHASE_SCAN_HEAP);

			vacrelstats->num_index_scans++;
		}

		/*
		 * Pin the visibility map page in case we need to mark the page
		 * all-visible.  In most cases this will be very cheap, because we'll
		 * already have the correct page pinned anyway.  However, it's
		 * possible that (a) next_unskippable_block is covered by a different
		 * VM page than the current block or (b) we released our pin and did a
		 * cycle of index vacuuming.
		 *
		 */
		visibilitymap_pin(onerel, blkno, &vmbuffer);

		buf = ReadBufferExtended(onerel, MAIN_FORKNUM, blkno,
								 RBM_NORMAL, vac_strategy);

		/* We need buffer cleanup lock so that we can prune HOT chains. */
		if (!ConditionalLockBufferForCleanup(buf))
		{
			/*
			 * If we're not performing an aggressive scan to guard against XID
			 * wraparound, and we don't want to forcibly check the page, then
			 * it's OK to skip vacuuming pages we get a lock conflict on. They
			 * will be dealt with in some future vacuum.
			 */
			if (!aggressive && !FORCE_CHECK_PAGE(blkno))
			{
				ReleaseBuffer(buf);
				vacrelstats->pinskipped_pages++;
				continue;
			}

			/*
			 * Read the page with share lock to see if any xids on it need to
			 * be frozen.  If not we just skip the page, after updating our
			 * scan statistics.  If there are some, we wait for cleanup lock.
			 *
			 * We could defer the lock request further by remembering the page
			 * and coming back to it later, or we could even register
			 * ourselves for multiple buffers and then service whichever one
			 * is received first.  For now, this seems good enough.
			 *
			 * If we get here with aggressive false, then we're just forcibly
			 * checking the page, and so we don't want to insist on getting
			 * the lock; we only need to know if the page contains tuples, so
			 * that we can update nonempty_pages correctly.  It's convenient
			 * to use lazy_check_needs_freeze() for both situations, though.
			 */
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			if (!lazy_check_needs_freeze(buf, &hastup))
			{
				UnlockReleaseBuffer(buf);
				vacrelstats->scanned_pages++;
				vacrelstats->pinskipped_pages++;
				if (hastup)
					vacrelstats->nonempty_pages = blkno + 1;
				continue;
			}
			if (!aggressive)
			{
				/*
				 * Here, we must not advance scanned_pages; that would amount
				 * to claiming that the page contains no freezable tuples.
				 */
				UnlockReleaseBuffer(buf);
				vacrelstats->pinskipped_pages++;
				if (hastup)
					vacrelstats->nonempty_pages = blkno + 1;
				continue;
			}
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			LockBufferForCleanup(buf);
			/* drop through to normal processing */
		}

		vacrelstats->scanned_pages++;
		vacrelstats->tupcount_pages++;

		page = BufferGetPage(buf);

		if (PageIsNew(page))
		{
			/*
			 * An all-zeroes page could be left over if a backend extends the
			 * relation but crashes before initializing the page. Reclaim such
			 * pages for use.
			 *
			 * We have to be careful here because we could be looking at a
			 * page that someone has just added to the relation and not yet
			 * been able to initialize (see RelationGetBufferForTuple). To
			 * protect against that, release the buffer lock, grab the
			 * relation extension lock momentarily, and re-lock the buffer. If
			 * the page is still uninitialized by then, it must be left over
			 * from a crashed backend, and we can initialize it.
			 *
			 * We don't really need the relation lock when this is a new or
			 * temp relation, but it's probably not worth the code space to
			 * check that, since this surely isn't a critical path.
			 *
			 * Note: the comparable code in vacuum.c need not worry because
			 * it's got exclusive lock on the whole relation.
			 */
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			LockRelationForExtension(onerel, ExclusiveLock);
			UnlockRelationForExtension(onerel, ExclusiveLock);
			LockBufferForCleanup(buf);
			if (PageIsNew(page))
			{
				ereport(WARNING,
						(errmsg("relation \"%s\" page %u is uninitialized --- fixing",
								relname, blkno)));
				PageInit(page, BufferGetPageSize(buf), 0);
				empty_pages++;
			}
			freespace = PageGetHeapFreeSpace(page);
			MarkBufferDirty(buf);
			UnlockReleaseBuffer(buf);

			RecordPageWithFreeSpace(onerel, blkno, freespace);
			continue;
		}

		if (PageIsEmpty(page))
		{
			empty_pages++;
			freespace = PageGetHeapFreeSpace(page);

			/* empty pages are always all-visible and all-frozen */
			if (!PageIsAllVisible(page))
			{
				START_CRIT_SECTION();

				/* mark buffer dirty before writing a WAL record */
				MarkBufferDirty(buf);

				/*
				 * It's possible that another backend has extended the heap,
				 * initialized the page, and then failed to WAL-log the page
				 * due to an ERROR.  Since heap extension is not WAL-logged,
				 * recovery might try to replay our record setting the page
				 * all-visible and find that the page isn't initialized, which
				 * will cause a PANIC.  To prevent that, check whether the
				 * page has been previously WAL-logged, and if not, do that
				 * now.
				 */
				if (RelationNeedsWAL(onerel) &&
					PageGetLSN(page) == InvalidXLogRecPtr)
					log_newpage_buffer(buf, true);

				PageSetAllVisible(page);
				visibilitymap_set(onerel, blkno, buf, InvalidXLogRecPtr,
								  vmbuffer, InvalidTransactionId,
								  VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN);
				END_CRIT_SECTION();
			}

			UnlockReleaseBuffer(buf);
			RecordPageWithFreeSpace(onerel, blkno, freespace);
			continue;
		}

		/*
		 * Prune all HOT-update chains in this page.
		 *
		 * We count tuples removed by the pruning step as removed by VACUUM.
		 */
		tups_vacuumed += heap_page_prune(onerel, buf, OldestXmin, false,
										 &vacrelstats->latestRemovedXid);

		/*
		 * Now scan the page to collect vacuumable items and check for tuples
		 * requiring freezing.
		 */
		all_visible = true;
		has_dead_tuples = false;
		nfrozen = 0;
		hastup = false;
		prev_dead_count = lvstate->dtctl->dt_count;
		maxoff = PageGetMaxOffsetNumber(page);

		/*
		 * Note: If you change anything in the loop below, also look at
		 * heap_page_is_all_visible to see if that needs to be changed.
		 */
		for (offnum = FirstOffsetNumber;
			 offnum <= maxoff;
			 offnum = OffsetNumberNext(offnum))
		{
			ItemId		itemid;

			itemid = PageGetItemId(page, offnum);

			/* Unused items require no processing, but we count 'em */
			if (!ItemIdIsUsed(itemid))
			{
				nunused += 1;
				continue;
			}

			/* Redirect items mustn't be touched */
			if (ItemIdIsRedirected(itemid))
			{
				hastup = true;	/* this page won't be truncatable */
				continue;
			}

			ItemPointerSet(&(tuple.t_self), blkno, offnum);

			/*
			 * DEAD item pointers are to be vacuumed normally; but we don't
			 * count them in tups_vacuumed, else we'd be double-counting (at
			 * least in the common case where heap_page_prune() just freed up
			 * a non-HOT tuple).
			 */
			if (ItemIdIsDead(itemid))
			{
				lazy_record_dead_tuple(lvstate, &(tuple.t_self));
				all_visible = false;
				continue;
			}

			Assert(ItemIdIsNormal(itemid));

			tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
			tuple.t_len = ItemIdGetLength(itemid);
			tuple.t_tableOid = RelationGetRelid(onerel);

			tupgone = false;

			switch (HeapTupleSatisfiesVacuum(&tuple, OldestXmin, buf))
			{
				case HEAPTUPLE_DEAD:

					/*
					 * Ordinarily, DEAD tuples would have been removed by
					 * heap_page_prune(), but it's possible that the tuple
					 * state changed since heap_page_prune() looked.  In
					 * particular an INSERT_IN_PROGRESS tuple could have
					 * changed to DEAD if the inserter aborted.  So this
					 * cannot be considered an error condition.
					 *
					 * If the tuple is HOT-updated then it must only be
					 * removed by a prune operation; so we keep it just as if
					 * it were RECENTLY_DEAD.  Also, if it's a heap-only
					 * tuple, we choose to keep it, because it'll be a lot
					 * cheaper to get rid of it in the next pruning pass than
					 * to treat it like an indexed tuple.
					 */
					if (HeapTupleIsHotUpdated(&tuple) ||
						HeapTupleIsHeapOnly(&tuple))
						nkeep += 1;
					else
						tupgone = true; /* we can delete the tuple */
					all_visible = false;
					break;
				case HEAPTUPLE_LIVE:
					/* Tuple is good --- but let's do some validity checks */
					if (onerel->rd_rel->relhasoids &&
						!OidIsValid(HeapTupleGetOid(&tuple)))
						elog(WARNING, "relation \"%s\" TID %u/%u: OID is invalid",
							 relname, blkno, offnum);

					/*
					 * Is the tuple definitely visible to all transactions?
					 *
					 * NB: Like with per-tuple hint bits, we can't set the
					 * PD_ALL_VISIBLE flag if the inserter committed
					 * asynchronously. See SetHintBits for more info. Check
					 * that the tuple is hinted xmin-committed because of
					 * that.
					 */
					if (all_visible)
					{
						TransactionId xmin;

						if (!HeapTupleHeaderXminCommitted(tuple.t_data))
						{
							all_visible = false;
							break;
						}

						/*
						 * The inserter definitely committed. But is it old
						 * enough that everyone sees it as committed?
						 */
						xmin = HeapTupleHeaderGetXmin(tuple.t_data);
						if (!TransactionIdPrecedes(xmin, OldestXmin))
						{
							all_visible = false;
							break;
						}

						/* Track newest xmin on page. */
						if (TransactionIdFollows(xmin, visibility_cutoff_xid))
							visibility_cutoff_xid = xmin;
					}
					break;
				case HEAPTUPLE_RECENTLY_DEAD:

					/*
					 * If tuple is recently deleted then we must not remove it
					 * from relation.
					 */
					nkeep += 1;
					all_visible = false;
					break;
				case HEAPTUPLE_INSERT_IN_PROGRESS:
					/* This is an expected case during concurrent vacuum */
					all_visible = false;
					break;
				case HEAPTUPLE_DELETE_IN_PROGRESS:
					/* This is an expected case during concurrent vacuum */
					all_visible = false;
					break;
				default:
					elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
					break;
			}

			if (tupgone)
			{
				lazy_record_dead_tuple(lvstate, &(tuple.t_self));
				HeapTupleHeaderAdvanceLatestRemovedXid(tuple.t_data,
													   &vacrelstats->latestRemovedXid);
				tups_vacuumed += 1;
				has_dead_tuples = true;
			}
			else
			{
				bool		tuple_totally_frozen;

				num_tuples += 1;
				hastup = true;

				/*
				 * Each non-removable tuple must be checked to see if it needs
				 * freezing.  Note we already have exclusive buffer lock.
				 */
				if (heap_prepare_freeze_tuple(tuple.t_data, FreezeLimit,
											  MultiXactCutoff, &frozen[nfrozen],
											  &tuple_totally_frozen))
					frozen[nfrozen++].offset = offnum;

				if (!tuple_totally_frozen)
					all_frozen = false;
			}
		}						/* scan along page */

		/*
		 * If we froze any tuples, mark the buffer dirty, and write a WAL
		 * record recording the changes.  We must log the changes to be
		 * crash-safe against future truncation of CLOG.
		 */
		if (nfrozen > 0)
		{
			START_CRIT_SECTION();

			MarkBufferDirty(buf);

			/* execute collected freezes */
			for (i = 0; i < nfrozen; i++)
			{
				ItemId		itemid;
				HeapTupleHeader htup;

				itemid = PageGetItemId(page, frozen[i].offset);
				htup = (HeapTupleHeader) PageGetItem(page, itemid);

				heap_execute_freeze_tuple(htup, &frozen[i]);
			}

			/* Now WAL-log freezing if necessary */
			if (RelationNeedsWAL(onerel))
			{
				XLogRecPtr	recptr;

				recptr = log_heap_freeze(onerel, buf, FreezeLimit,
										 frozen, nfrozen);
				PageSetLSN(page, recptr);
			}

			END_CRIT_SECTION();
		}

		/*
		 * If there are no indexes then we can vacuum the page right now
		 * instead of doing a second scan. Because each parallel worker uses its
		 * own dead tuple area they can vacuum independently.
		 */
		if (Irel == NULL && lvstate->dtctl->dt_count > 0)
		{
			/* Remove tuples from heap */
			lazy_vacuum_page(onerel, blkno, buf, 0, lvstate, &vmbuffer);
			has_dead_tuples = false;

			/*
			 * Forget the now-vacuumed tuples, and press on, but be careful
			 * not to reset latestRemovedXid since we want that value to be
			 * valid.
			 */
			lvstate->dtctl->dt_count = 0;

			vacuumed_pages++;
		}

		freespace = PageGetHeapFreeSpace(page);

		/* mark page all-visible, if appropriate */
		if (all_visible && !all_visible_according_to_vm)
		{
			uint8		flags = VISIBILITYMAP_ALL_VISIBLE;

			if (all_frozen)
				flags |= VISIBILITYMAP_ALL_FROZEN;

			/*
			 * It should never be the case that the visibility map page is set
			 * while the page-level bit is clear, but the reverse is allowed
			 * (if checksums are not enabled).  Regardless, set the both bits
			 * so that we get back in sync.
			 *
			 * NB: If the heap page is all-visible but the VM bit is not set,
			 * we don't need to dirty the heap page.  However, if checksums
			 * are enabled, we do need to make sure that the heap page is
			 * dirtied before passing it to visibilitymap_set(), because it
			 * may be logged.  Given that this situation should only happen in
			 * rare cases after a crash, it is not worth optimizing.
			 */
			PageSetAllVisible(page);
			MarkBufferDirty(buf);
			visibilitymap_set(onerel, blkno, buf, InvalidXLogRecPtr,
							  vmbuffer, visibility_cutoff_xid, flags);
		}

		/*
		 * As of PostgreSQL 9.2, the visibility map bit should never be set if
		 * the page-level bit is clear.  However, it's possible that the bit
		 * got cleared after we checked it and before we took the buffer
		 * content lock, so we must recheck before jumping to the conclusion
		 * that something bad has happened.
		 */
		else if (all_visible_according_to_vm && !PageIsAllVisible(page)
				 && VM_ALL_VISIBLE(onerel, blkno, &vmbuffer))
		{
			elog(WARNING, "page is not marked all-visible but visibility map bit is set in relation \"%s\" page %u",
				 relname, blkno);
			visibilitymap_clear(onerel, blkno, vmbuffer,
								VISIBILITYMAP_VALID_BITS);
		}

		/*
		 * It's possible for the value returned by GetOldestXmin() to move
		 * backwards, so it's not wrong for us to see tuples that appear to
		 * not be visible to everyone yet, while PD_ALL_VISIBLE is already
		 * set. The real safe xmin value never moves backwards, but
		 * GetOldestXmin() is conservative and sometimes returns a value
		 * that's unnecessarily small, so if we see that contradiction it just
		 * means that the tuples that we think are not visible to everyone yet
		 * actually are, and the PD_ALL_VISIBLE flag is correct.
		 *
		 * There should never be dead tuples on a page with PD_ALL_VISIBLE
		 * set, however.
		 */
		else if (PageIsAllVisible(page) && has_dead_tuples)
		{
			elog(WARNING, "page containing dead tuples is marked as all-visible in relation \"%s\" page %u",
				 relname, blkno);
			PageClearAllVisible(page);
			MarkBufferDirty(buf);
			visibilitymap_clear(onerel, blkno, vmbuffer,
								VISIBILITYMAP_VALID_BITS);
		}

		/*
		 * If the all-visible page is turned out to be all-frozen but not
		 * marked, we should so mark it.  Note that all_frozen is only valid
		 * if all_visible is true, so we must check both.
		 */
		else if (all_visible_according_to_vm && all_visible && all_frozen &&
				 !VM_ALL_FROZEN(onerel, blkno, &vmbuffer))
		{
			/*
			 * We can pass InvalidTransactionId as the cutoff XID here,
			 * because setting the all-frozen bit doesn't cause recovery
			 * conflicts.
			 */
			visibilitymap_set(onerel, blkno, buf, InvalidXLogRecPtr,
							  vmbuffer, InvalidTransactionId,
							  VISIBILITYMAP_ALL_FROZEN);
		}

		UnlockReleaseBuffer(buf);

		/* Remember the location of the last page with nonremovable tuples */
		if (hastup)
			vacrelstats->nonempty_pages = blkno + 1;

		/*
		 * If we remembered any tuples for deletion, then the page will be
		 * visited again by lazy_vacuum_heap, which will compute and record
		 * its post-compaction free space.  If not, then we're done with this
		 * page, so remember its free space as-is.  (This path will always be
		 * taken if there are no indexes.)
		 */
		if (lvstate->dtctl->dt_count == prev_dead_count)
			RecordPageWithFreeSpace(onerel, blkno, freespace);
	}

	/* report that everything is scanned and vacuumed */
	pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_SCANNED, blkno);

	pfree(frozen);

	/* save stats for use later */
	vacrelstats->scanned_tuples = num_tuples;
	vacrelstats->tuples_deleted = tups_vacuumed;
	vacrelstats->new_dead_tuples = nkeep;

	/* now we can compute the new value for pg_class.reltuples */
	if (!lvstate->parallel_mode)
		vacrelstats->new_rel_tuples = vac_estimate_reltuples(onerel, false,
															 nblocks,
															 vacrelstats->tupcount_pages,
															 num_tuples);

	/*
	 * Release any remaining pin on visibility map page.
	 */
	if (BufferIsValid(vmbuffer))
	{
		ReleaseBuffer(vmbuffer);
		vmbuffer = InvalidBuffer;
	}

	/* If any tuples need to be deleted, perform final vacuum cycle */
	/* XXX put a threshold on min number of tuples here? */
	if (lvstate->dtctl->dt_count > 0)
	{
		const int	hvp_index[] = {
			PROGRESS_VACUUM_PHASE,
			PROGRESS_VACUUM_NUM_INDEX_VACUUMS
		};
		int64		hvp_val[2];
#ifdef PLV_TIME
		elog(WARNING, "%d Scan %s", ParallelWorkerNumber, pg_rusage_show(&ru_scan));
#endif
		/*
		 * Here we're about to vacuum the table and indexes actually. Before
		 * entering vacuum state, we have to wait for other vacuum worker to
		 * reach here.
		 */
		lazy_prepare_vacuum(lvstate);
#ifdef PLV_TIME
		pg_rusage_init(&ru_vacuum);
#endif

		/* Log cleanup info before we touch indexes */
		vacuum_log_cleanup_info(onerel, vacrelstats);

		/* Report that we are now vacuuming indexes */
		pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
									 PROGRESS_VACUUM_PHASE_VACUUM_INDEX);

		/* Remove index entries */
		for (i = 0; i < nindexes; i++)
		{
			if (IsAssignedIndex(i, lvstate->pstate))
				lazy_vacuum_index(Irel[i], &indstats[i], lvstate);
		}

		/* Report that we are now vacuuming the heap */
		hvp_val[0] = PROGRESS_VACUUM_PHASE_VACUUM_HEAP;
		hvp_val[1] = vacrelstats->num_index_scans + 1;
		pgstat_progress_update_multi_param(2, hvp_index, hvp_val);

		/* Remove tuples from heap */
		pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
									 PROGRESS_VACUUM_PHASE_VACUUM_HEAP);

		lazy_vacuum_heap(onerel, lvstate);

		vacrelstats->num_index_scans++;
#ifdef PLV_TIME
		elog(WARNING, "%d VACUUM : %s", ParallelWorkerNumber, pg_rusage_show(&ru_vacuum));
#endif
	}

	/* report all blocks vacuumed; and that we're cleaning up */
	pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_VACUUMED, blkno);
	pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
								 PROGRESS_VACUUM_PHASE_INDEX_CLEANUP);

	/* Do post-vacuum cleanup and statistics update for each index */
	for (i = 0; i < nindexes; i++)
	{
		if (IsAssignedIndex(i, lvstate->pstate))
			lazy_cleanup_index(Irel[i], indstats[i], lvstate->vacrelstats,
							   lvstate->parallel_mode ? &(lvstate->indstats[i]) : NULL);
	}

	/* If no indexes, make log report that lazy_vacuum_heap would've made */
	if (vacuumed_pages)
		ereport(elevel,
				(errmsg("\"%s\": removed %.0f row versions in %u pages",
						RelationGetRelationName(onerel),
						tups_vacuumed, vacuumed_pages)));

	lv_endscan(lvscan);

	/*
	 * This is pretty messy, but we split it up so that we can skip emitting
	 * individual parts of the message when not applicable.
	 */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "------- worker %d TOTAL stats -------\n", ParallelWorkerNumber);
	appendStringInfo(&buf,
					 _("%.0f dead row versions cannot be removed yet, oldest xmin: %u\n"),
					 nkeep, OldestXmin);
	appendStringInfo(&buf, _("There were %.0f unused item pointers.\n"),
					 nunused);
	appendStringInfo(&buf, ngettext("Skipped %u page due to buffer pins, ",
									"Skipped %u pages due to buffer pins, ",
									vacrelstats->pinskipped_pages),
					 vacrelstats->pinskipped_pages);
	appendStringInfo(&buf, ngettext("%u frozen page.\n",
									"%u frozen pages.\n",
									vacrelstats->frozenskipped_pages),
					 vacrelstats->frozenskipped_pages);
	appendStringInfo(&buf, ngettext("%u page is entirely empty.\n",
									"%u pages are entirely empty.\n",
									empty_pages),
					 empty_pages);
	appendStringInfo(&buf, _("%s."), pg_rusage_show(&ru0));

	ereport(elevel,
			(errmsg("\"%s\": found %.0f removable, %.0f nonremovable row versions in %u out of %u pages",
					RelationGetRelationName(onerel),
					tups_vacuumed, num_tuples,
					vacrelstats->scanned_pages, nblocks),
			 errdetail_internal("%s", buf.data)));
	pfree(buf.data);
}

/*
 * gather_vacuum_stats() -- Gather vacuum statistics from workers
 */
static void
lazy_gather_vacuum_stats(ParallelContext *pcxt, LVRelStats *vacrelstats)
{
	int	i;
	LVRelStats *lvstats_list;

	lvstats_list = (LVRelStats *) shm_toc_lookup(pcxt->toc, VACUUM_KEY_VACUUM_STATS, false);

	/* Gather each worker stats */
	for (i = 0; i < pcxt->nworkers_launched; i++)
	{
		LVRelStats *wstats = lvstats_list + sizeof(LVRelStats) * i;

		vacrelstats->scanned_pages += wstats->scanned_pages;
		vacrelstats->pinskipped_pages += wstats->pinskipped_pages;
		vacrelstats->frozenskipped_pages += wstats->frozenskipped_pages;
		vacrelstats->scanned_tuples += wstats->scanned_tuples;
		vacrelstats->new_dead_tuples += wstats->new_dead_tuples;
		vacrelstats->pages_removed += wstats->pages_removed;
		vacrelstats->tuples_deleted += wstats->tuples_deleted;
		vacrelstats->nonempty_pages += wstats->nonempty_pages;
	}

	/* all vacuum workers have same value of rel_pages */
	vacrelstats->rel_pages = lvstats_list->rel_pages;
}

/*
 *	lazy_vacuum_heap() -- second pass over the heap
 *
 *		This routine marks dead tuples as unused and compacts out free
 *		space on their pages.  Pages not having dead tuples recorded from
 *		lazy_scan_heap are not visited at all.
 *
 * Note: the reason for doing this as a second pass is we cannot remove
 * the tuples until we've removed their index entries, and we want to
 * process index entry removal in batches as large as possible.
 */
static void
lazy_vacuum_heap(Relation onerel, LVState *lvstate)
{
	int			tupindex;
	int			npages;
	PGRUsage	ru0;
	BlockNumber	prev_tblk;
	Buffer		vmbuffer = InvalidBuffer;
	ItemPointer	deadtuples = lvstate->deadtuples;
	LVDeadTupleCtl *dtctl = lvstate->dtctl;
	BlockNumber	ntuples = 0;
	StringInfoData	buf;

	pg_rusage_init(&ru0);
	npages = 0;

	tupindex = 0;

	while (tupindex < dtctl->dt_count)
	{
		BlockNumber tblk;
		Buffer		buf;
		Page		page;
		Size		freespace;

		vacuum_delay_point();

		/*
		 * If the dead tuple TIDs are shared with all vacuum workers,
		 * we acquire the lock and advance tupindex before vacuuming.
		 *
		 * NB: The number of maximum tuple can be stored into single
		 * page is not a large number in most cases. We can use spinlock
		 * here.
		 */
		if (IsDeadTupleShared(lvstate))
		{
			SpinLockAcquire(&(dtctl->mutex));

			tupindex = dtctl->dt_index;

			if (tupindex >= dtctl->dt_count)
			{
				SpinLockRelease(&(dtctl->mutex));
				break;
			}

			/* Advance dtct->dt_index */
			prev_tblk = tblk = ItemPointerGetBlockNumber(&deadtuples[tupindex]);
			while(prev_tblk == tblk &&
				  dtctl->dt_index < dtctl->dt_count)
			{
				tblk = ItemPointerGetBlockNumber(&deadtuples[dtctl->dt_index]);
				dtctl->dt_index++;
				ntuples++;
			}

			SpinLockRelease(&(dtctl->mutex));
		}

		tblk = ItemPointerGetBlockNumber(&deadtuples[tupindex]);
		buf = ReadBufferExtended(onerel, MAIN_FORKNUM, tblk, RBM_NORMAL,
								 vac_strategy);
		if (!ConditionalLockBufferForCleanup(buf))
		{
			ReleaseBuffer(buf);
			++tupindex;
			continue;
		}
		tupindex = lazy_vacuum_page(onerel, tblk, buf, tupindex, lvstate,
									&vmbuffer);

		/* Now that we've compacted the page, record its available space */
		page = BufferGetPage(buf);
		freespace = PageGetHeapFreeSpace(page);

		UnlockReleaseBuffer(buf);
		RecordPageWithFreeSpace(onerel, tblk, freespace);
		npages++;
	}

	if (BufferIsValid(vmbuffer))
	{
		ReleaseBuffer(vmbuffer);
		vmbuffer = InvalidBuffer;
	}

#ifdef PLV_TIME
	elog(WARNING, "%d TABLE %s", ParallelWorkerNumber, pg_rusage_show(&ru0));
#endif
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "------- worker %d VACUUM HEAP stats -------\n", ParallelWorkerNumber);
	appendStringInfo(&buf,
					 "\"%s\": removed %d row versions in %d pages",
					 RelationGetRelationName(onerel), ntuples, npages);
	ereport(elevel,
			(errmsg("%s", buf.data),
			 errdetail_internal("%s", pg_rusage_show(&ru0))));
}

/*
 *	lazy_vacuum_page() -- free dead tuples on a page
 *					 and repair its fragmentation.
 *
 * Caller must hold pin and buffer cleanup lock on the buffer.
 *
 */
static int
lazy_vacuum_page(Relation onerel, BlockNumber blkno, Buffer buffer,
				 int tupindex, LVState *lvstate, Buffer *vmbuffer)
{
	Page		page = BufferGetPage(buffer);
	OffsetNumber unused[MaxOffsetNumber];
	int			uncnt = 0;
	TransactionId visibility_cutoff_xid;
	bool		all_frozen;
	LVRelStats	*vacrelstats = lvstate->vacrelstats;

	pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_VACUUMED, blkno);

	START_CRIT_SECTION();

	for (; tupindex < lvstate->dtctl->dt_count; tupindex++)
	{
		BlockNumber tblk;
		OffsetNumber toff;
		ItemId		itemid;

		tblk = ItemPointerGetBlockNumber(&lvstate->deadtuples[tupindex]);
		if (tblk != blkno)
			break;				/* past end of tuples for this block */
		toff = ItemPointerGetOffsetNumber(&lvstate->deadtuples[tupindex]);
		itemid = PageGetItemId(page, toff);
		ItemIdSetUnused(itemid);
		unused[uncnt++] = toff;
	}

	PageRepairFragmentation(page);

	/*
	 * Mark buffer dirty before we write WAL.
	 */
	MarkBufferDirty(buffer);

	/* XLOG stuff */
	if (RelationNeedsWAL(onerel))
	{
		XLogRecPtr	recptr;

		recptr = log_heap_clean(onerel, buffer,
								NULL, 0, NULL, 0,
								unused, uncnt,
								vacrelstats->latestRemovedXid);
		PageSetLSN(page, recptr);
	}

	/*
	 * End critical section, so we safely can do visibility tests (which
	 * possibly need to perform IO and allocate memory!). If we crash now the
	 * page (including the corresponding vm bit) might not be marked all
	 * visible, but that's fine. A later vacuum will fix that.
	 */
	END_CRIT_SECTION();

	/*
	 * Now that we have removed the dead tuples from the page, once again
	 * check if the page has become all-visible.  The page is already marked
	 * dirty, exclusively locked, and, if needed, a full page image has been
	 * emitted in the log_heap_clean() above.
	 */
	if (heap_page_is_all_visible(onerel, buffer, &visibility_cutoff_xid,
								 &all_frozen))
		PageSetAllVisible(page);

	/*
	 * All the changes to the heap page have been done. If the all-visible
	 * flag is now set, also set the VM all-visible bit (and, if possible, the
	 * all-frozen bit) unless this has already been done previously.
	 */
	if (PageIsAllVisible(page))
	{
		uint8		vm_status = visibilitymap_get_status(onerel, blkno, vmbuffer);
		uint8		flags = 0;

		/* Set the VM all-frozen bit to flag, if needed */
		if ((vm_status & VISIBILITYMAP_ALL_VISIBLE) == 0)
			flags |= VISIBILITYMAP_ALL_VISIBLE;
		if ((vm_status & VISIBILITYMAP_ALL_FROZEN) == 0 && all_frozen)
			flags |= VISIBILITYMAP_ALL_FROZEN;

		Assert(BufferIsValid(*vmbuffer));
		if (flags != 0)
			visibilitymap_set(onerel, blkno, buffer, InvalidXLogRecPtr,
							  *vmbuffer, visibility_cutoff_xid, flags);
	}

	return tupindex;
}

/*
 *	lazy_check_needs_freeze() -- scan page to see if any tuples
 *					 need to be cleaned to avoid wraparound
 *
 * Returns true if the page needs to be vacuumed using cleanup lock.
 * Also returns a flag indicating whether page contains any tuples at all.
 */
static bool
lazy_check_needs_freeze(Buffer buf, bool *hastup)
{
	Page		page = BufferGetPage(buf);
	OffsetNumber offnum,
				maxoff;
	HeapTupleHeader tupleheader;

	*hastup = false;

	/* If we hit an uninitialized page, we want to force vacuuming it. */
	if (PageIsNew(page))
		return true;

	/* Quick out for ordinary empty page. */
	if (PageIsEmpty(page))
		return false;

	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid;

		itemid = PageGetItemId(page, offnum);

		/* this should match hastup test in count_nondeletable_pages() */
		if (ItemIdIsUsed(itemid))
			*hastup = true;

		/* dead and redirect items never need freezing */
		if (!ItemIdIsNormal(itemid))
			continue;

		tupleheader = (HeapTupleHeader) PageGetItem(page, itemid);

		if (heap_tuple_needs_freeze(tupleheader, FreezeLimit,
									MultiXactCutoff, buf))
			return true;
	}							/* scan along page */

	return false;
}


/*
 *	lazy_vacuum_index() -- vacuum one index relation.
 *
 *		Delete all the index entries pointing to tuples listed in
 *		lvstate->deadtuples, and update running statistics.
 */
static void
lazy_vacuum_index(Relation indrel,
				  IndexBulkDeleteResult **stats,
				  LVState	*lvstate)
{
	IndexVacuumInfo ivinfo;
	StringInfoData buf;
	PGRUsage	ru0;

	pg_rusage_init(&ru0);

	ivinfo.index = indrel;
	ivinfo.analyze_only = false;
	ivinfo.estimated_count = true;
	ivinfo.message_level = elevel;
	ivinfo.num_heap_tuples = lvstate->vacrelstats->old_rel_tuples;
	ivinfo.strategy = vac_strategy;

	/* Do bulk deletion */
	*stats = index_bulk_delete(&ivinfo, *stats, lazy_tid_reaped, (void *) lvstate);

#ifdef PLV_TIME
	elog(WARNING, "%d INDEX(%d) %s", ParallelWorkerNumber, RelationGetRelid(indrel),
		 pg_rusage_show(&ru0));
#endif
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "------- worker %d VACUUM INDEX stats -------\n", ParallelWorkerNumber);
	appendStringInfo(&buf,
					 "scanned index \"%s\" to remove %d row versions",
					 RelationGetRelationName(indrel), lvstate->dtctl->dt_count);

	ereport(elevel,
			(errmsg("%s", buf.data),
			 errdetail_internal("%s", pg_rusage_show(&ru0))));
}

/*
 *	lazy_cleanup_index() -- do post-vacuum cleanup for one index relation.
 */
static void
lazy_cleanup_index(Relation indrel, IndexBulkDeleteResult *stats,
							   LVRelStats *vacrelstats, LVIndStats *indstat)
{
	IndexVacuumInfo ivinfo;
	StringInfoData	buf;
	PGRUsage	ru0;

	pg_rusage_init(&ru0);

	ivinfo.index = indrel;
	ivinfo.analyze_only = false;
	ivinfo.estimated_count = (vacrelstats->tupcount_pages < vacrelstats->rel_pages);
	ivinfo.message_level = elevel;
	ivinfo.num_heap_tuples = vacrelstats->new_rel_tuples;
	ivinfo.strategy = vac_strategy;

	stats = index_vacuum_cleanup(&ivinfo, stats);

	/* Will be updated by leader process after vacuumed */
	if (indstat)
		indstat->updated = false;

	if (!stats)
		return;

	/*
	 * Now update statistics in pg_class, but only if the index says the count
	 * is accurate. In parallel lazy vacuum, the worker can not update these
	 * information by itself, so save to DSM and then the launcher process
	 * updates it later.
	 */
	if (!stats->estimated_count)
	{
		if (indstat)
		{
			indstat->updated = true;
			indstat->num_pages = stats->num_pages;
			indstat->num_tuples = stats->num_index_tuples;
		}
		else
			vac_update_relstats(indrel,
								stats->num_pages,
								stats->num_index_tuples,
								0,
								false,
								InvalidTransactionId,
								InvalidMultiXactId,
								false);
	}

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "------- worker %d CLEANUP INDEX stats -------\n", ParallelWorkerNumber);
	appendStringInfo(&buf,
					 "index \"%s\" now contains %.0f row versions in %u pages",
					 RelationGetRelationName(indrel),
					 stats->num_index_tuples,
					 stats->num_pages);
	ereport(elevel,
			(errmsg("%s", buf.data),
					errdetail("%.0f index row versions were removed.\n"
							  "%u index pages have been deleted, %u are currently reusable.\n"
							  "%s.",
							  stats->tuples_removed,
							  stats->pages_deleted, stats->pages_free,
							  pg_rusage_show(&ru0))));

	pfree(stats);
}

/*
 * should_attempt_truncation - should we attempt to truncate the heap?
 *
 * Don't even think about it unless we have a shot at releasing a goodly
 * number of pages.  Otherwise, the time taken isn't worth it.
 *
 * Also don't attempt it if we are doing early pruning/vacuuming, because a
 * scan which cannot find a truncated heap page cannot determine that the
 * snapshot is too old to read that page.  We might be able to get away with
 * truncating all except one of the pages, setting its LSN to (at least) the
 * maximum of the truncated range if we also treated an index leaf tuple
 * pointing to a missing heap page as something to trigger the "snapshot too
 * old" error, but that seems fragile and seems like it deserves its own patch
 * if we consider it.
 *
 * This is split out so that we can test whether truncation is going to be
 * called for before we actually do it.  If you change the logic here, be
 * careful to depend only on fields that lazy_scan_heap updates on-the-fly.
 */
static bool
should_attempt_truncation(LVRelStats *vacrelstats)
{
	BlockNumber possibly_freeable;

	possibly_freeable = vacrelstats->rel_pages - vacrelstats->nonempty_pages;
	if (possibly_freeable > 0 &&
		(possibly_freeable >= REL_TRUNCATE_MINIMUM ||
		 possibly_freeable >= vacrelstats->rel_pages / REL_TRUNCATE_FRACTION) &&
		old_snapshot_threshold < 0)
		return true;
	else
		return false;
}

/*
 * lazy_truncate_heap - try to truncate off any empty pages at the end
 */
static void
lazy_truncate_heap(Relation onerel, LVRelStats *vacrelstats)
{
	BlockNumber old_rel_pages = vacrelstats->rel_pages;
	BlockNumber new_rel_pages;
	PGRUsage	ru0;
	int			lock_retry;

	pg_rusage_init(&ru0);

	/* Report that we are now truncating */
	pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
								 PROGRESS_VACUUM_PHASE_TRUNCATE);

	/*
	 * Loop until no more truncating can be done.
	 */
	do
	{
		/*
		 * We need full exclusive lock on the relation in order to do
		 * truncation. If we can't get it, give up rather than waiting --- we
		 * don't want to block other backends, and we don't want to deadlock
		 * (which is quite possible considering we already hold a lower-grade
		 * lock).
		 */
		vacrelstats->lock_waiter_detected = false;
		lock_retry = 0;
		while (true)
		{
			if (ConditionalLockRelation(onerel, AccessExclusiveLock))
				break;

			/*
			 * Check for interrupts while trying to (re-)acquire the exclusive
			 * lock.
			 */
			CHECK_FOR_INTERRUPTS();

			if (++lock_retry > (VACUUM_TRUNCATE_LOCK_TIMEOUT /
								VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL))
			{
				/*
				 * We failed to establish the lock in the specified number of
				 * retries. This means we give up truncating.
				 */
				vacrelstats->lock_waiter_detected = true;
				ereport(elevel,
						(errmsg("\"%s\": stopping truncate due to conflicting lock request",
								RelationGetRelationName(onerel))));
				return;
			}

			pg_usleep(VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL * 1000L);
		}

		/*
		 * Now that we have exclusive lock, look to see if the rel has grown
		 * whilst we were vacuuming with non-exclusive lock.  If so, give up;
		 * the newly added pages presumably contain non-deletable tuples.
		 */
		new_rel_pages = RelationGetNumberOfBlocks(onerel);
		if (new_rel_pages != old_rel_pages)
		{
			/*
			 * Note: we intentionally don't update vacrelstats->rel_pages with
			 * the new rel size here.  If we did, it would amount to assuming
			 * that the new pages are empty, which is unlikely. Leaving the
			 * numbers alone amounts to assuming that the new pages have the
			 * same tuple density as existing ones, which is less unlikely.
			 */
			UnlockRelation(onerel, AccessExclusiveLock);
			return;
		}

		/*
		 * Scan backwards from the end to verify that the end pages actually
		 * contain no tuples.  This is *necessary*, not optional, because
		 * other backends could have added tuples to these pages whilst we
		 * were vacuuming.
		 */
		new_rel_pages = count_nondeletable_pages(onerel, vacrelstats);

		if (new_rel_pages >= old_rel_pages)
		{
			/* can't do anything after all */
			UnlockRelation(onerel, AccessExclusiveLock);
			return;
		}

		/*
		 * Okay to truncate.
		 */
		RelationTruncate(onerel, new_rel_pages);

		/*
		 * We can release the exclusive lock as soon as we have truncated.
		 * Other backends can't safely access the relation until they have
		 * processed the smgr invalidation that smgrtruncate sent out ... but
		 * that should happen as part of standard invalidation processing once
		 * they acquire lock on the relation.
		 */
		UnlockRelation(onerel, AccessExclusiveLock);

		/*
		 * Update statistics.  Here, it *is* correct to adjust rel_pages
		 * without also touching reltuples, since the tuple count wasn't
		 * changed by the truncation.
		 */
		vacrelstats->pages_removed += old_rel_pages - new_rel_pages;
		vacrelstats->rel_pages = new_rel_pages;

		ereport(elevel,
				(errmsg("\"%s\": truncated %u to %u pages",
						RelationGetRelationName(onerel),
						old_rel_pages, new_rel_pages),
				 errdetail_internal("%s",
									pg_rusage_show(&ru0))));
		old_rel_pages = new_rel_pages;
	} while (new_rel_pages > vacrelstats->nonempty_pages &&
			 vacrelstats->lock_waiter_detected);
}

/*
 * Rescan end pages to verify that they are (still) empty of tuples.
 *
 * Returns number of nondeletable pages (last nonempty page + 1).
 */
static BlockNumber
count_nondeletable_pages(Relation onerel, LVRelStats *vacrelstats)
{
	BlockNumber blkno;
	BlockNumber prefetchedUntil;
	instr_time	starttime;

	/* Initialize the starttime if we check for conflicting lock requests */
	INSTR_TIME_SET_CURRENT(starttime);

	/*
	 * Start checking blocks at what we believe relation end to be and move
	 * backwards.  (Strange coding of loop control is needed because blkno is
	 * unsigned.)  To make the scan faster, we prefetch a few blocks at a time
	 * in forward direction, so that OS-level readahead can kick in.
	 */
	blkno = vacrelstats->rel_pages;
	StaticAssertStmt((PREFETCH_SIZE & (PREFETCH_SIZE - 1)) == 0,
					 "prefetch size must be power of 2");
	prefetchedUntil = InvalidBlockNumber;
	while (blkno > vacrelstats->nonempty_pages)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber offnum,
					maxoff;
		bool		hastup;

		/*
		 * Check if another process requests a lock on our relation. We are
		 * holding an AccessExclusiveLock here, so they will be waiting. We
		 * only do this once per VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL, and we
		 * only check if that interval has elapsed once every 32 blocks to
		 * keep the number of system calls and actual shared lock table
		 * lookups to a minimum.
		 */
		if ((blkno % 32) == 0)
		{
			instr_time	currenttime;
			instr_time	elapsed;

			INSTR_TIME_SET_CURRENT(currenttime);
			elapsed = currenttime;
			INSTR_TIME_SUBTRACT(elapsed, starttime);
			if ((INSTR_TIME_GET_MICROSEC(elapsed) / 1000)
				>= VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL)
			{
				if (LockHasWaitersRelation(onerel, AccessExclusiveLock))
				{
					ereport(elevel,
							(errmsg("\"%s\": suspending truncate due to conflicting lock request",
									RelationGetRelationName(onerel))));

					vacrelstats->lock_waiter_detected = true;
					return blkno;
				}
				starttime = currenttime;
			}
		}

		/*
		 * We don't insert a vacuum delay point here, because we have an
		 * exclusive lock on the table which we want to hold for as short a
		 * time as possible.  We still need to check for interrupts however.
		 */
		CHECK_FOR_INTERRUPTS();

		blkno--;

		/* If we haven't prefetched this lot yet, do so now. */
		if (prefetchedUntil > blkno)
		{
			BlockNumber prefetchStart;
			BlockNumber pblkno;

			prefetchStart = blkno & ~(PREFETCH_SIZE - 1);
			for (pblkno = prefetchStart; pblkno <= blkno; pblkno++)
			{
				PrefetchBuffer(onerel, MAIN_FORKNUM, pblkno);
				CHECK_FOR_INTERRUPTS();
			}
			prefetchedUntil = prefetchStart;
		}

		buf = ReadBufferExtended(onerel, MAIN_FORKNUM, blkno,
								 RBM_NORMAL, vac_strategy);

		/* In this phase we only need shared access to the buffer */
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buf);

		if (PageIsNew(page) || PageIsEmpty(page))
		{
			/* PageIsNew probably shouldn't happen... */
			UnlockReleaseBuffer(buf);
			continue;
		}

		hastup = false;
		maxoff = PageGetMaxOffsetNumber(page);
		for (offnum = FirstOffsetNumber;
			 offnum <= maxoff;
			 offnum = OffsetNumberNext(offnum))
		{
			ItemId		itemid;

			itemid = PageGetItemId(page, offnum);

			/*
			 * Note: any non-unused item should be taken as a reason to keep
			 * this page.  We formerly thought that DEAD tuples could be
			 * thrown away, but that's not so, because we'd not have cleaned
			 * out their index entries.
			 */
			if (ItemIdIsUsed(itemid))
			{
				hastup = true;
				break;			/* can stop scanning */
			}
		}						/* scan along page */

		UnlockReleaseBuffer(buf);

		/* Done scanning if we found a tuple here */
		if (hastup)
			return blkno + 1;
	}

	/*
	 * If we fall out of the loop, all the previously-thought-to-be-empty
	 * pages still are; we need not bother to look at the last known-nonempty
	 * page.
	 */
	return vacrelstats->nonempty_pages;
}

/*
 * lazy_space_alloc - space allocation decisions for lazy vacuum
 *
 * In parallel lazy vacuum the space for dead tuple locations are already
 * allocated in dynamic shared memory, so we allocate space for dead tuple
 * locations in local memory only when in not parallel lazy vacuum and set
 * MyDeadTuple.
 *
 * See the comments at the head of this file for rationale.
 */
static void
lazy_space_alloc(LVState *lvstate, BlockNumber relblocks)
{
	long maxtuples;

	/*
	 * In parallel mode, we already set the pointer to dead tuple
	 * array when initialize.
	 */
	if (lvstate->parallel_mode && lvstate->vacrelstats->nindexes > 0)
		return;

	maxtuples = lazy_get_max_dead_tuples(lvstate->vacrelstats);

	/*
	 * If in not parallel lazy vacuum, we need to allocate dead
	 * tuple array in local memory.
	 */
	lvstate->deadtuples = palloc0(sizeof(ItemPointerData) * (int)maxtuples);
	lvstate->dtctl = (LVDeadTupleCtl *) palloc(sizeof(LVDeadTupleCtl));
	lvstate->dtctl->dt_max = lazy_get_max_dead_tuples(lvstate->vacrelstats);
	lvstate->dtctl->dt_count = 0;
}

/*
 * lazy_record_dead_tuple - remember one deletable tuple
 *
 * Acquiring the spinlock before remember is required if the dead tuple
 * TIDs are shared with other vacuum workers.
 */
static void
lazy_record_dead_tuple(LVState *lvstate, ItemPointer itemptr)
{
	LVDeadTupleCtl *dtctl = lvstate->dtctl;

	if (IsDeadTupleShared(lvstate))
		SpinLockAcquire(&(dtctl->mutex));

	/*
	 * The array shouldn't overflow under normal behavior, but perhaps it
	 * could if we are given a really small maintenance_work_mem. In that
	 * case, just forget the last few tuples (we'll get 'em next time).
	 */
	if (dtctl->dt_count < dtctl->dt_max)
	{

		lvstate->deadtuples[dtctl->dt_count] = *itemptr;
		(dtctl->dt_count)++;
		/* XXX : Update progress information here */
	}

	if (IsDeadTupleShared(lvstate))
		SpinLockRelease(&(dtctl->mutex));
}

/*
 *	lazy_tid_reaped() -- is a particular tid deletable?
 *
 *		This has the right signature to be an IndexBulkDeleteCallback.
 *
 *		Assumes dead_tuples array is in sorted order.
 */
static bool
lazy_tid_reaped(ItemPointer itemptr, void *state)
{
	LVState *lvstate = (LVState *) state;
	ItemPointer res;

	/*
	 * We can assume that the dead tuple TIDs are sorted by TID location
	 * even when we shared the dead tuple TIDs with other vacuum workers.
	 */
	res = (ItemPointer) bsearch((void *) itemptr,
								(void *) lvstate->deadtuples,
								lvstate->dtctl->dt_count,
								sizeof(ItemPointerData),
								vac_cmp_itemptr);

	if (res != NULL)
		return true;

	return false;
}

/*
 * Comparator routines for use with qsort() and bsearch().
 */
static int
vac_cmp_itemptr(const void *left, const void *right)
{
	BlockNumber lblk,
				rblk;
	OffsetNumber loff,
				roff;

	lblk = ItemPointerGetBlockNumber((ItemPointer) left);
	rblk = ItemPointerGetBlockNumber((ItemPointer) right);

	if (lblk < rblk)
		return -1;
	if (lblk > rblk)
		return 1;

	loff = ItemPointerGetOffsetNumber((ItemPointer) left);
	roff = ItemPointerGetOffsetNumber((ItemPointer) right);

	if (loff < roff)
		return -1;
	if (loff > roff)
		return 1;

	return 0;
}

/*
 * Check if every tuple in the given page is visible to all current and future
 * transactions. Also return the visibility_cutoff_xid which is the highest
 * xmin amongst the visible tuples.  Set *all_frozen to true if every tuple
 * on this page is frozen.
 */
static bool
heap_page_is_all_visible(Relation rel, Buffer buf,
						 TransactionId *visibility_cutoff_xid,
						 bool *all_frozen)
{
	Page		page = BufferGetPage(buf);
	BlockNumber blockno = BufferGetBlockNumber(buf);
	OffsetNumber offnum,
				maxoff;
	bool		all_visible = true;

	*visibility_cutoff_xid = InvalidTransactionId;
	*all_frozen = true;

	/*
	 * This is a stripped down version of the line pointer scan in
	 * lazy_scan_heap(). So if you change anything here, also check that code.
	 */
	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff && all_visible;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid;
		HeapTupleData tuple;

		itemid = PageGetItemId(page, offnum);

		/* Unused or redirect line pointers are of no interest */
		if (!ItemIdIsUsed(itemid) || ItemIdIsRedirected(itemid))
			continue;

		ItemPointerSet(&(tuple.t_self), blockno, offnum);

		/*
		 * Dead line pointers can have index pointers pointing to them. So
		 * they can't be treated as visible
		 */
		if (ItemIdIsDead(itemid))
		{
			all_visible = false;
			*all_frozen = false;
			break;
		}

		Assert(ItemIdIsNormal(itemid));

		tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
		tuple.t_len = ItemIdGetLength(itemid);
		tuple.t_tableOid = RelationGetRelid(rel);

		switch (HeapTupleSatisfiesVacuum(&tuple, OldestXmin, buf))
		{
			case HEAPTUPLE_LIVE:
				{
					TransactionId xmin;

					/* Check comments in lazy_scan_heap. */
					if (!HeapTupleHeaderXminCommitted(tuple.t_data))
					{
						all_visible = false;
						*all_frozen = false;
						break;
					}

					/*
					 * The inserter definitely committed. But is it old enough
					 * that everyone sees it as committed?
					 */
					xmin = HeapTupleHeaderGetXmin(tuple.t_data);
					if (!TransactionIdPrecedes(xmin, OldestXmin))
					{
						all_visible = false;
						*all_frozen = false;
						break;
					}

					/* Track newest xmin on page. */
					if (TransactionIdFollows(xmin, *visibility_cutoff_xid))
						*visibility_cutoff_xid = xmin;

					/* Check whether this tuple is already frozen or not */
					if (all_visible && *all_frozen &&
						heap_tuple_needs_eventual_freeze(tuple.t_data))
						*all_frozen = false;
				}
				break;

			case HEAPTUPLE_DEAD:
			case HEAPTUPLE_RECENTLY_DEAD:
			case HEAPTUPLE_INSERT_IN_PROGRESS:
			case HEAPTUPLE_DELETE_IN_PROGRESS:
				{
					all_visible = false;
					*all_frozen = false;
					break;
				}
			default:
				elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
				break;
		}
	}							/* scan along page */

	return all_visible;
}

/*
 * Return the block number we need to scan next, or InvalidBlockNumber if scan
 * is done.
 *
 * Except when aggressive is set, we want to skip pages that are
 * all-visible according to the visibility map, but only when we can skip
 * at least SKIP_PAGES_THRESHOLD consecutive pages.  Since we're reading
 * sequentially, the OS should be doing readahead for us, so there's no
 * gain in skipping a page now and then; that's likely to disable
 * readahead and so be counterproductive. Also, skipping even a single
 * page means that we can't update relfrozenxid, so we only want to do it
 * if we can skip a goodly number of pages.
 *
 * When aggressive is set, we can't skip pages just because they are
 * all-visible, but we can still skip pages that are all-frozen, since
 * such pages do not need freezing and do not affect the value that we can
 * safely set for relfrozenxid or relminmxid.
 *
 * Before entering the main loop, establish the invariant that
 * next_unskippable_block is the next block number >= blkno that we can't
 * skip based on the visibility map, either all-visible for a regular scan
 * or all-frozen for an aggressive scan.  We set it to nblocks if there's
 * no such block.  We also set up the skipping_blocks flag correctly at
 * this stage.
 *
 * In not parallel mode, before entering the main loop, establish the
 * invariant that next_unskippable_block is the next block number >= blkno
 * that's not we can't skip based on the visibility map, either all-visible
 * for a regular scan or all-frozen for an aggressive scan.  We set it to
 * nblocks if there's no such block.  We also set up the skipping_blocks
 * flag correctly at this stage.
 *
 * In parallel mode, pstate is not NULL. We scan heap pages
 * using parallel heap scan description. Each worker calls heap_parallelscan_nextpage()
 * in order to exclusively get  block number we need to scan at next.
 * If given block is all-visible according to visibility map, we skip to
 * scan this block immediately unlike not parallel lazy scan.
 *
 * Note: The value returned by visibilitymap_get_status could be slightly
 * out-of-date, since we make this test before reading the corresponding
 * heap page or locking the buffer.  This is OK.  If we mistakenly think
 * that the page is all-visible or all-frozen when in fact the flag's just
 * been cleared, we might fail to vacuum the page.  It's easy to see that
 * skipping a page when aggressive is not set is not a very big deal; we
 * might leave some dead tuples lying around, but the next vacuum will
 * find them.  But even when aggressive *is* set, it's still OK if we miss
 * a page whose all-frozen marking has just been cleared.  Any new XIDs
 * just added to that page are necessarily newer than the GlobalXmin we
 * Computed, so they'll have no effect on the value to which we can safely
 * set relfrozenxid.  A similar argument applies for MXIDs and relminmxid.
 *
 * We will scan the table's last page, at least to the extent of
 * determining whether it has tuples or not, even if it should be skipped
 * according to the above rules; except when we've already determined that
 * it's not worth trying to truncate the table.  This avoids having
 * lazy_truncate_heap() take access-exclusive lock on the table to attempt
 * a truncation that just fails immediately because there are tuples in
 * the last page.  This is worth avoiding mainly because such a lock must
 * be replayed on any hot standby, where it can be disruptive.
 */
static BlockNumber
lazy_scan_get_nextpage(Relation onerel, LVState *lvstate,
					   LVScanDesc lvscan, bool *all_visible_according_to_vm,
					   Buffer *vmbuffer, int options, bool aggressive)
{
	BlockNumber blkno;
	LVRelStats	*vacrelstats = lvstate->vacrelstats;

	if (lvstate->parallel_mode)
	{
		/*
		 * In parallel lazy vacuum since it's hard to know how many consecutive
		 * all-visible pages exits on table we skip to scan the heap page immediately.
		 * if it is all-visible page.
		 */
		while ((blkno = heap_parallelscan_nextpage(lvscan->heapscan)) != InvalidBlockNumber)
		{
			*all_visible_according_to_vm = false;
			vacuum_delay_point();

			/* Consider to skip scan page according visibility map */
			if ((options & VACOPT_DISABLE_PAGE_SKIPPING) == 0 &&
				!FORCE_CHECK_PAGE(blkno))
			{
				uint8		vmstatus;

				vmstatus = visibilitymap_get_status(onerel, blkno, vmbuffer);

				if (aggressive)
				{
					if ((vmstatus & VISIBILITYMAP_ALL_FROZEN) != 0)
					{
						vacrelstats->frozenskipped_pages++;
						continue;
					}
					else if ((vmstatus & VISIBILITYMAP_ALL_VISIBLE) != 0)
						*all_visible_according_to_vm = true;
				}
				else
				{
					if ((vmstatus & VISIBILITYMAP_ALL_VISIBLE) != 0)
					{
						if ((vmstatus & VISIBILITYMAP_ALL_FROZEN) != 0)
							vacrelstats->frozenskipped_pages++;
						continue;
					}
				}
			}

			/* We need to scan current blkno, break */
			break;
		}
	}
	else
	{
		bool skipping_blocks = false;

		/* Initialize lv_nextunskippable_page if needed */
		if (lvscan->lv_cblock == 0 && (options & VACOPT_DISABLE_PAGE_SKIPPING) == 0)
		{
			while (lvscan->lv_next_unskippable_block < lvscan->lv_nblocks)
			{
				uint8		vmstatus;

				vmstatus = visibilitymap_get_status(onerel, lvscan->lv_next_unskippable_block,
													vmbuffer);
				if (aggressive)
				{
					if ((vmstatus & VISIBILITYMAP_ALL_FROZEN) == 0)
						break;
				}
				else
				{
					if ((vmstatus & VISIBILITYMAP_ALL_VISIBLE) == 0)
						break;
				}
				vacuum_delay_point();
				lvscan->lv_next_unskippable_block++;
			}

			if (lvscan->lv_next_unskippable_block >= SKIP_PAGES_THRESHOLD)
				skipping_blocks = true;
			else
				skipping_blocks = false;
		}

		/* Decide the block number we need to scan */
		for (blkno = lvscan->lv_cblock; blkno < lvscan->lv_nblocks; blkno++)
		{
			if (blkno == lvscan->lv_next_unskippable_block)
			{
				/* Time to advance next_unskippable_block */
				lvscan->lv_next_unskippable_block++;
				if ((options & VACOPT_DISABLE_PAGE_SKIPPING) == 0)
				{
					while (lvscan->lv_next_unskippable_block < lvscan->lv_nblocks)
					{
						uint8		vmstatus;

						vmstatus = visibilitymap_get_status(onerel,
															lvscan->lv_next_unskippable_block,
															vmbuffer);
						if (aggressive)
						{
							if ((vmstatus & VISIBILITYMAP_ALL_FROZEN) == 0)
								break;
						}
						else
						{
							if ((vmstatus & VISIBILITYMAP_ALL_VISIBLE) == 0)
								break;
						}
						vacuum_delay_point();
						lvscan->lv_next_unskippable_block++;
					}
				}

				/*
				 * We know we can't skip the current block.  But set up
				 * skipping_all_visible_blocks to do the right thing at the
				 * following blocks.
				 */
				if (lvscan->lv_next_unskippable_block - blkno > SKIP_PAGES_THRESHOLD)
					skipping_blocks = true;
				else
					skipping_blocks = false;

				/*
				 * Normally, the fact that we can't skip this block must mean that
				 * it's not all-visible.  But in an aggressive vacuum we know only
				 * that it's not all-frozen, so it might still be all-visible.
				 */
				if (aggressive && VM_ALL_VISIBLE(onerel, blkno, vmbuffer))
					*all_visible_according_to_vm = true;

				/* Found out that next unskippable block number */
				break;
			}
			else
			{
				/*
				 * The current block is potentially skippable; if we've seen a
				 * long enough run of skippable blocks to justify skipping it, and
				 * we're not forced to check it, then go ahead and skip.
				 * Otherwise, the page must be at least all-visible if not
				 * all-frozen, so we can set all_visible_according_to_vm = true.
				 */
				if (skipping_blocks && !FORCE_CHECK_PAGE(blkno))
				{
					/*
					 * Tricky, tricky.  If this is in aggressive vacuum, the page
					 * must have been all-frozen at the time we checked whether it
					 * was skippable, but it might not be any more.  We must be
					 * careful to count it as a skipped all-frozen page in that
					 * case, or else we'll think we can't update relfrozenxid and
					 * relminmxid.  If it's not an aggressive vacuum, we don't
					 * know whether it was all-frozen, so we have to recheck; but
					 * in this case an approximate answer is OK.
					 */
					if (aggressive || VM_ALL_FROZEN(onerel, blkno, vmbuffer))
						vacrelstats->frozenskipped_pages++;
					continue;
				}

				*all_visible_according_to_vm = true;

				/* We need to scan current blkno, break */
				break;
			}
		} /* for */

		/* Advance the current block number for the next scan */
		lvscan->lv_cblock = blkno + 1;
	}

	return (blkno == lvscan->lv_nblocks) ? InvalidBlockNumber : blkno;
}

/*
 * Begin lazy vacuum scan. lvscan->heapscan is NULL if
 * we're not in parallel lazy vacuum.
 */
static LVScanDesc
lv_beginscan(Relation onerel, ParallelHeapScanDesc pscan)
{
	LVScanDesc lvscan;

	lvscan = (LVScanDesc) palloc(sizeof(LVScanDescData));

	lvscan->lv_cblock = 0;
	lvscan->lv_next_unskippable_block = 0;
	lvscan->lv_nblocks = RelationGetNumberOfBlocks(onerel);

	if (pscan != NULL)
		lvscan->heapscan = heap_beginscan_parallel(onerel, pscan);
	else
		lvscan->heapscan = NULL;

	return lvscan;
}

/*
 * End lazy vacuum scan.
 */
static void
lv_endscan(LVScanDesc lvscan)
{
	if (lvscan->heapscan != NULL)
		heap_endscan(lvscan->heapscan);
	pfree(lvscan);
}

/* ----------------------------------------------------------------
 *						Parallel Lazy Vacuum Support
 * ----------------------------------------------------------------
 */

/*
 * Estimate storage for parallel lazy vacuum.
 */
static void
lazy_estimate_dsm(ParallelContext *pcxt, LVRelStats *vacrelstats)
{
	Size size = 0;
	int keys = 0;
	int vacuum_workers = pcxt->nworkers + 1;
	long maxtuples = lazy_get_max_dead_tuples(vacrelstats);

	/* Estimate size for parallel heap scan */
	size += heap_parallelscan_estimate(SnapshotAny);
	keys++;

	/* Estimate size for vacuum statistics for only workers*/
	size += BUFFERALIGN(mul_size(sizeof(LVRelStats), pcxt->nworkers));
	keys++;

	/* We have to share dead tuple information only when the table has indexes */
	if (vacrelstats->nindexes > 0)
	{
		/* Estimate size for index statistics */
		size += BUFFERALIGN(mul_size(sizeof(LVIndStats), vacrelstats->nindexes));
		keys++;

		/* Estimate size for dead tuple control */
		size += BUFFERALIGN(sizeof(LVDeadTupleCtl));
		keys++;

		/* Estimate size for dead tuple array */
		size += BUFFERALIGN(mul_size(
							 mul_size(sizeof(ItemPointerData), maxtuples),
							 vacuum_workers));
		keys++;
	}

	/* Estimate size for parallel lazy vacuum state */
	size += BUFFERALIGN(sizeof(LVParallelState));
	keys++;

	/* Estimate size for vacuum task */
	size += BUFFERALIGN(sizeof(VacuumInfo));
	keys++;

	shm_toc_estimate_chunk(&pcxt->estimator, size);
	shm_toc_estimate_keys(&pcxt->estimator, keys);
}

/*
 * Initialize dynamic shared memory for parallel lazy vacuum. We store
 * relevant informations of parallel heap scanning, dead tuple array
 * and vacuum statistics for each worker and some parameters for lazy vacuum.
 */
static void
lazy_initialize_dsm(ParallelContext *pcxt, Relation onerel, LVState *lvstate,
					int options, bool aggressive)
{
	ParallelHeapScanDesc pscan_ptr;
	ItemPointer	deadtuples_ptr;
	char 		*lvrelstats_ptr;
	LVParallelState *pstate_ptr;
	LVIndStats	*indstats_ptr;
	LVDeadTupleCtl	*dtctl_ptr;
	int i;
	int deadtuples_size;
	int lvrelstats_size;
	int	vacuum_workers = pcxt->nworkers + 1;
	long max_tuples = lazy_get_max_dead_tuples(lvstate->vacrelstats);

	/* Allocate and initialize DSM for vacuum stats for each worker */
	lvrelstats_size = mul_size(sizeof(LVRelStats), pcxt->nworkers);
	lvrelstats_ptr = shm_toc_allocate(pcxt->toc, lvrelstats_size);
	for (i = 0; i < pcxt->nworkers; i++)
	{
		char *start;

		start = lvrelstats_ptr + i * sizeof(LVRelStats);
		memcpy(start, lvstate->vacrelstats, sizeof(LVRelStats));
	}
	shm_toc_insert(pcxt->toc, VACUUM_KEY_VACUUM_STATS, lvrelstats_ptr);

	if (lvstate->vacrelstats->nindexes > 0)
	{
		/* Allocate and initialize DSM for dead tuple control */
		dtctl_ptr = (LVDeadTupleCtl *) shm_toc_allocate(pcxt->toc, sizeof(LVDeadTupleCtl));
		SpinLockInit(&(dtctl_ptr->mutex));
		dtctl_ptr->dt_max = max_tuples * vacuum_workers;
		dtctl_ptr->dt_count = 0;
		dtctl_ptr->dt_index = 0;
		lvstate->dtctl = dtctl_ptr;
		shm_toc_insert(pcxt->toc, VACUUM_KEY_DEAD_TUPLE_CTL, dtctl_ptr);

		/* Allocate and initialize DSM for dead tuple array */
		deadtuples_size = mul_size(mul_size(sizeof(ItemPointerData), max_tuples),
								   vacuum_workers);
		deadtuples_ptr = (ItemPointer) shm_toc_allocate(pcxt->toc,
														deadtuples_size);
		shm_toc_insert(pcxt->toc, VACUUM_KEY_DEAD_TUPLES, deadtuples_ptr);
		lvstate->deadtuples = deadtuples_ptr;

		/* Allocate DSM for index statistics */
		indstats_ptr = (LVIndStats *) shm_toc_allocate(pcxt->toc,
													   mul_size(sizeof(LVIndStats),
																lvstate->vacrelstats->nindexes));
		shm_toc_insert(pcxt->toc, VACUUM_KEY_INDEX_STATS, indstats_ptr);
		lvstate->indstats = indstats_ptr;
	}

	/* Allocate and initialize DSM for parallel scan description */
	pscan_ptr = (ParallelHeapScanDesc) shm_toc_allocate(pcxt->toc,
														heap_parallelscan_estimate(SnapshotAny));
	shm_toc_insert(pcxt->toc, VACUUM_KEY_PARALLEL_SCAN, pscan_ptr);
	heap_parallelscan_initialize(pscan_ptr, onerel, SnapshotAny);
	lvstate->pscan = pscan_ptr;

	/* Allocate and initialize DSM for parallel vacuum state */
	pstate_ptr = (LVParallelState *) shm_toc_allocate(pcxt->toc, sizeof(LVParallelState));
	shm_toc_insert(pcxt->toc, VACUUM_KEY_PARALLEL_STATE, pstate_ptr);

	ConditionVariableInit(&(pstate_ptr->cv));
	SpinLockInit(&(pstate_ptr->mutex));
	pstate_ptr->nworkers = vacuum_workers;
	pstate_ptr->state = VACSTATE_SCAN;
	pstate_ptr->info.aggressive = aggressive;
	pstate_ptr->info.options = options;
	pstate_ptr->info.oldestxmin = OldestXmin;
	pstate_ptr->info.freezelimit = FreezeLimit;
	pstate_ptr->info.multixactcutoff = MultiXactCutoff;
	pstate_ptr->info.elevel = elevel;
	lvstate->pstate = pstate_ptr;
}

/*
 * Initialize parallel lazy vacuum for worker.
 */
static LVState *
lazy_initialize_worker(shm_toc *toc)
{
	LVState	*lvstate;
	char *lvstats;

	lvstate = (LVState *) palloc(sizeof(LVState));
	lvstate->parallel_mode = true;

	/* Set up vacuum stats */
	lvstats = shm_toc_lookup(toc, VACUUM_KEY_VACUUM_STATS, false);
	lvstate->vacrelstats = (LVRelStats *) (lvstats +
										   sizeof(LVRelStats) * ParallelWorkerNumber);

	if (lvstate->vacrelstats->nindexes > 0)
	{
		/* Set up dead tuple control */
		lvstate->dtctl = (LVDeadTupleCtl *) shm_toc_lookup(toc, VACUUM_KEY_DEAD_TUPLE_CTL, false);

		/* Set up dead tuple array */
		lvstate->deadtuples = (ItemPointer) shm_toc_lookup(toc, VACUUM_KEY_DEAD_TUPLES, false);

		/* Set up index statistics */
		lvstate->indstats = (LVIndStats *) shm_toc_lookup(toc, VACUUM_KEY_INDEX_STATS, false);
	}

	/* Set up parallel vacuum state */
	lvstate->pstate = (LVParallelState *) shm_toc_lookup(toc, VACUUM_KEY_PARALLEL_STATE, false);

	/* Set up parallel heap scan description */
	lvstate->pscan = (ParallelHeapScanDesc) shm_toc_lookup(toc, VACUUM_KEY_PARALLEL_SCAN, false);

	/* Set up parameters for lazy vacuum */
	OldestXmin = lvstate->pstate->info.oldestxmin;
	FreezeLimit = lvstate->pstate->info.freezelimit;
	MultiXactCutoff = lvstate->pstate->info.multixactcutoff;
	elevel = lvstate->pstate->info.elevel;

	return lvstate;
}

/*
 * In the end of actual vacuumming on table and indexes actually, we have
 * to wait for other all vacuum workers to reach here before clearing dead
 * tuple TIDs information.
 */
static void
lazy_end_vacuum(LVState *lvstate)
{
	LVParallelState *pstate = lvstate->pstate;
	bool counted = false;

	/* Exit if in not parallel vacuum */
	if (!lvstate->parallel_mode)
	{
		lvstate->dtctl->dt_count = 0;
		return;
	}

	while (true)
	{
		int finish_count;
		int state;

		SpinLockAcquire(&(pstate->mutex));

		/* Fetch shared information */
		if (!counted)
			pstate->finish_count++;
		finish_count = pstate->finish_count;
		state = pstate->state;

		SpinLockRelease(&(pstate->mutex));

		if (state == VACSTATE_SCAN)
			break;

		/*
		 * Wake up other workers if counted up if first time to reach here and
		 * is a parallel worker.
		 */
		if (!counted && IsParallelWorker())
			ConditionVariableBroadcast(&pstate->cv);

		counted = true;

		/*
		 * If all launched parallel vacuum workers reached here, we can clear the
		 * dead tuple TIDs information.
		 */
		if (!IsParallelWorker() && finish_count == (lvstate->pcxt->nworkers_launched + 1))
		{
			/* Clear dead tuples */
			lvstate->dtctl->dt_count = 0;

			/* need spinlock ? */
			pstate->finish_count = 0;
			pstate->state = VACSTATE_SCAN;

			ConditionVariableBroadcast(&pstate->cv);
			break;
		}

		ConditionVariableSleep(&pstate->cv, WAIT_EVENT_PARALLEL_VACUUM_DONE);
	}

	ConditionVariableCancelSleep();
}

/*
 * Before starting actual vacuuming on table and indexes, we have to wait for
 * other all vacuum workers so that all worker can see the same dead tuple TIDs
 * information when vacuuming.
 */
static void
lazy_prepare_vacuum(LVState *lvstate)
{
	LVParallelState *pstate = lvstate->pstate;
	bool counted = false;

	/* Exit if in not parallel vacuum */
	if (!lvstate->parallel_mode)
		return;

	while (true)
	{
		int finish_count;
		int state;

		SpinLockAcquire(&(pstate->mutex));

		if (!counted)
			pstate->finish_count++;
		state = pstate->state;
		finish_count = pstate->finish_count;

		SpinLockRelease(&(pstate->mutex));

		if (state == VACSTATE_VACUUM)
			break;

		/*
		 * Wake up other workers if counted up if first time to reach here and
		 * is a parallel worker.
		 */
		if (!counted && IsParallelWorker())
			ConditionVariableBroadcast(&pstate->cv);

		counted = true;

		/*
		 * The leader process can change parallel vacuum state if all workers
		 * have reached here.
		 */
		if (!IsParallelWorker() && finish_count == (lvstate->pcxt->nworkers_launched + 1))
		{
			qsort((void *) lvstate->deadtuples, lvstate->dtctl->dt_count,
				  sizeof(ItemPointerData), vac_cmp_itemptr);

			/* XXX: need spinlock ? */
			pstate->finish_count = 0;
			pstate->state = VACSTATE_VACUUM;

			ConditionVariableBroadcast(&pstate->cv);
			break;
		}

		ConditionVariableSleep(&pstate->cv, WAIT_EVENT_PARALLEL_VACUUM_PREPARE);
	}

	ConditionVariableCancelSleep();
}

/*
 * Return the number of maximum dead tuples can be stored according
 * to vac_work_mem.
 */
static long
lazy_get_max_dead_tuples(LVRelStats *vacrelstats)
{
	long maxtuples;
	int	vac_work_mem = IsAutoVacuumWorkerProcess() &&
		autovacuum_work_mem != -1 ?
		autovacuum_work_mem : maintenance_work_mem;

	if (vacrelstats->nindexes != 0)
	{
		maxtuples = (vac_work_mem * 1024L) / sizeof(ItemPointerData);
		maxtuples = Min(maxtuples, INT_MAX);
		maxtuples = Min(maxtuples, MaxAllocSize / sizeof(ItemPointerData));

		/* curious coding here to ensure the multiplication can't overflow */
		if ((BlockNumber) (maxtuples / LAZY_ALLOC_TUPLES) > vacrelstats->old_rel_pages)
			maxtuples = vacrelstats->old_rel_pages * LAZY_ALLOC_TUPLES;

		/* stay sane if small maintenance_work_mem */
		maxtuples = Max(maxtuples, MaxHeapTuplesPerPage);
	}
	else
	{
		maxtuples = MaxHeapTuplesPerPage;
	}

	return maxtuples;
}
