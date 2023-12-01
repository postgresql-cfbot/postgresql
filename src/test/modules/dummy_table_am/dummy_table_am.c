/*-------------------------------------------------------------------------
 *
 * dummy_table_am.c
 *		Index AM template main file.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/test/modules/dummy_table_am/dummy_table_am.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "commands/vacuum.h"

PG_MODULE_MAGIC;

/* Handler for table AM */
PG_FUNCTION_INFO_V1(dummy_table_am_handler);

/* Base structures for scans */
typedef struct DummyScanDescData
{
	TableScanDescData rs_base;
}			DummyScanDescData;

typedef struct DummyScanDescData *DummyScanDesc;

/*
 * Slot related callbacks for Dummy Table Access Method
 */
static const TupleTableSlotOps *
dummy_table_am_slot_callbacks(Relation relation)
{
	elog(INFO, "%s", __func__);
	return &TTSOpsMinimalTuple;
}

/*
 * Table Scan Callbacks for dummy_table_am AM
 */
static TableScanDesc
dummy_table_am_scan_begin(Relation relation, Snapshot snapshot,
						  int nkeys, ScanKey key,
						  ParallelTableScanDesc parallel_scan,
						  uint32 flags)
{
	DummyScanDesc scan;

	scan = (DummyScanDesc) palloc(sizeof(DummyScanDescData));

	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = parallel_scan;

	elog(INFO, "%s", __func__);

	return (TableScanDesc) scan;
}

static void
dummy_table_am_scan_end(TableScanDesc sscan)
{
	DummyScanDesc scan = (DummyScanDesc) sscan;

	elog(INFO, "%s", __func__);

	pfree(scan);
}

static void
dummy_table_am_scan_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
						   bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	elog(INFO, "%s", __func__);
}

static bool
dummy_table_am_scan_getnextslot(TableScanDesc sscan, ScanDirection direction,
								TupleTableSlot *slot)
{
	elog(INFO, "%s", __func__);
	return false;
}

/*
 * Index Scan Callbacks for dummy_table_am AM
 */
static IndexFetchTableData *
dummy_table_am_index_fetch_begin(Relation rel)
{
	elog(INFO, "%s", __func__);
	return NULL;
}

static void
dummy_table_am_index_fetch_reset(IndexFetchTableData *scan)
{
	elog(INFO, "%s", __func__);
}

static void
dummy_table_am_index_fetch_end(IndexFetchTableData *scan)
{
	elog(INFO, "%s", __func__);
}

static bool
dummy_table_am_index_fetch_tuple(struct IndexFetchTableData *scan,
								 ItemPointer tid,
								 Snapshot snapshot,
								 TupleTableSlot *slot,
								 bool *call_again, bool *all_dead)
{
	elog(INFO, "%s", __func__);
	return 0;
}

/*
 * Callbacks for non-modifying operations on individual tuples for
 * dummy_table_am AM.
 */
static bool
dummy_table_am_fetch_row_version(Relation relation,
								 ItemPointer tid,
								 Snapshot snapshot,
								 TupleTableSlot *slot)
{
	elog(INFO, "%s", __func__);
	return false;
}

static void
dummy_table_am_get_latest_tid(TableScanDesc sscan,
							  ItemPointer tid)
{
	elog(INFO, "%s", __func__);
}

static bool
dummy_table_am_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	elog(INFO, "%s", __func__);
	return false;
}

static bool
dummy_table_am_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
										Snapshot snapshot)
{
	elog(INFO, "%s", __func__);
	return false;
}

static TransactionId
dummy_table_am_index_delete_tuples(Relation rel,
								   TM_IndexDeleteOp *delstate)
{
	elog(INFO, "%s", __func__);
	return InvalidTransactionId;
}

/*
 * Functions for manipulations of physical tuples for dummy_table_am AM.
 */
static void
dummy_table_am_tuple_insert(Relation relation, TupleTableSlot *slot,
							CommandId cid, int options, BulkInsertState bistate)
{
	elog(INFO, "%s", __func__);
}

static void
dummy_table_am_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
										CommandId cid, int options,
										BulkInsertState bistate,
										uint32 specToken)
{
	elog(INFO, "%s", __func__);
}

static void
dummy_table_am_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
										  uint32 spekToken, bool succeeded)
{
	elog(INFO, "%s", __func__);
}

static void
dummy_table_am_multi_insert(Relation relation, TupleTableSlot **slots,
							int ntuples, CommandId cid, int options,
							BulkInsertState bistate)
{
	elog(INFO, "%s", __func__);
}

static TM_Result
dummy_table_am_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
							Snapshot snapshot, Snapshot crosscheck, bool wait,
							TM_FailureData *tmfd, bool changingPart)
{
	elog(INFO, "%s", __func__);

	/* nothing to do, so it is always OK */
	return TM_Ok;
}

static TM_Result
dummy_table_am_tuple_update(Relation relation, ItemPointer otid,
							TupleTableSlot *slot, CommandId cid,
							Snapshot snapshot, Snapshot crosscheck,
							bool wait, TM_FailureData *tmfd,
							LockTupleMode *lockmode,
							TU_UpdateIndexes *update_indexes)
{
	elog(INFO, "%s", __func__);

	/* nothing to do, so it is always OK */
	return TM_Ok;
}

static TM_Result
dummy_table_am_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
						  TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
						  LockWaitPolicy wait_policy, uint8 flags,
						  TM_FailureData *tmfd)
{
	elog(INFO, "%s", __func__);

	/* nothing to do, so it is always OK */
	return TM_Ok;
}

static void
dummy_table_am_finish_bulk_insert(Relation relation, int options)
{
	elog(INFO, "%s", __func__);
}

/*
 * DDL related callbacks for dummy_table_am AM.
 */
static void
dummy_table_am_relation_set_new_filelocator(Relation rel,
											const RelFileLocator *newrnode,
											char persistence,
											TransactionId *freezeXid,
											MultiXactId *minmulti)
{
	elog(INFO, "%s", __func__);
}

static void
dummy_table_am_relation_nontransactional_truncate(Relation rel)
{
	elog(INFO, "%s", __func__);
}

static void
dummy_table_am_copy_data(Relation rel, const RelFileLocator *newrnode)
{
	elog(INFO, "%s", __func__);
}

static void
dummy_table_am_copy_for_cluster(Relation OldTable, Relation NewTable,
								Relation OldIndex, bool use_sort,
								TransactionId OldestXmin,
								TransactionId *xid_cutoff,
								MultiXactId *multi_cutoff,
								double *num_tuples,
								double *tups_vacuumed,
								double *tups_recently_dead)
{
	elog(INFO, "%s", __func__);
}

static void
dummy_table_am_vacuum(Relation onerel, VacuumParams *params,
					  BufferAccessStrategy bstrategy)
{
	elog(INFO, "%s", __func__);
}

static bool
dummy_table_am_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
									   BufferAccessStrategy bstrategy)
{
	elog(INFO, "%s", __func__);

	/* no data, so no point to analyze next block */
	return false;
}

static bool
dummy_table_am_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
									   double *liverows, double *deadrows,
									   TupleTableSlot *slot)
{
	elog(INFO, "%s", __func__);

	/* no data, so no point to analyze next tuple */
	return false;
}

static double
dummy_table_am_index_build_range_scan(Relation tableRelation,
									  Relation indexRelation,
									  IndexInfo *indexInfo,
									  bool allow_sync,
									  bool anyvisible,
									  bool progress,
									  BlockNumber start_blockno,
									  BlockNumber numblocks,
									  IndexBuildCallback callback,
									  void *callback_state,
									  TableScanDesc scan)
{
	elog(ERROR, "%s", __func__);

	/* no data, so no tuples */
	return 0;
}

static void
dummy_table_am_index_validate_scan(Relation tableRelation,
								   Relation indexRelation,
								   IndexInfo *indexInfo,
								   Snapshot snapshot,
								   ValidateIndexState *state)
{
	elog(INFO, "%s", __func__);
}

/*
 * Miscellaneous callbacks for the dummy_table_am AM
 */
static uint64
dummy_table_am_relation_size(Relation rel, ForkNumber forkNumber)
{
	elog(INFO, "%s", __func__);

	/* there is nothing */
	return 0;
}

/*
 * Check to see whether the table needs a TOAST table.
 */
static bool
dummy_table_am_relation_needs_toast_table(Relation rel)
{
	elog(INFO, "%s", __func__);

	/* no data, so no toast table needed */
	return false;
}

/*
 * Planner related callbacks for the dummy_table_am AM
 */
static void
dummy_table_am_estimate_rel_size(Relation rel, int32 *attr_widths,
								 BlockNumber *pages, double *tuples,
								 double *allvisfrac)
{
	/* no data available */
	if (attr_widths)
		*attr_widths = 0;
	if (pages)
		*pages = 0;
	if (tuples)
		*tuples = 0;
	if (allvisfrac)
		*allvisfrac = 0;

	elog(INFO, "%s", __func__);
}


/*
 * Executor related callbacks for the dummy_table_am AM
 */
static bool
dummy_table_am_scan_bitmap_next_block(TableScanDesc scan,
									  TBMIterateResult *tbmres)
{
	elog(INFO, "%s", __func__);

	/* no data, so no point to scan next block */
	return false;
}

static bool
dummy_table_am_scan_bitmap_next_tuple(TableScanDesc scan,
									  TBMIterateResult *tbmres,
									  TupleTableSlot *slot)
{
	elog(INFO, "%s", __func__);

	/* no data, so no point to scan next tuple */
	return false;
}

static bool
dummy_table_am_scan_sample_next_block(TableScanDesc scan,
									  SampleScanState *scanstate)
{
	elog(INFO, "%s", __func__);

	/* no data, so no point to scan next block for sampling */
	return false;
}

static bool
dummy_table_am_scan_sample_next_tuple(TableScanDesc scan,
									  SampleScanState *scanstate,
									  TupleTableSlot *slot)
{
	elog(INFO, "%s", __func__);

	/* no data, so no point to scan next tuple for sampling */
	return false;
}

/*
 * Definition of the dummy_table_am table access method.
 */
static const TableAmRoutine dummy_table_am_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = dummy_table_am_slot_callbacks,

	.scan_begin = dummy_table_am_scan_begin,
	.scan_end = dummy_table_am_scan_end,
	.scan_rescan = dummy_table_am_scan_rescan,
	.scan_getnextslot = dummy_table_am_scan_getnextslot,

	/* these are common helper functions */
	.parallelscan_estimate = table_block_parallelscan_estimate,
	.parallelscan_initialize = table_block_parallelscan_initialize,
	.parallelscan_reinitialize = table_block_parallelscan_reinitialize,

	.index_fetch_begin = dummy_table_am_index_fetch_begin,
	.index_fetch_reset = dummy_table_am_index_fetch_reset,
	.index_fetch_end = dummy_table_am_index_fetch_end,
	.index_fetch_tuple = dummy_table_am_index_fetch_tuple,

	.tuple_insert = dummy_table_am_tuple_insert,
	.tuple_insert_speculative = dummy_table_am_tuple_insert_speculative,
	.tuple_complete_speculative = dummy_table_am_tuple_complete_speculative,
	.multi_insert = dummy_table_am_multi_insert,
	.tuple_delete = dummy_table_am_tuple_delete,
	.tuple_update = dummy_table_am_tuple_update,
	.tuple_lock = dummy_table_am_tuple_lock,
	.finish_bulk_insert = dummy_table_am_finish_bulk_insert,

	.tuple_fetch_row_version = dummy_table_am_fetch_row_version,
	.tuple_get_latest_tid = dummy_table_am_get_latest_tid,
	.tuple_tid_valid = dummy_table_am_tuple_tid_valid,
	.tuple_satisfies_snapshot = dummy_table_am_tuple_satisfies_snapshot,
	.index_delete_tuples = dummy_table_am_index_delete_tuples,

	.relation_set_new_filelocator = dummy_table_am_relation_set_new_filelocator,
	.relation_nontransactional_truncate = dummy_table_am_relation_nontransactional_truncate,
	.relation_copy_data = dummy_table_am_copy_data,
	.relation_copy_for_cluster = dummy_table_am_copy_for_cluster,
	.relation_vacuum = dummy_table_am_vacuum,
	.scan_analyze_next_block = dummy_table_am_scan_analyze_next_block,
	.scan_analyze_next_tuple = dummy_table_am_scan_analyze_next_tuple,
	.index_build_range_scan = dummy_table_am_index_build_range_scan,
	.index_validate_scan = dummy_table_am_index_validate_scan,

	.relation_size = dummy_table_am_relation_size,
	.relation_needs_toast_table = dummy_table_am_relation_needs_toast_table,

	.relation_estimate_size = dummy_table_am_estimate_rel_size,

	.scan_bitmap_next_block = dummy_table_am_scan_bitmap_next_block,
	.scan_bitmap_next_tuple = dummy_table_am_scan_bitmap_next_tuple,
	.scan_sample_next_block = dummy_table_am_scan_sample_next_block,
	.scan_sample_next_tuple = dummy_table_am_scan_sample_next_tuple
};

/*
 * Table AM handler function: returns TableAmRoutine with access method
 * parameters and callbacks.
 */
Datum
dummy_table_am_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&dummy_table_am_methods);
}
