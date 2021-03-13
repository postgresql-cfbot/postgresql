/*-------------------------------------------------------------------------
 *
 * no_storage_handler.c
 *	  no_storage table access method code
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/no_storage/no_storage_handler.c
 *
 *
 * NOTES
 *	  This file introduces the table access method no_storage, that is
 *	  used as a fallback implementation for relation types without
 *	  storage.  Most of the callbacks defined in this file would lead
 *	  to failures for operations not authorized on such relations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "miscadmin.h"

#include "access/tableam.h"
#include "access/heapam.h"
#include "access/amapi.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "utils/fmgrprotos.h"

static const TableAmRoutine no_storage_methods;


/* ------------------------------------------------------------------------
 * Slot related callbacks for no_storage AM
 * ------------------------------------------------------------------------
 */

static const TupleTableSlotOps *
no_storage_slot_callbacks(Relation relation)
{
	if (relation->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
	{
		/*
		 * Historically FDWs expect to store heap tuples in slots. Continue
		 * handing them one, to make it less painful to adapt FDWs to new
		 * versions. The cost of a heap slot over a virtual slot is pretty
		 * small.
		 */
		return &TTSOpsHeapTuple;
	}

	/*
	 * These need to be supported, as some parts of the code (like COPY) need
	 * to create slots for such relations too. It seems better to centralize
	 * the knowledge that a heap slot is the right thing in that case here.
	 */
	if (relation->rd_rel->relkind != RELKIND_VIEW &&
		relation->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		elog(ERROR, "no_storage_slot_callbacks() failed on relation \"%s\"",
			 RelationGetRelationName(relation));
	return &TTSOpsVirtual;
}

/* ------------------------------------------------------------------------
 * Table Scan Callbacks for no_storage AM
 * ------------------------------------------------------------------------
 */

static TableScanDesc
no_storage_scan_begin(Relation relation, Snapshot snapshot,
					  int nkeys, ScanKey key,
					  ParallelTableScanDesc parallel_scan,
					  uint32 flags)
{
	elog(ERROR, "no_storage_scan_begin() failed on relation \"%s\"",
		 RelationGetRelationName(relation));
	return NULL;
}

static void
no_storage_scan_end(TableScanDesc sscan)
{
	elog(ERROR, "no_storage_scan_end() failed");
}

static void
no_storage_scan_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
					   bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	elog(ERROR, "no_storage_scan_rescan() failed");
}

static bool
no_storage_scan_getnextslot(TableScanDesc sscan, ScanDirection direction,
							TupleTableSlot *slot)
{
	elog(ERROR, "no_storage_scan_getnextslot() failed");
	return false;				/* keep compiler quiet */
}

/* ------------------------------------------------------------------------
 * Index Scan Callbacks for no_storage AM
 * ------------------------------------------------------------------------
 */

static IndexFetchTableData *
no_storage_index_fetch_begin(Relation rel)
{
	elog(ERROR, "no_storage_index_fetch_begin() failed on relation \"%s\"",
		 RelationGetRelationName(rel));
	return NULL;
}

static void
no_storage_index_fetch_reset(IndexFetchTableData *scan)
{
	elog(ERROR, "no_storage_index_fetch_reset() failed");
}

static void
no_storage_index_fetch_end(IndexFetchTableData *scan)
{
	elog(ERROR, "no_storage_index_fetch_end() failed");
}

static bool
no_storage_index_fetch_tuple(struct IndexFetchTableData *scan,
							 ItemPointer tid,
							 Snapshot snapshot,
							 TupleTableSlot *slot,
							 bool *call_again, bool *all_dead)
{
	elog(ERROR, "no_storage_index_fetch_tuple() failed");
	return 0;
}


/* ------------------------------------------------------------------------
 * Callbacks for non-modifying operations on individual tuples for
 * no_storage AM.
 * ------------------------------------------------------------------------
 */

static bool
no_storage_fetch_row_version(Relation relation,
							 ItemPointer tid,
							 Snapshot snapshot,
							 TupleTableSlot *slot)
{
	elog(ERROR, "no_storage_fetch_row_version() failed");
	return false;
}

static void
no_storage_get_latest_tid(TableScanDesc sscan,
						  ItemPointer tid)
{
	elog(ERROR, "no_storage_get_latest_tid() failed");
}

static bool
no_storage_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	elog(ERROR, "no_storage_tuple_tid_valid() failed");
	return false;
}

static bool
no_storage_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
									Snapshot snapshot)
{
	elog(ERROR, "no_storage_tuple_satisfies_snapshot() failed on relation \"%s\"",
		 RelationGetRelationName(rel));
	return false;
}

static TransactionId
no_storage_index_delete_tuples(Relation rel,
							   TM_IndexDeleteOp * delstate)
{
	elog(ERROR, "no_storage_index_delete_tuples() failed on relation \"%s\"",
		 RelationGetRelationName(rel));
	return InvalidTransactionId;
}

/* ----------------------------------------------------------------------------
 *  Functions for manipulations of physical tuples for no_storage AM.
 * ----------------------------------------------------------------------------
 */

static void
no_storage_tuple_insert(Relation relation, TupleTableSlot *slot,
						CommandId cid, int options, BulkInsertState bistate)
{
	elog(ERROR, "no_storage_tuple_insert() failed on relation \"%s\"",
		 RelationGetRelationName(relation));
}

static void
no_storage_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
									CommandId cid, int options,
									BulkInsertState bistate,
									uint32 specToken)
{
	elog(ERROR, "no_storage_tuple_insert_speculative() failed on relation \"%s\"",
		 RelationGetRelationName(relation));
}

static void
no_storage_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
									  uint32 spekToken, bool succeeded)
{
	elog(ERROR, "no_storage_tuple_complete_speculative() failed on relation \"%s\"",
		 RelationGetRelationName(relation));
}

static void
no_storage_multi_insert(Relation relation, TupleTableSlot **slots,
						int ntuples, CommandId cid, int options,
						BulkInsertState bistate)
{
	elog(ERROR, "no_storage_multi_insert() failed on relation \"%s\"",
		 RelationGetRelationName(relation));
}

static TM_Result
no_storage_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
						Snapshot snapshot, Snapshot crosscheck, bool wait,
						TM_FailureData *tmfd, bool changingPart)
{
	elog(ERROR, "no_storage_tuple_delete() failed on relation \"%s\"",
		 RelationGetRelationName(relation));
	return TM_Ok;
}


static TM_Result
no_storage_tuple_update(Relation relation, ItemPointer otid,
						TupleTableSlot *slot, CommandId cid,
						Snapshot snapshot, Snapshot crosscheck,
						bool wait, TM_FailureData *tmfd,
						LockTupleMode *lockmode, bool *update_indexes)
{
	elog(ERROR, "no_storage_tuple_update() failed on relation \"%s\"",
		 RelationGetRelationName(relation));
	return TM_Ok;
}

static TM_Result
no_storage_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
					  TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
					  LockWaitPolicy wait_policy, uint8 flags,
					  TM_FailureData *tmfd)
{
	elog(ERROR, "no_storage_tuple_lock() failed on relation \"%s\"",
		 RelationGetRelationName(relation));
	return TM_Ok;
}

static void
no_storage_finish_bulk_insert(Relation relation, int options)
{
	elog(ERROR, "no_storage_finish_bulk_insert() failed on relation \"%s\"",
		 RelationGetRelationName(relation));
}


/* ------------------------------------------------------------------------
 * DDL related callbacks for no_storage AM.
 * ------------------------------------------------------------------------
 */

static void
no_storage_relation_set_new_filenode(Relation rel,
									 const RelFileNode *newrnode,
									 char persistence,
									 TransactionId *freezeXid,
									 MultiXactId *minmulti)
{
	elog(ERROR, "no_storage_relation_set_new_filenode() failed on relation \"%s\"",
		 RelationGetRelationName(rel));
}

static void
no_storage_relation_nontransactional_truncate(Relation rel)
{
	elog(ERROR, "no_storage_relation_nontransactional_truncate() failed on relation \"%s\"",
		 RelationGetRelationName(rel));
}

static void
no_storage_copy_data(Relation rel, const RelFileNode *newrnode)
{
	elog(ERROR, "no_storage_copy_data() failed on relation \"%s\"",
		 RelationGetRelationName(rel));
}

static void
no_storage_copy_for_cluster(Relation OldTable, Relation NewTable,
							Relation OldIndex, bool use_sort,
							TransactionId OldestXmin,
							TransactionId *xid_cutoff,
							MultiXactId *multi_cutoff,
							double *num_tuples,
							double *tups_vacuumed,
							double *tups_recently_dead)
{
	elog(ERROR, "no_storage_copy_for_cluster() failed on relation \"%s\"",
		 RelationGetRelationName(OldTable));
}

static void
no_storage_vacuum(Relation onerel, VacuumParams *params,
				  BufferAccessStrategy bstrategy)
{
	elog(ERROR, "no_storage_vacuum() failed on relation \"%s\"",
		 RelationGetRelationName(onerel));
}

static bool
no_storage_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
								   BufferAccessStrategy bstrategy)
{
	elog(ERROR, "no_storage_scan_analyze_next_block() failed");
	return false;
}

static bool
no_storage_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
								   double *liverows, double *deadrows,
								   TupleTableSlot *slot)
{
	elog(ERROR, "no_storage_scan_analyze_next_tuple() failed");
	return false;
}

static double
no_storage_index_build_range_scan(Relation tableRelation,
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
	elog(ERROR, "no_storage_index_build_range_scan() failed on relation \"%s\"",
		 RelationGetRelationName(tableRelation));
	return 0;
}

static void
no_storage_index_validate_scan(Relation tableRelation,
							   Relation indexRelation,
							   IndexInfo *indexInfo,
							   Snapshot snapshot,
							   ValidateIndexState *state)
{
	elog(ERROR, "no_storage_index_validate_scan() failed on relation \"%s\"",
		 RelationGetRelationName(tableRelation));
}


/* ------------------------------------------------------------------------
 * Miscellaneous callbacks for the no_storage AM
 * ------------------------------------------------------------------------
 */

static uint64
no_storage_relation_size(Relation rel, ForkNumber forkNumber)
{
	elog(ERROR, "no_storage_relation_size() failed on relation \"%s\"",
		 RelationGetRelationName(rel));
	return 0;
}

/*
 * Check to see whether the table needs a TOAST table.
 */
static bool
no_storage_relation_needs_toast_table(Relation rel)
{
	elog(ERROR, "no_storage_relation_needs_toast_table() failed on relation \"%s\"",
		 RelationGetRelationName(rel));
	return false;
}


/* ------------------------------------------------------------------------
 * Planner related callbacks for the no_storage AM
 * ------------------------------------------------------------------------
 */

static void
no_storage_estimate_rel_size(Relation rel, int32 *attr_widths,
							 BlockNumber *pages, double *tuples,
							 double *allvisfrac)
{
	elog(ERROR, "no_storage_estimate_rel_size() failed on relation \"%s\"",
		 RelationGetRelationName(rel));
}


/* ------------------------------------------------------------------------
 * Executor related callbacks for the no_storage AM
 * ------------------------------------------------------------------------
 */

static bool
no_storage_scan_bitmap_next_block(TableScanDesc scan,
								  TBMIterateResult *tbmres)
{
	elog(ERROR, "no_storage_scan_bitmap_next_block() failed");
	return false;
}

static bool
no_storage_scan_bitmap_next_tuple(TableScanDesc scan,
								  TBMIterateResult *tbmres,
								  TupleTableSlot *slot)
{
	elog(ERROR, "no_storage_scan_bitmap_next_tuple() failed");
	return false;
}

static bool
no_storage_scan_sample_next_block(TableScanDesc scan,
								  SampleScanState *scanstate)
{
	elog(ERROR, "no_storage_scan_sample_next_block() failed");
	return false;
}

static bool
no_storage_scan_sample_next_tuple(TableScanDesc scan,
								  SampleScanState *scanstate,
								  TupleTableSlot *slot)
{
	elog(ERROR, "no_storage_scan_sample_next_tuple() failed");
	return false;
}


/* ------------------------------------------------------------------------
 * Definition of the no_storage table access method.
 * ------------------------------------------------------------------------
 */

static const TableAmRoutine no_storage_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = no_storage_slot_callbacks,

	.scan_begin = no_storage_scan_begin,
	.scan_end = no_storage_scan_end,
	.scan_rescan = no_storage_scan_rescan,
	.scan_getnextslot = no_storage_scan_getnextslot,

	/* these are common helper functions */
	.parallelscan_estimate = table_block_parallelscan_estimate,
	.parallelscan_initialize = table_block_parallelscan_initialize,
	.parallelscan_reinitialize = table_block_parallelscan_reinitialize,

	.index_fetch_begin = no_storage_index_fetch_begin,
	.index_fetch_reset = no_storage_index_fetch_reset,
	.index_fetch_end = no_storage_index_fetch_end,
	.index_fetch_tuple = no_storage_index_fetch_tuple,

	.tuple_insert = no_storage_tuple_insert,
	.tuple_insert_speculative = no_storage_tuple_insert_speculative,
	.tuple_complete_speculative = no_storage_tuple_complete_speculative,
	.multi_insert = no_storage_multi_insert,
	.tuple_delete = no_storage_tuple_delete,
	.tuple_update = no_storage_tuple_update,
	.tuple_lock = no_storage_tuple_lock,
	.finish_bulk_insert = no_storage_finish_bulk_insert,

	.tuple_fetch_row_version = no_storage_fetch_row_version,
	.tuple_get_latest_tid = no_storage_get_latest_tid,
	.tuple_tid_valid = no_storage_tuple_tid_valid,
	.tuple_satisfies_snapshot = no_storage_tuple_satisfies_snapshot,
	.index_delete_tuples = no_storage_index_delete_tuples,

	.relation_set_new_filenode = no_storage_relation_set_new_filenode,
	.relation_nontransactional_truncate = no_storage_relation_nontransactional_truncate,
	.relation_copy_data = no_storage_copy_data,
	.relation_copy_for_cluster = no_storage_copy_for_cluster,
	.relation_vacuum = no_storage_vacuum,
	.scan_analyze_next_block = no_storage_scan_analyze_next_block,
	.scan_analyze_next_tuple = no_storage_scan_analyze_next_tuple,
	.index_build_range_scan = no_storage_index_build_range_scan,
	.index_validate_scan = no_storage_index_validate_scan,

	.relation_size = no_storage_relation_size,
	.relation_needs_toast_table = no_storage_relation_needs_toast_table,

	.relation_estimate_size = no_storage_estimate_rel_size,

	.scan_bitmap_next_block = no_storage_scan_bitmap_next_block,
	.scan_bitmap_next_tuple = no_storage_scan_bitmap_next_tuple,
	.scan_sample_next_block = no_storage_scan_sample_next_block,
	.scan_sample_next_tuple = no_storage_scan_sample_next_tuple
};

Datum
no_storage_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&no_storage_methods);
}
