/*-------------------------------------------------------------------------
 *
 * tableam.h
 *	  POSTGRES table access method definitions.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/tableam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TABLEAM_H
#define TABLEAM_H

#include "access/heapam.h"
#include "access/tableam_common.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"

#define DEFAULT_TABLE_ACCESS_METHOD	"heap_tableam"

typedef union tuple_data
{
	TransactionId xid;
	CommandId	cid;
	ItemPointerData tid;
}			tuple_data;

typedef enum tuple_data_flags
{
	XMIN = 0,
	UPDATED_XID,
	CMIN,
	TID,
	CTID
}			tuple_data_flags;

/* Function pointer to let the index tuple insert from storage am */
typedef List *(*InsertIndexTuples) (TupleTableSlot *slot, EState *estate, bool noDupErr,
									bool *specConflict, List *arbiterIndexes);

/* Function pointer to let the index tuple delete from storage am */
typedef void (*DeleteIndexTuples) (Relation rel, ItemPointer tid, TransactionId old_xmin);

extern TableScanDesc table_beginscan_parallel(Relation relation, ParallelHeapScanDesc parallel_scan);
extern ParallelHeapScanDesc tableam_get_parallelheapscandesc(TableScanDesc sscan);
extern HeapPageScanDesc tableam_get_heappagescandesc(TableScanDesc sscan);
extern void table_syncscan_report_location(Relation rel, BlockNumber location);
extern void table_setscanlimits(TableScanDesc sscan, BlockNumber startBlk, BlockNumber numBlks);
extern TableScanDesc table_beginscan(Relation relation, Snapshot snapshot,
				  int nkeys, ScanKey key);
extern TableScanDesc table_beginscan_catalog(Relation relation, int nkeys, ScanKey key);
extern TableScanDesc table_beginscan_strat(Relation relation, Snapshot snapshot,
						int nkeys, ScanKey key,
						bool allow_strat, bool allow_sync);
extern TableScanDesc table_beginscan_bm(Relation relation, Snapshot snapshot,
					 int nkeys, ScanKey key);
extern TableScanDesc table_beginscan_sampling(Relation relation, Snapshot snapshot,
						   int nkeys, ScanKey key,
						   bool allow_strat, bool allow_sync, bool allow_pagemode);

extern void table_endscan(TableScanDesc scan);
extern void table_rescan(TableScanDesc scan, ScanKey key);
extern void table_rescan_set_params(TableScanDesc scan, ScanKey key,
						  bool allow_strat, bool allow_sync, bool allow_pagemode);
extern void table_scan_update_snapshot(TableScanDesc scan, Snapshot snapshot);

extern TableTuple table_scan_getnext(TableScanDesc sscan, ScanDirection direction);
extern TupleTableSlot *table_scan_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot);
extern TableTuple table_tuple_fetch_from_offset(TableScanDesc sscan, BlockNumber blkno, OffsetNumber offset);

extern void storage_get_latest_tid(Relation relation,
					   Snapshot snapshot,
					   ItemPointer tid);

extern bool table_fetch(Relation relation,
			  ItemPointer tid,
			  Snapshot snapshot,
			  TableTuple * stuple,
			  Buffer *userbuf,
			  bool keep_buf,
			  Relation stats_relation);

extern bool table_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer,
						  Snapshot snapshot, HeapTuple heapTuple,
						  bool *all_dead, bool first_call);

extern bool table_hot_search(ItemPointer tid, Relation relation, Snapshot snapshot,
				   bool *all_dead);

extern HTSU_Result table_lock_tuple(Relation relation, ItemPointer tid, Snapshot snapshot,
				   TableTuple *stuple, CommandId cid, LockTupleMode mode,
				   LockWaitPolicy wait_policy, uint8 flags,
				   HeapUpdateFailureData *hufd);

extern Oid table_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
			   int options, BulkInsertState bistate, InsertIndexTuples IndexFunc,
			   EState *estate, List *arbiterIndexes, List **recheckIndexes);

extern HTSU_Result table_delete(Relation relation, ItemPointer tid, CommandId cid,
			   Snapshot crosscheck, bool wait, DeleteIndexTuples IndexFunc,
			   HeapUpdateFailureData *hufd, bool changingPart);

extern HTSU_Result table_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
			   EState *estate, CommandId cid, Snapshot crosscheck, bool wait,
			   HeapUpdateFailureData *hufd, LockTupleMode *lockmode,
			   InsertIndexTuples IndexFunc, List **recheckIndexes);

extern void table_multi_insert(Relation relation, HeapTuple *tuples, int ntuples,
					 CommandId cid, int options, BulkInsertState bistate);

extern tuple_data table_tuple_get_data(Relation relation, TableTuple tuple, tuple_data_flags flags);

extern TableTuple table_tuple_by_datum(Relation relation, Datum data, Oid tableoid);

extern void table_get_latest_tid(Relation relation,
					   Snapshot snapshot,
					   ItemPointer tid);

extern void table_sync(Relation rel);

extern BulkInsertState table_getbulkinsertstate(Relation rel);
extern void table_freebulkinsertstate(Relation rel, BulkInsertState bistate);
extern void table_releasebulkinsertstate(Relation rel, BulkInsertState bistate);

extern RewriteState table_begin_rewrite(Relation OldHeap, Relation NewHeap,
				   TransactionId OldestXmin, TransactionId FreezeXid,
				   MultiXactId MultiXactCutoff, bool use_wal);
extern void table_end_rewrite(Relation rel, RewriteState state);
extern void table_rewrite_tuple(Relation rel, RewriteState state, HeapTuple oldTuple,
				   HeapTuple newTuple);
extern bool table_rewrite_dead_tuple(Relation rel, RewriteState state, HeapTuple oldTuple);

#endif		/* TABLEAM_H */
