/*---------------------------------------------------------------------
 *
 * storageamapi.h
 *		API for Postgres storage access methods
 *
 * Copyright (c) 2016, PostgreSQL Global Development Group
 *
 * src/include/access/storageamapi.h
 *---------------------------------------------------------------------
 */
#ifndef STORAGEAMAPI_H
#define STORAGEAMAPI_H

#include "access/htup.h"
#include "access/heapam.h"
#include "access/storageam.h"
#include "access/sdir.h"
#include "access/skey.h"
#include "executor/tuptable.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

typedef StorageScanDesc (*scan_begin_hook) (Relation relation,
										Snapshot snapshot,
										int nkeys, ScanKey key,
										ParallelHeapScanDesc parallel_scan,
										bool allow_strat,
										bool allow_sync,
										bool allow_pagemode,
										bool is_bitmapscan,
										bool is_samplescan,
										bool temp_snap);
typedef ParallelHeapScanDesc (*scan_get_parallelheapscandesc_hook) (StorageScanDesc scan);
typedef HeapPageScanDesc (*scan_get_heappagescandesc_hook) (StorageScanDesc scan);
typedef void (*scan_setlimits_hook) (StorageScanDesc sscan, BlockNumber startBlk, BlockNumber numBlks);

/* must return a TupleTableSlot? */
typedef StorageTuple (*scan_getnext_hook) (StorageScanDesc scan,
											ScanDirection direction);

typedef TupleTableSlot* (*scan_getnext_slot_hook) (StorageScanDesc scan,
										ScanDirection direction, TupleTableSlot *slot);
typedef StorageTuple (*scan_fetch_tuple_from_offset_hook) (StorageScanDesc scan,
							BlockNumber blkno, OffsetNumber offset);

typedef void (*scan_end_hook) (StorageScanDesc scan);


typedef void (*scan_getpage_hook) (StorageScanDesc scan, BlockNumber page);
typedef void (*scan_rescan_hook) (StorageScanDesc scan, ScanKey key, bool set_params,
					bool allow_strat, bool allow_sync, bool allow_pagemode);
typedef void (*scan_update_snapshot_hook) (StorageScanDesc scan, Snapshot snapshot);

typedef bool (*hot_search_buffer_hook) (ItemPointer tid, Relation relation,
					   Buffer buffer, Snapshot snapshot, HeapTuple heapTuple,
					   bool *all_dead, bool first_call);

typedef Oid (*tuple_insert_hook) (Relation relation,
									  TupleTableSlot *tupslot,
									  CommandId cid,
									  int options,
									  BulkInsertState bistate);

typedef HTSU_Result (*tuple_delete_hook) (Relation relation,
											  ItemPointer tid,
											  CommandId cid,
											  Snapshot crosscheck,
											  bool wait,
											  HeapUpdateFailureData *hufd);

typedef HTSU_Result (*tuple_update_hook) (Relation relation,
											  ItemPointer otid,
											  TupleTableSlot *slot,
											  CommandId cid,
											  Snapshot crosscheck,
											  bool wait,
											  HeapUpdateFailureData *hufd,
											  LockTupleMode *lockmode);

typedef bool (*tuple_fetch_hook) (Relation relation,
									ItemPointer tid,
									Snapshot snapshot,
									StorageTuple *tuple,
									Buffer *userbuf,
									bool keep_buf,
									Relation stats_relation);

typedef HTSU_Result (*tuple_lock_hook) (Relation relation,
											ItemPointer tid,
											StorageTuple *tuple,
											CommandId cid,
											LockTupleMode mode,
											LockWaitPolicy wait_policy,
											bool follow_update,
											Buffer *buffer,
											HeapUpdateFailureData *hufd);

typedef void (*multi_insert_hook) (Relation relation, HeapTuple *tuples, int ntuples,
				  CommandId cid, int options, BulkInsertState bistate);

typedef bool (*tuple_freeze_hook) (HeapTupleHeader tuple, TransactionId cutoff_xid,
				  TransactionId cutoff_multi);

typedef void (*tuple_get_latest_tid_hook) (Relation relation,
											Snapshot snapshot,
											ItemPointer tid);

typedef tuple_data (*get_tuple_data_hook) (StorageTuple tuple, tuple_data_flags flags);

typedef StorageTuple (*tuple_from_datum_hook) (Datum data, Oid tableoid);

typedef bool (*tuple_is_heaponly_hook) (StorageTuple tuple);

typedef void (*slot_store_tuple_hook) (TupleTableSlot *slot,
										   StorageTuple tuple,
										   bool shouldFree,
										   bool minumumtuple);
typedef void (*slot_clear_tuple_hook) (TupleTableSlot *slot);
typedef Datum (*slot_getattr_hook) (TupleTableSlot *slot,
										int attnum, bool *isnull);
typedef void (*slot_virtualize_tuple_hook) (TupleTableSlot *slot, int16 upto);

typedef HeapTuple (*slot_tuple_hook) (TupleTableSlot *slot, bool palloc_copy);
typedef MinimalTuple (*slot_min_tuple_hook) (TupleTableSlot *slot, bool palloc_copy);

typedef void (*slot_update_tableoid_hook) (TupleTableSlot *slot, Oid tableoid);

typedef void (*speculative_finish_hook) (Relation rel,
											 TupleTableSlot *slot);
typedef void (*speculative_abort_hook) (Relation rel,
											TupleTableSlot *slot);

typedef void (*relation_sync_hook) (Relation relation);

typedef bool (*snapshot_satisfies_hook) (StorageTuple htup, Snapshot snapshot, Buffer buffer);
typedef HTSU_Result (*snapshot_satisfies_update_hook) (StorageTuple htup, CommandId curcid, Buffer buffer);
typedef HTSV_Result (*snapshot_satisfies_vacuum_hook) (StorageTuple htup, TransactionId OldestXmin, Buffer buffer);

typedef struct StorageSlotAmRoutine
{
	/* Operations on TupleTableSlot */
	slot_store_tuple_hook	slot_store_tuple;
	slot_virtualize_tuple_hook	slot_virtualize_tuple;
	slot_clear_tuple_hook	slot_clear_tuple;
	slot_getattr_hook	slot_getattr;
	slot_tuple_hook     slot_tuple;
	slot_min_tuple_hook      slot_min_tuple;
	slot_update_tableoid_hook slot_update_tableoid;
} StorageSlotAmRoutine;

typedef StorageSlotAmRoutine* (*slot_storageam_hook) (void);

/*
 * API struct for a storage AM.  Note this must be stored in a single palloc'd
 * chunk of memory.
 *
 * XXX currently all functions are together in a single struct.  Would it be
 * worthwhile to split the slot-accessor functions to a different struct?
 * That way, MinimalTuple could be handled without a complete StorageAmRoutine
 * for them -- it'd only have a few functions in TupleTableSlotAmRoutine or so.
 */
typedef struct StorageAmRoutine
{
	NodeTag		type;

	/* Operations on relation scans */
	scan_begin_hook scan_begin;
	scan_get_parallelheapscandesc_hook scan_get_parallelheapscandesc;
	scan_get_heappagescandesc_hook scan_get_heappagescandesc;
	scan_setlimits_hook scansetlimits;
	scan_getnext_hook scan_getnext;
	scan_getnext_slot_hook scan_getnextslot;
	scan_fetch_tuple_from_offset_hook scan_fetch_tuple_from_offset;
	scan_end_hook scan_end;
	scan_getpage_hook scan_getpage;
	scan_rescan_hook scan_rescan;
	scan_update_snapshot_hook scan_update_snapshot;
	hot_search_buffer_hook hot_search_buffer; /* heap_hot_search_buffer */

	// heap_sync_function		heap_sync;		/* heap_sync */
	/* not implemented */
	//	parallelscan_estimate_function	parallelscan_estimate;	/* heap_parallelscan_estimate */
	//	parallelscan_initialize_function parallelscan_initialize;	/* heap_parallelscan_initialize */
	//	parallelscan_begin_function	parallelscan_begin;	/* heap_beginscan_parallel */

	/* Operations on physical tuples */
	tuple_insert_hook		tuple_insert;	/* heap_insert */
	tuple_update_hook		tuple_update;	/* heap_update */
	tuple_delete_hook		tuple_delete;	/* heap_delete */
	tuple_fetch_hook		tuple_fetch;	/* heap_fetch */
	tuple_lock_hook			tuple_lock;		/* heap_lock_tuple */
	multi_insert_hook		multi_insert;	/* heap_multi_insert */
	tuple_freeze_hook       tuple_freeze;	/* heap_freeze_tuple */
	tuple_get_latest_tid_hook tuple_get_latest_tid; /* heap_get_latest_tid */

	get_tuple_data_hook		get_tuple_data;
	tuple_is_heaponly_hook	tuple_is_heaponly;
	tuple_from_datum_hook	tuple_from_datum;


	slot_storageam_hook	slot_storageam;

	/*
	 * Speculative insertion support operations
	 *
	 * Setting a tuple's speculative token is a slot-only operation, so no need
	 * for a storage AM method, but after inserting a tuple containing a
	 * speculative token, the insertion must be completed by these routines:
	 */
	speculative_finish_hook	speculative_finish;
	speculative_abort_hook	speculative_abort;


	relation_sync_hook	relation_sync;	/* heap_sync */

	snapshot_satisfies_hook snapshot_satisfies[END_OF_VISIBILITY];
	snapshot_satisfies_update_hook snapshot_satisfiesUpdate; /* HeapTupleSatisfiesUpdate */
	snapshot_satisfies_vacuum_hook snapshot_satisfiesVacuum; /* HeapTupleSatisfiesVacuum */
} StorageAmRoutine;

extern StorageAmRoutine *GetStorageAmRoutine(Oid amhandler);
extern StorageAmRoutine *GetStorageAmRoutineByAmId(Oid amoid);
extern StorageAmRoutine *GetHeapamStorageAmRoutine(void);

#endif		/* STORAGEAMAPI_H */
