/*-------------------------------------------------------------------------
 *
 * relscan.h
 *	  POSTGRES relation scan descriptor definitions.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/relscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELSCAN_H
#define RELSCAN_H

#include "access/htup_details.h"
#include "access/itup.h"
#include "nodes/tidbitmap.h"
#include "port/atomics.h"
#include "storage/relfilelocator.h"
#include "storage/spin.h"
#include "utils/relcache.h"


struct ParallelTableScanDescData;
struct TableScanInstrumentation;

/*
 * Generic descriptor for table scans. This is the base-class for table scans,
 * which needs to be embedded in the scans of individual AMs.
 */
typedef struct TableScanDescData
{
	/* scan parameters */
	Relation	rs_rd;			/* heap relation descriptor */
	struct SnapshotData *rs_snapshot;	/* snapshot to see */
	int			rs_nkeys;		/* number of scan keys */
	struct ScanKeyData *rs_key; /* array of scan key descriptors */

	/*
	 * Scan type-specific members
	 */
	union
	{
		/* Iterator for Bitmap Table Scans */
		TBMIterator rs_tbmiterator;

		/*
		 * Range of ItemPointers for table_scan_getnextslot_tidrange() to
		 * scan.
		 */
		struct
		{
			ItemPointerData rs_mintid;
			ItemPointerData rs_maxtid;
		}			tidrange;
	}			st;

	/*
	 * Information about type and behaviour of the scan, a bitmask of members
	 * of the ScanOptions enum (see tableam.h).
	 */
	uint32		rs_flags;

	struct ParallelTableScanDescData *rs_parallel;	/* parallel scan
													 * information */

	/*
	 * Instrumentation counters maintained by all table AMs.
	 */
	struct TableScanInstrumentation *rs_instrument;
} TableScanDescData;
typedef struct TableScanDescData *TableScanDesc;

/*
 * Shared state for parallel table scan.
 *
 * Each backend participating in a parallel table scan has its own
 * TableScanDesc in backend-private memory, and those objects all contain a
 * pointer to this structure.  The information here must be sufficient to
 * properly initialize each new TableScanDesc as workers join the scan, and it
 * must act as a information what to scan for those workers.
 */
typedef struct ParallelTableScanDescData
{
	RelFileLocator phs_locator; /* physical relation to scan */
	bool		phs_syncscan;	/* report location to syncscan logic? */
	bool		phs_snapshot_any;	/* SnapshotAny, not phs_snapshot_data? */
	Size		phs_snapshot_off;	/* data for snapshot */
} ParallelTableScanDescData;
typedef struct ParallelTableScanDescData *ParallelTableScanDesc;

/*
 * Shared state for parallel table scans, for block oriented storage.
 */
typedef struct ParallelBlockTableScanDescData
{
	ParallelTableScanDescData base;

	BlockNumber phs_nblocks;	/* # blocks in relation at start of scan */
	slock_t		phs_mutex;		/* mutual exclusion for setting startblock */
	BlockNumber phs_startblock; /* starting block number */
	BlockNumber phs_numblock;	/* # blocks to scan, or InvalidBlockNumber if
								 * no limit */
	pg_atomic_uint64 phs_nallocated;	/* number of blocks allocated to
										 * workers so far. */
}			ParallelBlockTableScanDescData;
typedef struct ParallelBlockTableScanDescData *ParallelBlockTableScanDesc;

/*
 * Per backend state for parallel table scan, for block-oriented storage.
 */
typedef struct ParallelBlockTableScanWorkerData
{
	uint64		phsw_nallocated;	/* Current # of blocks into the scan */
	uint32		phsw_chunk_remaining;	/* # blocks left in this chunk */
	uint32		phsw_chunk_size;	/* The number of blocks to allocate in
									 * each I/O chunk for the scan */
} ParallelBlockTableScanWorkerData;
typedef struct ParallelBlockTableScanWorkerData *ParallelBlockTableScanWorker;

/*
 * Base class for fetches from a table via an index. This is the base-class
 * for such scans, which needs to be embedded in the respective struct for
 * individual AMs.
 */
typedef struct IndexFetchTableData
{
	Relation	rel;

	/*
	 * Bitmask of ScanOptions affecting the relation. No SO_INTERNAL_FLAGS are
	 * permitted.
	 */
	uint32		flags;

	/*
	 * Side channel for table AMs whose update chains can reach a different
	 * set of index-key values than the arriving index entry recorded (heap's
	 * HOT-selectively-updated chains).  Set true by the table AM when the
	 * walk to the live tuple crossed a HOT/SIU hop after the entry's own
	 * tuple, meaning the arriving entry's stored key may no longer match the
	 * live tuple and the index-access layer must recheck it.  Left false when
	 * no such hop was crossed (the entry is definitely current), and always
	 * false for AMs without such chains.
	 */
	bool		xs_hot_indexed_recheck;

	/*
	 * Companion to xs_hot_indexed_recheck.  xs_hot_indexed_crossed is the
	 * union of the per-hop modified-attrs bitmaps the walk crossed after the
	 * entry's own tuple, over heap attribute numbers (bit attnum-1 for a
	 * 1-based attnum).  The index-access layer tests it against the arriving
	 * index's key columns to judge staleness without a key comparison: any
	 * overlap means a crossed hop changed one of the index's inputs, so the
	 * entry is stale.  The union is complete (every crossed live hop and
	 * collapse-survivor stub contributes its bitmap, and collapse only
	 * reclaims members subsumed by surviving hops), so disjointness reliably
	 * means fresh.  It is NULL for AMs without such chains and is sized by
	 * the table AM for the heap relation's column count.
	 */
	uint8	   *xs_hot_indexed_crossed;

	/*
	 * Set by the table AM when it returns a tuple: true iff every chain
	 * member the walk skipped before reaching the returned (visible) tuple is
	 * dead to all transactions (below the global xmin horizon).  Combined
	 * with a stale verdict (the crossed-attribute bitmap overlapped the
	 * index's key columns), this lets the index-access layer
	 * kill the arriving leaf: no snapshot can reach a matching version
	 * through it, so it is redundant.  AMs without such chains leave it
	 * false.
	 */
	bool		xs_prefix_all_dead;
} IndexFetchTableData;

struct IndexScanInstrumentation;

/*
 * We use the same IndexScanDescData structure for both amgettuple-based
 * and amgetbitmap-based index scans.  Some fields are only relevant in
 * amgettuple-based scans.
 */
typedef struct IndexScanDescData
{
	/* scan parameters */
	Relation	heapRelation;	/* heap relation descriptor, or NULL */
	Relation	indexRelation;	/* index relation descriptor */
	struct SnapshotData *xs_snapshot;	/* snapshot to see */
	int			numberOfKeys;	/* number of index qualifier conditions */
	int			numberOfOrderBys;	/* number of ordering operators */
	struct ScanKeyData *keyData;	/* array of index qualifier descriptors */
	struct ScanKeyData *orderByData;	/* array of ordering op descriptors */
	bool		xs_want_itup;	/* caller requests index tuples */
	bool		xs_index_only;	/* caller is an index-only scan that may
								 * return tuples without fetching the heap;
								 * AMs must retain leaf-page pins for such
								 * scans (VM all-visible / TID-recycle race),
								 * whereas a plain scan that sets xs_want_itup
								 * only to inspect the index tuple still
								 * fetches the heap and may drop pins */
	bool		xs_temp_snap;	/* unregister snapshot at scan end? */

	/* signaling to index AM about killing index tuples */
	bool		kill_prior_tuple;	/* last-returned tuple is dead */
	bool		ignore_killed_tuples;	/* do not return killed entries */
	bool		xactStartedInRecovery;	/* prevents killing/seeing killed
										 * tuples */

	/* index access method's private state */
	void	   *opaque;			/* access-method-specific info */

	/*
	 * Instrumentation counters maintained by all index AMs during both
	 * amgettuple calls and amgetbitmap calls (unless field remains NULL)
	 */
	struct IndexScanInstrumentation *instrument;

	/*
	 * In an index-only scan, a successful amgettuple call must fill either
	 * xs_itup (and xs_itupdesc) or xs_hitup (and xs_hitupdesc) to provide the
	 * data returned by the scan.  It can fill both, in which case the heap
	 * format will be used.
	 */
	IndexTuple	xs_itup;		/* index tuple returned by AM */
	struct TupleDescData *xs_itupdesc;	/* rowtype descriptor of xs_itup */
	HeapTuple	xs_hitup;		/* index data returned by AM, as HeapTuple */
	struct TupleDescData *xs_hitupdesc; /* rowtype descriptor of xs_hitup */

	ItemPointerData xs_heaptid; /* result */
	bool		xs_heap_continue;	/* T if must keep walking, potential
									 * further results */
	IndexFetchTableData *xs_heapfetch;

	bool		xs_recheck;		/* T means scan keys must be rechecked */

	/*
	 * T means the index entry that reached xs_heaptid is stale: the HOT chain
	 * walked to reach the tuple crossed a HOT-selectively-updated (HOT/SIU)
	 * hop that changed an attribute this index covers, so the arriving
	 * entry's stored key no longer matches the live tuple.  The executor
	 * drops such a tuple; the row is re-supplied by the fresh entry inserted
	 * for the new value.  Unlike xs_recheck (set by lossy AMs such as GiST
	 * and GIN), this is computed by the index-access layer by testing the
	 * heap AM's crossed-attribute bitmap (xs_hot_indexed_crossed) against
	 * this index's key columns: any overlap means a crossed hop changed one
	 * of the index's inputs, so the entry is stale.
	 */
	bool		xs_hot_indexed_stale;

	/*
	 * When fetching with an ordering operator, the values of the ORDER BY
	 * expressions of the last returned tuple, according to the index.  If
	 * xs_recheckorderby is true, these need to be rechecked just like the
	 * scan keys, and the values returned here are a lower-bound on the actual
	 * values.
	 */
	Datum	   *xs_orderbyvals;
	bool	   *xs_orderbynulls;
	bool		xs_recheckorderby;

	/* parallel index scan information, in shared memory */
	struct ParallelIndexScanDescData *parallel_scan;
} IndexScanDescData;

/* Generic structure for parallel scans */
typedef struct ParallelIndexScanDescData
{
	RelFileLocator ps_locator;	/* physical table relation to scan */
	RelFileLocator ps_indexlocator; /* physical index relation to scan */
	Size		ps_offset_am;	/* Offset to am-specific structure */
	char		ps_snapshot_data[FLEXIBLE_ARRAY_MEMBER];
}			ParallelIndexScanDescData;

struct TupleTableSlot;

/* Struct for storage-or-index scans of system tables */
typedef struct SysScanDescData
{
	Relation	heap_rel;		/* catalog being scanned */
	Relation	irel;			/* NULL if doing heap scan */
	struct TableScanDescData *scan; /* only valid in storage-scan case */
	struct IndexScanDescData *iscan;	/* only valid in index-scan case */
	struct SnapshotData *snapshot;	/* snapshot to unregister at end of scan */
	struct TupleTableSlot *slot;
} SysScanDescData;

#endif							/* RELSCAN_H */
