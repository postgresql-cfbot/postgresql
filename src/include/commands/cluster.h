/*-------------------------------------------------------------------------
 *
 * cluster.h
 *	  header file for postgres cluster command stuff
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * src/include/commands/cluster.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_H
#define CLUSTER_H

#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "parser/parse_node.h"
#include "replication/logical.h"
#include "storage/lock.h"
#include "storage/relfilelocator.h"
#include "utils/relcache.h"
#include "utils/resowner.h"
#include "utils/tuplestore.h"


/* flag bits for ClusterParams->options */
#define CLUOPT_VERBOSE 0x01		/* print progress info */
#define CLUOPT_RECHECK 0x02		/* recheck relation state */
#define CLUOPT_RECHECK_ISCLUSTERED 0x04 /* recheck relation state for
										 * indisclustered */
#define CLUOPT_CONCURRENT 0x08	/* allow concurrent data changes */

/* options for CLUSTER */
typedef struct ClusterParams
{
	bits32		options;		/* bitmask of CLUOPT_* */
} ClusterParams;

/*
 * The following definitions are used for concurrent processing.
 */

extern RelFileLocator	clustered_rel_locator;
extern RelFileLocator	clustered_rel_toast_locator;

extern PGDLLIMPORT int	cluster_max_xlock_time;

/*
 * Lock level for the concurrent variant of CLUSTER / VACUUM FULL.
 *
 * Like for lazy VACUUM, we choose the strongest lock that still allows
 * INSERT, UPDATE and DELETE.
 *
 * Note that the lock needs to be released temporarily a few times during the
 * processing. In such cases it should be checked after re-locking that the
 * relation / index hasn't changed in the system catalog while the lock was
 * not held.
 */
#define LOCK_CLUSTER_CONCURRENT	ShareUpdateExclusiveLock

typedef enum
{
	CHANGE_INSERT,
	CHANGE_UPDATE_OLD,
	CHANGE_UPDATE_NEW,
	CHANGE_DELETE,
	CHANGE_TRUNCATE
} ConcurrentChangeKind;

typedef struct ConcurrentChange
{
	/* See the enum above. */
	ConcurrentChangeKind kind;

	/* Transaction that changes the data. */
	TransactionId	xid;

	/* For UPDATE / DELETE, the location of the old tuple version. */
	ItemPointerData	old_tid;

	/*
	 * Historic catalog snapshot that was used to decode this change.
	 */
	Snapshot	snapshot;

	/*
	 * The actual tuple.
	 *
	 * The tuple data follows the ConcurrentChange structure. Before use make
	 * sure the tuple is correctly aligned (ConcurrentChange can be stored as
	 * bytea) and that tuple->t_data is fixed.
	 */
	HeapTupleData tup_data;
} ConcurrentChange;

/*
 * Logical decoding state.
 *
 * Here we store the data changes that we decode from WAL while the table
 * contents is being copied to a new storage. Also the necessary metadata
 * needed to apply these changes to the table is stored here.
 */
typedef struct ClusterDecodingState
{
	/* The relation whose changes we're decoding. */
	Oid			relid;

	/*
	 * Decoded changes are stored here. Although we try to avoid excessive
	 * batches, it can happen that the changes need to be stored to disk. The
	 * tuplestore does this transparently.
	 */
	Tuplestorestate *tstore;
	/* XID of the last change added to tstore. */
	TransactionId	last_change_xid	PG_USED_FOR_ASSERTS_ONLY;

	/* The current number of changes in tstore. */
	double		nchanges;

	/*
	 * Descriptor to store the ConcurrentChange structure serialized (bytea).
	 * We can't store the tuple directly because tuplestore only supports
	 * minimum tuple and we may need to transfer OID system column from the
	 * output plugin. Also we need to transfer the change kind, so it's better
	 * to put everything in the structure than to use 2 tuplestores "in
	 * parallel".
	 */
	TupleDesc	tupdesc_change;

	/* Tuple descriptor needed to update indexes. */
	TupleDesc	tupdesc;

	/* Slot to retrieve data from tstore. */
	TupleTableSlot *tsslot;

	/*
	 * Historic catalog snapshot that was used to decode the most recent
	 * change.
	 */
	Snapshot	snapshot;
	/* LSN of the record  */
	XLogRecPtr	snapshot_lsn;

	ResourceOwner resowner;
} ClusterDecodingState;

extern void cluster(ParseState *pstate, ClusterStmt *stmt, bool isTopLevel);
extern void cluster_rel(Relation OldHeap, Oid indexOid,	ClusterParams *params,
						bool isTopLevel);
extern void check_index_is_clusterable(Relation OldHeap, Oid indexOid,
									   LOCKMODE lockmode);
extern void mark_index_clustered(Relation rel, Oid indexOid, bool is_internal);
extern bool check_relation_is_clusterable_concurrently(Relation rel, int elevel,
													   const char *stmt);
extern void cluster_decode_concurrent_changes(LogicalDecodingContext *ctx,
											  XLogRecPtr end_of_wal,
											  struct timeval *must_complete);
extern Oid	make_new_heap(Oid OIDOldHeap, Oid NewTableSpace, Oid NewAccessMethod,
						  char relpersistence, LOCKMODE lockmode_old,
						  LOCKMODE *lockmode_new_p);
extern void finish_heap_swap(Oid OIDOldHeap, Oid OIDNewHeap,
							 bool is_system_catalog,
							 bool swap_toast_by_content,
							 bool check_constraints,
							 bool is_internal,
							 bool reindex,
							 TransactionId frozenXid,
							 MultiXactId cutoffMulti,
							 char newrelpersistence);

extern Size ClusterShmemSize(void);
extern void ClusterShmemInit(void);
extern bool is_concurrent_cluster_in_progress(Oid relid);
extern void check_for_concurrent_cluster(Oid relid, LOCKMODE lockmode);
#endif							/* CLUSTER_H */
