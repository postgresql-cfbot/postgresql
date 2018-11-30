/*-------------------------------------------------------------------------
 *
 * vacuum_internal.h
 *	  Internal declarations for lazy vacuum
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/vacuum_internal.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef VACUUM_INTERNAL_H
#define VACUUM_INTERNAL_H

/* DSM key for parallel lazy vacuum */
#define VACUUM_KEY_SHARED			UINT64CONST(0xFFFFFFFFFFF00001)
#define VACUUM_KEY_VACUUM_STATS		UINT64CONST(0xFFFFFFFFFFF00002)
#define VACUUM_KEY_INDEX_STATS	    UINT64CONST(0xFFFFFFFFFFF00003)
#define VACUUM_KEY_DEAD_TUPLES		UINT64CONST(0xFFFFFFFFFFF00004)
#define VACUUM_KEY_WORKERS			UINT64CONST(0xFFFFFFFFFFF00005)
#define VACUUM_KEY_QUERY_TEXT		UINT64CONST(0xFFFFFFFFFFF00006)

/*
 * Type definitions of lazy vacuum. The fields of these structs are
 * accessed by only the vacuum leader.
 */
typedef struct LVScanDescData LVScanDescData;
typedef struct LVScanDescData *LVScanDesc;
typedef struct LVTidMap	LVTidMap;
typedef struct LVIndStats LVIndStats;

/* Vacuum worker state for parallel lazy vacuum */
typedef enum VacWorkerSate
{
	VACSTATE_INVALID = 0,
	VACSTATE_SCAN,
	VACSTATE_VACUUM_INDEX,
	VACSTATE_VACUUM_HEAP,
	VACSTATE_CLEANUP_INDEX,
	VACSTATE_WORKER_DONE,
	VACSTATE_COMPLETED
} VacWorkerState;

/*
 * The 'pid' always starts with InvalidPid, which means the vacuum worker
 * is starting up. It's sets by the vacuum worker itself during start up. When
 * the vacuum worker exits or detaches the vacuum worker slot, 'pid' is set to 0,
 * which means the vacuum worker is dead.
 */
typedef struct VacuumWorker
{
	pid_t			pid;	/* parallel worker's pid.
							   InvalidPid = not started yet; 0 = dead */

	VacWorkerState	state;	/* current worker's state */
	slock_t			mutex;	/* protect the above fields */
} VacuumWorker;

/* Struct to control parallel vacuum workers */
typedef struct LVWorkerState
{
	int		nparticipantvacuum;		/* only parallel worker, not including
									   the leader */
	int		nparticipantvacuum_launched;	/* actual launched workers of
											   nparticipantvacuum */

	/* condition variable signaled when changing status */
	ConditionVariable	cv;

	/* protect workers array */
	LWLock				vacuumlock;

	VacuumWorker workers[FLEXIBLE_ARRAY_MEMBER];
} LVWorkerState;
#define SizeOfLVWorkerState offsetof(LVWorkerState, workers) + sizeof(VacuumWorker)

typedef struct LVRelStats
{
	/* hasindex = true means two-pass strategy; false means one-pass */
	bool		hasindex;
	/* Overall statistics about rel */
	BlockNumber old_rel_pages;		/* previous value of pg_class.relpages */
	BlockNumber rel_pages;			/* total number of pages */
	BlockNumber scanned_pages;		/* number of pages we examined */
	BlockNumber pinskipped_pages;	/* # of pages we skipped due to a pin */
	BlockNumber frozenskipped_pages;	/* # of frozen pages we skipped */
	BlockNumber tupcount_pages;		/* pages whose tuples we counted */
	BlockNumber empty_pages;		/* # of empty pages */
	BlockNumber vacuumed_pages;		/* # of pages we vacuumed */
	double		num_tuples;			/* total number of nonremoval tuples */
	double		live_tuples;		/* live tuples (reltuples estimate) */
	double		tuples_deleted;		/* tuples cleaned up by vacuum */
	double		unused_tuples;		/* unused item pointers */
	double		old_live_tuples;	/* previous value of pg_class.reltuples */
	double		new_rel_tuples;		/* new estimated total # of tuples */
	double		new_live_tuples;	/* new estimated total # of live tuples */
	double		new_dead_tuples;	/* new estimated total # of dead tuples */
	BlockNumber pages_removed;
	BlockNumber nonempty_pages; /* actually, last nonempty page + 1 */
	int			num_index_scans;
	TransactionId latestRemovedXid;
	bool		lock_waiter_detected;
} LVRelStats;

/*
 * Shared information among parallel workers.
 */
typedef struct LVShared
{
	/* Target relation's OID */
	Oid		relid;

	/* Options and thresholds used for lazy vacuum */
	VacuumOption		options;
	bool				aggressive;		/* is an aggressive vacuum? */
	bool				is_wraparound;	/* for anti-wraparound purpose? */
	int					elevel;			/* verbose logging */
	TransactionId	oldestXmin;
	TransactionId	freezeLimit;
	MultiXactId		multiXactCutoff;

	/* Vacuum delay */
	int		cost_delay;
	int		cost_limit;

	int		max_dead_tuples_per_worker;	/* Maximum tuples each worker can have */
	ParallelHeapScanDescData heapdesc;	/* for heap scan */
} LVShared;

/*
 * Working state for lazy vacuum execution. LVState is used by both vacuum
 * workers and the vacuum leader. In parallel lazy vacuum, the 'vacrelstats'
 * for vacuum worker and the 'dead_tuples' exit in shared memory in addition
 * to the three fields for parallel lazy vacuum: 'lvshared', 'indstats' and
 * 'pcxt'.
 */
typedef struct LVState
{
	/* Vacuum target relation and indexes */
	Oid			relid;
	Relation	relation;
	Relation	*indRels;
	int			nindexes;

	/* Used during scanning heap */
	IndexBulkDeleteResult	**indbulkstats;
	xl_heap_freeze_tuple	*frozen;
	BlockNumber				next_fsm_block_to_vacuum;
	BlockNumber				current_block; /* block number being scanned */
	VacuumOption			options;
	bool					is_wraparound;

	/* Scan description for lazy vacuum */
	LVScanDesc	lvscan;
	bool		aggressive;

	/* Vacuum statistics for the target table */
	LVRelStats	*vacrelstats;

	/* Dead tuple array */
	LVTidMap	*dead_tuples;

	/*
	 * The following fields are only present when a parallel lazy vacuum
	 * is performed.
	 */
	LVShared		*lvshared;	/* shared information among vacuum workers */
	LVIndStats		*indstats;	/* shared index statistics */

} LVState;

extern LVWorkerState	*WorkerState;

extern LVScanDesc lv_beginscan(Relation relation, LVShared *lvshared,
							   bool aggressive, bool disable_page_skipping);
extern void lv_endscan(LVScanDesc lvscan);
extern int do_lazy_scan_heap(LVState *lvstate, bool *isFinished);
extern void lazy_cleanup_all_indexes(LVState *lvstate);
extern void lazy_vacuum_all_indexes(LVState *lvstate);
extern void lazy_vacuum_heap(Relation onerel, LVState *lvstate);
extern void vacuum_set_xid_limits_for_worker(TransactionId oldestxmin,
											 TransactionId freezelimit,
											 MultiXactId multixactcutoff);
extern void vacuum_set_elevel_for_worker(int elevel);
extern void lazy_space_alloc(LVState *lvstate, BlockNumber relblocks);


#endif							/* VACUUM_INTERNAL_H */
