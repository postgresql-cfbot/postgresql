/*-------------------------------------------------------------------------
 *
 * vacuumworker.c
 *	  Parallel lazy vacuum worker.
 *
 * The parallel vacuum worker process is a process that helps lazy vacuums.
 * It continues to wait for its state to be changed by the vacuum leader process.
 * After finished any state it sets state as done. Normal termination is also
 * by the leader process.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/vacuumworker.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/parallel.h"
#include "access/xact.h"
#include "commands/vacuum.h"
#include "commands/vacuum_internal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"

static VacuumWorker	*MyVacuumWorker = NULL;

/* Parallel vacuum worker function prototypes */
static void lvworker_set_state(VacWorkerState new_state);
static VacWorkerState lvworker_get_state(void);
static void lvworker_mainloop(LVState *lvstate);
static void lvworker_wait_for_next_work(void);
static void lvworker_attach(void);
static void lvworker_detach(void);
static void lvworker_onexit(int code, Datum arg);

/*
 * Perform work within a launched parallel process.
 */
void
lazy_parallel_vacuum_main(dsm_segment *seg, shm_toc *toc)
{
	LVState		*lvstate = (LVState *) palloc(sizeof(LVState));
	LVShared	*lvshared;
	LVRelStats	*vacrelstats;
	char		*sharedquery;

	ereport(DEBUG1,
			(errmsg("starting parallel lazy vacuum worker")));

	/* Register the callback function */
	before_shmem_exit(lvworker_onexit, (Datum) 0);

	/* Look up worker state and attach to the vacuum worker slot */
	WorkerState = (LVWorkerState *) shm_toc_lookup(toc, VACUUM_KEY_WORKERS, false);
	lvworker_attach();

	/* Set shared state */
	lvshared = (LVShared *) shm_toc_lookup(toc, VACUUM_KEY_SHARED, false);

	/*
	 * Set debug_query_string. The debug_query_string can not be found in
	 * autovacuum case.
	 */
	sharedquery = shm_toc_lookup(toc, VACUUM_KEY_QUERY_TEXT, true);
	if (sharedquery)
	{
		debug_query_string = sharedquery;
		pgstat_report_activity(STATE_RUNNING, debug_query_string);
	}
	else
		pgstat_report_activity(STATE_RUNNING, lvshared->is_wraparound ?
							   "autovacuum: parallel worker (to prevent wraparound)" :
							   "autovacuum: parallel worker");

	/* Set individual vacuum statistics */
	vacrelstats = (LVRelStats *) shm_toc_lookup(toc, VACUUM_KEY_VACUUM_STATS, false);

	/* Set lazy vacuum state */
	lvstate->relid = lvshared->relid;
	lvstate->aggressive = lvshared->aggressive;
	lvstate->options = lvshared->options;
	lvstate->vacrelstats = vacrelstats + ParallelWorkerNumber;
	lvstate->relation = relation_open(lvstate->relid, ShareUpdateExclusiveLock);
	vac_open_indexes(lvstate->relation, RowExclusiveLock, &lvstate->nindexes,
					 &lvstate->indRels);
	lvstate->lvshared = lvshared;
	lvstate->indstats = NULL;
	lvstate->dead_tuples = NULL;

	/*
	 * Set the PROC_IN_VACUUM flag, which lets other concurrent VACUUMs know that
	 * they can ignore this one while determining their OldestXmin. Also set the
	 * PROC_VACUUM_FOR_WRAPAROUND flag. Please see the comment in vacuum_rel for
	 * details.
	 */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	MyPgXact->vacuumFlags |= PROC_IN_VACUUM;
	if (lvshared->is_wraparound)
		MyPgXact->vacuumFlags |= PROC_VACUUM_FOR_WRAPAROUND;
	LWLockRelease(ProcArrayLock);

	/* Set the space for both index statistics and dead tuples if table with index */
	if (lvstate->nindexes > 0)
	{
		LVTidMap		*dead_tuples;
		LVIndStats		*indstats;

		/* Attach shared dead tuples */
		dead_tuples = (LVTidMap *) shm_toc_lookup(toc, VACUUM_KEY_DEAD_TUPLES, false);
		lvstate->dead_tuples = dead_tuples;

		/* Attach Shared index stats */
		indstats = (LVIndStats *) shm_toc_lookup(toc, VACUUM_KEY_INDEX_STATS, false);
		lvstate->indstats = indstats;

		/* Prepare for index bulkdelete */
		lvstate->indbulkstats = (IndexBulkDeleteResult **)
			palloc0(lvstate->nindexes * sizeof(IndexBulkDeleteResult *));
	}
	else
	{
		/* Dead tuple are stored into the local memory if no indexes */
		lazy_space_alloc(lvstate, RelationGetNumberOfBlocks(lvstate->relation));
		lvstate->indstats = NULL;
	}

	/* Restore vacuum xid limits and elevel */
	vacuum_set_xid_limits_for_worker(lvshared->oldestXmin, lvshared->freezeLimit,
									 lvshared->multiXactCutoff);
	vacuum_set_elevel_for_worker(lvshared->elevel);

	pgstat_progress_start_command(PROGRESS_COMMAND_VACUUM,
								  lvshared->relid);

	/* Restore vacuum delay */
	VacuumCostDelay = lvshared->cost_delay;
	VacuumCostLimit = lvshared->cost_limit;
	VacuumCostActive = (VacuumCostDelay > 0);
	VacuumCostBalance = 0;
	VacuumPageHit = 0;
	VacuumPageMiss = 0;
	VacuumPageDirty = 0;

	/* Begin lazy heap scan */
	lvstate->lvscan = lv_beginscan(lvstate->relation, lvstate->lvshared, lvshared->aggressive,
						  (lvstate->options.flags & VACOPT_DISABLE_PAGE_SKIPPING) != 0);

	/* Prepare other fields */
	lvstate->frozen = palloc(sizeof(xl_heap_freeze_tuple) * MaxHeapTuplesPerPage);

	/* Enter the main loop */
	lvworker_mainloop(lvstate);

	/* The lazy vacuum has done, do the post-processing */
	lv_endscan(lvstate->lvscan);
	pgstat_progress_end_command();
	lvworker_detach();
	cancel_before_shmem_exit(lvworker_onexit, (Datum) 0);

	vac_close_indexes(lvstate->nindexes, lvstate->indRels, RowExclusiveLock);
	heap_close(lvstate->relation, ShareUpdateExclusiveLock);
}

/*
 * Main loop for vacuum workers.
 */
static void
lvworker_mainloop(LVState *lvstate)
{
	bool	exit = false;

	/*
	 * Loop until the leader commands it to exit.
	 */
	while (!exit)
	{
		VacWorkerState mystate;

		/* Wait for the status to be changed by the leader */
		lvworker_wait_for_next_work();

		/* Get my new state */
		mystate = lvworker_get_state();

		/* Dispatch the work according to the state */
		switch (mystate)
		{
			case VACSTATE_SCAN:
				{
					bool dummy;
					do_lazy_scan_heap(lvstate, &dummy);
					break;
				}
			case VACSTATE_VACUUM_INDEX:
				{
					lazy_vacuum_all_indexes(lvstate);
					break;
				}
			case VACSTATE_VACUUM_HEAP:
				{
					lazy_vacuum_heap(lvstate->relation, lvstate);
					break;
				}
			case VACSTATE_CLEANUP_INDEX:
				{
					lazy_cleanup_all_indexes(lvstate);
					break;
				}
			case VACSTATE_COMPLETED:
				{
					/* The leader asked us to exit */
					exit = true;
					break;
				}
			case VACSTATE_INVALID:
			case VACSTATE_WORKER_DONE:
				{
					elog(ERROR, "unexpected vacuum state %d", mystate);
					break;
				}
		}

		/* Set my state as done after finished */
		lvworker_set_state(VACSTATE_WORKER_DONE);
	}
}

/*
 * Wait for the my state to be changed by the vacuum leader.
 */
static void
lvworker_wait_for_next_work(void)
{
	VacWorkerState mystate;

	for (;;)
	{
		mystate = lvworker_get_state();

		/* Got the next valid state by the vacuum leader */
		if (mystate != VACSTATE_WORKER_DONE && mystate != VACSTATE_INVALID)
			break;

		/* Sleep until the next notification */
		ConditionVariableSleep(&WorkerState->cv, WAIT_EVENT_PARALLEL_VACUUM);
	}

	ConditionVariableCancelSleep();
}

/*
 * lvworker_get_state - get my current state
 */
static VacWorkerState
lvworker_get_state(void)
{
	VacWorkerState state;

	SpinLockAcquire(&MyVacuumWorker->mutex);
	state = MyVacuumWorker->state;
	SpinLockRelease(&MyVacuumWorker->mutex);

	return state;
}

/*
 * lvworker_set_state - set new state to my state
 */
static void
lvworker_set_state(VacWorkerState new_state)
{
	SpinLockAcquire(&MyVacuumWorker->mutex);
	MyVacuumWorker->state = new_state;
	SpinLockRelease(&MyVacuumWorker->mutex);

	ConditionVariableBroadcast(&WorkerState->cv);
}

/*
 * Clean up function for parallel vacuum worker
 */
static void
lvworker_onexit(int code, Datum arg)
{
	if (IsInParallelMode() && MyVacuumWorker)
		lvworker_detach();
}

/*
 * Detach the worker and cleanup worker information.
 */
static void
lvworker_detach(void)
{
	SpinLockAcquire(&MyVacuumWorker->mutex);
	MyVacuumWorker->state = VACSTATE_INVALID;
	MyVacuumWorker->pid = 0;	/* the worker is dead */
	SpinLockRelease(&MyVacuumWorker->mutex);

	MyVacuumWorker = NULL;
}

/*
 * Attach to a worker slot according to its ParallelWorkerNumber.
 */
static void
lvworker_attach(void)
{
	VacuumWorker *vworker;

	LWLockAcquire(&WorkerState->vacuumlock, LW_EXCLUSIVE);
	vworker = &WorkerState->workers[ParallelWorkerNumber];
	vworker->pid = MyProcPid;
	vworker->state = VACSTATE_SCAN; /* first state */
	LWLockRelease(&WorkerState->vacuumlock);

	MyVacuumWorker = vworker;
}
