/*--------------------------------------------------------------------
 * execParallel.h
 *		POSTGRES parallel execution interface
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/executor/execParallel.h
 *--------------------------------------------------------------------
 */

#ifndef EXECPARALLEL_H
#define EXECPARALLEL_H

#include "access/parallel.h"
#include "executor/execdesc.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "utils/dsa.h"

typedef struct SharedExecutorInstrumentation SharedExecutorInstrumentation;

typedef struct ParallelExecutorInfo
{
	PlanState  *planstate;		/* plan subtree we're running in parallel */
	ParallelContext *pcxt;		/* parallel context we're using */
	BufferUsage *buffer_usage;	/* points to bufusage area in DSM */
	WalUsage   *wal_usage;		/* walusage area in DSM */
	SharedExecutorInstrumentation *instrumentation; /* optional */
	struct SharedJitInstrumentation *jit_instrumentation;	/* optional */
	dsa_area   *area;			/* points to DSA area in DSM */
	dsa_pointer param_exec;		/* serialized PARAM_EXEC parameters */
	bool		finished;		/* set true by ExecParallelFinish */
	/* These two arrays have pcxt->nworkers_launched entries: */
	shm_mq_handle **tqueue;		/* tuple queues for worker output */
	struct TupleQueueReader **reader;	/* tuple reader/writer support */
	/* Number of tuples inserted by all workers. */
	volatile pg_atomic_uint64	*processed;
} ParallelExecutorInfo;

/*
 * List the commands here for which parallel insertions are possible.
 */
typedef enum ParallelInsertCmdKind
{
	PARALLEL_INSERT_CMD_UNDEF = 0,
	PARALLEL_INSERT_CMD_CREATE_TABLE_AS
} ParallelInsertCmdKind;

/*
 * Information sent to planner to account for tuple cost calculations in
 * cost_gather for parallel insertions in commands such as CTAS.
 *
 * We need to let the planner know that there will be no tuples received by
 * Gather node if workers insert the tuples in parallel.
 */
typedef enum ParallelInsertCmdTupleCostOpt
{
	PARALLEL_INSERT_SELECT_QUERY = 1 << 0, /* turn on this before planning */
	/*
	 * Turn on this while planning for upper Gather path to ignore parallel
	 * tuple cost in cost_gather.
	 */
	PARALLEL_INSERT_CAN_IGN_TUP_COST = 1 << 1,
	/* Turn on this after the cost is ignored. */
	PARALLEL_INSERT_TUP_COST_IGNORED = 1 << 2,
	/* Turn on this in case tuple cost needs to be ignored for Append cases. */
	PARALLEL_INSERT_CAN_IGN_TUP_COST_APPEND = 1 << 3
} ParallelInsertCmdTupleCostOpt;

/*
 * For each of the command added to ParallelInsertCmdKind, add a corresponding
 * structure encompassing the information that's required to be shared across
 * different functions. The way it works is as follows: in the caller, fill in
 * the information into one of below structures based on the command kind, pass
 * the command kind and a pointer to the filled in structure as a void pointer
 * to required functions, say ExecInitParallelPlan. The called functions will
 * use command kind to dereference the void pointer to corresponding structure.
 *
 * This way, the functions that are needed for parallel insertions can be
 * generic, clean and extensible.
 */
typedef struct ParallelInsertCTASInfo
{
	IntoClause *intoclause;
	Oid objectid;
} ParallelInsertCTASInfo;

extern ParallelExecutorInfo *ExecInitParallelPlan(PlanState *planstate,
												  EState *estate, Bitmapset *sendParam, int nworkers,
												  int64 tuples_needed,
												  ParallelInsertCmdKind parallel_ins_cmd,
												  void *parallel_ins_info);
extern void ExecParallelCreateReaders(ParallelExecutorInfo *pei);
extern void ExecParallelFinish(ParallelExecutorInfo *pei);
extern void ExecParallelCleanup(ParallelExecutorInfo *pei);
extern void ExecParallelReinitialize(PlanState *planstate,
									 ParallelExecutorInfo *pei, Bitmapset *sendParam);

extern void ParallelQueryMain(dsm_segment *seg, shm_toc *toc);
extern ParallelInsertCmdKind GetParallelInsertCmdType(DestReceiver *dest);
extern void *GetParallelInsertCmdInfo(DestReceiver *dest,
									  ParallelInsertCmdKind ins_cmd);
extern bool IsParallelInsertionAllowed(ParallelInsertCmdKind ins_cmd,
									   void *ins_info);
extern void SetParallelInsertState(ParallelInsertCmdKind ins_cmd,
								   QueryDesc *queryDesc,
								   uint8 *tuple_cost_opts);
#endif							/* EXECPARALLEL_H */
