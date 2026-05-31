/*-------------------------------------------------------------------------
 *
 * parallel_index_build.h
 *	  Shared infrastructure for parallel index builds.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/parallel_index_build.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARALLEL_INDEX_BUILD_H
#define PARALLEL_INDEX_BUILD_H

#include "access/parallel.h"
#include "access/relscan.h"
#include "storage/condition_variable.h"
#include "storage/lockdefs.h"
#include "storage/spin.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

struct Instrumentation;

/*
 * Shared state common to all parallel index builds.
 *
 * Access methods embed this as the first member of their own shared state
 * and append any AM-specific fields, followed by the ParallelTableScanDescData
 * for the heap scan.  The immutable fields are set once by the leader before
 * workers are  launched; the mutable fields are maintained by the workers
 * under mutex and read back by the leader once all workers have signalled
 * completion.
 */
typedef struct ParallelIndexBuildShared
{
	/* Immutable state, set by the leader before launching workers */
	Oid			heaprelid;
	Oid			indexrelid;
	bool		isconcurrent;
	int			scantuplesortstates;

	/* Query ID, for report in worker processes */
	int64		queryid;

	/*
	 * workersdonecv is used to monitor the progress of workers.  All parallel
	 * participants must indicate that they are done before the leader can use
	 * the results built by the workers.
	 */
	ConditionVariable workersdonecv;

	/*
	 * mutex protects the mutable fields below.
	 */
	slock_t		mutex;

	/*
	 * Mutable state that is maintained by workers for all index types, and
	 * reported back to leader at end of the scans.
	 *
	 * nparticipantsdone is the number of worker processes finished.
	 *
	 * reltuples is the total number of input heap tuples.
	 *
	 * indtuples is the total number of tuples that made it into the index.
	 */
	int			nparticipantsdone;
	double		reltuples;
	double		indtuples;

	/*
	 * Mutable state that is maintained for exact index AMs (e.g. nbtree), and
	 * unused for lossy AMs.
	 *
	 * havedead indicates if RECENTLY_DEAD tuples were encountered during
	 * build.
	 *
	 * brokenhotchain indicates if any worker detected a broken HOT chain
	 * during build.
	 */
	bool		havedead;
	bool		brokenhotchain;
}			ParallelIndexBuildShared;

/* Leader-side helpers */
extern ParallelContext *ParallelIndexBuildCreateContext(const char *worker_function,
														int nworkers);
extern Snapshot ParallelIndexBuildGetSnapshot(bool isconcurrent);
extern Size ParallelIndexBuildEstimateShared(Relation heap, Snapshot snapshot,
											 Size am_shared_size);
extern void ParallelIndexBuildInitShared(ParallelIndexBuildShared * shared,
										 Relation heap, Relation index,
										 bool isconcurrent,
										 int scantuplesortstates,
										 ParallelTableScanDesc pscan,
										 Snapshot snapshot);
extern void ParallelIndexBuildWaitForWorkers(ParallelIndexBuildShared * shared,
											 int nparticipants,
											 double *reltuples, double *indtuples,
											 bool *havedead, bool *brokenhotchain);
extern void ParallelIndexBuildEnd(ParallelContext *pcxt,
								  struct Instrumentation *instr,
								  Snapshot snapshot);

/* Worker-side helpers */
extern void ParallelIndexBuildReportScanDone(ParallelIndexBuildShared * shared,
											 double reltuples, double indtuples,
											 bool havedead, bool brokenhotchain);
extern void ParallelIndexBuildOpenRelations(ParallelIndexBuildShared * shared,
											Relation *heapRel, Relation *indexRel,
											LOCKMODE *heapLockmode,
											LOCKMODE *indexLockmode);
extern void ParallelIndexBuildCloseRelations(Relation heapRel, Relation indexRel,
											 LOCKMODE heapLockmode,
											 LOCKMODE indexLockmode);

#endif							/* PARALLEL_INDEX_BUILD_H */
