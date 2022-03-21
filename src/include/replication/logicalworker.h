/*-------------------------------------------------------------------------
 *
 * logicalworker.h
 *	  Exports for logical replication workers.
 *
 * Portions Copyright (c) 2016-2022, PostgreSQL Global Development Group
 *
 * src/include/replication/logicalworker.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICALWORKER_H
#define LOGICALWORKER_H

/*
 * Counters for cumulative transaction statistics of
 * apply commits and rollbacks.
 */
typedef struct SubscriptionXactStats
{
	int64		apply_commit_count;
	int64		apply_rollback_count;
}			SubscriptionXactStats;

extern void ApplyWorkerMain(Datum main_arg);

extern bool IsLogicalWorker(void);

#endif							/* LOGICALWORKER_H */
