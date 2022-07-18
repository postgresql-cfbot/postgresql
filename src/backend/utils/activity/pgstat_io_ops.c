/* -------------------------------------------------------------------------
 *
 * pgstat_io_ops.c
 *	  Implementation of IO operation statistics.
 *
 * This file contains the implementation of IO operation statistics. It is kept
 * separate from pgstat.c to enforce the line between the statistics access /
 * storage implementation and the details about individual types of
 * statistics.
 *
 * Copyright (c) 2001-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_io_ops.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/pgstat_internal.h"

static PgStat_IOPathOps pending_IOOpStats;
bool have_ioopstats = false;


/*
 * Flush out locally pending IO Operation statistics entries
 *
 * If nowait is true, this function returns false on lock failure. Otherwise
 * this function always returns true.
 *
 * If nowait is true, this function returns true if the lock could not be
 * acquired. Otherwise return false.
 */
bool
pgstat_flush_io_ops(bool nowait)
{
	PgStatShared_IOPathOps *stats_shmem;

	if (!have_ioopstats)
		return false;


	stats_shmem =
		&pgStatLocal.shmem->io_ops.stats[MyBackendType];

	if (!nowait)
		LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(&stats_shmem->lock, LW_EXCLUSIVE))
		return true;


	for (int i = 0; i < IOPATH_NUM_TYPES; i++)
	{
		PgStat_IOOpCounters *sharedent = &stats_shmem->data[i];
		PgStat_IOOpCounters *pendingent = &pending_IOOpStats.data[i];

#define IO_OP_ACC(fld) sharedent->fld += pendingent->fld
		IO_OP_ACC(allocs);
		IO_OP_ACC(extends);
		IO_OP_ACC(fsyncs);
		IO_OP_ACC(reads);
		IO_OP_ACC(writes);
#undef IO_OP_ACC
	}

	LWLockRelease(&stats_shmem->lock);

	memset(&pending_IOOpStats, 0, sizeof(pending_IOOpStats));

	have_ioopstats = false;

	return false;
}

void
pgstat_io_ops_snapshot_cb(void)
{
	PgStatShared_BackendIOPathOps *all_backend_stats_shmem = &pgStatLocal.shmem->io_ops;
	PgStat_BackendIOPathOps *all_backend_stats_snap = &pgStatLocal.snapshot.io_ops;

	for (int i = 0; i < BACKEND_NUM_TYPES; i++)
	{
		PgStatShared_IOPathOps *stats_shmem = &all_backend_stats_shmem->stats[i];
		PgStat_IOPathOps *stats_snap = &all_backend_stats_snap->stats[i];

		LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);
		/*
		 * Use the lock in the first BackendType's PgStat_IOPathOps to protect the
		 * reset timestamp as well.
		 */
		if (i == 0)
			all_backend_stats_snap->stat_reset_timestamp = all_backend_stats_shmem->stat_reset_timestamp;

		memcpy(stats_snap->data, stats_shmem->data, sizeof(stats_shmem->data));
		LWLockRelease(&stats_shmem->lock);
	}

}

void
pgstat_io_ops_reset_all_cb(TimestampTz ts)
{
	PgStatShared_BackendIOPathOps *all_backend_stats_shmem = &pgStatLocal.shmem->io_ops;

	for (int i = 0; i < BACKEND_NUM_TYPES; i++)
	{
		PgStatShared_IOPathOps *stats_shmem = &all_backend_stats_shmem->stats[i];

		LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);

		/*
		 * Use the lock in the first BackendType's PgStat_IOPathOps to protect the
		 * reset timestamp as well.
		 */
		if (i == 0)
			all_backend_stats_shmem->stat_reset_timestamp = ts;

		memset(stats_shmem->data, 0, sizeof(stats_shmem->data));
		LWLockRelease(&stats_shmem->lock);
	}
}

void
pgstat_count_io_op(IOOp io_op, IOPath io_path)
{
	PgStat_IOOpCounters *pending_counters = &pending_IOOpStats.data[io_path];

	switch (io_op)
	{
		case IOOP_ALLOC:
			pending_counters->allocs++;
			break;
		case IOOP_EXTEND:
			pending_counters->extends++;
			break;
		case IOOP_FSYNC:
			pending_counters->fsyncs++;
			break;
		case IOOP_READ:
			pending_counters->reads++;
			break;
		case IOOP_WRITE:
			pending_counters->writes++;
			break;
	}

	have_ioopstats = true;
}

PgStat_BackendIOPathOps*
pgstat_fetch_backend_io_path_ops(void)
{
	pgstat_snapshot_fixed(PGSTAT_KIND_IOOPS);

	return &pgStatLocal.snapshot.io_ops;
}

const char *
pgstat_io_path_desc(IOPath io_path)
{
	switch (io_path)
	{
		case IOPATH_LOCAL:
			return "Local";
		case IOPATH_SHARED:
			return "Shared";
		case IOPATH_STRATEGY:
			return "Strategy";
	}

	elog(ERROR, "unrecognized IOPath value: %d", io_path);
}

const char *
pgstat_io_op_desc(IOOp io_op)
{
	switch (io_op)
	{
		case IOOP_ALLOC:
			return "Alloc";
		case IOOP_EXTEND:
			return "Extend";
		case IOOP_FSYNC:
			return "Fsync";
		case IOOP_READ:
			return "Read";
		case IOOP_WRITE:
			return "Write";
	}

	elog(ERROR, "unrecognized IOOp value: %d", io_op);
}
