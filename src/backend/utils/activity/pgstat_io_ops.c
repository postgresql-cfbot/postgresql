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
 * Copyright (c) 2021-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_io_ops.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/pgstat_internal.h"

static PgStat_IOContextOps pending_IOOpStats;
bool		have_ioopstats = false;


/*
 * Helper function to accumulate source PgStat_IOOpCounters into target
 * PgStat_IOOpCounters. If either of the passed-in PgStat_IOOpCounters are
 * members of PgStatShared_IOContextOps, the caller is responsible for ensuring
 * that the appropriate lock is held.
 */
static void
pgstat_accum_io_op(PgStat_IOOpCounters *target, PgStat_IOOpCounters *source, IOOp io_op)
{
	switch (io_op)
	{
		case IOOP_CLOCKSWEEP:
			target->clocksweeps += source->clocksweeps;
			return;
		case IOOP_EXTEND:
			target->extends += source->extends;
			return;
		case IOOP_FSYNC:
			target->fsyncs += source->fsyncs;
			return;
		case IOOP_HIT:
			target->hits += source->hits;
			return;
		case IOOP_READ:
			target->reads += source->reads;
			return;
		case IOOP_REUSE:
			target->reuses += source->reuses;
			return;
		case IOOP_WRITE:
			target->writes += source->writes;
			return;
	}

	elog(ERROR, "unrecognized IOOp value: %d", io_op);
}

void
pgstat_count_io_op(IOOp io_op, IOContext io_context)
{
	PgStat_IOOpCounters *pending_counters;

	Assert(io_context < IOCONTEXT_NUM_TYPES);
	Assert(io_op < IOOP_NUM_TYPES);
	Assert(pgstat_expect_io_op(MyBackendType, io_context, io_op));

	pending_counters = &pending_IOOpStats.data[io_context];

	switch (io_op)
	{
		case IOOP_CLOCKSWEEP:
			pending_counters->clocksweeps++;
			break;
		case IOOP_EXTEND:
			pending_counters->extends++;
			break;
		case IOOP_FSYNC:
			pending_counters->fsyncs++;
			break;
		case IOOP_HIT:
			pending_counters->hits++;
			break;
		case IOOP_READ:
			pending_counters->reads++;
			break;
		case IOOP_REUSE:
			pending_counters->reuses++;
			break;
		case IOOP_WRITE:
			pending_counters->writes++;
			break;
	}

	have_ioopstats = true;
}

PgStat_BackendIOContextOps *
pgstat_fetch_backend_io_context_ops(void)
{
	pgstat_snapshot_fixed(PGSTAT_KIND_IOOPS);

	return &pgStatLocal.snapshot.io_ops;
}

/*
 * Flush out locally pending IO Operation statistics entries
 *
 * If no stats have been recorded, this function returns false.
 *
 * If nowait is true, this function returns true if the lock could not be
 * acquired. Otherwise, return false.
 */
bool
pgstat_flush_io_ops(bool nowait)
{
	PgStatShared_IOContextOps *type_shstats;
	bool		expect_backend_stats = true;

	if (!have_ioopstats)
		return false;

	type_shstats =
		&pgStatLocal.shmem->io_ops.stats[MyBackendType];

	if (!nowait)
		LWLockAcquire(&type_shstats->lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(&type_shstats->lock, LW_EXCLUSIVE))
		return true;

	expect_backend_stats = pgstat_io_op_stats_collected(MyBackendType);

	for (int io_context = 0; io_context < IOCONTEXT_NUM_TYPES; io_context++)
	{
		PgStat_IOOpCounters *sharedent = &type_shstats->data[io_context];
		PgStat_IOOpCounters *pendingent = &pending_IOOpStats.data[io_context];

		if (!expect_backend_stats ||
			!pgstat_bktype_io_context_valid(MyBackendType, (IOContext) io_context))
		{
			pgstat_io_context_ops_assert_zero(sharedent);
			pgstat_io_context_ops_assert_zero(pendingent);
			continue;
		}

		for (int io_op = 0; io_op < IOOP_NUM_TYPES; io_op++)
		{
			if (!(pgstat_io_op_valid(MyBackendType, (IOContext) io_context,
									 (IOOp) io_op)))
			{
				pgstat_io_op_assert_zero(sharedent, (IOOp) io_op);
				pgstat_io_op_assert_zero(pendingent, (IOOp) io_op);
				continue;
			}

			pgstat_accum_io_op(sharedent, pendingent, (IOOp) io_op);
		}
	}

	LWLockRelease(&type_shstats->lock);

	memset(&pending_IOOpStats, 0, sizeof(pending_IOOpStats));

	have_ioopstats = false;

	return false;
}

const char *
pgstat_io_context_desc(IOContext io_context)
{
	switch (io_context)
	{
		case IOCONTEXT_BULKREAD:
			return "bulkread";
		case IOCONTEXT_BULKWRITE:
			return "bulkwrite";
		case IOCONTEXT_LOCAL:
			return "local";
		case IOCONTEXT_SHARED:
			return "shared";
		case IOCONTEXT_VACUUM:
			return "vacuum";
	}

	elog(ERROR, "unrecognized IOContext value: %d", io_context);
}

const char *
pgstat_io_op_desc(IOOp io_op)
{
	switch (io_op)
	{
		case IOOP_CLOCKSWEEP:
			return "clocksweep";
		case IOOP_EXTEND:
			return "extend";
		case IOOP_FSYNC:
			return "fsync";
		case IOOP_HIT:
			return "hit";
		case IOOP_READ:
			return "read";
		case IOOP_REUSE:
			return "reused";
		case IOOP_WRITE:
			return "write";
	}

	elog(ERROR, "unrecognized IOOp value: %d", io_op);
}

void
pgstat_io_ops_reset_all_cb(TimestampTz ts)
{
	PgStatShared_BackendIOContextOps *backends_stats_shmem = &pgStatLocal.shmem->io_ops;

	for (int i = 0; i < BACKEND_NUM_TYPES; i++)
	{
		PgStatShared_IOContextOps *stats_shmem = &backends_stats_shmem->stats[i];

		LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);

		/*
		 * Use the lock in the first BackendType's PgStat_IOContextOps to
		 * protect the reset timestamp as well.
		 */
		if (i == 0)
			backends_stats_shmem->stat_reset_timestamp = ts;

		memset(stats_shmem->data, 0, sizeof(stats_shmem->data));
		LWLockRelease(&stats_shmem->lock);
	}
}

void
pgstat_io_ops_snapshot_cb(void)
{
	PgStatShared_BackendIOContextOps *backends_stats_shmem = &pgStatLocal.shmem->io_ops;
	PgStat_BackendIOContextOps *backends_stats_snap = &pgStatLocal.snapshot.io_ops;

	for (int i = 0; i < BACKEND_NUM_TYPES; i++)
	{
		PgStatShared_IOContextOps *stats_shmem = &backends_stats_shmem->stats[i];
		PgStat_IOContextOps *stats_snap = &backends_stats_snap->stats[i];

		LWLockAcquire(&stats_shmem->lock, LW_SHARED);

		/*
		 * Use the lock in the first BackendType's PgStat_IOContextOps to
		 * protect the reset timestamp as well.
		 */
		if (i == 0)
			backends_stats_snap->stat_reset_timestamp =
				backends_stats_shmem->stat_reset_timestamp;

		memcpy(stats_snap->data, stats_shmem->data, sizeof(stats_shmem->data));
		LWLockRelease(&stats_shmem->lock);
	}

}

/*
* IO Operation statistics are not collected for all BackendTypes.
*
* The following BackendTypes do not participate in the cumulative stats
* subsystem or do not do IO operations worth reporting statistics on:
* - Syslogger because it is not connected to shared memory
* - Archiver because most relevant archiving IO is delegated to a
*   specialized command or module
* - WAL Receiver and WAL Writer IO is not tracked in pg_stat_io for now
*
* Function returns true if BackendType participates in the cumulative stats
* subsystem for IO Operations and false if it does not.
*/
bool
pgstat_io_op_stats_collected(BackendType bktype)
{
	return bktype != B_INVALID && bktype != B_ARCHIVER && bktype != B_LOGGER &&
		bktype != B_WAL_RECEIVER && bktype != B_WAL_WRITER;
}

/*
 * Some BackendTypes do not perform IO operations in certain IOContexts. Check
 * that the given BackendType is expected to do IO in the given IOContext.
 */
bool
pgstat_bktype_io_context_valid(BackendType bktype, IOContext io_context)
{
	bool		no_local;

	/*
	 * In core Postgres, only regular backends and WAL Sender processes
	 * executing queries should use local buffers. Parallel workers will not
	 * use local buffers (see InitLocalBuffers()); however, extensions
	 * leveraging background workers have no such limitation, so track IO
	 * Operations in IOCONTEXT_LOCAL for BackendType B_BG_WORKER.
	 */
	no_local = bktype == B_AUTOVAC_LAUNCHER || bktype == B_BG_WRITER || bktype
		== B_CHECKPOINTER || bktype == B_AUTOVAC_WORKER || bktype ==
		B_STANDALONE_BACKEND || bktype == B_STARTUP;

	if (io_context == IOCONTEXT_LOCAL && no_local)
		return false;

	/*
	 * Some BackendTypes do not currently perform any IO operations in certain
	 * IOContexts, and, while it may not be inherently incorrect for them to
	 * do so, excluding those rows from the view makes the view easier to use.
	 */
	if ((io_context == IOCONTEXT_BULKREAD || io_context == IOCONTEXT_BULKWRITE
		 || io_context == IOCONTEXT_VACUUM) && (bktype == B_CHECKPOINTER
												|| bktype == B_BG_WRITER))
		return false;

	if (io_context == IOCONTEXT_VACUUM && bktype == B_AUTOVAC_LAUNCHER)
		return false;

	if (io_context == IOCONTEXT_BULKWRITE && (bktype == B_AUTOVAC_WORKER ||
											  bktype == B_AUTOVAC_LAUNCHER))
		return false;

	return true;
}

/*
 * Some BackendTypes will never do certain IOOps and some IOOps should not
 * occur in certain IOContexts. Check that the given IOOp is valid for the
 * given BackendType in the given IOContext. Note that there are currently no
 * cases of an IOOp being invalid for a particular BackendType only within a
 * certain IOContext.
 */
bool
pgstat_io_op_valid(BackendType bktype, IOContext io_context, IOOp io_op)
{
	bool		strategy_io_context;

	/*
	 * Some BackendTypes should never track IO Operation statistics.
	 */
	Assert(pgstat_io_op_stats_collected(bktype));

	/*
	 * Some BackendTypes will not do certain IOOps.
	 */
	if ((bktype == B_BG_WRITER || bktype == B_CHECKPOINTER) &&
		(io_op == IOOP_READ || io_op == IOOP_CLOCKSWEEP || io_op == IOOP_HIT))
		return false;

	if ((bktype == B_AUTOVAC_LAUNCHER || bktype == B_BG_WRITER || bktype ==
		 B_CHECKPOINTER) && io_op == IOOP_EXTEND)
		return false;

	/*
	 * Some IOOps are not valid in certain IOContexts
	 */
	if (io_op == IOOP_EXTEND && io_context == IOCONTEXT_BULKREAD)
		return false;

	if (io_op == IOOP_REUSE &&
		(io_context == IOCONTEXT_SHARED || io_context == IOCONTEXT_LOCAL))
		return false;

	/*
	 * Temporary tables using local buffers are not logged and thus do not
	 * require fsync'ing.
	 *
	 * IOOP_FSYNC IOOps done by a backend using a BufferAccessStrategy are
	 * counted in the IOCONTEXT_SHARED IOContext. See comment in
	 * ForwardSyncRequest() for more details.
	 */
	strategy_io_context = io_context == IOCONTEXT_BULKREAD || io_context ==
		IOCONTEXT_BULKWRITE || io_context == IOCONTEXT_VACUUM;

	if ((io_context == IOCONTEXT_LOCAL || strategy_io_context) &&
		io_op == IOOP_FSYNC)
		return false;

	return true;
}

bool
pgstat_expect_io_op(BackendType bktype, IOContext io_context, IOOp io_op)
{
	if (!pgstat_io_op_stats_collected(bktype))
		return false;

	if (!pgstat_bktype_io_context_valid(bktype, io_context))
		return false;

	if (!(pgstat_io_op_valid(bktype, io_context, io_op)))
		return false;

	return true;
}
