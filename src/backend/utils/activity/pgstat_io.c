/* -------------------------------------------------------------------------
 *
 * pgstat_io.c
 *	  Implementation of IO statistics.
 *
 * This file contains the implementation of IO statistics. It is kept separate
 * from pgstat.c to enforce the line between the statistics access / storage
 * implementation and the details about individual types of statistics.
 *
 * Copyright (c) 2021-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_io.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/instrument.h"
#include "storage/bufmgr.h"
#include "access/xlog.h"
#include "utils/pgstat_internal.h"


typedef struct PgStat_PendingIO
{
	PgStat_Counter counts[IOOBJECT_NUM_TYPES][IOCONTEXT_NUM_TYPES][IOOP_NUM_TYPES];
	instr_time	pending_times[IOOBJECT_NUM_TYPES][IOCONTEXT_NUM_TYPES][IOOP_NUM_TYPES];
} PgStat_PendingIO;


static PgStat_PendingIO PendingIOStats;
bool		have_iostats = false;


/*
 * Check that stats have not been counted for any combination of IOObject,
 * IOContext, and IOOp which are not tracked for the passed-in BackendType. If
 * stats are tracked for this combination and IO times are non-zero, counts
 * should be non-zero.
 *
 * The passed-in PgStat_BktypeIO must contain stats from the BackendType
 * specified by the second parameter. Caller is responsible for locking the
 * passed-in PgStat_BktypeIO, if needed.
 */
bool
pgstat_bktype_io_stats_valid(PgStat_BktypeIO *backend_io,
							 BackendType bktype)
{
	for (int io_object = 0; io_object < IOOBJECT_NUM_TYPES; io_object++)
	{
		for (int io_context = 0; io_context < IOCONTEXT_NUM_TYPES; io_context++)
		{
			for (int io_op = 0; io_op < IOOP_NUM_TYPES; io_op++)
			{
				/* we do track it */
				if (pgstat_tracks_io_op(bktype, io_object, io_context, io_op))
				{
					/* ensure that if IO times are non-zero, counts are > 0 */
					if (backend_io->times[io_object][io_context][io_op] != 0 &&
						backend_io->counts[io_object][io_context][io_op] <= 0)
						return false;

					continue;
				}

				/* we don't track it, and it is not 0 */
				if (backend_io->counts[io_object][io_context][io_op] != 0)
					return false;
			}
		}
	}

	return true;
}

void
pgstat_count_io_op(IOObject io_object, IOContext io_context, IOOp io_op)
{
	pgstat_count_io_op_n(io_object, io_context, io_op, 1);
}

void
pgstat_count_io_op_n(IOObject io_object, IOContext io_context, IOOp io_op, uint32 cnt)
{
	Assert((unsigned int) io_object < IOOBJECT_NUM_TYPES);
	Assert((unsigned int) io_context < IOCONTEXT_NUM_TYPES);
	Assert((unsigned int) io_op < IOOP_NUM_TYPES);
	Assert(pgstat_tracks_io_op(MyBackendType, io_object, io_context, io_op));

	/*
	 * Only count PendingWalStats.wal_sync in pg_stat_io because
	 * PendingWalStats.wal_write could count different than what pg_stat_io
	 * counts e.g. system calls. Also, PendingWalStats doesn't count WAL init
	 * or WAL reads. So, they are not included too.
	 */
	if (io_object == IOOBJECT_WAL && io_context == IOCONTEXT_NORMAL &&
	    io_op == IOOP_FSYNC)
		PendingWalStats.wal_sync += cnt;

	PendingIOStats.counts[io_object][io_context][io_op] += cnt;

	have_iostats = true;
}

/*
 * Prepares io_time for pgstat_count_io_op_time() function. It needs to return
 * current time if there is a chance that any 'time' can be tracked.
 */
instr_time
pgstat_prepare_io_time(void)
{
	instr_time	io_start;

	if (track_io_timing || track_wal_io_timing)
		INSTR_TIME_SET_CURRENT(io_start);
	else
		/*
		 * If time won't be tracked, there is no need to set io_start but
		 * compiler complains about uninitialized use. So, set it to zero.
		 */
		INSTR_TIME_SET_ZERO(io_start);

	return io_start;
}

/*
 * Decide if the io timing needs be tracked
 */
bool
pgstat_should_track_io_time(IOObject io_object, IOContext io_context)
{
	/*
	 * io times of IOOBJECT_WAL IOObject needs to be tracked when
	 * 'track_wal_io_timing' is set regardless of 'track_io_timing'.
	 */
	if (io_object == IOOBJECT_WAL)
		return track_wal_io_timing;

	return track_io_timing;
}

/*
 * Like pgstat_count_io_op_n() except it also accumulates time.
 */
void
pgstat_count_io_op_time(IOObject io_object, IOContext io_context, IOOp io_op,
						instr_time start_time, uint32 cnt)
{
	/*
	 * Accumulate different type of times here. We want to eventually
	 * deduplicate these counters, so we are consolidating them first.
	 * This also makes it easy to compare what is tracked for which
	 * stats or instrumentation purpose.
	 *
	 * Some IO counters didn't moved here because they track at a different
	 * level of granularity or at a different point in the call stack.
	 *
	 * pgstat_count_buffer is for pgstat_database. Since pg_stat_database only
	 * counts blk_read_time and blk_write_time, it is set for IOOP_READ and
	 * IOOP_WRITE.
	 *
	 * pgBufferUsage is for EXPLAIN. pgBufferUsage has only write and read stats
	 * for shared/local and temporary blocks. Only shared/local blocks are counted
	 * here.
	 *
	 * PendingWalStats is for counting WAL stats on pg_stat_wal. It
	 * only counts IOOBJECT_WAL / IOCONTEXT_NORMAL.
	 *
	 * At the end of the if case, accumulate time for pg_stat_io.
	 */
	if (pgstat_should_track_io_time(io_object, io_context))
	{
		instr_time	io_time;

		INSTR_TIME_SET_CURRENT(io_time);
		INSTR_TIME_SUBTRACT(io_time, start_time);

		if (io_op == IOOP_FSYNC)
		{
			/* Track IOOBJECT_WAL/IOCONTEXT_NORMAL times on PendingWalStats */
			if (io_object == IOOBJECT_WAL && io_context == IOCONTEXT_NORMAL)
				INSTR_TIME_ADD(PendingWalStats.wal_sync_time, io_time);
		}
		else if (io_op == IOOP_READ)
		{
			pgstat_count_buffer_read_time(INSTR_TIME_GET_MICROSEC(io_time));
			if (io_object == IOOBJECT_RELATION)
				INSTR_TIME_ADD(pgBufferUsage.blk_read_time, io_time);
		}
		else if (io_op == IOOP_WRITE)
		{
			pgstat_count_buffer_write_time(INSTR_TIME_GET_MICROSEC(io_time));
			if (io_object == IOOBJECT_RELATION)
				INSTR_TIME_ADD(pgBufferUsage.blk_write_time, io_time);

			/* Track IOOBJECT_WAL/IOCONTEXT_NORMAL times on PendingWalStats */
			else if (io_object == IOOBJECT_WAL && io_context == IOCONTEXT_NORMAL)
				INSTR_TIME_ADD(PendingWalStats.wal_write_time, io_time);
		}

		INSTR_TIME_ADD(PendingIOStats.pending_times[io_object][io_context][io_op],
					   io_time);
	}

	/* IO OP times are counted, now count IO OPs */
	pgstat_count_io_op_n(io_object, io_context, io_op, cnt);
}

PgStat_IO *
pgstat_fetch_stat_io(void)
{
	pgstat_snapshot_fixed(PGSTAT_KIND_IO);

	return &pgStatLocal.snapshot.io;
}

/*
 * Flush out locally pending IO statistics
 *
 * If no stats have been recorded, this function returns false.
 *
 * If nowait is true, this function returns true if the lock could not be
 * acquired. Otherwise, return false.
 */
bool
pgstat_flush_io(bool nowait)
{
	LWLock	   *bktype_lock;
	PgStat_BktypeIO *bktype_shstats;

	if (!have_iostats)
		return false;

	bktype_lock = &pgStatLocal.shmem->io.locks[MyBackendType];
	bktype_shstats =
		&pgStatLocal.shmem->io.stats.stats[MyBackendType];

	if (!nowait)
		LWLockAcquire(bktype_lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(bktype_lock, LW_EXCLUSIVE))
		return true;

	for (int io_object = 0; io_object < IOOBJECT_NUM_TYPES; io_object++)
	{
		for (int io_context = 0; io_context < IOCONTEXT_NUM_TYPES; io_context++)
		{
			for (int io_op = 0; io_op < IOOP_NUM_TYPES; io_op++)
			{
				instr_time	time;

				bktype_shstats->counts[io_object][io_context][io_op] +=
					PendingIOStats.counts[io_object][io_context][io_op];

				time = PendingIOStats.pending_times[io_object][io_context][io_op];

				bktype_shstats->times[io_object][io_context][io_op] +=
					INSTR_TIME_GET_MICROSEC(time);
			}
		}
	}

	Assert(pgstat_bktype_io_stats_valid(bktype_shstats, MyBackendType));

	LWLockRelease(bktype_lock);

	memset(&PendingIOStats, 0, sizeof(PendingIOStats));

	have_iostats = false;

	return false;
}

const char *
pgstat_get_io_context_name(IOContext io_context)
{
	switch (io_context)
	{
		case IOCONTEXT_BULKREAD:
			return "bulkread";
		case IOCONTEXT_BULKWRITE:
			return "bulkwrite";
		case IOCONTEXT_NORMAL:
			return "normal";
		case IOCONTEXT_VACUUM:
			return "vacuum";
		case IOCONTEXT_INIT:
			return "init";
	}

	elog(ERROR, "unrecognized IOContext value: %d", io_context);
	pg_unreachable();
}

/*
 * op_bytes can change according to IOObject and IOContext.
 * Return BLCKSZ as default because most of the
 * IOObject / IOContext uses BLCKSZ.
 */
int
pgstat_get_io_op_bytes(IOObject io_object, IOContext io_context)
{
	if (io_object == IOOBJECT_WAL)
	{
		if (io_context == IOCONTEXT_NORMAL)
			return XLOG_BLCKSZ;
		else if (io_context == IOCONTEXT_INIT)
			return wal_segment_size;
	}

	return BLCKSZ;
}

const char *
pgstat_get_io_object_name(IOObject io_object)
{
	switch (io_object)
	{
		case IOOBJECT_RELATION:
			return "relation";
		case IOOBJECT_TEMP_RELATION:
			return "temp relation";
		case IOOBJECT_WAL:
			return "wal";
	}

	elog(ERROR, "unrecognized IOObject value: %d", io_object);
	pg_unreachable();
}

void
pgstat_io_reset_all_cb(TimestampTz ts)
{
	for (int i = 0; i < BACKEND_NUM_TYPES; i++)
	{
		LWLock	   *bktype_lock = &pgStatLocal.shmem->io.locks[i];
		PgStat_BktypeIO *bktype_shstats = &pgStatLocal.shmem->io.stats.stats[i];

		LWLockAcquire(bktype_lock, LW_EXCLUSIVE);

		/*
		 * Use the lock in the first BackendType's PgStat_BktypeIO to protect
		 * the reset timestamp as well.
		 */
		if (i == 0)
			pgStatLocal.shmem->io.stats.stat_reset_timestamp = ts;

		memset(bktype_shstats, 0, sizeof(*bktype_shstats));
		LWLockRelease(bktype_lock);
	}
}

void
pgstat_io_snapshot_cb(void)
{
	for (int i = 0; i < BACKEND_NUM_TYPES; i++)
	{
		LWLock	   *bktype_lock = &pgStatLocal.shmem->io.locks[i];
		PgStat_BktypeIO *bktype_shstats = &pgStatLocal.shmem->io.stats.stats[i];
		PgStat_BktypeIO *bktype_snap = &pgStatLocal.snapshot.io.stats[i];

		LWLockAcquire(bktype_lock, LW_SHARED);

		/*
		 * Use the lock in the first BackendType's PgStat_BktypeIO to protect
		 * the reset timestamp as well.
		 */
		if (i == 0)
			pgStatLocal.snapshot.io.stat_reset_timestamp =
				pgStatLocal.shmem->io.stats.stat_reset_timestamp;

		/* using struct assignment due to better type safety */
		*bktype_snap = *bktype_shstats;
		LWLockRelease(bktype_lock);
	}
}

/*
* IO statistics are not collected for all BackendTypes.
*
* The following BackendTypes do not participate in the cumulative stats
* subsystem or do not perform IO on which we currently track:
* - Syslogger because it is not connected to shared memory
* - Archiver because most relevant archiving IO is delegated to a
*   specialized command or module
* - WAL Receiver and WAL Writer IO is not tracked in pg_stat_io for now
*
* Function returns true if BackendType participates in the cumulative stats
* subsystem for IO and false if it does not.
*
* When adding a new BackendType, also consider adding relevant restrictions to
* pgstat_tracks_io_object() and pgstat_tracks_io_op().
*/
bool
pgstat_tracks_io_bktype(BackendType bktype)
{
	/*
	 * List every type so that new backend types trigger a warning about
	 * needing to adjust this switch.
	 */
	switch (bktype)
	{
		case B_INVALID:
		case B_ARCHIVER:
		case B_LOGGER:
			return false;

		case B_WAL_RECEIVER:
		case B_WAL_WRITER:
		case B_AUTOVAC_LAUNCHER:
		case B_AUTOVAC_WORKER:
		case B_BACKEND:
		case B_BG_WORKER:
		case B_BG_WRITER:
		case B_CHECKPOINTER:
		case B_STANDALONE_BACKEND:
		case B_STARTUP:
		case B_WAL_SENDER:
			return true;
	}

	return false;
}

/*
 * Some BackendTypes do not perform IO on certain IOObjects or in certain
 * IOContexts. Some IOObjects are never operated on in some IOContexts. Check
 * that the given BackendType is expected to do IO in the given IOContext and
 * on the given IOObject and that the given IOObject is expected to be operated
 * on in the given IOContext.
 */
bool
pgstat_tracks_io_object(BackendType bktype, IOObject io_object,
						IOContext io_context)
{
	bool		no_temp_rel;

	/*
	 * Some BackendTypes should never track IO statistics.
	 */
	if (!pgstat_tracks_io_bktype(bktype))
		return false;

	/*
	 * Currently, IO on IOOBJECT_WAL IOObject can only occur in the
	 * IOCONTEXT_NORMAL and IOCONTEXT_INIT IOContext.
	 */
	if (io_object == IOOBJECT_WAL &&
	    (io_context != IOCONTEXT_NORMAL &&
	     io_context != IOCONTEXT_INIT))
		return false;

	/*
	 * Currently, IO on temporary relations can only occur in the
	 * IOCONTEXT_NORMAL IOContext.
	 */
	if (io_context != IOCONTEXT_NORMAL &&
		io_object == IOOBJECT_TEMP_RELATION)
		return false;

	/*
	 * In core Postgres, only regular backends and WAL Sender processes
	 * executing queries will use local buffers and operate on temporary
	 * relations. Parallel workers will not use local buffers (see
	 * InitLocalBuffers()); however, extensions leveraging background workers
	 * have no such limitation, so track IO on IOOBJECT_TEMP_RELATION for
	 * BackendType B_BG_WORKER.
	 */
	no_temp_rel = bktype == B_AUTOVAC_LAUNCHER || bktype == B_BG_WRITER ||
		bktype == B_CHECKPOINTER || bktype == B_AUTOVAC_WORKER ||
		bktype == B_STANDALONE_BACKEND || bktype == B_STARTUP;

	if (no_temp_rel && io_context == IOCONTEXT_NORMAL &&
		io_object == IOOBJECT_TEMP_RELATION)
		return false;

	/*
	 * Some BackendTypes do not currently perform any IO in certain
	 * IOContexts, and, while it may not be inherently incorrect for them to
	 * do so, excluding those rows from the view makes the view easier to use.
	 */
	if ((bktype == B_CHECKPOINTER || bktype == B_BG_WRITER) &&
		(io_context == IOCONTEXT_BULKREAD ||
		 io_context == IOCONTEXT_BULKWRITE ||
		 io_context == IOCONTEXT_VACUUM))
		return false;

	if (bktype == B_AUTOVAC_LAUNCHER && io_context == IOCONTEXT_VACUUM)
		return false;

	if ((bktype == B_AUTOVAC_WORKER || bktype == B_AUTOVAC_LAUNCHER) &&
		io_context == IOCONTEXT_BULKWRITE)
		return false;

	return true;
}

/*
 * Some BackendTypes will never do certain IOOps and some IOOps should not
 * occur in certain IOContexts or on certain IOObjects. Check that the given
 * IOOp is valid for the given BackendType in the given IOContext and on the
 * given IOObject. Note that there are currently no cases of an IOOp being
 * invalid for a particular BackendType only within a certain IOContext and/or
 * only on a certain IOObject.
 */
bool
pgstat_tracks_io_op(BackendType bktype, IOObject io_object,
					IOContext io_context, IOOp io_op)
{
	bool		strategy_io_context;

	/* if (io_context, io_object) will never collect stats, we're done */
	if (!pgstat_tracks_io_object(bktype, io_object, io_context))
		return false;

	/*
	 * Some BackendTypes will not do certain IOOps.
	 */
	if ((bktype == B_BG_WRITER || bktype == B_CHECKPOINTER) &&
		(io_op == IOOP_READ || io_op == IOOP_EVICT || io_op == IOOP_HIT))
		return false;

	if ((bktype == B_AUTOVAC_LAUNCHER || bktype == B_BG_WRITER ||
		 bktype == B_CHECKPOINTER) && io_op == IOOP_EXTEND)
		return false;

	/*
	 * Temporary tables are not logged and thus do not require fsync'ing.
	 * Writeback is not requested for temporary tables.
	 */
	if (io_object == IOOBJECT_TEMP_RELATION &&
		(io_op == IOOP_FSYNC || io_op == IOOP_WRITEBACK))
		return false;

	/*
	 * Some IOOps are not valid in certain IOContexts and some IOOps are only
	 * valid in certain contexts.
	 */
	if (io_context == IOCONTEXT_BULKREAD && io_op == IOOP_EXTEND)
		return false;

	strategy_io_context = io_context == IOCONTEXT_BULKREAD ||
		io_context == IOCONTEXT_BULKWRITE || io_context == IOCONTEXT_VACUUM;

	/*
	 * IOOP_REUSE is only relevant when a BufferAccessStrategy is in use.
	 */
	if (!strategy_io_context && io_op == IOOP_REUSE)
		return false;

	/*
	 * Some IOOps are not valid in certain IOContexts & IOObjects and some
	 * IOOps are only valid in certain IOContexts & IOObjects.
	 */

	/*
	 * In IOOBJECT_WAL io_object, IOCONTEXT_INIT io_context means
	 * operations done while creating new WAL segments.
	 */
	if(io_object == IOOBJECT_WAL && io_context == IOCONTEXT_INIT &&
	   !(io_op == IOOP_WRITE || io_op == IOOP_FSYNC))
	   return false;

	/*
	 * In IOOBJECT_WAL io_object, IOCONTEXT_NORMAL io_context means
	 * operations done on already created WAL segments.
	 */
	if(io_object == IOOBJECT_WAL && io_context == IOCONTEXT_NORMAL &&
	   !(io_op == IOOP_WRITE || io_op == IOOP_READ || io_op == IOOP_FSYNC))
	   return false;

	/*
	 * IOOP_FSYNC IOOps done by a backend using a BufferAccessStrategy are
	 * counted in the IOCONTEXT_NORMAL IOContext. See comment in
	 * register_dirty_segment() for more details.
	 */
	if (strategy_io_context && io_op == IOOP_FSYNC)
		return false;


	return true;
}
