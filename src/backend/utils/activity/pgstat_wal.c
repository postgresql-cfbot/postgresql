/* -------------------------------------------------------------------------
 *
 * pgstat_wal.c
 *	  Implementation of WAL statistics.
 *
 * This file contains the implementation of WAL statistics. It is kept
 * separate from pgstat.c to enforce the line between the statistics access /
 * storage implementation and the details about individual types of
 * statistics.
 *
 * Copyright (c) 2001-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_wal.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlog.h"
#include "funcapi.h"
#include "utils/pgstat_internal.h"
#include "executor/instrument.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/pg_lsn.h"


PgStat_PendingWalStats PendingWalStats = {0};

/*
 * WAL usage counters saved from pgWalUsage at the previous call to
 * pgstat_report_wal(). This is used to calculate how much WAL usage
 * happens between pgstat_report_wal() calls, by subtracting
 * the previous counters from the current ones.
 */
static WalUsage prevWalUsage;


static void lsntime_merge(LSNTime *target, LSNTime *src);
static void lsntime_insert(LSNTimeline *timeline, TimestampTz time, XLogRecPtr lsn);

XLogRecPtr	estimate_lsn_at_time(const LSNTimeline *timeline, TimestampTz time);
TimestampTz estimate_time_at_lsn(const LSNTimeline *timeline, XLogRecPtr lsn);

/*
 * Calculate how much WAL usage counters have increased and update
 * shared WAL and IO statistics.
 *
 * Must be called by processes that generate WAL, that do not call
 * pgstat_report_stat(), like walwriter.
 *
 * "force" set to true ensures that the statistics are flushed; note that
 * this needs to acquire the pgstat shmem LWLock, waiting on it.  When
 * set to false, the statistics may not be flushed if the lock could not
 * be acquired.
 */
void
pgstat_report_wal(bool force)
{
	bool		nowait;

	/* like in pgstat.c, don't wait for lock acquisition when !force */
	nowait = !force;

	/* flush wal stats */
	pgstat_flush_wal(nowait);

	/* flush IO stats */
	pgstat_flush_io(nowait);
}

/*
 * Support function for the SQL-callable pgstat* functions. Returns
 * a pointer to the WAL statistics struct.
 */
PgStat_WalStats *
pgstat_fetch_stat_wal(void)
{
	pgstat_snapshot_fixed(PGSTAT_KIND_WAL);

	return &pgStatLocal.snapshot.wal;
}

/*
 * Calculate how much WAL usage counters have increased by subtracting the
 * previous counters from the current ones.
 *
 * If nowait is true, this function returns true if the lock could not be
 * acquired. Otherwise return false.
 */
bool
pgstat_flush_wal(bool nowait)
{
	PgStatShared_Wal *stats_shmem = &pgStatLocal.shmem->wal;
	WalUsage	wal_usage_diff = {0};

	Assert(IsUnderPostmaster || !IsPostmasterEnvironment);
	Assert(pgStatLocal.shmem != NULL &&
		   !pgStatLocal.shmem->is_shutdown);

	/*
	 * This function can be called even if nothing at all has happened. Avoid
	 * taking lock for nothing in that case.
	 */
	if (!pgstat_have_pending_wal())
		return false;

	/*
	 * We don't update the WAL usage portion of the local WalStats elsewhere.
	 * Calculate how much WAL usage counters were increased by subtracting the
	 * previous counters from the current ones.
	 */
	WalUsageAccumDiff(&wal_usage_diff, &pgWalUsage, &prevWalUsage);

	if (!nowait)
		LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(&stats_shmem->lock, LW_EXCLUSIVE))
		return true;

#define WALSTAT_ACC(fld, var_to_add) \
	(stats_shmem->stats.fld += var_to_add.fld)
#define WALSTAT_ACC_INSTR_TIME(fld) \
	(stats_shmem->stats.fld += INSTR_TIME_GET_MICROSEC(PendingWalStats.fld))
	WALSTAT_ACC(wal_records, wal_usage_diff);
	WALSTAT_ACC(wal_fpi, wal_usage_diff);
	WALSTAT_ACC(wal_bytes, wal_usage_diff);
	WALSTAT_ACC(wal_buffers_full, PendingWalStats);
	WALSTAT_ACC(wal_write, PendingWalStats);
	WALSTAT_ACC(wal_sync, PendingWalStats);
	WALSTAT_ACC_INSTR_TIME(wal_write_time);
	WALSTAT_ACC_INSTR_TIME(wal_sync_time);
#undef WALSTAT_ACC_INSTR_TIME
#undef WALSTAT_ACC

	LWLockRelease(&stats_shmem->lock);

	/*
	 * Save the current counters for the subsequent calculation of WAL usage.
	 */
	prevWalUsage = pgWalUsage;

	/*
	 * Clear out the statistics buffer, so it can be re-used.
	 */
	MemSet(&PendingWalStats, 0, sizeof(PendingWalStats));

	return false;
}

void
pgstat_init_wal(void)
{
	/*
	 * Initialize prevWalUsage with pgWalUsage so that pgstat_flush_wal() can
	 * calculate how much pgWalUsage counters are increased by subtracting
	 * prevWalUsage from pgWalUsage.
	 */
	prevWalUsage = pgWalUsage;
}

/*
 * To determine whether any WAL activity has occurred since last time, not
 * only the number of generated WAL records but also the numbers of WAL
 * writes and syncs need to be checked. Because even transaction that
 * generates no WAL records can write or sync WAL data when flushing the
 * data pages.
 */
bool
pgstat_have_pending_wal(void)
{
	return pgWalUsage.wal_records != prevWalUsage.wal_records ||
		PendingWalStats.wal_write != 0 ||
		PendingWalStats.wal_sync != 0;
}

void
pgstat_wal_reset_all_cb(TimestampTz ts)
{
	PgStatShared_Wal *stats_shmem = &pgStatLocal.shmem->wal;

	LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);
	memset(&stats_shmem->stats, 0, sizeof(stats_shmem->stats));
	stats_shmem->stats.stat_reset_timestamp = ts;
	LWLockRelease(&stats_shmem->lock);
}

void
pgstat_wal_snapshot_cb(void)
{
	PgStatShared_Wal *stats_shmem = &pgStatLocal.shmem->wal;

	LWLockAcquire(&stats_shmem->lock, LW_SHARED);
	memcpy(&pgStatLocal.snapshot.wal, &stats_shmem->stats,
		   sizeof(pgStatLocal.snapshot.wal));
	LWLockRelease(&stats_shmem->lock);
}

/*
 * Set *target to be the earlier of *target or *src.
 */
static void
lsntime_merge(LSNTime *target, LSNTime *src)
{
	LSNTime		result;
	uint64		new_members = target->members + src->members;

	if (target->time < src->time)
		result = *target;
	else if (src->time < target->time)
		result = *src;
	else if (target->lsn < src->lsn)
		result = *target;
	else if (src->lsn < target->lsn)
		result = *src;
	else
		result = *target;

	*target = result;
	target->members = new_members;
	src->members = 1;
}

static int
lsntime_merge_target(LSNTimeline *timeline)
{
	/* Don't merge if free space available */
	Assert(timeline->length == LSNTIMELINE_VOLUME);

	for (int i = timeline->length; i-- > 0;)
	{
		/*
		 * An array element can represent up to twice the number of members
		 * represented by the preceding array element.
		 */
		if (timeline->data[i].members < (2 * timeline->data[i - 1].members))
			return i;
	}

	/* Should not be reachable or we are out of space */
	Assert(false);
}

/*
 * Insert a new LSNTime into the LSNTimeline in the first available element,
 * or, if there are no empty elements, insert it into the element at index 0,
 * merge the logical members of two old buckets and move the intervening
 * elements down by one.
 */
void
lsntime_insert(LSNTimeline *timeline, TimestampTz time,
			   XLogRecPtr lsn)
{
	int			merge_target;
	LSNTime		entrant = {.lsn = lsn,.time = time,.members = 1};

	if (timeline->length < LSNTIMELINE_VOLUME)
	{
		/*
		 * The new entry should exceed the most recent entry to ensure time
		 * moves forward on the timeline.
		 */
		Assert(timeline->length == 0 ||
			   (lsn >= timeline->data[LSNTIMELINE_VOLUME - timeline->length].lsn &&
				time >= timeline->data[LSNTIMELINE_VOLUME - timeline->length].time));

		/*
		 * If there are unfilled elements in the timeline, then insert the
		 * passed-in LSNTime into the tail of the array.
		 */
		timeline->length++;
		timeline->data[LSNTIMELINE_VOLUME - timeline->length] = entrant;
		return;
	}

	/*
	 * If all elements in the timeline represent at least one member, merge
	 * the oldest element whose membership is < 2x its predecessor with its
	 * preceding member. Then shift all elements preceding these two elements
	 * down by one and insert the passed-in LSNTime at array index 0.
	 */
	merge_target = lsntime_merge_target(timeline);
	Assert(merge_target >= 0 && merge_target < timeline->length);
	lsntime_merge(&timeline->data[merge_target], &timeline->data[merge_target - 1]);
	memmove(&timeline->data[1], &timeline->data[0], sizeof(LSNTime) * merge_target - 1);
	timeline->data[0] = entrant;
}

/*
 * Translate time to a LSN using the provided timeline. The timeline will not
 * be modified.
 */
XLogRecPtr
estimate_lsn_at_time(const LSNTimeline *timeline, TimestampTz time)
{
	XLogRecPtr	result;
	int64		time_elapsed,
				lsns_elapsed;
	LSNTime		start = {.time = PgStartTime,.lsn = PgStartLSN};
	LSNTime		end = {.time = GetCurrentTimestamp(),.lsn = GetXLogInsertRecPtr()};

	/*
	 * If the provided time is before DB startup, the best we can do is return
	 * the start LSN.
	 */
	if (time < start.time)
		return start.lsn;

	/*
	 * If the provided time is after now, the current LSN is our best
	 * estimate.
	 */
	if (time >= end.time)
		return end.lsn;

	/*
	 * Loop through the timeline. Stop at the first LSNTime earlier than our
	 * target time. This LSNTime will be our interpolation start point. If
	 * there's an LSNTime later than that, then that will be our interpolation
	 * end point.
	 */
	for (int i = LSNTIMELINE_VOLUME - timeline->length; i < LSNTIMELINE_VOLUME; i++)
	{
		if (timeline->data[i].time > time)
			continue;

		start = timeline->data[i];
		if (i > 0)
			end = timeline->data[i - 1];
		goto stop;
	}

	/*
	 * If we exhausted the timeline, then use its earliest LSNTime as our
	 * interpolation end point.
	 */
	if (timeline->length > 0)
		end = timeline->data[timeline->length - 1];

stop:
	Assert(end.time > start.time);
	Assert(end.lsn > start.lsn);
	time_elapsed = end.time - start.time;
	Assert(time_elapsed != 0);
	lsns_elapsed = end.lsn - start.lsn;
	Assert(lsns_elapsed != 0);
	result = (double) (time - start.time) / time_elapsed * lsns_elapsed + start.lsn;
	return Max(result, 0);
}

/*
 * Translate lsn to a time using the provided timeline. The timeline will not
 * be modified.
 */
TimestampTz
estimate_time_at_lsn(const LSNTimeline *timeline, XLogRecPtr lsn)
{
	int64		time_elapsed,
				lsns_elapsed;
	TimestampTz result;
	LSNTime		start = {.time = PgStartTime,.lsn = PgStartLSN};
	LSNTime		end = {.time = GetCurrentTimestamp(),.lsn = GetXLogInsertRecPtr()};

	/*
	 * If the LSN is before DB startup, the best we can do is return that
	 * time.
	 */
	if (lsn <= start.lsn)
		return start.time;

	/*
	 * If the target LSN is after the current insert LSN, the current time is
	 * our best estimate.
	 */
	if (lsn >= end.lsn)
		return end.time;

	/*
	 * Loop through the timeline. Stop at the first LSNTime earlier than our
	 * target LSN. This LSNTime will be our interpolation start point. If
	 * there's an LSNTime later than that, then that will be our interpolation
	 * end point.
	 */
	for (int i = LSNTIMELINE_VOLUME - timeline->length; i < LSNTIMELINE_VOLUME; i++)
	{
		if (timeline->data[i].lsn > lsn)
			continue;

		start = timeline->data[i];
		if (i > 0)
			end = timeline->data[i - 1];
		goto stop;
	}

	/*
	 * If we exhausted the timeline, then use its earliest LSNTime as our
	 * interpolation end point.
	 */
	if (timeline->length > 0)
		end = timeline->data[timeline->length - 1];

stop:
	Assert(end.time > start.time);
	Assert(end.lsn > start.lsn);
	time_elapsed = end.time - start.time;
	Assert(time_elapsed != 0);
	lsns_elapsed = end.lsn - start.lsn;
	Assert(lsns_elapsed != 0);
	result = (double) (lsn - start.lsn) / lsns_elapsed * time_elapsed + start.time;
	return Max(result, 0);
}

void
pgstat_wal_update_lsntimeline(TimestampTz time, XLogRecPtr lsn)
{
	PgStatShared_Wal *stats_shmem = &pgStatLocal.shmem->wal;

	LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);
	lsntime_insert(&stats_shmem->stats.timeline, time, lsn);
	LWLockRelease(&stats_shmem->lock);
}

PG_FUNCTION_INFO_V1(pg_estimate_lsn_at_time);
PG_FUNCTION_INFO_V1(pg_estimate_time_at_lsn);
PG_FUNCTION_INFO_V1(pg_lsntimeline);

Datum
pg_estimate_time_at_lsn(PG_FUNCTION_ARGS)
{
	XLogRecPtr	lsn = PG_GETARG_LSN(0);
	PgStatShared_Wal *stats_shmem = &pgStatLocal.shmem->wal;
	TimestampTz result;

	LWLockAcquire(&stats_shmem->lock, LW_SHARED);
	result = estimate_time_at_lsn(&stats_shmem->stats.timeline, lsn);
	LWLockRelease(&stats_shmem->lock);

	PG_RETURN_TIMESTAMPTZ(result);
}

Datum
pg_estimate_lsn_at_time(PG_FUNCTION_ARGS)
{
	PgStatShared_Wal *stats_shmem = &pgStatLocal.shmem->wal;
	TimestampTz time = PG_GETARG_TIMESTAMPTZ(0);
	XLogRecPtr	result;

	LWLockAcquire(&stats_shmem->lock, LW_SHARED);
	result = estimate_lsn_at_time(&stats_shmem->stats.timeline, time);
	LWLockRelease(&stats_shmem->lock);

	PG_RETURN_LSN(result);
}

Datum
pg_lsntimeline(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	LSNTimeline *timeline;
	PgStatShared_Wal *stats_shmem = &pgStatLocal.shmem->wal;

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	LWLockAcquire(&stats_shmem->lock, LW_SHARED);
	timeline = &stats_shmem->stats.timeline;
	for (int i = LSNTIMELINE_VOLUME - timeline->length; i < LSNTIMELINE_VOLUME; i++)
	{
		Datum		values[2] = {0};
		bool		nulls[2] = {0};

		values[0] = timeline->data[i].time;
		values[1] = timeline->data[i].lsn;
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}
	LWLockRelease(&stats_shmem->lock);
	return (Datum) 0;
}
