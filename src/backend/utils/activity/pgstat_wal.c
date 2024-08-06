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
#include "executor/instrument.h"
#include "math.h"
#include "utils/pgstat_internal.h"
#include "utils/timestamp.h"


PgStat_PendingWalStats PendingWalStats = {0};

/*
 * WAL usage counters saved from pgWalUsage at the previous call to
 * pgstat_report_wal(). This is used to calculate how much WAL usage
 * happens between pgstat_report_wal() calls, by subtracting
 * the previous counters from the current ones.
 */
static WalUsage prevWalUsage;

static double lsn_ts_calculate_error_area(LSNTime *left,
										  LSNTime *mid,
										  LSNTime *right);
static unsigned char lsntime_to_drop(LSNTimeStream *stream);
static void lsntime_insert(LSNTimeStream *stream, TimestampTz time,
						   XLogRecPtr lsn);

static void stream_get_bounds_for_lsn(const LSNTimeStream *stream,
									  XLogRecPtr target_lsn,
									  LSNTime *lower,
									  LSNTime *upper);

static void stream_get_bounds_for_time(const LSNTimeStream *stream,
									   TimestampTz target_time,
									   LSNTime *lower,
									   LSNTime *upper);


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
 * Simple wrapper of pgstat_wal_flush_cb()
 */
void
pgstat_flush_wal(bool nowait)
{
	(void) pgstat_wal_flush_cb(nowait);
}

/*
 * Calculate how much WAL usage counters have increased by subtracting the
 * previous counters from the current ones.
 *
 * If nowait is true, this function returns true if the lock could not be
 * acquired. Otherwise return false.
 */
bool
pgstat_wal_flush_cb(bool nowait)
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
	if (!pgstat_wal_have_pending_cb())
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
pgstat_wal_init_backend_cb(void)
{
	/*
	 * Initialize prevWalUsage with pgWalUsage so that pgstat_wal_flush_cb()
	 * can calculate how much pgWalUsage counters are increased by subtracting
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
pgstat_wal_have_pending_cb(void)
{
	return pgWalUsage.wal_records != prevWalUsage.wal_records ||
		PendingWalStats.wal_write != 0 ||
		PendingWalStats.wal_sync != 0;
}

void
pgstat_wal_init_shmem_cb(void *stats)
{
	PgStatShared_Wal *stats_shmem = (PgStatShared_Wal *) stats;

	LWLockInitialize(&stats_shmem->lock, LWTRANCHE_PGSTATS_DATA);
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
 * Given three LSNTimes, calculate the area of the triangle they form were
 * they plotted with time on the X axis and LSN on the Y axis. An
 * illustration:
 *
 *   LSN
 *    |
 *    |                                                         * right
 *    |
 *    |
 *    |
 *    |                                                * mid    * C
 *    |
 *    |
 *    |
 *    |  * left                                        * B      * A
 *    |
 *    +------------------------------------------------------------------
 *
 * The area of the triangle with vertices (left, mid, right) is the error
 * incurred over the interval [left, right] were we to interpolate with just
 * [left, right] rather than [left, mid] and [mid, right].
 */
static double
lsn_ts_calculate_error_area(LSNTime *left, LSNTime *mid, LSNTime *right)
{
	double		left_time = left->time,
				left_lsn = left->lsn;
	double		mid_time = mid->time,
				mid_lsn = mid->lsn;
	double		right_time = right->time,
				right_lsn = right->lsn;
	double		rectangle_all,
				triangle1,
				triangle2,
				triangle3,
				rectangle_part,
				area_to_subtract;

	/* Area of the rectangle with opposing corners left and right */
	rectangle_all = (right_time - left_time) * (right_lsn - left_lsn);

	/* Area of the right triangle with vertices left, right, and A */
	triangle1 = rectangle_all / 2;

	/* Area of the right triangle with vertices left, mid, and B */
	triangle2 = (mid_lsn - left_lsn) * (mid_time - left_time) / 2;

	/* Area of the right triangle with vertices mid, right, and C */
	triangle3 = (right_lsn - mid_lsn) * (right_time - mid_time) / 2;

	/* Area of the rectangle with vertices mid, A, B, and C */
	rectangle_part = (right_lsn - mid_lsn) * (mid_time - left_time);

	/* Sum up the area to subtract first to produce a more precise answer */
	area_to_subtract = triangle2 + triangle3 + rectangle_part;

	/* Area of the triangle with vertices left, mid, and right */
	return fabs(triangle1 - area_to_subtract);
}

/*
 * Determine which LSNTime to drop from a full LSNTimeStream.
 * Drop the LSNTime whose absence would introduce the least error into future
 * linear interpolation on the stream.
 *
 * We determine the error that would be introduced by dropping a point on the
 * stream by calculating the area of the triangle formed by the LSNTime and
 * its adjacent LSNTimes. We do this for each LSNTime in the stream (except
 * for the first and last LSNTimes) and choose the LSNTime with the smallest
 * error (area).
 *
 * We avoid extrapolation by never dropping the first or last points.
 */
static unsigned char
lsntime_to_drop(LSNTimeStream *stream)
{
	double		min_area;
	unsigned char target_point;

	/* Don't drop points if free spots available are available */
	Assert(stream->length == LSNTIMESTREAM_VOLUME);
	StaticAssertStmt(LSNTIMESTREAM_VOLUME >= 3, "LSNTIMESTREAM_VOLUME < 3");

	min_area = lsn_ts_calculate_error_area(&stream->data[0],
										   &stream->data[1],
										   &stream->data[2]);

	target_point = 1;

	for (size_t i = 2; i < stream->length - 1; i++)
	{
		LSNTime    *left = &stream->data[i - 1];
		LSNTime    *mid = &stream->data[i];
		LSNTime    *right = &stream->data[i + 1];
		double		area = lsn_ts_calculate_error_area(left, mid, right);

		if (area < min_area)
		{
			min_area = area;
			target_point = i;
		}
	}

	return target_point;
}

/*
 * Insert a new LSNTime into the LSNTimeStream in the first available element.
 * If there are no empty elements, drop an LSNTime from the stream to make
 * room for the new LSNTime.
 */
static void
lsntime_insert(LSNTimeStream *stream, TimestampTz time,
			   XLogRecPtr lsn)
{
	unsigned char drop;
	LSNTime		entrant = {.lsn = lsn,.time = time};

	if (stream->length < LSNTIMESTREAM_VOLUME)
	{
		/*
		 * Time must move forward on the stream. If the clock moves backwards,
		 * for example in an NTP correction, we'll just skip inserting this
		 * LSNTime.
		 *
		 * Translating LSN <-> time is most meaningful if the LSNTimeStream
		 * entries are the position of a single location in the WAL over time.
		 * Though time must monotonically increase, it is valid to insert
		 * multiple LSNTimes with the same LSN. Imagine a period of time in
		 * which no new WAL records are inserted.
		 */
		if (stream->length > 0 &&
			(time <= stream->data[stream->length - 1].time ||
			 lsn < stream->data[stream->length - 1].lsn))
		{
			ereport(WARNING,
					errmsg("Won't insert non-monotonic \"%lu, %s\" to LSNTimeStream.",
						   lsn, timestamptz_to_str(time)));
			return;
		}

		stream->data[stream->length++] = entrant;
		return;
	}

	drop = lsntime_to_drop(stream);

	memmove(&stream->data[drop],
			&stream->data[drop + 1],
			sizeof(LSNTime) * (stream->length - 1 - drop));

	stream->data[stream->length - 1] = entrant;
}


/*
 * Returns a range of LSNTimes starting at lower and ending at upper and
 * covering the target_time. If target_time is before the stream, lower will
 * contain the minimum values for the datatypes. If target_time is newer than
 * the stream, upper will contain the maximum values for the datatypes.
 */
static void
stream_get_bounds_for_time(const LSNTimeStream *stream,
						   TimestampTz target_time,
						   LSNTime *lower,
						   LSNTime *upper)
{
	Assert(lower && upper);

	/*
	 * If the target_time is "off the stream" -- either the stream has no
	 * members or the target_time is older than all values in the stream or
	 * newer than all values -- the lower and/or upper bounds may be the min
	 * or max value for the datatypes, respectively.
	 */
	*lower = LSNTIME_INIT(InvalidXLogRecPtr, INT64_MIN);
	*upper = LSNTIME_INIT(UINT64_MAX, INT64_MAX);

	/*
	 * If the LSNTimeStream has no members, it provides no information about
	 * the range.
	 */
	if (stream->length == 0)
	{
		elog(DEBUG1,
			 "Attempt to identify LSN bounds for time: \"%s\" using empty LSNTimeStream.",
			 timestamptz_to_str(target_time));
		return;
	}

	/*
	 * If the target_time is older than the stream, the oldest member in the
	 * stream is our upper bound.
	 */
	if (target_time <= stream->data[0].time)
	{
		*upper = stream->data[0];
		if (target_time == stream->data[0].time)
			*lower = stream->data[0];
		return;
	}

	/*
	 * Loop through the stream and stop at the first LSNTime newer than or
	 * equal to our target time. Skip the first LSNTime, as we know it is
	 * older than our target time.
	 */
	for (size_t i = 1; i < stream->length; i++)
	{
		if (target_time == stream->data[i].time)
		{
			*lower = stream->data[i];
			*upper = stream->data[i];
			return;
		}

		if (target_time < stream->data[i].time)
		{
			/* Time must increase monotonically on the stream. */
			Assert(stream->data[i - 1].time <
				   stream->data[i].time);
			*lower = stream->data[i - 1];
			*upper = stream->data[i];
			return;
		}
	}

	/*
	 * target_time is newer than the stream, so the newest member in the
	 * stream is our lower bound.
	 */
	*lower = stream->data[stream->length - 1];
}

/*
 * Try to find an upper and lower bound for the possible LSN values at the
 * provided target_time. If the target_time doesn't fall on the provided
 * LSNTimeStream, we compare the target_time to the current time and see if we
 * can fill in a missing boundary. Note that we do not consult the
 * current time if the target_time fell on the stream -- even if doing so
 * might provide a tighter range.
 */
void
lsn_bounds_for_time(const LSNTimeStream *stream, TimestampTz target_time,
					LSNTime *lower, LSNTime *upper)
{
	TimestampTz current_time;
	XLogRecPtr	current_lsn;

	stream_get_bounds_for_time(stream, target_time, lower, upper);

	/*
	 * We found valid upper and lower bounds for target_time, so we're done.
	 */
	if (lower->lsn != InvalidXLogRecPtr && upper->lsn != UINT64_MAX)
		return;

	/*
	 * The target_time was either off the stream or the stream has no members.
	 * In either case, see if we can use the current time and LSN to provide
	 * one (or both) of the bounds.
	 */
	current_time = GetCurrentTimestamp();
	current_lsn = GetXLogInsertRecPtr();

	if (lower->lsn == InvalidXLogRecPtr && target_time >= current_time)
		*lower = LSNTIME_INIT(current_lsn, current_time);

	if (upper->lsn == UINT64_MAX && target_time <= current_time)
		*upper = LSNTIME_INIT(current_lsn, current_time);

	Assert(upper->lsn >= lower->lsn);
}

/*
 * Returns a range of LSNTimes starting at lower and ending at upper and
 * covering the target_lsn. If target_lsn is before the stream, lower will
 * contain the minimum values for the datatypes. If target_time is newer than
 * the stream, upper will contain the maximum values for the datatypes.
 */
static void
stream_get_bounds_for_lsn(const LSNTimeStream *stream,
						  XLogRecPtr target_lsn,
						  LSNTime *lower,
						  LSNTime *upper)
{
	Assert(lower && upper);

	/*
	 * If the target_time is "off the stream" -- either the stream has no
	 * members or the target_time is older than all values in the stream or
	 * newer than all values -- the lower and/or upper bounds may be the min
	 * or max value for the datatypes, respectively.
	 */
	*lower = LSNTIME_INIT(InvalidXLogRecPtr, INT64_MIN);
	*upper = LSNTIME_INIT(UINT64_MAX, INT64_MAX);

	/*
	 * If the LSNTimeStream has no members, it provides no information about
	 * the range.
	 */
	if (stream->length == 0)
	{
		elog(DEBUG1,
			 "Attempt to identify time bounds for LSN: \"%lu\" using empty LSNTimeStream.",
			 target_lsn);
		return;
	}

	/*
	 * If the target_lsn is older than the stream, the oldest member in the
	 * stream is our upper bound.
	 */
	if (target_lsn <= stream->data[0].lsn)
	{
		*upper = stream->data[0];
		if (target_lsn == stream->data[0].lsn)
			*lower = stream->data[0];
		return;
	}

	/*
	 * Loop through the stream and stop at the first LSNTime newer than or
	 * equal to our target time. Skip the first LSNTime, as we know it is
	 * older than our target time.
	 */
	for (size_t i = 1; i < stream->length; i++)
	{
		if (target_lsn == stream->data[i].lsn)
		{
			*lower = stream->data[i - 1];
			*upper = stream->data[i];
			return;
		}

		if (target_lsn < stream->data[i].lsn)
		{
			/* LSNs must not decrease on the stream. */
			Assert(stream->data[i - 1].lsn <=
				   stream->data[i].lsn);
			*lower = stream->data[i - 1];
			*upper = stream->data[i];
			return;
		}
	}

	/*
	 * target_lsn is newer than the stream, so the newest member in the stream
	 * is our lower bound.
	 */
	*lower = stream->data[stream->length - 1];
}

/*
 * Try to find an upper and lower bound for the possible times covering the
 * provided target_lsn. If the target_lsn doesn't fall on the provided
 * LSNTimeStream, we compare the target_lsn to the current insert LSN and see
 * if we can fill in a missing boundary. Note that we do not consult
 * the current insert LSN if the target_lsn fell on the stream -- even if
 * doing so might provide a tighter range.
 */
void
time_bounds_for_lsn(const LSNTimeStream *stream, XLogRecPtr target_lsn,
					LSNTime *lower, LSNTime *upper)
{
	TimestampTz current_time;
	XLogRecPtr	current_lsn;

	stream_get_bounds_for_lsn(stream, target_lsn, lower, upper);

	/*
	 * We found valid upper and lower bounds for target_lsn, so we're done.
	 */
	if (lower->time != INT64_MIN && upper->time != INT64_MAX)
		return;

	/*
	 * The target_lsn was either off the stream or the stream has no members.
	 * In either case, see if we can use the current time and LSN to provide
	 * one (or both) of the bounds.
	 */
	current_time = GetCurrentTimestamp();
	current_lsn = GetXLogInsertRecPtr();

	if (lower->time == INT64_MIN && target_lsn >= current_lsn)
		*lower = LSNTIME_INIT(current_lsn, current_time);

	if (upper->time == INT64_MAX && target_lsn <= current_lsn)
		*upper = LSNTIME_INIT(current_lsn, current_time);

	Assert(upper->time >= lower->time);
}
