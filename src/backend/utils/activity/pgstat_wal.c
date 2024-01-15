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
#include "utils/pgstat_internal.h"
#include "executor/instrument.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"


PgStat_PendingWalStats PendingWalStats = {0};

/*
 * WAL usage counters saved from pgWalUsage at the previous call to
 * pgstat_report_wal(). This is used to calculate how much WAL usage
 * happens between pgstat_report_wal() calls, by subtracting
 * the previous counters from the current ones.
 */
static WalUsage prevWalUsage;


static void lsntime_absorb(LSNTime *a, const LSNTime *b);
static void lsntime_insert(LSNTimeline *timeline, TimestampTz time, XLogRecPtr lsn);

static XLogRecPtr estimate_lsn_at_time(const LSNTimeline *timeline, TimestampTz time);
static TimestampTz estimate_time_at_lsn(const LSNTimeline *timeline, XLogRecPtr lsn);

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
 * Set *a to be the earlier of *a or *b.
 */
static void
lsntime_absorb(LSNTime *a, const LSNTime *b)
{
	LSNTime		result;
	int			new_members = a->members + b->members;

	if (a->time < b->time)
		result = *a;
	else if (b->time < a->time)
		result = *b;
	else if (a->lsn < b->lsn)
		result = *a;
	else if (b->lsn < a->lsn)
		result = *b;
	else
		result = *a;

	*a = result;
	a->members = new_members;
}

/*
 * Insert a new LSNTime into the LSNTimeline in the first element with spare
 * capacity.
 */
static void
lsntime_insert(LSNTimeline *timeline, TimestampTz time,
			   XLogRecPtr lsn)
{
	LSNTime		temp;
	LSNTime		carry = {.lsn = lsn,.time = time,.members = 1};

	for (int i = 0; i < timeline->length; i++)
	{
		bool		full;
		LSNTime    *cur = &timeline->data[i];

		/*
		 * An array element's capacity to represent members is 2 ^ its
		 * position in the array.
		 */
		full = cur->members >= (1 << i);

		/*
		 * If the current element is not yet at capacity, then insert the
		 * passed-in LSNTime into this element by taking the smaller of the it
		 * and the current LSNTime element. This is required to ensure that
		 * time moves forward on the timeline.
		 */
		if (!full)
		{
			Assert(cur->members == carry.members);
			Assert(cur->members + carry.members <= 1 << i);
			lsntime_absorb(cur, &carry);
			return;
		}

		/*
		 * If the current element is full, ensure that the inserting LSNTime
		 * is larger than the current element. This must be true for time to
		 * move forward on the timeline.
		 */
		Assert(carry.lsn >= cur->lsn || carry.time >= cur->time);

		/*
		 * If the element is at capacity, swap the element with the carry and
		 * continue on to find an element with space to represent the new
		 * member.
		 */
		temp = *cur;
		*cur = carry;
		carry = temp;
	}

	/*
	 * Time to use another element in the array -- and increase the length in
	 * the process
	 */
	timeline->data[timeline->length] = carry;
	timeline->length++;
}


/*
 * Translate time to a LSN using the provided timeline. The timeline will not
 * be modified.
 */
static XLogRecPtr
estimate_lsn_at_time(const LSNTimeline *timeline, TimestampTz time)
{
	TimestampTz time_elapsed;
	XLogRecPtr	lsns_elapsed;
	double		result;

	LSNTime		start = {.time = PgStartTime,.lsn = PgStartLSN};
	LSNTime		end = {.time = GetCurrentTimestamp(),.lsn = GetXLogInsertRecPtr()};

	/*
	 * If the target time is after the current time, our best estimate of the
	 * LSN is the current insert LSN.
	 */
	if (time >= end.time)
		return end.lsn;

	for (int i = 0; i < timeline->length; i++)
	{
		/* Pass times more recent than our target time */
		if (timeline->data[i].time > time)
			continue;

		/* Found the first element before our target time */
		start = timeline->data[i];

		/*
		 * If there is only one element in the array, use the current time as
		 * the end of the range. Otherwise it is the element preceding our
		 * start.
		 */
		if (i > 0)
			end = timeline->data[i - 1];
		break;
	}

	time_elapsed = end.time - start.time;
	Assert(time_elapsed != 0);

	lsns_elapsed = end.lsn - start.lsn;
	Assert(lsns_elapsed != 0);

	result = (double) (time - start.time) / time_elapsed * lsns_elapsed + start.lsn;
	if (result < 0)
		return InvalidXLogRecPtr;
	return result;
}

/*
 * Translate lsn to a time using the provided timeline. The timeline will not
 * be modified.
 */
static TimestampTz
estimate_time_at_lsn(const LSNTimeline *timeline, XLogRecPtr lsn)
{
	TimestampTz time_elapsed;
	XLogRecPtr	lsns_elapsed;
	TimestampTz result;

	LSNTime		start = {.time = PgStartTime,.lsn = PgStartLSN};
	LSNTime		end = {.time = GetCurrentTimestamp(),.lsn = GetXLogInsertRecPtr()};

	/*
	 * If the target LSN is after the current insert LSN, the current time is
	 * our best estimate.
	 */
	if (lsn >= end.lsn)
		return end.time;

	for (int i = 0; i < timeline->length; i++)
	{
		/* Pass LSNs more recent than our target LSN */
		if (timeline->data[i].lsn > lsn)
			continue;

		/* Found the first element before our target LSN */
		start = timeline->data[i];

		/*
		 * If there is only one element in the array, use the current LSN and
		 * time as the end of the range. Otherwise, use the preceding element
		 * (the first element occuring before our target LSN in the timeline).
		 */
		if (i > 0)
			end = timeline->data[i - 1];
		break;
	}

	time_elapsed = end.time - start.time;
	Assert(time_elapsed != 0);

	lsns_elapsed = end.lsn - start.lsn;
	Assert(lsns_elapsed != 0);

	result = (lsn - start.lsn) / lsns_elapsed * time_elapsed + start.time;
	if (result < 0)
		return 0;
	return result;
}

XLogRecPtr
pgstat_wal_estimate_lsn_at_time(TimestampTz time)
{
	XLogRecPtr	result;
	PgStatShared_Wal *stats_shmem = &pgStatLocal.shmem->wal;

	LWLockAcquire(&stats_shmem->lock, LW_SHARED);
	result = estimate_lsn_at_time(&stats_shmem->stats.timeline, time);
	LWLockRelease(&stats_shmem->lock);

	return result;
}

TimestampTz
pgstat_wal_estimate_time_at_lsn(XLogRecPtr lsn)
{
	TimestampTz result;
	PgStatShared_Wal *stats_shmem = &pgStatLocal.shmem->wal;

	LWLockAcquire(&stats_shmem->lock, LW_SHARED);
	result = estimate_time_at_lsn(&stats_shmem->stats.timeline, lsn);
	LWLockRelease(&stats_shmem->lock);

	return result;
}

void
pgstat_wal_update_lsntimeline(TimestampTz time, XLogRecPtr lsn)
{
	PgStatShared_Wal *stats_shmem = &pgStatLocal.shmem->wal;

	LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);
	lsntime_insert(&stats_shmem->stats.timeline, time, lsn);
	LWLockRelease(&stats_shmem->lock);
}
