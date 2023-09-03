/* -------------------------------------------------------------------------
 *
 * pgstat_slru.c
 *	  Implementation of SLRU statistics.
 *
 * This file contains the implementation of SLRU statistics. It is kept
 * separate from pgstat.c to enforce the line between the statistics access /
 * storage implementation and the details about individual types of
 * statistics.
 *
 * Copyright (c) 2001-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_slru.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/pgstat_internal.h"
#include "utils/timestamp.h"


static inline PgStat_SLRUStats *get_slru_entry(int slru_idx);
static void pgstat_reset_slru_counter_internal(int index, TimestampTz ts);


/*
 * SLRU statistics counts waiting to be flushed out.  We assume this variable
 * inits to zeroes.  Entries are one-to-one with slru_names[].  Changes of
 * SLRU counters are reported within critical sections so we use static memory
 * in order to avoid memory allocation.
 */
static PgStat_SLRUStats pending_SLRUStats[NREL_NUM_RELS + 1];
bool		have_slrustats = false;


/*
 * Reset counters for a single SLRU.
 *
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
void
pgstat_reset_slru(const char *name)
{
	TimestampTz ts = GetCurrentTimestamp();

	Assert(name != NULL);

	pgstat_reset_slru_counter_internal(pgstat_get_slru_index(name), ts);
}

/*
 * SLRU statistics count accumulation functions --- called from slru.c
 */

void
pgstat_count_slru_page_zeroed(int slru_idx)
{
	get_slru_entry(slru_idx)->blocks_zeroed += 1;
}

void
pgstat_count_slru_page_hit(int slru_idx)
{
	get_slru_entry(slru_idx)->blocks_hit += 1;
}

void
pgstat_count_slru_page_exists(int slru_idx)
{
	get_slru_entry(slru_idx)->blocks_exists += 1;
}

void
pgstat_count_slru_page_read(int slru_idx)
{
	get_slru_entry(slru_idx)->blocks_read += 1;
}

void
pgstat_count_slru_page_written(int slru_idx)
{
	get_slru_entry(slru_idx)->blocks_written += 1;
}

void
pgstat_count_slru_flush(int slru_idx)
{
	get_slru_entry(slru_idx)->flush += 1;
}

void
pgstat_count_slru_truncate(int slru_idx)
{
	get_slru_entry(slru_idx)->truncate += 1;
}

/*
 * Support function for the SQL-callable pgstat* functions. Returns
 * a pointer to the slru statistics struct.
 */
PgStat_SLRUStats *
pgstat_fetch_slru(void)
{
	pgstat_snapshot_fixed(PGSTAT_KIND_SLRU);

	return pgStatLocal.snapshot.slru;
}

/*
 * Returns SLRU name for an index.
 */
const char *
pgstat_get_slru_name(int slru_idx)
{
	return NrelName(slru_idx);
}

/*
 * Determine index of entry for a SLRU with a given name.
 */
int
pgstat_get_slru_index(const char *name)
{
	return NrelRelIdByName(name);
}

/*
 * Flush out locally pending SLRU stats entries
 *
 * If nowait is true, this function returns false on lock failure. Otherwise
 * this function always returns true.
 *
 * If nowait is true, this function returns true if the lock could not be
 * acquired. Otherwise return false.
 */
bool
pgstat_slru_flush(bool nowait)
{
	PgStatShared_SLRU *stats_shmem = &pgStatLocal.shmem->slru;
	int			i;

	if (!have_slrustats)
		return false;

	if (!nowait)
		LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(&stats_shmem->lock, LW_EXCLUSIVE))
		return true;

	for (i = 0; i < NREL_NUM_RELS + 1; i++)
	{
		PgStat_SLRUStats *sharedent = &stats_shmem->stats[i];
		PgStat_SLRUStats *pendingent = &pending_SLRUStats[i];

#define SLRU_ACC(fld) sharedent->fld += pendingent->fld
		SLRU_ACC(blocks_zeroed);
		SLRU_ACC(blocks_hit);
		SLRU_ACC(blocks_read);
		SLRU_ACC(blocks_written);
		SLRU_ACC(blocks_exists);
		SLRU_ACC(flush);
		SLRU_ACC(truncate);
#undef SLRU_ACC
	}

	/* done, clear the pending entry */
	MemSet(pending_SLRUStats, 0, sizeof(pending_SLRUStats));

	LWLockRelease(&stats_shmem->lock);

	have_slrustats = false;

	return false;
}

void
pgstat_slru_reset_all_cb(TimestampTz ts)
{
	for (int i = 0; i < NREL_NUM_RELS + 1; i++)
		pgstat_reset_slru_counter_internal(i, ts);
}

void
pgstat_slru_snapshot_cb(void)
{
	PgStatShared_SLRU *stats_shmem = &pgStatLocal.shmem->slru;

	LWLockAcquire(&stats_shmem->lock, LW_SHARED);

	memcpy(pgStatLocal.snapshot.slru, &stats_shmem->stats,
		   sizeof(stats_shmem->stats));

	LWLockRelease(&stats_shmem->lock);
}

/*
 * Returns pointer to entry with counters for given SLRU.
 */
static inline PgStat_SLRUStats *
get_slru_entry(int slru_idx)
{
	pgstat_assert_is_up();

	/*
	 * The postmaster should never register any SLRU statistics counts; if it
	 * did, the counts would be duplicated into child processes via fork().
	 */
	Assert(IsUnderPostmaster || !IsPostmasterEnvironment);

	Assert((slru_idx >= 0) && (slru_idx < NREL_NUM_RELS + 1));

	have_slrustats = true;

	return &pending_SLRUStats[slru_idx];
}

static void
pgstat_reset_slru_counter_internal(int index, TimestampTz ts)
{
	PgStatShared_SLRU *stats_shmem = &pgStatLocal.shmem->slru;

	LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);

	memset(&stats_shmem->stats[index], 0, sizeof(PgStat_SLRUStats));
	stats_shmem->stats[index].stat_reset_timestamp = ts;

	LWLockRelease(&stats_shmem->lock);
}
