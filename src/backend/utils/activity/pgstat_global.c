/* -------------------------------------------------------------------------
 *
 * pgstat_global.c
 *	  Implementation of all global statistics.
 *
 * This file contains the implementation of global statistics. It is kept
 * separate from pgstat.c to enforce the line between the statistics access /
 * storage implementation and the details about individual types of
 * statistics.
 *
 * Copyright (c) 2001-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_global.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/pgstat_internal.h"
#include "utils/timestamp.h"
#include "executor/instrument.h"
#include "utils/memutils.h"
#include "replication/slot.h"


/* ----------
 * pending stats state that is directly modified from outside the stats system
 * ----------
 */

PgStat_BgWriterStats PendingBgWriterStats = {0};
PgStat_CheckpointerStats PendingCheckpointerStats = {0};
PgStat_WalStats	WalStats = {0};


/*
 * SLRU statistics counts waiting to be written to the shared activity
 * statistics.  We assume this variable inits to zeroes.  Entries are
 * one-to-one with slru_names[].
 * Changes of SLRU counters are reported within critical sections so we use
 * static memory in order to avoid memory allocation.
 */
static PgStat_SLRUStats pending_SLRUStats[SLRU_NUM_ELEMENTS];
bool have_slrustats = false;

/*
 * WAL usage counters saved from pgWALUsage at the previous call to
 * pgstat_report_wal(). This is used to calculate how much WAL usage
 * happens between pgstat_report_wal() calls, by substracting
 * the previous counters from the current ones.
 */
static WalUsage prevWalUsage;


/* ----------
 * pgstat_reset_shared_counters() -
 *
 *	Reset cluster-wide shared counters.
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 *
 *  We don't scribble on shared stats while resetting to avoid locking on
 *  shared stats struct. Instead, just record the current counters in another
 *  shared struct, which is protected by StatsLock. See
 *  pgstat_fetch_stat_(archiver|bgwriter|checkpointer) for the reader side.
 * ----------
 */
void
pgstat_reset_shared_counters(const char *target)
{
	TimestampTz now = GetCurrentTimestamp();
	PgStat_Shared_Reset_Target t;

	if (strcmp(target, "archiver") == 0)
		t = RESET_ARCHIVER;
	else if (strcmp(target, "bgwriter") == 0)
		t = RESET_BGWRITER;
	else if (strcmp(target, "wal") == 0)
		t = RESET_WAL;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unrecognized reset target: \"%s\"", target),
				 errhint("Target must be \"archiver\", \"bgwriter\" or \"wal\".")));

	/* Reset statistics for the cluster. */

	switch (t)
	{
		case RESET_ARCHIVER:
			LWLockAcquire(StatsLock, LW_EXCLUSIVE);
			pgstat_copy_global_stats(&pgStatShmem->archiver.reset_offset,
									 &pgStatShmem->archiver.stats,
									 sizeof(PgStat_ArchiverStats),
									 &pgStatShmem->archiver.changecount);
			pgStatShmem->archiver.reset_offset.stat_reset_timestamp = now;
			LWLockRelease(StatsLock);
			break;

		case RESET_BGWRITER:
			LWLockAcquire(StatsLock, LW_EXCLUSIVE);
			pgstat_copy_global_stats(&pgStatShmem->bgwriter.reset_offset,
									 &pgStatShmem->bgwriter.stats,
									 sizeof(PgStat_BgWriterStats),
									 &pgStatShmem->bgwriter.changecount);
			pgstat_copy_global_stats(&pgStatShmem->checkpointer.reset_offset,
									 &pgStatShmem->checkpointer.stats,
									 sizeof(PgStat_CheckpointerStats),
									 &pgStatShmem->checkpointer.changecount);
			pgStatShmem->bgwriter.reset_offset.stat_reset_timestamp = now;
			LWLockRelease(StatsLock);
			break;

		case RESET_WAL:

			/*
			 * Differently from the two cases above, WAL statistics has many
			 * writer processes with the shared stats protected by
			 * pgStatShmem->wal.lock.
			 */
			LWLockAcquire(&pgStatShmem->wal.lock, LW_EXCLUSIVE);
			MemSet(&pgStatShmem->wal.stats, 0, sizeof(PgStat_WalStats));
			pgStatShmem->wal.stats.stat_reset_timestamp = now;
			LWLockRelease(&pgStatShmem->wal.lock);
			break;
	}
}

/* ----------
 * pgstat_report_archiver() -
 *
 * Report archiver statistics
 * ----------
 */
void
pgstat_report_archiver(const char *xlog, bool failed)
{
	TimestampTz now = GetCurrentTimestamp();

	changecount_before_write(&pgStatShmem->archiver.changecount);

	if (failed)
	{
		++pgStatShmem->archiver.stats.failed_count;
		memcpy(&pgStatShmem->archiver.stats.last_failed_wal, xlog,
			   sizeof(pgStatShmem->archiver.stats.last_failed_wal));
		pgStatShmem->archiver.stats.last_failed_timestamp = now;
	}
	else
	{
		++pgStatShmem->archiver.stats.archived_count;
		memcpy(&pgStatShmem->archiver.stats.last_archived_wal, xlog,
			   sizeof(pgStatShmem->archiver.stats.last_archived_wal));
		pgStatShmem->archiver.stats.last_archived_timestamp = now;
	}

	changecount_after_write(&pgStatShmem->archiver.changecount);
}

PgStat_ArchiverStats *
pgstat_fetch_stat_archiver(void)
{
	pgstat_snapshot_global(PGSTAT_KIND_ARCHIVER);

	return &stats_snapshot.archiver;
}

void
pgstat_snapshot_archiver(void)
{
	PgStat_ArchiverStats reset;
	PgStat_ArchiverStats *reset_offset = &pgStatShmem->archiver.reset_offset;

	pgstat_copy_global_stats(&stats_snapshot.archiver,
							 &pgStatShmem->archiver.stats,
							 sizeof(PgStat_ArchiverStats),
							 &pgStatShmem->archiver.changecount);

	LWLockAcquire(StatsLock, LW_SHARED);
	memcpy(&reset, reset_offset, sizeof(PgStat_ArchiverStats));
	LWLockRelease(StatsLock);

	/* compensate by reset offsets */
	if (stats_snapshot.archiver.archived_count == reset.archived_count)
	{
		stats_snapshot.archiver.last_archived_wal[0] = 0;
		stats_snapshot.archiver.last_archived_timestamp = 0;
	}
	stats_snapshot.archiver.archived_count -= reset.archived_count;

	if (stats_snapshot.archiver.failed_count == reset.failed_count)
	{
		stats_snapshot.archiver.last_failed_wal[0] = 0;
		stats_snapshot.archiver.last_failed_timestamp = 0;
	}
	stats_snapshot.archiver.failed_count -= reset.failed_count;

	stats_snapshot.archiver.stat_reset_timestamp = reset.stat_reset_timestamp;
}

/* ----------
 * pgstat_report_bgwriter() -
 *
 * Report bgwriter statistics
 * ----------
 */
void
pgstat_report_bgwriter(void)
{
	static const PgStat_BgWriterStats all_zeroes;
	PgStat_BgWriterStats *s = &pgStatShmem->bgwriter.stats;
	PgStat_BgWriterStats *l = &PendingBgWriterStats;

	Assert(!pgStatShmem->is_shutdown);

	/*
	 * This function can be called even if nothing at all has happened. In
	 * this case, avoid taking lock for a completely empty stats.
	 */
	if (memcmp(&PendingBgWriterStats, &all_zeroes, sizeof(PgStat_BgWriterStats)) == 0)
		return;

	changecount_before_write(&pgStatShmem->bgwriter.changecount);

	s->buf_written_clean += l->buf_written_clean;
	s->maxwritten_clean += l->maxwritten_clean;
	s->buf_alloc += l->buf_alloc;

	changecount_after_write(&pgStatShmem->bgwriter.changecount);

	/*
	 * Clear out the statistics buffer, so it can be re-used.
	 */
	MemSet(&PendingBgWriterStats, 0, sizeof(PendingBgWriterStats));
}

PgStat_BgWriterStats *
pgstat_fetch_stat_bgwriter(void)
{
	pgstat_snapshot_global(PGSTAT_KIND_BGWRITER);

	return &stats_snapshot.bgwriter;
}

void
pgstat_snapshot_bgwriter(void)
{
	PgStat_BgWriterStats reset;
	PgStat_BgWriterStats *reset_offset = &pgStatShmem->bgwriter.reset_offset;

	pgstat_copy_global_stats(&stats_snapshot.bgwriter,
							 &pgStatShmem->bgwriter.stats,
							 sizeof(PgStat_BgWriterStats),
							 &pgStatShmem->bgwriter.changecount);

	LWLockAcquire(StatsLock, LW_SHARED);
	memcpy(&reset, reset_offset, sizeof(PgStat_BgWriterStats));
	LWLockRelease(StatsLock);

	/* compensate by reset offsets */
	stats_snapshot.bgwriter.buf_written_clean -= reset.buf_written_clean;
	stats_snapshot.bgwriter.maxwritten_clean -= reset.maxwritten_clean;
	stats_snapshot.bgwriter.buf_alloc -= reset.buf_alloc;
	stats_snapshot.bgwriter.stat_reset_timestamp = reset.stat_reset_timestamp;
}

/* ----------
 * pgstat_report_checkpointer() -
 *
 * Report checkpointer statistics
 * ----------
 */
void
pgstat_report_checkpointer(void)
{
	/* We assume this initializes to zeroes */
	static const PgStat_CheckpointerStats all_zeroes;
	PgStat_CheckpointerStats *s = &pgStatShmem->checkpointer.stats;
	PgStat_CheckpointerStats *l = &PendingCheckpointerStats;

	/*
	 * This function can be called even if nothing at all has happened. In
	 * this case, avoid taking lock for a completely empty stats.
	 */
	if (memcmp(&PendingCheckpointerStats, &all_zeroes,
			   sizeof(PgStat_CheckpointerStats)) == 0)
		return;

	changecount_before_write(&pgStatShmem->checkpointer.changecount);

	s->timed_checkpoints += l->timed_checkpoints;
	s->requested_checkpoints += l->requested_checkpoints;
	s->checkpoint_write_time += l->checkpoint_write_time;
	s->checkpoint_sync_time += l->checkpoint_sync_time;
	s->buf_written_checkpoints += l->buf_written_checkpoints;
	s->buf_written_backend += l->buf_written_backend;
	s->buf_fsync_backend += l->buf_fsync_backend;

	changecount_after_write(&pgStatShmem->checkpointer.changecount);

	/*
	 * Clear out the statistics buffer, so it can be re-used.
	 */
	MemSet(&PendingCheckpointerStats, 0, sizeof(PendingCheckpointerStats));
}

PgStat_CheckpointerStats *
pgstat_fetch_stat_checkpointer(void)
{
	pgstat_snapshot_global(PGSTAT_KIND_CHECKPOINTER);

	return &stats_snapshot.checkpointer;
}

void
pgstat_snapshot_checkpointer(void)
{
	PgStat_CheckpointerStats reset;
	PgStat_CheckpointerStats *reset_offset = &pgStatShmem->checkpointer.reset_offset;

	pgstat_copy_global_stats(&stats_snapshot.checkpointer,
							 &pgStatShmem->checkpointer.stats,
							 sizeof(PgStat_CheckpointerStats),
							 &pgStatShmem->checkpointer.changecount);

	LWLockAcquire(StatsLock, LW_SHARED);
	memcpy(&reset, reset_offset, sizeof(PgStat_CheckpointerStats));
	LWLockRelease(StatsLock);

	/* compensate by reset offsets */
	stats_snapshot.checkpointer.timed_checkpoints -= reset.timed_checkpoints;
	stats_snapshot.checkpointer.requested_checkpoints -= reset.requested_checkpoints;
	stats_snapshot.checkpointer.buf_written_checkpoints -= reset.buf_written_checkpoints;
	stats_snapshot.checkpointer.buf_written_backend -= reset.buf_written_backend;
	stats_snapshot.checkpointer.buf_fsync_backend -= reset.buf_fsync_backend;
	stats_snapshot.checkpointer.checkpoint_write_time -= reset.checkpoint_write_time;
	stats_snapshot.checkpointer.checkpoint_sync_time -= reset.checkpoint_sync_time;
}

/* ----------
 * pgstat_reset_replslot_counter() -
 *
 *	Tell the statistics collector to reset a single replication slot
 *	counter, or all replication slots counters (when name is null).
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_replslot_counter(const char *name)
{
	int			startidx;
	int			endidx;
	int			i;
	TimestampTz ts;

	/*
	 * AFIXME: pgstats has business no looking into slot.c structures at
	 * this level of detail.
	 */

	if (name)
	{
		ReplicationSlot *slot;

		/* Check if the slot exits with the given name. */
		LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
		slot = SearchNamedReplicationSlot(name);
		LWLockRelease(ReplicationSlotControlLock);

		if (!slot)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("replication slot \"%s\" does not exist",
							name)));

		/*
		 * Nothing to do for physical slots as we collect stats only for
		 * logical slots.
		 */
		if (SlotIsPhysical(slot))
			return;

		/* reset this one entry */
		startidx = endidx = slot - ReplicationSlotCtl->replication_slots;
	}
	else
	{
		/* reset all existent entries */
		startidx = 0;
		endidx = max_replication_slots - 1;
	}

	ts = GetCurrentTimestamp();
	LWLockAcquire(&pgStatShmem->replslot.lock, LW_EXCLUSIVE);
	for (i = startidx; i <= endidx; i++)
	{
		PgStat_ReplSlotStats *statent = &pgStatShmem->replslot.stats[i];
		size_t off;

		off = offsetof(PgStat_ReplSlotStats, slotname) + sizeof(char[NAMEDATALEN]);

		memset(((char *) statent) + off, 0, sizeof(pgStatShmem->replslot.stats[i]) - off);
		statent->stat_reset_timestamp = ts;
	}
	LWLockRelease(&pgStatShmem->replslot.lock);
}

/* ----------
 * pgstat_report_replslot() -
 *
 * Report replication slot activity.
 * ----------
 */
void
pgstat_report_replslot(uint32 index,
					   const char *slotname,
					   PgStat_Counter spilltxns, PgStat_Counter spillcount,
					   PgStat_Counter spillbytes, PgStat_Counter streamtxns,
					   PgStat_Counter streamcount, PgStat_Counter streambytes)
{
	PgStat_ReplSlotStats *statent;

	Assert(index < max_replication_slots);
	Assert(slotname[0] != '\0' && strlen(slotname) < NAMEDATALEN);

	if (!pgstat_track_counts)
		return;

	statent = &pgStatShmem->replslot.stats[index];

	LWLockAcquire(&pgStatShmem->replslot.lock, LW_EXCLUSIVE);

	/* clear the counters if not used */
	if (statent->index == -1)
	{
		memset(statent, 0, sizeof(*statent));
		statent->index = index;
		strlcpy(statent->slotname, slotname, NAMEDATALEN);
	}
	else if (strcmp(slotname, statent->slotname) != 0)
	{
		/* AFIXME: Is there a valid way this can happen? */
		elog(ERROR, "stats out of sync");
	}
	else
	{
		Assert(statent->index == index);
	}

	statent->spill_txns += spilltxns;
	statent->spill_count += spillcount;
	statent->spill_bytes += spillbytes;
	statent->stream_txns += streamtxns;
	statent->stream_count += streamcount;
	statent->stream_bytes += streambytes;

	LWLockRelease(&pgStatShmem->replslot.lock);
}

/* ----------
 * pgstat_report_replslot_drop() -
 *
 * Report replication slot drop.
 * ----------
 */
void
pgstat_report_replslot_drop(uint32 index, const char *slotname)
{
	PgStat_ReplSlotStats *statent;

	Assert(index < max_replication_slots);
	Assert(slotname[0] != '\0' && strlen(slotname) < NAMEDATALEN);

	if (!pgstat_track_counts)
		return;

	statent = &pgStatShmem->replslot.stats[index];

	LWLockAcquire(&pgStatShmem->replslot.lock, LW_EXCLUSIVE);
	/*
	 * NB: need to accept that there might not be any stats, e.g. if we threw
	 * away stats after a crash restart.
	 */
	statent->index = -1;
	LWLockRelease(&pgStatShmem->replslot.lock);
}

static void
pgstat_reset_slru_counter_internal(int index, TimestampTz ts)
{
	LWLockAcquire(&pgStatShmem->slru.lock, LW_EXCLUSIVE);

	memset(&pgStatShmem->slru.stats[index], 0, sizeof(PgStat_SLRUStats));
	pgStatShmem->slru.stats[index].stat_reset_timestamp = ts;

	LWLockRelease(&pgStatShmem->slru.lock);
}

PgStat_ReplSlotStats *
pgstat_fetch_replslot(int *nslots_p)
{
	pgstat_snapshot_global(PGSTAT_KIND_REPLSLOT);

	*nslots_p = stats_snapshot.replslot_count;
	return stats_snapshot.replslot;
}

void
pgstat_snapshot_replslot(void)
{
	if (stats_snapshot.replslot == NULL)
	{
		stats_snapshot.replslot = (PgStat_ReplSlotStats *)
			MemoryContextAlloc(TopMemoryContext,
							   sizeof(PgStat_ReplSlotStats) * max_replication_slots);
	}

	stats_snapshot.replslot_count = 0;

	LWLockAcquire(&pgStatShmem->replslot.lock, LW_EXCLUSIVE);

	for (int i = 0; i < max_replication_slots; i++)
	{
		PgStat_ReplSlotStats *statent = &pgStatShmem->replslot.stats[i];

		if (statent->index != -1)
		{
			stats_snapshot.replslot[stats_snapshot.replslot_count++] = *statent;
		}
	}

	LWLockRelease(&pgStatShmem->replslot.lock);
}

/* ----------
 * pgstat_reset_slru_counter() -
 *
 *	Tell the statistics collector to reset a single SLRU counter, or all
 *	SLRU counters (when name is null).
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_slru_counter(const char *name)
{
	int			i;
	TimestampTz ts = GetCurrentTimestamp();

	if (name)
	{
		i = pgstat_slru_index(name);
		pgstat_reset_slru_counter_internal(i, ts);
	}
	else
	{
		for (i = 0; i < SLRU_NUM_ELEMENTS; i++)
			pgstat_reset_slru_counter_internal(i, ts);
	}
}

/*
 * pgstat_flush_slru - flush out locally pending SLRU stats entries
 *
 * If nowait is true, this function returns false on lock failure. Otherwise
 * this function always returns true. Writer processes are mutually excluded
 * using LWLock, but readers are expected to use change-count protocol to avoid
 * interference with writers.
 *
 * Returns true if not all pending stats have been flushed out.
 */
bool
pgstat_flush_slru(bool nowait)
{
	int			i;

	if (!have_slrustats)
		return false;

	/* lock the shared entry to protect the content, skip if failed */
	if (!nowait)
		LWLockAcquire(&pgStatShmem->slru.lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(&pgStatShmem->slru.lock,
									   LW_EXCLUSIVE))
		return true;			/* failed to acquire lock, skip */


	for (i = 0; i < SLRU_NUM_ELEMENTS; i++)
	{
		PgStat_SLRUStats *sharedent = &pgStatShmem->slru.stats[i];
		PgStat_SLRUStats *pendingent = &pending_SLRUStats[i];

		sharedent->blocks_zeroed += pendingent->blocks_zeroed;
		sharedent->blocks_hit += pendingent->blocks_hit;
		sharedent->blocks_read += pendingent->blocks_read;
		sharedent->blocks_written += pendingent->blocks_written;
		sharedent->blocks_exists += pendingent->blocks_exists;
		sharedent->flush += pendingent->flush;
		sharedent->truncate += pendingent->truncate;
	}

	/* done, clear the pending entry */
	MemSet(pending_SLRUStats, 0, SizeOfSlruStats);

	LWLockRelease(&pgStatShmem->slru.lock);

	have_slrustats = false;

	return false;
}

/*
 * slru_entry
 *
 * Returns pointer to entry with counters for given SLRU (based on the name
 * stored in SlruCtl as lwlock tranche name).
 */
static inline PgStat_SLRUStats *
slru_entry(int slru_idx)
{
	/*
	 * The postmaster should never register any SLRU statistics counts; if it
	 * did, the counts would be duplicated into child processes via fork().
	 */
	Assert(IsUnderPostmaster || !IsPostmasterEnvironment);

	Assert((slru_idx >= 0) && (slru_idx < SLRU_NUM_ELEMENTS));

	have_slrustats = true;

	return &pending_SLRUStats[slru_idx];
}

/*
 * SLRU statistics count accumulation functions --- called from slru.c
 */

void
pgstat_count_slru_page_zeroed(int slru_idx)
{
	slru_entry(slru_idx)->blocks_zeroed += 1;
}

void
pgstat_count_slru_page_hit(int slru_idx)
{
	slru_entry(slru_idx)->blocks_hit += 1;
}

void
pgstat_count_slru_page_exists(int slru_idx)
{
	slru_entry(slru_idx)->blocks_exists += 1;
}

void
pgstat_count_slru_page_read(int slru_idx)
{
	slru_entry(slru_idx)->blocks_read += 1;
}

void
pgstat_count_slru_page_written(int slru_idx)
{
	slru_entry(slru_idx)->blocks_written += 1;
}

void
pgstat_count_slru_flush(int slru_idx)
{
	slru_entry(slru_idx)->flush += 1;
}

void
pgstat_count_slru_truncate(int slru_idx)
{
	slru_entry(slru_idx)->truncate += 1;
}

/*
 * pgstat_slru_name
 *
 * Returns SLRU name for an index. The index may be above SLRU_NUM_ELEMENTS,
 * in which case this returns NULL. This allows writing code that does not
 * know the number of entries in advance.
 */
const char *
pgstat_slru_name(int slru_idx)
{
	if (slru_idx < 0 || slru_idx >= SLRU_NUM_ELEMENTS)
		return NULL;

	return slru_names[slru_idx];
}

/*
 * pgstat_slru_index
 *
 * Determine index of entry for a SLRU with a given name. If there's no exact
 * match, returns index of the last "other" entry used for SLRUs defined in
 * external projects.
 */
int
pgstat_slru_index(const char *name)
{
	int			i;

	for (i = 0; i < SLRU_NUM_ELEMENTS; i++)
	{
		if (strcmp(slru_names[i], name) == 0)
			return i;
	}

	/* return index of the last entry (which is the "other" one) */
	return (SLRU_NUM_ELEMENTS - 1);
}

void
pgstat_snapshot_slru(void)
{
	LWLockAcquire(&pgStatShmem->slru.lock, LW_SHARED);

	memcpy(stats_snapshot.slru, &pgStatShmem->slru.stats, SizeOfSlruStats);

	LWLockRelease(&pgStatShmem->slru.lock);
}

PgStat_SLRUStats *
pgstat_fetch_slru(void)
{
	pgstat_snapshot_global(PGSTAT_KIND_SLRU);

	return stats_snapshot.slru;
}

/* ----------
 * pgstat_report_wal() -
 *
 * Calculate how much WAL usage counters have increased and update
 * shared statistics.
 *
 * Must be called by processes that generate WAL.
 * ----------
 */
void
pgstat_report_wal(bool force)
{
	Assert(!pgStatShmem->is_shutdown);

	pgstat_flush_wal(force);
}

/*
 * Calculate how much WAL usage counters have increased by substracting the
 * previous counters from the current ones.
 *
 * If nowait is true, this function returns true if the lock could not be
 * acquired. Otherwise return false.
 */
bool
pgstat_flush_wal(bool nowait)
{
	PgStat_WalStats *s = &pgStatShmem->wal.stats;
	PgStat_WalStats *l = &WalStats;
	WalUsage	all_zeroes PG_USED_FOR_ASSERTS_ONLY = {0};

	Assert(IsUnderPostmaster || !IsPostmasterEnvironment);
	Assert(pgStatShmem != NULL);

	/*
	 * We don't update the WAL usage portion of the local WalStats elsewhere.
	 * Instead, fill in that portion with the difference of pgWalUsage since
	 * the previous call.
	 */
	Assert(memcmp(&l->wal_usage, &all_zeroes, sizeof(WalUsage)) == 0);
	WalUsageAccumDiff(&l->wal_usage, &pgWalUsage, &prevWalUsage);

	/*
	 * This function can be called even if nothing at all has happened. Avoid
	 * taking lock for nothing in that case.
	 */
	if (!walstats_pending())
		return false;

	/* lock the shared entry to protect the content, skip if failed */
	if (!nowait)
		LWLockAcquire(&pgStatShmem->wal.lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(&pgStatShmem->wal.lock,
									   LW_EXCLUSIVE))
	{
		MemSet(&WalStats, 0, sizeof(WalStats));
		return true;			/* failed to acquire lock, skip */
	}

	s->wal_usage.wal_records += l->wal_usage.wal_records;
	s->wal_usage.wal_fpi += l->wal_usage.wal_fpi;
	s->wal_usage.wal_bytes += l->wal_usage.wal_bytes;
	s->wal_buffers_full += l->wal_buffers_full;
	s->wal_write += l->wal_write;
	s->wal_write_time += l->wal_write_time;
	s->wal_sync += l->wal_sync;
	s->wal_sync_time += l->wal_sync_time;
	LWLockRelease(&pgStatShmem->wal.lock);

	/*
	 * Save the current counters for the subsequent calculation of WAL usage.
	 */
	prevWalUsage = pgWalUsage;

	/*
	 * Clear out the statistics buffer, so it can be re-used.
	 */
	MemSet(&WalStats, 0, sizeof(WalStats));

	return false;
}

void
pgstat_wal_initialize(void)
{
	/*
	 * Initialize prevWalUsage with pgWalUsage so that pgstat_report_wal() can
	 * calculate how much pgWalUsage counters are increased by substracting
	 * prevWalUsage from pgWalUsage.
	 */
	prevWalUsage = pgWalUsage;
}

/*
 * XXXX: always try to flush WAL stats. We don't want to manipulate another
 * counter during XLogInsert so we don't have an effecient short cut to know
 * whether any counter gets incremented.
 */
bool
walstats_pending(void)
{
	static const PgStat_WalStats all_zeroes;

	return memcmp(&WalStats, &all_zeroes,
				  offsetof(PgStat_WalStats, stat_reset_timestamp)) != 0;
}

PgStat_WalStats *
pgstat_fetch_stat_wal(void)
{
	pgstat_snapshot_global(PGSTAT_KIND_WAL);

	return &stats_snapshot.wal;
}

void
pgstat_snapshot_wal(void)
{
	LWLockAcquire(StatsLock, LW_SHARED);
	memcpy(&stats_snapshot.wal, &pgStatShmem->wal.stats, sizeof(PgStat_WalStats));
	LWLockRelease(StatsLock);
}
