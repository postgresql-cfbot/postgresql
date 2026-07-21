/* -------------------------------------------------------------------------
 *
 * pgstat_relfilenode.c
 *	  Implementation of per-relfilenode statistics.
 *
 * This file contains the implementation of PGSTAT_KIND_RELFILENODE statistics,
 * for tuple counters (inserts, updates, deletes, live/dead tuples, etc.) keyed
 * by relfilenode.  These counters are rebuilt during crash recovery, from WAL
 * replayed.
 *
 * Copyright (c) 2001-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_relfilenode.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/memutils.h"
#include "utils/pgstat_internal.h"


/*
 * Flush out pending stats for a relfilenode entry.
 *
 * If nowait is true and the lock could not be immediately acquired, returns
 * false without flushing the entry.  Otherwise returns true.
 */
bool
pgstat_relfilenode_flush_cb(PgStat_EntryRef *entry_ref, bool nowait)
{
	PgStat_RelationStatus *lstats;		/* pending stats entry */
	PgStatShared_RelFileNode *shrfnstats;
	PgStat_StatRFNodeEntry *rfnentry;

	lstats = (PgStat_RelationStatus *) entry_ref->pending;
	shrfnstats = (PgStatShared_RelFileNode *) entry_ref->shared_stats;

	/*
	 * Ignore entries that didn't accumulate any actual counts, such as
	 * tables that were opened but not modified.
	 */
	if (pg_memory_is_all_zeros(&lstats->tab.counts,
							   sizeof(struct PgStat_TableCounts)))
		return true;

	if (!pgstat_lock_entry(entry_ref, nowait))
		return false;

	/* Add the values to the shared entry. */
	rfnentry = &shrfnstats->stats;

	/*
	 * If table was truncated/dropped, first reset the live/dead counters.
	 */
	if (lstats->tab.counts.truncdropped)
	{
		rfnentry->live_tuples = 0;
		rfnentry->dead_tuples = 0;
		rfnentry->ins_since_vacuum = 0;
	}

	rfnentry->live_tuples += lstats->tab.counts.delta_live_tuples;
	rfnentry->dead_tuples += lstats->tab.counts.delta_dead_tuples;
	rfnentry->mod_since_analyze += lstats->tab.counts.changed_tuples;

	/*
	 * Using tuples_inserted to update ins_since_vacuum means we track
	 * aborted inserts too.  This isn't ideal, but not worth adding an extra
	 * field for — it may just trigger autovacuums slightly more often than
	 * necessary.
	 */
	rfnentry->ins_since_vacuum += lstats->tab.counts.tuples_inserted;

	/* Clamp live_tuples in case of negative delta_live_tuples */
	rfnentry->live_tuples = Max(rfnentry->live_tuples, 0);
	/* Likewise for dead_tuples */
	rfnentry->dead_tuples = Max(rfnentry->dead_tuples, 0);

	pgstat_unlock_entry(entry_ref);

	return true;
}

/*
 * Callback to reset the timestamp on a relfilenode stats entry.
 */
void
pgstat_relfilenode_reset_timestamp_cb(PgStatShared_Common *header,
									  TimestampTz ts)
{
	((PgStatShared_RelFileNode *) header)->stats.stat_reset_time = ts;
}

/*
 * Fetch the relfilenode stats entry for the given database and relfilenode.
 */
PgStat_StatRFNodeEntry *
pgstat_fetch_stat_rfnodeentry(Oid dboid, RelFileNumber rfn)
{
	return (PgStat_StatRFNodeEntry *)
		pgstat_fetch_entry(PGSTAT_KIND_RELFILENODE, dboid, (uint64) rfn, NULL);
}

/*
 * WAL replay: increment tuple counters in the relfilenode entry.
 *
 * This is called during WAL replay to rebuild tuple counters from
 * heap WAL records.  It directly updates the shared stats entry without
 * going through the pending/flush machinery.
 */
void
pgstat_wal_replay_insert(Oid dboid, RelFileNumber relfilenode, int ntuples)
{
	PgStat_EntryRef *entry_ref;
	PgStatShared_RelFileNode *shrfnstats;
	PgStat_StatRFNodeEntry *rfnentry;

	entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RELFILENODE,
											dboid, (uint64) relfilenode,
											false);
	shrfnstats = (PgStatShared_RelFileNode *) entry_ref->shared_stats;
	rfnentry = &shrfnstats->stats;

	rfnentry->tuples_inserted += ntuples;
	rfnentry->mod_since_analyze += ntuples;
	rfnentry->ins_since_vacuum += ntuples;
	rfnentry->live_tuples += ntuples;

	pgstat_unlock_entry(entry_ref);
}

void
pgstat_wal_replay_update(Oid dboid, RelFileNumber relfilenode,
						 bool hot, bool newpage)
{
	PgStat_EntryRef *entry_ref;
	PgStatShared_RelFileNode *shrfnstats;
	PgStat_StatRFNodeEntry *rfnentry;

	entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RELFILENODE,
											dboid, (uint64) relfilenode,
											false);
	shrfnstats = (PgStatShared_RelFileNode *) entry_ref->shared_stats;
	rfnentry = &shrfnstats->stats;

	rfnentry->tuples_updated++;
	rfnentry->mod_since_analyze++;
	rfnentry->dead_tuples++;
	if (hot)
		rfnentry->tuples_hot_updated++;
	if (newpage)
		rfnentry->tuples_newpage_updated++;

	pgstat_unlock_entry(entry_ref);
}

void
pgstat_wal_replay_delete(Oid dboid, RelFileNumber relfilenode)
{
	PgStat_EntryRef *entry_ref;
	PgStatShared_RelFileNode *shrfnstats;
	PgStat_StatRFNodeEntry *rfnentry;

	entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RELFILENODE,
											dboid, (uint64) relfilenode,
											false);
	shrfnstats = (PgStatShared_RelFileNode *) entry_ref->shared_stats;
	rfnentry = &shrfnstats->stats;

	rfnentry->tuples_deleted++;
	rfnentry->mod_since_analyze++;
	rfnentry->dead_tuples++;
	rfnentry->live_tuples--;
	rfnentry->live_tuples = Max(rfnentry->live_tuples, 0);

	pgstat_unlock_entry(entry_ref);
}
