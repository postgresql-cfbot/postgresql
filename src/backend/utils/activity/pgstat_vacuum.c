/* -------------------------------------------------------------------------
 *
 * pgstat_vacuum.c
 *	  Implementation of extended vacuum statistics.
 *
 * This file contains the implementation of extended vacuum statistics. It is
 * kept separate from pgstat_relation.c and pgstat_database.c to reduce the
 * memory footprint of the regular relation and database statistics: vacuum
 * metrics require significantly more space per relation, so they live in their
 * own PGSTAT_KIND_VACUUM_RELATION stats kind.
 *
 * Copyright (c) 2001-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_vacuum.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pgstat.h"
#include "utils/memutils.h"
#include "utils/pgstat_internal.h"

#define ACCUMULATE_SUBFIELD(substruct, field) (dst->substruct.field += src->substruct.field)

/*
 * Accumulate the per-table extended vacuum counters collected so far.
 *
 * Only the counters derived directly from the vacuum's own bookkeeping are
 * summed here.  The buffer, WAL and timing counters (and the per-index
 * counters) are accumulated by additional code added together with the
 * helpers that gather them.
 */
static void
pgstat_accumulate_extvac_stats_relations(PgStat_VacuumRelationCounts *dst,
										 PgStat_VacuumRelationCounts *src)
{
	if (!pgstat_track_vacuum_statistics)
		return;

	if (dst->type == PGSTAT_EXTVAC_INVALID)
		dst->type = src->type;

	Assert(src->type != PGSTAT_EXTVAC_INVALID &&
		   src->type != PGSTAT_EXTVAC_DB &&
		   src->type == dst->type);

	ACCUMULATE_SUBFIELD(common, tuples_deleted);

	if (dst->type == PGSTAT_EXTVAC_TABLE)
	{
		ACCUMULATE_SUBFIELD(table, pages_scanned);
		ACCUMULATE_SUBFIELD(table, pages_removed);
		ACCUMULATE_SUBFIELD(table, tuples_frozen);
		ACCUMULATE_SUBFIELD(table, recently_dead_tuples);
		ACCUMULATE_SUBFIELD(table, missed_dead_pages);
		ACCUMULATE_SUBFIELD(table, missed_dead_tuples);
	}
	else if (dst->type == PGSTAT_EXTVAC_INDEX)
	{
		ACCUMULATE_SUBFIELD(index, pages_deleted);
	}
}

/*
 * Report that the relation was just vacuumed, accumulating its extended
 * statistics into the per-relation entry.
 */
void
pgstat_report_vacuum_extstats(Oid tableoid, bool shared,
							  PgStat_VacuumRelationCounts *params)
{
	PgStat_EntryRef *entry_ref;
	PgStat_RelationVacuumPending *relpending;
	Oid			dboid = (shared ? InvalidOid : MyDatabaseId);

	if (!pgstat_track_vacuum_statistics)
		return;

	/*
	 * Accumulate into a pending entry instead of taking a shared-stats lock
	 * here; pgstat_report_stat() flushes it to shared memory through the
	 * registered flush callback once the vacuum's transaction completes.
	 */
	entry_ref = pgstat_prep_pending_entry(PGSTAT_KIND_VACUUM_RELATION,
										  dboid, tableoid, NULL);
	relpending = (PgStat_RelationVacuumPending *) entry_ref->pending;
	pgstat_accumulate_extvac_stats_relations(&relpending->counts, params);
}

/*
 * Flush out pending per-relation extended vacuum stats for the entry.
 *
 * If nowait is true, this function returns false if the lock could not be
 * acquired immediately, otherwise true is returned.
 */
bool
pgstat_vacuum_relation_flush_cb(PgStat_EntryRef *entry_ref, bool nowait)
{
	PgStatShared_VacuumRelation *shtabstats;
	PgStat_RelationVacuumPending *pendingent;

	pendingent = (PgStat_RelationVacuumPending *) entry_ref->pending;
	shtabstats = (PgStatShared_VacuumRelation *) entry_ref->shared_stats;

	/* Ignore entries that didn't accumulate any actual counts. */
	if (pg_memory_is_all_zeros(pendingent,
							   sizeof(PgStat_RelationVacuumPending)))
		return true;

	if (!pgstat_lock_entry(entry_ref, nowait))
		return false;

	pgstat_accumulate_extvac_stats_relations(&shtabstats->stats,
											 &pendingent->counts);

	pgstat_unlock_entry(entry_ref);

	return true;
}

/*
 * Support function for the SQL-callable pgstat* functions. Returns the vacuum
 * collected statistics for one relation or NULL.
 */
PgStat_VacuumRelationCounts *
pgstat_fetch_stat_vacuum_tabentry(Oid relid, Oid dbid)
{
	return (PgStat_VacuumRelationCounts *)
		pgstat_fetch_entry(PGSTAT_KIND_VACUUM_RELATION, dbid, relid, NULL);
}
