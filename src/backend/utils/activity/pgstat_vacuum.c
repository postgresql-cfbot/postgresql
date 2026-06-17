/* -------------------------------------------------------------------------
 *
 * pgstat_vacuum.c
 *	  Implementation of extended vacuum statistics.
 *
 * This file contains the implementation of extended vacuum statistics. It is
 * kept separate from pgstat_relation.c and pgstat_database.c to reduce the
 * memory footprint of the regular relation and database statistics: vacuum
 * metrics require significantly more space per relation, so they live in their
 * own PGSTAT_KIND_VACUUM_RELATION and PGSTAT_KIND_VACUUM_DB stats kinds.
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

/* ----------
 * GUC parameters
 * ----------
 */
bool		pgstat_track_vacuum_statistics_for_relations = false;

#define ACCUMULATE_FIELD(field) (dst->field += src->field)
#define ACCUMULATE_SUBFIELD(substruct, field) (dst->substruct.field += src->substruct.field)

/*
 * Accumulate the counters that are common to heap relations, indexes and
 * databases.
 */
static void
pgstat_accumulate_common(PgStat_CommonCounts *dst, const PgStat_CommonCounts *src)
{
	ACCUMULATE_FIELD(wal_records);
	ACCUMULATE_FIELD(wal_fpi);
	ACCUMULATE_FIELD(wal_bytes);

	ACCUMULATE_FIELD(tuples_deleted);
	ACCUMULATE_FIELD(interrupts_count);
}

/*
 * Accumulate per-relation (heap or index) extended vacuum counters.
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

	pgstat_accumulate_common(&dst->common, &src->common);

	/*
	 * The wraparound failsafe is a per-relation flag (0/1), not a running
	 * count: reflect whether the latest vacuum of this relation engaged it,
	 * rather than summing across vacuums.
	 */
	dst->common.wraparound_failsafe_count = src->common.wraparound_failsafe_count;

	if (dst->type == PGSTAT_EXTVAC_TABLE)
	{
		ACCUMULATE_SUBFIELD(table, pages_scanned);
		ACCUMULATE_SUBFIELD(table, pages_removed);
		ACCUMULATE_SUBFIELD(table, missed_dead_pages);
			ACCUMULATE_SUBFIELD(table, tuples_frozen);
		ACCUMULATE_SUBFIELD(table, recently_dead_tuples);
		ACCUMULATE_SUBFIELD(table, missed_dead_tuples);
	}
	else if (dst->type == PGSTAT_EXTVAC_INDEX)
	{
		ACCUMULATE_SUBFIELD(index, pages_deleted);
	}
}

/*
 * Accumulate per-database extended vacuum counters.
 */
static void
pgstat_accumulate_extvac_stats_db(PgStat_VacuumDBCounts *dst,
								  PgStat_VacuumDBCounts *src)
{
	if (!pgstat_track_vacuum_statistics)
		return;

	pgstat_accumulate_common(&dst->common, &src->common);

	/*
	 * At the database level the failsafe is a count: how many relation vacuums
	 * engaged the wraparound failsafe.
	 */
	dst->common.wraparound_failsafe_count += src->common.wraparound_failsafe_count;
	dst->errors += src->errors;
}

/*
 * Report that the relation was just vacuumed, accumulating both its own
 * extended statistics and the database-wide aggregate.
 */
void
pgstat_report_vacuum_extstats(Oid tableoid, bool shared,
							  PgStat_VacuumRelationCounts *params)
{
	PgStat_EntryRef *entry_ref;
	PgStat_RelationVacuumPending *relpending;
	PgStat_VacuumDBCounts *dbpending;
	Oid			dboid = (shared ? InvalidOid : MyDatabaseId);

	if (!pgstat_track_vacuum_statistics)
		return;

	/*
	 * Accumulate into pending entries instead of taking a shared-stats lock
	 * here; pgstat_report_stat() flushes them to shared memory through the
	 * registered flush callbacks once the vacuum's transaction completes.
	 */

	/* Per-relation extended vacuum statistics */
	entry_ref = pgstat_prep_pending_entry(PGSTAT_KIND_VACUUM_RELATION,
										  dboid, tableoid, NULL);
	relpending = (PgStat_RelationVacuumPending *) entry_ref->pending;
	pgstat_accumulate_extvac_stats_relations(&relpending->counts, params);

	/* Database-wide aggregate of the same work */
	entry_ref = pgstat_prep_pending_entry(PGSTAT_KIND_VACUUM_DB,
										  dboid, InvalidOid, NULL);
	dbpending = (PgStat_VacuumDBCounts *) entry_ref->pending;
	pgstat_accumulate_common(&dbpending->common, &params->common);
	/* count this relation's failsafe flag into the database-wide total */
	dbpending->common.wraparound_failsafe_count += params->common.wraparound_failsafe_count;
}

/*
 * Report that a vacuum was interrupted by an error.
 *
 * This is a database-wide counter only: an interrupted vacuum aborts its
 * transaction, so reporting per-relation would require creating a relation
 * stats entry from the error path (which the aborting transaction may roll
 * back).  Called from the vacuum error callback, we therefore update shared
 * memory directly rather than going through pending entries, which might never
 * be flushed.
 *
 * The database id is InvalidOid for shared relations, just as in
 * pgstat_report_vacuum_extstats(); it must not be hard-coded to MyDatabaseId.
 */
void
pgstat_report_vacuum_error(bool shared)
{
	PgStat_EntryRef *entry_ref;
	PgStatShared_VacuumDB *shdbentry;
	Oid			dboid = (shared ? InvalidOid : MyDatabaseId);

	if (!pgstat_track_vacuum_statistics)
		return;

	entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_VACUUM_DB,
											dboid, InvalidOid, false);
	shdbentry = (PgStatShared_VacuumDB *) entry_ref->shared_stats;
	shdbentry->stats.common.interrupts_count++;
	pgstat_unlock_entry(entry_ref);
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
 * Flush out pending per-database extended vacuum stats for the entry.
 */
bool
pgstat_vacuum_db_flush_cb(PgStat_EntryRef *entry_ref, bool nowait)
{
	PgStatShared_VacuumDB *sharedent;
	PgStat_VacuumDBCounts *pendingent;

	pendingent = (PgStat_VacuumDBCounts *) entry_ref->pending;
	sharedent = (PgStatShared_VacuumDB *) entry_ref->shared_stats;

	if (pg_memory_is_all_zeros(pendingent, sizeof(PgStat_VacuumDBCounts)))
		return true;

	if (!pgstat_lock_entry(entry_ref, nowait))
		return false;

	pgstat_accumulate_extvac_stats_db(&sharedent->stats, pendingent);

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

/*
 * Support function for the SQL-callable pgstat* functions. Returns the vacuum
 * collected statistics for one database or NULL.
 */
PgStat_VacuumDBCounts *
pgstat_fetch_stat_vacuum_dbentry(Oid dbid)
{
	return (PgStat_VacuumDBCounts *)
		pgstat_fetch_entry(PGSTAT_KIND_VACUUM_DB, dbid, InvalidOid, NULL);
}
