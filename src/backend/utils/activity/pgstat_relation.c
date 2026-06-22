/* -------------------------------------------------------------------------
 *
 * pgstat_relation.c
 *	  Implementation of relation statistics.
 *
 * This file contains the implementation of relation statistics. It is kept
 * separate from pgstat.c to enforce the line between the statistics access /
 * storage implementation and the details about individual types of
 * statistics.
 *
 * Copyright (c) 2001-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_relation.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/pgstat_internal.h"
#include "utils/rel.h"
#include "utils/relmapper.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

/* Pending rewrite operations for stats copying */
typedef struct PgStat_PendingRewrite
{
	RelFileLocator old_locator;
	RelFileLocator new_locator;
	RelFileLocator original_locator;
	int			nest_level;		/* Transaction nesting level where rewrite
								 * occurred */
	struct PgStat_PendingRewrite *next;
} PgStat_PendingRewrite;

/* The pending rewrites list for current transaction */
static PgStat_PendingRewrite *pending_rewrites = NULL;

/* Record that's written to 2PC state file when pgstat state is persisted */
typedef struct TwoPhasePgStatRecord
{
	PgStat_Counter tuples_inserted; /* tuples inserted in xact */
	PgStat_Counter tuples_updated;	/* tuples updated in xact */
	PgStat_Counter tuples_deleted;	/* tuples deleted in xact */
	/* tuples i/u/d prior to truncate/drop */
	PgStat_Counter inserted_pre_truncdrop;
	PgStat_Counter updated_pre_truncdrop;
	PgStat_Counter deleted_pre_truncdrop;
	RelFileLocator locator;		/* table's rd_locator */
	bool		truncdropped;	/* was the relation truncated/dropped? */
	RelFileLocator rewrite_old_locator;
	int			rewrite_nest_level;
} TwoPhasePgStatRecord;


static PgStat_TableStatus *pgstat_prep_relation_pending(RelFileLocator locator);
static void add_tabstat_xact_level(PgStat_TableStatus *pgstat_info, int nest_level);
static void ensure_tabstat_xact_level(PgStat_TableStatus *pgstat_info);
static void save_truncdrop_counters(PgStat_TableXactStatus *trans, bool is_drop);
static void restore_truncdrop_counters(PgStat_TableXactStatus *trans);


/*
 * Copy stats between RelFileLocator. This is used for things like REINDEX
 * CONCURRENTLY.
 */
void
pgstat_copy_relation_stats(RelFileLocator dst, RelFileLocator src, bool increment)
{
	PgStat_StatTabEntry *srcstats;
	PgStatShared_Relation *dstshstats;
	PgStat_EntryRef *dst_ref;

	srcstats = (PgStat_StatTabEntry *) pgstat_fetch_entry(PGSTAT_KIND_RELATION,
														  src.dbOid,
														  RelFileLocatorToPgStatObjid(src),
														  NULL);
	if (!srcstats)
		return;

	dst_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RELATION,
										  dst.dbOid,
										  RelFileLocatorToPgStatObjid(dst),
										  false);

	dstshstats = (PgStatShared_Relation *) dst_ref->shared_stats;

	if (!increment)
		dstshstats->stats = *srcstats;
	else
	{
		/* Increment those statistics */
#define RELFSTAT_ACC(fld, stats_to_add) \
		(dstshstats->stats.fld += stats_to_add->fld)
		RELFSTAT_ACC(numscans, srcstats);
		RELFSTAT_ACC(tuples_returned, srcstats);
		RELFSTAT_ACC(tuples_fetched, srcstats);
		RELFSTAT_ACC(tuples_inserted, srcstats);
		RELFSTAT_ACC(tuples_updated, srcstats);
		RELFSTAT_ACC(tuples_deleted, srcstats);
		RELFSTAT_ACC(tuples_hot_updated, srcstats);
		RELFSTAT_ACC(tuples_newpage_updated, srcstats);
		RELFSTAT_ACC(live_tuples, srcstats);
		RELFSTAT_ACC(dead_tuples, srcstats);
		RELFSTAT_ACC(mod_since_analyze, srcstats);
		RELFSTAT_ACC(ins_since_vacuum, srcstats);
		RELFSTAT_ACC(blocks_fetched, srcstats);
		RELFSTAT_ACC(blocks_hit, srcstats);
		RELFSTAT_ACC(vacuum_count, srcstats);
		RELFSTAT_ACC(autovacuum_count, srcstats);
		RELFSTAT_ACC(analyze_count, srcstats);
		RELFSTAT_ACC(autoanalyze_count, srcstats);
		RELFSTAT_ACC(total_vacuum_time, srcstats);
		RELFSTAT_ACC(total_autovacuum_time, srcstats);
		RELFSTAT_ACC(total_analyze_time, srcstats);
		RELFSTAT_ACC(total_autoanalyze_time, srcstats);
#undef RELFSTAT_ACC

		/* Replace those statistics */
#define RELFSTAT_REP(fld, stats_to_rep) \
		(dstshstats->stats.fld = stats_to_rep->fld)
		RELFSTAT_REP(lastscan, srcstats);
		RELFSTAT_REP(last_vacuum_time, srcstats);
		RELFSTAT_REP(last_autovacuum_time, srcstats);
		RELFSTAT_REP(last_analyze_time, srcstats);
		RELFSTAT_REP(last_autoanalyze_time, srcstats);
#undef RELFSTAT_REP
	}

	pgstat_unlock_entry(dst_ref);
}

/*
 * Initialize a relcache entry to count access statistics.  Called whenever a
 * relation is opened.
 *
 * We assume that a relcache entry's pgstat_info field is zeroed by relcache.c
 * when the relcache entry is made; thereafter it is long-lived data.
 *
 * This does not create a reference to a stats entry in shared memory, nor
 * allocate memory for the pending stats. That happens in
 * pgstat_assoc_relation().
 */
void
pgstat_init_relation(Relation rel)
{
	char		relkind = rel->rd_rel->relkind;

	/*
	 * We only count stats for relations with storage and partitioned tables
	 * and we don't count stats generated during a rewrite.
	 */
	if ((!RELKIND_HAS_STORAGE(relkind) && relkind != RELKIND_PARTITIONED_TABLE) ||
		OidIsValid(rel->rd_rel->relrewrite))
	{
		rel->pgstat_enabled = false;
		rel->pgstat_info = NULL;
		return;
	}

	if (!pgstat_track_counts)
	{
		if (rel->pgstat_info)
			pgstat_unlink_relation(rel);

		/* We're not counting at all */
		rel->pgstat_enabled = false;
		rel->pgstat_info = NULL;
		return;
	}

	rel->pgstat_enabled = true;
}

/*
 * Prepare for statistics for this relation to be collected.
 *
 * This ensures we have a reference to the stats entry before stats can be
 * generated. That is important because a relation drop in another connection
 * could otherwise lead to the stats entry being dropped, which then later
 * would get recreated when flushing stats.
 *
 * This is separate from pgstat_init_relation() as it is not uncommon for
 * relcache entries to be opened without ever getting stats reported.
 */
void
pgstat_assoc_relation(Relation rel)
{
	RelFileLocator locator;
	PgStat_TableStatus *pgstat_info;

	Assert(rel->pgstat_enabled);
	Assert(rel->pgstat_info == NULL);

	/*
	 * Don't associate stats for relations without storage and non partitioned
	 * tables.
	 */
	if (!RELKIND_HAS_STORAGE(rel->rd_rel->relkind) &&
		rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		return;

	if (rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		locator = rel->rd_locator;
	else
	{
		/*
		 * Partitioned tables don't have storage, so construct a synthetic
		 * locator for statistics tracking. Use a reserved pseudo tablespace
		 * OID that cannot conflict with real tablespaces, and the relation
		 * OID as relNumber. This ensures no collision with regular relations
		 * even after OID wraparound.
		 */
		locator.dbOid = (rel->rd_rel->relisshared ? InvalidOid : MyDatabaseId);
		locator.spcOid = PSEUDO_PARTITION_TABLE_SPCOID;
		locator.relNumber = rel->rd_id;
	}

	/*
	 * If this relation was rewritten during the current transaction we may be
	 * reopening it with its new RelFileLocator. In that case, continue using
	 * the stats entry associated with the old locator rather than creating a
	 * new one. This ensures all stats from before and after the rewrite are
	 * tracked in a single entry which will be properly copied to the new
	 * locator at transaction commit.
	 */
	if (pending_rewrites != NULL)
	{
		PgStat_PendingRewrite *rewrite;

		for (rewrite = pending_rewrites; rewrite != NULL; rewrite = rewrite->next)
		{
			if (locator.dbOid == rewrite->new_locator.dbOid &&
				locator.spcOid == rewrite->new_locator.spcOid &&
				locator.relNumber == rewrite->new_locator.relNumber)
			{
				pgstat_info = pgstat_prep_relation_pending(rewrite->old_locator);
				goto found_entry;
			}
		}
	}

	/* Else find or make the PgStat_TableStatus entry, and update link */
	pgstat_info = pgstat_prep_relation_pending(locator);

found_entry:
	rel->pgstat_info = pgstat_info;

	/*
	 * For relations stats, we key by physical file location, not by relation
	 * OID. This means during operations like ALTER TYPE it's possible that
	 * the relation OID changes but the relfilenode stays the same (no actual
	 * rewrite needed). Unlink the old relation first.
	 */
	if (pgstat_info->relation != NULL &&
		pgstat_info->relation != rel)
	{
		pgstat_info->relation->pgstat_info = NULL;
		pgstat_info->relation = NULL;
	}

	/* don't allow link a stats to multiple relcache entries */
	Assert(pgstat_info->relation == NULL);

	/* mark this relation as the owner */
	pgstat_info->relation = rel;
}

/*
 * Break the mutual link between a relcache entry and pending stats entry.
 * This must be called whenever one end of the link is removed.
 */
void
pgstat_unlink_relation(Relation rel)
{
	/* remove the link to stats info if any */
	if (rel->pgstat_info == NULL)
		return;

	/* link sanity check */
	Assert(rel->pgstat_info->relation == rel);
	rel->pgstat_info->relation = NULL;
	rel->pgstat_info = NULL;
}

/*
 * Ensure that stats are dropped if transaction aborts.
 */
void
pgstat_create_relation(Relation rel)
{
	/* don't track stats for relations without storage */
	if (!RELKIND_HAS_STORAGE(rel->rd_rel->relkind))
		return;

	pgstat_create_transactional(PGSTAT_KIND_RELATION,
								rel->rd_locator.dbOid,
								RelFileLocatorToPgStatObjid(rel->rd_locator));
}

/*
 * Ensure that stats are dropped if transaction commits.
 */
void
pgstat_drop_relation(Relation rel)
{
	int			nest_level = GetCurrentTransactionNestLevel();
	PgStat_TableStatus *pgstat_info;
	bool		skip_transactional_drop = false;

	/* don't track stats for relations without storage */
	if (!RELKIND_HAS_STORAGE(rel->rd_rel->relkind))
		return;

	/* Check if this drop is part of a pending rewrite */
	if (pending_rewrites != NULL)
	{
		PgStat_PendingRewrite *rewrite;

		for (rewrite = pending_rewrites; rewrite != NULL; rewrite = rewrite->next)
		{
			if (rel->rd_locator.dbOid == rewrite->old_locator.dbOid &&
				rel->rd_locator.spcOid == rewrite->old_locator.spcOid &&
				rel->rd_locator.relNumber == rewrite->old_locator.relNumber)
			{
				skip_transactional_drop = true;
				break;
			}
		}
	}

	/*
	 * If it is part of a rewrite, drop its stats later, for example in
	 * AtEOXact_PgStat_Relations(), so skip it here.
	 */
	if (!skip_transactional_drop)
		pgstat_drop_transactional(PGSTAT_KIND_RELATION,
								  rel->rd_locator.dbOid,
								  RelFileLocatorToPgStatObjid(rel->rd_locator));

	if (!pgstat_should_count_relation(rel))
		return;

	/*
	 * Transactionally set counters to 0. That ensures that accesses to
	 * pg_stat_xact_all_tables inside the transaction show 0.
	 */
	pgstat_info = rel->pgstat_info;
	if (pgstat_info->trans &&
		pgstat_info->trans->nest_level == nest_level)
	{
		save_truncdrop_counters(pgstat_info->trans, true);
		pgstat_info->trans->tuples_inserted = 0;
		pgstat_info->trans->tuples_updated = 0;
		pgstat_info->trans->tuples_deleted = 0;
	}
}

/*
 * Report that the table was just vacuumed and flush IO statistics.
 */
void
pgstat_report_vacuum(Relation rel, PgStat_Counter livetuples,
					 PgStat_Counter deadtuples, TimestampTz starttime)
{
	PgStat_EntryRef *entry_ref;
	PgStatShared_Relation *shtabentry;
	PgStat_StatTabEntry *tabentry;
	TimestampTz ts;
	PgStat_Counter elapsedtime;
	RelFileLocator locator;

	if (!pgstat_track_counts)
		return;

	locator = rel->rd_locator;

	/* Store the data in the table's hash table entry. */
	ts = GetCurrentTimestamp();
	elapsedtime = TimestampDifferenceMilliseconds(starttime, ts);

	/* block acquiring lock for the same reason as pgstat_report_autovac() */
	entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RELATION, locator.dbOid,
											RelFileLocatorToPgStatObjid(locator),
											false);

	shtabentry = (PgStatShared_Relation *) entry_ref->shared_stats;
	tabentry = &shtabentry->stats;

	tabentry->live_tuples = livetuples;
	tabentry->dead_tuples = deadtuples;

	/*
	 * It is quite possible that a non-aggressive VACUUM ended up skipping
	 * various pages, however, we'll zero the insert counter here regardless.
	 * It's currently used only to track when we need to perform an "insert"
	 * autovacuum, which are mainly intended to freeze newly inserted tuples.
	 * Zeroing this may just mean we'll not try to vacuum the table again
	 * until enough tuples have been inserted to trigger another insert
	 * autovacuum.  An anti-wraparound autovacuum will catch any persistent
	 * stragglers.
	 */
	tabentry->ins_since_vacuum = 0;

	if (AmAutoVacuumWorkerProcess())
	{
		tabentry->last_autovacuum_time = ts;
		tabentry->autovacuum_count++;
		tabentry->total_autovacuum_time += elapsedtime;
	}
	else
	{
		tabentry->last_vacuum_time = ts;
		tabentry->vacuum_count++;
		tabentry->total_vacuum_time += elapsedtime;
	}

	pgstat_unlock_entry(entry_ref);

	/*
	 * Flush IO statistics now. pgstat_report_stat() will flush IO stats,
	 * however this will not be called until after an entire autovacuum cycle
	 * is done -- which will likely vacuum many relations -- or until the
	 * VACUUM command has processed all tables and committed.
	 */
	pgstat_flush_io(false);
	(void) pgstat_flush_backend(false, PGSTAT_BACKEND_FLUSH_IO);
}

/*
 * Report that the table was just analyzed and flush IO statistics.
 *
 * Caller must provide new live- and dead-tuples estimates, as well as a
 * flag indicating whether to reset the mod_since_analyze counter.
 */
void
pgstat_report_analyze(Relation rel,
					  PgStat_Counter livetuples, PgStat_Counter deadtuples,
					  bool resetcounter, TimestampTz starttime)
{
	PgStat_EntryRef *entry_ref;
	PgStatShared_Relation *shtabentry;
	PgStat_StatTabEntry *tabentry;
	TimestampTz ts;
	PgStat_Counter elapsedtime;
	RelFileLocator locator;

	if (!pgstat_track_counts)
		return;

	/*
	 * Unlike VACUUM, ANALYZE might be running inside a transaction that has
	 * already inserted and/or deleted rows in the target table. ANALYZE will
	 * have counted such rows as live or dead respectively. Because we will
	 * report our counts of such rows at transaction end, we should subtract
	 * off these counts from the update we're making now, else they'll be
	 * double-counted after commit.  (This approach also ensures that the
	 * shared stats entry ends up with the right numbers if we abort instead
	 * of committing.)
	 *
	 * Waste no time on partitioned tables, though.
	 */
	if (pgstat_should_count_relation(rel) &&
		rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
	{
		PgStat_TableXactStatus *trans;

		for (trans = rel->pgstat_info->trans; trans; trans = trans->upper)
		{
			livetuples -= trans->tuples_inserted - trans->tuples_deleted;
			deadtuples -= trans->tuples_updated + trans->tuples_deleted;
		}
		/* count stuff inserted by already-aborted subxacts, too */
		deadtuples -= rel->pgstat_info->counts.delta_dead_tuples;
		/* Since ANALYZE's counts are estimates, we could have underflowed */
		livetuples = Max(livetuples, 0);
		deadtuples = Max(deadtuples, 0);
	}

	/* Store the data in the table's hash table entry. */
	ts = GetCurrentTimestamp();
	elapsedtime = TimestampDifferenceMilliseconds(starttime, ts);

	if (rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		locator = rel->rd_locator;
	else
	{
		/*
		 * Partitioned tables don't have storage, so construct a synthetic
		 * locator for statistics tracking. Use a reserved pseudo tablespace
		 * OID that cannot conflict with real tablespaces, and the relation
		 * OID as relNumber. This ensures no collision with regular relations
		 * even after OID wraparound.
		 */
		locator.dbOid = (rel->rd_rel->relisshared ? InvalidOid : MyDatabaseId);
		locator.spcOid = PSEUDO_PARTITION_TABLE_SPCOID;
		locator.relNumber = rel->rd_id;
	}
	/* block acquiring lock for the same reason as pgstat_report_autovac() */
	entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RELATION,
											locator.dbOid,
											RelFileLocatorToPgStatObjid(locator),
											false);
	/* can't get dropped while accessed */
	Assert(entry_ref != NULL && entry_ref->shared_stats != NULL);

	shtabentry = (PgStatShared_Relation *) entry_ref->shared_stats;
	tabentry = &shtabentry->stats;

	tabentry->live_tuples = livetuples;
	tabentry->dead_tuples = deadtuples;

	/*
	 * If commanded, reset mod_since_analyze to zero.  This forgets any
	 * changes that were committed while the ANALYZE was in progress, but we
	 * have no good way to estimate how many of those there were.
	 */
	if (resetcounter)
		tabentry->mod_since_analyze = 0;

	if (AmAutoVacuumWorkerProcess())
	{
		tabentry->last_autoanalyze_time = ts;
		tabentry->autoanalyze_count++;
		tabentry->total_autoanalyze_time += elapsedtime;
	}
	else
	{
		tabentry->last_analyze_time = ts;
		tabentry->analyze_count++;
		tabentry->total_analyze_time += elapsedtime;
	}

	pgstat_unlock_entry(entry_ref);

	/* see pgstat_report_vacuum() */
	pgstat_flush_io(false);
	(void) pgstat_flush_backend(false, PGSTAT_BACKEND_FLUSH_IO);
}

/*
 * count a tuple insertion of n tuples
 */
void
pgstat_count_heap_insert(Relation rel, PgStat_Counter n)
{
	if (pgstat_should_count_relation(rel))
	{
		PgStat_TableStatus *pgstat_info = rel->pgstat_info;

		ensure_tabstat_xact_level(pgstat_info);
		pgstat_info->trans->tuples_inserted += n;
	}
}

/*
 * count a tuple update
 */
void
pgstat_count_heap_update(Relation rel, bool hot, bool newpage)
{
	Assert(!(hot && newpage));

	if (pgstat_should_count_relation(rel))
	{
		PgStat_TableStatus *pgstat_info = rel->pgstat_info;

		ensure_tabstat_xact_level(pgstat_info);
		pgstat_info->trans->tuples_updated++;

		/*
		 * tuples_hot_updated and tuples_newpage_updated counters are
		 * nontransactional, so just advance them
		 */
		if (hot)
			pgstat_info->counts.tuples_hot_updated++;
		else if (newpage)
			pgstat_info->counts.tuples_newpage_updated++;
	}
}

/*
 * count a tuple deletion
 */
void
pgstat_count_heap_delete(Relation rel)
{
	if (pgstat_should_count_relation(rel))
	{
		PgStat_TableStatus *pgstat_info = rel->pgstat_info;

		ensure_tabstat_xact_level(pgstat_info);
		pgstat_info->trans->tuples_deleted++;
	}
}

/*
 * update tuple counters due to truncate
 */
void
pgstat_count_truncate(Relation rel)
{
	if (pgstat_should_count_relation(rel))
	{
		PgStat_TableStatus *pgstat_info = rel->pgstat_info;

		ensure_tabstat_xact_level(pgstat_info);
		save_truncdrop_counters(pgstat_info->trans, false);
		pgstat_info->trans->tuples_inserted = 0;
		pgstat_info->trans->tuples_updated = 0;
		pgstat_info->trans->tuples_deleted = 0;
	}
}

/*
 * update dead-tuples count
 *
 * The semantics of this are that we are reporting the nontransactional
 * recovery of "delta" dead tuples; so delta_dead_tuples decreases
 * rather than increasing, and the change goes straight into the per-table
 * counter, not into transactional state.
 */
void
pgstat_update_heap_dead_tuples(Relation rel, int delta)
{
	if (pgstat_should_count_relation(rel))
	{
		PgStat_TableStatus *pgstat_info = rel->pgstat_info;

		pgstat_info->counts.delta_dead_tuples -= delta;
	}
}

/*
 * Support function for the SQL-callable pgstat* functions. Returns
 * the collected statistics for one table or NULL. NULL doesn't mean
 * that the table doesn't exist, just that there are no statistics, so the
 * caller is better off to report ZERO instead.
 */
PgStat_StatTabEntry *
pgstat_fetch_stat_tabentry(Oid relid)
{
	return pgstat_fetch_stat_tabentry_ext(relid, NULL);
}

PgStat_StatTabEntry *
pgstat_fetch_stat_tabentry_by_locator(RelFileLocator locator, bool *may_free)
{
	return (PgStat_StatTabEntry *) pgstat_fetch_entry(
													  PGSTAT_KIND_RELATION,
													  locator.dbOid,
													  RelFileLocatorToPgStatObjid(locator),
													  may_free);
}

/*
 * More efficient version of pgstat_fetch_stat_tabentry(), allowing to specify
 * whether the to-be-accessed table is a shared relation or not.  This version
 * also returns whether the caller can pfree() the result if desired.
 */
PgStat_StatTabEntry *
pgstat_fetch_stat_tabentry_ext(Oid reloid, bool *may_free)
{
	RelFileLocator locator;

	if (!pgstat_reloid_to_relfilelocator(reloid, &locator))
		return NULL;

	return pgstat_fetch_stat_tabentry_by_locator(locator, may_free);
}

/*
 * find any existing PgStat_TableStatus entry for rel
 *
 * Find any existing PgStat_TableStatus entry for rel_id in the current
 * database. If not found, try finding from shared tables.
 *
 * If an entry is found, copy it and increment the copy's counters with their
 * subtransaction counterparts, then return the copy.  The caller may need to
 * pfree() the copy.
 *
 * If no entry found, return NULL, don't create a new one.
 */
PgStat_TableStatus *
find_tabstat_entry(Oid rel_id)
{
	PgStat_EntryRef *entry_ref;
	PgStat_TableXactStatus *trans;
	PgStat_TableStatus *tabentry = NULL;
	PgStat_TableStatus *tablestatus = NULL;
	RelFileLocator locator;

	if (!pgstat_reloid_to_relfilelocator(rel_id, &locator))
		return NULL;

	entry_ref = pgstat_fetch_pending_entry(PGSTAT_KIND_RELATION,
										   locator.dbOid,
										   RelFileLocatorToPgStatObjid(locator));

	if (!entry_ref)
		return tablestatus;

	tabentry = (PgStat_TableStatus *) entry_ref->pending;
	tablestatus = palloc_object(PgStat_TableStatus);
	*tablestatus = *tabentry;

	/*
	 * Reset tablestatus->trans in the copy of PgStat_TableStatus as it may
	 * point to a shared memory area.  Its data is saved below, so removing it
	 * does not matter.
	 */
	tablestatus->trans = NULL;

	/*
	 * Live subtransaction counts are not included yet.  This is not a hot
	 * code path so reconcile tuples_inserted, tuples_updated and
	 * tuples_deleted even if the caller may not be interested in this data.
	 */
	for (trans = tabentry->trans; trans != NULL; trans = trans->upper)
	{
		tablestatus->counts.tuples_inserted += trans->tuples_inserted;
		tablestatus->counts.tuples_updated += trans->tuples_updated;
		tablestatus->counts.tuples_deleted += trans->tuples_deleted;
	}

	return tablestatus;
}

/*
 * Perform relation stats specific end-of-transaction work. Helper for
 * AtEOXact_PgStat.
 *
 * Transfer transactional insert/update counts into the base tabstat entries.
 * We don't bother to free any of the transactional state, since it's all in
 * TopTransactionContext and will go away anyway.
 */
void
AtEOXact_PgStat_Relations(PgStat_SubXactStatus *xact_state, bool isCommit)
{
	PgStat_TableXactStatus *trans;

	for (trans = xact_state->first; trans != NULL; trans = trans->next)
	{
		PgStat_TableStatus *tabstat;

		Assert(trans->nest_level == 1);
		Assert(trans->upper == NULL);
		tabstat = trans->parent;
		Assert(tabstat->trans == trans);
		/* restore pre-truncate/drop stats (if any) in case of aborted xact */
		if (!isCommit)
			restore_truncdrop_counters(trans);
		/* count attempted actions regardless of commit/abort */
		tabstat->counts.tuples_inserted += trans->tuples_inserted;
		tabstat->counts.tuples_updated += trans->tuples_updated;
		tabstat->counts.tuples_deleted += trans->tuples_deleted;
		if (isCommit)
		{
			tabstat->counts.truncdropped = trans->truncdropped;
			if (trans->truncdropped)
			{
				/* forget live/dead stats seen by backend thus far */
				tabstat->counts.delta_live_tuples = 0;
				tabstat->counts.delta_dead_tuples = 0;
			}
			/* insert adds a live tuple, delete removes one */
			tabstat->counts.delta_live_tuples +=
				trans->tuples_inserted - trans->tuples_deleted;
			/* update and delete each create a dead tuple */
			tabstat->counts.delta_dead_tuples +=
				trans->tuples_updated + trans->tuples_deleted;
			/* insert, update, delete each count as one change event */
			tabstat->counts.changed_tuples +=
				trans->tuples_inserted + trans->tuples_updated +
				trans->tuples_deleted;
		}
		else
		{
			/* inserted tuples are dead, deleted tuples are unaffected */
			tabstat->counts.delta_dead_tuples +=
				trans->tuples_inserted + trans->tuples_updated;
			/* an aborted xact generates no changed_tuple events */
		}
		tabstat->trans = NULL;
	}

	/* preserve the stats in case of rewrite */
	if (isCommit && pending_rewrites != NULL)
	{
		PgStat_PendingRewrite *rewrite;
		PgStat_PendingRewrite *prev = NULL;
		PgStat_PendingRewrite *current = pending_rewrites;
		PgStat_PendingRewrite *next;

		/* reverse the rewrites list to process in chronological order */
		while (current != NULL)
		{
			next = current->next;
			current->next = prev;
			prev = current;
			current = next;
		}

		/* now process rewrites in chronological order */
		for (rewrite = prev; rewrite != NULL; rewrite = rewrite->next)
		{
			PgStat_EntryRef *old_entry_ref;

			old_entry_ref = pgstat_fetch_pending_entry(PGSTAT_KIND_RELATION,
													   rewrite->old_locator.dbOid,
													   RelFileLocatorToPgStatObjid(rewrite->old_locator));

			if (old_entry_ref && old_entry_ref->pending)
				pgstat_relation_flush_cb(old_entry_ref, false);

			pgstat_copy_relation_stats(rewrite->new_locator,
									   rewrite->old_locator, true);

			/* drop old locator's stats */
			if (!pgstat_drop_entry(PGSTAT_KIND_RELATION,
								   rewrite->old_locator.dbOid,
								   RelFileLocatorToPgStatObjid(rewrite->old_locator)))
				pgstat_request_entry_refs_gc();
		}
	}

	pending_rewrites = NULL;
}

/*
 * Perform relation stats specific end-of-sub-transaction work. Helper for
 * AtEOSubXact_PgStat.
 *
 * Transfer transactional insert/update counts into the next higher
 * subtransaction state.
 */
void
AtEOSubXact_PgStat_Relations(PgStat_SubXactStatus *xact_state, bool isCommit, int nestDepth)
{
	PgStat_TableXactStatus *trans;
	PgStat_TableXactStatus *next_trans;

	/*
	 * If we don't commit then remove the associated rewrites if any, to keep
	 * the rewrite chain in sync with what will be eventually committed.
	 */
	if (!isCommit)
	{
		PgStat_PendingRewrite **rewrite_ptr = &pending_rewrites;

		while (*rewrite_ptr != NULL)
		{
			if ((*rewrite_ptr)->nest_level >= nestDepth)
			{
				PgStat_PendingRewrite *to_remove = *rewrite_ptr;

				*rewrite_ptr = (*rewrite_ptr)->next;
				pfree(to_remove);
			}
			else
			{
				rewrite_ptr = &((*rewrite_ptr)->next);
			}
		}
	}

	for (trans = xact_state->first; trans != NULL; trans = next_trans)
	{
		PgStat_TableStatus *tabstat;

		next_trans = trans->next;
		Assert(trans->nest_level == nestDepth);
		tabstat = trans->parent;
		Assert(tabstat->trans == trans);

		if (isCommit)
		{
			if (trans->upper && trans->upper->nest_level == nestDepth - 1)
			{
				if (trans->truncdropped)
				{
					/* propagate the truncate/drop status one level up */
					save_truncdrop_counters(trans->upper, false);
					/* replace upper xact stats with ours */
					trans->upper->tuples_inserted = trans->tuples_inserted;
					trans->upper->tuples_updated = trans->tuples_updated;
					trans->upper->tuples_deleted = trans->tuples_deleted;
				}
				else
				{
					trans->upper->tuples_inserted += trans->tuples_inserted;
					trans->upper->tuples_updated += trans->tuples_updated;
					trans->upper->tuples_deleted += trans->tuples_deleted;
				}
				tabstat->trans = trans->upper;
				pfree(trans);
			}
			else
			{
				/*
				 * When there isn't an immediate parent state, we can just
				 * reuse the record instead of going through a palloc/pfree
				 * pushup (this works since it's all in TopTransactionContext
				 * anyway).  We have to re-link it into the parent level,
				 * though, and that might mean pushing a new entry into the
				 * pgStatXactStack.
				 */
				PgStat_SubXactStatus *upper_xact_state;

				upper_xact_state = pgstat_get_xact_stack_level(nestDepth - 1);
				trans->next = upper_xact_state->first;
				upper_xact_state->first = trans;
				trans->nest_level = nestDepth - 1;
			}
		}
		else
		{
			/*
			 * On abort, update top-level tabstat counts, then forget the
			 * subtransaction
			 */

			/* first restore values obliterated by truncate/drop */
			restore_truncdrop_counters(trans);
			/* count attempted actions regardless of commit/abort */
			tabstat->counts.tuples_inserted += trans->tuples_inserted;
			tabstat->counts.tuples_updated += trans->tuples_updated;
			tabstat->counts.tuples_deleted += trans->tuples_deleted;
			/* inserted tuples are dead, deleted tuples are unaffected */
			tabstat->counts.delta_dead_tuples +=
				trans->tuples_inserted + trans->tuples_updated;
			tabstat->trans = trans->upper;
			pfree(trans);
		}
	}
}

/*
 * Generate 2PC records for all the pending transaction-dependent relation
 * stats.
 */
void
AtPrepare_PgStat_Relations(PgStat_SubXactStatus *xact_state)
{
	PgStat_TableXactStatus *trans;
	PgStat_PendingRewrite *rewrite;

	/*
	 * For each tabstat, find its matching rewrite and remove it from the
	 * pending rewrites list. This way, after processing all tabstats, pending
	 * rewrites will only contain rewrite only transactions.
	 */
	for (trans = xact_state->first; trans != NULL; trans = trans->next)
	{
		PgStat_TableStatus *tabstat PG_USED_FOR_ASSERTS_ONLY;
		TwoPhasePgStatRecord record;
		PgStat_PendingRewrite **rewrite_ptr;
		bool		found_rewrite = false;

		Assert(trans->nest_level == 1);
		Assert(trans->upper == NULL);
		tabstat = trans->parent;
		Assert(tabstat->trans == trans);

		record.tuples_inserted = trans->tuples_inserted;
		record.tuples_updated = trans->tuples_updated;
		record.tuples_deleted = trans->tuples_deleted;
		record.inserted_pre_truncdrop = trans->inserted_pre_truncdrop;
		record.updated_pre_truncdrop = trans->updated_pre_truncdrop;
		record.deleted_pre_truncdrop = trans->deleted_pre_truncdrop;

		if (tabstat->relation != NULL)
			record.locator = tabstat->relation->rd_locator;
		else
			record.locator = tabstat->locator;

		record.truncdropped = trans->truncdropped;
		record.rewrite_nest_level = 0;

		/*
		 * Look for a matching rewrite and remove it from pending rewrites. We
		 * check three possible matches:
		 *
		 * The new_locator when stats have been added after the rewrite. The
		 * old_locator when stats have been added before the rewrite but not
		 * after. The original_locator when this tabstat is part of a rewrite
		 * chain.
		 */
		rewrite_ptr = &pending_rewrites;
		while (*rewrite_ptr != NULL)
		{
			rewrite = *rewrite_ptr;

			if ((record.locator.dbOid == rewrite->new_locator.dbOid &&
				 record.locator.spcOid == rewrite->new_locator.spcOid &&
				 record.locator.relNumber == rewrite->new_locator.relNumber) ||
				(tabstat->locator.dbOid == rewrite->old_locator.dbOid &&
				 tabstat->locator.spcOid == rewrite->old_locator.spcOid &&
				 tabstat->locator.relNumber == rewrite->old_locator.relNumber) ||
				(tabstat->locator.dbOid == rewrite->original_locator.dbOid &&
				 tabstat->locator.spcOid == rewrite->original_locator.spcOid &&
				 tabstat->locator.relNumber == rewrite->original_locator.relNumber))
			{
				/*
				 * Found matching rewrite. Record the rewrite information and
				 * remove this rewrite from the list since it's now handled.
				 */
				record.rewrite_old_locator = rewrite->original_locator;
				record.rewrite_nest_level = rewrite->nest_level;
				record.locator = rewrite->new_locator;
				found_rewrite = true;

				/* Remove from pending_rewrites list */
				*rewrite_ptr = rewrite->next;
				pfree(rewrite);
				break;
			}
			else
			{
				/* Move to next rewrite in the list */
				rewrite_ptr = &(rewrite->next);
			}
		}

		/* If no rewrite found, clear the rewrite fields */
		if (!found_rewrite)
		{
			memset(&record.rewrite_old_locator, 0, sizeof(RelFileLocator));
		}

		RegisterTwoPhaseRecord(TWOPHASE_RM_PGSTAT_ID, 0,
							   &record, sizeof(TwoPhasePgStatRecord));
	}

	/*
	 * Now process any rewrites still pending. These are rewrite only
	 * transactions. We need to preserve their stats even though there's no
	 * tabstat entry for them.
	 */
	for (rewrite = pending_rewrites; rewrite != NULL; rewrite = rewrite->next)
	{
		TwoPhasePgStatRecord record;

		memset(&record, 0, sizeof(TwoPhasePgStatRecord));
		record.locator = rewrite->new_locator;
		record.rewrite_old_locator = rewrite->original_locator;
		record.rewrite_nest_level = rewrite->nest_level;
		record.truncdropped = false;

		RegisterTwoPhaseRecord(TWOPHASE_RM_PGSTAT_ID, 0,
							   &record, sizeof(TwoPhasePgStatRecord));
	}

	pending_rewrites = NULL;
}

/*
 * All we need do here is unlink the transaction stats state from the
 * nontransactional state.  The nontransactional action counts will be
 * reported to the stats system immediately, while the effects on live and
 * dead tuple counts are preserved in the 2PC state file.
 *
 * Note: AtEOXact_PgStat_Relations is not called during PREPARE.
 */
void
PostPrepare_PgStat_Relations(PgStat_SubXactStatus *xact_state)
{
	PgStat_TableXactStatus *trans;

	for (trans = xact_state->first; trans != NULL; trans = trans->next)
	{
		PgStat_TableStatus *tabstat;

		tabstat = trans->parent;
		tabstat->trans = NULL;
	}

	pending_rewrites = NULL;
}

/*
 * 2PC processing routine for COMMIT PREPARED case.
 *
 * Load the saved counts into our local pgstats state.
 */
void
pgstat_twophase_postcommit(FullTransactionId fxid, uint16 info,
						   void *recdata, uint32 len)
{
	TwoPhasePgStatRecord *rec = (TwoPhasePgStatRecord *) recdata;
	PgStat_TableStatus *pgstat_info;

	/* Find or create a tabstat entry for the rel */
	pgstat_info = pgstat_prep_relation_pending(rec->locator);

	/* Same math as in AtEOXact_PgStat, commit case */
	pgstat_info->counts.tuples_inserted += rec->tuples_inserted;
	pgstat_info->counts.tuples_updated += rec->tuples_updated;
	pgstat_info->counts.tuples_deleted += rec->tuples_deleted;
	pgstat_info->counts.truncdropped = rec->truncdropped;
	if (rec->truncdropped)
	{
		/* forget live/dead stats seen by backend thus far */
		pgstat_info->counts.delta_live_tuples = 0;
		pgstat_info->counts.delta_dead_tuples = 0;
	}
	pgstat_info->counts.delta_live_tuples +=
		rec->tuples_inserted - rec->tuples_deleted;
	pgstat_info->counts.delta_dead_tuples +=
		rec->tuples_updated + rec->tuples_deleted;
	pgstat_info->counts.changed_tuples +=
		rec->tuples_inserted + rec->tuples_updated +
		rec->tuples_deleted;

	if (rec->rewrite_nest_level > 0)
	{
		PgStat_EntryRef *old_entry_ref;

		/* Flush any pending stats for old locator first */
		old_entry_ref = pgstat_fetch_pending_entry(PGSTAT_KIND_RELATION,
												   rec->rewrite_old_locator.dbOid,
												   RelFileLocatorToPgStatObjid(rec->rewrite_old_locator));

		if (old_entry_ref && old_entry_ref->pending)
			pgstat_relation_flush_cb(old_entry_ref, false);

		/* Copy stats from old to new locator */
		pgstat_copy_relation_stats(rec->locator, rec->rewrite_old_locator,
								   true);

		/* Drop old locator's stats */
		if (!pgstat_drop_entry(PGSTAT_KIND_RELATION,
							   rec->rewrite_old_locator.dbOid,
							   RelFileLocatorToPgStatObjid(rec->rewrite_old_locator)))
			pgstat_request_entry_refs_gc();
	}
}

/*
 * 2PC processing routine for ROLLBACK PREPARED case.
 *
 * Load the saved counts into our local pgstats state, but treat them
 * as aborted.
 */
void
pgstat_twophase_postabort(FullTransactionId fxid, uint16 info,
						  void *recdata, uint32 len)
{
	TwoPhasePgStatRecord *rec = (TwoPhasePgStatRecord *) recdata;
	PgStat_TableStatus *pgstat_info;
	RelFileLocator target_locator;

	/*
	 * For aborted transactions with rewrites (like TRUNCATE), we need to
	 * restore stats to the old locator, not the new one. The new locator
	 * should be dropped since the rewrite is being rolled back.
	 */
	if (rec->rewrite_nest_level > 0)
	{
		/* Use the old locator */
		target_locator = rec->rewrite_old_locator;
	}
	else
	{
		/* No rewrite, use the original locator */
		target_locator = rec->locator;
	}

	/* Find or create a tabstat entry for the target locator */
	pgstat_info = pgstat_prep_relation_pending(target_locator);

	/* Same math as in AtEOXact_PgStat, abort case */
	if (rec->truncdropped)
	{
		rec->tuples_inserted = rec->inserted_pre_truncdrop;
		rec->tuples_updated = rec->updated_pre_truncdrop;
		rec->tuples_deleted = rec->deleted_pre_truncdrop;
	}
	pgstat_info->counts.tuples_inserted += rec->tuples_inserted;
	pgstat_info->counts.tuples_updated += rec->tuples_updated;
	pgstat_info->counts.tuples_deleted += rec->tuples_deleted;
	pgstat_info->counts.delta_dead_tuples +=
		rec->tuples_inserted + rec->tuples_updated;
}

/*
 * Flush out pending stats for the entry
 *
 * If nowait is true and the lock could not be immediately acquired, returns
 * false without flushing the entry.  Otherwise returns true.
 *
 * Some of the stats are copied to the corresponding pending database stats
 * entry when successfully flushing.
 */
bool
pgstat_relation_flush_cb(PgStat_EntryRef *entry_ref, bool nowait)
{
	Oid			dboid;
	PgStat_TableStatus *lstats; /* pending stats entry  */
	PgStatShared_Relation *shtabstats;
	PgStat_StatTabEntry *tabentry;	/* table entry of shared stats */
	PgStat_StatDBEntry *dbentry;	/* pending database entry */

	dboid = entry_ref->shared_entry->key.dboid;
	lstats = (PgStat_TableStatus *) entry_ref->pending;
	shtabstats = (PgStatShared_Relation *) entry_ref->shared_stats;

	/*
	 * Ignore entries that didn't accumulate any actual counts, such as
	 * indexes that were opened by the planner but not used.
	 */
	if (pg_memory_is_all_zeros(&lstats->counts,
							   sizeof(struct PgStat_TableCounts)))
		return true;

	if (!pgstat_lock_entry(entry_ref, nowait))
		return false;

	/* add the values to the shared entry. */
	tabentry = &shtabstats->stats;

	tabentry->numscans += lstats->counts.numscans;
	if (lstats->counts.numscans)
	{
		TimestampTz t;

		/*
		 * Checking the transaction state due to the flush call in
		 * pgstat_twophase_postcommit() that would break the assertion on the
		 * state in GetCurrentTransactionStopTimestamp().
		 */
		if (!IsTransactionState())
			t = GetCurrentTransactionStopTimestamp();
		else
			t = GetCurrentTimestamp();

		if (t > tabentry->lastscan)
			tabentry->lastscan = t;
	}
	tabentry->tuples_returned += lstats->counts.tuples_returned;
	tabentry->tuples_fetched += lstats->counts.tuples_fetched;
	tabentry->tuples_inserted += lstats->counts.tuples_inserted;
	tabentry->tuples_updated += lstats->counts.tuples_updated;
	tabentry->tuples_deleted += lstats->counts.tuples_deleted;
	tabentry->tuples_hot_updated += lstats->counts.tuples_hot_updated;
	tabentry->tuples_newpage_updated += lstats->counts.tuples_newpage_updated;

	/*
	 * If table was truncated/dropped, first reset the live/dead counters.
	 */
	if (lstats->counts.truncdropped)
	{
		tabentry->live_tuples = 0;
		tabentry->dead_tuples = 0;
		tabentry->ins_since_vacuum = 0;
	}

	tabentry->live_tuples += lstats->counts.delta_live_tuples;
	tabentry->dead_tuples += lstats->counts.delta_dead_tuples;
	tabentry->mod_since_analyze += lstats->counts.changed_tuples;

	/*
	 * Using tuples_inserted to update ins_since_vacuum does mean that we'll
	 * track aborted inserts too.  This isn't ideal, but otherwise probably
	 * not worth adding an extra field for.  It may just amount to autovacuums
	 * triggering for inserts more often than they maybe should, which is
	 * probably not going to be common enough to be too concerned about here.
	 */
	tabentry->ins_since_vacuum += lstats->counts.tuples_inserted;

	tabentry->blocks_fetched += lstats->counts.blocks_fetched;
	tabentry->blocks_hit += lstats->counts.blocks_hit;

	/* Clamp live_tuples in case of negative delta_live_tuples */
	tabentry->live_tuples = Max(tabentry->live_tuples, 0);
	/* Likewise for dead_tuples */
	tabentry->dead_tuples = Max(tabentry->dead_tuples, 0);

	pgstat_unlock_entry(entry_ref);

	/* The entry was successfully flushed, add the same to database stats */
	dbentry = pgstat_prep_database_pending(dboid);
	dbentry->tuples_returned += lstats->counts.tuples_returned;
	dbentry->tuples_fetched += lstats->counts.tuples_fetched;
	dbentry->tuples_inserted += lstats->counts.tuples_inserted;
	dbentry->tuples_updated += lstats->counts.tuples_updated;
	dbentry->tuples_deleted += lstats->counts.tuples_deleted;
	dbentry->blocks_fetched += lstats->counts.blocks_fetched;
	dbentry->blocks_hit += lstats->counts.blocks_hit;

	return true;
}

void
pgstat_relation_delete_pending_cb(PgStat_EntryRef *entry_ref)
{
	PgStat_TableStatus *pending = (PgStat_TableStatus *) entry_ref->pending;

	if (pending->relation)
		pgstat_unlink_relation(pending->relation);
}

void
pgstat_relation_reset_timestamp_cb(PgStatShared_Common *header, TimestampTz ts)
{
	((PgStatShared_Relation *) header)->stats.stat_reset_time = ts;
}

/*
 * Find or create a PgStat_TableStatus entry for rel. New entry is created and
 * initialized if not exists.
 */
static PgStat_TableStatus *
pgstat_prep_relation_pending(RelFileLocator locator)
{
	PgStat_EntryRef *entry_ref;
	PgStat_TableStatus *pending;
	uint64		objid;

	objid = RelFileLocatorToPgStatObjid(locator);

	entry_ref = pgstat_prep_pending_entry(PGSTAT_KIND_RELATION,
										  locator.dbOid,
										  objid, NULL);

	pending = entry_ref->pending;
	pending->id = objid;
	pending->locator = locator;

	return pending;
}

/*
 * add a new (sub)transaction state record
 */
static void
add_tabstat_xact_level(PgStat_TableStatus *pgstat_info, int nest_level)
{
	PgStat_SubXactStatus *xact_state;
	PgStat_TableXactStatus *trans;

	/*
	 * If this is the first rel to be modified at the current nest level, we
	 * first have to push a transaction stack entry.
	 */
	xact_state = pgstat_get_xact_stack_level(nest_level);

	/* Now make a per-table stack entry */
	trans = (PgStat_TableXactStatus *)
		MemoryContextAllocZero(TopTransactionContext,
							   sizeof(PgStat_TableXactStatus));
	trans->nest_level = nest_level;
	trans->upper = pgstat_info->trans;
	trans->parent = pgstat_info;
	trans->next = xact_state->first;
	xact_state->first = trans;
	pgstat_info->trans = trans;
}

/*
 * Add a new (sub)transaction record if needed.
 */
static void
ensure_tabstat_xact_level(PgStat_TableStatus *pgstat_info)
{
	int			nest_level = GetCurrentTransactionNestLevel();

	if (pgstat_info->trans == NULL ||
		pgstat_info->trans->nest_level != nest_level)
		add_tabstat_xact_level(pgstat_info, nest_level);
}

/*
 * Whenever a table is truncated/dropped, we save its i/u/d counters so that
 * they can be cleared, and if the (sub)xact that executed the truncate/drop
 * later aborts, the counters can be restored to the saved (pre-truncate/drop)
 * values.
 *
 * Note that for truncate we do this on the first truncate in any particular
 * subxact level only.
 */
static void
save_truncdrop_counters(PgStat_TableXactStatus *trans, bool is_drop)
{
	if (!trans->truncdropped || is_drop)
	{
		trans->inserted_pre_truncdrop = trans->tuples_inserted;
		trans->updated_pre_truncdrop = trans->tuples_updated;
		trans->deleted_pre_truncdrop = trans->tuples_deleted;
		trans->truncdropped = true;
	}
}

/*
 * restore counters when a truncate aborts
 */
static void
restore_truncdrop_counters(PgStat_TableXactStatus *trans)
{
	if (trans->truncdropped)
	{
		trans->tuples_inserted = trans->inserted_pre_truncdrop;
		trans->tuples_updated = trans->updated_pre_truncdrop;
		trans->tuples_deleted = trans->deleted_pre_truncdrop;
	}
}

/*
 * Convert a relation OID to its corresponding RelFileLocator for statistics
 * tracking purposes.
 *
 * Returns true on success, false if the relation doesn't need statistics
 * tracking.
 *
 * For partitioned tables, constructs a synthetic locator using the relation
 * OID as relNumber, since they don't have storage.
 */
bool
pgstat_reloid_to_relfilelocator(Oid reloid, RelFileLocator *locator)
{
	HeapTuple	tuple;
	Form_pg_class relform;
	bool		result = true;

	/* get the relation's tuple from pg_class */
	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(reloid));

	if (!HeapTupleIsValid(tuple))
		return false;

	relform = (Form_pg_class) GETSTRUCT(tuple);

	/* skip relations without storage and non partitioned tables */
	if (!RELKIND_HAS_STORAGE(relform->relkind) &&
		relform->relkind != RELKIND_PARTITIONED_TABLE)
	{
		ReleaseSysCache(tuple);
		return false;
	}

	if (relform->relkind != RELKIND_PARTITIONED_TABLE)
	{
		/* build the RelFileLocator */
		locator->relNumber = relform->relfilenode;
		locator->spcOid = relform->reltablespace;

		/* handle default tablespace */
		if (!OidIsValid(locator->spcOid))
			locator->spcOid = MyDatabaseTableSpace;

		/* handle dbOid for global vs local relations */
		if (locator->spcOid == GLOBALTABLESPACE_OID)
			locator->dbOid = InvalidOid;
		else
			locator->dbOid = MyDatabaseId;

		/* handle mapped relations */
		if (!RelFileNumberIsValid(locator->relNumber))
		{
			locator->relNumber = RelationMapOidToFilenumber(reloid,
															relform->relisshared);
			if (!RelFileNumberIsValid(locator->relNumber))
			{
				ReleaseSysCache(tuple);
				return false;
			}
		}
	}
	else
	{
		/*
		 * Partitioned tables don't have storage, so construct a synthetic
		 * locator for statistics tracking. Use a reserved pseudo tablespace
		 * OID that cannot conflict with real tablespaces, and the relation
		 * OID as relNumber. This ensures no collision with regular relations
		 * even after OID wraparound.
		 */
		locator->dbOid = (relform->relisshared ? InvalidOid : MyDatabaseId);
		locator->spcOid = PSEUDO_PARTITION_TABLE_SPCOID;
		locator->relNumber = relform->oid;
	}

	ReleaseSysCache(tuple);
	return result;
}

/*
 * Mark that a relation rewrite has occurred, preserving the original locator
 * so stats can be copied at transaction commit.
 */
void
pgstat_mark_rewrite(RelFileLocator old_locator, RelFileLocator new_locator)
{
	PgStat_PendingRewrite *rewrite;
	PgStat_PendingRewrite *existing;
	RelFileLocator original_locator = old_locator;

	for (existing = pending_rewrites; existing != NULL; existing = existing->next)
	{
		if (old_locator.dbOid == existing->new_locator.dbOid &&
			old_locator.spcOid == existing->new_locator.spcOid &&
			old_locator.relNumber == existing->new_locator.relNumber)
		{
			original_locator = existing->original_locator;
			break;
		}
	}

	/* Allocate in TopTransactionContext memory context */
	rewrite = MemoryContextAlloc(TopTransactionContext,
								 sizeof(PgStat_PendingRewrite));

	rewrite->old_locator = old_locator;
	rewrite->new_locator = new_locator;
	rewrite->original_locator = original_locator;
	rewrite->nest_level = GetCurrentTransactionNestLevel();

	/* Add to the list */
	rewrite->next = pending_rewrites;
	pending_rewrites = rewrite;
}

void
pgstat_clear_rewrite(void)
{
	pending_rewrites = NULL;
}
