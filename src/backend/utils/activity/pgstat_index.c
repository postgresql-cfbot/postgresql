/* -------------------------------------------------------------------------
 *
 * pgstat_index.c
 *	  Implementation of index statistics.
 *
 * This file contains the implementation of function index. It is kept
 * separate from pgstat.c to enforce the line between the statistics access /
 * storage implementation and the details about individual types of
 * statistics.
 *
 * Copyright (c) 2001-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_index.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "utils/pgstat_internal.h"
#include "utils/rel.h"
#include "catalog/catalog.h"

static PgStat_IndexStatus *pgstat_prep_index_pending(Oid rel_id, bool isshared);

/*
 * Copy stats between indexes. This is used for things like REINDEX
 * CONCURRENTLY.
 */
void
pgstat_copy_index_stats(Relation dst, Relation src)
{
	PgStat_StatIndEntry *srcstats;
	PgStatShared_Index *dstshstats;
	PgStat_EntryRef *dst_ref;

	srcstats = pgstat_fetch_stat_indentry_ext(src->rd_rel->relisshared,
											  RelationGetRelid(src));
	if (!srcstats)
		return;

	dst_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_INDEX,
										  dst->rd_rel->relisshared ? InvalidOid : MyDatabaseId,
										  RelationGetRelid(dst),
										  false);

	dstshstats = (PgStatShared_Index *) dst_ref->shared_stats;
	dstshstats->stats = *srcstats;

	pgstat_unlock_entry(dst_ref);
}

/*
 * Initialize a relcache entry to count access statistics.  Called whenever an
 * index is opened.
 *
 * We assume that a relcache entry's pgstatind_info field is zeroed by relcache.c
 * when the relcache entry is made; thereafter it is long-lived data.
 *
 * This does not create a reference to a stats entry in shared memory, nor
 * allocate memory for the pending stats. That happens in
 * pgstat_assoc_index().
 */
void
pgstat_init_index(Relation rel)
{
	/*
	 * We only count stats for indexes
	 */
	Assert(rel->rd_rel->relkind == RELKIND_INDEX);

	if (!pgstat_track_counts)
	{
		if (rel->pgstatind_info != NULL)
			pgstat_unlink_index(rel);

		/* We're not counting at all */
		rel->pgstat_enabled = false;
		rel->pgstatind_info = NULL;
		return;
	}

	rel->pgstat_enabled = true;
}

/*
 * Prepare for statistics for this index to be collected.
 *
 * This ensures we have a reference to the stats entry before stats can be
 * generated. That is important because an index drop in another
 * connection could otherwise lead to the stats entry being dropped, which then
 * later would get recreated when flushing stats.
 *
 * This is separate from pgstat_init_index() as it is not uncommon for
 * relcache entries to be opened without ever getting stats reported.
 */
void
pgstat_assoc_index(Relation rel)
{
	Assert(rel->pgstat_enabled);
	Assert(rel->pgstatind_info == NULL);

	/* Else find or make the PgStat_IndexStatus entry, and update link */
	rel->pgstatind_info = pgstat_prep_index_pending(RelationGetRelid(rel),
													rel->rd_rel->relisshared);

	/* don't allow link a stats to multiple relcache entries */
	Assert(rel->pgstatind_info->relation == NULL);

	/* mark this relation as the owner */
	rel->pgstatind_info->relation = rel;
}

/*
 * Break the mutual link between a relcache entry and pending index stats entry.
 * This must be called whenever one end of the link is removed.
 */
void
pgstat_unlink_index(Relation rel)
{

	if (rel->pgstatind_info == NULL)
		return;

	/* link sanity check for the index stats */
	if (rel->pgstatind_info)
	{
		Assert(rel->pgstatind_info->relation == rel);
		rel->pgstatind_info->relation = NULL;
		rel->pgstatind_info = NULL;
	}
}

/*
 * Ensure that index stats are dropped if transaction aborts.
 */
void
pgstat_create_index(Relation rel)
{
	pgstat_create_transactional(PGSTAT_KIND_INDEX,
								rel->rd_rel->relisshared ? InvalidOid : MyDatabaseId,
								RelationGetRelid(rel));
}

/*
 * Ensure that index stats are dropped if transaction commits.
 */
void
pgstat_drop_index(Relation rel)
{
	pgstat_drop_transactional(PGSTAT_KIND_INDEX,
							  rel->rd_rel->relisshared ? InvalidOid : MyDatabaseId,
							  RelationGetRelid(rel));
}

/*
 * Support function for the SQL-callable pgstat* functions. Returns
 * the collected statistics for one index or NULL. NULL doesn't mean
 * that the index doesn't exist, just that there are no statistics, so the
 * caller is better off to report ZERO instead.
 */
PgStat_StatIndEntry *
pgstat_fetch_stat_indentry(Oid relid)
{
	return pgstat_fetch_stat_indentry_ext(IsSharedRelation(relid), relid);
}

/*
 * More efficient version of pgstat_fetch_stat_indentry(), allowing to specify
 * whether the to-be-accessed index is shared or not.
 */
PgStat_StatIndEntry *
pgstat_fetch_stat_indentry_ext(bool shared, Oid reloid)
{
	Oid			dboid = (shared ? InvalidOid : MyDatabaseId);

	return (PgStat_StatIndEntry *)
		pgstat_fetch_entry(PGSTAT_KIND_INDEX, dboid, reloid);
}

/*
 * find any existing PgStat_IndexStatus entry for rel
 *
 * Find any existing PgStat_IndexStatus entry for rel_id in the current
 * database. If not found, try finding from shared indexes.
 *
 * If no entry found, return NULL, don't create a new one
 */
PgStat_IndexStatus *
find_indstat_entry(Oid rel_id)
{
	PgStat_EntryRef *entry_ref;

	entry_ref = pgstat_fetch_pending_entry(PGSTAT_KIND_INDEX, MyDatabaseId, rel_id);
	if (!entry_ref)
		entry_ref = pgstat_fetch_pending_entry(PGSTAT_KIND_INDEX, InvalidOid, rel_id);

	if (entry_ref)
		return entry_ref->pending;
	return NULL;
}

/*
 * Flush out pending stats for the entry
 *
 * If nowait is true, this function returns false if lock could not
 * immediately acquired, otherwise true is returned.
 *
 * Some of the stats are copied to the corresponding pending database stats
 * entry when successfully flushing.
 */
bool
pgstat_index_flush_cb(PgStat_EntryRef *entry_ref, bool nowait)
{
	static const PgStat_IndexCounts all_zeroes;
	Oid			dboid;

	PgStat_IndexStatus *lstats; /* pending stats entry  */
	PgStatShared_Index *shrelcomstats;
	PgStat_StatIndEntry *indentry;	/* index entry of shared stats */
	PgStat_StatDBEntry *dbentry;	/* pending database entry */

	dboid = entry_ref->shared_entry->key.dboid;
	lstats = (PgStat_IndexStatus *) entry_ref->pending;
	shrelcomstats = (PgStatShared_Index *) entry_ref->shared_stats;

	/*
	 * Ignore entries that didn't accumulate any actual counts, such as
	 * indexes that were opened by the planner but not used.
	 */
	if (memcmp(&lstats->i_counts, &all_zeroes,
			   sizeof(PgStat_IndexCounts)) == 0)
	{
		return true;
	}

	if (!pgstat_lock_entry(entry_ref, nowait))
		return false;

	/* add the values to the shared entry. */
	indentry = &shrelcomstats->stats;

	indentry->numscans += lstats->i_counts.i_numscans;

	if (lstats->i_counts.i_numscans)
	{
		TimestampTz t = GetCurrentTransactionStopTimestamp();

		if (t > indentry->lastscan)
			indentry->lastscan = t;
	}
	indentry->tuples_returned += lstats->i_counts.i_tuples_returned;
	indentry->tuples_fetched += lstats->i_counts.i_tuples_fetched;
	indentry->blocks_fetched += lstats->i_counts.i_blocks_fetched;
	indentry->blocks_hit += lstats->i_counts.i_blocks_hit;

	pgstat_unlock_entry(entry_ref);

	/* The entry was successfully flushed, add the same to database stats */
	dbentry = pgstat_prep_database_pending(dboid);
	dbentry->tuples_returned += lstats->i_counts.i_tuples_returned;
	dbentry->tuples_fetched += lstats->i_counts.i_tuples_fetched;
	dbentry->blocks_fetched += lstats->i_counts.i_blocks_fetched;
	dbentry->blocks_hit += lstats->i_counts.i_blocks_hit;

	return true;
}

void
pgstat_index_delete_pending_cb(PgStat_EntryRef *entry_ref)
{
	PgStat_IndexStatus *pending = (PgStat_IndexStatus *) entry_ref->pending;

	if (pending->relation)
		pgstat_unlink_index(pending->relation);
}

/*
 * Find or create a PgStat_IndexStatus entry for rel. New entry is created and
 * initialized if not exists.
 */
static PgStat_IndexStatus *
pgstat_prep_index_pending(Oid rel_id, bool isshared)
{
	PgStat_EntryRef *entry_ref;
	PgStat_IndexStatus *pending;

	entry_ref = pgstat_prep_pending_entry(PGSTAT_KIND_INDEX,
										  isshared ? InvalidOid : MyDatabaseId,
										  rel_id, NULL);
	pending = entry_ref->pending;
	pending->r_id = rel_id;
	pending->r_shared = isshared;

	return pending;
}
