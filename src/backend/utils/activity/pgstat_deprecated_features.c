/* -------------------------------------------------------------------------
 *
 * pgstat_deprecated_features.c
 *	  Implementation of deprecated feature usage statistics.
 *
 * This file contains the implementation of deprecated feature usage
 * statistics.  It is kept separate from pgstat.c to enforce the line
 * between the statistics access / storage implementation and the details
 * about individual types of statistics.
 *
 * Copyright (c) 2001-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_deprecated_features.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/pgstat_internal.h"
#include "utils/timestamp.h"


/*
 * Names of the deprecated features that we keep usage statistics for.
 * Entries are one-to-one with the PgStat_DeprecatedFeature enum and must
 * be kept in the same order, sorted by name.
 */
static const char *const pgstat_deprecated_feature_names[] = {
	[PGSTAT_DEPRECATED_FEATURE_GLOBAL_TEMPORARY_TABLE] = "global_temporary_table",
	[PGSTAT_DEPRECATED_FEATURE_LOCAL_TEMPORARY_TABLE] = "local_temporary_table",
	[PGSTAT_DEPRECATED_FEATURE_MD5_PASSWORD_AUTH] = "md5_password_auth",
	[PGSTAT_DEPRECATED_FEATURE_MD5_PASSWORD_SET] = "md5_password_set",
};

StaticAssertDecl(lengthof(pgstat_deprecated_feature_names) == PGSTAT_NUM_DEPRECATED_FEATURES,
				 "pgstat_deprecated_feature_names[] inconsistent with PgStat_DeprecatedFeature");

/*
 * Deprecated feature usage counts waiting to be flushed out.  We assume
 * this variable inits to zeroes.  Entries are one-to-one with
 * pgstat_deprecated_feature_names[].
 */
static PgStat_DeprecatedFeatureStats pending_DeprecatedFeatureStats[PGSTAT_NUM_DEPRECATED_FEATURES];
static bool have_deprecated_feature_stats = false;


/*
 * Count one use of a deprecated feature.
 */
void
pgstat_count_deprecated_feature(PgStat_DeprecatedFeature feature)
{
	PgStat_DeprecatedFeatureStats *pendingent;

	pgstat_assert_is_up();

	/*
	 * The postmaster should never count deprecated feature usage; if it did,
	 * the counts would be duplicated into child processes via fork().
	 */
	Assert(IsUnderPostmaster || !IsPostmasterEnvironment);

	Assert(feature < PGSTAT_NUM_DEPRECATED_FEATURES);

	pendingent = &pending_DeprecatedFeatureStats[feature];
	pendingent->usage_count++;
	pendingent->last_used = GetCurrentTimestamp();

	have_deprecated_feature_stats = true;
	pgstat_report_fixed = true;
}

/*
 * Support function for the SQL-callable pgstat* functions.  Returns
 * a pointer to the deprecated feature usage statistics struct.
 */
PgStat_DeprecatedFeatureStats *
pgstat_fetch_deprecated_features(void)
{
	pgstat_snapshot_fixed(PGSTAT_KIND_DEPRECATED_FEATURES);

	return pgStatLocal.snapshot.deprecated_features;
}

/*
 * Returns the name of a deprecated feature for an index.  The index may be
 * above PGSTAT_NUM_DEPRECATED_FEATURES, in which case this returns NULL.
 * This allows writing code that does not know the number of entries in
 * advance.
 */
const char *
pgstat_get_deprecated_feature_name(int idx)
{
	if (idx < 0 || idx >= PGSTAT_NUM_DEPRECATED_FEATURES)
		return NULL;

	return pgstat_deprecated_feature_names[idx];
}

/*
 * Flush out locally pending deprecated feature usage stats entries
 *
 * If nowait is true, this function returns true if the lock could not be
 * acquired. Otherwise return false.
 */
bool
pgstat_deprecated_features_flush_cb(bool nowait)
{
	PgStatShared_DeprecatedFeatures *stats_shmem = &pgStatLocal.shmem->deprecated_features;
	int			i;

	if (!have_deprecated_feature_stats)
		return false;

	if (!nowait)
		LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(&stats_shmem->lock, LW_EXCLUSIVE))
		return true;

	for (i = 0; i < PGSTAT_NUM_DEPRECATED_FEATURES; i++)
	{
		PgStat_DeprecatedFeatureStats *sharedent = &stats_shmem->stats[i];
		PgStat_DeprecatedFeatureStats *pendingent = &pending_DeprecatedFeatureStats[i];

		sharedent->usage_count += pendingent->usage_count;
		sharedent->last_used = Max(sharedent->last_used,
								   pendingent->last_used);
	}

	/* done, clear the pending entry */
	MemSet(pending_DeprecatedFeatureStats, 0,
		   sizeof(pending_DeprecatedFeatureStats));

	LWLockRelease(&stats_shmem->lock);

	have_deprecated_feature_stats = false;

	return false;
}

void
pgstat_deprecated_features_init_shmem_cb(void *stats)
{
	PgStatShared_DeprecatedFeatures *stats_shmem = (PgStatShared_DeprecatedFeatures *) stats;

	LWLockInitialize(&stats_shmem->lock, LWTRANCHE_PGSTATS_DATA);
}

void
pgstat_deprecated_features_reset_all_cb(TimestampTz ts)
{
	PgStatShared_DeprecatedFeatures *stats_shmem = &pgStatLocal.shmem->deprecated_features;

	LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);

	for (int i = 0; i < PGSTAT_NUM_DEPRECATED_FEATURES; i++)
	{
		memset(&stats_shmem->stats[i], 0,
			   sizeof(PgStat_DeprecatedFeatureStats));
		stats_shmem->stats[i].stat_reset_timestamp = ts;
	}

	LWLockRelease(&stats_shmem->lock);
}

void
pgstat_deprecated_features_snapshot_cb(void)
{
	PgStatShared_DeprecatedFeatures *stats_shmem = &pgStatLocal.shmem->deprecated_features;

	LWLockAcquire(&stats_shmem->lock, LW_SHARED);

	memcpy(pgStatLocal.snapshot.deprecated_features, &stats_shmem->stats,
		   sizeof(stats_shmem->stats));

	LWLockRelease(&stats_shmem->lock);
}
