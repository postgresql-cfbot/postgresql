/*--------------------------------------------------------------------------
 *
 * injection_stats.c
 *		Code for statistics of injection points.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/test/modules/injection_points/injection_stats.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "common/hashfn.h"
#include "injection_stats.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/pgstat_internal.h"

/* Maximum name length */
#define INJ_NAME_MAXLEN 64

/* Structures for statistics of injection points */
typedef struct PgStat_StatInjEntry
{
	char		name[INJ_NAME_MAXLEN];	/* used for serialization */
	PgStat_Counter numcalls;	/* number of times point has been run */
} PgStat_StatInjEntry;

typedef struct PgStatShared_InjectionPoint
{
	PgStatShared_Common header;
	PgStat_StatInjEntry stats;
} PgStatShared_InjectionPoint;

static bool injection_stats_flush_cb(PgStat_EntryRef *entry_ref, bool nowait);
static void injection_stats_to_serialized_name_cb(const PgStat_HashKey *key,
												  const PgStatShared_Common *header,
												  NameData *name);
static bool injection_stats_from_serialized_name_cb(const NameData *name,
													PgStat_HashKey *key);

static const PgStat_KindInfo injection_stats = {
	.name = "injection_points",
	.fixed_amount = false,		/* Bounded by the number of points */

	/* Injection points are system-wide */
	.accessed_across_databases = true,

	.shared_size = sizeof(PgStatShared_InjectionPoint),
	.shared_data_off = offsetof(PgStatShared_InjectionPoint, stats),
	.shared_data_len = sizeof(((PgStatShared_InjectionPoint *) 0)->stats),
	.pending_size = sizeof(PgStat_StatInjEntry),
	.flush_pending_cb = injection_stats_flush_cb,
	.to_serialized_name = injection_stats_to_serialized_name_cb,
	.from_serialized_name = injection_stats_from_serialized_name_cb,
};

/*
 * Compute stats entry idx from point name with a 4-byte hash.
 */
#define PGSTAT_INJ_IDX(name) hash_bytes((const unsigned char *) name, strlen(name))

static PgStat_Kind inj_stats_kind = PGSTAT_KIND_INVALID;

/*
 * Callbacks for stats handling
 */
static bool
injection_stats_flush_cb(PgStat_EntryRef *entry_ref, bool nowait)
{
	PgStat_StatInjEntry *localent;
	PgStatShared_InjectionPoint *shfuncent;

	localent = (PgStat_StatInjEntry *) entry_ref->pending;
	shfuncent = (PgStatShared_InjectionPoint *) entry_ref->shared_stats;

	if (!pgstat_lock_entry(entry_ref, nowait))
		return false;

	shfuncent->stats.numcalls += localent->numcalls;
	return true;
}

/*
 * Transforms a hash key into a name that is then stored on disk.
 */
static void
injection_stats_to_serialized_name_cb(const PgStat_HashKey *key,
									  const PgStatShared_Common *header,
									  NameData *name)
{
	PgStatShared_InjectionPoint *shstatent =
		(PgStatShared_InjectionPoint *) header;

	namestrcpy(name, shstatent->stats.name);
}

/*
 * Computes the hash key used for this injection point name.
 */
static bool
injection_stats_from_serialized_name_cb(const NameData *name,
										PgStat_HashKey *key)
{
	uint32		idx = PGSTAT_INJ_IDX(NameStr(*name));

	key->kind = inj_stats_kind;
	key->dboid = InvalidOid;
	key->objoid = idx;
	return true;
}

/*
 * Support function for the SQL-callable pgstat* functions.  Returns
 * a pointer to the injection point statistics struct.
 */
static PgStat_StatInjEntry *
pgstat_fetch_stat_injentry(const char *name)
{
	PgStat_StatInjEntry *entry = NULL;

	if (inj_stats_kind == PGSTAT_KIND_INVALID)
		inj_stats_kind = pgstat_add_kind(&injection_stats);

	/* Compile the lookup key as a hash of the point name */
	entry = (PgStat_StatInjEntry *) pgstat_fetch_entry(inj_stats_kind,
													   InvalidOid,
													   PGSTAT_INJ_IDX(name));
	return entry;
}

/*
 * Workhorse to do the registration work, called in _PG_init().
 */
void
pgstat_register_inj(void)
{
	if (inj_stats_kind == PGSTAT_KIND_INVALID)
		inj_stats_kind = pgstat_add_kind(&injection_stats);
}

/*
 * Report injection point creation.
 */
void
pgstat_create_inj(const char *name)
{
	PgStat_EntryRef *entry_ref;
	PgStatShared_InjectionPoint *shstatent;

	if (inj_stats_kind == PGSTAT_KIND_INVALID)
		inj_stats_kind = pgstat_add_kind(&injection_stats);

	entry_ref = pgstat_get_entry_ref_locked(inj_stats_kind, InvalidOid,
											PGSTAT_INJ_IDX(name), false);
	shstatent = (PgStatShared_InjectionPoint *) entry_ref->shared_stats;

	/* initialize shared memory data */
	memset(&shstatent->stats, 0, sizeof(shstatent->stats));
	strlcpy(shstatent->stats.name, name, INJ_NAME_MAXLEN);

	pgstat_unlock_entry(entry_ref);
}

/*
 * Report injection point drop.
 */
void
pgstat_drop_inj(const char *name)
{
	if (inj_stats_kind == PGSTAT_KIND_INVALID)
		inj_stats_kind = pgstat_add_kind(&injection_stats);

	if (!pgstat_drop_entry(inj_stats_kind, InvalidOid,
						   PGSTAT_INJ_IDX(name)))
		pgstat_request_entry_refs_gc();
}

/*
 * Report statistics for injection point.
 *
 * This is simple because the set of stats to report currently is simple:
 * track the number of times a point has been run.
 */
void
pgstat_report_inj(const char *name)
{
	PgStat_EntryRef *entry_ref;
	PgStatShared_InjectionPoint *shstatent;
	PgStat_StatInjEntry *statent;

	if (inj_stats_kind == PGSTAT_KIND_INVALID)
		inj_stats_kind = pgstat_add_kind(&injection_stats);

	entry_ref = pgstat_get_entry_ref_locked(inj_stats_kind, InvalidOid,
											PGSTAT_INJ_IDX(name), false);

	shstatent = (PgStatShared_InjectionPoint *) entry_ref->shared_stats;
	statent = &shstatent->stats;

	/* Update the injection point statistics */
	statent->numcalls++;

	pgstat_unlock_entry(entry_ref);
}

/*
 * SQL function returning the number of times an injection point
 * has been called.
 */
PG_FUNCTION_INFO_V1(injection_points_stats_numcalls);
Datum
injection_points_stats_numcalls(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	PgStat_StatInjEntry *entry = pgstat_fetch_stat_injentry(name);

	if (entry == NULL)
		PG_RETURN_NULL();

	PG_RETURN_INT64(entry->numcalls);
}
