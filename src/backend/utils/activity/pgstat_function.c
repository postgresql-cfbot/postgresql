/* -------------------------------------------------------------------------
 *
 * pgstat_function.c
 *	  Implementation of function statistics.
 *
 * This file contains the implementation of function statistics. It is kept
 * separate from pgstat.c to enforce the line between the statistics access /
 * storage implementation and the details about individual types of
 * statistics.
 *
 * Copyright (c) 2001-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_function.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/pgstat_internal.h"
#include "utils/timestamp.h"


/* ----------
 * GUC parameters
 * ----------
 */
int			pgstat_track_functions = TRACK_FUNC_OFF;


/*
 * Total time charged to functions so far in the current backend.
 * We use this to help separate "self" and "other" time charges.
 * (We assume this initializes to zero.)
 */
static instr_time total_func_time;


/*
 * Ensure that stats are dropped if transaction aborts.
 */
void
pgstat_create_function(Oid proid)
{
	pgstat_schedule_create(PGSTAT_KIND_FUNCTION,
						   MyDatabaseId,
						   proid);
}

/*
 * Ensure that stats are dropped if transaction commits.
 */
void
pgstat_drop_function(Oid proid)
{
	/*
	 * AFIXME: This is not bulletproof, because functions are not locked, and
	 * therefore no sinval processing occurs when calling a function. Which in
	 * turn means that new stats entries might be created for stats whose
	 * shared stats entry already has been dropped.
	 *
	 * A probably solution is to not allow dropping stats from
	 * pgStatSharedHash until no transaction is alive that could potentially
	 * still see the row. E.g. by adding an xid to PgStatShmHashEntry which
	 * needs to be older than the horizon for the entry to be removed.
	 *
	 * Or we could decide not to care, and ensure that we reset stats in
	 * pgstat_create_function() / pgstat_schedule_create().
	 */

	pgstat_schedule_drop(PGSTAT_KIND_FUNCTION,
						 MyDatabaseId,
						 proid);
}

/*
 * Initialize function call usage data.
 * Called by the executor before invoking a function.
 */
void
pgstat_init_function_usage(FunctionCallInfo fcinfo,
						   PgStat_FunctionCallUsage *fcu)
{
	PgStatSharedRef *shared_ref;
	PgStat_BackendFunctionEntry *pending;

	if (pgstat_track_functions <= fcinfo->flinfo->fn_stats)
	{
		/* stats not wanted */
		fcu->fs = NULL;
		return;
	}

	shared_ref = pgstat_pending_prepare(PGSTAT_KIND_FUNCTION,
											 MyDatabaseId,
											 fcinfo->flinfo->fn_oid);
	pending = shared_ref->pending;

	fcu->fs = &pending->f_counts;

	/* save stats for this function, later used to compensate for recursion */
	fcu->save_f_total_time = pending->f_counts.f_total_time;

	/* save current backend-wide total time */
	fcu->save_total = total_func_time;

	/* get clock time as of function start */
	INSTR_TIME_SET_CURRENT(fcu->f_start);
}

/*
 * Calculate function call usage and update stat counters.
 * Called by the executor after invoking a function.
 *
 * In the case of a set-returning function that runs in value-per-call mode,
 * we will see multiple pgstat_init_function_usage/pgstat_end_function_usage
 * calls for what the user considers a single call of the function.  The
 * finalize flag should be TRUE on the last call.
 */
void
pgstat_end_function_usage(PgStat_FunctionCallUsage *fcu, bool finalize)
{
	PgStat_FunctionCounts *fs = fcu->fs;
	instr_time	f_total;
	instr_time	f_others;
	instr_time	f_self;

	/* stats not wanted? */
	if (fs == NULL)
		return;

	/* total elapsed time in this function call */
	INSTR_TIME_SET_CURRENT(f_total);
	INSTR_TIME_SUBTRACT(f_total, fcu->f_start);

	/* self usage: elapsed minus anything already charged to other calls */
	f_others = total_func_time;
	INSTR_TIME_SUBTRACT(f_others, fcu->save_total);
	f_self = f_total;
	INSTR_TIME_SUBTRACT(f_self, f_others);

	/* update backend-wide total time */
	INSTR_TIME_ADD(total_func_time, f_self);

	/*
	 * Compute the new f_total_time as the total elapsed time added to the
	 * pre-call value of f_total_time.  This is necessary to avoid
	 * double-counting any time taken by recursive calls of myself.  (We do
	 * not need any similar kluge for self time, since that already excludes
	 * any recursive calls.)
	 */
	INSTR_TIME_ADD(f_total, fcu->save_f_total_time);

	/* update counters in function stats table */
	if (finalize)
		fs->f_numcalls++;
	fs->f_total_time = f_total;
	INSTR_TIME_ADD(fs->f_self_time, f_self);
}

/*
 * pgstat_flush_function - flush out a local function stats entry
 *
 * If nowait is true, this function returns false on lock failure. Otherwise
 * this function always returns true.
 *
 * Returns true if the entry is successfully flushed out.
 */
bool
pgstat_flush_function(PgStatSharedRef *shared_ref, bool nowait)
{
	PgStat_BackendFunctionEntry *localent;	/* local stats entry */
	PgStatShm_StatFuncEntry *shfuncent = NULL; /* shared stats entry */

	Assert(shared_ref->shared_entry->key.kind == PGSTAT_KIND_FUNCTION);
	localent = (PgStat_BackendFunctionEntry *) shared_ref->pending;

	/* localent always has non-zero content */

	if (!pgstat_shared_stat_lock(shared_ref, nowait))
		return false;			/* failed to acquire lock, skip */

	shfuncent = (PgStatShm_StatFuncEntry *) shared_ref->shared_stats;

	shfuncent->stats.f_numcalls += localent->f_counts.f_numcalls;
	shfuncent->stats.f_total_time +=
		INSTR_TIME_GET_MICROSEC(localent->f_counts.f_total_time);
	shfuncent->stats.f_self_time +=
		INSTR_TIME_GET_MICROSEC(localent->f_counts.f_self_time);

	pgstat_shared_stat_unlock(shared_ref);

	return true;
}

/* ----------
 * find_funcstat_entry - find any existing PgStat_BackendFunctionEntry entry
 *		for specified function
 *
 * If no entry, return NULL, don't create a new one
 * ----------
 */
PgStat_BackendFunctionEntry *
find_funcstat_entry(Oid func_id)
{
	PgStatSharedRef *shared_ref;

	shared_ref = pgstat_pending_fetch(PGSTAT_KIND_FUNCTION, MyDatabaseId, func_id);

	if (shared_ref)
		return shared_ref->pending;
	return NULL;
}

/* ----------
 * pgstat_fetch_stat_funcentry() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	the collected statistics for one function or NULL.
 *
 *  The returned entry is stored in static memory so the content is valid until
 *	the next call of the same function for the different function id.
 * ----------
 */
PgStat_StatFuncEntry *
pgstat_fetch_stat_funcentry(Oid func_id)
{
	return (PgStat_StatFuncEntry *)
		pgstat_fetch_entry(PGSTAT_KIND_FUNCTION, MyDatabaseId, func_id);
}
