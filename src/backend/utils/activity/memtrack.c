/*-------------------------------------------------------------------------
 *
 * memtrack.c
 *	  track and manage memory usage by the PostgreSQL server.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/memtrack.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "utils/backend_status.h"
#include "utils/memtrack.h"
#include "storage/proc.h"
#include "storage/pg_shmem.h"
#include "utils/pgstat_internal.h"

/*
 * Max backend memory allocation allowed (MB). 0 = disabled.
 * Max backend bytes is the same but in bytes.
 * These default to "0", meaning don't check bounds for total memory.
 */
int			max_total_memory_mb = 0;
int64		max_total_memory_bytes = 0;

/*
 * Private variables for tracking memory use.
 * These values are preset so memory tracking is active on startup.
 * After a fork(), they must be reset using 'fork_tracked_memory()'.
 */
PgStat_Memory my_memory = INITIAL_ALLOCATED_MEMORY;
PgStat_Memory reported_memory = NO_ALLOCATED_MEMORY;
int64		reservation_lower_bound = 0;
int64		reservation_upper_bound = 0;

/*
 * Reset memory tracking after a fork.
 * We actually keep the memory intact, but
 * the memory hasn't been added to the global totals.
 *
 * The counters are properly initialized at startup,
 * so this function only needs to be called after a fork().
 */
void
fork_tracked_memory(void)
{
	/* This new process hasn't reported any memory yet. */
	reported_memory = NO_ALLOCATED_MEMORY;

	/* Force allocations to be reported once ProcGlobal is initialized. */
	reservation_lower_bound = 0;
	reservation_upper_bound = 0;

	/* Release the DSM reservations since we didn't create them. */
	update_local_reservation(-my_memory.subTotal[PG_ALLOC_DSM], PG_ALLOC_DSM);
}

/*
 * Clean up memory counters as backend is exiting.
 *
 * DSM memory is not automatically returned, so it persists in the counters.
 * All other memory will disappear, so those counters are set to zero.
 *
 * Ideally, this function would be called last, but in practice there are some
 * late memory releases that happen after it is called.
 */
void
exit_tracked_memory(void)
{
	/*
	 * Release all of our private (non-dsm) memory. We don't release dsm
	 * shared memory since it survives process exit.
	 */
	for (int type = 0; type < PG_ALLOC_TYPE_MAX; type++)
		if (type != PG_ALLOC_DSM)
			update_local_reservation(-my_memory.subTotal[type], type);

	/* Report the final values to shmem (just once) */
	(void) update_global_reservation(0, 0);

	/*
	 * Sometimes we get late memory releases after this function is called.
	 * We've already reported all our private memory as released. Set the
	 * bounds to ensure we don't report those late releases twice.
	 */
	reservation_lower_bound = INT64_MIN;
	reservation_upper_bound = INT64_MAX;
}


/*
 * Update memory reservation for a new request.
 *
 * There are two versions of this function. This one, which updates
 * global values in shared memory, and an optimized update_local_reservation()
 * which only updates private values.
 *
 * This routine is the "slow path". We invoke it periodically to update
 * global values and pgstat statistics.
 *
 * We also invoke it whenever we reserve DSM memory. This ensures the
 * DSM memory counter is up-to-date, and more important, ensures it
 * never goes negative.
 */
bool
update_global_reservation(int64 size, pg_allocator_type type)
{
	int64		delta;
	uint64		dummy;
	PgStatShared_Memtrack *global = &pgStatLocal.shmem->memtrack;

	/*
	 * If we are still initializing, only update the private counters.
	 * The tests are:
	 *   1) Is pg shared memory attached?
	 *   2) Are statistics initialized?
	 *   3) Is postmaster up and running?
	 *   4) If backend, is MyBEEntry set up?
	 */
	if (UsedShmemSegAddr == NULL || pgStatLocal.shmem == NULL || PostmasterPid == 0 ||
		(MyProcPid != PostmasterPid && MyBEEntry == NULL))
		return update_local_reservation(size, type);

	/* Verify totals are not negative. This is both a pre- and post-condition. */
	Assert((int64) pg_atomic_read_u64(&global->total_memory_reserved) >= 0);
	Assert((int64) pg_atomic_read_u64(&global->total_dsm_reserved) >= 0);

	/* Calculate total bytes allocated or freed since last report */
	delta = my_memory.total + size - reported_memory.total;

	/*
	 * If memory limits are set, we are increasing our reservation and we
	 * are not the postmaster...
	 */
	if (max_total_memory_bytes > 0 && size > 0 && MyProcPid != PostmasterPid && delta > 0)
	{
		/* Update the global total memory counter subject to the upper limit. */
		if (!pg_atomic_fetch_add_limit_u64(&global->total_memory_reserved, delta, max_total_memory_bytes, &dummy))
			return false;
	}

	/*
	 * Otherwise, update the global counter with no limit checking.
	 */
	else
		(void) pg_atomic_fetch_add_u64(&global->total_memory_reserved, delta);

	/*
	 * Update the private memory counters. This must happen after the limit is
	 * checked.
	 */
	(void) update_local_reservation(size, type);

	/*
	 * Update the global dsm memory counter. Since we always take this path
	 * when dsm memory is allocated, the reported value is up-to-date, and we
	 * can simply add in the new size. We don't need to calculate the delta as
	 * we do for private memory allocators.
	 */
	if (type == PG_ALLOC_DSM)
		(void) pg_atomic_fetch_add_u64(&global->total_dsm_reserved, size);

	/* Report the current memory allocations for either postmaster or backend */
	if (MyProcPid == PostmasterPid)
		pgstat_report_postmaster_memory();
	else
		pgstat_report_backend_memory();

	/* Remember the values we just reported */
	reported_memory = my_memory;

	/* Update bounds so they bracket our new allocation size. */
	reservation_upper_bound = my_memory.total + allocation_allowance_refill_qty;
	reservation_lower_bound = my_memory.total - allocation_allowance_refill_qty;

	/*
	 * Verify totals are not negative. By checking as a post-condition, we are
	 * more likely to identify the code that caused the problem.
	 */
	Assert((int64) pg_atomic_read_u64(&global->total_memory_reserved) >= 0);
	Assert((int64) pg_atomic_read_u64(&global->total_dsm_reserved) >= 0);

	return true;
}
