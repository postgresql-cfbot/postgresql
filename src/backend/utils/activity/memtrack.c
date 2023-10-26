
#include <unistd.h>

#include "postgres.h"
#include "miscadmin.h"
#include "utils/backend_status.h"
#include "utils/memtrack.h"
#include "storage/proc.h"

/*
 * Max backend memory allocation allowed (MB). 0 = disabled.
 * Max backend bytes is the same but in bytes.
 * These default to "0", meaning don't check bounds for total memory.
 */
int			max_total_bkend_mem = 0;
int64       max_total_bkend_bytes = 0;

/*
 * Local variables for tracking memory use.
 * These values are preset so memory tracking is active on startup.
 * After a fork(), they must be reset using 'init_backend_memory()'.
 */
PgBackendMemoryStatus  my_memory = INIT_BACKEND_MEMORY;
PgBackendMemoryStatus  reported_memory = NO_BACKEND_MEMORY;
int64      allocation_lower_bound = 0;
int64      allocation_upper_bound = 0;

/* ---------
 * init_backend_memory() -
 *
 * Called immediately after a fork().
 * Resets local memory counters to their initial startup values
 */
void
init_backend_memory(void)
{
	debug("\n");
	/* Start with nothing allocated. */
	my_memory = INIT_BACKEND_MEMORY;
	reported_memory = NO_BACKEND_MEMORY;

	/* Force the next allocation to do global bounds checking. */
	allocation_lower_bound = 0;
	allocation_upper_bound = 0;
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
exit_backend_memory(void)
{
	debug("\n");
	/*
	 * Release non-dsm memory.
	 * We don't release dsm shared memory since it survives process exit.
	 */
	for (int type = 0; type < PG_ALLOC_TYPE_MAX; type++)
		if (type != PG_ALLOC_DSM)
			release_backend_memory(my_memory.allocated_bytes_by_type[type], type);

	/* Force the final values to be posted to shmem */
	update_global_allocation(0, PG_ALLOC_OTHER);

	/* If we get a late request, send it to the long path. */
	allocation_lower_bound = 0;
	allocation_upper_bound = 0;
}


/*
 * Update backend memory allocation for a new request.
 *
 * There are two versions of this function. This one, which updates
 * global values in shared memory, and an optimized update_local_allocation()
 * which only updates local values.
 *
 * This version is the "slow path". We invoke it periodically to update
 * global values and pgstat statistics.
 */
bool update_global_allocation(int64 size, pg_allocator_type type)
{
	int64 new_allocated_bytes;
	int64 dsm_delta;

	/* If we are still initializing, ignore the request. It should be part of initial allocation. */
	if (ProcGlobal == NULL || MyProcPid == 0)
		return update_local_allocation(size, type);

	debug("size=%ld type=%d\n", size, type);

	/* Calculate new number of bytes reflecting the reservation/release */
	new_allocated_bytes = my_memory.allocated_bytes + size;

	/* If reserving new memory and we are limited by max_total_bkend ... */
	if (size > 0 && max_total_bkend_bytes > 0 && MyAuxProcType == NotAnAuxProcess && MyProcPid != PostmasterPid)
	{
		/* Update the global total memory counter subject to the upper limit. */
		if (!atomic_add_within_bounds_i64(&ProcGlobal->total_bkend_mem_bytes,
										  new_allocated_bytes - reported_memory.allocated_bytes,
										  0, max_total_bkend_bytes))
			return false;
	}

		/* Otherwise, update the global counter with no limit checking */
	else
		pg_atomic_add_fetch_u64(&ProcGlobal->total_bkend_mem_bytes,
								new_allocated_bytes - reported_memory.allocated_bytes);

	/* Update the local memory counters. This must happen after bounds checking */
	update_local_allocation(size, type);

	/* Update the global dsm memory counter to reflect changes since our last report */
	dsm_delta = my_memory.allocated_bytes_by_type[PG_ALLOC_DSM] - reported_memory.allocated_bytes_by_type[PG_ALLOC_DSM];
	pg_atomic_add_fetch_u64(&ProcGlobal->global_dsm_allocation, dsm_delta);

	/* Update pgstat statistics if we are initialized as a backend process. */
	if (MyBEEntry != NULL)
	{
		PGSTAT_BEGIN_WRITE_ACTIVITY(MyBEEntry);
		MyBEEntry->st_memory = my_memory;
		PGSTAT_END_WRITE_ACTIVITY(MyBEEntry);
	}

	/* Remember the values we just reported to pgstat */
	reported_memory = my_memory;

	/* Update bounds so they bracket our new allocation size. */
	allocation_upper_bound = my_memory.allocated_bytes + allocation_allowance_refill_qty;
	allocation_lower_bound = my_memory.allocated_bytes - allocation_allowance_refill_qty;

	Assert((int64)pg_atomic_read_u64(&ProcGlobal->total_bkend_mem_bytes) >= 0);
	Assert((int64)pg_atomic_read_u64(&ProcGlobal->global_dsm_allocation) >= 0);

	return true;
}
