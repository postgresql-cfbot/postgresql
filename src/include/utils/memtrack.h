
#ifndef MEMTRACK_H
#define MEMTRACK_H

/* ----------
 * Backend memory accounting functions.
 *   Track how much memory each backend is using
 *   and place a cluster-wide limit on the total amount of backend memory.
 *
 * The main functions are:
 *     init_backend_memory()
 *     reserve_backend_memory()
 *     release_backend_memory()
 *     exit_backend_memory()
 *
 * All local variables are properly initialized at startup, so init_backend_memory()
 * only needs to be called after a fork() system call.
 *
 * The reserve/release functions implement both a "fast path" and a "slow path".
 * The fast path is used for most allocations, and it only references
 * local variables. The slow path is invoked periodically; it updates
 * shared memory and checks for limits on total backend memory.
 *
 * The following local variables represent the "TRUTH" of this backend's memory allocations.
 *   my_memory.allocated_bytes:               total amount of memory allocated by this backend.
 *   my_memory.allocated_bytes_by_type[type]: subtotals by allocator type.
 *
 * The local values are periodically reported to pgstgt.
 * The following variables hold the last reported values
 *    reported_memory.allocated_bytes
 *    reported_memory.allocated_bytes_by_type[type]:
 *
 * The "slow path" is invoked when my_memory.allocate_bytes exceeds these bounds.
 * Once invokoked, it updates the reported values and sets new bounds.
 *   allocation_upper_bound:          update when my_memory.allocated_bytes exceeds this
 *   allocation_lower_bound:          update when my_memory.allocated_bytes drops below this
 *   allocation_allowance_refill_qty  amount of memory to allocate or release before updating again.
 *
 * These counters are the values seen  by pgstat. They are a copy of reported_memory.
 *   proc->st_memory.allocated_bytes:               last total reported to pgstat
 *   proc->st_memory.allocated_bytes_by_type[type]: last reported subtotals reported to pgstat
 *
 * Limits on total backend memory. If max_total_bkend_bytes is zero, there is no limit.
 *   ProcGlobal->total_bkend_mem_bytes:       total amount of memory reserved by all backends, including shared memory
 *   max_total_bkend_bytes:                   maximum amount of memory allowed to be reserved by all backends.
 *
 * And finally,
 *   initial_allocation_allowance:            each backend consumes this much memory simply by existing.
 *   ProcGlobal->global_dsm_allocated_bytes:  total amount of shared memory allocated by all backends.
 * ----------
 */

#include <unistd.h>
#include "postgres.h"
#include "port/atomics.h"

/*
 * Define a debug macro which becomes noop() when debug is disabled.
 */
#define debug(args...)  (void)0


/* Various types of memory allocators we are tracking. */
typedef enum pg_allocator_type
{
	PG_ALLOC_OTHER = 0,    /* Not tracked, but part of total memory */
	PG_ALLOC_ASET,         /* Allocation Set           */
	PG_ALLOC_DSM,          /* Dynamic shared memory    */
	PG_ALLOC_GENERATION,   /* Generation Context (all freed at once) */
	PG_ALLOC_SLAB,         /* Slab allocator 		 */
	PG_ALLOC_TYPE_MAX,     /* (Last, for array sizing) */
} pg_allocator_type;


/*
 * PgBackendMemoryStatus
 *
 * For each backend, track how much memory has been allocated.
 * Note may be possible to have negative values, say if one backend
 * creates DSM segments and another backend destroys them.
 */
typedef struct PgBackendMemoryStatus
{
	int64		allocated_bytes;
	int64       allocated_bytes_by_type[PG_ALLOC_TYPE_MAX];
} PgBackendMemoryStatus;


/* These values are candidates for GUC variables.  We chose 1MV arbitrarily. */
static const int64 initial_allocation_allowance = 1024 * 1024;  /* 1MB */
static const int64 allocation_allowance_refill_qty = 1024 * 1024;  /* 1MB */

/* Compile time initialization constants */
#define INIT_BACKEND_MEMORY (PgBackendMemoryStatus) \
	{initial_allocation_allowance, {initial_allocation_allowance}}
#define NO_BACKEND_MEMORY (PgBackendMemoryStatus) \
	{0, {0}}

/* Manage memory allocation for backends. */
extern PGDLLIMPORT PgBackendMemoryStatus my_memory;
extern PGDLLIMPORT PgBackendMemoryStatus reported_memory;
extern PGDLLIMPORT int64 allocation_upper_bound;
extern PGDLLIMPORT int64 allocation_lower_bound;

extern PGDLLIMPORT int64 max_total_bkend_bytes;
extern PGDLLIMPORT int32 max_total_bkend_mem;

/* These are the main entry points for backend memory accounting */
extern void init_backend_memory(void);
static inline bool reserve_backend_memory(int64 size, pg_allocator_type type);
static inline void release_backend_memory(int64 size, pg_allocator_type type);
extern void exit_backend_memory(void);

/* Helper functions for backend memory accounting */
static inline bool update_local_allocation(int64 size, pg_allocator_type type);
extern bool update_global_allocation(int64 size, pg_allocator_type type);

/* ----------
 * reserve_backend_memory() -
 *  Called to report a desired increase in memory for this backend.
 *  true if successful.
 */
static inline bool
reserve_backend_memory(int64 size, pg_allocator_type type)
{
	Assert(size >= 0);

	/* quick optimization */
	if (size == 0)
		return true;

	/* CASE: the new allocation is within bounds. Take the fast path. */
	else if (my_memory.allocated_bytes + size <= allocation_upper_bound)
		return update_local_allocation(size, type);

	/* CASE: out of bounds. Update pgstat and check memory limits */
	else
		return update_global_allocation(size, type);
}

/* ----------
 * unreserve_memory() -
 *  Called to report decrease in memory allocated for this backend.
 *  Note we should have already called "reserve_backend_memory"
 *  so we should never end up with a negative total allocation.
 */
static inline void
release_backend_memory(int64 size, pg_allocator_type type)
{
	Assert(size >= 0);

	/* quick optimization */
	if (size == 0)
		return;

	/* CASE: In bounds, take the fast path */
	else if (my_memory.allocated_bytes - size >= allocation_lower_bound)
		update_local_allocation(-size, type);

	/* CASE: Out of bounds. Update pgstat and memory totals */
	else
		update_global_allocation(-size, type);
}


/*
* Fast path for reserving and releasing memory.
* This version is used for most allocations, and it
* is stripped down to the bare minimum to reduce impact
* on performance. It only updates local variables.
*/
static inline bool
update_local_allocation(int64 size, pg_allocator_type type)
{
	/* Update our local memory counters. */
	my_memory.allocated_bytes += size;
	my_memory.allocated_bytes_by_type[type] += size;

	return true;
}


/*--------------------------------------------
 * Convenience functions based on malloc/free
 *------------------------------------------*/

/*
 * Reserve memory from malloc if we can.
 */
static inline void *
malloc_backend(int64 size, pg_allocator_type type)
{
	void *ptr;

	/* reserve the memory if able to */
	if (!reserve_backend_memory(size, type))
		return NULL;

	/* Allocate the memory, releasing the reservation if failed */
	ptr = malloc(size);
	if (ptr == NULL)
		release_backend_memory(size, type);

	return ptr;
}

/*
 * Free memory which was allocated with malloc_reserved.
 * Note: most mallocs have a non-portable method to
 * get the size of a block of memory. Dropping the "size" parameter
 * would greatly simplify the calling code.
 */
static inline void
free_backend(void *ptr, int64 size, pg_allocator_type type)
{
	release_backend_memory(size, type);
	free(ptr);
}


/*
 * Realloc reserved memory.
 */
static inline void *
realloc_backend(void *block, int64 new_size, int64 old_size, pg_allocator_type type)
{
	void *ptr;
	bool success;

	/* Update the reservation to the new size */
	release_backend_memory(old_size, type);
	success = reserve_backend_memory(new_size, type);

	/* If unable, free the old memory and return NULL */
	if (!success)
	{
		free(block);
		return NULL;
	}

	/* Now, actually resize the memory */
	ptr = realloc(block, new_size);

	/*
	 * If unable to resize, release the allocation.
	 * The actual memory has already been freed.
	 */
	if (ptr == NULL)
		release_backend_memory(new_size, type);

	return ptr;
}


/* True if adding a and b would overflow */
static inline bool addition_overflow(int64 a, int64 b)
{
	int64 result = a + b;
	return (a > 0 && b > 0 && result < 0) || (a < 0 && b < 0 && result > 0);
}

/*
 * Helper function to add to an atomic sum, as long as the result is within bounds.
 * TODO: consider moving to atomics.h
 */
static inline bool
atomic_add_within_bounds_i64(pg_atomic_uint64 *ptr, int64 add, int64 lower_bound, int64 upper_bound)
{
	int64 oldval;
	int64 newval;

	for (;;)
	{
		oldval = (int64)pg_atomic_read_u64(ptr);
		newval = oldval + add;

		/* check if we are out of bounds */
		if (newval < lower_bound || newval > upper_bound || addition_overflow(oldval, add))
			return false;

		if (pg_atomic_compare_exchange_u64(ptr, (uint64 *)&oldval, newval))
			return true;
	}
}



#endif //POSTGRES_IDE_MEMTRACK_H
