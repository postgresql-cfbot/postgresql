#ifndef MEMTRACK_H
#define MEMTRACK_H

/* ----------
 * Memory accounting functions.
 *   Track how much memory each process is using and place an
 *   overall limit on how much memory a database server can allocate.
 *
 * The main functions are:
 *     fork_tracked_memory()
 *     reserve_tracked_memory()
 *     release_tracked_memory()
 *     exit_tracked_memory()
 *
 * These routines implement an approximate total for memory allocated by the
 * database server. For efficency, each process accurately tracks its own memory,
 * but it only periodicaly updates to the global total. This approximate total is
 * "close enough" for monitoring memory use by the server.
 *
 * Note we make an exception for DSM memory. The global total for DSM memory is
 * always kept up-to-date. The problem is, an approximate DSM total can go negative as
 * DSM is released. While not inherently evil, a negative DSM total could be extremely
 * confusing.
 *
 * All private variables are properly initialized at startup, so fork_tracked_memory()
 * only needs to be called after a fork() system call.
 *
 * The reserve/release functions implement both a "fast path" and a "slow path".
 * The fast path is used for most allocations, and it only references
 * private (hon-shared) variables. The slow path is invoked periodically; it updates
 * shared memory and checks for limits on total server memory.
 *
 * The following private variables represent the "TRUTH" of how much memory the process allocated.
 * The total can be calculated as the sum of the subtotals, but keeping a separate total simplifies
 * and shortens some of the code paths.
 *   my_memory.total:               total amount of memory allocated by this process
 *   my_memory.subTotal[type]:      subtotals by allocator type.
 *
 * The private values are periodically reported to pgstat.
 * The following variables hold the last reported values
 *    reported_memory.total
 *    reported_memory.subTotal[type]:
 *
 * The "slow path" is invoked when my_memory.allocated_bytes exceeds these bounds.
 * Once invoked, it updates the reported values and sets new bounds.
 *   reservation_upper_bound:          update when my_memory.allocated_bytes exceeds this
 *   reservation_lower_bound:          update when my_memory.allocated_bytes drops below this
 *   allocation_allowance_refill_qty  amount of memory to allocate or release before updating again.
 *
 * These counters are the values seen  by pgstat. They are a copy of reported_memory.
 *   proc->st_memory.total:               total for pgstat
 *   proc->st_memory.subTotal[type]:      last subtotals for pgstat
 *
 * Limits on total server memory. If max_total_memory_bytes is zero, there is no limit.
 *   ProcGlobal->total_reserved:       total amount of memory reserved by the server, including shared memory
 *   max_total_memory_bytes:               maximum memory the server can allocate
 *
 * And finally,
 *   initial_allocation_allowance:            each process consumes this much memory simply by existing.
 *   ProcGlobal->dsm_reserved:             total amount of DSM memory allocated by the server
 *
 * Note this header file works in conjunction with memtrack.c and pgstat_memtrack.c.
 * The former is focused on gathering memory data and implementing a max
 * limit, while the latter implements views for reporting memory statistics.
 * The two sets of routines work together and share common data structures.
 * ----------
 */

#include <unistd.h>
#include "postgres.h"
#include "fmgr.h"
#include "memdebug.h"
#include "common/int.h"
#include "port/atomics.h"
#include "utils/backend_status.h"
#include "utils/memtrack.h"
#include "utils/pgstat_internal.h"

/* This value is a candidate to be a GUC variable.  We chose 1MB arbitrarily. */
static const int64 allocation_allowance_refill_qty = 1024 * 1024;	/* 1MB */

/* Compile time initialization constants */
#define initial_allocation_allowance (1024 * 1024)
#define INITIAL_ALLOCATED_MEMORY (PgStat_Memory) \
	{initial_allocation_allowance, {initial_allocation_allowance}}
#define NO_ALLOCATED_MEMORY (PgStat_Memory) \
	{0, {0}}

/* Manage memory allocation for backends. */
extern PGDLLIMPORT PgStat_Memory my_memory;
extern PGDLLIMPORT PgStat_Memory reported_memory;
extern PGDLLIMPORT PgStat_Memory my_memory_snap;
extern PGDLLIMPORT int64 reservation_upper_bound;
extern PGDLLIMPORT int64 reservation_lower_bound;

extern PGDLLIMPORT int64 max_total_memory_bytes;
extern PGDLLIMPORT int32 max_total_memory_mb;

/* These are the main entry points for memory tracking */
extern void fork_tracked_memory(void);
static inline bool reserve_tracked_memory(int64 size, pg_allocator_type type);
static inline bool release_tracked_memory(int64 size, pg_allocator_type type);
extern void exit_tracked_memory(void);
extern void pgstat_init_memtrack(PgStatShared_Memtrack *global);

/* Helper functions for memory tracking */
static inline bool update_local_reservation(int64 size, pg_allocator_type type);
extern bool update_global_reservation(int64 size, pg_allocator_type type);

/* pgstat helper functions */
void pgstat_report_backend_memory(void);
void pgstat_report_postmaster_memory(void);
void pgstat_init_memtrack(PgStatShared_Memtrack *global);
void pgstat_backend_memory_reservation_cb(void);
int64 getContextMemoryTotal(void);

/* SQL Callable functions */
extern Datum pg_stat_get_memory_reservation(PG_FUNCTION_ARGS);
extern Datum pg_get_backend_memory_allocation(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_global_memory_tracking(PG_FUNCTION_ARGS);

/*--------------------------------------------
 * Keep track of memory coming from malloc()/free().
 * Replacing malloc()/free() calls with these routines
 * keeps track of most Postgres memory allocations.
 * For the other cases, allocate memory as desired and
 * report the allocations using reserve_tracked_memory()
 * and release_tracked_memory().
 *------------------------------------------*/

/*
 * Allocate tracked memory using malloc.
 */
static inline void *
malloc_tracked(int64 size, pg_allocator_type type)
{
	void	   *ptr;

	/* reserve the memory if able to */
	if (!reserve_tracked_memory(size, type))
		return NULL;

	/* Allocate the memory, releasing the reservation if failed */
	ptr = malloc(size);
	if (ptr == NULL)
		release_tracked_memory(size, type);

	return ptr;
}

/*
 * Free memory which was allocated with malloc_tracked.
 * Note: most mallocs have a non-portable method to
 * get the size of a block of memory. Dropping the "size" parameter
 * would greatly simplify the calling code.
 */
static inline void
free_tracked(void *ptr, int64 size, pg_allocator_type type)
{
	release_tracked_memory(size, type);
#ifdef CLOBBER_FREED_MEMORY
	wipe_mem(ptr, size);
#endif
	free(ptr);
}


/*
 * Realloc tracked memory.
 */
static inline void *
realloc_tracked(void *block, int64 new_size, int64 old_size, pg_allocator_type type)
{
	void	   *ptr;
	bool		success;

	/* try to reserve the new memory size */
	int64 delta = new_size - old_size;
	success = (delta > 0)? reserve_tracked_memory(delta, type)
		                 : release_tracked_memory(-delta, type);

	/* If unable, free the memory and return NULL */
	if (!success)
	{
		free_tracked(block, old_size, type);
		return NULL;
	}

	/* Now, do the reallocation. If unsuccessful, release it. */
	ptr = realloc(block, new_size);
	if (ptr == NULL)
		release_tracked_memory(new_size, type);

	return ptr;
}


/*-----------------------------------------------------------------
 * Report memory as it is allocated or released.
 * These routines are inlined so the "fast path" through them is
 * as efficient as possible.
 *---------------------------------------------------------------*/

/*
 *  Report a desired increase in memory for this process.
 *  true if successful.
 */
static inline bool
reserve_tracked_memory(int64 size, pg_allocator_type type)
{
	Assert(size >= 0);

	/* CASE: no change in reserved memory. Do nothing. */
	if (size == 0)
		return true;

	/* CASE: Bounds reached, take the slow path and update pgstat globals */
	if (my_memory.total + size >= reservation_upper_bound || type == PG_ALLOC_DSM)
		return update_global_reservation(size, type);

	/* OTHERWISE: take the fast path and only update local variables */
	return update_local_reservation(size, type);
}

/* ----------
 *  Report a decrease in memory allocated for this process.
 *  Note we should have already called "reserve_tracked_memory".
 *  We should never end up with a negative subtotal, except
 *  for DSM memory when one process allocates it and another process
 *  releases it.
 */
static inline bool
release_tracked_memory(int64 size, pg_allocator_type type)
{
	Assert(size >= 0);

	/* CASE: no change in reserved memory. Do nothing. */
	if (size == 0)
		return true;

	/* CASE: Bounds reached, take the slow path and update pgstat globals */
	if (my_memory.total - size <= reservation_lower_bound || type == PG_ALLOC_DSM)
		return update_global_reservation(-size, type);

	/* OTHERWISE: take the fast path and only update local variables */
	return update_local_reservation(-size, type);
}


/*
* Fast path for reserving and releasing memory.
* This version is used for most allocations, and it
* is stripped down to the bare minimum to reduce impact
* on performance. It only updates private (non-shared) variables.
*/
static inline bool
update_local_reservation(int64 size, pg_allocator_type type)
{
	/* Update our local memory counters. */
	my_memory.total += size;
	my_memory.subTotal[type] += size;

	return true;
}


#endif							/* //POSTGRES_IDE_MEMTRACK_H */
