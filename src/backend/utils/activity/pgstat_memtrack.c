/* -------------------------------------------------------------------------
 *
 * pgstat_memtrack.c
 *	  Implementation of memory tracking statistics.
 *
 * This file contains the implementation of memtrack statistics. It is kept
 * separate from pgstat.c to enforce the line between the statistics access /
 * storage implementation and the details about individual types of
 * statistics.
 *
 * Copyright (c) 2001-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_memtrack.c
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/pgstat_internal.h"
#include "utils/memtrack.h"
#include "utils/memutils_internal.h"
#include "utils/tuplestore.h"
#include "funcapi.h"
#include "storage/pg_shmem.h"

inline static Size asMB(Size bytes);
static void get_postmaster_reservation_row(bool *nulls, Datum *values);
static void get_backend_reservation_row(int idx, bool *nulls, Datum *values);
static void clearRow(bool *nulls, Datum *values, int count);

/*
 * Report postmaster memory allocations to pgstat.
 * Note memory statistics are accumulated in my_memory.
 * This function copies them into pgstat shared memory.
 * Only the postmaster should call this function.
 */
void
pgstat_report_postmaster_memory(void)
{
	PgStatShared_Memtrack *global = &pgStatLocal.shmem->memtrack;
	Assert(pgStatLocal.shmem != NULL);
	Assert(MyProcPid == PostmasterPid);

	pgstat_begin_changecount_write(&global->postmasterChangeCount);
	global->postmasterMemory = my_memory;
	pgstat_end_changecount_write(&global->postmasterChangeCount);
}


/*
 * Report background memory allocations to pgstat.
 */
void
pgstat_report_backend_memory(void)
{
	Assert(MyBEEntry != NULL);
	PGSTAT_BEGIN_WRITE_ACTIVITY(MyBEEntry);
	MyBEEntry->st_memory = my_memory;
	PGSTAT_END_WRITE_ACTIVITY(MyBEEntry);
}


/*
 * Initialize the pgstat global memory counters,
 * Called once during server startup.
 */
void
pgstat_init_memtrack(PgStatShared_Memtrack *global)
{
	Size		shmem_bytes;
	Size		shmem_mb;

	/* Get the size of the shared memory */
	shmem_bytes = ShmemGetSize();
	shmem_mb = asMB(shmem_bytes);

	/* Initialize the global memory counters. Total memory includes shared memory */
	pg_atomic_init_u64(&global->total_memory_reserved, shmem_bytes);
	pg_atomic_init_u64(&global->total_dsm_reserved, 0);

	/*
	 * Validate the server's memory limit if one is set.
	 */
	if (max_total_memory_mb > 0)
	{
		Size		connection_mb;
		Size		required_mb;

		/* Error if backend memory limit is less than shared memory size */
		if (max_total_memory_mb < shmem_mb)
			ereport(ERROR,
					errmsg("configured max_total_memory %dMB is < shared_memory_size %zuMB",
						   max_total_memory_mb, shmem_mb),
					errhint("Disable or increase the configuration parameter \"max_total_memory\"."));

		/* Decide how much memory is needed to support the connections. */
		connection_mb = asMB(MaxConnections * (initial_allocation_allowance + allocation_allowance_refill_qty));
		required_mb = shmem_mb + connection_mb;

		/* Warning if there isn't anough memory to support the connections */
		if (max_total_memory_mb < required_mb)
			ereport(WARNING,
					errmsg("max_total_memory %dMB should be increased to at least %zuMB to support %d connections",
						   max_total_memory_mb, required_mb, MaxConnections));

		/* We prefer to use max_total_memory_mb as bytes rather than MB */
		max_total_memory_bytes = (int64) max_total_memory_mb * 1024 * 1024;
	}
}


/*
 * Take a snapshot of the global memtrack values if not
 * already done, and point to the snapshot values.
 */
PgStat_Memtrack *
pgstat_fetch_stat_memtrack(void)
{
	/* Take a snapshot of both the memtrack globals and the backends */
	pgstat_read_current_status();

	/* Return a pointer to the globals snapshot */
	return &pgStatLocal.snapshot.memtrack;
}


/*
 * Callback to populate the memtrack globals snapshot with current values.
 */
void
pgstat_memtrack_snapshot_cb(void)
{
	PgStatShared_Memtrack *global = &pgStatLocal.shmem->memtrack;
	PgStat_Memtrack *snap = &pgStatLocal.snapshot.memtrack;

	/* Get a copy of the postmaster's memory allocations */
	pgstat_copy_changecounted_stats(&snap->postmasterMemory,
									&global->postmasterMemory,
									sizeof(snap->postmasterMemory),
									&global->postmasterChangeCount);

	/* Get a copy of the global atomic counters. */
	snap->dsm_reserved = (int64) pg_atomic_read_u64(&global->total_dsm_reserved);
	snap->total_reserved = (int64) pg_atomic_read_u64(&global->total_memory_reserved);
}


/*
 * SQL callable function to get the memory allocation of PG backends.
 * Returns a row for each backend, consisting of:
 *    pid     						- backend's process id
 *    allocated_bytes				- total number of bytes allocated by backend
 *    init_allocated_bytes			- subtotal attributed to each process at startup
 *    aset_allocated_bytes			- subtotal from allocation sets
 *    dsm_allocated_bytes			- subtotal attributed to dynamic shared memory (DSM)
 *    generation_allocated_bytes	- subtotal from generation allocator
 *    slab_allocated_bytes			- subtotal from slab allocator
 */
Datum
pg_stat_get_memory_reservation(PG_FUNCTION_ARGS)
{
#define RESERVATION_COLS    (7)
	int num_backends;
	int backendIdx;
	Datum values[RESERVATION_COLS];
	bool nulls[RESERVATION_COLS];

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	InitMaterializedSRF(fcinfo, 0);

	/* Take a snapshot if not already done */
	pgstat_read_current_status();

	/* Get the postmaster memory reservations and output the row */
	get_postmaster_reservation_row(nulls, values);
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

	/* Do for each backend */
	num_backends = pgstat_fetch_stat_numbackends();
	for (backendIdx = 1; backendIdx <= num_backends; backendIdx++)
	{
		/* Get the backend's memory reservations and output the row */
		get_backend_reservation_row(backendIdx, nulls, values);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum)0;
}


/*
 * Get a backend process' memory reservations as a row of values.
 */
static void
get_backend_reservation_row(int idx,  bool *nulls, Datum *values)
{
	LocalPgBackendStatus *local_beentry;
	PgBackendStatus *beentry;

	/* Fetch the data for the backend */
	local_beentry = pgstat_get_local_beentry_by_index(idx);
	beentry = &local_beentry->backendStatus;

	clearRow(nulls, values, RESERVATION_COLS);

	/* Process id */
	values[0] = Int32GetDatum(beentry->st_procpid);

	/* total memory allocated */
	values[1] = UInt64GetDatum(beentry->st_memory.total);

	/* Subtotals of memory */
	values[2] = UInt64GetDatum(beentry->st_memory.subTotal[PG_ALLOC_INIT]);
	values[3] = UInt64GetDatum(beentry->st_memory.subTotal[PG_ALLOC_ASET]);
	values[4] = UInt64GetDatum(beentry->st_memory.subTotal[PG_ALLOC_DSM]);
	values[5] = UInt64GetDatum(beentry->st_memory.subTotal[PG_ALLOC_GENERATION]);
	values[6] = UInt64GetDatum(beentry->st_memory.subTotal[PG_ALLOC_SLAB]);
}


/*
 * Get the Postmaster's memory allocation as a row of values.
 */
static void
get_postmaster_reservation_row(bool *nulls, Datum *values)
{
	PgStat_Memtrack *memtrack;

	clearRow(nulls, values, RESERVATION_COLS);

	/* Fetch the values and build a row */
	memtrack = pgstat_fetch_stat_memtrack();

	/*  postmaster pid */
	values[0] = PostmasterPid;

	/* Report total menory allocated */
	values[1] = UInt64GetDatum(memtrack->postmasterMemory.total);

	/* Report subtotals of memory allocated */
	/* Subtotals of memory */
	values[2] = UInt64GetDatum(memtrack->postmasterMemory.subTotal[PG_ALLOC_INIT]);
	values[3] = UInt64GetDatum(memtrack->postmasterMemory.subTotal[PG_ALLOC_ASET]);
	values[4] = UInt64GetDatum(memtrack->postmasterMemory.subTotal[PG_ALLOC_DSM]);
	values[5] = UInt64GetDatum(memtrack->postmasterMemory.subTotal[PG_ALLOC_GENERATION]);
	values[6] = UInt64GetDatum(memtrack->postmasterMemory.subTotal[PG_ALLOC_SLAB]);
}


/*
 * SQL callable function to get the server's memory reservation statistics.
 * Returns a single row with the following values (in bytes)
 *   total_memory_reserved   - total memory reserved by server
 *   dsm_memory_reserved     - dsm memory reserved by server
 *   total_memory_available   - memory remaining (null if no limit set)
 *   static_shared_memory     - configured shared memory
 */
Datum
pg_stat_get_global_memory_tracking(PG_FUNCTION_ARGS)
{
#define MEMTRACK_COLS	4

	TupleDesc	tupdesc;
	int64		total_memory_reserved;
	Datum		values[MEMTRACK_COLS];
	bool		nulls[MEMTRACK_COLS];
	PgStat_Memtrack *snap;

	/* Get access to the snapshot */
	snap = pgstat_fetch_stat_memtrack();

	/* Initialise attributes information in the tuple descriptor. */
	tupdesc = CreateTemplateTupleDesc(MEMTRACK_COLS);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "total_memory_reserved",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "dsm_memory_reserved",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "total_memory_available",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "static_shared_memory",
					   INT8OID, -1, 0);
	BlessTupleDesc(tupdesc);

	/* Start with clean row */
	clearRow(nulls, values, MEMTRACK_COLS);

	/* Get total_memory_reserved */
	total_memory_reserved = snap->total_reserved;
	values[0] = Int64GetDatum(total_memory_reserved);

	/* Get dsm_memory_reserved */
	values[1] = Int64GetDatum(snap->dsm_reserved);

	/* Get total_memory_available */
	if (max_total_memory_bytes > 0)
		values[2] = Int64GetDatum(max_total_memory_bytes - total_memory_reserved);
	else
		nulls[2] = true;

	/* Get the static shared memory size in bytes. More precise than GUC value. */
	values[3] = ShmemGetSize();

	/* Return the single record */
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
 * SQL callable function to return the memory reservations
 * for the calling backend. This function returns current
 * accurate numbers, whereas pg_stat_memory_reservation() returns
 * slightly out-of-date, approximate numbers.
 */
Datum
pg_get_backend_memory_allocation(PG_FUNCTION_ARGS)
{
#define BACKEND_COLS (6)
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Datum values[BACKEND_COLS];
	bool nulls[BACKEND_COLS];

	/* A single row */
	InitMaterializedSRF(fcinfo, 0);
	clearRow(nulls, values, BACKEND_COLS);

	/* pid */
	values[0] = UInt32GetDatum(MyProcPid);

	/*
	 * Get the total memory from scanning the contexts.
	 */
	if (TopMemoryContext == NULL)
		nulls[1] = true;
	else
		values[1] = UInt64GetDatum(getContextMemoryTotal());

	/* Report subtotals of memory allocated (don't report "initial" reservation */
	values[2] = UInt64GetDatum(my_memory.subTotal[PG_ALLOC_ASET]);
	values[3] = UInt64GetDatum(my_memory.subTotal[PG_ALLOC_DSM]);
	values[4] = UInt64GetDatum(my_memory.subTotal[PG_ALLOC_GENERATION]);
	values[5] = UInt64GetDatum(my_memory.subTotal[PG_ALLOC_SLAB]);

	/* Return a single tuple */
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	return (Datum) 0;
}

/*
 * How much memory is allocated to contexts.
 * Scan active contexts starting at the top context, and add in freed contexts
 * from the various allocators.
 */
int64
getContextMemoryTotal()
{
	return MemoryContextMemAllocated(TopMemoryContext, true) +
		   AllocSetGetFreeMem();
}

/*
 * Clear out a row of values.
 */
static void
clearRow(bool *nulls, Datum *values, int count)
{
	int idx;
	for (idx = 0; idx < count; idx++)
	{
		nulls[idx] = false;
		values[idx] = (Datum)0;
	}
}

/*
 * Convert size in bytes to size in MB, rounding up.
 */
inline static Size
asMB(Size bytes)
{
	return ((bytes + 1024 * 1024 - 1) / (1024 * 1024));
}
