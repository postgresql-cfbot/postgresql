/*-------------------------------------------------------------------------
 *
 * buf_resize.c
 *	  Online resize coordinator for the shared buffer pool.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_resize.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>
#include <signal.h>

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/dynamic_shared_buffers.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/injection_point.h"

PG_FUNCTION_INFO_V1(pg_resize_shared_buffers);

/*
 * `coordinator_active` tells the cleanup callback whether *this* backend
 * currently holds the resize_in_progress flag.
 *
 * `cleanup_registered` ensures we only call before_shmem_exit() once per
 * backend lifetime.
 *
 * `inflight_expand_target` is non-zero when DoExpand starts. The cleanup
 * callback uses it to surface a WARNING if an expand was interrupted.
 */
static volatile bool coordinator_active = false;
static volatile bool cleanup_registered = false;
static volatile int inflight_expand_target = 0;

/*
 * Emit a (key, value, unit) tuple to the function's result set. If value_null
 * is true, value and unit are emitted as NULL.
 *
 * Used to return tuples from the pg_resize_shared_buffers() function. The
 * tupledesc of the returned rows must match the function's OUT arguments.
 */
static void
EmitResizeMetricRow(ReturnSetInfo *rsinfo, const char *key, double value,
					const char *unit, bool value_null)
{
	Datum		values[3];
	bool		nulls[3];

	values[0] = CStringGetTextDatum(key);
	nulls[0] = false;
	if (value_null)
	{
		nulls[1] = true;
		values[1] = (Datum) 0;
		nulls[2] = true;
		values[2] = (Datum) 0;
	}
	else
	{
		nulls[1] = false;
		nulls[2] = false;
		values[1] = Float8GetDatum(value);
		values[2] = CStringGetTextDatum(unit);
	}
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}

/* Rounded timing row; value is in seconds, unit is "seconds". */
static void
EmitResizeTimeRow(ReturnSetInfo *rsinfo, const char *key, double elapsed_sec)
{
	double		t = round(elapsed_sec * 100.0) / 100.0;

	EmitResizeMetricRow(rsinfo, key, t, "seconds", false);
}

static void
EmitResizeBytesRow(ReturnSetInfo *rsinfo, const char *key, double bytes)
{
	EmitResizeMetricRow(rsinfo, key, bytes, "bytes", false);
}

static void
SharedBufferResizeBarrier(ProcSignalBarrierType barrier, const char *barrier_name)
{
	WaitForProcSignalBarrier(EmitProcSignalBarrier(barrier));
	elog(LOG, "all backends acknowledged %s barrier", barrier_name);
}

/*
 * Parse a user-supplied size string (e.g. "256MB", "32768") into a number of
 * shared buffers.  Raises ERROR on invalid input or out-of-range size.
 */
static int
ParseNewSize(const char *new_size_str)
{
	const char *hintmsg = NULL;
	int			new_size;

	if (!parse_int(new_size_str, &new_size, GUC_UNIT_BLOCKS, &hintmsg))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for shared_buffers: \"%s\"", new_size_str),
				 hintmsg ? errhint("%s", _(hintmsg)) : 0));

	if (new_size < MIN_SHARED_BUFFERS)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("shared_buffers must be at least %d, got %d",
						MIN_SHARED_BUFFERS, new_size)));

	if (new_size > GetMaxNBuffers())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("shared_buffers (%d) cannot exceed max_shared_buffers (%d)",
						new_size, GetMaxNBuffers())));

	return new_size;
}

/*
 * Sleep up to `timeout_ms` milliseconds.
 */
static void
ResizeWaitMs(int timeout_ms)
{
	int			rc;

	rc = WaitLatch(MyLatch,
				   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				   timeout_ms,
				   WAIT_EVENT_PG_SLEEP);
	if (rc & WL_LATCH_SET)
		ResetLatch(MyLatch);
	CHECK_FOR_INTERRUPTS();
}

/*
 * Shrink protocol: lower lowNBuffers first to restrict allocations, evict
 * the [low, high) range, then drop highNBuffers to lowNBuffers, and only then
 * release the OS-level memory backing that range.
 *
 * - pre:  lowNBuffers == highNBuffers == old_size > new_size
 * - post (success):
 *     lowNBuffers == highNBuffers == new_size
 * - post (interrupted before highNBuffers is lowered):
 *     lowNBuffers == new_size, highNBuffers == old_size
 *     (recoverable: ResetResizeInProgress() rolls lowNBuffers back to high)
 * - post (madvise(MADV_REMOVE) failure during BufferManagerShmemShrink):
 *     lowNBuffers == highNBuffers == new_size. We cannot roll back the shrink.
 *
 * Raises ERROR on unrecoverable failure.
 */
static void
DoShrink(ReturnSetInfo *rsinfo, int old_size, int new_size)
{
	instr_time	phase_start;
	instr_time	phase_end;
	Size		mem_bytes;
	bool		shrink_success;

	Assert(new_size < old_size);
	Assert(pg_atomic_read_u32(&DSBCtrl->lowNBuffers) ==
		   pg_atomic_read_u32(&DSBCtrl->highNBuffers));

	CHECK_FOR_INTERRUPTS();

	/*
	 * Reset the clock-sweep cursor before lowering the low water mark. The
	 * existing cursor may point above new_size. Once we publish the new
	 * lowNBuffers, ClockSweepTick() may otherwise immediately wrap past
	 * the new buffers via modulo arithmetic. Resetting to 0 means the
	 * next sweep starts from the bottom of the surviving range.
	 */
	StrategyReset(old_size, new_size);

	elog(LOG, "[Shrink Barrier]: restricting allocations to %d buffers", new_size);
	INSTR_TIME_SET_CURRENT(phase_start);
	/*
	 * Wait for all backends to acknowledge the new lowNBuffers. After the
	 * barrier returns, all new buffer allocations will land in [0, lowNBuffers)
	 * range. For buffers in [lowNBuffers, highNBuffers), backends can
	 * hold pins and create new pins on buffers already pinned.
	 * The EvictExtraBuffers() loop below will wait for all buffers in
	 * [lowNBuffers, highNBuffers) to be unpinned.
	 */
	SharedBufferResizeBarrier(PROCSIGNAL_BARRIER_SHBUF_RESIZE, CppAsString(PROCSIGNAL_BARRIER_SHBUF_RESIZE));
	INSTR_TIME_SET_CURRENT(phase_end);
	INSTR_TIME_SUBTRACT(phase_end, phase_start);
	EmitResizeTimeRow(rsinfo, "Barrier", INSTR_TIME_GET_DOUBLE(phase_end));
	elog(LOG, "[Shrink Barrier]: Restricted allocations to %d buffers in %f seconds", new_size, INSTR_TIME_GET_DOUBLE(phase_end));

	INJECTION_POINT("buf-resize-shrink-after-barrier", NULL);

	/*
	 * Evict all pages in [lowNBuffers, highNBuffers).
	 */
	elog(LOG, "[Shrink]: evicting buffers %u..%u", new_size, old_size);
	INSTR_TIME_SET_CURRENT(phase_start);
	{
		instr_time	last_log;

		INSTR_TIME_SET_CURRENT(last_log);
		while (!EvictExtraBuffers(new_size, old_size))
		{
			instr_time	now;

			ResizeWaitMs(100);

			INSTR_TIME_SET_CURRENT(now);
			INSTR_TIME_SUBTRACT(now, last_log);
			if (INSTR_TIME_GET_DOUBLE(now) >= 5.0)
			{
				elog(LOG, "still waiting for buffers to be unpinned");
				INSTR_TIME_SET_CURRENT(last_log);
			}
		}
	}
	INSTR_TIME_SET_CURRENT(phase_end);
	INSTR_TIME_SUBTRACT(phase_end, phase_start);
	EmitResizeTimeRow(rsinfo, "Buffer relocation", INSTR_TIME_GET_DOUBLE(phase_end));
	elog(LOG, "[Shrink]: evicted %d buffers in %f seconds", old_size - new_size, INSTR_TIME_GET_DOUBLE(phase_end));

	CHECK_FOR_INTERRUPTS();
	/*
	 * All the victim buffers are now empty and won't be allocated by backends.
	 * Take AccessNBuffersLock in exclusive mode so we wait for any backend
	 * still iterating with the old highNBuffers and prevent new
	 * ones from starting until we published the new highNBuffers.
	 */
	INSTR_TIME_SET_CURRENT(phase_start);
	LWLockAcquire(&DSBCtrl->AccessNBuffersLock, LW_EXCLUSIVE);
	pg_atomic_write_u32(&DSBCtrl->highNBuffers, new_size);
	LWLockRelease(&DSBCtrl->AccessNBuffersLock);

	INJECTION_POINT("buf-resize-shrink-before-madvise", NULL);

	/*
	 * Release the memory.
	 */
	mem_bytes = BufferManagerShmemShrink(new_size, old_size, &shrink_success);
	INSTR_TIME_SET_CURRENT(phase_end);
	INSTR_TIME_SUBTRACT(phase_end, phase_start);
	EmitResizeTimeRow(rsinfo, "Shrink shmem", INSTR_TIME_GET_DOUBLE(phase_end));
	EmitResizeBytesRow(rsinfo, "Shrink shmem", (double) mem_bytes);
	elog(LOG, "[Shrink]: released %zu bytes of memory in %f seconds", mem_bytes, INSTR_TIME_GET_DOUBLE(phase_end));

	if (!shrink_success)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("shared_buffers shrink from %d to %d failed",
						old_size, new_size),
				 errdetail("madvise(MADV_REMOVE) failed while releasing buffer-pool memory; the failure is not recoverable."),
				 errhint("Check the server log for the underlying madvise() error.")));
}

/*
 * Expand protocol: allocate memory for the [old_size, new_size) range,
 * initialize the new buffer descriptors, then publish both new lowNBuffers
 * and highNBuffers atomically under the exclusive lock.
 *
 * - pre:  lowNBuffers == highNBuffers == old_size < new_size
 * - post (success):
 *     lowNBuffers == highNBuffers == new_size
 * - post (madvise(MADV_POPULATE_WRITE) failure during BufferManagerShmemExpand):
 *     lowNBuffers == highNBuffers == old_size (water marks NOT advanced)
 *     Some bytes in [old_size, new_size) of the four buffer-pool arrays may
 *     have been allocated from the OS but never published to backends.
 *
 * Raises ERROR on madvise failure.
 */
static void
DoExpand(ReturnSetInfo *rsinfo, int old_size, int new_size)
{
	instr_time	phase_start;
	instr_time	phase_end;
	Size		mem_bytes;
	bool		expand_success;

	Assert(new_size > old_size);
	Assert(pg_atomic_read_u32(&DSBCtrl->lowNBuffers) ==
		   pg_atomic_read_u32(&DSBCtrl->highNBuffers));

	INSTR_TIME_SET_CURRENT(phase_start);

	inflight_expand_target = new_size;

	/*
	 * Allocate physical memory and initialize the new buffer descriptors
	 * BEFORE acquiring AccessNBuffersLock. Backends iterating the buffer
	 * pool only look at [0, highNBuffers); since highNBuffers is still at
	 * old_size, the new range is invisible to them, so it is safe to touch
	 * without the lock.
	 */
	mem_bytes = BufferManagerShmemExpand(old_size, new_size, &expand_success);
	if (!expand_success)
	{
		INSTR_TIME_SET_CURRENT(phase_end);
		INSTR_TIME_SUBTRACT(phase_end, phase_start);
		EmitResizeTimeRow(rsinfo, "Expand shmem", INSTR_TIME_GET_DOUBLE(phase_end));
		EmitResizeBytesRow(rsinfo, "Expand shmem", (double) mem_bytes);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("shared_buffers expand from %d to %d failed",
						old_size, new_size),
				 errdetail("madvise(MADV_POPULATE_WRITE) failed while populating buffer-pool memory; the new range was not made visible to backends."),
				 errhint("Check the server log for the underlying madvise() error and retry.")));
	}

	BufferManagerShmemInitBuffers(old_size, new_size);

	INJECTION_POINT("buf-resize-expand-before-publish", NULL);

	/*
	 * Hold AccessNBuffersLock in exclusive mode while we publish the new
	 * water marks. Backends taking the lock in shared mode (e.g. via
	 * BEGIN_NBUFFERS_ACCESS) either run entirely before this critical
	 * section and see lowNBuffers == highNBuffers == old_size, or entirely
	 * after and see lowNBuffers == highNBuffers == new_size with valid
	 * memory; they never observe the partially initialized intermediate
	 * state.  Concurrent atomics readers (clock sweep / freelist) may
	 * briefly see lowNBuffers < highNBuffers between the two writes below;
	 * that is fine because both bounds are now backed by initialized
	 * memory, so a clock sweep wrapping into the [old_size, new_size) range
	 * is safe.
	 */
	LWLockAcquire(&DSBCtrl->AccessNBuffersLock, LW_EXCLUSIVE);
	/*
	 * Reset the clock-sweep cursor to the start of the new buffers so the
	 * next clock pass tries the freshly added empty buffers before
	 * re-scanning existing ones with usage_count == 0.
	 */
	StrategyReset(old_size, new_size);
	LWLockRelease(&DSBCtrl->AccessNBuffersLock);

	/*
	 * The expand is complete.
	 */
	inflight_expand_target = 0;

	INSTR_TIME_SET_CURRENT(phase_end);
	INSTR_TIME_SUBTRACT(phase_end, phase_start);
	EmitResizeTimeRow(rsinfo, "Expand shmem", INSTR_TIME_GET_DOUBLE(phase_end));
	EmitResizeBytesRow(rsinfo, "Expand shmem", (double) mem_bytes);
	elog(LOG, "[Expand]: expanded buffer pool memory with %zu bytes in %f seconds", mem_bytes, INSTR_TIME_GET_DOUBLE(phase_end));
}

/*
 * Cleanup callback. Runs from the transaction-abort PG_CATCH path *and* from
 * before_shmem_exit() if the backend dies while holding the resize slot.
 *
 * Rollback policy:
 *   - Partial shrink (lowNBuffers < highNBuffers): restore lowNBuffers to
 *     highNBuffers so the buffer pool is consistent at the larger size.
 *     Memory for [lowNBuffers, highNBuffers) is still mapped, so rolling
 *     back is safe.
 *   - Partial expand: BufferManagerShmemExpand() may have populated some of
 *     [old_size, inflight_expand_target) without publishing the new water
 *     marks. This is wasteful but harmless. We surface a WARNING so operators
 *     know to retry the resize.
 */
static void
ResetResizeInProgress(int code, Datum arg)
{
	uint32		high;
	uint32		low;
	int			expand_target;
	bool		shrink_failed = false;
	bool		expand_failed = false;

	if (!coordinator_active || DSBCtrl == NULL)
		return;

	Assert(DSBCtrl->resize_in_progress);
	Assert(DSBCtrl->coordinator_pid == MyProcPid);

	coordinator_active = false;

	high = pg_atomic_read_u32(&DSBCtrl->highNBuffers);
	low = pg_atomic_read_u32(&DSBCtrl->lowNBuffers);
	if (low < high)
	{
		shrink_failed = true;
		pg_atomic_write_u32(&DSBCtrl->lowNBuffers, high);
	}

	expand_target = inflight_expand_target;
	if (expand_target != 0)
	{
		expand_failed = true;
		inflight_expand_target = 0;
	}

	ReleaseResizeCoordinator();

	/*
	 * Emit user-visible warnings AFTER all critical cleanup.
	 */
	if (shrink_failed)
		ereport(WARNING,
				(errmsg("shared_buffers shrink was interrupted; rolling back lowNBuffers from %u to %u",
						(unsigned) low, (unsigned) high)));

	if (expand_failed)
		ereport(WARNING,
				(errmsg("shared_buffers expand to %d was interrupted",
						expand_target),
				 errdetail("Some memory in [%u, %d) may have been allocated from the OS but not made visible to backends. It will sit unused in shmem until a future successful resize re-initializes that range.",
						   (unsigned) high, expand_target)));
}

Datum
pg_resize_shared_buffers(PG_FUNCTION_ARGS)
{
	instr_time	total_start;
	instr_time	total_end;

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			old_size;
	int			new_size;
	char	   *new_size_str;

	INSTR_TIME_SET_CURRENT(total_start);

	if (!enable_dynamic_shared_buffers)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("shared buffer pool resizing requires enable_dynamic_shared_buffers")));

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to resize shared_buffers")));

	if (PG_NARGS() != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_resize_shared_buffers requires exactly one argument (the new shared_buffers value)")));
	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("new_size argument to pg_resize_shared_buffers must not be NULL")));

	/*
	 * Restrict callers to regular client backends.
	 */
	Assert(MyBackendType == B_BACKEND);

	/*
	 * Parse the requested size first so we fail fast on bad input before
	 * claiming the resize_in_progress flag.
	 */
	new_size_str = text_to_cstring(PG_GETARG_TEXT_PP(0));
	new_size = ParseNewSize(new_size_str);

	InitMaterializedSRF(fcinfo, 0);

	/*
	 * Register the FATAL-exit cleanup once per backend lifetime.
	 */
	if (!cleanup_registered)
	{
		before_shmem_exit(ResetResizeInProgress, (Datum) 0);
		cleanup_registered = true;
	}

	if (!ClaimResizeCoordinator())
	{
		elog(LOG, "shared buffer resizing is already in progress");
		EmitResizeMetricRow(rsinfo, "resize already in progress", 0, NULL, true);
		return (Datum) 0;
	}

	/*
	 * Mark this backend as the local coordinator so the cleanup callback
	 * knows to release the shared slot on error / exit.
	 */
	coordinator_active = true;

	PG_TRY();
	{
		INJECTION_POINT("buf-resize-after-claim", NULL);

		old_size = pg_atomic_read_u32(&DSBCtrl->lowNBuffers);
		/*
		 * The highNBuffers should be equal to lowNBuffers.
		 */
		Assert(pg_atomic_read_u32(&DSBCtrl->highNBuffers) == old_size);

		if (old_size == new_size)
		{
			elog(LOG, "shared buffers are already at %d, no need to resize", old_size);
			EmitResizeTimeRow(rsinfo, "No resize", 0.0);
		}
		else
		{
			elog(LOG, "resizing shared buffers from %d to %d", old_size, new_size);

			if (new_size < old_size)
				DoShrink(rsinfo, old_size, new_size);
			else
				DoExpand(rsinfo, old_size, new_size);

			Assert(pg_atomic_read_u32(&DSBCtrl->lowNBuffers) == (uint32) new_size);
			Assert(pg_atomic_read_u32(&DSBCtrl->highNBuffers) == (uint32) new_size);

			INSTR_TIME_SET_CURRENT(total_end);
			INSTR_TIME_SUBTRACT(total_end, total_start);
			EmitResizeTimeRow(rsinfo, "Total Resize Time", INSTR_TIME_GET_DOUBLE(total_end));
			elog(LOG, "successfully resized shared buffers to %d", new_size);
		}
	}
	PG_CATCH();
	{
		ResetResizeInProgress(0, (Datum) 0);
		PG_RE_THROW();
	}
	PG_END_TRY();

	ResetResizeInProgress(0, (Datum) 0);
	return (Datum) 0;
}
