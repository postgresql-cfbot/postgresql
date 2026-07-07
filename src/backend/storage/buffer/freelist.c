/*-------------------------------------------------------------------------
 *
 * freelist.c
 *	  routines for managing the buffer pool's replacement strategy.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/freelist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pgstat.h"
#include "port/atomics.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/subsystems.h"
#include "port/pg_numa.h"

#define INT_ACCESS_ONCE(var)	((int)(*((volatile int *)&(var))))


/*
 * The shared freelist control information.
 */
typedef struct
{
	/* Spinlock: protects the values below */
	slock_t		buffer_strategy_lock;

	/*
	 * clock-sweep hand: index of next buffer to consider grabbing. Note that
	 * this isn't a concrete buffer - we only ever increase the value. So, to
	 * get an actual buffer, it needs to be used modulo NBuffers.
	 */
	pg_atomic_uint32 nextVictimBuffer;

	/*
	 * Statistics.  These counters should be wide enough that they can't
	 * overflow during a single bgwriter cycle.
	 */
	uint32		completePasses; /* Complete cycles of the clock-sweep */
	pg_atomic_uint32 numBufferAllocs;	/* Buffers allocated since last reset */

	/*
	 * Number of clock-hand values a backend claims per atomic fetch-add.
	 * Computed once at startup (see StrategyCtlShmemInit).  Kept in shared
	 * memory rather than a backend-local static so that EXEC_BACKEND children
	 * (which do not inherit the postmaster's statics) see the same value; a
	 * backend-local copy would silently reset to 1 there, disabling batching
	 * on Windows.
	 */
	uint32		batchSize;

	/*
	 * Bgworker process to be notified upon activity or -1 if none. See
	 * StrategyNotifyBgWriter.
	 */
	int			bgwprocno;
} BufferStrategyControl;

/* Pointers to shared state */
static BufferStrategyControl *StrategyControl = NULL;

static void StrategyCtlShmemRequest(void *arg);
static void StrategyCtlShmemInit(void *arg);

const ShmemCallbacks StrategyCtlShmemCallbacks = {
	.request_fn = StrategyCtlShmemRequest,
	.init_fn = StrategyCtlShmemInit,
};

/*
 * Per-backend state for the batched clock sweep.  Each backend claims a run
 * of consecutive clock-hand values with a single atomic fetch-add and then
 * iterates through them privately, so the contended nextVictimBuffer cache
 * line is touched roughly 1/batch as often.  MyBatchPos is the next hand
 * value to hand out; MyBatchEnd is one past the end of the claimed run.  Both
 * are absolute (monotonically increasing) hand values; the buffer id is the
 * value modulo NBuffers.
 */
static uint32 MyBatchPos = 0;
static uint32 MyBatchEnd = 0;

/*
 * ClockSweepTick - Helper routine for StrategyGetBuffer()
 *
 * Return the next buffer to consider for eviction.  Backends claim batches of
 * consecutive buffer IDs from the shared clock hand, then iterate through
 * them locally without further atomic operations.  This preserves the global
 * sweep order while reducing contention on the shared counter.
 */
static inline uint32
ClockSweepTick(void)
{
	uint32		victim;

	if (MyBatchPos >= MyBatchEnd)
	{
		/*
		 * Claim a fresh batch from the shared clock hand.  This is the only
		 * atomic operation per batch, reducing contention by the batch size.
		 */
		uint32		start;
		uint32		batch_size = StrategyControl->batchSize;

		start = pg_atomic_fetch_add_u32(&StrategyControl->nextVictimBuffer,
										batch_size);

		if (start >= (uint32) NBuffers)
		{
			start = start % NBuffers;

			/*
			 * The counter has grown past NBuffers; try to wrap it back.  We
			 * must hold the spinlock so StrategySyncStart() can read
			 * nextVictimBuffer and completePasses consistently.
			 *
			 * With batching, multiple backends may each land a fetch-add that
			 * returns a value past NBuffers in the same pass.  After
			 * acquiring the spinlock we re-read the counter: if another
			 * backend already wrapped it below NBuffers we are done.
			 */
			SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
			{
				uint32		current;
				uint32		wrapped;

				current = pg_atomic_read_u32(&StrategyControl->nextVictimBuffer);
				if (current >= (uint32) NBuffers)
				{
					wrapped = current % NBuffers;
					if (pg_atomic_compare_exchange_u32(&StrategyControl->nextVictimBuffer,
													   &current, wrapped))
						StrategyControl->completePasses++;
				}
			}
			SpinLockRelease(&StrategyControl->buffer_strategy_lock);
		}
		else if (start + batch_size > (uint32) NBuffers)
		{
			/*
			 * The fetch-add returned a value below NBuffers, but this batch
			 * spans the wrap point (start .. NBuffers-1 then 0 .. k are
			 * iterated locally via "% NBuffers").  That crossing is a
			 * complete pass too, but the wrap-and-increment path above only
			 * fires when the fetch-add return is itself >= NBuffers, so
			 * without this it would go uncounted and completePasses would
			 * drift low (it feeds bgwriter pacing / StrategySyncStart, not
			 * correctness).  The batch is capped at NBuffers, so a batch
			 * spans the wrap at most once; count it under the spinlock for a
			 * consistent (nextVictimBuffer, completePasses) pair.
			 */
			SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
			StrategyControl->completePasses++;
			SpinLockRelease(&StrategyControl->buffer_strategy_lock);
		}

		MyBatchPos = start;
		MyBatchEnd = start + batch_size;
	}

	victim = MyBatchPos % NBuffers;
	MyBatchPos++;

	return victim;
}

/*
 * StrategyGetBuffer
 *
 *	Called by the bufmgr to get the next candidate buffer to use in
 *	GetVictimBuffer(). The only hard requirement GetVictimBuffer() has is that
 *	the selected buffer must not currently be pinned by anyone.
 *
 *	It is the callers responsibility to ensure the buffer ownership can be
 *	tracked via TrackNewBufferPin().
 *
 *	The buffer is pinned and marked as owned, using TrackNewBufferPin(),
 *	before returning.
 */
BufferDesc *
StrategyGetBuffer(uint64 *buf_state)
{
	BufferDesc *buf;
	int			bgwprocno;
	int			trycounter;
	bool		force_cool;

	/*
	 * If asked, we need to waken the bgwriter. Since we don't want to rely on
	 * a spinlock for this we force a read from shared memory once, and then
	 * set the latch based on that value. We need to go through that length
	 * because otherwise bgwprocno might be reset while/after we check because
	 * the compiler might just reread from memory.
	 *
	 * This can possibly set the latch of the wrong process if the bgwriter
	 * dies in the wrong moment. But since PGPROC->procLatch is never
	 * deallocated the worst consequence of that is that we set the latch of
	 * some arbitrary process.
	 */
	bgwprocno = INT_ACCESS_ONCE(StrategyControl->bgwprocno);
	if (bgwprocno != -1)
	{
		/* reset bgwprocno first, before setting the latch */
		StrategyControl->bgwprocno = -1;

		/*
		 * Not acquiring ProcArrayLock here which is slightly icky. It's
		 * actually fine because procLatch isn't ever freed, so we just can
		 * potentially set the wrong process' (or no process') latch.
		 */
		SetLatch(&GetPGProcByNumber(bgwprocno)->procLatch);
	}

	/*
	 * We count buffer allocation requests so that the bgwriter can estimate
	 * the rate of buffer consumption.
	 */
	pg_atomic_fetch_add_u32(&StrategyControl->numBufferAllocs, 1);

	/*
	 * Use the cooling-stage clock sweep to find a victim.
	 *
	 * A buffer is HOT (recently used) or COOL (an eviction candidate).  We
	 * prefer to reclaim an already-COOL buffer and demote a HOT buffer to
	 * COOL only once a full sweep has found no COOL victim (force_cool) -- so
	 * an abundant supply of COOL/probationary pages (e.g. a scan) is drained
	 * before the hot working set is cooled.  Newly loaded pages are admitted
	 * COOL (see BufferAlloc), so scan resistance falls out of the algorithm.
	 *
	 * trycounter bounds the search.  Any tick that does not produce a victim
	 * and does not make progress -- a pinned buffer, or a HOT buffer skipped
	 * on a prefer-COOL pass -- decrements it.  Cooling a HOT buffer (under
	 * force_cool) is progress and resets it.  When a full pass (NBuffers)
	 * makes no progress we escalate to force_cool so the next pass cools HOT
	 * buffers into victims; if a force_cool pass ALSO makes no progress every
	 * buffer is pinned and we fail, matching the stock "no unpinned buffers
	 * available" contract.
	 */
	trycounter = NBuffers;
	force_cool = false;
	for (;;)
	{
		uint64		old_buf_state;
		uint64		local_buf_state;
		bool		no_progress = false;

		buf = GetBufferDescriptor(ClockSweepTick());

		/*
		 * Check whether the buffer can be used and pin it if so. Do this
		 * using a CAS loop, to avoid having to lock the buffer header.
		 */
		old_buf_state = pg_atomic_read_u64(&buf->state);
		for (;;)
		{
			local_buf_state = old_buf_state;

			/* If the buffer is pinned we cannot use it; keep scanning. */
			if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0)
			{
				no_progress = true;
				break;
			}

			/* See equivalent code in PinBuffer() */
			if (unlikely(local_buf_state & BM_LOCKED))
			{
				old_buf_state = WaitBufHdrUnlocked(buf);
				continue;
			}

			if (BUF_STATE_GET_COOLSTATE(local_buf_state) != BUF_COOLSTATE_COOL)
			{
				/*
				 * HOT buffer.  Prefer a COOL victim: on a normal pass just
				 * advance the hand (no progress).  Under force_cool apply the
				 * same second-chance rule the bgwriter's pre-cooling uses: a
				 * HOT buffer whose ref bit is set (recently accessed) has the
				 * ref bit cleared and is left HOT this pass; only a HOT buffer
				 * whose ref bit is already clear is demoted HOT -> COOL.  This
				 * keeps a just-touched buffer from being cooled the instant the
				 * bgwriter falls behind and the foreground has to cool for
				 * itself.  Either transition is progress toward a victim.
				 */
				if (!force_cool)
				{
					no_progress = true;
					break;		/* advance the hand, look for COOL */
				}

				if (BUF_STATE_GET_REFBIT(local_buf_state))
					local_buf_state &= ~BUF_REFBIT;			/* second chance: clear ref, stay HOT */
				else
					local_buf_state &= ~BUF_USAGECOUNT_MASK;	/* HOT -> COOL, clear ref bit */

				if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state,
												   local_buf_state))
				{
					/*
					 * Making a cooling-state transition is progress toward a
					 * victim, so reset the counter.  Stay in force_cool: we made
					 * a transition but have not yet produced a reclaimable COOL
					 * victim (a ref-clear leaves the buffer HOT; a demote leaves
					 * it COOL for a later tick to claim), and dropping out here
					 * would waste a full no-progress pass re-escalating.  An
					 * all-HOT, all-recently-referenced pool thus takes up to ~3
					 * full passes to yield a victim (discover no COOL, clear ref
					 * bits, cool + reclaim) -- still within the stock clock's
					 * worst case (up to BM_MAX_USAGE_COUNT+1 passes), and in
					 * practice the bgwriter pre-cooling keeps a COOL victim
					 * available so force_cool rarely fires at all.  force_cool
					 * ends naturally once a COOL victim is found and returned
					 * below.
					 */
					trycounter = NBuffers;
					break;
				}
			}
			else
			{
				/* COOL and unpinned: claim it.  Pin if the CAS succeeds. */
				local_buf_state += BUF_REFCOUNT_ONE;

				if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state,
												   local_buf_state))
				{
					/* Found a usable buffer */
					*buf_state = local_buf_state;

					TrackNewBufferPin(BufferDescriptorGetBuffer(buf));

					return buf;
				}
			}
		}

		/*
		 * A tick that made no progress toward a victim counts down
		 * trycounter. A full unproductive pass escalates to force_cool (cool
		 * HOT buffers into victims); a second unproductive full pass means
		 * everything is pinned, so fail rather than spin forever.  (A failed
		 * CAS above is neither progress nor a full miss: we simply retry the
		 * same buffer.)
		 */
		if (no_progress && --trycounter == 0)
		{
			if (force_cool)
				elog(ERROR, "no unpinned buffers available");
			force_cool = true;
			trycounter = NBuffers;
		}
	}
}

/*
 * StrategySyncStart -- tell BgBufferSync where to start syncing
 *
 * The result is the buffer index of the best buffer to sync first.
 * BgBufferSync() will proceed circularly around the buffer array from there.
 *
 * In addition, we return the completed-pass count (which is effectively
 * the higher-order bits of nextVictimBuffer) and the count of recent buffer
 * allocs if non-NULL pointers are passed.  The alloc count is reset after
 * being read.
 */
int
StrategySyncStart(uint32 *complete_passes, uint32 *num_buf_alloc)
{
	uint32		nextVictimBuffer;
	int			result;

	SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
	nextVictimBuffer = pg_atomic_read_u32(&StrategyControl->nextVictimBuffer);
	result = nextVictimBuffer % NBuffers;

	if (complete_passes)
	{
		*complete_passes = StrategyControl->completePasses;

		/*
		 * Additionally add the number of wraparounds that happened before
		 * completePasses could be incremented. C.f. ClockSweepTick().
		 */
		*complete_passes += nextVictimBuffer / NBuffers;
	}

	if (num_buf_alloc)
	{
		*num_buf_alloc = pg_atomic_exchange_u32(&StrategyControl->numBufferAllocs, 0);
	}
	SpinLockRelease(&StrategyControl->buffer_strategy_lock);
	return result;
}

/*
 * StrategyNotifyBgWriter -- set or clear allocation notification latch
 *
 * If bgwprocno isn't -1, the next invocation of StrategyGetBuffer will
 * set that latch.  Pass -1 to clear the pending notification before it
 * happens.  This feature is used by the bgwriter process to wake itself up
 * from hibernation, and is not meant for anybody else to use.
 */
void
StrategyNotifyBgWriter(int bgwprocno)
{
	/*
	 * We acquire buffer_strategy_lock just to ensure that the store appears
	 * atomic to StrategyGetBuffer.  The bgwriter should call this rather
	 * infrequently, so there's no performance penalty from being safe.
	 */
	SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
	StrategyControl->bgwprocno = bgwprocno;
	SpinLockRelease(&StrategyControl->buffer_strategy_lock);
}


/*
 * StrategyCtlShmemRequest -- request shared memory for the buffer
 *		cache replacement strategy.
 */
static void
StrategyCtlShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = "Buffer Strategy Status",
					   .size = sizeof(BufferStrategyControl),
					   .ptr = (void **) &StrategyControl
		);
}

/*
 * StrategyCtlShmemInit -- initialize the buffer cache replacement strategy.
 */
static void
StrategyCtlShmemInit(void *arg)
{
	SpinLockInit(&StrategyControl->buffer_strategy_lock);

	/* Initialize the clock-sweep pointer */
	pg_atomic_init_u32(&StrategyControl->nextVictimBuffer, 0);

	/* Clear statistics */
	StrategyControl->completePasses = 0;
	pg_atomic_init_u32(&StrategyControl->numBufferAllocs, 0);

	/* No pending notification */
	StrategyControl->bgwprocno = -1;

	/*
	 * Decide whether to batch the clock sweep.
	 *
	 * Batching claims a run of consecutive buffer IDs per atomic fetch-add so
	 * concurrent backends touch the shared nextVictimBuffer cache line ~1/batch
	 * as often -- a win only when that line actually bounces across sockets,
	 * i.e. on multi-node NUMA hardware.  On a single socket the atomic is
	 * already node-local and batching would only make backends skip ahead for
	 * no benefit, so we fall back to batch size 1 there (byte-identical to the
	 * stock one-buffer-at-a-time clock sweep).
	 *
	 * Batch (> 1) only when libnuma reports more than one node
	 * (pg_numa_get_max_node() >= 1); pg_numa_init() returns -1 when NUMA is
	 * unavailable.  (Benchmarking showed the win holds with or without huge
	 * pages, so huge-page availability is intentionally not part of the gate.)
	 */
	if (pg_numa_init() != -1 &&
		pg_numa_get_max_node() >= 1)
		StrategyControl->batchSize = Min(PG_CACHE_LINE_SIZE / (uint32) sizeof(uint32),
										 (uint32) NBuffers);
	else
		StrategyControl->batchSize = 1;
}
