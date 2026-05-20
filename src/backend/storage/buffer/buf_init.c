/*-------------------------------------------------------------------------
 *
 * buf_init.c
 *	  buffer manager initialization routines
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_init.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#ifdef __linux__
#include <sys/mman.h>
#endif

#include "miscadmin.h"
#include "storage/aio.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/pg_shmem.h"
#include "storage/shmem.h"
#include "utils/memdebug.h"

BufferDescPadded *BufferDescriptors;
char	   *BufferBlocks;
ConditionVariableMinimallyPadded *BufferIOCVArray;
WritebackContext BackendWritebackContext;
CkptSortItem *CkptBufferIds;

static void BufferManagerShmemRequest(void *arg);
static void BufferManagerShmemInit(void *arg);
static void BufferManagerShmemAttach(void *arg);

const ShmemCallbacks BufferManagerShmemCallbacks = {
	.request_fn = BufferManagerShmemRequest,
	.init_fn = BufferManagerShmemInit,
	.attach_fn = BufferManagerShmemAttach,
};

/*
 * Data Structures:
 *		buffers live in a freelist and a lookup data structure.
 *
 *
 * Buffer Lookup:
 *		Two important notes.  First, the buffer has to be
 *		available for lookup BEFORE an IO begins.  Otherwise
 *		a second process trying to read the buffer will
 *		allocate its own copy and the buffer pool will
 *		become inconsistent.
 *
 * Buffer Replacement:
 *		see freelist.c.  A buffer cannot be replaced while in
 *		use either by data manager or during IO.
 *
 *
 * Synchronization/Locking:
 *
 * IO_IN_PROGRESS -- this is a flag in the buffer descriptor.
 *		It must be set when an IO is initiated and cleared at
 *		the end of the IO.  It is there to make sure that one
 *		process doesn't start to use a buffer while another is
 *		faulting it in.  see WaitIO and related routines.
 *
 * refcount --	Counts the number of processes holding pins on a buffer.
 *		A buffer is pinned during IO and immediately after a BufferAlloc().
 *		Pins must be released before end of transaction.  For efficiency the
 *		shared refcount isn't increased if an individual backend pins a buffer
 *		multiple times. Check the PrivateRefCount infrastructure in bufmgr.c.
 */

/*
 * Initialize a single buffer descriptor.
 *
 * Buffers are exclusively found via clock sweep (the freelist was removed
 * in commit 2c789405275). This function is called both from
 * BufferManagerShmemInit at boot and from BufferManagerShmemInitBuffers
 * during an online expand.
 */
static void
InitializeBuffer(int buf_id)
{
	BufferDesc *buf = GetBufferDescriptor(buf_id);

	ClearBufferTag(&buf->tag);
	pg_atomic_init_u32(&buf->state, 0);
	buf->wait_backend_pgprocno = INVALID_PROC_NUMBER;
	buf->buf_id = buf_id;
	pgaio_wref_clear(&buf->io_wref);

	LWLockInitialize(BufferDescriptorGetContentLock(buf),
					 LWTRANCHE_BUFFER_CONTENT);

	ConditionVariableInit(BufferDescriptorGetIOCV(buf));
}

/*
 * Page size used both to lay out the buffer-pool arrays in shared memory and
 * to align the per-slice madvise() ranges issued during expand/shrink.
 */
static Size
buffer_pool_madvise_alignment(void)
{
#ifdef __linux__
	if (huge_pages == HUGE_PAGES_ON)
	{
		Size		hugepagesize = 0;

		GetHugePageSize(&hugepagesize, NULL);
		if (hugepagesize > 0)
			return hugepagesize;
		/* Conservative fallback if /proc/meminfo lookup failed. */
		return (Size) 2 * 1024 * 1024;
	}
#endif
	return (Size) sysconf(_SC_PAGESIZE);
}

/*
 * Return the exact byte length of the expanded range [lowNBuffers,
 * highNBuffers) that this call touches (memset / madvise), or 0 when a no-op
 * or on platforms where the Linux path is not used. On
 * madvise(MADV_POPULATE_WRITE) failure, *success is set to false and 0 is
 * returned; the new range is not guaranteed to be backed by physical memory,
 * so callers should stop expanding rather than continue and risk a SIGBUS on
 * first touch.
 */
static Size
BufferPoolArrayPhysicalExpand(void *baseptr, Size elem_size,
							  int lowNBuffers, int highNBuffers,
							  bool *success)
{
#ifdef __linux__
	char	   *base;
	Size		off;
	Size		len;
	uintptr_t	region_start;
	uintptr_t	region_end;
	uintptr_t	ms;
	uintptr_t	me;
	Size		os_page_size = buffer_pool_madvise_alignment();
#endif

	if (baseptr == NULL || elem_size == 0 || highNBuffers <= lowNBuffers)
		return 0;

#ifdef __linux__
	base = (char *) baseptr;
	Assert(os_page_size != 0);

	off = mul_size((Size) lowNBuffers, elem_size);
	len = mul_size((Size) (highNBuffers - lowNBuffers), elem_size);

	region_start = (uintptr_t) (base + off);
	region_end = region_start + len;

	ms = TYPEALIGN_DOWN((Size) os_page_size, region_start);
	me = TYPEALIGN((Size) os_page_size, region_end);

#ifdef USE_VALGRIND
	VALGRIND_MAKE_MEM_DEFINED((void *) region_start, len);
#endif

#if defined(MADV_HUGEPAGE) && defined(MADV_POPULATE_WRITE)
#ifdef USE_ASSERT_CHECKING
	if (mprotect((void *) ms, me - ms, PROT_READ | PROT_WRITE) < 0 && errno != ENOMEM)
		elog(WARNING, "mprotect(PROT_READ|PROT_WRITE) before buffer pool expand: %m");
#endif
	/*
	 * If huge pages is on, MADV_HUGEPAGE advice will fail.
	 */
	if (huge_pages_status != HUGE_PAGES_ON &&
		madvise((void *) ms, me - ms, MADV_HUGEPAGE) < 0)
		elog(WARNING, "madvise(MADV_HUGEPAGE) on expanded buffer pool array: %m");

	if (madvise((void *) ms, me - ms, MADV_POPULATE_WRITE) < 0)
	{
		elog(WARNING, "madvise(MADV_POPULATE_WRITE) on expanded buffer pool array: %m");
		*success = false;
		return 0;
	}
#else
	/*
	 * No MADV_POPULATE_WRITE on this platform: memset is the only way to
	 * force population. memset can't return a failure, so this path always
	 * "succeeds"; if the underlying mapping is actually unbacked the SIGBUS
	 * will hit during the memset itself.
	 */
	memset((void *) region_start, 0, len);
#endif

	return len;
#else
	return 0;					/* no local physical work off Linux */
#endif
}

/*
 * Return the exact byte length passed to a successful MADV_REMOVE, or 0 if no
 * page-aligned run was freed (no-op case).  On madvise() failure, *success is
 * set to false and 0 is returned.
 *
 * The released slice is [lowNBuffers, highNBuffers); we trim physical storage
 * for the entire inactive tail [lowNBuffers, MaxNBuffers) so that incremental
 * shrinks don't strand page-aligned spans above highNBuffers (see comment
 * inside).
 */
static Size
BufferPoolArrayPhysicalShrink(void *baseptr, Size elem_size,
							  int lowNBuffers, int highNBuffers,
							  bool *success)
{
#ifdef __linux__
	char	   *base;
	Size		off;
	Size		tail_len;
	Size		logical_len;
	uintptr_t	region_start;
	uintptr_t	region_end;
	uintptr_t	ms;
	uintptr_t	me;
	Size		os_page_size = buffer_pool_madvise_alignment();
#endif

	if (baseptr == NULL || elem_size == 0 || lowNBuffers >= highNBuffers)
		return 0;

#ifdef __linux__
	base = (char *) baseptr;
	Assert(os_page_size != 0);

	off = mul_size((Size) lowNBuffers, elem_size);
	/* See function header: tail spans up to MaxNBuffers, not highNBuffers. */
	tail_len = mul_size((Size) (GetMaxNBuffers() - lowNBuffers), elem_size);
	logical_len = mul_size((Size) (highNBuffers - lowNBuffers), elem_size);

	region_start = (uintptr_t) (base + off);
	region_end = region_start + tail_len;

	/*
	 * MADV_REMOVE requires a page-aligned address and a multiple of the page
	 * size for length.  Only full pages wholly inside the released logical
	 * range can be trimmed.
	 */
	ms = TYPEALIGN((Size) os_page_size, region_start);
	me = TYPEALIGN_DOWN((Size) os_page_size, region_end);

	if (ms >= me)
		return 0;

	if (madvise((void *) ms, me - ms, MADV_REMOVE) < 0)
	{
		elog(WARNING, "madvise(MADV_REMOVE) on buffer pool array tail failed: %m");
		*success = false;
		return 0;
	}

#ifdef USE_VALGRIND
	VALGRIND_MAKE_MEM_NOACCESS((void *) ms, me - ms);
#endif
#ifdef USE_ASSERT_CHECKING
	/*
	 * Catch stray reads/writes after shrink.
	 */
	if (mprotect((void *) ms, me - ms, PROT_NONE) < 0 && errno != ENOMEM)
		elog(WARNING, "mprotect(PROT_NONE) on buffer pool array tail: %m");
#endif

	return logical_len;
#else
	return 0;					/* no local physical work off Linux */
#endif
}

/*
 * Register shared memory area for the buffer pool.
 */
static void
BufferManagerShmemRequest(void *arg)
{
	bool		foundBufs,
				foundDescs,
				foundIOCV,
				foundBufCkpt;
	int			max_nbuffers;
	Size		os_page_size = buffer_pool_madvise_alignment();
	Assert(os_page_size != 0);

	if (enable_dynamic_shared_buffers)
	{
		if (MaxNBuffers == 0)
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("max_shared_buffers must be set when enable_dynamic_shared_buffers is on"),
					 errhint("Set max_shared_buffers to a value at least as large as shared_buffers.")));
		if (MaxNBuffers < NBuffersGUC)
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("max_shared_buffers (%d) must be at least shared_buffers (%d) when enable_dynamic_shared_buffers is on",
							MaxNBuffers, NBuffersGUC)));
	}

	max_nbuffers = GetMaxNBuffers();

	/* Align descriptors for madvise (same granularity as buffer blocks). */
	BufferDescriptors = (BufferDescPadded *)
		TYPEALIGN(os_page_size,
				  ShmemInitStruct("Buffer Descriptors",
								  max_nbuffers * sizeof(BufferDescPadded) + 2 * os_page_size,
								  &foundDescs));

	ShmemRequestStruct(.name = "Buffer Blocks",
					   .size = NBuffers * (Size) BLCKSZ,
	/* Align buffer pool on IO page size boundary. */
	BufferBlocks = (char *)
		TYPEALIGN(os_page_size,
				ShmemInitStruct("Buffer Blocks",
					max_nbuffers * (Size) BLCKSZ + 2 * os_page_size,
								&foundBufs));

	/* Align I/O condition variables for madvise. */
	BufferIOCVArray = (ConditionVariableMinimallyPadded *)
		TYPEALIGN(os_page_size,
				  ShmemInitStruct("Buffer IO Condition Variables",
								  max_nbuffers * sizeof(ConditionVariableMinimallyPadded) + 2 * os_page_size,
								  &foundIOCV));

	/*
	 * The array used to sort to-be-checkpointed buffer ids is located in
	 * shared memory, to avoid having to allocate significant amounts of
	 * memory at runtime. As that'd be in the middle of a checkpoint, or when
	 * the checkpointer is restarted, memory allocation failures would be
	 * painful.
	 */
	CkptBufferIds = (CkptSortItem *)
		TYPEALIGN(os_page_size,
				  ShmemInitStruct("Checkpoint BufferIds",
								  max_nbuffers * sizeof(CkptSortItem) + 2 * os_page_size,
								  &foundBufCkpt));

/*
 * Initialize shared buffer pool
 *
 * This is called once during shared-memory initialization (either in the
 * postmaster, or in a standalone backend).
 */
static void
BufferManagerShmemInit(void *arg)
{
	/*
	 * Initialize all the buffer headers.
	 */
	for (int i = 0; i < NBuffers; i++)
	{
		int			i;

		if (enable_dynamic_shared_buffers)
		{
			bool		success = true;

			/*
			 * Request physical memory for NBuffersGUC. A madvise failure
			 * here means we cannot eagerly populate the initial buffer
			 * pool; rather than start with possibly-unbacked memory and
			 * SIGBUS on first access, we PANIC so the postmaster fails
			 * to start cleanly.
			 */
			BufferManagerShmemExpand(0, NBuffersGUC, &success);
			if (!success)
				elog(PANIC, "could not populate initial shared buffer pool: madvise(MADV_POPULATE_WRITE) failed");
		}

		/*
		 * Initialize all the buffer headers for the active pool size.
		 * The clock sweep is the sole replacement mechanism, so there is
		 * no freelist to link them into.
		 */
		for (i = 0; i < NBuffersGUC; i++)
			InitializeBuffer(i);
	}

	/* Initialize per-backend file flush context */
	WritebackContextInit(&BackendWritebackContext,
						 &backend_flush_after);

	/*
	 * Initialize the DSB water marks. DSBCtrl is NULL in special contexts
	 * such as the WAL redo process, where DSB is not used.
	 */
	if (DSBCtrl != NULL && !foundDescs)
	{
		pg_atomic_write_u32(&DSBCtrl->lowNBuffers, NBuffersGUC);
		pg_atomic_write_u32(&DSBCtrl->highNBuffers, NBuffersGUC);
	}
}

/*
 * BufferManagerShmemSize
 *
 * All buffer arrays are allocated in the single shared-memory heap.  We size
 * them to GetMaxNBuffers() so the pool fits its upper bound.
 */
Size
BufferManagerShmemSize(void)
{
	Size		size = 0;
	Size		os_page_size = buffer_pool_madvise_alignment();
	int			max_nbuffers = GetMaxNBuffers();
	Assert(os_page_size != 0);

	/* size of buffer descriptors, plus alignment padding for madvise */
	size = add_size(size, mul_size(max_nbuffers, sizeof(BufferDescPadded)));
	size = add_size(size, mul_size(2, os_page_size));

	/* size of data pages, plus alignment padding */
	size = add_size(size, mul_size(2, os_page_size));
	size = add_size(size, mul_size(max_nbuffers, (Size) BLCKSZ));

	/* size of stuff controlled by freelist.c */
	size = add_size(size, StrategyShmemSize());

	/* size of I/O condition variables, plus alignment padding for madvise */
	size = add_size(size, mul_size(max_nbuffers,
								   sizeof(ConditionVariableMinimallyPadded)));
	size = add_size(size, mul_size(2, os_page_size));

	/* size of checkpoint sort array in bufmgr.c, plus alignment padding */
	size = add_size(size, mul_size(max_nbuffers, sizeof(CkptSortItem)));
	size = add_size(size, mul_size(2, os_page_size));

	return size;
}

/*
 * Allocate backing memory pages from the OS for the buffer-pool slice
 * [lowNBuffers, highNBuffers).
 *
 * Returns the sum, over the four buffer-pool arrays, of the exact byte length
 * each BufferPoolArrayPhysicalExpand call touched. *success is set to true on
 * success, or false if any madvise(MADV_POPULATE_WRITE) call failed.
 */
Size
BufferManagerShmemExpand(int lowNBuffers, int highNBuffers, bool *success)
{
	int			max_nbuffers = GetMaxNBuffers();
	Size		total = 0;

	if (highNBuffers > max_nbuffers)
		elog(PANIC, "buffer pool expand exceeds allocation (low=%d high=%d allocated=%d)",
			 lowNBuffers, highNBuffers, max_nbuffers);

	Assert(lowNBuffers < highNBuffers);

	*success = true;

	total = add_size(total, BufferPoolArrayPhysicalExpand(BufferDescriptors, sizeof(BufferDescPadded),
														  lowNBuffers, highNBuffers, success));
	if (!*success)
		return total;

	total = add_size(total, BufferPoolArrayPhysicalExpand(BufferBlocks, (Size) BLCKSZ,
														  lowNBuffers, highNBuffers, success));
	if (!*success)
		return total;

	total = add_size(total, BufferPoolArrayPhysicalExpand(BufferIOCVArray,
														  sizeof(ConditionVariableMinimallyPadded),
														  lowNBuffers, highNBuffers, success));
	if (!*success)
		return total;

	total = add_size(total, BufferPoolArrayPhysicalExpand(CkptBufferIds, sizeof(CkptSortItem),
														  lowNBuffers, highNBuffers, success));
	return total;
}

void
BufferManagerShmemInitBuffers(int lowNBuffers, int highNBuffers)
{
	int			i;

	/* Clock sweep will pick up the new buffers; nothing else to do. */
	for (i = lowNBuffers; i < highNBuffers; i++)
		InitializeBuffer(i);
}

/*
 * Release the buffer-pool slice [lowNBuffers, highNBuffers) back to the OS
 * when shrinking the pool.
 *
 * Returns the sum, over the four buffer-pool arrays, of the exact byte length
 * each BufferPoolArrayPhysicalShrink call touched. *success is set to true on
 * success, or false if any madvise() call failed; on failure we stop after the
 * failing array (so total reflects only the arrays that were fully released).
 */
Size
BufferManagerShmemShrink(int lowNBuffers, int highNBuffers, bool *success)
{
	int			max_nbuffers = GetMaxNBuffers();
	Size		total = 0;

	if (highNBuffers > max_nbuffers)
		elog(PANIC, "buffer pool shrink exceeds allocation (low=%d high=%d allocated=%d)",
			 lowNBuffers, highNBuffers, max_nbuffers);

	Assert(lowNBuffers < highNBuffers);

	*success = true;

	total = add_size(total, BufferPoolArrayPhysicalShrink(BufferDescriptors, sizeof(BufferDescPadded),
														  lowNBuffers, highNBuffers, success));
	if (!*success)
		return total;

	total = add_size(total, BufferPoolArrayPhysicalShrink(BufferBlocks, (Size) BLCKSZ,
														  lowNBuffers, highNBuffers, success));
	if (!*success)
		return total;

	total = add_size(total, BufferPoolArrayPhysicalShrink(BufferIOCVArray,
														  sizeof(ConditionVariableMinimallyPadded),
														  lowNBuffers, highNBuffers, success));
	if (!*success)
		return total;

	total = add_size(total, BufferPoolArrayPhysicalShrink(CkptBufferIds, sizeof(CkptSortItem),
														  lowNBuffers, highNBuffers, success));

	return total;
}
