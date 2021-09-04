/*-------------------------------------------------------------------------
 *
 * aio_posix.c
 *	  Routines for POSIX AIO.
 *
 * The POSIX AIO API is provided by the kernels of at least FreeBSD, NetBSD,
 * macOS, AIX and HP-UX.  It's also provided by user space-managed threads in
 * the runtime libraries of Linux (Glibc, Musl), illumos and Solaris.
 *
 * The main complication for PostgreSQL's current process-based architecture
 * is that it's not possible for one backend to consume the completion of an
 * IO submitted by another.  To avoid deadlocks, any backend that is waiting
 * for an abitrary IO must be able to make progress, so this module has to
 * overcome that limitation.  It does that by sending a signal to the
 * submitting backend to ask it to collect the result and pass it on.
 *
 * XXX Set requester in flags while running synchronous IO to prevent signaling
 * XXX Tidy
 * XXX pgbench fails on macos with "buffer beyond EOF"
 *
 * For macOS, the default kern.aio* settings are inadequate and must be
 * increased.  For AIX, shared_memory_type must be set to sysv because kernel
 * AIO cannot access mmap'd memory.  For Solaris and Linux, libc emulates POSIX
 * AIO with threads, which may not work well.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/aio/aio_posix.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <aio.h>
#include <fcntl.h>

#include "pgstat.h"
#include "miscadmin.h"
#include "storage/aio_internal.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "utils/memutils.h"

/* AIO_LISTIO_MAX is missing on glibc systems.  16 is pretty conservative. */
#ifndef AIO_LISTIO_MAX
#define AIO_LISTIO_MAX 16
#endif

typedef struct pgaio_posix_aio_listio_buffer
{
	int			nios;
	struct aiocb *cbs[AIO_LISTIO_MAX];
#if defined(LIO_READV) && defined(LIO_WRITEV)
	struct iovec iovecs[AIO_LISTIO_MAX][IOV_MAX];
#endif
}			pgaio_posix_aio_listio_buffer;

/* Variables used by the interprocess interrupt system. */
static int	pgaio_posix_aio_num_iocbs;
static struct aiocb **pgaio_posix_aio_iocbs;
static volatile sig_atomic_t pgaio_posix_aio_interrupt_pending;
static volatile sig_atomic_t pgaio_posix_aio_interrupt_holdoff;

/* Helper function declarations. */
static void pgaio_posix_aio_disable_interrupt(void);
static void pgaio_posix_aio_enable_interrupt(void);

static int	pgaio_posix_aio_take_baton(PgAioInProgress * io, bool have_result);
static bool pgaio_posix_aio_give_baton(PgAioInProgress * io, int result);
static void pgaio_posix_aio_release_baton(PgAioInProgress * io);

static PgAioInProgress * io_for_iocb(struct aiocb *cb);
static struct aiocb *iocb_for_io(struct PgAioInProgress *io);

static void pgaio_posix_aio_activate_io(struct PgAioInProgress *io);
static void pgaio_posix_aio_deactivate_io(struct PgAioInProgress *io);

static void pgaio_posix_aio_submit_one(PgAioInProgress * io,
									   pgaio_posix_aio_listio_buffer * listio_buffer);
static int	pgaio_posix_aio_flush_listio(pgaio_posix_aio_listio_buffer * lb);
static int	pgaio_posix_aio_start_rw(PgAioInProgress * io,
									 pgaio_posix_aio_listio_buffer * lb);
static void pgaio_posix_aio_kernel_io_done(PgAioInProgress * io,
										   int result,
										   bool in_interrupt_handler);
static int	pgaio_posix_aio_drain_internal(bool block, bool in_interrupt_handler);
static void pgaio_posix_aio_process_interrupt(void);

static uint64
			pgaio_posix_aio_make_flags(uint64 control_flags,
									   uint32 submitter_id,
									   uint32 completer_id);

/* Entry points for IoMethodOps. */
static void pgaio_posix_aio_shmem_init(void);
static int pgaio_posix_aio_submit(int max_submit, bool drain);
static void pgaio_posix_aio_wait_one(PgAioContext *context, PgAioInProgress *io, uint64 ref_generation, uint32 wait_event_info);
static void pgaio_posix_aio_io_retry(PgAioInProgress *io);
static int pgaio_posix_aio_drain(PgAioContext *context, bool block, bool call_shared);
static void pgaio_posix_aio_closing_fd(int fd);

/* Bits and masks for determining the completer for an IO. */
#define PGAIO_POSIX_AIO_FLAG_AVAILABLE			0x0100000000000000
#define PGAIO_POSIX_AIO_FLAG_REQUESTED			0x0200000000000000
#define PGAIO_POSIX_AIO_FLAG_GRANTED			0x0400000000000000
#define PGAIO_POSIX_AIO_FLAG_DONE				0x0800000000000000
#define PGAIO_POSIX_AIO_FLAG_RESULT				0x1000000000000000
#define PGAIO_POSIX_AIO_FLAG_SUBMITTER_MASK		0x00ffffff00000000
#define PGAIO_POSIX_AIO_FLAG_COMPLETER_MASK		0x0000000000ffffff
#define PGAIO_POSIX_AIO_FLAG_SUBMITTER_SHIFT	32

/* Result values from pgaio_posix_aio_take_baton(). */
#define PGAIO_POSIX_AIO_BATON_GRANTED			0
#define PGAIO_POSIX_AIO_BATON_DENIED			1
#define PGAIO_POSIX_AIO_BATON_WOULDBLOCK		2

/* On systems with no O_DSYNC, just use the stronger O_SYNC. */
#ifdef O_DSYNC
#define PG_O_DSYNC O_DSYNC
#else
#define PG_O_DSYNC O_SYNC
#endif

const IoMethodOps pgaio_posix_aio_ops = {
	.shmem_init = pgaio_posix_aio_shmem_init,
	.submit = pgaio_posix_aio_submit,
	.retry = pgaio_posix_aio_io_retry,
	.wait_one = pgaio_posix_aio_wait_one,
	.drain = pgaio_posix_aio_drain,
	.closing_fd = pgaio_posix_aio_closing_fd,

	/* FreeBSD has asynchronous scatter/gather as an extension. */
#if defined(LIO_READV) && defined(LIO_WRITEV)
	.can_scatter_gather_direct = true,
	.can_scatter_gather_buffered = true
#endif
};

/* Module initialization. */

/*
 * Initialize shared memory data structures.
 */
static void
pgaio_posix_aio_shmem_init(void)
{
	for (int i = 0; i < max_aio_in_progress; i++)
	{
		PgAioInProgress *io = &aio_ctl->in_progress_io[i];

		pg_atomic_init_u64(&io->io_method_data.posix_aio.flags, 0);
	}

	/*
	 * We need this array in every backend, including single process.
	 * XXX It's not "shmem", so where should it go?
	 */
	if (!pgaio_posix_aio_iocbs)
		pgaio_posix_aio_iocbs =
			MemoryContextAlloc(TopMemoryContext,
							   sizeof(struct aiocb *) * max_aio_in_flight);
}


/* Functions for submitting IOs to the kernel. */

/*
 * Submit a given number of pending IOs to the kernel, and optionally drain
 * any results that have arrived, without waiting.
 */
static int
pgaio_posix_aio_submit(int max_submit, bool drain)
{
	PgAioInProgress *ios[PGAIO_SUBMIT_BATCH_SIZE];
	int			nios = 0;
	pgaio_posix_aio_listio_buffer listio_buffer = {0};

	/*
	 * We can't allow the interrupt handler to run while we modify data
	 * structures and interact with the AIO APIs, because we need a consistent
	 * view of the set of running aiocbs.
	 */
	START_CRIT_SECTION();
	pgaio_posix_aio_disable_interrupt();

	while (!dlist_is_empty(&my_aio->pending))
	{
		dlist_node *node;
		PgAioInProgress *io;

		if (nios == max_submit)
			break;

		node = dlist_pop_head_node(&my_aio->pending);
		io = dlist_container(PgAioInProgress, io_node, node);

		pgaio_io_prepare_submit(io, 0);

		my_aio->submissions_total_count++;

		pgaio_posix_aio_submit_one(io, &listio_buffer);

		ios[nios] = io;
		++nios;
	}
	pgaio_posix_aio_flush_listio(&listio_buffer);

	pgaio_posix_aio_enable_interrupt();
	END_CRIT_SECTION();

	/* XXXX copied from uring submit */

	/*
	 * Others might have been waiting for this IO. Because it wasn't marked as
	 * in-flight until now, they might be waiting for the CV. Wake'em up.
	 */
	pgaio_broadcast_ios(ios, nios);

	/* callbacks will be called later by pgaio_submit() */
	if (drain)
		pgaio_drain(NULL,
					 /* block = */ false,
					 /* call_shared = */ false,
					 /* call_local = */ false);

	return nios;
}

/*
 * Resubmit an IO that was only partially completed (for example, a short
 * read) or that the kernel told us to retry.
 */
static void
pgaio_posix_aio_io_retry(PgAioInProgress * io)
{
	pgaio_posix_aio_listio_buffer listio_buffer = {0};

	WRITE_ONCE_F(io->flags) |= PGAIOIP_INFLIGHT;

	if (io->result == -EAGAIN)
	{
		/*
		 * The kernel said it's out of resources.  Wait for at least one IO
		 * submitted by this backend to complete before we continue (if there
		 * is one), because otherwise we'd have a busy retry loop.  This
		 * probably means the kernel needs some tuning.
		 *
		 * XXX If the problem is caused by other backends, we don't try to
		 * wait for them to drain.  Hopefully they will do that.
		 */
		pgaio_posix_aio_drain(NULL, true, true);
	}

	/* See comments in pgaio_posix_aio_submit(). */
	START_CRIT_SECTION();
	pgaio_posix_aio_disable_interrupt();

	pgaio_posix_aio_submit_one(io, &listio_buffer);
	pgaio_posix_aio_flush_listio(&listio_buffer);

	pgaio_posix_aio_enable_interrupt();
	END_CRIT_SECTION();

	pgaio_complete_ios(false);

	ConditionVariableBroadcast(&io->cv);
}

/*
 * Queue up an IO request for the kernel in listio_buffer where possible, and
 * otherwise tell the kernel to initiate the IO immediately.
 */
static void
pgaio_posix_aio_submit_one(PgAioInProgress * io,
						   pgaio_posix_aio_listio_buffer * listio_buffer)
{
	struct aiocb *iocb = iocb_for_io(io);
	int			rc = -1;

	pg_atomic_add_fetch_u32(&my_aio->inflight_count, 1);

	/* No result yet. */
	io->io_method_data.posix_aio.raw_result = PGAIO_POSIX_RESULT_INVALID;

	/*
	 * Other backends are free to request the right to complete this IO if
	 * they are blocked on it.  Publish our submitter ID, so that we'll
	 * recognize it as our own, and other processes will be able to signal us
	 * if necessary.
	 */
	pg_atomic_write_u64(&io->io_method_data.posix_aio.flags,
						pgaio_posix_aio_make_flags(PGAIO_POSIX_AIO_FLAG_AVAILABLE,
												   my_aio_id,
												   0));

	/*
	 * Every IO needs the index of the head IO in a merge chain, so that we
	 * can find the iocb that the kernel knows about.
	 */
	for (PgAioInProgress * cur = io;;)
	{
		cur->io_method_data.posix_aio.merge_head_idx = pgaio_io_id(io);
		if (cur->merge_with_idx == PGAIO_MERGE_INVALID)
			break;
		cur = &aio_ctl->in_progress_io[cur->merge_with_idx];
	}

	/* This IOCB is not "active" yet. */
	io->io_method_data.posix_aio.iocb_index = -1;

	/* Populate the POSIX AIO iocb. */
	memset(iocb, 0, sizeof(*iocb));
	iocb->aio_sigevent.sigev_notify = SIGEV_NONE;

	switch (io->op)
	{
		case PGAIO_OP_READ:
			iocb->aio_fildes = io->op_data.read.fd;
			iocb->aio_offset = io->op_data.read.offset +
				io->op_data.read.already_done;
			rc = pgaio_posix_aio_start_rw(io, listio_buffer);
			break;
		case PGAIO_OP_WRITE:
			iocb->aio_fildes = io->op_data.write.fd;
			iocb->aio_offset = io->op_data.write.offset +
				io->op_data.write.already_done;
			rc = pgaio_posix_aio_start_rw(io, listio_buffer);
			break;
		case PGAIO_OP_FSYNC:
			iocb->aio_fildes = io->op_data.fsync.fd;
			pgaio_posix_aio_activate_io(io);
			rc = aio_fsync(io->op_data.fsync.datasync ? PG_O_DSYNC : O_SYNC,
						   iocb);
			break;
		case PGAIO_OP_FLUSH_RANGE:

			/*
			 * This is supposed to represent Linux's sync_file_range(), which
			 * initiates writeback for only a certain range of a file.
			 * Initiating fdatasync() seems close to the intended behavior.
			 * XXX this is a bad idea.  Should we just do nothing instead?
			 */
			iocb->aio_fildes = io->op_data.flush_range.fd;
			pgaio_posix_aio_activate_io(io);
			rc = aio_fsync(PG_O_DSYNC, iocb);
			break;
		case PGAIO_OP_NOP:
			rc = 0;
			break;
		case PGAIO_OP_INVALID:
			rc = -1;
			errno = EOPNOTSUPP;
			break;
	}

	/* If we failed to submit, then try to reap immediately. */
	if (rc < 0)
	{
		pgaio_posix_aio_kernel_io_done(io, -errno, false);
		pgaio_complete_ios(false);
	}
}

/*
 * Submit all IOs that have accumlated in "lb" to the kernel in a single
 * lio_listio() call.
 */
static int
pgaio_posix_aio_flush_listio(pgaio_posix_aio_listio_buffer * lb)
{
	int			rc;

	Assert(pgaio_posix_aio_interrupt_holdoff > 0);

	if (lb->nios == 0)
		return 0;

	/* Activate all of the individual IOs. */
	for (int i = 0; i < lb->nios; ++i)
		pgaio_posix_aio_activate_io(io_for_iocb(lb->cbs[i]));

	/* Try to initiate all of these IOs with one system call. */
	rc = lio_listio(LIO_NOWAIT, lb->cbs, lb->nios, NULL);
	if (rc < 0)
	{
		if (errno == EAGAIN || errno == EINTR || errno == EIO)
		{
			int			listio_errno = errno;

			/*
			 * POSIX says that for these three errors only, some of the IOs
			 * may have been queued.  We have to figure out which ones.
			 */
			for (int i = 0; i < lb->nios; ++i)
			{
				PgAioInProgress *io;
				int			error_status;

				io = io_for_iocb(lb->cbs[i]);
				error_status = aio_error(iocb_for_io(io));
				if (error_status == EINPROGRESS || error_status == 0)
					continue;	/* submitted or already finished */

				/*
				 * If we failed to submit, we may see -1 here and the reason
				 * in errno.
				 */
				if (error_status < 0)
					error_status = errno;
				if (error_status == EINVAL)
				{
					/*
					 * We failed to initiate this IO, so the kernel doesn't
					 * even recognize the iocb.  We have to report the EAGAIN
					 * etc that we received from lio_listio() rather than
					 * EINVAL so that pgaio_process_io_completion treats it as
					 * a soft failure.
					 */
					error_status = listio_errno;
				}
				pgaio_posix_aio_kernel_io_done(io, -error_status, false);
			}
			pgaio_complete_ios(false);
		}
		else
		{
			int			error_status = errno;

			/*
			 * The only other error documented by POSIX is EINVAL.  Whether we
			 * get that or something undocumented, replicate the error we got
			 * from lio_listio() into all the IOs.
			 */
			for (int i = 0; i < lb->nios; ++i)
			{
				PgAioInProgress *io;

				io = io_for_iocb(lb->cbs[i]);
				pgaio_posix_aio_kernel_io_done(io, -error_status, false);
			}
			pgaio_complete_ios(false);
		}
	}
	lb->nios = 0;

	return 0;
}

/*
 * Add one IO to the buffer "lb", flushing if it's already full.
 */
static int
pgaio_posix_aio_add_listio(pgaio_posix_aio_listio_buffer * lb, PgAioInProgress * io)
{
	struct aiocb *iocb;
	int index;

	if (lb->nios == AIO_LISTIO_MAX)
	{
		int			rc;

		rc = pgaio_posix_aio_flush_listio(lb);
		if (rc < 0)
			return rc;
	}

	index = lb->nios;
	iocb = iocb_for_io(io);
	lb->cbs[index] = iocb;
#if defined(LIO_READV) && defined(LIO_WRITEV)
	if (iocb->aio_lio_opcode == LIO_READV || iocb->aio_lio_opcode == LIO_WRITEV) {
		/* Copy the iovecs into a new buffer with the right lifetime. */
		if (iocb->aio_iovcnt > IOV_MAX)
			elog(ERROR, "too many iovecs");
		memcpy(&lb->iovecs[index][0],
			   unvolatize(void *, iocb->aio_iov),
			   sizeof(struct iovec) * iocb->aio_iovcnt);
		iocb->aio_iov = &lb->iovecs[index][0];
	}
#endif
	++lb->nios;

	return 0;
}

/*
 * Start a read or write, or add it to "lb" to be started along with others,
 * depending on available system calls on this system.
 *
 * Assumes that aio_filedes and aio_offset are already set.
 */
static int
pgaio_posix_aio_start_rw(PgAioInProgress * io,
						 pgaio_posix_aio_listio_buffer * lb)
{
	struct aiocb *cb = iocb_for_io(io);
	struct iovec iov[IOV_MAX];
	int			iovcnt PG_USED_FOR_ASSERTS_ONLY;

	iovcnt = pgaio_fill_iov(iov, io);

	if (iovcnt > 1)
	{
#if defined(LIO_READV) && defined(LIO_WRITEV)
		/* FreeBSD supports async readv/writev in an lio batch. */
		cb->aio_iov = iov;	/* this will be copied */
		cb->aio_iovcnt = iovcnt;
		cb->aio_lio_opcode = io->op == PGAIO_OP_WRITE ? LIO_WRITEV : LIO_READV;
		return pgaio_posix_aio_add_listio(lb, io);
#endif

		/* pgaio_can_scatter_gather() should not have allowed this. */
		elog(ERROR, "unexpected vector read/write");
		pg_unreachable();
	}
	else
	{
		/*
		 * Standard POSIX AIO doesn't have scatter/gather.  This IO might still
		 * have been merged from multiple IOs that access adjacent regions of a
		 * file, but only if the memory is also adjacent.
		 */
		Assert(iovcnt == 1);
		cb->aio_buf = iov[0].iov_base;
		cb->aio_nbytes = iov[0].iov_len;
		cb->aio_lio_opcode = io->op == PGAIO_OP_WRITE ? LIO_WRITE : LIO_READ;
		return pgaio_posix_aio_add_listio(lb, io);
	}
}


/* Functions for waiting for IOs to complete. */

/*
 * Wait for a given IO/generation to complete.
 */
static void
pgaio_posix_aio_wait_one(PgAioContext *context,
						 PgAioInProgress * io,
						 uint64 ref_generation,
						 uint32 wait_event_info)
{
	PgAioInProgress *last_merge_head_io = NULL;
	PgAioInProgress *merge_head_io;
	PgAioIPFlags flags;
	bool		release_baton = false;

	for (;;)
	{
		uint32		merge_head_idx;

		if (pgaio_io_recycled(io, ref_generation, &flags) ||
			!(flags & PGAIOIP_INFLIGHT))
			break;

		/*
		 * Find the IO that is the head of the merge chain.  This information
		 * may be arbitrarily out of date, but we'll cope with that.
		 */
		merge_head_idx = io->io_method_data.posix_aio.merge_head_idx;
		merge_head_io = &aio_ctl->in_progress_io[merge_head_idx];

		/*
		 * If we retry and finish up looking at a new merge head IO, release
		 * the baton on the last one.  We should only hold the baton on an IO
		 * that we will detinitely complete.
		 */
		if (last_merge_head_io != NULL &&
			last_merge_head_io != merge_head_io &&
			release_baton)
		{
			pgaio_posix_aio_release_baton(last_merge_head_io);
			release_baton = false;
		}
		last_merge_head_io = merge_head_io;

		switch (pgaio_posix_aio_take_baton(merge_head_io, false))
		{
			case PGAIO_POSIX_AIO_BATON_GRANTED:

				/*
				 * We're now the completer for the head of the merged IO
				 * chain. It's possibly a later generation than the one we're
				 * actually waiting for, but the result is available now so
				 * let's process it anyway and check the generation again.
				 */
				pgaio_process_io_completion(merge_head_io,
											merge_head_io->io_method_data.posix_aio.raw_result);
				break;
			case PGAIO_POSIX_AIO_BATON_WOULDBLOCK:

				/*
				 * We're the submitter.  It's possibly a later generation than
				 * the one we're waiting for, but in that case we'll exit the
				 * next loop.
				 */
				release_baton = true;
				pgaio_posix_aio_drain(NULL, true, false);
				break;
			case PGAIO_POSIX_AIO_BATON_DENIED:

				/*
				 * Someone else is already signed up to reap the merged IO
				 * chain, or this is a later generation and we'll detect that
				 * in the next loop.  Wait on the IO.
				 */
				ConditionVariablePrepareToSleep(&io->cv);
				if (pgaio_io_recycled(io, ref_generation, &flags) ||
					!(flags & PGAIOIP_INFLIGHT))
					break;
				ConditionVariableSleep(&io->cv,
									   WAIT_EVENT_AIO_IO_COMPLETE_ONE);
				break;
			default:
				elog(ERROR, "unexpected value");
		}
	}

	/* Complete anything on our "reaped" list. */
	pgaio_complete_ios(false);

	/*
	 * If we became the requester but then exited because it turned out the
	 * generation had moved, we'd better undo that.
	 */
	if (release_baton)
		pgaio_posix_aio_release_baton(merge_head_io);

	ConditionVariableCancelSleep();
}

/*
 * Drain completion events from the kernel, reaping them if possible.  If
 * block is true, wait for at least one to complete, unless there are none in
 * flight.
 */
static int
pgaio_posix_aio_drain(PgAioContext *context, bool block, bool call_shared)
{
	int			ndrained;

	START_CRIT_SECTION();
	pgaio_posix_aio_disable_interrupt();
	ndrained = pgaio_posix_aio_drain_internal(block, false);
	pgaio_posix_aio_enable_interrupt();

	if (call_shared)
		pgaio_complete_ios(false);
	END_CRIT_SECTION();

	return ndrained;
}

/*
 * If in_interrupt_handler is true, results from the kernel are written into
 * shared memory for processing later or by another backend.
 */
static int
pgaio_posix_aio_drain_internal(bool block, bool in_interrupt_handler)
{
	struct timespec timeout = {0, 0};
	int			ndrained = 0;

	for (;;)
	{
		struct aiocb *iocb;
		ssize_t		rc;

		if (pgaio_posix_aio_num_iocbs == 0)
			break;

#ifdef HAVE_AIO_WAITCOMPLETE
		/* FreeBSD has a convenient way to consume completions like a queue. */
		rc = aio_waitcomplete(&iocb,
							  ndrained == 0 && block ? NULL : &timeout);
		if (rc < 0 && iocb == NULL)
		{
			if (errno == EAGAIN)
				break;			/* no IOs in progress? */
			else if (errno == EINPROGRESS)
				break;			/* all IOs in progress, !block */
			else if (errno != EINTR)
				elog(ERROR, "could not wait for IO completion: %m");
			continue;			/* signaled; retry */
		}
		if (rc < 0)
			rc = -errno;
		pgaio_posix_aio_kernel_io_done(io_for_iocb(iocb), rc,
									   in_interrupt_handler);
		ndrained++;
#else
		/* Standard POSIX requires us to do a bit more work. */
		rc = aio_suspend((const struct aiocb *const *) pgaio_posix_aio_iocbs,
						 pgaio_posix_aio_num_iocbs,
						 ndrained == 0 && block ? NULL : &timeout);
		if (rc < 0)
		{
			if (errno == EAGAIN)
				break;			/* all IOs in progress */
			if (errno != EINTR)
				elog(ERROR, "could not wait for IO completion: %m");
			continue;			/* signaled; retry */
		}
		/* Something has completed, but what? */
		for (int i = 0; i < pgaio_posix_aio_num_iocbs;)
		{
			int			error_status;

			/* Still running? */
			iocb = pgaio_posix_aio_iocbs[i];
			error_status = aio_error(iocb);
			if (error_status < 0)
				error_status = errno;
			if (error_status == EINPROGRESS)
			{
				++i;
				continue;
			}
			/* Consume the result. */
			rc = aio_return(iocb);
			if (rc < 0)
				rc = -errno;
			pgaio_posix_aio_kernel_io_done(io_for_iocb(iocb), rc,
										   in_interrupt_handler);
			ndrained++;

			/* Don't advance i; another item was moved into element i. */
		}
#endif
	}

	return ndrained;
}










/*
 * Given an aiocb, return the associated PgAioInProgress.
 */
static PgAioInProgress *
io_for_iocb(struct aiocb *cb)
{
	return (PgAioInProgress *)
		(((char *) cb) - offsetof(PgAioInProgress,
								  io_method_data.posix_aio.iocb));
}

static struct aiocb *
iocb_for_io(PgAioInProgress * io)
{
	return &io->io_method_data.posix_aio.iocb;
}

/*
 * Record that an iocb is running in the kernel.
 */
static void
pgaio_posix_aio_activate_io(PgAioInProgress * io)
{
	Assert(pgaio_posix_aio_interrupt_holdoff > 0);

	if (pgaio_posix_aio_num_iocbs == max_aio_in_flight)
		elog(PANIC, "too many IOs in flight");

	io->io_method_data.posix_aio.iocb_index = pgaio_posix_aio_num_iocbs++;
	pgaio_posix_aio_iocbs[io->io_method_data.posix_aio.iocb_index] =
		iocb_for_io(io);
}

/*
 * Forget about an iocb that is no longer known to the kernel.
 */
static void
pgaio_posix_aio_deactivate_io(PgAioInProgress * io)
{
	int			highest_index = pgaio_posix_aio_num_iocbs - 1;
	int			gap_index = io->io_method_data.posix_aio.iocb_index;
	PgAioInProgress *migrant;

	Assert(pgaio_posix_aio_interrupt_holdoff > 0);

	/* Nothing to do if not activated. */
	if (gap_index == -1)
		return;

	if (gap_index != highest_index)
	{
		/* Migrate the highest entry into the new empty slot, to avoid gaps. */
		migrant = io_for_iocb(pgaio_posix_aio_iocbs[highest_index]);
		migrant->io_method_data.posix_aio.iocb_index = gap_index;
		pgaio_posix_aio_iocbs[gap_index] = iocb_for_io(migrant);
	}
	Assert(pgaio_posix_aio_num_iocbs > 0);
	--pgaio_posix_aio_num_iocbs;
}

/*
 * The kernel has provided the result for an IO that we submitted.  Complete
 * it if we're not in an interrupt handler.  Otherwise, pass the result on to
 * anyone who is blocked waiting for it.
 */
static void
pgaio_posix_aio_kernel_io_done(PgAioInProgress * io,
							   int result,
							   bool in_interrupt_handler)
{
	pg_atomic_fetch_sub_u32(&my_aio->inflight_count, 1);

	/*
	 * Maintain the array of active iocbs.  If this was an IO that was actually
	 * submitted to the kernel, it should have been "activated", but if we
	 * failed to submit or it was a degenerate case like NOP then it's not
	 * "active".
	 */
	pgaio_posix_aio_disable_interrupt();
	pgaio_posix_aio_deactivate_io(io);
	pgaio_posix_aio_enable_interrupt();

	if (likely(!in_interrupt_handler))
	{
		int			rc PG_USED_FOR_ASSERTS_ONLY;

		/*
		 * We can reap the result immediately, without any expensive
		 * interprocess communication.  Hopefully this is the way most IOs are
		 * completed.
		 *
		 * The submitter can always take the baton, when the result comes in.
		 * If someone else is waiting for it, this will wake them up their
		 * request is denied.
		 */
		rc = pgaio_posix_aio_take_baton(io, true);
		Assert(rc == PGAIO_POSIX_AIO_BATON_GRANTED);
		pgaio_process_io_completion(io, result);
	}
	else
	{
		/*
		 * We can't do much in a signal handler.  Store the value for later,
		 * for whoever arrives first to take the baton.  If someone is waiting
		 * already, give them the baton now.
		 */
		pgaio_posix_aio_give_baton(io, result);
	}
}


/* Functions for negotiating who is allowed to complete an IO. */

static uint64
pgaio_posix_aio_make_flags(uint64 control_flags,
						   uint32 submitter_id,
						   uint32 completer_id)
{
	return control_flags |
		(((uint64) submitter_id) << PGAIO_POSIX_AIO_FLAG_SUBMITTER_SHIFT) |
		completer_id;
}

static uint32
pgaio_posix_aio_submitter_from_flags(uint64 flags)
{
	return (flags & PGAIO_POSIX_AIO_FLAG_SUBMITTER_MASK) >>
		PGAIO_POSIX_AIO_FLAG_SUBMITTER_SHIFT;
}

static uint32
pgaio_posix_aio_completer_from_flags(uint64 flags)
{
	return flags & PGAIO_POSIX_AIO_FLAG_COMPLETER_MASK;
}

static bool
pgaio_posix_aio_update_flags(PgAioInProgress * io,
							 uint64 old_flags,
							 uint64 control_flags,
							 uint32 submitter_id,
							 uint32 completer_id)
{
	return pg_atomic_compare_exchange_u64(&io->io_method_data.posix_aio.flags,
										  &old_flags,
										  pgaio_posix_aio_make_flags(control_flags,
																	 submitter_id,
																	 completer_id));
}

/*
 * Try to get permission to complete this IO.  The possible results are:
 *
 * PGAIO_POSIX_AIO_BATON_GRANTED: the result is available in raw_result, and
 * the caller is now obligated to run pgaio_process_io_completion() on it.
 * Waits if necessary for the raw result to be available.
 *
 * PGAIO_POSIX_AIO_BATON_WOULDBLOCK: the caller is the IO's submitter, the IO
 * is still active, and the caller should drain.  It's been marked as
 * "requested" by this backend, which will prevent other backends from
 * signalling us uselessly while we wait/drain.  It's possible that the IO is
 * a later generation than the caller wanted, so the caller must call
 * pgaio_posix_aio_release_baton().
 *
 * PGAIO_POSIX_AIO_BATON_DENIED: some other backend has or will handle it.
 * The caller should wait using the IO CV after checking the generation.
 */
static int
pgaio_posix_aio_take_baton(PgAioInProgress * io, bool have_result)
{
	uint32		submitter_id;
	uint32		completer_id;
	uint64		flags;

	for (;;)
	{
		flags = pg_atomic_read_u64(&io->io_method_data.posix_aio.flags);
		submitter_id = pgaio_posix_aio_submitter_from_flags(flags);
		completer_id = pgaio_posix_aio_completer_from_flags(flags);

		if (flags & PGAIO_POSIX_AIO_FLAG_AVAILABLE)
		{
			if ((flags & PGAIO_POSIX_AIO_FLAG_RESULT) || have_result)
			{
				/*
				 * The raw result from the kernel is already, available, or
				 * the caller (submitter) has it.  Grant the baton
				 * immediately.
				 */
				if (!pgaio_posix_aio_update_flags(io,
												  flags,
												  PGAIO_POSIX_AIO_FLAG_DONE,
												  submitter_id,
												  my_aio_id))
					continue;	/* lost race, try again */
				return PGAIO_POSIX_AIO_BATON_GRANTED;
			}
			else
			{
				/* Request the right to complete. */
				if (!pgaio_posix_aio_update_flags(io,
												  flags,
												  PGAIO_POSIX_AIO_FLAG_REQUESTED,
												  submitter_id,
												  my_aio_id))
					continue;	/* lost race, try again */

				/*
				 * If we're the submitter, we'll need to drain to make
				 * progress.  Setting the flag to "requested" will prevent
				 * anyone else from harrassing us with procsignals.
				 */
				if (submitter_id == my_aio_id)
					return PGAIO_POSIX_AIO_BATON_WOULDBLOCK;

				/*
				 * Interrupt the submitter to tell it we are waiting.  It will
				 * see that the baton is requested.
				 *
				 * XXX Looking up the backendId would make this more
				 * efficient, but it seems to be borked for the startup
				 * process, which advertises a bogus backendId in its PGPROC.
				 * *FIXME*
				 */
				SendProcSignal(ProcGlobal->allProcs[submitter_id].pid,
							   PROCSIG_POSIX_AIO,
							   InvalidBackendId);

				/* Go around again. */
			}
		}
		else if (flags & PGAIO_POSIX_AIO_FLAG_REQUESTED)
		{
			if (have_result)
			{
				/*
				 * Someone, maybe the submitter and maybe another backend, has
				 * requested the baton.  Now the submitter has the result from
				 * the kernel, so we give it priority and steal the baton: it
				 * is on CPU and can also avoid some later ping-pong.
				 */
				if (!pgaio_posix_aio_update_flags(io,
												  flags,
												  PGAIO_POSIX_AIO_FLAG_DONE,
												  submitter_id,
												  my_aio_id))
					continue;	/* lost race, try again */

				Assert(submitter_id == my_aio_id);

				/* Wake previous completer, if different.  Request denied. */
				if (completer_id != my_aio_id)
					SetLatch(&ProcGlobal->allProcs[completer_id].procLatch);

				return PGAIO_POSIX_AIO_BATON_GRANTED;
			}
			else if (submitter_id == my_aio_id)
			{
				/*
				 * The submitter doesn't have the result yet, but is waiting
				 * for this IO.  It would probably save some ping-pong if the
				 * submitter completes it, so replace any other requester.
				 */
				if (completer_id != my_aio_id)
				{
					if (!pgaio_posix_aio_update_flags(io,
													  flags,
													  PGAIO_POSIX_AIO_FLAG_REQUESTED,
													  submitter_id,
													  my_aio_id))
						continue;	/* lost race, try again */

					/* Wake previous requester.  Request denied. */
					SetLatch(&ProcGlobal->allProcs[completer_id].procLatch);
				}

				/*
				 * The submitter can't wait in here, the submitter is the only
				 * one that can get the answer from the kernel.
				 */
				return PGAIO_POSIX_AIO_BATON_WOULDBLOCK;
			}
			else if (completer_id == my_aio_id)
			{
				/*
				 * We're waiting for the submitter to give us the baton, or
				 * deny it, and set our latch.  We'll keep sleeping until we
				 * see a new state.
				 */
				WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1, 0);
				ResetLatch(MyLatch);

				/* Go around again. */
			}
			else
			{
				/*
				 * Someone else has requested the baton and is waiting.  No
				 * point in trying to usurp it, we'd only have to wait too.
				 * We'll have to wait on the IO CV.
				 */
				return PGAIO_POSIX_AIO_BATON_DENIED;
			}
		}
		else if (flags & PGAIO_POSIX_AIO_FLAG_GRANTED)
		{
			/* It was granted to someone.  Was it us? */
			if (completer_id == my_aio_id)
			{
				if (!pgaio_posix_aio_update_flags(io,
												  flags,
												  PGAIO_POSIX_AIO_FLAG_DONE,
												  submitter_id,
												  my_aio_id))
					continue;	/* lost race, try again */
				return PGAIO_POSIX_AIO_BATON_GRANTED;
			}
			return PGAIO_POSIX_AIO_BATON_DENIED;
		}
		else
		{
			/* Initial or done state. */
			return PGAIO_POSIX_AIO_BATON_DENIED;
		}
	}
}

/*
 * Store the raw result.  The submitter calls this when it has the result from
 * the kernel but is not in a position to run completions.  If a backend is
 * waiting to complete this IO, pass the baton to it and wake it up.
 */
static bool
pgaio_posix_aio_give_baton(PgAioInProgress * io, int result)
{
	uint64		flags;
	uint32		submitter_id;
	uint32		completer_id;

	/*
	 * We don't put the vale in io->result; that field's for an individual IO,
	 * whereas this is the result for a merged IO chain, to be expanded by
	 * pgaio_process_io_completion().
	 */
	io->io_method_data.posix_aio.raw_result = result;

	for (;;)
	{
		flags = pg_atomic_read_u64(&io->io_method_data.posix_aio.flags);
		submitter_id = pgaio_posix_aio_submitter_from_flags(flags);
		completer_id = pgaio_posix_aio_completer_from_flags(flags);

		/*
		 * We should only be trying to grant the baton for IOs that we
		 * submitted and have not yet been granted, and we should definitely
		 * see a fresh enough value because we wrote it.
		 */
		Assert(submitter_id == my_aio_id);
		Assert((flags & PGAIO_POSIX_AIO_FLAG_AVAILABLE) ||
			   (flags & PGAIO_POSIX_AIO_FLAG_REQUESTED));

		if (flags & PGAIO_POSIX_AIO_FLAG_AVAILABLE)
		{
			if (!pgaio_posix_aio_update_flags(io,
											  flags,
											  PGAIO_POSIX_AIO_FLAG_AVAILABLE |
											  PGAIO_POSIX_AIO_FLAG_RESULT,
											  submitter_id,
											  completer_id))
				continue;		/* lost race, try again (not expected) */

			/*
			 * There's no one to grant the baton to, but the next backend to
			 * try to take the baton will succeed immediately.
			 */
			break;
		}
		else if (flags & PGAIO_POSIX_AIO_FLAG_REQUESTED)
		{
			if (!pgaio_posix_aio_update_flags(io,
											  flags,
											  PGAIO_POSIX_AIO_FLAG_GRANTED,
											  submitter_id,
											  completer_id))
				continue;		/* lost race, try again (not expected) */

			/* Wake the completer. */
			SetLatch(&ProcGlobal->allProcs[completer_id].procLatch);

			return true;
		}
		else
		{
			elog(PANIC, "unreachable");
		}
	}

	return false;
}

/*
 * While waiting, there's a small chance we might take the baton for an IO
 * whose generation has rolled over (ie it's been recycled).  Give it away.
 */
void
pgaio_posix_aio_release_baton(PgAioInProgress * io)
{
	uint32		submitter_id;
	uint32		completer_id;
	uint64		flags;

	for (;;)
	{
		flags = pg_atomic_read_u64(&io->io_method_data.posix_aio.flags);
		submitter_id = pgaio_posix_aio_submitter_from_flags(flags);
		completer_id = pgaio_posix_aio_completer_from_flags(flags);

		if ((flags & PGAIO_POSIX_AIO_FLAG_REQUESTED) &&
			(completer_id == my_aio_id))
		{
			/*
			 * Set it back to available and wake up anyone who might be
			 * waiting.  Let them wait for and process completions while we go
			 * and do something else.
			 */
			Assert(completer_id == my_aio_id);
			Assert(!(flags & PGAIO_POSIX_AIO_FLAG_RESULT));
			if (!pgaio_posix_aio_update_flags(io,
											  flags,
											  PGAIO_POSIX_AIO_FLAG_AVAILABLE,
											  submitter_id,
											  0))
				continue;
			ConditionVariableBroadcast(&io->cv);
		}
		else if ((flags & PGAIO_POSIX_AIO_FLAG_GRANTED) &&
				 (completer_id == my_aio_id))
		{
			/*
			 * Since we have the result, we might as well run completion.  It
			 * would be confusing if the state could go from "granted" back to
			 * "available".
			 *
			 * XXX How is this state reached?
			 */
			Assert(completer_id == my_aio_id);
			if (!pgaio_posix_aio_update_flags(io,
											  flags,
											  PGAIO_POSIX_AIO_FLAG_DONE,
											  submitter_id,
											  completer_id))
				continue;
			pgaio_process_io_completion(io,
										io->io_method_data.posix_aio.raw_result);
		}
		break;
	}
}


/* Functions for dealing with interrupts received from other processes. */

/*
 * Handler for PROCSIG_POSIX_AIO.  Called in signal handler context.
 */
void
HandlePosixAioInterrupt(void)
{
	/*
	 * If the main context is currently manipulating iocbs, just remember that
	 * the signal arrived.
	 */
	if (pgaio_posix_aio_interrupt_holdoff > 0)
	{
		pgaio_posix_aio_interrupt_pending = true;
		return;
	}
	pgaio_posix_aio_process_interrupt();
}

/*
 * Collect results from any IOs that other backends are waiting for.
 *
 * Runs in signal handler context.  Also runs in user context.
 */
static void
pgaio_posix_aio_process_interrupt(void)
{
	bool		found_waiter;

	/*
	 * It's not safe to use spinlocks or lwlocks or anything else that is not
	 * guaranteed to make progress in a signal handler, so we do our baton
	 * negotiation with atomics.  We can't do that on (ancient) platforms
	 * where our atomics are simulated with spinlocks.
	 */
#ifdef PG_HAVE_ATOMIC_U64_SIMULATION
#error "Cannot use simulated atomics in signal handlers."
#endif

	for (;;)
	{
		/*
		 * Search all IOs submitted by this backend to see if there are any
		 * that other backends are blocked on.
		 */
		found_waiter = false;
		for (int i = 0; i < pgaio_posix_aio_num_iocbs; ++i)
		{
			PgAioInProgress *io = io_for_iocb(pgaio_posix_aio_iocbs[i]);

			if (pg_atomic_read_u64(&io->io_method_data.posix_aio.flags) &
				PGAIO_POSIX_AIO_FLAG_REQUESTED)
			{
				found_waiter = true;
				break;
			}
		}

		/* If we didn't find any, we're done. */
		if (!found_waiter)
			break;

		/*
		 * Wait for the kernel to tell us that one or more IO has returned.
		 * It's a shame to have temporarily given up up whatever we were doing
		 * in our user context to wait around for an IO to finish on behalf of
		 * another backend, but that backend can't do it, and we can't block
		 * others' progress or we might deadlock.  This problem and much of
		 * this code would go away if we used threads instead of process.
		 */
		pgaio_posix_aio_drain_internal(true /* block */ ,
									   true /* in_interrupt_handler */ );
	}
}

/*
 * Disable interrupt processing while interacting with aiocbs.  This avoids
 * undefined behaviour in aio_suspend() for IOs that have already returned,
 * and problems with implementations that are not as async signal safe as they
 * should be.
 */
static void
pgaio_posix_aio_disable_interrupt(void)
{
	pgaio_posix_aio_interrupt_holdoff++;
}

/*
 * Renable interrupt processing after interacting with aiocbs, and handle any
 * interrupts we missed.
 */
static void
pgaio_posix_aio_enable_interrupt(void)
{
	Assert(pgaio_posix_aio_interrupt_holdoff > 0);
	if (--pgaio_posix_aio_interrupt_holdoff == 0)
	{
		while (pgaio_posix_aio_interrupt_pending)
		{
			pgaio_posix_aio_interrupt_pending = false;
			pgaio_posix_aio_interrupt_holdoff++;
			pgaio_posix_aio_process_interrupt();
			pgaio_posix_aio_interrupt_holdoff--;
		}
	}
}

/*
 * POSIX leaves it unspecified whether the OS cancels IOs when you close the
 * underlying descriptor.  Some do (macOS), some don't (FreeBSD) and one starts
 * mixing up unrelated fds (glibc).
 */
#if !defined(__freebsd__)
#define PGAIO_POSIX_AIO_DRAIN_FDS_BEFORE_CLOSING
#endif

/*
 * Drain all in progress IOs from a file descriptor, if necessary on this
 * platform.
 */
static void
pgaio_posix_aio_closing_fd(int fd)
{
#if defined(PGAIO_POSIX_AIO_DRAIN_FDS_BEFORE_CLOSING)
	struct aiocb *iocb;
	bool waiting;

	START_CRIT_SECTION();
	pgaio_posix_aio_disable_interrupt();
	for (;;)
	{
		waiting = false;
		for (int i = 0; i < pgaio_posix_aio_num_iocbs; ++i)
		{
			iocb = pgaio_posix_aio_iocbs[i];
			if (iocb->aio_fildes == fd)
			{
				waiting = true;
				break;
			}
		}

		if (!waiting)
			break;

		pgaio_posix_aio_drain_internal(true /* block */,
									   false /* in_interrupt_handler */);
	}
	pgaio_posix_aio_enable_interrupt();
	pgaio_complete_ios(false);
	END_CRIT_SECTION();

#endif
}
