/*-------------------------------------------------------------------------
 *
 * wal_allocator.c
 *
 * The WAL allocator is new as of Postgres 15.  It attempts to keep regular
 * backends from having to allocate new WAL segments.  Even when
 * wal_recycle is enabled, pre-allocating WAL segments can yield
 * significant performance improvements in certain scenarios.  Note that
 * regular backends are still empowered to create new WAL segments if the
 * WAL allocator fails to generate enough new segments.
 *
 * The WAL allocator is controlled by one parameter: wal_allocator_max_size.
 * wal_allocator_max_size specifies the maximum amount of WAL to pre-
 * allocate.  If this value is not divisible by the WAL segment size, fewer
 * WAL segments will be pre-allocated.
 *
 * The WAL allocator is started by the postmaster as soon as the startup
 * subprocess finishes, or as soon as recovery begins if we are doing
 * archive recovery.  It remains alive until the postmaster commands it to
 * terminate.  Normal termination is by SIGTERM, which instructs the
 * WAL allocator to exit(0).  Emergency termination is by SIGQUIT; like any
 * backend, the WAL allocator will simply abort and exit on SIGQUIT.
 *
 * If the WAL allocator exits unexpectedly, the postmaster treats that the
 * same as a backend crash: shared memory may be corrupted, so remaining
 * backends should be killed by SIGQUIT and then a recovery cycle started.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/wal_allocator.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/time.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "libpq/pqsignal.h"
#include "pgstat.h"
#include "postmaster/interrupt.h"
#include "postmaster/wal_allocator.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#define WAL_ALLOC_TIMEOUT_S (60)

/*
 * GUC parameters
 */
int			wal_alloc_max_size_mb = 64;

static void DoWalPreAllocation(void);
static void ScanForExistingPreallocatedSegments(void);

/*
 * Main entry point for WAL allocator process
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
void
WalAllocatorMain(void)
{
	sigjmp_buf	local_sigjmp_buf;
	MemoryContext wal_alloc_context;

	/*
	 * Properly accept or ignore signals that might be sent to us.
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	/* SIGQUIT handler was already set up by InitPostmasterChild */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, SIG_DFL);

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.
	 */
	wal_alloc_context = AllocSetContextCreate(TopMemoryContext,
											  "WAL Allocator",
											  ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(wal_alloc_context);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * You might wonder why this isn't coded as an infinite loop around a
	 * PG_TRY construct.  The reason is that this is the bottom of the
	 * exception stack, and so with PG_TRY there would be no exception handler
	 * in force at all during the CATCH part.  By leaving the outermost setjmp
	 * always active, we have at least some chance of recovering from an error
	 * during error recovery.  (If we get into an infinite loop thereby, it
	 * will soon be stopped by overflow of elog.c's internal state stack.)
	 *
	 * Note that we use sigsetjmp(..., 1), so that the prevailing signal mask
	 * (to wit, BlockSig) will be restored when longjmp'ing to here.  Thus,
	 * signals other than SIGQUIT will be blocked until we complete error
	 * recovery.  It might seem that this policy makes the HOLD_INTERRUPTS()
	 * call redundant, but it is not since InterruptPending might be set
	 * already.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * These operations are really just a minimal subset of
		 * AbortTransaction().  We don't have very many resources to worry
		 * about.
		 */
		LWLockReleaseAll();
		ConditionVariableCancelSleep();
		pgstat_report_wait_end();
		AbortBufferIO();
		UnlockBuffers();
		ReleaseAuxProcessResources(false);
		AtEOXact_Buffers(false);
		AtEOXact_SMgr();
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(wal_alloc_context);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(wal_alloc_context);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/*
		 * Sleep at least 1 second after any error.  A write error is likely
		 * to be repeated, and we don't want to be filling the error logs as
		 * fast as we can.
		 */
		pg_usleep(1000000L);

		/*
		 * Close all open files after any error.  This is helpful on Windows,
		 * where holding deleted files open causes various strange errors.
		 * It's not clear we need it elsewhere, but shouldn't hurt.
		 */
		smgrcloseall();

		/* Report wait end here, when there is no further possibility of wait */
		pgstat_report_wait_end();
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	/*
	 * Advertise our latch that backends can use to wake us up while we're
	 * sleeping.
	 */
	ProcGlobal->walAllocatorLatch = &MyProc->procLatch;

	/*
	 * Before we go into the main loop, scan the pre-allocated segments
	 * directory and look for anything that was left over from the last time.
	 */
	ScanForExistingPreallocatedSegments();

	/*
	 * Loop forever
	 */
	for (;;)
	{
		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		HandleMainLoopInterrupts();

		DoWalPreAllocation();

		/* XXX: Send activity statistics to the stats collector */

		/* Sleep until we are signaled or WAL_ALLOC_TIMEOUT_S has elapsed. */
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 WAL_ALLOC_TIMEOUT_S * 1000L /* convert to ms */,
						 WAIT_EVENT_WAL_ALLOCATOR_MAIN);
	}

	pg_unreachable();
}

/*
 * DoWalPreAllocation
 *
 * Tries to allocate up to wal_allocator_max_size worth of WAL.
 */
static void
DoWalPreAllocation(void)
{
	int		max_prealloc_segs;

	max_prealloc_segs = wal_alloc_max_size_mb / (wal_segment_size / (1024 * 1024));

	while (!ShutdownRequestPending &&
		   GetNumPreallocatedWalSegs() < max_prealloc_segs)
	{
		char tmppath[MAXPGPATH];

		snprintf(tmppath, MAXPGPATH, "%s/preallocated_segments/xlogtemp", XLOGDIR);
		CreateEmptyWalSegment(tmppath);
		InstallPreallocatedWalSeg(tmppath);
		elog(DEBUG2, "pre-allocated WAL segment");

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);

			max_prealloc_segs = wal_alloc_max_size_mb / (wal_segment_size / (1024 * 1024));
		}
	}
}

/*
 * ScanForExistingPreallocatedSegments
 *
 * This function searches through pg_wal/preallocated_segments for any segments
 * that were left over from a previous WAL allocator process and sets the
 * tracking variable in shared memory accordingly.
 */
static void
ScanForExistingPreallocatedSegments(void)
{
	int i = 0;

	/*
	 * fsync the preallocated_segments directory in case any renames have yet to
	 * be flushed to disk.  This is probably not really necessary, but it seems
	 * nice to know that all the segments we find are really where we think they
	 * are.
	 */
	fsync_fname_ext(XLOGDIR "/preallocated_segments", true, false, FATAL);

	/*
	 * Gather all the preallocated segments we can find.
	 */
	while (true)
	{
		FILE *fd;
		char path[MAXPGPATH];

		snprintf(path, MAXPGPATH, "%s/preallocated_segments/xlogtemp.%d",
				 XLOGDIR, i);

		fd = AllocateFile(path, "r");
		if (fd != NULL)
		{
			FreeFile(fd);
			i++;
		}
		else
		{
			if (errno != ENOENT)
				ereport(FATAL,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\": %m", path)));
			break;
		}
	}

	SetNumPreallocatedWalSegs(i);
	elog(DEBUG2, "found %d preallocated segments during startup", i);
}

bool
WalPreallocationEnabled(void)
{
	return wal_alloc_max_size_mb >= wal_segment_size / (1024 * 1024);
}
