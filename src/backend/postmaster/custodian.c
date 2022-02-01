/*-------------------------------------------------------------------------
 *
 * custodian.c
 *
 * The custodian process is new as of Postgres 15.  It's main purpose is to
 * offload tasks that could otherwise delay startup and checkpointing, but
 * it needn't be restricted to just those things.  Offloaded tasks should
 * not be synchronous (e.g., checkpointing shouldn't need to wait for the
 * custodian to complete a task before proceeding).  Also, ensure that any
 * offloaded tasks are either not required during single-user mode or are
 * performed separately during single-user mode.
 *
 * The custodian is not an essential process and can shutdown quickly when
 * requested.  The custodian will wake up approximately once every 5
 * minutes to perform its tasks, but backends can (and should) set its
 * latch to wake it up sooner.
 *
 * Normal termination is by SIGTERM, which instructs the bgwriter to
 * exit(0).  Emergency termination is by SIGQUIT; like any backend, the
 * custodian will simply abort and exit on SIGQUIT.
 *
 * If the custodian exits unexpectedly, the postmaster treats that the same
 * as a backend crash: shared memory may be corrupted, so remaining
 * backends should be killed by SIGQUIT and then a recovery cycle started.
 *
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *   src/backend/postmaster/custodian.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <time.h>

#include "access/rewriteheap.h"
#include "libpq/pqsignal.h"
#include "pgstat.h"
#include "postmaster/custodian.h"
#include "postmaster/interrupt.h"
#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "utils/memutils.h"

#define CUSTODIAN_TIMEOUT_S (300)		/* 5 minutes */

/*
 * Main entry point for custodian process
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
void
CustodianMain(void)
{
	sigjmp_buf	local_sigjmp_buf;
	MemoryContext custodian_context;

	/*
	 * Properly accept or ignore signals that might be sent to us.
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
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
	custodian_context = AllocSetContextCreate(TopMemoryContext,
											  "Custodian",
											  ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(custodian_context);

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
	 * recovery.  It might seem that this policy makes the HOLD_INTERRUPS()
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
		MemoryContextSwitchTo(custodian_context);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(custodian_context);

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
	 * Advertise out latch that backends can use to wake us up while we're
	 * sleeping.
	 */
	ProcGlobal->custodianLatch = &MyProc->procLatch;

	/*
	 * Loop forever
	 */
	for (;;)
	{
		pg_time_t	start_time;
		pg_time_t	end_time;
		int			elapsed_secs;
		int			cur_timeout;

		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		HandleMainLoopInterrupts();

		start_time = (pg_time_t) time(NULL);

		/*
		 * Remove any pgsql_tmp directories that have been staged for deletion.
		 * Since pgsql_tmp directories can accumulate many files, removing all
		 * of the files during startup (which we used to do) can take a very
		 * long time.  To avoid delaying startup, we simply have startup rename
		 * the temporary directories, and we clean them up here.
		 *
		 * pgsql_tmp directories are not staged or cleaned in single-user mode,
		 * so we don't need any extra handling outside of the custodian process
		 * for this.
		 */
		RemovePgTempFiles(false, false);

		/*
		 * Remove any replication slot directories that have been staged for
		 * deletion.  Since slot directories can accumulate many files, removing
		 * all of the files during startup (which we used to do) can take a very
		 * long time.  To avoid delaying startup, we simply have startup rename
		 * the slot directories, and we clean them up here.
		 *
		 * Replication slot directories are not staged or cleaned in single-user
		 * mode, so we don't need any extra handling outside of the custodian
		 * process for this.
		 */
		RemoveStagedSlotDirectories();

		/*
		 * Remove serialized snapshots that are no longer required by any
		 * logical replication slot.
		 *
		 * It is not important for these to be removed in single-user mode, so
		 * we don't need any extra handling outside of the custodian process for
		 * this.
		 */
		RemoveOldSerializedSnapshots();

		/*
		 * Remove logical rewrite mapping files that are no longer needed.
		 *
		 * It is not important for these to be removed in single-user mode, so
		 * we don't need any extra handling outside of the custodian process for
		 * this.
		 */
		RemoveOldLogicalRewriteMappings();

		/* Calculate how long to sleep */
		end_time = (pg_time_t) time(NULL);
		elapsed_secs = end_time - start_time;
		if (elapsed_secs >= CUSTODIAN_TIMEOUT_S)
			continue;			/* no sleep for us */
		cur_timeout = CUSTODIAN_TIMEOUT_S - elapsed_secs;

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 cur_timeout * 1000L /* convert to ms */ ,
						 WAIT_EVENT_CUSTODIAN_MAIN);
	}

	pg_unreachable();
}
