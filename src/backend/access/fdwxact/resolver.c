/*-------------------------------------------------------------------------
 *
 * resolver.c
 *
 * The foreign transaction resolver background worker resolves foreign
 * transactions that participate to a distributed transaction. A resolver
 * process is started by foreign transaction launcher for each databases.
 *
 * A resolver process continues to resolve foreign transactions on the
 * database, which the backend process is waiting for resolution.
 *
 * Normal termination is by SIGTERM, which instructs the resolver process
 * to exit(0) at the next convenient moment. Emergency  termination is by
 * SIGQUIT; like any backend. The resolver process also terminate by timeouts
 * only if there is no pending foreign transactions on the database waiting
 * to be resolved.
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/fdwxact/resolver.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "access/fdwxact.h"
#include "access/fdwxact_resolver.h"
#include "access/fdwxact_launcher.h"
#include "access/resolver_internal.h"
#include "access/transam.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "funcapi.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"

/* max sleep time between cycles (3min) */
#define DEFAULT_NAPTIME_PER_CYCLE 180000L

/* GUC parameters */
int foreign_xact_resolution_retry_interval;
int foreign_xact_resolver_timeout = 60 * 1000;
bool foreign_xact_resolve_indoubt_xacts;

FdwXactRslvCtlData *FdwXactRslvCtl;

static void FXRslvLoop(void);
static long FXRslvComputeSleepTime(TimestampTz now, TimestampTz targetTime);
static void FXRslvCheckTimeout(TimestampTz now);

static void fdwxact_resolver_sighup(SIGNAL_ARGS);
static void fdwxact_resolver_onexit(int code, Datum arg);
static void fdwxact_resolver_detach(void);
static void fdwxact_resolver_attach(int slot);

/* Flags set by signal handlers */
static volatile sig_atomic_t got_SIGHUP = false;

/* Set flag to reload configuration at next convenient time */
static void
fdwxact_resolver_sighup(SIGNAL_ARGS)
{
	int		save_errno = errno;

	got_SIGHUP = true;

	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Detach the resolver and cleanup the resolver info.
 */
static void
fdwxact_resolver_detach(void)
{
	/* Block concurrent access */
	LWLockAcquire(FdwXactResolverLock, LW_EXCLUSIVE);

	MyFdwXactResolver->pid = InvalidPid;
	MyFdwXactResolver->in_use = false;
	MyFdwXactResolver->dbid = InvalidOid;

	LWLockRelease(FdwXactResolverLock);
}

/*
 * Cleanup up foreign transaction resolver info.
 */
static void
fdwxact_resolver_onexit(int code, Datum arg)
{
	fdwxact_resolver_detach();

	FdwXactLauncherRequestToLaunch();
}

/*
 * Attach to a slot.
 */
static void
fdwxact_resolver_attach(int slot)
{
	/* Block concurrent access */
	LWLockAcquire(FdwXactResolverLock, LW_EXCLUSIVE);

	Assert(slot >= 0 && slot < max_foreign_xact_resolvers);
	MyFdwXactResolver = &FdwXactRslvCtl->resolvers[slot];

	if (!MyFdwXactResolver->in_use)
	{
		LWLockRelease(FdwXactResolverLock);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("foreign transaction resolver slot %d is empty, cannot attach",
						slot)));
	}

	Assert(OidIsValid(MyFdwXactResolver->dbid));

	MyFdwXactResolver->pid = MyProcPid;
	MyFdwXactResolver->latch = &MyProc->procLatch;
	MyFdwXactResolver->last_resolved_time = 0;

	before_shmem_exit(fdwxact_resolver_onexit, (Datum) 0);

	LWLockRelease(FdwXactResolverLock);
}

/* Foreign transaction resolver entry point */
void
FdwXactResolverMain(Datum main_arg)
{
	int slot = DatumGetInt32(main_arg);

	/* Attach to a slot */
	fdwxact_resolver_attach(slot);

	/* Establish signal handlers */
	pqsignal(SIGHUP, fdwxact_resolver_sighup);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnectionByOid(MyFdwXactResolver->dbid, InvalidOid, 0);

	StartTransactionCommand();

	ereport(LOG,
			(errmsg("foreign transaction resolver for database \"%s\" has started",
					get_database_name(MyFdwXactResolver->dbid))));

	CommitTransactionCommand();

	/* Initialize stats to a sanish value */
	MyFdwXactResolver->last_resolved_time = GetCurrentTimestamp();

	/* Run the main loop */
	FXRslvLoop();

	proc_exit(0);
}

/*
 * Fdwxact resolver main loop
 */
static void
FXRslvLoop(void)
{
	MemoryContext resolver_ctx;

	resolver_ctx = AllocSetContextCreate(TopMemoryContext,
										 "Foreign Transaction Resolver",
										 ALLOCSET_DEFAULT_SIZES);

	/* Enter main loop */
	for (;;)
	{
		PGPROC			*waiter = NULL;
		TransactionId	waitXid = InvalidTransactionId;
		TimestampTz		resolutionTs = -1;
		int			rc;
		TimestampTz	now;
		long		sleep_time = DEFAULT_NAPTIME_PER_CYCLE;

		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		MemoryContextSwitchTo(resolver_ctx);

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		now = GetCurrentTimestamp();

		/*
		 * Process waiter until either the queue gets empty or got the waiter
		 * that has future resolution time.
		 */
		while ((waiter = FdwXactGetWaiter(&resolutionTs, &waitXid)) != NULL)
		{
			CHECK_FOR_INTERRUPTS();
			Assert(TransactionIdIsValid(waitXid));

			if	(resolutionTs > now)
				break;

			elog(DEBUG2, "resolver got one waiter with xid %u", waitXid);

			/* Resolve the waiting distributed transaction */
			StartTransactionCommand();
			FdwXactResolveTransactionAndReleaseWaiter(MyDatabaseId, waitXid,
													  waiter);
			CommitTransactionCommand();

			/* Update my stats */
			SpinLockAcquire(&(MyFdwXactResolver->mutex));
			MyFdwXactResolver->last_resolved_time = GetCurrentTimestamp();
			SpinLockRelease(&(MyFdwXactResolver->mutex));
		}

		FXRslvCheckTimeout(now);

		sleep_time = FXRslvComputeSleepTime(now, resolutionTs);

		MemoryContextResetAndDeleteChildren(resolver_ctx);
		MemoryContextSwitchTo(TopMemoryContext);

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   sleep_time,
					   WAIT_EVENT_FDWXACT_RESOLVER_MAIN);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}
}

/*
 * Check whether there have been foreign transactions by the backend within
 * foreign_xact_resolver_timeout and shutdown if not.
 */
static void
FXRslvCheckTimeout(TimestampTz now)
{
	TimestampTz last_resolved_time;
	TimestampTz timeout;

	if (foreign_xact_resolver_timeout == 0)
		return;

	last_resolved_time = MyFdwXactResolver->last_resolved_time;
	timeout = TimestampTzPlusMilliseconds(last_resolved_time,
										  foreign_xact_resolver_timeout);

	if (now < timeout)
		return;

	LWLockAcquire(FdwXactResolutionLock, LW_SHARED);
	if (!FdwXactWaiterExists(MyDatabaseId))
	{
		StartTransactionCommand();
		ereport(LOG,
				(errmsg("foreign transaction resolver for database \"%s\" will stop because the timeout",
						get_database_name(MyDatabaseId))));
		CommitTransactionCommand();

		/*
		 * Keep holding FdwXactResolutionLock until detached the slot. It is
		 * necessary to prevent a race condition; a waiter enqueues after
		 * checked FdwXactWaiterExists.
		 */
		fdwxact_resolver_detach();
		LWLockRelease(FdwXactResolutionLock);
		proc_exit(0);
	}
	else
		elog(DEBUG2, "resolver reached to the timeout but don't exist as the queue is not empty");

	LWLockRelease(FdwXactResolutionLock);
}

/*
 * Compute how long we should sleep by the next cycle. We can sleep until the time
 * out or the next resolution time given by nextResolutionTs.
 */
static long
FXRslvComputeSleepTime(TimestampTz now, TimestampTz nextResolutionTs)
{
	long	sleeptime = DEFAULT_NAPTIME_PER_CYCLE;

	if (foreign_xact_resolver_timeout > 0)
	{
		TimestampTz timeout;
		long	sec_to_timeout;
		int		microsec_to_timeout;

		/* Compute relative time until wakeup. */
		timeout = TimestampTzPlusMilliseconds(MyFdwXactResolver->last_resolved_time,
											  foreign_xact_resolver_timeout);
		TimestampDifference(now, timeout,
							&sec_to_timeout, &microsec_to_timeout);

		sleeptime = Min(sleeptime,
						sec_to_timeout * 1000 + microsec_to_timeout / 1000);
	}

	if (nextResolutionTs > 0)
	{
		long	sec_to_timeout;
		int		microsec_to_timeout;

		TimestampDifference(now, nextResolutionTs,
							&sec_to_timeout, &microsec_to_timeout);

		sleeptime = Min(sleeptime,
						sec_to_timeout * 1000 + microsec_to_timeout / 1000);
	}

	return sleeptime;
}

bool
IsFdwXactResolver(void)
{
	return MyFdwXactResolver != NULL;
}
