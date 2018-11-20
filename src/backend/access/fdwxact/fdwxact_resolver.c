/*-------------------------------------------------------------------------
 *
 * fdwxact_resolver.c
 *
 * The foreign transaction resolver background worker resolves foreign
 * transactions that participate to a distributed transaction. A resolver
 * process is started by foreign transaction launcher for every databases.
 *
 * A resolver process continues to resolve foreign transactions on a database
 * It resolves two types of foreign transactions: on-line foreign transaction
 * and dangling foreign transaction. The on-line foreign transaction is a
 * foreign transaction that a concurrent backend process is waiting for
 * resolution. The dangling transaction is a foreign transaction that corresponding
 * distributed transaction ended up in in-doubt state. A resolver process
 * doesn' exit as long as there is at least one unresolved foreign transaction
 * on the database even if the timeout has come.
 *
 * Normal termination is by SIGTERM, which instructs the resolver process
 * to exit(0) at the next convenient moment. Emergency  termination is by
 * SIGQUIT; like any backend. The resolver process also terminate by timeouts
 * only if there is no pending foreign transactions on the database waiting
 * to be resolved.
 *
 * Portions Copyright (c) 2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/fdwxact/fdwxact_resolver.c
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

/* GUC parameters */
int foreign_xact_resolution_retry_interval;
int foreign_xact_resolver_timeout = 60 * 1000;

//static MemoryContext ResolveContext = NULL;
FdwXactRslvCtlData *FdwXactRslvCtl;

static void FdwXactRslvLoop(void);
static long FdwXactRslvComputeSleepTime(TimestampTz now);
static void FdwXactRslvCheckTimeout(TimestampTz now);

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
	FdwXactLauncherWakeupToRetry();
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

	MyFdwXactResolver->pid = MyProcPid;
	MyFdwXactResolver->latch = &MyProc->procLatch;
	TIMESTAMP_NOBEGIN(MyFdwXactResolver->last_resolved_time);

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
	FdwXactRslvLoop();

	proc_exit(0);
}

/*
 * Fdwxact resolver main loop
 */
static void
FdwXactRslvLoop(void)
{
	TimestampTz last_retry_time = 0;
	MemoryContext resolver_ctx;

	resolver_ctx = AllocSetContextCreate(TopMemoryContext,
										 "Foreign Transaction Resolver",
										 ALLOCSET_DEFAULT_SIZES);

	/* Enter main loop */
	for (;;)
	{
		int			rc;
		TimestampTz	now;
		long		sleep_time;
		bool		resolved;

		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		MemoryContextSwitchTo(resolver_ctx);

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Resolve one distributed transaction */
		StartTransactionCommand();
		resolved = FdwXactResolveDistributedTransaction(MyDatabaseId, true);
		CommitTransactionCommand();

		now = GetCurrentTimestamp();

		/* Update my state */
		if (resolved)
			MyFdwXactResolver->last_resolved_time = now;

		if (TimestampDifferenceExceeds(last_retry_time, now,
									   foreign_xact_resolution_retry_interval))
		{
			StartTransactionCommand();
			resolved = FdwXactResolveDistributedTransaction(MyDatabaseId, false);
			CommitTransactionCommand();

			last_retry_time = GetCurrentTimestamp();

			/* Update my state */
			if (resolved)
				MyFdwXactResolver->last_resolved_time = last_retry_time;
		}

		/* Check for fdwxact resolver timeout */
		FdwXactRslvCheckTimeout(now);

		/*
		 * If we have resolved any distributed transaction we go the next
		 * without both resolving dangling transaction and sleeping because
		 * there might be other on-line transactions waiting to be resolved.
		 */
		if (!resolved)
		{
			/* Resolve dangling transactions as mush as possible */
			StartTransactionCommand();
			FdwXactResolveAllDanglingTransactions(MyDatabaseId);
			CommitTransactionCommand();

			sleep_time = FdwXactRslvComputeSleepTime(now);

			MemoryContextResetAndDeleteChildren(resolver_ctx);
			MemoryContextSwitchTo(TopMemoryContext);

			rc = WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   sleep_time,
						   WAIT_EVENT_FDW_XACT_RESOLVER_MAIN);

			if (rc & WL_POSTMASTER_DEATH)
				proc_exit(1);
		}
	}
}

/*
 * Check whether there have been foreign transactions by the backend within
 * foreign_xact_resolver_timeout and shutdown if not.
 */
static void
FdwXactRslvCheckTimeout(TimestampTz now)
{
	TimestampTz timeout;

	if (foreign_xact_resolver_timeout == 0)
		return;

	timeout = TimestampTzPlusMilliseconds(MyFdwXactResolver->last_resolved_time,
										  foreign_xact_resolver_timeout);

	if (now < timeout)
		return;

	/*
	 * Reached to the timeout. We exit if there is no more both pending on-line
	 * transactions and dangling transactions.
	 */
	if (!fdw_xact_exists(InvalidTransactionId, MyDatabaseId, InvalidOid,
						 InvalidOid))
	{
		StartTransactionCommand();
		ereport(LOG,
				(errmsg("foreign transaction resolver for database \"%s\" will stop because the timeout",
						get_database_name(MyFdwXactResolver->dbid))));
		CommitTransactionCommand();

		fdwxact_resolver_detach();
		proc_exit(0);
	}
}

/*
 * Compute how long we should sleep by the next cycle. Return the sleep time
 * in milliseconds, -1 means that we reached to the timeout and should exits
 */
static long
FdwXactRslvComputeSleepTime(TimestampTz now)
{
	static TimestampTz	wakeuptime = 0;
	long	sleeptime;
	long	sec_to_timeout;
	int		microsec_to_timeout;

	if (now >= wakeuptime)
		wakeuptime = TimestampTzPlusMilliseconds(now,
												 foreign_xact_resolution_retry_interval);

	/* Compute relative time until wakeup. */
	TimestampDifference(now, wakeuptime,
						&sec_to_timeout, &microsec_to_timeout);

	sleeptime = sec_to_timeout * 1000 + microsec_to_timeout / 1000;

	return sleeptime;
}

bool
IsFdwXactResolver(void)
{
	return MyFdwXactResolver != NULL;
}
