/*-------------------------------------------------------------------------
 *
 * launcher.c
 *
 * The foreign transaction resolver launcher process starts foreign
 * transaction resolver processes. The launcher schedules resolver
 * process to be started when arrived a requested by backend process.
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/fdwxact/launcher.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "pgstat.h"
#include "funcapi.h"

#include "access/fdwxact.h"
#include "access/fdwxact_launcher.h"
#include "access/fdwxact_resolver.h"
#include "access/resolver_internal.h"
#include "access/twophase.h"
#include "commands/dbcommands.h"
#include "funcapi.h"
#include "nodes/pg_list.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"

/* max sleep time between cycles (3min) */
#define DEFAULT_NAPTIME_PER_CYCLE 180000L

static void fdwxact_launcher_onexit(int code, Datum arg);
static void fdwxact_launcher_sighup(SIGNAL_ARGS);
static void fdwxact_launch_resolver(Oid dbid);
static bool fdwxact_relaunch_resolvers(void);

static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGUSR2 = false;
FdwXactResolver *MyFdwXactResolver = NULL;

/*
 * Wake up the launcher process to request launching new resolvers
 * immediately.
 */
void
FdwXactLauncherRequestToLaunch(void)
{
	if (FdwXactRslvCtl->launcher_pid != InvalidPid)
		kill(FdwXactRslvCtl->launcher_pid, SIGUSR2);
}

/* Report shared memory space needed by FdwXactRsoverShmemInit */
Size
FdwXactRslvShmemSize(void)
{
	Size		size = 0;

	size = add_size(size, SizeOfFdwXactRslvCtlData);
	size = add_size(size, mul_size(max_foreign_xact_resolvers,
								   sizeof(FdwXactResolver)));

	return size;
}

/*
 * Allocate and initialize foreign transaction resolver shared
 * memory.
 */
void
FdwXactRslvShmemInit(void)
{
	bool		found;

	FdwXactRslvCtl = ShmemInitStruct("Foreign transactions resolvers",
									 FdwXactRslvShmemSize(),
									 &found);

	if (!IsUnderPostmaster)
	{
		int			slot;

		/* First time through, so initialize */
		MemSet(FdwXactRslvCtl, 0, FdwXactRslvShmemSize());
		SHMQueueInit(&(FdwXactRslvCtl->fdwxact_queue));
		FdwXactRslvCtl->launcher_pid = InvalidPid;

		for (slot = 0; slot < max_foreign_xact_resolvers; slot++)
		{
			FdwXactResolver *resolver = &FdwXactRslvCtl->resolvers[slot];

			memset(resolver, 0, sizeof(FdwXactResolver));
			SpinLockInit(&(resolver->mutex));
		}
	}
}

/*
 * Cleanup function for fdwxact launcher
 *
 * Called on fdwxact launcher exit.
 */
static void
fdwxact_launcher_onexit(int code, Datum arg)
{
	FdwXactRslvCtl->launcher_pid = InvalidPid;
}

/* SIGHUP: set flag to reload configuration at next convenient time */
static void
fdwxact_launcher_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGHUP = true;

	SetLatch(MyLatch);

	errno = save_errno;
}

/* SIGUSR2: set flag to launch new resolver process immediately */
static void
fdwxact_launcher_sigusr2(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGUSR2 = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Main loop for the fdwxact launcher process.
 */
void
FdwXactLauncherMain(Datum main_arg)
{
	TimestampTz last_start_time = 0;

	ereport(DEBUG1,
			(errmsg("fdwxact resolver launcher started")));

	before_shmem_exit(fdwxact_launcher_onexit, (Datum) 0);

	Assert(FdwXactRslvCtl->launcher_pid == InvalidPid);
	FdwXactRslvCtl->launcher_pid = MyProcPid;
	FdwXactRslvCtl->launcher_latch = &MyProc->procLatch;

	pqsignal(SIGHUP, fdwxact_launcher_sighup);
	pqsignal(SIGUSR2, fdwxact_launcher_sigusr2);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	/* Enter main loop */
	for (;;)
	{
		TimestampTz now;
		long		wait_time = DEFAULT_NAPTIME_PER_CYCLE;
		int			rc;

		CHECK_FOR_INTERRUPTS();
		ResetLatch(MyLatch);

		now = GetCurrentTimestamp();

		/*
		 * Limit the start retry to once a
		 * foreign_xact_resolution_retry_interval but always attempt to
		 * start when requested.
		 */
		if (got_SIGUSR2 ||
			TimestampDifferenceExceeds(last_start_time, now,
									   foreign_xact_resolution_retry_interval))
		{
			MemoryContext oldctx;
			MemoryContext subctx;
			bool		launched;

			if (got_SIGUSR2)
				got_SIGUSR2 = false;

			subctx = AllocSetContextCreate(TopMemoryContext,
										   "Foreign Transaction Launcher",
										   ALLOCSET_DEFAULT_SIZES);
			oldctx = MemoryContextSwitchTo(subctx);

			/*
			 * Launch foreign transaction resolvers that are requested but not
			 * running.
			 */
			launched = fdwxact_relaunch_resolvers();
			if (launched)
			{
				last_start_time = now;
				wait_time = foreign_xact_resolution_retry_interval;
			}

			/* Switch back to original memory context. */
			MemoryContextSwitchTo(oldctx);
			/* Clean the temporary memory. */
			MemoryContextDelete(subctx);
		}
		else
		{
			/*
			 * The wait in previous cycle was interrupted in less than
			 * foreign_xact_resolution_retry_interval since last resolver
			 * started, this usually means crash of the resolver, so we should
			 * retry in foreign_xact_resolution_retry_interval again.
			 */
			wait_time = foreign_xact_resolution_retry_interval;
		}

		/* Wait for more work */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   wait_time,
					   WAIT_EVENT_FDWXACT_LAUNCHER_MAIN);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
	}

	/* Not reachable */
}

/*
 * Request launcher to launch a new foreign transaction resolver process
 * or wake up the resolver if it's already running.
 */
void
FdwXactLaunchOrWakeupResolver(void)
{
	volatile FdwXactResolver *resolver;
	bool		found = false;

	/*
	 * Looking for a resolver process that is running and working on the same
	 * database.
	 */
	LWLockAcquire(FdwXactResolverLock, LW_SHARED);
	for (int i = 0; i < max_foreign_xact_resolvers; i++)
	{
		resolver = &FdwXactRslvCtl->resolvers[i];

		if (resolver->in_use &&
			resolver->dbid == MyDatabaseId)
		{
			found = true;
			break;
		}
	}
	LWLockRelease(FdwXactResolverLock);

	if (found)
	{
		/* Found the running resolver */
		elog(DEBUG1,
			 "found a running foreign transaction resolver process for database %u",
			 MyDatabaseId);

		/*
		 * Wakeup the resolver. It's possible that the resolver is starting up
		 * and doesn't attach its slot yet. Since the resolver will find
		 * FdwXact entry we inserted soon we don't anything.
		 */
		if (resolver->latch)
			SetLatch(resolver->latch);

		return;
	}

	/* Otherwise wake up the launcher to launch new resolver */
	FdwXactLauncherRequestToLaunch();
}

/*
 * Launch a foreign transaction resolver process that will connect to given
 * 'dbid'.
 */
static void
fdwxact_launch_resolver(Oid dbid)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *bgw_handle;
	FdwXactResolver *resolver;
	int			unused_slot = -1;
	int			i;

	LWLockAcquire(FdwXactResolverLock, LW_EXCLUSIVE);

	/* Find unused resolver slot */
	for (i = 0; i < max_foreign_xact_resolvers; i++)
	{
		FdwXactResolver *resolver = &FdwXactRslvCtl->resolvers[i];

		if (!resolver->in_use)
		{
			unused_slot = i;
			break;
		}
	}

	/* No unused found */
	if (i >= max_foreign_xact_resolvers)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("out of foreign transaction resolver slots"),
				 errhint("You might need to increase max_foreign_transaction_resolvers.")));

	resolver = &FdwXactRslvCtl->resolvers[unused_slot];
	resolver->in_use = true;
	resolver->dbid = dbid;
	LWLockRelease(FdwXactResolverLock);

	/* Register the new dynamic worker */
	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "FdwXactResolverMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "foreign transaction resolver for database %u", resolver->dbid);
	snprintf(bgw.bgw_type, BGW_MAXLEN, "foreign transaction resolver");
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_notify_pid = MyProcPid;
	bgw.bgw_main_arg = Int32GetDatum(unused_slot);

	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
	{
		/* Failed to launch, cleanup the worker slot */
		SpinLockAcquire(&(MyFdwXactResolver->mutex));
		resolver->in_use = false;
		SpinLockRelease(&(MyFdwXactResolver->mutex));

		ereport(WARNING,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("out of background worker slots"),
				 errhint("You might need to increase max_worker_processes.")));
	}

	/*
	 * We don't need to wait until it attaches here because we're going to
	 * wait until all foreign transactions are resolved.
	 */
}

/*
 * Launch or relaunch foreign transaction resolvers on database that has
 * at least one FdwXact entry but no resolver is running on it.
 */
static bool
fdwxact_relaunch_resolvers(void)
{
	HTAB	   *fdwxact_dbs;
	HTAB	   *resolver_dbs;
	HASHCTL		ctl;
	HASH_SEQ_STATUS status;
	Oid		   *entry;
	bool		launched;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(Oid);

	/*
	 * Create a hash map for the database that has at least one foreign
	 * transaction to resolve.
	 */
	fdwxact_dbs = hash_create("fdwxact dblist",
							  32, &ctl, HASH_ELEM | HASH_BLOBS);

	/* Collect database oids that has at least one FdwXact entry to resolve */
	LWLockAcquire(FdwXactLock, LW_SHARED);
	for (int i = 0; i < FdwXactCtl->num_fdwxacts; i++)
	{
		FdwXact		fdwxact = FdwXactCtl->fdwxacts[i];

		if (!fdwxact->valid)
			continue;

		/*
		 * We need to launch resolver process if the foreign transaction
		 * is not held by anyone and is not a part of the local prepared
		 * transaction.
		 */
		if (fdwxact->locking_backend == InvalidBackendId &&
			!TwoPhaseExists(fdwxact->local_xid))
			hash_search(fdwxact_dbs, &(fdwxact->dbid), HASH_ENTER, NULL);
	}
	LWLockRelease(FdwXactLock);

	/* There is no foreign transaction to resolve, no need to launch new one */
	if (hash_get_num_entries(fdwxact_dbs) == 0)
	{
		hash_destroy(fdwxact_dbs);
		return false;
	}

	/* Create a hash map for databases on which a resolver is running */
	resolver_dbs = hash_create("resolver dblist",
							   32, &ctl, HASH_ELEM | HASH_BLOBS);

	/* Collect database oids on which resolvers are running */
	LWLockAcquire(FdwXactResolverLock, LW_SHARED);
	for (int i = 0; i < max_foreign_xact_resolvers; i++)
	{
		FdwXactResolver *resolver = &FdwXactRslvCtl->resolvers[i];

		if (!resolver->in_use)
			continue;

		hash_search(resolver_dbs, &(resolver->dbid), HASH_ENTER, NULL);
	}
	LWLockRelease(FdwXactResolverLock);

	/*
	 * Find databases on which no resolver is running and launch new
	 * resolver process on them.
	 */
	hash_seq_init(&status, fdwxact_dbs);
	while ((entry = (Oid *) hash_seq_search(&status)) != NULL)
	{
		bool		found;

		hash_search(resolver_dbs, entry, HASH_FIND, &found);

		if (!found)
		{
			/* No resolver is running on this database, launch new one */
			fdwxact_launch_resolver(*entry);
			launched = true;
		}
	}

	hash_destroy(fdwxact_dbs);
	hash_destroy(resolver_dbs);

	return launched;
}

/* Register a background worker running the foreign transaction launcher */
void
FdwXactLauncherRegister(void)
{
	BackgroundWorker bgw;

	if (max_foreign_xact_resolvers == 0)
		return;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "FdwXactLauncherMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "foreign transaction launcher");
	snprintf(bgw.bgw_type, BGW_MAXLEN,
			 "foreign transaction launcher");
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
}

bool
IsFdwXactLauncher(void)
{
	return FdwXactRslvCtl->launcher_pid == MyProcPid;
}

/*
 * Stop the fdwxact resolver running on the given database.
 */
Datum
pg_stop_foreign_xact_resolver(PG_FUNCTION_ARGS)
{
	Oid			dbid = PG_GETARG_OID(0);
	FdwXactResolver *resolver = NULL;
	int			i;

	/* Must be super user */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to stop foreign transaction resolver")));

	if (!OidIsValid(dbid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid database id")));

	LWLockAcquire(FdwXactResolverLock, LW_SHARED);

	/* Find the running resolver process on the given database */
	for (i = 0; i < max_foreign_xact_resolvers; i++)
	{
		resolver = &FdwXactRslvCtl->resolvers[i];

		/* found! */
		if (resolver->in_use && resolver->dbid == dbid)
			break;
	}

	if (i >= max_foreign_xact_resolvers)
		ereport(ERROR,
				(errmsg("there is no running foreign transaction resolver process on database %d",
						dbid)));

	/* Found the resolver, terminate it ... */
	kill(resolver->pid, SIGTERM);

	/* ... and wait for it to die */
	for (;;)
	{
		int			rc;

		/* is it gone? */
		if (!resolver->in_use)
			break;

		LWLockRelease(FdwXactResolverLock);

		/* Wait a bit --- we don't expect to have to wait long. */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   10L, WAIT_EVENT_BGWORKER_SHUTDOWN);

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}

		LWLockAcquire(FdwXactResolverLock, LW_SHARED);
	}

	LWLockRelease(FdwXactResolverLock);

	PG_RETURN_BOOL(true);
}
