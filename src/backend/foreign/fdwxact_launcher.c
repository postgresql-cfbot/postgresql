/*-------------------------------------------------------------------------
 *
 * fdwxact_launcher.c
 *
 * The foreign transaction resolver launcher process starts foreign
 * transaction resolver processes. The launcher schedules resolver
 * process to be started when arrived a requested by backend process.
 *
 * There is a shared memory area where the information of resolver process
 * is stored. Requesting of starting new resolver process by backend process
 * is done via that shared memory area. Note that the launcher is assuming
 * that there is no more than one starting request for a database.
 *
 * Portions Copyright (c) 2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/foreign/fdwxact_launcher.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "pgstat.h"
#include "funcapi.h"

#include "foreign/fdwxact.h"
#include "foreign/fdwxact_launcher.h"
#include "foreign/fdwxact_resolver.h"
#include "foreign/resolver_internal.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"

#define DEFAULT_NAPTIME_PER_CYCLE 180000L

static void fdwxact_launcher_onexit(int code, Datum arg);
static void fdwxact_launcher_sighup(SIGNAL_ARGS);
static void fdwxact_launch_resolver(Oid dbid, int slot);
static bool fdwxact_relaunch_resolvers(void);

static volatile sig_atomic_t got_SIGHUP = false;
FdwXactResolver *MyFdwXactResolver = NULL;

Datum pg_stat_get_fdwxact_resolver(PG_FUNCTION_ARGS);

/*
 * Wake up the launcher process.
 */
void
FdwXactLauncherWakeup(void)
{
	if (FdwXactRslvCtl->launcher_pid != InvalidPid)
		kill(FdwXactRslvCtl->launcher_pid, SIGUSR1);
}

/* Report shared memory space needed by FdwXactRsoverShmemInit */
Size
FdwXactRslvShmemSize(void)
{
	Size		size = 0;

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
	bool found;

	FdwXactRslvCtl = ShmemInitStruct("Foreign transactions resolvers",
									 FdwXactRslvShmemSize(),
									 &found);

	if (!IsUnderPostmaster)
	{
		int	slot;

		/* First time through, so initialize */
		MemSet(FdwXactRslvCtl, 0, FdwXactRslvShmemSize());

		SHMQueueInit(&(FdwXactRslvCtl->FdwXactQueue));

		for (slot = 0; slot < max_foreign_xact_resolvers; slot++)
		{
			FdwXactResolver *resolver = &FdwXactRslvCtl->resolvers[slot];

			resolver->pid = InvalidPid;
			resolver->dbid = InvalidOid;
			resolver->in_use = false;
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
	int	save_errno = errno;

	got_SIGHUP = true;

	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Main loop for the fdwxact launcher process.
 */
void
FdwXactLauncherMain(Datum main_arg)
{
	TimestampTz	last_start_time = 0;

	ereport(DEBUG1,
			(errmsg("fdwxact resolver launcher started")));

	before_shmem_exit(fdwxact_launcher_onexit, (Datum) 0);

	Assert(FdwXactRslvCtl->launcher_pid == 0);
	FdwXactRslvCtl->launcher_pid = MyProcPid;

	pqsignal(SIGHUP, fdwxact_launcher_sighup);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	/* Enter main loop */
	for (;;)
	{
		TimestampTz	now;
		long	wait_time = DEFAULT_NAPTIME_PER_CYCLE;
		int		rc;

		CHECK_FOR_INTERRUPTS();

		now = GetCurrentTimestamp();

		if (TimestampDifferenceExceeds(last_start_time, now,
									   foreign_xact_resolution_retry_interval))
		{
			bool launched;

			/*
			 * Launch foreign transaction resolvers that are requested
			 * but not running.
			 */
			launched = fdwxact_relaunch_resolvers();
			if (launched)
				last_start_time = now;
		}
		else
		{
			/*
			 * The wint in previous cycle was interrupted in less than
			 * foreign_xact_resolution_retry_interval since last resolver
			 * started, this usually means crash of the resolver, so we
			 * should retry in foreign_xact_resolution_retry_interval again.
			 */
			wait_time = foreign_xact_resolution_retry_interval;
		}

		/* Wait for more work */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   wait_time,
					   WAIT_EVENT_FDW_XACT_LAUNCHER_MAIN);

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
 * Request launcher to launch a new foreign transaction resolver worker
 * if not running yet. A foreign transaction resolver worker is responsible
 * for resolution of foreign transaction that are registered on a database.
 * So if a resolver worker already is launched, we don't need to launch new
 * one.
 */
void
fdwxact_maybe_launch_resolver(bool ignore_error)
{
	FdwXactResolver *resolver;
	bool	found = false;
	int		i;

	/*
	 * Looking for a resolver process that is running and working on the
	 * same database.
	 */
	LWLockAcquire(FdwXactResolverLock, LW_SHARED);
	for (i = 0; i < max_foreign_xact_resolvers; i++)
	{
		resolver = &FdwXactRslvCtl->resolvers[i];

		if (resolver->in_use &&
			resolver->pid != InvalidPid &&
			resolver->dbid == MyDatabaseId)
		{
			found = true;
			break;
		}
	}
	LWLockRelease(FdwXactResolverLock);

	/*
	 * If we found the resolver for my database, we don't need to launch new
	 * one but wake running worker up.
	 */
	if (found)
	{
		SetLatch(resolver->latch);

		elog(DEBUG1, "found a running foreign transaction resolver process for database %u",
			 MyDatabaseId);

		return;
	}

	/* Looking for unused worker slot */
	LWLockAcquire(FdwXactResolverLock, LW_EXCLUSIVE);
	for (i = 0; i < max_foreign_xact_resolvers; i++)
	{
		resolver = &FdwXactRslvCtl->resolvers[i];

		if (!resolver->in_use)
		{
			found = true;
			break;
		}
	}

	/*
	 * However if there are no more free worker slots, inform user about it before
	 * exiting.
	 */
	if (!found)
	{
		LWLockRelease(FdwXactResolverLock);

		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("out of foreign trasanction resolver slots"),
				 errhint("You might need to increase max_foreign_transaction_resolvers.")));
		return;
	}

	Assert(resolver->pid == InvalidPid);

	/* Found a new resolver process */
	resolver->dbid = MyDatabaseId;
	resolver->in_use = true;

	LWLockRelease(FdwXactResolverLock);

	/* Wake up launcher */
	FdwXactLauncherWakeup();
}

/*
 * Launch a foreign transaction resolver process that will connect to given
 * 'dbid' at 'slot' if given. If slot is negative value we find an unused slot.
 * Note that caller must hold FdwXactResolverLock in exclusive mode.
 */
static void
fdwxact_launch_resolver(Oid dbid, int slot)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *bgw_handle;
	FdwXactResolver *resolver;
	int launch_slot = slot;

	/* If slot number is invalid, we find an unused slot */
	if (launch_slot < 0)
	{
		int i;

		for (i = 0; i < max_foreign_xact_resolvers; i++)
		{
			FdwXactResolver *resolver = &FdwXactRslvCtl->resolvers[i];

			if (resolver->in_use && resolver->dbid == dbid)
				return;

			if (!resolver->in_use)
			{
				launch_slot = i;
				break;
			}
		}
	}

	/* No unused found */
	if (launch_slot < 0)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("out of foreign trasanction resolver slots"),
				 errhint("You might need to increase max_foreign_transaction_resolvers.")));

	resolver = &FdwXactRslvCtl->resolvers[launch_slot];
	resolver->in_use = true;
	resolver->dbid = dbid;

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
	bgw.bgw_main_arg = Int32GetDatum(launch_slot);
	bgw.bgw_notify_pid = (Datum) 0;

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
	 * We don't need to wait until it attaches here because we're going to wait
	 * until all foreign transactions are resolved.
	 */
}

/*
 * Launch all foreign transaction resolvers that are required by backend process
 * but not running.
 */
static bool
fdwxact_relaunch_resolvers(void)
{
	int i, j;
	int num_launches = 0;
	int num_unused_slots = 0;
	int num_dbs = 0;
	bool launched = false;
	Oid	*dbs_to_launch;
	Oid *dbs_having_worker = palloc0(sizeof(Oid) * max_foreign_xact_resolvers);

	/*
	 * Launch resolver workers on the databases that are requested
	 * by backend processes.
	 */
	LWLockAcquire(FdwXactResolverLock, LW_EXCLUSIVE);
	for (i = 0; i < max_foreign_xact_resolvers; i++)
	{
		FdwXactResolver *resolver = &FdwXactRslvCtl->resolvers[i];

		/* Remember unused worker slots */
		if (!resolver->in_use)
			num_unused_slots++;

		/* Remember databases that are having a resolve worker */
		if (OidIsValid(resolver->dbid))
			dbs_having_worker[num_dbs++] = resolver->dbid;

		/* Launch new foreign transaction resolver worker on the database */
		if (resolver->in_use &&
			OidIsValid(resolver->dbid) &&
			resolver->pid == InvalidPid)
		{
			fdwxact_launch_resolver(resolver->dbid, i);
			launched = true;
		}
	}
	LWLockRelease(FdwXactResolverLock);

	/* There is no unused slot, exit */
	if (num_unused_slots == 0)
		return launched;

	dbs_to_launch = (Oid *) palloc(sizeof(Oid) * num_unused_slots);

	/*
	 * If there is unused slot, we can launch foreign transaction resolver
	 * on databases that has unresolved foreign transaction but doesn't
	 * have any resolver. This usually happens when resolvers crash for
	 * whatever reason. Scanning all FdwXact entries could takes time but
	 * since this is a relaunch case it's not harmless.
	 */
	LWLockAcquire(FdwXactLock, LW_SHARED);
	for (i = 0; i < FdwXactCtl->numFdwXacts; i++)
	{
		FdwXact fdw_xact = FdwXactCtl->fdw_xacts[i];
		bool found = false;

		if (num_launches > num_unused_slots)
			break;

		for (j = 0; j < num_dbs; j++)
		{
			if (dbs_having_worker[j] == fdw_xact->dbid)
			{
				found = true;
				break;
			}
		}

		if (found)
			continue;

		dbs_to_launch[num_launches++] = fdw_xact->dbid;
	}
	LWLockRelease(FdwXactLock);

	/* Launch resolver process for a database at any worker slot */
	LWLockAcquire(FdwXactResolverLock, LW_EXCLUSIVE);
	for (i = 0; i < num_launches; i++)
	{
		fdwxact_launch_resolver(dbs_to_launch[i], -1);
		launched = true;
	}
	LWLockRelease(FdwXactResolverLock);

	return launched;
}

/*
 * FdwXactLauncherRegister
 *		Register a background worker running the foreign transaction
 *      launcher.
 */
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
 * Returns activity of foreign transaction resolvers, including pids, the number
 * of tasks and the last resolution time.
 */
Datum
pg_stat_get_fdwxact_resolver(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_FDWXACT_RESOLVERS_COLS 3
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	int i;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	for (i = 0; i < max_foreign_xact_resolvers; i++)
	{
		FdwXactResolver	*resolver = &FdwXactRslvCtl->resolvers[i];
		pid_t	pid;
		Oid		dbid;
		TimestampTz last_resolved_time;
		Datum		values[PG_STAT_GET_FDWXACT_RESOLVERS_COLS];
		bool		nulls[PG_STAT_GET_FDWXACT_RESOLVERS_COLS];


		SpinLockAcquire(&(MyFdwXactResolver->mutex));
		if (resolver->pid == 0)
		{
			SpinLockRelease(&(MyFdwXactResolver->mutex));
			continue;
		}

		pid = resolver->pid;
		dbid = resolver->dbid;
		last_resolved_time = resolver->last_resolved_time;
		SpinLockRelease(&(MyFdwXactResolver->mutex));

		memset(nulls, 0, sizeof(nulls));
		/* pid */
		values[0] = Int32GetDatum(pid);

		/* dbid */
		values[1] = ObjectIdGetDatum(dbid);

		/* last_resolved_time */
		if (last_resolved_time == 0)
			nulls[2] = true;
		else
			values[2] = TimestampTzGetDatum(last_resolved_time);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}
