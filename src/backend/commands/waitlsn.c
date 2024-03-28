/*-------------------------------------------------------------------------
 *
 * waitlsn.c
 *	  Implements waiting for the given LSN, which is used in
 *	  CALL pg_wait_for_wal_replay_lsn(target_lsn pg_lsn, timeout float8).
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/commands/waitlsn.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <float.h>
#include <math.h>

#include "pgstat.h"
#include "fmgr.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "access/xlogrecovery.h"
#include "catalog/pg_type.h"
#include "commands/waitlsn.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/sinvaladt.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/fmgrprotos.h"

/* Add to / delete from shared memory array */
static void addLSNWaiter(XLogRecPtr lsn);
static void deleteLSNWaiter(void);

struct WaitLSNState *waitLSN = NULL;
static volatile sig_atomic_t haveShmemItem = false;

/*
 * Report the amount of shared memory space needed for WaitLSNState
 */
Size
WaitLSNShmemSize(void)
{
	Size		size;

	size = offsetof(WaitLSNState, procInfos);
	size = add_size(size, mul_size(MaxBackends, sizeof(WaitLSNProcInfo)));
	return size;
}

/* Initialize the WaitLSNState in the shared memory */
void
WaitLSNShmemInit(void)
{
	bool		found;

	waitLSN = (WaitLSNState *) ShmemInitStruct("WaitLSNState",
											   WaitLSNShmemSize(),
											   &found);
	if (!found)
	{
		SpinLockInit(&waitLSN->mutex);
		waitLSN->numWaitedProcs = 0;
		pg_atomic_init_u64(&waitLSN->minLSN, PG_UINT64_MAX);
	}
}

/*
 * Add the information about the LSN waiter backend to the shared memory
 * array.
 */
static void
addLSNWaiter(XLogRecPtr lsn)
{
	WaitLSNProcInfo cur;
	int			i;

	SpinLockAcquire(&waitLSN->mutex);

	cur.procnum = MyProcNumber;
	cur.waitLSN = lsn;

	for (i = 0; i < waitLSN->numWaitedProcs; i++)
	{
		if (waitLSN->procInfos[i].waitLSN >= cur.waitLSN)
		{
			WaitLSNProcInfo tmp;

			tmp = waitLSN->procInfos[i];
			waitLSN->procInfos[i] = cur;
			cur = tmp;
		}
	}
	waitLSN->procInfos[i] = cur;
	waitLSN->numWaitedProcs++;

	pg_atomic_write_u64(&waitLSN->minLSN, waitLSN->procInfos[i].waitLSN);
	SpinLockRelease(&waitLSN->mutex);
}

/*
 * Delete the information about the LSN waiter backend from the shared memory
 * array.
 */
static void
deleteLSNWaiter(void)
{
	int			i;
	bool		found = false;

	SpinLockAcquire(&waitLSN->mutex);

	for (i = 0; i < waitLSN->numWaitedProcs; i++)
	{
		if (waitLSN->procInfos[i].procnum == MyProcNumber)
			found = true;

		if (found && i < waitLSN->numWaitedProcs - 1)
		{
			waitLSN->procInfos[i] = waitLSN->procInfos[i + 1];
		}
	}

	if (!found)
	{
		SpinLockRelease(&waitLSN->mutex);
		return;
	}
	waitLSN->numWaitedProcs--;

	if (waitLSN->numWaitedProcs != 0)
		pg_atomic_write_u64(&waitLSN->minLSN, waitLSN->procInfos[i].waitLSN);
	else
		pg_atomic_write_u64(&waitLSN->minLSN, PG_UINT64_MAX);

	SpinLockRelease(&waitLSN->mutex);
}

/*
 * Set all latches in shared memory to signal that new LSN has been replayed
*/
void
WaitLSNSetLatches(XLogRecPtr curLSN)
{
	int			i;
	int		   *wakeUpProcNums;
	int			numWakeUpProcs;

	wakeUpProcNums = palloc(sizeof(int) * MaxBackends);

	SpinLockAcquire(&waitLSN->mutex);

	/*
	 * Remember processes, whose waited LSNs are already replayed.  We should
	 * set their latches later after spinlock release.
	 */
	for (i = 0; i < waitLSN->numWaitedProcs; i++)
	{
		if (waitLSN->procInfos[i].waitLSN > curLSN)
			break;

		wakeUpProcNums[i] = waitLSN->procInfos[i].procnum;
	}

	/*
	 * Immediately remove those processes from the shmem array.  Otherwise,
	 * shmem array items will be here till corresponding processes wake up and
	 * delete themselves.
	 */
	numWakeUpProcs = i;
	for (i = 0; i < waitLSN->numWaitedProcs - numWakeUpProcs; i++)
		waitLSN->procInfos[i] = waitLSN->procInfos[i + numWakeUpProcs];
	waitLSN->numWaitedProcs -= numWakeUpProcs;

	if (waitLSN->numWaitedProcs != 0)
		pg_atomic_write_u64(&waitLSN->minLSN, waitLSN->procInfos[i].waitLSN);
	else
		pg_atomic_write_u64(&waitLSN->minLSN, PG_UINT64_MAX);

	SpinLockRelease(&waitLSN->mutex);

	/*
	 * Set latches for processes, whose waited LSNs are already replayed. This
	 * involves spinlocks.  So, we shouldn't do this under a spinlock.
	 */
	for (i = 0; i < numWakeUpProcs; i++)
	{
		PGPROC	   *backend;

		backend = GetPGProcByNumber(wakeUpProcNums[i]);
		SetLatch(&backend->procLatch);
	}
	pfree(wakeUpProcNums);
}

/*
 * Delete our item from shmem array if any.
 */
void
WaitLSNCleanup(void)
{
	if (haveShmemItem)
		deleteLSNWaiter();
}

/*
 * Wait using MyLatch till the given LSN is replayed, the postmaster dies or
 * timeout happens.
 */
void
WaitForLSN(XLogRecPtr lsn, float8 timeout)
{
	XLogRecPtr	curLSN;
	TimestampTz endtime;

	/* Shouldn't be called when shmem isn't initialized */
	Assert(waitLSN);

	/* Should be only called by a backend */
	Assert(MyBackendType == B_BACKEND);

	if (!RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is not in progress"),
				 errhint("Waiting for LSN can only be executed during recovery.")));

	endtime = TimestampTzPlusSeconds(GetCurrentTimestamp(), timeout);

	addLSNWaiter(lsn);
	haveShmemItem = true;

	for (;;)
	{
		int			rc;
		int			latch_events = WL_LATCH_SET | WL_EXIT_ON_PM_DEATH;
		long		delay_ms = 0;

		/* Check if the waited LSN has been replayed */
		curLSN = GetXLogReplayRecPtr(NULL);
		if (lsn <= curLSN)
			break;

		if (timeout > 0.0)
		{
			delay_ms = (endtime - GetCurrentTimestamp()) / 1000;
			latch_events |= WL_TIMEOUT;
			if (delay_ms <= 0)
				break;
		}

		CHECK_FOR_INTERRUPTS();

		/* If postmaster dies, finish immediately */
		if (!PostmasterIsAlive())
			break;

		rc = WaitLatch(MyLatch, latch_events, delay_ms,
					   WAIT_EVENT_WAIT_FOR_WAL_REPLAY);

		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}

	if (lsn > curLSN)
	{
		deleteLSNWaiter();
		haveShmemItem = false;
		ereport(ERROR,
				(errcode(ERRCODE_QUERY_CANCELED),
				 errmsg("canceling waiting for LSN due to timeout")));
	}
	else
	{
		haveShmemItem = false;
	}
}

Datum
pg_wait_for_wal_replay_lsn(PG_FUNCTION_ARGS)
{
	XLogRecPtr	target_lsn = PG_GETARG_LSN(0);
	float8		timeout = PG_GETARG_FLOAT8(1);
	CallContext *context = (CallContext *) fcinfo->context;

	if (context->atomic)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_wait_for_wal_replay_lsn() must be only called in non-atomic context")));

	if (ActiveSnapshotSet())
		PopActiveSnapshot();
	Assert(!ActiveSnapshotSet());

	(void) WaitForLSN(target_lsn, timeout);

	PG_RETURN_VOID();
}
