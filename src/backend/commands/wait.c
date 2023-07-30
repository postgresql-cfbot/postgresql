/*-------------------------------------------------------------------------
 *
 * wait.c
 *	  Implements wait lsn, which allows waiting for events such as
 *	  LSN having been replayed on replica.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 2023, Regents of PostgresPro
 *
 * IDENTIFICATION
 *	  src/backend/commands/wait.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/xact.h"
#include "access/xlogrecovery.h"
#include "access/xlogdefs.h"
#include "commands/wait.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/backendid.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/sinvaladt.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"

/* Add to shared memory array */
static void AddWaitedLSN(XLogRecPtr lsn_to_wait);

/* Shared memory structure */
typedef struct
{
	int			backend_maxid;
	pg_atomic_uint64 min_lsn; /* XLogRecPtr of minimal waited for LSN */
	slock_t		mutex;
	/* LSNs that different backends are waiting */
	XLogRecPtr	lsn[FLEXIBLE_ARRAY_MEMBER];
} WaitState;

static WaitState *state;

/*
 * Add the wait event of the current backend to shared memory array
 */
static void
AddWaitedLSN(XLogRecPtr lsn_to_wait)
{
	SpinLockAcquire(&state->mutex);
	if (state->backend_maxid < MyBackendId)
		state->backend_maxid = MyBackendId;

	state->lsn[MyBackendId] = lsn_to_wait;

	if (lsn_to_wait < state->min_lsn.value)
		state->min_lsn.value = lsn_to_wait;
	SpinLockRelease(&state->mutex);
}

/*
 * Delete wait event of the current backend from the shared memory array.
 */
void
DeleteWaitedLSN(void)
{
	int			i;
	XLogRecPtr	lsn_to_delete;

	SpinLockAcquire(&state->mutex);

	lsn_to_delete = state->lsn[MyBackendId];
	state->lsn[MyBackendId] = InvalidXLogRecPtr;

	/* If we are deleting the minimal LSN, then choose the next min_lsn */
	if (lsn_to_delete != InvalidXLogRecPtr &&
		lsn_to_delete == state->min_lsn.value)
	{
		state->min_lsn.value = PG_UINT64_MAX;
		for (i = 2; i <= state->backend_maxid; i++)
			if (state->lsn[i] != InvalidXLogRecPtr &&
				state->lsn[i] < state->min_lsn.value)
				state->min_lsn.value = state->lsn[i];
	}

	/* If deleting from the end of the array, shorten the array's used part */
	if (state->backend_maxid == MyBackendId)
		for (i = (MyBackendId); i >= 2; i--)
			if (state->lsn[i] != InvalidXLogRecPtr)
			{
				state->backend_maxid = i;
				break;
			}

	SpinLockRelease(&state->mutex);
}

/*
 * Report amount of shared memory space needed for WaitState
 */
Size
WaitShmemSize(void)
{
	Size		size;

	size = offsetof(WaitState, lsn);
	size = add_size(size, mul_size(MaxBackends + 1, sizeof(XLogRecPtr)));
	return size;
}

/*
 * Initialize an array of events to wait for in shared memory
 */
void
WaitShmemInit(void)
{
	bool		found;
	uint32		i;

	state = (WaitState *) ShmemInitStruct("pg_wait_lsn",
										  WaitShmemSize(),
										  &found);
	if (!found)
	{
		SpinLockInit(&state->mutex);

		for (i = 0; i < (MaxBackends + 1); i++)
			state->lsn[i] = InvalidXLogRecPtr;

		state->backend_maxid = 0;
		state->min_lsn.value = PG_UINT64_MAX;
	}
}

/*
 * Set latches in shared memory to signal that new LSN has been replayed
 */
void
WaitSetLatch(XLogRecPtr cur_lsn)
{
	uint32		i;
	int			backend_maxid;
	PGPROC	   *backend;

	SpinLockAcquire(&state->mutex);
	backend_maxid = state->backend_maxid;

	for (i = 2; i <= backend_maxid; i++)
	{
		backend = BackendIdGetProc(i);

		if (backend && state->lsn[i] != 0 &&
			state->lsn[i] <= cur_lsn)
		{
			SetLatch(&backend->procLatch);
		}
	}
	SpinLockRelease(&state->mutex);
}

/*
 * Get minimal LSN that someone waits for
 */
XLogRecPtr
GetMinWaitedLSN(void)
{
	return state->min_lsn.value;
}

/*
 * On WAIT use a latch to wait till LSN is replayed,
 * postmaster dies or timeout happens. Timeout is specified in milliseconds.
 * Returns true if LSN was reached and false otherwise.
 */
bool
WaitUtility(XLogRecPtr target_lsn, const int timeout_ms)
{
	XLogRecPtr	cur_lsn = GetXLogReplayRecPtr(NULL);
	int			latch_events;
	float8		endtime;
	bool		res = false;
	bool		wait_forever = (timeout_ms <= 0);

	/*
	 * In transactions, that have isolation level repeatable read or higher
	 * wait lsn creates a snapshot if called first in a block, which can
	 * lead the transaction to working incorrectly
	 */

	if (IsTransactionBlock() && XactIsoLevel != XACT_READ_COMMITTED) {
		ereport(WARNING,
				errmsg("Waitlsn may work incorrectly in this isolation level"),
				errhint("Call wait lsn before starting the transaction"));
	}

	endtime = GetNowFloat() + timeout_ms / 1000.0;

	latch_events = WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH;

	/* Check if we already reached the needed LSN */
	if (cur_lsn >= target_lsn)
		return true;

	AddWaitedLSN(target_lsn);

	for (;;)
	{
		int			rc;
		float8		time_left = 0;
		long		time_left_ms = 0;

		time_left = endtime - GetNowFloat();

		/* Use 100 ms as the default timeout to check for interrupts */
		if (wait_forever || time_left < 0 || time_left > 0.1)
			time_left_ms = 100;
		else
			time_left_ms = (long) ceil(time_left * 1000.0);

		/* If interrupt, LockErrorCleanup() will do DeleteWaitedLSN() for us */
		CHECK_FOR_INTERRUPTS();

		/* If postmaster dies, finish immediately */
		if (!PostmasterIsAlive())
			break;

		rc = WaitLatch(MyLatch, latch_events, time_left_ms,
					   WAIT_EVENT_CLIENT_READ);

		ResetLatch(MyLatch);

		if (rc & WL_LATCH_SET)
			cur_lsn = GetXLogReplayRecPtr(NULL);

		if (rc & WL_TIMEOUT)
		{
			cur_lsn = GetXLogReplayRecPtr(NULL);
			/* If the time specified by user has passed, stop waiting */
			time_left = endtime - GetNowFloat();
			if (!wait_forever && time_left <= 0.0)
				break;
		}

		/* If LSN has been replayed */
		if (target_lsn <= cur_lsn)
			break;
	}

	DeleteWaitedLSN();

	if (cur_lsn < target_lsn)
		ereport(WARNING,
				 errmsg("LSN was not reached"),
				 errhint("Try to increase wait time."));
	else
		res = true;

	return res;
}

Datum
pg_wait_lsn(PG_FUNCTION_ARGS)
{
	XLogRecPtr		trg_lsn = PG_GETARG_LSN(0);
	uint64_t		delay = PG_GETARG_INT32(1);

	PG_RETURN_BOOL(WaitUtility(trg_lsn, delay));
}
