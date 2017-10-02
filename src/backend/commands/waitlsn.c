/*-------------------------------------------------------------------------
 *
 * waitlsn.c
 *	  WaitLSN statment: WAITLSN
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 2017, Regents of PostgresPro
 *
 * IDENTIFICATION
 *	  src/backend/commands/waitlsn.c
 *
 *-------------------------------------------------------------------------
 */

/*
 * -------------------------------------------------------------------------
 * Wait for LSN been replayed on slave
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "pgstat.h"
#include "utils/pg_lsn.h"
#include "storage/latch.h"
#include "miscadmin.h"
#include "storage/spin.h"
#include "storage/backendid.h"
#include "access/xact.h"
#include "storage/shmem.h"
#include "storage/ipc.h"
#include "utils/timestamp.h"
#include "storage/pmsignal.h"
#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "commands/waitlsn.h"
#include "storage/proc.h"
#include "access/transam.h"
#include "funcapi.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"

/* Latches Own-DisownLatch and AbortCаllBack */
static uint32 WaitLSNShmemSize(void);
static void WLDisownLatchAbort(XactEvent event, void *arg);
static void WLOwnLatch(XLogRecPtr trg_lsn);
static void WLDisownLatch(void);

void		_PG_init(void);

/* Shared memory structures */
typedef struct
{
	int					pid;
	volatile slock_t	slock;
	Latch				latch;
	XLogRecPtr			trg_lsn;
} BIDLatch;

typedef struct
{
	char			dummy;			// УБРАТЬ
	int				backend_maxid;
	XLogRecPtr		min_lsn;
	BIDLatch		l_arr[FLEXIBLE_ARRAY_MEMBER];
} GlobState;

static volatile GlobState  *state;
bool						is_latch_owned = false;

/* Take Latch for current backend at the begining of WAITLSN */
static void
WLOwnLatch(XLogRecPtr trg_lsn)
{
	SpinLockAcquire(&state->l_arr[MyBackendId].slock);
	OwnLatch(&state->l_arr[MyBackendId].latch);
	is_latch_owned = true;

	if (state->backend_maxid < MyBackendId)
		state->backend_maxid = MyBackendId;

	state->l_arr[MyBackendId].pid = MyProcPid;
	state->l_arr[MyBackendId].trg_lsn = trg_lsn;
	SpinLockRelease(&state->l_arr[MyBackendId].slock);

	if (trg_lsn < state->min_lsn)
		state->min_lsn = trg_lsn;
}

/* Release Latch for current backend at the end of WAITLSN */
static void
WLDisownLatch(void)
{
	int i;
	XLogRecPtr trg_lsn = state->l_arr[MyBackendId].trg_lsn;

	SpinLockAcquire(&state->l_arr[MyBackendId].slock);
	DisownLatch(&state->l_arr[MyBackendId].latch);
	is_latch_owned = false;
	state->l_arr[MyBackendId].pid = 0;
	state->l_arr[MyBackendId].trg_lsn = InvalidXLogRecPtr;

	/* Update state->min_lsn iff it is nessesary choosing next min_lsn */
	if (state->min_lsn == trg_lsn)
	{
		state->min_lsn = PG_UINT64_MAX;
		for (i = 2; i <= state->backend_maxid; i++)
			if (state->l_arr[i].trg_lsn != InvalidXLogRecPtr &&
				state->l_arr[i].trg_lsn < state->min_lsn)
				state->min_lsn = state->l_arr[i].trg_lsn;
	}

	if (state->backend_maxid == MyBackendId)
		for (i = (MaxConnections+1); i >=2; i--)
			if (state->l_arr[i].pid != 0)
			{
				state->backend_maxid = i;
				break;
			}

	SpinLockRelease(&state->l_arr[MyBackendId].slock);
}

/* CallBack function on abort*/
static void
WLDisownLatchAbort(XactEvent event, void *arg)
{
	if (is_latch_owned && (event == XACT_EVENT_PARALLEL_ABORT ||
						   event == XACT_EVENT_ABORT))
	{
		WLDisownLatch();
	}
}

/* Module load callback */
void
_PG_init(void)
{
	if (!IsUnderPostmaster)
		RegisterXactCallback(WLDisownLatchAbort, NULL);
}

/* Get size of shared memory to room GlobState */
static uint32
WaitLSNShmemSize(void)
{
	return offsetof(GlobState, l_arr) + sizeof(BIDLatch) * (MaxConnections+1);
}

/* Init array of Latches in shared memory */
void
WaitLSNShmemInit(void)
{
	bool	found;
	uint	i;

	state = (GlobState *) ShmemInitStruct("pg_wait_lsn",
										  WaitLSNShmemSize(),
										  &found);
	if (!found)
	{
		for (i = 0; i < (MaxConnections+1); i++)
		{
			state->l_arr[i].pid = 0;
			state->l_arr[i].trg_lsn = InvalidXLogRecPtr;
			SpinLockInit(&state->l_arr[i].slock);
			InitSharedLatch(&state->l_arr[i].latch);
		}
		state->backend_maxid = 0;
		state->min_lsn = PG_UINT64_MAX;
	}
}

/* Set all Latches in shared memory cause new LSN been replayed*/
void
WaitLSNSetLatch(XLogRecPtr cur_lsn)
{
	uint i;

	for (i = 2; i <= state->backend_maxid; i++)
	{
		if (state->l_arr[i].trg_lsn != 0)
		{
		SpinLockAcquire(&state->l_arr[i].slock);
		if (state->l_arr[i].trg_lsn <= cur_lsn)
			SetLatch(&state->l_arr[i].latch);
		SpinLockRelease(&state->l_arr[i].slock);
		}
	}
}

/* Get minimal LSN that will be next */
XLogRecPtr
GetMinWaitLSN(void)
{
	return state->min_lsn;
}

/*
 * On WAITLSN own latch and wait till LSN is replayed, Postmaster death, interruption
 * or timeout.
 */
void
WaitLSNUtility(const char *lsn, const int delay, DestReceiver *dest)
{
	XLogRecPtr		trg_lsn = DatumGetLSN(DirectFunctionCall1(pg_lsn_in, CStringGetDatum(lsn)));
	XLogRecPtr		cur_lsn;
	int				latch_events;
	uint64_t		tdelay = delay;
	long			secs;
	int				microsecs;
	TimestampTz		timer = GetCurrentTimestamp();
	TupOutputState	*tstate;
	TupleDesc		tupdesc;
	char		   *value = "false";

	if (delay > 0)
		latch_events = WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH;
	else
		latch_events = WL_LATCH_SET | WL_POSTMASTER_DEATH;

	WLOwnLatch(trg_lsn);

	for (;;)
	{
		cur_lsn = GetXLogReplayRecPtr(NULL);

		/* If LSN had been Replayed */
		if (trg_lsn <= cur_lsn)
			break;

		/* If the postmaster dies, finish immediately */
		if (!PostmasterIsAlive())
			break;

		/* If Delay time is over */
		if (latch_events & WL_TIMEOUT)
		{
			if (TimestampDifferenceExceeds(timer,GetCurrentTimestamp(),delay))
				break;
			TimestampDifference(timer,GetCurrentTimestamp(),&secs, &microsecs);
			tdelay = delay - (secs*1000 + microsecs/1000);
		}

		MyPgXact->xmin = InvalidTransactionId;
		WaitLatch(&state->l_arr[MyBackendId].latch, latch_events, tdelay, WAIT_EVENT_CLIENT_READ);
		ResetLatch(&state->l_arr[MyBackendId].latch);

		/* CHECK_FOR_INTERRUPTS if they comes then disown latch current */
		if (InterruptPending)
		{
			WLDisownLatch();
			ProcessInterrupts();
		}

	}

	WLDisownLatch();

	if (trg_lsn > cur_lsn)
		elog(NOTICE,"LSN is not reached. Try to make bigger delay.");
	else
		value = "true";

	/* need a tuple descriptor representing a single TEXT column */
	tupdesc = CreateTemplateTupleDesc(1, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "LSN reached", TEXTOID, -1, 0);
	/* prepare for projection of tuples */
	tstate = begin_tup_output_tupdesc(dest, tupdesc);
	/* Send it */
	do_text_output_oneline(tstate, value);
	end_tup_output(tstate);
}
