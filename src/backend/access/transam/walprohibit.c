/*-------------------------------------------------------------------------
 *
 * walprohibit.c
 * 		PostgreSQL write-ahead log prohibit states
 *
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * src/backend/access/transam/walprohibit.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/walprohibit.h"
#include "fmgr.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "storage/condition_variable.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "storage/latch.h"
#include "utils/acl.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"

/*
 * Assert flag to enforce WAL insert permission check rule before starting a
 * critical section for the WAL writes.  For this, either of
 * CheckWALPermitted(), AssertWALPermittedHaveXID(), or AssertWALPermitted()
 * must be called before starting the critical section.
 */
#ifdef USE_ASSERT_CHECKING
WALProhibitCheckState walprohibit_checked_state = WALPROHIBIT_UNCHECKED;
#endif

/*
 * Private state.
 */
static bool WALProhibitStateChangeIsInProgress;

/*
 * Shared-memory WAL prohibit state structure
 */
typedef struct WALProhibitData
{
	/*
	 * Indicates current WAL prohibit state counter and the last two bits of
	 * this counter indicates current wal prohibit state.
	 */
	pg_atomic_uint32 wal_prohibit_counter;

	/* Signaled when requested WAL prohibit state changes */
	ConditionVariable wal_prohibit_cv;
} WALProhibitData;

static WALProhibitData *WALProhibit = NULL;

static void CompleteWALProhibitChange(void);
static inline uint32 GetWALProhibitCounter(void);
static inline uint32 AdvanceWALProhibitStateCounter(void);

/*
 * ProcessBarrierWALProhibit()
 *
 *	Force a backend to take an appropriate action when system wide WAL prohibit
 *	state is changing.
 */
bool
ProcessBarrierWALProhibit(void)
{
	/*
	 * Kill off any transactions that have an XID *before* allowing the system
	 * to go WAL prohibit state.
	 */
	if (FullTransactionIdIsValid(GetTopFullTransactionIdIfAny()))
	{
		/*
		 * Should be here only while transiting towards the WAL prohibit
		 * state.
		 */
		Assert(GetWALProhibitState() == WALPROHIBIT_STATE_GOING_READ_ONLY);

		/*
		 * XXX: Kill off the whole session by throwing FATAL instead of
		 * killing transaction by throwing ERROR due to following reasons that
		 * need be thought:
		 *
		 * 1. Due to some presents challenges with the wire protocol, we could
		 * not simply kill of idle transaction.
		 *
		 * 2. If we are here in subtransaction then the ERROR will kill the
		 * current subtransaction only.  In the case of invalidations, that
		 * might be good enough, but for XID assignment it's not, because
		 * assigning an XID to a subtransaction also causes higher
		 * sub-transaction levels and the parent transaction to get XIDs.
		 */
		ereport(FATAL,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("WAL is now prohibited"),
				 errhint("Sessions with open write transactions must be terminated.")));
	}

	/* Return to "check" state */
	ResetLocalXLogInsertAllowed();

	return true;
}

/*
 * pg_prohibit_wal()
 *
 *	SQL callable function to toggle WAL prohibit state.
 *
 *	NB: Function always returns true that leaves scope for the future code
 *	changes might need to return false for some reason.
 */
Datum
pg_prohibit_wal(PG_FUNCTION_ARGS)
{
	bool		walprohibit = PG_GETARG_BOOL(0);
	uint32		wal_prohibit_counter;
	uint32		target_counter_value;

	/* WAL prohibit state changes not allowed during recovery. */
	PreventCommandDuringRecovery("pg_prohibit_wal()");

	/* For more detail on state transition, see comment for WALProhibitState */
	switch (GetWALProhibitState())
	{
		case WALPROHIBIT_STATE_READ_WRITE:
			if (!walprohibit)
				PG_RETURN_BOOL(true);	/* already in the requested state */
			break;

		case WALPROHIBIT_STATE_GOING_READ_WRITE:
			if (walprohibit)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("system state transition to WAL permission is already in progress"),
						 errhint("Try again after sometime.")));
			break;

		case WALPROHIBIT_STATE_READ_ONLY:
			if (walprohibit)
				PG_RETURN_BOOL(true);	/* already in the requested state */
			break;

		case WALPROHIBIT_STATE_GOING_READ_ONLY:
			if (!walprohibit)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("system state transition to WAL prohibition is already in progress"),
						 errhint("Try again after sometime.")));
			break;
	}

	wal_prohibit_counter = AdvanceWALProhibitStateCounter();
	target_counter_value = wal_prohibit_counter + 1;

#ifdef USE_ASSERT_CHECKING
	{
		/* Target state must be the requested one. */
		WALProhibitState target_state =
			CounterGetWALProhibitState(target_counter_value);

		Assert((walprohibit && target_state == WALPROHIBIT_STATE_READ_ONLY) ||
			   (!walprohibit && target_state == WALPROHIBIT_STATE_READ_WRITE));
	}
#endif

	/*
	 * If in a standalone backend, just do it ourselves.
	 */
	if (!IsPostmasterEnvironment)
	{
		ProcessWALProhibitStateChangeRequest();
		PG_RETURN_BOOL(true);
	}

	/*
	 * It is not a final state since we yet to convey this WAL prohibit state
	 * to all backend.  Checkpointer will do that and update the shared memory
	 * wal prohibit state counter and control file.
	 */
	if (!SendSignalToCheckpointer(SIGUSR1))
	{
		ereport(WARNING,
				(errmsg("could not change system state now"),
				 errdetail("Checkpointer might not be running."),
				 errhint("The relaunched checkpointer process will automatically complete the system state change.")));
		PG_RETURN_BOOL(true);		/* no wait */
	}

	/* Wait for the state counter in shared memory to change. */
	ConditionVariablePrepareToSleep(&WALProhibit->wal_prohibit_cv);

	/*
	 * We'll be done once the wal prohibit state counter reaches to target
	 * value.
	 */
	while (GetWALProhibitCounter() < target_counter_value)
		ConditionVariableSleep(&WALProhibit->wal_prohibit_cv,
							   WAIT_EVENT_WALPROHIBIT_STATE_CHANGE);
	ConditionVariableCancelSleep();

	PG_RETURN_BOOL(true);
}

/*
 * IsWALProhibited()
 *
 *	Is the system still in WAL prohibited state?
 */
bool
IsWALProhibited(void)
{
	/* Other than read-write state will be considered as read-only */
	return (GetWALProhibitState() != WALPROHIBIT_STATE_READ_WRITE);
}

/*
 * CompleteWALProhibitChange()
 *
 *	Complete WAL prohibit state transition.
 *
 *	Based on the final WAL prohibited state to be transit, the in-memory state
 *	update decided to do before or after emitting global barrier.
 *
 *	The idea behind this is that when we say the system is WAL prohibited, then
 *	WAL writes in all the backend should be prohibited, but when the system is
 *	no longer WAL prohibited, then it is not necessary to take out all backend
 *	from WAL prohibited state.  No harm if we let those backend run as read-only
 *	for some more time until we emit the barrier since those might have
 *	connected when the system was in WAL prohibited state and might doing a
 *	read-only operation. Those who might connect now onward can immediately
 *	start read-write operations.
 *
 *	Therefore, while moving the system to WAL is no longer prohibited, then set
 *	update system state immediately and emit barrier later. But, while moving
 *	the system to WAL prohibited then we emit the global barrier first to ensure
 *	that no backend do the WAL writes before we set system state to WAL
 *	prohibited.
 */
static void
CompleteWALProhibitChange(void)
{
	uint64		barrier_gen;
	bool		wal_prohibited;

	/* Fetch shared wal prohibit state */
	WALProhibitState cur_state = GetWALProhibitState();

	/* Should be here only in transition state */
	if (cur_state == WALPROHIBIT_STATE_READ_WRITE ||
		cur_state == WALPROHIBIT_STATE_READ_ONLY)
		return;

	wal_prohibited = (cur_state == WALPROHIBIT_STATE_GOING_READ_ONLY);

	/*
	 * If the server is started in wal prohibited state then the
	 * required wal write operation in the startup process to
	 * start the server normally has been skipped, if it is, then
	 * does that right away.
	 */
	if (!wal_prohibited &&
		GetXLogWriteAllowedState() != XLOG_ACCEPT_WRITES_DONE)
		PerformPendingXLogAcceptWrites();

	/*
	 * Update control file to make the state persistent.
	 *
	 * Once wal prohibit state transition set then that needs to be completed.
	 * If the server crashes before the state completion, then the control file
	 * information will be used to set the final wal prohibit state on restart.
	 */
	SetControlFileWALProhibitFlag(wal_prohibited);

	/* Going out of WAL prohibited state then update state right away. */
	if (!wal_prohibited)
	{
		uint32		wal_prohibit_counter PG_USED_FOR_ASSERTS_ONLY;

		/* The operation to allow wal writes should be done by now  */
		Assert(GetXLogWriteAllowedState() == XLOG_ACCEPT_WRITES_DONE);

		wal_prohibit_counter = AdvanceWALProhibitStateCounter();

		/*
		 * Should have set counter for the final state where wal is no longer
		 * prohibited.
		 */
		Assert(CounterGetWALProhibitState(wal_prohibit_counter) ==
			   WALPROHIBIT_STATE_READ_WRITE);
	}

	/*
	 * WAL prohibit state change is initiated.  We need to complete the state
	 * transition by setting requested WAL prohibit state in all backends.
	 */
	elog(DEBUG1, "waiting for backends to adopt requested WAL prohibit state change");

	/* Emit global barrier */
	barrier_gen = EmitProcSignalBarrier(PROCSIGNAL_BARRIER_WALPROHIBIT);
	WaitForProcSignalBarrier(barrier_gen);

	/*
	 * Don't need to be too aggressive to flush XLOG data right away since
	 * XLogFlush is not restricted in the wal prohibited state as well.
	 */
	XLogFlush(GetXLogWriteRecPtr());

	/*
	 * Increment wal prohibit state counter in share memory once the barrier has
	 * been processed by all the backend that ensures that all backends are in
	 * wal prohibited state.
	 */
	if (wal_prohibited)
	{
		uint32		wal_prohibit_counter PG_USED_FOR_ASSERTS_ONLY;

		wal_prohibit_counter = AdvanceWALProhibitStateCounter();

		/* Should have set counter for the final wal prohibited state */
		Assert(CounterGetWALProhibitState(wal_prohibit_counter) ==
			   WALPROHIBIT_STATE_READ_ONLY);
	}

	if (wal_prohibited)
		ereport(LOG, (errmsg("WAL is now prohibited")));
	else
		ereport(LOG, (errmsg("WAL is no longer prohibited")));

	/* Wake up the backend waiting on this. */
	ConditionVariableBroadcast(&WALProhibit->wal_prohibit_cv);
}

/*
 * AdvanceWALProhibitStateCounter()
 *
 *	Increment wal prohibit counter by 1.
 */
static inline uint32
AdvanceWALProhibitStateCounter(void)
{
	return pg_atomic_add_fetch_u32(&WALProhibit->wal_prohibit_counter, 1);
}

/*
 * ProcessWALProhibitStateChangeRequest()
 */
void
ProcessWALProhibitStateChangeRequest(void)
{
	/*
	 * Must be called by the checkpointer process or single-user backend.
	 */
	if (!(AmCheckpointerProcess() || !IsPostmasterEnvironment))
		return;

	/*
	 * Quick exit if the state transition is already in progress to avoid a
	 * recursive call to process wal prohibit state transition in some case e.g.
	 * the end-of-recovery checkpoint.
	 */
	if (WALProhibitStateChangeIsInProgress)
		return;

	WALProhibitStateChangeIsInProgress = true;

	PG_TRY();
	{
		CompleteWALProhibitChange();
	}
	PG_FINALLY();
	{
		WALProhibitStateChangeIsInProgress = false;
	}
	PG_END_TRY();
}

/*
 * GetWALProhibitCounter()
 */
static inline uint32
GetWALProhibitCounter(void)
{
	return pg_atomic_read_u32(&WALProhibit->wal_prohibit_counter);
}

/*
 * GetWALProhibitState()
 */
WALProhibitState
GetWALProhibitState(void)
{
	return CounterGetWALProhibitState(GetWALProhibitCounter());
}

/*
 * WALProhibitStateCounterInit()
 *
 * Initialization of shared wal prohibit state counter.
 */
void
WALProhibitStateCounterInit(bool wal_prohibited)
{
	WALProhibitState new_state;

	Assert(AmStartupProcess() || !IsPostmasterEnvironment);

	new_state = wal_prohibited ?
		WALPROHIBIT_STATE_READ_ONLY : WALPROHIBIT_STATE_READ_WRITE;

	pg_atomic_init_u32(&WALProhibit->wal_prohibit_counter, (uint32) new_state);
}

/*
 * WALProhibitStateShmemInit()
 *
 *	Initialization of shared memory for WAL prohibit state.
 */
void
WALProhibitStateShmemInit(void)
{
	bool		found;

	WALProhibit = (WALProhibitData *)
		ShmemInitStruct("WAL Prohibit State",
						sizeof(WALProhibitData),
						&found);

	if (!found)
	{
		/* First time through ... */
		memset(WALProhibit, 0, sizeof(WALProhibitData));
		ConditionVariableInit(&WALProhibit->wal_prohibit_cv);
	}
}
