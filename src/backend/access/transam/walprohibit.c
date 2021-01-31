/*-------------------------------------------------------------------------
 *
 * walprohibit.c
 * 		PostgreSQL write-ahead log prohibit states
 *
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
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
WALPermitCheckState walpermit_checked_state = WALPERMIT_UNCHECKED;
#endif

/*
 * Shared-memory WAL prohibit state
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
static uint32 GetWALProhibitCounter(void);

/*
 * ProcessBarrierWALProhibit()
 *
 * Handle WAL prohibit state change request.
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
		 * Should be here only while transiting towards the WAL prohibit state.
		 */
		Assert(GetWALProhibitState(GetWALProhibitCounter()) ==
			   WALPROHIBIT_STATE_GOING_READ_ONLY);

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
				 errmsg("system is now read only"),
				 errhint("Sessions with open write transactions must be terminated.")));
	}

	/* Return to "check" state */
	ResetLocalXLogInsertAllowed();

	return true;
}

/*
 * pg_prohibit_wal()
 *
 * SQL callable function to toggle WAL prohibit state.
 */
Datum
pg_prohibit_wal(PG_FUNCTION_ARGS)
{
	bool		walprohibit = PG_GETARG_BOOL(0);
	uint32		wal_prohibit_counter;
	uint32		target_counter_value;
	bool		increment;

	/* WAL prohibit state changes not allowed during recovery */
	PreventCommandDuringRecovery("pg_prohibit_wal()");

	wal_prohibit_counter = GetWALProhibitCounter();

	/* For more detail on state transition, see comment for WALProhibitState */
	switch (GetWALProhibitState(wal_prohibit_counter))
	{
		case WALPROHIBIT_STATE_READ_WRITE:
			if (!walprohibit)
				PG_RETURN_VOID();		/* already in the requested state */
			increment = true;
			break;

		case WALPROHIBIT_STATE_GOING_READ_WRITE:
			if (walprohibit)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("system state transition to read write is already in progress"),
						 errhint("Try after sometime again.")));
			increment = false;
			break;

		case WALPROHIBIT_STATE_READ_ONLY:
			if (walprohibit)
				PG_RETURN_VOID();		/* already in the requested state */
			increment = true;
			break;

		case WALPROHIBIT_STATE_GOING_READ_ONLY:
			if (!walprohibit)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("system state transition to read only is already in progress"),
						 errhint("Try after sometime again.")));
			increment = false;
			break;
	}

	if (increment)
		wal_prohibit_counter =
			pg_atomic_add_fetch_u32(&WALProhibit->wal_prohibit_counter, 1);
	target_counter_value = wal_prohibit_counter + 1;

#ifdef USE_ASSERT_CHECKING
	{
		/* Target state must be the requested one. */
		WALProhibitState target_state = GetWALProhibitState(target_counter_value);
		Assert((walprohibit && target_state == WALPROHIBIT_STATE_READ_ONLY) ||
			   (!walprohibit && target_state == WALPROHIBIT_STATE_READ_WRITE));
	}
#endif

	/*
	 * If in a standalone backend, just do it ourselves.
	 */
	if (!IsPostmasterEnvironment)
	{
		CompleteWALProhibitChange();
		PG_RETURN_VOID();
	}

	/*
	 * It is not a final state since we yet to convey this WAL prohibit state to
	 * all backend.  Signal the checkpointer to do that and update the shared
	 * memory wal prohibit state counter.
	 */
	if (!SendSignalToCheckpointer(SIGUSR1))
	{
		ereport(WARNING,
				(errmsg("could not change system state now"),
				 errdetail("Checkpointer might not running."),
				 errhint("The relaunched checkpointer process will automatically complete the system state change.")));
		PG_RETURN_VOID();	/* no wait */
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

	PG_RETURN_VOID();
}

/*
 * Is the system still in WAL prohibited state?
 */
bool
IsWALProhibited(void)
{
	WALProhibitState cur_state = GetWALProhibitState(GetWALProhibitCounter());

	return (cur_state != WALPROHIBIT_STATE_READ_WRITE &&
			cur_state != WALPROHIBIT_STATE_GOING_READ_WRITE);
}

/*
 * CompleteWALProhibitChange()
 *
 * Complete the requested WAL prohibit state transition.
 */
static void
CompleteWALProhibitChange(void)
{
	uint64		barrier_gen;
	bool		wal_prohibited;

	/* Fetch shared wal prohibit state counter value */
	uint32		wal_prohibit_counter = GetWALProhibitCounter();
	WALProhibitState cur_state = GetWALProhibitState(wal_prohibit_counter);

	/*
	 * Must be called from checkpointer.  Otherwise, it must be single-user
	 * backend.
	 */
	Assert(AmCheckpointerProcess() || !IsPostmasterEnvironment);

	/* Should be here in transition state */
	Assert(cur_state == WALPROHIBIT_STATE_GOING_READ_ONLY ||
		   cur_state == WALPROHIBIT_STATE_GOING_READ_WRITE);

	/*
	 * WAL prohibit state change is initiated.  We need to complete the state
	 * transition by setting requested WAL prohibit state in all backends.
	 */
	elog(DEBUG1, "waiting for backends to adopt requested WAL prohibit state change");

	/* Emit global barrier */
	barrier_gen = EmitProcSignalBarrier(PROCSIGNAL_BARRIER_WALPROHIBIT);
	WaitForProcSignalBarrier(barrier_gen);

	/* Return to "check" state  */
	ResetLocalXLogInsertAllowed();

	/*
	 * We don't need to be too aggressive to flush XLOG data right away since
	 * XLogFlush is not restricted in the wal prohibited state as well.
	 */
	XLogFlush(GetXLogWriteRecPtr());

	/*
	 * There won't be any other process for the final state transition so that
	 * the shared wal prohibit state counter shouldn't have been changed by now.
	 */
	Assert(GetWALProhibitCounter() == wal_prohibit_counter);

	/* Increment wal prohibit state counter in share memory. */
	wal_prohibit_counter =
		pg_atomic_add_fetch_u32(&WALProhibit->wal_prohibit_counter, 1);

	/* Should have set counter for the final state */
	cur_state = GetWALProhibitState(wal_prohibit_counter);
	Assert(cur_state == WALPROHIBIT_STATE_READ_ONLY ||
		   cur_state == WALPROHIBIT_STATE_READ_WRITE);

	wal_prohibited = (cur_state == WALPROHIBIT_STATE_READ_ONLY);

	/* Update the control file to make state persistent */
	SetControlFileWALProhibitFlag(wal_prohibited);

	if (wal_prohibited)
		ereport(LOG, (errmsg("system is now read only")));
	else
	{
		/*
		 * Request checkpoint if the end-of-recovery checkpoint has been skipped
		 * previously.
		 */
		if (RecoveryCheckpointIsSkipped())
		{
			RequestCheckpoint(CHECKPOINT_IMMEDIATE);
			SetRecoveryCheckpointSkippedFlag(false);
		}
		ereport(LOG, (errmsg("system is now read write")));
	}

	/* Wake up all backends waiting on this. */
	ConditionVariableBroadcast(&WALProhibit->wal_prohibit_cv);
}

/*
 * ProcessWALProhibitStateChangeRequest()
 *
 * Checkpointer will complete wal prohibit state change request.
 */
void
ProcessWALProhibitStateChangeRequest(void)
{
	WALProhibitState cur_state;

	/*
	 * Must be called by the checkpointer process.  Checkpointer has to be sure
	 * it has processed all pending wal prohibit state change requests as soon
	 * as possible.  Since CreateCheckPoint and ProcessSyncRequests sometimes
	 * runs in non-checkpointer processes, do nothing if not checkpointer.
	 */
	if (!AmCheckpointerProcess())
		return;

	cur_state = GetWALProhibitState(GetWALProhibitCounter());

	while (cur_state != WALPROHIBIT_STATE_READ_WRITE)
	{
		if (cur_state == WALPROHIBIT_STATE_GOING_READ_ONLY ||
			cur_state == WALPROHIBIT_STATE_GOING_READ_WRITE)
		{
			CompleteWALProhibitChange();
		}
		else if (cur_state == WALPROHIBIT_STATE_READ_ONLY)
		{
			int			rc;

			/*
			 * Don't let Checkpointer process do anything until someone wakes it
			 * up.  For example a backend might later on request us to put the
			 * system back to read-write state.
			 */
			rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1,
						   WAIT_EVENT_WALPROHIBIT_STATE);

			/*
			 * If the postmaster dies or a shutdown request is received, just
			 * bail out.
			 */
			if (rc & WL_POSTMASTER_DEATH || ShutdownRequestPending)
				return;
		}

		/* Get the latest state */
		cur_state = GetWALProhibitState(GetWALProhibitCounter());
	}
}

/*
 * GetWALProhibitCounter()
 *
 * Atomically return the current server WAL prohibited state counter.
 */
static uint32
GetWALProhibitCounter(void)
{
	return pg_atomic_read_u32(&WALProhibit->wal_prohibit_counter);
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
 * Initialization of shared memory for WAL prohibit state.
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
