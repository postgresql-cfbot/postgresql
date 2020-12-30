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
typedef struct WALProhibitStateData
{
	/*
	 * Indicates current WAL prohibit state generation and the last two bits of
	 * this generation indicates current wal prohibit state.
	 */
	pg_atomic_uint32 shared_state_generation;

	/* Signaled when requested WAL prohibit state changes */
	ConditionVariable walprohibit_cv;
} WALProhibitStateData;

static WALProhibitStateData *WALProhibitState = NULL;

static void RequestWALProhibitChange(uint32 cur_state_gen);

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
		Assert(WALPROHIBIT_CURRENT_STATE(GetWALProhibitStateGen()) ==
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
		 * current subtransaction only. In the case of invalidations, that
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
 * AlterSystemSetWALProhibitState()
 *
 * Execute ALTER SYSTEM READ { ONLY | WRITE } statement.
 */
void
AlterSystemSetWALProhibitState(AlterSystemWALProhibitState *stmt)
{
	/* Check permission for pg_alter_wal_prohibit_state() */
	if (pg_proc_aclcheck(F_PG_ALTER_WAL_PROHIBIT_STATE,
						 GetUserId(), ACL_EXECUTE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for command ALTER SYSTEM"),
				 errhint("Get execute permission for pg_alter_wal_prohibit_state() to this user.")));

	/* Alter WAL prohibit state not allowed during recovery */
	PreventCommandDuringRecovery("ALTER SYSTEM");

	/* Execute function to alter wal prohibit state */
	(void) OidFunctionCall1(F_PG_ALTER_WAL_PROHIBIT_STATE,
							BoolGetDatum(stmt->walprohibited));
}

/*
 * pg_alter_wal_prohibit_state()
 *
 * SQL callable function to alter system read write state.
 */
Datum
pg_alter_wal_prohibit_state(PG_FUNCTION_ARGS)
{
	bool		walprohibited = PG_GETARG_BOOL(0);
	uint32		cur_state_gen;

	/* Alter WAL prohibit state not allowed during recovery */
	PreventCommandDuringRecovery("pg_alter_wal_prohibit_state()");

	/*
	 * It is not a final state since we yet to convey this WAL prohibit state to
	 * all backend.
	 */
	cur_state_gen = SetWALProhibitState(walprohibited, false);

	/* Server is already in requested state */
	if (!cur_state_gen)
		PG_RETURN_VOID();

	/*
	 * Signal the checkpointer to do the actual state transition, and wait for
	 * the state change to occur.
	 */
	RequestWALProhibitChange(cur_state_gen);

	PG_RETURN_VOID();
}

/*
 * RequestWALProhibitChange()
 *
 * Request checkpointer to make the WALProhibitState to read-only.
 */
static void
RequestWALProhibitChange(uint32 cur_state_gen)
{
	/* Must not be called from checkpointer */
	Assert(!AmCheckpointerProcess());
	Assert(GetWALProhibitStateGen() & WALPROHIBIT_TRANSITION_IN_PROGRESS);

	/*
	 * If in a standalone backend, just do it ourselves.
	 */
	if (!IsPostmasterEnvironment)
	{
		CompleteWALProhibitChange(cur_state_gen);
		return;
	}

	/* Signal checkpointer process */
	SendsSignalToCheckpointer(SIGINT);

	/* Wait for the state to change to read-only */
	ConditionVariablePrepareToSleep(&WALProhibitState->walprohibit_cv);
	for (;;)
	{
		/* We'll be done once wal prohibit state generation changes */
		if (GetWALProhibitStateGen() != cur_state_gen)
			break;

		ConditionVariableSleep(&WALProhibitState->walprohibit_cv,
							   WAIT_EVENT_WALPROHIBIT_STATE_CHANGE);
	}
	ConditionVariableCancelSleep();
}

/*
 * CompleteWALProhibitChange()
 *
 * Checkpointer will call this to complete the requested WAL prohibit state
 * transition.
 */
void
CompleteWALProhibitChange(uint32 cur_state_gen)
{
	uint64		barrier_gen;
	bool		wal_prohibited;

	/*
	 * Must be called from checkpointer. Otherwise, it must be single-user
	 * backend.
	 */
	Assert(AmCheckpointerProcess() || !IsPostmasterEnvironment);
	Assert(cur_state_gen & WALPROHIBIT_TRANSITION_IN_PROGRESS);

	/*
	 * WAL prohibit state change is initiated. We need to complete the state
	 * transition by setting requested WAL prohibit state in all backends.
	 */
	elog(DEBUG1, "waiting for backends to adopt requested WAL prohibit state change");

	/* Emit global barrier */
	barrier_gen = EmitProcSignalBarrier(PROCSIGNAL_BARRIER_WALPROHIBIT);
	WaitForProcSignalBarrier(barrier_gen);

	/* And flush all inserts. */
	XLogFlush(GetXLogInsertRecPtr());

	wal_prohibited =
		(WALPROHIBIT_NEXT_STATE(cur_state_gen) == WALPROHIBIT_STATE_READ_ONLY);

	/* Set the final state */
	(void) SetWALProhibitState(wal_prohibited, true);

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
		if (LastCheckPointIsSkipped())
		{
			RequestCheckpoint(CHECKPOINT_IMMEDIATE);
			SetLastCheckPointSkipped(false);
		}
		ereport(LOG, (errmsg("system is now read write")));
	}

	/* Wake up all backends waiting on this. */
	ConditionVariableBroadcast(&WALProhibitState->walprohibit_cv);
}

/*
 * GetWALProhibitStateGen()
 *
 * Atomically return the current server WAL prohibited state generation.
 */
uint32
GetWALProhibitStateGen(void)
{
	return pg_atomic_read_u32(&WALProhibitState->shared_state_generation);
}

/*
 * SetWALProhibitState()
 *
 * Increments current shared WAL prohibit state generation concerning to
 * requested state and returns the same.
 *
 * For the transition state request where is_final_state is false if the server
 * desired transition state is the same as the current state which might have
 * been requested by some other backend and has been proceeded then the current
 * wal prohibit generation will be returned so that this backend can wait until
 * the shared wal prohibited generation change for the final state.  And, if the
 * server is already completely moved to the requested state then the requester
 * backend doesn't need to wait, in that case, 0 will be returned.
 *
 * The final state can only be requested by the checkpointer or by the
 * single-user so that there will be no chance that the server is already in the
 * desired final state.
 */
uint32
SetWALProhibitState(bool wal_prohibited, bool is_final_state)
{
	uint32		new_state;
	uint32		cur_state;
	uint32		cur_state_gen;
	uint32		next_state_gen;

	/* Get the current state */
	cur_state_gen = GetWALProhibitStateGen();
	cur_state = WALPROHIBIT_CURRENT_STATE(cur_state_gen);

	/* Compute new state */
	if (is_final_state)
	{
		/*
		 * Only checkpointer or single-user can set the final wal prohibit
		 * state.
		 */
		Assert(AmCheckpointerProcess() || !IsPostmasterEnvironment);

		new_state = wal_prohibited ?
			WALPROHIBIT_STATE_READ_ONLY : WALPROHIBIT_STATE_READ_WRITE;

		/*
		 * There won't be any other process for the final state setting so that
		 * the next final state will be the desired state.
		 */
		Assert(WALPROHIBIT_NEXT_STATE(cur_state) == new_state);
	}
	else
	{
		new_state = wal_prohibited ?
			WALPROHIBIT_STATE_GOING_READ_ONLY :
			WALPROHIBIT_STATE_GOING_READ_WRITE;

		/* Server is already in the requested transition state */
		if (cur_state == new_state)
			return cur_state;		/* Wait for state transition completion */

		/* Server is already in requested state */
		if (WALPROHIBIT_NEXT_STATE(new_state) == cur_state)
			return 0;		/* No wait is needed */

		/* Prevent concurrent contrary in progress transition state setting */
		if (cur_state & WALPROHIBIT_TRANSITION_IN_PROGRESS)
		{
			if (cur_state & WALPROHIBIT_STATE_READ_ONLY)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("system state transition to read only is already in progress"),
						 errhint("Try after sometime again.")));
			else
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("system state transition to read write is already in progress"),
						 errhint("Try after sometime again.")));
		}
	}

	/*
	 * Update new state generation in share memory only if the state generation
	 * hasn't changed until now we have checked.
	 */
	next_state_gen = cur_state_gen + 1;
	(void) pg_atomic_compare_exchange_u32(&WALProhibitState->shared_state_generation,
										  &cur_state_gen, next_state_gen);

	/* To be sure that any later reads of memory happen strictly after this. */
	pg_memory_barrier();

	return next_state_gen;
}

/*
 * WALProhibitStateGenerationInit()
 *
 * Initialization of shared wal prohibit state generation.
 */
void
WALProhibitStateGenerationInit(bool wal_prohibited)
{
	uint32	new_state;

	Assert(AmStartupProcess() || !IsPostmasterEnvironment);

	new_state = wal_prohibited ?
		WALPROHIBIT_STATE_READ_ONLY : WALPROHIBIT_STATE_READ_WRITE;

	pg_atomic_init_u32(&WALProhibitState->shared_state_generation, new_state);
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

	WALProhibitState = (WALProhibitStateData *)
		ShmemInitStruct("WAL Prohibit State",
						sizeof(WALProhibitStateData),
						&found);

	if (found)
		return;

	/* First time through ... */
	memset(WALProhibitState, 0, sizeof(WALProhibitStateData));
	ConditionVariableInit(&WALProhibitState->walprohibit_cv);
}
