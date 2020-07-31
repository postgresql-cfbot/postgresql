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
#include "postmaster/bgwriter.h"
#include "storage/procsignal.h"

/*
 * Assert flag to enforce WAL insert permission check rule before starting a
 * critical section for the WAL writes.  For this, either of CheckWALPermitted,
 * AssertWALPermittedHaveXID, or AssertWALPermitted must be called before
 * starting the critical section.
 */
#ifdef USE_ASSERT_CHECKING
WALPermitCheckState walpermit_checked_state = WALPERMIT_UNCHECKED;
#endif

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
		Assert(GetWALProhibitState() & WALPROHIBIT_STATE_READ_ONLY);

		/*
		 * XXX: Kill off the whole session by throwing FATAL instead of killing
		 * transaction by throwing ERROR due to following reasons that need be
		 * thought:
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
				 errhint("Cannot continue a transaction if it has performed writes while system is read only.")));
	}

	/* Return to "check" state */
	ResetLocalXLogInsertAllowed();

	return true;
}

/*
 * AlterSystemSetWALProhibitState
 *
 * Execute ALTER SYSTEM READ { ONLY | WRITE } statement.
 */
void
AlterSystemSetWALProhibitState(AlterSystemWALProhibitState *stmt)
{
	uint32		state;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to execute ALTER SYSTEM command")));

	/* Alter WAL prohibit state not allowed during recovery */
	PreventCommandDuringRecovery("ALTER SYSTEM");

	/* Requested state */
	state = stmt->WALProhibited ?
		WALPROHIBIT_STATE_READ_ONLY : WALPROHIBIT_STATE_READ_WRITE;

	/*
	 * Since we yet to convey this WAL prohibit state to all backend mark it
	 * in-progress.
	 */
	state |= WALPROHIBIT_TRANSITION_IN_PROGRESS;

	if (!SetWALProhibitState(state))
		return; /* server is already in the desired state */

	/*
	 * Signal the checkpointer to do the actual state transition, and wait for
	 * the state change to occur.
	 */
	WALProhibitRequest();
}
