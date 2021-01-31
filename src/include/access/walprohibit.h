/*
 * walprohibit.h
 *
 * PostgreSQL write-ahead log prohibit states
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * src/include/access/walprohibit.h
 */
#ifndef WALPROHIBIT_H
#define WALPROHIBIT_H

#include "access/xact.h"
#include "access/xlog.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"

extern bool ProcessBarrierWALProhibit(void);
extern void MarkCheckPointSkippedInWalProhibitState(void);
extern void WALProhibitStateCounterInit(bool wal_prohibited);
extern void WALProhibitStateShmemInit(void);
extern bool IsWALProhibited(void);
extern void ProcessWALProhibitStateChangeRequest(void);

/*
 * WAL Prohibit States.
 *
 * There are four possible states. 	A brand new database cluster is always
 * initially WALPROHIBIT_STATE_READ_WRITE.  If the user tries to make it read
 * only, then we enter the state WALPROHIBIT_STATE_GOING_READ_ONLY.  When the
 * transition is complete, we enter the state WALPROHIBIT_STATE_READ_ONLY.  If
 * the user subsequently tries to make it read write, we will enter the state
 * WALPROHIBIT_STATE_GOING_READ_WRITE.  When that transition is complete, we
 * will enter the state WALPROHIBIT_STATE_READ_WRITE.  These four state
 * transitions are the only ones possible; for example, if we're currently in
 * state WALPROHIBIT_STATE_GOING_READ_ONLY, an attempt to go read-write will
 * produce an error, and a second attempt to go read-only will not cause a state
 * change.  Thus, we can represent the state as a shared-memory counter whose
 * value only ever changes by adding 1.  The initial value at postmaster startup
 * is either 0 or 2, depending on whether the control file specifies the system
 * is starting read-write or read-only.
 */
typedef enum
{
	WALPROHIBIT_STATE_READ_WRITE = 0,		/* WAL permitted */
	WALPROHIBIT_STATE_GOING_READ_ONLY = 1,
	WALPROHIBIT_STATE_READ_ONLY = 2,		/* WAL prohibited */
	WALPROHIBIT_STATE_GOING_READ_WRITE = 3
} WALProhibitState;

static inline WALProhibitState
GetWALProhibitState(uint32 wal_prohibit_counter)
{
	/* Extract last two bits */
	return (WALProhibitState) (wal_prohibit_counter & 3);
}

/* Never reaches when WAL is prohibited. */
static inline void
AssertWALPermitted(void)
{
	/*
	 * Recovery in the startup process never is in wal prohibited state.
	 */
	Assert(InRecovery || XLogInsertAllowed());

#ifdef USE_ASSERT_CHECKING
	walpermit_checked_state = WALPERMIT_CHECKED;
#endif
}

/*
 * XID-bearing transactions are killed off by "ALTER SYSTEM READ ONLY", so any
 * part of the code that can only be reached with an XID assigned is never
 * reached when WAL is prohibited.
 */
static inline void
AssertWALPermittedHaveXID(void)
{
	/* Must be performing an INSERT, UPDATE or DELETE, so we'll have an XID */
	Assert(FullTransactionIdIsValid(GetTopFullTransactionIdIfAny()));
	AssertWALPermitted();
}

/*
 * In opposite to the above assertion if a transaction doesn't have valid XID
 * (e.g. VACUUM) then it won't be killed while changing the system state to WAL
 * prohibited.  Therefore, we need to explicitly error out before entering into
 * the critical section.
 */
static inline void
CheckWALPermitted(void)
{
	if (!XLogInsertAllowed())
		ereport(ERROR,
				(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
				 errmsg("system is now read only")));

#ifdef USE_ASSERT_CHECKING
	walpermit_checked_state = WALPERMIT_CHECKED;
#endif
}

#endif							/* WALPROHIBIT_H */
