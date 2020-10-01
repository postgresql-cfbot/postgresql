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
extern void AlterSystemSetWALProhibitState(AlterSystemWALProhibitState *stmt);
extern void CompleteWALProhibitChange(uint32 wal_state);
extern uint32 GetWALProhibitStateGen(void);
extern uint32 SetWALProhibitState(bool wal_prohibited, bool is_final_state);
extern void MarkCheckPointSkippedInWalProhibitState(void);
extern void WALProhibitStateGenerationInit(bool wal_prohibited);
extern void WALProhibitStateShmemInit(void);

/*
 * The WAL Prohibit States.
 *
 * 	The odd number represents the transition state and whereas the even number
 * 	represents the final state.  These states can be distinguished by checking
 * 	the 0th bits aka transition bit.
 */
#define	WALPROHIBIT_STATE_READ_WRITE		(uint32) 0	/* WAL permitted */
#define	WALPROHIBIT_STATE_GOING_READ_ONLY	(uint32) 1
#define	WALPROHIBIT_STATE_READ_ONLY			(uint32) 2	/* WAL prohibited */
#define	WALPROHIBIT_STATE_GOING_READ_WRITE	(uint32) 3

/* The transition bit to distinguish states.  */
#define	WALPROHIBIT_TRANSITION_IN_PROGRESS	((uint32) 1 << 0)

/* Extract last two bits */
#define	WALPROHIBIT_CURRENT_STATE(stateGeneration)	\
	((uint32)(stateGeneration) & ((uint32) ((1 << 2) - 1)))
#define	WALPROHIBIT_NEXT_STATE(stateGeneration)	\
	WALPROHIBIT_CURRENT_STATE((stateGeneration + 1))

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
