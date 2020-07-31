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

/* WAL Prohibit States */
#define	WALPROHIBIT_STATE_READ_WRITE		0x0000	/* WAL permitted */
#define	WALPROHIBIT_STATE_READ_ONLY			0x0001	/* WAL prohibited */

/*
 * The bit is used in state transition from one state to another.  When this
 * bit is set then the state indicated by the 0th position bit is yet to
 * confirmed.
 */
#define WALPROHIBIT_TRANSITION_IN_PROGRESS	0x0002

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

#endif		/* WALPROHIBIT_H */
