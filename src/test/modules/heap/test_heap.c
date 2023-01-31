/*----------------------------------------------------------------------
 * test_heap.c
 *		Support test functions for the heap
 *
 * Copyright (c) 2014-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/heap/test_heap.c
 *----------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/subtrans.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/pmsignal.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

/*
 * Set the given XID in the current epoch to the next XID
 */
PG_FUNCTION_INFO_V1(set_next_xid);
Datum
set_next_xid(PG_FUNCTION_ARGS)
{
	TransactionId next_xid = PG_GETARG_TRANSACTIONID(0);
	uint32 epoch;

	if (!TransactionIdIsNormal(next_xid))
		elog(ERROR, "cannot set invalid transaction id");

	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);

	if (TransactionIdPrecedes(next_xid,
							  XidFromFullTransactionId(ShmemVariableCache->nextXid)))
	{
		LWLockRelease(XidGenLock);
		elog(ERROR, "cannot set transaction id older than the current transaction id");
	}

	/*
	 * If the new XID is past xidVacLimit, start trying to force autovacuum
	 * cycles.
	 */
	if (TransactionIdFollowsOrEquals(next_xid, ShmemVariableCache->xidVacLimit))
	{
		/* For safety, we release XidGenLock while sending signal */
		LWLockRelease(XidGenLock);
		SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	}

	/* Construct the new XID in the current epoch */
	epoch = EpochFromFullTransactionId(ShmemVariableCache->nextXid);
	ShmemVariableCache->nextXid =
		FullTransactionIdFromEpochAndXid(epoch, next_xid);

	ExtendCLOG(next_xid);
	ExtendCommitTs(next_xid);
	ExtendSUBTRANS(next_xid);

	LWLockRelease(XidGenLock);

	PG_RETURN_VOID();
}

