/*-------------------------------------------------------------------------
 *
 * test_sto.c
 *	  Functions to support isolation tests for snapshot too old.
 *
 * These functions are not intended for use in a production database and
 * could cause corruption.
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 *	  src/test/modules/snapshot_too_old/test_sto.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "storage/lwlock.h"
#include "utils/old_snapshot.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(test_sto_reset_all_state);
PG_FUNCTION_INFO_V1(test_sto_clobber_snapshot_timestamp);

/*
 * Revert to initial state.  This is not safe except in carefully
 * controlled tests.
 */
Datum
test_sto_reset_all_state(PG_FUNCTION_ARGS)
{

	LWLockAcquire(OldSnapshotTimeMapLock, LW_EXCLUSIVE);
	oldSnapshotControl->count_used = 0;
	oldSnapshotControl->current_timestamp = 0;
	oldSnapshotControl->head_offset = 0;
	oldSnapshotControl->head_timestamp = 0;
	LWLockRelease(OldSnapshotTimeMapLock);

	SpinLockAcquire(&oldSnapshotControl->mutex_latest_xmin);
	oldSnapshotControl->latest_xmin = InvalidTransactionId;
	oldSnapshotControl->next_map_update = 0;
	SpinLockRelease(&oldSnapshotControl->mutex_latest_xmin);

	SpinLockAcquire(&oldSnapshotControl->mutex_current);
	oldSnapshotControl->current_timestamp = 0;
	SpinLockRelease(&oldSnapshotControl->mutex_current);

	SpinLockAcquire(&oldSnapshotControl->mutex_threshold);
	oldSnapshotControl->threshold_timestamp = 0;
	oldSnapshotControl->threshold_xid = InvalidTransactionId;
	SpinLockRelease(&oldSnapshotControl->mutex_threshold);

	PG_RETURN_NULL();
}

/*
 * Update the minimum time used in snapshot-too-old code.  If set ahead of the
 * current wall clock time (for example, the year 3000), this allows testing
 * with arbitrary times.  This is not safe except in carefully controlled
 * tests.
 */
Datum
test_sto_clobber_snapshot_timestamp(PG_FUNCTION_ARGS)
{
	TimestampTz new_current_timestamp = PG_GETARG_TIMESTAMPTZ(0);

	SpinLockAcquire(&oldSnapshotControl->mutex_current);
	oldSnapshotControl->current_timestamp = new_current_timestamp;
	SpinLockRelease(&oldSnapshotControl->mutex_current);

	PG_RETURN_NULL();
}
