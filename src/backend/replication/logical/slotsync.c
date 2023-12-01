/*-------------------------------------------------------------------------
 * slotsync.c
 *	   PostgreSQL worker for synchronizing slots to a standby server from the
 *         primary server.
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/slotsync.c
 *
 * This file contains the code for slot sync worker on a physical standby
 * to fetch logical failover slots information from the primary server,
 * create the slots on the standby and synchronize them periodically.
 *
 * It also takes care of dropping the slots which were created by it and are
 * currently not needed to be synchronized.
 *
 * It takes a nap of WORKER_DEFAULT_NAPTIME_MS before every next
 * synchronization. If there is no activity observed on the primary server for
 * some time, the nap time is increased to WORKER_INACTIVITY_NAPTIME_MS, but if
 * any activity is observed, the nap time reverts to the default value.
 *---------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/table.h"
#include "access/xlogrecovery.h"
#include "catalog/pg_database.h"
#include "commands/dbcommands.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "replication/logical.h"
#include "replication/logicallauncher.h"
#include "replication/logicalworker.h"
#include "replication/walreceiver.h"
#include "replication/worker_internal.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc_hooks.h"
#include "utils/pg_lsn.h"
#include "utils/varlena.h"

/*
 * Structure to hold information fetched from the primary server about a logical
 * replication slot.
 */
typedef struct RemoteSlot
{
	char	   *name;
	char	   *plugin;
	char	   *database;
	bool		two_phase;
	bool		failover;
	XLogRecPtr	restart_lsn;
	XLogRecPtr	confirmed_lsn;
	TransactionId catalog_xmin;

	/* RS_INVAL_NONE if valid, or the reason of invalidation */
	ReplicationSlotInvalidationCause invalidated;
} RemoteSlot;

/*
 * Struct for sharing information between startup process and slot
 * sync worker.
 *
 * Slot sync worker's pid is needed by startup process in order to
 * shut it down during promotion.
 */
typedef struct SlotSyncWorkerCtx
{
	pid_t		pid;
	slock_t		mutex;
} SlotSyncWorkerCtx;

SlotSyncWorkerCtx *SlotSyncWorker = NULL;

/* GUC variable */
bool		enable_syncslot = false;

/* The last sync-cycle time when the worker updated any of the slots. */
static TimestampTz last_update_time;

/* Worker's nap time in case of regular activity on the primary server */
#define WORKER_DEFAULT_NAPTIME_MS                   10L /* 10 ms */

/* Worker's nap time in case of no-activity on the primary server */
#define WORKER_INACTIVITY_NAPTIME_MS                10000L	/* 10 sec */

/*
 * Inactivity Threshold in ms before increasing nap time of worker.
 *
 * If the lsn of slot being monitored did not change for this threshold time,
 * then increase nap time of current worker from WORKER_DEFAULT_NAPTIME_MS to
 * WORKER_INACTIVITY_NAPTIME_MS.
 */
#define WORKER_INACTIVITY_THRESHOLD_MS 10000L	/* 10 sec */

/*
 * Number of attempts for wait_for_primary_slot_catchup() after
 * which it aborts the wait and the slot sync worker then moves
 * to the next slot creation/sync.
 */
#define WORKER_PRIMARY_CATCHUP_WAIT_ATTEMPTS 5

static void ProcessSlotSyncInterrupts(WalReceiverConn **wrconn,
									  List **standby_slots);


/*
 * Wait for remote slot to pass locally reserved position.
 *
 * Ping and wait for the primary server for
 * WORKER_PRIMARY_CATCHUP_WAIT_ATTEMPTS during a slot creation, if it still
 * does not catch up, abort the wait. The ones for which wait is aborted will
 * attempt the wait and sync in the next sync-cycle.
 *
 * *persist will be set to false if the slot has disappeared or was invalidated
 * on the primary; otherwise, it will be set to true.
 */
static bool
wait_for_primary_slot_catchup(WalReceiverConn *wrconn, RemoteSlot *remote_slot,
							  bool *persist)
{
#define WAIT_OUTPUT_COLUMN_COUNT 4
	StringInfoData cmd;
	int			wait_count = 0;

	if (persist)
		*persist = true;

	ereport(LOG,
			errmsg("waiting for remote slot \"%s\" LSN (%X/%X) and catalog xmin"
				   " (%u) to pass local slot LSN (%X/%X) and catalog xmin (%u)",
				   remote_slot->name,
				   LSN_FORMAT_ARGS(remote_slot->restart_lsn),
				   remote_slot->catalog_xmin,
				   LSN_FORMAT_ARGS(MyReplicationSlot->data.restart_lsn),
				   MyReplicationSlot->data.catalog_xmin));

	initStringInfo(&cmd);
	appendStringInfo(&cmd,
					 "SELECT conflicting, restart_lsn, confirmed_flush_lsn,"
					 " catalog_xmin FROM pg_catalog.pg_replication_slots"
					 " WHERE slot_name = %s",
					 quote_literal_cstr(remote_slot->name));

	for (;;)
	{
		XLogRecPtr	new_invalidated;
		XLogRecPtr	new_restart_lsn;
		XLogRecPtr	new_confirmed_lsn;
		TransactionId new_catalog_xmin;
		WalRcvExecResult *res;
		TupleTableSlot *slot;
		int			rc;
		bool		isnull;
		Oid			slotRow[WAIT_OUTPUT_COLUMN_COUNT] = {BOOLOID, LSNOID, LSNOID,
		XIDOID};

		CHECK_FOR_INTERRUPTS();

		/* Handle any termination request if any */
		ProcessSlotSyncInterrupts(&wrconn, NULL /* standby_slots */ );

		res = walrcv_exec(wrconn, cmd.data, WAIT_OUTPUT_COLUMN_COUNT, slotRow);

		if (res->status != WALRCV_OK_TUPLES)
			ereport(ERROR,
					(errmsg("could not fetch slot info for slot \"%s\" from the"
							" primary server: %s",
							remote_slot->name, res->err)));

		slot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
		if (!tuplestore_gettupleslot(res->tuplestore, true, false, slot))
		{
			ereport(WARNING,
					(errmsg("slot \"%s\" disappeared from the primary server,"
							" slot creation aborted", remote_slot->name)));
			pfree(cmd.data);
			walrcv_clear_result(res);

			/*
			 * The slot being created will be dropped when it is released (see
			 * ReplicationSlotRelease).
			 */
			if (persist)
				*persist = false;

			return false;
		}

		/*
		 * It is possible to get null values for LSN and Xmin if slot is
		 * invalidated on the primary server, so handle accordingly.
		 */
		new_invalidated = DatumGetBool(slot_getattr(slot, 1, &isnull));
		Assert(!isnull);

		new_restart_lsn = DatumGetLSN(slot_getattr(slot, 2, &isnull));
		if (new_invalidated || isnull)
		{
			ereport(WARNING,
					(errmsg("slot \"%s\" invalidated on the primary server,"
							" slot creation aborted", remote_slot->name)));
			pfree(cmd.data);
			ExecClearTuple(slot);
			walrcv_clear_result(res);

			/*
			 * The slot being created will be dropped when it is released (see
			 * ReplicationSlotRelease).
			 */
			if (persist)
				*persist = false;

			return false;
		}

		/*
		 * Once we got valid restart_lsn, then confirmed_lsn and catalog_xmin
		 * are expected to be valid/non-null.
		 */
		new_confirmed_lsn = DatumGetLSN(slot_getattr(slot, 3, &isnull));
		Assert(!isnull);

		new_catalog_xmin = DatumGetTransactionId(slot_getattr(slot,
															  4, &isnull));
		Assert(!isnull);

		ExecClearTuple(slot);
		walrcv_clear_result(res);

		if (new_restart_lsn >= MyReplicationSlot->data.restart_lsn &&
			TransactionIdFollowsOrEquals(new_catalog_xmin,
										 MyReplicationSlot->data.catalog_xmin))
		{
			/* Update new values in remote_slot */
			remote_slot->restart_lsn = new_restart_lsn;
			remote_slot->confirmed_lsn = new_confirmed_lsn;
			remote_slot->catalog_xmin = new_catalog_xmin;

			ereport(LOG,
					errmsg("wait over for remote slot \"%s\" as its LSN (%X/%X)"
						   " and catalog xmin (%u) has now passed local slot LSN"
						   " (%X/%X) and catalog xmin (%u)",
						   remote_slot->name,
						   LSN_FORMAT_ARGS(new_restart_lsn),
						   new_catalog_xmin,
						   LSN_FORMAT_ARGS(MyReplicationSlot->data.restart_lsn),
						   MyReplicationSlot->data.catalog_xmin));
			pfree(cmd.data);

			return true;
		}

		if (++wait_count >= WORKER_PRIMARY_CATCHUP_WAIT_ATTEMPTS)
		{
			ereport(LOG,
					errmsg("aborting the wait for remote slot \"%s\"",
						   remote_slot->name));
			pfree(cmd.data);

			return false;
		}

		/*
		 * XXX: Is waiting for 2 seconds before retrying enough or more or
		 * less?
		 */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   2000L,
					   WAIT_EVENT_REPL_SLOTSYNC_PRIMARY_CATCHUP);

		ResetLatch(MyLatch);

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}
}

/*
 * Update local slot metadata as per remote_slot's positions
 */
static void
local_slot_update(RemoteSlot *remote_slot)
{
	Assert(MyReplicationSlot->data.invalidated == RS_INVAL_NONE);

	LogicalConfirmReceivedLocation(remote_slot->confirmed_lsn);
	LogicalIncreaseXminForSlot(remote_slot->confirmed_lsn,
							   remote_slot->catalog_xmin);
	LogicalIncreaseRestartDecodingForSlot(remote_slot->confirmed_lsn,
										  remote_slot->restart_lsn);

	SpinLockAcquire(&MyReplicationSlot->mutex);
	MyReplicationSlot->data.invalidated = remote_slot->invalidated;
	SpinLockRelease(&MyReplicationSlot->mutex);

	ReplicationSlotMarkDirty();
}

/*
 * Drop the slots for which sync is initiated but not yet completed
 * i.e. they are still waiting for the primary server to catch up.
 */
void
slotsync_drop_initiated_slots(void)
{
	List	   *slots = NIL;
	ListCell   *lc;

	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

	for (int i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

		if (s->in_use && s->data.sync_state == SYNCSLOT_STATE_INITIATED)
			slots = lappend(slots, s);
	}

	LWLockRelease(ReplicationSlotControlLock);

	foreach(lc, slots)
	{
		ReplicationSlot *s = (ReplicationSlot *) lfirst(lc);

		ReplicationSlotDrop(NameStr(s->data.name), true, false);
		ereport(LOG,
				(errmsg("dropped replication slot \"%s\" of dbid %d as it "
						"was not sync-ready", NameStr(s->data.name),
						s->data.database)));
	}

	list_free(slots);
}

/*
 * Get list of local logical slot names which are synchronized from
 * the primary server.
 */
static List *
get_local_synced_slot_names(void)
{
	List	   *localSyncedSlots = NIL;

	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

	for (int i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

		/* Check if it is logical synchronized slot */
		if (s->in_use && SlotIsLogical(s) &&
			(s->data.sync_state != SYNCSLOT_STATE_NONE))
		{
			localSyncedSlots = lappend(localSyncedSlots, s);
		}
	}

	LWLockRelease(ReplicationSlotControlLock);

	return localSyncedSlots;
}

/*
 * Helper function to check if local_slot is present in remote_slots list.
 *
 * It also checks if logical slot is locally invalidated i.e. invalidated on
 * the standby but valid on the primary server. If found so, it sets
 * locally_invalidated to true.
 */
static bool
check_sync_slot_validity(ReplicationSlot *local_slot, List *remote_slots,
						 bool *locally_invalidated)
{
	ListCell   *cell;

	foreach(cell, remote_slots)
	{
		RemoteSlot *remote_slot = (RemoteSlot *) lfirst(cell);

		if (strcmp(remote_slot->name, NameStr(local_slot->data.name)) == 0)
		{
			/*
			 * If remote slot is not invalidated but local slot is marked as
			 * invalidated, then set the bool.
			 */
			SpinLockAcquire(&local_slot->mutex);
			*locally_invalidated =
				(remote_slot->invalidated == RS_INVAL_NONE) &&
				(local_slot->data.invalidated != RS_INVAL_NONE);
			SpinLockRelease(&local_slot->mutex);

			return true;
		}
	}

	return false;
}

/*
 * Drop obsolete slots
 *
 * Drop the slots that no longer need to be synced i.e. these either do not
 * exist on the primary or are no longer enabled for failover.
 *
 * Also drop the slots that are valid on the primary that got invalidated
 * on the standby due to conflict (say required rows removed on the primary).
 * The assumption is, that these will get recreated in next sync-cycle and
 * it is okay to drop and recreate such slots as long as these are not
 * consumable on the standby (which is the case currently).
 */
static void
drop_obsolete_slots(List *remote_slot_list)
{
	List	   *local_slot_list = NIL;
	ListCell   *lc_slot;

	/*
	 * Get the list of local 'synced' slot so that those not on remote could
	 * be dropped.
	 */
	local_slot_list = get_local_synced_slot_names();

	foreach(lc_slot, local_slot_list)
	{
		ReplicationSlot *local_slot = (ReplicationSlot *) lfirst(lc_slot);
		bool		local_exists = false;
		bool		locally_invalidated = false;

		local_exists = check_sync_slot_validity(local_slot, remote_slot_list,
												&locally_invalidated);

		/*
		 * Drop the local slot either if it is not in the remote slots list or
		 * is invalidated while remote slot is still valid.
		 */
		if (!local_exists || locally_invalidated)
		{
			ReplicationSlotDrop(NameStr(local_slot->data.name), true, false);

			ereport(LOG,
					(errmsg("dropped replication slot \"%s\" of dbid %d",
							NameStr(local_slot->data.name),
							local_slot->data.database)));
		}
	}
}

/*
 * Constructs the query in order to get failover logical slots
 * information from the primary server.
 */
static void
construct_slot_query(StringInfo s)
{
	/*
	 * Fetch slots with failover enabled.
	 *
	 * If we are on cascading standby, we should fetch only those slots from
	 * the first standby which have sync_state as either 'n' or 'r'. Slots
	 * with sync_state as 'i' are not sync ready yet. And when we are on the
	 * first standby, the primary server is supposed to have slots with
	 * sync_state as 'n' only (or it may have all 'n', 'r' and 'i' if standby
	 * is promoted as primary). Thus in all the cases, filter sync_state !='i'
	 * is appropriate one.
	 */
	appendStringInfo(s,
					 "SELECT slot_name, plugin, confirmed_flush_lsn,"
					 " restart_lsn, catalog_xmin, two_phase, failover,"
					 " database, pg_get_slot_invalidation_cause(slot_name)"
					 " FROM pg_catalog.pg_replication_slots"
					 " WHERE failover and sync_state != 'i'");
}

/*
 * Wait for cascading physical standbys corresponding to physical slots
 * specified in standby_slot_names GUC to confirm receiving given lsn.
 */
static void
wait_for_standby_confirmation(XLogRecPtr wait_for_lsn,
							  WalReceiverConn *wrconn)
{
	List	   *standby_slots;

	/* Nothing to be done */
	if (strcmp(standby_slot_names, "") == 0)
		return;

	standby_slots = GetStandbySlotList(true);

	for (;;)
	{
		int			rc;

		WalSndFilterStandbySlots(wait_for_lsn, &standby_slots);

		/* Exit if done waiting for every slot. */
		if (standby_slots == NIL)
			break;

		/*
		 * This will reload configuration and will refresh the standby_slots
		 * as well provided standby_slot_names GUC is changed by the user.
		 */
		ProcessSlotSyncInterrupts(&wrconn, &standby_slots);

		/*
		 * XXX: Is waiting for 5 second before retrying enough or more or
		 * less?
		 */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   5000L,
					   WAIT_EVENT_WAL_SENDER_WAIT_FOR_STANDBY_CONFIRMATION);

		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}
}

/*
 * Synchronize single slot to given position.
 *
 * This creates a new slot if there is no existing one and updates the
 * metadata of the slot as per the data received from the primary server.
 *
 * The 'sync_state' in slot.data is set to SYNCSLOT_STATE_INITIATED
 * immediately after creation. It stays in same state until the
 * initialization is complete. The initialization is considered to
 * be completed once the remote_slot catches up with locally reserved
 * position and local slot is updated. The sync_state is then changed
 * to SYNCSLOT_STATE_READY.
 */
static void
synchronize_one_slot(WalReceiverConn *wrconn, RemoteSlot *remote_slot,
					 bool *slot_updated)
{
	ReplicationSlot *s;
	char		sync_state = 0;

	/*
	 * Make sure that concerned WAL is received before syncing slot to target
	 * lsn received from the primary server.
	 *
	 * This check should never pass as on the primary server, we have waited
	 * for the standby's confirmation before updating the logical slot.
	 */
	SpinLockAcquire(&WalRcv->mutex);
	if (remote_slot->confirmed_lsn > WalRcv->latestWalEnd)
	{
		SpinLockRelease(&WalRcv->mutex);
		elog(ERROR, "skipping sync of slot \"%s\" as the received slot sync "
			 "LSN %X/%X is ahead of the standby position %X/%X",
			 remote_slot->name,
			 LSN_FORMAT_ARGS(remote_slot->confirmed_lsn),
			 LSN_FORMAT_ARGS(WalRcv->latestWalEnd));
	}
	SpinLockRelease(&WalRcv->mutex);

	/* Search for the named slot */
	if ((s = SearchNamedReplicationSlot(remote_slot->name, true)))
	{
		SpinLockAcquire(&s->mutex);
		sync_state = s->data.sync_state;
		SpinLockRelease(&s->mutex);
	}

	StartTransactionCommand();

	/*
	 * Already existing slot (created by slot sync worker) and ready for sync,
	 * acquire and sync it.
	 */
	if (sync_state == SYNCSLOT_STATE_READY)
	{
		ReplicationSlotAcquire(remote_slot->name, true);

		/*
		 * Copy the invalidation cause from remote only if local slot is not
		 * invalidated locally, we don't want to overwrite existing one.
		 */
		if (MyReplicationSlot->data.invalidated == RS_INVAL_NONE)
		{
			SpinLockAcquire(&MyReplicationSlot->mutex);
			MyReplicationSlot->data.invalidated = remote_slot->invalidated;
			SpinLockRelease(&MyReplicationSlot->mutex);
		}

		/* Skip the sync if slot has been invalidated locally. */
		if (MyReplicationSlot->data.invalidated != RS_INVAL_NONE)
			goto cleanup;

		/*
		 * With hot_standby_feedback enabled and invalidations handled
		 * apropriately as above, this should never happen.
		 */
		if (remote_slot->restart_lsn < MyReplicationSlot->data.restart_lsn)
		{
			ereport(ERROR,
					errmsg("not synchronizing local slot \"%s\" LSN(%X/%X)"
						   " to remote slot's LSN(%X/%X) as synchronization "
						   " would move it backwards", remote_slot->name,
						   LSN_FORMAT_ARGS(MyReplicationSlot->data.restart_lsn),
						   LSN_FORMAT_ARGS(remote_slot->restart_lsn)));

			goto cleanup;
		}

		if (remote_slot->confirmed_lsn != MyReplicationSlot->data.confirmed_flush ||
			remote_slot->restart_lsn != MyReplicationSlot->data.restart_lsn ||
			remote_slot->catalog_xmin != MyReplicationSlot->data.catalog_xmin)
		{
			/* Update LSN of slot to remote slot's current position */
			local_slot_update(remote_slot);
			ReplicationSlotSave();
			*slot_updated = true;
		}
	}

	/*
	 * Already existing slot but not ready (i.e. waiting for the primary
	 * server to catch-up), lets attempt to make it sync-ready now.
	 */
	else if (sync_state == SYNCSLOT_STATE_INITIATED)
	{
		ReplicationSlotAcquire(remote_slot->name, true);

		/*
		 * Copy the invalidation cause from remote only if local slot is not
		 * invalidated locally, we don't want to overwrite existing one.
		 */
		if (MyReplicationSlot->data.invalidated == RS_INVAL_NONE)
		{
			SpinLockAcquire(&MyReplicationSlot->mutex);
			MyReplicationSlot->data.invalidated = remote_slot->invalidated;
			SpinLockRelease(&MyReplicationSlot->mutex);
		}

		/* Skip the sync if slot has been invalidated locally. */
		if (MyReplicationSlot->data.invalidated != RS_INVAL_NONE)
			goto cleanup;

		/*
		 * Refer the slot creation part (last 'else' block) for more details
		 * on this wait.
		 */
		if (remote_slot->restart_lsn < MyReplicationSlot->data.restart_lsn ||
			TransactionIdPrecedes(remote_slot->catalog_xmin,
								  MyReplicationSlot->data.catalog_xmin))
		{
			if (!wait_for_primary_slot_catchup(wrconn, remote_slot, NULL))
			{
				goto cleanup;
			}
		}

		/*
		 * Wait for primary is over, update the lsns and mark the slot as
		 * READY for further syncs.
		 */
		local_slot_update(remote_slot);
		SpinLockAcquire(&MyReplicationSlot->mutex);
		MyReplicationSlot->data.sync_state = SYNCSLOT_STATE_READY;
		SpinLockRelease(&MyReplicationSlot->mutex);

		/* Save the changes */
		ReplicationSlotMarkDirty();
		ReplicationSlotSave();

		*slot_updated = true;

		ereport(LOG, errmsg("newly locally created slot \"%s\" is sync-ready now",
							remote_slot->name));
	}
	/* User created slot with the same name exists, raise ERROR. */
	else if (sync_state == SYNCSLOT_STATE_NONE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("skipping sync of slot \"%s\" as it is a user created"
						" slot", remote_slot->name),
				 errdetail("This slot has failover enabled on the primary and"
						   " thus is sync candidate but user created slot with"
						   " the same name already exists on the standby")));
	}
	/* Otherwise create the slot first. */
	else
	{
		TransactionId xmin_horizon = InvalidTransactionId;
		ReplicationSlot *slot;

		ReplicationSlotCreate(remote_slot->name, true, RS_EPHEMERAL,
							  remote_slot->two_phase,
							  remote_slot->failover,
							  SYNCSLOT_STATE_INITIATED);

		slot = MyReplicationSlot;

		SpinLockAcquire(&slot->mutex);
		slot->data.database = get_database_oid(remote_slot->database, false);

		namestrcpy(&slot->data.plugin, remote_slot->plugin);
		SpinLockRelease(&slot->mutex);

		ReplicationSlotReserveWal();

		LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
		xmin_horizon = GetOldestSafeDecodingTransactionId(true);
		SpinLockAcquire(&slot->mutex);
		slot->effective_catalog_xmin = xmin_horizon;
		slot->data.catalog_xmin = xmin_horizon;
		SpinLockRelease(&slot->mutex);
		ReplicationSlotsComputeRequiredXmin(true);
		LWLockRelease(ProcArrayLock);

		/*
		 * If the local restart_lsn and/or local catalog_xmin is ahead of
		 * those on the remote then we cannot create the local slot in sync
		 * with the primary server because that would mean moving the local
		 * slot backwards and we might not have WALs retained for old LSN. In
		 * this case we will wait for the primary server's restart_lsn and
		 * catalog_xmin to catch up with the local one before attempting the
		 * sync.
		 */
		if (remote_slot->restart_lsn < MyReplicationSlot->data.restart_lsn ||
			TransactionIdPrecedes(remote_slot->catalog_xmin,
								  MyReplicationSlot->data.catalog_xmin))
		{
			bool		persist;

			if (!wait_for_primary_slot_catchup(wrconn, remote_slot, &persist))
			{
				/*
				 * The remote slot didn't catch up to locally reserved
				 * position.
				 *
				 * We do not drop the slot because the restart_lsn can be
				 * ahead of the current location when recreating the slot in
				 * the next cycle. It may take more time to create such a
				 * slot. Therefore, we persist it (provided remote-slot is
				 * still valid) and attempt the wait and synchronization in
				 * the next cycle.
				 */
				if (persist)
				{
					ReplicationSlotPersist();
					*slot_updated = true;
				}

				goto cleanup;
			}
		}


		/*
		 * Wait for primary is either not needed or is over. Update the lsns
		 * and mark the slot as READY for further syncs.
		 */
		local_slot_update(remote_slot);
		SpinLockAcquire(&MyReplicationSlot->mutex);
		MyReplicationSlot->data.sync_state = SYNCSLOT_STATE_READY;
		SpinLockRelease(&MyReplicationSlot->mutex);

		/* Mark the slot as PERSISTENT and save the changes to disk */
		ReplicationSlotPersist();
		*slot_updated = true;

		ereport(LOG, errmsg("newly locally created slot \"%s\" is sync-ready now",
							remote_slot->name));
	}

cleanup:

	ReplicationSlotRelease();
	CommitTransactionCommand();

	return;
}

/*
 * Synchronize slots.
 *
 * Gets the failover logical slots info from the primary server and update
 * the slots locally. Creates the slots if not present on the standby.
 *
 * Returns nap time for the next sync-cycle.
 */
static long
synchronize_slots(WalReceiverConn *wrconn)
{
#define SLOTSYNC_COLUMN_COUNT 9
	Oid			slotRow[SLOTSYNC_COLUMN_COUNT] = {TEXTOID, TEXTOID, LSNOID,
	LSNOID, XIDOID, BOOLOID, BOOLOID, TEXTOID, INT2OID};

	WalRcvExecResult *res;
	TupleTableSlot *slot;
	StringInfoData s;
	List	   *remote_slot_list = NIL;
	MemoryContext oldctx = CurrentMemoryContext;
	long		naptime = WORKER_DEFAULT_NAPTIME_MS;
	XLogRecPtr	max_confirmed_lsn = 0;
	ListCell   *cell;
	bool		slot_updated = false;
	TimestampTz now;

	/* The primary_slot_name is not set yet or WALs not received yet */
	SpinLockAcquire(&WalRcv->mutex);
	if (!WalRcv ||
		(WalRcv->slotname[0] == '\0') ||
		XLogRecPtrIsInvalid(WalRcv->latestWalEnd))
	{
		SpinLockRelease(&WalRcv->mutex);
		return naptime;
	}
	SpinLockRelease(&WalRcv->mutex);

	/* The syscache access needs a transaction env. */
	StartTransactionCommand();

	/*
	 * Make result tuples live outside TopTransactionContext to make them
	 * accessible even after transaction is committed.
	 */
	MemoryContextSwitchTo(oldctx);

	/* Construct query to get slots info from the primary server */
	initStringInfo(&s);
	construct_slot_query(&s);

	elog(DEBUG2, "slot sync worker's query:%s \n", s.data);

	/* Execute the query */
	res = walrcv_exec(wrconn, s.data, SLOTSYNC_COLUMN_COUNT, slotRow);
	pfree(s.data);

	if (res->status != WALRCV_OK_TUPLES)
		ereport(ERROR,
				(errmsg("could not fetch failover logical slots info "
						"from the primary server: %s", res->err)));

	CommitTransactionCommand();

	/* Switch to oldctx we saved */
	MemoryContextSwitchTo(oldctx);

	/* Construct the remote_slot tuple and synchronize each slot locally */
	slot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
	while (tuplestore_gettupleslot(res->tuplestore, true, false, slot))
	{
		bool		isnull;
		RemoteSlot *remote_slot = palloc0(sizeof(RemoteSlot));

		remote_slot->name = TextDatumGetCString(slot_getattr(slot, 1, &isnull));
		Assert(!isnull);

		remote_slot->plugin = TextDatumGetCString(slot_getattr(slot, 2, &isnull));
		Assert(!isnull);

		if (remote_slot->confirmed_lsn > max_confirmed_lsn)
			max_confirmed_lsn = remote_slot->confirmed_lsn;

		/*
		 * It is possible to get null values for LSN and Xmin if slot is
		 * invalidated on the primary server, so handle accordingly.
		 */
		remote_slot->confirmed_lsn = DatumGetLSN(slot_getattr(slot, 3, &isnull));
		if (isnull)
			remote_slot->confirmed_lsn = InvalidXLogRecPtr;

		remote_slot->restart_lsn = DatumGetLSN(slot_getattr(slot, 4, &isnull));
		if (isnull)
			remote_slot->restart_lsn = InvalidXLogRecPtr;

		remote_slot->catalog_xmin = DatumGetTransactionId(slot_getattr(slot,
																	   5, &isnull));
		if (isnull)
			remote_slot->catalog_xmin = InvalidTransactionId;

		remote_slot->two_phase = DatumGetBool(slot_getattr(slot, 6, &isnull));
		Assert(!isnull);

		remote_slot->failover = DatumGetBool(slot_getattr(slot, 7, &isnull));
		Assert(!isnull);

		remote_slot->database = TextDatumGetCString(slot_getattr(slot,
																 8, &isnull));
		Assert(!isnull);

		remote_slot->invalidated = DatumGetInt16(slot_getattr(slot, 9, &isnull));
		Assert(!isnull);

		/* Create list of remote slots */
		remote_slot_list = lappend(remote_slot_list, remote_slot);

		ExecClearTuple(slot);
	}

	/*
	 * Drop local slots that no longer need to be synced. Do it before
	 * synchronize_one_slot to allow dropping of slots before actual sync
	 * which are invalidated locally while still valid on the primary server.
	 */
	drop_obsolete_slots(remote_slot_list);

	/*
	 * If there are cascading standbys, wait for their confirmation before we
	 * update synced logical slots locally.
	 *
	 * Instead of waiting on confirmation for lsn of each slot, let us wait
	 * once for confirmation on max_confirmed_lsn. If that is confirmed by
	 * each cascading standby, we are good to update all the slots.
	 */
	if (remote_slot_list)
		wait_for_standby_confirmation(max_confirmed_lsn, wrconn);

	/* Now sync the slots locally */
	foreach(cell, remote_slot_list)
	{
		RemoteSlot *remote_slot = (RemoteSlot *) lfirst(cell);

		synchronize_one_slot(wrconn, remote_slot, &slot_updated);
	}

	now = GetCurrentTimestamp();

	/*
	 * If any of the slots get updated in this sync-cycle, retain default
	 * naptime and update 'last_update_time' in slot sync worker. But if no
	 * activity is observed in this sync-cycle, then increase naptime provided
	 * inactivity time reaches threshold.
	 */
	if (slot_updated)
		last_update_time = now;

	else if (TimestampDifferenceExceeds(last_update_time,
										now, WORKER_INACTIVITY_THRESHOLD_MS))
		naptime = WORKER_INACTIVITY_NAPTIME_MS;

	/* We are done, free remote_slot_list elements */
	list_free_deep(remote_slot_list);

	walrcv_clear_result(res);

	return naptime;
}

/*
 * Connect to the remote (primary) server.
 *
 * This uses GUC primary_conninfo in order to connect to the primary.
 * For slot sync to work, primary_conninfo is required to specify dbname
 * as well.
 */
static WalReceiverConn *
remote_connect(void)
{
	WalReceiverConn *wrconn = NULL;
	char	   *err;

	wrconn = walrcv_connect(PrimaryConnInfo, true, false,
							cluster_name[0] ? cluster_name : "slotsyncworker", &err);
	if (wrconn == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not connect to the primary server: %s", err)));
	return wrconn;
}

/*
 * Checks if GUCs are set appropriately before starting slot sync worker
 */
static void
validate_slotsync_parameters(char **dbname)
{
	/*
	 * Since 'enable_syncslot' is ON, check that other GUC settings
	 * (primary_slot_name, hot_standby_feedback, wal_level, primary_conninfo)
	 * are compatible with slot synchronization. If not, raise ERROR.
	 */

	/*
	 * A physical replication slot(primary_slot_name) is required on the
	 * primary to ensure that the rows needed by the standby are not removed
	 * after restarting, so that the synchronized slot on the standby will not
	 * be invalidated.
	 */
	if (PrimarySlotName == NULL || strcmp(PrimarySlotName, "") == 0)
		ereport(ERROR,
				errmsg("exiting slots synchronization as primary_slot_name is not set"));

	/*
	 * Hot_standby_feedback must be enabled to cooperate with the physical
	 * replication slot, which allows informing the primary about the xmin and
	 * catalog_xmin values on the standby.
	 */
	if (!hot_standby_feedback)
		ereport(ERROR,
				errmsg("exiting slots synchronization as hot_standby_feedback is off"));

	/*
	 * Logical decoding requires wal_level >= logical and we currently only
	 * synchronize logical slots.
	 */
	if (wal_level < WAL_LEVEL_LOGICAL)
		ereport(ERROR,
				errmsg("exiting slots synchronisation as it requires wal_level >= logical"));

	/*
	 * The primary_conninfo is required to make connection to primary for
	 * getting slots information.
	 */
	if (PrimaryConnInfo == NULL || strcmp(PrimaryConnInfo, "") == 0)
		ereport(ERROR,
				errmsg("exiting slots synchronization as primary_conninfo is not set"));

	/*
	 * The slot sync worker needs a database connection for walrcv_exec to
	 * work.
	 */
	*dbname = walrcv_get_dbname_from_conninfo(PrimaryConnInfo);
	if (*dbname == NULL)
		ereport(ERROR,
				errmsg("exiting slots synchronization as dbname is not specified in primary_conninfo"));

}

/*
 * Re-read the config file.
 *
 * If any of the slot sync GUCs changed, validate the values again
 * through validate_slotsync_parameters() which will exit the worker
 * if validaity fails.
 */
static void
slotsync_reread_config(List **standby_slots)
{
	char	   *conninfo = pstrdup(PrimaryConnInfo);
	char	   *slotname = pstrdup(PrimarySlotName);
	bool		syncslot = enable_syncslot;
	bool		standbyfeedback = hot_standby_feedback;
	bool		revalidate = false;
	char	   *dbname;

	dbname = walrcv_get_dbname_from_conninfo(PrimaryConnInfo);
	Assert(dbname);

	/*
	 * Reload configs and recreate the standby_slot_names_list if GUC
	 * standby_slot_names changed.
	 */
	if (standby_slots)
		WalSndRereadConfigAndReInitSlotList(standby_slots);
	else
		ProcessConfigFile(PGC_SIGHUP);

	if ((strcmp(conninfo, PrimaryConnInfo) != 0) ||
		(strcmp(slotname, PrimarySlotName) != 0) ||
		(syncslot != enable_syncslot) ||
		(standbyfeedback != hot_standby_feedback))
	{
		revalidate = true;
	}

	pfree(conninfo);
	pfree(slotname);

	if (revalidate)
	{
		char	   *new_dbname;

		validate_slotsync_parameters(&new_dbname);

		/*
		 * Since we have initialized this worker with old dbname, thus exit if
		 * dbname changed. Let it get restarted and connect to new dbname
		 * specified.
		 */
		if (strcmp(dbname, new_dbname) != 0)
		{
			ereport(ERROR,
					errmsg("exiting slot sync woker as dbname in primary_conninfo changed"));
		}
	}
}


/*
 * Interrupt handler for main loop of slot sync worker.
 */
static void
ProcessSlotSyncInterrupts(WalReceiverConn **wrconn,
						  List **standby_slots)
{
	CHECK_FOR_INTERRUPTS();

	if (ShutdownRequestPending)
	{
		ereport(LOG,
				errmsg("replication slot sync worker is shutting"
					   " down on receiving SIGINT"));

		walrcv_disconnect(*wrconn);
		proc_exit(0);
	}


	if (ConfigReloadPending)
	{
		ConfigReloadPending = false;
		slotsync_reread_config(standby_slots);
	}
}

/*
 * Cleanup function for logical replication launcher.
 *
 * Called on logical replication launcher exit.
 */
static void
slotsync_worker_onexit(int code, Datum arg)
{
	SpinLockAcquire(&SlotSyncWorker->mutex);
	SlotSyncWorker->pid = 0;
	SpinLockRelease(&SlotSyncWorker->mutex);
}

/*
 * The main loop of our worker process.
 *
 * It connects to the primary server, fetches logical failover slots
 * information periodically in order to create and sync the slots.
 */
void
ReplSlotSyncWorkerMain(Datum main_arg)
{
	WalReceiverConn *wrconn = NULL;
	char	   *dbname;

	ereport(LOG, errmsg("replication slot sync worker started"));

	before_shmem_exit(slotsync_worker_onexit, (Datum) 0);

	SpinLockAcquire(&SlotSyncWorker->mutex);

	Assert(SlotSyncWorker->pid == 0);

	/* Advertise our PID so that the startup process can kill us on promotion */
	SlotSyncWorker->pid = MyProcPid;

	SpinLockRelease(&SlotSyncWorker->mutex);

	/* Setup signal handling */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* Load the libpq-specific functions */
	load_file("libpqwalreceiver", false);

	validate_slotsync_parameters(&dbname);

	/*
	 * Connect to the database specified by user in PrimaryConnInfo. We need a
	 * database connection for walrcv_exec to work. Please see comments atop
	 * libpqrcv_exec.
	 */
	BackgroundWorkerInitializeConnection(dbname, NULL, 0);

	/* Connect to the primary server */
	wrconn = remote_connect();

	/* Main wait loop. */
	for (;;)
	{
		int			rc;
		long		naptime;

		ProcessSlotSyncInterrupts(&wrconn, NULL /* standby_slots */ );

		naptime = synchronize_slots(wrconn);

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   naptime,
					   WAIT_EVENT_REPL_SLOTSYNC_MAIN);

		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}

	/*
	 * The slot sync worker can not get here because it will only stop when it
	 * receives a SIGINT from the logical replication launcher, or when there
	 * is an error.
	 */
	Assert(false);
}

/*
 * Is current process the slot sync worker?
 */
bool
IsSlotSyncWorker(void)
{
	return SlotSyncWorker->pid == MyProcPid;
}

/*
 * Shut down the slot sync worker.
 */
void
ShutDownSlotSync(void)
{
	SpinLockAcquire(&SlotSyncWorker->mutex);
	if (!SlotSyncWorker->pid)
	{
		SpinLockRelease(&SlotSyncWorker->mutex);
		return;
	}

	kill(SlotSyncWorker->pid, SIGINT);

	SpinLockRelease(&SlotSyncWorker->mutex);

	/* Wait for it to die. */
	for (;;)
	{
		int			rc;

		/* Wait a bit, we don't expect to have to wait long. */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   10L, WAIT_EVENT_BGWORKER_SHUTDOWN);

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}

		SpinLockAcquire(&SlotSyncWorker->mutex);

		/* Is it gone? */
		if (!SlotSyncWorker->pid)
			break;

		SpinLockRelease(&SlotSyncWorker->mutex);
	}

	SpinLockRelease(&SlotSyncWorker->mutex);
}

/*
 * Allocate and initialize slow sync worker shared memory
 */
void
SlotSyncWorkerShmemInit(void)
{
	Size		size;
	bool		found;

	size = sizeof(SlotSyncWorkerCtx);
	size = MAXALIGN(size);

	SlotSyncWorker = (SlotSyncWorkerCtx *)
		ShmemInitStruct("Slot Sync Worker Data", size, &found);

	if (!found)
	{
		memset(SlotSyncWorker, 0, size);
		SpinLockInit(&SlotSyncWorker->mutex);
	}
}

/*
 * Register the background worker for slots synchronization provided
 * enable_syncslot is ON.
 */
void
SlotSyncWorkerRegister(void)
{
	BackgroundWorker bgw;

	if (!enable_syncslot)
	{
		ereport(LOG,
				errmsg("skipping slots synchronization as enable_syncslot is disabled."));
		return;
	}

	memset(&bgw, 0, sizeof(bgw));

	/* We need database connection which needs shared-memory access as well. */
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;

	/* Start as soon as a consistent state has been reached in a hot standby */
	bgw.bgw_start_time = BgWorkerStart_ConsistentState_HotStandby;

	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ReplSlotSyncWorkerMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "replication slot sync worker");
	snprintf(bgw.bgw_type, BGW_MAXLEN,
			 "slot sync worker");

	bgw.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
}
