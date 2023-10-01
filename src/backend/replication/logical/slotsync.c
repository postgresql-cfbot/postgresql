/*-------------------------------------------------------------------------
 * slotsync.c
 *	   PostgreSQL worker for synchronizing slots to a standby from the
 *         primary
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/slotsync.c
 *
 * This file contains the code for slot-sync workers on physical standby
 * to fetch logical replication slot information from the primary server
 * (PrimaryConnInfo), create the slots on the standby, and synchronize
 * them periodically. Slot-sync workers only synchronize slots configured
 * in 'synchronize_slot_names'.
 *
 * It also takes care of dropping the slots which were created by it and are
 * currently not needed to be synchronized.
 *
 * It takes a nap of WORKER_DEFAULT_NAPTIME_MS before every next
 * synchronization. If there is no activity observed on the primary for some
 * time, the nap time is increased to WORKER_INACTIVITY_NAPTIME_MS, but if any
 * activity is observed, the nap time reverts to the default value.
 *---------------------------------------------------------------------------
 */

#include "postgres.h"

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
	bool		conflicting;
	XLogRecPtr	restart_lsn;
	XLogRecPtr	confirmed_lsn;
	TransactionId catalog_xmin;

	/* RS_INVAL_NONE if valid, or the reason of invalidation */
	ReplicationSlotInvalidationCause invalidated;
} RemoteSlot;

List	   *sync_slot_names_list = NIL;

/* Worker's nap time in case of regular activity on primary */
#define WORKER_DEFAULT_NAPTIME_MS                   10L /* 10 ms */

/* Worker's nap time in case of no-activity on primary */
#define WORKER_INACTIVITY_NAPTIME_MS                10000L	/* 10 sec */

/*
 * Inactivity Threshold in ms before increasing nap time of worker.
 *
 * If the lsn of slot being monitored did not change for this threshold time,
 * then increase nap time of current worker from WORKER_DEFAULT_NAPTIME_MS to
 * WORKER_INACTIVITY_NAPTIME_MS.
 */
#define WORKER_INACTIVITY_THRESHOLD_MS 1000L	/* 1 sec */

/*
 * Wait for remote slot to pass locally reserved position.
 */
static bool
wait_for_primary_slot_catchup(WalReceiverConn *wrconn, RemoteSlot *remote_slot)

{
#define WAIT_OUTPUT_COLUMN_COUNT 3
	StringInfoData cmd;

	ereport(LOG,
			errmsg("waiting for remote slot \"%s\" LSN (%u/%X) and catalog xmin"
				   " (%u) to pass local slot LSN (%u/%X) and and catalog xmin (%u)",
				   remote_slot->name,
				   LSN_FORMAT_ARGS(remote_slot->restart_lsn),
				   remote_slot->catalog_xmin,
				   LSN_FORMAT_ARGS(MyReplicationSlot->data.restart_lsn),
				   MyReplicationSlot->data.catalog_xmin));

	initStringInfo(&cmd);
	appendStringInfo(&cmd,
					 "SELECT restart_lsn, confirmed_flush_lsn, catalog_xmin"
					 "  FROM pg_catalog.pg_replication_slots"
					 " WHERE slot_name = %s",
					 quote_literal_cstr(remote_slot->name));

	for (;;)
	{
		XLogRecPtr	new_restart_lsn;
		XLogRecPtr	new_confirmed_lsn;
		TransactionId new_catalog_xmin;
		WalRcvExecResult *res;
		TupleTableSlot *slot;
		int			rc;
		bool		isnull;
		Oid			slotRow[WAIT_OUTPUT_COLUMN_COUNT] = {LSNOID, LSNOID, XIDOID};

		CHECK_FOR_INTERRUPTS();

		/* Check if this standby is promoted while we are waiting */
		if (!RecoveryInProgress())
		{
			/*
			 * The remote slot didn't pass the locally reserved position at
			 * the time of local promotion, so it's not safe to use.
			 */
			ereport(
					WARNING,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg(
							"slot-sync wait for slot %s interrupted by promotion, "
							"slot creation aborted", remote_slot->name)));
			pfree(cmd.data);
			return false;
		}

		res = walrcv_exec(wrconn, cmd.data, WAIT_OUTPUT_COLUMN_COUNT, slotRow);

		if (res->status != WALRCV_OK_TUPLES)
			ereport(ERROR,
					(errmsg("could not fetch slot info for slot \"%s\" from"
							" primary: %s", remote_slot->name, res->err)));

		slot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
		if (!tuplestore_gettupleslot(res->tuplestore, true, false, slot))
		{
			ereport(WARNING,
					(errmsg("slot \"%s\" disappeared from the primary, aborting"
							" slot creation", remote_slot->name)));
			pfree(cmd.data);
			walrcv_clear_result(res);
			return false;
		}

		/*
		 * It is possible to get null values for lsns and xmin if slot is
		 * invalidated on primary, so handle accordingly.
		 */
		new_restart_lsn = DatumGetLSN(slot_getattr(slot, 1, &isnull));
		if (isnull)
		{
			ereport(WARNING,
					(errmsg("slot \"%s\" invalidated on primary, aborting"
							" slot creation", remote_slot->name)));
			pfree(cmd.data);
			ExecClearTuple(slot);
			walrcv_clear_result(res);
			return false;
		}

		/*
		 * Once we got valid restart_lsn, then confirmed_lsn and catalog_xmin
		 * are expected to be valid/non-null, so assert if found null.
		 */
		new_confirmed_lsn = DatumGetLSN(slot_getattr(slot, 2, &isnull));
		Assert(!isnull);

		new_catalog_xmin = DatumGetTransactionId(slot_getattr(slot,
															  3, &isnull));
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
						   "and catalog xmin (%u) has now passed local slot LSN"
						   " (%X/%X) and catalog xmin (%u)",
						   remote_slot->name,
						   LSN_FORMAT_ARGS(new_restart_lsn),
						   new_catalog_xmin,
						   LSN_FORMAT_ARGS(MyReplicationSlot->data.restart_lsn),
						   MyReplicationSlot->data.catalog_xmin));
			pfree(cmd.data);
			return true;
		}

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   WORKER_DEFAULT_NAPTIME_MS,
					   WAIT_EVENT_REPL_SLOTSYNC_MAIN);

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
	LogicalConfirmReceivedLocation(remote_slot->confirmed_lsn);
	LogicalIncreaseXminForSlot(remote_slot->confirmed_lsn,
							   remote_slot->catalog_xmin);
	LogicalIncreaseRestartDecodingForSlot(remote_slot->confirmed_lsn,
										  remote_slot->restart_lsn);
	MyReplicationSlot->data.invalidated = remote_slot->invalidated;
	ReplicationSlotMarkDirty();
}

/*
 * Get list of local logical slot names which are synchronized from
 * primary and belongs to one of the DBs passed in.
 */
static List *
get_local_synced_slot_names(Oid *dbids)
{
	List	   *localSyncedSlots = NIL;

	Assert(LWLockHeldByMeInMode(SlotSyncWorkerLock, LW_SHARED));

	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

	for (int i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

		/* Check if it is logical synchronized slot */
		if (s->in_use && SlotIsLogical(s) && s->data.synced)
		{
			for (int j = 0; j < MySlotSyncWorker->dbcount; j++)
			{
				/*
				 * Add it to output list if this belongs to one of the
				 * worker's dbs.
				 */
				if (s->data.database == dbids[j])
				{
					localSyncedSlots = lappend(localSyncedSlots, s);
					break;
				}
			}
		}
	}

	LWLockRelease(ReplicationSlotControlLock);

	return localSyncedSlots;
}

/*
 * Helper function to check if local_slot is present in remote_slots list.
 *
 * It also checks if logical slot is locally invalidated i.e. invalided on
 * standby but valid on primary. If found so, it sets locally_invalidated to
 * true.
 */
static bool
slot_exists_locally(List *remote_slots, ReplicationSlot *local_slot,
					bool *locally_invalidated)
{
	ListCell   *cell;

	foreach(cell, remote_slots)
	{
		RemoteSlot *remote_slot = (RemoteSlot *) lfirst(cell);

		if (strcmp(remote_slot->name, NameStr(local_slot->data.name)) == 0)
		{
			/*
			 * if remote slot is marked as non-conflicting (i.e. not
			 * invalidated) but local slot is marked as invalidated, then set
			 * the bool.
			 */
			if (!remote_slot->conflicting &&
				SlotIsLogical(local_slot) &&
				local_slot->data.invalidated != RS_INVAL_NONE)
				*locally_invalidated = true;

			return true;
		}
	}

	return false;
}

/*
 * Use slot_name in query.
 *
 * Check the dbid of the slot and if the dbid is one of the dbids managed by
 * current worker, then use this slot-name in query to get the data from
 * primary. If the slot is not created yet on standby (first time it is being
 * queried), then too, use this slot in query.
 */
static bool
use_slot_in_query(char *slot_name, Oid *dbids)
{
	bool		slot_found = false;
	bool		relevant_db = false;
	ReplicationSlot *slot;

	Assert(LWLockHeldByMeInMode(SlotSyncWorkerLock, LW_SHARED));

	/* Search for the local slot with the same name as slot_name */
	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

	for (int i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

		if (s->in_use && SlotIsLogical(s) &&
			(strcmp(NameStr(s->data.name), slot_name) == 0))
		{
			slot_found = true;
			slot = s;
			break;
		}
	}

	/* Check if slot belongs to one of the input dbids */
	if (slot_found)
	{
		for (int j = 0; j < MySlotSyncWorker->dbcount; j++)
		{
			if (slot->data.database == dbids[j])
			{
				relevant_db = true;
				break;
			}
		}
	}

	LWLockRelease(ReplicationSlotControlLock);

	/*
	 * Return TRUE if either slot is not yet created on standby or if it
	 * belongs to one of the dbs passed in dbids.
	 */
	if (!slot_found || relevant_db)
		return true;

	return false;
}


/*
 * Compute nap time for MySlotSyncWorker.
 *
 * The slot-sync worker takes a nap before it again checks for slots on primary.
 * The time for each nap is computed here.
 *
 * The first slot managed by each worker is chosen for monitoring purpose.
 * If the lsn of that slot changes during each sync-check time, then the
 * nap time is kept at regular value of WORKER_DEFAULT_NAPTIME_MS.
 * When no lsn change is observed for WORKER_INACTIVITY_THRESHOLD_MS
 * time, then the nap time is increased to WORKER_INACTIVITY_NAPTIME_MS.
 * This nap time is brought back to WORKER_DEFAULT_NAPTIME_MS as soon as
 * lsn change is observed.
 *
 * The caller is supposed to ignore return-value of 0. The 0 value is returned
 * for the slots other that slot being monitored.
 */
static long
compute_naptime(RemoteSlot *remote_slot)
{
	if (NameStr(MySlotSyncWorker->monitoring_info.slot_name)[0] == '\0')
	{
		/*
		 * First time, just update the name and lsn and return regular nap
		 * time. Start comparison from next time onward.
		 */
		strcpy(NameStr(MySlotSyncWorker->monitoring_info.slot_name),
			   remote_slot->name);
		MySlotSyncWorker->monitoring_info.confirmed_lsn =
			remote_slot->confirmed_lsn;

		MySlotSyncWorker->monitoring_info.last_update_time =
			GetCurrentTimestamp();

		return WORKER_DEFAULT_NAPTIME_MS;
	}

	/* If this is the slot being monitored by this worker, compute nap time */
	if (strcmp(remote_slot->name,
			   NameStr(MySlotSyncWorker->monitoring_info.slot_name)) == 0)
	{
		TimestampTz now = GetCurrentTimestamp();

		/*
		 * If new received lsn (remote one) is different from what we have in
		 * our local slot, then update last_update_time.
		 */
		if (MySlotSyncWorker->monitoring_info.confirmed_lsn !=
			remote_slot->confirmed_lsn)
			MySlotSyncWorker->monitoring_info.last_update_time = now;

		MySlotSyncWorker->monitoring_info.confirmed_lsn =
			remote_slot->confirmed_lsn;

		/* If the inactivity time reaches the threshold, increase nap time */
		if (TimestampDifferenceExceeds(MySlotSyncWorker->monitoring_info.last_update_time,
									   now, WORKER_INACTIVITY_THRESHOLD_MS))
			return WORKER_INACTIVITY_NAPTIME_MS;
		else
			return WORKER_DEFAULT_NAPTIME_MS;
	}

	/* If it is not the slot being monitored, return 0 */
	return 0;
}

/*
 * Get Remote Slot's invalidation cause.
 *
 * This gets invalidation cause of remote slot.
 */
static ReplicationSlotInvalidationCause
get_remote_invalidation_cause(WalReceiverConn *wrconn, char *slot_name)
{
	WalRcvExecResult *res;
	Oid			slotRow[1] = {INT2OID};
	StringInfoData cmd;
	bool		isnull;
	TupleTableSlot *slot;
	ReplicationSlotInvalidationCause cause;
	MemoryContext oldctx = CurrentMemoryContext;

	/* Syscache access needs a transaction env. */
	StartTransactionCommand();

	/* Make things live outside TX context */
	MemoryContextSwitchTo(oldctx);

	initStringInfo(&cmd);
	appendStringInfo(&cmd,
					 "select pg_get_slot_invalidation_cause(%s)",
					 quote_literal_cstr(slot_name));
	res = walrcv_exec(wrconn, cmd.data, 1, slotRow);
	pfree(cmd.data);

	CommitTransactionCommand();

	/* Switch to oldctx we saved */
	MemoryContextSwitchTo(oldctx);

	if (res->status != WALRCV_OK_TUPLES)
		ereport(ERROR,
				(errmsg("could not fetch invalidation cuase for slot \"%s\" from"
						" primary: %s", slot_name, res->err)));

	slot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
	if (!tuplestore_gettupleslot(res->tuplestore, true, false, slot))
		ereport(ERROR,
				(errmsg("slot \"%s\" disapeared from the primary",
						slot_name)));

	cause = DatumGetInt16(slot_getattr(slot, 1, &isnull));
	Assert(!isnull);

	ExecClearTuple(slot);
	walrcv_clear_result(res);

	return cause;
}

/*
 * Drop obsolete slots
 *
 * Drop the slots which no longer need to be synced i.e. these either
 * do not exist on primary or are no longer part of synchronize_slot_names.
 *
 * Also drop the slots which are valid on primary and got invalidated
 * on standby due to conflict (say required rows removed on primary).
 * The assumption is, these will get recreated in next sync-cycle and
 * it is okay to drop and recreate such slots as long as these are not
 * consumable on standby (which is the case currently).
 */
static void
drop_obsolete_slots(Oid *dbids, List *remote_slot_list)
{
	List	   *local_slot_list = NIL;
	ListCell   *lc_slot;

	Assert(LWLockHeldByMeInMode(SlotSyncWorkerLock, LW_SHARED));

	/*
	 * Get the list of local slots for dbids managed by this worker, so that
	 * those not on remote could be dropped.
	 */
	local_slot_list = get_local_synced_slot_names(dbids);

	foreach(lc_slot, local_slot_list)
	{
		ReplicationSlot *local_slot = (ReplicationSlot *) lfirst(lc_slot);
		bool		local_exists = false;
		bool		locally_invalidated = false;

		local_exists = slot_exists_locally(remote_slot_list, local_slot,
										   &locally_invalidated);

		/*
		 * Drop the local slot either if it is not in the remote slots list or
		 * is invalidated while remote slot is still valid.
		 */
		if (!local_exists || locally_invalidated)
		{
			ReplicationSlotDrop(NameStr(local_slot->data.name), true);

			/* If this slot is being monitored, clean-up the monitoring info */
			if (strcmp(NameStr(local_slot->data.name),
					   NameStr(MySlotSyncWorker->monitoring_info.slot_name)) == 0)
			{
				MemSet(NameStr(MySlotSyncWorker->monitoring_info.slot_name), 0, NAMEDATALEN);
				MySlotSyncWorker->monitoring_info.confirmed_lsn = 0;
				MySlotSyncWorker->monitoring_info.last_update_time = 0;
			}

			elog(LOG, "Dropped replication slot \"%s\" ",
				 NameStr(local_slot->data.name));
		}
	}
}

/*
 * Construct Slot Query
 *
 * It constructs the query using dbids array and sync_slot_names_list
 * in order to get slots information from the primary.
 */
static bool
construct_slot_query(StringInfo s, Oid *dbids)
{
	Assert(LWLockHeldByMeInMode(SlotSyncWorkerLock, LW_SHARED));

	appendStringInfo(s,
					 "SELECT slot_name, plugin, confirmed_flush_lsn,"
					 " restart_lsn, catalog_xmin, two_phase, conflicting, "
					 " database FROM pg_catalog.pg_replication_slots"
					 " WHERE database IN ");

	appendStringInfoChar(s, '(');
	for (int i = 0; i < MySlotSyncWorker->dbcount; i++)
	{
		char	   *dbname;

		if (i != 0)
			appendStringInfoChar(s, ',');

		dbname = get_database_name(dbids[i]);
		appendStringInfo(s, "%s",
						 quote_literal_cstr(dbname));
		pfree(dbname);
	}
	appendStringInfoChar(s, ')');

	if (strcmp(synchronize_slot_names, "") != 0 &&
		strcmp(synchronize_slot_names, "*") != 0)
	{
		ListCell   *lc;
		bool		first_slot = true;


		foreach(lc, sync_slot_names_list)
		{
			char	   *slot_name = lfirst(lc);

			if (!use_slot_in_query(slot_name, dbids))
				continue;

			if (first_slot)
				appendStringInfoString(s, " AND slot_name IN (");
			else
				appendStringInfoChar(s, ',');

			appendStringInfo(s, "%s",
							 quote_literal_cstr(slot_name));
			first_slot = false;
		}

		if (!first_slot)
			appendStringInfoChar(s, ')');
	}

	return true;
}

/*
 * Synchronize single slot to given position.
 *
 * This creates new slot if there is no existing one and updates the
 * metadata of existing slots as per the data received from the primary.
 */
static void
synchronize_one_slot(WalReceiverConn *wrconn, RemoteSlot *remote_slot)
{
	bool		found = false;

	/* Good to check again if standby is promoted */
	if (!RecoveryInProgress())
	{
		ereport(
				WARNING,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg(
						"slot-sync for slot %s interrupted by promotion, "
						"sync not possible", remote_slot->name)));
		return;
	}

	/*
	 * Make sure that concerned WAL is received before syncing slot to target
	 * lsn received from the primary.
	 */
	if (remote_slot->confirmed_lsn > WalRcv->latestWalEnd)
	{
		ereport(WARNING,
				errmsg("skipping sync of slot \"%s\" as the received slot-sync "
					   "lsn %X/%X is ahead of the standby position %X/%X",
					   remote_slot->name,
					   LSN_FORMAT_ARGS(remote_slot->confirmed_lsn),
					   LSN_FORMAT_ARGS(WalRcv->latestWalEnd)));
		return;
	}

	/* Search for the named slot and mark it active if we find it. */
	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
	for (int i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

		if (!s->in_use)
			continue;

		if (strcmp(NameStr(s->data.name), remote_slot->name) == 0)
		{
			found = true;
			break;
		}
	}
	LWLockRelease(ReplicationSlotControlLock);

	StartTransactionCommand();

	/* Already existing slot, acquire */
	if (found)
	{
		ReplicationSlotAcquire(remote_slot->name, true);

		if (remote_slot->confirmed_lsn < MyReplicationSlot->data.confirmed_flush)
		{
			ereport(WARNING,
					errmsg("not synchronizing slot %s; synchronization would move"
						   " it backward", remote_slot->name));

			ReplicationSlotRelease();
			CommitTransactionCommand();
			return;
		}

		/* Update lsns of slot to remote slot's current position */
		local_slot_update(remote_slot);
		ReplicationSlotSave();
	}
	/* Otherwise create the slot first. */
	else
	{
		TransactionId xmin_horizon = InvalidTransactionId;
		ReplicationSlot *slot;

		ReplicationSlotCreate(remote_slot->name, true, RS_EPHEMERAL,
							  remote_slot->two_phase);
		slot = MyReplicationSlot;

		SpinLockAcquire(&slot->mutex);
		slot->data.database = get_database_oid(remote_slot->database, false);
		slot->data.synced = true;
		namestrcpy(&slot->data.plugin, remote_slot->plugin);
		SpinLockRelease(&slot->mutex);

		ReplicationSlotReserveWal();

		LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
		xmin_horizon = GetOldestSafeDecodingTransactionId(true);
		slot->effective_catalog_xmin = xmin_horizon;
		slot->data.catalog_xmin = xmin_horizon;
		ReplicationSlotsComputeRequiredXmin(true);
		LWLockRelease(ProcArrayLock);

		/*
		 * We might not have the WALs retained locally corresponding to
		 * remote's restart_lsn if our local restart_lsn and/or local
		 * catalog_xmin is ahead of remote's one. And thus we can not create
		 * the local slot in sync with primary as that would mean moving local
		 * slot backward. Thus wait for primary's restart_lsn and catalog_xmin
		 * to catch up with the local ones and then do the sync.
		 */
		if (remote_slot->restart_lsn < MyReplicationSlot->data.restart_lsn ||
			TransactionIdPrecedes(remote_slot->catalog_xmin,
								  MyReplicationSlot->data.catalog_xmin))
		{
			if (!wait_for_primary_slot_catchup(wrconn, remote_slot))
			{
				/*
				 * The remote slot didn't catch up to locally reserved
				 * position
				 */
				ReplicationSlotRelease();
				CommitTransactionCommand();
				return;
			}

		}

		/* Update lsns of slot to remote slot's current position */
		local_slot_update(remote_slot);
		ReplicationSlotPersist();
	}

	ReplicationSlotRelease();
	CommitTransactionCommand();
}

/*
 * Synchronize slots.
 *
 * It looks into dbids array maintained in dsa and gets the logical slots info
 * from the primary for the slots configured in synchronize_slot_names and
 * belonging to concerned dbids. It then updates the slots locally as per the
 * data received from the primary. It creates the slots if not present on the
 * standby.
 *
 * It returns nap time for the next sync-cycle.
 */
static long
synchronize_slots(dsa_area *dsa, WalReceiverConn *wrconn)
{
#define SLOTSYNC_COLUMN_COUNT 8
	Oid			slotRow[SLOTSYNC_COLUMN_COUNT] = {TEXTOID, TEXTOID, LSNOID,
	LSNOID, XIDOID, BOOLOID, BOOLOID, TEXTOID};

	WalRcvExecResult *res;
	TupleTableSlot *slot;
	StringInfoData s;
	List	   *remote_slot_list = NIL;
	MemoryContext oldctx = CurrentMemoryContext;
	long		naptime = WORKER_DEFAULT_NAPTIME_MS;
	long		value;
	Oid		   *dbids;
	ListCell   *cell;

	if (strcmp(synchronize_slot_names, "") == 0 || !WalRcv)
		return naptime;

	/* The primary_slot_name is not set yet */
	if (WalRcv->slotname[0] == '\0')
		return naptime;

	/* WALs not received yet */
	if (XLogRecPtrIsInvalid(WalRcv->latestWalEnd))
		return naptime;

	/*
	 * No more writes to dbcount and dbids by launcher after this until we
	 * release this lock.
	 */
	LWLockAcquire(SlotSyncWorkerLock, LW_SHARED);

	/*
	 * There is a small window between CHECK_FOR_INTERRUPTS done last and
	 * above lock acquiring, so there is a chance that synchronize_slot_names
	 * has changed making dbs assigned to this worker as invalid. In that
	 * case, launcher will make dbcount=0 and will send SIGINT to this worker.
	 * So check dbcount before proceeding.
	 */
	if (!MySlotSyncWorker->dbcount)
	{
		/* Return and handle the interrupts in main loop */
		return false;
	}

	/* Get dbids from dsa */
	dbids = (Oid *) dsa_get_address(dsa, MySlotSyncWorker->dbids_dp);

	/* The syscache access needs a transaction env. */
	StartTransactionCommand();

	/* Make things live outside TX context */
	MemoryContextSwitchTo(oldctx);

	/* Construct query to get slots info from the primary */
	initStringInfo(&s);
	if (!construct_slot_query(&s, dbids))
	{
		pfree(s.data);
		CommitTransactionCommand();
		LWLockRelease(SlotSyncWorkerLock);
		return naptime;
	}

	elog(DEBUG2, "slot-sync worker%d's query:%s \n", MySlotSyncWorker->slot,
		 s.data);

	/* Execute the query */
	res = walrcv_exec(wrconn, s.data, SLOTSYNC_COLUMN_COUNT, slotRow);
	pfree(s.data);

	if (res->status != WALRCV_OK_TUPLES)
		ereport(ERROR,
				(errmsg("could not fetch slot info from the primary: %s",
						res->err)));

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

		/*
		 * It is possible to get null values for lsns and xmin if slot is
		 * invalidated on primary, so handle accordingly.
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

		remote_slot->conflicting = DatumGetBool(slot_getattr(slot, 7, &isnull));
		Assert(!isnull);

		remote_slot->database = TextDatumGetCString(slot_getattr(slot,
																 8, &isnull));
		Assert(!isnull);

		if (remote_slot->conflicting)
			remote_slot->invalidated = get_remote_invalidation_cause(wrconn,
																	 remote_slot->name);
		else
			remote_slot->invalidated = RS_INVAL_NONE;

		/* Create list of remote slot names to be used by drop_obsolete_slots */
		remote_slot_list = lappend(remote_slot_list, remote_slot);

		synchronize_one_slot(wrconn, remote_slot);

		/*
		 * Update nap time in case of non-zero value returned. The zero value
		 * is returned if remote_slot is not the one being monitored.
		 */
		value = compute_naptime(remote_slot);
		if (value)
			naptime = value;

		ExecClearTuple(slot);
	}

	/*
	 * Drop local slots which no longer need to be synced i.e. these either do
	 * not exist on primary or are no longer part of synchronize_slot_names.
	 */
	drop_obsolete_slots(dbids, remote_slot_list);

	LWLockRelease(SlotSyncWorkerLock);

	/* We are done, free remot_slot_list elements */
	foreach(cell, remote_slot_list)
	{
		RemoteSlot *remote_slot = (RemoteSlot *) lfirst(cell);

		pfree(remote_slot);
	}

	walrcv_clear_result(res);

	return naptime;
}

/*
 * Initialize the list from raw synchronize_slot_names and cache it, in order
 * to avoid parsing it repeatedly. Done at slot-sync worker startup and after
 * each SIGHUP.
 */
static void
SlotSyncInitSlotNamesList()
{
	char	   *rawname;

	if (strcmp(synchronize_slot_names, "") != 0 &&
		strcmp(synchronize_slot_names, "*") != 0)
	{
		rawname = pstrdup(synchronize_slot_names);
		SplitIdentifierString(rawname, ',', &sync_slot_names_list);
	}
}

/*
 * Connect to remote (primary) server.
 *
 * This uses primary_conninfo in order to connect to primary. For slot-sync
 * to work, primary_conninfo is expected to have dbname as well.
 */
static WalReceiverConn *
remote_connect()
{
	WalReceiverConn *wrconn = NULL;
	char	   *err;

	wrconn = walrcv_connect(PrimaryConnInfo, true, false, "slot-sync", &err);
	if (wrconn == NULL)
		ereport(ERROR,
				(errmsg("could not connect to the primary server: %s", err)));
	return wrconn;
}

/*
 * Reconnect to remote (primary) server if PrimaryConnInfo got changed.
 */
static WalReceiverConn *
reconnect_if_needed(WalReceiverConn *wrconn_prev, char *conninfo_prev)
{
	WalReceiverConn *wrconn = NULL;

	/* If no change in PrimaryConnInfo, return previous connection itself */
	if (strcmp(conninfo_prev, PrimaryConnInfo) == 0)
		return wrconn_prev;

	walrcv_disconnect(wrconn_prev);
	wrconn = remote_connect();
	return wrconn;
}

/*
 * Interrupt handler for main loop of slot-sync worker.
 */
static bool
ProcessSlotSyncInterrupts(WalReceiverConn *wrconn, char **conninfo_prev)
{
	bool		reload_done = false;

	CHECK_FOR_INTERRUPTS();

	if (ShutdownRequestPending)
	{
		elog(LOG, "Replication slot-sync worker %d is shutting"
			 " down on receiving SIGINT", MySlotSyncWorker->slot);

		/*
		 * TODO: we need to take care of dropping the slots belonging to dbids
		 * of this worker before exiting, for the case when all the dbids of
		 * this worker are obsoleted/dropped on primary and that is the reason
		 * for this worker's exit.
		 */
		walrcv_disconnect(wrconn);
		proc_exit(0);
	}

	if (ConfigReloadPending)
	{
		ConfigReloadPending = false;

		/* Save the PrimaryConnInfo before reloading */
		*conninfo_prev = pstrdup(PrimaryConnInfo);

		ProcessConfigFile(PGC_SIGHUP);
		SlotSyncInitSlotNamesList();
		reload_done = true;
	}

	return reload_done;
}

/*
 * Detach the worker from DSM and update 'proc' and 'in_use'.
 * Logical replication launcher will come to know using these
 * that the worker has shutdown.
 */
static void
slotsync_worker_detach(int code, Datum arg)
{
	dsa_detach((dsa_area *) DatumGetPointer(arg));
	LWLockAcquire(SlotSyncWorkerLock, LW_EXCLUSIVE);
	MySlotSyncWorker->hdr.in_use = false;
	MySlotSyncWorker->hdr.proc = NULL;
	LWLockRelease(SlotSyncWorkerLock);
}

/*
 * The main loop of our worker process.
 */
void
ReplSlotSyncWorkerMain(Datum main_arg)
{
	int			worker_slot = DatumGetInt32(main_arg);
	dsa_handle	handle;
	dsa_area   *dsa;
	WalReceiverConn *wrconn = NULL;
	char	   *dbname;

	/* Setup signal handling */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/*
	 * Attach to the dynamic shared memory segment for the slot-sync worker
	 * and find its table of contents.
	 */
	memcpy(&handle, MyBgworkerEntry->bgw_extra, sizeof(dsa_handle));
	dsa = dsa_attach(handle);
	if (!dsa)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not map dynamic shared memory "
						"segment for slot-sync worker")));

	/* Primary initialization is complete. Now attach to our slot. */
	slotsync_worker_attach(worker_slot);

	elog(LOG, "Replication slot-sync worker %d started", worker_slot);

	before_shmem_exit(slotsync_worker_detach, PointerGetDatum(dsa));

	/* Load the libpq-specific functions */
	load_file("libpqwalreceiver", false);

	/*
	 * Get the user provided dbname from the connection string, if dbname not
	 * provided, skip sync.
	 */
	dbname = walrcv_get_dbname_from_conninfo(PrimaryConnInfo);
	if (dbname == NULL)
		proc_exit(0);

	/*
	 * Connect to the database specified by user in PrimaryConnInfo. We need
	 * database connection for walrcv_exec to work. Please see comments atop
	 * libpqrcv_exec.
	 */
	BackgroundWorkerInitializeConnection(dbname,
										 NULL,
										 0);
	SlotSyncInitSlotNamesList();

	/* Connect to primary node */
	wrconn = remote_connect();

	/* Main wait loop. */
	for (;;)
	{
		int			rc;
		long		naptime;
		bool		config_reloaded = false;
		char	   *conninfo_prev;

		config_reloaded = ProcessSlotSyncInterrupts(wrconn, &conninfo_prev);

		/* Reconnect if primary_conninfo got changed */
		if (config_reloaded)
			wrconn = reconnect_if_needed(wrconn, conninfo_prev);

		if (!RecoveryInProgress())
			proc_exit(0);

		naptime = synchronize_slots(dsa, wrconn);

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   naptime,
					   WAIT_EVENT_REPL_SLOTSYNC_MAIN);

		ResetLatch(MyLatch);

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
		{
			walrcv_disconnect(wrconn);
			proc_exit(1);
		}
	}

	/*
	 * The slot-sync worker must not get here because it will only stop when
	 * it receives a SIGINT from the logical replication launcher, or when
	 * there is an error. None of these cases will allow the code to reach
	 * here.
	 */
	Assert(false);
}
