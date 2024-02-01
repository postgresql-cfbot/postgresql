/*-------------------------------------------------------------------------
 * slotsync.c
 *	   PostgreSQL worker for synchronizing slots to a standby server from the
 *         primary server.
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/slotsync.c
 *
 * This file contains the code for slot sync worker on a physical standby
 * to fetch logical failover slots information from the primary server,
 * create the slots on the standby and synchronize them periodically.
 *
 * While creating the slot on physical standby, if the local restart_lsn and/or
 * local catalog_xmin is ahead of those on the remote then the worker cannot
 * create the local slot in sync with the primary server because that would
 * mean moving the local slot backwards and the standby might not have WALs
 * retained for old LSN. In this case, the worker will mark the slot as
 * RS_TEMPORARY. Once the primary server catches up, the worker will mark the
 * slot as RS_PERSISTENT (which means sync-ready) and will perform the sync
 * periodically.
 *
 * The worker also takes care of dropping the slots which were created by it
 * and are currently not needed to be synchronized.
 *
 * It waits for a period of time before the next synchronization, with the
 * duration varying based on whether any slots were updated during the last
 * cycle. Refer to the comments above wait_for_slot_activity() for more details.
 *
 * Slot synchronization is currently not supported on the cascading standby.
 *---------------------------------------------------------------------------
 */

#include "postgres.h"

#include <time.h>

#include "access/genam.h"
#include "access/table.h"
#include "access/xlog_internal.h"
#include "access/xlogrecovery.h"
#include "catalog/pg_database.h"
#include "commands/dbcommands.h"
#include "libpq/pqsignal.h"
#include "pgstat.h"
#include "postmaster/interrupt.h"
#include "postmaster/bgworker.h"
#include "postmaster/fork_process.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "replication/logical.h"
#include "replication/logicallauncher.h"
#include "replication/logicalworker.h"
#include "replication/walreceiver.h"
#include "replication/worker_internal.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc_hooks.h"
#include "utils/pg_lsn.h"
#include "utils/ps_status.h"
#include "utils/timeout.h"
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
 * Slot sync worker's pid is needed by the startup process in order to
 * shut it down during promotion. Startup process shuts down the slot
 * sync worker and also sets stopSignaled=true to handle the race condition
 * when postmaster has not noticed the promotion yet and thus may end up
 * restarting slot sync worker. If stopSignaled is set, the worker will
 * exit in such a case.
 *
 * The last_start_time is needed by postmaster to start the slot sync
 * worker once per SLOTSYNC_RESTART_INTERVAL_SEC. In cases where a
 * immediate restart is expected (e.g., slot sync GUCs change), slot
 * sync worker will reset last_start_time before exiting, so that postmaster
 * can start the worker without waiting for SLOTSYNC_RESTART_INTERVAL_SEC.
 */
typedef struct SlotSyncWorkerCtxStruct
{
	pid_t		pid;
	bool		stopSignaled;
	time_t		last_start_time;
	slock_t		mutex;
} SlotSyncWorkerCtxStruct;

SlotSyncWorkerCtxStruct *SlotSyncWorker = NULL;

/* GUC variable */
bool		enable_syncslot = false;

/*
 * The sleep time (ms) between slot-sync cycles varies dynamically
 * (within a MIN/MAX range) according to slot activity. See
 * wait_for_slot_activity() for details.
 */
#define MIN_WORKER_NAPTIME_MS  200
#define MAX_WORKER_NAPTIME_MS  30000	/* 30s */
static long sleep_ms = MIN_WORKER_NAPTIME_MS;

/* The restart interval for slot sync work used by postmaster */
#define SLOTSYNC_RESTART_INTERVAL_SEC 10

/* Flag to tell if we are in a slot sync worker process */
static bool am_slotsync_worker = false;

static void ProcessSlotSyncInterrupts(WalReceiverConn *wrconn, bool restart);

#ifdef EXEC_BACKEND
static pid_t slotsyncworker_forkexec(void);
#endif
NON_EXEC_STATIC void ReplSlotSyncWorkerMain(int argc, char *argv[]) pg_attribute_noreturn();

/*
 * If necessary, update local slot metadata based on the data from the remote
 * slot.
 *
 * If no update was needed (the data of the remote slot is the same as the
 * local slot) return false, otherwise true.
 */
static bool
local_slot_update(RemoteSlot *remote_slot, Oid remote_dbid)
{
	ReplicationSlot *slot = MyReplicationSlot;
	NameData	plugin_name;

	Assert(slot->data.invalidated == RS_INVAL_NONE);

	if (strcmp(remote_slot->plugin, NameStr(slot->data.plugin)) == 0 &&
		remote_dbid == slot->data.database &&
		remote_slot->restart_lsn == slot->data.restart_lsn &&
		remote_slot->catalog_xmin == slot->data.catalog_xmin &&
		remote_slot->two_phase == slot->data.two_phase &&
		remote_slot->failover == slot->data.failover &&
		remote_slot->confirmed_lsn == slot->data.confirmed_flush)
		return false;

	/* Avoid expensive operations while holding a spinlock. */
	namestrcpy(&plugin_name, remote_slot->plugin);

	SpinLockAcquire(&slot->mutex);
	slot->data.plugin = plugin_name;
	slot->data.database = remote_dbid;
	slot->data.two_phase = remote_slot->two_phase;
	slot->data.failover = remote_slot->failover;
	slot->data.restart_lsn = remote_slot->restart_lsn;
	slot->data.confirmed_flush = remote_slot->confirmed_lsn;
	slot->data.catalog_xmin = remote_slot->catalog_xmin;
	slot->effective_catalog_xmin = remote_slot->catalog_xmin;
	SpinLockRelease(&slot->mutex);

	if (remote_slot->catalog_xmin != slot->data.catalog_xmin)
		ReplicationSlotsComputeRequiredXmin(false);

	if (remote_slot->restart_lsn != slot->data.restart_lsn)
		ReplicationSlotsComputeRequiredLSN();

	return true;
}

/*
 * Get list of local logical slots which are synchronized from
 * the primary server.
 */
static List *
get_local_synced_slots(void)
{
	List	   *local_slots = NIL;

	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

	for (int i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

		/* Check if it is a synchronized slot */
		if (s->in_use && s->data.synced)
		{
			Assert(SlotIsLogical(s));
			local_slots = lappend(local_slots, s);
		}
	}

	LWLockRelease(ReplicationSlotControlLock);

	return local_slots;
}

/*
 * Helper function to check if local_slot is present in remote_slots list.
 *
 * It also checks if the slot on the standby server was invalidated while the
 * corresponding remote slot in the list remained valid. If found so, it sets
 * the locally_invalidated flag to true.
 */
static bool
check_sync_slot_on_remote(ReplicationSlot *local_slot, List *remote_slots,
						  bool *locally_invalidated)
{
	foreach_ptr(RemoteSlot, remote_slot, remote_slots)
	{
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
 * Additionally, it drops slots that are valid on the primary but got
 * invalidated on the standby. This situation may occur due to the following
 * reasons:
 * - The max_slot_wal_keep_size on the standby is insufficient to retain WAL
 *   records from the restart_lsn of the slot.
 * - primary_slot_name is temporarily reset to null and the physical slot is
 *   removed.
 * - The primary changes wal_level to a level lower than logical.
 *
 * The assumption is that these dropped slots will get recreated in next
 * sync-cycle and it is okay to drop and recreate such slots as long as these
 * are not consumable on the standby (which is the case currently).
 */
static void
drop_obsolete_slots(List *remote_slot_list)
{
	List	   *local_slots = get_local_synced_slots();

	foreach_ptr(ReplicationSlot, local_slot, local_slots)
	{
		bool		remote_exists = false;
		bool		locally_invalidated = false;

		remote_exists = check_sync_slot_on_remote(local_slot, remote_slot_list,
												  &locally_invalidated);

		/*
		 * Drop the local slot either if it is not in the remote slots list or
		 * is invalidated while remote slot is still valid.
		 */
		if (!remote_exists || locally_invalidated)
		{
			bool		synced_slot;

			/*
			 * Use shared lock to prevent a conflict with
			 * ReplicationSlotsDropDBSlots(), trying to drop the same slot
			 * during a drop-database operation.
			 */
			LockSharedObject(DatabaseRelationId, local_slot->data.database,
							 0, AccessShareLock);

			/*
			 * There is a possibility of parallel database drop and
			 * re-creation of new slot by user in the small window between
			 * getting the slot to drop and locking the db. This new
			 * user-created slot may end up using the same shared memory as
			 * that of 'local_slot'. Thus check if local_slot is still the
			 * synced one before performing actual drop.
			 */
			SpinLockAcquire(&local_slot->mutex);
			synced_slot = local_slot->in_use && local_slot->data.synced;
			SpinLockRelease(&local_slot->mutex);
			if (synced_slot)
			{
				ReplicationSlotAcquire(NameStr(local_slot->data.name), true);
				ReplicationSlotDropAcquired();
			}

			UnlockSharedObject(DatabaseRelationId, local_slot->data.database,
							   0, AccessShareLock);

			ereport(LOG,
					errmsg("dropped replication slot \"%s\" of dbid %d",
						   NameStr(local_slot->data.name),
						   local_slot->data.database));
		}
	}
}

/*
 * Reserve WAL for the currently active slot using the specified WAL location
 * (restart_lsn).
 *
 * If the given WAL location has been removed, reserve WAL using the oldest
 * existing WAL segment.
 */
static void
reserve_wal_for_slot(XLogRecPtr restart_lsn)
{
	XLogSegNo	oldest_segno;
	XLogSegNo	segno;
	ReplicationSlot *slot = MyReplicationSlot;

	Assert(slot != NULL);
	Assert(XLogRecPtrIsInvalid(slot->data.restart_lsn));

	while (true)
	{
		SpinLockAcquire(&slot->mutex);
		slot->data.restart_lsn = restart_lsn;
		SpinLockRelease(&slot->mutex);

		/* Prevent WAL removal as fast as possible */
		ReplicationSlotsComputeRequiredLSN();

		XLByteToSeg(slot->data.restart_lsn, segno, wal_segment_size);

		/*
		 * Find the oldest existing WAL segment file.
		 *
		 * Normally, we can determine it by using the last removed segment
		 * number. However, if no WAL segment files have been removed by a
		 * checkpoint since startup, we need to search for the oldest segment
		 * file currently existing in XLOGDIR.
		 */
		oldest_segno = XLogGetLastRemovedSegno() + 1;

		if (oldest_segno == 1)
			oldest_segno = XLogGetOldestSegno(0);

		/*
		 * If all required WAL is still there, great, otherwise retry. The
		 * slot should prevent further removal of WAL, unless there's a
		 * concurrent ReplicationSlotsComputeRequiredLSN() after we've written
		 * the new restart_lsn above, so normally we should never need to loop
		 * more than twice.
		 */
		if (segno >= oldest_segno)
			break;

		/* Retry using the location of the oldest wal segment */
		XLogSegNoOffsetToRecPtr(oldest_segno, 0, wal_segment_size, restart_lsn);
	}
}

/*
 * Update the LSNs and persist the slot for further syncs if the remote
 * restart_lsn and catalog_xmin have caught up with the local ones, otherwise
 * do nothing.
 *
 * Return true if the slot is marked as RS_PERSISTENT (sync-ready), otherwise
 * false.
 */
static bool
update_and_persist_slot(RemoteSlot *remote_slot, Oid remote_dbid)
{
	ReplicationSlot *slot = MyReplicationSlot;

	/*
	 * Check if the primary server has caught up. Refer to the comment atop
	 * the file for details on this check.
	 *
	 * We also need to check if remote_slot's confirmed_lsn becomes valid. It
	 * is possible to get null values for confirmed_lsn and catalog_xmin if on
	 * the primary server the slot is just created with a valid restart_lsn
	 * and slot-sync worker has fetched the slot before the primary server
	 * could set valid confirmed_lsn and catalog_xmin.
	 */
	if (remote_slot->restart_lsn < slot->data.restart_lsn ||
		XLogRecPtrIsInvalid(remote_slot->confirmed_lsn) ||
		TransactionIdPrecedes(remote_slot->catalog_xmin,
							  slot->data.catalog_xmin))
	{
		/*
		 * The remote slot didn't catch up to locally reserved position.
		 *
		 * We do not drop the slot because the restart_lsn can be ahead of the
		 * current location when recreating the slot in the next cycle. It may
		 * take more time to create such a slot. Therefore, we keep this slot
		 * and attempt the wait and synchronization in the next cycle.
		 */
		return false;
	}

	/* First time slot update, the function must return true */
	if (!local_slot_update(remote_slot, remote_dbid))
		elog(ERROR, "failed to update slot");

	ReplicationSlotPersist();

	ereport(LOG,
			errmsg("newly created slot \"%s\" is sync-ready now",
				   remote_slot->name));

	return true;
}

/*
 * Synchronize single slot to given position.
 *
 * This creates a new slot if there is no existing one and updates the
 * metadata of the slot as per the data received from the primary server.
 *
 * The slot is created as a temporary slot and stays in the same state until the
 * the remote_slot catches up with locally reserved position and local slot is
 * updated. The slot is then persisted and is considered as sync-ready for
 * periodic syncs.
 *
 * Returns TRUE if the local slot is updated.
 */
static bool
synchronize_one_slot(RemoteSlot *remote_slot, Oid remote_dbid)
{
	ReplicationSlot *slot;
	bool		slot_updated = false;
	XLogRecPtr	latestFlushPtr;

	/*
	 * Make sure that concerned WAL is received and flushed before syncing
	 * slot to target lsn received from the primary server.
	 *
	 * This check will never pass if on the primary server, user has
	 * configured standby_slot_names GUC correctly, otherwise this can hit
	 * frequently.
	 */
	latestFlushPtr = GetStandbyFlushRecPtr(NULL);
	if (remote_slot->confirmed_lsn > latestFlushPtr)
	{
		ereport(LOG,
				errmsg("skipping slot synchronization as the received slot sync"
					   " LSN %X/%X for slot \"%s\" is ahead of the standby position %X/%X",
					   LSN_FORMAT_ARGS(remote_slot->confirmed_lsn),
					   remote_slot->name,
					   LSN_FORMAT_ARGS(latestFlushPtr)));

		return false;
	}

	/* Search for the named slot */
	if ((slot = SearchNamedReplicationSlot(remote_slot->name, true)))
	{
		bool		synced;

		SpinLockAcquire(&slot->mutex);
		synced = slot->data.synced;
		SpinLockRelease(&slot->mutex);

		/* User-created slot with the same name exists, raise ERROR. */
		if (!synced)
			ereport(ERROR,
					errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("exiting from slot synchronization because same"
						   " name slot \"%s\" already exists on the standby",
						   remote_slot->name));

		/*
		 * Slot created by the slot sync worker exists, sync it.
		 *
		 * It is important to acquire the slot here before checking
		 * invalidation. If we don't acquire the slot first, there could be a
		 * race condition that the local slot could be invalidated just after
		 * checking the 'invalidated' flag here and we could end up
		 * overwriting 'invalidated' flag to remote_slot's value. See
		 * InvalidatePossiblyObsoleteSlot() where it invalidates slot directly
		 * if the slot is not acquired by other processes.
		 */
		ReplicationSlotAcquire(remote_slot->name, true);

		Assert(slot == MyReplicationSlot);

		/*
		 * Copy the invalidation cause from remote only if local slot is not
		 * invalidated locally, we don't want to overwrite existing one.
		 */
		if (slot->data.invalidated == RS_INVAL_NONE)
		{
			SpinLockAcquire(&slot->mutex);
			slot->data.invalidated = remote_slot->invalidated;
			SpinLockRelease(&slot->mutex);

			/* Make sure the invalidated state persists across server restart */
			ReplicationSlotMarkDirty();
			ReplicationSlotSave();
			slot_updated = true;
		}

		/* Skip the sync of an invalidated slot */
		if (slot->data.invalidated != RS_INVAL_NONE)
		{
			ReplicationSlotRelease();
			return slot_updated;
		}

		/* Slot not ready yet, let's attempt to make it sync-ready now. */
		if (slot->data.persistency == RS_TEMPORARY)
		{
			slot_updated = update_and_persist_slot(remote_slot, remote_dbid);
		}

		/* Slot ready for sync, so sync it. */
		else
		{
			/*
			 * Sanity check: As long as the invalidations are handled
			 * appropriately as above, this should never happen.
			 */
			if (remote_slot->restart_lsn < slot->data.restart_lsn)
				elog(ERROR,
					 "cannot synchronize local slot \"%s\" LSN(%X/%X)"
					 " to remote slot's LSN(%X/%X) as synchronization"
					 " would move it backwards", remote_slot->name,
					 LSN_FORMAT_ARGS(slot->data.restart_lsn),
					 LSN_FORMAT_ARGS(remote_slot->restart_lsn));

			/* Make sure the slot changes persist across server restart */
			if (local_slot_update(remote_slot, remote_dbid))
			{
				slot_updated = true;
				ReplicationSlotMarkDirty();
				ReplicationSlotSave();
			}
		}
	}
	/* Otherwise create the slot first. */
	else
	{
		NameData	plugin_name;
		TransactionId xmin_horizon = InvalidTransactionId;

		/* Skip creating the local slot if remote_slot is invalidated already */
		if (remote_slot->invalidated != RS_INVAL_NONE)
			return false;

		ReplicationSlotCreate(remote_slot->name, true, RS_TEMPORARY,
							  remote_slot->two_phase,
							  remote_slot->failover,
							  true);

		/* For shorter lines. */
		slot = MyReplicationSlot;

		/* Avoid expensive operations while holding a spinlock. */
		namestrcpy(&plugin_name, remote_slot->plugin);

		SpinLockAcquire(&slot->mutex);
		slot->data.database = remote_dbid;
		slot->data.plugin = plugin_name;
		SpinLockRelease(&slot->mutex);

		reserve_wal_for_slot(remote_slot->restart_lsn);

		LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
		xmin_horizon = GetOldestSafeDecodingTransactionId(true);
		SpinLockAcquire(&slot->mutex);
		slot->effective_catalog_xmin = xmin_horizon;
		slot->data.catalog_xmin = xmin_horizon;
		SpinLockRelease(&slot->mutex);
		ReplicationSlotsComputeRequiredXmin(true);
		LWLockRelease(ProcArrayLock);

		(void) update_and_persist_slot(remote_slot, remote_dbid);
		slot_updated = true;
	}

	ReplicationSlotRelease();

	return slot_updated;
}

/*
 * Maps the pg_replication_slots.conflict_reason text value to
 * ReplicationSlotInvalidationCause enum value
 */
static ReplicationSlotInvalidationCause
get_slot_invalidation_cause(char *conflict_reason)
{
	Assert(conflict_reason);

	if (strcmp(conflict_reason, SLOT_INVAL_WAL_REMOVED_TEXT) == 0)
		return RS_INVAL_WAL_REMOVED;
	else if (strcmp(conflict_reason, SLOT_INVAL_HORIZON_TEXT) == 0)
		return RS_INVAL_HORIZON;
	else if (strcmp(conflict_reason, SLOT_INVAL_WAL_LEVEL_TEXT) == 0)
		return RS_INVAL_WAL_LEVEL;
	else
		Assert(0);

	/* Keep compiler quiet */
	return RS_INVAL_NONE;
}

/*
 * Synchronize slots.
 *
 * Gets the failover logical slots info from the primary server and updates
 * the slots locally. Creates the slots if not present on the standby.
 *
 * Returns TRUE if any of the slots gets updated in this sync-cycle.
 */
static bool
synchronize_slots(WalReceiverConn *wrconn)
{
#define SLOTSYNC_COLUMN_COUNT 9
	Oid			slotRow[SLOTSYNC_COLUMN_COUNT] = {TEXTOID, TEXTOID, LSNOID,
	LSNOID, XIDOID, BOOLOID, BOOLOID, TEXTOID, TEXTOID};

	WalRcvExecResult *res;
	TupleTableSlot *tupslot;
	StringInfoData s;
	List	   *remote_slot_list = NIL;
	bool		some_slot_updated = false;
	XLogRecPtr	latestWalEnd;

	/*
	 * The primary_slot_name is not set yet or WALs not received yet.
	 * Synchronization is not possible if the walreceiver is not started.
	 */
	latestWalEnd = GetWalRcvLatestWalEnd();
	SpinLockAcquire(&WalRcv->mutex);
	if ((WalRcv->slotname[0] == '\0') ||
		XLogRecPtrIsInvalid(latestWalEnd))
	{
		SpinLockRelease(&WalRcv->mutex);
		return false;
	}
	SpinLockRelease(&WalRcv->mutex);

	/* The syscache access in walrcv_exec() needs a transaction env. */
	StartTransactionCommand();

	initStringInfo(&s);

	/* Construct query to fetch slots with failover enabled. */
	appendStringInfo(&s,
					 "SELECT slot_name, plugin, confirmed_flush_lsn,"
					 " restart_lsn, catalog_xmin, two_phase, failover,"
					 " database, conflict_reason"
					 " FROM pg_catalog.pg_replication_slots"
					 " WHERE failover and NOT temporary");

	/* Execute the query */
	res = walrcv_exec(wrconn, s.data, SLOTSYNC_COLUMN_COUNT, slotRow);
	pfree(s.data);

	if (res->status != WALRCV_OK_TUPLES)
		ereport(ERROR,
				errmsg("could not fetch failover logical slots info from the primary server: %s",
					   res->err));

	/* Construct the remote_slot tuple and synchronize each slot locally */
	tupslot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
	while (tuplestore_gettupleslot(res->tuplestore, true, false, tupslot))
	{
		bool		isnull;
		RemoteSlot *remote_slot = palloc0(sizeof(RemoteSlot));
		Datum		d;
		int			col = 0;

		remote_slot->name = TextDatumGetCString(slot_getattr(tupslot, ++col,
															 &isnull));
		Assert(!isnull);

		remote_slot->plugin = TextDatumGetCString(slot_getattr(tupslot, ++col,
															   &isnull));
		Assert(!isnull);

		/*
		 * It is possible to get null values for LSN and Xmin if slot is
		 * invalidated on the primary server, so handle accordingly.
		 */
		d = slot_getattr(tupslot, ++col, &isnull);
		remote_slot->confirmed_lsn = isnull ? InvalidXLogRecPtr :
			DatumGetLSN(d);

		d = slot_getattr(tupslot, ++col, &isnull);
		remote_slot->restart_lsn = isnull ? InvalidXLogRecPtr : DatumGetLSN(d);

		d = slot_getattr(tupslot, ++col, &isnull);
		remote_slot->catalog_xmin = isnull ? InvalidTransactionId :
			DatumGetTransactionId(d);

		remote_slot->two_phase = DatumGetBool(slot_getattr(tupslot, ++col,
														   &isnull));
		Assert(!isnull);

		remote_slot->failover = DatumGetBool(slot_getattr(tupslot, ++col,
														  &isnull));
		Assert(!isnull);

		remote_slot->database = TextDatumGetCString(slot_getattr(tupslot,
																 ++col, &isnull));
		Assert(!isnull);

		d = slot_getattr(tupslot, ++col, &isnull);
		remote_slot->invalidated = isnull ? RS_INVAL_NONE :
			get_slot_invalidation_cause(TextDatumGetCString(d));

		/* Sanity check */
		Assert(col == SLOTSYNC_COLUMN_COUNT);

		/* Create list of remote slots */
		remote_slot_list = lappend(remote_slot_list, remote_slot);

		ExecClearTuple(tupslot);
	}

	/* Drop local slots that no longer need to be synced. */
	drop_obsolete_slots(remote_slot_list);

	/* Now sync the slots locally */
	foreach_ptr(RemoteSlot, remote_slot, remote_slot_list)
	{
		Oid			remote_dbid = get_database_oid(remote_slot->database, false);

		/*
		 * Use shared lock to prevent a conflict with
		 * ReplicationSlotsDropDBSlots(), trying to drop the same slot during
		 * a drop-database operation.
		 */
		LockSharedObject(DatabaseRelationId, remote_dbid, 0, AccessShareLock);

		some_slot_updated |= synchronize_one_slot(remote_slot, remote_dbid);

		UnlockSharedObject(DatabaseRelationId, remote_dbid, 0, AccessShareLock);
	}

	/* We are done, free remote_slot_list elements */
	list_free_deep(remote_slot_list);

	walrcv_clear_result(res);

	CommitTransactionCommand();

	return some_slot_updated;
}

/*
 * Checks the primary server info.
 *
 * Using the specified primary server connection, check whether we are a
 * cascading standby. It also validates primary_slot_name for non-cascading
 * standbys.
 */
static void
check_primary_info(WalReceiverConn *wrconn, bool *am_cascading_standby,
				   bool *primary_slot_invalid)
{
#define PRIMARY_INFO_OUTPUT_COL_COUNT 2
	WalRcvExecResult *res;
	Oid			slotRow[PRIMARY_INFO_OUTPUT_COL_COUNT] = {BOOLOID, BOOLOID};
	StringInfoData cmd;
	bool		isnull;
	TupleTableSlot *tupslot;
	bool		valid;
	bool		remote_in_recovery;

	/* The syscache access in walrcv_exec() needs a transaction env. */
	StartTransactionCommand();

	Assert(am_cascading_standby != NULL);
	Assert(primary_slot_invalid != NULL);

	*am_cascading_standby = false;	/* overwritten later if cascading */
	*primary_slot_invalid = false;	/* overwritten later if invalid */

	initStringInfo(&cmd);
	appendStringInfo(&cmd,
					 "SELECT pg_is_in_recovery(), count(*) = 1"
					 " FROM pg_catalog.pg_replication_slots"
					 " WHERE slot_type='physical' AND slot_name=%s",
					 quote_literal_cstr(PrimarySlotName));

	res = walrcv_exec(wrconn, cmd.data, PRIMARY_INFO_OUTPUT_COL_COUNT, slotRow);
	pfree(cmd.data);

	if (res->status != WALRCV_OK_TUPLES)
		ereport(ERROR,
				errmsg("could not fetch primary_slot_name \"%s\" info from the primary server: %s",
					   PrimarySlotName, res->err),
				errhint("Check if \"primary_slot_name\" is configured correctly."));

	tupslot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
	if (!tuplestore_gettupleslot(res->tuplestore, true, false, tupslot))
		elog(ERROR,
			 "failed to fetch tuple for the primary server slot specified by \"primary_slot_name\"");

	remote_in_recovery = DatumGetBool(slot_getattr(tupslot, 1, &isnull));
	Assert(!isnull);

	if (remote_in_recovery)
	{
		/* No need to check further, just set am_cascading_standby to true */
		*am_cascading_standby = true;
	}
	else
	{
		/*
		 * We are not cascading standby, thus good to proceed with
		 * primary_slot_name validity check now.
		 */
		valid = DatumGetBool(slot_getattr(tupslot, 2, &isnull));
		Assert(!isnull);

		if (!valid)
		{
			*primary_slot_invalid = true;
			ereport(LOG,
					errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("bad configuration for slot synchronization"),
			/* translator: second %s is a GUC variable name */
					errdetail("The primary server slot \"%s\" specified by \"%s\" is not valid.",
							  PrimarySlotName, "primary_slot_name"));
		}
	}

	ExecClearTuple(tupslot);
	walrcv_clear_result(res);
	CommitTransactionCommand();
}

/*
 * Returns true if all necessary GUCs for slot synchronization are set
 * appropriately, otherwise returns false.
 *
 * If all checks pass, extracts the dbname from the primary_conninfo GUC and
 * and return it in output dbname arg.
 */
static bool
validate_parameters_and_get_dbname(char **dbname)
{
	/*
	 * A physical replication slot(primary_slot_name) is required on the
	 * primary to ensure that the rows needed by the standby are not removed
	 * after restarting, so that the synchronized slot on the standby will not
	 * be invalidated.
	 */
	if (PrimarySlotName == NULL || *PrimarySlotName == '\0')
	{
		ereport(LOG,
		/* translator: %s is a GUC variable name */
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("bad configuration for slot synchronization"),
				errhint("\"%s\" must be defined.", "primary_slot_name"));
		return false;
	}

	/*
	 * hot_standby_feedback must be enabled to cooperate with the physical
	 * replication slot, which allows informing the primary about the xmin and
	 * catalog_xmin values on the standby.
	 */
	if (!hot_standby_feedback)
	{
		ereport(LOG,
		/* translator: %s is a GUC variable name */
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("bad configuration for slot synchronization"),
				errhint("\"%s\" must be enabled.", "hot_standby_feedback"));
		return false;
	}

	/*
	 * Logical decoding requires wal_level >= logical and we currently only
	 * synchronize logical slots.
	 */
	if (wal_level < WAL_LEVEL_LOGICAL)
	{
		ereport(LOG,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("bad configuration for slot synchronization"),
				errhint("\"wal_level\" must be >= logical."));
		return false;
	}

	/*
	 * The primary_conninfo is required to make connection to primary for
	 * getting slots information.
	 */
	if (PrimaryConnInfo == NULL || *PrimaryConnInfo == '\0')
	{
		ereport(LOG,
		/* translator: %s is a GUC variable name */
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("bad configuration for slot synchronization"),
				errhint("\"%s\" must be defined.", "primary_conninfo"));
		return false;
	}

	/*
	 * The slot sync worker needs a database connection for walrcv_exec to
	 * work.
	 */
	*dbname = walrcv_get_dbname_from_conninfo(PrimaryConnInfo);
	if (*dbname == NULL)
	{
		ereport(LOG,

		/*
		 * translator: 'dbname' is a specific option; %s is a GUC variable
		 * name
		 */
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("bad configuration for slot synchronization"),
				errhint("'dbname' must be specified in \"%s\".", "primary_conninfo"));
		return false;
	}

	return true;
}

/*
 * Check that all necessary GUCs for slot synchronization are set
 * appropriately. If not, sleep for MAX_WORKER_NAPTIME_MS and check again.
 * The idea is to become no-op until we get valid GUCs values.
 *
 * If all checks pass, extracts the dbname from the primary_conninfo GUC and
 * returns it.
 */
static char *
wait_for_valid_params_and_get_dbname(void)
{
	char	   *dbname;
	int			rc;

	/* Sanity check. */
	Assert(enable_syncslot);

	for (;;)
	{
		if (validate_parameters_and_get_dbname(&dbname))
			break;

		ereport(LOG, errmsg("skipping slot synchronization"));

		ProcessSlotSyncInterrupts(NULL, false);

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   MAX_WORKER_NAPTIME_MS,
					   WAIT_EVENT_REPL_SLOTSYNC_MAIN);

		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}

	return dbname;
}

/*
 * Re-read the config file.
 *
 * If any of the slot sync GUCs have changed, exit the worker and
 * let it get restarted by the postmaster. The worker to be exited for
 * restart purpose only if the caller passed restart as true.
 */
static void
slotsync_reread_config(bool restart)
{
	char	   *old_primary_conninfo = pstrdup(PrimaryConnInfo);
	char	   *old_primary_slotname = pstrdup(PrimarySlotName);
	bool		old_enable_syncslot = enable_syncslot;
	bool		old_hot_standby_feedback = hot_standby_feedback;
	bool		conninfo_changed;
	bool		primary_slotname_changed;

	Assert(enable_syncslot);

	ConfigReloadPending = false;
	ProcessConfigFile(PGC_SIGHUP);

	conninfo_changed = strcmp(old_primary_conninfo, PrimaryConnInfo) != 0;
	primary_slotname_changed = strcmp(old_primary_slotname, PrimarySlotName) != 0;
	pfree(old_primary_conninfo);
	pfree(old_primary_slotname);

	if (old_enable_syncslot != enable_syncslot)
	{
		ereport(LOG,
		/* translator: %s is a GUC variable name */
				errmsg("slot sync worker will shutdown because %s is disabled", "enable_syncslot"));
		proc_exit(0);
	}

	/* The caller instructed to skip restart */
	if (!restart)
		return;

	if (conninfo_changed ||
		primary_slotname_changed ||
		(old_hot_standby_feedback != hot_standby_feedback))
	{
		ereport(LOG,
				errmsg("slot sync worker will restart because of a parameter change"));

		/*
		 * Reset the last-start time for this worker so that the postmaster
		 * can restart it without waiting for SLOTSYNC_RESTART_INTERVAL_SEC.
		 */
		SlotSyncWorker->last_start_time = 0;

		proc_exit(0);
	}

}

/*
 * Interrupt handler for main loop of slot sync worker.
 */
static void
ProcessSlotSyncInterrupts(WalReceiverConn *wrconn, bool restart)
{
	CHECK_FOR_INTERRUPTS();

	if (ShutdownRequestPending)
	{
		if (wrconn)
			walrcv_disconnect(wrconn);
		ereport(LOG,
				errmsg("replication slot sync worker is shutting down on receiving SIGINT"));
		proc_exit(0);
	}

	if (ConfigReloadPending)
		slotsync_reread_config(restart);
}

/*
 * Cleanup function for slotsync worker.
 *
 * Called on slotsync worker exit.
 */
static void
slotsync_worker_onexit(int code, Datum arg)
{
	SpinLockAcquire(&SlotSyncWorker->mutex);
	SlotSyncWorker->pid = InvalidPid;
	SpinLockRelease(&SlotSyncWorker->mutex);
}

/*
 * Sleep for long enough that we believe it's likely that the slots on primary
 * get updated.
 *
 * If there is no slot activity the wait time between sync-cycles will double
 * (to a maximum of 30s). If there is some slot activity the wait time between
 * sync-cycles is reset to the minimum (200ms).
 */
static void
wait_for_slot_activity(bool some_slot_updated, bool recheck_primary_info)
{
	int			rc;

	if (recheck_primary_info)
	{
		/*
		 * If we are on the cascading standby or primary_slot_name configured
		 * is not valid, then we will skip the sync and take a longer nap
		 * before we can do check_primary_info() again.
		 */
		sleep_ms = MAX_WORKER_NAPTIME_MS;
	}
	else if (!some_slot_updated)
	{
		/*
		 * No slots were updated, so double the sleep time, but not beyond the
		 * maximum allowable value.
		 */
		sleep_ms = Min(sleep_ms * 2, MAX_WORKER_NAPTIME_MS);
	}
	else
	{
		/*
		 * Some slots were updated since the last sleep, so reset the sleep
		 * time.
		 */
		sleep_ms = MIN_WORKER_NAPTIME_MS;
	}

	rc = WaitLatch(MyLatch,
				   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				   sleep_ms,
				   WAIT_EVENT_REPL_SLOTSYNC_MAIN);

	if (rc & WL_LATCH_SET)
		ResetLatch(MyLatch);
}

/*
 * The main loop of our worker process.
 *
 * It connects to the primary server, fetches logical failover slots
 * information periodically in order to create and sync the slots.
 */
NON_EXEC_STATIC void
ReplSlotSyncWorkerMain(int argc, char *argv[])
{
	WalReceiverConn *wrconn = NULL;
	char	   *dbname;
	bool		am_cascading_standby;
	bool		primary_slot_invalid;
	char	   *err;
	sigjmp_buf	local_sigjmp_buf;
	StringInfoData app_name;

	am_slotsync_worker = true;

	MyBackendType = B_SLOTSYNC_WORKER;

	init_ps_display(NULL);

	SetProcessingMode(InitProcessing);

	/*
	 * Create a per-backend PGPROC struct in shared memory.  We must do this
	 * before we access any shared memory.
	 */
	InitProcess();

	/*
	 * Early initialization.
	 */
	BaseInit();

	Assert(SlotSyncWorker != NULL);

	SpinLockAcquire(&SlotSyncWorker->mutex);
	Assert(SlotSyncWorker->pid == InvalidPid);

	/*
	 * Startup process signaled the slot sync worker to stop, so if meanwhile
	 * postmaster ended up starting the worker again, exit.
	 */
	if (SlotSyncWorker->stopSignaled)
	{
		SpinLockRelease(&SlotSyncWorker->mutex);
		proc_exit(0);
	}

	/* Advertise our PID so that the startup process can kill us on promotion */
	SlotSyncWorker->pid = MyProcPid;
	SpinLockRelease(&SlotSyncWorker->mutex);

	ereport(LOG, errmsg("replication slot sync worker started"));

	on_shmem_exit(slotsync_worker_onexit, (Datum) 0);

	/* Setup signal handling */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, die);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGCHLD, SIG_DFL);

	/*
	 * Establishes SIGALRM handler and initialize timeout module. It is needed
	 * by InitPostgres to register different timeouts.
	 */
	InitializeTimeouts();

	/* Load the libpq-specific functions */
	load_file("libpqwalreceiver", false);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * We just need to clean up, report the error, and go away.
	 *
	 * If we do not have this handling here, then since this worker process
	 * operates at the bottom of the exception stack, ERRORs turn into FATALs.
	 * Therefore, we create our own exception handler to catch ERRORs.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * We can now go away.  Note that because we called InitProcess, a
		 * callback was registered to do ProcKill, which will clean up
		 * necessary state.
		 */
		proc_exit(0);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	dbname = wait_for_valid_params_and_get_dbname();

	/*
	 * Connect to the database specified by user in primary_conninfo. We need
	 * a database connection for walrcv_exec to work. Please see comments atop
	 * libpqrcv_exec.
	 */
	InitPostgres(dbname, InvalidOid, NULL, InvalidOid, 0, NULL);

	SetProcessingMode(NormalProcessing);

	initStringInfo(&app_name);
	if (cluster_name[0])
		appendStringInfo(&app_name, "%s_%s", cluster_name, "slotsyncworker");
	else
		appendStringInfo(&app_name, "%s", "slotsyncworker");

	/*
	 * Establish the connection to the primary server for slots
	 * synchronization.
	 */
	wrconn = walrcv_connect(PrimaryConnInfo, false, false, false,
							app_name.data,
							&err);
	pfree(app_name.data);

	if (!wrconn)
		ereport(ERROR,
				errcode(ERRCODE_CONNECTION_FAILURE),
				errmsg("could not connect to the primary server: %s", err));

	/*
	 * Using the specified primary server connection, check whether we are
	 * cascading standby and validates primary_slot_name for
	 * non-cascading-standbys.
	 */
	check_primary_info(wrconn, &am_cascading_standby, &primary_slot_invalid);

	/* Main wait loop */
	for (;;)
	{
		bool		some_slot_updated = false;
		bool		recheck_primary_info = am_cascading_standby || primary_slot_invalid;

		ProcessSlotSyncInterrupts(wrconn, true);

		if (!recheck_primary_info)
			some_slot_updated = synchronize_slots(wrconn);
		else if (primary_slot_invalid)
			ereport(LOG, errmsg("skipping slot synchronization"));

		wait_for_slot_activity(some_slot_updated, recheck_primary_info);

		/*
		 * If the standby was promoted then what was previously a cascading
		 * standby might no longer be one, so recheck each time.
		 */
		if (recheck_primary_info)
			check_primary_info(wrconn, &am_cascading_standby, &primary_slot_invalid);
	}

	/*
	 * The slot sync worker can not get here because it will only stop when it
	 * receives a SIGINT from the startup process, or when there is an error.
	 */
	Assert(false);
}

/*
 * Is current process the slot sync worker?
 */
bool
IsLogicalSlotSyncWorker(void)
{
	return am_slotsync_worker;
}

/*
 * Shut down the slot sync worker.
 */
void
ShutDownSlotSync(void)
{
	SpinLockAcquire(&SlotSyncWorker->mutex);

	SlotSyncWorker->stopSignaled = true;

	if (SlotSyncWorker->pid == InvalidPid)
	{
		SpinLockRelease(&SlotSyncWorker->mutex);
		return;
	}
	SpinLockRelease(&SlotSyncWorker->mutex);

	kill(SlotSyncWorker->pid, SIGINT);

	/* Wait for it to die */
	for (;;)
	{
		int			rc;

		/* Wait a bit, we don't expect to have to wait long */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   10L, WAIT_EVENT_REPL_SLOTSYNC_SHUTDOWN);

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}

		SpinLockAcquire(&SlotSyncWorker->mutex);

		/* Is it gone? */
		if (SlotSyncWorker->pid == InvalidPid)
			break;

		SpinLockRelease(&SlotSyncWorker->mutex);
	}

	SpinLockRelease(&SlotSyncWorker->mutex);
}

/*
 * Allocate and initialize slot sync worker shared memory
 */
void
SlotSyncWorkerShmemInit(void)
{
	Size		size;
	bool		found;

	size = sizeof(SlotSyncWorkerCtxStruct);
	size = MAXALIGN(size);

	SlotSyncWorker = (SlotSyncWorkerCtxStruct *)
		ShmemInitStruct("Slot Sync Worker Data", size, &found);

	if (!found)
	{
		memset(SlotSyncWorker, 0, size);
		SlotSyncWorker->pid = InvalidPid;
		SpinLockInit(&SlotSyncWorker->mutex);
	}
}

#ifdef EXEC_BACKEND
/*
 * The forkexec routine for the slot sync worker process.
 *
 * Format up the arglist, then fork and exec.
 */
static pid_t
slotsyncworker_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkssworker";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}
#endif

/*
 * SlotSyncWorkerCanRestart
 *
 * Returns true if the worker is allowed to restart if enough time has
 * passed (SLOTSYNC_RESTART_INTERVAL_SEC) since it was launched last.
 * Otherwise returns false.
 *
 * This is a safety valve to protect against continuous respawn attempts if the
 * worker is dying immediately at launch. Note that since we will retry to
 * launch the worker from the postmaster main loop, we will get another
 * chance later.
 */
bool
SlotSyncWorkerCanRestart(void)
{
	time_t		curtime = time(NULL);

	/* Return false if too soon since last start. */
	if ((unsigned int) (curtime - SlotSyncWorker->last_start_time) <
		(unsigned int) SLOTSYNC_RESTART_INTERVAL_SEC)
		return false;

	SlotSyncWorker->last_start_time = curtime;

	return true;
}

/*
 * Main entry point for slot sync worker process, to be called from the
 * postmaster.
 */
int
StartSlotSyncWorker(void)
{
	pid_t		pid;

#ifdef EXEC_BACKEND
	switch ((pid = slotsyncworker_forkexec()))
	{
#else
	switch ((pid = fork_process()))
	{
		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			ReplSlotSyncWorkerMain(0, NULL);
			break;
#endif
		case -1:
			ereport(LOG,
					(errmsg("could not fork slot sync worker process: %m")));
			return 0;

		default:
			return (int) pid;
	}

	/* shouldn't get here */
	return 0;
}
