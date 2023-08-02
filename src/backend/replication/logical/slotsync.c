/*-------------------------------------------------------------------------
 * slotsync.c
 *	   PostgreSQL worker for synchronizing slots to a standby from primary
 *
 * Copyright (c) 2016-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/slotsync.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/dbcommands.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "replication/logical.h"
#include "replication/logicallauncher.h"
#include "replication/logicalworker.h"
#include "replication/walreceiver.h"
#include "replication/worker_internal.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/guc_hooks.h"
#include "utils/pg_lsn.h"
#include "utils/varlena.h"

typedef struct RemoteSlot
{
	char *name;
	char *plugin;
	char *database;
	XLogRecPtr restart_lsn;
	XLogRecPtr confirmed_lsn;
	TransactionId catalog_xmin;
} RemoteSlot;

/*
 * Wait for remote slot to pass localy reserved position.
 */
static void
wait_for_primary_slot_catchup(WalReceiverConn *wrconn, char *slot_name,
							  XLogRecPtr min_lsn)
{
	WalRcvExecResult *res;
	TupleTableSlot *slot;
	Oid			slotRow[1] = {LSNOID};
	StringInfoData cmd;
	bool		isnull;
	XLogRecPtr	restart_lsn;

	for (;;)
	{
		int			rc;

		CHECK_FOR_INTERRUPTS();

		initStringInfo(&cmd);
		appendStringInfo(&cmd,
						 "SELECT restart_lsn"
						 "  FROM pg_catalog.pg_replication_slots"
						 " WHERE slot_name = %s",
						 quote_literal_cstr(slot_name));
		res = walrcv_exec(wrconn, cmd.data, 1, slotRow);

		if (res->status != WALRCV_OK_TUPLES)
			ereport(ERROR,
					(errmsg("could not fetch slot info for slot \"%s\" from primary: %s",
							slot_name, res->err)));

		slot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
		if (!tuplestore_gettupleslot(res->tuplestore, true, false, slot))
			ereport(ERROR,
					(errmsg("slot \"%s\" disapeared from provider",
							slot_name)));

		restart_lsn = DatumGetLSN(slot_getattr(slot, 1, &isnull));
		Assert(!isnull);

		ExecClearTuple(slot);
		walrcv_clear_result(res);

		if (restart_lsn >= min_lsn)
			break;

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   wal_retrieve_retry_interval,
					   WAIT_EVENT_REPL_SLOT_SYNC_MAIN);

		ResetLatch(MyLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}
}

/*
 * Advance local slot to remote_slot's positions
 */
static void
local_slot_advance(RemoteSlot *remote_slot)
{
	LogicalConfirmReceivedLocation(remote_slot->confirmed_lsn);
	LogicalIncreaseXminForSlot(remote_slot->confirmed_lsn,
							   remote_slot->catalog_xmin);
	LogicalIncreaseRestartDecodingForSlot(remote_slot->confirmed_lsn,
										  remote_slot->restart_lsn);
	ReplicationSlotMarkDirty();
}

/*
 * Synchronize single slot to given position.
 *
 * This optionally creates new slot if there is no existing one.
 */
static void
synchronize_one_slot(WalReceiverConn *wrconn, RemoteSlot *remote_slot)
{
	bool		found = false;

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
			elog(DEBUG1,
				 "not synchronizing slot %s; synchronization would move it backward",
				 remote_slot->name);

			ReplicationSlotRelease();
			CommitTransactionCommand();
			return;
		}

		/* advance current lsns of slot to remote slot's current position */
		local_slot_advance(remote_slot);
		ReplicationSlotSave();
	}
	/* Otherwise create the slot first. */
	else
	{
		TransactionId xmin_horizon = InvalidTransactionId;
		ReplicationSlot *slot;

		ReplicationSlotCreate(remote_slot->name, true, RS_EPHEMERAL, false);
		slot = MyReplicationSlot;

		SpinLockAcquire(&slot->mutex);
		slot->data.database = get_database_oid(remote_slot->database, false);
		namestrcpy(&slot->data.plugin, remote_slot->plugin);
		SpinLockRelease(&slot->mutex);

		ReplicationSlotReserveWal();

		LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
		xmin_horizon = GetOldestSafeDecodingTransactionId(true);
		slot->effective_catalog_xmin = xmin_horizon;
		slot->data.catalog_xmin = xmin_horizon;
		ReplicationSlotsComputeRequiredXmin(true);
		LWLockRelease(ProcArrayLock);

		if (remote_slot->confirmed_lsn < MyReplicationSlot->data.restart_lsn)
		{
			ereport(LOG,
					errmsg("waiting for remote slot \"%s\" LSN (%X/%X) to pass local slot LSN (%X/%X)",
						   remote_slot->name,
						   LSN_FORMAT_ARGS(remote_slot->confirmed_lsn), LSN_FORMAT_ARGS(MyReplicationSlot->data.restart_lsn)));

			wait_for_primary_slot_catchup(wrconn, remote_slot->name,
										  MyReplicationSlot->data.restart_lsn);
		}


		/* advance current lsns of slot to remote slot's current position */
		local_slot_advance(remote_slot);
		ReplicationSlotPersist();
	}

	ReplicationSlotRelease();
	CommitTransactionCommand();
}

static void
synchronize_slots(void)
{
	WalRcvExecResult *res;
	WalReceiverConn *wrconn = NULL;
	TupleTableSlot *slot;
	Oid			slotRow[6] = {TEXTOID, TEXTOID, LSNOID, LSNOID, XIDOID, TEXTOID};
	StringInfoData s;
	char	   *database;
	char	   *err;
	int        i;
	MemoryContext oldctx = CurrentMemoryContext;

	if (!WalRcv)
		return;

	/* syscache access needs a transaction env. */
	StartTransactionCommand();
	/* make dbname live outside TX context */
	MemoryContextSwitchTo(oldctx);

	database = get_database_name(MyDatabaseId);
	initStringInfo(&s);
	appendStringInfo(&s, "%s dbname=%s", PrimaryConnInfo, database);
	wrconn = walrcv_connect(s.data, true, false, "slot_sync", &err);

	if (wrconn == NULL)
		ereport(ERROR,
				(errmsg("could not connect to the primary server: %s", err)));

	resetStringInfo(&s);
	appendStringInfo(&s,
					 "SELECT slot_name, plugin, confirmed_flush_lsn,"
					 " restart_lsn, catalog_xmin, database"
					 "  FROM pg_catalog.pg_replication_slots"
					 " WHERE database IN ");

	Assert (MySlotSyncWorker->dbcount);

	appendStringInfoChar(&s, '(');
	for (i = 0; i < MySlotSyncWorker->dbcount; i++)
	{
		char	   *dbname;
		if (i != 0)
			appendStringInfoChar(&s, ',');

		dbname = get_database_name(MySlotSyncWorker->dbids[i]);
		appendStringInfo(&s, "%s",
						 quote_literal_cstr(dbname));
		pfree(dbname);
	}
	appendStringInfoChar(&s, ')');

	if (strcmp(synchronize_slot_names, "") != 0 && strcmp(synchronize_slot_names, "*") != 0)
	{
		char	   *rawname;
		List	   *namelist;
		ListCell   *lc;

		rawname = pstrdup(synchronize_slot_names);
		SplitIdentifierString(rawname, ',', &namelist);

		appendStringInfoString(&s, " AND slot_name IN (");
		foreach (lc, namelist)
		{
			if (lc != list_head(namelist))
				appendStringInfoChar(&s, ',');
			appendStringInfo(&s, "%s",
							 quote_literal_cstr(lfirst(lc)));
		}
		appendStringInfoChar(&s, ')');
	}

	res = walrcv_exec(wrconn, s.data, 6, slotRow);
	pfree(s.data);

	if (res->status != WALRCV_OK_TUPLES)
		ereport(ERROR,
				(errmsg("could not fetch slot info from primary: %s",
						res->err)));

	CommitTransactionCommand();
	/* CommitTransactionCommand switches to TopMemoryContext */
	MemoryContextSwitchTo(oldctx);

	slot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
	while (tuplestore_gettupleslot(res->tuplestore, true, false, slot))
	{
		bool		isnull;
		RemoteSlot *remote_slot = palloc0(sizeof(RemoteSlot));

		remote_slot->name = TextDatumGetCString(slot_getattr(slot, 1, &isnull));
		Assert(!isnull);

		remote_slot->plugin = TextDatumGetCString(slot_getattr(slot, 2, &isnull));
		Assert(!isnull);

		remote_slot->confirmed_lsn = DatumGetLSN(slot_getattr(slot, 3, &isnull));
		Assert(!isnull);

		remote_slot->restart_lsn = DatumGetLSN(slot_getattr(slot, 4, &isnull));
		Assert(!isnull);

		remote_slot->catalog_xmin = DatumGetTransactionId(slot_getattr(slot, 5, &isnull));
		Assert(!isnull);

		remote_slot->database = TextDatumGetCString(slot_getattr(slot, 6, &isnull));
		Assert(!isnull);

		synchronize_one_slot(wrconn, remote_slot);
		pfree(remote_slot);

		ExecClearTuple(slot);
	}

	walrcv_clear_result(res);
	pfree(database);

	walrcv_disconnect(wrconn);
}

/*
 * The main loop of our worker process.
 */
void
ReplSlotSyncMain(Datum main_arg)
{
	int			worker_slot = DatumGetInt32(main_arg);

	/* Attach to slot */
	slotsync_worker_attach(worker_slot);

	/* Establish signal handlers. */
	BackgroundWorkerUnblockSignals();

	/* Load the libpq-specific functions */
	load_file("libpqwalreceiver", false);

	/* Connect to our database. */
	BackgroundWorkerInitializeConnectionByOid(MySlotSyncWorker->dbid,
											  MySlotSyncWorker->userid,
											  0);

	StartTransactionCommand();
	ereport(LOG,
			(errmsg("replication slot synchronization worker %d for database \"%s\" has started",
					worker_slot, get_database_name(MySlotSyncWorker->dbid))));
	CommitTransactionCommand();

	/* Main wait loop. */
	for (;;)
	{
		int			rc;

		CHECK_FOR_INTERRUPTS();

		if (!RecoveryInProgress())
			return;

		if (strcmp(synchronize_slot_names, "") == 0)
			return;

		synchronize_slots();

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   wal_retrieve_retry_interval,
					   WAIT_EVENT_REPL_SLOT_SYNC_MAIN);

		ResetLatch(MyLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}
}
