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
#include "replication/logicalworker.h"
#include "replication/walreceiver.h"
#include "replication/worker_internal.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/varlena.h"

char	   *synchronize_slot_names;
char	   *standby_slot_names;

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
 * Synchronize single slot to given position.
 *
 * This optionally creates new slot if there is no existing one.
 */
static void
synchronize_one_slot(WalReceiverConn *wrconn, char *slot_name, char *database,
					 char *plugin_name, XLogRecPtr target_lsn)
{
	bool		found = false;
	XLogRecPtr	endlsn;

	/* Search for the named slot and mark it active if we find it. */
	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
	for (int i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

		if (!s->in_use)
			continue;

		if (strcmp(NameStr(s->data.name), slot_name) == 0)
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
		ReplicationSlotAcquire(slot_name, true);

		if (target_lsn < MyReplicationSlot->data.confirmed_flush)
		{
			elog(DEBUG1,
				 "not synchronizing slot %s; synchronization would move it backward",
				 slot_name);

			ReplicationSlotRelease();
			CommitTransactionCommand();
			return;
		}
	}
	/* Otherwise create the slot first. */
	else
	{
		TransactionId xmin_horizon = InvalidTransactionId;
		ReplicationSlot *slot;

		ReplicationSlotCreate(slot_name, true, RS_EPHEMERAL, false);
		slot = MyReplicationSlot;

		SpinLockAcquire(&slot->mutex);
		slot->data.database = get_database_oid(database, false);
		namestrcpy(&slot->data.plugin, plugin_name);
		SpinLockRelease(&slot->mutex);

		ReplicationSlotReserveWal();

		LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
		xmin_horizon = GetOldestSafeDecodingTransactionId(true);
		slot->effective_catalog_xmin = xmin_horizon;
		slot->data.catalog_xmin = xmin_horizon;
		ReplicationSlotsComputeRequiredXmin(true);
		LWLockRelease(ProcArrayLock);

		if (target_lsn < MyReplicationSlot->data.restart_lsn)
		{
			ereport(LOG,
					errmsg("waiting for remote slot \"%s\" LSN (%X/%X) to pass local slot LSN (%X/%X)",
						   slot_name,
						   LSN_FORMAT_ARGS(target_lsn), LSN_FORMAT_ARGS(MyReplicationSlot->data.restart_lsn)));

			wait_for_primary_slot_catchup(wrconn, slot_name,
										  MyReplicationSlot->data.restart_lsn);
		}

		ReplicationSlotPersist();
	}

	endlsn = pg_logical_replication_slot_advance(target_lsn);

	elog(DEBUG3, "synchronized slot %s to lsn (%X/%X)",
		 slot_name, LSN_FORMAT_ARGS(endlsn));

	ReplicationSlotRelease();
	CommitTransactionCommand();
}

static void
synchronize_slots(void)
{
	WalRcvExecResult *res;
	WalReceiverConn *wrconn = NULL;
	TupleTableSlot *slot;
	Oid			slotRow[3] = {TEXTOID, TEXTOID, LSNOID};
	StringInfoData s;
	char	   *database;
	char	   *err;
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
	wrconn = walrcv_connect(s.data, true, "slot_sync", &err);

	if (wrconn == NULL)
		ereport(ERROR,
				(errmsg("could not connect to the primary server: %s", err)));

	resetStringInfo(&s);
	appendStringInfo(&s,
					 "SELECT slot_name, plugin, confirmed_flush_lsn"
					 "  FROM pg_catalog.pg_replication_slots"
					 " WHERE database = %s",
					 quote_literal_cstr(database));
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

	res = walrcv_exec(wrconn, s.data, 3, slotRow);
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
		char	   *slot_name;
		char	   *plugin_name;
		XLogRecPtr	confirmed_flush_lsn;
		bool		isnull;

		slot_name = TextDatumGetCString(slot_getattr(slot, 1, &isnull));
		Assert(!isnull);

		plugin_name = TextDatumGetCString(slot_getattr(slot, 2, &isnull));
		Assert(!isnull);

		confirmed_flush_lsn = DatumGetLSN(slot_getattr(slot, 3, &isnull));
		Assert(!isnull);

		synchronize_one_slot(wrconn, slot_name, database, plugin_name,
							 confirmed_flush_lsn);

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
	logicalrep_worker_attach(worker_slot);

	/* Establish signal handlers. */
	BackgroundWorkerUnblockSignals();

	/* Load the libpq-specific functions */
	load_file("libpqwalreceiver", false);

	/* Connect to our database. */
	BackgroundWorkerInitializeConnectionByOid(MyLogicalRepWorker->dbid,
											  MyLogicalRepWorker->userid,
											  0);

	StartTransactionCommand();
	ereport(LOG,
			(errmsg("replication slot synchronization worker for database \"%s\" has started",
					get_database_name(MyLogicalRepWorker->dbid))));
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

/*
 * Routines for handling the GUC variable(s)
 */

bool
check_synchronize_slot_names(char **newval, void **extra, GucSource source)
{
	/* Special handling for "*" which means all. */
	if (strcmp(*newval, "*") == 0)
	{
		return true;
	}
	else
	{
		char	   *rawname;
		List	   *namelist;
		ListCell   *lc;

		/* Need a modifiable copy of string */
		rawname = pstrdup(*newval);

		/* Parse string into list of identifiers */
		if (!SplitIdentifierString(rawname, ',', &namelist))
		{
			/* syntax error in name list */
			GUC_check_errdetail("List syntax is invalid.");
			pfree(rawname);
			list_free(namelist);
			return false;
		}

		foreach(lc, namelist)
		{
			char	   *curname = (char *) lfirst(lc);

			ReplicationSlotValidateName(curname, ERROR);
		}

		pfree(rawname);
		list_free(namelist);
	}

	return true;
}


bool
check_standby_slot_names(char **newval, void **extra, GucSource source)
{
	char	   *rawname;
	List	   *namelist;
	ListCell   *lc;

	/* Need a modifiable copy of string */
	rawname = pstrdup(*newval);

	/* Parse string into list of identifiers */
	if (!SplitIdentifierString(rawname, ',', &namelist))
	{
		/* syntax error in name list */
		GUC_check_errdetail("List syntax is invalid.");
		pfree(rawname);
		list_free(namelist);
		return false;
	}

	foreach(lc, namelist)
	{
		char	   *curname = (char *) lfirst(lc);

		ReplicationSlotValidateName(curname, ERROR);
	}

	pfree(rawname);
	list_free(namelist);

	return true;
}
