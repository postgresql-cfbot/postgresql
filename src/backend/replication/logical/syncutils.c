/*-------------------------------------------------------------------------
 * syncutils.c
 *	  PostgreSQL logical replication: common synchronization code
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/syncutils.c
 *
 * NOTES
 *	  This file contains code common to table synchronization workers, and
 *	  the sequence synchronization worker.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_subscription_rel.h"
#include "pgstat.h"
#include "replication/logicallauncher.h"
#include "replication/origin.h"
#include "replication/slot.h"
#include "replication/worker_internal.h"
#include "storage/ipc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

typedef enum
{
	SYNC_RELATIONS_STATE_NEEDS_REBUILD,
	SYNC_RELATIONS_STATE_REBUILD_STARTED,
	SYNC_RELATIONS_STATE_VALID,
} SyncingRelationsState;

static SyncingRelationsState relation_states_validity = SYNC_RELATIONS_STATE_NEEDS_REBUILD;
List	   *table_states_not_ready = NIL;
List	   *sequence_states_not_ready = NIL;

/*
 * Exit routine for synchronization worker.
 */
void
pg_attribute_noreturn()
SyncFinishWorker(LogicalRepWorkerType wtype)
{
	Assert(wtype == WORKERTYPE_TABLESYNC || wtype == WORKERTYPE_SEQUENCESYNC);

	/*
	 * Commit any outstanding transaction. This is the usual case, unless
	 * there was nothing to do for the table.
	 */
	if (IsTransactionState())
	{
		CommitTransactionCommand();
		pgstat_report_stat(true);
	}

	/* And flush all writes. */
	XLogFlush(GetXLogWriteRecPtr());

	StartTransactionCommand();
	if (wtype == WORKERTYPE_TABLESYNC)
		ereport(LOG,
				errmsg("logical replication table synchronization worker for subscription \"%s\", table \"%s\" has finished",
					   MySubscription->name,
					   get_rel_name(MyLogicalRepWorker->relid)));
	else
		ereport(LOG,
				errmsg("logical replication sequence synchronization worker for subscription \"%s\" has finished",
					   MySubscription->name));
	CommitTransactionCommand();

	/* Find the leader apply worker and signal it. */
	logicalrep_worker_wakeup(MyLogicalRepWorker->subid, InvalidOid);

	/* This is a clean exit, so no need to set a sequence failure time. */
	if (wtype == WORKERTYPE_SEQUENCESYNC)
		cancel_before_shmem_exit(logicalrep_seqsyncworker_failuretime, 0);

	/* Stop gracefully */
	proc_exit(0);
}

/*
 * Callback from syscache invalidation.
 */
void
SyncInvalidateRelationStates(Datum arg, int cacheid, uint32 hashvalue)
{
	relation_states_validity = SYNC_RELATIONS_STATE_NEEDS_REBUILD;
}

/*
 * Process possible state change(s) of tables that are being synchronized and
 * start new tablesync workers for the newly added tables and start new
 * sequencesync worker for the newly added sequences.
 */
void
SyncProcessRelations(XLogRecPtr current_lsn)
{
	switch (MyLogicalRepWorker->type)
	{
		case WORKERTYPE_PARALLEL_APPLY:

			/*
			 * Skip for parallel apply workers because they only operate on
			 * tables that are in a READY state. See pa_can_start() and
			 * should_apply_changes_for_rel().
			 */
			break;

		case WORKERTYPE_TABLESYNC:
			ProcessSyncingTablesForSync(current_lsn);
			break;

		case WORKERTYPE_APPLY:
			/*
			 * We need up-to-date sync state info for subscription tables and
			 * sequences here.
			 */
			FetchRelationStates();

			ProcessSyncingTablesForApply(current_lsn);
			ProcessSyncingSequencesForApply();
			break;

		case WORKERTYPE_SEQUENCESYNC:
			/* Should never happen. */
			Assert(0);
			break;

		case WORKERTYPE_UNKNOWN:
			/* Should never happen. */
			elog(ERROR, "Unknown worker type");
	}
}

/*
 * Common code to fetch the up-to-date sync state info into the static lists.
 *
 * The pg_subscription_rel catalog is shared by tables and sequences. Changes to
 * either sequences or tables can affect the validity of relation states, so we
 * update both table_states_not_ready and sequence_states_not_ready
 * simultaneously to ensure consistency.
 *
 * Returns true if subscription has 1 or more tables, else false.
 */
bool
FetchRelationStates(void)
{
	/*
	 * This is declared as static, since the same value can be used until the
	 * system table is invalidated.
	 */
	static bool has_subtables = false;
	bool		started_tx = false;

	if (relation_states_validity != SYNC_RELATIONS_STATE_VALID)
	{
		MemoryContext oldctx;
		List	   *rstates;
		ListCell   *lc;
		SubscriptionRelState *rstate;

		relation_states_validity = SYNC_RELATIONS_STATE_REBUILD_STARTED;

		/* Clean the old lists. */
		list_free_deep(table_states_not_ready);
		list_free_deep(sequence_states_not_ready);
		table_states_not_ready = NIL;
		sequence_states_not_ready = NIL;

		if (!IsTransactionState())
		{
			StartTransactionCommand();
			started_tx = true;
		}

		/* Fetch tables and sequences that are in non-ready state. */
		rstates = GetSubscriptionRelations(MySubscription->oid, true, true,
										   false);

		/* Allocate the tracking info in a permanent memory context. */
		oldctx = MemoryContextSwitchTo(CacheMemoryContext);
		foreach(lc, rstates)
		{
			rstate = palloc(sizeof(SubscriptionRelState));
			memcpy(rstate, lfirst(lc), sizeof(SubscriptionRelState));

			if (get_rel_relkind(rstate->relid) == RELKIND_SEQUENCE)
				sequence_states_not_ready = lappend(sequence_states_not_ready, rstate);
			else
				table_states_not_ready = lappend(table_states_not_ready, rstate);
		}
		MemoryContextSwitchTo(oldctx);

		/*
		 * Does the subscription have tables?
		 *
		 * If there were not-READY tables found then we know it does. But if
		 * table_states_not_ready was empty we still need to check again to
		 * see if there are 0 tables.
		 */
		has_subtables = (table_states_not_ready != NIL) ||
			HasSubscriptionTables(MySubscription->oid);

		/*
		 * If the subscription relation cache has been invalidated since we
		 * entered this routine, we still use and return the relations we just
		 * finished constructing, to avoid infinite loops, but we leave the
		 * table states marked as stale so that we'll rebuild it again on next
		 * access. Otherwise, we mark the table states as valid.
		 */
		if (relation_states_validity == SYNC_RELATIONS_STATE_REBUILD_STARTED)
			relation_states_validity = SYNC_RELATIONS_STATE_VALID;
	}

	if (started_tx)
	{
		CommitTransactionCommand();
		pgstat_report_stat(true);
	}

	return has_subtables;
}
