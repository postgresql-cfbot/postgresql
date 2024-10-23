/*-------------------------------------------------------------------------
 * sequencesync.c
 *	  PostgreSQL logical replication: sequence synchronization
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/sequencesync.c
 *
 * NOTES
 *	  This file contains code for sequence synchronization for
 *	  logical replication.
 *
 * Sequences to be synchronized by the sequencesync worker will
 * be added to pg_subscription_rel in INIT state when one of the following
 * commands is executed:
 * CREATE SUBSCRIPTION
 * ALTER SUBSCRIPTION ... REFRESH PUBLICATION
 * ALTER SUBSCRIPTION ... REFRESH PUBLICATION SEQUENCES
 *
 * The apply worker will periodically check if there are any sequences in INIT
 * state and will start a sequencesync worker if needed.
 *
 * The sequencesync worker retrieves the sequences to be synchronized from the
 * pg_subscription_rel catalog table.  It synchronizes multiple sequences per
 * single transaction by fetching the sequence value and page LSN from the
 * remote publisher and updating them in the local subscriber sequence.  After
 * synchronization, it sets the sequence state to READY.
 *
 * So the state progression is always just: INIT -> READY.
 *
 * To avoid creating too many transactions, up to MAX_SEQUENCES_SYNC_PER_BATCH
 * (100) sequences are synchronized per transaction. The locks on the sequence
 * relation will be periodically released at each transaction commit.
 *
 * XXX: An alternative design was considered where the launcher process would
 * periodically check for sequences that need syncing and then start the
 * sequencesync worker. However, the approach of having the apply worker
 * manage the sequencesync worker was chosen for the following reasons:
 * a) It avoids overloading the launcher, which handles various other
 *    subscription requests.
 * b) It offers a more straightforward path for extending support for
 *    incremental sequence synchronization.
 * c) It utilizes the existing tablesync worker code to start the sequencesync
 *    process, thus preventing code duplication in the launcher.
 * d) It simplifies code maintenance by consolidating changes to a single
 *    location rather than multiple components.
 * e) The apply worker can access the sequences that need to be synchronized
 *    from the pg_subscription_rel system catalog. Whereas the launcher process
 *    operates without direct database access so would need a framework to
 *    establish connections with the databases to retrieve the sequences for
 *    synchronization.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/table.h"
#include "catalog/pg_sequence.h"
#include "catalog/pg_subscription_rel.h"
#include "commands/sequence.h"
#include "pgstat.h"
#include "replication/logicallauncher.h"
#include "replication/logicalworker.h"
#include "replication/worker_internal.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/lsyscache.h"
#include "utils/pg_lsn.h"
#include "utils/rls.h"
#include "utils/syscache.h"
#include "utils/usercontext.h"

/*
 * Handle sequence synchronization cooperation from the apply worker.
 *
 * Walk over all subscription sequences that are individually tracked by the
 * apply process (currently, all that have state SUBREL_STATE_INIT) and manage
 * synchronization for them.
 *
 * If a sequencesync worker is running already, there is no need to start a new
 * one; the existing sequencesync worker will synchronize all the sequences. If
 * there are still any sequences to be synced after the sequencesync worker
 * exited, then a new sequencesync worker can be started in the next iteration.
 * To prevent starting the sequencesync worker at a high frequency after a
 * failure, we store its last failure time. We start the sequencesync worker
 * again after waiting at least wal_retrieve_retry_interval.
 */
void
ProcessSyncingSequencesForApply(void)
{
	bool		started_tx = false;

	Assert(!IsTransactionState());

	/* Start the sequencesync worker if needed, and there is not one already. */
	foreach_ptr(SubscriptionRelState, rstate, sequence_states_not_ready)
	{
		LogicalRepWorker *syncworker;
		int			nsyncworkers;

		if (!started_tx)
		{
			StartTransactionCommand();
			started_tx = true;
		}

		Assert(get_rel_relkind(rstate->relid) == RELKIND_SEQUENCE);

		if (rstate->state != SUBREL_STATE_INIT)
			continue;

		/*
		 * Check if there is a sequence worker already running?
		 */
		LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);

		syncworker = logicalrep_worker_find(MyLogicalRepWorker->subid,
											InvalidOid, WORKERTYPE_SEQUENCESYNC,
											true);
		if (syncworker)
		{
			/* Now safe to release the LWLock */
			LWLockRelease(LogicalRepWorkerLock);
			break;
		}

		/*
		 * Count running sync workers for this subscription, while we have the
		 * lock.
		 */
		nsyncworkers = logicalrep_sync_worker_count(MyLogicalRepWorker->subid);

		/* Now safe to release the LWLock */
		LWLockRelease(LogicalRepWorkerLock);

		/*
		 * If there are free sync worker slot(s), start a new sequencesync
		 * worker, and break from the loop.
		 */
		if (nsyncworkers < max_sync_workers_per_subscription)
		{
			TimestampTz now = GetCurrentTimestamp();

			if (!MyLogicalRepWorker->sequencesync_failure_time ||
				TimestampDifferenceExceeds(MyLogicalRepWorker->sequencesync_failure_time,
										   now, wal_retrieve_retry_interval))
			{
				MyLogicalRepWorker->sequencesync_failure_time = 0;

				logicalrep_worker_launch(WORKERTYPE_SEQUENCESYNC,
										 MyLogicalRepWorker->dbid,
										 MySubscription->oid,
										 MySubscription->name,
										 MyLogicalRepWorker->userid,
										 InvalidOid,
										 DSM_HANDLE_INVALID);
				break;
			}
		}
	}

	if (started_tx)
	{
		CommitTransactionCommand();
		pgstat_report_stat(true);
	}
}

/*
 * fetch_remote_sequence_data
 *
 * Retrieves sequence data (last_value, log_cnt, page_lsn, and is_called)
 * from a remote node.
 *
 * Output Parameters:
 * - log_cnt: The log count of the sequence.
 * - is_called: Indicates if the sequence has been called.
 * - page_lsn: The log sequence number of the sequence page.
 * - last_value: The last value of the sequence.
 *
 * Returns:
 * - TRUE if parameters match for the local and remote sequences.
 * - FALSE if parameters differ for the local and remote sequences.
 */
static bool
fetch_remote_sequence_data(WalReceiverConn *conn, Oid relid, Oid remoteid,
						   char *nspname, char *relname, int64 *log_cnt,
						   bool *is_called, XLogRecPtr *page_lsn,
						   int64 *last_value)
{
#define REMOTE_SEQ_COL_COUNT 10
	Oid			tableRow[REMOTE_SEQ_COL_COUNT] = {INT8OID, INT8OID, BOOLOID,
	LSNOID, OIDOID, INT8OID, INT8OID, INT8OID, INT8OID, BOOLOID};

	WalRcvExecResult *res;
	StringInfoData cmd;
	TupleTableSlot *slot;
	bool		isnull;
	Oid			seqtypid;
	int64		seqstart;
	int64		seqincrement;
	int64		seqmin;
	int64		seqmax;
	bool		seqcycle;
	bool		seq_params_match;
	HeapTuple	tup;
	Form_pg_sequence seqform;
	int			col = 0;

	initStringInfo(&cmd);
	appendStringInfo(&cmd,
					 "SELECT last_value, log_cnt, is_called, page_lsn,\n"
					 "seqtypid, seqstart, seqincrement, seqmin, seqmax, seqcycle\n"
					 "FROM pg_sequence_state(%d), pg_sequence where seqrelid = %d",
					 remoteid, remoteid);

	res = walrcv_exec(conn, cmd.data, REMOTE_SEQ_COL_COUNT, tableRow);
	pfree(cmd.data);

	if (res->status != WALRCV_OK_TUPLES)
		ereport(ERROR,
				errmsg("could not receive sequence list from the publisher: %s",
					   res->err));

	/* Process the sequence. */
	slot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
	if (!tuplestore_gettupleslot(res->tuplestore, true, false, slot))
		ereport(ERROR,
				errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("sequence \"%s.%s\" not found on publisher",
					   nspname, relname));

	*last_value = DatumGetInt64(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	*log_cnt = DatumGetInt64(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	*is_called = DatumGetBool(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	*page_lsn = DatumGetLSN(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	seqtypid = DatumGetObjectId(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	seqstart = DatumGetInt64(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	seqincrement = DatumGetInt64(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	seqmin = DatumGetInt64(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	seqmax = DatumGetInt64(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	seqcycle = DatumGetBool(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	/* Sanity check */
	Assert(col == REMOTE_SEQ_COL_COUNT);

	/* Get the local sequence */
	tup = SearchSysCache1(SEQRELID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for sequence \"%s.%s\"",
			 nspname, relname);

	seqform = (Form_pg_sequence) GETSTRUCT(tup);

	seq_params_match = seqform->seqtypid == seqtypid &&
		seqform->seqmin == seqmin && seqform->seqmax == seqmax &&
		seqform->seqcycle == seqcycle &&
		seqform->seqstart == seqstart &&
		seqform->seqincrement == seqincrement;

	ReleaseSysCache(tup);
	ExecDropSingleTupleTableSlot(slot);
	walrcv_clear_result(res);

	return seq_params_match;
}

/*
 * Copy existing data of a sequence from publisher.
 *
 * Fetch the sequence value from the publisher and set the subscriber sequence
 * with the same value. Caller is responsible for locking the local
 * relation.
 *
 * The output parameter 'sequence_mismatch' indicates if a local/remote
 * sequence parameter mismatch was detected.
 */
static XLogRecPtr
copy_sequence(WalReceiverConn *conn, Relation rel,
			  bool *sequence_mismatch)
{
	StringInfoData cmd;
	int64		seq_last_value;
	int64		seq_log_cnt;
	bool		seq_is_called;
	XLogRecPtr	seq_page_lsn = InvalidXLogRecPtr;
	WalRcvExecResult *res;
	Oid			tableRow[] = {OIDOID, CHAROID};
	TupleTableSlot *slot;
	LogicalRepRelId remoteid;	/* unique id of the relation */
	char		relkind PG_USED_FOR_ASSERTS_ONLY;
	bool		isnull;
	char	   *nspname = get_namespace_name(RelationGetNamespace(rel));
	char	   *relname = RelationGetRelationName(rel);
	Oid			relid = RelationGetRelid(rel);

	Assert(!*sequence_mismatch);

	/* Fetch Oid. */
	initStringInfo(&cmd);
	appendStringInfo(&cmd, "SELECT c.oid, c.relkind\n"
					 "FROM pg_catalog.pg_class c\n"
					 "INNER JOIN pg_catalog.pg_namespace n\n"
					 "  ON (c.relnamespace = n.oid)\n"
					 "WHERE n.nspname = %s AND c.relname = %s",
					 quote_literal_cstr(nspname),
					 quote_literal_cstr(relname));

	res = walrcv_exec(conn, cmd.data,
					  lengthof(tableRow), tableRow);
	if (res->status != WALRCV_OK_TUPLES)
		ereport(ERROR,
				errcode(ERRCODE_CONNECTION_FAILURE),
				errmsg("sequence \"%s.%s\" info could not be fetched from publisher: %s",
					   nspname, relname, res->err));

	slot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
	if (!tuplestore_gettupleslot(res->tuplestore, true, false, slot))
		ereport(ERROR,
				errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("sequence \"%s.%s\" not found on publisher",
					   nspname, relname));

	remoteid = DatumGetObjectId(slot_getattr(slot, 1, &isnull));
	Assert(!isnull);
	relkind = DatumGetChar(slot_getattr(slot, 2, &isnull));
	Assert(!isnull);
	Assert(relkind == RELKIND_SEQUENCE);

	ExecDropSingleTupleTableSlot(slot);
	walrcv_clear_result(res);

	*sequence_mismatch = !fetch_remote_sequence_data(conn, relid, remoteid,
													 nspname, relname,
													 &seq_log_cnt, &seq_is_called,
													 &seq_page_lsn, &seq_last_value);

	SetSequence(RelationGetRelid(rel), seq_last_value, seq_is_called,
				seq_log_cnt);

	/* return the LSN when the sequence state was set */
	return seq_page_lsn;
}

/*
 * report_mismatched_sequences
 *
 * Report any sequence mismatches as a single warning log.
 */
static void
report_mismatched_sequences(StringInfo mismatched_seqs)
{
	if (mismatched_seqs->len)
	{
		ereport(WARNING,
				errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg("parameters differ for the remote and local sequences (%s) for subscription \"%s\"",
					   mismatched_seqs->data, MySubscription->name),
				errhint("Alter/Re-create local sequences to have the same parameters as the remote sequences."));

		resetStringInfo(mismatched_seqs);
	}
}

/*
 * append_mismatched_sequences
 *
 * Appends details of sequences that have discrepancies between the publisher
 * and subscriber to the mismatched_seqs string.
 */
static void
append_mismatched_sequences(StringInfo mismatched_seqs, Relation seqrel)
{
	if (mismatched_seqs->len)
		appendStringInfoString(mismatched_seqs, ", ");

	appendStringInfo(mismatched_seqs, "\"%s.%s\"",
					 get_namespace_name(RelationGetNamespace(seqrel)),
					 RelationGetRelationName(seqrel));
}

/*
 * Start syncing the sequences in the sync worker.
 */
static void
LogicalRepSyncSequences(void)
{
	char	   *err;
	bool		must_use_password;
	List	   *sequences;
	List	   *sequences_not_synced = NIL;
	char		slotname[NAMEDATALEN];
	AclResult	aclresult;
	UserContext ucxt;
	bool		run_as_owner = false;
	int			curr_seq = 0;
	int			seq_count;
	bool		start_txn = true;
	Oid			subid = MyLogicalRepWorker->subid;
	MemoryContext oldctx;
	StringInfo	mismatched_seqs = makeStringInfo();

/*
 * Synchronizing each sequence individually incurs overhead from starting
 * and committing a transaction repeatedly. Additionally, we want to avoid
 * keeping transactions open for extended periods by setting excessively
 * high values.
 */
#define MAX_SEQUENCES_SYNC_PER_BATCH 100

	StartTransactionCommand();

	/* Get the sequences that should be synchronized. */
	sequences = GetSubscriptionRelations(subid, false, true, false);

	/* Allocate the tracking info in a permanent memory context. */
	oldctx = MemoryContextSwitchTo(CacheMemoryContext);
	foreach_ptr(SubscriptionRelState, seq_state, sequences)
	{
		SubscriptionRelState *rstate = palloc(sizeof(SubscriptionRelState));

		memcpy(rstate, seq_state, sizeof(SubscriptionRelState));
		sequences_not_synced = lappend(sequences_not_synced, rstate);
	}
	MemoryContextSwitchTo(oldctx);

	CommitTransactionCommand();

	/* Is the use of a password mandatory? */
	must_use_password = MySubscription->passwordrequired &&
		!MySubscription->ownersuperuser;

	snprintf(slotname, NAMEDATALEN, "pg_%u_sync_sequences_" UINT64_FORMAT,
			 subid, GetSystemIdentifier());

	/*
	 * Here we use the slot name instead of the subscription name as the
	 * application_name, so that it is different from the leader apply worker,
	 * so that synchronous replication can distinguish them.
	 */
	LogRepWorkerWalRcvConn =
		walrcv_connect(MySubscription->conninfo, true, true,
					   must_use_password,
					   slotname, &err);
	if (LogRepWorkerWalRcvConn == NULL)
		ereport(ERROR,
				errcode(ERRCODE_CONNECTION_FAILURE),
				errmsg("could not connect to the publisher: %s", err));

	seq_count = list_length(sequences_not_synced);
	foreach_ptr(SubscriptionRelState, seqinfo, sequences_not_synced)
	{
		Relation	sequence_rel;
		XLogRecPtr	sequence_lsn;
		bool		sequence_mismatch = false;

		CHECK_FOR_INTERRUPTS();

		if (start_txn)
		{
			StartTransactionCommand();
			start_txn = false;
		}

		sequence_rel = table_open(seqinfo->relid, RowExclusiveLock);

		/*
		 * Make sure that the copy command runs as the sequence owner, unless
		 * the user has opted out of that behaviour.
		 */
		run_as_owner = MySubscription->runasowner;
		if (!run_as_owner)
			SwitchToUntrustedUser(sequence_rel->rd_rel->relowner, &ucxt);

		/*
		 * Check that our sequencesync worker has permission to insert into
		 * the target sequence.
		 */
		aclresult = pg_class_aclcheck(RelationGetRelid(sequence_rel), GetUserId(),
									  ACL_INSERT);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult,
						   get_relkind_objtype(sequence_rel->rd_rel->relkind),
						   RelationGetRelationName(sequence_rel));

		/*
		 * COPY FROM does not honor RLS policies.  That is not a problem for
		 * subscriptions owned by roles with BYPASSRLS privilege (or
		 * superuser, who has it implicitly), but other roles should not be
		 * able to circumvent RLS.  Disallow logical replication into RLS
		 * enabled relations for such roles.
		 */
		if (check_enable_rls(RelationGetRelid(sequence_rel), InvalidOid, false) == RLS_ENABLED)
			ereport(ERROR,
					errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("user \"%s\" cannot replicate into sequence with row-level security enabled: \"%s\"",
						   GetUserNameFromId(GetUserId(), true),
						   RelationGetRelationName(sequence_rel)));

		/*
		 * In case sequence copy fails, throw a warning for the sequences that
		 * did not match before exiting.
		 */
		PG_TRY();
		{
			sequence_lsn = copy_sequence(LogRepWorkerWalRcvConn, sequence_rel,
										 &sequence_mismatch);
		}
		PG_CATCH();
		{
			if (sequence_mismatch)
				append_mismatched_sequences(mismatched_seqs, sequence_rel);

			report_mismatched_sequences(mismatched_seqs);
			PG_RE_THROW();
		}
		PG_END_TRY();

		if (sequence_mismatch)
			append_mismatched_sequences(mismatched_seqs, sequence_rel);

		UpdateSubscriptionRelState(subid, seqinfo->relid, SUBREL_STATE_READY,
								   sequence_lsn);

		table_close(sequence_rel, NoLock);

		curr_seq++;

		/*
		 * Have we reached the end of the current batch of sequences, or last
		 * remaining sequences to synchronize?
		 */
		if (((curr_seq % MAX_SEQUENCES_SYNC_PER_BATCH) == 0) ||
			curr_seq == seq_count)
		{
			/* LOG all the sequences synchronized during current batch. */
			for (int i = (curr_seq - 1) - ((curr_seq - 1) % MAX_SEQUENCES_SYNC_PER_BATCH);
				 i < curr_seq; i++)
			{
				SubscriptionRelState *done_seq;

				done_seq = (SubscriptionRelState *) lfirst(list_nth_cell(sequences_not_synced, i));

				ereport(DEBUG1,
						errmsg_internal("logical replication synchronization for subscription \"%s\", sequence \"%s\" has finished",
										get_subscription_name(subid, false), get_rel_name(done_seq->relid)));
			}

			report_mismatched_sequences(mismatched_seqs);

			ereport(LOG,
					errmsg("logical replication synchronized %d of %d sequences for subscription \"%s\" ",
						   curr_seq, seq_count, get_subscription_name(subid, false)));

			/* Commit this batch, and prepare for next batch. */
			CommitTransactionCommand();
			start_txn = true;
		}
	}

	list_free_deep(sequences_not_synced);
	if (!run_as_owner && seq_count)
		RestoreUserContext(&ucxt);
}

/*
 * Execute the initial sync with error handling. Disable the subscription,
 * if required.
 *
 * Allocate the slot name in long-lived context on return. Note that we don't
 * handle FATAL errors which are probably because of system resource error and
 * are not repeatable.
 */
static void
start_sequence_sync()
{
	Assert(am_sequencesync_worker());

	PG_TRY();
	{
		/* Call initial sync. */
		LogicalRepSyncSequences();
	}
	PG_CATCH();
	{
		if (MySubscription->disableonerr)
			DisableSubscriptionAndExit();
		else
		{
			/*
			 * Report the worker failed during sequence synchronization. Abort
			 * the current transaction so that the stats message is sent in an
			 * idle state.
			 */
			AbortOutOfAnyTransaction();
			pgstat_report_subscription_error(MySubscription->oid, false);

			PG_RE_THROW();
		}
	}
	PG_END_TRY();
}

/* Logical Replication sequencesync worker entry point */
void
SequenceSyncWorkerMain(Datum main_arg)
{
	int			worker_slot = DatumGetInt32(main_arg);

	SetupApplyOrSyncWorker(worker_slot);

	start_sequence_sync();

	SyncFinishWorker(WORKERTYPE_SEQUENCESYNC);
}
