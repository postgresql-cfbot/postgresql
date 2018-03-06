/*-------------------------------------------------------------------------
 * worker.c
 *	   PostgreSQL logical replication worker (apply)
 *
 * Copyright (c) 2016-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/worker.c
 *
 * NOTES
 *	  This file contains the worker which applies logical changes as they come
 *	  from remote logical replication stream.
 *
 *	  The main worker (apply) is started by logical replication worker
 *	  launcher for every enabled subscription in a database. It uses
 *	  walsender protocol to communicate with publisher.
 *
 *	  This module includes server facing code and shares libpqwalreceiver
 *	  module with walreceiver for providing the libpq specific functionality.
 *
 *
 * STREAMED TRANSACTIONS
 * ---------------------
 *
 * Streamed transactions (large transactions exceeding a memory limit on the
 * upstream) are not applied immediately, but instead the data is written
 * to files and then applied at once when the final commit arrives.
 *
 * Unlike the regular (non-streamed) case, handling streamed transactions has
 * to handle aborts of both the toplevel transaction and subtransactions. This
 * is achieved by tracking offsets for subtransactions, which is then used
 * to truncate the file with serialized changes.
 *
 * The files are placed in /tmp by default, and the filenames include both
 * the XID of the toplevel transaction and OID of the subscription. This
 * is necessary so that different workers processing a remote transaction
 * with the same XID don't interfere.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "miscadmin.h"
#include "pgstat.h"
#include "funcapi.h"

#include "access/xact.h"
#include "access/xlog_internal.h"

#include "catalog/namespace.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_subscription_rel.h"
#include "catalog/pg_tablespace.h"

#include "commands/trigger.h"

#include "executor/executor.h"
#include "executor/nodeModifyTable.h"

#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"

#include "mb/pg_wchar.h"

#include "nodes/makefuncs.h"

#include "optimizer/planner.h"

#include "parser/parse_relation.h"

#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "postmaster/walwriter.h"

#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/logicalproto.h"
#include "replication/logicalrelation.h"
#include "replication/logicalworker.h"
#include "replication/reorderbuffer.h"
#include "replication/origin.h"
#include "replication/snapbuild.h"
#include "replication/walreceiver.h"
#include "replication/worker_internal.h"

#include "rewrite/rewriteHandler.h"

#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"

#include "tcop/tcopprot.h"

#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/dynahash.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timeout.h"
#include "utils/tqual.h"
#include "utils/syscache.h"

#define NAPTIME_PER_CYCLE 1000	/* max sleep time between cycles (1s) */

typedef struct FlushPosition
{
	dlist_node	node;
	XLogRecPtr	local_end;
	XLogRecPtr	remote_end;
} FlushPosition;

static dlist_head lsn_mapping = DLIST_STATIC_INIT(lsn_mapping);

typedef struct SlotErrCallbackArg
{
	LogicalRepRelation *rel;
	int			attnum;
} SlotErrCallbackArg;

static MemoryContext ApplyMessageContext = NULL;
MemoryContext ApplyContext = NULL;

WalReceiverConn *wrconn = NULL;

Subscription *MySubscription = NULL;
bool		MySubscriptionValid = false;

bool		in_remote_transaction = false;
static XLogRecPtr remote_final_lsn = InvalidXLogRecPtr;

/* fields valid only when processing streamed transaction */
bool		in_streamed_transaction = false;

static TransactionId stream_xid = InvalidTransactionId;
static int	stream_fd = -1;

typedef struct SubXactInfo
{
	TransactionId xid;			/* XID of the subxact */
	off_t		offset;			/* offset in the file */
}			SubXactInfo;

static uint32 nsubxacts = 0;
static uint32 nsubxacts_max = 0;
static SubXactInfo * subxacts = NULL;
static TransactionId subxact_last = InvalidTransactionId;

static void subxact_filename(char *path, Oid subid, TransactionId xid);
static void changes_filename(char *path, Oid subid, TransactionId xid);

/*
 * Information about subtransactions of a given toplevel transaction.
 */
static void subxact_info_write(Oid subid, TransactionId xid);
static void subxact_info_read(Oid subid, TransactionId xid);
static void subxact_info_add(TransactionId xid);

/*
 * Serialize and deserialize changes for a toplevel transaction.
 */
static void stream_cleanup_files(Oid subid, TransactionId xid);
static void stream_open_file(Oid subid, TransactionId xid, bool first);
static void stream_write_change(char action, StringInfo s);
static void stream_close_file(void);

/*
 * Array of serialized XIDs.
 */
static int	nxids = 0;
static int	maxnxids = 0;
static TransactionId	*xids = NULL;

static bool handle_streamed_transaction(const char action, StringInfo s);

static void send_feedback(XLogRecPtr recvpos, bool force, bool requestReply);

static void store_flush_position(XLogRecPtr remote_lsn);

static void maybe_reread_subscription(void);

/* Flags set by signal handlers */
static volatile sig_atomic_t got_SIGHUP = false;

/* prototype needed because of stream_commit */
static void apply_dispatch(StringInfo s);

/*
 * Should this worker apply changes for given relation.
 *
 * This is mainly needed for initial relation data sync as that runs in
 * separate worker process running in parallel and we need some way to skip
 * changes coming to the main apply worker during the sync of a table.
 *
 * Note we need to do smaller or equals comparison for SYNCDONE state because
 * it might hold position of end of initial slot consistent point WAL
 * record + 1 (ie start of next record) and next record can be COMMIT of
 * transaction we are now processing (which is what we set remote_final_lsn
 * to in apply_handle_begin).
 */
static bool
should_apply_changes_for_rel(LogicalRepRelMapEntry *rel)
{
	if (am_tablesync_worker())
		return MyLogicalRepWorker->relid == rel->localreloid;
	else
		return (rel->state == SUBREL_STATE_READY ||
				(rel->state == SUBREL_STATE_SYNCDONE &&
				 rel->statelsn <= remote_final_lsn));
}

/*
 * Make sure that we started local transaction.
 *
 * Also switches to ApplyMessageContext as necessary.
 */
static bool
ensure_transaction(void)
{
	if (IsTransactionState())
	{
		SetCurrentStatementStartTimestamp();

		if (CurrentMemoryContext != ApplyMessageContext)
			MemoryContextSwitchTo(ApplyMessageContext);

		return false;
	}

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();

	maybe_reread_subscription();

	MemoryContextSwitchTo(ApplyMessageContext);
	return true;
}

/*
 * Handle streamed transactions.
 *
 * If in streaming mode (receiving a block of streamed transaction), we
 * simply redirect it to a file for the proper toplevel transaction.
 *
 * Returns true for streamed transactions, false otherwise (regular mode).
 */
static bool
handle_streamed_transaction(const char action, StringInfo s)
{
	TransactionId xid;

	/* not in streaming mode */
	if (!in_streamed_transaction)
		return false;

	Assert(stream_fd != -1);
	Assert(TransactionIdIsValid(stream_xid));

	/*
	 * We should have received XID of the subxact as the first part of the
	 * message, so extract it.
	 */
	xid = pq_getmsgint(s, 4);

	Assert(TransactionIdIsValid(xid));

	/* Add the new subxact to the array (unless already there). */
	subxact_info_add(xid);

	/* write the change to the current file */
	stream_write_change(action, s);

	return true;
}

/*
 * Executor state preparation for evaluation of constraint expressions,
 * indexes and triggers.
 *
 * This is based on similar code in copy.c
 */
static EState *
create_estate_for_relation(LogicalRepRelMapEntry *rel)
{
	EState	   *estate;
	ResultRelInfo *resultRelInfo;
	RangeTblEntry *rte;

	estate = CreateExecutorState();

	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel->localrel);
	rte->relkind = rel->localrel->rd_rel->relkind;
	estate->es_range_table = list_make1(rte);

	resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfo(resultRelInfo, rel->localrel, 1, NULL, 0);

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;

	estate->es_output_cid = GetCurrentCommandId(true);

	/* Triggers might need a slot */
	if (resultRelInfo->ri_TrigDesc)
		estate->es_trig_tuple_slot = ExecInitExtraTupleSlot(estate, NULL);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	return estate;
}

/*
 * Executes default values for columns for which we can't map to remote
 * relation columns.
 *
 * This allows us to support tables which have more columns on the downstream
 * than on the upstream.
 */
static void
slot_fill_defaults(LogicalRepRelMapEntry *rel, EState *estate,
				   TupleTableSlot *slot)
{
	TupleDesc	desc = RelationGetDescr(rel->localrel);
	int			num_phys_attrs = desc->natts;
	int			i;
	int			attnum,
				num_defaults = 0;
	int		   *defmap;
	ExprState **defexprs;
	ExprContext *econtext;

	econtext = GetPerTupleExprContext(estate);

	/* We got all the data via replication, no need to evaluate anything. */
	if (num_phys_attrs == rel->remoterel.natts)
		return;

	defmap = (int *) palloc(num_phys_attrs * sizeof(int));
	defexprs = (ExprState **) palloc(num_phys_attrs * sizeof(ExprState *));

	for (attnum = 0; attnum < num_phys_attrs; attnum++)
	{
		Expr	   *defexpr;

		if (TupleDescAttr(desc, attnum)->attisdropped)
			continue;

		if (rel->attrmap[attnum] >= 0)
			continue;

		defexpr = (Expr *) build_column_default(rel->localrel, attnum + 1);

		if (defexpr != NULL)
		{
			/* Run the expression through planner */
			defexpr = expression_planner(defexpr);

			/* Initialize executable expression in copycontext */
			defexprs[num_defaults] = ExecInitExpr(defexpr, NULL);
			defmap[num_defaults] = attnum;
			num_defaults++;
		}

	}

	for (i = 0; i < num_defaults; i++)
		slot->tts_values[defmap[i]] =
			ExecEvalExpr(defexprs[i], econtext, &slot->tts_isnull[defmap[i]]);
}

/*
 * Error callback to give more context info about type conversion failure.
 */
static void
slot_store_error_callback(void *arg)
{
	SlotErrCallbackArg *errarg = (SlotErrCallbackArg *) arg;
	Oid			remotetypoid,
				localtypoid;

	if (errarg->attnum < 0)
		return;

	remotetypoid = errarg->rel->atttyps[errarg->attnum];
	localtypoid = logicalrep_typmap_getid(remotetypoid);
	errcontext("processing remote data for replication target relation \"%s.%s\" column \"%s\", "
			   "remote type %s, local type %s",
			   errarg->rel->nspname, errarg->rel->relname,
			   errarg->rel->attnames[errarg->attnum],
			   format_type_be(remotetypoid),
			   format_type_be(localtypoid));
}

/*
 * Store data in C string form into slot.
 * This is similar to BuildTupleFromCStrings but TupleTableSlot fits our
 * use better.
 */
static void
slot_store_cstrings(TupleTableSlot *slot, LogicalRepRelMapEntry *rel,
					char **values)
{
	int			natts = slot->tts_tupleDescriptor->natts;
	int			i;
	SlotErrCallbackArg errarg;
	ErrorContextCallback errcallback;

	ExecClearTuple(slot);

	/* Push callback + info on the error context stack */
	errarg.rel = &rel->remoterel;
	errarg.attnum = -1;
	errcallback.callback = slot_store_error_callback;
	errcallback.arg = (void *) &errarg;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* Call the "in" function for each non-dropped attribute */
	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, i);
		int			remoteattnum = rel->attrmap[i];

		if (!att->attisdropped && remoteattnum >= 0 &&
			values[remoteattnum] != NULL)
		{
			Oid			typinput;
			Oid			typioparam;

			errarg.attnum = remoteattnum;

			getTypeInputInfo(att->atttypid, &typinput, &typioparam);
			slot->tts_values[i] = OidInputFunctionCall(typinput,
													   values[remoteattnum],
													   typioparam,
													   att->atttypmod);
			slot->tts_isnull[i] = false;
		}
		else
		{
			/*
			 * We assign NULL to dropped attributes, NULL values, and missing
			 * values (missing values should be later filled using
			 * slot_fill_defaults).
			 */
			slot->tts_values[i] = (Datum) 0;
			slot->tts_isnull[i] = true;
		}
	}

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;

	ExecStoreVirtualTuple(slot);
}

/*
 * Modify slot with user data provided as C strings.
 * This is somewhat similar to heap_modify_tuple but also calls the type
 * input function on the user data as the input is the text representation
 * of the types.
 */
static void
slot_modify_cstrings(TupleTableSlot *slot, LogicalRepRelMapEntry *rel,
					 char **values, bool *replaces)
{
	int			natts = slot->tts_tupleDescriptor->natts;
	int			i;
	SlotErrCallbackArg errarg;
	ErrorContextCallback errcallback;

	slot_getallattrs(slot);
	ExecClearTuple(slot);

	/* Push callback + info on the error context stack */
	errarg.rel = &rel->remoterel;
	errarg.attnum = -1;
	errcallback.callback = slot_store_error_callback;
	errcallback.arg = (void *) &errarg;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* Call the "in" function for each replaced attribute */
	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, i);
		int			remoteattnum = rel->attrmap[i];

		if (remoteattnum < 0)
			continue;

		if (!replaces[remoteattnum])
			continue;

		if (values[remoteattnum] != NULL)
		{
			Oid			typinput;
			Oid			typioparam;

			errarg.attnum = remoteattnum;

			getTypeInputInfo(att->atttypid, &typinput, &typioparam);
			slot->tts_values[i] = OidInputFunctionCall(typinput,
													   values[remoteattnum],
													   typioparam,
													   att->atttypmod);
			slot->tts_isnull[i] = false;
		}
		else
		{
			slot->tts_values[i] = (Datum) 0;
			slot->tts_isnull[i] = true;
		}
	}

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;

	ExecStoreVirtualTuple(slot);
}

/*
 * Handle BEGIN message.
 */
static void
apply_handle_begin(StringInfo s)
{
	LogicalRepBeginData begin_data;

	logicalrep_read_begin(s, &begin_data);

	remote_final_lsn = begin_data.final_lsn;

	in_remote_transaction = true;

	pgstat_report_activity(STATE_RUNNING, NULL);
}

/*
 * Handle COMMIT message.
 *
 * TODO, support tracking of multiple origins
 */
static void
apply_handle_commit(StringInfo s)
{
	LogicalRepCommitData commit_data;

	logicalrep_read_commit(s, &commit_data);

	Assert(commit_data.commit_lsn == remote_final_lsn);

	/* The synchronization worker runs in single transaction. */
	if (IsTransactionState() && !am_tablesync_worker())
	{
		/*
		 * Update origin state so we can restart streaming from correct
		 * position in case of crash.
		 */
		replorigin_session_origin_lsn = commit_data.end_lsn;
		replorigin_session_origin_timestamp = commit_data.committime;

		CommitTransactionCommand();
		pgstat_report_stat(false);

		store_flush_position(commit_data.end_lsn);
	}
	else
	{
		/* Process any invalidation messages that might have accumulated. */
		AcceptInvalidationMessages();
		maybe_reread_subscription();
	}

	in_remote_transaction = false;

	/* Process any tables that are being synchronized in parallel. */
	process_syncing_tables(commit_data.end_lsn);

	pgstat_report_activity(STATE_IDLE, NULL);
}

/*
 * Handle ORIGIN message.
 *
 * TODO, support tracking of multiple origins
 */
static void
apply_handle_origin(StringInfo s)
{
	/*
	 * ORIGIN message can only come inside remote transaction and before any
	 * actual writes.
	 */
	if (!in_remote_transaction ||
		(IsTransactionState() && !am_tablesync_worker()))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("ORIGIN message sent out of order")));
}

/*
 * Handle STREAM START message.
 */
static void
apply_handle_stream_start(StringInfo s)
{
	bool		first_segment;

	Assert(!in_streamed_transaction);

	/* notify handle methods we're processing a remote transaction */
	in_streamed_transaction = true;

	/* extract XID of the top-level transaction */
	stream_xid = logicalrep_read_stream_start(s, &first_segment);

	/* open the spool file for this transaction */
	stream_open_file(MyLogicalRepWorker->subid, stream_xid, first_segment);

	/*
	 * if this is not the first segment, open existing file
	 *
	 * XXX Note that the cleanup is performed by stream_open_file.
	 */
	if (!first_segment)
		subxact_info_read(MyLogicalRepWorker->subid, stream_xid);

	pgstat_report_activity(STATE_RUNNING, NULL);
}

/*
 * Handle STREAM STOP message.
 */
static void
apply_handle_stream_stop(StringInfo s)
{
	Assert(in_streamed_transaction);

	/*
	 * Close the file with serialized changes, and serialize information about
	 * subxacts for the toplevel transaction.
	 */
	subxact_info_write(MyLogicalRepWorker->subid, stream_xid);
	stream_close_file();

	in_streamed_transaction = false;

	pgstat_report_activity(STATE_IDLE, NULL);
}

/*
 * Handle STREAM abort message.
 */
static void
apply_handle_stream_abort(StringInfo s)
{
	TransactionId xid;
	TransactionId subxid;

	Assert(!in_streamed_transaction);

	logicalrep_read_stream_abort(s, &xid, &subxid);

	/*
	 * If the two XIDs are the same, it's in fact abort of toplevel xact, so
	 * just delete the files with serialized info.
	 */
	if (xid == subxid)
	{
		char		path[MAXPGPATH];

		/*
		 * XXX Maybe this should be an error instead? Can we receive abort for
		 * a toplevel transaction we haven't received?
		 */

		changes_filename(path, MyLogicalRepWorker->subid, xid);

		if (unlink(path) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not remove file \"%s\": %m", path)));

		subxact_filename(path, MyLogicalRepWorker->subid, xid);

		if (unlink(path) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not remove file \"%s\": %m", path)));

		return;
	}
	else
	{
		/*
		 * OK, so it's a subxact. We need to read the subxact file for the
		 * toplevel transaction, determine the offset tracked for the subxact,
		 * and truncate the file with changes. We also remove the subxacts
		 * with higher offsets (or rather higher XIDs).
		 *
		 * We intentionally scan the array from the tail, because we're likely
		 * aborting a change for the most recent subtransactions.
		 *
		 * XXX Can we rely on the subxact XIDs arriving in sorted order? That
		 * would allow us to use binary search here.
		 *
		 * XXX Or perhaps we can rely on the aborts to arrive in the reverse
		 * order, i.e. from the inner-most subxact (when nested)? In which
		 * case we could simply check the last element.
		 */

		int64		i;
		int64		subidx;
		bool		found = false;
		char		path[MAXPGPATH];

		subidx = -1;
		subxact_info_read(MyLogicalRepWorker->subid, xid);

		/* FIXME optimize the search by bsearch on sorted data */
		for (i = nsubxacts; i > 0; i--)
		{
			if (subxacts[i - 1].xid == subxid)
			{
				subidx = (i - 1);
				found = true;
				break;
			}
		}

		/* We should not receive aborts for unknown subtransactions. */
		Assert(found);

		/* OK, truncate the file at the right offset. */
		Assert((subidx >= 0) && (subidx < nsubxacts));

		changes_filename(path, MyLogicalRepWorker->subid, xid);

		if (truncate(path, subxacts[subidx].offset))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not truncate file \"%s\": %m", path)));

		/* discard the subxacts added later */
		nsubxacts = subidx;

		/* write the updated subxact list */
		subxact_info_write(MyLogicalRepWorker->subid, xid);
	}
}

/*
 * Handle STREAM COMMIT message.
 */
static void
apply_handle_stream_commit(StringInfo s)
{
	int			fd;
	TransactionId xid;
	StringInfoData s2;
	int			nchanges;

	char		path[MAXPGPATH];
	char	   *buffer = NULL;
	LogicalRepCommitData commit_data;

	MemoryContext oldcxt;

	Assert(!in_streamed_transaction);

	xid = logicalrep_read_stream_commit(s, &commit_data);

	elog(DEBUG1, "received commit for streamed transaction %u", xid);

	/* open the spool file for the committed transaction */
	changes_filename(path, MyLogicalRepWorker->subid, xid);

	elog(DEBUG1, "replaying changes from file '%s'", path);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						path)));
	}

	/* XXX Should this be allocated in another memory context? */

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	buffer = palloc(8192);
	initStringInfo(&s2);

	MemoryContextSwitchTo(oldcxt);

	ensure_transaction();

	/*
	 * Make sure the handle apply_dispatch methods are aware we're in a remote
	 * transaction.
	 */
	in_remote_transaction = true;
	pgstat_report_activity(STATE_RUNNING, NULL);

	/*
	 * Read the entries one by one and pass them through the same logic as in
	 * apply_dispatch.
	 */
	nchanges = 0;
	while (true)
	{
		int			nbytes;
		int			len;

		/* read length of the on-disk record */
		pgstat_report_wait_start(WAIT_EVENT_LOGICAL_CHANGES_READ);
		nbytes = read(fd, &len, sizeof(len));
		pgstat_report_wait_end();

		/* have we reached end of the file? */
		if (nbytes == 0)
			break;

		/* do we have a correct length? */
		if (nbytes != sizeof(len))
		{
			int			save_errno = errno;

			CloseTransientFile(fd);
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file: %m")));
			return;
		}

		Assert(len > 0);

		/* make sure we have sufficiently large buffer */
		buffer = repalloc(buffer, len);

		/* and finally read the data into the buffer */
		pgstat_report_wait_start(WAIT_EVENT_LOGICAL_CHANGES_READ);
		if (read(fd, buffer, len) != len)
		{
			int			save_errno = errno;

			CloseTransientFile(fd);
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file: %m")));
			return;
		}
		pgstat_report_wait_end();

		/* copy the buffer to the stringinfo and call apply_dispatch */
		resetStringInfo(&s2);
		appendBinaryStringInfo(&s2, buffer, len);

		/* Ensure we are reading the data into our memory context. */
		oldcxt = MemoryContextSwitchTo(ApplyMessageContext);

		apply_dispatch(&s2);

		MemoryContextReset(ApplyMessageContext);

		MemoryContextSwitchTo(oldcxt);

		nchanges++;

		if (nchanges % 1000 == 0)
			elog(DEBUG1, "replayed %d changes from file '%s'",
				 nchanges, path);

		/*
		 * send feedback to upstream
		 *
		 * XXX Probably should send a valid LSN. But which one?
		 */
		send_feedback(InvalidXLogRecPtr, false, false);
	}

	CloseTransientFile(fd);

	/*
	 * Update origin state so we can restart streaming from correct
	 * position in case of crash.
	 */
	replorigin_session_origin_lsn = commit_data.end_lsn;
	replorigin_session_origin_timestamp = commit_data.committime;

	CommitTransactionCommand();
	pgstat_report_stat(false);

	store_flush_position(commit_data.end_lsn);

	elog(DEBUG1, "replayed %d (all) changes from file '%s'",
		 nchanges, path);

	in_remote_transaction = false;
	pgstat_report_activity(STATE_IDLE, NULL);

	/* unlink the files with serialized changes and subxact info */
	stream_cleanup_files(MyLogicalRepWorker->subid, xid);

	pfree(buffer);
	pfree(s2.data);
}

/*
 * Handle RELATION message.
 *
 * Note we don't do validation against local schema here. The validation
 * against local schema is postponed until first change for given relation
 * comes as we only care about it when applying changes for it anyway and we
 * do less locking this way.
 */
static void
apply_handle_relation(StringInfo s)
{
	LogicalRepRelation *rel;

	if (handle_streamed_transaction('R', s))
		return;

	rel = logicalrep_read_rel(s);
	logicalrep_relmap_update(rel);
}

/*
 * Handle TYPE message.
 *
 * Note we don't do local mapping here, that's done when the type is
 * actually used.
 */
static void
apply_handle_type(StringInfo s)
{
	LogicalRepTyp typ;

	if (handle_streamed_transaction('Y', s))
		return;

	logicalrep_read_typ(s, &typ);
	logicalrep_typmap_update(&typ);
}

/*
 * Get replica identity index or if it is not defined a primary key.
 *
 * If neither is defined, returns InvalidOid
 */
static Oid
GetRelationIdentityOrPK(Relation rel)
{
	Oid			idxoid;

	idxoid = RelationGetReplicaIndex(rel);

	if (!OidIsValid(idxoid))
		idxoid = RelationGetPrimaryKeyIndex(rel);

	return idxoid;
}

/*
 * Handle INSERT message.
 */
static void
apply_handle_insert(StringInfo s)
{
	LogicalRepRelMapEntry *rel;
	LogicalRepTupleData newtup;
	LogicalRepRelId relid;
	EState	   *estate;
	TupleTableSlot *remoteslot;
	MemoryContext oldctx;

	if (handle_streamed_transaction('I', s))
		return;

	ensure_transaction();

	relid = logicalrep_read_insert(s, &newtup);
	rel = logicalrep_rel_open(relid, RowExclusiveLock);
	if (!should_apply_changes_for_rel(rel))
	{
		/*
		 * The relation can't become interesting in the middle of the
		 * transaction so it's safe to unlock it.
		 */
		logicalrep_rel_close(rel, RowExclusiveLock);
		return;
	}

	/* Initialize the executor state. */
	estate = create_estate_for_relation(rel);
	remoteslot = ExecInitExtraTupleSlot(estate,
										RelationGetDescr(rel->localrel));

	/* Process and store remote tuple in the slot */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	slot_store_cstrings(remoteslot, rel, newtup.values);
	slot_fill_defaults(rel, estate, remoteslot);
	MemoryContextSwitchTo(oldctx);

	PushActiveSnapshot(GetTransactionSnapshot());
	ExecOpenIndices(estate->es_result_relation_info, false);

	/* Do the insert. */
	ExecSimpleRelationInsert(estate, remoteslot);

	/* Cleanup. */
	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();

	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(estate);

	ExecResetTupleTable(estate->es_tupleTable, false);
	FreeExecutorState(estate);

	logicalrep_rel_close(rel, NoLock);

	CommandCounterIncrement();
}

/*
 * Check if the logical replication relation is updatable and throw
 * appropriate error if it isn't.
 */
static void
check_relation_updatable(LogicalRepRelMapEntry *rel)
{
	/* Updatable, no error. */
	if (rel->updatable)
		return;

	/*
	 * We are in error mode so it's fine this is somewhat slow. It's better to
	 * give user correct error.
	 */
	if (OidIsValid(GetRelationIdentityOrPK(rel->localrel)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("publisher did not send replica identity column "
						"expected by the logical replication target relation \"%s.%s\"",
						rel->remoterel.nspname, rel->remoterel.relname)));
	}

	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("logical replication target relation \"%s.%s\" has "
					"neither REPLICA IDENTITY index nor PRIMARY "
					"KEY and published relation does not have "
					"REPLICA IDENTITY FULL",
					rel->remoterel.nspname, rel->remoterel.relname)));
}

/*
 * Handle UPDATE message.
 *
 * TODO: FDW support
 */
static void
apply_handle_update(StringInfo s)
{
	LogicalRepRelMapEntry *rel;
	LogicalRepRelId relid;
	Oid			idxoid;
	EState	   *estate;
	EPQState	epqstate;
	LogicalRepTupleData oldtup;
	LogicalRepTupleData newtup;
	bool		has_oldtup;
	TupleTableSlot *localslot;
	TupleTableSlot *remoteslot;
	bool		found;
	MemoryContext oldctx;

	if (handle_streamed_transaction('U', s))
		return;

	ensure_transaction();

	relid = logicalrep_read_update(s, &has_oldtup, &oldtup,
								   &newtup);
	rel = logicalrep_rel_open(relid, RowExclusiveLock);
	if (!should_apply_changes_for_rel(rel))
	{
		/*
		 * The relation can't become interesting in the middle of the
		 * transaction so it's safe to unlock it.
		 */
		logicalrep_rel_close(rel, RowExclusiveLock);
		return;
	}

	/* Check if we can do the update. */
	check_relation_updatable(rel);

	/* Initialize the executor state. */
	estate = create_estate_for_relation(rel);
	remoteslot = ExecInitExtraTupleSlot(estate,
										RelationGetDescr(rel->localrel));
	localslot = ExecInitExtraTupleSlot(estate,
									   RelationGetDescr(rel->localrel));
	EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1);

	PushActiveSnapshot(GetTransactionSnapshot());
	ExecOpenIndices(estate->es_result_relation_info, false);

	/* Build the search tuple. */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	slot_store_cstrings(remoteslot, rel,
						has_oldtup ? oldtup.values : newtup.values);
	MemoryContextSwitchTo(oldctx);

	/*
	 * Try to find tuple using either replica identity index, primary key or
	 * if needed, sequential scan.
	 */
	idxoid = GetRelationIdentityOrPK(rel->localrel);
	Assert(OidIsValid(idxoid) ||
		   (rel->remoterel.replident == REPLICA_IDENTITY_FULL && has_oldtup));

	if (OidIsValid(idxoid))
		found = RelationFindReplTupleByIndex(rel->localrel, idxoid,
											 LockTupleExclusive,
											 remoteslot, localslot);
	else
		found = RelationFindReplTupleSeq(rel->localrel, LockTupleExclusive,
										 remoteslot, localslot);

	ExecClearTuple(remoteslot);

	/*
	 * Tuple found.
	 *
	 * Note this will fail if there are other conflicting unique indexes.
	 */
	if (found)
	{
		/* Process and store remote tuple in the slot */
		oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
		ExecStoreTuple(localslot->tts_tuple, remoteslot, InvalidBuffer, false);
		slot_modify_cstrings(remoteslot, rel, newtup.values, newtup.changed);
		MemoryContextSwitchTo(oldctx);

		EvalPlanQualSetSlot(&epqstate, remoteslot);

		/* Do the actual update. */
		ExecSimpleRelationUpdate(estate, &epqstate, localslot, remoteslot);
	}
	else
	{
		/*
		 * The tuple to be updated could not be found.
		 *
		 * TODO what to do here, change the log level to LOG perhaps?
		 */
		elog(DEBUG1,
			 "logical replication did not find row for update "
			 "in replication target relation \"%s\"",
			 RelationGetRelationName(rel->localrel));
	}

	/* Cleanup. */
	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();

	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(estate);

	EvalPlanQualEnd(&epqstate);
	ExecResetTupleTable(estate->es_tupleTable, false);
	FreeExecutorState(estate);

	logicalrep_rel_close(rel, NoLock);

	CommandCounterIncrement();
}

/*
 * Handle DELETE message.
 *
 * TODO: FDW support
 */
static void
apply_handle_delete(StringInfo s)
{
	LogicalRepRelMapEntry *rel;
	LogicalRepTupleData oldtup;
	LogicalRepRelId relid;
	Oid			idxoid;
	EState	   *estate;
	EPQState	epqstate;
	TupleTableSlot *remoteslot;
	TupleTableSlot *localslot;
	bool		found;
	MemoryContext oldctx;

	if (handle_streamed_transaction('D', s))
		return;

	ensure_transaction();

	relid = logicalrep_read_delete(s, &oldtup);
	rel = logicalrep_rel_open(relid, RowExclusiveLock);
	if (!should_apply_changes_for_rel(rel))
	{
		/*
		 * The relation can't become interesting in the middle of the
		 * transaction so it's safe to unlock it.
		 */
		logicalrep_rel_close(rel, RowExclusiveLock);
		return;
	}

	/* Check if we can do the delete. */
	check_relation_updatable(rel);

	/* Initialize the executor state. */
	estate = create_estate_for_relation(rel);
	remoteslot = ExecInitExtraTupleSlot(estate,
										RelationGetDescr(rel->localrel));
	localslot = ExecInitExtraTupleSlot(estate,
									   RelationGetDescr(rel->localrel));
	EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1);

	PushActiveSnapshot(GetTransactionSnapshot());
	ExecOpenIndices(estate->es_result_relation_info, false);

	/* Find the tuple using the replica identity index. */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	slot_store_cstrings(remoteslot, rel, oldtup.values);
	MemoryContextSwitchTo(oldctx);

	/*
	 * Try to find tuple using either replica identity index, primary key or
	 * if needed, sequential scan.
	 */
	idxoid = GetRelationIdentityOrPK(rel->localrel);
	Assert(OidIsValid(idxoid) ||
		   (rel->remoterel.replident == REPLICA_IDENTITY_FULL));

	if (OidIsValid(idxoid))
		found = RelationFindReplTupleByIndex(rel->localrel, idxoid,
											 LockTupleExclusive,
											 remoteslot, localslot);
	else
		found = RelationFindReplTupleSeq(rel->localrel, LockTupleExclusive,
										 remoteslot, localslot);
	/* If found delete it. */
	if (found)
	{
		EvalPlanQualSetSlot(&epqstate, localslot);

		/* Do the actual delete. */
		ExecSimpleRelationDelete(estate, &epqstate, localslot);
	}
	else
	{
		/* The tuple to be deleted could not be found. */
		ereport(DEBUG1,
				(errmsg("logical replication could not find row for delete "
						"in replication target relation \"%s\"",
						RelationGetRelationName(rel->localrel))));
	}

	/* Cleanup. */
	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();

	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(estate);

	EvalPlanQualEnd(&epqstate);
	ExecResetTupleTable(estate->es_tupleTable, false);
	FreeExecutorState(estate);

	logicalrep_rel_close(rel, NoLock);

	CommandCounterIncrement();
}


/*
 * Logical replication protocol message dispatcher.
 */
static void
apply_dispatch(StringInfo s)
{
	char		action = pq_getmsgbyte(s);

	switch (action)
	{
			/* BEGIN */
		case 'B':
			apply_handle_begin(s);
			break;
			/* COMMIT */
		case 'C':
			apply_handle_commit(s);
			break;
			/* INSERT */
		case 'I':
			apply_handle_insert(s);
			break;
			/* UPDATE */
		case 'U':
			apply_handle_update(s);
			break;
			/* DELETE */
		case 'D':
			apply_handle_delete(s);
			break;
			/* RELATION */
		case 'R':
			apply_handle_relation(s);
			break;
			/* TYPE */
		case 'Y':
			apply_handle_type(s);
			break;
			/* ORIGIN */
		case 'O':
			apply_handle_origin(s);
			break;
			/* STREAM START */
		case 'S':
			apply_handle_stream_start(s);
			break;
			/* STREAM END */
		case 'E':
			apply_handle_stream_stop(s);
			break;
			/* STREAM ABORT */
		case 'A':
			apply_handle_stream_abort(s);
			break;
			/* STREAM COMMIT */
		case 'c':
			apply_handle_stream_commit(s);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("invalid logical replication message type \"%c\"", action)));
	}
}

/*
 * Figure out which write/flush positions to report to the walsender process.
 *
 * We can't simply report back the last LSN the walsender sent us because the
 * local transaction might not yet be flushed to disk locally. Instead we
 * build a list that associates local with remote LSNs for every commit. When
 * reporting back the flush position to the sender we iterate that list and
 * check which entries on it are already locally flushed. Those we can report
 * as having been flushed.
 *
 * The have_pending_txes is true if there are outstanding transactions that
 * need to be flushed.
 */
static void
get_flush_position(XLogRecPtr *write, XLogRecPtr *flush,
				   bool *have_pending_txes)
{
	dlist_mutable_iter iter;
	XLogRecPtr	local_flush = GetFlushRecPtr();

	*write = InvalidXLogRecPtr;
	*flush = InvalidXLogRecPtr;

	dlist_foreach_modify(iter, &lsn_mapping)
	{
		FlushPosition *pos =
		dlist_container(FlushPosition, node, iter.cur);

		*write = pos->remote_end;

		if (pos->local_end <= local_flush)
		{
			*flush = pos->remote_end;
			dlist_delete(iter.cur);
			pfree(pos);
		}
		else
		{
			/*
			 * Don't want to uselessly iterate over the rest of the list which
			 * could potentially be long. Instead get the last element and
			 * grab the write position from there.
			 */
			pos = dlist_tail_element(FlushPosition, node,
									 &lsn_mapping);
			*write = pos->remote_end;
			*have_pending_txes = true;
			return;
		}
	}

	*have_pending_txes = !dlist_is_empty(&lsn_mapping);
}

/*
 * Store current remote/local lsn pair in the tracking list.
 */
static void
store_flush_position(XLogRecPtr remote_lsn)
{
	FlushPosition *flushpos;

	/* Need to do this in permanent context */
	MemoryContextSwitchTo(ApplyContext);

	/* Track commit lsn  */
	flushpos = (FlushPosition *) palloc(sizeof(FlushPosition));
	flushpos->local_end = XactLastCommitEnd;
	flushpos->remote_end = remote_lsn;

	dlist_push_tail(&lsn_mapping, &flushpos->node);
	MemoryContextSwitchTo(ApplyMessageContext);
}


/* Update statistics of the worker. */
static void
UpdateWorkerStats(XLogRecPtr last_lsn, TimestampTz send_time, bool reply)
{
	MyLogicalRepWorker->last_lsn = last_lsn;
	MyLogicalRepWorker->last_send_time = send_time;
	MyLogicalRepWorker->last_recv_time = GetCurrentTimestamp();
	if (reply)
	{
		MyLogicalRepWorker->reply_lsn = last_lsn;
		MyLogicalRepWorker->reply_time = send_time;
	}
}

/*
 * Cleanup function.
 *
 * Called on logical replication worker exit.
 */
static void
worker_onexit(int code, Datum arg)
{
	int	i;

	elog(LOG, "cleanup files for %d transactions", nxids);

	for (i = nxids-1; i >= 0; i--)
		stream_cleanup_files(MyLogicalRepWorker->subid, xids[i]);
}

/*
 * Apply main loop.
 */
static void
LogicalRepApplyLoop(XLogRecPtr last_received)
{
	/*
	 * Init the ApplyMessageContext which we clean up after each replication
	 * protocol message.
	 */
	ApplyMessageContext = AllocSetContextCreate(ApplyContext,
												"ApplyMessageContext",
												ALLOCSET_DEFAULT_SIZES);

	/* do cleanup on worker exit (e.g. after DROP SUBSCRIPTION) */
	before_shmem_exit(worker_onexit, (Datum) 0);

	/* mark as idle, before starting to loop */
	pgstat_report_activity(STATE_IDLE, NULL);

	for (;;)
	{
		pgsocket	fd = PGINVALID_SOCKET;
		int			rc;
		int			len;
		char	   *buf = NULL;
		bool		endofstream = false;
		TimestampTz last_recv_timestamp = GetCurrentTimestamp();
		bool		ping_sent = false;
		long		wait_time;

		CHECK_FOR_INTERRUPTS();

		MemoryContextSwitchTo(ApplyMessageContext);

		len = walrcv_receive(wrconn, &buf, &fd);

		if (len != 0)
		{
			/* Process the data */
			for (;;)
			{
				CHECK_FOR_INTERRUPTS();

				if (len == 0)
				{
					break;
				}
				else if (len < 0)
				{
					ereport(LOG,
							(errmsg("data stream from publisher has ended")));
					endofstream = true;
					break;
				}
				else
				{
					int			c;
					StringInfoData s;

					/* Reset timeout. */
					last_recv_timestamp = GetCurrentTimestamp();
					ping_sent = false;

					/* Ensure we are reading the data into our memory context. */
					MemoryContextSwitchTo(ApplyMessageContext);

					s.data = buf;
					s.len = len;
					s.cursor = 0;
					s.maxlen = -1;

					c = pq_getmsgbyte(&s);

					if (c == 'w')
					{
						XLogRecPtr	start_lsn;
						XLogRecPtr	end_lsn;
						TimestampTz send_time;

						start_lsn = pq_getmsgint64(&s);
						end_lsn = pq_getmsgint64(&s);
						send_time = pq_getmsgint64(&s);

						if (last_received < start_lsn)
							last_received = start_lsn;

						if (last_received < end_lsn)
							last_received = end_lsn;

						UpdateWorkerStats(last_received, send_time, false);

						apply_dispatch(&s);
					}
					else if (c == 'k')
					{
						XLogRecPtr	end_lsn;
						TimestampTz timestamp;
						bool		reply_requested;

						end_lsn = pq_getmsgint64(&s);
						timestamp = pq_getmsgint64(&s);
						reply_requested = pq_getmsgbyte(&s);

						if (last_received < end_lsn)
							last_received = end_lsn;

						send_feedback(last_received, reply_requested, false);
						UpdateWorkerStats(last_received, timestamp, true);
					}
					/* other message types are purposefully ignored */

					MemoryContextReset(ApplyMessageContext);
				}

				len = walrcv_receive(wrconn, &buf, &fd);
			}
		}

		/* confirm all writes so far */
		send_feedback(last_received, false, false);

		if (!in_remote_transaction)
		{
			/*
			 * If we didn't get any transactions for a while there might be
			 * unconsumed invalidation messages in the queue, consume them
			 * now.
			 */
			AcceptInvalidationMessages();
			maybe_reread_subscription();

			/* Process any table synchronization changes. */
			process_syncing_tables(last_received);
		}

		/* Cleanup the memory. */
		MemoryContextResetAndDeleteChildren(ApplyMessageContext);
		MemoryContextSwitchTo(TopMemoryContext);

		/* Check if we need to exit the streaming loop. */
		if (endofstream)
		{
			TimeLineID	tli;

			walrcv_endstreaming(wrconn, &tli);
			break;
		}

		/*
		 * Wait for more data or latch.  If we have unflushed transactions,
		 * wake up after WalWriterDelay to see if they've been flushed yet (in
		 * which case we should send a feedback message).  Otherwise, there's
		 * no particular urgency about waking up unless we get data or a
		 * signal.
		 */
		if (!dlist_is_empty(&lsn_mapping))
			wait_time = WalWriterDelay;
		else
			wait_time = NAPTIME_PER_CYCLE;

		rc = WaitLatchOrSocket(MyLatch,
							   WL_SOCKET_READABLE | WL_LATCH_SET |
							   WL_TIMEOUT | WL_POSTMASTER_DEATH,
							   fd, wait_time,
							   WAIT_EVENT_LOGICAL_APPLY_MAIN);

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (rc & WL_TIMEOUT)
		{
			/*
			 * We didn't receive anything new. If we haven't heard anything
			 * from the server for more than wal_receiver_timeout / 2, ping
			 * the server. Also, if it's been longer than
			 * wal_receiver_status_interval since the last update we sent,
			 * send a status update to the master anyway, to report any
			 * progress in applying WAL.
			 */
			bool		requestReply = false;

			/*
			 * Check if time since last receive from standby has reached the
			 * configured limit.
			 */
			if (wal_receiver_timeout > 0)
			{
				TimestampTz now = GetCurrentTimestamp();
				TimestampTz timeout;

				timeout =
					TimestampTzPlusMilliseconds(last_recv_timestamp,
												wal_receiver_timeout);

				if (now >= timeout)
					ereport(ERROR,
							(errmsg("terminating logical replication worker due to timeout")));

				/*
				 * We didn't receive anything new, for half of receiver
				 * replication timeout. Ping the server.
				 */
				if (!ping_sent)
				{
					timeout = TimestampTzPlusMilliseconds(last_recv_timestamp,
														  (wal_receiver_timeout / 2));
					if (now >= timeout)
					{
						requestReply = true;
						ping_sent = true;
					}
				}
			}

			send_feedback(last_received, requestReply, requestReply);
		}
	}
}

/*
 * Send a Standby Status Update message to server.
 *
 * 'recvpos' is the latest LSN we've received data to, force is set if we need
 * to send a response to avoid timeouts.
 */
static void
send_feedback(XLogRecPtr recvpos, bool force, bool requestReply)
{
	static StringInfo reply_message = NULL;
	static TimestampTz send_time = 0;

	static XLogRecPtr last_recvpos = InvalidXLogRecPtr;
	static XLogRecPtr last_writepos = InvalidXLogRecPtr;
	static XLogRecPtr last_flushpos = InvalidXLogRecPtr;

	XLogRecPtr	writepos;
	XLogRecPtr	flushpos;
	TimestampTz now;
	bool		have_pending_txes;

	/*
	 * If the user doesn't want status to be reported to the publisher, be
	 * sure to exit before doing anything at all.
	 */
	if (!force && wal_receiver_status_interval <= 0)
		return;

	/* It's legal to not pass a recvpos */
	if (recvpos < last_recvpos)
		recvpos = last_recvpos;

	get_flush_position(&writepos, &flushpos, &have_pending_txes);

	/*
	 * No outstanding transactions to flush, we can report the latest received
	 * position. This is important for synchronous replication.
	 */
	if (!have_pending_txes)
		flushpos = writepos = recvpos;

	if (writepos < last_writepos)
		writepos = last_writepos;

	if (flushpos < last_flushpos)
		flushpos = last_flushpos;

	now = GetCurrentTimestamp();

	/* if we've already reported everything we're good */
	if (!force &&
		writepos == last_writepos &&
		flushpos == last_flushpos &&
		!TimestampDifferenceExceeds(send_time, now,
									wal_receiver_status_interval * 1000))
		return;
	send_time = now;

	if (!reply_message)
	{
		MemoryContext oldctx = MemoryContextSwitchTo(ApplyContext);

		reply_message = makeStringInfo();
		MemoryContextSwitchTo(oldctx);
	}
	else
		resetStringInfo(reply_message);

	pq_sendbyte(reply_message, 'r');
	pq_sendint64(reply_message, recvpos);	/* write */
	pq_sendint64(reply_message, flushpos);	/* flush */
	pq_sendint64(reply_message, writepos);	/* apply */
	pq_sendint64(reply_message, now);	/* sendTime */
	pq_sendbyte(reply_message, requestReply);	/* replyRequested */

	elog(DEBUG2, "sending feedback (force %d) to recv %X/%X, write %X/%X, flush %X/%X",
		 force,
		 (uint32) (recvpos >> 32), (uint32) recvpos,
		 (uint32) (writepos >> 32), (uint32) writepos,
		 (uint32) (flushpos >> 32), (uint32) flushpos
		);

	walrcv_send(wrconn, reply_message->data, reply_message->len);

	if (recvpos > last_recvpos)
		last_recvpos = recvpos;
	if (writepos > last_writepos)
		last_writepos = writepos;
	if (flushpos > last_flushpos)
		last_flushpos = flushpos;
}

/*
 * Reread subscription info if needed. Most changes will be exit.
 */
static void
maybe_reread_subscription(void)
{
	MemoryContext oldctx;
	Subscription *newsub;
	bool		started_tx = false;

	/* When cache state is valid there is nothing to do here. */
	if (MySubscriptionValid)
		return;

	/* This function might be called inside or outside of transaction. */
	if (!IsTransactionState())
	{
		StartTransactionCommand();
		started_tx = true;
	}

	/* Ensure allocations in permanent context. */
	oldctx = MemoryContextSwitchTo(ApplyContext);

	newsub = GetSubscription(MyLogicalRepWorker->subid, true);

	/*
	 * Exit if the subscription was removed. This normally should not happen
	 * as the worker gets killed during DROP SUBSCRIPTION.
	 */
	if (!newsub)
	{
		ereport(LOG,
				(errmsg("logical replication apply worker for subscription \"%s\" will "
						"stop because the subscription was removed",
						MySubscription->name)));

		proc_exit(0);
	}

	/*
	 * Exit if the subscription was disabled. This normally should not happen
	 * as the worker gets killed during ALTER SUBSCRIPTION ... DISABLE.
	 */
	if (!newsub->enabled)
	{
		ereport(LOG,
				(errmsg("logical replication apply worker for subscription \"%s\" will "
						"stop because the subscription was disabled",
						MySubscription->name)));

		proc_exit(0);
	}

	/*
	 * Exit if connection string was changed. The launcher will start new
	 * worker.
	 */
	if (strcmp(newsub->conninfo, MySubscription->conninfo) != 0)
	{
		ereport(LOG,
				(errmsg("logical replication apply worker for subscription \"%s\" will "
						"restart because the connection information was changed",
						MySubscription->name)));

		proc_exit(0);
	}

	/*
	 * Exit if subscription name was changed (it's used for
	 * fallback_application_name). The launcher will start new worker.
	 */
	if (strcmp(newsub->name, MySubscription->name) != 0)
	{
		ereport(LOG,
				(errmsg("logical replication apply worker for subscription \"%s\" will "
						"restart because subscription was renamed",
						MySubscription->name)));

		proc_exit(0);
	}

	/* !slotname should never happen when enabled is true. */
	Assert(newsub->slotname);

	/*
	 * We need to make new connection to new slot if slot name has changed so
	 * exit here as well if that's the case.
	 */
	if (strcmp(newsub->slotname, MySubscription->slotname) != 0)
	{
		ereport(LOG,
				(errmsg("logical replication apply worker for subscription \"%s\" will "
						"restart because the replication slot name was changed",
						MySubscription->name)));

		proc_exit(0);
	}

	/*
	 * Exit if publication list was changed. The launcher will start new
	 * worker.
	 */
	if (!equal(newsub->publications, MySubscription->publications))
	{
		ereport(LOG,
				(errmsg("logical replication apply worker for subscription \"%s\" will "
						"restart because subscription's publications were changed",
						MySubscription->name)));

		proc_exit(0);
	}

	/* Check for other changes that should never happen too. */
	if (newsub->dbid != MySubscription->dbid)
	{
		elog(ERROR, "subscription %u changed unexpectedly",
			 MyLogicalRepWorker->subid);
	}

	/* Clean old subscription info and switch to new one. */
	FreeSubscription(MySubscription);
	MySubscription = newsub;

	MemoryContextSwitchTo(oldctx);

	/* Change synchronous commit according to the user's wishes */
	SetConfigOption("synchronous_commit", MySubscription->synccommit,
					PGC_BACKEND, PGC_S_OVERRIDE);

	if (started_tx)
		CommitTransactionCommand();

	MySubscriptionValid = true;
}

/*
 * Callback from subscription syscache invalidation.
 */
static void
subscription_change_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	MySubscriptionValid = false;
}

/*
 * subxact_info_write
 *	  Store information about subxacts for a toplevel transaction.
 *
 * For each subxact we store offset of it's first change in the main file.
 * The file is always over-written as a whole, and we also include CRC32C
 * checksum of the information.
 *
 * XXX We should only store subxacts that were not aborted yet.
 *
 * XXX Maybe we should only include the checksum when the cluster is
 * initialized with checksums?
 *
 * XXX Add calls to pgstat_report_wait_start/pgstat_report_wait_end.
 */
static void
subxact_info_write(Oid subid, TransactionId xid)
{
	int			fd;
	char		path[MAXPGPATH];
	uint32		checksum;
	Size		len;

	Assert(TransactionIdIsValid(xid));

	subxact_filename(path, subid, xid);

	fd = OpenTransientFile(path, O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY);
	if (fd < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m",
						path)));
		return;
	}

	len = sizeof(SubXactInfo) * nsubxacts;

	/* compute the checksum */
	INIT_CRC32C(checksum);
	COMP_CRC32C(checksum, (char *) &nsubxacts, sizeof(nsubxacts));
	COMP_CRC32C(checksum, (char *) subxacts, len);
	FIN_CRC32C(checksum);

	pgstat_report_wait_start(WAIT_EVENT_LOGICAL_SUBXACT_WRITE);

	if (write(fd, &checksum, sizeof(checksum)) != sizeof(checksum))
	{
		int			save_errno = errno;

		CloseTransientFile(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m",
						path)));
		return;
	}

	if (write(fd, &nsubxacts, sizeof(nsubxacts)) != sizeof(nsubxacts))
	{
		int			save_errno = errno;

		CloseTransientFile(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m",
						path)));
		return;
	}

	if ((len > 0) && (write(fd, subxacts, len) != len))
	{
		int			save_errno = errno;

		CloseTransientFile(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m",
						path)));
		return;
	}

	pgstat_report_wait_end();

	/*
	 * We don't need to fsync or anything, as we'll recreate the files after a
	 * crash from scratch. So just close the file.
	 */
	CloseTransientFile(fd);

	/*
	 * But we free the memory allocated for subxact info. There might be one
	 * exceptional transaction with many subxacts, and we don't want to keep
	 * the memory allocated forewer.
	 *
	 */
	if (subxacts)
		pfree(subxacts);

	subxacts = NULL;
	subxact_last = InvalidTransactionId;
	nsubxacts = 0;
	nsubxacts_max = 0;
}

/*
 * subxact_info_read
 *	  Restore information about subxacts of a streamed transaction.
 *
 * Read information about subxacts into the global variables, and while
 * reading the information verify the checksum.
 *
 * XXX Add calls to pgstat_report_wait_start/pgstat_report_wait_end.
 *
 * XXX Do we need to allocate it in TopMemoryContext?
 */
static void
subxact_info_read(Oid subid, TransactionId xid)
{
	int			fd;
	char		path[MAXPGPATH];
	uint32		checksum;
	uint32		checksum_new;
	Size		len;
	MemoryContext oldctx;

	Assert(TransactionIdIsValid(xid));
	Assert(!subxacts);
	Assert(nsubxacts == 0);
	Assert(nsubxacts_max == 0);

	subxact_filename(path, subid, xid);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						path)));
		return;
	}

	pgstat_report_wait_start(WAIT_EVENT_LOGICAL_SUBXACT_READ);

	/* read the checksum */
	if (read(fd, &checksum, sizeof(checksum)) != sizeof(checksum))
	{
		int			save_errno = errno;

		CloseTransientFile(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m",
						path)));
		return;
	}

	/* read number of subxact items */
	if (read(fd, &nsubxacts, sizeof(nsubxacts)) != sizeof(nsubxacts))
	{
		int			save_errno = errno;

		CloseTransientFile(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m",
						path)));
		return;
	}

	pgstat_report_wait_end();

	len = sizeof(SubXactInfo) * nsubxacts;

	/* we keep the maximum as a power of 2 */
	nsubxacts_max = 1 << my_log2(nsubxacts);

	/* subxacts are long-lived */
	oldctx = MemoryContextSwitchTo(TopMemoryContext);
	subxacts = palloc(nsubxacts_max * sizeof(SubXactInfo));
	MemoryContextSwitchTo(oldctx);

	pgstat_report_wait_start(WAIT_EVENT_LOGICAL_SUBXACT_READ);

	if ((len > 0) && ((read(fd, subxacts, len)) != len))
	{
		int			save_errno = errno;

		CloseTransientFile(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m",
						path)));
		return;
	}

	pgstat_report_wait_end();

	/* recompute the checksum */
	INIT_CRC32C(checksum_new);
	COMP_CRC32C(checksum_new, (char *) &nsubxacts, sizeof(nsubxacts));
	COMP_CRC32C(checksum_new, (char *) subxacts, len);
	FIN_CRC32C(checksum_new);

	if (checksum_new != checksum)
		ereport(ERROR,
				(errmsg("checksum failure when reading subxacts")));

	CloseTransientFile(fd);
}

/*
 * subxact_info_add
 *	  Add information about a subxact (offset in the main file).
 *
 * XXX Do we need to allocate it in TopMemoryContext?
 */
static void
subxact_info_add(TransactionId xid)
{
	int64		i;

	/*
	 * If the XID matches the toplevel transaction, we don't want to add it.
	 */
	if (stream_xid == xid)
		return;

	/*
	 * In most cases we're checking the same subxact as we've already seen in
	 * the last call, so make ure just ignore it (this change comes later).
	 */
	if (subxact_last == xid)
		return;

	/* OK, remember we're processing this XID. */
	subxact_last = xid;

	/*
	 * Check if the transaction is already present in the array of subxact. We
	 * intentionally scan the array from the tail, because we're likely adding
	 * a change for the most recent subtransactions.
	 *
	 * XXX Can we rely on the subxact XIDs arriving in sorted order? That
	 * would allow us to use binary search here.
	 */
	for (i = nsubxacts; i > 0; i--)
	{
		/* found, so we're done */
		if (subxacts[i - 1].xid == xid)
			return;
	}

	/* This is a new subxact, so we need to add it to the array. */

	if (nsubxacts == 0)
	{
		MemoryContext oldctx;

		nsubxacts_max = 128;
		oldctx = MemoryContextSwitchTo(TopMemoryContext);
		subxacts = palloc(nsubxacts_max * sizeof(SubXactInfo));
		MemoryContextSwitchTo(oldctx);
	}
	else if (nsubxacts == nsubxacts_max)
	{
		nsubxacts_max *= 2;
		subxacts = repalloc(subxacts, nsubxacts_max * sizeof(SubXactInfo));
	}

	subxacts[nsubxacts].xid = xid;
	subxacts[nsubxacts].offset = lseek(stream_fd, 0, SEEK_END);

	nsubxacts++;
}

/* format filename for file containing the info about subxacts */
static void
subxact_filename(char *path, Oid subid, TransactionId xid)
{
	char		tempdirpath[MAXPGPATH];

	TempTablespacePath(tempdirpath, DEFAULTTABLESPACE_OID);

	/*
	 * We might need to create the tablespace's tempfile directory, if no
	 * one has yet done so.
	 *
	 * Don't check for error from mkdir; it could fail if the directory
	 * already exists (maybe someone else just did the same thing).  If
	 * it doesn't work then we'll bomb out when opening the file
	 */
	mkdir(tempdirpath, S_IRWXU);

	snprintf(path, MAXPGPATH, "%s/logical-%u-%u.subxacts",
			 tempdirpath, subid, xid);
}

/* format filename for file containing serialized changes */
static void
changes_filename(char *path, Oid subid, TransactionId xid)
{
	char		tempdirpath[MAXPGPATH];

	TempTablespacePath(tempdirpath, DEFAULTTABLESPACE_OID);

	/*
	 * We might need to create the tablespace's tempfile directory, if no
	 * one has yet done so.
	 *
	 * Don't check for error from mkdir; it could fail if the directory
	 * already exists (maybe someone else just did the same thing).  If
	 * it doesn't work then we'll bomb out when opening the file
	 */
	mkdir(tempdirpath, S_IRWXU);

	snprintf(path, MAXPGPATH, "%s/logical-%u-%u.changes",
			 tempdirpath, subid, xid);
}

/*
 * stream_cleanup_files
 *	  Cleanup files for a subscription / toplevel transaction.
 *
 * Remove files with serialized changes and subxact info for a particular
 * toplevel transaction. Each subscription has a separate set of files.
 */
static void
stream_cleanup_files(Oid subid, TransactionId xid)
{
	int			i;
	char		path[MAXPGPATH];

	subxact_filename(path, subid, xid);

	if ((unlink(path) < 0) && (errno != ENOENT))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove file \"%s\": %m", path)));

	changes_filename(path, subid, xid);

	if ((unlink(path) < 0) && (errno != ENOENT))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove file \"%s\": %m", path)));

	/*
	 * Cleanup the XID from the array - find the XID in the array and
	 * remove it by shifting all the remaining elements. The array is
	 * bound to be fairly small (maximum number of in-progress xacts,
	 * so max_connections + max_prepared_transactions) so simply loop
	 * through the array and find index of the XID. Then move the rest
	 * of the array by one element to the left.
	 */
	for (i = 0; i < nxids; i++)
	{
		if (xids[i] == xid)
			break;
	}

	/* subtract now, it makes the next step simpler */
	nxids--;

	/* shift to the left (unless the XID was last in the array) */
	if (i < nxids)
		memmove(&xids[i], &xids[i+1], sizeof(TransactionId) * (nxids - i));
}

/*
 * stream_open_file
 *	  Open file we'll use to serialize changes for a toplevel transaction.
 *
 * Open a file for streamed changes from a toplevel transaction identified
 * by stream_xid (global variable). If it's the first chunk of streamed
 * changes for this transaction, perform cleanup by removing existing
 * files after a possible previous crash.
 *
 * This can only be called at the beginning of a "streaming" block, i.e.
 * between stream_start/stream_stop messages from the upstream.
 */
static void
stream_open_file(Oid subid, TransactionId xid, bool first_segment)
{
	char		path[MAXPGPATH];
	int			flags;

	Assert(in_streamed_transaction);
	Assert(OidIsValid(subid));
	Assert(TransactionIdIsValid(xid));
	Assert(stream_fd == -1);

	/*
	 * If this is the first segment for this transaction, try removing
	 * existing files (if there are any, possibly after a crash).
	 */
	if (first_segment)
	{
		MemoryContext	oldcxt;

		stream_cleanup_files(subid, xid);

		oldcxt = MemoryContextSwitchTo(TopMemoryContext);

		/*
		 * We need to remember the XIDs we spilled to files, so that we can
		 * remove them at worker exit (e.g. after DROP SUBSCRIPTION).
		 *
		 * The number of XIDs we may need to track is fairly small, because
		 * we can only stream toplevel xacts (so limited by max_connections
		 * and max_prepared_transactions), and we only stream the large ones.
		 * So we simply keep the XIDs in an unsorted array. If the number of
		 * xacts gets large for some reason (e.g. very high max_connections),
		 * a more elaborate approach might be better - e.g. sorted array, to
		 * speed-up the lookups.
		 */
		if (nxids == maxnxids)	/* array of XIDs is full */
		{
			if (!xids)
			{
				maxnxids = 64;
				xids = palloc(maxnxids * sizeof(TransactionId));
			}
			else
			{
				maxnxids = 2 * maxnxids;
				xids = repalloc(xids, maxnxids * sizeof(TransactionId));
			}
		}

		xids[nxids++] = xid;

		MemoryContextSwitchTo(oldcxt);
	}

	changes_filename(path, subid, xid);

	elog(DEBUG1, "opening file '%s' for streamed changes", path);

	/*
	 * If this is the first streamed segment, the file must not exist, so
	 * make sure we're the ones creating it. Otherwise just open the file
	 * for writing, in append mode.
	 */
	if (first_segment)
		flags = (O_WRONLY | O_CREAT | O_EXCL | PG_BINARY);
	else
		flags = (O_WRONLY | O_APPEND | PG_BINARY);

	stream_fd = OpenTransientFile(path, flags);

	if (stream_fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						path)));
}

/*
 * stream_close_file
 *	  Close the currently open file with streamed changes.
 *
 * This can only be called at the beginning of a "streaming" block, i.e.
 * between stream_start/stream_stop messages from the upstream.
 */
static void
stream_close_file(void)
{
	Assert(in_streamed_transaction);
	Assert(TransactionIdIsValid(stream_xid));
	Assert(stream_fd != -1);

	CloseTransientFile(stream_fd);

	stream_xid = InvalidTransactionId;
	stream_fd = -1;
}

/*
 * stream_write_change
 *	  Serialize a change to a file for the current toplevel transaction.
 *
 * The change is serialied in a simple format, with length (not including
 * the length), action code (identifying the message type) and message
 * contents (without the subxact TransactionId value).
 *
 * XXX The subxact file includes CRC32C of the contents. Maybe we should
 * include something like that here too, but doing so will not be as
 * straighforward, because we write the file in chunks.
 */
static void
stream_write_change(char action, StringInfo s)
{
	int			len;

	Assert(in_streamed_transaction);
	Assert(TransactionIdIsValid(stream_xid));
	Assert(stream_fd != -1);

	/* total on-disk size, including the action type character */
	len = (s->len - s->cursor) + sizeof(char);

	pgstat_report_wait_start(WAIT_EVENT_LOGICAL_CHANGES_WRITE);

	/* first write the size */
	if (write(stream_fd, &len, sizeof(len)) != sizeof(len))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not serialize streamed change to file: %m")));

	/* then the action */
	if (write(stream_fd, &action, sizeof(action)) != sizeof(action))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not serialize streamed change to file: %m")));

	/* and finally the remaining part of the buffer (after the XID) */
	len = (s->len - s->cursor);

	if (write(stream_fd, &s->data[s->cursor], len) != len)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not serialize streamed change to file: %m")));

	pgstat_report_wait_end();
}

/* SIGHUP: set flag to reload configuration at next convenient time */
static void
logicalrep_worker_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGHUP = true;

	/* Waken anything waiting on the process latch */
	SetLatch(MyLatch);

	errno = save_errno;
}

/* Logical Replication Apply worker entry point */
void
ApplyWorkerMain(Datum main_arg)
{
	int			worker_slot = DatumGetInt32(main_arg);
	MemoryContext oldctx;
	char		originname[NAMEDATALEN];
	XLogRecPtr	origin_startpos;
	char	   *myslotname;
	WalRcvStreamOptions options;

	/* Attach to slot */
	logicalrep_worker_attach(worker_slot);

	/* Setup signal handling */
	pqsignal(SIGHUP, logicalrep_worker_sighup);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* Initialise stats to a sanish value */
	MyLogicalRepWorker->last_send_time = MyLogicalRepWorker->last_recv_time =
		MyLogicalRepWorker->reply_time = GetCurrentTimestamp();

	/* Load the libpq-specific functions */
	load_file("libpqwalreceiver", false);

	Assert(CurrentResourceOwner == NULL);
	CurrentResourceOwner = ResourceOwnerCreate(NULL,
											   "logical replication apply");

	/* Run as replica session replication role. */
	SetConfigOption("session_replication_role", "replica",
					PGC_SUSET, PGC_S_OVERRIDE);

	/* Connect to our database. */
	BackgroundWorkerInitializeConnectionByOid(MyLogicalRepWorker->dbid,
											  MyLogicalRepWorker->userid);

	/* Load the subscription into persistent memory context. */
	ApplyContext = AllocSetContextCreate(TopMemoryContext,
										 "ApplyContext",
										 ALLOCSET_DEFAULT_SIZES);
	StartTransactionCommand();
	oldctx = MemoryContextSwitchTo(ApplyContext);
	MySubscription = GetSubscription(MyLogicalRepWorker->subid, false);
	MySubscriptionValid = true;
	MemoryContextSwitchTo(oldctx);

	/* Setup synchronous commit according to the user's wishes */
	SetConfigOption("synchronous_commit", MySubscription->synccommit,
					PGC_BACKEND, PGC_S_OVERRIDE);

	if (!MySubscription->enabled)
	{
		ereport(LOG,
				(errmsg("logical replication apply worker for subscription \"%s\" will not "
						"start because the subscription was disabled during startup",
						MySubscription->name)));

		proc_exit(0);
	}

	/* Keep us informed about subscription changes. */
	CacheRegisterSyscacheCallback(SUBSCRIPTIONOID,
								  subscription_change_cb,
								  (Datum) 0);

	if (am_tablesync_worker())
		ereport(LOG,
				(errmsg("logical replication table synchronization worker for subscription \"%s\", table \"%s\" has started",
						MySubscription->name, get_rel_name(MyLogicalRepWorker->relid))));
	else
		ereport(LOG,
				(errmsg("logical replication apply worker for subscription \"%s\" has started",
						MySubscription->name)));

	CommitTransactionCommand();

	/* Connect to the origin and start the replication. */
	elog(DEBUG1, "connecting to publisher using connection string \"%s\"",
		 MySubscription->conninfo);

	if (am_tablesync_worker())
	{
		char	   *syncslotname;

		/* This is table synchroniation worker, call initial sync. */
		syncslotname = LogicalRepSyncTableStart(&origin_startpos);

		/* The slot name needs to be allocated in permanent memory context. */
		oldctx = MemoryContextSwitchTo(ApplyContext);
		myslotname = pstrdup(syncslotname);
		MemoryContextSwitchTo(oldctx);

		pfree(syncslotname);
	}
	else
	{
		/* This is main apply worker */
		RepOriginId originid;
		TimeLineID	startpointTLI;
		char	   *err;
		int			server_version;

		myslotname = MySubscription->slotname;

		/*
		 * This shouldn't happen if the subscription is enabled, but guard
		 * against DDL bugs or manual catalog changes.  (libpqwalreceiver will
		 * crash if slot is NULL.)
		 */
		if (!myslotname)
			ereport(ERROR,
					(errmsg("subscription has no replication slot set")));

		/* Setup replication origin tracking. */
		StartTransactionCommand();
		snprintf(originname, sizeof(originname), "pg_%u", MySubscription->oid);
		originid = replorigin_by_name(originname, true);
		if (!OidIsValid(originid))
			originid = replorigin_create(originname);
		replorigin_session_setup(originid);
		replorigin_session_origin = originid;
		origin_startpos = replorigin_session_get_progress(false);
		CommitTransactionCommand();

		wrconn = walrcv_connect(MySubscription->conninfo, true, MySubscription->name,
								&err);
		if (wrconn == NULL)
			ereport(ERROR,
					(errmsg("could not connect to the publisher: %s", err)));

		/*
		 * We don't really use the output identify_system for anything but it
		 * does some initializations on the upstream so let's still call it.
		 */
		(void) walrcv_identify_system(wrconn, &startpointTLI,
									  &server_version);

	}

	/*
	 * Setup callback for syscache so that we know when something changes in
	 * the subscription relation state.
	 */
	CacheRegisterSyscacheCallback(SUBSCRIPTIONRELMAP,
								  invalidate_syncing_table_states,
								  (Datum) 0);

	/* Build logical replication streaming options. */
	options.logical = true;
	options.startpoint = origin_startpos;
	options.slotname = myslotname;
	options.proto.logical.proto_version = LOGICALREP_PROTO_VERSION_NUM;
	options.proto.logical.publication_names = MySubscription->publications;
	options.proto.logical.work_mem = MySubscription->workmem;
	options.proto.logical.streaming = MySubscription->stream;

	/* Start normal logical streaming replication. */
	walrcv_startstreaming(wrconn, &options);

	/* Run the main loop. */
	LogicalRepApplyLoop(origin_startpos);

	proc_exit(0);
}

/*
 * Is current process a logical replication worker?
 */
bool
IsLogicalWorker(void)
{
	return MyLogicalRepWorker != NULL;
}
