/*-------------------------------------------------------------------------
 *
 * pgoutput.c
 *		Logical Replication output plugin
 *
 * Copyright (c) 2012-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/backend/replication/pgoutput/pgoutput.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tupconvert.h"
#include "access/xact.h"
#include "catalog/partition.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_publication_rel.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_coerce.h"
#include "replication/logical.h"
#include "replication/logicalproto.h"
#include "replication/logicalrelation.h"
#include "replication/origin.h"
#include "replication/pgoutput.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

PG_MODULE_MAGIC;

extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

static void pgoutput_startup(LogicalDecodingContext *ctx,
							 OutputPluginOptions *opt, bool is_init);
static void pgoutput_shutdown(LogicalDecodingContext *ctx);
static void pgoutput_begin_txn(LogicalDecodingContext *ctx,
							   ReorderBufferTXN *txn);
static void pgoutput_commit_txn(LogicalDecodingContext *ctx,
								ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void pgoutput_change(LogicalDecodingContext *ctx,
							ReorderBufferTXN *txn, Relation rel,
							ReorderBufferChange *change);
static void pgoutput_truncate(LogicalDecodingContext *ctx,
							  ReorderBufferTXN *txn, int nrelations, Relation relations[],
							  ReorderBufferChange *change);
static void pgoutput_message(LogicalDecodingContext *ctx,
							 ReorderBufferTXN *txn, XLogRecPtr message_lsn,
							 bool transactional, const char *prefix,
							 Size sz, const char *message);
static bool pgoutput_origin_filter(LogicalDecodingContext *ctx,
								   RepOriginId origin_id);
static void pgoutput_begin_prepare_txn(LogicalDecodingContext *ctx,
									   ReorderBufferTXN *txn);
static void pgoutput_prepare_txn(LogicalDecodingContext *ctx,
								 ReorderBufferTXN *txn, XLogRecPtr prepare_lsn);
static void pgoutput_commit_prepared_txn(LogicalDecodingContext *ctx,
										 ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void pgoutput_rollback_prepared_txn(LogicalDecodingContext *ctx,
										   ReorderBufferTXN *txn,
										   XLogRecPtr prepare_end_lsn,
										   TimestampTz prepare_time);
static void pgoutput_stream_start(struct LogicalDecodingContext *ctx,
								  ReorderBufferTXN *txn);
static void pgoutput_stream_stop(struct LogicalDecodingContext *ctx,
								 ReorderBufferTXN *txn);
static void pgoutput_stream_abort(struct LogicalDecodingContext *ctx,
								  ReorderBufferTXN *txn,
								  XLogRecPtr abort_lsn);
static void pgoutput_stream_commit(struct LogicalDecodingContext *ctx,
								   ReorderBufferTXN *txn,
								   XLogRecPtr commit_lsn);
static void pgoutput_stream_prepare_txn(LogicalDecodingContext *ctx,
										ReorderBufferTXN *txn, XLogRecPtr prepare_lsn);

static bool publications_valid;
static bool in_streaming;

static List *LoadPublications(List *pubnames);
static void publication_invalidation_cb(Datum arg, int cacheid,
										uint32 hashvalue);
static void send_relation_and_attrs(Relation relation, TransactionId xid,
									LogicalDecodingContext *ctx);
static void send_repl_origin(LogicalDecodingContext *ctx,
							 RepOriginId origin_id, XLogRecPtr origin_lsn,
							 bool send_origin);

/*
 * Entry in the map used to remember which relation schemas we sent.
 *
 * The schema_sent flag determines if the current schema record for the
 * relation (and for its ancestor if publish_as_relid is set) was already
 * sent to the subscriber (in which case we don't need to send it again).
 *
 * The schema cache on downstream is however updated only at commit time,
 * and with streamed transactions the commit order may be different from
 * the order the transactions are sent in. Also, the (sub) transactions
 * might get aborted so we need to send the schema for each (sub) transaction
 * so that we don't lose the schema information on abort. For handling this,
 * we maintain the list of xids (streamed_txns) for those we have already sent
 * the schema.
 *
 * For partitions, 'pubactions' considers not only the table's own
 * publications, but also those of all of its ancestors.
 */
typedef struct RelationSyncEntry
{
	Oid			relid;			/* relation oid */

	bool		schema_sent;
	List	   *streamed_txns;	/* streamed toplevel transactions with this
								 * schema */

	bool		replicate_valid;
	PublicationActions pubactions;

	/*
	 * Row-filter related members:
	 * The flag 'rowfilter_valid' indicates if the exprstate has been assigned
	 * yet or not. We cannot just use the exprstate value for this purpose
	 * because there might be no filter at all for the current relid (e.g.
	 * every exprstate is NULL).
	 * The row-filter exprstate is stored per pubaction type.
	 */
	bool		rowfilter_valid;
#define IDX_PUBACTION_INSERT 	0
#define IDX_PUBACTION_UPDATE 	1
#define IDX_PUBACTION_DELETE 	2
#define IDX_PUBACTION_TRUNCATE	3
	ExprState	   *exprstate[4];	/* ExprState for row filter(s). One per pubaction. */
	TupleTableSlot *scantuple;		/* tuple table slot for row filter */
	TupleTableSlot *new_tuple;		/* slot for storing deformed new tuple during updates */
	TupleTableSlot *old_tuple;		/* slot for storing deformed old tuple during updates */
	TupleTableSlot *tmp_new_tuple;	/* slot for temporary new tuple used for expression evaluation */

	/*
	 * OID of the relation to publish changes as.  For a partition, this may
	 * be set to one of its ancestors whose schema will be used when
	 * replicating changes, if publish_via_partition_root is set for the
	 * publication.
	 */
	Oid			publish_as_relid;

	/*
	 * Map used when replicating using an ancestor's schema to convert tuples
	 * from partition's type to the ancestor's; NULL if publish_as_relid is
	 * same as 'relid' or if unnecessary due to partition and the ancestor
	 * having identical TupleDesc.
	 */
	TupleConversionMap *map;
} RelationSyncEntry;

/* Map used to remember which relation schemas we sent. */
static HTAB *RelationSyncCache = NULL;

static void init_rel_sync_cache(MemoryContext decoding_context);
static void cleanup_rel_sync_cache(TransactionId xid, bool is_commit);
static RelationSyncEntry *get_rel_sync_entry(PGOutputData *data, Relation relation);
static void rel_sync_cache_relation_cb(Datum arg, Oid relid);
static void rel_sync_cache_publication_cb(Datum arg, int cacheid,
										  uint32 hashvalue);
static void set_schema_sent_in_streamed_txn(RelationSyncEntry *entry,
											TransactionId xid);
static bool get_schema_sent_in_streamed_txn(RelationSyncEntry *entry,
											TransactionId xid);

/* row filter routines */
static EState *create_estate_for_relation(Relation rel);
static void pgoutput_row_filter_init(PGOutputData *data, Relation relation, RelationSyncEntry *entry);
static ExprState *pgoutput_row_filter_init_expr(Node *rfnode);
static bool pgoutput_row_filter_exec_expr(ExprState *state, ExprContext *econtext);
static bool pgoutput_row_filter(int idx_pubaction, Relation relation, HeapTuple oldtuple,
								HeapTuple newtuple, TupleTableSlot *slot,
								RelationSyncEntry *entry);
static bool pgoutput_row_filter_update_check(int idx_pubaction, Relation relation, HeapTuple oldtuple,
									   HeapTuple newtuple, RelationSyncEntry *entry,
									   ReorderBufferChangeType *action);

/*
 * Specify output plugin callbacks
 */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = pgoutput_startup;
	cb->begin_cb = pgoutput_begin_txn;
	cb->change_cb = pgoutput_change;
	cb->truncate_cb = pgoutput_truncate;
	cb->message_cb = pgoutput_message;
	cb->commit_cb = pgoutput_commit_txn;

	cb->begin_prepare_cb = pgoutput_begin_prepare_txn;
	cb->prepare_cb = pgoutput_prepare_txn;
	cb->commit_prepared_cb = pgoutput_commit_prepared_txn;
	cb->rollback_prepared_cb = pgoutput_rollback_prepared_txn;
	cb->filter_by_origin_cb = pgoutput_origin_filter;
	cb->shutdown_cb = pgoutput_shutdown;

	/* transaction streaming */
	cb->stream_start_cb = pgoutput_stream_start;
	cb->stream_stop_cb = pgoutput_stream_stop;
	cb->stream_abort_cb = pgoutput_stream_abort;
	cb->stream_commit_cb = pgoutput_stream_commit;
	cb->stream_change_cb = pgoutput_change;
	cb->stream_message_cb = pgoutput_message;
	cb->stream_truncate_cb = pgoutput_truncate;
	/* transaction streaming - two-phase commit */
	cb->stream_prepare_cb = pgoutput_stream_prepare_txn;
}

static void
parse_output_parameters(List *options, PGOutputData *data)
{
	ListCell   *lc;
	bool		protocol_version_given = false;
	bool		publication_names_given = false;
	bool		binary_option_given = false;
	bool		messages_option_given = false;
	bool		streaming_given = false;
	bool		two_phase_option_given = false;

	data->binary = false;
	data->streaming = false;
	data->messages = false;
	data->two_phase = false;

	foreach(lc, options)
	{
		DefElem    *defel = (DefElem *) lfirst(lc);

		Assert(defel->arg == NULL || IsA(defel->arg, String));

		/* Check each param, whether or not we recognize it */
		if (strcmp(defel->defname, "proto_version") == 0)
		{
			int64		parsed;

			if (protocol_version_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			protocol_version_given = true;

			if (!scanint8(strVal(defel->arg), true, &parsed))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid proto_version")));

			if (parsed > PG_UINT32_MAX || parsed < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("proto_version \"%s\" out of range",
								strVal(defel->arg))));

			data->protocol_version = (uint32) parsed;
		}
		else if (strcmp(defel->defname, "publication_names") == 0)
		{
			if (publication_names_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			publication_names_given = true;

			if (!SplitIdentifierString(strVal(defel->arg), ',',
									   &data->publication_names))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_NAME),
						 errmsg("invalid publication_names syntax")));
		}
		else if (strcmp(defel->defname, "binary") == 0)
		{
			if (binary_option_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			binary_option_given = true;

			data->binary = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "messages") == 0)
		{
			if (messages_option_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			messages_option_given = true;

			data->messages = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "streaming") == 0)
		{
			if (streaming_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			streaming_given = true;

			data->streaming = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "two_phase") == 0)
		{
			if (two_phase_option_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			two_phase_option_given = true;

			data->two_phase = defGetBoolean(defel);
		}
		else
			elog(ERROR, "unrecognized pgoutput option: %s", defel->defname);
	}
}

/*
 * Initialize this plugin
 */
static void
pgoutput_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
				 bool is_init)
{
	PGOutputData *data = palloc0(sizeof(PGOutputData));

	/* Create our memory context for private allocations. */
	data->context = AllocSetContextCreate(ctx->context,
										  "logical replication output context",
										  ALLOCSET_DEFAULT_SIZES);

	ctx->output_plugin_private = data;

	/* This plugin uses binary protocol. */
	opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

	/*
	 * This is replication start and not slot initialization.
	 *
	 * Parse and validate options passed by the client.
	 */
	if (!is_init)
	{
		/* Parse the params and ERROR if we see any we don't recognize */
		parse_output_parameters(ctx->output_plugin_options, data);

		/* Check if we support requested protocol */
		if (data->protocol_version > LOGICALREP_PROTO_MAX_VERSION_NUM)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("client sent proto_version=%d but we only support protocol %d or lower",
							data->protocol_version, LOGICALREP_PROTO_MAX_VERSION_NUM)));

		if (data->protocol_version < LOGICALREP_PROTO_MIN_VERSION_NUM)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("client sent proto_version=%d but we only support protocol %d or higher",
							data->protocol_version, LOGICALREP_PROTO_MIN_VERSION_NUM)));

		if (list_length(data->publication_names) < 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("publication_names parameter missing")));

		/*
		 * Decide whether to enable streaming. It is disabled by default, in
		 * which case we just update the flag in decoding context. Otherwise
		 * we only allow it with sufficient version of the protocol, and when
		 * the output plugin supports it.
		 */
		if (!data->streaming)
			ctx->streaming = false;
		else if (data->protocol_version < LOGICALREP_PROTO_STREAM_VERSION_NUM)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("requested proto_version=%d does not support streaming, need %d or higher",
							data->protocol_version, LOGICALREP_PROTO_STREAM_VERSION_NUM)));
		else if (!ctx->streaming)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("streaming requested, but not supported by output plugin")));

		/* Also remember we're currently not streaming any transaction. */
		in_streaming = false;

		/*
		 * Here, we just check whether the two-phase option is passed by
		 * plugin and decide whether to enable it at later point of time. It
		 * remains enabled if the previous start-up has done so. But we only
		 * allow the option to be passed in with sufficient version of the
		 * protocol, and when the output plugin supports it.
		 */
		if (!data->two_phase)
			ctx->twophase_opt_given = false;
		else if (data->protocol_version < LOGICALREP_PROTO_TWOPHASE_VERSION_NUM)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("requested proto_version=%d does not support two-phase commit, need %d or higher",
							data->protocol_version, LOGICALREP_PROTO_TWOPHASE_VERSION_NUM)));
		else if (!ctx->twophase)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("two-phase commit requested, but not supported by output plugin")));
		else
			ctx->twophase_opt_given = true;

		/* Init publication state. */
		data->publications = NIL;
		publications_valid = false;
		CacheRegisterSyscacheCallback(PUBLICATIONOID,
									  publication_invalidation_cb,
									  (Datum) 0);

		/* Initialize relation schema cache. */
		init_rel_sync_cache(CacheMemoryContext);
	}
	else
	{
		/*
		 * Disable the streaming and prepared transactions during the slot
		 * initialization mode.
		 */
		ctx->streaming = false;
		ctx->twophase = false;
	}
}

/*
 * BEGIN callback
 */
static void
pgoutput_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	bool		send_replication_origin = txn->origin_id != InvalidRepOriginId;

	OutputPluginPrepareWrite(ctx, !send_replication_origin);
	logicalrep_write_begin(ctx->out, txn);

	send_repl_origin(ctx, txn->origin_id, txn->origin_lsn,
					 send_replication_origin);

	OutputPluginWrite(ctx, true);
}

/*
 * COMMIT callback
 */
static void
pgoutput_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					XLogRecPtr commit_lsn)
{
	OutputPluginUpdateProgress(ctx);

	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_commit(ctx->out, txn, commit_lsn);
	OutputPluginWrite(ctx, true);
}

/*
 * BEGIN PREPARE callback
 */
static void
pgoutput_begin_prepare_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	bool		send_replication_origin = txn->origin_id != InvalidRepOriginId;

	OutputPluginPrepareWrite(ctx, !send_replication_origin);
	logicalrep_write_begin_prepare(ctx->out, txn);

	send_repl_origin(ctx, txn->origin_id, txn->origin_lsn,
					 send_replication_origin);

	OutputPluginWrite(ctx, true);
}

/*
 * PREPARE callback
 */
static void
pgoutput_prepare_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					 XLogRecPtr prepare_lsn)
{
	OutputPluginUpdateProgress(ctx);

	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_prepare(ctx->out, txn, prepare_lsn);
	OutputPluginWrite(ctx, true);
}

/*
 * COMMIT PREPARED callback
 */
static void
pgoutput_commit_prepared_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
							 XLogRecPtr commit_lsn)
{
	OutputPluginUpdateProgress(ctx);

	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_commit_prepared(ctx->out, txn, commit_lsn);
	OutputPluginWrite(ctx, true);
}

/*
 * ROLLBACK PREPARED callback
 */
static void
pgoutput_rollback_prepared_txn(LogicalDecodingContext *ctx,
							   ReorderBufferTXN *txn,
							   XLogRecPtr prepare_end_lsn,
							   TimestampTz prepare_time)
{
	OutputPluginUpdateProgress(ctx);

	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_rollback_prepared(ctx->out, txn, prepare_end_lsn,
									   prepare_time);
	OutputPluginWrite(ctx, true);
}

/*
 * Write the current schema of the relation and its ancestor (if any) if not
 * done yet.
 */
static void
maybe_send_schema(LogicalDecodingContext *ctx,
				  ReorderBufferChange *change,
				  Relation relation, RelationSyncEntry *relentry)
{
	bool		schema_sent;
	TransactionId xid = InvalidTransactionId;
	TransactionId topxid = InvalidTransactionId;

	/*
	 * Remember XID of the (sub)transaction for the change. We don't care if
	 * it's top-level transaction or not (we have already sent that XID in
	 * start of the current streaming block).
	 *
	 * If we're not in a streaming block, just use InvalidTransactionId and
	 * the write methods will not include it.
	 */
	if (in_streaming)
		xid = change->txn->xid;

	if (change->txn->toptxn)
		topxid = change->txn->toptxn->xid;
	else
		topxid = xid;

	/*
	 * Do we need to send the schema? We do track streamed transactions
	 * separately, because those may be applied later (and the regular
	 * transactions won't see their effects until then) and in an order that
	 * we don't know at this point.
	 *
	 * XXX There is a scope of optimization here. Currently, we always send
	 * the schema first time in a streaming transaction but we can probably
	 * avoid that by checking 'relentry->schema_sent' flag. However, before
	 * doing that we need to study its impact on the case where we have a mix
	 * of streaming and non-streaming transactions.
	 */
	if (in_streaming)
		schema_sent = get_schema_sent_in_streamed_txn(relentry, topxid);
	else
		schema_sent = relentry->schema_sent;

	/* Nothing to do if we already sent the schema. */
	if (schema_sent)
		return;

	/*
	 * Nope, so send the schema.  If the changes will be published using an
	 * ancestor's schema, not the relation's own, send that ancestor's schema
	 * before sending relation's own (XXX - maybe sending only the former
	 * suffices?).  This is also a good place to set the map that will be used
	 * to convert the relation's tuples into the ancestor's format, if needed.
	 */
	if (relentry->publish_as_relid != RelationGetRelid(relation))
	{
		Relation	ancestor = RelationIdGetRelation(relentry->publish_as_relid);
		TupleDesc	indesc = RelationGetDescr(relation);
		TupleDesc	outdesc = RelationGetDescr(ancestor);
		MemoryContext oldctx;

		/* Map must live as long as the session does. */
		oldctx = MemoryContextSwitchTo(CacheMemoryContext);

		/*
		 * Make copies of the TupleDescs that will live as long as the map
		 * does before putting into the map.
		 */
		indesc = CreateTupleDescCopy(indesc);
		outdesc = CreateTupleDescCopy(outdesc);
		relentry->map = convert_tuples_by_name(indesc, outdesc);
		if (relentry->map == NULL)
		{
			/* Map not necessary, so free the TupleDescs too. */
			FreeTupleDesc(indesc);
			FreeTupleDesc(outdesc);
		}

		MemoryContextSwitchTo(oldctx);
		send_relation_and_attrs(ancestor, xid, ctx);
		RelationClose(ancestor);
	}

	send_relation_and_attrs(relation, xid, ctx);

	if (in_streaming)
		set_schema_sent_in_streamed_txn(relentry, topxid);
	else
		relentry->schema_sent = true;
}

/*
 * Sends a relation
 */
static void
send_relation_and_attrs(Relation relation, TransactionId xid,
						LogicalDecodingContext *ctx)
{
	TupleDesc	desc = RelationGetDescr(relation);
	int			i;

	/*
	 * Write out type info if needed.  We do that only for user-created types.
	 * We use FirstGenbkiObjectId as the cutoff, so that we only consider
	 * objects with hand-assigned OIDs to be "built in", not for instance any
	 * function or type defined in the information_schema. This is important
	 * because only hand-assigned OIDs can be expected to remain stable across
	 * major versions.
	 */
	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);

		if (att->attisdropped || att->attgenerated)
			continue;

		if (att->atttypid < FirstGenbkiObjectId)
			continue;

		OutputPluginPrepareWrite(ctx, false);
		logicalrep_write_typ(ctx->out, xid, att->atttypid);
		OutputPluginWrite(ctx, false);
	}

	OutputPluginPrepareWrite(ctx, false);
	logicalrep_write_rel(ctx->out, xid, relation);
	OutputPluginWrite(ctx, false);
}

/*
 * Executor state preparation for evaluation of row filter expressions for the
 * specified relation.
 */
static EState *
create_estate_for_relation(Relation rel)
{
	EState			*estate;
	RangeTblEntry	*rte;

	estate = CreateExecutorState();

	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;
	rte->rellockmode = AccessShareLock;
	ExecInitRangeTable(estate, list_make1(rte));

	estate->es_output_cid = GetCurrentCommandId(false);

	return estate;
}

/*
 * Initialize for row filter expression execution.
 */
static ExprState *
pgoutput_row_filter_init_expr(Node *rfnode)
{
	ExprState	   *exprstate;
	Oid				exprtype;
	Expr		   *expr;

	/* Cache ExprState using CacheMemoryContext. */
	Assert(CurrentMemoryContext = CacheMemoryContext);

	/* Prepare expression for execution */
	exprtype = exprType(rfnode);
	expr = (Expr *) coerce_to_target_type(NULL, rfnode, exprtype, BOOLOID, -1, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);

	if (expr == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_CANNOT_COERCE),
				 errmsg("row filter returns type %s that cannot be cast to the expected type %s",
						format_type_be(exprtype),
						format_type_be(BOOLOID)),
				 errhint("You will need to rewrite the row filter.")));

	/*
	 * This is the same code as ExecPrepareExpr() but that is not used because
	 * we have no EState to pass it. There should probably be another function
	 * in the executor to handle the execution outside a normal Plan tree
	 * context.
	 */
	expr = expression_planner(expr);
	exprstate = ExecInitExpr(expr, NULL);

	return exprstate;
}

/*
 * Evaluates row filter.
 *
 * If the row filter evaluates to NULL, it is taken as false i.e. the change
 * isn't replicated.
 */
static bool
pgoutput_row_filter_exec_expr(ExprState *state, ExprContext *econtext)
{
	Datum		ret;
	bool		isnull;

	Assert(state != NULL);

	ret = ExecEvalExprSwitchContext(state, econtext, &isnull);

	elog(DEBUG3, "row filter evaluates to %s (isnull: %s)",
		 DatumGetBool(ret) ? "true" : "false",
		 isnull ? "true" : "false");

	if (isnull)
		return false;

	return DatumGetBool(ret);
}

/*
 * Update is checked against the row filter, if any.
 * Updates are transformed to inserts and deletes based on the
 * old_tuple and new_tuple. The new action is updated in the
 * action parameter. If not updated, action remains as update.
 * old-row (no match)    new-row (no match)  -> (drop change)
 * old-row (no match)    new row (match)     -> INSERT
 * old-row (match)       new-row (no match)  -> DELETE
 * old-row (match)       new row (match)     -> UPDATE
 * If the change is to be replicated returns true, else false.
 */
static bool
pgoutput_row_filter_update_check(int idx_pubaction, Relation relation, HeapTuple oldtuple, HeapTuple newtuple, RelationSyncEntry *entry, ReorderBufferChangeType *action)
{
	TupleDesc	desc = RelationGetDescr(relation);
	int		i;
	bool	old_matched, new_matched;
	TupleTableSlot	*tmp_new_slot, *old_slot, *new_slot;

	/* Bail out if there is no row filter */
	if (!entry->exprstate[idx_pubaction])
		return true;

	/* update requires a new tuple */
	Assert(newtuple);

	elog(DEBUG3, "table \"%s.%s\" has row filter",
			get_namespace_name(get_rel_namespace(RelationGetRelid(relation))),
			get_rel_name(relation->rd_id));

	/*
	 * If no old_tuple, then none of the replica identity columns changed
	 * and this would reduce to a simple update.
	 */
	if (!oldtuple)
	{
		*action = REORDER_BUFFER_CHANGE_UPDATE;
		return pgoutput_row_filter(idx_pubaction, relation, NULL, newtuple, NULL, entry);
	}

	old_slot = entry->old_tuple;
	new_slot = entry->new_tuple;
	tmp_new_slot = entry->tmp_new_tuple;
	ExecClearTuple(old_slot);
	ExecClearTuple(new_slot);
	ExecClearTuple(tmp_new_slot);

	heap_deform_tuple(newtuple, desc, new_slot->tts_values, new_slot->tts_isnull);
	heap_deform_tuple(oldtuple, desc, old_slot->tts_values, old_slot->tts_isnull);

	ExecStoreVirtualTuple(old_slot);
	ExecStoreVirtualTuple(new_slot);

	tmp_new_slot = ExecCopySlot(tmp_new_slot, new_slot);

	/*
	 * For updates, both the newtuple and oldtuple needs to be checked
	 * against the row-filter. The newtuple might not have all the
	 * replica identity columns, in which case it needs to be
	 * copied over from the oldtuple.
	 */
	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);

		/* if the column in the new_tuple is null, nothing to do */
		if (tmp_new_slot->tts_isnull[i])
			continue;

		/*
		 * Unchanged toasted replica identity columns are
		 * only detoasted in the old tuple, copy this over to the newtuple.
		 */
		if ((att->attlen == -1 && VARATT_IS_EXTERNAL_ONDISK(tmp_new_slot->tts_values[i])) &&
				(!old_slot->tts_isnull[i] &&
					!(VARATT_IS_EXTERNAL_ONDISK(old_slot->tts_values[i]))))
		{
			Assert(!old_slot->tts_isnull[i] &&
				   !VARATT_IS_EXTERNAL_ONDISK(old_slot->tts_values[i]));
			tmp_new_slot->tts_values[i] = old_slot->tts_values[i];
		}

	}

	old_matched = pgoutput_row_filter(idx_pubaction, relation, NULL, NULL, old_slot, entry);
	new_matched = pgoutput_row_filter(idx_pubaction, relation, NULL, NULL, tmp_new_slot, entry);

	if (!old_matched && !new_matched)
		return false;

	if (!old_matched && new_matched)
		*action = REORDER_BUFFER_CHANGE_INSERT;
	else if (old_matched && !new_matched)
		*action = REORDER_BUFFER_CHANGE_DELETE;
	else if (new_matched && old_matched)
		*action = REORDER_BUFFER_CHANGE_UPDATE;

	return true;
}

/*
 * Initialize the row filter, the first time.
 */
static void
pgoutput_row_filter_init(PGOutputData *data, Relation relation, RelationSyncEntry *entry)
{
	ListCell   *lc;
	List	   *rfnodes[4] = {NIL, NIL, NIL, NIL}; /* One per pubaction */
	bool		no_filter[4] = {false, false, false, false}; /* One per pubaction */

	/*
	 * If the row filter caching is currently flagged "invalid" then it means we
	 * don't know yet if there is/isn't any row filters for this relation.
	 *
	 * This code is usually one-time execution.
	 *
	 * NOTE: The ExprState cache could have been created up-front in the
	 * function get_rel_sync_entry() instead of the deferred on-the-fly
	 * assignment below. The reason for choosing to do it here is because there
	 * are some scenarios where the get_rel_sync_entry() is called but where a
	 * row will not be published. For example, for truncate, we may not need
	 * any row evaluation, so there is no need to compute it. It would also be
	 * a waste if any error happens before actually evaluating the filter. And
	 * tomorrow there could be other operations (which use get_rel_sync_entry)
	 * but which don't need to build ExprState. Furthermore, because the
	 * decision to publish or not is made AFTER the call to get_rel_sync_entry
	 * it may be that the filter evaluation is not necessary at all. So the
	 * decision was to defer this logic to last moment when we know it will be
	 * needed.
	 */
	if (!entry->rowfilter_valid)
	{
		MemoryContext	oldctx;

		/* Release the tuple table slot if it already exists. */
		if (entry->scantuple != NULL)
		{
			ExecDropSingleTupleTableSlot(entry->scantuple);
			entry->scantuple = NULL;
		}

		/*
		 * Find if there are any row filters for this relation. If there are,
		 * then prepare the necessary ExprState and cache it in entry->exprstate.
		 *
		 * NOTE: All publication-table mappings must be checked.
		 *
		 * NOTE: If the relation is a partition and pubviaroot is true, use
		 * the row filter of the topmost partitioned table instead of the row
		 * filter of its own partition.
		 *
		 * NOTE: Multiple row-filters for the same table are combined by OR-ing
		 * them together, but this means that if (in any of the publications)
		 * there is *no* filter then effectively none of the other filters have
		 * any meaning either.
		 */
		foreach(lc, data->publications)
		{
			Publication *pub = lfirst(lc);
			HeapTuple	rftuple;
			Datum		rfdatum;
			bool		rfisnull;

			/*
			 * Lookup if there is a row-filter, and if yes remember it in a list (per pubaction).
			 * In code following this 'publications' loop we will combine all filters.
			 */
			rftuple = SearchSysCache2(PUBLICATIONRELMAP, ObjectIdGetDatum(entry->publish_as_relid), ObjectIdGetDatum(pub->oid));
			if (HeapTupleIsValid(rftuple))
			{
				rfdatum = SysCacheGetAttr(PUBLICATIONRELMAP, rftuple, Anum_pg_publication_rel_prqual, &rfisnull);

				if (!rfisnull)
				{
					Node	   *rfnode;

					oldctx = MemoryContextSwitchTo(CacheMemoryContext);
					/* Gather the rfnodes per pubaction of this publiaction. */
					if (pub->pubactions.pubinsert)
					{
						rfnode = stringToNode(TextDatumGetCString(rfdatum));
						rfnodes[IDX_PUBACTION_INSERT] = lappend(rfnodes[IDX_PUBACTION_INSERT], rfnode);
					}
					if (pub->pubactions.pubupdate)
					{
						rfnode = stringToNode(TextDatumGetCString(rfdatum));
						rfnodes[IDX_PUBACTION_UPDATE] = lappend(rfnodes[IDX_PUBACTION_UPDATE], rfnode);
					}
					if (pub->pubactions.pubdelete)
					{
						rfnode = stringToNode(TextDatumGetCString(rfdatum));
						rfnodes[IDX_PUBACTION_DELETE] = lappend(rfnodes[IDX_PUBACTION_DELETE], rfnode);
					}
					if (pub->pubactions.pubtruncate)
					{
						rfnode = stringToNode(TextDatumGetCString(rfdatum));
						rfnodes[IDX_PUBACTION_TRUNCATE] = lappend(rfnodes[IDX_PUBACTION_TRUNCATE], rfnode);
					}
					MemoryContextSwitchTo(oldctx);
				}
				else
				{
					/* Remember which pubactions have no row-filter. */
					if (pub->pubactions.pubinsert)
						no_filter[IDX_PUBACTION_INSERT] = true;
					if (pub->pubactions.pubupdate)
						no_filter[IDX_PUBACTION_UPDATE] = true;
					if (pub->pubactions.pubdelete)
						no_filter[IDX_PUBACTION_DELETE] = true;
					if (pub->pubactions.pubinsert)
						no_filter[IDX_PUBACTION_TRUNCATE] = true;
				}

				ReleaseSysCache(rftuple);
			}

		} /* loop all subscribed publications */

		/*
		 * Now all the filters for all pubactions are known, let's try to combine them
		 * when their pubactions are same.
		 */
		{
			int		idx;
			bool	found_filters = false;

			/* For each pubaction... */
			for (idx = 0; idx < 4; idx++)
			{
				int n_filters;

				/*
				 * If one or more publications with this pubaction had no filter at all,
				 * then that nullifies the effect of all other filters for the same
				 * pubaction (because filters get OR'ed together).
				 */
				if (no_filter[idx])
				{
					if (rfnodes[idx])
					{
						list_free_deep(rfnodes[idx]);
						rfnodes[idx] = NIL;
					}
				}

				/*
				 * If there was one or more filter for this pubaction then combine them
				 * (if necessary) and cache the ExprState.
				 */
				n_filters = list_length(rfnodes[idx]);
				if (n_filters > 0)
				{
					Node	   *rfnode;

					oldctx = MemoryContextSwitchTo(CacheMemoryContext);
					rfnode = n_filters > 1 ? makeBoolExpr(OR_EXPR, rfnodes[idx], -1) : linitial(rfnodes[idx]);
					entry->exprstate[idx] = pgoutput_row_filter_init_expr(rfnode);
					MemoryContextSwitchTo(oldctx);

					found_filters = true; /* flag that we will need slots made */
				}
			} /* for each pubaction */

			if (found_filters)
			{
				TupleDesc	tupdesc = RelationGetDescr(relation);

				/*
				 * Create tuple table slots for row filter. Create a copy of the
				 * TupleDesc as it needs to live as long as the cache remains.
				 */
				oldctx = MemoryContextSwitchTo(CacheMemoryContext);
				tupdesc = CreateTupleDescCopy(tupdesc);
				entry->scantuple = MakeSingleTupleTableSlot(tupdesc, &TTSOpsHeapTuple);
				entry->old_tuple = MakeSingleTupleTableSlot(tupdesc, &TTSOpsVirtual);
				entry->new_tuple = MakeSingleTupleTableSlot(tupdesc, &TTSOpsVirtual);
				entry->tmp_new_tuple = MakeSingleTupleTableSlot(tupdesc, &TTSOpsVirtual);
				MemoryContextSwitchTo(oldctx);
			}
		}

		entry->rowfilter_valid = true;
	}
}

/*
 * Change is checked against the row filter, if any.
 *
 * If it returns true, the change is replicated, otherwise, it is not.
 */
static bool
pgoutput_row_filter(int idx_pubaction, Relation relation, HeapTuple oldtuple, HeapTuple newtuple, TupleTableSlot *slot,
RelationSyncEntry *entry)
{
	EState	   *estate;
	ExprContext *ecxt;
	bool		result = true;
	Oid         relid = RelationGetRelid(relation);

	/* Bail out if there is no row filter */
	if (!entry->exprstate[idx_pubaction])
		return true;

	if (message_level_is_interesting(DEBUG3))
		elog(DEBUG3, "table \"%s.%s\" has row filter",
			 get_namespace_name(get_rel_namespace(relid)),
			 get_rel_name(relid));

	PushActiveSnapshot(GetTransactionSnapshot());

	estate = create_estate_for_relation(relation);

	/* Prepare context per tuple */
	ecxt = GetPerTupleExprContext(estate);
	ecxt->ecxt_scantuple = entry->scantuple;

	if (newtuple || oldtuple)
		ExecStoreHeapTuple(newtuple ? newtuple : oldtuple, ecxt->ecxt_scantuple, false);
	else
	{
		ecxt->ecxt_scantuple = slot;
	}

	/*
	 * NOTE: Multiple publication row-filters have already been combined to a
	 * single exprstate (for this pubaction).
	 */
	if (entry->exprstate[idx_pubaction])
	{
		/* Evaluates row filter */
		result = pgoutput_row_filter_exec_expr(entry->exprstate[idx_pubaction], ecxt);
	}

	/* Cleanup allocated resources */
	FreeExecutorState(estate);
	PopActiveSnapshot();

	return result;
}

/*
 * Sends the decoded DML over wire.
 *
 * This is called both in streaming and non-streaming modes.
 */
static void
pgoutput_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				Relation relation, ReorderBufferChange *change)
{
	PGOutputData *data = (PGOutputData *) ctx->output_plugin_private;
	MemoryContext old;
	RelationSyncEntry *relentry;
	TransactionId xid = InvalidTransactionId;
	Relation	ancestor = NULL;

	if (!is_publishable_relation(relation))
		return;

	/*
	 * Remember the xid for the change in streaming mode. We need to send xid
	 * with each change in the streaming mode so that subscriber can make
	 * their association and on aborts, it can discard the corresponding
	 * changes.
	 */
	if (in_streaming)
		xid = change->txn->xid;

	relentry = get_rel_sync_entry(data, relation);

	/* First check the table filter */
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			if (!relentry->pubactions.pubinsert)
				return;
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			if (!relentry->pubactions.pubupdate)
				return;
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			if (!relentry->pubactions.pubdelete)
				return;
			break;
		default:
			Assert(false);
	}

	/* Avoid leaking memory by using and resetting our own context */
	old = MemoryContextSwitchTo(data->context);

	/* Initialize the row_filter */
	pgoutput_row_filter_init(data, relation, relentry);

	/* Send the data */
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			{
				HeapTuple	tuple = &change->data.tp.newtuple->tuple;

				/* Check row filter. */
				if (!pgoutput_row_filter(IDX_PUBACTION_INSERT, relation, NULL, tuple, NULL, relentry))
					break;

				/*
				 * Schema should be sent before the logic that replaces the
				 * relation because it also sends the ancestor's relation.
				 */
				maybe_send_schema(ctx, change, relation, relentry);

				/* Switch relation if publishing via root. */
				if (relentry->publish_as_relid != RelationGetRelid(relation))
				{
					Assert(relation->rd_rel->relispartition);
					ancestor = RelationIdGetRelation(relentry->publish_as_relid);
					relation = ancestor;
					/* Convert tuple if needed. */
					if (relentry->map)
						tuple = execute_attr_map_tuple(tuple, relentry->map);
				}

				OutputPluginPrepareWrite(ctx, true);
				logicalrep_write_insert(ctx->out, xid, relation, tuple,
										data->binary);
				OutputPluginWrite(ctx, true);
				break;
			}
		case REORDER_BUFFER_CHANGE_UPDATE:
			{
				HeapTuple	oldtuple = change->data.tp.oldtuple ?
				&change->data.tp.oldtuple->tuple : NULL;
				HeapTuple	newtuple = &change->data.tp.newtuple->tuple;
				ReorderBufferChangeType modified_action = REORDER_BUFFER_CHANGE_UPDATE;

				if (!pgoutput_row_filter_update_check(IDX_PUBACTION_UPDATE, relation, oldtuple, newtuple, relentry,
												&modified_action))
					break;

				maybe_send_schema(ctx, change, relation, relentry);

				/* Switch relation if publishing via root. */
				if (relentry->publish_as_relid != RelationGetRelid(relation))
				{
					Assert(relation->rd_rel->relispartition);
					ancestor = RelationIdGetRelation(relentry->publish_as_relid);
					relation = ancestor;
					/* Convert tuples if needed. */
					if (relentry->map)
					{
						if (oldtuple)
							oldtuple = execute_attr_map_tuple(oldtuple,
															  relentry->map);
						newtuple = execute_attr_map_tuple(newtuple,
														  relentry->map);
					}
				}

				OutputPluginPrepareWrite(ctx, true);

				switch (modified_action)
				{
					case REORDER_BUFFER_CHANGE_INSERT:
						logicalrep_write_insert(ctx->out, xid, relation, newtuple,
												data->binary);
						break;
					case REORDER_BUFFER_CHANGE_UPDATE:
						logicalrep_write_update(ctx->out, xid, relation,
												oldtuple, newtuple, relentry->old_tuple,
												relentry->new_tuple,
												data->binary);
						break;
					case REORDER_BUFFER_CHANGE_DELETE:
						logicalrep_write_delete(ctx->out, xid, relation, oldtuple,
												data->binary);
						break;
					default:
						Assert(false);
				}

				OutputPluginWrite(ctx, true);
				break;
			}
		case REORDER_BUFFER_CHANGE_DELETE:
			if (change->data.tp.oldtuple)
			{
				HeapTuple	oldtuple = &change->data.tp.oldtuple->tuple;

				/* Check row filter. */
				if (!pgoutput_row_filter(IDX_PUBACTION_DELETE, relation, oldtuple, NULL, NULL, relentry))
					break;

				maybe_send_schema(ctx, change, relation, relentry);

				/* Switch relation if publishing via root. */
				if (relentry->publish_as_relid != RelationGetRelid(relation))
				{
					Assert(relation->rd_rel->relispartition);
					ancestor = RelationIdGetRelation(relentry->publish_as_relid);
					relation = ancestor;
					/* Convert tuple if needed. */
					if (relentry->map)
						oldtuple = execute_attr_map_tuple(oldtuple, relentry->map);
				}

				OutputPluginPrepareWrite(ctx, true);
				logicalrep_write_delete(ctx->out, xid, relation, oldtuple,
										data->binary);
				OutputPluginWrite(ctx, true);
			}
			else
				elog(DEBUG1, "didn't send DELETE change because of missing oldtuple");
			break;
		default:
			Assert(false);
	}

	if (RelationIsValid(ancestor))
	{
		RelationClose(ancestor);
		ancestor = NULL;
	}

	/* Cleanup */
	MemoryContextSwitchTo(old);
	MemoryContextReset(data->context);
}

static void
pgoutput_truncate(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				  int nrelations, Relation relations[], ReorderBufferChange *change)
{
	PGOutputData *data = (PGOutputData *) ctx->output_plugin_private;
	MemoryContext old;
	RelationSyncEntry *relentry;
	int			i;
	int			nrelids;
	Oid		   *relids;
	TransactionId xid = InvalidTransactionId;

	/* Remember the xid for the change in streaming mode. See pgoutput_change. */
	if (in_streaming)
		xid = change->txn->xid;

	old = MemoryContextSwitchTo(data->context);

	relids = palloc0(nrelations * sizeof(Oid));
	nrelids = 0;

	for (i = 0; i < nrelations; i++)
	{
		Relation	relation = relations[i];
		Oid			relid = RelationGetRelid(relation);

		if (!is_publishable_relation(relation))
			continue;

		relentry = get_rel_sync_entry(data, relation);

		if (!relentry->pubactions.pubtruncate)
			continue;

		/*
		 * Don't send partitions if the publication wants to send only the
		 * root tables through it.
		 */
		if (relation->rd_rel->relispartition &&
			relentry->publish_as_relid != relid)
			continue;

		relids[nrelids++] = relid;
		maybe_send_schema(ctx, change, relation, relentry);
	}

	if (nrelids > 0)
	{
		OutputPluginPrepareWrite(ctx, true);
		logicalrep_write_truncate(ctx->out,
								  xid,
								  nrelids,
								  relids,
								  change->data.truncate.cascade,
								  change->data.truncate.restart_seqs);
		OutputPluginWrite(ctx, true);
	}

	MemoryContextSwitchTo(old);
	MemoryContextReset(data->context);
}

static void
pgoutput_message(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				 XLogRecPtr message_lsn, bool transactional, const char *prefix, Size sz,
				 const char *message)
{
	PGOutputData *data = (PGOutputData *) ctx->output_plugin_private;
	TransactionId xid = InvalidTransactionId;

	if (!data->messages)
		return;

	/*
	 * Remember the xid for the message in streaming mode. See
	 * pgoutput_change.
	 */
	if (in_streaming)
		xid = txn->xid;

	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_message(ctx->out,
							 xid,
							 message_lsn,
							 transactional,
							 prefix,
							 sz,
							 message);
	OutputPluginWrite(ctx, true);
}

/*
 * Currently we always forward.
 */
static bool
pgoutput_origin_filter(LogicalDecodingContext *ctx,
					   RepOriginId origin_id)
{
	return false;
}

/*
 * Shutdown the output plugin.
 *
 * Note, we don't need to clean the data->context as it's child context
 * of the ctx->context so it will be cleaned up by logical decoding machinery.
 */
static void
pgoutput_shutdown(LogicalDecodingContext *ctx)
{
	if (RelationSyncCache)
	{
		hash_destroy(RelationSyncCache);
		RelationSyncCache = NULL;
	}
}

/*
 * Load publications from the list of publication names.
 */
static List *
LoadPublications(List *pubnames)
{
	List	   *result = NIL;
	ListCell   *lc;

	foreach(lc, pubnames)
	{
		char	   *pubname = (char *) lfirst(lc);
		Publication *pub = GetPublicationByName(pubname, false);

		result = lappend(result, pub);
	}

	return result;
}

/*
 * Publication cache invalidation callback.
 */
static void
publication_invalidation_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	publications_valid = false;

	/*
	 * Also invalidate per-relation cache so that next time the filtering info
	 * is checked it will be updated with the new publication settings.
	 */
	rel_sync_cache_publication_cb(arg, cacheid, hashvalue);
}

/*
 * START STREAM callback
 */
static void
pgoutput_stream_start(struct LogicalDecodingContext *ctx,
					  ReorderBufferTXN *txn)
{
	bool		send_replication_origin = txn->origin_id != InvalidRepOriginId;

	/* we can't nest streaming of transactions */
	Assert(!in_streaming);

	/*
	 * If we already sent the first stream for this transaction then don't
	 * send the origin id in the subsequent streams.
	 */
	if (rbtxn_is_streamed(txn))
		send_replication_origin = false;

	OutputPluginPrepareWrite(ctx, !send_replication_origin);
	logicalrep_write_stream_start(ctx->out, txn->xid, !rbtxn_is_streamed(txn));

	send_repl_origin(ctx, txn->origin_id, InvalidXLogRecPtr,
					 send_replication_origin);

	OutputPluginWrite(ctx, true);

	/* we're streaming a chunk of transaction now */
	in_streaming = true;
}

/*
 * STOP STREAM callback
 */
static void
pgoutput_stream_stop(struct LogicalDecodingContext *ctx,
					 ReorderBufferTXN *txn)
{
	/* we should be streaming a trasanction */
	Assert(in_streaming);

	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_stream_stop(ctx->out);
	OutputPluginWrite(ctx, true);

	/* we've stopped streaming a transaction */
	in_streaming = false;
}

/*
 * Notify downstream to discard the streamed transaction (along with all
 * it's subtransactions, if it's a toplevel transaction).
 */
static void
pgoutput_stream_abort(struct LogicalDecodingContext *ctx,
					  ReorderBufferTXN *txn,
					  XLogRecPtr abort_lsn)
{
	ReorderBufferTXN *toptxn;

	/*
	 * The abort should happen outside streaming block, even for streamed
	 * transactions. The transaction has to be marked as streamed, though.
	 */
	Assert(!in_streaming);

	/* determine the toplevel transaction */
	toptxn = (txn->toptxn) ? txn->toptxn : txn;

	Assert(rbtxn_is_streamed(toptxn));

	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_stream_abort(ctx->out, toptxn->xid, txn->xid);
	OutputPluginWrite(ctx, true);

	cleanup_rel_sync_cache(toptxn->xid, false);
}

/*
 * Notify downstream to apply the streamed transaction (along with all
 * it's subtransactions).
 */
static void
pgoutput_stream_commit(struct LogicalDecodingContext *ctx,
					   ReorderBufferTXN *txn,
					   XLogRecPtr commit_lsn)
{
	/*
	 * The commit should happen outside streaming block, even for streamed
	 * transactions. The transaction has to be marked as streamed, though.
	 */
	Assert(!in_streaming);
	Assert(rbtxn_is_streamed(txn));

	OutputPluginUpdateProgress(ctx);

	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_stream_commit(ctx->out, txn, commit_lsn);
	OutputPluginWrite(ctx, true);

	cleanup_rel_sync_cache(txn->xid, true);
}

/*
 * PREPARE callback (for streaming two-phase commit).
 *
 * Notify the downstream to prepare the transaction.
 */
static void
pgoutput_stream_prepare_txn(LogicalDecodingContext *ctx,
							ReorderBufferTXN *txn,
							XLogRecPtr prepare_lsn)
{
	Assert(rbtxn_is_streamed(txn));

	OutputPluginUpdateProgress(ctx);
	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_stream_prepare(ctx->out, txn, prepare_lsn);
	OutputPluginWrite(ctx, true);
}

/*
 * Initialize the relation schema sync cache for a decoding session.
 *
 * The hash table is destroyed at the end of a decoding session. While
 * relcache invalidations still exist and will still be invoked, they
 * will just see the null hash table global and take no action.
 */
static void
init_rel_sync_cache(MemoryContext cachectx)
{
	HASHCTL		ctl;

	if (RelationSyncCache != NULL)
		return;

	/* Make a new hash table for the cache */
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(RelationSyncEntry);
	ctl.hcxt = cachectx;

	RelationSyncCache = hash_create("logical replication output relation cache",
									128, &ctl,
									HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	Assert(RelationSyncCache != NULL);

	CacheRegisterRelcacheCallback(rel_sync_cache_relation_cb, (Datum) 0);
	CacheRegisterSyscacheCallback(PUBLICATIONRELMAP,
								  rel_sync_cache_publication_cb,
								  (Datum) 0);
	CacheRegisterSyscacheCallback(PUBLICATIONNAMESPACEMAP,
								  rel_sync_cache_publication_cb,
								  (Datum) 0);
}

/*
 * We expect relatively small number of streamed transactions.
 */
static bool
get_schema_sent_in_streamed_txn(RelationSyncEntry *entry, TransactionId xid)
{
	ListCell   *lc;

	foreach(lc, entry->streamed_txns)
	{
		if (xid == (uint32) lfirst_int(lc))
			return true;
	}

	return false;
}

/*
 * Add the xid in the rel sync entry for which we have already sent the schema
 * of the relation.
 */
static void
set_schema_sent_in_streamed_txn(RelationSyncEntry *entry, TransactionId xid)
{
	MemoryContext oldctx;

	oldctx = MemoryContextSwitchTo(CacheMemoryContext);

	entry->streamed_txns = lappend_int(entry->streamed_txns, xid);

	MemoryContextSwitchTo(oldctx);
}

/*
 * Find or create entry in the relation schema cache.
 *
 * This looks up publications that the given relation is directly or
 * indirectly part of (the latter if it's really the relation's ancestor that
 * is part of a publication) and fills up the found entry with the information
 * about which operations to publish and whether to use an ancestor's schema
 * when publishing.
 */
static RelationSyncEntry *
get_rel_sync_entry(PGOutputData *data, Relation relation)
{
	RelationSyncEntry *entry;
	Oid			relid = RelationGetRelid(relation);
	bool		am_partition = get_rel_relispartition(relid);
	char		relkind = get_rel_relkind(relid);
	bool		found;
	MemoryContext oldctx;

	Assert(RelationSyncCache != NULL);

	/* Find cached relation info, creating if not found */
	entry = (RelationSyncEntry *) hash_search(RelationSyncCache,
											  (void *) &relid,
											  HASH_ENTER, &found);
	Assert(entry != NULL);

	/* Not found means schema wasn't sent */
	if (!found)
	{
		/* immediately make a new entry valid enough to satisfy callbacks */
		entry->schema_sent = false;
		entry->streamed_txns = NIL;
		entry->replicate_valid = false;
		entry->rowfilter_valid = false;
		entry->pubactions.pubinsert = entry->pubactions.pubupdate =
			entry->pubactions.pubdelete = entry->pubactions.pubtruncate = false;
		entry->scantuple = NULL;
		entry->new_tuple = NULL;
		entry->old_tuple = NULL;
		entry->tmp_new_tuple = NULL;
		entry->exprstate[IDX_PUBACTION_INSERT] = NULL;
		entry->exprstate[IDX_PUBACTION_UPDATE] = NULL;
		entry->exprstate[IDX_PUBACTION_DELETE] = NULL;
		entry->exprstate[IDX_PUBACTION_TRUNCATE] = NULL;
		entry->publish_as_relid = InvalidOid;
		entry->map = NULL;		/* will be set by maybe_send_schema() if
								 * needed */
	}

	/* Validate the entry */
	if (!entry->replicate_valid)
	{
		Oid			schemaId = get_rel_namespace(relid);
		List	   *pubids = GetRelationPublications(relid);

		/*
		 * We don't acquire a lock on the namespace system table as we build
		 * the cache entry using a historic snapshot and all the later changes
		 * are absorbed while decoding WAL.
		 */
		List	   *schemaPubids = GetSchemaPublications(schemaId);
		ListCell   *lc;
		Oid			publish_as_relid = relid;

		/* Reload publications if needed before use. */
		if (!publications_valid)
		{
			oldctx = MemoryContextSwitchTo(CacheMemoryContext);
			if (data->publications)
				list_free_deep(data->publications);

			data->publications = LoadPublications(data->publication_names);
			MemoryContextSwitchTo(oldctx);
			publications_valid = true;
		}

		/*
		 * Build publication cache. We can't use one provided by relcache as
		 * relcache considers all publications given relation is in, but here
		 * we only need to consider ones that the subscriber requested.
		 */
		foreach(lc, data->publications)
		{
			Publication *pub = lfirst(lc);
			bool		publish = false;

			if (pub->alltables)
			{
				publish = true;
				if (pub->pubviaroot && am_partition)
					publish_as_relid = llast_oid(get_partition_ancestors(relid));
			}

			if (!publish)
			{
				bool		ancestor_published = false;

				/*
				 * For a partition, check if any of the ancestors are
				 * published.  If so, note down the topmost ancestor that is
				 * published via this publication, which will be used as the
				 * relation via which to publish the partition's changes.
				 */
				if (am_partition)
				{
					List	   *ancestors = get_partition_ancestors(relid);
					ListCell   *lc2;

					/*
					 * Find the "topmost" ancestor that is in this
					 * publication.
					 */
					foreach(lc2, ancestors)
					{
						Oid			ancestor = lfirst_oid(lc2);

						if (list_member_oid(GetRelationPublications(ancestor),
											pub->oid) ||
							list_member_oid(GetSchemaPublications(get_rel_namespace(ancestor)),
											pub->oid))
						{
							ancestor_published = true;
							if (pub->pubviaroot)
								publish_as_relid = ancestor;
						}
					}
				}

				if (list_member_oid(pubids, pub->oid) ||
					list_member_oid(schemaPubids, pub->oid) ||
					ancestor_published)
					publish = true;
			}

			/*
			 * Don't publish changes for partitioned tables, because
			 * publishing those of its partitions suffices, unless partition
			 * changes won't be published due to pubviaroot being set.
			 */
			if (publish &&
				(relkind != RELKIND_PARTITIONED_TABLE || pub->pubviaroot))
			{
				entry->pubactions.pubinsert |= pub->pubactions.pubinsert;
				entry->pubactions.pubupdate |= pub->pubactions.pubupdate;
				entry->pubactions.pubdelete |= pub->pubactions.pubdelete;
				entry->pubactions.pubtruncate |= pub->pubactions.pubtruncate;
			}

		}

		list_free(pubids);

		entry->publish_as_relid = publish_as_relid;
		entry->replicate_valid = true;
	}

	return entry;
}

/*
 * Cleanup list of streamed transactions and update the schema_sent flag.
 *
 * When a streamed transaction commits or aborts, we need to remove the
 * toplevel XID from the schema cache. If the transaction aborted, the
 * subscriber will simply throw away the schema records we streamed, so
 * we don't need to do anything else.
 *
 * If the transaction is committed, the subscriber will update the relation
 * cache - so tweak the schema_sent flag accordingly.
 */
static void
cleanup_rel_sync_cache(TransactionId xid, bool is_commit)
{
	HASH_SEQ_STATUS hash_seq;
	RelationSyncEntry *entry;
	ListCell   *lc;

	Assert(RelationSyncCache != NULL);

	hash_seq_init(&hash_seq, RelationSyncCache);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		/*
		 * We can set the schema_sent flag for an entry that has committed xid
		 * in the list as that ensures that the subscriber would have the
		 * corresponding schema and we don't need to send it unless there is
		 * any invalidation for that relation.
		 */
		foreach(lc, entry->streamed_txns)
		{
			if (xid == (uint32) lfirst_int(lc))
			{
				if (is_commit)
					entry->schema_sent = true;

				entry->streamed_txns =
					foreach_delete_current(entry->streamed_txns, lc);
				break;
			}
		}
	}
}

/*
 * Relcache invalidation callback
 */
static void
rel_sync_cache_relation_cb(Datum arg, Oid relid)
{
	RelationSyncEntry *entry;
	int	idx;

	/*
	 * We can get here if the plugin was used in SQL interface as the
	 * RelSchemaSyncCache is destroyed when the decoding finishes, but there
	 * is no way to unregister the relcache invalidation callback.
	 */
	if (RelationSyncCache == NULL)
		return;

	/*
	 * Nobody keeps pointers to entries in this hash table around outside
	 * logical decoding callback calls - but invalidation events can come in
	 * *during* a callback if we access the relcache in the callback. Because
	 * of that we must mark the cache entry as invalid but not remove it from
	 * the hash while it could still be referenced, then prune it at a later
	 * safe point.
	 *
	 * Getting invalidations for relations that aren't in the table is
	 * entirely normal, since there's no way to unregister for an invalidation
	 * event. So we don't care if it's found or not.
	 */
	entry = (RelationSyncEntry *) hash_search(RelationSyncCache, &relid,
											  HASH_FIND, NULL);

	/*
	 * Reset schema sent status as the relation definition may have changed.
	 * Also free any objects that depended on the earlier definition.
	 */
	if (entry != NULL)
	{
		entry->schema_sent = false;
		list_free(entry->streamed_txns);
		entry->streamed_txns = NIL;
		if (entry->map)
		{
			/*
			 * Must free the TupleDescs contained in the map explicitly,
			 * because free_conversion_map() doesn't.
			 */
			FreeTupleDesc(entry->map->indesc);
			FreeTupleDesc(entry->map->outdesc);
			free_conversion_map(entry->map);
		}
		entry->map = NULL;

		/*
		 * Row filter cache cleanups. (Will be rebuilt later if needed).
		 */
		entry->rowfilter_valid = false;
		if (entry->scantuple != NULL)
		{
			ExecDropSingleTupleTableSlot(entry->scantuple);
			entry->scantuple = NULL;
		}
		/* Cleanup the ExprState for each of the pubactions. */
		for (idx = 0; idx < 4; idx++)
		{
			if (entry->exprstate[idx] != NULL)
			{
				pfree(entry->exprstate[idx]);
				entry->exprstate[idx] = NULL;
			}
		}
	}
}

/*
 * Publication relation/schema map syscache invalidation callback
 */
static void
rel_sync_cache_publication_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS status;
	RelationSyncEntry *entry;
	MemoryContext oldctx;

	/*
	 * We can get here if the plugin was used in SQL interface as the
	 * RelSchemaSyncCache is destroyed when the decoding finishes, but there
	 * is no way to unregister the relcache invalidation callback.
	 */
	if (RelationSyncCache == NULL)
		return;

	oldctx = MemoryContextSwitchTo(CacheMemoryContext);

	/*
	 * There is no way to find which entry in our cache the hash belongs to so
	 * mark the whole cache as invalid.
	 */
	hash_seq_init(&status, RelationSyncCache);
	while ((entry = (RelationSyncEntry *) hash_seq_search(&status)) != NULL)
	{
		entry->replicate_valid = false;

		/*
		 * There might be some relations dropped from the publication so we
		 * don't need to publish the changes for them.
		 */
		entry->pubactions.pubinsert = false;
		entry->pubactions.pubupdate = false;
		entry->pubactions.pubdelete = false;
		entry->pubactions.pubtruncate = false;
	}

	MemoryContextSwitchTo(oldctx);
}

/* Send Replication origin */
static void
send_repl_origin(LogicalDecodingContext *ctx, RepOriginId origin_id,
				 XLogRecPtr origin_lsn, bool send_origin)
{
	if (send_origin)
	{
		char	   *origin;

		/*----------
		 * XXX: which behaviour do we want here?
		 *
		 * Alternatives:
		 *  - don't send origin message if origin name not found
		 *    (that's what we do now)
		 *  - throw error - that will break replication, not good
		 *  - send some special "unknown" origin
		 *----------
		 */
		if (replorigin_by_oid(origin_id, true, &origin))
		{
			/* Message boundary */
			OutputPluginWrite(ctx, false);
			OutputPluginPrepareWrite(ctx, true);

			logicalrep_write_origin(ctx->out, origin, origin_lsn);
		}
	}
}
