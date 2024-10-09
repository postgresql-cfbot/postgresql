/* TODO Move into src/backend/cluster/ (and rename?) */
/*-------------------------------------------------------------------------
 *
 * pgoutput_cluster.c
 *		Logical Replication output plugin for CLUSTER command
 *
 * Copyright (c) 2012-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/backend/replication/pgoutput_cluster/pgoutput_cluster.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heaptoast.h"
#include "commands/cluster.h"
#include "replication/snapbuild.h"

PG_MODULE_MAGIC;

static void plugin_startup(LogicalDecodingContext *ctx,
						   OutputPluginOptions *opt, bool is_init);
static void plugin_shutdown(LogicalDecodingContext *ctx);
static void plugin_begin_txn(LogicalDecodingContext *ctx,
							 ReorderBufferTXN *txn);
static void plugin_commit_txn(LogicalDecodingContext *ctx,
							  ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void plugin_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
						  Relation rel, ReorderBufferChange *change);
static void plugin_truncate(struct LogicalDecodingContext *ctx,
							ReorderBufferTXN *txn, int nrelations,
							Relation relations[],
							ReorderBufferChange *change);
static void store_change(LogicalDecodingContext *ctx,
						 ConcurrentChangeKind kind, HeapTuple tuple);

void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = plugin_startup;
	cb->begin_cb = plugin_begin_txn;
	cb->change_cb = plugin_change;
	cb->truncate_cb = plugin_truncate;
	cb->commit_cb = plugin_commit_txn;
	cb->shutdown_cb = plugin_shutdown;
}


/* initialize this plugin */
static void
plugin_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
			   bool is_init)
{
	ctx->output_plugin_private = NULL;

	/* Probably unnecessary, as we don't use the SQL interface ... */
	opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

	if (ctx->output_plugin_options != NIL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("This plugin does not expect any options")));
	}
}

static void
plugin_shutdown(LogicalDecodingContext *ctx)
{
}

/*
 * As we don't release the slot during processing of particular table, there's
 * no room for SQL interface, even for debugging purposes. Therefore we need
 * neither OutputPluginPrepareWrite() nor OutputPluginWrite() in the plugin
 * callbacks. (Although we might want to write custom callbacks, this API
 * seems to be unnecessarily generic for our purposes.)
 */

/* BEGIN callback */
static void
plugin_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
}

/* COMMIT callback */
static void
plugin_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				  XLogRecPtr commit_lsn)
{
}

/*
 * Callback for individual changed tuples
 */
static void
plugin_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
			  Relation relation, ReorderBufferChange *change)
{
	ClusterDecodingState *dstate;

	dstate = (ClusterDecodingState *) ctx->output_writer_private;

	/* Only interested in one particular relation. */
	if (relation->rd_id != dstate->relid)
		return;

	/* Decode entry depending on its type */
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			{
				HeapTuple	newtuple;

				newtuple = change->data.tp.newtuple != NULL ?
					change->data.tp.newtuple : NULL;

				/*
				 * Identity checks in the main function should have made this
				 * impossible.
				 */
				if (newtuple == NULL)
					elog(ERROR, "Incomplete insert info.");

				store_change(ctx, CHANGE_INSERT, newtuple);
			}
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			{
				HeapTuple	oldtuple,
							newtuple;

				oldtuple = change->data.tp.oldtuple != NULL ?
					change->data.tp.oldtuple : NULL;
				newtuple = change->data.tp.newtuple != NULL ?
					change->data.tp.newtuple : NULL;

				if (newtuple == NULL)
					elog(ERROR, "Incomplete update info.");

				if (oldtuple != NULL)
					store_change(ctx, CHANGE_UPDATE_OLD, oldtuple);

				store_change(ctx, CHANGE_UPDATE_NEW, newtuple);
			}
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			{
				HeapTuple	oldtuple;

				oldtuple = change->data.tp.oldtuple ?
					change->data.tp.oldtuple : NULL;

				if (oldtuple == NULL)
					elog(ERROR, "Incomplete delete info.");

				store_change(ctx, CHANGE_DELETE, oldtuple);
			}
			break;
		default:
			/* Should not come here */
			Assert(false);
			break;
	}
}

static void
plugin_truncate(struct LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				int nrelations, Relation relations[],
				ReorderBufferChange *change)
{
	ClusterDecodingState *dstate;
	int		i;
	Relation	relation = NULL;

	dstate = (ClusterDecodingState *) ctx->output_writer_private;

	/* Find the relation we are processing. */
	for (i = 0; i < nrelations; i++)
	{
		relation = relations[i];

		if (RelationGetRelid(relation) == dstate->relid)
			break;
	}

	/* Is this truncation of another relation? */
	if (i == nrelations)
		return;

	store_change(ctx, CHANGE_TRUNCATE, NULL);
}

/* Store concurrent data change. */
static void
store_change(LogicalDecodingContext *ctx, ConcurrentChangeKind kind,
			 HeapTuple tuple)
{
	ClusterDecodingState *dstate;
	char	   *change_raw;
	ConcurrentChange *change;
	bool		flattened = false;
	Size		size;
	Datum		values[1];
	bool		isnull[1];
	char	   *dst;

	dstate = (ClusterDecodingState *) ctx->output_writer_private;

	size = MAXALIGN(VARHDRSZ) + sizeof(ConcurrentChange);

	if (tuple)
	{
		/*
		 * ReorderBufferCommit() stores the TOAST chunks in its private memory
		 * context and frees them after having called
		 * apply_change(). Therefore we need flat copy (including TOAST) that
		 * we eventually copy into the memory context which is available to
		 * decode_concurrent_changes().
		 */
		if (HeapTupleHasExternal(tuple))
		{
			/*
			 * toast_flatten_tuple_to_datum() might be more convenient but we
			 * don't want the decompression it does.
			 */
			tuple = toast_flatten_tuple(tuple, dstate->tupdesc);
			flattened = true;
		}

		size += tuple->t_len;
	}

	/* XXX Isn't there any function / macro to do this? */
	if (size >= 0x3FFFFFFF)
		elog(ERROR, "Change is too big.");

	/* Construct the change. */
	change_raw = (char *) palloc0(size);
	SET_VARSIZE(change_raw, size);
	change = (ConcurrentChange *) VARDATA(change_raw);
	change->kind = kind;

	/* No other information is needed for TRUNCATE. */
	if (change->kind == CHANGE_TRUNCATE)
		goto store;

	/*
	 * Copy the tuple.
	 *
	 * CAUTION: change->tup_data.t_data must be fixed on retrieval!
	 */
	memcpy(&change->tup_data, tuple, sizeof(HeapTupleData));
	dst = (char *) change + sizeof(ConcurrentChange);
	memcpy(dst, tuple->t_data, tuple->t_len);

	/* The data has been copied. */
	if (flattened)
		pfree(tuple);

store:
	/* Store as tuple of 1 bytea column. */
	values[0] = PointerGetDatum(change_raw);
	isnull[0] = false;
	tuplestore_putvalues(dstate->tstore, dstate->tupdesc_change,
						 values, isnull);

	/* Accounting. */
	dstate->nchanges++;

	/* Cleanup. */
	pfree(change_raw);
}

