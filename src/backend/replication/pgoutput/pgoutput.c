/*-------------------------------------------------------------------------
 *
 * pgoutput.c
 *		Logical Replication output plugin
 *
 * Copyright (c) 2012-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/backend/replication/pgoutput/pgoutput.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tupconvert.h"
#include "catalog/pg_publication.h"
#include "fmgr.h"
#include "replication/logical.h"
#include "replication/logicalproto.h"
#include "replication/origin.h"
#include "replication/pgoutput.h"
#include "utils/int8.h"
#include "utils/inval.h"
#include "utils/memutils.h"
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
static bool pgoutput_origin_filter(LogicalDecodingContext *ctx,
								   RepOriginId origin_id);

static bool publications_valid;

static List *LoadPublications(List *pubnames);
static void publication_invalidation_cb(Datum arg, int cacheid,
										uint32 hashvalue);
static void send_relation_and_attrs(Relation relation, LogicalDecodingContext *ctx);

/* Entry in the map used to remember which relation schemas we sent. */
typedef struct RelationSyncEntry
{
	Oid			relid;			/* relation oid */

	/*
	 * Did we send the schema -- either own or ancestor's?
	 */
	bool		schema_sent;
	bool		replicate_valid;
	PublicationActions pubactions;

	/*
	 * Valid if publishing relation's changes as changes to some ancestor,
	 * that is, if relation is a partition.  The map, if any, will be used
	 * to convert the tuples from partition's type to the ancestor's.
	 */
	Oid			replicate_as_relid;
	TupleConversionMap *map;
} RelationSyncEntry;

/* Map used to remember which relation schemas we sent. */
static HTAB *RelationSyncCache = NULL;

static void init_rel_sync_cache(MemoryContext decoding_context);
static RelationSyncEntry *get_rel_sync_entry(PGOutputData *data, Relation rel);
static void rel_sync_cache_relation_cb(Datum arg, Oid relid);
static void rel_sync_cache_publication_cb(Datum arg, int cacheid,
										  uint32 hashvalue);

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
	cb->commit_cb = pgoutput_commit_txn;
	cb->filter_by_origin_cb = pgoutput_origin_filter;
	cb->shutdown_cb = pgoutput_shutdown;
}

static void
parse_output_parameters(List *options, uint32 *protocol_version,
						List **publication_names)
{
	ListCell   *lc;
	bool		protocol_version_given = false;
	bool		publication_names_given = false;

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

			*protocol_version = (uint32) parsed;
		}
		else if (strcmp(defel->defname, "publication_names") == 0)
		{
			if (publication_names_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			publication_names_given = true;

			if (!SplitIdentifierString(strVal(defel->arg), ',',
									   publication_names))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_NAME),
						 errmsg("invalid publication_names syntax")));
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
		parse_output_parameters(ctx->output_plugin_options,
								&data->protocol_version,
								&data->publication_names);

		/* Check if we support requested protocol */
		if (data->protocol_version > LOGICALREP_PROTO_VERSION_NUM)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("client sent proto_version=%d but we only support protocol %d or lower",
							data->protocol_version, LOGICALREP_PROTO_VERSION_NUM)));

		if (data->protocol_version < LOGICALREP_PROTO_MIN_VERSION_NUM)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("client sent proto_version=%d but we only support protocol %d or higher",
							data->protocol_version, LOGICALREP_PROTO_MIN_VERSION_NUM)));

		if (list_length(data->publication_names) < 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("publication_names parameter missing")));

		/* Init publication state. */
		data->publications = NIL;
		publications_valid = false;
		CacheRegisterSyscacheCallback(PUBLICATIONOID,
									  publication_invalidation_cb,
									  (Datum) 0);

		/* Initialize relation schema cache. */
		init_rel_sync_cache(CacheMemoryContext);
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

	if (send_replication_origin)
	{
		char	   *origin;

		/* Message boundary */
		OutputPluginWrite(ctx, false);
		OutputPluginPrepareWrite(ctx, true);

		/*----------
		 * XXX: which behaviour do we want here?
		 *
		 * Alternatives:
		 *	- don't send origin message if origin name not found
		 *	  (that's what we do now)
		 *	- throw error - that will break replication, not good
		 *	- send some special "unknown" origin
		 *----------
		 */
		if (replorigin_by_oid(txn->origin_id, true, &origin))
			logicalrep_write_origin(ctx->out, origin, txn->origin_lsn);
	}

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
 * Write the current schema of the relation and its ancestor (if any) if not
 * done yet.
 */
static void
maybe_send_schema(LogicalDecodingContext *ctx,
				  Relation relation, RelationSyncEntry *relentry)
{
	if (relentry->schema_sent)
		return;

	/* If needed, send the ancestor's schema first. */
	if (OidIsValid(relentry->replicate_as_relid))
	{
		Relation ancestor =
			RelationIdGetRelation(relentry->replicate_as_relid);
		TupleDesc	indesc = RelationGetDescr(relation);
		TupleDesc 	outdesc = RelationGetDescr(ancestor);
		MemoryContext oldctx;

		/* Map must live as long as the session does. */
		oldctx = MemoryContextSwitchTo(CacheMemoryContext);
		relentry->map = convert_tuples_by_name(indesc, outdesc);
		MemoryContextSwitchTo(oldctx);
		send_relation_and_attrs(ancestor, ctx);
		RelationClose(ancestor);
	}

	send_relation_and_attrs(relation, ctx);
	relentry->schema_sent = true;
}

/*
 * Sends a relation
 */
static void
send_relation_and_attrs(Relation relation, LogicalDecodingContext *ctx)
{
	TupleDesc	desc = RelationGetDescr(relation);
	int			i;

	/*
	 * Write out type info if needed.  We do that only for user-created
	 * types.  We use FirstGenbkiObjectId as the cutoff, so that we only
	 * consider objects with hand-assigned OIDs to be "built in", not for
	 * instance any function or type defined in the information_schema.
	 * This is important because only hand-assigned OIDs can be expected
	 * to remain stable across major versions.
	 */
	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);

		if (att->attisdropped || att->attgenerated)
			continue;

		if (att->atttypid < FirstGenbkiObjectId)
			continue;

		OutputPluginPrepareWrite(ctx, false);
		logicalrep_write_typ(ctx->out, att->atttypid);
		OutputPluginWrite(ctx, false);
	}

	OutputPluginPrepareWrite(ctx, false);
	logicalrep_write_rel(ctx->out, relation);
	OutputPluginWrite(ctx, false);
}

/*
 * Sends the decoded DML over wire.
 */
static void
pgoutput_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				Relation relation, ReorderBufferChange *change)
{
	PGOutputData *data = (PGOutputData *) ctx->output_plugin_private;
	MemoryContext old;
	RelationSyncEntry *relentry;

	if (!is_publishable_relation(relation))
		return;

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

	maybe_send_schema(ctx, relation, relentry);

	/* Send the data */
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			{
				HeapTuple	tuple = &change->data.tp.newtuple->tuple;

				if (OidIsValid(relentry->replicate_as_relid))
				{
					relation = RelationIdGetRelation(relentry->replicate_as_relid);
					if (relentry->map)
						tuple = execute_attr_map_tuple(tuple, relentry->map);
				}

				OutputPluginPrepareWrite(ctx, true);
				logicalrep_write_insert(ctx->out, relation, tuple);
				OutputPluginWrite(ctx, true);
				break;
			}
		case REORDER_BUFFER_CHANGE_UPDATE:
			{
				HeapTuple	oldtuple = change->data.tp.oldtuple ?
				&change->data.tp.oldtuple->tuple : NULL;
				HeapTuple	newtuple = &change->data.tp.newtuple->tuple;

				if (OidIsValid(relentry->replicate_as_relid))
				{
					relation = RelationIdGetRelation(relentry->replicate_as_relid);
					if (relentry->map)
					{
						oldtuple = execute_attr_map_tuple(oldtuple, relentry->map);
						newtuple = execute_attr_map_tuple(newtuple, relentry->map);
					}
				}

				OutputPluginPrepareWrite(ctx, true);
				logicalrep_write_update(ctx->out, relation, oldtuple, newtuple);
				OutputPluginWrite(ctx, true);
				break;
			}
		case REORDER_BUFFER_CHANGE_DELETE:
			if (change->data.tp.oldtuple)
			{
				HeapTuple	oldtuple = &change->data.tp.oldtuple->tuple;

				if (OidIsValid(relentry->replicate_as_relid))
				{
					relation = RelationIdGetRelation(relentry->replicate_as_relid);
					if (relentry->map)
						oldtuple = execute_attr_map_tuple(oldtuple, relentry->map);
				}

				OutputPluginPrepareWrite(ctx, true);
				logicalrep_write_delete(ctx->out, relation, oldtuple);
				OutputPluginWrite(ctx, true);
			}
			else
				elog(DEBUG1, "didn't send DELETE change because of missing oldtuple");
			break;
		default:
			Assert(false);
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

		relids[nrelids++] = relid;
		maybe_send_schema(ctx, relation, relentry);
	}

	if (nrelids > 0)
	{
		OutputPluginPrepareWrite(ctx, true);
		logicalrep_write_truncate(ctx->out,
								  nrelids,
								  relids,
								  change->data.truncate.cascade,
								  change->data.truncate.restart_seqs);
		OutputPluginWrite(ctx, true);
	}

	MemoryContextSwitchTo(old);
	MemoryContextReset(data->context);
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
	MemoryContext old_ctxt;

	if (RelationSyncCache != NULL)
		return;

	/* Make a new hash table for the cache */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(RelationSyncEntry);
	ctl.hcxt = cachectx;

	old_ctxt = MemoryContextSwitchTo(cachectx);
	RelationSyncCache = hash_create("logical replication output relation cache",
									128, &ctl,
									HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
	(void) MemoryContextSwitchTo(old_ctxt);

	Assert(RelationSyncCache != NULL);

	CacheRegisterRelcacheCallback(rel_sync_cache_relation_cb, (Datum) 0);
	CacheRegisterSyscacheCallback(PUBLICATIONRELMAP,
								  rel_sync_cache_publication_cb,
								  (Datum) 0);
}

/*
 * Find or create entry in the relation schema cache.
 *
 * For a partition, the schema of the top-most ancestor that is published
 * will be used instead of that of the partition itself, so the information
 * about ancestor's publications is looked up here and saved in the schema
 * cache entry.
 */
static RelationSyncEntry *
get_rel_sync_entry(PGOutputData *data, Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	RelationSyncEntry *entry;
	bool		found;
	MemoryContext oldctx;

	Assert(RelationSyncCache != NULL);

	/* Find cached function info, creating if not found */
	oldctx = MemoryContextSwitchTo(CacheMemoryContext);
	entry = (RelationSyncEntry *) hash_search(RelationSyncCache,
											  (void *) &relid,
											  HASH_ENTER, &found);
	MemoryContextSwitchTo(oldctx);
	Assert(entry != NULL);

	/* Not found means schema wasn't sent */
	if (!found || !entry->replicate_valid)
	{
		List	   *pubids = GetRelationPublications(relid);
		ListCell   *lc;

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
		entry->pubactions.pubinsert = entry->pubactions.pubupdate =
			entry->pubactions.pubdelete = entry->pubactions.pubtruncate = false;

		foreach(lc, data->publications)
		{
			Publication *pub = lfirst(lc);

			if (pub->alltables || list_member_oid(pubids, pub->oid))
			{
				entry->pubactions.pubinsert |= pub->pubactions.pubinsert;
				entry->pubactions.pubupdate |= pub->pubactions.pubupdate;
				entry->pubactions.pubdelete |= pub->pubactions.pubdelete;
				entry->pubactions.pubtruncate |= pub->pubactions.pubtruncate;
			}

			if (entry->pubactions.pubinsert && entry->pubactions.pubupdate &&
				entry->pubactions.pubdelete && entry->pubactions.pubtruncate)
				break;
		}

		/*
		 * For partitions, we prefer to publish their changes using an
		 * ancestor's schema (usually the top-most ancestor) if it is
		 * published, but only if a publication explicitly lists the ancestor
		 * as its member (that is, not a FOR ALL TABLES publication).
		 */
		if (rel->rd_rel->relispartition)
		{
			List	   *ancestor_pubids;
			List	   *published_ancestors = NIL;
			Oid			topmost_published_ancestor = InvalidOid;

			ancestor_pubids =
				GetRelationAncestorPublications(rel, &published_ancestors);

			foreach(lc, data->publications)
			{
				Publication *pub = lfirst(lc);
					ListCell *lc1,
							 *lc2;

				forboth(lc1, ancestor_pubids, lc2, published_ancestors)
				{
					if (lfirst_oid(lc1) == pub->oid)
					{
						entry->pubactions.pubinsert |= pub->pubactions.pubinsert;
						entry->pubactions.pubupdate |= pub->pubactions.pubupdate;
						entry->pubactions.pubdelete |= pub->pubactions.pubdelete;
						entry->pubactions.pubtruncate |= pub->pubactions.pubtruncate;
						topmost_published_ancestor = lfirst_oid(lc2);
					}
				}

				if (entry->pubactions.pubinsert && entry->pubactions.pubupdate &&
					entry->pubactions.pubdelete && entry->pubactions.pubtruncate)
					break;
			}

			if (OidIsValid(topmost_published_ancestor))
				entry->replicate_as_relid = topmost_published_ancestor;

			list_free(ancestor_pubids);
			list_free(published_ancestors);
		}

		list_free(pubids);

		entry->replicate_valid = true;
	}

	if (!found)
		entry->schema_sent = false;

	return entry;
}

/*
 * Relcache invalidation callback
 */
static void
rel_sync_cache_relation_cb(Datum arg, Oid relid)
{
	RelationSyncEntry *entry;

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
	 */
	if (entry != NULL)
		entry->schema_sent = false;
}

/*
 * Publication relation map syscache invalidation callback
 */
static void
rel_sync_cache_publication_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS status;
	RelationSyncEntry *entry;

	/*
	 * We can get here if the plugin was used in SQL interface as the
	 * RelSchemaSyncCache is destroyed when the decoding finishes, but there
	 * is no way to unregister the relcache invalidation callback.
	 */
	if (RelationSyncCache == NULL)
		return;

	/*
	 * There is no way to find which entry in our cache the hash belongs to so
	 * mark the whole cache as invalid.
	 */
	hash_seq_init(&status, RelationSyncCache);
	while ((entry = (RelationSyncEntry *) hash_seq_search(&status)) != NULL)
		entry->replicate_valid = false;
}
