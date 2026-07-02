/*-------------------------------------------------------------------------
 * relation.c
 *	   PostgreSQL logical replication relation mapping cache
 *
 * Copyright (c) 2016-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/relation.c
 *
 * NOTES
 *	  Routines in this file mainly have to do with mapping the properties
 *	  of local replication target relations to the properties of their
 *	  remote counterpart.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/amapi.h"
#include "access/genam.h"
#include "access/table.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_subscription_rel.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "replication/logicalrelation.h"
#include "replication/worker_internal.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


static MemoryContext LogicalRepRelMapContext = NULL;

static HTAB *LogicalRepRelMap = NULL;

/*
 * Partition map (LogicalRepPartMap)
 *
 * When a partitioned table is used as replication target, replicated
 * operations are actually performed on its leaf partitions, which requires
 * the partitions to also be mapped to the remote relation.  Parent's entry
 * (LogicalRepRelMapEntry) cannot be used as-is for all partitions, because
 * individual partitions may have different attribute numbers, which means
 * attribute mappings to remote relation's attributes must be maintained
 * separately for each partition.
 */
static MemoryContext LogicalRepPartMapContext = NULL;
static HTAB *LogicalRepPartMap = NULL;
typedef struct LogicalRepPartMapEntry
{
	Oid			partoid;		/* LogicalRepPartMap's key */
	LogicalRepRelMapEntry relmapentry;
} LogicalRepPartMapEntry;

static Oid	FindLogicalRepLocalIndex(Relation localrel, LogicalRepRelation *remoterel,
									 AttrMap *attrMap);

/*
 * Relcache invalidation callback for our relation map cache.
 */
static void
logicalrep_relmap_invalidate_cb(Datum arg, Oid reloid)
{
	LogicalRepRelMapEntry *entry;

	/* Just to be sure. */
	if (LogicalRepRelMap == NULL)
		return;

	if (reloid != InvalidOid)
	{
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, LogicalRepRelMap);

		/* TODO, use inverse lookup hashtable? */
		while ((entry = (LogicalRepRelMapEntry *) hash_seq_search(&status)) != NULL)
		{
			if (entry->localreloid == reloid)
			{
				entry->localrelvalid = false;
				entry->local_unique_indexes_collected = false;
				entry->local_fkeys_collected = false;
				hash_seq_term(&status);
				break;
			}
		}
	}
	else
	{
		/* invalidate all cache entries */
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, LogicalRepRelMap);

		while ((entry = (LogicalRepRelMapEntry *) hash_seq_search(&status)) != NULL)
		{
			entry->localrelvalid = false;
			entry->local_unique_indexes_collected = false;
			entry->local_fkeys_collected = false;
		}
	}
}

/*
 * Initialize the relation map cache.
 */
static void
logicalrep_relmap_init(void)
{
	HASHCTL		ctl;

	if (!LogicalRepRelMapContext)
		LogicalRepRelMapContext =
			AllocSetContextCreate(CacheMemoryContext,
								  "LogicalRepRelMapContext",
								  ALLOCSET_DEFAULT_SIZES);

	/* Initialize the relation hash table. */
	ctl.keysize = sizeof(LogicalRepRelId);
	ctl.entrysize = sizeof(LogicalRepRelMapEntry);
	ctl.hcxt = LogicalRepRelMapContext;

	LogicalRepRelMap = hash_create("logicalrep relation map cache", 128, &ctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* Watch for invalidation events. */
	CacheRegisterRelcacheCallback(logicalrep_relmap_invalidate_cb,
								  (Datum) 0);
}

/*
 * Release local index list
 */
static void
free_local_unique_indexes(LogicalRepRelMapEntry *entry)
{
	Assert(am_leader_apply_worker());

	foreach_ptr(LogicalRepSubscriberIdx, idxinfo, entry->local_unique_indexes)
		bms_free(idxinfo->indexkeys);

	list_free_deep(entry->local_unique_indexes);
	entry->local_unique_indexes = NIL;
}

/*
 * Release local foreign key lists.
 */
static void
free_local_fkeys(LogicalRepRelMapEntry *entry)
{
	Assert(am_leader_apply_worker());

	foreach_ptr(LogicalRepSubscriberFK, fkinfo, entry->local_fkeys)
	{
		list_free(fkinfo->fkattnums);
		list_free(fkinfo->fkattnums_old);
	}

	foreach_ptr(LogicalRepSubscriberRefFK, refinfo, entry->local_referenced_fkeys)
	{
		list_free(refinfo->refattnums);
		list_free(refinfo->refattnums_old);
	}

	list_free_deep(entry->local_fkeys);
	list_free_deep(entry->local_referenced_fkeys);

	entry->local_fkeys = NIL;
	entry->local_referenced_fkeys = NIL;
}

/*
 * Free the entry of a relation map cache.
 */
static void
logicalrep_relmap_free_entry(LogicalRepRelMapEntry *entry)
{
	LogicalRepRelation *remoterel;

	remoterel = &entry->remoterel;

	pfree(remoterel->nspname);
	pfree(remoterel->relname);

	if (remoterel->natts > 0)
	{
		int			i;

		for (i = 0; i < remoterel->natts; i++)
			pfree(remoterel->attnames[i]);

		pfree(remoterel->attnames);
		pfree(remoterel->atttyps);
	}
	bms_free(remoterel->attkeys);

	if (entry->attrmap)
		free_attrmap(entry->attrmap);

	if (entry->local_unique_indexes != NIL)
		free_local_unique_indexes(entry);

	if (entry->local_fkeys != NIL || entry->local_referenced_fkeys != NIL)
		free_local_fkeys(entry);
}

/*
 * Add new entry or update existing entry in the relation map cache.
 *
 * Called when new relation mapping is sent by the publisher to update
 * our expected view of incoming data from said publisher.
 *
 * Note that we do not check the user-defined constraints here. PostgreSQL has
 * already assumed that CHECK constraints' conditions are immutable and here
 * follows the rule.
 */
void
logicalrep_relmap_update(LogicalRepRelation *remoterel)
{
	MemoryContext oldctx;
	LogicalRepRelMapEntry *entry;
	bool		found;
	int			i;

	if (LogicalRepRelMap == NULL)
		logicalrep_relmap_init();

	/*
	 * HASH_ENTER returns the existing entry if present or creates a new one.
	 */
	entry = hash_search(LogicalRepRelMap, &remoterel->remoteid,
						HASH_ENTER, &found);

	if (found)
		logicalrep_relmap_free_entry(entry);

	memset(entry, 0, sizeof(LogicalRepRelMapEntry));

	/* Make cached copy of the data */
	oldctx = MemoryContextSwitchTo(LogicalRepRelMapContext);
	entry->remoterel.remoteid = remoterel->remoteid;
	entry->remoterel.nspname = pstrdup(remoterel->nspname);
	entry->remoterel.relname = pstrdup(remoterel->relname);
	entry->remoterel.natts = remoterel->natts;
	entry->remoterel.attnames = palloc_array(char *, remoterel->natts);
	entry->remoterel.atttyps = palloc_array(Oid, remoterel->natts);
	for (i = 0; i < remoterel->natts; i++)
	{
		entry->remoterel.attnames[i] = pstrdup(remoterel->attnames[i]);
		entry->remoterel.atttyps[i] = remoterel->atttyps[i];
	}
	entry->remoterel.replident = remoterel->replident;

	/*
	 * XXX The walsender currently does not transmit the relkind of the remote
	 * relation when replicating changes. Since we support replicating only
	 * table changes at present, we default to initializing relkind as
	 * RELKIND_RELATION. This is needed in CheckSubscriptionRelkind() to check
	 * if the publisher and subscriber relation kinds are compatible.
	 */
	entry->remoterel.relkind =
		(remoterel->relkind == 0) ? RELKIND_RELATION : remoterel->relkind;

	entry->remoterel.attkeys = bms_copy(remoterel->attkeys);

	entry->parallel_safe = LOGICALREP_PARALLEL_UNKNOWN;
	entry->local_unique_indexes_collected = false;
	entry->local_fkeys_collected = false;
	MemoryContextSwitchTo(oldctx);
}

/*
 * Find attribute index in TupleDesc struct by attribute name.
 *
 * Returns -1 if not found.
 */
static int
logicalrep_rel_att_by_name(LogicalRepRelation *remoterel, const char *attname)
{
	int			i;

	for (i = 0; i < remoterel->natts; i++)
	{
		if (strcmp(remoterel->attnames[i], attname) == 0)
			return i;
	}

	return -1;
}

/*
 * Returns a comma-separated string of attribute names based on the provided
 * relation and bitmap indicating which attributes to include.
 */
static char *
logicalrep_get_attrs_str(LogicalRepRelation *remoterel, Bitmapset *atts)
{
	StringInfoData attsbuf;
	bool		first = true;
	int			i = -1;

	Assert(!bms_is_empty(atts));

	initStringInfo(&attsbuf);

	while ((i = bms_next_member(atts, i)) >= 0)
	{
		if (first)
			appendStringInfo(&attsbuf, _("\"%s\""), remoterel->attnames[i]);
		else
			appendStringInfo(&attsbuf, _(", \"%s\""), remoterel->attnames[i]);
		first = false;
	}

	return attsbuf.data;
}

/*
 * If attempting to replicate missing or generated columns, report an error.
 * Prioritize 'missing' errors if both occur though the prioritization is
 * arbitrary.
 */
static void
logicalrep_report_missing_or_gen_attrs(LogicalRepRelation *remoterel,
									   Bitmapset *missingatts,
									   Bitmapset *generatedatts)
{
	if (!bms_is_empty(missingatts))
		ereport(ERROR,
				errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg_plural("logical replication target relation \"%s.%s\" is missing replicated column: %s",
							  "logical replication target relation \"%s.%s\" is missing replicated columns: %s",
							  bms_num_members(missingatts),
							  remoterel->nspname,
							  remoterel->relname,
							  logicalrep_get_attrs_str(remoterel,
													   missingatts)));

	if (!bms_is_empty(generatedatts))
		ereport(ERROR,
				errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg_plural("logical replication target relation \"%s.%s\" has incompatible generated column: %s",
							  "logical replication target relation \"%s.%s\" has incompatible generated columns: %s",
							  bms_num_members(generatedatts),
							  remoterel->nspname,
							  remoterel->relname,
							  logicalrep_get_attrs_str(remoterel,
													   generatedatts)));
}

/*
 * Check if replica identity matches and mark the updatable flag.
 *
 * We allow for stricter replica identity (fewer columns) on subscriber as
 * that will not stop us from finding unique tuple. IE, if publisher has
 * identity (id,timestamp) and subscriber just (id) this will not be a
 * problem, but in the opposite scenario it will.
 *
 * We just mark the relation entry as not updatable here if the local
 * replica identity is found to be insufficient for applying
 * updates/deletes (inserts don't care!) and leave it to
 * check_relation_updatable() to throw the actual error if needed.
 */
static void
logicalrep_rel_mark_updatable(LogicalRepRelMapEntry *entry)
{
	Bitmapset  *idkey;
	LogicalRepRelation *remoterel = &entry->remoterel;
	int			i;

	entry->updatable = true;

	idkey = RelationGetIndexAttrBitmap(entry->localrel,
									   INDEX_ATTR_BITMAP_IDENTITY_KEY);
	/* fallback to PK if no replica identity */
	if (idkey == NULL)
	{
		idkey = RelationGetIndexAttrBitmap(entry->localrel,
										   INDEX_ATTR_BITMAP_PRIMARY_KEY);

		/*
		 * If no replica identity index and no PK, the published table must
		 * have replica identity FULL.
		 */
		if (idkey == NULL && remoterel->replident != REPLICA_IDENTITY_FULL)
			entry->updatable = false;
	}

	i = -1;
	while ((i = bms_next_member(idkey, i)) >= 0)
	{
		int			attnum = i + FirstLowInvalidHeapAttributeNumber;

		if (!AttrNumberIsForUserDefinedAttr(attnum))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("logical replication target relation \"%s.%s\" uses "
							"system columns in REPLICA IDENTITY index",
							remoterel->nspname, remoterel->relname)));

		attnum = AttrNumberGetAttrOffset(attnum);

		if (entry->attrmap->attnums[attnum] < 0 ||
			!bms_is_member(entry->attrmap->attnums[attnum], remoterel->attkeys))
		{
			entry->updatable = false;
			break;
		}
	}
}

/*
 * Collect all local unique indexes that can be used for dependency tracking
 *
 * This function collects all types of unique indexes, including those with
 * index expressions and partial indexes. However, to avoid the overhead and
 * complexity of executing expressions, we do not evaluate them during
 * dependency tracking.
 *
 * For indexes with expressions, only the non-expression columns are recorded in
 * the bitmap. The dependency tracking function will use only these columns,
 * which may lead to false dependency detection. For example, consider a unique
 * index defined as UNIQUE (a, func(b)), where b is an expression column. Rows
 * (1, 2) and (1, 3) will be treated as dependent even though they are not. This
 * is acceptable, as it is still better than disabling parallelism for all
 * relations that have expression indexes.
 *
 * Similarly, partial indexes may also cause false dependencies due to predicate
 * expressions. For the same reason, we consider this acceptable as well.
 *
 * To avoid redundant dependency tracking, indexes whose key columns are the
 * same as, or a superset of, the replica identity key columns are skipped,
 * since tracking the replica identity keys already covers their scope.
 *
 * Columns not in the replica identity key are excluded from the unique column
 * set. Since the old tuple of an UPDATE or DELETE contains only replica
 * identity key columns, any other columns would be missing and thus unavailable
 * for dependency tracking.
 */
static void
collect_indexes_for_dependency_tracking(LogicalRepRelMapEntry *entry)
{
	List	   *idxlist;

	free_local_unique_indexes(entry);

	/*
	 * XXX For partitioned tables, we must collect unique indexes from leaf
	 * partitions, which are the actual replication targets. This is because
	 * leaf partitions can have unique indexes that are not present on the
	 * partitioned table, and those indexes can be used for dependency tracking.
	 * However, collecting unique indexes from leaf partitions requires building
	 * the tuple, and executing partition pruning expressions, which could be
	 * expensive for each change on a partitioned table. For now, we skip
	 * collecting local unique indexes for partitioned tables and create a dummy
	 * entry, ensuring that changes on partitioned tables are not applied in
	 * parallel.
	 */
	if (entry->localrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		MemoryContext oldctx;
		LogicalRepSubscriberIdx *indexinfo;

		oldctx = MemoryContextSwitchTo(LogicalRepRelMapContext);
		indexinfo = palloc(sizeof(LogicalRepSubscriberIdx));
		indexinfo->indexoid = InvalidOid;
		indexinfo->indexkeys = NULL;
		indexinfo->nulls_distinct = false;
		entry->local_unique_indexes = lappend(entry->local_unique_indexes,
											  indexinfo);
		MemoryContextSwitchTo(oldctx);

		entry->local_unique_indexes_collected = true;

		return;
	}

	idxlist = RelationGetIndexList(entry->localrel);

	/* Iterate indexes to list all usable indexes */
	foreach_oid(idxoid, idxlist)
	{
		Relation	idxrel;
		int			indnkeys;
		AttrMap    *attrmap;
		MemoryContext oldctx;
		LogicalRepSubscriberIdx *indexinfo;
		Bitmapset  *indexkeys = NULL;
		bool		nulls_distinct;

		idxrel = index_open(idxoid, AccessShareLock);

		/* Only unique indexes are considered */
		if (!idxrel->rd_index->indisunique)
		{
			index_close(idxrel, AccessShareLock);
			continue;
		}

		indnkeys = idxrel->rd_index->indnkeyatts;
		nulls_distinct = !idxrel->rd_index->indnullsnotdistinct;
		attrmap = entry->attrmap;

		Assert(indnkeys);

		/* Seek each attributes and add to a Bitmap */
		for (int i = 0; i < indnkeys; i++)
		{
			AttrNumber	localcol = idxrel->rd_index->indkey.values[i];
			AttrNumber	remotecol;

			/* Skip expression */
			if (!AttributeNumberIsValid(localcol))
				continue;

			remotecol = attrmap->attnums[AttrNumberGetAttrOffset(localcol)];

			/* Skip if the column does not exist on publisher node */
			if (remotecol < 0)
				continue;

			/* Skip columns that are not part of the replica identity key */
			if (!bms_is_member(remotecol, entry->remoterel.attkeys))
				continue;

			/* Checks are passed, remember the attribute */
			indexkeys = bms_add_member(indexkeys, remotecol);
		}

		index_close(idxrel, AccessShareLock);

		/*
		 * Skip indexes whose key columns are a superset of the replica identity
		 * key.
		 */
		if (bms_equal(entry->remoterel.attkeys, indexkeys) ||
			bms_is_subset(entry->remoterel.attkeys, indexkeys))
		{
			bms_free(indexkeys);
			continue;
		}

		oldctx = MemoryContextSwitchTo(LogicalRepRelMapContext);
		indexinfo = palloc(sizeof(LogicalRepSubscriberIdx));
		indexinfo->indexoid = idxoid;
		indexinfo->indexkeys = bms_copy(indexkeys);
		indexinfo->nulls_distinct = nulls_distinct;
		entry->local_unique_indexes = lappend(entry->local_unique_indexes,
											  indexinfo);
		MemoryContextSwitchTo(oldctx);

		bms_free(indexkeys);
	}

	list_free(idxlist);

	entry->local_unique_indexes_collected = true;
}

/*
 * Search a relmap entry by local relation OID.
 */
static LogicalRepRelMapEntry *
logicalrep_get_relentry_by_local_oid(Oid localreloid)
{
	HASH_SEQ_STATUS status;
	LogicalRepRelMapEntry *entry = NULL;

	if (LogicalRepRelMap == NULL)
		return NULL;

	hash_seq_init(&status, LogicalRepRelMap);
	while ((entry = (LogicalRepRelMapEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->localreloid == localreloid)
		{
			hash_seq_term(&status);
			return entry;
		}
	}

	return NULL;
}

/*
 * Return remote relation IDs that have FK dependency metadata tied to the
 * specified relation, either as referencing-side or referenced-side entries.
 */
List *
logicalrep_get_fk_related_relids(LogicalRepRelation *remoterel)
{
	LogicalRepRelMapEntry *target;
	bool		found;
	List	   *related = NIL;
	Oid		relnamespace;
	Oid			remoteid = remoterel->remoteid;

	if (LogicalRepRelMap == NULL)
		return NIL;

	target = hash_search(LogicalRepRelMap, &remoteid, HASH_FIND, &found);

	/*
	 * If not exists yet, build a new entry so that we can collect all the
	 * referenced and referencing tables.
	 */
	if (!found)
	{
		logicalrep_relmap_update(remoterel);
		target = hash_search(LogicalRepRelMap, &remoteid, HASH_FIND, &found);
	}

	Assert(found);

	/* Collect the tables if not yet */
	if (!target->local_fkeys_collected)
	{
		bool		needs_start = !IsTransactionOrTransactionBlock();

		if (needs_start)
			StartTransactionCommand();

		/* Return if the relation does not exist */
		relnamespace = get_namespace_oid(remoterel->nspname, true);
		if (!OidIsValid(get_relname_relid(remoterel->relname, relnamespace)))
		{
			if (needs_start)
				CommitTransactionCommand();

			return NIL;
		}

		logicalrep_rel_load(NULL, remoteid, AccessShareLock);

		if (needs_start)
			CommitTransactionCommand();
	}

	Assert(target->local_fkeys_collected);

	/* Collect all the tables referenced by the given table */
	foreach_ptr(LogicalRepSubscriberFK, fkinfo, target->local_fkeys)
		related = list_append_unique_oid(related, fkinfo->ref_remoteid);

	/* Collect all the tables referencing the given table */
	foreach_ptr(LogicalRepSubscriberRefFK, refinfo, target->local_referenced_fkeys)
		related = list_append_unique_oid(related, refinfo->fk_remoteid);

	/* Remove the given table itself from the list */
	related = list_delete_oid(related, remoteid);

	return related;
}

/*
 * Return true if the FK constraint is always deferred.
 */
static bool
foreign_key_is_always_deferred(Oid conoid)
{
	HeapTuple	tup;
	Form_pg_constraint con;
	bool		deferred;

	tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(conoid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for foreign key %u", conoid);

	con = (Form_pg_constraint) GETSTRUCT(tup);
	deferred = con->condeferrable && con->condeferred;
	ReleaseSysCache(tup);

	return deferred;
}

/*
 * Check whether the table has any referencing-side foreign key triggers (ON
 * INSERT or ON UPDATE) enabled in replica mode that could cause a foreign key
 * violation error.
 */
static bool
fkey_trigger_enabled_in_replica(Relation rel, Oid conoid)
{
	TriggerDesc *trigdesc = rel->trigdesc;

	if (trigdesc == NULL)
		return false;

	for (int i = 0; i < trigdesc->numtriggers; i++)
	{
		Trigger    *trig = &trigdesc->triggers[i];

		/* Keep only FK-side check triggers that belong to this FK constraint */
		if (trig->tgconstraint != conoid ||
			(trig->tgfoid != F_RI_FKEY_CHECK_INS &&
			 trig->tgfoid != F_RI_FKEY_CHECK_UPD))
			continue;

		/* In replica mode, only REPLICA/ALWAYS triggers can fire */
		if (trig->tgenabled == TRIGGER_FIRES_ON_REPLICA ||
			trig->tgenabled == TRIGGER_FIRES_ALWAYS)
			return true;
	}

	return false;
}

/*
 * Check whether the table has any referenced-side foreign key triggers (ON
 * DELETE or ON UPDATE) enabled in replica mode that could cause a foreign key
 * violation error.
 */
static bool
refkey_trigger_enabled_in_replica(Relation rel, Oid conoid)
{
	TriggerDesc *trigdesc = rel->trigdesc;

	if (trigdesc == NULL)
		return false;

	for (int i = 0; i < trigdesc->numtriggers; i++)
	{
		Trigger    *trig = &trigdesc->triggers[i];

		/* Keep only PK-side action triggers for this FK constraint */
		if (trig->tgconstraint != conoid ||
			(trig->tgfoid != F_RI_FKEY_NOACTION_DEL &&
			 trig->tgfoid != F_RI_FKEY_NOACTION_UPD &&
			 trig->tgfoid != F_RI_FKEY_RESTRICT_DEL &&
			 trig->tgfoid != F_RI_FKEY_RESTRICT_UPD))
			continue;

		/* In replica mode, only REPLICA/ALWAYS triggers can fire. */
		if (trig->tgenabled == TRIGGER_FIRES_ON_REPLICA ||
			trig->tgenabled == TRIGGER_FIRES_ALWAYS)
			return true;
	}

	return false;
}

/*
 * Build a list of foreign key remote columns in the order of the referenced
 * table's remote columns.
 */
static void
build_fk_remote_attnums(LogicalRepRelMapEntry *fkentry,
						LogicalRepRelMapEntry *refentry,
						int nkeys,
						const AttrNumber *fk_conkey,
						const AttrNumber *ref_confkey,
						List **fkattnums,
						List **fkattnums_old,
						List **refattnums,
						List **refattnums_old)
{
	int			pkatt = -1;
	List	   *fkcols = NIL;
	List	   *fkcols_old = NIL;
	List	   *refcols = NIL;
	List	   *refcols_old = NIL;

	/*
	 * Traverse the referenced table's remote replica identity columns in order,
	 * and for each, find the corresponding foreign key column that matches it.
	 */
	while ((pkatt = bms_next_member(refentry->remoterel.attkeys, pkatt)) >= 0)
	{
		for (int i = 0; i < nkeys; i++)
		{
			AttrNumber	fk_local_attnum = fk_conkey[i];
			AttrNumber	ref_local_attnum = ref_confkey[i];
			int			fk_remote_attnum;
			int			ref_remote_attnum;

			fk_remote_attnum = fkentry->attrmap->attnums[AttrNumberGetAttrOffset(fk_local_attnum)];
			ref_remote_attnum = refentry->attrmap->attnums[AttrNumberGetAttrOffset(ref_local_attnum)];

			/* Skip if not the current traversed column */
			if (ref_remote_attnum != pkatt)
				continue;

			/* Skip columns that are unavailable in the remote table */
			if (ref_remote_attnum < 0 || fk_remote_attnum < 0)
				continue;

			fkcols = lappend_int(fkcols, fk_remote_attnum + 1);
			refcols = lappend_int(refcols, ref_remote_attnum + 1);

			/* Old tuple contains only replica identity columns. */
			if (bms_is_member(fk_remote_attnum, fkentry->remoterel.attkeys) &&
				bms_is_member(ref_remote_attnum, refentry->remoterel.attkeys))
			{
				fkcols_old = lappend_int(fkcols_old, fk_remote_attnum + 1);
				refcols_old = lappend_int(refcols_old, ref_remote_attnum + 1);
			}

			break;
		}
	}

	*fkattnums = fkcols;
	*fkattnums_old = fkcols_old;
	*refattnums = refcols;
	*refattnums_old = refcols_old;
}

/*
 * Collect referenced-side key projections for dependency tracking
 *
 * Helper for collect_fkeys_for_dependency_tracking(). See that function for
 * detailed comments.
 */
static void
collect_refkeys_for_dependency_tracking(LogicalRepRelMapEntry *entry)
{
	Relation	fkeyRel;
	SysScanDesc fkeyScan;
	HeapTuple	tuple;
	Oid			relid = RelationGetRelid(entry->localrel);

	Assert(OidIsValid(relid));

	fkeyRel = table_open(ConstraintRelationId, AccessShareLock);

	fkeyScan = systable_beginscan(fkeyRel, InvalidOid, false,
								  NULL, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(fkeyScan)))
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(tuple);
		LogicalRepRelMapEntry *fkentry;
		LogicalRepSubscriberRefFK *refinfo;
		List	   *fkattnums;
		List	   *fkattnums_old;
		List	   *refattnums;
		List	   *refattnums_old;
		AttrNumber	conkey[INDEX_MAX_KEYS] = {0};
		AttrNumber	confkey[INDEX_MAX_KEYS] = {0};
		int			numfks;
		MemoryContext oldctx;

		/* Not a foreign key */
		if (con->contype != CONSTRAINT_FOREIGN)
			continue;

		/* Not referencing the given table */
		if (con->confrelid != relid)
			continue;

		/* Skip if FK enforcement is disabled */
		if (!con->conenforced)
			continue;

		/* Always-deferred FK does not need dependency tracking. */
		if (foreign_key_is_always_deferred(con->oid))
			continue;

		/*
		 * Skip when no replica-mode ON DELETE/ON UPDATE violation trigger can
		 * fire for this FK on the referenced table.
		 */
		if (!refkey_trigger_enabled_in_replica(entry->localrel, con->oid))
			continue;

		fkentry = logicalrep_get_relentry_by_local_oid(con->conrelid);

		/*
		 * Skip if the referencing table is not published or has not replicated
		 * any changes.
		 */
		if (!fkentry || !fkentry->attrmap)
			continue;

		DeconstructFkConstraintRow(tuple, &numfks, conkey, confkey,
								   NULL, NULL, NULL, NULL, NULL);

		build_fk_remote_attnums(fkentry, entry, numfks, conkey, confkey,
								&fkattnums, &fkattnums_old,
								&refattnums, &refattnums_old);

		list_free(fkattnums);
		list_free(fkattnums_old);

		oldctx = MemoryContextSwitchTo(LogicalRepRelMapContext);
		refinfo = palloc_object(LogicalRepSubscriberRefFK);
		refinfo->conoid = con->oid;
		refinfo->fk_remoteid = fkentry->remoterel.remoteid;
		refinfo->refattnums = list_copy(refattnums);
		refinfo->refattnums_old = list_copy(refattnums_old);
		entry->local_referenced_fkeys = lappend(entry->local_referenced_fkeys,
												refinfo);
		MemoryContextSwitchTo(oldctx);

		list_free(refattnums);
		list_free(refattnums_old);
	}

	systable_endscan(fkeyScan);

	table_close(fkeyRel, AccessShareLock);
}

/*
 * Collect foreign keys for dependency tracking.
 *
 * This function collects both foreign keys in the referencing table and the
 * primary key in the referenced table, as both are needed for dependency
 * tracking (see applyparallelworker.c for details).
 *
 * For a primary key, we directly record the bitmap of remote columns that the
 * foreign key references.
 *
 * For a foreign key in the referencing table, we cannot use the columns as-is
 * because their order on the remote side may differ from the local referencing
 * and referenced table. To ensure consistency, we store a list of column
 * numbers in the order of the referenced table's remote columns. The dependency
 * tracking function will traverse this list to build the hash key.
 *
 * For one foreign key constraint on the table, We collect two set of column
 * numbers for both the referencing and referenced tables: one for the new tuple
 * of an INSERT or UPDATE, and the other for the old tuple of an UPDATE or
 * DELETE. The former includes all remote columns that the foreign key
 * references, while the latter includes only those that are part of the replica
 * identity key. This is because the old tuple of an UPDATE or DELETE contains
 * only replica identity key columns, and any other columns would be missing and
 * thus unavailable for dependency tracking.
 *
 * If there are multiple foreign keys referencing other tables or if the primary
 * key is referenced by multiple foreign keys, we will have multiple sets of
 * column numbers.
 *
 * When recording or checking a foreign key dependency, we fill columns in the
 * referencing table that are outside the replica identity as NULL, and during
 * comparison, we treat NULL as equal to any value. This is safe because the
 * referenced key is a primary key (no NULLs allowed), and NULL values in the
 * referencing key never participate in foreign key constraint checks.
 * Therefore, we will not encounter genuine NULL values in the remote columns.
 */
static void
collect_fkeys_for_dependency_tracking(LogicalRepRelMapEntry *entry)
{
	List	   *fkeys;
	MemoryContext oldctx;

	if (entry->local_fkeys != NIL || entry->local_referenced_fkeys != NIL)
		free_local_fkeys(entry);

	fkeys = copyObject(RelationGetFKeyList(entry->localrel));

	/* Collect foreign keys where this table is the referencing side */
	foreach_ptr(ForeignKeyCacheInfo, fk, fkeys)
	{
		LogicalRepRelMapEntry *refentry;
		List	  *fkattnums;
		List	  *fkattnums_old;
		List	  *refattnums;
		List	  *refattnums_old;
		LogicalRepSubscriberFK *fkinfo;

		/* Skip if FK enforcement is disabled */
		if (!fk->conenforced)
			continue;

		/*
		 * Skip if this FK's check trigger is disabled in replica mode.
		 */
		if (!fkey_trigger_enabled_in_replica(entry->localrel, fk->conoid))
			continue;

		/*
		 * Deferrable foreign keys do not need tracking, as they won't cause
		 * constraint violations as long as commit order is preserved.
		 */
		if (foreign_key_is_always_deferred(fk->conoid))
			continue;

		refentry = logicalrep_get_relentry_by_local_oid(fk->confrelid);

		/*
		 * Skip if the referenced table is not published or has not replicated
		 * any changes.
		 */
		if (!refentry || !refentry->attrmap)
			continue;

		build_fk_remote_attnums(entry, refentry, fk->nkeys, fk->conkey,
								fk->confkey, &fkattnums, &fkattnums_old,
								&refattnums, &refattnums_old);

		list_free(refattnums);
		list_free(refattnums_old);

		oldctx = MemoryContextSwitchTo(LogicalRepRelMapContext);
		fkinfo = palloc_object(LogicalRepSubscriberFK);
		fkinfo->conoid = fk->conoid;
		fkinfo->ref_remoteid = refentry->remoterel.remoteid;
		fkinfo->fkattnums = list_copy(fkattnums);
		fkinfo->fkattnums_old = list_copy(fkattnums_old);
		entry->local_fkeys = lappend(entry->local_fkeys, fkinfo);
		MemoryContextSwitchTo(oldctx);

		list_free(fkattnums);
		list_free(fkattnums_old);
	}

	list_free_deep(fkeys);

	/* Collect keys where this table is the referenced side */
	collect_refkeys_for_dependency_tracking(entry);

	entry->local_fkeys_collected = true;
}

/*
 * Check all local triggers for the relation to see the parallelizability.
 *
 * We regard relations as applicable in parallel if all triggers are immutable.
 * Result is directly set to LogicalRepRelMapEntry::parallel_safe.
 */
static void
check_defined_triggers(LogicalRepRelMapEntry *entry)
{
	TriggerDesc *trigdesc;

	/*
	 * Skip if the parallelizability has already been checked. Possilble if
	 * the relation has expression indexes.
	 */
	if (entry->parallel_safe != LOGICALREP_PARALLEL_UNKNOWN)
		return;

	trigdesc = entry->localrel->trigdesc;

	/* Quick exit if triffer is not defined */
	if (trigdesc == NULL)
	{
		entry->parallel_safe = LOGICALREP_PARALLEL_SAFE;
		return;
	}

	/* Seek triggers one by one to see the volatility */
	for (int i = 0; i < trigdesc->numtriggers; i++)
	{
		Trigger    *trigger = &trigdesc->triggers[i];

		Assert(OidIsValid(trigger->tgfoid));

		/* Skip if the trigger is not enabled for logical replication */
		if (trigger->tgenabled == TRIGGER_DISABLED ||
			trigger->tgenabled == TRIGGER_FIRES_ON_ORIGIN)
			continue;

		/* Check the volatility of the trigger. Exit if it is not immutable */
		if (func_volatile(trigger->tgfoid) != PROVOLATILE_IMMUTABLE)
		{
			entry->parallel_safe = LOGICALREP_PARALLEL_RESTRICTED;
			return;
		}
	}

	/* All triggers are immutable, set as parallel safe */
	entry->parallel_safe = LOGICALREP_PARALLEL_SAFE;
}

/*
 * Actual workhorse for logicalrep_rel_open().
 *
 * Caller must specify *either* entry or key. If the entry is specified, its
 * attributes are filled and returned. The logical relation is kept opening.
 * If the key is given, the corresponding entry is first searched in the hash
 * table and processed as in the above case. At the end, logical replication is
 * closed.
 */
void
logicalrep_rel_load(LogicalRepRelMapEntry *entry, LogicalRepRelId remoteid,
					LOCKMODE lockmode)
{
	LogicalRepRelation *remoterel;

	Assert((entry && !remoteid) || (!entry && remoteid));

	if (!entry)
	{
		bool		found;

		if (LogicalRepRelMap == NULL)
			logicalrep_relmap_init();

		/* Search for existing entry. */
		entry = hash_search(LogicalRepRelMap, &remoteid,
							HASH_FIND, &found);

		if (!found)
			elog(ERROR, "no relation map entry for remote relation ID %u",
				 remoteid);
	}

	remoterel = &entry->remoterel;

	/* Ensure we don't leak a relcache refcount. */
	if (entry->localrel)
		elog(ERROR, "remote relation ID %u is already open", remoteid);

	/*
	 * When opening and locking a relation, pending invalidation messages are
	 * processed which can invalidate the relation.  Hence, if the entry is
	 * currently considered valid, try to open the local relation by OID and
	 * see if invalidation ensues.
	 */
	if (entry->localrelvalid)
	{
		entry->localrel = try_table_open(entry->localreloid, lockmode);
		if (!entry->localrel)
		{
			/* Table was renamed or dropped. */
			entry->localrelvalid = false;
			entry->local_unique_indexes_collected = false;
			entry->local_fkeys_collected = false;
		}
		else if (!entry->localrelvalid)
		{
			/* Note we release the no-longer-useful lock here. */
			table_close(entry->localrel, lockmode);
			entry->localrel = NULL;
		}
	}

	/*
	 * If the entry has been marked invalid since we last had lock on it,
	 * re-open the local relation by name and rebuild all derived data.
	 */
	if (!entry->localrelvalid)
	{
		Oid			relid;
		TupleDesc	desc;
		MemoryContext oldctx;
		int			i;
		Bitmapset  *missingatts;
		Bitmapset  *generatedattrs = NULL;

		/* Release the no-longer-useful attrmap, if any. */
		if (entry->attrmap)
		{
			free_attrmap(entry->attrmap);
			entry->attrmap = NULL;
		}

		/* Try to find and lock the relation by name. */
		relid = RangeVarGetRelid(makeRangeVar(remoterel->nspname,
											  remoterel->relname, -1),
								 lockmode, true);
		if (!OidIsValid(relid))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("logical replication target relation \"%s.%s\" does not exist",
							remoterel->nspname, remoterel->relname)));
		entry->localrel = table_open(relid, NoLock);
		entry->localreloid = relid;

		/* Check for supported relkind. */
		CheckSubscriptionRelkind(entry->localrel->rd_rel->relkind,
								 remoterel->relkind,
								 remoterel->nspname, remoterel->relname);

		/*
		 * Build the mapping of local attribute numbers to remote attribute
		 * numbers and validate that we don't miss any replicated columns as
		 * that would result in potentially unwanted data loss.
		 */
		desc = RelationGetDescr(entry->localrel);
		oldctx = MemoryContextSwitchTo(LogicalRepRelMapContext);
		entry->attrmap = make_attrmap(desc->natts);
		MemoryContextSwitchTo(oldctx);

		/* check and report missing attrs, if any */
		missingatts = bms_add_range(NULL, 0, remoterel->natts - 1);
		for (i = 0; i < desc->natts; i++)
		{
			int			attnum;
			Form_pg_attribute attr = TupleDescAttr(desc, i);

			if (attr->attisdropped)
			{
				entry->attrmap->attnums[i] = -1;
				continue;
			}

			attnum = logicalrep_rel_att_by_name(remoterel,
												NameStr(attr->attname));

			entry->attrmap->attnums[i] = attnum;
			if (attnum >= 0)
			{
				/* Remember which subscriber columns are generated. */
				if (attr->attgenerated)
					generatedattrs = bms_add_member(generatedattrs, attnum);

				missingatts = bms_del_member(missingatts, attnum);
			}
		}

		logicalrep_report_missing_or_gen_attrs(remoterel, missingatts,
											   generatedattrs);

		/* be tidy */
		bms_free(generatedattrs);
		bms_free(missingatts);

		/*
		 * Set if the table's replica identity is enough to apply
		 * update/delete.
		 */
		logicalrep_rel_mark_updatable(entry);

		/*
		 * Finding a usable index is an infrequent task. It occurs when an
		 * operation is first performed on the relation, or after invalidation
		 * of the relation cache entry (such as ANALYZE or CREATE/DROP index
		 * on the relation).
		 */
		entry->localindexoid = FindLogicalRepLocalIndex(entry->localrel, remoterel,
														entry->attrmap);

		/*
		 * Leader must also collect all local unique indexes for dependency
		 * tracking.
		 */
		if (am_leader_apply_worker())
		{
			entry->parallel_safe = LOGICALREP_PARALLEL_UNKNOWN;
			collect_indexes_for_dependency_tracking(entry);
			check_defined_triggers(entry);
		}

		entry->localrelvalid = true;
	}

	if (am_leader_apply_worker() && !entry->local_fkeys_collected)
		collect_fkeys_for_dependency_tracking(entry);

	if (entry->state != SUBREL_STATE_READY)
		entry->state = GetSubscriptionRelState(MySubscription->oid,
											   entry->localreloid,
											   &entry->statelsn);

	if (remoteid)
		logicalrep_rel_close(entry, lockmode);
}

/*
 * Open the local relation associated with the remote one.
 *
 * Rebuilds the Relcache mapping if it was invalidated by local DDL.
 */
LogicalRepRelMapEntry *
logicalrep_rel_open(LogicalRepRelId remoteid, LOCKMODE lockmode)
{
	LogicalRepRelMapEntry *entry;
	bool		found;

	if (LogicalRepRelMap == NULL)
		logicalrep_relmap_init();

	/* Search for existing entry. */
	entry = hash_search(LogicalRepRelMap, &remoteid,
						HASH_FIND, &found);

	if (!found)
		elog(ERROR, "no relation map entry for remote relation ID %u",
			 remoteid);

	logicalrep_rel_load(entry, 0, lockmode);

	return entry;
}

/*
 * Close the previously opened logical relation.
 */
void
logicalrep_rel_close(LogicalRepRelMapEntry *rel, LOCKMODE lockmode)
{
	table_close(rel->localrel, lockmode);
	rel->localrel = NULL;
}

/*
 * Partition cache: look up partition LogicalRepRelMapEntry's
 *
 * Unlike relation map cache, this is keyed by partition OID, not remote
 * relation OID, because we only have to use this cache in the case where
 * partitions are not directly mapped to any remote relation, such as when
 * replication is occurring with one of their ancestors as target.
 */

/*
 * Relcache invalidation callback
 */
static void
logicalrep_partmap_invalidate_cb(Datum arg, Oid reloid)
{
	LogicalRepPartMapEntry *entry;

	/* Just to be sure. */
	if (LogicalRepPartMap == NULL)
		return;

	if (reloid != InvalidOid)
	{
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, LogicalRepPartMap);

		/* TODO, use inverse lookup hashtable? */
		while ((entry = (LogicalRepPartMapEntry *) hash_seq_search(&status)) != NULL)
		{
			if (entry->relmapentry.localreloid == reloid)
			{
				entry->relmapentry.localrelvalid = false;
				hash_seq_term(&status);
				break;
			}
		}
	}
	else
	{
		/* invalidate all cache entries */
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, LogicalRepPartMap);

		while ((entry = (LogicalRepPartMapEntry *) hash_seq_search(&status)) != NULL)
			entry->relmapentry.localrelvalid = false;
	}
}

/*
 * Reset the entries in the partition map that refer to remoterel.
 *
 * Called when new relation mapping is sent by the publisher to update our
 * expected view of incoming data from said publisher.
 *
 * Note that we don't update the remoterel information in the entry here,
 * we will update the information in logicalrep_partition_open to avoid
 * unnecessary work.
 */
void
logicalrep_partmap_reset_relmap(LogicalRepRelation *remoterel)
{
	HASH_SEQ_STATUS status;
	LogicalRepPartMapEntry *part_entry;
	LogicalRepRelMapEntry *entry;

	if (LogicalRepPartMap == NULL)
		return;

	hash_seq_init(&status, LogicalRepPartMap);
	while ((part_entry = (LogicalRepPartMapEntry *) hash_seq_search(&status)) != NULL)
	{
		entry = &part_entry->relmapentry;

		if (entry->remoterel.remoteid != remoterel->remoteid)
			continue;

		logicalrep_relmap_free_entry(entry);

		memset(entry, 0, sizeof(LogicalRepRelMapEntry));
	}
}

/*
 * Initialize the partition map cache.
 */
static void
logicalrep_partmap_init(void)
{
	HASHCTL		ctl;

	if (!LogicalRepPartMapContext)
		LogicalRepPartMapContext =
			AllocSetContextCreate(CacheMemoryContext,
								  "LogicalRepPartMapContext",
								  ALLOCSET_DEFAULT_SIZES);

	/* Initialize the relation hash table. */
	ctl.keysize = sizeof(Oid);	/* partition OID */
	ctl.entrysize = sizeof(LogicalRepPartMapEntry);
	ctl.hcxt = LogicalRepPartMapContext;

	LogicalRepPartMap = hash_create("logicalrep partition map cache", 64, &ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* Watch for invalidation events. */
	CacheRegisterRelcacheCallback(logicalrep_partmap_invalidate_cb,
								  (Datum) 0);
}

/*
 * logicalrep_partition_open
 *
 * Returned entry reuses most of the values of the root table's entry, save
 * the attribute map, which can be different for the partition.  However,
 * we must physically copy all the data, in case the root table's entry
 * gets freed/rebuilt.
 *
 * Note there's no logicalrep_partition_close, because the caller closes the
 * component relation.
 */
LogicalRepRelMapEntry *
logicalrep_partition_open(LogicalRepRelMapEntry *root,
						  Relation partrel, AttrMap *map)
{
	LogicalRepRelMapEntry *entry;
	LogicalRepPartMapEntry *part_entry;
	LogicalRepRelation *remoterel = &root->remoterel;
	Oid			partOid = RelationGetRelid(partrel);
	AttrMap    *attrmap = root->attrmap;
	bool		found;
	MemoryContext oldctx;

	if (LogicalRepPartMap == NULL)
		logicalrep_partmap_init();

	/* Search for existing entry. */
	part_entry = (LogicalRepPartMapEntry *) hash_search(LogicalRepPartMap,
														&partOid,
														HASH_ENTER, &found);

	entry = &part_entry->relmapentry;

	/*
	 * We must always overwrite entry->localrel with the latest partition
	 * Relation pointer, because the Relation pointed to by the old value may
	 * have been cleared after the caller would have closed the partition
	 * relation after the last use of this entry.  Note that localrelvalid is
	 * only updated by the relcache invalidation callback, so it may still be
	 * true irrespective of whether the Relation pointed to by localrel has
	 * been cleared or not.
	 */
	if (found && entry->localrelvalid)
	{
		entry->localrel = partrel;
		return entry;
	}

	/* Switch to longer-lived context. */
	oldctx = MemoryContextSwitchTo(LogicalRepPartMapContext);

	if (!found)
	{
		memset(part_entry, 0, sizeof(LogicalRepPartMapEntry));
		part_entry->partoid = partOid;
	}

	/* Release the no-longer-useful attrmap, if any. */
	if (entry->attrmap)
	{
		free_attrmap(entry->attrmap);
		entry->attrmap = NULL;
	}

	if (!entry->remoterel.remoteid)
	{
		int			i;

		/* Remote relation is copied as-is from the root entry. */
		entry->remoterel.remoteid = remoterel->remoteid;
		entry->remoterel.nspname = pstrdup(remoterel->nspname);
		entry->remoterel.relname = pstrdup(remoterel->relname);
		entry->remoterel.natts = remoterel->natts;
		entry->remoterel.attnames = palloc_array(char *, remoterel->natts);
		entry->remoterel.atttyps = palloc_array(Oid, remoterel->natts);
		for (i = 0; i < remoterel->natts; i++)
		{
			entry->remoterel.attnames[i] = pstrdup(remoterel->attnames[i]);
			entry->remoterel.atttyps[i] = remoterel->atttyps[i];
		}
		entry->remoterel.replident = remoterel->replident;
		entry->remoterel.attkeys = bms_copy(remoterel->attkeys);
	}

	entry->localrel = partrel;
	entry->localreloid = partOid;

	/*
	 * If the partition's attributes don't match the root relation's, we'll
	 * need to make a new attrmap which maps partition attribute numbers to
	 * remoterel's, instead of the original which maps root relation's
	 * attribute numbers to remoterel's.
	 *
	 * Note that 'map' which comes from the tuple routing data structure
	 * contains 1-based attribute numbers (of the parent relation).  However,
	 * the map in 'entry', a logical replication data structure, contains
	 * 0-based attribute numbers (of the remote relation).
	 */
	if (map)
	{
		AttrNumber	attno;

		entry->attrmap = make_attrmap(map->maplen);
		for (attno = 0; attno < entry->attrmap->maplen; attno++)
		{
			AttrNumber	root_attno = map->attnums[attno];

			/* 0 means it's a dropped attribute.  See comments atop AttrMap. */
			if (root_attno == 0)
				entry->attrmap->attnums[attno] = -1;
			else
				entry->attrmap->attnums[attno] = attrmap->attnums[root_attno - 1];
		}
	}
	else
	{
		/* Lacking copy_attmap, do this the hard way. */
		entry->attrmap = make_attrmap(attrmap->maplen);
		memcpy(entry->attrmap->attnums, attrmap->attnums,
			   attrmap->maplen * sizeof(AttrNumber));
	}

	/* Set if the table's replica identity is enough to apply update/delete. */
	logicalrep_rel_mark_updatable(entry);

	/* state and statelsn are left set to 0. */
	MemoryContextSwitchTo(oldctx);

	/*
	 * Finding a usable index is an infrequent task. It occurs when an
	 * operation is first performed on the relation, or after invalidation of
	 * the relation cache entry (such as ANALYZE or CREATE/DROP index on the
	 * relation).
	 *
	 * We also prefer to run this code on the oldctx so that we do not leak
	 * anything in the LogicalRepPartMapContext (hence CacheMemoryContext).
	 */
	entry->localindexoid = FindLogicalRepLocalIndex(partrel, remoterel,
													entry->attrmap);

	/*
	 * TODO: Parallel apply cannot collect indexes from leaf partition for now.
	 * Just mark local indexes are collected. (See
	 * collect_indexes_for_dependency_tracking() for details.)
	 */
	entry->local_unique_indexes_collected = true;
	entry->local_fkeys_collected = true;

	entry->localrelvalid = true;

	return entry;
}

/*
 * Returns the oid of an index that can be used by the apply worker to scan
 * the relation.
 *
 * We expect to call this function when REPLICA IDENTITY FULL is defined for
 * the remote relation.
 *
 * If no suitable index is found, returns InvalidOid.
 */
static Oid
FindUsableIndexForReplicaIdentityFull(Relation localrel, AttrMap *attrmap)
{
	List	   *idxlist = RelationGetIndexList(localrel);

	foreach_oid(idxoid, idxlist)
	{
		bool		isUsableIdx;
		Relation	idxRel;

		idxRel = index_open(idxoid, AccessShareLock);
		isUsableIdx = IsIndexUsableForReplicaIdentityFull(idxRel, attrmap);
		index_close(idxRel, AccessShareLock);

		/* Return the first eligible index found */
		if (isUsableIdx)
			return idxoid;
	}

	return InvalidOid;
}

/*
 * Returns true if the index is usable for replica identity full.
 *
 * The index must have an equal strategy for each key column, be non-partial,
 * and the leftmost field must be a column (not an expression) that references
 * the remote relation column. These limitations help to keep the index scan
 * similar to PK/RI index scans.
 *
 * attrmap is a map of local attributes to remote ones. We can consult this
 * map to check whether the local index attribute has a corresponding remote
 * attribute.
 *
 * Note that the limitations of index scans for replica identity full only
 * adheres to a subset of the limitations of PK/RI. For example, we support
 * columns that are marked as [NULL] or we are not interested in the [NOT
 * DEFERRABLE] aspect of constraints here. It works for us because we always
 * compare the tuples for non-PK/RI index scans. See
 * RelationFindReplTupleByIndex().
 *
 * XXX: To support partial indexes, the required changes are likely to be larger.
 * If none of the tuples satisfy the expression for the index scan, we fall-back
 * to sequential execution, which might not be a good idea in some cases.
 */
bool
IsIndexUsableForReplicaIdentityFull(Relation idxrel, AttrMap *attrmap)
{
	AttrNumber	keycol;
	oidvector  *indclass;

	/* The index must not be a partial index */
	if (!heap_attisnull(idxrel->rd_indextuple, Anum_pg_index_indpred, NULL))
		return false;

	Assert(idxrel->rd_index->indnatts >= 1);

	indclass = (oidvector *) DatumGetPointer(SysCacheGetAttrNotNull(INDEXRELID,
																	idxrel->rd_indextuple,
																	Anum_pg_index_indclass));

	/* Ensure that the index has a valid equal strategy for each key column */
	for (int i = 0; i < idxrel->rd_index->indnkeyatts; i++)
	{
		Oid			opfamily;

		opfamily = get_opclass_family(indclass->values[i]);
		if (IndexAmTranslateCompareType(COMPARE_EQ, idxrel->rd_rel->relam, opfamily, true) == InvalidStrategy)
			return false;
	}

	/*
	 * For indexes other than PK and REPLICA IDENTITY, we need to match the
	 * local and remote tuples.  The equality routine tuples_equal() cannot
	 * accept a data type where the type cache cannot provide an equality
	 * operator.
	 */
	for (int i = 0; i < idxrel->rd_att->natts; i++)
	{
		TypeCacheEntry *typentry;

		typentry = lookup_type_cache(TupleDescAttr(idxrel->rd_att, i)->atttypid, TYPECACHE_EQ_OPR_FINFO);
		if (!OidIsValid(typentry->eq_opr_finfo.fn_oid))
			return false;
	}

	/* The leftmost index field must not be an expression */
	keycol = idxrel->rd_index->indkey.values[0];
	if (!AttributeNumberIsValid(keycol))
		return false;

	/*
	 * And the leftmost index field must reference the remote relation column.
	 * This is because if it doesn't, the sequential scan is favorable over
	 * index scan in most cases.
	 */
	if (attrmap->maplen <= AttrNumberGetAttrOffset(keycol) ||
		attrmap->attnums[AttrNumberGetAttrOffset(keycol)] < 0)
		return false;

	/*
	 * The given index access method must implement "amgettuple", which will
	 * be used later to fetch the tuples.  See RelationFindReplTupleByIndex().
	 */
	if (GetIndexAmRoutineByAmId(idxrel->rd_rel->relam, false)->amgettuple == NULL)
		return false;

	return true;
}

/*
 * Return the OID of the replica identity index if one is defined;
 * the OID of the PK if one exists and is not deferrable;
 * otherwise, InvalidOid.
 */
Oid
GetRelationIdentityOrPK(Relation rel)
{
	Oid			idxoid;

	idxoid = RelationGetReplicaIndex(rel);

	if (!OidIsValid(idxoid))
		idxoid = RelationGetPrimaryKeyIndex(rel, false);

	return idxoid;
}

/*
 * Returns the index oid if we can use an index for subscriber. Otherwise,
 * returns InvalidOid.
 */
static Oid
FindLogicalRepLocalIndex(Relation localrel, LogicalRepRelation *remoterel,
						 AttrMap *attrMap)
{
	Oid			idxoid;

	/*
	 * We never need index oid for partitioned tables, always rely on leaf
	 * partition's index.
	 */
	if (localrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		return InvalidOid;

	/*
	 * Simple case, we already have a primary key or a replica identity index.
	 */
	idxoid = GetRelationIdentityOrPK(localrel);
	if (OidIsValid(idxoid))
		return idxoid;

	if (remoterel->replident == REPLICA_IDENTITY_FULL)
	{
		/*
		 * We are looking for one more opportunity for using an index. If
		 * there are any indexes defined on the local relation, try to pick a
		 * suitable index.
		 *
		 * The index selection safely assumes that all the columns are going
		 * to be available for the index scan given that remote relation has
		 * replica identity full.
		 *
		 * Note that we are not using the planner to find the cheapest method
		 * to scan the relation as that would require us to either use lower
		 * level planner functions which would be a maintenance burden in the
		 * long run or use the full-fledged planner which could cause
		 * overhead.
		 */
		return FindUsableIndexForReplicaIdentityFull(localrel, attrMap);
	}

	return InvalidOid;
}

/*
 * Get the number of entries in the LogicalRepRelMap.
 */
int
logicalrep_get_num_rels(void)
{
	if (LogicalRepRelMap == NULL)
		return 0;

	return hash_get_num_entries(LogicalRepRelMap);
}

/*
 * Write all the remote relation information from the LogicalRepRelMapEntry to
 * the output stream.
 */
void
logicalrep_write_all_rels(StringInfo out)
{
	LogicalRepRelMapEntry *entry;
	HASH_SEQ_STATUS status;

	if (LogicalRepRelMap == NULL)
		return;

	hash_seq_init(&status, LogicalRepRelMap);

	while ((entry = (LogicalRepRelMapEntry *) hash_seq_search(&status)) != NULL)
		logicalrep_write_internal_rel(out, &entry->remoterel);
}

/*
 * Get the LogicalRepRelMapEntry corresponding to the given relid without
 * opening the local relation.
 */
LogicalRepRelMapEntry *
logicalrep_get_relentry(LogicalRepRelId remoteid)
{
	LogicalRepRelMapEntry *entry;
	bool		found;

	if (LogicalRepRelMap == NULL)
		logicalrep_relmap_init();

	/* Search for existing entry. */
	entry = hash_search(LogicalRepRelMap, (void *) &remoteid,
						HASH_FIND, &found);

	if (!found)
		elog(DEBUG1, "no relation map entry for remote relation ID %u",
			 remoteid);

	return entry;
}
