/*-------------------------------------------------------------------------
 * relation.c
 *	   PostgreSQL logical replication relation mapping cache
 *
 * Copyright (c) 2016-2023, PostgreSQL Global Development Group
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

#include "access/genam.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_subscription_rel.h"
#include "catalog/pg_operator.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "parser/parse_relation.h"
#include "replication/logicalrelation.h"
#include "replication/worker_internal.h"
#include "optimizer/cost.h"
#include "optimizer/paramassign.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/restrictinfo.h"
#include "utils/inval.h"
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

static Oid	FindLogicalRepUsableIndex(Relation localrel,
									  LogicalRepRelation *remoterel);

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
			entry->localrelvalid = false;
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
}

/*
 * Add new entry or update existing entry in the relation map cache.
 *
 * Called when new relation mapping is sent by the publisher to update
 * our expected view of incoming data from said publisher.
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
	entry = hash_search(LogicalRepRelMap, (void *) &remoterel->remoteid,
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
	entry->remoterel.attnames = palloc(remoterel->natts * sizeof(char *));
	entry->remoterel.atttyps = palloc(remoterel->natts * sizeof(Oid));
	for (i = 0; i < remoterel->natts; i++)
	{
		entry->remoterel.attnames[i] = pstrdup(remoterel->attnames[i]);
		entry->remoterel.atttyps[i] = remoterel->atttyps[i];
	}
	entry->remoterel.replident = remoterel->replident;
	entry->remoterel.attkeys = bms_copy(remoterel->attkeys);
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
 * Report error with names of the missing local relation column(s), if any.
 */
static void
logicalrep_report_missing_attrs(LogicalRepRelation *remoterel,
								Bitmapset *missingatts)
{
	if (!bms_is_empty(missingatts))
	{
		StringInfoData missingattsbuf;
		int			missingattcnt = 0;
		int			i;

		initStringInfo(&missingattsbuf);

		while ((i = bms_first_member(missingatts)) >= 0)
		{
			missingattcnt++;
			if (missingattcnt == 1)
				appendStringInfo(&missingattsbuf, _("\"%s\""),
								 remoterel->attnames[i]);
			else
				appendStringInfo(&missingattsbuf, _(", \"%s\""),
								 remoterel->attnames[i]);
		}

		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg_plural("logical replication target relation \"%s.%s\" is missing replicated column: %s",
							   "logical replication target relation \"%s.%s\" is missing replicated columns: %s",
							   missingattcnt,
							   remoterel->nspname,
							   remoterel->relname,
							   missingattsbuf.data)));
	}
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
 * Open the local relation associated with the remote one.
 *
 * Rebuilds the Relcache mapping if it was invalidated by local DDL.
 */
LogicalRepRelMapEntry *
logicalrep_rel_open(LogicalRepRelId remoteid, LOCKMODE lockmode)
{
	LogicalRepRelMapEntry *entry;
	bool		found;
	LogicalRepRelation *remoterel;

	if (LogicalRepRelMap == NULL)
		logicalrep_relmap_init();

	/* Search for existing entry. */
	entry = hash_search(LogicalRepRelMap, (void *) &remoteid,
						HASH_FIND, &found);

	if (!found)
		elog(ERROR, "no relation map entry for remote relation ID %u",
			 remoteid);

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

			if (attr->attisdropped || attr->attgenerated)
			{
				entry->attrmap->attnums[i] = -1;
				continue;
			}

			attnum = logicalrep_rel_att_by_name(remoterel,
												NameStr(attr->attname));

			entry->attrmap->attnums[i] = attnum;
			if (attnum >= 0)
				missingatts = bms_del_member(missingatts, attnum);
		}

		logicalrep_report_missing_attrs(remoterel, missingatts);

		/* be tidy */
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
		entry->usableIndexOid = FindLogicalRepUsableIndex(entry->localrel, remoterel);

		entry->localrelvalid = true;
	}

	if (entry->state != SUBREL_STATE_READY)
		entry->state = GetSubscriptionRelState(MySubscription->oid,
											   entry->localreloid,
											   &entry->statelsn);

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
														(void *) &partOid,
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
		entry = &part_entry->relmapentry;
		entry->remoterel.remoteid = remoterel->remoteid;
		entry->remoterel.nspname = pstrdup(remoterel->nspname);
		entry->remoterel.relname = pstrdup(remoterel->relname);
		entry->remoterel.natts = remoterel->natts;
		entry->remoterel.attnames = palloc(remoterel->natts * sizeof(char *));
		entry->remoterel.atttyps = palloc(remoterel->natts * sizeof(Oid));
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

	/*
	 * Finding a usable index is an infrequent task. It occurs when an
	 * operation is first performed on the relation, or after invalidation of
	 * of the relation cache entry (such as ANALYZE or CREATE/DROP index on
	 * the relation).
	 */
	entry->usableIndexOid = FindLogicalRepUsableIndex(partrel, remoterel);

	entry->localrelvalid = true;

	/* state and statelsn are left set to 0. */
	MemoryContextSwitchTo(oldctx);

	return entry;
}

/*
 * Returns a valid index oid if the input path is an index path.
 *
 * Otherwise, returns InvalidOid.
 */
static Oid
GetIndexOidFromPath(Path *path)
{
	if (path->pathtype == T_IndexScan || path->pathtype == T_IndexOnlyScan)
	{
		IndexPath  *index_sc = (IndexPath *) path;

		return index_sc->indexinfo->indexoid;
	}

	return InvalidOid;
}

/*
 * Returns true if the given index consists only of expressions such as:
 * 	CREATE INDEX idx ON table(foo(col));
 *
 * Returns false even if there is one column reference:
 * 	 CREATE INDEX idx ON table(foo(col), col_2);
 */
bool
IsIndexOnlyOnExpression(IndexInfo *indexInfo)
{
	for (int i = 0; i < indexInfo->ii_NumIndexKeyAttrs; i++)
	{
		AttrNumber	attnum = indexInfo->ii_IndexAttrNumbers[i];

		if (AttributeNumberIsValid(attnum))
			return false;
	}

	return true;
}

/*
 * Iterates over the input path list and returns another path list that
 * includes index [only] scans where paths with non-btree indexes, partial
 * indexes or indexes on only expressions have been removed.
 */
static List *
SuitableIndexPathsForRepIdentFull(List *pathlist)
{
	ListCell   *lc;
	List	   *suitableIndexList = NIL;

	foreach(lc, pathlist)
	{
		Path	   *path = (Path *) lfirst(lc);
		Oid			idxoid = GetIndexOidFromPath(path);

		if (!OidIsValid(idxoid))
		{
			/* Unrelated Path, skip */
			continue;
		}
		else
		{
			Relation	indexRelation;
			IndexInfo  *indexInfo;
			bool		is_btree;
			bool		is_partial;
			bool		is_only_on_expression;

			indexRelation = index_open(idxoid, AccessShareLock);
			indexInfo = BuildIndexInfo(indexRelation);
			is_btree = (indexInfo->ii_Am == BTREE_AM_OID);
			is_partial = (indexInfo->ii_Predicate != NIL);
			is_only_on_expression = IsIndexOnlyOnExpression(indexInfo);
			index_close(indexRelation, AccessShareLock);

			/* eliminating not suitable index scan path */
			if (is_btree && !is_partial && !is_only_on_expression)
				suitableIndexList = lappend(suitableIndexList, path);
		}
	}

	return suitableIndexList;
}

/*
 * This is not a generic function. It is a helper function for
 * GetCheapestReplicaIdentityFullPath. The function creates a dummy PlannerInfo
 * for the given relationId as if the relation is queried with SELECT command.
 */
static PlannerInfo *
GenerateDummySelectPlannerInfoForRelation(Oid relationId)
{
	PlannerInfo *root;
	Query	   *query;
	PlannerGlobal *glob;
	RangeTblEntry *rte;

	/* Set up mostly-dummy planner state */
	query = makeNode(Query);
	query->commandType = CMD_SELECT;

	glob = makeNode(PlannerGlobal);

	root = makeNode(PlannerInfo);
	root->parse = query;
	root->glob = glob;
	root->query_level = 1;
	root->planner_cxt = CurrentMemoryContext;
	root->wt_param_id = -1;

	/* Build a minimal RTE for the rel */
	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = relationId;
	rte->relkind = RELKIND_RELATION;
	rte->rellockmode = AccessShareLock;
	rte->lateral = false;
	rte->inh = false;
	rte->inFromCl = true;
	query->rtable = list_make1(rte);

	addRTEPermissionInfo(&query->rteperminfos, rte);

	/* Set up RTE/RelOptInfo arrays */
	setup_simple_rel_arrays(root);

	return root;
}

/*
 * Generate all the possible paths for the given subscriber relation,
 * for cases where the source relation is replicated via REPLICA
 * IDENTITY FULL. The function returns the cheapest Path among the
 * eligible paths, see SuitableIndexPathsForRepIdentFull().
 *
 * The function emulates getting the cheapest path for a query in
 * the form of:
 * 	"SELECT FROM localrel
 * 	 WHERE attr1 = $1 AND attr2 = $2 ... AND attrN = $N"
 *
 * The function guarantees to return a path, because it adds
 * sequential scan path if needed.
 *
 * The function assumes that all the columns will be provided during
 * the execution phase, given that REPLICA IDENTITY FULL guarantees
 * that.
 */
static Path *
GetCheapestReplicaIdentityFullPath(Relation localrel)
{
	PlannerInfo *root;
	RelOptInfo *rel;
	int			attno;
	RangeTblRef *rt;
	List	   *joinList;

	/* Build PlannerInfo */
	root = GenerateDummySelectPlannerInfoForRelation(localrel->rd_id);

	/* Build RelOptInfo */
	rel = build_simple_rel(root, 1, NULL);

	/*
	 * Generate restrictions for all columns in the form of attr1 = $1 AND
	 * attr2 = $2 ... AND attrN = $N
	 */
	for (attno = 0; attno < RelationGetNumberOfAttributes(localrel); attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(localrel->rd_att, attno);

		if (!attr->attisdropped)
		{
			Expr	   *eq_op;
			TypeCacheEntry *typentry;
			RestrictInfo *restrict_info;
			Var		   *leftarg;
			Param	   *rightarg;
			int			varno = 1;

			typentry = lookup_type_cache(attr->atttypid, TYPECACHE_EQ_OPR_FINFO);

			if (!OidIsValid(typentry->eq_opr))
				continue;		/* no equality operator skip this column */

			leftarg =
				makeVar(varno, attr->attnum, attr->atttypid, attr->atttypmod,
						attr->attcollation, 0);

			rightarg = makeNode(Param);
			rightarg->paramkind = PARAM_EXTERN;
			rightarg->paramid = list_length(rel->baserestrictinfo) + 1;
			rightarg->paramtype = attr->atttypid;
			rightarg->paramtypmod = attr->atttypmod;
			rightarg->paramcollid = attr->attcollation;
			rightarg->location = -1;

			eq_op = make_opclause(typentry->eq_opr, BOOLOID, false,
								  (Expr *) leftarg, (Expr *) rightarg,
								  InvalidOid, attr->attcollation);

			restrict_info = make_simple_restrictinfo(root, eq_op);

			rel->baserestrictinfo = lappend(rel->baserestrictinfo, restrict_info);
		}
	}

	/* Build joinList, which consists of a single relation */
	rt = makeNode(RangeTblRef);
	rt->rtindex = 1;
	joinList = list_make1(rt);

	/*
	 * Make sure the planner generates the relevant paths, including all the
	 * possible index scans as well as sequential scan.
	 */
	rel = make_one_rel(root, joinList);

	/*
	 * Currently it is not possible for the planner to pick a partial index or
	 * indexes only on expressions. We still want to be explicit and eliminate
	 * such paths proactively.
	 *
	 * The reason that the planner would not pick partial indexes and indexes
	 * with only expressions based on the way currently baserestrictinfos are
	 * formed (e.g., col_1 = $1 ... AND col_N = $N).
	 *
	 * For the partial indexes, check_index_predicates() (via
	 * operator_predicate_proof()) checks whether the predicate of the index
	 * is implied by the baserestrictinfos. The check always returns false
	 * because index predicates formed with CONSTs and baserestrictinfos are
	 * formed with PARAMs. Hence, partial indexes are never picked.
	 *
	 * Indexes that consist of only expressions (e.g., no simple column
	 * references on the index) are also eliminated with similar reasoning.
	 * match_restriction_clauses_to_index() (via match_index_to_operand())
	 * eliminates the use of the index if the restriction does not have the
	 * equal expression with the index.
	 *
	 * XXX: We also eliminate non-btree indexes, which could be relaxed if
	 * needed. If we allow non-btree indexes, we should adjust
	 * RelationFindReplTupleByIndex() to support such indexes.
	 */
	rel->pathlist =
		SuitableIndexPathsForRepIdentFull(rel->pathlist);

	if (rel->pathlist == NIL)
	{
		/*
		 * If there are no suitable indexes, we should always be able to
		 * fallback to sequential scan.
		 */
		Path	   *seqScanPath = create_seqscan_path(root, rel, NULL, 0);

		add_path(rel, seqScanPath);
	}

	set_cheapest(rel);

	Assert(rel->cheapest_total_path != NULL);

	return rel->cheapest_total_path;
}

/*
 * Returns an index oid if the planner submodules pick index scans
 * over sequential scan.
 *
 * Otherwise, returns InvalidOid.
 *
 * Note that this is not a generic function, it expects REPLICA
 * IDENTITY FULL for the remote relation.
 */
static Oid
FindUsableIndexForReplicaIdentityFull(Relation localrel)
{
	MemoryContext usableIndexContext;
	MemoryContext oldctx;
	Path	   *cheapest_total_path;
	Oid			idxoid;

	usableIndexContext = AllocSetContextCreate(CurrentMemoryContext,
											   "usableIndexContext",
											   ALLOCSET_DEFAULT_SIZES);
	oldctx = MemoryContextSwitchTo(usableIndexContext);

	cheapest_total_path = GetCheapestReplicaIdentityFullPath(localrel);

	idxoid = GetIndexOidFromPath(cheapest_total_path);

	MemoryContextSwitchTo(oldctx);

	MemoryContextDelete(usableIndexContext);

	return idxoid;
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
 * Returns an index oid if we can use an index for subscriber. If not,
 * returns InvalidOid.
 */
static Oid
FindLogicalRepUsableIndex(Relation localrel, LogicalRepRelation *remoterel)
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
	 *
	 * Note that we do not use index scans below when enable_indexscan is
	 * false. Allowing primary key or replica identity even when index scan is
	 * disabled is the legacy behaviour. So we hesitate to move the below
	 * enable_indexscan check to be done earlier in this function.
	 */
	idxoid = GetRelationIdentityOrPK(localrel);
	if (OidIsValid(idxoid))
		return idxoid;

	/* If index scans are disabled, use a sequential scan */
	if (!enable_indexscan)
		return InvalidOid;

	if (remoterel->replident == REPLICA_IDENTITY_FULL &&
		RelationGetIndexList(localrel) != NIL)
	{
		/*
		 * If we had a primary key or relation identity with a unique index,
		 * we would have already found and returned that oid. At this point,
		 * the remote relation has replica identity full and we have at least
		 * one local index defined.
		 *
		 * We are looking for one more opportunity for using an index. If
		 * there are any indexes defined on the local relation, try to pick
		 * the cheapest index.
		 *
		 * The index selection safely assumes that all the columns are going
		 * to be available for the index scan given that remote relation has
		 * replica identity full.
		 */
		return FindUsableIndexForReplicaIdentityFull(localrel);
	}

	return InvalidOid;
}
