/*-------------------------------------------------------------------------
 *
 * toasting.c
 *	  This file contains routines to support creation of toast tables
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/catalog/toasting.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/toasterapi.h"
#include "access/toast_compression.h"
#include "access/xact.h"
#include "catalog/binary_upgrade.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "catalog/toasting.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"

static void CheckAndCreateToastTable(Oid relOid, Datum reloptions,
									 LOCKMODE lockmode, bool check,
									 Oid OIDOldToast);
static bool needs_toast_table(Relation rel);


/*
 * CreateToastTable variants
 *		If the table needs a toast table, and doesn't already have one,
 *		then create a toast table for it.
 *
 * reloptions for the toast table can be passed, too.  Pass (Datum) 0
 * for default reloptions.
 *
 * We expect the caller to have verified that the relation is a table and have
 * already done any necessary permission checks.  Callers expect this function
 * to end with CommandCounterIncrement if it makes any changes.
 */
void
AlterTableCreateToastTable(Oid relOid, Datum reloptions, LOCKMODE lockmode)
{
	CheckAndCreateToastTable(relOid, reloptions, lockmode, true, InvalidOid);
}

void
NewHeapCreateToastTable(Oid relOid, Datum reloptions, LOCKMODE lockmode,
						Oid OIDOldToast)
{
	CheckAndCreateToastTable(relOid, reloptions, lockmode, false, OIDOldToast);
}

void
NewRelationCreateToastTable(Oid relOid, Datum reloptions)
{
	CheckAndCreateToastTable(relOid, reloptions, AccessExclusiveLock, false,
							 InvalidOid);
}

static void
CheckAndCreateToastTable(Oid relOid, Datum reloptions, LOCKMODE lockmode,
						 bool check, Oid OIDOldToast)
{
	Relation	rel;
	int			i;
	TupleDesc	tupDesc;
	List	   *tsrOids = NIL;

	rel = table_open(relOid, lockmode);

	tupDesc = RelationGetDescr(rel);

	/*
	 * Create toaster data storage (heap table for generic toaster), once per
	 * table for each toster.
	 */
	for(i = 0; i < tupDesc->natts; i++)
	{
		FormData_pg_attribute   *attr = TupleDescAttr(tupDesc, i);
		TsrRoutine				*tsr;

		if (attr->attisdropped || !OidIsValid(attr->atttoaster))
			continue;

		/* such toaster is already created its storage */
		if (list_member_oid(tsrOids, attr->atttoaster))
			continue;

		tsr = SearchTsrCache(attr->atttoaster);

		tsr->init(rel, InvalidOid, InvalidOid, reloptions, lockmode, check, OIDOldToast);

		tsrOids = lappend_oid(tsrOids, attr->atttoaster);
	}

	table_close(rel, NoLock);
}

/*
 * Create a toast table during bootstrap
 *
 * Here we need to prespecify the OIDs of the toast table and its index
 */
void
BootstrapToastTable(char *relName, Oid toastOid, Oid toastIndexOid)
{
	Relation	rel;
	TupleDesc	tupDesc;
	List	   *tsrOids = NIL;

	rel = table_openrv(makeRangeVar(NULL, relName, -1), AccessExclusiveLock);

	if (rel->rd_rel->relkind != RELKIND_RELATION &&
		rel->rd_rel->relkind != RELKIND_MATVIEW)
		elog(ERROR, "\"%s\" is not a table or materialized view",
			 relName);

	/* create_toast_table does all the work */
	tupDesc = RelationGetDescr(rel);
	for(int i = 0; i < tupDesc->natts; i++)
	{
		FormData_pg_attribute   *attr = TupleDescAttr(tupDesc, i);
		TsrRoutine				*tsr;

		if (attr->attisdropped || !OidIsValid(attr->atttoaster))
			continue;

		/* such toaster is already created its storage */
		if (list_member_oid(tsrOids, attr->atttoaster))
			continue;

		tsr = SearchTsrCache(attr->atttoaster);

		if (!tsr)
			elog(ERROR, "\"%s\" does not require a toast table", relName);
		else
			tsr->init(rel, toastOid, toastIndexOid, (Datum) 0,
								AccessExclusiveLock, false, InvalidOid);

		tsrOids = lappend_oid(tsrOids, attr->atttoaster);
	}

/*
	if (!create_toast_table(rel, toastOid, toastIndexOid, (Datum) 0,
							AccessExclusiveLock, false, InvalidOid))
		elog(ERROR, "\"%s\" does not require a toast table",
			 relName);
*/
	table_close(rel, NoLock);
}


/*
 * create_toast_table --- do main work
 *
 * rel is already opened and locked
 * toastOid and toastIndexOid are normally InvalidOid, but during
 * bootstrap they can be nonzero to specify hand-assigned OIDs
 */
bool
create_toast_table(Relation rel, Oid toastOid, Oid toastIndexOid,
				   Datum reloptions, LOCKMODE lockmode, bool check,
				   Oid OIDOldToast)
{
	Oid			relOid = RelationGetRelid(rel);
	HeapTuple	reltup;
	TupleDesc	tupdesc;
	bool		shared_relation;
	bool		mapped_relation;
	Relation	toast_rel;
	Relation	class_rel;
	Oid			toast_relid;
	Oid			namespaceid;
	char		toast_relname[NAMEDATALEN];
	char		toast_idxname[NAMEDATALEN];
	IndexInfo  *indexInfo;
	Oid			collationObjectId[2];
	Oid			classObjectId[2];
	int16		coloptions[2];
	ObjectAddress baseobject,
				toastobject;

	/*
	 * Is it already toasted?
	 */
	if (rel->rd_rel->reltoastrelid != InvalidOid)
		return false;

	/*
	 * Check to see whether the table actually needs a TOAST table.
	 */
	if (!IsBinaryUpgrade)
	{
		/* Normal mode, normal check */
		if (!needs_toast_table(rel))
			return false;
	}
	else
	{
		/*
		 * In binary-upgrade mode, create a TOAST table if and only if
		 * pg_upgrade told us to (ie, a TOAST table OID has been provided).
		 *
		 * This indicates that the old cluster had a TOAST table for the
		 * current table.  We must create a TOAST table to receive the old
		 * TOAST file, even if the table seems not to need one.
		 *
		 * Contrariwise, if the old cluster did not have a TOAST table, we
		 * should be able to get along without one even if the new version's
		 * needs_toast_table rules suggest we should have one.  There is a lot
		 * of daylight between where we will create a TOAST table and where
		 * one is really necessary to avoid failures, so small cross-version
		 * differences in the when-to-create heuristic shouldn't be a problem.
		 * If we tried to create a TOAST table anyway, we would have the
		 * problem that it might take up an OID that will conflict with some
		 * old-cluster table we haven't seen yet.
		 */
		if (!OidIsValid(binary_upgrade_next_toast_pg_class_oid))
			return false;
	}

	/*
	 * If requested check lockmode is sufficient. This is a cross check in
	 * case of errors or conflicting decisions in earlier code.
	 */
	if (check && lockmode != AccessExclusiveLock)
		elog(ERROR, "AccessExclusiveLock required to add toast table.");

	/*
	 * Create the toast table and its index
	 */
	snprintf(toast_relname, sizeof(toast_relname),
			 "pg_toast_%u", relOid);
	snprintf(toast_idxname, sizeof(toast_idxname),
			 "pg_toast_%u_index", relOid);

	/* this is pretty painful...  need a tuple descriptor */
	tupdesc = CreateTemplateTupleDesc(3);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1,
					   "chunk_id",
					   OIDOID,
					   -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2,
					   "chunk_seq",
					   INT4OID,
					   -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3,
					   "chunk_data",
					   BYTEAOID,
					   -1, 0);

	/*
	 * Ensure that the toast table doesn't itself get toasted, or we'll be
	 * toast :-(.  This is essential for chunk_data because type bytea is
	 * toastable; hit the other two just to be sure.
	 */
	TupleDescAttr(tupdesc, 0)->attstorage = TYPSTORAGE_PLAIN;
	TupleDescAttr(tupdesc, 1)->attstorage = TYPSTORAGE_PLAIN;
	TupleDescAttr(tupdesc, 2)->attstorage = TYPSTORAGE_PLAIN;

	/* Toast field should not be compressed */
	TupleDescAttr(tupdesc, 0)->attcompression = InvalidCompressionMethod;
	TupleDescAttr(tupdesc, 1)->attcompression = InvalidCompressionMethod;
	TupleDescAttr(tupdesc, 2)->attcompression = InvalidCompressionMethod;

	/*
	 * Toast tables for regular relations go in pg_toast; those for temp
	 * relations go into the per-backend temp-toast-table namespace.
	 */
	if (isTempOrTempToastNamespace(rel->rd_rel->relnamespace))
		namespaceid = GetTempToastNamespace();
	else
		namespaceid = PG_TOAST_NAMESPACE;

	/* Toast table is shared if and only if its parent is. */
	shared_relation = rel->rd_rel->relisshared;

	/* It's mapped if and only if its parent is, too */
	mapped_relation = RelationIsMapped(rel);

	toast_relid = heap_create_with_catalog(toast_relname,
										   namespaceid,
										   rel->rd_rel->reltablespace,
										   toastOid,
										   InvalidOid,
										   InvalidOid,
										   rel->rd_rel->relowner,
										   table_relation_toast_am(rel),
										   tupdesc,
										   NIL,
										   RELKIND_TOASTVALUE,
										   rel->rd_rel->relpersistence,
										   shared_relation,
										   mapped_relation,
										   ONCOMMIT_NOOP,
										   reloptions,
										   false,
										   true,
										   true,
										   OIDOldToast,
										   NULL);
	Assert(toast_relid != InvalidOid);

	/* make the toast relation visible, else table_open will fail */
	CommandCounterIncrement();

	/* ShareLock is not really needed here, but take it anyway */
	toast_rel = table_open(toast_relid, ShareLock);

	/*
	 * Create unique index on chunk_id, chunk_seq.
	 *
	 * NOTE: the normal TOAST access routines could actually function with a
	 * single-column index on chunk_id only. However, the slice access
	 * routines use both columns for faster access to an individual chunk. In
	 * addition, we want it to be unique as a check against the possibility of
	 * duplicate TOAST chunk OIDs. The index might also be a little more
	 * efficient this way, since btree isn't all that happy with large numbers
	 * of equal keys.
	 */

	indexInfo = makeNode(IndexInfo);
	indexInfo->ii_NumIndexAttrs = 2;
	indexInfo->ii_NumIndexKeyAttrs = 2;
	indexInfo->ii_IndexAttrNumbers[0] = 1;
	indexInfo->ii_IndexAttrNumbers[1] = 2;
	indexInfo->ii_Expressions = NIL;
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_Predicate = NIL;
	indexInfo->ii_PredicateState = NULL;
	indexInfo->ii_ExclusionOps = NULL;
	indexInfo->ii_ExclusionProcs = NULL;
	indexInfo->ii_ExclusionStrats = NULL;
	indexInfo->ii_OpclassOptions = NULL;
	indexInfo->ii_Unique = true;
	indexInfo->ii_NullsNotDistinct = false;
	indexInfo->ii_ReadyForInserts = true;
	indexInfo->ii_CheckedUnchanged = false;
	indexInfo->ii_IndexUnchanged = false;
	indexInfo->ii_Concurrent = false;
	indexInfo->ii_BrokenHotChain = false;
	indexInfo->ii_ParallelWorkers = 0;
	indexInfo->ii_Am = BTREE_AM_OID;
	indexInfo->ii_AmCache = NULL;
	indexInfo->ii_Context = CurrentMemoryContext;

	collationObjectId[0] = InvalidOid;
	collationObjectId[1] = InvalidOid;

	classObjectId[0] = OID_BTREE_OPS_OID;
	classObjectId[1] = INT4_BTREE_OPS_OID;

	coloptions[0] = 0;
	coloptions[1] = 0;

	index_create(toast_rel, toast_idxname, toastIndexOid, InvalidOid,
				 InvalidOid, InvalidOid,
				 indexInfo,
				 list_make2("chunk_id", "chunk_seq"),
				 BTREE_AM_OID,
				 rel->rd_rel->reltablespace,
				 collationObjectId, classObjectId, coloptions, (Datum) 0,
				 INDEX_CREATE_IS_PRIMARY, 0, true, true, NULL);

	table_close(toast_rel, NoLock);

	/*
	 * Store the toast table's OID in the parent relation's pg_class row
	 */
	class_rel = table_open(RelationRelationId, RowExclusiveLock);

	reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relOid));
	if (!HeapTupleIsValid(reltup))
		elog(ERROR, "cache lookup failed for relation %u", relOid);

	((Form_pg_class) GETSTRUCT(reltup))->reltoastrelid = toast_relid;

	if (!IsBootstrapProcessingMode())
	{
		/* normal case, use a transactional update */
		CatalogTupleUpdate(class_rel, &reltup->t_self, reltup);
	}
	else
	{
		/* While bootstrapping, we cannot UPDATE, so overwrite in-place */
		heap_inplace_update(class_rel, reltup);
	}

	heap_freetuple(reltup);

	table_close(class_rel, RowExclusiveLock);

	/*
	 * Register dependency from the toast table to the main, so that the toast
	 * table will be deleted if the main is.  Skip this in bootstrap mode.
	 */
	if (!IsBootstrapProcessingMode())
	{
		baseobject.classId = RelationRelationId;
		baseobject.objectId = relOid;
		baseobject.objectSubId = 0;
		toastobject.classId = RelationRelationId;
		toastobject.objectId = toast_relid;
		toastobject.objectSubId = 0;

		recordDependencyOn(&toastobject, &baseobject, DEPENDENCY_INTERNAL);
	}

	/*
	 * Make changes visible
	 */
	CommandCounterIncrement();

	return true;
}

/*
 * Check to see whether the table needs a TOAST table.
 */
static bool
needs_toast_table(Relation rel)
{
	/*
	 * No need to create a TOAST table for partitioned tables.
	 */
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		return false;

	/*
	 * We cannot allow toasting a shared relation after initdb (because
	 * there's no way to mark it toasted in other databases' pg_class).
	 */
	if (rel->rd_rel->relisshared && !IsBootstrapProcessingMode())
		return false;

	/*
	 * Ignore attempts to create toast tables on catalog tables after initdb.
	 * Which catalogs get toast tables is explicitly chosen in catalog/pg_*.h.
	 * (We could get here via some ALTER TABLE command if the catalog doesn't
	 * have a toast table.)
	 */
	if (IsCatalogRelation(rel) && !IsBootstrapProcessingMode())
		return false;

	/* Otherwise, let the AM decide. */
	return table_relation_needs_toast_table(rel);
}

/* ----------
 * toast_get_valid_index
 *
 *	Get OID of valid index associated to given toast relation. A toast
 *	relation can have only one valid index at the same time.
 */
Oid
toast_get_valid_index(Oid toastoid, LOCKMODE lock)
{
	int			num_indexes;
	int			validIndex;
	Oid			validIndexOid;
	Relation   *toastidxs;
	Relation	toastrel;

	/* Open the toast relation */
	toastrel = table_open(toastoid, lock);

	/* Look for the valid index of the toast relation */
	validIndex = toast_open_indexes(toastrel,
									lock,
									&toastidxs,
									&num_indexes);
	validIndexOid = RelationGetRelid(toastidxs[validIndex]);

	/* Close the toast relation and all its indexes */
	toast_close_indexes(toastidxs, num_indexes, NoLock);
	table_close(toastrel, NoLock);

	return validIndexOid;
}

/* ----------
 * toast_open_indexes
 *
 *	Get an array of the indexes associated to the given toast relation
 *	and return as well the position of the valid index used by the toast
 *	relation in this array. It is the responsibility of the caller of this
 *	function to close the indexes as well as free them.
 */
int
toast_open_indexes(Relation toastrel,
				   LOCKMODE lock,
				   Relation **toastidxs,
				   int *num_indexes)
{
	int			i = 0;
	int			res = 0;
	bool		found = false;
	List	   *indexlist;
	ListCell   *lc;

	/* Get index list of the toast relation */
	indexlist = RelationGetIndexList(toastrel);
	Assert(indexlist != NIL);

	*num_indexes = list_length(indexlist);

	/* Open all the index relations */
	*toastidxs = (Relation *) palloc(*num_indexes * sizeof(Relation));
	foreach(lc, indexlist)
		(*toastidxs)[i++] = index_open(lfirst_oid(lc), lock);

	/* Fetch the first valid index in list */
	for (i = 0; i < *num_indexes; i++)
	{
		Relation	toastidx = (*toastidxs)[i];

		if (toastidx->rd_index->indisvalid)
		{
			res = i;
			found = true;
			break;
		}
	}

	/*
	 * Free index list, not necessary anymore as relations are opened and a
	 * valid index has been found.
	 */
	list_free(indexlist);

	/*
	 * The toast relation should have one valid index, so something is going
	 * wrong if there is nothing.
	 */
	if (!found)
		elog(ERROR, "no valid index found for toast relation with Oid %u",
			 RelationGetRelid(toastrel));

	return res;
}

/* ----------
 * toast_close_indexes
 *
 *	Close an array of indexes for a toast relation and free it. This should
 *	be called for a set of indexes opened previously with toast_open_indexes.
 */
void
toast_close_indexes(Relation *toastidxs, int num_indexes, LOCKMODE lock)
{
	int			i;

	/* Close relations and clean up things */
	for (i = 0; i < num_indexes; i++)
		index_close(toastidxs[i], lock);
	pfree(toastidxs);
}


/* ----------
 * init_toast_snapshot
 *
 *	Initialize an appropriate TOAST snapshot.  We must use an MVCC snapshot
 *	to initialize the TOAST snapshot; since we don't know which one to use,
 *	just use the oldest one.  This is safe: at worst, we will get a "snapshot
 *	too old" error that might have been avoided otherwise.
 */
void
init_toast_snapshot(Snapshot toast_snapshot)
{
	Snapshot	snapshot = GetOldestSnapshot();

	/*
	 * GetOldestSnapshot returns NULL if the session has no active snapshots.
	 * We can get that if, for example, a procedure fetches a toasted value
	 * into a local variable, commits, and then tries to detoast the value.
	 * Such coding is unsafe, because once we commit there is nothing to
	 * prevent the toast data from being deleted.  Detoasting *must* happen in
	 * the same transaction that originally fetched the toast pointer.  Hence,
	 * rather than trying to band-aid over the problem, throw an error.  (This
	 * is not very much protection, because in many scenarios the procedure
	 * would have already created a new transaction snapshot, preventing us
	 * from detecting the problem.  But it's better than nothing, and for sure
	 * we shouldn't expend code on masking the problem more.)
	 */
	if (snapshot == NULL)
		elog(ERROR, "cannot fetch toast data without an active snapshot");

	InitToastSnapshot(*toast_snapshot, snapshot->lsn, snapshot->whenTaken);
}
