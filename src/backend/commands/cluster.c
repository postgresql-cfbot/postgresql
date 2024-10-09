/*-------------------------------------------------------------------------
 *
 * cluster.c
 *	  CLUSTER a table on an index.  This is now also used for VACUUM FULL.
 *
 * There is hardly anything left of Paul Brown's original implementation...
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/cluster.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/toast_internals.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_am.h"
#include "catalog/pg_control.h"
#include "catalog/pg_database.h"
#include "catalog/pg_inherits.h"
#include "catalog/toasting.h"
#include "commands/cluster.h"
#include "commands/defrem.h"
#include "commands/progress.h"
#include "commands/tablecmds.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "pgstat.h"
#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/snapbuild.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "utils/acl.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/relmapper.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

typedef struct RewriteStateData *RewriteState;

/*
 * This struct is used to pass around the information on tables to be
 * clustered. We need this so we can make a list of them when invoked without
 * a specific table/index pair.
 */
typedef struct
{
	Oid			tableOid;
	Oid			indexOid;
} RelToCluster;

/*
 * The following definitions are used for concurrent processing.
 */

/*
 * OID of the table being processed by this backend.
 */
static Oid	clustered_rel	= InvalidOid;
/* The same for its TOAST relation. */
static Oid	clustered_rel_toast	= InvalidOid;

/*
 * The locators are used to avoid logical decoding of data that we do not need
 * for our table.
 */
RelFileLocator	clustered_rel_locator = {.relNumber = InvalidOid};
RelFileLocator	clustered_rel_toast_locator = {.relNumber = InvalidOid};

/* XXX Do we also need to mention VACUUM FULL CONCURRENTLY? */
#define CLUSTER_IN_PROGRESS_MESSAGE \
	"relation \"%s\" is already being processed by CLUSTER CONCURRENTLY"

/*
 * Everything we need to call ExecInsertIndexTuples().
 */
typedef struct IndexInsertState
{
	ResultRelInfo *rri;
	EState	   *estate;
	ExprContext *econtext;

	Relation	ident_index;
} IndexInsertState;

/*
 * Catalog information to check if another backend changed the relation in
 * such a way that makes CLUSTER CONCURRENTLY unable to continue. Such changes
 * are possible because cluster_rel() has to release its lock on the relation
 * in order to acquire AccessExclusiveLock that it needs to swap the relation
 * files.
 *
 * The most obvious problem is that the tuple descriptor has changed, since
 * then the tuples we try to insert into the new storage are not guaranteed to
 * fit into the storage.
 *
 * Another problem is relfilenode changed by another backend. It's not
 * necessarily a correctness issue (e.g. when the other backend ran
 * cluster_rel()), but it's safer for us to terminate the table processing in
 * such cases. However, this information is also needs to be checked during
 * logical decoding, so we store it in global variables clustered_rel_locator
 * and clustered_rel_toast_locator above.
 *
 * Where possible, commands which might change the relation in an incompatible
 * way should check if CLUSTER CONCURRENTLY is running, before they start to
 * do the actual changes (see is_concurrent_cluster_in_progress()). Anything
 * else must be caught by check_catalog_changes(), which uses this structure.
 */
typedef struct CatalogState
{
	/* Tuple descriptor of the relation. */
	TupleDesc	tupdesc;

	/* The number of indexes tracked. */
	int		ninds;
	/* The index OIDs. */
	Oid		*ind_oids;
	/* The index tuple descriptors. */
	TupleDesc	*ind_tupdescs;

	/* The following are copies of the corresponding fields of pg_class. */
	char	relpersistence;
	char	replident;

	/* rd_replidindex */
	Oid		replidindex;
} CatalogState;

/* The WAL segment being decoded. */
static XLogSegNo	cluster_current_segment = 0;

static void cluster_multiple_rels(List *rtcs, ClusterParams *params,
								  LOCKMODE lockmode, bool isTopLevel);
static void rebuild_relation(Relation OldHeap, Relation index, bool verbose,
							 bool concurrent);
static void copy_table_data(Relation NewHeap, Relation OldHeap, Relation OldIndex,
							Snapshot snapshot, LogicalDecodingContext *decoding_ctx,
							bool verbose, bool *pSwapToastByContent,
							TransactionId *pFreezeXid, MultiXactId *pCutoffMulti);
static List *get_tables_to_cluster(MemoryContext cluster_context);
static List *get_tables_to_cluster_partitioned(MemoryContext cluster_context,
											   Oid indexOid);
static bool cluster_is_permitted_for_relation(Oid relid, Oid userid);
static void check_concurrent_cluster_requirements(Relation rel,
												  bool isTopLevel,
												  bool isCluster);
static void begin_concurrent_cluster(Relation *rel_p, Relation *index_p,
									 bool *entered_p);
static void end_concurrent_cluster(bool error);
static void cluster_before_shmem_exit_callback(int code, Datum arg);
static CatalogState *get_catalog_state(Relation rel);
static void free_catalog_state(CatalogState *state);
static void check_catalog_changes(Relation rel, CatalogState *cat_state);
static LogicalDecodingContext *setup_logical_decoding(Oid relid,
													  const char *slotname,
													  TupleDesc tupdesc);
static HeapTuple get_changed_tuple(ConcurrentChange *change);
static void apply_concurrent_changes(ClusterDecodingState *dstate,
									 Relation rel, ScanKey key, int nkeys,
									 IndexInsertState *iistate);
static void apply_concurrent_insert(Relation rel, ConcurrentChange *change,
									HeapTuple tup, IndexInsertState *iistate,
									TupleTableSlot *index_slot);
static void apply_concurrent_update(Relation rel, HeapTuple tup,
									HeapTuple tup_target,
									ConcurrentChange *change,
									IndexInsertState *iistate,
									TupleTableSlot *index_slot);
static void apply_concurrent_delete(Relation rel, HeapTuple tup_target,
									ConcurrentChange *change);
static HeapTuple find_target_tuple(Relation rel, ScanKey key, int nkeys,
								   HeapTuple tup_key,
								   Snapshot snapshot,
								   IndexInsertState *iistate,
								   TupleTableSlot *ident_slot,
								   IndexScanDesc *scan_p);
static void process_concurrent_changes(LogicalDecodingContext *ctx,
									   XLogRecPtr end_of_wal,
									   Relation rel_dst,
									   Relation rel_src,
									   ScanKey ident_key,
									   int ident_key_nentries,
									   IndexInsertState *iistate);
static IndexInsertState *get_index_insert_state(Relation relation,
												Oid ident_index_id);
static ScanKey build_identity_key(Oid ident_idx_oid, Relation rel_src,
								  int *nentries);
static void free_index_insert_state(IndexInsertState *iistate);
static void cleanup_logical_decoding(LogicalDecodingContext *ctx);
static void rebuild_relation_finish_concurrent(Relation NewHeap, Relation OldHeap,
											   Relation cl_index,
											   CatalogState	*cat_state,
											   LogicalDecodingContext *ctx,
											   bool swap_toast_by_content,
											   TransactionId frozenXid,
											   MultiXactId cutoffMulti);
static List *build_new_indexes(Relation NewHeap, Relation OldHeap, List *OldIndexes);

/*
 * Use this API when relation needs to be unlocked, closed and re-opened. If
 * the relation got dropped while being unlocked, raise ERROR that mentions
 * the relation name rather than OID.
 */
typedef struct RelReopenInfo
{
	/*
	 * The relation to be closed. Pointer to the value is stored here so that
	 * the user gets his reference updated automatically on re-opening.
	 *
	 * When calling unlock_and_close_relations(), 'relid' can be passed
	 * instead of 'rel_p' when the caller only needs to gather information for
	 * subsequent opening.
	 */
	Relation	*rel_p;
	Oid		relid;

	char		relkind;
	LOCKMODE	lockmode_orig;	/* The existing lock mode */
	LOCKMODE	lockmode_new;	/* The lock mode after the relation is
								 * re-opened */

	char	*relname;			/* Relation name, initialized automatically. */
} RelReopenInfo;

static void init_rel_reopen_info(RelReopenInfo *rri, Relation *rel_p,
								 Oid relid, LOCKMODE lockmode_orig,
								 LOCKMODE lockmode_new);
static void unlock_and_close_relations(RelReopenInfo *rels, int nrel);
static void reopen_relations(RelReopenInfo *rels, int nrel);

/*---------------------------------------------------------------------------
 * This cluster code allows for clustering multiple tables at once. Because
 * of this, we cannot just run everything on a single transaction, or we
 * would be forced to acquire exclusive locks on all the tables being
 * clustered, simultaneously --- very likely leading to deadlock.
 *
 * To solve this we follow a similar strategy to VACUUM code,
 * clustering each relation in a separate transaction. For this to work,
 * we need to:
 *	- provide a separate memory context so that we can pass information in
 *	  a way that survives across transactions
 *	- start a new transaction every time a new relation is clustered
 *	- check for validity of the information on to-be-clustered relations,
 *	  as someone might have deleted a relation behind our back, or
 *	  clustered one on a different index
 *	- end the transaction
 *
 * The single-relation case does not have any such overhead.
 *
 * We also allow a relation to be specified without index.  In that case,
 * the indisclustered bit will be looked up, and an ERROR will be thrown
 * if there is no index with the bit set.
 *---------------------------------------------------------------------------
 */
void
cluster(ParseState *pstate, ClusterStmt *stmt, bool isTopLevel)
{
	ListCell   *lc;
	ClusterParams params = {0};
	bool		verbose = false;
	bool		concurrent = false;
	Relation	rel = NULL;
	Oid			indexOid = InvalidOid;
	MemoryContext cluster_context;
	List	   *rtcs;
	LOCKMODE lockmode;

	/* Parse option list */
	foreach(lc, stmt->params)
	{
		DefElem    *opt = (DefElem *) lfirst(lc);

		if (strcmp(opt->defname, "verbose") == 0)
			verbose = defGetBoolean(opt);
		else if (strcmp(opt->defname, "concurrently") == 0)
			concurrent = defGetBoolean(opt);
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized CLUSTER option \"%s\"",
							opt->defname),
					 parser_errposition(pstate, opt->location)));
	}

	params.options =
		(verbose ? CLUOPT_VERBOSE : 0) |
		(concurrent ? CLUOPT_CONCURRENT : 0);

	/*
	 * Determine the lock mode expected by cluster_rel().
	 *
	 * In the exclusive case, we obtain AccessExclusiveLock right away to
	 * avoid lock-upgrade hazard in the single-transaction case. In the
	 * CONCURRENT case, the AccessExclusiveLock will only be used at the end
	 * of processing, supposedly for very short time. Until then, we'll have
	 * to unlock the relation temporarily, so there's no lock-upgrade hazard.
	 */
	lockmode = (params.options & CLUOPT_CONCURRENT) == 0 ?
		AccessExclusiveLock : LOCK_CLUSTER_CONCURRENT;

	if (stmt->relation != NULL)
	{
		/* This is the single-relation case. */
		Oid			tableOid;

		/* Find, lock, and check permissions on the table. */
		tableOid = RangeVarGetRelidExtended(stmt->relation,
											lockmode,
											0,
											RangeVarCallbackMaintainsTable,
											NULL);
		rel = table_open(tableOid, NoLock);

		/*
		 * Reject clustering a remote temp table ... their local buffer
		 * manager is not going to cope.
		 */
		if (RELATION_IS_OTHER_TEMP(rel))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot cluster temporary tables of other sessions")));

		if (stmt->indexname == NULL)
		{
			ListCell   *index;

			/* We need to find the index that has indisclustered set. */
			foreach(index, RelationGetIndexList(rel))
			{
				indexOid = lfirst_oid(index);
				if (get_index_isclustered(indexOid))
					break;
				indexOid = InvalidOid;
			}

			if (!OidIsValid(indexOid))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("there is no previously clustered index for table \"%s\"",
								stmt->relation->relname)));
		}
		else
		{
			/*
			 * The index is expected to be in the same namespace as the
			 * relation.
			 */
			indexOid = get_relname_relid(stmt->indexname,
										 rel->rd_rel->relnamespace);
			if (!OidIsValid(indexOid))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("index \"%s\" for table \"%s\" does not exist",
								stmt->indexname, stmt->relation->relname)));
		}

		if (rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		{
			/*
			 * Do the job. (The function will close the relation, lock is kept
			 * till commit.)
			 */
			cluster_rel(rel, indexOid, &params, isTopLevel);

			return;
		}
	}

	/*
	 * By here, we know we are in a multi-table situation.  In order to avoid
	 * holding locks for too long, we want to process each table in its own
	 * transaction.  This forces us to disallow running inside a user
	 * transaction block.
	 */
	PreventInTransactionBlock(isTopLevel, "CLUSTER");

	/* Also, we need a memory context to hold our list of relations */
	cluster_context = AllocSetContextCreate(PortalContext,
											"Cluster",
											ALLOCSET_DEFAULT_SIZES);

	/*
	 * Either we're processing a partitioned table, or we were not given any
	 * table name at all.  In either case, obtain a list of relations to
	 * process.
	 *
	 * In the former case, an index name must have been given, so we don't
	 * need to recheck its "indisclustered" bit, but we have to check that it
	 * is an index that we can cluster on.  In the latter case, we set the
	 * option bit to have indisclustered verified.
	 *
	 * Rechecking the relation itself is necessary here in all cases.
	 */
	params.options |= CLUOPT_RECHECK;
	if (rel != NULL)
	{
		Assert(rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);
		check_index_is_clusterable(rel, indexOid, AccessShareLock);
		rtcs = get_tables_to_cluster_partitioned(cluster_context, indexOid);

		/* close relation, releasing lock on parent table */
		table_close(rel, lockmode);
	}
	else
	{
		rtcs = get_tables_to_cluster(cluster_context);
		params.options |= CLUOPT_RECHECK_ISCLUSTERED;
	}

	/* Do the job. */
	cluster_multiple_rels(rtcs, &params, lockmode, isTopLevel);

	/* Start a new transaction for the cleanup work. */
	StartTransactionCommand();

	/* Clean up working storage */
	MemoryContextDelete(cluster_context);
}

/*
 * Given a list of relations to cluster, process each of them in a separate
 * transaction.
 *
 * We expect to be in a transaction at start, but there isn't one when we
 * return.
 */
static void
cluster_multiple_rels(List *rtcs, ClusterParams *params, LOCKMODE lockmode,
					  bool isTopLevel)
{
	ListCell   *lc;

	/* Commit to get out of starting transaction */
	PopActiveSnapshot();
	CommitTransactionCommand();

	/* Cluster the tables, each in a separate transaction */
	foreach(lc, rtcs)
	{
		RelToCluster *rtc = (RelToCluster *) lfirst(lc);
		Relation	rel;

		/* Start a new transaction for each relation. */
		StartTransactionCommand();

		/* functions in indexes may want a snapshot set */
		PushActiveSnapshot(GetTransactionSnapshot());

		rel = table_open(rtc->tableOid, lockmode);

		/* Not all relations cannot be processed in the concurrent mode. */
		if ((params->options & CLUOPT_CONCURRENT) == 0 ||
			check_relation_is_clusterable_concurrently(rel, DEBUG1,
													   "CLUSTER (CONCURRENTLY)"))
		{
			/* Do the job. (The function will close the relation, lock is kept
			 * till commit.) */
			cluster_rel(rel, rtc->indexOid, params, isTopLevel);
		}
		else
			table_close(rel, lockmode);

		PopActiveSnapshot();
		CommitTransactionCommand();
	}
}

/*
 * cluster_rel
 *
 * This clusters the table by creating a new, clustered table and
 * swapping the relfilenumbers of the new table and the old table, so
 * the OID of the original table is preserved.  Thus we do not lose
 * GRANT, inheritance nor references to this table (this was a bug
 * in releases through 7.3).
 *
 * Indexes are rebuilt too, via REINDEX. Since we are effectively bulk-loading
 * the new table, it's better to create the indexes afterwards than to fill
 * them incrementally while we load the table.
 *
 * If indexOid is InvalidOid, the table will be rewritten in physical order
 * instead of index order.  This is the new implementation of VACUUM FULL,
 * and error messages should refer to the operation as VACUUM not CLUSTER.
 *
 * We expect that OldHeap is already locked. The lock mode is
 * AccessExclusiveLock for normal processing and LOCK_CLUSTER_CONCURRENT for
 * concurrent processing (so that SELECT, INSERT, UPDATE and DELETE commands
 * work, but cluster_rel() cannot be called concurrently for the same
 * relation).
 *
 * Note that, in the concurrent case, the function releases the lock at some
 * point, in order to get AccessExclusiveLock for the final steps (i.e. to
 * swap the relation files). To make things simpler, the caller should expect
 * OldHeap to be closed on return, regardless CLUOPT_CONCURRENT. (The
 * AccessExclusiveLock is kept till the end of the transaction.)
 */
void
cluster_rel(Relation OldHeap, Oid indexOid, ClusterParams *params,
			bool isTopLevel)
{
	Oid			tableOid = RelationGetRelid(OldHeap);
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;
	bool		verbose = ((params->options & CLUOPT_VERBOSE) != 0);
	bool		recheck = ((params->options & CLUOPT_RECHECK) != 0);
	Relation	index = NULL;
	bool		concurrent = ((params->options & CLUOPT_CONCURRENT) != 0);
	LOCKMODE	lmode;
	bool		entered, success;

	/* Check that the correct lock is held. */
	lmode = !concurrent ? AccessExclusiveLock : LOCK_CLUSTER_CONCURRENT;

	/*
	 * Skip the relation if it's being processed concurrently. In such a case,
	 * we cannot rely on a lock because the other backend needs to release it
	 * temporarily at some point.
	 *
	 * This check should not take place until we have a lock that prevents
	 * another backend from starting VACUUM FULL / CLUSTER CONCURRENTLY after
	 * our check.
	 */
	Assert(CheckRelationLockedByMe(OldHeap, lmode, false));
	if (is_concurrent_cluster_in_progress(tableOid))
	{
		ereport(NOTICE,
				(errmsg(CLUSTER_IN_PROGRESS_MESSAGE,
						RelationGetRelationName(OldHeap))));
		table_close(OldHeap, lmode);
		return;
	}

	/* There are specific requirements on concurrent processing. */
	if (concurrent)
	{
		check_concurrent_cluster_requirements(OldHeap, isTopLevel,
											  OidIsValid(indexOid));

		check_relation_is_clusterable_concurrently(OldHeap, ERROR,
												   "CLUSTER (CONCURRENTLY)");
	}

	/* Check for user-requested abort. */
	CHECK_FOR_INTERRUPTS();

	pgstat_progress_start_command(PROGRESS_COMMAND_CLUSTER, tableOid);
	if (OidIsValid(indexOid))
		pgstat_progress_update_param(PROGRESS_CLUSTER_COMMAND,
									 PROGRESS_CLUSTER_COMMAND_CLUSTER);
	else
		pgstat_progress_update_param(PROGRESS_CLUSTER_COMMAND,
									 PROGRESS_CLUSTER_COMMAND_VACUUM_FULL);

	/*
	 * Switch to the table owner's userid, so that any index functions are run
	 * as that user.  Also lock down security-restricted operations and
	 * arrange to make GUC variable changes local to this command.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(OldHeap->rd_rel->relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();
	RestrictSearchPath();

	/*
	 * Since we may open a new transaction for each relation, we have to check
	 * that the relation still is what we think it is.
	 *
	 * If this is a single-transaction CLUSTER, we can skip these tests. We
	 * *must* skip the one on indisclustered since it would reject an attempt
	 * to cluster a not-previously-clustered index.
	 */
	if (recheck)
	{
		/* Check that the user still has privileges for the relation */
		if (!cluster_is_permitted_for_relation(tableOid, save_userid))
		{
			relation_close(OldHeap, lmode);
			goto out;
		}

		/*
		 * Silently skip a temp table for a remote session.  Only doing this
		 * check in the "recheck" case is appropriate (which currently means
		 * somebody is executing a database-wide CLUSTER or on a partitioned
		 * table), because there is another check in cluster() which will stop
		 * any attempt to cluster remote temp tables by name.  There is
		 * another check in cluster_rel which is redundant, but we leave it
		 * for extra safety.
		 */
		if (RELATION_IS_OTHER_TEMP(OldHeap))
		{
			relation_close(OldHeap, lmode);
			goto out;
		}

		if (OidIsValid(indexOid))
		{
			/*
			 * Check that the index still exists
			 */
			if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(indexOid)))
			{
				relation_close(OldHeap, lmode);
				goto out;
			}

			/*
			 * Check that the index is still the one with indisclustered set,
			 * if needed.
			 */
			if ((params->options & CLUOPT_RECHECK_ISCLUSTERED) != 0 &&
				!get_index_isclustered(indexOid))
			{
				relation_close(OldHeap, lmode);
				goto out;
			}
		}
	}

	/*
	 * We allow VACUUM FULL, but not CLUSTER, on shared catalogs.  CLUSTER
	 * would work in most respects, but the index would only get marked as
	 * indisclustered in the current database, leading to unexpected behavior
	 * if CLUSTER were later invoked in another database.
	 */
	if (OidIsValid(indexOid) && OldHeap->rd_rel->relisshared)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot cluster a shared catalog")));
	/*
	 * The CONCURRENT case should have been rejected earlier because it does
	 * not support system catalogs.
	 */
	Assert(!(OldHeap->rd_rel->relisshared && concurrent));

	/*
	 * Don't process temp tables of other backends ... their local buffer
	 * manager is not going to cope.
	 */
	if (RELATION_IS_OTHER_TEMP(OldHeap))
	{
		if (OidIsValid(indexOid))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot cluster temporary tables of other sessions")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot vacuum temporary tables of other sessions")));
	}

	/*
	 * Also check for active uses of the relation in the current transaction,
	 * including open scans and pending AFTER trigger events.
	 */
	CheckTableNotInUse(OldHeap, OidIsValid(indexOid) ? "CLUSTER" : "VACUUM");

	/* Check heap and index are valid to cluster on */
	if (OidIsValid(indexOid))
	{
		check_index_is_clusterable(OldHeap, indexOid, lmode);
		/* Open the index (It should already be locked.) */
		index = index_open(indexOid, NoLock);
	}

	/*
	 * Quietly ignore the request if this is a materialized view which has not
	 * been populated from its query. No harm is done because there is no data
	 * to deal with, and we don't want to throw an error if this is part of a
	 * multi-relation request -- for example, CLUSTER was run on the entire
	 * database.
	 */
	if (OldHeap->rd_rel->relkind == RELKIND_MATVIEW &&
		!RelationIsPopulated(OldHeap))
	{
		index_close(index, lmode);
		relation_close(OldHeap, lmode);
		goto out;
	}

	Assert(OldHeap->rd_rel->relkind == RELKIND_RELATION ||
		   OldHeap->rd_rel->relkind == RELKIND_MATVIEW ||
		   OldHeap->rd_rel->relkind == RELKIND_TOASTVALUE);

	/*
	 * All predicate locks on the tuples or pages are about to be made
	 * invalid, because we move tuples around.  Promote them to relation
	 * locks.  Predicate locks on indexes will be promoted when they are
	 * reindexed.
	 *
	 * During concurrent processing, the heap as well as its indexes stay in
	 * operation, so we postpone this step until they are locked using
	 * AccessExclusiveLock near the end of the processing.
	 */
	if (!concurrent)
		TransferPredicateLocksToHeapRelation(OldHeap);

	/* rebuild_relation does all the dirty work */
	entered = false;
	success = false;
	PG_TRY();
	{
		/*
		 * For concurrent processing, make sure other transactions treat this
		 * table as if it was a system / user catalog, and WAL the relevant
		 * additional information. ERROR is raised if another backend is
		 * processing the same table.
		 */
		if (concurrent)
		{
			Relation	*index_p = index ? &index : NULL;

			begin_concurrent_cluster(&OldHeap, index_p, &entered);
		}

		rebuild_relation(OldHeap, index, verbose,
						 (params->options & CLUOPT_CONCURRENT) != 0);
		success = true;
	}
	PG_FINALLY();
	{
		if (concurrent && entered)
			end_concurrent_cluster(!success);
	}
	PG_END_TRY();

	/*
	 * NB: rebuild_relation does table_close() on OldHeap, and also on index,
	 * if the pointer is valid.
	 */

out:
	/* Roll back any GUC changes executed by index functions */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	pgstat_progress_end_command();
}

/*
 * Verify that the specified heap and index are valid to cluster on
 *
 * Side effect: obtains lock on the index.  The caller may
 * in some cases already have AccessExclusiveLock on the table, but
 * not in all cases so we can't rely on the table-level lock for
 * protection here.
 */
void
check_index_is_clusterable(Relation OldHeap, Oid indexOid, LOCKMODE lockmode)
{
	Relation	OldIndex;

	OldIndex = index_open(indexOid, lockmode);

	/*
	 * Check that index is in fact an index on the given relation
	 */
	if (OldIndex->rd_index == NULL ||
		OldIndex->rd_index->indrelid != RelationGetRelid(OldHeap))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index for table \"%s\"",
						RelationGetRelationName(OldIndex),
						RelationGetRelationName(OldHeap))));

	/* Index AM must allow clustering */
	if (!OldIndex->rd_indam->amclusterable)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot cluster on index \"%s\" because access method does not support clustering",
						RelationGetRelationName(OldIndex))));

	/*
	 * Disallow clustering on incomplete indexes (those that might not index
	 * every row of the relation).  We could relax this by making a separate
	 * seqscan pass over the table to copy the missing rows, but that seems
	 * expensive and tedious.
	 */
	if (!heap_attisnull(OldIndex->rd_indextuple, Anum_pg_index_indpred, NULL))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot cluster on partial index \"%s\"",
						RelationGetRelationName(OldIndex))));

	/*
	 * Disallow if index is left over from a failed CREATE INDEX CONCURRENTLY;
	 * it might well not contain entries for every heap row, or might not even
	 * be internally consistent.  (But note that we don't check indcheckxmin;
	 * the worst consequence of following broken HOT chains would be that we
	 * might put recently-dead tuples out-of-order in the new table, and there
	 * is little harm in that.)
	 */
	if (!OldIndex->rd_index->indisvalid)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot cluster on invalid index \"%s\"",
						RelationGetRelationName(OldIndex))));

	/* Drop relcache refcnt on OldIndex, but keep lock */
	index_close(OldIndex, NoLock);
}

/*
 * mark_index_clustered: mark the specified index as the one clustered on
 *
 * With indexOid == InvalidOid, will mark all indexes of rel not-clustered.
 */
void
mark_index_clustered(Relation rel, Oid indexOid, bool is_internal)
{
	HeapTuple	indexTuple;
	Form_pg_index indexForm;
	Relation	pg_index;
	ListCell   *index;

	/* Disallow applying to a partitioned table */
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot mark index clustered in partitioned table")));

	/*
	 * If the index is already marked clustered, no need to do anything.
	 */
	if (OidIsValid(indexOid))
	{
		if (get_index_isclustered(indexOid))
			return;
	}

	/*
	 * Check each index of the relation and set/clear the bit as needed.
	 */
	pg_index = table_open(IndexRelationId, RowExclusiveLock);

	foreach(index, RelationGetIndexList(rel))
	{
		Oid			thisIndexOid = lfirst_oid(index);

		indexTuple = SearchSysCacheCopy1(INDEXRELID,
										 ObjectIdGetDatum(thisIndexOid));
		if (!HeapTupleIsValid(indexTuple))
			elog(ERROR, "cache lookup failed for index %u", thisIndexOid);
		indexForm = (Form_pg_index) GETSTRUCT(indexTuple);

		/*
		 * Unset the bit if set.  We know it's wrong because we checked this
		 * earlier.
		 */
		if (indexForm->indisclustered)
		{
			indexForm->indisclustered = false;
			CatalogTupleUpdate(pg_index, &indexTuple->t_self, indexTuple);
		}
		else if (thisIndexOid == indexOid)
		{
			/* this was checked earlier, but let's be real sure */
			if (!indexForm->indisvalid)
				elog(ERROR, "cannot cluster on invalid index %u", indexOid);
			indexForm->indisclustered = true;
			CatalogTupleUpdate(pg_index, &indexTuple->t_self, indexTuple);
		}

		InvokeObjectPostAlterHookArg(IndexRelationId, thisIndexOid, 0,
									 InvalidOid, is_internal);

		heap_freetuple(indexTuple);
	}

	table_close(pg_index, RowExclusiveLock);
}

/*
 * Check if the CONCURRENTLY option is legal for the relation.
 */
bool
check_relation_is_clusterable_concurrently(Relation rel, int elevel,
										   const char *stmt)
{
	char	relpersistence, replident;
	Oid		ident_idx;

	/* Data changes in system relations are not logically decoded. */
	if (IsCatalogRelation(rel))
	{
		ereport(elevel,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot process relation \"%s\"",
						RelationGetRelationName(rel)),
				 errhint("%s is not supported for catalog relations.", stmt)));
		return false;
	}

	if (IsToastRelation(rel))
	{
		ereport(elevel,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot process relation \"%s\"",
						RelationGetRelationName(rel)),
				 errhint("%s is not supported for TOAST relations, unless the main relation is processed too.",
						 stmt)));
		return false;
	}

	relpersistence = rel->rd_rel->relpersistence;
	if (relpersistence != RELPERSISTENCE_PERMANENT)
	{
		ereport(elevel,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot process relation \"%s\"",
						RelationGetRelationName(rel)),
				 errhint("%s is only allowed for permanent relations.",
						 stmt)));
		return false;
	}

	/* With NOTHING, WAL does not contain the old tuple. */
	replident = rel->rd_rel->relreplident;
	if (replident == REPLICA_IDENTITY_NOTHING)
	{
		ereport(elevel,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot process relation \"%s\"",
						RelationGetRelationName(rel)),
				 errhint("Relation \"%s\" has insufficient replication identity.",
						 RelationGetRelationName(rel))));
		return false;
	}

	/*
	 * Identity index is not set if the replica identity is FULL, but PK might
	 * exist in such a case.
	 */
	ident_idx = RelationGetReplicaIndex(rel);
	if (!OidIsValid(ident_idx) && OidIsValid(rel->rd_pkindex))
		ident_idx = rel->rd_pkindex;
	if (!OidIsValid(ident_idx))
	{
		ereport(elevel,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot process relation \"%s\"",
						RelationGetRelationName(rel)),
				 (errhint("Relation \"%s\" has no identity index.",
						  RelationGetRelationName(rel)))));
		return false;
	}

	return true;
}

/*
 * rebuild_relation: rebuild an existing relation in index or physical order
 *
 * OldHeap: table to rebuild --- must be opened and locked. See cluster_rel()
 * for comments on the required lock strength.
 *
 * index: index to cluster by, or NULL to rewrite in physical order. Must be
 * opened and locked.
 *
 * On exit, the heap (and also the index, if one was passed) are closed, but
 * still locked with AccessExclusiveLock. (The function handles the lock
 * upgrade if 'concurrent' is true.)
 */
static void
rebuild_relation(Relation OldHeap, Relation index, bool verbose,
				 bool concurrent)
{
	Oid			tableOid = RelationGetRelid(OldHeap);
	Oid			indexOid = index ? RelationGetRelid(index) : InvalidOid;
	Oid			accessMethod = OldHeap->rd_rel->relam;
	Oid			tableSpace = OldHeap->rd_rel->reltablespace;
	Oid			OIDNewHeap;
	Relation	NewHeap;
	char		relpersistence;
	bool		swap_toast_by_content;
	TransactionId frozenXid;
	MultiXactId cutoffMulti;
	NameData	slotname;
	LogicalDecodingContext *ctx = NULL;
	Snapshot	snapshot = NULL;
	CatalogState	*cat_state = NULL;
	LOCKMODE	lmode_new;

	if (concurrent)
	{
		TupleDesc	tupdesc;
		RelReopenInfo	rri[2];
		int		nrel;

		/*
		 * CLUSTER CONCURRENTLY is not allowed in a transaction block, so this
		 * should never fire.
		 */
		Assert(GetTopTransactionIdIfAny() == InvalidTransactionId);

		/*
		 * A single backend should not execute multiple CLUSTER commands at a
		 * time, so use PID to make the slot unique.
		 */
		snprintf(NameStr(slotname), NAMEDATALEN, "cluster_%d", MyProcPid);

		/*
		 * Gather catalog information so that we can check later if the old
		 * relation has not changed while unlocked.
		 *
		 * Since this function also checks if the relation can be processed,
		 * it's important to call it before we setup the logical decoding,
		 * because that can take some time. Not sure if it's necessary to do
		 * it even earlier.
		 */
		cat_state = get_catalog_state(OldHeap);

		tupdesc = CreateTupleDescCopy(RelationGetDescr(OldHeap));

		/*
		 * Unlock the relation (and possibly the clustering index) to avoid
		 * deadlock because setup_logical_decoding() will wait for all the
		 * running transactions (with XID assigned) to finish. Some of those
		 * transactions might be waiting for a lock on our relation.
		 */
		nrel = 0;
		init_rel_reopen_info(&rri[nrel++], &OldHeap, InvalidOid,
							 LOCK_CLUSTER_CONCURRENT,
							 LOCK_CLUSTER_CONCURRENT);
		if (index)
			init_rel_reopen_info(&rri[nrel++], &index, InvalidOid,
								 LOCK_CLUSTER_CONCURRENT,
								 LOCK_CLUSTER_CONCURRENT);
		unlock_and_close_relations(rri, nrel);

		/* Prepare to capture the concurrent data changes. */
		ctx = setup_logical_decoding(tableOid, NameStr(slotname), tupdesc);

		/* Lock the table (and index) again. */
		reopen_relations(rri, nrel);

		/*
		 * Check if a 'tupdesc' could have changed while the relation was
		 * unlocked.
		 */
		check_catalog_changes(OldHeap, cat_state);

		snapshot = SnapBuildInitialSnapshotForCluster(ctx->snapshot_builder);
	}

	if (OidIsValid(indexOid))
		/* Mark the correct index as clustered */
		mark_index_clustered(OldHeap, indexOid, true);

	/* Remember info about rel before closing OldHeap */
	relpersistence = OldHeap->rd_rel->relpersistence;

	/*
	 * Create the transient table that will receive the re-ordered data.
	 *
	 * NoLock for the old heap because we already have it locked and want to
	 * keep unlocking straightforward.
	 */
	lmode_new = AccessExclusiveLock;
	OIDNewHeap = make_new_heap(tableOid, tableSpace,
							   accessMethod,
							   relpersistence,
							   NoLock, &lmode_new);
	Assert(lmode_new == AccessExclusiveLock || lmode_new == NoLock);
	/* Lock iff not done above. */
	NewHeap = table_open(OIDNewHeap, lmode_new == NoLock ?
						 AccessExclusiveLock : NoLock);

	/* Copy the heap data into the new table in the desired order */
	copy_table_data(NewHeap, OldHeap, index, snapshot, ctx, verbose,
					&swap_toast_by_content, &frozenXid, &cutoffMulti);

	if (concurrent)
	{
		rebuild_relation_finish_concurrent(NewHeap, OldHeap, index,
										   cat_state, ctx,
										   swap_toast_by_content,
										   frozenXid, cutoffMulti);

		pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE,
									 PROGRESS_CLUSTER_PHASE_FINAL_CLEANUP);

		/* Done with decoding. */
		FreeSnapshot(snapshot);
		free_catalog_state(cat_state);
		cleanup_logical_decoding(ctx);
		ReplicationSlotRelease();
		ReplicationSlotDrop(NameStr(slotname), false);
	}
	else
	{
		bool		is_system_catalog = IsSystemRelation(OldHeap);

		/* Close relcache entries, but keep lock until transaction commit */
		table_close(OldHeap, NoLock);
		if (index)
			index_close(index, NoLock);

		/*
		 * Close the new relation so it can be dropped as soon as the storage
		 * is swapped. The relation is not visible to others, so we could
		 * unlock it completely, but it's simpler to pass NoLock than to track
		 * all the lock acquired so far.
		 */
		table_close(NewHeap, NoLock);

		/*
		 * Swap the physical files of the target and transient tables, then
		 * rebuild the target's indexes and throw away the transient table.
		 */
		finish_heap_swap(tableOid, OIDNewHeap, is_system_catalog,
						 swap_toast_by_content, false, true, true,
						 frozenXid, cutoffMulti,
						 relpersistence);
	}
}


/*
 * Create the transient table that will be filled with new data during
 * CLUSTER, ALTER TABLE, and similar operations.  The transient table
 * duplicates the logical structure of the OldHeap; but will have the
 * specified physical storage properties NewTableSpace, NewAccessMethod, and
 * relpersistence.
 *
 * After this, the caller should load the new heap with transferred/modified
 * data, then call finish_heap_swap to complete the operation.
 *
 * If a specific lock mode is needed for the new relation, pass it via the
 * in/out parameter lockmode_new_p. On exit, the output value tells whether
 * the lock was actually acquired.
 */
Oid
make_new_heap(Oid OIDOldHeap, Oid NewTableSpace, Oid NewAccessMethod,
			  char relpersistence, LOCKMODE lockmode_old,
			  LOCKMODE *lockmode_new_p)
{
	TupleDesc	OldHeapDesc;
	char		NewHeapName[NAMEDATALEN];
	Oid			OIDNewHeap;
	Oid			toastid;
	Relation	OldHeap;
	HeapTuple	tuple;
	Datum		reloptions;
	bool		isNull;
	Oid			namespaceid;
	LOCKMODE	lockmode_new;

	if (lockmode_new_p)
	{
		lockmode_new = *lockmode_new_p;
		*lockmode_new_p = NoLock;
	}
	else
		lockmode_new = lockmode_old;

	OldHeap = table_open(OIDOldHeap, lockmode_old);
	OldHeapDesc = RelationGetDescr(OldHeap);

	/*
	 * Note that the NewHeap will not receive any of the defaults or
	 * constraints associated with the OldHeap; we don't need 'em, and there's
	 * no reason to spend cycles inserting them into the catalogs only to
	 * delete them.
	 */

	/*
	 * But we do want to use reloptions of the old heap for new heap.
	 */
	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(OIDOldHeap));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", OIDOldHeap);
	reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions,
								 &isNull);
	if (isNull)
		reloptions = (Datum) 0;

	if (relpersistence == RELPERSISTENCE_TEMP)
		namespaceid = LookupCreationNamespace("pg_temp");
	else
		namespaceid = RelationGetNamespace(OldHeap);

	/*
	 * Create the new heap, using a temporary name in the same namespace as
	 * the existing table.  NOTE: there is some risk of collision with user
	 * relnames.  Working around this seems more trouble than it's worth; in
	 * particular, we can't create the new heap in a different namespace from
	 * the old, or we will have problems with the TEMP status of temp tables.
	 *
	 * Note: the new heap is not a shared relation, even if we are rebuilding
	 * a shared rel.  However, we do make the new heap mapped if the source is
	 * mapped.  This simplifies swap_relation_files, and is absolutely
	 * necessary for rebuilding pg_class, for reasons explained there.
	 */
	snprintf(NewHeapName, sizeof(NewHeapName), "pg_temp_%u", OIDOldHeap);

	OIDNewHeap = heap_create_with_catalog(NewHeapName,
										  namespaceid,
										  NewTableSpace,
										  InvalidOid,
										  InvalidOid,
										  InvalidOid,
										  OldHeap->rd_rel->relowner,
										  NewAccessMethod,
										  OldHeapDesc,
										  NIL,
										  RELKIND_RELATION,
										  relpersistence,
										  false,
										  RelationIsMapped(OldHeap),
										  ONCOMMIT_NOOP,
										  reloptions,
										  false,
										  true,
										  true,
										  OIDOldHeap,
										  NULL);
	Assert(OIDNewHeap != InvalidOid);

	ReleaseSysCache(tuple);

	/*
	 * Advance command counter so that the newly-created relation's catalog
	 * tuples will be visible to table_open.
	 */
	CommandCounterIncrement();

	/*
	 * If necessary, create a TOAST table for the new relation.
	 *
	 * If the relation doesn't have a TOAST table already, we can't need one
	 * for the new relation.  The other way around is possible though: if some
	 * wide columns have been dropped, NewHeapCreateToastTable can decide that
	 * no TOAST table is needed for the new table.
	 *
	 * Note that NewHeapCreateToastTable ends with CommandCounterIncrement, so
	 * that the TOAST table will be visible for insertion.
	 */
	toastid = OldHeap->rd_rel->reltoastrelid;
	if (OidIsValid(toastid))
	{
		/* keep the existing toast table's reloptions, if any */
		tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(toastid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for relation %u", toastid);
		reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions,
									 &isNull);
		if (isNull)
			reloptions = (Datum) 0;

		NewHeapCreateToastTable(OIDNewHeap, reloptions, lockmode_new, toastid);
		if (lockmode_new_p)
			*lockmode_new_p = lockmode_new;

		ReleaseSysCache(tuple);
	}

	table_close(OldHeap, NoLock);

	return OIDNewHeap;
}

/*
 * Do the physical copying of table data.
 *
 * 'snapshot' and 'decoding_ctx': see table_relation_copy_for_cluster(). Pass
 * iff concurrent processing is required.
 *
 * There are three output parameters:
 * *pSwapToastByContent is set true if toast tables must be swapped by content.
 * *pFreezeXid receives the TransactionId used as freeze cutoff point.
 * *pCutoffMulti receives the MultiXactId used as a cutoff point.
 */
static void
copy_table_data(Relation NewHeap, Relation OldHeap, Relation OldIndex,
				Snapshot snapshot, LogicalDecodingContext *decoding_ctx,
				bool verbose, bool *pSwapToastByContent,
				TransactionId *pFreezeXid, MultiXactId *pCutoffMulti)
{
	Oid		OIDOldHeap = RelationGetRelid(OldHeap);
	Oid		OIDOldIndex = OldIndex ? RelationGetRelid(OldIndex) : InvalidOid;
	Oid		OIDNewHeap = RelationGetRelid(NewHeap);
	Relation	relRelation;
	HeapTuple	reltup;
	Form_pg_class relform;
	TupleDesc	oldTupDesc PG_USED_FOR_ASSERTS_ONLY;
	TupleDesc	newTupDesc PG_USED_FOR_ASSERTS_ONLY;
	VacuumParams params;
	struct VacuumCutoffs cutoffs;
	bool		use_sort;
	double		num_tuples = 0,
				tups_vacuumed = 0,
				tups_recently_dead = 0;
	BlockNumber num_pages;
	int			elevel = verbose ? INFO : DEBUG2;
	PGRUsage	ru0;
	char	   *nspname;
	bool		concurrent = snapshot != NULL;

	pg_rusage_init(&ru0);

	/* Store a copy of the namespace name for logging purposes */
	nspname = get_namespace_name(RelationGetNamespace(OldHeap));

	/*
	 * Their tuple descriptors should be exactly alike, but here we only need
	 * assume that they have the same number of columns.
	 */
	oldTupDesc = RelationGetDescr(OldHeap);
	newTupDesc = RelationGetDescr(NewHeap);
	Assert(newTupDesc->natts == oldTupDesc->natts);

	/*
	 * If the OldHeap has a toast table, get lock on the toast table to keep
	 * it from being vacuumed.  This is needed because autovacuum processes
	 * toast tables independently of their main tables, with no lock on the
	 * latter.  If an autovacuum were to start on the toast table after we
	 * compute our OldestXmin below, it would use a later OldestXmin, and then
	 * possibly remove as DEAD toast tuples belonging to main tuples we think
	 * are only RECENTLY_DEAD.  Then we'd fail while trying to copy those
	 * tuples.
	 *
	 * We don't need to open the toast relation here, just lock it.  The lock
	 * will be held till end of transaction.
	 *
	 * In the CONCURRENT case, the lock does not help because we need to
	 * release it temporarily at some point. Instead, we expect VACUUM /
	 * CLUSTER to skip tables which are present in ClusteredRelsHash.
	 */
	if (OldHeap->rd_rel->reltoastrelid && !concurrent)
		LockRelationOid(OldHeap->rd_rel->reltoastrelid, AccessExclusiveLock);

	/*
	 * If both tables have TOAST tables, perform toast swap by content.  It is
	 * possible that the old table has a toast table but the new one doesn't,
	 * if toastable columns have been dropped.  In that case we have to do
	 * swap by links.  This is okay because swap by content is only essential
	 * for system catalogs, and we don't support schema changes for them.
	 */
	if (OldHeap->rd_rel->reltoastrelid && NewHeap->rd_rel->reltoastrelid)
	{
		*pSwapToastByContent = true;

		/*
		 * When doing swap by content, any toast pointers written into NewHeap
		 * must use the old toast table's OID, because that's where the toast
		 * data will eventually be found.  Set this up by setting rd_toastoid.
		 * This also tells toast_save_datum() to preserve the toast value
		 * OIDs, which we want so as not to invalidate toast pointers in
		 * system catalog caches, and to avoid making multiple copies of a
		 * single toast value.
		 *
		 * Note that we must hold NewHeap open until we are done writing data,
		 * since the relcache will not guarantee to remember this setting once
		 * the relation is closed.  Also, this technique depends on the fact
		 * that no one will try to read from the NewHeap until after we've
		 * finished writing it and swapping the rels --- otherwise they could
		 * follow the toast pointers to the wrong place.  (It would actually
		 * work for values copied over from the old toast table, but not for
		 * any values that we toast which were previously not toasted.)
		 */
		NewHeap->rd_toastoid = OldHeap->rd_rel->reltoastrelid;
	}
	else
		*pSwapToastByContent = false;

	/*
	 * Compute xids used to freeze and weed out dead tuples and multixacts.
	 * Since we're going to rewrite the whole table anyway, there's no reason
	 * not to be aggressive about this.
	 */
	memset(&params, 0, sizeof(VacuumParams));
	vacuum_get_cutoffs(OldHeap, &params, &cutoffs);

	/*
	 * FreezeXid will become the table's new relfrozenxid, and that mustn't go
	 * backwards, so take the max.
	 */
	{
		TransactionId relfrozenxid = OldHeap->rd_rel->relfrozenxid;

		if (TransactionIdIsValid(relfrozenxid) &&
			TransactionIdPrecedes(cutoffs.FreezeLimit, relfrozenxid))
			cutoffs.FreezeLimit = relfrozenxid;
	}

	/*
	 * MultiXactCutoff, similarly, shouldn't go backwards either.
	 */
	{
		MultiXactId relminmxid = OldHeap->rd_rel->relminmxid;

		if (MultiXactIdIsValid(relminmxid) &&
			MultiXactIdPrecedes(cutoffs.MultiXactCutoff, relminmxid))
			cutoffs.MultiXactCutoff = relminmxid;
	}

	/*
	 * Decide whether to use an indexscan or seqscan-and-optional-sort to scan
	 * the OldHeap.  We know how to use a sort to duplicate the ordering of a
	 * btree index, and will use seqscan-and-sort for that case if the planner
	 * tells us it's cheaper.  Otherwise, always indexscan if an index is
	 * provided, else plain seqscan.
	 */
	if (OldIndex != NULL && OldIndex->rd_rel->relam == BTREE_AM_OID)
	{
		ResourceOwner	oldowner = CurrentResourceOwner;

		/*
		 * In the CONCURRENT case, do the planning in a subtransaction so that
		 * we don't leave any additional locks behind us that we cannot
		 * release easily.
		 */
		if (concurrent)
		{
			Assert(CheckRelationLockedByMe(OldHeap, LOCK_CLUSTER_CONCURRENT,
										   false));
			Assert(CheckRelationLockedByMe(OldIndex, LOCK_CLUSTER_CONCURRENT,
										   false));
			BeginInternalSubTransaction("plan_cluster_use_sort");
		}

		use_sort = plan_cluster_use_sort(OIDOldHeap, OIDOldIndex);

		if (concurrent)
		{
			PgBackendProgress	progress;

			/*
			 * Command progress reporting gets terminated at subtransaction
			 * end. Save the status so it can be eventually restored.
			 */
			memcpy(&progress, &MyBEEntry->st_progress,
				   sizeof(PgBackendProgress));

			/* Release the locks by aborting the subtransaction. */
			RollbackAndReleaseCurrentSubTransaction();

			/* Restore the progress reporting status. */
			pgstat_progress_restore_state(&progress);

			CurrentResourceOwner = oldowner;
		}
	}
	else
		use_sort = false;

	/* Log what we're doing */
	if (OldIndex != NULL && !use_sort)
		ereport(elevel,
				(errmsg("clustering \"%s.%s\" using index scan on \"%s\"",
						nspname,
						RelationGetRelationName(OldHeap),
						RelationGetRelationName(OldIndex))));
	else if (use_sort)
		ereport(elevel,
				(errmsg("clustering \"%s.%s\" using sequential scan and sort",
						nspname,
						RelationGetRelationName(OldHeap))));
	else
		ereport(elevel,
				(errmsg("vacuuming \"%s.%s\"",
						nspname,
						RelationGetRelationName(OldHeap))));

	/*
	 * Hand off the actual copying to AM specific function, the generic code
	 * cannot know how to deal with visibility across AMs. Note that this
	 * routine is allowed to set FreezeXid / MultiXactCutoff to different
	 * values (e.g. because the AM doesn't use freezing).
	 */
	table_relation_copy_for_cluster(OldHeap, NewHeap, OldIndex, use_sort,
									cutoffs.OldestXmin, snapshot,
									decoding_ctx,
									&cutoffs.FreezeLimit,
									&cutoffs.MultiXactCutoff,
									&num_tuples, &tups_vacuumed,
									&tups_recently_dead);

	/* return selected values to caller, get set as relfrozenxid/minmxid */
	*pFreezeXid = cutoffs.FreezeLimit;
	*pCutoffMulti = cutoffs.MultiXactCutoff;

	/*
	 * Reset rd_toastoid just to be tidy --- it shouldn't be looked at
	 * again. In the CONCURRENT case, we need to set it again before applying
	 * the concurrent changes.
	 */
	NewHeap->rd_toastoid = InvalidOid;

	num_pages = RelationGetNumberOfBlocks(NewHeap);

	/* Log what we did */
	ereport(elevel,
			(errmsg("\"%s.%s\": found %.0f removable, %.0f nonremovable row versions in %u pages",
					nspname,
					RelationGetRelationName(OldHeap),
					tups_vacuumed, num_tuples,
					RelationGetNumberOfBlocks(OldHeap)),
			 errdetail("%.0f dead row versions cannot be removed yet.\n"
					   "%s.",
					   tups_recently_dead,
					   pg_rusage_show(&ru0))));

	/* Update pg_class to reflect the correct values of pages and tuples. */
	relRelation = table_open(RelationRelationId, RowExclusiveLock);

	reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(OIDNewHeap));
	if (!HeapTupleIsValid(reltup))
		elog(ERROR, "cache lookup failed for relation %u", OIDNewHeap);
	relform = (Form_pg_class) GETSTRUCT(reltup);

	relform->relpages = num_pages;
	relform->reltuples = num_tuples;

	/* Don't update the stats for pg_class.  See swap_relation_files. */
	if (OIDOldHeap != RelationRelationId)
		CatalogTupleUpdate(relRelation, &reltup->t_self, reltup);
	else
		CacheInvalidateRelcacheByTuple(reltup);

	/* Clean up. */
	heap_freetuple(reltup);
	table_close(relRelation, RowExclusiveLock);

	/* Make the update visible */
	CommandCounterIncrement();
}

/*
 * Swap the physical files of two given relations.
 *
 * We swap the physical identity (reltablespace, relfilenumber) while keeping
 * the same logical identities of the two relations.  relpersistence is also
 * swapped, which is critical since it determines where buffers live for each
 * relation.
 *
 * We can swap associated TOAST data in either of two ways: recursively swap
 * the physical content of the toast tables (and their indexes), or swap the
 * TOAST links in the given relations' pg_class entries.  The former is needed
 * to manage rewrites of shared catalogs (where we cannot change the pg_class
 * links) while the latter is the only way to handle cases in which a toast
 * table is added or removed altogether.
 *
 * Additionally, the first relation is marked with relfrozenxid set to
 * frozenXid.  It seems a bit ugly to have this here, but the caller would
 * have to do it anyway, so having it here saves a heap_update.  Note: in
 * the swap-toast-links case, we assume we don't need to change the toast
 * table's relfrozenxid: the new version of the toast table should already
 * have relfrozenxid set to RecentXmin, which is good enough.
 *
 * Lastly, if r2 and its toast table and toast index (if any) are mapped,
 * their OIDs are emitted into mapped_tables[].  This is hacky but beats
 * having to look the information up again later in finish_heap_swap.
 */
static void
swap_relation_files(Oid r1, Oid r2, bool target_is_pg_class,
					bool swap_toast_by_content,
					bool is_internal,
					TransactionId frozenXid,
					MultiXactId cutoffMulti,
					Oid *mapped_tables)
{
	Relation	relRelation;
	HeapTuple	reltup1,
				reltup2;
	Form_pg_class relform1,
				relform2;
	RelFileNumber relfilenumber1,
				relfilenumber2;
	RelFileNumber swaptemp;
	char		swptmpchr;
	Oid			relam1,
				relam2;

	/* We need writable copies of both pg_class tuples. */
	relRelation = table_open(RelationRelationId, RowExclusiveLock);

	reltup1 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r1));
	if (!HeapTupleIsValid(reltup1))
		elog(ERROR, "cache lookup failed for relation %u", r1);
	relform1 = (Form_pg_class) GETSTRUCT(reltup1);

	reltup2 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r2));
	if (!HeapTupleIsValid(reltup2))
		elog(ERROR, "cache lookup failed for relation %u", r2);
	relform2 = (Form_pg_class) GETSTRUCT(reltup2);

	relfilenumber1 = relform1->relfilenode;
	relfilenumber2 = relform2->relfilenode;
	relam1 = relform1->relam;
	relam2 = relform2->relam;

	if (RelFileNumberIsValid(relfilenumber1) &&
		RelFileNumberIsValid(relfilenumber2))
	{
		/*
		 * Normal non-mapped relations: swap relfilenumbers, reltablespaces,
		 * relpersistence
		 */
		Assert(!target_is_pg_class);

		swaptemp = relform1->relfilenode;
		relform1->relfilenode = relform2->relfilenode;
		relform2->relfilenode = swaptemp;

		swaptemp = relform1->reltablespace;
		relform1->reltablespace = relform2->reltablespace;
		relform2->reltablespace = swaptemp;

		swaptemp = relform1->relam;
		relform1->relam = relform2->relam;
		relform2->relam = swaptemp;

		swptmpchr = relform1->relpersistence;
		relform1->relpersistence = relform2->relpersistence;
		relform2->relpersistence = swptmpchr;

		/* Also swap toast links, if we're swapping by links */
		if (!swap_toast_by_content)
		{
			swaptemp = relform1->reltoastrelid;
			relform1->reltoastrelid = relform2->reltoastrelid;
			relform2->reltoastrelid = swaptemp;
		}
	}
	else
	{
		/*
		 * Mapped-relation case.  Here we have to swap the relation mappings
		 * instead of modifying the pg_class columns.  Both must be mapped.
		 */
		if (RelFileNumberIsValid(relfilenumber1) ||
			RelFileNumberIsValid(relfilenumber2))
			elog(ERROR, "cannot swap mapped relation \"%s\" with non-mapped relation",
				 NameStr(relform1->relname));

		/*
		 * We can't change the tablespace nor persistence of a mapped rel, and
		 * we can't handle toast link swapping for one either, because we must
		 * not apply any critical changes to its pg_class row.  These cases
		 * should be prevented by upstream permissions tests, so these checks
		 * are non-user-facing emergency backstop.
		 */
		if (relform1->reltablespace != relform2->reltablespace)
			elog(ERROR, "cannot change tablespace of mapped relation \"%s\"",
				 NameStr(relform1->relname));
		if (relform1->relpersistence != relform2->relpersistence)
			elog(ERROR, "cannot change persistence of mapped relation \"%s\"",
				 NameStr(relform1->relname));
		if (relform1->relam != relform2->relam)
			elog(ERROR, "cannot change access method of mapped relation \"%s\"",
				 NameStr(relform1->relname));
		if (!swap_toast_by_content &&
			(relform1->reltoastrelid || relform2->reltoastrelid))
			elog(ERROR, "cannot swap toast by links for mapped relation \"%s\"",
				 NameStr(relform1->relname));

		/*
		 * Fetch the mappings --- shouldn't fail, but be paranoid
		 */
		relfilenumber1 = RelationMapOidToFilenumber(r1, relform1->relisshared);
		if (!RelFileNumberIsValid(relfilenumber1))
			elog(ERROR, "could not find relation mapping for relation \"%s\", OID %u",
				 NameStr(relform1->relname), r1);
		relfilenumber2 = RelationMapOidToFilenumber(r2, relform2->relisshared);
		if (!RelFileNumberIsValid(relfilenumber2))
			elog(ERROR, "could not find relation mapping for relation \"%s\", OID %u",
				 NameStr(relform2->relname), r2);

		/*
		 * Send replacement mappings to relmapper.  Note these won't actually
		 * take effect until CommandCounterIncrement.
		 */
		RelationMapUpdateMap(r1, relfilenumber2, relform1->relisshared, false);
		RelationMapUpdateMap(r2, relfilenumber1, relform2->relisshared, false);

		/* Pass OIDs of mapped r2 tables back to caller */
		*mapped_tables++ = r2;
	}

	/*
	 * Recognize that rel1's relfilenumber (swapped from rel2) is new in this
	 * subtransaction. The rel2 storage (swapped from rel1) may or may not be
	 * new.
	 */
	{
		Relation	rel1,
					rel2;

		rel1 = relation_open(r1, NoLock);
		rel2 = relation_open(r2, NoLock);
		rel2->rd_createSubid = rel1->rd_createSubid;
		rel2->rd_newRelfilelocatorSubid = rel1->rd_newRelfilelocatorSubid;
		rel2->rd_firstRelfilelocatorSubid = rel1->rd_firstRelfilelocatorSubid;
		RelationAssumeNewRelfilelocator(rel1);
		relation_close(rel1, NoLock);
		relation_close(rel2, NoLock);
	}

	/*
	 * In the case of a shared catalog, these next few steps will only affect
	 * our own database's pg_class row; but that's okay, because they are all
	 * noncritical updates.  That's also an important fact for the case of a
	 * mapped catalog, because it's possible that we'll commit the map change
	 * and then fail to commit the pg_class update.
	 */

	/* set rel1's frozen Xid and minimum MultiXid */
	if (relform1->relkind != RELKIND_INDEX)
	{
		Assert(!TransactionIdIsValid(frozenXid) ||
			   TransactionIdIsNormal(frozenXid));
		relform1->relfrozenxid = frozenXid;
		relform1->relminmxid = cutoffMulti;
	}

	/* swap size statistics too, since new rel has freshly-updated stats */
	{
		int32		swap_pages;
		float4		swap_tuples;
		int32		swap_allvisible;

		swap_pages = relform1->relpages;
		relform1->relpages = relform2->relpages;
		relform2->relpages = swap_pages;

		swap_tuples = relform1->reltuples;
		relform1->reltuples = relform2->reltuples;
		relform2->reltuples = swap_tuples;

		swap_allvisible = relform1->relallvisible;
		relform1->relallvisible = relform2->relallvisible;
		relform2->relallvisible = swap_allvisible;
	}

	/*
	 * Update the tuples in pg_class --- unless the target relation of the
	 * swap is pg_class itself.  In that case, there is zero point in making
	 * changes because we'd be updating the old data that we're about to throw
	 * away.  Because the real work being done here for a mapped relation is
	 * just to change the relation map settings, it's all right to not update
	 * the pg_class rows in this case. The most important changes will instead
	 * performed later, in finish_heap_swap() itself.
	 */
	if (!target_is_pg_class)
	{
		CatalogIndexState indstate;

		indstate = CatalogOpenIndexes(relRelation);
		CatalogTupleUpdateWithInfo(relRelation, &reltup1->t_self, reltup1,
								   indstate);
		CatalogTupleUpdateWithInfo(relRelation, &reltup2->t_self, reltup2,
								   indstate);
		CatalogCloseIndexes(indstate);
	}
	else
	{
		/* no update ... but we do still need relcache inval */
		CacheInvalidateRelcacheByTuple(reltup1);
		CacheInvalidateRelcacheByTuple(reltup2);
	}

	/*
	 * Now that pg_class has been updated with its relevant information for
	 * the swap, update the dependency of the relations to point to their new
	 * table AM, if it has changed.
	 */
	if (relam1 != relam2)
	{
		if (changeDependencyFor(RelationRelationId,
								r1,
								AccessMethodRelationId,
								relam1,
								relam2) != 1)
			elog(ERROR, "could not change access method dependency for relation \"%s.%s\"",
				 get_namespace_name(get_rel_namespace(r1)),
				 get_rel_name(r1));
		if (changeDependencyFor(RelationRelationId,
								r2,
								AccessMethodRelationId,
								relam2,
								relam1) != 1)
			elog(ERROR, "could not change access method dependency for relation \"%s.%s\"",
				 get_namespace_name(get_rel_namespace(r2)),
				 get_rel_name(r2));
	}

	/*
	 * Post alter hook for modified relations. The change to r2 is always
	 * internal, but r1 depends on the invocation context.
	 */
	InvokeObjectPostAlterHookArg(RelationRelationId, r1, 0,
								 InvalidOid, is_internal);
	InvokeObjectPostAlterHookArg(RelationRelationId, r2, 0,
								 InvalidOid, true);

	/*
	 * If we have toast tables associated with the relations being swapped,
	 * deal with them too.
	 */
	if (relform1->reltoastrelid || relform2->reltoastrelid)
	{
		if (swap_toast_by_content)
		{
			if (relform1->reltoastrelid && relform2->reltoastrelid)
			{
				/* Recursively swap the contents of the toast tables */
				swap_relation_files(relform1->reltoastrelid,
									relform2->reltoastrelid,
									target_is_pg_class,
									swap_toast_by_content,
									is_internal,
									frozenXid,
									cutoffMulti,
									mapped_tables);
			}
			else
			{
				/* caller messed up */
				elog(ERROR, "cannot swap toast files by content when there's only one");
			}
		}
		else
		{
			/*
			 * We swapped the ownership links, so we need to change dependency
			 * data to match.
			 *
			 * NOTE: it is possible that only one table has a toast table.
			 *
			 * NOTE: at present, a TOAST table's only dependency is the one on
			 * its owning table.  If more are ever created, we'd need to use
			 * something more selective than deleteDependencyRecordsFor() to
			 * get rid of just the link we want.
			 */
			ObjectAddress baseobject,
						toastobject;
			long		count;

			/*
			 * We disallow this case for system catalogs, to avoid the
			 * possibility that the catalog we're rebuilding is one of the
			 * ones the dependency changes would change.  It's too late to be
			 * making any data changes to the target catalog.
			 */
			if (IsSystemClass(r1, relform1))
				elog(ERROR, "cannot swap toast files by links for system catalogs");

			/* Delete old dependencies */
			if (relform1->reltoastrelid)
			{
				count = deleteDependencyRecordsFor(RelationRelationId,
												   relform1->reltoastrelid,
												   false);
				if (count != 1)
					elog(ERROR, "expected one dependency record for TOAST table, found %ld",
						 count);
			}
			if (relform2->reltoastrelid)
			{
				count = deleteDependencyRecordsFor(RelationRelationId,
												   relform2->reltoastrelid,
												   false);
				if (count != 1)
					elog(ERROR, "expected one dependency record for TOAST table, found %ld",
						 count);
			}

			/* Register new dependencies */
			baseobject.classId = RelationRelationId;
			baseobject.objectSubId = 0;
			toastobject.classId = RelationRelationId;
			toastobject.objectSubId = 0;

			if (relform1->reltoastrelid)
			{
				baseobject.objectId = r1;
				toastobject.objectId = relform1->reltoastrelid;
				recordDependencyOn(&toastobject, &baseobject,
								   DEPENDENCY_INTERNAL);
			}

			if (relform2->reltoastrelid)
			{
				baseobject.objectId = r2;
				toastobject.objectId = relform2->reltoastrelid;
				recordDependencyOn(&toastobject, &baseobject,
								   DEPENDENCY_INTERNAL);
			}
		}
	}

	/*
	 * If we're swapping two toast tables by content, do the same for their
	 * valid index. The swap can actually be safely done only if the relations
	 * have indexes.
	 */
	if (swap_toast_by_content &&
		relform1->relkind == RELKIND_TOASTVALUE &&
		relform2->relkind == RELKIND_TOASTVALUE)
	{
		Oid			toastIndex1,
					toastIndex2;

		/* Get valid index for each relation */
		toastIndex1 = toast_get_valid_index(r1,
											AccessExclusiveLock);
		toastIndex2 = toast_get_valid_index(r2,
											AccessExclusiveLock);

		swap_relation_files(toastIndex1,
							toastIndex2,
							target_is_pg_class,
							swap_toast_by_content,
							is_internal,
							InvalidTransactionId,
							InvalidMultiXactId,
							mapped_tables);
	}

	/* Clean up. */
	heap_freetuple(reltup1);
	heap_freetuple(reltup2);

	table_close(relRelation, RowExclusiveLock);
}

/*
 * Remove the transient table that was built by make_new_heap, and finish
 * cleaning up (including rebuilding all indexes on the old heap).
 */
void
finish_heap_swap(Oid OIDOldHeap, Oid OIDNewHeap,
				 bool is_system_catalog,
				 bool swap_toast_by_content,
				 bool check_constraints,
				 bool is_internal,
				 bool reindex,
				 TransactionId frozenXid,
				 MultiXactId cutoffMulti,
				 char newrelpersistence)
{
	ObjectAddress object;
	Oid			mapped_tables[4];
	int			i;

	/* Report that we are now swapping relation files */
	pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE,
								 PROGRESS_CLUSTER_PHASE_SWAP_REL_FILES);

	/* Zero out possible results from swapped_relation_files */
	memset(mapped_tables, 0, sizeof(mapped_tables));

	/*
	 * Swap the contents of the heap relations (including any toast tables).
	 * Also set old heap's relfrozenxid to frozenXid.
	 */
	swap_relation_files(OIDOldHeap, OIDNewHeap,
						(OIDOldHeap == RelationRelationId),
						swap_toast_by_content, is_internal,
						frozenXid, cutoffMulti, mapped_tables);

	/*
	 * If it's a system catalog, queue a sinval message to flush all catcaches
	 * on the catalog when we reach CommandCounterIncrement.
	 */
	if (is_system_catalog)
		CacheInvalidateCatalog(OIDOldHeap);

	if (reindex)
	{
		int			reindex_flags;
		ReindexParams reindex_params = {0};

		/*
		 * Rebuild each index on the relation (but not the toast table, which
		 * is all-new at this point).  It is important to do this before the
		 * DROP step because if we are processing a system catalog that will
		 * be used during DROP, we want to have its indexes available.  There
		 * is no advantage to the other order anyway because this is all
		 * transactional, so no chance to reclaim disk space before commit.
		 * We do not need a final CommandCounterIncrement() because
		 * reindex_relation does it.
		 *
		 * Note: because index_build is called via reindex_relation, it will never
		 * set indcheckxmin true for the indexes.  This is OK even though in some
		 * sense we are building new indexes rather than rebuilding existing ones,
		 * because the new heap won't contain any HOT chains at all, let alone
		 * broken ones, so it can't be necessary to set indcheckxmin.
		 */
		reindex_flags = REINDEX_REL_SUPPRESS_INDEX_USE;
		if (check_constraints)
			reindex_flags |= REINDEX_REL_CHECK_CONSTRAINTS;

		/*
		 * Ensure that the indexes have the same persistence as the parent
		 * relation.
		 */
		if (newrelpersistence == RELPERSISTENCE_UNLOGGED)
			reindex_flags |= REINDEX_REL_FORCE_INDEXES_UNLOGGED;
		else if (newrelpersistence == RELPERSISTENCE_PERMANENT)
			reindex_flags |= REINDEX_REL_FORCE_INDEXES_PERMANENT;

		/* Report that we are now reindexing relations */
		pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE,
									 PROGRESS_CLUSTER_PHASE_REBUILD_INDEX);

		reindex_relation(NULL, OIDOldHeap, reindex_flags, &reindex_params);
	}

	/* Report that we are now doing clean up */
	pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE,
								 PROGRESS_CLUSTER_PHASE_FINAL_CLEANUP);

	/*
	 * If the relation being rebuilt is pg_class, swap_relation_files()
	 * couldn't update pg_class's own pg_class entry (check comments in
	 * swap_relation_files()), thus relfrozenxid was not updated. That's
	 * annoying because a potential reason for doing a VACUUM FULL is a
	 * imminent or actual anti-wraparound shutdown.  So, now that we can
	 * access the new relation using its indices, update relfrozenxid.
	 * pg_class doesn't have a toast relation, so we don't need to update the
	 * corresponding toast relation. Not that there's little point moving all
	 * relfrozenxid updates here since swap_relation_files() needs to write to
	 * pg_class for non-mapped relations anyway.
	 */
	if (OIDOldHeap == RelationRelationId)
	{
		Relation	relRelation;
		HeapTuple	reltup;
		Form_pg_class relform;

		relRelation = table_open(RelationRelationId, RowExclusiveLock);

		reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(OIDOldHeap));
		if (!HeapTupleIsValid(reltup))
			elog(ERROR, "cache lookup failed for relation %u", OIDOldHeap);
		relform = (Form_pg_class) GETSTRUCT(reltup);

		relform->relfrozenxid = frozenXid;
		relform->relminmxid = cutoffMulti;

		CatalogTupleUpdate(relRelation, &reltup->t_self, reltup);

		table_close(relRelation, RowExclusiveLock);
	}

	/* Destroy new heap with old filenumber */
	object.classId = RelationRelationId;
	object.objectId = OIDNewHeap;
	object.objectSubId = 0;

	/*
	 * The new relation is local to our transaction and we know nothing
	 * depends on it, so DROP_RESTRICT should be OK.
	 */
	performDeletion(&object, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

	/* performDeletion does CommandCounterIncrement at end */

	/*
	 * Now we must remove any relation mapping entries that we set up for the
	 * transient table, as well as its toast table and toast index if any. If
	 * we fail to do this before commit, the relmapper will complain about new
	 * permanent map entries being added post-bootstrap.
	 */
	for (i = 0; OidIsValid(mapped_tables[i]); i++)
		RelationMapRemoveMapping(mapped_tables[i]);

	/*
	 * At this point, everything is kosher except that, if we did toast swap
	 * by links, the toast table's name corresponds to the transient table.
	 * The name is irrelevant to the backend because it's referenced by OID,
	 * but users looking at the catalogs could be confused.  Rename it to
	 * prevent this problem.
	 *
	 * Note no lock required on the relation, because we already hold an
	 * exclusive lock on it.
	 */
	if (!swap_toast_by_content)
	{
		Relation	newrel;

		newrel = table_open(OIDOldHeap, NoLock);
		if (OidIsValid(newrel->rd_rel->reltoastrelid))
		{
			Oid			toastidx;
			char		NewToastName[NAMEDATALEN];

			/* Get the associated valid index to be renamed */
			toastidx = toast_get_valid_index(newrel->rd_rel->reltoastrelid,
											 NoLock);

			/* rename the toast table ... */
			snprintf(NewToastName, NAMEDATALEN, "pg_toast_%u",
					 OIDOldHeap);
			RenameRelationInternal(newrel->rd_rel->reltoastrelid,
								   NewToastName, true, false);

			/* ... and its valid index too. */
			snprintf(NewToastName, NAMEDATALEN, "pg_toast_%u_index",
					 OIDOldHeap);

			RenameRelationInternal(toastidx,
								   NewToastName, true, true);

			/*
			 * Reset the relrewrite for the toast. The command-counter
			 * increment is required here as we are about to update the tuple
			 * that is updated as part of RenameRelationInternal.
			 */
			CommandCounterIncrement();
			ResetRelRewrite(newrel->rd_rel->reltoastrelid);
		}
		relation_close(newrel, NoLock);
	}

	/* if it's not a catalog table, clear any missing attribute settings */
	if (!is_system_catalog)
	{
		Relation	newrel;

		newrel = table_open(OIDOldHeap, NoLock);
		RelationClearMissing(newrel);
		relation_close(newrel, NoLock);
	}
}


/*
 * Get a list of tables that the current user has privileges on and
 * have indisclustered set.  Return the list in a List * of RelToCluster
 * (stored in the specified memory context), each one giving the tableOid
 * and the indexOid on which the table is already clustered.
 */
static List *
get_tables_to_cluster(MemoryContext cluster_context)
{
	Relation	indRelation;
	TableScanDesc scan;
	ScanKeyData entry;
	HeapTuple	indexTuple;
	Form_pg_index index;
	MemoryContext old_context;
	List	   *rtcs = NIL;

	/*
	 * Get all indexes that have indisclustered set and that the current user
	 * has the appropriate privileges for.
	 */
	indRelation = table_open(IndexRelationId, AccessShareLock);
	ScanKeyInit(&entry,
				Anum_pg_index_indisclustered,
				BTEqualStrategyNumber, F_BOOLEQ,
				BoolGetDatum(true));
	scan = table_beginscan_catalog(indRelation, 1, &entry);
	while ((indexTuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		RelToCluster *rtc;

		index = (Form_pg_index) GETSTRUCT(indexTuple);

		if (!cluster_is_permitted_for_relation(index->indrelid, GetUserId()))
			continue;

		/* Use a permanent memory context for the result list */
		old_context = MemoryContextSwitchTo(cluster_context);

		rtc = (RelToCluster *) palloc(sizeof(RelToCluster));
		rtc->tableOid = index->indrelid;
		rtc->indexOid = index->indexrelid;
		rtcs = lappend(rtcs, rtc);

		MemoryContextSwitchTo(old_context);
	}
	table_endscan(scan);

	relation_close(indRelation, AccessShareLock);

	return rtcs;
}

/*
 * Given an index on a partitioned table, return a list of RelToCluster for
 * all the children leaves tables/indexes.
 *
 * Like expand_vacuum_rel, but here caller must hold AccessExclusiveLock
 * on the table containing the index.
 */
static List *
get_tables_to_cluster_partitioned(MemoryContext cluster_context, Oid indexOid)
{
	List	   *inhoids;
	ListCell   *lc;
	List	   *rtcs = NIL;
	MemoryContext old_context;

	/* Do not lock the children until they're processed */
	inhoids = find_all_inheritors(indexOid, NoLock, NULL);

	foreach(lc, inhoids)
	{
		Oid			indexrelid = lfirst_oid(lc);
		Oid			relid = IndexGetRelation(indexrelid, false);
		RelToCluster *rtc;

		/* consider only leaf indexes */
		if (get_rel_relkind(indexrelid) != RELKIND_INDEX)
			continue;

		/*
		 * It's possible that the user does not have privileges to CLUSTER the
		 * leaf partition despite having such privileges on the partitioned
		 * table.  We skip any partitions which the user is not permitted to
		 * CLUSTER.
		 */
		if (!cluster_is_permitted_for_relation(relid, GetUserId()))
			continue;

		/* Use a permanent memory context for the result list */
		old_context = MemoryContextSwitchTo(cluster_context);

		rtc = (RelToCluster *) palloc(sizeof(RelToCluster));
		rtc->tableOid = relid;
		rtc->indexOid = indexrelid;
		rtcs = lappend(rtcs, rtc);

		MemoryContextSwitchTo(old_context);
	}

	return rtcs;
}

/*
 * Return whether userid has privileges to CLUSTER relid.  If not, this
 * function emits a WARNING.
 */
static bool
cluster_is_permitted_for_relation(Oid relid, Oid userid)
{
	if (pg_class_aclcheck(relid, userid, ACL_MAINTAIN) == ACLCHECK_OK)
		return true;

	ereport(WARNING,
			(errmsg("permission denied to cluster \"%s\", skipping it",
					get_rel_name(relid))));
	return false;
}

#define REPL_PLUGIN_NAME	"pgoutput_cluster"

/*
 * Each relation being processed by CLUSTER CONCURRENTLY must be in the
 * clusteredRels hashtable.
 */
typedef struct ClusteredRel
{
	Oid		relid;
	Oid		dbid;
} ClusteredRel;

static HTAB *ClusteredRelsHash = NULL;

/* Maximum number of entries in the hashtable. */
static int maxClusteredRels = 0;

Size
ClusterShmemSize(void)
{
	/*
	 * A replication slot is needed for the processing, so use this GUC to
	 * allocate memory for the hashtable. Reserve also space for TOAST
	 * relations.
	 */
	maxClusteredRels = max_replication_slots * 2;

	return hash_estimate_size(maxClusteredRels, sizeof(ClusteredRel));
}

void
ClusterShmemInit(void)
{
	HASHCTL		info;

	info.keysize = sizeof(ClusteredRel);
	info.entrysize = info.keysize;

	ClusteredRelsHash = ShmemInitHash("Clustered Relations",
									  maxClusteredRels,
									  maxClusteredRels,
									  &info,
									  HASH_ELEM | HASH_BLOBS);
}

/*
 * Perform a preliminary check whether CLUSTER / VACUUM FULL CONCURRENTLY is
 * possible. Note that here we only check things that should not change if we
 * release the relation lock temporarily. The information that can change due
 * to unlocking is checked in get_catalog_state().
 */
static void
check_concurrent_cluster_requirements(Relation rel, bool isTopLevel,
									  bool isCluster)
{
	const char	*stmt;

	if (isCluster)
		stmt = "CLUSTER (CONCURRENTLY)";
	else
		stmt = "VACUUM (FULL, CONCURRENTLY)";

	/*
	 * Make sure we have no XID assigned, otherwise call of
	 * setup_logical_decoding() can cause a deadlock.
	 */
	PreventInTransactionBlock(isTopLevel, stmt);

	CheckSlotPermissions();

	/*
	 * Use an existing function to check if we can use logical
	 * decoding. However note that RecoveryInProgress() should already have
	 * caused error, as it does for the non-concurrent VACUUM FULL / CLUSTER.
	 */
	CheckLogicalDecodingRequirements();

	/* See ClusterShmemSize() */
	if (max_replication_slots < 2)
		ereport(ERROR,
				errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				(errmsg("%s requires \"max_replication_slots\" to be at least 2",
						stmt)));
}

/*
 * Call this function before CLUSTER CONCURRENTLY starts to setup logical
 * decoding. It makes sure that other users of the table put enough
 * information into WAL.
 *
 * The point is that on various places we expect that the table we're
 * processing is treated like a system catalog. For example, we need to be
 * able to scan it using a "historic snapshot" anytime during the processing
 * (as opposed to scanning only at the start point of the decoding, logical
 * replication does during initial table synchronization), in order to apply
 * concurrent UPDATE / DELETE commands.
 *
 * Since we need to close and reopen the relation here, the 'rel_p' and
 * 'index_p' arguments are in/out.
 *
 * 'enter_p' receives a bool value telling whether relation OID was entered
 * into the hashtable or not.
 */
static void
begin_concurrent_cluster(Relation *rel_p, Relation *index_p,
						 bool *entered_p)
{
	Relation	rel = *rel_p;
	Oid		relid, toastrelid;
	ClusteredRel	key, *entry;
	bool	found;
	RelReopenInfo	rri[2];
	int		nrel;
	static bool before_shmem_exit_callback_setup = false;

	relid = RelationGetRelid(rel);

	/*
	 * Make sure that we do not leave an entry in ClusteredRelsHash if exiting
	 * due to FATAL.
	 */
	if (!before_shmem_exit_callback_setup)
	{
		before_shmem_exit(cluster_before_shmem_exit_callback, 0);
		before_shmem_exit_callback_setup = true;
	}

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	key.dbid = MyDatabaseId;

	*entered_p = false;
	LWLockAcquire(ClusteredRelsLock, LW_EXCLUSIVE);
	entry = (ClusteredRel *)
		hash_search(ClusteredRelsHash, &key, HASH_ENTER_NULL, &found);
	if (found)
	{
		/*
		 * Since CLUSTER CONCURRENTLY takes ShareRowExclusiveLock, a conflict
		 * should occur much earlier. However that lock may be released
		 * temporarily, see below.  Anyway, we should complain whatever the
		 * reason of the conflict might be.
		 */
		ereport(ERROR,
				(errmsg(CLUSTER_IN_PROGRESS_MESSAGE,
						RelationGetRelationName(rel))));
	}
	if (entry == NULL)
		ereport(ERROR,
				(errmsg("too many requests for CLUSTER CONCURRENTLY at a time")),
				(errhint("Please consider increasing the \"max_replication_slots\" configuration parameter.")));

	/*
	 * Even if the insertion of TOAST relid should fail below, the caller has
	 * to do cleanup.
	 */
	*entered_p = true;

	/*
	 * Enable the callback to remove the entry in case of exit. We should not
	 * do this earlier, otherwise an attempt to insert already existing entry
	 * could make us remove that entry (inserted by another backend) during
	 * ERROR handling.
	 */
	Assert(!OidIsValid(clustered_rel));
	clustered_rel = relid;

	/*
	 * TOAST relation is not accessed using historic snapshot, but we enter it
	 * here to protect it from being VACUUMed by another backend. (Lock does
	 * not help in the CONCURRENT case because cannot hold it continuously
	 * till the end of the transaction.) See the comments on locking TOAST
	 * relation in copy_table_data().
	 */
	toastrelid = rel->rd_rel->reltoastrelid;
	if (OidIsValid(toastrelid))
	{
		key.relid = toastrelid;
		entry = (ClusteredRel *)
			hash_search(ClusteredRelsHash, &key, HASH_ENTER_NULL, &found);
		if (found)
			/*
			 * If we could enter the main fork the TOAST should succeed
			 * too. Nevertheless, check.
			 */
			ereport(ERROR,
					(errmsg("TOAST relation of \"%s\" is already being processed by CLUSTER CONCURRENTLY",
							RelationGetRelationName(rel))));
		if (entry == NULL)
			ereport(ERROR,
					(errmsg("too many requests for CLUSTER CONCURRENT at a time")),
					(errhint("Please consider increasing the \"max_replication_slots\" configuration parameter.")));

		Assert(!OidIsValid(clustered_rel_toast));
		clustered_rel_toast = toastrelid;
	}
	LWLockRelease(ClusteredRelsLock);

	/*
	 * Make sure that other backends are aware of the new hash entry.
	 *
	 * Besides sending the invalidation message, we need to force re-opening
	 * of the relation, which includes the actual invalidation (and thus
	 * checking of our hashtable on the next access).
	 */
	CacheInvalidateRelcacheImmediate(rel);
	/*
	 * Since the hashtable only needs to be checked by write transactions,
	 * lock the relation in a mode that conflicts with any DML command. (The
	 * reading transactions are supposed to close the relation before opening
	 * it with higher lock.) Once we have the relation (and its index) locked,
	 * we unlock it immediately and then re-lock using the original mode.
	 */
	nrel = 0;
	init_rel_reopen_info(&rri[nrel++], rel_p, InvalidOid,
						 LOCK_CLUSTER_CONCURRENT, ShareLock);
	if (index_p)
	{
		/*
		 * Another transaction might want to open both the relation and the
		 * index. If it already has the relation lock and is waiting for the
		 * index lock, we should release the index lock, otherwise our request
		 * for ShareLock on the relation can end up in a deadlock.
		 */
		init_rel_reopen_info(&rri[nrel++], index_p, InvalidOid,
							 LOCK_CLUSTER_CONCURRENT, ShareLock);
	}
	unlock_and_close_relations(rri, nrel);
	/*
	 * XXX It's not strictly necessary to lock the index here, but it's
	 * probably not worth teaching the "reopen API" about this special case.
	 */
	reopen_relations(rri, nrel);

	/* Switch back to the original lock. */
	nrel = 0;
	init_rel_reopen_info(&rri[nrel++], rel_p, InvalidOid,
						 ShareLock, LOCK_CLUSTER_CONCURRENT);
	if (index_p)
		init_rel_reopen_info(&rri[nrel++], index_p, InvalidOid,
							 ShareLock, LOCK_CLUSTER_CONCURRENT);
	unlock_and_close_relations(rri, nrel);
	reopen_relations(rri, nrel);

	/* Avoid logical decoding of other relations by this backend. */
	clustered_rel_locator = rel->rd_locator;
	if (OidIsValid(toastrelid))
	{
		Relation	toastrel;

		/* Avoid logical decoding of other TOAST relations. */
		toastrel = table_open(toastrelid, AccessShareLock);
		clustered_rel_toast_locator = toastrel->rd_locator;
		table_close(toastrel, AccessShareLock);
	}
}

/*
 * Call this when done with CLUSTER CONCURRENTLY.
 *
 * 'error' tells whether the function is being called in order to handle
 * error.
 */
static void
end_concurrent_cluster(bool error)
{
	ClusteredRel	key, *entry, *entry_toast = NULL;
	Oid		relid = clustered_rel;
	Oid		toastrelid = clustered_rel_toast;

	/* Remove the relation from the hash if we managed to insert one. */
	if (OidIsValid(clustered_rel))
	{
		memset(&key, 0, sizeof(key));
		key.relid = clustered_rel;
		key.dbid = MyDatabaseId;
		LWLockAcquire(ClusteredRelsLock, LW_EXCLUSIVE);
		entry = hash_search(ClusteredRelsHash, &key, HASH_REMOVE, NULL);

		/*
		 * By clearing this variable we also disable
		 * cluster_before_shmem_exit_callback().
		 */
		clustered_rel = InvalidOid;
	}

	/* Remove the TOAST relation if there is one. */
	if (OidIsValid(clustered_rel_toast))
	{
		key.relid = clustered_rel_toast;
		entry_toast = hash_search(ClusteredRelsHash, &key, HASH_REMOVE,
								  NULL);

		clustered_rel_toast = InvalidOid;
	}
	LWLockRelease(ClusteredRelsLock);

	/* Restore normal function of logical decoding. */
	clustered_rel_locator.relNumber = InvalidOid;
	clustered_rel_toast_locator.relNumber = InvalidOid;

	/*
	 * On normal completion (!error), we should not really fail to remove the
	 * entry. But if it wasn't there for any reason, raise ERROR to make sure
	 * the transaction is aborted: if other transactions, while changing the
	 * contents of the relation, didn't know that CLUSTER CONCURRENTLY was in
	 * progress, they could have missed to WAL enough information, and thus we
	 * could have produced an inconsistent table contents.
	 *
	 * On the other hand, if we are already handling an error, there's no
	 * reason to worry about inconsistent contents of the new storage because
	 * the transaction is going to be rolled back anyway. Furthermore, by
	 * raising ERROR here we'd shadow the original error.
	 */
	if (!error)
	{
		char	*relname;

		if (OidIsValid(relid) && entry == NULL)
		{
			relname = get_rel_name(relid);
			if (!relname)
				ereport(ERROR,
						(errmsg("cache lookup failed for relation %u",
								relid)));

			ereport(ERROR,
					(errmsg("relation \"%s\" not found among clustered relations",
							relname)));
		}

		/*
		 * Likewise, the TOAST relation should not have disappeared.
		 */
		if (OidIsValid(toastrelid) && entry_toast == NULL)
		{
			relname = get_rel_name(key.relid);
			if (!relname)
				ereport(ERROR,
						(errmsg("cache lookup failed for relation %u",
								key.relid)));

			ereport(ERROR,
					(errmsg("relation \"%s\" not found among clustered relations",
							relname)));
		}
	}

	/*
	 * Note: unlike begin_concurrent_cluster(), here we do not lock/unlock the
	 * relation: 1) On normal completion, the caller is already holding
	 * AccessExclusiveLock (till the end of the transaction), 2) on ERROR /
	 * FATAL, we try to do the cleanup asap, but the worst case is that other
	 * backends will write unnecessary information to WAL until they close the
	 * relation.
	 */
}

/*
 * A wrapper to call end_concurrent_cluster() as a before_shmem_exit callback.
 */
static void
cluster_before_shmem_exit_callback(int code, Datum arg)
{
	if (OidIsValid(clustered_rel) || OidIsValid(clustered_rel_toast))
		end_concurrent_cluster(true);
}

/*
 * Check if relation is currently being processed by CLUSTER CONCURRENTLY.
 */
bool
is_concurrent_cluster_in_progress(Oid relid)
{
	ClusteredRel	key, *entry;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	key.dbid = MyDatabaseId;

	LWLockAcquire(ClusteredRelsLock, LW_SHARED);
	entry = (ClusteredRel *)
		hash_search(ClusteredRelsHash, &key, HASH_FIND, NULL);
	LWLockRelease(ClusteredRelsLock);

	return entry != NULL;
}

/*
 * Check if VACUUM FULL / CLUSTER CONCURRENTLY is already running for given
 * relation, and if so, raise ERROR. The problem is that cluster_rel() needs
 * to release its lock on the relation temporarily at some point, so our lock
 * alone does not help. Commands that might break what cluster_rel() is doing
 * should call this function first.
 *
 * Return without checking if lockmode allows for race conditions which would
 * make the result meaningless. In that case, cluster_rel() itself should
 * throw ERROR if the relation was changed by us in an incompatible
 * way. However, if it managed to do most of its work by then, a lot of CPU
 * time might be wasted.
 */
void
check_for_concurrent_cluster(Oid relid, LOCKMODE lockmode)
{
	/*
	 * If the caller does not have a lock that conflicts with
	 * LOCK_CLUSTER_CONCURRENT, the check makes little sense because the
	 * VACUUM FULL / CLUSTER CONCURRENTLY can start anytime after the check.
	 */
	if (lockmode < LOCK_CLUSTER_CONCURRENT)
		return;

	if (is_concurrent_cluster_in_progress(relid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(CLUSTER_IN_PROGRESS_MESSAGE,
						get_rel_name(relid))));

}

/*
 * Check if relation is eligible for CLUSTER CONCURRENTLY and retrieve the
 * catalog state to be passed later to check_catalog_changes.
 *
 * Caller is supposed to hold (at least) LOCK_CLUSTER_CONCURRENT on the
 * relation.
 */
static CatalogState *
get_catalog_state(Relation rel)
{
	CatalogState	*result = palloc_object(CatalogState);
	List	*ind_oids;
	ListCell	*lc;
	int		ninds, i;
	char	relpersistence = rel->rd_rel->relpersistence;
	char	replident = rel->rd_rel->relreplident;
	Oid		ident_idx = RelationGetReplicaIndex(rel);
	TupleDesc	td_src = RelationGetDescr(rel);

	/*
	 * While gathering the catalog information, check if there is a reason not
	 * to proceed.
	 */
	check_relation_is_clusterable_concurrently(rel, ERROR,
											   "CLUSTER (CONCURRENTLY)");

	/* No index should be dropped while we are checking it. */
	Assert(CheckRelationLockedByMe(rel, ShareUpdateExclusiveLock, true));

	ind_oids = RelationGetIndexList(rel);
	result->ninds = ninds = list_length(ind_oids);
	result->ind_oids = palloc_array(Oid, ninds);
	result->ind_tupdescs = palloc_array(TupleDesc, ninds);
	i = 0;
	foreach(lc, ind_oids)
	{
		Oid	ind_oid = lfirst_oid(lc);
		Relation	index;
		TupleDesc	td_ind_src, td_ind_dst;

		/*
		 * Weaker lock should be o.k. for the index, but this one should not
		 * break anything either.
		 */
		index = index_open(ind_oid, ShareUpdateExclusiveLock);

		result->ind_oids[i] = RelationGetRelid(index);
		td_ind_src = RelationGetDescr(index);
		td_ind_dst = palloc(TupleDescSize(td_ind_src));
		TupleDescCopy(td_ind_dst, td_ind_src);
		result->ind_tupdescs[i] = td_ind_dst;
		i++;

		index_close(index, ShareUpdateExclusiveLock);
	}

	/* Fill-in the relation info. */
	result->tupdesc = palloc(TupleDescSize(td_src));
	TupleDescCopy(result->tupdesc, td_src);
	result->relpersistence = relpersistence;
	result->replident = replident;
	result->replidindex = ident_idx;

	return	result;
}

static void
free_catalog_state(CatalogState *state)
{
	/* We are only interested in indexes. */
	if (state->ninds == 0)
		return;

	for (int i = 0; i < state->ninds; i++)
		FreeTupleDesc(state->ind_tupdescs[i]);

	FreeTupleDesc(state->tupdesc);
	pfree(state->ind_oids);
	pfree(state->ind_tupdescs);
	pfree(state);
}

/*
 * Raise ERROR if 'rel' changed in a way that does not allow further
 * processing of CLUSTER CONCURRENTLY.
 *
 * Besides the relation's tuple descriptor, it's important to check indexes:
 * concurrent change of index definition (can it happen in other way than
 * dropping and re-creating the index, accidentally with the same OID?) can be
 * a problem because we may already have the new index built. If an index was
 * created or dropped concurrently, we'd fail to swap the index storage. In
 * any case, we prefer to check the indexes early to get an explicit error
 * message about the mismatch. Furthermore, the earlier we detect the change,
 * the fewer CPU cycles we waste.
 *
 * Note that we do not check constraints because the transaction which changed
 * them must have ensured that the existing tuples satisfy the new
 * constraints. If any DML commands were necessary for that, we will simply
 * decode them from WAL and apply them to the new storage.
 *
 * Caller is supposed to hold (at least) ShareUpdateExclusiveLock on the
 * relation.
 */
static void
check_catalog_changes(Relation rel, CatalogState *cat_state)
{
	Oid		reltoastrelid = rel->rd_rel->reltoastrelid;
	List	*ind_oids;
	ListCell	*lc;
	LOCKMODE	lockmode;
	Oid		ident_idx;
	TupleDesc	td, td_cp;

	/* First, check the relation info. */

	/* TOAST is not easy to change, but check. */
	if (reltoastrelid != clustered_rel_toast)
		ereport(ERROR,
				errmsg("TOAST relation of relation \"%s\" changed by another transaction",
					   RelationGetRelationName(rel)));

	/*
	 * Likewise, check_for_concurrent_cluster() should prevent others from
	 * changing the relation file concurrently, but it's our responsibility to
	 * avoid data loss. (The original locators are stored outside cat_state,
	 * but the check belongs to this function.)
	 */
	if (!RelFileLocatorEquals(rel->rd_locator, clustered_rel_locator))
		ereport(ERROR,
				(errmsg("file of relation \"%s\" changed by another transaction",
						RelationGetRelationName(rel))));
	if (OidIsValid(reltoastrelid))
	{
		Relation	toastrel;

		toastrel = table_open(reltoastrelid, AccessShareLock);
		if (!RelFileLocatorEquals(toastrel->rd_locator,
								  clustered_rel_toast_locator))
			ereport(ERROR,
					(errmsg("file of relation \"%s\" changed by another transaction",
							RelationGetRelationName(toastrel))));
		table_close(toastrel, AccessShareLock);
	}

	if (rel->rd_rel->relpersistence != cat_state->relpersistence)
		ereport(ERROR,
				errmsg("persistence of relation \"%s\" changed by another transaction",
					   RelationGetRelationName(rel)));

	if (cat_state->replident != rel->rd_rel->relreplident)
		ereport(ERROR,
				errmsg("replica identity of relation \"%s\" changed by another transaction",
					   RelationGetRelationName(rel)));

	ident_idx = RelationGetReplicaIndex(rel);
	if (ident_idx == InvalidOid && rel->rd_pkindex != InvalidOid)
		ident_idx = rel->rd_pkindex;
	if (cat_state->replidindex != ident_idx)
		ereport(ERROR,
				errmsg("identity index of relation \"%s\" changed by another transaction",
					   RelationGetRelationName(rel)));

	/*
	 * As cat_state contains a copy (which has the constraint info cleared),
	 * create a temporary copy for the comparison.
	 */
	td = RelationGetDescr(rel);
	td_cp = palloc(TupleDescSize(td));
	TupleDescCopy(td_cp, td);
	if (!equalTupleDescs(cat_state->tupdesc, td_cp))
		ereport(ERROR,
				errmsg("definition of relation \"%s\" changed by another transaction",
					   RelationGetRelationName(rel)));
	FreeTupleDesc(td_cp);

	/* Now we are only interested in indexes. */
	if (cat_state->ninds == 0)
		return;

	/* No index should be dropped while we are checking the relation. */
	lockmode = ShareUpdateExclusiveLock;
	Assert(CheckRelationLockedByMe(rel, lockmode, true));

	ind_oids = RelationGetIndexList(rel);
	if (list_length(ind_oids) != cat_state->ninds)
		goto failed_index;

	foreach(lc, ind_oids)
	{
		Oid	ind_oid = lfirst_oid(lc);
		int		i;
		TupleDesc	tupdesc;
		Relation	index;

		/* Find the index in cat_state. */
		for (i = 0; i < cat_state->ninds; i++)
		{
			if (cat_state->ind_oids[i] == ind_oid)
				break;
		}
		/*
		 * OID not found, i.e. the index was replaced by another one. XXX
		 * Should we yet try to find if an index having the desired tuple
		 * descriptor exists? Or should we always look for the tuple
		 * descriptor and not use OIDs at all?
		 */
		if (i == cat_state->ninds)
			goto failed_index;

		/* Check the tuple descriptor. */
		index = try_index_open(ind_oid, lockmode);
		if (index == NULL)
			goto failed_index;
		tupdesc = RelationGetDescr(index);
		if (!equalTupleDescs(cat_state->ind_tupdescs[i], tupdesc))
			goto failed_index;
		index_close(index, lockmode);
	}

	return;

failed_index:
	ereport(ERROR,
			(errmsg("index(es) of relation \"%s\" changed by another transaction",
					RelationGetRelationName(rel))));
}

/*
 * This function is much like pg_create_logical_replication_slot() except that
 * the new slot is neither released (if anyone else could read changes from
 * our slot, we could miss changes other backends do while we copy the
 * existing data into temporary table), nor persisted (it's easier to handle
 * crash by restarting all the work from scratch).
 *
 * XXX Even though CreateInitDecodingContext() does not set state to
 * RS_PERSISTENT, it does write the slot to disk. We rely on
 * RestoreSlotFromDisk() to delete ephemeral slots during startup. (Both ERROR
 * and FATAL should lead to cleanup even before the cluster goes down.)
 */
static LogicalDecodingContext *
setup_logical_decoding(Oid relid, const char *slotname, TupleDesc tupdesc)
{
	LogicalDecodingContext *ctx;
	ClusterDecodingState *dstate;

	/* RS_TEMPORARY so that the slot gets cleaned up on ERROR. */
	ReplicationSlotCreate(slotname, true, RS_TEMPORARY, false, false, false);

	/*
	 * Neither prepare_write nor do_write callback nor update_progress is
	 * useful for us.
	 *
	 * Regarding the value of need_full_snapshot, we pass false because the
	 * table we are processing is present in ClusteredRelsHash and therefore,
	 * regarding logical decoding, treated like a catalog.
	 */
	ctx = CreateInitDecodingContext(REPL_PLUGIN_NAME,
									NIL,
									false,
									InvalidXLogRecPtr,
									XL_ROUTINE(.page_read = read_local_xlog_page,
											   .segment_open = wal_segment_open,
											   .segment_close = wal_segment_close),
									NULL, NULL, NULL);

	/*
	 * We don't have control on setting fast_forward, so at least check it.
	 */
	Assert(!ctx->fast_forward);

	DecodingContextFindStartpoint(ctx);

	/* Some WAL records should have been read. */
	Assert(ctx->reader->EndRecPtr != InvalidXLogRecPtr);

	XLByteToSeg(ctx->reader->EndRecPtr, cluster_current_segment,
				wal_segment_size);

	/*
	 * Setup structures to store decoded changes.
	 */
	dstate = palloc0(sizeof(ClusterDecodingState));
	dstate->relid = relid;
	dstate->tstore = tuplestore_begin_heap(false, false,
										   maintenance_work_mem);
#ifdef USE_ASSERT_CHECKING
	dstate->last_change_xid = InvalidTransactionId;
#endif
	dstate->tupdesc = tupdesc;

	/* Initialize the descriptor to store the changes ... */
	dstate->tupdesc_change = CreateTemplateTupleDesc(1);

	TupleDescInitEntry(dstate->tupdesc_change, 1, NULL, BYTEAOID, -1, 0);
	/* ... as well as the corresponding slot. */
	dstate->tsslot = MakeSingleTupleTableSlot(dstate->tupdesc_change,
											  &TTSOpsMinimalTuple);

	dstate->resowner = ResourceOwnerCreate(CurrentResourceOwner,
										   "logical decoding");

	ctx->output_writer_private = dstate;
	return ctx;
}

/*
 * Retrieve tuple from a change structure. As for the change, no alignment is
 * assumed.
 */
static HeapTuple
get_changed_tuple(ConcurrentChange *change)
{
	HeapTupleData tup_data;
	HeapTuple	result;
	char	   *src;

	/*
	 * Ensure alignment before accessing the fields. (This is why we can't use
	 * heap_copytuple() instead of this function.)
	 */
	memcpy(&tup_data, &change->tup_data, sizeof(HeapTupleData));

	result = (HeapTuple) palloc(HEAPTUPLESIZE + tup_data.t_len);
	memcpy(result, &tup_data, sizeof(HeapTupleData));
	result->t_data = (HeapTupleHeader) ((char *) result + HEAPTUPLESIZE);
	src = (char *) change + sizeof(ConcurrentChange);
	memcpy(result->t_data, src, result->t_len);

	return result;
}

/*
 * Decode logical changes from the WAL sequence up to end_of_wal.
 */
void
cluster_decode_concurrent_changes(LogicalDecodingContext *ctx,
								  XLogRecPtr end_of_wal)
{
	ClusterDecodingState *dstate;
	ResourceOwner resowner_old;
	PgBackendProgress	progress;

	/*
	 * Invalidate the "present" cache before moving to "(recent) history".
	 */
	InvalidateSystemCaches();

	dstate = (ClusterDecodingState *) ctx->output_writer_private;
	resowner_old = CurrentResourceOwner;
	CurrentResourceOwner = dstate->resowner;

	/*
	 * reorderbuffer.c uses internal subtransaction, whose abort ends the
	 * command progress reporting. Save the status here so we can restore when
	 * done with the decoding.
	 */
	memcpy(&progress, &MyBEEntry->st_progress, sizeof(PgBackendProgress));

	PG_TRY();
	{
		while (ctx->reader->EndRecPtr < end_of_wal)
		{
			XLogRecord *record;
			XLogSegNo	segno_new;
			char	   *errm = NULL;
			XLogRecPtr	end_lsn;

			record = XLogReadRecord(ctx->reader, &errm);
			if (errm)
				elog(ERROR, "%s", errm);

			if (record != NULL)
				LogicalDecodingProcessRecord(ctx, ctx->reader);

			/*
			 * If WAL segment boundary has been crossed, inform the decoding
			 * system that the catalog_xmin can advance. (We can confirm more
			 * often, but a filling a single WAL segment should not take much
			 * time.)
			 */
			end_lsn = ctx->reader->EndRecPtr;
			XLByteToSeg(end_lsn, segno_new, wal_segment_size);
			if (segno_new != cluster_current_segment)
			{
				LogicalConfirmReceivedLocation(end_lsn);
				elog(DEBUG1, "cluster: confirmed receive location %X/%X",
					 (uint32) (end_lsn >> 32), (uint32) end_lsn);
				cluster_current_segment = segno_new;
			}

			CHECK_FOR_INTERRUPTS();
		}
		InvalidateSystemCaches();
		CurrentResourceOwner = resowner_old;
	}
	PG_CATCH();
	{
		InvalidateSystemCaches();
		CurrentResourceOwner = resowner_old;
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Restore the progress reporting status. */
	pgstat_progress_restore_state(&progress);
}

/*
 * Apply changes that happened during the initial load.
 *
 * Scan key is passed by caller, so it does not have to be constructed
 * multiple times. Key entries have all fields initialized, except for
 * sk_argument.
 */
static void
apply_concurrent_changes(ClusterDecodingState *dstate, Relation rel,
						 ScanKey key, int nkeys, IndexInsertState *iistate)
{
	TupleTableSlot *index_slot, *ident_slot;
	HeapTuple	tup_old = NULL;

	if (dstate->nchanges == 0)
		return;

	/* TupleTableSlot is needed to pass the tuple to ExecInsertIndexTuples(). */
	index_slot = MakeSingleTupleTableSlot(dstate->tupdesc, &TTSOpsHeapTuple);
	iistate->econtext->ecxt_scantuple = index_slot;

	/* A slot to fetch tuples from identity index. */
	ident_slot = table_slot_create(rel, NULL);

	while (tuplestore_gettupleslot(dstate->tstore, true, false,
								   dstate->tsslot))
	{
		bool		shouldFree;
		HeapTuple	tup_change,
					tup,
					tup_exist;
		char	   *change_raw;
		ConcurrentChange *change;
		Snapshot	snapshot;
		bool		isnull[1];
		Datum		values[1];

		CHECK_FOR_INTERRUPTS();

		/* Get the change from the single-column tuple. */
		tup_change = ExecFetchSlotHeapTuple(dstate->tsslot, false, &shouldFree);
		heap_deform_tuple(tup_change, dstate->tupdesc_change, values, isnull);
		Assert(!isnull[0]);

		/* This is bytea, but char* is easier to work with. */
		change_raw = (char *) DatumGetByteaP(values[0]);

		change = (ConcurrentChange *) VARDATA(change_raw);

		/* TRUNCATE change contains no tuple, so process it separately. */
		if (change->kind == CHANGE_TRUNCATE)
		{
			/*
			 * All the things that ExecuteTruncateGuts() does (such as firing
			 * triggers or handling the DROP_CASCADE behavior) should have
			 * taken place on the source relation. Thus we only do the actual
			 * truncation of the new relation (and its indexes).
			 */
			heap_truncate_one_rel(rel);

			pfree(tup_change);
			continue;
		}

		/*
		 * Extract the tuple from the change. The tuple is copied here because
		 * it might be assigned to 'tup_old', in which case it needs to
		 * survive into the next iteration.
		 */
		tup = get_changed_tuple(change);

		if (change->kind == CHANGE_UPDATE_OLD)
		{
			Assert(tup_old == NULL);
			tup_old = tup;
		}
		else if (change->kind == CHANGE_INSERT)
		{
			Assert(tup_old == NULL);

			apply_concurrent_insert(rel, change, tup, iistate, index_slot);

			pfree(tup);
		}
		else if (change->kind == CHANGE_UPDATE_NEW ||
				 change->kind == CHANGE_DELETE)
		{
			IndexScanDesc	ind_scan = NULL;
			HeapTuple	tup_key;

			if (change->kind == CHANGE_UPDATE_NEW)
			{
				tup_key = tup_old != NULL ? tup_old : tup;
			}
			else
			{
				Assert(tup_old == NULL);
				tup_key = tup;
			}

			/*
			 * Find the tuple to be updated or deleted.
			 *
			 * As the table being CLUSTERed concurrently is considered an
			 * "user catalog", new CID is WAL-logged and decoded. And since we
			 * use the same XID that the original DMLs did, the snapshot used
			 * for the logical decoding (by now converted to a non-historic
			 * MVCC snapshot) should see the tuples inserted previously into
			 * the new heap and/or updated there.
			 */
			snapshot = change->snapshot;

			/*
			 * Set what should be considered current transaction (and
			 * subtransactions) during visibility check.
			 *
			 * Note that this snapshot was created from a historic snapshot
			 * using SnapBuildMVCCFromHistoric(), which does not touch
			 * 'subxip'. Thus, unlike in a regular MVCC snapshot, the array
			 * only contains the transactions whose data changes we are
			 * applying, and its subtransactions. That's exactly what we need
			 * to check if particular xact is a "current transaction:".
			 */
			SetClusterCurrentXids(snapshot->subxip, snapshot->subxcnt);

			tup_exist = find_target_tuple(rel, key, nkeys, tup_key, snapshot,
										  iistate, ident_slot, &ind_scan);
			if (tup_exist == NULL)
				elog(ERROR, "Failed to find target tuple");

			if (change->kind == CHANGE_UPDATE_NEW)
				apply_concurrent_update(rel, tup, tup_exist, change, iistate,
										index_slot);
			else
				apply_concurrent_delete(rel, tup_exist, change);

			ResetClusterCurrentXids();

			if (tup_old != NULL)
			{
				pfree(tup_old);
				tup_old = NULL;
			}

			pfree(tup);
			index_endscan(ind_scan);
		}
		else
			elog(ERROR, "Unrecognized kind of change: %d", change->kind);

		/* Free the snapshot if this is the last change that needed it. */
		Assert(change->snapshot->active_count > 0);
		change->snapshot->active_count--;
		if (change->snapshot->active_count == 0)
		{
			if (change->snapshot == dstate->snapshot)
				dstate->snapshot = NULL;
			FreeSnapshot(change->snapshot);
		}

		/* TTSOpsMinimalTuple has .get_heap_tuple==NULL. */
		Assert(shouldFree);
		pfree(tup_change);
	}

	tuplestore_clear(dstate->tstore);
	dstate->nchanges = 0;

	/* Cleanup. */
	ExecDropSingleTupleTableSlot(index_slot);
	ExecDropSingleTupleTableSlot(ident_slot);
}

static void
apply_concurrent_insert(Relation rel, ConcurrentChange *change, HeapTuple tup,
						IndexInsertState *iistate, TupleTableSlot *index_slot)
{
	Snapshot	snapshot = change->snapshot;
	List	   *recheck;

	/*
	 * For INSERT, the visibility information is not important, but we use the
	 * snapshot to get CID. Index functions might need the whole snapshot
	 * anyway.
	 */
	SetClusterCurrentXids(snapshot->subxip, snapshot->subxcnt);

	/*
	 * Write the tuple into the new heap.
	 *
	 * The snapshot is the one we used to decode the insert (though converted
	 * to "non-historic" MVCC snapshot), i.e. the snapshot's curcid is the
	 * tuple CID incremented by one (due to the "new CID" WAL record that got
	 * written along with the INSERT record). Thus if we want to use the
	 * original CID, we need to subtract 1 from curcid.
	 */
	Assert(snapshot->curcid != InvalidCommandId &&
		   snapshot->curcid > FirstCommandId);

	heap_insert(rel, tup, change->xid, snapshot->curcid - 1,
				HEAP_INSERT_NO_LOGICAL, NULL);

	/*
	 * Update indexes.
	 *
	 * In case functions in the index need the active snapshot and caller
	 * hasn't set one.
	 */
	PushActiveSnapshot(snapshot);
	ExecStoreHeapTuple(tup, index_slot, false);
	recheck = ExecInsertIndexTuples(iistate->rri,
									index_slot,
									iistate->estate,
									false,	/* update */
									false,	/* noDupErr */
									NULL,	/* specConflict */
									NIL, /* arbiterIndexes */
									false	/* onlySummarizing */
		);
	PopActiveSnapshot();
	ResetClusterCurrentXids();

	/*
	 * If recheck is required, it must have been preformed on the source
	 * relation by now. (All the logical changes we process here are already
	 * committed.)
	 */
	list_free(recheck);

	pgstat_progress_incr_param(PROGRESS_CLUSTER_HEAP_TUPLES_INSERTED, 1);
}

static void
apply_concurrent_update(Relation rel, HeapTuple tup, HeapTuple tup_target,
						ConcurrentChange *change, IndexInsertState *iistate,
						TupleTableSlot *index_slot)
{
	List	   *recheck;
	LockTupleMode	lockmode;
	TU_UpdateIndexes	update_indexes;
	TM_Result	res;
	Snapshot snapshot	= change->snapshot;
	TM_FailureData tmfd;

	/*
	 * Write the new tuple into the new heap. ('tup' gets the TID assigned
	 * here.)
	 *
	 * Regarding CID, see the comment in apply_concurrent_insert().
	 */
	Assert(snapshot->curcid != InvalidCommandId &&
		   snapshot->curcid > FirstCommandId);

	res = heap_update(rel, &tup_target->t_self, tup,
					  change->xid, snapshot->curcid - 1,
					  InvalidSnapshot,
					  false, /* no wait - only we are doing changes */
					  &tmfd, &lockmode, &update_indexes,
					  /* wal_logical */
					  false);
	if (res != TM_Ok)
		ereport(ERROR, (errmsg("failed to apply concurrent UPDATE")));

	ExecStoreHeapTuple(tup, index_slot, false);

	if (update_indexes != TU_None)
	{
		PushActiveSnapshot(snapshot);
		recheck = ExecInsertIndexTuples(iistate->rri,
										index_slot,
										iistate->estate,
										true,	/* update */
										false,	/* noDupErr */
										NULL,	/* specConflict */
										NIL, /* arbiterIndexes */
										/* onlySummarizing */
										update_indexes == TU_Summarizing);
		PopActiveSnapshot();
		list_free(recheck);
	}

	pgstat_progress_incr_param(PROGRESS_CLUSTER_HEAP_TUPLES_UPDATED, 1);
}

static void
apply_concurrent_delete(Relation rel, HeapTuple tup_target,
						ConcurrentChange *change)
{
	TM_Result	res;
	TM_FailureData tmfd;
	Snapshot	snapshot = change->snapshot;

	/* Regarding CID, see the comment in apply_concurrent_insert(). */
	Assert(snapshot->curcid != InvalidCommandId &&
		   snapshot->curcid > FirstCommandId);

	res = heap_delete(rel, &tup_target->t_self, change->xid,
					  snapshot->curcid - 1, InvalidSnapshot, false,
					  &tmfd, false,
					  /* wal_logical */
					  false);

	if (res != TM_Ok)
		ereport(ERROR, (errmsg("failed to apply concurrent DELETE")));

	pgstat_progress_incr_param(PROGRESS_CLUSTER_HEAP_TUPLES_DELETED, 1);
}

/*
 * Find the tuple to be updated or deleted.
 *
 * 'key' is a pre-initialized scan key, into which the function will put the
 * key values.
 *
 * 'tup_key' is a tuple containing the key values for the scan.
 *
 * On exit,'*scan_p' contains the scan descriptor used. The caller must close
 * it when he no longer needs the tuple returned.
 */
static HeapTuple
find_target_tuple(Relation rel, ScanKey key, int nkeys, HeapTuple tup_key,
				  Snapshot snapshot, IndexInsertState *iistate,
				  TupleTableSlot *ident_slot, IndexScanDesc *scan_p)
{
	IndexScanDesc scan;
	Form_pg_index ident_form;
	int2vector *ident_indkey;
	HeapTuple	result = NULL;

	scan = index_beginscan(rel, iistate->ident_index, snapshot,
						   nkeys, 0);
	*scan_p = scan;
	index_rescan(scan, key, nkeys, NULL, 0);

	/* Info needed to retrieve key values from heap tuple. */
	ident_form = iistate->ident_index->rd_index;
	ident_indkey = &ident_form->indkey;

	/* Use the incoming tuple to finalize the scan key. */
	for (int i = 0; i < scan->numberOfKeys; i++)
	{
		ScanKey		entry;
		bool		isnull;
		int16		attno_heap;

		entry = &scan->keyData[i];
		attno_heap = ident_indkey->values[i];
		entry->sk_argument = heap_getattr(tup_key,
										  attno_heap,
										  rel->rd_att,
										  &isnull);
		Assert(!isnull);
	}
	if (index_getnext_slot(scan, ForwardScanDirection, ident_slot))
	{
		bool		shouldFree;

		result = ExecFetchSlotHeapTuple(ident_slot, false, &shouldFree);
		/* TTSOpsBufferHeapTuple has .get_heap_tuple != NULL. */
		Assert(!shouldFree);
	}

	return result;
}

/*
 * Decode and apply concurrent changes.
 *
 * Pass rel_src iff its reltoastrelid is needed.
 */
static void
process_concurrent_changes(LogicalDecodingContext *ctx, XLogRecPtr end_of_wal,
						   Relation rel_dst, Relation rel_src, ScanKey ident_key,
						   int ident_key_nentries, IndexInsertState *iistate)
{
	ClusterDecodingState *dstate;

	pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE,
								 PROGRESS_CLUSTER_PHASE_CATCH_UP);

	dstate = (ClusterDecodingState *) ctx->output_writer_private;

	cluster_decode_concurrent_changes(ctx, end_of_wal);

	if (dstate->nchanges == 0)
		return;

	PG_TRY();
	{
		/*
		 * Make sure that TOAST values can eventually be accessed via the old
		 * relation - see comment in copy_table_data().
		 */
		if (rel_src)
			rel_dst->rd_toastoid = rel_src->rd_rel->reltoastrelid;

		apply_concurrent_changes(dstate, rel_dst, ident_key,
								 ident_key_nentries, iistate);
	}
	PG_FINALLY();
	{
		ResetClusterCurrentXids();

		if (rel_src)
			rel_dst->rd_toastoid = InvalidOid;
	}
	PG_END_TRY();
}

static IndexInsertState *
get_index_insert_state(Relation relation, Oid ident_index_id)
{
	EState	   *estate;
	int			i;
	IndexInsertState *result;

	result = (IndexInsertState *) palloc0(sizeof(IndexInsertState));
	estate = CreateExecutorState();
	result->econtext = GetPerTupleExprContext(estate);

	result->rri = (ResultRelInfo *) palloc(sizeof(ResultRelInfo));
	InitResultRelInfo(result->rri, relation, 0, 0, 0);
	ExecOpenIndices(result->rri, false);

	/*
	 * Find the relcache entry of the identity index so that we spend no extra
	 * effort to open / close it.
	 */
	for (i = 0; i < result->rri->ri_NumIndices; i++)
	{
		Relation	ind_rel;

		ind_rel = result->rri->ri_IndexRelationDescs[i];
		if (ind_rel->rd_id == ident_index_id)
			result->ident_index = ind_rel;
	}
	if (result->ident_index == NULL)
		elog(ERROR, "Failed to open identity index");

	/* Only initialize fields needed by ExecInsertIndexTuples(). */
	result->estate = estate;

	return result;
}

/*
 * Build scan key to process logical changes.
 */
static ScanKey
build_identity_key(Oid ident_idx_oid, Relation rel_src, int *nentries)
{
	Relation	ident_idx_rel;
	Form_pg_index ident_idx;
	int			n,
				i;
	ScanKey		result;

	Assert(OidIsValid(ident_idx_oid));
	ident_idx_rel = index_open(ident_idx_oid, AccessShareLock);
	ident_idx = ident_idx_rel->rd_index;
	n = ident_idx->indnatts;
	result = (ScanKey) palloc(sizeof(ScanKeyData) * n);
	for (i = 0; i < n; i++)
	{
		ScanKey		entry;
		int16		relattno;
		Form_pg_attribute att;
		Oid			opfamily,
					opcintype,
					opno,
					opcode;

		entry = &result[i];
		relattno = ident_idx->indkey.values[i];
		if (relattno >= 1)
		{
			TupleDesc	desc;

			desc = rel_src->rd_att;
			att = TupleDescAttr(desc, relattno - 1);
		}
		else
			elog(ERROR, "Unexpected attribute number %d in index", relattno);

		opfamily = ident_idx_rel->rd_opfamily[i];
		opcintype = ident_idx_rel->rd_opcintype[i];
		opno = get_opfamily_member(opfamily, opcintype, opcintype,
								   BTEqualStrategyNumber);

		if (!OidIsValid(opno))
			elog(ERROR, "Failed to find = operator for type %u", opcintype);

		opcode = get_opcode(opno);
		if (!OidIsValid(opcode))
			elog(ERROR, "Failed to find = operator for operator %u", opno);

		/* Initialize everything but argument. */
		ScanKeyInit(entry,
					i + 1,
					BTEqualStrategyNumber, opcode,
					(Datum) NULL);
		entry->sk_collation = att->attcollation;
	}
	index_close(ident_idx_rel, AccessShareLock);

	*nentries = n;
	return result;
}

static void
free_index_insert_state(IndexInsertState *iistate)
{
	ExecCloseIndices(iistate->rri);
	FreeExecutorState(iistate->estate);
	pfree(iistate->rri);
	pfree(iistate);
}

static void
cleanup_logical_decoding(LogicalDecodingContext *ctx)
{
	ClusterDecodingState *dstate;

	dstate = (ClusterDecodingState *) ctx->output_writer_private;

	ExecDropSingleTupleTableSlot(dstate->tsslot);
	FreeTupleDesc(dstate->tupdesc_change);
	FreeTupleDesc(dstate->tupdesc);
	tuplestore_end(dstate->tstore);

	FreeDecodingContext(ctx);
}

/*
 * The final steps of rebuild_relation() for concurrent processing.
 *
 * On entry, NewHeap is locked in AccessExclusiveLock mode. OldHeap and its
 * clustering index (if one is passed) are still locked in a mode that allows
 * concurrent data changes. On exit, both tables and their indexes are closed,
 * but locked in AccessExclusiveLock mode.
 */
static void
rebuild_relation_finish_concurrent(Relation NewHeap, Relation OldHeap,
								   Relation cl_index,
								   CatalogState	*cat_state,
								   LogicalDecodingContext *ctx,
								   bool swap_toast_by_content,
								   TransactionId frozenXid,
								   MultiXactId cutoffMulti)
{
	LOCKMODE	lockmode_old	PG_USED_FOR_ASSERTS_ONLY;
	List	*ind_oids_new;
	Oid		old_table_oid = RelationGetRelid(OldHeap);
	Oid		new_table_oid = RelationGetRelid(NewHeap);
	List	*ind_oids_old = RelationGetIndexList(OldHeap);
	ListCell	*lc, *lc2;
	char		relpersistence;
	bool		is_system_catalog;
	Oid		ident_idx_old, ident_idx_new;
	IndexInsertState *iistate;
	ScanKey		ident_key;
	int		ident_key_nentries;
	XLogRecPtr	wal_insert_ptr, end_of_wal;
	char		dummy_rec_data = '\0';
	RelReopenInfo	*rri = NULL;
	int		nrel;
	Relation	*ind_refs_all, *ind_refs_p;

	/* Like in cluster_rel(). */
	lockmode_old = LOCK_CLUSTER_CONCURRENT;
	Assert(CheckRelationLockedByMe(OldHeap, lockmode_old, false));
	Assert(cl_index == NULL ||
		   CheckRelationLockedByMe(cl_index, lockmode_old, false));
	/* This is expected from the caller. */
	Assert(CheckRelationLockedByMe(NewHeap, AccessExclusiveLock, false));

	ident_idx_old = RelationGetReplicaIndex(OldHeap);

	/*
	 * Unlike the exclusive case, we build new indexes for the new relation
	 * rather than swapping the storage and reindexing the old relation. The
	 * point is that the index build can take some time, so we do it before we
	 * get AccessExclusiveLock on the old heap and therefore we cannot swap
	 * the heap storage yet.
	 *
	 * index_create() will lock the new indexes using AccessExclusiveLock
	 * creation - no need to change that.
	 */
	ind_oids_new = build_new_indexes(NewHeap, OldHeap, ind_oids_old);

	/*
	 * Processing shouldn't start w/o valid identity index.
	 */
	Assert(OidIsValid(ident_idx_old));

	/* Find "identity index" on the new relation. */
	ident_idx_new = InvalidOid;
	forboth(lc, ind_oids_old, lc2, ind_oids_new)
	{
		Oid	ind_old = lfirst_oid(lc);
		Oid	ind_new = lfirst_oid(lc2);

		if (ident_idx_old == ind_old)
		{
			ident_idx_new = ind_new;
			break;
		}
	}
	if (!OidIsValid(ident_idx_new))
		/*
		 * Should not happen, given our lock on the old relation.
		 */
		ereport(ERROR,
				(errmsg("Identity index missing on the new relation")));

	/* Executor state to update indexes. */
	iistate = get_index_insert_state(NewHeap, ident_idx_new);

	/*
	 * Build scan key that we'll use to look for rows to be updated / deleted
	 * during logical decoding.
	 */
	ident_key = build_identity_key(ident_idx_new, OldHeap, &ident_key_nentries);

	/*
	 * Flush all WAL records inserted so far (possibly except for the last
	 * incomplete page, see GetInsertRecPtr), to minimize the amount of data
	 * we need to flush while holding exclusive lock on the source table.
	 */
	wal_insert_ptr = GetInsertRecPtr();
	XLogFlush(wal_insert_ptr);
	end_of_wal = GetFlushRecPtr(NULL);

	/*
	 * Apply concurrent changes first time, to minimize the time we need to
	 * hold AccessExclusiveLock. (Quite some amount of WAL could have been
	 * written during the data copying and index creation.)
	 */
	process_concurrent_changes(ctx, end_of_wal, NewHeap,
							   swap_toast_by_content ? OldHeap : NULL,
							   ident_key, ident_key_nentries, iistate);

	/*
	 * Release the locks that allowed concurrent data changes, in order to
	 * acquire the AccessExclusiveLock.
	 */
	nrel = 0;
	/*
	 * We unlock the old relation (and its clustering index), but then we will
	 * lock the relation and *all* its indexes because we want to swap their
	 * storage.
	 *
	 * (NewHeap is already locked, as well as its indexes.)
	 */
	rri = palloc_array(RelReopenInfo, 1 + list_length(ind_oids_old));
	init_rel_reopen_info(&rri[nrel++], &OldHeap, InvalidOid,
						 LOCK_CLUSTER_CONCURRENT, AccessExclusiveLock);
	/* References to the re-opened indexes will be stored in this array. */
	ind_refs_all = palloc_array(Relation, list_length(ind_oids_old));
	ind_refs_p = ind_refs_all;
	/* The clustering index is a special case. */
	if (cl_index)
	{
		*ind_refs_p = cl_index;
		init_rel_reopen_info(&rri[nrel], ind_refs_p, InvalidOid,
							 LOCK_CLUSTER_CONCURRENT, AccessExclusiveLock);
		nrel++;
		ind_refs_p++;
	}
	/*
	 * Initialize also the entries for the other indexes (currently unlocked)
	 * because we will have to lock them.
	 */
	foreach(lc, ind_oids_old)
	{
		Oid		ind_oid;

		ind_oid = lfirst_oid(lc);
		/* Clustering index is already in the array, or there is none. */
		if (cl_index && RelationGetRelid(cl_index) == ind_oid)
			continue;

		Assert(nrel < (1 + list_length(ind_oids_old)));

		*ind_refs_p = NULL;
		init_rel_reopen_info(&rri[nrel],
							 /*
							  * In this special case we do not have the
							  * relcache reference, use OID instead.
							  */
							 ind_refs_p,
							 ind_oid,
							 NoLock, /* Nothing to unlock. */
							 AccessExclusiveLock);

		nrel++;
		ind_refs_p++;
	}
	/* Perform the actual unlocking and re-locking. */
	unlock_and_close_relations(rri, nrel);
	reopen_relations(rri, nrel);

	/*
	 * In addition, lock the OldHeap's TOAST relation that we skipped for the
	 * CONCURRENTLY option in copy_table_data(). This lock will be needed to
	 * swap the relation files.
	 */
	if (OidIsValid(OldHeap->rd_rel->reltoastrelid))
		LockRelationOid(OldHeap->rd_rel->reltoastrelid, AccessExclusiveLock);

	/*
	 * Check if the new indexes match the old ones, i.e. no changes occurred
	 * while OldHeap was unlocked.
	 *
	 * XXX It's probably not necessary to check the relation tuple descriptor
	 * here because the logical decoding was already active when we released
	 * the lock, and thus the corresponding data changes won't be lost.
	 * However processing of those changes might take a lot of time.
	 */
	check_catalog_changes(OldHeap, cat_state);

	/*
	 * Tuples and pages of the old heap will be gone, but the heap will stay.
	 */
	TransferPredicateLocksToHeapRelation(OldHeap);
	/* The same for indexes. */
	for (int i = 0; i < (nrel - 1); i++)
	{
		Relation	index = ind_refs_all[i];

		TransferPredicateLocksToHeapRelation(index);

		/*
		 * References to indexes on the old relation are not needed anymore,
		 * however locks stay till the end of the transaction.
		 */
		index_close(index, NoLock);
	}
	pfree(ind_refs_all);

	/*
	 * Flush anything we see in WAL, to make sure that all changes committed
	 * while we were waiting for the exclusive lock are available for
	 * decoding. This should not be necessary if all backends had
	 * synchronous_commit set, but we can't rely on this setting.
	 *
	 * Unfortunately, GetInsertRecPtr() may lag behind the actual insert
	 * position, and GetLastImportantRecPtr() points at the start of the last
	 * record rather than at the end. Thus the simplest way to determine the
	 * insert position is to insert a dummy record and use its LSN.
	 *
	 * XXX Consider using GetLastImportantRecPtr() and adding the size of the
	 * last record (plus the total size of all the page headers the record
	 * spans)?
	 */
	XLogBeginInsert();
	XLogRegisterData(&dummy_rec_data, 1);
	wal_insert_ptr = XLogInsert(RM_XLOG_ID, XLOG_NOOP);
	XLogFlush(wal_insert_ptr);
	end_of_wal = GetFlushRecPtr(NULL);

	/* Apply the concurrent changes again. */
	process_concurrent_changes(ctx, end_of_wal, NewHeap,
							   swap_toast_by_content ? OldHeap : NULL,
							   ident_key, ident_key_nentries, iistate);

	/* Remember info about rel before closing OldHeap */
	relpersistence = OldHeap->rd_rel->relpersistence;
	is_system_catalog = IsSystemRelation(OldHeap);

	pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE,
								 PROGRESS_CLUSTER_PHASE_SWAP_REL_FILES);

	forboth(lc, ind_oids_old, lc2, ind_oids_new)
	{
		Oid	ind_old = lfirst_oid(lc);
		Oid	ind_new = lfirst_oid(lc2);
		Oid			mapped_tables[4];

		/* Zero out possible results from swapped_relation_files */
		memset(mapped_tables, 0, sizeof(mapped_tables));

		swap_relation_files(ind_old, ind_new,
							(old_table_oid == RelationRelationId),
							swap_toast_by_content,
							true,
							InvalidTransactionId,
							InvalidMultiXactId,
							mapped_tables);

#ifdef USE_ASSERT_CHECKING
		/*
		 * Concurrent processing is not supported for system relations, so
		 * there should be no mapped tables.
		 */
		for (int i = 0; i < 4; i++)
			Assert(mapped_tables[i] == 0);
#endif
	}

	/* The new indexes must be visible for deletion. */
	CommandCounterIncrement();

	/* Close the old heap but keep lock until transaction commit. */
	table_close(OldHeap, NoLock);
	/* Close the new heap. (We didn't have to open its indexes). */
	table_close(NewHeap, NoLock);

	/* Cleanup what we don't need anymore. (And close the identity index.) */
	pfree(ident_key);
	free_index_insert_state(iistate);

	/*
	 * Swap the relations and their TOAST relations and TOAST indexes. This
	 * also drops the new relation and its indexes.
	 *
	 * (System catalogs are currently not supported.)
	 */
	Assert(!is_system_catalog);
	finish_heap_swap(old_table_oid, new_table_oid,
					 is_system_catalog,
					 swap_toast_by_content,
					 false, true, false,
					 frozenXid, cutoffMulti,
					 relpersistence);

	pfree(rri);
}

/*
 * Build indexes on NewHeap according to those on OldHeap.
 *
 * OldIndexes is the list of index OIDs on OldHeap.
 *
 * A list of OIDs of the corresponding indexes created on NewHeap is
 * returned. The order of items does match, so we can use these arrays to swap
 * index storage.
 */
static List *
build_new_indexes(Relation NewHeap, Relation OldHeap, List *OldIndexes)
{
	StringInfo	ind_name;
	ListCell	*lc;
	List	   *result = NIL;

	pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE,
								 PROGRESS_CLUSTER_PHASE_REBUILD_INDEX);

	ind_name = makeStringInfo();

	foreach(lc, OldIndexes)
	{
		Oid			ind_oid,
					ind_oid_new,
					tbsp_oid;
		Relation	ind;
		IndexInfo  *ind_info;
		int			i,
					heap_col_id;
		List	   *colnames;
		int16		indnatts;
		Oid		   *collations,
				   *opclasses;
		HeapTuple	tup;
		bool		isnull;
		Datum		d;
		oidvector  *oidvec;
		int2vector *int2vec;
		size_t		oid_arr_size;
		size_t		int2_arr_size;
		int16	   *indoptions;
		text	   *reloptions = NULL;
		bits16		flags;
		Datum		*opclassOptions;
		NullableDatum *stattargets;

		ind_oid = lfirst_oid(lc);
		ind = index_open(ind_oid, AccessShareLock);
		ind_info = BuildIndexInfo(ind);

		tbsp_oid = ind->rd_rel->reltablespace;
		/*
		 * Index name really doesn't matter, we'll eventually use only their
		 * storage. Just make them unique within the table.
		 */
		resetStringInfo(ind_name);
		appendStringInfo(ind_name, "ind_%d",
						 list_cell_number(OldIndexes, lc));

		flags = 0;
		if (ind->rd_index->indisprimary)
			flags |= INDEX_CREATE_IS_PRIMARY;

		colnames = NIL;
		indnatts = ind->rd_index->indnatts;
		oid_arr_size = sizeof(Oid) * indnatts;
		int2_arr_size = sizeof(int16) * indnatts;

		collations = (Oid *) palloc(oid_arr_size);
		for (i = 0; i < indnatts; i++)
		{
			char	   *colname;

			heap_col_id = ind->rd_index->indkey.values[i];
			if (heap_col_id > 0)
			{
				Form_pg_attribute att;

				/* Normal attribute. */
				att = TupleDescAttr(OldHeap->rd_att, heap_col_id - 1);
				colname = pstrdup(NameStr(att->attname));
				collations[i] = att->attcollation;
			}
			else if (heap_col_id == 0)
			{
				HeapTuple	tuple;
				Form_pg_attribute att;

				/*
				 * Expression column is not present in relcache. What we need
				 * here is an attribute of the *index* relation.
				 */
				tuple = SearchSysCache2(ATTNUM,
										ObjectIdGetDatum(ind_oid),
										Int16GetDatum(i + 1));
				if (!HeapTupleIsValid(tuple))
					elog(ERROR,
						 "cache lookup failed for attribute %d of relation %u",
						 i + 1, ind_oid);
				att = (Form_pg_attribute) GETSTRUCT(tuple);
				colname = pstrdup(NameStr(att->attname));
				collations[i] = att->attcollation;
				ReleaseSysCache(tuple);
			}
			else
				elog(ERROR, "Unexpected column number: %d",
					 heap_col_id);

			colnames = lappend(colnames, colname);
		}

		/*
		 * Special effort needed for variable length attributes of
		 * Form_pg_index.
		 */
		tup = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(ind_oid));
		if (!HeapTupleIsValid(tup))
			elog(ERROR, "cache lookup failed for index %u", ind_oid);
		d = SysCacheGetAttr(INDEXRELID, tup, Anum_pg_index_indclass, &isnull);
		Assert(!isnull);
		oidvec = (oidvector *) DatumGetPointer(d);
		opclasses = (Oid *) palloc(oid_arr_size);
		memcpy(opclasses, oidvec->values, oid_arr_size);

		d = SysCacheGetAttr(INDEXRELID, tup, Anum_pg_index_indoption,
							&isnull);
		Assert(!isnull);
		int2vec = (int2vector *) DatumGetPointer(d);
		indoptions = (int16 *) palloc(int2_arr_size);
		memcpy(indoptions, int2vec->values, int2_arr_size);
		ReleaseSysCache(tup);

		tup = SearchSysCache1(RELOID, ObjectIdGetDatum(ind_oid));
		if (!HeapTupleIsValid(tup))
			elog(ERROR, "cache lookup failed for index relation %u", ind_oid);
		d = SysCacheGetAttr(RELOID, tup, Anum_pg_class_reloptions, &isnull);
		reloptions = !isnull ? DatumGetTextPCopy(d) : NULL;
		ReleaseSysCache(tup);

		opclassOptions = palloc0(sizeof(Datum) * ind_info->ii_NumIndexAttrs);
		for (i = 0; i < ind_info->ii_NumIndexAttrs; i++)
			opclassOptions[i] = get_attoptions(ind_oid, i + 1);

		stattargets = get_index_stattargets(ind_oid, ind_info);

		/*
		 * Neither parentIndexRelid nor parentConstraintId needs to be passed
		 * since the new catalog entries (pg_constraint, pg_inherits) would
		 * eventually be dropped. Therefore there's no need to record valid
		 * dependency on parents.
		 */
		ind_oid_new = index_create(NewHeap,
								   ind_name->data,
								   InvalidOid,
								   InvalidOid,	/* parentIndexRelid */
								   InvalidOid,	/* parentConstraintId */
								   InvalidOid,
								   ind_info,
								   colnames,
								   ind->rd_rel->relam,
								   tbsp_oid,
								   collations,
								   opclasses,
								   opclassOptions,
								   indoptions,
								   stattargets,
								   PointerGetDatum(reloptions),
								   flags,	/* flags */
								   0,	/* constr_flags */
								   false,	/* allow_system_table_mods */
								   false,	/* is_internal */
								   NULL /* constraintId */
			);
		result = lappend_oid(result, ind_oid_new);

		index_close(ind, AccessShareLock);
		list_free_deep(colnames);
		pfree(collations);
		pfree(opclasses);
		pfree(indoptions);
		if (reloptions)
			pfree(reloptions);
	}

	return result;
}

static void
init_rel_reopen_info(RelReopenInfo *rri, Relation *rel_p, Oid relid,
					 LOCKMODE lockmode_orig, LOCKMODE lockmode_new)
{
	rri->rel_p = rel_p;
	rri->relid = relid;
	rri->lockmode_orig = lockmode_orig;
	rri->lockmode_new = lockmode_new;
}

/*
 * Unlock and close relations specified by items of the 'rels' array. 'nrels'
 * is the number of items.
 *
 * Information needed to (re)open the relations (or to issue meaningful ERROR)
 * is added to the array items.
 */
static void
unlock_and_close_relations(RelReopenInfo *rels, int nrel)
{
	int		i;
	RelReopenInfo	*rri;

	/*
	 * First, retrieve the information that we will need for re-opening.
	 *
	 * We could close (and unlock) each relation as soon as we have gathered
	 * the related information, but then we would have to be careful not to
	 * unlock the table until we have the info on all its indexes. (Once we
	 * unlock the table, any index can be dropped, and thus we can fail to get
	 * the name we want to report if re-opening fails.) It seem simpler to
	 * separate the work into two iterations.
	 */
	for (i = 0; i < nrel; i++)
	{
		Relation	rel;

		rri = &rels[i];
		rel = *rri->rel_p;

		if (rel)
		{
			Assert(CheckRelationLockedByMe(rel, rri->lockmode_orig, false));
			Assert(!OidIsValid(rri->relid));

			rri->relid = RelationGetRelid(rel);
			rri->relkind = rel->rd_rel->relkind;
			rri->relname = pstrdup(RelationGetRelationName(rel));
		}
		else
		{
			Assert(OidIsValid(rri->relid));

			rri->relname = get_rel_name(rri->relid);
			rri->relkind = get_rel_relkind(rri->relid);
		}
	}

	/* Second, close the relations. */
	for (i = 0; i < nrel; i++)
	{
		Relation	rel;

		rri = &rels[i];
		rel = *rri->rel_p;

		/* Close the relation if the caller passed one. */
		if (rel)
		{
			if (rri->relkind == RELKIND_RELATION)
				table_close(rel, rri->lockmode_orig);
			else
			{
				Assert(rri->relkind == RELKIND_INDEX);

				index_close(rel, rri->lockmode_orig);
			}
		}
	}
}

/*
 * Re-open the relations closed previously by unlock_and_close_relations().
 */
static void
reopen_relations(RelReopenInfo *rels, int nrel)
{
	for (int i = 0; i < nrel; i++)
	{
		RelReopenInfo	*rri = &rels[i];
		Relation	rel;

		if (rri->relkind == RELKIND_RELATION)
		{
			rel = try_table_open(rri->relid, rri->lockmode_new);
		}
		else
		{
			Assert(rri->relkind == RELKIND_INDEX);

			rel = try_index_open(rri->relid, rri->lockmode_new);
		}

		if (rel == NULL)
		{
			const char	*kind_str;

			kind_str = (rri->relkind == RELKIND_RELATION) ? "table" : "index";
			ereport(ERROR,
					(errmsg("could not open \%s \"%s\"", kind_str,
							rri->relname),
					 errhint("The %s could have been dropped by another transaction.",
							 kind_str)));
		}
		*rri->rel_p = rel;

		pfree(rri->relname);
	}
}
