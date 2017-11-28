/*-------------------------------------------------------------------------
 *
 * lockcmds.c
 *	  LOCK command support code
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/lockcmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/namespace.h"
#include "catalog/pg_inherits_fn.h"
#include "commands/lockcmds.h"
#include "miscadmin.h"
#include "parser/parse_clause.h"
#include "parser/parsetree.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "rewrite/rewriteHandler.h"
#include "access/heapam.h"

static void LockTableRecurse(Oid reloid, LOCKMODE lockmode, bool nowait, Oid userid);
static AclResult LockTableAclCheck(Oid relid, LOCKMODE lockmode, Oid userid);
static void RangeVarCallbackForLockTable(const RangeVar *rv, Oid relid,
							 Oid oldrelid, void *arg);
static void LockViewRecurse(Oid reloid, Oid root_reloid, LOCKMODE lockmode, bool nowait);
static void LockViewCheck(Relation view, Query *viewquery);

/*
 * LOCK TABLE
 */
void
LockTableCommand(LockStmt *lockstmt)
{
	ListCell   *p;

	/*---------
	 * During recovery we only accept these variations:
	 * LOCK TABLE foo IN ACCESS SHARE MODE
	 * LOCK TABLE foo IN ROW SHARE MODE
	 * LOCK TABLE foo IN ROW EXCLUSIVE MODE
	 * This test must match the restrictions defined in LockAcquireExtended()
	 *---------
	 */
	if (lockstmt->mode > RowExclusiveLock)
		PreventCommandDuringRecovery("LOCK TABLE");

	/*
	 * Iterate over the list and process the named relations one at a time
	 */
	foreach(p, lockstmt->relations)
	{
		RangeVar   *rv = (RangeVar *) lfirst(p);
		bool		recurse = rv->inh;
		Oid			reloid;

		reloid = RangeVarGetRelidExtended(rv, lockstmt->mode, false,
										  lockstmt->nowait,
										  RangeVarCallbackForLockTable,
										  (void *) &lockstmt->mode);

		if (get_rel_relkind(reloid) == RELKIND_VIEW)
			LockViewRecurse(reloid, reloid, lockstmt->mode, lockstmt->nowait);
		else if (recurse)
			LockTableRecurse(reloid, lockstmt->mode, lockstmt->nowait, GetUserId());
	}
}

/*
 * Before acquiring a table lock on the named table, check whether we have
 * permission to do so.
 */
static void
RangeVarCallbackForLockTable(const RangeVar *rv, Oid relid, Oid oldrelid,
							 void *arg)
{
	LOCKMODE	lockmode = *(LOCKMODE *) arg;
	char		relkind;
	AclResult	aclresult;

	if (!OidIsValid(relid))
		return;					/* doesn't exist, so no permissions check */
	relkind = get_rel_relkind(relid);
	if (!relkind)
		return;					/* woops, concurrently dropped; no permissions
								 * check */


	/* Currently, we only allow plain tables or auto-updatable views to be locked */
	if (relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE &&
		relkind != RELKIND_VIEW)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table or view",
						rv->relname)));

	/* Check permissions. */
	aclresult = LockTableAclCheck(relid, lockmode, GetUserId());
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_CLASS, rv->relname);
}

/*
 * Apply LOCK TABLE recursively over an inheritance tree
 *
 * We use find_inheritance_children not find_all_inheritors to avoid taking
 * locks far in advance of checking privileges.  This means we'll visit
 * multiply-inheriting children more than once, but that's no problem.
 */
static void
LockTableRecurse(Oid reloid, LOCKMODE lockmode, bool nowait, Oid userid)
{
	List	   *children;
	ListCell   *lc;

	children = find_inheritance_children(reloid, NoLock);

	foreach(lc, children)
	{
		Oid			childreloid = lfirst_oid(lc);
		AclResult	aclresult;

		/* Check permissions before acquiring the lock. */
		aclresult = LockTableAclCheck(childreloid, lockmode, userid);
		if (aclresult != ACLCHECK_OK)
		{
			char	   *relname = get_rel_name(childreloid);

			if (!relname)
				continue;		/* child concurrently dropped, just skip it */
			aclcheck_error(aclresult, ACL_KIND_CLASS, relname);
		}

		/* We have enough rights to lock the relation; do so. */
		if (!nowait)
			LockRelationOid(childreloid, lockmode);
		else if (!ConditionalLockRelationOid(childreloid, lockmode))
		{
			/* try to throw error by name; relation could be deleted... */
			char	   *relname = get_rel_name(childreloid);

			if (!relname)
				continue;		/* child concurrently dropped, just skip it */
			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("could not obtain lock on relation \"%s\"",
							relname)));
		}

		/*
		 * Even if we got the lock, child might have been concurrently
		 * dropped. If so, we can skip it.
		 */
		if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(childreloid)))
		{
			/* Release useless lock */
			UnlockRelationOid(childreloid, lockmode);
			continue;
		}

		LockTableRecurse(childreloid, lockmode, nowait, userid);
	}
}

/*
 * Apply LOCK TABLE recursively over a view
 */
static void
LockViewRecurse(Oid reloid, Oid root_reloid, LOCKMODE lockmode, bool nowait)
{
	Relation		 view;
	Query			*viewquery;
	RangeTblRef		*rtr;
	RangeTblEntry	*base_rte;
	Oid				 baseoid;
	AclResult		 aclresult;
	char			*relname;
	char			 relkind;

	view = heap_open(reloid, NoLock);
	viewquery = get_view_query(view);

	/* Check whether the view is lockable */
	LockViewCheck(view, viewquery);

	heap_close(view, NoLock);

	Assert(list_length(viewquery->jointree->fromlist) == 1);
	rtr = linitial_node(RangeTblRef, viewquery->jointree->fromlist);
	base_rte = rt_fetch(rtr->rtindex, viewquery->rtable);
	Assert(base_rte->rtekind == RTE_RELATION);

	baseoid = base_rte->relid;
	relname = get_rel_name(baseoid);
	relkind = get_rel_relkind(baseoid);

	/* Currently, we only allow plain tables or auto-updatable views to be locked */
	if (relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE &&
		relkind != RELKIND_VIEW)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table or view",
						relname)));

	/* Check infinite recursion in the view definition */
	if (baseoid == root_reloid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("infinite recursion detected in rules for relation \"%s\"",
						get_rel_name(root_reloid))));

	/* Check permissions with the view owner's priviledge. */
	aclresult = LockTableAclCheck(baseoid, lockmode, view->rd_rel->relowner);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_CLASS, relname);

	/* We have enough rights to lock the relation; do so. */
	if (!nowait)
		LockRelationOid(baseoid, lockmode);
	else if (!ConditionalLockRelationOid(baseoid, lockmode))
		ereport(ERROR,
				(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
				 errmsg("could not obtain lock on relation \"%s\"",
						relname)));

	if (relkind == RELKIND_VIEW)
		LockViewRecurse(baseoid, root_reloid, lockmode, nowait);
	else if (base_rte->inh)
		LockTableRecurse(baseoid, lockmode, nowait, view->rd_rel->relowner);
}

/*
 * Check whether the current user is permitted to lock this relation.
 */
static AclResult
LockTableAclCheck(Oid reloid, LOCKMODE lockmode, Oid userid)
{
	AclResult	aclresult;
	AclMode		aclmask;

	/* Verify adequate privilege */
	if (lockmode == AccessShareLock)
		aclmask = ACL_SELECT;
	else if (lockmode == RowExclusiveLock)
		aclmask = ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE;
	else
		aclmask = ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE;

	aclresult = pg_class_aclcheck(reloid, userid, aclmask);

	return aclresult;
}

/*
 * Check whether the view is lockable.
 *
 * Currently, only auto-updatable views can be locked, that is,
 * views whose definition are simple and that doesn't have
 * instead of rules or triggers are lockable.
 */
static void
LockViewCheck(Relation view, Query *viewquery)
{
	const char	*relname = RelationGetRelationName(view);
	const char	*auto_update_detail;
	int			 i;

	auto_update_detail = view_query_is_auto_updatable(viewquery, false);
	if (auto_update_detail)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot lock view \"%s\"", relname),
					 errdetail_internal("%s", _(auto_update_detail))));


	/* Confirm the view doesn't have instead of rules */
	for (i=0; i<view->rd_rules->numLocks; i++)
	{
		if (view->rd_rules->rules[i]->isInstead && view->rd_rules->rules[i]->event != CMD_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot lock view \"%s\"", relname),
					 errdetail_internal("views that have an INSTEAD OF rule are not lockable")));
	}

	/* Confirm the view doesn't have instead of triggers */
	if (view->trigdesc &&
		(view->trigdesc->trig_insert_instead_row ||
		 view->trigdesc->trig_update_instead_row ||
		 view->trigdesc->trig_delete_instead_row))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot lock view \"%s\"", relname),
				 errdetail_internal("views that have an INSTEAD OF trigger are not lockable")));
}
