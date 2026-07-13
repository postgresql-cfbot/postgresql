/*-------------------------------------------------------------------------
 *
 * keyjoincmds.c
 *	  Commands for manipulating stored key-join proofs.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/keyjoincmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/skey.h"
#include "access/table.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_rewrite.h"
#include "common/int.h"
#include "commands/keyjoin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "parser/parse_key_join.h"
#include "storage/lmgr.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"

static Oid	get_rule_event_relation(Oid ruleOid);
static List *find_dependent_key_join_objects(Oid refclassid, Oid refobjid);
static void revalidate_dependent_key_join_objects(List *objects);
static void revalidate_dependent_key_join_object(const ObjectAddress *object);
static bool object_address_list_member(List *objects, Oid classId,
									   Oid objectId);
static int	object_address_list_cmp(const ListCell *a, const ListCell *b);
static ObjectAddress *make_object_address(Oid classId, Oid objectId);
static void revalidate_dependent_key_join_relation(Oid relationOid);
static Node *revalidate_key_join_node(Node *stored);
static void reconcile_key_join_dependencies(const ObjectAddress *owner,
											List *revalidated);

static Oid
get_rule_event_relation(Oid ruleOid)
{
	Relation	rewriteRel;
	ScanKeyData key[1];
	SysScanDesc scan;
	HeapTuple	tup;
	Oid			result = InvalidOid;

	rewriteRel = table_open(RewriteRelationId, AccessShareLock);
	ScanKeyInit(&key[0],
				Anum_pg_rewrite_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ruleOid));
	scan = systable_beginscan(rewriteRel, RewriteOidIndexId, true,
							  NULL, 1, key);
	tup = systable_getnext(scan);
	if (!HeapTupleIsValid(tup))
	{
		/*
		 * The dependent rule can be dropped after pg_depend is scanned but
		 * before this lookup acquires pg_rewrite.  Then the owning object is
		 * gone too, so there is no stored proof left to revalidate.
		 */
		systable_endscan(scan);
		table_close(rewriteRel, AccessShareLock);
		return InvalidOid;
	}
	result = ((Form_pg_rewrite) GETSTRUCT(tup))->ev_class;
	Assert(OidIsValid(result));
	systable_endscan(scan);
	table_close(rewriteRel, AccessShareLock);

	return result;
}

static List *
find_dependent_key_join_objects(Oid refclassid, Oid refobjid)
{
	Relation	depRel;
	ScanKeyData key[2];
	SysScanDesc scan;
	HeapTuple	tup;
	List	   *result = NIL;

	depRel = table_open(DependRelationId, AccessShareLock);
	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(refclassid));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(refobjid));
	scan = systable_beginscan(depRel, DependReferenceIndexId, true,
							  NULL, 2, key);

	while (HeapTupleIsValid((tup = systable_getnext(scan))))
	{
		Form_pg_depend dep = (Form_pg_depend) GETSTRUCT(tup);
		Oid			classid;
		Oid			objectid;

		if (dep->deptype != DEPENDENCY_KEYJOIN)
			continue;

		if (dep->classid == RewriteRelationId)
		{
			classid = RelationRelationId;
			objectid = get_rule_event_relation(dep->objid);
			if (!OidIsValid(objectid))
				continue;
		}
		else
			elog(ERROR, "unexpected class %u for key join proof dependent",
				 dep->classid);

		if (!object_address_list_member(result, classid, objectid))
			result = lappend(result, make_object_address(classid, objectid));
	}

	systable_endscan(scan);
	table_close(depRel, AccessShareLock);

	list_sort(result, object_address_list_cmp);
	return result;
}

static void
revalidate_dependent_key_join_objects(List *objects)
{
	foreach_ptr(ObjectAddress, object, objects)
		revalidate_dependent_key_join_object(object);
}

static void
revalidate_dependent_key_join_object(const ObjectAddress *object)
{
	switch (object->classId)
	{
		case RelationRelationId:
			revalidate_dependent_key_join_relation(object->objectId);
			break;
		default:
			elog(ERROR, "unexpected class %u for key join proof dependent",
				 object->classId);
			break;
	}
}

static void
revalidate_dependent_key_join_relation(Oid relationOid)
{
	Relation	rel;

	rel = relation_open(relationOid, AccessShareLock);

	/*
	 * Walk every rule attached to the dependent relation.  A stored key-join
	 * proof can live in any rule action: a view's or matview's _RETURN rule,
	 * an INSTEAD-OF rule on a view, a DO ALSO/INSTEAD rule on a plain table,
	 * etc.  Each key-join-bearing action must be revalidated so DDL that
	 * would make the proof unprovable is rejected with the existing "key join
	 * cannot be proven from available constraints" error.
	 *
	 * Each rule's proofs are re-derived against the current catalog.  If a
	 * proof no longer holds, or its executable form would change, the
	 * triggering DDL is rejected.  Otherwise pg_depend is reconciled (per rule)
	 * to the re-derived evidence, keeping it the authoritative record of what
	 * the proof relies on.
	 */
	if (rel->rd_rules == NULL)
	{
		/*
		 * A rule can be dropped after pg_depend is scanned but before this
		 * relation lock is acquired.  RelationBuildRuleLock() leaves
		 * rd_rules NULL when no rules remain, so there is no stored proof
		 * left on this relation to revalidate.
		 */
		relation_close(rel, AccessShareLock);
		return;
	}
	Assert(rel->rd_rules->numLocks > 0);
	for (int i = 0; i < rel->rd_rules->numLocks; i++)
	{
		RewriteRule *rule = rel->rd_rules->rules[i];
		ObjectAddress owner;
		List	   *revalidated = NIL;
		Node	   *node;

		foreach_node(Query, action, rule->actions)
		{
			node = revalidate_key_join_node((Node *) action);
			if (node != NULL)
				revalidated = lappend(revalidated, node);
		}

		if (rule->qual != NULL)
		{
			node = revalidate_key_join_node(rule->qual);
			if (node != NULL)
				revalidated = lappend(revalidated, node);
		}

		/* A rule's proof dependencies are recorded under the rule's OID. */
		ObjectAddressSet(owner, RewriteRelationId, rule->ruleId);
		reconcile_key_join_dependencies(&owner, revalidated);
	}

	relation_close(rel, NoLock);
}

/*
 * revalidate_key_join_node
 *		Re-derive the key-join proofs in a copy of one stored tree against the
 *		current catalog and verify the copy's executable form is unchanged.
 *
 *		Returns the re-derived copy (whose KeyJoinNode evidence now reflects the
 *		current catalog) so the caller can reconcile pg_depend from it, or NULL
 *		if the tree carries no key join.  ereports if the proof can no longer be
 *		derived or if re-deriving it would change the stored executable form.
 */
static Node *
revalidate_key_join_node(Node *stored)
{
	Node	   *copy;

	Assert(stored != NULL);

	if (!storedNodeContainsKeyJoin(stored))
		return NULL;

	copy = copyObject(stored);
	revalidateStoredKeyJoinProofsInNode(copy);

	if (!keyJoinExecutableFormUnchanged(stored, copy))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FOREIGN_KEY),
				 errmsg("stored key join proof would change its executable form")));

	return copy;
}

/*
 * reconcile_key_join_dependencies
 *		Replace the owner's DEPENDENCY_KEYJOIN edges with the proof dependencies
 *		of the re-derived nodes.
 *
 *		pg_depend is the single source of truth for a stored proof's evidence.
 *		Re-recording it from the freshly re-derived proof keeps it a faithful
 *		superset of what the proof relies on, so a later DROP of any supporting
 *		object still triggers revalidation -- even when the proof re-pinned to an
 *		equivalent object (e.g. a unique index rebuilt by REINDEX CONCURRENTLY).
 */
static void
reconcile_key_join_dependencies(const ObjectAddress *owner, List *revalidated)
{
	if (revalidated == NIL)
		return;

	deleteDependencyRecordsForKeyJoin(owner->classId, owner->objectId);
	recordDependencyOnKeyJoinProofs(owner, (Node *) revalidated);
}

void
RevalidateDependentKeyJoinObjectsOnConstraint(Oid constraintOid)
{
	List	   *objects;

	Assert(OidIsValid(constraintOid));

	objects = find_dependent_key_join_objects(ConstraintRelationId,
											  constraintOid);
	revalidate_dependent_key_join_objects(objects);
}

void
RevalidateDependentKeyJoinObjectsOnRelation(Oid relationOid)
{
	List	   *objects;

	Assert(OidIsValid(relationOid));

	objects = find_dependent_key_join_objects(RelationRelationId,
											  relationOid);
	revalidate_dependent_key_join_objects(objects);
}

void
RevalidateDependentKeyJoinObjectsOnProcedure(Oid procOid)
{
	List	   *objects;

	Assert(OidIsValid(procOid));

	objects = find_dependent_key_join_objects(ProcedureRelationId, procOid);
	revalidate_dependent_key_join_objects(objects);
}

static bool
object_address_list_member(List *objects, Oid classId, Oid objectId)
{
	foreach_ptr(ObjectAddress, object, objects)
	{
		Assert(object->objectSubId == 0);

		if (object->classId == classId && object->objectId == objectId)
			return true;
	}
	return false;
}

static int
object_address_list_cmp(const ListCell *a, const ListCell *b)
{
	const ObjectAddress *obj1 = (const ObjectAddress *) lfirst(a);
	const ObjectAddress *obj2 = (const ObjectAddress *) lfirst(b);
	int			cmp;

	Assert(obj1->objectSubId == 0);
	Assert(obj2->objectSubId == 0);

	cmp = pg_cmp_u32(obj1->classId, obj2->classId);
	if (cmp != 0)
		return cmp;
	return pg_cmp_u32(obj1->objectId, obj2->objectId);
}

static ObjectAddress *
make_object_address(Oid classId, Oid objectId)
{
	ObjectAddress *object = palloc_object(ObjectAddress);

	ObjectAddressSet(*object, classId, objectId);
	return object;
}
