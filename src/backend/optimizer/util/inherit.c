/*-------------------------------------------------------------------------
 *
 * inherit.c
 *	  Routines to process child relations in inheritance trees
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/inherit.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"
#include "access/table.h"
#include "catalog/partition.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/appendinfo.h"
#include "optimizer/clauses.h"
#include "optimizer/inherit.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "partitioning/partprune.h"
#include "utils/rel.h"


static PlannerInfo *create_inherited_target_child_root(PlannerInfo *root,
									AppendRelInfo *appinfo);
static void expand_inherited_rtentry(PlannerInfo *root, RelOptInfo *rel,
					  RangeTblEntry *rte, Index rti);
static void expand_nonpartitioned_inherited_rtentry(PlannerInfo *root,
						 RangeTblEntry *rte, Index rti, RelOptInfo *rel);
static void expand_partitioned_rtentry(PlannerInfo *root,
						   RangeTblEntry *parentrte,
						   Index parentRTindex, RelOptInfo *parentrel);
static RelOptInfo *add_inheritance_child_rel(PlannerInfo *root,
						  RangeTblEntry *parentrte,
						  Index parentRTindex, RelOptInfo *parentrel,
						  PlanRowMark *top_parentrc, Relation childrel,
						  RangeTblEntry **childrte_p, Index *childRTindex_p);
static Bitmapset *translate_col_privs(const Bitmapset *parent_privs,
					List *translated_vars);
static RelOptInfo *build_inheritance_child_rel(PlannerInfo *root,
					   RelOptInfo *parent,
					   Index childRTindex);
static List *add_rowmark_junk_columns(PlannerInfo *root, PlanRowMark *rc);
static bool apply_child_basequals(PlannerInfo *root, RelOptInfo *rel,
					  RelOptInfo *childrel,
					  RangeTblEntry *childRTE, AppendRelInfo *appinfo);

/*
 * expand_inherited_tables
 *		Expand each rangetable entry that represents an inheritance set
 *		into an "append relation".  At the conclusion of this process,
 *		the "inh" flag is set in all and only those RTEs that are append
 *		relation parents.
 *
 * Note that although we're calling the combined output of inheritance set
 * relations an "append relation" here, the caller may not always want to
 * combine the relations.  For example, if the parent of the inheritance
 * set is the query's target relation, each child relation is processed
 * on its own as the query's target relation.
 */
void
expand_inherited_tables(PlannerInfo *root)
{
	int			orig_rtable_size;
	Index		rti;

	Assert(root->simple_rel_array_size > 0);
	orig_rtable_size = root->simple_rel_array_size;

	/*
	 * expand_inherited_rtentry may add RTEs to parse->rtable. The function is
	 * expected to recursively handle any RTEs that it creates with inh=true.
	 * So just scan as far as the original end of the rtable list.
	 */
	for (rti = 1; rti < orig_rtable_size; rti++)
	{
		RelOptInfo *brel = root->simple_rel_array[rti];
		RangeTblEntry *rte = root->simple_rte_array[rti];

		/* there may be empty slots corresponding to non-baserel RTEs */
		if (brel == NULL)
			continue;

		if (rte->inh)
			expand_inherited_rtentry(root, brel, rte, rti);
	}
}

/*
 * expand_inherited_rtentry
 *		This initializes RelOptInfos for inheritance child relations if the
 *		passed-in relation has any
 *
 * 'rel' is the parent relation, whose range table entry ('rte') has been
 * marked to require adding children.  Parent could either be a subquery (if
 * we flattened UNION ALL query) or a table that's known to have (or once had)
 * inheritance children.  The latter consists of both regular inheritance
 * parents and partitioned tables.
 *
 * For a subquery parent, there is not much to be done here because the
 * children's RTEs are already present in the query, so we just initialize
 * RelOptInfos for them.  Also, the AppendRelInfos for child subqueries
 * have already been added.
 *
 * For tables, we need to add the children to the range table and initialize
 * AppendRelInfos, RelOptInfos, and PlanRowMarks (if any) for them.  For
 * a partitioned parent, we only add the children remaining after pruning.
 * For regular inheritance parents, we find the children using
 * find_all_inheritors and add all of them.
 *
 * If it turns out that there are no children, then we set rte->inh to false
 * to let the caller know that only the parent table needs to be scanned.  The
 * caller can accordingly switch to a non-Append path.  For a partitioned
 * parent, that means an empty relation because parents themselves contain no
 * data.
 *
 * For the regular inheritance case, the parent also gets another RTE with
 * inh = false to represent it as a child to be scanned as part of the
 * inheritance set.  The original RTE is considered to represent the whole
 * inheritance set.
 */
static void
expand_inherited_rtentry(PlannerInfo *root, RelOptInfo *rel,
						 RangeTblEntry *rte, Index rti)
{
	Assert(rte->inh);
	/* Inheritance parent (partitioned or not) or UNION ALL parent subquery. */
	Assert(rte->rtekind == RTE_RELATION || rte->rtekind == RTE_SUBQUERY);

	/*
	 * UNION ALL children already got RTEs and AppendRelInfos, so just build
	 * RelOptInfos and return.
	 *
	 * It might be a bit odd that this code is in this, because there is
	 * nothing to expand really.
	 */
	if (rte->rtekind == RTE_SUBQUERY)
	{
		ListCell   *l;

		/*
		 * We don't need to use expand_planner_arrays in this case, because
		 * no new child RTEs are created.  setup_simple_rel_arrays() and
		 * setup_append_rel_array would've considered these child RTEs when
		 * allocating space for various arrays.
		 */
		foreach(l, root->append_rel_list)
		{
			AppendRelInfo *appinfo = lfirst(l);
			Index		childRTindex = appinfo->child_relid;

			if (appinfo->parent_relid != rti)
				continue;

			Assert(childRTindex < root->simple_rel_array_size);
			Assert(root->simple_rte_array[childRTindex] != NULL);

			/*
			 * We set the correct value of baserestricinfo and
			 * baserestrict_min_security below.
			 */
			root->simple_rel_array[childRTindex] =
				build_inheritance_child_rel(root, rel, appinfo->child_relid);
		}
	}
	else
	{
		Assert(rte->rtekind == RTE_RELATION);
		Assert(has_subclass(rte->relid));

		/*
		 * The rewriter should already have obtained an appropriate lock on
		 * each relation named in the query.  However, for each child relation
		 * we add to the query, we must obtain an appropriate lock, because
		 * this will be the first use of those relations in the
		 * parse/rewrite/plan pipeline.  Child rels should use the same
		 * lockmode as their parent.
		 */
		Assert(rte->rellockmode != NoLock);

		if (rte->relkind == RELKIND_PARTITIONED_TABLE)
			expand_partitioned_rtentry(root, rte, rti, rel);
		else
			expand_nonpartitioned_inherited_rtentry(root, rte, rti, rel);
	}
}

/*
 * expand_nonpartitioned_inherited_rtentry
 *		Add entries for all the child tables to the query's rangetable, and
 *		build AppendRelInfo nodes for all the child tables and add them to
 *		root->append_rel_list.
 *
 * Note that the original RTE is considered to represent the whole
 * inheritance set.  The first of the generated RTEs is an RTE for the same
 * table, but with inh = false, to represent the parent table in its role
 * as a simple member of the inheritance set.
 *
 * A childless table is never considered to be an inheritance set. For
 * regular inheritance, a parent RTE must always have at least two associated
 * AppendRelInfos: one corresponding to the parent table as a simple member of
 * inheritance set and one or more corresponding to the actual children.
 */
static void
expand_nonpartitioned_inherited_rtentry(PlannerInfo *root,
										RangeTblEntry *rte,
										Index rti,
										RelOptInfo *rel)
{
	Oid			parentOID;
	PlanRowMark *oldrc;
	LOCKMODE	lockmode = rte->rellockmode;
	List	   *inhOIDs;
	ListCell   *l;
	int			num_children;
	int			num_children_added = 0;

	Assert(rte->rtekind == RTE_RELATION);
	Assert(lockmode != NoLock);
	parentOID = rte->relid;

	/* Scan for all members of inheritance set, acquire needed locks */
	inhOIDs = find_all_inheritors(parentOID, lockmode, NULL);

	/*
	 * Check that there's at least one descendant, else treat as no-child
	 * case.  This could happen despite has_subclass() check performed by
	 * subquery_planner, if table once had a child but no longer does.
	 */
	num_children = list_length(inhOIDs);
	if (num_children < 2)
	{
		/* Clear flag before returning */
		rte->inh = false;
		return;
	}

	/*
	 * If parent relation is selected FOR UPDATE/SHARE, preprocess_rowmarks
	 * should've set isParent = true.  We'll generate a new PlanRowMark for
	 * each child.
	 */
	oldrc = get_plan_rowmark(root->rowMarks, rti);
	Assert(oldrc == NULL || oldrc->isParent);

	/*
	 * Must expand PlannerInfo arrays by num_children before we can add
	 * children.
	 */
	Assert(num_children > 0);
	expand_planner_arrays(root, num_children);

	foreach(l, inhOIDs)
	{
		Oid			childOID = lfirst_oid(l);
		Relation	newrelation;
		RangeTblEntry *childrte;
		Index		childRTindex;

		/* Already locked above. */
		newrelation = heap_open(childOID, NoLock);

		/*
		 * It is possible that the parent table has children that are temp
		 * tables of other backends.  We cannot safely access such tables
		 * (because of buffering issues), and the best thing to do seems
		 * to be to silently ignore them.
		 */
		if (childOID != parentOID && RELATION_IS_OTHER_TEMP(newrelation))
		{
			table_close(newrelation, lockmode);
			continue;
		}

		(void) add_inheritance_child_rel(root, rte, rti, rel, oldrc,
										 newrelation, &childrte,
										 &childRTindex);
		Assert(childrte != NULL);
		/* All regular inheritance children are leaf children. */
		Assert(!childrte->inh);
		Assert(childRTindex > 0);

		/* Close child relations, but keep locks */
		heap_close(newrelation, NoLock);
		num_children_added++;
	}

	/*
	 * If all children, including the parent (as child rel), were
	 * excluded, mark the parent rel as empty.  If all the children were temp
	 * tables, pretend it's a non-inheritance situation; we don't need Append
	 * node in that case.  The duplicate RTE we added for the parent table is
	 * harmless, so we don't bother to get rid of it; ditto for the useless
	 * PlanRowMark node.
	 */
	if (num_children_added == 0)
		mark_dummy_rel(rel);
	else if (num_children_added == 1)
		rte->inh = false;

	/*
	 * Add junk columns needed by the row mark if any and also add the
	 * relevant expressions to the root parent's reltarget.
	 */
	if (oldrc)
	{
		List   *tlist = add_rowmark_junk_columns(root, oldrc);

		build_base_rel_tlists(root, tlist);
	}
}

/*
 * expand_partitioned_rtentry
 *		Prunes unnecessary partitions of a partitioned table and adds
 *		remaining ones to the Query and the PlannerInfo
 *
 * Partitions are added to the query in order in which they are found in
 * the parent's PartitionDesc.
 */
static void
expand_partitioned_rtentry(PlannerInfo *root, RangeTblEntry *parentrte,
						   Index parentRTindex, RelOptInfo *parentrel)
{
	LOCKMODE	lockmode = parentrte->rellockmode;
	PlanRowMark *rootrc = NULL;
	int			i;
	Bitmapset  *partindexes;
	Index		rootParentRTindex = parentrel->inh_root_parent > 0 ?
									parentrel->inh_root_parent :
									parentRTindex;

	/*
	 * Initialize partitioned_child_rels to contain this RT index.
	 *
	 * Note that during the set_append_rel_pathlist() phase, values of the
	 * indexes of partitioned relations that appear down in the tree will be
	 * bubbled up into root parent's list so that when we've created Paths for
	 * all the children, the root table's list will contain all such indexes.
	 */
	parentrel->partitioned_child_rels = list_make1_int(parentRTindex);

	/* Perform pruning. */
	partindexes = prune_append_rel_partitions(parentrel);
	parentrel->live_parts = partindexes;

	/* Must expand PlannerInfo arrays before we can add children. */
	if (bms_num_members(partindexes) > 0)
		expand_planner_arrays(root, bms_num_members(partindexes));

	/*
	 * For partitioned tables, we also store the partition RelOptInfo
	 * pointers in the parent's RelOptInfo.
	 */
	parentrel->part_rels = (RelOptInfo **) palloc0(sizeof(RelOptInfo *) *
												   parentrel->nparts);

	rootrc = get_plan_rowmark(root->rowMarks, rootParentRTindex);
	Assert(rootrc == NULL || rootrc->isParent);
	i = -1;
	while ((i = bms_next_member(partindexes, i)) >= 0)
	{
		Oid		childOID = parentrel->part_oids[i];
		Relation	newrelation;
		RelOptInfo *childrel;
		RangeTblEntry *childrte;
		Index		childRTindex;

		/*
		 * Open rel; this's the first time of opening partitions for this
		 * query, so take the appropriate locks.
		 */
		newrelation = table_open(childOID, lockmode);
		Assert(!RELATION_IS_OTHER_TEMP(newrelation));

		/*
		 * A partitioned child table with 0 children is a dummy rel, so don't
		 * bother creating planner objects for it.
		 */
		if (newrelation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE &&
			RelationGetPartitionDesc(newrelation)->nparts == 0)
		{
			heap_close(newrelation, NoLock);
			continue;
		}

		childrel = add_inheritance_child_rel(root, parentrte, parentRTindex,
											 parentrel, rootrc, newrelation,
											 &childrte, &childRTindex);
		Assert(childrel != NULL);
		parentrel->part_rels[i] = childrel;

		/* Close child relations, but keep locks */
		table_close(newrelation, NoLock);

		/* If the child is partitioned itself, expand it too. */
		if (childrel->part_scheme)
		{
			Assert(childrte->inh);
			expand_partitioned_rtentry(root, childrte, childRTindex,
									   childrel);
		}
	}

	/*
	 * Add junk columns needed by the row mark if any and also add the
	 * relevant expressions to the root parent's reltarget.
	 */
	if (rootrc)
	{
		List   *tlist = add_rowmark_junk_columns(root, rootrc);

		build_base_rel_tlists(root, tlist);
	}
}

/*
 * add_inheritance_child_rel
 *		Build a RangeTblEntry, an AppendRelInfo, a PlanRowMark, and finally
 *		a RelOptInfo for an inheritance child relation.
 *
 * The return value is the RelOptInfo that's added.
 *
 * PlanRowMarks still carry the top-parent's RTI, and the top-parent's
 * allMarkTypes field still accumulates values from all descendents.
 *
 * "parentrte" and "parentRTindex" are immediate parent's RTE and
 * RTI. "top_parentrc" is top parent's PlanRowMark.
 */
static RelOptInfo *
add_inheritance_child_rel(PlannerInfo *root, RangeTblEntry *parentrte,
						  Index parentRTindex, RelOptInfo *parentrel,
						  PlanRowMark *top_parentrc, Relation childrel,
						  RangeTblEntry **childrte_p, Index *childRTindex_p)
{
	Query	   *parse = root->parse;
	Oid			childOID = RelationGetRelid(childrel);
	RangeTblEntry *childrte;
	Index		childRTindex;
	AppendRelInfo *appinfo;
	RelOptInfo *childrelopt;

	/*
	 * Build an RTE for the child, and attach to query's rangetable list. We
	 * copy most fields of the parent's RTE, but replace relation OID and
	 * relkind, and set inh appropriately.  Also, set requiredPerms to zero
	 * since all required permissions checks are done on the original RTE.
	 * Likewise, set the child's securityQuals to empty, because we only want
	 * to apply the parent's RLS conditions regardless of what RLS properties
	 * individual children may have.  (This is an intentional choice to make
	 * inherited RLS work like regular permissions checks.) The parent
	 * securityQuals will be propagated to children along with other base
	 * restriction clauses, so we don't need to do it here.
	 */
	childrte = copyObject(parentrte);
	*childrte_p = childrte;
	childrte->relid = childOID;
	childrte->relkind = childrel->rd_rel->relkind;
	/*
	 * A partitioned child will need to be expanded as an append parent
	 * itself, so set its inh to true.
	 */
	childrte->inh = (childrte->relkind == RELKIND_PARTITIONED_TABLE);
	childrte->requiredPerms = 0;
	childrte->securityQuals = NIL;
	parse->rtable = lappend(parse->rtable, childrte);
	childRTindex = list_length(parse->rtable);
	*childRTindex_p = childRTindex;

	/* Create an AppendRelInfo and add it to planner's global list. */
	appinfo = make_append_rel_info(parentrel, parentrte,
								   RelationGetDescr(childrel),
								   RelationGetRelid(childrel),
								   RelationGetForm(childrel)->reltype,
								   childRTindex);
	root->append_rel_list = lappend(root->append_rel_list, appinfo);

	/*
	 * Translate the column permissions bitmaps to the child's attnums (we
	 * have to build the translated_vars list before we can do this). But
	 * if this is the parent table, leave copyObject's result alone.
	 *
	 * Note: we need to do this even though the executor won't run any
	 * permissions checks on the child RTE.  The insertedCols/updatedCols
	 * bitmaps may be examined for trigger-firing purposes.
	 */
	if (childrte->relid != parentrte->relid)
	{
		childrte->selectedCols = translate_col_privs(parentrte->selectedCols,
													 appinfo->translated_vars);
		childrte->insertedCols = translate_col_privs(parentrte->insertedCols,
													 appinfo->translated_vars);
		childrte->updatedCols = translate_col_privs(parentrte->updatedCols,
													appinfo->translated_vars);
	}

	/*
	 * Build a PlanRowMark if parent is marked FOR UPDATE/SHARE.
	 */
	if (top_parentrc)
	{
		PlanRowMark *childrc = makeNode(PlanRowMark);

		childrc->rti = childRTindex;
		childrc->prti = top_parentrc->rti;
		childrc->rowmarkId = top_parentrc->rowmarkId;
		/* Reselect rowmark type, because relkind might not match parent */
		childrc->markType = select_rowmark_type(childrte,
												top_parentrc->strength);
		childrc->allMarkTypes = (1 << childrc->markType);
		childrc->strength = top_parentrc->strength;
		childrc->waitPolicy = top_parentrc->waitPolicy;

		/*
		 * We mark RowMarks for partitioned child tables as parent RowMarks so
		 * that the executor ignores them (except their existence means that
		 * the child tables be locked using appropriate mode).
		 */
		childrc->isParent = (childrte->relkind == RELKIND_PARTITIONED_TABLE);

		/* Include child's rowmark type in top parent's allMarkTypes */
		top_parentrc->allMarkTypes |= childrc->allMarkTypes;

		root->rowMarks = lappend(root->rowMarks, childrc);
	}

	/*
	 * Add the RelOptInfo.  Even though we may not really scan this relation
	 * for reasons such as contradictory quals, we still need to create one,
	 * because for every RTE in the query's range table, there must be an
	 * accompanying RelOptInfo.
	 */

	/* First, store the RTE and appinfos into planner arrays. */
	Assert(root->simple_rte_array[childRTindex] == NULL);
	root->simple_rte_array[childRTindex] = childrte;
	Assert(root->append_rel_array[childRTindex] == NULL);
	root->append_rel_array[childRTindex] = appinfo;

	childrelopt = build_inheritance_child_rel(root, parentrel, childRTindex);
	Assert(childrelopt != NULL);

	return childrelopt;
}

/*
 *	build_inheritance_child_rel
 *		Build a RelOptInfo for child relation of an inheritance set
 *
 * After creating the RelOptInfo for the given child RT index, it goes on to
 * initialize some of its fields based on the parent RelOptInfo.
 *
 * If the quals in baserestrictinfo turn out to be self-contradictory,
 * RelOptInfo is marked dummy before returning.
 */
static RelOptInfo *
build_inheritance_child_rel(PlannerInfo *root,
					   RelOptInfo *parent,
					   Index childRTindex)
{
	RelOptInfo *childrel;
	RangeTblEntry *childRTE = root->simple_rte_array[childRTindex];
	AppendRelInfo *appinfo = root->append_rel_array[childRTindex];

	/* Build the RelOptInfo. */
	childrel = build_simple_rel(root, childRTindex, parent);

	/*
	 * Propagate lateral_relids and lateral_referencers from appendrel
	 * parent rels to their child rels.  We intentionally give each child rel
	 * the same minimum parameterization, even though it's quite possible that
	 * some don't reference all the lateral rels.  This is because any append
	 * path for the parent will have to have the same parameterization for
	 * every child anyway, and there's no value in forcing extra
	 * reparameterize_path() calls.  Similarly, a lateral reference to the
	 * parent prevents use of otherwise-movable join rels for each child.
	 */
	childrel->direct_lateral_relids = parent->direct_lateral_relids;
	childrel->lateral_relids = parent->lateral_relids;
	childrel->lateral_referencers = parent->lateral_referencers;

	/*
	 * We have to copy the parent's quals to the child, with appropriate
	 * substitution of variables.  However, only the baserestrictinfo
	 * quals are needed before we can check for constraint exclusion; so
	 * do that first and then check to see if we can disregard this child.
	 */
	if (!apply_child_basequals(root, parent, childrel, childRTE, appinfo) ||
		relation_excluded_by_constraints(root, childrel, childRTE))
	{
		/*
		 * Some restriction clause reduced to constant FALSE or NULL after
		 * substitution, so this child need not be scanned.
		 */
		set_dummy_rel_pathlist(childrel);
	}

	return childrel;
}

/*
 * add_rowmark_junk_columns
 * 		Add necessary junk columns for rowmarked inheritance parent rel.
 *
 * These values are needed for locking of rels selected FOR UPDATE/SHARE, and
 * to do EvalPlanQual rechecking.  See comments for PlanRowMark in
 * plannodes.h.
 */
static List *
add_rowmark_junk_columns(PlannerInfo *root, PlanRowMark *rc)
{
	List   *tlist = root->processed_tlist;
	Var		   *var;
	char		resname[32];
	TargetEntry *tle;

	if (rc->allMarkTypes & ~(1 << ROW_MARK_COPY))
	{
		/* Need to fetch TID */
		var = makeVar(rc->rti,
					  SelfItemPointerAttributeNumber,
					  TIDOID,
					  -1,
					  InvalidOid,
					  0);
		snprintf(resname, sizeof(resname), "ctid%u", rc->rowmarkId);
		tle = makeTargetEntry((Expr *) var,
							  list_length(tlist) + 1,
							  pstrdup(resname),
							  true);
		tlist = lappend(tlist, tle);
	}
	if (rc->allMarkTypes & (1 << ROW_MARK_COPY))
	{
		/* Need the whole row as a junk var */
		var = makeWholeRowVar(root->simple_rte_array[rc->rti],
							  rc->rti,
							  0,
							  false);
		snprintf(resname, sizeof(resname), "wholerow%u", rc->rowmarkId);
		tle = makeTargetEntry((Expr *) var,
							  list_length(tlist) + 1,
							  pstrdup(resname),
							  true);
		tlist = lappend(tlist, tle);
	}

	/* For inheritance cases, always fetch the tableoid too. */
	var = makeVar(rc->rti,
				  TableOidAttributeNumber,
				  OIDOID,
				  -1,
				  InvalidOid,
				  0);
	snprintf(resname, sizeof(resname), "tableoid%u", rc->rowmarkId);
	tle = makeTargetEntry((Expr *) var,
						  list_length(tlist) + 1,
						  pstrdup(resname),
						  true);
	tlist = lappend(tlist, tle);

	return tlist;
}

/*
 * translate_col_privs
 *	  Translate a bitmapset representing per-column privileges from the
 *	  parent rel's attribute numbering to the child's.
 *
 * The only surprise here is that we don't translate a parent whole-row
 * reference into a child whole-row reference.  That would mean requiring
 * permissions on all child columns, which is overly strict, since the
 * query is really only going to reference the inherited columns.  Instead
 * we set the per-column bits for all inherited columns.
 */
static Bitmapset *
translate_col_privs(const Bitmapset *parent_privs,
					List *translated_vars)
{
	Bitmapset  *child_privs = NULL;
	bool		whole_row;
	int			attno;
	ListCell   *lc;

	/* System attributes have the same numbers in all tables */
	for (attno = FirstLowInvalidHeapAttributeNumber + 1; attno < 0; attno++)
	{
		if (bms_is_member(attno - FirstLowInvalidHeapAttributeNumber,
						  parent_privs))
			child_privs = bms_add_member(child_privs,
										 attno - FirstLowInvalidHeapAttributeNumber);
	}

	/* Check if parent has whole-row reference */
	whole_row = bms_is_member(InvalidAttrNumber - FirstLowInvalidHeapAttributeNumber,
							  parent_privs);

	/* And now translate the regular user attributes, using the vars list */
	attno = InvalidAttrNumber;
	foreach(lc, translated_vars)
	{
		Var		   *var = lfirst_node(Var, lc);

		attno++;
		if (var == NULL)		/* ignore dropped columns */
			continue;
		if (whole_row ||
			bms_is_member(attno - FirstLowInvalidHeapAttributeNumber,
						  parent_privs))
			child_privs = bms_add_member(child_privs,
										 var->varattno - FirstLowInvalidHeapAttributeNumber);
	}

	return child_privs;
}

/*
 * add_inherited_target_child_roots
 *		For each child of the query's result relation, this translates the
 *		original query to match the child and creates a PlannerInfo containing
 *		the translated query
 *
 * Child PlannerInfo reuses most of the parent PlannerInfo's fields unchanged,
 * except unexpanded_tlist, processed_tlist, and all_baserels, all of which
 * are based on the child relation.
 */
void
add_inherited_target_child_roots(PlannerInfo *root)
{
	Index		resultRelation = root->parse->resultRelation;
	ListCell   *lc;

	Assert(root->inh_target_child_roots != NULL);

	foreach(lc, root->append_rel_list)
	{
		AppendRelInfo *appinfo = lfirst(lc);
		RangeTblEntry *childRTE;
		PlannerInfo   *subroot;

		if (appinfo->parent_relid != resultRelation)
			continue;

		/*
		 * Create a PlannerInfo for processing this child target relation
		 * with.
		 */
		subroot = create_inherited_target_child_root(root, appinfo);
		root->inh_target_child_roots[appinfo->child_relid] = subroot;

		/*
		 * If the child is a partitioned table, recurse to do this for its
		 * partitions.
		 */
		childRTE = root->simple_rte_array[appinfo->child_relid];
		if (childRTE->inh)
			add_inherited_target_child_roots(subroot);
	}
}

/*
 * create_inherited_target_child_root
 *		Workhorse of add_inherited_target_child_roots
 */
static PlannerInfo *
create_inherited_target_child_root(PlannerInfo *root, AppendRelInfo *appinfo)
{
	PlannerInfo *subroot;
	List	   *tlist;

	Assert(root->parse->commandType == CMD_UPDATE ||
		   root->parse->commandType == CMD_DELETE);

	/*
	 * Translate the original query to replace Vars of the parent table
	 * by the corresponding Vars of the child table and to make child the main
	 * target relation of the query.
	 */
	subroot = makeNode(PlannerInfo);
	memcpy(subroot, root, sizeof(PlannerInfo));

	/*
	 * Restore the original, unexpanded targetlist, that is, the one before
	 * preprocess_targetlist was run on the original query.  We'll run
	 * preprocess_targetlist after translating the query and the targetlist,
	 * so that it is expanded according to child's tuple descriptor.
	 */
	root->parse->targetList = root->unexpanded_tlist;
	subroot->parse = (Query *) adjust_appendrel_attrs(root,
													  (Node *) root->parse,
													  1, &appinfo);

	/*
	 * Save the just translated targetlist as unexpanded_tlist in the child's
	 * subroot, so that this child's own children can use it.  Must use copy
	 * because subroot->parse->targetList will be modified soon.
	 */
	subroot->unexpanded_tlist = list_copy(subroot->parse->targetList);

	/*
	 * Apply planner's expansion of targetlist, such as adding various junk
	 * column, filling placeholder entries for dropped columns, etc., all of
	 * which occurs with the child's TupleDesc.
	 */
	tlist = preprocess_targetlist(subroot, true);
	subroot->processed_tlist = tlist;

	/* Add any newly added Vars to the child RelOptInfo. */
	build_base_rel_tlists(subroot, tlist);

	/*
	 * Adjust all_baserels to replace the original target relation with the
	 * child target relation.  Copy it before modifying though.
	 */
	subroot->all_baserels = adjust_child_relids(subroot->all_baserels,
												1, &appinfo);

	return subroot;
}

/*
 * apply_child_basequals
 *		Populate childrel's quals based on rel's quals, translating them using
 *		appinfo.
 *
 * If any of the resulting clauses evaluate to false or NULL, we return false
 * and don't apply any quals.  Caller can mark the relation as a dummy rel in
 * this case, since it needn't be scanned.
 *
 * If any resulting clauses evaluate to true, they're unnecessary and we don't
 * apply then.
 */
static bool
apply_child_basequals(PlannerInfo *root, RelOptInfo *rel,
					  RelOptInfo *childrel, RangeTblEntry *childRTE,
					  AppendRelInfo *appinfo)
{
	List	   *childquals;
	Index		cq_min_security;
	ListCell   *lc;

	/*
	 * The child rel's targetlist might contain non-Var expressions, which
	 * means that substitution into the quals could produce opportunities for
	 * const-simplification, and perhaps even pseudoconstant quals. Therefore,
	 * transform each RestrictInfo separately to see if it reduces to a
	 * constant or pseudoconstant.  (We must process them separately to keep
	 * track of the security level of each qual.)
	 */
	childquals = NIL;
	cq_min_security = UINT_MAX;
	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
		Node	   *childqual;
		ListCell   *lc2;

		Assert(IsA(rinfo, RestrictInfo));
		childqual = adjust_appendrel_attrs(root,
										   (Node *) rinfo->clause,
										   1, &appinfo);
		childqual = eval_const_expressions(root, childqual);
		/* check for flat-out constant */
		if (childqual && IsA(childqual, Const))
		{
			if (((Const *) childqual)->constisnull ||
				!DatumGetBool(((Const *) childqual)->constvalue))
			{
				/* Restriction reduces to constant FALSE or NULL */
				return false;
			}
			/* Restriction reduces to constant TRUE, so drop it */
			continue;
		}
		/* might have gotten an AND clause, if so flatten it */
		foreach(lc2, make_ands_implicit((Expr *) childqual))
		{
			Node	   *onecq = (Node *) lfirst(lc2);
			bool		pseudoconstant;

			/* check for pseudoconstant (no Vars or volatile functions) */
			pseudoconstant =
				!contain_vars_of_level(onecq, 0) &&
				!contain_volatile_functions(onecq);
			if (pseudoconstant)
			{
				/* tell createplan.c to check for gating quals */
				root->hasPseudoConstantQuals = true;
			}
			/* reconstitute RestrictInfo with appropriate properties */
			childquals = lappend(childquals,
								 make_restrictinfo((Expr *) onecq,
												   rinfo->is_pushed_down,
												   rinfo->outerjoin_delayed,
												   pseudoconstant,
												   rinfo->security_level,
												   NULL, NULL, NULL));
			/* track minimum security level among child quals */
			cq_min_security = Min(cq_min_security, rinfo->security_level);
		}
	}

	/*
	 * In addition to the quals inherited from the parent, we might have
	 * securityQuals associated with this particular child node. (Currently
	 * this can only happen in appendrels originating from UNION ALL;
	 * inheritance child tables don't have their own securityQuals, see
	 * expand_inherited_rtentry().)	Pull any such securityQuals up into the
	 * baserestrictinfo for the child.  This is similar to
	 * process_security_barrier_quals() for the parent rel, except that we
	 * can't make any general deductions from such quals, since they don't
	 * hold for the whole appendrel.
	 */
	if (childRTE->securityQuals)
	{
		Index		security_level = 0;

		foreach(lc, childRTE->securityQuals)
		{
			List	   *qualset = (List *) lfirst(lc);
			ListCell   *lc2;

			foreach(lc2, qualset)
			{
				Expr	   *qual = (Expr *) lfirst(lc2);

				/* not likely that we'd see constants here, so no check */
				childquals = lappend(childquals,
									 make_restrictinfo(qual,
													   true, false, false,
													   security_level,
													   NULL, NULL, NULL));
				cq_min_security = Min(cq_min_security, security_level);
			}
			security_level++;
		}
		Assert(security_level <= root->qual_security_level);
	}

	/*
	 * OK, we've got all the baserestrictinfo quals for this child.
	 */
	childrel->baserestrictinfo = childquals;
	childrel->baserestrict_min_security = cq_min_security;

	return true;
}
