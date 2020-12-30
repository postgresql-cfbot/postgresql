/*-------------------------------------------------------------------------
 *
 * inherit.c
 *	  Routines to process child relations in inheritance trees
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
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
#include "nodes/nodeFuncs.h"
#include "optimizer/appendinfo.h"
#include "optimizer/inherit.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "partitioning/partdesc.h"
#include "partitioning/partprune.h"
#include "rewrite/rewriteHandler.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"


static void expand_partitioned_rtentry(PlannerInfo *root, RelOptInfo *relinfo,
									   RangeTblEntry *parentrte,
									   Index parentRTindex, Relation parentrel,
									   PlanRowMark *top_parentrc, LOCKMODE lockmode);
static void expand_single_inheritance_child(PlannerInfo *root,
											RangeTblEntry *parentrte,
											Index parentRTindex, Relation parentrel,
											PlanRowMark *top_parentrc, Relation childrel,
											RangeTblEntry **childrte_p,
											Index *childRTindex_p);
static Bitmapset *translate_col_privs(const Bitmapset *parent_privs,
									  List *translated_vars);
static void expand_appendrel_subquery(PlannerInfo *root, RelOptInfo *rel,
									  RangeTblEntry *rte, Index rti);
static void add_inherit_result_relation_info(PlannerInfo *root, Index rti,
							Relation relation,
							InheritResultRelInfo *parentInfo);
static List *adjust_inherited_tlist(List *tlist, AppendRelInfo *context);
static void add_child_junk_attrs(PlannerInfo *root,
					Index childRTindex, Relation childrelation,
					RelOptInfo *rel, Relation relation);
static void add_inherit_junk_var(PlannerInfo *root, char *attrname, Node *child_expr,
					   AppendRelInfo *appinfo,
					   RelOptInfo *parentrelinfo, Relation parentrelation);


/*
 * expand_inherited_rtentry
 *		Expand a rangetable entry that has the "inh" bit set.
 *
 * "inh" is only allowed in two cases: RELATION and SUBQUERY RTEs.
 *
 * "inh" on a plain RELATION RTE means that it is a partitioned table or the
 * parent of a traditional-inheritance set.  In this case we must add entries
 * for all the interesting child tables to the query's rangetable, and build
 * additional planner data structures for them, including RelOptInfos,
 * AppendRelInfos, and possibly PlanRowMarks.
 *
 * Note that the original RTE is considered to represent the whole inheritance
 * set.  In the case of traditional inheritance, the first of the generated
 * RTEs is an RTE for the same table, but with inh = false, to represent the
 * parent table in its role as a simple member of the inheritance set.  For
 * partitioning, we don't need a second RTE because the partitioned table
 * itself has no data and need not be scanned.
 *
 * "inh" on a SUBQUERY RTE means that it's the parent of a UNION ALL group,
 * which is treated as an appendrel similarly to inheritance cases; however,
 * we already made RTEs and AppendRelInfos for the subqueries.  We only need
 * to build RelOptInfos for them, which is done by expand_appendrel_subquery.
 */
void
expand_inherited_rtentry(PlannerInfo *root, RelOptInfo *rel,
						 RangeTblEntry *rte, Index rti)
{
	Oid			parentOID;
	Relation	oldrelation;
	LOCKMODE	lockmode;
	PlanRowMark *oldrc;
	bool		old_isParent = false;
	int			old_allMarkTypes = 0;
	ListCell   *l;
	List	   *newvars = NIL;

	Assert(rte->inh);			/* else caller error */

	if (rte->rtekind == RTE_SUBQUERY)
	{
		expand_appendrel_subquery(root, rel, rte, rti);
		return;
	}

	Assert(rte->rtekind == RTE_RELATION);

	parentOID = rte->relid;

	/*
	 * We used to check has_subclass() here, but there's no longer any need
	 * to, because subquery_planner already did.
	 */

	/*
	 * The rewriter should already have obtained an appropriate lock on each
	 * relation named in the query, so we can open the parent relation without
	 * locking it.  However, for each child relation we add to the query, we
	 * must obtain an appropriate lock, because this will be the first use of
	 * those relations in the parse/rewrite/plan pipeline.  Child rels should
	 * use the same lockmode as their parent.
	 */
	oldrelation = table_open(parentOID, NoLock);
	lockmode = rte->rellockmode;

	/*
	 * If parent relation is selected FOR UPDATE/SHARE, we need to mark its
	 * PlanRowMark as isParent = true, and generate a new PlanRowMark for each
	 * child.
	 */
	oldrc = get_plan_rowmark(root->rowMarks, rti);
	if (oldrc)
	{
		old_isParent = oldrc->isParent;
		oldrc->isParent = true;
		/* Save initial value of allMarkTypes before children add to it */
		old_allMarkTypes = oldrc->allMarkTypes;
	}

	/*
	 * Make an InheritResultRelInfo for the root parent if it's an
	 * UPDATE/DELETE result relation.
	 */
	if (rti == root->parse->resultRelation&&
		root->parse->commandType != CMD_INSERT)
	{
		/* Make an array indexable by RT indexes for easy lookup. */
		root->inherit_result_rel_array = (InheritResultRelInfo **)
			palloc0(root->simple_rel_array_size *
					sizeof(InheritResultRelInfo *));

		add_inherit_result_relation_info(root, root->parse->resultRelation,
										 oldrelation, NULL);
	}

	/* Scan the inheritance set and expand it */
	if (oldrelation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		/*
		 * Partitioned table, so set up for partitioning.
		 */
		Assert(rte->relkind == RELKIND_PARTITIONED_TABLE);

		/*
		 * Recursively expand and lock the partitions.  While at it, also
		 * extract the partition key columns of all the partitioned tables.
		 */
		expand_partitioned_rtentry(root, rel, rte, rti,
								   oldrelation, oldrc, lockmode);
	}
	else
	{
		/*
		 * Ordinary table, so process traditional-inheritance children.  (Note
		 * that partitioned tables are not allowed to have inheritance
		 * children, so it's not possible for both cases to apply.)
		 */
		List	   *inhOIDs;

		/* Scan for all members of inheritance set, acquire needed locks */
		inhOIDs = find_all_inheritors(parentOID, lockmode, NULL);

		/*
		 * We used to special-case the situation where the table no longer has
		 * any children, by clearing rte->inh and exiting.  That no longer
		 * works, because this function doesn't get run until after decisions
		 * have been made that depend on rte->inh.  We have to treat such
		 * situations as normal inheritance.  The table itself should always
		 * have been found, though.
		 */
		Assert(inhOIDs != NIL);
		Assert(linitial_oid(inhOIDs) == parentOID);

		/* Expand simple_rel_array and friends to hold child objects. */
		expand_planner_arrays(root, list_length(inhOIDs));

		/*
		 * Expand inheritance children in the order the OIDs were returned by
		 * find_all_inheritors.
		 */
		foreach(l, inhOIDs)
		{
			Oid			childOID = lfirst_oid(l);
			Relation	newrelation;
			RangeTblEntry *childrte;
			Index		childRTindex;

			/* Open rel if needed; we already have required locks */
			if (childOID != parentOID)
				newrelation = table_open(childOID, NoLock);
			else
				newrelation = oldrelation;

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

			/* Create RTE and AppendRelInfo, plus PlanRowMark if needed. */
			expand_single_inheritance_child(root, rte, rti, oldrelation,
											oldrc, newrelation,
											&childrte, &childRTindex);

			/* Create the otherrel RelOptInfo too. */
			(void) build_simple_rel(root, childRTindex, rel);

			/* Close child relations, but keep locks */
			if (childOID != parentOID)
				table_close(newrelation, NoLock);
		}
	}

	/*
	 * Some children might require different mark types, which would've been
	 * reported into oldrc.  If so, add relevant entries to the top-level
	 * targetlist and update parent rel's reltarget.  This should match what
	 * preprocess_targetlist() would have added if the mark types had been
	 * requested originally.
	 */
	if (oldrc)
	{
		int			new_allMarkTypes = oldrc->allMarkTypes;
		Var		   *var;
		TargetEntry *tle;
		char		resname[32];

		/* The old PlanRowMark should already have necessitated adding TID */
		Assert(old_allMarkTypes & ~(1 << ROW_MARK_COPY));

		/* Add whole-row junk Var if needed, unless we had it already */
		if ((new_allMarkTypes & (1 << ROW_MARK_COPY)) &&
			!(old_allMarkTypes & (1 << ROW_MARK_COPY)))
		{
			var = makeWholeRowVar(planner_rt_fetch(oldrc->rti, root),
								  oldrc->rti,
								  0,
								  false);
			snprintf(resname, sizeof(resname), "wholerow%u", oldrc->rowmarkId);
			tle = makeTargetEntry((Expr *) var,
								  list_length(root->processed_tlist) + 1,
								  pstrdup(resname),
								  true);
			root->processed_tlist = lappend(root->processed_tlist, tle);
			newvars = lappend(newvars, var);
		}

		/* Add tableoid junk Var, unless we had it already */
		if (!old_isParent)
		{
			var = makeVar(oldrc->rti,
						  TableOidAttributeNumber,
						  OIDOID,
						  -1,
						  InvalidOid,
						  0);
			snprintf(resname, sizeof(resname), "tableoid%u", oldrc->rowmarkId);
			tle = makeTargetEntry((Expr *) var,
								  list_length(root->processed_tlist) + 1,
								  pstrdup(resname),
								  true);
			root->processed_tlist = lappend(root->processed_tlist, tle);
			newvars = lappend(newvars, var);
		}
	}

	/*
	 * Also pull any appendrel parent junk vars added due to child result
	 * relations.
	 */
	if (rti == root->parse->resultRelation &&
		list_length(root->inherit_junk_tlist) > 0)
		newvars = list_concat(newvars,
							  pull_var_clause((Node *)
											  root->inherit_junk_tlist, 0));

	/*
	 * Add the newly added Vars to parent's reltarget.  We needn't worry
	 * about the children's reltargets, they'll be made later.
	 */
	if (newvars != NIL)
		add_vars_to_targetlist(root, newvars, bms_make_singleton(0), false);

	table_close(oldrelation, NoLock);
}

/*
 * expand_partitioned_rtentry
 *		Recursively expand an RTE for a partitioned table.
 */
static void
expand_partitioned_rtentry(PlannerInfo *root, RelOptInfo *relinfo,
						   RangeTblEntry *parentrte,
						   Index parentRTindex, Relation parentrel,
						   PlanRowMark *top_parentrc, LOCKMODE lockmode)
{
	PartitionDesc partdesc;
	Bitmapset  *live_parts;
	int			num_live_parts;
	int			i;

	check_stack_depth();

	Assert(parentrte->inh);

	partdesc = PartitionDirectoryLookup(root->glob->partition_directory,
										parentrel);

	/* A partitioned table should always have a partition descriptor. */
	Assert(partdesc);

	/*
	 * Note down whether any partition key cols are being updated. Though it's
	 * the root partitioned table's updatedCols we are interested in, we
	 * instead use parentrte to get the updatedCols. This is convenient
	 * because parentrte already has the root partrel's updatedCols translated
	 * to match the attribute ordering of parentrel.
	 */
	if (!root->partColsUpdated)
		root->partColsUpdated =
			has_partition_attrs(parentrel, parentrte->updatedCols, NULL);

	/*
	 * There shouldn't be any generated columns in the partition key.
	 */
	Assert(!has_partition_attrs(parentrel, parentrte->extraUpdatedCols, NULL));

	/* Nothing further to do here if there are no partitions. */
	if (partdesc->nparts == 0)
		return;

	/*
	 * Perform partition pruning using restriction clauses assigned to parent
	 * relation.  live_parts will contain PartitionDesc indexes of partitions
	 * that survive pruning.  Below, we will initialize child objects for the
	 * surviving partitions.
	 */
	live_parts = prune_append_rel_partitions(relinfo);

	/* Expand simple_rel_array and friends to hold child objects. */
	num_live_parts = bms_num_members(live_parts);
	if (num_live_parts > 0)
		expand_planner_arrays(root, num_live_parts);

	/*
	 * We also store partition RelOptInfo pointers in the parent relation.
	 * Since we're palloc0'ing, slots corresponding to pruned partitions will
	 * contain NULL.
	 */
	Assert(relinfo->part_rels == NULL);
	relinfo->part_rels = (RelOptInfo **)
		palloc0(relinfo->nparts * sizeof(RelOptInfo *));

	/*
	 * Create a child RTE for each live partition.  Note that unlike
	 * traditional inheritance, we don't need a child RTE for the partitioned
	 * table itself, because it's not going to be scanned.
	 */
	i = -1;
	while ((i = bms_next_member(live_parts, i)) >= 0)
	{
		Oid			childOID = partdesc->oids[i];
		Relation	childrel;
		RangeTblEntry *childrte;
		Index		childRTindex;
		RelOptInfo *childrelinfo;

		/* Open rel, acquiring required locks */
		childrel = table_open(childOID, lockmode);

		/*
		 * Temporary partitions belonging to other sessions should have been
		 * disallowed at definition, but for paranoia's sake, let's double
		 * check.
		 */
		if (RELATION_IS_OTHER_TEMP(childrel))
			elog(ERROR, "temporary relation from another session found as partition");

		/* Create RTE and AppendRelInfo, plus PlanRowMark if needed. */
		expand_single_inheritance_child(root, parentrte, parentRTindex,
										parentrel, top_parentrc, childrel,
										&childrte, &childRTindex);

		/* Create the otherrel RelOptInfo too. */
		childrelinfo = build_simple_rel(root, childRTindex, relinfo);
		relinfo->part_rels[i] = childrelinfo;
		relinfo->all_partrels = bms_add_members(relinfo->all_partrels,
												childrelinfo->relids);

		/* If this child is itself partitioned, recurse */
		if (childrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		{
			expand_partitioned_rtentry(root, childrelinfo,
									   childrte, childRTindex,
									   childrel, top_parentrc, lockmode);

			/*
			 * Add junk attributes needed by this child relation or really by
			 * its children.  We must do this after having added all the leaf
			 * children of this relation, because the add_child_junk_attrs()
			 * call below simply propagates their junk attributes that are in
			 * the form of this child relation's vars up to its own parent.
			 */
			if (is_result_relation(childRTindex, root))
				add_child_junk_attrs(root, childRTindex, childrel,
									 relinfo, parentrel);
		}

		/* Close child relation, but keep locks */
		table_close(childrel, NoLock);
	}
}

/*
 * expand_single_inheritance_child
 *		Build a RangeTblEntry and an AppendRelInfo, plus maybe a PlanRowMark.
 *
 * We now expand the partition hierarchy level by level, creating a
 * corresponding hierarchy of AppendRelInfos and RelOptInfos, where each
 * partitioned descendant acts as a parent of its immediate partitions.
 * (This is a difference from what older versions of PostgreSQL did and what
 * is still done in the case of table inheritance for unpartitioned tables,
 * where the hierarchy is flattened during RTE expansion.)
 *
 * PlanRowMarks still carry the top-parent's RTI, and the top-parent's
 * allMarkTypes field still accumulates values from all descendents.
 *
 * "parentrte" and "parentRTindex" are immediate parent's RTE and
 * RTI. "top_parentrc" is top parent's PlanRowMark.
 *
 * The child RangeTblEntry and its RTI are returned in "childrte_p" and
 * "childRTindex_p" resp.
 */
static void
expand_single_inheritance_child(PlannerInfo *root, RangeTblEntry *parentrte,
								Index parentRTindex, Relation parentrel,
								PlanRowMark *top_parentrc, Relation childrel,
								RangeTblEntry **childrte_p,
								Index *childRTindex_p)
{
	Query	   *parse = root->parse;
	Oid			parentOID = RelationGetRelid(parentrel);
	Oid			childOID = RelationGetRelid(childrel);
	RangeTblEntry *childrte;
	Index		childRTindex;
	AppendRelInfo *appinfo;
	TupleDesc	child_tupdesc;
	List	   *parent_colnames;
	List	   *child_colnames;

	/*
	 * Build an RTE for the child, and attach to query's rangetable list. We
	 * copy most scalar fields of the parent's RTE, but replace relation OID,
	 * relkind, and inh for the child.  Also, set requiredPerms to zero since
	 * all required permissions checks are done on the original RTE. Likewise,
	 * set the child's securityQuals to empty, because we only want to apply
	 * the parent's RLS conditions regardless of what RLS properties
	 * individual children may have.  (This is an intentional choice to make
	 * inherited RLS work like regular permissions checks.) The parent
	 * securityQuals will be propagated to children along with other base
	 * restriction clauses, so we don't need to do it here.  Other
	 * infrastructure of the parent RTE has to be translated to match the
	 * child table's column ordering, which we do below, so a "flat" copy is
	 * sufficient to start with.
	 */
	childrte = makeNode(RangeTblEntry);
	memcpy(childrte, parentrte, sizeof(RangeTblEntry));
	Assert(parentrte->rtekind == RTE_RELATION); /* else this is dubious */
	childrte->relid = childOID;
	childrte->relkind = childrel->rd_rel->relkind;
	/* A partitioned child will need to be expanded further. */
	if (childrte->relkind == RELKIND_PARTITIONED_TABLE)
	{
		Assert(childOID != parentOID);
		childrte->inh = true;
	}
	else
		childrte->inh = false;
	childrte->requiredPerms = 0;
	childrte->securityQuals = NIL;

	/* Link not-yet-fully-filled child RTE into data structures */
	parse->rtable = lappend(parse->rtable, childrte);
	childRTindex = list_length(parse->rtable);
	*childrte_p = childrte;
	*childRTindex_p = childRTindex;

	/*
	 * Build an AppendRelInfo struct for each parent/child pair.
	 */
	appinfo = make_append_rel_info(parentrel, childrel,
								   parentRTindex, childRTindex);
	root->append_rel_list = lappend(root->append_rel_list, appinfo);

	/* tablesample is probably null, but copy it */
	childrte->tablesample = copyObject(parentrte->tablesample);

	/*
	 * Construct an alias clause for the child, which we can also use as eref.
	 * This is important so that EXPLAIN will print the right column aliases
	 * for child-table columns.  (Since ruleutils.c doesn't have any easy way
	 * to reassociate parent and child columns, we must get the child column
	 * aliases right to start with.  Note that setting childrte->alias forces
	 * ruleutils.c to use these column names, which it otherwise would not.)
	 */
	child_tupdesc = RelationGetDescr(childrel);
	parent_colnames = parentrte->eref->colnames;
	child_colnames = NIL;
	for (int cattno = 0; cattno < child_tupdesc->natts; cattno++)
	{
		Form_pg_attribute att = TupleDescAttr(child_tupdesc, cattno);
		const char *attname;

		if (att->attisdropped)
		{
			/* Always insert an empty string for a dropped column */
			attname = "";
		}
		else if (appinfo->parent_colnos[cattno] > 0 &&
				 appinfo->parent_colnos[cattno] <= list_length(parent_colnames))
		{
			/* Duplicate the query-assigned name for the parent column */
			attname = strVal(list_nth(parent_colnames,
									  appinfo->parent_colnos[cattno] - 1));
		}
		else
		{
			/* New column, just use its real name */
			attname = NameStr(att->attname);
		}
		child_colnames = lappend(child_colnames, makeString(pstrdup(attname)));
	}

	/*
	 * We just duplicate the parent's table alias name for each child.  If the
	 * plan gets printed, ruleutils.c has to sort out unique table aliases to
	 * use, which it can handle.
	 */
	childrte->alias = childrte->eref = makeAlias(parentrte->eref->aliasname,
												 child_colnames);

	/*
	 * Translate the column permissions bitmaps to the child's attnums (we
	 * have to build the translated_vars list before we can do this).  But if
	 * this is the parent table, we can just duplicate the parent's bitmaps.
	 *
	 * Note: we need to do this even though the executor won't run any
	 * permissions checks on the child RTE.  The insertedCols/updatedCols
	 * bitmaps may be examined for trigger-firing purposes.
	 */
	if (childOID != parentOID)
	{
		childrte->selectedCols = translate_col_privs(parentrte->selectedCols,
													 appinfo->translated_vars);
		childrte->insertedCols = translate_col_privs(parentrte->insertedCols,
													 appinfo->translated_vars);
		childrte->updatedCols = translate_col_privs(parentrte->updatedCols,
													appinfo->translated_vars);
		childrte->extraUpdatedCols = translate_col_privs(parentrte->extraUpdatedCols,
														 appinfo->translated_vars);
	}
	else
	{
		childrte->selectedCols = bms_copy(parentrte->selectedCols);
		childrte->insertedCols = bms_copy(parentrte->insertedCols);
		childrte->updatedCols = bms_copy(parentrte->updatedCols);
		childrte->extraUpdatedCols = bms_copy(parentrte->extraUpdatedCols);
	}

	/*
	 * Store the RTE and appinfo in the respective PlannerInfo arrays, which
	 * the caller must already have allocated space for.
	 */
	Assert(childRTindex < root->simple_rel_array_size);
	Assert(root->simple_rte_array[childRTindex] == NULL);
	root->simple_rte_array[childRTindex] = childrte;
	Assert(root->append_rel_array[childRTindex] == NULL);
	root->append_rel_array[childRTindex] = appinfo;

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
		 * the child tables will be locked using the appropriate mode).
		 */
		childrc->isParent = (childrte->relkind == RELKIND_PARTITIONED_TABLE);

		/* Include child's rowmark type in top parent's allMarkTypes */
		top_parentrc->allMarkTypes |= childrc->allMarkTypes;

		root->rowMarks = lappend(root->rowMarks, childrc);
	}

	/*
	 * If this appears to be a child of an UPDATE/DELETE result relation, we
	 * need to remember some additional information.
	 */
	if (is_result_relation(parentRTindex, root))
	{
		InheritResultRelInfo *parentInfo = root->inherit_result_rel_array[parentRTindex];
		RelOptInfo *parentrelinfo = root->simple_rel_array[parentRTindex];

		add_inherit_result_relation_info(root, childRTindex, childrel,
										 parentInfo);

		/*
		 * Add junk attributes needed by this leaf child result relation, if
		 * one.
		 */
		if (childrte->relkind != RELKIND_PARTITIONED_TABLE)
			add_child_junk_attrs(root, childRTindex, childrel, parentrelinfo,
								 parentrel);
	}
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
 * expand_appendrel_subquery
 *		Add "other rel" RelOptInfos for the children of an appendrel baserel
 *
 * "rel" is a subquery relation that has the rte->inh flag set, meaning it
 * is a UNION ALL subquery that's been flattened into an appendrel, with
 * child subqueries listed in root->append_rel_list.  We need to build
 * a RelOptInfo for each child relation so that we can plan scans on them.
 */
static void
expand_appendrel_subquery(PlannerInfo *root, RelOptInfo *rel,
						  RangeTblEntry *rte, Index rti)
{
	ListCell   *l;

	foreach(l, root->append_rel_list)
	{
		AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);
		Index		childRTindex = appinfo->child_relid;
		RangeTblEntry *childrte;
		RelOptInfo *childrel;

		/* append_rel_list contains all append rels; ignore others */
		if (appinfo->parent_relid != rti)
			continue;

		/* find the child RTE, which should already exist */
		Assert(childRTindex < root->simple_rel_array_size);
		childrte = root->simple_rte_array[childRTindex];
		Assert(childrte != NULL);

		/* Build the child RelOptInfo. */
		childrel = build_simple_rel(root, childRTindex, rel);

		/* Child may itself be an inherited rel, either table or subquery. */
		if (childrte->inh)
			expand_inherited_rtentry(root, childrel, childrte, childRTindex);
	}
}


/*
 * apply_child_basequals
 *		Populate childrel's base restriction quals from parent rel's quals,
 *		translating them using appinfo.
 *
 * If any of the resulting clauses evaluate to constant false or NULL, we
 * return false and don't apply any quals.  Caller should mark the relation as
 * a dummy rel in this case, since it doesn't need to be scanned.
 */
bool
apply_child_basequals(PlannerInfo *root, RelOptInfo *parentrel,
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
	foreach(lc, parentrel->baserestrictinfo)
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
	 * securityQuals associated with this particular child node.  (Currently
	 * this can only happen in appendrels originating from UNION ALL;
	 * inheritance child tables don't have their own securityQuals, see
	 * expand_single_inheritance_child().)  Pull any such securityQuals up
	 * into the baserestrictinfo for the child.  This is similar to
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

/*
 * add_inherit_result_relation
 *		Adds information to PlannerInfo about an inherited UPDATE/DELETE
 *		result relation
 */
static void
add_inherit_result_relation_info(PlannerInfo *root, Index rti,
								 Relation relation,
								 InheritResultRelInfo *parentInfo)
{
	InheritResultRelInfo *resultInfo = makeNode(InheritResultRelInfo);

	if (parentInfo == NULL)
	{
		/* Root result relation. */
		resultInfo->resultRelation = rti;
		resultInfo->withCheckOptions = root->parse->withCheckOptions;
		resultInfo->returningList = root->parse->returningList;
		if (root->parse->commandType == CMD_UPDATE)
		{
			resultInfo->processed_tlist = root->processed_tlist;
			resultInfo->update_tlist = root->update_tlist;
		}
	}
	else
	{
		/* Child result relation. */
		AppendRelInfo *appinfo = root->append_rel_array[rti];

		Assert(appinfo != NULL);

		resultInfo->resultRelation = rti;

		if (parentInfo->withCheckOptions)
			resultInfo->withCheckOptions = (List *)
				adjust_appendrel_attrs(root,
									   (Node *) parentInfo->withCheckOptions,
									   1, &appinfo);
		if (parentInfo->returningList)
			resultInfo->returningList = (List *)
				adjust_appendrel_attrs(root,
									   (Node *) parentInfo->returningList,
									   1, &appinfo);

		/* Build UPDATE targetlist for this child. */
		if (root->parse->commandType == CMD_UPDATE)
		{
			List    *tmp;

			/*
			 * First fix up any Vars in the parent's version of the top-level
			 * targetlist.
			 */
			resultInfo->processed_tlist = (List *)
				adjust_appendrel_attrs(root,
									   (Node *) parentInfo->processed_tlist,
									   1, &appinfo);

			/*
			 * Re-assign resnos to match child attnos.  The returned tlist may
			 * have the individual entries in a different order than they were
			 * in before because it will match the order of child's attributes,
			 * which we need to make update_tlist below.  But we better not
			 * overwrite resultInfo->processed_tlist, because that list must
			 * be in the root parent attribute order to match the order of
			 * top-level targetlist.
			 */
			tmp = adjust_inherited_tlist(resultInfo->processed_tlist, appinfo);
			resultInfo->update_tlist = make_update_tlist(copyObject(tmp),
														 rti, relation);
		}
	}

	root->inherit_result_rels = lappend(root->inherit_result_rels, resultInfo);
	Assert(root->inherit_result_rel_array);
	Assert(root->inherit_result_rel_array[rti] == NULL);
	root->inherit_result_rel_array[rti] = resultInfo;
}

/*
 * Adjust the targetlist entries of an inherited UPDATE operation
 *
 * The input tlist is that of an UPDATE targeting the given parent table.
 * Expressions of the individual target entries have already been fixed to
 * convert any parent table Vars in them into child table Vars, but the
 * target resnos still match the parent attnos, which we fix here to match
 * the corresponding child table attnos.
 *
 * In some cases this can force us to re-order the tlist to preserve resno
 * ordering, which make_update_targetlist() that will be called on this tlist
 * later expects to be the case.
 *
 * Note that the caller must be okay with the input tlist being scribbled on.
 */
static List *
adjust_inherited_tlist(List *tlist, AppendRelInfo *context)
{
	bool		changed_it = false;
	ListCell   *tl;
	List	   *new_tlist;
	bool		more;
	int			attrno;

	/* This should only happen for an inheritance case, not UNION ALL */
	Assert(OidIsValid(context->parent_reloid));

	/* Scan tlist and update resnos to match attnums of child rel */
	foreach(tl, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(tl);
		Var		   *childvar;

		if (tle->resjunk)
			continue;			/* ignore junk items */

		/* Look up the translation of this column: it must be a Var */
		if (tle->resno <= 0 ||
			tle->resno > list_length(context->translated_vars))
			elog(ERROR, "attribute %d of relation \"%s\" does not exist",
				 tle->resno, get_rel_name(context->parent_reloid));
		childvar = (Var *) list_nth(context->translated_vars, tle->resno - 1);
		if (childvar == NULL || !IsA(childvar, Var))
			elog(ERROR, "attribute %d of relation \"%s\" does not exist",
				 tle->resno, get_rel_name(context->parent_reloid));

		if (tle->resno != childvar->varattno)
		{
			tle->resno = childvar->varattno;
			changed_it = true;
		}
	}

	/*
	 * If we changed anything, re-sort the tlist by resno, and make sure
	 * resjunk entries have resnos above the last real resno.  The sort
	 * algorithm is a bit stupid, but for such a seldom-taken path, small is
	 * probably better than fast.
	 */
	if (!changed_it)
		return tlist;

	new_tlist = NIL;
	more = true;
	for (attrno = 1; more; attrno++)
	{
		more = false;
		foreach(tl, tlist)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(tl);

			if (tle->resjunk)
				continue;		/* ignore junk items */

			if (tle->resno == attrno)
				new_tlist = lappend(new_tlist, tle);
			else if (tle->resno > attrno)
				more = true;
		}
	}

	foreach(tl, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(tl);

		if (!tle->resjunk)
			continue;			/* here, ignore non-junk items */

		tle->resno = attrno;
		new_tlist = lappend(new_tlist, tle);
		attrno++;
	}

	return new_tlist;
}

/*
 * add_child_junk_attrs
 *		Adds junk attributes needed by leaf child result relations to
 *		identify tuples to be updated/deleted, and for each tuple also
 *		the result relation to perform the operation on
 *
 * While preprocess_targetlist() would have added junk attributes needed to
 * identify rows to be updated/deleted based on whatever the root parent says
 * they are, not all leaf result relations may be able to use the same junk
 * attributes.  For example, in the case of leaf result relations that are
 * foreign tables, junk attributes to use are determined by their FDW's
 * AddForeignUpdateTargets().
 *
 * Even though leaf result relations are scanned at the bottom of the plan
 * tree, any junk attributes needed must be present in the top-level tlist,
 * so to add a junk attribute for a given leaf result relation really means
 * adding corresponding column of the top parent relation to the top-level
 * targetlist from where it will be propagated back down to the leaf result
 * relation.  In some cases, a leaf relation's junk attribute may be such that
 * no column of the root parent can be mapped to it, in which case we must add
 * "fake" parent columns to the targetlist and set things up to map those
 * columns' vars to desired junk attribute expressions in the reltargets of
 * leaf result relation that need them.  This logic of how leaf-level junk
 * attributes are mapped to top-level level vars and back is present in
 * add_inherit_junk_var().
 *
 * The leaf-level junk attribute that is added to identify the leaf result
 * relation for each tuple to be updated/deleted is really a Const node
 * containing an integer value that gives the index of the leaf result
 * relation in the subquery's list of result relations.  This adds an
 * entry named "__result_index" to the top-level tlist which wraps a fake
 * parent var that maps back to the Const node for each leaf result
 * relation.
 */
static void
add_child_junk_attrs(PlannerInfo *root,
					 Index childRTindex, Relation childrelation,
					 RelOptInfo *parentrelinfo, Relation parentrelation)
{
	AppendRelInfo  *appinfo = root->append_rel_array[childRTindex];
	ListCell	   *lc;
	List		   *child_junk_attrs = NIL;

	/*
	 * For a non-leaf child relation, we simply need to bubble up to its
	 * parent any entries containing its vars that would be added for junk
	 * attributes of its own children.
	 */
	if (childrelation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		foreach(lc, root->inherit_junk_tlist)
		{
			TargetEntry *tle = lfirst(lc);
			Var   *var = (Var *) tle->expr;

			if (var->varno == childRTindex)
				child_junk_attrs = lappend(child_junk_attrs, tle);
		}
	}
	else
	{
		/* Leaf child case. */
		RangeTblEntry  *childrte = root->simple_rte_array[childRTindex];
		Query			parsetree;
		Node		   *childexpr;
		TargetEntry	   *tle;

		/* The "__result_index" column. */
		childexpr = (Node *) makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
									   Int32GetDatum(root->lastResultRelIndex++),
									   false, true),
		tle = makeTargetEntry((Expr *) childexpr, 1, "__result_index", true);
		child_junk_attrs = lappend(child_junk_attrs, tle);

		/*
		 * Now call rewriteTargetListUD() to add junk attributes into the
		 * parsetree.  We pass a slightly altered version of the original
		 * parsetree to show the child result relation as the main target
		 * relation.  It is assumed here that rewriteTargetListUD and any
		 * code downstream to it do not inspect the parsetree beside to
		 * figure out the varno to assign to the Vars that will be added
		 * to the targetlist.
		 */
		memcpy(&parsetree, root->parse, sizeof(Query));
		parsetree.resultRelation = childRTindex;
		parsetree.targetList = NIL;
		rewriteTargetListUD(&parsetree, childrte, childrelation);
		child_junk_attrs = list_concat(child_junk_attrs,
									   parsetree.targetList);
	}

	/* Add parent vars for each of the child junk attributes. */
	foreach(lc, child_junk_attrs)
	{
		TargetEntry *tle = lfirst(lc);

		Assert(tle->resjunk);

		add_inherit_junk_var(root, tle->resname, (Node *) tle->expr,
							 appinfo, parentrelinfo, parentrelation);
	}
}

/*
 * add_inherit_junk_var
 *		Checks if the query's top-level tlist (root->processed_tlist) or
 *		root->inherit_junk_tlist contains an entry for a junk attribute
 *		with given name and if the parent var therein translates to
 *		given child junk expression
 *
 * If not, add the parent var to appropriate list -- top-level tlist if parent
 * is top-level parent, root->inherit_junk_tlist otherwise.
 *
 * If the parent var found or added is not for a real column or is a "fake"
 * var, which will be he case if no real column of the parent translates to
 * provided child expression, then add mapping information in provided
 * AppendRelInfo to translate such fake parent var to provided child
 * expression.
 */
static void
add_inherit_junk_var(PlannerInfo *root, char *attrname, Node *child_expr,
					 AppendRelInfo *appinfo,
					 RelOptInfo *parentrelinfo, Relation parentrelation)
{
	AttrNumber	max_parent_attno = RelationGetNumberOfAttributes(parentrelation);
	AttrNumber	max_child_attno = appinfo->num_child_cols;
	ListCell   *lc;
	Var		   *parent_var = NULL;
	Index		parent_varno = parentrelinfo->relid;
	AttrNumber	parent_attno;

	Assert(appinfo && parent_varno == appinfo->parent_relid);

	/*
	 * The way we decide if a given parent var found in the targetlist is the
	 * one that will give the desired child var back upon translation is to
	 * check whether the child var refers to an inherited user column or a
	 * system column that is same as the one that the parent var refers to.
	 * If the child var refers to a fake column, parent var must likewise
	 * refer to a fake column itself.
	 *
	 * There is a special case where the desired child expression is a Const
	 * node wrapped in an entry named "__result_index", in which case, simply
	 * finding an entry with that name containing a parent's var suffices.
	 *
	 * If no such parent var is found, we will add one.
	 */
	foreach(lc, list_concat_copy(root->inherit_junk_tlist,
								 root->processed_tlist))
	{
		TargetEntry *tle = lfirst(lc);
		Var	   *var = (Var *) tle->expr;
		Var	   *child_var = (Var *) child_expr;

		if (!tle->resjunk)
			continue;

		/* Ignore RETURNING expressions in the top-level tlist. */
		if (tle->resname == NULL)
			continue;

		if (strcmp(attrname, tle->resname) != 0)
			continue;

		if (!IsA(var,  Var))
			elog(ERROR, "junk column \"%s\" is not a Var", attrname);

		/* Ignore junk vars of other relations. */
		if (var->varno != parent_varno)
			continue;

		/* special case */
		if (strcmp(attrname, "__result_index") == 0)
		{
			/* The parent var had better not be a normal user column. */
			Assert(var->varattno > max_parent_attno);
			parent_var = var;
			break;
		}

		if (!IsA(child_expr, Var))
			elog(ERROR, "child junk column \"%s\" is not a Var", attrname);

		/*
		 * So we found parent var referring to the column that the child wants
		 * added, but check if that's really the case.
		 */
		if (var->vartype == child_var->vartype &&
			var->vartypmod == child_var->vartypmod &&

			(/* child var refers to same system column as parent var */
			 (child_var->varattno <= 0 &&
			  child_var->varattno == var->varattno) ||

			 /* child var refers to same user column as parent var */
			 (child_var->varattno > 0 &&
			  child_var->varattno <= max_child_attno &&
			  var->varattno == appinfo->parent_colnos[child_var->varattno]) ||

			 /* both child var and parent var refer to "fake" column */
			 (child_var->varattno > max_child_attno &&
			  var->varattno > max_parent_attno)))
		{
			parent_var = var;
			break;
		}

		/*
		 * Getting here means that did find a parent column with the given
		 * name but it's not equivalent to the child column we're trying
		 * to add to the targetlist.  Adding a second var with child's type
		 * would not be correct.
		 */
		elog(ERROR, "child junk column \"%s\" conflicts with parent junk column with same name",
			 attrname);
	}

	/*
	 * If no parent column matching the child column found in the targetlist,
	 * add.
	 */
	if (parent_var == NULL)
	{
		TargetEntry *tle;
		bool		fake_column = true;
		AttrNumber	resno;
		Oid			parent_vartype = exprType((Node *) child_expr);
		int32		parent_vartypmod = exprTypmod((Node *) child_expr);
		Oid			parent_varcollid = exprCollation((Node *) child_expr);

		/*
		 * If the child expression is either an inherited user column, or
		 * wholerow, or ctid, it can be mapped to a parent var.  If the child
		 * expression does not refer to a column, or a column that parent does
		 * not contain, then we will need to make a "fake" parent column to
		 * stand for the child expression.  We will set things up below using
		 * the child's AppendRelInfo such that when translated, the fake parent
		 * column becomes the child expression.  Note that these fake columns
		 * don't leave the planner, because the parent's reltarget is never
		 * actually computed during execution (see set_dummy_tlist_references()
		 * and how it applies to Append and similar plan nodes).
		 */
		if (IsA(child_expr, Var))
		{
			Var   *child_var = (Var *) child_expr;

			if (child_var->varattno > 0 &&
				child_var->varattno <= appinfo->num_child_cols &&
				appinfo->parent_colnos[child_var->varattno] > 0)
			{
				/* A user-defined parent column. */
				parent_attno = appinfo->parent_colnos[child_var->varattno];
				fake_column = false;
			}
			else if (child_var->varattno == 0)
			{
				/* wholerow */
				parent_attno = 0;
				parent_vartype = parentrelation->rd_rel->reltype;
				fake_column = false;
			}
			else if (child_var->varattno == SelfItemPointerAttributeNumber)
			{
				/* ctid */
				parent_attno = SelfItemPointerAttributeNumber;
				fake_column = false;
			}
		}

		/*
		 * A fake parent column is represented by a Var with fake varattno.
		 * We use attribute numbers starting from parent's max_attr + 1.
		 */
		if (fake_column)
		{
			int		array_size;

			parent_attno = parentrelinfo->max_attr + 1;

			/* Must expand attr_needed array for the new fake Var. */
			array_size = parentrelinfo->max_attr - parentrelinfo->min_attr + 1;
			parentrelinfo->attr_needed = (Relids *)
					repalloc(parentrelinfo->attr_needed,
							 (array_size + 1) * sizeof(Relids));
			parentrelinfo->attr_widths = (int32 *)
					repalloc(parentrelinfo->attr_widths,
							 (array_size + 1) * sizeof(int32));
			parentrelinfo->attr_needed[array_size] = NULL;
			parentrelinfo->attr_widths[array_size] = 0;
			parentrelinfo->max_attr += 1;
		}

		parent_var = makeVar(parent_varno, parent_attno, parent_vartype,
							 parent_vartypmod, parent_varcollid, 0);

		/*
		 * Only the top-level parent's vars will make it into the top-level
		 * tlist, so choose resno likewise.  Other TLEs containing vars of
		 * intermediate parents only serve as placeholders for remembering
		 * child junk attribute names and expressions so as to avoid re-adding
		 * duplicates as the code at the beginning of this function does, so
		 * their resnos don't need to be correct.
		 */
		if (parent_varno == root->parse->resultRelation)
			resno = list_length(root->processed_tlist) + 1;
		else
			resno = 1;
		tle = makeTargetEntry((Expr *) parent_var, resno, attrname, true);

		root->inherit_junk_tlist = lappend(root->inherit_junk_tlist, tle);
		if (parent_varno == root->parse->resultRelation)
			root->processed_tlist = lappend(root->processed_tlist, tle);
	}

	/*
	 * While appinfo->translated_vars contains child column vars mapped from
	 * real parent column vars, we maintain a list of child expressions that
	 * are mapped from fake parent vars in appinfo->translated_fake_vars.
	 */
	parent_attno = parent_var->varattno;
	if (parent_attno > max_parent_attno)
	{
		int		fake_var_offset = max_parent_attno - parent_attno - 1;

		/*
		 * For parent's fake columns with attribute number smaller than the
		 * current fake attno, we assume that they are not mapped to any
		 * expression of this child, which is indicated by having a NULL in
		 * the map.
		 */
		if (fake_var_offset > 0)
		{
			int		offset;

			Assert(list_length(appinfo->translated_fake_vars) > 0);
			for (offset = 0; offset < fake_var_offset; offset++)
			{
				/*
				 * Don't accidentally overwrite other expressions of this
				 * child.
				 */
				if (list_nth(appinfo->translated_fake_vars, offset) != NULL)
					continue;

				appinfo->translated_fake_vars =
					lappend(appinfo->translated_fake_vars, NULL);
			}

			if (list_nth(appinfo->translated_fake_vars, offset) != NULL)
				elog(ERROR, "fake attno %u of parent %u already mapped",
					 parent_var->varattno, parent_varno);
		}

		appinfo->translated_fake_vars = lappend(appinfo->translated_fake_vars,
												child_expr);
	}
}

/*
 * translate_fake_parent_var
 * 		For a "fake" parent var, return corresponding child expression in
 * 		appinfo->translated_fake_vars if one has been added, NULL const node
 * 		otherwise
 */
Node *
translate_fake_parent_var(Var *var, AppendRelInfo *appinfo)
{
	int		max_parent_attno = list_length(appinfo->translated_vars);
	int		offset = var->varattno - max_parent_attno - 1;
	Node   *result = NULL;

	if (offset < list_length(appinfo->translated_fake_vars))
		result = (Node *) list_nth(appinfo->translated_fake_vars, offset);

	/*
	 * It's possible for some fake parent vars to map to a valid expression
	 * in only some child relations but not in others.  In that case, we
	 * return a NULL const node for those other relations.
	 */
	if (result == NULL)
		return (Node *) makeNullConst(var->vartype, var->vartypmod,
									  var->varcollid);

	return result;
}
