/*-------------------------------------------------------------------------
 *
 * uniquekeys.c
 *	  Utilities for matching and building unique keys
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/uniquekeys.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/appendinfo.h"
#include "optimizer/optimizer.h"
#include "optimizer/tlist.h"
#include "rewrite/rewriteManip.h"


/*
 * This struct is used to help populate_joinrel_uniquekeys.
 *
 * added_to_joinrel is true if a uniquekey (from outerrel or innerrel)
 * has been added to joinrel.
 * useful is true if the exprs of the uniquekey still appears in joinrel.
 */
typedef struct UniqueKeyContextData
{
	UniqueKey	*uniquekey;
	bool	added_to_joinrel;
	bool	useful;
} *UniqueKeyContext;

static List *initialize_uniquecontext_for_joinrel(RelOptInfo *inputrel);
static bool innerrel_keeps_unique(PlannerInfo *root,
								  RelOptInfo *outerrel,
								  RelOptInfo *innerrel,
								  List *restrictlist,
								  bool reverse);

static List *get_exprs_from_uniqueindex(IndexOptInfo *unique_index,
										List *const_exprs,
										List *const_expr_opfamilies,
										Bitmapset *used_varattrs,
										bool *useful,
										bool *multi_nullvals);
static List *get_exprs_from_uniquekey(PlannerInfo *root,
									  RelOptInfo *joinrel,
									  RelOptInfo *rel1,
									  UniqueKey *ukey);
static void add_uniquekey_for_onerow(RelOptInfo *rel);
static bool add_combined_uniquekey(PlannerInfo *root,
								   RelOptInfo *joinrel,
								   RelOptInfo *outer_rel,
								   RelOptInfo *inner_rel,
								   UniqueKey *outer_ukey,
								   UniqueKey *inner_ukey,
								   JoinType jointype);

/* Used for unique indexes checking for partitioned table */
static bool index_constains_partkey(RelOptInfo *partrel,  IndexOptInfo *ind);
static IndexOptInfo *simple_copy_indexinfo_to_parent(PlannerInfo *root,
													 RelOptInfo *parentrel,
													 IndexOptInfo *from);
static bool simple_indexinfo_equal(IndexOptInfo *ind1, IndexOptInfo *ind2);
static void adjust_partition_unique_indexlist(PlannerInfo *root,
											  RelOptInfo *parentrel,
											  RelOptInfo *childrel,
											  List **global_unique_index);

/* Helper function for grouped relation and distinct relation. */
static void add_uniquekey_from_sortgroups(PlannerInfo *root,
										  RelOptInfo *rel,
										  List *sortgroups);

/*
 * populate_baserel_uniquekeys
 *		Build list of unique keys for the base relation.
 *
 * Inspects unique indexes defined on the relation and determines what
 * unique keys are valid.  Partial indexes are considered too, if the
 * predicate is valid.
 *
 * This also inspects baserestrictinfo, because we need to determine
 * which opclass families are interesting when inspecting indexes. If we
 * have a unique index and distinct clause with a mismatching opclasses,
 * we should not use that.
 *
 * XXX Why does this look at baserestrictinfo?
 *
 * XXX What about collations?
 */
void
populate_baserel_uniquekeys(PlannerInfo *root,
							RelOptInfo *baserel,
							List *indexlist)
{
	ListCell *lc;
	List	*matched_uniq_indexes = NIL;

	/* Attrs appears in rel->reltarget->exprs. */
	Bitmapset *used_attrs = NULL;

	List	*const_exprs = NIL;
	List	*expr_opfamilies = NIL;

	Assert(baserel->rtekind == RTE_RELATION);

	if (!indexlist)
		return;

	/*
	 * Determine which unique indexes to use to build the unique keys.
	 * We have to skip partial with predicates not matched by the query,
	 * and unique indexes that are not immediately enforced.
	 *
	 * XXX Do we actually skip indexes that are not immediate?
	 * XXX What about hypothetical indexes?
	 */
	foreach(lc, indexlist)
	{
		IndexOptInfo *ind = (IndexOptInfo *) lfirst(lc);

		if (!ind->unique || !ind->immediate ||
			(ind->indpred != NIL && !ind->predOK))
			continue;

		matched_uniq_indexes = lappend(matched_uniq_indexes, ind);
	}

	/* If there are not applicable unique indexes, we're done. */
	if (matched_uniq_indexes  == NIL)
		return;

	/*
	 * Determine which attrs are referenced in baserel->reltarget.  To use the
	 * unique key info, we need all the columns - a unique index on (a,b) may
	 * not be unique on (a).  If a column is missing in reltarget, the nodes
	 * above can't possibly use it, and we can just ignore any matching index.
	 */
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid, &used_attrs);

	/*
	 * Check which attrno is used at a mergeable const filter
	 *
	 * XXX This is not lookint att attrno at all, maybe obsolete comment?
	 *
	 * Seems the primary purpose of this is determining which opclass
	 * families to use when matching unique indexes in the next loop?
	 */
	foreach(lc, baserel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		if (rinfo->mergeopfamilies == NIL)
			continue;

		/*
		 * XXX What if bms_is_empty is true for both left_relids/right_relids?
		 * Or what if it's false in both cases?
		 */
		if (bms_is_empty(rinfo->left_relids))
		{
			const_exprs = lappend(const_exprs, get_rightop(rinfo->clause));
		}
		else if (bms_is_empty(rinfo->right_relids))
		{
			const_exprs = lappend(const_exprs, get_leftop(rinfo->clause));
		}
		else
			continue;

		expr_opfamilies = lappend(expr_opfamilies, rinfo->mergeopfamilies);
	}

	/*
	 * Now try to match unique indexes to attributes in reltarget, and to
	 * merge operator families. The index may be on the right attributes,
	 * but if it's not matching the opfamily it's useless.
	 *
	 * XXX Can we have multiple baserestrictinfo for the same attribute,
	 * with different opfamilies? Probably not.
	 */
	foreach(lc, matched_uniq_indexes)
	{
		bool	multi_nullvals,
				useful;

		IndexOptInfo *index_info = (IndexOptInfo *) lfirst_node(IndexOptInfo, lc);

		List   *exprs = get_exprs_from_uniqueindex(index_info,
												   const_exprs,
												   expr_opfamilies,
												   used_attrs,
												   &useful,
												   &multi_nullvals);

		if (!useful)
			continue;

		/*
		 * All the columns in Unique Index matched with a restrictinfo, so
		 * that we know there's just a one row in the result. If we find
		 * such index, we're done - we discard all other unique keys and
		 * keep just this special one. In principle, this is a stronger
		 * guarantee, because all subsets of one row are still unique.
		 *
		 * XXX Is it correct to just return? Doesn't that prevent some
		 * optimizations that might be possible with the other keys?
		 */
		if (exprs == NIL)
		{
			/* discards all previous uniquekeys */
			add_uniquekey_for_onerow(baserel);
			return;
		}

		baserel->uniquekeys = lappend(baserel->uniquekeys,
									  makeUniqueKey(exprs, multi_nullvals));
	}
}


/*
 * populate_partitionedrel_uniquekeys
 *		Determine unique keys for a partitioned relation.
 *
 * Inspects unique keys for all partitions and derives unique keys that
 * are valid for the whole partitioned table. There are two basic cases:
 *
 * 1) There's only one remaining partition (thanks to pruning all other
 * partitions). In this case all the unique keys from the partition are
 * trivially valid for the partitioned table.
 *
 * 2) All the partitions have the same unique index (on the same set of
 * columns), and the index includes the partition key. This ensures the
 * combination of values is unique for the whole partitioned table.
 */
void
populate_partitionedrel_uniquekeys(PlannerInfo *root,
								   RelOptInfo *rel,
								   List *childrels)
{
	ListCell	*lc;
	List	*global_uniq_indexlist = NIL;
	RelOptInfo *childrel;
	bool is_first = true;

	/* XXX What about append rels? At least for the one-child case? */
	Assert(IS_PARTITIONED_REL(rel));

	/* if there are no child relations, we're done. */
	if (childrels == NIL)
		return;

	/*
	 * If there is only one partition used in this query, the UniqueKey for
	 * a child relation is still valid for the parent level. We need to
	 * convert the format from child expr to parent expr.
	 */
	if (list_length(childrels) == 1)
	{
		RelOptInfo *childrel = linitial_node(RelOptInfo, childrels);
		ListCell	*lc;

		Assert(childrel->reloptkind == RELOPT_OTHER_MEMBER_REL);

		/* If the partition has a single row, so does the parent. */
		if (relation_is_onerow(childrel))
		{
			add_uniquekey_for_onerow(rel);
			return;
		}

		/*
		 * Inspect the unique keys one by one, try reusing them for the
		 * parent relation.
		 *
		 * FIXME This needs more work to handle expressions and not just
		 * simple Vars.
		 */
		foreach(lc, childrel->uniquekeys)
		{
			ListCell   *lc2;
			List	   *parent_exprs = NIL;
			bool		can_reuse = true;

			UniqueKey *ukey = lfirst_node(UniqueKey, lc);
			AppendRelInfo *appinfo = find_appinfo_by_child(root, childrel->relid);

			/*
			 * XXX Not sure what exactly we do here. Surely we deal with
			 * expressions at child/parent level elsewhere? Can't we just
			 * copy the code from there?
			 */
			foreach(lc2, ukey->exprs)
			{
				Var *var = (Var *) lfirst(lc2);

				/*
				 * XXX For now this only supports simple Var expressions,
				 * so if there's a more complex expression we'll not copy
				 * the unique key to the parent.
				 */
				if(!IsA(var, Var))
				{
					can_reuse = false;
					break;
				}

				/* Convert it to parent var */
				parent_exprs = lappend(parent_exprs,
									   find_parent_var(appinfo, var));
			}

			/* ignore unique keys with complex expressions */
			if (!can_reuse)
				continue;

			rel->uniquekeys = lappend(rel->uniquekeys,
									  makeUniqueKey(parent_exprs,
													ukey->multi_nullvals));
		}

		return;
	}

	/*
	 * A parent with multiple child relations. We only care about indexes that
	 * are in all child relations, so we loop through indexes on the first one
	 * and check that they exist in the other child relations too.
	 */

	childrel = linitial_node(RelOptInfo, childrels);
	foreach(lc, childrel->indexlist)
	{
		IndexOptInfo *ind = lfirst(lc);
		IndexOptInfo *modified_index;

		/*
		 * Ignore indexes that are not unique, immediately enforced. Partial
		 * indexes with mismatched predicate are useless too.
		 */
		if (!ind->unique || !ind->immediate ||
			(ind->indpred != NIL && !ind->predOK))
			continue;

		/*
		 * During simple_copy_indexinfo_to_parent, we need to convert var from
		 * child var to parent var, index on expression is too complex to handle.
		 * so ignore it for now.
		 *
		 * FIXME We should support indexes on expressions.
		 */
		if (ind->indexprs != NIL)
			continue;

		/*
		 * Adopt the index definition for the parent.
		 *
		 * XXX This seems rather weird. We're constructing "artificial" index
		 * for the partitioned table (kinda like a global index). Can't we
		 * just have some simpler struct representing it?
		 */
		modified_index = simple_copy_indexinfo_to_parent(root, rel, ind);

		/*
		 * If the unique index doesn't contain partkey, then it is unique
		 * on this partition only, so it is useless for us.
		 *
		 * XXX Can't we do this check before simple_copy_indexinfo_to_parent?
		 */
		if (!index_constains_partkey(rel, modified_index))
			continue;

		global_uniq_indexlist = lappend(global_uniq_indexlist,  modified_index);
	}

	/* if there are no applicable unique indexes, we're done */
	if (!global_uniq_indexlist)
		return;

	/*
	 * We iterate over the child relations first, and inspect the unique
	 * indexes for each hild, because this way we can stop early if we
	 * happen to eliminate all the unique indexes.
	 */
	foreach(lc, childrels)
	{
		RelOptInfo *child = lfirst(lc);

		/* skip the first index, which is where we got the list from */
		if (is_first)
		{
			is_first = false;
			continue;
		}

		/* match the unique keys to indexes on this child */
		adjust_partition_unique_indexlist(root, rel, child, &global_uniq_indexlist);

		/*
		 * If we have eliminated all unique indexes, no point in looking at
		 * the remaining child relations.
		 */
		if (!global_uniq_indexlist)
			break;
	}

	/* Now we have a list of unique index which are exactly same on all child
	 * relations. Set the UniqueKey just like it is non-partition table.
	 */
	populate_baserel_uniquekeys(root, rel, global_uniq_indexlist);
}


/*
 * populate_distinctrel_uniquekeys
 *		Update unique keys for relation produced by DISTINCT.
 *
 * We can keep all unique keys from the input relations, because DISTINCT
 * can only remove rows - it can't duplicate them. Also, the DISTINCT clause
 * itself is a unique key, so add that.
 */
void
populate_distinctrel_uniquekeys(PlannerInfo *root,
								RelOptInfo *inputrel,
								RelOptInfo *distinctrel)
{
	/* The unique key before the distinct is still valid. */
	distinctrel->uniquekeys = list_copy(inputrel->uniquekeys);

	add_uniquekey_from_sortgroups(root, distinctrel, root->parse->distinctClause);
}

/*
 * populate_grouprel_uniquekeys
 *		
 */
void
populate_grouprel_uniquekeys(PlannerInfo *root,
							 RelOptInfo *grouprel,
							 RelOptInfo *inputrel)

{
	Query *parse = root->parse;
	ListCell *lc;

	/*
	 * XXX Is this actually valid, before checking fro grouping sets?
	 * The grouping sets may produce duplicate row even with just a single
	 * input row, I think.
	 */
	if (relation_is_onerow(inputrel))
	{
		add_uniquekey_for_onerow(grouprel);
		return;
	}

	/*
	 * Bail out if there are grouping sets.
	 *
	 * XXX Could we maybe inspect the grouping sets and determine if this
	 * generates distinct combinations? In some cases that's clearly not
	 * the case (rollup, cube), but for some simple cases it might.
	 */
	if (parse->groupingSets)
		return;

	/* It has aggregation but without a group by, so only one row returned */
	if (!parse->groupClause)
		add_uniquekey_for_onerow(grouprel);

	/*
	 * A regular group by, without grouping sets.
	 *
	 * Obviously, the whole group clause determines a unique key. But if
	 * there are smaller unique keys on the input rel, we prefer those
	 * because those are more flexible. If (a,b) is unique, (a,b,c) is
	 * unique too. Only when there are no such smaller unique keys, we
	 * add the unique key derived from the group clause.
	 */
	foreach(lc, inputrel->uniquekeys)
	{
		UniqueKey *ukey = lfirst_node(UniqueKey, lc);

		/*
		 * Ignore unique keys on the input that are not subset of the
		 * group clause. We can't use incomplete unique keys.
		 */
		if (!list_is_subset(ukey->exprs, grouprel->reltarget->exprs))
			continue;

		grouprel->uniquekeys = lappend(grouprel->uniquekeys, ukey);
	}

	/*
	 * Group clause must be a super-set of of grouprel->reltarget->exprs,
	 * except for the aggregation expressions. So if we found a smaller
	 * unique key on the input relation, don't bother adding a unique key
	 * for the group clause.
	 */
	if (!grouprel->uniquekeys)
		add_uniquekey_from_sortgroups(root,
									  grouprel,
									  root->parse->groupClause);
}

/*
 * simple_copy_uniquekeys
 *		Copy yhe unique keys between relations.
 *
 * Using a function for the one-line code makes us easy to check where we
 * simply copied the uniquekey.
 *
 * XXX Seems like an overkill, not sure what's the purpose?
 */
void
simple_copy_uniquekeys(RelOptInfo *oldrel,
					   RelOptInfo *newrel)
{
	newrel->uniquekeys = oldrel->uniquekeys;
}

/*
 * populate_unionrel_uniquekeys
 *		Determine unique keys for UNION relation.
 *
 * XXX Does this need to care about UNION vs. UNION ALL? At least in the
 * one-row code path?
 */
void
populate_unionrel_uniquekeys(PlannerInfo *root,
							 RelOptInfo *unionrel)
{
	ListCell   *lc;
	List	   *exprs = NIL;

	Assert(unionrel->uniquekeys == NIL);

	/* XXX Why are we copying the expressions? */
	foreach(lc, unionrel->reltarget->exprs)
		exprs = lappend(exprs, lfirst(lc));

	/* SQL: select union select; is valid, we need to handle it here. */
	if (exprs == NIL)
		add_uniquekey_for_onerow(unionrel);
	else
		unionrel->uniquekeys = lappend(unionrel->uniquekeys,
									   makeUniqueKey(exprs,false));

}

/*
 * populate_joinrel_uniquekeys
 *		Determine unique keys for a join relation.
 *
 * populate uniquekeys for joinrel. We will check each relation to see if its
 * UniqueKey is still valid via innerrel_keeps_unique, if so, we add it to
 * joinrel.  The multi_nullvals field will be changed to true for some outer
 * join cases and one-row UniqueKey needs to be converted to normal UniqueKey
 * for the same case as well.
 * For the uniquekey in either baserel which can't be unique after join, we still
 * check to see if combination of UniqueKeys from both side is still useful for us.
 * if yes, we add it to joinrel as well.
 */
void
populate_joinrel_uniquekeys(PlannerInfo *root, RelOptInfo *joinrel,
							RelOptInfo *outerrel, RelOptInfo *innerrel,
							List *restrictlist, JoinType jointype)
{
	ListCell   *lc,
			   *lc2;
	List	   *clause_list = NIL;
	List	   *outerrel_ukey_ctx;
	List	   *innerrel_ukey_ctx;
	bool		inner_onerow,
				outer_onerow;
	bool		mergejoin_allowed;

	/* For SEMI/ANTI join, we care only about the outerrel unique keys. */
	if (jointype == JOIN_SEMI || jointype == JOIN_ANTI)
	{
		foreach(lc, outerrel->uniquekeys)
		{
			UniqueKey	*uniquekey = lfirst_node(UniqueKey, lc);

			/* Keep the unique key if it's included in the joinrel. */
			if (list_is_subset(uniquekey->exprs, joinrel->reltarget->exprs))
				joinrel->uniquekeys = lappend(joinrel->uniquekeys, uniquekey);
		}

		return;
	}

	/* XXX What about JOIN_RIGHT? */
	Assert(jointype == JOIN_LEFT || jointype == JOIN_FULL || jointype == JOIN_INNER);

	/*
	 * For regular joins, we need to combine unique keys from both sides
	 * of the join, to get a new unique key for the join relation. So if
	 * either side does not have a unique key, bail out.
	 */
	if (innerrel->uniquekeys == NIL || outerrel->uniquekeys == NIL)
		return;

	/* XXX maybe move to the if blocks? Not needed outside. */
	inner_onerow = relation_is_onerow(innerrel);
	outer_onerow = relation_is_onerow(outerrel);

	outerrel_ukey_ctx = initialize_uniquecontext_for_joinrel(outerrel);
	innerrel_ukey_ctx = initialize_uniquecontext_for_joinrel(innerrel);

	clause_list = select_mergejoin_clauses(root,
										   joinrel, outerrel, innerrel,
										   restrictlist, jointype,
										   &mergejoin_allowed);

	/*
	 * XXX Seems a bit weird that it's called innerrel_keeps_unique but we
	 * seem to use it in both directions. Or what's the "reverse" for? The
	 * "reverse" name is not particularly descriptive.
	 */
	if (innerrel_keeps_unique(root, innerrel, outerrel, clause_list, true))
	{
		bool	outer_impact = (jointype == JOIN_FULL);

		/* Inspect unique keys on the outer relation. */
		foreach(lc, outerrel_ukey_ctx)
		{
			UniqueKeyContext ctx = (UniqueKeyContext)lfirst(lc);

			/*
			 * If the output of the join does not include all the parts of the
			 * unique key, it's useless, so mark it accordingly and ignore it.
			 */
			if (!list_is_subset(ctx->uniquekey->exprs, joinrel->reltarget->exprs))
			{
				ctx->useful = false;
				continue;
			}

			/*
			 * When the outer relation has one row, and the unique key is not
			 * duplicated after join, so the joinrel will still have just one
			 * row unless the jointype == JOIN_FULL. In that case we're done,
			 * it's the strictest unique key possible.
			 *
			 * If it's one-row with a JOIN_FULL, it might produce multiple
			 * rows with NULLs, so set multi_nullvals. We also need to set
			 * the exprs correctly since it can't be NIL any more.
			 *
			 * For other cases (not one-row relation), we just reuse the
			 * unique key, but we may need to tweak the multi_nullvals.
			 */
			if (outer_onerow && !outer_impact)
			{
				add_uniquekey_for_onerow(joinrel);
				return;
			}
			else if (outer_onerow)	/* one-row and FULL join */
			{
				ListCell *lc2;

				foreach(lc2, get_exprs_from_uniquekey(root, joinrel, outerrel, NULL))
				{
					joinrel->uniquekeys = lappend(joinrel->uniquekeys,
												  makeUniqueKey(lfirst(lc2), true));
				}
			}
			else
			{
				if (!ctx->uniquekey->multi_nullvals && outer_impact)
					/* Change multi_nullvals to true due to the full join. */
					joinrel->uniquekeys = lappend(joinrel->uniquekeys,
												  makeUniqueKey(ctx->uniquekey->exprs, true));
				else
					/* Just reuse it */
					joinrel->uniquekeys = lappend(joinrel->uniquekeys,
												  ctx->uniquekey);
			}

			/*
			 * Mark the unique key as added, so that we can ignore it later
			 * when combining unique keys from both sides of the join.
			 */
			ctx->added_to_joinrel = true;
		}
	}

	/*
	 * XXX Seems this actually checks if "outerrel keeps unique" so the name
	 * is misleading. Of maybe it's the previous block, not sure.
	 *
	 * XXX So why does this consider JOIN_FULL and JOIN_LEFT, while the previous
	 * block only cares about JOIN_FULL?
	 *
	 * XXX This is almost exact copy of the previous block, so maybe make it
	 * a separate function and just call it twice?
	 */
	if (innerrel_keeps_unique(root, outerrel, innerrel, clause_list, false))
	{
		bool	outer_impact = (jointype == JOIN_FULL || jointype == JOIN_LEFT);

		/* Inspect unique keys on the inner relation. */
		foreach(lc, innerrel_ukey_ctx)
		{
			UniqueKeyContext ctx = (UniqueKeyContext)lfirst(lc);

			/*
			 * If the output of the join does not include all the parts of the
			 * unique key, it's useless, so mark it accordingly and ignore it.
			 */
			if (!list_is_subset(ctx->uniquekey->exprs, joinrel->reltarget->exprs))
			{
				ctx->useful = false;
				continue;
			}

			if (inner_onerow &&  !outer_impact)
			{
				add_uniquekey_for_onerow(joinrel);
				return;
			}
			else if (inner_onerow)
			{
				ListCell *lc2;
				foreach(lc2, get_exprs_from_uniquekey(root, joinrel, innerrel, NULL))
				{
					joinrel->uniquekeys = lappend(joinrel->uniquekeys,
												  makeUniqueKey(lfirst(lc2), true));
				}
			}
			else
			{
				if (!ctx->uniquekey->multi_nullvals && outer_impact)
					/* Need to change multi_nullvals to true due to the outer join. */
					joinrel->uniquekeys = lappend(joinrel->uniquekeys,
												  makeUniqueKey(ctx->uniquekey->exprs,
																true));
				else
					joinrel->uniquekeys = lappend(joinrel->uniquekeys,
												  ctx->uniquekey);

			}

			/*
			 * Mark the unique key as added, so that we can ignore it later
			 * when combining unique keys from both sides of the join.
			 */
			ctx->added_to_joinrel = true;
		}
	}

	/*
	 * XXX What if either of the previous two conditions did not match? In
	 * that case we haven't updated the useful flag, and maybe the unique
	 * key is not useful, but we don't know, right? So we should not be
	 * using it in the next loop. Or maybe we should evaluate the flag
	 * before the loops.
	 */

	/*
	 * The combination of the UniqueKey from both sides is unique as well,
	 * regardless of the join type. But don't bother to add it if its
	 * subset has been added to joinrel already or when it's not useful for
	 * the joinrel.
	 *
	 * XXX Maybe we should have a flag that both sides have useful keys?
	 * Or maybe the loops are short/cheap?
	 */
	foreach(lc, outerrel_ukey_ctx)
	{
		UniqueKeyContext ctx1 = (UniqueKeyContext) lfirst(lc);

		/* when not useful or already added to the joinrel, skip it */
		if (ctx1->added_to_joinrel || !ctx1->useful)
			continue;

		foreach(lc2, innerrel_ukey_ctx)
		{
			UniqueKeyContext ctx2 = (UniqueKeyContext) lfirst(lc2);

			/* when not useful or already added to the joinrel, skip it */
			if (ctx2->added_to_joinrel || !ctx2->useful)
				continue;

			/* If we add a onerow UniqueKey, we don't need another key. */
			if (add_combined_uniquekey(root, joinrel, outerrel, innerrel,
									   ctx1->uniquekey, ctx2->uniquekey,
									   jointype))
				return;
		}
	}
}


/*
 * convert_subquery_uniquekeys
 *		Covert the UniqueKey in subquery to outer relation.
 *
 * XXX Explain what exactly does the conversion do?
 */
void convert_subquery_uniquekeys(PlannerInfo *root,
								 RelOptInfo *currel,
								 RelOptInfo *sub_final_rel)
{
	ListCell	*lc;

	if (sub_final_rel->uniquekeys == NIL)
		return;

	if (relation_is_onerow(sub_final_rel))
	{
		add_uniquekey_for_onerow(currel);
		return;
	}

	Assert(currel->subroot != NULL);

	foreach(lc, sub_final_rel->uniquekeys)
	{
		UniqueKey *ukey = lfirst_node(UniqueKey, lc);
		ListCell	*lc;
		List	*exprs = NIL;
		bool	ukey_useful = true;

		/* One row case is handled above */
		Assert(ukey->exprs != NIL);
		foreach(lc, ukey->exprs)
		{
			Var *var;
			TargetEntry *tle = tlist_member(lfirst(lc),
											currel->subroot->processed_tlist);
			if (tle == NULL)
			{
				ukey_useful = false;
				break;
			}
			var = find_var_for_subquery_tle(currel, tle);
			if (var == NULL)
			{
				ukey_useful = false;
				break;
			}
			exprs = lappend(exprs, var);
		}

		if (ukey_useful)
			currel->uniquekeys = lappend(currel->uniquekeys,
										 makeUniqueKey(exprs,
													   ukey->multi_nullvals));

	}
}

/*
 * innerrel_keeps_unique
 *		Check if Unique key on the innerrel is valid after join.
 *
 * innerrel's UniqueKey will be still valid if innerrel's any-column mergeop
 * outrerel's uniquekey exists in clause_list
 *
 * Note: the clause_list must be a list of mergeable restrictinfo already.
 *
 * XXX Misleading name? We seem to use it for "outerrel_keeps_unique" too.
 */
static bool
innerrel_keeps_unique(PlannerInfo *root,
					  RelOptInfo *outerrel,
					  RelOptInfo *innerrel,
					  List *clause_list,
					  bool reverse)
{
	ListCell	*lc, *lc2, *lc3;

	/* XXX probably not needed, duplicate with the check in the caller
	 * (populate_joinrel_uniquekeys). But it's cheap. */
	if (outerrel->uniquekeys == NIL || innerrel->uniquekeys == NIL)
		return false;

	/* Check if there is outerrel's uniquekey in mergeable clause. */
	foreach(lc, outerrel->uniquekeys)
	{
		List   *outer_uq_exprs = lfirst_node(UniqueKey, lc)->exprs;
		bool	clauselist_matchs_all_exprs = true;

		foreach(lc2, outer_uq_exprs)
		{
			Node *outer_uq_expr = lfirst(lc2);
			bool find_uq_expr_in_clauselist = false;

			foreach(lc3, clause_list)
			{
				RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc3);
				Node *outer_expr;

				if (reverse)
					outer_expr = rinfo->outer_is_left ? get_rightop(rinfo->clause) : get_leftop(rinfo->clause);
				else
					outer_expr = rinfo->outer_is_left ? get_leftop(rinfo->clause) : get_rightop(rinfo->clause);

				if (equal(outer_expr, outer_uq_expr))
				{
					find_uq_expr_in_clauselist = true;
					break;
				}
			}
			if (!find_uq_expr_in_clauselist)
			{
				/* No need to check the next exprs in the current uniquekey */
				clauselist_matchs_all_exprs = false;
				break;
			}
		}

		if (clauselist_matchs_all_exprs)
			return true;
	}
	return false;
}


/*
 * relation_is_onerow
 *		Check if it is a one-row relation by checking UniqueKey.
 *
 * The one-row is a special case - there has to be just a single unique key,
 * with no expressions.
 */
bool
relation_is_onerow(RelOptInfo *rel)
{
	UniqueKey *ukey;

	/* there has to be exactly one unique key */
	if (list_length(rel->uniquekeys) != 1)
		return false;

	ukey = linitial_node(UniqueKey, rel->uniquekeys);

	/* the unique key must have no expressions */
	return (ukey->exprs == NIL);
}

/*
 * relation_has_uniquekeys_for
 *		Determines if the relation has unique key for a list of expressions.
 *
 * Returns true iff we can prove that the relation cannot return multiple rows
 * with the same values in the provided expression.
 *
 * allow_multinulls determines whether we allow multiple NULL values or not.
 *
 * The special "one-row" unique key is considered incompatible with all
 * possible expressions.
 */
bool
relation_has_uniquekeys_for(PlannerInfo *root, RelOptInfo *rel,
							List *exprs, bool allow_multinulls)
{
	ListCell *lc;

	/*
	 * For UniqueKey->onerow case, the uniquekey->exprs is empty as well
	 * so we can't rely on list_is_subset to handle this special cases
	 */
	if (exprs == NIL)
		return false;

	foreach(lc, rel->uniquekeys)
	{
		UniqueKey *ukey = lfirst_node(UniqueKey, lc);

		if (ukey->multi_nullvals && !allow_multinulls)
			continue;

		if (list_is_subset(ukey->exprs, exprs))
			return true;
	}

	return false;
}


/*
 * get_exprs_from_uniqueindex
 *		Return a list of expressions from a unique index.
 *
 * Provided with a list of expressions and opclass families, we try to match
 * it to the index. If useful, we produce a list of index expressions (subset
 * of the list we provided).
 *
 * We simply walk through the index expressions, and for each expression we
 * check three things:
 *
 * 1) If there's a matching (expr = Const) clause, we can simply ignore the
 * expressions. Unique index on (a,b,c) guarantees uniqueness on (a,b) when
 * there's condition (c=1).
 *
 * 2) Check that the index expression is present in the relation we're
 * dealing with. If not, the unique key would be useless anyway, and the
 * index can't produce unique key.
 *
 * XXX Shouldn't it be enough to return NULL when the index is not useful?
 * The extra flag seems a bit unnecessary.
 */
static List *
get_exprs_from_uniqueindex(IndexOptInfo *unique_index,
						   List *const_exprs,
						   List *const_expr_opfamilies,
						   Bitmapset *used_varattrs,
						   bool *useful,
						   bool *multi_nullvals)
{
	List	*exprs = NIL;
	ListCell	*indexpr_item;
	int	c = 0;

	*useful = true;
	*multi_nullvals = false;

	indexpr_item = list_head(unique_index->indexprs);
	for(c = 0; c < unique_index->ncolumns; c++)
	{
		int			attr = unique_index->indexkeys[c];
		Expr	   *expr;
		bool		matched_const = false;
		ListCell   *lc1, *lc2;

		if (attr > 0)
		{
			/* regular attribute, just use the expression from index tlist */
			expr = list_nth_node(TargetEntry, unique_index->indextlist, c)->expr;
		}
		else if (attr == 0)
		{
			/* expression from the index */
			expr = lfirst(indexpr_item);
			indexpr_item = lnext(unique_index->indexprs, indexpr_item);
		}
		else /* attr < 0 */
		{
			/* Index on system column is not supported */
			Assert(false);
		}

		/* should have a valid expression now */
		Assert(expr);

		/*
		 * Check if there's (index_col = Const) condition, and that it's using
		 * a compatible opfamily. If yes, we can remove the index_col from the
		 * final UniqueKey->exprs, because the value is constant (so removing
		 * it can't introduce duplicities).
		 */
		forboth(lc1, const_exprs, lc2, const_expr_opfamilies)
		{
			List   *opfamilies = (List *) lfirst(lc2);
			Node   *cexpr = (Node *) lfirst(lc1);

			if (list_member_oid(opfamilies, unique_index->opfamily[c]) &&
				match_index_to_operand(cexpr, c, unique_index))
			{
				matched_const = true;
				break;
			}
		}

		/* it's constant, so ignore the expression */
		if (matched_const)
			continue;

		/*
		 * Check if the indexed expr is used in rel. We do this after the
		 * (col = Const) check, because nn expression may be in a a restrict
		 * clause and not in the reltarget. So we don't want to rule out an
		 * index unnecessarily.
		 */
		if (attr > 0)
		{
			/*
			 * Normal indexed column, if the col is not used, then the index
			 * is useless for uniquekey.
			 */
			attr -= FirstLowInvalidHeapAttributeNumber;

			if (!bms_is_member(attr, used_varattrs))
			{
				*useful = false;
				break;
			}
		}
		else if (!list_member(unique_index->rel->reltarget->exprs, expr))
		{
			/* Expression index but the expression is not used in rel */
			*useful = false;
			break;
		}

		/* check not null property. */
		if (attr == 0)
		{
			/* We never know if an expression yields null or not */
			*multi_nullvals = true;
		}
		else if (!bms_is_member(attr, unique_index->rel->notnullattrs) &&
				 !bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
								unique_index->rel->notnullattrs))
		{
			*multi_nullvals = true;
		}

		exprs = lappend(exprs, expr);
	}

	return exprs;
}


/*
 * add_uniquekey_for_onerow
 *		Create a special unique key signifying that the rel has one row.
 *
 * If we are sure that the relation only returns one row (it might return
 * no rows, but we still consider that unique), then all the columns are
 * trivially unique.
 *
 * However we don't need to create UniqueKey with every column, we just
 * set exprs = NIL, because that's easier to identify. We don't want to
 * add unnecessary unique keys (such that we already have a unique key
 * for a subset of the expressions), and with (exprs == NIL) we can just
 * assume we have one unique key for each column in the rel.
 *
 * We discard all other unique keys, since it has the strongest semantics.
 */
void
add_uniquekey_for_onerow(RelOptInfo *rel)
{
	/*
	 * We overwrite the previous UniqueKey on purpose since this one has
	 * the strongest semantic (all other unique keys are implied by it).
	 */
	rel->uniquekeys = list_make1(makeUniqueKey(NIL, false));
}


/*
 * initialize_uniquecontext_for_joinrel
 *		Return a List of UniqueKeyContext for an inputrel.
 */
static List *
initialize_uniquecontext_for_joinrel(RelOptInfo *inputrel)
{
	List	   *res = NIL;
	ListCell   *lc;

	foreach(lc, inputrel->uniquekeys)
	{
		UniqueKeyContext context;

		context = palloc(sizeof(struct UniqueKeyContextData));
		context->uniquekey = lfirst_node(UniqueKey, lc);
		context->added_to_joinrel = false;
		context->useful = true;

		res = lappend(res, context);
	}

	return res;
}

/*
 * get_exprs_from_uniquekey
 *		Extract expressions that are part of a unique key.
 *
 * The meaning of the result is a bit different in regular and one-row cases.
 * For the regular case, the list of expressions form a single unique key,
 * i.e. the combination of values is unique.
 *
 * For the one-row case, each individual expression is known to be unique
 * (simply because in a single row everything is unique).
 *
 * rel1: The relation which you want to get the exprs.
 * ukey: The UniqueKey you want to get the exprs.
 */
static List *
get_exprs_from_uniquekey(PlannerInfo *root, RelOptInfo *joinrel,
						 RelOptInfo *rel1, UniqueKey *ukey)
{
	ListCell   *lc;
	List	   *res = NIL;
	bool		onerow = (rel1 != NULL) && relation_is_onerow(rel1);

	/* We require at least one of those to be true. */
	Assert(onerow || ukey);

	/* if not a one-row unique key, just return the key's expressions */
	if (!onerow)
		return list_make1(ukey->exprs);

	/*
	 * If it's a one-row relation, we simply extract the expressions that
	 * still exist in the reltarget.
	 */
	foreach(lc, joinrel->reltarget->exprs)
	{
		Bitmapset  *relids = pull_varnos(root, lfirst(lc));

		if (bms_is_subset(relids, rel1->relids))
			res = lappend(res, list_make1(lfirst(lc)));
	}

	return res;
}

/*
 * Partitioned table Unique Keys.
 * The partition table unique key is maintained as:
 * 1. The index must be unique as usual.
 * 2. The index must contains partition key.
 * 3. The index must exist on all the child rel. see simple_indexinfo_equal for
 *    how we compare it.
 */

/*
 * index_constains_partkey
 *		Determines if the index includes a partition key.
 *
 * XXX Surely we already have a code doing this already? E.g. when creating
 * a unique index on a partitioned table we define that.
 */
static bool
index_constains_partkey(RelOptInfo *partrel, IndexOptInfo *ind)
{
	ListCell	*lc;
	int	i;

	Assert(IS_PARTITIONED_REL(partrel));
	Assert(partrel->part_scheme->partnatts > 0);

	for(i = 0; i < partrel->part_scheme->partnatts; i++)
	{
		Node   *part_expr = linitial(partrel->partexprs[i]);
		bool	found_in_index = false;

		foreach(lc, ind->indextlist)
		{
			Expr   *index_expr = lfirst_node(TargetEntry, lc)->expr;

			if (equal(index_expr, part_expr))
			{
				found_in_index = true;
				break;
			}
		}

		if (!found_in_index)
			return false;
	}

	return true;
}

/*
 * simple_indexinfo_equal
 *		Compare two indexes to determine if they are the same.
 *
 * We need to do this because simple_copy_indexinfo_to_parent does change
 * some elements. So this is not exactly the same as calling equal().
 *
 * XXX I wonder if we could simply use equal(), somehow? In fact, we should
 * probably build something much simpler than IndexOptInfo, just enough to
 * do the checks.
 */
static bool
simple_indexinfo_equal(IndexOptInfo *ind1, IndexOptInfo *ind2)
{
	Size oid_cmp_len = sizeof(Oid) * ind1->ncolumns;

	return ((ind1->ncolumns == ind2->ncolumns) &&
			(ind1->unique == ind2->unique) &&
			(memcmp(ind1->indexkeys, ind2->indexkeys, sizeof(int) * ind1->ncolumns) == 0) &&
			(memcmp(ind1->opfamily, ind2->opfamily, oid_cmp_len) == 0) &&
			(memcmp(ind1->opcintype, ind2->opcintype, oid_cmp_len) == 0) &&
			(memcmp(ind1->sortopfamily, ind2->sortopfamily, oid_cmp_len) == 0) &&
			(equal(get_tlist_exprs(ind1->indextlist, true),
				   get_tlist_exprs(ind2->indextlist, true))));
}


/*
 * The below macros are used for simple_copy_indexinfo_to_parent which is so
 * customized that I don't want to put it to copyfuncs.c. So copy it here.
 */
#define COPY_POINTER_FIELD(fldname, sz) \
	do { \
		Size	_size = (sz); \
		newnode->fldname = palloc(_size); \
		memcpy(newnode->fldname, from->fldname, _size); \
	} while (0)

#define COPY_NODE_FIELD(fldname) \
	(newnode->fldname = copyObjectImpl(from->fldname))

#define COPY_SCALAR_FIELD(fldname) \
	(newnode->fldname = from->fldname)


/*
 * simple_copy_indexinfo_to_parent
 *		Copy index info from child to parent, with necessary tweaks.
 *
 * We use this copy to check:
 *
 * 1. If the same/matching index exists in all the childrels.
 * 2. If the parentrel->reltarget/basicrestrict info matches this index.
 *
 * XXX IMHO we should probably build something much simpler than a full
 * IndexOptInfo copy, just enough to do the checks.
 *
 * XXX The fact that we copy so much data seems wrong, and having to
 * define macros from copyfuncs.c seems like a very suspicious thing.
 * One reason is that IndeOptInfo is fairly large struct, especially
 * with all the fields, and we allocate it very often.
 */
static IndexOptInfo *
simple_copy_indexinfo_to_parent(PlannerInfo *root,
								RelOptInfo *parentrel,
								IndexOptInfo *from)
{
	IndexOptInfo *newnode = makeNode(IndexOptInfo);
	AppendRelInfo *appinfo = find_appinfo_by_child(root, from->rel->relid);
	ListCell	*lc;
	int	idx = 0;

	COPY_SCALAR_FIELD(ncolumns);
	COPY_SCALAR_FIELD(nkeycolumns);
	COPY_SCALAR_FIELD(unique);
	COPY_SCALAR_FIELD(immediate);
	/* We just need to know if it is NIL or not */
	COPY_SCALAR_FIELD(indpred);
	COPY_SCALAR_FIELD(predOK);
	COPY_POINTER_FIELD(indexkeys, from->ncolumns * sizeof(int));
	COPY_POINTER_FIELD(indexcollations, from->ncolumns * sizeof(Oid));
	COPY_POINTER_FIELD(opfamily, from->ncolumns * sizeof(Oid));
	COPY_POINTER_FIELD(opcintype, from->ncolumns * sizeof(Oid));
	COPY_POINTER_FIELD(sortopfamily, from->ncolumns * sizeof(Oid));
	COPY_NODE_FIELD(indextlist);

	/* Convert index exprs on child expr to expr on parent */
	foreach(lc, newnode->indextlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		/* Index on expression is ignored */
		Assert(IsA(tle->expr, Var));
		tle->expr = (Expr *) find_parent_var(appinfo, (Var *) tle->expr);
		newnode->indexkeys[idx] = castNode(Var, tle->expr)->varattno;
		idx++;
	}
	newnode->rel = parentrel;
	return newnode;
}

/*
 * adjust_partition_unique_indexlist
 *		Checks and eliminates indexes that do not exist on the child relation.
 *
 * Walks the list of unique indexes, and eliminates those that don't match
 * the child relation (i.e. where a matching child index does not exist).
 * This is used to iteratively filter the list of candidate unique keys.
 *
 * After processing all child relations, the list contains only indexes that
 * exist in all the child relations.
 */
static void
adjust_partition_unique_indexlist(PlannerInfo *root,
								  RelOptInfo *parentrel,
								  RelOptInfo *childrel,
								  List **indexes)
{
	ListCell	*lc, *lc2;

	foreach(lc, *indexes)
	{
		IndexOptInfo	*g_ind = lfirst_node(IndexOptInfo, lc);
		bool found_in_child = false;

		foreach(lc2, childrel->indexlist)
		{
			IndexOptInfo   *p_ind = lfirst_node(IndexOptInfo, lc2);
			IndexOptInfo   *p_ind_copy;

			/*
			 * Ignore child indexes that can't possibly match (not unique or
			 * immediate, etc.)
			 *
			 * XXX We do these checks in many places, so maybe turn it into
			 * a reusable macro?
			 */
			if ((!p_ind->unique) || (!p_ind->immediate) ||
				(p_ind->indpred != NIL) && (!p_ind->predOK))
				continue;

			/*
			 * XXX This seems possibly quite expensive. Imagine there are many
			 * child relations, with a bunch of unique indexes each. Then this
			 * generates a copy for each unique index in each child relation,
			 * something like O(N^2/2) copies.
			 */
			p_ind_copy = simple_copy_indexinfo_to_parent(root, parentrel, p_ind);

			/* Found a matching index for the child relation, we're done. */
			if (simple_indexinfo_equal(p_ind_copy, g_ind))
			{
				found_in_child = true;
				break;
			}
		}

		/* No matching index in the child, so remove it from the list. */
		if (!found_in_child)
			*indexes = foreach_delete_current(*indexes, lc);
	}
}

/*
 * Helper function for groupres/distinctrel
 *
 * FIXME Not sure about this.
 */
static void
add_uniquekey_from_sortgroups(PlannerInfo *root, RelOptInfo *rel, List *sortgroups)
{
	Query *parse = root->parse;
	List	*exprs;

	/*
	 * XXX: If there are some vars which are not in the current levelsup, the
	 * semantic is imprecise, should we avoid it or not? levelsup = 1 is just
	 * a demo, maybe we need to check every level other than 0, if so, looks
	 * we have to write another pull_var_walker.
	 */
	List	*upper_vars = pull_vars_of_level((Node*)sortgroups, 1);

	if (upper_vars != NIL)
		return;

	/* sortgroupclause can't be multi_nullvals */
	exprs = get_sortgrouplist_exprs(sortgroups, parse->targetList);
	rel->uniquekeys = lappend(rel->uniquekeys,
							  makeUniqueKey(exprs, false));
}


/*
 * add_combined_uniquekey
 *		Add a unique key for a join, combined from keys on inner/outer side.
 *
 * The combination of both UniqueKeys is a valid UniqueKey for joinrel no
 * matter what's the exact jointype.
 *
 * Returns true if the unique key is "one-row" variant, so that the caller
 * can stop considering further combinations.
 */
bool
add_combined_uniquekey(PlannerInfo *root,
					   RelOptInfo *joinrel,
					   RelOptInfo *outer_rel,
					   RelOptInfo *inner_rel,
					   UniqueKey *outer_ukey,
					   UniqueKey *inner_ukey,
					   JoinType jointype)
{
	bool		multi_nullvals;
	ListCell   *lc1, *lc2;

	/*
	 * If either side has multi_nullvals, or we are dealing with an outer join,
	 * the combined UniqueKey has multi_nullvals too.
	 */
	multi_nullvals = outer_ukey->multi_nullvals ||
		inner_ukey->multi_nullvals || IS_OUTER_JOIN(jointype);

	/* The only case we can get onerow joinrel after join */
	if (relation_is_onerow(outer_rel) &&
		relation_is_onerow(inner_rel) &&
		jointype == JOIN_INNER)
	{
		add_uniquekey_for_onerow(joinrel);
		return true;
	}

	/*
	 * XXX Isn't this wrong? Why is it combining expressions that are part
	 * of the two unique keys? Imagine we have outer unique key on (a1, a2)
	 * and inner outer key on (b1, b2). Then this adds four unique keys
	 * for the join (a1,b1), (a1,b2), (a2,b1) and (a2,b2). Shouldn't it
	 * just add (a1,a2,b1,b2)?
	 */
	foreach(lc1, get_exprs_from_uniquekey(root, joinrel, outer_rel, outer_ukey))
	{
		/*
		 * XXX This calls get_exprs_from_uniquekey repeatedly for each outer
		 * loop. Maybe we should calculate it just once before the loop.
		 */
		foreach(lc2, get_exprs_from_uniquekey(root, joinrel, inner_rel, inner_ukey))
		{
			List *exprs = list_concat_copy(lfirst_node(List, lc1), lfirst_node(List, lc2));

			joinrel->uniquekeys = lappend(joinrel->uniquekeys,
										  makeUniqueKey(exprs,
														multi_nullvals));
		}
	}

	return false;
}

/* FIXME comment */
List*
build_uniquekeys(PlannerInfo *root, List *sortclauses)
{
	List *result = NIL;
	List *sortkeys;
	ListCell *l;
	List *exprs = NIL;

	sortkeys = make_pathkeys_for_uniquekeys(root,
											sortclauses,
											root->processed_tlist);

	/* Create a uniquekey and add it to the list */
	foreach(l, sortkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(l);
		EquivalenceClass *ec = pathkey->pk_eclass;
		EquivalenceMember *mem = (EquivalenceMember*) lfirst(list_head(ec->ec_members));
		exprs = lappend(exprs, mem->em_expr);
	}

	result = lappend(result, makeUniqueKey(exprs, false));

	return result;
}

/* FIXME comment */
bool
query_has_uniquekeys_for(PlannerInfo *root, List *pathuniquekeys,
						 bool allow_multinulls)
{
	ListCell *lc;
	ListCell *lc2;

	/* root->query_uniquekeys are the requested DISTINCT clauses on query level
	 * pathuniquekeys are the unique keys on current path.
	 * All requested query_uniquekeys must be satisfied by the pathuniquekeys
	 */
	foreach(lc, root->query_uniquekeys)
	{
		UniqueKey *query_ukey = lfirst_node(UniqueKey, lc);
		bool satisfied = false;
		foreach(lc2, pathuniquekeys)
		{
			UniqueKey *ukey = lfirst_node(UniqueKey, lc2);
			if (ukey->multi_nullvals && !allow_multinulls)
				continue;
			if (list_length(ukey->exprs) == 0 &&
				list_length(query_ukey->exprs) != 0)
				continue;
			if (list_is_subset(ukey->exprs, query_ukey->exprs))
			{
				satisfied = true;
				break;
			}
		}
		if (!satisfied)
			return false;
	}
	return true;
}
