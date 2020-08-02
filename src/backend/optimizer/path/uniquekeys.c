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

static List *initililze_uniquecontext_for_joinrel(RelOptInfo *inputrel);
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
static List *get_exprs_from_uniquekey(RelOptInfo *joinrel,
									  RelOptInfo *rel1,
									  UniqueKey *ukey);
static void add_uniquekey_for_onerow(RelOptInfo *rel);
static bool add_combined_uniquekey(RelOptInfo *joinrel,
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
 *		Populate 'baserel' uniquekeys list by looking at the rel's unique index
 * and baserestrictinfo
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

	foreach(lc, indexlist)
	{
		IndexOptInfo *ind = (IndexOptInfo *) lfirst(lc);
		if (!ind->unique || !ind->immediate ||
			(ind->indpred != NIL && !ind->predOK))
			continue;
		matched_uniq_indexes = lappend(matched_uniq_indexes, ind);
	}

	if (matched_uniq_indexes  == NIL)
		return;

	/* Check which attrs is used in baserel->reltarget */
	pull_varattnos((Node *)baserel->reltarget->exprs, baserel->relid, &used_attrs);

	/* Check which attrno is used at a mergeable const filter */
	foreach(lc, baserel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		if (rinfo->mergeopfamilies == NIL)
			continue;

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

	foreach(lc, matched_uniq_indexes)
	{
		bool	multi_nullvals, useful;
		List	*exprs = get_exprs_from_uniqueindex(lfirst_node(IndexOptInfo, lc),
													const_exprs,
													expr_opfamilies,
													used_attrs,
													&useful,
													&multi_nullvals);
		if (useful)
		{
			if (exprs == NIL)
			{
				/* All the columns in Unique Index matched with a restrictinfo */
				add_uniquekey_for_onerow(baserel);
				return;
			}
			baserel->uniquekeys = lappend(baserel->uniquekeys,
										  makeUniqueKey(exprs, multi_nullvals));
		}
	}
}


/*
 * populate_partitionedrel_uniquekeys
 * The UniqueKey on partitionrel comes from 2 cases:
 * 1). Only one partition is involved in this query, the unique key can be
 * copied to parent rel from childrel.
 * 2). There are some unique index which includes partition key and exists
 * in all the related partitions.
 * We never mind rule 2 if we hit rule 1.
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

	Assert(IS_PARTITIONED_REL(rel));

	if (childrels == NIL)
		return;

	/*
	 * If there is only one partition used in this query, the UniqueKey in childrel is
	 * still valid in parent level, but we need convert the format from child expr to
	 * parent expr.
	 */
	if (list_length(childrels) == 1)
	{
		/* Check for Rule 1 */
		RelOptInfo *childrel = linitial_node(RelOptInfo, childrels);
		ListCell	*lc;
		Assert(childrel->reloptkind == RELOPT_OTHER_MEMBER_REL);
		if (relation_is_onerow(childrel))
		{
			add_uniquekey_for_onerow(rel);
			return;
		}

		foreach(lc, childrel->uniquekeys)
		{
			UniqueKey *ukey = lfirst_node(UniqueKey, lc);
			AppendRelInfo *appinfo = find_appinfo_by_child(root, childrel->relid);
			List *parent_exprs = NIL;
			bool can_reuse = true;
			ListCell	*lc2;
			foreach(lc2, ukey->exprs)
			{
				Var *var = (Var *)lfirst(lc2);
				/*
				 * If the expr comes from a expression, it is hard to build the expression
				 * in parent so ignore that case for now.
				 */
				if(!IsA(var, Var))
				{
					can_reuse = false;
					break;
				}
				/* Convert it to parent var */
				parent_exprs = lappend(parent_exprs, find_parent_var(appinfo, var));
			}
			if (can_reuse)
				rel->uniquekeys = lappend(rel->uniquekeys,
										  makeUniqueKey(parent_exprs,
														ukey->multi_nullvals));
		}
	}
	else
	{
		/* Check for rule 2 */
		childrel = linitial_node(RelOptInfo, childrels);
		foreach(lc, childrel->indexlist)
		{
			IndexOptInfo *ind = lfirst(lc);
			IndexOptInfo *modified_index;
			if (!ind->unique || !ind->immediate ||
				(ind->indpred != NIL && !ind->predOK))
				continue;

			/*
			 * During simple_copy_indexinfo_to_parent, we need to convert var from
			 * child var to parent var, index on expression is too complex to handle.
			 * so ignore it for now.
			 */
			if (ind->indexprs != NIL)
				continue;

			modified_index = simple_copy_indexinfo_to_parent(root, rel, ind);
			/*
			 * If the unique index doesn't contain partkey, then it is unique
			 * on this partition only, so it is useless for us.
			 */
			if (!index_constains_partkey(rel, modified_index))
				continue;

			global_uniq_indexlist = lappend(global_uniq_indexlist,  modified_index);
		}

		if (global_uniq_indexlist != NIL)
		{
			foreach(lc, childrels)
			{
				RelOptInfo *child = lfirst(lc);
				if (is_first)
				{
					is_first = false;
					continue;
				}
				adjust_partition_unique_indexlist(root, rel, child, &global_uniq_indexlist);
			}
			/* Now we have a list of unique index which are exactly same on all childrels,
			 * Set the UniqueKey just like it is non-partition table
			 */
			populate_baserel_uniquekeys(root, rel, global_uniq_indexlist);
		}
	}
}


/*
 * populate_distinctrel_uniquekeys
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
 */
void
populate_grouprel_uniquekeys(PlannerInfo *root,
							 RelOptInfo *grouprel,
							 RelOptInfo *inputrel)

{
	Query *parse = root->parse;
	bool input_ukey_added = false;
	ListCell *lc;

	if (relation_is_onerow(inputrel))
	{
		add_uniquekey_for_onerow(grouprel);
		return;
	}
	if (parse->groupingSets)
		return;

	/* A Normal group by without grouping set. */
	if (parse->groupClause)
	{
		/*
		 * Current even the groupby clause is Unique already, but if query has aggref
		 * We have to create grouprel still. To keep the UnqiueKey short, we will check
		 * the UniqueKey of input_rel still valid, if so we reuse it.
		 */
		foreach(lc, inputrel->uniquekeys)
		{
			UniqueKey *ukey = lfirst_node(UniqueKey, lc);
			if (list_is_subset(ukey->exprs, grouprel->reltarget->exprs))
			{
				grouprel->uniquekeys = lappend(grouprel->uniquekeys,
											   ukey);
				input_ukey_added = true;
			}
		}
		if (!input_ukey_added)
			/*
			 * group by clause must be a super-set of grouprel->reltarget->exprs except the
			 * aggregation expr, so if such exprs is unique already, no bother to generate
			 * new uniquekey for group by exprs.
			 */
			add_uniquekey_from_sortgroups(root,
										  grouprel,
										  root->parse->groupClause);
	}
	else
		/* It has aggregation but without a group by, so only one row returned */
		add_uniquekey_for_onerow(grouprel);
}

/*
 * simple_copy_uniquekeys
 * Using a function for the one-line code makes us easy to check where we simply
 * copied the uniquekey.
 */
void
simple_copy_uniquekeys(RelOptInfo *oldrel,
					   RelOptInfo *newrel)
{
	newrel->uniquekeys = oldrel->uniquekeys;
}

/*
 *  populate_unionrel_uniquekeys
 */
void
populate_unionrel_uniquekeys(PlannerInfo *root,
							  RelOptInfo *unionrel)
{
	ListCell	*lc;
	List	*exprs = NIL;

	Assert(unionrel->uniquekeys == NIL);

	foreach(lc, unionrel->reltarget->exprs)
	{
		exprs = lappend(exprs, lfirst(lc));
	}

	if (exprs == NIL)
		/* SQL: select union select; is valid, we need to handle it here. */
		add_uniquekey_for_onerow(unionrel);
	else
		unionrel->uniquekeys = lappend(unionrel->uniquekeys,
									   makeUniqueKey(exprs,false));

}

/*
 * populate_joinrel_uniquekeys
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
	ListCell *lc, *lc2;
	List	*clause_list = NIL;
	List	*outerrel_ukey_ctx;
	List	*innerrel_ukey_ctx;
	bool	inner_onerow, outer_onerow;
	bool	mergejoin_allowed;

	/* Care about the outerrel relation only for SEMI/ANTI join */
	if (jointype == JOIN_SEMI || jointype == JOIN_ANTI)
	{
		foreach(lc, outerrel->uniquekeys)
		{
			UniqueKey	*uniquekey = lfirst_node(UniqueKey, lc);
			if (list_is_subset(uniquekey->exprs, joinrel->reltarget->exprs))
				joinrel->uniquekeys = lappend(joinrel->uniquekeys, uniquekey);
		}
		return;
	}

	Assert(jointype == JOIN_LEFT || jointype == JOIN_FULL || jointype == JOIN_INNER);

	/* Fast path */
	if (innerrel->uniquekeys == NIL || outerrel->uniquekeys == NIL)
		return;

	inner_onerow = relation_is_onerow(innerrel);
	outer_onerow = relation_is_onerow(outerrel);

	outerrel_ukey_ctx = initililze_uniquecontext_for_joinrel(outerrel);
	innerrel_ukey_ctx = initililze_uniquecontext_for_joinrel(innerrel);

	clause_list = select_mergejoin_clauses(root, joinrel, outerrel, innerrel,
										   restrictlist, jointype,
										   &mergejoin_allowed);

	if (innerrel_keeps_unique(root, innerrel, outerrel, clause_list, true /* reverse */))
	{
		bool outer_impact = jointype == JOIN_FULL;
		foreach(lc, outerrel_ukey_ctx)
		{
			UniqueKeyContext ctx = (UniqueKeyContext)lfirst(lc);

			if (!list_is_subset(ctx->uniquekey->exprs, joinrel->reltarget->exprs))
			{
				ctx->useful = false;
				continue;
			}

			/* Outer relation has one row, and the unique key is not duplicated after join,
			 * the joinrel will still has one row unless the jointype == JOIN_FULL.
			 */
			if (outer_onerow && !outer_impact)
			{
				add_uniquekey_for_onerow(joinrel);
				return;
			}
			else if (outer_onerow)
			{
				/*
				 * The onerow outerrel becomes multi rows and multi_nullvals
				 * will be changed to true. We also need to set the exprs correctly since it
				 * can't be NIL any more.
				 */
				ListCell *lc2;
				foreach(lc2, get_exprs_from_uniquekey(joinrel, outerrel, NULL))
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
			ctx->added_to_joinrel = true;
		}
	}

	if (innerrel_keeps_unique(root, outerrel, innerrel, clause_list, false))
	{
		bool outer_impact = jointype == JOIN_FULL || jointype == JOIN_LEFT;;

		foreach(lc, innerrel_ukey_ctx)
		{
			UniqueKeyContext ctx = (UniqueKeyContext)lfirst(lc);

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
				foreach(lc2, get_exprs_from_uniquekey(joinrel, innerrel, NULL))
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
			ctx->added_to_joinrel = true;
		}
	}

	/*
	 * The combination of the UniqueKey from both sides is unique as well regardless
	 * of join type, but no bother to add it if its subset has been added to joinrel
	 * already or it is not useful for the joinrel.
	 */
	foreach(lc, outerrel_ukey_ctx)
	{
		UniqueKeyContext ctx1 = (UniqueKeyContext) lfirst(lc);
		if (ctx1->added_to_joinrel || !ctx1->useful)
			continue;
		foreach(lc2, innerrel_ukey_ctx)
		{
			UniqueKeyContext ctx2 = (UniqueKeyContext) lfirst(lc2);
			if (ctx2->added_to_joinrel || !ctx2->useful)
				continue;
			if (add_combined_uniquekey(joinrel, outerrel, innerrel,
									   ctx1->uniquekey, ctx2->uniquekey,
									   jointype))
				/* If we set a onerow UniqueKey to joinrel, we don't need other. */
				return;
		}
	}
}


/*
 * convert_subquery_uniquekeys
 *
 * Covert the UniqueKey in subquery to outer relation.
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
 *
 * Check if Unique key of the innerrel is valid after join. innerrel's UniqueKey
 * will be still valid if innerrel's any-column mergeop outrerel's uniquekey
 * exists in clause_list.
 *
 * Note: the clause_list must be a list of mergeable restrictinfo already.
 */
static bool
innerrel_keeps_unique(PlannerInfo *root,
					  RelOptInfo *outerrel,
					  RelOptInfo *innerrel,
					  List *clause_list,
					  bool reverse)
{
	ListCell	*lc, *lc2, *lc3;

	if (outerrel->uniquekeys == NIL || innerrel->uniquekeys == NIL)
		return false;

	/* Check if there is outerrel's uniquekey in mergeable clause. */
	foreach(lc, outerrel->uniquekeys)
	{
		List	*outer_uq_exprs = lfirst_node(UniqueKey, lc)->exprs;
		bool clauselist_matchs_all_exprs = true;
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
 * Check if it is a one-row relation by checking UniqueKey.
 */
bool
relation_is_onerow(RelOptInfo *rel)
{
	UniqueKey *ukey;
	if (rel->uniquekeys == NIL)
		return false;
	ukey = linitial_node(UniqueKey, rel->uniquekeys);
	return ukey->exprs == NIL && list_length(rel->uniquekeys) == 1;
}

/*
 * relation_has_uniquekeys_for
 *		Returns true if we have proofs that 'rel' cannot return multiple rows with
 *		the same values in each of 'exprs'.  Otherwise returns false.
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
 *
 * Return a list of exprs which is unique. set useful to false if this
 * unique index is not useful for us.
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
		int attr = unique_index->indexkeys[c];
		Expr *expr;
		bool	matched_const = false;
		ListCell	*lc1, *lc2;

		if(attr > 0)
		{
			expr = list_nth_node(TargetEntry, unique_index->indextlist, c)->expr;
		}
		else if (attr == 0)
		{
			/* Expression index */
			expr = lfirst(indexpr_item);
			indexpr_item = lnext(unique_index->indexprs, indexpr_item);
		}
		else /* attr < 0 */
		{
			/* Index on system column is not supported */
			Assert(false);
		}

		/*
		 * Check index_col = Const case with regarding to opfamily checking
		 * If we can remove the index_col from the final UniqueKey->exprs.
		 */
		forboth(lc1, const_exprs, lc2, const_expr_opfamilies)
		{
			if (list_member_oid((List *)lfirst(lc2), unique_index->opfamily[c])
				&& match_index_to_operand((Node *) lfirst(lc1), c, unique_index))
			{
				matched_const = true;
				break;
			}
		}

		if (matched_const)
			continue;

		/* Check if the indexed expr is used in rel */
		if (attr > 0)
		{
			/*
			 * Normal Indexed column, if the col is not used, then the index is useless
			 * for uniquekey.
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
			/* We never know if a expression yields null or not */
			*multi_nullvals = true;
		}
		else if (!bms_is_member(attr, unique_index->rel->notnullattrs)
				 && !bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
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
 * If we are sure that the relation only returns one row, then all the columns
 * are unique. However we don't need to create UniqueKey for every column, we
 * just set exprs = NIL and overwrites all the other UniqueKey on this RelOptInfo
 * since this one has strongest semantics.
 */
void
add_uniquekey_for_onerow(RelOptInfo *rel)
{
	/*
	 * We overwrite the previous UniqueKey on purpose since this one has the
	 * strongest semantic.
	 */
	rel->uniquekeys = list_make1(makeUniqueKey(NIL, false));
}


/*
 * initililze_uniquecontext_for_joinrel
 * Return a List of UniqueKeyContext for an inputrel
 */
static List *
initililze_uniquecontext_for_joinrel(RelOptInfo *inputrel)
{
	List	*res = NIL;
	ListCell *lc;
	foreach(lc,  inputrel->uniquekeys)
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
 *	Unify the way of get List of exprs from a one-row UniqueKey or
 * normal UniqueKey. for the onerow case, every expr in rel1 is a valid
 * UniqueKey. Return a List of exprs.
 *
 * rel1: The relation which you want to get the exprs.
 * ukey: The UniqueKey you want to get the exprs.
 */
static List *
get_exprs_from_uniquekey(RelOptInfo *joinrel, RelOptInfo *rel1, UniqueKey *ukey)
{
	ListCell *lc;
	bool onerow = rel1 != NULL && relation_is_onerow(rel1);

	List	*res = NIL;
	Assert(onerow || ukey);
	if (onerow)
	{
		/* Only cares about the exprs still exist in joinrel */
		foreach(lc, joinrel->reltarget->exprs)
		{
			Bitmapset *relids = pull_varnos(lfirst(lc));
			if (bms_is_subset(relids, rel1->relids))
			{
				res = lappend(res, list_make1(lfirst(lc)));
			}
		}
	}
	else
	{
		res = list_make1(ukey->exprs);
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
 * return true if the index contains the partiton key.
 */
static bool
index_constains_partkey(RelOptInfo *partrel,  IndexOptInfo *ind)
{
	ListCell	*lc;
	int	i;
	Assert(IS_PARTITIONED_REL(partrel));
	Assert(partrel->part_scheme->partnatts > 0);

	for(i = 0; i < partrel->part_scheme->partnatts; i++)
	{
		Node *part_expr = linitial(partrel->partexprs[i]);
		bool found_in_index = false;
		foreach(lc, ind->indextlist)
		{
			Expr *index_expr = lfirst_node(TargetEntry, lc)->expr;
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
 *
 * Used to check if the 2 index is same as each other. The index here
 * is COPIED from childrel and did some tiny changes(see
 * simple_copy_indexinfo_to_parent)
 */
static bool
simple_indexinfo_equal(IndexOptInfo *ind1, IndexOptInfo *ind2)
{
	Size oid_cmp_len = sizeof(Oid) * ind1->ncolumns;

	return ind1->ncolumns == ind2->ncolumns &&
		ind1->unique == ind2->unique &&
		memcmp(ind1->indexkeys, ind2->indexkeys, sizeof(int) * ind1->ncolumns) == 0 &&
		memcmp(ind1->opfamily, ind2->opfamily, oid_cmp_len) == 0 &&
		memcmp(ind1->opcintype, ind2->opcintype, oid_cmp_len) == 0 &&
		memcmp(ind1->sortopfamily, ind2->sortopfamily, oid_cmp_len) == 0 &&
		equal(get_tlist_exprs(ind1->indextlist, true),
			  get_tlist_exprs(ind2->indextlist, true));
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
 * simple_copy_indexinfo_to_parent (from partition)
 * Copy the IndexInfo from child relation to parent relation with some modification,
 * which is used to test:
 * 1. If the same index exists in all the childrels.
 * 2. If the parentrel->reltarget/basicrestrict info matches this index.
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
 *
 * global_unique_indexes: At the beginning, it contains the copy & modified
 * unique index from the first partition. And then check if each index in it still
 * exists in the following partitions. If no, remove it. at last, it has an
 * index list which exists in all the partitions.
 */
static void
adjust_partition_unique_indexlist(PlannerInfo *root,
								  RelOptInfo *parentrel,
								  RelOptInfo *childrel,
								  List **global_unique_indexes)
{
	ListCell	*lc, *lc2;
	foreach(lc, *global_unique_indexes)
	{
		IndexOptInfo	*g_ind = lfirst_node(IndexOptInfo, lc);
		bool found_in_child = false;

		foreach(lc2, childrel->indexlist)
		{
			IndexOptInfo   *p_ind = lfirst_node(IndexOptInfo, lc2);
			IndexOptInfo   *p_ind_copy;
			if (!p_ind->unique || !p_ind->immediate ||
				(p_ind->indpred != NIL && !p_ind->predOK))
				continue;
			p_ind_copy = simple_copy_indexinfo_to_parent(root, parentrel, p_ind);
			if (simple_indexinfo_equal(p_ind_copy, g_ind))
			{
				found_in_child = true;
				break;
			}
		}
		if (!found_in_child)
			/* The index doesn't exist in childrel, remove it from global_unique_indexes */
			*global_unique_indexes = foreach_delete_current(*global_unique_indexes, lc);
	}
}

/* Helper function for groupres/distinctrel */
static void
add_uniquekey_from_sortgroups(PlannerInfo *root, RelOptInfo *rel, List *sortgroups)
{
	Query *parse = root->parse;
	List	*exprs;

	/*
	 * XXX: If there are some vars which is not in current levelsup, the semantic is
	 * imprecise, should we avoid it or not? levelsup = 1 is just a demo, maybe we need to
	 * check every level other than 0, if so, looks we have to write another
	 * pull_var_walker.
	 */
	List	*upper_vars = pull_vars_of_level((Node*)sortgroups, 1);

	if (upper_vars != NIL)
		return;

	exprs = get_sortgrouplist_exprs(sortgroups, parse->targetList);
	rel->uniquekeys = lappend(rel->uniquekeys,
							  makeUniqueKey(exprs,
											false /* sortgroupclause can't be multi_nullvals */));
}


/*
 * add_combined_uniquekey
 * The combination of both UniqueKeys is a valid UniqueKey for joinrel no matter
 * the jointype.
 */
bool
add_combined_uniquekey(RelOptInfo *joinrel,
					   RelOptInfo *outer_rel,
					   RelOptInfo *inner_rel,
					   UniqueKey *outer_ukey,
					   UniqueKey *inner_ukey,
					   JoinType jointype)
{

	ListCell	*lc1, *lc2;

	/* Either side has multi_nullvals or we have outer join,
	 * the combined UniqueKey has multi_nullvals */
	bool multi_nullvals = outer_ukey->multi_nullvals ||
		inner_ukey->multi_nullvals || IS_OUTER_JOIN(jointype);

	/* The only case we can get onerow joinrel after join */
	if  (relation_is_onerow(outer_rel)
		 && relation_is_onerow(inner_rel)
		 && jointype == JOIN_INNER)
	{
		add_uniquekey_for_onerow(joinrel);
		return true;
	}

	foreach(lc1, get_exprs_from_uniquekey(joinrel, outer_rel, outer_ukey))
	{
		foreach(lc2, get_exprs_from_uniquekey(joinrel, inner_rel, inner_ukey))
		{
			List *exprs = list_concat_copy(lfirst_node(List, lc1), lfirst_node(List, lc2));
			joinrel->uniquekeys = lappend(joinrel->uniquekeys,
										  makeUniqueKey(exprs,
														multi_nullvals));
		}
	}
	return false;
}

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
		if (EC_MUST_BE_REDUNDANT(ec))
			continue;
		exprs = lappend(exprs, mem->em_expr);
	}

	result = lappend(result, makeUniqueKey(exprs, false));

	return result;
}

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
