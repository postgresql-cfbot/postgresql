/*-------------------------------------------------------------------------
 *
 * uniquekey.c
 *	  Utilities for maintaining uniquekey.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/uniquekey.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"


/* Functions to populate UniqueKey */
static bool add_uniquekey_for_uniqueindex(PlannerInfo *root,
										  IndexOptInfo *unique_index,
										  List *mergeable_const_peer,
										  List *expr_opfamilies);

static bool is_uniquekey_nulls_removed(PlannerInfo *root,
									   UniqueKey *ukey,
									   RelOptInfo *rel);
static UniqueKey *adjust_uniquekey_multinull_for_joinrel(PlannerInfo *root,
														 UniqueKey *joinrel_ukey,
														 RelOptInfo *rel,
														 bool below_outer_side);

static bool populate_joinrel_uniquekey_for_rel(PlannerInfo *root, RelOptInfo *joinrel,
											   RelOptInfo *rel, RelOptInfo *other_rel,
											   List *restrictlist, JoinType jointype);
static void populate_joinrel_composite_uniquekey(PlannerInfo *root,
												 RelOptInfo *joinrel,
												 RelOptInfo *outerrel,
												 RelOptInfo *innerrel,
												 List	*restrictlist,
												 JoinType jointype,
												 bool outeruk_still_valid,
												 bool inneruk_still_valid);

static void convert_subquery_uniquekey(PlannerInfo *root, RelOptInfo *rel, UniqueKey *sub_ukey);
static EquivalenceClass * find_outer_ec_with_subquery_em(PlannerInfo *root, RelOptInfo *rel,
														 EquivalenceClass *sub_ec,
														 EquivalenceMember *sub_em);
static List *convert_subquery_eclass_list(PlannerInfo *root, RelOptInfo *rel,
										  List *sub_eclass_list);

/* UniqueKey is subset of .. */
static bool uniquekey_contains_in(PlannerInfo *root, UniqueKey *ukey,
								  List *ecs, Relids relids);

/* Avoid useless UniqueKey. */
static bool unique_ecs_useful_for_distinct(PlannerInfo *root, List *ecs);
static bool unique_ecs_useful_for_merging(PlannerInfo *root, RelOptInfo *rel,
										  List *unique_ecs);
static bool is_uniquekey_useful_afterjoin(PlannerInfo *root, UniqueKey *ukey,
										  RelOptInfo *joinrel);

/* Helper functions to create UniqueKey. */
static UniqueKey *make_uniquekey(Bitmapset *unique_expr_indexes,
								 bool multi_null,
								 bool useful_for_distinct);
static void mark_rel_singlerow(PlannerInfo *root, RelOptInfo *rel);

/* Debug only */
static void print_uniquekey(PlannerInfo *root, RelOptInfo *rel);

/*
 * populate_baserel_uniquekeys
 */
void
populate_baserel_uniquekeys(PlannerInfo *root, RelOptInfo *rel)
{
	ListCell	*lc;
	List	*mergeable_const_peer = NIL, *expr_opfamilies = NIL;

	/*
	 * ColX = {Const} AND ColY = {Const2} AND ColZ > {Const3},
	 * gather ColX and ColY into mergeable_const_peer.
	 */
	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		if (rinfo->mergeopfamilies == NIL)
			continue;

		if (bms_is_empty(rinfo->left_relids))
			mergeable_const_peer = lappend(mergeable_const_peer, get_rightop(rinfo->clause));
		else if (bms_is_empty(rinfo->right_relids))
			mergeable_const_peer = lappend(mergeable_const_peer, get_leftop(rinfo->clause));
		else
			continue;
		expr_opfamilies = lappend(expr_opfamilies, rinfo->mergeopfamilies);
	}

	foreach(lc, rel->indexlist)
	{
		IndexOptInfo *index = (IndexOptInfo *)lfirst(lc);
		if (!index->unique || !index->immediate ||
			(index->indpred != NIL && !index->predOK))
			continue;

		if (add_uniquekey_for_uniqueindex(root, index,
										  mergeable_const_peer,
										  expr_opfamilies))
			/* Find a singlerow case, no need to go through any more. */
			return;
	}

	print_uniquekey(root, rel);
}

/*
 * populate_joinrel_uniquekeys
 */
void
populate_joinrel_uniquekeys(PlannerInfo *root, RelOptInfo *joinrel,
							RelOptInfo *outerrel, RelOptInfo *innerrel,
							List *restrictlist, JoinType jointype)
{
	bool outeruk_still_valid = false, inneruk_still_valid = false;
	if (jointype == JOIN_SEMI || jointype == JOIN_ANTI)
	{
		ListCell	*lc;
		foreach(lc, outerrel->uniquekeys)
		{
			/*
			 * SEMI/ANTI join can be used to remove NULL values as well.
			 * So we need to adjust multi_nulls for join.
			 */
			joinrel->uniquekeys = lappend(joinrel->uniquekeys,
										  adjust_uniquekey_multinull_for_joinrel(root,
																				 lfirst(lc),
																				 joinrel,
																				 false));
		}
		return;
	}

	if (outerrel->uniquekeys == NIL || innerrel->uniquekeys == NIL)
		return;

	switch(jointype)
	{
		case JOIN_INNER:
			outeruk_still_valid = populate_joinrel_uniquekey_for_rel(root, joinrel, outerrel,
																	 innerrel, restrictlist, jointype);
			inneruk_still_valid = populate_joinrel_uniquekey_for_rel(root, joinrel, innerrel,
																	 outerrel, restrictlist, jointype);
			break;

		case JOIN_LEFT:
			/*
			 * For left join, we are sure the innerrel's multi_nulls would be true
			 * and it can't become to multi_nulls=false any more. so just discard it
			 * and only check the outerrel and composited ones.
			 */
			outeruk_still_valid = populate_joinrel_uniquekey_for_rel(root, joinrel, outerrel,
																	 innerrel, restrictlist, jointype);
			break;

		case JOIN_FULL:
			/*
			 * Both sides would contains multi_nulls, don't maintain it
			 * any more.
			 */
			break;

		default:
			elog(ERROR, "unexpected join_type %d", jointype);
	}

	populate_joinrel_composite_uniquekey(root, joinrel,
										 outerrel,
										 innerrel,
										 restrictlist,
										 jointype,
										 outeruk_still_valid,
										 inneruk_still_valid);


	return;
}

/*
 * populate_subquery_uniquekeys
 *
 * 'rel': outer query's RelOptInfo for the subquery relation.
 * 'subquery_uniquekeys': the subquery's output pathkeys, in its terms.
 * 'subquery_tlist': the subquery's output targetlist, in its terms.
 *
 *  subquery issues: a). tlist mapping.  b). interesting uniquekey. c). not nulls.
 */
void
populate_subquery_uniquekeys(PlannerInfo *root, RelOptInfo *rel, RelOptInfo *sub_final_rel)
{
	List	*sub_uniquekeys = sub_final_rel->uniquekeys;
	ListCell	*lc;
	foreach(lc, sub_uniquekeys)
	{
		UniqueKey *sub_ukey = lfirst_node(UniqueKey, lc);
		convert_subquery_uniquekey(root, rel, sub_ukey);
	}
}

/*
 * populate_uniquekeys_from_pathkeys
 *
 */
void
populate_uniquekeys_from_pathkeys(PlannerInfo *root, RelOptInfo *rel, List *pathkeys)
{
	ListCell *lc;
	List	*unique_exprs = NIL;
	if (pathkeys == NIL)
		return;
	foreach(lc, pathkeys)
	{
		PathKey *pathkey = lfirst(lc);
		unique_exprs = lappend(unique_exprs, pathkey->pk_eclass);
	}
	rel->uniquekeys = list_make1(
		make_uniquekey(bms_make_singleton(list_length(root->unique_exprs)),
					   false,
					   true));
	root->unique_exprs = lappend(root->unique_exprs, unique_exprs);
}


void
simple_copy_uniquekeys(RelOptInfo *tarrel, RelOptInfo *srcrel)
{
	tarrel->uniquekeys = srcrel->uniquekeys;
}

/*
 * relation_is_distinct_for
 *		Check if the relation is distinct for.
 */
bool
relation_is_distinct_for(PlannerInfo *root, RelOptInfo *rel, List *distinct_pathkey)
{
	ListCell	*lc;
	List	*lecs = NIL;
	Relids	relids = NULL;
	foreach(lc, distinct_pathkey)
	{
		PathKey *pathkey = lfirst(lc);
		lecs = lappend(lecs, pathkey->pk_eclass);
		/*
		 * Note that ec_relids doesn't include child member, but
		 * distinct would not operate on childrel as well.
		 */
		relids = bms_union(relids, pathkey->pk_eclass->ec_relids);
	}

	foreach(lc, rel->uniquekeys)
	{
		UniqueKey *ukey = lfirst(lc);
		if (ukey->multi_nulls)
			continue;

		if (uniquekey_contains_in(root, ukey, lecs, relids))
			return true;
	}
	return false;
}

/*
 * add_uniquekey_for_uniqueindex
 *	 populate a UniqueKey if necessary, return true iff the UniqueKey is an
 * SingleRow.
 */
static bool
add_uniquekey_for_uniqueindex(PlannerInfo *root, IndexOptInfo *unique_index,
							  List *mergeable_const_peer, List *expr_opfamilies)
{
	List	*unique_exprs = NIL, *unique_ecs = NIL;
	ListCell	*indexpr_item;
	int	c = 0;
	RelOptInfo *rel = unique_index->rel;
	bool	multinull = false;
	bool	used_for_distinct = false;
	Bitmapset *unique_exprs_index;

	indexpr_item = list_head(unique_index->indexprs);
	/* Gather all the non-const exprs */
	for (c = 0; c < unique_index->nkeycolumns; c++)
	{
		int attr = unique_index->indexkeys[c];
		Expr *expr;
		bool	matched_const = false;
		ListCell	*lc1, *lc2;
		if (attr > 0)
		{
			Var *var;
			expr = list_nth_node(TargetEntry, unique_index->indextlist, c)->expr;
			var = castNode(Var, expr);
			Assert(IsA(expr, Var));
			if (!bms_is_member(var->varattno - FirstLowInvalidHeapAttributeNumber,
							  rel->notnull_attrs[0]))
				multinull = true;
		}
		else if (attr == 0)
		{
			/* Expression index */
			expr = lfirst(indexpr_item);
			indexpr_item = lnext(unique_index->indexprs, indexpr_item);
			/* We can't grantee an FuncExpr will not return NULLs */
			multinull = true;
		}
		else /* attr < 0 */
		{
			/* Index on OID is possible, not handle it for now. */
			return false;
		}

		/*
		 * Check index_col = Const case with regarding to opfamily checking
		 * If so, we can remove the index_col from the final UniqueKey->exprs.
		 */
		forboth(lc1, mergeable_const_peer, lc2, expr_opfamilies)
		{
			if (list_member_oid((List *) lfirst(lc2), unique_index->opfamily[c]) &&
				match_index_to_operand((Node *) lfirst(lc1), c, unique_index))
			{
				matched_const = true;
				break;
			}
		}

		if (matched_const)
			continue;

		unique_exprs = lappend(unique_exprs, expr);
	}

	if (unique_exprs == NIL)
	{
		/*
		 * SingleRow case. Checking if it is useful is ineffective
		 * so just keep it.
		 */
		mark_rel_singlerow(root, rel);
		return true;
	}

	unique_ecs = build_equivalanceclass_list_for_exprs(root, unique_exprs, rel);

	if (unique_ecs == NIL)
	{
		/* It is neither used in distinct_pathkey nor mergeable clause */
		return false;
	}

	/*
	 * Check if we need to setup the UniqueKey and set the used_for_distinct accordingly.
	 */
	if (unique_ecs_useful_for_distinct(root, unique_ecs))
	{
		used_for_distinct = true;
	}
	else if (!unique_ecs_useful_for_merging(root, rel, unique_ecs))
		/*
		 * Neither used in distinct pathkey nor used in mergeable clause.
		 * this is possible even if unique_ecs != NIL.
		 */
		return false;
	else
	{
		/*
		 * unique_ecs_useful_for_merging(root, rel, unique_ecs) is true,
		 * we did nothing in this case.
		 */
	}
	unique_exprs_index = bms_make_singleton(list_length(root->unique_exprs));
	root->unique_exprs = lappend(root->unique_exprs, unique_ecs);
	rel->uniquekeys = lappend(rel->uniquekeys,
							  make_uniquekey(unique_exprs_index,
											 multinull,
											 used_for_distinct));
	return false;
}

/*
 * is_uniquekey_nulls_removed
 *
 *	note this function will not consider the OUTER JOIN impacts. Caller should
 * take care of it.
 *	-- Use my way temporary (RelOptInfo.notnull_attrs) until Tom's is ready.
 */
static bool
is_uniquekey_nulls_removed(PlannerInfo *root,
						   UniqueKey *ukey,
						   RelOptInfo *joinrel)
{
	int i = -1;

	while((i = bms_next_member(ukey->unique_expr_indexes, i)) >= 0)
	{
		Node *node = list_nth(root->unique_exprs, i);
		List	*ecs;
		ListCell	*lc;
		if (IsA(node, SingleRow))
			continue;
		ecs = castNode(List, node);
		foreach(lc, ecs)
		{
			EquivalenceClass *ec = lfirst_node(EquivalenceClass, lc);
			ListCell *emc;
			foreach(emc, ec->ec_members)
			{
				EquivalenceMember *em = lfirst_node(EquivalenceMember, emc);
				int relid;
				Var *var;
				Bitmapset *notnull_attrs;
				if (!bms_is_subset(em->em_relids, joinrel->relids))
					continue;

				if (!bms_get_singleton_member(em->em_relids, &relid))
					continue;

				if (!IsA(em->em_expr, Var))
					continue;

				var = castNode(Var, em->em_expr);

				if (relid != var->varno)
					continue;

				notnull_attrs = joinrel->notnull_attrs[var->varno];

				if (!bms_is_member(var->varattno - FirstLowInvalidHeapAttributeNumber,
								   notnull_attrs))
					return false;
				else
					break; /* Break to check next ECs */
			}
		}
	}
	return true;
}

/*
 * adjust_uniquekey_multinull_for_joinrel
 *
 *	After the join, some NULL values can be removed due to join-clauses.
 * but the outer join can generated null values again. Return the final
 * state of the UniqueKey on joinrel.
 */
static UniqueKey *
adjust_uniquekey_multinull_for_joinrel(PlannerInfo *root,
									   UniqueKey *ukey,
									   RelOptInfo *joinrel,
									   bool below_outer_side)
{
	if (below_outer_side)
	{
		if (ukey->multi_nulls)
			/* we need it to be multi_nulls, but it is already, just return it. */
			return ukey;
		else
			/* we need it to be multi_nulls, but it is not, create a new one. */
			return make_uniquekey(ukey->unique_expr_indexes,
								  true,
								  ukey->use_for_distinct);
	}
	else
	{
		/*
		 * We need to check if the join clauses can remove the NULL values. However
		 * if it doesn't contain NULL values at the first, we don't need to check it.
		 */
		if (!ukey->multi_nulls)
			return ukey;
		else
		{
			/*
			 * Multi null values exists. It's time to check if the nulls values
			 * are removed via outer join.
			 */
			if (!is_uniquekey_nulls_removed(root, ukey, joinrel))
				/* null values can be removed, return the original one. */
				return ukey;
			else
				return make_uniquekey(ukey->unique_expr_indexes,
									  false, ukey->use_for_distinct);
		}
	}
}

/*
 * populate_joinrel_uniquekey_for_rel
 *
 *    Check if rel.any_column = other_rel.unique_key_columns.
 * The return value is if the rel->uniquekeys still valid. If
 * yes, added the uniquekeys in rel to joinrel and return true.
 * otherwise, return false.
 */
static bool
populate_joinrel_uniquekey_for_rel(PlannerInfo *root, RelOptInfo *joinrel,
								   RelOptInfo *rel, RelOptInfo *other_rel,
								   List *restrictlist, JoinType type)
{
	bool	rel_keep_unique = false;
	List *other_ecs = NIL;
	Relids	other_relids = NULL;
	ListCell	*lc;

	/*
	 * Gather all the other ECs regarding to rel, if all the unique ecs contains
	 * in this list, then it hits our expectations.
	 */
	foreach(lc, restrictlist)
	{
		RestrictInfo *r = lfirst_node(RestrictInfo, lc);

		if (r->mergeopfamilies == NIL)
			continue;

		if (bms_equal(r->left_relids, rel->relids) && r->right_ec != NULL)
		{
			other_ecs = lappend(other_ecs, r->right_ec);
			other_relids = bms_add_members(other_relids, r->right_relids);
		}
		else if (bms_equal(r->right_relids, rel->relids) && r->left_ec != NULL)
		{
			other_ecs = lappend(other_ecs, r->right_ec);
			other_relids = bms_add_members(other_relids, r->left_relids);
		}
	}

	foreach(lc, other_rel->uniquekeys)
	{
		UniqueKey *ukey = lfirst_node(UniqueKey, lc);
		if (uniquekey_contains_in(root, ukey, other_ecs, other_relids))
		{
			rel_keep_unique = true;
			break;
		}
	}

	if (!rel_keep_unique)
		return false;

	foreach(lc, rel->uniquekeys)
	{

		UniqueKey *ukey = lfirst_node(UniqueKey, lc);

		if (is_uniquekey_useful_afterjoin(root, ukey, joinrel))
		{
			ukey = adjust_uniquekey_multinull_for_joinrel(root,
														  ukey,
														  joinrel,
														  false /* outer_side, caller grantees this */);
			joinrel->uniquekeys = lappend(joinrel->uniquekeys, ukey);
		}
	}

	return true;
}


/*
 * Populate_joinrel_composited_uniquekey
 *
 *	A composited unqiuekey is valid no matter with join type and restrictlist.
 */
static void
populate_joinrel_composite_uniquekey(PlannerInfo *root,
									 RelOptInfo *joinrel,
									 RelOptInfo *outerrel,
									 RelOptInfo *innerrel,
									 List	*restrictlist,
									 JoinType jointype,
									 bool left_added,
									 bool right_added)
{
	ListCell	*lc;
	if (left_added || right_added)
		/* No need to create the composited ones */
		return;

	foreach(lc, outerrel->uniquekeys)
	{
		UniqueKey	*outer_ukey = adjust_uniquekey_multinull_for_joinrel(root,
																		 lfirst(lc),
																		 joinrel,
																		 jointype == JOIN_FULL);
		ListCell	*lc2;

		if (!is_uniquekey_useful_afterjoin(root, outer_ukey, joinrel))
			continue;

		foreach(lc2, innerrel->uniquekeys)
		{
			UniqueKey	*inner_ukey = adjust_uniquekey_multinull_for_joinrel(root,
																			 lfirst(lc2),
																			 joinrel,
																			 (jointype == JOIN_FULL || jointype == JOIN_LEFT)
				);

			UniqueKey	*comp_ukey;

			if (!is_uniquekey_useful_afterjoin(root, inner_ukey, joinrel))
				continue;

			comp_ukey = make_uniquekey(
				/* unique_expr_indexes is easy, just union the both sides. */
				bms_union(outer_ukey->unique_expr_indexes, inner_ukey->unique_expr_indexes),
				/*
				 * If both are !multi_nulls, then the composited one is !multi_null
				 * no matter with jointype and join clauses. otherwise, it is multi
				 * nulls no matter with other factors.
				 *
				 */
				outer_ukey->multi_nulls || inner_ukey->multi_nulls,
				/*
				 * we need both sides are used in distinct to say the composited
				 * one is used for distinct as well.
				 */
				outer_ukey->use_for_distinct && inner_ukey->use_for_distinct);

			joinrel->uniquekeys = lappend(joinrel->uniquekeys, comp_ukey);
		}
	}
}


/*
 * uniquekey_contains_in
 *	Return if UniqueKey contains in the list of EquivalenceClass
 * or the UniqueKey's SingleRow contains in relids.
 *
 */
static bool
uniquekey_contains_in(PlannerInfo *root, UniqueKey *ukey, List *ecs, Relids relids)
{
	int i = -1;
	while ((i = bms_next_member(ukey->unique_expr_indexes, i)) >= 0)
	{
		Node *exprs = list_nth(root->unique_exprs, i);
		if (IsA(exprs, SingleRow))
		{
			SingleRow *singlerow = castNode(SingleRow, exprs);
			if (!bms_is_member(singlerow->relid, relids))
				/*
				 * UniqueKey request a ANY expr on relid on the relid(which
				 * indicates we don't have other EquivalenceClass for this
				 * relation), but the relid doesn't contains in relids, which
				 * indicate there is no such Expr in target, then we are sure
				 * to return false.
				 */
				return false;
			else
			{
				/*
				 * We have SingleRow on relid, and the relid is in relids.
				 * We don't need to check any more for this expr. This is
				 * right for sure.
				 */
			}
		}
		else
		{
			Assert(IsA(exprs, List));
			if (!list_is_subset_ptr((List *)exprs, ecs))
				return false;
		}
	}
	return true;
}

/*
 * unique_ecs_useful_for_distinct
 *	return true if all the EquivalenceClass in ecs exists in root->distinct_pathkey.
 */
static bool
unique_ecs_useful_for_distinct(PlannerInfo *root, List *ecs)
{
	ListCell *lc;
	foreach(lc, ecs)
	{
		EquivalenceClass *ec = lfirst_node(EquivalenceClass, lc);
		ListCell *p;
		bool found = false;
		foreach(p,  root->distinct_pathkeys)
		{
			PathKey *pathkey = lfirst_node(PathKey, p);
			/*
			 * Both of them should point to an element in root->eq_classes.
			 * so the address should be same. and equal function doesn't
			 * support EquivalenceClass yet.
			 */
			if (ec == pathkey->pk_eclass)
			{
				found = true;
				break;
			}
		}
		if (!found)
			return false;
	}
	return true;
}

/*
 * unique_ecs_useful_for_merging
 *	return true if all the unique_ecs exists in rel's join restrictInfo.
 */
static bool
unique_ecs_useful_for_merging(PlannerInfo *root, RelOptInfo *rel, List *unique_ecs)
{
	ListCell	*lc;

	foreach(lc, unique_ecs)
	{
		EquivalenceClass *ec = lfirst(lc);
		if (!ec_useful_for_merging(root, rel, ec))
			return false;
	}

	return true;
}

/*
 * is_uniquekey_useful_afterjoin
 *
 *  is useful when it contains in distinct_pathkey or in mergable join clauses.
 */
static bool
is_uniquekey_useful_afterjoin(PlannerInfo *root, UniqueKey *ukey,
							 RelOptInfo *joinrel)
{
	int	i = -1;

	if (ukey->use_for_distinct)
		return true;

	while((i = bms_next_member(ukey->unique_expr_indexes, i)) >= 0)
	{
		Node *exprs =  list_nth(root->unique_exprs, i);
		if (IsA(exprs, List))
		{
			if (!unique_ecs_useful_for_merging(root, joinrel, (List *)exprs))
				return false;
		}
		else
		{
			Assert(IsA(exprs, SingleRow));
			/*
			 * Ideally we should check if there are a expr on SingleRow
			 * used in joinrel's joinclauses, but it can't be checked effectively
			 * for now, so we just check the rest part. so just think
			 * it is useful.
			 */
		}
	}
	return true;
}

/*
 * find_outer_ec_with_subquery_em
 *
 *	Given a em in subquery, return the related EquivalenceClass outside.
 */
static EquivalenceClass *
find_outer_ec_with_subquery_em(PlannerInfo *root, RelOptInfo *rel,
							   EquivalenceClass *sub_ec, EquivalenceMember *sub_em)
{
	TargetEntry *sub_tle;
	Var *outer_var;
	EquivalenceClass *outer_ec;

	sub_tle = get_tle_from_expr(sub_em->em_expr, rel->subroot->processed_tlist);

	if (!sub_tle)
		return NULL;

	outer_var = find_var_for_subquery_tle(rel, sub_tle);
	if (!outer_var)
		return NULL;

	outer_ec = get_eclass_for_sort_expr(root,
										(Expr *)outer_var,
										NULL,
										sub_ec->ec_opfamilies,
										sub_em->em_datatype,
										sub_ec->ec_collation,
										0,
										rel->relids,
										false);
	return outer_ec;
}


/*
 * convert_subquery_eclass_list
 *
 *		Given a list of eclass in subquery, find the corresponding eclass in outer side.
 * return NULL if no related eclass outside is found for any eclass in subquery.
 */
static List *
convert_subquery_eclass_list(PlannerInfo *root, RelOptInfo *rel, List *sub_eclass_list)
{
	ListCell	*lc;
	List	*ec_list = NIL;
	foreach(lc, sub_eclass_list)
	{
		EquivalenceClass *sub_ec = lfirst_node(EquivalenceClass, lc);
		EquivalenceClass *ec = NULL;
		ListCell	*emc;
		foreach(emc, sub_ec->ec_members)
		{
			EquivalenceMember *sub_em = lfirst(emc);
			if ((ec = find_outer_ec_with_subquery_em(root, rel, sub_ec, sub_em)) != NULL)
				break;
		}
		if (!ec)
			return NIL;
		ec_list = lappend(ec_list, ec);
	}
	return ec_list;
}


/*
 * convert_subquery_uniquekey
 *
 */
static void
convert_subquery_uniquekey(PlannerInfo *root, RelOptInfo *rel, UniqueKey *sub_ukey)
{
	PlannerInfo *sub_root = rel->subroot;
	List	*unique_exprs_list = NIL;
	Bitmapset	*unique_exprs_indexes = NULL;
	UniqueKey	*ukey = NULL;
	int i = -1;
	ListCell	*lc;
	while((i = bms_next_member(sub_ukey->unique_expr_indexes, i)) >= 0)
	{
		Node *sub_eq_list = list_nth(sub_root->unique_exprs, i);
		if (IsA(sub_eq_list, SingleRow))
		{
			/*
			 * TODO: Unclear what to do, don't think it hard before the overall
			 * design is accepted.
			 */
			return;
		}
		else
		{
			List *upper_eq_list;
			Assert(IsA(sub_eq_list, List));
			/*
			 * Note: upper_eq_list is just part of uniquekey's exprs, to covert the whole
			 * UniqueKey, we needs all the parts are shown in the upper rel.
			 */
			upper_eq_list = convert_subquery_eclass_list(root, rel, (List *)sub_eq_list);
			if (upper_eq_list == NIL)
			{
				if (unique_exprs_list != NIL)
					pfree(unique_exprs_list);
				return;
			}
			unique_exprs_list = lappend(unique_exprs_list, upper_eq_list);
		}
	}

	foreach(lc, unique_exprs_list)
	{
		unique_exprs_indexes = bms_add_member(unique_exprs_indexes, list_length(root->unique_exprs));
		root->unique_exprs = lappend(root->unique_exprs, lfirst(lc));
	}

	ukey = make_uniquekey(unique_exprs_indexes,
						  sub_ukey->multi_nulls,
						  /* TODO: need check again, case SELECT * FROM (SELECT u FROM x OFFSET 0) v where x.u = 0; */
						  true);
	rel->uniquekeys = lappend(rel->uniquekeys, ukey);
}

/*
 *	make_uniquekey
 */
static UniqueKey *
make_uniquekey(Bitmapset *unique_expr_indexes, bool multi_null, bool useful_for_distinct)
{
	UniqueKey *ukey = makeNode(UniqueKey);
	ukey->unique_expr_indexes = unique_expr_indexes;
	ukey->multi_nulls = multi_null;
	ukey->use_for_distinct = useful_for_distinct;
	return ukey;
}

/*
 * mark_rel_singlerow
 *	mark a relation as singlerow.
 */
static void
mark_rel_singlerow(PlannerInfo *root, RelOptInfo *rel)
{
	int exprs_pos = list_length(root->unique_exprs);
	Bitmapset *unique_exprs_index = bms_make_singleton(exprs_pos);
	SingleRow *singlerow = makeNode(SingleRow);
	singlerow->relid = rel->relid;
	root->unique_exprs = lappend(root->unique_exprs, singlerow);
	rel->uniquekeys = list_make1(make_uniquekey(unique_exprs_index,
												false /* multi-null */,
												true /* arbitrary decision */));
}

/*
 * print_uniquekey
 *	Used for easier reivew, should be removed before commit.
 */
static void
print_uniquekey(PlannerInfo *root, RelOptInfo *rel)
{
	if (false)
	{
		ListCell	*lc;
		elog(INFO, "Rel = %s", bmsToString(rel->relids));
		foreach(lc, rel->uniquekeys)
		{
			UniqueKey *ukey = lfirst_node(UniqueKey, lc);
			int i = -1;
			elog(INFO, "UNIQUEKEY{indexes=%s, multinull=%d}",
				 bmsToString(ukey->unique_expr_indexes),
				 ukey->multi_nulls
				);

			while ((i = bms_next_member(ukey->unique_expr_indexes, i)) >= 0)
			{
				Node *node = (Node *) list_nth(root->unique_exprs, i);
				if (IsA(node, SingleRow))
					elog(INFO,
						 "Expr(%d) SingleRow{relid = %d}",
						 i, castNode(SingleRow, node)->relid);
				else
					elog(INFO,
						 "EC(%d), %s", i, nodeToString(node)
						);
			}
		}
	}
}
