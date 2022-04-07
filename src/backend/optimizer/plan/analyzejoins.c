/*-------------------------------------------------------------------------
 *
 * analyzejoins.c
 *	  Routines for simplifying joins after initial query analysis
 *
 * While we do a great deal of join simplification in prep/prepjointree.c,
 * certain optimizations cannot be performed at that stage for lack of
 * detailed information about the query.  The routines here are invoked
 * after initsplan.c has done its work, and can do additional join removal
 * and simplification steps based on the information extracted.  The penalty
 * is that we have to work harder to clean up after ourselves when we modify
 * the query, since the derived data structures have to be updated too.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/plan/analyzejoins.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_class.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/joininfo.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"
#include "utils/lsyscache.h"

bool enable_self_join_removal;

/* local functions */
static bool join_is_removable(PlannerInfo *root, SpecialJoinInfo *sjinfo);
static void remove_rel_from_query(PlannerInfo *root, int relid,
								  Relids joinrelids, int subst_relid);
static List *remove_rel_from_joinlist(List *joinlist, int relid, int *nremoved);
static bool rel_supports_distinctness(PlannerInfo *root, RelOptInfo *rel);
static bool rel_is_distinct_for(PlannerInfo *root, RelOptInfo *rel,
								List *clause_list);
static Oid	distinct_col_search(int colno, List *colnos, List *opids);
static bool is_innerrel_unique_for(PlannerInfo *root,
								   Relids joinrelids,
								   Relids outerrelids,
								   RelOptInfo *innerrel,
								   JoinType jointype,
								   List *restrictlist);
static void change_rinfo(RestrictInfo* rinfo, Index from, Index to);
static Bitmapset* change_relid(Relids relids, Index oldId, Index newId);
static void change_varno(Expr *expr, Index oldRelid, Index newRelid);


/*
 * remove_useless_joins
 *		Check for relations that don't actually need to be joined at all,
 *		and remove them from the query.
 *
 * We are passed the current joinlist and return the updated list.  Other
 * data structures that have to be updated are accessible via "root".
 */
List *
remove_useless_joins(PlannerInfo *root, List *joinlist)
{
	ListCell   *lc;

	/*
	 * We are only interested in relations that are left-joined to, so we can
	 * scan the join_info_list to find them easily.
	 */
restart:
	foreach(lc, root->join_info_list)
	{
		SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) lfirst(lc);
		int			innerrelid;
		int			nremoved;

		/* Skip if not removable */
		if (!join_is_removable(root, sjinfo))
			continue;

		/*
		 * Currently, join_is_removable can only succeed when the sjinfo's
		 * righthand is a single baserel.  Remove that rel from the query and
		 * joinlist.
		 */
		innerrelid = bms_singleton_member(sjinfo->min_righthand);

		remove_rel_from_query(root, innerrelid,
							  bms_union(sjinfo->min_lefthand,
										sjinfo->min_righthand), 0);

		/* We verify that exactly one reference gets removed from joinlist */
		nremoved = 0;
		joinlist = remove_rel_from_joinlist(joinlist, innerrelid, &nremoved);
		if (nremoved != 1)
			elog(ERROR, "failed to find relation %d in joinlist", innerrelid);

		/*
		 * We can delete this SpecialJoinInfo from the list too, since it's no
		 * longer of interest.  (Since we'll restart the foreach loop
		 * immediately, we don't bother with foreach_delete_current.)
		 */
		root->join_info_list = list_delete_cell(root->join_info_list, lc);

		/*
		 * Restart the scan.  This is necessary to ensure we find all
		 * removable joins independently of ordering of the join_info_list
		 * (note that removal of attr_needed bits may make a join appear
		 * removable that did not before).
		 */
		goto restart;
	}

	return joinlist;
}

/*
 * clause_sides_match_join
 *	  Determine whether a join clause is of the right form to use in this join.
 *
 * We already know that the clause is a binary opclause referencing only the
 * rels in the current join.  The point here is to check whether it has the
 * form "outerrel_expr op innerrel_expr" or "innerrel_expr op outerrel_expr",
 * rather than mixing outer and inner vars on either side.  If it matches,
 * we set the transient flag outer_is_left to identify which side is which.
 */
static inline bool
clause_sides_match_join(RestrictInfo *rinfo, Relids outerrelids,
						Relids innerrelids)
{
	if (bms_is_subset(rinfo->left_relids, outerrelids) &&
		bms_is_subset(rinfo->right_relids, innerrelids))
	{
		/* lefthand side is outer */
		rinfo->outer_is_left = true;
		return true;
	}
	else if (bms_is_subset(rinfo->left_relids, innerrelids) &&
			 bms_is_subset(rinfo->right_relids, outerrelids))
	{
		/* righthand side is outer */
		rinfo->outer_is_left = false;
		return true;
	}
	return false;				/* no good for these input relations */
}

/*
 * join_is_removable
 *	  Check whether we need not perform this special join at all, because
 *	  it will just duplicate its left input.
 *
 * This is true for a left join for which the join condition cannot match
 * more than one inner-side row.  (There are other possibly interesting
 * cases, but we don't have the infrastructure to prove them.)  We also
 * have to check that the inner side doesn't generate any variables needed
 * above the join.
 */
static bool
join_is_removable(PlannerInfo *root, SpecialJoinInfo *sjinfo)
{
	int			innerrelid;
	RelOptInfo *innerrel;
	Relids		joinrelids;
	List	   *clause_list = NIL;
	ListCell   *l;
	int			attroff;

	/*
	 * Must be a non-delaying left join to a single baserel, else we aren't
	 * going to be able to do anything with it.
	 */
	if (sjinfo->jointype != JOIN_LEFT ||
		sjinfo->delay_upper_joins)
		return false;

	if (!bms_get_singleton_member(sjinfo->min_righthand, &innerrelid))
		return false;

	innerrel = find_base_rel(root, innerrelid);

	/*
	 * Before we go to the effort of checking whether any innerrel variables
	 * are needed above the join, make a quick check to eliminate cases in
	 * which we will surely be unable to prove uniqueness of the innerrel.
	 */
	if (!rel_supports_distinctness(root, innerrel))
		return false;

	/* Compute the relid set for the join we are considering */
	joinrelids = bms_union(sjinfo->min_lefthand, sjinfo->min_righthand);

	/*
	 * We can't remove the join if any inner-rel attributes are used above the
	 * join.
	 *
	 * Note that this test only detects use of inner-rel attributes in higher
	 * join conditions and the target list.  There might be such attributes in
	 * pushed-down conditions at this join, too.  We check that case below.
	 *
	 * As a micro-optimization, it seems better to start with max_attr and
	 * count down rather than starting with min_attr and counting up, on the
	 * theory that the system attributes are somewhat less likely to be wanted
	 * and should be tested last.
	 */
	for (attroff = innerrel->max_attr - innerrel->min_attr;
		 attroff >= 0;
		 attroff--)
	{
		if (!bms_is_subset(innerrel->attr_needed[attroff], joinrelids))
			return false;
	}

	/*
	 * Similarly check that the inner rel isn't needed by any PlaceHolderVars
	 * that will be used above the join.  We only need to fail if such a PHV
	 * actually references some inner-rel attributes; but the correct check
	 * for that is relatively expensive, so we first check against ph_eval_at,
	 * which must mention the inner rel if the PHV uses any inner-rel attrs as
	 * non-lateral references.  Note that if the PHV's syntactic scope is just
	 * the inner rel, we can't drop the rel even if the PHV is variable-free.
	 */
	foreach(l, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(l);

		if (bms_overlap(phinfo->ph_lateral, innerrel->relids))
			return false;		/* it references innerrel laterally */
		if (bms_is_subset(phinfo->ph_needed, joinrelids))
			continue;			/* PHV is not used above the join */
		if (!bms_overlap(phinfo->ph_eval_at, innerrel->relids))
			continue;			/* it definitely doesn't reference innerrel */
		if (bms_is_subset(phinfo->ph_eval_at, innerrel->relids))
			return false;		/* there isn't any other place to eval PHV */
		if (bms_overlap(pull_varnos(root, (Node *) phinfo->ph_var->phexpr),
						innerrel->relids))
			return false;		/* it does reference innerrel */
	}

	/*
	 * Search for mergejoinable clauses that constrain the inner rel against
	 * either the outer rel or a pseudoconstant.  If an operator is
	 * mergejoinable then it behaves like equality for some btree opclass, so
	 * it's what we want.  The mergejoinability test also eliminates clauses
	 * containing volatile functions, which we couldn't depend on.
	 */
	foreach(l, innerrel->joininfo)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(l);

		/*
		 * If it's not a join clause for this outer join, we can't use it.
		 * Note that if the clause is pushed-down, then it is logically from
		 * above the outer join, even if it references no other rels (it might
		 * be from WHERE, for example).
		 */
		if (RINFO_IS_PUSHED_DOWN(restrictinfo, joinrelids))
		{
			/*
			 * If such a clause actually references the inner rel then join
			 * removal has to be disallowed.  We have to check this despite
			 * the previous attr_needed checks because of the possibility of
			 * pushed-down clauses referencing the rel.
			 */
			if (bms_is_member(innerrelid, restrictinfo->clause_relids))
				return false;
			continue;			/* else, ignore; not useful here */
		}

		/* Ignore if it's not a mergejoinable clause */
		if (!restrictinfo->can_join ||
			restrictinfo->mergeopfamilies == NIL)
			continue;			/* not mergejoinable */

		/*
		 * Check if clause has the form "outer op inner" or "inner op outer",
		 * and if so mark which side is inner.
		 */
		if (!clause_sides_match_join(restrictinfo, sjinfo->min_lefthand,
									 innerrel->relids))
			continue;			/* no good for these input relations */

		/* OK, add to list */
		clause_list = lappend(clause_list, restrictinfo);
	}

	/*
	 * Now that we have the relevant equality join clauses, try to prove the
	 * innerrel distinct.
	 */
	if (rel_is_distinct_for(root, innerrel, clause_list))
		return true;

	/*
	 * Some day it would be nice to check for other methods of establishing
	 * distinctness.
	 */
	return false;
}


/*
 * Remove the target relid from the planner's data structures, having
 * determined that there is no need to include it in the query. Or replace
 * with another relid.
 * To reusability, this routine can work in two modes: delete relid from a plan
 * or replace it. It is used in replace mode in a self-join removing process.
 *
 * We are not terribly thorough here.  We must make sure that the rel is
 * no longer treated as a baserel, and that attributes of other baserels
 * are no longer marked as being needed at joins involving this rel.
 * Also, join quals involving the rel have to be removed from the joininfo
 * lists, but only if they belong to the outer join identified by joinrelids.
 */
static void
remove_rel_from_query(PlannerInfo *root, int relid, Relids joinrelids,
					  int subst_relid)
{
	RelOptInfo *rel = find_base_rel(root, relid);
	List	   *joininfos;
	Index		rti;
	ListCell   *l;

	Assert(subst_relid == 0 || relid != subst_relid);

	/*
	 * Mark the rel as "dead" to show it is no longer part of the join tree.
	 * (Removing it from the baserel array altogether seems too risky.)
	 */
	rel->reloptkind = RELOPT_DEADREL;

	/*
	 * Remove references to the rel from other baserels' attr_needed arrays.
	 */
	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *otherrel = root->simple_rel_array[rti];
		int			attroff;

		/* there may be empty slots corresponding to non-baserel RTEs */
		if (otherrel == NULL)
			continue;

		Assert(otherrel->relid == rti); /* sanity check on array */

		/* no point in processing target rel itself */
		if (otherrel == rel)
			continue;

		for (attroff = otherrel->max_attr - otherrel->min_attr;
			 attroff >= 0;
			 attroff--)
		{
			otherrel->attr_needed[attroff] =
				change_relid(otherrel->attr_needed[attroff], relid, subst_relid);
		}

		/* Update lateral references. */
		change_varno((Expr*)otherrel->lateral_vars, relid, subst_relid);
	}

	/*
	 * Likewise remove references from SpecialJoinInfo data structures.
	 *
	 * This is relevant in case the outer join we're deleting is nested inside
	 * other outer joins: the upper joins' relid sets have to be adjusted. The
	 * RHS of the target outer join will be made empty here, but that's OK
	 * since caller will delete that SpecialJoinInfo entirely.
	 */
	foreach(l, root->join_info_list)
	{
		SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) lfirst(l);

		sjinfo->min_lefthand = change_relid(sjinfo->min_lefthand, relid, subst_relid);
		sjinfo->min_righthand = change_relid(sjinfo->min_righthand, relid, subst_relid);
		sjinfo->syn_lefthand = change_relid(sjinfo->syn_lefthand, relid, subst_relid);
		sjinfo->syn_righthand = change_relid(sjinfo->syn_righthand, relid, subst_relid);

		change_varno((Expr*)sjinfo->semi_rhs_exprs, relid, subst_relid);
	}

	/*
	 * Likewise remove references from PlaceHolderVar data structures,
	 * removing any no-longer-needed placeholders entirely.
	 *
	 * Removal is a bit trickier than it might seem: we can remove PHVs that
	 * are used at the target rel and/or in the join qual, but not those that
	 * are used at join partner rels or above the join.  It's not that easy to
	 * distinguish PHVs used at partner rels from those used in the join qual,
	 * since they will both have ph_needed sets that are subsets of
	 * joinrelids.  However, a PHV used at a partner rel could not have the
	 * target rel in ph_eval_at, so we check that while deciding whether to
	 * remove or just update the PHV.  There is no corresponding test in
	 * join_is_removable because it doesn't need to distinguish those cases.
	 */
	foreach(l, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(l);

		if (subst_relid == 0 && bms_is_subset(phinfo->ph_needed, joinrelids) &&
			bms_is_member(relid, phinfo->ph_eval_at))
			root->placeholder_list = foreach_delete_current(root->placeholder_list,
															l);
		else
		{
			phinfo->ph_eval_at = change_relid(phinfo->ph_eval_at, relid, subst_relid);
			Assert(!bms_is_empty(phinfo->ph_eval_at));
			phinfo->ph_needed = change_relid(phinfo->ph_needed, relid, subst_relid);
			Assert(subst_relid != 0 || !bms_is_member(relid, phinfo->ph_lateral));
			phinfo->ph_lateral = change_relid(phinfo->ph_lateral, relid, subst_relid);
			phinfo->ph_var->phrels = change_relid(phinfo->ph_var->phrels, relid, subst_relid);
		}
	}

	if (subst_relid != 0)
	{
		foreach(l, rel->baserestrictinfo)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);

			change_rinfo(rinfo, relid, subst_relid);
			distribute_restrictinfo_to_rels(root, rinfo);
		}
	}

	/*
	 * Remove any joinquals referencing the rel from the joininfo lists.
	 *
	 * In some cases, a joinqual has to be put back after deleting its
	 * reference to the target rel.  This can occur for pseudoconstant and
	 * outerjoin-delayed quals, which can get marked as requiring the rel in
	 * order to force them to be evaluated at or above the join.  We can't
	 * just discard them, though.  Only quals that logically belonged to the
	 * outer join being discarded should be removed from the query.
	 *
	 * We must make a copy of the rel's old joininfo list before starting the
	 * loop, because otherwise remove_join_clause_from_rels would destroy the
	 * list while we're scanning it.
	 */
	joininfos = list_copy(rel->joininfo);
	foreach(l, joininfos)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);

		remove_join_clause_from_rels(root, rinfo, rinfo->required_relids);
		change_rinfo(rinfo, relid, subst_relid);

		if (RINFO_IS_PUSHED_DOWN(rinfo, joinrelids))
		{
			/* Recheck that qual doesn't actually reference the target rel */
			Assert(!bms_is_member(relid, rinfo->clause_relids));

			/*
			 * The required_relids probably aren't shared with anything else,
			 * but let's copy them just to be sure.
			 */
			rinfo->required_relids = bms_copy(rinfo->required_relids);
			rinfo->required_relids = bms_del_member(rinfo->required_relids,
													relid);
			distribute_restrictinfo_to_rels(root, rinfo);
		}
		else if (subst_relid != 0)
			distribute_restrictinfo_to_rels(root, rinfo);
	}

	/*
	 * There may be references to the rel in root->fkey_list, but if so,
	 * match_foreign_keys_to_quals() will get rid of them.
	 */
}

/*
 * Remove any occurrences of the target relid from a joinlist structure.
 *
 * It's easiest to build a whole new list structure, so we handle it that
 * way.  Efficiency is not a big deal here.
 *
 * *nremoved is incremented by the number of occurrences removed (there
 * should be exactly one, but the caller checks that).
 */
static List *
remove_rel_from_joinlist(List *joinlist, int relid, int *nremoved)
{
	List	   *result = NIL;
	ListCell   *jl;

	foreach(jl, joinlist)
	{
		Node	   *jlnode = (Node *) lfirst(jl);

		if (IsA(jlnode, RangeTblRef))
		{
			int			varno = ((RangeTblRef *) jlnode)->rtindex;

			if (varno == relid)
				(*nremoved)++;
			else
				result = lappend(result, jlnode);
		}
		else if (IsA(jlnode, List))
		{
			/* Recurse to handle subproblem */
			List	   *sublist;

			sublist = remove_rel_from_joinlist((List *) jlnode,
											   relid, nremoved);
			/* Avoid including empty sub-lists in the result */
			if (sublist)
				result = lappend(result, sublist);
		}
		else
		{
			elog(ERROR, "unrecognized joinlist node type: %d",
				 (int) nodeTag(jlnode));
		}
	}

	return result;
}


/*
 * reduce_unique_semijoins
 *		Check for semijoins that can be simplified to plain inner joins
 *		because the inner relation is provably unique for the join clauses.
 *
 * Ideally this would happen during reduce_outer_joins, but we don't have
 * enough information at that point.
 *
 * To perform the strength reduction when applicable, we need only delete
 * the semijoin's SpecialJoinInfo from root->join_info_list.  (We don't
 * bother fixing the join type attributed to it in the query jointree,
 * since that won't be consulted again.)
 */
void
reduce_unique_semijoins(PlannerInfo *root)
{
	ListCell   *lc;

	/*
	 * Scan the join_info_list to find semijoins.
	 */
	foreach(lc, root->join_info_list)
	{
		SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) lfirst(lc);
		int			innerrelid;
		RelOptInfo *innerrel;
		Relids		joinrelids;
		List	   *restrictlist;

		/*
		 * Must be a non-delaying semijoin to a single baserel, else we aren't
		 * going to be able to do anything with it.  (It's probably not
		 * possible for delay_upper_joins to be set on a semijoin, but we
		 * might as well check.)
		 */
		if (sjinfo->jointype != JOIN_SEMI ||
			sjinfo->delay_upper_joins)
			continue;

		if (!bms_get_singleton_member(sjinfo->min_righthand, &innerrelid))
			continue;

		innerrel = find_base_rel(root, innerrelid);

		/*
		 * Before we trouble to run generate_join_implied_equalities, make a
		 * quick check to eliminate cases in which we will surely be unable to
		 * prove uniqueness of the innerrel.
		 */
		if (!rel_supports_distinctness(root, innerrel))
			continue;

		/* Compute the relid set for the join we are considering */
		joinrelids = bms_union(sjinfo->min_lefthand, sjinfo->min_righthand);

		/*
		 * Since we're only considering a single-rel RHS, any join clauses it
		 * has must be clauses linking it to the semijoin's min_lefthand.  We
		 * can also consider EC-derived join clauses.
		 */
		restrictlist =
			list_concat(generate_join_implied_equalities(root,
														 joinrelids,
														 sjinfo->min_lefthand,
														 innerrel),
						innerrel->joininfo);

		/* Test whether the innerrel is unique for those clauses. */
		if (!innerrel_is_unique(root,
								joinrelids, sjinfo->min_lefthand, innerrel,
								JOIN_SEMI, restrictlist, true))
			continue;

		/* OK, remove the SpecialJoinInfo from the list. */
		root->join_info_list = foreach_delete_current(root->join_info_list, lc);
	}
}


/*
 * rel_supports_distinctness
 *		Could the relation possibly be proven distinct on some set of columns?
 *
 * This is effectively a pre-checking function for rel_is_distinct_for().
 * It must return true if rel_is_distinct_for() could possibly return true
 * with this rel, but it should not expend a lot of cycles.  The idea is
 * that callers can avoid doing possibly-expensive processing to compute
 * rel_is_distinct_for()'s argument lists if the call could not possibly
 * succeed.
 */
static bool
rel_supports_distinctness(PlannerInfo *root, RelOptInfo *rel)
{
	/* We only know about baserels ... */
	if (rel->reloptkind != RELOPT_BASEREL)
		return false;
	if (rel->rtekind == RTE_RELATION)
	{
		/*
		 * For a plain relation, we only know how to prove uniqueness by
		 * reference to unique indexes.  Make sure there's at least one
		 * suitable unique index.  It must be immediately enforced, and if
		 * it's a partial index, it must match the query.  (Keep these
		 * conditions in sync with relation_has_unique_index_for!)
		 */
		ListCell   *lc;

		foreach(lc, rel->indexlist)
		{
			IndexOptInfo *ind = (IndexOptInfo *) lfirst(lc);

			if (ind->unique && ind->immediate &&
				(ind->indpred == NIL || ind->predOK))
				return true;
		}
	}
	else if (rel->rtekind == RTE_SUBQUERY)
	{
		Query	   *subquery = root->simple_rte_array[rel->relid]->subquery;

		/* Check if the subquery has any qualities that support distinctness */
		if (query_supports_distinctness(subquery))
			return true;
	}
	/* We have no proof rules for any other rtekinds. */
	return false;
}

/*
 * rel_is_distinct_for
 *		Does the relation return only distinct rows according to clause_list?
 *
 * clause_list is a list of join restriction clauses involving this rel and
 * some other one.  Return true if no two rows emitted by this rel could
 * possibly join to the same row of the other rel.
 *
 * The caller must have already determined that each condition is a
 * mergejoinable equality with an expression in this relation on one side, and
 * an expression not involving this relation on the other.  The transient
 * outer_is_left flag is used to identify which side references this relation:
 * left side if outer_is_left is false, right side if it is true.
 *
 * Note that the passed-in clause_list may be destructively modified!  This
 * is OK for current uses, because the clause_list is built by the caller for
 * the sole purpose of passing to this function.
 */
static bool
rel_is_distinct_for(PlannerInfo *root, RelOptInfo *rel, List *clause_list)
{
	/*
	 * We could skip a couple of tests here if we assume all callers checked
	 * rel_supports_distinctness first, but it doesn't seem worth taking any
	 * risk for.
	 */
	if (rel->reloptkind != RELOPT_BASEREL)
		return false;
	if (rel->rtekind == RTE_RELATION)
	{
		/*
		 * Examine the indexes to see if we have a matching unique index.
		 * relation_has_unique_index_for automatically adds any usable
		 * restriction clauses for the rel, so we needn't do that here.
		 */
		if (relation_has_unique_index_for(root, rel, clause_list, NIL, NIL))
			return true;
	}
	else if (rel->rtekind == RTE_SUBQUERY)
	{
		Index		relid = rel->relid;
		Query	   *subquery = root->simple_rte_array[relid]->subquery;
		List	   *colnos = NIL;
		List	   *opids = NIL;
		ListCell   *l;

		/*
		 * Build the argument lists for query_is_distinct_for: a list of
		 * output column numbers that the query needs to be distinct over, and
		 * a list of equality operators that the output columns need to be
		 * distinct according to.
		 *
		 * (XXX we are not considering restriction clauses attached to the
		 * subquery; is that worth doing?)
		 */
		foreach(l, clause_list)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, l);
			Oid			op;
			Var		   *var;

			/*
			 * Get the equality operator we need uniqueness according to.
			 * (This might be a cross-type operator and thus not exactly the
			 * same operator the subquery would consider; that's all right
			 * since query_is_distinct_for can resolve such cases.)  The
			 * caller's mergejoinability test should have selected only
			 * OpExprs.
			 */
			op = castNode(OpExpr, rinfo->clause)->opno;

			/* caller identified the inner side for us */
			if (rinfo->outer_is_left)
				var = (Var *) get_rightop(rinfo->clause);
			else
				var = (Var *) get_leftop(rinfo->clause);

			/*
			 * We may ignore any RelabelType node above the operand.  (There
			 * won't be more than one, since eval_const_expressions() has been
			 * applied already.)
			 */
			if (var && IsA(var, RelabelType))
				var = (Var *) ((RelabelType *) var)->arg;

			/*
			 * If inner side isn't a Var referencing a subquery output column,
			 * this clause doesn't help us.
			 */
			if (!var || !IsA(var, Var) ||
				var->varno != relid || var->varlevelsup != 0)
				continue;

			colnos = lappend_int(colnos, var->varattno);
			opids = lappend_oid(opids, op);
		}

		if (query_is_distinct_for(subquery, colnos, opids))
			return true;
	}
	return false;
}


/*
 * query_supports_distinctness - could the query possibly be proven distinct
 *		on some set of output columns?
 *
 * This is effectively a pre-checking function for query_is_distinct_for().
 * It must return true if query_is_distinct_for() could possibly return true
 * with this query, but it should not expend a lot of cycles.  The idea is
 * that callers can avoid doing possibly-expensive processing to compute
 * query_is_distinct_for()'s argument lists if the call could not possibly
 * succeed.
 */
bool
query_supports_distinctness(Query *query)
{
	/* SRFs break distinctness except with DISTINCT, see below */
	if (query->hasTargetSRFs && query->distinctClause == NIL)
		return false;

	/* check for features we can prove distinctness with */
	if (query->distinctClause != NIL ||
		query->groupClause != NIL ||
		query->groupingSets != NIL ||
		query->hasAggs ||
		query->havingQual ||
		query->setOperations)
		return true;

	return false;
}

/*
 * query_is_distinct_for - does query never return duplicates of the
 *		specified columns?
 *
 * query is a not-yet-planned subquery (in current usage, it's always from
 * a subquery RTE, which the planner avoids scribbling on).
 *
 * colnos is an integer list of output column numbers (resno's).  We are
 * interested in whether rows consisting of just these columns are certain
 * to be distinct.  "Distinctness" is defined according to whether the
 * corresponding upper-level equality operators listed in opids would think
 * the values are distinct.  (Note: the opids entries could be cross-type
 * operators, and thus not exactly the equality operators that the subquery
 * would use itself.  We use equality_ops_are_compatible() to check
 * compatibility.  That looks at btree or hash opfamily membership, and so
 * should give trustworthy answers for all operators that we might need
 * to deal with here.)
 */
bool
query_is_distinct_for(Query *query, List *colnos, List *opids)
{
	ListCell   *l;
	Oid			opid;

	Assert(list_length(colnos) == list_length(opids));

	/*
	 * DISTINCT (including DISTINCT ON) guarantees uniqueness if all the
	 * columns in the DISTINCT clause appear in colnos and operator semantics
	 * match.  This is true even if there are SRFs in the DISTINCT columns or
	 * elsewhere in the tlist.
	 */
	if (query->distinctClause)
	{
		foreach(l, query->distinctClause)
		{
			SortGroupClause *sgc = (SortGroupClause *) lfirst(l);
			TargetEntry *tle = get_sortgroupclause_tle(sgc,
													   query->targetList);

			opid = distinct_col_search(tle->resno, colnos, opids);
			if (!OidIsValid(opid) ||
				!equality_ops_are_compatible(opid, sgc->eqop))
				break;			/* exit early if no match */
		}
		if (l == NULL)			/* had matches for all? */
			return true;
	}

	/*
	 * Otherwise, a set-returning function in the query's targetlist can
	 * result in returning duplicate rows, despite any grouping that might
	 * occur before tlist evaluation.  (If all tlist SRFs are within GROUP BY
	 * columns, it would be safe because they'd be expanded before grouping.
	 * But it doesn't currently seem worth the effort to check for that.)
	 */
	if (query->hasTargetSRFs)
		return false;

	/*
	 * Similarly, GROUP BY without GROUPING SETS guarantees uniqueness if all
	 * the grouped columns appear in colnos and operator semantics match.
	 */
	if (query->groupClause && !query->groupingSets)
	{
		foreach(l, query->groupClause)
		{
			SortGroupClause *sgc = (SortGroupClause *) lfirst(l);
			TargetEntry *tle = get_sortgroupclause_tle(sgc,
													   query->targetList);

			opid = distinct_col_search(tle->resno, colnos, opids);
			if (!OidIsValid(opid) ||
				!equality_ops_are_compatible(opid, sgc->eqop))
				break;			/* exit early if no match */
		}
		if (l == NULL)			/* had matches for all? */
			return true;
	}
	else if (query->groupingSets)
	{
		/*
		 * If we have grouping sets with expressions, we probably don't have
		 * uniqueness and analysis would be hard. Punt.
		 */
		if (query->groupClause)
			return false;

		/*
		 * If we have no groupClause (therefore no grouping expressions), we
		 * might have one or many empty grouping sets. If there's just one,
		 * then we're returning only one row and are certainly unique. But
		 * otherwise, we know we're certainly not unique.
		 */
		if (list_length(query->groupingSets) == 1 &&
			((GroupingSet *) linitial(query->groupingSets))->kind == GROUPING_SET_EMPTY)
			return true;
		else
			return false;
	}
	else
	{
		/*
		 * If we have no GROUP BY, but do have aggregates or HAVING, then the
		 * result is at most one row so it's surely unique, for any operators.
		 */
		if (query->hasAggs || query->havingQual)
			return true;
	}

	/*
	 * UNION, INTERSECT, EXCEPT guarantee uniqueness of the whole output row,
	 * except with ALL.
	 */
	if (query->setOperations)
	{
		SetOperationStmt *topop = castNode(SetOperationStmt, query->setOperations);

		Assert(topop->op != SETOP_NONE);

		if (!topop->all)
		{
			ListCell   *lg;

			/* We're good if all the nonjunk output columns are in colnos */
			lg = list_head(topop->groupClauses);
			foreach(l, query->targetList)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(l);
				SortGroupClause *sgc;

				if (tle->resjunk)
					continue;	/* ignore resjunk columns */

				/* non-resjunk columns should have grouping clauses */
				Assert(lg != NULL);
				sgc = (SortGroupClause *) lfirst(lg);
				lg = lnext(topop->groupClauses, lg);

				opid = distinct_col_search(tle->resno, colnos, opids);
				if (!OidIsValid(opid) ||
					!equality_ops_are_compatible(opid, sgc->eqop))
					break;		/* exit early if no match */
			}
			if (l == NULL)		/* had matches for all? */
				return true;
		}
	}

	/*
	 * XXX Are there any other cases in which we can easily see the result
	 * must be distinct?
	 *
	 * If you do add more smarts to this function, be sure to update
	 * query_supports_distinctness() to match.
	 */

	return false;
}

/*
 * distinct_col_search - subroutine for query_is_distinct_for
 *
 * If colno is in colnos, return the corresponding element of opids,
 * else return InvalidOid.  (Ordinarily colnos would not contain duplicates,
 * but if it does, we arbitrarily select the first match.)
 */
static Oid
distinct_col_search(int colno, List *colnos, List *opids)
{
	ListCell   *lc1,
			   *lc2;

	forboth(lc1, colnos, lc2, opids)
	{
		if (colno == lfirst_int(lc1))
			return lfirst_oid(lc2);
	}
	return InvalidOid;
}


/*
 * innerrel_is_unique
 *	  Check if the innerrel provably contains at most one tuple matching any
 *	  tuple from the outerrel, based on join clauses in the 'restrictlist'.
 *
 * We need an actual RelOptInfo for the innerrel, but it's sufficient to
 * identify the outerrel by its Relids.  This asymmetry supports use of this
 * function before joinrels have been built.  (The caller is expected to
 * also supply the joinrelids, just to save recalculating that.)
 *
 * The proof must be made based only on clauses that will be "joinquals"
 * rather than "otherquals" at execution.  For an inner join there's no
 * difference; but if the join is outer, we must ignore pushed-down quals,
 * as those will become "otherquals".  Note that this means the answer might
 * vary depending on whether IS_OUTER_JOIN(jointype); since we cache the
 * answer without regard to that, callers must take care not to call this
 * with jointypes that would be classified differently by IS_OUTER_JOIN().
 *
 * The actual proof is undertaken by is_innerrel_unique_for(); this function
 * is a frontend that is mainly concerned with caching the answers.
 * In particular, the force_cache argument allows overriding the internal
 * heuristic about whether to cache negative answers; it should be "true"
 * if making an inquiry that is not part of the normal bottom-up join search
 * sequence.
 */
bool
innerrel_is_unique(PlannerInfo *root,
				   Relids joinrelids,
				   Relids outerrelids,
				   RelOptInfo *innerrel,
				   JoinType jointype,
				   List *restrictlist,
				   bool force_cache)
{
	MemoryContext old_context;
	ListCell   *lc;

	/* Certainly can't prove uniqueness when there are no joinclauses */
	if (restrictlist == NIL)
		return false;

	/*
	 * Make a quick check to eliminate cases in which we will surely be unable
	 * to prove uniqueness of the innerrel.
	 */
	if (!rel_supports_distinctness(root, innerrel))
		return false;

	/*
	 * Query the cache to see if we've managed to prove that innerrel is
	 * unique for any subset of this outerrel.  We don't need an exact match,
	 * as extra outerrels can't make the innerrel any less unique (or more
	 * formally, the restrictlist for a join to a superset outerrel must be a
	 * superset of the conditions we successfully used before).
	 */
	foreach(lc, innerrel->unique_for_rels)
	{
		Relids		unique_for_rels = (Relids) lfirst(lc);

		if (bms_is_subset(unique_for_rels, outerrelids))
			return true;		/* Success! */
	}

	/*
	 * Conversely, we may have already determined that this outerrel, or some
	 * superset thereof, cannot prove this innerrel to be unique.
	 */
	foreach(lc, innerrel->non_unique_for_rels)
	{
		Relids		unique_for_rels = (Relids) lfirst(lc);

		if (bms_is_subset(outerrelids, unique_for_rels))
			return false;
	}

	/* No cached information, so try to make the proof. */
	if (is_innerrel_unique_for(root, joinrelids, outerrelids, innerrel,
							   jointype, restrictlist))
	{
		/*
		 * Cache the positive result for future probes, being sure to keep it
		 * in the planner_cxt even if we are working in GEQO.
		 *
		 * Note: one might consider trying to isolate the minimal subset of
		 * the outerrels that proved the innerrel unique.  But it's not worth
		 * the trouble, because the planner builds up joinrels incrementally
		 * and so we'll see the minimally sufficient outerrels before any
		 * supersets of them anyway.
		 */
		old_context = MemoryContextSwitchTo(root->planner_cxt);
		innerrel->unique_for_rels = lappend(innerrel->unique_for_rels,
											bms_copy(outerrelids));
		MemoryContextSwitchTo(old_context);

		return true;			/* Success! */
	}
	else
	{
		/*
		 * None of the join conditions for outerrel proved innerrel unique, so
		 * we can safely reject this outerrel or any subset of it in future
		 * checks.
		 *
		 * However, in normal planning mode, caching this knowledge is totally
		 * pointless; it won't be queried again, because we build up joinrels
		 * from smaller to larger.  It is useful in GEQO mode, where the
		 * knowledge can be carried across successive planning attempts; and
		 * it's likely to be useful when using join-search plugins, too. Hence
		 * cache when join_search_private is non-NULL.  (Yeah, that's a hack,
		 * but it seems reasonable.)
		 *
		 * Also, allow callers to override that heuristic and force caching;
		 * that's useful for reduce_unique_semijoins, which calls here before
		 * the normal join search starts.
		 */
		if (force_cache || root->join_search_private)
		{
			old_context = MemoryContextSwitchTo(root->planner_cxt);
			innerrel->non_unique_for_rels =
				lappend(innerrel->non_unique_for_rels,
						bms_copy(outerrelids));
			MemoryContextSwitchTo(old_context);
		}

		return false;
	}
}

/*
 * is_innerrel_unique_for
 *	  Check if the innerrel provably contains at most one tuple matching any
 *	  tuple from the outerrel, based on join clauses in the 'restrictlist'.
 */
static bool
is_innerrel_unique_for(PlannerInfo *root,
					   Relids joinrelids,
					   Relids outerrelids,
					   RelOptInfo *innerrel,
					   JoinType jointype,
					   List *restrictlist)
{
	List	   *clause_list = NIL;
	ListCell   *lc;

	/*
	 * Search for mergejoinable clauses that constrain the inner rel against
	 * the outer rel.  If an operator is mergejoinable then it behaves like
	 * equality for some btree opclass, so it's what we want.  The
	 * mergejoinability test also eliminates clauses containing volatile
	 * functions, which we couldn't depend on.
	 */
	foreach(lc, restrictlist)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(lc);

		/*
		 * As noted above, if it's a pushed-down clause and we're at an outer
		 * join, we can't use it.
		 */
		if (IS_OUTER_JOIN(jointype) &&
			RINFO_IS_PUSHED_DOWN(restrictinfo, joinrelids))
			continue;

		/* Ignore if it's not a mergejoinable clause */
		if (!restrictinfo->can_join ||
			restrictinfo->mergeopfamilies == NIL)
			continue;			/* not mergejoinable */

		/*
		 * Check if clause has the form "outer op inner" or "inner op outer",
		 * and if so mark which side is inner.
		 */
		if (!clause_sides_match_join(restrictinfo, outerrelids,
									 innerrel->relids))
			continue;			/* no good for these input relations */

		/* OK, add to list */
		clause_list = lappend(clause_list, restrictinfo);
	}

	/* Let rel_is_distinct_for() do the hard work */
	return rel_is_distinct_for(root, innerrel, clause_list);
}

typedef struct ChangeVarnoContext
{
	Index oldRelid;
	Index newRelid;
} ChangeVarnoContext;


static bool
change_varno_walker(Node *node, ChangeVarnoContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var* var = (Var*)node;
		if (var->varno == context->oldRelid)
		{
			var->varno = context->newRelid;
			var->varnosyn = context->newRelid;
			var->location = -1;
		}
		else if (var->varno == context->newRelid)
			var->location = -1;

		return false;
	}
	if (IsA(node, RestrictInfo))
	{
		change_rinfo((RestrictInfo*)node, context->oldRelid, context->newRelid);
		return false;
	}
	return expression_tree_walker(node, change_varno_walker, context);
}

/*
 * For all Vars in the expression that have varno = oldRelid, set
 * varno = newRelid.
 */
static void
change_varno(Expr *expr, Index oldRelid, Index newRelid)
{
	ChangeVarnoContext context;

	if (newRelid == 0)
		return;

	context.oldRelid = oldRelid;
	context.newRelid = newRelid;
	change_varno_walker((Node *) expr, &context);
}

/*
 * Substitute newId for oldId in relids.
 */
static Bitmapset*
change_relid(Relids relids, Index oldId, Index newId)
{
	if (newId == 0)
		/* Delete relid without substitution. */
		return bms_del_member(relids, oldId);

	if (bms_is_member(oldId, relids))
		return bms_add_member(bms_del_member(bms_copy(relids), oldId), newId);

	return relids;
}

static void
change_rinfo(RestrictInfo* rinfo, Index from, Index to)
{
	bool is_req_equal;

	if (to == 0)
		return;

	is_req_equal =
		(rinfo->required_relids == rinfo->clause_relids) ? true : false;

	change_varno(rinfo->clause, from, to);
	change_varno(rinfo->orclause, from, to);
	rinfo->clause_relids = change_relid(rinfo->clause_relids, from, to);
	if (is_req_equal)
		rinfo->required_relids = rinfo->clause_relids;
	else
		rinfo->required_relids = change_relid(rinfo->required_relids, from, to);
	rinfo->left_relids = change_relid(rinfo->left_relids, from, to);
	rinfo->right_relids = change_relid(rinfo->right_relids, from, to);
	rinfo->outer_relids = change_relid(rinfo->outer_relids, from, to);
	rinfo->nullable_relids = change_relid(rinfo->nullable_relids, from, to);
}

/*
 * Update EC members to point to the remaining relation instead of the removed
 * one, removing duplicates.
 */
static void
update_ec_members(EquivalenceClass *ec, Index toRemove, Index toKeep)
{
	int			counter;

	for (counter = 0; counter < list_length(ec->ec_members); )
	{
		ListCell			*cell = list_nth_cell(ec->ec_members, counter);
		EquivalenceMember	*em = lfirst(cell);
		int					counter1;

		if (!bms_is_member(toRemove, em->em_relids))
		{
			counter++;
			continue;
		}

		em->em_relids = change_relid(em->em_relids, toRemove, toKeep);
		/* We only process inner joins */
		change_varno(em->em_expr, toRemove, toKeep);

		/*
		 * After we switched the equivalence member to the remaining relation,
		 * check that it is not the same as the existing member, and if it
		 * is, delete it.
		 */
		for (counter1 = 0; counter1 < list_length(ec->ec_members); counter1++)
		{
			EquivalenceMember	*other;

			if (counter1 == counter)
				continue;

			other = castNode(EquivalenceMember, list_nth(ec->ec_members, counter1));

			if (equal(other->em_expr, em->em_expr))
				break;
		}

		if (counter1 < list_length(ec->ec_members))
			ec->ec_members = list_delete_cell(ec->ec_members, cell);
		else
			counter++;
	}
}

/*
 * Update EC sources to point to the remaining relation instead of the
 * removed one.
 */
static void
update_ec_sources(List **sources, Index toRemove, Index toKeep)
{
	int			counter;

	for (counter = 0; counter < list_length(*sources); )
	{
		ListCell		*cell = list_nth_cell(*sources, counter);
		RestrictInfo	*rinfo = castNode(RestrictInfo, lfirst(cell));
		int				counter1;

		if (!bms_is_member(toRemove, rinfo->required_relids))
		{
			counter++;
			continue;
		}

		change_varno(rinfo->clause, toRemove, toKeep);

		/*
		 * After switching the clause to the remaining relation, check it for
		 * redundancy with existing ones. We don't have to check for
		 * redundancy with derived clauses, because we've just deleted them.
		 */
		for (counter1 = 0; counter1 < list_length(*sources); counter1++)
		{
			RestrictInfo *other;

			if (counter1 == counter)
				continue;

			other = castNode(RestrictInfo, list_nth(*sources, counter1));
			if (equal(rinfo->clause, other->clause))
				break;
		}

		if (counter1 < list_length(*sources))
			*sources = list_delete_cell(*sources, cell);
		else
		{
			counter++;

			/* We will keep this RestrictInfo, correct its relids. */
			change_rinfo(rinfo, toRemove, toKeep);
		}
	}
}

/*
 * Remove a relation after we have proven that it participates only in an
 * unneeded unique self join.
 *
 * The joinclauses list is destructively changed.
 */
static void
remove_self_join_rel(PlannerInfo *root, PlanRowMark *kmark, PlanRowMark *rmark,
					 RelOptInfo *toKeep, RelOptInfo *toRemove)
{
	ListCell *cell;
	int i;
	List *target = NIL;

	/*
	 * Include all eclass mentions of removed relation into the eclass mentions
	 * of kept relation.
	 */
	toKeep->eclass_indexes = bms_add_members(toRemove->eclass_indexes,
														toKeep->eclass_indexes);

	/*
	 * Now, baserestrictinfo replenished with restrictions from removing
	 * relation. It is needed to remove duplicates and replace degenerated
	 * clauses with a NullTest.
	 */
	foreach(cell, toKeep->baserestrictinfo)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, cell);
		ListCell *otherCell;

		Assert(!bms_is_member(toRemove->relid, rinfo->clause_relids));

		/*
		 * If this clause is a mergejoinable equality clause that compares a
		 * variable to itself, i.e., has the form of "X=X", replace it with
		 * null test.
		 */
		if (rinfo->mergeopfamilies && IsA(rinfo->clause, OpExpr))
		{
			Expr *leftOp;
			Expr *rightOp;

			leftOp = (Expr *) get_leftop(rinfo->clause);
			rightOp = (Expr *) get_rightop(rinfo->clause);

			if (leftOp != NULL && equal(leftOp, rightOp))
			{
				NullTest *nullTest = makeNode(NullTest);
				nullTest->arg = leftOp;
				nullTest->nulltesttype = IS_NOT_NULL;
				nullTest->argisrow = false;
				nullTest->location = -1;
				rinfo->clause = (Expr *) nullTest;
			}
		}

		/* Search for duplicates. */
		foreach(otherCell, target)
		{
			RestrictInfo *other = lfirst_node(RestrictInfo, otherCell);

			if (other == rinfo ||
				(rinfo->parent_ec != NULL
				 && other->parent_ec == rinfo->parent_ec)
				 || equal(rinfo->clause, other->clause))
			{
				break;
			}
		}

		if (otherCell != NULL)
			/* Duplicate found */
			continue;

		target = lappend(target, rinfo);
	}

	list_free(toKeep->baserestrictinfo);
	toKeep->baserestrictinfo = target;

	/*
	 * Update the equivalence classes that reference the removed relations.
	 */
	foreach(cell, root->eq_classes)
	{
		EquivalenceClass *ec = lfirst(cell);

		if (!bms_is_member(toRemove->relid, ec->ec_relids))
		{
			/*
			 * This EC doesn't reference the removed relation, nothing to be
			 * done for it.
			 */
			continue;
		}

		/*
		 * Update the EC members to reference the remaining relation instead
		 * of the removed one.
		 */
		update_ec_members(ec, toRemove->relid, toKeep->relid);
		ec->ec_relids = change_relid(ec->ec_relids, toRemove->relid, toKeep->relid);

		/*
		 * We will now update source and derived clauses of the EC.
		 *
		 * Restriction clauses for base relations are already distributed to
		 * the respective baserestrictinfo lists (see
		 * generate_implied_equalities). The above code has already processed
		 * this list, and updated these clauses to reference the remaining
		 * relation, so we can skip them here based on their relids.
		 *
		 * Likewise, we have already processed the join clauses that join the
		 * removed relation to the remaining one.
		 *
		 * Finally, there are join clauses that join the removed relation to
		 * some third relation. We can't just delete the source clauses and
		 * regenerate them from the EC, because the corresponding equality
		 * operators might be missing (see the handling of ec_broken).
		 * Therefore, we will update the references in the source clauses.
		 *
		 * Derived clauses can be generated again, so it is simpler to just
		 * delete them.
		 */
		list_free(ec->ec_derives);
		ec->ec_derives = NULL;
		update_ec_sources(&ec->ec_sources, toRemove->relid, toKeep->relid);
	}

	/*
	 * Transfer the targetlist and attr_needed flags.
	 */
	Assert(toRemove->reltarget->sortgrouprefs == 0);

	foreach (cell, toRemove->reltarget->exprs)
	{
		Expr *node = lfirst(cell);
		change_varno(node, toRemove->relid, toKeep->relid);
		if (!list_member(toKeep->reltarget->exprs, node))
			toKeep->reltarget->exprs = lappend(toKeep->reltarget->exprs, node);
	}

	for (i = toKeep->min_attr; i <= toKeep->max_attr; i++)
	{
		int attno = i - toKeep->min_attr;
		toKeep->attr_needed[attno] = bms_add_members(toKeep->attr_needed[attno],
													 toRemove->attr_needed[attno]);
	}

	/*
	 * If the removed relation has a row mark, transfer it to the remaining
	 * one.
	 *
	 * If both rels have row marks, just keep the one corresponding to the
	 * remaining relation, because we verified earlier that they have the same
	 * strength.
	 *
	 * Also make sure that the scratch->row_marks cache is up to date, because
	 * we are going to use it for further join removals.
	 */
	if (rmark)
	{
		if (kmark)
		{
			Assert(kmark->markType == rmark->markType);

			root->rowMarks = list_delete_ptr(root->rowMarks, kmark);
		}
		else
		{
			/* Shouldn't have inheritance children here. */
			Assert(kmark->rti == kmark->prti);

			rmark->rti = toKeep->relid;
			rmark->prti = toKeep->relid;
		}
	}

	/*
	 * Change varno in some special cases with non-trivial RangeTblEntry
	 */
	foreach(cell, root->parse->rtable)
	{
		RangeTblEntry *rte	= lfirst(cell);

		switch(rte->rtekind)
		{
			case RTE_FUNCTION:
				change_varno((Expr*)rte->functions, toRemove->relid, toKeep->relid);
				break;
			case RTE_TABLEFUNC:
				change_varno((Expr*)rte->tablefunc, toRemove->relid, toKeep->relid);
				break;
			case RTE_VALUES:
				change_varno((Expr*)rte->values_lists, toRemove->relid, toKeep->relid);
				break;
			default:
				/* no op */
				break;
		}
	}

	/*
	 * Replace varno in root targetlist and HAVING clause.
	 */
	change_varno((Expr *) root->processed_tlist, toRemove->relid, toKeep->relid);
	change_varno((Expr *) root->parse->havingQual, toRemove->relid, toKeep->relid);

	/*
	 * Transfer join and restriction clauses from the removed relation to the
	 * remaining one. We change the Vars of the clause to point to the
	 * remaining relation instead of the removed one. The clauses that require
	 * a subset of joinrelids become restriction clauses of the remaining
	 * relation, and others remain join clauses. We append them to
	 * baserestrictinfo and joininfo respectively, trying not to introduce
	 * duplicates.
	 *
	 * We also have to process the 'joinclauses' list here, because it
	 * contains EC-derived join clauses which must become filter clauses. It
	 * is not enough to just correct the ECs, because the EC-derived
	 * restrictions are generated before join removal (see
	 * generate_base_implied_equalities).
	 */
}

/*
 * split_selfjoin_quals
 *		Processes 'joinquals' building two lists, one with a list of quals
 *		where the columns/exprs on either side of the join match and another
 *		list containing the remaining quals.
 *
 * 'joinquals' must only contain quals for a RTE_RELATION being joined to
 * itself.
 */
static void
split_selfjoin_quals(PlannerInfo *root, List *joinquals, List **selfjoinquals,
					 List **otherjoinquals)
{
	ListCell *lc;
	List	 *sjoinquals = NIL;
	List	 *ojoinquals = NIL;

	foreach(lc, joinquals)
	{
		RestrictInfo	*rinfo = lfirst_node(RestrictInfo, lc);
		OpExpr			*expr;
		Expr			*leftexpr;
		Expr			*rightexpr;

		/* In general, clause looks like F(arg1) = G(arg2) ? */
		if (!rinfo->mergeopfamilies ||
			bms_num_members(rinfo->clause_relids) != 2 ||
			bms_num_members(rinfo->left_relids) != 1 ||
			bms_num_members(rinfo->right_relids) != 1)
		{
			ojoinquals = lappend(ojoinquals, rinfo);
			continue;
		}

		expr = (OpExpr *) rinfo->clause;

		if (!IsA(expr, OpExpr) || list_length(expr->args) != 2)
		{
			ojoinquals = lappend(ojoinquals, rinfo);
			continue;
		}

		leftexpr = (Expr *) copyObject(get_leftop(rinfo->clause));
		rightexpr = (Expr *) copyObject(get_rightop(rinfo->clause));

		/* Can't match of the exprs are not of the same type */
		if (leftexpr->type != rightexpr->type)
		{
			ojoinquals = lappend(ojoinquals, rinfo);
			continue;
		}

		change_varno(rightexpr,
					 bms_next_member(rinfo->right_relids, -1),
					 bms_next_member(rinfo->left_relids, -1));

		if (equal(leftexpr, rightexpr))
			sjoinquals = lappend(sjoinquals, rinfo);
		else
			ojoinquals = lappend(ojoinquals, rinfo);
	}

	*selfjoinquals = sjoinquals;
	*otherjoinquals = ojoinquals;
}

/*
 * Find and remove unique self joins in a group of base relations that have
 * the same Oid.
 *
 * Returns a set of relids that were removed.
 */
static Relids
remove_self_joins_one_group(PlannerInfo *root, Relids relids)
{
	Relids joinrelids = NULL;
	Relids result = NULL;
	int k; /* Index of kept relation */
	int r = -1; /* Index of removed relation */

	if (bms_num_members(relids) < 2)
		return NULL;

	while ((r = bms_next_member(relids, r)) > 0)
	{
		RelOptInfo *outer = root->simple_rel_array[r];
		k = r;

		while ((k = bms_next_member(relids, k)) > 0)
		{
			RelOptInfo	*inner = root->simple_rel_array[k];
			List	   *restrictlist;
			List	   *selfjoinquals;
			List	   *otherjoinquals;
			ListCell	*lc;
			bool		jinfo_check = true;
			PlanRowMark	*omark = NULL;
			PlanRowMark	*imark = NULL;

			/* A sanity check: the relations have the same Oid. */
			Assert(root->simple_rte_array[k]->relid ==
				   root->simple_rte_array[r]->relid);

			/*
			 * It is impossible to optimize two relations if they belong to
			 * different rules of order restriction. Otherwise planner can't
			 * be able to find any variants of correct query plan.
			 */
			foreach(lc, root->join_info_list)
			{
				SpecialJoinInfo *info = (SpecialJoinInfo *) lfirst(lc);

				if (bms_is_member(k, info->syn_lefthand) &&
					!bms_is_member(r, info->syn_lefthand))
					jinfo_check = false;
				else if (bms_is_member(k, info->syn_righthand) &&
					!bms_is_member(r, info->syn_righthand))
					jinfo_check = false;
				else if (bms_is_member(r, info->syn_lefthand) &&
					!bms_is_member(k, info->syn_lefthand))
					jinfo_check = false;
				else if (bms_is_member(r, info->syn_righthand) &&
					!bms_is_member(k, info->syn_righthand))
					jinfo_check = false;

				if (!jinfo_check)
					break;
			}

			if (!jinfo_check)
				continue;

			/* Reuse joinrelids bitset to avoid reallocation. */
			joinrelids = bms_del_members(joinrelids, joinrelids);

			/*
			 * We only deal with base rels here, so their relids bitset
			 * contains only one member -- their relid.
			 */
			joinrelids = bms_add_member(joinrelids, r);
			joinrelids = bms_add_member(joinrelids, k);

			/* Is it a unique self join? */
			restrictlist = build_joinrel_restrictlist(root, joinrelids, outer,
													  inner);

			/*
			 * Process restrictlist to seperate out the self join quals from
			 * the other quals. e.g x = x goes to selfjoinquals and a = b to
			 * otherjoinquals.
			 */
			split_selfjoin_quals(root, restrictlist, &selfjoinquals,
								 &otherjoinquals);

			if (list_length(selfjoinquals) == 0)
			{
				/*
				 * XXX:
				 * we would detect self-join without quals like 'x==x' if we had
				 * an foreign key constraint on some of other quals and this join
				 * haven't any columns from the outer in the target list.
				 * But it is still complex task.
				 */
				continue;
			}

			/*
			 * Determine if the inner table can duplicate outer rows.  We must
			 * bypass the unique rel cache here since we're possibly using a
			 * subset of join quals. We can use 'force_cache' = true when all
			 * join quals are selfjoin quals.  Otherwise we could end up
			 * putting false negatives in the cache.
			 */
			if (!innerrel_is_unique(root, joinrelids, outer->relids,
									inner, JOIN_INNER, selfjoinquals,
									list_length(otherjoinquals) == 0))
				continue;

			/* See for row marks. */
			foreach (lc, root->rowMarks)
			{
				PlanRowMark *rowMark = (PlanRowMark *) lfirst(lc);

				if (rowMark->rti == k)
				{
					Assert(imark == NULL);
					imark = rowMark;
				}
				else if (rowMark->rti == r)
				{
					Assert(omark == NULL);
					omark = rowMark;
				}

				if (omark && imark)
					break;
			}

			/*
			 * We can't remove the join if the relations have row marks of
			 * different strength (e.g. one is locked FOR UPDATE and another
			 * just has ROW_MARK_REFERENCE for EvalPlanQual rechecking).
			 */
			if (omark && imark && omark->markType != imark->markType)
				continue;

			/*
			 * Be safe to do not remove table participated in complicated PH
			 */
			foreach(lc, root->placeholder_list)
			{
				PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(lc);

				/* there isn't any other place to eval PHV */
				if (bms_is_subset(phinfo->ph_eval_at, joinrelids) ||
					bms_is_subset(phinfo->ph_needed, joinrelids))
					break;
			}

			if (lc)
				continue;

			/*
			 * We can remove either relation, so remove the outer one, to
			 * simplify this loop.
			 */

			/*
			 * Add join restrictions to joininfo of removing relation to simplify
			 * the relids replacing procedure.
			 */
			outer->joininfo = list_concat(outer->joininfo, restrictlist);

			/* Firstly, replace index of excluding relation with keeping. */
			remove_rel_from_query(root, outer->relid, joinrelids, inner->relid);

			/* Secondly, fix restrictions of keeping relation */
			remove_self_join_rel(root, imark, omark, inner, outer);
			result = bms_add_member(result, r);

			/* We removed the outer relation, try the next one. */
			break;
		}
	}

	return result;
}

/*
 * Iteratively form a group of relation indexes with the same oid and launch
 * the routine that detects self-joins in this group and removes excessive
 * range table entries.
 *
 * At the end of iteration, exclude the group from the overall relids list.
 * So each next iteration of the cycle will involve less and less value of
 * relids.
 */
static Relids
remove_self_joins_one_level(PlannerInfo *root, Relids relids, Relids ToRemove)
{
	while (!bms_is_empty(relids))
	{
		Relids group = NULL;
		Oid groupOid;
		int i;

		i = bms_first_member(relids);
		groupOid = root->simple_rte_array[i]->relid;
		Assert(OidIsValid(groupOid));
		group = bms_add_member(group, i);

		/* Create group of relation indexes with the same oid. */
		while ((i = bms_next_member(relids, i)) > 0)
		{
			RangeTblEntry *rte = root->simple_rte_array[i];

			Assert(OidIsValid(rte->relid));

			if (rte->relid == groupOid)
				group = bms_add_member(group, i);
		}

		relids = bms_del_members(relids, group);
		ToRemove = bms_add_members(ToRemove,
								   remove_self_joins_one_group(root, group));
		bms_free(group);
	}
	return ToRemove;
}

/*
 * For each level of joinlist form a set of base relations and launch the
 * routine of the self-join removal optimization. Recurse into sub-joinlists to
 * handle deeper levels.
 */
static Relids
remove_self_joins_recurse(PlannerInfo *root, List *joinlist, Relids ToRemove)
{
	ListCell *lc;
	Relids relids = NULL;

	/* Collect the ids of base relations at one level of the join tree. */
	foreach (lc, joinlist)
	{
		switch (((Node *) lfirst(lc))->type)
		{
			case T_List:
				/* Recursively go inside the sub-joinlist */
				ToRemove = remove_self_joins_recurse(root,
													 (List *) lfirst(lc),
													 ToRemove);
				break;
			case T_RangeTblRef:
			{
				RangeTblRef *ref = (RangeTblRef *) lfirst(lc);
				RangeTblEntry *rte = root->simple_rte_array[ref->rtindex];

				/*
				 * We only care about base relations from which we select
				 * something.
				 */
				if (rte->rtekind != RTE_RELATION ||
					rte->relkind != RELKIND_RELATION ||
					root->simple_rel_array[ref->rtindex] == NULL)
					break;

				Assert(!bms_is_member(ref->rtindex, relids));
				relids = bms_add_member(relids, ref->rtindex);

				/*
				 * Limit the number of joins we process to control the quadratic
				 * behavior.
				 */
				if (bms_num_members(relids) > join_collapse_limit)
					break;
			}
				break;
			default:
				Assert(false);
		}
	}

	if (bms_num_members(relids) >= 2)
		ToRemove = remove_self_joins_one_level(root, relids, ToRemove);

	bms_free(relids);
	return ToRemove;
}

/*
 * Find and remove useless self joins.
 *
 * We search for joins where the same relation is joined to itself on all
 * columns of some unique index. If this condition holds, then, for
 * each outer row, only one inner row matches, and it is the same row
 * of the same relation. This allows us to remove the join and replace
 * it with a scan that combines WHERE clauses from both sides. The join
 * clauses themselves assume the form of X = X and can be replaced with
 * NOT NULL clauses.
 *
 * For the sake of simplicity, we don't apply this optimization to special
 * joins. Here is a list of what we could do in some particular cases:
 * 'a a1 semi join a a2': is reduced to inner by reduce_unique_semijoins,
 * and then removed normally.
 * 'a a1 anti join a a2': could simplify to a scan with 'outer quals AND
 * (IS NULL on join columns OR NOT inner quals)'.
 * 'a a1 left join a a2': could simplify to a scan like inner, but without
 * NOT NULL conditions on join columns.
 * 'a a1 left join (a a2 join b)': can't simplify this, because join to b
 * can both remove rows and introduce duplicates.
 *
 * To search for removable joins, we order all the relations on their Oid,
 * go over each set with the same Oid, and consider each pair of relations
 * in this set. We check that both relation are made unique by the same
 * unique index with the same clauses.
 *
 * To remove the join, we mark one of the participating relations as
 * dead, and rewrite all references to it to point to the remaining
 * relation. This includes modifying RestrictInfos, EquivalenceClasses and
 * EquivalenceMembers. We also have to modify the row marks. The join clauses
 * of the removed relation become either restriction or join clauses, based on
 * whether they reference any relations not participating in the removed join.
 *
 * 'targetlist' is the top-level targetlist of query. If it has any references
 * to the removed relations, we update them to point to the remaining ones.
 */
List *
remove_useless_self_joins(PlannerInfo *root, List *joinlist)
{
	Relids ToRemove = NULL;
	int relid = -1;

	if (!enable_self_join_removal)
		return joinlist;

	/*
	 * Merge pairs of relations participated in self-join. Remove
	 * unnecessary range table entries.
	 */
	ToRemove = remove_self_joins_recurse(root, joinlist, ToRemove);
	while ((relid = bms_next_member(ToRemove, relid)) >= 0)
	{
		int nremoved = 0;
		joinlist = remove_rel_from_joinlist(joinlist, relid, &nremoved);
	}

	return joinlist;
}
