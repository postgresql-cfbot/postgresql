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
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
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
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/predtest.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "utils/lsyscache.h"

/* local functions */
static bool join_is_removable(PlannerInfo *root, SpecialJoinInfo *sjinfo);
static void remove_rel_from_query(PlannerInfo *root, int relid,
					  Relids joinrelids);
static List *remove_rel_from_joinlist(List *joinlist, int relid, int *nremoved);
static bool rel_supports_distinctness(PlannerInfo *root, RelOptInfo *rel);
static bool rel_is_distinct_for(PlannerInfo *root, RelOptInfo *rel,
					List *clause_list, UniqueIndexInfo **info);
static Oid	distinct_col_search(int colno, List *colnos, List *opids);
static bool is_innerrel_unique_for(PlannerInfo *root,
					   Relids joinrelids,
					   Relids outerrelids,
					   RelOptInfo *innerrel,
					   JoinType jointype,
					   List *restrictlist,
					   UniqueIndexInfo **info);


/*
 * remove_useless_joins
 *		Check for relations that don't actually need to be joined at all,
 *		and remove them from the query.
 *
 * We are passed the current joinlist and return the updated list.  Other
 * data structures that have to be updated are accessible via "root".
 */
List *
remove_useless_left_joins(PlannerInfo *root, List *joinlist)
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
										sjinfo->min_righthand));

		/* We verify that exactly one reference gets removed from joinlist */
		nremoved = 0;
		joinlist = remove_rel_from_joinlist(joinlist, innerrelid, &nremoved);
		if (nremoved != 1)
			elog(ERROR, "failed to find relation %d in joinlist", innerrelid);

		/*
		 * We can delete this SpecialJoinInfo from the list too, since it's no
		 * longer of interest.
		 */
		root->join_info_list = list_delete_ptr(root->join_info_list, sjinfo);

		/*
		 * Restart the scan.  This is necessary to ensure we find all
		 * removable joins independently of ordering of the join_info_list
		 * (note that removal of attr_needed bits may make a join appear
		 * removable that did not before).  Also, since we just deleted the
		 * current list cell, we'd have to have some kluge to continue the
		 * list scan anyway.
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
		if (bms_overlap(pull_varnos((Node *) phinfo->ph_var->phexpr),
						innerrel->relids))
			return false;		/* it does reference innerrel */
	}

	/*
	 * Check for pushed-down clauses referencing the inner rel. If there is
	 * such a clause then join removal has to be disallowed.  We have to
	 * check this despite the previous attr_needed checks because of the
	 * possibility of pushed-down clauses referencing the rel.
	 */
	foreach(l, innerrel->joininfo)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(l);
		if (RINFO_IS_PUSHED_DOWN(restrictinfo, joinrelids)
			&& bms_is_member(innerrel->relid, restrictinfo->clause_relids))
				return false;
	}

	return is_innerrel_unique_for(root, joinrelids, sjinfo->min_lefthand,
								  innerrel, sjinfo->jointype, innerrel->joininfo,
								  NULL /*unique_index*/);
}

/*
 * Remove the target relid from the planner's data structures, having
 * determined that there is no need to include it in the query.
 *
 * We are not terribly thorough here.  We must make sure that the rel is
 * no longer treated as a baserel, and that attributes of other baserels
 * are no longer marked as being needed at joins involving this rel.
 * Also, join quals involving the rel have to be removed from the joininfo
 * lists, but only if they belong to the outer join identified by joinrelids.
 */
static void
remove_rel_from_query(PlannerInfo *root, int relid, Relids joinrelids)
{
	RelOptInfo *rel = find_base_rel(root, relid);
	List	   *joininfos;
	Index		rti;
	ListCell   *l;
	ListCell   *nextl;

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
				bms_del_member(otherrel->attr_needed[attroff], relid);
		}
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

		sjinfo->min_lefthand = bms_del_member(sjinfo->min_lefthand, relid);
		sjinfo->min_righthand = bms_del_member(sjinfo->min_righthand, relid);
		sjinfo->syn_lefthand = bms_del_member(sjinfo->syn_lefthand, relid);
		sjinfo->syn_righthand = bms_del_member(sjinfo->syn_righthand, relid);
	}

	/*
	 * Likewise remove references from PlaceHolderVar data structures,
	 * removing any no-longer-needed placeholders entirely.
	 *
	 * Removal is a bit tricker than it might seem: we can remove PHVs that
	 * are used at the target rel and/or in the join qual, but not those that
	 * are used at join partner rels or above the join.  It's not that easy to
	 * distinguish PHVs used at partner rels from those used in the join qual,
	 * since they will both have ph_needed sets that are subsets of
	 * joinrelids.  However, a PHV used at a partner rel could not have the
	 * target rel in ph_eval_at, so we check that while deciding whether to
	 * remove or just update the PHV.  There is no corresponding test in
	 * join_is_removable because it doesn't need to distinguish those cases.
	 */
	for (l = list_head(root->placeholder_list); l != NULL; l = nextl)
	{
		PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(l);

		nextl = lnext(l);
		Assert(!bms_is_member(relid, phinfo->ph_lateral));
		if (bms_is_subset(phinfo->ph_needed, joinrelids) &&
			bms_is_member(relid, phinfo->ph_eval_at))
			root->placeholder_list = list_delete_ptr(root->placeholder_list,
													 phinfo);
		else
		{
			phinfo->ph_eval_at = bms_del_member(phinfo->ph_eval_at, relid);
			Assert(!bms_is_empty(phinfo->ph_eval_at));
			phinfo->ph_needed = bms_del_member(phinfo->ph_needed, relid);
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
	ListCell   *next;

	/*
	 * Scan the join_info_list to find semijoins.  We can't use foreach
	 * because we may delete the current cell.
	 */
	for (lc = list_head(root->join_info_list); lc != NULL; lc = next)
	{
		SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) lfirst(lc);
		int			innerrelid;
		RelOptInfo *innerrel;
		Relids		joinrelids;
		List	   *restrictlist;

		next = lnext(lc);

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
								JOIN_SEMI, restrictlist, true, NULL /*index_info*/))
			continue;

		/* OK, remove the SpecialJoinInfo from the list. */
		root->join_info_list = list_delete_ptr(root->join_info_list, sjinfo);
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
 *
 * If unique_index is not null, it is set to point to the index that guarantees
 * uniqueness for a base relation.
 */
static bool
rel_is_distinct_for(PlannerInfo *root, RelOptInfo *rel, List *clause_list,
					UniqueIndexInfo **index_info)
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
		return relation_has_unique_index_for(root, rel, clause_list, NIL, NIL,
											 index_info);
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
				lg = lnext(lg);

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
 *
 * If index_info_out is not null, it is set to point to a new UniqueIndexInfo
 * allocated in root memory context, that describes the index that guarantees
 * uniqueness.
 */
bool
innerrel_is_unique(PlannerInfo *root,
				   Relids joinrelids,
				   Relids outerrelids,
				   RelOptInfo *innerrel,
				   JoinType jointype,
				   List *restrictlist,
				   bool force_cache,
				   UniqueIndexInfo **index_info_out)
{
	MemoryContext old_context;
	ListCell   *lc;
	UniqueIndexInfo *index_info;

	if (index_info_out)
		*index_info_out = NULL;

	/*
	 * It is possible to prove uniqueness even in the absence of joinclauses,
	 * just from baserestrictinfos alone. However, in these cases the inner
	 * relation returns one row at most, so join removal won't give much
	 * benefit. It seems better to save some planning time by ignoring these
	 * cases.
	 */
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
		Relids		unique_for_rels = (Relids) linitial(lfirst(lc));

		if (bms_is_subset(unique_for_rels, outerrelids))
		{
			if (index_info_out)
				*index_info_out = lsecond(lfirst(lc));
			return true;		/* Success! */
		}
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
							   jointype, restrictlist, &index_info))
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
							list_make2(bms_copy(outerrelids), index_info));
		MemoryContextSwitchTo(old_context);

		if (index_info_out)
			*index_info_out = index_info;

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
					   List *restrictlist,
					   UniqueIndexInfo **index_info)
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
	return rel_is_distinct_for(root, innerrel, clause_list, index_info);
}

typedef struct
{
	Index oldRelid;
	Index newRelid;
	bool found;
} ChangeVarnoContext;

static bool
change_varno_walker(Node *node, ChangeVarnoContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var) && ((Var *) node)->varno == context->oldRelid)
	{
		((Var *) node)->varno = context->newRelid;
		context->found = true;
		return false;
	}

	return expression_tree_walker(node, change_varno_walker, context);
}

/*
 * For all Vars in the expression that have varno = oldRelid, set
 * varno = newRelid.
 * Return whether any changes were made.
 */
static bool
change_varno(Expr *expr, Index oldRelid, Index newRelid)
{
	ChangeVarnoContext context;
	context.oldRelid = oldRelid;
	context.newRelid = newRelid;
	context.found = false;
	change_varno_walker((Node *) expr, &context);
	return context.found;
}

/*
 * Substitute newId for oldId in relids.
 */
static void
change_relid(Relids *relids, Index oldId, Index newId)
{
	if (bms_is_member(oldId, *relids))
		*relids = bms_add_member(bms_del_member(*relids, oldId), newId);
}

/*
 * Move EC members from the removed relation to the remaining one,
 * removing duplicates.
 */
static void
move_ec_members(EquivalenceClass *ec, Index toRemove, Index toKeep)
{
	ListCell *prev = NULL;
	ListCell *cell = NULL;
	ListCell *next = list_head(ec->ec_members);

	while (next)
	{
		EquivalenceMember *em;

		prev = cell;
		cell = next;
		next = lnext(next);

		em = lfirst(cell);
		if (change_varno(em->em_expr, toRemove, toKeep))
		{
			/*
			 * If we transferred the equivalence member to another relation,
			 * check that is not the same as the existing member, and if it
			 * is, delete it.
			 */
			ListCell *lc;
			foreach (lc, ec->ec_members)
			{
				if (lc == cell)
					continue;

				if (equal(((EquivalenceMember *) lfirst(lc))->em_expr,
						  em->em_expr))
				{
					ec->ec_members = list_delete_cell(ec->ec_members, cell, prev);
					cell = prev;
					break;
				}
			}

			if (lc == NULL)
			{
				/*
				 * We get to keep this EquivalenceMember. Correct its relids.
				 * nullable_relids should be empty, because self join removal
				 * only works for inner joins.
				 */
				Assert(em->em_nullable_relids == NULL);
				change_relid(&em->em_relids, toRemove, toKeep);
			}
		}
	}
}

/*
 * Remove EC sources referencing given relation.
 */
static void
filter_ec_sources(List **sources, Index relToRemove)
{
	ListCell *prev = NULL;
	ListCell *cell = NULL;
	ListCell *next = list_head(*sources);

	while (next)
	{
		RestrictInfo *rinfo;

		prev = cell;
		cell = next;
		next = lnext(next);

		rinfo = castNode(RestrictInfo, lfirst(cell));

		if (bms_is_member(relToRemove, rinfo->required_relids))
		{
			*sources = list_delete_cell(*sources, cell, prev);
			cell = prev;
		}
	}
}

/*
 * Scratch space for the unique self join removal code.
 */
typedef struct
{
	PlannerInfo *root;

	/* Temporary array for relation ids. */
	Index *relids;

	/*
	 * Array of Relids, one for each relation, indexed by relation id.
	 * Each element is a set of relation ids with which this relation
	 * has a special join.
	 */
	Relids *special_join_rels;

	/* Array of row marks indexed by relid. */
	PlanRowMark **row_marks;

	/* Bitmapset for join relids that is used to avoid reallocation. */
	Relids joinrelids;

	/*
	 * Top-level targetlist of the query. We have to update any references
	 * it has to the relations we remove.
	 */
	 List *targetlist;
} UsjScratch;

/*
 * Remove a relation after we have proven that it participates only in an
 * unneeded unique self join.
 *
 * The joinclauses list is destructively changed.
 */
static void
remove_self_join_rel(UsjScratch *scratch, Relids joinrelids, List *joinclauses,
					 RelOptInfo *toKeep, RelOptInfo *toRemove)
{
	PlannerInfo *root = scratch->root;
	ListCell *prev, *cell, *next;
	List *toAppend;
	int i;

	/*
	 * Transfer join and restriction clauses from the removed relation to the
	 * remaining one. We change the Vars of the clause to point to the remaining
	 * relation instead of the removed one. The clauses that require a subset of
	 * joinrelids become restriction clauses of the remaining relation, and
	 * others remain join clauses. We append them to baserestrictinfo and
	 * joininfo respectively, trying not to introduce duplicates.
	 *
	 * We also have to process the 'joinclauses' list here, because it contains
	 * EC-derived join clauses which must become filter clauses. It is not enough
	 * to just correct the ECs, because the EC-derived restrictions are generated
	 * before join removal (see generate_base_implied_equalities).
	 */
	toAppend = list_concat(joinclauses, toRemove->baserestrictinfo);
	toAppend = list_concat(toAppend, toRemove->joininfo);

	foreach(cell, toAppend)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, cell);
		bool is_join_clause = !bms_is_subset(rinfo->required_relids, joinrelids);
		List **target = is_join_clause ? &toKeep->joininfo : &toKeep->baserestrictinfo;

		/* We can't have an EC-derived clause that joins to some third relation */
		Assert( !(is_join_clause && rinfo->parent_ec != NULL) );

		/*
		 * Do not add multiple clauses derived from the same equivalence class.
		 */
		if (is_redundant_derived_clause(rinfo, *target))
			continue;

		/*
		 * Replace the references to the removed relation with references to
		 * the remaining one.
		 */
		change_varno(rinfo->clause, toRemove->relid, toKeep->relid);

		/*
		 * After we have replaced the Vars, check that the resulting clause is
		 * not implied by the existing ones.
		 */
		if (!contain_mutable_functions((Node *) rinfo->clause)
			&& predicate_implied_by(list_make1(rinfo->clause),
									*target, false /*weak*/ ))
			continue;			/* provably implied by r1 */

		/*
		 * If the clause has the form of "X=X", replace it with null test.
		 */
		if (rinfo->mergeopfamilies)
		{
			Expr *leftOp = (Expr *) get_leftop(rinfo->clause);
			Expr *rightOp = (Expr *) get_rightop(rinfo->clause);

			if (leftOp != NULL && equal(leftOp, rightOp))
			{
				NullTest *test = makeNode(NullTest);
				test->arg = leftOp;
				test->nulltesttype = IS_NOT_NULL;
				test->argisrow = false;
				test->location = -1;
				rinfo->clause = (Expr *) test;
			}
		}

		/*
		 * Finally, correct the relids of the rinfo, replace the clause with
		 * the one we just constructed, and append it to the remaining relation.
		 */
		change_relid(&rinfo->required_relids, toRemove->relid, toKeep->relid);
		change_relid(&rinfo->left_relids, toRemove->relid, toKeep->relid);
		change_relid(&rinfo->right_relids, toRemove->relid, toKeep->relid);
		change_relid(&rinfo->clause_relids, toRemove->relid, toKeep->relid);

		*target = lappend(*target, rinfo);
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
			toKeep->reltarget->exprs = lappend(toKeep->reltarget->exprs,
											   node);
	}

	for (i = toKeep->min_attr; i <= toKeep->max_attr; i++)
	{
		int attno = i - toKeep->min_attr;
		toKeep->attr_needed[attno] = bms_add_members(toKeep->attr_needed[attno],
													 toRemove->attr_needed[attno]);
	}
	
	/*
	 * If the removed relation has a row mark, transfer it to the remaining one.
	 *
	 * If both rels have row marks, just keep the one corresponding to the
	 * remaining relation, because we verified earlier that they have the same
	 * strength.
	 *
	 * Also make sure that the scratch->row_marks cache is up to date, because
	 * we are going to use it for further join removals.
	 */
	if (scratch->row_marks[toRemove->relid])
	{
		PlanRowMark **markToRemove = &scratch->row_marks[toRemove->relid];
		PlanRowMark **markToKeep = &scratch->row_marks[toKeep->relid];
		if (*markToKeep)
		{
			Assert((*markToKeep)->markType == (*markToRemove)->markType);

			root->rowMarks = list_delete_ptr(root->rowMarks, *markToKeep);
			*markToKeep = NULL;
		}
		else
		{
			*markToKeep = *markToRemove;
			*markToRemove = NULL;

			/* Shouldn't have inheritance children here. */
			Assert((*markToKeep)->rti == (*markToKeep)->prti);

			(*markToKeep)->rti = toKeep->relid;
			(*markToKeep)->prti = toKeep->relid;
		}
	}

	/*
	 * Likewise remove references from SpecialJoinInfo data structures.
	 *
	 * This is relevant in case the outer join we're deleting is nested inside
	 * other outer joins: the upper joins' relid sets have to be adjusted. The
	 * RHS of the target outer join will be made empty here, but that's OK
	 * since caller will delete that SpecialJoinInfo entirely.
	 */
	foreach (cell, root->join_info_list)
	{
		SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) lfirst(cell);

		sjinfo->min_lefthand = bms_del_member(sjinfo->min_lefthand, toRemove->relid);
		sjinfo->min_righthand = bms_del_member(sjinfo->min_righthand, toRemove->relid);
		sjinfo->syn_lefthand = bms_del_member(sjinfo->syn_lefthand, toRemove->relid);
		sjinfo->syn_righthand = bms_del_member(sjinfo->syn_righthand, toRemove->relid);
	}


	// !!!FIXME what about placeholders and upper-level tlists (e.g. for grouping)?
	// The placeholders apparently work somehow due to the fact that they reference
	// the same Var objects that we modify to point to the other relation.

	/*
	 * We must move the equivalence members that reference the removed relation
	 * to the remaining one, being careful not to introduce duplicate members.
	 * If a EC contains one member and is not used for sorting, it can be removed
	 * altogether.
	 */
	prev = NULL;
	cell = NULL;
	next = list_head(root->eq_classes);
	while (next)
	{
		EquivalenceClass *ec;

		prev = cell;
		cell = next;
		next = lnext(next);

		ec = lfirst(cell);

		if (!bms_is_member(toRemove->relid, ec->ec_relids))
		{
			/*
			 * This EC doesn't reference the removed relation,
			 * nothing to be done for it.
			 */
			continue;
		}

		/*
		 * Update the EC to reference the remaining relation instead
		 * of the removed one.
		 */
		move_ec_members(ec, toRemove->relid, toKeep->relid);
		change_relid(&ec->ec_relids, toRemove->relid, toKeep->relid);

		if (ec->ec_sortref == 0 && list_length(ec->ec_members) <= 1)
		{
			/*
			 * This EC is not used for sorting and contains one member.
			 * It won't generate any join clauses and can be removed.
			 */
			root->eq_classes = list_delete_cell(root->eq_classes, cell, prev);
			cell = prev;
		}
		else
		{
			/*
			 * The updated EC should be kept.
			 *
			 * Some of its source and derived RestrictInfos point to the removed
			 * relation. There is no straightforward way to determine whether
			 * such a RestrictInfo should be removed or switched to the
			 * remaining relation. We remove all of them, and will generate
			 * the correct ones from equivalence members on demand.
			 *
			 * This is not important for the ECs that are only used for sorting,
			 * but we process them too for the sake of consistency.
			 */
			filter_ec_sources(&ec->ec_sources, toRemove->relid);
			filter_ec_sources(&ec->ec_derives, toRemove->relid);
			ec->ec_relids = bms_del_member(ec->ec_relids, toRemove->relid);
		}
	}

	/*
	 * Mark the rel as "dead" to show it is no longer part of the join tree.
	 * (Removing it from the baserel array altogether seems too risky.)
	 */
	toRemove->reloptkind = RELOPT_DEADREL;

	/*
	 * Remove references to the rel from other baserels' attr_needed arrays.
	 */
	for (i = 1; i < root->simple_rel_array_size; i++)
	{
		RelOptInfo *otherrel = root->simple_rel_array[i];
		int			attroff;

		/* no point in processing target rel itself */
		if (i == toRemove->relid)
			continue;

		/* there may be empty slots corresponding to non-baserel RTEs */
		if (otherrel == NULL)
			continue;

		Assert(otherrel->relid == i); /* sanity check on array */

		for (attroff = otherrel->max_attr - otherrel->min_attr;
			 attroff >= 0;
			 attroff--)
		{
			otherrel->attr_needed[attroff] =
				bms_del_member(otherrel->attr_needed[attroff], toRemove->relid);
		}
	}
}

/*
 * Test whether the relations are joined on the same unique column.
 */
static bool
is_unique_self_join(PlannerInfo *root, Relids joinrelids, RelOptInfo *outer,
					RelOptInfo *inner, List *restrictlist)
{
	UniqueIndexInfo *outeridx = NULL;
	UniqueIndexInfo *inneridx = NULL;
	ListCell *outerCell, *innerCell;

	innerrel_is_unique(root, joinrelids, inner->relids,
						   outer, JOIN_INNER, restrictlist, true, &outeridx);
	if (!outeridx)
		return false;

	innerrel_is_unique(root, joinrelids, outer->relids,
						   inner, JOIN_INNER, restrictlist, true, &inneridx);
	if (!inneridx)
		return false;

	/* We must have the same unique index for both relations. */
	if (outeridx->index->indexoid != inneridx->index->indexoid)
		return false;

	/*
	 * The index clauses must also be the same. The varnos are different, so
	 * make a copy and replace all varnos of one relation with another, so
	 * that we can compare them with equal().
	 */
	forboth(innerCell, inneridx->clauses, outerCell, outeridx->clauses)
	{
		Expr *innerExpr = copyObject(castNode(RestrictInfo, lfirst(innerCell))->clause);
		Expr *outerExpr = copyObject(castNode(RestrictInfo, lfirst(outerCell))->clause);
		change_varno(outerExpr, outer->relid, inner->relid);
		change_varno(innerExpr, outer->relid, inner->relid);
		if (!equal(outerExpr, innerExpr))
		{
			pfree(outerExpr);
			pfree(innerExpr);
			return false;
		}
		pfree(outerExpr);
		pfree(innerExpr);
	}

	return true;
}

/*
 * Find and remove unique self joins in a group of base relations that have
 * the same Oid.
 *
 * Returns IntList of the relids that were removed.
 */
static List *
remove_self_joins_one_group(UsjScratch *scratch, Index *relids, int n)
{
	PlannerInfo *root = scratch->root;
	Relids joinrelids = scratch->joinrelids;
	List *result = NIL;
	int i, o;
	ListCell *lc;

	if (n < 2)
		return NIL;

	for (o = 0; o < n; o++)
	{
		RelOptInfo *outer = root->simple_rel_array[relids[o]];

		for (i = o + 1; i < n; i++)
		{
			RelOptInfo *inner = root->simple_rel_array[relids[i]];
			List *restrictlist;

			/* A sanity check: the relations have the same Oid. */
			Assert(root->simple_rte_array[relids[i]]->relid
					== root->simple_rte_array[relids[o]]->relid);

			/*
			 * This optimization applies to inner joins only, so skip any relations
			 * that form a special join.
			 */
			if (bms_is_member(relids[i], scratch->special_join_rels[relids[o]]))
				continue;

			/* Reuse joinrelids bitset to avoid reallocation. */
			joinrelids = bms_del_members(joinrelids, joinrelids);

			/*
			 * We only deal with base rels here, so their relids bitset
			 * contains only one member -- their relid.
			 */
			joinrelids = bms_add_member(joinrelids, relids[o]);
			joinrelids = bms_add_member(joinrelids, relids[i]);

			/* Is it a unique self join? */
			restrictlist = build_joinrel_restrictlist(root, joinrelids, outer,
													  inner);
			if (!is_unique_self_join(root, joinrelids, outer, inner,
										   restrictlist))
				continue;

			/*
			 * We can't remove the join if the relations have row marks of
			 * different strength (e.g. one is locked FOR UPDATE and another
			 * just has ROW_MARK_REFERENCE for EvalPlanQual rechecking).
			 */
			if (scratch->row_marks[relids[i]] && scratch->row_marks[relids[o]]
				&& scratch->row_marks[relids[i]]->markType
					!= scratch->row_marks[relids[o]]->markType)
			{
				continue;
			}

			/*
			 * We can remove either relation, so remove the outer one,
			 * to simplify this loop.
			 */
			remove_self_join_rel(scratch, joinrelids, restrictlist, inner, outer);
			result = lappend_int(result, relids[o]);

			/*
			 * Replace varno in root targetlist.
			 */
			foreach(lc, scratch->targetlist)
				change_varno(lfirst(lc), relids[o], relids[i]);

			/* We removed the outer relation, try the next one. */
			break;
		}
	}

	scratch->joinrelids = joinrelids;
	return result;
}

/*
 * A qsort comparator to sort the relids by the relation Oid.
 */
static int
compare_rte(const Index *left, const Index *right, PlannerInfo *root)
{
	return root->simple_rte_array[*left]->relid
		< root->simple_rte_array[*right]->relid;
}

/*
 * Find and remove unique self joins on a particular level of the join tree.
 *
 * We sort the relations by Oid and then examine each group with the same Oid.
 * If we removed any relation, remove it from joinlist as well.
 */
static void
remove_self_joins_one_level(UsjScratch *scratch, List **joinlist)
{
	ListCell *lc;
	List *relidsToRemove = NIL;
	Oid groupOid;
	int groupStart;
	int i;
	int n = 0;
	Index *relid_ascending = scratch->relids;
	PlannerInfo *root = scratch->root;

	/*
	 * Collect the ids of base relations at this level of the join tree.
	 */
	foreach (lc, *joinlist)
	{
		RangeTblEntry *rte;
		RelOptInfo *rel;
		RangeTblRef *ref = (RangeTblRef *) lfirst(lc);
		if (!IsA(ref, RangeTblRef))
			continue;

		rte = root->simple_rte_array[ref->rtindex];
		rel = root->simple_rel_array[ref->rtindex];

		/* We only care about base relations from which we select something. */
		if (rte->rtekind != RTE_RELATION || rte->relkind != RELKIND_RELATION
			|| rel == NULL)
		{
			continue;
		}

		/* This optimization won't work for tables that have inheritance children. */
		if (rte->inh)
			continue;

		relid_ascending[n++] = ref->rtindex;

		/* Limit the number of joins we process to control the quadratic behavior. */
		if (n > join_collapse_limit)
			break;
	}

	if (n < 2)
		return;

	/*
	 * Find and process the groups of relations that have same Oid.
	 */
	qsort_arg(relid_ascending, n, sizeof(*relid_ascending),
			  (qsort_arg_comparator) compare_rte, root);
	groupOid = root->simple_rte_array[relid_ascending[0]]->relid;
	groupStart = 0;
	for (i = 1; i < n; i++)
	{
		RangeTblEntry *rte = root->simple_rte_array[relid_ascending[i]];
		Assert(rte->relid != InvalidOid);
		if (rte->relid != groupOid)
		{
			relidsToRemove = list_concat(relidsToRemove,
				remove_self_joins_one_group(scratch, &relid_ascending[groupStart],
					i - groupStart));
			groupOid = rte->relid;
			groupStart = i;
		}
	}
	Assert(groupOid != InvalidOid);
	Assert(groupStart < n);
	relidsToRemove = list_concat(relidsToRemove,
		remove_self_joins_one_group(scratch, &relid_ascending[groupStart],
			n - groupStart));

	/*
	 * Delete the removed relations from joinlist.
	 */
	foreach(lc, relidsToRemove)
	{
		Index indexToRemove = lfirst_int(lc);
		ListCell *prev = NULL, *next = NULL;
		ListCell *lc2 = list_head(*joinlist);
		while (lc2)
		{
			next = lnext(lc2);
			if (castNode(RangeTblRef, lfirst(lc2))->rtindex == indexToRemove)
				*joinlist = list_delete_cell(*joinlist, lc2, prev);
			else
				prev = lc2;
			lc2 = next;
		}
	}

	return;
}

/*
 * Find and remove unique self joins on a single level of a join tree, and
 * recurse to handle deeper levels.
 */
static void
remove_self_joins_recurse(UsjScratch *scratch, List **joinlist)
{
	ListCell *lc;
	foreach (lc, *joinlist)
	{
		switch (((Node*) lfirst(lc))->type)
		{
			case T_List:
				remove_self_joins_recurse(scratch, (List **) &lfirst(lc));
				break;
			case T_RangeTblRef:
				break;
			default:
				Assert(false);
		}
	}
	remove_self_joins_one_level(scratch, joinlist);
}

/*
 * Find and remove unique self joins in the entire join tree.
 *
 * We try to find joins where the same physical relation is joined to
 * itself on all the columns of a unique index. Such joins can be
 * can be replaced with a scan using the combined filters.
 *
 * When we wind such a join, we mark one of the participating relation as
 * dead, and rewrite all references to it to point to the remaining
 * relation.
 *
 * 'targetlist' is the top-level targetlist of query. We fix any references
 * it has to the relations we remove.
 */
void
remove_useless_self_joins(PlannerInfo *root, List **joinlist, List *targetlist)
{
	ListCell *lc;
	UsjScratch scratch;

	scratch.root = root;
	scratch.relids = palloc(root->simple_rel_array_size * sizeof(Index));
	scratch.special_join_rels = palloc0(root->simple_rel_array_size * sizeof(Relids));
	scratch.row_marks = palloc0(root->simple_rel_array_size * sizeof(PlanRowMark *));
	scratch.joinrelids = NULL;
	scratch.targetlist = targetlist;

	/* Find out which relations have special joins to which. */
	foreach(lc, root->join_info_list)
	{
		SpecialJoinInfo *info = (SpecialJoinInfo *) lfirst(lc);
		int bit = -1;
		while ((bit = bms_next_member(info->min_lefthand, bit)) >= 0)
		{
			RelOptInfo *rel = find_base_rel(root, bit);
			scratch.special_join_rels[rel->relid] =
				bms_add_members(scratch.special_join_rels[rel->relid],
					info->min_righthand);
		}

		bit = -1;
		while ((bit = bms_next_member(info->min_righthand, bit)) >= 0)
		{
			RelOptInfo *rel = find_base_rel(root, bit);
			scratch.special_join_rels[rel->relid] =
				bms_add_members(scratch.special_join_rels[rel->relid],
					info->min_lefthand);
		}
	}

	/* Collect row marks. */
	foreach (lc, root->rowMarks)
	{
		PlanRowMark *rowMark = (PlanRowMark *) lfirst(lc);

		/* Can't have more than one row mark for a relation. */
		Assert(scratch.row_marks[rowMark->rti] == NULL);

		scratch.row_marks[rowMark->rti] = rowMark;
	}

	/* Finally, remove the joins. */
	remove_self_joins_recurse(&scratch, joinlist);
}
