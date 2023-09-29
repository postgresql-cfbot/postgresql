/*-------------------------------------------------------------------------
 *
 * orclauses.c
 *	  Routines to extract restriction OR clauses from join OR clauses
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/orclauses.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/orclauses.h"
#include "optimizer/restrictinfo.h"
#include "utils/lsyscache.h"
#include "parser/parse_expr.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"


static bool is_safe_restriction_clause_for(RestrictInfo *rinfo, RelOptInfo *rel);
static Expr *extract_or_clause(RestrictInfo *or_rinfo, RelOptInfo *rel);
static void consider_new_or_clause(PlannerInfo *root, RelOptInfo *rel,
								   Expr *orclause, RestrictInfo *join_or_rinfo);


int			or_transform_limit = 2;

typedef struct OrClauseGroupEntry
{
	Node		   *node;
	List		   *consts;
	Oid				scalar_type;
	Oid				opno;
	Expr 		   *expr;
	RestrictInfo   *rinfo;
} OrClauseGroupEntry;

static List *
transform_ors(PlannerInfo *root, List *baserestrictinfo)
{
	ListCell	   *lc_clause;
	List	   	   *modified_rinfo = NIL;
	bool		    something_changed = false;


	foreach (lc_clause, baserestrictinfo)
	{
		RestrictInfo   	   *rinfo = lfirst_node(RestrictInfo, lc_clause);
		RestrictInfo	   *rinfo_base = copyObject(rinfo);
		List		   	   *groups_list = NIL;
		List		       *or_list = NIL;
		ListCell	   	   *lc_eargs,
					   	   *lc_rargs;

		if (!restriction_is_or_clause(rinfo) ||
			list_length(((BoolExpr *) rinfo->clause)->args) < or_transform_limit)
		{
			/* Add a clause without changes */
			modified_rinfo = lappend(modified_rinfo, rinfo);
			continue;
		}
		forboth(lc_eargs, ((BoolExpr *) rinfo->clause)->args,
				lc_rargs, ((BoolExpr *) rinfo->orclause)->args)
		{
			Expr			   *orqual = (Expr *) lfirst(lc_eargs);
			Node			   *const_expr;
			Node			   *nconst_expr;
			ListCell		   *lc_groups;
			OrClauseGroupEntry *gentry;
			RestrictInfo	   *sub_rinfo;

			/* If this is not an 'OR' expression, skip the transformation */
			if (!IsA(lfirst(lc_rargs), RestrictInfo))
			{
				or_list = lappend(or_list, orqual);
				continue;
			}
			sub_rinfo = lfirst_node(RestrictInfo, lc_rargs);

			/* Check: it is an expr of the form 'F(x) oper ConstExpr' */
			if (!IsA(orqual, OpExpr) ||
			    !(bms_is_empty(sub_rinfo->left_relids) ^
				bms_is_empty(sub_rinfo->right_relids)) ||
				contain_volatile_functions((Node *) orqual))
			{
				/* Again, it's not the expr we can transform */
				or_list = lappend(or_list, (void *) orqual);
				continue;
			}


			/*
			* Detect the constant side of the clause. Recall non-constant
			* expression can be made not only with Vars, but also with Params,
			* which is not bonded with any relation. Thus, we detect the const
			* side - if another side is constant too, the orqual couldn't be
			* an OpExpr.
			* Get pointers to constant and expression sides of the qual.
			*/
			const_expr =bms_is_empty(sub_rinfo->left_relids) ?
												get_leftop(sub_rinfo->clause) :
												get_rightop(sub_rinfo->clause);
			nconst_expr = bms_is_empty(sub_rinfo->left_relids) ?
												get_rightop(sub_rinfo->clause) :
												get_leftop(sub_rinfo->clause);

			if (!op_mergejoinable(((OpExpr *) sub_rinfo->clause)->opno, exprType(nconst_expr)))
			{
				or_list = lappend(or_list, (void *) orqual);
				continue;
			}

			/*
			* At this point we definitely have a transformable clause.
			* Classify it and add into specific group of clauses, or create new
			* group.
			* TODO: to manage complexity in the case of many different clauses
			* (X1=C1) OR (X2=C2 OR) ... (XN = CN) we could invent something
			* like a hash table. But also we believe, that the case of many
			* different variable sides is very rare.
			*/
			foreach(lc_groups, groups_list)
			{
				OrClauseGroupEntry *v = (OrClauseGroupEntry *) lfirst(lc_groups);

				Assert(v->node != NULL);

				if (equal(v->node, nconst_expr))
				{
					v->consts = lappend(v->consts, const_expr);
					nconst_expr = NULL;
					break;
				}
			}

			if (nconst_expr == NULL)
				/*
					* The clause classified successfully and added into existed
					* clause group.
					*/
				continue;

			/* New clause group needed */
			gentry = palloc(sizeof(OrClauseGroupEntry));
			gentry->node = nconst_expr;
			gentry->consts = list_make1(const_expr);
			gentry->rinfo = sub_rinfo;
			groups_list = lappend(groups_list,  (void *) gentry);
		}

		if (groups_list == NIL)
		{
			/*
			* No any transformations possible with this list of arguments. Here we
			* already made all underlying transformations. Thus, just return the
			* transformed bool expression.
			*/
			modified_rinfo = lappend(modified_rinfo, rinfo);
			continue;
		}
		else
		{
			ListCell	   *lc_args;

			/* Let's convert each group of clauses to an IN operation. */

			/*
			* Go through the list of groups and convert each, where number of
			* consts more than 1. trivial groups move to OR-list again
			*/

			foreach(lc_args, groups_list)
			{
				List			   *allexprs;
				Oid				    scalar_type;
				Oid					array_type;
				OrClauseGroupEntry *gentry = (OrClauseGroupEntry *) lfirst(lc_args);

				Assert(list_length(gentry->consts) > 0);

				if (list_length(gentry->consts) == 1)
				{
					/*
					* Only one element in the class. Return rinfo into the BoolExpr
					* args list unchanged.
					*/
					list_free(gentry->consts);
					or_list = lappend(or_list, gentry->rinfo->clause);
					continue;
				}

				/*
				* Do the transformation.
				*
				* First of all, try to select a common type for the array elements.
				* Note that since the LHS' type is first in the list, it will be
				* preferred when there is doubt (eg, when all the RHS items are
				* unknown literals).
				*
				* Note: use list_concat here not lcons, to avoid damaging rnonvars.
				*
				* As a source of insides, use make_scalar_array_op()
				*/
				allexprs = list_concat(list_make1(gentry->node), gentry->consts);
				scalar_type = select_common_type(NULL, allexprs, NULL, NULL);

				if (scalar_type != RECORDOID && OidIsValid(scalar_type))
					array_type = get_array_type(scalar_type);
				else
					array_type = InvalidOid;

				if (array_type != InvalidOid && scalar_type != InvalidOid)
				{
					/*
					* OK: coerce all the right-hand non-Var inputs to the common
					* type and build an ArrayExpr for them.
					*/
					List	   *aexprs;
					ArrayExpr  *newa;
					ScalarArrayOpExpr *saopexpr;
					ListCell *l;

					aexprs = NIL;

					foreach(l, gentry->consts)
					{
						Node	   *rexpr = (Node *) lfirst(l);

						rexpr = coerce_to_common_type(NULL, rexpr,
													scalar_type,
													"IN");
						aexprs = lappend(aexprs, rexpr);
					}

					newa = makeNode(ArrayExpr);
					/* array_collid will be set by parse_collate.c */
					newa->element_typeid = scalar_type;
					newa->array_typeid = array_type;
					newa->multidims = false;
					newa->elements = aexprs;
					newa->location = -1;

					saopexpr =
						(ScalarArrayOpExpr *)
							make_scalar_array_op(NULL,
												list_make1(makeString((char *) "=")),
												true,
												gentry->node,
												(Node *) newa,
												-1);
					saopexpr->inputcollid = exprInputCollation((Node *)gentry->rinfo->clause);

					or_list = lappend(or_list, (void *) saopexpr);

					something_changed = true;
				}
				else
				{
					/*
					* Each group contains only one element - use rinfo as is.
					*/
					list_free(gentry->consts);
					or_list = lappend(or_list, gentry->expr);
					continue;
				}

				/*
				* Make a new version of the restriction. Remember source restriction
				* can be used in another path (SeqScan, for example).
				*/

				/* One more trick: assemble correct clause */
				rinfo = make_restrictinfo(root,
						list_length(or_list) > 1 ? make_orclause(or_list) :
													(Expr *) linitial(or_list),
						rinfo->has_clone,
						rinfo->is_clone,
						rinfo->is_pushed_down,
						rinfo->pseudoconstant,
						rinfo->security_level,
						rinfo->required_relids,
						rinfo->outer_relids,
						rinfo->outer_relids);
				rinfo->eval_cost=rinfo_base->eval_cost;
				rinfo->norm_selec=rinfo_base->norm_selec;
				rinfo->outer_selec=rinfo_base->outer_selec;
				rinfo->left_bucketsize=rinfo_base->left_bucketsize;
				rinfo->right_bucketsize=rinfo_base->right_bucketsize;
				rinfo->left_mcvfreq=rinfo_base->left_mcvfreq;
				rinfo->right_mcvfreq=rinfo_base->right_mcvfreq;
				modified_rinfo = lappend(modified_rinfo, rinfo);
				something_changed = true;
			}
		}
		list_free(or_list);
		list_free_deep(groups_list);
	}

	/*
		* Check if transformation has made. If nothing changed - return
		* baserestrictinfo as is.
		*/
	if (something_changed)
	{
		return modified_rinfo;
	}

	list_free(modified_rinfo);
	return baserestrictinfo;
}

/*
 * extract_restriction_or_clauses
 *	  Examine join OR-of-AND clauses to see if any useful restriction OR
 *	  clauses can be extracted.  If so, add them to the query.
 *
 * Although a join clause must reference multiple relations overall,
 * an OR of ANDs clause might contain sub-clauses that reference just one
 * relation and can be used to build a restriction clause for that rel.
 * For example consider
 *		WHERE ((a.x = 42 AND b.y = 43) OR (a.x = 44 AND b.z = 45));
 * We can transform this into
 *		WHERE ((a.x = 42 AND b.y = 43) OR (a.x = 44 AND b.z = 45))
 *			AND (a.x = 42 OR a.x = 44)
 *			AND (b.y = 43 OR b.z = 45);
 * which allows the latter clauses to be applied during the scans of a and b,
 * perhaps as index qualifications, and in any case reducing the number of
 * rows arriving at the join.  In essence this is a partial transformation to
 * CNF (AND of ORs format).  It is not complete, however, because we do not
 * unravel the original OR --- doing so would usually bloat the qualification
 * expression to little gain.
 *
 * The added quals are partially redundant with the original OR, and therefore
 * would cause the size of the joinrel to be underestimated when it is finally
 * formed.  (This would be true of a full transformation to CNF as well; the
 * fault is not really in the transformation, but in clauselist_selectivity's
 * inability to recognize redundant conditions.)  We can compensate for this
 * redundancy by changing the cached selectivity of the original OR clause,
 * canceling out the (valid) reduction in the estimated sizes of the base
 * relations so that the estimated joinrel size remains the same.  This is
 * a MAJOR HACK: it depends on the fact that clause selectivities are cached
 * and on the fact that the same RestrictInfo node will appear in every
 * joininfo list that might be used when the joinrel is formed.
 * And it doesn't work in cases where the size estimation is nonlinear
 * (i.e., outer and IN joins).  But it beats not doing anything.
 *
 * We examine each base relation to see if join clauses associated with it
 * contain extractable restriction conditions.  If so, add those conditions
 * to the rel's baserestrictinfo and update the cached selectivities of the
 * join clauses.  Note that the same join clause will be examined afresh
 * from the point of view of each baserel that participates in it, so its
 * cached selectivity may get updated multiple times.
 */
void
extract_restriction_or_clauses(PlannerInfo *root)
{
	Index		rti;

	/* Examine each baserel for potential join OR clauses */
	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *rel = root->simple_rel_array[rti];
		ListCell   *lc;

		/* there may be empty slots corresponding to non-baserel RTEs */
		if (rel == NULL)
			continue;

		Assert(rel->relid == rti);	/* sanity check on array */

		/* ignore RTEs that are "other rels" */
		if (rel->reloptkind != RELOPT_BASEREL)
			continue;

		rel->baserestrictinfo  = transform_ors(root, rel->baserestrictinfo);
		//rel->joininfo = transform_ors(root, rel->joininfo);

		/*
		 * Find potentially interesting OR joinclauses.  We can use any
		 * joinclause that is considered safe to move to this rel by the
		 * parameterized-path machinery, even though what we are going to do
		 * with it is not exactly a parameterized path.
		 */
		foreach(lc, rel->joininfo)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

			if (restriction_is_or_clause(rinfo) &&
				join_clause_is_movable_to(rinfo, rel))
			{
				/* Try to extract a qual for this rel only */
				Expr	   *orclause = extract_or_clause(rinfo, rel);

				/*
				 * If successful, decide whether we want to use the clause,
				 * and insert it into the rel's restrictinfo list if so.
				 */
				if (orclause)
				{
					consider_new_or_clause(root, rel, orclause, rinfo);
				}
			}
		}
	}
}

/*
 * Is the given primitive (non-OR) RestrictInfo safe to move to the rel?
 */
static bool
is_safe_restriction_clause_for(RestrictInfo *rinfo, RelOptInfo *rel)
{
	/*
	 * We want clauses that mention the rel, and only the rel.  So in
	 * particular pseudoconstant clauses can be rejected quickly.  Then check
	 * the clause's Var membership.
	 */
	if (rinfo->pseudoconstant)
		return false;
	if (!bms_equal(rinfo->clause_relids, rel->relids))
		return false;

	/* We don't want extra evaluations of any volatile functions */
	if (contain_volatile_functions((Node *) rinfo->clause))
		return false;

	return true;
}

/*
 * Try to extract a restriction clause mentioning only "rel" from the given
 * join OR-clause.
 *
 * We must be able to extract at least one qual for this rel from each of
 * the arms of the OR, else we can't use it.
 *
 * Returns an OR clause (not a RestrictInfo!) pertaining to rel, or NULL
 * if no OR clause could be extracted.
 */
static Expr *
extract_or_clause(RestrictInfo *or_rinfo, RelOptInfo *rel)
{
	List	   *clauselist = NIL;
	ListCell   *lc;

	/*
	 * Scan each arm of the input OR clause.  Notice we descend into
	 * or_rinfo->orclause, which has RestrictInfo nodes embedded below the
	 * toplevel OR/AND structure.  This is useful because we can use the info
	 * in those nodes to make is_safe_restriction_clause_for()'s checks
	 * cheaper.  We'll strip those nodes from the returned tree, though,
	 * meaning that fresh ones will be built if the clause is accepted as a
	 * restriction clause.  This might seem wasteful --- couldn't we re-use
	 * the existing RestrictInfos?	But that'd require assuming that
	 * selectivity and other cached data is computed exactly the same way for
	 * a restriction clause as for a join clause, which seems undesirable.
	 */
	Assert(is_orclause(or_rinfo->orclause));
	foreach(lc, ((BoolExpr *) or_rinfo->orclause)->args)
	{
		Node	   *orarg = (Node *) lfirst(lc);
		List	   *subclauses = NIL;
		Node	   *subclause;

		/* OR arguments should be ANDs or sub-RestrictInfos */
		if (is_andclause(orarg))
		{
			List	   *andargs = ((BoolExpr *) orarg)->args;
			ListCell   *lc2;

			foreach(lc2, andargs)
			{
				RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc2);

				if (restriction_is_or_clause(rinfo))
				{
					/*
					 * Recurse to deal with nested OR.  Note we *must* recurse
					 * here, this isn't just overly-tense optimization: we
					 * have to descend far enough to find and strip all
					 * RestrictInfos in the expression.
					 */
					Expr	   *suborclause;

					suborclause = extract_or_clause(rinfo, rel);
					if (suborclause)
						subclauses = lappend(subclauses, suborclause);
				}
				else if (is_safe_restriction_clause_for(rinfo, rel))
					subclauses = lappend(subclauses, rinfo->clause);
			}
		}
		else
		{
			RestrictInfo *rinfo = castNode(RestrictInfo, orarg);

			Assert(!restriction_is_or_clause(rinfo));
			if (is_safe_restriction_clause_for(rinfo, rel))
				subclauses = lappend(subclauses, rinfo->clause);
		}

		/*
		 * If nothing could be extracted from this arm, we can't do anything
		 * with this OR clause.
		 */
		if (subclauses == NIL)
			return NULL;

		/*
		 * OK, add subclause(s) to the result OR.  If we found more than one,
		 * we need an AND node.  But if we found only one, and it is itself an
		 * OR node, add its subclauses to the result instead; this is needed
		 * to preserve AND/OR flatness (ie, no OR directly underneath OR).
		 */
		subclause = (Node *) make_ands_explicit(subclauses);
		if (is_orclause(subclause))
			clauselist = list_concat(clauselist,
									 ((BoolExpr *) subclause)->args);
		else
			clauselist = lappend(clauselist, subclause);
	}

	/*
	 * If we got a restriction clause from every arm, wrap them up in an OR
	 * node.  (In theory the OR node might be unnecessary, if there was only
	 * one arm --- but then the input OR node was also redundant.)
	 */
	if (clauselist != NIL)
		return make_orclause(clauselist);
	return NULL;
}

/*
 * Consider whether a successfully-extracted restriction OR clause is
 * actually worth using.  If so, add it to the planner's data structures,
 * and adjust the original join clause (join_or_rinfo) to compensate.
 */
static void
consider_new_or_clause(PlannerInfo *root, RelOptInfo *rel,
					   Expr *orclause, RestrictInfo *join_or_rinfo)
{
	RestrictInfo *or_rinfo;
	Selectivity or_selec,
				orig_selec;

	/*
	 * Build a RestrictInfo from the new OR clause.  We can assume it's valid
	 * as a base restriction clause.
	 */
	or_rinfo = make_restrictinfo(root,
								 orclause,
								 true,
								 false,
								 false,
								 false,
								 join_or_rinfo->security_level,
								 NULL,
								 NULL,
								 NULL);

	/*
	 * Estimate its selectivity.  (We could have done this earlier, but doing
	 * it on the RestrictInfo representation allows the result to get cached,
	 * saving work later.)
	 */
	or_selec = clause_selectivity(root, (Node *) or_rinfo,
								  0, JOIN_INNER, NULL);

	/*
	 * The clause is only worth adding to the query if it rejects a useful
	 * fraction of the base relation's rows; otherwise, it's just going to
	 * cause duplicate computation (since we will still have to check the
	 * original OR clause when the join is formed).  Somewhat arbitrarily, we
	 * set the selectivity threshold at 0.9.
	 */
	if (or_selec > 0.9)
		return;					/* forget it */

	/*
	 * OK, add it to the rel's restriction-clause list.
	 */
	rel->baserestrictinfo = lappend(rel->baserestrictinfo, or_rinfo);
	rel->baserestrict_min_security = Min(rel->baserestrict_min_security,
										 or_rinfo->security_level);

	/*
	 * Adjust the original join OR clause's cached selectivity to compensate
	 * for the selectivity of the added (but redundant) lower-level qual. This
	 * should result in the join rel getting approximately the same rows
	 * estimate as it would have gotten without all these shenanigans.
	 *
	 * XXX major hack alert: this depends on the assumption that the
	 * selectivity will stay cached.
	 *
	 * XXX another major hack: we adjust only norm_selec, the cached
	 * selectivity for JOIN_INNER semantics, even though the join clause
	 * might've been an outer-join clause.  This is partly because we can't
	 * easily identify the relevant SpecialJoinInfo here, and partly because
	 * the linearity assumption we're making would fail anyway.  (If it is an
	 * outer-join clause, "rel" must be on the nullable side, else we'd not
	 * have gotten here.  So the computation of the join size is going to be
	 * quite nonlinear with respect to the size of "rel", so it's not clear
	 * how we ought to adjust outer_selec even if we could compute its
	 * original value correctly.)
	 */
	if (or_selec > 0)
	{
		SpecialJoinInfo sjinfo;

		/*
		 * Make up a SpecialJoinInfo for JOIN_INNER semantics.  (Compare
		 * approx_tuple_count() in costsize.c.)
		 */
		sjinfo.type = T_SpecialJoinInfo;
		sjinfo.min_lefthand = bms_difference(join_or_rinfo->clause_relids,
											 rel->relids);
		sjinfo.min_righthand = rel->relids;
		sjinfo.syn_lefthand = sjinfo.min_lefthand;
		sjinfo.syn_righthand = sjinfo.min_righthand;
		sjinfo.jointype = JOIN_INNER;
		sjinfo.ojrelid = 0;
		sjinfo.commute_above_l = NULL;
		sjinfo.commute_above_r = NULL;
		sjinfo.commute_below_l = NULL;
		sjinfo.commute_below_r = NULL;
		/* we don't bother trying to make the remaining fields valid */
		sjinfo.lhs_strict = false;
		sjinfo.semi_can_btree = false;
		sjinfo.semi_can_hash = false;
		sjinfo.semi_operators = NIL;
		sjinfo.semi_rhs_exprs = NIL;

		/* Compute inner-join size */
		orig_selec = clause_selectivity(root, (Node *) join_or_rinfo,
										0, JOIN_INNER, &sjinfo);

		/* And hack cached selectivity so join size remains the same */
		join_or_rinfo->norm_selec = orig_selec / or_selec;
		/* ensure result stays in sane range */
		if (join_or_rinfo->norm_selec > 1)
			join_or_rinfo->norm_selec = 1;
		/* as explained above, we don't touch outer_selec */
	}
}
