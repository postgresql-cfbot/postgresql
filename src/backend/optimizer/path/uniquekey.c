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
#include "utils/lsyscache.h"

/*
 * Information on a column of an unique index for which we are trying to
 * create an unique key.
 */
typedef struct UniqueKeyExpr
{
	/* The expression itself. */
	Expr	*expr;

	/* Operator family to find / create suitable equivalence class. */
	Oid		opfamily;
} UniqueKeyExpr;

/* Functions to populate UniqueKey */
static bool add_uniquekey_for_uniqueindex(PlannerInfo *root,
										  IndexOptInfo *unique_index,
										  List *truncatable_exprs,
										  List *expr_opfamilies);
static Bitmapset *build_ec_positions_for_exprs(PlannerInfo *root, List *exprs,
											   RelOptInfo *rel);
static int find_ec_position_matching_expr(PlannerInfo *root,
										  RelOptInfo *baserel,
										  Expr *expr, List *opfamilies);
static bool unique_ecs_useful_for_distinct(PlannerInfo *root, Bitmapset *ec_indexes);

/* Helper functions to create UniqueKey. */
static UniqueKey * make_uniquekey(Bitmapset *item_indexes,
								  List *opfamily_lists,
								  bool useful_for_distinct);
static void mark_rel_singlerow(RelOptInfo *rel, int relid);

static UniqueKey * rel_singlerow_uniquekey(RelOptInfo *rel);

/* Debug only */
static void print_uniquekey(PlannerInfo *root, RelOptInfo *rel);

static bool uniquekey_contains_in(PlannerInfo *root, UniqueKey * ukey, Bitmapset *ecs, Relids relids);
static bool is_uniquekey_useful_afterjoin(PlannerInfo *root, UniqueKey * ukey, RelOptInfo *joinrel);
static int find_expr_pos_in_list(Expr *expr, List *list);

/*
 * populate_baserel_uniquekeys
 *
 *		UniqueKey on baserel comes from unique indexes. Any expression
 * which equals with Const can be stripped and the left expressions are
 * still unique.
 */
void
populate_baserel_uniquekeys(PlannerInfo *root, RelOptInfo *rel)
{
	ListCell   *lc;
	List	   *truncatable_exprs = NIL,
			   *expr_opfamilies = NIL;

	if (rel->reloptkind != RELOPT_BASEREL)
		return;

	/*
	 * Currently we only use UniqueKey for mark-distinct-as-noop case, so if
	 * there is no-distinct-clause at all, we can ignore the maintenance at
	 * the first place. however for code coverage at the development stage, we
	 * bypass this fastpath on purpose.
	 *
	 * XXX: even we want this fastpath, we still need to distinguish even the
	 * current subquery has no DISTINCT, but the upper query may have.
	 */

	/*
	 * if (root->distinct_pathkeys == NIL) return;
	 */
	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		if (rinfo->mergeopfamilies == NIL)
			continue;

		if (!IsA(rinfo->clause, OpExpr))
			continue;

		if (bms_is_empty(rinfo->left_relids))
			truncatable_exprs = lappend(truncatable_exprs, get_rightop(rinfo->clause));
		else if (bms_is_empty(rinfo->right_relids))
			truncatable_exprs = lappend(truncatable_exprs, get_leftop(rinfo->clause));
		else
			continue;

		expr_opfamilies = lappend(expr_opfamilies, rinfo->mergeopfamilies);
	}

	foreach(lc, rel->indexlist)
	{
		IndexOptInfo *index = (IndexOptInfo *) lfirst(lc);

		/*
		 * Like in relation_has_unique_index_for(), we shouldn't use predOK
		 * because its validity might depend on join clauses, however here we
		 * test uniqueness of the join input.
		 */
		if (!index->unique || !index->immediate || index->indpred != NIL)
			continue;

		if (add_uniquekey_for_uniqueindex(root, index,
										  truncatable_exprs,
										  expr_opfamilies))
			/* Find a singlerow case, no need to go through other indexes. */
			return;
	}

	print_uniquekey(root, rel);
}


/*
 * add_uniquekey_for_uniqueindex
 *
 *		 populate a UniqueKey if it is interesting, return true iff the
 * UniqueKey is an SingleRow. Only the interesting UniqueKeys are kept.
 */
static bool
add_uniquekey_for_uniqueindex(PlannerInfo *root, IndexOptInfo *unique_index,
							  List *truncatable_exprs, List *expr_opfamilies)
{
	List	   *unique_exprs = NIL;
	Bitmapset  *unique_ecs = NULL;
	ListCell   *indexpr_item;
	RelOptInfo *rel = unique_index->rel;
	bool		used_for_distinct;
	int			c, i;
	List		*opfamily_lists = NIL;

	indexpr_item = list_head(unique_index->indexprs);

	for (c = 0; c < unique_index->nkeycolumns; c++)
	{
		int			attr = unique_index->indexkeys[c];
		Expr	   *expr;		/* The candidate for UniqueKey expression. */
		bool		matched_const = false;
		ListCell   *lc1,
				   *lc2;
		UniqueKeyExpr	*uexpr;

		if (attr > 0)
		{
			expr = list_nth_node(TargetEntry, unique_index->indextlist, c)->expr;
		}
		else if (attr == 0)
		{
			/* Expression index */
			expr = lfirst(indexpr_item);
			indexpr_item = lnext(unique_index->indexprs, indexpr_item);
		}
		else					/* attr < 0 */
		{
			/* Index on OID is possible, not handle it for now. */
			return false;
		}

		/* Ignore the expr which are equals to const. */
		forboth(lc1, truncatable_exprs, lc2, expr_opfamilies)
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

		uexpr = palloc_object(UniqueKeyExpr);
		uexpr->expr = expr;
		uexpr->opfamily = unique_index->opfamily[c];
		unique_exprs = lappend(unique_exprs, uexpr);
	}

	if (unique_exprs == NIL)
	{
		/* single row is always interesting. */
		mark_rel_singlerow(rel, rel->relid);
		return true;
	}

	/*
	 * if no EquivalenceClass is found for any exprs in unique exprs, we are
	 * sure the whole exprs are not in the DISTINCT clause or mergeable join
	 * clauses. so it is not interesting.
	 */
	unique_ecs = build_ec_positions_for_exprs(root, unique_exprs, rel);
	if (unique_ecs == NULL)
		return false;

	/* Collect the operator families. */
	i = -1;
	while ((i = bms_next_member(unique_ecs, i)) >= 0)
	{
		EquivalenceClass	*ec = list_nth(root->eq_classes, i);

		opfamily_lists = lappend(opfamily_lists, ec->ec_opfamilies);
	}

	used_for_distinct = unique_ecs_useful_for_distinct(root, unique_ecs);


	rel->uniquekeys = lappend(rel->uniquekeys,
							  make_uniquekey(unique_ecs, opfamily_lists,
											 used_for_distinct));
	return false;
}

/*
 * find_ec_position_matching_expr
 *		Locate the position of EquivalenceClass whose members matching the
 *		given expr. Try to create EC if suitable one does not exist. Return -1
 *		if there is not enough information in the catalog about the data type
 *		to create the EC.
 */
static int
find_ec_position_matching_expr(PlannerInfo *root, RelOptInfo *baserel,
							   Expr *expr, List *opfamilies)
{
	int			i = -1;
	EquivalenceClass *ec;
	int		ec_index;
	JoinDomain *jdomain;
	ListCell	*lc;

	/*
	 * XXX Currently the function is only used to build unique keys, so we
	 * don't create ECs for boolean columns: normal EC processing does not
	 * create them as well and build_index_pathkeys() considers boolean
	 * pathkeys redundant, so missing boolean EC is o.k. However, if we
	 * created the boolean ECs, build_index_pathkeys() would return the
	 * corresponding pathkeys, and those could mismatch query_pathkeys.
	 * Shouldn't this be handled in pathkeys_useful_for_ordering()?
	 */
	foreach(lc, opfamilies)
	{
		/* XXX The result should be the same for each item of the list. */
		if (IsBooleanOpfamily(lfirst_oid(lc)))
			return -1;
	}

	while ((i = bms_next_member(baserel->eclass_indexes, i)) >= 0)
	{
		List	*diff;

		ec = list_nth(root->eq_classes, i);

		/*
		 * The EC must understand equality in the same way as the unique index
		 * that guarantees the uniqueness. That is, ec_opfamilies must contain
		 * all the opfamilies passed by the caller.
		 */
		diff = list_difference_oid(opfamilies, ec->ec_opfamilies);
		if (list_length(diff) > 0)
			continue;

		if (find_ec_member_matching_expr(ec, expr, baserel->relids))
			return i;
	}

	/* Create the EC. */
	jdomain = makeNode(JoinDomain);
	jdomain->jd_relids = pull_varnos(root, (Node *) expr);
	ec = get_eclass_for_sort_expr(root, expr,
								  opfamilies,
								  exprType((Node *) expr),
								  exprCollation((Node *) expr),
								  0,
								  NULL,
								  jdomain,
								  true);
	ec_index = list_length(root->eq_classes) - 1;

	/* The new EC should now be known to both 'root' and 'baserel'. */
	Assert(ec == llast(root->eq_classes));
	Assert(bms_is_member(ec_index, baserel->eclass_indexes));

	return ec_index;
}

/*
 * build_ec_positions_for_exprs
 *
 *		Given a list of exprs, find the related EC positions for each of
 *		them. if any exprs has no EC related, return NULL;
 */
static Bitmapset *
build_ec_positions_for_exprs(PlannerInfo *root, List *exprs, RelOptInfo *rel)
{
	ListCell   *lc;
	Bitmapset  *res = NULL;

	foreach(lc, exprs)
	{
		UniqueKeyExpr	*uexpr = (UniqueKeyExpr *) lfirst(lc);
		int			pos = find_ec_position_matching_expr(root, rel,
														 uexpr->expr,
														 list_make1_oid(uexpr->opfamily));

		if (pos < 0)
		{
			bms_free(res);
			return NULL;
		}
		res = bms_add_member(res, pos);
	}
	return res;

}

/*
 *	make_uniquekey
 *		Based on UnqiueKey rules, it is impossible for a UnqiueKey
 * which have item_indexes and relid both set. This function just
 * handle item_indexes case.
 */
static UniqueKey *
make_uniquekey(Bitmapset *item_indexes, List *opfamily_lists,
			   bool useful_for_distinct)
{
	UniqueKey  *ukey = makeNode(UniqueKey);

	ukey->item_indexes = item_indexes;
	ukey->opfamily_lists = opfamily_lists;
	ukey->relid = 0;
	ukey->use_for_distinct = useful_for_distinct;
	return ukey;
}

/*
 * mark_rel_singlerow
 *	mark a relation as singlerow.
 */
static void
mark_rel_singlerow(RelOptInfo *rel, int relid)
{
	UniqueKey  *ukey = makeNode(UniqueKey);

	ukey->relid = relid;
	rel->uniquekeys = list_make1(ukey);
}

static inline bool
uniquekey_is_singlerow(UniqueKey * ukey)
{
	return ukey->relid != 0;
}

/*
 *
 *	Return the UniqueKey if rel is a singlerow Relation. othwise
 * return NULL.
 */
static UniqueKey *
rel_singlerow_uniquekey(RelOptInfo *rel)
{
	if (rel->uniquekeys != NIL)
	{
		UniqueKey  *ukey = linitial_node(UniqueKey, rel->uniquekeys);

		if (ukey->relid)
			return ukey;
	}
	return NULL;
}

/*
 * print_uniquekey
 *	Used for easier reivew, should be removed before commit.
 */
static void
print_uniquekey(PlannerInfo *root, RelOptInfo *rel)
{
	if (!enable_geqo)
	{
		ListCell   *lc;

		elog(INFO, "Rel = %s", bmsToString(rel->relids));
		foreach(lc, rel->uniquekeys)
		{
			UniqueKey  *ukey = lfirst_node(UniqueKey, lc);

			elog(INFO, "UNIQUEKEY{indexes=%s, singlerow_rels=%d, use_for_distinct=%d}",
				 bmsToString(ukey->item_indexes),
				 ukey->relid,
				 ukey->use_for_distinct);
		}
	}
}

/*
 *	is it possible that the var contains multi NULL values in the given
 * RelOptInfo rel?
 */
static bool
var_is_nullable(PlannerInfo *root, Var *var, RelOptInfo *rel)
{
	RelOptInfo *base_rel;

	/* check if the outer join can add the NULL values.  */
	if (bms_overlap(var->varnullingrels, rel->relids))
		return true;

	/* check if the user data has the NULL values. */
	base_rel = root->simple_rel_array[var->varno];
	return !bms_is_member(var->varattno - FirstLowInvalidHeapAttributeNumber, base_rel->notnullattrs);
}


/*
 * uniquekey_contains_multinulls
 *
 *	Check if the uniquekey contains nulls values.
 */
bool
uniquekey_contains_multinulls(PlannerInfo *root, RelOptInfo *rel, UniqueKey * ukey)
{
	int			i = -1;

	while ((i = bms_next_member(ukey->item_indexes, i)) >= 0)
	{
		EquivalenceClass *ec = list_nth_node(EquivalenceClass, root->eq_classes, i);
		ListCell   *lc;

		foreach(lc, ec->ec_members)
		{
			EquivalenceMember *em = lfirst_node(EquivalenceMember, lc);
			Var		   *var;

			var = (Var *) em->em_expr;

			if (!IsA(var, Var))
				continue;

			if (var_is_nullable(root, var, rel))
				return true;
			else

				/*
				 * If any one of member in the EC is not nullable, we all the
				 * members are not nullable since they are equal with each
				 * other.
				 */
				break;
		}
	}

	return false;
}


/*
 * relation_is_distinct_for
 *
 * Check if the rel is distinct for distinct_pathkey.
 */
bool
relation_is_distinct_for(PlannerInfo *root, RelOptInfo *rel, List *distinct_pathkey)
{
	ListCell   *lc;
	UniqueKey  *singlerow_ukey = rel_singlerow_uniquekey(rel);
	Bitmapset  *pathkey_bm = NULL;

	if (singlerow_ukey)
	{
		return !uniquekey_contains_multinulls(root, rel, singlerow_ukey);
	}

	foreach(lc, distinct_pathkey)
	{
		PathKey    *pathkey = lfirst_node(PathKey, lc);
		int			pos = list_member_ptr_pos(root->eq_classes, pathkey->pk_eclass);

		if (pos == -1)
			return false;

		pathkey_bm = bms_add_member(pathkey_bm, pos);
	}

	foreach(lc, rel->uniquekeys)
	{
		UniqueKey  *ukey = lfirst_node(UniqueKey, lc);

		if (bms_is_subset(ukey->item_indexes, pathkey_bm) &&
			!uniquekey_contains_multinulls(root, rel, ukey))
			return true;
	}

	return false;
}

/*
 * unique_ecs_useful_for_distinct
 *
 *	Return true if all the EquivalenceClass for ecs exists in root->distinct_pathkey.
 */
static bool
unique_ecs_useful_for_distinct(PlannerInfo *root, Bitmapset *ec_indexes)
{
	int			i = -1;

	while ((i = bms_next_member(ec_indexes, i)) >= 0)
	{
		EquivalenceClass *ec = list_nth(root->eq_classes, i);
		ListCell   *p;
		bool		found = false;

		foreach(p, root->distinct_pathkeys)
		{
			PathKey    *pathkey = lfirst_node(PathKey, p);

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
 * populate_joinrel_uniquekey_for_rel
 *
 *    Check if the pattern of rel.any_column = other_rel.unique_key_column
 * exists, if so, the uniquekey in rel is still valid after join and it is
 * added into joinrel and return true. otherwise return false.
 */
static bool
populate_joinrel_uniquekey_for_rel(PlannerInfo *root, RelOptInfo *joinrel,
								   RelOptInfo *rel, RelOptInfo *other_rel,
								   List *restrictlist)
{
	bool		rel_keep_unique = false;
	Bitmapset  *other_ecs = NULL;
	Relids		other_relids = NULL;
	ListCell   *lc;

	if (rel_singlerow_uniquekey(other_rel))
	{
		/*
		 * any uniquekeys stuff join with single-row, its uniqueness is still
		 * kept.
		 */
		goto done;
	}

	/* find out the ECs which the rel.any_columns equals to. */
	foreach(lc, restrictlist)
	{
		RestrictInfo *r = lfirst_node(RestrictInfo, lc);

		if (r->mergeopfamilies == NIL)
			continue;

		/* Build the Bitmapset for easy comparing. */
		if (bms_equal(r->left_relids, rel->relids) && r->right_ec != NULL)
		{
			other_ecs = bms_add_member(other_ecs, list_member_ptr_pos(root->eq_classes, r->right_ec));
			other_relids = bms_add_members(other_relids, r->right_relids);
		}
		else if (bms_equal(r->right_relids, rel->relids) && r->left_ec != NULL)
		{
			other_ecs = bms_add_member(other_ecs, list_member_ptr_pos(root->eq_classes, r->left_ec));
			other_relids = bms_add_members(other_relids, r->left_relids);
		}
	}

	/* Check if these ECs include a uniquekey of other_rel */
	foreach(lc, other_rel->uniquekeys)
	{
		UniqueKey  *ukey = lfirst_node(UniqueKey, lc);

		if (uniquekey_contains_in(root, ukey, other_ecs, other_relids))
		{
			rel_keep_unique = true;
			break;
		}
	}

	if (!rel_keep_unique)
		return false;

done:

	/*
	 * Now copy the uniquekey in rel to joinrel, but first we need to know if
	 * it is useful.
	 */
	foreach(lc, rel->uniquekeys)
	{
		UniqueKey  *ukey = lfirst_node(UniqueKey, lc);

		if (is_uniquekey_useful_afterjoin(root, ukey, joinrel))
		{
			if (uniquekey_is_singlerow(ukey))
			{
				/*
				 * XXX (?): a). NULL values. b). other relids rather than
				 * ukey->relid.
				 */
				mark_rel_singlerow(joinrel, ukey->relid);
				break;
			}
			joinrel->uniquekeys = lappend(joinrel->uniquekeys, ukey);
		}
	}

	return true;
}

/*
 * populate_joinrel_uniquekeys
 */
void
populate_joinrel_uniquekeys(PlannerInfo *root, RelOptInfo *joinrel,
							RelOptInfo *outerrel, RelOptInfo *innerrel,
							List *restrictlist, JoinType jointype)
{
	bool		outeruk_still_valid = false,
				inneruk_still_valid = false;
	ListCell   *lc,
			   *lc2;

	if (jointype == JOIN_SEMI || jointype == JOIN_ANTI)
	{
		foreach(lc, outerrel->uniquekeys)
		{
			/*
			 * the uniquekey on the outer side is not changed after semi/anti
			 * join.
			 */
			joinrel->uniquekeys = lappend(joinrel->uniquekeys, lfirst(lc));
		}
		return;
	}

	if (outerrel->uniquekeys == NIL || innerrel->uniquekeys == NIL)
		return;

	outeruk_still_valid = populate_joinrel_uniquekey_for_rel(root, joinrel, outerrel,
															 innerrel, restrictlist);
	inneruk_still_valid = populate_joinrel_uniquekey_for_rel(root, joinrel, innerrel,
															 outerrel, restrictlist);

	if (outeruk_still_valid || inneruk_still_valid)

		/*
		 * the uniquekey on outers or inners have been added into joinrel so
		 * the combined uniuqekey from both sides is not needed.
		 */
		return;

	/*
	 * The combined UniqueKey is still unique no matter the join method or
	 * join clauses. So let build the combined ones.
	 */
	foreach(lc, outerrel->uniquekeys)
	{
		UniqueKey  *outer_ukey = lfirst(lc);

		if (!is_uniquekey_useful_afterjoin(root, outer_ukey, joinrel))
			/* discard the uniquekey which is not interesting. */
			continue;

		/* singlerow will make the inneruk_still_valid true */
		Assert(!uniquekey_is_singlerow(outer_ukey));

		foreach(lc2, innerrel->uniquekeys)
		{
			UniqueKey  *inner_ukey = lfirst(lc2);
			Bitmapset	*indexes_new;
			List	*opfamily_lists_new = NIL;
			int		i;
			ListCell	*lo, *li;

			if (!is_uniquekey_useful_afterjoin(root, inner_ukey, joinrel))
				continue;

			/* singlerow will make the outeruk_still_valid true */
			Assert(!uniquekey_is_singlerow(inner_ukey));

			/* Assemble opfamily_lists_new in the correct order.  */
			i = -1;
			indexes_new = bms_union(outer_ukey->item_indexes, inner_ukey->item_indexes);
			lo = list_head(outer_ukey->opfamily_lists);
			li = list_head(inner_ukey->opfamily_lists);
			while ((i = bms_next_member(indexes_new, i)) >= 0)
			{
				List	*l;

				if (bms_is_member(i, outer_ukey->item_indexes))
				{
					l = lfirst(lo);
					lo = lnext(outer_ukey->opfamily_lists, lo);
				}
				else
				{
					Assert(bms_is_member(i, inner_ukey->item_indexes));

					l = lfirst(li);
					li = lnext(inner_ukey->opfamily_lists, li);
				}

				opfamily_lists_new = lappend(opfamily_lists_new, l);
			}

			joinrel->uniquekeys = lappend(joinrel->uniquekeys,
										  make_uniquekey(
														 indexes_new,
														 opfamily_lists_new,
														 outer_ukey->use_for_distinct || inner_ukey->use_for_distinct));
		}
	}
}

/*
 * uniquekey_contains_in
 *	Return if UniqueKey contains in the list of EquivalenceClass
 * or the UniqueKey's SingleRow contains in relids.
 */
static bool
uniquekey_contains_in(PlannerInfo *root, UniqueKey * ukey, Bitmapset *ecs, Relids relids)
{

	if (uniquekey_is_singlerow(ukey))
	{
		return bms_is_member(ukey->relid, relids);
	}

	return bms_is_subset(ukey->item_indexes, ecs);
}


/*
 * uniquekey_useful_for_merging
 *	Check if the uniquekey is useful for mergejoins above the given relation.
 *
 * similar with pathkeys_useful_for_merging.
 */
static bool
uniquekey_useful_for_merging(PlannerInfo *root, UniqueKey * ukey, RelOptInfo *rel)
{

	int			i = -1;

	while ((i = bms_next_member(ukey->item_indexes, i)) >= 0)
	{
		EquivalenceClass *ec = list_nth(root->eq_classes, i);
		ListCell   *j;
		bool		matched = false;

		if (rel->has_eclass_joins && eclass_useful_for_merging(root, ec, rel))
		{
			matched = true;
		}
		else
		{
			foreach(j, rel->joininfo)
			{
				RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(j);

				if (restrictinfo->mergeopfamilies == NIL)
					continue;
				update_mergeclause_eclasses(root, restrictinfo);

				if (ec == restrictinfo->left_ec || ec == restrictinfo->right_ec)
				{
					matched = true;
					break;
				}
			}
		}

		if (!matched)
			return false;
	}

	return true;
}

/*
 * is_uniquekey_useful_afterjoin
 *
 *  uniquekey is useful when it contains in distinct_pathkey or in mergable join clauses.
 */
static bool
is_uniquekey_useful_afterjoin(PlannerInfo *root, UniqueKey * ukey, RelOptInfo *joinrel)
{
	if (ukey->use_for_distinct)
		return true;

	/* XXX might needs a better judgement */
	if (uniquekey_is_singlerow(ukey))
		return true;


	return uniquekey_useful_for_merging(root, ukey, joinrel);
}


/*
 * convert_unique_keys_for_rel
 *
 * Convert unique keys of 'input_relation' and assign them to 'rel'. The
 * comments of UniqueKey explain the difference in the format.
 *
 * This function assumes that at least one of the relations is "upper
 * relation". If 'input_rel' is upper and 'rel' is base/join rel, pass NULL
 * for 'target'.
 */
void
convert_unique_keys_for_rel(PlannerInfo *root, RelOptInfo *rel,
							PathTarget *target, RelOptInfo *input_rel,
							PathTarget *input_target)
{
	ListCell	*lc1;

	/* No unique keys, in case we return early. */
	rel->uniquekeys = NIL;

	/*
	 * Set-returning functions can break uniqueness. This does not necessarily
	 * affect all of the upper relations, but checking is not trivial and
	 * should be done by the caller rather than by this function. Make the
	 * logic simple for now.
	 */
	if (root->parse->hasTargetSRFs)
		return;

	/* No keys to convert? */
	if (input_rel->uniquekeys == NIL)
		return;

	/*
	 * If both relations are upper and if they have the same target,
	 * just copy the uniquekeys unmodified.
	 */
	if (IS_UPPER_REL(rel) && IS_UPPER_REL(input_rel) &&
		target == input_target)
	{
		rel->uniquekeys = copyObject(input_rel->uniquekeys);
		return;
	}

	foreach(lc1, input_rel->uniquekeys)
	{
		UniqueKey  *key = lfirst_node(UniqueKey, lc1);
		Bitmapset	*item_indexes_new = NULL;
		List	*opfamily_lists_new = NIL;
		int		matched;
		int		i = -1;
		ListCell	*lc2;

		if (!IS_UPPER_REL(input_rel))
		{
			/* Conversion between base/join rels is currently not needed. */
			Assert(IS_UPPER_REL(rel));

			/*
			 * For each key, check its ECs and find the EM present in
			 * 'target'.
			 */
			while ((i = bms_next_member(key->item_indexes, i)) >= 0)
			{
				EquivalenceClass *ec = list_nth(root->eq_classes, i);

				matched = -1;
				foreach(lc2, ec->ec_members)
				{
					EquivalenceMember *em;
					Expr	*expr;

					em = lfirst_node(EquivalenceMember, lc2);
					expr = em->em_expr;

					/* EMs can be wrapped in RelabelType. */
					while (expr && IsA(expr, RelabelType))
						expr = ((RelabelType *) expr)->arg;

					/*
					 * Currently we only accept Vars, for simplicity (and
					 * because the unique key shouldn't contain other
					 * nodes). In general, we look for expressions that map
					 * unique input value to unique output value. Not sure
					 * though how we can recognize them easily.
					 */
					if (!IsA(expr, Var))
						continue;

					matched = find_expr_pos_in_list(expr, target->exprs);
					if (matched >= 0)
						break;
				}

				if (matched >= 0)
				{
					item_indexes_new = bms_add_member(item_indexes_new,
													  matched);
					opfamily_lists_new = lappend(opfamily_lists_new,
												 ec->ec_opfamilies);
				}
				else
				{
					/* The caller does not want an incomplete unique key. */
					bms_free(item_indexes_new);
					item_indexes_new = NULL;
					list_free(opfamily_lists_new);
					opfamily_lists_new = NIL;
					break;
				}
			}
		}
		else
		{
			ListCell	*opfamily_cell = list_head(key->opfamily_lists);

			/* IS_UPPER_REL(input_rel) */

			Assert(bms_num_members(key->item_indexes) ==
				   list_length(key->opfamily_lists));

			/*
			 * For an upper relation, 'index_items' point to expressions of an
			 * existing target. For each key, find its target expressions in
			 * 'target' or (if target==NULL) in the members of the ECs 'rel'
			 * belongs to.
			 */
			while ((i = bms_next_member(key->item_indexes, i)) >= 0)
			{
				Expr	*expr;

				expr = (Expr *) list_nth(input_target->exprs, i);

				if (target)
				{
					Assert(IS_UPPER_REL(rel));

					matched = find_expr_pos_in_list(expr, target->exprs);
				}
				else
				{
					ListCell	*lc3;
					Var		*target_var = NULL;

					/*
					 * 'rel' should be a base relation, not upper or join
					 * relation.
					 */
					Assert(!IS_UPPER_REL(rel));
					Assert(rel->relid > 0);

					/*
					 * Base relation should use plain Var nodes to reference
					 * the subquery target. Find the Var in the base relation
					 * target that points to this expression.
					 */
					foreach(lc3, rel->reltarget->exprs)
					{
						Var		*var;

						/*
						 * If a general expression happens to be there, give
						 * up because it can break uniqueness of the relation
						 * output.
						 */
						if (!IsA(lfirst(lc3), Var))
							return;

						var = lfirst_node(Var, lc3);

						if (var->varno == rel->relid &&
							var->varattno == (i + 1))
						{
							target_var = var;
							break;
						}
					}
					if (target_var)
					{
						List		*opfamilies = lfirst(opfamily_cell);

						matched = find_ec_position_matching_expr(root, rel,
																 (Expr *) target_var,
																 opfamilies);
						opfamily_cell = lnext(key->opfamily_lists, opfamily_cell);
					}
					else
						matched = -1;

					if (matched >= 0)
					{
						/*
						 * A unique index does not by itself guarantee the
						 * column is never NULL (Postgres unique indexes
						 * permit multiple NULLs). Only mark the output
						 * not-null when the underlying expression is a
						 * plain Var that is itself already known not-null
						 * in the subquery's own planning context.
						 */
						if (IsA(expr, Var) &&
							!var_is_nullable(rel->subroot, (Var *) expr, input_rel))
							rel->notnullattrs = bms_add_member(rel->notnullattrs,
															   target_var->varattno - FirstLowInvalidHeapAttributeNumber);
					}
					else
						matched = -1;
				}

				if (matched >= 0)
					item_indexes_new = bms_add_member(item_indexes_new,
													  matched);
				else
				{
					/* The caller does not want an incomplete unique key. */
					bms_free(item_indexes_new);
					item_indexes_new = NULL;
					break;
				}
			}
		}

		if (item_indexes_new)
		{
			UniqueKey	*key_new;

			if (!IS_UPPER_REL(input_rel))
				Assert(opfamily_lists_new);
			else
			{
				Assert(opfamily_lists_new == NIL);

				opfamily_lists_new = copyObject(key->opfamily_lists);
			}


			key_new = make_uniquekey(item_indexes_new, opfamily_lists_new,
									 true);
			rel->uniquekeys = lappend(rel->uniquekeys, key_new);
		}
	}
}

List *
create_uniquekeys_for_sortop(SetOperationStmt *topop, List *targetlist)
{
	ListCell	*l, *lg;
	int		i;
	Bitmapset	*key_idxs = NULL;
	List	*opfamily_lists = NIL;
	UniqueKey	*key;

	if (topop->all)
	{
		/*
		 * There is no easy way to combine unique keys of the individual
		 * queries.
		 */
		return NIL;
	}

	/* Iterate like in query_is_distinct_for() */
	lg = list_head(topop->groupClauses);
	i = 0;
	foreach(l, targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(l);
		SortGroupClause *sgc;
		List	*sgc_opfamilies;

		if (tle->resjunk)
		{
			i++;
			continue;	/* ignore resjunk columns */
		}

		key_idxs = bms_add_member(key_idxs, i++);

		/* non-resjunk columns should have grouping clauses */
		Assert(lg != NULL);
		sgc = (SortGroupClause *) lfirst(lg);
		lg = lnext(topop->groupClauses, lg);
		sgc_opfamilies = get_mergejoin_opfamilies(sgc->eqop);
		if (sgc_opfamilies == NIL)
		{
			/* XXX Shouldn' this be ERROR? */
			bms_free(key_idxs);
			key_idxs = NULL;
			break;
		}
		opfamily_lists = lappend(opfamily_lists, sgc_opfamilies);
	}

	if (key_idxs == NULL)
		return NIL;

	key = make_uniquekey(key_idxs, opfamily_lists, true);

	return list_make1(key);
}

/*
 * Return a zero-based index of an expression in a list, or -1 if not found.
 */
static int
find_expr_pos_in_list(Expr *expr, List *list)
{
	ListCell	*lc;
	int		idx = 0;

	foreach(lc, list)
	{
		Expr	*lexpr = (Expr *) lfirst(lc);

		if (equal(expr, lexpr))
			return idx;

		idx++;
	}

	return -1;
}
