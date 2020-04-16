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
#include "optimizer/optimizer.h"
#include "rewrite/rewriteManip.h"


/*
 * This struct is used to help populate_joinrel_uniquekeys,
 * Set added_to_joinrel to true if a uniquekey has been added to joinrel.
 * For a joinrel, if both sides have UniqueKey, then the combine of them
 * must be unique for the joinrel as well, But we don't need to add it if
 * either of them has been added to joinrel already. We use this struct to
 * maintain such info.
 */
typedef struct UniqueKeyContextData
{
	UniqueKey	*uniquekey;
	/* Set to true if the unique key has been added to joinrel->uniquekeys */
	bool	added_to_joinrel;
	/* If this uniquekey is still useful after join */
	bool	useful;
} *UniqueKeyContext;


static List *gather_mergeable_baserestrictlist(RelOptInfo *rel);
static List *gather_mergeable_joinclauses(RelOptInfo *joinrel,
										  RelOptInfo *rel1,
										  RelOptInfo *rel2,
										  List *restirctlist,
										  JoinType jointype);
static bool match_index_to_baserestrictinfo(IndexOptInfo *unique_ind,
											List *restrictlist);
static List *initililze_uniquecontext_for_joinrel(RelOptInfo *joinrel,
												  RelOptInfo *inputrel);

static bool innerrel_keeps_unique(PlannerInfo *root,
								  RelOptInfo *outerrel,
								  RelOptInfo *innerrel,
								  List *restrictlist,
								  bool reverse);
static bool clause_sides_match_join(RestrictInfo *rinfo,
									Relids outerrelids,
									Relids innerrelids);
static void add_uniquekey_from_index(RelOptInfo *rel,
									 IndexOptInfo *unique_index);
static void add_uniquekey_for_onerow(RelOptInfo *rel);

/* Used for unique indexes checking for partitioned table */
static bool index_constains_partkey(RelOptInfo *partrel,  IndexOptInfo *ind);
static IndexOptInfo *simple_copy_indexinfo_to_parent(RelOptInfo *parentrel,
													 IndexOptInfo *from);
static bool simple_indexinfo_equal(IndexOptInfo *ind1, IndexOptInfo *ind2);
static void adjust_partition_unique_indexlist(RelOptInfo *parentrel,
											  RelOptInfo *childrel,
											  List **global_unique_index);
/* Helper function for groupres/distinctrel */
static void add_uniquekey_from_sortgroups(PlannerInfo *root,
										  RelOptInfo *rel,
										  List *sortgroups);

/*
 * populate_baserel_uniquekeys
 *		Populate 'baserel' uniquekeys list by looking at the rel's unique index
 * add baserestrictinfo
 */
void
populate_baserel_uniquekeys(PlannerInfo *root,
							RelOptInfo *baserel,
							List *indexlist)
{
	ListCell *lc;
	List	*restrictlist = gather_mergeable_baserestrictlist(baserel);
	bool	return_one_row = false;
	List	*matched_uk_indexes = NIL;

	Assert(baserel->rtekind == RTE_RELATION);

	if (root->parse->hasTargetSRFs)
		return;

	if (baserel->reloptkind == RELOPT_OTHER_MEMBER_REL)
		/*
		 * Set UniqueKey on member rel is useless, we have to recompute it at
		 * upper level, see populate_partitionedrel_uniquekeys for reference
		 */
		return;

	foreach(lc, indexlist)
	{
		IndexOptInfo *ind = (IndexOptInfo *) lfirst(lc);
		if (!ind->unique || !ind->immediate ||
			(ind->indpred != NIL && !ind->predOK))
			continue;

		if (match_index_to_baserestrictinfo(ind, restrictlist))
		{
			return_one_row = true;
			break;
		}

		if (ind->indexprs != NIL)
			/* We can't guarantee if an expression returns a NULL value, so ignore it */
			continue;
		matched_uk_indexes = lappend(matched_uk_indexes, ind);
	}

	if (return_one_row)
	{
		add_uniquekey_for_onerow(baserel);
	}
	else
	{
		foreach(lc, matched_uk_indexes)
			add_uniquekey_from_index(baserel, lfirst_node(IndexOptInfo, lc));
	}
}


/*
 * populate_partitioned_rel_uniquekeys
 * The unique index can be used for UniqueKey based on:
 * 1). It must include partition keys
 * 2). All the childrels must has the same indexes.
 */
void
populate_partitionedrel_uniquekeys(PlannerInfo *root,
								   RelOptInfo *rel,
								   List *childrels)
{
	ListCell	*lc;
	List	*global_unique_indexlist = NIL;
	RelOptInfo *childrel;
	bool is_first = true;

	Assert(IS_PARTITIONED_REL(rel));

	if (root->parse->hasTargetSRFs)
		return;

	if (childrels == NIL)
		return;

	childrel = linitial_node(RelOptInfo, childrels);
	foreach(lc, childrel->indexlist)
	{
		IndexOptInfo *ind = lfirst(lc);
		IndexOptInfo *global_ind;
		if (!ind->unique || !ind->immediate ||
			(ind->indpred != NIL && !ind->predOK))
			continue;

		global_ind = simple_copy_indexinfo_to_parent(rel, ind);
		/*
		 * If the unique index doesn't contain partkey, then it is unique
		 * on this partition only, so it is useless for us.
		 */
		if (!index_constains_partkey(rel, global_ind))
			continue;
		global_unique_indexlist = lappend(global_unique_indexlist,  global_ind);
	}

	/* Fast path */
	if (global_unique_indexlist == NIL)
		return;

	foreach(lc, childrels)
	{
		RelOptInfo *child = lfirst(lc);
		if (is_first)
		{
			is_first = false;
			continue;
		}
		adjust_partition_unique_indexlist(rel, child, &global_unique_indexlist);
	}

	/* Now we have the unique index list which as exactly same on all childrels,
	 * Set the UniqueIndex just like it is non-partition table
	 */
	populate_baserel_uniquekeys(root, rel, global_unique_indexlist);
}


/*
 * populate_distinctrel_uniquekeys
 */
void
populate_distinctrel_uniquekeys(PlannerInfo *root,
								RelOptInfo *inputrel,
								RelOptInfo *distinctrel)
{
	/* The unique key before the distinct is still valid */
	distinctrel->uniquekeys = list_copy(inputrel->uniquekeys);
	add_uniquekey_from_sortgroups(root, distinctrel, root->parse->distinctClause);
}

/*
 * populate_grouprel_uniquekeys
 */
void
populate_grouprel_uniquekeys(PlannerInfo *root,
							 RelOptInfo *grouprel)
{
	Query *parse = root->parse;
	if (parse->hasTargetSRFs)
		return;

	if (parse->groupingSets)
		return;

	/* A Normal group by without grouping set */
	if (parse->groupClause)
		add_uniquekey_from_sortgroups(root,
									  grouprel,
									  root->parse->groupClause);
	else
		/* it has aggregation but without a group by, so must be one line return */
		add_uniquekey_for_onerow(grouprel);
}

/*
 * simple_copy_uniquekeys
 * Using a function for the one-line code makes us easy to check where we simply
 * copied the uniquiekeys.
 */
void
simple_copy_uniquekeys(RelOptInfo *oldrel,
					   RelOptInfo *newrel)
{
	newrel->uniquekeys = oldrel->uniquekeys;
}

/*
 *  populate_unionrel_uniquiekeys
 */
void
populate_unionrel_uniquiekeys(PlannerInfo *root,
							  RelOptInfo *unionrel)
{
	ListCell	*lc;
	List	*exprs = NIL;
	List	*colnos = NIL;
	int i = 1;

	Assert(unionrel->uniquekeys == NIL);

	foreach(lc,  unionrel->reltarget->exprs)
	{
		exprs = lappend(exprs, lfirst(lc));
		colnos = lappend_int(colnos, i);
		i++;
	}
	unionrel->uniquekeys = lappend(unionrel->uniquekeys,
								   makeUniqueKey(exprs, colnos, true, false));
}

/*
 * populate_joinrel_uniquekeys
 *
 * populate uniquekeys for joinrel. We will check each relation to see if it's
 * UniqueKey is still valid via innerrel_keeps_unique, if so, we add it to
 * joinrel.  The multi_nullvals field will be changed from false to true
 * for some outer join cases.

 * For the uniquekey in either baserel which can't be unique after join, we still
 * check to see if combination of UniqueKeys from both side is still useful for us.
 * if yes, we add it to joinrel as well. multi_nullvals is set to true if either
 * side have multi_nullvals equals true.
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
	bool	outer_is_onerow, inner_is_onerow;
	if (root->parse->hasTargetSRFs)
		return;

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

    /* Fast path */
	if (innerrel->uniquekeys == NIL || outerrel->uniquekeys == NIL)
		return;

	outer_is_onerow = relation_is_onerow(outerrel);
	inner_is_onerow = relation_is_onerow(innerrel);

	outerrel_ukey_ctx = initililze_uniquecontext_for_joinrel(joinrel, outerrel);
	innerrel_ukey_ctx = initililze_uniquecontext_for_joinrel(joinrel, innerrel);

	clause_list = gather_mergeable_joinclauses(joinrel, outerrel, innerrel,
											   restrictlist, jointype);


	if (innerrel_keeps_unique(root, outerrel, innerrel, clause_list, false))
	{
		foreach(lc, innerrel_ukey_ctx)
		{
			UniqueKeyContext ctx = (UniqueKeyContext)lfirst(lc);
			if (!list_is_subset(ctx->uniquekey->exprs, joinrel->reltarget->exprs))
			{
				/* The UniqueKey on baserel is not useful on the joinrel */
				ctx->useful = false;
				continue;
			}
			if ((jointype == JOIN_LEFT || jointype == JOIN_FULL) && !ctx->uniquekey->multi_nullvals)
			{
				/* Change the multi_nullvals to true at this case */
				joinrel->uniquekeys = lappend(joinrel->uniquekeys,
											  makeUniqueKey(ctx->uniquekey->exprs,
															ctx->uniquekey->positions,
															true,
															false));
			}
			else if (inner_is_onerow)
			{
				/* Since rows in innerrel can't be duplicated AND if innerrel is onerow,
				 * the join result will be onerow also as well. Note: onerow implies
				 * multi_nullvals = false.
				 */
				add_uniquekey_for_onerow(joinrel);
				return;
			}
			else
			{
				joinrel->uniquekeys = lappend(joinrel->uniquekeys, ctx->uniquekey);
				ctx->added_to_joinrel = true;
			}
		}
	}

	if (innerrel_keeps_unique(root, innerrel, outerrel, clause_list, true))
	{
		foreach(lc, outerrel_ukey_ctx)
		{
			UniqueKeyContext ctx = (UniqueKeyContext)lfirst(lc);
			if (!list_is_subset(ctx->uniquekey->exprs, joinrel->reltarget->exprs))
			{
				ctx->useful = false;
				continue;
			}
			/* NULL values in outer rel can be duplicated under JOIN_FULL only */
			if (jointype == JOIN_FULL && ctx->uniquekey->multi_nullvals)
			{
				joinrel->uniquekeys = lappend(joinrel->uniquekeys,
											  makeUniqueKey(ctx->uniquekey->exprs,
															ctx->uniquekey->positions,
															true,
															false));

			}
			else if (outer_is_onerow)
			{
				add_uniquekey_for_onerow(joinrel);
				return;
			}
			else
			{
				joinrel->uniquekeys = lappend(joinrel->uniquekeys, ctx->uniquekey);
				ctx->added_to_joinrel = true;
			}
		}
	}

	/* The combination of the UniqueKey from both sides is unique as well regardless of
	 * join type, But no bother to add it if its subset has been added already.
	 */
	foreach(lc, outerrel_ukey_ctx)
	{
		UniqueKeyContext ctx1 = (UniqueKeyContext) lfirst(lc);
		if (ctx1->added_to_joinrel || !ctx1->useful)
			continue;
		foreach(lc2, innerrel_ukey_ctx)
		{
			UniqueKeyContext ctx2 = (UniqueKeyContext) lfirst(lc2);
			List	*exprs = NIL, *colnos = NIL;
			bool multi_nullvals;
			if (ctx2->added_to_joinrel || !ctx2->useful)
				continue;
			exprs = list_copy(ctx1->uniquekey->exprs);
			colnos = list_copy(ctx1->uniquekey->positions);
			exprs = list_concat(exprs, ctx2->uniquekey->exprs);
			colnos = list_concat(colnos, ctx2->uniquekey->positions);

			multi_nullvals = ctx1->uniquekey->multi_nullvals || ctx2->uniquekey->multi_nullvals;
			joinrel->uniquekeys = lappend(joinrel->uniquekeys,
										  makeUniqueKey(exprs,
														colnos,
														multi_nullvals,
														/* All onerow cases has been handled above */
														false));
		}
	}
}


/*
 * Used to avoid multi scan of rel->reltarget->exprs, See populate_subquery_uniquekeys
 */
typedef struct SubqueryUniqueKeyData
{
	/*
	 * Only the Var reference to subquery's unique is unique as well, we can't
	 * guarantee others
	 */
	Var *var;

	/* The position of the var in the rel->reltarget */
	int pos;
} *SubqueryUniqueKeyContext;

/*
 * convert_subquery_uniquekeys
 *
 * currel is the RelOptInfo in current level, sub_final_rel is get from the fetch_upper_rel
 * we need to convert the UniqueKey from sub_final_rel to currel via the positions info in
 * UniqueKey. Example:
 *
 * select t2.colx from t1, (select max(y), colx from t3 group by colx) t2 where ..
 * The UniqueKey in sub_final_rel is Var(varno=1, varattrno=N), position=2.
 * the UniqueKey in currel will be Var(varno=2, varattrno=2), position= 1
 */
void convert_subquery_uniquekeys(PlannerInfo *root,
								 RelOptInfo *currel,
								 RelOptInfo *sub_final_rel)
{
	SubqueryUniqueKeyContext *ctx_array;
	SubqueryUniqueKeyContext ctx;
	Index max_colno_subq = 0;
	ListCell	*lc, *lc2;
	int pos = 0;

	if (sub_final_rel->uniquekeys == NIL)
		/* This should be a common case */
		return;

	if (relation_is_onerow(sub_final_rel))
	{
		add_uniquekey_for_onerow(currel);
		return;
	}
	/*
	 * Calculate max_colno in subquery. In fact we can check this with
	 * list_length(sub_final_rel->reltarget->exprs), However, reltarget
	 * is not set on UPPERREL_FINAL relation, so do it this way
	 */
	foreach(lc, sub_final_rel->uniquekeys)
	{
		UniqueKey * ukey = lfirst_node(UniqueKey, lc);
		foreach(lc2, ukey->positions)
		{
			Index colno = lfirst_int(lc2);
			if (max_colno_subq < colno)
				max_colno_subq = colno;
		}
	}

	Assert(max_colno_subq > 0);
	ctx_array = palloc0(sizeof(SubqueryUniqueKeyContext) * (max_colno_subq + 1));

	/*
	 * Create an array for each expr in currel->reltarget->exprs, the array index
	 * is the colno in subquery, so that we can get the expr quickly given a colno_subq
	 */
	foreach(lc, currel->reltarget->exprs)
	{
		Var *var;
		int colno_subq;
		pos++;
		if (!IsA(lfirst(lc), Var))
			continue;

		var = lfirst_node(Var, lc);
		colno_subq = var->varattno;
		if (colno_subq > max_colno_subq)
			continue;
		ctx_array[colno_subq] = palloc0(sizeof(struct SubqueryUniqueKeyData));
		ctx = ctx_array[colno_subq]; /* corresponding to subquery's uniquekey->positions[x] */
		ctx->pos = pos; /* the position in current targetlist,  will be used to set UniqueKey */
		ctx->var = var;
	}

	/* Convert the UniqueKey from sub_final_rel to currel */
	foreach(lc, sub_final_rel->uniquekeys)
	{
		UniqueKey * ukey = lfirst_node(UniqueKey, lc);
		bool uniquekey_useful = true;
		List	*exprs = NIL;
		List	*colnos = NIL;
		foreach(lc2, ukey->positions)
		{
			Index sub_colno = lfirst_int(lc2);
			ctx = ctx_array[sub_colno];
			if (ctx == NULL)
			{
				/* The column is not used outside */
				uniquekey_useful = false;
				break;
			}
			exprs = lappend(exprs, ctx->var);
			colnos = lappend_int(colnos, ctx->pos);
		}
		if (uniquekey_useful)
			currel->uniquekeys = lappend(currel->uniquekeys,
										 makeUniqueKey(exprs,
													   colnos,
													   ukey->multi_nullvals,
													   ukey->onerow));
	}
}


/*
 * innerrel_keeps_unique
 *
 * Check if Unique key of the innerrel is valid after join. innerrel's UniqueKey
 * will be still valid if innerrel's uniquekey mergeop outrerel's uniquekey exists
 * in clause_list.
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

	if (relation_is_onerow(innerrel))
		return true;

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
			/* If the clauselist match any uk from outerrel, the innerrel will be unique
			 * based on the fact that innerrel->uniquekeys != NIL which is checked at the
			 * beginning
			 */
			return true;
	}
	return false;
}


/*
 * relation_is_onerow
 * Check if it is a one-row relation by checking UniqueKey
 */
bool
relation_is_onerow(RelOptInfo *rel)
{
	UniqueKey *ukey;
	if (rel->uniquekeys == NIL)
		return false;
	ukey = linitial_node(UniqueKey, rel->uniquekeys);
	if (ukey->onerow)
	{
		/* Some helpful tiny check for UniqueKey */

		/* 1. We will only store one UniqueKey for this rel */
		Assert(list_length(rel->uniquekeys) == 1);
		/* 2. multi_nullvals must be false */
		Assert(!ukey->multi_nullvals);
		/* 3. exprs & positions must be NIL */
		Assert(ukey->exprs == NIL);
		Assert(ukey->positions == NIL);
	}
	return ukey->onerow;
}

/*
 * relation_has_uniquekeys_for
 *		Returns true if we have proofs that 'rel' cannot return multiple rows with
 *		the same values in each of 'exprs'.  Otherwise returns false.
 */
bool
relation_has_uniquekeys_for(PlannerInfo *root, RelOptInfo *rel, List *exprs)
{
	ListCell *lc;

	/* For UniqueKey->onerow case, the uniquekey->exprs is empty as well
	 * so we can't rely on list_is_subset to handle this special cases
	 */
	if (exprs == NIL)
		return false;

	foreach(lc, rel->uniquekeys)
	{
		UniqueKey *ukey = lfirst_node(UniqueKey, lc);
		if (ukey->multi_nullvals)
			continue;
		if (list_is_subset(ukey->exprs, exprs))
			return true;
	}
	return false;
}


/*
 * Examine the rel's restriction clauses for usable var = const clauses
 */
static List*
gather_mergeable_baserestrictlist(RelOptInfo *rel)
{
	List	*restrictlist = NIL;
	ListCell	*lc;
	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(lc);

		/*
		 * Note: can_join won't be set for a restriction clause, but
		 * mergeopfamilies will be if it has a mergejoinable operator and
		 * doesn't contain volatile functions.
		 */
		if (restrictinfo->mergeopfamilies == NIL)
			continue;			/* not mergejoinable */

		/*
		 * The clause certainly doesn't refer to anything but the given rel.
		 * If either side is pseudoconstant then we can use it.
		 */
		if (bms_is_empty(restrictinfo->left_relids))
		{
			/* righthand side is inner */
			restrictinfo->outer_is_left = true;
		}
		else if (bms_is_empty(restrictinfo->right_relids))
		{
			/* lefthand side is inner */
			restrictinfo->outer_is_left = false;
		}
		else
			continue;

		/* OK, add to list */
		restrictlist = lappend(restrictlist, restrictinfo);
	}
	return restrictlist;
}


/*
 * gather_mergeable_joinclauses
 */
static List*
gather_mergeable_joinclauses(RelOptInfo *joinrel,
							 RelOptInfo *outerrel,
							 RelOptInfo *innerrel,
							 List *restrictlist,
							 JoinType jointype)
{
	List	*clause_list = NIL;
	ListCell	*lc;
	foreach(lc, restrictlist)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *)lfirst(lc);
		if (IS_OUTER_JOIN(jointype) &&
			RINFO_IS_PUSHED_DOWN(restrictinfo, joinrel->relids))
			continue;

		/* Ignore if it's not a mergejoinable clause */
		if (!restrictinfo->can_join ||
			restrictinfo->mergeopfamilies == NIL)
			continue;			/* not mergejoinable */

		/*
		 * Check if clause has the form "outer op inner" or "inner op outer",
		 * and if so mark which side is inner.
		 */
		if (!clause_sides_match_join(restrictinfo, outerrel->relids, innerrel->relids))
			continue;			/* no good for these input relations */

		/* OK, add to list */
		clause_list = lappend(clause_list, restrictinfo);
	}
	return clause_list;
}


/*
 * Return true if uk = Const in the restrictlist
 */
static bool
match_index_to_baserestrictinfo(IndexOptInfo *unique_ind, List *restrictlist)
{
	int c = 0;

	/* A fast path to avoid the 2 loop scan */
	if (list_length(restrictlist) < unique_ind->ncolumns)
		return false;

	for(c = 0;  c < unique_ind->ncolumns; c++)
	{
		ListCell	*lc;
		bool	found_in_restrictinfo = false;
		foreach(lc, restrictlist)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
			Node	   *rexpr;

			/*
			 * The condition's equality operator must be a member of the
			 * index opfamily, else it is not asserting the right kind of
			 * equality behavior for this index.  We check this first
			 * since it's probably cheaper than match_index_to_operand().
			 */
			if (!list_member_oid(rinfo->mergeopfamilies, unique_ind->opfamily[c]))
				continue;

			/*
			 * XXX at some point we may need to check collations here too.
			 * For the moment we assume all collations reduce to the same
			 * notion of equality.
			 */

			/* OK, see if the condition operand matches the index key */
			if (rinfo->outer_is_left)
				rexpr = get_rightop(rinfo->clause);
			else
				rexpr = get_leftop(rinfo->clause);

			if (match_index_to_operand(rexpr, c, unique_ind))
			{
				found_in_restrictinfo = true;
				break;
			}
		}
		if (!found_in_restrictinfo)
			return false;
	}
	return true;
}

/*
 * add_uniquekey_from_index
 * 	We only add the Index Vars whose expr exists in rel->reltarget
 */
static void
add_uniquekey_from_index(RelOptInfo *rel, IndexOptInfo *unique_index)
{
	int	pos;
	List	*exprs = NIL;
	List	*positions = NIL;
	bool	multi_nullvals = false;

	/* Fast path. Check if the indexed columns are used in this relation
	 * If not, return fast.
	 */
	for(pos = 0; pos < unique_index->ncolumns; pos++)
	{
		int attno = unique_index->indexkeys[pos] - rel->min_attr;
		if (bms_is_empty(rel->attr_needed[attno]))
			return;
	}

	/* We still need to check the rel->reltarget->exprs to get the exprs and positions */
	for(pos = 0; pos < unique_index->ncolumns; pos++)
	{
		ListCell	*lc;
		bool	find_in_exprs = false;

		foreach(lc, rel->reltarget->exprs)
		{
			Var *var;
			if (!IsA(lfirst(lc), Var))
				continue;
			var = lfirst_node(Var, lc);
			if (match_index_to_operand((Node *)lfirst(lc), pos, unique_index))
			{
				find_in_exprs = true;
				exprs = lappend(exprs, lfirst(lc));
				positions = lappend_int(positions, pos+1);
				if (!bms_is_member(var->varattno - FirstLowInvalidHeapAttributeNumber,
								   rel->notnullattrs))
					multi_nullvals = true;
				break;
			}
		}
		if (!find_in_exprs)
			return;
	}

	if (exprs != NIL)
	{
		rel->uniquekeys = lappend(rel->uniquekeys,
								  makeUniqueKey(exprs, positions, multi_nullvals, false));
	}
}


/*
 * add_uniquekey_for_onerow
 * If we are sure about the relation only returns one row, then all the columns
 * are unique. There is no need to create UniqueKey for every expr, we just set
 * UniqueKey->onerow to true is OK
 */
void
add_uniquekey_for_onerow(RelOptInfo *rel)
{
	rel->uniquekeys = list_make1(makeUniqueKey(NIL, /* No need to set exprs */
											   NIL, /* No need to set positions */
											   false, /* onerow can't have multi_nullvals */
											   true));

}

/*
 * initililze_uniquecontext_for_joinrel
 * Return a List of UniqueKeyContext for an inputrel, we also filter out
 * all the uniquekeys which are not possible to use later
 */
static List *
initililze_uniquecontext_for_joinrel(RelOptInfo *joinrel,  RelOptInfo *inputrel)
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
 * clause_sides_match_join
 *	  Determine whether a join clause is of the right form to use in this join.
 *
 * We already know that the clause is a binary opclause referencing only the
 * rels in the current join.  The point here is to check whether it has the
 * form "outerrel_expr op innerrel_expr" or "innerrel_expr op outerrel_expr",
 * rather than mixing outer and inner vars on either side.  If it matches,
 * we set the transient flag outer_is_left to identify which side is which.
 */
static bool
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
 * Partitioned table Unique Keys.
 * The partition table unique key is maintained as:
 * 1. The index must be unique as usual.
 * 2. The index must contains partition key.
 * 3. The index must exist on all the child rel. see simple_indexinfo_equal for
 *    how we compare it.
 */

/* index_constains_partkey
 * return true if the index contains the partiton key.
 */
static bool
index_constains_partkey(RelOptInfo *partrel,  IndexOptInfo *ind)
{
	ListCell	*lc;
	int	i;
	Assert(IS_PARTITIONED_REL(partrel));

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
 * is COPIED from childrel and did some tiny changes(see simple_copy_indexinfo_to_parent)
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
		equal(ind1->indextlist, ind2->indextlist);
}

/*
 * Copy these macros from copyfuncs.c since I don't want make
 * simple_copy_indexinfo_to_parent public since it is a so customized copy.
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
 * Copy the IndexInfo from child index info to parent, which will be used to
 * 1. Test if the same index exists in all the childrels.
 * 2. if the parentrel->reltarget/basicrestrict info matches this index.
 * The copied and modified index is just used in this scope.
 */
static IndexOptInfo *
simple_copy_indexinfo_to_parent(RelOptInfo *parentrel,
								IndexOptInfo *from)
{
	IndexOptInfo *newnode = makeNode(IndexOptInfo);

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

	/*
	 * We have to change this to let the later index match (like pk = 1)
	 * rel->reltarget work
	 */
	ChangeVarNodes((Node*) newnode->indextlist,
				   from->rel->relid,
				   parentrel->relid, 0);
	newnode->rel = parentrel;
	return newnode;
}

/*
 * adjust_partition_unique_indexlist
 *
 * Check the current known global_unique_indexes to see if every index here
 * all exists in the given childrel, if not, it will be removed from
 * the list
 */
static void
adjust_partition_unique_indexlist(RelOptInfo *parentrel,
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
			p_ind_copy = simple_copy_indexinfo_to_parent(parentrel, p_ind);
			if (simple_indexinfo_equal(p_ind_copy, g_ind))
			{
				found_in_child = true;
				break;
			}
		}

		if (!found_in_child)
			/* There is no same index on other childrel, remove it */
			*global_unique_indexes = foreach_delete_current(*global_unique_indexes, lc);
	}
}

/* Helper function for groupres/distinctrel */
static void
add_uniquekey_from_sortgroups(PlannerInfo *root, RelOptInfo *rel, List *sortgroups)
{
	Query *parse = root->parse;
	ListCell *lc;
	List	*exprs = NIL,  *colnos = NIL;

	/* XXX: If there are some vars which is not in current levelsup, the semantic is
	 * imprecise, should we avoid it? levelsup = 1 is just a demo, maybe we need to
	 * check every level other than 0, if so, we need write another pull_var_walker.
	 */
	List	*upper_vars = pull_vars_of_level((Node*)sortgroups, 1);

	if (upper_vars != NIL)
		return;

	foreach(lc, sortgroups)
	{
		Index sortref = lfirst_node(SortGroupClause, lc)->tleSortGroupRef;
		int c = 1;
		foreach(lc, parse->targetList)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lc);
			if (tle->ressortgroupref == sortref)
			{
				exprs = lappend(exprs, tle->expr);
				colnos = lappend_int(colnos, c);
			}
			++c;
		}
	}
	rel->uniquekeys = lappend(rel->uniquekeys,
							  makeUniqueKey(exprs,
											colnos,
											false, /* sortgroupclause can't be multi_nullvals */
											relation_is_onerow(rel) /* should be always false */
								  ));
}
