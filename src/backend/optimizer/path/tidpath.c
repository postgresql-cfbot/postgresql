/*-------------------------------------------------------------------------
 *
 * tidpath.c
 *	  Routines to determine which TID conditions are usable for scanning
 *	  a given relation, and create TidPaths accordingly.
 *
 * What we are looking for here is WHERE conditions of the forms:
 * - "CTID = c", which can be implemented by just fetching
 *    the tuple directly via heap_fetch().
 * - "CTID IN (pseudoconstant, ...)" or "CTID = ANY(pseudoconstant_array)"
 * - "CTID > pseudoconstant", etc. for >, >=, <, and <=.
 * - "CTID > pseudoconstant AND CTID < pseudoconstant", etc., with up to one
 *   lower bound and one upper bound.
 *
 * We can also handle OR'd conditions of the above form, such as
 * "(CTID = const1) OR (CTID >= const2) OR CTID IN (...)".
 *
 * We also support "WHERE CURRENT OF cursor" conditions (CurrentOfExpr),
 * which amount to "CTID = run-time-determined-TID".  These could in
 * theory be translated to a simple comparison of CTID to the result of
 * a function, but in practice it works better to keep the special node
 * representation all the way through to execution.
 *
 * There is currently no special support for joins involving CTID; in
 * particular nothing corresponding to best_inner_indexscan().  Since it's
 * not very useful to store TIDs of one table in another table, there
 * doesn't seem to be enough use-case to justify adding a lot of code
 * for that.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/tidpath.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/restrictinfo.h"


static bool IsTidVar(Var *var, int varno);
static bool IsTidComparison(OpExpr *node, int varno, Oid expected_comparison_operator);
static bool IsTidEqualAnyClause(ScalarArrayOpExpr *node, int varno);
static bool IsUsableRangeQual(Node *expr, int varno, bool want_lower_bound);
static List *MakeTidRangeQuals(List *quals);
static List *TidCompoundRangeQualFromExpr(Node *expr, int varno);
static List *TidQualFromExpr(Node *expr, int varno);
static List *TidQualFromBaseRestrictinfo(RelOptInfo *rel);


static bool
IsTidVar(Var *var, int varno)
{
	return (var->varattno == SelfItemPointerAttributeNumber &&
			var->vartype == TIDOID &&
			var->varno == varno &&
			var->varlevelsup == 0);
}

/*
 * Check to see if an opclause is of the form
 *		CTID OP pseudoconstant
 * or
 *		pseudoconstant OP CTID
 * where OP is the expected comparison operator.
 *
 * We check that the CTID Var belongs to relation "varno".  That is probably
 * redundant considering this is only applied to restriction clauses, but
 * let's be safe.
 */
static bool
IsTidComparison(OpExpr *node, int varno, Oid expected_comparison_operator)
{
	Node	   *arg1,
			   *arg2,
			   *other;
	Var		   *var;

	/* Operator must be the expected one */
	if (node->opno != expected_comparison_operator)
		return false;
	if (list_length(node->args) != 2)
		return false;
	arg1 = linitial(node->args);
	arg2 = lsecond(node->args);

	/* Look for CTID as either argument */
	other = NULL;
	if (arg1 && IsA(arg1, Var))
	{
		var = (Var *) arg1;
		if (IsTidVar(var, varno))
			other = arg2;
	}
	if (!other && arg2 && IsA(arg2, Var))
	{
		var = (Var *) arg2;
		if (IsTidVar(var, varno))
			other = arg1;
	}
	if (!other)
		return false;
	if (exprType(other) != TIDOID)
		return false;			/* probably can't happen */

	/* The other argument must be a pseudoconstant */
	if (!is_pseudo_constant_clause(other))
		return false;

	return true;				/* success */
}

#define IsTidEqualClause(node, varno)	IsTidComparison(node, varno, TIDEqualOperator)
#define IsTidLTClause(node, varno)		IsTidComparison(node, varno, TIDLessOperator)
#define IsTidLEClause(node, varno)		IsTidComparison(node, varno, TIDLessEqOperator)
#define IsTidGTClause(node, varno)		IsTidComparison(node, varno, TIDGreaterOperator)
#define IsTidGEClause(node, varno)		IsTidComparison(node, varno, TIDGreaterEqOperator)

#define IsTidRangeClause(node, varno)	(IsTidLTClause(node, varno) || \
										 IsTidLEClause(node, varno) || \
										 IsTidGTClause(node, varno) || \
										 IsTidGEClause(node, varno))

/*
 * Check to see if a clause is of the form
 *		CTID = ANY (pseudoconstant_array)
 */
static bool
IsTidEqualAnyClause(ScalarArrayOpExpr *node, int varno)
{
	Node	   *arg1,
			   *arg2;

	/* Operator must be tideq */
	if (node->opno != TIDEqualOperator)
		return false;
	if (!node->useOr)
		return false;
	Assert(list_length(node->args) == 2);
	arg1 = linitial(node->args);
	arg2 = lsecond(node->args);

	/* CTID must be first argument */
	if (arg1 && IsA(arg1, Var))
	{
		Var		   *var = (Var *) arg1;

		if (IsTidVar(var, varno))
		{
			/* The other argument must be a pseudoconstant */
			if (is_pseudo_constant_clause(arg2))
				return true;	/* success */
		}
	}

	return false;
}

/*
 * IsUsableRangeQual
 *		Check if the expr is range qual of the expected type.
 */
static bool
IsUsableRangeQual(Node *expr, int varno, bool want_lower_bound)
{
	if (is_opclause(expr) && IsTidRangeClause((OpExpr *) expr, varno))
	{
		bool		is_lower_bound = IsTidGTClause((OpExpr *) expr, varno) || IsTidGEClause((OpExpr *) expr, varno);
		Node	   *leftop = get_leftop((Expr *) expr);

		if (!IsA(leftop, Var) ||!IsTidVar((Var *) leftop, varno))
			is_lower_bound = !is_lower_bound;

		if (is_lower_bound == want_lower_bound)
			return true;
	}

	return false;
}

static List *
MakeTidRangeQuals(List *quals)
{
	if (list_length(quals) == 1)
		return quals;
	else
		return list_make1(make_andclause(quals));
}

/*
 * TidCompoundRangeQualFromExpr
 *
 * 		Extract a compound CTID range condition from the given qual expression
 */
static List *
TidCompoundRangeQualFromExpr(Node *expr, int varno)
{
	List	   *rlst = NIL;
	ListCell   *l;
	bool		found_lower = false;
	bool		found_upper = false;
	List	   *found_quals = NIL;

	foreach(l, ((BoolExpr *) expr)->args)
	{
		Node	   *clause = (Node *) lfirst(l);

		/* Check if this clause contains a range qual */
		if (!found_lower && IsUsableRangeQual(clause, varno, true))
		{
			found_lower = true;
			found_quals = lappend(found_quals, clause);
		}

		if (!found_upper && IsUsableRangeQual(clause, varno, false))
		{
			found_upper = true;
			found_quals = lappend(found_quals, clause);
		}
	}

	/* If one or both range quals was specified, use them. */
	if (found_quals)
		rlst = MakeTidRangeQuals(found_quals);

	return rlst;
}

/*
 *	Extract a set of CTID conditions from the given qual expression
 *
 *	Returns a List of CTID qual expressions (with implicit OR semantics
 *	across the list), or NIL if there are no usable conditions.
 *
 *	If the expression is an AND clause, we can use a CTID condition
 *	from any sub-clause.  If it is an OR clause, we must be able to
 *	extract a CTID condition from every sub-clause, or we can't use it.
 *
 *	In theory, in the AND case we could get CTID conditions from different
 *	sub-clauses, in which case we could try to pick the most efficient one.
 *	In practice, such usage seems very unlikely, so we don't bother; we
 *	just exit as soon as we find the first candidate.
 */
static List *
TidQualFromExpr(Node *expr, int varno)
{
	List	   *rlst = NIL;
	ListCell   *l;

	if (is_opclause(expr))
	{
		/* base case: check for tideq opclause */
		if (IsTidEqualClause((OpExpr *) expr, varno))
			rlst = list_make1(expr);
		else if (IsTidRangeClause((OpExpr *) expr, varno))
			rlst = list_make1(expr);
	}
	else if (expr && IsA(expr, ScalarArrayOpExpr))
	{
		/* another base case: check for tid = ANY clause */
		if (IsTidEqualAnyClause((ScalarArrayOpExpr *) expr, varno))
			rlst = list_make1(expr);
	}
	else if (expr && IsA(expr, CurrentOfExpr))
	{
		/* another base case: check for CURRENT OF on this rel */
		if (((CurrentOfExpr *) expr)->cvarno == varno)
			rlst = list_make1(expr);
	}
	else if (and_clause(expr))
	{
		/* look for a range qual in the clause */
		rlst = TidCompoundRangeQualFromExpr(expr, varno);

		/* if no range qual was found, look for any other TID qual */
		if (!rlst)
		{
			foreach(l, ((BoolExpr *) expr)->args)
			{
				rlst = TidQualFromExpr((Node *) lfirst(l), varno);
				if (rlst)
					break;
			}
		}
	}
	else if (or_clause(expr))
	{
		foreach(l, ((BoolExpr *) expr)->args)
		{
			List	   *frtn = TidQualFromExpr((Node *) lfirst(l), varno);

			if (frtn)
				rlst = list_concat(rlst, frtn);
			else
			{
				if (rlst)
					list_free(rlst);
				rlst = NIL;
				break;
			}
		}
	}
	return rlst;
}

/*
 * Extract a set of CTID conditions from the rel's baserestrictinfo list
 *
 * Normally we just use the first RestrictInfo item with some usable quals,
 * but it's also possible for a good compound range qual, such as
 * "CTID > ? AND CTID < ?", to be split across two items.  So we look for
 * lower/upper bound range quals in all items and use them if any were found.
 * In principal there might be more than one lower or upper bound), but we
 * just use the first one found of each type.
 */
static List *
TidQualFromBaseRestrictinfo(RelOptInfo *rel)
{
	List	   *rlst = NIL;
	ListCell   *l;
	bool		found_lower = false;
	bool		found_upper = false;
	List	   *found_quals = NIL;

	foreach(l, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);
		Node	   *clause = (Node *) rinfo->clause;

		/*
		 * If clause must wait till after some lower-security-level
		 * restriction clause, reject it.
		 */
		if (!restriction_is_securely_promotable(rinfo, rel))
			continue;

		/* Look for lower and upper bound range quals. */
		if (!found_lower && IsUsableRangeQual((Node *) clause, rel->relid, true))
		{
			found_lower = true;
			found_quals = lappend(found_quals, clause);
			continue;
		}

		if (!found_upper && IsUsableRangeQual((Node *) clause, rel->relid, false))
		{
			found_upper = true;
			found_quals = lappend(found_quals, clause);
			continue;
		}

		/* Look for other TID quals. */
		rlst = TidQualFromExpr((Node *) clause, rel->relid);
		if (rlst)
			break;
	}

	/* Use a range qual if any were found. */
	if (found_quals)
		rlst = MakeTidRangeQuals(found_quals);

	return rlst;
}

/*
 * create_tidscan_paths
 *	  Create paths corresponding to direct TID scans of the given rel.
 *
 *	  Path keys and direction will be set on the scans if it looks useful.
 *
 *	  Candidate paths are added to the rel's pathlist (using add_path).
 */
void
create_tidscan_paths(PlannerInfo *root, RelOptInfo *rel)
{
	Relids		required_outer;
	List	   *pathkeys = NULL;
	ScanDirection direction = ForwardScanDirection;
	List	   *tidquals;

	/*
	 * We don't support pushing join clauses into the quals of a tidscan, but
	 * it could still have required parameterization due to LATERAL refs in
	 * its tlist.
	 */
	required_outer = rel->lateral_relids;

	/*
	 * Try to determine the best scan direction and create some useful
	 * pathkeys.
	 */
	if (has_useful_pathkeys(root, rel))
	{
		/*
		 * Build path keys corresponding to ORDER BY ctid ASC, and check
		 * whether they will be useful for this scan.  If not, build path keys
		 * for DESC, and try that; set the direction to BackwardScanDirection
		 * if so.  If neither of them will be useful, no path keys will be
		 * set.
		 */
		pathkeys = build_tidscan_pathkeys(root, rel, ForwardScanDirection);
		if (!pathkeys_contained_in(pathkeys, root->query_pathkeys))
		{
			pathkeys = build_tidscan_pathkeys(root, rel, BackwardScanDirection);
			if (pathkeys_contained_in(pathkeys, root->query_pathkeys))
				direction = BackwardScanDirection;
			else
				pathkeys = NULL;
		}
	}

	tidquals = TidQualFromBaseRestrictinfo(rel);

	/*
	 * If there are tidquals or some useful pathkeys were found, then it's
	 * worth generating a tidscan path.
	 */
	if (tidquals || pathkeys)
		add_path(rel, (Path *) create_tidscan_path(root, rel, tidquals, pathkeys,
												   direction, required_outer));
}
