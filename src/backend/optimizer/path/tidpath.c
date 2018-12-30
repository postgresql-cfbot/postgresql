/*-------------------------------------------------------------------------
 *
 * tidpath.c
 *	  Routines to determine which TID conditions are usable for scanning
 *	  a given relation, and create TidPaths accordingly.
 *
 * What we are looking for here is WHERE conditions of the forms:
 * - "CTID = pseudoconstant", which can be implemented by just fetching
 *    the tuple directly via heap_fetch().
 * - "CTID IN (pseudoconstant, ...)" or "CTID = ANY(pseudoconstant_array)"
 * - "CTID > pseudoconstant", etc. for >, >=, <, and <=.
 * - "CTID > pseudoconstant AND CTID < pseudoconstant AND ...", etc.
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
static bool IsTidBinaryExpression(OpExpr *node, int varno);
static bool IsTidEqualClause(OpExpr *node, int varno);
static bool IsTidRangeClause(OpExpr *node, int varno);
static bool IsTidEqualAnyClause(ScalarArrayOpExpr *node, int varno);
static List *MakeTidRangeQuals(List *quals);
static List *TidCompoundRangeQualFromExpr(Node *expr, int varno);
static List *TidQualFromExpr(Node *expr, int varno);
static List *TidQualFromBaseRestrictinfo(RelOptInfo *rel);


/* Quick check to see if `var` looks like CTID. */
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
 * where OP is assumed to be a binary.  We don't check opno -- that's usually
 * done by the caller -- but we check the numer of arguments.
 *
 * We check that the CTID Var belongs to relation "varno".  That is probably
 * redundant considering this is only applied to restriction clauses, but
 * let's be safe.
 */
static bool
IsTidBinaryExpression(OpExpr *node, int varno)
{
	Node	   *arg1,
			   *arg2,
			   *other;
	Var		   *var;

	/* Operator must be the expected one */
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

/*
 * Check to see if a clause is of the form
 *		CTID = pseudoconstant
 * or
 *		pseudoconstant = CTID
 */
static bool
IsTidEqualClause(OpExpr *node, int varno)
{
	if (node->opno != TIDEqualOperator)
		return false;
	return IsTidBinaryExpression(node, varno);
}

/*
 * Check to see if a clause is of the form
 *		CTID op pseudoconstant
 * or
 *		pseudoconstant op CTID
 * where op is a range comparison operator like >, >=, <, or <=.
 */
static bool
IsTidRangeClause(OpExpr *node, int varno)
{
	if (node->opno != TIDLessOperator &&
		node->opno != TIDLessEqOperator &&
		node->opno != TIDGreaterOperator &&
		node->opno != TIDGreaterEqOperator)
		return false;
	return IsTidBinaryExpression(node, varno);
}

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
 * Turn a list of range quals into the expected structure: if there's more than
 * one, wrap them in a top-level AND-clause.
 */
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
	ListCell   *l;
	List	   *found_quals = NIL;

	foreach(l, ((BoolExpr *) expr)->args)
	{
		Node	   *clause = (Node *) lfirst(l);

		/* If this clause contains a range qual, add it to the list. */
		if (is_opclause(clause) && IsTidRangeClause((OpExpr *) clause, varno))
			found_quals = lappend(found_quals, clause);
	}

	/* If we found any, make an AND clause out of them. */
	if (found_quals)
		return MakeTidRangeQuals(found_quals);
	else
		return NIL;
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
		if (rlst == NIL)
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
 * "CTID > ? AND CTID < ?", to be split across multiple items.  So we look for
 * range quals in all items and use them if any were found.
 */
static List *
TidQualFromBaseRestrictinfo(RelOptInfo *rel)
{
	List	   *rlst = NIL;
	ListCell   *l;
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

		/* If this clause contains a range qual, add it to the list. */
		if (is_opclause(clause) &&
			IsTidRangeClause((OpExpr *) clause, rel->relid))
		{
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
 *	  Path keys will be set to "CTID ASC" by default, or "CTID DESC" if it
 *	  looks more useful.
 *
 *	  Candidate paths are added to the rel's pathlist (using add_path).
 */
void
create_tidscan_paths(PlannerInfo *root, RelOptInfo *rel)
{
	Relids		required_outer;
	List	   *pathkeys = NIL;
	ScanDirection scandir = ForwardScanDirection;
	List	   *tidquals;

	/*
	 * We don't support pushing join clauses into the quals of a tidscan, but
	 * it could still have required parameterization due to LATERAL refs in
	 * its tlist.
	 */
	required_outer = rel->lateral_relids;

	tidquals = TidQualFromBaseRestrictinfo(rel);

	/*
	 * Look for a suitable direction by trying both forward and backward
	 * pathkeys.  But don't set any pathkeys if neither direction helps the
	 * scan (we don't want to generate tid paths for everything).
	 */
	if (has_useful_pathkeys(root, rel))
	{
		pathkeys = build_tidscan_pathkeys(root, rel, ForwardScanDirection);
		if (!pathkeys_contained_in(pathkeys, root->query_pathkeys))
		{
			pathkeys = build_tidscan_pathkeys(root, rel, BackwardScanDirection);
			if (pathkeys_contained_in(pathkeys, root->query_pathkeys))
				scandir = BackwardScanDirection;
			else
				pathkeys = NIL;
		}
	}
	else if (tidquals)
	{
		/*
		 * Otherwise, default to a forward scan -- but only if tid quals were
		 * found (we don't want to generate tid paths for everything).
		 */
		pathkeys = build_tidscan_pathkeys(root, rel, ForwardScanDirection);
	}

	/*
	 * If there are tidquals or some useful pathkeys were found, then it's
	 * worth generating a tidscan path.
	 */
	if (tidquals || pathkeys)
		add_path(rel, (Path *) create_tidscan_path(root, rel, tidquals, pathkeys,
												   scandir, required_outer));
}
