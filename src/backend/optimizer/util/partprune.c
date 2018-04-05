/*-------------------------------------------------------------------------
 *
 * partprune.c
 *		Parses clauses attempting to match them up to partition keys of a
 *		given relation and generates a set of "pruning steps", which can be
 *		later "executed" either from the planner or the executor to determine
 *		the minimum set of partitions which match the given clauses.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		  src/backend/optimizer/util/partprune.c
 *
 *-------------------------------------------------------------------------
*/

#include "postgres.h"

#include "access/hash.h"
#include "access/nbtree.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/partprune.h"
#include "optimizer/planner.h"
#include "optimizer/predtest.h"
#include "optimizer/prep.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteManip.h"
#include "utils/lsyscache.h"

/*
 * Information about a clause matched with a partition key.
 */
typedef struct PartClauseInfo
{
	int			keyno;			/* Partition key number (0 to partnatts - 1)  */
	Oid			opno;			/* operator used to compare partkey to 'expr' */
	bool		op_is_ne;		/* is clause's original operator <> ? */
	Expr	   *expr;			/* The expr the partition key is being
								 * compared to */
	Oid			cmpfn;			/* Oid of function to compare 'expr' to the
								 * partition key */

	/* cached info. */
	int			op_strategy;
} PartClauseInfo;

/*
 * PartClauseMatchStatus
 *		Describes the result match_clause_to_partition_key produces for a
 *		given clause and the partition key to match with that are passed to it
 */
typedef enum PartClauseMatchStatus
{
	PARTCLAUSE_NOMATCH,
	PARTCLAUSE_MATCH_CLAUSE,
	PARTCLAUSE_MATCH_NULLNESS,
	PARTCLAUSE_MATCH_STEPS,
	PARTCLAUSE_MATCH_CONTRADICT,
	PARTCLAUSE_UNSUPPORTED
}			PartClauseMatchStatus;

/*
 * GeneratePruningStepsContext
 *		Information about the current state of generation of "pruning steps"
 *		for a given set of clauses
 *
 * generate_partition_pruning_steps() initializes an instance of this struct,
 * which is used throughout the step generation process.
 */
typedef struct GeneratePruningStepsContext
{
	int		next_step_id;
	List   *steps;
}			GeneratePruningStepsContext;

static List *generate_partition_pruning_steps_internal(RelOptInfo *rel,
									  GeneratePruningStepsContext *context,
									  List *clauses,
									  bool *constfalse);
static PartClauseMatchStatus match_clause_to_partition_key(RelOptInfo *rel,
								GeneratePruningStepsContext *context,
								Expr *clause, Expr *partkey, int partkeyidx,
								bool *key_is_null, bool *key_is_not_null,
								PartClauseInfo **pc, List **clause_steps);
static bool match_boolean_partition_clause(Oid partopfamily, Expr *clause,
							   Expr *partkey, Expr **rightop);
static PartitionPruneStep *generate_pruning_steps_from_opexprs(
								PartitionScheme part_scheme,
								GeneratePruningStepsContext *context,
								List **keyclauses,
								Bitmapset *nullkeys);
static List *get_steps_using_prefix(GeneratePruningStepsContext *context,
					   int step_opstrategy,
					   bool step_op_is_ne,
					   Expr *step_lastexpr,
					   Oid step_lastcmpfn,
					   int step_lastkeyno,
					   Bitmapset *step_nullkeys,
					   List *prefix);
static List *get_steps_using_prefix_recurse(GeneratePruningStepsContext *context,
							   int step_opstrategy,
							   bool step_op_is_ne,
							   Expr *step_lastexpr,
							   Oid step_lastcmpfn,
							   int step_lastkeyno,
							   Bitmapset *step_nullkeys,
							   ListCell *start,
							   List *step_exprs,
							   List *step_cmpfns);
static PartitionPruneStep *generate_pruning_step_op(GeneratePruningStepsContext *context,
							int opstrategy, bool op_is_ne,
							List *exprs, List *cmpfns, Bitmapset *nullkeys);
static PartitionPruneStep *generate_pruning_step_combine(GeneratePruningStepsContext *context,
							List *source_stepids,
							PartitionPruneCombineOp combineOp);

/*
 * prune_append_rel_partitions
 *		Returns RT indexes of the minimum set of child partitions which must
 *		be scanned to satisfy rel's baserestrictinfo quals.
 *
 * Callers must ensure that 'rel' is a partitioned table.
 */
Relids
prune_append_rel_partitions(RelOptInfo *rel)
{
	Relids		result = NULL;
	List	   *clauses = rel->baserestrictinfo;
	List	   *pruning_steps;
	bool		constfalse;

	Assert(clauses != NIL);
	Assert(rel->part_scheme != NULL);

	/* Quick exit. */
	if (rel->nparts == 0)
		return NULL;

	/* process clauses */
	pruning_steps = generate_partition_pruning_steps(rel, clauses,
													 &constfalse);

	if (!constfalse)
	{
		/* Actual pruning happens here. */
		PartitionPruneContext context;
		Bitmapset  *partindexes;
		int			i;

		/* Initiate partition pruning using clauses. */
		memset(&context, 0, sizeof(context));
		context.strategy = rel->part_scheme->strategy;
		context.partnatts = rel->part_scheme->partnatts;
		context.partopfamily = rel->part_scheme->partopfamily;
		context.partopcintype = rel->part_scheme->partopcintype;
		context.partcollation = rel->part_scheme->partcollation;
		context.partsupfunc = rel->part_scheme->partsupfunc;
		context.nparts = rel->nparts;
		context.boundinfo = rel->boundinfo;

		partindexes = get_matching_partitions(&context, pruning_steps);

		/* Add selected partitions' RT indexes to result. */
		i = -1;
		while ((i = bms_next_member(partindexes, i)) >= 0)
			result = bms_add_member(result, rel->part_rels[i]->relid);
	}

	return result;
}

/*
 * generate_partition_pruning_steps
 *		Processes 'clauses' and returns a list of "partition pruning steps"
 *
 * If any of the clause in the input list is a pseudo-constant "false",
 * *constfalse is set to true upon return.
 */
List *
generate_partition_pruning_steps(RelOptInfo *rel, List *clauses,
								 bool *constfalse)
{
	GeneratePruningStepsContext context;

	context.next_step_id = 0;
	context.steps = NIL;

	/* The clauses list may be modified below, so better make a copy. */
	clauses = list_copy(clauses);

	/*
	 * For sub-partitioned tables there's a corner case where if the
	 * sub-partitioned table shares any partition keys with its parent, then
	 * it's possible that the partitioning hierarchy allows the parent
	 * partition to only contain a narrower range of values than the
	 * sub-partitioned table does.  In this case it is possible that we'd
	 * include partitions that could not possibly have any tuples matching
	 * 'clauses'.  The possibility of such a partition arrangement is perhaps
	 * unlikely for non-default partitions, but it may be more likely in the
	 * case of default partitions, so we'll add the parent partition table's
	 * partition qual to the clause list in this case only.  This may result
	 * in the default partition being eliminated.
	 */
	if (partition_bound_has_default(rel->boundinfo) &&
		rel->partition_qual != NIL)
	{
		List	   *partqual = rel->partition_qual;

		partqual = (List *) expression_planner((Expr *) partqual);

		/* Fix Vars to have the desired varno */
		if (rel->relid != 1)
			ChangeVarNodes((Node *) partqual, 1, rel->relid, 0);

		clauses = list_concat(clauses, partqual);
	}

	/* Down into the rabbit-hole. */
	(void) generate_partition_pruning_steps_internal(rel, &context, clauses,
													 constfalse);

	return context.steps;
}

/* Module-local functions */

/*
 * generate_partition_pruning_steps_internal
 *		Processes 'clauses' to generate partition pruning steps.
 *
 * From OpExpr clauses that are mutually AND'd, we find combinations of those
 * that match to the partition key columns and for every such combination,
 * we emit a PartitionPruneStepOp containing a vector of expressions whose
 * values are used as a look up key to search partitions by comparing the
 * values with partition bounds.  Relevant details of the operator and a
 * vector of (possibly cross-type) comparison functions is also included with
 * each step.
 *
 * For BoolExpr clauses, we recursively generate steps for each of its
 * arguments and generate PartitionPruneStepCombine step that will combine
 * results of those steps.
 *
 * All of the generated steps are added to the context's steps List and each
 * one gets an identifier which is unique across all recursive invocations.
 *
 * If when going through clauses, we find any that are marked as pseudoconstant
 * and contains a constant false value, we stop generating any further steps
 * and simply return NIL (that is, no pruning steps) after setting *constfalse
 * to true.  The caller should consider all partitions as pruned in that case.
 * We may do the same if we find that mutually contradictory clauses are
 * present, but were not turned into a pseudoconstant at higher levels.
 *
 * Note: the 'clauses' List may be modified inside this function. Callers may
 * like to make a copy of it before passing them to this function.
 */
static List *
generate_partition_pruning_steps_internal(RelOptInfo *rel,
									GeneratePruningStepsContext *context,
										  List *clauses,
										  bool *constfalse)
{
	PartitionScheme part_scheme = rel->part_scheme;
	List	   *keyclauses[PARTITION_MAX_KEYS];
	Bitmapset  *nullkeys = NULL,
			   *notnullkeys = NULL;
	bool		generate_opsteps = false;
	List	   *result = NIL;
	ListCell   *lc;

	*constfalse = false;
	memset(keyclauses, 0, sizeof(keyclauses));
	foreach(lc, clauses)
	{
		Expr	   *clause = (Expr *) lfirst(lc);
		int			i;

		if (IsA(clause, RestrictInfo))
		{
			RestrictInfo *rinfo = (RestrictInfo *) clause;

			clause = rinfo->clause;
			if (rinfo->pseudoconstant &&
				IsA(rinfo->clause, Const) &&
				!DatumGetBool(((Const *) clause)->constvalue))
			{
				*constfalse = true;
				return NIL;
			}
		}

		/* Get the BoolExpr's out of the way. */
		if (IsA(clause, BoolExpr))
		{
			/*
			 * Generate steps for arguments.
			 *
			 * While steps generated for the arguments themselves will be
			 * added to context->steps during recursion and will be evaluated
			 * independently, collect their step IDs to be stored in the
			 * combine step we'll be creating.
			 */
			if (or_clause((Node *) clause))
			{
				List	   *arg_stepids = NIL;
				bool		all_args_constfalse = true;
				ListCell   *lc1;

				/*
				 * Get pruning step for each arg.  If we get constfalse for
				 * all args, it means the OR expression is false as a whole.
				 */
				foreach(lc1, ((BoolExpr *) clause)->args)
				{
					Expr	   *arg = lfirst(lc1);
					bool		arg_constfalse;
					List	   *argsteps;

					argsteps =
						generate_partition_pruning_steps_internal(rel, context,
																  list_make1(arg),
																  &arg_constfalse);
					if (!arg_constfalse)
						all_args_constfalse = false;

					if (argsteps != NIL)
					{
						PartitionPruneStep *step;

						Assert(list_length(argsteps) == 1);
						step =  (PartitionPruneStep *) linitial(argsteps);
						arg_stepids = lappend_int(arg_stepids, step->step_id);
					}
					else
					{
						/*
						 * No steps either means that arg_constfalse is true
						 * or the arg didn't contain a clause matching this
						 * partition key.
						 *
						 * In case of the latter, we cannot prune using such
						 * an arg.  To indicate that to the pruning code, we
						 * must construct a dummy PartitionPruneStepCombine
						 * whose source_stepids is set to an empty List.
						 * However, if we can prove using constraint exclusion
						 * that the clause refutes the table's partition
						 * constraint (if it's sub-partitioned), we need not
						 * bother with that.  That is, we effectively ignore
						 * this OR arm.
						 */
						List	   *partconstr = rel->partition_qual;
						PartitionPruneStep *orstep;

						/* Just ignore this argument. */
						if (arg_constfalse)
							continue;

						if (partconstr)
						{
							partconstr = (List *)
								expression_planner((Expr *) partconstr);
							if (rel->relid != 1)
								ChangeVarNodes((Node *) partconstr, 1,
											   rel->relid, 0);
							if (predicate_refuted_by(partconstr,
													 list_make1(arg),
													 false))
								continue;
						}

						orstep = generate_pruning_step_combine(context,
															   NIL,
															   COMBINE_UNION);
						arg_stepids = lappend_int(arg_stepids,
												  orstep->step_id);
					}
				}

				*constfalse = all_args_constfalse;

				/* Check if any contradicting clauses were found */
				if (*constfalse)
					return NIL;

				if (arg_stepids != NIL)
					result =
						lappend(result,
								generate_pruning_step_combine(context,
															  arg_stepids,
															  COMBINE_UNION));
				continue;
			}
			else if (and_clause((Node *) clause))
			{
				List	   *args = ((BoolExpr *) clause)->args;
				List	   *argsteps,
						   *arg_stepids = NIL;
				ListCell   *lc1;

				/*
				 * args may itself contain clauses of arbitrary type, so just
				 * recurse and later combine the component partitions sets
				 * using a combine step.
				 */
				argsteps =
					generate_partition_pruning_steps_internal(rel,
															  context,
															  args,
															  constfalse);
				if (*constfalse)
					return NIL;

				foreach (lc1, argsteps)
				{
					PartitionPruneStep *step = lfirst(lc1);

					arg_stepids = lappend_int(arg_stepids, step->step_id);
				}

				if (arg_stepids)
					result =
						lappend(result,
								generate_pruning_step_combine(context,
														  arg_stepids,
														  COMBINE_INTERSECT));
				continue;
			}

			/*
			 * Fall-through for a NOT clause, which if it's a Boolean clause,
			 * will be handled in match_clause_to_partition_key(). We
			 * currently don't perform any pruning for more complex NOT
			 * clauses.
			 */
		}

		/*
		 * Must be a clause for which we can check if one of its args matches
		 * the partition key.
		 */
		for (i = 0; i < part_scheme->partnatts; i++)
		{
			Expr	   *partkey = linitial(rel->partexprs[i]);
			bool		unsupported_clause = false,
						key_is_null = false,
						key_is_not_null = false;
			PartClauseInfo *pc = NULL;
			List	   *clause_steps = NIL;

			switch (match_clause_to_partition_key(rel, context,
												  clause, partkey, i,
												  &key_is_null,
												  &key_is_not_null,
												  &pc, &clause_steps))
			{
				case PARTCLAUSE_MATCH_CLAUSE:
					Assert(pc != NULL);

					/*
					 * Since we only allow strict operators, check for any
					 * contradicting IS NULL.
					 */
					if (bms_is_member(i, nullkeys))
					{
						*constfalse = true;
						return NIL;
					}
					generate_opsteps = true;
					keyclauses[i] = lappend(keyclauses[i], pc);
					break;

				case PARTCLAUSE_MATCH_NULLNESS:
					if (key_is_null)
					{
						/* check for conflicting IS NOT NULL */
						if (bms_is_member(i, notnullkeys))
						{
							*constfalse = true;
							return NIL;
						}
						nullkeys = bms_add_member(nullkeys, i);
					}
					else if (key_is_not_null)
					{
						/* check for conflicting IS NULL */
						if (bms_is_member(i, nullkeys))
						{
							*constfalse = true;
							return NIL;
						}
						notnullkeys = bms_add_member(notnullkeys, i);
					}
					else
						Assert(false);
					break;

				case PARTCLAUSE_MATCH_STEPS:
					Assert(clause_steps != NIL);
					result = list_concat(result, clause_steps);
					break;

				case PARTCLAUSE_MATCH_CONTRADICT:
					/* We've nothing more to do if a contradiction was found. */
					*constfalse = true;
					return NIL;

				case PARTCLAUSE_NOMATCH:
					/*
					 * Clause didn't match this key, but it might match the
					 * next one.
					 */
					continue;

				case PARTCLAUSE_UNSUPPORTED:
					/* This clause cannot be used for pruning. */
					unsupported_clause = true;
					break;

				default:
					Assert(false);
					break;
			}

			/* go check the next clause. */
			if (unsupported_clause)
				break;
		}
	}

	/*
	 * If generate_opsteps is set to false it means no OpExprs were directly
	 * present in the input list.
	 */
	if (!generate_opsteps)
	{
		/*
		 * Generate one prune step for the information derived from IS NULL, if
		 * any.  To prune hash partitions, we must have found IS NULL clauses
		 * for all partition keys.
		 */
		if (!bms_is_empty(nullkeys) &&
			(part_scheme->strategy != PARTITION_STRATEGY_HASH ||
			 bms_num_members(nullkeys) == part_scheme->partnatts))
			result =
				lappend(result,
						generate_pruning_step_op(context, 0, false, NIL, NIL,
												 nullkeys));

		/*
		 * Note that for IS NOT NULL clauses, simply having step suffices;
		 * there is no need to propagate the exact details of which keys are
		 * required to be NOT NULL.  Hash partitioning expects to see actual
		 * values to perform any pruning.
		 */
		if (!bms_is_empty(notnullkeys) &&
			part_scheme->strategy != PARTITION_STRATEGY_HASH)
			result =
				lappend(result,
						generate_pruning_step_op(context, 0, false,
												 NIL, NIL, NULL));
	}
	else
	{
		PartitionPruneStep *step;

		/* Generate pruning steps from OpExpr clauses in keyclauses. */
		step = generate_pruning_steps_from_opexprs(part_scheme, context,
												   keyclauses, nullkeys);
		if (step != NULL)
			result = lappend(result, step);
	}

	/*
	 * Finally, results from all entries appearing in result should be
	 * combined using an INTERSECT combine step, if there are more than 1.
	 */
	if (list_length(result) > 1)
	{
		List *step_ids = NIL;

		foreach(lc, result)
		{
			PartitionPruneStep *step = lfirst(lc);

			step_ids = lappend_int(step_ids, step->step_id);
		}

		if (step_ids != NIL)
			result = lappend(result,
							 generate_pruning_step_combine(context, step_ids,
														   COMBINE_INTERSECT));
	}

	return result;
}

/*
 * If the partition key has a collation, then the clause must have the same
 * input collation.  If the partition key is non-collatable, we assume the
 * collation doesn't matter, because while collation wasn't considered when
 * performing partitioning, the clause still may have a collation assigned
 * due to the other input being of a collatable type.
 */
#define PartCollMatchesExprColl(partcoll, exprcoll) \
	((partcoll) == InvalidOid || (partcoll) == (exprcoll))

/*
 * match_clause_to_partition_key
 *		Attempt to match the given 'clause' with the specified partition key.
 *
 * Return value:
 *
 *	One of PARTCLAUSE_MATCH_* enum values if the clause is successfully
 *	matched to the partition key.  If it is PARTCLAUSE_MATCH_CONTRADICT, then
 *	this means the clause is self-contradictory (which can happen only if it's
 *	a BoolExpr whose arguments may be self-contradictory)
 *
 * 	PARTCLAUSE_NOMATCH if the clause doesn't match *this* partition key but
 *	the caller should continue trying because it may match a subsequent key
 *
 *	PARTCLAUSE_UNSUPPORTED if the clause cannot be used for pruning at all,
 *	even if it may have been matched with a key, due to one of its properties,
 *	such as volatility of the arguments
 *
 * Based on the returned enum value, different output arguments are set as
 * follows:
 *
 *	PARTCLAUSE_UNSUPPORTED or
 *	PARTCLAUSE_NOMATCH or
 *	PARTCLAUSE_MATCH_CONTRADICT: None set (caller shouldn't rely on any of
 *	them being set)
 *
 *	PARTCLAUSE_MATCH_CLAUSE: *pc set to PartClauseInfo constructed for the
 *	matched clause
 *
 *	PARTCLAUSE_MATCH_NULLNESS: either *key_is_null or *key_is_not_null set
 *	based on whether the matched clause was a IS NULL or IS NOT NULL clause,
 *	respectively
 *
 *	PARTCLAUSE_MATCH_STEPS: *clause_steps set to list of "partition pruning
 *	step(s)" generated for the clause due to it being a BoolExpr or a
 *	ScalarArrayOpExpr that's turned into one
 */
static PartClauseMatchStatus
match_clause_to_partition_key(RelOptInfo *rel,
							  GeneratePruningStepsContext *context,
							  Expr *clause, Expr *partkey, int partkeyidx,
							  bool *key_is_null, bool *key_is_not_null,
							  PartClauseInfo **pc, List **clause_steps)
{
	PartitionScheme part_scheme = rel->part_scheme;
	Expr	   *expr;
	Oid			partopfamily = part_scheme->partopfamily[partkeyidx],
				partcoll = part_scheme->partcollation[partkeyidx];

	/*
	 * Recognize specially shaped clauses that match with the Boolean
	 * partition key.
	 */
	if (match_boolean_partition_clause(partopfamily, clause, partkey, &expr))
	{
		*pc = (PartClauseInfo *) palloc(sizeof(PartClauseInfo));
		(*pc)->keyno = partkeyidx;
		/* Do pruning with the Boolean equality operator. */
		(*pc)->opno = BooleanEqualOperator;
		(*pc)->op_is_ne = false;
		(*pc)->expr = expr;
		/* We know that expr is of Boolean type. */
		(*pc)->cmpfn = rel->part_scheme->partsupfunc[partkeyidx].fn_oid;
		(*pc)->op_strategy = InvalidStrategy;

		return PARTCLAUSE_MATCH_CLAUSE;
	}
	else if (IsA(clause, OpExpr) &&
			 list_length(((OpExpr *) clause)->args) == 2)
	{
		OpExpr	   *opclause = (OpExpr *) clause;
		Expr	   *leftop,
				   *rightop;
		Oid			commutator = InvalidOid,
					negator = InvalidOid;
		Oid			cmpfn;
		Oid			exprtype;
		bool		is_opne_listp = false;

		leftop = (Expr *) get_leftop(clause);
		if (IsA(leftop, RelabelType))
			leftop = ((RelabelType *) leftop)->arg;
		rightop = (Expr *) get_rightop(clause);
		if (IsA(rightop, RelabelType))
			rightop = ((RelabelType *) rightop)->arg;

		/* check if the clause matches this partition key */
		if (equal(leftop, partkey))
			expr = rightop;
		else if (equal(rightop, partkey))
		{
			expr = leftop;
			commutator = get_commutator(opclause->opno);

			/* nothing we can do unless we can swap the operands */
			if (!OidIsValid(commutator))
				return PARTCLAUSE_UNSUPPORTED;
		}
		else
			/* clause does not match this partition key, but perhaps next. */
			return PARTCLAUSE_NOMATCH;

		/*
		 * Partition key also consists of a collation that's specified for it,
		 * so try to match it too.  There may be multiple keys with the same
		 * expression but different collations.
		 */
		if (!PartCollMatchesExprColl(partcoll, opclause->inputcollid))
			return PARTCLAUSE_NOMATCH;

		/*
		 * Matched with this key.  Now check various properties of the clause
		 * to see if it's sane to use it for pruning.  If any of the
		 * properties makes it unsuitable for pruning, then the clause is
		 * useless no matter which key it's matched to.
		 */

		/*
		 * Only allow strict operators.  This will guarantee nulls are
		 * filtered.
		 */
		if (!op_strict(opclause->opno))
			return PARTCLAUSE_UNSUPPORTED;

		/* We can't use any volatile expressions to prune partitions. */
		if (contain_volatile_functions((Node *) expr))
			return PARTCLAUSE_UNSUPPORTED;

		/*
		 * Normally we only bother with operators that are listed as being
		 * part of the partitioning operator family.  But we make an exception
		 * in one case -- operators named '<>' are not listed in any operator
		 * family whatsoever, in which case, we try to perform partition
		 * pruning with it only if list partitioning is in use.
		 */
		if (!op_in_opfamily(opclause->opno, partopfamily))
		{
			if (part_scheme->strategy != PARTITION_STRATEGY_LIST)
				return PARTCLAUSE_UNSUPPORTED;

			/*
			 * To confirm if the operator is really '<>', check if its negator
			 * is a btree equality operator.
			 */
			negator = get_negator(opclause->opno);
			if (OidIsValid(negator) && op_in_opfamily(negator, partopfamily))
			{
				Oid			lefttype;
				Oid			righttype;
				int			strategy;

				get_op_opfamily_properties(negator, partopfamily, false,
										   &strategy, &lefttype, &righttype);

				if (strategy == BTEqualStrategyNumber)
					is_opne_listp = true;
			}

			/* Operator isn't really what we were hoping it'd be. */
			if (!is_opne_listp)
				return PARTCLAUSE_UNSUPPORTED;
		}

		/* Check if we're going to need a cross-type comparison function. */
		exprtype = exprType((Node *) expr);
		if (exprtype != part_scheme->partopcintype[partkeyidx])
		{
			switch (part_scheme->strategy)
			{
				case PARTITION_STRATEGY_LIST:
				case PARTITION_STRATEGY_RANGE:
					cmpfn =
					get_opfamily_proc(part_scheme->partopfamily[partkeyidx],
									  part_scheme->partopcintype[partkeyidx],
									  exprtype, BTORDER_PROC);
					break;

				case PARTITION_STRATEGY_HASH:
					cmpfn =
					get_opfamily_proc(part_scheme->partopfamily[partkeyidx],
									  exprtype, exprtype, HASHEXTENDED_PROC);
					break;

				default:
					elog(ERROR, "invalid partition strategy: %c",
						 part_scheme->strategy);
					break;
			}

			/* If we couldn't find one, we cannot use this expression. */
			if (!OidIsValid(cmpfn))
				return PARTCLAUSE_UNSUPPORTED;
		}
		else
			cmpfn = part_scheme->partsupfunc[partkeyidx].fn_oid;

		*pc = (PartClauseInfo *) palloc(sizeof(PartClauseInfo));
		(*pc)->keyno = partkeyidx;

		/* For <> operator clauses, pass on the negator. */
		(*pc)->op_is_ne = false;
		(*pc)->op_strategy = InvalidStrategy;

		if (is_opne_listp)
		{
			Assert(OidIsValid(negator));
			(*pc)->opno = negator;
			(*pc)->op_is_ne = true;
			/*
			 * We already know the strategy in this case, so may as well set
			 * it rather than having to look it up later.
			 */
			(*pc)->op_strategy = BTEqualStrategyNumber;
		}
		/* And if commuted before matching, pass on the commutator */
		else if (OidIsValid(commutator))
			(*pc)->opno = commutator;
		else
			(*pc)->opno = opclause->opno;

		(*pc)->expr = expr;
		(*pc)->cmpfn = cmpfn;

		return PARTCLAUSE_MATCH_CLAUSE;
	}
	else if (IsA(clause, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;
		Oid			saop_op = saop->opno;
		Oid			saop_coll = saop->inputcollid;
		Expr	   *leftop = (Expr *) linitial(saop->args),
				   *rightop = (Expr *) lsecond(saop->args);
		List	   *elem_exprs,
				   *elem_clauses;
		ListCell   *lc1;

		if (IsA(leftop, RelabelType))
			leftop = ((RelabelType *) leftop)->arg;

		/* Check it matches this partition key */
		if (!equal(leftop, partkey) ||
			!PartCollMatchesExprColl(partcoll, saop->inputcollid))
			return PARTCLAUSE_NOMATCH;

		/*
		 * Matched with this key.  Check various properties of the clause to
		 * see if it can sanely be used for partition pruning.
		 */

		/*
		 * Only allow strict operators.  This will guarantee nulls are
		 * filtered.
		 */
		if (!op_strict(saop->opno))
			return PARTCLAUSE_UNSUPPORTED;

		/* Useless if the array has any volatile functions. */
		if (contain_volatile_functions((Node *) rightop))
			return PARTCLAUSE_UNSUPPORTED;

		/*
		 * In case of NOT IN (..), we get a '<>', which we handle if list
		 * partitioning is in use and we're able to confirm that it's negator
		 * is a btree equality operator belonging to the partitioning operator
		 * family.
		 */
		if (!op_in_opfamily(saop_op, partopfamily))
		{
			Oid			negator;

			if (part_scheme->strategy != PARTITION_STRATEGY_LIST)
				return PARTCLAUSE_UNSUPPORTED;

			negator = get_negator(saop_op);
			if (OidIsValid(negator) && op_in_opfamily(negator, partopfamily))
			{
				int			strategy;
				Oid			lefttype,
							righttype;

				get_op_opfamily_properties(negator, partopfamily,
										   false, &strategy,
										   &lefttype, &righttype);
				if (strategy != BTEqualStrategyNumber)
					return PARTCLAUSE_UNSUPPORTED;
			}
		}

		/*
		 * First generate a list of Const nodes, one for each array element.
		 */
		elem_exprs = NIL;
		if (IsA(rightop, Const))
		{
			Const	   *arr = (Const *) lsecond(saop->args);
			ArrayType  *arrval = DatumGetArrayTypeP(arr->constvalue);
			int16		elemlen;
			bool		elembyval;
			char		elemalign;
			Datum	   *elem_values;
			bool	   *elem_nulls;
			int			num_elems,
						i;

			get_typlenbyvalalign(ARR_ELEMTYPE(arrval),
								 &elemlen, &elembyval, &elemalign);
			deconstruct_array(arrval,
							  ARR_ELEMTYPE(arrval),
							  elemlen, elembyval, elemalign,
							  &elem_values, &elem_nulls,
							  &num_elems);
			for (i = 0; i < num_elems; i++)
			{
				/* Only consider non-null values. */
				if (!elem_nulls[i])
				{
					Const	   *elem_expr = makeConst(ARR_ELEMTYPE(arrval),
													  -1, arr->constcollid,
													  elemlen,
													  elem_values[i],
													  false, elembyval);

					elem_exprs = lappend(elem_exprs, elem_expr);
				}
			}
		}
		else
		{
			ArrayExpr  *arrexpr = castNode(ArrayExpr, rightop);

			/*
			 * For a nested ArrayExpr, we don't know how to get the actual
			 * scalar values out into a flat list, so we give up doing
			 * anything with this ScalarArrayOpExpr.
			 */
			if (arrexpr->multidims)
				return PARTCLAUSE_UNSUPPORTED;

			elem_exprs = arrexpr->elements;
		}

		/*
		 * Now generate a list of clauses, one for each array element, of the
		 * form saop_leftop saop_op elem_expr
		 */
		elem_clauses = NIL;
		foreach(lc1, elem_exprs)
		{
			Expr	   *rightop = (Expr *) lfirst(lc1),
					   *elem_clause;

			elem_clause = (Expr *) make_opclause(saop_op, BOOLOID,
												 false,
												 leftop, rightop,
												 InvalidOid,
												 saop_coll);
			elem_clauses = lappend(elem_clauses, elem_clause);
		}

		/*
		 * Build a combine step as if for an OR clause or add the clauses to
		 * the end of the list that's being processed currently.
		 */
		if (saop->useOr && list_length(elem_clauses) > 1)
		{
			Expr	   *orexpr;
			bool		constfalse;

			orexpr = makeBoolExpr(OR_EXPR, elem_clauses, -1);
			*clause_steps =
				generate_partition_pruning_steps_internal(rel, context,
														  list_make1(orexpr),
														  &constfalse);
			if (constfalse)
				return PARTCLAUSE_MATCH_CONTRADICT;
			Assert(list_length(*clause_steps) == 1);
			return PARTCLAUSE_MATCH_STEPS;
		}
		else
		{
			bool		constfalse;

			*clause_steps =
				generate_partition_pruning_steps_internal(rel, context,
														  elem_clauses,
														  &constfalse);
			if (constfalse)
				return PARTCLAUSE_MATCH_CONTRADICT;
			Assert(list_length(*clause_steps) >= 1);
			return PARTCLAUSE_MATCH_STEPS;
		}
	}
	else if (IsA(clause, NullTest))
	{
		NullTest   *nulltest = (NullTest *) clause;
		Expr	   *arg = nulltest->arg;

		if (IsA(arg, RelabelType))
			arg = ((RelabelType *) arg)->arg;

		/* Does arg match with this partition key column? */
		if (!equal(arg, partkey))
			return PARTCLAUSE_NOMATCH;

		if (nulltest->nulltesttype == IS_NULL)
			*key_is_null = true;
		else
			*key_is_not_null = true;

		return PARTCLAUSE_MATCH_NULLNESS;
	}

	return PARTCLAUSE_UNSUPPORTED;
}

/*
 * match_boolean_partition_clause
 *
 * Sets *rightop to a Const containing true or false value and returns true if
 * we're able to match the clause to the partition key as specially-shaped
 * Boolean clause.  Returns false otherwise with *rightop set to NULL.
 */
static bool
match_boolean_partition_clause(Oid partopfamily, Expr *clause, Expr *partkey,
							   Expr **rightop)
{
	Expr	   *leftop;

	*rightop = NULL;

	if (!IsBooleanOpfamily(partopfamily))
		return false;

	if (IsA(clause, BooleanTest))
	{
		BooleanTest *btest = (BooleanTest *) clause;

		/* Only IS [NOT] TRUE/FALSE are any good to us */
		if (btest->booltesttype == IS_UNKNOWN ||
			btest->booltesttype == IS_NOT_UNKNOWN)
			return false;

		leftop = btest->arg;
		if (IsA(leftop, RelabelType))
			leftop = ((RelabelType *) leftop)->arg;

		if (equal(leftop, partkey))
			*rightop = (btest->booltesttype == IS_TRUE ||
						btest->booltesttype == IS_NOT_FALSE)
				? (Expr *) makeBoolConst(true, false)
				: (Expr *) makeBoolConst(false, false);

		if (*rightop)
			return true;
	}
	else
	{
		leftop = not_clause((Node *) clause)
			? get_notclausearg(clause)
			: clause;

		if (IsA(leftop, RelabelType))
			leftop = ((RelabelType *) leftop)->arg;

		/* Clause does not match this partition key. */
		if (equal(leftop, partkey))
			*rightop = not_clause((Node *) clause)
				? (Expr *) makeBoolConst(false, false)
				: (Expr *) makeBoolConst(true, false);
		else if (equal(negate_clause((Node *) leftop), partkey))
			*rightop = (Expr *) makeBoolConst(false, false);

		if (*rightop)
			return true;
	}

	return false;
}

/*
 * generate_pruning_steps_from_opexprs
 *
 * 'keyclauses' contains one list of clauses per partition key.  We check here
 * if we have found clauses for a valid subset of the partition key. In some
 * cases, (depending on the type of partitioning being used) if we didn't
 * find clauses for a given key, we discard clauses that may have been
 * found for any subsequent keys; see specific notes below.
 */
static PartitionPruneStep *
generate_pruning_steps_from_opexprs(PartitionScheme part_scheme,
									GeneratePruningStepsContext *context,
									List **keyclauses,
									Bitmapset *nullkeys)
{
	ListCell   *lc;
	List	   *opsteps = NIL;
	List	   *btree_clauses[BTMaxStrategyNumber],
			   *hash_clauses[HTMaxStrategyNumber];
	bool		need_next_less,
				need_next_eq,
				need_next_greater;
	int			i;

	memset(btree_clauses, 0, sizeof(btree_clauses));
	memset(hash_clauses, 0, sizeof(hash_clauses));
	for (i = 0; i < part_scheme->partnatts; i++)
	{
		List   *clauselist = keyclauses[i];
		bool	consider_next_key = true;

		/*
		 * To be useful for pruning, we must have clauses for a prefix of
		 * partition keys in the case of range partitioning.  So, ignore
		 * clauses for keys after this one.
		 */
		if (part_scheme->strategy == PARTITION_STRATEGY_RANGE &&
			clauselist == NIL)
			break;

		/*
		 * For hash partitioning, if a column doesn't have the necessary
		 * equality clause, there should be an IS NULL clause, otherwise
		 * pruning is not possible.
		 */
		if (part_scheme->strategy == PARTITION_STRATEGY_HASH &&
			clauselist == NIL && !bms_is_member(i, nullkeys))
			return NULL;

		need_next_eq = need_next_less = need_next_greater = true;
		foreach(lc, clauselist)
		{
			PartClauseInfo *pc = (PartClauseInfo *) lfirst(lc);
			Oid			lefttype,
						righttype;

			/* Look up the operator's btree/hash strategy number. */
			if (pc->op_strategy == InvalidStrategy)
				get_op_opfamily_properties(pc->opno,
										   part_scheme->partopfamily[i],
										   false,
										   &pc->op_strategy,
										   &lefttype,
										   &righttype);

			switch (part_scheme->strategy)
			{
				case PARTITION_STRATEGY_LIST:
				case PARTITION_STRATEGY_RANGE:
					{
						PartClauseInfo *last = NULL;
						bool		inclusive = false;

						/*
						 * Add this clause to the list of clauses to be used
						 * for pruning if this is the first such key for this
						 * operator strategy or if it is consecutively next to
						 * the last column for which a clause with this
						 * operator strategy was matched.
						 */
						if (btree_clauses[pc->op_strategy - 1] != NIL)
							last = llast(btree_clauses[pc->op_strategy - 1]);

						if (last == NULL ||
							i == last->keyno || i == last->keyno + 1)
							btree_clauses[pc->op_strategy - 1] =
								lappend(btree_clauses[pc->op_strategy - 1], pc);

						/*
						 * We may not need the next clause if they're of
						 * certain strategy.
						 */
						switch (pc->op_strategy)
						{
							case BTLessEqualStrategyNumber:
								inclusive = true;
								/* fall through */
							case BTLessStrategyNumber:
								if (!inclusive)
									need_next_eq = need_next_less = false;
								break;
							case BTEqualStrategyNumber:
								/* always accept clauses for the next key. */
								break;
							case BTGreaterEqualStrategyNumber:
								inclusive = true;
								/* fall through */
							case BTGreaterStrategyNumber:
								if (!inclusive)
									need_next_eq = need_next_greater = false;
								break;
						}

						/* We may want to change our mind. */
						if (consider_next_key)
							consider_next_key = (need_next_eq ||
												 need_next_less ||
												 need_next_greater);
						break;
					}

				case PARTITION_STRATEGY_HASH:
					if (pc->op_strategy != HTEqualStrategyNumber)
						elog(ERROR, "invalid clause for hash partitioning");
					hash_clauses[pc->op_strategy - 1] =
						lappend(hash_clauses[pc->op_strategy - 1], pc);
					break;

				default:
					elog(ERROR, "invalid partition strategy: %c",
						 part_scheme->strategy);
					break;
			}
		}

		/*
		 * If we've decided that clauses for subsequent partition keys
		 * wouldn't be useful for pruning, don't search any further.
		 */
		if (!consider_next_key)
			break;
	}

	/*
	 * Now, we have divided clauses according to their operator strategies.
	 * Check for each strategy if we can generate pruning step(s) by
	 * collecting a list of expressions whose values will constitute a vector
	 * that can be used as a look-up key by a partition bound searching
	 * function.
	 */
	switch (part_scheme->strategy)
	{
		case PARTITION_STRATEGY_LIST:
		case PARTITION_STRATEGY_RANGE:
			{
				List	   *eq_clauses = btree_clauses[BTEqualStrategyNumber - 1];
				List	   *le_clauses = btree_clauses[BTLessEqualStrategyNumber - 1];
				List	   *ge_clauses = btree_clauses[BTGreaterEqualStrategyNumber - 1];

				/*
				 * For each clause under consideration for a given strategy,
				 * we collect expressions from clauses for earlier keys, whose
				 * operator strategy is inclusive, into a list called 'prefix'.
				 * By appending the clause's own expression to the 'prefix',
				 * we'll generate one step using the so generated vector and
				 * assign the current strategy to it.  Actually, 'prefix' might
				 * contain multiple clauses for the same key, in which case,
				 * we must generate steps for various combinations of
				 * expressions of different keys, which get_steps_using_prefix
				 * takes care of for us.
				 */
				for (i = 0; i < BTMaxStrategyNumber; i++)
				{
					PartClauseInfo *pc;
					List	   *pc_steps;

					foreach(lc, btree_clauses[i])
					{
						ListCell   *lc1;
						List	   *prefix = NIL;

						/* Clause under consideration. */
						pc = lfirst(lc);

						/*
						 * Expressions from = clauses can always be in the
						 * prefix, provided they're from an earlier key.
						 */
						foreach(lc1, eq_clauses)
						{
							PartClauseInfo *eqpc = lfirst(lc1);

							if (eqpc->keyno == pc->keyno)
								break;
							if (eqpc->keyno < pc->keyno)
								prefix = lappend(prefix, eqpc);
						}

						/*
						 * If we're generating steps for </<= strategy, we can
						 * add other <= clauses to the prefix, provided they're
						 * from an earlier key.
						 */
						if (i == BTLessStrategyNumber - 1 ||
							i == BTLessEqualStrategyNumber - 1)
						{
							foreach(lc1, le_clauses)
							{
								PartClauseInfo *lepc = lfirst(lc1);

								if (lepc->keyno == pc->keyno)
									break;
								if (lepc->keyno < pc->keyno)
									prefix = lappend(prefix, lepc);
							}
						}

						/*
						 * If we're generating steps for >/>= strategy, we can
						 * add other >= clauses to the prefix, provided they're
						 * from an earlier key.
						 */
						if (i == BTGreaterStrategyNumber - 1 ||
							i == BTGreaterEqualStrategyNumber - 1)
						{
							foreach(lc1, ge_clauses)
							{
								PartClauseInfo *gepc = lfirst(lc1);

								if (gepc->keyno == pc->keyno)
									break;
								if (gepc->keyno < pc->keyno)
									prefix = lappend(prefix, gepc);
							}
						}

						/*
						 * As mentioned above, if 'prefix' contains multiple
						 * expressions for the same key, the following will
						 * generate multiple steps, one for each combination
						 * of the expressions for different keys.
						 *
						 * Note that we pass NULL for step_nullkeys, because
						 * we don't search list/range partition bounds where
						 * some keys are NULL.
						 */
						Assert(pc->op_strategy == i + 1);
						pc_steps = get_steps_using_prefix(context, i + 1,
														  pc->op_is_ne,
														  pc->expr,
														  pc->cmpfn,
														  pc->keyno,
														  NULL,
														  prefix);
						opsteps = list_concat(opsteps, list_copy(pc_steps));
					}
				}
				break;
			}

		case PARTITION_STRATEGY_HASH:
			{
				List *eq_clauses = hash_clauses[HTEqualStrategyNumber - 1];

				/* For hash partitioning, we have just the = strategy. */
				if (eq_clauses != NIL)
				{
					PartClauseInfo *pc;
					List   *pc_steps;
					List   *prefix = NIL;
					int		last_keyno;
					ListCell *lc1;

					/*
					 * Locate the clause for the greatest column.  This may
					 * not belong to the last partition key, but it is the
					 * clause belonging to the last partition key we found a
					 * clause for above.
					 */
					pc = llast(eq_clauses);

					/*
					 * There might be multiple clauses which matched to that
					 * partition key; find the first such clause.  While at it,
					 * add all the clauses before that one to 'prefix'.
					 */
					last_keyno = pc->keyno;
					foreach(lc, eq_clauses)
					{
						pc = lfirst(lc);
						if (pc->keyno == last_keyno)
							break;
						prefix = lappend(prefix, pc);
					}

					/*
					 * For each clause for the "last" column, after appending
					 * the clause's own expression to the 'prefix', we'll
					 * generate one step using the so generated vector and
					 * and assign = as its strategy.  Actually, 'prefix' might
					 * contain multiple clauses for the same key, in which
					 * case, we must generate steps for various combinations
					 * of expressions of different keys, which
					 * get_steps_using_prefix will take care of for us.
					 */
					for_each_cell(lc1, lc)
					{
						pc = lfirst(lc1);

						/*
						 * Note that we pass nullkeys for step_nullkeys,
						 * because we need to tell hash partition bound search
						 * function which of the keys we found IS NULL clauses
						 * for.
						 */
						Assert(pc->op_strategy == HTEqualStrategyNumber);
						pc_steps =
								get_steps_using_prefix(context,
													   HTEqualStrategyNumber,
													   false,
													   pc->expr,
													   pc->cmpfn,
													   pc->keyno,
													   nullkeys,
													   prefix);
						opsteps = list_concat(opsteps, list_copy(pc_steps));
					}
				}
				break;
			}

		default:
			elog(ERROR, "invalid partition strategy: %c",
				 part_scheme->strategy);
			break;
	}

	/* Finally, add a combine step to mutualy AND opsteps, if needed. */
	if (list_length(opsteps) > 1)
	{
		List *opstep_ids = NIL;

		foreach(lc, opsteps)
		{
			PartitionPruneStep *step = lfirst(lc);

			opstep_ids = lappend_int(opstep_ids, step->step_id);
		}

		if (opstep_ids != NIL)
			return generate_pruning_step_combine(context, opstep_ids,
												 COMBINE_INTERSECT);
		return NULL;
	}
	else if (opsteps != NIL)
		return linitial(opsteps);

	return NULL;
}

/*
 * get_steps_using_prefix
 *		Generate list of PartitionPruneStepOp steps each consisting of given
 *		opstrategy
 *
 * To generate steps, step_lastexpr and step_lastcmpfn are appended to
 * expressions and cmpfns, respectively, extracted from the clauses in
 * 'prefix'.  Actually, since 'prefix' may contain multiple clauses for the
 * same partition key column, we must generate steps for various combinations
 * of the clauses of different keys.
 */
static List *
get_steps_using_prefix(GeneratePruningStepsContext *context,
					   int step_opstrategy,
					   bool step_op_is_ne,
					   Expr *step_lastexpr,
					   Oid step_lastcmpfn,
					   int step_lastkeyno,
					   Bitmapset *step_nullkeys,
					   List *prefix)
{
	/* Quick exit if there are no values to prefix with. */
	if (list_length(prefix) == 0)
	{
		PartitionPruneStep *step;

		step = generate_pruning_step_op(context,
										step_opstrategy, step_op_is_ne,
										list_make1(step_lastexpr),
										list_make1_oid(step_lastcmpfn),
										step_nullkeys);
		return list_make1(step);
	}

	/* Recurse to generate steps for various combinations. */
	return get_steps_using_prefix_recurse(context,
										  step_opstrategy,
										  step_op_is_ne,
										  step_lastexpr,
										  step_lastcmpfn,
										  step_lastkeyno,
										  step_nullkeys,
										  list_head(prefix),
										  NIL, NIL);
}

/*
 * get_steps_using_prefix_recurse
 *		Recursively generate combinations of clauses for different partition
 *		keys and start generating steps upon reaching clauses for the greatest
 *		column that is less than the one for which we're currently generating
 *		steps (that is, step_lastkeyno)
 *
 * 'start' is where we should start iterating for the current invocation.
 * 'step_exprs' and 'step_cmpfns' each contains the expressions and cmpfns
 * we've generated so far from the clauses for the previous part keys.
 */
static List *
get_steps_using_prefix_recurse(GeneratePruningStepsContext *context,
							   int step_opstrategy,
							   bool step_op_is_ne,
							   Expr *step_lastexpr,
							   Oid step_lastcmpfn,
							   int step_lastkeyno,
							   Bitmapset *step_nullkeys,
							   ListCell *start,
							   List *step_exprs,
							   List *step_cmpfns)
{
	List	   *result = NIL;
	ListCell   *lc;
	int			cur_keyno;

	/* Actually, recursion would be limited by PARTITION_MAX_KEYS. */
	check_stack_depth();

	/* Check if we need to recurse. */
	Assert(start != NULL);
	cur_keyno = ((PartClauseInfo *) lfirst(start))->keyno;
	if (cur_keyno < step_lastkeyno - 1)
	{
		PartClauseInfo *pc;
		ListCell   *next_start;

		/*
		 * For each clause with cur_keyno, adds its expr and cmpfn to
		 * step_exprs and step_cmpfns, respectively, and recurse after setting
		 * next_start to the ListCell of the first clause for the next
		 * partition key.
		 */
		for_each_cell(lc, start)
		{
			pc = lfirst(lc);

			if (pc->keyno > cur_keyno)
				break;
		}
		next_start = lc;

		for_each_cell(lc, start)
		{
			pc = lfirst(lc);
			if (pc->keyno == cur_keyno)
			{
				/* clean up before starting a new recursion cycle. */
				if (cur_keyno == 0)
				{
					list_free(step_exprs);
					list_free(step_cmpfns);
					step_exprs = list_make1(pc->expr);
					step_cmpfns = list_make1_oid(pc->cmpfn);
				}
				else
				{
					step_exprs = lappend(step_exprs, pc->expr);
					step_cmpfns = lappend_oid(step_cmpfns, pc->cmpfn);
				}
			}
			else
			{
				Assert(pc->keyno > cur_keyno);
				break;
			}

			result =
				list_concat(result,
							get_steps_using_prefix_recurse(context,
														   step_opstrategy,
														   step_op_is_ne,
														   step_lastexpr,
														   step_lastcmpfn,
														   step_lastkeyno,
														   step_nullkeys,
														   next_start,
														   step_exprs,
														   step_cmpfns));
		}
	}
	else
	{
		/*
		 * End the current recursion cycle and start generating steps, one
		 * for each clause with cur_keyno, which is all clauses from here
		 * onward till the end of the list.
		 */
		Assert(list_length(step_exprs) == cur_keyno);
		for_each_cell(lc, start)
		{
			PartClauseInfo *pc = lfirst(lc);
			List   *step_exprs1,
				   *step_cmpfns1;

			Assert(pc->keyno == cur_keyno);

			/* Leave the original step_exprs unmodified. */
			step_exprs1 = list_copy(step_exprs);
			step_exprs1 = lappend(step_exprs1, pc->expr);
			step_exprs1 = lappend(step_exprs1, step_lastexpr);

			/* Leave the original step_cmpfns unmodified. */
			step_cmpfns1 = list_copy(step_cmpfns);
			step_cmpfns1 = lappend_oid(step_cmpfns1, pc->cmpfn);
			step_cmpfns1 = lappend_oid(step_cmpfns1, step_lastcmpfn);

			result =
				lappend(result,
						generate_pruning_step_op(context,
												 step_opstrategy, step_op_is_ne,
												 step_exprs1, step_cmpfns1,
												 step_nullkeys));
		}
	}

	return result;
}

/*
 * The following functions generate pruning steps of various types.  Each step
 * that's created is added to a context's 'steps' List and receives unique
 * step identifier.
 */
static PartitionPruneStep *
generate_pruning_step_op(GeneratePruningStepsContext *context,
						 int opstrategy, bool op_is_ne,
						 List *exprs, List *cmpfns,
						 Bitmapset *nullkeys)
{
	PartitionPruneStepOp *opstep = makeNode(PartitionPruneStepOp);

	opstep->step.step_id = context->next_step_id++;

	/*
	 * For clauses that contain an <> operator, set opstrategy to
	 * InvalidStrategy to signal get_matching_list_bounds to do the
	 * right thing.
	 */
	if (op_is_ne)
	{
		Assert(opstrategy == BTEqualStrategyNumber);
		opstep->opstrategy = InvalidStrategy;
	}
	else
		opstep->opstrategy = opstrategy;
	Assert(list_length(exprs) == list_length(cmpfns));
	opstep->exprs = exprs;
	opstep->cmpfns = cmpfns;
	opstep->nullkeys = nullkeys;

	context->steps = lappend(context->steps, opstep);

	return (PartitionPruneStep *) opstep;
}

static PartitionPruneStep *
generate_pruning_step_combine(GeneratePruningStepsContext *context,
							List *source_stepids,
							PartitionPruneCombineOp combineOp)
{
	PartitionPruneStepCombine *cstep = makeNode(PartitionPruneStepCombine);

	cstep->step.step_id = context->next_step_id++;
	cstep->combineOp = combineOp;
	cstep->source_stepids = source_stepids;

	context->steps = lappend(context->steps, cstep);

	return (PartitionPruneStep *) cstep;
}
