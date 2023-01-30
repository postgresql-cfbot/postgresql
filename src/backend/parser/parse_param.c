/*-------------------------------------------------------------------------
 *
 * parse_param.c
 *	  handle parameters in parser
 *
 * This code covers two cases that are used within the core backend:
 *		* a fixed list of parameters with known types
 *		* an expandable list of parameters whose types can optionally
 *		  be determined from context
 * In both cases, only explicit $n references (ParamRef nodes) are supported.
 *
 * Note that other approaches to parameters are possible using the parser
 * hooks defined in ParseState.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_param.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>

#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_param.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


typedef struct FixedParamState
{
	const Oid  *paramTypes;		/* array of parameter type OIDs */
	int			numParams;		/* number of array entries */
} FixedParamState;

/*
 * In the varparams case, the caller-supplied OID array (if any) can be
 * re-palloc'd larger at need.  A zero array entry means that parameter number
 * hasn't been seen, while UNKNOWNOID means the parameter has been used but
 * its type is not yet known.
 */
typedef struct VarParamState
{
	Oid		  **paramTypes;		/* array of parameter type OIDs */
	int		   *numParams;		/* number of array entries */
} VarParamState;

static Node *fixed_paramref_hook(ParseState *pstate, ParamRef *pref);
static Node *variable_paramref_hook(ParseState *pstate, ParamRef *pref);
static Node *variable_coerce_param_hook(ParseState *pstate, Param *param,
										Oid targetTypeId, int32 targetTypeMod,
										int location);
static bool check_parameter_resolution_walker(Node *node, ParseState *pstate);
static bool query_contains_extern_params_walker(Node *node, void *context);


/*
 * Set up to process a query containing references to fixed parameters.
 */
void
setup_parse_fixed_parameters(ParseState *pstate,
							 const Oid *paramTypes, int numParams)
{
	FixedParamState *parstate = palloc(sizeof(FixedParamState));

	parstate->paramTypes = paramTypes;
	parstate->numParams = numParams;
	pstate->p_ref_hook_state = (void *) parstate;
	pstate->p_paramref_hook = fixed_paramref_hook;
	/* no need to use p_coerce_param_hook */
}

/*
 * Set up to process a query containing references to variable parameters.
 */
void
setup_parse_variable_parameters(ParseState *pstate,
								Oid **paramTypes, int *numParams)
{
	VarParamState *parstate = palloc(sizeof(VarParamState));

	parstate->paramTypes = paramTypes;
	parstate->numParams = numParams;
	pstate->p_ref_hook_state = (void *) parstate;
	pstate->p_paramref_hook = variable_paramref_hook;
	pstate->p_coerce_param_hook = variable_coerce_param_hook;
}

/*
 * Transform a ParamRef using fixed parameter types.
 */
static Node *
fixed_paramref_hook(ParseState *pstate, ParamRef *pref)
{
	FixedParamState *parstate = (FixedParamState *) pstate->p_ref_hook_state;
	int			paramno = pref->number;
	Param	   *param;

	/* Check parameter number is valid */
	if (paramno <= 0 || paramno > parstate->numParams ||
		!OidIsValid(parstate->paramTypes[paramno - 1]))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_PARAMETER),
				 errmsg("there is no parameter $%d", paramno),
				 parser_errposition(pstate, pref->location)));

	param = makeNode(Param);
	param->paramkind = PARAM_EXTERN;
	param->paramid = paramno;
	param->paramtype = parstate->paramTypes[paramno - 1];
	param->paramtypmod = -1;
	param->paramcollid = get_typcollation(param->paramtype);
	param->location = pref->location;

	return (Node *) param;
}

/*
 * Transform a ParamRef using variable parameter types.
 *
 * The only difference here is we must enlarge the parameter type array
 * as needed.
 */
static Node *
variable_paramref_hook(ParseState *pstate, ParamRef *pref)
{
	VarParamState *parstate = (VarParamState *) pstate->p_ref_hook_state;
	int			paramno = pref->number;
	Oid		   *pptype;
	Param	   *param;

	/* Check parameter number is in range */
	if (paramno <= 0 || paramno > INT_MAX / sizeof(Oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_PARAMETER),
				 errmsg("there is no parameter $%d", paramno),
				 parser_errposition(pstate, pref->location)));
	if (paramno > *parstate->numParams)
	{
		/* Need to enlarge param array */
		if (*parstate->paramTypes)
			*parstate->paramTypes = repalloc0_array(*parstate->paramTypes, Oid,
													*parstate->numParams, paramno);
		else
			*parstate->paramTypes = palloc0_array(Oid, paramno);
		*parstate->numParams = paramno;
	}

	/* Locate param's slot in array */
	pptype = &(*parstate->paramTypes)[paramno - 1];

	/* If not seen before, initialize to UNKNOWN type */
	if (*pptype == InvalidOid)
		*pptype = UNKNOWNOID;

	/*
	 * If the argument is of type void and it's procedure call, interpret it
	 * as unknown.  This allows the JDBC driver to not have to distinguish
	 * function and procedure calls.  See also another component of this hack
	 * in ParseFuncOrColumn().
	 */
	if (*pptype == VOIDOID && pstate->p_expr_kind == EXPR_KIND_CALL_ARGUMENT)
		*pptype = UNKNOWNOID;

	param = makeNode(Param);
	param->paramkind = PARAM_EXTERN;
	param->paramid = paramno;
	param->paramtype = *pptype;
	param->paramtypmod = -1;
	param->paramcollid = get_typcollation(param->paramtype);
	param->location = pref->location;

	return (Node *) param;
}

/*
 * Coerce a Param to a query-requested datatype, in the varparams case.
 */
static Node *
variable_coerce_param_hook(ParseState *pstate, Param *param,
						   Oid targetTypeId, int32 targetTypeMod,
						   int location)
{
	if (param->paramkind == PARAM_EXTERN && param->paramtype == UNKNOWNOID)
	{
		/*
		 * Input is a Param of previously undetermined type, and we want to
		 * update our knowledge of the Param's type.
		 */
		VarParamState *parstate = (VarParamState *) pstate->p_ref_hook_state;
		Oid		   *paramTypes = *parstate->paramTypes;
		int			paramno = param->paramid;

		if (paramno <= 0 ||		/* shouldn't happen, but... */
			paramno > *parstate->numParams)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_PARAMETER),
					 errmsg("there is no parameter $%d", paramno),
					 parser_errposition(pstate, param->location)));

		if (paramTypes[paramno - 1] == UNKNOWNOID)
		{
			/* We've successfully resolved the type */
			paramTypes[paramno - 1] = targetTypeId;
		}
		else if (paramTypes[paramno - 1] == targetTypeId)
		{
			/* We previously resolved the type, and it matches */
		}
		else
		{
			/* Oops */
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_PARAMETER),
					 errmsg("inconsistent types deduced for parameter $%d",
							paramno),
					 errdetail("%s versus %s",
							   format_type_be(paramTypes[paramno - 1]),
							   format_type_be(targetTypeId)),
					 parser_errposition(pstate, param->location)));
		}

		param->paramtype = targetTypeId;

		/*
		 * Note: it is tempting here to set the Param's paramtypmod to
		 * targetTypeMod, but that is probably unwise because we have no
		 * infrastructure that enforces that the value delivered for a Param
		 * will match any particular typmod.  Leaving it -1 ensures that a
		 * run-time length check/coercion will occur if needed.
		 */
		param->paramtypmod = -1;

		/*
		 * This module always sets a Param's collation to be the default for
		 * its datatype.  If that's not what you want, you should be using the
		 * more general parser substitution hooks.
		 */
		param->paramcollid = get_typcollation(param->paramtype);

		/* Use the leftmost of the param's and coercion's locations */
		if (location >= 0 &&
			(param->location < 0 || location < param->location))
			param->location = location;

		return (Node *) param;
	}

	/* Else signal to proceed with normal coercion */
	return NULL;
}

/*
 * Check for consistent assignment of variable parameters after completion
 * of parsing with parse_variable_parameters.
 *
 * Note: this code intentionally does not check that all parameter positions
 * were used, nor that all got non-UNKNOWN types assigned.  Caller of parser
 * should enforce that if it's important.
 */
void
check_variable_parameters(ParseState *pstate, Query *query)
{
	VarParamState *parstate = (VarParamState *) pstate->p_ref_hook_state;

	/* If numParams is zero then no Params were generated, so no work */
	if (*parstate->numParams > 0)
		(void) query_tree_walker(query,
								 check_parameter_resolution_walker,
								 (void *) pstate, 0);
}

/*
 * Traverse a fully-analyzed tree to verify that parameter symbols
 * match their types.  We need this because some Params might still
 * be UNKNOWN, if there wasn't anything to force their coercion,
 * and yet other instances seen later might have gotten coerced.
 */
static bool
check_parameter_resolution_walker(Node *node, ParseState *pstate)
{
	if (node == NULL)
		return false;
	if (IsA(node, Param))
	{
		Param	   *param = (Param *) node;

		if (param->paramkind == PARAM_EXTERN)
		{
			VarParamState *parstate = (VarParamState *) pstate->p_ref_hook_state;
			int			paramno = param->paramid;

			if (paramno <= 0 || /* shouldn't happen, but... */
				paramno > *parstate->numParams)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_PARAMETER),
						 errmsg("there is no parameter $%d", paramno),
						 parser_errposition(pstate, param->location)));

			if (param->paramtype != (*parstate->paramTypes)[paramno - 1])
				ereport(ERROR,
						(errcode(ERRCODE_AMBIGUOUS_PARAMETER),
						 errmsg("could not determine data type of parameter $%d",
								paramno),
						 parser_errposition(pstate, param->location)));
		}
		return false;
	}
	if (IsA(node, Query))
	{
		/* Recurse into RTE subquery or not-yet-planned sublink subquery */
		return query_tree_walker((Query *) node,
								 check_parameter_resolution_walker,
								 (void *) pstate, 0);
	}
	return expression_tree_walker(node, check_parameter_resolution_walker,
								  (void *) pstate);
}

/*
 * Check to see if a fully-parsed query tree contains any PARAM_EXTERN Params.
 */
bool
query_contains_extern_params(Query *query)
{
	return query_tree_walker(query,
							 query_contains_extern_params_walker,
							 NULL, 0);
}

static bool
query_contains_extern_params_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Param))
	{
		Param	   *param = (Param *) node;

		if (param->paramkind == PARAM_EXTERN)
			return true;
		return false;
	}
	if (IsA(node, Query))
	{
		/* Recurse into RTE subquery or not-yet-planned sublink subquery */
		return query_tree_walker((Query *) node,
								 query_contains_extern_params_walker,
								 context, 0);
	}
	return expression_tree_walker(node, query_contains_extern_params_walker,
								  context);
}

/*
 * Walk a query tree and find out what tables and columns a parameter is
 * associated with.
 *
 * We need to find 1) parameters written directly into a table column, and 2)
 * binary predicates relating a parameter to a table column.
 *
 * We just need to find Var and Param nodes in appropriate places.  We don't
 * need to do harder things like looking through casts, since this is used for
 * column encryption, and encrypted columns can't be usefully cast to
 * anything.
 */

struct find_param_origs_context
{
	const Query *query;
	Oid		   *param_orig_tbls;
	AttrNumber *param_orig_cols;
};

static bool
find_param_origs_walker(Node *node, struct find_param_origs_context *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, OpExpr) || IsA(node, DistinctExpr) || IsA(node, NullIfExpr))
	{
		OpExpr	   *opexpr = (OpExpr *) node;

		if (list_length(opexpr->args) == 2)
		{
			Node	   *lexpr = linitial(opexpr->args);
			Node	   *rexpr = lsecond(opexpr->args);
			Var		   *v = NULL;
			Param	   *p = NULL;

			if (IsA(lexpr, Var) && IsA(rexpr, Param))
			{
				v = castNode(Var, lexpr);
				p = castNode(Param, rexpr);
			}
			else if (IsA(rexpr, Var) && IsA(lexpr, Param))
			{
				v = castNode(Var, rexpr);
				p = castNode(Param, lexpr);
			}

			if (v && p)
			{
				RangeTblEntry *rte;

				rte = rt_fetch(v->varno, context->query->rtable);
				if (rte->rtekind == RTE_RELATION)
				{
					context->param_orig_tbls[p->paramid - 1] = rte->relid;
					context->param_orig_cols[p->paramid - 1] = v->varattno;
				}
			}
		}
		return false;
	}

	/*
	 * TargetEntry in a query with a result relation
	 */
	if (IsA(node, TargetEntry) && context->query->resultRelation > 0)
	{
		TargetEntry *te = (TargetEntry *) node;
		RangeTblEntry *resrte;

		resrte = rt_fetch(context->query->resultRelation, context->query->rtable);
		if (resrte->rtekind == RTE_RELATION)
		{
			/*
			 * Param directly in a target list
			 */
			if (IsA(te->expr, Param))
			{
				Param	   *p = (Param *) te->expr;

				context->param_orig_tbls[p->paramid - 1] = resrte->relid;
				context->param_orig_cols[p->paramid - 1] = te->resno;
			}
			/*
			 * If it's a Var, check whether it corresponds to a VALUES list
			 * with top-level parameters.  This covers multi-row INSERTS.
			 */
			else if (IsA(te->expr, Var))
			{
				Var	   *v = (Var *) te->expr;
				RangeTblEntry *srcrte;

				srcrte = rt_fetch(v->varno, context->query->rtable);
				if (srcrte->rtekind == RTE_VALUES)
				{
					ListCell *lc;

					foreach (lc, srcrte->values_lists)
					{
						List *values_list = lfirst_node(List, lc);
						Node *value = list_nth(values_list, v->varattno - 1);

						if (IsA(value, Param))
						{
							Param	   *p = (Param *) value;

							context->param_orig_tbls[p->paramid - 1] = resrte->relid;
							context->param_orig_cols[p->paramid - 1] = te->resno;
						}
					}
				}
			}
		}
		return false;
	}

	if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node, find_param_origs_walker, context, 0);
	}

	return expression_tree_walker(node, find_param_origs_walker, context);
}

void
find_param_origs(List *query_list, Oid **param_orig_tbls, AttrNumber **param_orig_cols)
{
	struct find_param_origs_context context;
	ListCell   *lc;

	context.param_orig_tbls = *param_orig_tbls;
	context.param_orig_cols = *param_orig_cols;

	foreach (lc, query_list)
	{
		Query	   *query = lfirst_node(Query, lc);

		context.query = query;
		query_tree_walker(query, find_param_origs_walker, &context, 0);
	}
}
