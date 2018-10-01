/*
 * src/tutorial/subscripting.c
 *
 ******************************************************************************
  This file contains routines that can be bound to a Postgres backend and
  called by the backend in the process of processing queries.  The calling
  format for these routines is dictated by Postgres architecture.
******************************************************************************/

#include "postgres.h"

#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/execExpr.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"

PG_MODULE_MAGIC;

typedef struct Custom
{
	int	first;
	int	second;
}	Custom;

SubscriptingRef * custom_subscript_prepare(bool isAssignment, SubscriptingRef *sbsref);
SubscriptingRef * custom_subscript_validate(bool isAssignment, SubscriptingRef *sbsref,
											ParseState *pstate);
Datum custom_subscript_fetch(Datum containerSource, SubscriptingRefState *sbstate);
Datum custom_subscript_assign(Datum containerSource, SubscriptingRefState *sbstate);

PG_FUNCTION_INFO_V1(custom_in);
PG_FUNCTION_INFO_V1(custom_out);
PG_FUNCTION_INFO_V1(custom_subscripting_handler);

/*****************************************************************************
 * Input/Output functions
 *****************************************************************************/

Datum
custom_in(PG_FUNCTION_ARGS)
{
	char	*str = PG_GETARG_CSTRING(0);
	int		firstValue,
			secondValue;
	Custom	*result;

	if (sscanf(str, " ( %d , %d )", &firstValue, &secondValue) != 2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for complex: \"%s\"",
						str)));


	result = (Custom *) palloc(sizeof(Custom));
	result->first = firstValue;
	result->second = secondValue;
	PG_RETURN_POINTER(result);
}

Datum
custom_out(PG_FUNCTION_ARGS)
{
	Custom	*custom = (Custom *) PG_GETARG_POINTER(0);
	char	*result;

	result = psprintf("(%d, %d)", custom->first, custom->second);
	PG_RETURN_CSTRING(result);
}

/*****************************************************************************
 * Custom subscripting logic functions
 *****************************************************************************/

Datum
custom_subscripting_handler(PG_FUNCTION_ARGS)
{
	SubscriptRoutines *sbsroutines = (SubscriptRoutines *)
									 palloc(sizeof(SubscriptRoutines));

	sbsroutines->prepare = custom_subscript_prepare;
	sbsroutines->validate = custom_subscript_validate;
	sbsroutines->fetch = custom_subscript_fetch;
	sbsroutines->assign = custom_subscript_assign;

	PG_RETURN_POINTER(sbsroutines);
}

SubscriptingRef *
custom_subscript_prepare(bool isAssignment, SubscriptingRef *sbsref)
{
	sbsref->refelemtype = INT4OID;
	sbsref->refassgntype = INT4OID;
	return sbsref;
}

SubscriptingRef *
custom_subscript_validate(bool isAssignment, SubscriptingRef *sbsref,
						  ParseState *pstate)
{
	List			   *upperIndexpr = NIL;
	ListCell		   *l;

	if (sbsref->reflowerindexpr != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("custom subscript does not support slices"),
				 parser_errposition(pstate, exprLocation(
						 ((Node *)lfirst(sbsref->reflowerindexpr->head))))));

	foreach(l, sbsref->refupperindexpr)
	{
		Node *subexpr = (Node *) lfirst(l);

		if (subexpr == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("custom subscript does not support slices"),
					 parser_errposition(pstate, exprLocation(
						((Node *) lfirst(sbsref->refupperindexpr->head))))));

		subexpr = coerce_to_target_type(pstate,
										subexpr, exprType(subexpr),
										INT4OID, -1,
										COERCION_ASSIGNMENT,
										COERCE_IMPLICIT_CAST,
										-1);
		if (subexpr == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("custom subscript must have integer type"),
					 parser_errposition(pstate, exprLocation(subexpr))));

		upperIndexpr = lappend(upperIndexpr, subexpr);

		if (isAssignment)
		{
			Node *assignExpr = (Node *) sbsref->refassgnexpr;
			Node *new_from;

			new_from = coerce_to_target_type(pstate,
					assignExpr, exprType(assignExpr),
					INT4OID, -1,
					COERCION_ASSIGNMENT,
					COERCE_IMPLICIT_CAST,
					-1);
			if (new_from == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("custom assignment requires int type"),
						 errhint("You will need to rewrite or cast the expression."),
						 parser_errposition(pstate, exprLocation(assignExpr))));
			sbsref->refassgnexpr = (Expr *)new_from;
		}
	}

	sbsref->refupperindexpr = upperIndexpr;

	return sbsref;
}

Datum
custom_subscript_fetch(Datum containerSource, SubscriptingRefState *sbstate)
{
	Custom	*container= (Custom *) containerSource;
	int		 index;

	if (sbstate->numupper != 1)
		ereport(ERROR, (errmsg("custom does not support nested subscripting")));

	index = DatumGetInt32(sbstate->upper[0]);

	if (index == 1)
		return (Datum) container->first;
	else
		return (Datum) container->second;
}

Datum
custom_subscript_assign(Datum containerSource, SubscriptingRefState *sbstate)
{
	int	index;
	Custom *container = (Custom *) containerSource;

	if (sbstate->resnull)
		return containerSource;

	if (sbstate->numupper != 1)
		ereport(ERROR, (errmsg("custom does not support nested subscripting")));

	index = DatumGetInt32(sbstate->upper[0]);

	if (index == 1)
		container->first = DatumGetInt32(sbstate->replacevalue);
	else
		container->second = DatumGetInt32(sbstate->replacevalue);

	return (Datum) container;
}
