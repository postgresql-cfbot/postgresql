/*-------------------------------------------------------------------------
 *
 * execSRF.c
 *	  Routines implementing the API for set-returning functions
 *
 * This file serves nodeFunctionscan.c and nodeProjectSet.c, providing
 * common code for calling set-returning functions according to the
 * ReturnSetInfo API.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execSRF.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/objectaccess.h"
#include "executor/execdebug.h"
#include "executor/nodeMaterial.h"
#include "executor/nodeFunctionscan.h"
#include "executor/nodeSRFScan.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_coerce.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"


/* static function decls */
static void init_sexpr(Oid foid, Oid input_collation, Expr *node,
					   SetExprState *sexpr, PlanState *parent,
					   MemoryContext sexprCxt, bool allowSRF, bool needDescForSRF);
static void ShutdownSetExpr(Datum arg);
static void ExecEvalFuncArgs(FunctionCallInfo fcinfo,
							 List *argList, ExprContext *econtext);
static void ExecPrepareTuplestoreResult(SetExprState *sexpr,
										ExprContext *econtext,
										Tuplestorestate *resultStore,
										TupleDesc resultDesc);


/*
 * Prepare function call in FROM (ROWS FROM) or targetlist SRF function
 * call for execution for execution.
 *
 * This is used by nodeFunctionscan.c and nodeProjectSet.c.
 */
SetExprState *
ExecInitFunctionResultSet(Expr *expr,
						  ExprContext *econtext, PlanState *parent)
{
	SetExprState *state = makeNode(SetExprState);

	state->funcReturnsSet = false;
	state->expr = expr;
	state->func.fn_oid = InvalidOid;

	if (IsA(expr, FuncExpr))
	{
		/*
		 * For a FunctionScan or ProjectSet, the passed expression tree can be a
		 * FuncExpr, since the grammar only allows a function call at the top
		 * level of a table function reference.
		 */
		FuncExpr   *func = (FuncExpr *) expr;

		state->funcReturnsSet = func->funcretset;
		state->args = ExecInitExprList(func->args, parent);
		init_sexpr(func->funcid, func->inputcollid, expr, state, parent,
				   econtext->ecxt_per_query_memory, func->funcretset, true);
	}
	else if (IsA(expr, OpExpr))
	{
		/*
		 * For ProjectSet, the expression node could be an OpExpr.
		 */
		OpExpr	   *op = (OpExpr *) expr;

		state->funcReturnsSet = op->opretset;
		state->args = ExecInitExprList(op->args, parent);
		init_sexpr(op->opfuncid, op->inputcollid, expr, state, parent,
				   econtext->ecxt_per_query_memory, op->opretset, true);
	}
	else
	{
		/*
		 * However, again for FunctionScan, if the function doesn't return set
		 * then the planner might have replaced the function call via constant-
		 * folding or inlining.  So if we see any other kind of expression node,
		 * execute it via the general ExecEvalExpr() code.  That code path will
		 * not support set-returning functions buried in the expression, though.
		 */
		MemoryContext oldcontext;

		state->elidedFuncState = ExecInitExpr(expr, parent);

		oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);

		/* By performing InitFunctionCallInfoData here, we avoid palloc0() */
		state->fcinfo = palloc(SizeForFunctionCallInfo(list_length(state->args)));

		MemoryContextSwitchTo(oldcontext);

		InitFunctionCallInfoData(*state->fcinfo, NULL, 0, InvalidOid, NULL, NULL);
	}

	return state;
}

/*
 *		ExecMakeFunctionResultSet
 *
 * Evaluate the arguments to a set-returning function and then call the
 * function itself.  The argument expressions may not contain set-returning
 * functions (the planner is supposed to have separated evaluation for those).
 *
 * This should be called in a short-lived (per-tuple) context, argContext
 * needs to live until all rows have been returned (i.e. *isDone set to
 * ExprEndResult or ExprSingleResult).
 *
 * This is used by nodeProjectSet.c and nodeFunctionscan.c.
 */
Datum
ExecMakeFunctionResultSet(SetExprState *fcache,
						  ExprContext *econtext,
						  MemoryContext argContext,
						  bool *isNull,
						  ExprDoneCond *isDone)
{
	List	   *arguments;
	Datum		result;
	FunctionCallInfo fcinfo;
	PgStat_FunctionCallUsage fcusage;
	ReturnSetInfo *rsinfo;
	bool		callit;
	int			i;

restart:

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	/*
	 * If a previous call of the function returned a set result in the form of
	 * a tuplestore, continue reading rows from the tuplestore until it's
	 * empty.
	 */
	if (fcache->funcResultStore)
	{
		TupleTableSlot *slot = fcache->funcResultSlot;
		MemoryContext oldContext;
		bool		foundTup;

		/*
		 * Have to make sure tuple in slot lives long enough, otherwise
		 * clearing the slot could end up trying to free something already
		 * freed.
		 */
		oldContext = MemoryContextSwitchTo(slot->tts_mcxt);
		foundTup = tuplestore_gettupleslot(fcache->funcResultStore, true, false,
										   fcache->funcResultSlot);
		MemoryContextSwitchTo(oldContext);

		if (foundTup)
		{
			*isDone = ExprMultipleResult;
			if (fcache->funcReturnsTuple)
			{
				/* We must return the whole tuple as a Datum. */
				*isNull = false;
				return ExecFetchSlotHeapTupleDatum(fcache->funcResultSlot);
			}
			else
			{
				/* Extract the first column and return it as a scalar. */
				return slot_getattr(fcache->funcResultSlot, 1, isNull);
			}
		}
		/* Exhausted the tuplestore, so clean up */
		tuplestore_end(fcache->funcResultStore);
		fcache->funcResultStore = NULL;
		*isDone = ExprEndResult;
		*isNull = true;
		return (Datum) 0;
	}

	/*
	 * Prepare a resultinfo node for communication.  We always do this even if
	 * not expecting a set result, so that we can pass expectedDesc.  In the
	 * generic-expression case, the expression doesn't actually get to see the
	 * resultinfo, but set it up anyway because we use some of the fields as
	 * our own state variables.
	 */
	fcinfo = fcache->fcinfo;
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	if (rsinfo == NULL)
	{
		MemoryContext oldContext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);

		rsinfo = makeNode (ReturnSetInfo);
		rsinfo->econtext = econtext;
		rsinfo->expectedDesc = fcache->funcResultDesc;
		fcinfo->resultinfo = (Node *) rsinfo;

		MemoryContextSwitchTo(oldContext);
	}

	/*
	 * arguments is a list of expressions to evaluate before passing to the
	 * function manager.  We skip the evaluation if it was already done in the
	 * previous call (ie, we are continuing the evaluation of a set-valued
	 * function).  Otherwise, collect the current argument values into fcinfo.
	 *
	 * The arguments have to live in a context that lives at least until all
	 * rows from this SRF have been returned, otherwise ValuePerCall SRFs
	 * would reference freed memory after the first returned row.
	 */
	arguments = fcache->args;
	if (!fcache->setArgsValid)
	{
		MemoryContext oldContext = MemoryContextSwitchTo(argContext);

		ExecEvalFuncArgs(fcinfo, arguments, econtext);
		MemoryContextSwitchTo(oldContext);

		/* Reset the rsinfo structure */
		rsinfo->allowedModes = (int) (SFRM_ValuePerCall | SFRM_Materialize);
		/* note we do not set SFRM_Materialize_Random or _Preferred */
		rsinfo->returnMode = SFRM_ValuePerCall;
		/* isDone is filled below */
		rsinfo->setResult = NULL;
		rsinfo->setDesc = NULL;
	}
	else
	{
		/* Reset flag (we may set it again below) */
		fcache->setArgsValid = false;
	}

	/*
	 * Now call the function, passing the evaluated parameter values.
	 */

	/*
	 * If function is strict, and there are any NULL arguments, skip calling
	 * the function.
	 */
	callit = true;
	if (fcache->func.fn_strict)
	{
		for (i = 0; i < fcinfo->nargs; i++)
		{
			if (fcinfo->args[i].isnull)
			{
				callit = false;
				break;
			}
		}
	}

	if (callit)
	{
		if (!fcache->elidedFuncState)
		{
			pgstat_init_function_usage(fcinfo, &fcusage);

			fcinfo->isnull = false;
			rsinfo->isDone = ExprSingleResult;
			result = FunctionCallInvoke(fcinfo);
			*isNull = fcinfo->isnull;
			*isDone = rsinfo->isDone;

			pgstat_end_function_usage(&fcusage,
									  rsinfo->isDone != ExprMultipleResult);
		}
		else
		{
			result =
				ExecEvalExpr(fcache->elidedFuncState, econtext, isNull);
			*isDone = ExprSingleResult;
		}
	}
	else
	{
		/* for a strict SRF, result for NULL is an empty set */
		result = (Datum) 0;
		*isNull = true;
		*isDone = ExprEndResult;
	}

	/* Which protocol does function want to use? */
	if (rsinfo->returnMode == SFRM_ValuePerCall)
	{
		if (*isDone != ExprEndResult)
		{
			/*
			 * Obtain a suitable tupdesc, when we first encounter a non-NULL result.
			 */
			if (rsinfo->setDesc == NULL)
			{
				if (fcache->funcReturnsTuple && !*isNull)
				{
					HeapTupleHeader td = DatumGetHeapTupleHeader(result);

					/*
					 * This is the first non-NULL result from the
					 * function.  Use the type info embedded in the
					 * rowtype Datum to look up the needed tupdesc.  Make
					 * a copy for the query.
					 */
					MemoryContext oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
					rsinfo->setDesc = lookup_rowtype_tupdesc_copy(HeapTupleHeaderGetTypeId(td), HeapTupleHeaderGetTypMod(td));
					MemoryContextSwitchTo(oldcontext);
				}
			}

			/*
			 * Save the current argument values to re-use on the next call.
			 */
			if (*isDone == ExprMultipleResult)
			{
				fcache->setArgsValid = true;
				/* Register cleanup callback if we didn't already */
				if (!fcache->shutdown_reg)
				{
					RegisterExprContextCallback(econtext,
												ShutdownSetExpr,
												PointerGetDatum(fcache));
					fcache->shutdown_reg = true;
				}
			}
		}
	}
	else if (rsinfo->returnMode == SFRM_Materialize)
	{
		/* check we're on the same page as the function author */
		if (rsinfo->isDone != ExprSingleResult)
			ereport(ERROR,
					(errcode(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED),
					 errmsg("table-function protocol for materialize mode was not followed")));
		if (rsinfo->setResult != NULL)
		{
			/* prepare to return values from the tuplestore */
			ExecPrepareTuplestoreResult(fcache, econtext,
										rsinfo->setResult,
										rsinfo->setDesc);

			/*
			 * If we are being invoked by a Materialize node, attempt
			 * to donate the returned tuplstore to it.
			 */
			if (ExecSRFDonateResultTuplestore(fcache))
			{
				*isDone = ExprMultipleResult;
				return 0;
			}
			else
			{
				/* loop back to top to start returning from tuplestore */
				goto restart;
			}
		}
		/* if setResult was left null, treat it as empty set */
		*isDone = ExprEndResult;
		*isNull = true;
		result = (Datum) 0;
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED),
				 errmsg("unrecognized table-function returnMode: %d",
						(int) rsinfo->returnMode)));

	return result;
}


/*
 * init_sexpr - initialize a SetExprState node during first use
 */
static void
init_sexpr(Oid foid, Oid input_collation, Expr *node,
		   SetExprState *sexpr, PlanState *parent,
		   MemoryContext sexprCxt, bool allowSRF, bool needDescForSRF)
{
	AclResult	aclresult;
	size_t		numargs = list_length(sexpr->args);

	/* Check permission to call function */
	aclresult = pg_proc_aclcheck(foid, GetUserId(), ACL_EXECUTE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(foid));
	InvokeFunctionExecuteHook(foid);

	/*
	 * Safety check on nargs.  Under normal circumstances this should never
	 * fail, as parser should check sooner.  But possibly it might fail if
	 * server has been compiled with FUNC_MAX_ARGS smaller than some functions
	 * declared in pg_proc?
	 */
	if (list_length(sexpr->args) > FUNC_MAX_ARGS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
				 errmsg_plural("cannot pass more than %d argument to a function",
							   "cannot pass more than %d arguments to a function",
							   FUNC_MAX_ARGS,
							   FUNC_MAX_ARGS)));

	/* Set up the primary fmgr lookup information */
	fmgr_info_cxt(foid, &(sexpr->func), sexprCxt);
	fmgr_info_set_expr((Node *) sexpr->expr, &(sexpr->func));

	/* Initialize the function call parameter struct as well */
	sexpr->fcinfo =
		(FunctionCallInfo) palloc(SizeForFunctionCallInfo(numargs));
	InitFunctionCallInfoData(*sexpr->fcinfo, &(sexpr->func),
							 numargs,
							 input_collation, NULL, NULL);
	sexpr->fcinfo->resultinfo = NULL;

	/* If function returns set, check if that's allowed by caller */
	if (sexpr->func.fn_retset && !allowSRF)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set"),
				 parent ? executor_errposition(parent->state,
											   exprLocation((Node *) node)) : 0));

	/* Otherwise, caller should have marked the sexpr correctly */
	Assert(sexpr->func.fn_retset == sexpr->funcReturnsSet);

	/* If function returns set, prepare expected tuple descriptor */
	if (sexpr->func.fn_retset && needDescForSRF)
	{
		TypeFuncClass functypclass;
		Oid			funcrettype;
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		functypclass = get_expr_result_type(sexpr->func.fn_expr,
											&funcrettype,
											&tupdesc);

		/* Must save tupdesc in sexpr's context */
		oldcontext = MemoryContextSwitchTo(sexprCxt);

		if (functypclass == TYPEFUNC_COMPOSITE ||
			functypclass == TYPEFUNC_COMPOSITE_DOMAIN)
		{
			/* Composite data type, e.g. a table's row type */
			Assert(tupdesc);
			/* Must copy it out of typcache for safety */
			sexpr->funcResultDesc = CreateTupleDescCopy(tupdesc);
			sexpr->funcReturnsTuple = true;
		}
		else if (functypclass == TYPEFUNC_SCALAR)
		{
			/* Base data type, i.e. scalar */
			tupdesc = CreateTemplateTupleDesc(1);
			TupleDescInitEntry(tupdesc,
							   (AttrNumber) 1,
							   NULL,
							   funcrettype,
							   -1,
							   0);
			sexpr->funcResultDesc = tupdesc;
			sexpr->funcReturnsTuple = false;
		}
		else if (functypclass == TYPEFUNC_RECORD)
		{
			/* This will work if function doesn't need an expectedDesc */
			sexpr->funcResultDesc = NULL;
			sexpr->funcReturnsTuple = true;
		}
		else
		{
			/* Else, we will fail if function needs an expectedDesc */
			sexpr->funcResultDesc = NULL;
		}

		MemoryContextSwitchTo(oldcontext);
	}
	else
		sexpr->funcResultDesc = NULL;

	/* Initialize additional state */
	sexpr->funcResultStore = NULL;
	sexpr->funcResultSlot = NULL;
	sexpr->shutdown_reg = false;
	sexpr->funcResultStoreDonationEnabled = false;
}

/*
 * callback function in case a SetExprState needs to be shut down before it
 * has been run to completion
 */
static void
ShutdownSetExpr(Datum arg)
{
	SetExprState *sexpr = castNode(SetExprState, DatumGetPointer(arg));
	ReturnSetInfo *rsinfo = castNode(ReturnSetInfo, sexpr->fcinfo->resultinfo);

	/* If we have a slot, make sure it's let go of any tuplestore pointer */
	if (sexpr->funcResultSlot)
		ExecClearTuple(sexpr->funcResultSlot);

	/* Release any open tuplestore */
	if (sexpr->funcResultStore)
		tuplestore_end(sexpr->funcResultStore);
	sexpr->funcResultStore = NULL;

	/* Release the ReturnSetInfo structure */
	if (rsinfo != NULL)
	{
		pfree(rsinfo);
		sexpr->fcinfo->resultinfo = NULL;
	}

	/* Clear any active set-argument state */
	sexpr->setArgsValid = false;

	/* execUtils will deregister the callback... */
	sexpr->shutdown_reg = false;
}

/*
 * Evaluate arguments for a function.
 */
static void
ExecEvalFuncArgs(FunctionCallInfo fcinfo,
				 List *argList,
				 ExprContext *econtext)
{
	int			i;
	ListCell   *arg;

	i = 0;
	foreach(arg, argList)
	{
		ExprState  *argstate = (ExprState *) lfirst(arg);

		fcinfo->args[i].value = ExecEvalExpr(argstate,
											 econtext,
											 &fcinfo->args[i].isnull);
		i++;
	}

	Assert(i == fcinfo->nargs);
}

/*
 *		ExecPrepareTuplestoreResult
 *
 * Subroutine for ExecMakeFunctionResultSet: prepare to extract rows from a
 * tuplestore function result.  We must set up a funcResultSlot (unless
 * already done in a previous call cycle) and verify that the function
 * returned the expected tuple descriptor.
 */
static void
ExecPrepareTuplestoreResult(SetExprState *sexpr,
							ExprContext *econtext,
							Tuplestorestate *resultStore,
							TupleDesc resultDesc)
{
	sexpr->funcResultStore = resultStore;

	if (sexpr->funcResultSlot == NULL)
	{
		/* Create a slot so we can read data out of the tuplestore */
		TupleDesc	slotDesc;
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(sexpr->func.fn_mcxt);

		/*
		 * If we were not able to determine the result rowtype from context,
		 * and the function didn't return a tupdesc, we have to fail.
		 */
		if (sexpr->funcResultDesc)
			slotDesc = sexpr->funcResultDesc;
		else if (resultDesc)
		{
			/* don't assume resultDesc is long-lived */
			slotDesc = CreateTupleDescCopy(resultDesc);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning setof record called in "
							"context that cannot accept type record")));
			slotDesc = NULL;	/* keep compiler quiet */
		}

		sexpr->funcResultSlot = MakeSingleTupleTableSlot(slotDesc,
														 &TTSOpsMinimalTuple);
		MemoryContextSwitchTo(oldcontext);
	}

	/*
	 * If function provided a tupdesc, cross-check it.  We only really need to
	 * do this for functions returning RECORD, but might as well do it always.
	 */
	if (resultDesc)
	{
		if (sexpr->funcResultDesc)
			tupledesc_match(sexpr->funcResultDesc, resultDesc);

		/*
		 * If it is a dynamically-allocated TupleDesc, free it: it is
		 * typically allocated in a per-query context, so we must avoid
		 * leaking it across multiple usages.
		 */
		if (resultDesc->tdrefcount == -1)
			FreeTupleDesc(resultDesc);
	}

	/* Register cleanup callback if we didn't already */
	if (!sexpr->shutdown_reg)
	{
		RegisterExprContextCallback(econtext,
									ShutdownSetExpr,
									PointerGetDatum(sexpr));
		sexpr->shutdown_reg = true;
	}
}
