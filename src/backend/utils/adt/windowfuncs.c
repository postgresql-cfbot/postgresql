/*-------------------------------------------------------------------------
 *
 * windowfuncs.c
 *	  Standard window functions defined in SQL spec.
 *
 * Portions Copyright (c) 2000-2021, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/windowfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/stratnum.h"
#include "catalog/pg_operator_d.h"
#include "nodes/makefuncs.h"
#include "nodes/supportnodes.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "windowapi.h"

/*
 * ranking process information
 */
typedef struct rank_context
{
	int64		rank;			/* current rank */
} rank_context;

/*
 * ntile process information
 */
typedef struct
{
	int32		ntile;			/* current result */
	int64		rows_per_bucket;	/* row number of current bucket */
	int64		boundary;		/* how many rows should be in the bucket */
	int64		remainder;		/* (total rows) % (bucket num) */
} ntile_context;

static bool rank_up(WindowObject winobj);
static Datum leadlag_common(FunctionCallInfo fcinfo,
							bool forward, bool withoffset, bool withdefault);


/*
 * utility routine for *_rank functions.
 */
static bool
rank_up(WindowObject winobj)
{
	bool		up = false;		/* should rank increase? */
	int64		curpos = WinGetCurrentPosition(winobj);
	rank_context *context;

	context = (rank_context *)
		WinGetPartitionLocalMemory(winobj, sizeof(rank_context));

	if (context->rank == 0)
	{
		/* first call: rank of first row is always 1 */
		Assert(curpos == 0);
		context->rank = 1;
	}
	else
	{
		Assert(curpos > 0);
		/* do current and prior tuples match by ORDER BY clause? */
		if (!WinRowsArePeers(winobj, curpos - 1, curpos))
			up = true;
	}

	/* We can advance the mark, but only *after* access to prior row */
	WinSetMarkPosition(winobj, curpos);

	return up;
}

/*
 * process_row_number_rank_run_condition
 *		Common function for prosupport WindowFunctionRunCondition for
 *		row_number(), rank() and dense_rank().
 */
static void
process_row_number_rank_run_condition(WindowFunctionRunCondition *req)
{
	OpExpr	   *opexpr = req->opexpr;
	Oid			opno = opexpr->opno;
	Expr	   *arg;
	WindowFunc *windowfunc;
	List	   *opfamilies;
	ListCell   *lc;

	/* Early abort of execution is not possible if there's a PARTITION BY */
	if (req->window_clause->partitionClause != NULL)
	{
		req->runopexpr = NULL;
		return;
	}

	if (!req->wfunc_left)
	{
		arg = linitial(opexpr->args);
		windowfunc = lsecond(opexpr->args);
	}
	else
	{
		windowfunc = linitial(opexpr->args);
		arg = lsecond(opexpr->args);
	}

	/*
	 * We're only able to handle run conditions that compare the window result
	 * to a constant.
	 *
	 * XXX We could probably do better than just Consts.  Exec Params should
	 * work too.
	 */
	if (!IsA(arg, Const))
	{
		req->runopexpr = NULL;
		return;
	}

	opfamilies = get_op_btree_interpretation(opno);

	foreach(lc, opfamilies)
	{
		OpBtreeInterpretation *oi = (OpBtreeInterpretation *) lfirst(lc);

		if (req->wfunc_left)
		{
			/*
			 * When the opexpr is uses < or <= with the window function on the
			 * left, then we can use the opexpr directly.  We can also set
			 * keep_original to false too as the planner does not need to keep
			 * this qual as a filter in the query above the subquery
			 * containing the window function.
			 */
			if (oi->strategy == BTLessStrategyNumber ||
				oi->strategy == BTLessEqualStrategyNumber)
			{
				req->keep_original = false;
				req->runopexpr = req->opexpr;
				break;
			}

			/*
			 * For equality conditions we need all rows up until the const
			 * being compared.  We make an OpExpr with a <= operator so that
			 * we stop processing just after we find our equality match.
			 */
			else if (oi->strategy == BTEqualStrategyNumber)
			{
				OpExpr *newopexpr;
				Oid leop;

				leop = get_opfamily_member(oi->opfamily_id,
											oi->oplefttype,
											oi->oprighttype,
											BTLessEqualStrategyNumber);

				newopexpr = (OpExpr *) make_opclause(leop,
													 opexpr->opresulttype,
													 opexpr->opretset,
													 (Expr *) windowfunc,
													 arg,
													 opexpr->opcollid,
													 opexpr->inputcollid);
				newopexpr->opfuncid = get_opcode(leop);

				/*
				 * We must keep the original equality condition as this <=
				 * OpExpr won't filter out all the earlier records.
				 */
				req->keep_original = true;
				req->runopexpr = newopexpr;
				break;
			}
		}
		else
		{
			/*
			 * When the window function var is on the right, we look for > and
			 * >= operators.  e.g: 10 >= row_number() ...
			 */
			if (oi->strategy == BTGreaterStrategyNumber ||
				oi->strategy == BTGreaterEqualStrategyNumber)
			{
				req->keep_original = false;
				req->runopexpr = req->opexpr;
				break;
			}

			/*
			 * For equality conditions we need all rows up until the const
			 * being compared.  The window function is on the right here, so
			 * we make an OpExpr with <const> >= <wfunc> so that we stop
			 * processing just after we find our equality match. We don't
			 * reverse the condition and use <= because we may have a cross
			 * type opfamily.
			 */
			else if (oi->strategy == BTEqualStrategyNumber)
			{
				OpExpr *newopexpr;
				Oid geop;

				geop = get_opfamily_member(oi->opfamily_id,
											oi->oplefttype,
											oi->oprighttype,
											BTGreaterEqualStrategyNumber);

				newopexpr = (OpExpr *) make_opclause(geop,
													 opexpr->opresulttype,
													 opexpr->opretset,
													 arg,
													 (Expr *) windowfunc,
													 opexpr->opcollid,
													 opexpr->inputcollid);
				newopexpr->opfuncid = get_opcode(geop);

				/*
				 * We must keep the original equality condition as this >=
				 * OpExpr won't filter out all the earlier records.
				 */
				req->keep_original = true;
				req->runopexpr = newopexpr;
				break;
			}
		}
	}

	list_free(opfamilies);
}

/*
 * row_number
 * just increment up from 1 until current partition finishes.
 */
Datum
window_row_number(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	int64		curpos = WinGetCurrentPosition(winobj);

	WinSetMarkPosition(winobj, curpos);
	PG_RETURN_INT64(curpos + 1);
}

/*
 * window_row_number_support
 *		prosupport function for window_row_number()
 */
Datum
window_row_number_support(PG_FUNCTION_ARGS)
{
	Node	   *rawreq = (Node *) PG_GETARG_POINTER(0);

	if (IsA(rawreq, WindowFunctionRunCondition))
	{
		WindowFunctionRunCondition *req = (WindowFunctionRunCondition *) rawreq;

		process_row_number_rank_run_condition(req);
		PG_RETURN_POINTER(req);
	}

	PG_RETURN_POINTER(NULL);
}

/*
 * rank
 * Rank changes when key columns change.
 * The new rank number is the current row number.
 */
Datum
window_rank(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	rank_context *context;
	bool		up;

	up = rank_up(winobj);
	context = (rank_context *)
		WinGetPartitionLocalMemory(winobj, sizeof(rank_context));
	if (up)
		context->rank = WinGetCurrentPosition(winobj) + 1;

	PG_RETURN_INT64(context->rank);
}

/*
 * window_rank_support
 *		prosupport function for window_rank()
 */
Datum
window_rank_support(PG_FUNCTION_ARGS)
{
	Node	   *rawreq = (Node *) PG_GETARG_POINTER(0);

	if (IsA(rawreq, WindowFunctionRunCondition))
	{
		WindowFunctionRunCondition *req = (WindowFunctionRunCondition *) rawreq;

		process_row_number_rank_run_condition(req);
		PG_RETURN_POINTER(req);
	}

	PG_RETURN_POINTER(NULL);
}

/*
 * dense_rank
 * Rank increases by 1 when key columns change.
 */
Datum
window_dense_rank(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	rank_context *context;
	bool		up;

	up = rank_up(winobj);
	context = (rank_context *)
		WinGetPartitionLocalMemory(winobj, sizeof(rank_context));
	if (up)
		context->rank++;

	PG_RETURN_INT64(context->rank);
}

/*
 * window_dense_rank_support
 *		prosupport function for window_dense_rank()
 */
Datum
window_dense_rank_support(PG_FUNCTION_ARGS)
{
	Node	   *rawreq = (Node *) PG_GETARG_POINTER(0);

	if (IsA(rawreq, WindowFunctionRunCondition))
	{
		WindowFunctionRunCondition *req = (WindowFunctionRunCondition *) rawreq;

		process_row_number_rank_run_condition(req);
		PG_RETURN_POINTER(req);
	}

	PG_RETURN_POINTER(NULL);
}

/*
 * percent_rank
 * return fraction between 0 and 1 inclusive,
 * which is described as (RK - 1) / (NR - 1), where RK is the current row's
 * rank and NR is the total number of rows, per spec.
 */
Datum
window_percent_rank(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	rank_context *context;
	bool		up;
	int64		totalrows = WinGetPartitionRowCount(winobj);

	Assert(totalrows > 0);

	up = rank_up(winobj);
	context = (rank_context *)
		WinGetPartitionLocalMemory(winobj, sizeof(rank_context));
	if (up)
		context->rank = WinGetCurrentPosition(winobj) + 1;

	/* return zero if there's only one row, per spec */
	if (totalrows <= 1)
		PG_RETURN_FLOAT8(0.0);

	PG_RETURN_FLOAT8((float8) (context->rank - 1) / (float8) (totalrows - 1));
}

/*
 * cume_dist
 * return fraction between 0 and 1 inclusive,
 * which is described as NP / NR, where NP is the number of rows preceding or
 * peers to the current row, and NR is the total number of rows, per spec.
 */
Datum
window_cume_dist(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	rank_context *context;
	bool		up;
	int64		totalrows = WinGetPartitionRowCount(winobj);

	Assert(totalrows > 0);

	up = rank_up(winobj);
	context = (rank_context *)
		WinGetPartitionLocalMemory(winobj, sizeof(rank_context));
	if (up || context->rank == 1)
	{
		/*
		 * The current row is not peer to prior row or is just the first, so
		 * count up the number of rows that are peer to the current.
		 */
		int64		row;

		context->rank = WinGetCurrentPosition(winobj) + 1;

		/*
		 * start from current + 1
		 */
		for (row = context->rank; row < totalrows; row++)
		{
			if (!WinRowsArePeers(winobj, row - 1, row))
				break;
			context->rank++;
		}
	}

	PG_RETURN_FLOAT8((float8) context->rank / (float8) totalrows);
}

/*
 * ntile
 * compute an exact numeric value with scale 0 (zero),
 * ranging from 1 (one) to n, per spec.
 */
Datum
window_ntile(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	ntile_context *context;

	context = (ntile_context *)
		WinGetPartitionLocalMemory(winobj, sizeof(ntile_context));

	if (context->ntile == 0)
	{
		/* first call */
		int64		total;
		int32		nbuckets;
		bool		isnull;

		total = WinGetPartitionRowCount(winobj);
		nbuckets = DatumGetInt32(WinGetFuncArgCurrent(winobj, 0, &isnull));

		/*
		 * per spec: If NT is the null value, then the result is the null
		 * value.
		 */
		if (isnull)
			PG_RETURN_NULL();

		/*
		 * per spec: If NT is less than or equal to 0 (zero), then an
		 * exception condition is raised.
		 */
		if (nbuckets <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ARGUMENT_FOR_NTILE),
					 errmsg("argument of ntile must be greater than zero")));

		context->ntile = 1;
		context->rows_per_bucket = 0;
		context->boundary = total / nbuckets;
		if (context->boundary <= 0)
			context->boundary = 1;
		else
		{
			/*
			 * If the total number is not divisible, add 1 row to leading
			 * buckets.
			 */
			context->remainder = total % nbuckets;
			if (context->remainder != 0)
				context->boundary++;
		}
	}

	context->rows_per_bucket++;
	if (context->boundary < context->rows_per_bucket)
	{
		/* ntile up */
		if (context->remainder != 0 && context->ntile == context->remainder)
		{
			context->remainder = 0;
			context->boundary -= 1;
		}
		context->ntile += 1;
		context->rows_per_bucket = 1;
	}

	PG_RETURN_INT32(context->ntile);
}

/*
 * leadlag_common
 * common operation of lead() and lag()
 * For lead() forward is true, whereas for lag() it is false.
 * withoffset indicates we have an offset second argument.
 * withdefault indicates we have a default third argument.
 */
static Datum
leadlag_common(FunctionCallInfo fcinfo,
			   bool forward, bool withoffset, bool withdefault)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	int32		offset;
	bool		const_offset;
	Datum		result;
	bool		isnull;
	bool		isout;

	if (withoffset)
	{
		offset = DatumGetInt32(WinGetFuncArgCurrent(winobj, 1, &isnull));
		if (isnull)
			PG_RETURN_NULL();
		const_offset = get_fn_expr_arg_stable(fcinfo->flinfo, 1);
	}
	else
	{
		offset = 1;
		const_offset = true;
	}

	result = WinGetFuncArgInPartition(winobj, 0,
									  (forward ? offset : -offset),
									  WINDOW_SEEK_CURRENT,
									  const_offset,
									  &isnull, &isout);

	if (isout)
	{
		/*
		 * target row is out of the partition; supply default value if
		 * provided.  otherwise it'll stay NULL
		 */
		if (withdefault)
			result = WinGetFuncArgCurrent(winobj, 2, &isnull);
	}

	if (isnull)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(result);
}

/*
 * lag
 * returns the value of VE evaluated on a row that is 1
 * row before the current row within a partition,
 * per spec.
 */
Datum
window_lag(PG_FUNCTION_ARGS)
{
	return leadlag_common(fcinfo, false, false, false);
}

/*
 * lag_with_offset
 * returns the value of VE evaluated on a row that is OFFSET
 * rows before the current row within a partition,
 * per spec.
 */
Datum
window_lag_with_offset(PG_FUNCTION_ARGS)
{
	return leadlag_common(fcinfo, false, true, false);
}

/*
 * lag_with_offset_and_default
 * same as lag_with_offset but accepts default value
 * as its third argument.
 */
Datum
window_lag_with_offset_and_default(PG_FUNCTION_ARGS)
{
	return leadlag_common(fcinfo, false, true, true);
}

/*
 * lead
 * returns the value of VE evaluated on a row that is 1
 * row after the current row within a partition,
 * per spec.
 */
Datum
window_lead(PG_FUNCTION_ARGS)
{
	return leadlag_common(fcinfo, true, false, false);
}

/*
 * lead_with_offset
 * returns the value of VE evaluated on a row that is OFFSET
 * number of rows after the current row within a partition,
 * per spec.
 */
Datum
window_lead_with_offset(PG_FUNCTION_ARGS)
{
	return leadlag_common(fcinfo, true, true, false);
}

/*
 * lead_with_offset_and_default
 * same as lead_with_offset but accepts default value
 * as its third argument.
 */
Datum
window_lead_with_offset_and_default(PG_FUNCTION_ARGS)
{
	return leadlag_common(fcinfo, true, true, true);
}

/*
 * first_value
 * return the value of VE evaluated on the first row of the
 * window frame, per spec.
 */
Datum
window_first_value(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	Datum		result;
	bool		isnull;

	result = WinGetFuncArgInFrame(winobj, 0,
								  0, WINDOW_SEEK_HEAD, true,
								  &isnull, NULL);
	if (isnull)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(result);
}

/*
 * last_value
 * return the value of VE evaluated on the last row of the
 * window frame, per spec.
 */
Datum
window_last_value(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	Datum		result;
	bool		isnull;

	result = WinGetFuncArgInFrame(winobj, 0,
								  0, WINDOW_SEEK_TAIL, true,
								  &isnull, NULL);
	if (isnull)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(result);
}

/*
 * nth_value
 * return the value of VE evaluated on the n-th row from the first
 * row of the window frame, per spec.
 */
Datum
window_nth_value(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	bool		const_offset;
	Datum		result;
	bool		isnull;
	int32		nth;

	nth = DatumGetInt32(WinGetFuncArgCurrent(winobj, 1, &isnull));
	if (isnull)
		PG_RETURN_NULL();
	const_offset = get_fn_expr_arg_stable(fcinfo->flinfo, 1);

	if (nth <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE),
				 errmsg("argument of nth_value must be greater than zero")));

	result = WinGetFuncArgInFrame(winobj, 0,
								  nth - 1, WINDOW_SEEK_HEAD, const_offset,
								  &isnull, NULL);
	if (isnull)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(result);
}
