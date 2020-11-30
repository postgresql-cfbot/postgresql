/*-------------------------------------------------------------------------
 *
 * windowfuncs.c
 *	  Standard window functions defined in SQL spec.
 *
 * Portions Copyright (c) 2000-2020, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/windowfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/typedvalue.h"
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

#define PROXY_CONTEXT_MAGIC 19730715

typedef struct
{
	int			magic;
	Oid			typid;
	int16		typlen;
	bool		typbyval;
	int			allocsize;
	bool		isnull;
	Datum		value;
	char		data[FLEXIBLE_ARRAY_MEMBER];
} proxy_context;

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

/*
 * High level access function. These functions are wrappers for windows API
 * for PL languages based on usage WindowObjectProxy.
 */
Datum
windowobject_get_current_position(PG_FUNCTION_ARGS)
{
	WindowObjectProxy	wop;
	WindowObject winobj;
	int64		pos;

	wop = (WindowObjectProxy) DatumGetPointer(PG_GETARG_DATUM(0));

	winobj = wop->winobj;
	Assert(WindowObjectIsValid(winobj));

	pos = WinGetCurrentPosition(winobj);

	PG_RETURN_INT64(pos);
}

Datum
windowobject_set_mark_position(PG_FUNCTION_ARGS)
{
	WindowObjectProxy	wop;
	WindowObject winobj;
	int64		pos;

	wop = (WindowObjectProxy) DatumGetPointer(PG_GETARG_DATUM(0));

	winobj = wop->winobj;
	Assert(WindowObjectIsValid(winobj));

	pos = PG_GETARG_INT64(1);

	WinSetMarkPosition(winobj, pos);

	PG_RETURN_VOID();
}

Datum
windowobject_get_partition_rowcount(PG_FUNCTION_ARGS)
{
	WindowObjectProxy	wop;
	WindowObject winobj;
	int64		rc;

	wop = (WindowObjectProxy) DatumGetPointer(PG_GETARG_DATUM(0));

	winobj = wop->winobj;
	Assert(WindowObjectIsValid(winobj));

	rc = WinGetPartitionRowCount(winobj);

	PG_RETURN_INT64(rc);
}

Datum
windowobject_rows_are_peers(PG_FUNCTION_ARGS)
{
	WindowObjectProxy	wop;
	WindowObject winobj;
	int64		pos1,
				pos2;

	wop = (WindowObjectProxy) DatumGetPointer(PG_GETARG_DATUM(0));

	winobj = wop->winobj;
	Assert(WindowObjectIsValid(winobj));

	pos1 = PG_GETARG_INT64(1);
	pos2 = PG_GETARG_INT64(2);

	PG_RETURN_BOOL(WinRowsArePeers(winobj, pos1, pos2));
}

#define SEEK_CURRENT_STR		"seek_current"
#define SEEK_HEAD_STR			"seek_head"
#define SEEK_TAIL_STR			"seek_tail"

#define STRLEN(s)				(sizeof(s) - 1)

static int
get_seek_type(text *seektype)
{
	char	   *str;
	int			len;
	int			result;

	str = VARDATA_ANY(seektype);
	len = VARSIZE_ANY_EXHDR(seektype);

	if (len == STRLEN(SEEK_CURRENT_STR) && strncmp(str, SEEK_CURRENT_STR, len) == 0)
		result = WINDOW_SEEK_CURRENT;
	else if (len == STRLEN(SEEK_HEAD_STR) && strncmp(str, SEEK_HEAD_STR, len) == 0)
		result = WINDOW_SEEK_HEAD;
	else if (len == STRLEN(SEEK_TAIL_STR) && strncmp(str, SEEK_TAIL_STR, len) == 0)
		result = WINDOW_SEEK_TAIL;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("seek type value must be \"seek_current\", \"seek_head\" or \"seek_tail\"")));

	return result;
}

static Oid
wop_funcarg_info(WindowObjectProxy wop,
						   int argno,
						   int16 *typlen,
						   bool *typbyval)
{
	WindowObjectProxyMutable	*mutable_data = wop->mutable_data;

	if (argno != mutable_data->last_argno)
	{
		Oid		argtypid = get_fn_expr_argtype(wop->fcinfo->flinfo, argno);

		mutable_data->typid = getBaseType(argtypid);
		get_typlenbyval(mutable_data->typid,
						&mutable_data->typlen,
						&mutable_data->typbyval);
		mutable_data->last_argno = argno;
	}

	*typlen = mutable_data->typlen;
	*typbyval = mutable_data->typbyval;

	return mutable_data->typid;
}

Datum
windowobject_get_func_arg_partition(PG_FUNCTION_ARGS)
{
	WindowObjectProxy	wop;
	WindowObject winobj;
	int			argno;
	int			relpos;
	int			seektype;
	bool		set_mark;
	Datum		value;
	bool		isnull;
	Oid			typid;
	int16		typlen;
	bool		typbyval;

	wop = (WindowObjectProxy) DatumGetPointer(PG_GETARG_DATUM(0));

	winobj = wop->winobj;
	Assert(WindowObjectIsValid(winobj));

	argno = PG_GETARG_INT32(1);

	if (argno < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("arg number less than one")));

	argno -= 1;

	relpos = PG_GETARG_INT32(2);
	seektype = get_seek_type(PG_GETARG_TEXT_P(3));
	set_mark = PG_GETARG_BOOL(4);

	value = WinGetFuncArgInPartition(winobj,
											argno,
											relpos,
											seektype,
											set_mark,
											&isnull,
											&wop->mutable_data->isout);

	if (isnull)
		PG_RETURN_NULL();

	typid = wop_funcarg_info(wop, argno, &typlen, &typbyval);

	PG_RETURN_DATUM(makeTypedValue(value, typid, typlen, typbyval));
}

Datum
windowobject_get_func_arg_frame(PG_FUNCTION_ARGS)
{
	WindowObjectProxy	wop;
	WindowObject winobj;
	int			argno;
	int			relpos;
	int			seektype;
	bool		set_mark;
	Datum		value;
	bool		isnull;
	Oid			typid;
	int16		typlen;
	bool		typbyval;

	wop = (WindowObjectProxy) DatumGetPointer(PG_GETARG_DATUM(0));

	winobj = wop->winobj;
	Assert(WindowObjectIsValid(winobj));

	argno = PG_GETARG_INT32(1);
	relpos = PG_GETARG_INT32(2);
	seektype = get_seek_type(PG_GETARG_TEXT_P(3));
	set_mark = PG_GETARG_BOOL(4);

	if (argno < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("arg number less than one")));

	argno -= 1;

	value = WinGetFuncArgInFrame(winobj,
											argno,
											relpos,
											seektype,
											set_mark,
											&isnull,
											&wop->mutable_data->isout);

	if (isnull)
		PG_RETURN_NULL();

	typid = wop_funcarg_info(wop, argno, &typlen, &typbyval);

	PG_RETURN_DATUM(makeTypedValue(value, typid, typlen, typbyval));
}

Datum
windowobject_get_func_arg_current(PG_FUNCTION_ARGS)
{
	WindowObjectProxy	wop;
	WindowObject winobj;
	int			argno;
	Datum		value;
	bool		isnull;
	Oid			typid;
	int16		typlen;
	bool		typbyval;

	wop = (WindowObjectProxy) DatumGetPointer(PG_GETARG_DATUM(0));

	winobj = wop->winobj;

	Assert(WindowObjectIsValid(winobj));

	argno = PG_GETARG_INT32(1);

	if (argno < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("arg number less than one")));

	argno -= 1;

	value = WinGetFuncArgCurrent(winobj, argno, &isnull);

	wop->mutable_data->isout = false;

	if (isnull)
		PG_RETURN_NULL();

	typid = wop_funcarg_info(wop, argno, &typlen, &typbyval);
	PG_RETURN_DATUM(makeTypedValue(value, typid, typlen, typbyval));
}

static void
copy_datum_to_partition_context(proxy_context *pcontext,
								Datum value,
								bool isnull)
{
	if (!isnull)
	{
		if (pcontext->typbyval)
			pcontext->value = value;
		else if (pcontext->typlen == -1)
		{
			struct varlena *s = (struct varlena *) DatumGetPointer(value);

			memcpy(pcontext->data, s, VARSIZE_ANY(s));
			pcontext->value = PointerGetDatum(pcontext->data);
		}
		else
		{
			memcpy(pcontext->data, DatumGetPointer(value), pcontext->typlen);
			pcontext->value = PointerGetDatum(pcontext->data);
		}

		pcontext->isnull = false;
	}
	else
	{
		pcontext->value = (Datum) 0;
		pcontext->isnull = true;
	}
}

/*
 * Returns estimated size of windowobject partition context
 */
static int
estimate_partition_context_size(Datum value,
								bool isnull,
								int16 typlen,
								int16 minsize,
								int *realsize)
{
	if(typlen != -1)
	{
		if (typlen < sizeof(Datum))
		{
			*realsize = offsetof(proxy_context, data);

			return *realsize;
		}

		if (typlen > 1024)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("size of value is greather than limit (1024 bytes)")));

		*realsize = offsetof(proxy_context, data) + typlen;

		return *realsize;
	}
	else
	{
		if (!isnull)
		{
			int		size = VARSIZE_ANY_EXHDR(DatumGetPointer(value));

			if (size > 1024)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("size of value is greather than limit (1024 bytes)")));

			*realsize = size;

			size += size / 3;

			return offsetof(proxy_context, data)
					+ MAXALIGN(size > minsize ? size : minsize);
		}
		else
		{
			/* by default we allocate 30 bytes */
			*realsize = 0;

			return offsetof(proxy_context, data) + MAXALIGN(minsize);
		}
	}
}

#define VARLENA_MINSIZE				32

static proxy_context *
get_partition_context(FunctionCallInfo fcinfo, bool write_mode)
{
	WindowObjectProxy	wop;
	WindowObject winobj;
	Oid			typid;
	int16		typlen;
	bool		typbyval;
	Datum		value = (Datum) 0;
	bool		isnull = true;
	int			allocsize;
	int			minsize;
	int			realsize;
	proxy_context *pcontext;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("windowobject is NULL")));

	wop = (WindowObjectProxy) DatumGetPointer(PG_GETARG_DATUM(0));
	winobj = wop->winobj;
	Assert(WindowObjectIsValid(winobj));

	if (PG_ARGISNULL(2))
		minsize = VARLENA_MINSIZE;
	else
		minsize = PG_GETARG_INT32(2);

	if (!PG_ARGISNULL(1))
	{
		value = PG_GETARG_DATUM(1);
		isnull = false;
	}

	typid = get_fn_expr_argtype(fcinfo->flinfo, 1);
	if (!OidIsValid(typid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot detect type of context value")));

	typid = getBaseType(typid);
	get_typlenbyval(typid, &typlen, &typbyval);

	Assert(typlen != -2);

	allocsize = estimate_partition_context_size(value,
												isnull,
												typlen,
												minsize,
												&realsize);

	pcontext = (proxy_context *) WinGetPartitionLocalMemory(winobj, allocsize);

	/* fresh pcontext has zeroed memory */
	Assert(pcontext->magic == 0 || pcontext->magic == PROXY_CONTEXT_MAGIC);

	if (pcontext->allocsize > 0)
	{
		if (realsize > pcontext->allocsize)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("the value cannot be saved to allocated buffer"),
					 errhint("Try to increase the minsize argument.")));

		if (pcontext->typid != typid)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("partition context was initialized for different type")));

		if (write_mode)
			copy_datum_to_partition_context(pcontext, value, isnull);

	}
	else
	{
		pcontext->magic = PROXY_CONTEXT_MAGIC;
		pcontext->typid = typid;
		pcontext->typlen = typlen;
		pcontext->typbyval = typbyval;
		pcontext->allocsize = allocsize;

		copy_datum_to_partition_context(pcontext, value, isnull);
	}

	return pcontext;
}

Datum
windowobject_set_partition_context_value(PG_FUNCTION_ARGS)
{
	(void) get_partition_context(fcinfo, true);

	PG_RETURN_VOID();
}

Datum
windowobject_get_partition_context_value(PG_FUNCTION_ARGS)
{
	proxy_context *pcontext;

	pcontext = get_partition_context(fcinfo, false);

	if (pcontext->isnull)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(pcontext->value);
}
