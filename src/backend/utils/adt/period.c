/*-------------------------------------------------------------------------
 *
 * period.c
 *	  Functions to support periods.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/period.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/tuptable.h"
#include "fmgr.h"
#include "nodes/primnodes.h"
#include "utils/fmgrprotos.h"
#include "utils/period.h"
#include "utils/rangetypes.h"

Datum period_to_range(TupleTableSlot *slot, int startattno, int endattno, Oid rangetype)
{
	Datum startvalue;
	Datum endvalue;
	Datum result;
	bool	startisnull;
	bool	endisnull;
	LOCAL_FCINFO(fcinfo, 2);
	FmgrInfo	flinfo;
	FuncExpr   *f;

	InitFunctionCallInfoData(*fcinfo, &flinfo, 2, InvalidOid, NULL, NULL);
	f = makeNode(FuncExpr);
	f->funcresulttype = rangetype;
	flinfo.fn_expr = (Node *) f;
	flinfo.fn_extra = NULL;

	/* compute oldvalue */
	startvalue = slot_getattr(slot, startattno, &startisnull);
	endvalue = slot_getattr(slot, endattno, &endisnull);

	fcinfo->args[0].value = startvalue;
	fcinfo->args[0].isnull = startisnull;
	fcinfo->args[1].value = endvalue;
	fcinfo->args[1].isnull = endisnull;

	result = range_constructor2(fcinfo);
	if (fcinfo->isnull)
		elog(ERROR, "function %u returned NULL", flinfo.fn_oid);

	return result;
}
