/*--------------------------------------------------------------------------
 *
 * test_copy_format.c
 *		Code for testing custom COPY format.
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_copy_format/test_copy_format.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "commands/copyapi.h"
#include "commands/defrem.h"

PG_MODULE_MAGIC;

static void
CopyFromInFunc(CopyFromState cstate, Oid atttypid,
			   FmgrInfo *finfo, Oid *typioparam)
{
	ereport(NOTICE, (errmsg("CopyFromInFunc: atttypid=%d", atttypid)));
}

static void
CopyFromStart(CopyFromState cstate, TupleDesc tupDesc)
{
	ereport(NOTICE, (errmsg("CopyFromStart: natts=%d", tupDesc->natts)));
}

static bool
CopyFromOneRow(CopyFromState cstate, ExprContext *econtext, Datum *values, bool *nulls)
{
	ereport(NOTICE, (errmsg("CopyFromOneRow")));
	return false;
}

static void
CopyFromEnd(CopyFromState cstate)
{
	ereport(NOTICE, (errmsg("CopyFromEnd")));
}

static const CopyFromRoutine CopyFromRoutineTestCopyFormat = {
	.type = T_CopyFromRoutine,
	.CopyFromInFunc = CopyFromInFunc,
	.CopyFromStart = CopyFromStart,
	.CopyFromOneRow = CopyFromOneRow,
	.CopyFromEnd = CopyFromEnd,
};

static void
CopyToOutFunc(CopyToState cstate, Oid atttypid, FmgrInfo *finfo)
{
	ereport(NOTICE, (errmsg("CopyToOutFunc: atttypid=%d", atttypid)));
}

static void
CopyToStart(CopyToState cstate, TupleDesc tupDesc)
{
	ereport(NOTICE, (errmsg("CopyToStart: natts=%d", tupDesc->natts)));
}

static void
CopyToOneRow(CopyToState cstate, TupleTableSlot *slot)
{
	ereport(NOTICE, (errmsg("CopyToOneRow: tts_nvalid=%u", slot->tts_nvalid)));
}

static void
CopyToEnd(CopyToState cstate)
{
	ereport(NOTICE, (errmsg("CopyToEnd")));
}

static const CopyToRoutine CopyToRoutineTestCopyFormat = {
	.type = T_CopyToRoutine,
	.CopyToOutFunc = CopyToOutFunc,
	.CopyToStart = CopyToStart,
	.CopyToOneRow = CopyToOneRow,
	.CopyToEnd = CopyToEnd,
};

PG_FUNCTION_INFO_V1(test_copy_format);
Datum
test_copy_format(PG_FUNCTION_ARGS)
{
	bool		is_from = PG_GETARG_BOOL(0);

	ereport(NOTICE,
			(errmsg("test_copy_format: is_from=%s", is_from ? "true" : "false")));

	if (is_from)
		PG_RETURN_POINTER(&CopyFromRoutineTestCopyFormat);
	else
		PG_RETURN_POINTER(&CopyToRoutineTestCopyFormat);
}
