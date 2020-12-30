/*-------------------------------------------------------------------------
 *
 * barrier.c
 *	  emit ProcSignalBarriers for testing purposes
 *
 * Copyright (c) 2016-2020, PostgreSQL Global Development Group
 *
 *	  contrib/barrier/barrier.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "storage/procsignal.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(emit_barrier);
PG_FUNCTION_INFO_V1(wait_barrier);

static ProcSignalBarrierType
get_barrier_type(text *barrier_type)
{
	char	   *btype = text_to_cstring(barrier_type);

	if (strcmp(btype, "placeholder") == 0)
		return PROCSIGNAL_BARRIER_PLACEHOLDER;

	elog(ERROR, "unknown barrier type: \"%s\"", btype);
}

Datum
emit_barrier(PG_FUNCTION_ARGS)
{
	text	   *barrier_type = PG_GETARG_TEXT_PP(0);
	int32		count = PG_GETARG_INT32(1);
	int32		i;
	ProcSignalBarrierType t = get_barrier_type(barrier_type);

	for (i = 0; i < count; ++i)
	{
		CHECK_FOR_INTERRUPTS();
		EmitProcSignalBarrier(t);
	}

	PG_RETURN_VOID();
}

Datum
wait_barrier(PG_FUNCTION_ARGS)
{
	text	   *barrier_type = PG_GETARG_TEXT_PP(0);
	ProcSignalBarrierType t = get_barrier_type(barrier_type);
	uint64		generation;

	generation = EmitProcSignalBarrier(t);
	elog(NOTICE, "waiting for barrier");
	WaitForProcSignalBarrier(generation);

	PG_RETURN_VOID();
}
