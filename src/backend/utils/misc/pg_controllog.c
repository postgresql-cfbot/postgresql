/*-------------------------------------------------------------------------
 *
 * pg_controllog.c
 *
 * Routines to expose the contents of the control log file via a set of SQL
 * functions.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/pg_controllog.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/pg_type.h"
#include "common/controllog_utils.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"

/*
 * pg_operation_log
 *
 * Returns list of operation log data.
 * NOTE: this is a set-returning-function (SRF).
 */
Datum
pg_operation_log(PG_FUNCTION_ARGS)
{
#define PG_OPERATION_LOG_COLS	6
	FuncCallContext *funcctx;
	OperationLogBuffer *log_buffer;

	/*
	 * Initialize tuple descriptor & function call context.
	 */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcxt;
		TupleDesc	tupdesc;
		bool		crc_ok;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcxt = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* read the control file */
		log_buffer = get_operation_log(DataDir, &crc_ok);
		if (!crc_ok)
			ereport(ERROR,
					(errmsg("calculated CRC checksum does not match value stored in file")));

		tupdesc = CreateTemplateTupleDesc(PG_OPERATION_LOG_COLS);

		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "event",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "edition",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "version",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "lsn",
						   PG_LSNOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "last",
						   TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "count",
						   INT4OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/* The only state we need is the operation log buffer. */
		funcctx->user_fctx = (void *) log_buffer;

		MemoryContextSwitchTo(oldcxt);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	log_buffer = (OperationLogBuffer *) funcctx->user_fctx;

	if (funcctx->call_cntr < get_operation_log_count(log_buffer))
	{
		Datum		result;
		Datum		values[PG_OPERATION_LOG_COLS];
		bool		nulls[PG_OPERATION_LOG_COLS];
		HeapTuple	tuple;
		OperationLogData *data = get_operation_log_element(log_buffer, (uint16) funcctx->call_cntr);
		int			major_version,
					minor_version,
					patch_version;

		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(nulls, 0, sizeof(nulls));
		MemSet(values, 0, sizeof(values));

		/* event */
		values[0] = CStringGetTextDatum(get_operation_log_type_name(data->ol_type));

		/* edition */
		values[1] = CStringGetTextDatum(get_str_edition(data->ol_edition));

		/* version */
		patch_version = data->ol_version % 100;
		minor_version = (data->ol_version / 100) % 100;
		major_version = data->ol_version / 10000;
		if (major_version < 1000)
			values[2] = CStringGetTextDatum(psprintf("%u.%u.%u.%u", major_version / 100,
													 major_version % 100,
													 minor_version, patch_version));
		else
			values[2] = CStringGetTextDatum(psprintf("%u.%u.%u", major_version / 100,
													 minor_version, patch_version));

		/* lsn */
		values[3] = LSNGetDatum(data->ol_lsn);

		/* last */
		values[4] = TimestampTzGetDatum(time_t_to_timestamptz(data->ol_timestamp));

		/* count */
		values[5] = Int32GetDatum(data->ol_count);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		SRF_RETURN_NEXT(funcctx, result);
	}

	/* done when there are no more elements left */
	SRF_RETURN_DONE(funcctx);
}
