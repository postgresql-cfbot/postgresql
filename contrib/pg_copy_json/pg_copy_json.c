/*--------------------------------------------------------------------------
 *
 * pg_copy_json.c
 *		COPY TO JSON (JavaScript Object Notation) format.
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/test_copy_format.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "commands/copy.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "utils/json.h"

PG_MODULE_MAGIC;

typedef struct
{
	/*
	 * Force output of square brackets as array decorations at the beginning
	 * and end of output, with commas between the rows.
	 */
	bool	force_array;
	bool	force_array_specified;
	
	/* need delimiter to start next json array element */
	bool	json_row_delim_needed;
} CopyJsonData;

static inline void
InitCopyJsonData(CopyJsonData *p)
{
	Assert(p);
	p->force_array = false;
	p->force_array_specified = false;
	p->json_row_delim_needed = false;
}

static void
CopyToJsonSendEndOfRow(CopyToState cstate)
{
	switch (cstate->copy_dest)
	{
		case COPY_DEST_FILE:
			/* Default line termination depends on platform */
#ifndef WIN32
			CopySendChar(cstate, '\n');
#else
			CopySendString(cstate, "\r\n");
#endif
			break;
		case COPY_DEST_FRONTEND:
			/* The FE/BE protocol uses \n as newline for all platforms */
			CopySendChar(cstate, '\n');
			break;
		default:
			break;
	}
	CopyToStateFlush(cstate);
}

static bool
CopyToJsonProcessOption(CopyToState cstate, DefElem *defel)
{
	CopyJsonData	   *p;

	if (cstate->opaque == NULL)
	{
		MemoryContext oldcontext;
		oldcontext = MemoryContextSwitchTo(cstate->copycontext);
		cstate->opaque = palloc0(sizeof(CopyJsonData));
		MemoryContextSwitchTo(oldcontext);
		InitCopyJsonData(cstate->opaque);
	}

	p = (CopyJsonData *)cstate->opaque;

	if (strcmp(defel->defname, "force_array") == 0)
	{
		if (p->force_array_specified)
			ereport(ERROR,
					errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("CopyToJsonProcessOption: redundant options \"%s\"=\"%s\"",
						   defel->defname, defGetString(defel)));
		p->force_array_specified = true;
		p->force_array = defGetBoolean(defel);

		return true;
	}

	return false;
}

static void
CopyToJsonSendCopyBegin(CopyToState cstate)
{
	StringInfoData buf;
	int16		format = 0;

	pq_beginmessage(&buf, PqMsg_CopyOutResponse);
	pq_sendbyte(&buf, format);	/* overall format */
	/*
	 * JSON mode is always one non-binary column
	 */
	pq_sendint16(&buf, 1);
	pq_sendint16(&buf, 0);
	pq_endmessage(&buf);
}

static void
CopyToJsonStart(CopyToState cstate, TupleDesc tupDesc)
{
	CopyJsonData	   *p;

	if (cstate->opaque == NULL)
	{
		MemoryContext oldcontext;
		oldcontext = MemoryContextSwitchTo(cstate->copycontext);
		cstate->opaque = palloc0(sizeof(CopyJsonData));
		MemoryContextSwitchTo(oldcontext);
		InitCopyJsonData(cstate->opaque);
	}

	/* No need to alloc cstate->out_functions */

	p = (CopyJsonData *)cstate->opaque;

	/* If FORCE_ARRAY has been specified send the open bracket. */
	if (p->force_array)
	{
		CopySendChar(cstate, '[');
		CopyToJsonSendEndOfRow(cstate);
	}
}

static void
CopyToJsonOneRow(CopyToState cstate, TupleTableSlot *slot)
{
	Datum				rowdata;
	StringInfo			result;
	CopyJsonData	   *p;

	Assert(cstate->opaque);
	p = (CopyJsonData *)cstate->opaque;

	if(!cstate->rel)
	{
		for (int i = 0; i < slot->tts_tupleDescriptor->natts; i++)
		{
			/* Flat-copy the attribute array */
			memcpy(TupleDescAttr(slot->tts_tupleDescriptor, i),
			TupleDescAttr(cstate->queryDesc->tupDesc, i),
							1 * sizeof(FormData_pg_attribute));
		}
		BlessTupleDesc(slot->tts_tupleDescriptor);
	}
	rowdata = ExecFetchSlotHeapTupleDatum(slot);
	result = makeStringInfo();
	composite_to_json(rowdata, result, false);

	if (p->json_row_delim_needed)
		CopySendChar(cstate, ',');
	else if (p->force_array)
	{
		/* first row needs no delimiter */
		CopySendChar(cstate, ' ');
		p->json_row_delim_needed = true;
	}
	CopySendData(cstate, result->data, result->len);
	CopyToJsonSendEndOfRow(cstate);
}

static void
CopyToJsonEnd(CopyToState cstate)
{
	CopyJsonData	   *p;

	Assert(cstate->opaque);
	p = (CopyJsonData *)cstate->opaque;

	/* If FORCE_ARRAY has been specified send the close bracket. */
	if (p->force_array)
	{
		CopySendChar(cstate, ']');
		CopyToJsonSendEndOfRow(cstate);
	}
}

static const CopyToRoutine CopyToRoutineJson = {
	.type = T_CopyToRoutine,
	.CopyToProcessOption = CopyToJsonProcessOption,
	.CopyToSendCopyBegin = CopyToJsonSendCopyBegin,
	.CopyToStart = CopyToJsonStart,
	.CopyToOneRow = CopyToJsonOneRow,
	.CopyToEnd = CopyToJsonEnd,
};

PG_FUNCTION_INFO_V1(copy_json);
Datum
copy_json(PG_FUNCTION_ARGS)
{
	bool		is_from = PG_GETARG_BOOL(0);

	if (is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot use JSON mode in COPY FROM")));

	PG_RETURN_POINTER(&CopyToRoutineJson);
}
