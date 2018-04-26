/*----------------------------------------------------------------------
 *
 * tableamapi.c
 *		Support routines for API for Postgres table access methods
 *
 * FIXME: looks like this should be in amapi.c.
 *
 * Copyright (c) 2016, PostgreSQL Global Development Group
 *
 * src/backend/access/table/tableamapi.c
 *----------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/tableamapi.h"
#include "catalog/pg_am.h"
#include "catalog/pg_proc.h"
#include "utils/syscache.h"
#include "utils/memutils.h"


/*
 * GetTableAmRoutine
 *		Call the specified access method handler routine to get its
 *		TableAmRoutine struct, which will be palloc'd in the caller's
 *		memory context.
 */
TableAmRoutine *
GetTableAmRoutine(Oid amhandler)
{
	Datum		datum;
	TableAmRoutine *routine;

	datum = OidFunctionCall0(amhandler);
	routine = (TableAmRoutine *) DatumGetPointer(datum);

	if (routine == NULL || !IsA(routine, TableAmRoutine))
		elog(ERROR, "Table access method handler %u did not return a TableAmRoutine struct",
			 amhandler);

	return routine;
}

/* A crock */
TableAmRoutine *
GetHeapamTableAmRoutine(void)
{
	Datum		datum;
	static TableAmRoutine * HeapTableAmRoutine = NULL;

	if (HeapTableAmRoutine == NULL)
	{
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		datum = OidFunctionCall0(HEAP_TABLE_AM_HANDLER_OID);
		HeapTableAmRoutine = (TableAmRoutine *) DatumGetPointer(datum);
		MemoryContextSwitchTo(oldcxt);
	}

	return HeapTableAmRoutine;
}

/*
 * GetTableAmRoutineByAmId - look up the handler of the table access
 * method with the given OID, and get its TableAmRoutine struct.
 */
TableAmRoutine *
GetTableAmRoutineByAmId(Oid amoid)
{
	regproc		amhandler;
	HeapTuple	tuple;
	Form_pg_am	amform;

	/* Get handler function OID for the access method */
	tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(amoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for access method %u",
			 amoid);
	amform = (Form_pg_am) GETSTRUCT(tuple);

	/* Check that it is a table access method */
	if (amform->amtype != AMTYPE_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("access method \"%s\" is not of type %s",
						NameStr(amform->amname), "TABLE")));

	amhandler = amform->amhandler;

	/* Complain if handler OID is invalid */
	if (!RegProcedureIsValid(amhandler))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("table access method \"%s\" does not have a handler",
						NameStr(amform->amname))));

	ReleaseSysCache(tuple);

	/* And finally, call the handler function to get the API struct. */
	return GetTableAmRoutine(amhandler);
}
