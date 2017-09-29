/*----------------------------------------------------------------------
 *
 * storageamapi.c
 *		Support routines for API for Postgres storage access methods
 *
 * FIXME: looks like this should be in amapi.c.
 *
 * Copyright (c) 2016, PostgreSQL Global Development Group
 *
 * src/backend/access/heap/storageamapi.c
 *----------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/storageamapi.h"
#include "catalog/pg_am.h"
#include "catalog/pg_proc.h"
#include "utils/syscache.h"
#include "utils/memutils.h"


/*
 * GetStorageAmRoutine
 *		Call the specified access method handler routine to get its
 *		StorageAmRoutine struct, which will be palloc'd in the caller's
 *		memory context.
 */
StorageAmRoutine *
GetStorageAmRoutine(Oid amhandler)
{
	Datum		datum;
	StorageAmRoutine *routine;

	datum = OidFunctionCall0(amhandler);
	routine = (StorageAmRoutine *) DatumGetPointer(datum);

	if (routine == NULL || !IsA(routine, StorageAmRoutine))
		elog(ERROR, "storage access method handler %u did not return a StorageAmRoutine struct",
			 amhandler);

	return routine;
}

/* A crock */
StorageAmRoutine *
GetHeapamStorageAmRoutine(void)
{
	Datum datum;
	static StorageAmRoutine *HeapamStorageAmRoutine = NULL;

	if (HeapamStorageAmRoutine == NULL)
	{
		MemoryContext	oldcxt;

		oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		datum = OidFunctionCall0(HEAPAM_STORAGE_AM_HANDLER_OID);
		HeapamStorageAmRoutine = (StorageAmRoutine *) DatumGetPointer(datum);
		MemoryContextSwitchTo(oldcxt);
	}

	return HeapamStorageAmRoutine;
}

/*
 * GetStorageAmRoutineByAmId - look up the handler of the storage access
 * method with the given OID, and get its StorageAmRoutine struct.
 */
StorageAmRoutine *
GetStorageAmRoutineByAmId(Oid amoid)
{
	regproc     amhandler;
	HeapTuple   tuple;
	Form_pg_am  amform;

	/* Get handler function OID for the access method */
	tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(amoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for access method %u",
			 amoid);
	amform = (Form_pg_am) GETSTRUCT(tuple);

	/* Check that it is a storage access method */
	if (amform->amtype != AMTYPE_STORAGE)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("access method \"%s\" is not of type %s",
						NameStr(amform->amname), "STORAGE")));

	amhandler = amform->amhandler;

	/* Complain if handler OID is invalid */
	if (!RegProcedureIsValid(amhandler))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("storage access method \"%s\" does not have a handler",
						NameStr(amform->amname))));

	ReleaseSysCache(tuple);

	/* And finally, call the handler function to get the API struct. */
	return GetStorageAmRoutine(amhandler);
}
