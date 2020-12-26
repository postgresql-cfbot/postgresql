/*-------------------------------------------------------------------------
 *
 * compression/compressamapi.c
 *	  Functions for compression methods
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/compression/compressamapi.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/compressamapi.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "catalog/pg_am.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"

/*
 * CompressionOidToId - Convert compression Oid to built-in compression id.
 *
 * For more details refer comment atop CompressionId in compressamapi.h
 */
CompressionId
CompressionOidToId(Oid cmoid)
{
	switch (cmoid)
	{
		case PGLZ_COMPRESSION_AM_OID:
			return PGLZ_COMPRESSION_ID;
		case LZ4_COMPRESSION_AM_OID:
			return LZ4_COMPRESSION_ID;
		default:
			elog(ERROR, "Invalid compression method oid %u", cmoid);
	}
}

/*
 * CompressionIdToOid - Convert built-in compression id to Oid
 *
 * For more details refer comment atop CompressionId in compressamapi.h
 */
Oid
CompressionIdToOid(CompressionId cmid)
{
	switch (cmid)
	{
		case PGLZ_COMPRESSION_ID:
			return PGLZ_COMPRESSION_AM_OID;
		case LZ4_COMPRESSION_ID:
			return LZ4_COMPRESSION_AM_OID;
		default:
			elog(ERROR, "Invalid compression method id %d", cmid);
	}
}

/*
 * GetCompressionAmRoutineByAmId - look up the handler of the compression access
 * method with the given OID, and get its CompressionAmRoutine struct.
 */
CompressionAmRoutine *
GetCompressionAmRoutineByAmId(Oid amoid)
{
	HeapTuple	tuple;
	Form_pg_am	amform;
	regproc		amhandler;
	Datum		datum;
	CompressionAmRoutine *routine;

	/* Get handler function OID for the access method */
	tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(amoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for access method %u",
			 amoid);

	amform = (Form_pg_am)GETSTRUCT(tuple);

	/* Check if it's an index access method as opposed to some other AM */
	if (amform->amtype != AMTYPE_COMPRESSION)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("access method \"%s\" is not of type %s",
						NameStr(amform->amname), "INDEX")));

	amhandler = amform->amhandler;

	/* Complain if handler OID is invalid */
	if (!RegProcedureIsValid(amhandler))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("index access method \"%s\" does not have a handler",
						NameStr(amform->amname))));

	ReleaseSysCache(tuple);

	/* And finally, call the handler function to get the API struct */
	datum = OidFunctionCall0(amhandler);
	routine = (CompressionAmRoutine *) DatumGetPointer(datum);

	return routine;
}
