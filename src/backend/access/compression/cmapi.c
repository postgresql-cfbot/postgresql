/*-------------------------------------------------------------------------
 *
 * compression/cmapi.c
 *	  Functions for compression access methods
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/compression/cmapi.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "access/cmapi.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "catalog/pg_am.h"
#include "catalog/pg_attr_compression.h"
#include "commands/defrem.h"
#include "utils/syscache.h"

/*
 * InvokeCompressionAmHandler - call the specified access method handler routine to get
 * its CompressionAmRoutine struct, which will be palloc'd in the caller's context.
 */
CompressionAmRoutine *
InvokeCompressionAmHandler(Oid amhandler)
{
	Datum		datum;
	CompressionAmRoutine *routine;

	datum = OidFunctionCall0(amhandler);
	routine = (CompressionAmRoutine *) DatumGetPointer(datum);

	if (routine == NULL || !IsA(routine, CompressionAmRoutine))
		elog(ERROR, "compression method handler function %u "
			 "did not return an CompressionAmRoutine struct",
			 amhandler);

	return routine;
}

/*
 * GetCompressionAmRoutine
 *
 * Return CompressionAmRoutine by attribute compression Oid
 */
CompressionAmRoutine *
GetCompressionAmRoutine(Oid acoid)
{
	Oid			amoid;
	regproc		amhandler;
	HeapTuple	tuple;
	Form_pg_attr_compression acform;

	tuple = SearchSysCache1(ATTCOMPRESSIONOID, ObjectIdGetDatum(acoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for attribute compression %u", acoid);

	/* extract access method name and get handler for it */
	acform = (Form_pg_attr_compression) GETSTRUCT(tuple);
	amoid = get_compression_am_oid(NameStr(acform->acname), false);
	amhandler = get_am_handler_oid(amoid, AMTYPE_COMPRESSION, false);
	ReleaseSysCache(tuple);

	/* And finally, call the handler function to get the API struct. */
	return InvokeCompressionAmHandler(amhandler);
}

/*
 * GetAttrCompressionOptions
 *
 * Parse array of attribute compression options and return it as a list.
 */
List *
GetAttrCompressionOptions(Oid acoid)
{
	HeapTuple	tuple;
	List	   *result = NIL;
	bool		isnull;
	Datum		acoptions;

	tuple = SearchSysCache1(ATTCOMPRESSIONOID, ObjectIdGetDatum(acoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for an attribute compression %u", acoid);

	/* options could be NULL, so we can't use form struct */
	acoptions = SysCacheGetAttr(ATTCOMPRESSIONOID, tuple,
								Anum_pg_attr_compression_acoptions, &isnull);

	if (!isnull)
		result = untransformRelOptions(acoptions);

	ReleaseSysCache(tuple);
	return result;
}

/*
 * GetAttrCompressionAmOid
 *
 * Return access method Oid by attribute compression Oid
 */
Oid
GetAttrCompressionAmOid(Oid acoid)
{
	Oid			result;
	HeapTuple	tuple;
	Form_pg_attr_compression acform;

	/* extract access method Oid */
	tuple = SearchSysCache1(ATTCOMPRESSIONOID, ObjectIdGetDatum(acoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for attribute compression %u", acoid);

	acform = (Form_pg_attr_compression) GETSTRUCT(tuple);
	result = get_compression_am_oid(NameStr(acform->acname), false);
	ReleaseSysCache(tuple);

	return result;
}
