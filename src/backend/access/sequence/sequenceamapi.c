/*-------------------------------------------------------------------------
 *
 * sequenceamapi.c
 *	  general sequence access method routines
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/sequence/sequenceamapi.c
 *
 *
 * Sequence access method allows the SQL Standard Sequence objects to be
 * managed according to either the default access method or a pluggable
 * replacement. Each sequence can only use one access method at a time,
 * though different sequence access methods can be in use by different
 * sequences at the same time.
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "access/sequenceam.h"
#include "catalog/pg_am.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/guc_hooks.h"
#include "utils/syscache.h"


/* GUC */
char	   *default_sequence_access_method = DEFAULT_SEQUENCE_ACCESS_METHOD;

/*
 * GetSequenceAmRoutine
 *		Call the specified access method handler routine to get its
 *		SequenceAmRoutine struct, which will be palloc'd in the caller's
 *		memory context.
 */
const SequenceAmRoutine *
GetSequenceAmRoutine(Oid amhandler)
{
	Datum		datum;
	SequenceAmRoutine *routine;

	datum = OidFunctionCall0(amhandler);
	routine = (SequenceAmRoutine *) DatumGetPointer(datum);

	if (routine == NULL || !IsA(routine, SequenceAmRoutine))
		elog(ERROR, "sequence access method handler %u did not return a SequenceAmRoutine struct",
			 amhandler);

	/*
	 * Assert that all required callbacks are present.  That makes it a bit
	 * easier to keep AMs up to date, e.g. when forward porting them to a new
	 * major version.
	 */
	Assert(routine->get_table_am != NULL);
	Assert(routine->init != NULL);
	Assert(routine->nextval != NULL);
	Assert(routine->setval != NULL);
	Assert(routine->reset != NULL);
	Assert(routine->get_state != NULL);
	Assert(routine->change_persistence != NULL);

	return routine;
}

/*
 * GetSequenceAmRoutineId
 *		Call pg_am and retrieve the OID of the access method handler.
 */
Oid
GetSequenceAmRoutineId(Oid amoid)
{
	Oid			amhandleroid;
	HeapTuple	tuple;
	Form_pg_am	aform;

	tuple = SearchSysCache1(AMOID,
							ObjectIdGetDatum(amoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for access method %u", amoid);
	aform = (Form_pg_am) GETSTRUCT(tuple);
	Assert(aform->amtype == AMTYPE_SEQUENCE);
	amhandleroid = aform->amhandler;
	ReleaseSysCache(tuple);

	return amhandleroid;
}

/* check_hook: validate new default_sequence_access_method */
bool
check_default_sequence_access_method(char **newval, void **extra,
									 GucSource source)
{
	if (**newval == '\0')
	{
		GUC_check_errdetail("%s cannot be empty.",
							"default_sequence_access_method");
		return false;
	}

	if (strlen(*newval) >= NAMEDATALEN)
	{
		GUC_check_errdetail("%s is too long (maximum %d characters).",
							"default_sequence_access_method", NAMEDATALEN - 1);
		return false;
	}

	/*
	 * If we aren't inside a transaction, or not connected to a database, we
	 * cannot do the catalog access necessary to verify the method.  Must
	 * accept the value on faith.
	 */
	if (IsTransactionState() && MyDatabaseId != InvalidOid)
	{
		if (!OidIsValid(get_sequence_am_oid(*newval, true)))
		{
			/*
			 * When source == PGC_S_TEST, don't throw a hard error for a
			 * nonexistent sequence access method, only a NOTICE. See comments
			 * in guc.h.
			 */
			if (source == PGC_S_TEST)
			{
				ereport(NOTICE,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("sequence access method \"%s\" does not exist",
								*newval)));
			}
			else
			{
				GUC_check_errdetail("sequence access method \"%s\" does not exist.",
									*newval);
				return false;
			}
		}
	}

	return true;
}
