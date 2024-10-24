/*------------------------------------------------------------------------
 *
 * system_version.c
 *	  Functions for reporting version of system components.
 *
 * A system component is defined very broadly here, it might be the PostgreSQL
 * core itself, the compiler, the host system, any dependency that is used at
 * compile time or run time.
 *
 * Version reporting is implemented via a hash table containing the component's
 * name as a key and the callback to fetch the version string. Every component
 * can register such a callback during initialization and is responsible for
 * exposing its own information. The idea is that storing a callback instead of
 * a version string directly allows for more flexibility about how and when the
 * information could be reported.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/system_version.c
 *
 *------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unicode/uchar.h>

#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/system_version.h"

static HTAB	   *versions = NULL;

void
add_system_version(const char* name, SystemVersionCB cb, VersionType type)
{
	SystemVersion   *hentry;
	const char 		*key;
	bool 			found;

	if (!versions)
	{
		HASHCTL		ctl;

		ctl.keysize = NAMEDATALEN;
		ctl.entrysize = sizeof(SystemVersion);
		ctl.hcxt = CurrentMemoryContext;

		versions = hash_create("System versions table",
									   MAX_SYSTEM_VERSIONS,
									   &ctl,
									   HASH_ELEM | HASH_STRINGS);
	}

	key = pstrdup(name);
	hentry = (SystemVersion *) hash_search(versions, key,
										   HASH_ENTER, &found);

	if (found)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("duplicated system version")));

	hentry->callback = cb;
	hentry->type = type;
}

/*
 * pg_get_system_versions
 *
 * List information about system versions.
 */
Datum
pg_get_system_versions(PG_FUNCTION_ARGS)
{
#define PG_GET_SYS_VERSIONS_COLS 3
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	HASH_SEQ_STATUS status;
	SystemVersion *hentry;

	/* Build tuplestore to hold the result rows */
	InitMaterializedSRF(fcinfo, 0);

	if (!versions)
		return (Datum) 0;

	hash_seq_init(&status, versions);
	while ((hentry = (SystemVersion *) hash_seq_search(&status)) != NULL)
	{
		Datum		values[PG_GET_SYS_VERSIONS_COLS] = {0};
		bool		nulls[PG_GET_SYS_VERSIONS_COLS] = {0};
		bool 		available = false;
		const char* version = hentry->callback(&available);

		if (!available)
			continue;

		values[0] = CStringGetTextDatum(hentry->name);
		values[1] = CStringGetTextDatum(version);
		values[2] = hentry->type;

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum) 0;
}
