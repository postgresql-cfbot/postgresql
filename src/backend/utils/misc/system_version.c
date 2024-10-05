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
 * Libraries reporting is implemented similarly via a hash table containing
 * only the library file path. This information is populated directly during
 * the initialization.
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
static HTAB	   *libraries = NULL;

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
 * Register versions that describe core components and do not correspond to any
 * individual component.
 */
void
register_core_versions()
{
	add_system_version("Core", core_get_version, CompileTime);
	add_system_version("Arch", core_get_arch, CompileTime);
	add_system_version("Compiler", core_get_compiler, CompileTime);
	add_system_version("ICU", icu_get_version, RunTime);
	add_system_version("Glibc", glibc_get_version, RunTime);
}

const char*
core_get_version(bool *available)
{
	*available = true;
	return (const char*) psprintf("%s", PG_VERSION);
}

const char*
core_get_arch(bool *available)
{
	*available = true;
	return (const char*) psprintf("%s", PG_ARCH_STR);
}

const char*
core_get_compiler(bool *available)
{
	*available = true;
	return (const char*) psprintf("%s", PG_CC_STR);
}

const char*
icu_get_version(bool *available)
{
#ifdef USE_ICU
	UVersionInfo UCDVersion;
	char* version = palloc0(U_MAX_VERSION_STRING_LENGTH);

	*available = true;
	u_getUnicodeVersion(UCDVersion);
	u_versionToString(UCDVersion, version);
	return (const char*) version;
#else
	*available = false;
	return (const char*) "";
#endif
}

const char*
glibc_get_version(bool *available)
{
	*available = true;
	return (const char*) gnu_get_libc_version();
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

/*
 * Walk through list of shared objects and populate the libraries hash table.
 */
void
register_libraries()
{
	HASHCTL		ctl;

	ctl.keysize = NAMEDATALEN;
	ctl.entrysize = sizeof(SystemLibrary);
	ctl.hcxt = CurrentMemoryContext;

	libraries = hash_create("Libraries table",
								   MAX_SYSTEM_LIBRARIES,
								   &ctl,
								   HASH_ELEM | HASH_STRINGS);

	dl_iterate_phdr(add_library, NULL);
}

int add_library(struct dl_phdr_info *info, size_t size, void *data)
{
	const char 		*key;
	bool 			found;

	if (strcmp(info->dlpi_name, "") == 0)
	{
		/* The first visited object is the main program with the empty name,
		 * which is not so interesting. */
		return 0;
	}

	key = pstrdup(info->dlpi_name);
	hash_search(libraries, key, HASH_ENTER, &found);

	if (found)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("duplicated library")));

	return 0;
}

/*
 * pg_get_libraries
 *
 * List information about shared objects.
 */
Datum
pg_get_system_libraries(PG_FUNCTION_ARGS)
{
#define PG_GET_SYS_LIBRARIES_COLS 1
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	HASH_SEQ_STATUS status;
	SystemLibrary *hentry;

	/* Build tuplestore to hold the result rows */
	InitMaterializedSRF(fcinfo, 0);

	if (!versions)
		return (Datum) 0;

	hash_seq_init(&status, libraries);
	while ((hentry = (SystemLibrary *) hash_seq_search(&status)) != NULL)
	{
		Datum		values[PG_GET_SYS_LIBRARIES_COLS] = {0};
		bool		nulls[PG_GET_SYS_LIBRARIES_COLS] = {0};

		values[0] = CStringGetTextDatum(hentry->filepath);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum) 0;
}
