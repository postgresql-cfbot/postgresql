/*-------------------------------------------------------------------------
 *
 * injection_point.c
 *	  Routines to control and run injection points in the code.
 *
 * Injection points can be used to call arbitrary callbacks in specific
 * places of the code, registering callbacks that would be run in the code
 * paths where a named injection point exists.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/injection_point.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>

#include "fmgr.h"
#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"

#ifdef USE_INJECTION_POINTS

/*
 * Hash table for storing injection points.
 *
 * InjectionPointHash is used to find an injection point by name.
 */
static HTAB *InjectionPointHash;	/* find points from names */

/* Field sizes */
#define INJ_NAME_MAXLEN		64
#define INJ_LIB_MAXLEN		128
#define INJ_FUNC_MAXLEN		128

typedef struct InjectionPointEntry
{
	char		name[INJ_NAME_MAXLEN];	/* hash key */
	char		library[INJ_LIB_MAXLEN];	/* library */
	char		function[INJ_FUNC_MAXLEN];	/* function */
} InjectionPointEntry;

#define INJECTION_POINT_HASH_INIT_SIZE	16
#define INJECTION_POINT_HASH_MAX_SIZE	128

/*
 * Local cache of injection callbacks already loaded, stored in
 * TopMemoryContext.
 */
typedef struct InjectionPointArrayEntry
{
	char		name[INJ_NAME_MAXLEN];
	InjectionPointCallback callback;
} InjectionPointCacheEntry;

static HTAB *InjectionPointCache = NULL;

/* utilities to handle the local array cache */
static void
injection_point_cache_add(const char *name,
						  InjectionPointCallback callback)
{
	InjectionPointCacheEntry *entry;
	bool		found;

	/* If first time, initialize */
	if (InjectionPointCache == NULL)
	{
		HASHCTL		hash_ctl;

		hash_ctl.keysize = sizeof(char[INJ_NAME_MAXLEN]);
		hash_ctl.entrysize = sizeof(InjectionPointCacheEntry);
		hash_ctl.hcxt = TopMemoryContext;

		InjectionPointCache = hash_create("InjectionPoint cache hash",
										  INJECTION_POINT_HASH_MAX_SIZE,
										  &hash_ctl,
										  HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
	}

	entry = (InjectionPointCacheEntry *)
		hash_search(InjectionPointCache, name, HASH_ENTER, &found);

	if (!found)
	{
		memcpy(entry->name, name, strlen(name));
		entry->callback = callback;
	}
}

/*
 * Remove entry from the local cache.  Note that this leaks a callback
 * loaded but removed later on, which should have no consequence from
 * a testing perspective.
 */
static void
injection_point_cache_remove(const char *name)
{
	/* Leave if no cache */
	if (InjectionPointCache == NULL)
		return;

	(void) hash_search(InjectionPointCache, name, HASH_REMOVE, NULL);
}

static InjectionPointCallback
injection_point_cache_get(const char *name)
{
	bool		found;
	InjectionPointCacheEntry *entry;

	/* no callback if no cache yet */
	if (InjectionPointCache == NULL)
		return NULL;

	entry = (InjectionPointCacheEntry *)
		hash_search(InjectionPointCache, name, HASH_FIND, &found);

	if (found)
		return entry->callback;

	return NULL;
}
#endif							/* USE_INJECTION_POINTS */

/*
 * Return the space for dynamic shared hash table.
 */
Size
InjectionPointShmemSize(void)
{
#ifdef USE_INJECTION_POINTS
	Size		sz = 0;

	sz = add_size(sz, hash_estimate_size(INJECTION_POINT_HASH_MAX_SIZE,
										 sizeof(InjectionPointEntry)));
	return sz;
#else
	return 0;
#endif
}

/*
 * Allocate shmem space for dynamic shared hash.
 */
void
InjectionPointShmemInit(void)
{
#ifdef USE_INJECTION_POINTS
	HASHCTL		info;

	/* key is a NULL-terminated string */
	info.keysize = sizeof(char[INJ_NAME_MAXLEN]);
	info.entrysize = sizeof(InjectionPointEntry);
	InjectionPointHash = ShmemInitHash("InjectionPoint hash",
									   INJECTION_POINT_HASH_INIT_SIZE,
									   INJECTION_POINT_HASH_MAX_SIZE,
									   &info,
									   HASH_ELEM | HASH_STRINGS);
#endif
}

#ifdef USE_INJECTION_POINTS
static bool
file_exists(const char *name)
{
	struct stat st;

	Assert(name != NULL);
	if (stat(name, &st) == 0)
		return !S_ISDIR(st.st_mode);
	else if (!(errno == ENOENT || errno == ENOTDIR))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not access file \"%s\": %m", name)));
	return false;
}
#endif

/*
 * Attach a new injection point.
 */
void
InjectionPointAttach(const char *name,
					 const char *library,
					 const char *function)
{
#ifdef USE_INJECTION_POINTS
	InjectionPointEntry *entry_by_name;
	bool		found;

	if (strlen(name) >= INJ_NAME_MAXLEN)
		elog(ERROR, "injection point name %s too long", name);
	if (strlen(library) >= INJ_LIB_MAXLEN)
		elog(ERROR, "injection point library %s too long", library);
	if (strlen(function) >= INJ_FUNC_MAXLEN)
		elog(ERROR, "injection point function %s too long", function);

	/*
	 * Allocate and register a new injection point.  A new point should not
	 * exist.  For testing purposes this should be fine.
	 */
	LWLockAcquire(InjectionPointLock, LW_EXCLUSIVE);
	entry_by_name = (InjectionPointEntry *)
		hash_search(InjectionPointHash, name,
					HASH_ENTER, &found);
	if (found)
	{
		LWLockRelease(InjectionPointLock);
		elog(ERROR, "injection point \"%s\" already defined", name);
	}

	/* Save the entry */
	memcpy(entry_by_name->name, name, sizeof(entry_by_name->name));
	entry_by_name->name[INJ_NAME_MAXLEN - 1] = '\0';
	memcpy(entry_by_name->library, library, sizeof(entry_by_name->library));
	entry_by_name->library[INJ_LIB_MAXLEN - 1] = '\0';
	memcpy(entry_by_name->function, function, sizeof(entry_by_name->function));
	entry_by_name->function[INJ_FUNC_MAXLEN - 1] = '\0';

	LWLockRelease(InjectionPointLock);

#else
	elog(ERROR, "Injection points are not supported by this build");
#endif
}

/*
 * Detach an existing injection point.
 */
void
InjectionPointDetach(const char *name)
{
#ifdef USE_INJECTION_POINTS
	bool		found;

	LWLockAcquire(InjectionPointLock, LW_EXCLUSIVE);
	hash_search(InjectionPointHash, name, HASH_REMOVE, &found);
	LWLockRelease(InjectionPointLock);

	if (!found)
		elog(ERROR, "injection point \"%s\" not found", name);

#else
	elog(ERROR, "Injection points are not supported by this build");
#endif
}

/*
 * Execute an injection point, if defined.
 *
 * Check first the shared hash table, and adapt the local cache
 * depending on that as it could be possible that an entry to run
 * has been removed.
 */
void
InjectionPointRun(const char *name)
{
#ifdef USE_INJECTION_POINTS
	InjectionPointEntry *entry_by_name;
	bool		found;
	InjectionPointCallback injection_callback;

	LWLockAcquire(InjectionPointLock, LW_SHARED);
	entry_by_name = (InjectionPointEntry *)
		hash_search(InjectionPointHash, name,
					HASH_FIND, &found);
	LWLockRelease(InjectionPointLock);

	/*
	 * If not found, do nothing and remove it from the local cache if it
	 * existed there.
	 */
	if (!found)
	{
		injection_point_cache_remove(name);
		return;
	}

	/*
	 * Check if the callback exists in the local cache, to avoid unnecessary
	 * external loads.
	 */
	injection_callback = injection_point_cache_get(name);
	if (injection_callback == NULL)
	{
		char		path[MAXPGPATH];

		/* Found, so just run the callback registered */
		snprintf(path, MAXPGPATH, "%s/%s%s", pkglib_path,
				 entry_by_name->library, DLSUFFIX);

		if (!file_exists(path))
			elog(ERROR, "could not find injection library \"%s\"", path);

		injection_callback = (InjectionPointCallback)
			load_external_function(path, entry_by_name->function, true, NULL);

		/* add it to the local cache when found */
		injection_point_cache_add(name, injection_callback);
	}

	injection_callback(name);
#else
	elog(ERROR, "Injection points are not supported by this build");
#endif
}
