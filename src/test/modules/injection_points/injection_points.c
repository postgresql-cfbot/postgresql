/*--------------------------------------------------------------------------
 *
 * injection_points.c
 *		Code for testing injection points.
 *
 * Injection points are able to trigger user-defined callbacks in pre-defined
 * code paths.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/test/modules/injection_points/injection_points.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#ifndef WIN32
#include <sys/mman.h>
#endif

#include "fmgr.h"
#include "funcapi.h"
#include "injection_points.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"
#include "utils/tuplestore.h"
#include "utils/wait_event.h"

PG_MODULE_MAGIC;

/*
 * List of injection points stored in TopMemoryContext attached
 * locally to this process.
 */
static List *inj_list_local = NIL;

/*
 * Shared state information for injection points.
 *
 * This is mapped from a fixed file in the data directory (INJ_STATE_FILE)
 * rather than being allocated in the main shared memory segment or a DSM.
 * Backing it with a file lets external programs without a backend connection
 * map the same state, observe which injection points are being waited on and
 * release them (see injection_points_state.c).  This works in contexts where
 * condition variables and latches are unavailable, e.g. the postmaster or a
 * process that has not set up its PGPROC yet.
 *
 * The leading InjectionPointPublicState portion is the contract shared with
 * those external tools, so it must stay first and keep a frontend-compatible
 * layout (see injection_points.h).  The fields below it are backend-only.
 */
typedef struct InjectionPointSharedState
{
	/* Names of injection points attached to wait counters (slot in use). */
	char		name[INJ_MAX_WAIT][INJ_NAME_MAXLEN];

	/* Counters advancing when injection_points_wakeup() is called */
	pg_atomic_uint32 wait_counts[INJ_MAX_WAIT];

	/* Protects access to the name array (backend-only) */
	slock_t		lock;
} InjectionPointSharedState;

/* The public prefix must match InjectionPointPublicState bit for bit. */
StaticAssertDecl(offsetof(InjectionPointSharedState, name) == 0,
				 "name must be the first field of InjectionPointSharedState");
StaticAssertDecl(offsetof(InjectionPointSharedState, wait_counts) ==
				 offsetof(InjectionPointPublicState, wait_counts),
				 "wait_counts offset must match InjectionPointPublicState");
StaticAssertDecl(sizeof(pg_atomic_uint32) == sizeof(uint32),
				 "pg_atomic_uint32 must be layout-compatible with uint32");

/* Pointer to the mapped shared state. */
static InjectionPointSharedState *inj_state = NULL;

extern PGDLLEXPORT void injection_error(const char *name,
										const void *private_data,
										void *arg);
extern PGDLLEXPORT void injection_notice(const char *name,
										 const void *private_data,
										 void *arg);
extern PGDLLEXPORT void injection_wait(const char *name,
									   const void *private_data,
									   void *arg);

/* track if injection points attached in this process are linked to it */
static bool injection_point_local = false;

/* How injection_map_state() should open the backing file. */
typedef enum InjectionMapMode
{
	INJ_MAP_CREATE,				/* discard any stale file, create fresh */
	INJ_MAP_ATTACH,				/* map an already-existing file */
	INJ_MAP_ATTACH_OR_CREATE,	/* attach if present, else create */
} InjectionMapMode;

static void injection_shmem_init(void *arg);
static void injection_shmem_attach(void *arg);

static const ShmemCallbacks injection_shmem_callbacks = {
	/* Create and initialize the backing file once at postmaster startup. */
	.init_fn = injection_shmem_init,
	/* Re-map it in each child that does not inherit the mapping (Windows). */
	.attach_fn = injection_shmem_attach,
};

/*
 * Initialize a freshly-created shared state.
 */
static void
injection_point_init_state(InjectionPointSharedState *state)
{
	SpinLockInit(&state->lock);
	memset(state->name, 0, sizeof(state->name));
	for (int i = 0; i < INJ_MAX_WAIT; i++)
		pg_atomic_init_u32(&state->wait_counts[i], 0);
}

/*
 * Resolve the absolute path of the backing file.  Anchored at the data
 * directory rather than the current working directory: EXEC_BACKEND children
 * re-map the file from attach_fn before they chdir into the data directory, so
 * a relative path would resolve against the wrong directory there.  This also
 * matches the absolute path that out-of-process tools build from the data
 * directory they are given.
 */
static const char *
injection_state_file_path(void)
{
	static char path[MAXPGPATH];

	if (path[0] != '\0')
		return path;

	if (DataDir != NULL && DataDir[0] != '\0')
		snprintf(path, sizeof(path), "%s/%s", DataDir, INJ_STATE_FILE);
	else
		strlcpy(path, INJ_STATE_FILE, sizeof(path));

	return path;
}

/*
 * proc_exit callback removing the backing file.  Registered only by the
 * process that created it (the postmaster, or a lone backend when the module
 * is not preloaded), so that the file disappears together with the cluster.
 */
static void
injection_state_file_cleanup(int code, Datum arg)
{
	if (inj_state != NULL)
	{
#ifndef WIN32
		munmap(inj_state, sizeof(InjectionPointSharedState));
#else
		UnmapViewOfFile(inj_state);
#endif
		inj_state = NULL;
	}

	/*
	 * Only the postmaster (or a standalone backend) should unlink the file;
	 * forked children inherit this callback but must not remove it.
	 */
	if (!IsUnderPostmaster)
		(void) unlink(injection_state_file_path());
}

#ifndef WIN32
/*
 * Wait for a file created by a concurrent process to reach its final size.
 *
 * The winner of the O_EXCL create race makes the file at length zero and only
 * then ftruncate()s it to "size".  A process that lost the race and is
 * attaching could otherwise mmap() past end-of-file and take a SIGBUS on first
 * access.  The gap is just the few instructions between create and ftruncate,
 * so in practice this returns on the first check.
 */
static void
injection_wait_for_size(int fd, Size size, const char *path, int elevel)
{
	/* Generous bound; the writer needs only microseconds. */
	for (int i = 0; i < 10000; i++)
	{
		struct stat st;

		if (fstat(fd, &st) != 0)
		{
			ereport(elevel,
					(errcode_for_file_access(),
					 errmsg("could not stat injection point state file \"%s\": %m",
							path)));
			return;
		}
		if (st.st_size >= (off_t) size)
			return;
		pg_usleep(1000L);		/* 1ms */
	}

	ereport(elevel,
			(errmsg("injection point state file \"%s\" was never sized by its creator",
					path)));
}
#endif

/*
 * Map INJ_STATE_FILE into this process, creating and/or initializing it as
 * dictated by "mode", and set inj_state.
 *
 * The state is backed by an ordinary file so that external programs can map
 * the same bytes (POSIX mmap or, on Windows, a file-backed CreateFileMapping).
 * All accessors must use the mapping, never plain file reads, because file
 * I/O is not guaranteed to be coherent with a mapped view on Windows.
 */
static void
injection_map_state(InjectionMapMode mode, int elevel)
{
	Size		size = sizeof(InjectionPointSharedState);
	const char *path = injection_state_file_path();
	bool		created = false;

	if (inj_state != NULL)
		return;

	if (mode == INJ_MAP_CREATE)
		(void) unlink(path);	/* drop any stale file from a crash */

#ifndef WIN32
	{
		int			fd;
		int			oflags = O_RDWR;

		if (mode != INJ_MAP_ATTACH)
			oflags |= O_CREAT | O_EXCL;

		fd = OpenTransientFile(path, oflags);
		if (fd < 0 && mode == INJ_MAP_ATTACH_OR_CREATE && errno == EEXIST)
		{
			/* Lost the race to create it; just attach. */
			oflags = O_RDWR;
			fd = OpenTransientFile(path, oflags);

			/*
			 * The winner may not have ftruncate()d the file to full size yet;
			 * wait for it so the mmap() below cannot fault past end-of-file.
			 */
			if (fd >= 0)
				injection_wait_for_size(fd, size, path, elevel);
		}
		else if (fd >= 0 && (oflags & O_CREAT))
			created = true;

		if (fd < 0)
			ereport(elevel,
					(errcode_for_file_access(),
					 errmsg("could not open injection point state file \"%s\": %m",
							path)));

		if (created && ftruncate(fd, size) != 0)
		{
			CloseTransientFile(fd);
			(void) unlink(path);
			ereport(elevel,
					(errcode_for_file_access(),
					 errmsg("could not size injection point state file \"%s\": %m",
							path)));
		}

		inj_state = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
		CloseTransientFile(fd);

		if (inj_state == MAP_FAILED)
		{
			inj_state = NULL;
			ereport(elevel,
					(errcode_for_file_access(),
					 errmsg("could not map injection point state file \"%s\": %m",
							path)));
		}
	}
#else
	{
		HANDLE		hfile;
		HANDLE		hmap;
		DWORD		share = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
		DWORD		disp = (mode == INJ_MAP_ATTACH) ? OPEN_EXISTING : CREATE_NEW;

		hfile = CreateFile(path,
						   GENERIC_READ | GENERIC_WRITE,
						   share, NULL, disp, FILE_ATTRIBUTE_NORMAL, NULL);

		/*
		 * If creation failed because a file from an earlier cluster lifetime
		 * is in the way, attach to it instead of failing.  Don't insist on a
		 * specific error code: besides ERROR_FILE_EXISTS, a file whose
		 * deletion is still pending reports ERROR_ACCESS_DENIED.
		 * FILE_SHARE_DELETE (above) lets such a file be reopened and unlinked
		 * while still mapped.
		 */
		if (hfile == INVALID_HANDLE_VALUE && mode == INJ_MAP_ATTACH_OR_CREATE)
		{
			disp = OPEN_EXISTING;
			hfile = CreateFile(path,
							   GENERIC_READ | GENERIC_WRITE,
							   share, NULL, disp, FILE_ATTRIBUTE_NORMAL, NULL);
		}
		else if (hfile != INVALID_HANDLE_VALUE && disp == CREATE_NEW)
			created = true;

		if (hfile == INVALID_HANDLE_VALUE)
			ereport(elevel,
					(errmsg("could not open injection point state file \"%s\": error code %lu",
							path, GetLastError())));

		/* CreateFileMapping extends the backing file to the mapping size. */
		hmap = CreateFileMapping(hfile, NULL, PAGE_READWRITE, 0, (DWORD) size, NULL);
		if (hmap == NULL)
		{
			CloseHandle(hfile);
			ereport(elevel,
					(errmsg("could not create mapping for injection point state file \"%s\": error code %lu",
							path, GetLastError())));
		}

		inj_state = MapViewOfFile(hmap, FILE_MAP_ALL_ACCESS, 0, 0, size);
		CloseHandle(hmap);
		CloseHandle(hfile);

		if (inj_state == NULL)
			ereport(elevel,
					(errmsg("could not map injection point state file \"%s\": error code %lu",
							path, GetLastError())));
	}
#endif

	if (created)
	{
		injection_point_init_state(inj_state);
		on_proc_exit(injection_state_file_cleanup, 0);
	}
}

/*
 * Shared memory callbacks.  We do not request any space in the main segment;
 * the file mapping is the source of truth.  init_fn runs once in the
 * postmaster (children inherit the mapping through fork), while attach_fn
 * re-maps the file in children that do not inherit it (EXEC_BACKEND/Windows).
 */
static void
injection_shmem_init(void *arg)
{
	injection_map_state(INJ_MAP_CREATE, FATAL);
}

static void
injection_shmem_attach(void *arg)
{
	injection_map_state(INJ_MAP_ATTACH, FATAL);
}

/*
 * Ensure inj_state is available in the current process.
 *
 * Backends preloading the module inherit (fork) or re-map (EXEC_BACKEND) the
 * state set up at postmaster startup.  When the module is not preloaded, the
 * first process to reach here creates the file and the rest attach to it.
 */
static void
injection_init_shmem(void)
{
	if (inj_state != NULL)
		return;

	injection_map_state(INJ_MAP_ATTACH_OR_CREATE, ERROR);
}

/*
 * Check runtime conditions associated to an injection point.
 *
 * Returns true if the named injection point is allowed to run, and false
 * otherwise.
 */
static bool
injection_point_allowed(const InjectionPointCondition *condition)
{
	bool		result = true;

	switch (condition->type)
	{
		case INJ_CONDITION_PID:
			if (MyProcPid != condition->pid)
				result = false;
			break;
		case INJ_CONDITION_ALWAYS:
			break;
	}

	return result;
}

/*
 * before_shmem_exit callback to remove injection points linked to a
 * specific process.
 */
static void
injection_points_cleanup(int code, Datum arg)
{
	ListCell   *lc;

	/* Leave if nothing is tracked locally */
	if (!injection_point_local)
		return;

	/* Detach all the local points */
	foreach(lc, inj_list_local)
	{
		char	   *name = strVal(lfirst(lc));

		(void) InjectionPointDetach(name);
	}
}

/* Set of callbacks available to be attached to an injection point. */
void
injection_error(const char *name, const void *private_data, void *arg)
{
	const InjectionPointCondition *condition = private_data;
	char	   *argstr = arg;

	if (!injection_point_allowed(condition))
		return;

	if (argstr)
		elog(ERROR, "error triggered for injection point %s (%s)",
			 name, argstr);
	else
		elog(ERROR, "error triggered for injection point %s", name);
}

void
injection_notice(const char *name, const void *private_data, void *arg)
{
	const InjectionPointCondition *condition = private_data;
	char	   *argstr = arg;

	if (!injection_point_allowed(condition))
		return;

	if (argstr)
		elog(NOTICE, "notice triggered for injection point %s (%s)",
			 name, argstr);
	else
		elog(NOTICE, "notice triggered for injection point %s", name);
}

/* Wait until injection_points_wakeup() is called */
void
injection_wait(const char *name, const void *private_data, void *arg)
{
	uint32		old_wait_counts = 0;
	int			index = -1;
	uint32		injection_wait_event = 0;
	const InjectionPointCondition *condition = private_data;

	if (inj_state == NULL)
		injection_init_shmem();

	if (!injection_point_allowed(condition))
		return;

	/*
	 * Use the injection point name for this custom wait event.  Note that
	 * this custom wait event name is not released, but we don't care much for
	 * testing as this should be short-lived.
	 */
	injection_wait_event = WaitEventInjectionPointNew(name);

	/*
	 * Find a free slot to wait for, and register this injection point's name.
	 */
	SpinLockAcquire(&inj_state->lock);
	for (int i = 0; i < INJ_MAX_WAIT; i++)
	{
		if (inj_state->name[i][0] == '\0')
		{
			index = i;
			strlcpy(inj_state->name[i], name, INJ_NAME_MAXLEN);
			old_wait_counts = pg_atomic_read_u32(&inj_state->wait_counts[i]);
			break;
		}
	}
	SpinLockRelease(&inj_state->lock);

	if (index < 0)
		elog(ERROR, "could not find free slot for wait of injection point %s",
			 name);

	/*
	 * Wait until the counter is bumped by injection_points_wakeup().
	 *
	 * This loop starts with a short delay for responsiveness, enlarged to
	 * ease the CPU workload in slower environments.
	 */
#define INJ_WAIT_INITIAL_US		10	/* 10us */
#define INJ_WAIT_MAX_US			100000	/* 100ms */
	pgstat_report_wait_start(injection_wait_event);
	{
		int			delay_us = INJ_WAIT_INITIAL_US;

		while (pg_atomic_read_u32(&inj_state->wait_counts[index]) == old_wait_counts)
		{
			CHECK_FOR_INTERRUPTS();
			pg_usleep(delay_us);
			if (delay_us < INJ_WAIT_MAX_US)
				delay_us *= 2;
		}
	}
	pgstat_report_wait_end();

	/* Remove this injection point from the waiters. */
	SpinLockAcquire(&inj_state->lock);
	inj_state->name[index][0] = '\0';
	SpinLockRelease(&inj_state->lock);
}

/*
 * SQL function for creating an injection point.
 */
PG_FUNCTION_INFO_V1(injection_points_attach);
Datum
injection_points_attach(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *action = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char	   *function;
	InjectionPointCondition condition = {0};

	if (strcmp(action, "error") == 0)
		function = "injection_error";
	else if (strcmp(action, "notice") == 0)
		function = "injection_notice";
	else if (strcmp(action, "wait") == 0)
		function = "injection_wait";
	else
		elog(ERROR, "incorrect action \"%s\" for injection point creation", action);

	if (injection_point_local)
	{
		condition.type = INJ_CONDITION_PID;
		condition.pid = MyProcPid;
	}

	InjectionPointAttach(name, "injection_points", function, &condition,
						 sizeof(InjectionPointCondition));

	if (injection_point_local)
	{
		MemoryContext oldctx;

		/* Local injection point, so track it for automated cleanup */
		oldctx = MemoryContextSwitchTo(TopMemoryContext);
		inj_list_local = lappend(inj_list_local, makeString(pstrdup(name)));
		MemoryContextSwitchTo(oldctx);
	}

	PG_RETURN_VOID();
}

/*
 * SQL function for creating an injection point with library name, function
 * name and private data.
 */
PG_FUNCTION_INFO_V1(injection_points_attach_func);
Datum
injection_points_attach_func(PG_FUNCTION_ARGS)
{
	char	   *name;
	char	   *lib_name;
	char	   *function;
	bytea	   *private_data = NULL;
	int			private_data_size = 0;

	if (PG_ARGISNULL(0))
		elog(ERROR, "injection point name cannot be NULL");
	if (PG_ARGISNULL(1))
		elog(ERROR, "injection point library cannot be NULL");
	if (PG_ARGISNULL(2))
		elog(ERROR, "injection point function cannot be NULL");

	name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	lib_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
	function = text_to_cstring(PG_GETARG_TEXT_PP(2));

	if (!PG_ARGISNULL(3))
	{
		private_data = PG_GETARG_BYTEA_PP(3);
		private_data_size = VARSIZE_ANY_EXHDR(private_data);
	}

	if (private_data != NULL)
		InjectionPointAttach(name, lib_name, function, VARDATA_ANY(private_data),
							 private_data_size);
	else
		InjectionPointAttach(name, lib_name, function, NULL,
							 0);
	PG_RETURN_VOID();
}

/*
 * SQL function for loading an injection point.
 */
PG_FUNCTION_INFO_V1(injection_points_load);
Datum
injection_points_load(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (inj_state == NULL)
		injection_init_shmem();

	INJECTION_POINT_LOAD(name);

	PG_RETURN_VOID();
}

/*
 * SQL function for triggering an injection point.
 */
PG_FUNCTION_INFO_V1(injection_points_run);
Datum
injection_points_run(PG_FUNCTION_ARGS)
{
	char	   *name;
	char	   *arg = NULL;

	if (PG_ARGISNULL(0))
		PG_RETURN_VOID();
	name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (!PG_ARGISNULL(1))
		arg = text_to_cstring(PG_GETARG_TEXT_PP(1));

	INJECTION_POINT(name, arg);

	PG_RETURN_VOID();
}

/*
 * SQL function for triggering an injection point from cache.
 */
PG_FUNCTION_INFO_V1(injection_points_cached);
Datum
injection_points_cached(PG_FUNCTION_ARGS)
{
	char	   *name;
	char	   *arg = NULL;

	if (PG_ARGISNULL(0))
		PG_RETURN_VOID();
	name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (!PG_ARGISNULL(1))
		arg = text_to_cstring(PG_GETARG_TEXT_PP(1));

	INJECTION_POINT_CACHED(name, arg);

	PG_RETURN_VOID();
}

/*
 * SQL function for waking up an injection point waiting in injection_wait().
 */
PG_FUNCTION_INFO_V1(injection_points_wakeup);
Datum
injection_points_wakeup(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int			index = -1;

	if (inj_state == NULL)
		injection_init_shmem();

	/* Find the injection point then bump its wait counter */
	SpinLockAcquire(&inj_state->lock);
	for (int i = 0; i < INJ_MAX_WAIT; i++)
	{
		if (strcmp(name, inj_state->name[i]) == 0)
		{
			index = i;
			break;
		}
	}
	if (index < 0)
	{
		SpinLockRelease(&inj_state->lock);
		elog(ERROR, "could not find injection point %s to wake up", name);
	}
	SpinLockRelease(&inj_state->lock);

	pg_atomic_fetch_add_u32(&inj_state->wait_counts[index], 1);
	PG_RETURN_VOID();
}

/*
 * injection_points_set_local
 *
 * Track if any injection point created in this process ought to run only
 * in this process.  Such injection points are detached automatically when
 * this process exits.  This is useful to make test suites concurrent-safe.
 */
PG_FUNCTION_INFO_V1(injection_points_set_local);
Datum
injection_points_set_local(PG_FUNCTION_ARGS)
{
	/* Enable flag to add a runtime condition based on this process ID */
	injection_point_local = true;

	if (inj_state == NULL)
		injection_init_shmem();

	/*
	 * Register a before_shmem_exit callback to remove any injection points
	 * linked to this process.
	 */
	before_shmem_exit(injection_points_cleanup, (Datum) 0);

	PG_RETURN_VOID();
}

/*
 * SQL function for dropping an injection point.
 */
PG_FUNCTION_INFO_V1(injection_points_detach);
Datum
injection_points_detach(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (!InjectionPointDetach(name))
		elog(ERROR, "could not detach injection point \"%s\"", name);

	/* Remove point from local list, if required */
	if (inj_list_local != NIL)
	{
		MemoryContext oldctx;

		oldctx = MemoryContextSwitchTo(TopMemoryContext);
		inj_list_local = list_delete(inj_list_local, makeString(name));
		MemoryContextSwitchTo(oldctx);
	}

	PG_RETURN_VOID();
}

/*
 * SQL function for listing all the injection points attached.
 */
PG_FUNCTION_INFO_V1(injection_points_list);
Datum
injection_points_list(PG_FUNCTION_ARGS)
{
#define NUM_INJECTION_POINTS_LIST 3
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	List	   *inj_points;
	ListCell   *lc;

	/* Build a tuplestore to return our results in */
	InitMaterializedSRF(fcinfo, 0);

	inj_points = InjectionPointList();

	foreach(lc, inj_points)
	{
		Datum		values[NUM_INJECTION_POINTS_LIST];
		bool		nulls[NUM_INJECTION_POINTS_LIST];
		InjectionPointData *inj_point = lfirst(lc);

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = PointerGetDatum(cstring_to_text(inj_point->name));
		values[1] = PointerGetDatum(cstring_to_text(inj_point->library));
		values[2] = PointerGetDatum(cstring_to_text(inj_point->function));

		/* shove row into tuplestore */
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum) 0;
#undef NUM_INJECTION_POINTS_LIST
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	RegisterShmemCallbacks(&injection_shmem_callbacks);
}
