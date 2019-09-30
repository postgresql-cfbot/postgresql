/*--------------------------------------------------------------------------
 *
 * test_shm_mcxt.c
 *		Code to set up a ShmContext and test it
 *
 * Copyright (c) 2013-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_shm_mcxt/test_shm_mcxt.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "lib/ilist.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "nodes/memnodes.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/memutils.h"

#define MY_AREA_SIZE (1024 * 1024)


PG_MODULE_MAGIC;

void _PG_init(void);
PG_FUNCTION_INFO_V1(set_shared_list);
PG_FUNCTION_INFO_V1(get_shared_list);

static void shm_mcxt_shmem_startup_hook(void);

static shmem_startup_hook_type prev_shmem_startup_hook;
static void *my_raw_memory;
static dsa_area *my_area;
static MemoryContext my_shared_dsa_context;
static MemoryContext my_local_dsa_context;

static List **my_list;


void
_PG_init(void)
{
	/* This only works if preloaded by the postmaster. */
	if (!process_shared_preload_libraries_in_progress)
		return;

	/* Request a chunk of traditional shared memory. */
	RequestAddinShmemSpace(MY_AREA_SIZE);

	/* Register our hook for phase II of initialization. */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = shm_mcxt_shmem_startup_hook;
}

static void
shm_mcxt_shmem_startup_hook(void)
{
	MemoryContext	old_context;
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	old_context = MemoryContextSwitchTo(TopMemoryContext);

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	/* Allocate, or look up, a chunk of raw fixed-address shared memory. */
	my_raw_memory = ShmemInitStruct("my_area", MY_AREA_SIZE, &found);
	if (!found)
	{
		/*
		 * Create a new DSA area, and clamp its size so it can't make any
		 * segments outside the provided space.
		 */
		my_area = dsa_create_in_place(my_raw_memory, MY_AREA_SIZE, 0, NULL);
		dsa_set_size_limit(my_area, MY_AREA_SIZE);
	}
	else
	{
		/* Attach to an existing area. */
		my_area = dsa_attach_in_place(my_raw_memory, NULL);
	}

	/* Also allocate or look up a list header. */
	my_list = ShmemInitStruct("my_list", MY_AREA_SIZE, &found);
	if (!found)
		*my_list = NIL;

	LWLockRelease(AddinShmemInitLock);

	/* Create a memory context. */
	my_shared_dsa_context = CreatePermShmContext(NULL, "my_shared_context",
												 my_area, my_raw_memory);

	MemoryContextSwitchTo(old_context);
}

/* Set the positive number */
Datum
set_shared_list(PG_FUNCTION_ARGS)
{
	int i = PG_GETARG_INT32(0);
	ListCell *lc;
	MemoryContext old_context;

	my_local_dsa_context = CreateTempShmContext(CurrentMemoryContext,
									  "my_local_context",
									  my_shared_dsa_context);

	old_context = MemoryContextSwitchTo(my_local_dsa_context);

	/* Manipulate a list in shared memory. */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	if (i < 0)
		*my_list = list_delete_int(*my_list, -i);
	else
		*my_list = lappend_int(*my_list, i);
	LWLockRelease(AddinShmemInitLock);

	ChangeToPermShmContext(my_local_dsa_context, my_shared_dsa_context);

	/* Dump list. */
	elog(NOTICE, "Contents of list:");
	foreach(lc, *my_list)
		elog(NOTICE, " %d", lfirst_int(lc));

	MemoryContextSwitchTo(old_context);

	PG_RETURN_VOID();
}

/* Get the list of intergers registerd to shared list */
Datum
get_shared_list(PG_FUNCTION_ARGS)
{
   	FuncCallContext    *funcctx;
	MemoryContext		oldcontext;
   	int		result;
	ListCell **lcp;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
			
		/*
		 * Switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		

   	   	LWLockAcquire(AddinShmemInitLock, LW_SHARED);
   		/* allocate memory for user context to hold current cell */
		lcp = (ListCell **) palloc(sizeof(ListCell *));
   		*lcp = list_head(*my_list);
		funcctx->user_fctx = (void *) lcp;
		*lcp = list_head(*my_list);

		LWLockRelease(AddinShmemInitLock);

		MemoryContextSwitchTo(oldcontext);
	}
	
	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	lcp = (ListCell **) funcctx->user_fctx;

   	while (*lcp != NULL)
	{
		result =  lfirst_int(*lcp);
		*lcp = lnext(*lcp);
		SRF_RETURN_NEXT(funcctx, Int32GetDatum(result));
	}

	SRF_RETURN_DONE(funcctx);
}
