/*-------------------------------------------------------------------------
 *
 * test_shmem.c
 *		Helpers to test shmem allocation routines
 *
 * Test basic memory allocation in an extension module. One notable feature
 * that is not exercised by any other module in the repository is the
 * allocating (non-DSM) shared memory after postmaster startup.
 *
 * Copyright (c) 2020-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/test_shmem/test_shmem.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "storage/shmem.h"


PG_MODULE_MAGIC;

typedef struct TestShmemData
{
	int			value;
	bool		initialized;
	int			attach_count;
} TestShmemData;

static TestShmemData *TestShmem;

static bool attached_or_initialized = false;

static void test_shmem_request(void *arg);
static void test_shmem_init(void *arg);
static void test_shmem_attach(void *arg);
static void test_shmem_failure_request(void *arg);

static const ShmemCallbacks TestShmemCallbacks = {
	.flags = SHMEM_CALLBACKS_ALLOW_AFTER_STARTUP,
	.request_fn = test_shmem_request,
	.init_fn = test_shmem_init,
	.attach_fn = test_shmem_attach,
};

static const ShmemCallbacks TestShmemFailureCallbacks = {
	.flags = SHMEM_CALLBACKS_ALLOW_AFTER_STARTUP,
	.request_fn = test_shmem_failure_request,
};

static int	failure_mode;

static void
test_shmem_request(void *arg)
{
	elog(LOG, "test_shmem_request callback called");

	ShmemRequestStruct(.name = "test_shmem area",
					   .size = sizeof(TestShmemData),
					   .ptr = (void **) &TestShmem);
}

static void
test_shmem_init(void *arg)
{
	elog(LOG, "init callback called");
	if (TestShmem->initialized)
		elog(ERROR, "shmem area already initialized");
	TestShmem->initialized = true;

	if (attached_or_initialized)
		elog(ERROR, "attach or initialize already called in this process");
	attached_or_initialized = true;
}

static void
test_shmem_attach(void *arg)
{
	elog(LOG, "test_shmem_attach callback called");
	if (!TestShmem->initialized)
		elog(ERROR, "shmem area not yet initialized");
	TestShmem->attach_count++;

	if (attached_or_initialized)
		elog(ERROR, "attach or initialize already called in this process");
	attached_or_initialized = true;
}

/* Request callback used by the after-startup failure tests. */
static void
test_shmem_failure_request(void *arg)
{
	static void *ptr1;

	switch (failure_mode)
	{
		case 0:
			ShmemRequestStruct(.name = "test_shmem callback error area",
							   .size = 1024, .ptr = &ptr1);
			elog(ERROR, "test_shmem request callback failed on purpose");
		case 1:
			ShmemRequestStruct(.name = "test_shmem oversized area",
							   .size = (Size) 1024 * 1024 * 1024,
							   .ptr = &ptr1);
			break;
		default:
			elog(ERROR, "unrecognized test_shmem failure mode: %d", failure_mode);
	}
}

void
_PG_init(void)
{
	elog(LOG, "test_shmem module's _PG_init called");
	RegisterShmemCallbacks(&TestShmemCallbacks);
}

PG_FUNCTION_INFO_V1(test_shmem_failure);
Datum
test_shmem_failure(PG_FUNCTION_ARGS)
{
	failure_mode = PG_GETARG_INT32(0);
	RegisterShmemCallbacks(&TestShmemFailureCallbacks);
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(get_test_shmem_attach_count);
Datum
get_test_shmem_attach_count(PG_FUNCTION_ARGS)
{
	if (!attached_or_initialized)
		elog(ERROR, "shmem area not attached or initialized in this process");
	if (!TestShmem->initialized)
		elog(ERROR, "shmem area not yet initialized");
	PG_RETURN_INT32(TestShmem->attach_count);
}
