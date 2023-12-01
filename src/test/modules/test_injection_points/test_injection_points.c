/*--------------------------------------------------------------------------
 *
 * test_injection_points.c
 *		Code for testing injection points.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/test/modules/test_injection_points/test_injection_points.c
 *
 * Injection points are able to trigger user-defined callbacks in pre-defined
 * code paths.
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/injection_point.h"
#include "utils/wait_event.h"

PG_MODULE_MAGIC;

/* Shared state information for injection points. */
typedef struct TestInjectionPointSharedState
{
	/*
	 * Wait variable that can be registered at a given point, and that can be
	 * awakened via SQL.
	 */
	ConditionVariable	wait_point;
} TestInjectionPointSharedState;

/* Pointer to shared-memory state. */
static TestInjectionPointSharedState *inj_state = NULL;

/* Wait event when waiting on condition variable */
static uint32 test_injection_wait_event = 0;

extern PGDLLEXPORT void test_injection_error(const char *name);
extern PGDLLEXPORT void test_injection_notice(const char *name);
extern PGDLLEXPORT void test_injection_wait(const char *name);


static void
test_injection_init_shmem(void)
{
	bool		found;

	if (inj_state != NULL)
		return;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	inj_state = ShmemInitStruct("test_injection_points",
								sizeof(TestInjectionPointSharedState),
								&found);
	if (!found)
	{
		/* First time through ... */
		MemSet(inj_state, 0, sizeof(TestInjectionPointSharedState));
		ConditionVariableInit(&inj_state->wait_point);
	}
	LWLockRelease(AddinShmemInitLock);
}

/* Set of callbacks available at point creation */
void
test_injection_error(const char *name)
{
	elog(ERROR, "error triggered for injection point %s", name);
}

void
test_injection_notice(const char *name)
{
	elog(NOTICE, "notice triggered for injection point %s", name);
}

void
test_injection_wait(const char *name)
{
	if (inj_state == NULL)
		test_injection_init_shmem();
	if (test_injection_wait_event == 0)
		test_injection_wait_event = WaitEventExtensionNew("test_injection_wait");

	/* And sleep.. */
	ConditionVariablePrepareToSleep(&inj_state->wait_point);
	ConditionVariableSleep(&inj_state->wait_point, test_injection_wait_event);
	ConditionVariableCancelSleep();
}

/*
 * SQL function for creating an injection point.
 */
PG_FUNCTION_INFO_V1(test_injection_points_attach);
Datum
test_injection_points_attach(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *mode = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char	   *function;

	if (strcmp(mode, "error") == 0)
		function = "test_injection_error";
	else if (strcmp(mode, "notice") == 0)
		function = "test_injection_notice";
	else if (strcmp(mode, "wait") == 0)
		function = "test_injection_wait";
	else
		elog(ERROR, "incorrect mode \"%s\" for injection point creation", mode);

	InjectionPointAttach(name, "test_injection_points", function);

	PG_RETURN_VOID();
}

/*
 * SQL function for triggering an injection point.
 */
PG_FUNCTION_INFO_V1(test_injection_points_run);
Datum
test_injection_points_run(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	INJECTION_POINT(name);

	PG_RETURN_VOID();
}

/*
 * SQL function for waking a condition variable.
 */
PG_FUNCTION_INFO_V1(test_injection_points_wake);
Datum
test_injection_points_wake(PG_FUNCTION_ARGS)
{
	if (inj_state == NULL)
		test_injection_init_shmem();

	ConditionVariableBroadcast(&inj_state->wait_point);
	PG_RETURN_VOID();
}

/*
 * SQL function for dropping an injection point.
 */
PG_FUNCTION_INFO_V1(test_injection_points_detach);
Datum
test_injection_points_detach(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	InjectionPointDetach(name);

	PG_RETURN_VOID();
}
