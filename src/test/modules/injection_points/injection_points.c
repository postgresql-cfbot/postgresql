/*--------------------------------------------------------------------------
 *
 * injection_points.c
 *		Code for testing injection points.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/test/modules/injection_points/injection_points.c
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
static uint32 injection_wait_event = 0;

extern PGDLLEXPORT void injection_error(const char *name);
extern PGDLLEXPORT void injection_notice(const char *name);
extern PGDLLEXPORT void injection_wait(const char *name);


static void
injection_init_shmem(void)
{
	bool		found;

	if (inj_state != NULL)
		return;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	inj_state = ShmemInitStruct("injection_points",
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

/* Set of callbacks available to be attached to an injection point. */
void
injection_error(const char *name)
{
	elog(ERROR, "error triggered for injection point %s", name);
}

void
injection_notice(const char *name)
{
	elog(NOTICE, "notice triggered for injection point %s", name);
}

void
injection_wait(const char *name)
{
	if (inj_state == NULL)
		injection_init_shmem();
	if (injection_wait_event == 0)
		injection_wait_event = WaitEventExtensionNew("injection_wait");

	/* And sleep.. */
	ConditionVariablePrepareToSleep(&inj_state->wait_point);
	ConditionVariableSleep(&inj_state->wait_point, injection_wait_event);
	ConditionVariableCancelSleep();
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

	if (strcmp(action, "error") == 0)
		function = "injection_error";
	else if (strcmp(action, "notice") == 0)
		function = "injection_notice";
	else if (strcmp(mode, "wait") == 0)
		function = "injection_wait";
	else
		elog(ERROR, "incorrect action \"%s\" for injection point creation", action);

	InjectionPointAttach(name, "injection_points", function);

	PG_RETURN_VOID();
}

/*
 * SQL function for triggering an injection point.
 */
PG_FUNCTION_INFO_V1(injection_points_run);
Datum
injection_points_run(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	INJECTION_POINT(name);

	PG_RETURN_VOID();
}

/*
 * SQL function for waking a condition variable.
 */
PG_FUNCTION_INFO_V1(injection_points_wake);
Datum
injection_points_wake(PG_FUNCTION_ARGS)
{
	if (inj_state == NULL)
		injection_init_shmem();

	ConditionVariableBroadcast(&inj_state->wait_point);
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

	InjectionPointDetach(name);

	PG_RETURN_VOID();
}
