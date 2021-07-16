/*--------------------------------------------------------------------------
 *
 * test_process_interrupts_hook.c
 *		Code for testing RLS hooks.
 *
 * Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_hooks/test_process_interrupts_hook.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "storage/latch.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "test_hooks.h"

/* Saved hook values in case of unload */
static ProcessInterrupts_hook_type original_ProcessInterrupts_hook = NULL;

/* Next hooks to call in the chain */
static ProcessInterrupts_hook_type next_ProcessInterrupts_hook = NULL;

static int times_processed_interrupts;
static bool exit_deferred;
static bool saved_ProcDiePending;

static void
test_ProcessInterrupts_hook(void)
{
	/* Hook never gets to fire if interrupts are held */
	Assert(InterruptHoldoffCount == 0);

	/*
	 * Typically you'd HOLD_INTERRUPTS() instead of this hack to defer
	 * interrupts. But just as an example, this extension will defer
	 * processing of SIGTERM via die() while acting normally on other
	 * signals.
	 *
	 * It'll react to the die() when exit_deferred=false is set and its
	 * latch is set or it's woken for some other reason, since it does not
	 * restore InterruptsPending=true.
	 *
	 * This relies on running in a normal user backend that uses die()
	 * as its SIGTERM handler.
	 */
	if (exit_deferred && ProcDiePending)
	{
		saved_ProcDiePending = true;
		ProcDiePending = false;
	}
	else if (!exit_deferred && saved_ProcDiePending)
	{
		ProcDiePending = true;
		saved_ProcDiePending = false;
	}

	if (next_ProcessInterrupts_hook)
		next_ProcessInterrupts_hook();
	else
		standard_ProcessInterrupts();

	times_processed_interrupts ++;
}

void
install_test_process_interrupts_hook(void)
{
	/* Save values for unload  */
	original_ProcessInterrupts_hook = ProcessInterrupts_hook;

	/* Set our hooks */
	next_ProcessInterrupts_hook = ProcessInterrupts_hook;
	ProcessInterrupts_hook = test_ProcessInterrupts_hook;

	times_processed_interrupts = 0;
	exit_deferred = false;
}

void
uninstall_test_process_interrupts_hook(void)
{
	if (ProcessInterrupts_hook != test_ProcessInterrupts_hook)
		elog(ERROR, "cannot unload test_hooks: another hook has been registered on the ProcessInterrupts_hook");

	ProcessInterrupts_hook = original_ProcessInterrupts_hook;
}

PG_FUNCTION_INFO_V1(get_times_processed_interrupts);

Datum
get_times_processed_interrupts(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(times_processed_interrupts);
}

PG_FUNCTION_INFO_V1(set_exit_deferred);

Datum
set_exit_deferred(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		elog(ERROR, "argument must be non-null");
	exit_deferred = PG_GETARG_BOOL(0);
	if (!exit_deferred)
		InterruptPending = true;
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(test_hold_interrupts_hook);

/*
 * Make sure the hook doesn't fire when interrupts are held
 */
Datum
test_hold_interrupts_hook(PG_FUNCTION_ARGS)
{
	int rc;

	HOLD_INTERRUPTS();

	SetLatch(MyLatch);

	CHECK_FOR_INTERRUPTS();

	rc = WaitLatch(MyLatch, WL_LATCH_SET|WL_TIMEOUT|WL_EXIT_ON_PM_DEATH, 1000, PG_WAIT_EXTENSION);

	RESUME_INTERRUPTS();

	if (rc & WL_LATCH_SET)
		elog(ERROR, "received latch set event with interrupts held");
	if (rc & WL_TIMEOUT)
		elog(INFO, "timed out as expected");

	PG_RETURN_VOID();
}
