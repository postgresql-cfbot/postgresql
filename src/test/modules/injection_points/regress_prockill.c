/*-------------------------------------------------------------------------
 *
 * regress_prockill.c
 *		SQL helpers for the ProcKill lock-group TAP test
 *
 * Exposes lock-group formation without parallel query and helpers that
 * observe victim backends while they are inside ProcKill().
 *
 * Copyright (c) 2025-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/injection_points/regress_prockill.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/injection_point.h"
#include "utils/wait_event.h"

#include "injection_points.h"

/* Read a uint32 field exactly once (matches waitfuncs.c idiom). */
#define UINT32_ACCESS_ONCE(var)	((uint32) (*((volatile uint32 *) &(var))))

PG_FUNCTION_INFO_V1(prockill_become_lock_group_leader);

Datum
prockill_become_lock_group_leader(PG_FUNCTION_ARGS)
{
	BecomeLockGroupLeader();
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(prockill_become_lock_group_member);

Datum
prockill_become_lock_group_member(PG_FUNCTION_ARGS)
{
	int			leader_pid = PG_GETARG_INT32(0);
	PGPROC	   *leader;

	leader = BackendPidGetProc(leader_pid);
	if (leader == NULL)
		elog(ERROR, "backend with PID %d not found", leader_pid);

	if (!BecomeLockGroupMember(leader, leader_pid))
		elog(ERROR, "could not join lock group of backend %d", leader_pid);

	PG_RETURN_VOID();
}

/*
 * Attach injection_wait to point_name with INJ_CONDITION_PID scoping it to a
 * specific backend's PID. The caller is responsible for ensuring the
 * injection point fires only in the contexts of interest -- in the
 * lock-group teardown test, the point is placed inside a
 * MyProc->lockGroupLeader != NULL branch in ProcKill().
 *
 * Must be called from a controller session, not from a backend that is itself
 * about to fire the point.  injection_points_attach() registers a
 * before_shmem_exit(injection_points_cleanup) hook that would detach the
 * point as soon as the controller's session ends (and, if called from a
 * victim, before ProcKill ever runs).  Calling InjectionPointAttach()
 * directly leaves no such hook.
 */
PG_FUNCTION_INFO_V1(prockill_attach_injection_wait_pid);

Datum
prockill_attach_injection_wait_pid(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int			target_pid = PG_GETARG_INT32(1);
	InjectionPointCondition cond = {INJ_CONDITION_PID, target_pid};

	InjectionPointAttach(name, "injection_points", "injection_wait",
						 &cond, sizeof(cond));
	PG_RETURN_VOID();
}

/*
 * Return true if the backend with target_pid is currently waiting at the
 * named injection point, by reading PGPROC->wait_event_info directly.
 *
 * At the injection points in ProcKill(), the standard observability surfaces
 * (pg_stat_activity, BackendPidGetProc) are already unavailable: both
 * pgstat_beshutdown_hook and RemoveProcFromArray run as on_shmem_exit
 * callbacks before ProcKill.  The PGPROC slot itself remains intact until
 * ProcKill zeroes proc->pid near its end, so a direct allProcs scan works.
 */
PG_FUNCTION_INFO_V1(prockill_backend_in_injection);

Datum
prockill_backend_in_injection(PG_FUNCTION_ARGS)
{
	int			target_pid = PG_GETARG_INT32(0);
	char	   *want_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
	uint32		n = ProcGlobal->allProcCount;

	for (uint32 i = 0; i < n; i++)
	{
		PGPROC	   *proc = &ProcGlobal->allProcs[i];
		const char *ev;

		if (proc->pid != target_pid)
			continue;

		ev = pgstat_get_wait_event(UINT32_ACCESS_ONCE(proc->wait_event_info));
		PG_RETURN_BOOL(ev != NULL && strcmp(ev, want_name) == 0);
	}

	PG_RETURN_BOOL(false);
}
