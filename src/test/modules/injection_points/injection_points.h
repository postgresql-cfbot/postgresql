/*-------------------------------------------------------------------------
 *
 * injection_points.h
 *		Definitions for the injection points module
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/test/modules/injection_points/injection_points.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef INJECTION_POINTS_H
#define INJECTION_POINTS_H

#include <stdint.h>

/* Maximum number of waits usable in injection points at once */
#define INJ_MAX_WAIT	8
#define INJ_NAME_MAXLEN 64

/*
 * Name of the file under the data directory holding this module's wait/wakeup
 * state.  The state is mapped from this file (see injection_points.c) so that
 * external programs without a backend connection can observe and release
 * waiting processes (see injection_points_state.c).
 *
 * This is distinct from the core registry file (injection_points.shm, see
 * src/backend/utils/misc/injection_point.c) that records which points are
 * attached: the registry says *which* points exist, this file coordinates the
 * wait/wakeup of points whose action is "wait".
 */
#define INJ_STATE_FILE	"injection_points_wait.shm"

/*
 * Publicly-mappable portion of the injection point shared state.
 *
 * This describes the layout that external tools rely on when mapping
 * INJ_STATE_FILE.  It must stay at the very front of the backend-only
 * InjectionPointSharedState (see injection_points.c, which static-asserts
 * the layout), and must only use types that are also available to frontend
 * code: no slock_t, no pg_atomic_uint32.
 *
 * "name" holds the injection point name registered by a waiting process in
 * its slot, or an empty string if the slot is free.  "wait_counts" is bumped
 * to release the waiter occupying the matching slot; a 32-bit aligned counter
 * is binary-compatible with the backend's pg_atomic_uint32.
 */
typedef struct InjectionPointPublicState
{
	char		name[INJ_MAX_WAIT][INJ_NAME_MAXLEN];
	uint32_t	wait_counts[INJ_MAX_WAIT];
} InjectionPointPublicState;

typedef enum InjectionPointConditionType
{
	INJ_CONDITION_ALWAYS = 0,	/* always run */
	INJ_CONDITION_PID,			/* PID restriction */
} InjectionPointConditionType;

typedef struct InjectionPointCondition
{
	/* Type of the condition */
	InjectionPointConditionType type;

	/* ID of the process where the injection point is allowed to run */
	int			pid;
} InjectionPointCondition;

#endif							/* INJECTION_POINTS_H */
