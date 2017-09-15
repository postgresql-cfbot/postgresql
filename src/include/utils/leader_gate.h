/*-------------------------------------------------------------------------
 *
 * leader_gate.h
 *	  A deadlock avoidance mechanism for parallel execution.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/leader_gate.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LEADER_GATE_H
#define LEADER_GATE_H

#include "storage/spin.h"

typedef struct LeaderGate
{
	slock_t mutex;
	bool at_least_one_worker_attached;
	bool leader_attached;
	enum
	{
		LG_UNDECIDED,
		LG_LEADER_CONTINUES,
		LG_WORKERS_CONTINUE
	} state;
} LeaderGate;

extern void LeaderGateInit(LeaderGate *gate);
extern void LeaderGateAttach(LeaderGate *gate);
extern bool LeaderGateCanContinue(LeaderGate *gate);

#endif   /* LEADER_GATE_H */
