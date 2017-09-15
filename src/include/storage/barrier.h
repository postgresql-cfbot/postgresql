/*-------------------------------------------------------------------------
 *
 * barrier.h
 *	  Barriers for synchronizing cooperating processes.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/barrier.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BARRIER_H
#define BARRIER_H

/*
 * For the header previously known as "barrier.h", please include
 * "port/atomics.h", which deals with atomics, compiler barriers and memory
 * barriers.
 */

#include "storage/condition_variable.h"
#include "storage/spin.h"

typedef struct Barrier
{
	slock_t mutex;
	int		phase;			/* phase counter */
	int		participants;	/* the number of participants attached */
	int		arrived;		/* the number of participants that have arrived */
	bool	selected;		/* has one participant received 'true' yet? */
	ConditionVariable condition_variable;

#ifdef BARRIER_DEBUG
	int attach_at_phase[16];
#endif
} Barrier;

extern void BarrierInit(Barrier *barrier, int num_workers);
extern bool BarrierWait(Barrier *barrier, uint32 wait_event_info);
extern int BarrierAttach(Barrier *barrier);
extern bool BarrierDetach(Barrier *barrier);
extern int BarrierPhase(Barrier *barrier);
extern int BarrierParticipants(Barrier *barrier);

#ifdef BARRIER_DEBUG
extern void BarrierEnableDebug(Barrier *barrier, const char *name);
#endif

#endif   /* BARRIER_H */
