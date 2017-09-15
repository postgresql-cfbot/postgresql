/*-------------------------------------------------------------------------
 *
 * barrier.c
 *	  Barriers for synchronizing cooperating processes.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This implementation of barriers allows for static sets of participants
 * known up front, or dynamic sets of participants which processes can join or
 * leave at any time.  In the dynamic case, a phase number can be used to
 * track progress through a parallel algorithm, and may be necessary to
 * synchronize with the current phase of a multi-phase algorithm when a new
 * participant joins.  In the static case, the phase number is used
 * internally, but it isn't strictly necessary for client code to access it
 * because the phase can only advance when the declared number of participants
 * reaches the barrier so client code should be in no doubt about the current
 * phase of computation at all times.
 *
 * Consider a parallel algorithm that involves separate phases of computation
 * A, B and C where the output of each phase is needed before the next phase
 * can begin.
 *
 * In the case of a static barrier initialized with 4 participants, each
 * participant works on phase A, then calls BarrierWait to wait until all 4
 * participants have reached that point.  When BarrierWait returns control,
 * each participant can work on B, and so on.  Because the barrier knows how
 * many participants to expect, the phases of computation don't need labels or
 * numbers, since each process's program counter implies the current phase.
 * Even if some of the processes are slow to start up and begin running phase
 * A, the other participants are expecting them and will patiently wait at the
 * barrier.  The code could be written as follows:
 *
 *     perform_a();
 *     BarrierWait(&barrier, ...);
 *     perform_b();
 *     BarrierWait(&barrier, ...);
 *     perform_c();
 *     BarrierWait(&barrier, ...);
 *
 * If the number of participants is not known up front, then a dynamic barrier
 * is needed and the number should be set to zero at initialization.  New
 * complications arise because the number necessarily changes over time as
 * participants attach and detach, and therefore phases B, C or even the end
 * of processing may be reached before any given participant has started
 * running and attached.  Therefore the client code must perform an initial
 * test of the phase number after attaching, because it needs to find out
 * which phase of the algorithm has been reached by any participants that are
 * already attached in order to synchronize with that work.  Once the program
 * counter or some other representation of current progress is synchronized
 * with the barrier's phase, normal control flow can be used just as in the
 * static case.  Our example could be written using a switch statement with
 * cases that fall-through, as follows:
 *
 *     phase = BarrierAttach(&barrier);
 *     switch (phase)
 *     {
 *     case PHASE_A:
 *         perform_a();
 *         BarrierWait(&barrier, ...);
 *     case PHASE_B:
 *         perform_b();
 *         BarrierWait(&barrier, ...);
 *     case PHASE_C:
 *         perform_c();
 *         BarrierWait(&barrier, ...);
 *     default:
 *     }
 *     BarrierDetach(&barrier);
 *
 * Static barriers behave similarly to POSIX's pthread_barrier_t.  Dynamic
 * barriers behave similarly to Java's java.util.concurrent.Phaser.
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/barrier.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "storage/barrier.h"

#ifdef BARRIER_DEBUG
#include "access/parallel.h"
#include "pgstat.h"
#include "utils/guc.h"
#endif

#ifdef BARRIER_DEBUG
/* Debugging GUC. */
char *barrier_attach_phases = "";
#endif

static bool
BarrierWaitImpl(Barrier *barrier, uint32 wait_event_info, bool selectable);

/*
 * Initialize this barrier, setting a static number of participants that we
 * will wait for at each computation phase.  To use a dynamic number of
 * participants, this number should be zero, and BarrierAttach and
 * BarrierDetach should be used to register and deregister participants.
 */
void
BarrierInit(Barrier *barrier, int participants)
{
	SpinLockInit(&barrier->mutex);
	barrier->participants = participants;
	barrier->arrived = 0;
	barrier->phase = 0;
	barrier->selected = false;
	ConditionVariableInit(&barrier->condition_variable);
}

/*
 * Wait for all participants to arrive at this barrier, and then return in all
 * participants.  Increments the current phase.  The caller must be attached
 * to this barrier.
 *
 * While waiting, pg_stat_activity shows a wait_event_class and wait_event
 * controlled by the wait_event_info passed in, which should be a value from
 * from one of the WaitEventXXX enums defined in pgstat.h.
 *
 * Return true in one arbitrarily selected participant.  Return false in all
 * others.  The differing return code can be used to coordinate a phase of
 * work that must be done in only one participant while the others wait.
 */
bool
BarrierWait(Barrier *barrier, uint32 wait_event_info)
{
	return BarrierWaitImpl(barrier, wait_event_info, true);
}

/*
 * Attach to a barrier.  All waiting participants will now wait for this
 * participant to call BarrierWait or BarrierDetach.  Return the current
 * phase.
 */
int
BarrierAttach(Barrier *barrier)
{
	int		phase;

	SpinLockAcquire(&barrier->mutex);
	++barrier->participants;
	phase = barrier->phase;

#ifdef BARRIER_DEBUG
	/*
	 * See if this backend needs to simulate attaching later.  If so, 'wait'
	 * repeatedly until the requested phase is reached.
	 */
	{
		int		slot = ParallelWorkerNumber + 1;

		Assert(slot >= 0);
		if (slot < lengthof(barrier->attach_at_phase))
		{
			int		attach_at_phase = barrier->attach_at_phase[slot];

			/* Have we been asked to simulate attaching later? */
			if (attach_at_phase >= 0)
			{
				/*
				 * We were pre-registered by BarrierEnableDebug so that all
				 * backends being controlled could meet here at phase 0.
				 * We now undo the extra participant count.
				 */
				--barrier->participants;
				SpinLockRelease(&barrier->mutex);

				/* Revert to normal mode if we detach and reattach later. */
				barrier->attach_at_phase[slot] = -1;

				elog(LOG,
					 "ParallelWorkerNumber %d simulating BarrierAttach at phase %d...",
					 ParallelWorkerNumber, attach_at_phase);

				/*
				 * Now wait patiently until other backend progress to the
				 * requested phase, being careful not to be selected (one
				 * regular BarrierWait() caller must receive a true response).
				 */
				Assert(BarrierPhase(barrier) == 0);
				while (BarrierPhase(barrier) < attach_at_phase)
					BarrierWaitImpl(barrier, PG_WAIT_EXTENSION, false);
				Assert(BarrierPhase(barrier) == attach_at_phase);

				elog(LOG,
					 "... ParallelWorkerNumber %d attached at phase %d",
					 ParallelWorkerNumber, attach_at_phase);

				return attach_at_phase;
			}
		}
	}
#endif

	SpinLockRelease(&barrier->mutex);

	return phase;
}

/*
 * Detach from a barrier.  This may release other waiters from BarrierWait and
 * advance the phase, if they were only waiting for this backend.  Return
 * true if this participant was the last to detach.
 */
bool
BarrierDetach(Barrier *barrier)
{
	bool	release;
	bool	last;

	SpinLockAcquire(&barrier->mutex);
	Assert(barrier->participants > 0);
	--barrier->participants;

	/*
	 * If any other participants are waiting and we were the last participant
	 * waited for, release them.
	 */
	if (barrier->participants > 0 &&
		barrier->arrived == barrier->participants)
	{
		release = true;
		barrier->arrived = 0;
		++barrier->phase;
		Assert(barrier->selected);
		barrier->selected = false;
	}
	else
		release = false;

	last = barrier->participants == 0;
	SpinLockRelease(&barrier->mutex);

	if (release)
		ConditionVariableBroadcast(&barrier->condition_variable);

	return last;
}

/*
 * Return the current phase of a barrier.  The caller must be attached.
 */
int
BarrierPhase(Barrier *barrier)
{
	/*
	 * It is OK to read barrier->phase without locking, because it can't
	 * change without us (we are attached to it), and we executed a memory
	 * barrier when we either attached or participated in changing it last
	 * time.
	 */
	return barrier->phase;
}

/*
 * Return an instantaneous snapshot of the number of participants currently
 * attached to this barrier.  For debugging purposes only.
 */
int
BarrierParticipants(Barrier *barrier)
{
	int		participants;

	SpinLockAcquire(&barrier->mutex);
	participants = barrier->participants;
	SpinLockRelease(&barrier->mutex);

	return participants;
}

#ifdef BARRIER_DEBUG
/*
 * Configure a barrier for a special debug mode where different arrival
 * schedules can be controlled by setting the GUC "barrier_attach_phases".  If
 * the GUC is set to a value that has this name followed by ':' as a prefix,
 * then the phase at which each backend attaches  can be controlled so that
 * different interleavings can be tested.  Multiple name:xxx directives can
 * be separated with spaces.
 *
 * The part following ':' is a comma separate list of numbers.  The first
 * is the phase the leader should attach at, or -1 for any.  The rest control
 * the parallel workers, ordered by ParallelWorkerNumber.
 *
 * For example, if this function is called with name "HashJoin.barrier" then
 * setting barrier_attach_phases to "HashJoin.barrier:0,16,2" says that the
 * leader should attach first, worker 0 should simulate attaching at phase 16
 * and worker 1 should simulate attaching at phase 2.
 *
 * Must be called immediately after BarrierInit() and before any other access.
 */
void
BarrierEnableDebug(Barrier *barrier, const char *name)
{
	const char *p = barrier_attach_phases;
	size_t		len = strlen(name);
	size_t		i;

	while (*p != '\0')
	{
		if (strncmp(p, name, len) == 0 && p[len] == ':') {
			int			num_controlled = 0;

			/* Parse the control string. */
			i = 0;
			p += len + 1;
			while (*p != '\0' && *p != ' ')
			{
				long	phase;
				char   *next;

				if (i >= lengthof(barrier->attach_at_phase))
					elog(ERROR, "barrier debug string too long");
				phase = strtol(p, &next, 0);
				if (next == p)
					elog(ERROR, "could not parse barrier_attach_phase");
				barrier->attach_at_phase[i++] = (int) phase;
				if (phase >= 0)
					++num_controlled;
				p = next;
				if (*p == ',')
					++p;
			}

			/*
			 * We'll pre-attach all participants that we are controlling.
			 * That way the barrier can't get out of phase 0 until they
			 * attach, and then they can simulate attaching at a later phase
			 * in BarrierAttach().
			 */
			barrier->participants += num_controlled;

			/* Mark the remaining slots as unused. */
			while (i < lengthof(barrier->attach_at_phase))
				barrier->attach_at_phase[i++] = -1;

			return;
		}

		/*
		 * Step over this name and possibly a whitespace separator so that we
		 * can see if there is another one.
		 */
		while (*p != '\0' && *p != ' ')
			++p;
		while (*p != '\0' && *p == ' ')
			++p;
	}

	/* We didn't find this barrier's debugging name. */
	for (i = 0; i < lengthof(barrier->attach_at_phase); ++i)
		barrier->attach_at_phase[i] = -1;
}
#endif

/*
 * Regular BarrierWait() callers are eligible to be selected to return true,
 * but BarrierAttach() in a BARRIER_DEBUG build needs a way to wait without
 * ever being selected.  The first 'selectable' caller to arrive at each
 * phase will be selected.
 */
static bool
BarrierWaitImpl(Barrier *barrier, uint32 wait_event_info, bool selectable)
{
	bool	selected;
	bool	last;
	int		start_phase;
	int		next_phase;

	SpinLockAcquire(&barrier->mutex);
	start_phase = barrier->phase;
	next_phase = start_phase + 1;
	++barrier->arrived;
	if (!barrier->selected && selectable)
		barrier->selected = selected = true;
	else
		selected = false;
	if (barrier->arrived == barrier->participants)
	{
		last = true;
		barrier->arrived = 0;
		barrier->phase = next_phase;
		Assert(barrier->selected);
		barrier->selected = false;
	}
	else
		last = false;
	SpinLockRelease(&barrier->mutex);

	/*
	 * If we were the last expected participant to arrive, we can release our
	 * peers and return.
	 */
	if (last)
	{
		ConditionVariableBroadcast(&barrier->condition_variable);

		return selected;
	}

	/*
	 * Otherwise we have to wait for the last participant to arrive and
	 * advance the phase.
	 */
	ConditionVariablePrepareToSleep(&barrier->condition_variable);
	for (;;)
	{
		bool advanced;

		/*
		 * We know that phase must either be start_phase, indicating that we
		 * need to keep waiting, or next_phase, indicating that the last
		 * participant that we were waiting for has either arrived or detached
		 * so that the next phase has begun.  The phase cannot advance any
		 * further than that without this backend's participation, because
		 * this backend is attached.
		 */
		SpinLockAcquire(&barrier->mutex);
		Assert(barrier->phase == start_phase || barrier->phase == next_phase);
		advanced = barrier->phase == next_phase;
		SpinLockRelease(&barrier->mutex);
		if (advanced)
			break;
		ConditionVariableSleep(&barrier->condition_variable, wait_event_info);
	}
	ConditionVariableCancelSleep();

	return selected;
}
