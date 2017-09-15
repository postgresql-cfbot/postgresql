/*-------------------------------------------------------------------------
 *
 * leader_gate.c
 *	  A deadlock avoidance mechanism for parallel execution.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * In PostgreSQL's implementation of parallel query execution, leader
 * processes have two separate roles.  They are responsible for reading from
 * worker queues, but they may also choose to execute the same plan as workers
 * (see Gather and Gather Merge nodes).  We must therefore never allow a
 * leader to wait for workers with a synchronous Barrier, ConditionVariable,
 * Latch or other synchronization mechanism if there is any possibility that
 * those workers have emitted tuples.  Otherwise we could get into a situation
 * where a worker fills up its output tuple queue and begins waiting for the
 * leader to read from the queue to make space, while the leader is blocked
 * waiting for the worker inside an executor node.
 *
 * Longer term solutions to this problem involving asynchronous execution
 * might eventually be developed, but in the short term this gating mechanism
 * provides a way for the leader to bow out of parallel execution at a safe
 * time, while also making sure that it can't decide to bow out if no workers
 * have shown up.  In other words, this is a gate that allows only the leader
 * OR the workers through, but never both and never none.
 *
 * It works by allowing the participating processes to reach consensus by
 * holding a simple two-step election.  First, each participant must call
 * LeaderGateAttach(gate) in order to register to vote.  Next, at a point in
 * time where a participant might have emitted tuples but before it waits for
 * peers, it must call LeaderGateCanContinue(gate) to check if it is allowed
 * to continue.
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/leader_gate.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/parallel.h"
#include "utils/leader_gate.h"

/*
 * Initialize a LeaderGate.
 */
void
LeaderGateInit(LeaderGate *gate)
{
	SpinLockInit(&gate->mutex);
	gate->at_least_one_worker_attached = false;
	gate->leader_attached = false;
	gate->state = LG_UNDECIDED;
}

/*
 * Attach to a LeaderGate.  This must be called before LeaderGateCanContinue,
 * and as early as possible to maximize the likelihood of a positive outcome
 * for workers.  Any process that calls LeaderGateAttach *must* eventually
 * call LeaderGateCanContinue.
 */
void
LeaderGateAttach(LeaderGate *gate)
{
	SpinLockAcquire(&gate->mutex);
	if (IsParallelWorker())
		gate->at_least_one_worker_attached = true;
	else
	{
		Assert(!gate->leader_attached);
		gate->leader_attached = true;
	}
	SpinLockRelease(&gate->mutex);
}

/*
 * Decide whether the caller can continue.  The caller must previously have
 * called LeaderGateAttach.  Returns true in the leader and false in all
 * workers, or the opposite.
 */
bool
LeaderGateCanContinue(LeaderGate *gate)
{
	bool result;

	SpinLockAcquire(&gate->mutex);
	if (IsParallelWorker())
	{
		/* We are running in a worker process. */
		Assert(gate->at_least_one_worker_attached);
		switch (gate->state)
		{
		case LG_LEADER_CONTINUES:
			/*
			 * The leader has already decided to continue, so all workers must
			 * stop.
			 */
			result = false;
			break;
		case LG_WORKERS_CONTINUE:
			/*
			 * A fellow worker has already decided that all workers can
			 * continue.
			 */
			result = true;
			break;
		case LG_UNDECIDED:
			/*
			 * We know that at least one worker is running (me), so continue
			 * and record this decision so that the leader will see it.
			 */
			gate->state = LG_WORKERS_CONTINUE;
			result = true;
			break;
		}
	}
	else
	{
		/* We are running in the leader process. */
		Assert(gate->leader_attached);
		switch (gate->state)
		{
		case LG_LEADER_CONTINUES:
			/*
			 * The leader should never see this state because the only way it
			 * can be reached is by the leader and we can only call this
			 * function once.
			 */
			Assert(false);
			result = true;
			break;
		case LG_WORKERS_CONTINUE:
			/* The workers have already decided to continue. */
			result = false;
			break;
		case LG_UNDECIDED:
			/*
			 * If any workers have reported for duty, the leader stops.
			 * Otherwise it seizes control, because we have to make sure that
			 * SOMEONE continues.  Any workers that arrive after this have
			 * arrived too late and will stop.
			 */
			if (gate->at_least_one_worker_attached)
			{
				gate->state = LG_WORKERS_CONTINUE;
				result = false;
			}
			else
			{
				gate->state = LG_LEADER_CONTINUES;
				result = true;
			}
			break;
		}

	}
	SpinLockRelease(&gate->mutex);

	return result;
}
