/*-------------------------------------------------------------------------
 *
 * extension_lock.c
 *	  Relation extension lock manager
 *
 * This specialized lock manager is used only for relation extension
 * locks.  Unlike the heavyweight lock manager, it doesn't provide
 * deadlock detection or group locking.  Unlike lwlock.c, extension lock
 * waits are interruptible.  Unlike both systems, there is only one lock
 * mode.
 *
 * False sharing is possible.  We have a fixed-size array of locks, and
 * every database OID/relation OID combination is mapped to a slot in
 * the array.  Therefore, if two processes try to extend relations that
 * map to the same array slot, they will contend even though it would
 * be OK to let both proceed at once.  Since these locks are typically
 * taken only for very short periods of time, this doesn't seem likely
 * to be a big problem in practice.  If it is, we could make the array
 * bigger.
 *
 * The extension lock manager is much faster than the regular heavyweight
 * lock manager.  The lack of group locking is a feature, not a bug,
 * because while cooperating backends can all (for example) access a
 * relation on which they jointly hold AccessExclusiveLock at the same time,
 * it's not safe for them to extend the relation at the same time.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/backend/storage/lmgr/extension_lock.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"

#include "catalog/catalog.h"
#include "postmaster/postmaster.h"
#include "storage/extension_lock.h"
#include "utils/rel.h"

#define N_RELEXTLOCK_ENTS		1024

/*
 * We can't use bit 31 as the lock bit because pg_atomic_sub_fetch_u32 can't
 * handle an attempt to subtract INT_MIN.
 */
#define RELEXT_LOCK_BIT			((uint32) 1 << 30)
#define RELEXT_WAIT_COUNT_MASK	(RELEXT_LOCK_BIT - 1)

typedef struct RelExtLockTag
{
	Oid			dbid;			/* InvalidOid for a shared relation */
	Oid			relid;
} RelExtLockTag;

typedef struct RelExtLock
{
	pg_atomic_uint32 state;
	ConditionVariable cv;
} RelExtLock;

/*
 * Backend-private state for relation extension locks.  "relid" is the last
 * relation whose RelExtLock we looked up, and "lock" is a pointer to the
 * RelExtLock to which it mapped.  This speeds up the fairly common case where
 * we acquire the same relation extension lock repeatedly.  nLocks is 0 is the
 * number of times we've acquired that lock; 0 means we don't hold it, while
 * any value >0 means we do.
 *
 * A backend can't hold more than one relation extension lock at the same
 * time, although it can hold the same lock more than once.  Sometimes we try
 * to acquire a lock for additional forks while already holding the lock for
 * the main fork; for example, this might happen when adding extra relation
 * blocks for both relation and its free space map. But since this lock
 * manager doesn't distinguish between the forks, we just increment nLocks in
 * the case.
 */
typedef struct relextlock_handle
{
	Oid			relid;
	RelExtLock *lock;
	int			nLocks;			/* > 0 means holding it */
	bool		waiting;		/* true if we're waiting it */
} relextlock_handle;

static relextlock_handle held_relextlock;
static RelExtLock *RelExtLockArray;

static bool RelExtLockAcquire(Oid relid, bool conditional);
static bool RelExtLockAttemptLock(RelExtLock *relextlock);
static void RelExtLockRelease(void);
static inline uint32 RelExtLockTargetTagToIndex(RelExtLockTag *locktag);

/*
 * Estimate space required for a fixed-size array of RelExtLock structures.
 */
Size
RelExtLockShmemSize(void)
{
	return mul_size(N_RELEXTLOCK_ENTS, sizeof(RelExtLock));
}

/*
 * Initialize extension lock manager.
 */
void
InitRelExtLocks(void)
{
	bool		found;
	int			i;

	/* Verify that we have enough bits for maximum possible waiter count. */
	StaticAssertStmt(RELEXT_WAIT_COUNT_MASK >= MAX_BACKENDS,
					 "maximum waiter count of relation extension lock exceeds MAX_BACKENDS");

	RelExtLockArray = (RelExtLock *)
		ShmemInitStruct("Relation Extension Lock",
						N_RELEXTLOCK_ENTS * sizeof(RelExtLock),
						&found);

	/* we're the first - initialize */
	if (!found)
	{
		for (i = 0; i < N_RELEXTLOCK_ENTS; i++)
		{
			RelExtLock *relextlock = &RelExtLockArray[i];

			pg_atomic_init_u32(&(relextlock->state), 0);
			ConditionVariableInit(&(relextlock->cv));
		}
	}
}

/*
 * This lock is used to interlock addition of pages to relations.
 * We need such locking because bufmgr/smgr definition of P_NEW is not
 * race-condition-proof.
 *
 * We assume the caller is already holding some type of regular lock on
 * the relation, so no AcceptInvalidationMessages call is needed here.
 */
void
LockRelationForExtension(Relation relation)
{
	RelExtLockAcquire(RelationGetRelid(relation), false);
}

/*
 * As above, but only lock if we can get the lock without blocking.
 * Returns TRUE iff the lock was acquired.
 */
bool
ConditionalLockRelationForExtension(Relation relation)
{
	return RelExtLockAcquire(RelationGetRelid(relation), true);
}

/*
 * Estimate the number of processes waiting for the given relation extension
 * lock. Note that since multiple relations hash to the same RelExtLock entry,
 * the return value might be inflated.
 */
int
EstimateNumberOfExtensionLockWaiters(Relation relation)
{
	RelExtLockTag tag;
	RelExtLock *relextlock;
	uint32		state;
	Oid			relid = RelationGetRelid(relation);

	/* Make a lock tag */
	tag.dbid = IsSharedRelation(relid) ? InvalidOid : MyDatabaseId;
	tag.relid = relid;

	relextlock = &RelExtLockArray[RelExtLockTargetTagToIndex(&tag)];
	state = pg_atomic_read_u32(&(relextlock->state));

	return (state & RELEXT_WAIT_COUNT_MASK);
}

/*
 * Release a previously-acquired extension lock.
 */
void
UnlockRelationForExtension(Relation relation)
{
	Oid			relid = RelationGetRelid(relation);

	if (held_relextlock.nLocks <= 0 || relid != held_relextlock.relid)
	{
		elog(WARNING,
			 "relation extension lock for %u is not held",
			 relid);
		return;
	}

	/*
	 * If we acquired it multiple times, only change shared state when we have
	 * released it as many times as we acquired it.
	 */
	if (--held_relextlock.nLocks == 0)
		RelExtLockRelease();
}

/*
 * Release any extension lock held, and any wait count for an extension lock.
 * This is intended to be invoked during error cleanup.
 */
void
RelExtLockCleanup(void)
{
	if (held_relextlock.nLocks > 0)
	{
		/* Release the lock even if we acquired it multiple times. */
		held_relextlock.nLocks = 0;
		RelExtLockRelease();
		Assert(!held_relextlock.waiting);
	}
	else if (held_relextlock.waiting)
	{
		/* We were waiting for the lock; release the wait count we held. */
		held_relextlock.waiting = false;
		pg_atomic_sub_fetch_u32(&(held_relextlock.lock->state), 1);
	}
}

/*
 * Are we holding any extension lock?
 */
bool
IsAnyRelationExtensionLockHeld(void)
{
	return held_relextlock.nLocks > 0;
}

/*
 *		WaitForRelationExtensionLockToBeFree
 *
 * Wait for the relation extension lock on the given relation to
 * be free without acquiring it.
 */
void
WaitForRelationExtensionLockToBeFree(Relation relation)
{
	RelExtLock *relextlock;
	Oid			relid;

	relid = RelationGetRelid(relation);

	if (held_relextlock.nLocks > 0)
	{
		/*
		 * If we already hold the lock, nobody else does, so we can return
		 * immediately.
		 */
		if (relid == held_relextlock.relid)
			return;
		elog(ERROR,
			 "can only manipulate one relation extension lock at a time");
	}

	/*
	 * If the last relation extension lock we touched is the same one for
	 * which we now need to wait, we can use our cached pointer to the lock
	 * instead of recomputing it.
	 */
	if (relid == held_relextlock.relid)
		relextlock = held_relextlock.lock;
	else
	{
		RelExtLockTag tag;

		tag.dbid = IsSharedRelation(relid) ? InvalidOid : MyDatabaseId;
		tag.relid = relid;
		relextlock = &RelExtLockArray[RelExtLockTargetTagToIndex(&tag)];
		held_relextlock.relid = relid;
		held_relextlock.lock = relextlock;
	}

	for (;;)
	{
		uint32		state;

		state = pg_atomic_read_u32(&(relextlock)->state);

		/* Break if nobody is holding the lock on this relation */
		if ((state & RELEXT_LOCK_BIT) == 0)
			break;

		/* Could not get the lock, prepare to wait */
		if (!held_relextlock.waiting)
		{
			pg_atomic_add_fetch_u32(&(relextlock->state), 1);
			held_relextlock.waiting = true;
		}

		/* Sleep until something happens, then recheck */
		ConditionVariableSleep(&(relextlock->cv),
							   WAIT_EVENT_RELATION_EXTENSION_LOCK);
	}

	ConditionVariableCancelSleep();

	/* Release any wait count we hold */
	if (held_relextlock.waiting)
	{
		pg_atomic_sub_fetch_u32(&(relextlock->state), 1);
		held_relextlock.waiting = false;
	}
}

/*
 * Compute the hash code associated with a RelExtLock.
 */
static inline uint32
RelExtLockTargetTagToIndex(RelExtLockTag *locktag)
{
	return tag_hash(locktag, sizeof(RelExtLockTag)) % N_RELEXTLOCK_ENTS;
}

/*
 * Acquire a relation extension lock.
 */
static bool
RelExtLockAcquire(Oid relid, bool conditional)
{
	RelExtLock *relextlock;
	bool		mustwait;

	/*
	 * If we already hold the lock, we can just increase the count locally.
	 * Since we don't do deadlock detection, caller must not try to take a new
	 * relation extension lock while already holding them.
	 */
	if (held_relextlock.nLocks > 0)
	{
		if (relid != held_relextlock.relid)
			elog(ERROR,
				 "can only acquire one relation extension lock at a time");

		held_relextlock.nLocks++;
		return true;
	}

	/*
	 * If the last relation extension lock we touched is the same one for we
	 * now need to acquire, we can use our cached pointer to the lock instead
	 * of recomputing it.  This is likely to be a common case in practice.
	 */
	if (relid == held_relextlock.relid)
		relextlock = held_relextlock.lock;
	else
	{
		RelExtLockTag tag;

		/* Make a lock tag */
		tag.dbid = IsSharedRelation(relid) ? InvalidOid : MyDatabaseId;
		tag.relid = relid;

		relextlock = &RelExtLockArray[RelExtLockTargetTagToIndex(&tag)];

		/* Remember the lock we're interested in */
		held_relextlock.relid = relid;
		held_relextlock.lock = relextlock;
	}

	held_relextlock.waiting = false;
	for (;;)
	{
		mustwait = RelExtLockAttemptLock(relextlock);

		if (!mustwait)
			break;				/* got the lock */

		/* Could not got the lock, return iff in locking conditionally */
		if (conditional)
			return false;

		/* Could not get the lock, prepare to wait */
		if (!held_relextlock.waiting)
		{
			pg_atomic_add_fetch_u32(&(relextlock->state), 1);
			held_relextlock.waiting = true;
		}

		/* Sleep until something happens, then recheck */
		ConditionVariableSleep(&(relextlock->cv),
							   WAIT_EVENT_RELATION_EXTENSION_LOCK);
	}

	ConditionVariableCancelSleep();

	/* Release any wait count we hold */
	if (held_relextlock.waiting)
	{
		pg_atomic_sub_fetch_u32(&(relextlock->state), 1);
		held_relextlock.waiting = false;
	}

	Assert(!mustwait);

	/* Remember lock held by this backend */
	held_relextlock.relid = relid;
	held_relextlock.lock = relextlock;
	held_relextlock.nLocks = 1;

	/* We got the lock! */
	return true;
}

/*
 * Attempt to atomically acquire the relation extension lock.
 *
 * Returns true if the lock isn't free and we need to wait.
 */
static bool
RelExtLockAttemptLock(RelExtLock *relextlock)
{
	uint32		oldstate;

	oldstate = pg_atomic_read_u32(&relextlock->state);

	while (true)
	{
		bool		lock_free;

		lock_free = (oldstate & RELEXT_LOCK_BIT) == 0;

		if (!lock_free)
			return true;

		if (pg_atomic_compare_exchange_u32(&relextlock->state,
										   &oldstate,
										   oldstate | RELEXT_LOCK_BIT))
			return false;
	}

	pg_unreachable();
}

/*
 * Release extension lock in shared memory.  Should be called when our local
 * lock count drops to 0.
 */
static void
RelExtLockRelease(void)
{
	RelExtLock *relextlock;
	uint32		state;
	uint32		wait_counts;

	Assert(held_relextlock.nLocks == 0);

	relextlock = held_relextlock.lock;

	/* Release the lock */
	state = pg_atomic_sub_fetch_u32(&(relextlock->state), RELEXT_LOCK_BIT);

	/* If there may be waiters, wake them up */
	wait_counts = state & RELEXT_WAIT_COUNT_MASK;

	if (wait_counts > 0)
		ConditionVariableBroadcast(&(relextlock->cv));
}
