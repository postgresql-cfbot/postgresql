/*-------------------------------------------------------------------------
 *
 * extension_lock.c
 *	  Relation extension lock manager
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 * src/backend/storage/lmgr/extension_lock.c
 *
 * NOTES:
 *
 * This lock manager is specialized in relation extension locks; light
 * weight and interruptible lock manager. It's similar to heavy-weight
 * lock but doesn't have dead lock detection mechanism and group locking
 * mechanism.
 *
 * For lock acquisition we use an atomic compare-and-exchange on the
 * state variable. When a process tries to acquire a lock that conflicts
 * with existing lock, it is put to sleep using condition variables
 * if not conditional locking. When release the lock, we use an atomic
 * decrement to release the lock, but don't remove the RELEXTLOCK entry
 * in the hash table. The all unused entries will be reclaimed when
 * acquisition once the hash table got full.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "storage/extension_lock.h"
#include "utils/rel.h"

/*
 * Compute the hash code associated with a RELEXTLOCK.
 *
 * To avoid unnecessary recomputations of the hash code, we try to do this
 * just once per function, and then pass it around as needed.  Aside from
 * passing the hashcode to hash_search_with_hash_value(), we can extract
 * the lock partition number from the hashcode.
 */
#define RelExtLockTargetTagHashCode(relextlocktargettag) \
	get_hash_value(RelExtLockHash, (const void *) relextlocktargettag)

/*
 * The lockmgr's shared hash tables are partitioned to reduce contention.
 * To determine which partition a given relid belongs to, compute the tag's
 * hash code with ExtLockTagHashCode(), then apply one of these macros.
 * NB: NUM_RELEXTLOCK_PARTITIONS must be a power of 2!
 */
#define RelExtLockHashPartition(hashcode) \
	((hashcode) % NUM_RELEXTLOCK_PARTITIONS)
#define RelExtLockHashPartitionLock(hashcode) \
	(&MainLWLockArray[RELEXTLOCK_MANAGER_LWLOCK_OFFSET + \
					  LockHashPartition(hashcode)].lock)
#define RelExtLockHashPartitionLockByIndex(i) \
	(&MainLWLockArray[RELEXTLOCK_MANAGER_LWLOCK_OFFSET + (i)].lock)

#define	RELEXT_VAL_EXCLUSIVE	((uint32) 1 << 24)
#define RELEXT_VAL_SHARED		1

#define RELEXT_LOCK_MASK			((uint32) ((1 << 25) - 1))

typedef struct RELEXTLOCK
{
	/* hash key -- must be first */
	Oid					relid;

	/* state of exclusive/non-exclusive lock */
	pg_atomic_uint32	state;
	pg_atomic_uint32	pin_counts;

	ConditionVariable	cv;
} RELEXTLOCK;

/*
 * This structure holds information per-object relation extension
 * lock. held_extlocks represents the RelExtLocks we're holding.
 * We use this structure to keep track of locked relation extension locks
 * for release during error recovery.  At most one lock can be held at
 * once. Note that sometimes we could try to acquire a lock for the
 * additional forks while holding the lock for the main fork; for example,
 * adding extra relation blocks for both relation and its free space map.
 * But since this lock manager doesn't distinguish between the forks,
 * we just increment nLocks in the case.
 */
typedef	struct relextlock_handle
{
	RELEXTLOCK		*lock;
	RelExtLockMode	mode;	/* lock mode for this table entry */
	int				nLocks;
} relextlock_handle;

static relextlock_handle held_relextlock;
static int num_held_relextlocks = 0;

static bool RelExtLockAcquire(Oid relid, RelExtLockMode lockmode, bool conditional);
static void RelExtLockRelease(Oid rleid);
static bool RelExtLockAttemptLock(RELEXTLOCK *extlock, RelExtLockMode lockmode);
static bool RelExtLockShrinkLocks(void);

/*
 * Pointers to hash tables containing relation extension lock state
 *
 * The RelExtLockHash hash table is in shared memory
 */
static HTAB *RelExtLockHash;

/*
 * InitRelExtLock
 *      Initialize the relation extension lock manager's data structures.
 */
void
InitRelExtLock(long max_table_size)
{
	HASHCTL	info;
	long		init_table_size;

	/*
	 * Compute init/max size to request for lock hashtables.  Note these
	 * calculations must agree with LockShmemSize!
	 */
	init_table_size = max_table_size / 2;

	/*
	 * Allocate hash table for RELEXTLOCK structs. This stores per-relation
	 * lock.
	 */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(RELEXTLOCK);
	info.num_partitions = NUM_RELEXTLOCK_PARTITIONS;

	RelExtLockHash = ShmemInitHash("RELEXTLOCK Hash",
								   init_table_size,
								   max_table_size,
								   &info,
								   HASH_ELEM | HASH_BLOBS | HASH_PARTITION);
}

/*
 *		LockRelationForExtension
 *
 * This lock is used to interlock addition of pages to relations.
 * We need such locking because bufmgr/smgr definition of P_NEW is not
 * race-condition-proof.
 *
 * We assume the caller is already holding some type of regular lock on
 * the relation, so no AcceptInvalidationMessages call is needed here.
 */
void
LockRelationForExtension(Relation relation, RelExtLockMode lockmode)
{
	RelExtLockAcquire(relation->rd_id, lockmode, false);
}

/*
 *		ConditionalLockRelationForExtension
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns TRUE iff the lock was acquired.
 */
bool
ConditionalLockRelationForExtension(Relation relation, RelExtLockMode lockmode)
{
	return RelExtLockAcquire(relation->rd_id, lockmode, true);
}

/*
 *		RelationExtensionLockWaiterCount
 *
 * Count the number of processes waiting for the given relation extension lock.
 */
int
RelationExtensionLockWaiterCount(Relation relation)
{
	LWLock		*partitionLock;
	RELEXTLOCK	*extlock;
	Oid			relid;
	uint32		hashcode;
	uint32		pin_counts;
	bool		found;

	relid = RelationGetRelid(relation);

	hashcode = RelExtLockTargetTagHashCode(&relid);
	partitionLock = RelExtLockHashPartitionLock(hashcode);

	LWLockAcquire(partitionLock, LW_SHARED);

	extlock = (RELEXTLOCK *) hash_search_with_hash_value(RelExtLockHash,
														 (void *) &relid,
														 hashcode,
														 HASH_FIND, &found);

	LWLockRelease(partitionLock);

	/* We assume that we already acquire this lock */
	Assert(found);

	pin_counts = pg_atomic_read_u32(&(extlock->pin_counts));

	/* Except for me */
	return pin_counts - 1;
}

/*
 *		UnlockRelationForExtension
 */
void
UnlockRelationForExtension(Relation relation)
{
	RelExtLockRelease(relation->rd_id);
}

/*
 * RelationExtensionLockReleaseAll - release all currently-held relation extension locks
 */
void
RelExtLockReleaseAll(void)
{
	if (num_held_relextlocks > 0)
	{
		HOLD_INTERRUPTS();
		RelExtLockRelease(held_relextlock.lock->relid);
	}
}

/*
 * Return the number of holding relation extension locks.
 */
int
RelExtLockHoldingLockCount(void)
{
	return num_held_relextlocks;
}

/*
 * Acquire relation extension lock and create RELEXTLOCK hash entry on shared
 * hash table. If we're trying to acquire the same lock as what already held,
 * we just increment nLock locally and return without touching the hash table.
 */
static bool
RelExtLockAcquire(Oid relid, RelExtLockMode lockmode, bool conditional)
{
	RELEXTLOCK	*extlock = NULL;
	LWLock	*partitionLock;
	uint32	hashcode;
	bool	found;
	bool	mustwait;

	hashcode = RelExtLockTargetTagHashCode(&relid);
	partitionLock = RelExtLockHashPartitionLock(hashcode);

	/*
	 * If we already hold the lock, we can just increase the count locally.
	 * Since we don't support dead lock detection for relation extension
	 * lock and don't control the order of lock acquisition, it cannot not
	 * happen that trying to take a new lock while holding an another lock.
	 */
	if (num_held_relextlocks > 0)
	{
		if (relid == held_relextlock.lock->relid &&
			lockmode == held_relextlock.mode)
		{
			held_relextlock.nLocks++;
			return true;
		}
		else
			Assert(false);	/* cannot happen */
	}

	for (;;)
	{

		LWLockAcquire(partitionLock, LW_EXCLUSIVE);

		if (!extlock)
			extlock = (RELEXTLOCK *) hash_search_with_hash_value(RelExtLockHash,
																 (void * ) &relid,
																 hashcode, HASH_ENTER_NULL,
																 &found);

		/*
		 * Failed to create new hash entry. Try to shrink the hash table and
		 * retry.
		 */
		if (!extlock)
		{
			bool	successed;
			LWLockRelease(partitionLock);
			successed = RelExtLockShrinkLocks();

			if (!successed)
				ereport(ERROR,
						(errmsg("out of shared memory"),
						 errhint("You might need to increase max_pred_locks_per_transaction.")));

			continue;
		}

		/* Not found, initialize */
		if (!found)
		{
			extlock->relid = relid;
			pg_atomic_init_u32(&(extlock->state), 0);
			pg_atomic_init_u32(&(extlock->pin_counts), 0);
			ConditionVariableInit(&(extlock->cv));
		}

		/* Increment pin count */
		pg_atomic_add_fetch_u32(&(extlock->pin_counts), 1);

		mustwait = RelExtLockAttemptLock(extlock, lockmode);

		if (!mustwait)
			break;	/* got the lock */

		/* Could not got the lock, return iff in conditional locking */
		if (mustwait && conditional)
		{
			pg_atomic_sub_fetch_u32(&(extlock->pin_counts), 1);
			LWLockRelease(partitionLock);
			return false;
		}

		/* Release the partition lock before sleep */
		LWLockRelease(partitionLock);

		/* Sleep until the lock is released */
		ConditionVariableSleep(&(extlock->cv), WAIT_EVENT_RELATION_EXTENSION);
	}

	LWLockRelease(partitionLock);
	ConditionVariableCancelSleep();

	Assert(!mustwait);

	/* Remember lock held by this backend */
	held_relextlock.lock = extlock;
	held_relextlock.mode = lockmode;
	held_relextlock.nLocks = 1;
	num_held_relextlocks++;

	/* Always return true if not conditional lock */
	return true;
}

/*
 * RelExtLockRelease
 *
 * Release a previously acquired relation extension lock. We don't remove
 * hash entry at the time. Once the hash table got full, all un-pinned hash
 * entries will be removed.
 */
static void
RelExtLockRelease(Oid relid)
{
	RELEXTLOCK	*extlock;
	RelExtLockMode mode;
	LWLock	*partitionLock;
	uint32	hashcode;
	uint32	pin_counts;

	/* We should have acquired a lock before releasing */
	Assert(num_held_relextlocks > 0);

	if (relid != held_relextlock.lock->relid)
		ereport(ERROR,
				(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
				 errmsg("relation extension lock for %u is not held",
						relid)));

	/* Decrease the lock count locally */
	held_relextlock.nLocks--;

	/* If we are still holding the lock, we're done */
	 if (held_relextlock.nLocks > 0)
		return;

	hashcode = RelExtLockTargetTagHashCode(&relid);
	partitionLock = RelExtLockHashPartitionLock(hashcode);

	/* Keep holding the partition lock until unlocking is done */
	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	extlock = held_relextlock.lock;
	mode = held_relextlock.mode;

	if (mode == RELEXT_EXCLUSIVE)
		pg_atomic_sub_fetch_u32(&(extlock->state), RELEXT_VAL_EXCLUSIVE);
	else
		pg_atomic_sub_fetch_u32(&(extlock->state), RELEXT_VAL_SHARED);

	num_held_relextlocks--;

	/* Decrement pin counter */
	pin_counts = pg_atomic_sub_fetch_u32(&(extlock->pin_counts), 1);

	LWLockRelease(partitionLock);

	/* Wake up waiters if there are someone looking at this lock */
	if (pin_counts > 0)
		ConditionVariableBroadcast(&(extlock->cv));
}

/*
 * Internal function that attempts to atomically acquire the relation
 * extension lock in the passed in mode.
 *
 * Returns true if the lock isn't free and we need to wait.
 */
static bool
RelExtLockAttemptLock(RELEXTLOCK *extlock, RelExtLockMode lockmode)
{
	uint32	oldstate;

	oldstate = pg_atomic_read_u32(&extlock->state);

	while (true)
	{
		uint32	desired_state;
		bool	lock_free;

		desired_state = oldstate;

		if (lockmode == RELEXT_EXCLUSIVE)
		{
			lock_free = (oldstate & RELEXT_LOCK_MASK) == 0;
			if (lock_free)
				desired_state += RELEXT_VAL_EXCLUSIVE;
		}
		else
		{
			lock_free = (oldstate & RELEXT_VAL_EXCLUSIVE) == 0;
			if (lock_free)
				desired_state += RELEXT_VAL_SHARED;
		}

		if (pg_atomic_compare_exchange_u32(&extlock->state,
										   &oldstate, desired_state))
		{
			if (lock_free)
				return false;
			else
				return true;
		}
	}
	pg_unreachable();
}

/*
 * Reclaim all un-pinned RELEXTLOCK entries from the hash table.
 */
static bool
RelExtLockShrinkLocks(void)
{
	HASH_SEQ_STATUS	hstat;
	RELEXTLOCK		*extlock;
	List			*entries_to_remove = NIL;
	ListCell		*cell;
	int				i;

	/*
	 * To ensure consistency, take all partition locks in exclusive
	 * mode.
	 */
	for (i = 0; i < NUM_RELEXTLOCK_PARTITIONS; i++)
		LWLockAcquire(RelExtLockHashPartitionLockByIndex(i), LW_EXCLUSIVE);

	/* Collect all un-pinned RELEXTLOCK entries */
	hash_seq_init(&hstat, RelExtLockHash);
	while ((extlock = (RELEXTLOCK *) hash_seq_search(&hstat)) != NULL)
	{
		uint32	pin_count = pg_atomic_read_u32(&(extlock->pin_counts));

		if (pin_count == 0)
			entries_to_remove = lappend(entries_to_remove, extlock);
	}

	/* We could not find any entries that we can remove right now */
	if (list_length(entries_to_remove) == 0)
		return false;

	/* Remove collected entries from RelExtLockHash has table */
	foreach (cell, entries_to_remove)
	{
		RELEXTLOCK	*el = (RELEXTLOCK *) lfirst(cell);
		uint32	hc = RelExtLockTargetTagHashCode(&(el->relid));

		hash_search_with_hash_value(RelExtLockHash, (void *) &(el->relid),
									hc, HASH_REMOVE, NULL);
	}

	/* Release all partition locks */
	for (i = 0; i < NUM_RELEXTLOCK_PARTITIONS; i++)
		LWLockRelease(RelExtLockHashPartitionLockByIndex(i));

	return true;
}
