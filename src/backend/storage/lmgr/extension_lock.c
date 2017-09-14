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
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "pg_trace.h"
#include "postmaster/postmaster.h"
#include "replication/slot.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/proclist.h"
#include "storage/spin.h"
#include "storage/extension_lock.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#ifdef LWLOCK_STATS
#include "utils/hsearch.h"
#endif

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
 * NB: NUM_RELEXTENSIONLOCK_PARTITIONS must be a power of 2!
 */
#define RelExtLockHashPartition(hashcode) \
	((hashcode) % NUM_RELEXTLOCK_PARTITIONS)
#define RelExtLockHashPartitionLock(hashcode) \
	(&MainLWLockArray[RELEXTLOCK_MANAGER_LWLOCK_OFFSET + \
					  LockHashPartition(hashcode)].lock)
#define RelExtLockHashPartitionLockByIndex(i) \
	(&MainLWLockArray[RELEXTLOCK_MANAGER_LWLOCK_OFFSET + (i)].lock

#define	RELEXT_VAL_EXCLUSIVE	((uint32) 1 << 24)
#define RELEXT_VAL_SHARED		1

#define RELEXT_LOCKMASK			((uint32) ((1 << 25) - 1))

/* */
#define MAX_SIMUL_EXTLOCKS 8

/*
 * This structure holds information per-object relation extension
 * lock. held_extlocks represents the ExtLocks we're holding.
 */
typedef	struct relextlock_handle
{
	Oid			relid;
	RelExtLock	*lock;
	RelExtLockMode mode;	/* lock mode for this table entry */
} relextlock_handle;
static relextlock_handle held_relextlocks[MAX_SIMUL_EXTLOCKS];
static int num_held_relextlocks = 0;

static bool RelExtLockAcquire(Oid relid, RelExtLockMode lockmode, bool conditional);
static void RelExtLockRelease(Oid rleid, RelExtLockMode lockmode);
static bool RelExtLockAttemptLock(RelExtLock *ext_lock, RelExtLockMode lockmode);

/*
 * Pointers to hash tables containing lock state
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
	info.entrysize = sizeof(RelExtLock);
	info.num_partitions = NUM_RELEXTLOCK_PARTITIONS;

	RelExtLockHash = ShmemInitHash("RelExtLock Hash",
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
 * NOte that this routine doesn't acquire the partition lock. Please make sure
 * that the caller must acquire partitionlock in exclusive mode or we must call
 * this routine after acquired the relation extension lock of this relation.
 */
int
RelationExtensionLockWaiterCount(Relation relation)
{
	RelExtLock	*ext_lock;
	Oid		relid;
	uint32	nwaiters;
	uint32	hashcode;
	bool	found;

	relid = relation->rd_id;
	hashcode = RelExtLockTargetTagHashCode(&relid);

	ext_lock = (RelExtLock *) hash_search_with_hash_value(RelExtLockHash,
														  (void *) &relid,
														  hashcode,
														  HASH_FIND, &found);
	/* We assume that we already acquire this lock */
	Assert(found);

	nwaiters = pg_atomic_read_u32(&(ext_lock->nwaiters));

	return nwaiters;
}

/*
 *		UnlockRelationForExtension
 */
void
UnlockRelationForExtension(Relation relation, RelExtLockMode lockmode)
{
	RelExtLockRelease(relation->rd_id, lockmode);
}

/*
 * Acquire relation extension lock and create RELEXTLOCK hash entry on shared
 * hash table. To avoid dead-lock with partition lock and LWLock, we acquire
 * them but don't release it here. The caller must call DeleteRelExtLock later
 * to release these locks.
 */
static bool
RelExtLockAcquire(Oid relid, RelExtLockMode lockmode, bool conditional)
{
	RelExtLock	*ext_lock;
	LWLock	*partitionLock;
	uint32	hashcode;
	bool	found;
	bool	got_lock = false;
	bool	waited = false;

	hashcode = RelExtLockTargetTagHashCode(&relid);
	partitionLock = RelExtLockHashPartitionLock(hashcode);
	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	ext_lock = (RelExtLock *) hash_search_with_hash_value(RelExtLockHash,
														  (void * ) &relid,
														  hashcode, HASH_ENTER, &found);

	if (!ext_lock)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of shared memory"),
				 errhint("You might need to increase max_pred_locks_per_transaction.")));

	for (;;)
	{
		bool ret;

		ret = RelExtLockAttemptLock(ext_lock, lockmode);

		if (ret)
		{
			got_lock = true;

			if (waited)
				pg_atomic_sub_fetch_u32(&(ext_lock->nwaiters), 1);

			break;	/* got the lock */
		}

		/* Could not get lock, return if in conditional lock */
		if (!ret && conditional)
			break;

		/* Add to wait list */
		pg_atomic_add_fetch_u32(&(ext_lock->nwaiters), 1);
		ConditionVariableSleep(&(ext_lock->cv), WAIT_EVENT_RELATION_EXTENSION);
	}

	ConditionVariableCancelSleep();

	if (got_lock)
	{
		/* Add lock to list relation extension locks held by this backend */
		held_relextlocks[num_held_relextlocks].relid = relid;
		held_relextlocks[num_held_relextlocks].lock = ext_lock;
		held_relextlocks[num_held_relextlocks].mode = lockmode;
		num_held_relextlocks++;
	}
	else
		LWLockRelease(partitionLock);

	/* Always end up with true if not conditional lock */
	return got_lock;
}

/*
 * RelationExtensionLockReleaseAll - release all currently-held relation extension locks
 */
void
RelationExtensionLockReleaseAll(void)
{
	while (num_held_relextlocks > 0)
	{
		HOLD_INTERRUPTS();

		RelExtLockRelease(held_relextlocks[num_held_relextlocks - 1].relid,
						  held_relextlocks[num_held_relextlocks - 1].mode);
	}
}

/*
 * ExstLockRelease
 *
 * Remove RELEXTLOCK from shared RelExtLockHash hash table. Since other backends
 * might be acquiring it or waiting for this lock, we can delete it only if there
 * is no longer backends who are interested in it.
 *
 * Note that we assume partition lock for hash table is already acquired when
 * acquiring the lock. This routine should release partition lock as well after
 * released LWLock.
 */
static void
RelExtLockRelease(Oid relid, RelExtLockMode lockmode)
{
	RelExtLock	*ext_lock;
	RelExtLockMode mode;
	uint32	hashcode;
	LWLock	*partitionLock;
	uint32	oldstate;
	uint32	nwaiters;
	int i;

	hashcode = RelExtLockTargetTagHashCode(&relid);
	partitionLock = RelExtLockHashPartitionLock(hashcode);

	for (i = num_held_relextlocks; --i >= 0;)
		if (relid == held_relextlocks[i].relid &&
			lockmode == held_relextlocks[i].mode)
			break;

	if (i < 0)
		elog(ERROR, "relation extension lock for %u with lock mode %d is not held",
			 relid, lockmode);

	ext_lock = held_relextlocks[i].lock;
	mode = held_relextlocks[i].mode;

	num_held_relextlocks--;

	/* Shrink */
	for (; i < num_held_relextlocks; i++)
		held_relextlocks[i] = held_relextlocks[i + 1];

	if (mode == RELEXT_EXCLUSIVE)
		oldstate = pg_atomic_sub_fetch_u32(&(ext_lock->state), RELEXT_VAL_EXCLUSIVE);
	else
		oldstate = pg_atomic_sub_fetch_u32(&(ext_lock->state), RELEXT_VAL_SHARED);

	nwaiters = pg_atomic_read_u32(&(ext_lock->nwaiters));

	/* Wake up waiters if there are */
	if (nwaiters > 0)
		ConditionVariableBroadcast(&(ext_lock->cv));
	else
		hash_search_with_hash_value(RelExtLockHash, (void *) &relid,
									hashcode, HASH_REMOVE, NULL);

	LWLockRelease(partitionLock);
}

/*
 * Internal function that tries to atomically acquire the relation extension
 * lock in the passed in mode. Return true if we got the lock.
 */
static bool
RelExtLockAttemptLock(RelExtLock *ext_lock, RelExtLockMode lockmode)
{
	uint32	oldstate;

	oldstate = pg_atomic_read_u32(&ext_lock->state);

	while (true)
	{
		uint32	desired_state;
		bool	lock_free;

		desired_state = oldstate;

		if (lockmode == RELEXT_EXCLUSIVE)
		{
			lock_free = (oldstate & RELEXT_LOCKMASK) == 0;
			if (lock_free)
				desired_state += RELEXT_VAL_EXCLUSIVE;
		}
		else
		{
			lock_free = (oldstate & RELEXT_VAL_EXCLUSIVE) == 0;
			if (lock_free)
				desired_state += RELEXT_VAL_SHARED;
		}

		if (pg_atomic_compare_exchange_u32(&ext_lock->state,
										   &oldstate, desired_state))
		{
			if (lock_free)
				return true;
			else
				return false;
		}
	}
	pg_unreachable();
}
