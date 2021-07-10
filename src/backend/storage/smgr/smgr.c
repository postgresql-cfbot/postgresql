/*-------------------------------------------------------------------------
 *
 * smgr.c
 *	  public interface routines to storage manager switch.
 *
 *	  All file system operations in POSTGRES dispatch through these
 *	  routines.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/smgr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "lib/ilist.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "port/pg_bitutils.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/shmem.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/md.h"
#include "storage/smgr.h"
#include "utils/hsearch.h"
#include "utils/inval.h"

/*
 * An entry in the hash table that allows us to look up objects in the
 * SMgrSharedRelation pool by rnode (+ backend).
 */
typedef struct SMgrSharedRelationMapping
{
	RelFileNodeBackend rnode;
	int				index;
} SMgrSharedRelationMapping;

/*
 * An object in shared memory tracks the size of the forks of a relation.
 */
struct SMgrSharedRelation
{
	RelFileNodeBackend rnode;
	BlockNumber		nblocks[MAX_FORKNUM + 1];
	pg_atomic_uint32 flags;
	pg_atomic_uint64 generation;		/* mapping change */
	int64 usecount;     /* used for clock sweep */
};

/* For now, we borrow the buffer managers array of locks.  XXX fixme */
#define SR_PARTITIONS NUM_BUFFER_PARTITIONS
#define SR_PARTITION_LOCK(hash) (&MainLWLockArray[BUFFER_MAPPING_LWLOCK_OFFSET].lock)

/* Flags. */
#define SR_LOCKED					0x01
#define SR_VALID					0x02

/* Each forknum gets its own dirty, syncing and just dirtied bits. */
#define SR_DIRTY(forknum)			(0x04 << ((forknum) + (MAX_FORKNUM + 1) * 0))
#define SR_SYNCING(forknum)			(0x04 << ((forknum) + (MAX_FORKNUM + 1) * 1))
#define SR_JUST_DIRTIED(forknum)	(0x04 << ((forknum) + (MAX_FORKNUM + 1) * 2))

/* Masks to test if any forknum is currently dirty or syncing. */
#define SR_SYNCING_MASK				(((SR_SYNCING(MAX_FORKNUM + 1) - 1) ^ (SR_SYNCING(0) - 1)))
#define SR_DIRTY_MASK				(((SR_DIRTY(MAX_FORKNUM + 1) - 1) ^ (SR_DIRTY(0) - 1)))

/* Extract the lowest dirty forknum from flags (there must be at least one). */
#define SR_GET_ONE_DIRTY(mask)		pg_rightmost_one_pos32((((mask) >> 2) & (SR_DIRTY_MASK >> 2)))

typedef struct SMgrSharedRelationPool
{
	ConditionVariable sync_flags_cleared;
	pg_atomic_uint32 next;
	SMgrSharedRelation objects[FLEXIBLE_ARRAY_MEMBER];
} SMgrSharedRelationPool;

static SMgrSharedRelationPool *sr_pool;
static HTAB *sr_mapping_table;

/*
 * This struct of function pointers defines the API between smgr.c and
 * any individual storage manager module.  Note that smgr subfunctions are
 * generally expected to report problems via elog(ERROR).  An exception is
 * that smgr_unlink should use elog(WARNING), rather than erroring out,
 * because we normally unlink relations during post-commit/abort cleanup,
 * and so it's too late to raise an error.  Also, various conditions that
 * would normally be errors should be allowed during bootstrap and/or WAL
 * recovery --- see comments in md.c for details.
 */
typedef struct f_smgr
{
	void		(*smgr_init) (void);	/* may be NULL */
	void		(*smgr_shutdown) (void);	/* may be NULL */
	void		(*smgr_open) (SMgrRelation reln);
	void		(*smgr_close) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_create) (SMgrRelation reln, ForkNumber forknum,
								bool isRedo);
	bool		(*smgr_exists) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_unlink) (RelFileNodeBackend rnode, ForkNumber forknum,
								bool isRedo);
	void		(*smgr_extend) (SMgrRelation reln, ForkNumber forknum,
								BlockNumber blocknum, char *buffer, bool skipFsync);
	bool		(*smgr_prefetch) (SMgrRelation reln, ForkNumber forknum,
								  BlockNumber blocknum);
	void		(*smgr_read) (SMgrRelation reln, ForkNumber forknum,
							  BlockNumber blocknum, char *buffer);
	void		(*smgr_write) (SMgrRelation reln, ForkNumber forknum,
							   BlockNumber blocknum, char *buffer, bool skipFsync);
	void		(*smgr_writeback) (SMgrRelation reln, ForkNumber forknum,
								   BlockNumber blocknum, BlockNumber nblocks);
	BlockNumber (*smgr_nblocks) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_truncate) (SMgrRelation reln, ForkNumber forknum,
								  BlockNumber nblocks);
	void		(*smgr_immedsync) (SMgrRelation reln, ForkNumber forknum);
} f_smgr;

static const f_smgr smgrsw[] = {
	/* magnetic disk */
	{
		.smgr_init = mdinit,
		.smgr_shutdown = NULL,
		.smgr_open = mdopen,
		.smgr_close = mdclose,
		.smgr_create = mdcreate,
		.smgr_exists = mdexists,
		.smgr_unlink = mdunlink,
		.smgr_extend = mdextend,
		.smgr_prefetch = mdprefetch,
		.smgr_read = mdread,
		.smgr_write = mdwrite,
		.smgr_writeback = mdwriteback,
		.smgr_nblocks = mdnblocks,
		.smgr_truncate = mdtruncate,
		.smgr_immedsync = mdimmedsync,
	}
};

static const int NSmgr = lengthof(smgrsw);

/*
 * Each backend has a hashtable that stores all extant SMgrRelation objects.
 * In addition, "unowned" SMgrRelation objects are chained together in a list.
 */
static HTAB *SMgrRelationHash = NULL;

static dlist_head unowned_relns;

/* local function prototypes */
static void smgrshutdown(int code, Datum arg);

/* GUCs. */
int smgr_shared_relations = 10000;
int smgr_pool_sweep_times = 32;

/*
 * Try to get the size of a relation's fork without locking.
 */
static BlockNumber
smgrnblocks_fast(SMgrRelation reln, ForkNumber forknum)
{
	SMgrSharedRelation *sr = reln->smgr_shared;
	BlockNumber result;

	if (sr)
	{
		pg_read_barrier();

		/* We can load int-sized values atomically without special measures. */
		Assert(sizeof(sr->nblocks[forknum]) == sizeof(uint32));
		result = sr->nblocks[forknum];

		/*
		 * With a read barrier between the loads, we can check that the object
		 * still refers to the same rnode before trusting the answer.
		 */
		pg_read_barrier();

		if (pg_atomic_read_u64(&sr->generation) == reln->smgr_shared_generation)
		{
			/* no necessary to use a atomic operation, usecount can be imprecisely */
			sr->usecount++;
			return result;
		}

		/*
		 * The generation doesn't match, the shared relation must have been
		 * evicted since we got a pointer to it.  We'll need to do more work.
		 */
	}

	return InvalidBlockNumber;
}

/*
 * Try to get the size of a relation's fork by looking it up in the mapping
 * table with a shared lock.  This will succeed if the SMgrRelation already
 * exists.
 */
static BlockNumber
smgrnblocks_shared(SMgrRelation reln, ForkNumber forknum)
{
	SMgrSharedRelationMapping *mapping;
	SMgrSharedRelation *sr;
	uint32	hash;
	LWLock *mapping_lock;
	BlockNumber result = InvalidBlockNumber;

	hash = get_hash_value(sr_mapping_table, &reln->smgr_rnode);
	mapping_lock = SR_PARTITION_LOCK(hash);

	LWLockAcquire(mapping_lock, LW_SHARED);
	mapping = hash_search_with_hash_value(sr_mapping_table,
										  &reln->smgr_rnode,
										  hash,
										  HASH_FIND,
										  NULL);
	if (mapping)
	{
		sr = &sr_pool->objects[mapping->index];
		result = sr->nblocks[forknum];

		/* no necessary to use a atomic operation, usecount can be imprecisely */
		sr->usecount++;

		/* We can take the fast path until this SR is eventually evicted. */
		reln->smgr_shared = sr;
		reln->smgr_shared_generation = pg_atomic_read_u64(&sr->generation);
	}
	LWLockRelease(mapping_lock);

	return result;
}

/*
 * Lock a SMgrSharedRelation.  The lock is a spinlock that should be held for
 * only a few instructions.  The return value is the current set of flags,
 * which may be modified and then passed to smgr_unlock_sr() to be atomically
 * when the lock is released.
 */
static uint32
smgr_lock_sr(SMgrSharedRelation *sr)
{
	SpinDelayStatus spin_delay_status;

	init_local_spin_delay(&spin_delay_status);

	for (;;)
	{
		uint32	old_flags = pg_atomic_read_u32(&sr->flags);
		uint32	flags;

		if (!(old_flags & SR_LOCKED))
		{
			flags = old_flags | SR_LOCKED;
			if (pg_atomic_compare_exchange_u32(&sr->flags, &old_flags, flags))
				return flags;
		}

		perform_spin_delay(&spin_delay_status);
	}
	return 0; /* unreachable */
}

/*
 * Unlock a SMgrSharedRelation, atomically updating its flags at the same
 * time.
 */
static void
smgr_unlock_sr(SMgrSharedRelation *sr, uint32 flags)
{
	pg_write_barrier();
	pg_atomic_write_u32(&sr->flags, flags & ~SR_LOCKED);
}

/* LRU: sweep to find a sr to use. Just lock the sr when it returns */
static SMgrSharedRelation *
smgr_pool_sweep(void)
{
	SMgrSharedRelation *sr;
	uint32 index;
	uint32 flags;
	int sr_used_count = 0;

	for (;;)
	{
		/* Lock the next one in clock-hand order. */
		index = pg_atomic_fetch_add_u32(&sr_pool->next, 1) % smgr_shared_relations;
		sr = &sr_pool->objects[index];
		flags = smgr_lock_sr(sr);
		if (--(sr->usecount) <= 0)
		{
			elog(DEBUG5, "find block cache in sweep cache, use it");
			return sr;
		}
		if (++sr_used_count >= smgr_shared_relations * smgr_pool_sweep_times)
		{
			elog(LOG, "all the block caches are used frequently, use a random one");
			sr = &sr_pool->objects[random() % smgr_shared_relations];
			return sr;
		}
		smgr_unlock_sr(sr, flags);
	}
}

/*
 * Allocate a new invalid SMgrSharedRelation, and return it locked.
 *
 * The replacement algorithm is a simple FIFO design with no second chance for
 * now.
 */
static SMgrSharedRelation *
smgr_alloc_sr(void)
{
	SMgrSharedRelationMapping *mapping;
	SMgrSharedRelation *sr;
	uint32 index;
	LWLock *mapping_lock;
	uint32 flags;
	RelFileNodeBackend rnode;
	uint32 hash;

 retry:
	sr = smgr_pool_sweep();
	flags = pg_atomic_read_u32(&sr->flags);
	/* If it's unused, can return it, still locked, immediately. */
	if (!(flags & SR_VALID))
		return sr;

	/*
	 * Copy the rnode and unlock.  We'll briefly acquire both mapping and SR
	 * locks, but we need to do it in that order, so we'll unlock the SR
	 * first.
	 */
	index = sr - sr_pool->objects;
	rnode = sr->rnode;
	smgr_unlock_sr(sr, flags);

	hash = get_hash_value(sr_mapping_table, &rnode);
	mapping_lock = SR_PARTITION_LOCK(hash);

	LWLockAcquire(mapping_lock, LW_EXCLUSIVE);
	mapping = hash_search_with_hash_value(sr_mapping_table,
										  &rnode,
										  hash,
										  HASH_FIND,
										  NULL);
	if (!mapping || mapping->index != index)
	{
		/* Too slow, it's gone or now points somewhere else.  Go around. */
		LWLockRelease(mapping_lock);
		goto retry;
	}

	/* We will lock the SR for just a few instructions. */
	flags = smgr_lock_sr(sr);
	Assert(flags & SR_VALID);

	/*
	 * If another backend is currently syncing any fork, we aren't allowed to
	 * evict it, and waiting for it would be pointless because that other
	 * backend already plans to allocate it.  So go around.
	 */
	if (flags & SR_SYNCING_MASK)
	{
		smgr_unlock_sr(sr, flags);
		LWLockRelease(mapping_lock);
		goto retry;
	}

	/*
	 * We will sync every fork that is dirty, and then we'll try to
	 * evict it.
	 */
	while (flags & SR_DIRTY_MASK)
	{
		SMgrRelation reln;
		ForkNumber forknum = SR_GET_ONE_DIRTY(flags);

		/* Set the sync bit, clear the just-dirtied bit and unlock. */
		flags |= SR_SYNCING(forknum);
		flags &= ~SR_JUST_DIRTIED(forknum);
		smgr_unlock_sr(sr, flags);
		LWLockRelease(mapping_lock);

		/*
		 * Perform the I/O, with no locks held.
		 * XXX It sucks that we fsync every segment, not just the ones that need it...
		 */
		reln = smgropen(rnode.node, rnode.backend);
		smgrimmedsync(reln, forknum);

		/*
		 * Reacquire the locks.  The object can't have been evicted,
		 * because we set a sync bit.
		 */
		LWLockAcquire(mapping_lock, LW_EXCLUSIVE);
		flags = smgr_lock_sr(sr);
		Assert(flags & SR_SYNCING(forknum));
		flags &= ~SR_SYNCING(forknum);
		if (flags & SR_JUST_DIRTIED(forknum))
		{
			/*
			 * Someone else dirtied it while we were syncing, so we can't mark
			 * it clean.  Let's give up on this SR and go around again.
			 */
			smgr_unlock_sr(sr, flags);
			LWLockRelease(mapping_lock);
			goto retry;
		}

		/* This fork is clean! */
		flags &= ~SR_DIRTY(forknum);
	}

	/*
	 * If we made it this far, there are no dirty forks, so we're now allowed
	 * to evict the SR from the pool and the mapping table.  Make sure that
	 * smgrnblocks_fast() sees that its pointer is now invalid by bumping the
	 * generation.
	 */
	flags &= ~SR_VALID;
	pg_atomic_write_u64(&sr->generation,
						pg_atomic_read_u64(&sr->generation) + 1);
	pg_write_barrier();
	smgr_unlock_sr(sr, flags);

	/*
	 * If any callers to smgr_sr_drop() or smgr_sr_drop_db() had the misfortune
	 * to have to wait for us to finish syncing, we can now wake them up.
	 */
	ConditionVariableBroadcast(&sr_pool->sync_flags_cleared);

	/* Remove from the mapping table. */
	hash_search_with_hash_value(sr_mapping_table,
								&rnode,
								hash,
								HASH_REMOVE,
								NULL);
	LWLockRelease(mapping_lock);

	/*
	 * XXX: We unlock while doing HASH_REMOVE on principle.  Maybe it'd be OK
	 * to hold it now that the clock hand is far away and there is no way
	 * anyone can look up this SR through buffer mapping table.
	 */
	flags = smgr_lock_sr(sr);
	if (flags & SR_VALID)
	{
		/* Oops, someone else got it. */
		smgr_unlock_sr(sr, flags);
		goto retry;
	}

	return sr;
}

/*
 * Set the number of blocks in a relation, in shared memory, and optionally
 * also mark the relation as "dirty" (meaning the it must be fsync'd before it
 * can be evicted).
 */
static void
smgrnblocks_update(SMgrRelation reln,
				   ForkNumber forknum,
				   BlockNumber nblocks,
				   bool mark_dirty)
{
	SMgrSharedRelationMapping *mapping;
	SMgrSharedRelation *sr = NULL;
	uint32		hash;
	LWLock *mapping_lock;
	uint32 flags;

	hash = get_hash_value(sr_mapping_table, &reln->smgr_rnode);
	mapping_lock = SR_PARTITION_LOCK(hash);

 retry:
	LWLockAcquire(mapping_lock, LW_SHARED);
	mapping = hash_search_with_hash_value(sr_mapping_table,
										  &reln->smgr_rnode,
										  hash,
										  HASH_FIND,
										  NULL);
	if (mapping)
	{
		sr = &sr_pool->objects[mapping->index];
		flags = smgr_lock_sr(sr);
		if (mark_dirty)
		{
			/*
			 * Extend and truncate clobber the value, and there are no races
			 * to worry about because they can have higher level exclusive
			 * locking on the relation.
			 */
			sr->nblocks[forknum] = nblocks;

			/*
			 * Mark it dirty, and if it's currently being sync'd, make sure it
			 * stays dirty after that completes.
			 */
			flags |= SR_DIRTY(forknum);
			if (flags & SR_SYNCING(forknum))
				flags |= SR_JUST_DIRTIED(forknum);
		}
		else if (!(flags & SR_DIRTY(forknum)))
		{
			/*
			 * We won't clobber a dirty value with a non-dirty update, to
			 * avoid races against concurrent extend/truncate, but we can
			 * install a new clean value.
			 */
			sr->nblocks[forknum] = nblocks;
		}
		if (sr->usecount < smgr_pool_sweep_times)
			sr->usecount++;
		smgr_unlock_sr(sr, flags);
	}
	LWLockRelease(mapping_lock);

	/* If we didn't find it, then we'll need to allocate one. */
	if (!sr)
	{
		bool found;

		sr = smgr_alloc_sr();

		/* Upgrade to exclusive lock so we can create a mapping. */
		LWLockAcquire(mapping_lock, LW_EXCLUSIVE);
		mapping = hash_search_with_hash_value(sr_mapping_table,
											  &reln->smgr_rnode,
											  hash,
											  HASH_ENTER,
											  &found);
		if (!found)
		{
			/* Success!  Initialize. */
			mapping->index = sr - sr_pool->objects;
			sr->usecount = 1;
			smgr_unlock_sr(sr, SR_VALID);
			sr->rnode = reln->smgr_rnode;
			pg_atomic_write_u64(&sr->generation,
								pg_atomic_read_u64(&sr->generation) + 1);
			for (int i = 0; i <= MAX_FORKNUM; ++i)
				sr->nblocks[i] = InvalidBlockNumber;
			LWLockRelease(mapping_lock);
		}
		else
		{
			/* Someone beat us to it.  Go around again. */
			smgr_unlock_sr(sr, 0);		/* = not valid */
			LWLockRelease(mapping_lock);
			goto retry;
		}
	}
}

static void
smgr_drop_sr(RelFileNodeBackend *rnode)
{
	SMgrSharedRelationMapping *mapping;
	SMgrSharedRelation *sr;
	uint32	hash;
	LWLock *mapping_lock;
	uint32 flags;

	hash = get_hash_value(sr_mapping_table, rnode);
	mapping_lock = SR_PARTITION_LOCK(hash);

retry:
	LWLockAcquire(mapping_lock, LW_EXCLUSIVE);
	mapping = hash_search_with_hash_value(sr_mapping_table,
										  rnode,
										  hash,
										  HASH_FIND,
										  NULL);
	if (mapping)
	{
		sr = &sr_pool->objects[mapping->index];

		flags = smgr_lock_sr(sr);
		Assert(flags & SR_VALID);

		if (flags & SR_SYNCING_MASK)
		{
			/*
			 * Oops, someone's syncing one of its forks; nothing to do but
			 * wait until that's finished.
			 */
			smgr_unlock_sr(sr, flags);
			LWLockRelease(mapping_lock);
			ConditionVariableSleep(&sr_pool->sync_flags_cleared,
								   WAIT_EVENT_SMGR_DROP_SYNC);
			goto retry;
		}
		ConditionVariableCancelSleep();

		/* Make sure smgrnblocks_fast() knows it's invalidated. */
		pg_atomic_write_u64(&sr->generation,
							pg_atomic_read_u64(&sr->generation) + 1);
		pg_write_barrier();

		/* Mark it invalid and drop the mapping. */
		sr->usecount = 0;
		smgr_unlock_sr(sr, ~SR_VALID);
		hash_search_with_hash_value(sr_mapping_table,
									rnode,
									hash,
									HASH_REMOVE,
									NULL);
	}
	LWLockRelease(mapping_lock);
}

size_t
smgr_shmem_size(void)
{
	size_t size = 0;

	size = add_size(size,
					sizeof(offsetof(SMgrSharedRelationPool, objects) +
						   sizeof(SMgrSharedRelation) * smgr_shared_relations));
	size = add_size(size,
					hash_estimate_size(smgr_shared_relations,
									   sizeof(SMgrSharedRelationMapping)));

	return size;
}

void
smgr_shmem_init(void)
{
	HASHCTL		info;
	bool found;

	info.keysize = sizeof(RelFileNodeBackend);
	info.entrysize = sizeof(SMgrSharedRelationMapping);
	info.num_partitions = SR_PARTITIONS;
	sr_mapping_table = ShmemInitHash("SMgrSharedRelation Mapping Table",
									 smgr_shared_relations,
									 smgr_shared_relations,
									 &info,
									 HASH_ELEM | HASH_BLOBS | HASH_PARTITION);

	sr_pool = ShmemInitStruct("SMgrSharedRelation Pool",
							  offsetof(SMgrSharedRelationPool, objects) +
							  sizeof(SMgrSharedRelation) * smgr_shared_relations,
							  &found);
	if (!found)
	{
		ConditionVariableInit(&sr_pool->sync_flags_cleared);
		pg_atomic_init_u32(&sr_pool->next, 0);
		for (uint32 i = 0; i < smgr_shared_relations; ++i)
		{
			pg_atomic_init_u32(&sr_pool->objects[i].flags, 0);
			pg_atomic_init_u64(&sr_pool->objects[i].generation, 0);
			sr_pool->objects[i].usecount = 0;
		}
	}
}

/*
 *	smgrinit(), smgrshutdown() -- Initialize or shut down storage
 *								  managers.
 *
 * Note: smgrinit is called during backend startup (normal or standalone
 * case), *not* during postmaster start.  Therefore, any resources created
 * here or destroyed in smgrshutdown are backend-local.
 */
void
smgrinit(void)
{
	int			i;

	for (i = 0; i < NSmgr; i++)
	{
		if (smgrsw[i].smgr_init)
			smgrsw[i].smgr_init();
	}

	/* register the shutdown proc */
	on_proc_exit(smgrshutdown, 0);
}

/*
 * on_proc_exit hook for smgr cleanup during backend shutdown
 */
static void
smgrshutdown(int code, Datum arg)
{
	int			i;

	for (i = 0; i < NSmgr; i++)
	{
		if (smgrsw[i].smgr_shutdown)
			smgrsw[i].smgr_shutdown();
	}
}

/*
 *	smgropen() -- Return an SMgrRelation object, creating it if need be.
 *
 *		This does not attempt to actually open the underlying file.
 */
SMgrRelation
smgropen(RelFileNode rnode, BackendId backend)
{
	RelFileNodeBackend brnode;
	SMgrRelation reln;
	bool		found;

	if (SMgrRelationHash == NULL)
	{
		/* First time through: initialize the hash table */
		HASHCTL		ctl;

		ctl.keysize = sizeof(RelFileNodeBackend);
		ctl.entrysize = sizeof(SMgrRelationData);
		SMgrRelationHash = hash_create("smgr relation table", 400,
									   &ctl, HASH_ELEM | HASH_BLOBS);
		dlist_init(&unowned_relns);
	}

	/* Look up or create an entry */
	brnode.node = rnode;
	brnode.backend = backend;
	reln = (SMgrRelation) hash_search(SMgrRelationHash,
									  (void *) &brnode,
									  HASH_ENTER, &found);

	/* Initialize it if not present before */
	if (!found)
	{
		/* hash_search already filled in the lookup key */
		reln->smgr_owner = NULL;
		reln->smgr_targblock = InvalidBlockNumber;
		reln->smgr_shared = NULL;
		reln->smgr_shared_generation = 0;
		reln->smgr_which = 0;	/* we only have md.c at present */

		/* implementation-specific initialization */
		smgrsw[reln->smgr_which].smgr_open(reln);

		/* it has no owner yet */
		dlist_push_tail(&unowned_relns, &reln->node);
	}

	return reln;
}

/*
 * smgrsetowner() -- Establish a long-lived reference to an SMgrRelation object
 *
 * There can be only one owner at a time; this is sufficient since currently
 * the only such owners exist in the relcache.
 */
void
smgrsetowner(SMgrRelation *owner, SMgrRelation reln)
{
	/* We don't support "disowning" an SMgrRelation here, use smgrclearowner */
	Assert(owner != NULL);

	/*
	 * First, unhook any old owner.  (Normally there shouldn't be any, but it
	 * seems possible that this can happen during swap_relation_files()
	 * depending on the order of processing.  It's ok to close the old
	 * relcache entry early in that case.)
	 *
	 * If there isn't an old owner, then the reln should be in the unowned
	 * list, and we need to remove it.
	 */
	if (reln->smgr_owner)
		*(reln->smgr_owner) = NULL;
	else
		dlist_delete(&reln->node);

	/* Now establish the ownership relationship. */
	reln->smgr_owner = owner;
	*owner = reln;
}

/*
 * smgrclearowner() -- Remove long-lived reference to an SMgrRelation object
 *					   if one exists
 */
void
smgrclearowner(SMgrRelation *owner, SMgrRelation reln)
{
	/* Do nothing if the SMgrRelation object is not owned by the owner */
	if (reln->smgr_owner != owner)
		return;

	/* unset the owner's reference */
	*owner = NULL;

	/* unset our reference to the owner */
	reln->smgr_owner = NULL;

	/* add to list of unowned relations */
	dlist_push_tail(&unowned_relns, &reln->node);
}

/*
 *	smgrexists() -- Does the underlying file for a fork exist?
 */
bool
smgrexists(SMgrRelation reln, ForkNumber forknum)
{
	if (smgrnblocks_fast(reln, forknum) != InvalidBlockNumber)
		return true;

	return smgrsw[reln->smgr_which].smgr_exists(reln, forknum);
}

/*
 *	smgrclose() -- Close and delete an SMgrRelation object.
 */
void
smgrclose(SMgrRelation reln)
{
	SMgrRelation *owner;
	ForkNumber	forknum;

	for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
		smgrsw[reln->smgr_which].smgr_close(reln, forknum);

	owner = reln->smgr_owner;

	if (!owner)
		dlist_delete(&reln->node);

	if (hash_search(SMgrRelationHash,
					(void *) &(reln->smgr_rnode),
					HASH_REMOVE, NULL) == NULL)
		elog(ERROR, "SMgrRelation hashtable corrupted");

	/*
	 * Unhook the owner pointer, if any.  We do this last since in the remote
	 * possibility of failure above, the SMgrRelation object will still exist.
	 */
	if (owner)
		*owner = NULL;
}

/*
 *	smgrcloseall() -- Close all existing SMgrRelation objects.
 */
void
smgrcloseall(void)
{
	HASH_SEQ_STATUS status;
	SMgrRelation reln;

	/* Nothing to do if hashtable not set up */
	if (SMgrRelationHash == NULL)
		return;

	hash_seq_init(&status, SMgrRelationHash);

	while ((reln = (SMgrRelation) hash_seq_search(&status)) != NULL)
		smgrclose(reln);
}

/*
 *	smgrclosenode() -- Close SMgrRelation object for given RelFileNode,
 *					   if one exists.
 *
 * This has the same effects as smgrclose(smgropen(rnode)), but it avoids
 * uselessly creating a hashtable entry only to drop it again when no
 * such entry exists already.
 */
void
smgrclosenode(RelFileNodeBackend rnode)
{
	SMgrRelation reln;

	/* Nothing to do if hashtable not set up */
	if (SMgrRelationHash == NULL)
		return;

	reln = (SMgrRelation) hash_search(SMgrRelationHash,
									  (void *) &rnode,
									  HASH_FIND, NULL);
	if (reln != NULL)
		smgrclose(reln);
}

/*
 *	smgrcreate() -- Create a new relation.
 *
 *		Given an already-created (but presumably unused) SMgrRelation,
 *		cause the underlying disk file or other storage for the fork
 *		to be created.
 */
void
smgrcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	smgrsw[reln->smgr_which].smgr_create(reln, forknum, isRedo);
}

/*
 *	smgrdosyncall() -- Immediately sync all forks of all given relations
 *
 *		All forks of all given relations are synced out to the store.
 *
 *		This is equivalent to FlushRelationBuffers() for each smgr relation,
 *		then calling smgrimmedsync() for all forks of each relation, but it's
 *		significantly quicker so should be preferred when possible.
 */
void
smgrdosyncall(SMgrRelation *rels, int nrels)
{
	int			i = 0;
	ForkNumber	forknum;

	if (nrels == 0)
		return;

	FlushRelationsAllBuffers(rels, nrels);

	/*
	 * Sync the physical file(s).
	 */
	for (i = 0; i < nrels; i++)
	{
		int			which = rels[i]->smgr_which;

		for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
		{
			if (smgrsw[which].smgr_exists(rels[i], forknum))
				smgrsw[which].smgr_immedsync(rels[i], forknum);
		}
	}
}

/*
 *	smgrdounlinkall() -- Immediately unlink all forks of all given relations
 *
 *		All forks of all given relations are removed from the store.  This
 *		should not be used during transactional operations, since it can't be
 *		undone.
 *
 *		If isRedo is true, it is okay for the underlying file(s) to be gone
 *		already.
 */
void
smgrdounlinkall(SMgrRelation *rels, int nrels, bool isRedo)
{
	int			i = 0;
	RelFileNodeBackend *rnodes;
	ForkNumber	forknum;

	if (nrels == 0)
		return;

	/*
	 * Get rid of any remaining buffers for the relations.  bufmgr will just
	 * drop them without bothering to write the contents.
	 */
	DropRelFileNodesAllBuffers(rels, nrels);

	/*
	 * create an array which contains all relations to be dropped, and close
	 * each relation's forks at the smgr level while at it
	 */
	rnodes = palloc(sizeof(RelFileNodeBackend) * nrels);
	for (i = 0; i < nrels; i++)
	{
		RelFileNodeBackend rnode = rels[i]->smgr_rnode;
		int			which = rels[i]->smgr_which;

		rnodes[i] = rnode;

		/* Close the forks at smgr level */
		for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
			smgrsw[which].smgr_close(rels[i], forknum);
	}

	/*
	 * It'd be nice to tell the stats collector to forget them immediately,
	 * too. But we can't because we don't know the OIDs.
	 */

	/*
	 * Send a shared-inval message to force other backends to close any
	 * dangling smgr references they may have for these rels.  We should do
	 * this before starting the actual unlinking, in case we fail partway
	 * through that step.  Note that the sinval messages will eventually come
	 * back to this backend, too, and thereby provide a backstop that we
	 * closed our own smgr rel.
	 */
	for (i = 0; i < nrels; i++)
		CacheInvalidateSmgr(rnodes[i]);

	for (i = 0; i < nrels; i++)
		smgr_drop_sr(&rels[i]->smgr_rnode);

	/*
	 * Delete the physical file(s).
	 *
	 * Note: smgr_unlink must treat deletion failure as a WARNING, not an
	 * ERROR, because we've already decided to commit or abort the current
	 * xact.
	 */

	for (i = 0; i < nrels; i++)
	{
		int			which = rels[i]->smgr_which;

		for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
			smgrsw[which].smgr_unlink(rnodes[i], forknum, isRedo);
	}

	pfree(rnodes);
}


/*
 *	smgrextend() -- Add a new block to a file.
 *
 *		The semantics are nearly the same as smgrwrite(): write at the
 *		specified position.  However, this is to be used for the case of
 *		extending a relation (i.e., blocknum is at or beyond the current
 *		EOF).  Note that we assume writing a block beyond current EOF
 *		causes intervening file space to become filled with zeroes.
 */
void
smgrextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   char *buffer, bool skipFsync)
{
	smgrsw[reln->smgr_which].smgr_extend(reln, forknum, blocknum,
										 buffer, skipFsync);
	smgrnblocks_update(reln, forknum, blocknum + 1, true);
}

/*
 *	smgrprefetch() -- Initiate asynchronous read of the specified block of a relation.
 *
 *		In recovery only, this can return false to indicate that a file
 *		doesn't	exist (presumably it has been dropped by a later WAL
 *		record).
 */
bool
smgrprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	return smgrsw[reln->smgr_which].smgr_prefetch(reln, forknum, blocknum);
}

/*
 *	smgrread() -- read a particular block from a relation into the supplied
 *				  buffer.
 *
 *		This routine is called from the buffer manager in order to
 *		instantiate pages in the shared buffer cache.  All storage managers
 *		return pages in the format that POSTGRES expects.
 */
void
smgrread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 char *buffer)
{
	smgrsw[reln->smgr_which].smgr_read(reln, forknum, blocknum, buffer);
}

/*
 *	smgrwrite() -- Write the supplied buffer out.
 *
 *		This is to be used only for updating already-existing blocks of a
 *		relation (ie, those before the current EOF).  To extend a relation,
 *		use smgrextend().
 *
 *		This is not a synchronous write -- the block is not necessarily
 *		on disk at return, only dumped out to the kernel.  However,
 *		provisions will be made to fsync the write before the next checkpoint.
 *
 *		skipFsync indicates that the caller will make other provisions to
 *		fsync the relation, so we needn't bother.  Temporary relations also
 *		do not require fsync.
 */
void
smgrwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		  char *buffer, bool skipFsync)
{
	smgrsw[reln->smgr_which].smgr_write(reln, forknum, blocknum,
										buffer, skipFsync);
}


/*
 *	smgrwriteback() -- Trigger kernel writeback for the supplied range of
 *					   blocks.
 */
void
smgrwriteback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			  BlockNumber nblocks)
{
	smgrsw[reln->smgr_which].smgr_writeback(reln, forknum, blocknum,
											nblocks);
}

/*
 *	smgrnblocks() -- Calculate the number of blocks in the
 *					 supplied relation.
 */
BlockNumber
smgrnblocks(SMgrRelation reln, ForkNumber forknum)
{
	BlockNumber result;

	/* Can we get the answer from shared memory without locking? */
	result = smgrnblocks_fast(reln, forknum);
	if (result != InvalidBlockNumber)
		return result;

	/* Can we get the answer from shared memory with only a share lock? */
	result = smgrnblocks_shared(reln, forknum);
	if (result != InvalidBlockNumber)
		return result;

	/* Ask the kernel. */
	result = smgrsw[reln->smgr_which].smgr_nblocks(reln, forknum);

	/* Update the value in shared memory for faster service next time. */
	smgrnblocks_update(reln, forknum, result, false);

	return result;
}

/*
 *	smgrtruncate() -- Truncate the given forks of supplied relation to
 *					  each specified numbers of blocks
 *
 * The truncation is done immediately, so this can't be rolled back.
 *
 * The caller must hold AccessExclusiveLock on the relation, to ensure that
 * other backends receive the smgr invalidation event that this function sends
 * before they access any forks of the relation again.
 */
void
smgrtruncate(SMgrRelation reln, ForkNumber *forknum, int nforks, BlockNumber *nblocks)
{
	int			i;

	/*
	 * Get rid of any buffers for the about-to-be-deleted blocks. bufmgr will
	 * just drop them without bothering to write the contents.
	 */
	DropRelFileNodeBuffers(reln, forknum, nforks, nblocks);

	/*
	 * Send a shared-inval message to force other backends to close any smgr
	 * references they may have for this rel.  This is useful because they
	 * might have open file pointers to segments that got removed, and/or
	 * smgr_targblock variables pointing past the new rel end.  (The inval
	 * message will come back to our backend, too, causing a
	 * probably-unnecessary local smgr flush.  But we don't expect that this
	 * is a performance-critical path.)  As in the unlink code, we want to be
	 * sure the message is sent before we start changing things on-disk.
	 */
	CacheInvalidateSmgr(reln->smgr_rnode);

	/* Do the truncation */
	for (i = 0; i < nforks; i++)
	{
		smgrsw[reln->smgr_which].smgr_truncate(reln, forknum[i], nblocks[i]);
		smgrnblocks_update(reln, forknum[i], nblocks[i], true);
	}
}

/*
 *	smgrimmedsync() -- Force the specified relation to stable storage.
 *
 *		Synchronously force all previous writes to the specified relation
 *		down to disk.
 *
 *		This is useful for building completely new relations (eg, new
 *		indexes).  Instead of incrementally WAL-logging the index build
 *		steps, we can just write completed index pages to disk with smgrwrite
 *		or smgrextend, and then fsync the completed index file before
 *		committing the transaction.  (This is sufficient for purposes of
 *		crash recovery, since it effectively duplicates forcing a checkpoint
 *		for the completed index.  But it is *not* sufficient if one wishes
 *		to use the WAL log for PITR or replication purposes: in that case
 *		we have to make WAL entries as well.)
 *
 *		The preceding writes should specify skipFsync = true to avoid
 *		duplicative fsyncs.
 *
 *		Note that you need to do FlushRelationBuffers() first if there is
 *		any possibility that there are dirty buffers for the relation;
 *		otherwise the sync is not very meaningful.
 */
void
smgrimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	smgrsw[reln->smgr_which].smgr_immedsync(reln, forknum);
}

/*
 * When a database is dropped, we have to find and throw away all its
 * SMgrSharedRelation objects.
 */
void
smgrdropdb(Oid database)
{
	for (int i = 0; i < smgr_shared_relations; ++i)
	{
		SMgrSharedRelation *sr = &sr_pool->objects[i];
		RelFileNodeBackend rnode;
		uint32 flags;

		/* Hold the spinlock only while we copy out the rnode of matches. */
		flags = smgr_lock_sr(sr);
		if ((flags & SR_VALID) && sr->rnode.node.dbNode == database)
		{
			rnode = sr->rnode;
			smgr_unlock_sr(sr, flags);

			/* Drop, if it's still valid. */
			smgr_drop_sr(&rnode);
		}
		else
			smgr_unlock_sr(sr, flags);
	}
}

/*
 * AtEOXact_SMgr
 *
 * This routine is called during transaction commit or abort (it doesn't
 * particularly care which).  All transient SMgrRelation objects are closed.
 *
 * We do this as a compromise between wanting transient SMgrRelations to
 * live awhile (to amortize the costs of blind writes of multiple blocks)
 * and needing them to not live forever (since we're probably holding open
 * a kernel file descriptor for the underlying file, and we need to ensure
 * that gets closed reasonably soon if the file gets deleted).
 */
void
AtEOXact_SMgr(void)
{
	dlist_mutable_iter iter;

	/*
	 * Zap all unowned SMgrRelations.  We rely on smgrclose() to remove each
	 * one from the list.
	 */
	dlist_foreach_modify(iter, &unowned_relns)
	{
		SMgrRelation rel = dlist_container(SMgrRelationData, node,
										   iter.cur);

		Assert(rel->smgr_owner == NULL);

		smgrclose(rel);
	}
}
