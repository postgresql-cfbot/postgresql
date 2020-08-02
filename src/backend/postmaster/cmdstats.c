/* ----------
 * cmdstats.c
 *
 *	Copyright (c) 2001-2020, PostgreSQL Global Development Group
 *
 *	src/backend/postmaster/cmdstats.c
 * ----------
 */
#include "postgres.h"

#include "access/xact.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "postmaster/cmdstats.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/acl.h"
#include "utils/memutils.h"
#include "utils/varlena.h"


/*
 * Create a mapping from a CommandTag to its offset in the per-backend stats
 * (if any), else defaulting to -1.  There will be an entry in this array for
 * every CommandTag.
 */
typedef struct LocalStatsOffset {
	int			offset;
} LocalStatsOffset;

#define PG_CMDTAG(tag, name, evtrgok, rwrok, rowcnt, localstats) \
	[tag] = { .offset = localstats },

const LocalStatsOffset local_stats_offset[] = {
#include "tcop/cmdtaglist.h"
	[COMMAND_TAG_NEXTTAG] = { .offset = -1 }
};

#undef PG_CMDTAG

static void
merge_local_accum(CmdStats *pg_restrict stats, const LocalStatsAccum *pg_restrict accum)
{
	CommandTag tag;
	uint64 *dst = (uint64 *) stats->cnt;
	const uint64 *src = (const uint64 *) accum->cnt;

	for (tag = FIRST_CMDTAG; tag <= LAST_CMDTAG; tag++)
	{
		int offset = local_stats_offset[tag].offset;
		if (offset >= 0)
			dst[tag] += src[offset];
	}
}

static void
rollup_local_stats(CmdStats *pg_restrict stats, LocalCmdStats *pg_restrict localstats)
{
	CommandTag tag;
	uint64 *dst = (uint64 *) stats->cnt;
	pg_atomic_uint32 *src = (pg_atomic_uint32 *) localstats->cnt;

	LWLockAcquire(CmdStatsLock, LW_EXCLUSIVE);
	/*
	 * We need to read the src[] array element and zero it in the same atomic
	 * operation.  We hold an exclusive lock, but backends who merely update the
	 * value of their own count don't try to take the lock, so we get no protection
	 * against concurrent updates by them.
	 */
	for (tag = FIRST_CMDTAG; tag <= LAST_CMDTAG; tag++)
	{
		int offset = local_stats_offset[tag].offset;
		if (offset >= 0)
			dst[tag] += pg_atomic_exchange_u32(&(src[offset]), 0);
	}
	LWLockRelease(CmdStatsLock);
}

/* ----------
 * GUC parameters
 * ----------
 */
bool		cmdstats_tracking = false;

/*
 * This structure holds counters in shared memory tracking how many commands of
 * a given type have been run.
 *
 * CmdStatsShmem->cmdstats
 * -------------------------------
 * A single shared array of uint64 counts reserved for counting per commandtag
 * how many operations of that commandtag have been executed to completion.
 * Reading and writing these counts requires lwlocks.  The size of this array
 * is proportional to NUM_CMDSTATS_COUNTS.  These counts may underrepresent the
 * true totals if the backends are keeping local per-backend counts.
 *
 * CmdStatsShmem->localcmdstats
 * ------------------------------------
 * For the subset of commands that may be quick and be run in rapid succession,
 * to avoid adding the locking overhead to each run of the command, separate
 * counters are kept per backend.  These local per-backend counters can be
 * updated by the owning backend through atomics without the use of locking.
 * This requires shared memory that scales proportional to MaxBackends *
 * NUM_LOCAL_STATS.  Owing to the impact on the amount of shared memory
 * required, this treatment is reserved only to the fastest commands which
 * might otherwise suffer a measurable performance hit.
 *
 * LOCKING AND ATOMIC READS
 * ------------------------
 *
 * Both the shared array of counts and the local per-backend counts are stored
 * in shared memory.  The "local per-backend" counts are equally visible to all
 * backends.  By convention, backends may read and write their own per-backend
 * counts, but may only read counts from other backends.  Note that this
 * restriction holds even for backends holding the lwlock in exclusive mode.
 * Even then, the backend holding the lock must not modify the counts of any
 * other backend.
 *
 * Atomic reads and writes are supported on all architectures for uint32, but
 * only on a subset of architectures for uint64.  To make the implementation
 * the same on all architectures, the backends track their counts as uint32
 * variables.  It may be tempting to upgrade these counts to uint64 if atomic
 * support were universal, but that would double the amount of shared memory
 * for command statistics used per backend process.  It may also be tempting to
 * reduce to a uint16 in order to reduce the per backend shared memory usage,
 * but there is no atomics support for uint16.
 *
 * At any moment, the true clusterwide count per command is the sum of the
 * shared counter for that command and all backend local counters for that
 * command, if any.  Since uint32 is small enough to overflow, we keep a shared
 * uint64 counter per command tag for all command tags, including those which
 * get special per-backend counters.  This implementation assumes uint64 counts
 * will not overflow during the lifetime of a running cluster.
 *
 * Whenever a backend's uint32 counts are about to overflow, the backend must
 * acquire the shared CmdStatsLock exclusively, merge its own counts into the
 * shared counts, and then reset its own counts to zero.  Other backends which
 * are simultaneously updating their own uint32 counts are not blocked by this,
 * but if another backend were to simultaneously need to merge counts into the
 * shared counts, it would have to wait for the lock.  Since merging counts
 * only happens once every 2^32 commands per backend, backends are not expected
 * to block each other in this way sufficiently often to be of concern.
 *
 * Backends totaling up the counts to return to a caller as part of executing
 * one of the command stats system views must acquire the CmdStatsLock and hold
 * it while tallying the shared counts and all the backend counts together.
 * Each concurrently running backend may update its counts while this lock is
 * held, but no backend may merge its counts into the shared counts nor reset
 * its counts to zero.  Since the command stats system views make no
 * transaction isolation level guarantees about the stability of the counts,
 * this seems sufficient, so long as the atomicity of changes to the counts is
 * carefully handled.
 */
typedef struct
{
	CmdStats		*cmdstats;			/* Array of cluster totals */
	LocalCmdStats	*localcmdstats;		/* Array of backend totals */
}
#if defined(pg_attribute_aligned)
			pg_attribute_aligned(MAXIMUM_ALIGNOF)
#endif
CmdStatsShmemStruct;

static CmdStatsShmemStruct *CmdStatsShmem;

static Size
command_stats_struct_size(void)
{
	return MAXALIGN(sizeof(CmdStatsShmemStruct));
}

static Size
shared_cmdstats_size(void)
{
	return MAXALIGN(sizeof(CmdStats));
}

static Size
local_cmdstats_size(void)
{
	return mul_size(MAXALIGN(sizeof(LocalCmdStats)), MaxBackends);
}

/*
 * CmdStatsShmemSize
 *		Compute space needed for cmdstats-related shared memory
 */
Size
CmdStatsShmemSize(void)
{
	Size		size;

	if (!cmdstats_tracking)
		return 0;
	size = command_stats_struct_size();					/* The struct itself */
	size = add_size(size, shared_cmdstats_size());	/* cmdstats */
	size = add_size(size, local_cmdstats_size());	/* localcmdstats */
	return size;
}

/*
 * CmdStatsShmemInit
 *		Allocate and initialize cmdstats-related shared memory
 */
void
CmdStatsShmemInit(void)
{
	bool		found;
	Size		shmemsize = CmdStatsShmemSize();

	if (shmemsize == 0)
	{
		CmdStatsShmem = NULL;
		return;
	}

	CmdStatsShmem = (CmdStatsShmemStruct *)
		ShmemInitStruct("CmdStats Data", shmemsize, &found);

	if (!IsUnderPostmaster)
	{
		char	   *next;  /* really a void pointer, but char for portability */
		int			bkend, idx;

		/*
		 * We should only be called at postmaster startup, before any
		 * backends are looking at the CmdStatsShmem, so acquiring
		 * the lock is probably not required.  There doesn't seem to be
		 * any reason to skip the lock, though, since we only have to do
		 * this once.
		 */
		LWLockAcquire(CmdStatsLock, LW_EXCLUSIVE);

		/*
		 * Each pointer field in the struct points to memory contiguous with
		 * the struct itself.  Each such field is MAXALIGNED, but other than
		 * the padding that might introduce, the fields are in-order and back
		 * to back.
		 */
		memset(CmdStatsShmem, 0, shmemsize);
		next = (char*)CmdStatsShmem + command_stats_struct_size();
		CmdStatsShmem->cmdstats = (CmdStats *)next;
		next = next + shared_cmdstats_size();
		CmdStatsShmem->localcmdstats = (LocalCmdStats *)next;
		next = next + local_cmdstats_size();

		/* Initialize all the atomic variables before anybody can use them */
		for (bkend = 0; bkend < MaxBackends; bkend++)
			for (idx = 0; idx < NUM_LOCAL_STATS; idx++)
				pg_atomic_init_u32(
					&(CmdStatsShmem->localcmdstats[bkend].cnt[idx]), 0);

		/* It is now safe for other backends to get the lock */
		LWLockRelease(CmdStatsLock);

		/*
		 * For sanity, assert that we used precisely the amount of memory that
		 * we allocated.  There wouldn't be much harm in allocating a bit more
		 * than necessary, but we did not do so intentionally, so it would be
		 * indicative of a memory size calculation error if things don't match.
		 */
		Assert(next == (char *)CmdStatsShmem + shmemsize);

		elog(DEBUG1, "cmdstats finished initializing shared memory of size "
					 "%llu ", (long long unsigned int) shmemsize);
	}
	else
		Assert(found);
}

CmdStats *
cmdstats_shared_tally(void)
{
	int					backend,
						localidx,
						tagidx;
	LocalCmdStats  *local_stats;
	pg_atomic_uint32   *bkend_local_array;
	const CmdStats *shared_stats;
	const uint64	   *shared_array;
	CmdStats	   *result_stats;
	uint64			   *result_array;
	LocalStatsAccum		local_accum;
	uint64			   *accum_array;


	Assert(cmdstats_tracking);
	Assert(CmdStatsShmem);

	shared_stats = (const CmdStats *) CmdStatsShmem->cmdstats;
	shared_array = (const uint64 *) shared_stats->cnt;
	local_stats = (LocalCmdStats *) CmdStatsShmem->localcmdstats;

	result_stats = (CmdStats *) palloc0(sizeof(CmdStats));
	result_array = (uint64 *) result_stats->cnt;

	memset(&local_accum, 0, sizeof(local_accum));
	accum_array = (uint64*) &(local_accum.cnt[0]);

	LWLockAcquire(CmdStatsLock, LW_SHARED);
	for (backend = 0; backend < MaxBackends; backend++)
	{
		bkend_local_array = local_stats[backend].cnt;
		for (localidx = 0; localidx < NUM_LOCAL_STATS; localidx++)
			accum_array[localidx] += pg_atomic_read_u32(&(bkend_local_array[localidx]));
	}
	for (tagidx = 0; tagidx < NUM_CMDSTATS_COUNTS; tagidx++)
		result_array[tagidx] += shared_array[tagidx];
	merge_local_accum(result_stats, &local_accum);
	LWLockRelease(CmdStatsLock);

	return result_stats;
}

void
cmdstats_rollup_localstats(LocalCmdStats *localstats)
{
	Assert(cmdstats_tracking);
	Assert(CmdStatsShmem);

	rollup_local_stats(CmdStatsShmem->cmdstats, localstats);
}

void
cmdstats_increment(CommandTag commandtag)
{
	int		local_stats_idx;

	/* Should not get here unless cmdstats_tracking is true */
	Assert(cmdstats_tracking);
	Assert(CmdStatsShmem);
	Assert(commandtag >= FIRST_CMDTAG && commandtag < LAST_CMDTAG);

	/*
	 * If the commandtag has per-backend counters, we'll update the per-backend
	 * counter belonging to this backend, otherwise we'll update the shared
	 * counter used by all backends.  Note that updating the shared counter
	 * when we could have updated the per-backend counter is not a correctness
	 * problem, only a performance problem.  The per-backend counters
	 * eventually get rolled into the shared counters anyway.
	 *
	 * There is no good reason why a postmaster should ever call this function,
	 * so we complain loudly if it does.  Having the postmaster update a
	 * per-backend counter would be inappropriate, but if all shared counters
	 * have been properly initialized, the postmaster could in theory safely
	 * increment the shared counter for the commandtag.  But if the postmaster
	 * is calling this function it suggests that something has gone awry, and
	 * we can't really trust that the shared memory has been properly
	 * initialized yet, so a hard error seems more appropriate.
	 */
	local_stats_idx = local_stats_offset[commandtag].offset;
	if (local_stats_idx < 0)
	{
		/*
		 * There are no per-backend counters for this commandtag, so we must
		 * hold an exclusive lock while incrementing the shared counter.
		 * We don't expect this to be performed by the postmaster, but we don't
		 * really care, which is why we're not checking IsUnderPostmaster here.
		 */
		LWLockAcquire(CmdStatsLock, LW_EXCLUSIVE);
		CmdStatsShmem->cmdstats->cnt[commandtag]++;
		LWLockRelease(CmdStatsLock);
	}
	else if (IsUnderPostmaster)
	{
		/*
		 * There are per-backend counters for this commandtag, so we must
		 * actually be a backend to have one assigned to us (hence the
		 * IsUnderPostmaster check), and we have exclusive ownership of the
		 * counter so we don't need to take the lock.  But since we might
		 * increment our counter while others are simultaneously reading our
		 * counter, we must use atomic operations.
		 */
		int		bkend;
		pg_atomic_uint32 *cnt;

		/* Assert that we're called by a valid backend */
		Assert(MyBackendId >= 1 && MyBackendId <= MaxBackends);

		/* Assert that our argument is within bounds */
		Assert(local_stats_idx < NUM_LOCAL_STATS);

		/*
		 * MyBackendId runs from [1..MaxBackends] inclusive, but our
		 * indexes only operate from [0..MaxBackends-1]
		 */
		bkend = MyBackendId - 1;

		/* We have a local stats counter for This commandtag. */
		cnt = &(CmdStatsShmem->localcmdstats[bkend].cnt[local_stats_idx]);

		/*
		 * We need to verify that our counter will not overflow when we
		 * increment it.  We don't have to worry about anybody else changing
		 * our per-backend counter, so there is no race condition between
		 * checking for overflow and later incrementing.
		 *
		 * The atomic read is overkill, since nobody else can change the cnt.
		 * In most spots we need to read and write these values atomically,
		 * so we're just following that pattern here for good measure.
		 */
		if (pg_atomic_read_u32(cnt) >= PG_UINT32_MAX)
			/* Harvest and zero all our backend's stats.  This takes a lock */
			cmdstats_rollup_localstats(&CmdStatsShmem->localcmdstats[bkend]);

		/*
		 * Increment our count atomically.  This needs to be atomic since
		 * other backends could be reading the count simultaneously.
		 */
		pg_atomic_fetch_add_u32(cnt, 1);
	}
	else
	{
		elog(ERROR, "cmdstats_increment called by postmaster");
	}
}

void
reset_cmdstats(void)
{
	int					backend,
						localidx,
						tagidx;
	LocalCmdStats	   *local_stats;
	pg_atomic_uint32   *bkend_local_array;
	CmdStats		   *shared_stats;
	uint64			   *shared_array;

	Assert(cmdstats_tracking);
	Assert(CmdStatsShmem);

	shared_stats = (CmdStats *) CmdStatsShmem->cmdstats;
	shared_array = (uint64 *) shared_stats->cnt;
	local_stats = (LocalCmdStats *) CmdStatsShmem->localcmdstats;

	LWLockAcquire(CmdStatsLock, LW_EXCLUSIVE);
	for (backend = 0; backend < MaxBackends; backend++)
	{
		bkend_local_array = local_stats[backend].cnt;
		for (localidx = 0; localidx < NUM_LOCAL_STATS; localidx++)
			pg_atomic_write_u32(&(bkend_local_array[localidx]), 0);
	}
	for (tagidx = 0; tagidx < NUM_CMDSTATS_COUNTS; tagidx++)
		shared_array[tagidx] = 0;
	LWLockRelease(CmdStatsLock);
}
