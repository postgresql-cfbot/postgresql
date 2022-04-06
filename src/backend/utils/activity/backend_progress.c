/* ----------
 * backend_progress.c
 *
 *	Command progress reporting infrastructure.
 *
 *	Copyright (c) 2001-2022, PostgreSQL Global Development Group
 *
 *	src/backend/utils/activity/backend_progress.c
 * ----------
 */
#include "postgres.h"

#include "commands/progress.h"
#include "port/atomics.h"		/* for memory barriers */
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/shmem.h"
#include "utils/backend_progress.h"
#include "utils/backend_status.h"

/*
 * Structs for parallel progress tracking.
 *
 * The parallel workers and leader report progress
 * into a hash entry with a key of the leader pid.
 */
typedef struct ProgressParallelEntry
{
	pid_t   leader_pid;
	int64 	st_progress_param[PGSTAT_NUM_PROGRESS_PARAM];
} ProgressParallelEntry;

static HTAB *ProgressParallelHash;

/* We can only have as many parallel progress entries as max_worker_processes */
#define PROGRESS_PARALLEL_NUM_ENTRIES max_worker_processes

/*-----------
 * pgstat_progress_start_command() -
 *
 * Set st_progress_command (and st_progress_command_target) in own backend
 * entry.  Also, zero-initialize st_progress_param array.
 *-----------
 */
void
pgstat_progress_start_command(ProgressCommandType cmdtype, Oid relid)
{
	volatile PgBackendStatus *beentry = MyBEEntry;

	if (!beentry || !pgstat_track_activities)
		return;

	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
	beentry->st_progress_command = cmdtype;
	beentry->st_progress_command_target = relid;
	MemSet(&beentry->st_progress_param, 0, sizeof(beentry->st_progress_param));
	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/*-----------
 * pgstat_progress_update_param() -
 *
 * Update index'th member in st_progress_param[] of own backend entry.
 *-----------
 */
void
pgstat_progress_update_param(int index, int64 val)
{
	volatile PgBackendStatus *beentry = MyBEEntry;

	Assert(index >= 0 && index < PGSTAT_NUM_PROGRESS_PARAM);

	if (!beentry || !pgstat_track_activities)
		return;

	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
	beentry->st_progress_param[index] = val;
	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/*-----------
 * pgstat_progress_update_multi_param() -
 *
 * Update multiple members in st_progress_param[] of own backend entry.
 * This is atomic; readers won't see intermediate states.
 *-----------
 */
void
pgstat_progress_update_multi_param(int nparam, const int *index,
								   const int64 *val)
{
	volatile PgBackendStatus *beentry = MyBEEntry;
	int			i;

	if (!beentry || !pgstat_track_activities || nparam == 0)
		return;

	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

	for (i = 0; i < nparam; ++i)
	{
		Assert(index[i] >= 0 && index[i] < PGSTAT_NUM_PROGRESS_PARAM);

		beentry->st_progress_param[index[i]] = val[i];
	}

	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/*-----------
 * pgstat_progress_end_command() -
 *
 * Reset st_progress_command (and st_progress_command_target) in own backend
 * entry.  This signals the end of the command.
 *-----------
 */
void
pgstat_progress_end_command(void)
{
	volatile PgBackendStatus *beentry = MyBEEntry;

	if (!beentry || !pgstat_track_activities)
		return;

	if (beentry->st_progress_command == PROGRESS_COMMAND_INVALID)
		return;

	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
	beentry->st_progress_command = PROGRESS_COMMAND_INVALID;
	beentry->st_progress_command_target = InvalidOid;
	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/*-----------
 * ProgressParallelShmemInit() -
 *
 * Initialize the parallel progress hash.
 *-----------
 */
void
ProgressParallelShmemInit(void)
{
	HASHCTL     info;

	info.keysize = sizeof(pid_t);
	info.entrysize = sizeof(ProgressParallelEntry);

	ProgressParallelHash = ShmemInitHash("Parallel Progress hash",
									   PROGRESS_PARALLEL_NUM_ENTRIES,
									   PROGRESS_PARALLEL_NUM_ENTRIES,
									   &info,
									   HASH_ELEM | HASH_BLOBS);
}

/*-----------
 * ProgressParallelShmemSize() -
 *
 * Calculate the size of the parallel progress hash.
 *-----------
 */
Size
ProgressParallelShmemSize(void)
{
   Size        size = 0;

   /* parallel progress hash table */
   size = add_size(size, hash_estimate_size(PROGRESS_PARALLEL_NUM_ENTRIES,
											sizeof(ProgressParallelEntry)));

   return size;
}

/*-----------
 * pgstat_progress_update_param_parallel() -
 *
 * Update the index'th member in the st_progress_param[] of the
 * parallel progress hash table.
 *-----------
 */
void
pgstat_progress_update_param_parallel(int leader_pid, int index, int64 val)
{
	ProgressParallelEntry *entry;
	bool found;

	LWLockAcquire(ProgressParallelLock, LW_EXCLUSIVE);

	entry = (ProgressParallelEntry *) hash_search(ProgressParallelHash, &leader_pid, HASH_ENTER, &found);

	/*
	 * If the entry is not found, set the value for the index'th member,
	 * else increment the current value of the index'th member.
	 */
	if (!found)
		entry->st_progress_param[index] = val;
	else
		entry->st_progress_param[index] += val;

	LWLockRelease(ProgressParallelLock);
}

/*-----------
 * pgstat_progress_end_parallel() -
 *
 * This removes an entry with from the parallel progress
 * hash table.
 *-----------
 */
void
pgstat_progress_end_parallel(int leader_pid)
{
	LWLockAcquire(ProgressParallelLock, LW_EXCLUSIVE);

	/*
	* Remove from hashtable. It should always be present,
	* but don't complain if it's not.
	*/
	hash_search(ProgressParallelHash, &leader_pid, HASH_REMOVE, NULL);

	LWLockRelease(ProgressParallelLock);
}

/*-----------
 * pgstat_progress_end_parallel_callback() -
 *
 * PG_ENSURE_ERROR_CLEANUP callback. The caller is responsible
 * for ensuring cleanup when invoking pgstat_progress_update_param_parallel.
 *-----------
 */
void
pgstat_progress_end_parallel_callback(int code, Datum arg)
{
	pgstat_progress_end_parallel(DatumGetInt32(arg));
}

/*-----------
 * pgstat_progress_set_parallel() -
 *
 * This routine is called by pg_stat_get_progress_info
 * to update the datum with values from the parallel progress
 * hash.
 *-----------
 */
void
pgstat_progress_set_parallel(Datum *values)
{
	ProgressParallelEntry *entry;
	/* First element of the datum is always the pid */
	int leader_pid = values[0];

	LWLockAcquire(ProgressParallelLock, LW_SHARED);

	entry = (ProgressParallelEntry *) hash_search(ProgressParallelHash, &leader_pid, HASH_FIND, NULL);

	/*
	 * If an entry is not found, it is because we looked at a pid that is not involved in a parallel command,
	 * therefore release the read lock and return.
	 */
	if (!entry)
	{
		LWLockRelease(ProgressParallelLock);
		return;
	}

	for (int i = 0; i < PGSTAT_NUM_PROGRESS_PARAM; i++)
	{
		int64 val = entry->st_progress_param[i];

		/*
		 * We only care about hash entry members that have been updated by
		 * parallel workers ( or leader ). This is true if the member's value > 0.
		 */
		if (val > 0)
			values[i + PGSTAT_NUM_PROGRESS_COMMON] = val;
	}

	LWLockRelease(ProgressParallelLock);
}
