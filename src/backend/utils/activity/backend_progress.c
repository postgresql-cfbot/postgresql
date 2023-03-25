/* ----------
 * backend_progress.c
 *
 *	Command progress reporting infrastructure.
 *
 *	Copyright (c) 2001-2023, PostgreSQL Global Development Group
 *
 *	src/backend/utils/activity/backend_progress.c
 * ----------
 */
#include "postgres.h"

#include "commands/progress.h"
#include "port/atomics.h"		/* for memory barriers */
#include "utils/backend_progress.h"
#include "utils/backend_status.h"


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

/*
 * Check for consistency of progress data (current < total).
 *
 * Check during pgstat_progress_updates_*() rather than only from
 * pgstat_progress_end_command() to catch issues with uninitialized/stale data
 * from previous progress commands.
 *
 * If a command fails due to interrupt or error, the values may be less than
 * the expected final value.
 */
static void
pgstat_progress_asserts(void)
{
	volatile PgBackendStatus *beentry = MyBEEntry;
	volatile int64 *a = beentry->st_progress_param;

	switch (beentry->st_progress_command)
	{
		case PROGRESS_COMMAND_VACUUM:
			Assert(a[PROGRESS_VACUUM_HEAP_BLKS_SCANNED] <=
				   a[PROGRESS_VACUUM_TOTAL_HEAP_BLKS]);
			Assert(a[PROGRESS_VACUUM_HEAP_BLKS_VACUUMED] <=
				   a[PROGRESS_VACUUM_TOTAL_HEAP_BLKS]);
			Assert(a[PROGRESS_VACUUM_NUM_DEAD_TUPLES] <=
				   a[PROGRESS_VACUUM_MAX_DEAD_TUPLES]);
			break;

		case PROGRESS_COMMAND_ANALYZE:
			Assert(a[PROGRESS_ANALYZE_BLOCKS_DONE] <=
				   a[PROGRESS_ANALYZE_BLOCKS_TOTAL]);
			Assert(a[PROGRESS_ANALYZE_EXT_STATS_COMPUTED] <=
				   a[PROGRESS_ANALYZE_EXT_STATS_TOTAL]);
			Assert(a[PROGRESS_ANALYZE_CHILD_TABLES_DONE] <=
				   a[PROGRESS_ANALYZE_CHILD_TABLES_TOTAL]);
			break;

		case PROGRESS_COMMAND_CLUSTER:
			Assert(a[PROGRESS_CLUSTER_HEAP_BLKS_SCANNED] <=
				   a[PROGRESS_CLUSTER_TOTAL_HEAP_BLKS]);
			/* FALLTHROUGH */
			/* ..because CLUSTER rebuilds indexes */

		case PROGRESS_COMMAND_CREATE_INDEX:
			Assert(a[PROGRESS_CREATEIDX_TUPLES_DONE] <=
				   a[PROGRESS_CREATEIDX_TUPLES_TOTAL]);
			Assert(a[PROGRESS_CREATEIDX_PARTITIONS_DONE] <=
				   a[PROGRESS_CREATEIDX_PARTITIONS_TOTAL]);
			break;

		case PROGRESS_COMMAND_BASEBACKUP:
			/* progress reporting is optional for these */
			if (a[PROGRESS_BASEBACKUP_BACKUP_TOTAL] >= 0)
			{
				Assert(a[PROGRESS_BASEBACKUP_BACKUP_STREAMED] <=
					   a[PROGRESS_BASEBACKUP_BACKUP_TOTAL]);
				Assert(a[PROGRESS_BASEBACKUP_TBLSPC_STREAMED] <=
					   a[PROGRESS_BASEBACKUP_TBLSPC_TOTAL]);
			}
			break;

		case PROGRESS_COMMAND_COPY:
#if 0
			//This currently fails file_fdw tests, since pgstat_prorgress evidently fails
				// to support simultaneous copy commands, as happens during JOIN.
			/* bytes progress is not available in all cases */
				if (a[PROGRESS_COPY_BYTES_TOTAL] > 0)
				//Assert(a[PROGRESS_COPY_BYTES_PROCESSED] <= a[PROGRESS_COPY_BYTES_TOTAL]);
			if (a[PROGRESS_COPY_BYTES_PROCESSED] > a[PROGRESS_COPY_BYTES_TOTAL])
				elog(WARNING, "PROGRESS_COPY_BYTES_PROCESSED %ld %ld",
					 a[PROGRESS_COPY_BYTES_PROCESSED],
					 a[PROGRESS_COPY_BYTES_TOTAL]);
#endif
			break;

		case PROGRESS_COMMAND_INVALID:
			break;				/* Do nothing */
	}
}

/*
 * Check that newval >= oldval, and that when total is not being set twice.
 */
static void
pgstat_progress_assert_forward_progress(int command, int index,
										int64 oldval, int64 newval)
{
	switch (command)
	{
		case PROGRESS_COMMAND_ANALYZE:
			/*
			 * phase goes backwards for inheritance tables, which are sampled
			 * twice
			 */
			if (index != PROGRESS_ANALYZE_CURRENT_CHILD_TABLE_RELID &&
				index != PROGRESS_ANALYZE_PHASE)
				Assert(newval >= oldval);
			if (index == PROGRESS_ANALYZE_BLOCKS_TOTAL ||
				index == PROGRESS_ANALYZE_EXT_STATS_TOTAL ||
				index == PROGRESS_ANALYZE_CHILD_TABLES_TOTAL)
				Assert(oldval == 0);
			break;

		case PROGRESS_COMMAND_CLUSTER:
			if (index != PROGRESS_CLUSTER_INDEX_RELID)
				Assert(newval >= oldval);
			if (index == PROGRESS_CLUSTER_TOTAL_HEAP_BLKS)
				Assert(oldval == 0);
			break;

		case PROGRESS_COMMAND_CREATE_INDEX:
			if (index != PROGRESS_CREATEIDX_INDEX_OID &&
				index != PROGRESS_CREATEIDX_SUBPHASE &&
				index != PROGRESS_WAITFOR_CURRENT_PID &&
				index != PROGRESS_CREATEIDX_ACCESS_METHOD_OID)
				Assert(newval >= oldval);

			if (index == PROGRESS_CREATEIDX_TUPLES_TOTAL ||
				index == PROGRESS_CREATEIDX_PARTITIONS_TOTAL)
				Assert(oldval == 0);
			break;

		case PROGRESS_COMMAND_BASEBACKUP:
			if (index == PROGRESS_BASEBACKUP_BACKUP_TOTAL &&
				oldval == 0 && newval == -1)
				return;			/* Do nothing: this is the initial "null"
								 * state before the size is estimated */
			Assert(newval >= oldval);

			if (index == PROGRESS_BASEBACKUP_BACKUP_TOTAL ||
				index == PROGRESS_BASEBACKUP_TBLSPC_TOTAL)
				Assert(oldval == 0);
			break;

		case PROGRESS_COMMAND_COPY:
			Assert(newval >= oldval);
			if (index == PROGRESS_COPY_BYTES_TOTAL)
				Assert(oldval == 0);
			break;
		case PROGRESS_COMMAND_VACUUM:
			Assert(newval >= oldval);
			if (index == PROGRESS_VACUUM_TOTAL_HEAP_BLKS)
				Assert(oldval == 0);
			if (index == PROGRESS_VACUUM_MAX_DEAD_TUPLES)
				Assert(oldval == 0);
			break;

		case PROGRESS_COMMAND_INVALID:
			break;
	}
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

	if (index != PROGRESS_SCAN_BLOCKS_DONE)
	{
		/* Check that progress does not go backwards */
		int64		oldval = beentry->st_progress_param[index];

		pgstat_progress_assert_forward_progress(beentry->st_progress_command,
												index, oldval, val);
	}

	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
	beentry->st_progress_param[index] = val;
	PGSTAT_END_WRITE_ACTIVITY(beentry);

	pgstat_progress_asserts();
}

/*-----------
 * pgstat_progress_incr_param() -
 *
 * Increment index'th member in st_progress_param[] of the current backend.
 *-----------
 */
void
pgstat_progress_incr_param(int index, int64 incr)
{
	volatile PgBackendStatus *beentry = MyBEEntry;
	int64		val;

	Assert(index >= 0 && index < PGSTAT_NUM_PROGRESS_PARAM);

	if (!beentry || !pgstat_track_activities)
		return;

	/*
	 * Because no other process should write to this backend's own status, we
	 * can read its value from shared memory without needing to loop to ensure
	 * its consistency.
	 */
	val = beentry->st_progress_param[index];
	val += incr;

	pgstat_progress_update_param(index, val);
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

	pgstat_progress_asserts();
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
