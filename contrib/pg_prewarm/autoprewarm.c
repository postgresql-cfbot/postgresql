/*-------------------------------------------------------------------------
 *
 * autoprewarm.c
 *		Automatically prewarms the shared buffers when server restarts.
 *
 * DESCRIPTION
 *
 *		Autoprewarm is a bgworker process that automatically records the
 *		information about blocks which were present in shared buffers before
 *		server shutdown. Then prewarms the shared buffers on server restart
 *		with those blocks.
 *
 *		How does it work? When the shared library "pg_prewarm" is preloaded, a
 *		bgworker "autoprewarm" is launched immediately after the server has
 *		reached a consistent state. The bgworker will start loading blocks
 *		recorded until there is no free buffer left in the shared buffers. This
 *		way we do not replace any new blocks which were loaded either by the
 *		recovery process or the querying clients.
 *
 *		Once the "autoprewarm" bgworker has completed its prewarm task, it will
 *		start a new task to periodically dump the BlockInfoRecords related to
 *		the blocks which are currently in shared buffers. On next server
 *		restart, the bgworker will prewarm the shared buffers by loading those
 *		blocks. The GUC pg_prewarm.autoprewarm_interval will control the
 *		dumping activity of the bgworker.
 *
 *	Copyright (c) 2016-2017, PostgreSQL Global Development Group
 *
 *	IDENTIFICATION
 *		contrib/pg_prewarm/autoprewarm.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <unistd.h>

/* These are always necessary for a bgworker. */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* These are necessary for prewarm utilities. */
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "pgstat.h"
#include "storage/buf_internals.h"
#include "storage/dsm.h"
#include "storage/procsignal.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relfilenodemap.h"
#include "utils/resowner.h"

PG_FUNCTION_INFO_V1(autoprewarm_start_worker);
PG_FUNCTION_INFO_V1(autoprewarm_dump_now);

#define AUTOPREWARM_INTERVAL_SHUTDOWN_ONLY 0
#define AUTOPREWARM_INTERVAL_DEFAULT 300

#define AUTOPREWARM_FILE "autoprewarm.blocks"

/* Primary functions */
void		_PG_init(void);
void		autoprewarm_main(Datum main_arg);
static void dump_block_info_periodically(void);
static void autoprewarm_dump_launcher(void);
static void setup_autoprewarm(BackgroundWorker *autoprewarm,
				  const char *worker_name,
				  const char *worker_function,
				  Datum main_arg, int restart_time,
				  int extra_flags);
void		autoprewarm_database_main(Datum main_arg);

/*
 * Signal Handlers.
 */

static void apw_sigterm_handler(SIGNAL_ARGS);
static void apw_sighup_handler(SIGNAL_ARGS);

/* Flags set by signal handlers */
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

/*
 *	Signal handler for SIGTERM
 *	Set a flag to handle.
 */
static void
apw_sigterm_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 *	Signal handler for SIGHUP
 *	Set a flag to reread the config file.
 */
static void
apw_sighup_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/* ============================================================================
 * ==============	Types and variables used by autoprewarm   =============
 * ============================================================================
 */

/* Metadata of each persistent block which is dumped and used for loading. */
typedef struct BlockInfoRecord
{
	Oid			database;
	Oid			tablespace;
	Oid			filenode;
	ForkNumber	forknum;
	BlockNumber blocknum;
} BlockInfoRecord;

/* Tasks performed by autoprewarm workers.*/
typedef enum
{
	TASK_PREWARM_BUFFERPOOL,	/* prewarm the shared buffers. */
	TASK_DUMP_BUFFERPOOL_INFO	/* dump the shared buffer's block info. */
} AutoPrewarmTask;

/* Shared state information for autoprewarm bgworker. */
typedef struct AutoPrewarmSharedState
{
	LWLock		lock;			/* mutual exclusion */
	pid_t		bgworker_pid;	/* for main bgworker */
	pid_t		pid_using_dumpfile; /* for autoprewarm or block dump */
	bool		skip_prewarm_on_restart;	/* if set true, prewarm task will
											 * not be done */

	/* Following items are for communication with per-database worker */
	dsm_handle	block_info_handle;
	Oid			database;
	int			prewarm_start_idx;
	int			prewarm_stop_idx;
	uint32		prewarmed_blocks;
} AutoPrewarmSharedState;

static AutoPrewarmSharedState *apw_state = NULL;

/* GUC variable that controls the dump activity of autoprewarm. */
static int	autoprewarm_interval = 0;

/*
 * The GUC variable controls whether the server should run the autoprewarm
 * worker.
 */
static bool autoprewarm = true;

/* Compare member elements to check whether they are not equal. */
#define cmp_member_elem(fld)	\
do { \
	if (a->fld < b->fld)		\
		return -1;				\
	else if (a->fld > b->fld)	\
		return 1;				\
} while(0);

/*
 * blockinfo_cmp
 *		Compare function used for qsort().
 */
static int
blockinfo_cmp(const void *p, const void *q)
{
	BlockInfoRecord *a = (BlockInfoRecord *) p;
	BlockInfoRecord *b = (BlockInfoRecord *) q;

	cmp_member_elem(database);
	cmp_member_elem(tablespace);
	cmp_member_elem(filenode);
	cmp_member_elem(forknum);
	cmp_member_elem(blocknum);
	return 0;
}

/* ============================================================================
 * =================	Prewarm part of autoprewarm ========================
 * ============================================================================
 */

/*
 * detach_apw_shmem
 *		on_apw_exit reset the prewarm shared state
 */

static void
detach_apw_shmem(int code, Datum arg)
{
	if (apw_state->pid_using_dumpfile == MyProcPid)
		apw_state->pid_using_dumpfile = InvalidPid;
	if (apw_state->bgworker_pid == MyProcPid)
		apw_state->bgworker_pid = InvalidPid;
}

/*
 * init_apw_shmem
 *		Allocate and initialize autoprewarm related shared memory.
 */
static void
init_apw_shmem(void)
{
	bool		found = false;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	apw_state = ShmemInitStruct("autoprewarm",
								sizeof(AutoPrewarmSharedState),
								&found);
	if (!found)
	{
		/* First time through ... */
		LWLockInitialize(&apw_state->lock, LWLockNewTrancheId());
		apw_state->bgworker_pid = InvalidPid;
		apw_state->pid_using_dumpfile = InvalidPid;
		apw_state->skip_prewarm_on_restart = false;
	}

	LWLockRelease(AddinShmemInitLock);
}

/*
 * autoprewarm_database_main
 *		This subroutine loads the BlockInfoRecords of the database set in
 *		AutoPrewarmSharedState.
 *
 * Connect to the database and load the blocks of that database which are given
 * by [apw_state->prewarm_start_idx, apw_state->prewarm_stop_idx).
 */
void
autoprewarm_database_main(Datum main_arg)
{
	uint32		pos;
	BlockInfoRecord *block_info;
	Relation	rel = NULL;
	BlockNumber nblocks = 0;
	BlockInfoRecord *old_blk;
	dsm_segment *seg;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGTERM, die);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	init_apw_shmem();
	seg = dsm_attach(apw_state->block_info_handle);
	if (seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not map dynamic shared memory segment")));

	block_info = (BlockInfoRecord *) dsm_segment_address(seg);

	BackgroundWorkerInitializeConnectionByOid(apw_state->database, InvalidOid);
	old_blk = NULL;
	pos = apw_state->prewarm_start_idx;

	while (pos < apw_state->prewarm_stop_idx && have_free_buffer())
	{
		BlockInfoRecord *blk = &block_info[pos++];
		Buffer		buf;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Quit if we've reached records for another database. If previous
		 * blocks are of some global objects, then continue pre-warming.
		 */
		if (old_blk != NULL && old_blk->database != blk->database &&
			old_blk->database != 0)
			break;

		/*
		 * As soon as we encounter a block of a new relation, close the old
		 * relation. Note, that rel will be NULL if try_relation_open failed
		 * previously, in that case there is nothing to close.
		 */
		if (old_blk != NULL && old_blk->filenode != blk->filenode &&
			rel != NULL)
		{
			relation_close(rel, AccessShareLock);
			rel = NULL;
			CommitTransactionCommand();
		}

		/*
		 * Try to open each new relation, but only once, when we first
		 * encounter it. If it's been dropped, skip the associated blocks.
		 */
		if (old_blk == NULL || old_blk->filenode != blk->filenode)
		{
			Oid			reloid;

			Assert(rel == NULL);
			StartTransactionCommand();
			reloid = RelidByRelfilenode(blk->tablespace, blk->filenode);
			if (OidIsValid(reloid))
				rel = try_relation_open(reloid, AccessShareLock);

			if (!rel)
				CommitTransactionCommand();
		}
		if (!rel)
		{
			old_blk = blk;
			continue;
		}

		/* Once per fork, check for fork existence and size. */
		if (old_blk == NULL ||
			old_blk->filenode != blk->filenode ||
			old_blk->forknum != blk->forknum)
		{
			RelationOpenSmgr(rel);

			/*
			 * smgrexists is not safe for illegal forknum, hence check whether
			 * the passed forknum is valid before using it in smgrexists.
			 */
			if (blk->forknum > InvalidForkNumber &&
				blk->forknum <= MAX_FORKNUM &&
				smgrexists(rel->rd_smgr, blk->forknum))
				nblocks = RelationGetNumberOfBlocksInFork(rel, blk->forknum);
			else
				nblocks = 0;
		}

		/* Check whether blocknum is valid and within fork file size. */
		if (blk->blocknum >= nblocks)
		{
			/* Move to next forknum. */
			old_blk = blk;
			continue;
		}

		/* Prewarm buffer. */
		buf = ReadBufferExtended(rel, blk->forknum, blk->blocknum, RBM_NORMAL,
								 NULL);
		if (BufferIsValid(buf))
		{
			apw_state->prewarmed_blocks++;
			ReleaseBuffer(buf);
		}

		old_blk = blk;
	}

	dsm_detach(seg);

	/* Release lock on previous relation. */
	if (rel)
	{
		relation_close(rel, AccessShareLock);
		CommitTransactionCommand();
	}

	return;
}

/*
 * autoprewarm_one_database
 *		Register a per-database dynamic worker to load.
 */
static void
autoprewarm_one_database(void)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle = NULL;
	BgwHandleStatus status PG_USED_FOR_ASSERTS_ONLY;

	setup_autoprewarm(&worker, "per-database autoprewarm",
					  "autoprewarm_database_main",
					  (Datum) NULL, BGW_NEVER_RESTART,
					  BGWORKER_BACKEND_DATABASE_CONNECTION);

	/* Set bgw_notify_pid so that we can use WaitForBackgroundWorkerShutdown */
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("registering dynamic bgworker autoprewarm failed"),
				 errhint("Consider increasing configuration parameter \"max_worker_processes\".")));
	}

	status = WaitForBackgroundWorkerShutdown(handle);
	Assert(status == BGWH_STOPPED);
}

/*
 * autoprewarm_buffers
 *		The main routine that prewarms the shared buffers.
 *
 * The prewarm bgworker will first load all the BlockInfoRecords in
 * $PGDATA/AUTOPREWARM_FILE to a DSM. Further, these BlockInfoRecords are
 * separated based on their databases. Finally, for each group of
 * BlockInfoRecords a per-database worker will be launched to load the
 * corresponding blocks. Launch the next worker only after the previous one has
 * finished its job.
 */
static void
autoprewarm_buffers(void)
{
	FILE	   *file = NULL;
	uint32		num_elements,
				i;
	BlockInfoRecord *blkinfo;
	dsm_segment *seg;

	/*
	 * Since there can be at most one worker for prewarm, locking is not
	 * required for setting skip_prewarm_on_restart.
	 */
	apw_state->skip_prewarm_on_restart = true;

	LWLockAcquire(&apw_state->lock, LW_EXCLUSIVE);
	if (apw_state->pid_using_dumpfile == InvalidPid)
		apw_state->pid_using_dumpfile = MyProcPid;
	else
	{
		LWLockRelease(&apw_state->lock);
		ereport(LOG,
				(errmsg("skipping prewarm because block dump file is being written by PID %d",
						apw_state->pid_using_dumpfile)));
		return;
	}

	LWLockRelease(&apw_state->lock);

	file = AllocateFile(AUTOPREWARM_FILE, "r");
	if (!file)
	{
		if (errno != ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m",
							AUTOPREWARM_FILE)));

		apw_state->pid_using_dumpfile = InvalidPid;
		return;					/* No file to load. */
	}

	if (fscanf(file, "<<%u>>i\n", &num_elements) != 1)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from file \"%s\": %m",
						AUTOPREWARM_FILE)));
	}

	seg = dsm_create(sizeof(BlockInfoRecord) * num_elements, 0);

	blkinfo = (BlockInfoRecord *) dsm_segment_address(seg);

	for (i = 0; i < num_elements; i++)
	{
		/* Get next block. */
		if (5 != fscanf(file, "%u,%u,%u,%u,%u\n", &blkinfo[i].database,
						&blkinfo[i].tablespace, &blkinfo[i].filenode,
						(uint32 *) &blkinfo[i].forknum, &blkinfo[i].blocknum))
			break;
	}

	FreeFile(file);

	if (num_elements != i)
		elog(ERROR, "autoprewarm block dump has %u entries but expected %u",
			 i, num_elements);

	/*
	 * Sort the block number to increase the chance of sequential reads during
	 * load.
	 */
	pg_qsort(blkinfo, num_elements, sizeof(BlockInfoRecord), blockinfo_cmp);

	apw_state->block_info_handle = dsm_segment_handle(seg);
	apw_state->prewarm_start_idx = apw_state->prewarm_stop_idx = 0;
	apw_state->prewarmed_blocks = 0;

	/* Get the info position of the first block of the next database. */
	while (apw_state->prewarm_start_idx < num_elements)
	{
		uint32		i = apw_state->prewarm_start_idx;
		Oid			current_db = blkinfo[i].database;

		/*
		 * Advance the prewarm_stop_idx to the first BlockRecordInfo that does
		 * not belong to this database.
		 */
		i++;
		while (i < num_elements)
		{
			if (current_db != blkinfo[i].database)
			{
				/*
				 * Combine BlockRecordInfos of global object with the next
				 * non-global object.
				 */
				if (current_db != InvalidOid)
					break;
				current_db = blkinfo[i].database;
			}

			i++;
		}

		/*
		 * If we reach this point with current_db == InvalidOid, then only
		 * BlockRecordInfos belonging to global objects exist. Since, we can
		 * not connect with InvalidOid skip prewarming for these objects.
		 */
		if (current_db == InvalidOid)
			break;

		apw_state->prewarm_stop_idx = i;
		apw_state->database = current_db;

		Assert(apw_state->prewarm_start_idx < apw_state->prewarm_stop_idx);

		/*
		 * Register a per-database worker to load blocks of the database. Wait
		 * until it has finished before starting the next worker.
		 */
		autoprewarm_one_database();
		apw_state->prewarm_start_idx = apw_state->prewarm_stop_idx;
	}

	dsm_detach(seg);
	apw_state->block_info_handle = DSM_HANDLE_INVALID;

	apw_state->pid_using_dumpfile = InvalidPid;
	ereport(LOG,
			(errmsg("autoprewarm successfully prewarmed %d of %d previously-loaded blocks",
					apw_state->prewarmed_blocks, num_elements)));
}

/*
 * ============================================================================
 * ==============	Dump part of Autoprewarm =============================
 * ============================================================================
 */

/*
 * This submodule is for periodically dumping BlockRecordInfos in shared
 * buffers into a dump file AUTOPREWARM_FILE.
 * Each entry of BlockRecordInfo consists of database, tablespace, filenode,
 * forknum, blocknum. Note that this is in the text form so that the dump
 * information is readable and can be edited, if required.
 */

/*
 * dump_now
 *		Dumps BlockRecordInfos in shared buffers.
 */
static uint32
dump_now(bool is_bgworker)
{
	uint32		i;
	int			ret;
	uint32		num_blocks;
	BlockInfoRecord *block_info_array;
	BufferDesc *bufHdr;
	FILE	   *file;
	char		transient_dump_file_path[MAXPGPATH];

	LWLockAcquire(&apw_state->lock, LW_EXCLUSIVE);
	if (apw_state->pid_using_dumpfile == InvalidPid)
		apw_state->pid_using_dumpfile = MyProcPid;
	else
	{
		if (!is_bgworker)
			ereport(ERROR,
					(errmsg("could not perform block dump because dump file is being used by PID %d",
							apw_state->pid_using_dumpfile)));

		LWLockRelease(&apw_state->lock);
		ereport(LOG,
				(errmsg("skipping block dump because it is already being performed by PID %d",
						apw_state->pid_using_dumpfile)));
		return 0;
	}

	LWLockRelease(&apw_state->lock);

	block_info_array =
		(BlockInfoRecord *) palloc(sizeof(BlockInfoRecord) * NBuffers);

	for (num_blocks = 0, i = 0; i < NBuffers; i++)
	{
		uint32		buf_state;

		if (!is_bgworker)
			CHECK_FOR_INTERRUPTS();

		bufHdr = GetBufferDescriptor(i);

		/* Lock each buffer header before inspecting. */
		buf_state = LockBufHdr(bufHdr);

		if (buf_state & BM_TAG_VALID)
		{
			block_info_array[num_blocks].database = bufHdr->tag.rnode.dbNode;
			block_info_array[num_blocks].tablespace = bufHdr->tag.rnode.spcNode;
			block_info_array[num_blocks].filenode = bufHdr->tag.rnode.relNode;
			block_info_array[num_blocks].forknum = bufHdr->tag.forkNum;
			block_info_array[num_blocks].blocknum = bufHdr->tag.blockNum;
			++num_blocks;
		}

		UnlockBufHdr(bufHdr, buf_state);
	}

	snprintf(transient_dump_file_path, MAXPGPATH, "%s.tmp", AUTOPREWARM_FILE);
	file = AllocateFile(transient_dump_file_path, "w");
	if (!file)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						transient_dump_file_path)));

	ret = fprintf(file, "<<%u>>\n", num_blocks);
	if (ret < 0)
	{
		int			save_errno = errno;

		unlink(transient_dump_file_path);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\" : %m",
						transient_dump_file_path)));
	}

	for (i = 0; i < num_blocks; i++)
	{
		if (!is_bgworker)
			CHECK_FOR_INTERRUPTS();

		ret = fprintf(file, "%u,%u,%u,%u,%u\n",
					  block_info_array[i].database,
					  block_info_array[i].tablespace,
					  block_info_array[i].filenode,
					  (uint32) block_info_array[i].forknum,
					  block_info_array[i].blocknum);
		if (ret < 0)
		{
			int			save_errno = errno;

			FreeFile(file);
			unlink(transient_dump_file_path);
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\" : %m",
							transient_dump_file_path)));
		}
	}

	pfree(block_info_array);

	/*
	 * Rename transient_dump_file_path to AUTOPREWARM_FILE to make things
	 * permanent.
	 */
	ret = FreeFile(file);
	if (ret != 0)
	{
		int			save_errno = errno;

		unlink(transient_dump_file_path);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\" : %m",
						transient_dump_file_path)));
	}

	(void) durable_rename(transient_dump_file_path, AUTOPREWARM_FILE, ERROR);
	apw_state->pid_using_dumpfile = InvalidPid;

	ereport(DEBUG1,
			(errmsg("saved metadata info of %d blocks", num_blocks)));
	return num_blocks;
}

/*
 * dump_block_info_periodically
 *		 This loop periodically call dump_now().
 *
 * Call dum_now() at regular intervals defined by GUC variable
 * autoprewarm_interval.
 */
void
dump_block_info_periodically(void)
{
	TimestampTz last_dump_time = 0;

	while (!got_sigterm)
	{
		int			rc;
		struct timeval nap;

		nap.tv_sec = AUTOPREWARM_INTERVAL_DEFAULT;
		nap.tv_usec = 0;

		/* In case of a SIGHUP, just reload the configuration. */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (autoprewarm_interval > AUTOPREWARM_INTERVAL_SHUTDOWN_ONLY)
		{
			TimestampTz current_time = GetCurrentTimestamp();

			if (last_dump_time == 0 ||
				TimestampDifferenceExceeds(last_dump_time,
										   current_time,
										   (autoprewarm_interval * 1000)))
			{
				dump_now(true);
				last_dump_time = GetCurrentTimestamp();
				nap.tv_sec = autoprewarm_interval;
				nap.tv_usec = 0;
			}
			else
			{
				long		secs;
				int			usecs;

				TimestampDifference(last_dump_time, current_time,
									&secs, &usecs);
				nap.tv_sec = autoprewarm_interval - secs;
				nap.tv_usec = 0;
			}
		}
		else
			last_dump_time = 0;

		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   (nap.tv_sec * 1000L) + (nap.tv_usec / 1000L),
					   PG_WAIT_EXTENSION);
		ResetLatch(&MyProc->procLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	/* It's time for postmaster shutdown, let's dump for one last time. */
	dump_now(true);
}

/*
 * autoprewarm_main
 *		The main entry point of autoprewarm bgworker process.
 */
void
autoprewarm_main(Datum main_arg)
{
	AutoPrewarmTask todo_task;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGTERM, apw_sigterm_handler);
	pqsignal(SIGHUP, apw_sighup_handler);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);

	/* We're now ready to receive signals. */
	BackgroundWorkerUnblockSignals();

	todo_task = DatumGetInt32(main_arg);
	Assert(todo_task == TASK_PREWARM_BUFFERPOOL ||
		   todo_task == TASK_DUMP_BUFFERPOOL_INFO);
	init_apw_shmem();
	on_shmem_exit(detach_apw_shmem, 0);

	LWLockAcquire(&apw_state->lock, LW_EXCLUSIVE);
	if (apw_state->bgworker_pid != InvalidPid)
	{
		LWLockRelease(&apw_state->lock);
		ereport(LOG,
				(errmsg("autoprewarm worker is already running under PID %d",
						apw_state->bgworker_pid)));
		return;
	}

	apw_state->bgworker_pid = MyProcPid;
	LWLockRelease(&apw_state->lock);

	ereport(LOG,
			(errmsg("autoprewarm worker started")));

	/*
	 * We have finished initializing worker's state, let's start actual work.
	 */
	if (todo_task == TASK_PREWARM_BUFFERPOOL &&
		!apw_state->skip_prewarm_on_restart)
		autoprewarm_buffers();

	dump_block_info_periodically();

	ereport(LOG,
			(errmsg("autoprewarm worker stopped")));
}

/* ============================================================================
 * =============	Extension's entry functions/utilities	===============
 * ============================================================================
 */

/*
 * setup_autoprewarm
 *		A common function to initialize BackgroundWorker structure.
 */
static void
setup_autoprewarm(BackgroundWorker *autoprewarm, const char *worker_name,
				  const char *worker_function, Datum main_arg, int restart_time,
				  int extra_flags)
{
	MemSet(autoprewarm, 0, sizeof(BackgroundWorker));
	autoprewarm->bgw_flags = BGWORKER_SHMEM_ACCESS | extra_flags;

	/* Register the autoprewarm background worker */
	autoprewarm->bgw_start_time = BgWorkerStart_ConsistentState;
	autoprewarm->bgw_restart_time = restart_time;
	strcpy(autoprewarm->bgw_library_name, "pg_prewarm");
	strcpy(autoprewarm->bgw_function_name, worker_function);
	strncpy(autoprewarm->bgw_name, worker_name, BGW_MAXLEN);
	autoprewarm->bgw_main_arg = main_arg;
}

/*
 * _PG_init
 *		Extension's entry point.
 */
void
_PG_init(void)
{
	BackgroundWorker autoprewarm_worker;

	/* Define custom GUC variables. */

	DefineCustomIntVariable("pg_prewarm.autoprewarm_interval",
							"Sets the maximum time between two shared buffers dumps",
							"If set to zero, timer based dumping is disabled.",
							&autoprewarm_interval,
							AUTOPREWARM_INTERVAL_DEFAULT,
							AUTOPREWARM_INTERVAL_SHUTDOWN_ONLY, INT_MAX / 1000,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL,
							NULL,
							NULL);

	if (process_shared_preload_libraries_in_progress)
		DefineCustomBoolVariable("pg_prewarm.autoprewarm",
								 "Starts the autoprewarm worker.",
								 NULL,
								 &autoprewarm,
								 true,
								 PGC_POSTMASTER,
								 0,
								 NULL,
								 NULL,
								 NULL);
	else
	{
		/* If not run as a preloaded library, nothing more to do. */
		EmitWarningsOnPlaceholders("pg_prewarm");
		return;
	}

	EmitWarningsOnPlaceholders("pg_prewarm");

	/* Request additional shared resources. */
	RequestAddinShmemSpace(MAXALIGN(sizeof(AutoPrewarmSharedState)));

	/* If autoprewarm is disabled then nothing more to do. */
	if (!autoprewarm)
		return;

	/* Register autoprewarm worker. */
	setup_autoprewarm(&autoprewarm_worker, "autoprewarm", "autoprewarm_main",
					  Int32GetDatum(TASK_PREWARM_BUFFERPOOL), 0, 0);
	RegisterBackgroundWorker(&autoprewarm_worker);
}

/*
 * autoprewarm_dump_launcher
 *		Dynamically launch an autoprewarm dump worker.
 */
static void
autoprewarm_dump_launcher(void)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;

	setup_autoprewarm(&worker, "autoprewarm", "autoprewarm_main",
					  Int32GetDatum(TASK_DUMP_BUFFERPOOL_INFO), 0, 0);

	/* Set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("registering dynamic bgworker \"autoprewarm\" failed"),
				 errhint("Consider increasing configuration parameter \"max_worker_processes\".")));
	}

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status == BGWH_STOPPED)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start autoprewarm dump bgworker"),
				 errhint("More details may be available in the server log.")));
	}

	if (status == BGWH_POSTMASTER_DIED)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("cannot start bgworker autoprewarm without postmaster"),
				 errhint("Kill all remaining database processes and restart the database.")));
	}

	Assert(status == BGWH_STARTED);
}

/*
 * autoprewarm_start_worker
 *		The C-Language entry function to launch autoprewarm dump bgworker.
 */
Datum
autoprewarm_start_worker(PG_FUNCTION_ARGS)
{
	pid_t		pid;

	init_apw_shmem();
	pid = apw_state->bgworker_pid;
	if (pid != InvalidPid)
		ereport(ERROR,
				(errmsg("autoprewarm worker is already running under PID %d",
						pid)));

	autoprewarm_dump_launcher();
	PG_RETURN_VOID();
}

/*
 * autoprewarm_dump_now
 *		The C-Language entry function to dump immediately.
 */
Datum
autoprewarm_dump_now(PG_FUNCTION_ARGS)
{
	uint32		num_blocks = 0;

	init_apw_shmem();

	PG_TRY();
	{
		num_blocks = dump_now(false);
	}
	PG_CATCH();
	{
		detach_apw_shmem(0, 0);
		PG_RE_THROW();
	}
	PG_END_TRY();
	PG_RETURN_UINT32(num_blocks);
}
