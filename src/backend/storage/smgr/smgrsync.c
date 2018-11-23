/*-------------------------------------------------------------------------
 *
 * smgrsync.c
 *	  management of file synchronization.
 *
 * This modules tracks which files need to be fsynced or unlinked at the
 * next checkpoint, and performs those actions.  Normally the work is done
 * when called by the checkpointer, but it is also done in standalone mode
 * and startup.
 *
 * Originally this logic was inside md.c, but it is now made more general,
 * for reuse by other SMGR implementations that work with files.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/smgrsync.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/xlog.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "portability/instr_time.h"
#include "postmaster/bgwriter.h"
#include "postmaster/checkpointer.h"
#include "storage/relfilenode.h"
#include "storage/smgrsync.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"


/*
 * Special values for the segno member of SmgrFileTag.
 *
 * Note that CompactCheckpointerRequestQueue assumes that it's OK to remove an
 * fsync request from the queue if an identical, subsequent request is found.
 * See comments there before making changes here.
 */
#define FORGET_RELATION_FSYNC	(InvalidBlockNumber)
#define FORGET_DATABASE_FSYNC	(InvalidBlockNumber-1)
#define UNLINK_RELATION_REQUEST (InvalidBlockNumber-2)

/* intervals for calling AbsorbFsyncRequests in smgrsync and smgrpostckpt */
#define FSYNCS_PER_ABSORB		10
#define UNLINKS_PER_ABSORB		10

/*
 * An entry in the hash table of files that need to be flushed for the next
 * checkpoint.
 */
typedef struct PendingFsyncEntry
{
	SmgrFileTag	tag;
	File		file;
	uint64		cycle_ctr;
} PendingFsyncEntry;

typedef struct PendingUnlinkEntry
{
	RelFileNode rnode;			/* the dead relation to delete */
	uint64		cycle_ctr;		/* ckpt_cycle_ctr when request was made */
} PendingUnlinkEntry;

static uint32 open_fsync_queue_files = 0;
static bool sync_in_progress = false;
static uint64 ckpt_cycle_ctr = 0;

static HTAB *pendingFsyncTable = NULL;
static List *pendingUnlinks = NIL;
static MemoryContext pendingOpsCxt; /* context for the above  */

static void syncpass(bool include_current);

/*
 * Initialize the pending operations state, if necessary.
 */
void
smgrsync_init(void)
{
	/*
	 * Create pending-operations hashtable if we need it.  Currently, we need
	 * it if we are standalone (not under a postmaster) or if we are a startup
	 * or checkpointer auxiliary process.
	 */
	if (!IsUnderPostmaster || AmStartupProcess() || AmCheckpointerProcess())
	{
		HASHCTL		hash_ctl;

		/*
		 * XXX: The checkpointer needs to add entries to the pending ops table
		 * when absorbing fsync requests.  That is done within a critical
		 * section, which isn't usually allowed, but we make an exception. It
		 * means that there's a theoretical possibility that you run out of
		 * memory while absorbing fsync requests, which leads to a PANIC.
		 * Fortunately the hash table is small so that's unlikely to happen in
		 * practice.
		 */
		pendingOpsCxt = AllocSetContextCreate(TopMemoryContext,
											  "Pending ops context",
											  ALLOCSET_DEFAULT_SIZES);
		MemoryContextAllowInCriticalSection(pendingOpsCxt, true);

		MemSet(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(SmgrFileTag);
		hash_ctl.entrysize = sizeof(PendingFsyncEntry);
		hash_ctl.hcxt = pendingOpsCxt;
		pendingFsyncTable = hash_create("Pending Ops Table",
									  100L,
									  &hash_ctl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		pendingUnlinks = NIL;
	}
}

/*
 * Do pre-checkpoint work.
 *
 * To distinguish unlink requests that arrived before this checkpoint
 * started from those that arrived during the checkpoint, we use a cycle
 * counter similar to the one we use for fsync requests. That cycle
 * counter is incremented here.
 *
 * This must be called *before* the checkpoint REDO point is determined.
 * That ensures that we won't delete files too soon.
 *
 * Note that we can't do anything here that depends on the assumption
 * that the checkpoint will be completed.
 */
void
smgrpreckpt(void)
{
	/*
	 * Any unlink requests arriving after this point will be assigned the next
	 * cycle counter, and won't be unlinked until next checkpoint.
	 */
	ckpt_cycle_ctr++;
}

/*
 * Sync previous writes to stable storage.
 */
void
smgrsync(void)
{
	/*
	 * This is only called during checkpoints, and checkpoints should only
	 * occur in processes that have created a pendingFsyncTable.
	 */
	if (!pendingFsyncTable)
		elog(ERROR, "cannot sync without a pendingFsyncTable");

	/*
	 * If we are in the checkpointer, the sync had better include all fsync
	 * requests that were queued by backends up to this point.  The tightest
	 * race condition that could occur is that a buffer that must be written
	 * and fsync'd for the checkpoint could have been dumped by a backend just
	 * before it was visited by BufferSync().  We know the backend will have
	 * queued an fsync request before clearing the buffer's dirtybit, so we
	 * are safe as long as we do an Absorb after completing BufferSync().
	 */
	AbsorbAllFsyncRequests();

	syncpass(false);
}

/*
 * Do one pass over the the fsync request hashtable and perform the necessary
 * fsyncs. Increments the sync cycle counter.
 *
 * If include_current is true perform all fsyncs (this is done if too many
 * files are open), otherwise only perform the fsyncs belonging to the cycle
 * valid at call time.
 */
static void
syncpass(bool include_current)
{
	HASH_SEQ_STATUS hstat;
	PendingFsyncEntry *entry;
	int			absorb_counter;

	/* Statistics on sync times */
	instr_time	sync_start,
				sync_end,
				sync_diff;
	uint64		elapsed;
	int			processed = CheckpointStats.ckpt_sync_rels;
	uint64		longest = CheckpointStats.ckpt_longest_sync;
	uint64		total_elapsed = CheckpointStats.ckpt_agg_sync_time;

	/*
	 * To avoid excess fsync'ing (in the worst case, maybe a never-terminating
	 * checkpoint), we want to ignore fsync requests that are entered into the
	 * hashtable after this point --- they should be processed next time,
	 * instead.  We use GetCheckpointSyncCycle() to tell old entries apart
	 * from new ones: new ones will have cycle_ctr equal to
	 * IncCheckpointSyncCycle().
	 *
	 * In normal circumstances, all entries present in the table at this point
	 * will have cycle_ctr exactly equal to the current (about to be old)
	 * value of sync_cycle_ctr.  However, if we fail partway through the
	 * fsync'ing loop, then older values of cycle_ctr might remain when we
	 * come back here to try again.  Repeated checkpoint failures would
	 * eventually wrap the counter around to the point where an old entry
	 * might appear new, causing us to skip it, possibly allowing a checkpoint
	 * to succeed that should not have.  To forestall wraparound, any time the
	 * previous smgrsync() failed to complete, run through the table and
	 * forcibly set cycle_ctr = sync_cycle_ctr.
	 *
	 * Think not to merge this loop with the main loop, as the problem is
	 * exactly that that loop may fail before having visited all the entries.
	 * From a performance point of view it doesn't matter anyway, as this path
	 * will never be taken in a system that's functioning normally.
	 */
	if (sync_in_progress)
	{
		/* prior try failed, so update any stale cycle_ctr values */
		hash_seq_init(&hstat, pendingFsyncTable);
		while ((entry = (PendingFsyncEntry *) hash_seq_search(&hstat)) != NULL)
			entry->cycle_ctr = GetCheckpointSyncCycle();
	}

	/* Set flag to detect failure if we don't reach the end of the loop */
	sync_in_progress = true;

	/* Advance counter so that new hashtable entries are distinguishable */
	IncCheckpointSyncCycle();

	/* Now scan the hashtable for fsync requests to process */
	absorb_counter = FSYNCS_PER_ABSORB;
	hash_seq_init(&hstat, pendingFsyncTable);
	while ((entry = (PendingFsyncEntry *) hash_seq_search(&hstat)))
	{
		/*
		 * If processing fsync requests because of too may file handles, close
		 * regardless of cycle. Otherwise nothing to be closed might be found,
		 * and we want to make room as quickly as possible so more requests
		 * can be absorbed.
		 */
		if (!include_current)
		{
			/* If the entry is new then don't process it this time. */
			if (entry->cycle_ctr == GetCheckpointSyncCycle())
				continue;

			/* Else assert we haven't missed it */
			Assert((entry->cycle_ctr + 1) == GetCheckpointSyncCycle());
		}

		/*
		 * If fsync is off then we don't have to bother opening the file at
		 * all.  (We delay checking until this point so that changing fsync on
		 * the fly behaves sensibly.)
		 *
		 * XXX: Why is that an important goal? Doesn't give any interesting
		 * guarantees afaict?
		 */
		if (enableFsync)
		{
			File		file;

			/*
			 * The fsync table could contain requests to fsync segments that
			 * have been deleted (unlinked) by the time we get to them.  That
			 * used to be problematic, but now we have a filehandle to the
			 * deleted file. That means we might fsync an empty file
			 * superfluously, in a relatively tight window, which is
			 * acceptable.
			 */
			INSTR_TIME_SET_CURRENT(sync_start);

			if (entry->file == -1)
			{
				/*
				 * If we aren't transferring file descriptors directly to the
				 * checkpointer on this platform, we'll have to convert the
				 * tag to the path and open it (and close it again below).
				 */
				char		path[MAXPGPATH];

				smgrpath(&entry->tag, path);
				file = PathNameOpenFile(path, O_RDWR | PG_BINARY);
				if (file < 0)
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not open file \"%s\" to fsync: %m",
									path)));
			}
			else
			{
				/*
				 * Otherwise, we have kept the file descriptor from the oldest
				 * request for the same tag.
				 */
				file = entry->file;
			}

			if (FileSync(file, WAIT_EVENT_DATA_FILE_SYNC) < 0)
				ereport(data_sync_elevel(ERROR),
						(errcode_for_file_access(),
						 errmsg("could not fsync file \"%s\": %m",
								FilePathName(file))));

			/* Success; update statistics about sync timing */
			INSTR_TIME_SET_CURRENT(sync_end);
			sync_diff = sync_end;
			INSTR_TIME_SUBTRACT(sync_diff, sync_start);
			elapsed = INSTR_TIME_GET_MICROSEC(sync_diff);
			if (elapsed > longest)
				longest = elapsed;
			total_elapsed += elapsed;
			processed++;

			if (log_checkpoints)
				ereport(DEBUG1,
						(errmsg("checkpoint sync: number=%d file=%s time=%.3f msec",
								processed,
								FilePathName(file),
								(double) elapsed / 1000),
						 errhidestmt(true),
						 errhidecontext(true)));

			if (entry->file == -1)
				FileClose(file);
		}

		if (entry->file >= 0)
		{
			/*
			 * Close file.  XXX: centralize code.
			 */
			Assert(open_fsync_queue_files > 0);
			open_fsync_queue_files--;
			FileClose(entry->file);
			entry->file = -1;
		}

		/* Remove the entry. */
		if (hash_search(pendingFsyncTable, &entry->tag, HASH_REMOVE, NULL) == NULL)
			elog(ERROR, "pendingFsyncTable corrupted");

		/*
		 * If in checkpointer, we want to absorb pending requests every so
		 * often to prevent overflow of the fsync request queue.  It is
		 * unspecified whether newly-added entries will be visited by
		 * hash_seq_search, but we don't care since we don't need to
		 * process them anyway.
		 */
		if (absorb_counter-- <= 0)
		{
			/*
			 * Don't absorb if too many files are open. This pass will
			 * soon close some, so check again later.
			 */
			if (open_fsync_queue_files < ((max_safe_fds * 7) / 10))
				AbsorbFsyncRequests();
			absorb_counter = FSYNCS_PER_ABSORB;
		}
	}							/* end loop over hashtable entries */

	/* Flag successful completion of syncpass */
	sync_in_progress = false;

	/* Maintain sync performance metrics for report at checkpoint end */
	CheckpointStats.ckpt_sync_rels = processed;
	CheckpointStats.ckpt_longest_sync = longest;
	CheckpointStats.ckpt_agg_sync_time = total_elapsed;
}

/*
 * Do post-checkpoint work.
 *
 * Remove any lingering files that can now be safely removed.
 */
void
smgrpostckpt(void)
{
	int			absorb_counter;

	absorb_counter = UNLINKS_PER_ABSORB;
	while (pendingUnlinks != NIL)
	{
		PendingUnlinkEntry *entry = (PendingUnlinkEntry *) linitial(pendingUnlinks);
		char	   *path;

		/*
		 * New entries are appended to the end, so if the entry is new we've
		 * reached the end of old entries.
		 *
		 * Note: if just the right number of consecutive checkpoints fail, we
		 * could be fooled here by cycle_ctr wraparound.  However, the only
		 * consequence is that we'd delay unlinking for one more checkpoint,
		 * which is perfectly tolerable.
		 */
		if (entry->cycle_ctr == ckpt_cycle_ctr)
			break;

		/* Unlink the file */
		path = relpathperm(entry->rnode, MAIN_FORKNUM);
		if (unlink(path) < 0)
		{
			/*
			 * There's a race condition, when the database is dropped at the
			 * same time that we process the pending unlink requests. If the
			 * DROP DATABASE deletes the file before we do, we will get ENOENT
			 * here. rmtree() also has to ignore ENOENT errors, to deal with
			 * the possibility that we delete the file first.
			 */
			if (errno != ENOENT)
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("could not remove file \"%s\": %m", path)));
		}
		pfree(path);

		/* And remove the list entry */
		pendingUnlinks = list_delete_first(pendingUnlinks);
		pfree(entry);

		/*
		 * As in smgrsync, we don't want to stop absorbing fsync requests for a
		 * long time when there are many deletions to be done.  We can safely
		 * call AbsorbFsyncRequests() at this point in the loop (note it might
		 * try to delete list entries).
		 */
		if (--absorb_counter <= 0)
		{
			/* XXX: Centralize this condition */
			if (open_fsync_queue_files < ((max_safe_fds * 7) / 10))
				AbsorbFsyncRequests();
			absorb_counter = UNLINKS_PER_ABSORB;
		}
	}
}


/*
 * FsyncAtCheckpoint() -- Mark a relation segment as needing fsync
 *
 * If there is a local pending-ops table, just make an entry in it for
 * smgrsync to process later.  Otherwise, try to pass off the fsync request
 * to the checkpointer process.
 */
uint64
FsyncAtCheckpoint(const SmgrFileTag *tag, File file, uint64 last_cycle)
{
	uint64		cycle;

	pg_memory_barrier();
	cycle = GetCheckpointSyncCycle();

	/*
	 * For historical reasons checkpointer keeps track of the number of time
	 * backends perform writes themselves.
	 */
	if (!AmBackgroundWriterProcess())
		CountBackendWrite();

	/* Don't repeatedly register the same segment as dirty. */
	if (last_cycle == cycle)
		return cycle;

	if (pendingFsyncTable)
	{
		int fd;

		/*
		 * Push it into local pending-ops table.
		 *
		 * Gotta duplicate the fd - we can't have fd.c close it behind our
		 * back, as that'd lead to losing error reporting guarantees on
		 * Linux.  RememberFsyncRequest() will manage the lifetime.
		 */
		ReleaseLruFiles();
		fd = dup(FileGetRawDesc(file));
		if (fd < 0)
			elog(ERROR, "couldn't dup: %m");
		RememberFsyncRequest(tag, fd, FileGetOpenSeq(file));
	}
	else
		ForwardFsyncRequest(tag, file);

	return cycle;
}

/*
 * Schedule a file to be deleted after next checkpoint.
 *
 * As with FsyncAtCheckpoint, this could involve either a local or a remote
 * pending-ops table.
 */
void
UnlinkAfterCheckpoint(RelFileNodeBackend rnode)
{
	SmgrFileTag tag;

	tag.node = rnode.node;
	tag.forknum = MAIN_FORKNUM;
	tag.segno = UNLINK_RELATION_REQUEST;

	/* Should never be used with temp relations */
	Assert(!RelFileNodeBackendIsTemp(rnode));

	if (pendingFsyncTable)
	{
		/* push it into local pending-ops table */
		RememberFsyncRequest(&tag, -1, 0);
	}
	else
	{
		/* Notify the checkpointer about it. */
		Assert(IsUnderPostmaster);
		ForwardFsyncRequest(&tag, -1);
	}
}

/*
 * In archive recovery, we rely on checkpointer to do fsyncs, but we will have
 * already created the pendingFsyncTable during initialization of the startup
 * process.  Calling this function drops the local pendingFsyncTable so that
 * subsequent requests will be forwarded to checkpointer.
 */
void
SetForwardFsyncRequests(void)
{
	/* Perform any pending fsyncs we may have queued up, then drop table */
	if (pendingFsyncTable)
	{
		smgrsync();
		hash_destroy(pendingFsyncTable);
	}
	pendingFsyncTable = NULL;

	/*
	 * We should not have any pending unlink requests, since mdunlink doesn't
	 * queue unlink requests when isRedo.
	 */
	Assert(pendingUnlinks == NIL);
}


/*
 * RememberFsyncRequest() -- callback from checkpointer side of fsync request
 *
 * We stuff fsync requests into the local hash table for execution
 * during the checkpointer's next checkpoint.  UNLINK requests go into a
 * separate linked list, however, because they get processed separately.
 *
 * The range of possible segment numbers is way less than the range of
 * BlockNumber, so we can reserve high values of segno for special purposes.
 * We define three:
 * - FORGET_RELATION_FSYNC means to cancel pending fsyncs for a relation,
 *	 either for one fork, or all forks if forknum is InvalidForkNumber
 * - FORGET_DATABASE_FSYNC means to cancel pending fsyncs for a whole database
 * - UNLINK_RELATION_REQUEST is a request to delete the file after the next
 *	 checkpoint.
 * Note also that we're assuming real segment numbers don't exceed INT_MAX.
 *
 * (Handling FORGET_DATABASE_FSYNC requests is a tad slow because the hash
 * table has to be searched linearly, but dropping a database is a pretty
 * heavyweight operation anyhow, so we'll live with it.)
 */
void
RememberFsyncRequest(const SmgrFileTag *tag, int fd, uint64 open_seq)
{
	Assert(pendingFsyncTable);

	if (tag->segno == FORGET_RELATION_FSYNC ||
		tag->segno == FORGET_DATABASE_FSYNC)
	{
		HASH_SEQ_STATUS hstat;
		PendingFsyncEntry *entry;

		/* Remove fsync requests */
		hash_seq_init(&hstat, pendingFsyncTable);
		while ((entry = (PendingFsyncEntry *) hash_seq_search(&hstat)) != NULL)
		{
			if ((tag->segno == FORGET_RELATION_FSYNC &&
				 tag->node.dbNode == entry->tag.node.dbNode &&
				 tag->node.relNode == entry->tag.node.relNode &&
				 (tag->forknum == InvalidForkNumber ||
				  tag->forknum == entry->tag.forknum)) ||
				(tag->segno == FORGET_DATABASE_FSYNC &&
				 tag->node.dbNode == entry->tag.node.dbNode))
			{
				if (entry->file != -1)
				{
					Assert(open_fsync_queue_files > 0);
					open_fsync_queue_files--;
					FileClose(entry->file);
				}
				hash_search(pendingFsyncTable, entry, HASH_REMOVE, NULL);
			}
		}

		/* Remove unlink requests */
		if (tag->segno == FORGET_DATABASE_FSYNC)
		{
			ListCell   *cell,
					   *next,
					   *prev;

			prev = NULL;
			for (cell = list_head(pendingUnlinks); cell; cell = next)
			{
				PendingUnlinkEntry *entry = (PendingUnlinkEntry *) lfirst(cell);

				next = lnext(cell);
				if (tag->node.dbNode == entry->rnode.dbNode)
				{
					pendingUnlinks = list_delete_cell(pendingUnlinks, cell,
													  prev);
					pfree(entry);
				}
				else
					prev = cell;
			}
		}
	}
	else if (tag->segno == UNLINK_RELATION_REQUEST)
	{
		/* Unlink request: put it in the linked list */
		MemoryContext oldcxt = MemoryContextSwitchTo(pendingOpsCxt);
		PendingUnlinkEntry *entry;

		/* PendingUnlinkEntry doesn't store forknum, since it's always MAIN */
		Assert(tag->forknum == MAIN_FORKNUM);

		entry = palloc(sizeof(PendingUnlinkEntry));
		entry->rnode = tag->node;
		entry->cycle_ctr = ckpt_cycle_ctr;

		pendingUnlinks = lappend(pendingUnlinks, entry);

		MemoryContextSwitchTo(oldcxt);
	}
	else
	{
		/* Normal case: enter a request to fsync this segment */
		MemoryContext oldcxt = MemoryContextSwitchTo(pendingOpsCxt);
		PendingFsyncEntry *entry;
		bool		found;

		entry = (PendingFsyncEntry *) hash_search(pendingFsyncTable,
												  tag,
												  HASH_ENTER,
												  &found);
		/* if new entry, initialize it */
		if (!found)
		{
			entry->file = -1;
			entry->cycle_ctr = GetCheckpointSyncCycle();
		}

		/*
		 * NB: it's intentional that we don't change cycle_ctr if the entry
		 * already exists.  The cycle_ctr must represent the oldest fsync
		 * request that could be in the entry.
		 */

		if (fd >= 0)
		{
			File existing_file;
			File new_file;

			/*
			 * If we didn't have a file already, or we did have a file but it
			 * was opened later than this one, we'll keep the newly arrived
			 * one.
			 */
			existing_file = entry->file;
			if (existing_file == -1 ||
				FileGetOpenSeq(existing_file) > open_seq)
			{
				char path[MAXPGPATH];

				smgrpath(tag, path);

				new_file = FileOpenForFd(fd, path, open_seq);
				if (new_file < 0)
					elog(ERROR, "cannot open file");
				/* caller must have reserved entry */
				entry->file = new_file;

				if (existing_file != -1)
					FileClose(existing_file);
				else
					open_fsync_queue_files++;
			}
			else
			{
				/*
				 * File is already open. Have to keep the older fd, errors
				 * might only be reported to it, thus close the one we just
				 * got.
				 *
				 * XXX: check for errors.
				 */
				close(fd);
			}

			FlushFsyncRequestQueueIfNecessary();
		}

		MemoryContextSwitchTo(oldcxt);
	}
}

/*
 * Flush the fsync request queue enough to make sure there's room for at least
 * one more entry.
 */
bool
FlushFsyncRequestQueueIfNecessary(void)
{
	if (sync_in_progress)
		return false;

	while (true)
	{
		if (open_fsync_queue_files >= ((max_safe_fds * 7) / 10))
		{
			elog(DEBUG1,
				 "flush fsync request queue due to %u open files",
				 open_fsync_queue_files);
			syncpass(true);
			elog(DEBUG1,
				 "flushed fsync request, now at %u open files",
				 open_fsync_queue_files);
		}
		else
			break;
	}

	return true;
}

/*
 * ForgetRelationFsyncRequests -- forget any fsyncs for a relation fork
 *
 * forknum == InvalidForkNumber means all forks, although this code doesn't
 * actually know that, since it's just forwarding the request elsewhere.
 */
void
ForgetRelationFsyncRequests(RelFileNode rnode, ForkNumber forknum)
{
	SmgrFileTag tag;

	/* Create a special "forget relation" tag. */
	tag.node = rnode;
	tag.forknum = forknum;
	tag.segno = FORGET_RELATION_FSYNC;

	if (pendingFsyncTable)
	{
		/* standalone backend or startup process: fsync state is local */
		RememberFsyncRequest(&tag, -1, 0);
	}
	else if (IsUnderPostmaster)
	{
		/* Notify the checkpointer about it. */
		ForwardFsyncRequest(&tag, -1);

		/*
		 * Note we don't wait for the checkpointer to actually absorb the
		 * cancel message; see smgrsync() for the implications.
		 */
	}
}

/*
 * ForgetDatabaseFsyncRequests -- forget any fsyncs and unlinks for a DB
 */
void
ForgetDatabaseFsyncRequests(Oid dbid)
{
	SmgrFileTag tag;

	/* Create a special "forget database" tag. */
	tag.node.dbNode = dbid;
	tag.node.spcNode = 0;
	tag.node.relNode = 0;
	tag.forknum = InvalidForkNumber;
	tag.segno = FORGET_DATABASE_FSYNC;

	if (pendingFsyncTable)
	{
		/* standalone backend or startup process: fsync state is local */
		RememberFsyncRequest(&tag, -1, 0);
	}
	else if (IsUnderPostmaster)
	{
		/* see notes in ForgetRelationFsyncRequests */
		ForwardFsyncRequest(&tag, -1);
	}
}
