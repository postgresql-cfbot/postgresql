/*-------------------------------------------------------------------------
 *
 * undolog.c
 *	  management of undo logs
 *
 * PostgreSQL undo log manager.  This module is responsible for managing the
 * lifecycle of undo logs and their segment files, associating undo logs with
 * backends, and allocating space within undo logs.
 *
 * For the code that reads and writes blocks of data to the operating system,
 * see undofile.c.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/undo/undolog.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/session.h"
#include "access/undo.h"
#include "access/undolog.h"
#include "access/undolog_xlog.h"
#include "access/undopage.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "catalog/pg_class.h"
#include "catalog/pg_tablespace_d.h"
#include "commands/tablespace.h"
#include "funcapi.h"
#include "lib/ilist.h"
#include "lib/qunique.h"
#include "miscadmin.h"
#include "storage/procarray.h"
#include "storage/undofile.h"
#include "utils/builtins.h"
#include "utils/acl.h"
#include "utils/guc.h"
#include "utils/varlena.h"

/*
 * Main control structure for undo log management in shared memory.
 * UndoLogSlot objects are arranged in a fixed-size array, with no particular
 * ordering.
 */
typedef struct UndoLogSharedData
{
	slist_head	shared_free_lists[NUndoPersistenceLevels];
	UndoLogNumber low_logno;
	UndoLogNumber next_logno;
	UndoLogNumber nslots;
	UndoLogSlot slots[FLEXIBLE_ARRAY_MEMBER];
} UndoLogSharedData;

/* The shared memory region that all backends are attach to. */
UndoLogSharedData *UndoLogShared;

/* The per-backend cache of undo log number -> slot mappings. */
undologtable_hash *undologtable_cache;

/* The per-backend lowest known undo log number, for cache invalidation. */
UndoLogNumber undologtable_low_logno;

/* GUC variables */
char	   *undo_tablespaces = NULL;

static UndoLogSlot *allocate_undo_log_slot(void);
static void free_undo_log_slot(UndoLogSlot *log);
static void discard_undo_buffers(int logno, UndoLogOffset old_discard,
								 UndoLogOffset new_discard,
								 bool drop_tail);
static void scan_physical_range(void);
static bool choose_undo_tablespace(Oid *tablespace);

/*
 * How many undo logs can be active at a time?  This creates a theoretical
 * maximum amount of undo data that can exist, but if we set it to a multiple
 * of the maximum number of backends it will be a very high limit.
 * Alternative designs involving demand paging or dynamic shared memory could
 * remove this limit but would be complicated.
 */
static inline size_t
UndoLogNumSlots(void)
{
	return MaxBackends * 4;
}

/*
 * Return the amount of traditional shmem required for undo log management.
 */
Size
UndoLogShmemSize(void)
{
	return sizeof(UndoLogSharedData) +
		UndoLogNumSlots() * sizeof(UndoLogSlot);
}

/*
 * Initialize the undo log subsystem.  Called in each backend.
 */
void
UndoLogShmemInit(void)
{
	bool		found;

	UndoLogShared = (UndoLogSharedData *)
		ShmemInitStruct("UndoLogShared", UndoLogShmemSize(), &found);

	/* The postmaster initialized the shared memory state. */
	if (!IsUnderPostmaster)
	{
		Assert(!found);

		/*
		 * We start with no active undo logs.  StartUpUndoLogs() will recreate
		 * the undo logs that were known at the last checkpoint.
		 */
		memset(UndoLogShared, 0, sizeof(*UndoLogShared));
		UndoLogShared->nslots = UndoLogNumSlots();
		for (int i = 0; i < NUndoPersistenceLevels; ++i)
			slist_init(&UndoLogShared->shared_free_lists[i]);
		for (int i = 0; i < UndoLogShared->nslots; ++i)
		{
			memset(&UndoLogShared->slots[i], 0, sizeof(UndoLogShared->slots[i]));
			UndoLogShared->slots[i].logno = InvalidUndoLogNumber;
			LWLockInitialize(&UndoLogShared->slots[i].meta_lock,
							 LWTRANCHE_UNDOLOG);
			LWLockInitialize(&UndoLogShared->slots[i].file_lock,
							 LWTRANCHE_UNDOFILE);
		}
	}
	else
		Assert(found);

	/* Regular backends and standlone processes need a per-backend cache. */

	/*
	 * XXX: If if appears to exist already, then we much be in a crash restart
	 * and the memory context that contains the old one has already been
	 * reset. So proceed to create a new one.  This is horrible.
	 */
	/* Assert(undologtable_cache == NULL); */
	undologtable_cache = undologtable_create(UndoContext,
											 UndoLogNumSlots(),
											 NULL);

	/*
	 * Each backend has its own idea of the lowest undo log number in
	 * existence, and can trim undologtable_create entries when it advances.
	 */
	LWLockAcquire(UndoLogLock, LW_SHARED);
	undologtable_low_logno = UndoLogShared->low_logno;
	LWLockRelease(UndoLogLock);
}

/*
 * Figure out which directory holds an undo log based on tablespace.
 */
void
UndoLogDirectory(Oid tablespace, char *dir)
{
	if (tablespace == DEFAULTTABLESPACE_OID ||
		tablespace == InvalidOid)
		snprintf(dir, MAXPGPATH, "base/undo");
	else
		snprintf(dir, MAXPGPATH, "pg_tblspc/%u/%s/undo",
				 tablespace, TABLESPACE_VERSION_DIRECTORY);
}

/*
 * Compute the pathname to use for an undo log segment file.
 */
void
UndoLogSegmentPath(UndoLogNumber logno, int segno, Oid tablespace, char *path)
{
	char		dir[MAXPGPATH];

	/* Figure out which directory holds the segment, based on tablespace. */
	UndoLogDirectory(tablespace, dir);

	/*
	 * Build the path from log number and offset.  The pathname is the
	 * UndoRecPtr of the first byte in the segment in hexadecimal, with a
	 * period inserted between the components.
	 */
	snprintf(path, MAXPGPATH, "%s/%06X.%010zX", dir, logno,
			 segno * UndoLogSegmentSize);
}

/*
 * Check if an undo log position has been discarded.  'pointer' must be an
 * undo log pointer that was allocated at some point in the past, otherwise
 * the result is undefined.
 */
bool
UndoLogRecPtrIsDiscardedSlowPath(UndoRecPtr pointer)
{
	UndoLogNumber logno = UndoRecPtrGetLogNo(pointer);
	UndoLogSlot *slot;
	UndoRecPtr	discard;

	slot = UndoLogGetSlot(logno, true);

	if (slot == NULL)
	{
		/*
		 * If we couldn't find the undo log number, then it must be entirely
		 * discarded.  Set this backend's recent_discard value to the highest
		 * possible value, so that all records appear to be discarded to the
		 * fast-path code.
		 */
		discard = MakeUndoRecPtr(logno, UndoLogMaxSize);
	}
	else
	{
		LWLockAcquire(&slot->meta_lock, LW_SHARED);
		if (unlikely(logno != slot->logno))
		{
			/*
			 * The undo log has been entirely discarded since we looked it up
			 * above, and the UndoLogSlot is now unused or being used for some
			 * other undo log.  This is the same as not finding it.
			 */
			discard = MakeUndoRecPtr(logno, UndoLogMaxSize);
		}
		else
			discard = MakeUndoRecPtr(logno, slot->meta.discard);
		LWLockRelease(&slot->meta_lock);
	}

	/*
	 * Remember this discard pointer in this backend so that future lookups
	 * via UndoLogRecPtrIsDiscarded() have a chance of avoiding the slow path.
	 */
	UndoLogGetTableEntry(logno)->recent_discard = discard;

	return pointer < discard;
}

/*
 * Return all undo logs from our private free lists to the shared free lists.
 */
static void
clear_private_free_lists(void)
{
	if (!CurrentSession)
		return;

	for (int i = 0; i < NUndoPersistenceLevels; ++i)
	{
		UndoLogSlot *slot = CurrentSession->private_undolog_free_lists[i];

		if (slot)
		{
			LWLockAcquire(UndoLogLock, LW_EXCLUSIVE);
			LWLockAcquire(&slot->meta_lock, LW_EXCLUSIVE);
			/* Check pid because DROP TABLESPACE might have taken it from us. */
			if (slot->pid == MyProcPid)
			{
				Assert(slot->state == UNDOLOGSLOT_ON_PRIVATE_FREE_LIST);
				slot->pid = InvalidPid;
				slot->xid = InvalidTransactionId;
				slot->state = UNDOLOGSLOT_ON_SHARED_FREE_LIST;
				slist_push_head(&UndoLogShared->shared_free_lists[GetUndoPersistenceLevel(slot->meta.persistence)],
								&slot->next);
			}
			LWLockRelease(&slot->meta_lock);
			LWLockRelease(UndoLogLock);
		}
	}
}

/*
 * Return all undo logs from our private free lists to the shared free lists.
 */
void
AtProcExit_UndoLog(void)
{
	clear_private_free_lists();
}

/*
 * Create a new empty segment file on disk for the byte starting at 'end'.
 */
static void
allocate_empty_undo_segment(UndoLogNumber logno, Oid tablespace,
							UndoLogOffset end)
{
	struct stat stat_buffer;
	off_t		size;
	char		path[MAXPGPATH];
	void	   *zeroes;
	size_t		nzeroes = 8192;
	int			fd;

	UndoLogSegmentPath(logno, end / UndoLogSegmentSize, tablespace, path);

	/*
	 * Create and fully allocate a new file.  If we crashed and recovered then
	 * the file might already exist, so use flags that tolerate that. It's
	 * also possible that it exists but is too short, in which case we'll
	 * write the rest.  We don't really care what's in the file, we just want
	 * to make sure that the filesystem has allocated physical blocks for it,
	 * so that non-COW filesystems will report ENOSPC now rather than later
	 * when the space is needed and we'll avoid creating files with holes.
	 */
	fd = OpenTransientFile(path, O_RDWR | O_CREAT | PG_BINARY);
	if (fd < 0 && tablespace != 0)
	{
		char		undo_path[MAXPGPATH];

		/* Try creating the undo directory for this tablespace. */
		UndoLogDirectory(tablespace, undo_path);
		if (MakePGDirectory(undo_path) != 0 && errno != EEXIST)
		{
			char	   *parentdir;

			if (errno != ENOENT || !InRecovery)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not create directory \"%s\": %m",
								undo_path)));

			/*
			 * In recovery, it's possible that the tablespace directory
			 * doesn't exist because a later WAL record removed the whole
			 * tablespace.  In that case we create a regular directory to
			 * stand in for it.  This is similar to the logic in
			 * TablespaceCreateDbspace().
			 */

			/* create two parents up if not exist */
			parentdir = pstrdup(undo_path);
			get_parent_directory(parentdir);
			get_parent_directory(parentdir);
			/* Can't create parent and it doesn't already exist? */
			if (MakePGDirectory(parentdir) < 0 && errno != EEXIST)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not create directory \"%s\": %m",
								parentdir)));
			pfree(parentdir);

			/* create one parent up if not exist */
			parentdir = pstrdup(undo_path);
			get_parent_directory(parentdir);
			/* Can't create parent and it doesn't already exist? */
			if (MakePGDirectory(parentdir) < 0 && errno != EEXIST)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not create directory \"%s\": %m",
								parentdir)));
			pfree(parentdir);

			if (MakePGDirectory(undo_path) != 0 && errno != EEXIST)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not create directory \"%s\": %m",
								undo_path)));
		}

		fd = OpenTransientFile(path, O_RDWR | O_CREAT | PG_BINARY);
	}
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create new file \"%s\": %m", path)));
	if (fstat(fd, &stat_buffer) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fstat \"%s\": %m", path)));
	size = stat_buffer.st_size;

	/* A buffer full of zeroes we'll use to fill up new segment files. */
	zeroes = palloc0(nzeroes);

	while (size < UndoLogSegmentSize)
	{
		ssize_t		written;

		written = write(fd, zeroes, Min(nzeroes, UndoLogSegmentSize - size));
		if (written < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not initialize file \"%s\": %m", path)));
		size += written;
	}

	/*
	 * Ask the checkpointer to flush the contents of the file to disk before
	 * the next checkpoint.
	 */
	undofile_request_sync(logno, end / UndoLogSegmentSize, tablespace);

	CloseTransientFile(fd);

	pfree(zeroes);

	elog(DEBUG1, "created undo segment \"%s\"", path);
}

/*
 * Create a new undo segment, when it is unexpectedly not present.
 */
void
UndoLogNewSegment(UndoLogNumber logno, Oid tablespace, int segno)
{
	Assert(InRecovery);
	allocate_empty_undo_segment(logno, tablespace, segno * UndoLogSegmentSize);

	/*
	 * Ask the checkpointer to flush the new directory entry before next
	 * checkpoint.
	 */
	undofile_request_sync_dir(tablespace);
}

/*
 * At startup we scan the filesystem to find the range of physical storage for
 * each undo log.  This is recorded in 'begin' and 'end' in shared memory.
 * Since it runs at startup time, it is excused from the locking rules when
 * accessing UndoLogSlot entries.
 */
static void
scan_physical_range(void)
{
	Oid		   *tablespaces = palloc0(UndoLogNumSlots() * sizeof(Oid));
	int			ntablespaces = 0;


	/* Compute the set of tablespace directories to inspect. */
	for (int i = 0; i < UndoLogNumSlots(); ++i)
	{
		UndoLogSlot *slot = &UndoLogShared->slots[i];

		if (slot->logno == InvalidUndoLogNumber)
			continue;

		slot->begin = 0;
		slot->end = 0;
		tablespaces[ntablespaces++] = slot->meta.tablespace;
	}

	/* Compute the unique set of tablespaces. */
	qsort(tablespaces, ntablespaces, sizeof(Oid), oid_cmp);

	/* Make the set of tablespaces unique. */
	ntablespaces = qunique(tablespaces, ntablespaces, sizeof(Oid), oid_cmp);

	/* Visit every tablespace, looking for files. */
	for (int i = 0; i < ntablespaces; ++i)
	{
		char		tablespace_path[MAXPGPATH];
		DIR		   *dir;
		struct dirent *de;

		UndoLogDirectory(tablespaces[i], tablespace_path);

		/* Try to open the tablespace directory. */
		dir = AllocateDir(tablespace_path);
		if (!dir)
		{
			elog(LOG, "tablespace directory \"%s\" unexpectedly doesn't exist, while scanning undo logs",
				 tablespace_path);
			continue;
		}

		/* Scan the list of files. */
		while ((de = ReadDirExtended(dir, tablespace_path, LOG)))
		{
			UndoLogNumber logno;
			UndoLogOffset offset;
			int			offset_high;
			int			offset_low;
			UndoLogSlot *slot;

			if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
				continue;

			/* Can we parse the name as a segment file name? */
			if (strlen(de->d_name) != 17 ||
				sscanf(de->d_name, "%06X.%02X%08X", &logno, &offset_high, &offset_low) != 3)
			{
				elog(LOG, "unexpected file \"%s\" in \"%s\"", de->d_name, tablespace_path);
				continue;
			}

			/* Does it refer to an undo log that exists? */
			slot = UndoLogGetSlot(logno, true);
			if (!slot)
			{
				/*
				 * The segment might belong to an undo log that will be
				 * created later in the WAL, in crash recovery.  It could also
				 * be left-over junk after a crash, but for now we don't try
				 * to figure that out.
				 */
				continue;
			}

			/*
			 * Track the range of files we've seen for this undo log.  In
			 * various crash scenarios there could be holes in the sequence,
			 * but as long as all files covering the range [discard, insert)
			 * exist we'll tolerate that, recreating missing files as
			 * necessary.
			 */
			offset = ((UndoLogOffset) offset_high << 32) | offset_low;
			slot->begin = Min(offset, slot->begin);
			slot->end = Max(offset + UndoLogSegmentSize, slot->end);
		}
		FreeDir(dir);
	}
}

/*
 * Make sure that we have physical files to cover the range new_discard up to
 * but not including new_insert.  This unlinks unnecessary files, or renames
 * them to become new files, or creates new zero-filled files as appropriate
 * to advance 'begin' and 'end' to cover the given range.  Either value may be
 * given as zero, meaning don't advance that end (though advancing the begin
 * pointer can caus the end pointer to advance if a file can be renamed).
 */
void
UndoLogAdjustPhysicalRange(UndoLogNumber logno,
						   UndoLogOffset new_discard,
						   UndoLogOffset new_insert)
{
	UndoLogSlot *slot;
	UndoLogOffset new_begin = 0;
	UndoLogOffset new_end = 0;
	UndoLogOffset begin;
	UndoLogOffset end;
	UndoLogOffset insert,
				size;
	int			recycle = 0;

	/*
	 * Round new_discard down to the nearest segment boundary.  That's the
	 * lowest-numbered segment we need to keep.
	 */
	new_begin = new_discard - new_discard % UndoLogSegmentSize;

	slot = UndoLogGetSlot(logno, true);

	/*
	 * UndoLogDiscard() and UndoLogAllocate() can both reach this code, so we
	 * serialize access.
	 */
	LWLockAcquire(&slot->file_lock, LW_EXCLUSIVE);
	if (slot->logno != logno)
	{
		/*
		 * If it was entirely discarded while we were thinking about it, the
		 * slot could have been recycled and we now have nothign to do.
		 */
		LWLockRelease(&slot->file_lock);
		return;
	}

	/* Fetch information that will be needed below. */
	LWLockAcquire(&slot->meta_lock, LW_SHARED);
	insert = slot->meta.insert;
	size = slot->meta.size;
	end = slot->end;
	LWLockRelease(&slot->meta_lock);

	if (new_insert == 0)
		new_insert = insert;

	/*
	 * Round new_insert up to the nearest segment boundary, to make a valid
	 * end offset.  This will be one past the highest-numbered setgment we
	 * need to keep, but we may decide to keep more, below.
	 */
	new_end = new_insert + UndoLogSegmentSize - new_insert % UndoLogSegmentSize;

	/* First, deal with advancing 'begin', if we were asked to do that. */
	if (new_discard != 0)
	{
		/*
		 * Can we try to recycle an old segment to become a new one?  For now
		 * we only consider creating one spare segment.
		 */
		if (new_begin > slot->begin && (end + UndoLogSegmentSize) <= size)
			recycle = 1;

		for (begin = slot->begin;
			 begin < new_begin;
			 begin += UndoLogSegmentSize)
		{
			char		old_path[MAXPGPATH];

			/* Tell the checkpointer that the file is going away. */
			undofile_forget_sync(logno, begin / UndoLogSegmentSize,
								 slot->meta.tablespace);

			UndoLogSegmentPath(logno, begin / UndoLogSegmentSize,
							   slot->meta.tablespace, old_path);

			/*
			 * Rename or unlink as required.  Tolerate ENOENT, because some
			 * crash scenarios could leave holes in the sequence of segment
			 * files.
			 */
			if (recycle > 0)
			{
				char		new_path[MAXPGPATH];

				UndoLogSegmentPath(logno, end / UndoLogSegmentSize,
								   slot->meta.tablespace, new_path);

				if (rename(old_path, new_path) == 0)
				{
					elog(DEBUG1, "recycled undo segment \"%s\" -> \"%s\"",
						 old_path, new_path);
					--recycle;
					end += UndoLogSegmentSize;
				}
				else if (errno != ENOENT)
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not rename \"%s\" to \"%s\": %m",
									old_path, new_path)));
			}
			else
			{
				if (unlink(old_path) == 0)
					elog(DEBUG1, "unlinked undo segment \"%s\"", old_path);
				else if (errno != ENOENT)
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not unlink \"%s\": %m",
									old_path)));
			}
		}
	}

	/*
	 * Next, deal with advancing 'end', if we were asked to do that.
	 *
	 * Note that here we skip the segment(s) we got by recycling the old
	 * one(s). UndoPrepareToInsert() should initialize the pages before the
	 * first write. XXX pg_undo_dump will report errors if it hits the old
	 * contents - is that a serious issue?
	 */
	for (; end < new_end; end += UndoLogSegmentSize)
		allocate_empty_undo_segment(logno, UndoLogNumberGetTablespace(logno), end);

	/*
	 * If recycling added more space than required, make sure the slot
	 * metadata reflects that.
	 */
	if (end > new_end)
		new_end = end;

	/*
	 * Ask the checkpointer to flush the directory entries before next
	 * checkpoint, to make sure that newly created files can't disappear.
	 */
	undofile_request_sync_dir(slot->meta.tablespace);

	/* Update shared memory. */
	LWLockAcquire(&slot->meta_lock, LW_EXCLUSIVE);
	if (new_begin != 0)
		slot->begin = new_begin;
	slot->end = new_end;
	LWLockRelease(&slot->meta_lock);

	LWLockRelease(&slot->file_lock);
}

/*
 * Find or make an undo log for use in an UndoRecordSet.
 *
 * If min_logno is valid, use only slot with this or higher value of logno.
 */
UndoLogSlot *
UndoLogAcquire(char persistence, UndoLogNumber min_logno)
{
	UndoLogSlot *slot = NULL;
	slist_mutable_iter iter;
	xl_undolog_create xlrec;
	UndoPersistenceLevel plevel = GetUndoPersistenceLevel(persistence);
	bool		ts_lock_held = false;
	Oid			tablespace;

	/*
	 * Can we use a recently used undo log from our private free list? XXX Not
	 * actually a list yet, just a single item
	 *
	 * The locally cached slot cannot be used if the current URS imposes a
	 * limit on the logno, however we don't remove it because the next URS can
	 * start there.
	 */
	if (CurrentSession)
		slot = CurrentSession->private_undolog_free_lists[plevel];

	if (slot &&
		(min_logno == InvalidUndoLogNumber || slot->logno >= min_logno))
	{
		bool		dropped;

		LWLockAcquire(&slot->meta_lock, LW_EXCLUSIVE);

		/*
		 * While this slot was sitting on our private free list, the
		 * tablespace that contains it might have been dropped.  In that case,
		 * the PID will have been cleared (and perhaps even assigned some
		 * other PID), because it's no longer ours.
		 */
		if (slot->pid != MyProcPid)
		{
			/*
			 * Nope, it's been dropped, and the slot might even have been
			 * reused for a new undo log.  Forget about it.
			 */
			CurrentSession->private_undolog_free_lists[plevel] = NULL;
			dropped = true;
		}
		else
		{
			/* It's still ours.  Acquire. */
			Assert(slot->meta.persistence == persistence);
			Assert(slot->state == UNDOLOGSLOT_ON_PRIVATE_FREE_LIST);
			CurrentSession->private_undolog_free_lists[plevel] = NULL;
			slot->state = UNDOLOGSLOT_IN_UNDO_RECORD_SET;
			dropped = false;
		}
		LWLockRelease(&slot->meta_lock);

		if (!dropped)
			return slot;
	}

	/*
	 * Decide on a tablespace.  This acquires TablespaceCreateLock while
	 * resolving non-default names from the undo_tablespaces GUC, in which
	 * case we need to hold it until we've managed to get an UndoLogSlot into
	 * the right state.  That's how we prevent the tablespace you named in
	 * undo_tablespaces from being dropped concurrently.
	 */
	ts_lock_held = choose_undo_tablespace(&tablespace);

	/* Is there a suitable undo log on the appropriate shared freelist? */
	LWLockAcquire(UndoLogLock, LW_EXCLUSIVE);
	slist_foreach_modify(iter, &UndoLogShared->shared_free_lists[plevel])
	{
		slot = slist_container(UndoLogSlot, next, iter.cur);

		LWLockAcquire(&slot->meta_lock, LW_EXCLUSIVE);
		if (slot->meta.tablespace == tablespace &&
			(min_logno == InvalidUndoLogNumber ||
			 slot->meta.logno >= min_logno))
		{
			slist_delete_current(&iter);

			Assert(slot->meta.persistence == persistence);
			Assert(slot->state == UNDOLOGSLOT_ON_SHARED_FREE_LIST);
			slot->pid = MyProcPid;
			slot->xid = GetTopTransactionIdIfAny();
			slot->state = UNDOLOGSLOT_IN_UNDO_RECORD_SET;
			LWLockRelease(&slot->meta_lock);
			LWLockRelease(UndoLogLock);

			if (ts_lock_held)
				LWLockRelease(TablespaceCreateLock);

			return slot;
		}
		LWLockRelease(&slot->meta_lock);
	}

	/* We'll have to make a new one. */
	if (unlikely(UndoLogShared->next_logno > MaxUndoLogNumber))
	{
		/*
		 * You've used up all 16 exabytes of undo log addressing space.  This
		 * is expected to be a difficult state to reach using only 16 exabytes
		 * of WAL.
		 */
		elog(ERROR, "undo log address space exhausted");
	}

	/* Allocate and initialize a slot. */
	slot = allocate_undo_log_slot();
	if (unlikely(!slot))
		ereport(ERROR,
				(errmsg("could not create new undo log"),
				 errdetail("The maximum number of active undo logs is %zu.",
						   UndoLogNumSlots()),
				 errhint("Consider increasing max_connections.")));
	slot->logno = UndoLogShared->next_logno;
	Assert(slot->logno >= min_logno);
	slot->meta.insert = SizeOfUndoPageHeaderData;
	slot->meta.discard = SizeOfUndoPageHeaderData;
	slot->meta.logno = UndoLogShared->next_logno;
	slot->meta.tablespace = tablespace;
	slot->meta.persistence = persistence;
	slot->meta.size = UndoLogMaxSize;
	slot->pid = MyProcPid;
	slot->xid = GetTopTransactionIdIfAny();
	slot->state = UNDOLOGSLOT_IN_UNDO_RECORD_SET;

	/* Write a WAL log. */
	xlrec.logno = UndoLogShared->next_logno;
	xlrec.tablespace = slot->meta.tablespace;
	xlrec.persistence = slot->meta.persistence;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfUndologCreate);
	XLogInsert(RM_UNDOLOG_ID, XLOG_UNDOLOG_CREATE);

	++UndoLogShared->next_logno;

	LWLockRelease(UndoLogLock);

	if (ts_lock_held)
		LWLockRelease(TablespaceCreateLock);

	return slot;
}

/*
 * Return an UndoLogSlot to the local or shared free list.
 */
void
UndoLogRelease(UndoLogSlot *slot)
{
	UndoPersistenceLevel plevel;

	/* XXX explain why it's OK to do this without a lock */
	Assert(slot->state == UNDOLOGSLOT_IN_UNDO_RECORD_SET);
	plevel = GetUndoPersistenceLevel(slot->meta.persistence);

	/*
	 * For now, we'll only allow you to keep one undo log around on a private
	 * free "list", and push everything else back on the shared free list.
	 *
	 * TODO: Figure out a decent policy.  Maybe a GUC?  Why might you want
	 * more than one "sticky?" undo log per persistence level?  It could be
	 * useful to be able to get fast access to two, if in future we need them
	 * for multixact-like lock tracking.
	 */
	if (!CurrentSession->private_undolog_free_lists[plevel])
	{
		slot->state = UNDOLOGSLOT_ON_PRIVATE_FREE_LIST;
		CurrentSession->private_undolog_free_lists[plevel] = slot;
	}
	else
	{
		LWLockAcquire(UndoLogLock, LW_EXCLUSIVE);
		LWLockAcquire(&slot->meta_lock, LW_EXCLUSIVE);
		slot->pid = InvalidPid;
		slot->xid = InvalidTransactionId;
		slot->state = UNDOLOGSLOT_ON_SHARED_FREE_LIST;
		LWLockRelease(&slot->meta_lock);
		slist_push_head(&UndoLogShared->shared_free_lists[plevel], &slot->next);
		LWLockRelease(UndoLogLock);
	}
}

/*
 * size is passed because caller may need to insert the XLOG record before
 * changing the size in the shared memory.
 */
void
UndoLogTruncate(UndoLogSlot *uls, UndoLogOffset size)
{
	xl_undolog_truncate xlrec;

	/* TODO thinking about timing problems with checkpoints */

	xlrec.logno = uls->meta.logno;
	xlrec.size = size;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfUndologTruncate);
	XLogInsert(RM_UNDOLOG_ID, XLOG_UNDOLOG_TRUNCATE);
}

/*
 * Advance the discard pointer in one undo log, discarding all undo data
 * relating to one or more whole transactions.  The passed in undo pointer is
 * the address of the oldest data that the caller would like to keep, and the
 * affected undo log is implied by this pointer, ie
 * UndoRecPtrGetLogNo(discard_pointer).  Possibly also advance one or both of
 * the begin and end pointers (the range of physical storage), when segment
 * boundaries are crossed.
 *
 * After this call returns, all buffers that are wholly in the discarded range
 * will be discarded with DiscardBuffer().  Readers and writers must be
 * prepared to deal with InvalidBuffer when attempting to read, but already
 * pinned buffers remain valid but are specially marked to avoid writeback.
 * This arrangement allows us to avoid more heavy duty interlocking with
 * backends that may be reading or writing undo log contents.
 */
void
UndoDiscard(UndoRecPtr discard_point)
{
	UndoLogNumber logno = UndoRecPtrGetLogNo(discard_point);
	UndoLogOffset new_discard = UndoRecPtrGetOffset(discard_point);
	UndoLogOffset begin;
	UndoLogOffset insert;
	UndoLogOffset old_discard;
	UndoLogSlot *slot;
	bool		entirely_discarded;
	XLogRecPtr	recptr = InvalidXLogRecPtr;

	slot = UndoLogGetSlot(logno, true);
	if (unlikely(slot == NULL))
	{
		/*
		 * There is no slot for this undo log number, so it must be entirely
		 * discarded.
		 */
		return;
	}

	LWLockAcquire(&slot->meta_lock, LW_SHARED);
	if (unlikely(slot->logno != logno))
	{
		/* Already discarded entirely and the slot has been freed. */
		LWLockRelease(&slot->file_lock);
		return;
	}
	old_discard = slot->meta.discard;
	insert = slot->meta.insert;
	begin = slot->begin;
	entirely_discarded = insert == slot->meta.size;
	LWLockRelease(&slot->meta_lock);

	/*
	 * Sanity checks.  During crash recovery, we might finish up moving the
	 * discard pointer backwards, because CheckPointUndoLogs() could have
	 * captured a later version.
	 */
	if (unlikely(new_discard < old_discard && !InRecovery))
		elog(ERROR, "cannot move discard point backwards");
	if (unlikely(new_discard > insert && !entirely_discarded))
		elog(ERROR, "cannot move discard point past insert point");

	/*
	 * Log the discard operation in the WAL before updating anything in shared
	 * memory.  If we crossed a segment boundary and need to remove one or
	 * more segment files, we'll have to flush this WAL record, but defer that
	 * until just before we perform the filesystem operations in the hope of
	 * better pipelining.
	 */
	if (!InRecovery &&
		UndoLogNumberGetPersistence(logno) == RELPERSISTENCE_PERMANENT)
	{
		xl_undolog_discard xlrec;

		xlrec.logno = logno;
		xlrec.discard = new_discard;
		xlrec.entirely_discarded = entirely_discarded;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfUndologDiscard);
		recptr = XLogInsert(RM_UNDOLOG_ID, XLOG_UNDOLOG_DISCARD);
	}

	/*
	 * Update meta-data in shared memory.  After this is done, undofile_read()
	 * will begin to return false so that ReadBuffer() functions return
	 * invalid buffer for buffers before new_discard.  No new buffers in the
	 * discarded range can enter the buffer pool.
	 *
	 * If a concurrent checkpoint begins after the WAL record is logged, but
	 * before we update shared memory here, then the checkpoint will have the
	 * non-advanced begin, discard and end points.  That's OK; if we recover
	 * from that checkpoint and replay the WAL record, we'll try to discard
	 * again in recovery.
	 */
	LWLockAcquire(&slot->meta_lock, LW_EXCLUSIVE);
	if (likely(slot->logno == logno))
		slot->meta.discard = new_discard;
	LWLockRelease(&slot->meta_lock);

	/*
	 * Try to invalidate all existing buffers in the discarded range.  Any
	 * that can't be invalidated because they are currently pinned will remain
	 * valid, but have BM_DISCARDED set.  Either way, they can never be
	 * written back again after this point, so it's safe to unlink the
	 * underlying files after this point.
	 */
	discard_undo_buffers(logno, old_discard, new_discard, entirely_discarded);

	/*
	 * Have we crossed a segment file boundary?  If so, we'll need to do some
	 * filesystem operations.  Now that we've discarded all buffers, the
	 * buffer manager can't attempt to write back any data [TODO: really?  do
	 * we need to wait for in progress I/O to finish?], so it's safe to unlink
	 * or move files.  If there are any unused, perform expensive filesystem
	 * operations.
	 */
	if (new_discard / UndoLogSegmentSize > begin / UndoLogSegmentSize)
	{
		/*
		 * If we WAL-logged this discard operation (ie it's not temporary or
		 * unlogged), we need to flush that WAL record before we unlink any
		 * files.  This makes sure that we can't have a discard pointer that
		 * points to a non-existing file, after crash recovery.
		 */
		if (!XLogRecPtrIsInvalid(recptr))
			XLogFlush(recptr);
		UndoLogAdjustPhysicalRange(logno, new_discard, 0);
	}
}

/*
 * Write out the undo log meta data to the pg_undo directory.  The actual
 * contents of undo logs is in shared buffers and therefore handled by
 * CheckPointBuffers(), but here we record the table of undo logs and their
 * properties.
 */
void
CheckPointUndoLogs(UndoCheckpointContext *ctx)
{
	UndoLogMetaData *serialized = NULL;
	size_t		serialized_size = 0;
	char	   *data;
	UndoLogNumber num_logs;
	UndoLogNumber next_logno;
	int			i;
	UndoLogSlot **slots_to_free;
	int			nslots_to_free;

	/*
	 * We acquire UndoLogLock to prevent any undo logs from being created or
	 * discarded while we build a snapshot of them.  This isn't expected to
	 * take long on a healthy system because the number of active logs should
	 * be around the number of backends.  Holding this lock won't prevent
	 * concurrent access to the undo log, except when segments need to be
	 * added or removed.
	 */
	LWLockAcquire(UndoLogLock, LW_SHARED);

	/*
	 * Rather than doing the file I/O while we hold locks, we'll copy the
	 * meta-data into a palloc'd buffer.
	 */
	serialized_size = sizeof(UndoLogMetaData) * UndoLogNumSlots();
	serialized = (UndoLogMetaData *) palloc0(serialized_size);

	/*
	 * While we're scanning all slots, look for those that can now be freed
	 * because they hold no data (all discarded) and the insert pointer has
	 * reached 'size' (the end of this log).
	 */
	slots_to_free = palloc0(sizeof(UndoLogSlot *) * UndoLogNumSlots());
	nslots_to_free = 0;

	/* Scan through all slots looking for non-empty ones. */
	num_logs = 0;
	for (i = 0; i < UndoLogNumSlots(); ++i)
	{
		UndoLogSlot *slot = &UndoLogShared->slots[i];

		/* Skip empty slots. */
		if (slot->logno == InvalidUndoLogNumber)
			continue;

		/* Capture snapshot while holding each meta_lock. */
		LWLockAcquire(&slot->meta_lock, LW_SHARED);
		serialized[num_logs++] = slot->meta;
		if (slot->meta.discard == slot->meta.insert &&
			slot->meta.discard == slot->meta.size)
			slots_to_free[nslots_to_free++] = slot;
		LWLockRelease(&slot->meta_lock);
	}
	next_logno = UndoLogShared->next_logno;

	LWLockRelease(UndoLogLock);

	/* Write out the next log number and the number of active logs. */
	WriteUndoCheckpointData(ctx, &next_logno, sizeof(next_logno));
	WriteUndoCheckpointData(ctx, &num_logs, sizeof(num_logs));

	/* Write out the meta data for all active undo logs. */
	data = (char *) serialized;
	serialized_size = num_logs * sizeof(UndoLogMetaData);
	WriteUndoCheckpointData(ctx, data, serialized_size);
	pfree(serialized);

	for (int i = 0; i < nslots_to_free; ++i)
		free_undo_log_slot(slots_to_free[i]);
	pfree(slots_to_free);
}

/*
 * Find the new lowest existing undo log number.  This will allow very
 * long lived backends to give back some memory used in undologtable_cache
 * for ancient entirely discard undo logs.
 */
static void
compute_low_logno(void)
{
	UndoLogNumber low_logno;

	Assert(LWLockHeldByMeInMode(UndoLogLock, LW_EXCLUSIVE));

	low_logno = UndoLogShared->next_logno;
	for (UndoLogNumber i = 0; i < UndoLogNumSlots(); ++i)
	{
		UndoLogSlot *slot = &UndoLogShared->slots[i];

		if (slot->meta.logno != InvalidUndoLogNumber)
			low_logno = Min(slot->logno, low_logno);
	}
	UndoLogShared->low_logno = low_logno;
}

void
StartupUndoLogs(UndoCheckpointContext *ctx)
{
	int			i;
	int			nlogs;

	/* Read the active log number range. */
	ReadUndoCheckpointData(ctx, &UndoLogShared->next_logno,
						   sizeof(UndoLogShared->next_logno));
	ReadUndoCheckpointData(ctx, &nlogs, sizeof(nlogs));

	/*
	 * We'll acquire UndoLogLock just because allocate_undo_log() asserts we
	 * hold it (we don't actually expect concurrent access yet).
	 */
	LWLockAcquire(UndoLogLock, LW_EXCLUSIVE);

	/* Initialize all the logs and set up the freelist. */
	for (i = 0; i < nlogs; ++i)
	{
		UndoLogSlot *slot;

		/*
		 * Get a new UndoLogSlot.  If this checkpoint was created on a system
		 * with a higher max_connections setting, it's theoretically possible
		 * that we don't have enough space and cannot start up.
		 */
		slot = allocate_undo_log_slot();
		if (!slot)
			ereport(ERROR,
					(errmsg("not enough undo log slots to recover from checkpoint: need at least %d, have %zu",
							nlogs, UndoLogNumSlots()),
					 errhint("Consider increasing max_connections")));

		/* Read in the meta data for this undo log. */
		ReadUndoCheckpointData(ctx, &slot->meta, sizeof(slot->meta));

		/*
		 * At normal start-up, or during recovery, all active undo logs start
		 * out on the appropriate free list.
		 */
		slot->logno = slot->meta.logno;
		slot->pid = InvalidPid;
		slot->xid = InvalidTransactionId;
		slot->state = UNDOLOGSLOT_ON_SHARED_FREE_LIST;

		/* XXX isn't the state wrong if this doesn't do anything? */
		if (slot->meta.insert < slot->meta.size ||
			slot->meta.discard < slot->meta.insert)
			slist_push_head(&UndoLogShared->shared_free_lists[GetUndoPersistenceLevel(slot->meta.persistence)],
							&slot->next);
	}

	/*
	 * Initialize the lowest undo log number.  Backends don't need negative
	 * undologtable_cache entries below this number.
	 */
	compute_low_logno();

	LWLockRelease(UndoLogLock);

	/* Find the current begin and end pointers for each log. */
	scan_physical_range();
}

/*
 * Allocate a new UndoLogSlot object.
 */
static UndoLogSlot *
allocate_undo_log_slot(void)
{
	UndoLogSlot *slot;
	UndoLogNumber i;

	Assert(LWLockHeldByMeInMode(UndoLogLock, LW_EXCLUSIVE));

	for (i = 0; i < UndoLogNumSlots(); ++i)
	{
		slot = &UndoLogShared->slots[i];
		if (slot->logno == InvalidUndoLogNumber)
		{
			memset(&slot->meta, 0, sizeof(slot->meta));
			slot->pid = 0;
			slot->logno = -1;
			return slot;
		}
	}

	return NULL;
}

/*
 * Free an UndoLogSlot object in shared memory, so that it can be reused.
 * This is a rare event, and has complications for all code paths that access
 * slots.
 */
static void
free_undo_log_slot(UndoLogSlot *slot)
{
	/*
	 * When removing an undo log from a slot in shared memory, we acquire
	 * UndoLogLock, slot->meta_lock and slot->file_lock, so that other code
	 * can hold any one of those locks to prevent the slot from being
	 * recycled.
	 */
	LWLockAcquire(UndoLogLock, LW_EXCLUSIVE);
	LWLockAcquire(&slot->file_lock, LW_EXCLUSIVE);
	LWLockAcquire(&slot->meta_lock, LW_EXCLUSIVE);
	Assert(slot->logno != InvalidUndoLogNumber);
	slot->logno = InvalidUndoLogNumber;
	slot->state = UNDOLOGSLOT_FREE;
	memset(&slot->meta, 0, sizeof(slot->meta));
	LWLockRelease(&slot->meta_lock);
	LWLockRelease(&slot->file_lock);
	compute_low_logno();
	LWLockRelease(UndoLogLock);
}


/*
 * Get a pointer to an UndoLogSlot object corresponding to a given logno.
 *
 * In general, the caller must acquire the UndoLogSlot's meta_lock to access
 * the contents, and at that time must consider that the logno might have
 * changed because the undo log it contained has been entirely discarded.
 *
 * If the calling backend is currently attached to the undo log, that is not
 * possible, because logs can only reach UNDO_LOG_STATUS_DISCARDED after first
 * reaching UNDO_LOG_STATUS_FULL, and that only happens while detaching.
 *
 * Return NULL if the undo log has been entirely discarded.  It is an error to
 * ask for undo logs that have never been created.
 */
UndoLogSlot *
UndoLogGetSlot(UndoLogNumber logno, bool missing_ok)
{
	UndoLogSlot *slot = NULL;
	UndoLogTableEntry *entry;
	bool		found;

	Assert(!LWLockHeldByMe(UndoLogLock));

	/* First see if we already have it in our cache. */
	entry = undologtable_lookup(undologtable_cache, logno);
	if (likely(entry))
		slot = entry->slot;
	else
	{
		UndoLogNumber i;

		LWLockAcquire(UndoLogLock, LW_SHARED);

		/* Nope.  Linear search for the slot in shared memory. */
		for (i = 0; i < UndoLogNumSlots(); ++i)
		{
			if (UndoLogShared->slots[i].logno == logno)
			{
				/* Found it. */

				/*
				 * TODO: Should this function be usable in a critical section?
				 * Would it make sense to detect that we are in a critical
				 * section and just return the pointer to the log without
				 * updating the cache, to avoid any chance of allocating
				 * memory?
				 */

				entry = undologtable_insert(undologtable_cache, logno, &found);
				entry->number = logno;
				entry->slot = &UndoLogShared->slots[i];
				entry->tablespace = entry->slot->meta.tablespace;
				entry->persistence = entry->slot->meta.persistence;
				entry->recent_discard =
					MakeUndoRecPtr(logno, entry->slot->meta.discard);
				slot = entry->slot;
				break;
			}
		}

		/*
		 * While we have the lock, opportunistically see if we can advance our
		 * local record of the lowest known undo log, freeing cache memory and
		 * avoiding the need to create a negative cache entry.  This should be
		 * very rare.
		 */
		while (undologtable_low_logno < UndoLogShared->low_logno)
			undologtable_delete(undologtable_cache, undologtable_low_logno++);

		/*
		 * If we didn't find it, then it must already have been entirely
		 * discarded.  We create a negative cache entry so that we can answer
		 * this question quickly next time, unless it's below the known lowest
		 * logno.
		 *
		 * TODO: We could track the lowest known undo log number, to reduce
		 * the negative cache entry bloat.
		 */
		if (slot == NULL && logno >= undologtable_low_logno)
		{
			/*
			 * Sanity check: except during recovery, the caller should not be
			 * asking about undo logs that have never existed.
			 */
			if (logno >= UndoLogShared->next_logno)
			{
				if (!InRecovery)
					elog(ERROR, "undo log %u hasn't been created yet", logno);
			}
			else
			{
				entry = undologtable_insert(undologtable_cache, logno, &found);
				entry->number = logno;
				entry->slot = NULL;
				entry->tablespace = 0;
			}
		}

		LWLockRelease(UndoLogLock);
	}

	if (slot == NULL && !missing_ok)
		elog(ERROR, "unknown undo log number %d", logno);

	return slot;
}

/*
 * Visit every undo log.  This is only exported for the use of
 * CloseDanglingUndoRecordSets().
 *
 * XXX Find a better way to do this, I don't really like the way UndoLogSlot
 * is exposed by this file at all
 */
UndoLogSlot *
UndoLogGetNextSlot(UndoLogSlot *slot)
{
	/* Start at the beginning, or after the last value used. */
	if (slot == NULL)
		slot = &UndoLogShared->slots[0];
	else
		++slot;

	/* Return the next occupied slot. */
	while (slot != &UndoLogShared->slots[0] + UndoLogNumSlots())
	{
		if (slot->logno != InvalidUndoLogNumber)
			return slot;
		++slot;
	}

	/* No more slots. */
	return NULL;
}

/* check_hook: validate new undo_tablespaces */
bool
check_undo_tablespaces(char **newval, void **extra, GucSource source)
{
	char	   *rawname;
	List	   *namelist;

	/* Need a modifiable copy of string */
	rawname = pstrdup(*newval);

	/*
	 * Parse string into list of identifiers, just to check for
	 * well-formedness (unfortunateley we can't validate the names in the
	 * catalog yet).
	 */
	if (!SplitIdentifierString(rawname, ',', &namelist))
	{
		/* syntax error in name list */
		GUC_check_errdetail("List syntax is invalid.");
		pfree(rawname);
		list_free(namelist);
		return false;
	}

	/*
	 * Make sure we aren't already in a transaction that has been assigned an
	 * XID.  XXX A better test might be: do we have any UndoRecordSets?  The
	 * UndoRecordSet machinery doesn't currently support changing in the
	 * middle of a transaction (though it could with some more work).
	 */
	if (GetTopTransactionIdIfAny() != InvalidTransactionId)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 (errmsg("undo_tablespaces cannot be changed while a transaction is in progress"))));

	pfree(rawname);
	list_free(namelist);

	return true;
}

/* assign_hook: do extra actions as needed */
void
assign_undo_tablespaces(const char *newval, void *extra)
{
	/*
	 * Forget about anything on the private free lists.  This will force
	 * UndoLogAcquire() to reevaluate the GUC.
	 */
	clear_private_free_lists();
}

/*
 * Examine the undo_tablespaces GUC to find the OID of the tablespace this
 * backend should use for undo data.
 */
static bool
choose_undo_tablespace(Oid *tablespace)
{
	char	   *rawname;
	List	   *namelist;
	bool		need_to_unlock;
	int			length;

	/* We need a modifiable copy of string. */
	rawname = pstrdup(undo_tablespaces);

	/* Break string into list of identifiers. */
	if (!SplitIdentifierString(rawname, ',', &namelist))
		elog(ERROR, "undo_tablespaces is unexpectedly malformed");

	length = list_length(namelist);
	if (length == 0 ||
		(length == 1 && ((char *) linitial(namelist))[0] == '\0'))
	{
		/*
		 * If it's an empty string, then we'll use the default tablespace.  No
		 * locking is required because that can't be dropped.
		 */
		*tablespace = DEFAULTTABLESPACE_OID;
		need_to_unlock = false;
	}
	else
	{
		/*
		 * Choose an OID using our pid, so that if several backends have the
		 * same multi-tablespace setting they'll spread out.  We could easily
		 * do better than this if more serious load balancing is judged
		 * useful.
		 */
		int			index = MyProcPid % length;
		int			first_index = index;
		Oid			oid = InvalidOid;

		/*
		 * Take the tablespace create/drop lock.  This prevents the tablespace
		 * from being dropped.  The caller will have to release this lock,
		 * after having set up an UndoLogSlot so that
		 * DropUndoLogsInTablespace() stops allowing to be dropped.
		 */
		LWLockAcquire(TablespaceCreateLock, LW_SHARED);
		for (;;)
		{
			const char *name = list_nth(namelist, index);
			AclResult	aclresult;

			/* Try to resolve the name to an OID. */
			oid = get_tablespace_oid(name, true);
			if (oid == InvalidOid)
			{
				/* Unknown tablespace, try the next one. */
				index = (index + 1) % length;

				/*
				 * But if we've tried them all, it's time to complain.  We'll
				 * arbitrarily complain about the last one we tried in the
				 * error message.
				 */
				if (index == first_index)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
							 errmsg("tablespace \"%s\" does not exist", name),
							 errhint("Create the tablespace or set undo_tablespaces to a valid or empty list.")));
				continue;
			}

			if (oid == GLOBALTABLESPACE_OID)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("undo logs cannot be placed in pg_global tablespace")));

			/* Check permissions. */
			aclresult = pg_tablespace_aclcheck(oid, GetUserId(), ACL_CREATE);
			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, OBJECT_TABLESPACE, name);

			/* If we got here we succeeded in finding one we can use. */
			break;
		}

		Assert(oid != InvalidOid);
		*tablespace = oid;
		need_to_unlock = true;
	}

	pfree(rawname);
	list_free(namelist);

	return need_to_unlock;
}

/*
 * Try to drop all undo logs in the given tablespace.  Return true if
 * successful, or false if that couldn't be done because we found undo logs
 * that were currently in use in an UndoRecordSet, or that held undiscarded
 * data.
 */
bool
DropUndoLogsInTablespace(Oid tablespace)
{
	DIR		   *dir;
	char		undo_path[MAXPGPATH];
	UndoLogSlot **dropped_slots;
	UndoLogNumber *dropped_lognos;
	int			ndropped;

	/*
	 * While we hold TablespaceCreateLock, new undo logs can be created, but
	 * not in a non-default tablespace.  Therefore it's safe to make just one
	 * pass through the set of slots looking for the tablespace being dropped.
	 */
	Assert(LWLockHeldByMe(TablespaceCreateLock));
	Assert(tablespace != DEFAULTTABLESPACE_OID);

	/* First, try to kick everyone off any undo logs in this tablespace. */
	for (int i = 0; i < UndoLogNumSlots(); ++i)
	{
		bool		ok_to_drop = false;
		bool		skip = false;
		bool		move_to_shared_free_list = false;
		UndoLogSlot *slot = &UndoLogShared->slots[i];

		/* Check if this undo log is interesting and permits dropping. */
		LWLockAcquire(&slot->meta_lock, LW_EXCLUSIVE);
		if (slot->state == UNDOLOGSLOT_FREE ||
			slot->meta.tablespace != tablespace)
		{
			/* Not interesting. */
			skip = true;
		}
		else if (slot->meta.discard == slot->meta.insert &&
				 (slot->state == UNDOLOGSLOT_ON_SHARED_FREE_LIST ||
				  slot->state == UNDOLOGSLOT_ON_PRIVATE_FREE_LIST) &&
				 (slot->xid == InvalidTransactionId ||
				  !TransactionIdIsInProgress(slot->xid)))
		{
			/* No undiscarded data, and not currently in use. */
			if (slot->pid != InvalidPid)
			{
				/*
				 * We can't actually remove things from some other backend's
				 * private free list, but we can remove its PID so that it
				 * notices that we've seized it.  After we free the slot, it
				 * might even be reused by some other backend, but it'll never
				 * have this backend's PID again.
				 */
				Assert(slot->state == UNDOLOGSLOT_ON_PRIVATE_FREE_LIST);
				slot->pid = InvalidPid;
				slot->xid = InvalidTransactionId;
				move_to_shared_free_list = true;
			}
			ok_to_drop = true;
		}
		else
		{
			/*
			 * There is undiscarded data in this undo log, or it's busy in an
			 * UndoRecordSet that will insert some.  We can't let it be
			 * dropped.
			 */
			Assert(slot->meta.discard != slot->meta.insert ||
				   slot->state == UNDOLOGSLOT_IN_UNDO_RECORD_SET);
			ok_to_drop = false;
		}
		LWLockRelease(&slot->meta_lock);

		/* Is this slot irrelevant? */
		if (skip)
			continue;

		/* Did we find a reason for DROP TABLESPACE to fail? */
		if (!ok_to_drop)
			return false;

		/*
		 * It's OK to free this particular slot, but first we'll put it back
		 * on the appropriate shared free list.  No one can attach to it while
		 * we hold TablespaceCreateLock, but we might bail out in a future
		 * iteration of this loop, and in that case we need the undo log to
		 * remain usable.  We'll remove all appropriate logs from the free
		 * lists in a separate step below once we know that the DROP is
		 * cleared to proceed.
		 */
		if (move_to_shared_free_list)
		{
			LWLockAcquire(UndoLogLock, LW_EXCLUSIVE);
			LWLockAcquire(&slot->meta_lock, LW_EXCLUSIVE);
			slot->state = UNDOLOGSLOT_ON_SHARED_FREE_LIST;
			slist_push_head(&UndoLogShared->shared_free_lists[GetUndoPersistenceLevel(slot->meta.persistence)],
							&slot->next);
			LWLockRelease(&slot->meta_lock);
			LWLockRelease(UndoLogLock);
		}
	}

	/*
	 * We didn't find any undo logs that should prevent this tablespace from
	 * being dropped.  We moved any undo logs in this tablespace from private
	 * free lists to shared free lists.  No backend can acquire a non-default
	 * tablespace undo logs while we hold TablespaceCreateLock.  Therefore it
	 * is now safe to free the undo log slots.
	 *
	 * Bear in mind that CheckPointUndoLogs() might concurrently decide to
	 * free a slot for its own reasons, so we have to keep rechecking the
	 * state.
	 */
	for (int i = 0; i < UndoLogNumSlots(); ++i)
	{
		UndoLogSlot *slot = &UndoLogShared->slots[i];
		UndoLogNumber logno;
		UndoLogOffset discard;

		LWLockAcquire(&slot->meta_lock, LW_SHARED);
		if (slot->logno == InvalidUndoLogNumber || slot->meta.tablespace != tablespace)
		{
			LWLockRelease(&slot->meta_lock);
			continue;
		}
		logno = slot->meta.logno;
		discard = slot->meta.discard;
		LWLockRelease(&slot->meta_lock);

		/*
		 * Make sure no buffers remain.  When that is done by
		 * UndoLogDiscard(), the final page is left in shared_buffers because
		 * it may contain data, or at least be needed again very soon.  Here
		 * we need to drop even that page from the buffer pool.
		 */
		discard_undo_buffers(logno, discard, discard, true);

		/*
		 * Well drop this log.  The rest of its address space is wasted.
		 * Normally we would need a WAL record to update the full location,
		 * but this is covered by WAL record for dropping the tablespace.
		 */
		LWLockAcquire(&slot->meta_lock, LW_EXCLUSIVE);
		slot->meta.discard = slot->meta.size = slot->meta.insert;
		LWLockRelease(&slot->meta_lock);
	}

	/* Forget about all sync requests relating to this tablespace. */
	undofile_forget_sync_tablespace(tablespace);

	/* Unlink all undo segment files in this tablespace. */
	UndoLogDirectory(tablespace, undo_path);

	dir = AllocateDir(undo_path);
	if (dir != NULL)
	{
		struct dirent *de;

		while ((de = ReadDirExtended(dir, undo_path, LOG)) != NULL)
		{
			char		segment_path[MAXPGPATH];

			if (strcmp(de->d_name, ".") == 0 ||
				strcmp(de->d_name, "..") == 0)
				continue;
			snprintf(segment_path, sizeof(segment_path), "%s/%s",
					 undo_path, de->d_name);
			if (unlink(segment_path) < 0)
				elog(LOG, "couldn't unlink file \"%s\": %m", segment_path);
		}
		FreeDir(dir);
	}

	dropped_slots = palloc(sizeof(UndoLogSlot *) * UndoLogNumSlots());
	dropped_lognos = palloc(sizeof(UndoLogNumber) * UndoLogNumSlots());
	ndropped = 0;

	/* Remove all dropped undo logs from the free-lists. */
	LWLockAcquire(UndoLogLock, LW_EXCLUSIVE);
	for (int i = 0; i < NUndoPersistenceLevels; ++i)
	{
		UndoLogSlot *slot;
		slist_mutable_iter iter;

		slist_foreach_modify(iter, &UndoLogShared->shared_free_lists[i])
		{
			slot = slist_container(UndoLogSlot, next, iter.cur);

			LWLockAcquire(&slot->meta_lock, LW_SHARED);
			if (slot->meta.discard == slot->meta.insert &&
				slot->meta.size == slot->meta.insert)
			{
				slist_delete_current(&iter);
				dropped_lognos[ndropped] = slot->meta.logno;
				dropped_slots[ndropped++] = slot;
			}
			LWLockRelease(&slot->meta_lock);
		}
	}
	LWLockRelease(UndoLogLock);

	/* Free all the dropped slots. */
	for (int i = 0; i < ndropped; ++i)
		free_undo_log_slot(dropped_slots[i]);

	pfree(dropped_slots);
	pfree(dropped_lognos);

	return true;
}

/*
 * Reset undo logs of a given persistence level.  This should be called at
 * startup to blow away temporary undo logs, because only temporary relations
 * should hold references to temporary undo logs, and temporary relations don't
 * survive restart.  The same applies to unlogged relations and unlogged undo
 * logs, if there is a crash restart.
 */
void
ResetUndoLogs(char persistence)
{
	Assert(persistence == RELPERSISTENCE_TEMP ||
		   persistence == RELPERSISTENCE_UNLOGGED);

	for (int i = 0; i < UndoLogNumSlots(); ++i)
	{
		DIR		   *dir;
		struct dirent *de;
		char		undo_path[MAXPGPATH];
		char		segment_prefix[MAXPGPATH];
		size_t		segment_prefix_size;
		UndoLogSlot *slot;

		slot = &UndoLogShared->slots[i];

		if (slot->logno == InvalidUndoLogNumber)
			continue;

		if (slot->meta.persistence != persistence)
			continue;

		/* Scan the directory for files belonging to this undo log. */
		snprintf(segment_prefix, sizeof(segment_prefix), "%06X.", slot->logno);
		segment_prefix_size = strlen(segment_prefix);
		UndoLogDirectory(slot->meta.tablespace, undo_path);
		dir = AllocateDir(undo_path);
		if (dir == NULL)
			continue;
		while ((de = ReadDirExtended(dir, undo_path, LOG)) != NULL)
		{
			char		segment_path[MAXPGPATH];

			if (strncmp(de->d_name, segment_prefix, segment_prefix_size) != 0)
				continue;
			snprintf(segment_path, sizeof(segment_path), "%s/%s",
					 undo_path, de->d_name);
			elog(DEBUG1, "unlinked undo segment \"%s\"", segment_path);
			if (unlink(segment_path) < 0)
				elog(LOG, "couldn't unlink file \"%s\": %m", segment_path);
		}
		FreeDir(dir);

		/*
		 * We have no segment files.  Set the pointers to indicate that there
		 * is no data.  The discard and insert pointers point to the first
		 * usable byte in the segment we will create when we next try to
		 * allocate.  This is a bit strange, because it means that they are
		 * past the end pointer.  That's the same as when new undo logs are
		 * created.
		 *
		 * TODO: Should we rewind to zero instead, so we can reuse that (now)
		 * unreferenced address space?  Or should we put the log numbers on a
		 * free list where they cna be used again for any persistence level?
		 */
		slot->meta.insert = slot->meta.discard = slot->end +
			SizeOfUndoPageHeaderData;
	}
}

Datum
pg_stat_get_undo_logs(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_UNDO_LOGS_COLS 9
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	char	   *tablespace_name = NULL;
	Oid			last_tablespace = InvalidOid;
	int			i;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/* Scan all undo logs to build the results. */
	for (i = 0; i < UndoLogShared->nslots; ++i)
	{
		UndoLogSlot *slot = &UndoLogShared->slots[i];
		char		buffer[UndoRecPtrFormatBufferLength];
		Datum		values[PG_STAT_GET_UNDO_LOGS_COLS];
		bool		nulls[PG_STAT_GET_UNDO_LOGS_COLS] = {false};
		Oid			tablespace;

		/*
		 * This won't be a consistent result overall, but the values for each
		 * log will be consistent because we'll take the per-log lock while
		 * copying them.
		 */
		LWLockAcquire(&slot->meta_lock, LW_SHARED);

		/* Skip unused slots. */
		if (slot->logno == InvalidUndoLogNumber)
		{
			LWLockRelease(&slot->meta_lock);
			continue;
		}

		values[0] = ObjectIdGetDatum((Oid) slot->logno);
		values[1] = CStringGetTextDatum(
										slot->meta.persistence == RELPERSISTENCE_PERMANENT ? "permanent" :
										slot->meta.persistence == RELPERSISTENCE_UNLOGGED ? "unlogged" :
										slot->meta.persistence == RELPERSISTENCE_TEMP ? "temporary" : "?");
		tablespace = slot->meta.tablespace;

		snprintf(buffer, sizeof(buffer), UndoRecPtrFormat,
				 MakeUndoRecPtr(slot->logno, slot->begin));
		values[3] = CStringGetTextDatum(buffer);
		snprintf(buffer, sizeof(buffer), UndoRecPtrFormat,
				 MakeUndoRecPtr(slot->logno, slot->meta.discard));
		values[4] = CStringGetTextDatum(buffer);
		snprintf(buffer, sizeof(buffer), UndoRecPtrFormat,
				 MakeUndoRecPtr(slot->logno, slot->meta.insert));
		values[5] = CStringGetTextDatum(buffer);
		snprintf(buffer, sizeof(buffer), UndoRecPtrFormat,
				 MakeUndoRecPtr(slot->logno, slot->end));
		values[6] = CStringGetTextDatum(buffer);
		if (slot->xid == InvalidTransactionId)
			nulls[7] = true;
		else
			values[7] = TransactionIdGetDatum(slot->xid);
		if (slot->pid == InvalidPid)
			nulls[8] = true;
		else
			values[8] = Int32GetDatum((int32) slot->pid);
		LWLockRelease(&slot->meta_lock);

		/*
		 * Deal with potentially slow tablespace name lookup without the lock.
		 * Avoid making multiple calls to that expensive function for the
		 * common case of repeating tablespace.
		 */
		if (tablespace != last_tablespace)
		{
			if (tablespace_name)
				pfree(tablespace_name);
			tablespace_name = get_tablespace_name(tablespace);
			last_tablespace = tablespace;
		}
		if (tablespace_name)
		{
			values[2] = CStringGetTextDatum(tablespace_name);
			nulls[2] = false;
		}
		else
			nulls[2] = true;

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	if (tablespace_name)
		pfree(tablespace_name);
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
 * Forcibly throw away undo data.  This is an emergency-only procedure
 * designed to deal with situations where transaction rollback fails
 * repeatedly for some reason.  The state of the system is undefined after
 * this (but it's likely to result in uncommitted effects appearing as
 * committed).
 */
Datum
pg_force_discard_undo_log(PG_FUNCTION_ARGS)
{
	UndoLogNumber logno = PG_GETARG_INT32(0);
	UndoLogSlot *slot;
	UndoLogOffset new_discard;

	if (!superuser())
		elog(ERROR, "must be superuser");

	slot = UndoLogGetSlot(logno, true);
	if (slot == NULL)
		elog(ERROR, "undo log not found");

	/*
	 * Choose a new discard pointer value that is the current insert pointer
	 * of the undo log, and make sure no transaction is in progress in that
	 * undo log.  This must be a safe place to discard to, since no data will
	 * remain.
	 */
	LWLockAcquire(&slot->meta_lock, LW_SHARED);
	if (slot->meta.logno != logno)
		elog(ERROR, "undo log not found (slot recycled)");
	if (TransactionIdIsActive(slot->xid))
		elog(ERROR, "undo log in use by an active transaction");
	new_discard = slot->meta.insert;
	LWLockRelease(&slot->meta_lock);

	UndoDiscard(MakeUndoRecPtr(logno, new_discard));

	return (Datum) 0;
}

/*
 * Request that an undo log should behave as if it is full at the next
 * insertion, for testing purposes.  This wastes undo log address space.
 */
Datum
pg_force_truncate_undo_log(PG_FUNCTION_ARGS)
{
	UndoLogNumber logno = PG_GETARG_INT32(0);
	UndoLogSlot *slot;

	if (!superuser())
		elog(ERROR, "must be superuser");

	slot = UndoLogGetSlot(logno, false);

	/*
	 * We don't actually do anything immediately, because it's too complicated
	 * to coordinate with a concurrent insertion.  So instead we'll ask
	 * UndoLogAllocate() to do it at the next appropriate time.
	 */
	LWLockAcquire(&slot->meta_lock, LW_EXCLUSIVE);
	if (slot->meta.logno != logno)
		elog(ERROR, "undo log not found (slot recycled)");
	if (slot->pid != MyProcPid)
		elog(ERROR, "cannot truncate undo log that this backend is not attached to");
	slot->force_truncate = true;
	LWLockRelease(&slot->meta_lock);

	return (Datum) 0;
}

/*
 * replay the creation of a new undo log
 */
static void
undolog_xlog_create(XLogReaderState *record)
{
	xl_undolog_create *xlrec = (xl_undolog_create *) XLogRecGetData(record);
	UndoLogSlot *slot;

	/* Create meta-data space in shared memory. */
	LWLockAcquire(UndoLogLock, LW_EXCLUSIVE);

	/*
	 * If we recover from an online checkpoint, the undo log may already have
	 * been created in shared memory.  Usually we'll have to allocate a fresh
	 * slot.
	 */
	for (int i = 0; i < UndoLogNumSlots(); ++i)
	{
		slot = &UndoLogShared->slots[i];
		if (slot->logno == xlrec->logno)
			break;
		slot = NULL;
	}
	if (!slot)
		slot = allocate_undo_log_slot();

	LWLockAcquire(&slot->meta_lock, LW_EXCLUSIVE);
	slot->logno = xlrec->logno;
	slot->pid = InvalidPid;
	slot->meta.logno = xlrec->logno;
	slot->meta.size = UndoLogMaxSize;
	slot->meta.persistence = xlrec->persistence;
	slot->meta.tablespace = xlrec->tablespace;
	slot->meta.insert = SizeOfUndoPageHeaderData;
	slot->meta.discard = SizeOfUndoPageHeaderData;
	LWLockRelease(&slot->meta_lock);
	UndoLogShared->next_logno = Max(xlrec->logno + 1, UndoLogShared->next_logno);
	LWLockRelease(UndoLogLock);
}

/*
 * Drop all buffers for the given undo log, from old_discard up to
 * new_discard.  If drop_tail is true, also drop the buffer that holds
 * new_discard; this is used when discarding undo logs completely, for example
 * via DROP TABLESPACE.  If it is false, then the final buffer is not dropped
 * because it may contain data.
 *
 */
static void
discard_undo_buffers(int logno, UndoLogOffset old_discard,
					 UndoLogOffset new_discard, bool drop_tail)
{
	BlockNumber old_blockno;
	BlockNumber new_blockno;
	RelFileNode rnode;

	UndoRecPtrAssignRelFileNode(rnode, MakeUndoRecPtr(logno, old_discard));
	old_blockno = old_discard / BLCKSZ;
	new_blockno = new_discard / BLCKSZ;
	if (drop_tail)
		++new_blockno;
	if (UndoLogNumberGetPersistence(logno) == RELPERSISTENCE_TEMP)
	{
		while (old_blockno < new_blockno)
			DiscardLocalBuffer(rnode, UndoLogForkNum, old_blockno++);
	}
	else
	{
		while (old_blockno < new_blockno)
			DiscardBuffer(rnode, UndoLogForkNum, old_blockno++);
	}
}

/*
 * replay an undo segment discard record
 */
static void
undolog_xlog_discard(XLogReaderState *record)
{
	xl_undolog_discard *xlrec = (xl_undolog_discard *) XLogRecGetData(record);

	UndoDiscard(MakeUndoRecPtr(xlrec->logno, xlrec->discard));
}

/*
 * replay the truncation of an undo log.
 */
static void
undolog_xlog_truncate(XLogReaderState *record)
{
	xl_undolog_truncate *xlrec = (xl_undolog_truncate *) XLogRecGetData(record);
	UndoLogSlot *slot;

	slot = UndoLogGetSlot(xlrec->logno, false);

	slot->meta.size = xlrec->size;
}

static void
undolog_xlog_urs_close(XLogReaderState *record)
{
	/* This record is processed specially, UndoReplay() knows what to do. */
	UndoReplay(record, NULL, 0);
}

void
undolog_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_UNDOLOG_CREATE:
			undolog_xlog_create(record);
			break;
		case XLOG_UNDOLOG_DISCARD:
			undolog_xlog_discard(record);
			break;
		case XLOG_UNDOLOG_TRUNCATE:
			undolog_xlog_truncate(record);
			break;
		case XLOG_UNDOLOG_CLOSE_URS:
			undolog_xlog_urs_close(record);
			break;
		default:
			elog(PANIC, "undo_redo: unknown op code %u", info);
	}
}

/*
 * Get the undo persistence level that corresponds to a relation persistence
 * level.
 *
 * TODO Remove, UndoPersistenceForRelPersistence() exists already.
 */
UndoPersistenceLevel
GetUndoPersistenceLevel(char relpersistence)
{
	if (relpersistence == RELPERSISTENCE_TEMP)
		return UNDOPERSISTENCE_TEMP;
	if (relpersistence == RELPERSISTENCE_UNLOGGED)
		return UNDOPERSISTENCE_UNLOGGED;
	if (relpersistence == RELPERSISTENCE_PERMANENT)
		return UNDOPERSISTENCE_PERMANENT;
	ereport(ERROR, (errmsg("unrecognized persistence '%c'",
						   relpersistence)));
}
