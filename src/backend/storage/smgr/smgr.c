/*-------------------------------------------------------------------------
 *
 * smgr.c
 *	  public interface routines to storage manager switch.
 *
 *	  All file system operations in POSTGRES dispatch through these
 *	  routines.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/smgr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlogutils.h"
#include "lib/ilist.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/md.h"
#include "storage/smgr.h"
#include "utils/hsearch.h"
#include "utils/inval.h"


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
	void		(*smgr_open) (SMgrFileHandle sfile);
	void		(*smgr_close) (SMgrFileHandle sfile);
	void		(*smgr_create) (SMgrFileHandle sfile, bool isRedo);
	bool		(*smgr_exists) (SMgrFileHandle sfile);
	void		(*smgr_unlink) (SMgrFileLocator slocator, bool isRedo);
	void		(*smgr_extend) (SMgrFileHandle sfile,
								BlockNumber blocknum, char *buffer, bool skipFsync);
	bool		(*smgr_prefetch) (SMgrFileHandle sfile,
								  BlockNumber blocknum);
	void		(*smgr_read) (SMgrFileHandle sfile,
							  BlockNumber blocknum, char *buffer);
	void		(*smgr_write) (SMgrFileHandle sfile,
							   BlockNumber blocknum, char *buffer, bool skipFsync);
	void		(*smgr_writeback) (SMgrFileHandle sfile,
								   BlockNumber blocknum, BlockNumber nblocks);
	BlockNumber (*smgr_nblocks) (SMgrFileHandle sfile);
	void		(*smgr_truncate) (SMgrFileHandle sfile, BlockNumber nblocks);
	void		(*smgr_immedsync) (SMgrFileHandle sfile);
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
 * Each backend has a hashtable that stores all extant SMgrFileData objects.
 * In addition, "unowned" SMgrFile objects are chained together in a list.
 */
static HTAB *SMgrFileHash = NULL;

static dlist_head unowned_sfiles;

/* local function prototypes */
static void smgrshutdown(int code, Datum arg);


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
SMgrFileHandle
smgropen(RelFileLocator rlocator, BackendId backend, ForkNumber forkNum)
{
	SMgrFileLocator slocator;
	SMgrFileHandle sfile;
	bool		found;

	if (SMgrFileHash == NULL)
	{
		/* First time through: initialize the hash table */
		HASHCTL		ctl;

		ctl.keysize = sizeof(SMgrFileLocator);
		ctl.entrysize = sizeof(SMgrFileData);
		SMgrFileHash = hash_create("smgr relation table", 400,
									   &ctl, HASH_ELEM | HASH_BLOBS);
		dlist_init(&unowned_sfiles);
	}

	/* Look up or create an entry */
	slocator.locator = rlocator;
	slocator.backend = backend;
	slocator.forknum = forkNum;
	sfile = (SMgrFileHandle) hash_search(SMgrFileHash,
										 (void *) &slocator,
										 HASH_ENTER, &found);

	/* Initialize it if not present before */
	if (!found)
	{
		/* hash_search already filled in the lookup key */
		sfile->smgr_owner = NULL;
		sfile->smgr_targblock = InvalidBlockNumber;
		sfile->smgr_cached_nblocks = InvalidBlockNumber;
		sfile->smgr_which = 0;	/* we only have md.c at present */

		/* implementation-specific initialization */
		smgrsw[sfile->smgr_which].smgr_open(sfile);

		/* it has no owner yet */
		dlist_push_tail(&unowned_sfiles, &sfile->node);
	}

	return sfile;
}

/*
 * smgrsetowner() -- Establish a long-lived reference to an SMgrRelation object
 *
 * There can be only one owner at a time; this is sufficient since currently
 * the only such owners exist in the relcache.
 */
void
smgrsetowner(SMgrFileHandle *owner, SMgrFileHandle sfile)
{
	/* We don't support "disowning" an SMgrRelation here, use smgrclearowner */
	Assert(owner != NULL);

	/*
	 * First, unhook any old owner.  (Normally there shouldn't be any, but it
	 * seems possible that this can happen during swap_relation_files()
	 * depending on the order of processing.  It's ok to close the old
	 * relcache entry early in that case.)
	 *
	 * If there isn't an old owner, then the sfile should be in the unowned
	 * list, and we need to remove it.
	 */
	if (sfile->smgr_owner)
		*(sfile->smgr_owner) = NULL;
	else
		dlist_delete(&sfile->node);

	/* Now establish the ownership relationship. */
	sfile->smgr_owner = owner;
	*owner = sfile;
}

/*
 * smgrclearowner() -- Remove long-lived reference to an SMgrFileHandle object
 *					   if one exists
 */
void
smgrclearowner(SMgrFileHandle *owner, SMgrFileHandle sfile)
{
	/* Do nothing if the SMgrRelation object is not owned by the owner */
	if (sfile->smgr_owner != owner)
		return;

	/* unset the owner's reference */
	*owner = NULL;

	/* unset our reference to the owner */
	sfile->smgr_owner = NULL;

	/* add to list of unowned relations */
	dlist_push_tail(&unowned_sfiles, &sfile->node);
}

/*
 *	smgrexists() -- Does the underlying file exist?
 */
bool
smgrexists(SMgrFileHandle sfile)
{
	return smgrsw[sfile->smgr_which].smgr_exists(sfile);
}

/*
 *	smgrclose() -- Close and delete an SMgrFile object.
 */
void
smgrclose(SMgrFileHandle sfile)
{
	SMgrFileHandle *owner;

	smgrsw[sfile->smgr_which].smgr_close(sfile);

	owner = sfile->smgr_owner;

	if (!owner)
		dlist_delete(&sfile->node);

	if (hash_search(SMgrFileHash,
					(void *) &(sfile->smgr_locator),
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
 *	smgrrelease() -- Release all resources used by this object.
 *
 *	The object remains valid.
 */
static void
smgrrelease(SMgrFileHandle sfile)
{
	smgrsw[sfile->smgr_which].smgr_close(sfile);
	sfile->smgr_cached_nblocks = InvalidBlockNumber;
}

/*
 *	smgrreleaseall() -- Release resources used by all objects.
 *
 *	This is called for PROCSIGNAL_BARRIER_SMGRRELEASE.
 */
void
smgrreleaseall(void)
{
	HASH_SEQ_STATUS status;
	SMgrFileHandle sfile;

	/* Nothing to do if hashtable not set up */
	if (SMgrFileHash == NULL)
		return;

	hash_seq_init(&status, SMgrFileHash);

	while ((sfile = (SMgrFileHandle) hash_seq_search(&status)) != NULL)
		smgrrelease(sfile);
}

/*
 *	smgrcloseall() -- Close all existing SMgrRelation objects.
 */
void
smgrcloseall(void)
{
	HASH_SEQ_STATUS status;
	SMgrFileHandle sfile;

	/* Nothing to do if hashtable not set up */
	if (SMgrFileHash == NULL)
		return;

	hash_seq_init(&status, SMgrFileHash);

	while ((sfile = (SMgrFileHandle) hash_seq_search(&status)) != NULL)
		smgrclose(sfile);
}

/*
 *	smgrcloserellocator() -- Close SMgrRelation object for given RelFileLocator,
 *					   if one exists.
 *
 * This has the same effects as smgrclose(smgropen(rlocator)), but it avoids
 * uselessly creating a hashtable entry only to drop it again when no
 * such entry exists already.
 */
void
smgrcloserellocator(RelFileLocator rlocator, BackendId backend)
{
	SMgrFileHandle sfile;

	/* Nothing to do if hashtable not set up */
	if (SMgrFileHash == NULL)
		return;

	for (int i = 0; i <= MAX_FORKNUM; i++)
	{
		SMgrFileLocator slocator = { rlocator, backend, i };

		sfile = (SMgrFileHandle) hash_search(SMgrFileHash,
											 (void *) &slocator,
											 HASH_FIND, NULL);
		if (sfile != NULL)
			smgrclose(sfile);
	}
}

/*
 *	smgrcreate() -- Create a new file.
 *
 *		Given an already-created (but presumably unused) SMgrFileHandle,
 *		cause the underlying disk file or other storage for the fork
 *		to be created.
 */
void
smgrcreate(SMgrFileHandle sfile, bool isRedo)
{
	smgrsw[sfile->smgr_which].smgr_create(sfile, isRedo);
}

/*
 *	smgrunlink_multi() -- Immediately unlink given forks of given relation
 *
 *		The given forks of the relation are removed from the store.  This
 *		should not be used during transactional operations, since it can't be
 *		undone.
 *
 *		This handles multiple forks in one call, because the cache invalidation
 *		happens at relation granularity. If we had an smgrunlink() function
 *		to unlink just a single smgr file, and the caller wanted to delete
 *		multiple forks of a single relation, each call would send a new
 *		cache invalidation event, which would be wasteful.
 *
 *		If isRedo is true, it is okay for the underlying file(s) to be gone
 *		already.
 */
void
smgrunlink_multi(RelFileLocator rlocator, BackendId backend, ForkNumber *forks, int nforks, bool isRedo)
{
	int			which;
	int			i = 0;

	which = 0;	/* we only have md.c at present */

	/* Close the forks at smgr level */
	smgrcloserellocator(rlocator, backend);

	/*
	 * Send a shared-inval message to force other backends to close any
	 * dangling smgr references they may have for these rels.  We should do
	 * this before starting the actual unlinking, in case we fail partway
	 * through that step.  Note that the sinval messages will eventually come
	 * back to this backend, too, and thereby provide a backstop that we
	 * closed our own smgr rel.
	 */
	CacheInvalidateSmgr(rlocator, backend);

	/*
	 * Delete the physical file(s).
	 *
	 * Note: smgr_unlink must treat deletion failure as a WARNING, not an
	 * ERROR, because we've already decided to commit or abort the current
	 * xact.
	 */
	for (i = 0; i < nforks; i++)
	{
		SMgrFileLocator slocator = { rlocator, backend, forks[i] };
		smgrsw[which].smgr_unlink(slocator, isRedo);
	}
}

/*
 *	smgrdounlink() -- Immediately unlink a file
 *
 *		If isRedo is true, it is okay for the underlying file(s) to be gone
 *		already.
 *
 * To remove a relation transactionally, see RelationDropStorage() instead.
 * This will cause cache invalidation of all forks of the relation, not just
 * this one.
 */
void
smgrunlink(SMgrFileHandle sfile, bool isRedo)
{
	SMgrFileLocator locator;
	int			which;

	/* remember before closing */
	which = sfile->smgr_which;
	locator = sfile->smgr_locator;

	/* Close the file at smgr level */
	smgrclose(sfile);

	/*
	 * Send a shared-inval message to force other backends to close any
	 * dangling smgr references they may have for these rels.  We should do
	 * this before starting the actual unlinking, in case we fail partway
	 * through that step.  Note that the sinval messages will eventually come
	 * back to this backend, too, and thereby provide a backstop that we
	 * closed our own smgr rel.
	 */
	CacheInvalidateSmgr(locator.locator, locator.backend);

	/*
	 * Delete the physical file(s).
	 *
	 * Note: smgr_unlink must treat deletion failure as a WARNING, not an
	 * ERROR, because we've already decided to commit or abort the current
	 * xact.
	 */
	smgrsw[which].smgr_unlink(locator, isRedo);
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
smgrextend(SMgrFileHandle sfile, BlockNumber blocknum,
		   char *buffer, bool skipFsync)
{
	smgrsw[sfile->smgr_which].smgr_extend(sfile, blocknum,
										  buffer, skipFsync);

	/*
	 * Normally we expect this to increase nblocks by one, but if the cached
	 * value isn't as expected, just invalidate it so the next call asks the
	 * kernel.
	 */
	if (sfile->smgr_cached_nblocks == blocknum)
		sfile->smgr_cached_nblocks = blocknum + 1;
	else
		sfile->smgr_cached_nblocks = InvalidBlockNumber;
}

/*
 *	smgrprefetch() -- Initiate asynchronous read of the specified block of a relation.
 *
 *		In recovery only, this can return false to indicate that a file
 *		doesn't	exist (presumably it has been dropped by a later WAL
 *		record).
 */
bool
smgrprefetch(SMgrFileHandle sfile, BlockNumber blocknum)
{
	return smgrsw[sfile->smgr_which].smgr_prefetch(sfile, blocknum);
}

/*
 *	smgrread() -- read a particular block from a file into the supplied
 *				  buffer.
 *
 *		This routine is called from the buffer manager in order to
 *		instantiate pages in the shared buffer cache.  All storage managers
 *		return pages in the format that POSTGRES expects.
 */
void
smgrread(SMgrFileHandle sfile, BlockNumber blocknum, char *buffer)
{
	smgrsw[sfile->smgr_which].smgr_read(sfile, blocknum, buffer);
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
smgrwrite(SMgrFileHandle sfile, BlockNumber blocknum,
		  char *buffer, bool skipFsync)
{
	smgrsw[sfile->smgr_which].smgr_write(sfile, blocknum,
										 buffer, skipFsync);
}


/*
 *	smgrwriteback() -- Trigger kernel writeback for the supplied range of
 *					   blocks.
 */
void
smgrwriteback(SMgrFileHandle sfile, BlockNumber blocknum,
			  BlockNumber nblocks)
{
	smgrsw[sfile->smgr_which].smgr_writeback(sfile, blocknum,
											 nblocks);
}

/*
 *	smgrnblocks() -- Calculate the number of blocks in the
 *					 supplied relation.
 */
BlockNumber
smgrnblocks(SMgrFileHandle sfile)
{
	BlockNumber result;

	/* Check and return if we get the cached value for the number of blocks. */
	result = smgrnblocks_cached(sfile);
	if (result != InvalidBlockNumber)
		return result;

	result = smgrsw[sfile->smgr_which].smgr_nblocks(sfile);

	sfile->smgr_cached_nblocks = result;

	return result;
}

/*
 *	smgrnblocks_cached() -- Get the cached number of blocks in the supplied
 *							relation.
 *
 * Returns an InvalidBlockNumber when not in recovery and when the relation
 * fork size is not cached.
 */
BlockNumber
smgrnblocks_cached(SMgrFileHandle sfile)
{
	/*
	 * For now, we only use cached values in recovery due to lack of a shared
	 * invalidation mechanism for changes in file size.
	 */
	if (InRecovery && sfile->smgr_cached_nblocks != InvalidBlockNumber)
		return sfile->smgr_cached_nblocks;

	return InvalidBlockNumber;
}

/*
 *	smgrtruncate_multi() -- Truncate the given forks of supplied relation to
 *							each specified numbers of blocks
 *
 * The truncation is done immediately, so this can't be rolled back.
 *
 * The caller must hold AccessExclusiveLock on the relation, to ensure that
 * other backends receive the smgr invalidation event that this function sends
 * before they access any forks of the relation again.
 *
 * Like smgrunlink_multi(), this handles multiple forks in one call because
 * the cache invalidation happens at relation granularity.
 *
 * NB: The caller is responsible for dropping buffers! Before v16, this
 * function did it.
 */
void
smgrtruncate_multi(RelFileLocator rlocator, BackendId backend, ForkNumber *forks,
				   int nforks, BlockNumber *nblocks)
{
	int			i;

	Assert(nforks < MAX_FORKNUM + 1);

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
	CacheInvalidateSmgr(rlocator, backend);

	/* Do the truncations */
	for (i = 0; i < nforks; i++)
	{
		SMgrFileHandle sfile;

		sfile = smgropen(rlocator, backend, forks[i]);

		/* Make the cached size is invalid if we encounter an error. */
		sfile->smgr_cached_nblocks = InvalidBlockNumber;

		smgrsw[sfile->smgr_which].smgr_truncate(sfile, nblocks[i]);

		/*
		 * We might as well update the local smgr_cached_nblocks values. The
		 * smgr cache inval message that this function sent will cause other
		 * backends to invalidate their copies of smgr_fsm_nblocks and
		 * smgr_vm_nblocks, and these ones too at the next command boundary.
		 * But these ensure they aren't outright wrong until then.
		 */
		sfile->smgr_cached_nblocks = nblocks[i];
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
smgrimmedsync(SMgrFileHandle sfile)
{
	smgrsw[sfile->smgr_which].smgr_immedsync(sfile);
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
	dlist_foreach_modify(iter, &unowned_sfiles)
	{
		SMgrFileHandle sfile = dlist_container(SMgrFileData, node,
											   iter.cur);

		Assert(sfile->smgr_owner == NULL);

		smgrclose(sfile);
	}
}

/*
 * This routine is called when we are ordered to release all open files by a
 * ProcSignalBarrier.
 */
bool
ProcessBarrierSmgrRelease(void)
{
	smgrreleaseall();
	return true;
}
