/*-------------------------------------------------------------------------
 *
 * storage.c
 *	  code to create and destroy physical storage for relations
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/storage.c
 *
 * NOTES
 *	  Some of this code used to be in storage/smgr/smgr.c, and the
 *	  function names still reflect that.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/parallel.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "miscadmin.h"
#include "storage/freespace.h"
#include "storage/smgr.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/* GUC variables */
int			wal_skip_threshold = 2048;	/* in kilobytes */

/*
 * We keep a list of all relations (represented as RelFileLocator values)
 * that have been created or deleted in the current transaction.  When
 * a relation is created, we create the physical file immediately, but
 * remember it so that we can delete the file again if the current
 * transaction is aborted.  Conversely, a deletion request is NOT
 * executed immediately, but is just entered in the list.  When and if
 * the transaction commits, we can delete the physical file.
 *
 * To handle subtransactions, every entry is marked with its transaction
 * nesting level.  At subtransaction commit, we reassign the subtransaction's
 * entries to the parent nesting level.  At subtransaction abort, we can
 * immediately execute the abort-time actions for all entries of the current
 * nesting level.
 *
 * NOTE: the list is kept in TopMemoryContext to be sure it won't disappear
 * unbetimes.  It'd probably be OK to keep it in TopTransactionContext,
 * but I'm being paranoid.
 */

typedef struct PendingRelDelete
{
	RelFileLocator rlocator;	/* relation that may need to be deleted */
	BackendId	backend;		/* InvalidBackendId if not a temp rel */
	bool		atCommit;		/* T=delete at commit; F=delete at abort */
	int			nestLevel;		/* xact nesting level of request */
	struct PendingRelDelete *next;	/* linked-list link */
} PendingRelDelete;

typedef struct PendingRelSync
{
	RelFileLocator rlocator;
	bool		is_truncated;	/* Has the file experienced truncation? */
} PendingRelSync;

static PendingRelDelete *pendingDeletes = NULL; /* head of linked list */
static HTAB *pendingSyncHash = NULL;


/*
 * AddPendingSync
 *		Queue an at-commit fsync.
 */
static void
AddPendingSync(const RelFileLocator *rlocator)
{
	PendingRelSync *pending;
	bool		found;

	/* create the hash if not yet */
	if (!pendingSyncHash)
	{
		HASHCTL		ctl;

		ctl.keysize = sizeof(RelFileLocator);
		ctl.entrysize = sizeof(PendingRelSync);
		ctl.hcxt = TopTransactionContext;
		pendingSyncHash = hash_create("pending sync hash", 16, &ctl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	pending = hash_search(pendingSyncHash, rlocator, HASH_ENTER, &found);
	Assert(!found);
	pending->is_truncated = false;
}

/*
 * RelationCreateStorage
 *		Create physical storage for a relation.
 *
 * Create the underlying disk file storage for the relation. This only
 * creates the main fork; additional forks are created lazily by the
 * modules that need them.
 *
 * This function is transactional. The creation is WAL-logged, and if the
 * transaction aborts later on, the storage will be destroyed.  A caller
 * that does not want the storage to be destroyed in case of an abort may
 * pass register_delete = false.
 */
SMgrFileHandle
RelationCreateStorage(RelFileLocator rlocator, char relpersistence,
					  bool register_delete)
{
	SMgrFileHandle sfile;
	BackendId	backend;
	bool		needs_wal;

	Assert(!IsInParallelMode());	/* couldn't update pendingSyncHash */

	switch (relpersistence)
	{
		case RELPERSISTENCE_TEMP:
			backend = BackendIdForTempRelations();
			needs_wal = false;
			break;
		case RELPERSISTENCE_UNLOGGED:
			backend = InvalidBackendId;
			needs_wal = false;
			break;
		case RELPERSISTENCE_PERMANENT:
			backend = InvalidBackendId;
			needs_wal = true;
			break;
		default:
			elog(ERROR, "invalid relpersistence: %c", relpersistence);
			return NULL;		/* placate compiler */
	}

	sfile = smgropen(rlocator, backend, MAIN_FORKNUM);
	smgrcreate(sfile, false);

	if (needs_wal)
		log_smgrcreate(&rlocator, MAIN_FORKNUM);

	/*
	 * Add the relation to the list of stuff to delete at abort, if we are
	 * asked to do so.
	 */
	if (register_delete)
	{
		PendingRelDelete *pending;

		pending = (PendingRelDelete *)
			MemoryContextAlloc(TopMemoryContext, sizeof(PendingRelDelete));
		pending->rlocator = rlocator;
		pending->backend = backend;
		pending->atCommit = false;	/* delete if abort */
		pending->nestLevel = GetCurrentTransactionNestLevel();
		pending->next = pendingDeletes;
		pendingDeletes = pending;
	}

	if (relpersistence == RELPERSISTENCE_PERMANENT && !XLogIsNeeded())
	{
		Assert(backend == InvalidBackendId);
		AddPendingSync(&rlocator);
	}

	return sfile;
}

/*
 * Perform XLogInsert of an XLOG_SMGR_CREATE record to WAL.
 */
void
log_smgrcreate(const RelFileLocator *rlocator, ForkNumber forkNum)
{
	xl_smgr_create xlrec;

	/*
	 * Make an XLOG entry reporting the file creation.
	 */
	xlrec.rlocator = *rlocator;
	xlrec.forkNum = forkNum;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xlrec));
	XLogInsert(RM_SMGR_ID, XLOG_SMGR_CREATE | XLR_SPECIAL_REL_UPDATE);
}

/*
 * RelationDropStorage
 *		Schedule unlinking of physical storage at transaction commit.
 */
void
RelationDropStorage(Relation rel)
{
	PendingRelDelete *pending;

	/* Add the relation to the list of stuff to delete at commit */
	pending = (PendingRelDelete *)
		MemoryContextAlloc(TopMemoryContext, sizeof(PendingRelDelete));
	pending->rlocator = rel->rd_locator;
	pending->backend = rel->rd_backend;
	pending->atCommit = true;	/* delete if commit */
	pending->nestLevel = GetCurrentTransactionNestLevel();
	pending->next = pendingDeletes;
	pendingDeletes = pending;

	/*
	 * NOTE: if the relation was created in this transaction, it will now be
	 * present in the pending-delete list twice, once with atCommit true and
	 * once with atCommit false.  Hence, it will be physically deleted at end
	 * of xact in either case (and the other entry will be ignored by
	 * smgrDoPendingDeletes, so no error will occur).  We could instead remove
	 * the existing list entry and delete the physical file immediately, but
	 * for now I'll keep the logic simple.
	 */

	RelationCloseSmgr(rel);
}

/*
 * RelationPreserveStorage
 *		Mark a relation as not to be deleted after all.
 *
 * We need this function because relation mapping changes are committed
 * separately from commit of the whole transaction, so it's still possible
 * for the transaction to abort after the mapping update is done.
 * When a new physical relation is installed in the map, it would be
 * scheduled for delete-on-abort, so we'd delete it, and be in trouble.
 * The relation mapper fixes this by telling us to not delete such relations
 * after all as part of its commit.
 *
 * We also use this to reuse an old build of an index during ALTER TABLE, this
 * time removing the delete-at-commit entry.
 *
 * No-op if the relation is not among those scheduled for deletion.
 */
void
RelationPreserveStorage(RelFileLocator rlocator, bool atCommit)
{
	PendingRelDelete *pending;
	PendingRelDelete *prev;
	PendingRelDelete *next;

	prev = NULL;
	for (pending = pendingDeletes; pending != NULL; pending = next)
	{
		next = pending->next;
		if (RelFileLocatorEquals(rlocator, pending->rlocator)
			&& pending->atCommit == atCommit)
		{
			/* unlink and delete list entry */
			if (prev)
				prev->next = next;
			else
				pendingDeletes = next;
			pfree(pending);
			/* prev does not change */
		}
		else
		{
			/* unrelated entry, don't touch it */
			prev = pending;
		}
	}
}

/*
 * RelationTruncate
 *		Physically truncate a relation to the specified number of blocks.
 *
 * This includes getting rid of any buffers for the blocks that are to be
 * dropped.
 */
void
RelationTruncate(Relation rel, BlockNumber nblocks)
{
	bool		fsm;
	bool		vm;
	bool		need_fsm_vacuum = false;
	ForkNumber	forks[MAX_FORKNUM];
	BlockNumber blocks[MAX_FORKNUM];
	int			nforks = 0;

	/*
	 * Make sure smgr_targblock etc aren't pointing somewhere past new end.
	 * (Note: don't rely on this reln pointer below this loop.)
	 */
	for (int i = 0; i <= MAX_FORKNUM; i++)
	{
		SMgrFileHandle sfile = RelationGetSmgr(rel, i);

		sfile->smgr_targblock = InvalidBlockNumber;
		sfile->smgr_cached_nblocks = InvalidBlockNumber;
	}

	/* Prepare for truncation of MAIN fork of the relation */
	forks[nforks] = MAIN_FORKNUM;
	blocks[nforks] = nblocks;
	nforks++;

	/* Prepare for truncation of the FSM if it exists */
	fsm = smgrexists(RelationGetSmgr(rel, FSM_FORKNUM));
	if (fsm)
	{
		blocks[nforks] = FreeSpaceMapPrepareTruncateRel(rel, nblocks);
		if (BlockNumberIsValid(blocks[nforks]))
		{
			forks[nforks] = FSM_FORKNUM;
			nforks++;
			need_fsm_vacuum = true;
		}
	}

	/* Prepare for truncation of the visibility map too if it exists */
	vm = smgrexists(RelationGetSmgr(rel, VISIBILITYMAP_FORKNUM));
	if (vm)
	{
		blocks[nforks] = visibilitymap_prepare_truncate(rel, nblocks);
		if (BlockNumberIsValid(blocks[nforks]))
		{
			forks[nforks] = VISIBILITYMAP_FORKNUM;
			nforks++;
		}
	}

	RelationPreTruncate(rel);

	/*
	 * Make sure that a concurrent checkpoint can't complete while truncation
	 * is in progress.
	 *
	 * The truncation operation might drop buffers that the checkpoint
	 * otherwise would have flushed. If it does, then it's essential that the
	 * files actually get truncated on disk before the checkpoint record is
	 * written. Otherwise, if reply begins from that checkpoint, the
	 * to-be-truncated blocks might still exist on disk but have older
	 * contents than expected, which can cause replay to fail. It's OK for the
	 * blocks to not exist on disk at all, but not for them to have the wrong
	 * contents.
	 */
	Assert((MyProc->delayChkptFlags & DELAY_CHKPT_COMPLETE) == 0);
	MyProc->delayChkptFlags |= DELAY_CHKPT_COMPLETE;

	/*
	 * We WAL-log the truncation before actually truncating, which means
	 * trouble if the truncation fails. If we then crash, the WAL replay
	 * likely isn't going to succeed in the truncation either, and cause a
	 * PANIC. It's tempting to put a critical section here, but that cure
	 * would be worse than the disease. It would turn a usually harmless
	 * failure to truncate, that might spell trouble at WAL replay, into a
	 * certain PANIC.
	 */
	if (RelationNeedsWAL(rel))
	{
		/*
		 * Make an XLOG entry reporting the file truncation.
		 */
		XLogRecPtr	lsn;
		xl_smgr_truncate xlrec;

		xlrec.blkno = nblocks;
		xlrec.rlocator = rel->rd_locator;
		xlrec.flags = SMGR_TRUNCATE_ALL;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, sizeof(xlrec));

		lsn = XLogInsert(RM_SMGR_ID,
						 XLOG_SMGR_TRUNCATE | XLR_SPECIAL_REL_UPDATE);

		/*
		 * Flush, because otherwise the truncation of the main relation might
		 * hit the disk before the WAL record, and the truncation of the FSM
		 * or visibility map. If we crashed during that window, we'd be left
		 * with a truncated heap, but the FSM or visibility map would still
		 * contain entries for the non-existent heap pages.
		 */
		if (fsm || vm)
			XLogFlush(lsn);
	}

	/*
	 * First remove any buffers from the buffer pool that should no
	 * longer exist after truncation is complete, and then truncate the
	 * corresponding files on disk.
	 */
	DropRelationBuffers(rel->rd_locator, rel->rd_backend, forks, nforks, blocks);
	smgrtruncate_multi(rel->rd_locator, rel->rd_backend, forks, nforks, blocks);

	/* We've done all the critical work, so checkpoints are OK now. */
	MyProc->delayChkptFlags &= ~DELAY_CHKPT_COMPLETE;

	/*
	 * Update upper-level FSM pages to account for the truncation. This is
	 * important because the just-truncated pages were likely marked as
	 * all-free, and would be preferentially selected.
	 *
	 * NB: There's no point in delaying checkpoints until this is done.
	 * Because the FSM is not WAL-logged, we have to be prepared for the
	 * possibility of corruption after a crash anyway.
	 */
	if (need_fsm_vacuum)
		FreeSpaceMapVacuumRange(rel, nblocks, InvalidBlockNumber);
}

/*
 * RelationPreTruncate
 *		Perform AM-independent work before a physical truncation.
 *
 * If an access method's relation_nontransactional_truncate does not call
 * RelationTruncate(), it must call this before decreasing the table size.
 */
void
RelationPreTruncate(Relation rel)
{
	PendingRelSync *pending;

	if (!pendingSyncHash)
		return;

	pending = hash_search(pendingSyncHash,
						  &rel->rd_locator,
						  HASH_FIND, NULL);
	if (pending)
		pending->is_truncated = true;
}

/*
 * Copy a fork's data, block by block.
 *
 * Note that this requires that there is no dirty data in shared buffers. If
 * it's possible that there are, callers need to flush those using
 * e.g. FlushRelationBuffers(rel).
 *
 * Also note that this is frequently called via locutions such as
 *		RelationCopyStorage(RelationGetSmgr(rel), ...);
 * That's safe only because we perform only smgr and WAL operations here.
 * If we invoked anything else, a relcache flush could cause our SMgrFileHandle
 * argument to become a dangling pointer.
 */
void
RelationCopyStorage(SMgrFileHandle src, SMgrFileHandle dst,
					char relpersistence)
{
	PGAlignedBlock buf;
	Page		page;
	bool		use_wal;
	bool		copying_initfork;
	BlockNumber nblocks;
	BlockNumber blkno;

	page = (Page) buf.data;

	/*
	 * The init fork for an unlogged relation in many respects has to be
	 * treated the same as normal relation, changes need to be WAL logged and
	 * it needs to be synced to disk.
	 */
	copying_initfork = relpersistence == RELPERSISTENCE_UNLOGGED &&
		src->smgr_locator.forknum == INIT_FORKNUM;

	/*
	 * We need to log the copied data in WAL iff WAL archiving/streaming is
	 * enabled AND it's a permanent relation.  This gives the same answer as
	 * "RelationNeedsWAL(rel) || copying_initfork", because we know the
	 * current operation created new relation storage.
	 */
	use_wal = XLogIsNeeded() &&
		(relpersistence == RELPERSISTENCE_PERMANENT || copying_initfork);

	nblocks = smgrnblocks(src);

	for (blkno = 0; blkno < nblocks; blkno++)
	{
		/* If we got a cancel signal during the copy of the data, quit */
		CHECK_FOR_INTERRUPTS();

		smgrread(src, blkno, buf.data);

		if (!PageIsVerifiedExtended(page, blkno,
									PIV_LOG_WARNING | PIV_REPORT_STAT))
		{
			/*
			 * For paranoia's sake, capture the file path before invoking the
			 * ereport machinery.  This guards against the possibility of a
			 * relcache flush caused by, e.g., an errcontext callback.
			 * (errcontext callbacks shouldn't be risking any such thing, but
			 * people have been known to forget that rule.)
			 */
			char	   *relpath = smgrfilepath(src->smgr_locator);

			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid page in block %u of relation %s",
							blkno, relpath)));
		}

		/*
		 * WAL-log the copied page. Unfortunately we don't know what kind of a
		 * page this is, so we have to log the full page including any unused
		 * space.
		 */
		if (use_wal)
			log_newpage(&dst->smgr_locator.locator, dst->smgr_locator.forknum, blkno, page, false);

		PageSetChecksumInplace(page, blkno);

		/*
		 * Now write the page.  We say skipFsync = true because there's no
		 * need for smgr to schedule an fsync for this write; we'll do it
		 * ourselves below.
		 */
		smgrextend(dst, blkno, buf.data, true);
	}

	/*
	 * When we WAL-logged rel pages, we must nonetheless fsync them.  The
	 * reason is that since we're copying outside shared buffers, a CHECKPOINT
	 * occurring during the copy has no way to flush the previously written
	 * data to disk (indeed it won't know the new rel even exists).  A crash
	 * later on would replay WAL from the checkpoint, therefore it wouldn't
	 * replay our earlier WAL entries. If we do not fsync those pages here,
	 * they might still not be on disk when the crash occurs.
	 */
	if (use_wal || copying_initfork)
		smgrimmedsync(dst);
}

/*
 * RelFileLocatorSkippingWAL
 *		Check if a BM_PERMANENT relfilelocator is using WAL.
 *
 * Changes to certain relations must not write WAL; see "Skipping WAL for
 * New RelFileLocator" in src/backend/access/transam/README.  Though it is
 * known from Relation efficiently, this function is intended for the code
 * paths not having access to Relation.
 */
bool
RelFileLocatorSkippingWAL(RelFileLocator rlocator)
{
	if (!pendingSyncHash ||
		hash_search(pendingSyncHash, &rlocator, HASH_FIND, NULL) == NULL)
		return false;

	return true;
}

/*
 * EstimatePendingSyncsSpace
 *		Estimate space needed to pass syncs to parallel workers.
 */
Size
EstimatePendingSyncsSpace(void)
{
	long		entries;

	entries = pendingSyncHash ? hash_get_num_entries(pendingSyncHash) : 0;
	return mul_size(1 + entries, sizeof(RelFileLocator));
}

/*
 * SerializePendingSyncs
 *		Serialize syncs for parallel workers.
 */
void
SerializePendingSyncs(Size maxSize, char *startAddress)
{
	HTAB	   *tmphash;
	HASHCTL		ctl;
	HASH_SEQ_STATUS scan;
	PendingRelSync *sync;
	PendingRelDelete *delete;
	RelFileLocator *src;
	RelFileLocator *dest = (RelFileLocator *) startAddress;

	if (!pendingSyncHash)
		goto terminate;

	/* Create temporary hash to collect active relfilelocators */
	ctl.keysize = sizeof(RelFileLocator);
	ctl.entrysize = sizeof(RelFileLocator);
	ctl.hcxt = CurrentMemoryContext;
	tmphash = hash_create("tmp relfilelocators",
						  hash_get_num_entries(pendingSyncHash), &ctl,
						  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* collect all rlocator from pending syncs */
	hash_seq_init(&scan, pendingSyncHash);
	while ((sync = (PendingRelSync *) hash_seq_search(&scan)))
		(void) hash_search(tmphash, &sync->rlocator, HASH_ENTER, NULL);

	/* remove deleted rnodes */
	for (delete = pendingDeletes; delete != NULL; delete = delete->next)
		if (delete->atCommit)
			(void) hash_search(tmphash, (void *) &delete->rlocator,
							   HASH_REMOVE, NULL);

	hash_seq_init(&scan, tmphash);
	while ((src = (RelFileLocator *) hash_seq_search(&scan)))
		*dest++ = *src;

	hash_destroy(tmphash);

terminate:
	MemSet(dest, 0, sizeof(RelFileLocator));
}

/*
 * RestorePendingSyncs
 *		Restore syncs within a parallel worker.
 *
 * RelationNeedsWAL() and RelFileLocatorSkippingWAL() must offer the correct
 * answer to parallel workers.  Only smgrDoPendingSyncs() reads the
 * is_truncated field, at end of transaction.  Hence, don't restore it.
 */
void
RestorePendingSyncs(char *startAddress)
{
	RelFileLocator *rlocator;

	Assert(pendingSyncHash == NULL);
	for (rlocator = (RelFileLocator *) startAddress; rlocator->relNumber != 0;
		 rlocator++)
		AddPendingSync(rlocator);
}

/*
 *	smgrDoPendingDeletes() -- Take care of relation deletes at end of xact.
 *
 * This also runs when aborting a subxact; we want to clean up a failed
 * subxact immediately.
 *
 * Note: It's possible that we're being asked to remove a relation that has
 * no physical storage in any fork. In particular, it's possible that we're
 * cleaning up an old temporary relation for which RemovePgTempFiles has
 * already recovered the physical storage.
 */
void
smgrDoPendingDeletes(bool isCommit)
{
	int			nestLevel = GetCurrentTransactionNestLevel();
	PendingRelDelete *pending;
	PendingRelDelete *prev;
	PendingRelDelete *next;
	int			nlocators = 0,
				maxlocators = 0;
	RelFileLocatorBackend *locators = NULL;

	prev = NULL;
	for (pending = pendingDeletes; pending != NULL; pending = next)
	{
		next = pending->next;
		if (pending->nestLevel < nestLevel)
		{
			/* outer-level entries should not be processed yet */
			prev = pending;
		}
		else
		{
			/* unlink list entry first, so we don't retry on failure */
			if (prev)
				prev->next = next;
			else
				pendingDeletes = next;
			/* do deletion if called for */
			if (pending->atCommit == isCommit)
			{
				RelFileLocatorBackend rlocator = { pending->rlocator, pending->backend };

				/* allocate the initial array, or extend it, if needed */
				if (maxlocators == 0)
				{
					maxlocators = 8;
					locators = palloc(sizeof(RelFileLocatorBackend) * maxlocators);
				}
				else if (maxlocators <= nlocators)
				{
					maxlocators *= 2;
					locators = repalloc(locators, sizeof(RelFileLocatorBackend) * maxlocators);
				}

				locators[nlocators++] = rlocator;
			}
			/* must explicitly free the list entry */
			pfree(pending);
			/* prev does not change */
		}
	}

	if (nlocators > 0)
	{
		ForkNumber forks[MAX_FORKNUM + 1];

		for (int i = 0; i <= MAX_FORKNUM; i++)
			forks[i] = i;

		/*
		 * Get rid of any remaining buffers for the relations.  bufmgr will just
		 * drop them without bothering to write the contents.
		 */
		DropRelationsAllBuffers(locators, nlocators);

		for (int i = 0; i < nlocators; i++)
			smgrunlink_multi(locators[i].locator, locators[i].backend, forks, MAX_FORKNUM + 1, false);
		pfree(locators);
	}
}

/*
 * DropRelationFiles -- drop files of all given relations
 */
void
DropRelationFiles(RelFileLocator *delrels, int ndelrels, bool isRedo)
{
	RelFileLocatorBackend *locators;
	int			i;
	ForkNumber	all_forks[MAX_FORKNUM + 1];

	locators = palloc(sizeof(RelFileLocatorBackend) * ndelrels);
	for (i = 0; i < ndelrels; i++)
	{
		if (isRedo)
		{
			for (int fork = 0; fork <= MAX_FORKNUM; fork++)
				XLogDropRelation(delrels[i], fork);
		}
		locators[i].locator = delrels[i];
		locators[i].backend = InvalidBackendId;
	}

	/*
	 * Get rid of any remaining buffers for the relations.  bufmgr will just
	 * drop them without bothering to write the contents.
	 */
	DropRelationsAllBuffers(locators, ndelrels);

	for (int fork = 0; fork <= MAX_FORKNUM; fork++)
		all_forks[fork] = fork;

	for (i = 0; i < ndelrels; i++)
		smgrunlink_multi(locators[i].locator, locators[i].backend, all_forks, MAX_FORKNUM + 1, true);
}

/*
 *	smgrDoPendingSyncs() -- Take care of relation syncs at end of xact.
 */
void
smgrDoPendingSyncs(bool isCommit, bool isParallelWorker)
{
	PendingRelDelete *pending;
	int			nlocators = 0,
				maxlocators = 0;
	RelFileLocator *locators = NULL;
	HASH_SEQ_STATUS scan;
	PendingRelSync *pendingsync;

	Assert(GetCurrentTransactionNestLevel() == 1);

	if (!pendingSyncHash)
		return;					/* no relation needs sync */

	/* Abort -- just throw away all pending syncs */
	if (!isCommit)
	{
		pendingSyncHash = NULL;
		return;
	}

	AssertPendingSyncs_RelationCache();

	/* Parallel worker -- just throw away all pending syncs */
	if (isParallelWorker)
	{
		pendingSyncHash = NULL;
		return;
	}

	/* Skip syncing nodes that smgrDoPendingDeletes() will delete. */
	for (pending = pendingDeletes; pending != NULL; pending = pending->next)
		if (pending->atCommit)
			(void) hash_search(pendingSyncHash, (void *) &pending->rlocator,
							   HASH_REMOVE, NULL);

	hash_seq_init(&scan, pendingSyncHash);
	while ((pendingsync = (PendingRelSync *) hash_seq_search(&scan)))
	{
		ForkNumber	fork;
		BlockNumber nblocks[MAX_FORKNUM + 1];
		BlockNumber total_blocks = 0;

		/*
		 * We emit newpage WAL records for smaller relations.
		 *
		 * Small WAL records have a chance to be emitted along with other
		 * backends' WAL records.  We emit WAL records instead of syncing for
		 * files that are smaller than a certain threshold, expecting faster
		 * commit.  The threshold is defined by the GUC wal_skip_threshold.
		 */
		if (!pendingsync->is_truncated)
		{
			for (fork = 0; fork <= MAX_FORKNUM; fork++)
			{
				SMgrFileHandle sfile;

				sfile = smgropen(pendingsync->rlocator, InvalidBackendId, fork);
				if (smgrexists(sfile))
				{
					BlockNumber n = smgrnblocks(sfile);

					/* we shouldn't come here for unlogged relations */
					Assert(fork != INIT_FORKNUM);
					nblocks[fork] = n;
					total_blocks += n;
				}
				else
					nblocks[fork] = InvalidBlockNumber;
			}
		}

		/*
		 * Sync file or emit WAL records for its contents.
		 *
		 * Although we emit WAL record if the file is small enough, do file
		 * sync regardless of the size if the file has experienced a
		 * truncation. It is because the file would be followed by trailing
		 * garbage blocks after a crash recovery if, while a past longer file
		 * had been flushed out, we omitted syncing-out of the file and
		 * emitted WAL instead.  You might think that we could choose WAL if
		 * the current main fork is longer than ever, but there's a case where
		 * main fork is longer than ever but FSM fork gets shorter.
		 */
		if (pendingsync->is_truncated ||
			total_blocks * BLCKSZ / 1024 >= wal_skip_threshold)
		{
			/* allocate the initial array, or extend it, if needed */
			if (maxlocators == 0)
			{
				maxlocators = 8;
				locators = palloc(sizeof(RelFileLocatorBackend) * maxlocators);
			}
			else if (maxlocators <= nlocators)
			{
				maxlocators *= 2;
				locators = repalloc(locators, sizeof(RelFileLocatorBackend) * maxlocators);
			}

			locators[nlocators] = pendingsync->rlocator;
			nlocators++;
		}
		else
		{
			/* Emit WAL records for all blocks.  The file is small enough. */
			for (fork = 0; fork <= MAX_FORKNUM; fork++)
			{
				int			n = nblocks[fork];
				Relation	rel;

				if (!BlockNumberIsValid(n))
					continue;

				/*
				 * Emit WAL for the whole file.  Unfortunately we don't know
				 * what kind of a page this is, so we have to log the full
				 * page including any unused space.  ReadBufferExtended()
				 * counts some pgstat events; unfortunately, we discard them.
				 */
				rel = CreateFakeRelcacheEntry(pendingsync->rlocator);
				log_newpage_range(rel, fork, 0, n, false);
				FreeFakeRelcacheEntry(rel);
			}
		}
	}

	pendingSyncHash = NULL;

	FlushRelationsAllBuffers(locators, nlocators);

	for (int i = 0; i < nlocators; i++)
	{
		for (int fork = 0; fork <= MAX_FORKNUM; fork++)
		{
			SMgrFileHandle sfile = smgropen(locators[i], InvalidBackendId, fork);

			if (smgrexists(sfile))
				smgrimmedsync(sfile);
		}
	}
	if (locators != NULL)
		pfree(locators);
}

/*
 * smgrGetPendingDeletes() -- Get a list of non-temp relations to be deleted.
 *
 * The return value is the number of relations scheduled for termination.
 * *ptr is set to point to a freshly-palloc'd array of RelFileLocators.
 * If there are no relations to be deleted, *ptr is set to NULL.
 *
 * Only non-temporary relations are included in the returned list.  This is OK
 * because the list is used only in contexts where temporary relations don't
 * matter: we're either writing to the two-phase state file (and transactions
 * that have touched temp tables can't be prepared) or we're writing to xlog
 * (and all temporary files will be zapped if we restart anyway, so no need
 * for redo to do it also).
 *
 * Note that the list does not include anything scheduled for termination
 * by upper-level transactions.
 */
int
smgrGetPendingDeletes(bool forCommit, RelFileLocator **ptr)
{
	int			nestLevel = GetCurrentTransactionNestLevel();
	int			nrels;
	RelFileLocator *rptr;
	PendingRelDelete *pending;

	nrels = 0;
	for (pending = pendingDeletes; pending != NULL; pending = pending->next)
	{
		if (pending->nestLevel >= nestLevel && pending->atCommit == forCommit
			&& pending->backend == InvalidBackendId)
			nrels++;
	}
	if (nrels == 0)
	{
		*ptr = NULL;
		return 0;
	}
	rptr = (RelFileLocator *) palloc(nrels * sizeof(RelFileLocator));
	*ptr = rptr;
	for (pending = pendingDeletes; pending != NULL; pending = pending->next)
	{
		if (pending->nestLevel >= nestLevel && pending->atCommit == forCommit
			&& pending->backend == InvalidBackendId)
		{
			*rptr = pending->rlocator;
			rptr++;
		}
	}
	return nrels;
}

/*
 *	PostPrepare_smgr -- Clean up after a successful PREPARE
 *
 * What we have to do here is throw away the in-memory state about pending
 * relation deletes.  It's all been recorded in the 2PC state file and
 * it's no longer smgr's job to worry about it.
 */
void
PostPrepare_smgr(void)
{
	PendingRelDelete *pending;
	PendingRelDelete *next;

	for (pending = pendingDeletes; pending != NULL; pending = next)
	{
		next = pending->next;
		pendingDeletes = next;
		/* must explicitly free the list entry */
		pfree(pending);
	}
}


/*
 * AtSubCommit_smgr() --- Take care of subtransaction commit.
 *
 * Reassign all items in the pending-deletes list to the parent transaction.
 */
void
AtSubCommit_smgr(void)
{
	int			nestLevel = GetCurrentTransactionNestLevel();
	PendingRelDelete *pending;

	for (pending = pendingDeletes; pending != NULL; pending = pending->next)
	{
		if (pending->nestLevel >= nestLevel)
			pending->nestLevel = nestLevel - 1;
	}
}

/*
 * AtSubAbort_smgr() --- Take care of subtransaction abort.
 *
 * Delete created relations and forget about deleted relations.
 * We can execute these operations immediately because we know this
 * subtransaction will not commit.
 */
void
AtSubAbort_smgr(void)
{
	smgrDoPendingDeletes(false);
}

void
smgr_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	/* Backup blocks are not used in smgr records */
	Assert(!XLogRecHasAnyBlockRefs(record));

	if (info == XLOG_SMGR_CREATE)
	{
		xl_smgr_create *xlrec = (xl_smgr_create *) XLogRecGetData(record);
		SMgrFileHandle sfile;

		sfile = smgropen(xlrec->rlocator, InvalidBackendId, xlrec->forkNum);
		smgrcreate(sfile, true);
	}
	else if (info == XLOG_SMGR_TRUNCATE)
	{
		xl_smgr_truncate *xlrec = (xl_smgr_truncate *) XLogRecGetData(record);
		SMgrFileHandle sfile;
		Relation	rel;
		ForkNumber	forks[MAX_FORKNUM];
		BlockNumber blocks[MAX_FORKNUM];
		int			nforks = 0;
		bool		need_fsm_vacuum = false;

		sfile = smgropen(xlrec->rlocator, InvalidBackendId, MAIN_FORKNUM);

		/*
		 * Forcibly create relation if it doesn't exist (which suggests that
		 * it was dropped somewhere later in the WAL sequence).  As in
		 * XLogReadBufferForRedo, we prefer to recreate the rel and replay the
		 * log as best we can until the drop is seen.
		 */
		smgrcreate(sfile, true);

		/*
		 * Before we perform the truncation, update minimum recovery point to
		 * cover this WAL record. Once the relation is truncated, there's no
		 * going back. The buffer manager enforces the WAL-first rule for
		 * normal updates to relation files, so that the minimum recovery
		 * point is always updated before the corresponding change in the data
		 * file is flushed to disk. We have to do the same manually here.
		 *
		 * Doing this before the truncation means that if the truncation fails
		 * for some reason, you cannot start up the system even after restart,
		 * until you fix the underlying situation so that the truncation will
		 * succeed. Alternatively, we could update the minimum recovery point
		 * after truncation, but that would leave a small window where the
		 * WAL-first rule could be violated.
		 */
		XLogFlush(lsn);

		/* Prepare for truncation of MAIN fork */
		if ((xlrec->flags & SMGR_TRUNCATE_HEAP) != 0)
		{
			forks[nforks] = MAIN_FORKNUM;
			blocks[nforks] = xlrec->blkno;
			nforks++;

			/* Also tell xlogutils.c about it */
			XLogTruncateRelation(xlrec->rlocator, MAIN_FORKNUM, xlrec->blkno);
		}

		/* Prepare for truncation of FSM and VM too */
		rel = CreateFakeRelcacheEntry(xlrec->rlocator);

		DropRelationBuffers(xlrec->rlocator, InvalidBackendId, forks, nforks, blocks);

		if ((xlrec->flags & SMGR_TRUNCATE_FSM) != 0 &&
			smgrexists(smgropen(xlrec->rlocator, InvalidBackendId, FSM_FORKNUM)))
		{
			blocks[nforks] = FreeSpaceMapPrepareTruncateRel(rel, xlrec->blkno);
			if (BlockNumberIsValid(blocks[nforks]))
			{
				forks[nforks] = FSM_FORKNUM;
				nforks++;
				need_fsm_vacuum = true;
			}
		}
		if ((xlrec->flags & SMGR_TRUNCATE_VM) != 0 &&
			smgrexists(smgropen(xlrec->rlocator, InvalidBackendId, VISIBILITYMAP_FORKNUM)))
		{
			blocks[nforks] = visibilitymap_prepare_truncate(rel, xlrec->blkno);
			if (BlockNumberIsValid(blocks[nforks]))
			{
				forks[nforks] = VISIBILITYMAP_FORKNUM;
				nforks++;
			}
		}

		/* Do the real work to truncate relation forks */
		if (nforks > 0)
		{
			DropRelationBuffers(xlrec->rlocator, InvalidBackendId, forks, nforks, blocks);
			smgrtruncate_multi(xlrec->rlocator, InvalidBackendId, forks, nforks, blocks);
		}

		/*
		 * Update upper-level FSM pages to account for the truncation. This is
		 * important because the just-truncated pages were likely marked as
		 * all-free, and would be preferentially selected.
		 */
		if (need_fsm_vacuum)
			FreeSpaceMapVacuumRange(rel, xlrec->blkno,
									InvalidBlockNumber);

		FreeFakeRelcacheEntry(rel);
	}
	else
		elog(PANIC, "smgr_redo: unknown op code %u", info);
}
