/*-------------------------------------------------------------------------
 *
 * storage.c
 *	  code to create and destroy physical storage for relations
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
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

#include "miscadmin.h"

#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "storage/freespace.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"

 /* #define STORAGEDEBUG */	/* turns DEBUG elogs on */

#ifdef STORAGEDEBUG
#define STORAGE_elog(...)				elog(__VA_ARGS__)
#else
#define STORAGE_elog(...)
#endif

/*
 * We keep a list of all relations (represented as RelFileNode values)
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
	RelFileNode relnode;		/* relation that may need to be deleted */
	BackendId	backend;		/* InvalidBackendId if not a temp rel */
	bool		atCommit;		/* T=delete at commit; F=delete at abort */
	int			nestLevel;		/* xact nesting level of request */
	struct PendingRelDelete *next;	/* linked-list link */
} PendingRelDelete;

static PendingRelDelete *pendingDeletes = NULL; /* head of linked list */

/*
 * We also track relation files (RelFileNode values) that have been created
 * in the same transaction, and that have been modified without WAL-logging
 * the action (an optimization possible with wal_level=minimal). When we are
 * about to skip WAL-logging, a RelWalSkip entry is created, and
 * 'skip_wal_min_blk' is set to the current size of the relation. Any
 * operations on blocks < skip_wal_min_blk need to be WAL-logged as usual, but
 * for operations on higher blocks, WAL-logging is skipped.

 *
 * NB: after WAL-logging has been skipped for a block, we must not WAL-log
 * any subsequent actions on the same block either. Replaying the WAL record
 * of the subsequent action might fail otherwise, as the "before" state of
 * the block might not match, as the earlier actions were not WAL-logged.
 * Likewise, after we have WAL-logged an operation for a block, we must
 * WAL-log any subsequent operations on the same page as well. Replaying
 * a possible full-page-image from the earlier WAL record would otherwise
 * revert the page to the old state, even if we sync the relation at end
 * of transaction.
 *
 * If a relation is truncated (without creating a new relfilenode), and we
 * emit a WAL record of the truncation, we can't skip WAL-logging for any
 * of the truncated blocks anymore, as replaying the truncation record will
 * destroy all the data inserted after that. But if we have already decided
 * to skip WAL-logging changes to a relation, and the relation is truncated,
 * we don't need to WAL-log the truncation either.
 *
 * This mechanism is currently only used by heaps. Indexes are always
 * WAL-logged. Also, this only applies for wal_level=minimal; with higher
 * WAL levels we need the WAL for PITR/replication anyway.
 */
typedef struct RelWalSkip
{
	RelFileNode relnode;			/* relation created in same xact */
	bool		forks[MAX_FORKNUM + 1];	/* target forknums */
	BlockNumber skip_wal_min_blk;	/* WAL-logging skipped for blocks >=
									 * skip_wal_min_blk */
	BlockNumber wal_log_min_blk; 	/* The minimum blk number that requires
									 * WAL-logging even if skipped by the
									 * above*/
	SubTransactionId create_sxid;	/* subxid where this entry is created */
	SubTransactionId invalidate_sxid; /* subxid where this entry is
									   * invalidated */
	const TableAmRoutine *tableam;	/* Table access routine */
}	RelWalSkip;

/* Relations that need to be fsync'd at commit */
static HTAB *walSkipHash = NULL;

static RelWalSkip *getWalSkipEntry(Relation rel, bool create);
static RelWalSkip *getWalSkipEntryRNode(RelFileNode *node,
													  bool create);
static void smgrProcessWALSkipInval(bool isCommit, SubTransactionId mySubid,
						SubTransactionId parentSubid);

/*
 * RelationCreateStorage
 *		Create physical storage for a relation.
 *
 * Create the underlying disk file storage for the relation. This only
 * creates the main fork; additional forks are created lazily by the
 * modules that need them.
 *
 * This function is transactional. The creation is WAL-logged, and if the
 * transaction aborts later on, the storage will be destroyed.
 */
void
RelationCreateStorage(RelFileNode rnode, char relpersistence)
{
	PendingRelDelete *pending;
	SMgrRelation srel;
	BackendId	backend;
	bool		needs_wal;

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
			return;				/* placate compiler */
	}

	srel = smgropen(rnode, backend);
	smgrcreate(srel, MAIN_FORKNUM, false);

	if (needs_wal)
		log_smgrcreate(&srel->smgr_rnode.node, MAIN_FORKNUM);

	/* Add the relation to the list of stuff to delete at abort */
	pending = (PendingRelDelete *)
		MemoryContextAlloc(TopMemoryContext, sizeof(PendingRelDelete));
	pending->relnode = rnode;
	pending->backend = backend;
	pending->atCommit = false;	/* delete if abort */
	pending->nestLevel = GetCurrentTransactionNestLevel();
	pending->next = pendingDeletes;
	pendingDeletes = pending;
}

/*
 * Perform XLogInsert of an XLOG_SMGR_CREATE record to WAL.
 */
void
log_smgrcreate(RelFileNode *rnode, ForkNumber forkNum)
{
	xl_smgr_create xlrec;

	/*
	 * Make an XLOG entry reporting the file creation.
	 */
	xlrec.rnode = *rnode;
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
	pending->relnode = rel->rd_node;
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
RelationPreserveStorage(RelFileNode rnode, bool atCommit)
{
	PendingRelDelete *pending;
	PendingRelDelete *prev;
	PendingRelDelete *next;

	prev = NULL;
	for (pending = pendingDeletes; pending != NULL; pending = next)
	{
		next = pending->next;
		if (RelFileNodeEquals(rnode, pending->relnode)
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

	/* Open it at the smgr level if not already done */
	RelationOpenSmgr(rel);

	/*
	 * Make sure smgr_targblock etc aren't pointing somewhere past new end
	 */
	rel->rd_smgr->smgr_targblock = InvalidBlockNumber;
	rel->rd_smgr->smgr_fsm_nblocks = InvalidBlockNumber;
	rel->rd_smgr->smgr_vm_nblocks = InvalidBlockNumber;

	/* Truncate the FSM first if it exists */
	fsm = smgrexists(rel->rd_smgr, FSM_FORKNUM);
	if (fsm)
		FreeSpaceMapTruncateRel(rel, nblocks);

	/* Truncate the visibility map too if it exists. */
	vm = smgrexists(rel->rd_smgr, VISIBILITYMAP_FORKNUM);
	if (vm)
		visibilitymap_truncate(rel, nblocks);

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
		RelWalSkip *walskip;

		/* get pending sync entry, create if not yet */
		walskip = getWalSkipEntry(rel, true);

		/*
		 * walskip is null here if rel doesn't support WAL-logging skip,
		 * otherwise check for WAL-skipping status.
		 */
		if (walskip == NULL ||
			walskip->skip_wal_min_blk == InvalidBlockNumber ||
			walskip->skip_wal_min_blk < nblocks)
		{
			/*
			 * If WAL-skipping is enabled, this is the first time truncation
			 * of this relation in this transaction or truncation that leaves
			 * pages that need at-commit fsync.  Make an XLOG entry reporting
			 * the file truncation.
			 */
			XLogRecPtr		lsn;
			xl_smgr_truncate xlrec;

			xlrec.blkno = nblocks;
			xlrec.rnode = rel->rd_node;
			xlrec.flags = SMGR_TRUNCATE_ALL;

			XLogBeginInsert();
			XLogRegisterData((char *) &xlrec, sizeof(xlrec));

			lsn = XLogInsert(RM_SMGR_ID,
							 XLOG_SMGR_TRUNCATE | XLR_SPECIAL_REL_UPDATE);

			STORAGE_elog(DEBUG2,
						 "WAL-logged truncation of rel %u/%u/%u to %u blocks",
						 rel->rd_node.spcNode, rel->rd_node.dbNode,
						 rel->rd_node.relNode, nblocks);
			/*
			 * Flush, because otherwise the truncation of the main relation
			 * might hit the disk before the WAL record, and the truncation of
			 * the FSM or visibility map. If we crashed during that window,
			 * we'd be left with a truncated heap, but the FSM or visibility
			 * map would still contain entries for the non-existent heap
			 * pages.
			 */
			if (fsm || vm)
				XLogFlush(lsn);

			if (walskip)
			{
				/* no longer skip WAL-logging for the blocks */
				walskip->wal_log_min_blk = nblocks;
			}
		}
	}

	/* Do the real work */
	smgrtruncate(rel->rd_smgr, MAIN_FORKNUM, nblocks);
}

/*
 * Copy a fork's data, block by block.
 */
void
RelationCopyStorage(Relation srcrel, SMgrRelation dst, ForkNumber forkNum)
{
	PGAlignedBlock buf;
	Page		page;
	bool		use_wal;
	bool		copying_initfork;
	BlockNumber nblocks;
	BlockNumber blkno;
	SMgrRelation src = srcrel->rd_smgr;
	char 		relpersistence = srcrel->rd_rel->relpersistence;

	page = (Page) buf.data;

	/*
	 * The init fork for an unlogged relation in many respects has to be
	 * treated the same as normal relation, changes need to be WAL logged and
	 * it needs to be synced to disk.
	 */
	copying_initfork = relpersistence == RELPERSISTENCE_UNLOGGED &&
		forkNum == INIT_FORKNUM;

	if (relpersistence == RELPERSISTENCE_PERMANENT || copying_initfork)
	{
		/*
		 * We need to log the copied data in WAL iff WAL archiving/streaming
		 * is enabled AND it's a permanent relation.
		 */
		if (XLogIsNeeded())
			use_wal = true;

		/*
		 * If the rel is WAL-logged, must fsync before commit.  We use
		 * heap_sync to ensure that the toast table gets fsync'd too.  (For a
		 * temp or unlogged rel we don't care since the data will be gone
		 * after a crash anyway.)
		 *
		 * It's obvious that we must do this when not WAL-logging the
		 * copy. It's less obvious that we have to do it even if we did
		 * WAL-log the copied pages. The reason is that since we're copying
		 * outside shared buffers, a CHECKPOINT occurring during the copy has
		 * no way to flush the previously written data to disk (indeed it
		 * won't know the new rel even exists).  A crash later on would replay
		 * WAL from the checkpoint, therefore it wouldn't replay our earlier
		 * WAL entries. If we do not fsync those pages here, they might still
		 * not be on disk when the crash occurs.
		 */
		RecordPendingSync(srcrel, dst, forkNum);
	}

	nblocks = smgrnblocks(src, forkNum);

	for (blkno = 0; blkno < nblocks; blkno++)
	{
		/* If we got a cancel signal during the copy of the data, quit */
		CHECK_FOR_INTERRUPTS();

		smgrread(src, forkNum, blkno, buf.data);

		if (!PageIsVerified(page, blkno))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid page in block %u of relation %s",
							blkno,
							relpathbackend(src->smgr_rnode.node,
										   src->smgr_rnode.backend,
										   forkNum))));

		/*
		 * WAL-log the copied page. Unfortunately we don't know what kind of a
		 * page this is, so we have to log the full page including any unused
		 * space.
		 */
		if (use_wal)
			log_newpage(&dst->smgr_rnode.node, forkNum, blkno, page, false);

		PageSetChecksumInplace(page, blkno);

		/*
		 * Now write the page.  We say isTemp = true even if it's not a temp
		 * rel, because there's no need for smgr to schedule an fsync for this
		 * write; we'll do it ourselves below.
		 */
		smgrextend(dst, forkNum, blkno, buf.data, true);
	}
}

/*
 * Do changes to given heap page need to be WAL-logged?
 *
 * This takes into account any previous RecordPendingSync() requests.
 *
 * Note that it is required to check this before creating any WAL records for
 * heap pages - it is not merely an optimization! WAL-logging a record, when
 * we have already skipped a previous WAL record for the same page could lead
 * to failure at WAL replay, as the "before" state expected by the record
 * might not match what's on disk. Also, if the heap was truncated earlier, we
 * must WAL-log any changes to the once-truncated blocks, because replaying
 * the truncation record will destroy them.
 */
bool
BufferNeedsWAL(Relation rel, Buffer buf)
{
	BlockNumber		blkno = InvalidBlockNumber;
	RelWalSkip *walskip;

	if (!RelationNeedsWAL(rel))
		return false;

	/* fetch existing pending sync entry */
	walskip = getWalSkipEntry(rel, false);

	/*
	 * no point in doing further work if we know that we don't skip
	 * WAL-logging.
	 */
	if (!walskip)
	{
		STORAGE_elog(DEBUG2,
					 "not skipping WAL-logging for rel %u/%u/%u block %u",
					 rel->rd_node.spcNode, rel->rd_node.dbNode,
					 rel->rd_node.relNode, BufferGetBlockNumber(buf));
		return true;
	}

	Assert(BufferIsValid(buf));

	blkno = BufferGetBlockNumber(buf);

	/*
	 * We don't skip WAL-logging for pages that once done.
	 */
	if (walskip->skip_wal_min_blk == InvalidBlockNumber ||
		walskip->skip_wal_min_blk > blkno)
	{
		STORAGE_elog(DEBUG2, "not skipping WAL-logging for rel %u/%u/%u block %u, because skip_wal_min_blk is %u",
					 rel->rd_node.spcNode, rel->rd_node.dbNode,
					 rel->rd_node.relNode, blkno, walskip->skip_wal_min_blk);
		return true;
	}

	/*
	 * we don't skip WAL-logging for blocks that have got WAL-logged
	 * truncation
	 */
	if (walskip->wal_log_min_blk != InvalidBlockNumber &&
		walskip->wal_log_min_blk <= blkno)
	{
		STORAGE_elog(DEBUG2, "not skipping WAL-logging for rel %u/%u/%u block %u, because wal_log_min_blk is %u",
					 rel->rd_node.spcNode, rel->rd_node.dbNode,
					 rel->rd_node.relNode, blkno, walskip->wal_log_min_blk);
		return true;
	}

	STORAGE_elog(DEBUG2, "skipping WAL-logging for rel %u/%u/%u block %u",
				 rel->rd_node.spcNode, rel->rd_node.dbNode,
				 rel->rd_node.relNode, blkno);

	return false;
}

bool
BlockNeedsWAL(Relation rel, BlockNumber blkno)
{
	RelWalSkip *walskip;

	if (!RelationNeedsWAL(rel))
		return false;

	/* fetch exising pending sync entry */
	walskip = getWalSkipEntry(rel, false);

	/*
	 * no point in doing further work if we know that we don't skip
	 * WAL-logging.
	 */
	if (!walskip)
		return true;

	/*
	 * We don't skip WAL-logging for pages that once done.
	 */
	if (walskip->skip_wal_min_blk == InvalidBlockNumber ||
		walskip->skip_wal_min_blk > blkno)
	{
		STORAGE_elog(DEBUG2, "not skipping WAL-logging for rel %u/%u/%u block %u, because skip_wal_min_blk is %u",
					 rel->rd_node.spcNode, rel->rd_node.dbNode,
					 rel->rd_node.relNode, blkno, walskip->skip_wal_min_blk);
		return true;
	}

	/*
	 * we don't skip WAL-logging for blocks that have got WAL-logged
	 * truncation
	 */
	if (walskip->wal_log_min_blk != InvalidBlockNumber &&
		walskip->wal_log_min_blk <= blkno)
	{
		STORAGE_elog(DEBUG2, "not skipping WAL-logging for rel %u/%u/%u block %u, because wal_log_min_blk is %u",
					 rel->rd_node.spcNode, rel->rd_node.dbNode,
					 rel->rd_node.relNode, blkno, walskip->wal_log_min_blk);

		return true;
	}

	STORAGE_elog(DEBUG2, "skipping WAL-logging for rel %u/%u/%u block %u",
				 rel->rd_node.spcNode, rel->rd_node.dbNode,
				 rel->rd_node.relNode, blkno);

	return false;
}

/*
 * Remember that the given relation doesn't need WAL-logging for the blocks
 * after the current block size and for the blocks that are going to be synced
 * at commit.
 */
void
RecordWALSkipping(Relation rel)
{
	RelWalSkip *walskip;

	Assert(RelationNeedsWAL(rel));

	/* get pending sync entry, create if not yet  */
	walskip = getWalSkipEntry(rel, true);

	if (walskip == NULL)
		return;

	/*
	 *  Record only the first registration.
	 */
	if (walskip->skip_wal_min_blk != InvalidBlockNumber)
	{
		STORAGE_elog(DEBUG2, "WAL skipping for rel %u/%u/%u was already registered at block %u (new %u)",
					 rel->rd_node.spcNode, rel->rd_node.dbNode,
					 rel->rd_node.relNode, walskip->skip_wal_min_blk,
					 RelationGetNumberOfBlocks(rel));
		return;
	}

	STORAGE_elog(DEBUG2, "registering new WAL skipping rel %u/%u/%u at block %u",
				 rel->rd_node.spcNode, rel->rd_node.dbNode,
				 rel->rd_node.relNode, RelationGetNumberOfBlocks(rel));

	walskip->skip_wal_min_blk = RelationGetNumberOfBlocks(rel);
}

/*
 * Record commit-time file sync. This shouldn't be used mixing with
 * RecordWALSkipping.
 */
void
RecordPendingSync(Relation rel, SMgrRelation targetsrel, ForkNumber forknum)
{
	RelWalSkip *walskip;

	Assert(RelationNeedsWAL(rel));

	/* check for support for this feature */
	if (rel->rd_tableam == NULL ||
		rel->rd_tableam->relation_register_walskip == NULL)
		return;

	walskip = getWalSkipEntryRNode(&targetsrel->smgr_rnode.node, true);
	walskip->forks[forknum] = true;
	walskip->skip_wal_min_blk = 0;
	walskip->tableam = rel->rd_tableam;

	STORAGE_elog(DEBUG2,
				 "registering new pending sync for rel %u/%u/%u at block %u",
				 walskip->relnode.spcNode, walskip->relnode.dbNode,
				 walskip->relnode.relNode, 0);
}

/*
 * RelationInvalidateWALSkip() -- invalidate WAL-skip entry
 */
void
RelationInvalidateWALSkip(Relation rel)
{
	RelWalSkip *walskip;

	/* we know we don't have one */
	if (rel->rd_nowalskip)
		return;

	walskip = getWalSkipEntry(rel, false);

	if (!walskip)
		return;

	/*
	 * The state is reset at subtransaction commit/abort. No invalidation
	 * request must not come for the same relation in the same subtransaction.
	 */
	Assert(walskip->invalidate_sxid == InvalidSubTransactionId);

	walskip->invalidate_sxid = GetCurrentSubTransactionId();

	STORAGE_elog(DEBUG2,
				 "WAL skip of rel %u/%u/%u invalidated by sxid %d",
				 walskip->relnode.spcNode, walskip->relnode.dbNode,
				 walskip->relnode.relNode, walskip->invalidate_sxid);
}

/*
 * getWalSkipEntry: get WAL skip entry.
 *
 * Returns WAL skip entry for the relation. The entry tracks WAL-skipping
 * blocks for the relation.  The WAL-skipped blocks need fsync at commit time.
 * Creates one if needed when create is true. If rel doesn't support this
 * feature, returns true even if create is true.
 */
static inline RelWalSkip *
getWalSkipEntry(Relation rel, bool create)
{
	RelWalSkip *walskip_entry = NULL;

	if (rel->rd_walskip)
		return rel->rd_walskip;

	/* we know we don't have pending sync entry */
	if (!create && rel->rd_nowalskip)
		return NULL;

	/* check for support for this feature */
	if (rel->rd_tableam == NULL ||
		rel->rd_tableam->relation_register_walskip == NULL)
	{
		rel->rd_nowalskip = true;
		return NULL;
	}

	walskip_entry = getWalSkipEntryRNode(&rel->rd_node, create);

	if (!walskip_entry)
	{
		/* prevent further hash lookup */
		rel->rd_nowalskip = true;
		return NULL;
	}

	walskip_entry->forks[MAIN_FORKNUM] = true;
	walskip_entry->tableam = rel->rd_tableam;

	/* hold shortcut in Relation */
	rel->rd_nowalskip = false;
	rel->rd_walskip = walskip_entry;

	return walskip_entry;
}

/*
 * getWalSkipEntryRNode: get WAL skip entry by rnode
 *
 * Returns a WAL skip entry for the RelFileNode.
 */
static RelWalSkip *
getWalSkipEntryRNode(RelFileNode *rnode, bool create)
{
	RelWalSkip *walskip_entry = NULL;
	bool			found;

	if (!walSkipHash)
	{
		/* First time through: initialize the hash table */
		HASHCTL		ctl;

		if (!create)
			return NULL;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(RelFileNode);
		ctl.entrysize = sizeof(RelWalSkip);
		ctl.hash = tag_hash;
		walSkipHash = hash_create("pending relation sync table", 5,
								   &ctl, HASH_ELEM | HASH_FUNCTION);
	}

	walskip_entry = (RelWalSkip *)
		hash_search(walSkipHash, (void *) rnode,
					create ? HASH_ENTER: HASH_FIND,	&found);

	if (!walskip_entry)
		return NULL;

	/* new entry created */
	if (!found)
	{
		memset(&walskip_entry->forks, 0, sizeof(walskip_entry->forks));
		walskip_entry->wal_log_min_blk = InvalidBlockNumber;
		walskip_entry->skip_wal_min_blk = InvalidBlockNumber;
		walskip_entry->create_sxid = GetCurrentSubTransactionId();
		walskip_entry->invalidate_sxid = InvalidSubTransactionId;
		walskip_entry->tableam = NULL;
	}

	return walskip_entry;
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
	int			nrels = 0,
				i = 0,
				maxrels = 0;
	SMgrRelation *srels = NULL;

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
				SMgrRelation srel;

				srel = smgropen(pending->relnode, pending->backend);

				/* allocate the initial array, or extend it, if needed */
				if (maxrels == 0)
				{
					maxrels = 8;
					srels = palloc(sizeof(SMgrRelation) * maxrels);
				}
				else if (maxrels <= nrels)
				{
					maxrels *= 2;
					srels = repalloc(srels, sizeof(SMgrRelation) * maxrels);
				}

				srels[nrels++] = srel;
			}
			/* must explicitly free the list entry */
			pfree(pending);
			/* prev does not change */
		}
	}

	if (nrels > 0)
	{
		smgrdounlinkall(srels, nrels, false);

		for (i = 0; i < nrels; i++)
			smgrclose(srels[i]);

		pfree(srels);
	}
}

/*
 * smgrGetPendingDeletes() -- Get a list of non-temp relations to be deleted.
 *
 * The return value is the number of relations scheduled for termination.
 * *ptr is set to point to a freshly-palloc'd array of RelFileNodes.
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
smgrGetPendingDeletes(bool forCommit, RelFileNode **ptr)
{
	int			nestLevel = GetCurrentTransactionNestLevel();
	int			nrels;
	RelFileNode *rptr;
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
	rptr = (RelFileNode *) palloc(nrels * sizeof(RelFileNode));
	*ptr = rptr;
	for (pending = pendingDeletes; pending != NULL; pending = pending->next)
	{
		if (pending->nestLevel >= nestLevel && pending->atCommit == forCommit
			&& pending->backend == InvalidBackendId)
		{
			*rptr = pending->relnode;
			rptr++;
		}
	}
	return nrels;
}

/*
 * Finish bulk insert of files.
 */
void
smgrFinishBulkInsert(bool isCommit)
{
	if (!walSkipHash)
		return;

	if (isCommit)
	{
		HASH_SEQ_STATUS status;
		RelWalSkip *walskip;

		hash_seq_init(&status, walSkipHash);

		while ((walskip = hash_seq_search(&status)) != NULL)
		{
			/*
			 * On commit, process valid entreis. Rollback doesn't need sync on
			 * all changes during the transaction.
			 */
			if (walskip->skip_wal_min_blk != InvalidBlockNumber &&
				walskip->invalidate_sxid == InvalidSubTransactionId)
			{
				int f;

				FlushRelationBuffersWithoutRelCache(walskip->relnode, false);

				/*
				 * We mustn't create an entry when the table AM doesn't
				 * support WAL-skipping.
				 */
				Assert (walskip->tableam->finish_bulk_insert);

				/* flush all requested forks  */
				for (f = MAIN_FORKNUM ; f <= MAX_FORKNUM ; f++)
				{
					if (walskip->forks[f])
					{
						walskip->tableam->finish_bulk_insert(walskip->relnode, f);
						STORAGE_elog(DEBUG2, "finishing bulk insert to rel %u/%u/%u fork %d",
									 walskip->relnode.spcNode,
									 walskip->relnode.dbNode,
									 walskip->relnode.relNode, f);
					}
				}
			}
		}
	}

	hash_destroy(walSkipHash);
	walSkipHash = NULL;
}

/*
 * Process pending invalidation of WAL skip happened in the subtransaction
 */
void
smgrProcessWALSkipInval(bool isCommit, SubTransactionId mySubid,
						SubTransactionId parentSubid)
{
	HASH_SEQ_STATUS status;
	RelWalSkip *walskip;

	if (!walSkipHash)
		return;

	/* We expect that we don't have walSkipHash in almost all cases */
	hash_seq_init(&status, walSkipHash);

	while ((walskip = hash_seq_search(&status)) != NULL)
	{
		if (walskip->create_sxid == mySubid)
		{
			/*
			 * The entry was created in this subxact. Remove it on abort, or
			 * on commit after invalidation.
			 */
			if (!isCommit || walskip->invalidate_sxid == mySubid)
				hash_search(walSkipHash, &walskip->relnode,
							HASH_REMOVE, NULL);
			/* Treat committing valid entry as creation by the parent. */
			else if (walskip->invalidate_sxid == InvalidSubTransactionId)
				walskip->create_sxid = parentSubid;
		}
		else if (walskip->invalidate_sxid == mySubid)
		{
			/*
			 * This entry was created elsewhere then invalidated by this
			 * subxact. Treat commit as invalidation by the parent. Otherwise
			 * cancel invalidation.
			 */
			if (isCommit)
				walskip->invalidate_sxid = parentSubid;
			else
				walskip->invalidate_sxid = InvalidSubTransactionId;
		}
	}
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
AtSubCommit_smgr(SubTransactionId mySubid, SubTransactionId parentSubid)
{
	int			nestLevel = GetCurrentTransactionNestLevel();
	PendingRelDelete *pending;

	for (pending = pendingDeletes; pending != NULL; pending = pending->next)
	{
		if (pending->nestLevel >= nestLevel)
			pending->nestLevel = nestLevel - 1;
	}

	/* Remove invalidated WAL skip in this subtransaction */
	smgrProcessWALSkipInval(true, mySubid, parentSubid);
}

/*
 * AtSubAbort_smgr() --- Take care of subtransaction abort.
 *
 * Delete created relations and forget about deleted relations.
 * We can execute these operations immediately because we know this
 * subtransaction will not commit.
 */
void
AtSubAbort_smgr(SubTransactionId mySubid, SubTransactionId parentSubid)
{
	smgrDoPendingDeletes(false);

	/* Remove invalidated WAL skip in this subtransaction */
	smgrProcessWALSkipInval(false, mySubid, parentSubid);
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
		SMgrRelation reln;

		reln = smgropen(xlrec->rnode, InvalidBackendId);
		smgrcreate(reln, xlrec->forkNum, true);
	}
	else if (info == XLOG_SMGR_TRUNCATE)
	{
		xl_smgr_truncate *xlrec = (xl_smgr_truncate *) XLogRecGetData(record);
		SMgrRelation reln;
		Relation	rel;

		reln = smgropen(xlrec->rnode, InvalidBackendId);

		/*
		 * Forcibly create relation if it doesn't exist (which suggests that
		 * it was dropped somewhere later in the WAL sequence).  As in
		 * XLogReadBufferForRedo, we prefer to recreate the rel and replay the
		 * log as best we can until the drop is seen.
		 */
		smgrcreate(reln, MAIN_FORKNUM, true);

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

		if ((xlrec->flags & SMGR_TRUNCATE_HEAP) != 0)
		{
			smgrtruncate(reln, MAIN_FORKNUM, xlrec->blkno);

			/* Also tell xlogutils.c about it */
			XLogTruncateRelation(xlrec->rnode, MAIN_FORKNUM, xlrec->blkno);
		}

		/* Truncate FSM and VM too */
		rel = CreateFakeRelcacheEntry(xlrec->rnode);

		if ((xlrec->flags & SMGR_TRUNCATE_FSM) != 0 &&
			smgrexists(reln, FSM_FORKNUM))
			FreeSpaceMapTruncateRel(rel, xlrec->blkno);
		if ((xlrec->flags & SMGR_TRUNCATE_VM) != 0 &&
			smgrexists(reln, VISIBILITYMAP_FORKNUM))
			visibilitymap_truncate(rel, xlrec->blkno);

		FreeFakeRelcacheEntry(rel);
	}
	else
		elog(PANIC, "smgr_redo: unknown op code %u", info);
}
