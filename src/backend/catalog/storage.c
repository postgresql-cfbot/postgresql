/*-------------------------------------------------------------------------
 *
 * storage.c
 *	  code to create and destroy physical storage for relations
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
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

#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "storage/freespace.h"
#include "storage/smgr.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"

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
 * about to skip WAL-logging, a PendingRelSync entry is created, and
 * 'sync_above' is set to the current size of the relation. Any operations
 * on blocks < sync_above need to be WAL-logged as usual, but for operations
 * on higher blocks, WAL-logging is skipped.
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
typedef struct PendingRelSync
{
	RelFileNode relnode;		/* relation created in same xact */
	BlockNumber sync_above;		/* WAL-logging skipped for blocks >=
								 * sync_above */
	BlockNumber truncated_to;	/* truncation WAL record was written */
}	PendingRelSync;

/* Relations that need to be fsync'd at commit */
static HTAB *pendingSyncs = NULL;

static PendingRelSync *getPendingSyncEntry(Relation rel, bool create);

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
		PendingRelSync *pending_sync;

		/* get pending sync entry, create if not yet */
		pending_sync = getPendingSyncEntry(rel, true);

		if (pending_sync->sync_above == InvalidBlockNumber ||
			pending_sync->sync_above < nblocks)
		{
			/*
			 * This is the first time truncation of this relation in this
			 * transaction or truncation that leaves pages that need at-commit
			 * fsync.  Make an XLOG entry reporting the file truncation.
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

			elog(DEBUG2, "WAL-logged truncation of rel %u/%u/%u to %u blocks",
				 rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode,
				 nblocks);

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

			rel->pending_sync->truncated_to = nblocks;
		}
	}

	/* Do the real work */
	smgrtruncate(rel->rd_smgr, MAIN_FORKNUM, nblocks);
}

/*
 * getPendingSyncEntry: get pendig sync entry.
 *
 * Returns pending sync entry for the relation. The entry tracks pending
 * at-commit fsyncs for the relation.  Creates one if needed when create is
 * true.
 */  
static PendingRelSync *
getPendingSyncEntry(Relation rel, bool create)
{
	PendingRelSync *pendsync_entry = NULL;
	bool			found;

	if (rel->pending_sync)
		return rel->pending_sync;

	/* we know we don't have pending sync entry */
	if (!create && rel->no_pending_sync)
		return NULL;

	if (!pendingSyncs)
	{
		/* First time through: initialize the hash table */
		HASHCTL		ctl;

		if (!create)
			return NULL;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(RelFileNode);
		ctl.entrysize = sizeof(PendingRelSync);
		ctl.hash = tag_hash;
		pendingSyncs = hash_create("pending relation sync table", 5,
								   &ctl, HASH_ELEM | HASH_FUNCTION);
	}

	elog(DEBUG2, "getPendingSyncEntry: accessing hash for %d",
		 rel->rd_node.relNode);
	pendsync_entry = (PendingRelSync *)
		hash_search(pendingSyncs, (void *) &rel->rd_node,
					create ? HASH_ENTER: HASH_FIND,	&found);

	if (!pendsync_entry)
	{
		rel->no_pending_sync = true;
		return NULL;
	}

	/* new entry created */
	if (!found)
	{
		pendsync_entry->truncated_to = InvalidBlockNumber;
		pendsync_entry->sync_above = InvalidBlockNumber;
	}

	/* hold shortcut in Relation */
	rel->no_pending_sync = false;
	rel->pending_sync = pendsync_entry;

	return pendsync_entry;
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
 * RelationRemovePendingSync() -- remove pendingSync entry for a relation
 */
void
RelationRemovePendingSync(Relation rel)
{
	bool found;

	rel->pending_sync = NULL;
	rel->no_pending_sync = true;
	if (pendingSyncs)
	{
		elog(DEBUG2, "RelationRemovePendingSync: accessing hash");
		hash_search(pendingSyncs, (void *) &rel->rd_node, HASH_REMOVE, &found);
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
 * Remember that the given relation needs to be sync'd at commit, because we
 * are going to skip WAL-logging subsequent actions to it.
 */
void
RecordPendingSync(Relation rel)
{
	BlockNumber nblocks;
	PendingRelSync *pending_sync;

	Assert(RelationNeedsWAL(rel));

	/* get pending sync entry, create if not yet  */
	pending_sync = getPendingSyncEntry(rel, true);

	nblocks = RelationGetNumberOfBlocks(rel);

	if (pending_sync->sync_above != InvalidBlockNumber)
	{
		elog(DEBUG2,
			 "pending sync for rel %u/%u/%u was already registered at block %u (new %u)",
			 rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode,
			 rel->pending_sync->sync_above, nblocks);

		return;
	}

	elog(DEBUG2,
		 "registering new pending sync for rel %u/%u/%u at block %u",
		 rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode,
		 nblocks);
	pending_sync->sync_above = nblocks;
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
	PendingRelSync *pending_sync;

	if (!RelationNeedsWAL(rel))
		return false;

	/* fetch exising pending sync entry */
	pending_sync = getPendingSyncEntry(rel, false);

	/*
	 * no point in doing further work if we know that we don't have pending
	 * sync
	 */
	if (!pending_sync)
		return true;

	Assert(BufferIsValid(buf));

	blkno = BufferGetBlockNumber(buf);

	/* we don't skip WAL-logging for pages that already done */
	if (pending_sync->sync_above == InvalidBlockNumber ||
		pending_sync->sync_above > blkno)
	{
		elog(DEBUG2, "not skipping WAL-logging for rel %u/%u/%u block %u, because sync_above is %u",
			 rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode,
			 blkno, rel->pending_sync->sync_above);
		return true;
	}

	/*
	 * We have emitted a truncation record for this block.
	 */
	if (pending_sync->truncated_to != InvalidBlockNumber &&
		pending_sync->truncated_to <= blkno)
	{
		elog(DEBUG2, "not skipping WAL-logging for rel %u/%u/%u block %u, because it was truncated earlier in the same xact",
			 rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode,
			 blkno);
		return true;
	}

	elog(DEBUG2, "skipping WAL-logging for rel %u/%u/%u block %u",
		 rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode,
		 blkno);

	return false;
}

/*
 * Sync to disk any relations that we skipped WAL-logging for earlier.
 */
void
smgrDoPendingSyncs(bool isCommit)
{
	if (!pendingSyncs)
		return;

	if (isCommit)
	{
		HASH_SEQ_STATUS status;
		PendingRelSync *pending;

		hash_seq_init(&status, pendingSyncs);

		while ((pending = hash_seq_search(&status)) != NULL)
		{
			if (pending->sync_above != InvalidBlockNumber)
			{
				FlushRelationBuffersWithoutRelCache(pending->relnode, false);
				smgrimmedsync(smgropen(pending->relnode, InvalidBackendId), MAIN_FORKNUM);

				elog(DEBUG2, "syncing rel %u/%u/%u", pending->relnode.spcNode,
					 pending->relnode.dbNode, pending->relnode.relNode);
			}
		}
	}

	hash_destroy(pendingSyncs);
	pendingSyncs = NULL;
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
