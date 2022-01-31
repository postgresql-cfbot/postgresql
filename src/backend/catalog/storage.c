/*-------------------------------------------------------------------------
 *
 * storage.c
 *	  code to create and destroy physical storage for relations
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
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

#include "access/amapi.h"
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

#define	PCOP_UNLINK_FORK		(1 << 0)
#define	PCOP_UNLINK_MARK		(1 << 1)
#define	PCOP_SET_PERSISTENCE	(1 << 2)

typedef struct PendingCleanup
{
	RelFileNode relnode;		/* relation that may need to be deleted */
	int			op;				/* operation mask */
	bool		bufpersistence;	/* buffer persistence to set */
	int			unlink_forknum;	/* forknum to unlink */
	StorageMarks unlink_mark;	/* mark to unlink */
	BackendId	backend;		/* InvalidBackendId if not a temp rel */
	bool		atCommit;		/* T=delete at commit; F=delete at abort */
	int			nestLevel;		/* xact nesting level of request */
	struct PendingCleanup *next;	/* linked-list link */
} PendingCleanup;

typedef struct PendingRelSync
{
	RelFileNode rnode;
	bool		is_truncated;	/* Has the file experienced truncation? */
} PendingRelSync;

static PendingRelDelete *pendingDeletes = NULL; /* head of linked list */
static PendingCleanup   *pendingCleanups = NULL; /* head of linked list */
HTAB	   *pendingSyncHash = NULL;


/*
 * AddPendingSync
 *		Queue an at-commit fsync.
 */
static void
AddPendingSync(const RelFileNode *rnode)
{
	PendingRelSync *pending;
	bool		found;

	/* create the hash if not yet */
	if (!pendingSyncHash)
	{
		HASHCTL		ctl;

		ctl.keysize = sizeof(RelFileNode);
		ctl.entrysize = sizeof(PendingRelSync);
		ctl.hcxt = TopTransactionContext;
		pendingSyncHash = hash_create("pending sync hash", 16, &ctl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	pending = hash_search(pendingSyncHash, rnode, HASH_ENTER, &found);
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
 * transaction aborts later on, the storage will be destroyed.
 */
SMgrRelation
RelationCreateStorage(RelFileNode rnode, char relpersistence)
{
	PendingRelDelete *pendingdel;
	PendingCleanup	 *pendingclean;
	SMgrRelation srel;
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

	/*
	 * We are going to create a new storage file. If server crashes before the
	 * current transaction ends the file needs to be cleaned up. The
	 * SMGR_MARK_UNCOMMITED mark file prompts that work at the next startup.
	 */
	srel = smgropen(rnode, backend);
	log_smgrcreatemark(&rnode, MAIN_FORKNUM, SMGR_MARK_UNCOMMITTED);
	smgrcreatemark(srel, MAIN_FORKNUM, SMGR_MARK_UNCOMMITTED, false);
	smgrcreate(srel, MAIN_FORKNUM, false);

	if (needs_wal)
		log_smgrcreate(&srel->smgr_rnode.node, MAIN_FORKNUM);

	/* Add the relation to the list of stuff to delete at abort */
	pendingdel = (PendingRelDelete *)
		MemoryContextAlloc(TopMemoryContext, sizeof(PendingRelDelete));
	pendingdel->relnode = rnode;
	pendingdel->backend = backend;
	pendingdel->atCommit = false;	/* delete if abort */
	pendingdel->nestLevel = GetCurrentTransactionNestLevel();
	pendingdel->next = pendingDeletes;
	pendingDeletes = pendingdel;

	/* drop mark files at commit */
	pendingclean = (PendingCleanup *)
		MemoryContextAlloc(TopMemoryContext, sizeof(PendingCleanup));
	pendingclean->relnode = rnode;
	pendingclean->op = PCOP_UNLINK_MARK;
	pendingclean->unlink_forknum = MAIN_FORKNUM;
	pendingclean->unlink_mark = SMGR_MARK_UNCOMMITTED;
	pendingclean->backend = backend;
	pendingclean->atCommit = true;
	pendingclean->nestLevel = GetCurrentTransactionNestLevel();
	pendingclean->next = pendingCleanups;
	pendingCleanups = pendingclean;

	if (relpersistence == RELPERSISTENCE_PERMANENT && !XLogIsNeeded())
	{
		Assert(backend == InvalidBackendId);
		AddPendingSync(&rnode);
	}

	return srel;
}

/*
 * RelationCreateInitFork
 *		Create physical storage for the init fork of a relation.
 *
 * Create the init fork for the relation.
 *
 * This function is transactional. The creation is WAL-logged, and if the
 * transaction aborts later on, the init fork will be removed.
 */
void
RelationCreateInitFork(Relation rel)
{
	RelFileNode rnode = rel->rd_node;
	PendingCleanup *pending;
	PendingCleanup *prev;
	PendingCleanup *next;
	SMgrRelation srel;
	bool			  create = true;

	/* switch buffer persistence */
	SetRelationBuffersPersistence(RelationGetSmgr(rel), false, false);

	/*
	 * If we have entries for init-fork operations on this relation, that means
	 * that we have already registered pending delete entries to drop an
	 * init-fork preexisting since before the current transaction started. This
	 * function reverts that change just by removing the entries.
	 */
	prev = NULL;
	for (pending = pendingCleanups; pending != NULL; pending = next)
	{
		next = pending->next;

		if (RelFileNodeEquals(rnode, pending->relnode) &&
			pending->unlink_forknum == INIT_FORKNUM)
		{
			if (prev)
				prev->next = next;
			else
				pendingCleanups = next;

			pfree(pending);
			/* prev does not change */

			create = false;
		}
		else
			prev = pending;
	}

	if (!create)
		return;

	/*
	 * We are going to create an init fork. If server crashes before the
	 * current transaction ends the init fork left alone corrupts data while
	 * recovery.  The mark file works as the sentinel to identify that
	 * situation.
	 */
	srel = smgropen(rnode, InvalidBackendId);
	log_smgrcreatemark(&rnode, INIT_FORKNUM, SMGR_MARK_UNCOMMITTED);
	smgrcreatemark(srel, INIT_FORKNUM, SMGR_MARK_UNCOMMITTED, false);

	/* We don't have existing init fork, create it. */
	smgrcreate(srel, INIT_FORKNUM, false);

	/*
	 * index-init fork needs further initialization. ambuildempty shoud do
	 * WAL-log and file sync by itself but otherwise we do that by ourselves.
	 */
	if (rel->rd_rel->relkind == RELKIND_INDEX)
		rel->rd_indam->ambuildempty(rel);
	else
	{
		log_smgrcreate(&rnode, INIT_FORKNUM);
		smgrimmedsync(srel, INIT_FORKNUM);
	}

	/* drop the init fork, mark file and revert persistence at abort */
	pending = (PendingCleanup *)
		MemoryContextAlloc(TopMemoryContext, sizeof(PendingCleanup));
	pending->relnode = rnode;
	pending->op = PCOP_UNLINK_FORK | PCOP_UNLINK_MARK | PCOP_SET_PERSISTENCE;
	pending->unlink_forknum = INIT_FORKNUM;
	pending->unlink_mark = SMGR_MARK_UNCOMMITTED;
	pending->bufpersistence = true;
	pending->backend = InvalidBackendId;
	pending->atCommit = false;
	pending->nestLevel = GetCurrentTransactionNestLevel();
	pending->next = pendingCleanups;
	pendingCleanups = pending;

	/* drop mark file at commit */
	pending = (PendingCleanup *)
		MemoryContextAlloc(TopMemoryContext, sizeof(PendingCleanup));
	pending->relnode = rnode;
	pending->op = PCOP_UNLINK_MARK;
	pending->unlink_forknum = INIT_FORKNUM;
	pending->unlink_mark = SMGR_MARK_UNCOMMITTED;
	pending->backend = InvalidBackendId;
	pending->atCommit = true;
	pending->nestLevel = GetCurrentTransactionNestLevel();
	pending->next = pendingCleanups;
	pendingCleanups = pending;
}

/*
 * RelationDropInitFork
 *		Delete physical storage for the init fork of a relation.
 *
 * Register pending-delete of the init fork. The real deletion is performed by
 * smgrDoPendingDeletes at commit.
 *
 * This function is transactional. If the transaction aborts later on, the
 * deletion doesn't happen.
 */
void
RelationDropInitFork(Relation rel)
{
	RelFileNode rnode = rel->rd_node;
	PendingCleanup *pending;
	PendingCleanup *prev;
	PendingCleanup *next;
	bool		    inxact_created = false;

	/* switch buffer persistence */
	SetRelationBuffersPersistence(RelationGetSmgr(rel), true, false);

	/*
	 * If we have entries for init-fork operations of this relation, that means
	 * that we have created the init fork in the current transaction.  We
	 * remove the init fork and mark file immediately in that case.  Otherwise
	 * just register pending-delete for the existing init fork.
	 */
	prev = NULL;
	for (pending = pendingCleanups; pending != NULL; pending = next)
	{
		next = pending->next;

		if (RelFileNodeEquals(rnode, pending->relnode) &&
			pending->unlink_forknum != INIT_FORKNUM)
		{
			/* unlink list entry */
			if (prev)
				prev->next = next;
			else
				pendingCleanups = next;

			pfree(pending);
			/* prev does not change */

			inxact_created = true;
		}
		else
			prev = pending;
	}

	if (inxact_created)
	{
		SMgrRelation srel = smgropen(rnode, InvalidBackendId);

		/*
		 * INIT forks never be loaded to shared buffer so no point in dropping
		 * buffers for such files.
		 */
		log_smgrunlinkmark(&rnode, INIT_FORKNUM, SMGR_MARK_UNCOMMITTED);
		smgrunlinkmark(srel, INIT_FORKNUM, SMGR_MARK_UNCOMMITTED, false);
		log_smgrunlink(&rnode, INIT_FORKNUM);
		smgrunlink(srel, INIT_FORKNUM, false);
		return;
	}

	/* register drop of this init fork file at commit */
	pending = (PendingCleanup *)
		MemoryContextAlloc(TopMemoryContext, sizeof(PendingCleanup));
	pending->relnode = rnode;
	pending->op = PCOP_UNLINK_FORK;
	pending->unlink_forknum = INIT_FORKNUM;
	pending->backend = InvalidBackendId;
	pending->atCommit = true;
	pending->nestLevel = GetCurrentTransactionNestLevel();
	pending->next = pendingCleanups;
	pendingCleanups = pending;

	/* revert buffer-persistence changes at abort */
	pending = (PendingCleanup *)
		MemoryContextAlloc(TopMemoryContext, sizeof(PendingCleanup));
	pending->relnode = rnode;
	pending->op = PCOP_SET_PERSISTENCE;
	pending->bufpersistence = false;
	pending->backend = InvalidBackendId;
	pending->atCommit = false;
	pending->nestLevel = GetCurrentTransactionNestLevel();
	pending->next = pendingCleanups;
	pendingCleanups = pending;
}

/*
 * Perform XLogInsert of an XLOG_SMGR_CREATE record to WAL.
 */
void
log_smgrcreate(const RelFileNode *rnode, ForkNumber forkNum)
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
 * Perform XLogInsert of an XLOG_SMGR_UNLINK record to WAL.
 */
void
log_smgrunlink(const RelFileNode *rnode, ForkNumber forkNum)
{
	xl_smgr_unlink xlrec;

	/*
	 * Make an XLOG entry reporting the file unlink.
	 */
	xlrec.rnode = *rnode;
	xlrec.forkNum = forkNum;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xlrec));
	XLogInsert(RM_SMGR_ID, XLOG_SMGR_UNLINK | XLR_SPECIAL_REL_UPDATE);
}

/*
 * Perform XLogInsert of an XLOG_SMGR_CREATEMARK record to WAL.
 */
void
log_smgrcreatemark(const RelFileNode *rnode, ForkNumber forkNum,
				   StorageMarks mark)
{
	xl_smgr_mark xlrec;

	/*
	 * Make an XLOG entry reporting the file creation.
	 */
	xlrec.rnode = *rnode;
	xlrec.forkNum = forkNum;
	xlrec.mark = mark;
	xlrec.action = XLOG_SMGR_MARK_CREATE;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xlrec));
	XLogInsert(RM_SMGR_ID, XLOG_SMGR_MARK | XLR_SPECIAL_REL_UPDATE);
}

/*
 * Perform XLogInsert of an XLOG_SMGR_UNLINKMARK record to WAL.
 */
void
log_smgrunlinkmark(const RelFileNode *rnode, ForkNumber forkNum,
				   StorageMarks mark)
{
	xl_smgr_mark xlrec;

	/*
	 * Make an XLOG entry reporting the file creation.
	 */
	xlrec.rnode = *rnode;
	xlrec.forkNum = forkNum;
	xlrec.mark = mark;
	xlrec.action = XLOG_SMGR_MARK_UNLINK;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xlrec));
	XLogInsert(RM_SMGR_ID, XLOG_SMGR_MARK | XLR_SPECIAL_REL_UPDATE);
}

/*
 * Perform XLogInsert of an XLOG_SMGR_BUFPERSISTENCE record to WAL.
 */
void
log_smgrbufpersistence(const RelFileNode *rnode, bool persistence)
{
	xl_smgr_bufpersistence xlrec;

	/*
	 * Make an XLOG entry reporting the change of buffer persistence.
	 */
	xlrec.rnode = *rnode;
	xlrec.persistence = persistence;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xlrec));
	XLogInsert(RM_SMGR_ID, XLOG_SMGR_BUFPERSISTENCE | XLR_SPECIAL_REL_UPDATE);
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
	bool		need_fsm_vacuum = false;
	ForkNumber	forks[MAX_FORKNUM];
	BlockNumber blocks[MAX_FORKNUM];
	int			nforks = 0;
	SMgrRelation reln;

	/*
	 * Make sure smgr_targblock etc aren't pointing somewhere past new end.
	 * (Note: don't rely on this reln pointer below this loop.)
	 */
	reln = RelationGetSmgr(rel);
	reln->smgr_targblock = InvalidBlockNumber;
	for (int i = 0; i <= MAX_FORKNUM; ++i)
		reln->smgr_cached_nblocks[i] = InvalidBlockNumber;

	/* Prepare for truncation of MAIN fork of the relation */
	forks[nforks] = MAIN_FORKNUM;
	blocks[nforks] = nblocks;
	nforks++;

	/* Prepare for truncation of the FSM if it exists */
	fsm = smgrexists(RelationGetSmgr(rel), FSM_FORKNUM);
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
	vm = smgrexists(RelationGetSmgr(rel), VISIBILITYMAP_FORKNUM);
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
		xlrec.rnode = rel->rd_node;
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

	/* Do the real work to truncate relation forks */
	smgrtruncate(RelationGetSmgr(rel), forks, nforks, blocks);

	/*
	 * Update upper-level FSM pages to account for the truncation. This is
	 * important because the just-truncated pages were likely marked as
	 * all-free, and would be preferentially selected.
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
						  &(RelationGetSmgr(rel)->smgr_rnode.node),
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
 * If we invoked anything else, a relcache flush could cause our SMgrRelation
 * argument to become a dangling pointer.
 */
void
RelationCopyStorage(SMgrRelation src, SMgrRelation dst,
					ForkNumber forkNum, char relpersistence)
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
		forkNum == INIT_FORKNUM;

	/*
	 * We need to log the copied data in WAL iff WAL archiving/streaming is
	 * enabled AND it's a permanent relation.  This gives the same answer as
	 * "RelationNeedsWAL(rel) || copying_initfork", because we know the
	 * current operation created a new relfilenode.
	 */
	use_wal = XLogIsNeeded() &&
		(relpersistence == RELPERSISTENCE_PERMANENT || copying_initfork);

	nblocks = smgrnblocks(src, forkNum);

	for (blkno = 0; blkno < nblocks; blkno++)
	{
		/* If we got a cancel signal during the copy of the data, quit */
		CHECK_FOR_INTERRUPTS();

		smgrread(src, forkNum, blkno, buf.data);

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
			char	   *relpath = relpathbackend(src->smgr_rnode.node,
												 src->smgr_rnode.backend,
												 forkNum);

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
			log_newpage(&dst->smgr_rnode.node, forkNum, blkno, page, false);

		PageSetChecksumInplace(page, blkno);

		/*
		 * Now write the page.  We say skipFsync = true because there's no
		 * need for smgr to schedule an fsync for this write; we'll do it
		 * ourselves below.
		 */
		smgrextend(dst, forkNum, blkno, buf.data, true);
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
		smgrimmedsync(dst, forkNum);
}

/*
 * RelFileNodeSkippingWAL
 *		Check if a BM_PERMANENT relfilenode is using WAL.
 *
 * Changes of certain relfilenodes must not write WAL; see "Skipping WAL for
 * New RelFileNode" in src/backend/access/transam/README.  Though it is known
 * from Relation efficiently, this function is intended for the code paths not
 * having access to Relation.
 */
bool
RelFileNodeSkippingWAL(RelFileNode rnode)
{
	if (!pendingSyncHash ||
		hash_search(pendingSyncHash, &rnode, HASH_FIND, NULL) == NULL)
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
	return mul_size(1 + entries, sizeof(RelFileNode));
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
	RelFileNode *src;
	RelFileNode *dest = (RelFileNode *) startAddress;

	if (!pendingSyncHash)
		goto terminate;

	/* Create temporary hash to collect active relfilenodes */
	ctl.keysize = sizeof(RelFileNode);
	ctl.entrysize = sizeof(RelFileNode);
	ctl.hcxt = CurrentMemoryContext;
	tmphash = hash_create("tmp relfilenodes",
						  hash_get_num_entries(pendingSyncHash), &ctl,
						  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* collect all rnodes from pending syncs */
	hash_seq_init(&scan, pendingSyncHash);
	while ((sync = (PendingRelSync *) hash_seq_search(&scan)))
		(void) hash_search(tmphash, &sync->rnode, HASH_ENTER, NULL);

	/* remove deleted rnodes */
	for (delete = pendingDeletes; delete != NULL; delete = delete->next)
		if (delete->atCommit)
			(void) hash_search(tmphash, (void *) &delete->relnode,
							   HASH_REMOVE, NULL);

	hash_seq_init(&scan, tmphash);
	while ((src = (RelFileNode *) hash_seq_search(&scan)))
		*dest++ = *src;

	hash_destroy(tmphash);

terminate:
	MemSet(dest, 0, sizeof(RelFileNode));
}

/*
 * RestorePendingSyncs
 *		Restore syncs within a parallel worker.
 *
 * RelationNeedsWAL() and RelFileNodeSkippingWAL() must offer the correct
 * answer to parallel workers.  Only smgrDoPendingSyncs() reads the
 * is_truncated field, at end of transaction.  Hence, don't restore it.
 */
void
RestorePendingSyncs(char *startAddress)
{
	RelFileNode *rnode;

	Assert(pendingSyncHash == NULL);
	for (rnode = (RelFileNode *) startAddress; rnode->relNode != 0; rnode++)
		AddPendingSync(rnode);
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

		for (int i = 0; i < nrels; i++)
			smgrclose(srels[i]);

		pfree(srels);
	}
}

/*
 *	smgrDoPendingUnmark() -- Clean up work that emits WAL records
 *
 *  The operations handled in the function emits WAL records, which must be
 *  emitted before the commit record for the current transaction.
 */
void
smgrDoPendingCleanups(bool isCommit)
{
	int			nestLevel = GetCurrentTransactionNestLevel();
	PendingCleanup *pending;
	PendingCleanup *prev;
	PendingCleanup *next;

	prev = NULL;
	for (pending = pendingCleanups; pending != NULL; pending = next)
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
				pendingCleanups = next;

			/* do cleanup if called for */
			if (pending->atCommit == isCommit)
			{
				SMgrRelation srel;

				srel = smgropen(pending->relnode, pending->backend);

				Assert ((pending->op &
						 ~(PCOP_UNLINK_FORK | PCOP_UNLINK_MARK |
						   PCOP_SET_PERSISTENCE)) == 0);

				if (pending->op & PCOP_UNLINK_FORK)
				{
					/* other forks needs to drop buffers */
					Assert(pending->unlink_forknum == INIT_FORKNUM);

					/* Don't emit wal while recovery. */
					if (!InRecovery)
						log_smgrunlink(&pending->relnode,
									   pending->unlink_forknum);
					smgrunlink(srel, pending->unlink_forknum, false);
				}

				if (pending->op & PCOP_UNLINK_MARK)
				{
					SMgrRelation srel;

					if (!InRecovery)
						log_smgrunlinkmark(&pending->relnode,
										   pending->unlink_forknum,
										   pending->unlink_mark);
					srel = smgropen(pending->relnode, pending->backend);
					smgrunlinkmark(srel, pending->unlink_forknum,
								   pending->unlink_mark, InRecovery);
					smgrclose(srel);
				}

				if (pending->op & PCOP_SET_PERSISTENCE)
				{
					SetRelationBuffersPersistence(srel, pending->bufpersistence,
												  InRecovery);
				}
			}

			/* must explicitly free the list entry */
			pfree(pending);
			/* prev does not change */
		}
	}
}

/*
 *	smgrDoPendingSyncs() -- Take care of relation syncs at end of xact.
 */
void
smgrDoPendingSyncs(bool isCommit, bool isParallelWorker)
{
	PendingRelDelete *pending;
	int			nrels = 0,
				maxrels = 0;
	SMgrRelation *srels = NULL;
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
			(void) hash_search(pendingSyncHash, (void *) &pending->relnode,
							   HASH_REMOVE, NULL);

	hash_seq_init(&scan, pendingSyncHash);
	while ((pendingsync = (PendingRelSync *) hash_seq_search(&scan)))
	{
		ForkNumber	fork;
		BlockNumber nblocks[MAX_FORKNUM + 1];
		BlockNumber total_blocks = 0;
		SMgrRelation srel;

		srel = smgropen(pendingsync->rnode, InvalidBackendId);

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
				if (smgrexists(srel, fork))
				{
					BlockNumber n = smgrnblocks(srel, fork);

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
				rel = CreateFakeRelcacheEntry(srel->smgr_rnode.node);
				log_newpage_range(rel, fork, 0, n, false);
				FreeFakeRelcacheEntry(rel);
			}
		}
	}

	pendingSyncHash = NULL;

	if (nrels > 0)
	{
		smgrdosyncall(srels, nrels);
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
	else if (info == XLOG_SMGR_UNLINK)
	{
		xl_smgr_unlink *xlrec = (xl_smgr_unlink *) XLogRecGetData(record);
		SMgrRelation reln;

		reln = smgropen(xlrec->rnode, InvalidBackendId);
		smgrunlink(reln, xlrec->forkNum, true);
		smgrclose(reln);
	}
	else if (info == XLOG_SMGR_TRUNCATE)
	{
		xl_smgr_truncate *xlrec = (xl_smgr_truncate *) XLogRecGetData(record);
		SMgrRelation reln;
		Relation	rel;
		ForkNumber	forks[MAX_FORKNUM];
		BlockNumber blocks[MAX_FORKNUM];
		int			nforks = 0;
		bool		need_fsm_vacuum = false;

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

		/* Prepare for truncation of MAIN fork */
		if ((xlrec->flags & SMGR_TRUNCATE_HEAP) != 0)
		{
			forks[nforks] = MAIN_FORKNUM;
			blocks[nforks] = xlrec->blkno;
			nforks++;

			/* Also tell xlogutils.c about it */
			XLogTruncateRelation(xlrec->rnode, MAIN_FORKNUM, xlrec->blkno);
		}

		/* Prepare for truncation of FSM and VM too */
		rel = CreateFakeRelcacheEntry(xlrec->rnode);

		if ((xlrec->flags & SMGR_TRUNCATE_FSM) != 0 &&
			smgrexists(reln, FSM_FORKNUM))
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
			smgrexists(reln, VISIBILITYMAP_FORKNUM))
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
			smgrtruncate(reln, forks, nforks, blocks);

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
	else if (info == XLOG_SMGR_MARK)
	{
		xl_smgr_mark *xlrec = (xl_smgr_mark *) XLogRecGetData(record);
		SMgrRelation reln;
		PendingCleanup *pending;
		bool		created = false;

		reln = smgropen(xlrec->rnode, InvalidBackendId);

		switch (xlrec->action)
		{
			case XLOG_SMGR_MARK_CREATE:
				smgrcreatemark(reln, xlrec->forkNum, xlrec->mark, true);
				created = true;
				break;
			case XLOG_SMGR_MARK_UNLINK:
				smgrunlinkmark(reln, xlrec->forkNum, xlrec->mark, true);
				break;
			default:
				elog(ERROR, "unknown smgr_mark action \"%c\"", xlrec->mark);
		}

		if (created)
		{
			/* revert mark file operation at abort */
			pending = (PendingCleanup *)
				MemoryContextAlloc(TopMemoryContext, sizeof(PendingCleanup));
			pending->relnode = xlrec->rnode;
			pending->op = PCOP_UNLINK_MARK;
			pending->unlink_forknum = xlrec->forkNum;
			pending->unlink_mark = xlrec->mark;
			pending->backend = InvalidBackendId;
			pending->atCommit = false;
			pending->nestLevel = GetCurrentTransactionNestLevel();
			pending->next = pendingCleanups;
			pendingCleanups = pending;
		}
		else
		{
			/*
			 * Delete pending action for this mark file if any. We should have
			 * at most one entry for this action.
			 */
			PendingCleanup *prev = NULL;

			for (pending = pendingCleanups; pending != NULL;
				 pending = pending->next)
			{
				if (RelFileNodeEquals(xlrec->rnode, pending->relnode) &&
					pending->unlink_forknum == xlrec->forkNum &&
					(pending->op & PCOP_UNLINK_MARK) != 0)
				{
					if (prev)
						prev->next = pending->next;
					else
						pendingCleanups = pending->next;

					pfree(pending);
					break;
				}

				prev = pending;
			}
		}
	}
	else if (info == XLOG_SMGR_BUFPERSISTENCE)
	{
		xl_smgr_bufpersistence *xlrec =
			(xl_smgr_bufpersistence *) XLogRecGetData(record);
		SMgrRelation reln;
		PendingCleanup *pending;
		PendingCleanup *prev = NULL;

		reln = smgropen(xlrec->rnode, InvalidBackendId);
		SetRelationBuffersPersistence(reln, xlrec->persistence, true);

		/*
		 * Delete pending action for persistence change if any. We should have
		 * at most one entry for this action.
		 */
		for (pending = pendingCleanups; pending != NULL;
			 pending = pending->next)
		{
			if (RelFileNodeEquals(xlrec->rnode, pending->relnode) &&
				(pending->op & PCOP_SET_PERSISTENCE) != 0)
			{
				Assert (pending->bufpersistence == xlrec->persistence);

				if (prev)
					prev->next = pending->next;
				else
					pendingCleanups = pending->next;

				pfree(pending);
				break;
			}

			prev = pending;
		}

		/*
		 * Revert buffer-persistence changes at abort if the relation is going
		 * to different persistence from before this transaction.
		 */
		if (!pending)
		{
			pending = (PendingCleanup *)
				MemoryContextAlloc(TopMemoryContext, sizeof(PendingCleanup));
			pending->relnode = xlrec->rnode;
			pending->op = PCOP_SET_PERSISTENCE;
			pending->bufpersistence = !xlrec->persistence;
			pending->backend = InvalidBackendId;
			pending->atCommit = false;
			pending->nestLevel = GetCurrentTransactionNestLevel();
			pending->next = pendingCleanups;
			pendingCleanups = pending;
		}
	}
	else
		elog(PANIC, "smgr_redo: unknown op code %u", info);
}
