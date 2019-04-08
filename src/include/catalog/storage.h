/*-------------------------------------------------------------------------
 *
 * storage.h
 *	  prototypes for functions in backend/catalog/storage.c
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/storage.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STORAGE_H
#define STORAGE_H

#include "storage/block.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "utils/relcache.h"

extern void RelationCreateStorage(RelFileNode rnode, char relpersistence);
extern void RelationDropStorage(Relation rel);
extern void RelationPreserveStorage(RelFileNode rnode, bool atCommit);
extern void RelationTruncate(Relation rel, BlockNumber nblocks);
extern void RelationCopyStorage(Relation srcrel, SMgrRelation dst,
								ForkNumber forkNum);
extern bool BufferNeedsWAL(Relation rel, Buffer buf);
extern bool BlockNeedsWAL(Relation rel, BlockNumber blkno);
extern void RecordWALSkipping(Relation rel);
extern void RecordPendingSync(Relation rel, SMgrRelation srel,
							  ForkNumber forknum);
extern void RelationInvalidateWALSkip(Relation rel);

/*
 * These functions used to be in storage/smgr/smgr.c, which explains the
 * naming
 */
extern void smgrDoPendingDeletes(bool isCommit);
extern int	smgrGetPendingDeletes(bool forCommit, RelFileNode **ptr);
extern void smgrFinishBulkInsert(bool isCommit);
extern void AtSubCommit_smgr(SubTransactionId mySubid,
							 SubTransactionId parentSubid);
extern void AtSubAbort_smgr(SubTransactionId mySubid,
							 SubTransactionId parentSubid);
extern void PostPrepare_smgr(void);

#endif							/* STORAGE_H */
