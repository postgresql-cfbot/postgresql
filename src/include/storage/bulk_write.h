/*-------------------------------------------------------------------------
 *
 * bulk_write.h
 *	  Efficiently and reliably populate a new relation
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/bulk_write.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BULK_WRITE_H
#define BULK_WRITE_H

typedef struct BulkWriteState BulkWriteState;

/*
 * Temporary buffer to hold a page to until it's written out. Use
 * bulkw_get_buf() to reserve one of these.  This is a separate typedef to
 * make distinguish from other block-sized buffers passed around in the
 * system.
 */
typedef PGIOAlignedBlock *BulkWriteBuffer;

/* forward declared from smgr.h */
struct SMgrRelationData;

extern BulkWriteState *bulkw_start_rel(Relation rel, ForkNumber forknum);
extern BulkWriteState *bulkw_start_smgr(struct SMgrRelationData *smgr, ForkNumber forknum, bool use_wal);

extern BulkWriteBuffer bulkw_get_buf(BulkWriteState *bulkw);
extern void bulkw_write(BulkWriteState *bulkw, BlockNumber blocknum, BulkWriteBuffer buf, bool page_std);

extern void bulkw_finish(BulkWriteState *bulkw);

#endif							/* BULK_WRITE_H */
