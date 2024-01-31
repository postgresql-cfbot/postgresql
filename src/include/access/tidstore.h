/*-------------------------------------------------------------------------
 *
 * tidstore.h
 *	  Tid storage.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/tidstore.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TIDSTORE_H
#define TIDSTORE_H

#include "storage/itemptr.h"
#include "utils/dsa.h"

typedef dsa_pointer	TidStoreHandle;

typedef struct TidStore TidStore;
typedef struct TidStoreIter TidStoreIter;

/* Result struct for TidStoreIterateNext */
typedef struct TidStoreIterResult
{
	BlockNumber		blkno;
	int				max_offset;
	int				num_offsets;
	OffsetNumber	*offsets;
} TidStoreIterResult;

extern TidStore *TidStoreCreate(size_t max_bytes, dsa_area *dsa);
extern TidStore *TidStoreAttach(dsa_area *dsa, dsa_pointer rt_dp);
extern void TidStoreDetach(TidStore *ts);
extern void TidStoreLockExclusive(TidStore *ts);
extern void TidStoreLockShare(TidStore *ts);
extern void TidStoreUnlock(TidStore *ts);
extern void TidStoreDestroy(TidStore *ts);
extern void TidStoreSetBlockOffsets(TidStore *ts, BlockNumber blkno, OffsetNumber *offsets,
									int num_offsets);
extern bool TidStoreIsMember(TidStore *ts, ItemPointer tid);
extern TidStoreIter * TidStoreBeginIterate(TidStore *ts);
extern TidStoreIterResult *TidStoreIterateNext(TidStoreIter *iter);
extern void TidStoreEndIterate(TidStoreIter *iter);
extern size_t TidStoreMemoryUsage(TidStore *ts);
extern TidStoreHandle TidStoreGetHandle(TidStore *ts);

#endif		/* TIDSTORE_H */
