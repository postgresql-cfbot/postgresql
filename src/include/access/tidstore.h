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

typedef dsa_pointer tidstore_handle;

typedef struct TidStore TidStore;
typedef struct TidStoreIter TidStoreIter;

typedef struct TidStoreIterResult
{
	BlockNumber		blkno;
	OffsetNumber	*offsets;
	int				num_offsets;
} TidStoreIterResult;

extern TidStore *tidstore_create(size_t max_bytes, int max_offset, dsa_area *dsa);
extern TidStore *tidstore_attach(dsa_area *dsa, dsa_pointer handle);
extern void tidstore_detach(TidStore *ts);
extern void tidstore_destroy(TidStore *ts);
extern void tidstore_reset(TidStore *ts);
extern void tidstore_add_tids(TidStore *ts, BlockNumber blkno, OffsetNumber *offsets,
							  int num_offsets);
extern bool tidstore_lookup_tid(TidStore *ts, ItemPointer tid);
extern TidStoreIter * tidstore_begin_iterate(TidStore *ts);
extern TidStoreIterResult *tidstore_iterate_next(TidStoreIter *iter);
extern void tidstore_end_iterate(TidStoreIter *iter);
extern int64 tidstore_num_tids(TidStore *ts);
extern bool tidstore_is_full(TidStore *ts);
extern size_t tidstore_max_memory(TidStore *ts);
extern size_t tidstore_memory_usage(TidStore *ts);
extern tidstore_handle tidstore_get_handle(TidStore *ts);

#endif		/* TIDSTORE_H */
