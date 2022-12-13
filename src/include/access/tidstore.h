/*-------------------------------------------------------------------------
 *
 * tidstore.h
 *	  TID storage.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/tidstore.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TIDSTORE_H
#define TIDSTORE_H

#include "lib/radixtree.h"
#include "storage/itemptr.h"

typedef dsa_pointer tidstore_handle;

typedef struct TIDStore TIDStore;

typedef struct TIDStoreIter
{
	TIDStore	*ts;

	rt_iter		*tree_iter;

	bool		finished;

	uint64		next_key;
	uint64		next_val;

	BlockNumber		blkno;
	OffsetNumber	offsets[MaxOffsetNumber]; /* XXX: usually don't use up */
	int				num_offsets;

#ifdef USE_ASSERT_CHECKING
	uint64		itemptrs_index;
	int	prev_index;
#endif
} TIDStoreIter;

extern TIDStore *tidstore_create(dsa_area *dsa);
extern TIDStore *tidstore_attach(dsa_area *dsa, dsa_pointer handle);
extern void tidstore_detach(TIDStore *ts);
extern void tidstore_free(TIDStore *ts);
extern void tidstore_reset(TIDStore *ts);
extern void tidstore_add_tids(TIDStore *ts, BlockNumber blkno, OffsetNumber *offsets,
							  int num_offsets);
extern bool tidstore_lookup_tid(TIDStore *ts, ItemPointer tid);
extern TIDStoreIter * tidstore_begin_iterate(TIDStore *ts);
extern bool tidstore_iterate_next(TIDStoreIter *iter);
extern uint64 tidstore_num_tids(TIDStore *ts);
extern uint64 tidstore_memory_usage(TIDStore *ts);
extern tidstore_handle tidstore_get_handle(TIDStore *ts);

#endif		/* TIDSTORE_H */

