/*-------------------------------------------------------------------------
 *
 * ts_shared.h
 *	  tsearch shared memory management
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 *
 * src/include/tsearch/ts_shared.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TS_SHARED_H
#define TS_SHARED_H

#include "tsearch/ts_public.h"

typedef void *(*ts_dict_build_callback) (List *dictoptions, Size *size);

extern void *ts_dict_shmem_location(DictInitData *init_data,
									ts_dict_build_callback allocate_cb);
extern void ts_dict_shmem_release(Oid id, TransactionId xmin,
								  TransactionId xmax, ItemPointerData tid,
								  bool unpin_segment);

extern void TsearchShmemInit(void);
extern Size TsearchShmemSize(void);

#endif							/* TS_SHARED_H */
