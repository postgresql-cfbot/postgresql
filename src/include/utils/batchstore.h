#ifndef BATCH_STORE_H
#define BATCH_STORE_H

#include "access/htup.h"
#include "storage/dsm.h"
#include "storage/sharedfileset.h"

typedef struct BatchStoreData* BatchStore;
typedef struct BatchStoreParallelHashData* BatchStoreParallelHash;

typedef struct BatchStoreFuncs
{
	void (*hash_write)(BatchStore bs, MinimalTuple mtup, uint32 hash);
	MinimalTuple (*hash_read)(BatchStore bs, uint32 *hash);
}BatchStoreFuncs;

#define bs_write_hash(bs, mtup, hash) (*((BatchStoreFuncs*)bs)->hash_write)(bs, mtup, hash)
#define bs_read_hash(bs, phash) (*((BatchStoreFuncs*)bs)->hash_read)(bs, phash)

extern BatchStore bs_begin_hash(uint32 num_batches);

extern size_t bs_parallel_hash_estimate(uint32 num_batches, uint32 nparticipants);
extern BatchStore bs_init_parallel_hash(uint32 num_batches,
										uint32 nparticipants, uint32 my_participant_num,
										BatchStoreParallelHash bsph, dsm_segment *dsm_seg,
										SharedFileSet *fileset, const char *name);
extern BatchStore bs_attach_parallel_hash(BatchStoreParallelHash bsph, dsm_segment *dsm_seg,
										  SharedFileSet *fileset, uint32 my_participant_num);

extern void bs_destory(BatchStore bs);
extern void bs_clear(BatchStore bs);

extern void bs_end_write(BatchStore bs);

extern bool bs_next_batch(BatchStore bs, bool no_parallel);
extern void bs_rescan(BatchStore bs);
extern void bs_end_cur_batch(BatchStore bs);
#endif /* BATCH_STORE_H */