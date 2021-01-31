#include "postgres.h"

#include "access/htup_details.h"
#include "access/parallel.h"
#include "commands/tablespace.h"
#include "executor/nodeHash.h"
#include "port/atomics.h"
#include "storage/buffile.h"
#include "utils/batchstore.h"
#include "utils/memutils.h"
#include "utils/sharedtuplestore.h"

#define InvalidBatch UINT32_MAX

typedef enum BatchMethod
{
	BSM_HASH = 1,
	BSM_PARALLEL_HASH
}BatchMethod;

typedef struct BatchStoreParallelHashData
{
	pg_atomic_uint32	cur_batches;
	uint32				num_batches;
	uint32				num_participants;
}BatchStoreParallelHashData;

typedef struct BatchStoreData
{
	BatchStoreFuncs	func;
	BatchMethod	method;
	uint32		num_batches;
	void	   *cur_batch_ptr;
	uint32		cur_batch_num;
	union
	{
		/* for hash */
		struct
		{
			StringInfoData	hash_read_buf;
		};
		/* for parallel hash */
		struct
		{
			dsm_segment		   *dsm_seg;
			MemoryContext		accessor_mcontext;
			/* in shared memory, next idle parallel hash batch number */
			pg_atomic_uint32   *shm_ph_batch_num;
		};
	};
	void	 *all_batches[FLEXIBLE_ARRAY_MEMBER];
}BatchStoreData;

static void bs_write_normal_hash(BatchStore bs, MinimalTuple mtup, uint32 hash);
static MinimalTuple bs_read_normal_hash(BatchStore bs, uint32 *hash);

static void bs_write_parallel_hash(BatchStore bs, MinimalTuple mtup, uint32 hash);
static void bs_write_parallel_one_batch_hash(BatchStore bs, MinimalTuple mtup, uint32 hash);
static MinimalTuple bs_read_parallel_hash(BatchStore bs, uint32 *hash);

/*
 * make an empty batch store
 */
static inline BatchStore
make_empty_batch_store(uint32 num_batches, BatchMethod method)
{
	BatchStore bs = palloc0(offsetof(BatchStoreData, all_batches) +
								sizeof(void*) * num_batches);

	bs->method = method;
	bs->num_batches = num_batches;
	bs->cur_batch_num = InvalidBatch;

	return bs;
}

/*
 * Create a normal batch store
 */
BatchStore
bs_begin_hash(uint32 num_batches)
{
	BatchStore bs = make_empty_batch_store(num_batches, BSM_HASH);

	/* Initialize hash read buffer for MinimalTuple */
	initStringInfo(&bs->hash_read_buf);
	enlargeStringInfo(&bs->hash_read_buf, MINIMAL_TUPLE_DATA_OFFSET);
	MemSet(bs->hash_read_buf.data, 0, MINIMAL_TUPLE_DATA_OFFSET);

	PrepareTempTablespaces();

	bs->func.hash_write = bs_write_normal_hash;
	bs->func.hash_read = bs_read_normal_hash;
	return bs;
}

size_t
bs_parallel_hash_estimate(uint32 num_batches, uint32 nparticipants)
{
	return MAXALIGN(sizeof(struct BatchStoreParallelHashData)) +
				MAXALIGN(sts_estimate(nparticipants)) * num_batches;
}

/*
 * Create or attach shared batch store
 */
static BatchStore
bs_begin_parallel_hash(BatchStoreParallelHash bsph,
					   uint32 my_participant_num, bool init,
					   SharedFileSet *fileset, const char *name,
					   dsm_segment *dsm_seg)
{
	uint32			i;
	MemoryContext	oldcontext;
	char		   *addr;
	char			buffer[24];
	Size			sts_size = MAXALIGN(sts_estimate(bsph->num_participants));
	BatchStore		bs = make_empty_batch_store(bsph->num_batches, BSM_PARALLEL_HASH);

	bs->shm_ph_batch_num = &bsph->cur_batches;

	bs->accessor_mcontext = AllocSetContextCreate(CurrentMemoryContext,
												  "batch parallel hash",
												  ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(bs->accessor_mcontext);
	addr = ((char*)bsph) + MAXALIGN(sizeof(*bsph));
	for (i=bsph->num_batches;i>0;)
	{
		--i;
		if (init)
		{
			sprintf(buffer, "%s_%u", name, i);
			bs->all_batches[i] = sts_initialize((SharedTuplestore*)addr,
												bsph->num_participants,
												my_participant_num,
												sizeof(uint32),
												0,
												fileset,
												buffer);
		}else
		{
			bs->all_batches[i] = sts_attach((SharedTuplestore*)addr,
											my_participant_num,
											fileset);
		}
		addr += sts_size;
	}
	MemoryContextSwitchTo(oldcontext);

	bs->dsm_seg = dsm_seg;
	bs->func.hash_read = bs_read_parallel_hash;
	if (bs->num_batches == 1)
		bs->func.hash_write = bs_write_parallel_one_batch_hash;
	else
		bs->func.hash_write = bs_write_parallel_hash;

	return bs;
}

/*
 * Create a parallel batch store
 */
BatchStore
bs_init_parallel_hash(uint32 num_batches,
					  uint32 nparticipants, uint32 my_participant_num,
					  BatchStoreParallelHash bsph, dsm_segment *dsm_seg,
					  SharedFileSet *fileset, const char *name)
{
	Assert(name != NULL && fileset != NULL);
	bsph->num_batches = num_batches;
	bsph->num_participants = nparticipants;
	pg_atomic_init_u32(&bsph->cur_batches, InvalidBatch);

	return bs_begin_parallel_hash(bsph, my_participant_num, true, fileset, name, dsm_seg);
}

/*
 * Attach from parallel batch store
 */
BatchStore
bs_attach_parallel_hash(BatchStoreParallelHash bsph, dsm_segment *dsm_seg,
						SharedFileSet *fileset, uint32 my_participant_num)
{
	return bs_begin_parallel_hash(bsph, my_participant_num, false, fileset, NULL, dsm_seg);
}

/* Destory batch store */
void
bs_destory(BatchStore bs)
{
	uint32	i;
	if (bs == NULL)
		return;

	switch(bs->method)
	{
	case BSM_HASH:
		for(i=0;i<bs->num_batches;++i)
		{
			if (bs->all_batches[i])
				BufFileClose(bs->all_batches[i]);
		}
		pfree(bs->hash_read_buf.data);
		break;
	case BSM_PARALLEL_HASH:
		{
			BatchStoreParallelHash bsph = (BatchStoreParallelHash)(((char*)bs->shm_ph_batch_num) -
											offsetof(BatchStoreParallelHashData, cur_batches));
			uint32 count = bsph->num_batches;
			while (count > 0)
				sts_detach(bs->all_batches[--count]);
			MemoryContextDelete(bs->accessor_mcontext);
		}
		break;
	default:
		ereport(ERROR,
				(errmsg("unknown batch store method %u", bs->method)));
		break;
	}

	pfree(bs);
}

/*
 * Write MinimalTuple and hash value to normal batch store
 */
static void
bs_write_normal_hash(BatchStore bs, MinimalTuple mtup, uint32 hash)
{
	uint32 batch = hash % bs->num_batches;
	uint32 data_len = mtup->t_len - MINIMAL_TUPLE_DATA_OFFSET;
	BufFile *buffile = bs->all_batches[batch];

	if (unlikely(buffile == NULL))
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(bs));
		buffile = BufFileCreateTemp(false);
		bs->all_batches[batch] = buffile;
		MemoryContextSwitchTo(oldcontext);
	}

	BufFileWrite(buffile, &hash, sizeof(hash));
	BufFileWrite(buffile, &mtup->t_len, sizeof(mtup->t_len));
	BufFileWrite(buffile, ((char*)mtup) + MINIMAL_TUPLE_DATA_OFFSET, data_len);
}

/*
 * Read MinimalTuple and hash value from normal batch store
 */
static MinimalTuple
bs_read_normal_hash(BatchStore bs, uint32 *hash)
{
	MinimalTuple	mtup;
	size_t			nread;
	uint32			head[2];
	uint32			data_len;

	/* Read hash value and tuple length */
	nread = BufFileRead(bs->cur_batch_ptr, head, sizeof(head));
	if (nread == 0)
		return NULL;

	if (nread != sizeof(head))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from batch store temporary file: %m")));
	*hash = head[0];

	/* Enlarge buffer and read tuple data */
	enlargeStringInfo(&bs->hash_read_buf, head[1]);
	mtup = (MinimalTuple)bs->hash_read_buf.data;
	mtup->t_len = head[1];
	data_len = head[1] - MINIMAL_TUPLE_DATA_OFFSET;
	if (BufFileRead(bs->cur_batch_ptr, ((char*)mtup) + MINIMAL_TUPLE_DATA_OFFSET, data_len) != data_len)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from batch store temporary file: %m")));

	return mtup;
}

/* end write batch store, ready for read */
void
bs_end_write(BatchStore bs)
{
	uint32 i;
	switch(bs->method)
	{
	case BSM_HASH:
		/* nothing to do */
		break;
	case BSM_PARALLEL_HASH:
		for (i=bs->num_batches;i>0;)
			sts_end_write(bs->all_batches[--i]);
		bs->cur_batch_ptr = NULL;
		break;
	default:
		ereport(ERROR,
				(errmsg("unknown batch store method %u", bs->method)));
		break;
	}
}

/*
 * Write MinimalTuple and hash value to parallel batch store
 */
static void
bs_write_parallel_hash(BatchStore bs, MinimalTuple mtup, uint32 hash)
{
	sts_puttuple(bs->all_batches[hash%bs->num_batches],
				 &hash,
				 mtup);
}

/*
 * Write MinimalTuple and hash value to parallel batch store,
 * only for only have one batch
 */
static void
bs_write_parallel_one_batch_hash(BatchStore bs, MinimalTuple mtup, uint32 hash)
{
	Assert(bs->num_batches == 1);
	sts_puttuple(bs->all_batches[0],
				 &hash,
				 mtup);
}

/*
 * Read MinimalTuple and hash value from parallel batch store
 */
static MinimalTuple
bs_read_parallel_hash(BatchStore bs, uint32 *hash)
{
	return sts_scan_next(bs->cur_batch_ptr, hash);
}

/*
 * Get next batch from batch store, return false if no more.
 */
bool
bs_next_batch(BatchStore bs, bool no_parallel)
{
	uint32 batch;
	switch(bs->method)
	{
	case BSM_HASH:

		batch = bs->cur_batch_num;
		++batch;

		for (;batch < bs->num_batches;++batch)
		{
			if (bs->all_batches[batch])
			{
				bs->cur_batch_ptr = bs->all_batches[batch];
				bs->cur_batch_num = batch;
				if (BufFileSeek(bs->cur_batch_ptr, 0, 0, SEEK_SET) != 0)
				{
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("can not seek batch store file to head")));
				}
				return true;
			}
		}
		break;
	case BSM_PARALLEL_HASH:
		if (no_parallel)
		{
			batch = bs->cur_batch_num;
			++batch;
		}else
		{
			batch = pg_atomic_add_fetch_u32(bs->shm_ph_batch_num, 1);
		}

		if (batch < bs->num_batches)
		{
			bs->cur_batch_num = batch;
			bs->cur_batch_ptr = bs->all_batches[batch];
			sts_begin_scan(bs->cur_batch_ptr);
			return true;
		}
		break;
	default:
		ereport(ERROR,
				(errmsg("unknown batch store method %u", bs->method)));
		break;
	}

	return false;
}

void
bs_rescan(BatchStore bs)
{
	switch(bs->method)
	{
	case BSM_HASH:
		break;
	case BSM_PARALLEL_HASH:
		for (uint32 i=bs->num_batches;i>0;)
			sts_reinitialize(bs->all_batches[--i]);
		pg_atomic_write_u32(bs->shm_ph_batch_num, InvalidBatch);
		break;
	default:
		ereport(ERROR,
				(errmsg("unknown batch store method %u", bs->method)));
		break;
	}
	bs->cur_batch_ptr = NULL;
	bs->cur_batch_num = InvalidBatch;
}

/*
 * Delete all the contents of a batch store
 */
void
bs_clear(BatchStore bs)
{
	uint32	i;
	switch (bs->method)
	{
	case BSM_HASH:
		for (i=bs->num_batches;i>0;)
		{
			--i;
			if (bs->all_batches[i])
			{
				BufFileClose(bs->all_batches[i]);
				bs->all_batches[i] = 0;
			}
		}
		break;
	case BSM_PARALLEL_HASH:
		pg_atomic_write_u32(bs->shm_ph_batch_num, InvalidBatch);
		for (i=bs->num_batches;i>0;)
			sts_clear(bs->all_batches[--i]);
		break;
	default:
		ereport(ERROR,
				(errmsg("unknown batch store method %u", bs->method)));
		break;
	}
	bs->cur_batch_ptr = NULL;
	bs->cur_batch_num = InvalidBatch;
}

void
bs_end_cur_batch(BatchStore bs)
{
	switch(bs->method)
	{
	case BSM_HASH:
		bs->cur_batch_ptr = NULL;
		break;
	case BSM_PARALLEL_HASH:
		sts_end_scan(bs->cur_batch_ptr);
		bs->cur_batch_ptr = NULL;
		break;
	default:
		ereport(ERROR,
				(errmsg("unknown batch store method %u", bs->method)));
		break;
	}
}
