/*-------------------------------------------------------------------------
 *
 * cached_buf.c
 *	  routines for mapping relations to the indexes of auxillary cached
 *	  buffers.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/cached_buf.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/bufmgr.h"
#include "storage/buf_internals.h"


/*
 * Each relation and its buffer information are cached in a hash table
 * located in shared memory. These cached buffers are chained together
 * in a doubly-linked list.
 */
#define NUM_MAP_PARTITIONS_FOR_REL	128	/* relation-level */
#define NUM_MAP_PARTITIONS_IN_REL	4	/* block-level */
#define NUM_MAP_PARTITIONS \
	(NUM_MAP_PARTITIONS_FOR_REL * NUM_MAP_PARTITIONS_IN_REL)
#define CACHED_BUF_END_OF_LIST	(-1) /* end of doubly linked list */

/* hash table key */
typedef struct CachedBufTag
{
	RelFileNode		rnode;		/* relation */
	int			modOfBlockNum;	/* modulo of BlockNumber*/
} CachedBufTag;

/* entry to the cached buffer hash table */
typedef struct CachedBufEnt
{
	CachedBufTag	key;	/* hash table key */
	int		head;	/* index of dlist */
} CachedBufEnt;

/* entry to the doubly-linked list */
typedef struct BufDlistEnt
{
	int		prev;
	int		next;
} BufDlistEnt;

/* Locks to be used for the hash table operations */
typedef struct CombinedLock
{
	volatile bool	flag;	/* flag for lookup operation */
	slock_t		spinLock;	/* spinlock to protect the flag */
	LWLock		lwLock;	/* LWLock used in lookup */
} CombinedLock;

/* lock for cached buffer hash table */
typedef struct CachedBufTableLock
{
	CombinedLock	cacheLock[NUM_MAP_PARTITIONS];
	int		cacheTrancheId;
} CachedBufTableLock;

static HTAB *CachedBufHash = NULL;
static BufDlistEnt *BufDlistEntArray = NULL;
static CachedBufTableLock *CachedBufLockArray = NULL;
static void CombinedLockInitialize(CombinedLock *lock, int trancheId);
static uint32 CachedBufTableHashCode(CachedBufTag *tag);
static CombinedLock *GetCachedBufPartitionLock(CachedBufTag *tag,
											   uint32 *hashcode);
static void CombinedLockAcquireSpinLock(CombinedLock *lock);
static void CombinedLockAcquireLWLock(CombinedLock *lock);
static void CombinedLockReleaseSpinLock(CombinedLock *lock);
static void CombinedLockReleaseLWLock(CombinedLock *lock);
static inline BufDlistEnt *cb_dlist_entry(int buf_id);
static inline BufDlistEnt *cb_dlist_next(BufDlistEnt *entry);
static inline BufDlistEnt *cb_dlist_prev(BufDlistEnt *entry);
static inline bool cb_dlist_is_empty(CachedBufEnt *hash_entry);
static inline void cb_dlist_push_head(CachedBufEnt *hash_entry, int buf_id);
static inline void cb_dlist_delete(CachedBufEnt *hash_entry, int buf_id);
static inline void cb_dlist_combine(CachedBufEnt *main, CachedBufEnt *temp);

/*
 * CachedBufShmemSize
 * 		Estimate space needed for mapping cached buffer hash table
 *
 * 		size of lookup table is the desired hash table size
 *		(possibly more than NBuffers)
 */
Size
CachedBufShmemSize(void)
{
	Size		size = 0;

	/* size of cached buffer lookup table */
	size = add_size(size, hash_estimate_size(NBuffers +
											 NUM_MAP_PARTITIONS,
											 sizeof(CachedBufEnt)));

	/* size of cached buffer dlist entry array */
	size = add_size(size, mul_size(NBuffers, sizeof(BufDlistEnt)));

	/* size of locks */
	size = add_size(size, mul_size(NBuffers,
								   sizeof(CachedBufTableLock)));

	return size;
}

/* Initialize spinlock and LWLock for cached buffer hash table */
void
CombinedLockInitialize(CombinedLock *lock, int trancheId)
{
	lock->flag = false;
	SpinLockInit(&lock->spinLock);
	LWLockInitialize(&lock->lwLock, trancheId);
}

/*
 * InitCachedBufTable
 *      Initialize the cached buffer hash table and related data
 *      structures at shared memory initialization.
 */
void
InitCachedBufTable(int size)
{
	HASHCTL		info;
	bool		foundList,
				foundLock;
	int		i;

	info.keysize = sizeof(CachedBufTag);
	info.entrysize = sizeof(CachedBufEnt);
	info.num_partitions = NUM_MAP_PARTITIONS;

	CachedBufHash = ShmemInitHash("Cached Buffer Lookup Table",
								  size + NUM_MAP_PARTITIONS,
								  size + NUM_MAP_PARTITIONS,
								  &info,
								  HASH_ELEM | HASH_BLOBS |
								  HASH_PARTITION);

	BufDlistEntArray = ShmemInitStruct("buffer dlist entry array",
									   size * sizeof(BufDlistEnt),
									   &foundList);

	CachedBufLockArray = (CachedBufTableLock *)
			ShmemInitStruct("cached buffer hash partition lock",
							size * sizeof(CachedBufTableLock),
							&foundLock);

	if (!foundList && !foundLock)
	{
		CachedBufLockArray->cacheTrancheId = LWLockNewTrancheId();

		for (i = 0; i < NUM_MAP_PARTITIONS; i++)
			CombinedLockInitialize(&CachedBufLockArray->cacheLock[i],
								   CachedBufLockArray->cacheTrancheId);
	}
	LWLockRegisterTranche(CachedBufLockArray->cacheTrancheId,
						  "cached_buf_tranche_id");

}

 /*
  * CachedBufTableHashCode
  *      Compute the hash code associated with tag
  *
  * This must be passed to the insert/lookup/delete routines along with the
  * tag. We do it like this because the callers need to know the hash code
  * to determine which partition to lock, and we don't want to do the  hash
  * computation twice (hash_any is a bit slow).
  */
static uint32
CachedBufTableHashCode(CachedBufTag *tag)
{
	return get_hash_value(CachedBufHash, (void *) tag);
}

/*
 * GetCachedBufPartitionLock
 * 		Get lock instance for partition of cached buffer lookup table
 */
static CombinedLock *
GetCachedBufPartitionLock(CachedBufTag *tag, uint32 *hashcode)
{
	*hashcode = CachedBufTableHashCode(tag);

	return &CachedBufLockArray->cacheLock[*hashcode % NUM_MAP_PARTITIONS];
}

/*
 * CombinedLockAcquireSpinLock
 *		Acquire spinlock to proceed to inserting/deleting hash
 *		table entries
 *
 * This function waits for a lookup process (if flag is true) to finish,
 * before reacquring a spinlock.
 */
void
CombinedLockAcquireSpinLock(CombinedLock *lock)
{
	SpinLockAcquire(&lock->spinLock);

	while (lock->flag)
	{
		SpinLockRelease(&lock->spinLock);
		LWLockAcquire(&lock->lwLock, LW_EXCLUSIVE);
		LWLockRelease(&lock->lwLock);
		SpinLockAcquire(&lock->spinLock);
	}
}

/*
 * CombinedLockAcquireLWLock
 *		Get LWLock instance for partition of cached buffer lookup table
 *
 * To prevent other backend processes from modifying the lookup table,
 * this function sets the CombinedLock's flag to true.
 */
void
CombinedLockAcquireLWLock(CombinedLock *lock)
{
	LWLockAcquire(&lock->lwLock, LW_EXCLUSIVE);
	SpinLockAcquire(&lock->spinLock);
	lock->flag = true;
	SpinLockRelease(&lock->spinLock);
}

/*
 * CombinedLockReleaseSpinLock
 *		Release the spinlock used in insert/delete functions
 */
void
CombinedLockReleaseSpinLock(CombinedLock *lock)
{
	SpinLockRelease(&lock->spinLock);
}

/*
 * CombinedLockReleaseSpinLock
 *		Release the LWLock used in lookup function after changing the
 *		CombinedLock's flag to false.
 */
void
CombinedLockReleaseLWLock(CombinedLock *lock)
{
	lock->flag = false;
	LWLockRelease(&lock->lwLock);
}

/*
 * CachedBufTableInsert
 *		Insert a hash table entry for given tag and buffer ID
 *
 * If a hash entry for the tag already exists, only buffer ID is inserted.
 * Chain the buffer ID to the doubly-linked list.
 *
 * This function must hold spin lock.
 */
void
CachedBufTableInsert(RelFileNode *rnode, BlockNumber *blockNum, int buf_id)
{
	CachedBufTag	cb_tag;
	uint32		hashcode;
	CombinedLock	*combined_lock;
	CachedBufEnt	*hash_entry;
	bool		found;

	cb_tag.rnode = *rnode;
	cb_tag.modOfBlockNum = *blockNum % NUM_MAP_PARTITIONS_IN_REL;

	combined_lock = GetCachedBufPartitionLock(&cb_tag, &hashcode);
	CombinedLockAcquireSpinLock(combined_lock);

	/* look up or create a hash table entry */
	hash_entry = (CachedBufEnt *)
		hash_search_with_hash_value(CachedBufHash,
									(void *) &cb_tag,
									hashcode,
									HASH_ENTER,
									&found);

	/* If not found, initialize linked list */
	if (!found)
		hash_entry->head = CACHED_BUF_END_OF_LIST;

	cb_dlist_push_head(hash_entry, buf_id);

	CombinedLockReleaseSpinLock(combined_lock);
}

/*
 * CachedBufLookup
 *		Lookup the buffers for the given tag in the cached buffer
 *		hash table. Insert the buffer ID to the array of buffer
 *		IDs, and return the total number of buffers to be invalidated.
 *
 * This function must hold exclusive LWLock for tag's partition.
 */
int
CachedBufLookup(RelFileNode rnode, ForkNumber *forkNum, int nforks,
				int *forknum_indexes, BlockNumber *firstDelBlock,
				int *buf_id_array, int size)
{
	CachedBufTag	cb_tag;
	uint32		hashcode;
	CombinedLock	*combined_lock;
	CachedBufEnt	*hash_entry,
					temp_hash_entry;
	BufDlistEnt	*curr_entry = NULL;
	int		mod,
			i,
			curr_buf_id,
			new_curr_buf_id,
			count = 0;
	bool		target_buf = false;

	for (mod = 0; mod < NUM_MAP_PARTITIONS_IN_REL; mod++)
	{
		cb_tag.rnode = rnode;
		cb_tag.modOfBlockNum = mod;

		combined_lock = GetCachedBufPartitionLock(&cb_tag, &hashcode);
		CombinedLockAcquireLWLock(combined_lock);

		hash_entry = (CachedBufEnt *)
			hash_search_with_hash_value(CachedBufHash,
										(void *) &cb_tag,
										hashcode,
										HASH_FIND,
										NULL);

		if (!hash_entry)
		{
			CombinedLockReleaseLWLock(combined_lock);
			continue;
		}

		/* Initial temporary dlist */
		temp_hash_entry.head = CACHED_BUF_END_OF_LIST;

		/* When traversing the main dlist, start from head */
		curr_buf_id = hash_entry->head;

		while(curr_buf_id != CACHED_BUF_END_OF_LIST && count < size)
		{
			BufferDesc *bufHdr = GetBufferDescriptor(curr_buf_id);
			curr_entry = BufDlistEntArray + curr_buf_id;
			new_curr_buf_id = curr_entry->next;

			/* Check if it's our target buffer */
			for (i = 0; i < nforks; i++)
			{
				if (bufHdr->tag.forkNum != forkNum[i])
					continue;
				else
				{
					if (bufHdr->tag.blockNum >= firstDelBlock[i])
						target_buf = true;
					break;
				}
			}

			if (target_buf)
			{
				forknum_indexes[count] = i;
				buf_id_array[count] = curr_buf_id;
				++count;
			}
			else
			{
				/*
				* It's not the target buffer. Remove the current buffer ID
				* from the current list of target buffer IDs and store it
				* to a temporary list.
				*/
				cb_dlist_delete(hash_entry, curr_buf_id);
				cb_dlist_push_head(&temp_hash_entry, curr_buf_id);
			}
			/* Move current pointer to next */
			curr_buf_id = new_curr_buf_id;
		}

		/* Check if main dlist is now empty */
		if (cb_dlist_is_empty(hash_entry))
		{
			hash_entry->head = temp_hash_entry.head;
			temp_hash_entry.head = CACHED_BUF_END_OF_LIST;
		}

		/* If we have a temporary dlist, append it to the main dlist */
		if (!cb_dlist_is_empty(hash_entry) &&
			!cb_dlist_is_empty(&temp_hash_entry))
			cb_dlist_combine(hash_entry, &temp_hash_entry);

		CombinedLockReleaseLWLock(combined_lock);
	}

	return count;
}

/*
 * CachedBufLookupAll
 *		Lookup all the buffers of specified relations in the cached buffer
 *		hash table. Store all the buffer IDs to the array, and return
 *		the total number of buffers to be invalidated.
 *
 * This function must hold exclusive LWLock for tag's partition.
 */
int
CachedBufLookupAll(RelFileNode *rnode, int nnodes, int *buf_id_array,
				   int size)
{
	CachedBufTag	cb_tag;
	uint32		hashcode;
	CombinedLock	*combined_lock;
	CachedBufEnt	*hash_entry;
	BufDlistEnt	*curr_entry = NULL;
	int		i,
			mod,
			curr_buf_id,
			new_curr_buf_id,
			count = 0;

	for (i = 0; i < nnodes; i++)
	{
		cb_tag.rnode = rnode[i];

		for (mod = 0; mod < NUM_MAP_PARTITIONS_IN_REL; mod++)
		{
			cb_tag.modOfBlockNum = mod;

			combined_lock = GetCachedBufPartitionLock(&cb_tag, &hashcode);
			CombinedLockAcquireLWLock(combined_lock);

			hash_entry = (CachedBufEnt *)
				hash_search_with_hash_value(CachedBufHash,
											(void *) &cb_tag,
											hashcode,
											HASH_FIND,
											NULL);

			if (!hash_entry)
			{
				CombinedLockReleaseLWLock(combined_lock);
				continue;
			}

			/* When traversing the dlist, start from head */
			curr_buf_id = hash_entry->head;

			while(curr_buf_id != CACHED_BUF_END_OF_LIST && count < size)
			{
				curr_entry = BufDlistEntArray + curr_buf_id;
				new_curr_buf_id = curr_entry->next;
				buf_id_array[count] = curr_buf_id;
				++count;
				curr_buf_id = new_curr_buf_id; /* Move pointer */
			}

			CombinedLockReleaseLWLock(combined_lock);
		}
	}

	return count;
}


/*
 * CachedBufTableDelete
 *		Unlink the buffer ID from the doubly-linked list, then remove
 *		the hash entry for the given tag if its list is empty.
 *
 * This function must hold spin lock.
 */
void
CachedBufTableDelete(RelFileNode *rnode, BlockNumber *blockNum, int buf_id)
{
	CachedBufTag	cb_tag;
	uint32		hashcode;
	CombinedLock	*combined_lock;
	CachedBufEnt	*hash_entry;
	bool		found;

	cb_tag.rnode = *rnode;
	cb_tag.modOfBlockNum = *blockNum % NUM_MAP_PARTITIONS_IN_REL;

	combined_lock = GetCachedBufPartitionLock(&cb_tag, &hashcode);
	CombinedLockAcquireSpinLock(combined_lock);

	/* look up hash table entry */
	hash_entry = (CachedBufEnt *)
		hash_search_with_hash_value(CachedBufHash,
									(void *) &cb_tag,
									hashcode,
									HASH_FIND,
									&found);

	if (!found)		/* tag not found, nothing to do */
	{
		CombinedLockReleaseSpinLock(combined_lock);
		return;
	}

	cb_dlist_delete(hash_entry, buf_id);

	/*
	 * If there's no more cached elements for the given tag,
	 * remove the hash entry.
	 */
	if (cb_dlist_is_empty(hash_entry))
	{
		hash_entry = (CachedBufEnt *)
			hash_search_with_hash_value(CachedBufHash,
										(void *) &cb_tag,
										hashcode,
										HASH_REMOVE,
										NULL);

		if (!hash_entry)
			elog(ERROR, "cached buffer hash table corrupted");
	}

	CombinedLockReleaseSpinLock(combined_lock);
}

/*
 * inline functions for the doubly-linked list of cached buffers
 */
/* Return the dlist entry */
static inline BufDlistEnt *
cb_dlist_entry(int buf_id)
{
	return BufDlistEntArray + buf_id;
}

/* Return the next entry */
static inline BufDlistEnt *
cb_dlist_next(BufDlistEnt *entry)
{
	return entry->next == CACHED_BUF_END_OF_LIST ?
							NULL : cb_dlist_entry(entry->next);
}

/* Return the prev entry */
static inline BufDlistEnt *
cb_dlist_prev(BufDlistEnt *entry)
{
	return entry->prev == CACHED_BUF_END_OF_LIST ?
							NULL : cb_dlist_entry(entry->prev);
}

/* Return if dlist is empty */
static inline bool
cb_dlist_is_empty(CachedBufEnt *hash_entry)
{
	return hash_entry->head == CACHED_BUF_END_OF_LIST;
}

/* Push to head of dlist */
static inline void
cb_dlist_push_head(CachedBufEnt *hash_entry, int buf_id)
{
	BufDlistEnt	*new_entry = cb_dlist_entry(buf_id);
	new_entry->next = hash_entry->head;

	if (cb_dlist_is_empty(hash_entry))
		new_entry->prev = buf_id;
	else
	{
		BufDlistEnt	*head_entry = cb_dlist_entry(hash_entry->head);
		new_entry->prev = head_entry->prev;
		cb_dlist_next(new_entry)->prev = buf_id;
	}
	hash_entry->head = buf_id;
}

/* Remove the buffer ID from dlist */
static inline void
cb_dlist_delete(CachedBufEnt *hash_entry, int buf_id)
{
	BufDlistEnt	*curr_entry = cb_dlist_entry(buf_id);
	BufDlistEnt	*head_entry = cb_dlist_entry(hash_entry->head);
	BufDlistEnt	*tail_entry = cb_dlist_entry(head_entry->prev);
	BufDlistEnt	*next_entry = cb_dlist_next(curr_entry);
	BufDlistEnt	*prev_entry = cb_dlist_prev(curr_entry);

	/* If only one entry is in the list */
	if (head_entry == tail_entry)
	{
		hash_entry->head = CACHED_BUF_END_OF_LIST;
		return;
	}

	/*
	 * If there is a next entry, update its prev field.
	 * Otherwise, current entry is at tail (but not at head),
	 * so update the new tail.
	 */
	if (next_entry != NULL)
		next_entry->prev = curr_entry->prev;
	else
		head_entry->prev = curr_entry->prev;

	/*
	 * If the previous entry is not the tail entry, update its
	 * next field. Otherwise, current entry is at head (but not
	 * at tail). Update the new head entry and its tail pointer.
	 */
	if (prev_entry != tail_entry)
		prev_entry->next = curr_entry->next;
	else
		hash_entry->head = curr_entry->next;
}

/* Append head of temporary dlist to main dlist */
static inline void
cb_dlist_combine(CachedBufEnt *main, CachedBufEnt *temp)
{
	BufDlistEnt	*main_head_entry = cb_dlist_entry(main->head);
	BufDlistEnt	*temp_head_entry = cb_dlist_entry(temp->head);
	int	main_tail = main_head_entry->prev;
	int	temp_tail = temp_head_entry->prev;
	BufDlistEnt	*main_tail_entry = cb_dlist_entry(main_tail);

	/* Append the temporary dlist to main dlist */
	main_tail_entry->next = temp->head;
	temp_head_entry->prev = main_tail;
	main_head_entry->prev = temp_tail;

	/* Clear the head of temporary dlist */
	temp->head = CACHED_BUF_END_OF_LIST;
}
