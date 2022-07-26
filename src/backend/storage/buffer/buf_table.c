/*-------------------------------------------------------------------------
 *
 * buf_table.c
 *	  routines for mapping BufferTags to buffer indexes.
 *
 * Note: the routines in this file do no locking of their own.  The caller
 * must hold a suitable lock on the appropriate BufMappingLock, as specified
 * in the comments.  We can't do the locking inside these functions because
 * in most cases the caller needs to adjust the buffer header contents
 * before the lock is released (see notes in README).
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_table.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "miscadmin.h"

/* entry for buffer lookup hashtable */
typedef struct
{
	BufferTag	key;			/* Tag of a disk page */
	volatile int id;			/* Associated buffer ID */
} BufferLookupEnt;

static HTAB *SharedBufHash;
ConditionVariableMinimallyPadded *BufferInsertionCVArray;


/*
 * Estimate space needed for mapping hashtable
 *		size is the desired hash table size (possibly more than NBuffers)
 */
Size
BufTableShmemSize(int size)
{
	Size		sz;

	/*
	 * BufferAlloc inserts new buffer entry before deleting the old. That is
	 * why there is a need in additional free entry for every backend.
	 *
	 * Also size could not be less than NUM_BUFFER_PARTITIONS.
	 *
	 * And since get_hash_entry is not very  inefficiency of dynahash's
	 * get_hash_entry, it is better to have more spare entries, so we use both
	 * MaxBackends and NUM_BUFFER_PARTITIONS.
	 */
	size += MaxBackends + NUM_BUFFER_PARTITIONS;
	sz = hash_estimate_size(size, sizeof(BufferLookupEnt));

	/*
	 * Every backend should have associated ConditionVariable so we could map
	 * them 1:1 without resolving collisions. And additional one is allocated
	 * for startup process (xlog player), which have MyBackendId == -1.
	 */
	sz = add_size(sz, mul_size(MaxBackends + 1,
							   sizeof(ConditionVariableMinimallyPadded)));
	return sz;
}

/*
 * Initialize shmem hash table for mapping buffers
 *		size is the desired hash table size (possibly more than NBuffers)
 */
void
InitBufTable(int size)
{
	HASHCTL		info;
	bool		found;

	/* see comments in BufTableShmemSize */
	size += MaxBackends + NUM_BUFFER_PARTITIONS;

	/* assume no locking is needed yet */

	/* BufferTag maps to Buffer */
	info.keysize = sizeof(BufferTag);
	info.entrysize = sizeof(BufferLookupEnt);
	info.num_partitions = NUM_BUFFER_PARTITIONS;

	SharedBufHash = ShmemInitHash("Shared Buffer Lookup Table",
								  size, size,
								  &info,
								  HASH_ELEM | HASH_BLOBS | HASH_PARTITION |
								  HASH_FIXED_SIZE);
	BufferInsertionCVArray = (ConditionVariableMinimallyPadded *)
		ShmemInitStruct("Shared Buffer Backend Insertion CV",
						(MaxBackends + 1) *
						sizeof(ConditionVariableMinimallyPadded),
						&found);
	if (!found)
	{
		for (int i = 0; i < MaxBackends + 1; i++)
			ConditionVariableInit(&BufferInsertionCVArray[i].cv);
	}
}

/*
 * BufTableHashCode
 *		Compute the hash code associated with a BufferTag
 *
 * This must be passed to the lookup/insert/delete routines along with the
 * tag.  We do it like this because the callers need to know the hash code
 * in order to determine which buffer partition to lock, and we don't want
 * to do the hash computation twice (hash_any is a bit slow).
 */
uint32
BufTableHashCode(BufferTag *tagPtr)
{
	return get_hash_value(SharedBufHash, (void *) tagPtr);
}

/*
 * BufTableLookup
 *		Lookup the given BufferTag; return buffer ID, or -1 if not found
 *
 * Caller must hold at least share lock on BufMappingLock for tag's partition
 */
int
BufTableLookup(BufferTag *tagPtr, uint32 hashcode)
{
	BufferLookupEnt *result;

	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash,
									(void *) tagPtr,
									hashcode,
									HASH_FIND,
									NULL);

	if (!result)
		return -1;

	return result->id;
}

/*
 * BufTableInsert
 *		Insert a hashtable entry for given tag and buffer ID,
 *		unless an entry already exists for that tag
 *
 * Returns pointer to volatile int holding buffer index.
 * If table entry were just inserted, index is filled with -1.
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
volatile int *
BufTableInsert(BufferTag *tagPtr, uint32 hashcode)
{
	BufferLookupEnt *result;
	bool		found;

	Assert(tagPtr->blockNum != P_NEW);	/* invalid tag */

	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash,
									(void *) tagPtr,
									hashcode,
									HASH_ENTER,
									&found);

	if (!found)
		result->id = -1;

	return &result->id;
}

/*
 * BufTableDelete
 *		Delete the hashtable entry for given tag (which must exist)
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
void
BufTableDelete(BufferTag *tagPtr, uint32 hashcode)
{
	BufferLookupEnt *result;

	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash,
									(void *) tagPtr,
									hashcode,
									HASH_REMOVE,
									NULL);

	if (!result)				/* shouldn't happen */
		elog(ERROR, "shared buffer hash table corrupted");
}
