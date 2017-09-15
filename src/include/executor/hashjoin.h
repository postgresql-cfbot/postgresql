/*-------------------------------------------------------------------------
 *
 * hashjoin.h
 *	  internal structures for hash joins
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/hashjoin.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HASHJOIN_H
#define HASHJOIN_H

#include "nodes/execnodes.h"
#include "storage/buffile.h"
#include "storage/barrier.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"
#include "utils/leader_gate.h"
#include "utils/sharedtuplestore.h"

/* ----------------------------------------------------------------
 *				hash-join hash table structures
 *
 * Each active hashjoin has a HashJoinTable control block, which is
 * palloc'd in the executor's per-query context.  All other storage needed
 * for the hashjoin is kept in private memory contexts, two for each hashjoin.
 * This makes it easy and fast to release the storage when we don't need it
 * anymore.  (Exception: data associated with the temp files lives in the
 * per-query context too, since we always call buffile.c in that context.)
 *
 * The hashtable contexts are made children of the per-query context, ensuring
 * that they will be discarded at end of statement even if the join is
 * aborted early by an error.  (Likewise, any temporary files we make will
 * be cleaned up by the virtual file manager in event of an error.)
 *
 * Storage that should live through the entire join is allocated from the
 * "hashCxt", while storage that is only wanted for the current batch is
 * allocated in the "batchCxt".  By resetting the batchCxt at the end of
 * each batch, we free all the per-batch storage reliably and without tedium.
 *
 * During first scan of inner relation, we get its tuples from executor.
 * If nbatch > 1 then tuples that don't belong in first batch get saved
 * into inner-batch temp files. The same statements apply for the
 * first scan of the outer relation, except we write tuples to outer-batch
 * temp files.  After finishing the first scan, we do the following for
 * each remaining batch:
 *	1. Read tuples from inner batch file, load into hash buckets.
 *	2. Read tuples from outer batch file, match to hash buckets and output.
 *
 * It is possible to increase nbatch on the fly if the in-memory hash table
 * gets too big.  The hash-value-to-batch computation is arranged so that this
 * can only cause a tuple to go into a later batch than previously thought,
 * never into an earlier batch.  When we increase nbatch, we rescan the hash
 * table and dump out any tuples that are now of a later batch to the correct
 * inner batch file.  Subsequently, while reading either inner or outer batch
 * files, we might find tuples that no longer belong to the current batch;
 * if so, we just dump them out to the correct batch file.
 * ----------------------------------------------------------------
 */

/* these are in nodes/execnodes.h: */
/* typedef struct HashJoinTupleData *HashJoinTuple; */
/* typedef struct HashJoinTableData *HashJoinTable; */

typedef struct HashJoinTupleData
{
	/* link to next tuple in same bucket */
	union
	{
		dsa_pointer shared;
		struct HashJoinTupleData *unshared;
	} next;
	uint32		hashvalue;		/* tuple's hash code */
	/* Tuple data, in MinimalTuple format, follows on a MAXALIGN boundary */
}			HashJoinTupleData;

#define HJTUPLE_OVERHEAD  MAXALIGN(sizeof(HashJoinTupleData))
#define HJTUPLE_MINTUPLE(hjtup)  \
	((MinimalTuple) ((char *) (hjtup) + HJTUPLE_OVERHEAD))

/*
 * The head of the linked list of tuples in each bucket.  For shared hash
 * tables, it allows for tuples to be inserted into the bucket with an atomic
 * operation.  For unshared hash tables, it's a plain old pointer to the first
 * tuple.
 */
typedef union HashJoinBucketHead
{
	dsa_pointer_atomic shared;
	HashJoinTuple unshared;
} HashJoinBucketHead;

/*
 * If the outer relation's distribution is sufficiently nonuniform, we attempt
 * to optimize the join by treating the hash values corresponding to the outer
 * relation's MCVs specially.  Inner relation tuples matching these hash
 * values go into the "skew" hashtable instead of the main hashtable, and
 * outer relation tuples with these hash values are matched against that
 * table instead of the main one.  Thus, tuples with these hash values are
 * effectively handled as part of the first batch and will never go to disk.
 * The skew hashtable is limited to SKEW_WORK_MEM_PERCENT of the total memory
 * allowed for the join; while building the hashtables, we decrease the number
 * of MCVs being specially treated if needed to stay under this limit.
 *
 * Note: you might wonder why we look at the outer relation stats for this,
 * rather than the inner.  One reason is that the outer relation is typically
 * bigger, so we get more I/O savings by optimizing for its most common values.
 * Also, for similarly-sized relations, the planner prefers to put the more
 * uniformly distributed relation on the inside, so we're more likely to find
 * interesting skew in the outer relation.
 */
typedef struct HashSkewBucket
{
	bool		active;			/* is this bucket active? */
	uint32		hashvalue;		/* common hash value */
	HashJoinBucketHead tuples;	/* linked list of inner-relation tuples */
} HashSkewBucket;

#define SKEW_BUCKET_OVERHEAD  MAXALIGN(sizeof(HashSkewBucket))
#define INVALID_SKEW_BUCKET_NO	(-1)
#define SKEW_WORK_MEM_PERCENT  2
#define SKEW_MIN_OUTER_FRACTION  0.01

/*
 * To reduce palloc/dsa_allocate overhead, the HashJoinTuples for the current
 * batch are packed in 32kB buffers instead of pallocing each tuple
 * individually.
 */
typedef struct HashMemoryChunkData
{
	int			ntuples;		/* number of tuples stored in this chunk */
	size_t		maxlen;			/* size of the buffer holding the tuples */
	size_t		used;			/* number of buffer bytes already used */

	/* pointer to the next chunk (linked list) */
	union
	{
		dsa_pointer shared;
		struct HashMemoryChunkData *unshared;
	} next;

	char		data[FLEXIBLE_ARRAY_MEMBER];	/* buffer allocated at the end */
}			HashMemoryChunkData;

typedef struct HashMemoryChunkData *HashMemoryChunk;

#define HASH_CHUNK_SIZE			(32 * 1024L)
#define HASH_CHUNK_HEADER_SIZE	(offsetof(HashMemoryChunkData, data))
#define HASH_CHUNK_THRESHOLD	(HASH_CHUNK_SIZE / 4)

/*
 * State for a shared hash join table.  Each backend participating in a hash
 * join with a shared hash table also has a HashJoinTableData object in
 * backend-private memory, which points to this shared state in the DSM
 * segment.
 */
typedef struct SharedHashJoinTableData
{
	Barrier barrier;				/* synchronization for the hash join */
	dsa_pointer buckets;			/* shared hash table buckets */
	int nbuckets;
	int log2_nbuckets;
	int nbatch;
	int planned_participants;		/* number of planned workers + leader */

	dsa_pointer skew_buckets;		/* shared skew hash table buckets */
	dsa_pointer skew_bucket_nums;	/* buckets numbers ordered by mvc */
	Size		skew_space_used;	/* allowance given out to participants */
	Size		skew_space_allowed;	/* total allowance (all participants) */
	int			num_skew_buckets;	/* number of active skew buckets */
	int			skew_bucket_len;	/* number of skew bucket slots */
	int			current_skew_bucketno;	/* for unmatched scan */

	LeaderGate leader_gate;			/* gate to avoid leader/worker deadlock */

	Barrier shrink_barrier;			/* synchronization of hashtable shrink */
	bool shrink_needed;				/* flag indicating all must help shrink */
	long nfreed;					/* shared counter for hashtable shrink */
	long ninmemory;					/* shared counter for hashtable shrink */
	bool grow_enabled;				/* shared flag to prevent useless growth */

	LWLock chunk_lock;				/* protects the following members */
	dsa_pointer chunks;				/* chunks loaded for the current batch */
	dsa_pointer chunk_work_queue;	/* next chunk for shared processing */
	Size size;						/* size of buckets + chunks */
	Size ntuples;
} SharedHashJoinTableData;

typedef struct HashJoinTableData
{
	int			nbuckets;		/* # buckets in the in-memory hash table */
	int			log2_nbuckets;	/* its log2 (nbuckets must be a power of 2) */

	int			nbuckets_original;	/* # buckets when starting the first hash */
	int			nbuckets_optimal;	/* optimal # buckets (per batch) */
	int			log2_nbuckets_optimal;	/* log2(nbuckets_optimal) */

	/* buckets[i] is head of list of tuples in i'th in-memory bucket */
	HashJoinBucketHead *buckets;
	/* buckets array is per-batch storage, as are all the tuples */

	bool		keepNulls;		/* true to store unmatchable NULL tuples */

	HashSkewBucket *skewBucket;	/* hashtable of skew buckets */
	int			skewBucketLen;	/* size of skewBucket array (a power of 2!) */
	int			nSkewBuckets;	/* number of active skew buckets */
	int		   *skewBucketNums; /* array indexes of active skew buckets */

	int			nbatch;			/* number of batches */
	int			curbatch;		/* current batch #; 0 during 1st pass */

	int			nbatch_original;	/* nbatch when we started inner scan */
	int			nbatch_outstart;	/* nbatch when we started outer scan */

	bool		growEnabled;	/* flag to shut off nbatch increases */

	double		partialTuples;	/* # tuples obtained from inner plan by me */
	double		totalTuples;	/* # tuples obtained from inner plan by all */
	double		skewTuples;		/* # tuples inserted into skew tuples */

	/*
	 * These arrays are allocated for the life of the hash join, but only if
	 * nbatch > 1.  A file is opened only when we first write a tuple into it
	 * (otherwise its pointer remains NULL).  Note that the zero'th array
	 * elements never get used, since we will process rather than dump out any
	 * tuples of batch zero.
	 */
	BufFile   **innerBatchFile; /* buffered virtual temp file per batch */
	BufFile   **outerBatchFile; /* buffered virtual temp file per batch */

	/*
	 * Info about the datatype-specific hash functions for the datatypes being
	 * hashed. These are arrays of the same length as the number of hash join
	 * clauses (hash keys).
	 */
	FmgrInfo   *outer_hashfunctions;	/* lookup data for hash functions */
	FmgrInfo   *inner_hashfunctions;	/* lookup data for hash functions */
	bool	   *hashStrict;		/* is each hash join operator strict? */

	/*
	 * These variables track the space used by the main and skew hash tables,
	 * so that work_mem can be respected.
	 *
	 * When running a parallel-aware, main hash table space tracking is done
	 * in SharedHashJoinTableData when chunks are allocated and freed, not
	 * here, but the final spaceUsed value is copied into here for the benefit
	 * of EXPLAIN.  Skew table tuples are not managed with memory chunks, but
	 * the same general approach is used: each backend has its own budget in
	 * spaceAllowedSkew and asks for more from the shared memory counter only
	 * when it runs out, to reduce contention.
	 */
	Size		spaceUsed;		/* memory space currently used by hashtable */
	Size		spaceAllowed;	/* upper limit for space used */
	Size		spacePeak;		/* peak space used */
	Size		spaceUsedSkew;	/* skew hash table's current space usage */
	Size		spaceAllowedSkew;	/* upper limit for skew hashtable */

	MemoryContext hashCxt;		/* context for whole-hash-join storage */
	MemoryContext batchCxt;		/* context for this-batch-only storage */

	/* used for dense allocation of tuples (into linked chunks) */
	HashMemoryChunk chunks;		/* one list for the whole batch */

	/* used for scanning for unmatched tuples */
	HashMemoryChunk unmatched_chunks;
	HashMemoryChunk chunks_to_reinsert;

	HashMemoryChunk current_chunk;
	Size		current_chunk_index;

	/* State for coordinating shared hash tables. */
	dsa_area *area;
	SharedHashJoinTableData *shared;	/* the shared state */
	SharedTuplestoreAccessor *shared_inner_batches;
	SharedTuplestoreAccessor *shared_outer_batches;
	bool detached_early;				/* did we decide to detach early? */
	dsa_pointer current_chunk_shared;	/* DSA pointer to 'current_chunk' */

}			HashJoinTableData;

/* Check if a HashJoinTable is shared by parallel workers. */
#define HashJoinTableIsShared(table) ((table)->shared != NULL)

/* The phases of a parallel hash join. */
#define PHJ_PHASE_BEGINNING				0
#define PHJ_PHASE_CREATING				1
#define PHJ_PHASE_BUILDING				2
#define PHJ_PHASE_RESIZING				3
#define PHJ_PHASE_REINSERTING			4
#define PHJ_PHASE_PROBING				5	/* PHJ_PHASE_PROBING_BATCH(0) */
#define PHJ_PHASE_UNMATCHED				6	/* PHJ_PHASE_UNMATCHED_BATCH(0) */

/* The subphases for batches. */
#define PHJ_SUBPHASE_RESETTING			0
#define PHJ_SUBPHASE_LOADING			1
#define PHJ_SUBPHASE_PROBING			2
#define PHJ_SUBPHASE_UNMATCHED			3

/* The phases of parallel processing for batch(n). */
#define PHJ_PHASE_RESETTING_BATCH(n)	(PHJ_PHASE_UNMATCHED + (n) * 4 - 3)
#define PHJ_PHASE_LOADING_BATCH(n)		(PHJ_PHASE_UNMATCHED + (n) * 4 - 2)
#define PHJ_PHASE_PROBING_BATCH(n)		(PHJ_PHASE_UNMATCHED + (n) * 4 - 1)
#define PHJ_PHASE_UNMATCHED_BATCH(n)	(PHJ_PHASE_UNMATCHED + (n) * 4 - 0)

/* Phase number -> sub-phase within a batch. */
#define PHJ_PHASE_TO_SUBPHASE(p)										\
	(((int)(p) - PHJ_PHASE_UNMATCHED + PHJ_SUBPHASE_UNMATCHED) % 4)

/* Phase number -> batch number. */
#define PHJ_PHASE_TO_BATCHNO(p)											\
	(((int)(p) - PHJ_PHASE_UNMATCHED + PHJ_SUBPHASE_UNMATCHED) / 4)

/* The phases of ExecHashShrink. */
#define PHJ_SHRINK_PHASE_BEGINNING		0
#define PHJ_SHRINK_PHASE_CLEARING		1
#define PHJ_SHRINK_PHASE_WORKING		2
#define PHJ_SHRINK_PHASE_DECIDING		3
#define PHJ_SHRINK_PHASE(n) (n % 4)

#endif							/* HASHJOIN_H */
