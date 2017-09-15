/*-------------------------------------------------------------------------
 *
 * nodeHash.c
 *	  Routines to hash relations for hashjoin
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeHash.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		MultiExecHash	- generate an in-memory hash table of the relation
 *		ExecInitHash	- initialize node and subnodes
 *		ExecEndHash		- shutdown node and subnodes
 */

#include "postgres.h"

#include <math.h>
#include <limits.h>

#include "access/htup_details.h"
#include "access/parallel.h"
#include "catalog/pg_statistic.h"
#include "commands/tablespace.h"
#include "executor/execdebug.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/barrier.h"
#include "utils/dynahash.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/*
 * To avoid overflow we don't allow batch increases if we have more than this
 * number already.
 */
#define MAX_BATCHES_BEFORE_INCREASES_STOP \
	Min(INT_MAX / 2, MaxAllocSize / (sizeof(void *) * 2))

/*
 * A special value used in parallel-aware mode to indicate that a skew bucket
 * is defunct.
 */
#define SKEW_BUCKET_TOMBSTONE ((dsa_pointer) -1)

static void ExecHashIncreaseNumBatches(HashJoinTable hashtable, int nbatch);
static void ExecHashIncreaseNumBucketsIfNeeded(HashJoinTable hashtable);
static void ExecHashReinsertHashtableIfNeeded(HashJoinTable hashtable);
static void ExecHashBuildSkewHash(HashJoinTable hashtable, Hash *node,
					  int mcvsToUse);
static bool ExecHashSkewTableInsert(HashJoinTable hashtable,
						TupleTableSlot *slot,
						uint32 hashvalue,
						int bucketNumber);
static void ExecHashHandleSkewOverflow(HashJoinTable hashtable, int bucketno);
static void ExecHashTransferSkewTuples(HashJoinTable hashtable,
									   HashJoinTuple head_tuple,
									   dsa_pointer head_tuple_shared);
static dsa_pointer ExecHashDetachSkewBucket(HashJoinTable table, int bucketno);

static HashMemoryChunk ExecHashPopChunkQueue(HashJoinTable table,
											 dsa_pointer *shared);
static HashMemoryChunk ExecHashPopChunkQueueUnlocked(HashJoinTable table,
													 dsa_pointer *shared);
static HashJoinTuple ExecHashNextTupleInBucket(HashJoinTable table,
											   HashJoinTuple tuple);

static bool ExecHashInsertTupleIntoBucket(HashJoinTable table,
										  HashJoinBucketHead *head,
										  HashJoinTuple tuple,
										  dsa_pointer tuple_shared);
static HashJoinTuple ExecHashFirstTupleInBucket(HashJoinTable table, int bucketno);
static HashJoinTuple ExecHashFirstTupleInSkewBucket(HashJoinTable table,
													int bucketno,
													dsa_pointer *shared);
static void ExecHashFreeSkewTable(HashJoinTable hashtable);
static HashJoinTuple ExecHashNextTupleInBucket(HashJoinTable table,
											   HashJoinTuple tuple);

static HashJoinTuple ExecHashLoadPrivateTuple(HashJoinTable hashtable,
											  MinimalTuple tuple,
											  bool respect_work_mem);
static HashJoinTuple ExecHashLoadSharedTuple(HashJoinTable hashtable,
											 MinimalTuple tuple,
											 dsa_pointer *shared,
											 bool respect_work_mem);

/* ----------------------------------------------------------------
 *		ExecHash
 *
 *		stub for pro forma compliance
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecHash(PlanState *pstate)
{
	elog(ERROR, "Hash node does not support ExecProcNode call convention");
	return NULL;
}

/* ----------------------------------------------------------------
 *		MultiExecHash
 *
 *		build hash table for hashjoin, doing partitioning if more
 *		than one batch is required.
 * ----------------------------------------------------------------
 */
Node *
MultiExecHash(HashState *node)
{
	PlanState  *outerNode;
	List	   *hashkeys;
	HashJoinTable hashtable;
	TupleTableSlot *slot;
	ExprContext *econtext;
	uint32		hashvalue;
	Barrier	   *barrier;
	bool		is_shared;

	/* must provide our own instrumentation support */
	if (node->ps.instrument)
		InstrStartNode(node->ps.instrument);

	/*
	 * get state info from node
	 */
	outerNode = outerPlanState(node);
	hashtable = node->hashtable;

	/*
	 * set expression context
	 */
	hashkeys = node->hashkeys;
	econtext = node->ps.ps_ExprContext;

	is_shared = HashJoinTableIsShared(hashtable);

	if (is_shared)
	{
		/*
		 * Synchronize parallel hash table builds.  At this stage we know that
		 * the shared hash table has been created, but we don't know if our
		 * peers are still in MultiExecHash and if so how far through.  We use
		 * the phase to synchronize with them.
		 */
		barrier = &hashtable->shared->barrier;

		switch (BarrierPhase(barrier))
		{
			case PHJ_PHASE_BEGINNING:
				/* ExecHashTableCreate already handled this phase. */
				Assert(false);
			case PHJ_PHASE_CREATING:
				/* Wait for serial phase, and then either build or wait. */
				BarrierWait(barrier, WAIT_EVENT_HASH_CREATING);
				goto build;
			case PHJ_PHASE_BUILDING:
				/* Building is already underway.  Join in. */
				goto build;
			case PHJ_PHASE_RESIZING:
				/* Can't help with serial phase. */
				goto post_resize;
			case PHJ_PHASE_REINSERTING:
				/* Reinserting is in progress after resizing.  Let's help. */
				goto reinsert;
			default:
				/* The hash table building work is already finished. */
				goto finish;
		}
	}

 build:
	if (is_shared)
	{
		/* Make sure our local state is up-to-date so we can build. */
		Assert(BarrierPhase(barrier) == PHJ_PHASE_BUILDING);
		ExecHashUpdate(hashtable);

		/* Coordinate shrinking while we build the hash table. */
		BarrierAttach(&hashtable->shared->shrink_barrier);
	}

	/*
	 * get all inner tuples and insert into the hash table (or temp files)
	 */
	for (;;)
	{
		slot = ExecProcNode(outerNode);
		if (TupIsNull(slot))
			break;
		/* We have to compute the hash value */
		econtext->ecxt_innertuple = slot;
		if (ExecHashGetHashValue(hashtable, econtext, hashkeys,
								 false, hashtable->keepNulls,
								 &hashvalue))
		{
			int			bucketNumber;

			bucketNumber = ExecHashGetSkewBucket(hashtable, hashvalue);
			if (bucketNumber != INVALID_SKEW_BUCKET_NO)
			{
				/* It's a skew tuple, so put it into that hash table */
				if (ExecHashSkewTableInsert(hashtable, slot, hashvalue,
											bucketNumber))
					hashtable->skewTuples += 1;
				else
				{
					/* Bucket concurrently removed, so insert normally. */
					ExecHashTableInsert(hashtable, slot, hashvalue);
				}
			}
			else
			{
				/* Not subject to skew optimization, so insert normally */
				ExecHashTableInsert(hashtable, slot, hashvalue);
			}
			hashtable->partialTuples += 1;
			if (!is_shared)
				hashtable->totalTuples += 1;
		}
	}

	if (is_shared)
	{
		bool elected_to_resize;

		/* Shinking is finished. */
		BarrierDetach(&hashtable->shared->shrink_barrier);

		/*
		 * Wait for all participants to finish building the hash table and
		 * arrive here, so we can check the load factor and decide if we need
		 * to increase the number of buckets to reduce it.
		 */
		Assert(BarrierPhase(barrier) == PHJ_PHASE_BUILDING);
		elected_to_resize = BarrierWait(barrier, WAIT_EVENT_HASH_BUILDING);
		/*
		 * Resizing is a serial phase.  All but one should skip ahead to
		 * reinserting phase, so we use the barrier to elect one participant.
		 */
		if (!elected_to_resize)
			goto post_resize;
		Assert(BarrierPhase(barrier) == PHJ_PHASE_RESIZING);
	}

	/* resize the hash table if needed (NTUP_PER_BUCKET exceeded) */
	ExecHashUpdate(hashtable);
	ExecHashIncreaseNumBucketsIfNeeded(hashtable);

 post_resize:
	if (is_shared)
	{
		Assert(BarrierPhase(barrier) == PHJ_PHASE_RESIZING);
		BarrierWait(barrier, WAIT_EVENT_HASH_RESIZING);
		Assert(BarrierPhase(barrier) == PHJ_PHASE_REINSERTING);
	}

 reinsert:
	/*
	 * If the table was resized, insert tuples into the new buckets.  If it
	 * wasn't resized, then this will be a no-op.
	 */
	ExecHashUpdate(hashtable);
	ExecHashReinsertHashtableIfNeeded(hashtable);

	if (is_shared)
	{
		Assert(BarrierPhase(barrier) == PHJ_PHASE_REINSERTING);
		BarrierWait(barrier, WAIT_EVENT_HASH_REINSERTING);
		Assert(BarrierPhase(barrier) == PHJ_PHASE_PROBING);
	}

 finish:
	if (is_shared)
	{
		/*
		 * Building has finished.  The other workers may be probing or
		 * processing unmatched tuples for the initial batch, or dealing with
		 * later batches.  The next synchronization point is in ExecHashJoin's
		 * HJ_BUILD_HASHTABLE case, which will figure that out and synchronize
		 * this backend's local state machine with the phase.
		 */
		Assert(BarrierPhase(barrier) >= PHJ_PHASE_PROBING);

		LWLockAcquire(&hashtable->shared->chunk_lock, LW_EXCLUSIVE);

		/*
		 * Make sure the shared tuple count includes every tuple I loaded, and
		 * I see every tuple loaded by everyone, so that all participants
		 * agree on whether we can take the empty table optimization.
		 */
		if (hashtable->current_chunk != NULL)
			hashtable->shared->ntuples += hashtable->current_chunk->ntuples;
		hashtable->totalTuples = hashtable->shared->ntuples;

		/*
		 * Also update this backend's pointers and batch information.  In the
		 * switch cases above we can do this without acquiring a lock, because
		 * we know that the current phase is not one that allows concurrent
		 * changes.  Here we don't know the current phase, so we need
		 * interlocking against ExecHashLoadSharedTuple() to get a consistent
		 * snapshot.
		 */
		ExecHashUpdate(hashtable);

		LWLockRelease(&hashtable->shared->chunk_lock);
	}

	/* must provide our own instrumentation support */
	if (node->ps.instrument)
		InstrStopNode(node->ps.instrument, hashtable->partialTuples);

	/*
	 * We do not return the hash table directly because it's not a subtype of
	 * Node, and so would violate the MultiExecProcNode API.  Instead, our
	 * parent Hashjoin node is expected to know how to fish it out of our node
	 * state.  Ugly but not really worth cleaning up, since Hashjoin knows
	 * quite a bit more about Hash besides that.
	 */
	return NULL;
}

/* ----------------------------------------------------------------
 *		ExecInitHash
 *
 *		Init routine for Hash node
 * ----------------------------------------------------------------
 */
HashState *
ExecInitHash(Hash *node, EState *estate, int eflags)
{
	HashState  *hashstate;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	hashstate = makeNode(HashState);
	hashstate->ps.plan = (Plan *) node;
	hashstate->ps.state = estate;
	hashstate->ps.ExecProcNode = ExecHash;
	hashstate->hashtable = NULL;
	hashstate->hashkeys = NIL;	/* will be set by parent HashJoin */

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &hashstate->ps);

	/*
	 * initialize our result slot
	 */
	ExecInitResultTupleSlot(estate, &hashstate->ps);

	/*
	 * initialize child expressions
	 */
	hashstate->ps.qual =
		ExecInitQual(node->plan.qual, (PlanState *) hashstate);

	/*
	 * initialize child nodes
	 */
	outerPlanState(hashstate) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * initialize tuple type. no need to initialize projection info because
	 * this node doesn't do projections
	 */
	ExecAssignResultTypeFromTL(&hashstate->ps);
	hashstate->ps.ps_ProjInfo = NULL;

	return hashstate;
}

/* ---------------------------------------------------------------
 *		ExecEndHash
 *
 *		clean up routine for Hash node
 * ----------------------------------------------------------------
 */
void
ExecEndHash(HashState *node)
{
	PlanState  *outerPlan;

	/*
	 * free exprcontext
	 */
	ExecFreeExprContext(&node->ps);

	/*
	 * shut down the subplan
	 */
	outerPlan = outerPlanState(node);
	ExecEndNode(outerPlan);
}


/* ----------------------------------------------------------------
 *		ExecHashTableCreate
 *
 *		create an empty hashtable data structure for hashjoin.
 * ----------------------------------------------------------------
 */
HashJoinTable
ExecHashTableCreate(HashState *state, List *hashOperators, bool keepNulls)
{
	Hash	   *node;
	HashJoinTable hashtable;
	SharedHashJoinTable shared_hashtable;
	Plan	   *outerNode;
	size_t		space_allowed;
	int			nbuckets;
	int			nbatch;
	double		rows;
	int			num_skew_mcvs;
	int			log2_nbuckets;
	int			nkeys;
	int			i;
	ListCell   *ho;
	MemoryContext oldcxt;

	/*
	 * Get information about the size of the relation to be hashed (it's the
	 * "outer" subtree of this node, but the inner relation of the hashjoin).
	 * Compute the appropriate size of the hash table.
	 */
	node = (Hash *) state->ps.plan;
	outerNode = outerPlan(node);

	/*
	 * If this is shared hash table with a partial plan, then we can't use
	 * outerNode->plan_rows to estimate its size.  We need an estimate of the
	 * total number of rows from all partial plans running.
	 */
	rows = node->plan.parallel_aware ? node->rows_total : outerNode->plan_rows;

	shared_hashtable = state->shared_table_data;
	ExecChooseHashTableSize(rows, outerNode->plan_width,
							OidIsValid(node->skewTable),
							shared_hashtable != NULL,
							shared_hashtable != NULL ?
							shared_hashtable->planned_participants - 1 : 0,
							&space_allowed,
							&nbuckets, &nbatch, &num_skew_mcvs);

	/* nbuckets must be a power of 2 */
	log2_nbuckets = my_log2(nbuckets);
	Assert(nbuckets == (1 << log2_nbuckets));

	/*
	 * Initialize the hash table control block.
	 *
	 * The hashtable control block is just palloc'd from the executor's
	 * per-query memory context.
	 */
	hashtable = (HashJoinTable) palloc(sizeof(HashJoinTableData));
	hashtable->nbuckets = nbuckets;
	hashtable->nbuckets_original = nbuckets;
	hashtable->nbuckets_optimal = nbuckets;
	hashtable->log2_nbuckets = log2_nbuckets;
	hashtable->log2_nbuckets_optimal = log2_nbuckets;
	hashtable->buckets = NULL;
	hashtable->keepNulls = keepNulls;
	hashtable->skewBucket = NULL;
	hashtable->skewBucketLen = 0;
	hashtable->nSkewBuckets = 0;
	hashtable->skewBucketNums = NULL;
	hashtable->nbatch = nbatch;
	hashtable->curbatch = 0;
	hashtable->nbatch_original = nbatch;
	hashtable->nbatch_outstart = nbatch;
	hashtable->growEnabled = true;
	hashtable->partialTuples = 0;
	hashtable->totalTuples = 0;
	hashtable->skewTuples = 0;
	hashtable->innerBatchFile = NULL;
	hashtable->outerBatchFile = NULL;
	hashtable->spaceUsed = 0;
	hashtable->spacePeak = 0;
	hashtable->spaceAllowed = space_allowed;
	hashtable->spaceUsedSkew = 0;
	hashtable->spaceAllowedSkew =
		hashtable->spaceAllowed * SKEW_WORK_MEM_PERCENT / 100;
	hashtable->chunks = NULL;
	hashtable->unmatched_chunks = NULL;
	hashtable->chunks_to_reinsert = NULL;
	hashtable->current_chunk = NULL;
	hashtable->area = state->ps.state->es_query_dsa;
	hashtable->shared = state->shared_table_data;
	hashtable->shared_inner_batches = state->shared_inner_batches;
	hashtable->shared_outer_batches = state->shared_outer_batches;
	hashtable->detached_early = false;

#ifdef HJDEBUG
	printf("Hashjoin %p: initial nbatch = %d, nbuckets = %d\n",
		   hashtable, nbatch, nbuckets);
#endif

	/*
	 * Get info about the hash functions to be used for each hash key. Also
	 * remember whether the join operators are strict.
	 */
	nkeys = list_length(hashOperators);
	hashtable->outer_hashfunctions =
		(FmgrInfo *) palloc(nkeys * sizeof(FmgrInfo));
	hashtable->inner_hashfunctions =
		(FmgrInfo *) palloc(nkeys * sizeof(FmgrInfo));
	hashtable->hashStrict = (bool *) palloc(nkeys * sizeof(bool));
	i = 0;
	foreach(ho, hashOperators)
	{
		Oid			hashop = lfirst_oid(ho);
		Oid			left_hashfn;
		Oid			right_hashfn;

		if (!get_op_hash_functions(hashop, &left_hashfn, &right_hashfn))
			elog(ERROR, "could not find hash function for hash operator %u",
				 hashop);
		fmgr_info(left_hashfn, &hashtable->outer_hashfunctions[i]);
		fmgr_info(right_hashfn, &hashtable->inner_hashfunctions[i]);
		hashtable->hashStrict[i] = op_strict(hashop);
		i++;
	}

	/*
	 * Create temporary memory contexts in which to keep the hashtable working
	 * storage if using private hash table.  See notes in executor/hashjoin.h.
	 */
	hashtable->hashCxt = AllocSetContextCreate(CurrentMemoryContext,
											   "HashTableContext",
											   ALLOCSET_DEFAULT_SIZES);

	hashtable->batchCxt = AllocSetContextCreate(hashtable->hashCxt,
												"HashBatchContext",
												ALLOCSET_DEFAULT_SIZES);

	/* Allocate data that will live for the life of the hashjoin */

	oldcxt = MemoryContextSwitchTo(hashtable->hashCxt);

	if (nbatch > 1 && !HashJoinTableIsShared(hashtable))
	{
		/*
		 * allocate and initialize the file arrays in hashCxt
		 */
		hashtable->innerBatchFile = (BufFile **)
			palloc0(nbatch * sizeof(BufFile *));
		hashtable->outerBatchFile = (BufFile **)
			palloc0(nbatch * sizeof(BufFile *));
		/* The files will not be opened until needed... */
		/* ... but make sure we have temp tablespaces established for them */
		PrepareTempTablespaces();
	}

	MemoryContextSwitchTo(oldcxt);

	if (HashJoinTableIsShared(hashtable))
	{
		Barrier *barrier;

		/*
		 * Attach to the barrier.  The corresponding detach operation is in
		 * ExecHashTableDetach.
		 */
		barrier = &hashtable->shared->barrier;

		BarrierAttach(barrier);
		LeaderGateAttach(&hashtable->shared->leader_gate);

		/*
		 * So far we have no idea whether there are any other participants, and
		 * if so, what phase they are working on.  The only thing we care about
		 * at this point is whether someone has already created the shared
		 * hash table yet.  If not, one backend will be elected to do that
		 * now.
		 */
		if (BarrierPhase(barrier) == PHJ_PHASE_BEGINNING)
		{
			if (BarrierWait(barrier, WAIT_EVENT_HASH_BEGINNING))
			{
				Size bytes;
				HashJoinBucketHead *buckets;
				int i;
				SharedHashJoinTable shared;
				dsa_area *area;

				/* Serial phase: create the shared hash table */
				shared = hashtable->shared;
				area = hashtable->area;
				bytes = nbuckets * sizeof(HashJoinBucketHead);

				/* Allocate the hash table buckets. */
				shared->buckets = dsa_allocate(area, bytes);

				/* Initialize the hash table buckets to empty. */
				buckets = dsa_get_address(area, shared->buckets);
				for (i = 0; i < nbuckets; ++i)
					dsa_pointer_atomic_init(&buckets[i].shared,
											InvalidDsaPointer);

				/* Initialize the rest of parallel_state. */
				hashtable->shared->nbuckets = nbuckets;
				hashtable->shared->log2_nbuckets = log2_nbuckets;
				hashtable->shared->size = bytes;
				hashtable->shared->nbatch = hashtable->nbatch;

				/* Prepare for the skew optimization in one backend. */
				if (nbatch > 1)
					ExecHashBuildSkewHash(hashtable, node, num_skew_mcvs);

				/*
				 * The backend-local pointers in hashtable will be set up by
				 * ExecHashUpdate, at each point where they might have
				 * changed.
				 */
			}
			Assert(BarrierPhase(&hashtable->shared->barrier) ==
				   PHJ_PHASE_CREATING);
			/* The next synchronization point is in MultiExecHash. */
		}

		/*
		 * If using the skew optimization, each backend will need to ask for
		 * skew space from the shared space allowance as needed.  Initially
		 * every backend starts out with none.
		 */
		hashtable->spaceAllowedSkew = 0;
	}
	else
	{
		/*
		 * Prepare context for the first-scan space allocations; allocate the
		 * hashbucket array therein, and set each bucket "empty".
		 */
		MemoryContextSwitchTo(hashtable->batchCxt);

		hashtable->buckets = (HashJoinBucketHead *)
			palloc0(nbuckets * sizeof(HashJoinBucketHead));
		hashtable->spaceUsed = nbuckets * sizeof(HashJoinTuple);
		hashtable->spacePeak = hashtable->spaceUsed;

		MemoryContextSwitchTo(oldcxt);

		/*
		 * Set up for skew optimization, if possible and there's a need for
		 * more than one batch.  (In a one-batch join, there's no point in
		 * it.)
		 */
		if (nbatch > 1)
			ExecHashBuildSkewHash(hashtable, node, num_skew_mcvs);
	}

	return hashtable;
}


/*
 * Compute appropriate size for hashtable given the estimated size of the
 * relation to be hashed (number of rows and average row width).
 *
 * This is exported so that the planner's costsize.c can use it.
 */

/* Target bucket loading (tuples per bucket) */
#define NTUP_PER_BUCKET			1

void
ExecChooseHashTableSize(double ntuples, int tupwidth, bool useskew,
						bool shared, int parallel_workers,
						size_t *space_allowed,
						int *numbuckets,
						int *numbatches,
						int *num_skew_mcvs)
{
	int			tupsize;
	double		inner_rel_bytes;
	long		bucket_bytes;
	long		hash_table_bytes;
	long		skew_table_bytes;
	long		max_pointers;
	long		mppow2;
	int			nbatch = 1;
	int			nbuckets;
	double		dbuckets;

	/* Force a plausible relation size if no info */
	if (ntuples <= 0.0)
		ntuples = 1000.0;

	/* Estimate tupsize based on footprint of tuple in hashtable. */
	tupsize = HJTUPLE_OVERHEAD +
		MAXALIGN(SizeofMinimalTupleHeader) +
		MAXALIGN(tupwidth);

	/* Estimate total size including chunk overhead */
	if (tupsize > HASH_CHUNK_THRESHOLD)
	{
		/* Large tuples have a chunk each */
		inner_rel_bytes = ntuples * (tupsize + HASH_CHUNK_HEADER_SIZE);
	}
	else
	{
		int64 tuples;
		int tuples_per_chunk;
		int chunks;

		/* Small tuples get packed into fixed sized chunks */
		tuples_per_chunk = (HASH_CHUNK_SIZE - HASH_CHUNK_HEADER_SIZE) / tupsize;
		tuples = (int64) ntuples;
		chunks = tuples / tuples_per_chunk + (tuples % tuples_per_chunk != 0);
		inner_rel_bytes = HASH_CHUNK_SIZE * chunks;
	}

	/*
	 * Target in-memory hashtable size is work_mem kilobytes.  Shared hash
	 * tables are allowed to multiply work_mem by the number of participants,
	 * since other non-shared memory based plans allow each participant to use
	 * work_mem for the same total.
	 */
	hash_table_bytes = work_mem * 1024L;
	if (shared && parallel_workers > 0)
		hash_table_bytes *= parallel_workers + 1;	/* one for the leader */
	*space_allowed = hash_table_bytes;

	/*
	 * If skew optimization is possible, estimate the number of skew buckets
	 * that will fit in the memory allowed, and decrement the assumed space
	 * available for the main hash table accordingly.
	 *
	 * We make the optimistic assumption that each skew bucket will contain
	 * one inner-relation tuple.  If that turns out to be low, we will recover
	 * at runtime by reducing the number of skew buckets.
	 *
	 * hashtable->skewBucket will have up to 8 times as many HashSkewBuckets
	 * as the number of MCVs we allow, since ExecHashBuildSkewHash will round
	 * up to the next power of 2 and then multiply by 4 to reduce collisions.
	 */
	if (useskew)
	{
		skew_table_bytes = hash_table_bytes * SKEW_WORK_MEM_PERCENT / 100;

		/*----------
		 * Divisor is:
		 * size of a hash tuple +
		 * worst-case size of skewBucket[] per MCV +
		 * size of skewBucketNums[] entry
		 *----------
		 */
		*num_skew_mcvs = skew_table_bytes / (tupsize +
											 (8 * sizeof(HashSkewBucket)) +
											 sizeof(int));
		if (*num_skew_mcvs > 0)
			hash_table_bytes -= skew_table_bytes;
	}
	else
		*num_skew_mcvs = 0;

	/*
	 * Set nbuckets to achieve an average bucket load of NTUP_PER_BUCKET when
	 * memory is filled, assuming a single batch; but limit the value so that
	 * the pointer arrays we'll try to allocate do not exceed work_mem nor
	 * MaxAllocSize.
	 *
	 * Note that both nbuckets and nbatch must be powers of 2 to make
	 * ExecHashGetBucketAndBatch fast.
	 */
	max_pointers = (work_mem * 1024L) / sizeof(HashJoinBucketHead);
	max_pointers = Min(max_pointers, MaxAllocSize / sizeof(HashJoinBucketHead));
	/* If max_pointers isn't a power of 2, must round it down to one */
	mppow2 = 1L << my_log2(max_pointers);
	if (max_pointers != mppow2)
		max_pointers = mppow2 / 2;

	/* Also ensure we avoid integer overflow in nbatch and nbuckets */
	/* (this step is redundant given the current value of MaxAllocSize) */
	max_pointers = Min(max_pointers, INT_MAX / 2);

	dbuckets = ceil(ntuples / NTUP_PER_BUCKET);
	dbuckets = Min(dbuckets, max_pointers);
	nbuckets = (int) dbuckets;
	/* don't let nbuckets be really small, though ... */
	nbuckets = Max(nbuckets, 1024);
	/* ... and force it to be a power of 2. */
	nbuckets = 1 << my_log2(nbuckets);

	/*
	 * If there's not enough space to store the projected number of tuples and
	 * the required bucket headers, we will need multiple batches.
	 */
	bucket_bytes = sizeof(HashJoinBucketHead) * nbuckets;
	if (inner_rel_bytes + bucket_bytes > hash_table_bytes)
	{
		/* We'll need multiple batches */
		long		lbuckets;
		double		dbatch;
		int			minbatch;
		long		bucket_size;

		/*
		 * Estimate the number of buckets we'll want to have when work_mem is
		 * entirely full.  Each bucket will contain a bucket pointer plus
		 * NTUP_PER_BUCKET tuples, whose projected size already includes
		 * overhead for the hash code, pointer to the next tuple, etc.
		 */
		bucket_size = (tupsize * NTUP_PER_BUCKET + sizeof(HashJoinBucketHead));
		lbuckets = 1L << my_log2(hash_table_bytes / bucket_size);
		lbuckets = Min(lbuckets, max_pointers);
		nbuckets = (int) lbuckets;
		nbuckets = 1 << my_log2(nbuckets);
		bucket_bytes = nbuckets * sizeof(HashJoinBucketHead);

		/*
		 * Buckets are simple pointers to hashjoin tuples, while tupsize
		 * includes the pointer, hash code, and MinimalTupleData.  So buckets
		 * should never really exceed 25% of work_mem (even for
		 * NTUP_PER_BUCKET=1); except maybe for work_mem values that are not
		 * 2^N bytes, where we might get more because of doubling. So let's
		 * look for 50% here.
		 */
		Assert(bucket_bytes <= hash_table_bytes / 2);

		/* Calculate required number of batches. */
		dbatch = ceil(inner_rel_bytes / (hash_table_bytes - bucket_bytes));
		dbatch = Min(dbatch, max_pointers);
		minbatch = (int) dbatch;
		nbatch = 2;
		while (nbatch < minbatch)
			nbatch <<= 1;
	}

	Assert(nbuckets > 0);
	Assert(nbatch > 0);

	*numbuckets = nbuckets;
	*numbatches = nbatch;
}

/*
 * Detach from the shared hash table, freeing all memory if we are the last to
 * detach.
 */
void
ExecHashTableDetach(HashJoinTable hashtable)
{
	if (HashJoinTableIsShared(hashtable) && !hashtable->detached_early)
	{
		/* Make sure any shared temp files are closed. */
		sts_end_write_all_partitions(hashtable->shared_inner_batches);
		sts_end_write_all_partitions(hashtable->shared_outer_batches);
		sts_end_partial_scan(hashtable->shared_inner_batches);
		sts_end_partial_scan(hashtable->shared_outer_batches);

		/*
		 * Instead of waiting at the end of a hash join for all participants
		 * to finish, we detach and let the last to detach clean up the shared
		 * resources.  This avoids unnecessary waiting at the end of single
		 * batch probes.
		 */
		if (BarrierDetach(&hashtable->shared->barrier))
		{
			/* Serial: free the buckets and chunks */
			if (DsaPointerIsValid(hashtable->shared->buckets))
			{
				dsa_pointer chunk_shared;

				/*
				 * We could just forget about the memory, since the whole area
				 * will be freed at the end of the query anyway.  But that
				 * wouldn't work for rescans, where we'll be allocating a
				 * whole hashtable again, creating a leak.  Perhaps we could
				 * consider moving all the chunks to a freelist here for reuse
				 * in the cast of a rescan, so that we can avoid the cost of
				 * one backend freeing all chunks in the common case.
				 */
				dsa_free(hashtable->area, hashtable->shared->buckets);
				hashtable->shared->buckets = InvalidDsaPointer;
				hashtable->shared->chunk_work_queue = hashtable->shared->chunks;
				hashtable->shared->chunks = InvalidDsaPointer;
				while (ExecHashPopChunkQueue(hashtable, &chunk_shared) != NULL)
					dsa_free(hashtable->area, chunk_shared);
				ExecHashFreeSkewTable(hashtable);
			}
		}
		hashtable->shared = NULL;
	}
}

/* ----------------------------------------------------------------
 *		ExecHashTableDestroy
 *
 *		destroy a hash table
 * ----------------------------------------------------------------
 */
void
ExecHashTableDestroy(HashJoinTable hashtable)
{
	int			i;

	/* If shared, clean up shared memory and detach. */
	ExecHashTableDetach(hashtable);

	/*
	 * Make sure all the temp files are closed.  We skip batch 0, since it
	 * can't have any temp files (and the arrays might not even exist if
	 * nbatch is only 1).  Shared hash tables don't use these files.
	 */
	for (i = 1; i < hashtable->nbatch; i++)
	{
		if (hashtable->innerBatchFile != NULL && hashtable->innerBatchFile[i])
			BufFileClose(hashtable->innerBatchFile[i]);
		if (hashtable->outerBatchFile != NULL && hashtable->outerBatchFile[i])
			BufFileClose(hashtable->outerBatchFile[i]);
	}

	/* Release working memory (batchCxt is a child, so it goes away too) */
	MemoryContextDelete(hashtable->hashCxt);

	/* And drop the control block */
	pfree(hashtable);
}

/*
 * ExecHashIncreaseNumBatches
 *		increase the original number of batches in order to reduce
 *		current memory consumption.  The actual work of shrinking the
 *		hash table is done in ExecHashShrink().
 */
static void
ExecHashIncreaseNumBatches(HashJoinTable hashtable, int nbatch)
{
	int			oldnbatch = hashtable->nbatch;
	MemoryContext oldcxt;

	/* safety check to avoid overflow */
	if (oldnbatch > MAX_BATCHES_BEFORE_INCREASES_STOP)
		return;

	Assert(nbatch > 1);

	if (HashJoinTableIsShared(hashtable))
	{
		/*
		 * For shared hash tables, we don't need to manage batch file arrays
		 * and temporary tablespaces because the SharedTuplestore objects look
		 * after that for us.  We just note the new number.
		 */
		hashtable->nbatch = nbatch;
		return;
	}

#ifdef HJDEBUG
	printf("Hashjoin %p: increasing nbatch to %d because space = %zu\n",
		   hashtable, nbatch, hashtable->spaceUsed);
#endif

	oldcxt = MemoryContextSwitchTo(hashtable->hashCxt);

	if (hashtable->innerBatchFile == NULL)
	{
		/* we had no file arrays before */
		hashtable->innerBatchFile = (BufFile **)
			palloc0(nbatch * sizeof(BufFile *));
		hashtable->outerBatchFile = (BufFile **)
			palloc0(nbatch * sizeof(BufFile *));
		/* time to establish the temp tablespaces, too */
		PrepareTempTablespaces();
	}
	else
	{
		/* enlarge arrays and zero out added entries */
		hashtable->innerBatchFile = (BufFile **)
			repalloc(hashtable->innerBatchFile, nbatch * sizeof(BufFile *));
		hashtable->outerBatchFile = (BufFile **)
			repalloc(hashtable->outerBatchFile, nbatch * sizeof(BufFile *));
		MemSet(hashtable->innerBatchFile + oldnbatch, 0,
			   (nbatch - oldnbatch) * sizeof(BufFile *));
		MemSet(hashtable->outerBatchFile + oldnbatch, 0,
			   (nbatch - oldnbatch) * sizeof(BufFile *));
	}

	MemoryContextSwitchTo(oldcxt);

	hashtable->nbatch = nbatch;
}

/*
 * Process the queue of chunks whose tuples need to be redistributed into the
 * correct batches until it is empty.  In the best case this will shrink the
 * hash table, keeping about half of the tuples in memory and sending the rest
 * to a future batch.
 *
 * In a degenerate case, this will send ALL or NONE of the tuples out of the
 * current batch, which is convincing evidence that no amount of further
 * partitioning of this data will help.
 */
static void
ExecHashShrink(HashJoinTable hashtable)
{
	long		ninmemory;
	long		nfreed;
	dsa_pointer chunk_shared;
	HashMemoryChunk chunk;
	bool		is_shared = HashJoinTableIsShared(hashtable);

	if (is_shared)
	{
		/*
		 * Since a newly launched participant could arrive while shrinking is
		 * already underway, we need to be able to synchronize with the
		 * existing work.
		 */
		switch (PHJ_SHRINK_PHASE(BarrierPhase(&hashtable->shared->shrink_barrier)))
		{
			case PHJ_SHRINK_PHASE_BEGINNING:
				if (BarrierWait(&hashtable->shared->shrink_barrier,
								WAIT_EVENT_HASH_SHRINK_BEGINNING))
				{
					int		i;

					/* Serial phase: One participant clears buckets. */
					for (i = 0; i < hashtable->nbuckets; ++i)
						dsa_pointer_atomic_write(&hashtable->buckets[i].shared,
												 InvalidDsaPointer);
				}
				/* Fall through */
			case PHJ_SHRINK_PHASE_CLEARING:
				/* Wait for clearing to finish above. */
				BarrierWait(&hashtable->shared->shrink_barrier,
							WAIT_EVENT_HASH_SHRINK_CLEARING);
				/* Fall through */
			case PHJ_SHRINK_PHASE_WORKING:
				/* Run main shrinking code below. */
				break;
			case PHJ_SHRINK_PHASE_DECIDING:
				/*
				 * Tidier to wait and return here than goto end of function
				 * where there is another one of these.
				 */
				BarrierWait(&hashtable->shared->shrink_barrier,
							WAIT_EVENT_HASH_SHRINK_DECIDING);
				return;
		}
	}
	else
	{
		/* Clear the hash table buckets. */
		memset(hashtable->buckets, 0,
			   sizeof(HashJoinBucketHead) * hashtable->nbuckets);
	}

	/* Pop first chunk from the shrink queue. */
	if (is_shared)
		chunk = ExecHashPopChunkQueue(hashtable, &chunk_shared);
	else
	{
		chunk = hashtable->chunks;
		hashtable->chunks = NULL;
	}
	ninmemory = nfreed = 0;

	while (chunk != NULL)
	{
		/* position within the buffer (up to oldchunks->used) */
		size_t		idx = 0;

		/* process all tuples stored in this chunk (and then free it) */
		while (idx < chunk->used)
		{
			HashJoinTuple hashTuple = (HashJoinTuple) (chunk->data + idx);
			MinimalTuple tuple = HJTUPLE_MINTUPLE(hashTuple);
			int			bucketno;
			int			batchno;

			ninmemory++;

			ExecHashGetBucketAndBatch(hashtable, hashTuple->hashvalue,
									  &bucketno, &batchno);

			if (batchno == hashtable->curbatch)
			{
				/* keep tuple in memory - copy it into the new chunk */
				HashJoinTuple copyTuple;
				dsa_pointer shared = InvalidDsaPointer;

				if (is_shared)
					copyTuple = ExecHashLoadSharedTuple(hashtable, tuple, &shared,
														false);
				else
					copyTuple = ExecHashLoadPrivateTuple(hashtable, tuple, false);

				/* and add it back to the appropriate bucket */
				copyTuple->hashvalue = hashTuple->hashvalue;
				ExecHashInsertTupleIntoBucket(hashtable,
											  &hashtable->buckets[bucketno],
											  copyTuple, shared);
			}
			else
			{
				/* dump it out */
				Assert(batchno > hashtable->curbatch);
				if (is_shared)
					sts_puttuple(hashtable->shared_inner_batches, batchno,
								 &hashTuple->hashvalue,
								 tuple);
				else
					ExecHashJoinSaveTuple(tuple,
										  hashTuple->hashvalue,
										  &hashtable->innerBatchFile[batchno]);

				nfreed++;
			}

			/* next tuple in this chunk */
			idx += MAXALIGN(HJTUPLE_OVERHEAD + tuple->t_len);

			/* allow this loop to be cancellable */
			CHECK_FOR_INTERRUPTS();
		}

		/* Free chunk and pop next from the queue. */
		if (is_shared)
		{
			Size size = chunk->maxlen + HASH_CHUNK_HEADER_SIZE;

			Assert(chunk == dsa_get_address(hashtable->area, chunk_shared));
			dsa_free(hashtable->area, chunk_shared);

			LWLockAcquire(&hashtable->shared->chunk_lock, LW_EXCLUSIVE);
			Assert(hashtable->shared->size >= size);
			hashtable->shared->size -= size;
			hashtable->shared->nfreed += nfreed;
			hashtable->shared->ninmemory += ninmemory;
			nfreed = 0;
			ninmemory = 0;
			chunk = ExecHashPopChunkQueueUnlocked(hashtable, &chunk_shared);
			hashtable->spaceUsed = hashtable->shared->size;
			LWLockRelease(&hashtable->shared->chunk_lock);
		}
		else
		{
			Size size = chunk->maxlen + HASH_CHUNK_HEADER_SIZE;
			HashMemoryChunk nextchunk = chunk->next.unshared;

			Assert(hashtable->spaceUsed >= size);
			hashtable->spaceUsed -= size;
			pfree(chunk);
			chunk = nextchunk;
		}
	}

#ifdef HJDEBUG
	printf("Hashjoin %p: freed %ld of %ld tuples, space now %zu\n",
		   hashtable, nfreed, ninmemory, hashtable->spaceUsed);
#endif

	/*
	 * If we dumped out either all or none of the tuples in the table, disable
	 * further expansion of nbatch.  This situation implies that we have
	 * enough tuples of identical hashvalues to overflow spaceAllowed.
	 * Increasing nbatch will not fix it since there's no way to subdivide the
	 * group any more finely. We have to just gut it out and hope the server
	 * has enough RAM.
	 */
	if (is_shared)
	{
		/*
		 * Wait until all have finished shrinking chunks.  We need to do that
		 * because we need the total tuple counts before we can decide whether
		 * to prevent further attempts at shrinking.
		 */
		if (BarrierWait(&hashtable->shared->shrink_barrier,
						WAIT_EVENT_HASH_SHRINK_WORKING))
		{
			/* Serial phase: one participant decides whether that paid off. */
			if (hashtable->shared->nfreed == 0 ||
				hashtable->shared->nfreed == hashtable->shared->ninmemory)
			{
				hashtable->shared->grow_enabled = false;
#ifdef HJDEBUG
			printf("Hashjoin %p: disabling further increase of nbatch\n",
				   hashtable);
#endif
			}
			hashtable->shared->shrink_needed = false;
		}

		/* Wait for above decision to be made. */
		Assert(PHJ_SHRINK_PHASE(BarrierPhase(&hashtable->shared->shrink_barrier)) ==
			   PHJ_SHRINK_PHASE_DECIDING);
		BarrierWait(&hashtable->shared->shrink_barrier,
					WAIT_EVENT_HASH_SHRINK_DECIDING);
		Assert(PHJ_SHRINK_PHASE(BarrierPhase(&hashtable->shared->shrink_barrier)) ==
			   PHJ_SHRINK_PHASE_BEGINNING);
	}
	else
	{
		if (nfreed == 0 || nfreed == ninmemory)
		{
			hashtable->growEnabled = false;
#ifdef HJDEBUG
			printf("Hashjoin %p: disabling further increase of nbatch\n",
				   hashtable);
#endif
		}
	}
}

/*
 * Update the local hashtable with the current pointers and sizes from
 * hashtable->shared.
 */
void
ExecHashUpdate(HashJoinTable hashtable)
{
	if (!HashJoinTableIsShared(hashtable))
		return;

	/*
	 * It's only safe to read these values from shared memory if we have
	 * interlocking against concurrent changes.  That is achieved either by
	 * holding chunk_lock or by being the backend 'selected' to run a serial
	 * phase by the main barrier.  Unfortunately we have no way to assert that
	 * here.
	 */

	/* Read the current sizes. */
	hashtable->spaceUsed = hashtable->shared->size;
	if (hashtable->spaceUsed > hashtable->spacePeak)
		hashtable->spacePeak = hashtable->spaceUsed;
	hashtable->nbuckets = hashtable->shared->nbuckets;
	hashtable->log2_nbuckets = my_log2(hashtable->nbuckets);
	hashtable->nSkewBuckets = hashtable->shared->num_skew_buckets;
	hashtable->skewBucketLen = hashtable->shared->skew_bucket_len;

	/* Get the addresses where the arrays are mapped in this backend. */
	hashtable->buckets = (HashJoinBucketHead *)
		dsa_get_address(hashtable->area, hashtable->shared->buckets);
	hashtable->skewBucketNums = (int *)
		dsa_get_address(hashtable->area, hashtable->shared->skew_bucket_nums);
	hashtable->skewBucket = (HashSkewBucket *)
		dsa_get_address(hashtable->area, hashtable->shared->skew_buckets);

	/* Synchronize with the current batch and number of batches. */
	hashtable->curbatch =
		PHJ_PHASE_TO_BATCHNO(BarrierPhase(&hashtable->shared->barrier));
	if (hashtable->shared->nbatch > hashtable->nbatch)
		ExecHashIncreaseNumBatches(hashtable, hashtable->shared->nbatch);
}

/*
 * ExecHashIncreaseNumBuckets
 *		increase the original number of buckets in order to reduce
 *		number of tuples per bucket
 */
static void
ExecHashIncreaseNumBucketsIfNeeded(HashJoinTable hashtable)
{
	/* do nothing if not an increase (it's called increase for a reason) */
	if (hashtable->nbuckets >= hashtable->nbuckets_optimal)
		return;

	/* can't increase number of buckets once we have multiple batches */
	if (hashtable->nbatch > 1)
		return;

#ifdef HJDEBUG
	printf("Hashjoin %p: increasing nbuckets %d => %d\n",
		   hashtable, hashtable->nbuckets, hashtable->nbuckets_optimal);
#endif

	/* account for the increase in space that will be used by buckets */
	hashtable->spaceUsed += sizeof(HashJoinBucketHead) *
		(hashtable->nbuckets_optimal - hashtable->nbuckets);
	if (hashtable->spaceUsed > hashtable->spacePeak)
		hashtable->spacePeak = hashtable->spaceUsed;

	hashtable->nbuckets = hashtable->nbuckets_optimal;
	hashtable->log2_nbuckets = hashtable->log2_nbuckets_optimal;

	Assert(hashtable->nbuckets > 1);
	Assert(hashtable->nbuckets <= (INT_MAX / 2));
	Assert(hashtable->nbuckets == (1 << hashtable->log2_nbuckets));

	/*
	 * Just reallocate the proper number of buckets - we don't need to walk
	 * through them - we can walk the dense-allocated chunks (just like in
	 * ExecHashIncreaseNumBatches, but without all the copying into new
	 * chunks).  That happens in ExecHashReinsertHashtableIfNeeded.
	 */
	if (HashJoinTableIsShared(hashtable))
	{
		HashJoinBucketHead *buckets;
		int i;

		/*
		 * Only one backend enters this function so it can safely change the
		 * array while other backends wait.
		 */
		Assert(BarrierPhase(&hashtable->shared->barrier) == PHJ_PHASE_RESIZING);

		/* Free the existing bucket array. */
		dsa_free(hashtable->area, hashtable->shared->buckets);

		/*
		 * Share the bucket array and size information, which all backends
		 * will pick up when they run ExecHashUpdate.
		 */
		hashtable->shared->size = hashtable->spaceUsed;
		hashtable->shared->nbuckets = hashtable->nbuckets;
		hashtable->shared->log2_nbuckets = hashtable->log2_nbuckets;
		hashtable->shared->buckets =
			dsa_allocate(hashtable->area,
						 hashtable->nbuckets * sizeof(HashJoinBucketHead));

		/* Initialize the new buckets. */
		buckets = dsa_get_address(hashtable->area,
								  hashtable->shared->buckets);
		for (i = 0; i < hashtable->nbuckets; ++i)
			dsa_pointer_atomic_init(&buckets[i].shared, InvalidDsaPointer);

		/* ExecHashReinsert needs to process all chunks. */
		hashtable->shared->chunk_work_queue = hashtable->shared->chunks;
	}
	else
	{
		hashtable->buckets = (HashJoinBucketHead *)
			repalloc(hashtable->buckets,
					 hashtable->nbuckets * sizeof(HashJoinBucketHead));
		hashtable->chunks_to_reinsert = hashtable->chunks;
		memset(hashtable->buckets, 0,
			   hashtable->nbuckets * sizeof(HashJoinBucketHead));
	}
}

/*
 * ExecHashReinsert
 *		reinsert the tuples from all chunks into the hashtable after increasing
 *		the number of buckets
 */
static void
ExecHashReinsertHashtableIfNeeded(HashJoinTable hashtable)
{
	HashMemoryChunk chunk;
	dsa_pointer chunk_shared;
	bool is_shared = HashJoinTableIsShared(hashtable);

	/* scan through all tuples in all chunks to rebuild the hash table */
	if (is_shared)
		chunk = ExecHashPopChunkQueue(hashtable, &chunk_shared);
	else
		chunk = hashtable->chunks_to_reinsert;

	while (chunk != NULL)
	{
		/* process all tuples stored in this chunk */
		size_t		idx = 0;

		while (idx < chunk->used)
		{
			dsa_pointer hashTuple_shared = InvalidDsaPointer;
			HashJoinTuple hashTuple = (HashJoinTuple) (chunk->data + idx);
			int			bucketno;
			int			batchno;

			ExecHashGetBucketAndBatch(hashtable, hashTuple->hashvalue,
									  &bucketno, &batchno);

			/* add the tuple to the proper bucket */
			if (is_shared)
				hashTuple_shared = chunk_shared + HASH_CHUNK_HEADER_SIZE + idx;
			ExecHashInsertTupleIntoBucket(hashtable,
										  &hashtable->buckets[bucketno],
										  hashTuple, hashTuple_shared);

			/* advance index past the tuple */
			idx += MAXALIGN(HJTUPLE_OVERHEAD +
							HJTUPLE_MINTUPLE(hashTuple)->t_len);
		}

		/* advance to the next chunk */
		if (is_shared)
			chunk = ExecHashPopChunkQueue(hashtable, &chunk_shared);
		else
			chunk = chunk->next.unshared;

		/* allow this loop to be cancellable */
		CHECK_FOR_INTERRUPTS();
	}
}


/*
 * ExecHashTableInsert
 *		insert a tuple into the hash table depending on the hash value
 *		it may just go to a temp file for later batches
 *
 * Note: the passed TupleTableSlot may contain a regular, minimal, or virtual
 * tuple; the minimal case in particular is certain to happen while reloading
 * tuples from batch files.  We could save some cycles in the regular-tuple
 * case by not forcing the slot contents into minimal form; not clear if it's
 * worth the messiness required.
 */
void
ExecHashTableInsert(HashJoinTable hashtable,
					TupleTableSlot *slot,
					uint32 hashvalue)
{
	MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot);
	int			bucketno;
	int			batchno;

 retry:
	ExecHashGetBucketAndBatch(hashtable, hashvalue,
							  &bucketno, &batchno);

	/*
	 * decide whether to put the tuple in the hash table or a temp file
	 */
	if (batchno == hashtable->curbatch)
	{
		/*
		 * put the tuple in hash table
		 */
		HashJoinTuple hashTuple;
		double		ntuples = (hashtable->totalTuples - hashtable->skewTuples);
		dsa_pointer shared = InvalidDsaPointer;

		/* Create the HashJoinTuple */
		if (HashJoinTableIsShared(hashtable))
			hashTuple = ExecHashLoadSharedTuple(hashtable, tuple, &shared, true);
		else
			hashTuple = ExecHashLoadPrivateTuple(hashtable, tuple, true);
		if (hashTuple == NULL)
		{
			/*
			 * We ran out of work_mem.  Try to shrink the hash table and try
			 * again, in case this tuple now needs to be thrown into a future
			 * batch.
			 */
			ExecHashShrink(hashtable);
			goto retry;
		}

		/* Store the hash value in the HashJoinTuple header. */
		hashTuple->hashvalue = hashvalue;

		/*
		 * We always reset the tuple-matched flag on insertion.  This is okay
		 * even when reloading a tuple from a batch file, since the tuple
		 * could not possibly have been matched to an outer tuple before it
		 * went into the batch file.
		 */
		HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));

		/* Push it onto the front of the bucket's list */
		ExecHashInsertTupleIntoBucket(hashtable, &hashtable->buckets[bucketno],
									  hashTuple, shared);

		/*
		 * Increase the (optimal) number of buckets if we just exceeded the
		 * NTUP_PER_BUCKET threshold, but only when there's still a single
		 * batch.
		 */
		if (hashtable->nbatch == 1 &&
			ntuples > (hashtable->nbuckets_optimal * NTUP_PER_BUCKET))
		{
			/* Guard against integer overflow and alloc size overflow */
			if (hashtable->nbuckets_optimal <= INT_MAX / 2 &&
				hashtable->nbuckets_optimal * 2 <= MaxAllocSize / sizeof(HashJoinBucketHead))
			{
				hashtable->nbuckets_optimal *= 2;
				hashtable->log2_nbuckets_optimal += 1;
			}
		}
	}
	else
	{
		/*
		 * put the tuple into a temp file for later batches
		 */
		Assert(batchno > hashtable->curbatch);
		if (HashJoinTableIsShared(hashtable))
			sts_puttuple(hashtable->shared_inner_batches, batchno, &hashvalue,
						 tuple);
		else
			ExecHashJoinSaveTuple(tuple,
								  hashvalue,
								  &hashtable->innerBatchFile[batchno]);
	}
}

/*
 * ExecHashGetHashValue
 *		Compute the hash value for a tuple
 *
 * The tuple to be tested must be in either econtext->ecxt_outertuple or
 * econtext->ecxt_innertuple.  Vars in the hashkeys expressions should have
 * varno either OUTER_VAR or INNER_VAR.
 *
 * A TRUE result means the tuple's hash value has been successfully computed
 * and stored at *hashvalue.  A FALSE result means the tuple cannot match
 * because it contains a null attribute, and hence it should be discarded
 * immediately.  (If keep_nulls is true then FALSE is never returned.)
 */
bool
ExecHashGetHashValue(HashJoinTable hashtable,
					 ExprContext *econtext,
					 List *hashkeys,
					 bool outer_tuple,
					 bool keep_nulls,
					 uint32 *hashvalue)
{
	uint32		hashkey = 0;
	FmgrInfo   *hashfunctions;
	ListCell   *hk;
	int			i = 0;
	MemoryContext oldContext;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	if (outer_tuple)
		hashfunctions = hashtable->outer_hashfunctions;
	else
		hashfunctions = hashtable->inner_hashfunctions;

	foreach(hk, hashkeys)
	{
		ExprState  *keyexpr = (ExprState *) lfirst(hk);
		Datum		keyval;
		bool		isNull;

		/* rotate hashkey left 1 bit at each step */
		hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);

		/*
		 * Get the join attribute value of the tuple
		 */
		keyval = ExecEvalExpr(keyexpr, econtext, &isNull);

		/*
		 * If the attribute is NULL, and the join operator is strict, then
		 * this tuple cannot pass the join qual so we can reject it
		 * immediately (unless we're scanning the outside of an outer join, in
		 * which case we must not reject it).  Otherwise we act like the
		 * hashcode of NULL is zero (this will support operators that act like
		 * IS NOT DISTINCT, though not any more-random behavior).  We treat
		 * the hash support function as strict even if the operator is not.
		 *
		 * Note: currently, all hashjoinable operators must be strict since
		 * the hash index AM assumes that.  However, it takes so little extra
		 * code here to allow non-strict that we may as well do it.
		 */
		if (isNull)
		{
			if (hashtable->hashStrict[i] && !keep_nulls)
			{
				MemoryContextSwitchTo(oldContext);
				return false;	/* cannot match */
			}
			/* else, leave hashkey unmodified, equivalent to hashcode 0 */
		}
		else
		{
			/* Compute the hash function */
			uint32		hkey;

			hkey = DatumGetUInt32(FunctionCall1(&hashfunctions[i], keyval));
			hashkey ^= hkey;
		}

		i++;
	}

	MemoryContextSwitchTo(oldContext);

	*hashvalue = hashkey;
	return true;
}

/*
 * ExecHashGetBucketAndBatch
 *		Determine the bucket number and batch number for a hash value
 *
 * Note: on-the-fly increases of nbatch must not change the bucket number
 * for a given hash code (since we don't move tuples to different hash
 * chains), and must only cause the batch number to remain the same or
 * increase.  Our algorithm is
 *		bucketno = hashvalue MOD nbuckets
 *		batchno = (hashvalue DIV nbuckets) MOD nbatch
 * where nbuckets and nbatch are both expected to be powers of 2, so we can
 * do the computations by shifting and masking.  (This assumes that all hash
 * functions are good about randomizing all their output bits, else we are
 * likely to have very skewed bucket or batch occupancy.)
 *
 * nbuckets and log2_nbuckets may change while nbatch == 1 because of dynamic
 * bucket count growth.  Once we start batching, the value is fixed and does
 * not change over the course of the join (making it possible to compute batch
 * number the way we do here).
 *
 * nbatch is always a power of 2; we increase it only by doubling it.  This
 * effectively adds one more bit to the top of the batchno.
 */
void
ExecHashGetBucketAndBatch(HashJoinTable hashtable,
						  uint32 hashvalue,
						  int *bucketno,
						  int *batchno)
{
	uint32		nbuckets = (uint32) hashtable->nbuckets;
	uint32		nbatch = (uint32) hashtable->nbatch;

	if (nbatch > 1)
	{
		/* we can do MOD by masking, DIV by shifting */
		*bucketno = hashvalue & (nbuckets - 1);
		*batchno = (hashvalue >> hashtable->log2_nbuckets) & (nbatch - 1);
	}
	else
	{
		*bucketno = hashvalue & (nbuckets - 1);
		*batchno = 0;
	}
}

/*
 * ExecScanHashBucket
 *		scan a hash bucket for matches to the current outer tuple
 *
 * The current outer tuple must be stored in econtext->ecxt_outertuple.
 *
 * On success, the inner tuple is stored into hjstate->hj_CurTuple and
 * econtext->ecxt_innertuple, using hjstate->hj_HashTupleSlot as the slot
 * for the latter.
 */
bool
ExecScanHashBucket(HashJoinState *hjstate,
				   ExprContext *econtext)
{
	ExprState  *hjclauses = hjstate->hashclauses;
	HashJoinTable hashtable = hjstate->hj_HashTable;
	HashJoinTuple hashTuple = hjstate->hj_CurTuple;
	uint32		hashvalue = hjstate->hj_CurHashValue;

	/*
	 * hj_CurTuple is the address of the tuple last returned from the current
	 * bucket, or NULL if it's time to start scanning a new bucket.
	 *
	 * If the tuple hashed to a skew bucket then scan the skew bucket
	 * otherwise scan the standard hashtable bucket.
	 */
	if (hashTuple != NULL)
		hashTuple = ExecHashNextTupleInBucket(hashtable, hashTuple);
	else if (hjstate->hj_CurSkewBucketNo != INVALID_SKEW_BUCKET_NO)
		hashTuple = ExecHashFirstTupleInSkewBucket(hashtable,
												   hjstate->hj_CurSkewBucketNo,
												   NULL);
	else
		hashTuple = ExecHashFirstTupleInBucket(hashtable,
											   hjstate->hj_CurBucketNo);

	while (hashTuple != NULL)
	{
		if (hashTuple->hashvalue == hashvalue)
		{
			TupleTableSlot *inntuple;

			/* insert hashtable's tuple into exec slot so ExecQual sees it */
			inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
											 hjstate->hj_HashTupleSlot,
											 false);	/* do not pfree */
			econtext->ecxt_innertuple = inntuple;

			/* reset temp memory each time to avoid leaks from qual expr */
			ResetExprContext(econtext);

			if (ExecQual(hjclauses, econtext))
			{
				hjstate->hj_CurTuple = hashTuple;
				return true;
			}
		}

		hashTuple = ExecHashNextTupleInBucket(hashtable, hashTuple);
	}

	/*
	 * no match
	 */
	return false;
}

/*
 * ExecPrepHashTableForUnmatched
 *		set up for a series of ExecScanHashTableForUnmatched calls
 */
void
ExecPrepHashTableForUnmatched(HashJoinState *hjstate)
{
	/*----------
	 * During this scan we use the HashJoinState fields as follows:
	 *
	 * hj_HashTable->unmatched_chunks: the queue of chunks to scan
	 * hj_HashTable->current_chunk: chunk being scanned currently
	 * hj_HashTable->current_chunk_index: position within chunk
	 * hj_CurSkewBucketNo: next skew bucket (an index into skewBucketNums)
	 * hj_CurTuple: last skew tuple returned, or NULL to start next bucket
	 *----------
	 */
	hjstate->hj_HashTable->unmatched_chunks = hjstate->hj_HashTable->chunks;
	hjstate->hj_HashTable->current_chunk = NULL;
	hjstate->hj_CurSkewBucketNo = 0;
	hjstate->hj_CurTuple = NULL;
}

/*
 * Find the next non-empty skew bucket and returns its first tuple, or
 * NULL if there are not more skew buckets to scan.
 */
static HashJoinTuple
ExecHashNextSkewBucketFirstTuple(HashJoinState *hjstate)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	HashJoinTuple hashTuple = NULL;
	dsa_pointer	unused;
	int		j;

	if (HashJoinTableIsShared(hjstate->hj_HashTable))
	{
		LWLockAcquire(&hashtable->shared->chunk_lock, LW_EXCLUSIVE);
		while (hashtable->shared->current_skew_bucketno <
			   hashtable->shared->num_skew_buckets)
		{
			j = hashtable->skewBucketNums[hashtable->shared->current_skew_bucketno++];

			hashTuple = ExecHashFirstTupleInSkewBucket(hashtable, j,
													   &unused);
			if (hashTuple != NULL)
				break;
		}
		LWLockRelease(&hashtable->shared->chunk_lock);
	}
	else
	{
		while (hjstate->hj_CurSkewBucketNo < hashtable->nSkewBuckets)
		{
			j = hashtable->skewBucketNums[hjstate->hj_CurSkewBucketNo++];
			hashTuple = ExecHashFirstTupleInSkewBucket(hashtable, j,
													   &unused);
			if (hashTuple != NULL)
				break;
		}
	}

	return hashTuple;
}

/*
 * ExecScanHashTableForUnmatched
 *		scan the hash table for unmatched inner tuples
 *
 * On success, the inner tuple is stored into hjstate->hj_CurTuple and
 * econtext->ecxt_innertuple, using hjstate->hj_HashTupleSlot as the slot
 * for the latter.
 */
bool
ExecScanHashTableForUnmatched(HashJoinState *hjstate, ExprContext *econtext)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	HashJoinTuple hashTuple;
	MinimalTuple tuple;

	/*
	 * First, process the queue of chunks holding tuples that are in regular
	 * (non-skew) buckets.
	 */
	for (;;)
	{
		/* Do we need a new chunk to scan? */
		if (hashtable->current_chunk == NULL)
		{
			/* Pop the next chunk from the front of the queue. */
			if (HashJoinTableIsShared(hashtable))
			{
				hashtable->current_chunk =
					ExecHashPopChunkQueue(hashtable,
										  &hashtable->current_chunk_shared);
			}
			else if (hashtable->unmatched_chunks != NULL)
			{
				hashtable->current_chunk = hashtable->unmatched_chunks;
				hashtable->unmatched_chunks =
					hashtable->current_chunk->next.unshared;
			}
			hashtable->current_chunk_index = 0;
		}

		/* Have we run out of chunks to scan? */
		if (hashtable->current_chunk == NULL)
			break;

		/* Scan the current chunk looking for unmatched tuples. */
		while (hashtable->current_chunk_index < hashtable->current_chunk->used)
		{
			/* Take the next tuple from this chunk. */
			hashTuple = (HashJoinTuple)
				(hashtable->current_chunk->data + hashtable->current_chunk_index);
			tuple = HJTUPLE_MINTUPLE(hashTuple);
			hashtable->current_chunk_index +=
				MAXALIGN(HJTUPLE_OVERHEAD + tuple->t_len);

			/* Is it unmatched? */
			if (!HeapTupleHeaderHasMatch(tuple))
			{
				TupleTableSlot *inntuple;

				/* insert hashtable's tuple into exec slot */
				inntuple = ExecStoreMinimalTuple(tuple,
												 hjstate->hj_HashTupleSlot,
												 false); /* do not pfree */
				econtext->ecxt_innertuple = inntuple;

				/* reset context each time (see below for explanation) */
				ResetExprContext(econtext);
				return true;
			}
		}

		/* Go around again to get the next chunk from the queue. */
		hashtable->current_chunk = NULL;
	}

	/*
	 * Next, scan all skew buckets, since those tuples are not stored in
	 * chunks.
	 */
	hashTuple = hjstate->hj_CurTuple;
	for (;;)
	{
		/*
		 * hj_CurTuple is the address of the tuple last returned from the
		 * current bucket, or NULL if it's time to start scanning a new
		 * bucket.
		 */
		if (hashTuple != NULL)
			hashTuple = ExecHashNextTupleInBucket(hashtable, hashTuple);
		else
		{
			hashTuple = ExecHashNextSkewBucketFirstTuple(hjstate);
			if (hashTuple == NULL)
				break;				/* finished all buckets */
		}

		while (hashTuple != NULL)
		{
			if (!HeapTupleHeaderHasMatch(HJTUPLE_MINTUPLE(hashTuple)))
			{
				TupleTableSlot *inntuple;

				/* insert hashtable's tuple into exec slot */
				inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
												 hjstate->hj_HashTupleSlot,
												 false);	/* do not pfree */
				econtext->ecxt_innertuple = inntuple;

				/*
				 * Reset temp memory each time; although this function doesn't
				 * do any qual eval, the caller will, so let's keep it
				 * parallel to ExecScanHashBucket.
				 */
				ResetExprContext(econtext);

				hjstate->hj_CurTuple = hashTuple;
				return true;
			}

			hashTuple = ExecHashNextTupleInBucket(hashtable, hashTuple);
		}

		/* allow this loop to be cancellable */
		CHECK_FOR_INTERRUPTS();
	}

	/*
	 * no more unmatched tuples
	 */
	return false;
}

/*
 * ExecHashTableReset
 *
 *		reset hash table header for new batch
 */
void
ExecHashTableReset(HashJoinTable hashtable)
{
	MemoryContext oldcxt;
	int			nbuckets = hashtable->nbuckets;
	int			curbatch = hashtable->curbatch;

	if (HashJoinTableIsShared(hashtable))
	{
		/*
		 * Wait for all workers to finish accessing the hash table for the
		 * previous batch.
		 */
		Assert(BarrierPhase(&hashtable->shared->barrier) ==
			   PHJ_PHASE_UNMATCHED_BATCH(curbatch - 1));
		if (BarrierWait(&hashtable->shared->barrier, WAIT_EVENT_HASH_UNMATCHED))
		{
			dsa_pointer chunk_shared;
			int		i;

			/*
			 * Serial phase: prepare shared state for new batch.  It may be
			 * worth trying to parallelize the loops below in future.
			 */

			Assert(BarrierPhase(&hashtable->shared->barrier) ==
				   PHJ_PHASE_RESETTING_BATCH(hashtable->curbatch));

			/* Free the skew hash table if we've just finished batch 0. */
			ExecHashFreeSkewTable(hashtable);

			/* Clear the hash table. */
			for (i = 0; i < hashtable->nbuckets; ++i)
				dsa_pointer_atomic_write(&hashtable->buckets[i].shared,
										 InvalidDsaPointer);

			/* Free all the chunks. */
			hashtable->shared->chunk_work_queue = hashtable->shared->chunks;
			hashtable->shared->chunks = InvalidDsaPointer;
			while (ExecHashPopChunkQueue(hashtable, &chunk_shared) != NULL)
				dsa_free(hashtable->area, chunk_shared);

			/* Reset the hash table size. */
			hashtable->shared->size =
				(hashtable->nbuckets * sizeof(HashJoinBucketHead));

			/* Rewind the shared read heads for this batch, inner and outer. */
			sts_prepare_partial_scan(hashtable->shared_inner_batches,
									 curbatch);
			sts_prepare_partial_scan(hashtable->shared_outer_batches,
									 curbatch);
		}

		/*
		 * Each participant needs to make sure that data it has written for
		 * this partition is now read-only and visible to other participants.
		 */
		sts_end_write(hashtable->shared_inner_batches, curbatch);
		sts_end_write(hashtable->shared_outer_batches, curbatch);

		/*
		 * Wait again, so that all workers see the new hash table and can
		 * safely read from batch files from any participant because they have
		 * all ended writing.
		 */
		Assert(BarrierPhase(&hashtable->shared->barrier) ==
			   PHJ_PHASE_RESETTING_BATCH(curbatch));
		BarrierWait(&hashtable->shared->barrier, WAIT_EVENT_HASH_RESETTING);
		Assert(BarrierPhase(&hashtable->shared->barrier) ==
			   PHJ_PHASE_LOADING_BATCH(curbatch));
		ExecHashUpdate(hashtable);

		/* Forget the current chunks. */
		hashtable->current_chunk = NULL;
		return;
	}

	/*
	 * Release all the hash buckets and tuples acquired in the prior pass, and
	 * reinitialize the context for a new pass.
	 */
	MemoryContextReset(hashtable->batchCxt);
	oldcxt = MemoryContextSwitchTo(hashtable->batchCxt);

	/* Reallocate and reinitialize the hash bucket headers. */
	hashtable->buckets = (HashJoinBucketHead *)
		palloc0(nbuckets * sizeof(HashJoinBucketHead));

	hashtable->spaceUsed = nbuckets * sizeof(HashJoinBucketHead);

	/* Cannot be more than our previous peak; we had this size before. */
	Assert(hashtable->spaceUsed <= hashtable->spacePeak);

	MemoryContextSwitchTo(oldcxt);

	/* Forget the chunks (the memory was freed by the context reset above). */
	hashtable->chunks = NULL;

	/* Rewind the shared read heads for this batch, inner and outer. */
	if (hashtable->innerBatchFile[curbatch] != NULL)
	{
		if (BufFileSeek(hashtable->innerBatchFile[curbatch], 0, 0L, SEEK_SET))
			ereport(ERROR,
					(errcode_for_file_access(),
				   errmsg("could not rewind hash-join temporary file: %m")));
	}
	if (hashtable->outerBatchFile[curbatch] != NULL)
	{
		if (BufFileSeek(hashtable->outerBatchFile[curbatch], 0, 0L, SEEK_SET))
			ereport(ERROR,
					(errcode_for_file_access(),
				   errmsg("could not rewind hash-join temporary file: %m")));
	}
}

/*
 * ExecHashTableResetMatchFlags
 *		Clear all the HeapTupleHeaderHasMatch flags in the table
 */
void
ExecHashTableResetMatchFlags(HashJoinTable hashtable)
{
	dsa_pointer chunk_shared = InvalidDsaPointer;
	HashMemoryChunk chunk;
	HashJoinTuple tuple;
	int			i;

	/* Reset all flags in the main table ... */
	if (HashJoinTableIsShared(hashtable))
	{
		/* This only runs in the leader during rescan initialization. */
		Assert(!IsParallelWorker());
		hashtable->shared->chunk_work_queue = hashtable->shared->chunks;
		chunk = ExecHashPopChunkQueue(hashtable, &chunk_shared);
	}
	else
		chunk = hashtable->chunks;
	while (chunk != NULL)
	{
		Size index = 0;

		/* Clear the flag for all tuples in this chunk. */
		while (index < chunk->used)
		{
			tuple = (HashJoinTuple) (chunk->data + index);
			HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(tuple));
			index += MAXALIGN(HJTUPLE_OVERHEAD +
							  HJTUPLE_MINTUPLE(tuple)->t_len);
		}
		if (HashJoinTableIsShared(hashtable))
			chunk = ExecHashPopChunkQueue(hashtable, &chunk_shared);
		else
			chunk = chunk->next.unshared;
	}

	/* ... and the same for the skew buckets, if any */
	for (i = 0; i < hashtable->nSkewBuckets; i++)
	{
		int			j = hashtable->skewBucketNums[i];

		for (tuple = ExecHashFirstTupleInSkewBucket(hashtable, j, NULL);
			 tuple != NULL;
			 tuple = ExecHashNextTupleInBucket(hashtable, tuple))
			HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(tuple));
	}
}


void
ExecReScanHash(HashState *node)
{
	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->ps.lefttree->chgParam == NULL)
		ExecReScan(node->ps.lefttree);
}


/*
 * ExecHashBuildSkewHash
 *
 *		Set up for skew optimization if we can identify the most common values
 *		(MCVs) of the outer relation's join key.  We make a skew hash bucket
 *		for the hash value of each MCV, up to the number of slots allowed
 *		based on available memory.
 */
static void
ExecHashBuildSkewHash(HashJoinTable hashtable, Hash *node, int mcvsToUse)
{
	HeapTupleData *statsTuple;
	AttStatsSlot sslot;

	/* Do nothing if planner didn't identify the outer relation's join key */
	if (!OidIsValid(node->skewTable))
		return;
	/* Also, do nothing if we don't have room for at least one skew bucket */
	if (mcvsToUse <= 0)
		return;

	/*
	 * Try to find the MCV statistics for the outer relation's join key.
	 */
	statsTuple = SearchSysCache3(STATRELATTINH,
								 ObjectIdGetDatum(node->skewTable),
								 Int16GetDatum(node->skewColumn),
								 BoolGetDatum(node->skewInherit));
	if (!HeapTupleIsValid(statsTuple))
		return;

	if (get_attstatsslot(&sslot, statsTuple,
						 STATISTIC_KIND_MCV, InvalidOid,
						 ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS))
	{
		double		frac;
		int			nbuckets;
		FmgrInfo   *hashfunctions;
		int			i;

		if (mcvsToUse > sslot.nvalues)
			mcvsToUse = sslot.nvalues;

		/*
		 * Calculate the expected fraction of outer relation that will
		 * participate in the skew optimization.  If this isn't at least
		 * SKEW_MIN_OUTER_FRACTION, don't use skew optimization.
		 */
		frac = 0;
		for (i = 0; i < mcvsToUse; i++)
			frac += sslot.numbers[i];
		if (frac < SKEW_MIN_OUTER_FRACTION)
		{
			free_attstatsslot(&sslot);
			ReleaseSysCache(statsTuple);
			return;
		}

		/*
		 * Okay, set up the skew hashtable.
		 *
		 * skewBucket[] is an open addressing hashtable with a power of 2 size
		 * that is greater than the number of MCV values.  (This ensures there
		 * will be at least one null entry, so searches will always
		 * terminate.)
		 *
		 * Note: this code could fail if mcvsToUse exceeds INT_MAX/8 or
		 * MaxAllocSize/sizeof(void *)/8, but that is not currently possible
		 * since we limit pg_statistic entries to much less than that.
		 */
		nbuckets = 2;
		while (nbuckets <= mcvsToUse)
			nbuckets <<= 1;
		/* use two more bits just to help avoid collisions */
		nbuckets <<= 2;

		hashtable->skewBucketLen = nbuckets;

		if (HashJoinTableIsShared(hashtable))
		{
			int		i;

			/* Allocate the bucket memory in shared memory. */
			hashtable->shared->skew_buckets =
				dsa_allocate(hashtable->area,
							 nbuckets * sizeof(HashSkewBucket));
			hashtable->shared->skew_bucket_nums =
				dsa_allocate(hashtable->area, mcvsToUse * sizeof(int));
			hashtable->shared->skew_bucket_len = nbuckets;

			hashtable->skewBucket = (HashSkewBucket *)
				dsa_get_address(hashtable->area,
								hashtable->shared->skew_buckets);
			hashtable->skewBucketNums = (int *)
				dsa_get_address(hashtable->area,
								hashtable->shared->skew_bucket_nums);

			/* Initialize the skew buckets' atomic tuple pointers. */
			for (i = 0; i < nbuckets; ++i)
				dsa_pointer_atomic_init(&hashtable->skewBucket[i].tuples.shared,
										InvalidDsaPointer);

			/*
			 * Set up the space allowance.  Individual backends will be given
			 * allocations from this shared limit.
			 */
			hashtable->shared->skew_space_allowed =
				hashtable->spaceAllowedSkew;
			hashtable->shared->skew_space_used = 0;
		}
		else
		{
			/*
			 * We allocate the bucket memory in the hashtable's batch
			 * context. It is only needed during the first batch, and this
			 * ensures it will be automatically removed once the first batch
			 * is done.
			 */
			hashtable->skewBucket = (HashSkewBucket *)
				MemoryContextAllocZero(hashtable->batchCxt,
									   nbuckets * sizeof(HashSkewBucket));
			hashtable->skewBucketNums = (int *)
				MemoryContextAllocZero(hashtable->batchCxt,
									   mcvsToUse * sizeof(int));
		}

		hashtable->spaceUsed += nbuckets * sizeof(HashSkewBucket *)
			+ mcvsToUse * sizeof(int);
		hashtable->spaceUsedSkew += nbuckets * sizeof(HashSkewBucket *)
			+ mcvsToUse * sizeof(int);
		if (hashtable->spaceUsed > hashtable->spacePeak)
			hashtable->spacePeak = hashtable->spaceUsed;

		/*
		 * Create a skew bucket for each MCV hash value.
		 *
		 * Note: it is very important that we create the buckets in order of
		 * decreasing MCV frequency.  If we have to remove some buckets, they
		 * must be removed in reverse order of creation (see notes in
		 * ExecHashRemoveNextSkewBucket) and we want the least common MCVs to
		 * be removed first.
		 */
		hashfunctions = hashtable->outer_hashfunctions;

		for (i = 0; i < mcvsToUse; i++)
		{
			uint32		hashvalue;
			int			bucket;

			hashvalue = DatumGetUInt32(FunctionCall1(&hashfunctions[0],
													 sslot.values[i]));

			/*
			 * While we have not hit a hole in the hashtable and have not hit
			 * the desired bucket, we have collided with some previous hash
			 * value, so try the next bucket location.  NB: this code must
			 * match ExecHashGetSkewBucket.
			 */
			bucket = hashvalue & (nbuckets - 1);
			while (hashtable->skewBucket[bucket].active &&
				   hashtable->skewBucket[bucket].hashvalue != hashvalue)
				bucket = (bucket + 1) & (nbuckets - 1);

			/*
			 * If we found an existing bucket with the same hashvalue, leave
			 * it alone.  It's okay for two MCVs to share a hashvalue.
			 */
			if (hashtable->skewBucket[bucket].active)
				continue;

			/* Okay, create a new skew bucket for this hashvalue. */
			hashtable->skewBucket[bucket].active = true;
			hashtable->skewBucket[bucket].hashvalue = hashvalue;
			hashtable->skewBucketNums[hashtable->nSkewBuckets] = bucket;
			hashtable->nSkewBuckets++;
		}

		/* Publish the bucket count for ExecHashUpdate() to read. */
		if (HashJoinTableIsShared(hashtable))
			hashtable->shared->num_skew_buckets = hashtable->nSkewBuckets;

		free_attstatsslot(&sslot);
	}

	ReleaseSysCache(statsTuple);
}

/*
 * ExecHashGetSkewBucket
 *
 *		Returns the index of the skew bucket for this hashvalue,
 *		or INVALID_SKEW_BUCKET_NO if the hashvalue is not
 *		associated with any active skew bucket.
 */
int
ExecHashGetSkewBucket(HashJoinTable hashtable, uint32 hashvalue)
{
	int			bucket;

	/*
	 * Always return INVALID_SKEW_BUCKET_NO if not doing skew optimization (in
	 * particular, this happens after the initial batch is done).
	 */
	if (hashtable->nSkewBuckets == 0)
		return INVALID_SKEW_BUCKET_NO;

	/*
	 * Since skewBucketLen is a power of 2, we can do a modulo by ANDing.
	 */
	bucket = hashvalue & (hashtable->skewBucketLen - 1);

	/*
	 * While we have not hit a hole in the hashtable and have not hit the
	 * desired bucket, we have collided with some other hash value, so try the
	 * next bucket location.
	 */
	while (hashtable->skewBucket[bucket].active &&
		   hashtable->skewBucket[bucket].hashvalue != hashvalue)
		bucket = (bucket + 1) & (hashtable->skewBucketLen - 1);

	/*
	 * Found the desired bucket?
	 *
	 * During a parallel build this is an unsynchronized read of a flag in
	 * shared memory that might be concurrently set to false, but we can
	 * tolerate a stale true value here: we'll fail at insertion time.
	 */
	if (hashtable->skewBucket[bucket].active)
		return bucket;

	/*
	 * There must not be any hashtable entry for this hash value.
	 */
	return INVALID_SKEW_BUCKET_NO;
}

/*
 * ExecHashSkewTableInsert
 *
 *		Insert a tuple into the skew hashtable.
 *
 * This should generally match up with the current-batch case in
 * ExecHashTableInsert.  Return true if the insertion succeeded, and false if
 * the given skew bucket has been removed by ExecHashDetachSkewBucket().
 */
static bool
ExecHashSkewTableInsert(HashJoinTable hashtable,
						TupleTableSlot *slot,
						uint32 hashvalue,
						int bucketNumber)
{
	MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot);
	dsa_pointer	shared;
	HashJoinTuple hashTuple;
	int			hashTupleSize;

	/* Create the HashJoinTuple */
	hashTupleSize = HJTUPLE_OVERHEAD + tuple->t_len;
	if (HashJoinTableIsShared(hashtable))
	{
		shared = dsa_allocate(hashtable->area, hashTupleSize);
		hashTuple = (HashJoinTuple) dsa_get_address(hashtable->area, shared);
	}
	else
	{
		shared = InvalidDsaPointer;
		hashTuple = (HashJoinTuple) MemoryContextAlloc(hashtable->batchCxt,
													   hashTupleSize);
	}
	hashTuple->hashvalue = hashvalue;
	memcpy(HJTUPLE_MINTUPLE(hashTuple), tuple, tuple->t_len);
	HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));

	/* Push it onto the front of the skew bucket's list */
	if (!ExecHashInsertTupleIntoBucket(hashtable,
									   &hashtable->skewBucket[bucketNumber].tuples,
									   hashTuple, shared))
	{
		/*
		 * Failed to insert into a skew bucket because it was concurrently
		 * removed.  This tuple now belongs in regular storage.
		 */
		Assert(HashJoinTableIsShared(hashtable));
		dsa_free(hashtable->area, shared);
		return false;
	}

	/* Account for space used, and back off if we've used too much */
	if (!HashJoinTableIsShared(hashtable))
	{
		hashtable->spaceUsed += hashTupleSize;
		if (hashtable->spaceUsed > hashtable->spacePeak)
			hashtable->spacePeak = hashtable->spaceUsed;
	}
	hashtable->spaceUsedSkew += hashTupleSize;
	if (hashtable->spaceUsedSkew > hashtable->spaceAllowedSkew)
		ExecHashHandleSkewOverflow(hashtable, bucketNumber);

	/* Check we are not over the total spaceAllowed, either */
	if (!HashJoinTableIsShared(hashtable) &&
		hashtable->spaceUsed > hashtable->spaceAllowed &&
		hashtable->growEnabled)
	{
		ExecHashIncreaseNumBatches(hashtable, hashtable->nbatch * 2);
		ExecHashShrink(hashtable);
	}

	return true;
}

/*
 *		ExecHashTransferSkewTuples
 *
 *		Remove the least valuable skew bucket by pushing its tuples into
 *		the main hash table.
 *
 *		This function takes a tuple chain by pointer rather than looking in
 *		the skew bucket, because in the shared case by the time we arrive here
 *		the skew bucket tuple chain has been replaced with a tombstone.
 */
static void
ExecHashTransferSkewTuples(HashJoinTable hashtable,
						   HashJoinTuple head_tuple,
						   dsa_pointer head_tuple_shared)
{
	uint32		hashvalue;
	int			bucketno;
	int			batchno;
	HashJoinTuple hashTuple;
	dsa_pointer shared;

	/* If we have no tuples to transfer, there's nothing left to do. */
	if (head_tuple == NULL)
		return;

	/*
	 * Calculate which bucket and batch the tuples belong to in the main
	 * hashtable.  They all have the same hash value, so it's the same for all
	 * of them.  Also note that it's not possible for nbatch to increase while
	 * we are processing the tuples.
	 */
	hashvalue = head_tuple->hashvalue;
	ExecHashGetBucketAndBatch(hashtable, hashvalue, &bucketno, &batchno);

	/* Process all tuples in the bucket */
	hashTuple = head_tuple;
	shared = head_tuple_shared;
	while (hashTuple != NULL)
	{
		HashJoinTuple nextHashTuple =
			ExecHashNextTupleInBucket(hashtable, hashTuple);
		dsa_pointer next_shared = hashTuple->next.shared;
		MinimalTuple tuple;
		Size		tupleSize;

		Assert(hashTuple->hashvalue == hashvalue);
		Assert(hashtable->curbatch == 0);

		/*
		 * This code must agree with ExecHashTableInsert.  We do not use
		 * ExecHashTableInsert directly as ExecHashTableInsert expects a
		 * TupleTableSlot while we already have HashJoinTuples.
		 */
		tuple = HJTUPLE_MINTUPLE(hashTuple);
		tupleSize = HJTUPLE_OVERHEAD + tuple->t_len;

		/* Decide whether to put the tuple in the hash table or a temp file */
		if (batchno == hashtable->curbatch)
		{
			/* Move the tuple to the main hash table */
			HashJoinTuple copyTuple;
			dsa_pointer copy_shared;

			/*
			 * We must copy the tuple into the dense storage, else it will not
			 * be found by, eg, ExecHashIncreaseNumBatches.
			 */
			if (HashJoinTableIsShared(hashtable))
			{
				copyTuple = ExecHashLoadSharedTuple(hashtable, tuple,
													&copy_shared, false);
				dsa_free(hashtable->area, shared);
			}
			else
			{
				copy_shared = InvalidDsaPointer;
				copyTuple = ExecHashLoadPrivateTuple(hashtable, tuple, false);
				pfree(hashTuple);
			}

			copyTuple->hashvalue = hashvalue;
			ExecHashInsertTupleIntoBucket(hashtable,
										  &hashtable->buckets[bucketno],
										  copyTuple, copy_shared);

			/* We have reduced skew space, but overall space doesn't change */
			hashtable->spaceUsedSkew -= tupleSize;
		}
		else
		{
			/* Put the tuple into a temp file for later batches */
			Assert(batchno > hashtable->curbatch);
			if (HashJoinTableIsShared(hashtable))
			{
				sts_puttuple(hashtable->shared_inner_batches, batchno,
							 &hashvalue, tuple);
				dsa_free(hashtable->area, shared);
			}
			else
			{
				ExecHashJoinSaveTuple(tuple, hashvalue,
									  &hashtable->innerBatchFile[batchno]);
				pfree(hashTuple);
				hashtable->spaceUsed -= tupleSize;
				hashtable->spaceUsedSkew -= tupleSize;
			}
		}

		hashTuple = nextHashTuple;
		shared = next_shared;

		/* allow this loop to be cancellable */
		CHECK_FOR_INTERRUPTS();
	}

	/*
	 * NOTE: this is not nearly as simple as it looks on the surface, because
	 * of the possibility of collisions in the hashtable.  Suppose that hash
	 * values A and B collide at a particular hashtable entry, and that A was
	 * entered first so B gets shifted to a different table entry.  If we were
	 * to remove A first then ExecHashGetSkewBucket would mistakenly start
	 * reporting that B is not in the hashtable, because it would hit the NULL
	 * before finding B.  However, we always remove entries in the reverse
	 * order of creation, so this failure cannot happen.
	 */
}

/*
 * Copy 'tuple' into backend-local dense storage with a HashJoinTuple header,
 * and return a pointer to the new copy.  If 'respect_work_mem' is true, this
 * may return NULL to indicate that work_mem would be exceeded.  NULL
 * indicates that the caller should try to shrink the hash table.  The result
 * includes an uninitialized HashJoinTuple header which the caller must fill
 * in.
 */
static HashJoinTuple
ExecHashLoadPrivateTuple(HashJoinTable hashtable, MinimalTuple tuple,
						 bool respect_work_mem)
{
	HashMemoryChunk newChunk;
	Size		size;
	HashJoinTuple result;

	Assert(!HashJoinTableIsShared(hashtable));

	/* compute the size of the tuple + HashJoinTuple header */
	size = MAXALIGN(HJTUPLE_OVERHEAD + tuple->t_len);

	/*
	 * If tuple size is larger than of 1/4 of chunk size, allocate a separate
	 * chunk.
	 */
	if (size > HASH_CHUNK_THRESHOLD)
	{
		/* Check if work_mem would be exceeded. */
		if (respect_work_mem &&
			hashtable->growEnabled &&
			hashtable->spaceUsed + HASH_CHUNK_HEADER_SIZE + size >
			hashtable->spaceAllowed)
		{
			ExecHashIncreaseNumBatches(hashtable, hashtable->nbatch * 2);
			return NULL;
		}

		/* allocate new chunk and put it at the beginning of the list */
		newChunk = (HashMemoryChunk) MemoryContextAlloc(hashtable->batchCxt,
														offsetof(HashMemoryChunkData, data) + size);
		newChunk->maxlen = size;
		newChunk->used = 0;
		newChunk->ntuples = 0;

		/*
		 * Add this chunk to the list after the first existing chunk, so that
		 * we don't lose the remaining space in the "current" chunk.
		 */
		if (hashtable->chunks != NULL)
		{
			newChunk->next.unshared = hashtable->chunks->next.unshared;
			hashtable->chunks->next.unshared = newChunk;
		}
		else
		{
			newChunk->next.unshared = hashtable->chunks;
			hashtable->chunks = newChunk;
		}

		newChunk->used += size;
		newChunk->ntuples += 1;

		/* count this single-tuple chunk as space used */
		hashtable->spaceUsed += HASH_CHUNK_HEADER_SIZE + size;
		if (hashtable->spaceUsed > hashtable->spacePeak)
			hashtable->spacePeak = hashtable->spaceUsed;

		result = (HashJoinTuple) newChunk->data;
		memcpy(HJTUPLE_MINTUPLE(result), tuple, tuple->t_len);

		return result;
	}

	/*
	 * See if we have enough space for it in the current chunk (if any). If
	 * not, allocate a fresh chunk.
	 */
	if ((hashtable->chunks == NULL) ||
		(hashtable->chunks->maxlen - hashtable->chunks->used) < size)
	{
		if (respect_work_mem &&
			hashtable->growEnabled &&
			hashtable->spaceUsed + HASH_CHUNK_SIZE > hashtable->spaceAllowed)
		{
			/* work_mem would be exceeded: prepare to shrink hash table */
			ExecHashIncreaseNumBatches(hashtable, hashtable->nbatch * 2);
			return NULL;
		}

		/* allocate new chunk and put it at the beginning of the list */
		newChunk = (HashMemoryChunk) MemoryContextAlloc(hashtable->batchCxt,
														HASH_CHUNK_SIZE);

		newChunk->maxlen = HASH_CHUNK_SIZE - HASH_CHUNK_HEADER_SIZE;
		newChunk->used = size;
		newChunk->ntuples = 1;

		newChunk->next.unshared = hashtable->chunks;
		hashtable->chunks = newChunk;

		/* count this whole mostly-empty chunk as space used */
		hashtable->spaceUsed += HASH_CHUNK_SIZE;
		if (hashtable->spaceUsed > hashtable->spacePeak)
			hashtable->spacePeak = hashtable->spaceUsed;

		result = (HashJoinTuple) newChunk->data;
		memcpy(HJTUPLE_MINTUPLE(result), tuple, tuple->t_len);

		return result;
	}

	/* There is enough space in the current chunk, let's add the tuple */
	result = (HashJoinTuple)
		(hashtable->chunks->data + hashtable->chunks->used);

	hashtable->chunks->used += size;
	hashtable->chunks->ntuples += 1;

	memcpy(HJTUPLE_MINTUPLE(result), tuple, tuple->t_len);

	return result;
}

/*
 * Load a tuple into shared dense storage.  This is like
 * ExecHashLoadPrivateTuple(), but for parallel hash table builds.
 */
static HashJoinTuple
ExecHashLoadSharedTuple(HashJoinTable hashtable, MinimalTuple tuple,
						dsa_pointer *shared, bool respect_work_mem)
{
	dsa_pointer chunk_shared;
	HashMemoryChunk chunk;
	Size chunk_size;
	Size size;
	HashJoinTuple result;

	Assert(HashJoinTableIsShared(hashtable));

	/* compute the size of the tuple + HashJoinTuple header */
	size = MAXALIGN(HJTUPLE_OVERHEAD + tuple->t_len);

	/*
	 * Fast path: if there is enough space in this backend's current chunk,
	 * then we can allocate without any locking.
	 */
	chunk = hashtable->current_chunk;
	if (chunk != NULL &&
		size < HASH_CHUNK_THRESHOLD &&
		chunk->maxlen - chunk->used >= size)
	{

		chunk_shared = hashtable->current_chunk_shared;
		Assert(chunk == dsa_get_address(hashtable->area, chunk_shared));
		*shared = chunk_shared + HASH_CHUNK_HEADER_SIZE + chunk->used;
		result = (HashJoinTuple) (chunk->data + chunk->used);
		chunk->used += size;
		chunk->ntuples += 1;

		Assert(chunk->used <= chunk->maxlen);
		Assert(result == dsa_get_address(hashtable->area, *shared));

		memcpy(HJTUPLE_MINTUPLE(result), tuple, tuple->t_len);

		return result;
	}

	/* Slow path: try to allocate a new chunk. */
	LWLockAcquire(&hashtable->shared->chunk_lock, LW_EXCLUSIVE);

	/* Check if some other participant has increased nbatch. */
	if (hashtable->shared->nbatch > hashtable->nbatch)
	{
		Assert(respect_work_mem);
		ExecHashIncreaseNumBatches(hashtable, hashtable->shared->nbatch);
	}

	/* Check if we need to help shrinking. */
	if (hashtable->shared->shrink_needed && respect_work_mem)
	{
		hashtable->current_chunk = NULL;
		LWLockRelease(&hashtable->shared->chunk_lock);
		return NULL;
	}

	/* Oversized tuples get their own chunk. */
	if (size > HASH_CHUNK_THRESHOLD)
		chunk_size = size + HASH_CHUNK_HEADER_SIZE;
	else
		chunk_size = HASH_CHUNK_SIZE;

	/* If appropriate, check if work_mem would be exceeded by a new chunk. */
	if (respect_work_mem &&
		hashtable->shared->grow_enabled &&
		hashtable->shared->nbatch <= MAX_BATCHES_BEFORE_INCREASES_STOP &&
		(hashtable->shared->size +
		 chunk_size) > (work_mem * 1024L *
						hashtable->shared->planned_participants))
	{
		/*
		 * It would be exceeded.  Let's increase the number of batches, so we
		 * can try to shrink the hash table.
		 */
		hashtable->shared->nbatch *= 2;
		ExecHashIncreaseNumBatches(hashtable, hashtable->shared->nbatch);
		hashtable->shared->chunk_work_queue = hashtable->shared->chunks;
		hashtable->shared->chunks = InvalidDsaPointer;
		hashtable->shared->shrink_needed = true;
		hashtable->current_chunk = NULL;
		LWLockRelease(&hashtable->shared->chunk_lock);

		/* The caller needs to shrink the hash table. */
		return NULL;
	}

	/*
	 * If there was a chunk already, then add its tuple count to the shared
	 * total now.  The final chunk's count will be handled in finish_loading.
	 */
	if (chunk != NULL)
		hashtable->shared->ntuples += chunk->ntuples;

	/* We are cleared to allocate a new chunk. */
	chunk_shared = dsa_allocate(hashtable->area, chunk_size);
	hashtable->shared->size += chunk_size;

	/* Set up the chunk. */
	chunk = (HashMemoryChunk) dsa_get_address(hashtable->area, chunk_shared);
	*shared = chunk_shared + HASH_CHUNK_HEADER_SIZE;
	chunk->maxlen = chunk_size - offsetof(HashMemoryChunkData, data);
	chunk->used = size;
	chunk->ntuples = 1;

	/*
	 * Push it onto the list of chunks, so that it can be found if we need to
	 * increase the number of buckets or batches and process all tuples.
	 */
	chunk->next.shared = hashtable->shared->chunks;
	hashtable->shared->chunks = chunk_shared;

	if (size > HASH_CHUNK_THRESHOLD)
	{
		/*
		 * Count oversized tuples immediately, but don't bother making this
		 * chunk the 'current' chunk because it has no more space in it for
		 * next time.
		 */
		++hashtable->shared->ntuples;
	}
	else
	{
		/*
		 * Make this the current chunk so that we can use the fast path to
		 * fill the rest of it up in future called.  We will count this tuple
		 * later, when the chunk is full.
		 */
		hashtable->current_chunk = chunk;
		hashtable->current_chunk_shared = chunk_shared;
	}
	/*
	 * Update local copy of total tuples so it can be used to compute the load
	 * factor and trigger bucket growth.
	 */
	hashtable->totalTuples = hashtable->shared->ntuples;
	LWLockRelease(&hashtable->shared->chunk_lock);

	Assert(chunk->data == dsa_get_address(hashtable->area, *shared));
	result = (HashJoinTuple) chunk->data;
	memcpy(HJTUPLE_MINTUPLE(result), tuple, tuple->t_len);

	return result;
}

/*
 * Increase the space available for skew hash table tuples if possible.  This
 * is done either by asking for an increase in this backend's allowance
 * (applies to shared hash tables only), or transferring one or more skew
 * buckets into the regular hash table.  This should be called until
 * spaceUsedSkew <= spaceAllowedSkew.
 */
static void
ExecHashHandleSkewOverflow(HashJoinTable hashtable, int bucketno)
{
	HashJoinTuple detached_chain = NULL;
	dsa_pointer detached_chain_shared = InvalidDsaPointer;
	Size	overflow_size;
	Size	regainable_space = 0;
	Size	request_size;
	int		bucket_to_remove;

	Assert(hashtable->spaceUsedSkew > hashtable->spaceAllowedSkew);

	/*
	 * For private hash tables, we'll remove the least useful skew bucket
	 * repeatly until there is no overflow.
	 */
	if (!HashJoinTableIsShared(hashtable))
	{
		while (hashtable->spaceUsedSkew > hashtable->spaceAllowedSkew)
		{
			Assert(hashtable->nSkewBuckets > 0);
			bucket_to_remove =
				hashtable->skewBucketNums[hashtable->nSkewBuckets - 1];
			detached_chain =
				hashtable->skewBucket[bucket_to_remove].tuples.unshared;
			--hashtable->nSkewBuckets;
			hashtable->skewBucket[bucket_to_remove].tuples.unshared = NULL;
			hashtable->skewBucket[bucket_to_remove].active = false;
			ExecHashTransferSkewTuples(hashtable, detached_chain,
									   InvalidDsaPointer);
		}
		return;
	}

	/* For shared hash tables, try to get more space for this backend first. */
	overflow_size = hashtable->spaceUsedSkew - hashtable->spaceAllowedSkew;
	request_size = Max(HASH_CHUNK_SIZE, overflow_size);

	LWLockAcquire(&hashtable->shared->chunk_lock, LW_EXCLUSIVE);

	/* Fast path: grant more space to this backend if possible. */
	if (hashtable->shared->skew_space_used + request_size <=
		hashtable->shared->skew_space_allowed)
	{
		hashtable->spaceUsedSkew = overflow_size;
		hashtable->spaceAllowedSkew = request_size;
		hashtable->shared->skew_space_used += request_size;
		hashtable->shared->size += request_size;
		LWLockRelease(&hashtable->shared->chunk_lock);

		return;
	}

	/*
	 * Otherwise, remove the least useful skew bucket repeatedly until there
	 * is no overflow.
	 */
	hashtable->shared->skew_space_used += overflow_size;
	while (hashtable->shared->skew_space_used >
		   hashtable->shared->skew_space_allowed)
	{
		HashJoinTuple tuple;

		/* Choose a skew bucket to evict. */
		Assert(hashtable->shared->num_skew_buckets > 0);
		bucket_to_remove =
			hashtable->skewBucketNums[hashtable->shared->num_skew_buckets - 1];
		detached_chain_shared = ExecHashDetachSkewBucket(hashtable, bucket_to_remove);
		--hashtable->shared->num_skew_buckets;

		/*
		 * Sum the skew space we can reclaim by freeing this bucket while we
		 * hold the chunk lock.
		 */
		regainable_space = 0;
		tuple = (HashJoinTuple)
			dsa_get_address(hashtable->area, detached_chain_shared);
		while (tuple != NULL)
		{
			regainable_space +=
				HJTUPLE_OVERHEAD + HJTUPLE_MINTUPLE(tuple)->t_len;
			tuple = ExecHashNextTupleInBucket(hashtable, tuple);
		}

		/* Remove from the space used for skew tuples. */
		Assert(hashtable->shared->skew_space_used >= regainable_space);
		hashtable->shared->skew_space_used -= regainable_space;

		/*
		 * We mark this bucket as inactive so that ExecHashGetSkewBucket()
		 * will no longer find it.  For parallel hash builds, there is a race
		 * against concurrent inserters reading the active flag.  That's OK
		 * because any backend that happens to see the bucket as still active
		 * after the above atomic operation will eventually see the tombstone
		 * left behind by ExecHashDetachSkewBucket when it attempts to insert
		 * a tuple, and will bail out at that point.
		 */
		hashtable->skewBucket[bucket_to_remove].active = false;

		/*
		 * If we detached a chain of tuples, transfer them to the main hash
		 * table or batch storage.  Give up the lock while we do so, because
		 * transferring tuples might involve allocating new chunks.
		 */
		if (regainable_space > 0)
		{
			LWLockRelease(&hashtable->shared->chunk_lock);

			detached_chain = (HashJoinTuple)
				dsa_get_address(hashtable->area, detached_chain_shared);
			ExecHashTransferSkewTuples(hashtable, detached_chain,
									   detached_chain_shared);

			LWLockAcquire(&hashtable->shared->chunk_lock, LW_EXCLUSIVE);
		}

		Assert(hashtable->shared->size >= regainable_space);
		hashtable->shared->size -= regainable_space;
	}
	LWLockRelease(&hashtable->shared->chunk_lock);

	hashtable->spaceUsedSkew = 0;
	hashtable->spaceAllowedSkew = 0;
}

/*
 * Detach the chain of tuples in a skew bucket, replacing it with a tombstone
 * value indicating that the bucket is defunct.  Return the head of the chain
 * of tuples that has been detached, so that the caller can free them.
 */
static dsa_pointer
ExecHashDetachSkewBucket(HashJoinTable table, int bucketno)
{
	dsa_pointer head_tuple_shared;

	Assert(HashJoinTableIsShared(table));
	Assert(LWLockHeldByMe(&table->shared->chunk_lock));
	Assert(table->skewBucket != NULL);

	/*
	 * We use compare-and-swap for interlocking with concurrent callers of
	 * ExecHashInsertTupleIntoBucket().  Either the concurrent inserter wins
	 * and we have to try again so that we see the newly inserted head tuple,
	 * or we win and the concurrent inserter tries again and sees the
	 * tombstone.
	 */
	for (;;)
	{
		head_tuple_shared =
			dsa_pointer_atomic_read(&table->skewBucket[bucketno].tuples.shared);
		Assert(head_tuple_shared != SKEW_BUCKET_TOMBSTONE);
		if (dsa_pointer_atomic_compare_exchange(&table->skewBucket[bucketno].tuples.shared,
												&head_tuple_shared,
												SKEW_BUCKET_TOMBSTONE))
			break;
	}

	return head_tuple_shared;
}

/*
 * Free the memory occupied by the skew hash table.  We don't need to do
 * anything for the non-shared case, because then it's in the per-batch memory
 * context and will be automatically freed.
 */
static void
ExecHashFreeSkewTable(HashJoinTable hashtable)
{
	if (HashJoinTableIsShared(hashtable) &&
		DsaPointerIsValid(hashtable->shared->skew_buckets))
	{
		int		i;
		HashSkewBucket *skew_buckets = (HashSkewBucket *)
			dsa_get_address(hashtable->area,
							hashtable->shared->skew_buckets);

		/* Free the tuples in each skew bucket. */
		for (i = 0; i < hashtable->shared->skew_bucket_len; ++i)
		{
			dsa_pointer	shared;
			HashJoinTuple tuple;

			if (!skew_buckets[i].active)
				continue;

			shared = dsa_pointer_atomic_read(&skew_buckets[i].tuples.shared);
			Assert(shared != SKEW_BUCKET_TOMBSTONE);

			tuple = (HashJoinTuple) dsa_get_address(hashtable->area, shared);
			while (tuple != NULL)
			{
				dsa_pointer next_shared = tuple->next.shared;
				HashJoinTuple next = (HashJoinTuple)
					dsa_get_address(hashtable->area, next_shared);

				dsa_free(hashtable->area, shared);
				tuple = next;
				shared = next_shared;
			}
		}

		/* Free the skew bucket arrays. */
		dsa_free(hashtable->area, hashtable->shared->skew_buckets);
		dsa_free(hashtable->area, hashtable->shared->skew_bucket_nums);
		hashtable->shared->skew_buckets = InvalidDsaPointer;
		hashtable->shared->skew_bucket_len = 0;
		hashtable->shared->skew_bucket_nums = InvalidDsaPointer;
		hashtable->shared->num_skew_buckets = 0;
	}
}

/*
 * Insert a tuple at the front of a given bucket identified by number.  For
 * shared hash joins, tuple_shared must be provided, pointing to the tuple in
 * the dsa_area backing the table.  For private hash joins, it should be
 * InvalidDsaPointer.
 *
 * Return false if the bucket is defunct and the caller must now put the tuple
 * into the regular hash table or a batch file.  That can only happen for
 * shared skew buckets; other callers needn't check the return value.
 */
static bool
ExecHashInsertTupleIntoBucket(HashJoinTable table, HashJoinBucketHead *head,
							  HashJoinTuple tuple, dsa_pointer tuple_shared)
{
	if (HashJoinTableIsShared(table))
	{
		Assert(tuple == dsa_get_address(table->area, tuple_shared));
		for (;;)
		{
			tuple->next.shared =
				dsa_pointer_atomic_read(&head->shared);

			if (unlikely(tuple->next.shared == SKEW_BUCKET_TOMBSTONE))
				return false;

			if (dsa_pointer_atomic_compare_exchange(&head->shared,
													&tuple->next.shared,
													tuple_shared))
				break;
		}
	}
	else
	{
		tuple->next.unshared = head->unshared;
		head->unshared = tuple;
	}

	return true;
}

/*
 * Get the first tuple in a given bucket identified by number.
 */
static HashJoinTuple
ExecHashFirstTupleInBucket(HashJoinTable table, int bucketno)
{
	if (HashJoinTableIsShared(table))
	{
		dsa_pointer p =
			dsa_pointer_atomic_read(&table->buckets[bucketno].shared);

		return (HashJoinTuple) dsa_get_address(table->area, p);
	}
	else
		return table->buckets[bucketno].unshared;
}

/*
 * Get the first tuple in a given skew bucket number.
 */
static HashJoinTuple
ExecHashFirstTupleInSkewBucket(HashJoinTable table, int bucketno,
							   dsa_pointer *shared)
{
	if (HashJoinTableIsShared(table))
	{
		dsa_pointer p =
			dsa_pointer_atomic_read(&table->skewBucket[bucketno].tuples.shared);

		if (shared != NULL)
			*shared = p;

		return (HashJoinTuple) dsa_get_address(table->area, p);
	}
	else
		return table->skewBucket[bucketno].tuples.unshared;
}

/*
 * Get the next tuple in the same bucket as 'tuple'.
 */
static HashJoinTuple
ExecHashNextTupleInBucket(HashJoinTable table, HashJoinTuple tuple)
{
	if (HashJoinTableIsShared(table))
		return (HashJoinTuple)
			dsa_get_address(table->area, tuple->next.shared);
	else
		return tuple->next.unshared;
}

/*
 * Take the next available chunk from the queue of chunks being worked on in
 * parallel.  Return NULL if there are none left.  Otherwise return a pointer
 * to the chunk, and set *shared to the DSA pointer to the chunk.
 */
static HashMemoryChunk
ExecHashPopChunkQueueUnlocked(HashJoinTable hashtable, dsa_pointer *shared)
{
	HashMemoryChunk chunk;

	Assert(HashJoinTableIsShared(hashtable));
	Assert(LWLockHeldByMe(&hashtable->shared->chunk_lock));

	if (!DsaPointerIsValid(hashtable->shared->chunk_work_queue))
		return NULL;

	*shared = hashtable->shared->chunk_work_queue;
	chunk = (HashMemoryChunk)
		dsa_get_address(hashtable->area, *shared);
	hashtable->shared->chunk_work_queue = chunk->next.shared;

	return chunk;
}

/*
 * See pop_chunk_unlocked.
 */
static HashMemoryChunk
ExecHashPopChunkQueue(HashJoinTable hashtable, dsa_pointer *shared)
{
	HashMemoryChunk chunk;

	LWLockAcquire(&hashtable->shared->chunk_lock, LW_EXCLUSIVE);
	chunk = ExecHashPopChunkQueueUnlocked(hashtable, shared);
	LWLockRelease(&hashtable->shared->chunk_lock);

	return chunk;
}
