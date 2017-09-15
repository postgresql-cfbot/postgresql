/*-------------------------------------------------------------------------
 *
 * nodeHashjoin.c
 *	  Routines to handle hash join nodes
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeHashjoin.c
 *
 * NOTES:
 *
 * PARALLELISM
 *
 * Hash joins can participate in parallel query execution in two different
 * ways: (1) parallel-oblivious mode, where each backend builds an identical
 * hash table and then probes it with an outer relation that may or may not be
 * partial, and (2) parallel-aware mode where there is a shared hash table
 * that all participants help to build.  A parallel-aware hash join can save
 * time and space by dividing the work up and sharing the result, but has
 * extra communication overheads.
 *
 * Parallel-aware hash joins use the same state machine to track progress
 * through the hash join algorithm as parallel-oblivious hash joins.  In a
 * parallel-aware hash join, there is also a shared 'phase' which co-operating
 * backends use to synchronize their local state machine and program counter
 * with the multi-core join.  The phase is managed by a 'barrier' IPC
 * primitive.  The only way for it to move forward is through the
 * BarrierWait() primitive.  When all attached participants arrive at a
 * BarrierWait() call, the phase advances and the participants are released.
 *
 * When a participant begins working on a parallel hash join, it must first
 * figure out how much progress has already been made, because participants
 * don't wait for each other to begin.  For this reason there are switch
 * statements at key points in the code where we have to synchronize our local
 * state machine with the phase, and then jump to the correct part of the
 * algorithm so that we can get started.
 *
 * While running the algorithm, there are key points in the code where we must
 * wait for all participants to reach the same point before we can continue,
 * in the form of BarrierWait calls.  We cannot beginning building the hash
 * table until it has been created, and we cannot begin probing it until it is
 * entirely built.
 *
 * The phase is an integer which begins at zero and increments one by one, but
 * in the code it is referred to by symbolic names as follows:
 *
 *   PHJ_PHASE_BEGINNING   -- initial phase, before any participant acts
 *   PHJ_PHASE_CREATING	   -- one participant creates the shmem hash table
 *   PHJ_PHASE_BUILDING	   -- all participants build the hash table
 *   PHJ_PHASE_RESIZING	   -- one participant decides whether to expand buckets
 *   PHJ_PHASE_REINSERTING -- all participants reinsert tuples if necessary
 *   PHJ_PHASE_PROBING	   -- all participants probe the hash table
 *   PHJ_PHASE_UNMATCHED   -- all participants scan for unmatched tuples
 *
 * Then follow phases for later batches, in the case where the hash table
 * wouldn't fit in work_mem, so must be spilled to disk.  For each batch n,
 * the phases are encoded into numbers which are represented with macros:
 *
 *   PHJ_PHASE_RESETTING_BATCH(n) -- one participant prepared the hash table
 *   PHJ_PHASE_LOADING_BATCH(n)   -- all participants load the batch
 *   PHJ_PHASE_PROBING_BATCH(n)   -- all participants probe the batch
 *   PHJ_PHASE_UNMATCHED_BATCH(n) -- all participants scan for unmatched
 *
 * The only exception to the rule that the phase only travels in one direction
 * one step at a time is the during a rescan.  In that case, there is a time
 * when only the leader process is running, in between scans.  The leader is
 * then able to reset the Barrier to its initial state safely.
 *
 * If it turns out that we run out of work_mem because the planner
 * underestimated the number of batches required in order for each one to fit
 * in work_mem, then we may need to increase the number of batches at
 * execution time.  This can occur during PHJ_PHASE_BUILDING or
 * PHJ_PHASE_LOADING_BATCH(n), because those are the phases when we are
 * loading data into the hash table and we could discover that work_mem would
 * be exceeded by inserting one more tuple.  In this case a second barrier is
 * used to manage hash table shrinking.  Its phases are:
 *
 *   PHJ_SHRINK_PHASE_BEGINNING  -- initial phase
 *   PHJ_SHRINK_PHASE_CLEARING   -- one participant clears the hash table
 *   PHJ_SHRINK_PHASE_WORKING    -- all participants shrink the hash table
 *   PHJ_SHRINK_PHASE_DECIDING   -- one participant checks for degenerate case
 *
 * The 'degenerate case' refers to the case where our attempt to shrink the
 * hash table failed due to extreme skew that can't be helped by splitting
 * buckets, and we shouldn't try again.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "utils/probes.h"
#include "utils/sharedtuplestore.h"


/*
 * States of the ExecHashJoin state machine
 */
#define HJ_BUILD_HASHTABLE		1
#define HJ_NEED_NEW_OUTER		2
#define HJ_SCAN_BUCKET			3
#define HJ_FILL_OUTER_TUPLE		4
#define HJ_FILL_INNER_TUPLES	5
#define HJ_NEED_NEW_BATCH		6

/* Returns true if doing null-fill on outer relation */
#define HJ_FILL_OUTER(hjstate)	((hjstate)->hj_NullInnerTupleSlot != NULL)
/* Returns true if doing null-fill on inner relation */
#define HJ_FILL_INNER(hjstate)	((hjstate)->hj_NullOuterTupleSlot != NULL)

static TupleTableSlot *ExecHashJoinOuterGetTuple(PlanState *outerNode,
						  HashJoinState *hjstate,
						  uint32 *hashvalue);
static TupleTableSlot *ExecHashJoinGetSavedTuple(HashJoinState *hjstate,
						  BufFile *file,
						  uint32 *hashvalue,
						  TupleTableSlot *tupleSlot);
static bool ExecHashJoinNewBatch(HashJoinState *hjstate);
static void ExecHashJoinLoadBatch(HashJoinState *hjstate);

/* ----------------------------------------------------------------
 *		ExecHashJoin
 *
 *		This function implements the Hybrid Hashjoin algorithm.
 *
 *		Note: the relation we build hash table on is the "inner"
 *			  the other one is "outer".
 * ----------------------------------------------------------------
 */
static TupleTableSlot *			/* return: a tuple or NULL */
ExecHashJoin(PlanState *pstate)
{
	HashJoinState *node = castNode(HashJoinState, pstate);
	PlanState  *outerNode;
	HashState  *hashNode;
	ExprState  *joinqual;
	ExprState  *otherqual;
	ExprContext *econtext;
	HashJoinTable hashtable;
	TupleTableSlot *outerTupleSlot;
	uint32		hashvalue;
	int			batchno;

	/*
	 * get information from HashJoin node
	 */
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	hashNode = (HashState *) innerPlanState(node);
	outerNode = outerPlanState(node);
	hashtable = node->hj_HashTable;
	econtext = node->js.ps.ps_ExprContext;

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * run the hash join state machine
	 */
	for (;;)
	{
		/*
		 * It's possible to iterate this loop many times before returning a
		 * tuple, in some pathological cases such as needing to move much of
		 * the current batch to a later batch.  So let's check for interrupts
		 * each time through.
		 */
		CHECK_FOR_INTERRUPTS();

		switch (node->hj_JoinState)
		{
			case HJ_BUILD_HASHTABLE:

				/*
				 * First time through: build hash table for inner relation.
				 */
				Assert(hashtable == NULL);

				/*
				 * If the outer relation is completely empty, and it's not
				 * right/full join, we can quit without building the hash
				 * table.  However, for an inner join it is only a win to
				 * check this when the outer relation's startup cost is less
				 * than the projected cost of building the hash table.
				 * Otherwise it's best to build the hash table first and see
				 * if the inner relation is empty.  (When it's a left join, we
				 * should always make this check, since we aren't going to be
				 * able to skip the join on the strength of an empty inner
				 * relation anyway.)
				 *
				 * If we are rescanning the join, we make use of information
				 * gained on the previous scan: don't bother to try the
				 * prefetch if the previous scan found the outer relation
				 * nonempty. This is not 100% reliable since with new
				 * parameters the outer relation might yield different
				 * results, but it's a good heuristic.
				 *
				 * The only way to make the check is to try to fetch a tuple
				 * from the outer plan node.  If we succeed, we have to stash
				 * it away for later consumption by ExecHashJoinOuterGetTuple.
				 */
				if (HJ_FILL_INNER(node))
				{
					/* no chance to not build the hash table */
					node->hj_FirstOuterTupleSlot = NULL;
				}
				else if (hashNode->shared_table_data != NULL)
				{
					/*
					 * The empty-outer optimization is not implemented for
					 * shared hash tables yet.
					 */
					node->hj_FirstOuterTupleSlot = NULL;
				}
				else if (HJ_FILL_OUTER(node) ||
						 (outerNode->plan->startup_cost < hashNode->ps.plan->total_cost &&
						  !node->hj_OuterNotEmpty))
				{
					node->hj_FirstOuterTupleSlot = ExecProcNode(outerNode);
					if (TupIsNull(node->hj_FirstOuterTupleSlot))
					{
						node->hj_OuterNotEmpty = false;
						return NULL;
					}
					else
						node->hj_OuterNotEmpty = true;
				}
				else
					node->hj_FirstOuterTupleSlot = NULL;

				/*
				 * create the hash table
				 */
				hashtable = ExecHashTableCreate(hashNode,
												node->hj_HashOperators,
												HJ_FILL_INNER(node));
				node->hj_HashTable = hashtable;

				/*
				 * execute the Hash node, to build the hash table
				 */
				hashNode->hashtable = hashtable;
				(void) MultiExecProcNode((PlanState *) hashNode);

				if (HashJoinTableIsShared(hashtable))
				{
					Assert(BarrierPhase(&hashtable->shared->barrier) >=
						   PHJ_PHASE_BUILDING);

					/*
					 * There is a deadlock avoidance check at the end of
					 * probing.  It's unlikely, but we also need to check if
					 * we're so late to start that probing has already
					 * finished, so that it's already been determined whether
					 * leader or workers can continue.
					 *
					 * In this case there can't be any batch files created by
					 * us, because we missed the building phase, so there is
					 * nothing to do but exit early.
					 */
					if (BarrierPhase(&hashtable->shared->barrier) > PHJ_PHASE_PROBING &&
						!LeaderGateCanContinue(&hashtable->shared->leader_gate))
					{
						BarrierDetach(&hashtable->shared->barrier);
						hashtable->detached_early = true;
						return NULL;
					}
				}

				/*
				 * If the inner relation is completely empty, and we're not
				 * doing a left outer join, we can quit without scanning the
				 * outer relation.  This is safe for shared hash joins because
				 * MultiExecHash sums hashtable->totalTuples across
				 * participants.
				 */
				if (hashtable->totalTuples == 0 && !HJ_FILL_OUTER(node))
					return NULL;

				/*
				 * need to remember whether nbatch has increased since we
				 * began scanning the outer relation
				 */
				hashtable->nbatch_outstart = hashtable->nbatch;

				/*
				 * Reset OuterNotEmpty for scan.  (It's OK if we fetched a
				 * tuple above, because ExecHashJoinOuterGetTuple will
				 * immediately set it again.)
				 */
				node->hj_OuterNotEmpty = false;

				if (HashJoinTableIsShared(hashtable))
				{
					Barrier *barrier = &hashtable->shared->barrier;
					int phase = BarrierPhase(barrier);

					/*
					 * There is a deadlock avoidance check at the end of
					 * probing.  It's unlikely, but we also need to check if
					 * we're so late to start that probing has already
					 * finished, so that it's already been determined whether
					 * leader or workers can continue.
					 */
					if (BarrierPhase(&hashtable->shared->barrier) > PHJ_PHASE_PROBING &&
						!LeaderGateCanContinue(&hashtable->shared->leader_gate))
					{
						BarrierDetach(&hashtable->shared->barrier);
						hashtable->detached_early = true;
						return NULL;
					}

					/*
					 * Map the current phase to the appropriate initial state
					 * for this participant, so we can get started.
					 * MultiExecHash made sure that the parallel hash join has
					 * reached at least PHJ_PHASE_PROBING, but it's possible
					 * that this participant joined the work later than that,
					 * so we need another switch statement here to get our
					 * local state machine in sync.
					 */
					Assert(BarrierPhase(barrier) >= PHJ_PHASE_PROBING);
					switch (PHJ_PHASE_TO_SUBPHASE(phase))
					{
						case PHJ_SUBPHASE_RESETTING:
							/* Wait for serial phase to finish. */
							BarrierWait(barrier, WAIT_EVENT_HASHJOIN_RESETTING);
							Assert(PHJ_PHASE_TO_SUBPHASE(BarrierPhase(barrier)) ==
								   PHJ_SUBPHASE_LOADING);
							/* fall through */
						case PHJ_SUBPHASE_LOADING:
							/* Help load the current batch. */
							ExecHashUpdate(hashtable);
							ExecHashJoinLoadBatch(node);
							Assert(PHJ_PHASE_TO_SUBPHASE(BarrierPhase(barrier)) ==
								   PHJ_SUBPHASE_PROBING);
							/* fall through */
						case PHJ_SUBPHASE_PROBING:
							/* Help probe the hashtable. */
							ExecHashUpdate(hashtable);
							sts_begin_partial_scan(hashtable->shared_outer_batches,
												   hashtable->curbatch);
							node->hj_JoinState = HJ_NEED_NEW_OUTER;
							break;
						case PHJ_SUBPHASE_UNMATCHED:
							/* Help scan for unmatched inner tuples. */
							ExecHashUpdate(hashtable);
							ExecPrepHashTableForUnmatched(node);
							node->hj_JoinState = HJ_FILL_INNER_TUPLES;
							break;
					}
					continue;
				}
				else
					node->hj_JoinState = HJ_NEED_NEW_OUTER;

				/* FALL THRU */

			case HJ_NEED_NEW_OUTER:

				if (HashJoinTableIsShared(hashtable))
				{
					Assert(PHJ_PHASE_TO_BATCHNO(BarrierPhase(&hashtable->shared->barrier)) ==
						   hashtable->curbatch);
					Assert(PHJ_PHASE_TO_SUBPHASE(BarrierPhase(&hashtable->shared->barrier)) ==
						   PHJ_SUBPHASE_PROBING);
				}

				/*
				 * We don't have an outer tuple, try to get the next one
				 */
				outerTupleSlot = ExecHashJoinOuterGetTuple(outerNode,
														   node,
														   &hashvalue);
				if (TupIsNull(outerTupleSlot))
				{
					/* end of batch, or maybe whole join */

					if (HashJoinTableIsShared(hashtable))
					{
						/*
						 * An important optimization: if this is a
						 * single-batch join and not an outer join, there is
						 * no reason to synchronize again when we've finished
						 * probing.
						 */
						Assert(BarrierPhase(&hashtable->shared->barrier) ==
							   PHJ_PHASE_PROBING_BATCH(hashtable->curbatch));
						if (hashtable->nbatch == 1 && !HJ_FILL_INNER(node))
							return NULL;	/* end of join */

						/*
						 * Check if we are a leader that can't go further than
						 * probing the first batch, to avoid risk of deadlock
						 * against workers.
						 */
						if (!LeaderGateCanContinue(&hashtable->shared->leader_gate))
						{
							/*
							 * Other backends will need to handle all future
							 * batches written by me.  We don't detach until
							 * after we've finished writing to all batches so
							 * that they are flushed, otherwise another
							 * participant might try to read them too soon.
							 */
							sts_end_write_all_partitions(hashNode->shared_inner_batches);
							sts_end_write_all_partitions(hashNode->shared_outer_batches);
							BarrierDetach(&hashtable->shared->barrier);
							hashtable->detached_early = true;
							return NULL;
						}

						/*
						 * We can't start searching for unmatched tuples until
						 * all participants have finished probing, so we
						 * synchronize here.
						 */
						Assert(BarrierPhase(&hashtable->shared->barrier) ==
							   PHJ_PHASE_PROBING_BATCH(hashtable->curbatch));
						if (BarrierWait(&hashtable->shared->barrier,
										WAIT_EVENT_HASHJOIN_PROBING))
						{
							/* Serial phase: prepare for unmatched. */
							if (HJ_FILL_INNER(node))
							{
								hashtable->shared->chunk_work_queue =
									hashtable->shared->chunks;
								hashtable->shared->chunks = InvalidDsaPointer;
								hashtable->shared->current_skew_bucketno = 0;
							}
						}
						Assert(BarrierPhase(&hashtable->shared->barrier) ==
							   PHJ_PHASE_UNMATCHED_BATCH(hashtable->curbatch));
					}

					if (HJ_FILL_INNER(node))
					{
						/* set up to scan for unmatched inner tuples */
						ExecPrepHashTableForUnmatched(node);
						node->hj_JoinState = HJ_FILL_INNER_TUPLES;
					}
					else
						node->hj_JoinState = HJ_NEED_NEW_BATCH;
					continue;
				}

				econtext->ecxt_outertuple = outerTupleSlot;
				node->hj_MatchedOuter = false;

				/*
				 * Find the corresponding bucket for this tuple in the main
				 * hash table or skew hash table.
				 */
				node->hj_CurHashValue = hashvalue;
				ExecHashGetBucketAndBatch(hashtable, hashvalue,
										  &node->hj_CurBucketNo, &batchno);
				node->hj_CurSkewBucketNo = ExecHashGetSkewBucket(hashtable,
																 hashvalue);
				node->hj_CurTuple = NULL;

				/*
				 * The tuple might not belong to the current batch (where
				 * "current batch" includes the skew buckets if any).
				 */
				if (batchno != hashtable->curbatch &&
					node->hj_CurSkewBucketNo == INVALID_SKEW_BUCKET_NO)
				{
					/*
					 * Need to postpone this outer tuple to a later batch.
					 * Save it in the corresponding outer-batch file.
					 */
					Assert(batchno > hashtable->curbatch);
					if (HashJoinTableIsShared(hashtable))
						sts_puttuple(hashtable->shared_outer_batches,
									 batchno, &hashvalue,
									 ExecFetchSlotMinimalTuple(outerTupleSlot));
					else
						ExecHashJoinSaveTuple(ExecFetchSlotMinimalTuple(outerTupleSlot),
											  hashvalue,
											  &hashtable->outerBatchFile[batchno]);
					/* Loop around, staying in HJ_NEED_NEW_OUTER state */
					continue;
				}

				/* OK, let's scan the bucket for matches */
				node->hj_JoinState = HJ_SCAN_BUCKET;

				/* FALL THRU */

			case HJ_SCAN_BUCKET:

				/*
				 * Scan the selected hash bucket for matches to current outer
				 */
				if (!ExecScanHashBucket(node, econtext))
				{
					/* out of matches; check for possible outer-join fill */
					node->hj_JoinState = HJ_FILL_OUTER_TUPLE;
					continue;
				}

				/*
				 * We've got a match, but still need to test non-hashed quals.
				 * ExecScanHashBucket already set up all the state needed to
				 * call ExecQual.
				 *
				 * If we pass the qual, then save state for next call and have
				 * ExecProject form the projection, store it in the tuple
				 * table, and return the slot.
				 *
				 * Only the joinquals determine tuple match status, but all
				 * quals must pass to actually return the tuple.
				 */
				if (joinqual == NULL || ExecQual(joinqual, econtext))
				{
					node->hj_MatchedOuter = true;
					HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->hj_CurTuple));

					/* In an antijoin, we never return a matched tuple */
					if (node->js.jointype == JOIN_ANTI)
					{
						node->hj_JoinState = HJ_NEED_NEW_OUTER;
						continue;
					}

					/*
					 * If we only need to join to the first matching inner
					 * tuple, then consider returning this one, but after that
					 * continue with next outer tuple.
					 */
					if (node->js.single_match)
						node->hj_JoinState = HJ_NEED_NEW_OUTER;

					if (otherqual == NULL || ExecQual(otherqual, econtext))
						return ExecProject(node->js.ps.ps_ProjInfo);
					else
						InstrCountFiltered2(node, 1);
				}
				else
					InstrCountFiltered1(node, 1);
				break;

			case HJ_FILL_OUTER_TUPLE:

				/*
				 * The current outer tuple has run out of matches, so check
				 * whether to emit a dummy outer-join tuple.  Whether we emit
				 * one or not, the next state is NEED_NEW_OUTER.
				 */
				node->hj_JoinState = HJ_NEED_NEW_OUTER;

				if (!node->hj_MatchedOuter &&
					HJ_FILL_OUTER(node))
				{
					/*
					 * Generate a fake join tuple with nulls for the inner
					 * tuple, and return it if it passes the non-join quals.
					 */
					econtext->ecxt_innertuple = node->hj_NullInnerTupleSlot;

					if (otherqual == NULL || ExecQual(otherqual, econtext))
						return ExecProject(node->js.ps.ps_ProjInfo);
					else
						InstrCountFiltered2(node, 1);
				}
				break;

			case HJ_FILL_INNER_TUPLES:

				/*
				 * We have finished a batch, but we are doing right/full join,
				 * so any unmatched inner tuples in the hashtable have to be
				 * emitted before we continue to the next batch.
				 */
				if (!ExecScanHashTableForUnmatched(node, econtext))
				{
					/* no more unmatched tuples */
					node->hj_JoinState = HJ_NEED_NEW_BATCH;
					continue;
				}

				/*
				 * Generate a fake join tuple with nulls for the outer tuple,
				 * and return it if it passes the non-join quals.
				 */
				econtext->ecxt_outertuple = node->hj_NullOuterTupleSlot;

				if (otherqual == NULL || ExecQual(otherqual, econtext))
					return ExecProject(node->js.ps.ps_ProjInfo);
				else
					InstrCountFiltered2(node, 1);
				break;

			case HJ_NEED_NEW_BATCH:

				/*
				 * Try to advance to next batch.  Done if there are no more.
				 */
				if (!ExecHashJoinNewBatch(node))
					return NULL;	/* end of join */

				/* We'll need to read tuples from the outer batch. */
				if (HashJoinTableIsShared(hashtable))
					sts_begin_partial_scan(hashtable->shared_outer_batches,
										   hashtable->curbatch);

				node->hj_JoinState = HJ_NEED_NEW_OUTER;
				break;

			default:
				elog(ERROR, "unrecognized hashjoin state: %d",
					 (int) node->hj_JoinState);
		}
	}
}

/* ----------------------------------------------------------------
 *		ExecInitHashJoin
 *
 *		Init routine for HashJoin node.
 * ----------------------------------------------------------------
 */
HashJoinState *
ExecInitHashJoin(HashJoin *node, EState *estate, int eflags)
{
	HashJoinState *hjstate;
	Plan	   *outerNode;
	Hash	   *hashNode;
	List	   *lclauses;
	List	   *rclauses;
	List	   *hoperators;
	ListCell   *l;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	hjstate = makeNode(HashJoinState);
	hjstate->js.ps.plan = (Plan *) node;
	hjstate->js.ps.state = estate;
	hjstate->js.ps.ExecProcNode = ExecHashJoin;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &hjstate->js.ps);

	/*
	 * initialize child expressions
	 */
	hjstate->js.ps.qual =
		ExecInitQual(node->join.plan.qual, (PlanState *) hjstate);
	hjstate->js.jointype = node->join.jointype;
	hjstate->js.joinqual =
		ExecInitQual(node->join.joinqual, (PlanState *) hjstate);
	hjstate->hashclauses =
		ExecInitQual(node->hashclauses, (PlanState *) hjstate);

	/*
	 * initialize child nodes
	 *
	 * Note: we could suppress the REWIND flag for the inner input, which
	 * would amount to betting that the hash will be a single batch.  Not
	 * clear if this would be a win or not.
	 */
	outerNode = outerPlan(node);
	hashNode = (Hash *) innerPlan(node);

	outerPlanState(hjstate) = ExecInitNode(outerNode, estate, eflags);
	innerPlanState(hjstate) = ExecInitNode((Plan *) hashNode, estate, eflags);

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &hjstate->js.ps);
	hjstate->hj_OuterTupleSlot = ExecInitExtraTupleSlot(estate);

	/*
	 * detect whether we need only consider the first matching inner tuple
	 */
	hjstate->js.single_match = (node->join.inner_unique ||
								node->join.jointype == JOIN_SEMI);

	/* set up null tuples for outer joins, if needed */
	switch (node->join.jointype)
	{
		case JOIN_INNER:
		case JOIN_SEMI:
			break;
		case JOIN_LEFT:
		case JOIN_ANTI:
			hjstate->hj_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate,
									  ExecGetResultType(innerPlanState(hjstate)));
			break;
		case JOIN_RIGHT:
			hjstate->hj_NullOuterTupleSlot =
				ExecInitNullTupleSlot(estate,
									  ExecGetResultType(outerPlanState(hjstate)));
			break;
		case JOIN_FULL:
			hjstate->hj_NullOuterTupleSlot =
				ExecInitNullTupleSlot(estate,
									  ExecGetResultType(outerPlanState(hjstate)));
			hjstate->hj_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate,
									  ExecGetResultType(innerPlanState(hjstate)));
			break;
		default:
			elog(ERROR, "unrecognized join type: %d",
				 (int) node->join.jointype);
	}

	/*
	 * now for some voodoo.  our temporary tuple slot is actually the result
	 * tuple slot of the Hash node (which is our inner plan).  we can do this
	 * because Hash nodes don't return tuples via ExecProcNode() -- instead
	 * the hash join node uses ExecScanHashBucket() to get at the contents of
	 * the hash table.  -cim 6/9/91
	 */
	{
		HashState  *hashstate = (HashState *) innerPlanState(hjstate);
		TupleTableSlot *slot = hashstate->ps.ps_ResultTupleSlot;

		hjstate->hj_HashTupleSlot = slot;
	}

	/*
	 * initialize tuple type and projection info
	 */
	ExecAssignResultTypeFromTL(&hjstate->js.ps);
	ExecAssignProjectionInfo(&hjstate->js.ps, NULL);

	ExecSetSlotDescriptor(hjstate->hj_OuterTupleSlot,
						  ExecGetResultType(outerPlanState(hjstate)));

	/*
	 * initialize hash-specific info
	 */
	hjstate->hj_HashTable = NULL;
	hjstate->hj_FirstOuterTupleSlot = NULL;

	hjstate->hj_CurHashValue = 0;
	hjstate->hj_CurBucketNo = 0;
	hjstate->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
	hjstate->hj_CurTuple = NULL;

	/*
	 * Deconstruct the hash clauses into outer and inner argument values, so
	 * that we can evaluate those subexpressions separately.  Also make a list
	 * of the hash operator OIDs, in preparation for looking up the hash
	 * functions to use.
	 */
	lclauses = NIL;
	rclauses = NIL;
	hoperators = NIL;
	foreach(l, node->hashclauses)
	{
		OpExpr	   *hclause = lfirst_node(OpExpr, l);

		lclauses = lappend(lclauses, ExecInitExpr(linitial(hclause->args),
												  (PlanState *) hjstate));
		rclauses = lappend(rclauses, ExecInitExpr(lsecond(hclause->args),
												  (PlanState *) hjstate));
		hoperators = lappend_oid(hoperators, hclause->opno);
	}
	hjstate->hj_OuterHashKeys = lclauses;
	hjstate->hj_InnerHashKeys = rclauses;
	hjstate->hj_HashOperators = hoperators;
	/* child Hash node needs to evaluate inner hash keys, too */
	((HashState *) innerPlanState(hjstate))->hashkeys = rclauses;

	hjstate->hj_JoinState = HJ_BUILD_HASHTABLE;
	hjstate->hj_MatchedOuter = false;
	hjstate->hj_OuterNotEmpty = false;

	return hjstate;
}

/* ----------------------------------------------------------------
 *		ExecEndHashJoin
 *
 *		clean up routine for HashJoin node
 * ----------------------------------------------------------------
 */
void
ExecEndHashJoin(HashJoinState *node)
{
	/*
	 * Free hash table
	 */
	if (node->hj_HashTable)
	{
		ExecHashTableDestroy(node->hj_HashTable);
		node->hj_HashTable = NULL;
	}

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->js.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->hj_OuterTupleSlot);
	ExecClearTuple(node->hj_HashTupleSlot);

	/*
	 * clean up subtrees
	 */
	ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));
}

/*
 * ExecHashJoinOuterGetTuple
 *
 *		get the next outer tuple for hashjoin: either by
 *		executing the outer plan node in the first pass, or from
 *		the temp files for the hashjoin batches.
 *
 * Returns a null slot if no more outer tuples (within the current batch).
 *
 * On success, the tuple's hash value is stored at *hashvalue --- this is
 * either originally computed, or re-read from the temp file.
 */
static TupleTableSlot *
ExecHashJoinOuterGetTuple(PlanState *outerNode,
						  HashJoinState *hjstate,
						  uint32 *hashvalue)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	int			curbatch = hashtable->curbatch;
	TupleTableSlot *slot;

	if (curbatch == 0)			/* if it is the first pass */
	{
		/*
		 * Check to see if first outer tuple was already fetched by
		 * ExecHashJoin() and not used yet.
		 */
		slot = hjstate->hj_FirstOuterTupleSlot;
		if (!TupIsNull(slot))
			hjstate->hj_FirstOuterTupleSlot = NULL;
		else
			slot = ExecProcNode(outerNode);

		while (!TupIsNull(slot))
		{
			/*
			 * We have to compute the tuple's hash value.
			 */
			ExprContext *econtext = hjstate->js.ps.ps_ExprContext;

			econtext->ecxt_outertuple = slot;
			if (ExecHashGetHashValue(hashtable, econtext,
									 hjstate->hj_OuterHashKeys,
									 true,	/* outer tuple */
									 HJ_FILL_OUTER(hjstate),
									 hashvalue))
			{
				/* remember outer relation is not empty for possible rescan */
				hjstate->hj_OuterNotEmpty = true;

				return slot;
			}

			/*
			 * That tuple couldn't match because of a NULL, so discard it and
			 * continue with the next one.
			 */
			slot = ExecProcNode(outerNode);
		}
	}
	else if (curbatch < hashtable->nbatch)
	{
		if (HashJoinTableIsShared(hashtable))
		{
			MinimalTuple tuple;

			tuple = sts_gettuple(hashtable->shared_outer_batches, hashvalue);
			if (tuple != NULL)
			{
				slot = ExecStoreMinimalTuple(tuple,
											 hjstate->hj_OuterTupleSlot,
											 false);
				return slot;
			}
			else
				ExecClearTuple(hjstate->hj_OuterTupleSlot);
		}
		else
		{
			BufFile    *file = hashtable->outerBatchFile[curbatch];

			/*
			 * In outer-join cases, we could get here even though the batch file
			 * is empty.
			 */
			if (file == NULL)
				return NULL;

			slot = ExecHashJoinGetSavedTuple(hjstate,
											 file,
											 hashvalue,
											 hjstate->hj_OuterTupleSlot);
			if (!TupIsNull(slot))
				return slot;
		}
	}

	/* End of this batch */
	return NULL;
}

/*
 * ExecHashJoinNewBatch
 *		switch to a new hashjoin batch
 *
 * Returns true if successful, false if there are no more batches.
 */
static bool
ExecHashJoinNewBatch(HashJoinState *hjstate)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	int			nbatch;
	int			curbatch;

	nbatch = hashtable->nbatch;
	curbatch = hashtable->curbatch;

	if (HashJoinTableIsShared(hashtable))
		Assert(BarrierPhase(&hashtable->shared->barrier) ==
			   PHJ_PHASE_UNMATCHED_BATCH(curbatch));

	if (curbatch > 0)
	{
		/*
		 * We no longer need the previous outer batch file; close it right
		 * away to free disk space.  SharedTuplestore will take care of this
		 * for shared hash tables.
		 */
		if (!HashJoinTableIsShared(hashtable))
		{
			if (hashtable->outerBatchFile[curbatch])
				BufFileClose(hashtable->outerBatchFile[curbatch]);
			hashtable->outerBatchFile[curbatch] = NULL;
		}
	}
	else						/* we just finished the first batch */
	{
		/*
		 * Reset some of the skew optimization state variables, since we no
		 * longer need to consider skew tuples after the first batch. The
		 * memory context reset we are about to do will release the skew
		 * hashtable itself, unless it is shared.  The shared skew table
		 * will be freed in ExecHashTableReset().
		 */
		hashtable->skewBucket = NULL;
		hashtable->skewBucketNums = NULL;
		hashtable->nSkewBuckets = 0;
		hashtable->spaceUsedSkew = 0;
	}

	/*
	 * We can always skip over any batches that are completely empty on both
	 * sides.  We can sometimes skip over batches that are empty on only one
	 * side, but there are exceptions:
	 *
	 * 1. In a left/full outer join, we have to process outer batches even if
	 * the inner batch is empty.  Similarly, in a right/full outer join, we
	 * have to process inner batches even if the outer batch is empty.
	 *
	 * 2. If we have increased nbatch since the initial estimate, we have to
	 * scan inner batches since they might contain tuples that need to be
	 * reassigned to later inner batches.
	 *
	 * 3. Similarly, if we have increased nbatch since starting the outer
	 * scan, we have to rescan outer batches in case they contain tuples that
	 * need to be reassigned.
	 *
	 * 4. In a join with a shared hash table, we can't immediately see if a
	 * batch is empty, without trying to load it.
	 */
	curbatch++;
	while (!HashJoinTableIsShared(hashtable) &&
		   curbatch < nbatch &&
		   (hashtable->outerBatchFile[curbatch] == NULL ||
			hashtable->innerBatchFile[curbatch] == NULL))
	{
		if (hashtable->outerBatchFile[curbatch] &&
			HJ_FILL_OUTER(hjstate))
			break;				/* must process due to rule 1 */
		if (hashtable->innerBatchFile[curbatch] &&
			HJ_FILL_INNER(hjstate))
			break;				/* must process due to rule 1 */
		if (hashtable->innerBatchFile[curbatch] &&
			nbatch != hashtable->nbatch_original)
			break;				/* must process due to rule 2 */
		if (hashtable->outerBatchFile[curbatch] &&
			nbatch != hashtable->nbatch_outstart)
			break;				/* must process due to rule 3 */
		/* We can ignore this batch. */
		/* Release associated temp files right away. */
		if (hashtable->innerBatchFile[curbatch])
			BufFileClose(hashtable->innerBatchFile[curbatch]);
		hashtable->innerBatchFile[curbatch] = NULL;
		if (hashtable->outerBatchFile[curbatch])
			BufFileClose(hashtable->outerBatchFile[curbatch]);
		hashtable->outerBatchFile[curbatch] = NULL;
		curbatch++;
	}

	if (curbatch >= nbatch)
		return false;			/* no more batches */

	hashtable->curbatch = curbatch;

	/* Clear the hash table and load the batch. */
	ExecHashTableReset(hashtable);
	ExecHashJoinLoadBatch(hjstate);

	return true;
}

static void
ExecHashJoinLoadBatch(HashJoinState *hjstate)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	int			curbatch = hashtable->curbatch;
	TupleTableSlot *slot;
	uint32		hashvalue;

	/*
	 * NOTE: some tuples may be sent to future batches.  Also, it is
	 * possible for hashtable->nbatch to be increased here!
	 */

	if (HashJoinTableIsShared(hashtable))
	{
		MinimalTuple tuple;

		Assert(BarrierPhase(&hashtable->shared->barrier) ==
			   PHJ_PHASE_LOADING_BATCH(hashtable->curbatch));

		/*
		 * Shrinking may be triggered while loading if work_mem is exceeded.
		 * We need to be attached to shrink_barrier so that we can coordinate
		 * that among participants.
		 */
		BarrierAttach(&hashtable->shared->shrink_barrier);

		sts_begin_partial_scan(hashtable->shared_inner_batches, curbatch);
		while ((tuple = sts_gettuple(hashtable->shared_inner_batches,
									 &hashvalue)))
		{
			slot = ExecStoreMinimalTuple(tuple, hjstate->hj_HashTupleSlot,
										 false);
			ExecHashTableInsert(hashtable, slot, hashvalue);
		}
		ExecClearTuple(hjstate->hj_HashTupleSlot);

		BarrierDetach(&hashtable->shared->shrink_barrier);

		/*
		 * Wait until all participants have finished loading their portion of
		 * the hash table.
		 */
		Assert(BarrierPhase(&hashtable->shared->barrier) ==
			   PHJ_PHASE_LOADING_BATCH(hashtable->curbatch));
		BarrierWait(&hashtable->shared->barrier, WAIT_EVENT_HASHJOIN_LOADING);
		Assert(BarrierPhase(&hashtable->shared->barrier) ==
			   PHJ_PHASE_PROBING_BATCH(hashtable->curbatch));
	}
	else
	{
		BufFile    *innerFile;

		/* The reset position was reset to the start in ExecHashTableReset. */
		innerFile = hashtable->innerBatchFile[curbatch];
		while ((slot = ExecHashJoinGetSavedTuple(hjstate,
												 innerFile,
												 &hashvalue,
												 hjstate->hj_HashTupleSlot)))
			ExecHashTableInsert(hashtable, slot, hashvalue);
	}
}

/*
 * ExecHashJoinSaveTuple
 *		save a tuple to a batch file.
 *
 * The data recorded in the file for each tuple is its hash value,
 * then the tuple in MinimalTuple format.
 *
 * Note: it is important always to call this in the regular executor
 * context, not in a shorter-lived context; else the temp file buffers
 * will get messed up.
 */
void
ExecHashJoinSaveTuple(MinimalTuple tuple, uint32 hashvalue,
					  BufFile **fileptr)
{
	BufFile    *file = *fileptr;
	size_t		written;

	if (file == NULL)
	{
		/* First write to this batch file, so open it. */
		file = BufFileCreateTemp(false);
		*fileptr = file;
	}

	written = BufFileWrite(file, (void *) &hashvalue, sizeof(uint32));
	if (written != sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to hash-join temporary file: %m")));

	written = BufFileWrite(file, (void *) tuple, tuple->t_len);
	if (written != tuple->t_len)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to hash-join temporary file: %m")));
}

/*
 * ExecHashJoinGetSavedTuple
 *		read the next tuple from a batch file.  Return NULL if no more.
 *
 * On success, *hashvalue is set to the tuple's hash value, and the tuple
 * itself is stored in the given slot.
 */
static TupleTableSlot *
ExecHashJoinGetSavedTuple(HashJoinState *hjstate,
						  BufFile *file,
						  uint32 *hashvalue,
						  TupleTableSlot *tupleSlot)
{
	uint32		header[2];
	size_t		nread;
	MinimalTuple tuple;

	/*
	 * We check for interrupts here because this is typically taken as an
	 * alternative code path to an ExecProcNode() call, which would include
	 * such a check.
	 */
	CHECK_FOR_INTERRUPTS();

	/*
	 * Since both the hash value and the MinimalTuple length word are uint32,
	 * we can read them both in one BufFileRead() call without any type
	 * cheating.
	 */
	nread = BufFileRead(file, (void *) header, sizeof(header));
	if (nread == 0)				/* end of file */
	{
		ExecClearTuple(tupleSlot);
		return NULL;
	}
	if (nread != sizeof(header))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from hash-join temporary file: %m")));
	*hashvalue = header[0];
	tuple = (MinimalTuple) palloc(header[1]);
	tuple->t_len = header[1];
	nread = BufFileRead(file,
						(void *) ((char *) tuple + sizeof(uint32)),
						header[1] - sizeof(uint32));
	if (nread != header[1] - sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from hash-join temporary file: %m")));
	return ExecStoreMinimalTuple(tuple, tupleSlot, true);
}


void
ExecReScanHashJoin(HashJoinState *node)
{
	/*
	 * In a multi-batch join, we currently have to do rescans the hard way,
	 * primarily because batch temp files may have already been released. But
	 * if it's a single-batch join, and there is no parameter change for the
	 * inner subnode, then we can just re-use the existing hash table without
	 * rebuilding it.
	 */
	if (node->hj_HashTable != NULL)
	{
		if (HashJoinTableIsShared(node->hj_HashTable))
		{
			/*
			 * Only the leader is running now, so we can reinitialize the
			 * shared state.  It was originally initialized by
			 * ExecHashJoinInitializeDSM.
			 */
			Assert(!IsParallelWorker());
			BarrierInit(&node->hj_HashTable->shared->barrier, 0);
			BarrierInit(&node->hj_HashTable->shared->shrink_barrier, 0);
			node->hj_HashTable->shared->grow_enabled = true;
			LeaderGateInit(&node->hj_HashTable->shared->leader_gate);
		}

		if (node->hj_HashTable->nbatch == 1 &&
			node->js.ps.righttree->chgParam == NULL)
		{
			/*
			 * Okay to reuse the hash table; needn't rescan inner, either.
			 *
			 * However, if it's a right/full join, we'd better reset the
			 * inner-tuple match flags contained in the table.
			 */
			if (HJ_FILL_INNER(node))
				ExecHashTableResetMatchFlags(node->hj_HashTable);

			if (HashJoinTableIsShared(node->hj_HashTable))
			{
				/* Reattach and fast-forward to the probing phase. */
				BarrierAttach(&node->hj_HashTable->shared->barrier);
				while (BarrierPhase(&node->hj_HashTable->shared->barrier)
					   < PHJ_PHASE_PROBING)
					BarrierWait(&node->hj_HashTable->shared->barrier,
								WAIT_EVENT_HASHJOIN_REWINDING);
				LeaderGateAttach(&node->hj_HashTable->shared->leader_gate);
			}

			/*
			 * Also, we need to reset our state about the emptiness of the
			 * outer relation, so that the new scan of the outer will update
			 * it correctly if it turns out to be empty this time. (There's no
			 * harm in clearing it now because ExecHashJoin won't need the
			 * info.  In the other cases, where the hash table doesn't exist
			 * or we are destroying it, we leave this state alone because
			 * ExecHashJoin will need it the first time through.)
			 */
			node->hj_OuterNotEmpty = false;

			/* ExecHashJoin can skip the BUILD_HASHTABLE step */
			node->hj_JoinState = HJ_NEED_NEW_OUTER;
		}
		else
		{
			/* must destroy and rebuild hash table */
			ExecHashTableDestroy(node->hj_HashTable);
			node->hj_HashTable = NULL;
			node->hj_JoinState = HJ_BUILD_HASHTABLE;

			/*
			 * if chgParam of subnode is not null then plan will be re-scanned
			 * by first ExecProcNode.
			 */
			if (node->js.ps.righttree->chgParam == NULL)
				ExecReScan(node->js.ps.righttree);
		}
	}

	/* Always reset intra-tuple state */
	node->hj_CurHashValue = 0;
	node->hj_CurBucketNo = 0;
	node->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
	node->hj_CurTuple = NULL;

	node->hj_MatchedOuter = false;
	node->hj_FirstOuterTupleSlot = NULL;

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->js.ps.lefttree->chgParam == NULL)
		ExecReScan(node->js.ps.lefttree);
}

void
ExecShutdownHashJoin(HashJoinState *node)
{
	/*
	 * By the time ExecEndHashJoin runs in a worker, shared memory has been
	 * destroyed.  So this is our last chance to do any shared memory cleanup.
	 */
	if (node->hj_HashTable)
		ExecHashTableDetach(node->hj_HashTable);
}

void ExecHashJoinEstimate(HashJoinState *state, ParallelContext *pcxt)
{
	size_t size;

	/* The shared hash table. */
	size = sizeof(SharedHashJoinTableData);
	shm_toc_estimate_chunk(&pcxt->estimator, size);
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* The shared inner batches. */
	size = sts_estimate(pcxt->nworkers + 1);
	shm_toc_estimate_chunk(&pcxt->estimator, size);
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* The shared outer batches. */
	size = sts_estimate(pcxt->nworkers + 1);
	shm_toc_estimate_chunk(&pcxt->estimator, size);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

void
ExecHashJoinInitializeDSM(HashJoinState *state, ParallelContext *pcxt)
{
	int plan_node_id = state->js.ps.plan->plan_node_id;
	HashState *hashNode;
	SharedHashJoinTable shared;
	size_t size;
	int planned_participants;
	SharedTuplestore *inner_batches;
	SharedTuplestore *outer_batches;
	SharedTuplestoreAccessor *inner_batches_accessor;
	SharedTuplestoreAccessor *outer_batches_accessor;

	/*
	 * Disable shared hash table mode if we failed to create a real DSM
	 * segment, because that means that we don't have a DSA area to work
	 * with.
	 */
	if (pcxt->seg == NULL)
		return;

	/*
	 * Set up the state needed to coordinate access to the shared hash table,
	 * using the plan node ID as the toc key.
	 */
	planned_participants = pcxt->nworkers + 1;	/* possible workers + leader */
	size = sizeof(SharedHashJoinTableData);
	shared = shm_toc_allocate(pcxt->toc, size);
	BarrierInit(&shared->barrier, 0);
	BarrierInit(&shared->shrink_barrier, 0);
	shared->nbuckets = 0;
	shared->planned_participants = planned_participants;
	shared->skew_buckets = InvalidDsaPointer;
	shared->skew_bucket_nums = InvalidDsaPointer;
	shared->skew_space_used = 0;
	shared->skew_space_allowed = 0;
	shared->num_skew_buckets = 0;
	shared->skew_bucket_len = 0;
	shared->current_skew_bucketno = 0;
	shared->buckets = InvalidDsaPointer;
	shared->chunks = InvalidDsaPointer;
	shared->chunk_work_queue = InvalidDsaPointer;
	shared->size = 0;
	shared->ntuples = 0;
	shared->shrink_needed = false;
	shared->grow_enabled = true;
	shm_toc_insert(pcxt->toc, plan_node_id, shared);

#ifdef BARRIER_DEBUG
	BarrierEnableDebug(&shared->barrier, "HashJoin.barrier");
	BarrierEnableDebug(&shared->shrink_barrier, "HashJoin.shrink_barrier");
#endif

	LWLockInitialize(&shared->chunk_lock, LWTRANCHE_PARALLEL_HASH_JOIN_CHUNK);

	LeaderGateInit(&shared->leader_gate);

	/*
	 * We also need to create two variable-sized shared tuplestore objects,
	 * for inner and outer batch files.  It's not convenient to put those
	 * inside the SharedHashJoinTableData struct because we don't know their
	 * sizes.  We'll use separate TOC entries.
	 */

	inner_batches = shm_toc_allocate(pcxt->toc, sts_estimate(planned_participants));
	inner_batches_accessor =
		sts_initialize(inner_batches, planned_participants, 0, sizeof(uint32),
					   SHARED_TUPLESTORE_SINGLE_PASS, pcxt->seg);
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_EXECUTOR_NODE_NTH(plan_node_id, 1),
				   inner_batches);

	outer_batches = shm_toc_allocate(pcxt->toc, sts_estimate(planned_participants));
	outer_batches_accessor =
		sts_initialize(outer_batches, planned_participants, 0, sizeof(uint32),
					   SHARED_TUPLESTORE_SINGLE_PASS, pcxt->seg);
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_EXECUTOR_NODE_NTH(plan_node_id, 2),
				   outer_batches);

	/*
	 * Pass the SharedHashJoinTable to the hash node.  If the Gather node
	 * running in the leader backend decides to execute the hash join, it
	 * hasn't called ExecHashJoinInitializeWorker so it doesn't have
	 * state->shared_table_data set up.  So we must do it here.
	 */
	hashNode = (HashState *) innerPlanState(state);
	hashNode->shared_table_data = shared;
	hashNode->shared_inner_batches = inner_batches_accessor;
	hashNode->shared_outer_batches = outer_batches_accessor;
}

void
ExecHashJoinInitializeWorker(HashJoinState *state,
							 ParallelWorkerContext *pwcxt)
{
	HashState  *hashNode;
	SharedTuplestore *inner_shared_batches;
	SharedTuplestore *outer_shared_batches;
	int plan_node_id = state->js.ps.plan->plan_node_id;

	state->hj_sharedHashJoinTable = shm_toc_lookup(pwcxt->toc, plan_node_id,
												   false);

	/* Inject SharedHashJoinTable into the hash node. */
	hashNode = (HashState *) innerPlanState(state);
	hashNode->shared_table_data = state->hj_sharedHashJoinTable;
	Assert(hashNode->shared_table_data != NULL);

	/*
	 * Attach to the ShareTupleStore objects that manage our shared inner and
	 * outer batches.
	 */
	inner_shared_batches =
		shm_toc_lookup(pwcxt->toc,
					   PARALLEL_KEY_EXECUTOR_NODE_NTH(plan_node_id, 1), false);
	hashNode->shared_inner_batches = sts_attach(inner_shared_batches,
												ParallelWorkerNumber + 1,
												pwcxt->seg);
	outer_shared_batches =
		shm_toc_lookup(pwcxt->toc,
					   PARALLEL_KEY_EXECUTOR_NODE_NTH(plan_node_id, 2), false);
	hashNode->shared_outer_batches = sts_attach(outer_shared_batches,
												ParallelWorkerNumber + 1,
												pwcxt->seg);
}
