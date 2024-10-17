/*-------------------------------------------------------------------------
 *
 * nodeMergeAppend.c
 *	  routines to handle MergeAppend nodes.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeMergeAppend.c
 *
 *-------------------------------------------------------------------------
 */
/* INTERFACE ROUTINES
 *		ExecInitMergeAppend		- initialize the MergeAppend node
 *		ExecMergeAppend			- retrieve the next tuple from the node
 *		ExecEndMergeAppend		- shut down the MergeAppend node
 *		ExecReScanMergeAppend	- rescan the MergeAppend node
 *
 *	 NOTES
 *		A MergeAppend node contains a list of one or more subplans.
 *		These are each expected to deliver tuples that are sorted according
 *		to a common sort key.  The MergeAppend node merges these streams
 *		to produce output sorted the same way.
 *
 *		MergeAppend nodes don't make use of their left and right
 *		subtrees, rather they maintain a list of subplans so
 *		a typical MergeAppend node looks like this in the plan tree:
 *
 *				   ...
 *				   /
 *				MergeAppend---+------+------+--- nil
 *				/	\		  |		 |		|
 *			  nil	nil		 ...    ...    ...
 *								 subplans
 */

#include "postgres.h"

#include "executor/executor.h"
#include "executor/execAsync.h"
#include "executor/execPartition.h"
#include "executor/nodeMergeAppend.h"
#include "lib/binaryheap.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "utils/wait_event.h"

#define EVENT_BUFFER_SIZE                     16

/*
 * We have one slot for each item in the heap array.  We use SlotNumber
 * to store slot indexes.  This doesn't actually provide any formal
 * type-safety, but it makes the code more self-documenting.
 */
typedef int32 SlotNumber;

static TupleTableSlot *ExecMergeAppend(PlanState *pstate);
static int	heap_compare_slots(Datum a, Datum b, void *arg);

static void classify_matching_subplans(MergeAppendState *node);
static void ExecMergeAppendAsyncBegin(MergeAppendState *node);
static void ExecMergeAppendAsyncGetNext(MergeAppendState *node, int mplan);
static bool ExecMergeAppendAsyncRequest(MergeAppendState *node, int mplan);
static void ExecMergeAppendAsyncEventWait(MergeAppendState *node);


/* ----------------------------------------------------------------
 *		ExecInitMergeAppend
 *
 *		Begin all of the subscans of the MergeAppend node.
 * ----------------------------------------------------------------
 */
MergeAppendState *
ExecInitMergeAppend(MergeAppend *node, EState *estate, int eflags)
{
	MergeAppendState *mergestate = makeNode(MergeAppendState);
	PlanState **mergeplanstates;
	Bitmapset  *validsubplans;
	int			nplans;
	int			i,
				j;
	Bitmapset  *asyncplans;
	int			nasyncplans;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create new MergeAppendState for our node
	 */
	mergestate->ps.plan = (Plan *) node;
	mergestate->ps.state = estate;
	mergestate->ps.ExecProcNode = ExecMergeAppend;

	/* If run-time partition pruning is enabled, then set that up now */
	if (node->part_prune_info != NULL)
	{
		PartitionPruneState *prunestate;

		/*
		 * Set up pruning data structure.  This also initializes the set of
		 * subplans to initialize (validsubplans) by taking into account the
		 * result of performing initial pruning if any.
		 */
		prunestate = ExecInitPartitionPruning(&mergestate->ps,
											  list_length(node->mergeplans),
											  node->part_prune_info,
											  &validsubplans);
		mergestate->ms_prune_state = prunestate;
		nplans = bms_num_members(validsubplans);

		/*
		 * When no run-time pruning is required and there's at least one
		 * subplan, we can fill ms_valid_subplans immediately, preventing
		 * later calls to ExecFindMatchingSubPlans.
		 */
		if (!prunestate->do_exec_prune && nplans > 0)
		{
			mergestate->ms_valid_subplans = bms_add_range(NULL, 0, nplans - 1);
			mergestate->ms_valid_subplans_identified = true;
		}
	}
	else
	{
		nplans = list_length(node->mergeplans);

		/*
		 * When run-time partition pruning is not enabled we can just mark all
		 * subplans as valid; they must also all be initialized.
		 */
		Assert(nplans > 0);
		mergestate->ms_valid_subplans = validsubplans =
			bms_add_range(NULL, 0, nplans - 1);
		mergestate->ms_valid_subplans_identified = true;
		mergestate->ms_prune_state = NULL;
	}

	mergeplanstates = (PlanState **) palloc(nplans * sizeof(PlanState *));
	mergestate->mergeplans = mergeplanstates;
	mergestate->ms_nplans = nplans;

	mergestate->ms_slots = (TupleTableSlot **) palloc0(sizeof(TupleTableSlot *) * nplans);
	mergestate->ms_heap = binaryheap_allocate(nplans, heap_compare_slots,
											  mergestate);

	/*
	 * Miscellaneous initialization
	 *
	 * MergeAppend nodes do have Result slots, which hold pointers to tuples,
	 * so we have to initialize them.  FIXME
	 */
	ExecInitResultTupleSlotTL(&mergestate->ps, &TTSOpsVirtual);

	/* node returns slots from each of its subnodes, therefore not fixed */
	mergestate->ps.resultopsset = true;
	mergestate->ps.resultopsfixed = false;

	/*
	 * call ExecInitNode on each of the valid plans to be executed and save
	 * the results into the mergeplanstates array.
	 */
	j = 0;
	asyncplans = NULL;
	nasyncplans = 0;

	i = -1;
	while ((i = bms_next_member(validsubplans, i)) >= 0)
	{
		Plan	   *initNode = (Plan *) list_nth(node->mergeplans, i);

		/*
		 * Record async subplans.  When executing EvalPlanQual, we treat them
		 * as sync ones; don't do this when initializing an EvalPlanQual plan
		 * tree.
		 */
		if (initNode->async_capable && estate->es_epq_active == NULL)
		{
			asyncplans = bms_add_member(asyncplans, j);
			nasyncplans++;
		}

		mergeplanstates[j++] = ExecInitNode(initNode, estate, eflags);
	}

	mergestate->ps.ps_ProjInfo = NULL;

	/* Initialize async state */
	mergestate->ms_asyncplans = asyncplans;
	mergestate->ms_nasyncplans = nasyncplans;
	mergestate->ms_asyncrequests = NULL;
	mergestate->ms_asyncresults = NULL;
	mergestate->ms_has_asyncresults = NULL;
	mergestate->ms_asyncremain = NULL;
	mergestate->ms_needrequest = NULL;
	mergestate->ms_eventset = NULL;
	mergestate->ms_valid_asyncplans = NULL;

	if (nasyncplans > 0)
	{
		mergestate->ms_asyncrequests = (AsyncRequest **)
			palloc0(nplans * sizeof(AsyncRequest *));

		i = -1;
		while ((i = bms_next_member(asyncplans, i)) >= 0)
		{
			AsyncRequest *areq;

			areq = palloc(sizeof(AsyncRequest));
			areq->requestor = (PlanState *) mergestate;
			areq->requestee = mergeplanstates[i];
			areq->request_index = i;
			areq->callback_pending = false;
			areq->request_complete = false;
			areq->result = NULL;

			mergestate->ms_asyncrequests[i] = areq;
		}

		mergestate->ms_asyncresults = (TupleTableSlot **)
			palloc0(nplans * sizeof(TupleTableSlot *));

		if (mergestate->ms_valid_subplans_identified)
			classify_matching_subplans(mergestate);
	}

	/*
	 * initialize sort-key information
	 */
	mergestate->ms_nkeys = node->numCols;
	mergestate->ms_sortkeys = palloc0(sizeof(SortSupportData) * node->numCols);

	for (i = 0; i < node->numCols; i++)
	{
		SortSupport sortKey = mergestate->ms_sortkeys + i;

		sortKey->ssup_cxt = CurrentMemoryContext;
		sortKey->ssup_collation = node->collations[i];
		sortKey->ssup_nulls_first = node->nullsFirst[i];
		sortKey->ssup_attno = node->sortColIdx[i];

		/*
		 * It isn't feasible to perform abbreviated key conversion, since
		 * tuples are pulled into mergestate's binary heap as needed.  It
		 * would likely be counter-productive to convert tuples into an
		 * abbreviated representation as they're pulled up, so opt out of that
		 * additional optimization entirely.
		 */
		sortKey->abbreviate = false;

		PrepareSortSupportFromOrderingOp(node->sortOperators[i], sortKey);
	}

	/*
	 * initialize to show we have not run the subplans yet
	 */
	mergestate->ms_initialized = false;

	return mergestate;
}

/* ----------------------------------------------------------------
 *	   ExecMergeAppend
 *
 *		Handles iteration over multiple subplans.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecMergeAppend(PlanState *pstate)
{
	MergeAppendState *node = castNode(MergeAppendState, pstate);
	TupleTableSlot *result;
	SlotNumber	i;

	CHECK_FOR_INTERRUPTS();

	if (!node->ms_initialized)
	{
		/* Nothing to do if all subplans were pruned */
		if (node->ms_nplans == 0)
			return ExecClearTuple(node->ps.ps_ResultTupleSlot);

		/*
		 * If we've yet to determine the valid subplans then do so now.  If
		 * run-time pruning is disabled then the valid subplans will always be
		 * set to all subplans.
		 */
		if (!node->ms_valid_subplans_identified)
		{
			node->ms_valid_subplans =
				ExecFindMatchingSubPlans(node->ms_prune_state, false);
			node->ms_valid_subplans_identified = true;
		}

		/* If there are any async subplans, begin executing them. */
		if (node->ms_nasyncplans > 0)
			ExecMergeAppendAsyncBegin(node);

		/*
		 * First time through: pull the first tuple from each valid subplan,
		 * and set up the heap.
		 */
		i = -1;
		while ((i = bms_next_member(node->ms_valid_subplans, i)) >= 0)
		{
			node->ms_slots[i] = ExecProcNode(node->mergeplans[i]);
			if (!TupIsNull(node->ms_slots[i]))
				binaryheap_add_unordered(node->ms_heap, Int32GetDatum(i));
		}

		/* Look at async subplans */
		i = -1;
		while ((i = bms_next_member(node->ms_asyncplans, i)) >= 0)
		{
			ExecMergeAppendAsyncGetNext(node, i);
			if (!TupIsNull(node->ms_slots[i]))
				binaryheap_add_unordered(node->ms_heap, Int32GetDatum(i));
		}

		binaryheap_build(node->ms_heap);
		node->ms_initialized = true;
	}
	else
	{
		/*
		 * Otherwise, pull the next tuple from whichever subplan we returned
		 * from last time, and reinsert the subplan index into the heap,
		 * because it might now compare differently against the existing
		 * elements of the heap.  (We could perhaps simplify the logic a bit
		 * by doing this before returning from the prior call, but it's better
		 * to not pull tuples until necessary.)
		 */
		i = DatumGetInt32(binaryheap_first(node->ms_heap));
		if (bms_is_member(i, node->ms_asyncplans))
			ExecMergeAppendAsyncGetNext(node, i);
		else
		{
			Assert(bms_is_member(i, node->ms_valid_subplans));
			node->ms_slots[i] = ExecProcNode(node->mergeplans[i]);
		}
		if (!TupIsNull(node->ms_slots[i]))
			binaryheap_replace_first(node->ms_heap, Int32GetDatum(i));
		else
			(void) binaryheap_remove_first(node->ms_heap);
	}

	if (binaryheap_empty(node->ms_heap))
	{
		/* All the subplans are exhausted, and so is the heap */
		result = ExecClearTuple(node->ps.ps_ResultTupleSlot);
	}
	else
	{
		i = DatumGetInt32(binaryheap_first(node->ms_heap));
		result = node->ms_slots[i];
		/*  For async plan record that we can get the next tuple */
		node->ms_has_asyncresults = bms_del_member(node->ms_has_asyncresults, i);
	}

	return result;
}

/*
 * Compare the tuples in the two given slots.
 */
static int32
heap_compare_slots(Datum a, Datum b, void *arg)
{
	MergeAppendState *node = (MergeAppendState *) arg;
	SlotNumber	slot1 = DatumGetInt32(a);
	SlotNumber	slot2 = DatumGetInt32(b);

	TupleTableSlot *s1 = node->ms_slots[slot1];
	TupleTableSlot *s2 = node->ms_slots[slot2];
	int			nkey;

	Assert(!TupIsNull(s1));
	Assert(!TupIsNull(s2));

	for (nkey = 0; nkey < node->ms_nkeys; nkey++)
	{
		SortSupport sortKey = node->ms_sortkeys + nkey;
		AttrNumber	attno = sortKey->ssup_attno;
		Datum		datum1,
					datum2;
		bool		isNull1,
					isNull2;
		int			compare;

		datum1 = slot_getattr(s1, attno, &isNull1);
		datum2 = slot_getattr(s2, attno, &isNull2);

		compare = ApplySortComparator(datum1, isNull1,
									  datum2, isNull2,
									  sortKey);
		if (compare != 0)
		{
			INVERT_COMPARE_RESULT(compare);
			return compare;
		}
	}
	return 0;
}

/* ----------------------------------------------------------------
 *		ExecEndMergeAppend
 *
 *		Shuts down the subscans of the MergeAppend node.
 *
 *		Returns nothing of interest.
 * ----------------------------------------------------------------
 */
void
ExecEndMergeAppend(MergeAppendState *node)
{
	PlanState **mergeplans;
	int			nplans;
	int			i;

	/*
	 * get information from the node
	 */
	mergeplans = node->mergeplans;
	nplans = node->ms_nplans;

	/*
	 * shut down each of the subscans
	 */
	for (i = 0; i < nplans; i++)
		ExecEndNode(mergeplans[i]);
}

void
ExecReScanMergeAppend(MergeAppendState *node)
{
	int			i;
	int			nasyncplans = node->ms_nasyncplans;

	/*
	 * If any PARAM_EXEC Params used in pruning expressions have changed, then
	 * we'd better unset the valid subplans so that they are reselected for
	 * the new parameter values.
	 */
	if (node->ms_prune_state &&
		bms_overlap(node->ps.chgParam,
					node->ms_prune_state->execparamids))
	{
		node->ms_valid_subplans_identified = false;
		bms_free(node->ms_valid_subplans);
		node->ms_valid_subplans = NULL;
		bms_free(node->ms_valid_asyncplans);
		node->ms_valid_asyncplans = NULL;
	}

	for (i = 0; i < node->ms_nplans; i++)
	{
		PlanState  *subnode = node->mergeplans[i];

		/*
		 * ExecReScan doesn't know about my subplans, so I have to do
		 * changed-parameter signaling myself.
		 */
		if (node->ps.chgParam != NULL)
			UpdateChangedParamSet(subnode, node->ps.chgParam);

		/*
		 * If chgParam of subnode is not null then plan will be re-scanned by
		 * first ExecProcNode.
		 */
		if (subnode->chgParam == NULL)
			ExecReScan(subnode);
	}

	/* Reset async state */
	if (nasyncplans > 0)
	{
		i = -1;
		while ((i = bms_next_member(node->ms_asyncplans, i)) >= 0)
		{
			AsyncRequest *areq = node->ms_asyncrequests[i];

			areq->callback_pending = false;
			areq->request_complete = false;
			areq->result = NULL;
		}

		bms_free(node->ms_asyncremain);
		node->ms_asyncremain = NULL;
		bms_free(node->ms_needrequest);
		node->ms_needrequest = NULL;
		bms_free(node->ms_has_asyncresults);
		node->ms_has_asyncresults = NULL;
	}
	binaryheap_reset(node->ms_heap);
	node->ms_initialized = false;
}

/* ----------------------------------------------------------------
 *              classify_matching_subplans
 *
 *              Classify the node's ms_valid_subplans into sync ones and
 *              async ones, adjust it to contain sync ones only, and save
 *              async ones in the node's ms_valid_asyncplans.
 * ----------------------------------------------------------------
 */
static void
classify_matching_subplans(MergeAppendState *node)
{
	Bitmapset  *valid_asyncplans;

	Assert(node->ms_valid_subplans_identified);
	Assert(node->ms_valid_asyncplans == NULL);

	/* Nothing to do if there are no valid subplans. */
	if (bms_is_empty(node->ms_valid_subplans))
	{
		node->ms_asyncremain = NULL;
		return;
	}

	/* Nothing to do if there are no valid async subplans. */
	if (!bms_overlap(node->ms_valid_subplans, node->ms_asyncplans))
	{
		node->ms_asyncremain = NULL;
		return;
	}

	/* Get valid async subplans. */
	valid_asyncplans = bms_intersect(node->ms_asyncplans,
								   node->ms_valid_subplans);

	/* Adjust the valid subplans to contain sync subplans only. */
	node->ms_valid_subplans = bms_del_members(node->ms_valid_subplans,
											  valid_asyncplans);

	/* Save valid async subplans. */
	node->ms_valid_asyncplans = valid_asyncplans;
}

/* ----------------------------------------------------------------
 *              ExecMergeAppendAsyncBegin
 *
 *              Begin executing designed async-capable subplans.
 * ----------------------------------------------------------------
 */
static void
ExecMergeAppendAsyncBegin(MergeAppendState *node)
{
	int			i;

	/* Backward scan is not supported by async-aware MergeAppends. */
	Assert(ScanDirectionIsForward(node->ps.state->es_direction));

	/* We should never be called when there are no subplans */
	Assert(node->ms_nplans > 0);

	/* We should never be called when there are no async subplans. */
	Assert(node->ms_nasyncplans > 0);

	/* If we've yet to determine the valid subplans then do so now. */
	if (!node->ms_valid_subplans_identified)
	{
		node->ms_valid_subplans =
			ExecFindMatchingSubPlans(node->ms_prune_state, false);
		node->ms_valid_subplans_identified = true;

		classify_matching_subplans(node);
	}

	/* Initialize state variables. */
	node->ms_asyncremain = bms_copy(node->ms_valid_asyncplans);

	/* Nothing to do if there are no valid async subplans. */
	if (bms_is_empty(node->ms_asyncremain))
		return;

	/* Make a request for each of the valid async subplans. */
	i = -1;
	while ((i = bms_next_member(node->ms_valid_asyncplans, i)) >= 0)
	{
		AsyncRequest *areq = node->ms_asyncrequests[i];

		Assert(areq->request_index == i);
		Assert(!areq->callback_pending);

		/* Do the actual work. */
		ExecAsyncRequest(areq);
	}
}

/* ----------------------------------------------------------------
 *              ExecMergeAppendAsyncGetNext
 *
 *              Get the next tuple from specified asynchronous subplan.
 * ----------------------------------------------------------------
 */
static void
ExecMergeAppendAsyncGetNext(MergeAppendState *node, int mplan)
{
	node->ms_slots[mplan] = NULL;

	/* Request a tuple asynchronously. */
	if (ExecMergeAppendAsyncRequest(node, mplan))
		return;

	/*
	 * node->ms_asyncremain can be NULL if we have fetched tuples, but haven't
	 * returned them yet. In this case ExecMergeAppendAsyncRequest() above just
	 * returns tuples without performing a request.
	 */
	while (bms_is_member(mplan, node->ms_asyncremain))
	{
		CHECK_FOR_INTERRUPTS();

		/* Wait or poll for async events. */
		ExecMergeAppendAsyncEventWait(node);

		/* Request a tuple asynchronously. */
		if (ExecMergeAppendAsyncRequest(node, mplan))
			return;

		/*
		 * Waiting until there's no async requests pending or we got some
		 * tuples from our request
		 */
	}

	/* No tuples */
	return;
}

/* ----------------------------------------------------------------
 *              ExecMergeAppendAsyncRequest
 *
 *              Request a tuple asynchronously.
 * ----------------------------------------------------------------
 */
static bool
ExecMergeAppendAsyncRequest(MergeAppendState *node, int mplan)
{
	Bitmapset  *needrequest;
	int			i;

	/*
	 * If we've already fetched necessary data, just return it
	 */
	if (bms_is_member(mplan, node->ms_has_asyncresults))
	{
		node->ms_slots[mplan] = node->ms_asyncresults[mplan];
		return true;
	}

	/*
	 * Get a list of members which can process request and don't have data
	 * ready.
	 */
	needrequest = NULL;
	i = -1;
	while ((i = bms_next_member(node->ms_needrequest, i)) >= 0)
	{
		if (!bms_is_member(i, node->ms_has_asyncresults))
			needrequest = bms_add_member(needrequest, i);
	}

	/*
	 * If there's no members, which still need request, no need to send it.
	 */
	if (bms_is_empty(needrequest))
		return false;

	/* Clear ms_needrequest flag, as we are going to send requests now */
	node->ms_needrequest = bms_del_members(node->ms_needrequest, needrequest);

	/* Make a new request for each of the async subplans that need it. */
	i = -1;
	while ((i = bms_next_member(needrequest, i)) >= 0)
	{
		AsyncRequest *areq = node->ms_asyncrequests[i];

		/*
		 * We've just checked that subplan doesn't already have some fetched
		 * data
		 */
		Assert(!bms_is_member(i, node->ms_has_asyncresults));

		/* Do the actual work. */
		ExecAsyncRequest(areq);
	}
	bms_free(needrequest);

	/* Return needed asynchronously-generated results if any. */
	if (bms_is_member(mplan, node->ms_has_asyncresults))
	{
		node->ms_slots[mplan] = node->ms_asyncresults[mplan];
		return true;
	}

	return false;
}

/* ----------------------------------------------------------------
 *              ExecAsyncMergeAppendResponse
 *
 *              Receive a response from an asynchronous request we made.
 * ----------------------------------------------------------------
 */
void
ExecAsyncMergeAppendResponse(AsyncRequest *areq)
{
	MergeAppendState *node = (MergeAppendState *) areq->requestor;
	TupleTableSlot *slot = areq->result;

	/* The result should be a TupleTableSlot or NULL. */
	Assert(slot == NULL || IsA(slot, TupleTableSlot));
	Assert(!bms_is_member(areq->request_index, node->ms_has_asyncresults));

	node->ms_asyncresults[areq->request_index] = NULL;
	/* Nothing to do if the request is pending. */
	if (!areq->request_complete)
	{
		/* The request would have been pending for a callback. */
		Assert(areq->callback_pending);
		return;
	}

	/* If the result is NULL or an empty slot, there's nothing more to do. */
	if (TupIsNull(slot))
	{
		/* The ending subplan wouldn't have been pending for a callback. */
		Assert(!areq->callback_pending);
		node->ms_asyncremain = bms_del_member(node->ms_asyncremain, areq->request_index);
		return;
	}

	node->ms_has_asyncresults = bms_add_member(node->ms_has_asyncresults, areq->request_index);
	/* Save result so we can return it. */
	node->ms_asyncresults[areq->request_index] = slot;

	/*
	 * Mark the subplan that returned a result as ready for a new request.  We
	 * don't launch another one here immediately because it might complete.
	 */
	node->ms_needrequest = bms_add_member(node->ms_needrequest,
										  areq->request_index);
}

/* ----------------------------------------------------------------
 *		ExecMergeAppendAsyncEventWait
 *
 *		Wait or poll for file descriptor events and fire callbacks.
 * ----------------------------------------------------------------
 */
static void
ExecMergeAppendAsyncEventWait(MergeAppendState *node)
{
	int			nevents = node->ms_nasyncplans + 1; /* one for PM death */
	WaitEvent	occurred_event[EVENT_BUFFER_SIZE];
	int			noccurred;
	int			i;

	/* We should never be called when there are no valid async subplans. */
	Assert(bms_num_members(node->ms_asyncremain) > 0);

	node->ms_eventset = CreateWaitEventSet(CurrentResourceOwner, nevents);
	AddWaitEventToSet(node->ms_eventset, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET,
					  NULL, NULL);

	/* Give each waiting subplan a chance to add an event. */
	i = -1;
	while ((i = bms_next_member(node->ms_asyncplans, i)) >= 0)
	{
		AsyncRequest *areq = node->ms_asyncrequests[i];

		if (areq->callback_pending)
			ExecAsyncConfigureWait(areq);
	}

	/*
	 * No need for further processing if there are no configured events other
	 * than the postmaster death event.
	 */
	if (GetNumRegisteredWaitEvents(node->ms_eventset) == 1)
	{
		FreeWaitEventSet(node->ms_eventset);
		node->ms_eventset = NULL;
		return;
	}

	/* We wait on at most EVENT_BUFFER_SIZE events. */
	if (nevents > EVENT_BUFFER_SIZE)
		nevents = EVENT_BUFFER_SIZE;

	/*
	 * Wait until at least one event occurs.
	 */
	noccurred = WaitEventSetWait(node->ms_eventset, -1 /* no timeout */, occurred_event,
								 nevents, WAIT_EVENT_APPEND_READY);
	FreeWaitEventSet(node->ms_eventset);
	node->ms_eventset = NULL;
	if (noccurred == 0)
		return;

	/* Deliver notifications. */
	for (i = 0; i < noccurred; i++)
	{
		WaitEvent  *w = &occurred_event[i];

		/*
		 * Each waiting subplan should have registered its wait event with
		 * user_data pointing back to its AsyncRequest.
		 */
		if ((w->events & WL_SOCKET_READABLE) != 0)
		{
			AsyncRequest *areq = (AsyncRequest *) w->user_data;

			if (areq->callback_pending)
			{
				/*
				 * Mark it as no longer needing a callback.  We must do this
				 * before dispatching the callback in case the callback resets
				 * the flag.
				 */
				areq->callback_pending = false;

				/* Do the actual work. */
				ExecAsyncNotify(areq);
			}
		}
	}
}
