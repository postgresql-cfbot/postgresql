/*-------------------------------------------------------------------------
 *
 * nodeAppend.c
 *	  routines to handle append nodes.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeAppend.c
 *
 *-------------------------------------------------------------------------
 */
/* INTERFACE ROUTINES
 *		ExecInitAppend	- initialize the append node
 *		ExecAppend		- retrieve the next tuple from the node
 *		ExecEndAppend	- shut down the append node
 *		ExecReScanAppend - rescan the append node
 *
 *	 NOTES
 *		Each append node contains a list of one or more subplans which
 *		must be iteratively processed (forwards or backwards).
 *		Tuples are retrieved by executing the 'whichplan'th subplan
 *		until the subplan stops returning tuples, at which point that
 *		plan is shut down and the next started up.
 *
 *		Append nodes don't make use of their left and right
 *		subtrees, rather they maintain a list of subplans so
 *		a typical append node looks like this in the plan tree:
 *
 *				   ...
 *				   /
 *				Append -------+------+------+--- nil
 *				/	\		  |		 |		|
 *			  nil	nil		 ...    ...    ...
 *								 subplans
 *
 *		Append nodes are currently used for unions, and to support
 *		inheritance queries, where several relations need to be scanned.
 *		For example, in our standard person/student/employee/student-emp
 *		example, where student and employee inherit from person
 *		and student-emp inherits from student and employee, the
 *		query:
 *
 *				select name from person
 *
 *		generates the plan:
 *
 *				  |
 *				Append -------+-------+--------+--------+
 *				/	\		  |		  |		   |		|
 *			  nil	nil		 Scan	 Scan	  Scan	   Scan
 *							  |		  |		   |		|
 *							person employee student student-emp
 */

#include "postgres.h"

#include "executor/execdebug.h"
#include "executor/nodeAppend.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "storage/spin.h"

/*
 * Shared state for Parallel Append.
 *
 * Each backend participating in a Parallel Append has its own
 * descriptor in backend-private memory, and those objects all contain
 * a pointer to this structure.
 */
typedef struct ParallelAppendDescData
{
	LWLock		pa_lock;		/* mutual exclusion to choose next subplan */
	int			pa_first_plan;	/* plan to choose while wrapping around plans */
	int			pa_next_plan;	/* next plan to choose by any worker */

	/*
	 * pa_finished : workers currently executing the subplan. A worker which
	 * finishes a subplan should set pa_finished to true, so that no new
	 * worker picks this subplan. For non-partial subplan, a worker which picks
	 * up that subplan should immediately set to true, so as to make sure
	 * there are no more than 1 worker assigned to this subplan.
	 */
	bool		pa_finished[FLEXIBLE_ARRAY_MEMBER];
} ParallelAppendDescData;

typedef ParallelAppendDescData *ParallelAppendDesc;

/*
 * Special value of AppendState->as_whichplan for Parallel Append, which
 * indicates there are no plans left to be executed.
 */
#define PA_INVALID_PLAN -1

static TupleTableSlot *ExecAppend(PlanState *pstate);
static bool exec_append_seq_next(AppendState *appendstate);
static bool exec_append_parallel_next(AppendState *state);
static bool exec_append_leader_next(AppendState *state);
static int exec_append_get_next_plan(int curplan, int first_plan,
									  int last_plan);

/* ----------------------------------------------------------------
 *		exec_append_initialize_next
 *
 *		Sets up the append state node for the "next" scan.
 *
 *		Returns t iff there is a "next" scan to process.
 * ----------------------------------------------------------------
 */
static bool
exec_append_seq_next(AppendState *appendstate)
{
	int			whichplan;

	/*
	 * Not parallel-aware. Fine, just go on to the next subplan in the
	 * appropriate direction.
	 */
	if (ScanDirectionIsForward(appendstate->ps.state->es_direction))
		appendstate->as_whichplan++;
	else
		appendstate->as_whichplan--;

	/*
	 * get information from the append node
	 */
	whichplan = appendstate->as_whichplan;

	if (whichplan < 0)
	{
		/*
		 * if scanning in reverse, we start at the last scan in the list and
		 * then proceed back to the first.. in any case we inform ExecAppend
		 * that we are at the end of the line by returning FALSE
		 */
		appendstate->as_whichplan = 0;
		return FALSE;
	}
	else if (whichplan >= appendstate->as_nplans)
	{
		/*
		 * as above, end the scan if we go beyond the last scan in our list..
		 */
		appendstate->as_whichplan = appendstate->as_nplans - 1;
		return FALSE;
	}
	else
	{
		return TRUE;
	}
}

/* ----------------------------------------------------------------
 *		ExecInitAppend
 *
 *		Begin all of the subscans of the append node.
 *
 *	   (This is potentially wasteful, since the entire result of the
 *		append node may not be scanned, but this way all of the
 *		structures get allocated in the executor's top level memory
 *		block instead of that of the call to ExecAppend.)
 * ----------------------------------------------------------------
 */
AppendState *
ExecInitAppend(Append *node, EState *estate, int eflags)
{
	AppendState *appendstate = makeNode(AppendState);
	PlanState **appendplanstates;
	int			nplans;
	int			i;
	ListCell   *lc;

	/* check for unsupported flags */
	Assert(!(eflags & EXEC_FLAG_MARK));

	/*
	 * Lock the non-leaf tables in the partition tree controlled by this node.
	 * It's a no-op for non-partitioned parent tables.
	 */
	ExecLockNonLeafAppendTables(node->partitioned_rels, estate);

	/*
	 * Set up empty vector of subplan states
	 */
	nplans = list_length(node->appendplans);

	appendplanstates = (PlanState **) palloc0(nplans * sizeof(PlanState *));

	/*
	 * create new AppendState for our append node
	 */
	appendstate->ps.plan = (Plan *) node;
	appendstate->ps.state = estate;
	appendstate->ps.ExecProcNode = ExecAppend;
	appendstate->appendplans = appendplanstates;
	appendstate->as_nplans = nplans;

	/*
	 * Miscellaneous initialization
	 *
	 * Append plans don't have expression contexts because they never call
	 * ExecQual or ExecProject.
	 */

	/*
	 * append nodes still have Result slots, which hold pointers to tuples, so
	 * we have to initialize them.
	 */
	ExecInitResultTupleSlot(estate, &appendstate->ps);

	/*
	 * call ExecInitNode on each of the plans to be executed and save the
	 * results into the array "appendplans".
	 */
	i = 0;
	foreach(lc, node->appendplans)
	{
		Plan	   *initNode = (Plan *) lfirst(lc);

		appendplanstates[i] = ExecInitNode(initNode, estate, eflags);
		i++;
	}

	/*
	 * initialize output tuple type
	 */
	ExecAssignResultTypeFromTL(&appendstate->ps);
	appendstate->ps.ps_ProjInfo = NULL;

	/*
	 * Initialize to scan first subplan (but note that we'll override this
	 * later in the case of a parallel append).
	 */
	appendstate->as_whichplan = 0;

	return appendstate;
}

/* ----------------------------------------------------------------
 *	   ExecAppend
 *
 *		Handles iteration over multiple subplans.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecAppend(PlanState *pstate)
{
	AppendState *node = castNode(AppendState, pstate);

	/*
	 * Check if we are already finished plans from parallel append. This
	 * can happen if all the subplans are finished when this worker
	 * has not even started returning tuples.
	 */
	if (node->as_padesc && node->as_whichplan == PA_INVALID_PLAN)
		return ExecClearTuple(node->ps.ps_ResultTupleSlot);

	for (;;)
	{
		PlanState  *subnode;
		TupleTableSlot *result;

		CHECK_FOR_INTERRUPTS();

		/*
		 * figure out which subplan we are currently processing
		 */
		subnode = node->appendplans[node->as_whichplan];

		/*
		 * get a tuple from the subplan
		 */
		result = ExecProcNode(subnode);

		if (!TupIsNull(result))
		{
			/*
			 * If the subplan gave us something then return it as-is. We do
			 * NOT make use of the result slot that was set up in
			 * ExecInitAppend; there's no need for it.
			 */
			return result;
		}

		/*
		 * Go on to the "next" subplan. If no more subplans, return the empty
		 * slot set up for us by ExecInitAppend.
		 * Note: Parallel-aware Append follows different logic for choosing the
		 * next subplan.
		 */
		if (!node->as_padesc)
		{
			/*
			 */
			if (!exec_append_seq_next(node))
				return ExecClearTuple(node->ps.ps_ResultTupleSlot);
		}
		else
		{
			/*
			 * We are done with this subplan. There might be other workers
			 * still processing the last chunk of rows for this same subplan,
			 * but there's no point for new workers to run this subplan, so
			 * mark this subplan as finished.
			 */
			node->as_padesc->pa_finished[node->as_whichplan] = true;

			if (!exec_append_parallel_next(node))
				return ExecClearTuple(node->ps.ps_ResultTupleSlot);
		}

		/* Else loop back and try to get a tuple from the new subplan */
	}
}

/* ----------------------------------------------------------------
 *		ExecEndAppend
 *
 *		Shuts down the subscans of the append node.
 *
 *		Returns nothing of interest.
 * ----------------------------------------------------------------
 */
void
ExecEndAppend(AppendState *node)
{
	PlanState **appendplans;
	int			nplans;
	int			i;

	/*
	 * get information from the node
	 */
	appendplans = node->appendplans;
	nplans = node->as_nplans;

	/*
	 * shut down each of the subscans
	 */
	for (i = 0; i < nplans; i++)
		ExecEndNode(appendplans[i]);
}

void
ExecReScanAppend(AppendState *node)
{
	int			i;

	for (i = 0; i < node->as_nplans; i++)
	{
		PlanState  *subnode = node->appendplans[i];

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

	node->as_whichplan = 0;
}

/* ----------------------------------------------------------------
 *						Parallel Append Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecAppendEstimate
 *
 *		estimates the space required to serialize Append node.
 * ----------------------------------------------------------------
 */
void
ExecAppendEstimate(AppendState *node,
					ParallelContext *pcxt)
{
	node->pappend_len =
		add_size(offsetof(struct ParallelAppendDescData, pa_finished),
				 sizeof(bool) * node->as_nplans);

	shm_toc_estimate_chunk(&pcxt->estimator, node->pappend_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}


/* ----------------------------------------------------------------
 *		ExecAppendInitializeDSM
 *
 *		Set up a Parallel Append descriptor.
 * ----------------------------------------------------------------
 */
void
ExecAppendInitializeDSM(AppendState *node,
						 ParallelContext *pcxt)
{
	ParallelAppendDesc padesc;

	padesc = shm_toc_allocate(pcxt->toc, node->pappend_len);

	/*
	 * Just setting all the fields to 0 is enough. The logic of choosing the
	 * next plan in workers will take care of everything else.
	 */
	memset(padesc, 0, sizeof(ParallelAppendDescData));
	memset(padesc->pa_finished, 0, sizeof(bool) * node->as_nplans);

	LWLockInitialize(&padesc->pa_lock, LWTRANCHE_PARALLEL_APPEND);

	node->as_padesc = padesc;

	shm_toc_insert(pcxt->toc, node->ps.plan->plan_node_id, padesc);

	/* Choose the first subplan to be executed. */
	(void) exec_append_parallel_next(node);
}

/* ----------------------------------------------------------------
 *		ExecAppendReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecAppendReInitializeDSM(AppendState *node, ParallelContext *pcxt)
{
	ParallelAppendDesc padesc = node->as_padesc;

	padesc->pa_first_plan = padesc->pa_next_plan = 0;
	memset(padesc->pa_finished, 0, sizeof(bool) * node->as_nplans);

	/* Choose the first subplan to be executed. */
	(void) exec_append_parallel_next(node);
}

/* ----------------------------------------------------------------
 *		ExecAppendInitializeWorker
 *
 *		Copy relevant information from TOC into planstate, and initialize
 *		whatever is required to choose and execute the optimal subplan.
 * ----------------------------------------------------------------
 */
void
ExecAppendInitializeWorker(AppendState *node, shm_toc *toc)
{
	node->as_padesc = shm_toc_lookup(toc, node->ps.plan->plan_node_id, false);

	/* Choose the first subplan to be executed. */
	(void) exec_append_parallel_next(node);
}

/* ----------------------------------------------------------------
 *		exec_append_parallel_next
 *
 *		Determine the next subplan that should be executed. Each worker uses a
 *		shared field 'pa_next_plan' to start looking for unfinished plan,
 *		executes the subplan, then shifts ahead this field to the next
 *		subplan, so that other workers know which next plan to choose. This
 *		way, workers choose the subplans in round robin order, and thus they
 *		get evenly distributed among the subplans.
 *
 *		Returns false if and only if all subplans are already finished
 *		processing.
 * ----------------------------------------------------------------
 */
static bool
exec_append_parallel_next(AppendState *state)
{
	ParallelAppendDesc padesc = state->as_padesc;
	int		whichplan;
	int		initial_plan;
	int		first_partial_plan = ((Append *)state->ps.plan)->first_partial_plan;
	bool	found;

	Assert(padesc != NULL);

	/* Backward scan is not supported by parallel-aware plans */
	Assert(ScanDirectionIsForward(state->ps.state->es_direction));

	/* The parallel leader chooses its next subplan differently */
	if (!IsParallelWorker())
		return exec_append_leader_next(state);

	LWLockAcquire(&padesc->pa_lock, LW_EXCLUSIVE);

	/* Make a note of which subplan we have started with */
	initial_plan = padesc->pa_next_plan;

	/*
	 * Keep going to the next plan until we find an unfinished one. In the
	 * process, also keep track of the first unfinished non-partial subplan. As
	 * the non-partial subplans are taken one by one, the first unfinished
	 * subplan index will shift ahead, so that we don't have to visit the
	 * finished non-partial ones anymore.
	 */

	found = false;
	for (whichplan = initial_plan; whichplan != PA_INVALID_PLAN;)
	{
		/*
		 * Ignore plans that are already done processing. These also include
		 * non-partial subplans which have already been taken by a worker.
		 */
		if (!padesc->pa_finished[whichplan])
		{
			found = true;
			break;
		}

		/*
		 * Note: There is a chance that just after the child plan node is
		 * chosen above, some other worker finishes this node and sets
		 * pa_finished to true. In that case, this worker will go ahead and
		 * call ExecProcNode(child_node), which will return NULL tuple since it
		 * is already finished, and then once again this worker will try to
		 * choose next subplan; but this is ok : it's just an extra
		 * "choose_next_subplan" operation.
		 */

		/* Either go to the next plan, or wrap around to the first one */
		whichplan = exec_append_get_next_plan(whichplan, padesc->pa_first_plan,
								   state->as_nplans - 1);

		/*
		 * If we have wrapped around and returned to the same index again, we
		 * are done scanning.
		 */
		if (whichplan == initial_plan)
			break;
	}

	if (!found)
	{
		/*
		 * We didn't find any plan to execute, stop executing, and indicate
		 * the same for other workers to know that there is no next plan.
		 */
		padesc->pa_next_plan = state->as_whichplan = PA_INVALID_PLAN;
	}
	else
	{
		/*
		 * If this a non-partial plan, immediately mark it finished, and shift
		 * ahead pa_first_plan.
		 */
		if (whichplan < first_partial_plan)
		{
			padesc->pa_finished[whichplan] = true;
			padesc->pa_first_plan = whichplan + 1;
		}

		/*
		 * Set the chosen plan, and the next plan to be picked by other
		 * workers.
		 */
		state->as_whichplan = whichplan;
		padesc->pa_next_plan = exec_append_get_next_plan(whichplan,
														 padesc->pa_first_plan,
														 state->as_nplans - 1);
	}

	LWLockRelease(&padesc->pa_lock);

	return found;
}

/* ----------------------------------------------------------------
 *		exec_append_leader_next
 *
 *		To be used only if it's a parallel leader.
 *		With more workers, the leader is known to do more work servicing the
 *		worker tuple queue, and less work contributing to parallel processing.
 *		Hence, it should not take expensive plans, otherwise it will affect the
 *		total time to finish Append. Since we have non-partial plans sorted in
 *		descending cost, let the leader scan backwards from the last plan, i.e.
 *		the cheapest plan.
 * ----------------------------------------------------------------
 */
static bool
exec_append_leader_next(AppendState *state)
{
	ParallelAppendDesc padesc = state->as_padesc;
	int		first_plan;
	int		whichplan;
	int		first_partial_plan = ((Append *)state->ps.plan)->first_partial_plan;

	LWLockAcquire(&padesc->pa_lock, LW_EXCLUSIVE);

	/* The parallel leader should start from the last subplan. */
	first_plan = padesc->pa_first_plan;

	for (whichplan = state->as_nplans - 1; whichplan >= first_plan;
		 whichplan--)
	{
		if (!padesc->pa_finished[whichplan])
		{
			/* If this a non-partial plan, immediately mark it finished */
			if (whichplan < first_partial_plan)
				padesc->pa_finished[whichplan] = true;

			break;
		}
	}

	LWLockRelease(&padesc->pa_lock);

	/* Return false only if we didn't find any plan to execute */
	if (whichplan < first_plan)
	{
		state->as_whichplan = PA_INVALID_PLAN;
		return false;
	}
	else
	{
		state->as_whichplan = whichplan;
		return true;
	}
}

/* ----------------------------------------------------------------
 *		exec_append_get_next_plan
 *
 *		Either go to the next index, or wrap around to the first unfinished one.
 *		Returns this next index. While wrapping around, if the first unfinished
 *		one itself is past the last plan, returns PA_INVALID_PLAN.
 * ----------------------------------------------------------------
 */
static int
exec_append_get_next_plan(int curplan, int first_plan, int last_plan)
{
	Assert(curplan <= last_plan);

	if (curplan < last_plan)
		return curplan + 1;
	else
	{
		/*
		 * We are already at the last plan. If the first_plan itsef is the last
		 * plan or if it is past the last plan, that means there is no next
		 * plan remaining. Return Invalid.
		 */
		if (first_plan >= last_plan)
			return PA_INVALID_PLAN;

		return first_plan;
	}
}
