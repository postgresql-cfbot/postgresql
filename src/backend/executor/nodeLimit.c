/*-------------------------------------------------------------------------
 *
 * nodeLimit.c
 *	  Routines to handle limiting of query results where appropriate
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeLimit.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecLimit		- extract a limited range of tuples
 *		ExecInitLimit	- initialize node and subnodes..
 *		ExecEndLimit	- shutdown node and subnodes
 */

#include "postgres.h"

#include <math.h>

#include "executor/executor.h"
#include "executor/nodeLimit.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"

static void recompute_limits(LimitState *node);
static int64 compute_tuples_needed(LimitState *node);

#define IsPercentOption(opt) \
	(opt == LIMIT_OPTION_PERCENT || opt == LIMIT_OPTION_PER_WITH_TIES)

/* ----------------------------------------------------------------
 *		ExecLimit
 *
 *		This is a very simple node which just performs LIMIT/OFFSET
 *		filtering on the stream of tuples returned by a subplan.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *			/* return: a tuple or NULL */
ExecLimit(PlanState *pstate)
{
	LimitState *node = castNode(LimitState, pstate);
	ExprContext *econtext = node->ps.ps_ExprContext;
	ScanDirection direction;
	TupleTableSlot *slot;
	PlanState  *outerPlan;

	CHECK_FOR_INTERRUPTS();

	/*
	 * get information from the node
	 */
	direction = node->ps.state->es_direction;
	outerPlan = outerPlanState(node);
	slot = node->subSlot;

	/*
	 * The main logic is a simple state machine.
	 */
	switch (node->lstate)
	{
		case LIMIT_INITIAL:

			/*
			 * First call for this node, so compute limit/offset. (We can't do
			 * this any earlier, because parameters from upper nodes will not
			 * be set during ExecInitLimit.)  This also sets position = 0 and
			 * changes the state to LIMIT_RESCAN.
			 */
			recompute_limits(node);

			/* FALL THRU */

		case LIMIT_RESCAN:

			/*
			 * If backwards scan, just return NULL without changing state.
			 */
			if (!ScanDirectionIsForward(direction))
				return NULL;

			/*
			 * Check for empty window; if so, treat like empty subplan.
			 */
			if (IsPercentOption(node->limitOption))
			{
				if (node->percent == 0.0)
				{
					node->lstate = LIMIT_EMPTY;
					return NULL;
				}
			}
			else if (node->count <= 0 && !node->noCount)
			{
				node->lstate = LIMIT_EMPTY;
				return NULL;
			}

			/*
			 * Fetch rows from subplan until we reach position > offset.
			 */
			for (;;)
			{
				slot = ExecProcNode(outerPlan);
				if (TupIsNull(slot))
				{
					/*
					 * The subplan returns too few tuples for us to produce
					 * any output at all.
					 */
					node->lstate = LIMIT_EMPTY;
					return NULL;
				}

				/*
				 * Tuple at limit is needed for comparison in subsequent
				 * execution to detect ties.
				 */
				if (node->limitOption == LIMIT_OPTION_WITH_TIES &&
					node->position - node->offset == node->count - 1)
				{
					ExecCopySlot(node->last_slot, slot);
				}
				node->subSlot = slot;
				if (++node->position > node->offset)
					break;
			}

			/*
			 * We may needed this tuple in subsequent scan so put it into
			 * tuplestore.
			 */
			if (IsPercentOption(node->limitOption))
			{
				tuplestore_puttupleslot(node->tupleStore, slot);
				tuplestore_advance(node->tupleStore, true);
			}

			/*
			 * Okay, we have the first tuple of the window.
			 */
			node->lstate = LIMIT_INWINDOW;
			break;

		case LIMIT_EMPTY:

			/*
			 * The subplan is known to return no tuples (or not more than
			 * OFFSET tuples, in general).  So we return no tuples.
			 */
			return NULL;

		case LIMIT_INWINDOW:
			if (ScanDirectionIsForward(direction))
			{
				/*
				 * In case of coming back from backward scan the tuple is
				 * already in tuple store.
				 */
				if (IsPercentOption(node->limitOption) && node->backwardPosition > 0)
				{
					if (tuplestore_gettupleslot_heaptuple(node->tupleStore, true, true, slot))
					{
						node->subSlot = slot;
						node->position++;
						node->backwardPosition--;
						return slot;
					}
					else
					{
						node->lstate = LIMIT_SUBPLANEOF;
						return NULL;
					}
				}

				/*
				 * In LIMIT_OPTION_PERCENT case no need of executing outerPlan
				 * multiple times.
				 */
				if (IsPercentOption(node->limitOption) && node->reachEnd)
				{
					node->lstate = LIMIT_WINDOWEND;

					/*
					 * If we know we won't need to back up, we can release
					 * resources at this point.
					 */
					if (!(node->ps.state->es_top_eflags & EXEC_FLAG_BACKWARD))
						(void) ExecShutdownNode(outerPlan);

					/*
					 * The only operation from here is backward scan We have
					 * to move one postion forward to get previous tuple
					 */
					tuplestore_advance(node->tupleStore, true);

					return NULL;
				}

				/*
				 * When in percentage mode, we need to see if we can get any
				 * additional rows from the subplan (enough to increase the
				 * node->count value).
				 */
				if (IsPercentOption(node->limitOption))
				{
					/*
					 * loop until the node->count became greater than the
					 * number of tuple returned so far
					 */
					do
					{
						int64		cnt;

						slot = ExecProcNode(outerPlan);
						if (TupIsNull(slot))
						{
							node->reachEnd = true;
							if (node->limitOption == LIMIT_OPTION_PER_WITH_TIES)
							{
								slot = node->subSlot;
								tuplestore_advance(node->tupleStore, false);
								if (!tuplestore_gettupleslot_heaptuple(node->tupleStore, true, true, slot))
								{
									node->lstate = LIMIT_SUBPLANEOF;
									tuplestore_advance(node->tupleStore, true);
									return NULL;
								}

								ExecCopySlot(node->last_slot, slot);
								node->lstate = LIMIT_WINDOWEND_TIES;
								/* we'll fall through to the next case */
							}
							else
							{
								node->reachEnd = true;
								node->lstate = LIMIT_SUBPLANEOF;

								/*
								 * The only operation from here is backward
								 * scan but there's no API to refetch the
								 * tuple at the current position. We have to
								 * move one postion backward, and then we will
								 * scan forward for it for the first tuple and
								 * precede as usual for the rest
								 */
								tuplestore_advance(node->tupleStore, true);
								return NULL;
							}
						}
						if (node->lstate != LIMIT_WINDOWEND_TIES)
						{
							tuplestore_puttupleslot(node->tupleStore, slot);

							cnt = tuplestore_tuple_count(node->tupleStore) + node->offset;

							node->count = ceil(node->percent * cnt / 100.0);
						}
					} while (node->position - node->offset >= node->count && node->lstate != LIMIT_WINDOWEND_TIES);
				}

				/*
				 * Forwards scan, so check for stepping off end of window.  At
				 * the end of the window, the behavior depends on whether WITH
				 * TIES was specified: if so, we need to change the state
				 * machine to WINDOWEND_TIES, and fall through to the code for
				 * that case.  If not (nothing was specified, or ONLY was)
				 * return NULL without advancing the subplan or the position
				 * variable, but change the state machine to record having
				 * done so.
				 *
				 * Once at the end, ideally, we would shut down parallel
				 * resources; but that would destroy the parallel context
				 * which might be required for rescans.  To do that, we'll
				 * need to find a way to pass down more information about
				 * whether rescans are possible.
				 */
				if (!node->noCount &&
					node->position - node->offset >= node->count
					&& !IsPercentOption(node->limitOption) && node->lstate != LIMIT_WINDOWEND_TIES)
				{
					if (node->limitOption == LIMIT_OPTION_COUNT)
					{
						node->lstate = LIMIT_WINDOWEND;
						return NULL;
					}
					else
					{
						node->lstate = LIMIT_WINDOWEND_TIES;
						/* we'll fall through to the next case */
					}
				}
				else if (IsPercentOption(node->limitOption) && node->lstate != LIMIT_WINDOWEND_TIES)
				{
					if (tuplestore_gettupleslot_heaptuple(node->tupleStore, true, true, slot))
					{
						node->subSlot = slot;
						node->position++;
						break;
					}
					else
					{
						node->lstate = LIMIT_SUBPLANEOF;
						return NULL;
					}
				}
				else if (!IsPercentOption(node->limitOption) && node->lstate != LIMIT_WINDOWEND_TIES)
				{
					/*
					 * Get next tuple from subplan, if any.
					 */
					slot = ExecProcNode(outerPlan);
					if (TupIsNull(slot))
					{
						node->lstate = LIMIT_SUBPLANEOF;
						return NULL;
					}

					/*
					 * If WITH TIES is active, and this is the last in-window
					 * tuple, save it to be used in subsequent WINDOWEND_TIES
					 * processing.
					 */
					if (node->limitOption == LIMIT_OPTION_WITH_TIES &&
						node->position - node->offset == node->count - 1)
					{
						ExecCopySlot(node->last_slot, slot);
					}
					node->subSlot = slot;
					node->position++;
					break;
				}
			}
			else
			{
				/*
				 * Backwards scan, so check for stepping off start of window.
				 * As above, only change state-machine status if so.
				 */
				if (node->position <= node->offset + 1)
				{
					node->lstate = LIMIT_WINDOWSTART;
					return NULL;
				}

				/* In percent case the result is already in tuplestore */
				if (IsPercentOption(node->limitOption))
				{
					if (tuplestore_gettupleslot_heaptuple(node->tupleStore, false, true, slot))
					{
						node->subSlot = slot;
						node->position--;
						node->backwardPosition++;
						break;
					}
					else
						elog(ERROR, "LIMIT subplan failed to run backwards");
				}
				else
				{
					/*
					 * Get previous tuple from subplan; there should be one!
					 */
					slot = ExecProcNode(outerPlan);
					if (TupIsNull(slot))
						elog(ERROR, "LIMIT subplan failed to run backwards");
					node->subSlot = slot;
					node->position--;
					break;
				}
			}

			Assert(node->lstate == LIMIT_WINDOWEND_TIES);
			/* FALL THRU */

		case LIMIT_WINDOWEND_TIES:
			if (ScanDirectionIsForward(direction))
			{
				/*
				 * Advance the subplan or tuple store until we find the first
				 * row with different ORDER BY pathkeys.
				 */
				if (node->limitOption == LIMIT_OPTION_PER_WITH_TIES)
				{
					if (!tuplestore_gettupleslot_heaptuple(node->tupleStore, true, true, slot))
					{
						node->lstate = LIMIT_SUBPLANEOF;
						return NULL;
					}
				}
				else
				{
					slot = ExecProcNode(outerPlan);
					if (TupIsNull(slot))
					{
						node->lstate = LIMIT_SUBPLANEOF;
						return NULL;
					}
				}

				/*
				 * Test if the new tuple and the last tuple match. If so we
				 * return the tuple.
				 */
				econtext->ecxt_innertuple = slot;
				econtext->ecxt_outertuple = node->last_slot;
				if (ExecQualAndReset(node->eqfunction, econtext))
				{
					node->subSlot = slot;
					node->position++;
				}
				else
				{
					node->lstate = LIMIT_WINDOWEND;
					return NULL;
				}
			}
			else
			{
				/*
				 * Backwards scan, so check for stepping off start of window.
				 * Change only state-machine status if so.
				 */
				if (node->position <= node->offset + 1)
				{
					node->lstate = LIMIT_WINDOWSTART;
					return NULL;
				}

				/*
				 * Get previous tuple from subplan or tuple store; there
				 * should be one! And change state-machine status.
				 */
				if (node->limitOption == LIMIT_OPTION_PER_WITH_TIES)
				{
					if (tuplestore_gettupleslot_heaptuple(node->tupleStore, false, true, slot))
					{
						node->backwardPosition++;
						node->position--;
						node->subSlot = slot;
						node->lstate = LIMIT_INWINDOW;
					}
					else
						elog(ERROR, "LIMIT subplan failed to run backwards");
				}
				else
				{
					slot = ExecProcNode(outerPlan);
					if (TupIsNull(slot))
						elog(ERROR, "LIMIT subplan failed to run backwards");
					node->subSlot = slot;
					node->position--;
					node->lstate = LIMIT_INWINDOW;
				}
			}
			break;

		case LIMIT_SUBPLANEOF:
			if (ScanDirectionIsForward(direction))
				return NULL;

			/*
			 * Scan forward for the previous tuple. there should be one!  Note
			 * previous tuple must be in window.
			 */
			if (IsPercentOption(node->limitOption))
			{
				if (tuplestore_gettupleslot_heaptuple(node->tupleStore, false, true, slot))
				{
					node->subSlot = slot;
					node->lstate = LIMIT_INWINDOW;
				}
				else
					elog(ERROR, "LIMIT subplan failed to run backwards");
			}
			else
			{
				/*
				 * Backing up from subplan EOF, so re-fetch previous tuple;
				 * there should be one!  Note previous tuple must be in
				 * window.
				 */
				slot = ExecProcNode(outerPlan);
				if (TupIsNull(slot))
					elog(ERROR, "LIMIT subplan failed to run backwards");
				node->subSlot = slot;
				node->lstate = LIMIT_INWINDOW;
				/* position does not change 'cause we didn't advance it before */
			}
			break;

		case LIMIT_WINDOWEND:
			if (ScanDirectionIsForward(direction))
				return NULL;

			/*
			 * We already past one position to detect ties so re-fetch
			 * previous tuple; there should be one!  Note previous tuple must
			 * be in window.
			 */
			if (node->limitOption == LIMIT_OPTION_WITH_TIES)
			{
				slot = ExecProcNode(outerPlan);
				if (TupIsNull(slot))
					elog(ERROR, "LIMIT subplan failed to run backwards");
				node->subSlot = slot;
				node->lstate = LIMIT_INWINDOW;
			}
			if (node->limitOption == LIMIT_OPTION_PER_WITH_TIES)
			{
				if (tuplestore_gettupleslot_heaptuple(node->tupleStore, false, true, slot))
				{
					node->subSlot = slot;
					node->lstate = LIMIT_INWINDOW;
				}
				else
					elog(ERROR, "LIMIT subplan failed to run backwards");
			}
			else
			{
				/*
				 * Backing up from window end: simply re-return the last tuple
				 * fetched from the subplan.
				 */
				slot = node->subSlot;
				node->lstate = LIMIT_INWINDOW;
				/* position does not change 'cause we didn't advance it before */
			}
			break;

		case LIMIT_WINDOWSTART:
			if (!ScanDirectionIsForward(direction))
				return NULL;

			/*
			 * Advancing after having backed off window start: simply
			 * re-return the last tuple fetched from the subplan.
			 */
			slot = node->subSlot;
			node->lstate = LIMIT_INWINDOW;
			/* position does not change 'cause we didn't change it before */
			break;

		default:
			elog(ERROR, "impossible LIMIT state: %d",
				 (int) node->lstate);
			slot = NULL;		/* keep compiler quiet */
			break;
	}

	/* Return the current tuple */
	Assert(!TupIsNull(slot));

	return slot;
}

/*
 * Evaluate the limit/offset expressions --- done at startup or rescan.
 *
 * This is also a handy place to reset the current-position state info.
 */
static void
recompute_limits(LimitState *node)
{
	ExprContext *econtext = node->ps.ps_ExprContext;
	Datum		val;
	bool		isNull;

	if (node->limitOffset)
	{
		val = ExecEvalExprSwitchContext(node->limitOffset,
										econtext,
										&isNull);
		/* Interpret NULL offset as no offset */
		if (isNull)
			node->offset = 0;
		else
		{
			node->offset = DatumGetInt64(val);
			if (node->offset < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE),
						 errmsg("OFFSET must not be negative")));
		}
	}
	else
	{
		/* No OFFSET supplied */
		node->offset = 0;
	}

	if (node->limitCount)
	{
		val = ExecEvalExprSwitchContext(node->limitCount,
										econtext,
										&isNull);
		/* Interpret NULL count as no count (LIMIT ALL) */
		if (isNull)
		{
			node->count = 0;
			node->noCount = true;
		}
		else
		{
			if (IsPercentOption(node->limitOption))
			{
				/*
				 * We expect to return at least one row (unless there are no
				 * rows in the subplan), and we'll update this count later as
				 * we go.
				 */
				node->count = 0;
				node->percent = DatumGetFloat8(val);

				if (node->percent < 0)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE),
							 errmsg("PERCENT must not be negative")));

				if (node->percent > 100)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE),
							 errmsg("PERCENT must not be greater than 100")));

			}
			else
			{
				node->count = DatumGetInt64(val);
				if (node->count < 0)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE),
							 errmsg("LIMIT must not be negative")));

			}
		}
	}
	else
	{
		/* No COUNT supplied */
		node->count = 0;
		node->noCount = true;
	}

	/* Reset position to start-of-scan */
	node->position = 0;
	node->subSlot = NULL;
	node->reachEnd = false;
	node->backwardPosition = 0;

	/* Set state-machine state */
	node->lstate = LIMIT_RESCAN;

	/*
	 * Notify child node about limit.  Note: think not to "optimize" by
	 * skipping ExecSetTupleBound if compute_tuples_needed returns < 0.  We
	 * must update the child node anyway, in case this is a rescan and the
	 * previous time we got a different result. In LIMIT_OPTION_PERCENT option
	 * there are no bound on the number of output tuples
	 */
	ExecSetTupleBound(compute_tuples_needed(node), outerPlanState(node));
}

/*
 * Compute the maximum number of tuples needed to satisfy this Limit node.
 * Return a negative value if there is not a determinable limit.
 */
static int64
compute_tuples_needed(LimitState *node)
{
	if ((node->noCount) || (node->limitOption == LIMIT_OPTION_WITH_TIES)
		|| (IsPercentOption(node->limitOption)))
		return -1;
	/* Note: if this overflows, we'll return a negative value, which is OK */
	return node->count + node->offset;
}

/* ----------------------------------------------------------------
 *		ExecInitLimit
 *
 *		This initializes the limit node state structures and
 *		the node's subplan.
 * ----------------------------------------------------------------
 */
LimitState *
ExecInitLimit(Limit *node, EState *estate, int eflags)
{
	LimitState *limitstate;
	Plan	   *outerPlan;

	/* check for unsupported flags */
	Assert(!(eflags & EXEC_FLAG_MARK));

	/*
	 * create state structure
	 */
	limitstate = makeNode(LimitState);
	limitstate->ps.plan = (Plan *) node;
	limitstate->ps.state = estate;
	limitstate->ps.ExecProcNode = ExecLimit;

	limitstate->lstate = LIMIT_INITIAL;

	/*
	 * Miscellaneous initialization
	 *
	 * Limit nodes never call ExecQual or ExecProject, but they need an
	 * exprcontext anyway to evaluate the limit/offset parameters in.
	 */
	ExecAssignExprContext(estate, &limitstate->ps);

	/*
	 * initialize outer plan
	 */
	outerPlan = outerPlan(node);
	outerPlanState(limitstate) = ExecInitNode(outerPlan, estate, eflags);

	/*
	 * initialize child expressions
	 */
	limitstate->limitOffset = ExecInitExpr((Expr *) node->limitOffset,
										   (PlanState *) limitstate);
	limitstate->limitCount = ExecInitExpr((Expr *) node->limitCount,
										  (PlanState *) limitstate);
	limitstate->limitOption = node->limitOption;

	/*
	 * Initialize result type.
	 */
	ExecInitResultTypeTL(&limitstate->ps);

	limitstate->ps.resultopsset = true;
	limitstate->ps.resultops = ExecGetResultSlotOps(outerPlanState(limitstate),
													&limitstate->ps.resultopsfixed);

	/*
	 * limit nodes do no projections, so initialize projection info for this
	 * node appropriately
	 */
	limitstate->ps.ps_ProjInfo = NULL;

	/*
	 * Initialize the equality evaluation, to detect ties.
	 */
	if (node->limitOption == LIMIT_OPTION_WITH_TIES
		|| node->limitOption == LIMIT_OPTION_PER_WITH_TIES)
	{
		TupleDesc	desc;
		const TupleTableSlotOps *ops;

		desc = ExecGetResultType(outerPlanState(limitstate));
		ops = ExecGetResultSlotOps(outerPlanState(limitstate), NULL);

		limitstate->last_slot = ExecInitExtraTupleSlot(estate, desc, ops);
		limitstate->eqfunction = execTuplesMatchPrepare(desc,
														node->uniqNumCols,
														node->uniqColIdx,
														node->uniqOperators,
														node->uniqCollations,
														&limitstate->ps);
	}

	if (IsPercentOption(node->limitOption))
		limitstate->tupleStore = tuplestore_begin_heap(true, false, work_mem);

	return limitstate;
}

/* ----------------------------------------------------------------
 *		ExecEndLimit
 *
 *		This shuts down the subplan and frees resources allocated
 *		to this node.
 * ----------------------------------------------------------------
 */
void
ExecEndLimit(LimitState *node)
{
	ExecFreeExprContext(&node->ps);
	ExecEndNode(outerPlanState(node));
	if (node->tupleStore != NULL)
		tuplestore_end(node->tupleStore);
}


void
ExecReScanLimit(LimitState *node)
{
	/*
	 * Recompute limit/offset in case parameters changed, and reset the state
	 * machine.  We must do this before rescanning our child node, in case
	 * it's a Sort that we are passing the parameters down to.
	 */
	recompute_limits(node);

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->ps.lefttree->chgParam == NULL)
		ExecReScan(node->ps.lefttree);
	if (node->tupleStore != NULL)
		tuplestore_clear(node->tupleStore);
}
