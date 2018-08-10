/*-------------------------------------------------------------------------
 *
 * nodeSubplan.c
 *	  routines to support sub-selects appearing in expressions
 *
 * This module is concerned with executing SubPlan expression nodes, which
 * should not be confused with sub-SELECTs appearing in FROM.  SubPlans are
 * divided into "initplans", which are those that need only one evaluation per
 * query (among other restrictions, this requires that they don't use any
 * direct correlation variables from the parent plan level), and "regular"
 * subplans, which are re-evaluated every time their result is required.
 *
 * There are two ways a SubPlan node can return the result.  The first is
 * that ExecSubPlan() runs the subquery, and returns its result like any
 * other expression.  The second way is to return the result in executor
 * Params.  In this method, the execution happens when ExecSetParamPlan() is
 * called, and the subquery's results are set as the current values of
 * executor Params, as indicated by SubPlan->setParam.
 *
 * For correlated subplans, which need to be re-evaluated every time the
 * outer parameter values change, we optionally keep a cache of the results.
 * If the subplan is called again with the same set of parameters, we can
 * avoid re-executing the subquery, and return the result from the cache
 * instead.  This helps a lot if the subplan is expensive, and the same
 * parameters are used many times.
 *
 * TODO: The cache is currently only used in ExecSubPlan, not
 * ExecSetParamPlan.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSubplan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 *	 INTERFACE ROUTINES
 *		ExecSubPlan  - process a subselect
 *		ExecInitSubPlan - initialize a subselect
 */
#include "postgres.h"

#include <limits.h>
#include <math.h>

#include "access/htup_details.h"
#include "executor/executor.h"
#include "executor/nodeSubplan.h"
#include "lib/ilist.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/hashutils.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"


static Datum ExecHashSubPlan(SubPlanState *node,
				ExprContext *econtext,
				bool *isNull);
static Datum ExecScanSubPlan(SubPlanState *node,
				ExprContext *econtext,
				bool *isNull);
static void buildSubPlanHash(SubPlanState *node, ExprContext *econtext);
static bool findPartialMatch(TupleHashTable hashtable, TupleTableSlot *slot,
				 FmgrInfo *eqfunctions);
static bool slotAllNulls(TupleTableSlot *slot);
static bool slotNoNulls(TupleTableSlot *slot);

/*
 * Subplan result cache.
 *
 * If the subplan doesn't contain volatile expressions, we can avoid
 * executing it repeatedly for the same parameters, by caching the results.
 *
 * The cache is a hash table.  The cache key consists of the parameter values.
 * The number of parameters depends on the subquery, so we keep them at the
 * end of the struct.
 *
 * The cached result is the final result of ExecSubPlan, after evaluating any
 * 'testexpr' or other sublink-specific processing.  For ARRAY type sublink,
 * it is the fully formed array, for EXISTS and ROWCOMPARE, it is a boolean.
 *
 * The cache is of fixed size.  If it fills up, we throw away old entries,
 * using a simple LRU policy.
 */
typedef struct
{
	Datum		result;			/* result of the SubPlan with this param */
	bool		resultNull;

	dlist_node	lru_link;		/* link in the LRU list. */

	/*
	 * SubPlan parameters.  These form the cache key.
	 *
	 * After the 'params', there is a corresponding array of 'isnull' flags.
	 * Use SUBPLAN_RESULT_CACHE_ENTRY_GET_NULLS to access it.
	 */
	Datum		params[FLEXIBLE_ARRAY_MEMBER];

	/* bool		paramNulls[FLEXIBLE_ARRAY_MEMBER]; */

} SubPlanResultCacheEntry;

/* Macros to work with SubPlanResultCacheEntry */
#define SUBPLAN_RESULT_CACHE_KEY_SIZE(numparams) \
	((numparams) * sizeof(Datum) + (numparams) * sizeof(bool))
#define SUBPLAN_RESULT_CACHE_ENTRY_SIZE(numparams) \
	(offsetof(SubPlanResultCacheEntry, params) + \
	 SUBPLAN_RESULT_CACHE_KEY_SIZE(numparams))

#define SUBPLAN_RESULT_CACHE_ENTRY_GET_NULLS(entry, numparams) \
	((bool *) &(entry)->params[numparams])

static void init_result_cache(SubPlanState *sstate);
static SubPlanResultCacheEntry * lookup_result_cache(SubPlanState *sstate,
					SubPlanResultCacheEntry *key, bool *found);
static uint32 result_cache_hash(const void *key, Size keysize);
static int result_cache_match(const void *key1, const void *key2, Size keysize);
static void *result_cache_keycopy(void *dest, const void *src, Size keysize);

static SubPlanState *curr_sstate;

/* ----------------------------------------------------------------
 *		ExecSubPlan
 *
 * This is the main entry point for execution of a regular SubPlan.
 * ----------------------------------------------------------------
 */
Datum
ExecSubPlan(SubPlanState *node,
			ExprContext *econtext,
			bool *isNull)
{
	SubPlan    *subplan = node->subplan;

	CHECK_FOR_INTERRUPTS();

	/* Set non-null as default */
	*isNull = false;

	/* Sanity checks */
	if (subplan->subLinkType == CTE_SUBLINK)
		elog(ERROR, "CTE subplans should not be executed via ExecSubPlan");
	if (subplan->setParam != NIL && subplan->subLinkType != MULTIEXPR_SUBLINK)
		elog(ERROR, "cannot set parent params from subquery");

	/* Select appropriate evaluation strategy */
	if (subplan->useHashTable)
		return ExecHashSubPlan(node, econtext, isNull);
	else
		return ExecScanSubPlan(node, econtext, isNull);
}

/*
 * ExecHashSubPlan: store subselect result in an in-memory hash table
 */
static Datum
ExecHashSubPlan(SubPlanState *node,
				ExprContext *econtext,
				bool *isNull)
{
	SubPlan    *subplan = node->subplan;
	PlanState  *planstate = node->planstate;
	TupleTableSlot *slot;

	/* Shouldn't have any direct correlation Vars */
	if (subplan->parParam != NIL || node->args != NIL)
		elog(ERROR, "hashed subplan with direct correlation not supported");

	/*
	 * If first time through or we need to rescan the subplan, build the hash
	 * table.
	 */
	if (node->hashtable == NULL || planstate->chgParam != NULL)
		buildSubPlanHash(node, econtext);

	/*
	 * The result for an empty subplan is always FALSE; no need to evaluate
	 * lefthand side.
	 */
	*isNull = false;
	if (!node->havehashrows && !node->havenullrows)
		return BoolGetDatum(false);

	/*
	 * Evaluate lefthand expressions and form a projection tuple. First we
	 * have to set the econtext to use (hack alert!).
	 */
	node->projLeft->pi_exprContext = econtext;
	slot = ExecProject(node->projLeft);

	/*
	 * Note: because we are typically called in a per-tuple context, we have
	 * to explicitly clear the projected tuple before returning. Otherwise,
	 * we'll have a double-free situation: the per-tuple context will probably
	 * be reset before we're called again, and then the tuple slot will think
	 * it still needs to free the tuple.
	 */

	/*
	 * If the LHS is all non-null, probe for an exact match in the main hash
	 * table.  If we find one, the result is TRUE. Otherwise, scan the
	 * partly-null table to see if there are any rows that aren't provably
	 * unequal to the LHS; if so, the result is UNKNOWN.  (We skip that part
	 * if we don't care about UNKNOWN.) Otherwise, the result is FALSE.
	 *
	 * Note: the reason we can avoid a full scan of the main hash table is
	 * that the combining operators are assumed never to yield NULL when both
	 * inputs are non-null.  If they were to do so, we might need to produce
	 * UNKNOWN instead of FALSE because of an UNKNOWN result in comparing the
	 * LHS to some main-table entry --- which is a comparison we will not even
	 * make, unless there's a chance match of hash keys.
	 */
	if (slotNoNulls(slot))
	{
		if (node->havehashrows &&
			FindTupleHashEntry(node->hashtable,
							   slot,
							   node->cur_eq_comp,
							   node->lhs_hash_funcs) != NULL)
		{
			ExecClearTuple(slot);
			return BoolGetDatum(true);
		}
		if (node->havenullrows &&
			findPartialMatch(node->hashnulls, slot, node->cur_eq_funcs))
		{
			ExecClearTuple(slot);
			*isNull = true;
			return BoolGetDatum(false);
		}
		ExecClearTuple(slot);
		return BoolGetDatum(false);
	}

	/*
	 * When the LHS is partly or wholly NULL, we can never return TRUE. If we
	 * don't care about UNKNOWN, just return FALSE.  Otherwise, if the LHS is
	 * wholly NULL, immediately return UNKNOWN.  (Since the combining
	 * operators are strict, the result could only be FALSE if the sub-select
	 * were empty, but we already handled that case.) Otherwise, we must scan
	 * both the main and partly-null tables to see if there are any rows that
	 * aren't provably unequal to the LHS; if so, the result is UNKNOWN.
	 * Otherwise, the result is FALSE.
	 */
	if (node->hashnulls == NULL)
	{
		ExecClearTuple(slot);
		return BoolGetDatum(false);
	}
	if (slotAllNulls(slot))
	{
		ExecClearTuple(slot);
		*isNull = true;
		return BoolGetDatum(false);
	}
	/* Scan partly-null table first, since more likely to get a match */
	if (node->havenullrows &&
		findPartialMatch(node->hashnulls, slot, node->cur_eq_funcs))
	{
		ExecClearTuple(slot);
		*isNull = true;
		return BoolGetDatum(false);
	}
	if (node->havehashrows &&
		findPartialMatch(node->hashtable, slot, node->cur_eq_funcs))
	{
		ExecClearTuple(slot);
		*isNull = true;
		return BoolGetDatum(false);
	}
	ExecClearTuple(slot);
	return BoolGetDatum(false);
}

/*
 * ExecScanSubPlan: default case where we have to rescan subplan each time
 */
static Datum
ExecScanSubPlan(SubPlanState *node,
				ExprContext *econtext,
				bool *isNull)
{
	SubPlan    *subplan = node->subplan;
	PlanState  *planstate = node->planstate;
	SubLinkType subLinkType = subplan->subLinkType;
	MemoryContext oldcontext;
	TupleTableSlot *slot;
	Datum		result;
	bool		found = false;	/* true if got at least one subplan tuple */
	ListCell   *pvar;
	ListCell   *l;
	ArrayBuildStateAny *astate = NULL;
	SubPlanResultCacheEntry *resultCacheEntry = NULL;
	int			i;
	Datum	   *currParams;
	bool	   *currParamNulls;

	/*
	 * MULTIEXPR subplans, when "executed", just return NULL; but first we
	 * mark the subplan's output parameters as needing recalculation.  (This
	 * is a bit of a hack: it relies on the subplan appearing later in its
	 * targetlist than any of the referencing Params, so that all the Params
	 * have been evaluated before we re-mark them for the next evaluation
	 * cycle.  But in general resjunk tlist items appear after non-resjunk
	 * ones, so this should be safe.)  Unlike ExecReScanSetParamPlan, we do
	 * *not* set bits in the parent plan node's chgParam, because we don't
	 * want to cause a rescan of the parent.
	 */
	if (subLinkType == MULTIEXPR_SUBLINK)
	{
		EState	   *estate = node->parent->state;

		foreach(l, subplan->setParam)
		{
			int			paramid = lfirst_int(l);
			ParamExecData *prm = &(estate->es_param_exec_vals[paramid]);

			prm->execPlan = node;
		}
		*isNull = true;
		return (Datum) 0;
	}

	/* Initialize ArrayBuildStateAny in caller's context, if needed */
	if (subLinkType == ARRAY_SUBLINK)
		astate = initArrayResultAny(subplan->firstColType,
									CurrentMemoryContext, true);

	/*
	 * If first time through, initialize the result cache.
	 */
	if (subplan->useResultCache && node->resultcachetab == NULL)
		init_result_cache(node);

	/*
	 * We are probably in a short-lived expression-evaluation context. Switch
	 * to the per-query context for manipulating the child plan's chgParam,
	 * calling ExecProcNode on it, etc.
	 */
	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);

	/*
	 * Set Params of this plan from parent plan correlation values. (Any
	 * calculation we have to do is done in the parent econtext, since the
	 * Param values don't need to have per-query lifetime.)
	 */
	Assert(list_length(subplan->parParam) == list_length(node->args));

	i = 0;
	currParams = ((SubPlanResultCacheEntry *) node->currCacheKey)->params;
	currParamNulls = SUBPLAN_RESULT_CACHE_ENTRY_GET_NULLS((SubPlanResultCacheEntry *) node->currCacheKey, node->numparams);
	forboth(l, subplan->parParam, pvar, node->args)
	{
		int			paramid = lfirst_int(l);
		ParamExecData *prm = &(econtext->ecxt_param_exec_vals[paramid]);

		prm->value = ExecEvalExprSwitchContext((ExprState *) lfirst(pvar),
											   econtext,
											   &(prm->isnull));
		planstate->chgParam = bms_add_member(planstate->chgParam, paramid);

		if (subplan->useResultCache)
		{
			currParams[i] = prm->value;
			currParamNulls[i] = prm->isnull;
		}

		i++;
	}

	/*
	 * If we're caching the subplan results, check the cache first.
	 *
	 * On cache miss, this creates a new entry for this set of parameters.
	 * We'll fill it with the subplan's result later.
	 */
	if (subplan->useResultCache)
	{
		bool		found;

		resultCacheEntry = lookup_result_cache(node, node->currCacheKey,
											   &found);
		if (found)
		{
			/*
			 * Cache hit. If the result datatype is pass-by-ref, make a copy
			 * of it in a short-lived memory context.
			 */
			if (resultCacheEntry->resultNull)
			{
				result = (Datum) 0;
				*isNull = true;
			}
			else
			{
				result = datumCopy(resultCacheEntry->result,
								   node->resultByVal,
								   node->resultLen);
				*isNull = false;
			}

			return result;
		}

		/* Not found. Fall through to execute the subplan. */
	}

	/*
	 * Now that we've set up its parameters, we can reset the subplan.
	 */
	ExecReScan(planstate);

	/*
	 * For all sublink types except EXPR_SUBLINK and ARRAY_SUBLINK, the result
	 * is boolean as are the results of the combining operators. We combine
	 * results across tuples (if the subplan produces more than one) using OR
	 * semantics for ANY_SUBLINK or AND semantics for ALL_SUBLINK.
	 * (ROWCOMPARE_SUBLINK doesn't allow multiple tuples from the subplan.)
	 * NULL results from the combining operators are handled according to the
	 * usual SQL semantics for OR and AND.  The result for no input tuples is
	 * FALSE for ANY_SUBLINK, TRUE for ALL_SUBLINK, NULL for
	 * ROWCOMPARE_SUBLINK.
	 *
	 * For EXPR_SUBLINK we require the subplan to produce no more than one
	 * tuple, else an error is raised.  If zero tuples are produced, we return
	 * NULL.  Assuming we get a tuple, we just use its first column (there can
	 * be only one non-junk column in this case).
	 *
	 * For ARRAY_SUBLINK we allow the subplan to produce any number of tuples,
	 * and form an array of the first column's values.  Note in particular
	 * that we produce a zero-element array if no tuples are produced (this is
	 * a change from pre-8.3 behavior of returning NULL).
	 */
	result = BoolGetDatum(subLinkType == ALL_SUBLINK);
	*isNull = false;

	for (slot = ExecProcNode(planstate);
		 !TupIsNull(slot);
		 slot = ExecProcNode(planstate))
	{
		TupleDesc	tdesc = slot->tts_tupleDescriptor;
		Datum		rowresult;
		bool		rownull;
		int			col;
		ListCell   *plst;

		if (subLinkType == EXISTS_SUBLINK)
		{
			found = true;
			result = BoolGetDatum(true);
			break;
		}

		if (subLinkType == EXPR_SUBLINK)
		{
			/* cannot allow multiple input tuples for EXPR sublink */
			if (found)
				ereport(ERROR,
						(errcode(ERRCODE_CARDINALITY_VIOLATION),
						 errmsg("more than one row returned by a subquery used as an expression")));
			found = true;

			/*
			 * We need to copy the subplan's tuple in case the result is of
			 * pass-by-ref type --- our return value will point into this
			 * copied tuple!  Can't use the subplan's instance of the tuple
			 * since it won't still be valid after next ExecProcNode() call.
			 * node->curTuple keeps track of the copied tuple for eventual
			 * freeing.
			 */
			if (node->curTuple)
				heap_freetuple(node->curTuple);
			node->curTuple = ExecCopySlotTuple(slot);

			result = heap_getattr(node->curTuple, 1, tdesc, isNull);
			/* keep scanning subplan to make sure there's only one tuple */
			continue;
		}

		if (subLinkType == ARRAY_SUBLINK)
		{
			Datum		dvalue;
			bool		disnull;

			found = true;
			/* stash away current value */
			Assert(subplan->firstColType == TupleDescAttr(tdesc, 0)->atttypid);
			dvalue = slot_getattr(slot, 1, &disnull);
			astate = accumArrayResultAny(astate, dvalue, disnull,
										 subplan->firstColType, oldcontext);
			/* keep scanning subplan to collect all values */
			continue;
		}

		/* cannot allow multiple input tuples for ROWCOMPARE sublink either */
		if (subLinkType == ROWCOMPARE_SUBLINK && found)
			ereport(ERROR,
					(errcode(ERRCODE_CARDINALITY_VIOLATION),
					 errmsg("more than one row returned by a subquery used as an expression")));

		found = true;

		/*
		 * For ALL, ANY, and ROWCOMPARE sublinks, load up the Params
		 * representing the columns of the sub-select, and then evaluate the
		 * combining expression.
		 */
		col = 1;
		foreach(plst, subplan->paramIds)
		{
			int			paramid = lfirst_int(plst);
			ParamExecData *prmdata;

			prmdata = &(econtext->ecxt_param_exec_vals[paramid]);
			Assert(prmdata->execPlan == NULL);
			prmdata->value = slot_getattr(slot, col, &(prmdata->isnull));
			col++;
		}

		rowresult = ExecEvalExprSwitchContext(node->testexpr, econtext,
											  &rownull);

		if (subLinkType == ANY_SUBLINK)
		{
			/* combine across rows per OR semantics */
			if (rownull)
				*isNull = true;
			else if (DatumGetBool(rowresult))
			{
				result = BoolGetDatum(true);
				*isNull = false;
				break;			/* needn't look at any more rows */
			}
		}
		else if (subLinkType == ALL_SUBLINK)
		{
			/* combine across rows per AND semantics */
			if (rownull)
				*isNull = true;
			else if (!DatumGetBool(rowresult))
			{
				result = BoolGetDatum(false);
				*isNull = false;
				break;			/* needn't look at any more rows */
			}
		}
		else
		{
			/* must be ROWCOMPARE_SUBLINK */
			result = rowresult;
			*isNull = rownull;
		}
	}

	MemoryContextSwitchTo(oldcontext);

	if (subLinkType == ARRAY_SUBLINK)
	{
		/* We return the result in the caller's context */
		result = makeArrayResultAny(astate, oldcontext, true);
	}
	else if (!found)
	{
		/*
		 * deal with empty subplan result.  result/isNull were previously
		 * initialized correctly for all sublink types except EXPR and
		 * ROWCOMPARE; for those, return NULL.
		 */
		if (subLinkType == EXPR_SUBLINK ||
			subLinkType == ROWCOMPARE_SUBLINK)
		{
			result = (Datum) 0;
			*isNull = true;
		}
	}

	/*
	 * If we're using the result cache, copy the result of the subplan into
	 * the cache entry.
	 */
	if (resultCacheEntry)
	{
		if (*isNull)
		{
			resultCacheEntry->result = (Datum) 0;
			resultCacheEntry->resultNull = true;
		}
		else
		{
			MemoryContext oldcxt = MemoryContextSwitchTo(node->hashtablecxt);

			resultCacheEntry->result = datumCopy(result,
												 node->resultByVal,
												 node->resultLen);
			resultCacheEntry->resultNull = false;
			MemoryContextSwitchTo(oldcxt);
			if (!node->resultByVal)
				node->memUsed += GetMemoryChunkSpace(DatumGetPointer(resultCacheEntry->result));
		}
	}

	return result;
}

/*
 * buildSubPlanHash: load hash table by scanning subplan output.
 */
static void
buildSubPlanHash(SubPlanState *node, ExprContext *econtext)
{
	SubPlan    *subplan = node->subplan;
	PlanState  *planstate = node->planstate;
	int			ncols = list_length(subplan->paramIds);
	ExprContext *innerecontext = node->innerecontext;
	MemoryContext oldcontext;
	long		nbuckets;
	TupleTableSlot *slot;

	Assert(subplan->subLinkType == ANY_SUBLINK);

	/*
	 * If we already had any hash tables, destroy 'em; then create empty hash
	 * table(s).
	 *
	 * If we need to distinguish accurately between FALSE and UNKNOWN (i.e.,
	 * NULL) results of the IN operation, then we have to store subplan output
	 * rows that are partly or wholly NULL.  We store such rows in a separate
	 * hash table that we expect will be much smaller than the main table. (We
	 * can use hashing to eliminate partly-null rows that are not distinct. We
	 * keep them separate to minimize the cost of the inevitable full-table
	 * searches; see findPartialMatch.)
	 *
	 * If it's not necessary to distinguish FALSE and UNKNOWN, then we don't
	 * need to store subplan output rows that contain NULL.
	 */
	MemoryContextReset(node->hashtablecxt);
	node->hashtable = NULL;
	node->hashnulls = NULL;
	node->havehashrows = false;
	node->havenullrows = false;

	nbuckets = (long) Min(planstate->plan->plan_rows, (double) LONG_MAX);
	if (nbuckets < 1)
		nbuckets = 1;

	node->hashtable = BuildTupleHashTable(node->parent,
										  node->descRight,
										  ncols,
										  node->keyColIdx,
										  node->tab_eq_funcoids,
										  node->tab_hash_funcs,
										  nbuckets,
										  0,
										  node->hashtablecxt,
										  node->hashtempcxt,
										  false);

	if (!subplan->unknownEqFalse)
	{
		if (ncols == 1)
			nbuckets = 1;		/* there can only be one entry */
		else
		{
			nbuckets /= 16;
			if (nbuckets < 1)
				nbuckets = 1;
		}
		node->hashnulls = BuildTupleHashTable(node->parent,
											  node->descRight,
											  ncols,
											  node->keyColIdx,
											  node->tab_eq_funcoids,
											  node->tab_hash_funcs,
											  nbuckets,
											  0,
											  node->hashtablecxt,
											  node->hashtempcxt,
											  false);
	}

	/*
	 * We are probably in a short-lived expression-evaluation context. Switch
	 * to the per-query context for manipulating the child plan.
	 */
	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);

	/*
	 * Reset subplan to start.
	 */
	ExecReScan(planstate);

	/*
	 * Scan the subplan and load the hash table(s).  Note that when there are
	 * duplicate rows coming out of the sub-select, only one copy is stored.
	 */
	for (slot = ExecProcNode(planstate);
		 !TupIsNull(slot);
		 slot = ExecProcNode(planstate))
	{
		int			col = 1;
		ListCell   *plst;
		bool		isnew;

		/*
		 * Load up the Params representing the raw sub-select outputs, then
		 * form the projection tuple to store in the hashtable.
		 */
		foreach(plst, subplan->paramIds)
		{
			int			paramid = lfirst_int(plst);
			ParamExecData *prmdata;

			prmdata = &(innerecontext->ecxt_param_exec_vals[paramid]);
			Assert(prmdata->execPlan == NULL);
			prmdata->value = slot_getattr(slot, col,
										  &(prmdata->isnull));
			col++;
		}
		slot = ExecProject(node->projRight);

		/*
		 * If result contains any nulls, store separately or not at all.
		 */
		if (slotNoNulls(slot))
		{
			(void) LookupTupleHashEntry(node->hashtable, slot, &isnew);
			node->havehashrows = true;
		}
		else if (node->hashnulls)
		{
			(void) LookupTupleHashEntry(node->hashnulls, slot, &isnew);
			node->havenullrows = true;
		}

		/*
		 * Reset innerecontext after each inner tuple to free any memory used
		 * during ExecProject.
		 */
		ResetExprContext(innerecontext);
	}

	/*
	 * Since the projected tuples are in the sub-query's context and not the
	 * main context, we'd better clear the tuple slot before there's any
	 * chance of a reset of the sub-query's context.  Else we will have the
	 * potential for a double free attempt.  (XXX possibly no longer needed,
	 * but can't hurt.)
	 */
	ExecClearTuple(node->projRight->pi_state.resultslot);

	MemoryContextSwitchTo(oldcontext);
}

/*
 * execTuplesUnequal
 *		Return true if two tuples are definitely unequal in the indicated
 *		fields.
 *
 * Nulls are neither equal nor unequal to anything else.  A true result
 * is obtained only if there are non-null fields that compare not-equal.
 *
 * slot1, slot2: the tuples to compare (must have same columns!)
 * numCols: the number of attributes to be examined
 * matchColIdx: array of attribute column numbers
 * eqFunctions: array of fmgr lookup info for the equality functions to use
 * evalContext: short-term memory context for executing the functions
 */
static bool
execTuplesUnequal(TupleTableSlot *slot1,
				  TupleTableSlot *slot2,
				  int numCols,
				  AttrNumber *matchColIdx,
				  FmgrInfo *eqfunctions,
				  MemoryContext evalContext)
{
	MemoryContext oldContext;
	bool		result;
	int			i;

	/* Reset and switch into the temp context. */
	MemoryContextReset(evalContext);
	oldContext = MemoryContextSwitchTo(evalContext);

	/*
	 * We cannot report a match without checking all the fields, but we can
	 * report a non-match as soon as we find unequal fields.  So, start
	 * comparing at the last field (least significant sort key). That's the
	 * most likely to be different if we are dealing with sorted input.
	 */
	result = false;

	for (i = numCols; --i >= 0;)
	{
		AttrNumber	att = matchColIdx[i];
		Datum		attr1,
					attr2;
		bool		isNull1,
					isNull2;

		attr1 = slot_getattr(slot1, att, &isNull1);

		if (isNull1)
			continue;			/* can't prove anything here */

		attr2 = slot_getattr(slot2, att, &isNull2);

		if (isNull2)
			continue;			/* can't prove anything here */

		/* Apply the type-specific equality function */

		if (!DatumGetBool(FunctionCall2(&eqfunctions[i],
										attr1, attr2)))
		{
			result = true;		/* they are unequal */
			break;
		}
	}

	MemoryContextSwitchTo(oldContext);

	return result;
}

/*
 * findPartialMatch: does the hashtable contain an entry that is not
 * provably distinct from the tuple?
 *
 * We have to scan the whole hashtable; we can't usefully use hashkeys
 * to guide probing, since we might get partial matches on tuples with
 * hashkeys quite unrelated to what we'd get from the given tuple.
 *
 * Caller must provide the equality functions to use, since in cross-type
 * cases these are different from the hashtable's internal functions.
 */
static bool
findPartialMatch(TupleHashTable hashtable, TupleTableSlot *slot,
				 FmgrInfo *eqfunctions)
{
	int			numCols = hashtable->numCols;
	AttrNumber *keyColIdx = hashtable->keyColIdx;
	TupleHashIterator hashiter;
	TupleHashEntry entry;

	InitTupleHashIterator(hashtable, &hashiter);
	while ((entry = ScanTupleHashTable(hashtable, &hashiter)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();

		ExecStoreMinimalTuple(entry->firstTuple, hashtable->tableslot, false);
		if (!execTuplesUnequal(slot, hashtable->tableslot,
							   numCols, keyColIdx,
							   eqfunctions,
							   hashtable->tempcxt))
		{
			TermTupleHashIterator(&hashiter);
			return true;
		}
	}
	/* No TermTupleHashIterator call needed here */
	return false;
}

/*
 * slotAllNulls: is the slot completely NULL?
 *
 * This does not test for dropped columns, which is OK because we only
 * use it on projected tuples.
 */
static bool
slotAllNulls(TupleTableSlot *slot)
{
	int			ncols = slot->tts_tupleDescriptor->natts;
	int			i;

	for (i = 1; i <= ncols; i++)
	{
		if (!slot_attisnull(slot, i))
			return false;
	}
	return true;
}

/*
 * slotNoNulls: is the slot entirely not NULL?
 *
 * This does not test for dropped columns, which is OK because we only
 * use it on projected tuples.
 */
static bool
slotNoNulls(TupleTableSlot *slot)
{
	int			ncols = slot->tts_tupleDescriptor->natts;
	int			i;

	for (i = 1; i <= ncols; i++)
	{
		if (slot_attisnull(slot, i))
			return false;
	}
	return true;
}

/* ----------------------------------------------------------------
 *		ExecInitSubPlan
 *
 * Create a SubPlanState for a SubPlan; this is the SubPlan-specific part
 * of ExecInitExpr().  We split it out so that it can be used for InitPlans
 * as well as regular SubPlans.  Note that we don't link the SubPlan into
 * the parent's subPlan list, because that shouldn't happen for InitPlans.
 * Instead, ExecInitExpr() does that one part.
 * ----------------------------------------------------------------
 */
SubPlanState *
ExecInitSubPlan(SubPlan *subplan, PlanState *parent)
{
	SubPlanState *sstate = makeNode(SubPlanState);
	EState	   *estate = parent->state;

	sstate->subplan = subplan;

	/* Link the SubPlanState to already-initialized subplan */
	sstate->planstate = (PlanState *) list_nth(estate->es_subplanstates,
											   subplan->plan_id - 1);

	/* ... and to its parent's state */
	sstate->parent = parent;

	/* Initialize subexpressions */
	sstate->testexpr = ExecInitExpr((Expr *) subplan->testexpr, parent);
	sstate->args = ExecInitExprList(subplan->args, parent);

	/*
	 * initialize my state
	 */
	sstate->curTuple = NULL;
	sstate->curArray = PointerGetDatum(NULL);
	sstate->projLeft = NULL;
	sstate->projRight = NULL;
	sstate->hashtable = NULL;
	sstate->hashnulls = NULL;
	sstate->hashtablecxt = NULL;
	sstate->hashtempcxt = NULL;
	sstate->innerecontext = NULL;
	sstate->keyColIdx = NULL;
	sstate->tab_eq_funcoids = NULL;
	sstate->tab_hash_funcs = NULL;
	sstate->lhs_hash_funcs = NULL;
	sstate->cur_eq_funcs = NULL;

	/*
	 * If this is an initplan or MULTIEXPR subplan, it has output parameters
	 * that the parent plan will use, so mark those parameters as needing
	 * evaluation.  We don't actually run the subplan until we first need one
	 * of its outputs.
	 *
	 * A CTE subplan's output parameter is never to be evaluated in the normal
	 * way, so skip this in that case.
	 *
	 * Note that we don't set parent->chgParam here: the parent plan hasn't
	 * been run yet, so no need to force it to re-run.
	 */
	if (subplan->setParam != NIL && subplan->subLinkType != CTE_SUBLINK)
	{
		ListCell   *lst;

		foreach(lst, subplan->setParam)
		{
			int			paramid = lfirst_int(lst);
			ParamExecData *prm = &(estate->es_param_exec_vals[paramid]);

			prm->execPlan = sstate;
		}
	}

	/*
	 * Initialize memory contexts used by the initplan hash table or the
	 * result cache.
	 */
	if (subplan->useHashTable || subplan->useResultCache)
	{
		/* We need a memory context to hold the hash table(s) */
		sstate->hashtablecxt =
			AllocSetContextCreate(CurrentMemoryContext,
								  "Subplan HashTable Context",
								  ALLOCSET_DEFAULT_SIZES);
		/* and a small one for the hash tables to use as temp storage */
		sstate->hashtempcxt =
			AllocSetContextCreate(CurrentMemoryContext,
								  "Subplan HashTable Temp Context",
								  ALLOCSET_SMALL_SIZES);
	}

	/*
	 * If we are going to hash the subquery output, initialize relevant stuff.
	 * (We don't create the hashtable until needed, though.)
	 */
	if (subplan->useHashTable)
	{
		int			ncols,
					i;
		TupleDesc	tupDescLeft;
		TupleDesc	tupDescRight;
		TupleTableSlot *slot;
		List	   *oplist,
				   *lefttlist,
				   *righttlist;
		ListCell   *l;

		/* a short-lived exprcontext for function evaluation */
		sstate->innerecontext = CreateExprContext(estate);

		/* Silly little array of column numbers 1..n */
		ncols = list_length(subplan->paramIds);
		sstate->keyColIdx = (AttrNumber *) palloc(ncols * sizeof(AttrNumber));
		for (i = 0; i < ncols; i++)
			sstate->keyColIdx[i] = i + 1;

		/*
		 * We use ExecProject to evaluate the lefthand and righthand
		 * expression lists and form tuples.  (You might think that we could
		 * use the sub-select's output tuples directly, but that is not the
		 * case if we had to insert any run-time coercions of the sub-select's
		 * output datatypes; anyway this avoids storing any resjunk columns
		 * that might be in the sub-select's output.)  Run through the
		 * combining expressions to build tlists for the lefthand and
		 * righthand sides.
		 *
		 * We also extract the combining operators themselves to initialize
		 * the equality and hashing functions for the hash tables.
		 */
		if (IsA(subplan->testexpr, OpExpr))
		{
			/* single combining operator */
			oplist = list_make1(subplan->testexpr);
		}
		else if (and_clause((Node *) subplan->testexpr))
		{
			/* multiple combining operators */
			oplist = castNode(BoolExpr, subplan->testexpr)->args;
		}
		else
		{
			/* shouldn't see anything else in a hashable subplan */
			elog(ERROR, "unrecognized testexpr type: %d",
				 (int) nodeTag(subplan->testexpr));
			oplist = NIL;		/* keep compiler quiet */
		}
		Assert(list_length(oplist) == ncols);

		lefttlist = righttlist = NIL;
		sstate->tab_eq_funcoids = (Oid *) palloc(ncols * sizeof(Oid));
		sstate->tab_hash_funcs = (FmgrInfo *) palloc(ncols * sizeof(FmgrInfo));
		sstate->lhs_hash_funcs = (FmgrInfo *) palloc(ncols * sizeof(FmgrInfo));
		sstate->cur_eq_funcs = (FmgrInfo *) palloc(ncols * sizeof(FmgrInfo));
		i = 1;
		foreach(l, oplist)
		{
			OpExpr	   *opexpr = lfirst_node(OpExpr, l);
			Expr	   *expr;
			TargetEntry *tle;
			Oid			left_hashfn;
			Oid			right_hashfn;

			Assert(list_length(opexpr->args) == 2);

			/* Process lefthand argument */
			expr = (Expr *) linitial(opexpr->args);
			tle = makeTargetEntry(expr,
								  i,
								  NULL,
								  false);
			lefttlist = lappend(lefttlist, tle);

			/* Process righthand argument */
			expr = (Expr *) lsecond(opexpr->args);
			tle = makeTargetEntry(expr,
								  i,
								  NULL,
								  false);
			righttlist = lappend(righttlist, tle);

			/* Lookup the equality function (potentially cross-type) */
			sstate->tab_eq_funcoids[i - 1] = opexpr->opfuncid;
			fmgr_info(opexpr->opfuncid, &sstate->cur_eq_funcs[i - 1]);
			fmgr_info_set_expr((Node *) opexpr, &sstate->cur_eq_funcs[i - 1]);

			/* Lookup the associated hash functions */
			if (!get_op_hash_functions(opexpr->opno,
									   &left_hashfn, &right_hashfn))
				elog(ERROR, "could not find hash function for hash operator %u",
					 opexpr->opno);
			fmgr_info(left_hashfn, &sstate->lhs_hash_funcs[i - 1]);
			fmgr_info(right_hashfn, &sstate->tab_hash_funcs[i - 1]);

			i++;
		}

		/*
		 * Construct tupdescs, slots and projection nodes for left and right
		 * sides.  The lefthand expressions will be evaluated in the parent
		 * plan node's exprcontext, which we don't have access to here.
		 * Fortunately we can just pass NULL for now and fill it in later
		 * (hack alert!).  The righthand expressions will be evaluated in our
		 * own innerecontext.
		 */
		tupDescLeft = ExecTypeFromTL(lefttlist, false);
		slot = ExecInitExtraTupleSlot(estate, tupDescLeft);
		sstate->projLeft = ExecBuildProjectionInfo(lefttlist,
												   NULL,
												   slot,
												   parent,
												   NULL);

		sstate->descRight = tupDescRight = ExecTypeFromTL(righttlist, false);
		slot = ExecInitExtraTupleSlot(estate, tupDescRight);
		sstate->projRight = ExecBuildProjectionInfo(righttlist,
													sstate->innerecontext,
													slot,
													sstate->planstate,
													NULL);

		/*
		 * Create comparator for lookups of rows in the table (potentially
		 * across-type comparison).
		 */
		sstate->cur_eq_comp = ExecBuildGroupingEqual(tupDescLeft, tupDescRight,
													 ncols,
													 sstate->keyColIdx,
													 sstate->tab_eq_funcoids,
													 parent);

	}

	/*
	 * If we are going to use the result cache, which is keyed by the
	 * parameters, initialize the equality and hash functions for the
	 * parameter datatypes. (We don't create the cache itself until needed,
	 * though.)
	 */
	if (subplan->useResultCache)
	{
		int			i;

		Assert (!subplan->useHashTable);

		sstate->numparams = list_length(subplan->args);
		sstate->param_hash_funcs =
			(FmgrInfo *) palloc(sstate->numparams * sizeof(FmgrInfo));
		sstate->param_eq_funcs =
			(FmgrInfo *) palloc(sstate->numparams * sizeof(FmgrInfo));
		for (i = 0; i < sstate->numparams; i++)
		{
			fmgr_info(subplan->paramEqFuncs[i], &sstate->param_eq_funcs[i]);
			fmgr_info(subplan->paramHashFuncs[i], &sstate->param_hash_funcs[i]);
		}
	}

	return sstate;
}

/* ----------------------------------------------------------------
 *		ExecSetParamPlan
 *
 *		Executes a subplan and sets its output parameters.
 *
 * This is called from ExecEvalParamExec() when the value of a PARAM_EXEC
 * parameter is requested and the param's execPlan field is set (indicating
 * that the param has not yet been evaluated).  This allows lazy evaluation
 * of initplans: we don't run the subplan until/unless we need its output.
 * Note that this routine MUST clear the execPlan fields of the plan's
 * output parameters after evaluating them!
 * ----------------------------------------------------------------
 */
void
ExecSetParamPlan(SubPlanState *node, ExprContext *econtext)
{
	SubPlan    *subplan = node->subplan;
	PlanState  *planstate = node->planstate;
	SubLinkType subLinkType = subplan->subLinkType;
	MemoryContext oldcontext;
	TupleTableSlot *slot;
	ListCell   *pvar;
	ListCell   *l;
	bool		found = false;
	ArrayBuildStateAny *astate = NULL;

	if (subLinkType == ANY_SUBLINK ||
		subLinkType == ALL_SUBLINK)
		elog(ERROR, "ANY/ALL subselect unsupported as initplan");
	if (subLinkType == CTE_SUBLINK)
		elog(ERROR, "CTE subplans should not be executed via ExecSetParamPlan");

	/* Initialize ArrayBuildStateAny in caller's context, if needed */
	if (subLinkType == ARRAY_SUBLINK)
		astate = initArrayResultAny(subplan->firstColType,
									CurrentMemoryContext, true);

	/*
	 * Must switch to per-query memory context.
	 */
	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);

	/*
	 * Set Params of this plan from parent plan correlation values. (Any
	 * calculation we have to do is done in the parent econtext, since the
	 * Param values don't need to have per-query lifetime.)  Currently, we
	 * expect only MULTIEXPR_SUBLINK plans to have any correlation values.
	 */
	Assert(subplan->parParam == NIL || subLinkType == MULTIEXPR_SUBLINK);
	Assert(list_length(subplan->parParam) == list_length(node->args));

	forboth(l, subplan->parParam, pvar, node->args)
	{
		int			paramid = lfirst_int(l);
		ParamExecData *prm = &(econtext->ecxt_param_exec_vals[paramid]);

		prm->value = ExecEvalExprSwitchContext((ExprState *) lfirst(pvar),
											   econtext,
											   &(prm->isnull));
		planstate->chgParam = bms_add_member(planstate->chgParam, paramid);
	}

	/*
	 * Run the plan.  (If it needs to be rescanned, the first ExecProcNode
	 * call will take care of that.)
	 */
	for (slot = ExecProcNode(planstate);
		 !TupIsNull(slot);
		 slot = ExecProcNode(planstate))
	{
		TupleDesc	tdesc = slot->tts_tupleDescriptor;
		int			i = 1;

		if (subLinkType == EXISTS_SUBLINK)
		{
			/* There can be only one setParam... */
			int			paramid = linitial_int(subplan->setParam);
			ParamExecData *prm = &(econtext->ecxt_param_exec_vals[paramid]);

			prm->execPlan = NULL;
			prm->value = BoolGetDatum(true);
			prm->isnull = false;
			found = true;
			break;
		}

		if (subLinkType == ARRAY_SUBLINK)
		{
			Datum		dvalue;
			bool		disnull;

			found = true;
			/* stash away current value */
			Assert(subplan->firstColType == TupleDescAttr(tdesc, 0)->atttypid);
			dvalue = slot_getattr(slot, 1, &disnull);
			astate = accumArrayResultAny(astate, dvalue, disnull,
										 subplan->firstColType, oldcontext);
			/* keep scanning subplan to collect all values */
			continue;
		}

		if (found &&
			(subLinkType == EXPR_SUBLINK ||
			 subLinkType == MULTIEXPR_SUBLINK ||
			 subLinkType == ROWCOMPARE_SUBLINK))
			ereport(ERROR,
					(errcode(ERRCODE_CARDINALITY_VIOLATION),
					 errmsg("more than one row returned by a subquery used as an expression")));

		found = true;

		/*
		 * We need to copy the subplan's tuple into our own context, in case
		 * any of the params are pass-by-ref type --- the pointers stored in
		 * the param structs will point at this copied tuple! node->curTuple
		 * keeps track of the copied tuple for eventual freeing.
		 */
		if (node->curTuple)
			heap_freetuple(node->curTuple);
		node->curTuple = ExecCopySlotTuple(slot);

		/*
		 * Now set all the setParam params from the columns of the tuple
		 */
		foreach(l, subplan->setParam)
		{
			int			paramid = lfirst_int(l);
			ParamExecData *prm = &(econtext->ecxt_param_exec_vals[paramid]);

			prm->execPlan = NULL;
			prm->value = heap_getattr(node->curTuple, i, tdesc,
									  &(prm->isnull));
			i++;
		}
	}

	if (subLinkType == ARRAY_SUBLINK)
	{
		/* There can be only one setParam... */
		int			paramid = linitial_int(subplan->setParam);
		ParamExecData *prm = &(econtext->ecxt_param_exec_vals[paramid]);

		/*
		 * We build the result array in query context so it won't disappear;
		 * to avoid leaking memory across repeated calls, we have to remember
		 * the latest value, much as for curTuple above.
		 */
		if (node->curArray != PointerGetDatum(NULL))
			pfree(DatumGetPointer(node->curArray));
		node->curArray = makeArrayResultAny(astate,
											econtext->ecxt_per_query_memory,
											true);
		prm->execPlan = NULL;
		prm->value = node->curArray;
		prm->isnull = false;
	}
	else if (!found)
	{
		if (subLinkType == EXISTS_SUBLINK)
		{
			/* There can be only one setParam... */
			int			paramid = linitial_int(subplan->setParam);
			ParamExecData *prm = &(econtext->ecxt_param_exec_vals[paramid]);

			prm->execPlan = NULL;
			prm->value = BoolGetDatum(false);
			prm->isnull = false;
		}
		else
		{
			/* For other sublink types, set all the output params to NULL */
			foreach(l, subplan->setParam)
			{
				int			paramid = lfirst_int(l);
				ParamExecData *prm = &(econtext->ecxt_param_exec_vals[paramid]);

				prm->execPlan = NULL;
				prm->value = (Datum) 0;
				prm->isnull = true;
			}
		}
	}

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Mark an initplan as needing recalculation
 */
void
ExecReScanSetParamPlan(SubPlanState *node, PlanState *parent)
{
	PlanState  *planstate = node->planstate;
	SubPlan    *subplan = node->subplan;
	EState	   *estate = parent->state;
	ListCell   *l;

	/* sanity checks */
	if (subplan->parParam != NIL)
		elog(ERROR, "direct correlated subquery unsupported as initplan");
	if (subplan->setParam == NIL)
		elog(ERROR, "setParam list of initplan is empty");
	if (bms_is_empty(planstate->plan->extParam))
		elog(ERROR, "extParam set of initplan is empty");

	/*
	 * Don't actually re-scan: it'll happen inside ExecSetParamPlan if needed.
	 */

	/*
	 * Mark this subplan's output parameters as needing recalculation.
	 *
	 * CTE subplans are never executed via parameter recalculation; instead
	 * they get run when called by nodeCtescan.c.  So don't mark the output
	 * parameter of a CTE subplan as dirty, but do set the chgParam bit for it
	 * so that dependent plan nodes will get told to rescan.
	 */
	foreach(l, subplan->setParam)
	{
		int			paramid = lfirst_int(l);
		ParamExecData *prm = &(estate->es_param_exec_vals[paramid]);

		if (subplan->subLinkType != CTE_SUBLINK)
			prm->execPlan = node;

		parent->chgParam = bms_add_member(parent->chgParam, paramid);
	}
}


/*
 * ExecInitAlternativeSubPlan
 *
 * Initialize for execution of one of a set of alternative subplans.
 */
AlternativeSubPlanState *
ExecInitAlternativeSubPlan(AlternativeSubPlan *asplan, PlanState *parent)
{
	AlternativeSubPlanState *asstate = makeNode(AlternativeSubPlanState);
	double		num_calls;
	SubPlan    *subplan1;
	SubPlan    *subplan2;
	Cost		cost1;
	Cost		cost2;
	ListCell   *lc;

	asstate->subplan = asplan;

	/*
	 * Initialize subplans.  (Can we get away with only initializing the one
	 * we're going to use?)
	 */
	foreach(lc, asplan->subplans)
	{
		SubPlan    *sp = lfirst_node(SubPlan, lc);
		SubPlanState *sps = ExecInitSubPlan(sp, parent);

		asstate->subplans = lappend(asstate->subplans, sps);
		parent->subPlan = lappend(parent->subPlan, sps);
	}

	/*
	 * Select the one to be used.  For this, we need an estimate of the number
	 * of executions of the subplan.  We use the number of output rows
	 * expected from the parent plan node.  This is a good estimate if we are
	 * in the parent's targetlist, and an underestimate (but probably not by
	 * more than a factor of 2) if we are in the qual.
	 */
	num_calls = parent->plan->plan_rows;

	/*
	 * The planner saved enough info so that we don't have to work very hard
	 * to estimate the total cost, given the number-of-calls estimate.
	 */
	Assert(list_length(asplan->subplans) == 2);
	subplan1 = (SubPlan *) linitial(asplan->subplans);
	subplan2 = (SubPlan *) lsecond(asplan->subplans);

	cost1 = subplan1->startup_cost + num_calls * subplan1->per_call_cost;
	cost2 = subplan2->startup_cost + num_calls * subplan2->per_call_cost;

	if (cost1 < cost2)
		asstate->active = 0;
	else
		asstate->active = 1;

	return asstate;
}

/*
 * ExecAlternativeSubPlan
 *
 * Execute one of a set of alternative subplans.
 *
 * Note: in future we might consider changing to different subplans on the
 * fly, in case the original rowcount estimate turns out to be way off.
 */
Datum
ExecAlternativeSubPlan(AlternativeSubPlanState *node,
					   ExprContext *econtext,
					   bool *isNull)
{
	/* Just pass control to the active subplan */
	SubPlanState *activesp = list_nth_node(SubPlanState,
										   node->subplans, node->active);

	return ExecSubPlan(activesp, econtext, isNull);
}



/*-------------------
 * SubPlan result cache
 *-------------------
 */

static void
init_result_cache(SubPlanState *sstate)
{
	HASHCTL		hash_ctl;
	Size		entrysize = SUBPLAN_RESULT_CACHE_ENTRY_SIZE(sstate->numparams);
	long		nbuckets;
	int			i;
	ListCell   *l;
	MemoryContext oldcxt;

	nbuckets = 100; /* TODO? */

	/* Limit initial table size request to not more than work_mem */
	nbuckets = Min(nbuckets, (long) ((work_mem * 1024L) / entrysize));

	/* TODO: should we use a hash_iv like execGrouping.c does? */

	oldcxt = MemoryContextSwitchTo(sstate->hashtablecxt);

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = SUBPLAN_RESULT_CACHE_KEY_SIZE(sstate->numparams);
	hash_ctl.entrysize = SUBPLAN_RESULT_CACHE_ENTRY_SIZE(sstate->numparams);
	hash_ctl.hash = result_cache_hash;
	hash_ctl.match = result_cache_match;
	hash_ctl.keycopy = result_cache_keycopy;
	hash_ctl.hcxt = sstate->hashtablecxt;

	sstate->resultcachetab = hash_create("subplan result cache",
										 100 /* XXX */,
										 &hash_ctl,
										 HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_KEYCOPY | HASH_CONTEXT);

	sstate->paramLens = palloc(sstate->numparams * sizeof(int16));
	sstate->paramByVals = palloc(sstate->numparams * sizeof(bool));

	sstate->memUsed = 0;
	sstate->memLimit = work_mem * 1024L;

	i = 0;
	foreach(l, sstate->subplan->args)
	{
		Expr	   *arg = (Expr *) lfirst(l);

		get_typlenbyval(exprType((Node *) arg),
						&sstate->paramLens[i],
						&sstate->paramByVals[i]);
		i++;
	}

	/*
	 * Also initialize this temporary cache key, to hold the parameter
	 * values, while we look up in the cache.
	 */
	sstate->currCacheKey = (void *) palloc0(SUBPLAN_RESULT_CACHE_ENTRY_SIZE(sstate->numparams));

	/*
	 * For all sublink types except EXPR_SUBLINK and ARRAY_SUBLINK, the result is
	 * a boolean as are the results fo the combining operators.  The cache will store
	 * the final result, after evaluating any combining operators.
	 */
	if (sstate->subplan->subLinkType == EXPR_SUBLINK)
	{
		get_typlenbyval(sstate->subplan->firstColType,
						&sstate->resultLen,
						&sstate->resultByVal);
	}
	else if (sstate->subplan->subLinkType == ARRAY_SUBLINK)
	{
		/* array type. They're all varlenas */
		sstate->resultLen = -1;
		sstate->resultByVal = false;
	}
	else
	{
		/* boolean */
		sstate->resultLen = 1;
		sstate->resultByVal = true;
	}

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Insert or return existing entry in the cache.
 */
static SubPlanResultCacheEntry *
lookup_result_cache(SubPlanState *sstate, SubPlanResultCacheEntry *key, bool *found)
{
	MemoryContext oldContext;
	SubPlanResultCacheEntry *newentry;
	int			i;
	SubPlanState *prev_sstate;
	Datum	   *params = key->params;
	bool	   *isnulls = SUBPLAN_RESULT_CACHE_ENTRY_GET_NULLS(key, sstate->numparams);

	/* Need to run the hash functions in short-lived context */
	oldContext = MemoryContextSwitchTo(sstate->hashtempcxt);

	/*
	 * To keep memory use in check, if there are too many entries in the
	 * cache, remove the oldest entry to make space.
	 */
	while (sstate->memUsed > sstate->memLimit)
	{
		SubPlanResultCacheEntry *oldentry;
		bool		foundold;
		Datum	   *oldparams;
		bool	   *oldparamnulls;

		/* Remove the oldest entry */
		oldentry = dlist_tail_element(SubPlanResultCacheEntry, lru_link, &sstate->lru_list);

		dlist_delete(&oldentry->lru_link);

		if (!sstate->resultByVal && !oldentry->resultNull)
		{
			sstate->memUsed -= GetMemoryChunkSpace(DatumGetPointer(oldentry->result));
			pfree(DatumGetPointer(oldentry->result));
		}

		/* Remove the entry from the hash table. */
		prev_sstate = curr_sstate;
		curr_sstate = sstate;
		hash_search(sstate->resultcachetab, oldentry, HASH_REMOVE, &foundold);
		if (!foundold)
			elog(ERROR, "subplan cache is corrupt");
		curr_sstate = prev_sstate;

		sstate->memUsed -= SUBPLAN_RESULT_CACHE_ENTRY_SIZE(sstate->numparams);

		/*
		 * XXX we assume the key is still valid, even though we've removed it from
		 * the hash table.
		 */
		oldparams = oldentry->params;
		oldparamnulls = SUBPLAN_RESULT_CACHE_ENTRY_GET_NULLS(oldentry, sstate->numparams);
		for (i = 0; i < sstate->numparams; i++)
		{
			if (!sstate->paramByVals[i] && !oldparamnulls[i])
			{
				sstate->memUsed -= GetMemoryChunkSpace(DatumGetPointer(oldparams[i]));
				pfree(DatumGetPointer(oldparams[i]));
			}
		}
	}

	prev_sstate = curr_sstate;
	curr_sstate = sstate;
	newentry = hash_search(sstate->resultcachetab, key, HASH_ENTER, found);
	curr_sstate = prev_sstate;

	if (!(*found))
	{
		/*
		 * We created a new entry.
		 */
		Datum	   *newparams = newentry->params;

		sstate->memUsed += SUBPLAN_RESULT_CACHE_ENTRY_SIZE(sstate->numparams);

		dlist_push_head(&sstate->lru_list, &newentry->lru_link);

		/*
		 * Need to make a copy of the key in the long-lived memory context.
		 */
		MemoryContextSwitchTo(sstate->hashtablecxt);

		for (i = 0; i < sstate->numparams; i++)
		{
			if (!sstate->paramByVals[i] && !isnulls[i])
			{
				newparams[i] = datumCopy(params[i],
										 sstate->paramByVals[i],
										 sstate->paramLens[i]);
				sstate->memUsed += GetMemoryChunkSpace(DatumGetPointer(newparams[i]));
			}
		}
	}
	else
	{
		dlist_delete(&newentry->lru_link);
		dlist_push_head(&sstate->lru_list, &newentry->lru_link);
	}

	MemoryContextSwitchTo(oldContext);

	return newentry;
}

static uint32
result_cache_hash(const void *key, Size keysize)
{
	const SubPlanResultCacheEntry *entry = (const SubPlanResultCacheEntry *) key;
	SubPlanState *sstate = curr_sstate;
	int			i;
	uint32		hash = 0;
	const Datum *params = entry->params;
	const bool  *isnulls = SUBPLAN_RESULT_CACHE_ENTRY_GET_NULLS(entry, sstate->numparams);

	for (i = 0; i < sstate->numparams; i++)
	{
		uint32		thishash;

		if (isnulls[i])
			thishash = 0;
		else
			thishash = DatumGetUInt32(FunctionCall1(&sstate->param_hash_funcs[i],
													params[i]));
		hash = hash_combine(hash, thishash);
	}

	return hash;
}

static int
result_cache_match(const void *key1, const void *key2, Size keysize)
{
	const SubPlanResultCacheEntry *entry1 = (const SubPlanResultCacheEntry *) key1;
	const SubPlanResultCacheEntry *entry2 = (const SubPlanResultCacheEntry *) key2;
	SubPlanState *sstate = curr_sstate;
	const Datum *params1 = entry1->params;
	const bool *isnulls1 = SUBPLAN_RESULT_CACHE_ENTRY_GET_NULLS(entry1, sstate->numparams);
	const Datum *params2 = entry2->params;
	const bool *isnulls2 = SUBPLAN_RESULT_CACHE_ENTRY_GET_NULLS(entry2, sstate->numparams);
	int			i;

	for (i = 0; i < sstate->numparams; i++)
	{
		if (isnulls1[i] != isnulls2[i])
			return 1;
		else if (!isnulls1[i])
		{
			if (!DatumGetBool(FunctionCall2(&sstate->param_eq_funcs[0],
											params1[i],
											params2[i])))
				return 1;
		}
	}
	return 0;	/* they're equal */
}

static void *
result_cache_keycopy(void *dest, const void *src, Size keysize)
{
	SubPlanResultCacheEntry *destentry = (SubPlanResultCacheEntry *) dest;
	const SubPlanResultCacheEntry *srcentry = (const SubPlanResultCacheEntry *) src;

	memcpy(destentry->params, srcentry->params, keysize);

	return destentry;
}
