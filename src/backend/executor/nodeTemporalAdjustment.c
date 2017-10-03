#include "postgres.h"
#include "executor/executor.h"
#include "executor/nodeTemporalAdjustment.h"
#include "utils/memutils.h"
#include "access/htup_details.h"				/* for heap_getattr */
#include "utils/lsyscache.h"
#include "nodes/print.h"						/* for print_slot */
#include "utils/datum.h"						/* for datumCopy */
#include "utils/rangetypes.h"

/*
 * #define TEMPORAL_DEBUG
 * XXX PEMOSER Maybe we should use execdebug.h stuff here?
 */
#ifdef TEMPORAL_DEBUG
static char*
datumToString(Oid typeinfo, Datum attr)
{
	Oid			typoutput;
	bool		typisvarlena;
	getTypeOutputInfo(typeinfo, &typoutput, &typisvarlena);
	return OidOutputFunctionCall(typoutput, attr);
}

#define TPGdebug(...) 					{ printf(__VA_ARGS__); printf("\n"); fflush(stdout); }
#define TPGdebugDatum(attr, typeinfo) 	TPGdebug("%s = %s %ld\n", #attr, datumToString(typeinfo, attr), attr)
#define TPGdebugSlot(slot) 				{ printf("Printing Slot '%s'\n", #slot); print_slot(slot); fflush(stdout); }

#else
#define datumToString(typeinfo, attr)
#define TPGdebug(...)
#define TPGdebugDatum(attr, typeinfo)
#define TPGdebugSlot(slot)
#endif

/*
 * isLessThan
 *		We must check if the sweepline is before a timepoint, or if a timepoint
 *		is smaller than another. We initialize the function call info during
 *		ExecInit phase.
 */
static bool
isLessThan(Datum a, Datum b, TemporalAdjustmentState* node)
{
	node->ltFuncCallInfo.arg[0] = a;
	node->ltFuncCallInfo.arg[1] = b;
	node->ltFuncCallInfo.argnull[0] = false;
	node->ltFuncCallInfo.argnull[1] = false;

	/* Return value is never null, due to the pre-defined sub-query output */
	return DatumGetBool(FunctionCallInvoke(&node->ltFuncCallInfo));
}

/*
 * isEqual
 *		We must check if two timepoints are equal. We initialize the function
 *		call info during ExecInit phase.
 */
static bool
isEqual(Datum a, Datum b, TemporalAdjustmentState* node)
{
	node->eqFuncCallInfo.arg[0] = a;
	node->eqFuncCallInfo.arg[1] = b;
	node->eqFuncCallInfo.argnull[0] = false;
	node->eqFuncCallInfo.argnull[1] = false;

	/* Return value is never null, due to the pre-defined sub-query output */
	return DatumGetBool(FunctionCallInvoke(&node->eqFuncCallInfo));
}

/*
 * makeRange
 *		We split range types into two scalar boundary values (i.e., upper and
 *		lower bound). Due to this splitting, we can keep a single version of
 *		the algorithm with for two separate boundaries. However, we must combine
 *		these two scalars at the end to return the same datatypes as we got for
 *		the input. The drawback of this approach is that we loose boundary types
 *		here, i.e., we do not know if a bound was inclusive or exclusive. We
 *		initialize the function call info during ExecInit phase.
 */
static Datum
makeRange(Datum l, Datum u, TemporalAdjustmentState* node)
{
	node->rcFuncCallInfo.arg[0] = l;
	node->rcFuncCallInfo.arg[1] = u;
	node->rcFuncCallInfo.argnull[0] = false;
	node->rcFuncCallInfo.argnull[1] = false;

	/* Return value is never null, due to the pre-defined sub-query output */
	return FunctionCallInvoke(&node->rcFuncCallInfo);
}

static Datum
getLower(Datum range, TemporalAdjustmentState* node)
{
	node->loFuncCallInfo.arg[0] = range;
	node->loFuncCallInfo.argnull[0] = false;

	/* Return value is never null, due to the pre-defined sub-query output */
	return FunctionCallInvoke(&node->loFuncCallInfo);
}

static Datum
getUpper(Datum range, TemporalAdjustmentState* node)
{
	node->upFuncCallInfo.arg[0] = range;
	node->upFuncCallInfo.argnull[0] = false;

	/* Return value is never null, due to the pre-defined sub-query output */
	return FunctionCallInvoke(&node->upFuncCallInfo);
}

/*
 * temporalAdjustmentStoreTuple
 *      While we store result tuples, we must add the newly calculated temporal
 *      boundaries as two scalar fields or create a single range-typed field
 *      with the two given boundaries.
 */
static void
temporalAdjustmentStoreTuple(TemporalAdjustmentState* node,
							 TupleTableSlot* slotToModify,
							 TupleTableSlot* slotToStoreIn,
							 Datum newTs,
							 Datum newTe)
{
	MemoryContext oldContext;
	HeapTuple t;

	node->newValues[node->temporalCl->attNumTr - 1] =
					makeRange(newTs, newTe, node);

	oldContext = MemoryContextSwitchTo(node->ss.ps.ps_ResultTupleSlot->tts_mcxt);
	t = heap_modify_tuple(slotToModify->tts_tuple,
						  slotToModify->tts_tupleDescriptor,
						  node->newValues,
						  node->nullMask,
						  node->tsteMask);
	MemoryContextSwitchTo(oldContext);
	slotToStoreIn = ExecStoreTuple(t, slotToStoreIn, InvalidBuffer, true);

	TPGdebug("Storing tuple:");
	TPGdebugSlot(slotToStoreIn);
}

/*
 * slotGetAttrNotNull
 *      Same as slot_getattr, but throws an error if NULL is returned.
 */
static Datum
slotGetAttrNotNull(TupleTableSlot *slot, int attnum)
{
	bool isNull;
	Datum result;

	result = slot_getattr(slot, attnum, &isNull);

	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_NOT_NULL_VIOLATION),
				 errmsg("Attribute \"%s\" at position %d is null. Temporal " \
						"adjustment not possible.",
				 NameStr(TupleDescAttr(slot->tts_tupleDescriptor, attnum - 1)->attname),
				 attnum)));

	return result;
}

/*
 * heapGetAttrNotNull
 *      Same as heap_getattr, but throws an error if NULL is returned.
 */
static Datum
heapGetAttrNotNull(TupleTableSlot *slot, int attnum)
{
	bool isNull;
	Datum result;

	result = heap_getattr(slot->tts_tuple,
						  attnum,
						  slot->tts_tupleDescriptor,
						  &isNull);
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_NOT_NULL_VIOLATION),
				 errmsg("Attribute \"%s\" at position %d is null. Temporal " \
						"adjustment not possible.",
				 NameStr(TupleDescAttr(slot->tts_tupleDescriptor, attnum - 1)->attname),
				 attnum)));

	return result;
}

#define setSweepline(datum) \
	node->sweepline = datumCopy(datum, node->datumFormat->attbyval, node->datumFormat->attlen)

#define freeSweepline() \
	if (! node->datumFormat->attbyval) pfree(DatumGetPointer(node->sweepline))

/*
 * ExecTemporalAdjustment
 *
 * At this point we get an input, which is splitted into so-called temporal
 * groups. Each of these groups satisfy the theta-condition (see below), has
 * overlapping periods, and a row number as ID. The input is ordered by temporal
 * group ID, and the start and ending timepoints, i.e., P1 and P2. Temporal
 * normalizers do not make a distinction between start and end timepoints while
 * grouping, therefore we have only one timepoint attribute there (i.e., P1),
 * which is the union of start and end timepoints.
 *
 * This executor function implements both temporal primitives, namely temporal
 * aligner and temporal normalizer. We keep a sweep line which starts from
 * the lowest start point, and proceeds to the right. Please note, that
 * both algorithms need a different input to work.
 *
 * (1) TEMPORAL ALIGNER
 *     Temporal aligners are used to build temporal joins. The general idea of
 *     alignment is to split each tuple of its right argument r with respect to
 *     each tuple in the group of tuples in the left argument s that satisfies
 *     theta, and has overlapping timestamp intervals.
 *
 * 	Example:
 * 	  ... FROM (r ALIGN s ON theta WITH (r.t, s.t)) x
 *
 * 	Input: x(r_1, ..., r_n, RN, P1, P2)
 * 	  where r_1,...,r_n are all attributes from relation r. One of these
 * 	  attributes is a range-typed valid time attribute, namely T. The interval
 * 	  T = [TStart,TEnd) represents the VALID TIME of each tuple. RN is the
 * 	  temporal group ID or row number, P1 is the greatest starting
 * 	  timepoint, and P2 is the least ending timepoint of corresponding
 * 	  temporal attributes of the relations r and s. The interval [P1,P2)
 * 	  holds the already computed intersection between r- and s-tuples.
 *
 * (2) TEMPORAL NORMALIZER
 * 	   Temporal normalizers are used to build temporal set operations,
 * 	   temporal aggregations, and temporal projections (i.e., DISTINCT).
 * 	   The general idea of normalization is to split each tuple in r with
 * 	   respect to the group of tuples in s that match on the grouping
 * 	   attributes in B (i.e., the USING clause, which can also be empty, or
 * 	   contain more than one attribute). In addition, also non-equality
 * 	   comparisons can be made by substituting USING with "ON theta".
 *
 * 	Example:
 * 	  ... FROM (r NORMALIZE s USING(B) WITH (r.t, s.t)) x
 * 	  or
 * 	  ... FROM (r NORMALIZE s ON theta WITH (r.t, s.t)) x
 *
 * 	Input: x(r_1, ..., r_n, RN, P1)
 * 	  where r_1,...,r_n are all attributes from relation r. One of these
 * 	  attributes is a range-typed valid time attribute, namely T. The interval
 * 	  T = [TStart,TEnd) represents the VALID TIME of each tuple. RN is the
 * 	  temporal group ID or row number, and P1 is union of both
 * 	  timepoints TStart and TEnd of relation s.
 */
TupleTableSlot *
ExecTemporalAdjustment(TemporalAdjustmentState *node)
{
	PlanState  			*outerPlan 	= outerPlanState(node);
	TupleTableSlot 		*out		= node->ss.ps.ps_ResultTupleSlot;
	TupleTableSlot 		*curr		= outerPlan->ps_ResultTupleSlot;
	TupleTableSlot 		*prev 		= node->ss.ss_ScanTupleSlot;
	TemporalClause		*tc 		= node->temporalCl;
	bool 				 produced;
	bool 				 isNull;
	Datum 				 currP1;	/* Current tuple's P1 */
	Datum 				 currP2;	/* Current tuple's P2 (ALIGN only) */
	Datum				 currRN;	/* Current tuple's row number */
	Datum				 prevRN;	/* Previous tuple's row number */
	Datum				 prevTe;	/* Previous tuple's time end point*/

	if(node->firstCall)
	{
		curr = ExecProcNode(outerPlan);
		if(TupIsNull(curr))
			return NULL;

		prev = ExecCopySlot(prev, curr);
		node->sameleft = true;
		node->firstCall = false;
		node->outrn = 0;

		/*
		 * P1 is made of the lower or upper bounds of the valid time column,
		 * hence it must have the same type as the range (return element type)
		 * of lower(T) or upper(T).
		 */
		node->datumFormat = TupleDescAttr(curr->tts_tupleDescriptor, tc->attNumP1 - 1);
		setSweepline(getLower(slotGetAttrNotNull(curr, tc->attNumTr), node));
	}

	TPGdebugSlot(curr);
	TPGdebugDatum(node->sweepline, node->datumFormat->atttypid);
	TPGdebug("node->sameleft = %d", node->sameleft);

	produced = false;
	while(!produced && !TupIsNull(prev))
	{
		if(node->sameleft)
		{
			currRN = slotGetAttrNotNull(curr, tc->attNumRN);

			/*
			 * The right-hand-side of the LEFT OUTER JOIN can produce
			 * null-values, however we must produce a result tuple anyway with
			 * the attributes of the left-hand-side, if this happens.
			 */
			currP1 = slot_getattr(curr,  tc->attNumP1, &isNull);
			if (isNull)
				node->sameleft = false;

			if(!isNull && isLessThan(node->sweepline, currP1, node))
			{
				temporalAdjustmentStoreTuple(node, curr, out,
								node->sweepline, currP1);
				produced = true;
				freeSweepline();
				setSweepline(currP1);
				node->outrn = DatumGetInt64(currRN);
			}
			else
			{
				/*
				 * Temporal aligner: currP1/2 can never be NULL, therefore we
				 * never enter this block. We do not have to check for currP1/2
				 * equal NULL.
				 */
				if(node->alignment)
				{
					/* We fetched currP1 and currRN already */
					currP2 = slotGetAttrNotNull(curr, tc->attNumP2);

					/* If alignment check to not produce the same tuple again */
					if(TupIsNull(out)
						|| !isEqual(getLower(heapGetAttrNotNull(out, tc->attNumTr),
											 node),
									currP1, node)
						|| !isEqual(getUpper(heapGetAttrNotNull(out, tc->attNumTr),
											 node),
									currP2, node)
						|| node->outrn != DatumGetInt64(currRN))
					{
						temporalAdjustmentStoreTuple(node, curr, out,
													 currP1, currP2);

						/* sweepline = max(sweepline, curr.P2) */
						if (isLessThan(node->sweepline, currP2, node))
						{
							freeSweepline();
							setSweepline(currP2);
						}

						node->outrn = DatumGetInt64(currRN);
						produced = true;
					}
				}

				prev = ExecCopySlot(prev, curr);
				curr = ExecProcNode(outerPlan);

				if(TupIsNull(curr))
					node->sameleft = false;
				else
				{
					currRN = slotGetAttrNotNull(curr, tc->attNumRN);
					prevRN = slotGetAttrNotNull(prev, tc->attNumRN);
					node->sameleft =
							DatumGetInt64(currRN) == DatumGetInt64(prevRN);
				}
			}
		}
		else
		{
			prevTe = getUpper(heapGetAttrNotNull(prev, tc->attNumTr), node);

			if(isLessThan(node->sweepline, prevTe, node))
			{
				temporalAdjustmentStoreTuple(node, prev, out,
								node->sweepline, prevTe);

				/*
				 * We fetch the row number from the previous tuple slot,
				 * since it is possible that the current one is NULL, if we
				 * arrive here from sameleft = false set when curr = NULL.
				 */
				currRN = heapGetAttrNotNull(prev, tc->attNumRN);
				node->outrn = DatumGetInt64(currRN);
				produced = true;
			}

			if(TupIsNull(curr))
				prev = ExecClearTuple(prev);
			else
			{
				prev = ExecCopySlot(prev, curr);
				freeSweepline();
				setSweepline(getLower(slotGetAttrNotNull(curr, tc->attNumTr),
									  node));
			}
			node->sameleft = true;
		}
	}

	if(!produced) {
		ExecClearTuple(out);
		return NULL;
	}

	return out;
}

/*
 * ExecInitTemporalAdjustment
 * 		Initializes the tuple memory context, outer plan node, and function call
 * 		infos for makeRange, lessThan, and isEqual including collation types.
 * 		A range constructor function is only initialized if temporal boundaries
 * 		are given as range types.
 */
TemporalAdjustmentState *
ExecInitTemporalAdjustment(TemporalAdjustment *node, EState *estate, int eflags)
{
	TemporalAdjustmentState *state;
	FmgrInfo		 		*eqFunctionInfo = palloc(sizeof(FmgrInfo));
	FmgrInfo		 		*ltFunctionInfo = palloc(sizeof(FmgrInfo));
	FmgrInfo		 		*rcFunctionInfo = palloc(sizeof(FmgrInfo));
	FmgrInfo		 		*loFunctionInfo = palloc(sizeof(FmgrInfo));
	FmgrInfo		 		*upFunctionInfo = palloc(sizeof(FmgrInfo));

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	state = makeNode(TemporalAdjustmentState);
	state->ss.ps.plan = (Plan *) node;
	state->ss.ps.state = estate;
	state->ss.ps.ExecProcNode = (ExecProcNodeMtd) ExecTemporalAdjustment;

	/*
	 * Miscellaneous initialization
	 *
	 * Temporal Adjustment nodes have no ExprContext initialization because
	 * they never call ExecQual or ExecProject.  But they do need a per-tuple
	 * memory context anyway for calling execTuplesMatch.
	 * XXX PEMOSER Do we need this really?
	 */
	state->tempContext =
		AllocSetContextCreate(CurrentMemoryContext,
							  "TemporalAdjustment",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);

	/*
	 * Tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &state->ss.ps);
	ExecInitScanTupleSlot(estate, &state->ss);

	/*
	 * then initialize outer plan
	 */
	outerPlanState(state) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	* initialize source tuple type.
	*/
	ExecAssignScanTypeFromOuterPlan(&state->ss);

	/*
	 * Temporal adjustment nodes do no projections, so initialize projection
	 * info for this node appropriately
	 */
	ExecAssignResultTypeFromTL(&state->ss.ps);
	state->ss.ps.ps_ProjInfo = NULL;

	state->alignment = node->temporalCl->temporalType == TEMPORAL_TYPE_ALIGNER;
	state->temporalCl = copyObject(node->temporalCl);
	state->firstCall = true;
	state->sweepline = (Datum) 0;

	/*
	 * Init masks
	 */
	state->nullMask = palloc0(sizeof(bool) * node->numCols);
	state->tsteMask = palloc0(sizeof(bool) * node->numCols);
	state->tsteMask[state->temporalCl->attNumTr - 1] = true;

	/*
	 * Init buffer values for heap_modify_tuple
	 */
	state->newValues = palloc0(sizeof(Datum) * node->numCols);

	/* the parser should have made sure of this */
	Assert(OidIsValid(node->ltOperatorID));
	Assert(OidIsValid(node->eqOperatorID));

	/*
	 * Precompute fmgr lookup data for inner loop. We use "less than", "equal",
	 * and "range_constructor2" operators on columns with indexes "tspos",
	 * "tepos", and "trpos" respectively. To construct a range type we also
	 * assign the original range information from the targetlist entry which
	 * holds the range type from the input to the function call info expression.
	 * This expression is then used to determine the correct type and collation.
	 */
	fmgr_info(get_opcode(node->eqOperatorID), eqFunctionInfo);
	fmgr_info(get_opcode(node->ltOperatorID), ltFunctionInfo);

	InitFunctionCallInfoData(state->eqFuncCallInfo, eqFunctionInfo, 2,
							 node->sortCollationID, NULL, NULL);
	InitFunctionCallInfoData(state->ltFuncCallInfo, ltFunctionInfo, 2,
							 node->sortCollationID, NULL, NULL);

	/*
	 * Prepare function manager information to extract lower and upper bounds
	 * of range types, and to call the range constructor method to build a new
	 * range type out of two separate boundaries.
	 */
	fmgr_info(fmgr_internal_function("range_constructor2"), rcFunctionInfo);
	rcFunctionInfo->fn_expr = (fmNodePtr) node->rangeVar;
	InitFunctionCallInfoData(state->rcFuncCallInfo, rcFunctionInfo, 2,
							 node->rangeVar->varcollid, NULL, NULL);

	fmgr_info(fmgr_internal_function("range_lower"), loFunctionInfo);
	loFunctionInfo->fn_expr = (fmNodePtr) node->rangeVar;
	InitFunctionCallInfoData(state->loFuncCallInfo, loFunctionInfo, 1,
							 node->rangeVar->varcollid, NULL, NULL);

	fmgr_info(fmgr_internal_function("range_upper"), upFunctionInfo);
	upFunctionInfo->fn_expr = (fmNodePtr) node->rangeVar;
	InitFunctionCallInfoData(state->upFuncCallInfo, upFunctionInfo, 1,
							 node->rangeVar->varcollid, NULL, NULL);

#ifdef TEMPORAL_DEBUG
	printf("TEMPORAL ADJUSTMENT EXECUTOR INIT...\n");
	pprint(node->temporalCl);
	fflush(stdout);
#endif

	return state;
}

void
ExecEndTemporalAdjustment(TemporalAdjustmentState *node)
{
	/* clean up tuple table */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	MemoryContextDelete(node->tempContext);

	/* shut down the subplans */
	ExecEndNode(outerPlanState(node));
}


/*
 * XXX PEMOSER Is an ExecReScan needed for NORMALIZE/ALIGN?
 */
void
ExecReScanTemporalAdjustment(TemporalAdjustmentState *node)
{
	/* must clear result tuple so first input tuple is returned */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->ss.ps.lefttree->chgParam == NULL)
		ExecReScan(node->ss.ps.lefttree);
}
