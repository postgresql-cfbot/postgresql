/*-------------------------------------------------------------------------
 *
 * nodeSRFScan.c
 *	  Coordinates a scan over a single SRF function, or a non-SRF as if it
 *    were an SRF returning a single row.
 *
 *    SRFScan expands the function's output if it returns a tuple. If the
 *    SRF uses SFRM_Materialize, it will donate the returned tuplestore to
 *    the parent Materialize node, if there is one, to avoid double-
 *    materialisation.
 *
 *    The Planner knows nothing of the SRFScan structure. It is constructed
 *    by the Executor at execution time, and is reported in the EXPLAIN
 *    output.
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSRFScan.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "executor/nodeSRFScan.h"
#include "executor/nodeMaterial.h"
#include "funcapi.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

static TupleTableSlot *			/* result tuple from subplan */
ExecSRF(PlanState *node)
{
	SRFScanState *pstate = (SRFScanState *) node;
	ExprContext *econtext = pstate->ss.ps.ps_ExprContext;
	TupleTableSlot *resultSlot = pstate->ss.ps.ps_ResultTupleSlot;
	Datum result;
	ExprDoneCond *isdone = &pstate->elemdone;
	bool	   isnull;
	SetExprState *setexpr = pstate->setexpr;
	FunctionCallInfo fcinfo;
	ReturnSetInfo *rsinfo;

	/* We only support forward scans. */
	Assert(ScanDirectionIsForward(pstate->ss.ps.state->es_direction));

	ExecClearTuple(resultSlot);

	/*
	 * Only execute something if we are not already complete...
	 */
	if (*isdone == ExprEndResult)
		return NULL;

	/*
	 * Evaluate SRF - possibly continuing previously started output.
	 */
	result = ExecMakeFunctionResultSet((SetExprState *) setexpr,
										econtext, pstate->argcontext,
										&isnull, isdone);

	if (*isdone == ExprEndResult)
		return NULL;

	fcinfo = setexpr->fcinfo;
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	/* Have we donated the result store? */
	if (setexpr->funcResultStoreDonated)
		return 0;

	/*
	 * If we obtained a tupdesc, check it is appropriate, but not in
	 * the case of SFRM_Materialize becuase is will have been checked
	 * already.
	 */
	if (!pstate->tupdesc_checked &&
		setexpr->funcReturnsTuple &&
		rsinfo->returnMode != SFRM_Materialize &&
		rsinfo->setDesc && setexpr->funcResultDesc)
	{
		tupledesc_match (setexpr->funcResultDesc, rsinfo->setDesc);
		pstate->tupdesc_checked = true;
	}

	/*
	 * If returned a tupple, expand into multiple columns.
	 */
	if (setexpr->funcReturnsTuple)
	{
		if (!isnull)
		{
			HeapTupleHeader td = DatumGetHeapTupleHeader(result);
			HeapTupleData tmptup;

			/*
			 * In SFRM_Materialize mode, the type will have been checked
			 * already.
			 */
			if (rsinfo->returnMode != SFRM_Materialize)
			{
				/*
				 * Verify all later returned rows have same subtype;
				 * necessary in case the type is RECORD.
				 */
				if (HeapTupleHeaderGetTypeId(td) != rsinfo->setDesc->tdtypeid ||
					HeapTupleHeaderGetTypMod(td) != rsinfo->setDesc->tdtypmod)
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("rows returned by function are not all of the same row type")));
			}

			/*
			 * tuplestore_puttuple needs a HeapTuple not a bare
			 * HeapTupleHeader, but it doesn't need all the fields.
			 */
			tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
			tmptup.t_data = td;

			heap_deform_tuple (&tmptup, setexpr->funcResultDesc,
							   resultSlot->tts_values,
							   resultSlot->tts_isnull);
		}
		else
		{
			/*
			 * populate the result cols with nulls
			 */
			int i;
			for (i = 0; i < pstate->colcount; i++)
			{
				resultSlot->tts_values[i] = (Datum) 0;
				resultSlot->tts_isnull[i] = true;
			}
		}
	}
	else
	{
		/* Scalar-type case: just store the function result */
		resultSlot->tts_values[0] = result;
		resultSlot->tts_isnull[0] = isnull;
	}

	/*
	 * If we achieved obtained a single result, don't execute again.
	 */
	if (*isdone == ExprSingleResult)
		*isdone = ExprEndResult;

	ExecStoreVirtualTuple(resultSlot);
	return resultSlot;
}

SRFScanState *
ExecInitSRFScan(SRFScanPlan *node, EState *estate, int eflags)
{
	RangeTblFunction *rtfunc = (RangeTblFunction *) node->rtfunc;

	SRFScanState *srfstate;

	/*
	 * SRFScan should not have any children.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	srfstate = makeNode(SRFScanState);
	srfstate->ss.ps.plan = (Plan *) node;
	srfstate->ss.ps.state = estate;
	srfstate->ss.ps.ExecProcNode = ExecSRF;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &srfstate->ss.ps);

	srfstate->setexpr =
		ExecInitFunctionResultSet((Expr *) node->funcexpr,
								  srfstate->ss.ps.ps_ExprContext,
								  &srfstate->ss.ps);

	srfstate->setexpr->funcResultDesc = node->funcResultDesc;
	srfstate->setexpr->funcReturnsTuple = node->funcReturnsTuple;

	srfstate->colcount = rtfunc->funccolcount;

	srfstate->tupdesc_checked = false;

	/* Start with the assumption we will get some result. */
	srfstate->elemdone = ExprSingleResult;

	/*
	 * Initialize result type and slot. No need to initialize projection info
	 * because this node doesn't do projections (ps_ResultTupleSlot).
	 *
	 * material nodes only return tuples from their materialized relation.
	 */
	ExecInitScanTupleSlot(estate, &srfstate->ss, srfstate->setexpr->funcResultDesc,
						  &TTSOpsMinimalTuple);
	ExecInitResultTupleSlotTL(&srfstate->ss.ps, &TTSOpsMinimalTuple);
	ExecAssignScanProjectionInfo(&srfstate->ss);

	/*
	 * Create a memory context that ExecMakeFunctionResultSet can use to
	 * evaluate function arguments in.  We can't use the per-tuple context for
	 * this because it gets reset too often; but we don't want to leak
	 * evaluation results into the query-lifespan context either.  We use one
	 * context for the arguments of all tSRFs, as they have roughly equivalent
	 * lifetimes.
	 */
	srfstate->argcontext = AllocSetContextCreate(CurrentMemoryContext,
											  "SRF function arguments",
											  ALLOCSET_DEFAULT_SIZES);
	return srfstate;
}

void
ExecEndSRFScan(SRFScanState *node)
{
	/* Nothing to do */
}

void
ExecReScanSRF(SRFScanState *node)
{
	/* Expecting some results. */
	node->elemdone = ExprSingleResult;

	/* We must re-evaluate function call arguments. */
	node->setexpr->setArgsValid = false;
}

bool
ExecSRFDonateResultTuplestore(SetExprState *fcache)
{
	if (fcache->funcResultStoreDonationEnabled)
	{
		if (IsA (fcache->funcResultStoreDonationTarget, MaterialState))
		{
			MaterialState *target = (MaterialState *) fcache->funcResultStoreDonationTarget;

			ExecMaterialReceiveResultStore(target, fcache->funcResultStore);

			fcache->funcResultStore = NULL;

			fcache->funcResultStoreDonated = true;

			return true;
		}
	}

	return false;
}
