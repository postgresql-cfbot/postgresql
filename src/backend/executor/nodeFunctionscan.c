/*-------------------------------------------------------------------------
 *
 * nodeFunctionscan.c
 *	  Coordinates a scan over PL functions. It supports several use cases:
 *
 *      - single function scan, and multiple functions in ROWS FROM;
 *      - SRFs and regular functions;
 *      - tuple- and scalar-returning functions;
 *      - it will materialise if eflags call for it;
 *      - if possible, it will pipeline it’s output;
 *      - it avoids double-materialisation in case of SFRM_Materialize.
 *
 *    To achieve these, it depends upon the Materialize (for materialisation
 *    and pipelining) and SRFScan (for SRF handling, and tuple expansion,
 *    and double-materialisation avoidance) nodes, and the actual function
 *    invocation (for SRF- and regular functions alike) is done in execSRF.c.
 *
 *    The Planner knows nothing of the Materialize and SRFScan structures.
 *    They are constructed by the Executor at execution time, and are reported
 *    in the EXPLAIN output.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeFunctionscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecFunctionScan		scans a function.
 *		ExecFunctionNext		retrieve next tuple in sequential order.
 *		ExecInitFunctionScan	creates and initializes a functionscan node.
 *		ExecEndFunctionScan		releases any storage allocated.
 *		ExecReScanFunctionScan	rescans the function
 */
#include "postgres.h"

#include "catalog/pg_type.h"
#include "executor/nodeFunctionscan.h"
#include "executor/nodeSRFScan.h"
#include "executor/nodeMaterial.h"
#include "funcapi.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/syscache.h"


/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *		FunctionNext
 *
 *		This is a workhorse for ExecFunctionScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
FunctionNext(FunctionScanState *node)
{
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *scanslot;
	bool		alldone;
	int64		oldpos;
	int			funcno;
	int			att;

	/*
	 * get information from the estate and scan state
	 */
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	scanslot = node->ss.ss_ScanTupleSlot;

	if (node->simple)
	{
		/*
		 * Fast path for the trivial case: the function return type and scan
		 * result type are the same, so we fetch the function result straight
		 * into the scan result slot. No need to update ordinality or
		 * rowcounts either.
		 */
		TupleTableSlot *rs = node->funcstates[0].scanstate->ps.ps_ResultTupleSlot;

		/*
		 * Get the next tuple from the Scan node.
		 *
		 * If we have a rowcount for the function, and we know the previous
		 * read position was out of bounds, don't try the read. This allows
		 * backward scan to work when there are mixed row counts present.
		 */
		rs = ExecProcNode(&node->funcstates[0].scanstate->ps);

		if (TupIsNull(rs))
			return NULL;

		ExecCopySlot(scanslot, rs);

		return scanslot;
	}

	/*
	 * Increment or decrement ordinal counter before checking for end-of-data,
	 * so that we can move off either end of the result by 1 (and no more than
	 * 1) without losing correct count.  See PortalRunSelect for why we can
	 * assume that we won't be called repeatedly in the end-of-data state.
	 */
	oldpos = node->ordinal;
	if (ScanDirectionIsForward(direction))
		node->ordinal++;
	else
		node->ordinal--;

	/*
	 * Main loop over functions.
	 *
	 * We fetch the function results into func_slots (which match the function
	 * return types), and then copy the values to scanslot (which matches the
	 * scan result type), setting the ordinal column (if any) as well.
	 */
	ExecClearTuple(scanslot);
	att = 0;
	alldone = true;
	for (funcno = 0; funcno < node->nfuncs; funcno++)
	{
		FunctionScanPerFuncState *fs = &node->funcstates[funcno];
		TupleTableSlot *func_slot = fs->scanstate->ps.ps_ResultTupleSlot;
		int			i;

		/*
		 * Get the next tuple from the Scan node.
		 *
		 * If we have a rowcount for the function, and we know the previous
		 * read position was out of bounds, don't try the read. This allows
		 * backward scan to work when there are mixed row counts present.
		 */
		if (fs->rowcount != -1 && fs->rowcount < oldpos)
			ExecClearTuple(func_slot);
		else
			func_slot = ExecProcNode(&fs->scanstate->ps);

		if (TupIsNull(func_slot))
		{
			/*
			 * If we ran out of data for this function in the forward
			 * direction then we now know how many rows it returned. We need
			 * to know this in order to handle backwards scans. The row count
			 * we store is actually 1+ the actual number, because we have to
			 * position the tuplestore 1 off its end sometimes.
			 */
			if (ScanDirectionIsForward(direction) && fs->rowcount == -1)
				fs->rowcount = node->ordinal;

			/*
			 * populate the result cols with nulls
			 */
			for (i = 0; i < fs->colcount; i++)
			{
				scanslot->tts_values[att] = (Datum) 0;
				scanslot->tts_isnull[att] = true;
				att++;
			}
		}
		else
		{
			/*
			 * we have a result, so just copy it to the result cols.
			 */
			slot_getallattrs(func_slot);

			for (i = 0; i < fs->colcount; i++)
			{
				scanslot->tts_values[att] = func_slot->tts_values[i];
				scanslot->tts_isnull[att] = func_slot->tts_isnull[i];
				att++;
			}

			/*
			 * We're not done until every function result is exhausted; we pad
			 * the shorter results with nulls until then.
			 */
			alldone = false;
		}
	}

	/*
	 * ordinal col is always last, per spec.
	 */
	if (node->ordinality)
	{
		scanslot->tts_values[att] = Int64GetDatumFast(node->ordinal);
		scanslot->tts_isnull[att] = false;
	}

	/*
	 * If alldone, we just return the previously-cleared scanslot.  Otherwise,
	 * finish creating the virtual tuple.
	 */
	if (!alldone)
		ExecStoreVirtualTuple(scanslot);

	return scanslot;
}

/*
 * FunctionRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
FunctionRecheck(FunctionScanState *node, TupleTableSlot *slot)
{
	/* nothing to check */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecFunctionScan(node)
 *
 *		Scans the function sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecFunctionScan(PlanState *pstate)
{
	FunctionScanState *node = castNode(FunctionScanState, pstate);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) FunctionNext,
					(ExecScanRecheckMtd) FunctionRecheck);
}

/*
 * Helper function to build target list, which is required in order for
 * normal processing of ExecInit, from the tupdesc.
 */
static void
build_tlist_for_tupdesc(TupleDesc tupdesc, int colcount,
						List **mat_tlist, List **scan_tlist)
{
	Form_pg_attribute attr;
	int attno;

	for (attno = 1; attno <= colcount; attno++)
	{
		attr = TupleDescAttr(tupdesc, attno - 1);

		if (attr->attisdropped)
		{
			*scan_tlist = lappend(*scan_tlist,
							  makeTargetEntry((Expr *)
								  makeConst(INT2OID, -1,
											0,
											attr->attlen,
											0 /* value */, true /* isnull */,
											true),
								  attno, attr->attname.data,
								  attr->attisdropped));
			*mat_tlist = lappend(*mat_tlist,
							 makeTargetEntry((Expr *)
								 makeVar(1 /* varno */, attno, INT2OID, -1, 0, 0),
								 attno, attr->attname.data, attr->attisdropped));
		}
		else
		{
			*scan_tlist = lappend(*scan_tlist,
							  makeTargetEntry((Expr *)
								  makeVar(1 /* varno */, attno, attr->atttypid,
										  attr->atttypmod, attr->attcollation, 0),
								  attno, attr->attname.data, attr->attisdropped));
			*mat_tlist = lappend(*mat_tlist,
							 makeTargetEntry((Expr *)
								 makeVar(1 /* varno */, attno, attr->atttypid,
										 attr->atttypmod, attr->attcollation, 0),
								 attno, attr->attname.data, attr->attisdropped));
		}
	}
}

/* ----------------------------------------------------------------
 *		ExecInitFunctionScan
 * ----------------------------------------------------------------
 */
FunctionScanState *
ExecInitFunctionScan(FunctionScan *node, EState *estate, int eflags)
{
	FunctionScanState *scanstate;
	int			nfuncs = list_length(node->functions);
	TupleDesc	scan_tupdesc;
	int			i,
				natts;
	ListCell   *lc;
	bool 		needs_material;

	/* check for unsupported flags */
	Assert(!(eflags & EXEC_FLAG_MARK));

	/*
	 * FunctionScan should not have any children.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create new ScanState for node
	 */
	scanstate = makeNode(FunctionScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->ss.ps.ExecProcNode = ExecFunctionScan;
	scanstate->eflags = eflags;

	/*
	 * are we adding an ordinality column?
	 */
	scanstate->ordinality = node->funcordinality;

	scanstate->nfuncs = nfuncs;
	if (nfuncs == 1 && !node->funcordinality)
		scanstate->simple = true;
	else
		scanstate->simple = false;

	/* Only add a Mterialize node if required */
	needs_material = eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD);

	/*
	 * Ordinal 0 represents the "before the first row" position.
	 *
	 * We need to track ordinal position even when not adding an ordinality
	 * column to the result, in order to handle backwards scanning properly
	 * with multiple functions with different result sizes. (We can't position
	 * any individual function's tuplestore any more than 1 place beyond its
	 * end, so when scanning backwards, we need to know when to start
	 * including the function in the scan again.)
	 */
	scanstate->ordinal = 0;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	scanstate->funcstates = palloc(nfuncs * sizeof(FunctionScanPerFuncState));

	natts = 0;
	i = 0;
	foreach(lc, node->functions)
	{
		RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(lc);
		Node	   *funcexpr = rtfunc->funcexpr;
		int			colcount = rtfunc->funccolcount;
		FunctionScanPerFuncState *fs = &scanstate->funcstates[i];
		TypeFuncClass functypclass;
		Oid			funcrettype;
		TupleDesc	tupdesc;
		List /* TargetEntry* */ *mat_tlist = NIL;
		List /* TargetEntry* */ *scan_tlist = NIL;
		bool funcReturnsTuple;

		fs->rowcount = -1;

		/*
		 * Now determine if the function returns a simple or composite type,
		 * and build an appropriate targetlist.  Note that in the composite case,
		 * the function may now return more columns than it did when the plan
		 * was made; we have to ignore any columns beyond "colcount".
		 */
		functypclass = get_expr_result_type(funcexpr,
											&funcrettype,
											&tupdesc);

		if (functypclass == TYPEFUNC_COMPOSITE ||
			functypclass == TYPEFUNC_COMPOSITE_DOMAIN)
		{
			/* Composite data type, e.g. a table's row type */
			Assert(tupdesc);
			Assert(tupdesc->natts >= colcount);
			/* Must copy it out of typcache for safety */
			tupdesc = CreateTupleDescCopy(tupdesc);
			funcReturnsTuple = true;
		}
		else if (functypclass == TYPEFUNC_SCALAR)
		{
			/* Base data type, i.e. scalar */
			tupdesc = CreateTemplateTupleDesc(1);
			TupleDescInitEntry(tupdesc,
							   (AttrNumber) 1,
							   NULL,	/* don't care about the name here */
							   funcrettype,
							   -1,
							   0);
			TupleDescInitEntryCollation(tupdesc,
										(AttrNumber) 1,
										exprCollation(funcexpr));
			funcReturnsTuple = false;
		}
		else if (functypclass == TYPEFUNC_RECORD)
		{
			tupdesc = BuildDescFromLists(rtfunc->funccolnames,
										 rtfunc->funccoltypes,
										 rtfunc->funccoltypmods,
										 rtfunc->funccolcollations);

			/*
			 * For RECORD results, make sure a typmod has been assigned.  (The
			 * function should do this for itself, but let's cover things in
			 * case it doesn't.)
			 */
			BlessTupleDesc(tupdesc);
			funcReturnsTuple = true;
		}
		else
		{
			/* crummy error message, but parser should have caught this */
			elog(ERROR, "function in FROM has unsupported return type");
		}

		fs->colcount = colcount;

		/* Expand tupdesc into targetlists for the scan nodes */
		build_tlist_for_tupdesc(tupdesc, colcount, &mat_tlist, &scan_tlist);

		SRFScanPlan *srfscan = makeNode(SRFScanPlan);
		srfscan->funcexpr = funcexpr;
		srfscan->rtfunc = (Node *) rtfunc;
		srfscan->plan.targetlist = scan_tlist;
		srfscan->plan.extParam = rtfunc->funcparams;
		srfscan->plan.allParam = rtfunc->funcparams;
		srfscan->funcResultDesc = tupdesc;
		srfscan->funcReturnsTuple = funcReturnsTuple;
		Plan *scan = &srfscan->plan;

		if (needs_material)
		{
			Material *fscan = makeNode(Material);
			fscan->plan.lefttree = scan;
			fscan->plan.targetlist = mat_tlist;
			fscan->plan.extParam = rtfunc->funcparams;
			fscan->plan.allParam = rtfunc->funcparams;
			scan = &fscan->plan;
		}

		fs->scanstate = (ScanState *) ExecInitNode (scan, estate, eflags);

		if (needs_material)
		{
			/*
			 * Tell the SRFScan about its parent, so that it can donate
			 * the SRF's tuplestore if the SRF uses SFRM_Materialize.
			 */
			MaterialState *ms = (MaterialState *) fs->scanstate;
			SRFScanState *sss = (SRFScanState *) outerPlanState(ms);

			sss->setexpr->funcResultStoreDonationEnabled = true;
			sss->setexpr->funcResultStoreDonationTarget = &ms->ss.ps;
		}

		natts += colcount;
		i++;
	}

	/*
	 * Create the combined TupleDesc
	 *
	 * If there is just one function without ordinality, the scan result
	 * tupdesc is the same as the function result tupdesc --- except that we
	 * may stuff new names into it below, so drop any rowtype label.
	 */
	if (scanstate->simple)
	{
		SRFScanState *sss = IsA(scanstate->funcstates[0].scanstate, MaterialState) ?
				(SRFScanState *) outerPlanState((MaterialState *) scanstate->funcstates[0].scanstate) :
				(SRFScanState *) scanstate->funcstates[0].scanstate;

		scan_tupdesc = CreateTupleDescCopy(sss->setexpr->funcResultDesc);
		scan_tupdesc->tdtypeid = RECORDOID;
		scan_tupdesc->tdtypmod = -1;
	}
	else
	{
		AttrNumber	attno = 0;

		if (node->funcordinality)
			natts++;

		scan_tupdesc = CreateTemplateTupleDesc(natts);

		for (i = 0; i < nfuncs; i++)
		{
			SRFScanState *sss = IsA(scanstate->funcstates[i].scanstate, MaterialState) ?
					(SRFScanState *) outerPlanState((MaterialState *) scanstate->funcstates[i].scanstate) :
					(SRFScanState *) scanstate->funcstates[i].scanstate;

			TupleDesc	tupdesc = sss->setexpr->funcResultDesc;
			int			colcount = sss->colcount;
			int			j;

			for (j = 1; j <= colcount; j++)
				TupleDescCopyEntry(scan_tupdesc, ++attno, tupdesc, j);
		}

		/* If doing ordinality, add a column of type "bigint" at the end */
		if (node->funcordinality)
		{
			TupleDescInitEntry(scan_tupdesc,
							   ++attno,
							   NULL,	/* don't care about the name here */
							   INT8OID,
							   -1,
							   0);
		}

		Assert(attno == natts);
	}

	/*
	 * Initialize scan slot and type.
	 */
	ExecInitScanTupleSlot(estate, &scanstate->ss, scan_tupdesc,
						  &TTSOpsMinimalTuple);

	/*
	 * Initialize result slot, type and projection.
	 */
	ExecInitResultTypeTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.qual =
		ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);

	/*
	 * Create a memory context that ExecMakeTableFunctionResult can use to
	 * evaluate function arguments in.  We can't use the per-tuple context for
	 * this because it gets reset too often; but we don't want to leak
	 * evaluation results into the query-lifespan context either.  We just
	 * need one context, because we evaluate each function separately.
	 */
	scanstate->argcontext = AllocSetContextCreate(CurrentMemoryContext,
												  "Table function arguments",
												  ALLOCSET_DEFAULT_SIZES);

	return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndFunctionScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndFunctionScan(FunctionScanState *node)
{
	int			i;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	if (node->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * Release the Material scan resources
	 */
	for (i = 0; i < node->nfuncs; i++)
	{
		ExecEndNode(&node->funcstates[i].scanstate->ps);
	}
}

/* ----------------------------------------------------------------
 *		ExecReScanFunctionScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanFunctionScan(FunctionScanState *node)
{
	FunctionScan *scan = (FunctionScan *) node->ss.ps.plan;
	int			i;
	Bitmapset  *chgparam = node->ss.ps.chgParam;

	if (node->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	ExecScanReScan(&node->ss);

	/*
	 * We must recompute if an
	 * expression contains changed parameters.
	 */
	if (chgparam)
	{
		ListCell   *lc;

		i = 0;
		foreach(lc, scan->functions)
		{
			RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(lc);

			if (bms_overlap(chgparam, rtfunc->funcparams))
			{
				UpdateChangedParamSet(&node->funcstates[i].scanstate->ps,
									  node->ss.ps.chgParam);

				node->funcstates[i].rowcount = -1;
			}
			i++;
		}
	}

	/* Reset ordinality counter */
	node->ordinal = 0;

	/* Rescan them all */
	for (i = 0; i < node->nfuncs; i++)
	{
		ExecReScan(&node->funcstates[i].scanstate->ps);
	}
}
