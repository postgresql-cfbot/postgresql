/*-------------------------------------------------------------------------
 *
 * execScan.c
 *	  This code provides support for generalized relation scans. ExecScan
 *	  is passed a node and a pointer to a function to "do the right thing"
 *	  and return a tuple from the relation. ExecScan then does the tedious
 *	  stuff - checking the qualification and projecting the tuple
 *	  appropriately.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execScan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/executor.h"
#include "executor/execScan.h"
#include "miscadmin.h"
#include "utils/memutils.h"


static bool tlist_matches_tupdesc(PlanState *ps, List *tlist, Index varno, TupleDesc tupdesc);


TupleTableSlot *
ExecScanFetchEPQ(ScanState *node,
				 ExecScanAccessMtd accessMtd,
				 ExecScanRecheckMtd recheckMtd,
				 bool *done)
{
	EState	   *estate = node->ps.state;
	Index		scanrelid = ((Scan *) node->ps.plan)->scanrelid;

	if (scanrelid == 0)
	{
		TupleTableSlot *slot = node->ss_ScanTupleSlot;
		*done = true;

		/*
		 * This is a ForeignScan or CustomScan which has pushed down a
		 * join to the remote side.  The recheck method is responsible not
		 * only for rechecking the scan/join quals but also for storing
		 * the correct tuple in the slot.
		 */
		if (!(*recheckMtd) (node, slot))
			ExecClearTuple(slot);	/* would not be returned by scan */
		return slot;
	}
	else if (estate->es_epqTupleSet[scanrelid - 1])
	{
		TupleTableSlot *slot = node->ss_ScanTupleSlot;

		*done = true;

		/* Return empty slot if we already returned a tuple */
		if (estate->es_epqScanDone[scanrelid - 1])
			return ExecClearTuple(slot);
		/* Else mark to remember that we shouldn't return more */
		estate->es_epqScanDone[scanrelid - 1] = true;

		/* Return empty slot if we haven't got a test tuple */
		if (estate->es_epqTuple[scanrelid - 1] == NULL)
			return ExecClearTuple(slot);

		/* Store test tuple in the plan node's scan slot */
		ExecStoreTuple(estate->es_epqTuple[scanrelid - 1],
					   slot, InvalidBuffer, false);

		/* Check if it meets the access-method conditions */
		if (!(*recheckMtd) (node, slot))
			ExecClearTuple(slot);	/* would not be returned by scan */

		return slot;
	}

	return NULL;
}

/*
 * ExecAssignScanProjectionInfo
 *		Set up projection info for a scan node, if necessary.
 *
 * We can avoid a projection step if the requested tlist exactly matches
 * the underlying tuple type.  If so, we just set ps_ProjInfo to NULL.
 * Note that this case occurs not only for simple "SELECT * FROM ...", but
 * also in most cases where there are joins or other processing nodes above
 * the scan node, because the planner will preferentially generate a matching
 * tlist.
 *
 * ExecAssignScanType must have been called already.
 */
void
ExecAssignScanProjectionInfo(ScanState *node)
{
	Scan	   *scan = (Scan *) node->ps.plan;

	ExecAssignScanProjectionInfoWithVarno(node, scan->scanrelid);
}

/*
 * ExecAssignScanProjectionInfoWithVarno
 *		As above, but caller can specify varno expected in Vars in the tlist.
 */
void
ExecAssignScanProjectionInfoWithVarno(ScanState *node, Index varno)
{
	Scan	   *scan = (Scan *) node->ps.plan;

	if (tlist_matches_tupdesc(&node->ps,
							  scan->plan.targetlist,
							  varno,
							  node->ss_ScanTupleSlot->tts_tupleDescriptor))
		node->ps.ps_ProjInfo = NULL;
	else
		ExecAssignProjectionInfo(&node->ps,
								 node->ss_ScanTupleSlot->tts_tupleDescriptor);
}

static bool
tlist_matches_tupdesc(PlanState *ps, List *tlist, Index varno, TupleDesc tupdesc)
{
	int			numattrs = tupdesc->natts;
	int			attrno;
	bool		hasoid;
	ListCell   *tlist_item = list_head(tlist);

	/* Check the tlist attributes */
	for (attrno = 1; attrno <= numattrs; attrno++)
	{
		Form_pg_attribute att_tup = TupleDescAttr(tupdesc, attrno - 1);
		Var		   *var;

		if (tlist_item == NULL)
			return false;		/* tlist too short */
		var = (Var *) ((TargetEntry *) lfirst(tlist_item))->expr;
		if (!var || !IsA(var, Var))
			return false;		/* tlist item not a Var */
		/* if these Asserts fail, planner messed up */
		Assert(var->varno == varno);
		Assert(var->varlevelsup == 0);
		if (var->varattno != attrno)
			return false;		/* out of order */
		if (att_tup->attisdropped)
			return false;		/* table contains dropped columns */

		/*
		 * Note: usually the Var's type should match the tupdesc exactly, but
		 * in situations involving unions of columns that have different
		 * typmods, the Var may have come from above the union and hence have
		 * typmod -1.  This is a legitimate situation since the Var still
		 * describes the column, just not as exactly as the tupdesc does. We
		 * could change the planner to prevent it, but it'd then insert
		 * projection steps just to convert from specific typmod to typmod -1,
		 * which is pretty silly.
		 */
		if (var->vartype != att_tup->atttypid ||
			(var->vartypmod != att_tup->atttypmod &&
			 var->vartypmod != -1))
			return false;		/* type mismatch */

		tlist_item = lnext(tlist_item);
	}

	if (tlist_item)
		return false;			/* tlist too long */

	/*
	 * If the plan context requires a particular hasoid setting, then that has
	 * to match, too.
	 */
	if (ExecContextForcesOids(ps, &hasoid) &&
		hasoid != tupdesc->tdhasoid)
		return false;

	return true;
}

/*
 * ExecScanReScan
 *
 * This must be called within the ReScan function of any plan node type
 * that uses ExecScan().
 */
void
ExecScanReScan(ScanState *node)
{
	EState	   *estate = node->ps.state;

	/* Rescan EvalPlanQual tuple if we're inside an EvalPlanQual recheck */
	if (estate->es_epqScanDone != NULL)
	{
		Index		scanrelid = ((Scan *) node->ps.plan)->scanrelid;

		if (scanrelid > 0)
			estate->es_epqScanDone[scanrelid - 1] = false;
		else
		{
			Bitmapset  *relids;
			int			rtindex = -1;

			/*
			 * If an FDW or custom scan provider has replaced the join with a
			 * scan, there are multiple RTIs; reset the epqScanDone flag for
			 * all of them.
			 */
			if (IsA(node->ps.plan, ForeignScan))
				relids = ((ForeignScan *) node->ps.plan)->fs_relids;
			else if (IsA(node->ps.plan, CustomScan))
				relids = ((CustomScan *) node->ps.plan)->custom_relids;
			else
				elog(ERROR, "unexpected scan node: %d",
					 (int) nodeTag(node->ps.plan));

			while ((rtindex = bms_next_member(relids, rtindex)) >= 0)
			{
				Assert(rtindex > 0);
				estate->es_epqScanDone[rtindex - 1] = false;
			}
		}
	}
}
