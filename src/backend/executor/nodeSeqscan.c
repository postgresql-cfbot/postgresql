/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.c
 *	  Support routines for sequential scans of relations.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSeqscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecSeqScan				sequentially scans a relation.
 *		ExecSeqNext				retrieve next tuple in sequential order.
 *		ExecInitSeqScan			creates and initializes a seqscan node.
 *		ExecEndSeqScan			releases any storage allocated.
 *		ExecReScanSeqScan		rescans the relation
 *
 *		ExecSeqScanEstimate		estimates DSM space needed for parallel scan
 *		ExecSeqScanInitializeDSM initialize DSM for parallel scan
 *		ExecSeqScanReInitializeDSM reinitialize DSM for fresh parallel scan
 *		ExecSeqScanInitializeWorker attach to DSM info in parallel worker
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/execParallel.h"
#include "executor/execScan.h"
#include "executor/executor.h"
#include "executor/nodeSeqscan.h"
#include "pgstat.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

static TupleTableSlot *SeqNext(SeqScanState *node);

/* Private state for one scan qual evaluated in page batches. */
struct SeqScanBatchQual
{
	FmgrInfo	func;
	FunctionCallInfo fcinfo;
	ExprState  *param_expr;
	AttrNumber	attnum;
	uint8		var_argno;
};

/* Private state for page-batch qual evaluation. */
struct SeqScanBatchState
{
	struct SeqScanBatchQual *quals;
	ExprState  *scalar_qual;
	uint64		mask;			/* bit i is for batch index first + i */
	BlockNumber block;
	int			first;
	int			nrows;
	int			nquals;
};

static inline void
reset_batch_qual(struct SeqScanBatchState *state)
{
	state->nrows = 0;
}

static BufferHeapTupleTableSlot *
get_valid_batch_slot(TupleTableSlot *slot, BlockNumber *block)
{
	BufferHeapTupleTableSlot *bslot;
	HeapPageBatch *batch;

	if (!TTS_IS_BUFFERTUPLE(slot) ||
		!ItemPointerIsValid(&slot->tts_tid))
		return NULL;

	bslot = (BufferHeapTupleTableSlot *) slot;
	batch = &bslot->batch;
	if (batch->offsets == NULL ||
		!BufferIsValid(bslot->buffer) ||
		batch->current < 0 || batch->current >= batch->ntuples)
		return NULL;

	*block = BufferGetBlockNumber(bslot->buffer);
	if (ItemPointerGetBlockNumber(&slot->tts_tid) != *block ||
		ItemPointerGetOffsetNumber(&slot->tts_tid) !=
		batch->offsets[batch->current])
		return NULL;

	return bslot;
}

static pg_always_inline bool
reuse_batch_qual(struct SeqScanBatchState *state, TupleTableSlot *slot,
				 bool *result)
{
	BufferHeapTupleTableSlot *bslot;
	BlockNumber block;
	int			position;

	if (state->nrows <= 0 || state->nrows > 64)
		goto invalid;

	bslot = get_valid_batch_slot(slot, &block);
	if (bslot == NULL)
		goto invalid;

	position = bslot->batch.current - state->first;
	if (block != state->block ||
		position < 0 || position >= state->nrows)
		goto invalid;

	*result = (state->mask & (UINT64CONST(1) << position)) != 0;
	return true;

invalid:
	reset_batch_qual(state);
	return false;
}

static pg_always_inline bool
eval_scalar_qual(struct SeqScanBatchState *state, TupleTableSlot *slot,
				 ExprContext *econtext)
{
	econtext->ecxt_scantuple = slot;
	return ExecQual(state->scalar_qual, econtext);
}

static pg_always_inline Datum
heap_page_batch_getattr(Page page, const HeapPageBatch *batch, int index,
						TupleDesc tupledesc, AttrNumber attnum, bool *isnull)
{
	HeapTupleData tuple;
	ItemId		lpp;

	lpp = PageGetItemId(page, batch->offsets[index]);
	Assert(ItemIdIsNormal(lpp));
	tuple.t_data = (HeapTupleHeader) PageGetItem(page, lpp);
	tuple.t_len = ItemIdGetLength(lpp);

	return heap_getattr(&tuple, attnum, tupledesc, isnull);
}

static pg_always_inline bool
eval_batch_qual(struct SeqScanBatchQual *qual, Datum value, bool isnull)
{
	FunctionCallInfo fcinfo = qual->fcinfo;
	PgStat_FunctionCallUsage fcusage;
	Datum		result;
	bool		track_function;

	fcinfo->args[qual->var_argno].value = value;
	fcinfo->args[qual->var_argno].isnull = isnull;
	if (isnull)
		return false;
	Assert(!fcinfo->args[1 - qual->var_argno].isnull);

	track_function = pgstat_track_functions > fcinfo->flinfo->fn_stats;
	if (track_function)
		pgstat_init_function_usage(fcinfo, &fcusage);
	fcinfo->isnull = false;
	result = FunctionCallInvoke(fcinfo);
	if (track_function)
		pgstat_end_function_usage(&fcusage, true);

	return !fcinfo->isnull && DatumGetBool(result);
}

static void
prepare_batch_qual(struct SeqScanBatchState *state, TupleTableSlot *slot,
				   ScanDirection direction, ExprContext *econtext)
{
	BufferHeapTupleTableSlot *bslot;
	HeapPageBatch *batch;
	Datum		values[64];
	MemoryContext oldcontext;
	BlockNumber block;
	Page		page;
	uint64		mask = 0;
	int			first;
	int			nrows;
	int			position;

	Assert(ScanDirectionIsForward(direction) ||
		   ScanDirectionIsBackward(direction));
	Assert(state->nquals > 0);

	reset_batch_qual(state);

	bslot = get_valid_batch_slot(slot, &block);
	if (bslot == NULL)
		return;

	batch = &bslot->batch;
	if (ScanDirectionIsForward(direction))
	{
		first = batch->current + 1;
		nrows = Min(64, batch->ntuples - first);
	}
	else
	{
		nrows = Min(64, batch->current);
		first = batch->current - nrows;
	}
	if (nrows == 0)
		return;

	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	page = BufferGetPage(bslot->buffer);

	/* Evaluate in scan direction, but index values and mask in batch order. */
	for (int q = 0;
		 q < state->nquals && (q == 0 || mask != 0);
		 q++)
	{
		struct SeqScanBatchQual *qual = &state->quals[q];
		FunctionCallInfo fcinfo = qual->fcinfo;
		NullableDatum *arg = &fcinfo->args[1 - qual->var_argno];

		if (qual->param_expr != NULL)
			arg->value = ExecEvalExpr(qual->param_expr, econtext,
									  &arg->isnull);
		if (arg->isnull)
			mask = 0;
		else if (q == 0)
		{
			uint64		nulls = 0;

			/*
			 * Every row is a candidate for the first qual. Using the masked
			 * loops below would add per-row tests to the common single-qual
			 * case. Track nulls separately, then build the result mask.
			 */
			position = ScanDirectionIsForward(direction) ? 0 : nrows - 1;
			for (int i = 0; i < nrows; i++, position += direction)
			{
				bool		isnull;
				uint64		bit = UINT64CONST(1) << position;

				values[position] = heap_page_batch_getattr(page, batch,
														   first + position,
														   slot->tts_tupleDescriptor,
														   qual->attnum, &isnull);
				if (isnull)
					nulls |= bit;
			}

			position = ScanDirectionIsForward(direction) ? 0 : nrows - 1;
			for (int i = 0; i < nrows; i++, position += direction)
			{
				uint64		bit = UINT64CONST(1) << position;

				if (eval_batch_qual(qual, values[position],
									(nulls & bit) != 0))
					mask |= bit;
			}
		}
		else
		{
			/*
			 * For later quals, mask contains only rows accepted by all
			 * preceding quals. Testing it in both loops avoids fetching
			 * attributes and invoking operators for rows already rejected.
			 *
			 * If the preceding qual uses the same attribute, values for rows
			 * still present in the mask are already populated and non-NULL.
			 */
			if (qual->attnum != state->quals[q - 1].attnum)
			{
				position = ScanDirectionIsForward(direction) ? 0 : nrows - 1;
				for (int i = 0; i < nrows; i++, position += direction)
				{
					bool		isnull;
					uint64		bit = UINT64CONST(1) << position;

					if ((mask & bit) == 0)
						continue;
					values[position] = heap_page_batch_getattr(page, batch,
															   first + position,
															   slot->tts_tupleDescriptor,
															   qual->attnum, &isnull);
					if (isnull)
						mask &= ~bit;
				}
			}

			position = ScanDirectionIsForward(direction) ? 0 : nrows - 1;
			for (int i = 0; i < nrows && mask != 0;
				 i++, position += direction)
			{
				uint64		bit = UINT64CONST(1) << position;

				if ((mask & bit) == 0)
					continue;
				if (!eval_batch_qual(qual, values[position], false))
					mask &= ~bit;
			}
		}

		if (qual->param_expr != NULL)
		{
			arg->value = (Datum) 0;
			arg->isnull = true;
		}
	}
	MemoryContextSwitchTo(oldcontext);

	state->mask = mask;
	state->block = block;
	state->first = first;
	state->nrows = nrows;
}

static bool
is_batch_qual_value(Node *node)
{
	return IsA(node, Const) ||
		(IsA(node, Param) &&
		 (castNode(Param, node)->paramkind == PARAM_EXTERN ||
		  castNode(Param, node)->paramkind == PARAM_EXEC));
}

static OpExpr *
match_batch_qual(Node *clause, uint8 *var_argno)
{
	OpExpr	   *op;
	Node	   *left;
	Node	   *right;
	Var		   *var;

	if (!IsA(clause, OpExpr))
		return NULL;
	op = (OpExpr *) clause;
	if (list_length(op->args) != 2)
		return NULL;

	left = linitial(op->args);
	right = lsecond(op->args);
	if (IsA(left, Var) && is_batch_qual_value(right))
		*var_argno = 0;
	else if (is_batch_qual_value(left) && IsA(right, Var))
		*var_argno = 1;
	else
		return NULL;
	var = list_nth_node(Var, op->args, *var_argno);

	if (var->varattno <= 0 || var->varlevelsup != 0 ||
		var->varreturningtype != VAR_RETURNING_DEFAULT)
		return NULL;
	if (op->opresulttype != BOOLOID ||
		op->opretset ||
		func_volatile(op->opfuncid) != PROVOLATILE_IMMUTABLE ||
		!func_strict(op->opfuncid) ||
		!get_func_leakproof(op->opfuncid))
		return NULL;

	return op;
}

static void
init_batch_qual(struct SeqScanBatchQual *qual, OpExpr *op,
				PlanState *parent)
{
	uint8		var_argno = IsA(linitial(op->args), Var) ? 0 : 1;
	Expr	   *arg = (Expr *) list_nth(op->args, 1 - var_argno);
	Var		   *var = list_nth_node(Var, op->args, var_argno);

	fmgr_info(op->opfuncid, &qual->func);
	fmgr_info_set_expr((Node *) op, &qual->func);
	qual->fcinfo = palloc0(SizeForFunctionCallInfo(2));
	InitFunctionCallInfoData(*qual->fcinfo, &qual->func, 2,
							 op->inputcollid, NULL, NULL);
	if (IsA(arg, Const))
	{
		qual->fcinfo->args[1 - var_argno].value =
			castNode(Const, arg)->constvalue;
		qual->fcinfo->args[1 - var_argno].isnull =
			castNode(Const, arg)->constisnull;
	}
	else
	{
		Assert(IsA(arg, Param));
		qual->param_expr = ExecInitExpr(arg, parent);
	}
	qual->attnum = var->varattno;
	qual->var_argno = var_argno;
}

static struct SeqScanBatchState *
init_batch_state(SeqScan *node, PlanState *parent)
{
	struct SeqScanBatchState *state;
	List	   *clauses = node->scan.plan.qual;
	List	   *batch_clauses;
	List	   *remaining_clauses;
	int			nclauses = list_length(clauses);
	int			nquals = 0;
	int			i = 0;

	if (nclauses == 0)
		return NULL;

	foreach_ptr(Node, clause, clauses)
	{
		uint8		var_argno;

		if (match_batch_qual(clause, &var_argno) == NULL)
			break;
		nquals++;
	}
	if (nquals == 0)
		return NULL;

	batch_clauses = list_copy_head(clauses, nquals);
	remaining_clauses = list_copy_tail(clauses, nquals);
	state = palloc0_object(struct SeqScanBatchState);
	state->nquals = nquals;
	state->scalar_qual = ExecInitQual(batch_clauses, parent);
	parent->qual = ExecInitQual(remaining_clauses, parent);
	state->quals = palloc0_array(struct SeqScanBatchQual, state->nquals);
	foreach_ptr(OpExpr, op, batch_clauses)
		init_batch_qual(&state->quals[i++], op, parent);

	return state;
}

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		SeqNext
 *
 *		This is a workhorse for ExecSeqScan
 * ----------------------------------------------------------------
 */
static pg_always_inline TupleTableSlot *
SeqNext(SeqScanState *node)
{
	TableScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;

	/*
	 * get information from the estate and scan state
	 */
	scandesc = node->ss.ss_currentScanDesc;
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	slot = node->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		uint32		flags = SO_NONE;

		if (ScanRelIsReadOnly(&node->ss))
			flags |= SO_HINT_REL_READ_ONLY;

		if (estate->es_instrument & INSTRUMENT_IO)
			flags |= SO_SCAN_INSTRUMENT;

		/*
		 * We reach here if the scan is not parallel, or if we're serially
		 * executing a scan that was planned to be parallel.
		 */
		scandesc = table_beginscan(node->ss.ss_currentRelation,
								   estate->es_snapshot,
								   0, NULL, flags);
		node->ss.ss_currentScanDesc = scandesc;
	}

	/*
	 * get the next tuple from the table
	 */
	if (table_scan_getnextslot(scandesc, direction, slot))
		return slot;
	return NULL;
}

/*
 * SeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static pg_always_inline bool
SeqRecheck(SeqScanState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, SeqScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecSeqScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple. This variant is used when there is no es_epq_active, no qual
 *		and no projection.  Passing const-NULLs for these to ExecScanExtended
 *		allows the compiler to eliminate the additional code that would
 *		ordinarily be required for the evaluation of these.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecSeqScan(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	Assert(pstate->state->es_epq_active == NULL);
	Assert(pstate->qual == NULL);
	Assert(pstate->ps_ProjInfo == NULL);

	return ExecScanExtended(&node->ss,
							(ExecScanAccessMtd) SeqNext,
							(ExecScanRecheckMtd) SeqRecheck,
							NULL,
							NULL,
							NULL);
}

/*
 * Variant of ExecSeqScan() but when qual evaluation is required.
 */
static TupleTableSlot *
ExecSeqScanWithQual(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	/*
	 * Use pg_assume() for != NULL tests to make the compiler realize no
	 * runtime check for the field is needed in ExecScanExtended().
	 */
	Assert(pstate->state->es_epq_active == NULL);
	pg_assume(pstate->qual != NULL);
	Assert(pstate->ps_ProjInfo == NULL);

	return ExecScanExtended(&node->ss,
							(ExecScanAccessMtd) SeqNext,
							(ExecScanRecheckMtd) SeqRecheck,
							NULL,
							pstate->qual,
							NULL);
}

static pg_always_inline TupleTableSlot *
ExecSeqScanBatchLoop(PlanState *pstate, ExprState *rest_qual,
					 ProjectionInfo *projInfo, bool use_batch)
{
	SeqScanState *node = castNode(SeqScanState, pstate);
	ExprContext *econtext = pstate->ps_ExprContext;
	TupleTableSlot *result_slot;
	TupleTableSlot *slot;
	ScanDirection direction;
	bool		prepare_batch;
	bool		qual_result;

	Assert(pstate->state->es_epq_active == NULL);
	Assert(projInfo == pstate->ps_ProjInfo);
	Assert(node->batch_state != NULL);
	pg_assume(node->batch_state->scalar_qual != NULL);

	direction = pstate->state->es_direction;
	if (!use_batch)
		reset_batch_qual(node->batch_state);

	/*
	 * This is the no-EPQ path from ExecScanExtended(), kept here so batch
	 * qual evaluation can be added below.
	 */
	ResetExprContext(econtext);
	for (;;)
	{
		slot = ExecScanFetch(&node->ss, NULL,
							 (ExecScanAccessMtd) SeqNext,
							 (ExecScanRecheckMtd) SeqRecheck);
		if (TupIsNull(slot))
		{
			reset_batch_qual(node->batch_state);
			if (projInfo != NULL)
				return ExecClearTuple(projInfo->pi_state.resultslot);
			return slot;
		}

		prepare_batch = false;
		if (!use_batch)
			qual_result = eval_scalar_qual(node->batch_state, slot, econtext);
		else if (!reuse_batch_qual(node->batch_state, slot, &qual_result))
		{
			qual_result = eval_scalar_qual(node->batch_state, slot, econtext);
			prepare_batch = true;
		}

		if (qual_result && rest_qual != NULL)
		{
			econtext->ecxt_scantuple = slot;
			qual_result = ExecQual(rest_qual, econtext);
		}

		result_slot = slot;
		if (qual_result && projInfo != NULL)
		{
			econtext->ecxt_scantuple = slot;
			result_slot = ExecProject(projInfo);
		}

		/* Finish the current tuple before evaluating quals for later tuples. */
		if (prepare_batch)
			prepare_batch_qual(node->batch_state, slot, direction, econtext);

		if (qual_result)
			return result_slot;

		InstrCountFiltered1(node, 1);
		ResetExprContext(econtext);
	}
}

static pg_always_inline TupleTableSlot *
ExecSeqScanBatchDispatch(PlanState *pstate, ExprState *rest_qual,
						 ProjectionInfo *projInfo)
{
	ScanDirection direction = pstate->state->es_direction;

	if (ScanDirectionIsForward(direction) ||
		ScanDirectionIsBackward(direction))
		return ExecSeqScanBatchLoop(pstate, rest_qual, projInfo, true);
	return ExecSeqScanBatchLoop(pstate, rest_qual, projInfo, false);
}

/*
 * Variant of ExecSeqScan() when all scan quals can be evaluated in batches.
 */
static TupleTableSlot *
ExecSeqScanWithBatchQual(PlanState *pstate)
{
	Assert(pstate->qual == NULL);

	return ExecSeqScanBatchDispatch(pstate, NULL, NULL);
}

/*
 * Variant of ExecSeqScan() when batch quals are followed by remaining quals.
 */
static TupleTableSlot *
ExecSeqScanWithBatchQualRest(PlanState *pstate)
{
	pg_assume(pstate->qual != NULL);

	return ExecSeqScanBatchDispatch(pstate, pstate->qual, NULL);
}

/*
 * Variant of ExecSeqScan() when all scan quals can be evaluated in batches
 * and projection is required.
 */
static TupleTableSlot *
ExecSeqScanWithBatchQualProject(PlanState *pstate)
{
	Assert(pstate->qual == NULL);
	pg_assume(pstate->ps_ProjInfo != NULL);

	return ExecSeqScanBatchDispatch(pstate, NULL, pstate->ps_ProjInfo);
}

/*
 * Variant of ExecSeqScan() when batch quals are followed by remaining quals
 * and projection is required.
 */
static TupleTableSlot *
ExecSeqScanWithBatchQualRestProject(PlanState *pstate)
{
	pg_assume(pstate->qual != NULL);
	pg_assume(pstate->ps_ProjInfo != NULL);

	return ExecSeqScanBatchDispatch(pstate, pstate->qual,
									pstate->ps_ProjInfo);
}

/*
 * Variant of ExecSeqScan() but when projection is required.
 */
static TupleTableSlot *
ExecSeqScanWithProject(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	Assert(pstate->state->es_epq_active == NULL);
	Assert(pstate->qual == NULL);
	pg_assume(pstate->ps_ProjInfo != NULL);

	return ExecScanExtended(&node->ss,
							(ExecScanAccessMtd) SeqNext,
							(ExecScanRecheckMtd) SeqRecheck,
							NULL,
							NULL,
							pstate->ps_ProjInfo);
}

/*
 * Variant of ExecSeqScan() but when qual evaluation and projection are
 * required.
 */
static TupleTableSlot *
ExecSeqScanWithQualProject(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	Assert(pstate->state->es_epq_active == NULL);
	pg_assume(pstate->qual != NULL);
	pg_assume(pstate->ps_ProjInfo != NULL);

	return ExecScanExtended(&node->ss,
							(ExecScanAccessMtd) SeqNext,
							(ExecScanRecheckMtd) SeqRecheck,
							NULL,
							pstate->qual,
							pstate->ps_ProjInfo);
}

/*
 * Variant of ExecSeqScan for when EPQ evaluation is required.  We don't
 * bother adding variants of this for with/without qual and projection as
 * EPQ doesn't seem as exciting a case to optimize for.
 */
static TupleTableSlot *
ExecSeqScanEPQ(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) SeqNext,
					(ExecScanRecheckMtd) SeqRecheck);
}

/* ----------------------------------------------------------------
 *		ExecInitSeqScan
 * ----------------------------------------------------------------
 */
SeqScanState *
ExecInitSeqScan(SeqScan *node, EState *estate, int eflags)
{
	SeqScanState *scanstate;

	/*
	 * Once upon a time it was possible to have an outerPlan of a SeqScan, but
	 * not any more.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	scanstate = makeNode(SeqScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * open the scan relation
	 */
	scanstate->ss.ss_currentRelation =
		ExecOpenScanRelation(estate,
							 node->scan.scanrelid,
							 eflags);

	/* and create slot with the appropriate rowtype */
	ExecInitScanTupleSlot(estate, &scanstate->ss,
						  RelationGetDescr(scanstate->ss.ss_currentRelation),
						  table_slot_callbacks(scanstate->ss.ss_currentRelation),
						  TTS_FLAG_OBEYS_NOT_NULL_CONSTRAINTS);

	/*
	 * Initialize result type and projection.
	 */
	ExecInitResultTypeTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	/*
	 * initialize child expressions
	 */
	if (estate->es_epq_active == NULL)
		scanstate->batch_state = init_batch_state(node, &scanstate->ss.ps);
	if (scanstate->batch_state == NULL)
		scanstate->ss.ps.qual =
			ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);

	/*
	 * When EvalPlanQual() is not in use, assign ExecProcNode for this node
	 * based on the presence of batch state, qual and projection. Each
	 * ExecSeqScan*() variant is optimized for the specific combination of
	 * these conditions.
	 */
	if (scanstate->ss.ps.state->es_epq_active != NULL)
		scanstate->ss.ps.ExecProcNode = ExecSeqScanEPQ;
	else if (scanstate->batch_state != NULL)
	{
		if (scanstate->ss.ps.qual == NULL &&
			scanstate->ss.ps.ps_ProjInfo == NULL)
			scanstate->ss.ps.ExecProcNode = ExecSeqScanWithBatchQual;
		else if (scanstate->ss.ps.qual == NULL)
			scanstate->ss.ps.ExecProcNode = ExecSeqScanWithBatchQualProject;
		else if (scanstate->ss.ps.ps_ProjInfo == NULL)
			scanstate->ss.ps.ExecProcNode = ExecSeqScanWithBatchQualRest;
		else
			scanstate->ss.ps.ExecProcNode = ExecSeqScanWithBatchQualRestProject;
	}
	else if (scanstate->ss.ps.qual == NULL)
	{
		if (scanstate->ss.ps.ps_ProjInfo == NULL)
			scanstate->ss.ps.ExecProcNode = ExecSeqScan;
		else
			scanstate->ss.ps.ExecProcNode = ExecSeqScanWithProject;
	}
	else
	{
		if (scanstate->ss.ps.ps_ProjInfo == NULL)
			scanstate->ss.ps.ExecProcNode = ExecSeqScanWithQual;
		else
			scanstate->ss.ps.ExecProcNode = ExecSeqScanWithQualProject;
	}

	return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndSeqScan(SeqScanState *node)
{
	TableScanDesc scanDesc;

	/*
	 * get information from node
	 */
	scanDesc = node->ss.ss_currentScanDesc;

	/*
	 * Collect I/O stats for this process into shared instrumentation.
	 */
	if (node->sinstrument != NULL && IsParallelWorker())
	{
		SeqScanInstrumentation *si;

		Assert(ParallelWorkerNumber < node->sinstrument->num_workers);
		si = &node->sinstrument->sinstrument[ParallelWorkerNumber];

		if (scanDesc && scanDesc->rs_instrument)
		{
			AccumulateIOStats(&si->stats.io, &scanDesc->rs_instrument->io);
		}
	}

	/*
	 * close heap scan
	 */
	if (scanDesc != NULL)
		table_endscan(scanDesc);
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecReScanSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanSeqScan(SeqScanState *node)
{
	TableScanDesc scan;

	if (node->batch_state != NULL)
		reset_batch_qual(node->batch_state);

	scan = node->ss.ss_currentScanDesc;

	if (scan != NULL)
		table_rescan(scan,		/* scan desc */
					 NULL);		/* new scan keys */

	ExecScanReScan((ScanState *) node);
}

/* ----------------------------------------------------------------
 *						Parallel Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecSeqScanEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanEstimate(SeqScanState *node,
					ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;

	node->pscan_len = table_parallelscan_estimate(node->ss.ss_currentRelation,
												  estate->es_snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, node->pscan_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeDSM
 *
 *		Set up a parallel heap scan descriptor.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeDSM(SeqScanState *node,
						 ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;
	ParallelTableScanDesc pscan;
	uint32		flags = SO_NONE;

	if (ScanRelIsReadOnly(&node->ss))
		flags |= SO_HINT_REL_READ_ONLY;

	if (estate->es_instrument & INSTRUMENT_IO)
		flags |= SO_SCAN_INSTRUMENT;

	pscan = shm_toc_allocate(pcxt->toc, node->pscan_len);
	table_parallelscan_initialize(node->ss.ss_currentRelation,
								  pscan,
								  estate->es_snapshot);
	shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, pscan);

	node->ss.ss_currentScanDesc =
		table_beginscan_parallel(node->ss.ss_currentRelation, pscan, flags);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanReInitializeDSM(SeqScanState *node,
						   ParallelContext *pcxt)
{
	ParallelTableScanDesc pscan;

	pscan = node->ss.ss_currentScanDesc->rs_parallel;
	table_parallelscan_reinitialize(node->ss.ss_currentRelation, pscan);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeWorker(SeqScanState *node,
							ParallelWorkerContext *pwcxt)
{
	ParallelTableScanDesc pscan;
	uint32		flags = SO_NONE;

	if (ScanRelIsReadOnly(&node->ss))
		flags |= SO_HINT_REL_READ_ONLY;

	if (node->ss.ps.state->es_instrument & INSTRUMENT_IO)
		flags |= SO_SCAN_INSTRUMENT;

	pscan = shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, false);
	node->ss.ss_currentScanDesc =
		table_beginscan_parallel(node->ss.ss_currentRelation, pscan, flags);
}

/*
 * Compute the amount of space we'll need for the shared instrumentation and
 * inform pcxt->estimator.
 */
void
ExecSeqScanInstrumentEstimate(SeqScanState *node, ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;
	Size		size;

	if ((estate->es_instrument & INSTRUMENT_IO) == 0 || pcxt->nworkers == 0)
		return;

	size = add_size(offsetof(SharedSeqScanInstrumentation, sinstrument),
					mul_size(pcxt->nworkers, sizeof(SeqScanInstrumentation)));

	shm_toc_estimate_chunk(&pcxt->estimator, size);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/*
 * Set up parallel sequential scan instrumentation.
 */
void
ExecSeqScanInstrumentInitDSM(SeqScanState *node, ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;
	SharedSeqScanInstrumentation *sinstrument;
	Size		size;

	if ((estate->es_instrument & INSTRUMENT_IO) == 0 || pcxt->nworkers == 0)
		return;

	size = add_size(offsetof(SharedSeqScanInstrumentation, sinstrument),
					mul_size(pcxt->nworkers, sizeof(SeqScanInstrumentation)));
	sinstrument = shm_toc_allocate(pcxt->toc, size);
	memset(sinstrument, 0, size);
	sinstrument->num_workers = pcxt->nworkers;
	shm_toc_insert(pcxt->toc,
				   node->ss.ps.plan->plan_node_id +
				   PARALLEL_KEY_SCAN_INSTRUMENT_OFFSET,
				   sinstrument);
	node->sinstrument = sinstrument;
}

/*
 * Look up and save the location of the shared instrumentation.
 */
void
ExecSeqScanInstrumentInitWorker(SeqScanState *node,
								ParallelWorkerContext *pwcxt)
{
	EState	   *estate = node->ss.ps.state;

	if ((estate->es_instrument & INSTRUMENT_IO) == 0)
		return;

	node->sinstrument = shm_toc_lookup(pwcxt->toc,
									   node->ss.ps.plan->plan_node_id +
									   PARALLEL_KEY_SCAN_INSTRUMENT_OFFSET,
									   false);
}

/*
 * Transfer sequential scan instrumentation from DSM to private memory.
 */
void
ExecSeqScanRetrieveInstrumentation(SeqScanState *node)
{
	SharedSeqScanInstrumentation *sinstrument = node->sinstrument;
	Size		size;

	if (sinstrument == NULL)
		return;

	size = offsetof(SharedSeqScanInstrumentation, sinstrument)
		+ sinstrument->num_workers * sizeof(SeqScanInstrumentation);

	node->sinstrument = palloc(size);
	memcpy(node->sinstrument, sinstrument, size);
}
