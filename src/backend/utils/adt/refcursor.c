/*-------------------------------------------------------------------------
 *
 * refcursor.c
 *
 * IDENTIFICATION
 *       src/backend/utils/adt/refcursor.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "nodes/nodeFuncs.h"
#include "nodes/supportnodes.h"
#include "optimizer/optimizer.h"
#include "tcop/pquery.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/refcursor.h"
#include "utils/typcache.h"


typedef struct SingleSlotDestReceiver
{
	DestReceiver	pub;
	TupleTableSlot *received_slot;
	TupleDesc		tupdesc;
} SingleSlotDestReceiver;

/*
 * sqlfunction_startup --- executor startup
 */
static void
ssdr_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	SingleSlotDestReceiver *myState = (SingleSlotDestReceiver *) self;

	myState->tupdesc = typeinfo;
}

/*
 * sqlfunction_receive --- receive one tuple
 */
static bool
ssdr_receive(TupleTableSlot *slot, DestReceiver *self)
{
	SingleSlotDestReceiver *myState = (SingleSlotDestReceiver *) self;

	myState->received_slot = slot;

	return true;
}

/*
 * sqlfunction_shutdown --- executor end
 */
static void
ssdr_shutdown(DestReceiver *self)
{
	/* no-op */
}

/*
 * sqlfunction_destroy --- release DestReceiver object
 */
static void
ssdr_destroy(DestReceiver *self)
{
	pfree(self);
}

/*
 * CreateSingleSlotDestReceiver -- create a DestReceiver
 * that acquires a single tupleslot
 */
static DestReceiver *
CreateSingleSlotDestReceiver(void)
{
	SingleSlotDestReceiver *self = (SingleSlotDestReceiver *) palloc(sizeof(SingleSlotDestReceiver));

	self->pub.receiveSlot = ssdr_receive;
	self->pub.rStartup = ssdr_startup;
	self->pub.rShutdown = ssdr_shutdown;
	self->pub.rDestroy = ssdr_destroy;
	self->pub.mydest = -1;

	/* private fields will be set by ssdr_startup */

	return (DestReceiver *) self;
}

/*
 * ROWS_IN (REFCURSOR)
 */
Datum
rows_in_refcursor(PG_FUNCTION_ARGS)
{
	typedef struct
	{
		Portal					portal;
		SingleSlotDestReceiver *dest;
		TupleDesc				tupdesc;
	} rows_in_refcursor_fctx;

	ReturnSetInfo		   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	FuncCallContext 	   *funcctx;
	rows_in_refcursor_fctx *fctx;
	char 				   *portal_name;
	FetchDirection			direction;
	uint64					howMany;
	uint64					nfetched;
	bool					first_call;
	MemoryContext			oldcontext;
	HeapTuple				tuple;
	Datum					datum;
	HeapTupleHeader			result;

	/* stuff done only on the first call of the function */
	first_call = SRF_IS_FIRSTCALL();
	if (first_call)
	{
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* Check to see if caller supports us returning a set */
		if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("set-valued function called in context that cannot accept a set")));
		if (!(rsinfo->allowedModes & SFRM_ValuePerCall))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("value per call mode required, but it is not " \
							"allowed in this context")));

		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* allocate memory for user context */
		fctx = (rows_in_refcursor_fctx *) palloc(sizeof(rows_in_refcursor_fctx));

		fctx->dest = (SingleSlotDestReceiver *) CreateSingleSlotDestReceiver();

		MemoryContextSwitchTo(oldcontext);

		portal_name = text_to_cstring(PG_GETARG_TEXT_PP(0));

		fctx->portal = GetPortalByName(portal_name);

		/* Check that the portal exists */
		if (!PortalIsValid(fctx->portal))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_CURSOR),
					 errmsg("cursor \"%s\" does not exist", portal_name)));

		/* ensure the Portal is ready (has already been OPEN'ed) */
		if (! (fctx->portal->status == PORTAL_DEFINED ||
			   fctx->portal->status == PORTAL_READY))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_CURSOR_STATE),
					 errmsg("cursor \"%s\" is not OPEN", portal_name)));

		/*
		 * Ensure the Portal returns some results (so is not a utility
		 * command, or set of multiple statements.
		 */
		if (! (fctx->portal->strategy == PORTAL_ONE_SELECT ||
			   fctx->portal->strategy == PORTAL_ONE_RETURNING ||
			   fctx->portal->strategy == PORTAL_ONE_MOD_WITH))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_CURSOR_DEFINITION),
					 errmsg("cursor \"%s\" does not return result set", portal_name)));

		pfree(portal_name);

		funcctx->user_fctx = fctx;
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	fctx = funcctx->user_fctx;

	rsinfo->returnMode = SFRM_ValuePerCall;

	if (first_call)
	{
		direction = FETCH_ABSOLUTE; /* start from absolute position */
		howMany = 1; /* position = 1 (top); also reads one row */
	}
	else
	{
		direction = FETCH_FORWARD; /* othrewise advance forward */
		howMany = 1; /* count = 1 (read one row) */
	}

	/* Run the cursor... */
	nfetched = PortalRunFetch (fctx->portal, direction, howMany,
							   (DestReceiver *) fctx->dest);

	/*
	 * Initialise the Tuple Desriptor. (This can't be done until
	 * we have done our first fetch.)
	 */
	if (first_call)
	{
		MemoryContext per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
		MemoryContext oldcontext = MemoryContextSwitchTo(per_query_ctx);

		fctx->tupdesc = CreateTupleDescCopy (fctx->dest->tupdesc);

		/* For RECORD results, make sure a typmod has been assigned */
		if (fctx->tupdesc->tdtypeid == RECORDOID &&
			fctx->tupdesc->tdtypmod < 0)
			assign_record_type_typmod(fctx->tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	rsinfo->setDesc = fctx->tupdesc;

	Assert (nfetched <= 1);

	if (nfetched == 1)
	{
		/*
		 * Convert the TableTupleSlot to a HeapTuple (doesn't
		 * materialise and doesn't copy unless unavoidable).
		 */
		tuple = ExecFetchSlotHeapTuple (fctx->dest->received_slot,
												  /* materialise */ false,
												  NULL);

		/*
		 * Avoid making a copy if the HeapTuple is already
		 * fully in memory and marked with correct typeid/typmod.
		 */
		datum = PointerGetDatum (tuple->t_data);
		if (HeapTupleHasExternal(tuple) ||
			HeapTupleHeaderGetTypeId(tuple->t_data) != fctx->tupdesc->tdtypeid ||
			HeapTupleHeaderGetTypMod(tuple->t_data) != fctx->tupdesc->tdtypmod)
		{
			/*
			 *  Copy the tuple as a Datum, ensuring it is
			 *  fully in memory in the process.
			 */
			datum = heap_copy_tuple_as_datum (tuple, fctx->tupdesc);
		}

		/*
		 * Obtain HeapTupleHeader for the Datum, which is in
		 * memory, so should not require a copy.
		 */
		result = DatumGetHeapTupleHeader (datum);

		SRF_RETURN_NEXT (funcctx, PointerGetDatum (result));
	}
	else /* no rows retrieved */
	{
		 /* it will have been pfree()'ed by ssdr_destroy() */
		fctx->dest = NULL;

		/* fctx itself will be released when multi_call_memory_ctx goes. */

		SRF_RETURN_DONE(funcctx);
	}
}


/*
 * Planner support function for ROWS_IN (REFCURSOR)
 */
Datum
rows_in_refcursor_support(PG_FUNCTION_ARGS)
{
	Node			   *rawreq = (Node *) PG_GETARG_POINTER(0);
	Node			   *ret;
	Node 			   *req_node;
	SupportRequestRows *rows_req = NULL; /* keep compiler happy */
	SupportRequestCost *cost_req = NULL; /* keep compiler happy */
	List			   *args;
	Node			   *arg1;
	Const			   *cexpr;
	char			   *portal_name;
	Portal				portal;
	QueryDesc		   *qdesc;
	PlanState		   *planstate;
	Oid					typoutput;
	bool				typIsVarlena;

	if (IsA(rawreq, SupportRequestRows))
	{
		rows_req = (SupportRequestRows *) rawreq;

		req_node = rows_req->node;
	}
	else if (IsA (rawreq, SupportRequestCost))
	{
		cost_req = (SupportRequestCost *) rawreq;

		req_node = cost_req->node;
	}
	else
		PG_RETURN_POINTER(NULL);

	/* The call to ROWS_IN should be in a FuncExpr node. */
	if (!is_funcclause(req_node))
		PG_RETURN_POINTER(NULL);

	args = ((FuncExpr *) req_node)->args;
	if (args == NULL)
		PG_RETURN_POINTER(NULL);

	arg1 = linitial(args);

	/*
	 * We can only estimate the cost if the REFCURSOR is
	 * already simplified to a Const.
	 */
	if (!IsA (arg1, Const))
		PG_RETURN_POINTER(NULL);

	cexpr = (Const *) arg1;

	if (cexpr->constisnull)
		PG_RETURN_POINTER(NULL);

	if (cexpr->consttype != REFCURSOROID)
		PG_RETURN_POINTER(NULL);

	/*
	 * We can ignore a check on the collation because we are not
	 * interested in sorting, and typemod because REFCURSOR has
	 * no modifyable attributes.
	 */
	getTypeOutputInfo(cexpr->consttype, &typoutput, &typIsVarlena);

	portal_name = OidOutputFunctionCall(typoutput, cexpr->constvalue);

	portal = GetPortalByName(portal_name);

	/* Check that the portal exists */
	if (!PortalIsValid(portal))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CURSOR),
				 errmsg("cursor \"%s\" does not exist", portal_name)));

	/* ensure the Portal is ready (has already been OPEN'ed) */
	if (! (portal->status == PORTAL_DEFINED ||
		   portal->status == PORTAL_READY))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
				 errmsg("cursor \"%s\" is not OPEN", portal_name)));

	/*
	 * Ensure the Portal returns some results (so is
	 * not a utility command, or set of multiple statements.
	 */
	if (! (portal->strategy == PORTAL_ONE_SELECT ||
		   portal->strategy == PORTAL_ONE_RETURNING ||
		   portal->strategy == PORTAL_ONE_MOD_WITH))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_DEFINITION),
				 errmsg("cursor \"%s\" does not return result set", portal_name)));

	qdesc = portal->queryDesc;
	if (qdesc == NULL)
		PG_RETURN_POINTER(NULL);

	planstate = qdesc->planstate;
	if (planstate == NULL)
		PG_RETURN_POINTER(NULL);

	if (rows_req)
	{
		rows_req->rows = planstate->plan->plan_rows;
	}
	else if (cost_req)
	{
		cost_req->startup = planstate->plan->startup_cost;
		cost_req->per_tuple = (planstate->plan->total_cost - planstate->plan->startup_cost);
		if (planstate->plan->plan_rows != 0.0)
			cost_req->per_tuple /= planstate->plan->plan_rows;
	}

	ret = (Node *) rawreq;

	PG_RETURN_POINTER(ret);
}
