/*-------------------------------------------------------------------------
 *
 * execAsync.c
 *	  Support routines for asynchronous execution.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/execAsync.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/execAsync.h"
#include "executor/nodeAppend.h"
#include "executor/nodeForeignscan.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

/*
 * ExecAsyncConfigureWait: Add wait event to the WaitEventSet if needed.
 *
 * If reinit is true, the caller didn't reuse existing WaitEventSet.
 */
bool
ExecAsyncConfigureWait(WaitEventSet *wes, PlanState *node,
					   void *data, bool reinit)
{
	switch (nodeTag(node))
	{
	case T_ForeignScanState:
		return ExecForeignAsyncConfigureWait((ForeignScanState *)node,
											 wes, data, reinit);
		break;
	default:
			elog(ERROR, "unrecognized node type: %d",
				(int) nodeTag(node));
	}
}

/*
 * struct for memory context callback argument used in ExecAsyncEventWait
 */
typedef struct {
	int **p_refind;
	int *p_refindsize;
} ExecAsync_mcbarg;

/*
 * callback function to reset static variables pointing to the memory in
 * TopTransactionContext in ExecAsyncEventWait.
 */
static void ExecAsyncMemoryContextCallback(void *arg)
{
	/* arg is the address of the variable refind in ExecAsyncEventWait */
	ExecAsync_mcbarg *mcbarg = (ExecAsync_mcbarg *) arg;
	*mcbarg->p_refind = NULL;
	*mcbarg->p_refindsize = 0;
}

#define EVENT_BUFFER_SIZE 16

/*
 * ExecAsyncEventWait:
 *
 * Wait for async events to fire. Returns the Bitmapset of fired events.
 */
Bitmapset *
ExecAsyncEventWait(PlanState **nodes, Bitmapset *waitnodes, long timeout)
{
	static int *refind = NULL;
	static int refindsize = 0;
	WaitEventSet *wes;
	WaitEvent   occurred_event[EVENT_BUFFER_SIZE];
	int noccurred = 0;
	Bitmapset *fired_events = NULL;
	int i;
	int n;

	n = bms_num_members(waitnodes);
	wes = CreateWaitEventSet(TopTransactionContext,
							 TopTransactionResourceOwner, n);
	if (refindsize < n)
	{
		if (refindsize == 0)
			refindsize = EVENT_BUFFER_SIZE; /* XXX */
		while (refindsize < n)
			refindsize *= 2;
		if (refind)
			refind = (int *) repalloc(refind, refindsize * sizeof(int));
		else
		{
			static ExecAsync_mcbarg mcb_arg =
				{ &refind, &refindsize };
			static MemoryContextCallback mcb =
				{ ExecAsyncMemoryContextCallback, (void *)&mcb_arg, NULL };
			MemoryContext oldctxt =
				MemoryContextSwitchTo(TopTransactionContext);

			/*
			 * refind points to a memory block in
			 * TopTransactionContext. Register a callback to reset it.
			 */
			MemoryContextRegisterResetCallback(TopTransactionContext, &mcb);
			refind = (int *) palloc(refindsize * sizeof(int));
			MemoryContextSwitchTo(oldctxt);
		}
	}

	/* Prepare WaitEventSet for waiting on the waitnodes. */
	n = 0;
	for (i = bms_next_member(waitnodes, -1) ; i >= 0 ;
		 i = bms_next_member(waitnodes, i))
	{
		refind[i] = i;
		if (ExecAsyncConfigureWait(wes, nodes[i], refind + i, true))
			n++;
	}

	/* Return immediately if no node to wait. */
	if (n == 0)
	{
		FreeWaitEventSet(wes);
		return NULL;
	}

	noccurred = WaitEventSetWait(wes, timeout, occurred_event,
								 EVENT_BUFFER_SIZE,
								 WAIT_EVENT_ASYNC_WAIT);
	FreeWaitEventSet(wes);
	if (noccurred == 0)
		return NULL;

	for (i = 0 ; i < noccurred ; i++)
	{
		WaitEvent *w = &occurred_event[i];

		if ((w->events & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)) != 0)
		{
			int n = *(int*)w->user_data;

			fired_events = bms_add_member(fired_events, n);
		}
	}

	return fired_events;
}
