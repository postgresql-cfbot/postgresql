/*-------------------------------------------------------------------------
 *
 * execAsync.c
 *	  Support routines for asynchronous execution
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
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

static void ExecAsyncResponse(AsyncRequest *areq);

/*
 * Asynchronously request a tuple from a designed async-capable node.
 */
void
ExecAsyncRequest(AsyncRequest *areq)
{
	switch (nodeTag(areq->requestee))
	{
		case T_ForeignScanState:
			ExecAsyncForeignScanRequest(areq);
			break;
		default:
			/* If the node doesn't support async, caller messed up. */
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(areq->requestee));
	}

	ExecAsyncResponse(areq);
}

/*
 * Give the asynchronous node a chance to configure the file descriptor event
 * for which it wishes to wait.  We expect the node-type specific callback to
 * make a sigle call of the following form:
 *
 * AddWaitEventToSet(set, WL_SOCKET_READABLE, fd, NULL, areq);
 */
void
ExecAsyncConfigureWait(AsyncRequest *areq)
{
	switch (nodeTag(areq->requestee))
	{
		case T_ForeignScanState:
			ExecAsyncForeignScanConfigureWait(areq);
			break;
		default:
			/* If the node doesn't support async, caller messed up. */
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(areq->requestee));
	}
}

/*
 * Call the asynchronous node back when a relevant event has occurred.
 */
void
ExecAsyncNotify(AsyncRequest *areq)
{
	switch (nodeTag(areq->requestee))
	{
		case T_ForeignScanState:
			ExecAsyncForeignScanNotify(areq);
			break;
		default:
			/* If the node doesn't support async, caller messed up. */
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(areq->requestee));
	}

	ExecAsyncResponse(areq);
}

/*
 * Call the requestor back when an asynchronous node has produced a result.
 */
static void
ExecAsyncResponse(AsyncRequest *areq)
{
	switch (nodeTag(areq->requestor))
	{
		case T_AppendState:
			ExecAsyncAppendResponse(areq);
			break;
		default:
			/* If the node doesn't support async, caller messed up. */
			elog(ERROR, "unrecognized node type: %d",
				(int) nodeTag(areq->requestor));
	}
}

/*
 * A requestee node should call this function to deliver the tuple to its
 * requestor node.  The node can call this from its ExecAsyncRequest callback
 * if the requested tuple is available immediately.
 */
void
ExecAsyncRequestDone(AsyncRequest *areq, TupleTableSlot *result)
{
	areq->request_complete = true;
	areq->result = result;
}
