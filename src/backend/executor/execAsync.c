/*-------------------------------------------------------------------------
 *
 * execAsync.c
 *	  Support routines for asynchronous execution
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/execAsync.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/execAsync.h"
#include "executor/nodeForeignscan.h"

/*
 * Begin execution of a designed async-aware node.
 */
void
ExecAsyncBegin(AsyncRequest *areq)
{
	switch (nodeTag(areq->requestee))
	{
		case T_ForeignScanState:
			ExecAsyncForeignScanBegin(areq);
			break;
		default:
			/* If the node doesn't support async, caller messed up. */
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(areq->requestee));
	}
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
}

/*
 * Asynchronously request a tuple from the asynchronous node.
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
}

/*
 * A requestee node should call this function to indicate that it needs a
 * callback to deliver tuples to its requestor node.  The node can call this
 * from its ExecAsyncBegin, ExecAsyncNotify, or ExecAsyncRequest callback.
 */
void
ExecAsyncMarkAsNeedingCallback(AsyncRequest *areq)
{
	areq->callback_pending = true;
	areq->request_complete = false;
	areq->result = NULL;
}

/*
 * A requestee node should call this function to deliver the tuple to its
 * requestor node.  The node can call this from its ExecAsyncRequest callback
 * if the requested tuple is available immediately.
 */
void
ExecAsyncRequestDone(AsyncRequest *areq, TupleTableSlot *result)
{
	areq->callback_pending = false;
	areq->request_complete = true;
	areq->result = result;
}
