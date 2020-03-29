/*--------------------------------------------------------------------
 * execAsync.c
 *		Support functions for asynchronous query execution
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/backend/executor/execAsync.c
 *--------------------------------------------------------------------
 */
#ifndef EXECASYNC_H
#define EXECASYNC_H

#include "nodes/execnodes.h"
#include "storage/latch.h"

extern bool ExecAsyncConfigureWait(WaitEventSet *wes, PlanState *node,
								   void *data, bool reinit);
extern Bitmapset *ExecAsyncEventWait(PlanState **nodes, Bitmapset *waitnodes,
									 long timeout);
#endif   /* EXECASYNC_H */
