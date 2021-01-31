/*-------------------------------------------------------------------------
 *
 * nodeTidrangescan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * src/include/executor/nodeTidrangescan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODETIDRANGESCAN_H
#define NODETIDRANGESCAN_H

#include "nodes/execnodes.h"

extern TidRangeScanState *ExecInitTidRangeScan(TidRangeScan *node,
											   EState *estate, int eflags);
extern void ExecEndTidRangeScan(TidRangeScanState *node);
extern void ExecReScanTidRangeScan(TidRangeScanState *node);

#endif							/* NODETIDRANGESCAN_H */
