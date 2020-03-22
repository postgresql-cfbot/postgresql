/*-------------------------------------------------------------------------
 *
 * nodeFunctionscan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeFunctionscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEFUNCTIONSCAN_H
#define NODEFUNCTIONSCAN_H

#include "nodes/execnodes.h"

/*
 * Runtime data for each function being scanned.
 */
typedef struct FunctionScanPerFuncState
{
	int			colcount;		/* expected number of result columns */
	int64		rowcount;		/* # of rows in result set, -1 if not known */
	ScanState  *scanstate;		/* scan node: either SRFScan or Materialize */
} FunctionScanPerFuncState;

extern FunctionScanState *ExecInitFunctionScan(FunctionScan *node, EState *estate, int eflags);
extern void ExecEndFunctionScan(FunctionScanState *node);
extern void ExecReScanFunctionScan(FunctionScanState *node);

#endif							/* NODEFUNCTIONSCAN_H */
