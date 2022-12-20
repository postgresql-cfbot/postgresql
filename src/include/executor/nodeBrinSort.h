/*-------------------------------------------------------------------------
 *
 * nodeBrinSort.h
 *
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeBrinSort.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEBRIN_SORT_H
#define NODEBRIN_SORT_H

#include "access/genam.h"
#include "access/parallel.h"
#include "nodes/execnodes.h"

extern BrinSortState *ExecInitBrinSort(BrinSort *node, EState *estate, int eflags);
extern void ExecEndBrinSort(BrinSortState *node);
extern void ExecBrinSortMarkPos(BrinSortState *node);
extern void ExecBrinSortRestrPos(BrinSortState *node);
extern void ExecReScanBrinSort(BrinSortState *node);
extern void ExecBrinSortEstimate(BrinSortState *node, ParallelContext *pcxt);
extern void ExecBrinSortInitializeDSM(BrinSortState *node, ParallelContext *pcxt);
extern void ExecBrinSortReInitializeDSM(BrinSortState *node, ParallelContext *pcxt);
extern void ExecBrinSortInitializeWorker(BrinSortState *node,
										  ParallelWorkerContext *pwcxt);

/*
 * These routines are exported to share code with nodeIndexonlyscan.c and
 * nodeBitmapBrinSort.c
 */
extern void ExecIndexBuildScanKeys(PlanState *planstate, Relation index,
								   List *quals, bool isorderby,
								   ScanKey *scanKeys, int *numScanKeys,
								   IndexRuntimeKeyInfo **runtimeKeys, int *numRuntimeKeys,
								   IndexArrayKeyInfo **arrayKeys, int *numArrayKeys);
extern void ExecIndexEvalRuntimeKeys(ExprContext *econtext,
									 IndexRuntimeKeyInfo *runtimeKeys, int numRuntimeKeys);
extern bool ExecIndexEvalArrayKeys(ExprContext *econtext,
								   IndexArrayKeyInfo *arrayKeys, int numArrayKeys);
extern bool ExecIndexAdvanceArrayKeys(IndexArrayKeyInfo *arrayKeys, int numArrayKeys);

#endif							/* NODEBRIN_SORT_H */
