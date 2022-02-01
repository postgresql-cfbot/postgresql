/*-------------------------------------------------------------------------
 *
 * nodeModifyTable.h
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeModifyTable.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEMODIFYTABLE_H
#define NODEMODIFYTABLE_H

#include "access/tableam.h"
#include "executor/execMerge.h"
#include "nodes/execnodes.h"

extern void ExecComputeStoredGenerated(ResultRelInfo *resultRelInfo,
									   EState *estate, TupleTableSlot *slot,
									   CmdType cmdtype);

extern ModifyTableState *ExecInitModifyTable(ModifyTable *node, EState *estate, int eflags);
extern void ExecEndModifyTable(ModifyTableState *node);
extern void ExecReScanModifyTable(ModifyTableState *node);
extern TupleTableSlot *ExecPrepareTupleRouting(ModifyTableState *mtstate,
											   EState *estate,
											   struct PartitionTupleRouting *proute,
											   ResultRelInfo *targetRelInfo,
											   TupleTableSlot *slot,
											   ResultRelInfo **partRelInfo);
extern TupleTableSlot *ExecDelete(ModifyTableState *mtstate,
								  ResultRelInfo *resultRelInfo,
								  ItemPointer tupleid, HeapTuple oldtuple, TupleTableSlot *planSlot,
								  EPQState *epqstate, EState *estate,
								  bool processReturning,
								  bool canSetTag, bool changingPart,
								  bool *tupleDeleted,
								  ExecMergeActionInfo *actionState,
								  TupleTableSlot **epqslot);
extern TupleTableSlot *ExecUpdate(ModifyTableState *mtstate,
								  ResultRelInfo *resultRelInfo,
								  ItemPointer tupleid, HeapTuple oldtuple, TupleTableSlot *slot,
								  TupleTableSlot *planSlot, EPQState *epqstate, EState *estate,
								  ExecMergeActionInfo *actionInfo, bool canSetTag);
extern TupleTableSlot *ExecInsert(ModifyTableState *mtstate,
								  ResultRelInfo *resultRelInfo,
								  TupleTableSlot *slot,
								  TupleTableSlot *planSlot,
								  EState *estate,
								  ExecMergeActionInfo *actionInfo,
								  bool canSetTag);
extern void ExecCheckPlanOutput(Relation resultRel, List *targetList);

#endif							/* NODEMODIFYTABLE_H */
