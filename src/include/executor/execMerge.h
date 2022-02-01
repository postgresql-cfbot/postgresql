/*-------------------------------------------------------------------------
 *
 * execMerge.h
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/execMerge.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXECMERGE_H
#define EXECMERGE_H

#include "nodes/execnodes.h"

/* flags for mt_merge_subcommands */
#define MERGE_INSERT	0x01
#define MERGE_UPDATE	0x02
#define MERGE_DELETE	0x04


typedef struct ExecMergeActionInfo
{
	MergeActionState *actionState;
	TM_Result	result;
	TM_FailureData failureData;
	bool		updated;
	bool		deleted;
} ExecMergeActionInfo;


extern TupleTableSlot *ExecMerge(ModifyTableState *mtstate,
								 ResultRelInfo *resultRelInfo,
								 EState *estate, TupleTableSlot *slot);

extern void ExecInitMerge(ModifyTableState *mtstate, EState *estate);

extern void ExecInitMergeProjection(ModifyTableState *mtstate,
									ResultRelInfo *resultRelInfo);

#endif							/* NODEMERGE_H */
