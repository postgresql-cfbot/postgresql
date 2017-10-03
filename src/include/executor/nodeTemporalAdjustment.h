/*-------------------------------------------------------------------------
 *
 * nodeTemporalAdjustment.h
 *
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeLimit.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODETEMPORALADJUSTMENT_H
#define NODETEMPORALADJUSTMENT_H

#include "nodes/execnodes.h"

extern TemporalAdjustmentState *ExecInitTemporalAdjustment(TemporalAdjustment *node, EState *estate, int eflags);
extern TupleTableSlot *ExecTemporalAdjustment(TemporalAdjustmentState *node);
extern void ExecEndTemporalAdjustment(TemporalAdjustmentState *node);
extern void ExecReScanTemporalAdjustment(TemporalAdjustmentState *node);

#endif   /* NODETEMPORALADJUSTMENT_H */
