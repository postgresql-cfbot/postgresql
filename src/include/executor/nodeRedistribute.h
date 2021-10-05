#ifndef NODEREDISTRIBUTE_H
#define NODEREDISTRIBUTE_H

#include "access/parallel.h"
#include "nodes/execnodes.h"

extern RedistributeState *ExecInitRedistribute(Redistribute *node, EState *estate, int eflags);
extern void ExecEndRedistribute(RedistributeState *node);
extern void ExecReScanRedistribute(RedistributeState *node);

/* parallel scan support */
extern void ExecRedistributeEstimate(RedistributeState *node, ParallelContext *pcxt);
extern void ExecRedistributeInitializeDSM(RedistributeState *node, ParallelContext *pcxt);
extern void ExecRedistributeReInitializeDSM(RedistributeState *node, ParallelContext *pcxt);
extern void ExecRedistributeInitializeWorker(RedistributeState *node,
											 ParallelWorkerContext *pwcxt);
extern void ExecRedistributeParallelLaunched(RedistributeState *node,
											 ParallelContext *pcxt);
extern void ExecRedistributeRetrieveInstrumentation(RedistributeState *node);

#endif							/* NODEREDISTRIBUTE_H */
