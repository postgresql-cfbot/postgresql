
#ifndef NODE_BATCH_SORT_H
#define NODE_BATCH_SORT_H

#include "access/parallel.h"
#include "nodes/execnodes.h"

extern BatchSortState *ExecInitBatchSort(BatchSort *node, EState *estate, int eflags);
extern void ExecEndBatchSort(BatchSortState *node);
extern void ExecReScanBatchSort(BatchSortState *node);

/* parallel scan support */
extern void ExecBatchSortEstimate(BatchSortState *node, ParallelContext *pcxt);
extern void ExecBatchSortInitializeDSM(BatchSortState *node, ParallelContext *pcxt);
extern void ExecBatchSortReInitializeDSM(BatchSortState *node, ParallelContext *pcxt);
extern void ExecBatchSortInitializeWorker(BatchSortState *node, ParallelWorkerContext *pwcxt);
extern void ExecShutdownBatchSort(BatchSortState *node);

#endif							/* NODE_BATCH_SORT_H */