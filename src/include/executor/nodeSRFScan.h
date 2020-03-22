/*-------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *	  src/include/executor/nodeSRFScan.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef nodeSRFScan_h
#define nodeSRFScan_h

#include "nodes/execnodes.h"

typedef struct
{
	ScanState		ss;					/* its first field is NodeTag */
	SetExprState 	*setexpr;			/* state of the expression being evaluated */
	ExprDoneCond	elemdone;
	int				colcount;			/* # of columns */
	bool			tupdesc_checked;	/* has the return tupdesc been checked? */
	MemoryContext 	argcontext;			/* context for SRF arguments */
	PlanState		*parent;			/* the plan's parent node */
} SRFScanState;

extern SRFScanState *ExecInitSRFScan(SRFScanPlan *node, EState *estate, int eflags);
extern void ExecEndSRFScan(SRFScanState *node);
extern void ExecReScanSRF(SRFScanState *node);
extern bool ExecSRFDonateResultTuplestore(SetExprState *fcache);

#endif /* nodeSRFScan_h */
