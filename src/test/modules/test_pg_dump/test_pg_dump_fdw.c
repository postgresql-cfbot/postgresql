#include "postgres.h"

#include "catalog/pg_type.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"

static int curr_row = 0;

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_pg_dump_fdw_handler);

static void dumptestGetForeignRelSize(PlannerInfo *root,
									  RelOptInfo *baserel,
									  Oid foreigntableid);
static void dumptestGetForeignPaths(PlannerInfo *root,
									RelOptInfo *baserel,
									Oid foreigntableid);
static ForeignScan * dumptestGetForeignPlan(PlannerInfo *root,
								   RelOptInfo *baserel,
								   Oid foreigntableid,
								   ForeignPath *best_path,
								   List *tlist,
								   List *scan_clauses,
								   Plan *outer_plan);
static void dumptestBeginForeignScan(ForeignScanState *node,
									 int eflags);
static TupleTableSlot * dumptestIterateForeignScan(ForeignScanState *node);
static void dumptestReScanForeignScan(ForeignScanState *node);
static void dumptestEndForeignScan(ForeignScanState *node);

/*
 * Handler function
 */
Datum
test_pg_dump_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	fdwroutine->GetForeignRelSize = dumptestGetForeignRelSize;
	fdwroutine->GetForeignPaths = dumptestGetForeignPaths;
	fdwroutine->GetForeignPlan = dumptestGetForeignPlan;
	fdwroutine->BeginForeignScan = dumptestBeginForeignScan;
	fdwroutine->IterateForeignScan = dumptestIterateForeignScan;
	fdwroutine->ReScanForeignScan = dumptestReScanForeignScan;
	fdwroutine->EndForeignScan = dumptestEndForeignScan;

	PG_RETURN_POINTER(fdwroutine);
}

static void
dumptestGetForeignRelSize(PlannerInfo *root,
						  RelOptInfo *baserel,
						  Oid foreigntableid)
{
	baserel->rows = 1;
}

static void
dumptestGetForeignPaths(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid)
{
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
			 						 NULL /* default pathtarget */,
									 baserel->rows,
									 1,
									 1,
									 NIL,
									 baserel->lateral_relids,
									 NULL,
									 NIL));
}

static ForeignScan *
dumptestGetForeignPlan(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Oid foreigntableid,
					   ForeignPath *best_path,
					   List *tlist,
					   List *scan_clauses,
					   Plan *outer_plan)
{
	scan_clauses = extract_actual_clauses(scan_clauses, false);
	
	return make_foreignscan(tlist,
							scan_clauses,
							baserel->relid,
							NIL,
							best_path->fdw_private,
							NIL,
							NIL,
							outer_plan);
}

static void
dumptestBeginForeignScan(ForeignScanState *node, int eflags)
{
	TupleDesc		desc;

	desc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;

	for (int i = 0; i < desc->natts; i++)
	{
		if (desc->attrs[i].atttypid != INT4OID)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					 errmsg("test_pg_dump_fdw only supports INT4 columns")));
	}
}

static TupleTableSlot *
dumptestIterateForeignScan(ForeignScanState *node)
{
	TupleTableSlot *slot;
	TupleDesc		desc;

	/* limit the testcase to contain 10 rows */
	if (curr_row > 10)
		return NULL;

	slot = node->ss.ss_ScanTupleSlot;
	desc = slot->tts_tupleDescriptor;

	ExecClearTuple(slot);
	
	for (int i = 0; i < desc->natts; i++)
	{
		slot->tts_isnull[i] = false;
		slot->tts_values[i] = Int32GetDatum(curr_row);
	}

	ExecStoreVirtualTuple(slot);
	curr_row++;

	return slot;
}

static void
dumptestReScanForeignScan(ForeignScanState *node)
{
	(void) node;
	curr_row = 0;
}

static void
dumptestEndForeignScan(ForeignScanState *node)
{
	(void) node;
	curr_row = 0;
}
