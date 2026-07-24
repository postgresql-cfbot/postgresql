/*-------------------------------------------------------------------------
 *
 * test_extensible.c
 *		Test correctness of extensible node and custom scan registration
 *		functions defined in src/backend/nodes/extensible.c.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_extensible/test_extensible.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tableam.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/nodes.h"
#include "nodes/plannodes.h"
#include "nodes/readfuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

PG_MODULE_MAGIC;

/* Name of the test table that triggers our CustomScan injection */
#define TEST_TABLE_NAME		"test_extensible_tbl"

/* ----------------------------------------------------------------
 * TestExtNode – an ExtensibleNode subtype carrying a single int.
 *
 * In a real extension this would hold planning metadata passed from
 * the path stage to the execution stage via CustomScan.custom_private.
 * ----------------------------------------------------------------
 */
typedef struct TestExtNode
{
	ExtensibleNode base;		/* must be first */
	int			repeat_count;	/* how many times to return each scanned row */
}			TestExtNode;

#define TEST_EXT_NODE_NAME	"TestExtNode"
#define TEST_CUSTOM_SCAN_NAME	"TestCustomScan"

/* ----------------------------------------------------------------
 * ExtensibleNodeMethods callbacks
 * ----------------------------------------------------------------
 */

static void
test_ext_node_copy(ExtensibleNode *newnode, const ExtensibleNode *oldnode)
{
	((TestExtNode *) newnode)->repeat_count =
		((const TestExtNode *) oldnode)->repeat_count;
}

static bool
test_ext_node_equal(const ExtensibleNode *a, const ExtensibleNode *b)
{
	return ((const TestExtNode *) a)->repeat_count ==
		((const TestExtNode *) b)->repeat_count;
}

static void
test_ext_node_out(StringInfo str, const ExtensibleNode *node)
{
	appendStringInfo(str, " :repeat_count %d",
					 ((const TestExtNode *) node)->repeat_count);
}

static void
test_ext_node_read(ExtensibleNode *node)
{
	TestExtNode *tnode = (TestExtNode *) node;
	const char *token;
	int			length;

	token = pg_strtok(&length); /* skip :repeat_count */
	token = pg_strtok(&length); /* get value */
	tnode->repeat_count = atoi(token);
}

static const ExtensibleNodeMethods test_ext_node_methods =
{
	.extnodename = TEST_EXT_NODE_NAME,
	.node_size = sizeof(TestExtNode),
	.nodeCopy = test_ext_node_copy,
	.nodeEqual = test_ext_node_equal,
	.nodeOut = test_ext_node_out,
	.nodeRead = test_ext_node_read,
};

/* ----------------------------------------------------------------
 * TestCustomScanState – execution state for the custom scan.
 * ----------------------------------------------------------------
 */
typedef struct TestCustomScanState
{
	CustomScanState css;		/* must be first */
	TableScanDesc scandesc;
	int			repeat_count;	/* repeat_count from TestExtNode */
	int			repeats_left;	/* how many more times to return current row */
}			TestCustomScanState;

/* ----------------------------------------------------------------
 * Executor callbacks
 * ----------------------------------------------------------------
 */
static void
test_begin_custom_scan(CustomScanState *node, EState *estate, int eflags)
{
	TestCustomScanState *tstate = (TestCustomScanState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	TestExtNode *tnode;
	Relation	rel = node->ss.ss_currentRelation;

	/*
	 * Read repeat_count from the TestExtNode stored in custom_private. This
	 * is the key moment where the ExtensibleNode private data crosses from
	 * the plan tree into the executor state.
	 */
	Assert(cscan->custom_private != NIL);
	tnode = (TestExtNode *) linitial(cscan->custom_private);
	Assert(IsA(tnode, ExtensibleNode));
	Assert(strcmp(tnode->base.extnodename, TEST_EXT_NODE_NAME) == 0);

	Assert(tnode->repeat_count > 0);
	tstate->repeat_count = tnode->repeat_count;
	tstate->repeats_left = 0;

	/* Start a plain sequential heap scan. */
	tstate->scandesc = table_beginscan(rel, estate->es_snapshot, 0, NULL,
									   SO_NONE);
}

static TupleTableSlot *
test_exec_custom_scan(CustomScanState *node)
{
	TestCustomScanState *tstate = (TestCustomScanState *) node;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	/*
	 * If the current tuple still has repeats remaining, return it again
	 * without advancing the heap scan.  The repeat count comes from the
	 * TestExtNode that was read in BeginCustomScan.
	 */
	if (tstate->repeats_left > 0)
	{
		tstate->repeats_left--;
		return slot;
	}

	/* Fetch the next tuple from the heap. */
	if (!table_scan_getnextslot(tstate->scandesc, ForwardScanDirection, slot))
		return NULL;

	/* Schedule (repeat_count - 1) additional returns of this tuple. */
	tstate->repeats_left = tstate->repeat_count - 1;
	return slot;
}

static void
test_end_custom_scan(CustomScanState *node)
{
	TestCustomScanState *tstate = (TestCustomScanState *) node;

	if (tstate->scandesc)
		table_endscan(tstate->scandesc);
}

static void
test_rescan_custom_scan(CustomScanState *node)
{
	TestCustomScanState *tstate = (TestCustomScanState *) node;

	tstate->repeats_left = 0;
	table_rescan(tstate->scandesc, NULL);
}

static const CustomExecMethods test_custom_exec_methods =
{
	.CustomName = TEST_CUSTOM_SCAN_NAME,
	.BeginCustomScan = test_begin_custom_scan,
	.ExecCustomScan = test_exec_custom_scan,
	.EndCustomScan = test_end_custom_scan,
	.ReScanCustomScan = test_rescan_custom_scan,
};

static Node *
test_create_custom_scan_state(CustomScan *cscan)
{
	TestCustomScanState *tstate;

	tstate = (TestCustomScanState *)
		newNode(sizeof(TestCustomScanState), T_CustomScanState);
	tstate->css.methods = &test_custom_exec_methods;

	/*
	 * Use the heap-tuple slot type so table_scan_getnextslot() can fill it
	 * directly.
	 */
	tstate->css.slotOps = &TTSOpsBufferHeapTuple;

	return (Node *) tstate;
}

static const CustomScanMethods test_custom_scan_methods =
{
	.CustomName = TEST_CUSTOM_SCAN_NAME,
	.CreateCustomScanState = test_create_custom_scan_state,
};

/* ----------------------------------------------------------------
 * Planner callbacks
 * ----------------------------------------------------------------
 */
static Plan *
test_plan_custom_path(PlannerInfo *root,
					  RelOptInfo *rel,
					  struct CustomPath *best_path,
					  List *tlist,
					  List *clauses,
					  List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);

	/*
	 * Pass the ExtensibleNode from the path to the plan via custom_private.
	 * This is the recommended pattern for conveying private planning data
	 * from a CustomPath to its corresponding CustomScan.
	 */
	cscan->scan.plan.targetlist = tlist;
	cscan->scan.plan.qual = NIL;
	cscan->scan.scanrelid = rel->relid;
	cscan->flags = best_path->flags;
	cscan->custom_plans = NIL;
	cscan->custom_exprs = NIL;
	cscan->custom_private = best_path->custom_private;
	cscan->custom_scan_tlist = NIL;
	cscan->custom_relids = NULL;
	cscan->methods = &test_custom_scan_methods;

	return (Plan *) cscan;
}

static const CustomPathMethods test_custom_path_methods =
{
	.CustomName = TEST_CUSTOM_SCAN_NAME,
	.PlanCustomPath = test_plan_custom_path,
};

/* ----------------------------------------------------------------
 * set_rel_pathlist_hook – inject our CustomPath for the specific
 * test table.
 * ----------------------------------------------------------------
 */
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook = NULL;

static void
test_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
					  Index rti, RangeTblEntry *rte)
{
	CustomPath *cpath;
	TestExtNode *tnode;
	char	   *relname;

	/* Let previous hooks run first. */
	if (prev_set_rel_pathlist_hook)
		prev_set_rel_pathlist_hook(root, rel, rti, rte);

	/* Only handle plain base relations (ordinary tables). */
	if (rel->reloptkind != RELOPT_BASEREL || rte->rtekind != RTE_RELATION)
		return;

	/*
	 * Only inject our CustomPath for the specific marker table.  This
	 * prevents interference with system-catalog scans.
	 */
	relname = get_rel_name(rte->relid);
	if (relname == NULL || strcmp(relname, TEST_TABLE_NAME) != 0)
		return;

	/*
	 * Build a TestExtNode carrying the planner's row estimate.  In a real
	 * extension this might be a device handle or a cache key.
	 */
	tnode = (TestExtNode *) newNode(sizeof(TestExtNode), T_ExtensibleNode);
	tnode->base.extnodename = TEST_EXT_NODE_NAME;
	tnode->repeat_count = 2;	/* each row will be returned twice */

	/*
	 * Build the CustomPath.  We match the cost of the cheapest existing path
	 * so the planner is free to choose ours when it is equally cheap.
	 */
	cpath = makeNode(CustomPath);
	cpath->path.pathtype = T_CustomScan;
	cpath->path.parent = rel;
	cpath->path.pathtarget = rel->reltarget;
	cpath->path.rows = rel->rows;
	cpath->path.startup_cost = 0;
	cpath->path.total_cost = 0;
	cpath->flags = 0;
	cpath->custom_paths = NIL;
	cpath->custom_private = list_make1(tnode);
	cpath->methods = &test_custom_path_methods;

	add_path(rel, (Path *) cpath);
}

/* ----------------------------------------------------------------
 * SQL-callable test functions for Get* lookups
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(test_get_extensible_node_methods);
PG_FUNCTION_INFO_V1(test_get_custom_scan_methods);

/*
 * test_get_extensible_node_methods(name text, missing_ok bool)
 *
 * Thin wrapper around GetExtensibleNodeMethods().  Returns the registered
 * extnodename, or NULL when missing_ok = true and the name is not found.
 * Raises ERROR when missing_ok = false and the name is not found.
 */
Datum
test_get_extensible_node_methods(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	bool		missing_ok = PG_GETARG_BOOL(1);
	const ExtensibleNodeMethods *methods;

	methods = GetExtensibleNodeMethods(name, missing_ok);
	if (methods == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(cstring_to_text(methods->extnodename));
}

/*
 * test_get_custom_scan_methods(name text, missing_ok bool)
 *
 * Thin wrapper around GetCustomScanMethods().  Returns the registered
 * CustomName, or NULL when missing_ok = true and the name is not found.
 * Raises ERROR when missing_ok = false and the name is not found.
 */
Datum
test_get_custom_scan_methods(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	bool		missing_ok = PG_GETARG_BOOL(1);
	const CustomScanMethods *methods;

	methods = GetCustomScanMethods(name, missing_ok);
	if (methods == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(cstring_to_text(methods->CustomName));
}

/* ----------------------------------------------------------------
 * Module initialisation
 * ----------------------------------------------------------------
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errmsg("cannot load \"%s\" after startup",
						"test_extensible"),
				 errdetail("\"%s\" must be loaded with "
						   "\"shared_preload_libraries\".",
						   "test_extensible")));

	/* Register the custom scan methods. */
	RegisterCustomScanMethods(&test_custom_scan_methods);

	/* Register the extensible node type */
	RegisterExtensibleNodeMethods(&test_ext_node_methods);

	/* Install the path-list hook to inject CustomPaths for the test table. */
	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = test_set_rel_pathlist;
}
