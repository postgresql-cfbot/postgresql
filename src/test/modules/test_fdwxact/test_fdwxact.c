/*-------------------------------------------------------------------------
 *
 * test_fdwxact.c
 *		  Test modules for foreign transaction management
 *
 * This module implements three types of foreign data wrapper: the first
 * doesn't support any transaction FDW APIs, the second supports only
 * commit and rollback API and the third supports all transaction API including
 * prepare.
 *
 * Also, this module has an ability to inject an error at prepare callback or
 * commit callback using test_inject_error() SQL function. The information of
 * injected error is stored in the shared memory so that backend processes and
 * resolver processes can see it.
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_fdwxact/test_fdwxact.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/fdwxact.h"
#include "access/xact.h"
#include "commands/defrem.h"
#include "access/reloptions.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

#define TEST_FDWXCT_MAX_NAME_LEN 32

typedef struct testFdwXactSharedState
{
	char	elevel[TEST_FDWXCT_MAX_NAME_LEN];
	char	phase[TEST_FDWXCT_MAX_NAME_LEN];
	char	server[TEST_FDWXCT_MAX_NAME_LEN];
	LWLock	*lock;
} testFdwXactSharedState;
testFdwXactSharedState *fxss = NULL;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static bool log_api_calls = false;

void _PG_init(void);
void _PG_fini(void);
PG_FUNCTION_INFO_V1(test_fdw_handler);
PG_FUNCTION_INFO_V1(test_no2pc_fdw_handler);
PG_FUNCTION_INFO_V1(test_2pc_fdw_handler);
PG_FUNCTION_INFO_V1(test_inject_error);
PG_FUNCTION_INFO_V1(test_reset_error);

static void test_fdwxact_shmem_startup(void);
static bool check_event(char *servername, char *phase, int *elevel);
static void testGetForeignRelSize(PlannerInfo *root,
								  RelOptInfo *baserel,
								  Oid foreigntableid);
static void testGetForeignPaths(PlannerInfo *root,
								RelOptInfo *baserel,
								Oid foreigntableid);
static ForeignScan *testGetForeignPlan(PlannerInfo *root,
									   RelOptInfo *foreignrel,
									   Oid foreigntableid,
									   ForeignPath *best_path,
									   List *tlist,
									   List *scan_clauses,
									   Plan *outer_plan);
static void testBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *testIterateForeignScan(ForeignScanState *node);
static void testReScanForeignScan(ForeignScanState *node);
static void testEndForeignScan(ForeignScanState *node);
static void testBeginForeignModify(ModifyTableState *mtstate,
								   ResultRelInfo *resultRelInfo,
								   List *fdw_private,
								   int subplan_index,
								   int eflags);
static void testBeginForeignModifyWithRegistration(ModifyTableState *mtstate,
												   ResultRelInfo *resultRelInfo,
												   List *fdw_private,
												   int subplan_index,
												   int eflags);
static TupleTableSlot *testExecForeignInsert(EState *estate,
											 ResultRelInfo *resultRelInfo,
											 TupleTableSlot *slot,
											 TupleTableSlot *planSlot);
static void testBeginForeignInsertWithRegistration(ModifyTableState *mtstate,
												   ResultRelInfo *resultRelInfo);
static void testEndForeignModify(EState *estate,
								 ResultRelInfo *resultRelInfo);
static void testBeginForeignInsert(ModifyTableState *mtstate,
								   ResultRelInfo *resultRelInfo);
static void testEndForeignInsert(EState *estate,
								 ResultRelInfo *resultRelInfo);
static int	testIsForeignRelUpdatable(Relation rel);
static void testPrepareForeignTransaction(FdwXactInfo *finfo);
static void testCommitForeignTransaction(FdwXactInfo *finfo);
static void testRollbackForeignTransaction(FdwXactInfo *finfo);
static char *testGetPrepareId(TransactionId xid, Oid serverid,
							  Oid userid, int *prep_id_len);

void
_PG_init(void)
{
	DefineCustomBoolVariable("test_fdwxact.log_api_calls",
							 "Report transaction API calls to logs.",
							 NULL,
							 &log_api_calls,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	RequestAddinShmemSpace(MAXALIGN(sizeof(testFdwXactSharedState)));
	RequestNamedLWLockTranche("test_fdwxact", 1);

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = test_fdwxact_shmem_startup;
}

void
_PG_fini(void)
{
	/* Uninstall hooks. */
	shmem_startup_hook = prev_shmem_startup_hook;
}

static void
test_fdwxact_shmem_startup(void)
{
	bool found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	fxss = ShmemInitStruct("test_fdwxact",
						   sizeof(testFdwXactSharedState),
						   &found);
	if (!found)
	{
		memset(fxss->elevel, 0, TEST_FDWXCT_MAX_NAME_LEN);
		memset(fxss->phase, 0, TEST_FDWXCT_MAX_NAME_LEN);
		memset(fxss->server, 0, TEST_FDWXCT_MAX_NAME_LEN);
		fxss->lock = &(GetNamedLWLockTranche("test_fdwxact"))->lock;
	}
}

Datum
test_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	routine->GetForeignRelSize = testGetForeignRelSize;
	routine->GetForeignPaths = testGetForeignPaths;
	routine->GetForeignPlan = testGetForeignPlan;
	routine->BeginForeignScan = testBeginForeignScan;
	routine->IterateForeignScan = testIterateForeignScan;
	routine->ReScanForeignScan = testReScanForeignScan;
	routine->EndForeignScan = testEndForeignScan;

	/* Functions for updating foreign tables */
	routine->AddForeignUpdateTargets = NULL;
	routine->PlanForeignModify = NULL;
	routine->BeginForeignModify = testBeginForeignModify;
	routine->ExecForeignInsert = testExecForeignInsert;
	routine->EndForeignModify = testEndForeignModify;
	routine->BeginForeignInsert = testBeginForeignInsert;
	routine->EndForeignInsert = testEndForeignInsert;
	routine->IsForeignRelUpdatable = testIsForeignRelUpdatable;

	PG_RETURN_POINTER(routine);
}

Datum
test_no2pc_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	routine->GetForeignRelSize = testGetForeignRelSize;
	routine->GetForeignPaths = testGetForeignPaths;
	routine->GetForeignPlan = testGetForeignPlan;
	routine->BeginForeignScan = testBeginForeignScan;
	routine->IterateForeignScan = testIterateForeignScan;
	routine->ReScanForeignScan = testReScanForeignScan;
	routine->EndForeignScan = testEndForeignScan;

	/* Functions for updating foreign tables */
	routine->AddForeignUpdateTargets = NULL;
	routine->PlanForeignModify = NULL;
	routine->BeginForeignModify = testBeginForeignModifyWithRegistration;
	routine->ExecForeignInsert = testExecForeignInsert;
	routine->EndForeignModify = testEndForeignModify;
	routine->BeginForeignInsert = testBeginForeignInsertWithRegistration;
	routine->EndForeignInsert = testEndForeignInsert;
	routine->IsForeignRelUpdatable = testIsForeignRelUpdatable;

	/* Support only COMMIT and ROLLBACK */
	routine->CommitForeignTransaction = testCommitForeignTransaction;
	routine->RollbackForeignTransaction = testRollbackForeignTransaction;

	PG_RETURN_POINTER(routine);
}

Datum
test_2pc_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	routine->GetForeignRelSize = testGetForeignRelSize;
	routine->GetForeignPaths = testGetForeignPaths;
	routine->GetForeignPlan = testGetForeignPlan;
	routine->BeginForeignScan = testBeginForeignScan;
	routine->IterateForeignScan = testIterateForeignScan;
	routine->ReScanForeignScan = testReScanForeignScan;
	routine->EndForeignScan = testEndForeignScan;

	/* Functions for updating foreign tables */
	routine->AddForeignUpdateTargets = NULL;
	routine->PlanForeignModify = NULL;
	routine->BeginForeignModify = testBeginForeignModifyWithRegistration;
	routine->ExecForeignInsert = testExecForeignInsert;
	routine->EndForeignModify = testEndForeignModify;
	routine->BeginForeignInsert = testBeginForeignInsertWithRegistration;
	routine->EndForeignInsert = testEndForeignInsert;
	routine->IsForeignRelUpdatable = testIsForeignRelUpdatable;

	/* Support all functions for foreign transactions */
	routine->GetPrepareId = testGetPrepareId;
	routine->PrepareForeignTransaction = testPrepareForeignTransaction;
	routine->CommitForeignTransaction = testCommitForeignTransaction;
	routine->RollbackForeignTransaction = testRollbackForeignTransaction;

	PG_RETURN_POINTER(routine);
}

static void
testGetForeignRelSize(PlannerInfo *root,
					  RelOptInfo *baserel,
					  Oid foreigntableid)
{
	baserel->pages = 10;
	baserel->tuples = 100;
}

static void
testGetForeignPaths(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid)
{
	add_path(baserel, (Path *) create_foreignscan_path(root, baserel,
													   NULL,
													   10, 10, 10,
													   NIL,
													   baserel->lateral_relids,
													   NULL, NIL));
}

static ForeignScan *
testGetForeignPlan(PlannerInfo *root,
				   RelOptInfo *foreignrel,
				   Oid foreigntableid,
				   ForeignPath *best_path,
				   List *tlist,
				   List *scan_clauses,
				   Plan *outer_plan)
{
	return make_foreignscan(tlist,
							NIL,
							foreignrel->relid,
							NIL,
							NULL,
							NIL,
							NIL,
							outer_plan);
}

static void
testBeginForeignScan(ForeignScanState *node, int eflags)
{
	return;
}

static TupleTableSlot *
testIterateForeignScan(ForeignScanState *node)
{
	return ExecClearTuple(node->ss.ss_ScanTupleSlot);
}

static void
testReScanForeignScan(ForeignScanState *node)
{
	return;
}

static void
testEndForeignScan(ForeignScanState *node)
{
	return;
}

/* Register the foreign transaction */
static void
testRegisterFdwXact(ModifyTableState *mtstate,
					ResultRelInfo *resultRelInfo,
					bool modified)
{
	Relation rel = resultRelInfo->ri_RelationDesc;
	RangeTblEntry	*rte;
	ForeignTable *table;
	UserMapping	*usermapping;
	Oid		userid;

	rte = exec_rt_fetch(resultRelInfo->ri_RangeTableIndex,
						mtstate->ps.state);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();
	table = GetForeignTable(RelationGetRelid(rel));
	usermapping = GetUserMapping(userid, table->serverid);
	FdwXactRegisterXact(usermapping, modified);
}


static void
testBeginForeignModify(ModifyTableState *mtstate,
					   ResultRelInfo *resultRelInfo,
					   List *fdw_private,
					   int subplan_index,
					   int eflags)
{
	return;
}

static void
testBeginForeignModifyWithRegistration(ModifyTableState *mtstate,
									   ResultRelInfo *resultRelInfo,
									   List *fdw_private,
									   int subplan_index,
									   int eflags)
{
	testRegisterFdwXact(mtstate, resultRelInfo,
						(eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0);
	return;
}

static TupleTableSlot *
testExecForeignInsert(EState *estate,
					  ResultRelInfo *resultRelInfo,
					  TupleTableSlot *slot,
					  TupleTableSlot *planSlot)
{
	return slot;
}

static void
testEndForeignModify(EState *estate,
					 ResultRelInfo *resultRelInfo)
{
	return;
}

static void
testBeginForeignInsert(ModifyTableState *mtstate,
					   ResultRelInfo *resultRelInfo)
{
	return;
}

static void
testBeginForeignInsertWithRegistration(ModifyTableState *mtstate,
									   ResultRelInfo *resultRelInfo)
{
	testRegisterFdwXact(mtstate, resultRelInfo, true);
	return;
}

static void
testEndForeignInsert(EState *estate,
					 ResultRelInfo *resultRelInfo)
{
	return;
}

static int
testIsForeignRelUpdatable(Relation rel)
{
	/* allow only inserts */
	return (1 << CMD_INSERT);
}

static char *
testGetPrepareId(TransactionId xid, Oid serverid,
				 Oid userid, int *prep_id_len)
{
	static char buf[32] = {0};

	*prep_id_len = snprintf(buf, 32, "tx_%u", xid);

	return buf;
}

static void
testPrepareForeignTransaction(FdwXactInfo *finfo)
{
	int elevel;

	if (check_event(finfo->server->servername, "prepare", &elevel))
		elog(elevel, "injected error at prepare");

	if (log_api_calls)
		ereport(LOG, (errmsg("prepare %s on %s",
							 finfo->identifier,
							 finfo->server->servername)));
}

static void
testCommitForeignTransaction(FdwXactInfo *finfo)
{
	int elevel;
	TransactionId xid = GetTopTransactionIdIfAny();

	if (check_event(finfo->server->servername, "commit", &elevel))
		elog(elevel, "injected error at commit");

	if (log_api_calls)
	{
		if (finfo->flags && FDWXACT_FLAG_ONEPHASE)
			ereport(LOG, (errmsg("commit %u on %s",
								 xid, finfo->server->servername)));
		else
			ereport(LOG, (errmsg("commit prepared %s on %s",
								 finfo->identifier,
								 finfo->server->servername)));
	}
}

static void
testRollbackForeignTransaction(FdwXactInfo *finfo)
{
	TransactionId xid = GetTopTransactionIdIfAny();

	if (log_api_calls)
	{
		if (finfo->flags && FDWXACT_FLAG_ONEPHASE)
			ereport(LOG, (errmsg("rollback %u on %s",
								 xid, finfo->server->servername)));
		else
			ereport(LOG, (errmsg("rollback prepared %s on %s",
								 finfo->identifier,
								 finfo->server->servername)));
	}
}

/*
 * Check if an event is set at the phase on the server. If there is, set
 * elevel and return true.
 */
static bool
check_event(char *servername, char *phase, int *elevel)
{
	LWLockAcquire(fxss->lock, LW_SHARED);

	if (pg_strcasecmp(fxss->server, servername) != 0 ||
		pg_strcasecmp(fxss->phase, phase) != 0)
	{
		LWLockRelease(fxss->lock);
		return false;
	}

	/* Currently support only error and panic */
	if (pg_strcasecmp(fxss->elevel, "error") == 0)
		*elevel = ERROR;
	if (pg_strcasecmp(fxss->elevel, "panic") == 0)
		*elevel = PANIC;

	LWLockRelease(fxss->lock);

	return true;
}

/* SQL function to inject an error */
Datum
test_inject_error(PG_FUNCTION_ARGS)
{
	char *elevel = text_to_cstring(PG_GETARG_TEXT_P(0));
	char *phase = text_to_cstring(PG_GETARG_TEXT_P(1));
	char *server = text_to_cstring(PG_GETARG_TEXT_P(2));

	LWLockAcquire(fxss->lock, LW_EXCLUSIVE);
	strncpy(fxss->elevel, elevel, TEST_FDWXCT_MAX_NAME_LEN);
	strncpy(fxss->phase, phase, TEST_FDWXCT_MAX_NAME_LEN);
	strncpy(fxss->server, server, TEST_FDWXCT_MAX_NAME_LEN);
	LWLockRelease(fxss->lock);

	PG_RETURN_NULL();
}

/* SQL function to reset an error */
Datum
test_reset_error(PG_FUNCTION_ARGS)
{
	LWLockAcquire(fxss->lock, LW_EXCLUSIVE);
	memset(fxss->elevel, 0, TEST_FDWXCT_MAX_NAME_LEN);
	memset(fxss->phase, 0, TEST_FDWXCT_MAX_NAME_LEN);
	memset(fxss->server, 0, TEST_FDWXCT_MAX_NAME_LEN);
	LWLockRelease(fxss->lock);

	PG_RETURN_NULL();
}
