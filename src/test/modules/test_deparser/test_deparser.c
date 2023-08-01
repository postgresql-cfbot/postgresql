/*-------------------------------------------------------------------------
 *
 * test_deparser.c
 *		Test DDL deparser
 *
 * Copyright (c) 2020-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/test_deparser/test_deparser.c
 *
 * When running the regression test(tests in parallel_schedule), we replace the
 * executing ddl statement with the its deparsed version and execute the
 * deparsed statement, so that we can run all the regression with the deparsed
 * statement and can expect the output to be the same as the existing
 * expected *.out. As developers typically add new regression tests to test new
 * syntax, so we expect this test can automatically identify any new syntax
 * changes.
 *
 * The strategy is to run the regression test twice. The first run will create
 * event triggers to deparse ddl statements, it is intended to collect all the
 * deparsed statements and can catch ERRORs/WARNINGs caused by the deparser. We
 * can dump these statements from the database and then reload them in the
 * second regression run. This allows us to utilize the deparsed statements to
 * replace the local statements in the second regression run. This approach
 * does not need to handle any remote messages and client variable stuff during
 * execution, although it could take more time to finsh the test.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/dependency.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "commands/event_trigger.h"
#include "commands/seclabel.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "tcop/ddldeparse.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/portal.h"
#include "utils/queryenvironment.h"

PG_MODULE_MAGIC;

static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
extern EventTriggerQueryState *currentEventTriggerState;

static void tdeparser_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
									 bool readOnlyTree,
									 ProcessUtilityContext context, ParamListInfo params,
									 QueryEnvironment *queryEnv,
									 DestReceiver *dest, QueryCompletion *qc);

static void tdeparser_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
		bool execute_once);

static int	nesting_level = 0;
static int	cmd_count = 0;
static bool	deparse_mode = false;


static List *
change_deparsed_stmt(PlannedStmt *pstmt, const char *queryString)
{
	List		   *nstmt_list = NIL;
	List		   *parsetree_list;
	ListCell	   *lc;
	Oid				save_userid = 0;
	const char	   *deparsed_cmd = queryString;
	int				save_sec_context = 0;
	int				i;
	SPITupleTable  *tuptable;
	StringInfoData	cmd;

	if (!deparse_mode)
		return list_make1(pstmt);

	initStringInfo(&cmd);
	appendStringInfo(&cmd, "SELECT command, id, deparse_time from pg_catalog.deparse_test_commands "
				"WHERE id = %d AND test_name = '%s' AND original_command = current_query() order by deparse_time",
				cmd_count, application_name);
	elog(LOG, "ID: %d", cmd_count);

	/* Change to superuser to avoid access deny. */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
			save_sec_context | SECURITY_LOCAL_USERID_CHANGE);

	SPI_connect();

	/* Query the deparsed statement. */
	if (SPI_exec(cmd.data, 1024) != SPI_OK_SELECT)
		elog(ERROR, "SPI_exec failed: %s", cmd.data);

	if (SPI_tuptable == NULL)
		elog(ERROR, "could not find deparsed statement");

	tuptable = SPI_tuptable;

	/*
	 * Return if the statement was not deparsed which means this statement is
	 * expected to fail.
	 */
	if (tuptable->numvals == 0)
	{
		SetUserIdAndSecContext(save_userid, save_sec_context);
		elog(LOG, "no deparsed statement");
		SPI_finish();
		return list_make1(pstmt);
	}

	resetStringInfo(&cmd);

	for (i = 0 ; i < tuptable->numvals; i++)
	{
		char *value  = SPI_getvalue(tuptable->vals[i], tuptable->tupdesc, 1);
		appendStringInfo(&cmd, "%s;", value ? value : " ");
	}

	elog(LOG, "EXECQ: %s", cmd.data);

	deparsed_cmd = cmd.data;

	SPI_finish();

	SetUserIdAndSecContext(save_userid, save_sec_context);

	parsetree_list = pg_parse_query(deparsed_cmd);

	/*
	 * One statment could be deparsed into mulitiple statements, for example:
	 * "DROP TABLE t1,t2" will be deparsed into "DROP TABLE t1" AND "DROP TABLE
	 * t2". So we need to maintain them in a list.
	 */
	foreach(lc, parsetree_list)
	{
		List	   *plans;
		RawStmt    *rs = lfirst_node(RawStmt, lc);
		List	   *querytree_list = pg_analyze_and_rewrite_fixedparams(rs, deparsed_cmd,
																		NULL, 0, NULL);

		Assert(list_length(querytree_list) == 1);

		plans = pg_plan_queries(querytree_list, deparsed_cmd,
								CURSOR_OPT_PARALLEL_OK, NULL);

		Assert(list_length(plans) == 1);

		nstmt_list = lappend(nstmt_list, linitial(plans));
	}

	return nstmt_list;
}

/*
 * ProcessUtility hook
 */
static void
tdeparser_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
						 bool readOnlyTree,
						 ProcessUtilityContext context,
						 ParamListInfo params, QueryEnvironment *queryEnv,
						 DestReceiver *dest, QueryCompletion *qc)
{
	List *nplan_list;
	ListCell *lc;
	CommandTag tag = CreateCommandTag(pstmt->utilityStmt);

	nesting_level++;
	PG_TRY();
	{
		if (nesting_level == 1 && command_tag_event_trigger_ok(tag))
			nplan_list = change_deparsed_stmt(pstmt, queryString);
		else
			nplan_list = list_make1(pstmt);

		foreach(lc, nplan_list)
		{
			PlannedStmt *plan = (PlannedStmt *) lfirst(lc);
			if (prev_ProcessUtility)
				prev_ProcessUtility(plan, queryString, readOnlyTree,
									context, params, queryEnv,
									dest, qc);
			else
				standard_ProcessUtility(plan, queryString, readOnlyTree,
										context, params, queryEnv,
										dest, qc);
		}
	}
	PG_FINALLY();
	{
		if (nesting_level == 1)
			cmd_count++;

		nesting_level--;
	}
	PG_END_TRY();
}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void
tdeparser_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
			bool execute_once)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();
}

PG_FUNCTION_INFO_V1(tdparser_get_cmdcount);
PG_FUNCTION_INFO_V1(test_deparser_drop_command);

Datum
tdparser_get_cmdcount(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(cmd_count);
}

Datum
test_deparser_drop_command(PG_FUNCTION_ARGS)
{
	slist_iter	iter;

	/* Drop commands are not part commandlist but handled here as part of SQLDropList */
	slist_foreach(iter, &(currentEventTriggerState->SQLDropList))
	{
		SQLDropObject *obj;
		EventTriggerData *trigdata;

		trigdata = (EventTriggerData *) fcinfo->context;

		obj = slist_container(SQLDropObject, next, iter.cur);

		if (!obj->original)
			continue;

		if (strcmp(obj->objecttype, "table") == 0)
		{
			char	*command;

			command = deparse_drop_table(obj->objidentity, trigdata->parsetree);
			if (command)
			{
				StringInfoData cmd;

				command = deparse_ddl_json_to_string(command);
				initStringInfo(&cmd);

				appendStringInfo(&cmd, "INSERT INTO pg_catalog.deparse_test_commands "
						"(id, test_name, deparse_time, original_command, command) "
						"SELECT public.tdparser_get_cmdcount(), "
						"current_setting('application_name'), "
						"clock_timestamp(), current_query(), '%s'", command);

				/* insert deparsed statement into table */
				SPI_connect();
				if (SPI_exec(cmd.data, 8) != SPI_OK_INSERT)
					elog(ERROR, "SPI_exec failed: %s", cmd.data);
				SPI_finish();
			}
		}
	}

	return PointerGetDatum(NULL);
}

/* Module load function */
void
_PG_init(void)
{
	DefineCustomBoolVariable("test_deparser.deparse_mode",
			"Change the statement to deparsed one",
			NULL,
			&deparse_mode,
			false,
			PGC_SUSET,
			0,
			NULL,
			NULL,
			NULL);

	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = tdeparser_ProcessUtility;

	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = tdeparser_ExecutorRun;
}
