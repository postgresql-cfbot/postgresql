/*-------------------------------------------------------------------------
 *
 * libpq_protocol_cursor.c
 *		Tests for extended query protocol cursor options via
 *		PQsendBindWithCursorOptions (_pq_.protocol_cursor protocol extension).
 *
 * Copyright (c) 2024-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/libpq_protocol_cursor/libpq_protocol_cursor.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <string.h>

#include "libpq-fe.h"
#include "pg_getopt.h"

/*
 * Cursor option flags for PQsendBindWithCursorOptions, defined in libpq-fe.h
 * as PQ_BIND_CURSOR_*.  We use those directly.
 */

static const char *const progname = "libpq_protocol_cursor";

static void exit_nicely(PGconn *conn);
pg_noreturn static void pg_fatal_impl(int line, const char *fmt,...)
			pg_attribute_printf(2, 3);

static void
exit_nicely(PGconn *conn)
{
	PQfinish(conn);
	exit(1);
}

/*
 * The following few functions are wrapped in macros to make the reported line
 * number in an error match the line number of the invocation.
 */

/*
 * Print an error to stderr and terminate the program.
 */
#define pg_fatal(...) pg_fatal_impl(__LINE__, __VA_ARGS__)
pg_noreturn static void
pg_fatal_impl(int line, const char *fmt,...)
{
	va_list		args;

	fflush(stdout);

	fprintf(stderr, "\n%s:%d: ", progname, line);
	va_start(args, fmt);
	vfprintf(stderr, fmt, args);
	va_end(args);
	Assert(fmt[strlen(fmt) - 1] != '\n');
	fprintf(stderr, "\n");
	exit(1);
}

/*
 * Check that libpq next returns a PGresult with the specified status,
 * returning the PGresult so that caller can perform additional checks.
 */
#define confirm_result_status(conn, status) confirm_result_status_impl(__LINE__, conn, status)
static PGresult *
confirm_result_status_impl(int line, PGconn *conn, ExecStatusType status)
{
	PGresult   *res;

	res = PQgetResult(conn);
	if (res == NULL)
		pg_fatal_impl(line, "PQgetResult returned null unexpectedly: %s",
					  PQerrorMessage(conn));
	if (PQresultStatus(res) != status)
		pg_fatal_impl(line, "PQgetResult returned status %s, expected %s: %s",
					  PQresStatus(PQresultStatus(res)),
					  PQresStatus(status),
					  PQerrorMessage(conn));
	return res;
}

/*
 * Check that libpq next returns a PGresult with the specified status,
 * then free the PGresult.
 */
#define consume_result_status(conn, status) consume_result_status_impl(__LINE__, conn, status)
static void
consume_result_status_impl(int line, PGconn *conn, ExecStatusType status)
{
	PGresult   *res;

	res = confirm_result_status_impl(line, conn, status);
	PQclear(res);
}

/*
 * Check that libpq next returns a null PGresult.
 */
#define consume_null_result(conn) consume_null_result_impl(__LINE__, conn)
static void
consume_null_result_impl(int line, PGconn *conn)
{
	PGresult   *res;

	res = PQgetResult(conn);
	if (res != NULL)
		pg_fatal_impl(line, "expected NULL PGresult, got %s: %s",
					  PQresStatus(PQresultStatus(res)),
					  PQerrorMessage(conn));
}

/*
 * Test holdable cursor: create a portal with PQ_BIND_CURSOR_HOLD via Bind,
 * commit the transaction, then FETCH from the surviving portal.
 */
static void
test_holdable_cursor(PGconn *conn)
{
	PGresult   *res;

	fprintf(stderr, "test_holdable_cursor... ");

	res = PQexec(conn, "BEGIN");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("BEGIN failed: %s", PQerrorMessage(conn));
	PQclear(res);

	res = PQexec(conn, "CREATE TEMP TABLE IF NOT EXISTS holdable_test(id int)");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("CREATE TABLE failed: %s", PQerrorMessage(conn));
	PQclear(res);

	res = PQexec(conn, "INSERT INTO holdable_test VALUES (1), (2), (3)");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("INSERT failed: %s", PQerrorMessage(conn));
	PQclear(res);

	res = PQprepare(conn, "holdstmt", "SELECT * FROM holdable_test", 0, NULL);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("PREPARE failed: %s", PQerrorMessage(conn));
	PQclear(res);

	if (PQenterPipelineMode(conn) != 1)
		pg_fatal("failed to enter pipeline mode: %s", PQerrorMessage(conn));

	if (PQsendBindWithCursorOptions(conn, "holdstmt", 0, NULL, NULL, NULL, 0,
									"holdportal", PQ_BIND_CURSOR_HOLD) != 1)
		pg_fatal("PQsendBindWithCursorOptions failed: %s", PQerrorMessage(conn));

	if (PQsendQueryParams(conn, "COMMIT", 0, NULL, NULL, NULL, NULL, 0) != 1)
		pg_fatal("COMMIT failed: %s", PQerrorMessage(conn));

	if (PQsendQueryParams(conn, "FETCH ALL FROM holdportal", 0, NULL, NULL, NULL, NULL, 0) != 1)
		pg_fatal("FETCH failed: %s", PQerrorMessage(conn));

	if (PQsendClosePortal(conn, "holdportal") != 1)
		pg_fatal("PQsendClosePortal failed: %s", PQerrorMessage(conn));

	if (PQpipelineSync(conn) != 1)
		pg_fatal("pipeline sync failed: %s", PQerrorMessage(conn));

	/* Bind+Describe result (RowDescription metadata) */
	res = confirm_result_status(conn, PGRES_COMMAND_OK);
	if (PQnfields(res) != 1)
		pg_fatal("expected 1 field, got %d", PQnfields(res));
	PQclear(res);
	consume_null_result(conn);

	/* COMMIT result */
	consume_result_status(conn, PGRES_COMMAND_OK);
	consume_null_result(conn);

	/* FETCH after commit */
	res = confirm_result_status(conn, PGRES_TUPLES_OK);
	if (PQntuples(res) != 3)
		pg_fatal("expected 3 rows after commit, got %d", PQntuples(res));
	PQclear(res);
	consume_null_result(conn);

	/* CLOSE */
	consume_result_status(conn, PGRES_COMMAND_OK);
	consume_null_result(conn);

	consume_result_status(conn, PGRES_PIPELINE_SYNC);
	consume_null_result(conn);

	if (PQexitPipelineMode(conn) != 1)
		pg_fatal("failed to exit pipeline mode: %s", PQerrorMessage(conn));

	fprintf(stderr, "ok\n");
}

/*
 * Test scroll cursor: create a portal with PQ_BIND_CURSOR_SCROLL and verify
 * backward fetching works.
 */
static void
test_scroll_cursor(PGconn *conn)
{
	PGresult   *res;

	fprintf(stderr, "test_scroll_cursor... ");

	res = PQexec(conn, "BEGIN");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("BEGIN failed: %s", PQerrorMessage(conn));
	PQclear(res);

	res = PQexec(conn, "CREATE TEMP TABLE IF NOT EXISTS scroll_test(id int)");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("CREATE TABLE failed: %s", PQerrorMessage(conn));
	PQclear(res);

	res = PQexec(conn, "INSERT INTO scroll_test VALUES (1), (2), (3)");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("INSERT failed: %s", PQerrorMessage(conn));
	PQclear(res);

	res = PQprepare(conn, "scrollstmt", "SELECT * FROM scroll_test ORDER BY id", 0, NULL);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("PREPARE failed: %s", PQerrorMessage(conn));
	PQclear(res);

	if (PQenterPipelineMode(conn) != 1)
		pg_fatal("failed to enter pipeline mode: %s", PQerrorMessage(conn));

	if (PQsendBindWithCursorOptions(conn, "scrollstmt", 0, NULL, NULL, NULL, 0,
									"scrollportal", PQ_BIND_CURSOR_SCROLL) != 1)
		pg_fatal("PQsendBindWithCursorOptions failed: %s", PQerrorMessage(conn));

	/* Fetch forward then backward */
	if (PQsendQueryParams(conn, "FETCH 2 FROM scrollportal", 0, NULL, NULL, NULL, NULL, 0) != 1)
		pg_fatal("FETCH forward failed: %s", PQerrorMessage(conn));

	if (PQsendQueryParams(conn, "FETCH BACKWARD 1 FROM scrollportal", 0, NULL, NULL, NULL, NULL, 0) != 1)
		pg_fatal("FETCH backward failed: %s", PQerrorMessage(conn));

	if (PQsendClosePortal(conn, "scrollportal") != 1)
		pg_fatal("PQsendClosePortal failed: %s", PQerrorMessage(conn));

	if (PQsendQueryParams(conn, "COMMIT", 0, NULL, NULL, NULL, NULL, 0) != 1)
		pg_fatal("COMMIT failed: %s", PQerrorMessage(conn));

	if (PQpipelineSync(conn) != 1)
		pg_fatal("pipeline sync failed: %s", PQerrorMessage(conn));

	/* Bind+Describe result (RowDescription metadata) */
	res = confirm_result_status(conn, PGRES_COMMAND_OK);
	if (PQnfields(res) != 1)
		pg_fatal("expected 1 field, got %d", PQnfields(res));
	PQclear(res);
	consume_null_result(conn);

	/* FETCH forward 2 */
	res = confirm_result_status(conn, PGRES_TUPLES_OK);
	if (PQntuples(res) != 2)
		pg_fatal("expected 2 rows from forward fetch, got %d", PQntuples(res));
	PQclear(res);
	consume_null_result(conn);

	/* FETCH backward 1 - should get row with id=1 */
	res = confirm_result_status(conn, PGRES_TUPLES_OK);
	if (PQntuples(res) != 1)
		pg_fatal("expected 1 row from backward fetch, got %d", PQntuples(res));
	if (strcmp(PQgetvalue(res, 0, 0), "1") != 0)
		pg_fatal("expected value '1' from backward fetch, got '%s'", PQgetvalue(res, 0, 0));
	PQclear(res);
	consume_null_result(conn);

	/* CLOSE */
	consume_result_status(conn, PGRES_COMMAND_OK);
	consume_null_result(conn);

	/* COMMIT */
	consume_result_status(conn, PGRES_COMMAND_OK);
	consume_null_result(conn);

	consume_result_status(conn, PGRES_PIPELINE_SYNC);
	consume_null_result(conn);

	if (PQexitPipelineMode(conn) != 1)
		pg_fatal("failed to exit pipeline mode: %s", PQerrorMessage(conn));

	fprintf(stderr, "ok\n");
}

/*
 * Test no-scroll cursor: create a portal with PQ_BIND_CURSOR_NO_SCROLL and
 * verify backward fetching is rejected.
 */
static void
test_no_scroll_cursor(PGconn *conn)
{
	PGresult   *res;

	fprintf(stderr, "test_no_scroll_cursor... ");

	res = PQexec(conn, "BEGIN");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("BEGIN failed: %s", PQerrorMessage(conn));
	PQclear(res);

	res = PQexec(conn, "CREATE TEMP TABLE IF NOT EXISTS noscroll_test(id int)");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("CREATE TABLE failed: %s", PQerrorMessage(conn));
	PQclear(res);

	res = PQexec(conn, "INSERT INTO noscroll_test VALUES (1), (2), (3)");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("INSERT failed: %s", PQerrorMessage(conn));
	PQclear(res);

	res = PQprepare(conn, "noscrollstmt", "SELECT * FROM noscroll_test ORDER BY id", 0, NULL);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("PREPARE failed: %s", PQerrorMessage(conn));
	PQclear(res);

	if (PQenterPipelineMode(conn) != 1)
		pg_fatal("failed to enter pipeline mode: %s", PQerrorMessage(conn));

	if (PQsendBindWithCursorOptions(conn, "noscrollstmt", 0, NULL, NULL, NULL, 0,
									"noscrollportal", PQ_BIND_CURSOR_NO_SCROLL) != 1)
		pg_fatal("PQsendBindWithCursorOptions failed: %s", PQerrorMessage(conn));

	/* Forward fetch should work */
	if (PQsendQueryParams(conn, "FETCH 1 FROM noscrollportal", 0, NULL, NULL, NULL, NULL, 0) != 1)
		pg_fatal("FETCH forward failed: %s", PQerrorMessage(conn));

	/* Backward fetch should fail */
	if (PQsendQueryParams(conn, "FETCH BACKWARD 1 FROM noscrollportal", 0, NULL, NULL, NULL, NULL, 0) != 1)
		pg_fatal("FETCH backward send failed: %s", PQerrorMessage(conn));

	if (PQsendClosePortal(conn, "noscrollportal") != 1)
		pg_fatal("PQsendClosePortal failed: %s", PQerrorMessage(conn));

	if (PQpipelineSync(conn) != 1)
		pg_fatal("pipeline sync failed: %s", PQerrorMessage(conn));

	/* Bind+Describe result (RowDescription metadata) */
	res = confirm_result_status(conn, PGRES_COMMAND_OK);
	if (PQnfields(res) != 1)
		pg_fatal("expected 1 field, got %d", PQnfields(res));
	PQclear(res);
	consume_null_result(conn);

	/* FETCH forward 1 - should succeed */
	res = confirm_result_status(conn, PGRES_TUPLES_OK);
	if (PQntuples(res) != 1)
		pg_fatal("expected 1 row from forward fetch, got %d", PQntuples(res));
	PQclear(res);
	consume_null_result(conn);

	/* FETCH backward - should fail */
	consume_result_status(conn, PGRES_FATAL_ERROR);
	consume_null_result(conn);

	/* CLOSE - pipeline is aborted after the error */
	consume_result_status(conn, PGRES_PIPELINE_ABORTED);
	consume_null_result(conn);

	/* Pipeline sync resets the abort state */
	consume_result_status(conn, PGRES_PIPELINE_SYNC);
	consume_null_result(conn);

	if (PQexitPipelineMode(conn) != 1)
		pg_fatal("failed to exit pipeline mode: %s", PQerrorMessage(conn));

	/* Clean up: rollback the failed transaction */
	res = PQexec(conn, "ROLLBACK");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("ROLLBACK failed: %s", PQerrorMessage(conn));
	PQclear(res);

	fprintf(stderr, "ok\n");
}

/*
 * Test combined cursor options: create a holdable + scrollable portal,
 * commit the transaction, then fetch backward from the surviving portal.
 */
static void
test_holdable_scroll_cursor(PGconn *conn)
{
	PGresult   *res;

	fprintf(stderr, "test_holdable_scroll_cursor... ");

	res = PQexec(conn, "BEGIN");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("BEGIN failed: %s", PQerrorMessage(conn));
	PQclear(res);

	res = PQexec(conn, "CREATE TEMP TABLE IF NOT EXISTS holdscroll_test(id int)");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("CREATE TABLE failed: %s", PQerrorMessage(conn));
	PQclear(res);

	res = PQexec(conn, "INSERT INTO holdscroll_test VALUES (1), (2), (3)");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("INSERT failed: %s", PQerrorMessage(conn));
	PQclear(res);

	res = PQprepare(conn, "holdscrollstmt",
					"SELECT * FROM holdscroll_test ORDER BY id", 0, NULL);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("PREPARE failed: %s", PQerrorMessage(conn));
	PQclear(res);

	if (PQenterPipelineMode(conn) != 1)
		pg_fatal("failed to enter pipeline mode: %s", PQerrorMessage(conn));

	/* Combine HOLD and SCROLL options */
	if (PQsendBindWithCursorOptions(conn, "holdscrollstmt", 0, NULL, NULL, NULL, 0,
									"holdscrollportal",
									PQ_BIND_CURSOR_HOLD | PQ_BIND_CURSOR_SCROLL) != 1)
		pg_fatal("PQsendBindWithCursorOptions failed: %s", PQerrorMessage(conn));

	if (PQsendQueryParams(conn, "COMMIT", 0, NULL, NULL, NULL, NULL, 0) != 1)
		pg_fatal("COMMIT failed: %s", PQerrorMessage(conn));

	/* Fetch forward after commit — holdable keeps the portal alive */
	if (PQsendQueryParams(conn, "FETCH 2 FROM holdscrollportal",
						  0, NULL, NULL, NULL, NULL, 0) != 1)
		pg_fatal("FETCH forward failed: %s", PQerrorMessage(conn));

	/* Fetch backward — scroll option allows this */
	if (PQsendQueryParams(conn, "FETCH BACKWARD 1 FROM holdscrollportal",
						  0, NULL, NULL, NULL, NULL, 0) != 1)
		pg_fatal("FETCH backward failed: %s", PQerrorMessage(conn));

	if (PQsendClosePortal(conn, "holdscrollportal") != 1)
		pg_fatal("PQsendClosePortal failed: %s", PQerrorMessage(conn));

	if (PQpipelineSync(conn) != 1)
		pg_fatal("pipeline sync failed: %s", PQerrorMessage(conn));

	/* Bind+Describe result (RowDescription metadata) */
	res = confirm_result_status(conn, PGRES_COMMAND_OK);
	if (PQnfields(res) != 1)
		pg_fatal("expected 1 field, got %d", PQnfields(res));
	PQclear(res);
	consume_null_result(conn);

	/* COMMIT */
	consume_result_status(conn, PGRES_COMMAND_OK);
	consume_null_result(conn);

	/* FETCH forward 2 */
	res = confirm_result_status(conn, PGRES_TUPLES_OK);
	if (PQntuples(res) != 2)
		pg_fatal("expected 2 rows from forward fetch, got %d", PQntuples(res));
	PQclear(res);
	consume_null_result(conn);

	/* FETCH backward 1 — should get row with id=1 */
	res = confirm_result_status(conn, PGRES_TUPLES_OK);
	if (PQntuples(res) != 1)
		pg_fatal("expected 1 row from backward fetch, got %d", PQntuples(res));
	if (strcmp(PQgetvalue(res, 0, 0), "1") != 0)
		pg_fatal("expected value '1' from backward fetch, got '%s'",
				 PQgetvalue(res, 0, 0));
	PQclear(res);
	consume_null_result(conn);

	/* CLOSE */
	consume_result_status(conn, PGRES_COMMAND_OK);
	consume_null_result(conn);

	consume_result_status(conn, PGRES_PIPELINE_SYNC);
	consume_null_result(conn);

	if (PQexitPipelineMode(conn) != 1)
		pg_fatal("failed to exit pipeline mode: %s", PQerrorMessage(conn));

	fprintf(stderr, "ok\n");
}

/*
 * Test that cursor options on a DML statement are harmlessly ignored.
 * The portal gets cursorOptions set, but since it's not a DECLARE CURSOR,
 * the options have no effect.
 */
static void
test_dml_with_cursor_options(PGconn *conn)
{
	PGresult   *res;

	fprintf(stderr, "test_dml_with_cursor_options... ");

	res = PQexec(conn, "CREATE TEMP TABLE IF NOT EXISTS dml_test(id int)");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("CREATE TABLE failed: %s", PQerrorMessage(conn));
	PQclear(res);

	res = PQprepare(conn, "dmlstmt",
					"INSERT INTO dml_test VALUES (1), (2), (3)", 0, NULL);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("PREPARE failed: %s", PQerrorMessage(conn));
	PQclear(res);

	if (PQenterPipelineMode(conn) != 1)
		pg_fatal("failed to enter pipeline mode: %s", PQerrorMessage(conn));

	/* Pass SCROLL option on a DML — should be silently ignored */
	if (PQsendBindWithCursorOptions(conn, "dmlstmt", 0, NULL, NULL, NULL, 0,
									"dmlportal", PQ_BIND_CURSOR_SCROLL) != 1)
		pg_fatal("PQsendBindWithCursorOptions failed: %s", PQerrorMessage(conn));

	if (PQpipelineSync(conn) != 1)
		pg_fatal("pipeline sync failed: %s", PQerrorMessage(conn));

	/* Bind+Describe result */
	res = confirm_result_status(conn, PGRES_COMMAND_OK);
	PQclear(res);
	consume_null_result(conn);

	consume_result_status(conn, PGRES_PIPELINE_SYNC);
	consume_null_result(conn);

	if (PQexitPipelineMode(conn) != 1)
		pg_fatal("failed to exit pipeline mode: %s", PQerrorMessage(conn));

	/* Verify the INSERT didn't actually execute (Bind+Describe only) */
	res = PQexec(conn, "SELECT count(*) FROM dml_test");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		pg_fatal("SELECT count failed: %s", PQerrorMessage(conn));
	if (strcmp(PQgetvalue(res, 0, 0), "0") != 0)
		pg_fatal("expected 0 rows (Bind+Describe doesn't execute), got %s",
				 PQgetvalue(res, 0, 0));
	PQclear(res);

	fprintf(stderr, "ok\n");
}

/*
 * Test client-side validation: PQsendBindWithCursorOptions should reject
 * an unnamed (empty) portal.
 */
static void
test_unnamed_portal_rejected(PGconn *conn)
{
	PGresult   *res;

	fprintf(stderr, "test_unnamed_portal_rejected... ");

	res = PQprepare(conn, "rejectstmt", "SELECT 1", 0, NULL);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("PREPARE failed: %s", PQerrorMessage(conn));
	PQclear(res);

	if (PQenterPipelineMode(conn) != 1)
		pg_fatal("failed to enter pipeline mode: %s", PQerrorMessage(conn));

	/* Empty portal name should be rejected client-side */
	if (PQsendBindWithCursorOptions(conn, "rejectstmt", 0, NULL, NULL, NULL, 0,
									"", PQ_BIND_CURSOR_HOLD) != 0)
		pg_fatal("expected PQsendBindWithCursorOptions to reject empty portal name");

	/* NULL portal name should also be rejected */
	if (PQsendBindWithCursorOptions(conn, "rejectstmt", 0, NULL, NULL, NULL, 0,
									NULL, PQ_BIND_CURSOR_HOLD) != 0)
		pg_fatal("expected PQsendBindWithCursorOptions to reject NULL portal name");

	if (PQexitPipelineMode(conn) != 1)
		pg_fatal("failed to exit pipeline mode: %s", PQerrorMessage(conn));

	fprintf(stderr, "ok\n");
}

/*
 * Test that cursor options are rejected when _pq_.protocol_cursor is not negotiated.
 * HOLD is requested but the extension is disabled, so the API call itself
 * returns 0.
 */
static void
test_cursor_options_without_extension(PGconn *conn)
{
	PGresult   *res;

	fprintf(stderr, "test_cursor_options_without_extension... ");

	/*
	 * PQPortalCursorEnabled should return false when extension is not
	 * negotiated
	 */
	if (PQPortalCursorEnabled(conn) != 0)
		pg_fatal("expected PQPortalCursorEnabled to return false");

	res = PQprepare(conn, "noextstmt", "SELECT 1", 0, NULL);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("PREPARE failed: %s", PQerrorMessage(conn));
	PQclear(res);

	if (PQenterPipelineMode(conn) != 1)
		pg_fatal("failed to enter pipeline mode: %s", PQerrorMessage(conn));

	/* Non-zero cursorOptions should be rejected when extension is disabled */
	if (PQsendBindWithCursorOptions(conn, "noextstmt", 0, NULL, NULL, NULL, 0,
									"noextportal", PQ_BIND_CURSOR_HOLD) != 0)
		pg_fatal("expected PQsendBindWithCursorOptions to reject cursor options");

	/* Zero cursorOptions should still succeed */
	if (PQsendBindWithCursorOptions(conn, "noextstmt", 0, NULL, NULL, NULL, 0,
									"noextportal", 0) != 1)
		pg_fatal("PQsendBindWithCursorOptions with zero options failed: %s",
				 PQerrorMessage(conn));

	if (PQpipelineSync(conn) != 1)
		pg_fatal("pipeline sync failed: %s", PQerrorMessage(conn));

	/* Bind+Describe result */
	res = confirm_result_status(conn, PGRES_COMMAND_OK);
	PQclear(res);
	consume_null_result(conn);

	consume_result_status(conn, PGRES_PIPELINE_SYNC);
	consume_null_result(conn);

	if (PQexitPipelineMode(conn) != 1)
		pg_fatal("failed to exit pipeline mode: %s", PQerrorMessage(conn));

	fprintf(stderr, "ok\n");
}

static void
usage(const char *progname)
{
	fprintf(stderr, "%s tests extended query protocol cursor options.\n\n", progname);
	fprintf(stderr, "Usage:\n");
	fprintf(stderr, "  %s tests\n", progname);
	fprintf(stderr, "  %s TESTNAME [CONNINFO]\n", progname);
}

/*
 * Test that invalid cursor option flags are rejected client-side.
 */
static void
test_invalid_flags_rejected(PGconn *conn)
{
	PGresult   *res;

	fprintf(stderr, "test_invalid_flags_rejected... ");

	res = PQprepare(conn, "invalidstmt", "SELECT 1", 0, NULL);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("PREPARE failed: %s", PQerrorMessage(conn));
	PQclear(res);

	if (PQenterPipelineMode(conn) != 1)
		pg_fatal("failed to enter pipeline mode: %s", PQerrorMessage(conn));

	/* Flag 0x0008 is not a valid bind extension flag */
	if (PQsendBindWithCursorOptions(conn, "invalidstmt", 0, NULL, NULL, NULL, 0,
									"invalidportal", 0x0008) != 0)
		pg_fatal("expected PQsendBindWithCursorOptions to reject invalid flags");

	/* Combination of valid and invalid flags should also be rejected */
	if (PQsendBindWithCursorOptions(conn, "invalidstmt", 0, NULL, NULL, NULL, 0,
									"invalidportal",
									PQ_BIND_CURSOR_HOLD | 0x0100) != 0)
		pg_fatal("expected PQsendBindWithCursorOptions to reject mixed invalid flags");

	if (PQexitPipelineMode(conn) != 1)
		pg_fatal("failed to exit pipeline mode: %s", PQerrorMessage(conn));

	fprintf(stderr, "ok\n");
}

static void
print_test_list(void)
{
	printf("holdable_cursor\n");
	printf("scroll_cursor\n");
	printf("no_scroll_cursor\n");
	printf("holdable_scroll_cursor\n");
	printf("dml_with_cursor_options\n");
	printf("unnamed_portal_rejected\n");
	printf("invalid_flags_rejected\n");
	printf("cursor_options_without_extension\n");
}

int
main(int argc, char **argv)
{
	const char *conninfo = "";
	PGconn	   *conn;
	char	   *testname;
	PGresult   *res;

	if (argc < 2)
	{
		usage(argv[0]);
		exit(1);
	}

	testname = argv[1];

	if (strcmp(testname, "tests") == 0)
	{
		print_test_list();
		exit(0);
	}

	if (argc > 2)
		conninfo = argv[2];

	conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s\n",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}

	res = PQexec(conn, "SET lc_messages TO \"C\"");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("failed to set \"lc_messages\": %s", PQerrorMessage(conn));
	PQclear(res);

	if (strcmp(testname, "cursor_options_without_extension") == 0)
		test_cursor_options_without_extension(conn);
	else if (strcmp(testname, "dml_with_cursor_options") == 0)
		test_dml_with_cursor_options(conn);
	else if (strcmp(testname, "holdable_scroll_cursor") == 0)
		test_holdable_scroll_cursor(conn);
	else if (strcmp(testname, "holdable_cursor") == 0)
		test_holdable_cursor(conn);
	else if (strcmp(testname, "invalid_flags_rejected") == 0)
		test_invalid_flags_rejected(conn);
	else if (strcmp(testname, "no_scroll_cursor") == 0)
		test_no_scroll_cursor(conn);
	else if (strcmp(testname, "scroll_cursor") == 0)
		test_scroll_cursor(conn);
	else if (strcmp(testname, "unnamed_portal_rejected") == 0)
		test_unnamed_portal_rejected(conn);
	else
	{
		fprintf(stderr, "\"%s\" is not a recognized test name\n", testname);
		exit(1);
	}

	PQfinish(conn);
	return 0;
}
