/*
 * src/test/modules/test_libpq/testlibpqbatch.c
 *		Verify libpq batch execution functionality
 */
#include "postgres_fe.h"

#include <sys/time.h>

#include "catalog/pg_type_d.h"
#include "common/fe_memutils.h"
#include "libpq-fe.h"
#include "portability/instr_time.h"


static void exit_nicely(PGconn *conn);

const char *const progname = "testlibpqbatch";


#define DEBUG
#ifdef DEBUG
#define	pg_debug(...)  do { fprintf(stderr, __VA_ARGS__); } while (0)
#else
#define pg_debug(...)
#endif

static const char *const drop_table_sql =
"DROP TABLE IF EXISTS batch_demo";
static const char *const create_table_sql =
"CREATE UNLOGGED TABLE batch_demo(id serial primary key, itemno integer);";
static const char *const insert_sql =
"INSERT INTO batch_demo(itemno) VALUES ($1);";

/* max char length of an int32, plus sign and null terminator */
#define MAXINTLEN 12

static void
exit_nicely(PGconn *conn)
{
	PQfinish(conn);
	exit(1);
}

/*
 * Print an error to stderr and terminate the program.
 */
#define pg_fatal(...) pg_fatal_impl(__LINE__, __VA_ARGS__)
static void
pg_fatal_impl(int line, const char *fmt,...)
{
	va_list		args;

	fprintf(stderr, "\n");		/* XXX hack */
	fprintf(stderr, "%s:%d: ", progname, line);

	va_start(args, fmt);
	vfprintf(stderr, fmt, args);
	va_end(args);
	printf("Failure, exiting\n");
	exit(1);
}

static void
test_disallowed_in_batch(PGconn *conn)
{
	PGresult   *res = NULL;

	fprintf(stderr, "test error cases... ");

	if (PQisnonblocking(conn))
		pg_fatal("Expected blocking connection mode\n");

	if (PQenterBatchMode(conn) != 1)
		pg_fatal("Unable to enter batch mode\n");

	if (PQbatchStatus(conn) == PQBATCH_MODE_OFF)
		pg_fatal("Batch mode not activated properly\n");

	/* PQexec should fail in batch mode */
	res = PQexec(conn, "SELECT 1");
	if (PQresultStatus(res) != PGRES_FATAL_ERROR)
		pg_fatal("PQexec should fail in batch mode but succeeded\n");

	/* So should PQsendQuery */
	if (PQsendQuery(conn, "SELECT 1") != 0)
		pg_fatal("PQsendQuery should fail in batch mode but succeeded\n");

	/* Entering batch mode when already in batch mode is OK */
	if (PQenterBatchMode(conn) != 1)
		pg_fatal("re-entering batch mode should be a no-op but failed\n");

	if (PQisBusy(conn) != 0)
		pg_fatal("PQisBusy should return 0 when idle in batch, returned 1\n");

	/* ok, back to normal command mode */
	if (PQexitBatchMode(conn) != 1)
		pg_fatal("couldn't exit idle empty batch mode\n");

	if (PQbatchStatus(conn) != PQBATCH_MODE_OFF)
		pg_fatal("Batch mode not terminated properly\n");

	/* exiting batch mode when not in batch mode should be a no-op */
	if (PQexitBatchMode(conn) != 1)
		pg_fatal("batch mode exit when not in batch mode should succeed but failed\n");

	/* can now PQexec again */
	res = PQexec(conn, "SELECT 1");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		pg_fatal("PQexec should succeed after exiting batch mode but failed with: %s\n",
				 PQerrorMessage(conn));

	fprintf(stderr, "ok\n");
}

static void
test_simple_batch(PGconn *conn)
{
	PGresult   *res = NULL;
	const char *dummy_params[1] = {"1"};
	Oid			dummy_param_oids[1] = {INT4OID};

	fprintf(stderr, "simple batch... ");

	/*
	 * Enter batch mode and dispatch a set of operations, which we'll then
	 * process the results of as they come in.
	 *
	 * For a simple case we should be able to do this without interim
	 * processing of results since our out buffer will give us enough slush to
	 * work with and we won't block on sending. So blocking mode is fine.
	 */
	if (PQisnonblocking(conn))
		pg_fatal("Expected blocking connection mode\n");

	if (PQenterBatchMode(conn) != 1)
		pg_fatal("failed to enter batch mode: %s\n", PQerrorMessage(conn));

	if (PQsendQueryParams(conn, "SELECT $1",
						  1, dummy_param_oids, dummy_params,
						  NULL, NULL, 0) != 1)
		pg_fatal("dispatching SELECT failed: %s\n", PQerrorMessage(conn));

	if (PQexitBatchMode(conn) != 0)
		pg_fatal("exiting batch mode with work in progress should fail, but succeeded\n");

	if (PQbatchSendQueue(conn) != 1)
		pg_fatal("Ending a batch failed: %s\n", PQerrorMessage(conn));

	res = PQgetResult(conn);
	if (res == NULL)
		pg_fatal("PQgetResult returned null when there's a batch item: %s\n",
				 PQerrorMessage(conn));

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		pg_fatal("Unexpected result code %s from first batch item\n",
				 PQresStatus(PQresultStatus(res)));

	PQclear(res);
	res = NULL;

	if (PQgetResult(conn) != NULL)
		pg_fatal("PQgetResult returned something extra after first query result.\n");

	/*
	 * Even though we've processed the result there's still a sync to come and
	 * we can't exit batch mode yet
	 */
	if (PQexitBatchMode(conn) != 0)
		pg_fatal("exiting batch mode after query but before sync succeeded incorrectly\n");

	res = PQgetResult(conn);
	if (res == NULL)
		pg_fatal("PQgetResult returned null when sync result PGRES_BATCH_END expected: %s\n",
				 PQerrorMessage(conn));

	if (PQresultStatus(res) != PGRES_BATCH_END)
		pg_fatal("Unexpected result code %s instead of sync result, error: %s\n",
				 PQresStatus(PQresultStatus(res)), PQerrorMessage(conn));

	PQclear(res);
	res = NULL;

	if (PQgetResult(conn) != NULL)
		pg_fatal("PQgetResult returned something extra after end batch call\n");

	/* We're still in a batch... */
	if (PQbatchStatus(conn) == PQBATCH_MODE_OFF)
		pg_fatal("Fell out of batch mode somehow\n");

	/* ... until we end it, which we can safely do now */
	if (PQexitBatchMode(conn) != 1)
		pg_fatal("attempt to exit batch mode failed when it should've succeeded: %s\n",
				 PQerrorMessage(conn));

	if (PQbatchStatus(conn) != PQBATCH_MODE_OFF)
		pg_fatal("Exiting batch mode didn't seem to work\n");

	fprintf(stderr, "ok\n");
}

static void
test_multi_batch(PGconn *conn)
{
	PGresult   *res = NULL;
	const char *dummy_params[1] = {"1"};
	Oid			dummy_param_oids[1] = {INT4OID};

	fprintf(stderr, "multi batch... ");

	/*
	 * Queue up a couple of small batches and process each without returning
	 * to command mode first.
	 */
	if (PQenterBatchMode(conn) != 1)
		pg_fatal("failed to enter batch mode: %s\n", PQerrorMessage(conn));

	if (PQsendQueryParams(conn, "SELECT $1", 1, dummy_param_oids,
						  dummy_params, NULL, NULL, 0) != 1)
		pg_fatal("dispatching first SELECT failed: %s\n", PQerrorMessage(conn));

	if (PQbatchSendQueue(conn) != 1)
		pg_fatal("Ending first batch failed: %s\n", PQerrorMessage(conn));

	if (PQsendQueryParams(conn, "SELECT $1", 1, dummy_param_oids,
						  dummy_params, NULL, NULL, 0) != 1)
		pg_fatal("dispatching second SELECT failed: %s\n", PQerrorMessage(conn));

	if (PQbatchSendQueue(conn) != 1)
		pg_fatal("Ending second batch failed: %s\n", PQerrorMessage(conn));

	/* OK, start processing the batch results */
	res = PQgetResult(conn);
	if (res == NULL)
		pg_fatal("PQgetResult returned null when there's a batch item: %s\n",
				 PQerrorMessage(conn));

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		pg_fatal("Unexpected result code %s from first batch item\n",
				 PQresStatus(PQresultStatus(res)));
	PQclear(res);
	res = NULL;

	if (PQgetResult(conn) != NULL)
		pg_fatal("PQgetResult returned something extra after first result\n");

	if (PQexitBatchMode(conn) != 0)
		pg_fatal("exiting batch mode after query but before sync succeeded incorrectly\n");

	res = PQgetResult(conn);
	if (res == NULL)
		pg_fatal("PQgetResult returned null when sync result expected: %s\n",
				 PQerrorMessage(conn));

	if (PQresultStatus(res) != PGRES_BATCH_END)
		pg_fatal("Unexpected result code %s instead of sync result, error: %s\n",
				 PQresStatus(PQresultStatus(res)), PQerrorMessage(conn));

	PQclear(res);

	res = PQgetResult(conn);
	if (res != NULL)
		pg_fatal("Expected null result, got %s\n",
				 PQresStatus(PQresultStatus(res)));

	/* second batch */

	res = PQgetResult(conn);
	if (res == NULL)
		pg_fatal("PQgetResult returned null when there's a batch item: %s\n",
				 PQerrorMessage(conn));

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		pg_fatal("Unexpected result code %s from second batch item\n",
				 PQresStatus(PQresultStatus(res)));

	res = PQgetResult(conn);
	if (res != NULL)
		pg_fatal("Expected null result, got %s\n",
				 PQresStatus(PQresultStatus(res)));

	res = PQgetResult(conn);
	if (res == NULL)
		pg_fatal("PQgetResult returned null when there's a batch item: %s\n",
				 PQerrorMessage(conn));

	if (PQresultStatus(res) != PGRES_BATCH_END)
		pg_fatal("Unexpected result code %s from second end batch\n",
				 PQresStatus(PQresultStatus(res)));

	/* We're still in a batch... */
	if (PQbatchStatus(conn) == PQBATCH_MODE_OFF)
		pg_fatal("Fell out of batch mode somehow\n");

	/* until we end it, which we can safely do now */
	if (PQexitBatchMode(conn) != 1)
		pg_fatal("attempt to exit batch mode failed when it should've succeeded: %s\n",
				 PQerrorMessage(conn));

	if (PQbatchStatus(conn) != PQBATCH_MODE_OFF)
		pg_fatal("exiting batch mode didn't seem to work\n");

	fprintf(stderr, "ok\n");
}

/*
 * When an operation in a batch fails the rest of the batch is flushed. We
 * still have to get results for each batch item, but the item will just be
 * a PGRES_BATCH_ABORTED code.
 *
 * This intentionally doesn't use a transaction to wrap the batch. You should
 * usually use an xact, but in this case we want to observe the effects of each
 * statement.
 */
static void
test_batch_abort(PGconn *conn)
{
	PGresult   *res = NULL;
	const char *dummy_params[1] = {"1"};
	Oid			dummy_param_oids[1] = {INT4OID};
	int			i;

	fprintf(stderr, "aborted batch... ");

	res = PQexec(conn, drop_table_sql);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("dispatching DROP TABLE failed: %s\n", PQerrorMessage(conn));

	res = PQexec(conn, create_table_sql);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("dispatching CREATE TABLE failed: %s\n", PQerrorMessage(conn));

	/*
	 * Queue up a couple of small batches and process each without returning
	 * to command mode first. Make sure the second operation in the first
	 * batch ERRORs.
	 */
	if (PQenterBatchMode(conn) != 1)
		pg_fatal("failed to enter batch mode: %s\n", PQerrorMessage(conn));

	dummy_params[0] = "1";
	if (PQsendQueryParams(conn, insert_sql, 1, dummy_param_oids,
						  dummy_params, NULL, NULL, 0) != 1)
		pg_fatal("dispatching first insert failed: %s\n", PQerrorMessage(conn));

	if (PQsendQueryParams(conn, "SELECT no_such_function($1)",
						  1, dummy_param_oids, dummy_params,
						  NULL, NULL, 0) != 1)
		pg_fatal("dispatching error select failed: %s\n", PQerrorMessage(conn));

	dummy_params[0] = "2";
	if (PQsendQueryParams(conn, insert_sql, 1, dummy_param_oids,
						  dummy_params, NULL, NULL, 0) != 1)
		pg_fatal("dispatching second insert failed: %s\n", PQerrorMessage(conn));

	if (PQbatchSendQueue(conn) != 1)
		pg_fatal("Sending first batch failed: %s\n", PQerrorMessage(conn));

	dummy_params[0] = "3";
	if (PQsendQueryParams(conn, insert_sql, 1, dummy_param_oids,
						  dummy_params, NULL, NULL, 0) != 1)
		pg_fatal("dispatching second-batch insert failed: %s\n",
				 PQerrorMessage(conn));

	if (PQbatchSendQueue(conn) != 1)
		pg_fatal("Ending second batch failed: %s\n", PQerrorMessage(conn));

	/*
	 * OK, start processing the batch results.
	 *
	 * We should get a command-ok for the first query, then a fatal error and
	 * a batch aborted message for the second insert, a batch-end, then a
	 * command-ok and a batch-ok for the second batch operation.
	 */
	res = PQgetResult(conn);
	if (res == NULL)
		pg_fatal("Unexpected NULL result: %s", PQerrorMessage(conn));
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("Unexpected result status %s: %s\n",
				 PQresStatus(PQresultStatus(res)),
				 PQresultErrorMessage(res));
	PQclear(res);

	/* NULL result to signal end-of-results for this command */
	if ((res = PQgetResult(conn)) != NULL)
		pg_fatal("Expected null result, got %s\n",
				 PQresStatus(PQresultStatus(res)));

	/* Second query caused error, so we expect an error next */
	res = PQgetResult(conn);
	if (res == NULL)
		pg_fatal("Unexpected NULL result: %s\n", PQerrorMessage(conn));
	if (PQresultStatus(res) != PGRES_FATAL_ERROR)
		pg_fatal("Unexpected result code -- expected PGRES_FATAL_ERROR, got %s\n",
				 PQresStatus(PQresultStatus(res)));
	PQclear(res);

	/* NULL result to signal end-of-results for this command */
	if ((res = PQgetResult(conn)) != NULL)
		pg_fatal("Expected null result, got %s\n",
				 PQresStatus(PQresultStatus(res)));

	/*
	 * batch should now be aborted.
	 *
	 * Note that we could still queue more queries at this point if we wanted;
	 * they'd get added to a new third batch since we've already sent a
	 * second. The aborted flag relates only to the batch being received.
	 */
	if (PQbatchStatus(conn) != PQBATCH_MODE_ABORTED)
		pg_fatal("batch should be flagged as aborted but isn't\n");

	/* third query in batch, the second insert */
	res = PQgetResult(conn);
	if (res == NULL)
		pg_fatal("Unexpected NULL result: %s\n", PQerrorMessage(conn));
	if (PQresultStatus(res) != PGRES_BATCH_ABORTED)
		pg_fatal("Unexpected result code -- expected PGRES_BATCH_ABORTED, got %s\n",
				 PQresStatus(PQresultStatus(res)));
	PQclear(res);

	/* NULL result to signal end-of-results for this command */
	if ((res = PQgetResult(conn)) != NULL)
		pg_fatal("Expected null result, got %s\n", PQresStatus(PQresultStatus(res)));

	if (PQbatchStatus(conn) != PQBATCH_MODE_ABORTED)
		pg_fatal("batch should be flagged as aborted but isn't\n");

	/* Ensure we're still in batch */
	if (PQbatchStatus(conn) == PQBATCH_MODE_OFF)
		pg_fatal("Fell out of batch mode somehow\n");

	/*
	 * The end of a failed batch is a PGRES_BATCH_END.
	 *
	 * (This is so clients know to start processing results normally again and
	 * can tell the difference between skipped commands and the sync.)
	 */
	res = PQgetResult(conn);
	if (res == NULL)
		pg_fatal("Unexpected NULL result: %s\n", PQerrorMessage(conn));
	if (PQresultStatus(res) != PGRES_BATCH_END)
		pg_fatal("Unexpected result code from first batch sync\n"
				 "Expected PGRES_BATCH_END, got %s\n",
				 PQresStatus(PQresultStatus(res)));
	PQclear(res);

	/* XXX why do we have a NULL result after PGRES_BATCH_END? */
	res = PQgetResult(conn);
	if (res != NULL)
		pg_fatal("Expected null result, got %s\n", PQresStatus(PQresultStatus(res)));

	if (PQbatchStatus(conn) == PQBATCH_MODE_ABORTED)
		pg_fatal("sync should've cleared the aborted flag but didn't\n");

	/* We're still in a batch... */
	if (PQbatchStatus(conn) == PQBATCH_MODE_OFF)
		pg_fatal("Fell out of batch mode somehow\n");

	/* the insert from the second batch */
	res = PQgetResult(conn);
	if (res == NULL)
		pg_fatal("Unexpected NULL result: %s\n", PQerrorMessage(conn));
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("Unexpected result code %s from first item in second batch\n",
				 PQresStatus(PQresultStatus(res)));
	PQclear(res);

	/* Read the NULL result at the end of the command */
	if ((res = PQgetResult(conn)) != NULL)
		pg_fatal("Expected null result, got %s\n", PQresStatus(PQresultStatus(res)));

	/* the second batch sync */
	if ((res = PQgetResult(conn)) == NULL)
		pg_fatal("Unexpected NULL result: %s\n", PQerrorMessage(conn));
	if (PQresultStatus(res) != PGRES_BATCH_END)
		pg_fatal("Unexpected result code %s from second batch sync\n",
				 PQresStatus(PQresultStatus(res)));
	PQclear(res);

	if ((res = PQgetResult(conn)) != NULL)
		pg_fatal("Expected null result, got %s: %s\n",
				 PQresStatus(PQresultStatus(res)),
				 PQerrorMessage(conn));

	/* We're still in a batch... */
	if (PQbatchStatus(conn) == PQBATCH_MODE_OFF)
		pg_fatal("Fell out of batch mode somehow\n");

	/* until we end it, which we can safely do now */
	if (PQexitBatchMode(conn) != 1)
		pg_fatal("attempt to exit batch mode failed when it should've succeeded: %s\n",
				 PQerrorMessage(conn));

	if (PQbatchStatus(conn) != PQBATCH_MODE_OFF)
		pg_fatal("exiting batch mode didn't seem to work\n");

	fprintf(stderr, "ok\n");

	/*-
	 * Since we fired the batches off without a surrounding xact, the results
	 * should be:
	 *
	 * - Implicit xact started by server around 1st batch
	 * - First insert applied
	 * - Second statement aborted xact
	 * - Third insert skipped
	 * - Sync rolled back first implicit xact
	 * - Implicit xact created by server around 2nd batch
	 * - insert applied from 2nd batch
	 * - Sync commits 2nd xact
	 *
	 * So we should only have the value 3 that we inserted.
	 */
	res = PQexec(conn, "SELECT itemno FROM batch_demo");

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		pg_fatal("Expected tuples, got %s: %s\n",
				 PQresStatus(PQresultStatus(res)), PQerrorMessage(conn));
	if (PQntuples(res) != 1)
		pg_fatal("expected 1 result, got %d\n", PQntuples(res));
	for (i = 0; i < PQntuples(res); i++)
	{
		const char *val = PQgetvalue(res, i, 0);

		if (strcmp(val, "3") != 0)
			pg_fatal("expected only insert with value 3, got %s", val);
	}

	PQclear(res);
}

/* State machine enum for test_batch_insert */
typedef enum BatchInsertStep
{
	BI_BEGIN_TX,
	BI_DROP_TABLE,
	BI_CREATE_TABLE,
	BI_PREPARE,
	BI_INSERT_ROWS,
	BI_COMMIT_TX,
	BI_SYNC,
	BI_DONE
} BatchInsertStep;

static void
test_batch_insert(PGconn *conn, int n_rows)
{
	const char *insert_params[1];
	Oid			insert_param_oids[1] = {INT4OID};
	char		insert_param_0[MAXINTLEN];
	BatchInsertStep send_step = BI_BEGIN_TX,
				recv_step = BI_BEGIN_TX;
	int			rows_to_send,
				rows_to_receive;

	insert_params[0] = &insert_param_0[0];

	rows_to_send = rows_to_receive = n_rows;

	/*
	 * Do a batched insert into a table created at the start of the batch
	 */
	if (PQenterBatchMode(conn) != 1)
		pg_fatal("failed to enter batch mode: %s\n", PQerrorMessage(conn));

	while (send_step != BI_PREPARE)
	{
		const char *sql;

		switch (send_step)
		{
			case BI_BEGIN_TX:
				sql = "BEGIN TRANSACTION";
				send_step = BI_DROP_TABLE;
				break;

			case BI_DROP_TABLE:
				sql = drop_table_sql;
				send_step = BI_CREATE_TABLE;
				break;

			case BI_CREATE_TABLE:
				sql = create_table_sql;
				send_step = BI_PREPARE;
				break;

			default:
				pg_fatal("invalid state");
		}

		pg_debug("sending: %s\n", sql);
		if (PQsendQueryParams(conn, sql,
							  0, NULL, NULL, NULL, NULL, 0) != 1)
			pg_fatal("dispatching %s failed: %s\n", sql, PQerrorMessage(conn));
	}

	Assert(send_step == BI_PREPARE);
	pg_debug("sending: %s\n", insert_sql);
	if (PQsendPrepare(conn, "my_insert", insert_sql, 1, insert_param_oids) != 1)
		pg_fatal("dispatching PREPARE failed: %s\n", PQerrorMessage(conn));
	send_step = BI_INSERT_ROWS;

	/*
	 * Now we start inserting. We'll be sending enough data that we could fill
	 * our out buffer, so to avoid deadlocking we need to enter nonblocking
	 * mode and consume input while we send more output. As results of each
	 * query are processed we should pop them to allow processing of the next
	 * query. There's no need to finish the batch before processing results.
	 */
	if (PQsetnonblocking(conn, 1) != 0)
		pg_fatal("failed to set nonblocking mode: %s\n", PQerrorMessage(conn));

	while (recv_step != BI_DONE)
	{
		int			sock;
		fd_set		input_mask;
		fd_set		output_mask;

		sock = PQsocket(conn);

		if (sock < 0)
			break;				/* shouldn't happen */

		FD_ZERO(&input_mask);
		FD_SET(sock, &input_mask);
		FD_ZERO(&output_mask);
		FD_SET(sock, &output_mask);

		if (select(sock + 1, &input_mask, &output_mask, NULL, NULL) < 0)
		{
			fprintf(stderr, "select() failed: %s\n", strerror(errno));
			exit_nicely(conn);
		}

		/*
		 * Process any results, so we keep the server's out buffer free
		 * flowing and it can continue to process input
		 */
		if (FD_ISSET(sock, &input_mask))
		{
			PQconsumeInput(conn);

			/* Read until we'd block if we tried to read */
			while (!PQisBusy(conn) && recv_step < BI_DONE)
			{
				PGresult   *res;
				const char *cmdtag;
				const char *description = "";
				int			status;

				/*
				 * Read next result.  If no more results from this query,
				 * advance to the next query
				 */
				res = PQgetResult(conn);
				if (res == NULL)
					continue;

				status = PGRES_COMMAND_OK;
				switch (recv_step)
				{
					case BI_BEGIN_TX:
						cmdtag = "BEGIN";
						recv_step++;
						break;
					case BI_DROP_TABLE:
						cmdtag = "DROP TABLE";
						recv_step++;
						break;
					case BI_CREATE_TABLE:
						cmdtag = "CREATE TABLE";
						recv_step++;
						break;
					case BI_PREPARE:
						cmdtag = "";
						description = "PREPARE";
						recv_step++;
						break;
					case BI_INSERT_ROWS:
						cmdtag = "INSERT";
						rows_to_receive--;
						if (rows_to_receive == 0)
							recv_step++;
						break;
					case BI_COMMIT_TX:
						cmdtag = "COMMIT";
						recv_step++;
						break;
					case BI_SYNC:
						cmdtag = "";
						description = "SYNC";
						status = PGRES_BATCH_END;
						recv_step++;
						break;
					case BI_DONE:
						/* unreachable */
						description = "";
						abort();
				}

				if (PQresultStatus(res) != status)
					pg_fatal("%s reported status %s, expected %s\n"
							 "Error message: \"%s\"\n",
							 description, PQresStatus(PQresultStatus(res)),
							 PQresStatus(status), PQerrorMessage(conn));

				if (strncmp(PQcmdStatus(res), cmdtag, strlen(cmdtag)) != 0)
					pg_fatal("%s expected command tag '%s', got '%s'\n",
							 description, cmdtag, PQcmdStatus(res));

				pg_debug("Got %s OK\n", cmdtag[0] != '\0' ? cmdtag : description);

				PQclear(res);
			}
		}

		/* Write more rows and/or the end batch message, if needed */
		if (FD_ISSET(sock, &output_mask))
		{
			PQflush(conn);

			if (send_step == BI_INSERT_ROWS)
			{
				snprintf(&insert_param_0[0], MAXINTLEN, "%d", rows_to_send);

				if (PQsendQueryPrepared(conn, "my_insert",
										1, insert_params, NULL, NULL, 0) == 1)
				{
					pg_debug("sent row %d\n", rows_to_send);

					rows_to_send--;
					if (rows_to_send == 0)
						send_step = BI_COMMIT_TX;
				}
				else
				{
					/*
					 * in nonblocking mode, so it's OK for an insert to fail
					 * to send
					 */
					fprintf(stderr, "WARNING: failed to send insert #%d: %s\n",
							rows_to_send, PQerrorMessage(conn));
				}
			}
			else if (send_step == BI_COMMIT_TX)
			{
				if (PQsendQueryParams(conn, "COMMIT",
									  0, NULL, NULL, NULL, NULL, 0) == 1)
				{
					pg_debug("sent COMMIT\n");
					send_step = BI_SYNC;
				}
				else
				{
					fprintf(stderr, "WARNING: failed to send commit: %s\n",
							PQerrorMessage(conn));
				}
			}
			else if (send_step == BI_SYNC)
			{
				if (PQbatchSendQueue(conn) == 1)
				{
					fprintf(stdout, "Dispatched end batch message\n");
					send_step = BI_DONE;
				}
				else
				{
					fprintf(stderr, "WARNING: Ending a batch failed: %s\n",
							PQerrorMessage(conn));
				}
			}
		}
	}

	/* We've got the sync message and the batch should be done */
	if (PQexitBatchMode(conn) != 1)
		pg_fatal("attempt to exit batch mode failed when it should've succeeded: %s\n",
				 PQerrorMessage(conn));

	if (PQsetnonblocking(conn, 0) != 0)
		pg_fatal("failed to clear nonblocking mode: %s\n", PQerrorMessage(conn));
}

static void
test_singlerowmode(PGconn *conn)
{
	PGresult   *res;
	int			i;
	bool		batch_ended = false;

	/* 1 batch, 3 queries in it */
	if (PQenterBatchMode(conn) != 1)
		pg_fatal("failed to enter batch mode: %s\n",
				 PQerrorMessage(conn));

	for (i = 0; i < 3; i++)
	{
		char   *param[1];

		param[0] = psprintf("%d", 44 + i);

		if (PQsendQueryParams(conn,
							  "SELECT generate_series(42, $1)",
							  1,
							  NULL,
							  (const char **) param,
							  NULL,
							  NULL,
							  0) != 1)
			pg_fatal("failed to send query: %s\n",
					 PQerrorMessage(conn));
		pfree(param[0]);
	}
	PQbatchSendQueue(conn);

	for (i = 0; !batch_ended; i++)
	{
		bool		first = true;
		bool		saw_ending_tuplesok;
		bool		isSingleTuple = false;

		/* Set single row mode for only first 2 SELECT queries */
		if (i < 2)
		{
			if (PQsetSingleRowMode(conn) != 1)
				pg_fatal("PQsetSingleRowMode() failed for i=%d\n", i);
		}

		/* Consume rows for this query */
		saw_ending_tuplesok = false;
		while ((res = PQgetResult(conn)) != NULL)
		{
			ExecStatusType est = PQresultStatus(res);

			if (est == PGRES_BATCH_END)
			{
				fprintf(stderr, "end of batch reached\n");
				batch_ended = true;
				PQclear(res);
				if (i != 3)
					pg_fatal("Expected three results, got %d\n", i);
				break;
			}

			/* Expect SINGLE_TUPLE for queries 0 and 1, TUPLES_OK for 2 */
			if (first)
			{
				if (i <= 1 && est != PGRES_SINGLE_TUPLE)
					pg_fatal("Expected PGRES_SINGLE_TUPLE for query %d, got %s\n",
							 i, PQresStatus(est));
				if (i >= 2 && est != PGRES_TUPLES_OK)
					pg_fatal("Expected PGRES_TUPLES_OK for query %d, got %s\n",
							 i, PQresStatus(est));
				first = false;
			}

			fprintf(stderr, "Result status %s for query %d", PQresStatus(est), i);
			switch (est)
			{
				case PGRES_TUPLES_OK:
					fprintf(stderr, ", tuples: %d\n", PQntuples(res));
					saw_ending_tuplesok = true;
					if (isSingleTuple)
					{
						if (PQntuples(res) == 0)
							fprintf(stderr, "all tuples received in query %d\n", i);
						else
							pg_fatal("Expected to follow PGRES_SINGLE_TUPLE, "
									 "but received PGRES_TUPLES_OK directly instead\n");
					}
					break;

				case PGRES_SINGLE_TUPLE:
					fprintf(stderr, ", %d tuple: %s\n", PQntuples(res), PQgetvalue(res, 0, 0));
					break;

				default:
					pg_fatal("unexpected\n");
			}
			PQclear(res);
		}
		if (!batch_ended && !saw_ending_tuplesok)
			pg_fatal("didn't get expected terminating TUPLES_OK\n");
	}

	if (PQexitBatchMode(conn) != 1)
		pg_fatal("failed to end batch mode: %s\n", PQerrorMessage(conn));
}

static void
usage(const char *progname)
{
	fprintf(stderr, "%s tests libpq's batch-mode.\n\n", progname);
	fprintf(stderr, "Usage:\n");
	fprintf(stderr, "  %s testname [conninfo [number_of_rows]]\n", progname);
	fprintf(stderr, "Tests:\n");
	fprintf(stderr, "  disallowed_in_batch|simple_batch|multi_batch|batch_abort|\n");
	fprintf(stderr, "  singlerow|batch_insert\n");
}

int
main(int argc, char **argv)
{
	const char *conninfo = "";
	PGconn	   *conn;
	int			numrows = 10000;

	/*
	 * The testname parameter is mandatory; it can be followed by a conninfo
	 * string and number of rows.
	 */
	if (argc < 2 || argc > 4)
	{
		usage(argv[0]);
		exit(1);
	}

	if (argc >= 3)
		conninfo = pg_strdup(argv[2]);

	if (argc >= 4)
	{
		errno = 0;
		numrows = strtol(argv[3], NULL, 10);
		if (errno != 0 || numrows <= 0)
		{
			fprintf(stderr, "couldn't parse \"%s\" as a positive integer\n", argv[3]);
			exit(1);
		}
	}

	/* Make a connection to the database */
	conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s\n",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}

	if (strcmp(argv[1], "disallowed_in_batch") == 0)
		test_disallowed_in_batch(conn);
	else if (strcmp(argv[1], "simple_batch") == 0)
		test_simple_batch(conn);
	else if (strcmp(argv[1], "multi_batch") == 0)
		test_multi_batch(conn);
	else if (strcmp(argv[1], "batch_abort") == 0)
		test_batch_abort(conn);
	else if (strcmp(argv[1], "batch_insert") == 0)
		test_batch_insert(conn, numrows);
	else if (strcmp(argv[1], "singlerow") == 0)
		test_singlerowmode(conn);
	else
	{
		fprintf(stderr, "\"%s\" is not a recognized test name\n", argv[1]);
		usage(argv[0]);
		exit(1);
	}

	/* close the connection to the database and cleanup */
	PQfinish(conn);
	return 0;
}
