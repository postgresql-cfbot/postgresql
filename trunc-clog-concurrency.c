#include <libpq-fe.h>

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

static void
report_query_failure(const char *query, PGresult *res)
{
	fprintf(stderr, "query \"%s\" failed unexpectedly: %s",
			query, PQresultErrorMessage(res));
}

static void
safe_query(PGconn *conn, const char *query)
{
	PGresult *res;

	res = PQexec(conn, query);
	if (PQresultStatus(res) != PGRES_TUPLES_OK &&
		PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		report_query_failure(query, res);
		exit(1);
	}
	PQclear(res);
}

static int
is_stop_limit(PGresult *res)
{
	return PQresultStatus(res) == PGRES_FATAL_ERROR
		&& strcmp(PQresultErrorField(res, PG_DIAG_SQLSTATE), "54000") == 0;
}

int
main(int argc, char **argv)
{
	bool reached_stop_limit = false;
	PGconn *mutate_conn, *hold1_conn, *hold2_conn, *burn_conn;
	PGresult *res;
	int n_burns = 0, n_inserts = 0;

	if (argc != 1)
	{
		fputs("Usage: trunc-clog-concurrency\n", stderr);
		return 1;
	}

	mutate_conn = PQconnectdb("");
	if (PQstatus(mutate_conn) != CONNECTION_OK)
	{
		fprintf(stderr, "PGconnectdb failed: %s", PQerrorMessage(mutate_conn));
		return 1;
	}

	hold1_conn = PQconnectdb("");
	if (PQstatus(hold1_conn) != CONNECTION_OK)
	{
		fprintf(stderr, "PGconnectdb failed: %s", PQerrorMessage(hold1_conn));
		return 1;
	}

	hold2_conn = PQconnectdb("");
	if (PQstatus(hold2_conn) != CONNECTION_OK)
	{
		fprintf(stderr, "PGconnectdb failed: %s", PQerrorMessage(hold2_conn));
		return 1;
	}

	burn_conn = PQconnectdb("options=--JJ_xid=1000000");
	if (PQstatus(burn_conn) != CONNECTION_OK)
	{
		fprintf(stderr, "PGconnectdb failed: %s", PQerrorMessage(burn_conn));
		return 1;
	}

	/* Start a transaction having an xid. */
	safe_query(mutate_conn, "BEGIN;");
	safe_query(mutate_conn, "DROP TABLE IF EXISTS trunc_clog_concurrency;");
	safe_query(mutate_conn, "CREATE TABLE trunc_clog_concurrency ();");

	/* Burn the entire XID space. */
	while (!reached_stop_limit)
	{
		const char query[] = "SELECT txid_current();";
		res = PQexec(burn_conn, query);
		if (PQresultStatus(res) == PGRES_TUPLES_OK)
		{
			++n_burns;
			if (n_burns == 2)
			{
				safe_query(hold1_conn, "BEGIN ISOLATION LEVEL READ COMMITTED;");
				safe_query(hold1_conn, "CREATE TABLE trunc_clog_concurrency_hold1 ();");
			}
			if (n_burns == 10)
			{
				safe_query(hold2_conn, "BEGIN ISOLATION LEVEL READ COMMITTED;");
				safe_query(hold2_conn, "CREATE TABLE trunc_clog_concurrency_hold2 ();");
				system("psql -Xc 'select state, backend_xid, backend_xmin, query from pg_stat_activity'");
				system("psql -Xc 'select datname,datallowconn,datfrozenxid from pg_database'");
			}
			/* keep burning */;
		}
		else if (is_stop_limit(res))
		{
			reached_stop_limit = true;
			fprintf(stderr, "reached stop limit: %s",
					PQresultErrorMessage(res));
		}
		else
		{
			reached_stop_limit = true; /* FIXME not really */
			report_query_failure(query, res);
		}
		PQclear(res);
	}

	/* Finish the first transaction.  xmin raises from start to start+2M. */
	safe_query(mutate_conn, "COMMIT;");

	/* Raise datfrozenxid of all but template1 to start+2M.  No truncation. */
	system("for d in postgres template0 test; do vacuumdb -F $d; done; "
		   "echo -n 'DONE1 '; date");
	/* Raise xmin to start+10M */
	safe_query(hold1_conn, "COMMIT;");
	/* Sleep on lock before truncating to start+2M. */
	system("(vacuumdb -F template1; echo -n 'DONE2 '; date) &");
	usleep(4000*1000); /* 4s */

	/* Truncate to start+10M. */
	system("(vacuumdb -aF; echo -n 'DONE3 '; date)");
	system("psql -Xc 'select state, backend_xid, backend_xmin, query from pg_stat_activity'");
	system("psql -Xc 'select datname,datallowconn,datfrozenxid from pg_database'");

	/*
	 * We want to burn at least 1M xids (the amount protected by xidStopLimit)
	 * but not more than 200M (autovacuum_freeze_max_age default) to avoid a
	 * second set of VACUUMs.
	 */
	while (n_inserts < 150)
	{
		const char query[] =
			"INSERT INTO trunc_clog_concurrency DEFAULT VALUES";
		res = PQexec(burn_conn, query);
		if (PQresultStatus(res) == PGRES_COMMAND_OK)
		{
			n_inserts++;
			fprintf(stderr, "insert %d ", n_inserts);
			system("date >&2");
		}
		else if (is_stop_limit(res))
		{
			fprintf(stderr, "reached stop limit: %s",
					PQresultErrorMessage(res));
			break;
		}
		else
		{
			report_query_failure(query, res);
			return 1;
		}
		PQclear(res);
	}

	system("psql -Xc 'select state, backend_xid, backend_xmin, query from pg_stat_activity'");
	system("psql -Xc 'select datname,datallowconn,datfrozenxid from pg_database'");

	PQfinish(mutate_conn);
	PQfinish(hold1_conn);
	PQfinish(hold2_conn);
	PQfinish(burn_conn);

	return 0;
}
