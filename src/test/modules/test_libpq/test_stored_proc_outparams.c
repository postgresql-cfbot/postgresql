/*
 * src/test/examples/testlibpq.c
 *
 *
 * testlibpq.c
 *
 *      Test the C version of libpq, the PostgreSQL frontend library.
 */
#include <stdio.h>
#include <stdlib.h>
#include "libpq-fe.h"

static void test_sp_out(PGconn *conn, const char *query);

static void
exit_nicely(PGconn *conn)
{
	PQfinish(conn);
	exit(1);
}

int
main(int argc, char **argv)
{
	const char *conninfo;
	const char *query;
	PGconn	   *conn;

	/* required args: conninfo and query string */
	if (argc != 3)
	{
		fprintf(stderr, "usage: %s conninfo querystring\n", argv[0]);
		exit(2);
	}
	conninfo = argv[1];
	query = argv[2];

	/* Make a connection to the database */
	conn = PQconnectdb(conninfo);

	/* Check to see that the backend connection was successfully made */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}

	test_sp_out(conn, query);

	/* close the connection to the database and cleanup */
	PQfinish(conn);

	return 0;
}

#define INTOID 23
#define UNKNOWNOID 705

void
test_sp_out(PGconn *conn, const char *query)
{
	/*
	 * We expect a call to a procedure with one IN param and 2 OUT params. The
	 * query needs to match this pattern for success. Casts are needed in the
	 * query (only) to disambiguate. The IN param here has a hardcoded value
	 * of 2.
	 */

	PGresult   *res;
	const PQprintOpt opt = {1, 1, 0, 0, 0, 0, "|", NULL, NULL, NULL};
	int			nParams = 3;
	Oid			paramTypes[3] = {INTOID, UNKNOWNOID, UNKNOWNOID};
	const char *paramValues[3] = {"2", NULL, NULL};
	int			resultFormat = 0;

	res = PQexecParams(conn, query, nParams, paramTypes, paramValues,
					   NULL, NULL, resultFormat);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "CALL failed: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	PQprint(stdout, res, &opt);

}
