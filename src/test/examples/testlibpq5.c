/*
 * src/test/examples/testlibpq5.c
 *
 * testlibpq5.c
 *		Test logging of statement parameters in case of errors.
 */

#ifdef WIN32
#include <windows.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>
#include "libpq-fe.h"

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
	PGconn	   *conn;
	PGresult   *res;
	const char *paramValues[3];
	int			paramLengths[3];
	int			paramFormats[3];
	uint32_t	binaryIntVal;

	/*
	 * If the user supplies a parameter on the command line, use it as the
	 * conninfo string; otherwise default to setting dbname=postgres and using
	 * environment variables or defaults for all other connection parameters.
	 */
	if (argc > 1)
		conninfo = argv[1];
	else
		conninfo = "dbname = postgres";

	/* Make a connection to the database */
	conn = PQconnectdb(conninfo);

	/* Check to see that the backend connection was successfully made */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}

	/* Set always-secure search path, so malicious users can't take control. */
	res = PQexec(conn,
				 "SELECT pg_catalog.set_config('search_path', '', false)");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "SET failed: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	PQclear(res);

	/*
	 * Transmit parameters in different forms and make a statement fail.  User
	 * can then verify the server log.
	 */
	res = PQexec(conn, "SET log_parameters_on_error = on");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "SET failed: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	PQclear(res);

	paramValues[0] = (char *) &binaryIntVal;
	paramLengths[0] = sizeof(binaryIntVal);
	paramFormats[0] = 1;		/* binary */
	paramValues[1] = "2";
	paramLengths[1] = strlen(paramValues[1]) + 1;
	paramFormats[1] = 0;		/* text */
	paramValues[2] = (char *) "everyone's little $$ in \"dollar\"";
	paramLengths[2] = strlen(paramValues[2]) + 1;
	paramFormats[2] = 0;		/* text */
	/* divide by zero -- but server won't realize until execution */
	res = PQexecParams(conn,
					   "SELECT 1 / (random() / 2)::int + $1::int + $2::int, $3::text",
					   3,		/* # params */
					   NULL,	/* let the backend deduce param type */
					   paramValues,
					   paramLengths,
					   paramFormats,
					   1);		/* ask for binary results */

	if (PQresultStatus(res) != PGRES_FATAL_ERROR)
	{
		fprintf(stderr, "SELECT succeeded but was supposed to fail");
		PQclear(res);
		exit_nicely(conn);
	}
	PQclear(res);
	printf("Division by zero expected, got an error message from server: %s\n"
		   "Please make sure it has been logged with bind parameters in server log\n",
		   PQerrorMessage(conn));

	/* close the connection to the database and cleanup */
	PQfinish(conn);

	return 0;
}
