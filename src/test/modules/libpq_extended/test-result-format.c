#include "postgres_fe.h"

#include <stdio.h>
#include <stdlib.h>
#include "libpq-fe.h"

int
main(int argc, char *argv[])
{
	PGconn	   *conn;
	PGresult   *res;

	conn = PQconnectdb("");

	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit(1);
	}

	res = PQexecParams(conn, "SELECT 1::int4, 2::int8, 'abc'::text",
					   0, NULL, NULL, NULL, NULL,
					   -1);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "query failed: %s", PQerrorMessage(conn));
		exit(1);
	}

	for (int i = 0; i < PQnfields(res); i++)
	{
		printf("%d->%d ", i, PQfformat(res, i));
	}

	PQfinish(conn);
	return 0;
}
