/*
 * Copyright (c) 2021-2023, PostgreSQL Global Development Group
 */

#include "postgres_fe.h"

#include "libpq-fe.h"


/*
 * Test calls that don't support encryption
 */
static int
test1(PGconn *conn)
{
	PGresult   *res;
	const char *values[] = {"3", "val3", "33"};

	res = PQprepare(conn, "", "INSERT INTO tbl1 (a, b, c) VALUES ($1, $2, $3)",
					3, NULL);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "PQprepare() failed: %s\n",
				PQerrorMessage(conn));
		return 1;
	}

	res = PQexecPrepared(conn, "", 3, values, NULL, NULL, 0);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "PQexecPrepared() failed: %s\n",
				PQerrorMessage(conn));
		return 1;
	}

	return 0;
}

/*
 * Test forced encryption
 */
static int
test2(PGconn *conn)
{
	PGresult   *res,
			   *res2;
	const char *values[] = {"3", "val3", "33"};
	int			formats[] = {0x00, 0x10, 0x00};

	res = PQprepare(conn, "", "INSERT INTO tbl1 (a, b, c) VALUES ($1, $2, $3)",
					3, NULL);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "PQprepare() failed: %s\n",
				PQerrorMessage(conn));
		return 1;
	}

	res2 = PQdescribePrepared(conn, "");
	if (PQresultStatus(res2) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "PQdescribePrepared() failed: %s\n",
				PQerrorMessage(conn));
		return 1;
	}

	if (!(!PQparamisencrypted(res2, 0) &&
		  PQparamisencrypted(res2, 1)))
	{
		fprintf(stderr, "wrong results from PQparamisencrypted()\n");
		return 1;
	}

	res = PQexecPreparedDescribed(conn, "", 3, values, NULL, formats, 0, res2);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "PQexecPrepared() failed: %s\n",
				PQerrorMessage(conn));
		return 1;
	}

	return 0;
}

/*
 * Test what happens when you supply a binary parameter that is required to be
 * encrypted.
 */
static int
test3(PGconn *conn)
{
	PGresult   *res;
	const char *values[] = {""};
	int			lengths[] = {1};
	int			formats[] = {1};

	res = PQexecParams(conn, "INSERT INTO tbl1 (a, b, c) VALUES (100, NULL, $1)",
					   3, NULL, values, lengths, formats, 0);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "PQexecParams() failed: %s\n",
				PQerrorMessage(conn));
		return 1;
	}

	return 0;
}

/*
 * Test what happens when you request results in binary and the result rows
 * contain an encrypted column.
 */
static int
test4(PGconn *conn)
{
	PGresult   *res;

	res = PQexecParams(conn, "SELECT a, b, c FROM tbl1 WHERE a = 1", 0, NULL, NULL, NULL, NULL, 1);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "PQexecParams() failed: %s\n",
				PQerrorMessage(conn));
		return 1;
	}

	for (int row = 0; row < PQntuples(res); row++)
		for (int col = 0; col < PQnfields(res); col++)
			printf("<%d,%d>=%d:%s\n", row, col, PQfformat(res, col), PQgetvalue(res, row, col));
	return 0;
}

int
main(int argc, char **argv)
{
	PGconn	   *conn;
	int			ret = 0;

	conn = PQconnectdb("");
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s\n",
				PQerrorMessage(conn));
		return 1;
	}

	if (argc < 2 || argv[1] == NULL)
		return 87;
	else if (strcmp(argv[1], "test1") == 0)
		ret = test1(conn);
	else if (strcmp(argv[1], "test2") == 0)
		ret = test2(conn);
	else if (strcmp(argv[1], "test3") == 0)
		ret = test3(conn);
	else if (strcmp(argv[1], "test4") == 0)
		ret = test4(conn);
	else
		ret = 88;

	PQfinish(conn);
	return ret;
}
