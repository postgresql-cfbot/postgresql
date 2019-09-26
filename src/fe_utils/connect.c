/*-------------------------------------------------------------------------
 *
 * Utility functions to connect to a backend.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/fe_utils/connect.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "common/logging.h"
#include "fe_utils/connect.h"

#include "libpq-fe.h"

/*
 * connect_with_password_prompt
 *
 * Connect to a PostgreSQL backend with the given set of connection
 * parameters, optionally asking for password prompting.
 *
 * The caller can optionally pass a password with "saved_password"
 * which will be used at the first connection attempt.
 *
 * The result is the connection to the backend.  On error, the result
 * is NULL and an error is logged.
 */
PGconn *
connect_with_password_prompt(const char *hostname,
							 const char *port,
							 const char *username,
							 const char *dbname,
							 const char *progname,
							 char *saved_password,
							 bool enable_prompt)
{
	PGconn     *conn;
	bool        new_pass;
	static bool have_password = false;
	static char password[100];

	if (saved_password != NULL)
	{
		/* XXX: need to be more careful with overflows here */
		memcpy(password, saved_password, strlen(saved_password) + 1);
		have_password = true;
	}

	/*
	 * Start the connection.  Loop until we have a password if requested by
	 * backend.
	 */
	do
	{
#define PARAMS_ARRAY_SIZE	7

		const char *keywords[PARAMS_ARRAY_SIZE];
		const char *values[PARAMS_ARRAY_SIZE];

		keywords[0] = "host";
		values[0] = hostname;
		keywords[1] = "port";
		values[1] = port;
		keywords[2] = "user";
		values[2] = username;
		keywords[3] = "password";
		values[3] = have_password ? password : NULL;
		keywords[4] = "dbname";
		values[4] = dbname;
		keywords[5] = "fallback_application_name";
		values[5] = progname;
		keywords[6] = NULL;
		values[6] = NULL;

		new_pass = false;
		conn = PQconnectdbParams(keywords, values, true);

		if (!conn)
		{
			pg_log_error("could not connect to database \"%s\"",
						 dbname);
			return NULL;
		}

		if (PQstatus(conn) == CONNECTION_BAD &&
			PQconnectionNeedsPassword(conn) &&
			!have_password &&
			enable_prompt)
		{
			PQfinish(conn);
			simple_prompt("Password: ", password, sizeof(password), false);
			have_password = true;
			new_pass = true;
		}
	} while (new_pass);

	/* check to see that the backend connection was successfully made */
	if (PQstatus(conn) == CONNECTION_BAD)
	{
		pg_log_error("could not connect to database \"%s\": %s",
					 dbname, PQerrorMessage(conn));
		PQfinish(conn);
		return NULL;
	}

	return conn;
}
