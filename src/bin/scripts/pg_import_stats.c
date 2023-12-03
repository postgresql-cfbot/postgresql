/*-------------------------------------------------------------------------
 *
 * pg_import_stats
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/scripts/pg_import_stats.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#include "common.h"
#include "common/logging.h"
#include "fe_utils/cancel.h"
#include "fe_utils/option_utils.h"
#include "fe_utils/query_utils.h"
#include "fe_utils/simple_list.h"
#include "fe_utils/string_utils.h"

#define COPY_BUF_LEN 8192

static void help(const char *progname);

int
main(int argc, char *argv[])
{
	static struct option long_options[] = {
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"username", required_argument, NULL, 'U'},
		{"no-password", no_argument, NULL, 'w'},
		{"password", no_argument, NULL, 'W'},
		{"quiet", no_argument, NULL, 'q'},
		{"dbname", required_argument, NULL, 'd'},
		{NULL, 0, NULL, 0}
	};

	const char *progname;
	int			optindex;
	int			c;

	const char *dbname = NULL;
	char	   *host = NULL;
	char	   *port = NULL;
	char	   *username = NULL;
	enum trivalue prompt_password = TRI_DEFAULT;
	ConnParams	cparams;
	bool		quiet = false;

	PGconn	   *conn;

	FILE	   *copysrc= stdin;

	PGresult   *result;

	int		i;
	int		numtables;

	pg_logging_init(argv[0]);
	progname = get_progname(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pgscripts"));

	handle_help_version_opts(argc, argv, "clusterdb", help);

	while ((c = getopt_long(argc, argv, "d:h:p:qU:wW", long_options, &optindex)) != -1)
	{
		switch (c)
		{
			case 'd':
				dbname = pg_strdup(optarg);
				break;
			case 'h':
				host = pg_strdup(optarg);
				break;
			case 'p':
				port = pg_strdup(optarg);
				break;
			case 'q':
				quiet = true;
				break;
			case 'U':
				username = pg_strdup(optarg);
				break;
			case 'w':
				prompt_password = TRI_NO;
				break;
			case 'W':
				prompt_password = TRI_YES;
				break;
			default:
				/* getopt_long already emitted a complaint */
				pg_log_error_hint("Try \"%s --help\" for more information.", progname);
				exit(1);
		}
	}

	/*
	 * Non-option argument specifies database name as long as it wasn't
	 * already specified with -d / --dbname
	 */
	if (optind < argc && dbname == NULL)
	{
		dbname = argv[optind];
		optind++;
	}

	if (optind < argc)
	{
		pg_log_error("too many command-line arguments (first is \"%s\")",
					 argv[optind]);
		pg_log_error_hint("Try \"%s --help\" for more information.", progname);
		exit(1);
	}

	/* fill cparams except for dbname, which is set below */
	cparams.pghost = host;
	cparams.pgport = port;
	cparams.pguser = username;
	cparams.prompt_password = prompt_password;
	cparams.override_dbname = NULL;

	setup_cancel_handler(NULL);

	if (dbname == NULL)
	{
		if (getenv("PGDATABASE"))
			dbname = getenv("PGDATABASE");
		else if (getenv("PGUSER"))
			dbname = getenv("PGUSER");
		else
			dbname = get_user_name_or_exit(progname);
	}

	cparams.dbname = dbname;

	conn = connectDatabase(&cparams, progname, false, false, true);

	/* open file */

	/* iterate over records */


	result = PQexec(conn, 
		"CREATE TEMPORARY TABLE import_stats ( "
		"id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
		"schemaname text, relname text, server_version_num integer, "
		"n_tuples float4, n_pages integer, stats jsonb )");

	if (PQresultStatus(result) != PGRES_COMMAND_OK)
		pg_fatal("could not create temporary file: %s", PQerrorMessage(conn));

	PQclear(result);

	result = PQexec(conn, 
		"COPY import_stats(schemaname, relname, server_version_num, n_tuples, "
		"n_pages, stats) FROM STDIN");

	if (PQresultStatus(result) != PGRES_COPY_IN)
		pg_fatal("error copying data to import_stats: %s", PQerrorMessage(conn));

	for (;;)
	{
		char copybuf[COPY_BUF_LEN];

		int numread = fread(copybuf, 1, COPY_BUF_LEN, copysrc);

		if (ferror(copysrc))
			pg_fatal("error reading from source");

		if (numread == 0)
			break;

		if (PQputCopyData(conn, copybuf, numread) == -1)
			pg_fatal("eror during copy: %s", PQerrorMessage(conn));
	}

	if (PQputCopyEnd(conn, NULL) == -1)
		pg_fatal("eror during copy: %s", PQerrorMessage(conn));
	fclose(copysrc);

	result = PQgetResult(conn);

	if (PQresultStatus(result) != PGRES_COMMAND_OK)
		pg_fatal("error copying data to import_stats: %s", PQerrorMessage(conn));

	numtables = atol(PQcmdTuples(result));

	PQclear(result);

	result = PQprepare(conn, "import", 
		"SELECT pg_import_rel_stats(c.oid, s.server_version_num, "
		"             s.n_tuples, s.n_pages, s.stats) as import_result "
		"FROM import_stats AS s "
		"JOIN pg_namespace AS n ON n.nspname = s.schemaname "
		"JOIN pg_class AS c ON c.relnamespace = n.oid " 
		"                   AND c.relname = s.relname "
		"WHERE s.id = $1::bigint ",
		1, NULL);

	if (PQresultStatus(result) != PGRES_COMMAND_OK)
		pg_fatal("error in PREPARE: %s", PQerrorMessage(conn));

	PQclear(result);

	if (!quiet)
	{
		result = PQprepare(conn, "echo", 
			"SELECT s.schemaname, s.relname "
			"FROM import_stats AS s "
			"WHERE s.id = $1::bigint ",
			1, NULL);

		if (PQresultStatus(result) != PGRES_COMMAND_OK)
			pg_fatal("error in PREPARE: %s", PQerrorMessage(conn));

		PQclear(result);
	}

	for (i = 1; i <= numtables; i++)
	{
		char	istr[32];
		char   *schema = NULL;
		char   *table = NULL;

		const char *const values[] = {istr};

		snprintf(istr, 32, "%d", i);

		if (!quiet)
		{
			result = PQexecPrepared(conn, "echo", 1, values, NULL, NULL, 0);
			schema = pg_strdup(PQgetvalue(result, 0, 0));
			table = pg_strdup(PQgetvalue(result, 0, 1));
		}

		PQclear(result);

		result = PQexecPrepared(conn, "import", 1, values, NULL, NULL, 0);

		if (quiet)
		{
			PQclear(result);
			continue;
		}

		if (PQresultStatus(result) == PGRES_TUPLES_OK)
		{
			int 	rows = PQntuples(result);

			if (rows == 1)
			{
				char   *retval = PQgetvalue(result, 0, 0);
				if (*retval == 't')
					printf("%s.%s: imported\n", schema, table);
				else
					printf("%s.%s: failed\n", schema, table);
			}
			else if (rows == 0)
				printf("%s.%s: not found\n", schema, table);
			else
				pg_fatal("import function must return 0 or 1 rows");
		}
		else
			printf("%s.%s: error: %s\n", schema, table, PQerrorMessage(conn));

		if (schema != NULL)
			pfree(schema);

		if (table != NULL)
			pfree(table);

		PQclear(result);
	}

	exit(0);
}


static void
help(const char *progname)
{
	printf(_("%s clusters all previously clustered tables in a database.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]... [DBNAME]\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_("  -d, --dbname=DBNAME       database to cluster\n"));
	printf(_("  -q, --quiet               don't write any messages\n"));
	printf(_("  -t, --table=TABLE         cluster specific table(s) only\n"));
	printf(_("  -V, --version             output version information, then exit\n"));
	printf(_("  -?, --help                show this help, then exit\n"));
	printf(_("\nConnection options:\n"));
	printf(_("  -h, --host=HOSTNAME       database server host or socket directory\n"));
	printf(_("  -p, --port=PORT           database server port\n"));
	printf(_("  -U, --username=USERNAME   user name to connect as\n"));
	printf(_("  -w, --no-password         never prompt for password\n"));
	printf(_("  -W, --password            force password prompt\n"));
	printf(_("  --maintenance-db=DBNAME   alternate maintenance database\n"));
	printf(_("\nRead the description of the SQL command CLUSTER for details.\n"));
	printf(_("\nReport bugs to <%s>.\n"), PACKAGE_BUGREPORT);
	printf(_("%s home page: <%s>\n"), PACKAGE_NAME, PACKAGE_URL);
}
