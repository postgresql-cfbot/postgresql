/*-------------------------------------------------------------------------
 *
 * pg_amcheck.c
 *		Detects corruption within database relations.
 *
 * Copyright (c) 2017-2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_amcheck/pg_amcheck.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "catalog/pg_class.h"
#include "common/connect.h"
#include "common/logging.h"
#include "common/username.h"
#include "fe_utils/cancel.h"
#include "fe_utils/connect_utils.h"
#include "fe_utils/option_utils.h"
#include "fe_utils/parallel_slot.h"
#include "fe_utils/query_utils.h"
#include "fe_utils/simple_list.h"
#include "fe_utils/string_utils.h"
#include "getopt_long.h"		/* pgrminclude ignore */
#include "libpq-fe.h"
#include "pg_amcheck.h"
#include "pqexpbuffer.h"		/* pgrminclude ignore */
#include "storage/block.h"

/* Keep this in order by CheckType */
static const CheckTypeFilter ctfilter[] = {
	{
		.relam = HEAP_TABLE_AM_OID,
		.relkinds = CppAsString2(RELKIND_RELATION) ","
		CppAsString2(RELKIND_MATVIEW) ","
		CppAsString2(RELKIND_TOASTVALUE),
		.typname = "heap"
	},
	{
		.relam = BTREE_AM_OID,
		.relkinds = CppAsString2(RELKIND_INDEX),
		.typname = "btree index"
	}
};

/* Query for determining if contrib's amcheck is installed */
static const char *amcheck_sql =
"SELECT 1"
"\nFROM pg_catalog.pg_extension"
"\nWHERE extname OPERATOR(pg_catalog.=) 'amcheck'";


int
main(int argc, char *argv[])
{
	static struct option long_options[] = {
		/* Connection options */
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"username", required_argument, NULL, 'U'},
		{"no-password", no_argument, NULL, 'w'},
		{"password", no_argument, NULL, 'W'},
		{"maintenance-db", required_argument, NULL, 1},

		/* check options */
		{"all", no_argument, NULL, 'a'},
		{"dbname", required_argument, NULL, 'd'},
		{"exclude-dbname", required_argument, NULL, 'D'},
		{"echo", no_argument, NULL, 'e'},
		{"heapallindexed", no_argument, NULL, 'H'},
		{"index", required_argument, NULL, 'i'},
		{"exclude-index", required_argument, NULL, 'I'},
		{"jobs", required_argument, NULL, 'j'},
		{"parent-check", no_argument, NULL, 'P'},
		{"quiet", no_argument, NULL, 'q'},
		{"relation", required_argument, NULL, 'r'},
		{"exclude-relation", required_argument, NULL, 'R'},
		{"schema", required_argument, NULL, 's'},
		{"exclude-schema", required_argument, NULL, 'S'},
		{"table", required_argument, NULL, 't'},
		{"exclude-table", required_argument, NULL, 'T'},
		{"verbose", no_argument, NULL, 'v'},
		{"exclude-indexes", no_argument, NULL, 2},
		{"exclude-toast", no_argument, NULL, 3},
		{"exclude-toast-pointers", no_argument, NULL, 4},
		{"on-error-stop", no_argument, NULL, 5},
		{"skip", required_argument, NULL, 6},
		{"startblock", required_argument, NULL, 7},
		{"endblock", required_argument, NULL, 8},
		{"rootdescend", no_argument, NULL, 9},
		{"no-dependents", no_argument, NULL, 10},

		{NULL, 0, NULL, 0}
	};

	const char *progname;
	int			optindex;
	int			c;

	const char *maintenance_db = NULL;
	const char *connect_db = NULL;
	const char *host = NULL;
	const char *port = NULL;
	const char *username = NULL;
	enum trivalue prompt_password = TRI_DEFAULT;
	ConnParams	cparams;

	amcheckOptions checkopts = {
		.alldb = false,
		.echo = false,
		.quiet = false,
		.verbose = false,
		.dependents = true,
		.no_indexes = false,
		.on_error_stop = false,
		.parent_check = false,
		.rootdescend = false,
		.heapallindexed = false,
		.exclude_toast = false,
		.reconcile_toast = true,
		.skip = "none",
		.jobs = -1,
		.startblock = -1,
		.endblock = -1
	};

	amcheckObjects objects = {
		.databases = {NULL, NULL},
		.schemas = {NULL, NULL},
		.tables = {NULL, NULL},
		.indexes = {NULL, NULL},
		.exclude_databases = {NULL, NULL},
		.exclude_schemas = {NULL, NULL},
		.exclude_tables = {NULL, NULL},
		.exclude_indexes = {NULL, NULL}
	};

	pg_logging_init(argv[0]);
	progname = get_progname(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("contrib"));

	handle_help_version_opts(argc, argv, progname, help);

	/* process command-line options */
	while ((c = getopt_long(argc, argv, "ad:D:eh:Hi:I:j:p:Pqr:R:s:S:t:T:U:wWv",
							long_options, &optindex)) != -1)
	{
		char	   *endptr;

		switch (c)
		{
			case 'a':
				checkopts.alldb = true;
				break;
			case 'd':
				simple_string_list_append(&objects.databases, optarg);
				break;
			case 'D':
				simple_string_list_append(&objects.exclude_databases, optarg);
				break;
			case 'e':
				checkopts.echo = true;
				break;
			case 'h':
				host = pg_strdup(optarg);
				break;
			case 'H':
				checkopts.heapallindexed = true;
				break;
			case 'i':
				simple_string_list_append(&objects.indexes, optarg);
				break;
			case 'I':
				simple_string_list_append(&objects.exclude_indexes, optarg);
				break;
			case 'j':
				checkopts.jobs = atoi(optarg);
				if (checkopts.jobs <= 0)
				{
					pg_log_error("number of parallel jobs must be at least 1");
					exit(1);
				}
				break;
			case 'p':
				port = pg_strdup(optarg);
				break;
			case 'P':
				checkopts.parent_check = true;
				break;
			case 'q':
				checkopts.quiet = true;
				break;
			case 'r':
				simple_string_list_append(&objects.indexes, optarg);
				simple_string_list_append(&objects.tables, optarg);
				break;
			case 'R':
				simple_string_list_append(&objects.exclude_tables, optarg);
				simple_string_list_append(&objects.exclude_indexes, optarg);
				break;
			case 's':
				simple_string_list_append(&objects.schemas, optarg);
				break;
			case 'S':
				simple_string_list_append(&objects.exclude_schemas, optarg);
				break;
			case 't':
				simple_string_list_append(&objects.tables, optarg);
				break;
			case 'T':
				simple_string_list_append(&objects.exclude_tables, optarg);
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
			case 'v':
				checkopts.verbose = true;
				pg_logging_increase_verbosity();
				break;
			case 1:
				maintenance_db = pg_strdup(optarg);
				break;
			case 2:
				checkopts.no_indexes = true;
				break;
			case 3:
				checkopts.exclude_toast = true;
				break;
			case 4:
				checkopts.reconcile_toast = false;
				break;
			case 5:
				checkopts.on_error_stop = true;
				break;
			case 6:
				if (pg_strcasecmp(optarg, "all-visible") == 0)
					checkopts.skip = "all visible";
				else if (pg_strcasecmp(optarg, "all-frozen") == 0)
					checkopts.skip = "all frozen";
				else
				{
					fprintf(stderr, _("invalid skip options"));
					exit(1);
				}
				break;
			case 7:
				checkopts.startblock = strtol(optarg, &endptr, 10);
				if (*endptr != '\0')
				{
					fprintf(stderr,
							_("relation starting block argument contains garbage characters"));
					exit(1);
				}
				if (checkopts.startblock > (long) MaxBlockNumber)
				{
					fprintf(stderr,
							_("relation starting block argument out of bounds"));
					exit(1);
				}
				break;
			case 8:
				checkopts.endblock = strtol(optarg, &endptr, 10);
				if (*endptr != '\0')
				{
					fprintf(stderr,
							_("relation ending block argument contains garbage characters"));
					exit(1);
				}
				if (checkopts.startblock > (long) MaxBlockNumber)
				{
					fprintf(stderr,
							_("relation ending block argument out of bounds"));
					exit(1);
				}
				break;
			case 9:
				checkopts.rootdescend = true;
				checkopts.parent_check = true;
				break;
			case 10:
				checkopts.dependents = false;
				break;
			default:
				fprintf(stderr,
						_("Try \"%s --help\" for more information.\n"),
						progname);
				exit(1);
		}
	}

	if (checkopts.endblock >= 0 && checkopts.endblock < checkopts.startblock)
	{
		pg_log_error("relation ending block argument precedes starting block argument");
		exit(1);
	}

	/* non-option arguments specify database names */
	while (optind < argc)
	{
		if (connect_db == NULL)
			connect_db = argv[optind];
		simple_string_list_append(&objects.databases, argv[optind]);
		optind++;
	}

	/* fill cparams except for dbname, which is set below */
	cparams.pghost = host;
	cparams.pgport = port;
	cparams.pguser = username;
	cparams.prompt_password = prompt_password;
	cparams.override_dbname = NULL;

	setup_cancel_handler(NULL);

	/* choose the database for our initial connection */
	if (maintenance_db)
		cparams.dbname = maintenance_db;
	else if (connect_db != NULL)
		cparams.dbname = connect_db;
	else if (objects.databases.head != NULL)
		cparams.dbname = objects.databases.head->val;
	else
	{
		const char *default_db;

		if (getenv("PGDATABASE"))
			default_db = getenv("PGDATABASE");
		else if (getenv("PGUSER"))
			default_db = getenv("PGUSER");
		else
			default_db = get_user_name_or_exit(progname);

		if (objects.databases.head == NULL)
			simple_string_list_append(&objects.databases, default_db);

		cparams.dbname = default_db;
	}

	check_each_database(&cparams, &objects, &checkopts, progname);

	exit(0);
}

/*
 * check_each_database
 *
 * Connects to the initial database and resolves a list of all databases that
 * should be checked per the user supplied options.  Sequentially checks each
 * database in the list.
 *
 * The user supplied options may include zero databases, or only one database,
 * in which case we could skip the step of resolving a list of databases, but
 * it seems not worth optimizing, especially considering that there are
 * multiple ways in which no databases or just one database might be specified,
 * including a pattern that happens to match no entries or to match only one
 * entry in pg_database.
 *
 * cparams: parameters for the initial database connection
 * objects: lists of include and exclude patterns for filtering objects
 * checkopts: user supplied program options
 * progname: name of this program, such as "pg_amcheck"
 */
static void
check_each_database(ConnParams *cparams, const amcheckObjects *objects,
					const amcheckOptions *checkopts, const char *progname)
{
	PGconn	   *conn;
	PGresult   *databases;
	PQExpBufferData sql;
	int			ntups;
	int			i;
	SimpleStringList dbregex = {NULL, NULL};
	SimpleStringList exclude = {NULL, NULL};

	/*
	 * Get a list of all database SQL regexes to use for selecting database
	 * names.  We assemble these regexes from fully-qualified relation
	 * patterns and database patterns.  This process may result in the same
	 * database regex in the list multiple times, but the query against
	 * pg_database will deduplice, so we don't care.
	 */
	get_db_regexes_from_fqrps(&dbregex, &objects->tables);
	get_db_regexes_from_fqrps(&dbregex, &objects->indexes);
	get_db_regexes_from_patterns(&dbregex, &objects->databases);

	/*
	 * Assemble SQL regexes for databases to be excluded.  Note that excluded
	 * relations are not considered here, as excluding relation x.y.z does not
	 * imply excluding database x.  Excluding x.*.* would imply excluding
	 * database x, but we do not check for that here.
	 */
	get_db_regexes_from_patterns(&exclude, &objects->exclude_databases);

	conn = connectMaintenanceDatabase(cparams, progname, checkopts->echo);

	initPQExpBuffer(&sql);
	dbname_select(conn, &sql, &dbregex, checkopts->alldb);
	appendPQExpBufferStr(&sql, "\nEXCEPT");
	dbname_select(conn, &sql, &exclude, false);
	executeCommand(conn, "RESET search_path;", checkopts->echo);
	databases = executeQuery(conn, sql.data, checkopts->echo);
	if (PQresultStatus(databases) != PGRES_TUPLES_OK)
	{
		pg_log_error("query failed: %s", PQerrorMessage(conn));
		pg_log_error("query was: %s", sql.data);
		PQfinish(conn);
		exit(1);
	}

	termPQExpBuffer(&sql);
	PQclear(executeQuery(conn, ALWAYS_SECURE_SEARCH_PATH_SQL, checkopts->echo));
	PQfinish(conn);

	ntups = PQntuples(databases);
	if (ntups == 0 && !checkopts->quiet)
		printf(_("%s: no databases to check\n"), progname);

	for (i = 0; i < ntups; i++)
	{
		cparams->override_dbname = PQgetvalue(databases, i, 0);
		check_one_database(cparams, objects, checkopts, progname);
	}

	PQclear(databases);
}

/*
 * string_in_list
 *
 * Returns whether a given string is in the list of strings.
 */
static bool
string_in_list(const SimpleStringList *list, const char *str)
{
	const SimpleStringListCell *cell;

	for (cell = list->head; cell; cell = cell->next)
		if (strcmp(cell->val, str) == 0)
			return true;
	return false;
}

/*
 * check_one_database
 *
 * Connects to this next database and checks all relations that match the
 * supplied objects list.  Patterns in the object lists are matched to the
 * relations that exit in this next database.
 *
 * cparams: parameters for this next database connection
 * objects: lists of include and exclude patterns for filtering objects
 * checkopts: user supplied program options
 * progname: name of this program, such as "pg_amcheck"
 */
static void
check_one_database(const ConnParams *cparams, const amcheckObjects *objects,
				   const amcheckOptions *checkopts, const char *progname)
{
	PQExpBufferData sql;
	PGconn	   *conn;
	PGresult   *result;
	ParallelSlot *slots;
	int			ntups;
	int			i;
	int			parallel_workers;
	bool		inclusive;
	bool		failed = false;

	conn = connectDatabase(cparams, progname, checkopts->echo, false, true);

	if (!checkopts->quiet)
	{
		printf(_("%s: checking database \"%s\"\n"),
			   progname, PQdb(conn));
		fflush(stdout);
	}

	/*
	 * Verify that amcheck is installed for this next database.  User error
	 * could result in a database not having amcheck that should have it, but
	 * we also could be iterating over multiple databases where not all of
	 * them have amcheck installed (for example, 'template1').
	 */
	result = executeQuery(conn, amcheck_sql, checkopts->echo);
	if (PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		/* Querying the catalog failed. */
		pg_log_error(_("%s: database \"%s\": %s\n"),
					 progname, PQdb(conn), PQerrorMessage(conn));
		pg_log_error(_("%s: query was: %s"), progname, amcheck_sql);
		PQclear(result);
		PQfinish(conn);
		return;
	}
	ntups = PQntuples(result);
	PQclear(result);
	if (ntups == 0)
	{
		/* Querying the catalog succeeded, but amcheck is missing. */
		if (!checkopts->quiet &&
			(checkopts->verbose ||
			 string_in_list(&objects->databases, PQdb(conn))))
		{
			printf(_("%s: skipping database \"%s\": amcheck is not installed"),
				   progname, PQdb(conn));
		}
		PQfinish(conn);
		return;
	}

	/*
	 * If we were given no tables nor indexes to check, then we select all
	 * targets not excluded.  Otherwise, we select only the targets that we
	 * were given.
	 */
	inclusive = objects->tables.head == NULL &&
		objects->indexes.head == NULL;

	initPQExpBuffer(&sql);
	target_select(conn, &sql, objects, checkopts, progname, inclusive);
	executeCommand(conn, "RESET search_path;", checkopts->echo);
	result = executeQuery(conn, sql.data, checkopts->echo);
	if (PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		pg_log_error("query failed: %s", PQerrorMessage(conn));
		pg_log_error("query was: %s", sql.data);
		PQfinish(conn);
		exit(1);
	}
	termPQExpBuffer(&sql);
	PQclear(executeQuery(conn, ALWAYS_SECURE_SEARCH_PATH_SQL, checkopts->echo));

	/*
	 * If no rows are returned, there are no matching relations, so we are
	 * done.
	 */
	ntups = PQntuples(result);
	if (ntups == 0)
	{
		PQclear(result);
		PQfinish(conn);
		return;
	}

	/*
	 * Ensure parallel_workers is sane.  If there are more connections than
	 * relations to be checked, we don't need to use them all.
	 */
	parallel_workers = checkopts->jobs;
	if (parallel_workers > ntups)
		parallel_workers = ntups;
	if (parallel_workers <= 0)
		parallel_workers = 1;

	/*
	 * Setup the database connections.  We reuse the connection we already
	 * have for the first slot.  If not in parallel mode, the first slot in
	 * the array contains the connection.
	 */
	slots = ParallelSlotsSetup(cparams, progname, checkopts->echo, conn,
							   parallel_workers);

	initPQExpBuffer(&sql);

	/*
	 * Loop over all objects to be checked, and execute amcheck checking
	 * commands for each.  We do not wait for the checks to complete, nor do
	 * we handle the results of those checks in the loop.  We register
	 * handlers for doing all that.
	 */
	for (i = 0; i < ntups; i++)
	{
		ParallelSlot *free_slot;

		CheckType	checktype = atoi(PQgetvalue(result, i, 0));
		Oid			reloid = atooid(PQgetvalue(result, i, 1));

		if (CancelRequested)
		{
			failed = true;
			goto finish;
		}

		/*
		 * Get a parallel slot for the next amcheck command, blocking if
		 * necessary until one is available, or until a previously issued slot
		 * command fails, indicating that we should abort checking the
		 * remaining objects.
		 */
		free_slot = ParallelSlotsGetIdle(slots, parallel_workers);
		if (!free_slot)
		{
			/*
			 * Something failed.  We don't need to know what it was, because
			 * the handler should already have emitted the necessary error
			 * messages.
			 */
			failed = true;
			goto finish;
		}

		/* Execute the amcheck command for the given relation type. */
		switch (checktype)
		{
				/* heapam types */
			case CT_TABLE:
				prepare_table_command(&sql, checkopts, reloid);
				ParallelSlotSetHandler(free_slot, VerifyHeapamSlotHandler, sql.data);
				run_command(free_slot->connection, sql.data, checkopts, reloid,
							ctfilter[checktype].typname);
				break;

				/* btreeam types */
			case CT_BTREE:
				prepare_btree_command(&sql, checkopts, reloid);
				ParallelSlotSetHandler(free_slot, VerifyBtreeSlotHandler, NULL);
				run_command(free_slot->connection, sql.data, checkopts, reloid,
							ctfilter[checktype].typname);
				break;

				/* intentionally no default here */
		}
	}

	/*
	 * Wait for all slots to complete, or for one to indicate that an error
	 * occurred.  Like above, we rely on the handler emitting the necessary
	 * error messages.
	 */
	if (!ParallelSlotsWaitCompletion(slots, parallel_workers))
		failed = true;

finish:
	ParallelSlotsTerminate(slots, parallel_workers);
	pg_free(slots);

	termPQExpBuffer(&sql);

	if (failed)
		exit(1);
}

/*
 * prepare_table_command
 *
 * Creates a SQL command for running amcheck checking on the given heap
 * relation.  The command is phrased as a SQL query, with column order and
 * names matching the expectations of VerifyHeapamSlotHandler, which will
 * receive and handle each row returned from the verify_heapam() function.
 *
 * sql: buffer into which the table checking command will be written
 * checkopts: user supplied program options
 * reloid: relation of the table to be checked
 */
static void
prepare_table_command(PQExpBuffer sql, const amcheckOptions *checkopts,
					  Oid reloid)
{
	resetPQExpBuffer(sql);
	appendPQExpBuffer(sql,
					  "SELECT n.nspname, c.relname, v.blkno, v.offnum, v.attnum, v.msg"
					  "\nFROM public.verify_heapam("
					  "\nrelation := %u,"
					  "\non_error_stop := %s,"
					  "\ncheck_toast := %s,"
					  "\nskip := '%s'",
					  reloid,
					  checkopts->on_error_stop ? "true" : "false",
					  checkopts->reconcile_toast ? "true" : "false",
					  checkopts->skip);
	if (checkopts->startblock >= 0)
		appendPQExpBuffer(sql, ",\nstartblock := %ld", checkopts->startblock);
	if (checkopts->endblock >= 0)
		appendPQExpBuffer(sql, ",\nendblock := %ld", checkopts->endblock);
	appendPQExpBuffer(sql, "\n) v,"
					  "\npg_catalog.pg_class c"
					  "\nJOIN pg_catalog.pg_namespace n"
					  "\nON c.relnamespace OPERATOR(pg_catalog.=) n.oid"
					  "\nWHERE c.oid OPERATOR(pg_catalog.=) %u",
					  reloid);
}

/*
 * prepare_btree_command
 *
 * Creates a SQL command for running amcheck checking on the given btree index
 * relation.  The command does not select any columns, as btree checking
 * functions do not return any, but rather return corruption information by
 * raising errors, which VerifyBtreeSlotHandler expects.
 *
 * sql: buffer into which the table checking command will be written
 * checkopts: user supplied program options
 * reloid: relation of the table to be checked
 */
static void
prepare_btree_command(PQExpBuffer sql, const amcheckOptions *checkopts,
					  Oid reloid)
{
	resetPQExpBuffer(sql);
	if (checkopts->parent_check)
		appendPQExpBuffer(sql,
						  "SELECT public.bt_index_parent_check("
						  "\nindex := '%u'::regclass,"
						  "\nheapallindexed := %s,"
						  "\nrootdescend := %s)",
						  reloid,
						  (checkopts->heapallindexed ? "true" : "false"),
						  (checkopts->rootdescend ? "true" : "false"));
	else
		appendPQExpBuffer(sql,
						  "SELECT public.bt_index_check("
						  "\nindex := '%u'::regclass,"
						  "\nheapallindexed := %s)",
						  reloid,
						  (checkopts->heapallindexed ? "true" : "false"));
}

/*
 * run_command
 *
 * Sends a command to the server without waiting for the command to complete.
 * Logs an error if the command cannot be sent, but otherwise any errors are
 * expected to be handled by a ParallelSlotHandler.
 *
 * conn: connection to the server associated with the slot to use
 * sql: query to send
 * checkopts: user supplied program options
 * reloid: oid of the object being checked, for error reporting
 * typ: type of object being checked, for error reporting
 */
static void
run_command(PGconn *conn, const char *sql, const amcheckOptions *checkopts,
			Oid reloid, const char *typ)
{
	bool		status;

	if (checkopts->echo)
		printf("%s\n", sql);

	status = PQsendQuery(conn, sql) == 1;

	if (!status)
	{
		pg_log_error("check of %s with id %u in database \"%s\" failed: %s",
					 typ, reloid, PQdb(conn), PQerrorMessage(conn));
		pg_log_error("command was: %s", sql);
	}
}

/*
 * VerifyHeapamSlotHandler
 *
 * ParallelSlotHandler that receives results from a table checking command
 * created by prepare_table_command and outputs the results for the user.
 *
 * res: result from an executed sql query
 * conn: connection on which the sql query was executed
 * context: the sql query being handled, as a cstring
 */
static PGresult *
VerifyHeapamSlotHandler(PGresult *res, PGconn *conn, void *context)
{
	int			ntups = PQntuples(res);

	if (PQresultStatus(res) == PGRES_TUPLES_OK)
	{
		int			i;

		for (i = 0; i < ntups; i++)
		{
			if (!PQgetisnull(res, i, 4))
				printf("relation %s.%s, block %s, offset %s, attribute %s\n    %s\n",
					   PQgetvalue(res, i, 0),	/* schema */
					   PQgetvalue(res, i, 1),	/* relname */
					   PQgetvalue(res, i, 2),	/* blkno */
					   PQgetvalue(res, i, 3),	/* offnum */
					   PQgetvalue(res, i, 4),	/* attnum */
					   PQgetvalue(res, i, 5));	/* msg */

			else if (!PQgetisnull(res, i, 3))
				printf("relation %s.%s, block %s, offset %s\n    %s\n",
					   PQgetvalue(res, i, 0),	/* schema */
					   PQgetvalue(res, i, 1),	/* relname */
					   PQgetvalue(res, i, 2),	/* blkno */
					   PQgetvalue(res, i, 3),	/* offnum */
				/* attnum is null: 4 */
					   PQgetvalue(res, i, 5));	/* msg */

			else if (!PQgetisnull(res, i, 2))
				printf("relation %s.%s, block %s\n    %s\n",
					   PQgetvalue(res, i, 0),	/* schema */
					   PQgetvalue(res, i, 1),	/* relname */
					   PQgetvalue(res, i, 2),	/* blkno */
				/* offnum is null: 3 */
				/* attnum is null: 4 */
					   PQgetvalue(res, i, 5));	/* msg */

			else if (!PQgetisnull(res, i, 1))
				printf("relation %s.%s\n    %s\n",
					   PQgetvalue(res, i, 0),	/* schema */
					   PQgetvalue(res, i, 1),	/* relname */
				/* blkno is null:  2 */
				/* offnum is null: 3 */
				/* attnum is null: 4 */
					   PQgetvalue(res, i, 5));	/* msg */

			else
				printf("%s\n", PQgetvalue(res, i, 5));	/* msg */
		}
	}
	else if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		printf("%s\n", PQerrorMessage(conn));
		printf("query was: %s\n", (const char *) context);
	}

	return res;
}

/*
 * VerifyBtreeSlotHandler
 *
 * ParallelSlotHandler that receives results from a btree checking command
 * created by prepare_btree_command and outputs them for the user.  The results
 * from the btree checking command is assumed to be empty, but when the results
 * are an error code, the useful information about the corruption is expected
 * in the connection's error message.
 *
 * res: result from an executed sql query
 * conn: connection on which the sql query was executed
 * context: unused
 */
static PGresult *
VerifyBtreeSlotHandler(PGresult *res, PGconn *conn, void *context)
{
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		printf("%s\n", PQerrorMessage(conn));
	return res;
}

/*
 * help
 *
 * Prints help page for the program
 *
 * progname: the name of the executed program, such as "pg_amcheck"
 */
static void
help(const char *progname)
{
	printf(_("%s checks objects in a PostgreSQL database for corruption.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]... [DBNAME]\n"), progname);
	printf(_("\nTarget Options:\n"));
	printf(_("  -a, --all                 check all databases\n"));
	printf(_("  -d, --dbname=DBNAME       check specific database(s)\n"));
	printf(_("  -D, --exclude-dbname=DBNAME do NOT check specific database(s)\n"));
	printf(_("  -i, --index=INDEX         check specific index(es)\n"));
	printf(_("  -I, --exclude-index=INDEX do NOT check specific index(es)\n"));
	printf(_("  -r, --relation=RELNAME    check specific relation(s)\n"));
	printf(_("  -R, --exclude-relation=RELNAME do NOT check specific relation(s)\n"));
	printf(_("  -s, --schema=SCHEMA       check specific schema(s)\n"));
	printf(_("  -S, --exclude-schema=SCHEMA do NOT check specific schema(s)\n"));
	printf(_("  -t, --table=TABLE         check specific table(s)\n"));
	printf(_("  -T, --exclude-table=TABLE do NOT check specific table(s)\n"));
	printf(_("      --exclude-indexes     do NOT perform any index checking\n"));
	printf(_("      --exclude-toast       do NOT check any toast tables or indexes\n"));
	printf(_("      --no-dependents       do NOT automatically check dependent objects\n"));
	printf(_("\nIndex Checking Options:\n"));
	printf(_("  -H, --heapallindexed      check all heap tuples are found within indexes\n"));
	printf(_("  -P, --parent-check        check parent/child relationships during index checking\n"));
	printf(_("      --rootdescend         search from root page to refind tuples at the leaf level\n"));
	printf(_("\nTable Checking Options:\n"));
	printf(_("      --exclude-toast-pointers do NOT check relation toast pointers against toast\n"));
	printf(_("      --on-error-stop       stop checking a relation at end of first corrupt page\n"));
	printf(_("      --skip=OPTION         do NOT check \"all-frozen\" or \"all-visible\" blocks\n"));
	printf(_("      --startblock          begin checking table(s) at the given starting block number\n"));
	printf(_("      --endblock            check table(s) only up to the given ending block number\n"));
	printf(_("\nConnection options:\n"));
	printf(_("  -h, --host=HOSTNAME       database server host or socket directory\n"));
	printf(_("  -p, --port=PORT           database server port\n"));
	printf(_("  -U, --username=USERNAME   user name to connect as\n"));
	printf(_("  -w, --no-password         never prompt for password\n"));
	printf(_("  -W, --password            force password prompt\n"));
	printf(_("  --maintenance-db=DBNAME   alternate maintenance database\n"));
	printf(_("\nOther Options:\n"));
	printf(_("  -e, --echo                show the commands being sent to the server\n"));
	printf(_("  -j, --jobs=NUM            use this many concurrent connections to the server\n"));
	printf(_("  -q, --quiet               don't write any messages\n"));
	printf(_("  -v, --verbose             write a lot of output\n"));
	printf(_("  -V, --version             output version information, then exit\n"));
	printf(_("  -?, --help                show this help, then exit\n"));

	printf(_("\nRead the description of the amcheck contrib module for details.\n"));
	printf(_("\nReport bugs to <%s>.\n"), PACKAGE_BUGREPORT);
	printf(_("%s home page: <%s>\n"), PACKAGE_NAME, PACKAGE_URL);
}

/*
 * get_db_regexes_from_fqrps
 *
 * For each pattern in the patterns list, if it is in fully-qualified
 * database.schema.name format (fully-qualifed relation pattern (fqrp)), parse
 * the database portion of the pattern, convert it to SQL regex format, and
 * append it to the databases list.  Patterns that are not fully-qualified are
 * skipped over.  No deduplication of regexes is performed.
 *
 * regexes: list to which parsed and converted database regexes are appended
 * patterns: list of all patterns to parse
 */
static void
get_db_regexes_from_fqrps(SimpleStringList *regexes,
						  const SimpleStringList *patterns)
{
	const SimpleStringListCell *cell;
	PQExpBufferData dbnamebuf;
	PQExpBufferData schemabuf;
	PQExpBufferData namebuf;
	int			encoding = pg_get_encoding_from_locale(NULL, false);

	initPQExpBuffer(&dbnamebuf);
	initPQExpBuffer(&schemabuf);
	initPQExpBuffer(&namebuf);
	for (cell = patterns->head; cell; cell = cell->next)
	{
		/* parse the pattern as dbname.schema.relname, if possible */
		patternToSQLRegex(encoding, &dbnamebuf, &schemabuf, &namebuf,
						  cell->val, false);

		/* add the database name (or pattern), if any, to the list */
		if (dbnamebuf.data[0])
			simple_string_list_append(regexes, dbnamebuf.data);

		/* we do not use the schema or relname portions */

		/* we may have dirtied the buffers */
		resetPQExpBuffer(&dbnamebuf);
		resetPQExpBuffer(&schemabuf);
		resetPQExpBuffer(&namebuf);
	}
	termPQExpBuffer(&dbnamebuf);
	termPQExpBuffer(&schemabuf);
	termPQExpBuffer(&namebuf);
}

/*
 * get_db_regexes_from_patterns
 *
 * Convert each unqualified pattern in the list to SQL regex format and append
 * it to the regexes list.
 *
 * regexes: list to which converted regexes are appended
 * patterns: list of patterns to be converted
 */
static void
get_db_regexes_from_patterns(SimpleStringList *regexes,
							 const SimpleStringList *patterns)
{
	const SimpleStringListCell *cell;
	PQExpBufferData buf;
	int			encoding = pg_get_encoding_from_locale(NULL, false);

	initPQExpBuffer(&buf);
	for (cell = patterns->head; cell; cell = cell->next)
	{
		patternToSQLRegex(encoding, NULL, NULL, &buf, cell->val, false);
		if (buf.data[0])
			simple_string_list_append(regexes, buf.data);
		resetPQExpBuffer(&buf);
	}
	termPQExpBuffer(&buf);
}

/*
 * dbname_select
 *
 * Appends a statement which selects the names of all databases matching the
 * given SQL regular expressions.
 *
 * conn: connection to the initial database
 * sql: buffer into which the constructed sql statement is appended
 * regexes: list of database name regular expressions to match
 * alldb: when true, select all databases which allow connections
 */
static void
dbname_select(PGconn *conn, PQExpBuffer sql, const SimpleStringList *regexes,
			  bool alldb)
{
	SimpleStringListCell *cell;
	const char *comma;

	if (alldb)
	{
		appendPQExpBufferStr(sql, "\nSELECT datname::TEXT AS datname"
							 "\nFROM pg_database"
							 "\nWHERE datallowconn");
		return;
	}
	else if (regexes->head == NULL)
	{
		appendPQExpBufferStr(sql, "\nSELECT ''::TEXT AS datname"
							 "\nWHERE false");
		return;
	}

	appendPQExpBufferStr(sql, "\nSELECT datname::TEXT AS datname"
						 "\nFROM pg_database"
						 "\nWHERE datallowconn"
						 "\nAND datname::TEXT OPERATOR(pg_catalog.~) ANY(ARRAY[\n");
	for (cell = regexes->head, comma = ""; cell; cell = cell->next, comma = ",\n")
	{
		appendPQExpBufferStr(sql, comma);
		appendStringLiteralConn(sql, cell->val, conn);
		appendPQExpBufferStr(sql, "::TEXT COLLATE pg_catalog.default");
	}
	appendPQExpBufferStr(sql, "\n]::TEXT[])");
}

/*
 * schema_select
 *
 * Appends a statement which selects all schemas matching the given patterns
 *
 * conn: connection to the current database
 * sql: buffer into which the constructed sql statement is appended
 * fieldname: alias to use for the oid field within the created SELECT
 * statement
 * patterns: list of schema name patterns to match
 * inclusive: when patterns is an empty list, whether the select statement
 * should match all non-system schemas
 */
static void
schema_select(PGconn *conn, PQExpBuffer sql, const char *fieldname,
			  const SimpleStringList *patterns, bool inclusive)
{
	SimpleStringListCell *cell;
	const char *comma;
	int			encoding = PQclientEncoding(conn);

	if (patterns->head == NULL)
	{
		if (!inclusive)
			appendPQExpBuffer(sql, "\nSELECT 0::pg_catalog.oid AS %s WHERE false", fieldname);
		else
			appendPQExpBuffer(sql, "\nSELECT oid AS %s"
							  "\nFROM pg_catalog.pg_namespace"
							  "\nWHERE oid OPERATOR(pg_catalog.!=) pg_catalog.regnamespace('pg_catalog')"
							  "\nAND oid OPERATOR(pg_catalog.!=) pg_catalog.regnamespace('pg_toast')",
							  fieldname);
		return;
	}

	appendPQExpBuffer(sql, "\nSELECT oid AS %s"
					  "\nFROM pg_catalog.pg_namespace"
					  "\nWHERE nspname OPERATOR(pg_catalog.~) ANY(ARRAY[\n",
					  fieldname);
	for (cell = patterns->head, comma = ""; cell; cell = cell->next, comma = ",\n")
	{
		PQExpBufferData regexbuf;

		initPQExpBuffer(&regexbuf);
		patternToSQLRegex(encoding, NULL, NULL, &regexbuf, cell->val, false);
		appendPQExpBufferStr(sql, comma);
		appendStringLiteralConn(sql, regexbuf.data, conn);
		appendPQExpBufferStr(sql, "::TEXT COLLATE pg_catalog.default");
		termPQExpBuffer(&regexbuf);
	}
	appendPQExpBufferStr(sql, "\n]::TEXT[])");
}

/*
 * schema_cte
 *
 * Appends a Common Table Expression (CTE) which selects all schemas to be
 * checked, with the CTE and oid field named as requested.  The CTE will select
 * all schemas matching the include list except any schemas matching the
 * exclude list.
 *
 * conn: connection to the current database
 * sql: buffer into which the constructed sql statement is appended
 * ctename: name of the schema CTE to be created
 * fieldname: name of the oid field within the schema CTE to be created
 * include: list of schema name patterns for inclusion
 * exclude: list of schema name patterns for exclusion
 * inclusive: when 'include' is an empty list, whether to use all schemas in
 * the database in lieu of the include list.
 */
static void
schema_cte(PGconn *conn, PQExpBuffer sql, const char *ctename,
		   const char *fieldname, const SimpleStringList *include,
		   const SimpleStringList *exclude, bool inclusive)
{
	appendPQExpBuffer(sql, "\n%s (%s) AS (", ctename, fieldname);
	schema_select(conn, sql, fieldname, include, inclusive);
	appendPQExpBufferStr(sql, "\nEXCEPT");
	schema_select(conn, sql, fieldname, exclude, false);
	appendPQExpBufferStr(sql, "\n)");
}

/*
 * append_ctfilter_quals
 *
 * Appends quals to a buffer that restrict the rows selected from pg_class to
 * only those which match the given checktype.  No initial "WHERE" or "AND" is
 * appended, nor do we surround our appended clauses in parens.  The caller is
 * assumed to take care of such matters.
 *
 * sql: buffer into which the constructed sql quals are appended
 * relname: name (or alias) of pg_class in the surrounding query
 * checktype: struct containing filter info
 */
static void
append_ctfilter_quals(PQExpBuffer sql, const char *relname, CheckType checktype)
{
	appendPQExpBuffer(sql,
					  "%s.relam OPERATOR(pg_catalog.=) %u"
					  "\nAND %s.relkind OPERATOR(pg_catalog.=) ANY(ARRAY[%s])",
					  relname, ctfilter[checktype].relam,
					  relname, ctfilter[checktype].relkinds);
}

/*
 * relation_select
 *
 * Appends a statement which selects the oid of all relations matching the
 * given parameters.  Expects a mixture of qualified and unqualified relation
 * name patterns.
 *
 * For unqualified relation patterns, selects relations that match the relation
 * name portion of the pattern which are in namespaces that are in the given
 * namespace CTE.
 *
 * For qualified relation patterns, ignores the given namespace CTE and selects
 * relations that match the relation name portion of the pattern which are in
 * namespaces that match the schema portion of the pattern.
 *
 * For fully qualified relation patterns (database.schema.name), the pattern
 * will be ignored unless the database portion of the pattern matches the name
 * of the current database, as retrieved from conn.
 *
 * Only relations of the specified checktype will be selected.
 *
 * conn: connection to the current database
 * sql: buffer into which the constructed sql statement is appended
 * schemacte: name of the CTE which selects all schemas to be checked
 * schemafield: name of the oid field within the schema CTE
 * fieldname: alias to use for the oid field within the created SELECT
 * statement
 * patterns: list of (possibly qualified) relation name patterns to match
 * checktype: the type of relation to select
 * inclusive: when patterns is an empty list, whether the select statement
 * should match all relations of the given type
 */
static void
relation_select(PGconn *conn, PQExpBuffer sql, const char *schemacte,
				const char *schemafield, const char *fieldname,
				const SimpleStringList *patterns, CheckType checktype,
				bool inclusive)
{
	SimpleStringListCell *cell;
	const char *comma = "";
	const char *qor = "";
	PQExpBufferData qualified;
	PQExpBufferData unqualified;
	PQExpBufferData dbnamebuf;
	PQExpBufferData schemabuf;
	PQExpBufferData namebuf;
	int			encoding = PQclientEncoding(conn);

	if (patterns->head == NULL)
	{
		if (!inclusive)
			appendPQExpBuffer(sql,
							  "\nSELECT 0::pg_catalog.oid AS %s WHERE false",
							  fieldname);
		else
		{
			appendPQExpBuffer(sql,
							  "\nSELECT oid AS %s"
							  "\nFROM pg_catalog.pg_class c"
							  "\nJOIN %s n"
							  "\nON n.%s OPERATOR(pg_catalog.=) c.relnamespace"
							  "\nWHERE ",
							  fieldname, schemacte, schemafield);
			append_ctfilter_quals(sql, "c", checktype);
		}
		return;
	}

	/*
	 * We have to distinguish between schema-qualified and unqualified
	 * relation patterns.  The unqualified patterns need to be restricted by
	 * the list of schemas returned by the schema CTE, but not so for the
	 * qualified patterns.
	 *
	 * We treat fully-qualified relation patterns (database.schema.relation)
	 * like schema-qualified patterns except that we also require the database
	 * portion to match the current database name.
	 */
	initPQExpBuffer(&qualified);
	initPQExpBuffer(&unqualified);
	initPQExpBuffer(&dbnamebuf);
	initPQExpBuffer(&schemabuf);
	initPQExpBuffer(&namebuf);

	for (cell = patterns->head; cell; cell = cell->next)
	{
		patternToSQLRegex(encoding, &dbnamebuf, &schemabuf, &namebuf,
						  cell->val, false);

		if (schemabuf.data[0])
		{
			/* Qualified relation pattern */
			appendPQExpBuffer(&qualified, "%s\n(", qor);

			if (dbnamebuf.data[0])
			{
				/*
				 * Fully-qualified relation pattern.  Require the database
				 * name of our connection to match the database portion of the
				 * relation pattern.
				 */
				appendPQExpBufferStr(&qualified, "\n");
				appendStringLiteralConn(&qualified, PQdb(conn), conn);
				appendPQExpBufferStr(&qualified,
									 "::TEXT OPERATOR(pg_catalog.~) ");
				appendStringLiteralConn(&qualified, dbnamebuf.data, conn);
				appendPQExpBufferStr(&qualified,
									 "::TEXT COLLATE pg_catalog.default AND");
			}

			/*
			 * Require the namespace name to match the schema portion of the
			 * relation pattern and the relation name to match the relname
			 * portion of the relation pattern.
			 */
			appendPQExpBufferStr(&qualified,
								 "\nn.nspname OPERATOR(pg_catalog.~) ");
			appendStringLiteralConn(&qualified, schemabuf.data, conn);
			appendPQExpBufferStr(&qualified,
								 "::TEXT COLLATE pg_catalog.default AND"
								 "\nc.relname OPERATOR(pg_catalog.~) ");
			appendStringLiteralConn(&qualified, namebuf.data, conn);
			appendPQExpBufferStr(&qualified,
								 "::TEXT COLLATE pg_catalog.default)");
			qor = "\nOR";
		}
		else
		{
			/* Unqualified relation pattern */
			appendPQExpBufferStr(&unqualified, comma);
			appendStringLiteralConn(&unqualified, namebuf.data, conn);
			appendPQExpBufferStr(&unqualified,
								 "::TEXT COLLATE pg_catalog.default");
			comma = "\n, ";
		}

		resetPQExpBuffer(&dbnamebuf);
		resetPQExpBuffer(&schemabuf);
		resetPQExpBuffer(&namebuf);
	}

	if (qualified.data[0])
	{
		appendPQExpBuffer(sql,
						  "\nSELECT c.oid AS %s"
						  "\nFROM pg_catalog.pg_class c"
						  "\nJOIN pg_catalog.pg_namespace n"
						  "\nON c.relnamespace OPERATOR(pg_catalog.=) n.oid"
						  "\nWHERE (",
						  fieldname);
		appendPQExpBufferStr(sql, qualified.data);
		appendPQExpBufferStr(sql, ")\nAND ");
		append_ctfilter_quals(sql, "c", checktype);
		if (unqualified.data[0])
			appendPQExpBufferStr(sql, "\nUNION ALL");
	}
	if (unqualified.data[0])
	{
		appendPQExpBuffer(sql,
						  "\nSELECT c.oid AS %s"
						  "\nFROM pg_catalog.pg_class c"
						  "\nJOIN %s ls"
						  "\nON c.relnamespace OPERATOR(pg_catalog.=) ls.%s"
						  "\nWHERE c.relname OPERATOR(pg_catalog.~) ANY(ARRAY[",
						  fieldname, schemacte, schemafield);
		appendPQExpBufferStr(sql, unqualified.data);
		appendPQExpBufferStr(sql, "\n]::TEXT[])\nAND ");
		append_ctfilter_quals(sql, "c", checktype);
	}
}

/*
 * table_cte
 *
 * Appends to the buffer 'sql' a Common Table Expression (CTE) which selects
 * all table relations matching the given filters.
 *
 * conn: connection to the current database
 * sql: buffer into which the constructed sql statement is appended
 * schemacte: name of the CTE which selects all schemas to be checked
 * schemafield: name of the oid field within the schema CTE
 * ctename: name of the table CTE to be created
 * fieldname: name of the oid field within the table CTE to be created
 * include: list of table name patterns for inclusion
 * exclude: list of table name patterns for exclusion
 * inclusive: when 'include' is an empty list, whether the select statement
 * should match all relations
 * toast: whether to also select the associated toast tables
 */
static void
table_cte(PGconn *conn, PQExpBuffer sql, const char *schemacte,
		  const char *schemafield, const char *ctename, const char *fieldname,
		  const SimpleStringList *include, const SimpleStringList *exclude,
		  bool inclusive, bool toast)
{
	appendPQExpBuffer(sql, "\n%s (%s) AS (", ctename, fieldname);

	if (toast)
	{
		/*
		 * Compute the primary tables, then union on all associated toast
		 * tables.  We depend on left to right evaluation of the UNION before
		 * the EXCEPT which gets added below.   UNION and EXCEPT have equal
		 * precedence, so be careful if you rearrange this query.
		 */
		appendPQExpBuffer(sql, "\nWITH primary_table AS (");
		relation_select(conn, sql, schemacte, schemafield, fieldname, include,
						CT_TABLE, inclusive);
		appendPQExpBuffer(sql, "\n)"
						  "\nSELECT %s"
						  "\nFROM primary_table"
						  "\nUNION"
						  "\nSELECT c.reltoastrelid AS %s"
						  "\nFROM pg_catalog.pg_class c"
						  "\nJOIN primary_table pt"
						  "\nON pt.%s OPERATOR(pg_catalog.=) c.oid"
						  "\nWHERE c.reltoastrelid OPERATOR(pg_catalog.!=) 0",
						  fieldname, fieldname, fieldname);
	}
	else
		relation_select(conn, sql, schemacte, schemafield, fieldname, include,
						CT_TABLE, inclusive);

	appendPQExpBufferStr(sql, "\nEXCEPT");
	relation_select(conn, sql, schemacte, schemafield, fieldname, exclude,
					CT_TABLE, false);
	appendPQExpBufferStr(sql, "\n)");
}

/*
 * exclude_index_cte
 *		Appends a CTE which selects all indexes to be excluded
 *
 * conn: connection to the current database
 * sql: buffer into which the constructed sql CTE is appended
 * schemacte: name of the CTE which selects all schemas to be checked
 * schemafield: name of the oid field within the schema CTE
 * ctename: name of the index CTE to be created
 * fieldname: name of the oid field within the index CTE to be created
 * patterns: list of index name patterns to match
 */
static void
exclude_index_cte(PGconn *conn, PQExpBuffer sql, const char *schemacte,
				  const char *schemafield, const char *ctename,
				  const char *fieldname, const SimpleStringList *patterns)
{
	appendPQExpBuffer(sql, "\n%s (%s) AS (", ctename, fieldname);
	relation_select(conn, sql, schemacte, schemafield, fieldname, patterns,
					CT_BTREE, false);
	appendPQExpBufferStr(sql, "\n)");
}

/*
 * index_cte
 *		Appends a CTE which selects all indexes to be checked
 *
 * conn: connection to the current database
 * sql: buffer into which the constructed sql CTE is appended
 * schemacte: name of the CTE which selects all schemas to be checked
 * schemafield: name of the oid field within the schema CTE
 * ctename: name of the index CTE to be created
 * fieldname: name of the oid field within the index CTE to be created
 * excludecte: name of the CTE which contains all indexes to be excluded
 * tablescte: optional; if automatically including indexes for checked tables,
 *		the name of the CTE which contains all tables to be checked
 * tablesfield: if tablescte is not NULL, the name of the oid field in the
 *		tables CTE
 * patterns: list of index name patterns to match
 * inclusive: when 'include' is an empty list, whether the select statement should match all relations
 */
static void
index_cte(PGconn *conn, PQExpBuffer sql, const char *schemacte,
		  const char *schemafield, const char *ctename, const char *fieldname,
		  const char *excludecte, const char *tablescte,
		  const char *tablesfield, const SimpleStringList *patterns,
		  bool inclusive)
{
	appendPQExpBuffer(sql, "\n%s (%s) AS (", ctename, fieldname);
	appendPQExpBuffer(sql, "\nSELECT %s FROM (", fieldname);
	relation_select(conn, sql, schemacte, schemafield, fieldname, patterns,
					CT_BTREE, inclusive);
	if (tablescte)
	{
		appendPQExpBuffer(sql,
						  "\nUNION"
						  "\nSELECT i.indexrelid AS %s"
						  "\nFROM pg_catalog.pg_index i"
						  "\nJOIN %s t ON t.%s OPERATOR(pg_catalog.=) i.indrelid",
						  fieldname, tablescte, tablesfield);
	}
	appendPQExpBuffer(sql,
					  "\n) AS included_indexes"
					  "\nEXCEPT"
					  "\nSELECT %s FROM %s",
					  fieldname, excludecte);
	appendPQExpBufferStr(sql, "\n)");
}

/*
 * target_select
 *
 * Construct a query that will return a list of all tables and indexes in
 * the database matching the user specified options, sorted by size.  We
 * want the largest tables and indexes first, so that the parallel
 * processing of the larger database objects gets started sooner.
 *
 * If 'inclusive' is true, include all tables and indexes not otherwise
 * excluded; if false, include only tables and indexes explicitly included.
 *
 * conn: connection to the current database
 * sql: buffer into which the constructed sql select statement is appended
 * objects: lists of include and exclude patterns for filtering objects
 * checkopts: user supplied program options
 * progname: name of this program, such as "pg_amcheck"
 * inclusive: when list of objects to include is empty, whether the select
 * statement should match all objects not otherwise excluded
 */
static void
target_select(PGconn *conn, PQExpBuffer sql, const amcheckObjects *objects,
			  const amcheckOptions *checkopts, const char *progname,
			  bool inclusive)
{
	appendPQExpBufferStr(sql, "WITH");
	schema_cte(conn, sql, "namespaces", "nspoid", &objects->schemas,
			   &objects->exclude_schemas, inclusive);
	appendPQExpBufferStr(sql, ",");
	table_cte(conn, sql, "namespaces", "nspoid", "tables", "tbloid",
			  &objects->tables, &objects->exclude_tables, inclusive,
			  !checkopts->exclude_toast);
	if (!checkopts->no_indexes)
	{
		appendPQExpBufferStr(sql, ",");
		exclude_index_cte(conn, sql, "namespaces", "nspoid",
						  "excluded_indexes", "idxoid",
						  &objects->exclude_indexes);
		appendPQExpBufferStr(sql, ",");
		if (checkopts->dependents)
			index_cte(conn, sql, "namespaces", "nspoid", "indexes", "idxoid",
					  "excluded_indexes", "tables", "tbloid",
					  &objects->indexes, inclusive);
		else
			index_cte(conn, sql, "namespaces", "nspoid", "indexes", "idxoid",
					  "excluded_indexes", NULL, NULL, &objects->indexes,
					  inclusive);
	}
	appendPQExpBuffer(sql,
					  "\nSELECT checktype, oid FROM ("
					  "\nSELECT %u AS checktype, tables.tbloid AS oid, c.relpages"
					  "\nFROM pg_catalog.pg_class c"
					  "\nJOIN tables"
					  "\nON tables.tbloid OPERATOR(pg_catalog.=) c.oid"
					  "\nWHERE ",
					  CT_TABLE);
	append_ctfilter_quals(sql, "c", CT_TABLE);
	if (!checkopts->no_indexes)
	{
		appendPQExpBuffer(sql,
						  "\nUNION ALL"
						  "\nSELECT %u AS checktype, indexes.idxoid AS oid, c.relpages"
						  "\nFROM pg_catalog.pg_class c"
						  "\nJOIN indexes"
						  "\nON indexes.idxoid OPERATOR(pg_catalog.=) c.oid"
						  "\nWHERE ",
						  CT_BTREE);
		append_ctfilter_quals(sql, "c", CT_BTREE);
	}
	appendPQExpBufferStr(sql,
						 "\n) AS ss"
						 "\nORDER BY relpages DESC, checktype, oid");
}
