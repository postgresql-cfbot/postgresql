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

#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "common/logging.h"
#include "common/username.h"
#include "common/connect.h"
#include "fe_utils/print.h"
#include "fe_utils/simple_list.h"
#include "fe_utils/string_utils.h"
#include "pg_getopt.h"

const char *usage_text[] = {
	"pg_amcheck is the PostgreSQL command line frontend for the amcheck database corruption checker.",
	"",
	"Usage:",
	"  pg_amcheck [OPTION]... [DBNAME [USERNAME]]",
	"",
	"General options:",
	"  -V, --version                output version information, then exit",
	"  -?, --help                   show this help, then exit",
	"  -s, --strict-names           require include patterns to match at least one entity each",
	"  -o, --on-error-stop          stop checking at end of first corrupt page",
	"",
	"Schema checking options:",
	"  -n, --schema=PATTERN         check relations in the specified schema(s) only",
	"  -N, --exclude-schema=PATTERN do NOT check relations in the specified schema(s)",
	"",
	"Table checking options:",
	"  -t, --table=PATTERN          check the specified table(s) only",
	"  -T, --exclude-table=PATTERN  do NOT check the specified table(s)",
	"  -b, --startblock             begin checking table(s) at the given starting block number",
	"  -e, --endblock               check table(s) only up to the given ending block number",
	"  -f, --skip-all-frozen        do NOT check blocks marked as all frozen",
	"  -v, --skip-all-visible       do NOT check blocks marked as all visible",
	"",
	"TOAST table checking options:",
	"  -z, --check-toast            check associated toast tables and toast indexes",
	"  -Z, --skip-toast             do NOT check associated toast tables and toast indexes",
	"  -B, --toast-startblock       begin checking toast table(s) at the given starting block",
	"  -E, --toast-endblock         check toast table(s) only up to the given ending block",
	"",
	"Index checking options:",
	"  -x, --check-indexes          check btree indexes associated with tables being checked",
	"  -X, --skip-indexes           do NOT check any btree indexes",
	"  -i, --index=PATTERN          check the specified index(es) only",
	"  -I, --exclude-index=PATTERN  do NOT check the specified index(es)",
	"  -c, --check-corrupt          check indexes even if their associated table is corrupt",
	"  -C, --skip-corrupt           do NOT check indexes if their associated table is corrupt",
	"  -a, --heapallindexed         check index tuples against the table tuples",
	"  -A, --no-heapallindexed      do NOT check index tuples against the table tuples",
	"  -r, --rootdescend            search from the root page for each index tuple",
	"  -R, --no-rootdescend         do NOT search from the root page for each index tuple",
	"",
	"Connection options:",
	"  -d, --dbname=DBNAME          database name to connect to",
	"  -h, --host=HOSTNAME          database server host or socket directory",
	"  -p, --port=PORT              database server port",
	"  -U, --username=USERNAME      database user name",
	"  -w, --no-password            never prompt for password",
	"  -W, --password               force password prompt (should happen automatically)",
	"",
	NULL						/* sentinel */
};

typedef struct
AmCheckSettings
{
	char	   *dbname;
	char	   *host;
	char	   *port;
	char	   *username;
} ConnectOptions;

typedef enum trivalue
{
	TRI_DEFAULT,
	TRI_NO,
	TRI_YES
} trivalue;

typedef struct
{
	PGconn	   *db;				/* connection to backend */
	bool		notty;			/* stdin or stdout is not a tty (as determined
								 * on startup) */
	trivalue	getPassword;	/* prompt for a username and password */
	const char *progname;		/* in case you renamed pg_amcheck */
	bool		strict_names;	/* The specified names/patterns should to
								 * match at least one entity */
	bool		on_error_stop;	/* The checking of each table should stop
								 * after the first corrupt page is found. */
	bool		skip_frozen;	/* Do not check pages marked all frozen */
	bool		skip_visible;	/* Do not check pages marked all visible */
	bool		check_indexes;	/* Check btree indexes */
	bool		check_toast;	/* Check associated toast tables and indexes */
	bool		check_corrupt;	/* Check indexes even if table is corrupt */
	bool		heapallindexed;	/* Perform index to table reconciling checks */
	bool		rootdescend;	/* Perform index rootdescend checks */
	char	   *startblock;		/* Block number where checking begins */
	char	   *endblock;		/* Block number where checking ends, inclusive */
	char       *toaststart;		/* Block number where toast checking begins */
	char	   *toastend;		/* Block number where toast checking ends,
								 * inclusive */
} AmCheckSettings;

static AmCheckSettings settings;

/*
 * Object inclusion/exclusion lists
 *
 * The string lists record the patterns given by command-line switches,
 * which we then convert to lists of Oids of matching objects.
 */
static SimpleStringList schema_include_patterns = {NULL, NULL};
static SimpleOidList schema_include_oids = {NULL, NULL};
static SimpleStringList schema_exclude_patterns = {NULL, NULL};
static SimpleOidList schema_exclude_oids = {NULL, NULL};

static SimpleStringList table_include_patterns = {NULL, NULL};
static SimpleOidList table_include_oids = {NULL, NULL};
static SimpleStringList table_exclude_patterns = {NULL, NULL};
static SimpleOidList table_exclude_oids = {NULL, NULL};

static SimpleStringList index_include_patterns = {NULL, NULL};
static SimpleOidList index_include_oids = {NULL, NULL};
static SimpleStringList index_exclude_patterns = {NULL, NULL};
static SimpleOidList index_exclude_oids = {NULL, NULL};

/*
 * List of tables to be checked, compiled from above lists.
 */
static SimpleOidList checklist = {NULL, NULL};

/*
 * Strings to be constructed once upon first use.  These could be made
 * string constants instead, but that would require embedding knowledge
 * of the single character values for each relkind, such as 'm' for
 * materialized views, which we'd rather not embed here.
 */
static char *table_relkind_quals = NULL;
static char *index_relkind_quals = NULL;

/*
 * Functions to get pointers to the two strings, above, after initializing
 * them upon the first call to the function.
 */
static const char *get_table_relkind_quals(void);
static const char *get_index_relkind_quals(void);

/*
 * Functions for running the various corruption checks.
 */
static void check_tables(SimpleOidList *checklist);
static uint64 check_toast(Oid tbloid);
static uint64 check_table(Oid tbloid, const char *startblock,
						  const char *endblock, bool on_error_stop,
						  bool check_toast);
static uint64 check_indexes(Oid tbloid, const SimpleOidList *include_oids,
							const SimpleOidList *exclude_oids);
static uint64 check_index(const char *idxoid, const char *idxname,
						  const char *tblname);

/*
 * Functions implementing standard command line behaviors.
 */
static void parse_cli_options(int argc, char *argv[],
							  ConnectOptions *connOpts);
static void usage(void);
static void showVersion(void);
static void NoticeProcessor(void *arg, const char *message);

/*
 * Functions for converting command line options that include or exclude
 * schemas, tables, and indexes by pattern into internally useful lists of
 * Oids for objects that match those patterns.
 */
static void expand_schema_name_patterns(const SimpleStringList *patterns,
										const SimpleOidList *exclude_oids,
										SimpleOidList *oids,
										bool strict_names);
static void expand_relkind_name_patterns(const SimpleStringList *patterns,
										 const SimpleOidList *exclude_nsp_oids,
										 const SimpleOidList *exclude_oids,
										 SimpleOidList *oids,
										 bool strict_names,
										 const char *missing_errtext,
										 const char *relkind_quals);
static void expand_table_name_patterns(const SimpleStringList *patterns,
									   const SimpleOidList *exclude_nsp_oids,
									   const SimpleOidList *exclude_oids,
									   SimpleOidList *oids,
									   bool strict_names);
static void expand_index_name_patterns(const SimpleStringList *patterns,
									   const SimpleOidList *exclude_nsp_oids,
									   const SimpleOidList *exclude_oids,
									   SimpleOidList *oids,
									   bool strict_names);
static void get_table_check_list(const SimpleOidList *include_nsp,
								 const SimpleOidList *exclude_nsp,
								 const SimpleOidList *include_tbl,
								 const SimpleOidList *exclude_tbl,
								 SimpleOidList *checklist);
static PGresult *ExecuteSqlQuery(const char *query, char **error);
static PGresult *ExecuteSqlQueryOrDie(const char *query);

static void append_csv_oids(PQExpBuffer querybuf, const SimpleOidList *oids);
static void apply_filter(PQExpBuffer querybuf, const char *lval,
						 const SimpleOidList *oids, bool include);

#define fatal(...) do { pg_log_error(__VA_ARGS__); exit(1); } while(0)

/* Like fatal(), but with a complaint about a particular query. */
static void
die_on_query_failure(const char *query)
{
	pg_log_error("query failed: %s",
				 PQerrorMessage(settings.db));
	fatal("query was: %s", query);
}

#define EXIT_BADCONN 2

int
main(int argc, char **argv)
{
	ConnectOptions connOpts;
	bool		have_password = false;
	char		password[100];
	bool		new_pass;

	pg_logging_init(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_amcheck"));

	if (argc > 1)
	{
		if ((strcmp(argv[1], "-?") == 0) ||
			(argc == 2 && (strcmp(argv[1], "--help") == 0)))
		{
			usage();
			exit(EXIT_SUCCESS);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			showVersion();
			exit(EXIT_SUCCESS);
		}
	}

	memset(&settings, 0, sizeof(settings));
	settings.progname = get_progname(argv[0]);

	settings.db = NULL;
	setDecimalLocale();

	settings.notty = (!isatty(fileno(stdin)) || !isatty(fileno(stdout)));

	settings.getPassword = TRI_DEFAULT;

	/*
	 * Default behaviors for user settable options.  Note that these default to
	 * doing all the safe checks and none of the unsafe ones, on the theory
	 * that if a user says "pg_amcheck mydb" without specifying any additional
	 * options, we should check everything we know how to check without risking
	 * any backend aborts.
	 */

	settings.on_error_stop = false;
	settings.skip_frozen = false;
	settings.skip_visible = false;

	/* Index checking options */
	settings.check_indexes = false;
	settings.check_corrupt = false;
	settings.heapallindexed = false;
	settings.rootdescend = false;

	/*
	 * Reconciling toasted attributes from the main table against the toast
	 * table can crash the backend if the toast table or index are corrupt.  We
	 * can optionally check the toast table and then the toast index prior to
	 * checking the main table, but if the toast table or index are
	 * concurrently corrupted after we conclude they are valid, the check of
	 * the main table can crash the backend.  The oneous is on any caller who
	 * enables this option to make certain the environment is sufficiently
	 * stable that concurrent corruptions of the toast is not possible.
	 */
	settings.check_toast = false;

	parse_cli_options(argc, argv, &connOpts);

	if (settings.getPassword == TRI_YES)
	{
		/*
		 * We can't be sure yet of the username that will be used, so don't
		 * offer a potentially wrong one.  Typical uses of this option are
		 * noninteractive anyway.
		 */
		simple_prompt("Password: ", password, sizeof(password), false);
		have_password = true;
	}

	/* loop until we have a password if requested by backend */
	do
	{
#define ARRAY_SIZE	8
		const char **keywords = pg_malloc(ARRAY_SIZE * sizeof(*keywords));
		const char **values = pg_malloc(ARRAY_SIZE * sizeof(*values));

		keywords[0] = "host";
		values[0] = connOpts.host;
		keywords[1] = "port";
		values[1] = connOpts.port;
		keywords[2] = "user";
		values[2] = connOpts.username;
		keywords[3] = "password";
		values[3] = have_password ? password : NULL;
		keywords[4] = "dbname"; /* see do_connect() */
		if (connOpts.dbname == NULL)
		{
			if (getenv("PGDATABASE"))
				values[4] = getenv("PGDATABASE");
			else if (getenv("PGUSER"))
				values[4] = getenv("PGUSER");
			else
				values[4] = "postgres";
		}
		else
			values[4] = connOpts.dbname;
		keywords[5] = "fallback_application_name";
		values[5] = settings.progname;
		keywords[6] = "client_encoding";
		values[6] = (settings.notty ||
					 getenv("PGCLIENTENCODING")) ? NULL : "auto";
		keywords[7] = NULL;
		values[7] = NULL;

		new_pass = false;
		settings.db = PQconnectdbParams(keywords, values, true);
		if (settings.db == NULL)
		{
			pg_log_error("no connection to server after initial attempt");
			exit(EXIT_BADCONN);
		}

		free(keywords);
		free(values);

		if (PQstatus(settings.db) == CONNECTION_BAD &&
			PQconnectionNeedsPassword(settings.db) &&
			!have_password &&
			settings.getPassword != TRI_NO)
		{
			/*
			 * Before closing the old PGconn, extract the user name that was
			 * actually connected with.
			 */
			const char *realusername = PQuser(settings.db);
			char	   *password_prompt;

			if (realusername && realusername[0])
				password_prompt = psprintf(_("Password for user %s: "),
										   realusername);
			else
				password_prompt = pg_strdup(_("Password: "));
			PQfinish(settings.db);

			simple_prompt(password_prompt, password, sizeof(password), false);
			free(password_prompt);
			have_password = true;
			new_pass = true;
		}
	} while (new_pass);

	if (!settings.db)
	{
		pg_log_error("no connection to server");
		exit(EXIT_BADCONN);
	}

	if (PQstatus(settings.db) == CONNECTION_BAD)
	{
		pg_log_error("could not connect to server: %s",
					 PQerrorMessage(settings.db));
		PQfinish(settings.db);
		exit(EXIT_BADCONN);
	}

	/*
	 * Expand schema, table, and index exclusion patterns, if any.  Note that
	 * non-matching exclusion patterns are not an error, even when
	 * --strict-names was specified.
	 */
	expand_schema_name_patterns(&schema_exclude_patterns, NULL,
								&schema_exclude_oids, false);
	expand_table_name_patterns(&table_exclude_patterns, NULL, NULL,
							   &table_exclude_oids, false);
	expand_index_name_patterns(&index_exclude_patterns, NULL, NULL,
							   &index_exclude_oids, false);

	/* Expand schema selection patterns into Oid lists */
	if (schema_include_patterns.head != NULL)
	{
		expand_schema_name_patterns(&schema_include_patterns,
									&schema_exclude_oids,
									&schema_include_oids,
									settings.strict_names);
		if (schema_include_oids.head == NULL)
			fatal("no matching schemas were found");
	}

	/* Expand table selection patterns into Oid lists */
	if (table_include_patterns.head != NULL)
	{
		expand_table_name_patterns(&table_include_patterns,
								   &schema_exclude_oids,
								   &table_exclude_oids,
								   &table_include_oids,
								   settings.strict_names);
		if (table_include_oids.head == NULL)
			fatal("no matching tables were found");
	}

	/* Expand index selection patterns into Oid lists */
	if (index_include_patterns.head != NULL)
	{
		expand_index_name_patterns(&index_include_patterns,
								   &schema_exclude_oids,
								   &index_exclude_oids,
								   &index_include_oids,
								   settings.strict_names);
		if (index_include_oids.head == NULL)
			fatal("no matching indexes were found");
	}

	/*
	 * Compile list of all tables to be checked based on namespace and table
	 * includes and excludes.
	 */
	get_table_check_list(&schema_include_oids, &schema_exclude_oids,
						 &table_include_oids, &table_exclude_oids, &checklist);

	PQsetNoticeProcessor(settings.db, NoticeProcessor, NULL);

	/*
	 * All information about corrupt indexes are returned via ereport, not as
	 * tuples.  We want all the details to report if corruption exists.
	 */
	PQsetErrorVerbosity(settings.db, PQERRORS_VERBOSE);

	check_tables(&checklist);

	return 0;
}

/*
 * Conditionally add a restriction to a query such that lval must be an Oid in
 * the given list of Oids, except that for a null or empty oids list argument,
 * no filtering is done and we return without having modified the query buffer.
 *
 * The query argument must already have begun the WHERE clause and must be in a
 * state where we can append an AND clause.  No checking of this requirement is
 * done here.
 *
 * On return, the query buffer will be extended with an AND clause that filters
 * only those rows where the lval is an Oid present in the given list of oids.
 */
static inline void
include_filter(PQExpBuffer querybuf, const char *lval, const SimpleOidList *oids)
{
	apply_filter(querybuf, lval, oids, true);
}

/*
 * Same as include_filter, above, except that for a non-null, non-empty oids
 * list, the lval is restricted to not be any of the values in the list.
 */
static inline void
exclude_filter(PQExpBuffer querybuf, const char *lval, const SimpleOidList *oids)
{
	apply_filter(querybuf, lval, oids, false);
}

/*
 * Check each table from the given checklist per the user specified options.
 */
static void
check_tables(SimpleOidList *checklist)
{
	const SimpleOidListCell *cell;

	for (cell = checklist->head; cell; cell = cell->next)
	{
		uint64	corruptions = 0;
		bool	reconcile_toast;

		/*
		 * If we skip checking the toast table, or if during the check we
		 * detect any toast table corruption, the main table checks below must
		 * not reconcile toasted attributes against the toast table, as such
		 * accesses to the toast table might crash the backend.  Instead, skip
		 * such reconciliations for this table.
		 *
		 * This protection contains a race condition; the toast table or index
		 * could become corrupted concurrently with our checks, but prevention
		 * of such concurrent corruption is documented as the caller's
		 * reponsibility, so we don't worry about it here.
		 */
		reconcile_toast = false;
		if (settings.check_toast)
		{
			if (check_toast(cell->val) == 0)
				reconcile_toast = true;
		}

		corruptions = check_table(cell->val,
								  settings.startblock,
								  settings.endblock,
								  settings.on_error_stop,
								  reconcile_toast);

		if (settings.check_indexes)
		{
			bool old_heapallindexed;

			/* Optionally skip the index checks for a corrupt table. */
			if (corruptions && !settings.check_corrupt)
				continue;

			/*
			 * The btree checking logic which optionally checks the contents of
			 * an index against the corresponding table has not yet been
			 * sufficiently hardened against corrupt tables.  In particular,
			 * when called with heapallindexed true, it segfaults if the file
			 * backing the table relation has been erroneously unlinked.  In
			 * any event, it seems unwise to reconcile an index against its
			 * table when we already know the table is corrupt.
			 */
			old_heapallindexed = settings.heapallindexed;
			if (corruptions)
				settings.heapallindexed = false;

			corruptions += check_indexes(cell->val,
										 &index_include_oids,
										 &index_exclude_oids);

			settings.heapallindexed = old_heapallindexed;
		}
	}
}

/*
 * For a given main table relation, returns the associated toast table,
 * or InvalidOid if none exists.
 */
static Oid
get_toast_oid(Oid tbloid)
{
	PQExpBuffer	querybuf = createPQExpBuffer();
	PGresult   *res;
	char	   *error = NULL;
	Oid			result = InvalidOid;

	appendPQExpBuffer(querybuf,
					  "SELECT c.reltoastrelid"
					  "\nFROM pg_catalog.pg_class c"
					  "\nWHERE c.oid = %u",
					  tbloid);
	res = ExecuteSqlQuery(querybuf->data, &error);
	if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0)
		result = atooid(PQgetvalue(res, 0, 0));
	else if (error)
		die_on_query_failure(querybuf->data);

	PQclear(res);
	destroyPQExpBuffer(querybuf);

	return result;
}

/*
 * For the given main table relation, checks the associated toast table and
 * index, in any.  This should be performed *before* checking the main table
 * relation, as the checks inside verify_heapam assume both the toast table and
 * toast index are usable.
 *
 * Returns the number of corruptions detected.
 */
static uint64
check_toast(Oid tbloid)
{
	Oid			toastoid;
	uint64		corruption_cnt = 0;

	if (settings.db == NULL)
		fatal("no connection on entry to check_toast");

	toastoid = get_toast_oid(tbloid);
	if (OidIsValid(toastoid))
	{
		corruption_cnt = check_table(toastoid, settings.toaststart,
									 settings.toastend, settings.on_error_stop,
									 false);
		/*
		 * If the toast table is corrupt, checking the index is not safe.
		 * There is a race condition here, as the toast table could be
		 * concurrently corrupted, but preventing concurrent corruption is the
		 * caller's responsibility, not ours.
		 */
		if (corruption_cnt == 0)
			corruption_cnt += check_indexes(toastoid, NULL, NULL);
	}

	return corruption_cnt;
}

/*
 * Checks the given table for corruption, returning the number of corruptions
 * detected and printed to the user.
 */
static uint64
check_table(Oid tbloid, const char *startblock, const char *endblock,
			bool on_error_stop, bool check_toast)
{
	PQExpBuffer querybuf;
	PGresult   *res;
	int			i;
	char	   *skip;
	char	   *toast;
	const char *stop;
	char	   *error = NULL;
	uint64		corruption_cnt = 0;

	if (settings.db == NULL)
		fatal("no connection on entry to check_table");

	if (startblock == NULL)
		startblock = "NULL";
	if (endblock == NULL)
		endblock = "NULL";
	if (settings.skip_frozen)
		skip = pg_strdup("'all frozen'");
	else if (settings.skip_visible)
		skip = pg_strdup("'all visible'");
	else
		skip = pg_strdup("'none'");
	stop = (on_error_stop) ? "true" : "false";
	toast = (check_toast) ? "true" : "false";

	querybuf = createPQExpBuffer();

	appendPQExpBuffer(querybuf,
					  "SELECT c.relname, v.blkno, v.offnum, v.attnum, v.msg "
					  "FROM verify_heapam("
							"relation := %u, "
							"on_error_stop := %s, "
							"skip := %s, "
							"check_toast := %s, "
							"startblock := %s, "
							"endblock := %s) v, "
							"pg_catalog.pg_class c "
					  "WHERE c.oid = %u",
					  tbloid, stop, skip, toast, startblock, endblock, tbloid);

	res = ExecuteSqlQuery(querybuf->data, &error);
	if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0)
	{
		corruption_cnt += PQntuples(res);
		for (i = 0; i < PQntuples(res); i++)
		{
			printf("(relname=%s,blkno=%s,offnum=%s,attnum=%s)\n%s\n",
					PQgetvalue(res, i, 0),	/* relname */
					PQgetvalue(res, i, 1),	/* blkno */
					PQgetvalue(res, i, 2),	/* offnum */
					PQgetvalue(res, i, 3),	/* attnum */
					PQgetvalue(res, i, 4)); /* msg */
		}
	}
	else if (error)
	{
		corruption_cnt++;
		printf("%s\n", error);
		pfree(error);
	}

	PQclear(res);
	destroyPQExpBuffer(querybuf);
	return corruption_cnt;
}

static uint64
check_indexes(Oid tbloid, const SimpleOidList *include_oids,
			  const SimpleOidList *exclude_oids)
{
	PQExpBuffer querybuf;
	PGresult   *res;
	int			i;
	char	   *error = NULL;
	uint64		corruption_cnt = 0;

	if (settings.db == NULL)
		fatal("no connection on entry to check_indexes");

	querybuf = createPQExpBuffer();
	appendPQExpBuffer(querybuf,
					  "SELECT i.indexrelid, ci.relname, ct.relname"
					  "\nFROM pg_catalog.pg_index i, pg_catalog.pg_class ci, "
					  "pg_catalog.pg_class ct"
					  "\nWHERE i.indexrelid = ci.oid"
					  "\n    AND i.indrelid = ct.oid"
					  "\n    AND ci.relam = %u"
					  "\n    AND i.indrelid = %u",
					  BTREE_AM_OID, tbloid);
	include_filter(querybuf, "i.indexrelid", include_oids);
	exclude_filter(querybuf, "i.indexrelid", exclude_oids);

	res = ExecuteSqlQuery(querybuf->data, &error);
	if (PQresultStatus(res) == PGRES_TUPLES_OK)
	{
		for (i = 0; i < PQntuples(res); i++)
			corruption_cnt += check_index(PQgetvalue(res, i, 0),
										  PQgetvalue(res, i, 1),
										  PQgetvalue(res, i, 2));
	}
	else if (error)
	{
		corruption_cnt++;
		printf("%s\n", error);
		pfree(error);
	}

	PQclear(res);
	destroyPQExpBuffer(querybuf);

	return corruption_cnt;
}

static uint64
check_index(const char *idxoid, const char *idxname, const char *tblname)
{
	PQExpBuffer querybuf;
	PGresult   *res;
	uint64		corruption_cnt = 0;

	if (settings.db == NULL)
		fatal("no connection on entry to check_index");
	if (idxname == NULL)
		fatal("no index name on entry to check_index");
	if (tblname == NULL)
		fatal("no table name on entry to check_index");

	querybuf = createPQExpBuffer();
	appendPQExpBuffer(querybuf,
					  "SELECT bt_index_parent_check('%s'::regclass, %s, %s)",
					  idxoid,
					  settings.heapallindexed ? "true" : "false",
					  settings.rootdescend ? "true" : "false");
	res = PQexec(settings.db, querybuf->data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		corruption_cnt++;
		printf("index check failed for index %s of table %s:\n",
				idxname, tblname);
		printf("%s", PQerrorMessage(settings.db));
	}

	PQclear(res);
	destroyPQExpBuffer(querybuf);

	return corruption_cnt;
}

static void
parse_cli_options(int argc, char *argv[], ConnectOptions *connOpts)
{
	static struct option long_options[] =
	{
		{"check-corrupt", no_argument, NULL, 'c'},
		{"check-indexes", no_argument, NULL, 'x'},
		{"check-toast", no_argument, NULL, 'z'},
		{"dbname", required_argument, NULL, 'd'},
		{"endblock", required_argument, NULL, 'e'},
		{"exclude-index", required_argument, NULL, 'I'},
		{"exclude-schema", required_argument, NULL, 'N'},
		{"exclude-table", required_argument, NULL, 'T'},
		{"heapallindexed", no_argument, NULL, 'a'},
		{"help", optional_argument, NULL, '?'},
		{"host", required_argument, NULL, 'h'},
		{"index", required_argument, NULL, 'i'},
		{"no-heapallindexed", no_argument, NULL, 'A'},
		{"no-password", no_argument, NULL, 'w'},
		{"no-rootdescend", no_argument, NULL, 'R'},
		{"on-error-stop", no_argument, NULL, 'o'},
		{"password", no_argument, NULL, 'W'},
		{"port", required_argument, NULL, 'p'},
		{"rootdescend", no_argument, NULL, 'r'},
		{"schema", required_argument, NULL, 'n'},
		{"skip-all-frozen", no_argument, NULL, 'f'},
		{"skip-all-visible", no_argument, NULL, 'v'},
		{"skip-corrupt", no_argument, NULL, 'C'},
		{"skip-indexes", no_argument, NULL, 'X'},
		{"skip-toast", no_argument, NULL, 'Z'},
		{"startblock", required_argument, NULL, 'b'},
		{"strict-names", no_argument, NULL, 's'},
		{"table", required_argument, NULL, 't'},
		{"toast-endblock", required_argument, NULL, 'E'},
		{"toast-startblock", required_argument, NULL, 'B'},
		{"username", required_argument, NULL, 'U'},
		{"version", no_argument, NULL, 'V'},
		{NULL, 0, NULL, 0}
	};

	int			optindex;
	int			c;

	memset(connOpts, 0, sizeof *connOpts);

	while ((c = getopt_long(argc, argv, "aAb:B:cCd:e:E:fh:i:I:n:N:op:rRst:T:U:vVwWxXzZ?1",
							long_options, &optindex)) != -1)
	{
		switch (c)
		{
			case 'a':
				settings.heapallindexed = true;
				break;
			case 'A':
				settings.heapallindexed = false;
				break;
			case 'b':
				settings.startblock = pg_strdup(optarg);
				break;
			case 'B':
				settings.toaststart = pg_strdup(optarg);
				break;
			case 'c':
				settings.check_corrupt = true;
				break;
			case 'C':
				settings.check_corrupt = false;
				break;
			case 'd':
				connOpts->dbname = pg_strdup(optarg);
				break;
			case 'e':
				settings.endblock = pg_strdup(optarg);
				break;
			case 'E':
				settings.toastend = pg_strdup(optarg);
				break;
			case 'f':
				settings.skip_frozen = true;
				break;
			case 'h':
				connOpts->host = pg_strdup(optarg);
				break;
			case 'i':
				simple_string_list_append(&index_include_patterns, optarg);
				break;
			case 'I':
				simple_string_list_append(&index_exclude_patterns, optarg);
				break;
			case 'n':			/* include schema(s) */
				simple_string_list_append(&schema_include_patterns, optarg);
				break;
			case 'N':			/* exclude schema(s) */
				simple_string_list_append(&schema_exclude_patterns, optarg);
				break;
			case 'o':
				settings.on_error_stop = true;
				break;
			case 'p':
				connOpts->port = pg_strdup(optarg);
				break;
			case 's':
				settings.strict_names = true;
				break;
			case 'r':
				settings.rootdescend = true;
				break;
			case 'R':
				settings.rootdescend = false;
				break;
			case 't':			/* include table(s) */
				simple_string_list_append(&table_include_patterns, optarg);
				break;
			case 'T':			/* exclude table(s) */
				simple_string_list_append(&table_exclude_patterns, optarg);
				break;
			case 'U':
				connOpts->username = pg_strdup(optarg);
				break;
			case 'v':
				settings.skip_visible = true;
				break;
			case 'V':
				showVersion();
				exit(EXIT_SUCCESS);
			case 'w':
				settings.getPassword = TRI_NO;
				break;
			case 'W':
				settings.getPassword = TRI_YES;
				break;
			case 'x':
				settings.check_indexes = true;
				break;
			case 'X':
				settings.check_indexes = false;
				break;
			case 'z':
				settings.check_toast = true;
				break;
			case 'Z':
				settings.check_toast = false;
				break;
			case '?':
				if (optind <= argc &&
					strcmp(argv[optind - 1], "-?") == 0)
				{
					/* actual help option given */
					usage();
					exit(EXIT_SUCCESS);
				}
				else
				{
					/* getopt error (unknown option or missing argument) */
					goto unknown_option;
				}
				break;
			case 1:
				{
					if (!optarg || strcmp(optarg, "options") == 0)
						usage();
					else
						goto unknown_option;

					exit(EXIT_SUCCESS);
				}
				break;
			default:
		unknown_option:
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
						settings.progname);
				exit(EXIT_FAILURE);
				break;
		}
	}

	/*
	 * if we still have arguments, use it as the database name and username
	 */
	while (argc - optind >= 1)
	{
		if (!connOpts->dbname)
			connOpts->dbname = argv[optind];
		else if (!connOpts->username)
			connOpts->username = argv[optind];
		else
			pg_log_warning("extra command-line argument \"%s\" ignored",
						   argv[optind]);

		optind++;
	}

}

/*
 * usage
 *
 * print out command line arguments
 */
static void
usage(void)
{
	int			lineno;

	for (lineno = 0; usage_text[lineno]; lineno++)
		printf("%s\n", usage_text[lineno]);
	printf("Report bugs to <%s>.\n", PACKAGE_BUGREPORT);
	printf("%s home page: <%s>\n", PACKAGE_NAME, PACKAGE_URL);
}

static void
showVersion(void)
{
	puts("pg_amcheck (PostgreSQL) " PG_VERSION);
}

/*
 * for backend Notice messages (INFO, WARNING, etc)
 */
static void
NoticeProcessor(void *arg, const char *message)
{
	(void) arg;					/* not used */
	pg_log_info("%s", message);
}

/*
 * Helper function for apply_filter, below.
 */
static void
append_csv_oids(PQExpBuffer querybuf, const SimpleOidList *oids)
{
	const SimpleOidListCell *cell;
	const char *comma;

	for (comma = "", cell = oids->head; cell; comma = ", ", cell = cell->next)
		appendPQExpBuffer(querybuf, "%s%u", comma, cell->val);
}

/*
 * Internal implementation of include_filter and exclude_filter
 */
static void
apply_filter(PQExpBuffer querybuf, const char *lval, const SimpleOidList *oids,
			 bool include)
{
	if (!oids || !oids->head)
		return;
	if (include)
		appendPQExpBuffer(querybuf, "\nAND %s OPERATOR(pg_catalog.=) ANY(array[", lval);
	else
		appendPQExpBuffer(querybuf, "\nAND %s OPERATOR(pg_catalog.!=) ALL(array[", lval);
	append_csv_oids(querybuf, oids);
	appendPQExpBuffer(querybuf, "]::OID[])");
}

/*
 * Find and append to the given Oid list the Oids of all schemas matching the
 * given list of patterns but not included in the given list of excluded Oids.
 */
static void
expand_schema_name_patterns(const SimpleStringList *patterns,
							const SimpleOidList *exclude_nsp,
							SimpleOidList *oids,
							bool strict_names)
{
	PQExpBuffer querybuf;
	PGresult   *res;
	SimpleStringListCell *cell;
	int			i;

	if (settings.db == NULL)
		fatal("no connection on entry to expand_schema_name_patterns");

	if (patterns->head == NULL)
		return;					/* nothing to do */

	querybuf = createPQExpBuffer();

	/*
	 * The loop below runs multiple SELECTs might sometimes result in
	 * duplicate entries in the Oid list, but we don't care.
	 */

	for (cell = patterns->head; cell; cell = cell->next)
	{
		appendPQExpBufferStr(querybuf,
							 "SELECT oid FROM pg_catalog.pg_namespace n\n");
		processSQLNamePattern(settings.db, querybuf, cell->val, false,
							  false, NULL, "n.nspname", NULL, NULL);
		exclude_filter(querybuf, "n.oid", exclude_nsp);

		res = ExecuteSqlQueryOrDie(querybuf->data);
		if (strict_names && PQntuples(res) == 0)
			fatal("no matching schemas were found for pattern \"%s\"",
				  cell->val);

		for (i = 0; i < PQntuples(res); i++)
		{
			simple_oid_list_append(oids, atooid(PQgetvalue(res, i, 0)));
		}

		PQclear(res);
		resetPQExpBuffer(querybuf);
	}

	destroyPQExpBuffer(querybuf);
}

/*
 * Find and append to the given Oid list the Oids of all relations matching the
 * given list of patterns but not included in the given list of excluded Oids
 * nor in one of the given excluded namespaces.  The relations are filtered by
 * the given schema_quals.  They are further filtered by the given
 * relkind_quals, allowing the caller to restrict the relations to just indexes
 * or tables.  The missing_errtext should be a message for use in error
 * messages if no matching relations are found and strict_names was specified.
 */
static void
expand_relkind_name_patterns(const SimpleStringList *patterns,
							 const SimpleOidList *exclude_nsp_oids,
							 const SimpleOidList *exclude_oids,
							 SimpleOidList *oids,
							 bool strict_names,
							 const char *missing_errtext,
							 const char *relkind_quals)
{
	PQExpBuffer querybuf;
	PGresult   *res;
	SimpleStringListCell *cell;
	int			i;

	if (settings.db == NULL)
		fatal("no connection on entry to expand_relkind_name_patterns");

	if (patterns->head == NULL)
		return;					/* nothing to do */

	querybuf = createPQExpBuffer();

	/*
	 * this might sometimes result in duplicate entries in the Oid list, but
	 * we don't care.
	 */

	for (cell = patterns->head; cell; cell = cell->next)
	{
		/*
		 * Query must remain ABSOLUTELY devoid of unqualified names.  This
		 * would be unnecessary given a pg_table_is_visible() variant taking a
		 * search_path argument.
		 */
		appendPQExpBuffer(querybuf,
						  "SELECT c.oid"
						  "\nFROM pg_catalog.pg_class c"
						  "\n    LEFT JOIN pg_catalog.pg_namespace n"
						  "\n    ON n.oid OPERATOR(pg_catalog.=) c.relnamespace"
						  "\nWHERE c.relkind OPERATOR(pg_catalog.=) %s\n",
						  relkind_quals);
		exclude_filter(querybuf, "c.oid", exclude_oids);
		exclude_filter(querybuf, "n.oid", exclude_nsp_oids);
		processSQLNamePattern(settings.db, querybuf, cell->val, true,
							  false, "n.nspname", "c.relname", NULL, NULL);
		res = ExecuteSqlQueryOrDie(querybuf->data);
		if (strict_names && PQntuples(res) == 0)
			fatal("%s \"%s\"", missing_errtext, cell->val);

		for (i = 0; i < PQntuples(res); i++)
			simple_oid_list_append(oids, atooid(PQgetvalue(res, i, 0)));

		PQclear(res);
		resetPQExpBuffer(querybuf);
	}

	destroyPQExpBuffer(querybuf);
}

/*
 * Find the Oids of all tables matching the given list of patterns,
 * and append them to the given Oid list.
 */
static void
expand_table_name_patterns(const SimpleStringList *patterns, const SimpleOidList *exclude_nsp_oids,
						   const SimpleOidList *exclude_oids, SimpleOidList *oids, bool strict_names)
{
	expand_relkind_name_patterns(patterns, exclude_nsp_oids, exclude_oids, oids, strict_names,
								 "no matching tables were found for pattern",
								 get_table_relkind_quals());
}

/*
 * Find the Oids of all indexes matching the given list of patterns,
 * and append them to the given Oid list.
 */
static void
expand_index_name_patterns(const SimpleStringList *patterns, const SimpleOidList *exclude_nsp_oids,
						   const SimpleOidList *exclude_oids, SimpleOidList *oids, bool strict_names)
{
	expand_relkind_name_patterns(patterns, exclude_nsp_oids, exclude_oids, oids, strict_names,
								 "no matching indexes were found for pattern",
								 get_index_relkind_quals());
}

static void
get_table_check_list(const SimpleOidList *include_nsp, const SimpleOidList *exclude_nsp,
					 const SimpleOidList *include_tbl, const SimpleOidList *exclude_tbl,
					 SimpleOidList *checklist)
{
	PQExpBuffer querybuf;
	PGresult   *res;
	int			i;

	if (settings.db == NULL)
		fatal("no connection on entry to get_table_check_list");

	querybuf = createPQExpBuffer();

	appendPQExpBuffer(querybuf,
					  "SELECT c.oid"
					  "\nFROM pg_catalog.pg_class c, pg_catalog.pg_namespace n"
					  "\nWHERE n.oid OPERATOR(pg_catalog.=) c.relnamespace"
					  "\n	AND c.relkind OPERATOR(pg_catalog.=) %s\n",
					  get_table_relkind_quals());
	include_filter(querybuf, "n.oid", include_nsp);
	exclude_filter(querybuf, "n.oid", exclude_nsp);
	include_filter(querybuf, "c.oid", include_tbl);
	exclude_filter(querybuf, "c.oid", exclude_tbl);

	res = ExecuteSqlQueryOrDie(querybuf->data);
	for (i = 0; i < PQntuples(res); i++)
		simple_oid_list_append(checklist, atooid(PQgetvalue(res, i, 0)));

	PQclear(res);
	destroyPQExpBuffer(querybuf);
}

static PGresult *
ExecuteSqlQueryOrDie(const char *query)
{
	PGresult   *res;

	res = PQexec(settings.db, query);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		die_on_query_failure(query);
	return res;
}

/*
 * Execute the given SQL query.  This function should only be used for queries
 * which are not expected to fail under normal circumstances, as failures will
 * result in the printing of error messages, which will look a bit messy when
 * interleaved with corruption reports.
 *
 * On error, use the supplied error_context string and the error string
 * returned from the database connection to print an error message for the
 * user.
 *
 * The error_context argument is pfree'd by us at the end of the call.
 */
static PGresult *
ExecuteSqlQuery(const char *query, char **error)
{
	PGresult   *res;

	res = PQexec(settings.db, query);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		*error = pstrdup(PQerrorMessage(settings.db));
	return res;
}

/*
 * Return the cached relkind quals string for tables, computing it first if we
 * don't have one cached.
 */
static const char *
get_table_relkind_quals(void)
{
	if (!table_relkind_quals)
		table_relkind_quals = psprintf("ANY(array['%c', '%c', '%c'])",
									   RELKIND_RELATION, RELKIND_MATVIEW,
									   RELKIND_PARTITIONED_TABLE);
	return table_relkind_quals;
}

/*
 * Return the cached relkind quals string for indexes, computing it first if we
 * don't have one cached.
 */
static const char *
get_index_relkind_quals(void)
{
	if (!index_relkind_quals)
		index_relkind_quals = psprintf("'%c'", RELKIND_INDEX);
	return index_relkind_quals;
}
