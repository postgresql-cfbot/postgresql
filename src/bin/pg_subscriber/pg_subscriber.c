/*-------------------------------------------------------------------------
 *
 * pg_subscriber.c
 *	  Create a new logical replica from a base backup or a standby server
 *
 * Copyright (C) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/bin/pg_subscriber/pg_subscriber.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>

#include "catalog/pg_control.h"
#include "common/connect.h"
#include "common/controldata_utils.h"
#include "common/file_utils.h"
#include "common/logging.h"
#include "fe_utils/recovery_gen.h"
#include "fe_utils/simple_list.h"
#include "getopt_long.h"
#include "utils/pidfile.h"

typedef struct LogicalRepInfo
{
	Oid			oid;			/* database OID */
	char	   *dbname;			/* database name */
	char	   *pubconninfo;	/* publication connection string for logical
								 * replication */
	char	   *subconninfo;	/* subscription connection string for logical
								 * replication */
	char	   *pubname;		/* publication name */
	char	   *subname;		/* subscription name (also replication slot
								 * name) */

	bool		made_replslot;		/* replication slot was created */
	bool		made_publication;	/* publication was created */
	bool		made_subscription;	/* subscription was created */
}			LogicalRepInfo;

static void cleanup_objects_atexit(void);
static void usage();
static char *get_base_conninfo(char *conninfo, char *dbname,
							   const char *noderole);
static bool check_data_directory(const char *datadir);
static char *concat_conninfo_dbname(const char *conninfo, const char *dbname);
static PGconn *connect_database(const char *conninfo, bool secure_search_path);
static void disconnect_database(PGconn *conn);
static char *get_sysid_from_conn(const char *conninfo);
static char *get_control_from_datadir(const char *datadir);
static char *create_logical_replication_slot(PGconn *conn, LogicalRepInfo *dbinfo,
											 const char *slot_name);
static void drop_replication_slot(PGconn *conn, LogicalRepInfo *dbinfo, const char *slot_name);
static void pg_ctl_status(const char *pg_ctl_cmd, int rc, int action);
static bool postmaster_is_alive(pid_t pid);
static void wait_postmaster_connection(const char *conninfo);
static void wait_for_end_recovery(const char *conninfo);
static void create_publication(PGconn *conn, LogicalRepInfo *dbinfo);
static void drop_publication(PGconn *conn, LogicalRepInfo *dbinfo);
static void create_subscription(PGconn *conn, LogicalRepInfo *dbinfo);
static void drop_subscription(PGconn *conn, LogicalRepInfo *dbinfo);
static void set_replication_progress(PGconn *conn, LogicalRepInfo *dbinfo, const char *lsn);
static void enable_subscription(PGconn *conn, LogicalRepInfo *dbinfo);

#define	USEC_PER_SEC	1000000
#define	WAIT_INTERVAL	1		/* 1 second */

/* Options */
const char *progname;
static char *subscriber_dir = NULL;
static char *pub_conninfo_str = NULL;
static char *sub_conninfo_str = NULL;
static SimpleStringList database_names = {NULL, NULL};
static int	verbose = 0;
static bool	success = false;

static LogicalRepInfo  *dbinfo;

static int		num_dbs = 0;

static char		temp_replslot[NAMEDATALEN];
static bool		made_temp_replslot = false;

char		pidfile[MAXPGPATH]; /* subscriber PID file */

enum WaitPMResult
{
	POSTMASTER_READY,
	POSTMASTER_STANDBY,
	POSTMASTER_STILL_STARTING,
	POSTMASTER_FAILED
};


/*
 * Cleanup objects that were created by pg_subscriber if there is an error.
 *
 * Replication slots, publications and subscriptions are created. Dependind on
 * the step it failed, it should remove the already created objects if it is
 * possible (sometimes it won't work due to a connection issue).
 */
static void
cleanup_objects_atexit(void)
{
	PGconn	*conn;
	int		i;

	if (success)
		return;

	for (i = 0; i < num_dbs; i++)
	{
		if (dbinfo[i].made_subscription)
		{
			conn = connect_database(dbinfo[i].subconninfo, true);
			if (conn != NULL)
			{
				drop_subscription(conn, &dbinfo[i]);
				disconnect_database(conn);
			}
		}

		if (dbinfo[i].made_publication || dbinfo[i].made_replslot)
		{
			conn = connect_database(dbinfo[i].pubconninfo, true);
			if (conn != NULL)
			{
				if (dbinfo[i].made_publication)
					drop_publication(conn, &dbinfo[i]);
				if (dbinfo[i].made_replslot)
					drop_replication_slot(conn, &dbinfo[i], NULL);
				disconnect_database(conn);
			}
		}
	}

	if (made_temp_replslot)
	{
		conn = connect_database(dbinfo[0].pubconninfo, true);
		drop_replication_slot(conn, &dbinfo[0], temp_replslot);
		disconnect_database(conn);
	}
}

static void
usage(void)
{
	printf(_("%s creates a new logical replica from a base backup or a standby server.\n\n"),
		   progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]...\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_(" -D, --pgdata=DATADIR                location for the subscriber data directory\n"));
	printf(_(" -P, --publisher-conninfo=CONNINFO   publisher connection string\n"));
	printf(_(" -S, --subscriber-conninfo=CONNINFO  subscriber connection string\n"));
	printf(_(" -d, --database=DBNAME               database to create a subscription\n"));
	printf(_(" -v, --verbose                       output verbose messages\n"));
	printf(_(" -V, --version                       output version information, then exit\n"));
	printf(_(" -?, --help                          show this help, then exit\n"));
	printf(_("\nReport bugs to <%s>.\n"), PACKAGE_BUGREPORT);
	printf(_("%s home page: <%s>\n"), PACKAGE_NAME, PACKAGE_URL);
}

/*
 * Validate a connection string. Returns a base connection string that is a
 * connection string without a database name plus a fallback application name.
 * Since we might process multiple databases, each database name will be
 * appended to this base connection string to provide a final connection string.
 * If the second argument (dbname) is not null, returns dbname if the provided
 * connection string contains it. If option --database is not provided, uses
 * dbname as the only database to setup the logical replica.
 * It is the caller's responsibility to free the returned connection string and
 * dbname.
 */
static char *
get_base_conninfo(char *conninfo, char *dbname, const char *noderole)
{
	PQExpBuffer buf = createPQExpBuffer();
	PQconninfoOption *conn_opts = NULL;
	PQconninfoOption *conn_opt;
	char	   *errmsg = NULL;
	char	   *ret;
	int			i;

	if (verbose)
		pg_log_info("validating connection string on %s", noderole);

	conn_opts = PQconninfoParse(conninfo, &errmsg);
	if (conn_opts == NULL)
	{
		pg_log_error("could not parse connection string: %s", errmsg);
		return NULL;
	}

	i = 0;
	for (conn_opt = conn_opts; conn_opt->keyword != NULL; conn_opt++)
	{
		if (strcmp(conn_opt->keyword, "dbname") == 0 && conn_opt->val != NULL)
		{
			if (dbname)
				dbname = pg_strdup(conn_opt->val);
			continue;
		}

		if (conn_opt->val != NULL && conn_opt->val[0] != '\0')
		{
			if (i > 0)
				appendPQExpBufferChar(buf, ' ');
			appendPQExpBuffer(buf, "%s=%s", conn_opt->keyword, conn_opt->val);
			i++;
		}
	}

	if (i > 0)
		appendPQExpBufferChar(buf, ' ');
	appendPQExpBuffer(buf, "fallback_application_name=%s", progname);

	ret = pg_strdup(buf->data);

	destroyPQExpBuffer(buf);
	PQconninfoFree(conn_opts);

	return ret;
}

/*
 * Is it a cluster directory? These are preliminary checks. It is far from
 * making an accurate check. If it is not a clone from the publisher, it will
 * eventually fail in a future step.
 */
static bool
check_data_directory(const char *datadir)
{
	struct stat statbuf;
	char		versionfile[MAXPGPATH];

	if (verbose)
		pg_log_info("checking if directory \"%s\" is a cluster data directory",
					datadir);

	if (stat(datadir, &statbuf) != 0)
	{
		if (errno == ENOENT)
			pg_log_error("data directory \"%s\" does not exist", datadir);
		else
			pg_log_error("could not access directory \"%s\": %s", datadir, strerror(errno));

		return false;
	}

	snprintf(versionfile, MAXPGPATH, "%s/PG_VERSION", datadir);
	if (stat(versionfile, &statbuf) != 0 && errno == ENOENT)
	{
		pg_log_error("directory \"%s\" is not a database cluster directory", datadir);
		return false;
	}

	return true;
}

/*
 * Append database name into a base connection string.
 *
 * dbname is the only parameter that changes so it is not included in the base
 * connection string. This function concatenates dbname to build a "real"
 * connection string.
 */
static char *
concat_conninfo_dbname(const char *conninfo, const char *dbname)
{
	PQExpBuffer buf = createPQExpBuffer();
	char	   *ret;

	Assert(conninfo != NULL);

	appendPQExpBufferStr(buf, conninfo);
	appendPQExpBuffer(buf, " dbname=%s", dbname);
	appendPQExpBufferStr(buf, " replication=database");

	ret = pg_strdup(buf->data);
	destroyPQExpBuffer(buf);

	return ret;
}

static PGconn *
connect_database(const char *conninfo, bool secure_search_path)
{
	PGconn	   *conn;

	conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		pg_log_error("connection to database failed: %s", PQerrorMessage(conn));
		return NULL;
	}

	/* secure search_path */
	if (secure_search_path)
	{
		PGresult   *res;

		res = PQexec(conn, ALWAYS_SECURE_SEARCH_PATH_SQL);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			pg_log_error("could not clear search_path: %s", PQresultErrorMessage(res));
			return NULL;
		}
		PQclear(res);
	}

	return conn;
}

static void
disconnect_database(PGconn *conn)
{
	Assert(conn != NULL);

	PQfinish(conn);
}

/*
 * Obtain the system identifier using the provided connection. It will be used
 * to compare if a data directory is a clone of another one.
 */
static char *
get_sysid_from_conn(const char *conninfo)
{
	PGconn	   *conn;
	PGresult   *res;
	char	   *repconninfo;
	char	   *sysid = NULL;

	if (verbose)
		pg_log_info("getting system identifier from publisher");

	repconninfo = psprintf("%s replication=database", conninfo);
	conn = connect_database(repconninfo, false);
	if (conn == NULL)
		exit(1);

	res = PQexec(conn, "IDENTIFY_SYSTEM");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		pg_log_error("could not send replication command \"%s\": %s",
					 "IDENTIFY_SYSTEM", PQresultErrorMessage(res));
		PQclear(res);
		return NULL;
	}
	if (PQntuples(res) != 1 || PQnfields(res) < 3)
	{
		pg_log_error("could not identify system: got %d rows and %d fields, expected %d rows and %d or more fields",
					 PQntuples(res), PQnfields(res), 1, 3);

		PQclear(res);
		return NULL;
	}

	sysid = pg_strdup(PQgetvalue(res, 0, 0));

	disconnect_database(conn);

	return sysid;
}

/*
 * Obtain the system identifier from control file. It will be used to compare
 * if a data directory is a clone of another one. This routine is used locally
 * and avoids a replication connection.
 */
static char *
get_control_from_datadir(const char *datadir)
{
	ControlFileData *cf;
	bool		crc_ok;
	char	   *sysid = pg_malloc(32);

	if (verbose)
		pg_log_info("getting system identifier from subscriber");

	cf = get_controlfile(datadir, &crc_ok);
	if (!crc_ok)
	{
		pg_log_error("control file appears to be corrupt");
		exit(1);
	}

	snprintf(sysid, 32, UINT64_FORMAT, cf->system_identifier);

	pfree(cf);

	return sysid;
}

/*
 * Create a logical replication slot and returns a consistent LSN. The returned
 * LSN might be used to catch up the subscriber up to the required point.
 *
 * XXX CreateReplicationSlot() is not used because it does not provide the one-row
 * result set that contains the consistent LSN.
 */
static char *
create_logical_replication_slot(PGconn *conn, LogicalRepInfo *dbinfo,
								const char *slot_name)
{
	PQExpBuffer str = createPQExpBuffer();
	PGresult   *res;
	char	   *lsn = NULL;

	Assert(conn != NULL);

	if (verbose)
		pg_log_info("creating the replication slot \"%s\" on database \"%s\"", slot_name, dbinfo->dbname);

	appendPQExpBuffer(str, "CREATE_REPLICATION_SLOT \"%s\"", slot_name);
	appendPQExpBufferStr(str, " LOGICAL \"pgoutput\" NOEXPORT_SNAPSHOT");

	if (verbose)
		pg_log_info("command is: %s", str->data);

	res = PQexec(conn, str->data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		pg_log_error("could not create replication slot \"%s\" on database \"%s\": %s", slot_name, dbinfo->dbname,
					 PQresultErrorMessage(res));
		return lsn;
	}

	/* for cleanup purposes */
	if (slot_name == NULL)
		dbinfo->made_replslot = true;
	else
		made_temp_replslot = true;

	lsn = pg_strdup(PQgetvalue(res, 0, 1));

	PQclear(res);
	destroyPQExpBuffer(str);

	return lsn;
}

static void
drop_replication_slot(PGconn *conn, LogicalRepInfo *dbinfo, const char *slot_name)
{
	PQExpBuffer str = createPQExpBuffer();
	PGresult   *res;

	Assert(conn != NULL);

	if (verbose)
		pg_log_info("dropping the replication slot \"%s\" on database \"%s\"", slot_name, dbinfo->dbname);

	appendPQExpBuffer(str, "DROP_REPLICATION_SLOT \"%s\"", slot_name);

	res = PQexec(conn, str->data);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_log_error("could not drop replication slot \"%s\" on database \"%s\": %s", slot_name, dbinfo->dbname,
					 PQerrorMessage(conn));

	PQclear(res);
	destroyPQExpBuffer(str);
}

/*
 * Reports a suitable message if pg_ctl fails.
 */
static void
pg_ctl_status(const char *pg_ctl_cmd, int rc, int action)
{
	if (rc != 0)
	{
		if (WIFEXITED(rc))
		{
			pg_log_error("pg_ctl failed with exit code %d", WEXITSTATUS(rc));
		}
		else if (WIFSIGNALED(rc))
		{
#if defined(WIN32)
			pg_log_error("pg_ctl was terminated by exception 0x%X", WTERMSIG(rc));
			fprintf(stderr,
					"See C include file \"ntstatus.h\" for a description of the hexadecimal value.\n");
#else
			pg_log_error("pg_ctl was terminated by signal %d: %s",
						 WTERMSIG(rc), pg_strsignal(WTERMSIG(rc)));
#endif
		}
		else
		{
			pg_log_error("pg_ctl exited with unrecognized status %d", rc);
		}

		fprintf(stderr, "The failed command was: %s\n", pg_ctl_cmd);
		exit(1);
	}

	if (verbose)
	{
		if (action)
			pg_log_info("postmaster was started");
		else
			pg_log_info("postmaster was stopped");
	}
}

/*
 * XXX This function was copied from pg_ctl.c.
 *
 * We should probably move it to a common place.
 */
static bool
postmaster_is_alive(pid_t pid)
{
	/*
	 * Test to see if the process is still there.  Note that we do not
	 * consider an EPERM failure to mean that the process is still there;
	 * EPERM must mean that the given PID belongs to some other userid, and
	 * considering the permissions on $PGDATA, that means it's not the
	 * postmaster we are after.
	 *
	 * Don't believe that our own PID or parent shell's PID is the postmaster,
	 * either.  (Windows hasn't got getppid(), though.)
	 */
	if (pid == getpid())
		return false;
#ifndef WIN32
	if (pid == getppid())
		return false;
#endif
	if (kill(pid, 0) == 0)
		return true;
	return false;
}

/*
 * Returns after postmaster is accepting connections.
 */
static void
wait_postmaster_connection(const char *conninfo)
{
	PGPing		ret;
	long		pmpid;
	int			status = POSTMASTER_STILL_STARTING;

	if (verbose)
		pg_log_info("waiting for the postmaster to allow connections ...");

	/*
	 * Wait postmaster to come up. XXX this code path is a modified version of
	 * wait_for_postmaster().
	 */
	for (;;)
	{
		char	  **optlines;
		int			numlines;

		if ((optlines = readfile(pidfile, &numlines)) != NULL &&
			numlines >= LOCK_FILE_LINE_PM_STATUS)
		{
			/*
			 * Check the status line (this assumes a v10 or later server).
			 */
			char	   *pmstatus = optlines[LOCK_FILE_LINE_PM_STATUS - 1];

			pmpid = atol(optlines[LOCK_FILE_LINE_PID - 1]);

			if (strcmp(pmstatus, PM_STATUS_READY) == 0)
			{
				free_readfile(optlines);
				status = POSTMASTER_READY;
				break;
			}
			else if (strcmp(pmstatus, PM_STATUS_STANDBY) == 0)
			{
				free_readfile(optlines);
				status = POSTMASTER_STANDBY;
				break;
			}
		}

		free_readfile(optlines);

		pg_usleep(WAIT_INTERVAL * USEC_PER_SEC);
	}

	if (verbose)
		pg_log_info("postmaster.pid is available");

	if (status == POSTMASTER_STILL_STARTING)
	{
		pg_log_error("server did not start in time");
		exit(1);
	}
	else if (status == POSTMASTER_STANDBY)
	{
		pg_log_error("server is running but hot standby mode is not enabled");
		exit(1);
	}
	else if (status == POSTMASTER_FAILED)
	{
		pg_log_error("could not start server");
		fprintf(stderr, "Examine the log output.\n");
		exit(1);
	}

	if (verbose)
	{
		pg_log_info("postmaster is up and running");
		pg_log_info("waiting until the postmaster accepts connections ...");
	}

	/* Postmaster is up. Let's wait for it to accept connections. */
	for (;;)
	{
		ret = PQping(conninfo);
		if (ret == PQPING_OK)
			break;
		else if (ret == PQPING_NO_ATTEMPT)
			break;

		/*
		 * Postmaster started but for some reason it crashed leaving a
		 * postmaster.pid.
		 */
		if (!postmaster_is_alive((pid_t) pmpid))
		{
			pg_log_error("could not start server");
			fprintf(stderr, "Examine the log output.\n");
			exit(1);
		}

		pg_usleep(WAIT_INTERVAL * USEC_PER_SEC);
	}

	if (verbose)
		pg_log_info("postmaster is accepting connections");
}

/*
 * Returns after the server finishes the recovery process.
 */
static void
wait_for_end_recovery(const char *conninfo)
{
	PGconn	   *conn;
	PGresult   *res;
	int			status = POSTMASTER_STILL_STARTING;

	if (verbose)
		pg_log_info("waiting the postmaster to reach the consistent state ...");

	conn = connect_database(conninfo, true);
	if (conn == NULL)
		exit(1);

	for (;;)
	{
		bool		in_recovery;

		res = PQexec(conn, "SELECT pg_catalog.pg_is_in_recovery()");

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			pg_log_error("could not obtain recovery progress");
			exit(1);
		}

		if (PQntuples(res) != 1)
		{
			pg_log_error("unexpected result from pg_is_in_recovery function");
			exit(1);
		}

		in_recovery = (strcmp(PQgetvalue(res, 0, 0), "t") == 0);

		PQclear(res);

		/* Does the recovery process finish? */
		if (!in_recovery)
		{
			status = POSTMASTER_READY;
			break;
		}

		/* Keep waiting. */
		pg_usleep(WAIT_INTERVAL * USEC_PER_SEC);
	}

	disconnect_database(conn);

	if (status == POSTMASTER_STILL_STARTING)
	{
		pg_log_error("server did not end recovery");
		exit(1);
	}

	if (verbose)
		pg_log_info("postmaster reached the consistent state");
}

/*
 * Create a publication that includes all tables in the database.
 */
static void
create_publication(PGconn *conn, LogicalRepInfo *dbinfo)
{
	PQExpBuffer str = createPQExpBuffer();
	PGresult   *res;

	Assert(conn != NULL);

	/* Check if the publication needs to be created. */
	appendPQExpBuffer(str,
					  "SELECT puballtables FROM pg_catalog.pg_publication WHERE pubname = '%s'",
					  dbinfo->pubname);
	res = PQexec(conn, str->data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		pg_log_error("could not obtain publication information: %s",
					 PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(conn);
		exit(1);
	}

	if (PQntuples(res) == 1)
	{
		/*
		 * If publication name already exists and puballtables is true, let's
		 * use it. A previous run of pg_subscriber must have created this
		 * publication. Bail out.
		 */
		if (strcmp(PQgetvalue(res, 0, 0), "t") == 0)
		{
			if (verbose)
				pg_log_info("publication \"%s\" already exists", dbinfo->pubname);
			return;
		}
		else
		{
			/*
			 * XXX Unfortunately, if it reaches this code path, pg_subscriber
			 * will always fail here. That's bad but it is not expected that
			 * the user choose a name with pg_subscriber_ prefix followed by
			 * the exact database oid in which puballtables is false.
			 */
			pg_log_error("publication \"%s\" does not replicate changes for all tables",
						 dbinfo->pubname);
			PQclear(res);
			PQfinish(conn);
			exit(1);
		}
	}

	PQclear(res);
	resetPQExpBuffer(str);

	if (verbose)
		pg_log_info("creating publication \"%s\" on database \"%s\"", dbinfo->pubname, dbinfo->dbname);

	appendPQExpBuffer(str, "CREATE PUBLICATION %s FOR ALL TABLES", dbinfo->pubname);

	if (verbose)
		pg_log_info("command is: %s", str->data);

	res = PQexec(conn, str->data);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		pg_log_error("could not create publication \"%s\" on database \"%s\": %s",
					 dbinfo->pubname, dbinfo->dbname, PQerrorMessage(conn));
		PQfinish(conn);
		exit(1);
	}

	/* for cleanup purposes */
	dbinfo->made_publication = true;

	PQclear(res);
	destroyPQExpBuffer(str);
}

/*
 * Remove publication if it couldn't finish all steps.
 */
static void
drop_publication(PGconn *conn, LogicalRepInfo *dbinfo)
{
	PQExpBuffer str = createPQExpBuffer();
	PGresult   *res;

	Assert(conn != NULL);

	if (verbose)
		pg_log_info("dropping publication \"%s\" on database \"%s\"", dbinfo->pubname, dbinfo->dbname);

	appendPQExpBuffer(str, "DROP PUBLICATION %s", dbinfo->pubname);

	if (verbose)
		pg_log_info("command is: %s", str->data);

	res = PQexec(conn, str->data);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_log_error("could not drop publication \"%s\" on database \"%s\": %s", dbinfo->pubname, dbinfo->dbname, PQerrorMessage(conn));

	PQclear(res);
	destroyPQExpBuffer(str);
}

/*
 * Create a subscription with some predefined options.
 *
 * A replication slot was already created in a previous step. Let's use it. By
 * default, the subscription name is used as replication slot name. It is
 * not required to copy data. The subscription will be created but it will not
 * be enabled now. That's because the replication progress must be set and the
 * replication origin name (one of the function arguments) contains the
 * subscription OID in its name. Once the subscription is created,
 * set_replication_progress() can obtain the chosen origin name and set up its
 * initial location.
 */
static void
create_subscription(PGconn *conn, LogicalRepInfo *dbinfo)
{
	PQExpBuffer str = createPQExpBuffer();
	PGresult   *res;

	Assert(conn != NULL);

	if (verbose)
		pg_log_info("creating subscription \"%s\" on database \"%s\"", dbinfo->subname, dbinfo->dbname);

	appendPQExpBuffer(str,
					  "CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION %s "
					  "WITH (create_slot = false, copy_data = false, enabled = false)",
					  dbinfo->subname, dbinfo->pubconninfo, dbinfo->pubname);

	if (verbose)
		pg_log_info("command is: %s", str->data);

	res = PQexec(conn, str->data);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		pg_log_error("could not create subscription \"%s\" on database \"%s\": %s",
					 dbinfo->subname, dbinfo->dbname, PQerrorMessage(conn));
		PQfinish(conn);
		exit(1);
	}

	/* for cleanup purposes */
	dbinfo->made_subscription = true;

	PQclear(res);
	destroyPQExpBuffer(str);
}

/*
 * Remove subscription if it couldn't finish all steps.
 */
static void
drop_subscription(PGconn *conn, LogicalRepInfo *dbinfo)
{
	PQExpBuffer str = createPQExpBuffer();
	PGresult   *res;

	Assert(conn != NULL);

	if (verbose)
		pg_log_info("dropping subscription \"%s\" on database \"%s\"", dbinfo->subname, dbinfo->dbname);

	appendPQExpBuffer(str, "DROP SUBSCRIPTION %s", dbinfo->subname);

	if (verbose)
		pg_log_info("command is: %s", str->data);

	res = PQexec(conn, str->data);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_log_error("could not drop subscription \"%s\" on database \"%s\": %s", dbinfo->subname, dbinfo->dbname, PQerrorMessage(conn));

	PQclear(res);
	destroyPQExpBuffer(str);
}

/*
 * Sets the replication progress to the consistent LSN.
 *
 * The subscriber caught up to the consistent LSN provided by the temporary
 * replication slot. The goal is to set up the initial location for the logical
 * replication that is the exact LSN that the subscriber was promoted. Once the
 * subscription is enabled it will start streaming from that location onwards.
 */
static void
set_replication_progress(PGconn *conn, LogicalRepInfo *dbinfo, const char *lsn)
{
	PQExpBuffer str = createPQExpBuffer();
	PGresult   *res;
	Oid			suboid;
	char		originname[NAMEDATALEN];

	Assert(conn != NULL);

	appendPQExpBuffer(str,
					  "SELECT oid FROM pg_catalog.pg_subscription WHERE subname = '%s'", dbinfo->subname);

	res = PQexec(conn, str->data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		pg_log_error("could not obtain subscription OID: %s",
					 PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(conn);
		exit(1);
	}

	if (PQntuples(res) != 1)
	{
		pg_log_error("could not obtain subscription OID: got %d rows, expected %d rows",
					 PQntuples(res), 1);
		PQclear(res);
		PQfinish(conn);
		exit(1);
	}

	/*
	 * The origin name is defined as pg_%u. %u is the subscription OID. See
	 * ApplyWorkerMain().
	 */
	suboid = strtoul(PQgetvalue(res, 0, 0), NULL, 10);
	snprintf(originname, sizeof(originname), "pg_%u", suboid);

	PQclear(res);

	if (verbose)
		pg_log_info("setting the replication progress (node name \"%s\" ; LSN %s) on database \"%s\"",
					originname, lsn, dbinfo->dbname);

	resetPQExpBuffer(str);
	appendPQExpBuffer(str,
					  "SELECT pg_catalog.pg_replication_origin_advance('%s', '%s')", originname, lsn);

	if (verbose)
		pg_log_info("command is: %s", str->data);

	res = PQexec(conn, str->data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		pg_log_error("could not set replication progress for the subscription \"%s\": %s",
					 dbinfo->subname, PQresultErrorMessage(res));
		PQfinish(conn);
		exit(1);
	}

	PQclear(res);
	destroyPQExpBuffer(str);
}

/*
 * Enables the subscription.
 *
 * The subscription was created in a previous step but it was disabled. After
 * adjusting the initial location, enabling the subscription is the last step
 * of this setup.
 */
static void
enable_subscription(PGconn *conn, LogicalRepInfo *dbinfo)
{
	PQExpBuffer str = createPQExpBuffer();
	PGresult   *res;

	Assert(conn != NULL);

	if (verbose)
		pg_log_info("enabling subscription \"%s\" on database \"%s\"", dbinfo->subname, dbinfo->dbname);

	appendPQExpBuffer(str, "ALTER SUBSCRIPTION %s ENABLE", dbinfo->subname);

	if (verbose)
		pg_log_info("command is: %s", str->data);

	res = PQexec(conn, str->data);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		pg_log_error("could not enable subscription \"%s\": %s", dbinfo->subname,
					 PQerrorMessage(conn));
		PQfinish(conn);
		exit(1);
	}

	PQclear(res);
	destroyPQExpBuffer(str);
}

int
main(int argc, char **argv)
{
	static struct option long_options[] =
	{
		{"help", no_argument, NULL, '?'},
		{"version", no_argument, NULL, 'V'},
		{"pgdata", required_argument, NULL, 'D'},
		{"publisher-conninfo", required_argument, NULL, 'P'},
		{"subscriber-conninfo", required_argument, NULL, 'S'},
		{"database", required_argument, NULL, 'd'},
		{"verbose", no_argument, NULL, 'v'},
		{"stop-subscriber", no_argument, NULL, 1},
		{NULL, 0, NULL, 0}
	};

	int			c;
	int			option_index;

	char	   *pg_ctl_path;
	char	   *pg_ctl_cmd;
	int			rc;

	SimpleStringListCell *cell;

	char	   *pub_base_conninfo = NULL;
	char	   *sub_base_conninfo = NULL;
	char	   *dbname_conninfo;

	char	   *pub_sysid;
	char	   *sub_sysid;
	struct stat statbuf;

	PGconn	   *conn;
	char	   *consistent_lsn;

	PQExpBuffer recoveryconfcontents = NULL;

	int			i;

	pg_logging_init(argv[0]);
	progname = get_progname(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_subscriber"));

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage();
			exit(0);
		}
		else if (strcmp(argv[1], "-V") == 0
				 || strcmp(argv[1], "--version") == 0)
		{
			puts("pg_subscriber (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	atexit(cleanup_objects_atexit);

	/*
	 * Don't allow it to be run as root. It uses pg_ctl which does not allow
	 * it either.
	 */
#ifndef WIN32
	if (geteuid() == 0)
	{
		pg_log_error("cannot be executed by \"root\"");
		fprintf(stderr, _("You must run %s as the PostgreSQL superuser.\n"),
				progname);
		exit(1);
	}
#endif

	while ((c = getopt_long(argc, argv, "D:P:S:d:t:v",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'D':
				subscriber_dir = pg_strdup(optarg);
				break;
			case 'P':
				pub_conninfo_str = pg_strdup(optarg);
				break;
			case 'S':
				sub_conninfo_str = pg_strdup(optarg);
				break;
			case 'd':
				simple_string_list_append(&database_names, optarg);
				num_dbs++;
				break;
			case 'v':
				verbose++;
				break;
			default:

				/*
				 * getopt_long already emitted a complaint
				 */
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
						progname);
				exit(1);
		}
	}

	/*
	 * Any non-option arguments?
	 */
	if (optind < argc)
	{
		pg_log_error("too many command-line arguments (first is \"%s\")",
					 argv[optind]);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	/*
	 * Required arguments
	 */
	if (subscriber_dir == NULL)
	{
		pg_log_error("no subscriber data directory specified");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	/*
	 * Parse connection string. Build a base connection string that might be
	 * reused by multiple databases.
	 */
	if (pub_conninfo_str == NULL)
	{
		/*
		 * FIXME use primary_conninfo (if available) from subscriber and
		 * extract publisher connection string. Assume that there are
		 * identical entries for physical and logical replication. If there is
		 * not, we would fail anyway.
		 */
		pg_log_error("no publisher connection string specified");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}
	dbname_conninfo = pg_malloc(NAMEDATALEN);
	pub_base_conninfo = get_base_conninfo(pub_conninfo_str, dbname_conninfo,
										  "publisher");
	if (pub_base_conninfo == NULL)
		exit(1);

	if (sub_conninfo_str == NULL)
	{
		pg_log_error("no subscriber connection string specified");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}
	sub_base_conninfo = get_base_conninfo(sub_conninfo_str, NULL, "subscriber");
	if (sub_base_conninfo == NULL)
		exit(1);

	if (database_names.head == NULL)
	{
		if (verbose)
			pg_log_info("no database was specified");

		/*
		 * If --database option is not provided, try to obtain the dbname from
		 * the publisher conninfo. If dbname parameter is not available, error
		 * out.
		 */
		if (dbname_conninfo)
		{
			simple_string_list_append(&database_names, dbname_conninfo);
			num_dbs++;

			if (verbose)
				pg_log_info("database \"%s\" was extracted from the publisher connection string",
							dbname_conninfo);
		}
		else
		{
			pg_log_error("no database name specified");
			fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
					progname);
			exit(1);
		}
	}

	/*
	 * Get the absolute pg_ctl path on the subscriber.
	 */
	pg_ctl_path = pg_malloc(MAXPGPATH);
	rc = find_other_exec(argv[0], "pg_ctl",
						 "pg_ctl (PostgreSQL) " PG_VERSION "\n",
						 pg_ctl_path);
	if (rc < 0)
	{
		char		full_path[MAXPGPATH];

		if (find_my_exec(argv[0], full_path) < 0)
			strlcpy(full_path, progname, sizeof(full_path));
		if (rc == -1)
			pg_log_error("The program \"%s\" is needed by %s but was not found in the\n"
						 "same directory as \"%s\".\n"
						 "Check your installation.",
						 "pg_ctl", progname, full_path);
		else
			pg_log_error("The program \"%s\" was found by \"%s\"\n"
						 "but was not the same version as %s.\n"
						 "Check your installation.",
						 "pg_ctl", full_path, progname);
		exit(1);
	}

	if (verbose)
		pg_log_info("pg_ctl path is: %s", pg_ctl_path);

	/* rudimentary check for a data directory. */
	if (!check_data_directory(subscriber_dir))
		exit(1);

	/* subscriber PID file. */
	snprintf(pidfile, MAXPGPATH, "%s/postmaster.pid", subscriber_dir);

	/* Store database information for publisher and subscriber. */
	dbinfo = (LogicalRepInfo *) pg_malloc(num_dbs * sizeof(LogicalRepInfo));
	i = 0;
	for (cell = database_names.head; cell; cell = cell->next)
	{
		char	   *conninfo;

		/* Publisher. */
		conninfo = concat_conninfo_dbname(pub_base_conninfo, cell->val);
		dbinfo[i].pubconninfo = conninfo;
		dbinfo[i].dbname = cell->val;
		dbinfo[i].made_replslot = false;
		dbinfo[i].made_publication = false;
		dbinfo[i].made_subscription = false;
		/* other struct fields will be filled later. */

		/* Subscriber. */
		conninfo = concat_conninfo_dbname(sub_base_conninfo, cell->val);
		dbinfo[i].subconninfo = conninfo;

		i++;
	}

	/*
	 * Check if the subscriber data directory has the same system identifier
	 * than the publisher data directory.
	 */
	pub_sysid = pg_malloc(32);
	pub_sysid = get_sysid_from_conn(dbinfo[0].pubconninfo);
	sub_sysid = pg_malloc(32);
	sub_sysid = get_control_from_datadir(subscriber_dir);
	if (strcmp(pub_sysid, sub_sysid) != 0)
	{
		pg_log_error("subscriber data directory is not a base backup from the publisher");
		exit(1);
	}

	/*
	 * Stop the subscriber if it is a standby server. Before executing the
	 * transformation steps, make sure the subscriber is not running because
	 * one of the steps is to modify some recovery parameters that require a
	 * restart.
	 */
	if (stat(pidfile, &statbuf) == 0)
	{
		if (verbose)
		{
			pg_log_info("subscriber is up and running");
			pg_log_info("stopping the server to start the transformation steps");
		}

		pg_ctl_cmd = psprintf("\"%s\" stop -D \"%s\" -s", pg_ctl_path, subscriber_dir);
		rc = system(pg_ctl_cmd);
		pg_ctl_status(pg_ctl_cmd, rc, 0);
	}

	/*
	 * Create a replication slot for each database on the publisher.
	 */
	for (i = 0; i < num_dbs; i++)
	{
		PGresult   *res;
		char		replslotname[NAMEDATALEN];

		conn = connect_database(dbinfo[i].pubconninfo, true);
		if (conn == NULL)
			exit(1);

		res = PQexec(conn,
					 "SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database()");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			pg_log_error("could not obtain database OID: %s", PQresultErrorMessage(res));
			PQclear(res);
			PQfinish(conn);
			exit(1);
		}

		if (PQntuples(res) != 1)
		{
			pg_log_error("could not obtain database OID: got %d rows, expected %d rows",
						 PQntuples(res), 1);
			PQclear(res);
			PQfinish(conn);
			exit(1);
		}

		/* Remember database OID. */
		dbinfo[i].oid = strtoul(PQgetvalue(res, 0, 0), NULL, 10);

		PQclear(res);

		/*
		 * Build the replication slot name. The name must not exceed
		 * NAMEDATALEN - 1. This current schema uses a maximum of 36
		 * characters (14 + 10 + 1 + 10 + '\0'). System identifier is included
		 * to reduce the probability of collision. By default, subscription
		 * name is used as replication slot name.
		 */
		snprintf(replslotname, sizeof(replslotname),
				 "pg_subscriber_%u_%d",
				 dbinfo[i].oid,
				 (int) getpid());
		dbinfo[i].subname = pg_strdup(replslotname);

		/* Create replication slot on publisher. */
		if (create_logical_replication_slot(conn, &dbinfo[i], replslotname) != NULL)
			pg_log_info("create replication slot \"%s\" on publisher", replslotname);
		else
			exit(1);

		disconnect_database(conn);
	}

	/*
	 * Create a temporary logical replication slot to get a consistent LSN.
	 *
	 * This consistent LSN will be used later to advanced the recently created
	 * replication slots. We could probably use the last created replication
	 * slot, however, if this tool decides to support cloning the publisher
	 * (via pg_basebackup -- after creating the replication slots), the
	 * consistent point should be after the pg_basebackup finishes.
	 */
	conn = connect_database(dbinfo[0].pubconninfo, false);
	if (conn == NULL)
		exit(1);
	snprintf(temp_replslot, sizeof(temp_replslot), "pg_subscriber_%d_tmp",
			 (int) getpid());
	consistent_lsn = create_logical_replication_slot(conn, &dbinfo[0],
													 temp_replslot);

	/*
	 * Write recovery parameters.
	 *
	 * Despite of the recovery parameters will be written to the subscriber,
	 * use a publisher connection for the follwing recovery functions. The
	 * connection is only used to check the current server version (physical
	 * replica, same server version). The subscriber is not running yet.
	 */
	recoveryconfcontents = GenerateRecoveryConfig(conn, NULL);
	appendPQExpBuffer(recoveryconfcontents, "recovery_target_lsn = '%s'\n",
					  consistent_lsn);
	appendPQExpBuffer(recoveryconfcontents, "recovery_target_inclusive = true\n");
	appendPQExpBuffer(recoveryconfcontents, "recovery_target_action = promote\n");

	WriteRecoveryConfig(conn, subscriber_dir, recoveryconfcontents);
	disconnect_database(conn);

	/*
	 * Start subscriber and wait until accepting connections.
	 */
	if (verbose)
		pg_log_info("starting the subscriber");

	pg_ctl_cmd = psprintf("\"%s\" start -D \"%s\" -s", pg_ctl_path, subscriber_dir);
	rc = system(pg_ctl_cmd);
	pg_ctl_status(pg_ctl_cmd, rc, 1);
	wait_postmaster_connection(dbinfo[0].subconninfo);

	/*
	 * Waiting the subscriber to be promoted.
	 */
	wait_for_end_recovery(dbinfo[0].subconninfo);

	/*
	 * Create a publication for each database. This step should be executed
	 * after promoting the subscriber to avoid replicating unnecessary
	 * objects.
	 */
	for (i = 0; i < num_dbs; i++)
	{
		char		pubname[NAMEDATALEN];

		/* Connect to publisher. */
		conn = connect_database(dbinfo[i].pubconninfo, true);
		if (conn == NULL)
			exit(1);

		/*
		 * Build the publication name. The name must not exceed NAMEDATALEN -
		 * 1. This current schema uses a maximum of 35 characters (14 + 10 +
		 * '\0').
		 */
		snprintf(pubname, sizeof(pubname), "pg_subscriber_%u", dbinfo[i].oid);
		dbinfo[i].pubname = pg_strdup(pubname);

		create_publication(conn, &dbinfo[i]);

		disconnect_database(conn);
	}

	/*
	 * Create a subscription for each database.
	 */
	for (i = 0; i < num_dbs; i++)
	{
		/* Connect to subscriber. */
		conn = connect_database(dbinfo[i].subconninfo, true);
		if (conn == NULL)
			exit(1);

		create_subscription(conn, &dbinfo[i]);

		/* Set the replication progress to the correct LSN. */
		set_replication_progress(conn, &dbinfo[i], consistent_lsn);

		/* Enable subscription. */
		enable_subscription(conn, &dbinfo[i]);

		disconnect_database(conn);
	}

	/*
	 * The temporary replication slot is no longer required. Drop it.
	 * XXX we might not fail here. Instead, provide a warning so the user
	 * XXX eventually drops the replication slot later.
	 */
	conn = connect_database(dbinfo[0].pubconninfo, true);
	if (conn == NULL)
		exit(1);
	drop_replication_slot(conn, &dbinfo[0], temp_replslot);
	disconnect_database(conn);

	/*
	 * Stop the subscriber.
	 */
	if (verbose)
		pg_log_info("stopping the subscriber");

	pg_ctl_cmd = psprintf("\"%s\" stop -D \"%s\" -s", pg_ctl_path, subscriber_dir);
	rc = system(pg_ctl_cmd);
	pg_ctl_status(pg_ctl_cmd, rc, 0);

	success = true;

	return 0;
}
