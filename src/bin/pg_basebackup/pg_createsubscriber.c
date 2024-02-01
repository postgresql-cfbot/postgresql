/*-------------------------------------------------------------------------
 *
 * pg_createsubscriber.c
 *	  Create a new logical replica from a standby server
 *
 * Copyright (C) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/bin/pg_basebackup/pg_createsubscriber.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <signal.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <time.h>

#include "access/xlogdefs.h"
#include "catalog/pg_control.h"
#include "common/connect.h"
#include "common/controldata_utils.h"
#include "common/file_perm.h"
#include "common/file_utils.h"
#include "common/logging.h"
#include "fe_utils/recovery_gen.h"
#include "fe_utils/simple_list.h"
#include "getopt_long.h"
#include "port.h"
#include "utils/pidfile.h"

#define	PGS_OUTPUT_DIR	"pg_createsubscriber_output.d"

typedef struct LogicalRepPerdbInfo
{
	Oid		oid;
	char   *dbname;
	bool	made_replslot;		/* replication slot was created */
	bool	made_publication;	/* publication was created */
	bool	made_subscription; 	/* subscription was created */
} LogicalRepPerdbInfo;

typedef struct LogicalRepPerdbInfoArr
{
	LogicalRepPerdbInfo    *perdb;	/* array of db infos */
	int						ndbs;	/* number of db infos */
} LogicalRepPerdbInfoArr;

typedef struct PrimaryInfo
{
	char   *base_conninfo;
	uint64	sysid;
	bool	made_transient_replslot;
} PrimaryInfo;

typedef struct StandbyInfo
{
	char   *base_conninfo;
	char   *bindir;
	char   *pgdata;
	char   *primary_slot_name;
	char   *server_log;
	uint64	sysid;
} StandbyInfo;

static void cleanup_objects_atexit(void);
static void usage();
static bool get_exec_base_path(const char *path);
static bool check_data_directory(const char *datadir);
static char *get_primary_conninfo(StandbyInfo *standby);
static char *concat_conninfo_dbname(const char *conninfo, const char *dbname);
static void store_db_names(LogicalRepPerdbInfo **perdb, int ndbs);
static PGconn *connect_database(const char *base_conninfo, const char*dbname);
static void disconnect_database(PGconn *conn);
static void get_sysid_for_primary(PrimaryInfo *primary, char *dbname);
static void get_sysid_for_standby(StandbyInfo *standby);
static void modify_sysid(const char *bindir, const char *datadir);
static bool check_publisher(PrimaryInfo *primary, LogicalRepPerdbInfoArr *dbarr);
static bool setup_publisher(PrimaryInfo *primary, LogicalRepPerdbInfoArr *dbarr);
static bool check_subscriber(StandbyInfo *standby, LogicalRepPerdbInfoArr *dbarr);
static bool setup_subscriber(StandbyInfo *standby, PrimaryInfo *primary,
							 LogicalRepPerdbInfoArr *dbarr,
							 const char *consistent_lsn);
static char *create_logical_replication_slot(PGconn *conn, bool temporary,
											 LogicalRepPerdbInfo *perdb);
static void drop_replication_slot(PGconn *conn, LogicalRepPerdbInfo *perdb,
								  const char *slot_name);
static char *server_logfile_name(const char *datadir);
static void start_standby_server(StandbyInfo *standby);
static void stop_standby_server(StandbyInfo *standby);
static void pg_ctl_status(const char *pg_ctl_cmd, int rc, int action);


static void wait_for_end_recovery(StandbyInfo *standby,
								  const char *dbname);
static void create_publication(PGconn *conn, PrimaryInfo *primary,
							   LogicalRepPerdbInfo *perdb);
static void drop_publication(PGconn *conn, LogicalRepPerdbInfo *perdb);
static void create_subscription(PGconn *conn, StandbyInfo *standby,
								PrimaryInfo *primary,
								LogicalRepPerdbInfo *perdb);
static void drop_subscription(PGconn *conn, LogicalRepPerdbInfo *perdb);
static void set_replication_progress(PGconn *conn, LogicalRepPerdbInfo *perdb,
									 const char *lsn);
static void enable_subscription(PGconn *conn, LogicalRepPerdbInfo *perdb);

#define	USEC_PER_SEC	1000000
#define	WAIT_INTERVAL	1		/* 1 second */

/* Options */
static const char *progname;

static char *sub_conninfo_str = NULL;
static SimpleStringList database_names = {NULL, NULL};
static bool dry_run = false;
static bool retain = false;
static int	recovery_timeout = 0;

static bool success = false;

static LogicalRepPerdbInfoArr dbarr;
static PrimaryInfo primary;
static StandbyInfo standby;

enum WaitPMResult
{
	POSTMASTER_READY,
	POSTMASTER_STANDBY,
	POSTMASTER_STILL_STARTING,
	POSTMASTER_FAILED
};

/*
 * Build the replication slot name. The name must not exceed
 * NAMEDATALEN - 1. This current schema uses a maximum of 42
 * characters (20 + 10 + 1 + 10 + '\0'). PID is included to reduce the
 * probability of collision. By default, subscription name is used as
 * replication slot name.
 */
static inline void
get_subscription_name(Oid oid, int pid, char *subname, Size szsub)
{
	snprintf(subname, szsub, "pg_createsubscriber_%u_%d", oid, pid);
}

/*
 * Build the publication name. The name must not exceed NAMEDATALEN -
 * 1. This current schema uses a maximum of 31 characters (20 + 10 +
 * '\0').
 */
static inline void
get_publication_name(Oid oid, char *pubname, Size szpub)
{
	snprintf(pubname, szpub, "pg_createsubscriber_%u", oid);
}


/*
 * Cleanup objects that were created by pg_createsubscriber if there is an error.
 *
 * Replication slots, publications and subscriptions are created. Depending on
 * the step it failed, it should remove the already created objects if it is
 * possible (sometimes it won't work due to a connection issue).
 */
static void
cleanup_objects_atexit(void)
{
	PGconn	   *conn;
	int			i;

	if (success)
		return;

	for (i = 0; i < dbarr.ndbs; i++)
	{
		LogicalRepPerdbInfo *perdb = &dbarr.perdb[i];

		if (perdb->made_subscription)
		{

			conn = connect_database(standby.base_conninfo, perdb->dbname);
			if (conn != NULL)
			{
				drop_subscription(conn, perdb);

				if (perdb->made_publication)
					drop_publication(conn, perdb);
				disconnect_database(conn);
			}
		}

		if (perdb->made_publication || perdb->made_replslot)
		{
			if (perdb->made_publication)
				drop_publication(conn, perdb);
			if (perdb->made_replslot)
			{
				char replslotname[NAMEDATALEN];

				get_subscription_name(perdb->oid, (int) getpid(),
									  replslotname, NAMEDATALEN);
				drop_replication_slot(conn, perdb, replslotname);
			}
		}
	}
}

static void
usage(void)
{
	printf(_("%s creates a new logical replica from a standby server.\n\n"),
		   progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]...\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_(" -D, --pgdata=DATADIR                location for the subscriber data directory\n"));
	printf(_(" -S, --subscriber-server=CONNSTR     subscriber connection string\n"));
	printf(_(" -d, --database=DBNAME               database to create a subscription\n"));
	printf(_(" -n, --dry-run                       stop before modifying anything\n"));
	printf(_(" -t, --recovery-timeout=SECS         seconds to wait for recovery to end\n"));
	printf(_(" -r, --retain                        retain log file after success\n"));
	printf(_(" -v, --verbose                       output verbose messages\n"));
	printf(_(" -V, --version                       output version information, then exit\n"));
	printf(_(" -?, --help                          show this help, then exit\n"));
	printf(_("\nReport bugs to <%s>.\n"), PACKAGE_BUGREPORT);
	printf(_("%s home page: <%s>\n"), PACKAGE_NAME, PACKAGE_URL);
}

/*
 * Validate a connection string. Returns a base connection string that is a
 * connection string without a database name.
 * Since we might process multiple databases, each database name will be
 * appended to this base connection string to provide a final connection string.
 * If the second argument (dbname) is not null, returns dbname if the provided
 * connection string contains it. If option --database is not provided, uses
 * dbname as the only database to setup the logical replica.
 * It is the caller's responsibility to free the returned connection string and
 * dbname.
 */
static char *
get_base_conninfo(char *conninfo, char *dbname)
{
	PQExpBuffer buf = createPQExpBuffer();
	PQconninfoOption *conn_opts = NULL;
	PQconninfoOption *conn_opt;
	char	   *errmsg = NULL;
	char	   *ret;
	int			i;

	pg_log_info("validating connection string on subscriber");

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

	ret = pg_strdup(buf->data);

	destroyPQExpBuffer(buf);
	PQconninfoFree(conn_opts);

	return ret;
}

/*
 * Get the absolute binary path from another PostgreSQL binary (pg_ctl) and set
 * to StandbyInfo.
 */
static bool
get_exec_base_path(const char *path)
{
	int		rc;
	char	pg_ctl_path[MAXPGPATH];
	char   *p;

	rc = find_other_exec(path, "pg_ctl",
						 "pg_ctl (PostgreSQL) " PG_VERSION "\n",
						 pg_ctl_path);
	if (rc < 0)
	{
		char		full_path[MAXPGPATH];

		if (find_my_exec(path, full_path) < 0)
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
		return false;
	}

	pg_log_debug("pg_ctl path is: %s", pg_ctl_path);

	/* Extract the directory part from the path */
	p = strrchr(pg_ctl_path, 'p');
	Assert(p);

	*p = '\0';
	standby.bindir = pg_strdup(pg_ctl_path);

	return true;
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

	ret = pg_strdup(buf->data);
	destroyPQExpBuffer(buf);

	return ret;
}

/*
 * Initialize per-db structure and store the name of databases.
 */
static void
store_db_names(LogicalRepPerdbInfo **perdb, int ndbs)
{
	SimpleStringListCell   *cell;
	int						i = 0;

	dbarr.perdb = (LogicalRepPerdbInfo *) pg_malloc0(ndbs *
											   sizeof(LogicalRepPerdbInfo));

	for (cell = database_names.head; cell; cell = cell->next)
	{
		(*perdb)[i].dbname = pg_strdup(cell->val);
		i++;
	}
}

static PGconn *
connect_database(const char *base_conninfo, const char*dbname)
{
	PGconn	   *conn;
	PGresult   *res;
	char	   *conninfo = concat_conninfo_dbname(base_conninfo,
														 dbname);

	conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		pg_log_error("connection to database failed: %s", PQerrorMessage(conn));
		return NULL;
	}

	/* secure search_path */
	res = PQexec(conn, ALWAYS_SECURE_SEARCH_PATH_SQL);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		pg_log_error("could not clear search_path: %s", PQresultErrorMessage(res));
		return NULL;
	}
	PQclear(res);

	pfree(conninfo);
	return conn;
}

static void
disconnect_database(PGconn *conn)
{
	Assert(conn != NULL);

	PQfinish(conn);
}

/*
 * Obtain primary_conninfo from the target server. The value would be used for
 * connecting from the pg_createsubscriber itself and logical replication apply
 * worker.
 */
static char *
get_primary_conninfo(StandbyInfo *standby)
{
	PGconn	   *conn;
	PGresult   *res;
	char	   *primaryconninfo;

	pg_log_info("getting primary_conninfo from standby");

	/*
	 * Construct a connection string to the target instance. Since dbinfo has
	 * not stored infomation yet, we must directly get the first element of the
	 * database list.
	 */
	conn = connect_database(standby->base_conninfo, database_names.head->val);
	if (conn == NULL)
		exit(1);

	res = PQexec(conn, "SHOW primary_conninfo;");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		pg_log_error("could not send command \"%s\": %s",
					 "SHOW primary_conninfo;", PQresultErrorMessage(res));
		PQclear(res);
		disconnect_database(conn);
		exit(1);
	}

	primaryconninfo = pg_strdup(PQgetvalue(res, 0, 0));

	if (strlen(primaryconninfo) == 0)
	{
		pg_log_error("primary_conninfo is empty");
		pg_log_error_hint("Check whether the target server is really a physical standby.");
		exit(1);
	}

	PQclear(res);
	disconnect_database(conn);

	return primaryconninfo;
}

/*
 * Obtain the system identifier from the primary server. It will be used to
 * compare if a data directory is a clone of another one.
 */
static void
get_sysid_for_primary(PrimaryInfo *primary, char *dbname)
{
	PGconn	   *conn;
	PGresult   *res;

	pg_log_info("getting system identifier from publisher");

	conn = connect_database(primary->base_conninfo, dbname);
	if (conn == NULL)
		exit(1);

	res = PQexec(conn, "SELECT * FROM pg_control_system();");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		pg_log_error("could not send command \"%s\": %s",
					 "IDENTIFY_SYSTEM", PQresultErrorMessage(res));
		PQclear(res);
		disconnect_database(conn);
		exit(1);
	}
	if (PQntuples(res) != 1 || PQnfields(res) < 4)
	{
		pg_log_error("could not identify system: got %d rows and %d fields, expected %d rows and %d or more fields",
					 PQntuples(res), PQnfields(res), 1, 4);

		PQclear(res);
		disconnect_database(conn);
		exit(1);
	}

	primary->sysid = strtou64(PQgetvalue(res, 0, 2), NULL, 10);

	pg_log_info("system identifier is %llu on publisher",
				(unsigned long long) primary->sysid);

	disconnect_database(conn);
}

/*
 * Obtain the system identifier from control file. It will be used to compare
 * if a data directory is a clone of another one. This routine is used locally
 * and avoids a connection establishment.
 */
static void
get_sysid_for_standby(StandbyInfo *standby)
{
	ControlFileData *cf;
	bool		crc_ok;

	pg_log_info("getting system identifier from subscriber");

	cf = get_controlfile(standby->pgdata, &crc_ok);
	if (!crc_ok)
	{
		pg_log_error("control file appears to be corrupt");
		exit(1);
	}

	standby->sysid = cf->system_identifier;

	pg_log_info("system identifier is %llu on subscriber", (unsigned long long) standby->sysid);

	pfree(cf);
}

/*
 * Modify the system identifier. Since a standby server preserves the system
 * identifier, it makes sense to change it to avoid situations in which WAL
 * files from one of the systems might be used in the other one.
 */
static void
modify_sysid(const char *bindir, const char *datadir)
{
	ControlFileData *cf;
	bool		crc_ok;
	struct timeval tv;

	char	   *cmd_str;
	int			rc;

	pg_log_info("modifying system identifier from subscriber");

	cf = get_controlfile(datadir, &crc_ok);
	if (!crc_ok)
	{
		pg_log_error("control file appears to be corrupt");
		exit(1);
	}

	/*
	 * Select a new system identifier.
	 *
	 * XXX this code was extracted from BootStrapXLOG().
	 */
	gettimeofday(&tv, NULL);
	cf->system_identifier = ((uint64) tv.tv_sec) << 32;
	cf->system_identifier |= ((uint64) tv.tv_usec) << 12;
	cf->system_identifier |= getpid() & 0xFFF;

	if (!dry_run)
		update_controlfile(datadir, cf, true);

	pg_log_info("system identifier is %llu on subscriber", (unsigned long long) cf->system_identifier);

	pg_log_info("running pg_resetwal on the subscriber");

	cmd_str = psprintf("\"%s/pg_resetwal\" -D \"%s\"", bindir, datadir);

	pg_log_debug("command is: %s", cmd_str);

	if (!dry_run)
	{
		rc = system(cmd_str);
		if (rc == 0)
			pg_log_info("subscriber successfully changed the system identifier");
		else
			pg_log_error("subscriber failed to change system identifier: exit code: %d", rc);
	}

	pfree(cf);
}

/*
 * Create the publications and replication slots in preparation for logical
 * replication.
 */
static bool
setup_publisher(PrimaryInfo *primary, LogicalRepPerdbInfoArr *dbarr)
{
	PGconn	   *conn;
	PGresult   *res;

	for (int i = 0; i < dbarr->ndbs; i++)
	{
		LogicalRepPerdbInfo *perdb = &dbarr->perdb[i];

		conn = connect_database(primary->base_conninfo, perdb->dbname);
		if (conn == NULL)
			exit(1);

		res = PQexec(conn,
					 "SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database()");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			pg_log_error("could not obtain database OID: %s", PQresultErrorMessage(res));
			return false;
		}

		if (PQntuples(res) != 1)
		{
			pg_log_error("could not obtain database OID: got %d rows, expected %d rows",
						 PQntuples(res), 1);
			return false;
		}

		/* Remember database OID. */
		perdb->oid = strtoul(PQgetvalue(res, 0, 0), NULL, 10);

		PQclear(res);

		/*
		 * Create publication on publisher. This step should be executed
		 * *before* promoting the subscriber to avoid any transactions between
		 * consistent LSN and the new publication rows (such transactions
		 * wouldn't see the new publication rows resulting in an error).
		 */
		create_publication(conn, primary, perdb);

		/* Create replication slot on publisher. */
		if (create_logical_replication_slot(conn, false, perdb) == NULL && !dry_run)
			return false;

		disconnect_database(conn);
	}

	return true;
}

/*
 * Is the primary server ready for logical replication?
 */
static bool
check_publisher(PrimaryInfo *primary, LogicalRepPerdbInfoArr *dbarr)
{
	PGconn	   *conn;
	PGresult   *res;
	PQExpBuffer str = createPQExpBuffer();

	char	   *wal_level;
	int			max_repslots;
	int			cur_repslots;
	int			max_walsenders;
	int			cur_walsenders;

	pg_log_info("checking settings on publisher");

	/*
	 * Logical replication requires a few parameters to be set on publisher.
	 * Since these parameters are not a requirement for physical replication,
	 * we should check it to make sure it won't fail.
	 *
	 * wal_level = logical
	 * max_replication_slots >= current + number of dbs to be converted
	 * max_wal_senders >= current + number of dbs to be converted
	 */
	conn = connect_database(primary->base_conninfo, dbarr->perdb[0].dbname);
	if (conn == NULL)
		exit(1);

	res = PQexec(conn,
				 "WITH wl AS (SELECT setting AS wallevel FROM pg_settings WHERE name = 'wal_level'),"
				 "     total_mrs AS (SELECT setting AS tmrs FROM pg_settings WHERE name = 'max_replication_slots'),"
				 "     cur_mrs AS (SELECT count(*) AS cmrs FROM pg_replication_slots),"
				 "     total_mws AS (SELECT setting AS tmws FROM pg_settings WHERE name = 'max_wal_senders'),"
				 "     cur_mws AS (SELECT count(*) AS cmws FROM pg_stat_activity WHERE backend_type = 'walsender')"
				 "SELECT wallevel, tmrs, cmrs, tmws, cmws FROM wl, total_mrs, cur_mrs, total_mws, cur_mws");

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		pg_log_error("could not obtain publisher settings: %s", PQresultErrorMessage(res));
		return false;
	}

	wal_level = strdup(PQgetvalue(res, 0, 0));
	max_repslots = atoi(PQgetvalue(res, 0, 1));
	cur_repslots = atoi(PQgetvalue(res, 0, 2));
	max_walsenders = atoi(PQgetvalue(res, 0, 3));
	cur_walsenders = atoi(PQgetvalue(res, 0, 4));

	PQclear(res);

	pg_log_debug("subscriber: wal_level: %s", wal_level);
	pg_log_debug("subscriber: max_replication_slots: %d", max_repslots);
	pg_log_debug("subscriber: current replication slots: %d", cur_repslots);
	pg_log_debug("subscriber: max_wal_senders: %d", max_walsenders);
	pg_log_debug("subscriber: current wal senders: %d", cur_walsenders);

	/*
	 * If standby sets primary_slot_name, check if this replication slot is in
	 * use on primary for WAL retention purposes. This replication slot has no
	 * use after the transformation, hence, it will be removed at the end of
	 * this process.
	 */
	if (standby.primary_slot_name)
	{
		appendPQExpBuffer(str,
						  "SELECT 1 FROM pg_replication_slots WHERE active AND slot_name = '%s'",
						  standby.primary_slot_name);

		pg_log_debug("command is: %s", str->data);

		res = PQexec(conn, str->data);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			pg_log_error("could not obtain replication slot information: %s", PQresultErrorMessage(res));
			return false;
		}

		if (PQntuples(res) != 1)
		{
			pg_log_error("could not obtain replication slot information: got %d rows, expected %d row",
						 PQntuples(res), 1);
			pg_free(standby.primary_slot_name); /* it is not being used. */
			standby.primary_slot_name = NULL;
			return false;
		}
		else
		{
			pg_log_info("primary has replication slot \"%s\"",
						standby.primary_slot_name);
		}

		PQclear(res);
	}

	disconnect_database(conn);

	if (strcmp(wal_level, "logical") != 0)
	{
		pg_log_error("publisher requires wal_level >= logical");
		return false;
	}

	if (max_repslots - cur_repslots < dbarr->ndbs)
	{
		pg_log_error("publisher requires %d replication slots, but only %d remain",
					 dbarr->ndbs, max_repslots - cur_repslots);
		pg_log_error_hint("Consider increasing max_replication_slots to at least %d.",
						  cur_repslots + dbarr->ndbs);
		return false;
	}

	if (max_walsenders - cur_walsenders < dbarr->ndbs)
	{
		pg_log_error("publisher requires %d wal sender processes, but only %d remain",
					 dbarr->ndbs, max_walsenders - cur_walsenders);
		pg_log_error_hint("Consider increasing max_wal_senders to at least %d.",
						  cur_walsenders + dbarr->ndbs);
		return false;
	}

	return true;
}

/*
 * Is the standby server ready for logical replication?
 */
static bool
check_subscriber(StandbyInfo *standby, LogicalRepPerdbInfoArr *dbarr)
{
	PGconn	   *conn;
	PGresult   *res;

	int			max_lrworkers;
	int			max_repslots;
	int			max_wprocs;

	pg_log_info("checking settings on subscriber");

	/*
	 * Logical replication requires a few parameters to be set on subscriber.
	 * Since these parameters are not a requirement for physical replication,
	 * we should check it to make sure it won't fail.
	 *
	 * max_replication_slots >= number of dbs to be converted
	 * max_logical_replication_workers >= number of dbs to be converted
	 * max_worker_processes >= 1 + number of dbs to be converted
	 */
	conn = connect_database(standby->base_conninfo, dbarr->perdb[0].dbname);
	if (conn == NULL)
		exit(1);

	res = PQexec(conn,
				 "SELECT setting FROM pg_settings WHERE name IN ('max_logical_replication_workers', 'max_replication_slots', 'max_worker_processes', 'primary_slot_name') ORDER BY name");

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		pg_log_error("could not obtain subscriber settings: %s", PQresultErrorMessage(res));
		return false;
	}

	max_lrworkers = atoi(PQgetvalue(res, 0, 0));
	max_repslots = atoi(PQgetvalue(res, 1, 0));
	max_wprocs = atoi(PQgetvalue(res, 2, 0));
	if (strcmp(PQgetvalue(res, 3, 0), "") != 0)
		standby->primary_slot_name = pg_strdup(PQgetvalue(res, 3, 0));

	pg_log_debug("subscriber: max_logical_replication_workers: %d", max_lrworkers);
	pg_log_debug("subscriber: max_replication_slots: %d", max_repslots);
	pg_log_debug("subscriber: max_worker_processes: %d", max_wprocs);
	pg_log_debug("subscriber: primary_slot_name: %s", standby->primary_slot_name);

	PQclear(res);

	disconnect_database(conn);

	if (max_repslots < dbarr->ndbs)
	{
		pg_log_error("subscriber requires %d replication slots, but only %d remain",
					 dbarr->ndbs, max_repslots);
		pg_log_error_hint("Consider increasing max_replication_slots to at least %d.",
						  dbarr->ndbs);
		return false;
	}

	if (max_lrworkers < dbarr->ndbs)
	{
		pg_log_error("subscriber requires %d logical replication workers, but only %d remain",
					 dbarr->ndbs, max_lrworkers);
		pg_log_error_hint("Consider increasing max_logical_replication_workers to at least %d.",
						  dbarr->ndbs);
		return false;
	}

	if (max_wprocs < dbarr->ndbs + 1)
	{
		pg_log_error("subscriber requires %d worker processes, but only %d remain",
					 dbarr->ndbs + 1, max_wprocs);
		pg_log_error_hint("Consider increasing max_worker_processes to at least %d.",
						  dbarr->ndbs + 1);
		return false;
	}

	return true;
}

/*
 * Create the subscriptions, adjust the initial location for logical replication and
 * enable the subscriptions. That's the last step for logical repliation setup.
 */
static bool
setup_subscriber(StandbyInfo *standby, PrimaryInfo *primary,
				 LogicalRepPerdbInfoArr *dbarr, const char *consistent_lsn)
{
	PGconn	   *conn;

	for (int i = 0; i < dbarr->ndbs; i++)
	{
		LogicalRepPerdbInfo *perdb = &dbarr->perdb[i];

		/* Connect to subscriber. */
		conn = connect_database(standby->base_conninfo, perdb->dbname);
		if (conn == NULL)
			exit(1);

		/*
		 * Since the publication was created before the consistent LSN, it is
		 * available on the subscriber when the physical replica is promoted.
		 * Remove publications from the subscriber because it has no use.
		 */
		drop_publication(conn, perdb);

		create_subscription(conn, standby, primary, perdb);

		/* Set the replication progress to the correct LSN. */
		set_replication_progress(conn, perdb, consistent_lsn);

		/* Enable subscription. */
		enable_subscription(conn, perdb);

		disconnect_database(conn);
	}

	return true;
}

/*
 * Create a logical replication slot and returns a consistent LSN. The returned
 * LSN might be used to catch up the subscriber up to the required point.
 *
 * CreateReplicationSlot() is not used because it does not provide the one-row
 * result set that contains the consistent LSN.
 */
static char *
create_logical_replication_slot(PGconn *conn, bool temporary,
								LogicalRepPerdbInfo *perdb)
{
	PQExpBuffer str = createPQExpBuffer();
	PGresult   *res = NULL;
	char	   *lsn = NULL;
	char		slot_name[NAMEDATALEN];

	Assert(conn != NULL);

	/*
	 * Construct a name of logical replication slot. The formatting is
	 * different depends on its persistency.
	 *
	 * For persistent slots: the name must be same as the subscription.
	 * For temporary slots: OID is not needed, but another string is added.
 	 */
	if (temporary)
		snprintf(slot_name, NAMEDATALEN, "pg_subscriber_%d_startpoint",
				 (int) getpid());
	else
		get_subscription_name(perdb->oid, (int) getpid(), slot_name,
							  NAMEDATALEN);

	pg_log_info("creating the replication slot \"%s\" on database \"%s\"",
				slot_name, perdb->dbname);

	appendPQExpBuffer(str, "SELECT * FROM pg_create_logical_replication_slot('%s', 'pgoutput', %s, false, false);",
					  slot_name, temporary ? "true" : "false");

	pg_log_debug("command is: %s", str->data);

	if (!dry_run)
	{
		res = PQexec(conn, str->data);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			pg_log_error("could not create replication slot \"%s\" on database \"%s\": %s",
						 slot_name, perdb->dbname, PQresultErrorMessage(res));
			return lsn;
		}
	}

	pg_log_info("create replication slot \"%s\" on publisher", slot_name);

	/* for cleanup purposes */
	if (temporary)
		primary.made_transient_replslot = true;
	else
		perdb->made_replslot = true;

	if (!dry_run)
	{
		lsn = pg_strdup(PQgetvalue(res, 0, 1));
		PQclear(res);
	}

	destroyPQExpBuffer(str);

	return lsn;
}

static void
drop_replication_slot(PGconn *conn, LogicalRepPerdbInfo *perdb,
					  const char *slot_name)
{
	PQExpBuffer str = createPQExpBuffer();
	PGresult   *res;

	Assert(conn != NULL);

	pg_log_info("dropping the replication slot \"%s\" on database \"%s\"",
				slot_name, perdb->dbname);

	appendPQExpBuffer(str, "SELECT * FROM pg_drop_replication_slot('%s');",
					  slot_name);

	pg_log_debug("command is: %s", str->data);

	if (!dry_run)
	{
		res = PQexec(conn, str->data);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			pg_log_error("could not drop replication slot \"%s\" on database \"%s\": %s",
						 slot_name, perdb->dbname, PQerrorMessage(conn));

		PQclear(res);
	}

	destroyPQExpBuffer(str);
}

static char *
server_logfile_name(const char *datadir)
{
	char		timebuf[128];
	struct timeval time;
	time_t		tt;
	int			len;
	char	   *filename;

	/* append timestamp with ISO 8601 format. */
	gettimeofday(&time, NULL);
	tt = (time_t) time.tv_sec;
	strftime(timebuf, sizeof(timebuf), "%Y%m%dT%H%M%S", localtime(&tt));
	snprintf(timebuf + strlen(timebuf), sizeof(timebuf) - strlen(timebuf),
			 ".%03d", (int) (time.tv_usec / 1000));

	filename = (char *) pg_malloc0(MAXPGPATH);
	len = snprintf(filename, MAXPGPATH, "%s/%s/server_start_%s.log", datadir, PGS_OUTPUT_DIR, timebuf);
	if (len >= MAXPGPATH)
	{
		pg_log_error("log file path is too long");
		exit(1);
	}

	return filename;
}

static void
start_standby_server(StandbyInfo *standby)
{
	char	   *pg_ctl_cmd;
	int			rc;

	pg_ctl_cmd = psprintf("\"%s/pg_ctl\" start -D \"%s\" -s -l \"%s\"",
						  standby->bindir, standby->pgdata, standby->server_log);
	rc = system(pg_ctl_cmd);
	pg_ctl_status(pg_ctl_cmd, rc, 1);
}

static void
stop_standby_server(StandbyInfo *standby)
{
	char	   *pg_ctl_cmd;
	int			rc;

	pg_ctl_cmd = psprintf("\"%s/pg_ctl\" stop -D \"%s\" -s", standby->bindir,
						  standby->pgdata);
	rc = system(pg_ctl_cmd);
	pg_ctl_status(pg_ctl_cmd, rc, 0);
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
			pg_log_error_detail("See C include file \"ntstatus.h\" for a description of the hexadecimal value.");
#else
			pg_log_error("pg_ctl was terminated by signal %d: %s",
						 WTERMSIG(rc), pg_strsignal(WTERMSIG(rc)));
#endif
		}
		else
		{
			pg_log_error("pg_ctl exited with unrecognized status %d", rc);
		}

		pg_log_error_detail("The failed command was: %s", pg_ctl_cmd);
		exit(1);
	}

	if (action)
		pg_log_info("postmaster was started");
	else
		pg_log_info("postmaster was stopped");
}

/*
 * Returns after the server finishes the recovery process.
 *
 * If recovery_timeout option is set, terminate abnormally without finishing
 * the recovery process. By default, it waits forever.
 */
static void
wait_for_end_recovery(StandbyInfo *standby, const char *dbname)
{
	PGconn	   *conn;
	PGresult   *res;
	int			status = POSTMASTER_STILL_STARTING;
	int			timer = 0;

	pg_log_info("waiting the postmaster to reach the consistent state");

	conn = connect_database(standby->base_conninfo, dbname);
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

		/*
		 * Does the recovery process finish? In dry run mode, there is no
		 * recovery mode. Bail out as the recovery process has ended.
		 */
		if (!in_recovery || dry_run)
		{
			status = POSTMASTER_READY;
			break;
		}

		/*
		 * Bail out after recovery_timeout seconds if this option is set.
		 */
		if (recovery_timeout > 0 && timer >= recovery_timeout)
		{
			pg_log_error("recovery timed out");
			stop_standby_server(standby);
			exit(1);
		}

		/* Keep waiting. */
		pg_usleep(WAIT_INTERVAL * USEC_PER_SEC);

		timer += WAIT_INTERVAL;
	}

	disconnect_database(conn);

	if (status == POSTMASTER_STILL_STARTING)
	{
		pg_log_error("server did not end recovery");
		exit(1);
	}

	pg_log_info("postmaster reached the consistent state");
}

/*
 * Create a publication that includes all tables in the database.
 */
static void
create_publication(PGconn *conn, PrimaryInfo *primary,
				   LogicalRepPerdbInfo *perdb)
{
	PQExpBuffer str = createPQExpBuffer();
	PGresult   *res;
	char		pubname[NAMEDATALEN];

	Assert(conn != NULL);

	get_publication_name(perdb->oid, pubname, NAMEDATALEN);

	/* Check if the publication needs to be created. */
	appendPQExpBuffer(str,
					  "SELECT puballtables FROM pg_catalog.pg_publication WHERE pubname = '%s'",
					  pubname);
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
		 * use it. A previous run of pg_createsubscriber must have created
		 * this publication. Bail out.
		 */
		if (strcmp(PQgetvalue(res, 0, 0), "t") == 0)
		{
			pg_log_info("publication \"%s\" already exists", pubname);
			return;
		}
		else
		{
			/*
			 * Unfortunately, if it reaches this code path, it will always
			 * fail (unless you decide to change the existing publication
			 * name). That's bad but it is very unlikely that the user will
			 * choose a name with pg_createsubscriber_ prefix followed by the
			 * exact database oid in which puballtables is false.
			 */
			pg_log_error("publication \"%s\" does not replicate changes for all tables",
						 pubname);
			pg_log_error_hint("Consider renaming this publication.");
			PQclear(res);
			PQfinish(conn);
			exit(1);
		}
	}

	PQclear(res);
	resetPQExpBuffer(str);

	pg_log_info("creating publication \"%s\" on database \"%s\"", pubname, perdb->dbname);

	appendPQExpBuffer(str, "CREATE PUBLICATION %s FOR ALL TABLES", pubname);

	pg_log_debug("command is: %s", str->data);

	if (!dry_run)
	{
		res = PQexec(conn, str->data);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			pg_log_error("could not create publication \"%s\" on database \"%s\": %s",
						 pubname, perdb->dbname, PQerrorMessage(conn));;
			PQfinish(conn);
			exit(1);
		}
	}

	/* for cleanup purposes */
	perdb->made_publication = true;

	if (!dry_run)
		PQclear(res);

	destroyPQExpBuffer(str);
}

/*
 * Remove publication if it couldn't finish all steps.
 */
static void
drop_publication(PGconn *conn, LogicalRepPerdbInfo *perdb)
{
	PQExpBuffer str = createPQExpBuffer();
	PGresult   *res;
	char		pubname[NAMEDATALEN];

	Assert(conn != NULL);

	get_publication_name(perdb->oid, pubname, NAMEDATALEN);

	pg_log_info("dropping publication \"%s\" on database \"%s\"",
				pubname, perdb->dbname);

	appendPQExpBuffer(str, "DROP PUBLICATION %s", pubname);

	pg_log_debug("command is: %s", str->data);

	if (!dry_run)
	{
		res = PQexec(conn, str->data);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			pg_log_error("could not drop publication \"%s\" on database \"%s\": %s",
						 pubname, perdb->dbname, PQerrorMessage(conn));

		PQclear(res);
	}

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
create_subscription(PGconn *conn, StandbyInfo *standby,
					PrimaryInfo *primary,
					LogicalRepPerdbInfo *perdb)
{
	PQExpBuffer str = createPQExpBuffer();
	PGresult   *res;
	char		subname[NAMEDATALEN];
	char		pubname[NAMEDATALEN];
	char	   *escaped_conninfo;

	Assert(conn != NULL);

	get_subscription_name(perdb->oid, (int) getpid(), subname, NAMEDATALEN);
	get_publication_name(perdb->oid, pubname, NAMEDATALEN);

	pg_log_info("creating subscription \"%s\" on database \"%s\"", subname,
				perdb->dbname);

	escaped_conninfo = escape_single_quotes_ascii(primary->base_conninfo);

	appendPQExpBuffer(str,
					  "CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION %s "
					  "WITH (create_slot = false, copy_data = false, enabled = false)",
					  subname,
					  concat_conninfo_dbname(escaped_conninfo, perdb->dbname),
					  pubname);

	pg_log_debug("command is: %s", str->data);

	if (!dry_run)
	{
		res = PQexec(conn, str->data);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			pg_log_error("could not create subscription \"%s\" on database \"%s\": %s",
						 subname, perdb->dbname, PQerrorMessage(conn));
			PQfinish(conn);
			exit(1);
		}
	}

	/* for cleanup purposes */
	perdb->made_subscription = true;

	if (!dry_run)
		PQclear(res);

	pg_free(escaped_conninfo);
	destroyPQExpBuffer(str);
}

/*
 * Remove subscription if it couldn't finish all steps.
 */
static void
drop_subscription(PGconn *conn, LogicalRepPerdbInfo *perdb)
{
	PQExpBuffer str = createPQExpBuffer();
	PGresult   *res;
	char		subname[NAMEDATALEN];

	Assert(conn != NULL);

	get_subscription_name(perdb->oid, (int) getpid(), subname, NAMEDATALEN);

	pg_log_info("dropping subscription \"%s\" on database \"%s\"",
				subname, perdb->dbname);

	appendPQExpBuffer(str, "DROP SUBSCRIPTION %s", subname);

	pg_log_debug("command is: %s", str->data);

	if (!dry_run)
	{
		res = PQexec(conn, str->data);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			pg_log_error("could not drop subscription \"%s\" on database \"%s\": %s",
						 subname, perdb->dbname, PQerrorMessage(conn));

		PQclear(res);
	}

	destroyPQExpBuffer(str);
}

/*
 * Sets the replication progress to the consistent LSN.
 *
 * The subscriber caught up to the consistent LSN provided by the temporary
 * replication slot. The goal is to set up the initial location for the logical
 * replication that is the exact LSN that the subscriber was promoted. Once the
 * subscription is enabled it will start streaming from that location onwards.
 * In dry run mode, the subscription OID and LSN are set to invalid values for
 * printing purposes.
 */
static void
set_replication_progress(PGconn *conn, LogicalRepPerdbInfo *perdb,
						 const char *lsn)
{
	PQExpBuffer str = createPQExpBuffer();
	PGresult   *res;
	Oid			suboid;
	char		originname[NAMEDATALEN];
	char		lsnstr[17 + 1]; /* MAXPG_LSNLEN = 17 */
	char		subname[NAMEDATALEN];

	Assert(conn != NULL);

	get_subscription_name(perdb->oid, (int) getpid(), subname, NAMEDATALEN);

	appendPQExpBuffer(str,
					  "SELECT oid FROM pg_catalog.pg_subscription WHERE subname = '%s'",
					  subname);

	res = PQexec(conn, str->data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		pg_log_error("could not obtain subscription OID: %s",
					 PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(conn);
		exit(1);
	}

	if (PQntuples(res) != 1 && !dry_run)
	{
		pg_log_error("could not obtain subscription OID: got %d rows, expected %d rows",
					 PQntuples(res), 1);
		PQclear(res);
		PQfinish(conn);
		exit(1);
	}

	if (dry_run)
	{
		suboid = InvalidOid;
		snprintf(lsnstr, sizeof(lsnstr), "%X/%X", LSN_FORMAT_ARGS((XLogRecPtr) InvalidXLogRecPtr));
	}
	else
	{
		suboid = strtoul(PQgetvalue(res, 0, 0), NULL, 10);
		snprintf(lsnstr, sizeof(lsnstr), "%s", lsn);
	}

	/*
	 * The origin name is defined as pg_%u. %u is the subscription OID. See
	 * ApplyWorkerMain().
	 */
	snprintf(originname, sizeof(originname), "pg_%u", suboid);

	PQclear(res);

	pg_log_info("setting the replication progress (node name \"%s\" ; LSN %s) on database \"%s\"",
				originname, lsnstr, perdb->dbname);

	resetPQExpBuffer(str);
	appendPQExpBuffer(str,
					  "SELECT pg_catalog.pg_replication_origin_advance('%s', '%s')", originname, lsnstr);

	pg_log_debug("command is: %s", str->data);

	if (!dry_run)
	{
		res = PQexec(conn, str->data);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			pg_log_error("could not set replication progress for the subscription \"%s\": %s",
						 subname, PQresultErrorMessage(res));
			PQfinish(conn);
			exit(1);
		}

		PQclear(res);
	}

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
enable_subscription(PGconn *conn, LogicalRepPerdbInfo *perdb)
{
	PQExpBuffer str = createPQExpBuffer();
	PGresult   *res;
	char		subname[NAMEDATALEN];

	Assert(conn != NULL);

	get_subscription_name(perdb->oid, (int) getpid(), subname, NAMEDATALEN);
	pg_log_info("enabling subscription \"%s\" on database \"%s\"", subname,
				perdb->dbname);
	

	appendPQExpBuffer(str, "ALTER SUBSCRIPTION %s ENABLE", subname);

	pg_log_debug("command is: %s", str->data);

	if (!dry_run)
	{
		res = PQexec(conn, str->data);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			pg_log_error("could not enable subscription \"%s\": %s", subname,
						 PQerrorMessage(conn));
			PQfinish(conn);
			exit(1);
		}

		PQclear(res);
	}

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
		{"subscriber-server", required_argument, NULL, 'S'},
		{"database", required_argument, NULL, 'd'},
		{"dry-run", no_argument, NULL, 'n'},
		{"recovery-timeout", required_argument, NULL, 't'},
		{"retain", no_argument, NULL, 'r'},
		{"verbose", no_argument, NULL, 'v'},
		{NULL, 0, NULL, 0}
	};

	int			c;
	int			option_index;

	char	   *base_dir;
	int			len;

	char	   *dbname_conninfo = NULL;

	struct stat statbuf;

	PGconn	   *conn;
	char	   *consistent_lsn;

	PQExpBuffer recoveryconfcontents = NULL;

	char		pidfile[MAXPGPATH];

	pg_logging_init(argv[0]);
	pg_logging_set_level(PG_LOG_WARNING);
	progname = get_progname(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_createsubscriber"));

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
			puts("pg_createsubscriber (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	/*
	 * Don't allow it to be run as root. It uses pg_ctl which does not allow
	 * it either.
	 */
#ifndef WIN32
	if (geteuid() == 0)
	{
		pg_log_error("cannot be executed by \"root\"");
		pg_log_error_hint("You must run %s as the PostgreSQL superuser.",
						  progname);
		exit(1);
	}
#endif

	while ((c = getopt_long(argc, argv, "D:S:d:nrt:v",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'D':
				standby.pgdata = pg_strdup(optarg);
				break;
			case 'S':
				sub_conninfo_str = pg_strdup(optarg);
				break;
			case 'd':
				/* Ignore duplicated database names. */
				if (!simple_string_list_member(&database_names, optarg))
				{
					simple_string_list_append(&database_names, optarg);
					dbarr.ndbs++;
				}
				break;
			case 'n':
				dry_run = true;
				break;
			case 'r':
				retain = true;
				break;
			case 't':
				recovery_timeout = atoi(optarg);
				break;
			case 'v':
				pg_logging_increase_verbosity();
				break;
			default:
				/* getopt_long already emitted a complaint */
				pg_log_error_hint("Try \"%s --help\" for more information.", progname);
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
		pg_log_error_hint("Try \"%s --help\" for more information.", progname);
		exit(1);
	}

	/*
	 * Required arguments
	 */
	if (standby.pgdata == NULL)
	{
		pg_log_error("no subscriber data directory specified");
		pg_log_error_hint("Try \"%s --help\" for more information.", progname);
		exit(1);
	}

	if (sub_conninfo_str == NULL)
	{
		pg_log_error("no subscriber connection string specified");
		pg_log_error_hint("Try \"%s --help\" for more information.", progname);
		exit(1);
	}
	standby.base_conninfo = get_base_conninfo(sub_conninfo_str, dbname_conninfo);
	if (standby.base_conninfo == NULL)
		exit(1);

	if (database_names.head == NULL)
	{
		pg_log_info("no database was specified");

		/*
		 * If --database option is not provided, try to obtain the dbname from
		 * the subscriber conninfo. If dbname parameter is not available, error
		 * out.
		 */
		if (dbname_conninfo)
		{
			simple_string_list_append(&database_names, dbname_conninfo);
			dbarr.ndbs++;

			pg_log_info("database \"%s\" was extracted from the subscriber connection string",
						dbname_conninfo);
		}
		else
		{
			pg_log_error("no database name specified");
			pg_log_error_hint("Try \"%s --help\" for more information.", progname);
			exit(1);
		}
	}

	/* Obtain a connection string from the target */
	primary.base_conninfo = get_primary_conninfo(&standby);

	/*
	 * Get the absolute path of pg_ctl and pg_resetwal on the subscriber.
	 */
	if (!get_exec_base_path(argv[0]))
		exit(1);

	/* rudimentary check for a data directory. */
	if (!check_data_directory(standby.pgdata))
		exit(1);

	/* Store database information to dbarr */
	store_db_names(&dbarr.perdb, dbarr.ndbs);

	/* Register a function to clean up objects in case of failure. */
	atexit(cleanup_objects_atexit);

	/*
	 * Check if the subscriber data directory has the same system identifier
	 * than the publisher data directory.
	 */
	get_sysid_for_primary(&primary, dbarr.perdb[0].dbname);
	get_sysid_for_standby(&standby);
	if (primary.sysid != standby.sysid)
	{
		pg_log_error("subscriber data directory is not a copy of the source database cluster");
		exit(1);
	}

	/*
	 * Create the output directory to store any data generated by this tool.
	 */
	base_dir = (char *) pg_malloc0(MAXPGPATH);
	len = snprintf(base_dir, MAXPGPATH, "%s/%s", standby.pgdata, PGS_OUTPUT_DIR);
	if (len >= MAXPGPATH)
	{
		pg_log_error("directory path for subscriber is too long");
		exit(1);
	}

	if (mkdir(base_dir, pg_dir_create_mode) < 0 && errno != EEXIST)
	{
		pg_log_error("could not create directory \"%s\": %m", base_dir);
		exit(1);
	}

	standby.server_log = server_logfile_name(standby.pgdata);

	/* subscriber PID file. */
	snprintf(pidfile, MAXPGPATH, "%s/postmaster.pid", standby.pgdata);

	/*
	 * The standby server must be running. That's because some checks will be
	 * done (is it ready for a logical replication setup?). After that, stop
	 * the subscriber in preparation to modify some recovery parameters that
	 * require a restart.
	 */
	if (stat(pidfile, &statbuf) == 0)
	{
		/*
		 * Check if the standby server is ready for logical replication.
		 */
		if (!check_subscriber(&standby, &dbarr))
			exit(1);

		/*
		 * Check if the primary server is ready for logical replication. This
		 * routine checks if a replication slot is in use on primary so it
		 * relies on check_subscriber() to obtain the primary_slot_name.
		 * That's why it is called after it.
		 */
		if (!check_publisher(&primary, &dbarr))
			exit(1);

		/*
		 * Create the required objects for each database on publisher. This
		 * step is here mainly because if we stop the standby we cannot verify
		 * if the primary slot is in use. We could use an extra connection for
		 * it but it doesn't seem worth.
		 */
		if (!setup_publisher(&primary, &dbarr))
			exit(1);

		/* Stop the standby server. */
		pg_log_info("standby is up and running");
		pg_log_info("stopping the server to start the transformation steps");
		stop_standby_server(&standby);
	}
	else
	{
		pg_log_error("standby is not running");
		pg_log_error_hint("Start the standby and try again.");
		exit(1);
	}

	/*
	 * Create a temporary logical replication slot to get a consistent LSN.
	 *
	 * This consistent LSN will be used later to advanced the recently created
	 * replication slots. It is ok to use a temporary replication slot here
	 * because it will have a short lifetime and it is only used as a mark to
	 * start the logical replication.
	 *
	 * XXX we should probably use the last created replication slot to get a
	 * consistent LSN but it should be changed after adding pg_basebackup
	 * support.
	 */
	conn = connect_database(primary.base_conninfo, dbarr.perdb[0].dbname);
	if (conn == NULL)
		exit(1);
	consistent_lsn = create_logical_replication_slot(conn, true, &dbarr.perdb[0]);

	/*
	 * Write recovery parameters.
	 *
	 * Despite of the recovery parameters will be written to the subscriber,
	 * use a publisher connection for the follwing recovery functions. The
	 * connection is only used to check the current server version (physical
	 * replica, same server version). The subscriber is not running yet. In
	 * dry run mode, the recovery parameters *won't* be written. An invalid
	 * LSN is used for printing purposes.
	 */
	recoveryconfcontents = GenerateRecoveryConfig(conn, NULL);
	appendPQExpBuffer(recoveryconfcontents, "recovery_target_inclusive = true\n");
	appendPQExpBuffer(recoveryconfcontents, "recovery_target_action = promote\n");

	if (dry_run)
	{
		appendPQExpBuffer(recoveryconfcontents, "# dry run mode");
		appendPQExpBuffer(recoveryconfcontents, "recovery_target_lsn = '%X/%X'\n",
						  LSN_FORMAT_ARGS((XLogRecPtr) InvalidXLogRecPtr));
	}
	else
	{
		appendPQExpBuffer(recoveryconfcontents, "recovery_target_lsn = '%s'\n",
						  consistent_lsn);
		WriteRecoveryConfig(conn, standby.pgdata, recoveryconfcontents);
	}
	disconnect_database(conn);

	pg_log_debug("recovery parameters:\n%s", recoveryconfcontents->data);

	/*
	 * Start subscriber and wait until accepting connections.
	 */
	pg_log_info("starting the subscriber");
	start_standby_server(&standby);

	/*
	 * Waiting the subscriber to be promoted.
	 */
	wait_for_end_recovery(&standby, dbarr.perdb[0].dbname);

	/*
	 * Create the subscription for each database on subscriber. It does not
	 * enable it immediately because it needs to adjust the logical
	 * replication start point to the LSN reported by consistent_lsn (see
	 * set_replication_progress). It also cleans up publications created by
	 * this tool and replication to the standby.
	 */
	if (!setup_subscriber(&standby, &primary, &dbarr, consistent_lsn))
		exit(1);

	/*
	 * If the primary_slot_name exists on primary, drop it.
	 *
	 * XXX we might not fail here. Instead, we provide a warning so the user
	 * eventually drops this replication slot later.
	 */
	if (standby.primary_slot_name != NULL)
	{
		char *primary_slot_name = standby.primary_slot_name;
		LogicalRepPerdbInfo *perdb = &dbarr.perdb[0];

		conn = connect_database(primary.base_conninfo, perdb->dbname);
		if (conn != NULL)
		{
			drop_replication_slot(conn, perdb, primary_slot_name);
		}
		else
		{
			pg_log_warning("could not drop replication slot \"%s\" on primary", primary_slot_name);
			pg_log_warning_hint("Drop this replication slot soon to avoid retention of WAL files.");
		}
		disconnect_database(conn);
	}

	/*
	 * Stop the subscriber.
	 */
	pg_log_info("stopping the subscriber");
	stop_standby_server(&standby);

	/*
	 * Change system identifier.
	 */
	modify_sysid(standby.bindir, standby.pgdata);

	/*
	 * The log file is kept if retain option is specified or this tool does
	 * not run successfully. Otherwise, log file is removed.
	 */
	if (!retain)
		unlink(standby.server_log);

	success = true;

	pg_log_info("Done!");

	return 0;
}
