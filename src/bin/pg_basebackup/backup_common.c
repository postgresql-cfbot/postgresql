/*-------------------------------------------------------------------------
 *
 * backup_common.c - common code that is used in both pg_basebackup and pg_rewind.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#include "common/logging.h"
#include "fe_utils/string_utils.h"
#include "backup_common.h"

/* Contents of configuration file to be generated */
PQExpBuffer recoveryconfcontents = NULL;

bool writerecoveryconf = false;
char *replication_slot = NULL;
PGconn	   *conn = NULL;

void
disconnect_atexit(void)
{
	if (conn != NULL)
		PQfinish(conn);
}

/*
 * Escape a string so that it can be used as a value in a key-value pair
 * a configuration file.
 */
static char *
escape_quotes(const char *src)
{
	char	   *result = escape_single_quotes_ascii(src);

	if (!result)
	{
		pg_log_error("out of memory");
		exit(1);
	}
	return result;
}

/*
 * Create a configuration file in memory using a PQExpBuffer
 */
void
GenerateRecoveryConf(void)
{
	PQconninfoOption *connOptions;
	PQconninfoOption *option;
	PQExpBufferData conninfo_buf;
	char	   *escaped;

	recoveryconfcontents = createPQExpBuffer();
	if (!recoveryconfcontents)
	{
		pg_log_error("out of memory");
		exit(1);
	}

	/*
	 * In PostgreSQL 12 and newer versions, standby_mode is gone, replaced by
	 * standby.signal to trigger a standby state at recovery.
	 */
	if (conn && PQserverVersion(conn) < MINIMUM_VERSION_FOR_RECOVERY_GUC)
		appendPQExpBufferStr(recoveryconfcontents, "standby_mode = 'on'\n");

	connOptions = PQconninfo(conn);
	if (connOptions == NULL)
	{
		pg_log_error("out of memory");
		exit(1);
	}

	initPQExpBuffer(&conninfo_buf);
	for (option = connOptions; option && option->keyword; option++)
	{
		/* Omit empty settings and those libpqwalreceiver overrides. */
		if (strcmp(option->keyword, "replication") == 0 ||
			strcmp(option->keyword, "dbname") == 0 ||
			strcmp(option->keyword, "fallback_application_name") == 0 ||
			(option->val == NULL) ||
			(option->val != NULL && option->val[0] == '\0'))
			continue;

		/* Separate key-value pairs with spaces */
		if (conninfo_buf.len != 0)
			appendPQExpBufferChar(&conninfo_buf, ' ');

		/*
		 * Write "keyword=value" pieces, the value string is escaped and/or
		 * quoted if necessary.
		 */
		appendPQExpBuffer(&conninfo_buf, "%s=", option->keyword);
		appendConnStrVal(&conninfo_buf, option->val);
	}

	/*
	 * Escape the connection string, so that it can be put in the config file.
	 * Note that this is different from the escaping of individual connection
	 * options above!
	 */
	escaped = escape_quotes(conninfo_buf.data);
	appendPQExpBuffer(recoveryconfcontents, "primary_conninfo = '%s'\n", escaped);
	free(escaped);

	if (replication_slot)
	{
		escaped = escape_quotes(replication_slot);
		appendPQExpBuffer(recoveryconfcontents, "primary_slot_name = '%s'\n", replication_slot);
		free(escaped);
	}

	if (PQExpBufferBroken(recoveryconfcontents) ||
		PQExpBufferDataBroken(conninfo_buf))
	{
		pg_log_error("out of memory");
		exit(1);
	}

	termPQExpBuffer(&conninfo_buf);

	PQconninfoFree(connOptions);
}

/*
 * Write the configuration file into the directory specified in basedir,
 * with the contents already collected in memory appended.  Then write
 * the signal file into the basedir.  If the server does not support
 * recovery parameters as GUCs, the signal file is not necessary, and
 * configuration is written to recovery.conf.
 */
void
WriteRecoveryConf(char *target_dir)
{
	char		filename[MAXPGPATH];
	FILE	   *cf;
	bool		is_recovery_guc_supported = true;

	if (conn && PQserverVersion(conn) < MINIMUM_VERSION_FOR_RECOVERY_GUC)
		is_recovery_guc_supported = false;

	snprintf(filename, MAXPGPATH, "%s/%s", target_dir,
			 is_recovery_guc_supported ? "postgresql.auto.conf" : "recovery.conf");

	cf = fopen(filename, is_recovery_guc_supported ? "a" : "w");
	if (cf == NULL)
	{
		pg_log_error("could not open file \"%s\": %m", filename);
		exit(1);
	}

	if (fwrite(recoveryconfcontents->data, recoveryconfcontents->len, 1, cf) != 1)
	{
		pg_log_error("could not write to file \"%s\": %m", filename);
		exit(1);
	}

	fclose(cf);

	if (is_recovery_guc_supported)
	{
		snprintf(filename, MAXPGPATH, "%s/%s", target_dir, "standby.signal");
		cf = fopen(filename, "w");
		if (cf == NULL)
		{
			pg_log_error("could not create file \"%s\": %m", filename);
			exit(1);
		}

		fclose(cf);
	}
}
