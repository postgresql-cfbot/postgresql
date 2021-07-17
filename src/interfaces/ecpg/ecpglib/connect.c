/* src/interfaces/ecpg/ecpglib/connect.c */

#define POSTGRES_ECPG_INTERNAL
#include "postgres_fe.h"

#include "ecpg-pthread-win32.h"
#include "ecpgerrno.h"
#include "ecpglib.h"
#include "ecpglib_extern.h"
#include "ecpgtype.h"
#include "sqlca.h"

#ifdef ENABLE_THREAD_SAFETY
static pthread_mutex_t connections_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_key_t actual_connection_key;
static pthread_once_t actual_connection_key_once = PTHREAD_ONCE_INIT;
#endif
static struct connection *actual_connection = NULL;
static struct connection *all_connections = NULL;

#ifdef ENABLE_THREAD_SAFETY
static void
ecpg_actual_connection_init(void)
{
	pthread_key_create(&actual_connection_key, NULL);
}

void
ecpg_pthreads_init(void)
{
	pthread_once(&actual_connection_key_once, ecpg_actual_connection_init);
}
#endif

static struct connection *
ecpg_get_connection_nr(const char *connection_name)
{
	struct connection *ret = NULL;

	if ((connection_name == NULL) || (strcmp(connection_name, "CURRENT") == 0))
	{
#ifdef ENABLE_THREAD_SAFETY
		ret = pthread_getspecific(actual_connection_key);

		/*
		 * if no connection in TSD for this thread, get the global default
		 * connection and hope the user knows what they're doing (i.e. using
		 * their own mutex to protect that connection from concurrent accesses
		 */
		/* if !ret then  we  got the connection from TSD */
		if (NULL == ret)
			/* no TSD connection, going for global */
			ret = actual_connection;
#else
		ret = actual_connection;
#endif
	}
	else
	{
		struct connection *con;

		for (con = all_connections; con != NULL; con = con->next)
		{
			if (strcmp(connection_name, con->name) == 0)
				break;
		}
		ret = con;
	}

	return ret;
}

struct connection *
ecpg_get_connection(const char *connection_name)
{
	struct connection *ret = NULL;

	if ((connection_name == NULL) || (strcmp(connection_name, "CURRENT") == 0))
	{
#ifdef ENABLE_THREAD_SAFETY
		ret = pthread_getspecific(actual_connection_key);

		/*
		 * if no connection in TSD for this thread, get the global default
		 * connection and hope the user knows what they're doing (i.e. using
		 * their own mutex to protect that connection from concurrent accesses
		 */
		/* if !ret then  we  got the connection from TSD */
		if (NULL == ret)
			/* no TSD connection here either, using global */
			ret = actual_connection;
#else
		ret = actual_connection;
#endif
	}
	else
	{
#ifdef ENABLE_THREAD_SAFETY
		pthread_mutex_lock(&connections_mutex);
#endif

		ret = ecpg_get_connection_nr(connection_name);

#ifdef ENABLE_THREAD_SAFETY
		pthread_mutex_unlock(&connections_mutex);
#endif
	}

	return ret;
}

static void
ecpg_finish(struct connection *act)
{
	if (act != NULL)
	{
		struct ECPGtype_information_cache *cache,
				   *ptr;

		ecpg_deallocate_all_conn(0, ECPG_COMPAT_PGSQL, act);
		PQfinish(act->connection);

		/*
		 * no need to lock connections_mutex - we're always called by
		 * ECPGdisconnect or ECPGconnect, which are holding the lock
		 */

		/* remove act from the list */
		if (act == all_connections)
			all_connections = act->next;
		else
		{
			struct connection *con;

			for (con = all_connections; con->next && con->next != act; con = con->next);
			if (con->next)
				con->next = act->next;
		}

#ifdef ENABLE_THREAD_SAFETY
		if (pthread_getspecific(actual_connection_key) == act)
			pthread_setspecific(actual_connection_key, all_connections);
#endif
		if (actual_connection == act)
			actual_connection = all_connections;

		ecpg_log("ecpg_finish: connection %s closed\n", act->name ? act->name : "(null)");

		for (cache = act->cache_head; cache; ptr = cache, cache = cache->next, ecpg_free(ptr));
		ecpg_free(act->name);
		ecpg_free(act);
		/* delete cursor variables when last connection gets closed */
		if (all_connections == NULL)
		{
			struct var_list *iv_ptr;

			for (; ivlist; iv_ptr = ivlist, ivlist = ivlist->next, ecpg_free(iv_ptr));
		}
	}
	else
		ecpg_log("ecpg_finish: called an extra time\n");
}

bool
ECPGsetcommit(int lineno, const char *mode, const char *connection_name)
{
	struct connection *con = ecpg_get_connection(connection_name);
	PGresult   *results;

	if (!ecpg_init(con, connection_name, lineno))
		return false;

	ecpg_log("ECPGsetcommit on line %d: action \"%s\"; connection \"%s\"\n", lineno, mode, con->name);

	if (con->autocommit && strncmp(mode, "off", strlen("off")) == 0)
	{
		if (PQtransactionStatus(con->connection) == PQTRANS_IDLE)
		{
			results = PQexec(con->connection, "begin transaction");
			if (!ecpg_check_PQresult(results, lineno, con->connection, ECPG_COMPAT_PGSQL))
				return false;
			PQclear(results);
		}
		con->autocommit = false;
	}
	else if (!con->autocommit && strncmp(mode, "on", strlen("on")) == 0)
	{
		if (PQtransactionStatus(con->connection) != PQTRANS_IDLE)
		{
			results = PQexec(con->connection, "commit");
			if (!ecpg_check_PQresult(results, lineno, con->connection, ECPG_COMPAT_PGSQL))
				return false;
			PQclear(results);
		}
		con->autocommit = true;
	}

	return true;
}

bool
ECPGsetconn(int lineno, const char *connection_name)
{
	struct connection *con = ecpg_get_connection(connection_name);

	if (!ecpg_init(con, connection_name, lineno))
		return false;

#ifdef ENABLE_THREAD_SAFETY
	pthread_setspecific(actual_connection_key, con);
#else
	actual_connection = con;
#endif
	return true;
}


static void
ECPGnoticeReceiver(void *arg, const PGresult *result)
{
	char	   *sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);
	char	   *message = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);
	struct sqlca_t *sqlca = ECPGget_sqlca();
	int			sqlcode;

	if (sqlca == NULL)
	{
		ecpg_log("out of memory");
		return;
	}

	(void) arg;					/* keep the compiler quiet */
	if (sqlstate == NULL)
		sqlstate = ECPG_SQLSTATE_ECPG_INTERNAL_ERROR;

	if (message == NULL)		/* Shouldn't happen, but need to be sure */
		message = ecpg_gettext("empty message text");

	/* these are not warnings */
	if (strncmp(sqlstate, "00", 2) == 0)
		return;

	ecpg_log("ECPGnoticeReceiver: %s\n", message);

	/* map to SQLCODE for backward compatibility */
	if (strcmp(sqlstate, ECPG_SQLSTATE_INVALID_CURSOR_NAME) == 0)
		sqlcode = ECPG_WARNING_UNKNOWN_PORTAL;
	else if (strcmp(sqlstate, ECPG_SQLSTATE_ACTIVE_SQL_TRANSACTION) == 0)
		sqlcode = ECPG_WARNING_IN_TRANSACTION;
	else if (strcmp(sqlstate, ECPG_SQLSTATE_NO_ACTIVE_SQL_TRANSACTION) == 0)
		sqlcode = ECPG_WARNING_NO_TRANSACTION;
	else if (strcmp(sqlstate, ECPG_SQLSTATE_DUPLICATE_CURSOR) == 0)
		sqlcode = ECPG_WARNING_PORTAL_EXISTS;
	else
		sqlcode = 0;

	strncpy(sqlca->sqlstate, sqlstate, sizeof(sqlca->sqlstate));
	sqlca->sqlcode = sqlcode;
	sqlca->sqlwarn[2] = 'W';
	sqlca->sqlwarn[0] = 'W';

	strncpy(sqlca->sqlerrm.sqlerrmc, message, sizeof(sqlca->sqlerrm.sqlerrmc));
	sqlca->sqlerrm.sqlerrmc[sizeof(sqlca->sqlerrm.sqlerrmc) - 1] = 0;
	sqlca->sqlerrm.sqlerrml = strlen(sqlca->sqlerrm.sqlerrmc);

	ecpg_log("raising sqlcode %d\n", sqlcode);
}

/*
 * this function is used for parsing new style:
 *  <tcp|unix>:postgresql://server[:port][/db-name][?options]
 */
static int
parse_newstyle(int lineno, char *buf, char **host, char **dbname, char **port, char **options)
{
	int prefix_len;
	int connect_params = 0;
	char *p;
	char *start;
	char prevchar = '\0';
	bool is_unix = false;

	/* check specified protocol */
	if (strncmp(buf, "unix:", 5) == 0)
	{
		is_unix = true;
		prefix_len = 5;
	}
	else
		prefix_len = 4;

	/* check and skip the prefix */
	if (!strncmp(buf + prefix_len, "postgtresql://", strlen("postgresql://")))
		return -1;
	start = buf + prefix_len + strlen("postgresql://");
	p = start;


	/*
	 * Look for IPv6 address.
	 */
	if (*p == '[')
	{
		start = ++p;
		while (*p && *p != ']')
			++p;
		if (!*p)
		{
			ecpg_log("end of string reached when looking for matching \"]\" in IPv6 host address: \"%s\"\n", buf);
			return -1;
		}
		if (p == start)
		{
			ecpg_log("IPv6 host address may not be empty: \"%s\"\n", buf);
			return -1;
		}
		/* Cut off the bracket and advance */
		*(p++) = '\0';

		/*
		 * The address may be followed by a port specifier or a slash or a
		 * query.
		 */
		if (*p && *p != ':' && *p != '/' && *p != '?')
		{
			ecpg_log("unexpected character \"%c\" at position %d (expected \":\", \"/\" or \"?\"): \"%s]%s\"\n", *p, (int) (p - buf + 1), buf, p);
			return -1;
		}
	}
	else
	{
		/* Look ahead for possible user credentials designator */
		while (*p && *p != ':' && *p != '/' && *p != '?')
			++p;
	}

	/* Save the hostname terminator before we null it */
	prevchar = *p;
	*p = '\0';

	/* Duplicate string if TCP protocol is specified */
	if (!is_unix)
	{
		*host = ecpg_strdup(start, lineno);
		if (!(*host))
			return -1;
		connect_params++;
	}
	else
		/* this value is used only for error-reporting */
		*host = start;

	if (prevchar == ':')
	{
		/* port number is found */
		start = ++p;

		while (*p && *p != '/' && *p != '?')
			++p;

		prevchar = *p;
		*p = '\0';
		*port = ecpg_strdup(start, lineno);
		if (!(*port))
		{
			if (!is_unix)
				ecpg_free(*host);
			return -1;
		}
		connect_params++;
	}


	if (prevchar && prevchar != '?')
	{
		start = ++p;

		/* Look for query parameters */
		while (*p && *p != '?')
			++p;

		prevchar = *p;
		*p = '\0';

		if (strlen(start))
		{
			/* database name is specified */
			*dbname = ecpg_strdup(start, lineno);
			if (!(*dbname))
			{
				if (!is_unix)
					ecpg_free(*host);
				ecpg_free(*port);
				return -1;
			}
			connect_params++;
		}
	}

	if (prevchar)
	{
		start = ++p;
		if (strlen(start))
		{
			*options = ecpg_strdup(start, lineno);
			if (!(*options))
			{
				if (!is_unix)
					ecpg_free(*host);
				ecpg_free(*port);
				ecpg_free(*dbname);
				return -1;
			}
		}
	}

	if (is_unix)
	{
		if (strcmp(*host, "localhost") &&
			strcmp(*host, "127.0.0.1") &&
			strcmp(*host, "::1"))
		{
			/*
			 * The alternative of using "127.0.0.1" here is deprecated
			 * and undocumented; we'll keep it for backward
			 * compatibility's sake.
			 */
			ecpg_log("ECPGconnect: non-localhost access via sockets on line %d\n", lineno);
			ecpg_raise(lineno, ECPG_CONNECT, ECPG_SQLSTATE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION, *dbname ? *dbname : ecpg_gettext("<DEFAULT>"));
			if (*port)
				ecpg_free(*port);
			if (*options)
				ecpg_free(*options);
			if (*dbname)
				ecpg_free(*dbname);
			return -1;
		}
		/* If unix-domain is specified, host should be null */
		*host = NULL;
	}

	return connect_params;
}

/*
 * same as above, but used for oldstyle:
 *  database[@server][:port]
 */
static int
parse_oldstyle(int lineno, char *buf, char **host, char **dbname, char **port)
{
	int connect_params = 0;
	char *start = buf;
	char *p = start;
	char prevchar = '\0';

	/* Look ahead for possible user credentials designator */
	while (*p && *p != '@' && *p != ':')
		++p;

	/* Save the hostname terminator before we null it */
	prevchar = *p;
	*p = '\0';
	if (strlen(start))
	{
		*dbname = ecpg_strdup(start, lineno);
		if (!(*dbname))
			return -1;

		connect_params++;
	}

	if (prevchar == '@')
	{
		/* hostname is found */
		start = ++p;
		/*
		 * Look for IPv6 address.
		 */
		if (*p == '[')
		{
			start = ++p;
			while (*p && *p != ']')
				++p;
			if (!*p)
			{
				ecpg_log("end of string reached when looking for matching \"]\" in IPv6 host address: \"%s\"\n", buf);
				return -1;
			}
			if (p == start)
			{
				ecpg_log("IPv6 host address may not be empty: \"%s\"\n", buf);
				return -1;
			}
			/* Cut off the bracket and advance */
			*(p++) = '\0';

			/*
			* The address may be followed by a port specifier or a slash or a
			* query.
			*/
			if (*p && *p != ':')
			{
				ecpg_log("unexpected character \"%c\": \"%s]%s\"\n", *p, buf, p);
				return -1;
			}
		}
		else
		{
			while(*p && *p != ':')
				++p;

		}
		prevchar = *p;
		*p = '\0';
		if (strcmp(start, "localhost") && strcmp(start, "127.0.0.1"))
		{
			*host = ecpg_strdup(start, lineno);
			if (!(*host))
			{
				ecpg_free(*dbname);
				return -1;
			}
			connect_params++;
		}
	}

	if (prevchar == ':')
	{
		/* port number is found */
		start = ++p;
		*port = ecpg_strdup(start, lineno);
		if (!(*port))
		{
			ecpg_free(*dbname);
			ecpg_free(*host);
			return -1;
		}
		connect_params++;
	}

	return connect_params;
}

/*
 * parse option string.
 */
static int
parse_options(char *params, int current_params, const char **conn_keywords, const char **conn_values)
{
	int connect_params = current_params;
	while (*params)
	{
		/* blank must be skiped. */
		char       *keyword = *params == ' ' ? ++params : params;
		char       *value = NULL;
		char       *p = keyword;

		/*
		 * Scan the params string for '=' and '&', marking the end of keyword
		 * and value respectively.
		 */
		for (;;)
		{
			if (*p == '=')
			{
				/* Was there '=' already? */
				if (value != NULL)
				{
					ecpg_log("extra key/value separator \"=\" in query parameter: \"%s\"\n", keyword);
					return false;
				}
				/* Cut off keyword, advance to value */
				*p++ = '\0';
				value = p;
			}
			else if (*p == '&' || *p == '\0')
			{
				/*
				 * If not at the end, cut off value and advance; leave p
				 * pointing to start of the next parameter, if any.
				 */
				if (*p != '\0')
					*p++ = '\0';
				/* Was there '=' at all? */
				if (value == NULL)
				{
					ecpg_log("extra key/value separator \"=\" in query parameter: \"%s\"\n", keyword);
					return false;
				}
				/* Got keyword and value, go process them. */
				break;
			}
			else
				++p;				/* Advance over all other bytes. */
		}

		conn_keywords[connect_params] = keyword;
		conn_values[connect_params] = value;
		connect_params++;

		/* Proceed to next key=value pair, if any */
		params = p;
	}

	return connect_params;
}

/* this contains some quick hacks, needs to be cleaned up, but it works */
bool
ECPGconnect(int lineno, int c, const char *name, const char *user, const char *passwd, const char *connection_name, int autocommit)
{
	struct sqlca_t *sqlca = ECPGget_sqlca();
	enum COMPAT_MODE compat = c;
	struct connection *this;
	int			connect_params,
				conn_size = 0;
	char	   *buf = name ? ecpg_strdup(name, lineno) : NULL,
			   *host = NULL,
			   *port = NULL,
			   *dbname = NULL,
			   *options = NULL;
	const char **conn_keywords;
	const char **conn_values;

	if (sqlca == NULL)
	{
		ecpg_raise(lineno, ECPG_OUT_OF_MEMORY,
				   ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY, NULL);
		ecpg_free(buf);
		return false;
	}

	ecpg_init_sqlca(sqlca);

	/*
	 * clear auto_mem structure because some error handling functions might
	 * access it
	 */
	ecpg_clear_auto_mem();

	if (INFORMIX_MODE(compat))
	{
		char	   *envname;

		/*
		 * Informix uses an environment variable DBPATH that overrides the
		 * connection parameters given here. We do the same with PG_DBPATH as
		 * the syntax is different.
		 */
		envname = getenv("PG_DBPATH");
		if (envname)
		{
			ecpg_free(buf);
			buf = ecpg_strdup(envname, lineno);
		}

	}

	if (buf == NULL && connection_name == NULL)
		connection_name = "DEFAULT";

#if ENABLE_THREAD_SAFETY
	ecpg_pthreads_init();
#endif

	/* check if the identifier is unique */
	if (ecpg_get_connection(connection_name))
	{
		ecpg_free(buf);
		ecpg_log("ECPGconnect: connection identifier %s is already in use\n",
				 connection_name);
		return false;
	}

	if ((this = (struct connection *) ecpg_alloc(sizeof(struct connection), lineno)) == NULL)
	{
		ecpg_free(buf);
		return false;
	}

	if (buf != NULL)
	{
		/* get the detail information from buf */
		if (!strncmp(buf, "tcp:", 4)|| !strncmp(buf, "unix:", 5))
			conn_size = parse_newstyle(lineno, buf, &host, &dbname, &port, &options);
		else
			conn_size = parse_oldstyle(lineno, buf, &host, &dbname, &port);

		if (conn_size == -1)
		{
			free(this);
			return false;
		}

	}
	else
		dbname = NULL;

	/* add connection to our list */
#ifdef ENABLE_THREAD_SAFETY
	pthread_mutex_lock(&connections_mutex);
#endif
	if (connection_name != NULL)
		this->name = ecpg_strdup(connection_name, lineno);
	else
		this->name = ecpg_strdup(dbname, lineno);

	this->cache_head = NULL;
	this->prep_stmts = NULL;

	if (all_connections == NULL)
		this->next = NULL;
	else
		this->next = all_connections;

	all_connections = this;
#ifdef ENABLE_THREAD_SAFETY
	pthread_setspecific(actual_connection_key, all_connections);
#endif
	actual_connection = all_connections;

	ecpg_log("ECPGconnect: opening database %s on %s port %s %s%s %s%s\n",
			 dbname ? dbname : "<DEFAULT>",
			 host ? host : "<DEFAULT>",
			 port ? (ecpg_internal_regression_mode ? "<REGRESSION_PORT>" : port) : "<DEFAULT>",
			 options ? "with options " : "", options ? options : "",
			 (user && strlen(user) > 0) ? "for user " : "", user ? user : "");

	/* count options (this may produce an overestimate, it's ok) */
	if (options)
	{
		int i;
		for (i = 0; options[i]; i++)
			if (options[i] == '=')
				conn_size++;
	}

	if (user && strlen(user) > 0)
		conn_size++;
	if (passwd && strlen(passwd) > 0)
		conn_size++;

	/* allocate enough space for all connection parameters */
	conn_keywords = (const char **) ecpg_alloc((conn_size + 1) * sizeof(char *), lineno);
	conn_values = (const char **) ecpg_alloc(conn_size * sizeof(char *), lineno);
	if (conn_keywords == NULL || conn_values == NULL)
	{
		if (host)
			ecpg_free(host);
		if (port)
			ecpg_free(port);
		if (options)
			ecpg_free(options);
		if (dbname)
			ecpg_free(dbname);
		if (buf)
			ecpg_free(buf);
		if (conn_keywords)
			ecpg_free(conn_keywords);
		if (conn_values)
			ecpg_free(conn_values);
		free(this);
		return false;
	}

	connect_params = 0;
	if (dbname)
	{
		conn_keywords[connect_params] = "dbname";
		conn_values[connect_params] = dbname;
		connect_params++;
	}
	if (host)
	{
		conn_keywords[connect_params] = "host";
		conn_values[connect_params] = host;
		connect_params++;
	}
	if (port)
	{
		conn_keywords[connect_params] = "port";
		conn_values[connect_params] = port;
		connect_params++;
	}
	if (user && strlen(user) > 0)
	{
		conn_keywords[connect_params] = "user";
		conn_values[connect_params] = user;
		connect_params++;
	}
	if (passwd && strlen(passwd) > 0)
	{
		conn_keywords[connect_params] = "password";
		conn_values[connect_params] = passwd;
		connect_params++;
	}
	if (options)
		connect_params = parse_options(options, connect_params, conn_keywords, conn_values);


	Assert(connect_params <= conn_size);
	conn_keywords[connect_params] = NULL;	/* terminator */

	this->connection = PQconnectdbParams(conn_keywords, conn_values, 0);

	if (host)
		ecpg_free(host);
	if (port)
		ecpg_free(port);
	if (options)
		ecpg_free(options);
	if (buf)
		ecpg_free(buf);
	ecpg_free(conn_values);
	ecpg_free(conn_keywords);

	if (PQstatus(this->connection) == CONNECTION_BAD)
	{
		const char *errmsg = PQerrorMessage(this->connection);
		const char *db = dbname ? dbname : ecpg_gettext("<DEFAULT>");

		/* PQerrorMessage's result already has a trailing newline */
		ecpg_log("ECPGconnect: %s", errmsg);

		ecpg_finish(this);
#ifdef ENABLE_THREAD_SAFETY
		pthread_mutex_unlock(&connections_mutex);
#endif

		ecpg_raise(lineno, ECPG_CONNECT, ECPG_SQLSTATE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION, db);
		if (dbname)
			ecpg_free(dbname);

		return false;
	}

	if (dbname)
		ecpg_free(dbname);

#ifdef ENABLE_THREAD_SAFETY
	pthread_mutex_unlock(&connections_mutex);
#endif

	this->autocommit = autocommit;

	PQsetNoticeReceiver(this->connection, &ECPGnoticeReceiver, (void *) this);

	return true;
}

bool
ECPGdisconnect(int lineno, const char *connection_name)
{
	struct sqlca_t *sqlca = ECPGget_sqlca();
	struct connection *con;

	if (sqlca == NULL)
	{
		ecpg_raise(lineno, ECPG_OUT_OF_MEMORY,
				   ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY, NULL);
		return false;
	}

#ifdef ENABLE_THREAD_SAFETY
	pthread_mutex_lock(&connections_mutex);
#endif

	if (strcmp(connection_name, "ALL") == 0)
	{
		ecpg_init_sqlca(sqlca);
		for (con = all_connections; con;)
		{
			struct connection *f = con;

			con = con->next;
			ecpg_finish(f);
		}
	}
	else
	{
		con = ecpg_get_connection_nr(connection_name);

		if (!ecpg_init(con, connection_name, lineno))
		{
#ifdef ENABLE_THREAD_SAFETY
			pthread_mutex_unlock(&connections_mutex);
#endif
			return false;
		}
		else
			ecpg_finish(con);
	}

#ifdef ENABLE_THREAD_SAFETY
	pthread_mutex_unlock(&connections_mutex);
#endif

	return true;
}

PGconn *
ECPGget_PGconn(const char *connection_name)
{
	struct connection *con;

	con = ecpg_get_connection(connection_name);
	if (con == NULL)
		return NULL;

	return con->connection;
}
