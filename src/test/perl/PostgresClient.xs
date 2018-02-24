/**********************************************************************
 * PostgresClient.xs
 *
 * Simple client interface for perl
 *
 *    src/test/perl/PostgresClient.xs
 *
 **********************************************************************/
#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

/* conflicts with the same symbol defined by postgres_fe.h */
#undef _

#include "libpq-fe.h"
#include "const-c.inc"

typedef struct clientobj
{
	char *name;
	PGconn *conn;
	char *notice;
} clientobj;

static clientobj *getclientobj(SV *connsvrv, int ignore_err);
static void PgClientNoticeProcessor(void *clientobj, const char *message);

static clientobj *
getclientobj(SV *connsvrv, int ignore_err)
{
	SV		*connsvsv = SvRV(connsvrv);

	if (!sv_isobject(connsvrv) || !sv_isa(connsvrv, "PostgresClient"))
	{
		if (!ignore_err)
			croak("unexpected parameter");
	}

	return (clientobj *) SvIV(connsvsv);
}

static void
PgClientNoticeProcessor(void *clobj, const char *message)
{
	clientobj  *obj = (clientobj *) clobj;
	char	   *notice = obj->notice;
	int			len = 0;

	if (notice)
		len = strlen(notice);
	len += strlen(message);
	obj->notice = malloc(len + 1);
	obj->notice[0] = 0;
	if (notice)
	{
		strcpy(obj->notice, notice);
		free(notice);
	}
	strcat(obj->notice, message);
}

MODULE = PostgresClient		PACKAGE = PostgresClient
INCLUDE: const-xs.inc
PROTOTYPES: ENABLE

=pod

=item PostgresClient::connectdb(name, dbname[, params...])

Create a new connection as specified.

name: the name of this connection
dbname: the name of the database to connect
        this can be a connection string but the behavior is not defined
        when params is specified together.
params: reference to connection parameter hash or PostgresNode object.
=cut

SV *
connectdb(name, dbname, ...)
	char *name;
	char *dbname;
  CODE:
	PGconn *conn;
    clientobj *obj;
	SV   *options_sv;
	char *connstr;
	const char **keywords = NULL;
	const char **values = NULL;
	int nparams = 0;

	if (items < 1)
		croak("Usage: PostgresClient->connectdb(name, dbname, options|node)");

	/* build parameter list for PQconnectdbParmas() */
	if (items >= 3)
	{
		options_sv  = ST(2);

		if (sv_isobject(options_sv))
		{
			PQconninfoOption *options;
			PQconninfoOption *option;
			char *errmsg;
			int i;

			/* ask PostgresNode for connection string */
			if (!sv_isa(options_sv, "PostgresNode"))
				croak("node is not a PostgresNode object");

			PUSHMARK(SP);
			XPUSHs(options_sv);
			XPUSHs(sv_2mortal(newSVpv(dbname, 0)));
			PUTBACK;
			if (call_method("connstr", G_SCALAR) != 1)
				croak("failed to call PostgresNode::connstr");
			connstr = SvPV_nolen(POPs);

			options = PQconninfoParse(connstr, &errmsg);

			if (!options)
				croak("No options?");

			for (i = 0, option = options ; option->keyword  ; option++)
			{
				if (option->val)
					i++;
			}
			i += 2;  /* room for dbname and terminator */

			keywords = (const char **) malloc(sizeof(char *) * i);
			values = (const char **) malloc(sizeof(char *) * i);

			for (i = 0, option = options ; option->keyword ; option++)
			{
				if (!option->val || strcmp(option->keyword, "dbname") == 0)
					continue;

				keywords[i] = strdup(option->keyword);
				if (option->val)
					values[i] = strdup(option->val);
				else
					values[i] = NULL;
				i++;
			}
			PQconninfoFree(options);
			nparams = i;
		}
		else if (SvROK(options_sv) && SvTYPE(SvRV(options_sv)) == SVt_PVHV)
		{
			HV *params_hv = (HV *) SvRV(options_sv);
			HE *hent;
			int i;

			nparams = hv_iterinit(params_hv) + 2;
			keywords = (const char **) malloc(sizeof(char *) * nparams);
			values = (const char **) malloc(sizeof(char *) * nparams);

			i = 0;
			while ((hent = hv_iternext(params_hv)) != NULL)
			{
				I32 keylen;
				STRLEN vallen;
				SV *valsv;
				char *keystr, *valstr;

				keystr = hv_iterkey(hent, &keylen);

				/* ignore dbname */
				if (strncmp(keystr, "dbname", keylen) == 0)
					continue;

				valsv = hv_iterval(params_hv, hent);
				if (SvOK(valsv))
				{
					keywords[i] = strndup(keystr, keylen);
					valstr = SvPV(valsv, vallen);
					values[i] = strndup(valstr, vallen);
					i++;
				}
			}
			nparams = i;
		}
		else
			croak("Invalid paralmeter options");
	}
	else
	{
		keywords = (const char **) malloc(sizeof(char *) * 2);
		values = (const char **) malloc(sizeof(char *) * 2);
	}

	keywords[nparams] = strndup("dbname", 6);
	values[nparams] = strdup(dbname);
	keywords[++nparams] = 0;

	/* Connect using the parameters */
	conn = PQconnectdbParams(keywords, values, true);
	if (!conn)
		croak("connection failure");
	if (PQstatus(conn) == CONNECTION_BAD)
		croak("connection failure: %s", PQerrorMessage(conn));

	obj = malloc(sizeof(clientobj));
	obj->name = strdup(name);
	obj->conn = conn;
	obj->notice = NULL;

	PQsetNoticeProcessor(conn, PgClientNoticeProcessor, (void *)obj);
	RETVAL = sv_setref_pv(newSV(0), "PostgresClient", (void *) obj);

  OUTPUT:
	RETVAL

=pod

=item $client->name()

Get the name of this connection.
=cut

char *
name(connsvrv)
  CODE:
	RETVAL = getclientobj(ST(0), 0)->name;

  OUTPUT:
	RETVAL

=pod

=item $client->db()

Get the database name of the connection.
=cut

char *
db(connsvrv)
  CODE:
	PGconn *conn = getclientobj(ST(0), 0)->conn;
	RETVAL = PQdb(conn);

  OUTPUT:
	RETVAL

=pod

=item $client->user()

Get the user name of the connection.
=cut

char *
user(connsvrv)
  CODE:
	PGconn *conn = getclientobj(ST(0), 0)->conn;
	RETVAL = PQuser(conn);

  OUTPUT:
	RETVAL

=pod

=item $client->pass()

Get the password of the connection.
=cut

char *
pass(connsvrv)
  CODE:
	PGconn *conn = getclientobj(ST(0), 0)->conn;
	RETVAL = PQpass(conn);

  OUTPUT:
	RETVAL

=pod

=item $client->host()

Get the server host name of the connection.
=cut

char *
host(connsvrv)
  CODE:
	PGconn *conn = getclientobj(ST(0), 0)->conn;
	RETVAL = PQhost(conn);

  OUTPUT:
	RETVAL

=pod

=item $client->port()

Get the port of the connection.
=cut

char *
port(connsvrv)
  CODE:
	PGconn *conn = getclientobj(ST(0), 0)->conn;
	RETVAL = PQport(conn);

  OUTPUT:
	RETVAL

=pod

=item $client->notice()

Get the notice messages accumulated in the connection.
=cut

char *
notice(connsvrv)
  CODE:
	clientobj *obj = getclientobj(ST(0), 0);
	if (obj->notice)
		RETVAL = strdup(obj->notice);
	else
		RETVAL = NULL;

  OUTPUT:
	RETVAL

=pod

=item $client->clear_notice()

Clear the notice messages of the connection.
=cut

void
clear_notice(connsvrv)
  CODE:
	clientobj *obj = getclientobj(ST(0), 0);
	if (obj->notice)
	{
		free(obj->notice);
		obj->notice = NULL;
	}

=pod

=item $client->status()

Get the status of the connection.
=cut

int
status(connsvrv)
  CODE:
	PGconn *conn = getclientobj(ST(0), 0)->conn;

	RETVAL = PQstatus(conn);

  OUTPUT:
	RETVAL

=pod

=item $client->transactionStatus()

Get the transaction status of the connection.
=cut

int
transactionStatus(connsvrv)
  CODE:
	PGconn *conn = getclientobj(ST(0), 0)->conn;

	RETVAL = PQtransactionStatus(conn);

  OUTPUT:
	RETVAL

=pod

=item $client->errorMessage()

Get the error message of the connection.
=cut

char *
errorMessage(connsvrv)
  CODE:
	PGconn *conn = getclientobj(ST(0), 0)->conn;

	RETVAL = PQerrorMessage(conn);

  OUTPUT:
	RETVAL

=pod

=item $client->finish()

Properly close the connection.
=cut

void
finish(connsvrv)
  CODE:
	SV *connsvrv = ST(0);
	SV *connivsv = SvRV(connsvrv);
	clientobj *obj;

	if (!sv_isobject(connsvrv) || !sv_isa(connsvrv, "PostgresClient"))
		croak("unexpected parameter");
	obj = (clientobj *) SvIV(connivsv);
	if (obj)
	{
		PQfinish(obj->conn);
		free(obj->name);
		if (obj->notice)
			free(obj->notice);
		free(obj);
		sv_setiv(connivsv, 0);
	}

void
DESTROY(connsvrv)
  CODE:
	SV *connsvrv = ST(0);

	/* Silently ignore unexpected parameters */
	if (sv_isobject(connsvrv) && sv_isa(connsvrv, "PostgresClient"))
	{
		clientobj *obj = (clientobj *) SvIV(SvRV(connsvrv));
		if (obj)
		{
			if (obj->conn)
				PQfinish(obj->conn);
			free(obj->name);
			if (obj->notice)
				free(obj->notice);
			free(obj);
		}
	}


=pod

=item $client->exec()

Execute a query and return the result.
=cut

SV *
exec(connsvrv, query)
	char *query;
  CODE:
	PGconn *conn = getclientobj(ST(0), 0)->conn;
	PGresult *res;

	if (!conn)
		croak("connection closed");

	res = PQexec(conn, query);

	if (res)
		RETVAL = sv_setref_pv(newSV(0), "PgResult", (void *) res);
	else
		RETVAL = &PL_sv_undef;

  OUTPUT:
	RETVAL
