/*-------------------------------------------------------------------------
 *
 * fe-auth-oauth.c
 *	   The front-end (client) implementation of OAuth/OIDC authentication.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-auth-oauth.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "common/base64.h"
#include "common/hmac.h"
#include "common/jsonapi.h"
#include "common/oauth-common.h"
#include "fe-auth.h"
#include "fe-auth-oauth.h"
#include "mb/pg_wchar.h"

/* The exported OAuth callback mechanism. */
static void *oauth_init(PGconn *conn, const char *password,
						const char *sasl_mechanism);
static SASLStatus oauth_exchange(void *opaq, bool final,
								 char *input, int inputlen,
								 char **output, int *outputlen);
static bool oauth_channel_bound(void *opaq);
static void oauth_free(void *opaq);

const pg_fe_sasl_mech pg_oauth_mech = {
	oauth_init,
	oauth_exchange,
	oauth_channel_bound,
	oauth_free,
};

static void *
oauth_init(PGconn *conn, const char *password,
		   const char *sasl_mechanism)
{
	fe_oauth_state *state;

	/*
	 * We only support one SASL mechanism here; anything else is programmer
	 * error.
	 */
	Assert(sasl_mechanism != NULL);
	Assert(!strcmp(sasl_mechanism, OAUTHBEARER_NAME));

	state = calloc(1, sizeof(*state));
	if (!state)
		return NULL;

	state->state = FE_OAUTH_INIT;
	state->conn = conn;

	return state;
}

#define kvsep "\x01"

static char *
client_initial_response(PGconn *conn, const char *token)
{
	static const char * const resp_format = "n,," kvsep "auth=%s" kvsep kvsep;

	PQExpBufferData buf;
	char	   *response = NULL;

	if (!token)
	{
		/*
		 * Either programmer error, or something went badly wrong during the
		 * asynchronous fetch.
		 *
		 * TODO: users shouldn't see this; what action should they take if they
		 * do?
		 */
		libpq_append_conn_error(conn, "no OAuth token was set for the connection");
		return NULL;
	}

	initPQExpBuffer(&buf);
	appendPQExpBuffer(&buf, resp_format, token);

	if (!PQExpBufferDataBroken(buf))
		response = strdup(buf.data);

	termPQExpBuffer(&buf);
	return response;
}

#define ERROR_STATUS_FIELD "status"
#define ERROR_SCOPE_FIELD "scope"
#define ERROR_OPENID_CONFIGURATION_FIELD "openid-configuration"

struct json_ctx
{
	char		   *errmsg; /* any non-NULL value stops all processing */
	PQExpBufferData errbuf; /* backing memory for errmsg */
	int				nested; /* nesting level (zero is the top) */

	const char	   *target_field_name; /* points to a static allocation */
	char		  **target_field;      /* see below */

	/* target_field, if set, points to one of the following: */
	char		   *status;
	char		   *scope;
	char		   *discovery_uri;
};

#define oauth_json_has_error(ctx) \
	(PQExpBufferDataBroken((ctx)->errbuf) || (ctx)->errmsg)

#define oauth_json_set_error(ctx, ...) \
	do { \
		appendPQExpBuffer(&(ctx)->errbuf, __VA_ARGS__); \
		(ctx)->errmsg = (ctx)->errbuf.data; \
	} while (0)

static JsonParseErrorType
oauth_json_object_start(void *state)
{
	struct json_ctx	   *ctx = state;

	if (ctx->target_field)
	{
		Assert(ctx->nested == 1);

		oauth_json_set_error(ctx,
							 libpq_gettext("field \"%s\" must be a string"),
							 ctx->target_field_name);
	}

	++ctx->nested;
	return oauth_json_has_error(ctx) ? JSON_SEM_ACTION_FAILED : JSON_SUCCESS;
}

static JsonParseErrorType
oauth_json_object_end(void *state)
{
	struct json_ctx	   *ctx = state;

	--ctx->nested;
	return JSON_SUCCESS;
}

static JsonParseErrorType
oauth_json_object_field_start(void *state, char *name, bool isnull)
{
	struct json_ctx	   *ctx = state;

	if (ctx->nested == 1)
	{
		if (!strcmp(name, ERROR_STATUS_FIELD))
		{
			ctx->target_field_name = ERROR_STATUS_FIELD;
			ctx->target_field = &ctx->status;
		}
		else if (!strcmp(name, ERROR_SCOPE_FIELD))
		{
			ctx->target_field_name = ERROR_SCOPE_FIELD;
			ctx->target_field = &ctx->scope;
		}
		else if (!strcmp(name, ERROR_OPENID_CONFIGURATION_FIELD))
		{
			ctx->target_field_name = ERROR_OPENID_CONFIGURATION_FIELD;
			ctx->target_field = &ctx->discovery_uri;
		}
	}

	free(name);
	return JSON_SUCCESS;
}

static JsonParseErrorType
oauth_json_array_start(void *state)
{
	struct json_ctx	   *ctx = state;

	if (!ctx->nested)
	{
		ctx->errmsg = libpq_gettext("top-level element must be an object");
	}
	else if (ctx->target_field)
	{
		Assert(ctx->nested == 1);

		oauth_json_set_error(ctx,
							 libpq_gettext("field \"%s\" must be a string"),
							 ctx->target_field_name);
	}

	return oauth_json_has_error(ctx) ? JSON_SEM_ACTION_FAILED : JSON_SUCCESS;
}

static JsonParseErrorType
oauth_json_scalar(void *state, char *token, JsonTokenType type)
{
	struct json_ctx	   *ctx = state;

	if (!ctx->nested)
	{
		ctx->errmsg = libpq_gettext("top-level element must be an object");
	}
	else if (ctx->target_field)
	{
		Assert(ctx->nested == 1);

		if (type == JSON_TOKEN_STRING)
		{
			*ctx->target_field = token;

			ctx->target_field = NULL;
			ctx->target_field_name = NULL;

			return JSON_SUCCESS; /* don't free the token we're using */
		}

		oauth_json_set_error(ctx,
							 libpq_gettext("field \"%s\" must be a string"),
							 ctx->target_field_name);
	}

	free(token);
	return oauth_json_has_error(ctx) ? JSON_SEM_ACTION_FAILED : JSON_SUCCESS;
}

static bool
handle_oauth_sasl_error(PGconn *conn, char *msg, int msglen)
{
	JsonLexContext		lex = {0};
	JsonSemAction		sem = {0};
	JsonParseErrorType	err;
	struct json_ctx		ctx = {0};
	char			   *errmsg = NULL;

	/* Sanity check. */
	if (strlen(msg) != msglen)
	{
		appendPQExpBufferStr(&conn->errorMessage,
							 libpq_gettext("server's error message contained an embedded NULL"));
		return false;
	}

	initJsonLexContextCstringLen(&lex, msg, msglen, PG_UTF8, true);

	initPQExpBuffer(&ctx.errbuf);
	sem.semstate = &ctx;

	sem.object_start = oauth_json_object_start;
	sem.object_end = oauth_json_object_end;
	sem.object_field_start = oauth_json_object_field_start;
	sem.array_start = oauth_json_array_start;
	sem.scalar = oauth_json_scalar;

	err = pg_parse_json(&lex, &sem);

	if (err == JSON_SEM_ACTION_FAILED)
	{
		if (PQExpBufferDataBroken(ctx.errbuf))
			errmsg = libpq_gettext("out of memory");
		else if (ctx.errmsg)
			errmsg = ctx.errmsg;
		else
		{
			/*
			 * Developer error: one of the action callbacks didn't call
			 * oauth_json_set_error() before erroring out.
			 */
			Assert(oauth_json_has_error(&ctx));
			errmsg = "<unexpected empty error>";
		}
	}
	else if (err != JSON_SUCCESS)
		errmsg = json_errdetail(err, &lex);

	if (errmsg)
		appendPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("failed to parse server's error response: %s"),
						  errmsg);

	/* Don't need the error buffer or the JSON lexer anymore. */
	termPQExpBuffer(&ctx.errbuf);
	termJsonLexContext(&lex);

	if (errmsg)
		return false;

	/* TODO: what if these override what the user already specified? */
	if (ctx.discovery_uri)
	{
		if (conn->oauth_discovery_uri)
			free(conn->oauth_discovery_uri);

		conn->oauth_discovery_uri = ctx.discovery_uri;
	}

	if (ctx.scope)
	{
		if (conn->oauth_scope)
			free(conn->oauth_scope);

		conn->oauth_scope = ctx.scope;
	}
	/* TODO: missing error scope should clear any existing connection scope */

	if (!ctx.status)
	{
		appendPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("server sent error response without a status"));
		return false;
	}

	if (!strcmp(ctx.status, "invalid_token"))
	{
		/*
		 * invalid_token is the only error code we'll automatically retry for,
		 * but only if we have enough information to do so.
		 */
		if (conn->oauth_discovery_uri)
			conn->oauth_want_retry = true;
	}
	/* TODO: include status in hard failure message */

	return true;
}

static void
free_request(PGconn *conn, void *vreq)
{
	PQoauthBearerRequest *request = vreq;

	if (request->cleanup)
		request->cleanup(conn, request);

	free(request);
}

static PostgresPollingStatusType
run_user_oauth_flow(PGconn *conn, pgsocket *altsock)
{
	fe_oauth_state *state = conn->sasl_state;
	PQoauthBearerRequest *request = state->async_ctx;
	PostgresPollingStatusType status;

	if (!request->async)
	{
		libpq_append_conn_error(conn, "user-defined OAuth flow provided neither a token nor an async callback");
		return PGRES_POLLING_FAILED;
	}

	status = request->async(conn, request, altsock);
	if (status == PGRES_POLLING_FAILED)
	{
		libpq_append_conn_error(conn, "user-defined OAuth flow failed");
		return status;
	}
	else if (status == PGRES_POLLING_OK)
	{
		/*
		 * We already have a token, so copy it into the state. (We can't
		 * hold onto the original string, since it may not be safe for us to
		 * free() it.)
		 */
		PQExpBufferData	token;

		if (!request->token)
		{
			libpq_append_conn_error(conn, "user-defined OAuth flow did not provide a token");
			return PGRES_POLLING_FAILED;
		}

		initPQExpBuffer(&token);
		appendPQExpBuffer(&token, "Bearer %s", request->token);

		if (PQExpBufferDataBroken(token))
		{
			libpq_append_conn_error(conn, "out of memory");
			return PGRES_POLLING_FAILED;
		}

		state->token = token.data;
		return PGRES_POLLING_OK;
	}

	/* TODO: what if no altsock was set? */
	return status;
}

static bool
setup_token_request(PGconn *conn, fe_oauth_state *state)
{
	int			res;
	PQoauthBearerRequest request = {
		.openid_configuration = conn->oauth_discovery_uri,
		.scope = conn->oauth_scope,
	};

	Assert(request.openid_configuration);

	/* The client may have overridden the OAuth flow. */
	res = PQauthDataHook(PQAUTHDATA_OAUTH_BEARER_TOKEN, conn, &request);
	if (res > 0)
	{
		PQoauthBearerRequest *request_copy;

		if (request.token)
		{
			/*
			 * We already have a token, so copy it into the state. (We can't
			 * hold onto the original string, since it may not be safe for us to
			 * free() it.)
			 */
			PQExpBufferData	token;

			initPQExpBuffer(&token);
			appendPQExpBuffer(&token, "Bearer %s", request.token);

			if (PQExpBufferDataBroken(token))
			{
				libpq_append_conn_error(conn, "out of memory");
				goto fail;
			}

			state->token = token.data;

			/* short-circuit */
			if (request.cleanup)
				request.cleanup(conn, &request);
			return true;
		}

		request_copy = malloc(sizeof(*request_copy));
		if (!request_copy)
		{
			libpq_append_conn_error(conn, "out of memory");
			goto fail;
		}

		memcpy(request_copy, &request, sizeof(request));

		conn->async_auth = run_user_oauth_flow;
		state->async_ctx = request_copy;
		state->free_async_ctx = free_request;
	}
	else if (res < 0)
	{
		libpq_append_conn_error(conn, "user-defined OAuth flow failed");
		goto fail;
	}
	else
	{
		/* Use our built-in OAuth flow. */
		conn->async_auth = pg_fe_run_oauth_flow;
	}

	return true;

fail:
	if (request.cleanup)
		request.cleanup(conn, &request);
	return false;
}

static bool
derive_discovery_uri(PGconn *conn)
{
	PQExpBufferData discovery_buf;

	if (conn->oauth_discovery_uri || !conn->oauth_issuer)
	{
		/*
		 * Either we already have one, or we aren't able to derive one
		 * ourselves. The latter case is not an error condition; we'll just ask
		 * the server to provide one for us.
		 */
		return true;
	}

	initPQExpBuffer(&discovery_buf);

	Assert(!conn->oauth_discovery_uri);
	Assert(conn->oauth_issuer);

	/*
	 * If we don't yet have a discovery URI, but the user gave us an explicit
	 * issuer, use the .well-known discovery URI for that issuer.
	 */
	appendPQExpBufferStr(&discovery_buf, conn->oauth_issuer);
	appendPQExpBufferStr(&discovery_buf, "/.well-known/openid-configuration");

	if (PQExpBufferDataBroken(discovery_buf))
		goto cleanup;

	conn->oauth_discovery_uri = strdup(discovery_buf.data);

cleanup:
	termPQExpBuffer(&discovery_buf);
	return (conn->oauth_discovery_uri != NULL);
}

static SASLStatus
oauth_exchange(void *opaq, bool final,
			   char *input, int inputlen,
			   char **output, int *outputlen)
{
	fe_oauth_state *state = opaq;
	PGconn	   *conn = state->conn;

	*output = NULL;
	*outputlen = 0;

	switch (state->state)
	{
		case FE_OAUTH_INIT:
			Assert(inputlen == -1);

			if (!derive_discovery_uri(conn))
				return SASL_FAILED;

			if (conn->oauth_discovery_uri)
			{
				if (!conn->oauth_client_id)
				{
					/* We can't talk to a server without a client identifier. */
					libpq_append_conn_error(conn, "no oauth_client_id is set for the connection");
					return SASL_FAILED;
				}

				/*
				 * Decide whether we're using a user-provided OAuth flow, or the
				 * one we have built in.
				 */
				if (!setup_token_request(conn, state))
					return SASL_FAILED;

				if (state->token)
				{
					/*
					 * A really smart user implementation may have already given
					 * us the token (e.g. if there was an unexpired copy already
					 * cached). In that case, we can just fall through.
					 */
				}
				else
				{
					/*
					 * Otherwise, we have to hand the connection over to our
					 * OAuth implementation. This involves a number of HTTP
					 * connections and timed waits, so we escape the synchronous
					 * auth processing and tell PQconnectPoll to transfer
					 * control to our async implementation.
					 */
					Assert(conn->async_auth); /* should have been set already */
					state->state = FE_OAUTH_REQUESTING_TOKEN;
					return SASL_ASYNC;
				}
			}
			else
			{
				/*
				 * If we don't have a discovery URI to be able to request a
				 * token, we ask the server for one explicitly with an empty
				 * token. This doesn't require any asynchronous work.
				 */
				state->token = strdup("");
				if (!state->token)
				{
					libpq_append_conn_error(conn, "out of memory");
					return SASL_FAILED;
				}
			}

			/* fall through */

		case FE_OAUTH_REQUESTING_TOKEN:
			/* We should still be in the initial response phase. */
			Assert(inputlen == -1);

			*output = client_initial_response(conn, state->token);
			if (!*output)
				return SASL_FAILED;

			*outputlen = strlen(*output);
			state->state = FE_OAUTH_BEARER_SENT;

			return SASL_CONTINUE;

		case FE_OAUTH_BEARER_SENT:
			if (final)
			{
				/* TODO: ensure there is no message content here. */
				return SASL_COMPLETE;
			}

			/*
			 * Error message sent by the server.
			 */
			if (!handle_oauth_sasl_error(conn, input, inputlen))
				return SASL_FAILED;

			/*
			 * Respond with the required dummy message (RFC 7628, sec. 3.2.3).
			 */
			*output = strdup(kvsep);
			*outputlen = strlen(*output); /* == 1 */

			state->state = FE_OAUTH_SERVER_ERROR;
			return SASL_CONTINUE;

		case FE_OAUTH_SERVER_ERROR:
			/*
			 * After an error, the server should send an error response to fail
			 * the SASL handshake, which is handled in higher layers.
			 *
			 * If we get here, the server either sent *another* challenge which
			 * isn't defined in the RFC, or completed the handshake successfully
			 * after telling us it was going to fail. Neither is acceptable.
			 */
			appendPQExpBufferStr(&conn->errorMessage,
								 libpq_gettext("server sent additional OAuth data after error\n"));
			return SASL_FAILED;

		default:
			appendPQExpBufferStr(&conn->errorMessage,
								 libpq_gettext("invalid OAuth exchange state\n"));
			break;
	}

	return SASL_FAILED;
}

static bool
oauth_channel_bound(void *opaq)
{
	/* This mechanism does not support channel binding. */
	return false;
}

static void
oauth_free(void *opaq)
{
	fe_oauth_state *state = opaq;

	free(state->token);
	if (state->async_ctx)
		state->free_async_ctx(state->conn, state->async_ctx);

	free(state);
}
