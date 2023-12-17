/*-------------------------------------------------------------------------
 *
 * fe-auth-oauth-iddawc.c
 *	   The libiddawc implementation of OAuth/OIDC authentication.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-auth-oauth-iddawc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <iddawc.h>

#include "fe-auth.h"
#include "fe-auth-oauth.h"
#include "libpq-int.h"

#ifdef HAVE_I_LOAD_OPENID_CONFIG
/* Older versions of iddawc used 'load' instead of 'get' for some APIs. */
#define i_get_openid_config i_load_openid_config
#endif

static const char *
iddawc_error_string(int errcode)
{
	switch (errcode)
	{
		case I_OK:
			return "I_OK";

		case I_ERROR:
			return "I_ERROR";

		case I_ERROR_PARAM:
			return "I_ERROR_PARAM";

		case I_ERROR_MEMORY:
			return "I_ERROR_MEMORY";

		case I_ERROR_UNAUTHORIZED:
			return "I_ERROR_UNAUTHORIZED";

		case I_ERROR_SERVER:
			return "I_ERROR_SERVER";
	}

	return "<unknown>";
}

static void
iddawc_error(PGconn *conn, int errcode, const char *msg)
{
	appendPQExpBufferStr(&conn->errorMessage, libpq_gettext(msg));
	appendPQExpBuffer(&conn->errorMessage,
					  libpq_gettext(" (iddawc error %s)\n"),
					  iddawc_error_string(errcode));
}

static void
iddawc_request_error(PGconn *conn, struct _i_session *i, int err, const char *msg)
{
	const char *error_code;
	const char *desc;

	appendPQExpBuffer(&conn->errorMessage, "%s: ", libpq_gettext(msg));

	error_code = i_get_str_parameter(i, I_OPT_ERROR);
	if (!error_code)
	{
		/*
		 * The server didn't give us any useful information, so just print the
		 * error code.
		 */
		appendPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("(iddawc error %s)\n"),
						  iddawc_error_string(err));
		return;
	}

	/* If the server gave a string description, print that too. */
	desc = i_get_str_parameter(i, I_OPT_ERROR_DESCRIPTION);
	if (desc)
		appendPQExpBuffer(&conn->errorMessage, "%s ", desc);

	appendPQExpBuffer(&conn->errorMessage, "(%s)\n", error_code);
}

/*
 * Runs the device authorization flow using libiddawc. If successful, a malloc'd
 * token string in "Bearer xxxx..." format, suitable for sending to an
 * OAUTHBEARER server, is returned. NULL is returned on error.
 */
static char *
run_iddawc_auth_flow(PGconn *conn, const char *discovery_uri)
{
	struct _i_session session;
	PQExpBuffer	token_buf = NULL;
	int			err;
	int			auth_method;
	bool		user_prompted = false;
	const char *verification_uri;
	const char *user_code;
	const char *access_token;
	const char *token_type;
	char	   *token = NULL;

	i_init_session(&session);

	token_buf = createPQExpBuffer();
	if (!token_buf)
		goto cleanup;

	err = i_set_str_parameter(&session, I_OPT_OPENID_CONFIG_ENDPOINT, discovery_uri);
	if (err)
	{
		iddawc_error(conn, err, "failed to set OpenID config endpoint");
		goto cleanup;
	}

	err = i_get_openid_config(&session);
	if (err)
	{
		iddawc_error(conn, err, "failed to fetch OpenID discovery document");
		goto cleanup;
	}

	if (!i_get_str_parameter(&session, I_OPT_TOKEN_ENDPOINT))
	{
		appendPQExpBufferStr(&conn->errorMessage,
							 libpq_gettext("issuer has no token endpoint"));
		goto cleanup;
	}

	if (!i_get_str_parameter(&session, I_OPT_DEVICE_AUTHORIZATION_ENDPOINT))
	{
		appendPQExpBufferStr(&conn->errorMessage,
							 libpq_gettext("issuer does not support device authorization"));
		goto cleanup;
	}

	err = i_set_response_type(&session, I_RESPONSE_TYPE_DEVICE_CODE);
	if (err)
	{
		iddawc_error(conn, err, "failed to set device code response type");
		goto cleanup;
	}

	auth_method = I_TOKEN_AUTH_METHOD_NONE;
	if (conn->oauth_client_secret && *conn->oauth_client_secret)
		auth_method = I_TOKEN_AUTH_METHOD_SECRET_BASIC;

	err = i_set_parameter_list(&session,
		I_OPT_CLIENT_ID, conn->oauth_client_id,
		I_OPT_CLIENT_SECRET, conn->oauth_client_secret,
		I_OPT_TOKEN_METHOD, auth_method,
		I_OPT_SCOPE, conn->oauth_scope,
		I_OPT_NONE
	);
	if (err)
	{
		iddawc_error(conn, err, "failed to set client identifier");
		goto cleanup;
	}

	err = i_run_device_auth_request(&session);
	if (err)
	{
		iddawc_request_error(conn, &session, err,
							"failed to obtain device authorization");
		goto cleanup;
	}

	verification_uri = i_get_str_parameter(&session, I_OPT_DEVICE_AUTH_VERIFICATION_URI);
	if (!verification_uri)
	{
		appendPQExpBufferStr(&conn->errorMessage,
							 libpq_gettext("issuer did not provide a verification URI"));
		goto cleanup;
	}

	user_code = i_get_str_parameter(&session, I_OPT_DEVICE_AUTH_USER_CODE);
	if (!user_code)
	{
		appendPQExpBufferStr(&conn->errorMessage,
							 libpq_gettext("issuer did not provide a user code"));
		goto cleanup;
	}

	/*
	 * Poll the token endpoint until either the user logs in and authorizes the
	 * use of a token, or a hard failure occurs. We perform one ping _before_
	 * prompting the user, so that we don't make them do the work of logging in
	 * only to find that the token endpoint is completely unreachable.
	 */
	err = i_run_token_request(&session);
	while (err)
	{
		const char *error_code;
		uint		interval;

		error_code = i_get_str_parameter(&session, I_OPT_ERROR);

		/*
		 * authorization_pending and slow_down are the only acceptable errors;
		 * anything else and we bail.
		 */
		if (!error_code || (strcmp(error_code, "authorization_pending")
							&& strcmp(error_code, "slow_down")))
		{
			iddawc_request_error(conn, &session, err,
								"failed to obtain access token");
			goto cleanup;
		}

		if (!user_prompted)
		{
			int			res;
			PQpromptOAuthDevice prompt = {
				.verification_uri = verification_uri,
				.user_code = user_code,
				/* TODO: optional fields */
			};

			/*
			 * Now that we know the token endpoint isn't broken, give the user
			 * the login instructions.
			 */
			res = PQauthDataHook(PQAUTHDATA_PROMPT_OAUTH_DEVICE, conn,
								 &prompt);

			if (!res)
			{
				fprintf(stderr, "Visit %s and enter the code: %s",
						prompt.verification_uri, prompt.user_code);
			}
			else if (res < 0)
			{
				appendPQExpBufferStr(&conn->errorMessage,
									 libpq_gettext("device prompt failed\n"));
				goto cleanup;
			}

			user_prompted = true;
		}

		/*
		 * We are required to wait between polls; the server tells us how long.
		 * TODO: if interval's not set, we need to default to five seconds
		 * TODO: sanity check the interval
		 */
		interval = i_get_int_parameter(&session, I_OPT_DEVICE_AUTH_INTERVAL);

		/*
		 * A slow_down error requires us to permanently increase our retry
		 * interval by five seconds. RFC 8628, Sec. 3.5.
		 */
		if (!strcmp(error_code, "slow_down"))
		{
			interval += 5;
			i_set_int_parameter(&session, I_OPT_DEVICE_AUTH_INTERVAL, interval);
		}

		sleep(interval);

		/*
		 * XXX Reset the error code before every call, because iddawc won't do
		 * that for us. This matters if the server first sends a "pending" error
		 * code, then later hard-fails without sending an error code to
		 * overwrite the first one.
		 *
		 * That we have to do this at all seems like a bug in iddawc.
		 */
		i_set_str_parameter(&session, I_OPT_ERROR, NULL);

		err = i_run_token_request(&session);
	}

	access_token = i_get_str_parameter(&session, I_OPT_ACCESS_TOKEN);
	token_type = i_get_str_parameter(&session, I_OPT_TOKEN_TYPE);

	if (!access_token || !token_type || strcasecmp(token_type, "Bearer"))
	{
		appendPQExpBufferStr(&conn->errorMessage,
							 libpq_gettext("issuer did not provide a bearer token"));
		goto cleanup;
	}

	appendPQExpBufferStr(token_buf, "Bearer ");
	appendPQExpBufferStr(token_buf, access_token);

	if (PQExpBufferBroken(token_buf))
		goto cleanup;

	token = strdup(token_buf->data);

cleanup:
	if (token_buf)
		destroyPQExpBuffer(token_buf);
	i_clean_session(&session);

	return token;
}

PostgresPollingStatusType
pg_fe_run_oauth_flow(PGconn *conn, pgsocket *altsock)
{
	fe_oauth_state *state = conn->sasl_state;

	/* TODO: actually make this asynchronous */
	state->token = run_iddawc_auth_flow(conn, conn->oauth_discovery_uri);
	return state->token ? PGRES_POLLING_OK : PGRES_POLLING_FAILED;
}
