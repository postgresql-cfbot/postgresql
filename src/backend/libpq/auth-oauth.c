/*-------------------------------------------------------------------------
 *
 * auth-oauth.c
 *	  Server-side implementation of the SASL OAUTHBEARER mechanism.
 *
 * See the following RFC for more details:
 * - RFC 7628: https://tools.ietf.org/html/rfc7628
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/libpq/auth-oauth.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>

#include "common/oauth-common.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "libpq/auth.h"
#include "libpq/hba.h"
#include "libpq/oauth.h"
#include "libpq/sasl.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "utils/json.h"

/* GUC */
char	   *OAuthValidatorLibrary = "";

static void oauth_get_mechanisms(Port *port, StringInfo buf);
static void *oauth_init(Port *port, const char *selected_mech, const char *shadow_pass);
static int	oauth_exchange(void *opaq, const char *input, int inputlen,
						   char **output, int *outputlen, const char **logdetail);

static void load_validator_library(void);
static void shutdown_validator_library(int code, Datum arg);

static ValidatorModuleState *validator_module_state;
static const OAuthValidatorCallbacks *ValidatorCallbacks;

/* Mechanism declaration */
const pg_be_sasl_mech pg_be_oauth_mech = {
	oauth_get_mechanisms,
	oauth_init,
	oauth_exchange,

	PG_MAX_AUTH_TOKEN_LENGTH,
};


typedef enum
{
	OAUTH_STATE_INIT = 0,
	OAUTH_STATE_ERROR,
	OAUTH_STATE_FINISHED,
} oauth_state;

struct oauth_ctx
{
	oauth_state state;
	Port	   *port;
	const char *issuer;
	const char *scope;
};

static char *sanitize_char(char c);
static char *parse_kvpairs_for_auth(char **input);
static void generate_error_response(struct oauth_ctx *ctx, char **output, int *outputlen);
static bool validate(Port *port, const char *auth);

#define KVSEP 0x01
#define AUTH_KEY "auth"
#define BEARER_SCHEME "Bearer "

static void
oauth_get_mechanisms(Port *port, StringInfo buf)
{
	/* Only OAUTHBEARER is supported. */
	appendStringInfoString(buf, OAUTHBEARER_NAME);
	appendStringInfoChar(buf, '\0');
}

static void *
oauth_init(Port *port, const char *selected_mech, const char *shadow_pass)
{
	struct oauth_ctx *ctx;

	if (strcmp(selected_mech, OAUTHBEARER_NAME))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("client selected an invalid SASL authentication mechanism")));

	ctx = palloc0(sizeof(*ctx));

	ctx->state = OAUTH_STATE_INIT;
	ctx->port = port;

	Assert(port->hba);
	ctx->issuer = port->hba->oauth_issuer;
	ctx->scope = port->hba->oauth_scope;

	load_validator_library();

	return ctx;
}

static int
oauth_exchange(void *opaq, const char *input, int inputlen,
			   char **output, int *outputlen, const char **logdetail)
{
	char	   *p;
	char		cbind_flag;
	char	   *auth;

	struct oauth_ctx *ctx = opaq;

	*output = NULL;
	*outputlen = -1;

	/*
	 * If the client didn't include an "Initial Client Response" in the
	 * SASLInitialResponse message, send an empty challenge, to which the
	 * client will respond with the same data that usually comes in the
	 * Initial Client Response.
	 */
	if (input == NULL)
	{
		Assert(ctx->state == OAUTH_STATE_INIT);

		*output = pstrdup("");
		*outputlen = 0;
		return PG_SASL_EXCHANGE_CONTINUE;
	}

	/*
	 * Check that the input length agrees with the string length of the input.
	 */
	if (inputlen == 0)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("malformed OAUTHBEARER message"),
				 errdetail("The message is empty.")));
	if (inputlen != strlen(input))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("malformed OAUTHBEARER message"),
				 errdetail("Message length does not match input length.")));

	switch (ctx->state)
	{
		case OAUTH_STATE_INIT:
			/* Handle this case below. */
			break;

		case OAUTH_STATE_ERROR:

			/*
			 * Only one response is valid for the client during authentication
			 * failure: a single kvsep.
			 */
			if (inputlen != 1 || *input != KVSEP)
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("malformed OAUTHBEARER message"),
						 errdetail("Client did not send a kvsep response.")));

			/* The (failed) handshake is now complete. */
			ctx->state = OAUTH_STATE_FINISHED;
			return PG_SASL_EXCHANGE_FAILURE;

		default:
			elog(ERROR, "invalid OAUTHBEARER exchange state");
			return PG_SASL_EXCHANGE_FAILURE;
	}

	/* Handle the client's initial message. */
	p = pstrdup(input);

	/*
	 * OAUTHBEARER does not currently define a channel binding (so there is no
	 * OAUTHBEARER-PLUS, and we do not accept a 'p' specifier). We accept a
	 * 'y' specifier purely for the remote chance that a future specification
	 * could define one; then future clients can still interoperate with this
	 * server implementation. 'n' is the expected case.
	 */
	cbind_flag = *p;
	switch (cbind_flag)
	{
		case 'p':
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("malformed OAUTHBEARER message"),
					 errdetail("The server does not support channel binding for OAuth, but the client message includes channel binding data.")));
			break;

		case 'y':				/* fall through */
		case 'n':
			p++;
			if (*p != ',')
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("malformed OAUTHBEARER message"),
						 errdetail("Comma expected, but found character \"%s\".",
								   sanitize_char(*p))));
			p++;
			break;

		default:
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("malformed OAUTHBEARER message"),
					 errdetail("Unexpected channel-binding flag %s.",
							   sanitize_char(cbind_flag))));
	}

	/*
	 * Forbid optional authzid (authorization identity).  We don't support it.
	 */
	if (*p == 'a')
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("client uses authorization identity, but it is not supported")));
	if (*p != ',')
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("malformed OAUTHBEARER message"),
				 errdetail("Unexpected attribute %s in client-first-message.",
						   sanitize_char(*p))));
	p++;

	/* All remaining fields are separated by the RFC's kvsep (\x01). */
	if (*p != KVSEP)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("malformed OAUTHBEARER message"),
				 errdetail("Key-value separator expected, but found character %s.",
						   sanitize_char(*p))));
	p++;

	auth = parse_kvpairs_for_auth(&p);
	if (!auth)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("malformed OAUTHBEARER message"),
				 errdetail("Message does not contain an auth value.")));

	/* We should be at the end of our message. */
	if (*p)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("malformed OAUTHBEARER message"),
				 errdetail("Message contains additional data after the final terminator.")));

	if (!validate(ctx->port, auth))
	{
		generate_error_response(ctx, output, outputlen);

		ctx->state = OAUTH_STATE_ERROR;
		return PG_SASL_EXCHANGE_CONTINUE;
	}

	ctx->state = OAUTH_STATE_FINISHED;
	return PG_SASL_EXCHANGE_SUCCESS;
}

/*
 * Convert an arbitrary byte to printable form.  For error messages.
 *
 * If it's a printable ASCII character, print it as a single character.
 * otherwise, print it in hex.
 *
 * The returned pointer points to a static buffer.
 */
static char *
sanitize_char(char c)
{
	static char buf[5];

	if (c >= 0x21 && c <= 0x7E)
		snprintf(buf, sizeof(buf), "'%c'", c);
	else
		snprintf(buf, sizeof(buf), "0x%02x", (unsigned char) c);
	return buf;
}

/*
 * Performs syntactic validation of a key and value from the initial client
 * response. (Semantic validation of interesting values must be performed
 * later.)
 */
static void
validate_kvpair(const char *key, const char *val)
{
	/*-----
	 * From Sec 3.1:
	 *     key            = 1*(ALPHA)
	 */
	static const char *key_allowed_set =
		"abcdefghijklmnopqrstuvwxyz"
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ";

	size_t		span;

	if (!key[0])
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("malformed OAUTHBEARER message"),
				 errdetail("Message contains an empty key name.")));

	span = strspn(key, key_allowed_set);
	if (key[span] != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("malformed OAUTHBEARER message"),
				 errdetail("Message contains an invalid key name.")));

	/*-----
	 * From Sec 3.1:
	 *     value          = *(VCHAR / SP / HTAB / CR / LF )
	 *
	 * The VCHAR (visible character) class is large; a loop is more
	 * straightforward than strspn().
	 */
	for (; *val; ++val)
	{
		if (0x21 <= *val && *val <= 0x7E)
			continue;			/* VCHAR */

		switch (*val)
		{
			case ' ':
			case '\t':
			case '\r':
			case '\n':
				continue;		/* SP, HTAB, CR, LF */

			default:
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("malformed OAUTHBEARER message"),
						 errdetail("Message contains an invalid value.")));
		}
	}
}

/*
 * Consumes all kvpairs in an OAUTHBEARER exchange message. If the "auth" key is
 * found, its value is returned.
 */
static char *
parse_kvpairs_for_auth(char **input)
{
	char	   *pos = *input;
	char	   *auth = NULL;

	/*----
	 * The relevant ABNF, from Sec. 3.1:
	 *
	 *     kvsep          = %x01
	 *     key            = 1*(ALPHA)
	 *     value          = *(VCHAR / SP / HTAB / CR / LF )
	 *     kvpair         = key "=" value kvsep
	 *   ;;gs2-header     = See RFC 5801
	 *     client-resp    = (gs2-header kvsep *kvpair kvsep) / kvsep
	 *
	 * By the time we reach this code, the gs2-header and initial kvsep have
	 * already been validated. We start at the beginning of the first kvpair.
	 */

	while (*pos)
	{
		char	   *end;
		char	   *sep;
		char	   *key;
		char	   *value;

		/*
		 * Find the end of this kvpair. Note that input is null-terminated by
		 * the SASL code, so the strchr() is bounded.
		 */
		end = strchr(pos, KVSEP);
		if (!end)
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("malformed OAUTHBEARER message"),
					 errdetail("Message contains an unterminated key/value pair.")));
		*end = '\0';

		if (pos == end)
		{
			/* Empty kvpair, signifying the end of the list. */
			*input = pos + 1;
			return auth;
		}

		/*
		 * Find the end of the key name.
		 */
		sep = strchr(pos, '=');
		if (!sep)
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("malformed OAUTHBEARER message"),
					 errdetail("Message contains a key without a value.")));
		*sep = '\0';

		/* Both key and value are now safely terminated. */
		key = pos;
		value = sep + 1;
		validate_kvpair(key, value);

		if (!strcmp(key, AUTH_KEY))
		{
			if (auth)
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("malformed OAUTHBEARER message"),
						 errdetail("Message contains multiple auth values.")));

			auth = value;
		}
		else
		{
			/*
			 * The RFC also defines the host and port keys, but they are not
			 * required for OAUTHBEARER and we do not use them. Also, per Sec.
			 * 3.1, any key/value pairs we don't recognize must be ignored.
			 */
		}

		/* Move to the next pair. */
		pos = end + 1;
	}

	ereport(ERROR,
			(errcode(ERRCODE_PROTOCOL_VIOLATION),
			 errmsg("malformed OAUTHBEARER message"),
			 errdetail("Message did not contain a final terminator.")));

	return NULL;				/* unreachable */
}

static void
generate_error_response(struct oauth_ctx *ctx, char **output, int *outputlen)
{
	StringInfoData buf;
	StringInfoData issuer;

	/*
	 * The admin needs to set an issuer and scope for OAuth to work. There's
	 * not really a way to hide this from the user, either, because we can't
	 * choose a "default" issuer, so be honest in the failure message.
	 *
	 * TODO: see if there's a better place to fail, earlier than this.
	 */
	if (!ctx->issuer || !ctx->scope)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("OAuth is not properly configured for this user"),
				 errdetail_log("The issuer and scope parameters must be set in pg_hba.conf.")));

	/*------
	 * Build the .well-known URI based on our issuer.
	 * TODO: RFC 8414 defines a competing well-known URI, so we'll probably
	 * have to make this configurable too.
	 */
	initStringInfo(&issuer);
	appendStringInfoString(&issuer, ctx->issuer);
	appendStringInfoString(&issuer, "/.well-known/openid-configuration");

	initStringInfo(&buf);

	/*
	 * TODO: note that escaping here should be belt-and-suspenders, since
	 * escapable characters aren't valid in either the issuer URI or the scope
	 * list, but the HBA doesn't enforce that yet.
	 */
	appendStringInfoString(&buf, "{ \"status\": \"invalid_token\", ");

	appendStringInfoString(&buf, "\"openid-configuration\": ");
	escape_json(&buf, issuer.data);
	pfree(issuer.data);

	appendStringInfoString(&buf, ", \"scope\": ");
	escape_json(&buf, ctx->scope);

	appendStringInfoString(&buf, " }");

	*output = buf.data;
	*outputlen = buf.len;
}

/*-----
 * Validates the provided Authorization header and returns the token from
 * within it. NULL is returned on validation failure.
 *
 * Only Bearer tokens are accepted. The ABNF is defined in RFC 6750, Sec.
 * 2.1:
 *
 *      b64token    = 1*( ALPHA / DIGIT /
 *                        "-" / "." / "_" / "~" / "+" / "/" ) *"="
 *      credentials = "Bearer" 1*SP b64token
 *
 * The "credentials" construction is what we receive in our auth value.
 *
 * Since that spec is subordinate to HTTP (i.e. the HTTP Authorization
 * header format; RFC 7235 Sec. 2), the "Bearer" scheme string must be
 * compared case-insensitively. (This is not mentioned in RFC 6750, but
 * it's pointed out in RFC 7628 Sec. 4.)
 *
 * Invalid formats are technically a protocol violation, but we shouldn't
 * reflect any information about the sensitive Bearer token back to the
 * client; log at COMMERROR instead.
 *
 * TODO: handle the Authorization spec, RFC 7235 Sec. 2.1.
 */
static const char *
validate_token_format(const char *header)
{
	size_t		span;
	const char *token;
	static const char *const b64token_allowed_set =
		"abcdefghijklmnopqrstuvwxyz"
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		"0123456789-._~+/";

	/* If the token is empty or simply too short to be correct */
	if (!header || strlen(header) <= 7)
	{
		ereport(COMMERROR,
				(errmsg("malformed OAuth bearer token 1")));
		return NULL;
	}

	if (pg_strncasecmp(header, BEARER_SCHEME, strlen(BEARER_SCHEME)))
		return NULL;

	/* Pull the bearer token out of the auth value. */
	token = header + strlen(BEARER_SCHEME);

	/* Swallow any additional spaces. */
	while (*token == ' ')
		token++;

	/* Tokens must not be empty. */
	if (!*token)
	{
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("malformed OAuth bearer token 2"),
				 errdetail("Bearer token is empty.")));
		return NULL;
	}

	/*
	 * Make sure the token contains only allowed characters. Tokens may end
	 * with any number of '=' characters.
	 */
	span = strspn(token, b64token_allowed_set);
	while (token[span] == '=')
		span++;

	if (token[span] != '\0')
	{
		/*
		 * This error message could be more helpful by printing the
		 * problematic character(s), but that'd be a bit like printing a piece
		 * of someone's password into the logs.
		 */
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("malformed OAuth bearer token 3"),
				 errdetail("Bearer token is not in the correct format.")));
		return NULL;
	}

	return token;
}

static bool
validate(Port *port, const char *auth)
{
	int			map_status;
	ValidatorModuleResult *ret;
	const char *token;

	/* Ensure that we have a correct token to validate */
	if (!(token = validate_token_format(auth)))
		return false;

	/* Call the validation function from the validator module */
	ret = ValidatorCallbacks->validate_cb(validator_module_state,
										  token, port->user_name);

	if (!ret->authorized)
		return false;

	if (ret->authn_id)
		set_authn_id(port, ret->authn_id);

	if (port->hba->oauth_skip_usermap)
	{
		/*
		 * If the validator is our authorization authority, we're done.
		 * Authentication may or may not have been performed depending on the
		 * validator implementation; all that matters is that the validator
		 * says the user can log in with the target role.
		 */
		return true;
	}

	/* Make sure the validator authenticated the user. */
	if (ret->authn_id == NULL || ret->authn_id[0] == '\0')
	{
		/* TODO: use logdetail; reduce message duplication */
		ereport(LOG,
				(errmsg("OAuth bearer authentication failed for user \"%s\": validator provided no identity",
						port->user_name)));
		return false;
	}

	/* Finally, check the user map. */
	map_status = check_usermap(port->hba->usermap, port->user_name,
							   MyClientConnectionInfo.authn_id, false);
	return (map_status == STATUS_OK);
}

static void
load_validator_library(void)
{
	OAuthValidatorModuleInit validator_init;

	if (OAuthValidatorLibrary[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("oauth_validator_library is not set")));

	validator_init = (OAuthValidatorModuleInit)
		load_external_function(OAuthValidatorLibrary,
							   "_PG_oauth_validator_module_init", false, NULL);

	if (validator_init == NULL)
		ereport(ERROR,
				(errmsg("%s module \"%s\" have to define the symbol %s",
						"OAuth validator", OAuthValidatorLibrary, "_PG_oauth_validator_module_init")));

	ValidatorCallbacks = (*validator_init) ();

	validator_module_state = (ValidatorModuleState *) palloc0(sizeof(ValidatorModuleState));
	if (ValidatorCallbacks->startup_cb != NULL)
		ValidatorCallbacks->startup_cb(validator_module_state);

	before_shmem_exit(shutdown_validator_library, 0);
}

static void
shutdown_validator_library(int code, Datum arg)
{
	if (ValidatorCallbacks->shutdown_cb != NULL)
		ValidatorCallbacks->shutdown_cb(validator_module_state);
}
