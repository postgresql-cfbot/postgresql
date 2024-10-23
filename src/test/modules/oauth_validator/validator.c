/*-------------------------------------------------------------------------
 *
 * validator.c
 *	  Test module for serverside OAuth token validation callbacks
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/test/modules/oauth_validator/validator.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "libpq/oauth.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

static void validator_startup(ValidatorModuleState *state);
static void validator_shutdown(ValidatorModuleState *state);
static ValidatorModuleResult *validate_token(ValidatorModuleState *state,
											 const char *token,
											 const char *role);

static const OAuthValidatorCallbacks validator_callbacks = {
	.startup_cb = validator_startup,
	.shutdown_cb = validator_shutdown,
	.validate_cb = validate_token
};

static char *authn_id = NULL;

void
_PG_init(void)
{
	DefineCustomStringVariable("oauth_validator.authn_id",
							   "Authenticated identity to use for future connections",
							   NULL,
							   &authn_id,
							   NULL,
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	MarkGUCPrefixReserved("oauth_validator");
}

const OAuthValidatorCallbacks *
_PG_oauth_validator_module_init(void)
{
	return &validator_callbacks;
}

#define PRIVATE_COOKIE ((void *) 13579)

static void
validator_startup(ValidatorModuleState *state)
{
	state->private_data = PRIVATE_COOKIE;
}

static void
validator_shutdown(ValidatorModuleState *state)
{
	/* do nothing */
}

static ValidatorModuleResult *
validate_token(ValidatorModuleState *state, const char *token, const char *role)
{
	ValidatorModuleResult *res;

	/* Check to make sure our private state still exists. */
	if (state->private_data != PRIVATE_COOKIE)
		elog(ERROR, "oauth_validator: private state cookie changed to %p",
			 state->private_data);

	res = palloc(sizeof(ValidatorModuleResult));

	elog(LOG, "oauth_validator: token=\"%s\", role=\"%s\"", token, role);
	elog(LOG, "oauth_validator: issuer=\"%s\", scope=\"%s\"",
		 MyProcPort->hba->oauth_issuer,
		 MyProcPort->hba->oauth_scope);

	res->authorized = true;
	if (authn_id)
		res->authn_id = pstrdup(authn_id);
	else
		res->authn_id = pstrdup(role);

	return res;
}
