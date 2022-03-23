/* -------------------------------------------------------------------------
 *
 * test_auth_provider.c
 *			example authentication provider plugin
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/test_auth_provider/test_auth_provider.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "libpq/auth.h"
#include "libpq/libpq.h"
#include "libpq/scram.h"

PG_MODULE_MAGIC;

void _PG_init(void);

static char *get_encrypted_password_for_user(char *user_name);

/*
 * List of usernames / passwords to approve. Here we are not
 * getting passwords from Postgres but from this list. In a more real-life
 * extension, you can fetch valid credentials and authentication tokens /
 * passwords from an external authentication provider.
 */
char credentials[3][3][50] = {
	{"bob","alice","carol"},
	{"bob123","alice123","carol123"}
};

static int TestAuthenticationCheck(Port *port)
{
	int result = STATUS_ERROR;
	char *real_pass;
	const char *logdetail = NULL;
	ListCell *lc;

	/*
	 * If user's name is in the the "allow" list, do not request password
	 * for them and allow them to authenticate.
	 */
	foreach(lc,port->hba->custom_auth_options)
	{
		CustomOption *option = (CustomOption *) lfirst(lc);
		if (strcmp(option->name, "allow") == 0 &&
			strcmp(option->value, port->user_name) == 0)
		{
			set_authn_id(port, port->user_name);
			return STATUS_OK;
		}
	}

	/*
	 * Encrypt the password and validate that it's the same as the one
	 * returned by the client.
	 */
	real_pass = get_encrypted_password_for_user(port->user_name);
	if (real_pass)
	{
		result = CheckSASLAuth(&pg_be_scram_mech, port, real_pass, &logdetail);
		pfree(real_pass);
	}

	if (result == STATUS_OK)
		set_authn_id(port, port->user_name);

	return result;
}

/*
 * Get SCRAM encrypted version of the password for user.
 */
static char *
get_encrypted_password_for_user(char *user_name)
{
	char *password = NULL;
	int i;
	for (i=0; i<3; i++)
	{
		if (strcmp(user_name, credentials[0][i]) == 0)
		{
			password = pstrdup(pg_be_scram_build_secret(credentials[1][i]));
		}
	}

	return password;
}

static const char *TestAuthenticationError(Port *port)
{
	char *error_message = (char *)palloc (100);
	sprintf(error_message, "Test authentication failed for user %s", port->user_name);
	return error_message;
}

/*
 * Returns if the options passed are supported by the extension
 * and are valid. Currently only "allow" is supported.
 */
static bool TestAuthenticationOptions(char *name, char *val, HbaLine *hbaline, char **err_msg)
{
	/* Validate that an actual user is in the "allow" list. */
	if (strcmp(name,"allow") == 0)
	{
		for (int i=0;i<3;i++)
		{
			if (strcmp(val,credentials[0][i]) == 0)
			{
				return true;
			}
		}

		*err_msg = psprintf("\"%s\" is not valid value for option \"%s\"", val, name);
		return false;
	}
	else
	{
		*err_msg = psprintf("option \"%s\" not recognized by \"%s\" provider", val, hbaline->custom_provider);
		return false;
	}
}

void
_PG_init(void)
{
	RegisterAuthProvider("test", TestAuthenticationCheck,
						 TestAuthenticationError,TestAuthenticationOptions);
}
