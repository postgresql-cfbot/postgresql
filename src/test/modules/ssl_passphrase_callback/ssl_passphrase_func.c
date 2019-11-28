/*-------------------------------------------------------------------------
 *
 * ssl_passphrase_func.c
 *
 * Loadable PostgreSQL module fetch an ssl passphrase for the server cert.
 * instead of calling an external program. This implementation just hands
 * back the configured password rot13'd.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <float.h>
#include <stdio.h>

#include "libpq/libpq-be.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

void		_PG_init(void);
void		_PG_fini(void);

static char *ssl_passphrase = NULL;

static int rot13_passphrase(char *buf,
				 int size,
				 int rwflag,
				 void *userdata);

/*
 * Module load callback
 */
void
_PG_init(void)
{
	/* Define custom GUC variable. */
	DefineCustomStringVariable("ssl_passphrase.passphrase",
							   "passphrase before transformation",
							   NULL,
							   &ssl_passphrase,
							   NULL,
							   PGC_SIGHUP,
							   0,	/* no flags required */
							   NULL,
							   NULL,
							   NULL);
	if (ssl_passphrase)
	{
		ssl_passphrase_function = rot13_passphrase;
		ssl_passphrase_function_supports_reload = true;
	}
}

void
_PG_fini(void)
{
	/* do  nothing yet */
}

static int
rot13_passphrase(char *buf, int size, int rwflag, void *userdata)
{

	Assert(ssl_passphrase != NULL);
	StrNCpy(buf, ssl_passphrase, size);
	for (char *p = buf; *p; p++)
	{
		char		c = *p;

		if ((c >= 'a' && c <= 'm') || (c >= 'A' && c <= 'M'))
			*p = c + 13;
		else if ((c >= 'n' && c <= 'z') || (c >= 'N' && c <= 'Z'))
			*p = c - 13;
	}

	return strlen(buf);

}
