/*-------------------------------------------------------------------------
 *
 * fe-secure.c
 *	  functions related to setting up a secure connection to the backend.
 *	  Secure connections are expected to provide confidentiality,
 *	  message integrity and endpoint authentication.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-secure.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <signal.h>
#include <fcntl.h>
#include <ctype.h>

#ifdef WIN32
#include "win32.h"
#else
#include <sys/socket.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#endif

#include <sys/stat.h>

#ifdef WIN32
#include "pthread-win32.h"
#else
#include <pthread.h>
#endif

#include "fe-auth.h"
#include "libpq-fe.h"
#include "libpq-int.h"

/* ------------------------------------------------------------ */
/*			 Procedures common to all secure sessions			*/
/* ------------------------------------------------------------ */


int
PQsslInUse(PGconn *conn)
{
	if (!conn)
		return 0;
	return conn->ssl_in_use;
}

/*
 *	Exported function to allow application to tell us it's already
 *	initialized OpenSSL.
 */
void
PQinitSSL(int do_init)
{
#ifdef USE_SSL
	pgtls_init_library(do_init, do_init);
#endif
}

/*
 *	Exported function to allow application to tell us it's already
 *	initialized OpenSSL and/or libcrypto.
 */
void
PQinitOpenSSL(int do_ssl, int do_crypto)
{
#ifdef USE_SSL
	pgtls_init_library(do_ssl, do_crypto);
#endif
}

/*
 *	Initialize global SSL context
 */
int
pqsecure_initialize(PGconn *conn, bool do_ssl, bool do_crypto)
{
	int			r = 0;

#ifdef USE_SSL
	r = pgtls_init(conn, do_ssl, do_crypto);
#endif

	return r;
}

/*
 *	Begin or continue negotiating a secure session.
 */
PostgresPollingStatusType
pqsecure_open_client(PGconn *conn)
{
#ifdef USE_SSL
	return pgtls_open_client(conn);
#else
	/* shouldn't get here */
	return PGRES_POLLING_FAILED;
#endif
}


/* Dummy versions of SSL info functions, when built without SSL support */
#ifndef USE_SSL

void *
PQgetssl(PGconn *conn)
{
	return NULL;
}

void *
PQsslStruct(PGconn *conn, const char *struct_name)
{
	return NULL;
}

const char *
PQsslAttribute(PGconn *conn, const char *attribute_name)
{
	return NULL;
}

const char *const *
PQsslAttributeNames(PGconn *conn)
{
	static const char *const result[] = {NULL};

	return result;
}
#endif							/* USE_SSL */

/*
 * Dummy versions of OpenSSL key password hook functions, when built without
 * OpenSSL.
 */
#ifndef USE_OPENSSL

PQsslKeyPassHook_OpenSSL_type
PQgetSSLKeyPassHook_OpenSSL(void)
{
	return NULL;
}

void
PQsetSSLKeyPassHook_OpenSSL(PQsslKeyPassHook_OpenSSL_type hook)
{
	return;
}

int
PQdefaultSSLKeyPassHook_OpenSSL(char *buf, int size, PGconn *conn)
{
	return 0;
}
#endif							/* USE_OPENSSL */

/* Dummy version of GSSAPI information functions, when built without GSS support */
#ifndef ENABLE_GSS

void *
PQgetgssctx(PGconn *conn)
{
	return NULL;
}

int
PQgssEncInUse(PGconn *conn)
{
	return 0;
}

#endif							/* ENABLE_GSS */
