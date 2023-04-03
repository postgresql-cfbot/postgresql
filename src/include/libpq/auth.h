/*-------------------------------------------------------------------------
 *
 * auth.h
 *	  Definitions for network authentication routines
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/auth.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AUTH_H
#define AUTH_H

#include "libpq/libpq-be.h"

extern PGDLLIMPORT char *pg_krb_server_keyfile;
extern PGDLLIMPORT bool pg_krb_caseins_users;
extern PGDLLIMPORT char *pg_krb_realm;

extern void ClientAuthentication(Port *port);
extern void sendAuthRequest(Port *port, AuthRequest areq, const char *extradata,
							int extralen);

/* Hook for plugins to get control in ClientAuthentication() */
typedef void (*ClientAuthentication_hook_type) (Port *, int);
extern PGDLLIMPORT ClientAuthentication_hook_type ClientAuthentication_hook;

/* hook type for password manglers */
typedef char *(*auth_password_hook_typ) (char *input);

/* Default LDAP password mutator hook, can be overridden by a shared library */
extern PGDLLIMPORT auth_password_hook_typ ldap_password_hook;

/*
 * The failed connection events to be used in the FailedConnection_hook.
 */
typedef enum FailedConnectionEventType
{
	FCET_BAD_DATABASE_NAME,
	FCET_BAD_DATABASE_OID,
	FCET_BAD_DATABASE_PERMISSION,
	FCET_BAD_STARTUP_PACKET,
	FCET_STARTUP_PACKET_TIMEOUT
} FailedConnectionEventType;

/* kluge to avoid including libpq/libpq-be.h here */
struct Port;
typedef void (*FailedConnection_hook_type) (FailedConnectionEventType event, const struct Port *port);
extern PGDLLIMPORT FailedConnection_hook_type FailedConnection_hook;

#endif							/* AUTH_H */
