/*-------------------------------------------------------------------------
 *
 * auth.h
 *	  Definitions for network authentication routines
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/auth.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AUTH_H
#define AUTH_H

#include "libpq/libpq-be.h"

extern char *pg_krb_server_keyfile;
extern bool pg_krb_caseins_users;
extern char *pg_krb_realm;

extern void ClientAuthentication(Port *port);
extern void sendAuthRequest(Port *port, AuthRequest areq, const char *extradata,
							int extralen);
extern void set_authn_id(Port *port, const char *id);
extern char *recv_password_packet(Port *port);

/* Hook for plugins to get control in ClientAuthentication() */
typedef int (*CustomAuthenticationCheck_hook_type) (Port *);
typedef void (*ClientAuthentication_hook_type) (Port *, int);
extern PGDLLIMPORT ClientAuthentication_hook_type ClientAuthentication_hook;

/* Declarations for custom authentication providers */

/* Hook for plugins to report error messages in auth_failed() */
typedef const char * (*CustomAuthenticationError_hook_type) (Port *);

/* Hook for plugins to validate custom authentication options */
typedef bool (*CustomAuthenticationValidateOptions_hook_type)
			 (char *, char *, HbaLine *, char **);

typedef struct CustomAuthProvider
{
	const char *name;
	CustomAuthenticationCheck_hook_type auth_check_hook;
	CustomAuthenticationError_hook_type auth_error_hook;
	CustomAuthenticationValidateOptions_hook_type auth_options_hook;
} CustomAuthProvider;

extern void RegisterAuthProvider
		(const char *provider_name,
		 CustomAuthenticationCheck_hook_type CustomAuthenticationCheck_hook,
		 CustomAuthenticationError_hook_type CustomAuthenticationError_hook,
		 CustomAuthenticationValidateOptions_hook_type CustomAuthenticationOptions_hook);

extern CustomAuthProvider *get_provider_by_name(const char *name);

/*
 * Maximum accepted size of GSS and SSPI authentication tokens.
 * We also use this as a limit on ordinary password packet lengths.
 *
 * Kerberos tickets are usually quite small, but the TGTs issued by Windows
 * domain controllers include an authorization field known as the Privilege
 * Attribute Certificate (PAC), which contains the user's Windows permissions
 * (group memberships etc.). The PAC is copied into all tickets obtained on
 * the basis of this TGT (even those issued by Unix realms which the Windows
 * realm trusts), and can be several kB in size. The maximum token size
 * accepted by Windows systems is determined by the MaxAuthToken Windows
 * registry setting. Microsoft recommends that it is not set higher than
 * 65535 bytes, so that seems like a reasonable limit for us as well.
 */
#define PG_MAX_AUTH_TOKEN_LENGTH	65535

#endif							/* AUTH_H */
