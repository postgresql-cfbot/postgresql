/*-------------------------------------------------------------------------
 *
 * fe-auth-oauth.h
 *
 *	  Definitions for OAuth authentication implementations
 *
 * Portions Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * src/interfaces/libpq/fe-auth-oauth.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef FE_AUTH_OAUTH_H
#define FE_AUTH_OAUTH_H

#include "libpq-fe.h"
#include "libpq-int.h"


typedef enum
{
	FE_OAUTH_INIT,
	FE_OAUTH_REQUESTING_TOKEN,
	FE_OAUTH_BEARER_SENT,
	FE_OAUTH_SERVER_ERROR,
} fe_oauth_state_enum;

typedef struct
{
	fe_oauth_state_enum state;

	PGconn	   *conn;
	char	   *token;

	void	   *async_ctx;
	void	  (*free_async_ctx) (PGconn *conn, void *ctx);
} fe_oauth_state;

extern PostgresPollingStatusType pg_fe_run_oauth_flow(PGconn *conn, pgsocket *altsock);

#endif							/* FE_AUTH_OAUTH_H */
