/*-------------------------------------------------------------------------
 *
 * fe-secure-common.h
 *
 * common implementation-independent SSL support code
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-secure-common.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef FE_SECURE_COMMON_H
#define FE_SECURE_COMMON_H

#include "libpq-fe.h"

/*
 * In a frontend build, we can't include inet.h, but we still need to have
 * sensible definitions of these two constants.  Note that pg_inet_net_ntop()
 * assumes that PGSQL_AF_INET is equal to AF_INET.
 */
#define PGSQL_AF_INET	(AF_INET + 0)
#define PGSQL_AF_INET6	(AF_INET + 1)

extern int	pq_verify_peer_name_matches_certificate_name(PGconn *conn,
														 const char *namedata, size_t namelen,
														 char **store_name);
extern int	pq_verify_peer_name_matches_certificate_ip(PGconn *conn,
													   const char *addrdata, size_t addrlen,
													   char **store_name);
extern bool pq_verify_peer_name_matches_certificate(PGconn *conn);

#endif							/* FE_SECURE_COMMON_H */
