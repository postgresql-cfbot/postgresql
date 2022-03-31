/*-------------------------------------------------------------------------
 *
 * inet-common.h
 *
 *	  Common code for clients of the inet_* APIs.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/common/inet-common.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef INET_COMMON_H
#define INET_COMMON_H

/*
 * We use these values for the "family" field of inet_struct.
 *
 * Referencing all of the non-AF_INET types to AF_INET lets us work on
 * machines which may not have the appropriate address family (like
 * inet6 addresses when AF_INET6 isn't present) but doesn't cause a
 * dump/reload requirement.  Pre-7.4 databases used AF_INET for the family
 * type on disk.
 *
 * In a frontend build, we can't include inet.h, but we still need to have
 * sensible definitions of these two constants.  (Frontend clients should
 * include this header directly.)  Note that pg_inet_net_ntop()
 * assumes that PGSQL_AF_INET is equal to AF_INET.
 */
#define PGSQL_AF_INET	(AF_INET + 0)
#define PGSQL_AF_INET6	(AF_INET + 1)

#endif							/* INET_COMMON_H */
