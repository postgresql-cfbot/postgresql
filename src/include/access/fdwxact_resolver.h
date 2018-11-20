/*-------------------------------------------------------------------------
 *
 * fdwxact_resolver.h
 *	  PostgreSQL foreign transaction resolver definitions
 *
 *
 * Portions Copyright (c) 2018, PostgreSQL Global Development Group
 *
 * src/include/access/fdwxact_resolver.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FDWXACT_RESOLVER_H
#define FDWXACT_RESOLVER_H

#include "access/fdwxact.h"

extern void FdwXactResolverMain(Datum main_arg);
extern bool IsFdwXactResolver(void);

extern int foreign_xact_resolver_timeout;

#endif		/* FDWXACT_RESOLVER_H */
