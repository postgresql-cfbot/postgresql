/*-------------------------------------------------------------------------
 *
 * resolver_internal.h
 *	  Internal headers shared by fdwxact resolvers.
 *
 * Portions Copyright (c) 2018, PostgreSQL Global Development Group
 *
 * src/include/access/resovler_internal.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef _RESOLVER_INTERNAL_H
#define _RESOLVER_INTERNAL_H

#include "storage/latch.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/timestamp.h"

/*
 * Each foreign transaction resolver has a FdwXactResolver struct in
 * shared memory.  This struct is protected by FdwXactResolverLaunchLock.
 */
typedef struct FdwXactResolver
{
	pid_t	pid;	/* this resolver's PID, or 0 if not active */
	Oid		dbid;	/* database oid */

	/* Indicates if this slot is used of free */
	bool	in_use;

	/* Stats */
	TimestampTz	last_resolved_time;

	/* Protect shared variables shown above */
	slock_t	mutex;

	/*
	 * Pointer to the resolver's patch. Used by backends to wake up this
	 * resolver when it has work to do. NULL if the resolver isn't active.
	 */
	Latch	*latch;
} FdwXactResolver;

/* There is one FdwXactRslvCtlData struct for the whole database cluster */
typedef struct FdwXactRslvCtlData
{
	/*
	 * Foreign transaction resolution queues. Protected by FdwXactLock.
	 */
	SHM_QUEUE	FdwXactActiveQueue;
	SHM_QUEUE	FdwXactRetryQueue;

	/* Supervisor process and latch */
	pid_t		launcher_pid;
	Latch		*launcher_latch;

	FdwXactResolver resolvers[FLEXIBLE_ARRAY_MEMBER];
} FdwXactRslvCtlData;

extern FdwXactRslvCtlData *FdwXactRslvCtl;

extern FdwXactResolver *MyFdwXactResolver;
extern FdwXactRslvCtlData *FdwXactRslvCtl;

#endif	/* _RESOLVER_INTERNAL_H */
