/*-------------------------------------------------------------------------
 *
 * extension_lock.h
 *	  Relation extension lock manager
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/extension_lock.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EXTENSION_H
#define EXTENSION_H

#ifdef FRONTEND
#error "extension_lock.h may not be included from frontend code"
#endif

#include "storage/proclist_types.h"
#include "storage/s_lock.h"
#include "storage/condition_variable.h"
#include "port/atomics.h"

typedef struct RelExtLock
{
	Oid					relid;
	pg_atomic_uint32	state;
	pg_atomic_uint32	nwaiters;
	ConditionVariable	cv;
} RelExtLock;

typedef enum RelExtLockMode
{
	RELEXT_EXCLUSIVE,
	RELEXT_SHARED
} RelExtLockMode;

/* Lock a relation for extension */
extern void InitRelExtLock(long max_table_size);
extern void LockRelationForExtension(Relation relation, RelExtLockMode lockmode);
extern void UnlockRelationForExtension(Relation relation, RelExtLockMode lockmode);
extern bool ConditionalLockRelationForExtension(Relation relation, RelExtLockMode lockmode);
extern int	RelationExtensionLockWaiterCount(Relation relation);
extern void RelationExtensionLockReleaseAll(void);

#endif	/* EXTENSION_H */
