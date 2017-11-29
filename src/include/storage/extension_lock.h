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

#ifndef EXTENSION_LOCK_H
#define EXTENSION_LOCK_H

#ifdef FRONTEND
#error "extension_lock.h may not be included from frontend code"
#endif

#include "storage/proclist_types.h"
#include "storage/s_lock.h"
#include "storage/condition_variable.h"
#include "port/atomics.h"

typedef enum RelExtLockMode
{
	RELEXT_EXCLUSIVE,
	RELEXT_SHARED
} RelExtLockMode;

/* Lock a relation for extension */
extern void InitRelExtLock(long max_table_size);
extern void LockRelationForExtension(Relation relation, RelExtLockMode lockmode);
extern void UnlockRelationForExtension(Relation relation);
extern bool ConditionalLockRelationForExtension(Relation relation, RelExtLockMode lockmode);
extern int	RelationExtensionLockWaiterCount(Relation relation);
extern void RelExtLockReleaseAll(void);
extern int	RelExtLockHoldingLockCount(void);

#endif	/* EXTENSION_LOCK_H */
