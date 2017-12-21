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

#include "port/atomics.h"
#include "storage/s_lock.h"
#include "storage/condition_variable.h"
#include "storage/proclist_types.h"

/* Lock a relation for extension */
extern Size RelExtLockShmemSize(void);
extern void InitRelExtLocks(void);
extern void LockRelationForExtension(Relation relation);
extern void UnlockRelationForExtension(Relation relation);
extern bool ConditionalLockRelationForExtension(Relation relation);
extern int	EstimateNumberOfExtensionLockWaiters(Relation relation);
extern void WaitForRelationExtensionLockToBeFree(Relation relation);
extern void RelExtLockCleanup(void);
extern bool	IsAnyRelationExtensionLockHeld(void);

#endif	/* EXTENSION_LOCK_H */
