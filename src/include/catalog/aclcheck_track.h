/*-------------------------------------------------------------------------
 *
 * aclcheck_track.h
 *	  Track permission checks for revalidation after lock acquisition
 *	  in dependencyLockAndCheckObject().
 *
 * DDL may perform ACL checks on referenced objects without first holding a lock
 * on them. In that case, the lock is acquired much later, when recording
 * dependencies.
 * Track the ACL checks, so that we can re-check them after acquiring the
 * lock while recording dependencies.
 *
 * XXX: consider refactoring so that we perform the name lookup, acquire the
 * lock, and check ACLs all in unison, like RangeVarGetRelidExtended().
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/aclcheck_track.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ACLCHECK_TRACK_H
#define ACLCHECK_TRACK_H

#include "storage/sinval.h"
#include "utils/acl.h"

typedef struct AclCheckEntry
{
	Oid			classId;
	Oid			objectId;
	Oid			roleId;
	AclMode		mode;
	uint64		inval_count;
} AclCheckEntry;

typedef struct TrackAclTable
{
	AclCheckEntry *entries;
	int			count;
	int			max;
} TrackAclTable;

extern TrackAclTable *CurrentTrackAclTable;

extern TrackAclTable *CreateTrackAclTable(void);
extern void FreeTrackAclTable(TrackAclTable *acltable);

/*
 * Record an aclcheck for later revalidation.
 *
 * Called from object_aclcheck_ext() and pg_class_aclcheck_ext().
 * Only records when inside an utility statement.
 */
static inline void
aclcheck_track_record(Oid classId, Oid objectId, Oid roleId, AclMode mode)
{
	TrackAclTable *acltable = CurrentTrackAclTable;
	AclCheckEntry *entry;

	if (acltable == NULL)
		return;

	if (acltable->count >= acltable->max)
	{
		acltable->max *= 2;
		acltable->entries = (AclCheckEntry *)
			repalloc(acltable->entries, acltable->max * sizeof(AclCheckEntry));
	}

	entry = &acltable->entries[acltable->count++];
	entry->classId = classId;
	entry->objectId = objectId;
	entry->roleId = roleId;
	entry->mode = mode;
	entry->inval_count = SharedInvalidMessageCounter;
}

#endif							/* ACLCHECK_TRACK_H */
