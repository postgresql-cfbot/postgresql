/*-------------------------------------------------------------------------
 *
 * logicalrelation.h
 *	  Relation definitions for logical replication relation mapping.
 *
 * Portions Copyright (c) 2016-2022, PostgreSQL Global Development Group
 *
 * src/include/replication/logicalrelation.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICALRELATION_H
#define LOGICALRELATION_H

#include "access/attmap.h"
#include "replication/logicalproto.h"

/*
 *	States to determine if changes on one relation can be applied using an
 *	apply background worker.
 */
typedef enum ParalleApplySafety
{
	PARALLEL_APPLY_UNKNOWN = 0,	/* unknown  */
	PARALLEL_APPLY_SAFE,		/* Can apply changes in an apply background
								   worker */
	PARALLEL_APPLY_UNSAFE		/* Can not apply changes in an apply background
								   worker */
} ParalleApplySafety;

typedef struct LogicalRepRelMapEntry
{
	LogicalRepRelation remoterel;	/* key is remoterel.remoteid */

	/*
	 * Validity flag -- when false, revalidate all derived info at next
	 * logicalrep_rel_open.  (While the localrel is open, we assume our lock
	 * on that rel ensures the info remains good.)
	 */
	bool		localrelvalid;

	/* Mapping to local relation. */
	Oid			localreloid;	/* local relation id */
	Relation	localrel;		/* relcache entry (NULL when closed) */
	AttrMap    *attrmap;		/* map of local attributes to remote ones */
	bool		updatable;		/* Can apply updates/deletes? */
	ParalleApplySafety	parallel_apply;	/* Can apply changes in an apply
										   background worker? */

	/* Sync state. */
	char		state;
	XLogRecPtr	statelsn;
} LogicalRepRelMapEntry;

extern void logicalrep_relmap_update(LogicalRepRelation *remoterel);
extern void logicalrep_partmap_reset_relmap(LogicalRepRelation *remoterel);

extern LogicalRepRelMapEntry *logicalrep_rel_open(LogicalRepRelId remoteid,
												  LOCKMODE lockmode);
extern LogicalRepRelMapEntry *logicalrep_partition_open(LogicalRepRelMapEntry *root,
														Relation partrel, AttrMap *map);
extern void logicalrep_rel_close(LogicalRepRelMapEntry *rel,
								 LOCKMODE lockmode);

#endif							/* LOGICALRELATION_H */
