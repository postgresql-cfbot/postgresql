/*-------------------------------------------------------------------------
 *
 * logicalrelation.h
 *	  Relation definitions for logical replication relation mapping.
 *
 * Portions Copyright (c) 2016-2026, PostgreSQL Global Development Group
 *
 * src/include/replication/logicalrelation.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICALRELATION_H
#define LOGICALRELATION_H

#include "access/attmap.h"
#include "catalog/index.h"
#include "replication/logicalproto.h"

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
	Oid			localindexoid;	/* which index to use, or InvalidOid if none */

	/* Sync state. */
	char		state;
	XLogRecPtr	statelsn;

	/*
	 * The last remote transaction that modified the relation's schema or
	 * truncated the relation. Used in dependency tracking to ensure subsequent
	 * transactions modifying the same table wait for this transaction to finish
	 * applying (see check_dependency_on_rel).
	 */
	TransactionId last_depended_xid;

	/* Local unique indexes. Used for dependency tracking */
	List	   *local_unique_indexes;
	bool		local_unique_indexes_collected;

	/*
	 * Whether the relation can be applied in parallel or not. It is
	 * distinglish whether defined triggers are the immutable or not.
	 *
	 * Theoretically, we can determine the parallelizability for each type of
	 * replication messages, INSERT/UPDATE/DELETE/TRUNCATE. But it is not done
	 * yet to reduce the number of attributes.
	 *
	 * Note that we do not check the user-defined constraints here. PostgreSQL
	 * has already assumed that CHECK constraints' conditions are immutable and
	 * here follows the rule.
	 */
	char		parallel_safe;
} LogicalRepRelMapEntry;


typedef struct LogicalRepSubscriberIdx
{
	Oid			indexoid;	/* OID of the local key */
	Bitmapset  *indexkeys;	/* Bitmap of key columns *on remote* */
	bool		nulls_distinct;	/* Whether NULLs are considered distinct */
} LogicalRepSubscriberIdx;

extern void logicalrep_relmap_update(LogicalRepRelation *remoterel);
extern void logicalrep_partmap_reset_relmap(LogicalRepRelation *remoterel);

extern LogicalRepRelMapEntry *logicalrep_rel_open(LogicalRepRelId remoteid,
												  LOCKMODE lockmode);
extern void logicalrep_rel_load(LogicalRepRelMapEntry *entry,
								LogicalRepRelId remoteid, LOCKMODE lockmode);
extern LogicalRepRelMapEntry *logicalrep_partition_open(LogicalRepRelMapEntry *root,
														Relation partrel, AttrMap *map);
extern void logicalrep_rel_close(LogicalRepRelMapEntry *rel,
								 LOCKMODE lockmode);
extern bool IsIndexUsableForReplicaIdentityFull(Relation idxrel, AttrMap *attrmap);
extern Oid	GetRelationIdentityOrPK(Relation rel);
extern int	logicalrep_get_num_rels(void);
extern void logicalrep_write_all_rels(StringInfo out);
extern LogicalRepRelMapEntry *logicalrep_get_relentry(LogicalRepRelId remoteid);

#define LOGICALREP_PARALLEL_SAFE		's'
#define LOGICALREP_PARALLEL_RESTRICTED	'r'
#define LOGICALREP_PARALLEL_UNKNOWN		'u'

#endif							/* LOGICALRELATION_H */
