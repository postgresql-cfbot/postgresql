/*-------------------------------------------------------------------------
 * conflict.h
 *	   Exports for conflicts logging and resolvers configuration.
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONFLICT_H
#define CONFLICT_H

#include "nodes/execnodes.h"
#include "parser/parse_node.h"
#include "utils/timestamp.h"

/*
 * Conflict types that could occur while applying remote changes.
 *
 * This enum is used in statistics collection (see
 * PgStat_StatSubEntry::conflict_count and
 * PgStat_BackendSubEntry::conflict_count) as well, therefore, when adding new
 * values or reordering existing ones, ensure to review and potentially adjust
 * the corresponding statistics collection codes.
 */
typedef enum
{
	/* The row to be inserted violates unique constraint */
	CT_INSERT_EXISTS,

	/* The row to be updated was modified by a different origin */
	CT_UPDATE_ORIGIN_DIFFERS,

	/* The updated row value violates unique constraint */
	CT_UPDATE_EXISTS,

	/* The row to be updated is missing */
	CT_UPDATE_MISSING,

	/* The row to be deleted was modified by a different origin */
	CT_DELETE_ORIGIN_DIFFERS,

	/* The row to be deleted is missing */
	CT_DELETE_MISSING,

	/*
	 * Other conflicts, such as exclusion constraint violations, involve more
	 * complex rules than simple equality checks. These conflicts are left for
	 * future improvements.
	 */
} ConflictType;

#define CONFLICT_NUM_TYPES (CT_DELETE_MISSING + 1)

/*
 * Conflict resolvers that can be used to resolve various conflicts.
 *
 * See ConflictTypeResolverMap in conflict.c to find out which all
 * resolvers are supported for each conflict type.
 */
typedef enum ConflictResolver
{
	/* Apply the remote change */
	CR_APPLY_REMOTE,

	/* Keep the local change */
	CR_KEEP_LOCAL,

	/* Apply the remote change; skip if it cannot be applied */
	CR_APPLY_OR_SKIP,

	/* Apply the remote change; emit error if it cannot be applied */
	CR_APPLY_OR_ERROR,

	/* Skip applying the change */
	CR_SKIP,

	/* Error out */
	CR_ERROR,
} ConflictResolver;

#define CONFLICT_NUM_RESOLVERS (CR_ERROR + 1)

typedef struct ConflictTypeResolver
{
	const char *conflict_type_name;
	const char *conflict_resolver_name;
} ConflictTypeResolver;

extern bool GetTupleTransactionInfo(TupleTableSlot *localslot,
									TransactionId *xmin,
									RepOriginId *localorigin,
									TimestampTz *localts);
extern void ReportApplyConflict(EState *estate, ResultRelInfo *relinfo,
								int elevel, ConflictType type,
								TupleTableSlot *searchslot,
								TupleTableSlot *localslot,
								TupleTableSlot *remoteslot,
								Oid indexoid, TransactionId localxmin,
								RepOriginId localorigin, TimestampTz localts);
extern void InitConflictIndexes(ResultRelInfo *relInfo);
extern void SetSubConflictResolvers(Oid subId, List *resolvers);
extern void RemoveSubConflictResolvers(Oid confid);
extern List *ParseAndGetSubConflictResolvers(ParseState *pstate,
											 List *stmtresolvers,
											 bool add_defaults);
extern void UpdateSubConflictResolvers(List *conflict_resolvers, Oid subid);
extern ConflictType ValidateConflictType(const char *conflict_type);
extern ConflictType ValidateConflictTypeAndResolver(const char *conflict_type,
													const char *conflict_resolver);
extern List *GetDefaultConflictResolvers(void);
extern void ResetSubConflictResolver(Oid subid, char *conflict_type);

#endif
