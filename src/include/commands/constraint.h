/*-------------------------------------------------------------------------
 *
 * constraint.h
 *   PostgreSQL CONSTRAINT support declarations
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *   src/include/commands/constraint.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONSTRAINT_H
#define CONSTRAINT_H

#include "nodes/parsenodes.h"
#include "utils/relcache.h"

extern Constraint *makeCheckNotNullConstraint(Oid nspid,
											  char *constraint_name,
											  const char *relname,
											  const char *colname,
											  bool is_row,
											  Oid parent_oid);

extern char *tryExtractNotNullFromNode(Node *node, Relation rel);
extern AttrNumber tryExtractNotNullFromCatalog(HeapTuple constrTup);

#endif /* CONSTRAINT_H */
