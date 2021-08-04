/*-------------------------------------------------------------------------
 *
 * schemavariable.h
 *	  prototypes for schemavariable.c.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/schemavariable.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SCHEMAVARIABLE_H
#define SCHEMAVARIABLE_H

#include "catalog/objectaddress.h"
#include "catalog/pg_variable.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "tcop/cmdtag.h"
#include "utils/queryenvironment.h"

extern void ResetSchemaVariableCache(void);

extern void RemoveSchemaVariable(Oid varid);
extern ObjectAddress DefineSchemaVariable(ParseState *pstate, CreateSchemaVarStmt * stmt);

extern Datum GetSchemaVariable(Oid varid, bool *isNull, Oid expected_typid, bool copy);
extern Datum CopySchemaVariable(Oid varid, bool *isNull, Oid *typid);
extern void SetSchemaVariable(Oid varid, Datum value, bool isNull, Oid typid);
extern void SetSchemaVariableWithSecurityCheck(Oid varid, Datum value, bool isNull, Oid typid);

extern void ExecuteLetStmt(ParseState *pstate, LetStmt *stmt, ParamListInfo params,
						   QueryEnvironment *queryEnv, QueryCompletion *qc);

extern void register_variable_on_commit_action(Oid varid, VariableEOXAction action);

extern void AtPreEOXact_SchemaVariable_on_commit_actions(bool isCommit);
extern void AtEOXact_SchemaVariable_on_commit_actions(bool isCommit);

#endif
