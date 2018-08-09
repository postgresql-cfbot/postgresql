/*-------------------------------------------------------------------------
 *
 * schemavariable.h
 *	  prototypes for schemavariable.c.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
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
#include "utils/queryenvironment.h"

extern char *VariableGetName(Variable *var);

extern void ResetSchemaVariableCache(void);

extern void RemoveVariableById(Oid varid);
extern ObjectAddress DefineSchemaVariable(ParseState *pstate, CreateSchemaVarStmt *stmt);

extern Datum GetSchemaVariable(Oid varid, bool *isNull, Oid expected_typid);
extern void SetSchemaVariable(Oid varid, Datum value, bool isNull, Oid typid, int32 typmod);

extern void doLetStmt(PlannedStmt *pstmt, ParamListInfo params, QueryEnvironment *queryEnv, const char *queryString);

#endif
