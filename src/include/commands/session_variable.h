/*-------------------------------------------------------------------------
 *
 * sessionvariable.h
 *	  prototypes for sessionvariable.c.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/session_variable.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SESSIONVARIABLE_H
#define SESSIONVARIABLE_H

#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "parser/parse_node.h"
#include "tcop/cmdtag.h"
#include "utils/queryenvironment.h"

extern void SessionVariableDropPostprocess(Oid varid);
extern void AtPreEOXact_SessionVariables(void);

extern void SetSessionVariable(Oid varid, Datum value, bool isNull);
extern void SetSessionVariableWithSecurityCheck(Oid varid, Datum value, bool isNull);
extern Datum GetSessionVariable(Oid varid, bool *isNull, Oid *typid);
extern Datum GetSessionVariableWithTypeCheck(Oid varid, bool *isNull, Oid expected_typid);

extern void ExecuteLetStmt(ParseState *pstate, LetStmt *stmt, ParamListInfo params,
						   QueryEnvironment *queryEnv, QueryCompletion *qc);

extern void ResetSessionVariables(void);

#endif
