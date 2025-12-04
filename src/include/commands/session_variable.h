/*-------------------------------------------------------------------------
 *
 * sessionvariable.h
 *	  prototypes for sessionvariable.c.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/session_variable.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SESSIONVARIABLE_H
#define SESSIONVARIABLE_H

#include "catalog/objectaddress.h"
#include "nodes/params.h"
#include "parser/parse_node.h"
#include "nodes/parsenodes.h"
#include "tcop/cmdtag.h"

extern void CreateVariable(ParseState *pstate, CreateSessionVarStmt *stmt);
extern void DropVariableByName(DropSessionVarStmt *stmt);

extern Datum GetSessionVariableWithTypecheck(char *varname, Oid typid, int32 typmod, bool *isnull);
extern void SetSessionVariableWithTypecheck(char *varname,
											Oid typid, int32 typmod,
											Datum value, bool isnull);

extern void get_session_variable_type_typmod_collid(char *varname,
													Oid *typid,
													int32 *typmod,
													Oid *collid);

extern void ExecuteLetStmt(ParseState *pstate, LetStmt *stmt, ParamListInfo params,
						   QueryEnvironment *queryEnv, QueryCompletion *qc);

extern void ResetSessionVariables(void);

#endif
