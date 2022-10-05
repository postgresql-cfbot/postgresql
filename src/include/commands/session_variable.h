/*-------------------------------------------------------------------------
 *
 * session_variable.h
 *	  prototypes for session_variable.c.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/session_variable.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SESSIONVARIABLE_H
#define SESSIONVARIABLE_H

#include "catalog/objectaddress.h"
#include "catalog/pg_variable.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "tcop/cmdtag.h"
#include "utils/queryenvironment.h"

extern void ResetSessionVariables(void);
extern void SessionVariableCreatePostprocess(Oid varid, char eoxaction);
extern void SessionVariableDropPostprocess(Oid varid);
extern void SessionVariableCreatePostprocess(Oid varid, char eoxaction);

extern Datum CopySessionVariable(Oid varid, bool *isNull, Oid *typid);
extern Datum CopySessionVariableWithTypeCheck(Oid varid, bool *isNull, Oid expected_typid);
extern Datum GetSessionVariableWithTypeCheck(Oid varid, bool *isNull, Oid expected_typid);

extern void SetSessionVariable(Oid varid, Datum value, bool isNull);
extern void SetSessionVariableWithSecurityCheck(Oid varid, Datum value, bool isNull);

extern void AtPreEOXact_SessionVariable_on_xact_actions(bool isCommit);
extern void AtEOSubXact_SessionVariable_on_xact_actions(bool isCommit,
														SubTransactionId mySubid,
														SubTransactionId parentSubid);

extern void ExecuteLetStmt(ParseState *pstate, LetStmt *stmt, ParamListInfo params,
							QueryEnvironment *queryEnv, QueryCompletion *qc);

#endif
