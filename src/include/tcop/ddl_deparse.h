/*-------------------------------------------------------------------------
 *
 * ddl_deparse.h
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tcop/ddl_deparse.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DDL_DEPARSE_H
#define DDL_DEPARSE_H

#include "commands/event_trigger.h"
#include "tcop/deparse_utility.h"

extern char *deparse_utility_command(CollectedCommand *cmd, bool include_owner, bool verbose_mode);
extern char *deparse_ddl_json_to_string(char *jsonb, char** owner);
extern char *deparse_drop_command(const char *objidentity, const char *objecttype,
								  DropBehavior behavior);
extern char * deparse_AlterPublicationDropStmt(SQLDropObject *obj);

#endif							/* DDL_DEPARSE_H */
