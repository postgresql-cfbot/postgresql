/*-------------------------------------------------------------------------
 *
 * ddldeparse.h
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tcop/ddldeparse.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DDL_DEPARSE_H
#define DDL_DEPARSE_H

#include "tcop/deparse_utility.h"

extern char *deparse_utility_command(CollectedCommand *cmd);
extern char *deparse_ddl_json_to_string(char *jsonb);
extern char *deparse_drop_table(const char *objidentity, Node *parsetree);

#endif							/* DDL_DEPARSE_H */
