/*-------------------------------------------------------------------------
 *
 * colenccmds.h
 *	  prototypes for colenccmds.c.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/colenccmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef COLENCCMDS_H
#define COLENCCMDS_H

#include "catalog/objectaddress.h"
#include "parser/parse_node.h"

extern ObjectAddress CreateCEK(ParseState *pstate, DefineStmt *stmt);
extern ObjectAddress CreateCMK(ParseState *pstate, DefineStmt *stmt);

#endif							/* COLENCCMDS_H */
