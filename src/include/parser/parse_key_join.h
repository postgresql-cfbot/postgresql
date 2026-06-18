/*-------------------------------------------------------------------------
 *
 * parse_key_join.h
 *	  Handle key joins in parser
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_key_join.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_KEY_JOIN_H
#define PARSE_KEY_JOIN_H

#include "parser/parse_node.h"

extern void transformAndValidateKeyJoin(ParseState *pstate, JoinExpr *j,
										ParseNamespaceItem *l_nsitem,
										ParseNamespaceItem *r_nsitem,
										List *l_namespace);

#endif							/* PARSE_KEY_JOIN_H */
