/*-------------------------------------------------------------------------
 *
 * parse_graphtable.h
 *		parsing of GRAPH_TABLE
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_graphtable.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_GRAPHTABLE_H
#define PARSE_GRAPHTABLE_H

#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "parser/parse_node.h"

typedef struct GraphTableParseState
{
	Oid			graphid;
	List	   *variables;
} GraphTableParseState;

extern Node *graph_table_property_reference(ParseState *pstate, ColumnRef *cref, Node *var);

extern Node *transformGraphPattern(ParseState *pstate, GraphTableParseState *gpstate, GraphPattern *graph_pattern);

#endif							/* PARSE_GRAPHTABLE_H */
