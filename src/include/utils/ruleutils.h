/*-------------------------------------------------------------------------
 *
 * ruleutils.h
 *		Declarations for ruleutils.c
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/ruleutils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RULEUTILS_H
#define RULEUTILS_H

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "parser/parse_node.h"

struct Plan;					/* avoid including plannodes.h here */
struct PlannedStmt;

/* Flags for pg_get_indexdef_columns_extended() */
#define RULE_INDEXDEF_PRETTY		0x01
#define RULE_INDEXDEF_KEYS_ONLY		0x02	/* ignore included attributes */

extern char *pg_get_indexdef_string(Oid indexrelid);
extern char *pg_get_indexdef_columns(Oid indexrelid, bool pretty);
extern char *pg_get_indexdef_columns_extended(Oid indexrelid,
											  bits16 flags);
extern char *pg_get_querydef(Query *query, bool pretty);

extern char *pg_get_partkeydef_columns(Oid relid, bool pretty);
extern char *pg_get_partkeydef_string(Oid relid);
extern char *pg_get_partconstrdef_string(Oid partitionId, char *aliasname);

extern char *pg_get_constraintdef_command(Oid constraintId);
extern char *pg_get_constraintdef_string(Oid constraintId);
extern char *deparse_expression(Node *expr, List *dpcontext,
								bool forceprefix, bool showimplicit);
extern List *deparse_context_for(const char *aliasname, Oid relid);
extern List *deparse_context_for_plan_tree(struct PlannedStmt *pstmt,
										   List *rtable_names);
extern List *set_deparse_context_plan(List *dpcontext,
									  struct Plan *plan, List *ancestors);
extern List *select_rtable_names_for_explain(List *rtable,
											 Bitmapset *rels_used);
extern char *generate_collation_name(Oid collid);
extern char *generate_opclass_name(Oid opclass);
extern char *generate_function_name(Oid funcid, int nargs, List *argnames,
									Oid *argtypes, bool has_variadic,
									bool *use_variadic_p,
									ParseExprKind special_exprkind);
extern char *get_range_partbound_string(List *bound_datums);
extern void get_opclass_name(Oid opclass, Oid actual_datatype,
							 StringInfo buf);
extern char *flatten_reloptions(Oid relid);

extern char *pg_get_statisticsobjdef_string(Oid statextid);

extern char *relation_get_column_default(Relation rel, AttrNumber attno,
										 List *dpcontext);

#endif							/* RULEUTILS_H */
