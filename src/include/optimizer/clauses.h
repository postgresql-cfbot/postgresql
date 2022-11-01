/*-------------------------------------------------------------------------
 *
 * clauses.h
 *	  prototypes for clauses.c.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/clauses.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLAUSES_H
#define CLAUSES_H

#include "nodes/pathnodes.h"

typedef struct
{
	int			numWindowFuncs; /* total number of WindowFuncs found */
	Index		maxWinRef;		/* windowFuncs[] is indexed 0 .. maxWinRef */
	List	  **windowFuncs;	/* lists of WindowFuncs for each winref */
} WindowFuncLists;

/* Data structure to represent all level-zero Vars meeting some condition */
typedef struct
{
	int			max_varno;		/* maximum index in varattnos[] */
	/* Attnos in these sets are offset by FirstLowInvalidHeapAttributeNumber */
	Bitmapset  *varattnos[FLEXIBLE_ARRAY_MEMBER];
} VarAttnoSet;

extern bool contain_agg_clause(Node *clause);

extern bool contain_window_function(Node *clause);
extern WindowFuncLists *find_window_functions(Node *clause, Index maxWinRef);

extern double expression_returns_set_rows(PlannerInfo *root, Node *clause);

extern bool contain_subplans(Node *clause);

extern char max_parallel_hazard(Query *parse);
extern bool is_parallel_safe(PlannerInfo *root, Node *node);
extern bool contain_nonstrict_functions(Node *clause);
extern bool contain_exec_param(Node *clause, List *param_ids);
extern bool contain_leaked_vars(Node *clause);

extern VarAttnoSet *make_empty_varattnoset(int rangetable_length);
extern void varattnoset_add_members(VarAttnoSet *a, const VarAttnoSet *b);
extern Relids varattnoset_intersect_relids(const VarAttnoSet *a,
										   const VarAttnoSet *b);

extern Relids find_nonnullable_rels(Node *clause);
extern void find_nonnullable_vars(Node *clause, VarAttnoSet *attnos);
extern void find_forced_null_vars(Node *node, VarAttnoSet *attnos);
extern Var *find_forced_null_var(Node *node);

extern bool is_pseudo_constant_clause(Node *clause);
extern bool is_pseudo_constant_clause_relids(Node *clause, Relids relids);

extern int	NumRelids(PlannerInfo *root, Node *clause);

extern void CommuteOpExpr(OpExpr *clause);

extern Query *inline_set_returning_function(PlannerInfo *root,
											RangeTblEntry *rte);

extern Bitmapset *pull_paramids(Expr *expr);

#endif							/* CLAUSES_H */
