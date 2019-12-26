/*-------------------------------------------------------------------------
 *
 * subscripting.h
 *		API for generic type subscripting
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/subscripting.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SUBSCRIPTING_H
#define SUBSCRIPTING_H

#include "parser/parse_node.h"
#include "nodes/primnodes.h"

struct ParseState;
struct SubscriptingRefState;

/* Callback function signatures --- see xsubscripting.sgml for more info. */
typedef SubscriptingRef * (*SubscriptingPrepare) (bool isAssignment, SubscriptingRef *sbsef);

typedef SubscriptingRef * (*SubscriptingValidate) (bool isAssignment, SubscriptingRef *sbsef,
												   struct ParseState *pstate);

typedef Datum (*SubscriptingFetch) (Datum source, struct SubscriptingRefState *sbsrefstate);

typedef Datum (*SubscriptingAssign) (Datum source, struct SubscriptingRefState *sbrsefstate);

typedef void (*SubscriptingInit) (struct SubscriptingRefState *sbrefstate,
								  Datum source, bool isnull);

typedef int (*SubscriptingSelectExpr) (struct SubscriptingRefState *sbsreftate,
									   int subscriptNum, Oid subscriptType,
									   Oid *subscriptExprTypes, int nexprs);

typedef bool (*SubscriptingStep) (struct SubscriptingRefState *sbrefstate,
								  int subscriptNum, bool isupper);

typedef struct SubscriptRoutines
{
	SubscriptingPrepare		prepare;
	SubscriptingValidate	validate;
	SubscriptingFetch		fetch;
	SubscriptingAssign		assign;
	SubscriptingInit		init;
	SubscriptingStep		step;
	SubscriptingSelectExpr	selectexpr;
} SubscriptRoutines;


#endif							/* SUBSCRIPTING_H */
