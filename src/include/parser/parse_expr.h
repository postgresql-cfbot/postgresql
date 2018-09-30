/*-------------------------------------------------------------------------
 *
 * parse_expr.h
 *	  handle expressions in parser
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_expr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_EXPR_H
#define PARSE_EXPR_H

#include "parser/parse_node.h"

/* GUC parameters */
extern bool operator_precedence_warning;

extern Node *transformExpr(ParseState *pstate, Node *expr, ParseExprKind exprKind);

extern const char *ParseExprKindName(ParseExprKind exprKind);

typedef enum TransformNullEquals
{
	TRANSFORM_NULL_EQUALS_OFF = 0,	/* Disabled */
	TRANSFORM_NULL_EQUALS_WARN,		/* Issue a warning */
	TRANSFORM_NULL_EQUALS_ERR,		/* Error out */
	TRANSFORM_NULL_EQUALS_ON		/* Enabled */
} TransformNullEquals;

int transform_null_equals;

#endif							/* PARSE_EXPR_H */
