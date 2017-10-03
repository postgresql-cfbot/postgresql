/*-------------------------------------------------------------------------
 *
 * parse_temporal.h
 *	  handle temporal operators in parser
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_temporal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_TEMPORAL_H
#define PARSE_TEMPORAL_H

#include "parser/parse_node.h"

extern Node *
transformTemporalClauseResjunk(Query* qry);

extern Node *
transformTemporalClause(ParseState *pstate,
						Query *qry,
						SelectStmt *stmt);

extern Node *
transformTemporalAligner(ParseState *pstate,
						 JoinExpr *j);

extern Node *
transformTemporalNormalizer(ParseState *pstate,
							JoinExpr *j);

extern void
transformTemporalClauseAmbiguousColumns(ParseState *pstate,
										JoinExpr *j,
										List *l_colnames,
										List *r_colnames,
										List *l_colvars,
										List *r_colvars,
										RangeTblEntry *l_rte,
										RangeTblEntry *r_rte);

extern JoinExpr *
makeTemporalNormalizer(Node *larg,
					   Node *rarg,
					   List *bounds,
					   Node *quals,
					   Alias *alias);

extern JoinExpr *
makeTemporalAligner(Node *larg,
					Node *rarg,
					List *bounds,
					Node *quals,
					Alias *alias);

extern void
tpprint(const void *obj, const char *marker);

#endif   /* PARSE_TEMPORAL_H */
