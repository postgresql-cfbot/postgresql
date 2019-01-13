/*-------------------------------------------------------------------------
 *
 * predtest.h
 *	  prototypes for predtest.c
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/predtest.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PREDTEST_H
#define PREDTEST_H

#include "nodes/primnodes.h"

#define ARRAY_OPTIMIZATION_SIZE_LIMIT		100
extern PGDLLIMPORT int array_optimization_size_limit;

extern bool predicate_implied_by(List *predicate_list, List *clause_list,
					 bool weak);
extern bool predicate_refuted_by(List *predicate_list, List *clause_list,
					 bool weak);

#endif							/* PREDTEST_H */
