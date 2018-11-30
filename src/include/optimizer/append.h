/*-------------------------------------------------------------------------
 *
 * append.h
 *	  prototypes for append.c.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/append.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef APPEND_H
#define APPEND_H

#include "nodes/relation.h"

/*
 * append.c
 *	  utilities for dealing with append relations
 */
extern void expand_inherited_tables(PlannerInfo *root);

#endif							/* APPEND_H */
