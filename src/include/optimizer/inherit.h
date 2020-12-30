/*-------------------------------------------------------------------------
 *
 * inherit.h
 *	  prototypes for inherit.c.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/inherit.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INHERIT_H
#define INHERIT_H

#include "nodes/pathnodes.h"

/* Hook for plugins to get control over expand_inherited_rtentry() */
typedef void (*expand_inherited_rtentry_hook_type)(PlannerInfo *root,
												   RelOptInfo *rel,
												   RangeTblEntry *rte,
												   Index rti);

extern PGDLLIMPORT expand_inherited_rtentry_hook_type expand_inherited_rtentry_hook;


extern void expand_inherited_rtentry(PlannerInfo *root, RelOptInfo *rel,
									 RangeTblEntry *rte, Index rti);

extern bool apply_child_basequals(PlannerInfo *root, RelOptInfo *parentrel,
								  RelOptInfo *childrel, RangeTblEntry *childRTE,
								  AppendRelInfo *appinfo);

#endif							/* INHERIT_H */
