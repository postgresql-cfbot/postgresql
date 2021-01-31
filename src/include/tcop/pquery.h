/*-------------------------------------------------------------------------
 *
 * pquery.h
 *	  prototypes for pquery.c.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tcop/pquery.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PQUERY_H
#define PQUERY_H

#include "nodes/parsenodes.h"
#include "utils/guc.h"
#include "utils/portal.h"


extern PGDLLIMPORT Portal ActivePortal;
extern char *result_format_auto_binary_types;


extern PortalStrategy ChoosePortalStrategy(List *stmts);

extern List *FetchPortalTargetList(Portal portal);

extern List *FetchStatementTargetList(Node *stmt);

extern void PortalStart(Portal portal, ParamListInfo params,
						int eflags, Snapshot snapshot);

extern bool check_result_format_auto_binary_types(char **newval, void **extra, GucSource source);
extern void assign_result_format_auto_binary_types(const char *newval, void *extra);

extern void PortalSetResultFormat(Portal portal, int nFormats,
								  int16 *formats);

extern bool PortalRun(Portal portal, long count, bool isTopLevel,
					  bool run_once, DestReceiver *dest, DestReceiver *altdest,
					  QueryCompletion *qc);

extern uint64 PortalRunFetch(Portal portal,
							 FetchDirection fdirection,
							 long count,
							 DestReceiver *dest);

#endif							/* PQUERY_H */
