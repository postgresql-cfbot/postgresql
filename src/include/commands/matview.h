/*-------------------------------------------------------------------------
 *
 * matview.h
 *	  prototypes for matview.c.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/matview.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATVIEW_H
#define MATVIEW_H

#include "catalog/objectaddress.h"
#include "fmgr.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "utils/relcache.h"


extern void SetMatViewPopulatedState(Relation relation, bool newstate);

extern void SetMatViewIVMState(Relation relation, bool newstate);

extern ObjectAddress ExecRefreshMatView(RefreshMatViewStmt *stmt, const char *queryString,
										QueryCompletion *qc);
extern ObjectAddress RefreshMatViewByOid(Oid matviewOid, bool is_create, bool skipData,
										 bool concurrent, const char *queryString,
										 QueryCompletion *qc);

extern DestReceiver *CreateTransientRelDestReceiver(Oid transientoid);

extern bool MatViewIncrementalMaintenanceIsEnabled(void);

extern Datum IVM_immediate_before(PG_FUNCTION_ARGS);
extern Datum IVM_immediate_maintenance(PG_FUNCTION_ARGS);
extern Datum IVM_visible_in_prestate(PG_FUNCTION_ARGS);
extern void AtAbort_IVM(SubTransactionId subtxid);
extern void AtPreCommit_IVM(void);
extern void removeImmv(Oid immv_oid);
extern bool isIvmName(const char *s);

#endif							/* MATVIEW_H */
