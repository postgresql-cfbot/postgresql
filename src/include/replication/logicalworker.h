/*-------------------------------------------------------------------------
 *
 * logicalworker.h
 *	  Exports for logical replication workers.
 *
 * Portions Copyright (c) 2016-2022, PostgreSQL Global Development Group
 *
 * src/include/replication/logicalworker.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICALWORKER_H
#define LOGICALWORKER_H

#include "utils/guc.h"

extern char *synchronize_slot_names;
extern char *standby_slot_names;

extern void ApplyWorkerMain(Datum main_arg);
extern void ReplSlotSyncMain(Datum main_arg);

extern bool IsLogicalWorker(void);

extern bool check_synchronize_slot_names(char **newval, void **extra, GucSource source);
extern bool check_standby_slot_names(char **newval, void **extra, GucSource source);

#endif							/* LOGICALWORKER_H */
