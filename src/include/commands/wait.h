/*-------------------------------------------------------------------------
 *
 * wait.h
 *	  prototypes for commands/wait.c
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 2023, Regents of PostgresPro
 *
 * src/include/commands/wait.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef WAIT_H
#define WAIT_H
#include "postgres.h"
#include "tcop/dest.h"
#include "nodes/parsenodes.h"

extern bool WaitUtility(XLogRecPtr lsn, const int timeout_ms);
extern Size WaitShmemSize(void);
extern void WaitShmemInit(void);
extern void WaitSetLatch(XLogRecPtr cur_lsn);
extern XLogRecPtr GetMinWaitedLSN(void);
extern void DeleteWaitedLSN(void);

#endif							/* WAIT_H */
