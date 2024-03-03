/*-------------------------------------------------------------------------
 *
 * procsignal.h
 *	  Backtrace-related routines
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/procbacktrace.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PROCBACKTRACE_H
#define PROCBACKTRACE_H

extern void HandleLogBacktraceInterrupt(void);
extern void LoadBacktraceFunctions(void);

#endif							/* PROCBACKTRACE_H */
