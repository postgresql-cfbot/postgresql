/*-------------------------------------------------------------------------
 *
 * signalfuncs.h
 *	  Functions for signaling backends
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/signalfuncs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PROCSIGNALFUNC_H
#define PROCSIGNALFUNC_H

/*
 * prototypes for functions in signalfuncs.c
 */
extern bool SendProcSignalForLogInfo(pid_t pid, ProcSignalReason reason);

#endif							/* PROCSIGNALFUNC_H */
