/*-------------------------------------------------------------------------
 *
 * xlogrestore.h
 *		Prototypes and definitions for parallel restore commands execution
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/xlogrestore.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef XLOGRESTORE_H
#define XLOGRESTORE_H

#include "postgres.h"

/* GUC variables */
extern int	wal_max_prefetch_amount;
extern int	wal_prefetch_workers;

/*
 * Default max number of WAL files for pre-fetching from archive.
 * Zero value means that WAL files prefetching is turned off by default.
 */
#define DEFAULT_WAL_MAX_PREFETCH_AMOUNT	0

/*
 * Default value for a number of prefetch workers spawned by postmaster
 * on server startup for database recovering from archive.
 */
#define DEFAULT_TOTAL_PREFETCH_WORKERS 2

extern Size RestoreCommandShmemSize(void);
extern void RestoreCommandShmemInit(void);
extern bool RestoreCommandXLog(char *path, const char *xlogfname,
							   const char *recovername,
							   const off_t expectedSize,
							   bool cleanupEnabled);
extern void WALPrefetchWorkerMain(Datum main_arg) pg_attribute_noreturn();

#endif							/* XLOGRESTORE_H */
