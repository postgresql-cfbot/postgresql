/*-------------------------------------------------------------------------
 *
 * pgarch.h
 *	  Exports from postmaster/pgarch.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/postmaster/pgarch.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _PGARCH_H
#define _PGARCH_H

#include "port/atomics.h"
#include "storage/spin.h"

/* ----------
 * Archiver control info.
 *
 * We expect that archivable files within pg_wal will have names between
 * MIN_XFN_CHARS and MAX_XFN_CHARS in length, consisting only of characters
 * appearing in VALID_XFN_CHARS.  The status files in archive_status have
 * corresponding names with ".ready" or ".done" appended.
 * ----------
 */
#define MIN_XFN_CHARS	16
#define MAX_XFN_CHARS	40
#define VALID_XFN_CHARS "0123456789ABCDEF.history.backup.partial"

extern bool PgArchCanRestart(void);
pg_noreturn extern void PgArchiverMain(const void *startup_data, size_t startup_data_len);
extern void PgArchWakeup(void);
extern void PgArchForceDirScan(void);

/* Shared memory area for archiver process */
typedef struct PgArchData
{
	int			pgprocno;		/* proc number of archiver process */

	/* Lock to protect the `primary_last_archived`. */
	slock_t		lock;

	/* Last archived WAL segment file reported by the primary */
	char primary_last_archived[MAX_XFN_CHARS + 1];

	/*
	 * Forces a directory scan in pgarch_readyXlog().
	 */
	pg_atomic_uint32 force_dir_scan;
} PgArchData;


extern PgArchData *PgArch;


#endif							/* _PGARCH_H */
