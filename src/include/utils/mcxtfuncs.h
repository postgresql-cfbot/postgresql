/*-------------------------------------------------------------------------
 *
 * mcxtfuncs.h
 *	  Declarations for showing backend memory context.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/mcxtfuncs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MCXT_H
#define MCXT_H

/* Directory to store dumped memory files */
#define PG_MEMUSAGE_DIR		"pg_memusage"

#define PG_MEMCONTEXT_FILE_FORMAT_ID	0x01B5BC9E

/*
 * Size of the shmem hash table size(not a hard limit).
 *
 * Although it may be better to increase this number in the future (e.g.,
 * adding views for all the backend process of memory contexts), currently
 * small number would be enough.
 */
#define SHMEM_MEMCONTEXT_SIZE		 64

typedef struct mcxtdumpEntry
{
	pid_t		dst_pid;		/* pid of the signal receiver */
	pid_t		src_pid;		/* pid of the signal sender */
	bool		is_dumped;		/* is dumped to a file? */
} mcxtdumpEntry;

extern void ProcessDumpMemoryInterrupt(void);
extern void HandleProcSignalDumpMemory(void);
extern void McxtDumpShmemInit(void);
extern void pg_memusage_reset(void);

#endif							/* MCXT_H */
