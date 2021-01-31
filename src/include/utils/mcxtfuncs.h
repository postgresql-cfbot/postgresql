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

#define PG_MCXT_FILE_FORMAT_ID	0x01B5BC9E

typedef enum McxtDumpStatus
{
	MCXTDUMPSTATUS_ACCEPTABLE,		/* no one is requesting dumping */
	MCXTDUMPSTATUS_REQUESTING,		/* request has been issued, but dumper has not received it yet */
	MCXTDUMPSTATUS_DUMPING,			/* dumper is dumping files */
	MCXTDUMPSTATUS_DONE,			/* dumper has finished dumping and the requestor is working */
	MCXTDUMPSTATUS_CANCELING		/* requestor canceled the dumping */
} McxtDumpStatus;

typedef struct mcxtdumpShmemStruct
{
	pid_t		dst_pid;		/* pid of the signal receiver */
	pid_t		src_pid;		/* pid of the signal sender */
	McxtDumpStatus	dump_status;		/* dump status */
} mcxtdumpShmemStruct;

extern void ProcessDumpMemoryContextInterrupt(void);
extern void HandleProcSignalDumpMemoryContext(void);
extern void McxtDumpShmemInit(void);
extern void RemoveMcxtDumpFile(int);
extern void RemoveMemcxtFile(int);
#endif							/* MCXT_H */
