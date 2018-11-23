/*-------------------------------------------------------------------------
 *
 * smgrsync.h
 *	  management of file synchronization
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/smgrpending.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SMGRSYNC_H
#define SMGRSYNC_H

#include "postgres.h"

#include "storage/fd.h"


extern void smgrsync_init(void);
extern void smgrpreckpt(void);
extern void smgrsync(void);
extern void smgrpostckpt(void);

extern void UnlinkAfterCheckpoint(RelFileNodeBackend rnode);
extern uint64 FsyncAtCheckpoint(const SmgrFileTag *tag,
								File file,
								uint64 last_cycle);
extern void RememberFsyncRequest(const SmgrFileTag *tag,
								 int fd,
								 uint64 open_seq);
extern void SetForwardFsyncRequests(void);


#endif
