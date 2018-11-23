/*-------------------------------------------------------------------------
 *
 * checkpointer.h
 *	  Exports from postmaster/checkpointer.c.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 * src/include/postmaster/checkpointer.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CHECKPOINTER_H
#define CHECKPOINTER_H

#include "storage/smgr.h"
#include "storage/smgrsync.h"

/*
 * Control whether we transfer file descriptors to the checkpointer, to
 * preserve error state on certain kernels.  We don't yet have support for
 * sending files on Windows (it's entirely possible but it's not clear whether
 * it would actually be useful for anything on that platform).  The macro is
 * here just so that it can be commented out to test the non-fd-passing code
 * path on Unix systems.
 */
#ifndef WIN32
#define CHECKPOINTER_TRANSFER_FILES
#endif

/* GUC options */
extern int	CheckPointTimeout;
extern int	CheckPointWarning;
extern double CheckPointCompletionTarget;

/* The type used for counting checkpoint cycles. */
typedef uint32 CheckpointCycle;

/*
 * A tag identifying a file to be flushed by the checkpointer.  This is
 * convertible to the file's path, but it's convenient to have a small fixed
 * sized object to use as a hash table key.
 */
typedef struct DirtyFileTag
{
	RelFileNode node;
	ForkNumber forknum;
	int segno;
} DirtyFileTag;

extern void CheckpointerMain(void) pg_attribute_noreturn();
extern CheckpointCycle register_dirty_file(const DirtyFileTag *tag,
										   File file,
										   CheckpointCycle last_cycle);

extern void ForwardFsyncRequest(const SmgrFileTag *tag, File fd);
extern void RequestCheckpoint(int flags);
extern void CheckpointWriteDelay(int flags, double progress);

extern void AbsorbFsyncRequests(void);
extern void AbsorbAllFsyncRequests(void);

extern Size CheckpointerShmemSize(void);
extern void CheckpointerShmemInit(void);

extern uint64 GetCheckpointSyncCycle(void);
extern uint64 IncCheckpointSyncCycle(void);

extern bool FirstCallSinceLastCheckpoint(void);
extern void CountBackendWrite(void);

#endif
