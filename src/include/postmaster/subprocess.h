/*-------------------------------------------------------------------------
 *
 * subprocess.h
 *
 * Copyright (c) 1996-2020, PostgreSQL Global Development Group
 *
 * src/include/postmaster/subprocess.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SUBPROCESS_H
#define SUBPROCESS_H

#include "postmaster.h"

typedef enum BackendType
{
	B_INVALID = -1,
	B_CHECKER = 0,
	B_BOOTSTRAP,
	B_STARTUP,
	B_BG_WRITER,
	B_CHECKPOINTER,
	B_WAL_WRITER,
	B_WAL_RECEIVER,				/* end of Auxiliary Processes */
	B_AUTOVAC_LAUNCHER,
	B_AUTOVAC_WORKER,
	B_STATS_COLLECTOR,
	B_ARCHIVER,
	B_LOGGER,
	B_BG_WORKER,
	B_BACKEND,

	B_WAL_SENDER,				/* placeholder for wal sender so it can be identified in pgstat */
	NUMBACKENDTYPES				/* Must be last! */
} BackendType;

typedef int		(*SubprocessPrep) (int argc, char *argv[]);
typedef void	(*SubprocessEntryPoint) (int argc, char *argv[]);
typedef bool	(*SubprocessForkFailure) (int fork_errno);
typedef void	(*SubprocessPostmasterMain) (int argc, char *argv[]);

/* Current subprocess initializer */
extern void SetMySubprocess(BackendType type);
extern const char *GetBackendTypeDesc(BackendType type);

typedef struct PgSubprocess
{
	const char				   *name;
	const char				   *desc;
	bool						needs_aux_proc;
	bool						needs_shmem;
	bool						keep_postmaster_memcontext;
	SubprocessPrep				fork_prep;
	SubprocessEntryPoint		entrypoint;
	SubprocessForkFailure		fork_failure;
	SubprocessPostmasterMain	postmaster_main;
} PgSubprocess;

extern BackendType					MyBackendType;
extern PGDLLIMPORT PgSubprocess	   *MySubprocess;

#endif							/* SUBPROCESS_H */
