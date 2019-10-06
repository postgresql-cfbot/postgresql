/*-------------------------------------------------------------------------
 *
 * fork_process.h
 *	  Exports from postmaster/fork_process.c.
 *
 * Copyright (c) 1996-2019, PostgreSQL Global Development Group
 *
 * src/include/postmaster/fork_process.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FORK_PROCESS_H
#define FORK_PROCESS_H

#include "postmaster.h"

typedef enum
{
   NoForkProcess = -1,
   CheckerFork = 0,
   BootstrapFork,
   StartupFork,
   BgWriterFork,
   CheckpointerFork,
   WalWriterFork,
   WalReceiverFork,	/* end of Auxiliary Process Forks */
   AutoVacLauncherFork,
   AutoVacWorkerFork,
   PgstatCollectorFork,
   PgArchiverFork,
   SysLoggerFork,
   BgWorkerFork,
   BackendFork,

   NUMFORKPROCTYPES			/* Must be last! */
} ForkProcType;

extern ForkProcType MyForkProcType;

struct ForkProcData;

typedef void (*ChildProcessMain) (int argc, char *argv[]);

typedef struct ForkProcData
{
   char                   **av;
   int                     ac;
   int                     save_errno; /* for failure */
   char                   *type_desc;
   ChildProcessMain        postmaster_main;
   ChildProcessMain        child_main;
   ChildProcessMain        fail_main;
} ForkProcData;

extern pid_t fork_process(void);

#ifdef EXEC_BACKEND
extern pid_t postmaster_forkexec(ForkProcData *fork_data);
#endif

/* Hook for plugins to get control after a successful fork_process() */
typedef void (*ForkProcess_post_hook_type) ();
extern PGDLLIMPORT ForkProcess_post_hook_type ForkProcess_post_hook;

#endif							/* FORK_PROCESS_H */
