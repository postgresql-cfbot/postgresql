/*-------------------------------------------------------------------------
 *
 * subprocess.c
 *
 * Copyright (c) 2004-2020, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/syslogger.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "bootstrap/bootstrap.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker_internals.h"
#include "postmaster/bgwriter.h"
#include "postmaster/pgarch.h"
#include "postmaster/postmaster.h"
#include "postmaster/startup.h"
#include "postmaster/subprocess.h"
#include "postmaster/syslogger.h"
#include "postmaster/walwriter.h"
#include "replication/walreceiver.h"

PgSubprocess	   *MySubprocess;

static PgSubprocess process_types[] = {
	{
		.name = "boot",
		.desc = "checker",
		.needs_aux_proc = true,
		.needs_shmem = true,
		.keep_postmaster_memcontext = false,
		.entrypoint = CheckerModeMain
	},
	{
		.name = "boot",
		.desc = "bootstrap",
		.needs_aux_proc = true,
		.needs_shmem = true,
		.keep_postmaster_memcontext = false,
		.entrypoint = BootstrapModeMain
	},
	{
		.name = "boot",
		.desc = "startup",
		.needs_aux_proc = true,
		.needs_shmem = true,
		.keep_postmaster_memcontext = false,
		.entrypoint = StartupProcessMain,
		.fork_failure = StartupForkFailure
	},
	{
		.name = "boot",
		.desc = "background writer",
		.needs_aux_proc = true,
		.needs_shmem = true,
		.keep_postmaster_memcontext = false,
		.entrypoint = BackgroundWriterMain
	},
	{
		.name = "boot",
		.desc = "checkpointer",
		.needs_aux_proc = true,
		.needs_shmem = true,
		.keep_postmaster_memcontext = false,
		.entrypoint = CheckpointerMain
	},
	{
		.name = "boot",
		.desc = "wal writer",
		.needs_aux_proc = true,
		.needs_shmem = true,
		.keep_postmaster_memcontext = false,
		.entrypoint = WalWriterMain
	},
	{
		.name = "boot",
		.desc = "wal receiver",
		.needs_aux_proc = true,
		.needs_shmem = true,
		.keep_postmaster_memcontext = false,
		.entrypoint = WalReceiverMain
	},
	{
		.name = "avlauncher",
		.desc = "autovacuum launcher",
		.needs_aux_proc = false,
		.needs_shmem = true,
		.keep_postmaster_memcontext = false,
		.entrypoint = AutoVacLauncherMain
	},
	{
		.name = "avworker",
		.desc = "autovacuum worker",
		.needs_aux_proc = false,
		.needs_shmem = true,
		.keep_postmaster_memcontext = false,
		.entrypoint = AutoVacWorkerMain
	},
	{
		.name = "col",
		.desc = "statistics collector",
		.needs_aux_proc = false,
		.needs_shmem = false,
		.keep_postmaster_memcontext = false,
		.fork_prep = PgstatCollectorPrep,
		.entrypoint = PgstatCollectorMain
	},
	{
		.name = "arch",
		.desc = "archiver",
		.needs_aux_proc = false,
		.needs_shmem = false,
		.keep_postmaster_memcontext = false,
		.fork_prep = PgArchiverPrep,
		.entrypoint = PgArchiverMain
	},
	{
		.name = "log",
		.desc = "syslogger",
		.needs_aux_proc = false,
		.needs_shmem = false,
		.keep_postmaster_memcontext = true,
		.fork_prep = SysLoggerPrep,
		.entrypoint = SysLoggerMain,
		.postmaster_main = SysLoggerPostmasterMain
	},
	{
		.name = "bgworker",
		.desc = "background worker",
		.needs_aux_proc = false,
		.needs_shmem = true,
		.keep_postmaster_memcontext = true,
		.fork_prep = BackgroundWorkerPrep,
		.entrypoint = BackgroundWorkerMain,
		.fork_failure = BackgroundWorkerForkFailure,
		.postmaster_main = BackgroundWorkerPostmasterMain
	},
	{
		.name = "backend",
		.desc = "backend",
		.needs_aux_proc = false,
		.needs_shmem = true,
		.keep_postmaster_memcontext = true,
		.fork_prep = BackendPrep,
		.entrypoint = BackendMain,
		.fork_failure = BackendForkFailure,
		.postmaster_main = BackendPostmasterMain
	},
	{	/* Wal Sender placeholder */
		.name = "walsender",
		.desc = "wal sender",
		.needs_aux_proc = false,
		.needs_shmem = true,
		.keep_postmaster_memcontext = true,
		.fork_prep = BackendPrep,
		.entrypoint = BackendMain,
		.fork_failure = BackendForkFailure,
		.postmaster_main = BackendPostmasterMain
	}
};

void
SetMySubprocess(BackendType type)
{
	MyBackendType = type;
	MySubprocess = &process_types[type];
	MySubprocess->desc = gettext(MySubprocess->desc);
}

const char *
GetBackendTypeDesc(BackendType type)
{
	return gettext((&process_types[type])->desc);
}
