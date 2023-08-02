/*-------------------------------------------------------------------------
 *
 * launch_backend.c
 *	  Functions for launching backends and other postmaster child
 *	  processes.
 *
 * - explain EXEC_BACKEND and Windows
 * - postmaster calls postmaster_child_launch()
 * - the child process will be restored to roughly the same state, whether
 *   EXEC_BACKEND is used or not: it will be attached to shared memory,
 *   and fds and other resources that we've inherited from postmaster that are not needed
 *   in a child process have been closed.
 *
 *	  When a request message is received, we now fork() immediately.
 *	  The child process performs authentication of the request, and
 *	  then becomes a backend if successful.  This allows the auth code
 *	  to be written in a simple single-threaded style (as opposed to the
 *	  crufty "poor man's multitasking" code that used to be needed).
 *	  More importantly, it ensures that blockages in non-multithreaded
 *	  libraries like SSL or PAM cannot cause denial of service to other
 *	  clients.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/launch_backend.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <sys/param.h>
#include <netdb.h>
#include <limits.h>

#include "access/transam.h"
#include "access/xlog.h"
#include "libpq/libpq-be.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/queryjumble.h"
#include "port.h"
#include "postmaster/autovacuum.h"
#include "postmaster/auxprocess.h"
#include "postmaster/bgworker_internals.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "postmaster/pgarch.h"
#include "postmaster/postmaster.h"
#include "postmaster/startup.h"
#include "postmaster/syslogger.h"
#include "postmaster/walwriter.h"
#include "replication/walreceiver.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"

#ifdef EXEC_BACKEND
#include "storage/spin.h"
#endif


/* Type for a socket that can be inherited to a client process */
#ifdef WIN32
typedef struct
{
	SOCKET		origsocket;		/* Original socket value, or PGINVALID_SOCKET
								 * if not a socket */
	WSAPROTOCOL_INFO wsainfo;
} InheritableSocket;
#else
typedef int InheritableSocket;
#endif

#ifdef EXEC_BACKEND

/*
 * Structure contains all global variables passed to exec:ed backends
 */
typedef struct
{
	char		DataDir[MAXPGPATH];
	int32		MyCancelKey;
	int			MyPMChildSlot;
#ifndef WIN32
	unsigned long UsedShmemSegID;
#else
	void	   *ShmemProtectiveRegion;
	HANDLE		UsedShmemSegID;
#endif
	void	   *UsedShmemSegAddr;
	slock_t    *ShmemLock;
	VariableCache ShmemVariableCache;
	struct bkend *ShmemBackendArray;
#ifndef HAVE_SPINLOCKS
	PGSemaphore *SpinlockSemaArray;
#endif
	int			NamedLWLockTrancheRequests;
	NamedLWLockTranche *NamedLWLockTrancheArray;
	LWLockPadded *MainLWLockArray;
	slock_t    *ProcStructLock;
	PROC_HDR   *ProcGlobal;
	PGPROC	   *AuxiliaryProcs;
	PGPROC	   *PreparedXactProcs;
	PMSignalData *PMSignalState;
	pid_t		PostmasterPid;
	TimestampTz PgStartTime;
	TimestampTz PgReloadTime;
	pg_time_t	first_syslogger_file_time;
	bool		redirection_done;
	bool		IsBinaryUpgrade;
	bool		query_id_enabled;
	int			max_safe_fds;
	int			MaxBackends;
#ifdef WIN32
	HANDLE		PostmasterHandle;
	HANDLE		initial_signal_pipe;
	HANDLE		syslogPipe[2];
#else
	int			postmaster_alive_fds[2];
	int			syslogPipe[2];
#endif
	char		my_exec_path[MAXPGPATH];
	char		pkglib_path[MAXPGPATH];

	/*
	 * These are only used by backend processes, but it's here because passing
	 * a socket needs some special handling on Windows. 'client_sock' is an
	 * explicit argument to postmaster_child_launch, but is stored in
	 * MyClientSocket in the child process.
	 */
	ClientSocket client_sock;
	InheritableSocket inh_sock;
} BackendParameters;

static void read_backend_variables(char *id, char **startup_data, size_t *startup_data_len);
static void restore_backend_variables(BackendParameters *param);

#ifndef WIN32
static bool save_backend_variables(BackendParameters *param, ClientSocket *client_sock);
#else
static bool save_backend_variables(BackendParameters *param, ClientSocket *client_sock,
								   HANDLE childProcess, pid_t childPid);
#endif
#endif							/* EXEC_BACKEND */

static void InitPostmasterChild(bool am_syslogger);


#ifdef EXEC_BACKEND
static pid_t internal_forkexec(const char *entry_name, char *startup_data, size_t startup_data_len, ClientSocket *client_sock);
#endif							/* EXEC_BACKEND */

#ifndef WIN32
static pid_t fork_process(void);
#endif

typedef struct PMChildEntry
{
	const char *name;
	ChildEntryPoint entry_fn;
	bool		shmem_attach;
}			PMChildEntry;

const		PMChildEntry entry_kinds[] = {
	{"backend", BackendMain, true},

	{"autovacuum launcher", AutoVacLauncherMain, true},
	{"autovacuum worker", AutoVacWorkerMain, true},
	{"bgworker", BackgroundWorkerMain, true},
	{"syslogger", SysLoggerMain, false},

	{"startup", StartupProcessMain, true},
	{"bgwriter", BackgroundWriterMain, true},
	{"archiver", PgArchiverMain, true},
	{"checkpointer", CheckpointerMain, true},
	{"wal_writer", WalWriterMain, true},
	{"wal_receiver", WalReceiverMain, true},
};

const char *
PostmasterChildName(PostmasterChildType child_type)
{
	Assert(child_type >= 0 && child_type < lengthof(entry_kinds));
	return entry_kinds[child_type].name;
}

/*
 * Start a new postmaster child process.
 *
 * The startup_data must be a contigous block that is passed to the child
 * process.
 *
 * (In fork mode, it's inherited directly by the child process. In fork+exec mode,
 * it is written to a file and read back in the child process)
 */
pid_t
postmaster_child_launch(PostmasterChildType child_type, char *startup_data, size_t startup_data_len, ClientSocket *client_sock)
{
	pid_t		pid;

	Assert(child_type >= 0 && child_type < lengthof(entry_kinds));
	Assert(IsPostmasterEnvironment && !IsUnderPostmaster);

#ifdef EXEC_BACKEND
	pid = internal_forkexec(entry_kinds[child_type].name, startup_data, startup_data_len, client_sock);
	/* the child process will arrive in SubPostmasterMain */
#else							/* !EXEC_BACKEND */
	pid = fork_process();
	if (pid == 0)				/* child */
	{
		/* Detangle from postmaster */
		InitPostmasterChild(child_type == PMC_SYSLOGGER);

		/*
		 * Before blowing away PostmasterContext, save the startup data
		 */
		MemoryContextSwitchTo(TopMemoryContext);
		if (startup_data != NULL)
		{
			char	   *cp = palloc(startup_data_len);

			memcpy(cp, startup_data, startup_data_len);
			startup_data = cp;
		}

		if (client_sock)
		{
			MyClientSocket = palloc(sizeof(ClientSocket));
			memcpy(MyClientSocket, client_sock, sizeof(ClientSocket));
		}

		entry_kinds[child_type].entry_fn(startup_data, startup_data_len);
		Assert(false);
	}
#endif							/* EXEC_BACKEND */
	return pid;
}

#ifndef WIN32
/*
 * Wrapper for fork(). Return values are the same as those for fork():
 * -1 if the fork failed, 0 in the child process, and the PID of the
 * child in the parent process.  Signals are blocked while forking, so
 * the child must unblock.
 *
 * XXX from fork_process.c file header:
 *	 A simple wrapper on top of fork(). This does not handle the
 *	 EXEC_BACKEND case; it might be extended to do so, but it would be
 *	 considerably more complex.
 */
static pid_t
fork_process(void)
{
	pid_t		result;
	const char *oomfilename;
	sigset_t	save_mask;

#ifdef LINUX_PROFILE
	struct itimerval prof_itimer;
#endif

	/*
	 * Flush stdio channels just before fork, to avoid double-output problems.
	 */
	fflush(NULL);

#ifdef LINUX_PROFILE

	/*
	 * Linux's fork() resets the profiling timer in the child process. If we
	 * want to profile child processes then we need to save and restore the
	 * timer setting.  This is a waste of time if not profiling, however, so
	 * only do it if commanded by specific -DLINUX_PROFILE switch.
	 */
	getitimer(ITIMER_PROF, &prof_itimer);
#endif

	/*
	 * We start postmaster children with signals blocked.  This allows them to
	 * install their own handlers before unblocking, to avoid races where they
	 * might run the postmaster's handler and miss an important control
	 * signal. With more analysis this could potentially be relaxed.
	 */
	sigprocmask(SIG_SETMASK, &BlockSig, &save_mask);
	result = fork();
	if (result == 0)
	{
		/* fork succeeded, in child */
#ifdef LINUX_PROFILE
		setitimer(ITIMER_PROF, &prof_itimer, NULL);
#endif

		/*
		 * By default, Linux tends to kill the postmaster in out-of-memory
		 * situations, because it blames the postmaster for the sum of child
		 * process sizes *including shared memory*.  (This is unbelievably
		 * stupid, but the kernel hackers seem uninterested in improving it.)
		 * Therefore it's often a good idea to protect the postmaster by
		 * setting its OOM score adjustment negative (which has to be done in
		 * a root-owned startup script).  Since the adjustment is inherited by
		 * child processes, this would ordinarily mean that all the
		 * postmaster's children are equally protected against OOM kill, which
		 * is not such a good idea.  So we provide this code to allow the
		 * children to change their OOM score adjustments again.  Both the
		 * file name to write to and the value to write are controlled by
		 * environment variables, which can be set by the same startup script
		 * that did the original adjustment.
		 */
		oomfilename = getenv("PG_OOM_ADJUST_FILE");

		if (oomfilename != NULL)
		{
			/*
			 * Use open() not stdio, to ensure we control the open flags. Some
			 * Linux security environments reject anything but O_WRONLY.
			 */
			int			fd = open(oomfilename, O_WRONLY, 0);

			/* We ignore all errors */
			if (fd >= 0)
			{
				const char *oomvalue = getenv("PG_OOM_ADJUST_VALUE");
				int			rc;

				if (oomvalue == NULL)	/* supply a useful default */
					oomvalue = "0";

				rc = write(fd, oomvalue, strlen(oomvalue));
				(void) rc;
				close(fd);
			}
		}

		/* do post-fork initialization for random number generation */
		pg_strong_random_init();
	}
	else
	{
		/* in parent, restore signal mask */
		sigprocmask(SIG_SETMASK, &save_mask, NULL);
	}

	return result;
}

#endif							/* !WIN32 */

#ifdef EXEC_BACKEND
#ifndef WIN32

/*
 * internal_forkexec non-win32 implementation
 *
 * - writes out backend variables to the parameter file
 * - fork():s, and then exec():s the child process
 */
static pid_t
internal_forkexec(const char *entry_name, char *startup_data, size_t startup_data_len, ClientSocket *client_sock)
{
	static unsigned long tmpBackendFileNum = 0;
	pid_t		pid;
	char		tmpfilename[MAXPGPATH];
	BackendParameters param;
	FILE	   *fp;
	char	   *argv[4];
	char		forkav[MAXPGPATH];

	if (!save_backend_variables(&param, client_sock))
		return -1;				/* log made by save_backend_variables */

	/* Calculate name for temp file */
	snprintf(tmpfilename, MAXPGPATH, "%s/%s.backend_var.%d.%lu",
			 PG_TEMP_FILES_DIR, PG_TEMP_FILE_PREFIX,
			 MyProcPid, ++tmpBackendFileNum);

	/* Open file */
	fp = AllocateFile(tmpfilename, PG_BINARY_W);
	if (!fp)
	{
		/*
		 * As in OpenTemporaryFileInTablespace, try to make the temp-file
		 * directory, ignoring errors.
		 */
		(void) MakePGDirectory(PG_TEMP_FILES_DIR);

		fp = AllocateFile(tmpfilename, PG_BINARY_W);
		if (!fp)
		{
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not create file \"%s\": %m",
							tmpfilename)));
			return -1;
		}
	}

	if (fwrite(&param, sizeof(param), 1, fp) != 1)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m", tmpfilename)));
		FreeFile(fp);
		return -1;
	}

	/* write startup data */
	if (fwrite((char *) &startup_data_len, sizeof(size_t), 1, fp) != 1)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m", tmpfilename)));
		FreeFile(fp);
		return -1;
	}
	if (startup_data_len > 0)
	{
		if (fwrite(startup_data, startup_data_len, 1, fp) != 1)
		{
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m", tmpfilename)));
			FreeFile(fp);
			return -1;
		}
	}

	/* Release file */
	if (FreeFile(fp))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m", tmpfilename)));
		return -1;
	}

	/* set up argv properly */
	argv[0] = "postgres";
	snprintf(forkav, MAXPGPATH, "--forkchild=%s", entry_name);
	argv[1] = forkav;
	/* Insert temp file name after --fork argument */
	argv[2] = tmpfilename;
	argv[3] = NULL;

	/* Fire off execv in child */
	if ((pid = fork_process()) == 0)
	{
		if (execv(postgres_exec_path, argv) < 0)
		{
			ereport(LOG,
					(errmsg("could not execute server process \"%s\": %m",
							postgres_exec_path)));
			/* We're already in the child process here, can't return */
			exit(1);
		}
	}

	return pid;					/* Parent returns pid, or -1 on fork failure */
}
#else							/* WIN32 */

/*
 * internal_forkexec win32 implementation
 *
 * - starts backend using CreateProcess(), in suspended state
 * - writes out backend variables to the parameter file
 *	- during this, duplicates handles and sockets required for
 *	  inheritance into the new process
 * - resumes execution of the new process once the backend parameter
 *	 file is complete.
 */
static pid_t
internal_forkexec(const char *entry_name, char *startup_data, size_t startup_data_len, ClientSocket *client_sock)
{
	int			retry_count = 0;
	STARTUPINFO si;
	PROCESS_INFORMATION pi;
	int			i;
	int			j;
	char		cmdLine[MAXPGPATH * 2];
	HANDLE		paramHandle;
	BackendParameters *param;
	SECURITY_ATTRIBUTES sa;
	char		paramHandleStr[32];
	win32_deadchild_waitinfo *childinfo;
	char	   *argv[4];

	/* Resume here if we need to retry */
retry:

	/* Set up shared memory for parameter passing */
	ZeroMemory(&sa, sizeof(sa));
	sa.nLength = sizeof(sa);
	sa.bInheritHandle = TRUE;
	paramHandle = CreateFileMapping(INVALID_HANDLE_VALUE,
									&sa,
									PAGE_READWRITE,
									0,
									sizeof(BackendParameters),
									NULL);
	if (paramHandle == INVALID_HANDLE_VALUE)
	{
		ereport(LOG,
				(errmsg("could not create backend parameter file mapping: error code %lu",
						GetLastError())));
		return -1;
	}

	param = MapViewOfFile(paramHandle, FILE_MAP_WRITE, 0, 0, sizeof(BackendParameters));
	if (!param)
	{
		ereport(LOG,
				(errmsg("could not map backend parameter memory: error code %lu",
						GetLastError())));
		CloseHandle(paramHandle);
		return -1;
	}

	/* Insert temp file name after --fork argument */
#ifdef _WIN64
	sprintf(paramHandleStr, "%llu", (LONG_PTR) paramHandle);
#else
	sprintf(paramHandleStr, "%lu", (DWORD) paramHandle);
#endif

	/* set up argv properly */
	argv[0] = "postgres";
	snprintf(forkav, MAXPGPATH, "--forkchild=%s", entry_name);
	argv[1] = forkav;
	/* Insert temp file name after --fork argument */
	argv[2] = tmpfilename;
	argv[3] = NULL;

	/* Format the cmd line */
	cmdLine[sizeof(cmdLine) - 1] = '\0';
	cmdLine[sizeof(cmdLine) - 2] = '\0';
	snprintf(cmdLine, sizeof(cmdLine) - 1, "\"%s\"", postgres_exec_path);
	i = 0;
	while (argv[++i] != NULL)
	{
		j = strlen(cmdLine);
		snprintf(cmdLine + j, sizeof(cmdLine) - 1 - j, " \"%s\"", argv[i]);
	}
	if (cmdLine[sizeof(cmdLine) - 2] != '\0')
	{
		ereport(LOG,
				(errmsg("subprocess command line too long")));
		UnmapViewOfFile(param);
		CloseHandle(paramHandle);
		return -1;
	}

	memset(&pi, 0, sizeof(pi));
	memset(&si, 0, sizeof(si));
	si.cb = sizeof(si);

	/*
	 * Create the subprocess in a suspended state. This will be resumed later,
	 * once we have written out the parameter file.
	 */
	if (!CreateProcess(NULL, cmdLine, NULL, NULL, TRUE, CREATE_SUSPENDED,
					   NULL, NULL, &si, &pi))
	{
		ereport(LOG,
				(errmsg("CreateProcess() call failed: %m (error code %lu)",
						GetLastError())));
		UnmapViewOfFile(param);
		CloseHandle(paramHandle);
		return -1;
	}

	if (!save_backend_variables(param, client_sock, pi.hProcess, pi.dwProcessId))
	{
		/*
		 * log made by save_backend_variables, but we have to clean up the
		 * mess with the half-started process
		 */
		if (!TerminateProcess(pi.hProcess, 255))
			ereport(LOG,
					(errmsg_internal("could not terminate unstarted process: error code %lu",
									 GetLastError())));
		CloseHandle(pi.hProcess);
		CloseHandle(pi.hThread);
		UnmapViewOfFile(param);
		CloseHandle(paramHandle);
		return -1;				/* log made by save_backend_variables */
	}

	/* Drop the parameter shared memory that is now inherited to the backend */
	if (!UnmapViewOfFile(param))
		ereport(LOG,
				(errmsg("could not unmap view of backend parameter file: error code %lu",
						GetLastError())));
	if (!CloseHandle(paramHandle))
		ereport(LOG,
				(errmsg("could not close handle to backend parameter file: error code %lu",
						GetLastError())));

	/*
	 * Reserve the memory region used by our main shared memory segment before
	 * we resume the child process.  Normally this should succeed, but if ASLR
	 * is active then it might sometimes fail due to the stack or heap having
	 * gotten mapped into that range.  In that case, just terminate the
	 * process and retry.
	 */
	if (!pgwin32_ReserveSharedMemoryRegion(pi.hProcess))
	{
		/* pgwin32_ReserveSharedMemoryRegion already made a log entry */
		if (!TerminateProcess(pi.hProcess, 255))
			ereport(LOG,
					(errmsg_internal("could not terminate process that failed to reserve memory: error code %lu",
									 GetLastError())));
		CloseHandle(pi.hProcess);
		CloseHandle(pi.hThread);
		if (++retry_count < 100)
			goto retry;
		ereport(LOG,
				(errmsg("giving up after too many tries to reserve shared memory"),
				 errhint("This might be caused by ASLR or antivirus software.")));
		return -1;
	}

	/*
	 * Now that the backend variables are written out, we start the child
	 * thread so it can start initializing while we set up the rest of the
	 * parent state.
	 */
	if (ResumeThread(pi.hThread) == -1)
	{
		if (!TerminateProcess(pi.hProcess, 255))
		{
			ereport(LOG,
					(errmsg_internal("could not terminate unstartable process: error code %lu",
									 GetLastError())));
			CloseHandle(pi.hProcess);
			CloseHandle(pi.hThread);
			return -1;
		}
		CloseHandle(pi.hProcess);
		CloseHandle(pi.hThread);
		ereport(LOG,
				(errmsg_internal("could not resume thread of unstarted process: error code %lu",
								 GetLastError())));
		return -1;
	}

	/*
	 * Queue a waiter to signal when this child dies. The wait will be handled
	 * automatically by an operating system thread pool.  The memory will be
	 * freed by a later call to waitpid().
	 */
	childinfo = palloc(sizeof(win32_deadchild_waitinfo));
	childinfo->procHandle = pi.hProcess;
	childinfo->procId = pi.dwProcessId;

	if (!RegisterWaitForSingleObject(&childinfo->waitHandle,
									 pi.hProcess,
									 pgwin32_deadchild_callback,
									 childinfo,
									 INFINITE,
									 WT_EXECUTEONLYONCE | WT_EXECUTEINWAITTHREAD))
		ereport(FATAL,
				(errmsg_internal("could not register process for wait: error code %lu",
								 GetLastError())));

	/* Don't close pi.hProcess here - waitpid() needs access to it */

	CloseHandle(pi.hThread);

	return pi.dwProcessId;
}
#endif							/* WIN32 */

/*
 * SubPostmasterMain -- Get the fork/exec'd process into a state equivalent
 *			to what it would be if we'd simply forked on Unix, and then
 *			dispatch to the appropriate place.
 *
 * The first two command line arguments are expected to be "--forkFOO"
 * (where FOO indicates which postmaster child we are to become), and
 * the name of a variables file that we can read to load data that would
 * have been inherited by fork() on Unix.  Remaining arguments go to the
 * subprocess FooMain() routine. XXX
 */
void
SubPostmasterMain(int argc, char *argv[])
{
	PostmasterChildType child_type;
	char	   *startup_data;
	size_t		startup_data_len;

	/* In EXEC_BACKEND case we will not have inherited these settings */
	IsPostmasterEnvironment = true;
	whereToSendOutput = DestNone;

	/* Setup essential subsystems (to ensure elog() behaves sanely) */
	InitializeGUCOptions();

	/* Check we got appropriate args */
	if (argc < 3)
		elog(FATAL, "invalid subpostmaster invocation");

	if (strncmp(argv[1], "--forkchild=", 12) == 0)
	{
		char	   *entry_name = argv[1] + 12;
		bool		found = false;

		for (int idx = 0; idx < lengthof(entry_kinds); idx++)
		{
			if (strcmp(entry_kinds[idx].name, entry_name) == 0)
			{
				child_type = idx;
				found = true;
				break;
			}
		}
		if (!found)
			elog(ERROR, "unknown child kind %s", entry_name);
	}

	/* Read in the variables file */
	read_backend_variables(argv[2], &startup_data, &startup_data_len);

	/* Setup as postmaster child */
	InitPostmasterChild(child_type == PMC_SYSLOGGER);

	/*
	 * If appropriate, physically re-attach to shared memory segment. We want
	 * to do this before going any further to ensure that we can attach at the
	 * same address the postmaster used.  On the other hand, if we choose not
	 * to re-attach, we may have other cleanup to do.
	 *
	 * If testing EXEC_BACKEND on Linux, you should run this as root before
	 * starting the postmaster:
	 *
	 * sysctl -w kernel.randomize_va_space=0
	 *
	 * This prevents using randomized stack and code addresses that cause the
	 * child process's memory map to be different from the parent's, making it
	 * sometimes impossible to attach to shared memory at the desired address.
	 * Return the setting to its old value (usually '1' or '2') when finished.
	 */
	if (entry_kinds[child_type].shmem_attach)
		PGSharedMemoryReAttach();
	else
		PGSharedMemoryNoReAttach();

	/* Read in remaining GUC variables */
	read_nondefault_variables();

	/*
	 * Check that the data directory looks valid, which will also check the
	 * privileges on the data directory and update our umask and file/group
	 * variables for creating files later.  Note: this should really be done
	 * before we create any files or directories.
	 */
	checkDataDir();

	/*
	 * (re-)read control file, as it contains config. The postmaster will
	 * already have read this, but this process doesn't know about that.
	 */
	LocalProcessControlFile(false);

	/*
	 * Reload any libraries that were preloaded by the postmaster.  Since we
	 * exec'd this process, those libraries didn't come along with us; but we
	 * should load them into all child processes to be consistent with the
	 * non-EXEC_BACKEND behavior.
	 */
	process_shared_preload_libraries();

	/* Restore basic shared memory pointers */
	if (UsedShmemSegAddr != NULL)
		InitShmemAccess(UsedShmemSegAddr);

	/* Run backend or appropriate child */
	entry_kinds[child_type].entry_fn(startup_data, startup_data_len);

	abort();					/* shouldn't get here */
}
#endif							/* EXEC_BACKEND */

/* ----------------------------------------------------------------
 *	common process startup code
 * ----------------------------------------------------------------
 */

/*
 * Initialize the basic environment for a postmaster child
 *
 * Should be called as early as possible after the child's startup. However,
 * on EXEC_BACKEND builds it does need to be after read_backend_variables().
 */
static void
InitPostmasterChild(bool am_syslogger)
{
	/* Close the postmaster's sockets (as soon as we know them) */
	ClosePostmasterPorts(am_syslogger);

	IsUnderPostmaster = true;	/* we are a postmaster subprocess now */

	/*
	 * Start our win32 signal implementation. This has to be done after we
	 * read the backend variables, because we need to pick up the signal pipe
	 * from the parent process.
	 */
#ifdef WIN32
	pgwin32_signal_initialize();
#endif

	/*
	 * Set reference point for stack-depth checking.  This might seem
	 * redundant in !EXEC_BACKEND builds; but it's not because the postmaster
	 * launches its children from signal handlers, so we might be running on
	 * an alternative stack. XXX still true?
	 */
	(void) set_stack_base();

	InitProcessGlobals();

	/*
	 * make sure stderr is in binary mode before anything can possibly be
	 * written to it, in case it's actually the syslogger pipe, so the pipe
	 * chunking protocol isn't disturbed. Non-logpipe data gets translated on
	 * redirection (e.g. via pg_ctl -l) anyway.
	 */
#ifdef WIN32
	_setmode(fileno(stderr), _O_BINARY);
#endif

	/* We don't want the postmaster's proc_exit() handlers */
	on_exit_reset();

	/* In EXEC_BACKEND case we will not have inherited BlockSig etc values */
#ifdef EXEC_BACKEND
	pqinitmask();
#endif

	/* Initialize process-local latch support */
	InitializeLatchSupport();
	InitProcessLocalLatch();
	InitializeLatchWaitSet();

	/*
	 * If possible, make this process a group leader, so that the postmaster
	 * can signal any child processes too. Not all processes will have
	 * children, but for consistency we make all postmaster child processes do
	 * this.
	 */
#ifdef HAVE_SETSID
	if (setsid() < 0)
		elog(FATAL, "setsid() failed: %m");
#endif

	/*
	 * Every postmaster child process is expected to respond promptly to
	 * SIGQUIT at all times.  Therefore we centrally remove SIGQUIT from
	 * BlockSig and install a suitable signal handler.  (Client-facing
	 * processes may choose to replace this default choice of handler with
	 * quickdie().)  All other blockable signals remain blocked for now.
	 */
	pqsignal(SIGQUIT, SignalHandlerForCrashExit);

	sigdelset(&BlockSig, SIGQUIT);
	sigprocmask(SIG_SETMASK, &BlockSig, NULL);

	/* Request a signal if the postmaster dies, if possible. */
	PostmasterDeathSignalInit();

	/* Don't give the pipe to subprograms that we execute. */
#ifndef WIN32
	if (fcntl(postmaster_alive_fds[POSTMASTER_FD_WATCH], F_SETFD, FD_CLOEXEC) < 0)
		ereport(FATAL,
				(errcode_for_socket_access(),
				 errmsg_internal("could not set postmaster death monitoring pipe to FD_CLOEXEC mode: %m")));
#endif
}

/*
 * Initialize the basic environment for a standalone process.
 *
 * argv0 has to be suitable to find the program's executable.
 */
void
InitStandaloneProcess(const char *argv0)
{
	Assert(!IsPostmasterEnvironment);

	MyBackendType = B_STANDALONE_BACKEND;

	/*
	 * Start our win32 signal implementation
	 */
#ifdef WIN32
	pgwin32_signal_initialize();
#endif

	InitProcessGlobals();

	/* Initialize process-local latch support */
	InitializeLatchSupport();
	InitProcessLocalLatch();
	InitializeLatchWaitSet();

	/*
	 * For consistency with InitPostmasterChild, initialize signal mask here.
	 * But we don't unblock SIGQUIT or provide a default handler for it.
	 */
	pqinitmask();
	sigprocmask(SIG_SETMASK, &BlockSig, NULL);

	/* Compute paths, no postmaster to inherit from */
	if (my_exec_path[0] == '\0')
	{
		if (find_my_exec(argv0, my_exec_path) < 0)
			elog(FATAL, "%s: could not locate my own executable path",
				 argv0);
	}

	if (pkglib_path[0] == '\0')
		get_pkglib_path(my_exec_path, pkglib_path);
}


#ifdef EXEC_BACKEND

/*
 * The following need to be available to the save/restore_backend_variables
 * functions.  They are marked NON_EXEC_STATIC in their home modules.
 */
extern slock_t *ShmemLock;
extern slock_t *ProcStructLock;
extern PGPROC *AuxiliaryProcs;
extern PMSignalData *PMSignalState;
extern pg_time_t first_syslogger_file_time;
extern struct bkend *ShmemBackendArray;
extern bool redirection_done;

#ifndef WIN32
#define write_inheritable_socket(dest, src, childpid) ((*(dest) = (src)), true)
#define read_inheritable_socket(dest, src) (*(dest) = *(src))
#else
static bool write_duplicated_handle(HANDLE *dest, HANDLE src, HANDLE child);
static bool write_inheritable_socket(InheritableSocket *dest, SOCKET src,
									 pid_t childPid);
static void read_inheritable_socket(SOCKET *dest, InheritableSocket *src);
#endif


/* Save critical backend variables into the BackendParameters struct */
#ifndef WIN32
static bool
save_backend_variables(BackendParameters *param, ClientSocket *client_sock)
#else
static bool
save_backend_variables(BackendParameters *param, ClientSocket *client_sock,
					   HANDLE childProcess, pid_t childPid)
#endif
{
	if (client_sock)
		memcpy(&param->client_sock, client_sock, sizeof(ClientSocket));
	else
		memset(&param->client_sock, 0, sizeof(ClientSocket));
	if (!write_inheritable_socket(&param->inh_sock,
								  client_sock ? client_sock->sock : PGINVALID_SOCKET,
								  childPid))
		return false;

	strlcpy(param->DataDir, DataDir, MAXPGPATH);

	param->MyCancelKey = MyCancelKey;
	param->MyPMChildSlot = MyPMChildSlot;

#ifdef WIN32
	param->ShmemProtectiveRegion = ShmemProtectiveRegion;
#endif
	param->UsedShmemSegID = UsedShmemSegID;
	param->UsedShmemSegAddr = UsedShmemSegAddr;

	param->ShmemLock = ShmemLock;
	param->ShmemVariableCache = ShmemVariableCache;
	param->ShmemBackendArray = ShmemBackendArray;

#ifndef HAVE_SPINLOCKS
	param->SpinlockSemaArray = SpinlockSemaArray;
#endif
	param->NamedLWLockTrancheRequests = NamedLWLockTrancheRequests;
	param->NamedLWLockTrancheArray = NamedLWLockTrancheArray;
	param->MainLWLockArray = MainLWLockArray;
	param->ProcStructLock = ProcStructLock;
	param->ProcGlobal = ProcGlobal;
	param->AuxiliaryProcs = AuxiliaryProcs;
	param->PreparedXactProcs = PreparedXactProcs;
	param->PMSignalState = PMSignalState;

	param->PostmasterPid = PostmasterPid;
	param->PgStartTime = PgStartTime;
	param->PgReloadTime = PgReloadTime;
	param->first_syslogger_file_time = first_syslogger_file_time;

	param->redirection_done = redirection_done;
	param->IsBinaryUpgrade = IsBinaryUpgrade;
	param->query_id_enabled = query_id_enabled;
	param->max_safe_fds = max_safe_fds;

	param->MaxBackends = MaxBackends;

#ifdef WIN32
	param->PostmasterHandle = PostmasterHandle;
	if (!write_duplicated_handle(&param->initial_signal_pipe,
								 pgwin32_create_signal_listener(childPid),
								 childProcess))
		return false;
#else
	memcpy(&param->postmaster_alive_fds, &postmaster_alive_fds,
		   sizeof(postmaster_alive_fds));
#endif

	memcpy(&param->syslogPipe, &syslogPipe, sizeof(syslogPipe));

	strlcpy(param->my_exec_path, my_exec_path, MAXPGPATH);

	strlcpy(param->pkglib_path, pkglib_path, MAXPGPATH);

	return true;
}


#ifdef WIN32
/*
 * Duplicate a handle for usage in a child process, and write the child
 * process instance of the handle to the parameter file.
 */
static bool
write_duplicated_handle(HANDLE *dest, HANDLE src, HANDLE childProcess)
{
	HANDLE		hChild = INVALID_HANDLE_VALUE;

	if (!DuplicateHandle(GetCurrentProcess(),
						 src,
						 childProcess,
						 &hChild,
						 0,
						 TRUE,
						 DUPLICATE_CLOSE_SOURCE | DUPLICATE_SAME_ACCESS))
	{
		ereport(LOG,
				(errmsg_internal("could not duplicate handle to be written to backend parameter file: error code %lu",
								 GetLastError())));
		return false;
	}

	*dest = hChild;
	return true;
}

/*
 * Duplicate a socket for usage in a child process, and write the resulting
 * structure to the parameter file.
 * This is required because a number of LSPs (Layered Service Providers) very
 * common on Windows (antivirus, firewalls, download managers etc) break
 * straight socket inheritance.
 */
static bool
write_inheritable_socket(InheritableSocket *dest, SOCKET src, pid_t childpid)
{
	dest->origsocket = src;
	if (src != 0 && src != PGINVALID_SOCKET)
	{
		/* Actual socket */
		if (WSADuplicateSocket(src, childpid, &dest->wsainfo) != 0)
		{
			ereport(LOG,
					(errmsg("could not duplicate socket %d for use in backend: error code %d",
							(int) src, WSAGetLastError())));
			return false;
		}
	}
	return true;
}

/*
 * Read a duplicate socket structure back, and get the socket descriptor.
 */
static void
read_inheritable_socket(SOCKET *dest, InheritableSocket *src)
{
	SOCKET		s;

	if (src->origsocket == PGINVALID_SOCKET || src->origsocket == 0)
	{
		/* Not a real socket! */
		*dest = src->origsocket;
	}
	else
	{
		/* Actual socket, so create from structure */
		s = WSASocket(FROM_PROTOCOL_INFO,
					  FROM_PROTOCOL_INFO,
					  FROM_PROTOCOL_INFO,
					  &src->wsainfo,
					  0,
					  0);
		if (s == INVALID_SOCKET)
		{
			write_stderr("could not create inherited socket: error code %d\n",
						 WSAGetLastError());
			exit(1);
		}
		*dest = s;

		/*
		 * To make sure we don't get two references to the same socket, close
		 * the original one. (This would happen when inheritance actually
		 * works..
		 */
		closesocket(src->origsocket);
	}
}
#endif

static void
read_backend_variables(char *id, char **startup_data, size_t *startup_data_len)
{
	BackendParameters param;

#ifndef WIN32
	/* Non-win32 implementation reads from file */
	FILE	   *fp;

	/* Open file */
	fp = AllocateFile(id, PG_BINARY_R);
	if (!fp)
	{
		write_stderr("could not open backend variables file \"%s\": %s\n",
					 id, strerror(errno));
		exit(1);
	}

	if (fread(&param, sizeof(param), 1, fp) != 1)
	{
		write_stderr("could not read from backend variables file \"%s\": %s\n",
					 id, strerror(errno));
		exit(1);
	}

	/* read startup data */
	if (fread((char *) startup_data_len, sizeof(size_t), 1, fp) != 1)
	{
		write_stderr("could not read len from backend variables file \"%s\": %s\n",
					 id, strerror(errno));
		exit(1);
	}
	if (*startup_data_len > 0)
	{
		*startup_data = palloc(*startup_data_len);
		if (fread(*startup_data, *startup_data_len, 1, fp) != 1)
		{
			write_stderr("could not read startup data from backend variables file \"%s\": %s\n",
						 id, strerror(errno));
			exit(1);
		}
	}
	else
		*startup_data = NULL;

	/* Release file */
	FreeFile(fp);
	if (unlink(id) != 0)
	{
		write_stderr("could not remove file \"%s\": %s\n",
					 id, strerror(errno));
		exit(1);
	}
#else
	/* Win32 version uses mapped file */
	HANDLE		paramHandle;
	BackendParameters *paramp;

#ifdef _WIN64
	paramHandle = (HANDLE) _atoi64(id);
#else
	paramHandle = (HANDLE) atol(id);
#endif
	paramp = MapViewOfFile(paramHandle, FILE_MAP_READ, 0, 0, 0);
	if (!paramp)
	{
		write_stderr("could not map view of backend variables: error code %lu\n",
					 GetLastError());
		exit(1);
	}

	memcpy(&param, paramp, sizeof(BackendParameters));

	/* XXX: read startup data */

	if (!UnmapViewOfFile(paramp))
	{
		write_stderr("could not unmap view of backend variables: error code %lu\n",
					 GetLastError());
		exit(1);
	}

	if (!CloseHandle(paramHandle))
	{
		write_stderr("could not close handle to backend parameter variables: error code %lu\n",
					 GetLastError());
		exit(1);
	}
#endif

	restore_backend_variables(&param);
}

/* Restore critical backend variables from the BackendParameters struct */
static void
restore_backend_variables(BackendParameters *param)
{
	if (param->client_sock.sock != PGINVALID_SOCKET)
	{
		MyClientSocket = MemoryContextAlloc(TopMemoryContext, sizeof(ClientSocket));
		memcpy(MyClientSocket, &param->client_sock, sizeof(ClientSocket));
		read_inheritable_socket(&MyClientSocket->sock, &param->inh_sock);
	}

	SetDataDir(param->DataDir);

	MyCancelKey = param->MyCancelKey;
	MyPMChildSlot = param->MyPMChildSlot;

#ifdef WIN32
	ShmemProtectiveRegion = param->ShmemProtectiveRegion;
#endif
	UsedShmemSegID = param->UsedShmemSegID;
	UsedShmemSegAddr = param->UsedShmemSegAddr;

	ShmemLock = param->ShmemLock;
	ShmemVariableCache = param->ShmemVariableCache;
	ShmemBackendArray = param->ShmemBackendArray;

#ifndef HAVE_SPINLOCKS
	SpinlockSemaArray = param->SpinlockSemaArray;
#endif
	NamedLWLockTrancheRequests = param->NamedLWLockTrancheRequests;
	NamedLWLockTrancheArray = param->NamedLWLockTrancheArray;
	MainLWLockArray = param->MainLWLockArray;
	ProcStructLock = param->ProcStructLock;
	ProcGlobal = param->ProcGlobal;
	AuxiliaryProcs = param->AuxiliaryProcs;
	PreparedXactProcs = param->PreparedXactProcs;
	PMSignalState = param->PMSignalState;

	PostmasterPid = param->PostmasterPid;
	PgStartTime = param->PgStartTime;
	PgReloadTime = param->PgReloadTime;
	first_syslogger_file_time = param->first_syslogger_file_time;

	redirection_done = param->redirection_done;
	IsBinaryUpgrade = param->IsBinaryUpgrade;
	query_id_enabled = param->query_id_enabled;
	max_safe_fds = param->max_safe_fds;

	MaxBackends = param->MaxBackends;

#ifdef WIN32
	PostmasterHandle = param->PostmasterHandle;
	pgwin32_initial_signal_pipe = param->initial_signal_pipe;
#else
	memcpy(&postmaster_alive_fds, &param->postmaster_alive_fds,
		   sizeof(postmaster_alive_fds));
#endif

	memcpy(&syslogPipe, &param->syslogPipe, sizeof(syslogPipe));

	strlcpy(my_exec_path, param->my_exec_path, MAXPGPATH);

	strlcpy(pkglib_path, param->pkglib_path, MAXPGPATH);

	/*
	 * We need to restore fd.c's counts of externally-opened FDs; to avoid
	 * confusion, be sure to do this after restoring max_safe_fds.  (Note:
	 * BackendInitialize will handle this for client_sock->sock.)
	 */
#ifndef WIN32
	if (postmaster_alive_fds[0] >= 0)
		ReserveExternalFD();
	if (postmaster_alive_fds[1] >= 0)
		ReserveExternalFD();
#endif
}

#endif							/* EXEC_BACKEND */
