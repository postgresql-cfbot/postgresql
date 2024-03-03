/*-------------------------------------------------------------------------
 *
 * procbacktrace.c
 *	  Backtrace-related routines
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/procbacktrace.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#ifdef HAVE_EXECINFO_H
#include <execinfo.h>
#endif

#include "funcapi.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procbacktrace.h"
#include "storage/procsignal.h"
#include "utils/fmgrprotos.h"

#ifdef HAVE_EXECINFO_H
static volatile sig_atomic_t backtrace_functions_loaded = false;
#endif

/*
 * Handle receipt of an interrupt indicating logging of current backtrace.
 *
 * We capture the backtrace within this signal handler itself in a safe manner.
 * It is ensured that no memory allocations happen here. This function emits
 * backtrace to stderr as writing to server log allocates memory, making the
 * signal halder unsafe. Also, the shared library implementing the
 * backtrace-related functions is preloaded to avoid memory allocations upon
 * first-time loading, see LoadBacktraceFunctions.
 */
void
HandleLogBacktraceInterrupt(void)
{
#ifdef HAVE_EXECINFO_H
	void	   *buf[100];
	int			nframes;

	if (!backtrace_functions_loaded)
		return;

	nframes = backtrace(buf, lengthof(buf));

	write_stderr("logging current backtrace of process with PID %d:\n",
				 MyProcPid);
	backtrace_symbols_fd(buf, nframes, fileno(stderr));
#endif
}

/*
 * Load backtrace shared library.
 *
 * Any backtrace-related functions when called for the first time dynamically
 * loads the shared library, which usually triggers a call to malloc, making
 * them unsafe to use in signal handlers. This function makes backtrace-related
 * functions signal-safe.
 *
 * Note that this function is supposed to be called in the early life of a
 * process, preferably after signal handlers are setup; but not from within a
 * signal handler.
 */
void
LoadBacktraceFunctions(void)
{
#ifdef HAVE_BACKTRACE_SYMBOLS
	void	   *buf[2];

	if (backtrace_functions_loaded)
		return;

	/*
	 * It is enough to call any one backtrace-related function to ensure that
	 * the corresponding shared library is dynamically loaded. We just load
	 * two most recent function calls, as we don't use the backtrace anyway.
	 */
	backtrace(buf, lengthof(buf));

	backtrace_functions_loaded = true;
#endif
}

/*
 * pg_log_backend_backtrace
 *		Signal a backend or an auxiliary process to log its current backtrace.
 *
 * By default, only superusers are allowed to signal to log the backtrace
 * because allowing any users to issue this request at an unbounded
 * rate would cause lots of log messages on stderr and which can lead to
 * denial of service. Additional roles can be permitted with GRANT.
 *
 * On receipt of this signal, a backend or an auxiliary process emits the
 * current backtrace to stderr in the signal handler.
 */
Datum
pg_log_backend_backtrace(PG_FUNCTION_ARGS)
{
#ifdef HAVE_BACKTRACE_SYMBOLS
	int			pid = PG_GETARG_INT32(0);
	PGPROC	   *proc;
	BackendId	backendId = InvalidBackendId;

	proc = BackendPidGetProc(pid);

	/*
	 * See if the process with given pid is a backend or an auxiliary process.
	 *
	 * If the given process is a backend, use its backend id in
	 * SendProcSignal() later to speed up the operation. Otherwise, don't do
	 * that because auxiliary processes (except the startup process) don't
	 * have a valid backend id.
	 */
	if (proc != NULL)
		backendId = proc->backendId;
	else
		proc = AuxiliaryPidGetProc(pid);

	/*
	 * BackendPidGetProc() and AuxiliaryPidGetProc() return NULL if the pid
	 * isn't valid; but by the time we reach kill(), a process for which we
	 * get a valid proc here might have terminated on its own.  There's no way
	 * to acquire a lock on an arbitrary process to prevent that. But since
	 * this mechanism is usually used to debug a backend or an auxiliary
	 * process running and consuming lots of memory, that it might end on its
	 * own first and its memory contexts are not logged is not a problem.
	 */
	if (proc == NULL)
	{
		/*
		 * This is just a warning so a loop-through-resultset will not abort
		 * if one backend terminated on its own during the run.
		 */
		ereport(WARNING,
				(errmsg("PID %d is not a PostgreSQL server process", pid)));
		PG_RETURN_BOOL(false);
	}

	if (SendProcSignal(pid, PROCSIG_LOG_BACKTRACE, backendId) < 0)
	{
		/* Again, just a warning to allow loops */
		ereport(WARNING,
				(errmsg("could not send signal to process %d: %m", pid)));
		PG_RETURN_BOOL(false);
	}

	PG_RETURN_BOOL(true);
#else
	ereport(WARNING,
			errmsg("backtrace generation is not supported by this installation"),
			errhint("You need to build PostgreSQL with library containing backtrace_symbols."));

	PG_RETURN_BOOL(false);
#endif
}
