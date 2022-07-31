/*-------------------------------------------------------------------------
 *
 * custodian.c
 *
 * The custodian process handles a variety of non-critical tasks that might
 * otherwise delay startup, checkpointing, etc.  Offloaded tasks should not
 * be synchronous (e.g., checkpointing shouldn't wait for the custodian to
 * complete a task before proceeding).  However, tasks can be synchronously
 * executed when necessary (e.g., single-user mode).  The custodian is not
 * an essential process and can shutdown quickly when requested.  The
 * custodian only wakes up to perform its tasks when its latch is set.
 *
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *   src/backend/postmaster/custodian.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/rewriteheap.h"
#include "libpq/pqsignal.h"
#include "pgstat.h"
#include "postmaster/custodian.h"
#include "postmaster/interrupt.h"
#include "replication/snapbuild.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/smgr.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"

static void DoCustodianTasks(bool retry);
static CustodianTask CustodianGetNextTask(void);
static void CustodianEnqueueTask(CustodianTask task);
static const struct cust_task_funcs_entry *LookupCustodianFunctions(CustodianTask task);
static void CustodianSetLogicalRewriteCutoff(Datum arg);

typedef struct
{
	slock_t		cust_lck;

	CustodianTask task_queue_elems[NUM_CUSTODIAN_TASKS];
	int			task_queue_head;

	XLogRecPtr  logical_rewrite_mappings_cutoff;    /* can remove older mappings */
} CustodianShmemStruct;

static CustodianShmemStruct *CustodianShmem;

typedef void (*CustodianTaskFunction) (void);
typedef void (*CustodianTaskHandleArg) (Datum arg);

struct cust_task_funcs_entry
{
	CustodianTask task;
	CustodianTaskFunction task_func;		/* performs task */
	CustodianTaskHandleArg handle_arg_func;	/* handles additional info in request */
};

/*
 * Add new tasks here.
 *
 * task_func is the logic that will be executed via DoCustodianTasks() when the
 * matching task is requested via RequestCustodian().  handle_arg_func is an
 * optional function for providing extra information for the next invocation of
 * the task.  Typically, the extra information should be stored in shared
 * memory for access from the custodian process.  handle_arg_func is invoked
 * before enqueueing the task, and it will still be invoked regardless of
 * whether the task is already enqueued.
 */
static const struct cust_task_funcs_entry cust_task_functions[] = {
	{CUSTODIAN_REMOVE_TEMP_FILES, RemovePgTempFiles, NULL},
	{CUSTODIAN_REMOVE_SERIALIZED_SNAPSHOTS, RemoveOldSerializedSnapshots, NULL},
	{CUSTODIAN_REMOVE_REWRITE_MAPPINGS, RemoveOldLogicalRewriteMappings, CustodianSetLogicalRewriteCutoff},
	{INVALID_CUSTODIAN_TASK, NULL, NULL}	/* must be last */
};

/*
 * Main entry point for custodian process
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
void
CustodianMain(void)
{
	sigjmp_buf	local_sigjmp_buf;
	MemoryContext custodian_context;

	/*
	 * Properly accept or ignore signals that might be sent to us.
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	/* SIGQUIT handler was already set up by InitPostmasterChild */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, SIG_DFL);

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.
	 */
	custodian_context = AllocSetContextCreate(TopMemoryContext,
											  "Custodian",
											  ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(custodian_context);

	/*
	 * If an exception is encountered, processing resumes here.  As with other
	 * auxiliary processes, we cannot use PG_TRY because this is the bottom of
	 * the exception stack.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * These operations are really just a minimal subset of
		 * AbortTransaction().  We don't have very many resources to worry
		 * about.
		 */
		LWLockReleaseAll();
		ConditionVariableCancelSleep();
		AbortBufferIO();
		UnlockBuffers();
		ReleaseAuxProcessResources(false);
		AtEOXact_Buffers(false);
		AtEOXact_SMgr();
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(custodian_context);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(custodian_context);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/*
		 * Sleep at least 1 second after any error.  A write error is likely
		 * to be repeated, and we don't want to be filling the error logs as
		 * fast as we can.
		 */
		pg_usleep(1000000L);

		/*
		 * Close all open files after any error.  This is helpful on Windows,
		 * where holding deleted files open causes various strange errors.
		 * It's not clear we need it elsewhere, but shouldn't hurt.
		 */
		smgrcloseall();

		/* Report wait end here, when there is no further possibility of wait */
		pgstat_report_wait_end();
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	/*
	 * Advertise out latch that backends can use to wake us up while we're
	 * sleeping.
	 */
	ProcGlobal->custodianLatch = &MyProc->procLatch;

	/*
	 * Loop forever
	 */
	for (;;)
	{
		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		HandleMainLoopInterrupts();

		DoCustodianTasks(true);

		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0,
						 WAIT_EVENT_CUSTODIAN_MAIN);
	}

	pg_unreachable();
}

/*
 * DoCustodianTasks
 *		Perform requested custodian tasks
 *
 * If retry is true, the custodian will re-enqueue the currently running task if
 * an exception is encountered.
 */
static void
DoCustodianTasks(bool retry)
{
	CustodianTask	task;

	while ((task = CustodianGetNextTask()) != INVALID_CUSTODIAN_TASK)
	{
		CustodianTaskFunction func = (LookupCustodianFunctions(task))->task_func;

		PG_TRY();
		{
			(*func) ();
		}
		PG_CATCH();
		{
			if (retry)
				CustodianEnqueueTask(task);

			PG_RE_THROW();
		}
		PG_END_TRY();
	}
}

Size
CustodianShmemSize(void)
{
	return sizeof(CustodianShmemStruct);
}

void
CustodianShmemInit(void)
{
	Size		size = CustodianShmemSize();
	bool		found;

	CustodianShmem = (CustodianShmemStruct *)
		ShmemInitStruct("Custodian Data", size, &found);

	if (!found)
	{
		memset(CustodianShmem, 0, size);
		SpinLockInit(&CustodianShmem->cust_lck);
		for (int i = 0; i < NUM_CUSTODIAN_TASKS; i++)
			CustodianShmem->task_queue_elems[i] = INVALID_CUSTODIAN_TASK;
	}
}

/*
 * RequestCustodian
 *		Called to request a custodian task.
 *
 * If immediate is true, the task is performed immediately in the current
 * process, and this function will not return until it completes.  This is
 * mostly useful for single-user mode.  If immediate is false, the task is added
 * to the custodian's queue if it is not already enqueued, and this function
 * returns without waiting for the task to complete.
 *
 * arg can be used to provide additional information to the custodian that is
 * necessary for the task.  Typically, the handling function should store this
 * information in shared memory for later use by the custodian.  Note that the
 * task's handling function for arg is invoked before enqueueing the task, and
 * it will still be invoked regardless of whether the task is already enqueued.
 */
void
RequestCustodian(CustodianTask requested, bool immediate, Datum arg)
{
	CustodianTaskHandleArg arg_func = (LookupCustodianFunctions(requested))->handle_arg_func;

	/* First process any extra information provided in the request. */
	if (arg_func)
		(*arg_func) (arg);

	CustodianEnqueueTask(requested);

	if (immediate)
		DoCustodianTasks(false);
	else if (ProcGlobal->custodianLatch)
		SetLatch(ProcGlobal->custodianLatch);
}

/*
 * CustodianEnqueueTask
 *		Add a task to the custodian's queue
 *
 * If the task is already in the queue, this function has no effect.
 */
static void
CustodianEnqueueTask(CustodianTask task)
{
	Assert(task >= 0 && task < NUM_CUSTODIAN_TASKS);

	SpinLockAcquire(&CustodianShmem->cust_lck);

	for (int i = 0; i < NUM_CUSTODIAN_TASKS; i++)
	{
		int idx = (CustodianShmem->task_queue_head + i) % NUM_CUSTODIAN_TASKS;
		CustodianTask *elem = &CustodianShmem->task_queue_elems[idx];

		/*
		 * If the task is already queued in this slot or the slot is empty,
		 * enqueue the task here and return.
		 */
		if (*elem == INVALID_CUSTODIAN_TASK || *elem == task)
		{
			*elem = task;
			SpinLockRelease(&CustodianShmem->cust_lck);
			return;
		}
	}

	/* We should never run out of space in the queue. */
	elog(ERROR, "could not enqueue custodian task %d", task);
	pg_unreachable();
}

/*
 * CustodianGetNextTask
 *		Retrieve the next task that the custodian should execute
 *
 * The returned task is dequeued from the custodian's queue.  If no tasks are
 * queued, INVALID_CUSTODIAN_TASK is returned.
 */
static CustodianTask
CustodianGetNextTask(void)
{
	CustodianTask next_task;
	CustodianTask *elem;

	SpinLockAcquire(&CustodianShmem->cust_lck);

	elem = &CustodianShmem->task_queue_elems[CustodianShmem->task_queue_head];

	next_task = *elem;
	*elem = INVALID_CUSTODIAN_TASK;

	CustodianShmem->task_queue_head++;
	CustodianShmem->task_queue_head %= NUM_CUSTODIAN_TASKS;

	SpinLockRelease(&CustodianShmem->cust_lck);

	return next_task;
}

/*
 * LookupCustodianFunctions
 *		Given a custodian task, look up its function pointers.
 */
static const struct cust_task_funcs_entry *
LookupCustodianFunctions(CustodianTask task)
{
	const struct cust_task_funcs_entry *entry;

	Assert(task >= 0 && task < NUM_CUSTODIAN_TASKS);

	for (entry = cust_task_functions;
		 entry && entry->task != INVALID_CUSTODIAN_TASK;
		 entry++)
	{
		if (entry->task == task)
			return entry;
	}

	/* All tasks must have an entry. */
	elog(ERROR, "could not lookup functions for custodian task %d", task);
	pg_unreachable();
}

/*
 * Stores the provided cutoff LSN in the custodian's shared memory.
 *
 * It's okay if the cutoff LSN is updated before a previously set cutoff has
 * been used for cleaning up files.  If that happens, it just means that the
 * next invocation of RemoveOldLogicalRewriteMappings() will use a more accurate
 * cutoff.
 */
static void
CustodianSetLogicalRewriteCutoff(Datum arg)
{
	SpinLockAcquire(&CustodianShmem->cust_lck);
	CustodianShmem->logical_rewrite_mappings_cutoff = DatumGetLSN(arg);
	SpinLockRelease(&CustodianShmem->cust_lck);

	/* if pass-by-ref, free Datum memory */
#ifndef USE_FLOAT8_BYVAL
	pfree(DatumGetPointer(arg));
#endif
}

/*
 * Used by the custodian to determine which logical rewrite mapping files it can
 * remove.
 */
XLogRecPtr
CustodianGetLogicalRewriteCutoff(void)
{
	XLogRecPtr  cutoff;

	SpinLockAcquire(&CustodianShmem->cust_lck);
	cutoff = CustodianShmem->logical_rewrite_mappings_cutoff;
	SpinLockRelease(&CustodianShmem->cust_lck);

	return cutoff;
}
