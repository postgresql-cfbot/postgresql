/*-------------------------------------------------------------------------
 *
 * backend_signal.c
 *	  Routines for signalling backends
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/backend_signal.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>

#include "catalog/pg_authid.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "postmaster/syslogger.h"
#include "storage/backend_signal.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/acl.h"
#include "utils/builtins.h"

/*
 * Structure for registering a feedback payload to be sent to a cancelled, or
 * terminated backend. Each backend is registered per pid in the array which is
 * indexed by Backend ID. Reading and writing the message is protected by a
 * per-slot spinlock.
 */
typedef struct
{
	pid_t	dest_pid;		/* The pid of the process being signalled */
	pid_t	src_pid;		/* The pid of the processing signalling */
	slock_t	mutex;			/* Per-slot protection */
	char	message[MAX_CANCEL_MSG]; /* Message to send to signalled backend */
	int		orig_length;	/* Length of the message as passed by the user,
							 * if this length exceeds MAX_CANCEL_MSG it will
							 * be truncated but we store the original length
							 * in order to be able to convey truncation */
	int		sqlerrcode;		/* errcode to use when signalling backend */
} BackendSignalFeedbackShmemStruct;

static BackendSignalFeedbackShmemStruct	*BackendSignalFeedbackSlots = NULL;
static volatile BackendSignalFeedbackShmemStruct *MyCancelSlot = NULL;
static void CleanupBackendSignalFeedback(int status, Datum argument);
static int backend_feedback(pid_t backend_pid, char *message, int sqlerrcode);

/*
 * Send a signal to another backend.
 *
 * The signal is delivered if the user is either a superuser or the same role
 * as the backend being signaled. For "dangerous" signals, an explicit check
 * for superuser needs to be done prior to calling this function. If msg is
 * set, the contents will be passed as a message to the backend in the error
 * message.
 *
 * Returns 0 on success, 1 on general failure, 2 on normal permission error
 * and 3 if the caller needs to be a superuser.
 *
 * In the event of a general failure (return code 1), a warning message will
 * be emitted. For permission errors, doing that is the responsibility of
 * the caller.
 */
#define SIGNAL_BACKEND_SUCCESS 0
#define SIGNAL_BACKEND_ERROR 1
#define SIGNAL_BACKEND_NOPERMISSION 2
#define SIGNAL_BACKEND_NOSUPERUSER 3
static int
pg_signal_backend(int pid, int sig, char *msg)
{
	PGPROC	   *proc = BackendPidGetProc(pid);

	/*
	 * BackendPidGetProc returns NULL if the pid isn't valid; but by the time
	 * we reach kill(), a process for which we get a valid proc here might
	 * have terminated on its own.  There's no way to acquire a lock on an
	 * arbitrary process to prevent that. But since so far all the callers of
	 * this mechanism involve some request for ending the process anyway, that
	 * it might end on its own first is not a problem.
	 */
	if (proc == NULL)
	{
		/*
		 * This is just a warning so a loop-through-resultset will not abort
		 * if one backend terminated on its own during the run.
		 */
		ereport(WARNING,
				(errmsg("PID %d is not a PostgreSQL server process", pid)));
		return SIGNAL_BACKEND_ERROR;
	}

	/* Only allow superusers to signal superuser-owned backends. */
	if (superuser_arg(proc->roleId) && !superuser())
		return SIGNAL_BACKEND_NOSUPERUSER;

	/* Users can signal backends they have role membership in. */
	if (!has_privs_of_role(GetUserId(), proc->roleId) &&
		!has_privs_of_role(GetUserId(), DEFAULT_ROLE_SIGNAL_BACKENDID))
		return SIGNAL_BACKEND_NOPERMISSION;

	/* If the user supplied a message to the signalled backend */
	if (msg != NULL)
	{
		char *tmp = msg;

		/*
		 * The message to pass to the signalled backend is currently restricted
		 * to ASCII only, since the sending backend might use an encoding which
		 * is incompatible with the receiving with regards to conversion.
		 */
		while (*tmp != '\0')
		{
			if (!isascii(*tmp))
				ereport(ERROR,
						(errmsg("message is restricted to ASCII only")));
			tmp++;
		}

		if (sig == SIGINT)
			SetBackendCancelMessage(pid, msg);
		else
			SetBackendTerminationMessage(pid, msg);
	}

	/*
	 * Can the process we just validated above end, followed by the pid being
	 * recycled for a new process, before reaching here?  Then we'd be trying
	 * to kill the wrong thing.  Seems near impossible when sequential pid
	 * assignment and wraparound is used.  Perhaps it could happen on a system
	 * where pid re-use is randomized.  That race condition possibility seems
	 * too unlikely to worry about.
	 */

	/* If we have setsid(), signal the backend's whole process group */
#ifdef HAVE_SETSID
	if (kill(-pid, sig))
#else
	if (kill(pid, sig))
#endif
	{
		/* Again, just a warning to allow loops */
		ereport(WARNING,
				(errmsg("could not send signal to process %d: %m", pid)));
		return SIGNAL_BACKEND_ERROR;
	}
	return SIGNAL_BACKEND_SUCCESS;
}

/*
 * Signal to cancel a backend process.  This is allowed if you are a member of
 * the role whose process is being canceled.
 *
 * Note that only superusers can signal superuser-owned processes.
 */
Datum
pg_cancel_backend(PG_FUNCTION_ARGS)
{
	int			r;
	pid_t		pid;
	char 	   *msg = NULL;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	pid = PG_GETARG_INT32(0);

	if (PG_NARGS() == 2 && !PG_ARGISNULL(1))
		msg = text_to_cstring(PG_GETARG_TEXT_PP(1));

	r = pg_signal_backend(pid, SIGINT, msg);

	if (r == SIGNAL_BACKEND_NOSUPERUSER)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be a superuser to cancel superuser query"))));

	if (r == SIGNAL_BACKEND_NOPERMISSION)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be a member of the role whose query is being canceled or member of pg_signal_backend"))));

	PG_RETURN_BOOL(r == SIGNAL_BACKEND_SUCCESS);
}

/*
 * Signal to terminate a backend process.  This is allowed if you are a member
 * of the role whose process is being terminated.
 *
 * Note that only superusers can signal superuser-owned processes.
 */
Datum
pg_terminate_backend(PG_FUNCTION_ARGS)
{
	int			r;
	pid_t		pid;
	char 	   *msg = NULL;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	pid = PG_GETARG_INT32(0);

	if (PG_NARGS() == 2 && !PG_ARGISNULL(1))
		msg = text_to_cstring(PG_GETARG_TEXT_PP(1));

	r = pg_signal_backend(pid, SIGTERM, msg);

	if (r == SIGNAL_BACKEND_NOSUPERUSER)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be a superuser to terminate superuser process"))));

	if (r == SIGNAL_BACKEND_NOPERMISSION)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be a member of the role whose process is being terminated or member of pg_signal_backend"))));

	return (r == SIGNAL_BACKEND_SUCCESS);
}

/*
 * Signal to reload the database configuration
 *
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
Datum
pg_reload_conf(PG_FUNCTION_ARGS)
{
	if (kill(PostmasterPid, SIGHUP))
	{
		ereport(WARNING,
				(errmsg("failed to send signal to postmaster: %m")));
		PG_RETURN_BOOL(false);
	}

	PG_RETURN_BOOL(true);
}


/*
 * Rotate log file
 *
 * This function is kept to support adminpack 1.0.
 */
Datum
pg_rotate_logfile(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to rotate log files with adminpack 1.0"),
				  errhint("Consider using pg_logfile_rotate(), which is part of core, instead."))));

	if (!Logging_collector)
	{
		ereport(WARNING,
				(errmsg("rotation not possible because log collection not active")));
		PG_RETURN_BOOL(false);
	}

	SendPostmasterSignal(PMSIGNAL_ROTATE_LOGFILE);
	PG_RETURN_BOOL(true);
}

/*
 * Rotate log file
 *
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
Datum
pg_rotate_logfile_v2(PG_FUNCTION_ARGS)
{
	if (!Logging_collector)
	{
		ereport(WARNING,
				(errmsg("rotation not possible because log collection not active")));
		PG_RETURN_BOOL(false);
	}

	SendPostmasterSignal(PMSIGNAL_ROTATE_LOGFILE);
	PG_RETURN_BOOL(true);
}

/*
 * The following routines handle registering an optional message when
 * cancelling, or terminating, a backend as well changing the sqlerrcode used.
 * The combined payload of message/errcode is referred to as feedback.  The
 * message will be stored in shared memory and is limited to MAX_CANCEL_MSG
 * characters including the NULL terminator.
 *
 * Access to the feedback slots is protected by spinlocks.
 */

/*
 * Return the required size for the cancelation feedback Shmem area.
 */
Size
BackendSignalFeedbackShmemSize(void)
{
	return MaxBackends * sizeof(BackendSignalFeedbackShmemStruct);
}

/*
 * Create and initialize the Shmem structure for holding the feedback, the
 * bookkeeping for them and the spinlocks associated.
 */
void
BackendSignalFeedbackShmemInit(void)
{
	Size	size = BackendSignalFeedbackShmemSize();
	bool	found;
	int		i;

	BackendSignalFeedbackSlots = (BackendSignalFeedbackShmemStruct *)
		ShmemInitStruct("BackendSignalFeedbackSlots", size, &found);

	if (!found)
	{
		MemSet(BackendSignalFeedbackSlots, 0, size);

		for (i = 0; i < MaxBackends; i++)
			SpinLockInit(&(BackendSignalFeedbackSlots[i].mutex));
	}
}

/*
 * Set up the slot for the current backend_id
 */
void
BackendSignalFeedbackInit(int backend_id)
{
	volatile BackendSignalFeedbackShmemStruct *slot;

	slot = &BackendSignalFeedbackSlots[backend_id - 1];

	slot->message[0] = '\0';
	slot->orig_length = 0;
	slot->sqlerrcode = 0;
	slot->dest_pid = MyProcPid;

	MyCancelSlot = slot;

	on_shmem_exit(CleanupBackendSignalFeedback, Int32GetDatum(backend_id));
}

/*
 * Ensure that the slot is purged and emptied at exit. Any message gets
 * overwritten with null chars to avoid risking exposing a message intended for
 * another backend to a new backend.
 */
static void
CleanupBackendSignalFeedback(int status, Datum argument)
{
	int backend_id = DatumGetInt32(argument);
	volatile BackendSignalFeedbackShmemStruct *slot;

	slot = &BackendSignalFeedbackSlots[backend_id - 1];

	Assert(slot == MyCancelSlot);

	MyCancelSlot = NULL;

	if (slot->orig_length > 0)
		MemSet(slot->message, '\0', sizeof(slot->message));

	slot->orig_length = 0;
	slot->sqlerrcode = 0;
	slot->dest_pid = 0;
	slot->src_pid = 0;
}

/*
 * Set a message for the cancellation of the backend with the specified pid,
 * using the default sqlerrcode.
 */
int
SetBackendCancelMessage(pid_t backend_pid, char *message)
{
	return backend_feedback(backend_pid, message, ERRCODE_QUERY_CANCELED);
}

/*
 * Set a message for the termination of the backend with the specified pid,
 * using the default sqlerrcode.
 */
int
SetBackendTerminationMessage(pid_t backend_pid, char *message)
{
	return backend_feedback(backend_pid, message, ERRCODE_ADMIN_SHUTDOWN);
}

/*
 * Set both a message and a sqlerrcode for use when signalling the backend
 * with the specified pid.
 */
int
SetBackendSignalFeedback(pid_t backend_pid, char *message, int sqlerrcode)
{
	return backend_feedback(backend_pid, message, sqlerrcode);
}

/*
 * Sets a cancellation message for the backend with the specified pid, and
 * returns zero on success. If the backend isn't found, or no message is
 * passed, 1 is returned.  If two backends collide in setting a message, the
 * existing message will be overwritten by the last one in. The message will
 * be truncated to fit within MAX_CANCEL_MSG bytes.
 */
static int
backend_feedback(pid_t backend_pid, char *message, int sqlerrcode)
{
	int		i;
	int		len;

	if (!message)
		return 1;

	len = pg_mbcliplen(message, strlen(message), MAX_CANCEL_MSG - 1);

	for (i = 0; i < MaxBackends; i++)
	{
		BackendSignalFeedbackShmemStruct *slot = &BackendSignalFeedbackSlots[i];

		if (slot->dest_pid != 0 && slot->dest_pid == backend_pid)
		{
			SpinLockAcquire(&slot->mutex);
			if (slot->dest_pid != backend_pid)
			{
				SpinLockRelease(&slot->mutex);
				return 1;
			}

			/* Avoid risking to leak any part of a previously set message */
			MemSet(slot->message, '\0', sizeof(slot->message));

			memcpy(slot->message, message, len);
			slot->orig_length = pg_mbstrlen(message);
			slot->sqlerrcode = sqlerrcode;
			slot->src_pid = MyProcPid;
			SpinLockRelease(&slot->mutex);

			if (len != strlen(message))
				ereport(NOTICE,
						(errmsg("message is too long and has been truncated")));
			return 0;
		}
	}

	return 1;
}

/*
 * HasBackendSignalFeedback
 *		Test if there is a backend signalling feedback to consume
 *
 * Test whether there is feedback registered for the current backend that can
 * be consumed and presented to the user. It isn't strictly required to call
 * this function prior to consuming a potential message, but since consuming it
 * will clear it there can be cases where one would like to peek first.
 */
bool
HasBackendSignalFeedback(void)
{
	volatile BackendSignalFeedbackShmemStruct *slot = MyCancelSlot;
	bool 	has_message = false;

	if (slot != NULL)
	{
		SpinLockAcquire(&slot->mutex);
		has_message = ((slot->orig_length > 0) && (slot->sqlerrcode != 0));
		SpinLockRelease(&slot->mutex);
	}

	return has_message;
}

/*
 * ConsumeBackendSignalFeedback
 *		Read anc clear backend signalling feedback
 *
 * Return the configured signal feedback in buffer, which is buf_len bytes in
 * size.  The original length of the message is returned, or zero in case no
 * message was found. If the returned length exceeds that of Min(buf_len,
 * MAX_CANCEL_MSG), then truncation has been performed. The feedback (message
 * and errcode) is cleared on consumption. There is no point in passing a
 * buffer larger than MAX_CANCEL_NSG as that is the upper bound on what will be
 * stored in the slot.
 */
int
ConsumeBackendSignalFeedback(char *buffer, size_t buf_len, int *sqlerrcode,
							 pid_t *pid)
{
	volatile BackendSignalFeedbackShmemStruct *slot = MyCancelSlot;
	int		msg_length = 0;

	if (slot != NULL && slot->orig_length > 0)
	{
		SpinLockAcquire(&slot->mutex);
		strlcpy(buffer, (const char *) slot->message, buf_len);
		msg_length = slot->orig_length;
		*sqlerrcode = slot->sqlerrcode;
		*pid = slot->src_pid;
		slot->orig_length = 0;
		slot->message[0] = '\0';
		slot->sqlerrcode = 0;
		SpinLockRelease(&slot->mutex);
	}

	return msg_length;
}
