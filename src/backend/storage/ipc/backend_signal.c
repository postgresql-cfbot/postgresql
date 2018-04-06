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
#include "utils/builtins.h"

/*
 * Structure for registering a message to be sent to a cancelled, or terminated
 * backend. Each backend is registered per pid in the array which is indexed by
 * Backend ID. Reading and writing the message is protected by a per-slot
 * spinlock.
 */
typedef struct
{
	pid_t	pid;
	slock_t	mutex;
	char	message[MAX_CANCEL_MSG];
	int		len;
} BackendCancelShmemStruct;

static BackendCancelShmemStruct	*BackendCancelSlots = NULL;
static volatile BackendCancelShmemStruct *MyCancelSlot = NULL;
static void CleanupCancelBackend(int status, Datum argument);

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
		SetBackendCancelMessage(pid, msg);

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
static bool
pg_cancel_backend_internal(pid_t pid, char *msg)
{
	int			r = pg_signal_backend(pid, SIGINT, msg);

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

Datum
pg_cancel_backend(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(pg_cancel_backend_internal(PG_GETARG_INT32(0), NULL));
}

Datum
pg_cancel_backend_msg(PG_FUNCTION_ARGS)
{
	pid_t		pid;
	char 	   *msg = NULL;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	pid = PG_GETARG_INT32(0);

	if (PG_NARGS() == 2 && !PG_ARGISNULL(1))
		msg = text_to_cstring(PG_GETARG_TEXT_PP(1));

	PG_RETURN_BOOL(pg_cancel_backend_internal(pid, msg));
}

/*
 * Signal to terminate a backend process.  This is allowed if you are a member
 * of the role whose process is being terminated.
 *
 * Note that only superusers can signal superuser-owned processes.
 */
static bool
pg_terminate_backend_internal(pid_t pid, char *msg)
{
	int		r = pg_signal_backend(pid, SIGTERM, msg);

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

Datum
pg_terminate_backend(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(pg_terminate_backend_internal(PG_GETARG_INT32(0), NULL));
}

Datum
pg_terminate_backend_msg(PG_FUNCTION_ARGS)
{
	pid_t		pid;
	char 	   *msg = NULL;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	pid = PG_GETARG_INT32(0);

	if (PG_NARGS() == 2 && !PG_ARGISNULL(1))
		msg = text_to_cstring(PG_GETARG_TEXT_PP(1));

	PG_RETURN_BOOL(pg_terminate_backend_internal(pid, msg));
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
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
Datum
pg_rotate_logfile(PG_FUNCTION_ARGS)
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
 * cancelling, or terminating a backend. The message will be stored in
 * shared memory and is limited to MAX_CANCEL_MSG characters including
 * the NULL terminator.
 *
 * Access to the message slots is protected by spinlocks.
 */

/*
 * Return the required size for the cancelation message Shmem area.
 */
Size
CancelBackendMsgShmemSize(void)
{
	return MaxBackends * sizeof(BackendCancelShmemStruct);
}

/*
 * Create and initialize the Shmem structure for holding the messages, the
 * bookkeeping for them and the spinlocks associated.
 */
void
BackendCancelMessageShmemInit(void)
{
	Size	size = CancelBackendMsgShmemSize();
	bool	found;
	int		i;

	BackendCancelSlots = (BackendCancelShmemStruct *)
		ShmemInitStruct("BackendCancelSlots", size, &found);

	if (!found)
	{
		MemSet(BackendCancelSlots, 0, size);

		for (i = 0; i < MaxBackends; i++)
			SpinLockInit(&(BackendCancelSlots[i].mutex));
	}
}

/*
 * Set up the slot for the current backend_id
 */
void
BackendCancelMessageInit(int backend_id)
{
	volatile BackendCancelShmemStruct *slot;

	slot = &BackendCancelSlots[backend_id - 1];

	slot->message[0] = '\0';
	slot->len = 0;
	slot->pid = MyProcPid;

	MyCancelSlot = slot;

	on_shmem_exit(CleanupCancelBackend, Int32GetDatum(backend_id));
}

/*
 * Ensure that the slot is purged and emptied at exit. Any message gets
 * overwritten with null chars to avoid risking exposing a message intended for
 * another backend to a new backend.
 */
static void
CleanupCancelBackend(int status, Datum argument)
{
	int backend_id = DatumGetInt32(argument);
	volatile BackendCancelShmemStruct *slot;

	slot = &BackendCancelSlots[backend_id - 1];

	Assert(slot == MyCancelSlot);

	MyCancelSlot = NULL;

	if (slot->len > 0)
		MemSet(slot->message, '\0', sizeof(slot->message));

	slot->len = 0;
	slot->pid = 0;
}

/*
 * Sets a cancellation message for the backend with the specified pid, and
 * returns the length of the message actually created. If the returned length
 * is less than the length of the message parameter, truncation has occurred.
 * If the backend isn't found, -1 is returned. If no message is passed, zero is
 * returned. If two backends collide in setting a message, the existing message
 * will be overwritten by the last one in.
 */
int
SetBackendCancelMessage(pid_t backend_pid, char *message)
{
	int		i;
	int		len;

	if (!message)
		return 0;

	for (i = 0; i < MaxBackends; i++)
	{
		BackendCancelShmemStruct *slot = &BackendCancelSlots[i];

		if (slot->pid != 0 && slot->pid == backend_pid)
		{
			SpinLockAcquire(&slot->mutex);
			if (slot->pid != backend_pid)
			{
				SpinLockRelease(&slot->mutex);
				goto error;
			}

			len = pg_mbcliplen(message, strlen(message),
							   sizeof(slot->message) - 1);
			MemSet(slot->message, '\0', sizeof(slot->message));
			memcpy(slot->message, message, len);
			slot->len = len;
			SpinLockRelease(&slot->mutex);

			if (len != strlen(message))
				ereport(NOTICE,
						(errmsg("message is too long and has been truncated")));
			return len;
		}
	}

error:

	elog(LOG, "Cancellation message requested for missing backend %d by %d",
		 (int) backend_pid, MyProcPid);

	return -1;
}

/*
 * Test whether there is a cancelation message for the current backend that
 * can be consumed and presented to the user.
 */
bool
HasCancelMessage(void)
{
	volatile BackendCancelShmemStruct *slot = MyCancelSlot;
	bool 	has_message = false;

	if (slot != NULL)
	{
		SpinLockAcquire(&slot->mutex);
		has_message = (slot->len > 0);
		SpinLockRelease(&slot->mutex);
	}

	return has_message;
}

/*
 * Return the configured cancellation message and its length. If the returned
 * length is greater than the size of the passed buffer, truncation has been
 * performed. The message is cleared on reading.
 */
int
ConsumeCancelMessage(char *buffer, size_t buf_len)
{
	volatile BackendCancelShmemStruct *slot = MyCancelSlot;
	int		msg_length = 0;

	if (slot != NULL && slot->len > 0)
	{
		SpinLockAcquire(&slot->mutex);
		strlcpy(buffer, (const char *) slot->message, buf_len);
		msg_length = slot->len;
		slot->len = 0;
		slot->message[0] = '\0';
		SpinLockRelease(&slot->mutex);
	}

	return msg_length;
}
