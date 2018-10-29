/*-------------------------------------------------------------------------
 *
 * signal_message.c
 *	  Functions for sending a message to a signalled backend
 *
 * This file contains routines to handle registering an optional message when
 * cancelling, or terminating, a backend as well changing the sqlerrcode used.
 * The combined payload of message/errcode is referred to as feedback.  The
 * message will be stored in shared memory and is limited to MAX_CANCEL_MSG
 * characters including the NULL terminator.
 *
 * Access to the feedback slots is protected by spinlocks.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/signal_message.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "storage/signal_message.h"
#include "storage/spin.h"


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
 *		Read and clear backend signalling feedback
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
		/* Avoid risking to leak any part of a previously set message */
		MemSet(slot->message, '\0', sizeof(slot->message));
		slot->sqlerrcode = 0;
		SpinLockRelease(&slot->mutex);
	}

	return msg_length;
}
