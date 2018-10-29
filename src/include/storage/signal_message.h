/*-------------------------------------------------------------------------
 *
 * signal_message.h
 *		Declarations for sending a message to a signalled backend
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 *	  src/include/storage/signal_message.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SIGNAL_MESSAGE_H
#define SIGNAL_MESSAGE_H

#define MAX_CANCEL_MSG 128

extern Size BackendSignalFeedbackShmemSize(void);
extern void BackendSignalFeedbackShmemInit(void);
extern void BackendSignalFeedbackInit(int backend_id);

extern int SetBackendCancelMessage(pid_t backend, char *message);
extern int SetBackendTerminationMessage(pid_t backend, char *message);
extern int SetBackendSignalFeedback(pid_t backend, char *message, int sqlerrcode);
extern bool HasBackendSignalFeedback(void);
extern int ConsumeBackendSignalFeedback(char *msg, size_t len, int *sqlerrcode, pid_t *pid);

#endif /* SIGNAL_MESSAGE_H */
