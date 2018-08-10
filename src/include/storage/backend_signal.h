/*-------------------------------------------------------------------------
 *
 * backend_signal.h
 *		Declarations for backend signalling
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 *	  src/include/storage/backend_signal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BACKEND_SIGNAL_H
#define BACKEND_SIGNAL_H

#define MAX_CANCEL_MSG 128

extern Size BackendSignalFeedbackShmemSize(void);
extern void BackendSignalFeedbackShmemInit(void);
extern void BackendSignalFeedbackInit(int backend_id);

extern int SetBackendCancelMessage(pid_t backend, char *message);
extern int SetBackendTerminationMessage(pid_t backend, char *message);
extern int SetBackendSignalFeedback(pid_t backend, char *message, int sqlerrcode);
extern bool HasBackendSignalFeedback(void);
extern int ConsumeBackendSignalFeedback(char *msg, size_t len, int *sqlerrcode, pid_t *pid);

#endif /* BACKEND_SIGNAL_H */
