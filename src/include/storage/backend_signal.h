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

extern Size CancelBackendMsgShmemSize(void);
extern void BackendCancelMessageShmemInit(void);
extern void BackendCancelMessageInit(int backend_id);

extern int SetBackendCancelMessage(pid_t backend, char *message);
extern bool HasCancelMessage(void);
extern int ConsumeCancelMessage(char *msg, size_t len);

#endif /* BACKEND_SIGNAL_H */
