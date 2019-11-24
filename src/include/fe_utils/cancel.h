/*-------------------------------------------------------------------------
 *
 * Query Cancelation
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/fe_utils/cancel.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CANCEL_H
#define CANCEL_H

#include "libpq-fe.h"

extern void SetCancelConn(PGconn *conn);
extern void ResetCancelConn(void);
extern bool CancelRequested;

#ifndef WIN32
extern void setup_cancel_handler(void (*callback)(void));
#else
/*
 * Ensure that the signature is the same under windows, at the price of
 * an ignored function parameter.
 */
extern void setup_cancel_handler(void *ignored);
#endif /* WIN32 */

#endif /* CANCEL_H */
