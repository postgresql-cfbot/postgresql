/*-------------------------------------------------------------------------
 *
 * svariableReceiver.h
 *	  prototypes for svariableReceiver.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/svariableReceiver.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SVARIABLE_RECEIVER_H
#define SVARIABLE_RECEIVER_H

#include "tcop/dest.h"


extern DestReceiver *CreateVariableDestReceiver(void);

extern void SetVariableDestReceiverParams(DestReceiver *self, Oid varid);

#endif							/* SVARIABLE_RECEIVER_H */
