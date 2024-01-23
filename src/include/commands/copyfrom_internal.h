/*-------------------------------------------------------------------------
 *
 * copyfrom_internal.h
 *	  Internal definitions for COPY FROM command.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/copyfrom_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPYFROM_INTERNAL_H
#define COPYFROM_INTERNAL_H

#include "commands/copy.h"
#include "commands/copyapi.h"
#include "commands/trigger.h"
#include "nodes/miscnodes.h"

extern void ReceiveCopyBegin(CopyFromState cstate);
extern void ReceiveCopyBinaryHeader(CopyFromState cstate);

/* Callbacks for CopyFromRoutine->CopyFromOneRow */
extern bool CopyFromTextOneRow(CopyFromState cstate, ExprContext *econtext,
							   Datum *values, bool *nulls);
extern bool CopyFromCSVOneRow(CopyFromState cstate, ExprContext *econtext,
							  Datum *values, bool *nulls);
extern bool CopyFromBinaryOneRow(CopyFromState cstate, ExprContext *econtext,
								 Datum *values, bool *nulls);

#endif							/* COPYFROM_INTERNAL_H */
