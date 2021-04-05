/*-------------------------------------------------------------------------
 *
 * printtup.h
 *
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/printtup.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PRINTTUP_H
#define PRINTTUP_H

#include "utils/guc.h"
#include "utils/portal.h"

extern DestReceiver *printtup_create_DR(CommandDest dest);

extern void SetRemoteDestReceiverParams(DestReceiver *self, Portal portal);

extern void SendRowDescriptionMessage(StringInfo buf,
									  TupleDesc typeinfo, List *targetlist, int16 *formats);

extern void debugStartup(DestReceiver *self, int operation,
						 TupleDesc typeinfo);
extern bool debugtup(TupleTableSlot *slot, DestReceiver *self);

extern char *result_format_auto_binary_types;
extern bool check_result_format_auto_binary_types(char **newval, void **extra, GucSource source);
extern void assign_result_format_auto_binary_types(const char *newval, void *extra);

/* XXX these are really in executor/spi.c */
extern void spi_dest_startup(DestReceiver *self, int operation,
							 TupleDesc typeinfo);
extern bool spi_printtup(TupleTableSlot *slot, DestReceiver *self);

#endif							/* PRINTTUP_H */
