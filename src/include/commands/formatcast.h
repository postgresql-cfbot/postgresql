/*-------------------------------------------------------------------------
 *
 * formatcast.h
 *	  prototypes for formatcastcmds.c.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/formatcast.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FORMATCAST_H
#define FORMATCAST_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"

extern ObjectAddress CreateFormatCast(CreateFormatCastStmt *stmt);
extern Oid	get_format_cast_oid(Oid sourcetypeid, Oid targettypeid,
							  bool missing_ok);

#endif							/* FORMATCAST_H */
