/*-------------------------------------------------------------------------
 *
 * dbcommands_undo.h
 *		prototypes for UNDO support for backend/commands/dbcommands.c
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/dbcommands_undo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBCOMMANDS_UNDO_H
#define DBCOMMANDS_UNDO_H

#include "access/undodefs.h"
#include "lib/stringinfo.h"

#define	UNDO_DBASE_CREATE	0

typedef struct xu_dbase_create
{
	Oid			db_id;
	Oid			tablespace_id;
}			xu_dbase_create;

extern void dbase_undo(const WrittenUndoNode *record);
extern void dbase_undo_desc(StringInfo buf, const WrittenUndoNode *record);
#endif							/* DBCOMMANDS_UNDO_H */
