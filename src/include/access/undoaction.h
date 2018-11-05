/*-------------------------------------------------------------------------
 *
 * undoaction.h
 *	  undo action prototypes
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/undoaction.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNDOACTION_H
#define UNDOACTION_H

#include "postgres.h"

#include "access/undolog.h"
#include "access/undorecord.h"

/* undo record information */
typedef struct UndoRecInfo
{
	UndoRecPtr	urp;	/* undo recptr (undo record location). */
	UnpackedUndoRecord	*uur;	/* actual undo record. */
} UndoRecInfo;

#endif
