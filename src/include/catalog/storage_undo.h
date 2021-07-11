/*-------------------------------------------------------------------------
 *
 * storage_undo.h
 *	  prototypes for UNDO support for backend/catalog/storage.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/storage_undo.h
 *
 *-------------------------------------------------------------------------
 */
/* TODO Consider if "catalog" is the proper directory for this file. */
#ifndef STORAGE_UNDO_H
#define STORAGE_UNDO_H

#include "access/xlog_internal.h"
#include "lib/stringinfo.h"
#include "storage/relfilenode.h"

#define UNDO_SMGR_CREATE 0

typedef struct xu_smgr_create
{
	RelFileNode rnode;
	char		relpersistence;
}			xu_smgr_create;

extern void smgr_undo(const WrittenUndoNode *record);
extern void smgr_undo_desc(StringInfo buf, const WrittenUndoNode *record);

#endif
