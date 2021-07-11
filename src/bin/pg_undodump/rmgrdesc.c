/*
 * rmgrdesc.c
 *
 * pg_undodump resource managers definition
 *
 * src/bin/pg_undodump/rmgrdesc.c
 */
#define FRONTEND 1
#include "postgres.h"

#include "access/undoxacttest.h"
#include "catalog/storage_undo.h"
#include "commands/dbcommands_undo.h"
#include "rmgrdesc.h"

#define PG_RMGR(symname,name,redo,desc,identify,startup,cleanup,mask,undo,undo_desc) \
	{ name, undo_desc},

const RmgrDescData RmgrDescTable[RM_MAX_ID + 1] = {
#include "access/rmgrlist.h"
};
