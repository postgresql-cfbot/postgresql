/*
 * rmgr.c
 *
 * Resource managers definition
 *
 * src/backend/access/transam/rmgr.c
 */
#include "postgres.h"

#include "access/brin_xlog.h"
#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/generic_xlog.h"
#include "access/ginxlog.h"
#include "access/gistxlog.h"
#include "access/hash_xlog.h"
#include "access/heapam_xlog.h"
#include "access/multixact.h"
#include "access/nbtxlog.h"
#include "access/spgxlog.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "catalog/storage_xlog.h"
#include "commands/dbcommands_xlog.h"
#include "commands/sequence.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "replication/decode.h"
#include "replication/message.h"
#include "replication/origin.h"
#include "storage/standby.h"
#include "utils/memutils.h"
#include "utils/relmapper.h"

typedef struct CustomRmgrEntry {
	RmgrId rmid;
	RmgrData *rmgr;
} CustomRmgrEntry;

/* must be kept in sync with RmgrData definition in xlog_internal.h */
#define PG_RMGR(symname,name,redo,desc,identify,startup,cleanup,mask,decode) \
	{ name, redo, desc, identify, startup, cleanup, mask, decode },

const RmgrData RmgrTable[RM_MAX_ID + 1] = {
#include "access/rmgrlist.h"
};

static CustomRmgrEntry *CustomRmgrTable = NULL;
static int NumCustomRmgrs = 0;

/*
 * Register a new custom rmgr.
 *
 * Refer to https://wiki.postgresql.org/wiki/ExtensibleRmgr to reserve a
 * unique RmgrId for your extension, to avoid conflicts. During development,
 * use RM_EXPERIMENTAL_ID.
 */
void
RegisterCustomRmgr(RmgrId rmid, RmgrData *rmgr)
{
	if (rmid < RM_CUSTOM_MIN_ID || rmid > RM_CUSTOM_MAX_ID)
		ereport(PANIC, errmsg("custom rmgr id %d is out of range", rmid));

	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errmsg("custom rmgr must be registered while initializing modules in shared_preload_libraries")));

	ereport(LOG,
			(errmsg("registering custom rmgr \"%s\" with ID %d",
					rmgr->rm_name, rmid)));

	if (CustomRmgrTable == NULL)
		CustomRmgrTable = MemoryContextAllocZero(
			TopMemoryContext, sizeof(CustomRmgrEntry));

	/* check for existing builtin rmgr with the same name */
	for (int i = 0; i <= RM_MAX_ID; i++)
	{
		const RmgrData *existing_rmgr = &RmgrTable[i];

		if (!strcmp(existing_rmgr->rm_name, rmgr->rm_name))
			ereport(PANIC,
					(errmsg("custom rmgr \"%s\" has the same name as builtin rmgr",
							existing_rmgr->rm_name)));
	}

	/* check for conflicting custom rmgrs already registered */
	for (int i = 0; i < NumCustomRmgrs; i++)
	{
		CustomRmgrEntry entry = CustomRmgrTable[i];

		if (entry.rmid == rmid)
			ereport(PANIC,
					(errmsg("custom rmgr ID %d already registered with name \"%s\"",
							rmid, entry.rmgr->rm_name)));

		if (!strcmp(entry.rmgr->rm_name, rmgr->rm_name))
			ereport(PANIC,
					(errmsg("custom rmgr \"%s\" already registered with ID %d",
							rmgr->rm_name, entry.rmid)));
	}

	CustomRmgrTable = (CustomRmgrEntry *) repalloc(
		CustomRmgrTable, sizeof(CustomRmgrEntry) * NumCustomRmgrs + 1);

	CustomRmgrTable[NumCustomRmgrs].rmid = rmid;
	CustomRmgrTable[NumCustomRmgrs].rmgr = rmgr;
	NumCustomRmgrs++;
}

/*
 * GetCustomRmgr
 *
 * This is an O(N) list traversal because the expected size is very small.
 */
RmgrData
GetCustomRmgr(RmgrId rmid)
{
	if (rmid < RM_CUSTOM_MIN_ID || rmid > RM_CUSTOM_MAX_ID)
		ereport(PANIC, errmsg("custom rmgr id %d is out of range", rmid));

	for (int i = 0; i < NumCustomRmgrs; i++)
	{
		CustomRmgrEntry entry = CustomRmgrTable[i];
		if (entry.rmid == rmid)
			return *entry.rmgr;
	}

	ereport(PANIC, errmsg("custom rmgr with ID %d not found!", rmid));
}
