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
#include "utils/relmapper.h"

/* must be kept in sync with RmgrData definition in xlog_internal.h */
#define PG_RMGR(symname,name,redo,desc,identify,startup,cleanup,mask,decode) \
	{ name, redo, desc, identify, startup, cleanup, mask, decode },

RmgrData RmgrTable[RM_MAX_ID + 1] = {
#include "access/rmgrlist.h"
};

/*
 * Start up all resource managers.
 */
void
RmgrStartup(void)
{
	for (int rmid = 0; rmid <= RM_MAX_ID; rmid++)
	{
		if (!RmgrExists(rmid))
			continue;

		if (RmgrTable[rmid].rm_startup != NULL)
			RmgrTable[rmid].rm_startup();
	}
}

/*
 * Clean up all resource managers.
 */
void
RmgrCleanup(void)
{
	for (int rmid = 0; rmid <= RM_MAX_ID; rmid++)
	{
		if (!RmgrExists(rmid))
			continue;

		if (RmgrTable[rmid].rm_cleanup != NULL)
			RmgrTable[rmid].rm_cleanup();
	}
}

/*
 * Emit PANIC message when we encounter a record with an RmgrId we don't recognize.
 */
void
RmgrNotFound(RmgrId rmid)
{
	ereport(PANIC, (errmsg("resource manager with ID %d not registered", rmid),
					errhint("Include the extension module that implements this resource manager in shared_preload_libraries.")));
}

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
	if (rmgr->rm_name == NULL)
		ereport(PANIC, errmsg("custom resource manager is invalid"));

	if (rmid < RM_MIN_CUSTOM_ID)
		ereport(PANIC, errmsg("custom resource manager ID %d is out of range", rmid));

	if (!process_shared_preload_libraries_in_progress)
		ereport(PANIC,
				(errmsg("failed to register custom resource manager \"%s\" with ID %d", rmgr->rm_name, rmid),
				 errdetail("Custom resource manager must be registered while initializing modules in shared_preload_libraries.")));

	if (RmgrTable[rmid].rm_name != NULL)
		ereport(PANIC,
				(errmsg("failed to register custom resource manager \"%s\" with ID %d", rmgr->rm_name, rmid),
				 errdetail("Custom resource manager \"%s\" already registered with the same ID.",
						   RmgrTable[rmid].rm_name)));

	/* check for existing rmgr with the same name */
	for (int i = 0; i <= RM_MAX_ID; i++)
	{
		if (!RmgrExists(i))
			continue;

		if (!strcmp(RmgrTable[i].rm_name, rmgr->rm_name))
			ereport(PANIC,
				(errmsg("failed to register custom resource manager \"%s\" with ID %d", rmgr->rm_name, rmid),
				 errdetail("Existing resource manager with ID %d has the same name.", i)));
	}

	/* register it */
	RmgrTable[rmid] = *rmgr;
	ereport(LOG,
			(errmsg("registered custom resource manager \"%s\" with ID %d",
					rmgr->rm_name, rmid)));
}
