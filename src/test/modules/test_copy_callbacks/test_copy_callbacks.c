/*--------------------------------------------------------------------------
 *
 * test_copy_callbacks.c
 *		Code for testing COPY callbacks.
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_copy_callbacks/test_copy_callbacks.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/table.h"
#include "commands/copy.h"
#include "fmgr.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

static void
to_cb(void *data, int len)
{
	ereport(NOTICE,
			(errmsg("COPY TO callback called with data \"%s\" and length %d",
					(char *) data, len)));
}

PG_FUNCTION_INFO_V1(test_copy_to_callback);
Datum
test_copy_to_callback(PG_FUNCTION_ARGS)
{
	Relation	rel = table_open(PG_GETARG_OID(0), AccessShareLock);
	CopyToState cstate;

	cstate = BeginCopyTo(NULL, rel, NULL, RelationGetRelid(rel), NULL, NULL,
						 to_cb, NIL, NIL);
	(void) DoCopyTo(cstate);
	EndCopyTo(cstate);

	table_close(rel, AccessShareLock);

	PG_RETURN_VOID();
}
