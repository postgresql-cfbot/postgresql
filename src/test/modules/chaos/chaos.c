#include "postgres.h"

#include "access/transam.h"
#include "fmgr.h"
#include "storage/lwlock.h"

#include <limits.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(clobber_next_oid);

Datum
clobber_next_oid(PG_FUNCTION_ARGS)
{
	int64		oid = PG_GETARG_INT64(0);

	if (oid < FirstNormalObjectId || oid > UINT_MAX)
		elog(ERROR, "invalid oid");

	LWLockAcquire(OidGenLock, LW_EXCLUSIVE);
	ShmemVariableCache->nextOid = oid;
	LWLockRelease(OidGenLock);

	PG_RETURN_VOID();
}
