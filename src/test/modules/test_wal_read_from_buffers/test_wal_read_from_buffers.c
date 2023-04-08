/*--------------------------------------------------------------------------
 *
 * test_wal_read_from_buffers.c
 *		Test code for veryfing WAL read from WAL buffers.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	src/test/modules/test_wal_read_from_buffers/test_wal_read_from_buffers.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlog.h"
#include "fmgr.h"
#include "utils/pg_lsn.h"

PG_MODULE_MAGIC;

/*
 * SQL function for verifying that WAL data at a given LSN can be read from WAL
 * buffers. Returns true if read from WAL buffers, otherwise false.
 */
PG_FUNCTION_INFO_V1(test_wal_read_from_buffers);
Datum
test_wal_read_from_buffers(PG_FUNCTION_ARGS)
{
	XLogRecPtr	lsn;
	Size	nread;
	TimeLineID	tli;
	char	data[XLOG_BLCKSZ] = {0};

	lsn = PG_GETARG_LSN(0);

	if (XLogRecPtrIsInvalid(lsn))
		PG_RETURN_BOOL(false);

	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is in progress"),
				 errhint("WAL control functions cannot be executed during recovery.")));

	tli = GetWALInsertionTimeLine();

	nread = XLogReadFromBuffers(NULL, lsn, tli, XLOG_BLCKSZ, data);

	PG_RETURN_LSN(nread > 0);
}
