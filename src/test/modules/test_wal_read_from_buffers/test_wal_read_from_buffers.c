/*--------------------------------------------------------------------------
 *
 * test_wal_read_from_buffers.c
 *		Test module to read WAL from WAL buffers.
 *
 * Portions Copyright (c) 2023, PostgreSQL Global Development Group
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
	char		data[XLOG_BLCKSZ] = {0};
	Size		nread;

	nread = XLogReadFromBuffers(NULL, PG_GETARG_LSN(0),
								GetWALInsertionTimeLine(),
								XLOG_BLCKSZ, data);

	PG_RETURN_BOOL(nread > 0);
}
