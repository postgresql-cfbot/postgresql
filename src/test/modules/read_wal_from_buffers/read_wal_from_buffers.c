/*--------------------------------------------------------------------------
 *
 * read_wal_from_buffers.c
 *		Test module to read WAL from WAL buffers.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	src/test/modules/read_wal_from_buffers/read_wal_from_buffers.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlog.h"
#include "fmgr.h"
#include "utils/pg_lsn.h"

PG_MODULE_MAGIC;

/*
 * SQL function to read WAL from WAL buffers. Returns number of bytes read.
 */
PG_FUNCTION_INFO_V1(read_wal_from_buffers);
Datum
read_wal_from_buffers(PG_FUNCTION_ARGS)
{
	XLogRecPtr	startptr = PG_GETARG_LSN(0);
	int32		bytes_to_read = PG_GETARG_INT32(1);
	Size		bytes_read = 0;
	char	   *data = palloc0(bytes_to_read);

	bytes_read = WALReadFromBuffers(data, startptr,
									(Size) bytes_to_read,
									GetWALInsertionTimeLine());

	pfree(data);

	PG_RETURN_INT32(bytes_read);
}
