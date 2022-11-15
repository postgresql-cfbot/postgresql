/*--------------------------------------------------------------------------
 *
 * test_custom_rmgrs.c
 *		Code for testing custom WAL resource managers.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/test/modules/test_custom_rmgrs/test_custom_rmgrs.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/pg_lsn.h"

PG_MODULE_MAGIC;

/*
 * test_custom_rmgrs WAL record message.
 */
typedef struct xl_testcustomrmgrs_message
{
	Size		message_size;   /* size of the message */
	char		message[FLEXIBLE_ARRAY_MEMBER]; /* payload */
} xl_testcustomrmgrs_message;

#define SizeOfTestCustomRmgrsMessage	(offsetof(xl_testcustomrmgrs_message, message))
#define XLOG_TEST_CUSTOM_RMGRS_MESSAGE	0x00

/*
 * Typically one needs to reserve unique RmgrId at
 * https://wiki.postgresql.org/wiki/CustomWALResourceManagers. However, we use
 * experimental RmgrId in this test module.
 */
static RmgrId rmid = RM_EXPERIMENTAL_ID;
static RmgrData rmgr;

/*
 * Define these RMGR APIs to the taste, see xlog_internal.h and rmgrlist.h for
 * reference.
 */
void		testcustomrmgrs_redo(XLogReaderState *record);
void		testcustomrmgrs_desc(StringInfo buf, XLogReaderState *record);
const char *testcustomrmgrs_identify(uint8 info);

PG_FUNCTION_INFO_V1(test_custom_rmgrs_synthesize_wal_record);

/*
 * Module load callback
 */
void
_PG_init(void)
{
	/*
	 * In order to create our own custom resource manager, we have to be loaded
	 * via shared_preload_libraries.
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	MemSet(&rmgr, 0, sizeof(rmgr));
	rmgr.rm_name = "test_custom_rmgrs";
	rmgr.rm_redo = testcustomrmgrs_redo;
	rmgr.rm_desc = testcustomrmgrs_desc;
	rmgr.rm_identify = testcustomrmgrs_identify;

	RegisterCustomRmgr(rmid, &rmgr);
}

/* RMGR API implementation */

/*
 * Redo is basically just a noop for this module.
 */
void
testcustomrmgrs_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info != XLOG_TEST_CUSTOM_RMGRS_MESSAGE)
		elog(PANIC, "testcustomrmgrs__redo: unknown op code %u", info);
}

void
testcustomrmgrs_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_TEST_CUSTOM_RMGRS_MESSAGE)
	{
		xl_testcustomrmgrs_message *xlrec = (xl_testcustomrmgrs_message *) rec;

		appendStringInfo(buf, "payload (%zu bytes): %s", xlrec->message_size, xlrec->message);
	}
}

const char *
testcustomrmgrs_identify(uint8 info)
{
	if ((info & ~XLR_INFO_MASK) == XLOG_TEST_CUSTOM_RMGRS_MESSAGE)
		return "TEST_CUSTOM_RMGRS_MESSAGE";

	return NULL;
}

/*
 * SQL function for writing a synthesized message into WAL with the help of custom WAL
 * resource manager.
 */
Datum
test_custom_rmgrs_synthesize_wal_record(PG_FUNCTION_ARGS)
{
	int64		size = PG_GETARG_INT64(0);
	XLogRecPtr	lsn;
	char 		*message;
	static char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	xl_testcustomrmgrs_message xlrec;

	if (size <= 0)
		ereport(ERROR,
				(errmsg("size %lld must be greater than zero to synthesize WAL record",
						(long long) size)));

	/*
	 * Since this a test module, let's limit maximum size of WAL record to be
	 * 1MB to not over-allocate memory.
	 */
	if (size > 1024*1024)
		size = 1024*1024;

	message = (char *) palloc0(size);

	/* Prepare a random message */
	for (int i = 0; i < size; i++)
	{
		int key = rand() % (int)(sizeof(charset) -1);
		message[i] = charset[key];
	}

	xlrec.message_size = size;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfTestCustomRmgrsMessage);
	XLogRegisterData((char *) &message, size);

	/* Let's mark this record as unimportant, just in case. */
	XLogSetRecordFlags(XLOG_MARK_UNIMPORTANT);

	lsn = XLogInsert(rmid, XLOG_TEST_CUSTOM_RMGRS_MESSAGE);

	pfree(message);

	PG_RETURN_LSN(lsn);
}
