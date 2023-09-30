/*-------------------------------------------------------------------------
 *
 * simpleundolog.c
 *		Simple implementation of PostgreSQL transaction-undo-log manager
 *
 * In this module, procedures required during a transaction abort are
 * logged. Persisting this information becomes crucial, particularly for
 * ensuring reliable post-processing during the restart following a transaction
 * crash. At present, in this module, logging of information is performed by
 * simply appending data to a created file.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/clog.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/simpleundolog.h"
#include "access/twophase_rmgr.h"
#include "access/parallel.h"
#include "access/xact.h"
#include "catalog/storage_ulog.h"
#include "storage/fd.h"

#define ULOG_FILE_MAGIC 0x12345678

typedef struct UndoLogFileHeader
{
	int32 magic;
	bool  prepared;
} UndoLogFileHeader;

typedef struct UndoDescData
{
	const char *name;
	void	(*rm_undo) (SimpleUndoLogRecord *record, bool prepared);
} UndoDescData;

/* must be kept in sync with RmgrData definition in xlog_internal.h */
#define PG_RMGR(symname,name,redo,desc,identify,startup,cleanup,mask,decode,undo) \
	{ name, undo },

UndoDescData	UndoRoutines[RM_MAX_ID + 1] = {
#include "access/rmgrlist.h"
};
#undef PG_RMGR

#if defined(O_DSYNC)
static int undo_sync_mode = O_DSYNC;
#elif defined(O_SYNC)
static int undo_sync_mode = O_SYNC;
#else
static int undo_sync_mode = 0;
#endif

static char current_ulogfile_name[MAXPGPATH];
static int	current_ulogfile_fd = -1;
static int  current_xid = InvalidTransactionId;
static UndoLogFileHeader current_fhdr;

static void
undolog_check_file_header(void)
{
	if (read(current_ulogfile_fd, &current_fhdr, sizeof(current_fhdr)) < 0)
		ereport(PANIC,
				errcode_for_file_access(),
				errmsg("could not read undolog file \"%s\": %m",
					   current_ulogfile_name));
	if (current_fhdr.magic != ULOG_FILE_MAGIC)
		ereport(PANIC,
				errcode_for_file_access(),
				errmsg("invalid undolog file \"%s\": magic don't match",
					   current_ulogfile_name));
}

static bool
undolog_open_current_file(TransactionId xid, bool forread, bool append)
{
	int omode;

	if (current_ulogfile_fd >= 0)
	{
		/* use existing open file */
		if (current_xid == xid)
		{
			if (append)
				return true;

			if (lseek(current_ulogfile_fd,
					  sizeof(UndoLogFileHeader), SEEK_SET) < 0)
				ereport(PANIC,
						errcode_for_file_access(),
						errmsg("could not seek undolog file \"%s\": %m",
							   current_ulogfile_name));
		}

		close(current_ulogfile_fd);
		current_ulogfile_fd = -1;
		ReleaseExternalFD();
	}

	current_xid = xid;
	if (!TransactionIdIsValid(xid))
		return false;

	omode = PG_BINARY | undo_sync_mode;

	if (forread)
		omode |= O_RDONLY;
	else
	{
		omode |= O_RDWR;

		if (!append)
			omode |= O_TRUNC;
	}

	snprintf(current_ulogfile_name, MAXPGPATH, "%s/%08x",
			 SIMPLE_UNDOLOG_DIR, xid);
	current_ulogfile_fd = BasicOpenFile(current_ulogfile_name, omode);
	if (current_ulogfile_fd >= 0)
		undolog_check_file_header();
	else
	{
		if (forread)
			return false;

		current_fhdr.magic = ULOG_FILE_MAGIC;
		current_fhdr.prepared = false;

		omode |= O_CREAT;
		current_ulogfile_fd = BasicOpenFile(current_ulogfile_name, omode);
		if (current_ulogfile_fd < 0)
			ereport(PANIC,
					errcode_for_file_access(),
					errmsg("could not create undolog file \"%s\": %m",
						   current_ulogfile_name));

		if (write(current_ulogfile_fd, &current_fhdr, sizeof(current_fhdr)) < 0)
			ereport(PANIC,
					errcode_for_file_access(),
					errmsg("could not write undolog file \"%s\": %m",
						   current_ulogfile_name));
	}

	/*
	 * move file pointer to the end of the file. we do this not using O_APPEND,
	 * to allow us to modify data at any location in the file. We already moved
	 * to the first record in the case of !append.
	 */
	if (append)
	{
		if (lseek(current_ulogfile_fd, 0, SEEK_END) < 0)
			ereport(PANIC,
					errcode_for_file_access(),
					errmsg("could not seek undolog file \"%s\": %m",
						   current_ulogfile_name));
	}
	ReserveExternalFD();

	return true;
}

/*
 * Write ulog record
 */
void
SimpleUndoLogWrite(RmgrId rmgr, uint8 info,
				   TransactionId xid, void *data, int len)
{
	int reclen = sizeof(SimpleUndoLogRecord) + len;
	SimpleUndoLogRecord *rec = palloc(reclen);
	pg_crc32c	undodata_crc;

	Assert(!IsParallelWorker());
	Assert(xid != InvalidTransactionId);

	undolog_open_current_file(xid, false, true);

	rec->ul_tot_len = reclen;
	rec->ul_rmid = rmgr;
	rec->ul_info = info;
	rec->ul_xid = current_xid;

	memcpy((char *)rec + sizeof(SimpleUndoLogRecord), data, len);

	/* Calculate CRC of the data */
	INIT_CRC32C(undodata_crc);
	COMP_CRC32C(undodata_crc, rec,
				reclen - offsetof(SimpleUndoLogRecord, ul_rmid));
	rec->ul_crc = undodata_crc;


	if (write(current_ulogfile_fd, rec, reclen) < 0)
				ereport(ERROR,
						errcode_for_file_access(),
						errmsg("could not write to undolog file \"%s\": %m",
							   current_ulogfile_name));
}

static void
SimpleUndoLogUndo(bool cleanup)
{
	int		bufsize;
	char   *buf;

	bufsize = 1024;
	buf = palloc(bufsize);

	Assert(current_ulogfile_fd >= 0);

	while (read(current_ulogfile_fd, buf, sizeof(SimpleUndoLogRecord)) ==
		   sizeof(SimpleUndoLogRecord))
	{
		SimpleUndoLogRecord *rec = (SimpleUndoLogRecord *) buf;
		int readlen = rec->ul_tot_len - sizeof(SimpleUndoLogRecord);
		int ret;

		if (rec->ul_tot_len > bufsize)
		{
			bufsize *= 2;
			buf = repalloc(buf, bufsize);
		}

		ret = read(current_ulogfile_fd,
				   buf + sizeof(SimpleUndoLogRecord), readlen);
		if (ret != readlen)
		{
			if (ret < 0)
				ereport(ERROR,
						errcode_for_file_access(),
						errmsg("could not read undo log file \"%s\": %m",
							   current_ulogfile_name));

			ereport(ERROR,
					errcode_for_file_access(),
					errmsg("reading undo log expected %d bytes, but actually %d: %s",
						   readlen, ret, current_ulogfile_name));

		}

		UndoRoutines[rec->ul_rmid].rm_undo(rec,
										   current_fhdr.prepared && cleanup);
	}
}

void
AtEOXact_SimpleUndoLog(bool isCommit, TransactionId xid)
{
	if (IsParallelWorker())
		return;

	if (!undolog_open_current_file(xid, true, false))
		return;

	if (!isCommit)
		SimpleUndoLogUndo(false);

	if (current_ulogfile_fd > 0)
	{
		if (close(current_ulogfile_fd) != 0)
			ereport(PANIC, errcode_for_file_access(),
					errmsg("could not close file \"%s\": %m",
						   current_ulogfile_name));

		current_ulogfile_fd = -1;
		ReleaseExternalFD();
		durable_unlink(current_ulogfile_name, FATAL);
	}

	return;
}

void
UndoLogCleanup(void)
{
	DIR		   *dirdesc;
	struct dirent *de;
	char      **loglist;
	int			loglistspace = 128;
	int			loglistlen = 0;
	int			i;

	loglist = palloc(sizeof(char*) * loglistspace);

	dirdesc = AllocateDir(SIMPLE_UNDOLOG_DIR);
	while ((de = ReadDir(dirdesc, SIMPLE_UNDOLOG_DIR)) != NULL)
	{
		if (strspn(de->d_name, "01234567890abcdef") < strlen(de->d_name))
			continue;

		if (loglistlen >= loglistspace)
		{
			loglistspace *= 2;
			loglist = repalloc(loglist, sizeof(char*) * loglistspace);
		}
		loglist[loglistlen++] = pstrdup(de->d_name);
	}

	for (i = 0 ; i < loglistlen ; i++)
	{
		snprintf(current_ulogfile_name, MAXPGPATH, "%s/%s",
				 SIMPLE_UNDOLOG_DIR, loglist[i]);
		current_ulogfile_fd =  BasicOpenFile(current_ulogfile_name,
											O_RDWR | PG_BINARY |
											undo_sync_mode);
		undolog_check_file_header();
		SimpleUndoLogUndo(true);
		if (close(current_ulogfile_fd) != 0)
			ereport(PANIC, errcode_for_file_access(),
					errmsg("could not close file \"%s\": %m",
						   current_ulogfile_name));
		current_ulogfile_fd = -1;

		/* do not remove ulog files for prepared transactions */
		if (!current_fhdr.prepared)
			durable_unlink(current_ulogfile_name, FATAL);
	}
}

void
SimpleUndoLogSetPrpared(TransactionId xid, bool prepared)
{
	Assert(xid != InvalidTransactionId);

	undolog_open_current_file(xid, false, true);
	current_fhdr.prepared = prepared;
	if (lseek(current_ulogfile_fd, 0, SEEK_SET) < 0)
		ereport(PANIC,
				errcode_for_file_access(),
				errmsg("could not seek undolog file \"%s\": %m",
					   current_ulogfile_name));

	if (write(current_ulogfile_fd, &current_fhdr, sizeof(current_fhdr)) < 0)
		ereport(PANIC,
				errcode_for_file_access(),
				errmsg("could not write undolog file \"%s\": %m",
					   current_ulogfile_name));
}
