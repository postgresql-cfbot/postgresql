/*-------------------------------------------------------------------------
 *
 * simpleundolog.c
 *		Simple implementation of PostgreSQL transaction-undo-log manager
 *
 * This module logs the cleanup procedures required during a transaction abort.
 * The information is recorded in files to ensure post-crash recovery runs the
 * necessary cleanup procedures.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/simpleundolog.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>

#include "lib/stringinfo.h"
#include "access/parallel.h"
#include "access/simpleundolog.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/storage_ulog.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/memutils.h"


#define ULOG_FILE_MAGIC 0x474f4c55	/* 'ULOG' in big-endian */

/*
 * Struct for undo-log disk data
 *
 * Each undo-log file is named in the format "<topxid>-<subxid>" and contains
 * undo data for a subtransaction. The file begins with a header followed by
 * undo log records. An undo file is created when the first undo log is issued
 * during a transaction and is removed upon the top transaction's commit or
 * processed then removed during each subtransaction's rollback. When a
 * transaction is prepared, this state is marked in the file header. The
 * prepared undo files are processed by subsequent COMMIT/ROLLBACK PREPARED
 * commands in the same manner as non-prepared files. In the event of a server
 * crash during a transaction, non-prepared undo files left behind are handled
 * before recovery starts. The recovery process may create new undo files,
 * which are processed at the end of recovery. Prepared undo files are
 * preserved throughout the recovery process.
 */
typedef struct UndoLogFileHeader
{
	int32 magic;			/* fixed ULOG file magic number */
	UndoLogFileState state;	/* state of this file */
	/* SimpleUndoLogRecord follows */
} UndoLogFileHeader;

typedef struct UndoDescData
{
	const char *rm_name;
	void	(*rm_undo) (SimpleUndoLogRecord *record,
						UndoLogFileState state, bool isCommit, bool cleanup);
} UndoDescData;

#define PG_RMGR(symname,name,redo,desc,identify,startup,cleanup,mask,decode,undo) \
	{ name, undo },

UndoDescData	UndoRoutines[RM_MAX_ID + 1] = {
#include "access/rmgrlist.h"
};
#undef PG_RMGR

/*
 * During a transaction, all undo logs are managed by a linked list. This
 * linked list is used for quick lookup of existing undo log files. We expect
 * the number of files to be relatively small, so more efficient algorithms are
 * not used here. During recovery, this list may contain undo log files for
 * multiple top transactions.
 */
/* Undo log wroking state */
typedef struct ActiveULog
{
	TransactionId xid;
	struct ActiveULog *next;
} ActiveULog;

/*
 * Struct for top-level management variables.
 *
 * active_ulogs holds the xid-subxid pairs for all subtransactions that issued
 * undo logs during a top-level transaction.  Once an undo log file is opened,
 * topxid, subxid, filename, and fd are set according to the currently open
 * file.
 */
typedef struct ULogStateData
{
	ActiveULog *active_ulogs;	/* list of subxacts with ulogs */
	ActiveULog *current_ulog;	/* current open entry */
	ActiveULog *prev_ulog;		/* previous entry for removal */
	TransactionId xid;			/* xid of the current ulog */
	char file_name[MAXPGPATH];	/* current ulog file name */
	int	fd;						/* file descriptor */
	UndoLogFileHeader file_header;	/* current ulog file header */
} ULogStateData;

static ULogStateData ULogState =
{NULL, NULL, NULL, InvalidTransactionId, "", -1, {0}};

/* ULOG uses the same sync mode as XLOG, except for the PG_O_DIRECT bit. */
static int
ULogGetSyncBit(void)
{
	return XLogGetSyncBit() & ~PG_O_DIRECT;
}

/*
 * undolog_set_filename()
 *
 * Generates undo log file name for the xid pair.
 */
static void
undolog_set_filename(char *buf, TransactionId xid)
{
	snprintf(buf, MAXPGPATH, "%s/%08x", SIMPLE_UNDOLOG_DIR, xid);
}


/*
 * undolog_load_file_header()
 *
 * Loads the header of the currently open file into the global buffer. The file
 * pointer will point to the beginning of the first record after this function
 * returns.
 */
static void
undolog_load_file_header(void)
{
	if (lseek(ULogState.fd, 0, SEEK_SET) < 0)
		ereport(ERROR,
					errcode_for_file_access(),
				errmsg("could not seek undolog file \"%s\": %m",
					   ULogState.file_name));

	if (read(ULogState.fd,
			 &ULogState.file_header, sizeof(ULogState.file_header)) < 0)
		ereport(ERROR,
				errcode_for_file_access(),
				errmsg("could not read undolog file \"%s\": %m",
					   ULogState.file_name));
	if (ULogState.file_header.magic != ULOG_FILE_MAGIC)
		ereport(ERROR,
				errcode_for_file_access(),
				errmsg("invalid undolog file \"%s\": magic don't match",
					   ULogState.file_name));
}

/*
 * undolog_write_file_header()
 *
 * Writes the header of the currently open file from the global buffer. The file
 * pointer will point to the beginning of the first record after this function
 * returns.
 */
static void
undolog_write_file_header(void)
{
	if (lseek(ULogState.fd, 0, SEEK_SET) < 0)
		ereport(ERROR,
					errcode_for_file_access(),
				errmsg("could not seek undolog file \"%s\": %m",
					   ULogState.file_name));

	if (write(ULogState.fd,
			  &ULogState.file_header, sizeof(ULogState.file_header)) < 0)
		ereport(ERROR,
				errcode_for_file_access(),
				errmsg("could not write undolog file \"%s\": %m",
					   ULogState.file_name));
}

/*
 * undolog_sync_file()
 *
 * Sync the currently open ULOG file.
 */
static void
undolog_sync_file(void)
{
	const char *msg;

	/*
	 * This function counts ULOG sync operation stats as part of WAL
	 * operations.  In the future, we may want to separate ULOG stats from WAL
	 * stats.
	 */
	if (!XLogFsyncFile(ULogState.fd, &msg))
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg(msg, ULogState.file_name)));
	}
}

/*
 * undolog_select_entry()
 *
 * Finds and selects the in-memory entry for the given xid pair.
 *
 * If create is false, returns false if not found, and no entry is selected.
 * If create is true, returns true if found; otherwise, create one and returns
 * false.
 */
static bool
undolog_select_entry(TransactionId xid, bool create)
{
	ActiveULog *prev;

	Assert (TransactionIdIsValid(xid));

	/* short cut when the entry is already selected */
	if (ULogState.current_ulog &&
		ULogState.current_ulog->xid == xid)
		return true;

	ULogState.current_ulog = ULogState.prev_ulog = NULL;

	/* we no longer use this file, close it */
	if (ULogState.fd >= 0)
	{
		/* Switched between subtransactions, close the current file */
		if (close(ULogState.fd) != 0)
			ereport(ERROR, errcode_for_file_access(),
					errmsg("could not close file \"%s\": %m",
						   ULogState.file_name));

		ULogState.xid = InvalidTransactionId;
		ULogState.fd = -1;
		ReleaseExternalFD();
	}

	/* search for the existing entry */
	prev = NULL;
	for (ActiveULog *p = ULogState.active_ulogs ; p ; p = p->next)
	{
		if (p->xid == xid)
		{
			ULogState.current_ulog = p;
			ULogState.prev_ulog = prev;
			return true;
		}

		prev = p;
	}

	/* no existing entry found; create a new one */
	if (create)
	{
		ActiveULog *newlog = (ActiveULog *)
			MemoryContextAlloc(TopMemoryContext, sizeof(ActiveULog));

		newlog->xid = xid;
		newlog->next = ULogState.active_ulogs;
		ULogState.active_ulogs = newlog;
		ULogState.current_ulog = newlog;
		ULogState.prev_ulog = NULL;
	}

	return false;
}

/*
 * undolog_remove_entry()
 *
 * Removes the currently selected entry.
 *
 * The entry to be deleted must have been previously selected using
 * undolog_select_entry(), and the corresponding log file is expected to have
 * already been deleted if it exists.
 */
static bool
undolog_remove_entry()
{
	ActiveULog *p = ULogState.current_ulog;

	if (ULogState.prev_ulog)
		ULogState.prev_ulog = p->next;
	else
		ULogState.active_ulogs = p->next;

	ULogState.current_ulog = ULogState.prev_ulog = NULL;

	pfree(p);

	return true;
}

/*
 * undolog_init_file()
 *
 * Sets the initial header of an already-opened ulog file.
 *
 * The file pointer will point to just after the header after this function
 * returns.
 */
static void
undolog_init_file(void)
{
	ULogState.file_header.magic = ULOG_FILE_MAGIC;
	ULogState.file_header.state = ULOG_FILE_DEFAULT;

	if (write(ULogState.fd, &ULogState.file_header,
			  sizeof(ULogState.file_header)) < 0)
	ereport(ERROR,
			errcode_for_file_access(),
			errmsg("could not write undolog file \"%s\": %m",
				   ULogState.file_name));
}

/*
 * undolog_open_file_by_name()
 *
 * Opens a ULOG file specified by ULogState.file_name.
 *
 * Returns true if the file was found, and false if not found.
 *
 * If create is true, this function errors out if the file already
 * exists. During recovery, this function may attempt to create
 * already-existing ULOG file for an uncommitted prepared transaction.  In this
 * case, the existing file is opened instead of causing an error.
 *
 * Note that xid values in ULogState are set to invalid even after a sccessful
 * return. They will be set by undolog_open_fild().
 */
static bool
undolog_open_file_by_name(bool create)
{
	int omode = 0;
	int cmode = 0;

	Assert (ULogState.fd < 0);

	omode = PG_BINARY | O_RDWR | ULogGetSyncBit();

	if (create)
		cmode = O_CREAT | O_EXCL;

	ULogState.fd = BasicOpenFile(ULogState.file_name, omode | cmode);

	if (ULogState.fd < 0)
	{
		if (!create)
		{
			if (errno == ENOENT)
				return false;

			ereport(ERROR,
					errcode_for_file_access(),
					errmsg("could not open undolog file \"%s\": %m",
						   ULogState.file_name));
		}

		/*
		 * ULOG files for prepared transactions are preserved throughout the
		 * recovery process. Therefore, recovery may attempt to create an
		 * already existing file. If the file is confirmed to be prepared, we
		 * should continue the recovery and will ignore all ULOG writes to this
		 * file. See UndoLogCleanup() for details.

		 */
		if (errno == EEXIST && RecoveryInProgress())
		{
			ULogState.fd = BasicOpenFile(ULogState.file_name, omode);
			if (ULogState.fd >= 0)
			{
				undolog_load_file_header();
				if (ULogState.file_header.state == ULOG_FILE_DEFAULT)
					elog(PANIC, "non prepared file found: %s",
						 ULogState.file_name);

				elog(LOG, "ulog file for prepared transaction found: %s",
					 ULogState.file_name);
			}

			/* restore the orignal error number */
			errno = EEXIST;
		}

		if (ULogState.fd < 0)
			ereport(ERROR,
					errcode_for_file_access(),
					errmsg("could not create undolog file \"%s\": %m",
						   ULogState.file_name));
	}
	else if (create)
	{
		undolog_init_file();
		undolog_sync_file();
	}
	else
		undolog_load_file_header();

	ReserveExternalFD();
	ULogState.xid = InvalidTransactionId;

	/* in create mode, return false since the file was not found */
	if (create)
		return false;

	return true;
}

/*
 * undolog_open_file() - Opens a ulog file for the specified xid pair.
 *
 * See undolog_open_file_by_name() for more details.
 *
 * XID values and file_name in ULogState are set after a successful
 * return. Otherwise, they are set to invalid values.
 */
static bool
undolog_open_file(TransactionId xid, bool create)
{
	bool ret;

	/* shortcut for repeated usage of the same file */
	if (ULogState.xid == xid)
	{
		Assert(ULogState.fd >= 0);
		return true;
	}

	/* Switched between subtransactions, close the current file if any */
	if  (ULogState.fd >= 0)
	{
		if (close(ULogState.fd) != 0)
			ereport(ERROR, errcode_for_file_access(),
					errmsg("could not close file \"%s\": %m",
						   ULogState.file_name));

		ULogState.xid = InvalidTransactionId;
		ULogState.fd = -1;
		ReleaseExternalFD();
	}

	Assert (ULogState.xid == InvalidTransactionId &&
			ULogState.fd == -1);

	/* Set the file name */
	undolog_set_filename(ULogState.file_name, xid);

	/* Do the task */
	ret = undolog_open_file_by_name(create);

	/* Set the xid pair for this file if opened */
	if (ret || create)
		ULogState.xid = xid;

	return ret;
}

/*
 * undolog_close_file() - Closes the curerntly opened ulog file, if any.
 */
static void
undolog_close_file(void)
{
	if (ULogState.fd < 0)
		return;

	if (close(ULogState.fd) != 0)
		ereport(ERROR, errcode_for_file_access(),
				errmsg("could not close file \"%s\": %m",
					   ULogState.file_name));

	ULogState.xid = InvalidTransactionId;
	ULogState.fd = -1;
	ReleaseExternalFD();
}

/*
 * undolog_remove_file() - Removes a file specified by ULogState.file_name.
 *
 * The file must already be closed.
 */
static void
undolog_remove_file(void)
{
	durable_unlink(ULogState.file_name, FATAL);
	ULogState.file_name[0] = 0;
}

/*
 * undolog_remove_file_by_xid()
 *
 * Removes a file specified by an xid pair. Closes the file if it is open.
 */
static void
undolog_remove_file_by_xid(TransactionId xid)
{
	char file_name[MAXPGPATH];

	if (ULogState.xid == xid)
		undolog_close_file();

	undolog_set_filename(file_name, xid);
	durable_unlink(file_name, FATAL);
}

/*
 * SimpleUndoLogWrite() - Write an undolog record using current xid
 */
void
SimpleUndoLogWrite(RmgrId rmgr, uint8 info, void *data, int len)
{
	/*
	 *  The following lines may assign a new transaction ID. This is somewhat
	 *  clumsy, but the caller needs to assign it soon.
	 */
	TransactionId xid = GetCurrentTransactionId();

	SimpleUndoLogWriteRedo(rmgr, info, data, len, xid);
}

/*
 * SimpleUndoLogWriteRedo() -  Writes an undolog record
 *
 * topxid is the XID of the top-level transaction. subxid is the assigned
 * TransactionId of the current transaction, not a SubTransactionId.
 * InvalidTransactionId indicates that the current transaction is the top-level
 * transaction.  See SimpleUndoLogWrite().
 *
 * This function is exposed for use during recovery.
 */
void
SimpleUndoLogWriteRedo(RmgrId rmgr, uint8 info, void *data, int len,
					   TransactionId xid)
{
	int reclen = sizeof(SimpleUndoLogRecord) + len;
	SimpleUndoLogRecord *rec;
	pg_crc32c	undodata_crc;

	Assert(!IsParallelWorker());

	/* Inacitvate undo system during bootprocessing mode */
	if (IsBootstrapProcessingMode())
		return;

	/* We must be in xid-assigned transactions */
	Assert(TransactionIdIsValid(xid));

	/* The caller can set rmgr bits only. */
	if ((info & ~ULR_RMGR_INFO_MASK) != 0)
		elog(PANIC, "invalid ulog info mask %02X", info);

	if (!undolog_select_entry(xid, true))
	{
		/* new entry created, create the corresponding file */
		undolog_open_file(xid, true);
	}
	else
	{
		/* entry exists, open existing file */
		if (!undolog_open_file(xid, false))
			ereport(ERROR,
					errcode_for_file_access(),
					errmsg("could not open undolog file \"%s\": %m",
						   ULogState.file_name));
	}

	/*
	 * Writes to files for prepared transactions are ignored during
	 * recovery. See undolog_open_file_by_name() for more details.
	 */
	if (ULogState.file_header.state != ULOG_FILE_DEFAULT)
	{
		Assert (RecoveryInProgress());
		return;
	}

	rec = palloc(reclen);
	rec->ul_tot_len = reclen;
	rec->ul_rmid = rmgr;
	rec->ul_info = info;

	memcpy((char *)rec + sizeof(SimpleUndoLogRecord), data, len);

	/* Calculate CRC of the data */
	INIT_CRC32C(undodata_crc);
	COMP_CRC32C(undodata_crc, &rec->ul_rmid,
				reclen - offsetof(SimpleUndoLogRecord, ul_rmid));
	rec->ul_crc = undodata_crc;


	if (write(ULogState.fd, rec, reclen) < 0)
				ereport(ERROR,
						errcode_for_file_access(),
						errmsg("could not write to undolog file \"%s\": %m",
							   ULogState.file_name));

	pfree(rec);
	undolog_sync_file();
}

/*
 * ulog_process_ulogfile() - Processes the currently open undo log.
 *
 * The file must be opened beforehand.
 * If cleanup is true, this function informs the undo callback functions that
 * it is called during recovery cleanup and the transaction is prepared.  In
 * this case, the undo callback may need to behave differently.
 */
#define ULOG_READBUF_INIT_SIZE 32
static void
undolog_process_ulogfile(bool isCommit, bool cleanup, MemoryContext outercxt)
{
	static int		bufsize = 0;
	static char	   *buf = NULL;
	int				ret;

	StaticAssertDecl(sizeof(SimpleUndoLogRecord) <= ULOG_READBUF_INIT_SIZE,
					 "initial buffer size too small");

	Assert (ULogState.fd >= 0);
	Assert (outercxt);

	undolog_load_file_header();

	bufsize = ULOG_READBUF_INIT_SIZE;
	buf = palloc(bufsize);

	while ((ret = read(ULogState.fd, buf, sizeof(SimpleUndoLogRecord))) ==
		   sizeof(SimpleUndoLogRecord))
	{
		SimpleUndoLogRecord *rec = (SimpleUndoLogRecord *) buf;
		int readlen = rec->ul_tot_len - sizeof(SimpleUndoLogRecord);
		MemoryContext	oldcxt;
		pg_crc32c	undodata_crc;

		if (rec->ul_tot_len > bufsize)
		{
			bufsize *= 2;
			buf = repalloc(buf, bufsize);
			rec = (SimpleUndoLogRecord *) buf;
		}

		ret = read(ULogState.fd,
				   buf + sizeof(SimpleUndoLogRecord), readlen);
		if (ret != readlen)
		{
			if (ret < 0)
				ereport(ERROR,
						errcode_for_file_access(),
						errmsg("could not read undo log file \"%s\": %m",
							   ULogState.file_name));

			ereport(ERROR,
					errcode_for_file_access(),
					errmsg("reading undo log expected %d bytes, but actually %d: %s",
						   readlen, ret, ULogState.file_name));

		}

		/* CRC check */
		INIT_CRC32C(undodata_crc);
		COMP_CRC32C(undodata_crc, &rec->ul_rmid,
					rec->ul_tot_len - offsetof(SimpleUndoLogRecord, ul_rmid));
		if (!EQ_CRC32C(rec->ul_crc, undodata_crc))
		{
			/*
			 * The location is the byte immediately following the just-read
			 * record. We cannot issue ERROR because this function is called
			 * during abort processing.
			 */
			off_t off = lseek(ULogState.fd, 0, SEEK_CUR);
			ereport(WARNING,
					errmsg("incorrect undolog record checksum at %lld in %s",
						   (long long int) off, ULogState.file_name),
					errdetail("Aborted undo processing of the corresponding transaction."));
		}

		/* The undo routines may want to allcoate memory in the outer context */
		oldcxt = MemoryContextSwitchTo(outercxt);
		UndoRoutines[rec->ul_rmid].rm_undo(rec,
										   ULogState.file_header.state,
										   isCommit, cleanup);
		MemoryContextSwitchTo(oldcxt);
	}

	if (ret != 0)
	{
		if (ret < 0)
				ereport(ERROR,
						errcode_for_file_access(),
						errmsg("could not read undo log file \"%s\": %m",
							   ULogState.file_name));
		if (ret != sizeof(SimpleUndoLogRecord))
			ereport(ERROR,
					errcode_for_file_access(),
					errmsg("reading undo log expected %d bytes, but actually %d: %s",
						   (int) sizeof(SimpleUndoLogRecord), ret, ULogState.file_name));
	}
}

/*
 * ulog_undo() - Processes undo log for the specified xid pair
 *
 * The undo log file for the xid pair is removed before this function returns,
 * regardless whether it is processed or not. Therefore, the in-memory entry
 * for this xid pair must be removed afterwards, if any.
 */
static void
undolog_undo(bool isCommit, TransactionId xid)
{
	Assert(!IsParallelWorker());

	/* Return if no undo log exists for the xid pair */
	if (!undolog_open_file(xid, false))
		return;

	undolog_process_ulogfile(isCommit, false, CurrentMemoryContext);

	undolog_remove_file_by_xid(xid);
}

/*
 * ulog_exists() - Return true if the log file for the xid pair exists.
 *
 * This function has no side effects.
 */
static bool
undolog_exists(TransactionId xid)
{
	char fname[MAXPGPATH];
	struct stat statbuf;

	undolog_set_filename(fname, xid);

	if (stat(fname, &statbuf) < 0)
	{
		if (errno == ENOENT)
			return false;
		ereport(ERROR, errmsg("stat failed for undo file \"%s\": %m", fname));
	}

	return true;
}

/*
 * SimpleUndoLog_UndoByXid()
 *
 * Processes undo logs for the specified transaction, intended for usein
 * finishing prepared transactins or recovery.
 *
 * children is the list of subtransaction IDs of the topxid, with a length of
 * nchildren.
 */
void
SimpleUndoLog_UndoByXid(bool isCommit, TransactionId xid,
							 int nchildren, TransactionId *children)
{
	ActiveULog *p;
	ActiveULog *prev;

	Assert (RecoveryInProgress() || ULogState.active_ulogs == NULL);

	/* process undo logs */
	if (undolog_exists(xid))
		undolog_undo(isCommit, xid);

	for (int i = 0 ; i < nchildren ; i++)
	{
		if (undolog_exists(children[i]))
			undolog_undo(isCommit, children[i]);
	}

	/*
	 * Remove in-memory entries for this transaction tree if any.
	 * We have these entries only during recovery.
	 */
	Assert (RecoveryInProgress() || ULogState.active_ulogs == NULL);
	prev = NULL;
	for (p = ULogState.active_ulogs ; p ; p = p->next)
	{
		bool match = false;

		if (p->xid == xid)
			match = true;
		else
		{
			for (int i = 0 ; i < nchildren ; i++)
			{
				if (p->xid == children[i])
				{
					match = true;
					break;
				}
			}
		}

		if (!match)
		{
			prev = p;
			continue;
		}

		/* remove this entry */
		if (prev)
			prev->next = p->next;
		else
			ULogState.active_ulogs = p->next;
		pfree(p);

		break;
	}
}

/*
 * AtEOXact_SimpleUndoLog() - At end-of-xact processing of undo logs.
 *
 * Processes all existing undo log files, leaving none remaining after this
 * function returns.
 */
void
AtEOXact_SimpleUndoLog(bool isCommit)
{
	ActiveULog *p = ULogState.active_ulogs;

	if (!p)
		return;

	while(p)
	{
		ActiveULog *prev = p;

		undolog_undo(isCommit, p->xid);

		ULogState.active_ulogs = p = p->next;
		pfree(prev);
	}

	ULogState.active_ulogs = ULogState.current_ulog = NULL;
}

/*
 * AtEOXact_SimpleUndoLog() - At end-of-subxact processing of undo logs.
 *
 * The undo log for the subtransaction will be removed on abort. It will remain
 * on commit and be processed at the end of the top-level transaction.
 */
void
AtEOSubXact_SimpleUndoLog(bool isCommit)
{
	ActiveULog *p = ULogState.current_ulog;
	TransactionId xid;

	/*
	 * Undo logs of committed subtransactions are processed at the end of the
	 * top-level transaction.
	 */
	if (isCommit || !p)
		return;

	xid = GetCurrentTransactionIdIfAny();

	/* Return if the innermost subxid is not assigned. */
	if (!TransactionIdIsValid(xid))
		return;

	if (!undolog_select_entry(xid, false))
		return;

	undolog_undo(isCommit, xid);
	undolog_remove_entry();
}

/*
 * UndoLogCleanup() - On-recovery cleanup of undo log
 *
 * This function is called once before the redo process of recovery to remove
 * files created by uncommitted transactions before the server crash. It is
 * then called again after the redo process to clean up any leftover garbage
 * after the redo process.
 */
void
UndoLogCleanup(void)
{
	DIR		   *dirdesc;
	struct dirent *de;
	MemoryContext mcxt, outercxt;
	ActiveULog	*next;

	/*
	 * Some memory allocation occurs during this process. Use a separate memory
	 * context to avoid memory leaks.
	 */
	mcxt = AllocSetContextCreate(CurrentMemoryContext,
								 "UndologContext",
								 ALLOCSET_DEFAULT_SIZES);
	outercxt = MemoryContextSwitchTo(mcxt);

	undolog_close_file();

	/* scan through all undo log files */
	dirdesc = AllocateDir(SIMPLE_UNDOLOG_DIR);
	while ((de = ReadDir(dirdesc, SIMPLE_UNDOLOG_DIR)) != NULL)
	{
		if (strspn(de->d_name, "01234567890abcdef") < strlen(de->d_name))
			continue;

		snprintf(ULogState.file_name, MAXPGPATH, "%s/%s",
				 SIMPLE_UNDOLOG_DIR, de->d_name);

		undolog_open_file_by_name(false);
		undolog_process_ulogfile(false, true, outercxt);

		/* Mark this log as crashed after prepared if not yet done */
		if (ULogState.file_header.state == ULOG_FILE_PREPARED)
		{
			ULogState.file_header.state = ULOG_FILE_CRASH_AFTER_PREPARED;
			undolog_write_file_header();
			undolog_sync_file();
		}
		undolog_close_file();

		/*
		 * Do not remove ULOG files for prepared transactions.  We cannot
		 * remove them and let recovery recreate them, because an existing file
		 * for a prepared transaction may contain logs from before the latest
		 * checkpoint, which would be lost in the newly created ulog files.
		 */
		if (ULogState.file_header.state == ULOG_FILE_DEFAULT)
			undolog_remove_file();
	}

	MemoryContextSwitchTo(outercxt);
	MemoryContextDelete(mcxt);

	/* Clean up in-memory data */
	for (ActiveULog *p = ULogState.active_ulogs ; p ; p = next)
	{
		next = p->next;
		pfree(p);
	}

	ULogState.active_ulogs = ULogState.current_ulog = NULL;
	ULogState.xid = InvalidTransactionId;
	ULogState.file_name[0] = 0;
}

/*
 * AtPrepare_SimpleUndoLog()
 *
 * Mark all undo logs as prepared.
 *
 * This mark is referenced by crash recovery to determine that each UNDO log
 * file needs to be preserved.
 */
void
AtPrepare_SimpleUndoLog(void)
{
	ActiveULog *p = ULogState.active_ulogs;
	ActiveULog *prev = NULL;
	ActiveULog *tmp;

	Assert (!RecoveryInProgress());

	while(p)
	{
		/* return no undo log exists for the transaction */
		if (!undolog_open_file(p->xid, false))
			ereport(ERROR,
					errcode_for_file_access(),
					errmsg("failed to open undolog file \"%s\": %m",
						   ULogState.file_name));

		undolog_load_file_header();
		ULogState.file_header.state = ULOG_FILE_PREPARED;
		undolog_write_file_header();
		undolog_sync_file();
		undolog_close_file();

		if (prev)
			prev->next = p->next;
		else
			ULogState.active_ulogs = p->next;

		tmp = p->next;
		pfree(p);
		p = tmp;
	}
}
