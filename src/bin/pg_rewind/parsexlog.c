/*-------------------------------------------------------------------------
 *
 * parsexlog.c
 *	  Functions for reading Write-Ahead-Log
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <unistd.h>
#include <sys/stat.h>

#include "pg_rewind.h"
#include "filemap.h"
#include "logging.h"

#include "access/rmgr.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "catalog/storage_xlog.h"
#include "commands/dbcommands_xlog.h"


/*
 * RmgrNames is an array of resource manager names, to make error messages
 * a bit nicer.
 */
#define PG_RMGR(symname,name,redo,desc,identify,startup,cleanup,mask) \
  name,

static const char *RmgrNames[RM_MAX_ID + 1] = {
#include "access/rmgrlist.h"
};

static void extractPageInfo(XLogReaderState *record);

static int	xlogreadfd = -1;
static XLogSegNo xlogreadsegno = -1;
static char xlogfpath[MAXPGPATH];

typedef struct XLogPageReadPrivate
{
	const char *datadir;
	const char *restoreCommand;
	int			tliIndex;
} XLogPageReadPrivate;

static int SimpleXLogPageRead(XLogReaderState *xlogreader,
				   XLogRecPtr targetPagePtr,
				   int reqLen, XLogRecPtr targetRecPtr, char *readBuf,
				   TimeLineID *pageTLI);

static int RestoreArchivedWAL(const char *path, const char *xlogfname,
				   off_t expectedSize, const char *restoreCommand);

/*
 * Read WAL from the datadir/pg_wal, starting from 'startpoint' on timeline
 * index 'tliIndex' in target timeline history, until 'endpoint'. Make note of
 * the data blocks touched by the WAL records, and return them in a page map.
 */
void
extractPageMap(const char *datadir, XLogRecPtr startpoint, int tliIndex,
			   XLogRecPtr endpoint, const char *restore_command)
{
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char	   *errormsg;
	XLogPageReadPrivate private;

	private.datadir = datadir;
	private.tliIndex = tliIndex;
	private.restoreCommand = restore_command;
	xlogreader = XLogReaderAllocate(WalSegSz, &SimpleXLogPageRead,
									&private);
	if (xlogreader == NULL)
		pg_fatal("out of memory\n");

	do
	{
		record = XLogReadRecord(xlogreader, startpoint, &errormsg);

		if (record == NULL)
		{
			XLogRecPtr	errptr;

			errptr = startpoint ? startpoint : xlogreader->EndRecPtr;

			if (errormsg)
				pg_fatal("could not read WAL record at %X/%X: %s\n",
						 (uint32) (errptr >> 32), (uint32) (errptr),
						 errormsg);
			else
				pg_fatal("could not read WAL record at %X/%X\n",
						 (uint32) (startpoint >> 32),
						 (uint32) (startpoint));
		}

		extractPageInfo(xlogreader);

		startpoint = InvalidXLogRecPtr; /* continue reading at next record */

	} while (xlogreader->ReadRecPtr != endpoint);

	XLogReaderFree(xlogreader);
	if (xlogreadfd != -1)
	{
		close(xlogreadfd);
		xlogreadfd = -1;
	}
}

/*
 * Reads one WAL record. Returns the end position of the record, without
 * doing anything with the record itself.
 */
XLogRecPtr
readOneRecord(const char *datadir, XLogRecPtr ptr, int tliIndex)
{
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char	   *errormsg;
	XLogPageReadPrivate private;
	XLogRecPtr	endptr;

	private.datadir = datadir;
	private.tliIndex = tliIndex;
	xlogreader = XLogReaderAllocate(WalSegSz, &SimpleXLogPageRead,
									&private);
	if (xlogreader == NULL)
		pg_fatal("out of memory\n");

	record = XLogReadRecord(xlogreader, ptr, &errormsg);
	if (record == NULL)
	{
		if (errormsg)
			pg_fatal("could not read WAL record at %X/%X: %s\n",
					 (uint32) (ptr >> 32), (uint32) (ptr), errormsg);
		else
			pg_fatal("could not read WAL record at %X/%X\n",
					 (uint32) (ptr >> 32), (uint32) (ptr));
	}
	endptr = xlogreader->EndRecPtr;

	XLogReaderFree(xlogreader);
	if (xlogreadfd != -1)
	{
		close(xlogreadfd);
		xlogreadfd = -1;
	}

	return endptr;
}

/*
 * Find the previous checkpoint preceding given WAL location.
 */
void
findLastCheckpoint(const char *datadir, XLogRecPtr forkptr, int tliIndex,
				   XLogRecPtr *lastchkptrec, TimeLineID *lastchkpttli,
				   XLogRecPtr *lastchkptredo, const char *restoreCommand)
{
	/* Walk backwards, starting from the given record */
	XLogRecord *record;
	XLogRecPtr	searchptr;
	XLogReaderState *xlogreader;
	char	   *errormsg;
	XLogPageReadPrivate private;

	/*
	 * The given fork pointer points to the end of the last common record,
	 * which is not necessarily the beginning of the next record, if the
	 * previous record happens to end at a page boundary. Skip over the page
	 * header in that case to find the next record.
	 */
	if (forkptr % XLOG_BLCKSZ == 0)
	{
		if (XLogSegmentOffset(forkptr, WalSegSz) == 0)
			forkptr += SizeOfXLogLongPHD;
		else
			forkptr += SizeOfXLogShortPHD;
	}

	private.datadir = datadir;
	private.tliIndex = tliIndex;
	private.restoreCommand = restoreCommand;
	xlogreader = XLogReaderAllocate(WalSegSz, &SimpleXLogPageRead,
									&private);
	if (xlogreader == NULL)
		pg_fatal("out of memory\n");

	searchptr = forkptr;
	for (;;)
	{
		uint8		info;

		record = XLogReadRecord(xlogreader, searchptr, &errormsg);

		if (record == NULL)
		{
			if (errormsg)
				pg_fatal("could not find previous WAL record at %X/%X: %s\n",
						 (uint32) (searchptr >> 32), (uint32) (searchptr),
						 errormsg);
			else
				pg_fatal("could not find previous WAL record at %X/%X\n",
						 (uint32) (searchptr >> 32), (uint32) (searchptr));
		}

		/*
		 * Check if it is a checkpoint record. This checkpoint record needs to
		 * be the latest checkpoint before WAL forked and not the checkpoint
		 * where the master has been stopped to be rewinded.
		 */
		info = XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK;
		if (searchptr < forkptr &&
			XLogRecGetRmid(xlogreader) == RM_XLOG_ID &&
			(info == XLOG_CHECKPOINT_SHUTDOWN ||
			 info == XLOG_CHECKPOINT_ONLINE))
		{
			CheckPoint	checkPoint;

			memcpy(&checkPoint, XLogRecGetData(xlogreader), sizeof(CheckPoint));
			*lastchkptrec = searchptr;
			*lastchkpttli = checkPoint.ThisTimeLineID;
			*lastchkptredo = checkPoint.redo;
			break;
		}

		/* Walk backwards to previous record. */
		searchptr = record->xl_prev;
	}

	XLogReaderFree(xlogreader);
	if (xlogreadfd != -1)
	{
		close(xlogreadfd);
		xlogreadfd = -1;
	}
}

/* XLogreader callback function, to read a WAL page */
static int
SimpleXLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr,
				   int reqLen, XLogRecPtr targetRecPtr, char *readBuf,
				   TimeLineID *pageTLI)
{
	XLogPageReadPrivate *private = (XLogPageReadPrivate *) xlogreader->private_data;
	uint32		targetPageOff;
	XLogRecPtr	targetSegEnd;
	XLogSegNo	targetSegNo;
	int			r;

	XLByteToSeg(targetPagePtr, targetSegNo, WalSegSz);
	XLogSegNoOffsetToRecPtr(targetSegNo + 1, 0, WalSegSz, targetSegEnd);
	targetPageOff = XLogSegmentOffset(targetPagePtr, WalSegSz);

	/*
	 * See if we need to switch to a new segment because the requested record
	 * is not in the currently open one.
	 */
	if (xlogreadfd >= 0 &&
		!XLByteInSeg(targetPagePtr, xlogreadsegno, WalSegSz))
	{
		close(xlogreadfd);
		xlogreadfd = -1;
	}

	XLByteToSeg(targetPagePtr, xlogreadsegno, WalSegSz);

	if (xlogreadfd < 0)
	{
		char		xlogfname[MAXFNAMELEN];

		/*
		 * Since incomplete segments are copied into next timelines, switch to
		 * the timeline holding the required segment. Assuming this scan can
		 * be done both forward and backward, consider also switching timeline
		 * accordingly.
		 */
		while (private->tliIndex < targetNentries - 1 &&
			   targetHistory[private->tliIndex].end < targetSegEnd)
			private->tliIndex++;
		while (private->tliIndex > 0 &&
			   targetHistory[private->tliIndex].begin >= targetSegEnd)
			private->tliIndex--;

		XLogFileName(xlogfname, targetHistory[private->tliIndex].tli,
					 xlogreadsegno, WalSegSz);

		snprintf(xlogfpath, MAXPGPATH, "%s/" XLOGDIR "/%s", private->datadir, xlogfname);

		xlogreadfd = open(xlogfpath, O_RDONLY | PG_BINARY, 0);

		if (xlogreadfd < 0)
		{
			/*
			 * If we have no restore_command to execute, then exit.
			 */
			if (private->restoreCommand == NULL)
			{
				printf(_("could not open file \"%s\": %s\n"), xlogfpath,
					   strerror(errno));
				return -1;
			}

			/*
			 * Since we have restore_command to execute, then try to retrieve
			 * missing WAL file from the archive.
			 */
			xlogreadfd = RestoreArchivedWAL(private->datadir,
											xlogfname,
											WalSegSz,
											private->restoreCommand);

			if (xlogreadfd < 0)
				return -1;
			else
				pg_log(PG_DEBUG, "using restored from archive version of file \"%s\"\n",
					   xlogfpath);
		}
	}

	/*
	 * At this point, we have the right segment open.
	 */
	Assert(xlogreadfd != -1);

	/* Read the requested page */
	if (lseek(xlogreadfd, (off_t) targetPageOff, SEEK_SET) < 0)
	{
		printf(_("could not seek in file \"%s\": %s\n"), xlogfpath,
			   strerror(errno));
		return -1;
	}


	r = read(xlogreadfd, readBuf, XLOG_BLCKSZ);
	if (r != XLOG_BLCKSZ)
	{
		if (r < 0)
			printf(_("could not read file \"%s\": %s\n"), xlogfpath,
				   strerror(errno));
		else
			printf(_("could not read file \"%s\": read %d of %zu\n"),
				   xlogfpath, r, (Size) XLOG_BLCKSZ);

		return -1;
	}

	Assert(targetSegNo == xlogreadsegno);

	*pageTLI = targetHistory[private->tliIndex].tli;
	return XLOG_BLCKSZ;
}

/*
 * Extract information on which blocks the current record modifies.
 */
static void
extractPageInfo(XLogReaderState *record)
{
	int			block_id;
	RmgrId		rmid = XLogRecGetRmid(record);
	uint8		info = XLogRecGetInfo(record);
	uint8		rminfo = info & ~XLR_INFO_MASK;

	/* Is this a special record type that I recognize? */

	if (rmid == RM_DBASE_ID && rminfo == XLOG_DBASE_CREATE)
	{
		/*
		 * New databases can be safely ignored. It won't be present in the
		 * source system, so it will be deleted. There's one corner-case,
		 * though: if a new, different, database is also created in the source
		 * system, we'll see that the files already exist and not copy them.
		 * That's OK, though; WAL replay of creating the new database, from
		 * the source systems's WAL, will re-copy the new database,
		 * overwriting the database created in the target system.
		 */
	}
	else if (rmid == RM_DBASE_ID && rminfo == XLOG_DBASE_DROP)
	{
		/*
		 * An existing database was dropped. We'll see that the files don't
		 * exist in the target data dir, and copy them in toto from the source
		 * system. No need to do anything special here.
		 */
	}
	else if (rmid == RM_SMGR_ID && rminfo == XLOG_SMGR_CREATE)
	{
		/*
		 * We can safely ignore these. The file will be removed from the
		 * target, if it doesn't exist in source system. If a file with same
		 * name is created in source system, too, there will be WAL records
		 * for all the blocks in it.
		 */
	}
	else if (rmid == RM_SMGR_ID && rminfo == XLOG_SMGR_TRUNCATE)
	{
		/*
		 * We can safely ignore these. When we compare the sizes later on,
		 * we'll notice that they differ, and copy the missing tail from
		 * source system.
		 */
	}
	else if (info & XLR_SPECIAL_REL_UPDATE)
	{
		/*
		 * This record type modifies a relation file in some special way, but
		 * we don't recognize the type. That's bad - we don't know how to
		 * track that change.
		 */
		pg_fatal("WAL record modifies a relation, but record type is not recognized\n"
				 "lsn: %X/%X, rmgr: %s, info: %02X\n",
				 (uint32) (record->ReadRecPtr >> 32), (uint32) (record->ReadRecPtr),
				 RmgrNames[rmid], info);
	}

	for (block_id = 0; block_id <= record->max_block_id; block_id++)
	{
		RelFileNode rnode;
		ForkNumber	forknum;
		BlockNumber blkno;

		if (!XLogRecGetBlockTag(record, block_id, &rnode, &forknum, &blkno))
			continue;

		/* We only care about the main fork; others are copied in toto */
		if (forknum != MAIN_FORKNUM)
			continue;

		process_block_change(forknum, rnode, blkno);
	}
}

/*
 * Attempt to retrieve the specified file from off-line archival storage.
 * If successful return a file descriptor of restored WAL file, else
 * return -1.
 *
 * For fixed-size files, the caller may pass the expected size as an
 * additional crosscheck on successful recovery. If the file size is not
 * known, set expectedSize = 0.
 */
static int
RestoreArchivedWAL(const char *path, const char *xlogfname,
				   off_t expectedSize, const char *restoreCommand)
{
	char		xlogpath[MAXPGPATH],
				xlogRestoreCmd[MAXPGPATH],
			   *dp,
			   *endp;
	const char *sp;
	int			rc,
				xlogfd;
	struct stat stat_buf;

	snprintf(xlogpath, MAXPGPATH, "%s/" XLOGDIR "/%s", path, xlogfname);

	/*
	 * Construct the command to be executed.
	 */
	dp = xlogRestoreCmd;
	endp = xlogRestoreCmd + MAXPGPATH - 1;
	*endp = '\0';

	for (sp = restoreCommand; *sp; sp++)
	{
		if (*sp == '%')
		{
			switch (sp[1])
			{
				case 'p':
					/* %p: relative path of target file */
					sp++;
					StrNCpy(dp, xlogpath, endp - dp);
					make_native_path(dp);
					dp += strlen(dp);
					break;
				case 'f':
					/* %f: filename of desired file */
					sp++;
					StrNCpy(dp, xlogfname, endp - dp);
					dp += strlen(dp);
					break;
				case 'r':
					/* %r: filename of last restartpoint */
					pg_fatal("restore_command with %%r cannot be used with pg_rewind.\n");
					break;
				case '%':
					/* convert %% to a single % */
					sp++;
					if (dp < endp)
						*dp++ = *sp;
					break;
				default:
					/* otherwise treat the % as not special */
					if (dp < endp)
						*dp++ = *sp;
					break;
			}
		}
		else
		{
			if (dp < endp)
				*dp++ = *sp;
		}
	}
	*dp = '\0';

	/*
	 * Execute restore_command, which should copy
	 * the missing WAL file from archival storage.
	 */
	rc = system(xlogRestoreCmd);

	if (rc == 0)
	{
		/*
		 * Command apparently succeeded, but let's make sure the file is
		 * really there now and has the correct size.
		 */
		if (stat(xlogpath, &stat_buf) == 0)
		{
			if (expectedSize > 0 && stat_buf.st_size != expectedSize)
			{
				printf(_("archive file \"%s\" has wrong size: %lu instead of %lu, %s"),
						xlogfname, (unsigned long) stat_buf.st_size,
						(unsigned long) expectedSize, strerror(errno));
			}
			else
			{
				xlogfd = open(xlogpath, O_RDONLY | PG_BINARY, 0);

				if (xlogfd < 0)
					printf(_("could not open restored from archive file \"%s\": %s\n"),
							xlogpath, strerror(errno));
				else
					return xlogfd;
			}
		}
		else
		{
			/* Stat failed */
			printf(_("could not stat file \"%s\": %s"),
					xlogpath, strerror(errno));
		}
	}

	/*
	 * If the failure was due to any sort of signal, then it will be
	 * misleading to return message 'could not restore file...' and
	 * propagate result to the upper levels. We should exit right now.
	 */
	if (wait_result_is_any_signal(rc, false))
		pg_fatal("restore_command failed due to the signal: %s\n",
				 wait_result_to_str(rc));

	printf(_("could not restore file \"%s\" from archive\n"),
			xlogfname);

	return -1;
}
