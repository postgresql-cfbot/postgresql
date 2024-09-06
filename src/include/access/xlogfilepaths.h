/*
 * xlogfilepaths.h
 *
 * File name definitions and handling macros for PostgreSQL write-ahead logs.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/xlogfilepaths.h
 */
#ifndef XLOG_FILEPATHS_H
#define XLOG_FILEPATHS_H

#include "access/xlogdefs.h"

/*
 * The XLog directory and files (relative to $PGDATA)
 */
#define XLOGDIR					"pg_wal"

/* control files */
#define XLOG_CONTROL_FILE		"global/pg_control"
#define BACKUP_LABEL_FILE		"backup_label"
#define BACKUP_LABEL_OLD		"backup_label.old"

/* tablespace map */
#define TABLESPACE_MAP			"tablespace_map"
#define TABLESPACE_MAP_OLD		"tablespace_map.old"

/* files to signal run mode to standby */
#define RECOVERY_SIGNAL_FILE	"recovery.signal"
#define STANDBY_SIGNAL_FILE		"standby.signal"

/* files to signal promotion to primary */
#define PROMOTE_SIGNAL_FILE		"promote"

/* wal_segment_size can range from 1MB to 1GB */
#define WalSegMinSize 1024 * 1024
#define WalSegMaxSize 1024 * 1024 * 1024

/* default number of min and max wal segments */
#define DEFAULT_MIN_WAL_SEGS 5
#define DEFAULT_MAX_WAL_SEGS 64


/*
 * These macros encapsulate knowledge about the exact layout of XLog file
 * names, timeline history file names, and archive-status file names.
 */
#define MAXFNAMELEN		64

/* Length of XLog file name */
#define XLOG_FNAME_LEN	   24

/* check that the given size is a valid wal_segment_size */
#define IsPowerOf2(x) (x > 0 && ((x) & ((x)-1)) == 0)
#define IsValidWalSegSize(size) \
	 (IsPowerOf2(size) && \
	 ((size) >= WalSegMinSize && (size) <= WalSegMaxSize))

/* Number of segments in a logical XLOG file */
#define XLogSegmentsPerXLogId(wal_segsz_bytes)	\
	(UINT64CONST(0x100000000) / (wal_segsz_bytes))

/*
 * Compute an XLogRecPtr from a segment number and offset.
 */
#define XLogSegNoOffsetToRecPtr(segno, offset, wal_segsz_bytes, dest) \
		(dest) = (segno) * (wal_segsz_bytes) + (offset)
/*
 * Compute a segment number from an XLogRecPtr.
 *
 * For XLByteToSeg, do the computation at face value.  For XLByteToPrevSeg,
 * a boundary byte is taken to be in the previous segment.  This is suitable
 * for deciding which segment to write given a pointer to a record end,
 * for example.
 */
#define XLByteToSeg(xlrp, logSegNo, wal_segsz_bytes) \
	logSegNo = (xlrp) / (wal_segsz_bytes)

#define XLByteToPrevSeg(xlrp, logSegNo, wal_segsz_bytes) \
	logSegNo = ((xlrp) - 1) / (wal_segsz_bytes)

/* Compute the in-segment offset from an XLogRecPtr. */
#define XLogSegmentOffset(xlogptr, wal_segsz_bytes)	\
	((xlogptr) & ((wal_segsz_bytes) - 1))

/*
 * Convert values of GUCs measured in megabytes to equiv. segment count.
 * Rounds down.
 */
#define XLogMBVarToSegs(mbvar, wal_segsz_bytes) \
	((mbvar) / ((wal_segsz_bytes) / (1024 * 1024)))

/*
 * Is an XLogRecPtr within a particular XLOG segment?
 *
 * For XLByteInSeg, do the computation at face value.  For XLByteInPrevSeg,
 * a boundary byte is taken to be in the previous segment.
 */
#define XLByteInSeg(xlrp, logSegNo, wal_segsz_bytes) \
	(((xlrp) / (wal_segsz_bytes)) == (logSegNo))

#define XLByteInPrevSeg(xlrp, logSegNo, wal_segsz_bytes) \
	((((xlrp) - 1) / (wal_segsz_bytes)) == (logSegNo))

/*
 * XLOG file name handling functions
 */
static inline void
StatusFilePath(char *path, const char *xlog, const char *suffix)
{
	snprintf(path, MAXPGPATH, XLOGDIR "/archive_status/%s%s", xlog, suffix);
}

static inline bool
IsTLHistoryFileName(const char *fname)
{
	return (strlen(fname) == 8 + strlen(".history") &&
			strspn(fname, "0123456789ABCDEF") == 8 &&
			strcmp(fname + 8, ".history") == 0);
}

static inline void
XLogFromFileName(const char *fname, TimeLineID *tli, XLogSegNo *logSegNo, int wal_segsz_bytes)
{
	uint32		log;
	uint32		seg;

	sscanf(fname, "%08X%08X%08X", tli, &log, &seg);
	*logSegNo = (uint64) log * XLogSegmentsPerXLogId(wal_segsz_bytes) + seg;
}

static inline void
XLogFilePath(char *path, TimeLineID tli, XLogSegNo logSegNo, int wal_segsz_bytes)
{
	snprintf(path, MAXPGPATH, XLOGDIR "/%08X%08X%08X", tli,
			 (uint32) (logSegNo / XLogSegmentsPerXLogId(wal_segsz_bytes)),
			 (uint32) (logSegNo % XLogSegmentsPerXLogId(wal_segsz_bytes)));
}

/*
 * Generate a WAL segment file name.  Do not use this function in a helper
 * function allocating the result generated.
 */
static inline void
XLogFileName(char *fname, TimeLineID tli, XLogSegNo logSegNo, int wal_segsz_bytes)
{
	snprintf(fname, MAXFNAMELEN, "%08X%08X%08X", tli,
			 (uint32) (logSegNo / XLogSegmentsPerXLogId(wal_segsz_bytes)),
			 (uint32) (logSegNo % XLogSegmentsPerXLogId(wal_segsz_bytes)));
}

static inline void
XLogFileNameById(char *fname, TimeLineID tli, uint32 log, uint32 seg)
{
	snprintf(fname, MAXFNAMELEN, "%08X%08X%08X", tli, log, seg);
}

static inline bool
IsXLogFileName(const char *fname)
{
	return (strlen(fname) == XLOG_FNAME_LEN && \
			strspn(fname, "0123456789ABCDEF") == XLOG_FNAME_LEN);
}

/*
 * XLOG segment with .partial suffix.  Used by pg_receivewal and at end of
 * archive recovery, when we want to archive a WAL segment but it might not
 * be complete yet.
 */
static inline bool
IsPartialXLogFileName(const char *fname)
{
	return (strlen(fname) == XLOG_FNAME_LEN + strlen(".partial") &&
			strspn(fname, "0123456789ABCDEF") == XLOG_FNAME_LEN &&
			strcmp(fname + XLOG_FNAME_LEN, ".partial") == 0);
}

static inline void
TLHistoryFileName(char *fname, TimeLineID tli)
{
	snprintf(fname, MAXFNAMELEN, "%08X.history", tli);
}

static inline void
TLHistoryFilePath(char *path, TimeLineID tli)
{
	snprintf(path, MAXPGPATH, XLOGDIR "/%08X.history", tli);
}

static inline void
BackupHistoryFileName(char *fname, TimeLineID tli, XLogSegNo logSegNo, XLogRecPtr startpoint, int wal_segsz_bytes)
{
	snprintf(fname, MAXFNAMELEN, "%08X%08X%08X.%08X.backup", tli,
			 (uint32) (logSegNo / XLogSegmentsPerXLogId(wal_segsz_bytes)),
			 (uint32) (logSegNo % XLogSegmentsPerXLogId(wal_segsz_bytes)),
			 (uint32) (XLogSegmentOffset(startpoint, wal_segsz_bytes)));
}

static inline bool
IsBackupHistoryFileName(const char *fname)
{
	return (strlen(fname) > XLOG_FNAME_LEN &&
			strspn(fname, "0123456789ABCDEF") == XLOG_FNAME_LEN &&
			strcmp(fname + strlen(fname) - strlen(".backup"), ".backup") == 0);
}

static inline void
BackupHistoryFilePath(char *path, TimeLineID tli, XLogSegNo logSegNo, XLogRecPtr startpoint, int wal_segsz_bytes)
{
	snprintf(path, MAXPGPATH, XLOGDIR "/%08X%08X%08X.%08X.backup", tli,
			 (uint32) (logSegNo / XLogSegmentsPerXLogId(wal_segsz_bytes)),
			 (uint32) (logSegNo % XLogSegmentsPerXLogId(wal_segsz_bytes)),
			 (uint32) (XLogSegmentOffset((startpoint), wal_segsz_bytes)));
}


#endif							/* XLOG_FILEPATHS_H */
