/*
 * xlog_internal.h
 *
 * PostgreSQL write-ahead log internal declarations
 *
 * NOTE: this file is intended to contain declarations useful for
 * manipulating the XLOG files directly, but it is not supposed to be
 * needed by rmgr routines (redo support for individual record types).
 * So the XLogRecord typedef and associated stuff appear in xlogrecord.h.
 *
 * Note: This file must be includable in both frontend and backend contexts,
 * to allow stand-alone tools like pg_receivewal to deal with WAL files.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/xlog_internal.h
 */
#ifndef XLOG_INTERNAL_H
#define XLOG_INTERNAL_H

#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "access/xlogfilepaths.h"
#include "datatype/timestamp.h"
#include "lib/stringinfo.h"
#include "pgtime.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"


/*
 * Each page of XLOG file has a header like this:
 */
#define XLOG_PAGE_MAGIC 0xD116	/* can be used as WAL version indicator */

typedef struct XLogPageHeaderData
{
	uint16		xlp_magic;		/* magic value for correctness checks */
	uint16		xlp_info;		/* flag bits, see below */
	TimeLineID	xlp_tli;		/* TimeLineID of first record on page */
	XLogRecPtr	xlp_pageaddr;	/* XLOG address of this page */

	/*
	 * When there is not enough space on current page for whole record, we
	 * continue on the next page.  xlp_rem_len is the number of bytes
	 * remaining from a previous page; it tracks xl_tot_len in the initial
	 * header.  Note that the continuation data isn't necessarily aligned.
	 */
	uint32		xlp_rem_len;	/* total len of remaining data for record */
} XLogPageHeaderData;

#define SizeOfXLogShortPHD	MAXALIGN(sizeof(XLogPageHeaderData))

typedef XLogPageHeaderData *XLogPageHeader;

/*
 * When the XLP_LONG_HEADER flag is set, we store additional fields in the
 * page header.  (This is ordinarily done just in the first page of an
 * XLOG file.)	The additional fields serve to identify the file accurately.
 */
typedef struct XLogLongPageHeaderData
{
	XLogPageHeaderData std;		/* standard header fields */
	uint64		xlp_sysid;		/* system identifier from pg_control */
	uint32		xlp_seg_size;	/* just as a cross-check */
	uint32		xlp_xlog_blcksz;	/* just as a cross-check */
} XLogLongPageHeaderData;

#define SizeOfXLogLongPHD	MAXALIGN(sizeof(XLogLongPageHeaderData))

typedef XLogLongPageHeaderData *XLogLongPageHeader;

/* When record crosses page boundary, set this flag in new page's header */
#define XLP_FIRST_IS_CONTRECORD		0x0001
/* This flag indicates a "long" page header */
#define XLP_LONG_HEADER				0x0002
/* This flag indicates backup blocks starting in this page are optional */
#define XLP_BKP_REMOVABLE			0x0004
/* Replaces a missing contrecord; see CreateOverwriteContrecordRecord */
#define XLP_FIRST_IS_OVERWRITE_CONTRECORD 0x0008
/* All defined flag bits in xlp_info (used for validity checking of header) */
#define XLP_ALL_FLAGS				0x000F

#define XLogPageHeaderSize(hdr)		\
	(((hdr)->xlp_info & XLP_LONG_HEADER) ? SizeOfXLogLongPHD : SizeOfXLogShortPHD)

/* Check if an XLogRecPtr value is in a plausible range */
#define XRecOffIsValid(xlrp) \
		((xlrp) % XLOG_BLCKSZ >= SizeOfXLogShortPHD)


/*
 * Information logged when we detect a change in one of the parameters
 * important for Hot Standby.
 */
typedef struct xl_parameter_change
{
	int			MaxConnections;
	int			max_worker_processes;
	int			max_wal_senders;
	int			max_prepared_xacts;
	int			max_locks_per_xact;
	int			wal_level;
	bool		wal_log_hints;
	bool		track_commit_timestamp;
} xl_parameter_change;

/* logs restore point */
typedef struct xl_restore_point
{
	TimestampTz rp_time;
	char		rp_name[MAXFNAMELEN];
} xl_restore_point;

/* Overwrite of prior contrecord */
typedef struct xl_overwrite_contrecord
{
	XLogRecPtr	overwritten_lsn;
	TimestampTz overwrite_time;
} xl_overwrite_contrecord;

/* End of recovery mark, when we don't do an END_OF_RECOVERY checkpoint */
typedef struct xl_end_of_recovery
{
	TimestampTz end_time;
	TimeLineID	ThisTimeLineID; /* new TLI */
	TimeLineID	PrevTimeLineID; /* previous TLI we forked off from */
	int			wal_level;
} xl_end_of_recovery;

/*
 * The functions in xloginsert.c construct a chain of XLogRecData structs
 * to represent the final WAL record.
 */
typedef struct XLogRecData
{
	struct XLogRecData *next;	/* next struct in chain, or NULL */
	const char *data;			/* start of rmgr data to include */
	uint32		len;			/* length of rmgr data to include */
} XLogRecData;

/*
 * Recovery target action.
 */
typedef enum
{
	RECOVERY_TARGET_ACTION_PAUSE,
	RECOVERY_TARGET_ACTION_PROMOTE,
	RECOVERY_TARGET_ACTION_SHUTDOWN,
}			RecoveryTargetAction;

struct LogicalDecodingContext;
struct XLogRecordBuffer;

/*
 * Method table for resource managers.
 *
 * This struct must be kept in sync with the PG_RMGR definition in
 * rmgr.c.
 *
 * rm_identify must return a name for the record based on xl_info (without
 * reference to the rmid). For example, XLOG_BTREE_VACUUM would be named
 * "VACUUM". rm_desc can then be called to obtain additional detail for the
 * record, if available (e.g. the last block).
 *
 * rm_mask takes as input a page modified by the resource manager and masks
 * out bits that shouldn't be flagged by wal_consistency_checking.
 *
 * RmgrTable[] is indexed by RmgrId values (see rmgrlist.h). If rm_name is
 * NULL, the corresponding RmgrTable entry is considered invalid.
 */
typedef struct RmgrData
{
	const char *rm_name;
	void		(*rm_redo) (XLogReaderState *record);
	void		(*rm_desc) (StringInfo buf, XLogReaderState *record);
	const char *(*rm_identify) (uint8 info);
	void		(*rm_startup) (void);
	void		(*rm_cleanup) (void);
	void		(*rm_mask) (char *pagedata, BlockNumber blkno);
	void		(*rm_decode) (struct LogicalDecodingContext *ctx,
							  struct XLogRecordBuffer *buf);
} RmgrData;

extern PGDLLIMPORT RmgrData RmgrTable[];
extern void RmgrStartup(void);
extern void RmgrCleanup(void);
extern void RmgrNotFound(RmgrId rmid);
extern void RegisterCustomRmgr(RmgrId rmid, const RmgrData *rmgr);

#ifndef FRONTEND
static inline bool
RmgrIdExists(RmgrId rmid)
{
	return RmgrTable[rmid].rm_name != NULL;
}

static inline RmgrData
GetRmgr(RmgrId rmid)
{
	if (unlikely(!RmgrIdExists(rmid)))
		RmgrNotFound(rmid);
	return RmgrTable[rmid];
}
#endif

/*
 * Exported to support xlog switching from checkpointer
 */
extern pg_time_t GetLastSegSwitchData(XLogRecPtr *lastSwitchLSN);
extern XLogRecPtr RequestXLogSwitch(bool mark_unimportant);

extern void GetOldestRestartPoint(XLogRecPtr *oldrecptr, TimeLineID *oldtli);

extern void XLogRecGetBlockRefInfo(XLogReaderState *record, bool pretty,
								   bool detailed_format, StringInfo buf,
								   uint32 *fpi_len);

/*
 * Exported for the functions in timeline.c and xlogarchive.c.  Only valid
 * in the startup process.
 */
extern PGDLLIMPORT bool ArchiveRecoveryRequested;
extern PGDLLIMPORT bool InArchiveRecovery;
extern PGDLLIMPORT bool StandbyMode;
extern PGDLLIMPORT char *recoveryRestoreCommand;

#endif							/* XLOG_INTERNAL_H */
