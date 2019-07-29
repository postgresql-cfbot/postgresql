/*-------------------------------------------------------------------------
 *
 * xlogreader.h
 *		Definitions for the generic XLog reading facility
 *
 * Portions Copyright (c) 2013-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/xlogreader.h
 *
 * NOTES
 *		See the definition of the XLogReaderState struct for instructions on
 *		how to use the XLogReader infrastructure.
 *
 *		The basic idea is to allocate an XLogReaderState via
 *		XLogReaderAllocate(), and call XLogReadRecord() until it returns NULL.
 *
 *		After reading a record with XLogReadRecord(), it's decomposed into
 *		the per-block and main data parts, and the parts can be accessed
 *		with the XLogRec* macros and functions. You can also decode a
 *		record that's already constructed in memory, without reading from
 *		disk, by calling the DecodeXLogRecord() function.
 *-------------------------------------------------------------------------
 */
#ifndef XLOGREADER_H
#define XLOGREADER_H

#ifndef FRONTEND
#include "access/transam.h"
#endif

#include "access/xlogrecord.h"

typedef struct XLogReaderState XLogReaderState;

typedef struct
{
	/* Is this block ref in use? */
	bool		in_use;

	/* Identify the block this refers to */
	RelFileNode rnode;
	ForkNumber	forknum;
	BlockNumber blkno;

	/* copy of the fork_flags field from the XLogRecordBlockHeader */
	uint8		flags;

	/* Information on full-page image, if any */
	bool		has_image;		/* has image, even for consistency checking */
	bool		apply_image;	/* has image that should be restored */
	char	   *bkp_image;
	uint16		hole_offset;
	uint16		hole_length;
	uint16		bimg_len;
	uint8		bimg_info;

	/* Buffer holding the rmgr-specific data associated with this block */
	bool		has_data;
	char	   *data;
	uint16		data_len;
	uint16		data_bufsz;
} DecodedBkpBlock;

/* Return code from XLogReadRecord */
typedef enum XLogReadRecordResult
{
	XLREAD_SUCCESS,		/* record is successfully read */
	XLREAD_NEED_DATA,	/* need more data. see XLogReadRecord. */
	XLREAD_FAIL			/* failed during reading a record */
} XLogReadRecordResult;

/* internal state of XLogNeedData() */
typedef enum xlnd_stateid
{
	XLND_STATE_INIT,
	XLND_STATE_SEGHEADER,
	XLND_STATE_PAGEHEADER,
	XLND_STATE_PAGELONGHEADER
} xlnd_stateid;

/* internal state of XLogReadRecord() */
typedef enum xlread_stateid
{
	XLREAD_STATE_INIT,
	XLREAD_STATE_PAGE,
	XLREAD_STATE_CONTPAGE,
	XLREAD_STATE_CONTPAGE_HEADER,
	XLREAD_STATE_CONTRECORD,
	XLREAD_STATE_RECORD
} xlread_stateid;

struct XLogReaderState
{
	/* ----------------------------------------
	 * Public parameters
	 * ----------------------------------------
	 */

	/*
	 * Segment size of the to-be-parsed data (mandatory).
	 */
	int			wal_segment_size;

	/*
	 * System identifier of the xlog files we're about to read.  Set to zero
	 * (the default value) if unknown or unimportant.
	 */
	uint64		system_identifier;

	/*
	 * Start and end point of last record read.  EndRecPtr is also used as the
	 * position to read next, if XLogReadRecord receives an invalid recptr.
	 */
	XLogRecPtr	ReadRecPtr;		/* start of last record read */
	XLogRecPtr	EndRecPtr;		/* end+1 of last record read */


	/* ----------------------------------------
	 * Communication with page reader
	 * readBuf is XLOG_BLCKSZ bytes, valid up to at least readLen bytes.
	 *  ----------------------------------------
	 */
	/* parameters to page reader */
	XLogRecPtr	loadPagePtr;	/* Pointer to the page  */
	int			loadLen;		/* wanted length in bytes */
	char	   *readBuf;		/* buffer to store data */
	XLogRecPtr	currRecPtr;		/* beginning of the WAL record being read */

	/* return from page reader */
	int32		readLen;		/* bytes actually read, must be at least
								 * loadLen. -1 on error. */
	TimeLineID	readPageTLI;	/* TLI for data currently in readBuf */

	/* ----------------------------------------
	 * Decoded representation of current record
	 *
	 * Use XLogRecGet* functions to investigate the record; these fields
	 * should not be accessed directly.
	 * ----------------------------------------
	 */
	XLogRecord *decoded_record; /* currently decoded record */

	char	   *main_data;		/* record's main data portion */
	uint32		main_data_len;	/* main data portion's length */
	uint32		main_data_bufsz;	/* allocated size of the buffer */

	RepOriginId record_origin;

	/* information about blocks referenced by the record. */
	DecodedBkpBlock blocks[XLR_MAX_BLOCK_ID + 1];

	int			max_block_id;	/* highest block_id in use (-1 if none) */

	/* ----------------------------------------
	 * private/internal state
	 * ----------------------------------------
	 */

	/* last read segment and segment offset for data currently in readBuf */
	XLogSegNo	readSegNo;
	uint32		readOff;

	/*
	 * beginning of prior page read, and its TLI.  Doesn't necessarily
	 * correspond to what's in readBuf; used for timeline sanity checks.
	 */
	XLogRecPtr	latestPagePtr;
	TimeLineID	latestPageTLI;

	/* timeline to read it from, 0 if a lookup is required */
	TimeLineID	currTLI;

	/*
	 * Safe point to read to in currTLI if current TLI is historical
	 * (tliSwitchPoint) or InvalidXLogRecPtr if on current timeline.
	 *
	 * Actually set to the start of the segment containing the timeline switch
	 * that ends currTLI's validity, not the LSN of the switch its self, since
	 * we can't assume the old segment will be present.
	 */
	XLogRecPtr	currTLIValidUntil;

	/*
	 * If currTLI is not the most recent known timeline, the next timeline to
	 * read from when currTLIValidUntil is reached.
	 */
	TimeLineID	nextTLI;

	/*
	 * Buffer for current ReadRecord result (expandable), used when a record
	 * crosses a page boundary.
	 */
	char	   *readRecordBuf;
	uint32		readRecordBufSize;

	/* Buffer to hold error message */
	char	   *errormsg_buf;
};

/* Get a new XLogReader */
extern XLogReaderState *XLogReaderAllocate(int wal_segment_size);

/* Free an XLogReader */
extern void XLogReaderFree(XLogReaderState *state);

/* Read the next XLog record. Returns NULL on end-of-WAL or failure */
extern XLogReadRecordResult XLogReadRecord(XLogReaderState *state,
										   XLogRecPtr recptr,
										   XLogRecord **record,
										   char **errormsg);

/* Validate a page */
extern bool XLogReaderValidatePageHeader(XLogReaderState *state,
										 XLogRecPtr recptr, char *phdr);

/* Invalidate read state */
extern void XLogReaderInvalReadState(XLogReaderState *state);

#ifdef FRONTEND
/* Function type definition for the read_page callback */
typedef void (*XLogFindNextRecordCB) (XLogReaderState *xlogreader,
									  void *private);
extern XLogRecPtr XLogFindNextRecord(XLogReaderState *state, XLogRecPtr RecPtr,
									 XLogFindNextRecordCB read_page, void *private);
#endif							/* FRONTEND */

/* Functions for decoding an XLogRecord */

extern bool DecodeXLogRecord(XLogReaderState *state, XLogRecord *record,
							 char **errmsg);

#define XLogRecGetTotalLen(decoder) ((decoder)->decoded_record->xl_tot_len)
#define XLogRecGetPrev(decoder) ((decoder)->decoded_record->xl_prev)
#define XLogRecGetInfo(decoder) ((decoder)->decoded_record->xl_info)
#define XLogRecGetRmid(decoder) ((decoder)->decoded_record->xl_rmid)
#define XLogRecGetXid(decoder) ((decoder)->decoded_record->xl_xid)
#define XLogRecGetOrigin(decoder) ((decoder)->record_origin)
#define XLogRecGetData(decoder) ((decoder)->main_data)
#define XLogRecGetDataLen(decoder) ((decoder)->main_data_len)
#define XLogRecHasAnyBlockRefs(decoder) ((decoder)->max_block_id >= 0)
#define XLogRecHasBlockRef(decoder, block_id) \
	((decoder)->blocks[block_id].in_use)
#define XLogRecHasBlockImage(decoder, block_id) \
	((decoder)->blocks[block_id].has_image)
#define XLogRecBlockImageApply(decoder, block_id) \
	((decoder)->blocks[block_id].apply_image)

#ifndef FRONTEND
extern FullTransactionId XLogRecGetFullXid(XLogReaderState *record);
#endif

extern bool RestoreBlockImage(XLogReaderState *recoder, uint8 block_id, char *dst);
extern char *XLogRecGetBlockData(XLogReaderState *record, uint8 block_id, Size *len);
extern bool XLogRecGetBlockTag(XLogReaderState *record, uint8 block_id,
							   RelFileNode *rnode, ForkNumber *forknum,
							   BlockNumber *blknum);

#endif							/* XLOGREADER_H */
