/*-------------------------------------------------------------------------
 *
 * localam.h
 *	  Local sequence access method.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/localam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOCALAM_H
#define LOCALAM_H

#include "access/xlogreader.h"
#include "storage/relfilelocator.h"

/* XLOG stuff */
#define XLOG_LOCAL_SEQ_LOG			0x00

typedef struct xl_local_seq_rec
{
	RelFileLocator locator;
	/* SEQUENCE TUPLE DATA FOLLOWS AT THE END */
} xl_local_seq_rec;

extern void local_seq_redo(XLogReaderState *record);
extern void local_seq_desc(StringInfo buf, XLogReaderState *record);
extern const char *local_seq_identify(uint8 info);
extern void local_seq_mask(char *page, BlockNumber blkno);

#endif							/* LOCALAM_H */
