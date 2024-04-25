/*-------------------------------------------------------------------------
 *
 * seqlocalam.h
 *	  Local sequence access method.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/seqlocalam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SEQLOCALAM_H
#define SEQLOCALAM_H

#include "access/xlogreader.h"
#include "storage/relfilelocator.h"

/* XLOG stuff */
#define XLOG_SEQ_LOCAL_LOG			0x00

typedef struct xl_seq_local_rec
{
	RelFileLocator locator;
	/* SEQUENCE TUPLE DATA FOLLOWS AT THE END */
} xl_seq_local_rec;

/*
 * The "special area" of a local sequence's buffer page looks like this.
 */
#define SEQ_LOCAL_MAGIC	  0x1717

typedef struct seq_local_magic
{
	uint32		magic;
} seq_local_magic;

extern void seq_local_redo(XLogReaderState *record);
extern void seq_local_desc(StringInfo buf, XLogReaderState *record);
extern const char *seq_local_identify(uint8 info);
extern void seq_local_mask(char *page, BlockNumber blkno);

#endif							/* SEQLOCALAM_H */
