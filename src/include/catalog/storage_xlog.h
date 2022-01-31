/*-------------------------------------------------------------------------
 *
 * storage_xlog.h
 *	  prototypes for XLog support for backend/catalog/storage.c
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/storage_xlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STORAGE_XLOG_H
#define STORAGE_XLOG_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "storage/block.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"

/*
 * Declarations for smgr-related XLOG records
 *
 * Note: we log file creation, truncation and buffer persistence change here,
 * but logging of deletion actions is handled mainly by xact.c, because it is
 * part of transaction commit in most cases.  However, there's a case where
 * init forks are deleted outside control of transaction.
 */

/* XLOG gives us high 4 bits */
#define XLOG_SMGR_CREATE	0x10
#define XLOG_SMGR_TRUNCATE	0x20
#define XLOG_SMGR_UNLINK	0x30
#define XLOG_SMGR_MARK		0x40
#define XLOG_SMGR_BUFPERSISTENCE	0x50

typedef struct xl_smgr_create
{
	RelFileNode rnode;
	ForkNumber	forkNum;
} xl_smgr_create;

typedef struct xl_smgr_unlink
{
	RelFileNode rnode;
	ForkNumber	forkNum;
} xl_smgr_unlink;

typedef enum smgr_mark_action
{
	XLOG_SMGR_MARK_CREATE = 'c',
	XLOG_SMGR_MARK_UNLINK = 'u'
} smgr_mark_action;

typedef struct xl_smgr_mark
{
	RelFileNode 	rnode;
	ForkNumber		forkNum;
	StorageMarks	mark;
	smgr_mark_action action;
} xl_smgr_mark;

typedef struct xl_smgr_bufpersistence
{
	RelFileNode rnode;
	bool		persistence;
} xl_smgr_bufpersistence;

/* flags for xl_smgr_truncate */
#define SMGR_TRUNCATE_HEAP		0x0001
#define SMGR_TRUNCATE_VM		0x0002
#define SMGR_TRUNCATE_FSM		0x0004
#define SMGR_TRUNCATE_ALL		\
	(SMGR_TRUNCATE_HEAP|SMGR_TRUNCATE_VM|SMGR_TRUNCATE_FSM)

typedef struct xl_smgr_truncate
{
	BlockNumber blkno;
	RelFileNode rnode;
	int			flags;
} xl_smgr_truncate;

extern void log_smgrcreate(const RelFileNode *rnode, ForkNumber forkNum);
extern void log_smgrunlink(const RelFileNode *rnode, ForkNumber forkNum);
extern void log_smgrcreatemark(const RelFileNode *rnode, ForkNumber forkNum,
							   StorageMarks mark);
extern void log_smgrunlinkmark(const RelFileNode *rnode, ForkNumber forkNum,
							   StorageMarks mark);
extern void log_smgrbufpersistence(const RelFileNode *rnode, bool persistence);

extern void smgr_redo(XLogReaderState *record);
extern void smgr_desc(StringInfo buf, XLogReaderState *record);
extern const char *smgr_identify(uint8 info);

#endif							/* STORAGE_XLOG_H */
