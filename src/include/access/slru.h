/*-------------------------------------------------------------------------
 *
 * slru.h
 *		Buffering for transaction status logfiles
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/slru.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SLRU_H
#define SLRU_H

#include "access/xlogdefs.h"
#include "catalog/pg_tablespace_d.h"
#include "storage/buf.h"
#include "storage/lwlock.h"
#include "storage/smgr.h"
#include "storage/sync.h"

/* Pseudo database ID used for SLRU data. */
#define SLRU_DB_ID 9

/* Pseudo relation IDs used by each cache. */
#define SLRU_CLOG_REL_ID 0
#define SLRU_MULTIXACT_OFFSET_REL_ID 1
#define SLRU_MULTIXACT_MEMBER_REL_ID 2
#define SLRU_SUBTRANS_REL_ID 3
#define SLRU_SERIAL_REL_ID 4
#define SLRU_COMMITTS_REL_ID 5
#define SLRU_NOTIFY_REL_ID 6
#define SLRU_NUM_RELS 7

typedef bool (*SlruPagePrecedesFunction) (int, int);

static inline RelFileNode
SlruRelFileNode(Oid relNode)
{
	RelFileNode rnode = {DEFAULTTABLESPACE_OID, SLRU_DB_ID, relNode};
	return rnode;
}


/*
 * Define SLRU segment size.  A page is the same BLCKSZ as is used everywhere
 * else in Postgres.  The segment size can be chosen somewhat arbitrarily;
 * we make it 32 pages by default, or 256Kb, i.e. 1M transactions for CLOG
 * or 64K transactions for SUBTRANS.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * page numbering also wraps around at 0xFFFFFFFF/xxxx_XACTS_PER_PAGE (where
 * xxxx is CLOG or SUBTRANS, respectively), and segment numbering at
 * 0xFFFFFFFF/xxxx_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need
 * take no explicit notice of that fact in slru.c, except when comparing
 * segment and page numbers in SimpleLruTruncate (see PagePrecedes()).
 */
#define SLRU_PAGES_PER_SEGMENT	32

#ifdef USE_ASSERT_CHECKING
extern void SlruPagePrecedesUnitTests(SlruPagePrecedesFunction PagePrecedes,
									  int per_page);
#else
#define SlruPagePrecedesUnitTests(ctl, per_page) do {} while (0)
#endif
extern void SimpleLruTruncate(Oid rel_id, SlruPagePrecedesFunction PagePrecedes,
							  int cutoffPage);
extern bool SimpleLruDoesPhysicalPageExist(Oid rel_id, int pageno);

typedef bool (*SlruScanCallback) (Oid rel_id,
								  SlruPagePrecedesFunction PagePrecedes,
								  char *filename, int segpage,
								  void *data);
extern bool SlruScanDirectory(Oid rel_id, SlruPagePrecedesFunction PagePrecedes,
							  SlruScanCallback callback, void *data);
extern void SlruDeleteSegment(Oid rel_id, int segno);

/* SlruScanDirectory public callbacks */
extern bool SlruScanDirCbReportPresence(Oid rel_id,
										SlruPagePrecedesFunction PagePrecedes,
										char *filename,
										int segpage, void *data);
extern bool SlruScanDirCbDeleteAll(Oid rel_id, SlruPagePrecedesFunction PagePrecedes,
								   char *filename, int segpage,
								   void *data);

extern void CheckPointSLRU(void);

/* Buffer access */
extern Buffer ReadSlruBuffer(Oid rel_id, int pageno);
extern Buffer ZeroSlruBuffer(Oid rel_id, int pageno);

/* Interfaces use by stats view */
extern Oid SlruRelIdByName(const char *name);
extern const char *SlruName(Oid rel_id);

/* Sync callback */
extern int	slrusyncfiletag(const FileTag *ftag, char *path);

/* SMGR callbacks */
extern void slruopen(SMgrRelation reln);
extern void slruclose(SMgrRelation reln, ForkNumber forknum);
extern void slruread(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, char *buffer);
extern void slruwrite(SMgrRelation reln, ForkNumber forknum,
					  BlockNumber blocknum, char *buffer, bool skipFsync);
extern void slruwriteback(SMgrRelation reln, ForkNumber forknum,
						  BlockNumber blocknum, BlockNumber nblocks);

#endif							/* SLRU_H */
