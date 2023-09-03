/*-------------------------------------------------------------------------
 *
 * nrel.h
 *		Buffering for transaction status logfiles
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/nrel.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NREL_H
#define NREL_H

#include "access/xlogdefs.h"
#include "catalog/pg_tablespace_d.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "storage/lwlock.h"
#include "storage/smgr.h"
#include "storage/sync.h"

/* Pseudo database ID used for NREL data. */
#define NREL_DB_ID 9

/* Pseudo relation IDs used by each cache. */
#define NREL_CLOG_REL_ID 0 
#define NREL_MULTIXACT_OFFSET_REL_ID 1 
#define NREL_MULTIXACT_MEMBER_REL_ID 2 
#define NREL_SUBTRANS_REL_ID 3
#define NREL_COMMITTS_REL_ID 4 
#define NREL_SERIAL_REL_ID 5 
#define NREL_NOTIFY_REL_ID 6 
#define NREL_NUM_RELS 7 

typedef bool (*NrelPagePrecedesFunction) (int, int);

static inline RelFileLocator
NrelRelFileLocator(RelFileNumber relFileNumber)
{
	RelFileLocator rlocator = {DEFAULTTABLESPACE_OID, NREL_DB_ID, relFileNumber};
	return rlocator;
}


/*
 * Define NREL segment size.  A page is the same BLCKSZ as is used everywhere
 * else in Postgres.  The segment size can be chosen somewhat arbitrarily;
 * we make it 32 pages by default, or 256Kb, i.e. 1M transactions for CLOG
 * or 64K transactions for SUBTRANS.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * page numbering also wraps around at 0xFFFFFFFF/xxxx_XACTS_PER_PAGE (where
 * xxxx is CLOG or SUBTRANS, respectively), and segment numbering at
 * 0xFFFFFFFF/xxxx_XACTS_PER_PAGE/NREL_PAGES_PER_SEGMENT.  We need
 * take no explicit notice of that fact in nrel.c, except when comparing
 * segment and page numbers in NonRelTruncate (see PagePrecedes()).
 */
#define NREL_PAGES_PER_SEGMENT	32

#ifdef USE_ASSERT_CHECKING
extern void NrelPagePrecedesUnitTests(NrelPagePrecedesFunction PagePrecedes,
									  int per_page);
#else
#define NrelPagePrecedesUnitTests(ctl, per_page) do {} while (0)
#endif
extern void NonRelTruncate(Oid rel_id, NrelPagePrecedesFunction PagePrecedes,
							  int cutoffPage);
extern bool NonRelDoesPhysicalPageExist(Oid rel_id, int pageno);

typedef bool (*NrelScanCallback) (Oid rel_id,
								  NrelPagePrecedesFunction PagePrecedes,
								  char *filename, int segpage,
								  void *data);
extern bool NrelScanDirectory(Oid rel_id, NrelPagePrecedesFunction PagePrecedes,
							  NrelScanCallback callback, void *data);
extern void NrelDeleteSegment(Oid rel_id, int segno);

/* NrelScanDirectory public callbacks */
extern bool NrelScanDirCbReportPresence(Oid rel_id,
										NrelPagePrecedesFunction PagePrecedes,
										char *filename,
										int segpage, void *data);
extern bool NrelScanDirCbDeleteAll(Oid rel_id, NrelPagePrecedesFunction PagePrecedes,
								   char *filename, int segpage,
								   void *data);

extern void CheckPointNREL(void);

/* Buffer access */
extern Buffer ReadNrelBuffer(Oid rel_id, int pageno);
extern Buffer ZeroNrelBuffer(Oid rel_id, int pageno);

/* Interfaces use by stats view */
extern Oid NrelRelIdByName(const char *name);
extern const char *NrelName(Oid rel_id);

/* Sync callback */
extern int	nrelsyncfiletag(const FileTag *ftag, char *path);

/* SMGR callbacks */
extern void nrelopen(SMgrRelation reln);
extern void nrelclose(SMgrRelation reln, ForkNumber forknum);
extern void nrelread(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, char *buffer);
extern void nrelwrite(SMgrRelation reln, ForkNumber forknum,
					  BlockNumber blocknum, char *buffer, bool skipFsync);
extern void nrelwriteback(SMgrRelation reln, ForkNumber forknum,
						  BlockNumber blocknum, BlockNumber nblocks);

#endif							/* NREL_H */
