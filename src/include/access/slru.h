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
#define SLRU_SPC_ID 9

/* Pseudo database IDs used by each cache. */
#define PG_SLRU(symname,name,path, synchronize) \
	symname,

typedef enum SlruIds
{
#include "access/slrulist.h"
	SLRU_NEXT_ID
}			SlruIds;
#undef PG_SLRU

typedef bool (*SlruPagePrecedesFunction) (int, int);

static inline RelFileLocator
SlruRelFileLocator(uint32 slru_db_id, uint32 segment_id)
{
	RelFileLocator rlocator = {SLRU_SPC_ID, slru_db_id, segment_id};
	return rlocator;
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
extern void SimpleLruTruncate(int slru_id, SlruPagePrecedesFunction PagePrecedes,
							  int cutoffPage);
extern bool SimpleLruDoesPhysicalPageExist(int slru_id, int pageno);

typedef bool (*SlruScanCallback) (int slru_id,
								  SlruPagePrecedesFunction PagePrecedes,
								  char *filename, int segpage,
								  void *data);
extern bool SlruScanDirectory(int slru_id, SlruPagePrecedesFunction PagePrecedes,
							  SlruScanCallback callback, void *data);
extern void SlruDeleteSegment(int slru_id, int segno);

/* SlruScanDirectory public callbacks */
extern bool SlruScanDirCbReportPresence(int slru_id,
										SlruPagePrecedesFunction PagePrecedes,
										char *filename,
										int segpage, void *data);
extern bool SlruScanDirCbDeleteAll(int slru_id, SlruPagePrecedesFunction PagePrecedes,
								   char *filename, int segpage,
								   void *data);

/* Buffer access */
extern Buffer ReadSlruBuffer(int slru_id, int pageno);
extern Buffer ZeroSlruBuffer(int slru_id, int pageno);
extern bool ProbeSlruBuffer(int slru_id, int pageno);

/* Interfaces use by stats view */
extern Oid SlruRelIdByName(const char *name);
extern const char *SlruName(int slru_id);

#endif							/* SLRU_H */
