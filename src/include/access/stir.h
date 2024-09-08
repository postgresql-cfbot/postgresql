/*-------------------------------------------------------------------------
 *
 * stir.h
 *	  header file for postgres stir access method implementation.
 *
 *
 * Portions Copyright (c) 2024-2024, PostgreSQL Global Development Group
 *
 * src/include/access/stir.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _STIR_H_
#define _STIR_H_

#include "amapi.h"
#include "xlog.h"
#include "generic_xlog.h"
#include "itup.h"
#include "fmgr.h"
#include "nodes/pathnodes.h"

/* Support procedures numbers */
#define STIR_NPROC				0

/* Scan strategies */
#define STIR_NSTRATEGIES		1

#define STIR_OPTIONS_PROC				0

/* Macros for accessing bloom page structures */
#define StirPageGetOpaque(page) ((StirPageOpaque) PageGetSpecialPointer(page))
#define StirPageGetMaxOffset(page) (StirPageGetOpaque(page)->maxoff)
#define StirPageIsMeta(page) \
	((StirPageGetOpaque(page)->flags & BLOOM_META) != 0)
#define StirPageGetData(page)		((StirTuple *)PageGetContents(page))
#define StirPageGetTuple(page, offset) \
	((StirTuple *)(PageGetContents(page) \
		+ sizeof(StirTuple) * ((offset) - 1)))
#define StirPageGetNextTuple(tuple) \
	((StirTuple *)((Pointer)(tuple) + sizeof(StirTuple)))



/* Preserved page numbers */
#define STIR_METAPAGE_BLKNO	(0)
#define STIR_HEAD_BLKNO		(1) /* first data page */


/* Opaque for stir pages */
typedef struct StirPageOpaqueData
{
	OffsetNumber maxoff;		/* number of index tuples on page */
	uint16		flags;			/* see bit definitions below */
	uint16		unused;			/* placeholder to force maxaligning of size of
								 * StirPageOpaqueData and to place
								 * stir_page_id exactly at the end of page */
	uint16		stir_page_id;	/* for identification of STIR indexes */
} StirPageOpaqueData;

/* Stir page flags */
#define STIR_META		(1<<0)

typedef StirPageOpaqueData *StirPageOpaque;

#define STIR_PAGE_ID		0xFF84

/* Metadata of stir index */
typedef struct StirMetaPageData
{
	uint32		magickNumber;
	uint16		lastBlkNo;
	bool		skipInserts;
} StirMetaPageData;

/* Magic number to distinguish stir pages from others */
#define STIR_MAGICK_NUMBER (0xDBAC0DEF)

#define StirPageGetMeta(page)	((StirMetaPageData *) PageGetContents(page))

typedef struct StirTuple
{
	ItemPointerData heapPtr;
} StirTuple;

#define StirPageGetFreeSpace(state, page) \
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) \
		- StirPageGetMaxOffset(page) * (sizeof(StirTuple)) \
		- MAXALIGN(sizeof(StirPageOpaqueData)))

extern void StirFillMetapage(Relation index, Page metaPage, bool skipInserts);
extern void StirInitMetapage(Relation index, ForkNumber forknum);
extern void StirInitPage(Page page, uint16 flags);
extern void StirMarkAsSkipInserts(Relation index);

/* index access method interface functions */
extern bool stirvalidate(Oid opclassoid);
extern bool stirinsert(Relation index, Datum *values, bool *isnull,
					 ItemPointer ht_ctid, Relation heapRel,
					 IndexUniqueCheck checkUnique,
					 bool indexUnchanged,
					 struct IndexInfo *indexInfo);
extern IndexScanDesc stirbeginscan(Relation r, int nkeys, int norderbys);
extern void stirrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
					 ScanKey orderbys, int norderbys);
extern void stirendscan(IndexScanDesc scan);
extern IndexBuildResult *stirbuild(Relation heap, Relation index,
								 struct IndexInfo *indexInfo);
extern void stirbuildempty(Relation index);
extern IndexBulkDeleteResult *stirbulkdelete(IndexVacuumInfo *info,
										   IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback,
										   void *callback_state);
extern IndexBulkDeleteResult *stirvacuumcleanup(IndexVacuumInfo *info,
											  IndexBulkDeleteResult *stats);
extern bytea *stiroptions(Datum reloptions, bool validate);

#endif
