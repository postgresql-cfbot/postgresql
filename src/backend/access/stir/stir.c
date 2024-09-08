/*-------------------------------------------------------------------------
 *
 * stir.c
 *	  Implementation of Short-Term Index Replacement.
 *
 * Portions Copyright (c) 2024-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/stir/stir.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/stir.h"
#include "commands/vacuum.h"
#include "utils/index_selfuncs.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_opfamily.h"
#include "utils/catcache.h"
#include "access/amvalidate.h"
#include "utils/syscache.h"
#include "access/htup_details.h"
#include "catalog/pg_amproc.h"
#include "catalog/index.h"
#include "catalog/pg_amop.h"
#include "utils/regproc.h"
#include "storage/bufmgr.h"
#include "access/tableam.h"
#include "access/reloptions.h"
#include "utils/memutils.h"
#include "utils/fmgrprotos.h"

/*
 * Stir handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
Datum
stirhandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = STIR_NSTRATEGIES;
	amroutine->amsupport = STIR_NPROC;
	amroutine->amoptsprocnum = STIR_OPTIONS_PROC;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = false;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = false;
	amroutine->amcanbuildparallel = false;
	amroutine->amcaninclude = true;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amparallelvacuumoptions =
			VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_CLEANUP;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = stirbuild;
	amroutine->ambuildempty = stirbuildempty;
	amroutine->aminsert = stirinsert;
	amroutine->aminsertcleanup = NULL;
	amroutine->ambulkdelete = stirbulkdelete;
	amroutine->amvacuumcleanup = stirvacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = stircostestimate;
	amroutine->amoptions = stiroptions;
	amroutine->amproperty = NULL;
	amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = stirvalidate;
	amroutine->amadjustmembers = NULL;
	amroutine->ambeginscan = stirbeginscan;
	amroutine->amrescan = stirrescan;
	amroutine->amgettuple = NULL;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = stirendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

bool
stirvalidate(Oid opclassoid)
{
	bool result = true;
	HeapTuple classtup;
	Form_pg_opclass classform;
	Oid opfamilyoid;
	HeapTuple familytup;
	Form_pg_opfamily familyform;
	char *opfamilyname;
	CatCList *proclist,
			*oprlist;
	int i;

	/* Fetch opclass information */
	classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
	if (!HeapTupleIsValid(classtup))
		elog(ERROR, "cache lookup failed for operator class %u", opclassoid);
	classform = (Form_pg_opclass) GETSTRUCT(classtup);

	opfamilyoid = classform->opcfamily;


	/* Fetch opfamily information */
	familytup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfamilyoid));
	if (!HeapTupleIsValid(familytup))
		elog(ERROR, "cache lookup failed for operator family %u", opfamilyoid);
	familyform = (Form_pg_opfamily) GETSTRUCT(familytup);

	opfamilyname = NameStr(familyform->opfname);

	/* Fetch all operators and support functions of the opfamily */
	oprlist = SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid));
	proclist = SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid));

	/* Check individual operators */
	for (i = 0; i < oprlist->n_members; i++)
	{
		HeapTuple oprtup = &oprlist->members[i]->tuple;
		Form_pg_amop oprform = (Form_pg_amop) GETSTRUCT(oprtup);

		/* Check it's allowed strategy for stir */
		if (oprform->amopstrategy < 1 ||
			oprform->amopstrategy > STIR_NSTRATEGIES)
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							errmsg("stir opfamily %s contains operator %s with invalid strategy number %d",
								   opfamilyname,
								   format_operator(oprform->amopopr),
								   oprform->amopstrategy)));
			result = false;
		}

		/* stir doesn't support ORDER BY operators */
		if (oprform->amoppurpose != AMOP_SEARCH ||
			OidIsValid(oprform->amopsortfamily))
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							errmsg("stir opfamily %s contains invalid ORDER BY specification for operator %s",
								   opfamilyname,
								   format_operator(oprform->amopopr))));
			result = false;
		}

		/* Check operator signature --- same for all stir strategies */
		if (!check_amop_signature(oprform->amopopr, BOOLOID,
								  oprform->amoplefttype,
								  oprform->amoprighttype))
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							errmsg("stir opfamily %s contains operator %s with wrong signature",
								   opfamilyname,
								   format_operator(oprform->amopopr))));
			result = false;
		}
	}


	ReleaseCatCacheList(proclist);
	ReleaseCatCacheList(oprlist);
	ReleaseSysCache(familytup);
	ReleaseSysCache(classtup);

	return result;
}


void
StirFillMetapage(Relation index, Page metaPage, bool skipInserts)
{
	StirMetaPageData *metadata;

	StirInitPage(metaPage, STIR_META);
	metadata = StirPageGetMeta(metaPage);
	memset(metadata, 0, sizeof(StirMetaPageData));
	metadata->magickNumber = STIR_MAGICK_NUMBER;
	metadata->skipInserts = skipInserts;
	((PageHeader) metaPage)->pd_lower += sizeof(StirMetaPageData);
}

void
StirInitMetapage(Relation index, ForkNumber forknum)
{
	Buffer metaBuffer;
	Page metaPage;
	GenericXLogState *state;

	/*
	 * Make a new page; since it is first page it should be associated with
	 * block number 0 (STIR_METAPAGE_BLKNO).  No need to hold the extension
	 * lock because there cannot be concurrent inserters yet.
	 */
	metaBuffer = ReadBufferExtended(index, forknum, P_NEW, RBM_NORMAL, NULL);
	LockBuffer(metaBuffer, BUFFER_LOCK_EXCLUSIVE);
	Assert(BufferGetBlockNumber(metaBuffer) == STIR_METAPAGE_BLKNO);

	/* Initialize contents of meta page */
	state = GenericXLogStart(index);
	metaPage = GenericXLogRegisterBuffer(state, metaBuffer,
										 GENERIC_XLOG_FULL_IMAGE);
	StirFillMetapage(index, metaPage, forknum == INIT_FORKNUM);
	GenericXLogFinish(state);

	UnlockReleaseBuffer(metaBuffer);
}

/*
 * Initialize any page of a stir index.
 */
void
StirInitPage(Page page, uint16 flags)
{
	StirPageOpaque opaque;

	PageInit(page, BLCKSZ, sizeof(StirPageOpaqueData));

	opaque = StirPageGetOpaque(page);
	opaque->flags = flags;
	opaque->stir_page_id = STIR_PAGE_ID;
}

static bool
StirPageAddItem(Page page, StirTuple *tuple)
{
	StirTuple *itup;
	StirPageOpaque opaque;
	Pointer ptr;

	/* We shouldn't be pointed to an invalid page */
	Assert(!PageIsNew(page));

	/* Does new tuple fit on the page? */
	if (StirPageGetFreeSpace(state, page) < sizeof(StirTuple))
		return false;

	/* Copy new tuple to the end of page */
	opaque = StirPageGetOpaque(page);
	itup = StirPageGetTuple(page, opaque->maxoff + 1);
	memcpy((Pointer) itup, (Pointer) tuple, sizeof(StirTuple));

	/* Adjust maxoff and pd_lower */
	opaque->maxoff++;
	ptr = (Pointer) StirPageGetTuple(page, opaque->maxoff + 1);
	((PageHeader) page)->pd_lower = ptr - page;

	/* Assert we didn't overrun available space */
	Assert(((PageHeader) page)->pd_lower <= ((PageHeader) page)->pd_upper);
	return true;
}

bool
stirinsert(Relation index, Datum *values, bool *isnull,
		  ItemPointer ht_ctid, Relation heapRel,
		  IndexUniqueCheck checkUnique,
		  bool indexUnchanged,
		  struct IndexInfo *indexInfo)
{
	StirTuple *itup;
	MemoryContext oldCtx;
	MemoryContext insertCtx;
	StirMetaPageData *metaData;
	Buffer buffer,
			metaBuffer;
	Page page;
	GenericXLogState *state;
	uint16 blkNo;

	insertCtx = AllocSetContextCreate(CurrentMemoryContext,
									  "Stir insert temporary context",
									  ALLOCSET_DEFAULT_SIZES);

	oldCtx = MemoryContextSwitchTo(insertCtx);

	itup = (StirTuple *) palloc0(sizeof(StirTuple));
	itup->heapPtr = *ht_ctid;

	metaBuffer = ReadBuffer(index, STIR_METAPAGE_BLKNO);

	for (;;)
	{
		LockBuffer(metaBuffer, BUFFER_LOCK_SHARE);
		metaData = StirPageGetMeta(BufferGetPage(metaBuffer));
		if (metaData->skipInserts)
		{
			UnlockReleaseBuffer(metaBuffer);
			return false;
		}
		blkNo = metaData->lastBlkNo;
		/* Don't hold metabuffer lock while doing insert */
		LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);

		if (blkNo > 0)
		{
			buffer = ReadBuffer(index, blkNo);
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

			state = GenericXLogStart(index);
			page = GenericXLogRegisterBuffer(state, buffer, 0);

			Assert(!PageIsNew(page));

			if (StirPageAddItem(page, itup))
			{
				/* Success!  Apply the change, clean up, and exit */
				GenericXLogFinish(state);
				UnlockReleaseBuffer(buffer);
				ReleaseBuffer(metaBuffer);
				MemoryContextSwitchTo(oldCtx);
				MemoryContextDelete(insertCtx);
				return false;
			}

			/* Didn't fit, must try other pages */
			GenericXLogAbort(state);
			UnlockReleaseBuffer(buffer);
		}

		LockBuffer(metaBuffer, BUFFER_LOCK_EXCLUSIVE);

		state = GenericXLogStart(index);
		metaData = StirPageGetMeta(GenericXLogRegisterBuffer(state, metaBuffer, GENERIC_XLOG_FULL_IMAGE));
		if (blkNo != metaData->lastBlkNo)
		{
			Assert(blkNo < metaData->lastBlkNo);
			// someone else inserted the new page into the index, lets try again
			GenericXLogAbort(state);
			LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);
			continue;
		}
		else
		{
			/* Must extend the file */
			buffer = ExtendBufferedRel(BMR_REL(index), MAIN_FORKNUM, NULL,
									   EB_LOCK_FIRST);

			page = GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
			StirInitPage(page, 0);

			if (!StirPageAddItem(page, itup))
			{
				/* We shouldn't be here since we're inserting to an empty page */
				elog(ERROR, "could not add new stir tuple to empty page");
			}
			metaData->lastBlkNo = BufferGetBlockNumber(buffer);
			GenericXLogFinish(state);

			UnlockReleaseBuffer(buffer);
			UnlockReleaseBuffer(metaBuffer);

			MemoryContextSwitchTo(oldCtx);
			MemoryContextDelete(insertCtx);

			return false;
		}
	}
}

IndexScanDesc stirbeginscan(Relation r, int nkeys, int norderbys)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("\"%s\" is not a not implemented", __func__)));
}

void
stirrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		  ScanKey orderbys, int norderbys)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("\"%s\" is not a not implemented", __func__)));
}

void stirendscan(IndexScanDesc scan)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("\"%s\" is not a not implemented", __func__)));
}

IndexBuildResult *stirbuild(Relation heap, Relation index,
						   struct IndexInfo *indexInfo)
{
	IndexBuildResult *result;

	StirInitMetapage(index, MAIN_FORKNUM);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = 0;
	result->index_tuples = 0;
	return result;
}

void stirbuildempty(Relation index)
{
	StirInitMetapage(index, INIT_FORKNUM);
}

IndexBulkDeleteResult *stirbulkdelete(IndexVacuumInfo *info,
									 IndexBulkDeleteResult *stats,
									 IndexBulkDeleteCallback callback,
									 void *callback_state)
{
	Relation index = info->index;
	BlockNumber blkno, npages;
	Buffer buffer;
	Page page;

	if (!info->validate_index)
	{
		StirMarkAsSkipInserts(index);

		ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("\"%s\" is not a not implemented, seems like this index need to be dropped", __func__)));
		return NULL;
	}

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	/*
	 * Iterate over the pages. We don't care about concurrently added pages,
	 * because TODO
	 */
	npages = RelationGetNumberOfBlocks(index);
	for (blkno = STIR_HEAD_BLKNO; blkno < npages; blkno++)
	{
		StirTuple *itup, *itupEnd;

		vacuum_delay_point();

		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);

		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);

		if (PageIsNew(page))
		{
			UnlockReleaseBuffer(buffer);
			continue;
		}

		itup = StirPageGetTuple(page, FirstOffsetNumber);
		itupEnd = StirPageGetTuple(page, OffsetNumberNext(StirPageGetMaxOffset(page)));
		while (itup < itupEnd)
		{
			/* Do we have to delete this tuple? */
			if (callback(&itup->heapPtr, callback_state))
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("we never delete in stir")));
			}

			itup = StirPageGetNextTuple(itup);
		}

		UnlockReleaseBuffer(buffer);
	}

	return stats;
}

void StirMarkAsSkipInserts(Relation index)
{
	StirMetaPageData *metaData;
	Buffer metaBuffer;
	Page metaPage;
	GenericXLogState *state;

	metaBuffer = ReadBuffer(index, STIR_METAPAGE_BLKNO);
	LockBuffer(metaBuffer, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(index);
	metaPage = GenericXLogRegisterBuffer(state, metaBuffer,
										 GENERIC_XLOG_FULL_IMAGE);
	metaData = StirPageGetMeta(metaPage);
	if (!metaData->skipInserts)
	{
		metaData->skipInserts = true;
		GenericXLogFinish(state);
	}
	else
	{
		GenericXLogAbort(state);
	}
	UnlockReleaseBuffer(metaBuffer);
}

IndexBulkDeleteResult *stirvacuumcleanup(IndexVacuumInfo *info,
										IndexBulkDeleteResult *stats)
{
	StirMarkAsSkipInserts(info->index);
	ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("\"%s\" is not a not implemented, seems like this index need to be dropped", __func__)));
	return NULL;
}

bytea *stiroptions(Datum reloptions, bool validate)
{
	return NULL;
}

void stircostestimate(PlannerInfo *root, IndexPath *path,
					 double loop_count, Cost *indexStartupCost,
					 Cost *indexTotalCost, Selectivity *indexSelectivity,
					 double *indexCorrelation, double *indexPages)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("\"%s\" is not a not implemented", __func__)));
}
