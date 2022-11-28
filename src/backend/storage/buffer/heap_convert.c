/*-------------------------------------------------------------------------
 *
 * heap_convert.c
 *	  Heap page converter from 32bit to 64bit xid format
 *
 *	Copyright (c) 2015-2022, Postgres Professional
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/heap_convert.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "catalog/catalog.h"
#include "storage/bufmgr.h"
#include "storage/checksum.h"

static void repack_heap_tuples(Relation rel, Page page, Buffer buf,
							   BlockNumber blkno, bool double_xmax);

/*
 * itemoffcompare
 *		Sorting support for repack_tuples()
 */
int
itemoffcompare(const void *item1, const void *item2)
{
	/* Sort in decreasing itemoff order */
	return ((ItemIdCompactData *) item2)->itemoff -
		   ((ItemIdCompactData *) item1)->itemoff;
}

/*
 * Lazy page conversion from 32-bit to 64-bit XID at first read.
 */
void
convert_page(Relation rel, Page page, Buffer buf, BlockNumber blkno)
{
	static unsigned		logcnt = 0;
	bool				logit;
	PageHeader			hdr = (PageHeader) page;
	GenericXLogState   *state = NULL;
	uint16				checksum;
	bool				try_double_xmax;

	/* Not during XLog replaying */
	Assert(rel != NULL);

	/* Verify checksum */
	if (hdr->pd_checksum)
	{
		checksum = pg_checksum_page((char *) page, blkno);
		if (checksum != hdr->pd_checksum)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("page verification failed, calculated checksum %u but expected %u",
							checksum, hdr->pd_checksum)));
	}

	/*
	 * We occasionally force logging of page conversion, so never-changed
	 * pages are converted in the end. FORCE_LOG_EVERY is chosen arbitrarily
	 * to log neither too much nor too little.
	 */
#define FORCE_LOG_EVERY 128
	logit = !RecoveryInProgress() && XLogIsNeeded() && RelationNeedsWAL(rel);
	logit = logit && (++logcnt % FORCE_LOG_EVERY) == 0;
	if (logit)
	{
		state = GenericXLogStart(rel);
		page = GenericXLogRegisterBuffer(state, buf,
										 GENERIC_XLOG_FULL_IMAGE);
		hdr = (PageHeader) page;
	}
#ifdef USE_ASSERT_CHECKING
	else
	{
		/* Not already converted */
		Assert(PageGetPageLayoutVersion(page) != PG_PAGE_LAYOUT_VERSION);
		/* Page in 32-bit xid format should not have PageSpecial. */
		Assert(PageGetSpecialSize(page) == 0);
	}
#endif

	switch (rel->rd_rel->relkind)
	{
		case 't':
			try_double_xmax = hdr->pd_upper - hdr->pd_lower <
									MAXALIGN(sizeof(ToastPageSpecialData));
			repack_heap_tuples(rel, page, buf, blkno, try_double_xmax);
			break;
		case 'r':
		case 'p':
		case 'm':
			try_double_xmax = hdr->pd_upper - hdr->pd_lower <
									MAXALIGN(sizeof(HeapPageSpecialData));
			repack_heap_tuples(rel, page, buf, blkno, try_double_xmax);
			break;
		case 'i':
			/* no need to convert index */
		case 'S':
			/* no real need to convert sequences */
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("conversion for relation \"%s\" cannot be done",
							RelationGetRelationName(rel)),
					 errdetail_relkind_not_supported(rel->rd_rel->relkind)));
	}

	hdr->pd_checksum = pg_checksum_page((char *) page, blkno);

	PageSetPageSizeAndVersion(page, PageGetPageSize(page),
							  PG_PAGE_LAYOUT_VERSION);

	if (logit)
	{
		/*
		 * Finish logging buffer conversion and mark buffer as dirty.
		 */
		Assert(state != NULL);
		MarkBufferDirty(buf);
		GenericXLogFinish(state);
	}
	else
	{
		/*
		 * Otherwise, it will be logged with full-page-write record on first
		 * actual change.
		 */
		MarkBufferConverted(buf, true);
	}
}

/*
 * Convert xmin and xmax in a tuple.
 * This also considers special cases: "double xmax" page format and multixact
 * in xmax.
 */
static void
convert_heap_tuple_xids(HeapTupleHeader tuple, TransactionId xid_base,
						MultiXactId multi_base, bool double_xmax)
{
	/* Convert xmin */
	if (double_xmax)
	{
		/* Prepare tuple for "double xmax" page format */
		tuple->t_infomask |= HEAP_XMIN_FROZEN;
		tuple->t_choice.t_heap.t_xmin = 0;
	}
	else
	{
		TransactionId xmin = tuple->t_choice.t_heap.t_xmin;

		if (TransactionIdIsNormal(xmin))
		{
			if (HeapTupleHeaderXminFrozen(tuple))
				tuple->t_choice.t_heap.t_xmin = FrozenTransactionId;
			else if (HeapTupleHeaderXminInvalid(tuple))
				tuple->t_choice.t_heap.t_xmin = InvalidTransactionId;
			else
			{
				Assert(xmin >= xid_base + FirstNormalTransactionId);
				/* Subtract xid_base from normal xmin */
				tuple->t_choice.t_heap.t_xmin = xmin - xid_base;
			}
		}
	}

	/* If tuple has multixact flag, handle mxid wraparound */
	if ((tuple->t_infomask & HEAP_XMAX_IS_MULTI) &&
		!(tuple->t_infomask & HEAP_XMAX_INVALID))
	{
		MultiXactId mxid = tuple->t_choice.t_heap.t_xmax;

		/* Handle mxid wraparound */
		if (mxid < multi_base)
		{
			mxid += ((MultiXactId) 1 << 32) - FirstMultiXactId;
			Assert(mxid >= multi_base);
		}

		if (double_xmax)
		{
			/* Save converted mxid into "double xmax" format */
			HeapTupleHeaderSetDoubleXmax(tuple, mxid);
		}
		else
		{
			/*
			 * Save converted mxid offset relative to (minmxid - 1), which
			 * will be page's mxid base.
			 */
			Assert(mxid - multi_base + FirstMultiXactId <= PG_UINT32_MAX);
			tuple->t_choice.t_heap.t_xmax =
				(uint32) (mxid - multi_base + FirstMultiXactId);
		}
	}
	/* Convert xmax */
	else if (!(tuple->t_infomask & HEAP_XMAX_INVALID))
	{
		TransactionId xmax = tuple->t_choice.t_heap.t_xmax;

		if (double_xmax)
		{
			/* Save converted xmax into "double xmax" format */
			HeapTupleHeaderSetDoubleXmax(tuple, xmax);
		}
		else if (TransactionIdIsNormal(xmax))
		{
			/* Subtract xid_base from normal xmax */
			Assert(xmax >= xid_base + FirstNormalTransactionId);
			tuple->t_choice.t_heap.t_xmax = xmax - xid_base;
		}
	}
	else
	{
		if (double_xmax)
			HeapTupleHeaderSetDoubleXmax(tuple, InvalidTransactionId);
		else
			tuple->t_choice.t_heap.t_xmax = InvalidTransactionId;
	}
}

/*
 * Correct page xmin/xmax based on tuple xmin/xmax values.
 */
static void
compute_xid_min_max(HeapTuple tuple, MultiXactId multi_base,
					TransactionId *xid_min, TransactionId *xid_max,
					MultiXactId *multi_min, MultiXactId *multi_max)
{
	/* xmin */
	if (!HeapTupleHeaderXminInvalid(tuple->t_data) &&
		!HeapTupleHeaderXminFrozen(tuple->t_data))
	{
		TransactionId xid = HeapTupleGetRawXmin(tuple);

		if (TransactionIdIsNormal(xid))
		{
			*xid_max = Max(*xid_max, xid);
			*xid_min = Min(*xid_min, xid);
		}
	}

	/* xmax */
	if (!(tuple->t_data->t_infomask & HEAP_XMAX_INVALID))
	{
		TransactionId xid;

		if (tuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI)
		{
			MultiXactId mxid = HeapTupleGetRawXmax(tuple);

			Assert(MultiXactIdIsValid(mxid));

			/* Handle mxid wraparound */
			if (mxid < multi_base)
			{
				mxid += ((MultiXactId) 1 << 32) - FirstMultiXactId;
				Assert(mxid >= multi_base);
			}

			*multi_max = Max(*multi_max, mxid);
			*multi_min = Min(*multi_min, mxid);

			/*
			 * Also take into account hidden update xid, which can be
			 * extracted by the vacuum.
			 */
			if (tuple->t_data->t_infomask & HEAP_XMAX_LOCK_ONLY)
				xid = InvalidTransactionId;
			else
				xid = HeapTupleGetUpdateXid(tuple);
		}
		else
		{
			xid = HeapTupleGetRawXmax(tuple);
		}

		if (TransactionIdIsNormal(xid))
		{
			*xid_max = Max(*xid_max, xid);
			*xid_min = Min(*xid_min, xid);
		}
	}
}

/*
 * Returns true if both:
 *  - xid_max: an uppper boundary of xmin's and xmax'es of all tuples on a page
 *  - xid_min: a lower boundary of xmin's and xmax'es of all tuples on a page
 *    can be expressed by 32-bit number relative to page's xid_base/multi_base
 *    or invalid.
 *
 * True value effectively means that these tuples can be directly put on one
 * page in 64-xid format.
 */
static inline bool
xids_fit_page(TransactionId xid_min, TransactionId xid_max,
			  MultiXactId multi_min, MultiXactId multi_max)
{
	bool xid_max_fits = false;
	bool multi_max_fits = false;

	if (xid_max == InvalidTransactionId)
		xid_max_fits = true;

	if (xid_max - xid_min <= MaxShortTransactionId - FirstNormalTransactionId)
		xid_max_fits = true;

	if (multi_max == InvalidMultiXactId)
		multi_max_fits = true;

	if (multi_max - multi_min <= MaxShortTransactionId - FirstMultiXactId)
		multi_max_fits = true;

	return xid_max_fits && multi_max_fits;
}

/*
 * Set "base" for page in 64-bit XID format.
 *
 * This should not be called for double xmax pages. They do not have place for
 * page special.
 */
static inline void
heap_page_set_base(Page page,
				   TransactionId xid_min, TransactionId xid_max,
				   MultiXactId multi_min, MultiXactId multi_max,
				   TransactionId *xid_base, MultiXactId *multi_base,
				   bool is_toast)
{
	PageHeader			hdr = (PageHeader) page;

	if (xid_max != InvalidTransactionId)
		*xid_base = xid_min - FirstNormalTransactionId;
	else
		*xid_base = InvalidTransactionId;

	if (multi_max != InvalidMultiXactId)
		*multi_base = multi_min - FirstMultiXactId;
	else
		*multi_base = InvalidMultiXactId;

	if (is_toast)
	{
		ToastPageSpecial		special;

		hdr->pd_special = BLCKSZ - MAXALIGN(sizeof(ToastPageSpecialData));
		special = ToastPageGetSpecial(page);
		special->pd_xid_base = *xid_base;
	}
	else
	{
		HeapPageSpecial		special;

		hdr->pd_special = BLCKSZ - MAXALIGN(sizeof(HeapPageSpecialData));
		special = HeapPageGetSpecial(page);
		special->pd_xid_base = *xid_base;
		special->pd_multi_base = *multi_base;
	}
}

/*
 * repack_heap_tuples
 *		Convert heap page format reusing space of dead tuples
 */
static void
repack_heap_tuples(Relation rel, Page page, Buffer buf, BlockNumber blkno,
				   bool try_double_xmax)
{
	ItemIdCompactData	items[MaxHeapTuplesPerPage];
	ItemIdCompact		itemPtr = items;
	int					nitems = 0,
						maxoff = PageGetMaxOffsetNumber(page),
						idx,
						occupied_space = 0;
	Offset				upper;
	bool				double_xmax,
						special_fits,
						toast;
	PageHeader			hdr = (PageHeader) page,
						new_hdr;
	PGAlignedBlock		zerobuf = {0};
	Page				new_page;
	MultiXactId			multi_base = rel->rd_rel->relminmxid,
						multi_min = MaxMultiXactId,
						multi_max = InvalidMultiXactId;
	TransactionId		xid_base = rel->rd_rel->relfrozenxid,
						xid_min = MaxTransactionId,
						xid_max = InvalidTransactionId;

	toast = IsToastRelation(rel);

	if (TransactionIdIsNormal(hdr->pd_prune_xid))
		xid_min = xid_max = hdr->pd_prune_xid;

	for (idx = 0; idx < maxoff; idx++)
	{
		HeapTupleData		tuple;
		ItemId				lp;

		lp = PageGetItemId(page, idx + 1);

		/* Skip redirects and items without storage */
		if (!ItemIdHasStorage(lp))
			continue;

		/* Build in-memory tuple representation */
		tuple.t_tableOid = 1;	/* doesn't matter in this case */
		tuple.t_data = (HeapTupleHeader) PageGetItem(page, lp);
		HeapTupleCopyHeaderXids(&tuple);
		tuple.t_len = ItemIdGetLength(lp);
		ItemPointerSet(&(tuple.t_self), blkno, ItemIdGetOffset(lp));

		/*
		 * This is only needed to determine whether tuple is HEAPTUPLE_DEAD or
		 * HEAPTUPLE_RECENTLY_DEAD. And since this is the first time we read
		 * page after pg_upgrade, it cannot be HEAPTUPLE_RECENTLY_DEAD. See
		 * HeapTupleSatisfiesVacuum() for details
		 */
		if (try_double_xmax &&
			HeapTupleSatisfiesVacuum(&tuple,
									 (TransactionId) 1 << 32, buf) == HEAPTUPLE_DEAD)
		{
			ItemIdSetDead(lp);
		}

		if (ItemIdIsNormal(lp) && ItemIdHasStorage(lp))
		{
			itemPtr->offsetindex = idx;
			itemPtr->itemoff = ItemIdGetOffset(lp);
			if (unlikely(itemPtr->itemoff < hdr->pd_upper ||
						 itemPtr->itemoff >= hdr->pd_special))
			{
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("corrupted item pointer: %u",
								itemPtr->itemoff)));
			}

			itemPtr->alignedlen = MAXALIGN(ItemIdGetLength(lp));
			occupied_space += itemPtr->alignedlen;
			nitems++;
			itemPtr++;
			if (try_double_xmax)
			{
				HeapTupleSetXmin(&tuple, FrozenTransactionId);
				HeapTupleHeaderStoreXminFrozen(tuple.t_data);
			}

			compute_xid_min_max(&tuple, multi_base,
								&xid_min, &xid_max,
								&multi_min, &multi_max);
		}
	}

	new_page = (Page) zerobuf.data;
	MemSet(new_page, 0, BLCKSZ);
	/* Write new header */
	new_hdr = (PageHeader) new_page;
	*new_hdr = *hdr;
	new_hdr->pd_lower = SizeOfPageHeaderData + maxoff * sizeof(ItemIdData);

	if (toast)
		special_fits = BLCKSZ - new_hdr->pd_lower - occupied_space >=
						sizeof(ToastPageSpecialData);
	else
		special_fits = BLCKSZ - new_hdr->pd_lower - occupied_space >=
						sizeof(HeapPageSpecialData);

	double_xmax = !special_fits ||
				  !xids_fit_page(xid_min, xid_max, multi_min, multi_max);

	if (!double_xmax)
	{
		Assert(xid_max == InvalidTransactionId || xid_max >= xid_min);
		Assert(multi_max == InvalidMultiXactId || multi_max >= multi_min);

		heap_page_set_base(new_page,
						   xid_min, xid_max,
						   multi_min, multi_max,
						   &xid_base, &multi_base,
						   toast);

		HeapPageSetPruneXid(new_page, new_hdr->pd_prune_xid, toast);
	}
	else
	{
		/* No space for special area, switch to "double xmax" format */
		elog(DEBUG2, "convert heap page %u of relation \"%s\" to double xmax format",
			 blkno, RelationGetRelationName(rel));

		if (try_double_xmax)
		{
			xid_base = InvalidTransactionId;
			multi_base = InvalidMultiXactId;
		}
		else
		{
			repack_heap_tuples(rel, page, buf, blkno, true);
			return;
		}
	}

	/* Copy ItemIds with an offset */
	memcpy((char *) new_page + SizeOfPageHeaderData,
		   (char *) page + SizeOfPageHeaderData,
		   hdr->pd_lower - SizeOfPageHeaderData);

	/* Move live tuples */
	upper = new_hdr->pd_special;
	for (idx = 0; idx < nitems; idx++)
	{
		HeapTupleHeader		tuple;
		ItemId				lp;

		itemPtr = &items[idx];
		lp = PageGetItemId(new_page, itemPtr->offsetindex + 1);
		upper -= itemPtr->alignedlen;
		occupied_space -= itemPtr->alignedlen;

		memcpy((char *) new_page + upper,
			   (char *) page + itemPtr->itemoff,
			   itemPtr->alignedlen);

		tuple = (HeapTupleHeader) (((char *) new_page) + upper);

		convert_heap_tuple_xids(tuple, xid_base, multi_base, double_xmax);

		lp->lp_off = upper;
	}

	Assert(occupied_space == 0);

	new_hdr->pd_upper = upper;
	if (new_hdr->pd_lower > new_hdr->pd_upper)
		elog(ERROR, "cannot convert block %u of relation \"%s\"",
			 blkno, RelationGetRelationName(rel));

	memcpy(page, new_page, BLCKSZ);
}
