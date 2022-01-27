/*
 *	heap_convert.c
 *		Heap page converter from 32bit to 64bit xid format
 *
 *	Copyright (c) 2017, Postgres Professional
 *
 *	src/backend/storage/buffer/heap_convert.c
 */

#include "postgres.h"

#include "access/brin_page.h"
#include "access/ginblock.h"
#include "access/generic_xlog.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_am.h"
#include "catalog/pg_control.h"
#include "common/controldata_utils.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * Sorting support for repack_tuples()
 */
typedef struct itemData
{
	uint16		index;			/* linpointer array index */
	int16		offset;			/* page offset of item data */
	uint16		alignedlen;		/* MAXALIGN(item data len) */
} itemData;

/* Initialize special heap page area */
static void
InitHeapPageSpecial(PageHeader new, TransactionId xid_base,
					MultiXactId multi_base, TransactionId prune_xid)
{
	HeapPageSpecial special;

	new->pd_special	= BLCKSZ - MAXALIGN(sizeof(HeapPageSpecialData));

	special = (HeapPageSpecial) ((char *) new + new->pd_special);
	special->pd_xid_base = xid_base;
	special->pd_multi_base = multi_base;
	HeapPageSetPruneXid(new, prune_xid);
}

/* Used by get_previous_edition() */
ControlFileData *ControlFile = NULL;

static bool was_32bit_xid(PageHeader page);
static void convert_heap(Relation rel, Page page, Buffer buf, BlockNumber blkno);
static void repack_heap_tuples(Relation rel, Page page, Buffer buf,
							   BlockNumber blkno, bool double_xmax);
static void tuple_set_double_xmax(HeapTupleHeader tuple);


/*
 * itemoffcompare
 *		Sorting support for repack_tuples()
 */
int
itemoffcompare(const void *item1, const void *item2)
{
	/* Sort in decreasing itemoff order */
	return ((itemIdCompactData *) item2)->itemoff -
		((itemIdCompactData *) item1)->itemoff;
}

static bool
was_32bit_xid(PageHeader page)
{
	return PageGetPageLayoutVersion(page) < 5;
}

void
convert_page(Relation rel, Page page, Buffer buf, BlockNumber blkno)
{
	PageHeader	hdr = (PageHeader) page;
	GenericXLogState *state = NULL;
	Page	tmp_page = page;
	uint16	checksum;

	if (!rel)
		return;

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

	/* Start xlog record */
	if (!XactReadOnly && XLogIsNeeded() && RelationNeedsWAL(rel))
	{
		state = GenericXLogStart(rel);
		tmp_page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
	}

	PageSetPageSizeAndVersion((hdr), PageGetPageSize(hdr),
							  PG_PAGE_LAYOUT_VERSION);

	if (was_32bit_xid(hdr))
	{
		switch (rel->rd_rel->relkind)
		{
			case 'r':
			case 'p':
			case 't':
			case 'm':
				convert_heap(rel, tmp_page, buf, blkno);
				break;
			case 'i':
				/* no need to convert index */
			case 'S':
				/* no real need to convert sequences */
				break;
			default:
				elog(ERROR,
					 "Conversion for relkind '%c' is not implemented",
					 rel->rd_rel->relkind);
		}
	}

	/*
	 * Mark buffer dirty unless this is a read-only transaction (e.g. query
	 * is running on hot standby instance)
	 */
	if (!XactReadOnly)
	{
		/* Finish xlog record */
		if (XLogIsNeeded() && RelationNeedsWAL(rel))
		{
			Assert(state != NULL);
			GenericXLogFinish(state);
		}

		MarkBufferDirty(buf);
	}

	hdr = (PageHeader) page;
	hdr->pd_checksum = pg_checksum_page((char *) page, blkno);
}

static void
convert_heap(Relation rel, Page page, Buffer buf, BlockNumber blkno)
{
	PageHeader	page_hdr = (PageHeader) page;
	bool		heap_special_fits;

	/* Is there enough space to fit new page format? */
	heap_special_fits = page_hdr->pd_upper - page_hdr->pd_lower >= SizeOfPageSpecial;
	repack_heap_tuples(rel, page, buf, blkno, !heap_special_fits);
}

/*
 * Convert possibly wrapped around heap tuple's transaction and
 * multixact IDs after pg_upgrade.
 */
static void
convert_heap_tuple_xids(HeapTupleHeader tuple, TransactionId xid_base,
						MultiXactId mxid_base, bool double_xmax)
{
	TransactionId xid;

	/* Convert xmin xid */
	if (double_xmax)
		tuple_set_double_xmax(tuple);
	else
	{
		/* Subtract xid_base from normal xmin */
		xid = tuple->t_choice.t_heap.t_xmin;

		if (TransactionIdIsNormal(xid))
		{
			Assert(xid >= xid_base + FirstNormalTransactionId);
			tuple->t_choice.t_heap.t_xmin = xid - xid_base;
		}
	}

	/* If tuple has multixact flag, handle mxid wraparound */
	if ((tuple->t_infomask & HEAP_XMAX_IS_MULTI) &&
	   !(tuple->t_infomask & HEAP_XMAX_INVALID))
	{
		MultiXactId mxid = tuple->t_choice.t_heap.t_xmax;

		/* Handle mxid wraparound */
		if (mxid < mxid_base)
		{
			mxid += ((MultiXactId) 1 << 32) - FirstMultiXactId;
			Assert(mxid >= mxid_base);
		}

		if (double_xmax)
		{
			/* Save converted mxid into xmin/max */
			HeapTupleHeaderSetDoubleXmax(tuple, mxid);
		}
		else
		{
			/*
			 * Save converted mxid offset relative to (minmxid - 1),
			 * which will be page's mxid base.
			 */
			Assert(mxid - mxid_base + FirstMultiXactId <= PG_UINT32_MAX);
			tuple->t_choice.t_heap.t_xmax =
				(uint32) (mxid - mxid_base + FirstMultiXactId);
		}
	}
	/* Convert xmax xid */
	else if (!(tuple->t_infomask & HEAP_XMAX_INVALID))
	{
		xid = tuple->t_choice.t_heap.t_xmax;

		if (double_xmax)
		{
			/* Save converted mxid into xmin/max */
			HeapTupleHeaderSetDoubleXmax(tuple, xid);
		}
		else if (TransactionIdIsNormal(xid))
		{
			/* Subtract xid_base from normal xmax */
			Assert(xid >= xid_base + FirstNormalTransactionId);
			tuple->t_choice.t_heap.t_xmax = xid - xid_base;
		}
	}
}

/*
 * Compute page's [m]xid min/max values for based/"double xmax"
 * format conversions
 */
static void
compute_xid_min_max(HeapTuple tuple, MultiXactId mxid_base,
					TransactionId *xid_min, TransactionId *xid_max,
					MultiXactId *mxid_min, MultiXactId *mxid_max)
{
	if (!HeapTupleHeaderXminInvalid(tuple->t_data) &&
		!HeapTupleHeaderXminFrozen(tuple->t_data))
	{
		TransactionId xid = HeapTupleGetRawXmin(tuple);

		if (TransactionIdIsNormal(xid))
		{
			if (*xid_max < xid)
				*xid_max = xid;
			if (*xid_min > xid)
				*xid_min = xid;
		}
	}

	if (!(tuple->t_data->t_infomask & HEAP_XMAX_INVALID))
	{
		TransactionId xid;

		if (tuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI)
		{
			MultiXactId mxid = HeapTupleGetRawXmax(tuple);

			Assert(MultiXactIdIsValid(mxid));

			/* Handle mxid wraparound */
			if (mxid < mxid_base)
			{
				mxid += ((MultiXactId) 1 << 32) - FirstMultiXactId;
				Assert(mxid >= mxid_base);
			}

			if (*mxid_max < mxid)
				*mxid_max = mxid;
			if (*mxid_min > mxid)
				*mxid_min = mxid;

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
			if (*xid_max < xid)
				*xid_max = xid;
			if (*xid_min > xid)
				*xid_min = xid;
		}
	}
}

/* Returns true, if "double xmax" format */
static bool
init_heap_page_header(Relation rel, BlockNumber blkno, PageHeader new_hdr,
					  TransactionId prune_xid, bool header_fits,
					  TransactionId xid_min, TransactionId xid_max,
					  MultiXactId mxid_min, MultiXactId mxid_max,
					  TransactionId *xid_base, MultiXactId *mxid_base)
{
	if (header_fits &&
		(xid_max == InvalidTransactionId ||
		 xid_max - xid_min <= MaxShortTransactionId - FirstNormalTransactionId) &&
		(mxid_max == InvalidMultiXactId ||
		 mxid_max - mxid_min <= MaxShortTransactionId - FirstMultiXactId))
	{
		Assert(xid_max == InvalidTransactionId || xid_max >= xid_min);
		Assert(mxid_max == InvalidMultiXactId || mxid_max >= mxid_min);
		*xid_base = xid_max == InvalidTransactionId ? InvalidTransactionId : xid_min - FirstNormalTransactionId;
		*mxid_base = mxid_max == InvalidMultiXactId ? InvalidMultiXactId : mxid_min - FirstMultiXactId;

		InitHeapPageSpecial(new_hdr, *xid_base, *mxid_base, prune_xid);
		return false;
	}
	else
	{
		/* No space for special area, switch to "double xmax" format */
		new_hdr->pd_special = BLCKSZ;

		*xid_base = InvalidTransactionId;
		*mxid_base = InvalidMultiXactId;

		elog(DEBUG2, "convert heap page %u of relation %u to double xmax format",
			 blkno, RelationGetRelid(rel));
		return true;
	}
}

/*
 * repack_heap_tuples
 *		Convert heap page format reusing space of dead tuples
 */
static void
repack_heap_tuples(Relation rel, Page page, Buffer buf, BlockNumber blkno,
				   bool perhaps_double_xmax)
{
	itemIdCompactData items[MaxHeapTuplesPerPage];
	itemIdCompact itemPtr = items;
	ItemId		lp;
	int			nitems = 0;
	int			maxoff = PageGetMaxOffsetNumber(page);
	Offset		upper;
	int			idx;
	bool		double_xmax;

	PageHeader	hdr;
	PageHeader	new_hdr;
	char		new_page[BLCKSZ];
	MultiXactId mxid_base = rel->rd_rel->relminmxid;
	MultiXactId mxid_min = MaxMultiXactId;
	MultiXactId mxid_max = InvalidMultiXactId;
	TransactionId xid_base = rel->rd_rel->relfrozenxid;
	TransactionId xid_min = MaxTransactionId;
	TransactionId xid_max = InvalidTransactionId;

	int occupied_space = 0;

	hdr = (PageHeader) page;

	if (TransactionIdIsNormal(hdr->pd_prune_xid))
		xid_min = xid_max = hdr->pd_prune_xid;

	for (idx = 0; idx < maxoff; idx++)
	{
		HeapTupleData	tuple;

		lp = PageGetItemId(page, idx + 1);

		/* Skip redirects and items without storage */
		if (!ItemIdHasStorage(lp))
			continue;

		/* Build in-memory tuple representation */
		tuple.t_tableOid = 1; /* doesn't matter in this case */
		tuple.t_data = (HeapTupleHeader) PageGetItem(page, lp);
		tuple.t_xid_base = 0;
		tuple.t_multi_base = 0;
		tuple.t_len = ItemIdGetLength(lp);
		ItemPointerSet(&(tuple.t_self), blkno, ItemIdGetOffset(lp));

		/*
		 * This is only needed to determine whether tuple is HEAPTUPLE_DEAD or
		 * HEAPTUPLE_RECENTLY_DEAD. And since this is the first time we read
		 * page after pg_upgrade, it cannot be HEAPTUPLE_RECENTLY_DEAD. See
		 * HeapTupleSatisfiesVacuum() for details
		 */
		if (perhaps_double_xmax &&
			HeapTupleSatisfiesVacuum(&tuple, FirstUpgradedTransactionId, buf) == HEAPTUPLE_DEAD)
		{
			ItemIdSetDead(lp);
		}

		if (ItemIdIsNormal(lp) && ItemIdHasStorage(lp))
		{
			itemPtr->offsetindex = idx;
			itemPtr->itemoff = ItemIdGetOffset(lp);
			if (unlikely(itemPtr->itemoff < hdr->pd_upper ||
						 itemPtr->itemoff >= hdr->pd_special))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("corrupted item pointer: %u",
									   itemPtr->itemoff)));
			itemPtr->alignedlen = MAXALIGN(ItemIdGetLength(lp));
			occupied_space += itemPtr->alignedlen;
			nitems++;
			itemPtr++;
			if (perhaps_double_xmax)
			{
				HeapTupleSetXmin(&tuple, FrozenTransactionId);
				HeapTupleHeaderSetXminFrozen(tuple.t_data);
			}

			compute_xid_min_max(&tuple, mxid_base,
								&xid_min, &xid_max,
								&mxid_min, &mxid_max);
		}
	}

	/* Write new header */
	new_hdr = (PageHeader) new_page;
	*new_hdr = *hdr;
	new_hdr->pd_lower = SizeOfPageHeaderData + maxoff * sizeof(ItemIdData);

	double_xmax = init_heap_page_header(rel, blkno, new_hdr,
										hdr->pd_prune_xid,
										BLCKSZ - new_hdr->pd_lower - occupied_space >= sizeof(HeapPageSpecialData),
										xid_min, xid_max,
										mxid_min, mxid_max,
										&xid_base, &mxid_base);
	if (!perhaps_double_xmax && double_xmax)
		return repack_heap_tuples(rel, page, buf, blkno, true);

	/* Copy ItemIds with an offset */
	memcpy(new_page + SizeOfPageHeaderData,
		   page + SizeOfPageHeaderData,
		   hdr->pd_lower - SizeOfPageHeaderData);

	/* Move live tuples */
	upper = new_hdr->pd_special;
	for (idx = 0; idx < nitems; idx++)
	{
		HeapTupleHeader tuple;

		itemPtr = &items[idx];
		lp = PageGetItemId(new_page, itemPtr->offsetindex + 1);
		upper -= itemPtr->alignedlen;

		memcpy((char *) new_page + upper,
			   (char *) page + itemPtr->itemoff,
			   itemPtr->alignedlen);

		tuple = (HeapTupleHeader) (((char *) new_page) + upper);

		convert_heap_tuple_xids(tuple, xid_base, mxid_base, double_xmax);

		lp->lp_off = upper;

		occupied_space -= itemPtr->alignedlen;
	}
	Assert(occupied_space == 0);

	new_hdr->pd_upper = upper;
	if (new_hdr->pd_lower > new_hdr->pd_upper)
		elog(ERROR, "cannot convert block %u of relation '%s'",
			 blkno, RelationGetRelationName(rel));

	memcpy(page, new_page, BLCKSZ);
}

/*
 * Convert tuple for "double xmax" page format.
 */
static void
tuple_set_double_xmax(HeapTupleHeader tuple)
{
	tuple->t_infomask |= HEAP_XMIN_FROZEN;
	tuple->t_choice.t_heap.t_xmin = 0;
}
