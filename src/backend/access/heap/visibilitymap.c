/*-------------------------------------------------------------------------
 *
 * visibilitymap.c
 *	  bitmap for tracking visibility of heap tuples
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/visibilitymap.c
 *
 * INTERFACE ROUTINES
 *		visibilitymap_clear  - clear bits for one page in the visibility map
 *		visibilitymap_pin	 - pin a map page for setting a bit
 *		visibilitymap_pin_ok - check whether correct map page is already pinned
 *		visibilitymap_set	 - set a bit in a previously pinned page
 *		visibilitymap_get_status - get status of bits
 *		visibilitymap_snap_acquire - acquire snapshot of visibility map
 *		visibilitymap_snap_strategy - set VACUUM's scanning strategy
 *		visibilitymap_snap_next - get next block to scan from vmsnap
 *		visibilitymap_snap_release - release previously acquired snapshot
 *		visibilitymap_count  - count number of bits set in visibility map
 *		visibilitymap_prepare_truncate -
 *			prepare for truncation of the visibility map
 *
 * NOTES
 *
 * The visibility map is a bitmap with two bits (all-visible and all-frozen)
 * per heap page. A set all-visible bit means that all tuples on the page are
 * known visible to all transactions, and therefore the page doesn't need to
 * be vacuumed. A set all-frozen bit means that all tuples on the page are
 * completely frozen, and therefore the page doesn't need to be vacuumed even
 * if whole table scanning vacuum is required (e.g. anti-wraparound vacuum).
 * The all-frozen bit must be set only when the page is already all-visible.
 *
 * The map is conservative in the sense that we make sure that whenever a bit
 * is set, we know the condition is true, but if a bit is not set, it might or
 * might not be true.
 *
 * Clearing visibility map bits is not separately WAL-logged.  The callers
 * must make sure that whenever a bit is cleared, the bit is cleared on WAL
 * replay of the updating operation as well.
 *
 * When we *set* a visibility map during VACUUM, we must write WAL.  This may
 * seem counterintuitive, since the bit is basically a hint: if it is clear,
 * it may still be the case that every tuple on the page is visible to all
 * transactions; we just don't know that for certain.  The difficulty is that
 * there are two bits which are typically set together: the PD_ALL_VISIBLE bit
 * on the page itself, and the visibility map bit.  If a crash occurs after the
 * visibility map page makes it to disk and before the updated heap page makes
 * it to disk, redo must set the bit on the heap page.  Otherwise, the next
 * insert, update, or delete on the heap page will fail to realize that the
 * visibility map bit must be cleared, possibly causing index-only scans to
 * return wrong answers.
 *
 * VACUUM will normally skip pages for which the visibility map bit is set;
 * such pages can't contain any dead tuples and therefore don't need vacuuming.
 * VACUUM uses a snapshot of the visibility map to avoid scanning pages whose
 * visibility map bit gets concurrently unset.  This also provides us with a
 * convenient way of performing I/O prefetching on behalf of VACUUM, since the
 * pages that VACUUM's first heap pass will scan are fully predetermined.
 *
 * LOCKING
 *
 * In heapam.c, whenever a page is modified so that not all tuples on the
 * page are visible to everyone anymore, the corresponding bit in the
 * visibility map is cleared. In order to be crash-safe, we need to do this
 * while still holding a lock on the heap page and in the same critical
 * section that logs the page modification. However, we don't want to hold
 * the buffer lock over any I/O that may be required to read in the visibility
 * map page.  To avoid this, we examine the heap page before locking it;
 * if the page-level PD_ALL_VISIBLE bit is set, we pin the visibility map
 * bit.  Then, we lock the buffer.  But this creates a race condition: there
 * is a possibility that in the time it takes to lock the buffer, the
 * PD_ALL_VISIBLE bit gets set.  If that happens, we have to unlock the
 * buffer, pin the visibility map page, and relock the buffer.  This shouldn't
 * happen often, because only VACUUM currently sets visibility map bits,
 * and the race will only occur if VACUUM processes a given page at almost
 * exactly the same time that someone tries to further modify it.
 *
 * To set a bit, you need to hold a lock on the heap page. That prevents
 * the race condition where VACUUM sees that all tuples on the page are
 * visible to everyone, but another backend modifies the page before VACUUM
 * sets the bit in the visibility map.
 *
 * When a bit is set, the LSN of the visibility map page is updated to make
 * sure that the visibility map update doesn't get written to disk before the
 * WAL record of the changes that made it possible to set the bit is flushed.
 * But when a bit is cleared, we don't have to do that because it's always
 * safe to clear a bit in the map from correctness point of view.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam_xlog.h"
#include "access/visibilitymap.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "storage/buffile.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/inval.h"
#include "utils/spccache.h"


/*#define TRACE_VISIBILITYMAP */

/*
 * Size of the bitmap on each visibility map page, in bytes. There's no
 * extra headers, so the whole page minus the standard page header is
 * used for the bitmap.
 */
#define MAPSIZE (BLCKSZ - MAXALIGN(SizeOfPageHeaderData))

/* Number of heap blocks we can represent in one byte */
#define HEAPBLOCKS_PER_BYTE (BITS_PER_BYTE / BITS_PER_HEAPBLOCK)

/* Number of heap blocks we can represent in one visibility map page. */
#define HEAPBLOCKS_PER_PAGE (MAPSIZE * HEAPBLOCKS_PER_BYTE)

/* Mapping from heap block number to the right bit in the visibility map */
#define HEAPBLK_TO_MAPBLOCK(x) ((x) / HEAPBLOCKS_PER_PAGE)
#define HEAPBLK_TO_MAPBYTE(x) (((x) % HEAPBLOCKS_PER_PAGE) / HEAPBLOCKS_PER_BYTE)
#define HEAPBLK_TO_OFFSET(x) (((x) % HEAPBLOCKS_PER_BYTE) * BITS_PER_HEAPBLOCK)

/* Masks for counting subsets of bits in the visibility map. */
#define VISIBLE_MASK64	UINT64CONST(0x5555555555555555) /* The lower bit of each
														 * bit pair */
#define FROZEN_MASK64	UINT64CONST(0xaaaaaaaaaaaaaaaa) /* The upper bit of each
														 * bit pair */

/*
 * Prefetching of heap pages takes place as VACUUM requests the next block in
 * line from its visibility map snapshot
 *
 * XXX MIN_PREFETCH_SIZE of 32 is a little on the high side, but matches
 * hard-coded constant used by vacuumlazy.c when prefetching for rel
 * truncation.  Might be better to increase the maintenance_io_concurrency
 * default, or to do nothing like this at all.
 */
#define STAGED_BUFSIZE			(MAX_IO_CONCURRENCY * 2)
#define MIN_PREFETCH_SIZE		((BlockNumber) 32)

/*
 * Snapshot of visibility map at the start of a VACUUM operation
 */
struct vmsnapshot
{
	/* Target heap rel */
	Relation	rel;
	/* Scanning strategy used by VACUUM operation */
	vmstrategy	strat;
	/* Per-strategy final scanned_pages */
	BlockNumber rel_pages;
	BlockNumber scanned_pages_lazy;
	BlockNumber scanned_pages_eager;

	/*
	 * Materialized visibility map state.
	 *
	 * VM snapshots spill to a temp file when required.
	 */
	BlockNumber nvmpages;
	BufFile    *file;

	/*
	 * Prefetch distance, used to perform I/O prefetching of heap pages
	 */
	int			prefetch_distance;

	/* Current VM page cached */
	BlockNumber curvmpage;
	char	   *rawmap;
	PGAlignedBlock vmpage;

	/* Staging area for blocks returned to VACUUM */
	BlockNumber staged[STAGED_BUFSIZE];
	int			current_nblocks_staged;

	/*
	 * Next block from range of rel_pages to consider placing in staged block
	 * array (it will be placed there if it's going to be scanned by VACUUM)
	 */
	BlockNumber next_block;

	/*
	 * Number of blocks that we still need to return, and number of blocks
	 * that we still need to prefetch
	 */
	BlockNumber scanned_pages_to_return;
	BlockNumber scanned_pages_to_prefetch;

	/* offset of next block in line to return (from staged) */
	int			next_return_idx;
	/* offset of next block in line to prefetch (from staged) */
	int			next_prefetch_idx;
	/* offset of first garbage/invalid element (from staged) */
	int			first_invalid_idx;
};


/* prototypes for internal routines */
static Buffer vm_readbuf(Relation rel, BlockNumber blkno, bool extend);
static void vm_extend(Relation rel, BlockNumber vm_nblocks);
static void vm_snap_stage_blocks(vmsnapshot *vmsnap);
static uint8 vm_snap_get_status(vmsnapshot *vmsnap, BlockNumber heapBlk);


/*
 *	visibilitymap_clear - clear specified bits for one page in visibility map
 *
 * You must pass a buffer containing the correct map page to this function.
 * Call visibilitymap_pin first to pin the right one. This function doesn't do
 * any I/O.  Returns true if any bits have been cleared and false otherwise.
 */
bool
visibilitymap_clear(Relation rel, BlockNumber heapBlk, Buffer vmbuf, uint8 flags)
{
	BlockNumber mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);
	int			mapByte = HEAPBLK_TO_MAPBYTE(heapBlk);
	int			mapOffset = HEAPBLK_TO_OFFSET(heapBlk);
	uint8		mask = flags << mapOffset;
	char	   *map;
	bool		cleared = false;

	/* Must never clear all_visible bit while leaving all_frozen bit set */
	Assert(flags & VISIBILITYMAP_VALID_BITS);
	Assert(flags != VISIBILITYMAP_ALL_VISIBLE);

#ifdef TRACE_VISIBILITYMAP
	elog(DEBUG1, "vm_clear %s %d", RelationGetRelationName(rel), heapBlk);
#endif

	if (!BufferIsValid(vmbuf) || BufferGetBlockNumber(vmbuf) != mapBlock)
		elog(ERROR, "wrong buffer passed to visibilitymap_clear");

	LockBuffer(vmbuf, BUFFER_LOCK_EXCLUSIVE);
	map = PageGetContents(BufferGetPage(vmbuf));

	if (map[mapByte] & mask)
	{
		map[mapByte] &= ~mask;

		MarkBufferDirty(vmbuf);
		cleared = true;
	}

	LockBuffer(vmbuf, BUFFER_LOCK_UNLOCK);

	return cleared;
}

/*
 *	visibilitymap_pin - pin a map page for setting a bit
 *
 * Setting a bit in the visibility map is a two-phase operation. First, call
 * visibilitymap_pin, to pin the visibility map page containing the bit for
 * the heap page. Because that can require I/O to read the map page, you
 * shouldn't hold a lock on the heap page while doing that. Then, call
 * visibilitymap_set to actually set the bit.
 *
 * On entry, *vmbuf should be InvalidBuffer or a valid buffer returned by
 * an earlier call to visibilitymap_pin or visibilitymap_get_status on the same
 * relation. On return, *vmbuf is a valid buffer with the map page containing
 * the bit for heapBlk.
 *
 * If the page doesn't exist in the map file yet, it is extended.
 */
void
visibilitymap_pin(Relation rel, BlockNumber heapBlk, Buffer *vmbuf)
{
	BlockNumber mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);

	/* Reuse the old pinned buffer if possible */
	if (BufferIsValid(*vmbuf))
	{
		if (BufferGetBlockNumber(*vmbuf) == mapBlock)
			return;

		ReleaseBuffer(*vmbuf);
	}
	*vmbuf = vm_readbuf(rel, mapBlock, true);
}

/*
 *	visibilitymap_pin_ok - do we already have the correct page pinned?
 *
 * On entry, vmbuf should be InvalidBuffer or a valid buffer returned by
 * an earlier call to visibilitymap_pin or visibilitymap_get_status on the same
 * relation.  The return value indicates whether the buffer covers the
 * given heapBlk.
 */
bool
visibilitymap_pin_ok(BlockNumber heapBlk, Buffer vmbuf)
{
	BlockNumber mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);

	return BufferIsValid(vmbuf) && BufferGetBlockNumber(vmbuf) == mapBlock;
}

/*
 *	visibilitymap_set - set bit(s) on a previously pinned page
 *
 * recptr is the LSN of the XLOG record we're replaying, if we're in recovery,
 * or InvalidXLogRecPtr in normal running.  The VM page LSN is advanced to the
 * one provided; in normal running, we generate a new XLOG record and set the
 * page LSN to that value (though the heap page's LSN may *not* be updated;
 * see below).  cutoff_xid is the largest xmin on the page being marked
 * all-visible; it is needed for Hot Standby, and can be InvalidTransactionId
 * if the page contains no tuples.  It can also be set to InvalidTransactionId
 * when a page that is already all-visible is being marked all-frozen.
 *
 * Caller is expected to set the heap page's PD_ALL_VISIBLE bit before calling
 * this function. Except in recovery, caller should also pass the heap
 * buffer. When checksums are enabled and we're not in recovery, we must add
 * the heap buffer to the WAL chain to protect it from being torn.
 *
 * You must pass a buffer containing the correct map page to this function.
 * Call visibilitymap_pin first to pin the right one. This function doesn't do
 * any I/O.
 */
void
visibilitymap_set(Relation rel, BlockNumber heapBlk, Buffer heapBuf,
				  XLogRecPtr recptr, Buffer vmBuf, TransactionId cutoff_xid,
				  uint8 flags)
{
	BlockNumber mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);
	uint32		mapByte = HEAPBLK_TO_MAPBYTE(heapBlk);
	uint8		mapOffset = HEAPBLK_TO_OFFSET(heapBlk);
	Page		page;
	uint8	   *map;

#ifdef TRACE_VISIBILITYMAP
	elog(DEBUG1, "vm_set %s %d", RelationGetRelationName(rel), heapBlk);
#endif

	Assert(InRecovery || XLogRecPtrIsInvalid(recptr));
	Assert(InRecovery || PageIsAllVisible((Page) BufferGetPage(heapBuf)));

	/* Must never set all_frozen bit without also setting all_visible bit */
	Assert(flags & VISIBILITYMAP_VALID_BITS);
	Assert(flags != VISIBILITYMAP_ALL_FROZEN);

	/* Check that we have the right heap page pinned, if present */
	if (BufferIsValid(heapBuf) && BufferGetBlockNumber(heapBuf) != heapBlk)
		elog(ERROR, "wrong heap buffer passed to visibilitymap_set");

	/* Check that we have the right VM page pinned */
	if (!BufferIsValid(vmBuf) || BufferGetBlockNumber(vmBuf) != mapBlock)
		elog(ERROR, "wrong VM buffer passed to visibilitymap_set");

	page = BufferGetPage(vmBuf);
	map = (uint8 *) PageGetContents(page);
	LockBuffer(vmBuf, BUFFER_LOCK_EXCLUSIVE);

	if (flags != (map[mapByte] >> mapOffset & VISIBILITYMAP_VALID_BITS))
	{
		START_CRIT_SECTION();

		map[mapByte] |= (flags << mapOffset);
		MarkBufferDirty(vmBuf);

		if (RelationNeedsWAL(rel))
		{
			if (XLogRecPtrIsInvalid(recptr))
			{
				Assert(!InRecovery);
				recptr = log_heap_visible(rel->rd_locator, heapBuf, vmBuf,
										  cutoff_xid, flags);

				/*
				 * If data checksums are enabled (or wal_log_hints=on), we
				 * need to protect the heap page from being torn.
				 *
				 * If not, then we must *not* update the heap page's LSN. In
				 * this case, the FPI for the heap page was omitted from the
				 * WAL record inserted above, so it would be incorrect to
				 * update the heap page's LSN.
				 */
				if (XLogHintBitIsNeeded())
				{
					Page		heapPage = BufferGetPage(heapBuf);

					PageSetLSN(heapPage, recptr);
				}
			}
			PageSetLSN(page, recptr);
		}

		END_CRIT_SECTION();
	}

	LockBuffer(vmBuf, BUFFER_LOCK_UNLOCK);
}

/*
 *	visibilitymap_get_status - get status of bits
 *
 * Are all tuples on heapBlk visible to all or are marked frozen, according
 * to the visibility map?
 *
 * On entry, *vmbuf should be InvalidBuffer or a valid buffer returned by an
 * earlier call to visibilitymap_pin or visibilitymap_get_status on the same
 * relation. On return, *vmbuf is a valid buffer with the map page containing
 * the bit for heapBlk, or InvalidBuffer. The caller is responsible for
 * releasing *vmbuf after it's done testing and setting bits.
 *
 * NOTE: This function is typically called without a lock on the heap page,
 * so somebody else could change the bit just after we look at it.  In fact,
 * since we don't lock the visibility map page either, it's even possible that
 * someone else could have changed the bit just before we look at it, but yet
 * we might see the old value.  It is the caller's responsibility to deal with
 * all concurrency issues!
 */
uint8
visibilitymap_get_status(Relation rel, BlockNumber heapBlk, Buffer *vmbuf)
{
	BlockNumber mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);
	uint32		mapByte = HEAPBLK_TO_MAPBYTE(heapBlk);
	uint8		mapOffset = HEAPBLK_TO_OFFSET(heapBlk);
	char	   *map;
	uint8		result;

#ifdef TRACE_VISIBILITYMAP
	elog(DEBUG1, "vm_get_status %s %d", RelationGetRelationName(rel), heapBlk);
#endif

	/* Reuse the old pinned buffer if possible */
	if (BufferIsValid(*vmbuf))
	{
		if (BufferGetBlockNumber(*vmbuf) != mapBlock)
		{
			ReleaseBuffer(*vmbuf);
			*vmbuf = InvalidBuffer;
		}
	}

	if (!BufferIsValid(*vmbuf))
	{
		*vmbuf = vm_readbuf(rel, mapBlock, false);
		if (!BufferIsValid(*vmbuf))
			return false;
	}

	map = PageGetContents(BufferGetPage(*vmbuf));

	/*
	 * A single byte read is atomic.  There could be memory-ordering effects
	 * here, but for performance reasons we make it the caller's job to worry
	 * about that.
	 */
	result = ((map[mapByte] >> mapOffset) & VISIBILITYMAP_VALID_BITS);
	return result;
}

/*
 *	visibilitymap_snap_acquire - get read-only snapshot of visibility map
 *
 * Initializes VACUUM caller's snapshot, allocating memory in current context.
 * Used by VACUUM to determine which pages it must scan up front.
 *
 * Set scanned_pages_lazy and scanned_pages_eager to help VACUUM decide on its
 * scanning strategy.  These are VACUUM's scanned_pages when it opts to skip
 * all eligible pages and scanned_pages when it opts to just skip all-frozen
 * pages, respectively.
 *
 * Caller finalizes scanning strategy by calling visibilitymap_snap_strategy.
 * This determines the kind of blocks visibilitymap_snap_next should indicate
 * need to be scanned by VACUUM.
 */
vmsnapshot *
visibilitymap_snap_acquire(Relation rel, BlockNumber rel_pages,
						   BlockNumber *scanned_pages_lazy,
						   BlockNumber *scanned_pages_eager)
{
	BlockNumber nvmpages = 0,
				mapBlockLast = 0,
				all_visible = 0,
				all_frozen = 0;
	uint8		mapbits_last_page = 0;
	vmsnapshot *vmsnap;

#ifdef TRACE_VISIBILITYMAP
	elog(DEBUG1, "visibilitymap_snap_acquire %s %u",
		 RelationGetRelationName(rel), rel_pages);
#endif

	/*
	 * Allocate space for VM pages up to and including those required to have
	 * bits for the would-be heap block that is just beyond rel_pages
	 */
	if (rel_pages > 0)
	{
		mapBlockLast = HEAPBLK_TO_MAPBLOCK(rel_pages - 1);
		nvmpages = mapBlockLast + 1;
	}

	/* Allocate and initialize VM snapshot state */
	vmsnap = palloc0(sizeof(vmsnapshot));
	vmsnap->rel = rel;
	vmsnap->strat = VMSNAP_SCAN_ALL;	/* for now */
	vmsnap->rel_pages = rel_pages;	/* scanned_pages for VMSNAP_SCAN_ALL */
	vmsnap->scanned_pages_lazy = 0;
	vmsnap->scanned_pages_eager = 0;

	/*
	 * vmsnap temp file state.
	 *
	 * Only relations large enough to need more than one visibility map page
	 * use a temp file (cannot wholly rely on vmsnap's single page cache).
	 */
	vmsnap->nvmpages = nvmpages;
	vmsnap->file = NULL;
	if (nvmpages > 1)
		vmsnap->file = BufFileCreateTemp(false);
	vmsnap->prefetch_distance = 0;
#ifdef USE_PREFETCH
	vmsnap->prefetch_distance =
		get_tablespace_maintenance_io_concurrency(rel->rd_rel->reltablespace);
#endif
	vmsnap->prefetch_distance = Max(vmsnap->prefetch_distance, MIN_PREFETCH_SIZE);

	/* cache of VM pages read from temp file */
	vmsnap->curvmpage = 0;
	vmsnap->rawmap = NULL;

	/* staged blocks array state */
	vmsnap->current_nblocks_staged = 0;
	vmsnap->next_block = 0;
	vmsnap->scanned_pages_to_return = 0;
	vmsnap->scanned_pages_to_prefetch = 0;
	/* Offsets into staged blocks array */
	vmsnap->next_return_idx = 0;
	vmsnap->next_prefetch_idx = 0;
	vmsnap->first_invalid_idx = 0;

	for (BlockNumber mapBlock = 0; mapBlock <= mapBlockLast; mapBlock++)
	{
		Buffer		mapBuffer;
		char	   *map;
		uint64	   *umap;

		mapBuffer = vm_readbuf(rel, mapBlock, false);
		if (!BufferIsValid(mapBuffer))
		{
			/*
			 * Not all VM pages available.  Remember that, so that we'll treat
			 * relevant heap pages as not all-visible/all-frozen when asked.
			 */
			vmsnap->nvmpages = mapBlock;
			break;
		}

		/* Cache page locally */
		LockBuffer(mapBuffer, BUFFER_LOCK_SHARE);
		memcpy(vmsnap->vmpage.data, BufferGetPage(mapBuffer), BLCKSZ);
		UnlockReleaseBuffer(mapBuffer);

		/* Finish off this VM page using snapshot's vmpage cache */
		vmsnap->curvmpage = mapBlock;
		vmsnap->rawmap = map = PageGetContents(vmsnap->vmpage.data);
		umap = (uint64 *) map;

		if (mapBlock == mapBlockLast)
		{
			uint32		mapByte;
			uint8		mapOffset;

			/*
			 * The last VM page requires some extra steps.
			 *
			 * First get the status of the last heap page (page in the range
			 * of rel_pages) in passing.
			 */
			Assert(mapBlock == HEAPBLK_TO_MAPBLOCK(rel_pages - 1));
			mapByte = HEAPBLK_TO_MAPBYTE(rel_pages - 1);
			mapOffset = HEAPBLK_TO_OFFSET(rel_pages - 1);
			mapbits_last_page = ((map[mapByte] >> mapOffset) &
								 VISIBILITYMAP_VALID_BITS);

			/*
			 * Also defensively "truncate" our local copy of the last page in
			 * order to reliably exclude heap pages beyond the range of
			 * rel_pages.  This is just paranoia.
			 */
			mapByte = HEAPBLK_TO_MAPBYTE(rel_pages);
			mapOffset = HEAPBLK_TO_OFFSET(rel_pages);
			if (mapByte != 0 || mapOffset != 0)
			{
				MemSet(&map[mapByte + 1], 0, MAPSIZE - (mapByte + 1));
				map[mapByte] &= (1 << mapOffset) - 1;
			}
		}

		/* Maintain count of all-frozen and all-visible pages */
		for (int i = 0; i < MAPSIZE / sizeof(uint64); i++)
		{
			all_visible += pg_popcount64(umap[i] & VISIBLE_MASK64);
			all_frozen += pg_popcount64(umap[i] & FROZEN_MASK64);
		}

		/* Finally, write out vmpage cache VM page to vmsnap's temp file */
		if (vmsnap->file)
			BufFileWrite(vmsnap->file, vmsnap->vmpage.data, BLCKSZ);
	}

	/*
	 * Should always have at least as many all_visible pages as all_frozen
	 * pages.  Even still, we generally only interpret a page as all-frozen
	 * when both the all-visible and all-frozen bits are set together.  Clamp
	 * so that we'll avoid giving our caller an obviously bogus summary of the
	 * visibility map when certain pages only have their all-frozen bit set.
	 * More paranoia.
	 */
	Assert(all_frozen <= all_visible && all_visible <= rel_pages);
	all_frozen = Min(all_frozen, all_visible);

	/*
	 * Done copying all VM pages from authoritative VM into a VM snapshot.
	 *
	 * Figure out the final scanned_pages for the two skipping policies that
	 * we might use: skipallvis (skip both all-frozen and all-visible) and
	 * skipallfrozen (just skip all-frozen).
	 */
	vmsnap->scanned_pages_lazy = rel_pages - all_visible;
	vmsnap->scanned_pages_eager = rel_pages - all_frozen;

	/*
	 * When the last page is skippable in principle, it still won't be treated
	 * as skippable by visibilitymap_snap_next, which recognizes the last page
	 * as a special case.  Compensate by incrementing each scanning strategy's
	 * scanned_pages as needed to avoid counting the last page as skippable.
	 *
	 * As usual we expect that the all-frozen bit can only be set alongside
	 * the all-visible bit (for any given page), but only interpret a page as
	 * truly all-frozen when both of its VM bits are set together.
	 */
	if (mapbits_last_page & VISIBILITYMAP_ALL_VISIBLE)
	{
		vmsnap->scanned_pages_lazy++;
		if (mapbits_last_page & VISIBILITYMAP_ALL_FROZEN)
			vmsnap->scanned_pages_eager++;
	}

	*scanned_pages_lazy = vmsnap->scanned_pages_lazy;
	*scanned_pages_eager = vmsnap->scanned_pages_eager;

	return vmsnap;
}

/*
 *	visibilitymap_snap_strategy -- determine VACUUM's scanning strategy.
 *
 * VACUUM chooses a vmsnap strategy according to priorities around advancing
 * relfrozenxid.  See visibilitymap_snap_acquire.
 */
void
visibilitymap_snap_strategy(vmsnapshot *vmsnap, vmstrategy strat)
{
	int			nprefetch;

	/* Remember final scanning strategy */
	vmsnap->strat = strat;

	if (vmsnap->strat == VMSNAP_SCAN_LAZY)
		vmsnap->scanned_pages_to_return = vmsnap->scanned_pages_lazy;
	else if (vmsnap->strat == VMSNAP_SCAN_EAGER)
		vmsnap->scanned_pages_to_return = vmsnap->scanned_pages_eager;
	else
		vmsnap->scanned_pages_to_return = vmsnap->rel_pages;

	vmsnap->scanned_pages_to_prefetch = vmsnap->scanned_pages_to_return;

#ifdef TRACE_VISIBILITYMAP
	elog(DEBUG1, "visibilitymap_snap_strategy %s %d %u",
		 RelationGetRelationName(vmsnap->rel), (int) strat,
		 vmsnap->scanned_pages_to_return);
#endif

	/*
	 * Stage blocks (may have to read from temp file).
	 *
	 * We rely on the assumption that we'll always have a large enough staged
	 * blocks array to accommodate any possible prefetch distance.
	 */
	vm_snap_stage_blocks(vmsnap);

	nprefetch = Min(vmsnap->current_nblocks_staged, vmsnap->prefetch_distance);
#ifdef USE_PREFETCH
	for (int i = 0; i < nprefetch; i++)
	{
		PrefetchBuffer(vmsnap->rel, MAIN_FORKNUM, vmsnap->staged[i]);
	}
#endif

	vmsnap->scanned_pages_to_prefetch -= nprefetch;
	vmsnap->next_prefetch_idx += nprefetch;
}

/*
 *	visibilitymap_snap_next -- get next block to scan from vmsnap.
 *
 * Returns next block in line for VACUUM to scan according to vmsnap.  Caller
 * skips any and all blocks preceding returned block.
 *
 * VACUUM always scans the last page to determine whether it has tuples.  This
 * is useful as a way of avoiding certain pathological cases with heap rel
 * truncation.  We always return the final block (rel_pages - 1) here last.
 */
BlockNumber
visibilitymap_snap_next(vmsnapshot *vmsnap)
{
	BlockNumber next_block_to_scan;

	if (vmsnap->scanned_pages_to_return == 0)
		return InvalidBlockNumber;

	/* Prepare to return this block */
	next_block_to_scan = vmsnap->staged[vmsnap->next_return_idx++];
	vmsnap->current_nblocks_staged--;
	vmsnap->scanned_pages_to_return--;

	/*
	 * Did the staged blocks array just run out of blocks to return to caller,
	 * or do we need to stage more blocks for I/O prefetching purposes?
	 */
	Assert(vmsnap->next_prefetch_idx <= vmsnap->first_invalid_idx);
	if ((vmsnap->current_nblocks_staged == 0 &&
		 vmsnap->scanned_pages_to_return > 0) ||
		(vmsnap->next_prefetch_idx == vmsnap->first_invalid_idx &&
		 vmsnap->scanned_pages_to_prefetch > 0))
	{
		if (vmsnap->current_nblocks_staged > 0)
		{
			/*
			 * We've run out of prefetchable blocks, but still have some
			 * non-returned blocks.  Shift existing blocks to the start of the
			 * array.  The newly staged blocks go after these ones.
			 */
			memmove(&vmsnap->staged[0],
					&vmsnap->staged[vmsnap->next_return_idx],
					sizeof(BlockNumber) * vmsnap->current_nblocks_staged);
		}

		/*
		 * Reset offsets in staged blocks array, while accounting for likely
		 * presence of preexisting blocks that have already been prefetched
		 * but have yet to be returned to VACUUM caller
		 */
		vmsnap->next_prefetch_idx -= vmsnap->next_return_idx;
		vmsnap->first_invalid_idx -= vmsnap->next_return_idx;
		vmsnap->next_return_idx = 0;

		/* Stage more blocks (may have to read from temp file) */
		vm_snap_stage_blocks(vmsnap);
	}

	/*
	 * By here we're guaranteed to have at least one prefetchable block in the
	 * staged blocks array (unless we've already prefetched all blocks that
	 * will ever be returned to VACUUM caller)
	 */
	if (vmsnap->next_prefetch_idx < vmsnap->first_invalid_idx)
	{
#ifdef USE_PREFETCH
		/* Still have remaining blocks to prefetch, so prefetch next one */
		BlockNumber prefetch = vmsnap->staged[vmsnap->next_prefetch_idx++];

		PrefetchBuffer(vmsnap->rel, MAIN_FORKNUM, prefetch);
#else
		vmsnap->next_prefetch_idx++;
#endif
		Assert(vmsnap->current_nblocks_staged > 1);
		Assert(vmsnap->scanned_pages_to_prefetch > 0);
		vmsnap->scanned_pages_to_prefetch--;
	}
	else
	{
		Assert(vmsnap->scanned_pages_to_prefetch == 0);
	}

#ifdef TRACE_VISIBILITYMAP
	elog(DEBUG1, "visibilitymap_snap_next %s %u",
		 RelationGetRelationName(vmsnap->rel), next_block_to_scan);
#endif

	return next_block_to_scan;
}

/*
 *	visibilitymap_snap_release - release previously acquired snapshot
 *
 * Frees resources allocated in visibilitymap_snap_acquire for VACUUM.
 */
void
visibilitymap_snap_release(vmsnapshot *vmsnap)
{
	Assert(vmsnap->scanned_pages_to_return == 0);
	if (vmsnap->file)
		BufFileClose(vmsnap->file);
	pfree(vmsnap);
}

/*
 *	visibilitymap_count  - count number of bits set in visibility map
 *
 * Note: we ignore the possibility of race conditions when the table is being
 * extended concurrently with the call.  New pages added to the table aren't
 * going to be marked all-visible or all-frozen, so they won't affect the result.
 */
void
visibilitymap_count(Relation rel, BlockNumber *all_visible, BlockNumber *all_frozen)
{
	BlockNumber mapBlock;
	BlockNumber nvisible = 0;
	BlockNumber nfrozen = 0;

	/* all_visible must be specified */
	Assert(all_visible);

	for (mapBlock = 0;; mapBlock++)
	{
		Buffer		mapBuffer;
		uint64	   *map;
		int			i;

		/*
		 * Read till we fall off the end of the map.  We assume that any extra
		 * bytes in the last page are zeroed, so we don't bother excluding
		 * them from the count.
		 */
		mapBuffer = vm_readbuf(rel, mapBlock, false);
		if (!BufferIsValid(mapBuffer))
			break;

		/*
		 * We choose not to lock the page, since the result is going to be
		 * immediately stale anyway if anyone is concurrently setting or
		 * clearing bits, and we only really need an approximate value.
		 */
		map = (uint64 *) PageGetContents(BufferGetPage(mapBuffer));

		StaticAssertStmt(MAPSIZE % sizeof(uint64) == 0,
						 "unsupported MAPSIZE");
		if (all_frozen == NULL)
		{
			for (i = 0; i < MAPSIZE / sizeof(uint64); i++)
				nvisible += pg_popcount64(map[i] & VISIBLE_MASK64);
		}
		else
		{
			for (i = 0; i < MAPSIZE / sizeof(uint64); i++)
			{
				nvisible += pg_popcount64(map[i] & VISIBLE_MASK64);
				nfrozen += pg_popcount64(map[i] & FROZEN_MASK64);
			}
		}

		ReleaseBuffer(mapBuffer);
	}

	*all_visible = nvisible;
	if (all_frozen)
		*all_frozen = nfrozen;
}

/*
 *	visibilitymap_prepare_truncate -
 *			prepare for truncation of the visibility map
 *
 * nheapblocks is the new size of the heap.
 *
 * Return the number of blocks of new visibility map.
 * If it's InvalidBlockNumber, there is nothing to truncate;
 * otherwise the caller is responsible for calling smgrtruncate()
 * to truncate the visibility map pages.
 */
BlockNumber
visibilitymap_prepare_truncate(Relation rel, BlockNumber nheapblocks)
{
	BlockNumber newnblocks;

	/* last remaining block, byte, and bit */
	BlockNumber truncBlock = HEAPBLK_TO_MAPBLOCK(nheapblocks);
	uint32		truncByte = HEAPBLK_TO_MAPBYTE(nheapblocks);
	uint8		truncOffset = HEAPBLK_TO_OFFSET(nheapblocks);

#ifdef TRACE_VISIBILITYMAP
	elog(DEBUG1, "vm_truncate %s %d", RelationGetRelationName(rel), nheapblocks);
#endif

	/*
	 * If no visibility map has been created yet for this relation, there's
	 * nothing to truncate.
	 */
	if (!smgrexists(RelationGetSmgr(rel), VISIBILITYMAP_FORKNUM))
		return InvalidBlockNumber;

	/*
	 * Unless the new size is exactly at a visibility map page boundary, the
	 * tail bits in the last remaining map page, representing truncated heap
	 * blocks, need to be cleared. This is not only tidy, but also necessary
	 * because we don't get a chance to clear the bits if the heap is extended
	 * again.
	 */
	if (truncByte != 0 || truncOffset != 0)
	{
		Buffer		mapBuffer;
		Page		page;
		char	   *map;

		newnblocks = truncBlock + 1;

		mapBuffer = vm_readbuf(rel, truncBlock, false);
		if (!BufferIsValid(mapBuffer))
		{
			/* nothing to do, the file was already smaller */
			return InvalidBlockNumber;
		}

		page = BufferGetPage(mapBuffer);
		map = PageGetContents(page);

		LockBuffer(mapBuffer, BUFFER_LOCK_EXCLUSIVE);

		/* NO EREPORT(ERROR) from here till changes are logged */
		START_CRIT_SECTION();

		/* Clear out the unwanted bytes. */
		MemSet(&map[truncByte + 1], 0, MAPSIZE - (truncByte + 1));

		/*----
		 * Mask out the unwanted bits of the last remaining byte.
		 *
		 * ((1 << 0) - 1) = 00000000
		 * ((1 << 1) - 1) = 00000001
		 * ...
		 * ((1 << 6) - 1) = 00111111
		 * ((1 << 7) - 1) = 01111111
		 *----
		 */
		map[truncByte] &= (1 << truncOffset) - 1;

		/*
		 * Truncation of a relation is WAL-logged at a higher-level, and we
		 * will be called at WAL replay. But if checksums are enabled, we need
		 * to still write a WAL record to protect against a torn page, if the
		 * page is flushed to disk before the truncation WAL record. We cannot
		 * use MarkBufferDirtyHint here, because that will not dirty the page
		 * during recovery.
		 */
		MarkBufferDirty(mapBuffer);
		if (!InRecovery && RelationNeedsWAL(rel) && XLogHintBitIsNeeded())
			log_newpage_buffer(mapBuffer, false);

		END_CRIT_SECTION();

		UnlockReleaseBuffer(mapBuffer);
	}
	else
		newnblocks = truncBlock;

	if (smgrnblocks(RelationGetSmgr(rel), VISIBILITYMAP_FORKNUM) <= newnblocks)
	{
		/* nothing to do, the file was already smaller than requested size */
		return InvalidBlockNumber;
	}

	return newnblocks;
}

/*
 * Read a visibility map page.
 *
 * If the page doesn't exist, InvalidBuffer is returned, or if 'extend' is
 * true, the visibility map file is extended.
 */
static Buffer
vm_readbuf(Relation rel, BlockNumber blkno, bool extend)
{
	Buffer		buf;
	SMgrRelation reln;

	/*
	 * Caution: re-using this smgr pointer could fail if the relcache entry
	 * gets closed.  It's safe as long as we only do smgr-level operations
	 * between here and the last use of the pointer.
	 */
	reln = RelationGetSmgr(rel);

	/*
	 * If we haven't cached the size of the visibility map fork yet, check it
	 * first.
	 */
	if (reln->smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] == InvalidBlockNumber)
	{
		if (smgrexists(reln, VISIBILITYMAP_FORKNUM))
			smgrnblocks(reln, VISIBILITYMAP_FORKNUM);
		else
			reln->smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 0;
	}

	/* Handle requests beyond EOF */
	if (blkno >= reln->smgr_cached_nblocks[VISIBILITYMAP_FORKNUM])
	{
		if (extend)
			vm_extend(rel, blkno + 1);
		else
			return InvalidBuffer;
	}

	/*
	 * Use ZERO_ON_ERROR mode, and initialize the page if necessary. It's
	 * always safe to clear bits, so it's better to clear corrupt pages than
	 * error out.
	 *
	 * The initialize-the-page part is trickier than it looks, because of the
	 * possibility of multiple backends doing this concurrently, and our
	 * desire to not uselessly take the buffer lock in the normal path where
	 * the page is OK.  We must take the lock to initialize the page, so
	 * recheck page newness after we have the lock, in case someone else
	 * already did it.  Also, because we initially check PageIsNew with no
	 * lock, it's possible to fall through and return the buffer while someone
	 * else is still initializing the page (i.e., we might see pd_upper as set
	 * but other page header fields are still zeroes).  This is harmless for
	 * callers that will take a buffer lock themselves, but some callers
	 * inspect the page without any lock at all.  The latter is OK only so
	 * long as it doesn't depend on the page header having correct contents.
	 * Current usage is safe because PageGetContents() does not require that.
	 */
	buf = ReadBufferExtended(rel, VISIBILITYMAP_FORKNUM, blkno,
							 RBM_ZERO_ON_ERROR, NULL);
	if (PageIsNew(BufferGetPage(buf)))
	{
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		if (PageIsNew(BufferGetPage(buf)))
			PageInit(BufferGetPage(buf), BLCKSZ, 0);
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	}
	return buf;
}

/*
 * Ensure that the visibility map fork is at least vm_nblocks long, extending
 * it if necessary with zeroed pages.
 */
static void
vm_extend(Relation rel, BlockNumber vm_nblocks)
{
	BlockNumber vm_nblocks_now;
	PGAlignedBlock pg;
	SMgrRelation reln;

	PageInit((Page) pg.data, BLCKSZ, 0);

	/*
	 * We use the relation extension lock to lock out other backends trying to
	 * extend the visibility map at the same time. It also locks out extension
	 * of the main fork, unnecessarily, but extending the visibility map
	 * happens seldom enough that it doesn't seem worthwhile to have a
	 * separate lock tag type for it.
	 *
	 * Note that another backend might have extended or created the relation
	 * by the time we get the lock.
	 */
	LockRelationForExtension(rel, ExclusiveLock);

	/*
	 * Caution: re-using this smgr pointer could fail if the relcache entry
	 * gets closed.  It's safe as long as we only do smgr-level operations
	 * between here and the last use of the pointer.
	 */
	reln = RelationGetSmgr(rel);

	/*
	 * Create the file first if it doesn't exist.  If smgr_vm_nblocks is
	 * positive then it must exist, no need for an smgrexists call.
	 */
	if ((reln->smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] == 0 ||
		 reln->smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] == InvalidBlockNumber) &&
		!smgrexists(reln, VISIBILITYMAP_FORKNUM))
		smgrcreate(reln, VISIBILITYMAP_FORKNUM, false);

	/* Invalidate cache so that smgrnblocks() asks the kernel. */
	reln->smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = InvalidBlockNumber;
	vm_nblocks_now = smgrnblocks(reln, VISIBILITYMAP_FORKNUM);

	/* Now extend the file */
	while (vm_nblocks_now < vm_nblocks)
	{
		PageSetChecksumInplace((Page) pg.data, vm_nblocks_now);

		smgrextend(reln, VISIBILITYMAP_FORKNUM, vm_nblocks_now, pg.data, false);
		vm_nblocks_now++;
	}

	/*
	 * Send a shared-inval message to force other backends to close any smgr
	 * references they may have for this rel, which we are about to change.
	 * This is a useful optimization because it means that backends don't have
	 * to keep checking for creation or extension of the file, which happens
	 * infrequently.
	 */
	CacheInvalidateSmgr(reln->smgr_rlocator);

	UnlockRelationForExtension(rel, ExclusiveLock);
}

/*
 * Stage some heap blocks from vmsnap to return to VACUUM caller.
 *
 * Called when we completely run out of staged blocks to return to VACUUM, or
 * when vmsnap still has some pending staged blocks, but too few to be able to
 * prefetch incrementally as the remaining blocks are returned to VACUUM.
 */
static void
vm_snap_stage_blocks(vmsnapshot *vmsnap)
{
	Assert(vmsnap->current_nblocks_staged < STAGED_BUFSIZE);
	Assert(vmsnap->first_invalid_idx < STAGED_BUFSIZE);
	Assert(vmsnap->next_return_idx <= vmsnap->first_invalid_idx);
	Assert(vmsnap->next_prefetch_idx <= vmsnap->first_invalid_idx);

	while (vmsnap->next_block < vmsnap->rel_pages &&
		   vmsnap->current_nblocks_staged < STAGED_BUFSIZE)
	{
		for (;;)
		{
			uint8		mapbits = vm_snap_get_status(vmsnap,
													 vmsnap->next_block);

			if ((mapbits & VISIBILITYMAP_ALL_VISIBLE) == 0)
			{
				Assert((mapbits & VISIBILITYMAP_ALL_FROZEN) == 0);
				break;
			}

			/*
			 * Stop staging blocks just before final page, which must always
			 * be scanned by VACUUM
			 */
			if (vmsnap->next_block == vmsnap->rel_pages - 1)
				break;

			/* VMSNAP_SCAN_ALL forcing VACUUM to scan every page? */
			if (vmsnap->strat == VMSNAP_SCAN_ALL)
				break;

			/*
			 * Check if VACUUM must scan this page because it's not all-frozen
			 * and VACUUM opted to use VMSNAP_SCAN_EAGER strategy
			 */
			if ((mapbits & VISIBILITYMAP_ALL_FROZEN) == 0 &&
				vmsnap->strat == VMSNAP_SCAN_EAGER)
				break;

			/* VACUUM will skip this block -- so don't stage it for later */
			vmsnap->next_block++;
		}

		/* VACUUM will scan this block, so stage it for later */
		vmsnap->staged[vmsnap->first_invalid_idx++] = vmsnap->next_block++;
		vmsnap->current_nblocks_staged++;
	}
}

/*
 * Get status of bits from vm snapshot
 */
static uint8
vm_snap_get_status(vmsnapshot *vmsnap, BlockNumber heapBlk)
{
	BlockNumber mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);
	uint32		mapByte = HEAPBLK_TO_MAPBYTE(heapBlk);
	uint8		mapOffset = HEAPBLK_TO_OFFSET(heapBlk);

#ifdef TRACE_VISIBILITYMAP
	elog(DEBUG1, "vm_snap_get_status %u", heapBlk);
#endif

	/*
	 * If we didn't see the VM page when the snapshot was first acquired we
	 * defensively assume heapBlk not all-visible or all-frozen
	 */
	Assert(heapBlk <= vmsnap->rel_pages);
	if (unlikely(mapBlock >= vmsnap->nvmpages))
		return 0;

	/*
	 * Read from temp file when required.
	 *
	 * Although this routine supports random access, sequential access is
	 * expected.  We should only need to read each temp file page into cache
	 * at most once per VACUUM.
	 */
	if (unlikely(mapBlock != vmsnap->curvmpage))
	{
		if (BufFileSeekBlock(vmsnap->file, mapBlock) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not seek to block %u of vmsnap temporary file",
							mapBlock)));
		BufFileReadExact(vmsnap->file, vmsnap->vmpage.data, BLCKSZ);
		vmsnap->curvmpage = mapBlock;
		vmsnap->rawmap = PageGetContents(vmsnap->vmpage.data);
	}

	return ((vmsnap->rawmap[mapByte] >> mapOffset) & VISIBILITYMAP_VALID_BITS);
}
