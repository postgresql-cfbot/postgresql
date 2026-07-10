/*-------------------------------------------------------------------------
 *
 * itemptr.h
 *	  POSTGRES disk item pointer definitions.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/itemptr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ITEMPTR_H
#define ITEMPTR_H

#include "storage/block.h"
#include "storage/off.h"

/*
 * ItemPointer:
 *
 * This is a pointer to an item within a disk page of a known file
 * (for example, a cross-link from an index to its parent table).
 * ip_blkid tells us which block, ip_posid tells us which entry in
 * the linp (ItemIdData) array we want.
 *
 * Note: because there is an item pointer in each tuple header and index
 * tuple header on disk, it's very important not to waste space with
 * structure padding bytes.  The struct is designed to be six bytes long
 * (it contains three int16 fields) but a few compilers will pad it to
 * eight bytes unless coerced.  We apply appropriate persuasion where
 * possible.  If your compiler can't be made to play along, you'll waste
 * lots of space.
 */
typedef struct ItemPointerData
{
	BlockIdData ip_blkid;
	OffsetNumber ip_posid;
}

/* If compiler understands packed and aligned pragmas, use those */
#if defined(pg_attribute_packed) && defined(pg_attribute_aligned)
			pg_attribute_packed()
			pg_attribute_aligned(2)
#endif
ItemPointerData;

typedef ItemPointerData *ItemPointer;

/* ----------------
 *		special values used in heap tuples (t_ctid)
 * ----------------
 */

/*
 * If a heap tuple holds a speculative insertion token rather than a real
 * TID, ip_posid is set to SpecTokenOffsetNumber, and the token is stored in
 * ip_blkid. SpecTokenOffsetNumber must be higher than MaxOffsetNumber, so
 * that it can be distinguished from a valid offset number in a regular item
 * pointer.
 */
#define SpecTokenOffsetNumber		0xfffe

/*
 * When a tuple is moved to a different partition by UPDATE, the t_ctid of
 * the old tuple version is set to this magic value.
 */
#define MovedPartitionsOffsetNumber 0xfffd
#define MovedPartitionsBlockNumber	InvalidBlockNumber

/*
 * SIU (selective index update / HOT-indexed) "may-be-stale" marker bit.
 *
 * Set ONLY on a TID stored inside an INDEX TUPLE by a HOT-indexed update's
 * fresh entry -- never on a heap tuple's own t_ctid, and never on a plain
 * ItemPointer value passed around the executor/table AM (those still mean
 * exactly what they always have).  Such an entry points at a non-root,
 * heap-only chain member rather than at the chain's root the way every
 * other index entry does, so a bitmap scan must not trust a raw offset-
 * level match against it: BitmapAnd/BitmapOr intersect two indexes' TID
 * sets at block+offset granularity, and this entry's offset will not agree
 * with an unrelated, unchanged index's entry for the same logical row (see
 * the HOT-indexed design notes for the full false-negative scenario).  A
 * caller that finds this bit set on a heap-item TID must fall back to a
 * page-level (lossy) bitmap contribution instead of an exact one, forcing
 * the existing heap-side crossed-attribute recheck to make the final call.
 *
 * Bit 14 is free at every supported BLCKSZ: MaxOffsetNumber needs at most
 * 14 bits (BLCKSZ=32KB, the largest configurable block size) to represent
 * every legal offset, leaving bits 14-15 unused by the real offset value;
 * bit 15 is left alone because it is set in both of the sentinels above
 * (SpecTokenOffsetNumber, MovedPartitionsOffsetNumber), and staying clear
 * of that neighborhood avoids ever having to reason about the interaction.
 * nbtree separately overloads the top 4 bits of a *pivot or posting* tuple's
 * own t_tid (BT_STATUS_OFFSET_MASK, gated by INDEX_ALT_TID_MASK in t_info)
 * to mean something else entirely -- this bit is never read or set on that
 * field; it only ever applies to a genuine heap-row TID (including each
 * individual entry of a posting list's heap-TID array, which are ordinary
 * TIDs despite living inside a posting tuple).
 *
 * IMPORTANT: never mask this bit off unconditionally.  Both sentinels above
 * (SpecTokenOffsetNumber = 0xfffe, MovedPartitionsOffsetNumber = 0xfffd)
 * already have bit 14 set as part of their own value; blindly clearing it
 * would corrupt them into an unrecognized offset (this was caught by the
 * isolation suite: partition-key-update / MERGE tests silently stopped
 * detecting a concurrently-moved tuple).  A flagged real offset is always
 * well below the sentinel range even at the largest configurable BLCKSZ
 * (worst case 0x6000 at BLCKSZ=32KB, vs. sentinels at 0xfffd/0xfffe), so
 * ItemPointerOffsetNumberStrip only clears the bit when the raw value is
 * below that range; a sentinel passes through unchanged.
 */
#define ItemPointerSIUMaybeStaleFlag	((OffsetNumber) (1 << 14))

#define ItemPointerOffsetNumberMask	((OffsetNumber) ~ItemPointerSIUMaybeStaleFlag)

/*
 * ItemPointerOffsetNumberStrip
 *		Strip the SIU may-be-stale bit from a raw ip_posid value, UNLESS that
 *		value is one of the reserved sentinels (SpecTokenOffsetNumber,
 *		MovedPartitionsOffsetNumber), which must pass through untouched.
 */
static inline OffsetNumber
ItemPointerOffsetNumberStrip(OffsetNumber raw)
{
	if (raw >= MovedPartitionsOffsetNumber)	/* covers both sentinels */
		return raw;
	return (OffsetNumber) (raw & ItemPointerOffsetNumberMask);
}


/* ----------------
 *		support functions
 * ----------------
 */

/*
 * ItemPointerIsValid
 *		True iff the disk item pointer is not NULL.
 */
static inline bool
ItemPointerIsValid(const ItemPointerData *pointer)
{
	return pointer && pointer->ip_posid != 0;
}

/*
 * ItemPointerGetBlockNumberNoCheck
 *		Returns the block number of a disk item pointer.
 */
static inline BlockNumber
ItemPointerGetBlockNumberNoCheck(const ItemPointerData *pointer)
{
	return BlockIdGetBlockNumber(&pointer->ip_blkid);
}

/*
 * ItemPointerGetBlockNumber
 *		As above, but verifies that the item pointer looks valid.
 */
static inline BlockNumber
ItemPointerGetBlockNumber(const ItemPointerData *pointer)
{
	Assert(ItemPointerIsValid(pointer));
	return ItemPointerGetBlockNumberNoCheck(pointer);
}

/*
 * ItemPointerGetOffsetNumberNoCheck
 *		Returns the offset number of a disk item pointer, INCLUDING the SIU
 *		may-be-stale bit if set.  Only appropriate for code that explicitly
 *		wants the raw stored value (e.g. bitmap-scan code deciding whether to
 *		trust an exact match); anything that will use the result to locate a
 *		line pointer on a page must use ItemPointerGetOffsetNumber instead.
 */
static inline OffsetNumber
ItemPointerGetOffsetNumberNoCheck(const ItemPointerData *pointer)
{
	return pointer->ip_posid;
}

/*
 * ItemPointerGetOffsetNumber
 *		As above, but verifies that the item pointer looks valid, AND strips
 *		the SIU may-be-stale bit (see ItemPointerSIUMaybeStaleFlag) so every
 *		ordinary consumer -- anything that turns this into a page lookup, a
 *		comparison against another TID, or a value handed back to a caller
 *		that predates this bit's existence -- keeps seeing exactly the real
 *		offset it always has.  Only the handful of call sites that need to
 *		notice the flag (currently: each amgetbitmap implementation) use
 *		ItemPointerGetOffsetNumberNoCheck / ItemPointerIsSIUMaybeStale instead.
 */
static inline OffsetNumber
ItemPointerGetOffsetNumber(const ItemPointerData *pointer)
{
	Assert(ItemPointerIsValid(pointer));
	return ItemPointerOffsetNumberStrip(ItemPointerGetOffsetNumberNoCheck(pointer));
}

/*
 * ItemPointerIsSIUMaybeStale
 *		True iff this heap-row TID (as stored in an index tuple) was
 *		flagged by a HOT-indexed fresh-entry insert as pointing at a
 *		non-root chain member; see ItemPointerSIUMaybeStaleFlag.
 */
static inline bool
ItemPointerIsSIUMaybeStale(const ItemPointerData *pointer)
{
	return (pointer->ip_posid & ItemPointerSIUMaybeStaleFlag) != 0;
}

/*
 * ItemPointerSetSIUMaybeStale
 *		Set the SIU may-be-stale bit on a heap-row TID that will be stored
 *		in an index tuple.  Must only be called on a genuine heap TID, not
 *		on a repurposed nbtree pivot/posting t_tid.
 */
static inline void
ItemPointerSetSIUMaybeStale(ItemPointerData *pointer)
{
	Assert(pointer);
	/*
	 * The marker must only ever land on a genuine, in-range heap offset: a
	 * value in the sentinel range (SpecTokenOffsetNumber / MovedPartitions-
	 * OffsetNumber) already has bit 14 set and would be corrupted, and
	 * ItemPointerOffsetNumberStrip deliberately leaves such values untouched,
	 * so a later strip would not clear the flag.  Catch misuse in debug builds.
	 */
	Assert(ItemPointerGetOffsetNumberNoCheck(pointer) < MovedPartitionsOffsetNumber);
	pointer->ip_posid |= ItemPointerSIUMaybeStaleFlag;
}

/*
 * ItemPointerSet
 *		Sets a disk item pointer to the specified block and offset.
 */
static inline void
ItemPointerSet(ItemPointerData *pointer, BlockNumber blockNumber, OffsetNumber offNum)
{
	Assert(pointer);
	BlockIdSet(&pointer->ip_blkid, blockNumber);
	pointer->ip_posid = offNum;
}

/*
 * ItemPointerSetBlockNumber
 *		Sets a disk item pointer to the specified block.
 */
static inline void
ItemPointerSetBlockNumber(ItemPointerData *pointer, BlockNumber blockNumber)
{
	Assert(pointer);
	BlockIdSet(&pointer->ip_blkid, blockNumber);
}

/*
 * ItemPointerSetOffsetNumber
 *		Sets a disk item pointer to the specified offset.
 */
static inline void
ItemPointerSetOffsetNumber(ItemPointerData *pointer, OffsetNumber offsetNumber)
{
	Assert(pointer);
	pointer->ip_posid = offsetNumber;
}

/*
 * ItemPointerCopy
 *		Copies the contents of one disk item pointer to another.
 *
 * Should there ever be padding in an ItemPointer this would need to be handled
 * differently as it's used as hash key.
 */
static inline void
ItemPointerCopy(const ItemPointerData *fromPointer, ItemPointerData *toPointer)
{
	Assert(toPointer);
	Assert(fromPointer);
	*toPointer = *fromPointer;
}

/*
 * ItemPointerSetInvalid
 *		Sets a disk item pointer to be invalid.
 */
static inline void
ItemPointerSetInvalid(ItemPointerData *pointer)
{
	Assert(pointer);
	BlockIdSet(&pointer->ip_blkid, InvalidBlockNumber);
	pointer->ip_posid = InvalidOffsetNumber;
}

/*
 * ItemPointerIndicatesMovedPartitions
 *		True iff the block number indicates the tuple has moved to another
 *		partition.
 */
static inline bool
ItemPointerIndicatesMovedPartitions(const ItemPointerData *pointer)
{
	return
		ItemPointerGetOffsetNumber(pointer) == MovedPartitionsOffsetNumber &&
		ItemPointerGetBlockNumberNoCheck(pointer) == MovedPartitionsBlockNumber;
}

/*
 * ItemPointerSetMovedPartitions
 *		Indicate that the item referenced by the itempointer has moved into a
 *		different partition.
 */
static inline void
ItemPointerSetMovedPartitions(ItemPointerData *pointer)
{
	ItemPointerSet(pointer, MovedPartitionsBlockNumber, MovedPartitionsOffsetNumber);
}

/* ----------------
 *		externs
 * ----------------
 */

extern bool ItemPointerEquals(const ItemPointerData *pointer1, const ItemPointerData *pointer2);
extern int32 ItemPointerCompare(const ItemPointerData *arg1, const ItemPointerData *arg2);
extern void ItemPointerInc(ItemPointer pointer);
extern void ItemPointerDec(ItemPointer pointer);

/* ----------------
 *		Datum conversion functions
 * ----------------
 */

static inline ItemPointer
DatumGetItemPointer(Datum X)
{
	return (ItemPointer) DatumGetPointer(X);
}

static inline Datum
ItemPointerGetDatum(const ItemPointerData *X)
{
	return PointerGetDatum(X);
}

#define PG_GETARG_ITEMPOINTER(n) DatumGetItemPointer(PG_GETARG_DATUM(n))
#define PG_RETURN_ITEMPOINTER(x) return ItemPointerGetDatum(x)

#endif							/* ITEMPTR_H */
