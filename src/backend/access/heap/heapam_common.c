/*-------------------------------------------------------------------------
 *
 * heapam_common.c
 *	  heapam access method code that is common across all pluggable
 *	  storage modules
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/heapam_common.c
 *
 *
 * NOTES
 *	  This file contains the storage_ routines which implement
 *	  the POSTGRES storage access method used for all POSTGRES
 *	  relations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/heapam_common.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"

/* Static variables representing various special snapshot semantics */
SnapshotData SnapshotSelfData = {SELF_VISIBILITY};
SnapshotData SnapshotAnyData = {ANY_VISIBILITY};

/*
 * HeapTupleSetHintBits --- exported version of SetHintBits()
 *
 * This must be separate because of C99's brain-dead notions about how to
 * implement inline functions.
 */
void
HeapTupleSetHintBits(HeapTupleHeader tuple, Buffer buffer,
					 uint16 infomask, TransactionId xid)
{
	SetHintBits(tuple, buffer, infomask, xid);
}


/*
 * Is the tuple really only locked?  That is, is it not updated?
 *
 * It's easy to check just infomask bits if the locker is not a multi; but
 * otherwise we need to verify that the updating transaction has not aborted.
 *
 * This function is here because it follows the same time qualification rules
 * laid out at the top of this file.
 */
bool
HeapTupleHeaderIsOnlyLocked(HeapTupleHeader tuple)
{
	TransactionId xmax;

	/* if there's no valid Xmax, then there's obviously no update either */
	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return true;

	if (tuple->t_infomask & HEAP_XMAX_LOCK_ONLY)
		return true;

	/* invalid xmax means no update */
	if (!TransactionIdIsValid(HeapTupleHeaderGetRawXmax(tuple)))
		return true;

	/*
	 * if HEAP_XMAX_LOCK_ONLY is not set and not a multi, then this must
	 * necessarily have been updated
	 */
	if (!(tuple->t_infomask & HEAP_XMAX_IS_MULTI))
		return false;

	/* ... but if it's a multi, then perhaps the updating Xid aborted. */
	xmax = HeapTupleGetUpdateXid(tuple);

	/* not LOCKED_ONLY, so it has to have an xmax */
	Assert(TransactionIdIsValid(xmax));

	if (TransactionIdIsCurrentTransactionId(xmax))
		return false;
	if (TransactionIdIsInProgress(xmax))
		return false;
	if (TransactionIdDidCommit(xmax))
		return false;

	/*
	 * not current, not in progress, not committed -- must have aborted or
	 * crashed
	 */
	return true;
}


/*
 * HeapTupleIsSurelyDead
 *
 *	Cheaply determine whether a tuple is surely dead to all onlookers.
 *	We sometimes use this in lieu of HeapTupleSatisfiesVacuum when the
 *	tuple has just been tested by another visibility routine (usually
 *	HeapTupleSatisfiesMVCC) and, therefore, any hint bits that can be set
 *	should already be set.  We assume that if no hint bits are set, the xmin
 *	or xmax transaction is still running.  This is therefore faster than
 *	HeapTupleSatisfiesVacuum, because we don't consult PGXACT nor CLOG.
 *	It's okay to return FALSE when in doubt, but we must return TRUE only
 *	if the tuple is removable.
 */
bool
HeapTupleIsSurelyDead(HeapTuple htup, TransactionId OldestXmin)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	/*
	 * If the inserting transaction is marked invalid, then it aborted, and
	 * the tuple is definitely dead.  If it's marked neither committed nor
	 * invalid, then we assume it's still alive (since the presumption is that
	 * all relevant hint bits were just set moments ago).
	 */
	if (!HeapTupleHeaderXminCommitted(tuple))
		return HeapTupleHeaderXminInvalid(tuple) ? true : false;

	/*
	 * If the inserting transaction committed, but any deleting transaction
	 * aborted, the tuple is still alive.
	 */
	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return false;

	/*
	 * If the XMAX is just a lock, the tuple is still alive.
	 */
	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return false;

	/*
	 * If the Xmax is a MultiXact, it might be dead or alive, but we cannot
	 * know without checking pg_multixact.
	 */
	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
		return false;

	/* If deleter isn't known to have committed, assume it's still running. */
	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
		return false;

	/* Deleter committed, so tuple is dead if the XID is old enough. */
	return TransactionIdPrecedes(HeapTupleHeaderGetRawXmax(tuple), OldestXmin);
}

/*-----------------------
 *
 * Slot storage handler API
 * ----------------------
 */

static HeapTuple
heapam_get_tuple(TupleTableSlot *slot, bool palloc_copy)
{
	HeapTuple tup;
	HeapamTuple *stuple = (HeapamTuple *)slot->tts_storage;

	if (stuple)
	{
		if (stuple->hst_mintuple)
		{
			tup = heap_tuple_from_minimal_tuple(stuple->hst_mintuple);
		}
		else
		{
			if (!palloc_copy)
				tup = stuple->hst_heaptuple;
			else
				tup = heap_copytuple(stuple->hst_heaptuple);
		}
	}
	else
	{
		tup = heap_form_tuple(slot->tts_tupleDescriptor,
							slot->tts_values,
							slot->tts_isnull);
	}

	return tup;
}

static MinimalTuple
heapam_get_min_tuple(TupleTableSlot *slot, bool palloc_copy)
{
	MinimalTuple tup;
	HeapamTuple *stuple = (HeapamTuple *)slot->tts_storage;

	if (stuple)
	{
		if (stuple->hst_mintuple)
		{
			if (!palloc_copy)
				tup = stuple->hst_mintuple;
			else
				tup = heap_copy_minimal_tuple(stuple->hst_mintuple);
		}
		else
		{
			tup = minimal_tuple_from_heap_tuple(stuple->hst_heaptuple);
		}
	}
	else
	{
		tup = heap_form_minimal_tuple(slot->tts_tupleDescriptor,
									slot->tts_values,
									slot->tts_isnull);
	}

	return tup;
}


/*
 * slot_deform_tuple
 *		Given a TupleTableSlot, extract data from the slot's physical tuple
 *		into its Datum/isnull arrays.  Data is extracted up through the
 *		natts'th column (caller must ensure this is a legal column number).
 *
 *		This is essentially an incremental version of heap_deform_tuple:
 *		on each call we extract attributes up to the one needed, without
 *		re-computing information about previously extracted attributes.
 *		slot->tts_nvalid is the number of attributes already extracted.
 */
static void
slot_deform_tuple(TupleTableSlot *slot, int natts)
{
	HeapamTuple *stuple = (HeapamTuple *) slot->tts_storage;
	HeapTuple	tuple = stuple ? stuple->hst_heaptuple : NULL;
	TupleDesc	tupleDesc = slot->tts_tupleDescriptor;
	Datum	   *values = slot->tts_values;
	bool	   *isnull = slot->tts_isnull;
	HeapTupleHeader tup = tuple->t_data;
	bool		hasnulls = HeapTupleHasNulls(tuple);
	int			attnum;
	char	   *tp;				/* ptr to tuple data */
	long		off;			/* offset in tuple data */
	bits8	   *bp = tup->t_bits;	/* ptr to null bitmap in tuple */
	bool		slow;			/* can we use/set attcacheoff? */

	/*
	 * Check whether the first call for this tuple, and initialize or restore
	 * loop state.
	 */
	attnum = slot->tts_nvalid;
	if (attnum == 0)
	{
		/* Start from the first attribute */
		off = 0;
		slow = false;
	}
	else
	{
		/* Restore state from previous execution */
		off = stuple->hst_off;
		slow = stuple->hst_slow;
	}

	tp = (char *) tup + tup->t_hoff;

	for (; attnum < natts; attnum++)
	{
		Form_pg_attribute thisatt = TupleDescAttr(tupleDesc, attnum);

		if (hasnulls && att_isnull(attnum, bp))
		{
			values[attnum] = (Datum) 0;
			isnull[attnum] = true;
			slow = true;		/* can't use attcacheoff anymore */
			continue;
		}

		isnull[attnum] = false;

		if (!slow && thisatt->attcacheoff >= 0)
			off = thisatt->attcacheoff;
		else if (thisatt->attlen == -1)
		{
			/*
			 * We can only cache the offset for a varlena attribute if the
			 * offset is already suitably aligned, so that there would be no
			 * pad bytes in any case: then the offset will be valid for either
			 * an aligned or unaligned value.
			 */
			if (!slow &&
				off == att_align_nominal(off, thisatt->attalign))
				thisatt->attcacheoff = off;
			else
			{
				off = att_align_pointer(off, thisatt->attalign, -1,
										tp + off);
				slow = true;
			}
		}
		else
		{
			/* not varlena, so safe to use att_align_nominal */
			off = att_align_nominal(off, thisatt->attalign);

			if (!slow)
				thisatt->attcacheoff = off;
		}

		values[attnum] = fetchatt(thisatt, tp + off);

		off = att_addlength_pointer(off, thisatt->attlen, tp + off);

		if (thisatt->attlen <= 0)
			slow = true;		/* can't use attcacheoff anymore */
	}

	/*
	 * Save state for next execution
	 */
	slot->tts_nvalid = attnum;
	stuple->hst_off = off;
	stuple->hst_slow = slow;
}

static void
heapam_slot_virtualize_tuple(TupleTableSlot *slot, int16 upto)
{
	HeapamTuple *stuple;
	HeapTuple	tuple;
	int			attno;

	/* Quick out if we have 'em all already */
	if (slot->tts_nvalid >= upto)
		return;

	/* Check for caller error */
	if (upto <= 0 || upto > slot->tts_tupleDescriptor->natts)
		elog(ERROR, "invalid attribute number %d", upto);

	/*
	 * otherwise we had better have a physical tuple (tts_nvalid should equal
	 * natts in all virtual-tuple cases)
	 */
	stuple = slot->tts_storage; /* XXX SlotGetTupleStorage(slot) ??? */
	tuple = stuple->hst_heaptuple;
	if (tuple == NULL)			/* internal error */
		elog(ERROR, "cannot extract attribute from empty tuple slot");

	/*
	 * load up any slots available from physical tuple
	 */
	attno = HeapTupleHeaderGetNatts(tuple->t_data);
	attno = Min(attno, upto);

	slot_deform_tuple(slot, attno);

	/*
	 * If tuple doesn't have all the atts indicated by tupleDesc, read the
	 * rest as null
	 */
	for (; attno < upto; attno++)
	{
		slot->tts_values[attno] = (Datum) 0;
		slot->tts_isnull[attno] = true;
	}
	slot->tts_nvalid = upto;
}

static void
heapam_slot_update_tuple_tableoid(TupleTableSlot *slot, Oid tableoid)
{
	HeapTuple	tuple;

	tuple = heapam_get_tuple(slot, false);
	tuple->t_tableOid = tableoid;
}

static void
heapam_slot_store_tuple(TupleTableSlot *slot, StorageTuple tuple, bool shouldFree, bool minimum_tuple)
{
	HeapamTuple *stuple;
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);

	stuple = (HeapamTuple *)palloc0(sizeof(HeapamTuple));

	if (!minimum_tuple)
	{
		stuple->hst_heaptuple = tuple;
		stuple->hst_slow = false;
		stuple->hst_off = 0;
		stuple->hst_mintuple = NULL;
		slot->tts_shouldFreeMin = false;
		slot->tts_shouldFree = shouldFree;
	}
	else
	{
		stuple->hst_mintuple = tuple;
		stuple->hst_minhdr.t_len = ((MinimalTuple)tuple)->t_len + MINIMAL_TUPLE_OFFSET;
		stuple->hst_minhdr.t_data = (HeapTupleHeader) ((char *) tuple - MINIMAL_TUPLE_OFFSET);
		stuple->hst_heaptuple = &stuple->hst_minhdr;
		slot->tts_shouldFreeMin = shouldFree;
	}

	MemoryContextSwitchTo(oldcontext);

	slot->tts_tid = ((HeapTuple)tuple)->t_self;
	slot->tts_storage = stuple;
}

static void
heapam_slot_clear_tuple(TupleTableSlot *slot)
{
	HeapamTuple *stuple;

	/* XXX should this be an Assert() instead? */
	if (slot->tts_isempty)
		return;

	stuple = slot->tts_storage;
	if (stuple == NULL)
		return;

	if (slot->tts_shouldFree)
		heap_freetuple(stuple->hst_heaptuple);

	if (slot->tts_shouldFreeMin)
		heap_free_minimal_tuple(stuple->hst_mintuple);

	slot->tts_shouldFree = false;
	slot->tts_shouldFreeMin = false;

	pfree(stuple);
	slot->tts_storage = NULL;
}

/*
 * slot_getattr
 *		This function fetches an attribute of the slot's current tuple.
 *		It is functionally equivalent to heap_getattr, but fetches of
 *		multiple attributes of the same tuple will be optimized better,
 *		because we avoid O(N^2) behavior from multiple calls of
 *		nocachegetattr(), even when attcacheoff isn't usable.
 *
 *		A difference from raw heap_getattr is that attnums beyond the
 *		slot's tupdesc's last attribute will be considered NULL even
 *		when the physical tuple is longer than the tupdesc.
 */
static Datum
heapam_slot_getattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	HeapamTuple *stuple = slot->tts_storage;
	HeapTuple	tuple = stuple ? stuple->hst_heaptuple : NULL;
	TupleDesc	tupleDesc = slot->tts_tupleDescriptor;
	HeapTupleHeader tup;

	/*
	 * system attributes are handled by heap_getsysattr
	 */
	if (attnum <= 0)
	{
		if (tuple == NULL)		/* internal error */
			elog(ERROR, "cannot extract system attribute from virtual tuple");
		if (tuple == &(stuple->hst_minhdr))		/* internal error */
			elog(ERROR, "cannot extract system attribute from minimal tuple");
		return heap_getsysattr(tuple, attnum, tupleDesc, isnull);
	}

	/*
	 * fast path if desired attribute already cached
	 */
	if (attnum <= slot->tts_nvalid)
	{
		*isnull = slot->tts_isnull[attnum - 1];
		return slot->tts_values[attnum - 1];
	}

	/*
	 * return NULL if attnum is out of range according to the tupdesc
	 */
	if (attnum > tupleDesc->natts)
	{
		*isnull = true;
		return (Datum) 0;
	}

	/*
	 * otherwise we had better have a physical tuple (tts_nvalid should equal
	 * natts in all virtual-tuple cases)
	 */
	if (tuple == NULL)			/* internal error */
		elog(ERROR, "cannot extract attribute from empty tuple slot");

	/*
	 * return NULL if attnum is out of range according to the tuple
	 *
	 * (We have to check this separately because of various inheritance and
	 * table-alteration scenarios: the tuple could be either longer or shorter
	 * than the tupdesc.)
	 */
	tup = tuple->t_data;
	if (attnum > HeapTupleHeaderGetNatts(tup))
	{
		*isnull = true;
		return (Datum) 0;
	}

	/*
	 * check if target attribute is null: no point in groveling through tuple
	 */
	if (HeapTupleHasNulls(tuple) && att_isnull(attnum - 1, tup->t_bits))
	{
		*isnull = true;
		return (Datum) 0;
	}

	/*
	 * If the attribute's column has been dropped, we force a NULL result.
	 * This case should not happen in normal use, but it could happen if we
	 * are executing a plan cached before the column was dropped.
	 */
	if (TupleDescAttr(tupleDesc, (attnum - 1))->attisdropped)
	{
		*isnull = true;
		return (Datum) 0;
	}

	/*
	 * Extract the attribute, along with any preceding attributes.
	 */
	slot_deform_tuple(slot, attnum);

	/*
	 * The result is acquired from tts_values array.
	 */
	*isnull = slot->tts_isnull[attnum - 1];
	return slot->tts_values[attnum - 1];
}

StorageSlotAmRoutine*
heapam_storage_slot_handler(void)
{
	StorageSlotAmRoutine *amroutine = palloc(sizeof(StorageSlotAmRoutine));

	amroutine->slot_store_tuple = heapam_slot_store_tuple;
	amroutine->slot_virtualize_tuple = heapam_slot_virtualize_tuple;
	amroutine->slot_clear_tuple = heapam_slot_clear_tuple;
	amroutine->slot_tuple = heapam_get_tuple;
	amroutine->slot_min_tuple = heapam_get_min_tuple;
	amroutine->slot_getattr = heapam_slot_getattr;
	amroutine->slot_update_tableoid = heapam_slot_update_tuple_tableoid;

	return amroutine;
}

