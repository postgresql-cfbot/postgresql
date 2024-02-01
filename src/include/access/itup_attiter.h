/*-------------------------------------------------------------------------
 *
 * itup_attiter.h
 *	  POSTGRES index tuple attribute iterator definitions.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/itup_attiter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ITUP_ATTITER_H
#define ITUP_ATTITER_H

#include "access/itup.h"
#include "varatt.h"

typedef struct IAttrIterStateData
{
	int			offset;
	bool		slow;
	bool		isNull;
} IAttrIterStateData;

typedef IAttrIterStateData * IAttrIterState;

/* ----------------
 *		index_attiterinit
 *
 *		This gets called many times, so we macro the cacheable and NULL
 *		lookups, and call nocache_index_attiterinit() for the rest.
 *
 *		tup - the tuple being iterated on
 *		attnum - the attribute number that we start the iteration with
 *				 in the first index_attiternext call
 *		tupdesc - the tuple description
 *
 * ----------------
 */
#define index_attiterinit(tup, attnum, tupleDesc, iter) \
do { \
	if ((attnum) == 1) \
	{ \
		*(iter) = ((IAttrIterStateData) { \
			0 /* Offset of attribute 1 is always 0 */, \
			false /* slow */, \
			false /* isNull */ \
		}); \
	} \
	else if (!IndexTupleHasNulls(tup) && \
			 TupleDescAttr((tupleDesc), (attnum)-1)->attcacheoff >= 0) \
	{ \
		*(iter) = ((IAttrIterStateData) { \
			TupleDescAttr((tupleDesc), (attnum)-1)->attcacheoff, /* offset */ \
			false, /* slow */ \
			false /* isNull */ \
		}); \
	} \
	else \
		nocache_index_attiterinit((tup), (attnum) - 1, (tupleDesc), (iter)); \
} while (false);

/*
 * Initiate an index attribute iterator to attribute attnum,
 * and return the corresponding datum.
 *
 * This is nearly the same as index_deform_tuple, except that this
 * returns the internal state up to attnum, instead of populating the
 * datum- and isnull-arrays
 */
static inline void
nocache_index_attiterinit(IndexTuple tup, AttrNumber attnum, TupleDesc tupleDesc, IAttrIterState iter)
{
	bool		hasnulls = IndexTupleHasNulls(tup);
	int			curatt;
	char	   *tp;				/* ptr to tuple data */
	int			off;			/* offset in tuple data */
	bits8	   *bp;				/* ptr to null bitmap in tuple */
	bool		slow = false;	/* can we use/set attcacheoff? */
	bool		null = false;

	/* Assert to protect callers */
	Assert(PointerIsValid(iter));
	Assert(tupleDesc->natts <= INDEX_MAX_KEYS);
	Assert(attnum <= tupleDesc->natts);
	Assert(attnum > 0);

	/* XXX "knows" t_bits are just after fixed tuple header! */
	bp = (bits8 *) ((char *) tup + sizeof(IndexTupleData));

	tp = (char *) tup + IndexInfoFindDataOffset(tup->t_info);
	off = 0;

	for (curatt = 0; curatt < attnum; curatt++)
	{
		Form_pg_attribute thisatt = TupleDescAttr(tupleDesc, curatt);

		if (hasnulls && att_isnull(curatt, bp))
		{
			null = true;
			slow = true;		/* can't use attcacheoff anymore */
			continue;
		}

		null = false;

		if (!slow && thisatt->attcacheoff >= 0)
			off = thisatt->attcacheoff;
		else if (thisatt->attlen == -1)
		{
			off = att_align_pointer(off, thisatt->attalign, -1,
									tp + off);
			slow = true;
		}
		else
		{
			/* not varlena, so safe to use att_align_nominal */
			off = att_align_nominal(off, thisatt->attalign);
		}

		off = att_addlength_pointer(off, thisatt->attlen, tp + off);

		if (thisatt->attlen <= 0)
			slow = true;		/* can't use attcacheoff anymore */
	}

	iter->isNull = null;
	iter->offset = off;
	iter->slow = slow;
}

/* ----------------
 *		index_attiternext() - get the next attribute of an index tuple
 *
 *		This gets called many times, so we do the least amount of work
 *		possible.
 *
 *		The code does not attempt to update attcacheoff; as it is unlikely
 *		to reach a situation where the cached offset matters a lot.
 *		If the cached offset do matter, the caller should make sure that
 *		PopulateTupleDescCacheOffsets() was called on the tuple descriptor
 *		to populate the attribute offset cache.
 *
 * ----------------
 */
static inline Datum
index_attiternext(IndexTuple tup, AttrNumber attnum, TupleDesc tupleDesc, IAttrIterState iter)
{
	bool		hasnulls = IndexTupleHasNulls(tup);
	char	   *tp;				/* ptr to tuple data */
	bits8	   *bp;				/* ptr to null bitmap in tuple */
	Datum		datum;
	Form_pg_attribute thisatt = TupleDescAttr(tupleDesc, attnum - 1);

	Assert(PointerIsValid(iter));
	Assert(tupleDesc->natts <= INDEX_MAX_KEYS);
	Assert(attnum <= tupleDesc->natts);
	Assert(attnum > 0);

	bp = (bits8 *) ((char *) tup + sizeof(IndexTupleData));

	tp = (char *) tup + IndexInfoFindDataOffset(tup->t_info);

	if (hasnulls && att_isnull(attnum - 1, bp))
	{
		iter->isNull = true;
		iter->slow = true;
		return (Datum) 0;
	}

	iter->isNull = false;

	if (!iter->slow && thisatt->attcacheoff >= 0)
		iter->offset = thisatt->attcacheoff;
	else if (thisatt->attlen == -1)
	{
		iter->offset = att_align_pointer(iter->offset, thisatt->attalign, -1,
										 tp + iter->offset);
		iter->slow = true;
	}
	else
	{
		/* not varlena, so safe to use att_align_nominal */
		iter->offset = att_align_nominal(iter->offset, thisatt->attalign);
	}

	datum = fetchatt(thisatt, tp + iter->offset);

	iter->offset = att_addlength_pointer(iter->offset, thisatt->attlen, tp + iter->offset);

	if (thisatt->attlen <= 0)
		iter->slow = true;		/* can't use attcacheoff anymore */

	return datum;
}

#endif /* ITUP_ATTITER_H */
