/*-------------------------------------------------------------------------
 *
 * multirangetypes.h
 *	  Declarations for Postgres multirange types.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/multirangetypes.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MULTIRANGETYPES_H
#define MULTIRANGETYPES_H

#include "utils/typcache.h"
#include "utils/expandeddatum.h"


/*
 * Multiranges are varlena objects, so must meet the varlena convention that
 * the first int32 of the object contains the total object size in bytes.
 * Be sure to use VARSIZE() and SET_VARSIZE() to access it, though!
 */
typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	Oid			multirangetypid;	/* multirange type's own OID */
	uint32		rangeCount;		/* the number of ranges */

	/*
	 * Following the count are the range objects themselves, as ShortRangeType
	 * structs. Note that ranges are varlena too, depending on whether they
	 * have lower/upper bounds and because even their base types can be varlena.
	 * So we can't really index into this list.
	 */
} MultirangeType;

/*
 * Since each RangeType includes its type oid, it seems wasteful to store them
 * in multiranges like that on-disk. Instead we store ShortRangeType structs
 * that have everything but the type varlena header and oid. But we do want the
 * type oid in memory, so that we can call ordinary range functions. We use the
 * EXPANDED TOAST mechansim to convert between them.
 */
typedef struct
{
	int32		vl_len_;		/* varlena header */
	char		flags;			/* range flags */
	char		_padding;		/* Bounds must be aligned */
	/* Following the header are zero to two bound values. */
} ShortRangeType;

#define EMR_MAGIC 689375834		/* ID for debugging crosschecks */

typedef struct ExpandedMultirangeHeader
{
	/* Standard header for expanded objects */
	ExpandedObjectHeader hdr;

	/* Magic value identifying an expanded array (for debugging only) */
	int			emr_magic;

	Oid			multirangetypid;	/* multirange type's own OID */
	uint32		rangeCount;			/* the number of ranges */
	RangeType   **ranges;			/* array of ranges */

	/*
	 * flat_size is the current space requirement for the flat equivalent of
	 * the expanded multirange, if known; otherwise it's 0.  We store this to make
	 * consecutive calls of get_flat_size cheap.
	 */
	Size		flat_size;
} ExpandedMultirangeHeader;

/* Use this macro in preference to fetching multirangetypid field directly */
#define MultirangeTypeGetOid(mr)	((mr)->multirangetypid)
#define MultirangeIsEmpty(mr)  ((mr)->rangeCount == 0)

/*
 * fmgr macros for multirange type objects
 */
#define DatumGetMultirangeTypeP(X)		((MultirangeType *) PG_DETOAST_DATUM(X))
#define DatumGetMultirangeTypePCopy(X)	((MultirangeType *) PG_DETOAST_DATUM_COPY(X))
#define MultirangeTypePGetDatum(X)		PointerGetDatum(X)
#define PG_GETARG_MULTIRANGE_P(n)		DatumGetMultirangeTypeP(PG_GETARG_DATUM(n))
#define PG_GETARG_MULTIRANGE_P_COPY(n)	DatumGetMultirangeTypePCopy(PG_GETARG_DATUM(n))
#define PG_RETURN_MULTIRANGE_P(x)		return MultirangeTypePGetDatum(x)

/*
 * prototypes for functions defined in multirangetypes.c
 */

/* internal versions of the above */
extern bool multirange_eq_internal(TypeCacheEntry *typcache, MultirangeType *mr1,
								   MultirangeType *mr2);
extern bool multirange_ne_internal(TypeCacheEntry *typcache, MultirangeType *mr1,
								   MultirangeType *mr2);
extern bool multirange_contains_elem_internal(TypeCacheEntry *typcache, MultirangeType *mr,
											  Datum elem);
extern bool multirange_contains_range_internal(TypeCacheEntry *typcache, MultirangeType *mr,
											   RangeType *r);
extern bool multirange_contains_multirange_internal(TypeCacheEntry *typcache,
													MultirangeType *mr1,
													MultirangeType *mr2);
extern bool range_overlaps_multirange_internal(TypeCacheEntry *typcache, RangeType *r,
											   MultirangeType *mr);
extern bool multirange_overlaps_multirange_internal(TypeCacheEntry *typcache,
													MultirangeType *mr1,
													MultirangeType *mr2);
extern bool range_before_multirange_internal(TypeCacheEntry *typcache, RangeType *r,
											 MultirangeType *mr);
extern bool range_after_multirange_internal(TypeCacheEntry *typcache, RangeType *r,
											MultirangeType *mr);
extern bool multirange_before_multirange_internal(TypeCacheEntry *typcache,
												  MultirangeType *mr1,
												  MultirangeType *mr2);
extern MultirangeType *multirange_minus_internal(Oid mltrngtypoid,
												 TypeCacheEntry *rangetyp,
												 int32 range_count1,
												 RangeType **ranges1,
												 int32 range_count2,
												 RangeType **ranges2);
extern MultirangeType *multirange_intersect_internal(Oid mltrngtypoid,
													 TypeCacheEntry *rangetyp,
													 int32 range_count1,
													 RangeType **ranges1,
													 int32 range_count2,
													 RangeType **ranges2);

/* assorted support functions */
extern TypeCacheEntry *multirange_get_typcache(FunctionCallInfo fcinfo,
											   Oid mltrngtypid);
extern void multirange_deserialize(TypeCacheEntry *rangetyp,
								   const MultirangeType *range, int32 *range_count,
								   RangeType ***ranges);
extern MultirangeType *make_multirange(Oid mltrngtypoid,
									   TypeCacheEntry *typcache, int32 range_count, RangeType **ranges);
extern MultirangeType *make_empty_multirange(Oid mltrngtypoid, TypeCacheEntry *rangetyp);

extern Datum expand_multirange(Datum multirangedatum, MemoryContext parentcontext, TypeCacheEntry *rangetyp);

#endif							/* MULTIRANGETYPES_H */
