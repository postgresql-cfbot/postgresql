/*-------------------------------------------------------------------------
 *
 * nbtcompare.c
 *	  Comparison functions for btree access method.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtcompare.c
 *
 * NOTES
 *
 *	These functions are stored in pg_amproc.  For each operator class
 *	defined on btrees, they compute
 *
 *				compare(a, b):
 *						< 0 if a < b,
 *						= 0 if a == b,
 *						> 0 if a > b.
 *
 *	The result is always an int32 regardless of the input datatype.
 *
 *	Although any negative int32 is acceptable for reporting "<",
 *	and any positive int32 is acceptable for reporting ">", routines
 *	that work on 32-bit or wider datatypes can't just return "a - b".
 *	That could overflow and give the wrong answer.
 *
 *	NOTE: it is critical that the comparison function impose a total order
 *	on all non-NULL values of the data type, and that the datatype's
 *	boolean comparison operators (= < >= etc) yield results consistent
 *	with the comparison routine.  Otherwise bad behavior may ensue.
 *	(For example, the comparison operators must NOT punt when faced with
 *	NAN or other funny values; you must devise some collation sequence for
 *	all such values.)  If the datatype is not trivial, this is most
 *	reliably done by having the boolean operators invoke the same
 *	three-way comparison code that the btree function does.  Therefore,
 *	this file contains only btree support for "trivial" datatypes ---
 *	all others are in the /utils/adt/ files that implement their datatypes.
 *
 *	NOTE: these routines must not leak memory, since memory allocated
 *	during an index access won't be recovered till end of query.  This
 *	primarily affects comparison routines for toastable datatypes;
 *	they have to be careful to free any detoasted copy of an input datum.
 *
 *	NOTE: we used to forbid comparison functions from returning INT_MIN,
 *	but that proves to be too error-prone because some platforms' versions
 *	of memcmp() etc can return INT_MIN.  As a means of stress-testing
 *	callers, this file can be compiled with STRESS_SORT_INT_MIN defined
 *	to cause many of these functions to return INT_MIN or INT_MAX instead of
 *	their customary -1/+1.  For production, though, that's not a good idea
 *	since users or third-party code might expect the traditional results.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#include "common/int.h"
#include "utils/fmgrprotos.h"
#include "utils/sortsupport.h"

#define STRESS_SORT_INT_MIN_CMP(a, b) \
( \
	((a) > (b)) ? INT_MAX : \
	((a) < (b)) ? INT_MIN : \
	0 \
)

Datum
btboolcmp(PG_FUNCTION_ARGS)
{
	bool		a = PG_GETARG_BOOL(0);
	bool		b = PG_GETARG_BOOL(1);

	PG_RETURN_INT32((int32) a - (int32) b);
}

Datum
btint2cmp(PG_FUNCTION_ARGS)
{
	int16		a = PG_GETARG_INT16(0);
	int16		b = PG_GETARG_INT16(1);

#ifdef STRESS_SORT_INT_MIN
	PG_RETURN_INT32(STRESS_SORT_INT_MIN_CMP(a, b));
#else
	PG_RETURN_INT32(pg_cmp_s16(a, b));
#endif
}

static int
btint2fastcmp(Datum x, Datum y, SortSupport ssup)
{
	int16		a = DatumGetInt16(x);
	int16		b = DatumGetInt16(y);

#ifdef STRESS_SORT_INT_MIN
	return STRESS_SORT_INT_MIN_CMP(a, b);
#else
	return pg_cmp_s16(a, b);
#endif
}

Datum
btint2sortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

	ssup->comparator = btint2fastcmp;
	PG_RETURN_VOID();
}

Datum
btint4cmp(PG_FUNCTION_ARGS)
{
	int32		a = PG_GETARG_INT32(0);
	int32		b = PG_GETARG_INT32(1);

#ifdef STRESS_SORT_INT_MIN
	PG_RETURN_INT32(STRESS_SORT_INT_MIN_CMP(a, b));
#else
	PG_RETURN_INT32(pg_cmp_s32(a, b));
#endif
}

Datum
btint4sortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

	ssup->comparator = ssup_datum_int32_cmp;
	PG_RETURN_VOID();
}

Datum
btint8cmp(PG_FUNCTION_ARGS)
{
	int64		a = PG_GETARG_INT64(0);
	int64		b = PG_GETARG_INT64(1);

#ifdef STRESS_SORT_INT_MIN
	PG_RETURN_INT32(STRESS_SORT_INT_MIN_CMP(a, b));
#else
	PG_RETURN_INT32(pg_cmp_s64(a, b));
#endif
}

#if SIZEOF_DATUM < 8
static int
btint8fastcmp(Datum x, Datum y, SortSupport ssup)
{
	int64		a = DatumGetInt64(x);
	int64		b = DatumGetInt64(y);

#ifdef STRESS_SORT_INT_MIN
	return STRESS_SORT_INT_MIN_CMP(a, b);
#else
	return pg_cmp_s64(a, b);
#endif
}
#endif

Datum
btint8sortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

#if SIZEOF_DATUM >= 8
	ssup->comparator = ssup_datum_signed_cmp;
#else
	ssup->comparator = btint8fastcmp;
#endif
	PG_RETURN_VOID();
}

Datum
btint48cmp(PG_FUNCTION_ARGS)
{
	int32		a = PG_GETARG_INT32(0);
	int64		b = PG_GETARG_INT64(1);

#ifdef STRESS_SORT_INT_MIN
	PG_RETURN_INT32(STRESS_SORT_INT_MIN_CMP(a, b));
#else
	PG_RETURN_INT32(pg_cmp_s64(a, b));
#endif
}

Datum
btint84cmp(PG_FUNCTION_ARGS)
{
	int64		a = PG_GETARG_INT64(0);
	int32		b = PG_GETARG_INT32(1);

#ifdef STRESS_SORT_INT_MIN
	PG_RETURN_INT32(STRESS_SORT_INT_MIN_CMP(a, b));
#else
	PG_RETURN_INT32(pg_cmp_s64(a, b));
#endif
}

Datum
btint24cmp(PG_FUNCTION_ARGS)
{
	int16		a = PG_GETARG_INT16(0);
	int32		b = PG_GETARG_INT32(1);

#ifdef STRESS_SORT_INT_MIN
	PG_RETURN_INT32(STRESS_SORT_INT_MIN_CMP(a, b));
#else
	PG_RETURN_INT32(pg_cmp_s32(a, b));
#endif
}

Datum
btint42cmp(PG_FUNCTION_ARGS)
{
	int32		a = PG_GETARG_INT32(0);
	int16		b = PG_GETARG_INT16(1);

#ifdef STRESS_SORT_INT_MIN
	PG_RETURN_INT32(STRESS_SORT_INT_MIN_CMP(a, b));
#else
	PG_RETURN_INT32(pg_cmp_s32(a, b));
#endif
}

Datum
btint28cmp(PG_FUNCTION_ARGS)
{
	int16		a = PG_GETARG_INT16(0);
	int64		b = PG_GETARG_INT64(1);

#ifdef STRESS_SORT_INT_MIN
	PG_RETURN_INT32(STRESS_SORT_INT_MIN_CMP(a, b));
#else
	PG_RETURN_INT32(pg_cmp_s64(a, b));
#endif
}

Datum
btint82cmp(PG_FUNCTION_ARGS)
{
	int64		a = PG_GETARG_INT64(0);
	int16		b = PG_GETARG_INT16(1);

#ifdef STRESS_SORT_INT_MIN
	PG_RETURN_INT32(STRESS_SORT_INT_MIN_CMP(a, b));
#else
	PG_RETURN_INT32(pg_cmp_s64(a, b));
#endif
}

Datum
btoidcmp(PG_FUNCTION_ARGS)
{
	Oid			a = PG_GETARG_OID(0);
	Oid			b = PG_GETARG_OID(1);

#ifdef STRESS_SORT_INT_MIN
	PG_RETURN_INT32(STRESS_SORT_INT_MIN_CMP(a, b));
#else
	PG_RETURN_INT32(pg_cmp_u32(a, b));
#endif
}

static int
btoidfastcmp(Datum x, Datum y, SortSupport ssup)
{
	Oid			a = DatumGetObjectId(x);
	Oid			b = DatumGetObjectId(y);

#ifdef STRESS_SORT_INT_MIN
	return STRESS_SORT_INT_MIN_CMP(a, b);
#else
	return pg_cmp_u32(a, b);
#endif
}

Datum
btoidsortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

	ssup->comparator = btoidfastcmp;
	PG_RETURN_VOID();
}

Datum
btoidvectorcmp(PG_FUNCTION_ARGS)
{
	oidvector  *a = (oidvector *) PG_GETARG_POINTER(0);
	oidvector  *b = (oidvector *) PG_GETARG_POINTER(1);
	int			i;

	/* We arbitrarily choose to sort first by vector length */
	if (a->dim1 != b->dim1)
		PG_RETURN_INT32(a->dim1 - b->dim1);

	for (i = 0; i < a->dim1; i++)
	{
		Oid			aval = a->values[i];
		Oid			bval = b->values[i];

		if (aval != bval)
		{
#ifdef STRESS_SORT_INT_MIN
			PG_RETURN_INT32(STRESS_SORT_INT_MIN_CMP(aval, bval));
#else
			PG_RETURN_INT32(pg_cmp_u32(aval, bval));
#endif
		}
	}
	PG_RETURN_INT32(0);
}

Datum
btcharcmp(PG_FUNCTION_ARGS)
{
	char		a = PG_GETARG_CHAR(0);
	char		b = PG_GETARG_CHAR(1);

	/* Be careful to compare chars as unsigned */
	PG_RETURN_INT32((int32) ((uint8) a) - (int32) ((uint8) b));
}
