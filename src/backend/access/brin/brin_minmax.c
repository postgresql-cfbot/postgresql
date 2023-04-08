/*
 * brin_minmax.c
 *		Implementation of Min/Max opclass for BRIN
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/brin/brin_minmax.c
 */
#include "postgres.h"

#include "access/brin_internal.h"
#include "access/brin_tuple.h"
#include "access/genam.h"
#include "access/stratnum.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/sortsupport.h"

/*
 * We use some private sk_flags bits in preprocessed scan keys.  We're allowed
 * to use bits 16-31 (see skey.h).  The uppermost bits are copied from the
 * index's indoption[] array entry for the index attribute.
 */
#define SK_BRIN_SORTED	0x00010000	/* deconstructed and sorted array */


typedef struct MinmaxOpaque
{
	Oid			cached_subtype;
	FmgrInfo	strategy_procinfos[BTMaxStrategyNumber];
} MinmaxOpaque;

static FmgrInfo *minmax_get_strategy_procinfo(BrinDesc *bdesc, uint16 attno,
											  Oid subtype, uint16 strategynum);


Datum
brin_minmax_opcinfo(PG_FUNCTION_ARGS)
{
	Oid			typoid = PG_GETARG_OID(0);
	BrinOpcInfo *result;

	/*
	 * opaque->strategy_procinfos is initialized lazily; here it is set to
	 * all-uninitialized by palloc0 which sets fn_oid to InvalidOid.
	 */

	result = palloc0(MAXALIGN(SizeofBrinOpcInfo(2)) +
					 sizeof(MinmaxOpaque));
	result->oi_nstored = 2;
	result->oi_regular_nulls = true;
	result->oi_opaque = (MinmaxOpaque *)
		MAXALIGN((char *) result + SizeofBrinOpcInfo(2));
	result->oi_typcache[0] = result->oi_typcache[1] =
		lookup_type_cache(typoid, 0);

	PG_RETURN_POINTER(result);
}

/*
 * Examine the given index tuple (which contains partial status of a certain
 * page range) by comparing it to the given value that comes from another heap
 * tuple.  If the new value is outside the min/max range specified by the
 * existing tuple values, update the index tuple and return true.  Otherwise,
 * return false and do not modify in this case.
 */
Datum
brin_minmax_add_value(PG_FUNCTION_ARGS)
{
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *column = (BrinValues *) PG_GETARG_POINTER(1);
	Datum		newval = PG_GETARG_DATUM(2);
	bool		isnull PG_USED_FOR_ASSERTS_ONLY = PG_GETARG_DATUM(3);
	Oid			colloid = PG_GET_COLLATION();
	FmgrInfo   *cmpFn;
	Datum		compar;
	bool		updated = false;
	Form_pg_attribute attr;
	AttrNumber	attno;

	Assert(!isnull);

	attno = column->bv_attno;
	attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);

	/*
	 * If the recorded value is null, store the new value (which we know to be
	 * not null) as both minimum and maximum, and we're done.
	 */
	if (column->bv_allnulls)
	{
		column->bv_values[0] = datumCopy(newval, attr->attbyval, attr->attlen);
		column->bv_values[1] = datumCopy(newval, attr->attbyval, attr->attlen);
		column->bv_allnulls = false;
		PG_RETURN_BOOL(true);
	}

	/*
	 * Otherwise, need to compare the new value with the existing boundaries
	 * and update them accordingly.  First check if it's less than the
	 * existing minimum.
	 */
	cmpFn = minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
										 BTLessStrategyNumber);
	compar = FunctionCall2Coll(cmpFn, colloid, newval, column->bv_values[0]);
	if (DatumGetBool(compar))
	{
		if (!attr->attbyval)
			pfree(DatumGetPointer(column->bv_values[0]));
		column->bv_values[0] = datumCopy(newval, attr->attbyval, attr->attlen);
		updated = true;
	}

	/*
	 * And now compare it to the existing maximum.
	 */
	cmpFn = minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
										 BTGreaterStrategyNumber);
	compar = FunctionCall2Coll(cmpFn, colloid, newval, column->bv_values[1]);
	if (DatumGetBool(compar))
	{
		if (!attr->attbyval)
			pfree(DatumGetPointer(column->bv_values[1]));
		column->bv_values[1] = datumCopy(newval, attr->attbyval, attr->attlen);
		updated = true;
	}

	PG_RETURN_BOOL(updated);
}


static int
compare_array_values(const void *a, const void *b, void *arg)
{
	Datum	da = * (Datum *) a;
	Datum	db = * (Datum *) b;
	SortSupport	ssup = (SortSupport) arg;

	return ApplySortComparator(da, false, db, false, ssup);
}

/*
 * lower_boundary
 *		Determine lowest index so that (values[index] >= minvalue).
 *
 * The array of values is expected to be sorted, so this is the first value
 * that may fall into the [minvalue, maxvalue] range, as it exceeds minval.
 * It's not guaranteed, though, as it might exceed maxvalue too.
 */
static int
lower_boundary(Datum *values, int nvalues, Datum minvalue, SortSupport ssup)
{
	int		start = 0,
			end = (nvalues - 1);

	/* everything exceeds minval and might match */
	if (compare_array_values(&minvalue, &values[start], ssup) <= 0)
		return 0;

	/* nothing could match */
	if (compare_array_values(&minvalue, &values[end], ssup) > 0)
		return nvalues;

	while ((end - start) > 0)
	{
		int midpoint;
		int r;

		midpoint = start + (end - start) / 2;

		r = compare_array_values(&minvalue, &values[midpoint], ssup);

		if (r > 0)
			start = Max(midpoint, start + 1);
		else
			end = midpoint;
	}

	/* the value should meet the (v >=minvalue) requirement */
	Assert(compare_array_values(&values[start], &minvalue, ssup) >= 0);

	/* we know start can't be 0, so it's legal to subtract 1 */
	Assert(compare_array_values(&values[start-1], &minvalue, ssup) < 0);

	return start;
}

typedef struct ScanKeyArray {
	Oid		typeid;
	int		nelements;
	Datum  *elements;
} ScanKeyArray;

Datum
brin_minmax_preprocess(PG_FUNCTION_ARGS)
{
	// BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	ScanKey		key = (ScanKey) PG_GETARG_POINTER(1);
	ScanKey		newkey;
	ScanKeyArray *scanarray;

	ArrayType  *arrayval;
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;
	int			num_elems;
	Datum	   *elem_values;
	bool	   *elem_nulls;
	TypeCacheEntry *type;
	SortSupportData ssup;

	/* number of non-null elements in the array */
	int			num_nonnulls;

	/*
	 * ignore scalar keys
	 *
	 * XXX Maybe we should preprocess scalar keys too. It'd make the consistent
	 * function simpler by removing the branching.
	 */
	if (!(key->sk_flags & SK_SEARCHARRAY))
		PG_RETURN_POINTER(key);

	arrayval = DatumGetArrayTypeP(key->sk_argument);

	get_typlenbyvalalign(ARR_ELEMTYPE(arrayval),
						 &elmlen, &elmbyval, &elmalign);

	deconstruct_array(arrayval,
					  ARR_ELEMTYPE(arrayval),
					  elmlen, elmbyval, elmalign,
					  &elem_values, &elem_nulls, &num_elems);

	/* eliminate NULL elements */
	num_nonnulls = 0;
	for (int i = 0; i < num_elems; i++)
	{
		/* skip NULL elements */
		if (elem_nulls[i])
			continue;

		/* if needed, move the non-NULL ones */
		if (num_nonnulls != i)
			elem_values[num_nonnulls] = elem_values[i];

		num_nonnulls++;
	}

	num_elems = num_nonnulls;

	type = lookup_type_cache(ARR_ELEMTYPE(arrayval), TYPECACHE_LT_OPR);

	memset(&ssup, 0, sizeof(SortSupportData));

	ssup.ssup_collation = key->sk_collation;
	ssup.ssup_cxt = CurrentMemoryContext;

	PrepareSortSupportFromOrderingOp(type->lt_opr, &ssup);

	qsort_interruptible(elem_values, num_elems, sizeof(Datum),
						compare_array_values, &ssup);

	scanarray = palloc0(sizeof(ScanKeyArray));
	scanarray->typeid = ARR_ELEMTYPE(arrayval);
	scanarray->nelements = num_elems;
	scanarray->elements = elem_values;

	newkey = palloc0(sizeof(ScanKeyData));

	ScanKeyEntryInitializeWithInfo(newkey,
								   (key->sk_flags | SK_BRIN_SORTED),
								   key->sk_attno,
								   key->sk_strategy,
								   key->sk_subtype,
								   key->sk_collation,
								   &key->sk_func,
								   PointerGetDatum(scanarray));

	PG_RETURN_POINTER(newkey);
}


/*
 * Given an index tuple corresponding to a certain page range and a scan key,
 * return whether the scan key is consistent with the index tuple's min/max
 * values.  Return true if so, false otherwise.
 *
 * We're no longer dealing with NULL keys in the consistent function, that is
 * now handled by the AM code. That means we should not get any all-NULL ranges
 * either, because those can't be consistent with regular (not [IS] NULL) keys.
 */
Datum
brin_minmax_consistent(PG_FUNCTION_ARGS)
{
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *column = (BrinValues *) PG_GETARG_POINTER(1);
	ScanKey		key = (ScanKey) PG_GETARG_POINTER(2);
	Oid			colloid = PG_GET_COLLATION(),
				subtype;
	AttrNumber	attno;
	Datum		value;
	Datum		matches;
	FmgrInfo   *finfo;

	/* This opclass uses the old signature with only three arguments. */
	Assert(PG_NARGS() == 3);

	/* Should not be dealing with all-NULL ranges. */
	Assert(!column->bv_allnulls);

	attno = key->sk_attno;
	subtype = key->sk_subtype;
	value = key->sk_argument;

	/*
	 * For regular (scalar) scan keys, we simply compare the value to the
	 * range min/max values, and we're done. For preprocessed SK_SEARCHARRAY
	 * keys we need to loop through the deparsed values.
	 */
	if (likely(!(key->sk_flags & SK_BRIN_SORTED)))
	{
		switch (key->sk_strategy)
		{
			case BTLessStrategyNumber:
			case BTLessEqualStrategyNumber:
				finfo = minmax_get_strategy_procinfo(bdesc, attno, subtype,
													 key->sk_strategy);
				matches = FunctionCall2Coll(finfo, colloid, column->bv_values[0],
											value);
				break;
			case BTEqualStrategyNumber:

				/*
				 * In the equality case (WHERE col = someval), we want to return
				 * the current page range if the minimum value in the range <=
				 * scan key, and the maximum value >= scan key.
				 */
				finfo = minmax_get_strategy_procinfo(bdesc, attno, subtype,
													 BTLessEqualStrategyNumber);
				matches = FunctionCall2Coll(finfo, colloid, column->bv_values[0],
											value);
				if (!DatumGetBool(matches))
					break;
				/* max() >= scankey */
				finfo = minmax_get_strategy_procinfo(bdesc, attno, subtype,
													 BTGreaterEqualStrategyNumber);
				matches = FunctionCall2Coll(finfo, colloid, column->bv_values[1],
											value);
				break;
			case BTGreaterEqualStrategyNumber:
			case BTGreaterStrategyNumber:
				finfo = minmax_get_strategy_procinfo(bdesc, attno, subtype,
													 key->sk_strategy);
				matches = FunctionCall2Coll(finfo, colloid, column->bv_values[1],
											value);
				break;
			default:
				/* shouldn't happen */
				elog(ERROR, "invalid strategy number %d", key->sk_strategy);
				matches = 0;
				break;
		}
	}
	else
	{
		ScanKeyArray *array = (ScanKeyArray *) value;

		/* can happen if the IN list contained just NULLs */
		if (array->nelements == 0)
			PG_RETURN_BOOL(false);

		switch (key->sk_strategy)
		{
			case BTLessStrategyNumber:
			case BTLessEqualStrategyNumber:
				/*
				 * Check the last (largest) value in the array - at least this
				 * value has to exceed the range minval.
				 */
				finfo = minmax_get_strategy_procinfo(bdesc, attno, subtype,
													 key->sk_strategy);
				matches = FunctionCall2Coll(finfo, colloid, column->bv_values[0],
											array->elements[array->nelements-1]);
				break;
			case BTEqualStrategyNumber:

				/*
				 * In the equality case (WHERE col = someval), we want to return
				 * the current page range if the minimum value in the range <=
				 * scan key, and the maximum value >= scan key.
				 *
				 * We do this in two phases. We check the array min/max values to see
				 * if there even can be a matching value, and if yes we do a binary
				 * search to find the first value that exceeds range minval. And then
				 * we check if it actually matches the range.
				 *
				 * XXX The first phase is probably unnecessary, because lower_bound()
				 * does pretty much exactly that too.
				 */
				{
					Datum val;
					SortSupportData ssup;
					int			lower;
					TypeCacheEntry *type;

					/* Is the first (smallest) value after the BRIN range? */
					val = array->elements[0];

					finfo = minmax_get_strategy_procinfo(bdesc, attno, subtype,
														 BTLessEqualStrategyNumber);
					matches = FunctionCall2Coll(finfo, colloid, val, column->bv_values[1]);

					/* minval > max(range values) */
					if (!DatumGetBool(matches))
						break;

					/* Is the last (largest) value before the BRIN range? */
					val = array->elements[array->nelements-1];

					finfo = minmax_get_strategy_procinfo(bdesc, attno, subtype,
														 BTGreaterEqualStrategyNumber);
					matches = FunctionCall2Coll(finfo, colloid, val, column->bv_values[0]);

					/* maxval < min(range values) */
					if (!DatumGetBool(matches))
						break;

					/*
					 * OK, there might be some values matching the range. We have
					 * to search them one by one, or perhaps try binsearch.
					 */
					type = lookup_type_cache(array->typeid, TYPECACHE_LT_OPR);

					memset(&ssup, 0, sizeof(SortSupportData));

					ssup.ssup_collation = key->sk_collation;
					ssup.ssup_cxt = CurrentMemoryContext;

					PrepareSortSupportFromOrderingOp(type->lt_opr, &ssup);

					lower = lower_boundary(array->elements, array->nelements, column->bv_values[0], &ssup);

					/* no elements can possibly match */
					if (lower == array->nelements)
					{
						matches = BoolGetDatum(false);
						break;
					}

					/*
					 * OK, the first element must match the upper boundary too
					 * (if it does not, no following elements can).
					 */
					val = array->elements[lower];

					/*
					 * In the equality case (WHERE col = someval), we want to return
					 * the current page range if the minimum value in the range <=
					 * scan key, and the maximum value >= scan key.
					 */
					finfo = minmax_get_strategy_procinfo(bdesc, attno, subtype,
														 BTLessEqualStrategyNumber);
					matches = FunctionCall2Coll(finfo, colloid, column->bv_values[0],
												val);
					if (!DatumGetBool(matches))
						break;
					/* max() >= scankey */
					finfo = minmax_get_strategy_procinfo(bdesc, attno, subtype,
														 BTGreaterEqualStrategyNumber);
					matches = FunctionCall2Coll(finfo, colloid, column->bv_values[1],
												val);
					break;
				}
			case BTGreaterEqualStrategyNumber:
			case BTGreaterStrategyNumber:
				/*
				 * Check the first (smallest) value in the array - at least this
				 * value has to be smaller than the range maxval.
				 */
				finfo = minmax_get_strategy_procinfo(bdesc, attno, subtype,
													 key->sk_strategy);
				matches = FunctionCall2Coll(finfo, colloid, column->bv_values[1],
											array->elements[0]);
				break;
			default:
				/* shouldn't happen */
				elog(ERROR, "invalid strategy number %d", key->sk_strategy);
				matches = 0;
				break;
		}
	}

	PG_RETURN_DATUM(matches);
}

/*
 * Given two BrinValues, update the first of them as a union of the summary
 * values contained in both.  The second one is untouched.
 */
Datum
brin_minmax_union(PG_FUNCTION_ARGS)
{
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *col_a = (BrinValues *) PG_GETARG_POINTER(1);
	BrinValues *col_b = (BrinValues *) PG_GETARG_POINTER(2);
	Oid			colloid = PG_GET_COLLATION();
	AttrNumber	attno;
	Form_pg_attribute attr;
	FmgrInfo   *finfo;
	bool		needsadj;

	Assert(col_a->bv_attno == col_b->bv_attno);
	Assert(!col_a->bv_allnulls && !col_b->bv_allnulls);

	attno = col_a->bv_attno;
	attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);

	/* Adjust minimum, if B's min is less than A's min */
	finfo = minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
										 BTLessStrategyNumber);
	needsadj = FunctionCall2Coll(finfo, colloid, col_b->bv_values[0],
								 col_a->bv_values[0]);
	if (needsadj)
	{
		if (!attr->attbyval)
			pfree(DatumGetPointer(col_a->bv_values[0]));
		col_a->bv_values[0] = datumCopy(col_b->bv_values[0],
										attr->attbyval, attr->attlen);
	}

	/* Adjust maximum, if B's max is greater than A's max */
	finfo = minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
										 BTGreaterStrategyNumber);
	needsadj = FunctionCall2Coll(finfo, colloid, col_b->bv_values[1],
								 col_a->bv_values[1]);
	if (needsadj)
	{
		if (!attr->attbyval)
			pfree(DatumGetPointer(col_a->bv_values[1]));
		col_a->bv_values[1] = datumCopy(col_b->bv_values[1],
										attr->attbyval, attr->attlen);
	}

	PG_RETURN_VOID();
}

/*
 * Cache and return the procedure for the given strategy.
 *
 * Note: this function mirrors inclusion_get_strategy_procinfo; see notes
 * there.  If changes are made here, see that function too.
 */
static FmgrInfo *
minmax_get_strategy_procinfo(BrinDesc *bdesc, uint16 attno, Oid subtype,
							 uint16 strategynum)
{
	MinmaxOpaque *opaque;

	Assert(strategynum >= 1 &&
		   strategynum <= BTMaxStrategyNumber);

	opaque = (MinmaxOpaque *) bdesc->bd_info[attno - 1]->oi_opaque;

	/*
	 * We cache the procedures for the previous subtype in the opaque struct,
	 * to avoid repetitive syscache lookups.  If the subtype changed,
	 * invalidate all the cached entries.
	 */
	if (opaque->cached_subtype != subtype)
	{
		uint16		i;

		for (i = 1; i <= BTMaxStrategyNumber; i++)
			opaque->strategy_procinfos[i - 1].fn_oid = InvalidOid;
		opaque->cached_subtype = subtype;
	}

	if (opaque->strategy_procinfos[strategynum - 1].fn_oid == InvalidOid)
	{
		Form_pg_attribute attr;
		HeapTuple	tuple;
		Oid			opfamily,
					oprid;

		opfamily = bdesc->bd_index->rd_opfamily[attno - 1];
		attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);
		tuple = SearchSysCache4(AMOPSTRATEGY, ObjectIdGetDatum(opfamily),
								ObjectIdGetDatum(attr->atttypid),
								ObjectIdGetDatum(subtype),
								Int16GetDatum(strategynum));

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
				 strategynum, attr->atttypid, subtype, opfamily);

		oprid = DatumGetObjectId(SysCacheGetAttrNotNull(AMOPSTRATEGY, tuple,
														Anum_pg_amop_amopopr));
		ReleaseSysCache(tuple);
		Assert(RegProcedureIsValid(oprid));

		fmgr_info_cxt(get_opcode(oprid),
					  &opaque->strategy_procinfos[strategynum - 1],
					  bdesc->bd_context);
	}

	return &opaque->strategy_procinfos[strategynum - 1];
}
