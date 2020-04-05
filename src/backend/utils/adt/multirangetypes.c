/*-------------------------------------------------------------------------
 *
 * multirangetypes.c
 *	  I/O functions, operators, and support functions for multirange types.
 *
 * The stored (serialized) format of a multirange value is:
 *
 *	4 bytes: varlena header
 *	4 bytes: multirange type's OID
 *	4 bytes: the number of ranges in the multirange
 *	The range values, each maxaligned.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/multirangetypes.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tupmacs.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/rangetypes.h"
#include "utils/multirangetypes.h"
#include "utils/timestamp.h"
#include "utils/array.h"


/* fn_extra cache entry for one of the range I/O functions */
typedef struct MultirangeIOData
{
	TypeCacheEntry *typcache;	/* multirange type's typcache entry */
	FmgrInfo	typioproc;		/* range type's I/O proc */
	Oid			typioparam;		/* range type's I/O parameter */
} MultirangeIOData;

typedef enum
{
	MULTIRANGE_BEFORE_RANGE,
	MULTIRANGE_IN_RANGE,
	MULTIRANGE_IN_RANGE_ESCAPED,
	MULTIRANGE_IN_RANGE_QUOTED,
	MULTIRANGE_IN_RANGE_QUOTED_ESCAPED,
	MULTIRANGE_AFTER_RANGE,
	MULTIRANGE_FINISHED,
} MultirangeParseState;

static MultirangeIOData *get_multirange_io_data(FunctionCallInfo fcinfo, Oid mltrngtypid,
												IOFuncSelector func);
static int32 multirange_canonicalize(TypeCacheEntry *rangetyp, int32 input_range_count,
									 RangeType **ranges);


/*
 *----------------------------------------------------------
 * I/O FUNCTIONS
 *----------------------------------------------------------
 */

/*
 * Converts string to multirange.
 *
 * We expect curly brackets to bound the list,
 * with zero or more ranges separated by commas.
 * We accept whitespace anywhere:
 * before/after our brackets and around the commas.
 * Ranges can be the empty literal or some stuff inside parens/brackets.
 * Mostly we delegate parsing the individual range contents
 * to range_in, but we have to detect quoting and backslash-escaping
 * which can happen for range bounds.
 * Backslashes can escape something inside or outside a quoted string,
 * and a quoted string can escape quote marks with either backslashes
 * or double double-quotes.
 */
Datum
multirange_in(PG_FUNCTION_ARGS)
{
	char	   *input_str = PG_GETARG_CSTRING(0);
	Oid			mltrngtypoid = PG_GETARG_OID(1);
	Oid			typmod = PG_GETARG_INT32(2);
	TypeCacheEntry *rangetyp;
	int32		ranges_seen = 0;
	int32		range_count = 0;
	int32		range_capacity = 8;
	RangeType  *range;
	RangeType **ranges = palloc(range_capacity * sizeof(RangeType *));
	MultirangeIOData *cache;
	MultirangeType *ret;
	MultirangeParseState parse_state;
	const char *ptr = input_str;
	const char *range_str = NULL;
	int32		range_str_len;
	char	   *range_str_copy;

	cache = get_multirange_io_data(fcinfo, mltrngtypoid, IOFunc_input);
	rangetyp = cache->typcache->rngtype;

	/* consume whitespace */
	while (*ptr != '\0' && isspace((unsigned char) *ptr))
		ptr++;

	if (*ptr == '{')
		ptr++;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed multirange literal: \"%s\"",
						input_str),
				 errdetail("Missing left bracket.")));

	/* consume ranges */
	parse_state = MULTIRANGE_BEFORE_RANGE;
	for (; parse_state != MULTIRANGE_FINISHED; ptr++)
	{
		char		ch = *ptr;

		if (ch == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("malformed multirange literal: \"%s\"",
							input_str),
					 errdetail("Unexpected end of input.")));

		/* skip whitespace */
		if (isspace((unsigned char) ch))
			continue;

		switch (parse_state)
		{
			case MULTIRANGE_BEFORE_RANGE:
				if (ch == '[' || ch == '(')
				{
					range_str = ptr;
					parse_state = MULTIRANGE_IN_RANGE;
				}
				else if (ch == '}' && ranges_seen == 0)
					parse_state = MULTIRANGE_FINISHED;
				else if (pg_strncasecmp(ptr, RANGE_EMPTY_LITERAL,
										strlen(RANGE_EMPTY_LITERAL)) == 0)
				{
					ranges_seen++;
					/* nothing to do with an empty range */
					ptr += strlen(RANGE_EMPTY_LITERAL) - 1;
					parse_state = MULTIRANGE_AFTER_RANGE;
				}
				else
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
							 errmsg("malformed multirange literal: \"%s\"",
									input_str),
							 errdetail("Expected range start.")));
				break;
			case MULTIRANGE_IN_RANGE:
				if (ch == '"')
					parse_state = MULTIRANGE_IN_RANGE_QUOTED;
				else if (ch == '\\')
					parse_state = MULTIRANGE_IN_RANGE_ESCAPED;
				else if (ch == ']' || ch == ')')
				{
					range_str_len = ptr - range_str + 1;
					range_str_copy = pnstrdup(range_str, range_str_len);
					if (range_capacity == range_count)
					{
						range_capacity *= 2;
						ranges = (RangeType **)
							repalloc(ranges, range_capacity * sizeof(RangeType *));
					}
					ranges_seen++;
					range = DatumGetRangeTypeP(InputFunctionCall(&cache->typioproc,
																 range_str_copy,
																 cache->typioparam,
																 typmod));
					if (!RangeIsEmpty(range))
						ranges[range_count++] = range;
					parse_state = MULTIRANGE_AFTER_RANGE;
				}
				else
					 /* include it in range_str */ ;
				break;
			case MULTIRANGE_IN_RANGE_ESCAPED:
				/* include it in range_str */
				parse_state = MULTIRANGE_IN_RANGE;
				break;
			case MULTIRANGE_IN_RANGE_QUOTED:
				if (ch == '"')
					if (*(ptr + 1) == '"')
					{
						/* two quote marks means an escaped quote mark */
						ptr++;
					}
					else
						parse_state = MULTIRANGE_IN_RANGE;
				else if (ch == '\\')
					parse_state = MULTIRANGE_IN_RANGE_QUOTED_ESCAPED;
				else
					 /* include it in range_str */ ;
				break;
			case MULTIRANGE_AFTER_RANGE:
				if (ch == ',')
					parse_state = MULTIRANGE_BEFORE_RANGE;
				else if (ch == '}')
					parse_state = MULTIRANGE_FINISHED;
				else
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
							 errmsg("malformed multirange literal: \"%s\"",
									input_str),
							 errdetail("Expected comma or end of multirange.")));
				break;
			case MULTIRANGE_IN_RANGE_QUOTED_ESCAPED:
				/* include it in range_str */
				parse_state = MULTIRANGE_IN_RANGE_QUOTED;
				break;
			default:
				elog(ERROR, "unknown parse state: %d", parse_state);
		}
	}

	/* consume whitespace */
	while (*ptr != '\0' && isspace((unsigned char) *ptr))
		ptr++;

	if (*ptr != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed multirange literal: \"%s\"",
						input_str),
				 errdetail("Junk after right bracket.")));

	ret = make_multirange(mltrngtypoid, rangetyp, range_count, ranges);
	PG_RETURN_MULTIRANGE_P(ret);
}

Datum
multirange_out(PG_FUNCTION_ARGS)
{
	MultirangeType *multirange = PG_GETARG_MULTIRANGE_P(0);
	Oid			mltrngtypoid = MultirangeTypeGetOid(multirange);
	MultirangeIOData *cache;
	StringInfoData buf;
	RangeType  *range;
	char	   *rangeStr;
	Pointer		ptr = (char *) multirange;
	Pointer		end = ptr + VARSIZE(multirange);
	int32		range_count = 0;

	cache = get_multirange_io_data(fcinfo, mltrngtypoid, IOFunc_output);

	initStringInfo(&buf);

	appendStringInfoChar(&buf, '{');

	ptr = (char *) MAXALIGN(multirange + 1);
	while (ptr < end)
	{
		if (range_count > 0)
			appendStringInfoChar(&buf, ',');
		range = (RangeType *) ptr;
		rangeStr = OutputFunctionCall(&cache->typioproc, RangeTypePGetDatum(range));
		appendStringInfoString(&buf, rangeStr);
		ptr += MAXALIGN(VARSIZE(range));
		range_count++;
	}

	appendStringInfoChar(&buf, '}');

	PG_RETURN_CSTRING(buf.data);
}

/*
 * Binary representation: First a int32-sized count of ranges, followed by
 * ranges in their native binary representation.
 */
Datum
multirange_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	Oid			mltrngtypoid = PG_GETARG_OID(1);
	int32		typmod = PG_GETARG_INT32(2);
	MultirangeIOData *cache;
	uint32		range_count;
	RangeType **ranges;
	MultirangeType *ret;
	StringInfoData tmpbuf;

	cache = get_multirange_io_data(fcinfo, mltrngtypoid, IOFunc_receive);

	range_count = pq_getmsgint(buf, 4);
	ranges = palloc(range_count * sizeof(RangeType *));

	initStringInfo(&tmpbuf);
	for (int i = 0; i < range_count; i++)
	{
		uint32		range_len = pq_getmsgint(buf, 4);
		const char *range_data = pq_getmsgbytes(buf, range_len);

		resetStringInfo(&tmpbuf);
		appendBinaryStringInfo(&tmpbuf, range_data, range_len);

		ranges[i] = DatumGetRangeTypeP(ReceiveFunctionCall(&cache->typioproc,
														   &tmpbuf,
														   cache->typioparam,
														   typmod));
	}
	pfree(tmpbuf.data);

	pq_getmsgend(buf);

	ret = make_multirange(mltrngtypoid, cache->typcache->rngtype,
						  range_count, ranges);
	PG_RETURN_MULTIRANGE_P(ret);
}

Datum
multirange_send(PG_FUNCTION_ARGS)
{
	MultirangeType *multirange = PG_GETARG_MULTIRANGE_P(0);
	Oid			mltrngtypoid = MultirangeTypeGetOid(multirange);
	StringInfo	buf = makeStringInfo();
	RangeType **ranges;
	int32		range_count;
	MultirangeIOData *cache;

	cache = get_multirange_io_data(fcinfo, mltrngtypoid, IOFunc_send);

	/* construct output */
	pq_begintypsend(buf);

	pq_sendint32(buf, multirange->rangeCount);

	multirange_deserialize(multirange, &range_count, &ranges);
	for (int i = 0; i < range_count; i++)
	{
		Datum		range;

		range = RangeTypePGetDatum(ranges[i]);
		range = PointerGetDatum(SendFunctionCall(&cache->typioproc, range));

		pq_sendint32(buf, VARSIZE(range) - VARHDRSZ);
		pq_sendbytes(buf, VARDATA(range), VARSIZE(range) - VARHDRSZ);
	}

	PG_RETURN_BYTEA_P(pq_endtypsend(buf));
}

Datum
multirange_typanalyze(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(true);
}

/*
 * get_multirange_io_data: get cached information needed for multirange type I/O
 *
 * The multirange I/O functions need a bit more cached info than other multirange
 * functions, so they store a MultirangeIOData struct in fn_extra, not just a
 * pointer to a type cache entry.
 */
static MultirangeIOData *
get_multirange_io_data(FunctionCallInfo fcinfo, Oid mltrngtypid, IOFuncSelector func)
{
	MultirangeIOData *cache = (MultirangeIOData *) fcinfo->flinfo->fn_extra;

	if (cache == NULL || cache->typcache->type_id != mltrngtypid)
	{
		Oid			typiofunc;
		int16		typlen;
		bool		typbyval;
		char		typalign;
		char		typdelim;

		cache = (MultirangeIOData *) MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
														sizeof(MultirangeIOData));
		cache->typcache = lookup_type_cache(mltrngtypid, TYPECACHE_MULTIRANGE_INFO);
		if (cache->typcache->rngtype == NULL)
			elog(ERROR, "type %u is not a multirange type", mltrngtypid);

		/* get_type_io_data does more than we need, but is convenient */
		get_type_io_data(cache->typcache->rngtype->type_id,
						 func,
						 &typlen,
						 &typbyval,
						 &typalign,
						 &typdelim,
						 &cache->typioparam,
						 &typiofunc);

		if (!OidIsValid(typiofunc))
		{
			/* this could only happen for receive or send */
			if (func == IOFunc_receive)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("no binary input function available for type %s",
								format_type_be(cache->typcache->rngtype->type_id))));
			else
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("no binary output function available for type %s",
								format_type_be(cache->typcache->rngtype->type_id))));
		}
		fmgr_info_cxt(typiofunc, &cache->typioproc,
					  fcinfo->flinfo->fn_mcxt);

		fcinfo->flinfo->fn_extra = (void *) cache;
	}

	return cache;
}

/*
 *----------------------------------------------------------
 * SUPPORT FUNCTIONS
 *
 *	 These functions aren't in pg_proc, but are useful for
 *	 defining new generic multirange functions in C.
 *----------------------------------------------------------
 */

/*
 * multirange_get_typcache: get cached information about a multirange type
 *
 * This is for use by multirange-related functions that follow the convention
 * of using the fn_extra field as a pointer to the type cache entry for
 * the multirange type.  Functions that need to cache more information than
 * that must fend for themselves.
 */
TypeCacheEntry *
multirange_get_typcache(FunctionCallInfo fcinfo, Oid mltrngtypid)
{
	TypeCacheEntry *typcache = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;

	if (typcache == NULL ||
		typcache->type_id != mltrngtypid)
	{
		typcache = lookup_type_cache(mltrngtypid, TYPECACHE_MULTIRANGE_INFO);
		if (typcache->rngtype == NULL)
			elog(ERROR, "type %u is not a multirange type", mltrngtypid);
		fcinfo->flinfo->fn_extra = (void *) typcache;
	}

	return typcache;
}

/*
 * This serializes the multirange from a list of non-null ranges.
 * It also sorts the ranges and merges any that touch.
 * The ranges should already be detoasted, and there should be no NULLs.
 * This should be used by most callers.
 *
 * Note that we may change the `ranges` parameter (the pointers, but not
 * any already-existing RangeType contents).
 */
MultirangeType *
make_multirange(Oid mltrngtypoid, TypeCacheEntry *rangetyp, int32 range_count,
				RangeType **ranges)
{
	MultirangeType *multirange;
	int			i;
	int32		bytelen;
	Pointer		ptr;

	/* Sort and merge input ranges. */
	range_count = multirange_canonicalize(rangetyp, range_count, ranges);

	/*
	 * Count space for varlena header, multirange type's OID, other fields,
	 * and padding so that RangeTypes start aligned.
	 */
	bytelen = MAXALIGN(sizeof(MultirangeType));

	/* Count space for all ranges */
	for (i = 0; i < range_count; i++)
		bytelen += MAXALIGN(VARSIZE(ranges[i]));

	/* Note: zero-fill is required here, just as in heap tuples */
	multirange = palloc0(bytelen);
	SET_VARSIZE(multirange, bytelen);

	/* Now fill in the datum */
	multirange->multirangetypid = mltrngtypoid;
	multirange->rangeCount = range_count;

	ptr = (char *) MAXALIGN(multirange + 1);
	for (i = 0; i < range_count; i++)
	{
		memcpy(ptr, ranges[i], VARSIZE(ranges[i]));
		ptr += MAXALIGN(VARSIZE(ranges[i]));
	}

	return multirange;
}

/*
 * Converts a list of arbitrary ranges into a list that is sorted and merged.
 * Changes the contents of `ranges`.
 *
 * Returns the number of slots actually used, which may be less than
 * input_range_count but never more.
 *
 * We assume that no input ranges are null, but empties are okay.
 */
static int32
multirange_canonicalize(TypeCacheEntry *rangetyp, int32 input_range_count,
						RangeType **ranges)
{
	RangeType  *lastRange = NULL;
	RangeType  *currentRange;
	int32		i;
	int32		output_range_count = 0;

	/* Sort the ranges so we can find the ones that overlap/meet. */
	qsort_arg(ranges, input_range_count, sizeof(RangeType *), range_compare,
			  rangetyp);

	/* Now merge where possible: */
	for (i = 0; i < input_range_count; i++)
	{
		currentRange = ranges[i];
		if (RangeIsEmpty(currentRange))
			continue;

		if (lastRange == NULL)
		{
			ranges[output_range_count++] = lastRange = currentRange;
			continue;
		}

		/*
		 * range_adjacent_internal gives true if *either* A meets B or B meets
		 * A, which is not quite want we want, but we rely on the sorting
		 * above to rule out B meets A ever happening.
		 */
		if (range_adjacent_internal(rangetyp, lastRange, currentRange))
		{
			/* The two ranges touch (without overlap), so merge them: */
			ranges[output_range_count - 1] = lastRange =
				range_union_internal(rangetyp, lastRange, currentRange, false);
		}
		else if (range_before_internal(rangetyp, lastRange, currentRange))
		{
			/* There's a gap, so make a new entry: */
			lastRange = ranges[output_range_count] = currentRange;
			output_range_count++;
		}
		else
		{
			/* They must overlap, so merge them: */
			ranges[output_range_count - 1] = lastRange =
				range_union_internal(rangetyp, lastRange, currentRange, true);
		}
	}

	return output_range_count;
}

/*
 * multirange_deserialize: deconstruct a multirange value
 *
 * NB: the given multirange object must be fully detoasted; it cannot have a
 * short varlena header.
 */
void
multirange_deserialize(MultirangeType *multirange,
					   int32 *range_count, RangeType ***ranges)
{
	RangeType  *r;
	Pointer		ptr;
	Pointer		end;
	int32		i;

	*range_count = multirange->rangeCount;
	if (*range_count == 0)
	{
		ranges = NULL;
		return;
	}

	*ranges = palloc0(*range_count * sizeof(RangeType *));

	ptr = (char *) multirange;
	end = ptr + VARSIZE(multirange);
	ptr = (char *) MAXALIGN(multirange + 1);
	i = 0;
	while (ptr < end)
	{
		r = (RangeType *) ptr;
		(*ranges)[i++] = r;

		ptr += MAXALIGN(VARSIZE(r));
	}
}

MultirangeType *
make_empty_multirange(Oid mltrngtypoid, TypeCacheEntry *rangetyp)
{
	return make_multirange(mltrngtypoid, rangetyp, 0, NULL);
}

/*
 *----------------------------------------------------------
 * GENERIC FUNCTIONS
 *----------------------------------------------------------
 */

/*
 * Construct multirange value from zero or more ranges.
 * Since this is a variadic function we get passed an array.
 * The array must contain range types that match our return value,
 * and there must be no NULLs.
 */
Datum
multirange_constructor2(PG_FUNCTION_ARGS)
{
	Oid			mltrngtypid = get_fn_expr_rettype(fcinfo->flinfo);
	Oid			rngtypid;
	TypeCacheEntry *typcache;
	TypeCacheEntry *rangetyp;
	ArrayType  *rangeArray;
	int			range_count;
	Datum	   *elements;
	bool	   *nulls;
	RangeType **ranges;
	int			dims;
	int			i;

	typcache = multirange_get_typcache(fcinfo, mltrngtypid);
	rangetyp = typcache->rngtype;

	/*
	 * A no-arg invocation should call multirange_constructor0 instead, but
	 * returning an empty range is what that does.
	 */

	if (PG_NARGS() == 0)
		PG_RETURN_MULTIRANGE_P(make_multirange(mltrngtypid, rangetyp, 0, NULL));

	/*
	 * These checks should be guaranteed by our signature, but let's do them
	 * just in case.
	 */

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("multirange values cannot contain NULL members")));

	rangeArray = PG_GETARG_ARRAYTYPE_P(0);

	dims = ARR_NDIM(rangeArray);
	if (dims > 1)
		ereport(ERROR,
				(errcode(ERRCODE_CARDINALITY_VIOLATION),
				 errmsg("multiranges cannot be constructed from multi-dimensional arrays")));

	rngtypid = ARR_ELEMTYPE(rangeArray);
	if (rngtypid != rangetyp->type_id)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("type %u does not match constructor type", rngtypid)));

	/*
	 * Be careful: we can still be called with zero ranges, like this:
	 * `int4multirange(variadic '{}'::int4range[])
	 */
	if (dims == 0)
	{
		range_count = 0;
		ranges = NULL;
	}
	else
	{
		deconstruct_array(rangeArray, rngtypid, rangetyp->typlen, rangetyp->typbyval,
						  rangetyp->typalign, &elements, &nulls, &range_count);

		ranges = palloc0(range_count * sizeof(RangeType *));
		for (i = 0; i < range_count; i++)
		{
			if (nulls[i])
				ereport(ERROR,
						(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						 errmsg("multirange values cannot contain NULL members")));

			/* make_multirange will do its own copy */
			ranges[i] = DatumGetRangeTypeP(elements[i]);
		}
	}

	PG_RETURN_MULTIRANGE_P(make_multirange(mltrngtypid, rangetyp, range_count, ranges));
}

/*
 * Construct multirange value from a single range.
 * It'd be nice if we could just use multirange_constructor2
 * for this case, but we need a non-variadic single-arg function
 * to let us define a CAST from a range to its multirange.
 */
Datum
multirange_constructor1(PG_FUNCTION_ARGS)
{
	Oid			mltrngtypid = get_fn_expr_rettype(fcinfo->flinfo);
	Oid			rngtypid;
	TypeCacheEntry *typcache;
	TypeCacheEntry *rangetyp;
	RangeType  *range;

	typcache = multirange_get_typcache(fcinfo, mltrngtypid);
	rangetyp = typcache->rngtype;

	/*
	 * These checks should be guaranteed by our signature, but let's do them
	 * just in case.
	 */

	if (PG_ARGISNULL(0))
				ereport(ERROR,
						(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						 errmsg("multirange values cannot contain NULL members")));

	range = PG_GETARG_RANGE_P(0);

	/* Make sure the range type matches. */
	rngtypid = RangeTypeGetOid(range);
	if (rngtypid != rangetyp->type_id)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("type %u does not match constructor type", rngtypid)));

	PG_RETURN_MULTIRANGE_P(make_multirange(mltrngtypid, rangetyp, 1, &range));
}

/*
 * Constructor just like multirange_constructor1,
 * but opr_sanity gets angry if the same internal function
 * handles multiple functions with different arg counts.
 */
Datum
multirange_constructor0(PG_FUNCTION_ARGS)
{
	Oid			mltrngtypid = get_fn_expr_rettype(fcinfo->flinfo);
	TypeCacheEntry *typcache;
	TypeCacheEntry *rangetyp;

	typcache = multirange_get_typcache(fcinfo, mltrngtypid);
	rangetyp = typcache->rngtype;

	/* We should always be called with no arguments */

	if (PG_NARGS() == 0)
		PG_RETURN_MULTIRANGE_P(make_multirange(mltrngtypid, rangetyp, 0, NULL));
	else
		elog(ERROR,		/* can't happen */
			 "niladic multirange constructor must not receive arguments");
}


/* multirange, multirange -> multirange type functions */

/* multirange union */
Datum
multirange_union(PG_FUNCTION_ARGS)
{
	MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
	MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);
	TypeCacheEntry *typcache;
	int32		range_count1;
	int32		range_count2;
	int32		range_count3;
	RangeType **ranges1;
	RangeType **ranges2;
	RangeType **ranges3;

	if (MultirangeIsEmpty(mr1))
		PG_RETURN_MULTIRANGE_P(mr2);
	if (MultirangeIsEmpty(mr2))
		PG_RETURN_MULTIRANGE_P(mr1);

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

	multirange_deserialize(mr1, &range_count1, &ranges1);
	multirange_deserialize(mr2, &range_count2, &ranges2);

	range_count3 = range_count1 + range_count2;
	ranges3 = palloc0(range_count3 * sizeof(RangeType *));
	memcpy(ranges3, ranges1, range_count1 * sizeof(RangeType *));
	memcpy(ranges3 + range_count1, ranges2, range_count2 * sizeof(RangeType *));
	PG_RETURN_MULTIRANGE_P(make_multirange(typcache->type_id, typcache->rngtype,
										   range_count3, ranges3));
}

/* multirange minus */
Datum
multirange_minus(PG_FUNCTION_ARGS)
{
	MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
	MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);
	Oid			mltrngtypoid = MultirangeTypeGetOid(mr1);
	TypeCacheEntry *typcache;
	TypeCacheEntry *rangetyp;
	int32		range_count1;
	int32		range_count2;
	RangeType **ranges1;
	RangeType **ranges2;

	typcache = multirange_get_typcache(fcinfo, mltrngtypoid);
	rangetyp = typcache->rngtype;

	if (MultirangeIsEmpty(mr1) || MultirangeIsEmpty(mr2))
		PG_RETURN_MULTIRANGE_P(mr1);

	multirange_deserialize(mr1, &range_count1, &ranges1);
	multirange_deserialize(mr2, &range_count2, &ranges2);

	PG_RETURN_MULTIRANGE_P(multirange_minus_internal(mltrngtypoid,
													 rangetyp,
													 range_count1,
													 ranges1,
													 range_count2,
													 ranges2));
}

MultirangeType *
multirange_minus_internal(Oid mltrngtypoid, TypeCacheEntry *rangetyp,
						  int32 range_count1, RangeType **ranges1,
						  int32 range_count2, RangeType **ranges2)
{
	RangeType  *r1;
	RangeType  *r2;
	RangeType **ranges3;
	int32		range_count3;
	int32		i1;
	int32		i2;

	/*
	 * Worst case: every range in ranges1 makes a different cut to some range
	 * in ranges2.
	 */
	ranges3 = palloc0((range_count1 + range_count2) * sizeof(RangeType *));
	range_count3 = 0;

	/*
	 * For each range in mr1, keep subtracting until it's gone or the ranges
	 * in mr2 have passed it. After a subtraction we assign what's left back
	 * to r1. The parallel progress through mr1 and mr2 is similar to
	 * multirange_overlaps_multirange_internal.
	 */
	r2 = ranges2[0];
	for (i1 = 0, i2 = 0; i1 < range_count1; i1++)
	{
		r1 = ranges1[i1];

		/* Discard r2s while r2 << r1 */
		while (r2 != NULL && range_before_internal(rangetyp, r2, r1))
		{
			r2 = ++i2 >= range_count2 ? NULL : ranges2[i2];
		}

		while (r2 != NULL)
		{
			if (range_split_internal(rangetyp, r1, r2, &ranges3[range_count3], &r1))
			{
				/*
				 * If r2 takes a bite out of the middle of r1, we need two
				 * outputs
				 */
				range_count3++;
				r2 = ++i2 >= range_count2 ? NULL : ranges2[i2];

			}
			else if (range_overlaps_internal(rangetyp, r1, r2))
			{
				/*
				 * If r2 overlaps r1, replace r1 with r1 - r2.
				 */
				r1 = range_minus_internal(rangetyp, r1, r2);

				/*
				 * If r2 goes past r1, then we need to stay with it, in case
				 * it hits future r1s. Otherwise we need to keep r1, in case
				 * future r2s hit it. Since we already subtracted, there's no
				 * point in using the overright/overleft calls.
				 */
				if (RangeIsEmpty(r1) || range_before_internal(rangetyp, r1, r2))
					break;
				else
					r2 = ++i2 >= range_count2 ? NULL : ranges2[i2];

			}
			else
			{
				/*
				 * This and all future r2s are past r1, so keep them. Also
				 * assign whatever is left of r1 to the result.
				 */
				break;
			}
		}

		/*
		 * Nothing else can remove anything from r1, so keep it. Even if r1 is
		 * empty here, make_multirange will remove it.
		 */
		ranges3[range_count3++] = r1;
	}

	return make_multirange(mltrngtypoid, rangetyp, range_count3, ranges3);
}

/* multirange intersection */
Datum
multirange_intersect(PG_FUNCTION_ARGS)
{
	MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
	MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);
	Oid			mltrngtypoid = MultirangeTypeGetOid(mr1);
	TypeCacheEntry *typcache;
	TypeCacheEntry *rangetyp;
	int32		range_count1;
	int32		range_count2;
	RangeType **ranges1;
	RangeType **ranges2;

	typcache = multirange_get_typcache(fcinfo, mltrngtypoid);
	rangetyp = typcache->rngtype;

	if (MultirangeIsEmpty(mr1) || MultirangeIsEmpty(mr2))
		PG_RETURN_MULTIRANGE_P(make_empty_multirange(mltrngtypoid, rangetyp));

	multirange_deserialize(mr1, &range_count1, &ranges1);
	multirange_deserialize(mr2, &range_count2, &ranges2);

	PG_RETURN_MULTIRANGE_P(multirange_intersect_internal(mltrngtypoid,
														 rangetyp,
														 range_count1,
														 ranges1,
														 range_count2,
														 ranges2));
}

MultirangeType *
multirange_intersect_internal(Oid mltrngtypoid, TypeCacheEntry *rangetyp,
							  int32 range_count1, RangeType **ranges1,
							  int32 range_count2, RangeType **ranges2)
{
	RangeType  *r1;
	RangeType  *r2;
	RangeType **ranges3;
	int32		range_count3;
	int32		i1;
	int32		i2;

	if (range_count1 == 0 || range_count2 == 0)
		return make_multirange(mltrngtypoid, rangetyp, 0, NULL);

	/*-----------------------------------------------
	 * Worst case is a stitching pattern like this:
	 *
	 * mr1: --- --- --- ---
	 * mr2:   --- --- ---
	 * mr3:   - - - - - -
	 *
	 * That seems to be range_count1 + range_count2 - 1,
	 * but one extra won't hurt.
	 *-----------------------------------------------
	 */
	ranges3 = palloc0((range_count1 + range_count2) * sizeof(RangeType *));
	range_count3 = 0;

	/*
	 * For each range in mr1, keep intersecting until the ranges in mr2 have
	 * passed it. The parallel progress through mr1 and mr2 is similar to
	 * multirange_minus_multirange_internal, but we don't have to assign back
	 * to r1.
	 */
	r2 = ranges2[0];
	for (i1 = 0, i2 = 0; i1 < range_count1; i1++)
	{
		r1 = ranges1[i1];

		/* Discard r2s while r2 << r1 */
		while (r2 != NULL && range_before_internal(rangetyp, r2, r1))
		{
			r2 = ++i2 >= range_count2 ? NULL : ranges2[i2];
		}

		while (r2 != NULL)
		{
			if (range_overlaps_internal(rangetyp, r1, r2))
			{
				/* Keep the overlapping part */
				ranges3[range_count3++] = range_intersect_internal(rangetyp, r1, r2);

				/* If we "used up" all of r2, go to the next one... */
				if (range_overleft_internal(rangetyp, r2, r1))
					r2 = ++i2 >= range_count2 ? NULL : ranges2[i2];

				/* ...otherwise go to the next r1 */
				else
					break;
			}
			else
				/* We're past r1, so move to the next one */
				break;
		}

		/* If we're out of r2s, there can be no more intersections */
		if (r2 == NULL)
			break;
	}

	return make_multirange(mltrngtypoid, rangetyp, range_count3, ranges3);
}

/*
 * range_agg_transfn: combine adjacent/overlapping ranges.
 *
 * All we do here is gather the input ranges into an array
 * so that the finalfn can sort and combine them.
 */
Datum
range_agg_transfn(PG_FUNCTION_ARGS)
{
	MemoryContext aggContext;
	Oid			rngtypoid;
	ArrayBuildState *state;

	if (!AggCheckCallContext(fcinfo, &aggContext))
		elog(ERROR, "range_agg_transfn called in non-aggregate context");

	rngtypoid = get_fn_expr_argtype(fcinfo->flinfo, 1);
	if (!type_is_range(rngtypoid))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("range_agg must be called with a range")));

	if (PG_ARGISNULL(0))
		state = initArrayResult(rngtypoid, aggContext, false);
	else
		state = (ArrayBuildState *) PG_GETARG_POINTER(0);

	/* skip NULLs */
	if (!PG_ARGISNULL(1))
		accumArrayResult(state, PG_GETARG_DATUM(1), false, rngtypoid, aggContext);

	PG_RETURN_POINTER(state);
}

/*
 * range_agg_finalfn: use our internal array to merge touching ranges.
 */
Datum
range_agg_finalfn(PG_FUNCTION_ARGS)
{
	MemoryContext aggContext;
	Oid			mltrngtypoid;
	TypeCacheEntry *typcache;
	ArrayBuildState *state;
	int32		range_count;
	RangeType **ranges;
	int			i;

	if (!AggCheckCallContext(fcinfo, &aggContext))
		elog(ERROR, "range_agg_finalfn called in non-aggregate context");

	state = PG_ARGISNULL(0) ? NULL : (ArrayBuildState *) PG_GETARG_POINTER(0);
	if (state == NULL)
		/* This shouldn't be possible, but just in case.... */
		PG_RETURN_NULL();

	/* Also return NULL if we had zero inputs, like other aggregates */
	range_count = state->nelems;
	if (range_count == 0)
		PG_RETURN_NULL();

	mltrngtypoid = get_fn_expr_rettype(fcinfo->flinfo);
	typcache = multirange_get_typcache(fcinfo, mltrngtypoid);

	ranges = palloc0(range_count * sizeof(RangeType *));
	for (i = 0; i < range_count; i++)
		ranges[i] = DatumGetRangeTypeP(state->dvalues[i]);

	PG_RETURN_MULTIRANGE_P(make_multirange(mltrngtypoid, typcache->rngtype, range_count, ranges));
}

Datum
multirange_intersect_agg_transfn(PG_FUNCTION_ARGS)
{
	MemoryContext aggContext;
	Oid			mltrngtypoid;
	TypeCacheEntry *typcache;
	MultirangeType *result;
	MultirangeType *current;
	int32		range_count1;
	int32		range_count2;
	RangeType **ranges1;
	RangeType **ranges2;

	if (!AggCheckCallContext(fcinfo, &aggContext))
		elog(ERROR, "multirange_intersect_agg_transfn called in non-aggregate context");

	mltrngtypoid = get_fn_expr_argtype(fcinfo->flinfo, 1);
	if (!type_is_multirange(mltrngtypoid))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("range_intersect_agg must be called with a multirange")));

	typcache = multirange_get_typcache(fcinfo, mltrngtypoid);

	/* strictness ensures these are non-null */
	result = PG_GETARG_MULTIRANGE_P(0);
	current = PG_GETARG_MULTIRANGE_P(1);

	multirange_deserialize(result, &range_count1, &ranges1);
	multirange_deserialize(current, &range_count2, &ranges2);

	result = multirange_intersect_internal(mltrngtypoid,
										   typcache->rngtype,
										   range_count1,
										   ranges1,
										   range_count2,
										   ranges2);
	PG_RETURN_RANGE_P(result);
}


/* multirange -> element type functions */

/* extract lower bound value */
Datum
multirange_lower(PG_FUNCTION_ARGS)
{
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);
	TypeCacheEntry *typcache;
	int32		range_count;
	RangeType **ranges;
	bool		isnull;
	Datum		result;

	if (MultirangeIsEmpty(mr))
		PG_RETURN_NULL();

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	multirange_deserialize(mr, &range_count, &ranges);

	result = range_lower_internal(typcache->rngtype, ranges[0], &isnull);

	if (isnull)
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(result);
}

/* extract upper bound value */
Datum
multirange_upper(PG_FUNCTION_ARGS)
{
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);
	TypeCacheEntry *typcache;
	int32		range_count;
	RangeType **ranges;
	bool		isnull;
	Datum		result;

	if (MultirangeIsEmpty(mr))
		PG_RETURN_NULL();

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	multirange_deserialize(mr, &range_count, &ranges);

	result = range_upper_internal(typcache->rngtype, ranges[range_count - 1], &isnull);

	if (isnull)
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(result);
}


/* multirange -> bool functions */

/* is multirange empty? */
Datum
multirange_empty(PG_FUNCTION_ARGS)
{
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);

	PG_RETURN_BOOL(mr->rangeCount == 0);
}

/* is lower bound inclusive? */
Datum
multirange_lower_inc(PG_FUNCTION_ARGS)
{
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);
	int32		range_count;
	RangeType **ranges;

	if (MultirangeIsEmpty(mr))
		PG_RETURN_BOOL(false);

	multirange_deserialize(mr, &range_count, &ranges);

	PG_RETURN_BOOL(range_has_flag(ranges[0], RANGE_LB_INC));
}

/* is upper bound inclusive? */
Datum
multirange_upper_inc(PG_FUNCTION_ARGS)
{
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);
	int32		range_count;
	RangeType **ranges;

	if (MultirangeIsEmpty(mr))
		PG_RETURN_BOOL(false);

	multirange_deserialize(mr, &range_count, &ranges);

	PG_RETURN_BOOL(range_has_flag(ranges[range_count - 1], RANGE_UB_INC));
}

/* is lower bound infinite? */
Datum
multirange_lower_inf(PG_FUNCTION_ARGS)
{
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);
	int32		range_count;
	RangeType **ranges;

	if (MultirangeIsEmpty(mr))
		PG_RETURN_BOOL(false);

	multirange_deserialize(mr, &range_count, &ranges);

	PG_RETURN_BOOL(range_has_flag(ranges[0], RANGE_LB_INF));
}

/* is upper bound infinite? */
Datum
multirange_upper_inf(PG_FUNCTION_ARGS)
{
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);
	int32		range_count;
	RangeType **ranges;

	if (MultirangeIsEmpty(mr))
		PG_RETURN_BOOL(false);

	multirange_deserialize(mr, &range_count, &ranges);

	PG_RETURN_BOOL(range_has_flag(ranges[range_count - 1], RANGE_UB_INF));
}



/* multirange, element -> bool functions */

/* contains? */
Datum
multirange_contains_elem(PG_FUNCTION_ARGS)
{
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);
	Datum		val = PG_GETARG_DATUM(1);
	TypeCacheEntry *typcache;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	PG_RETURN_BOOL(multirange_contains_elem_internal(typcache, mr, val));
}

/* contained by? */
Datum
elem_contained_by_multirange(PG_FUNCTION_ARGS)
{
	Datum		val = PG_GETARG_DATUM(0);
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(1);
	TypeCacheEntry *typcache;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	PG_RETURN_BOOL(multirange_contains_elem_internal(typcache, mr, val));
}

/*
 * Test whether multirange mr contains a specific element value.
 */
bool
multirange_contains_elem_internal(TypeCacheEntry *typcache, MultirangeType *mr, Datum val)
{
	TypeCacheEntry *rangetyp;
	int32		range_count;
	RangeType **ranges;
	RangeType  *r;
	int			i;

	rangetyp = typcache->rngtype;

	multirange_deserialize(mr, &range_count, &ranges);

	for (i = 0; i < range_count; i++)
	{
		r = ranges[i];
		if (range_contains_elem_internal(rangetyp, r, val))
			return true;
	}

	return false;
}

/* multirange, range -> bool functions */

/* contains? */
Datum
multirange_contains_range(PG_FUNCTION_ARGS)
{
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);
	RangeType  *r = PG_GETARG_RANGE_P(1);
	TypeCacheEntry *typcache;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	PG_RETURN_BOOL(multirange_contains_range_internal(typcache, mr, r));
}

/* contained by? */
Datum
range_contained_by_multirange(PG_FUNCTION_ARGS)
{
	RangeType  *r = PG_GETARG_RANGE_P(0);
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(1);
	TypeCacheEntry *typcache;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	PG_RETURN_BOOL(multirange_contains_range_internal(typcache, mr, r));
}

/*
 * Test whether multirange mr contains a specific range r.
 */
bool
multirange_contains_range_internal(TypeCacheEntry *typcache, MultirangeType *mr, RangeType *r)
{
	TypeCacheEntry *rangetyp;
	int32		range_count;
	RangeType **ranges;
	RangeType  *mrr;
	int			i;

	rangetyp = typcache->rngtype;

	/*
	 * Every multirange contains an infinite number of empty ranges, even an
	 * empty one.
	 */
	if (RangeIsEmpty(r))
		return true;

	multirange_deserialize(mr, &range_count, &ranges);

	for (i = 0; i < range_count; i++)
	{
		mrr = ranges[i];
		if (range_contains_internal(rangetyp, mrr, r))
			return true;
	}

	return false;
}


/* multirange, multirange -> bool functions */

/* equality (internal version) */
bool
multirange_eq_internal(TypeCacheEntry *typcache, MultirangeType *mr1, MultirangeType *mr2)
{
	int32		range_count_1;
	int32		range_count_2;
	int32		i;
	RangeType **ranges1;
	RangeType **ranges2;
	RangeType  *r1;
	RangeType  *r2;

	/* Different types should be prevented by ANYMULTIRANGE matching rules */
	if (MultirangeTypeGetOid(mr1) != MultirangeTypeGetOid(mr2))
		elog(ERROR, "multirange types do not match");

	multirange_deserialize(mr1, &range_count_1, &ranges1);
	multirange_deserialize(mr2, &range_count_2, &ranges2);

	if (range_count_1 != range_count_2)
		return false;

	for (i = 0; i < range_count_1; i++)
	{
		r1 = ranges1[i];
		r2 = ranges2[i];

		if (!range_eq_internal(typcache->rngtype, r1, r2))
			return false;
	}

	return true;
}

/* equality */
Datum
multirange_eq(PG_FUNCTION_ARGS)
{
	MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
	MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);
	TypeCacheEntry *typcache;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

	PG_RETURN_BOOL(multirange_eq_internal(typcache, mr1, mr2));
}

/* inequality (internal version) */
bool
multirange_ne_internal(TypeCacheEntry *typcache, MultirangeType *mr1, MultirangeType *mr2)
{
	return (!multirange_eq_internal(typcache, mr1, mr2));
}

/* inequality */
Datum
multirange_ne(PG_FUNCTION_ARGS)
{
	MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
	MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);
	TypeCacheEntry *typcache;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

	PG_RETURN_BOOL(multirange_ne_internal(typcache, mr1, mr2));
}

/* overlaps? */
Datum
range_overlaps_multirange(PG_FUNCTION_ARGS)
{
	RangeType  *r = PG_GETARG_RANGE_P(0);
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(1);
	TypeCacheEntry *typcache;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	PG_RETURN_BOOL(range_overlaps_multirange_internal(typcache, r, mr));
}

Datum
multirange_overlaps_range(PG_FUNCTION_ARGS)
{
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);
	RangeType  *r = PG_GETARG_RANGE_P(1);
	TypeCacheEntry *typcache;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	PG_RETURN_BOOL(range_overlaps_multirange_internal(typcache, r, mr));
}

Datum
multirange_overlaps_multirange(PG_FUNCTION_ARGS)
{
	MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
	MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);
	TypeCacheEntry *typcache;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

	PG_RETURN_BOOL(multirange_overlaps_multirange_internal(typcache, mr1, mr2));
}

bool
range_overlaps_multirange_internal(TypeCacheEntry *typcache, RangeType *r, MultirangeType *mr)
{
	TypeCacheEntry *rangetyp;
	int32		range_count;
	int32		i;
	RangeType **ranges;
	RangeType  *mrr;

	/*
	 * Empties never overlap, even with empties. (This seems strange since
	 * they *do* contain each other, but we want to follow how ranges work.)
	 */
	if (RangeIsEmpty(r) || MultirangeIsEmpty(mr))
		return false;

	rangetyp = typcache->rngtype;

	multirange_deserialize(mr, &range_count, &ranges);

	/* Scan through mrr and once it gets to r they either overlap or not */
	mrr = ranges[0];

	/* Discard mrrs while mrr << r */
	i = 0;
	while (range_before_internal(rangetyp, mrr, r))
	{
		if (++i >= range_count)
			return false;
		mrr = ranges[i];
	}

	/* Now either we overlap or we passed r */
	return range_overlaps_internal(rangetyp, mrr, r);
}

bool
multirange_overlaps_multirange_internal(TypeCacheEntry *typcache, MultirangeType *mr1,
										MultirangeType *mr2)
{
	TypeCacheEntry *rangetyp;
	int32		range_count1;
	int32		range_count2;
	int32		i1;
	int32		i2;
	RangeType **ranges1;
	RangeType **ranges2;
	RangeType  *r1;
	RangeType  *r2;

	/*
	 * Empties never overlap, even with empties. (This seems strange since
	 * they *do* contain each other, but we want to follow how ranges work.)
	 */
	if (MultirangeIsEmpty(mr1) || MultirangeIsEmpty(mr2))
		return false;

	rangetyp = typcache->rngtype;

	multirange_deserialize(mr1, &range_count1, &ranges1);
	multirange_deserialize(mr2, &range_count2, &ranges2);

	/*
	 * Every range in mr1 gets a chance to overlap with the ranges in mr2, but
	 * we can use their ordering to avoid O(n^2). This is similar to
	 * range_overlaps_multirange where r1 : r2 :: mrr : r, but there if we
	 * don't find an overlap with r we're done, and here if we don't find an
	 * overlap with r2 we try the next r2.
	 */
	r1 = ranges1[0];
	for (i1 = 0, i2 = 0; i2 < range_count2; i2++)
	{
		r2 = ranges2[i2];

		/* Discard r1s while r1 << r2 */
		while (range_before_internal(rangetyp, r1, r2))
		{
			if (++i1 >= range_count1)
				return false;
			r1 = ranges1[i1];
		}

		/*
		 * If r1 && r2, we're done, otherwise we failed to find an overlap for
		 * r2, so go to the next one.
		 */
		if (range_overlaps_internal(rangetyp, r1, r2))
			return true;
	}

	/* We looked through all of mr2 without finding an overlap */
	return false;
}

/* does not extend to right of? */
Datum
range_overleft_multirange(PG_FUNCTION_ARGS)
{
	RangeType  *r = PG_GETARG_RANGE_P(0);
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(1);
	TypeCacheEntry *typcache;
	int32		range_count;
	RangeType **ranges;

	if (RangeIsEmpty(r) || MultirangeIsEmpty(mr))
		PG_RETURN_BOOL(false);

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	multirange_deserialize(mr, &range_count, &ranges);

	PG_RETURN_BOOL(range_overleft_internal(typcache->rngtype, r, ranges[range_count - 1]));
}

Datum
multirange_overleft_range(PG_FUNCTION_ARGS)
{
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);
	RangeType  *r = PG_GETARG_RANGE_P(1);
	TypeCacheEntry *typcache;
	int32		range_count;
	RangeType **ranges;

	if (RangeIsEmpty(r) || MultirangeIsEmpty(mr))
		PG_RETURN_BOOL(false);

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	multirange_deserialize(mr, &range_count, &ranges);

	PG_RETURN_BOOL(range_overleft_internal(typcache->rngtype,
										   ranges[range_count - 1], r));
}

Datum
multirange_overleft_multirange(PG_FUNCTION_ARGS)
{
	MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
	MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);
	TypeCacheEntry *typcache;
	int32		range_count1;
	int32		range_count2;
	RangeType **ranges1;
	RangeType **ranges2;

	if (MultirangeIsEmpty(mr1) || MultirangeIsEmpty(mr2))
		PG_RETURN_BOOL(false);

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

	multirange_deserialize(mr1, &range_count1, &ranges1);
	multirange_deserialize(mr2, &range_count2, &ranges2);

	PG_RETURN_BOOL(range_overleft_internal(typcache->rngtype,
										   ranges1[range_count1 - 1],
										   ranges2[range_count2 - 1]));
}

/* does not extend to left of? */
Datum
range_overright_multirange(PG_FUNCTION_ARGS)
{
	RangeType  *r = PG_GETARG_RANGE_P(0);
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(1);
	TypeCacheEntry *typcache;
	int32		range_count;
	RangeType **ranges;

	if (RangeIsEmpty(r) || MultirangeIsEmpty(mr))
		PG_RETURN_BOOL(false);

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	multirange_deserialize(mr, &range_count, &ranges);

	PG_RETURN_BOOL(range_overright_internal(typcache->rngtype, r, ranges[0]));
}

Datum
multirange_overright_range(PG_FUNCTION_ARGS)
{
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);
	RangeType  *r = PG_GETARG_RANGE_P(1);
	TypeCacheEntry *typcache;
	int32		range_count;
	RangeType **ranges;

	if (RangeIsEmpty(r) || MultirangeIsEmpty(mr))
		PG_RETURN_BOOL(false);

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	multirange_deserialize(mr, &range_count, &ranges);

	PG_RETURN_BOOL(range_overright_internal(typcache->rngtype, ranges[0], r));
}

Datum
multirange_overright_multirange(PG_FUNCTION_ARGS)
{
	MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
	MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);
	TypeCacheEntry *typcache;
	int32		range_count1;
	int32		range_count2;
	RangeType **ranges1;
	RangeType **ranges2;

	if (MultirangeIsEmpty(mr1) || MultirangeIsEmpty(mr2))
		PG_RETURN_BOOL(false);

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

	multirange_deserialize(mr1, &range_count1, &ranges1);
	multirange_deserialize(mr2, &range_count2, &ranges2);

	PG_RETURN_BOOL(range_overright_internal(typcache->rngtype,
											ranges1[0],
											ranges2[0]));
}

/* contains? */
Datum
multirange_contains_multirange(PG_FUNCTION_ARGS)
{
	MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
	MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);
	TypeCacheEntry *typcache;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

	PG_RETURN_BOOL(multirange_contains_multirange_internal(typcache, mr1, mr2));
}

/* contained by? */
Datum
multirange_contained_by_multirange(PG_FUNCTION_ARGS)
{
	MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
	MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);
	TypeCacheEntry *typcache;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

	PG_RETURN_BOOL(multirange_contains_multirange_internal(typcache, mr2, mr1));
}

/*
 * Test whether multirange mr1 contains every range from another multirange mr2.
 */
bool
multirange_contains_multirange_internal(TypeCacheEntry *typcache,
										MultirangeType *mr1, MultirangeType *mr2)
{
	TypeCacheEntry *rangetyp;
	int32		range_count1;
	int32		range_count2;
	RangeType **ranges1;
	RangeType **ranges2;
	RangeType  *r1;
	RangeType  *r2;
	int			i1,
				i2;

	rangetyp = typcache->rngtype;

	multirange_deserialize(mr1, &range_count1, &ranges1);
	multirange_deserialize(mr2, &range_count2, &ranges2);

	/*
	 * We follow the same logic for empties as ranges: - an empty multirange
	 * contains an empty range/multirange. - an empty multirange can't contain
	 * any other range/multirange. - an empty multirange is contained by any
	 * other range/multirange.
	 */

	if (range_count2 == 0)
		return true;
	if (range_count1 == 0)
		return false;

	/*
	 * Every range in mr2 must be contained by some range in mr1. To avoid
	 * O(n^2) we walk through both ranges in tandem.
	 */
	r1 = ranges1[0];
	for (i1 = 0, i2 = 0; i2 < range_count2; i2++)
	{
		r2 = ranges2[i2];

		/* Discard r1s while r1 << r2 */
		while (range_before_internal(rangetyp, r1, r2))
		{
			if (++i1 >= range_count1)
				return false;
			r1 = ranges1[i1];
		}

		/*
		 * If r1 @> r2, go to the next r2, otherwise return false (since every
		 * r1[n] and r1[n+1] must have a gap). Note this will give weird
		 * answers if you don't canonicalize, e.g. with a custom
		 * int2multirange {[1,1], [2,2]} there is a "gap". But that is
		 * consistent with other range operators, e.g. '[1,1]'::int2range -|-
		 * '[2,2]'::int2range is false.
		 */
		if (!range_contains_internal(rangetyp, r1, r2))
			return false;
	}

	/* All ranges in mr2 are satisfied */
	return true;
}

/* strictly left of? */
Datum
range_before_multirange(PG_FUNCTION_ARGS)
{
	RangeType  *r = PG_GETARG_RANGE_P(0);
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(1);
	TypeCacheEntry *typcache;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	PG_RETURN_BOOL(range_before_multirange_internal(typcache, r, mr));
}

Datum
multirange_before_range(PG_FUNCTION_ARGS)
{
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);
	RangeType  *r = PG_GETARG_RANGE_P(1);
	TypeCacheEntry *typcache;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	PG_RETURN_BOOL(range_after_multirange_internal(typcache, r, mr));
}

Datum
multirange_before_multirange(PG_FUNCTION_ARGS)
{
	MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
	MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);
	TypeCacheEntry *typcache;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

	PG_RETURN_BOOL(multirange_before_multirange_internal(typcache, mr1, mr2));
}

/* strictly right of? */
Datum
range_after_multirange(PG_FUNCTION_ARGS)
{
	RangeType  *r = PG_GETARG_RANGE_P(0);
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(1);
	TypeCacheEntry *typcache;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	PG_RETURN_BOOL(range_after_multirange_internal(typcache, r, mr));
}

Datum
multirange_after_range(PG_FUNCTION_ARGS)
{
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);
	RangeType  *r = PG_GETARG_RANGE_P(1);
	TypeCacheEntry *typcache;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	PG_RETURN_BOOL(range_before_multirange_internal(typcache, r, mr));
}

Datum
multirange_after_multirange(PG_FUNCTION_ARGS)
{
	MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
	MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);
	TypeCacheEntry *typcache;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

	PG_RETURN_BOOL(multirange_before_multirange_internal(typcache, mr2, mr1));
}

/* strictly left of? (internal version) */
bool
range_before_multirange_internal(TypeCacheEntry *typcache, RangeType *r,
								 MultirangeType *mr)
{
	int32		range_count;
	RangeType **ranges;

	if (RangeIsEmpty(r) || MultirangeIsEmpty(mr))
		return false;

	multirange_deserialize(mr, &range_count, &ranges);

	return range_before_internal(typcache->rngtype, r, ranges[0]);
}

bool
multirange_before_multirange_internal(TypeCacheEntry *typcache, MultirangeType *mr1,
									  MultirangeType *mr2)
{
	int32		range_count1;
	int32		range_count2;
	RangeType **ranges1;
	RangeType **ranges2;

	if (MultirangeIsEmpty(mr1) || MultirangeIsEmpty(mr2))
		return false;

	multirange_deserialize(mr1, &range_count1, &ranges1);
	multirange_deserialize(mr2, &range_count2, &ranges2);

	return range_before_internal(typcache->rngtype, ranges1[range_count1 - 1],
								 ranges2[0]);
}

/* strictly right of? (internal version) */
bool
range_after_multirange_internal(TypeCacheEntry *typcache, RangeType *r,
								MultirangeType *mr)
{
	int32		range_count;
	RangeType **ranges;

	if (RangeIsEmpty(r) || MultirangeIsEmpty(mr))
		return false;

	multirange_deserialize(mr, &range_count, &ranges);

	return range_after_internal(typcache->rngtype, r, ranges[range_count - 1]);
}

/* adjacent to? */
Datum
range_adjacent_multirange(PG_FUNCTION_ARGS)
{
	RangeType  *r = PG_GETARG_RANGE_P(0);
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(1);
	TypeCacheEntry *typcache;
	int32		range_count;
	RangeType **ranges;

	if (RangeIsEmpty(r) || MultirangeIsEmpty(mr))
		return false;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	multirange_deserialize(mr, &range_count, &ranges);

	PG_RETURN_BOOL(range_adjacent_internal(typcache->rngtype, r, ranges[0]));
}

Datum
multirange_adjacent_range(PG_FUNCTION_ARGS)
{
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);
	RangeType  *r = PG_GETARG_RANGE_P(1);
	TypeCacheEntry *typcache;
	int32		range_count;
	RangeType **ranges;

	if (RangeIsEmpty(r) || MultirangeIsEmpty(mr))
		return false;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	multirange_deserialize(mr, &range_count, &ranges);

	PG_RETURN_BOOL(range_adjacent_internal(typcache->rngtype, ranges[range_count - 1], r));
}

Datum
multirange_adjacent_multirange(PG_FUNCTION_ARGS)
{
	MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
	MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);
	TypeCacheEntry *typcache;
	int32		range_count1;
	int32		range_count2;
	RangeType **ranges1;
	RangeType **ranges2;

	if (MultirangeIsEmpty(mr1) || MultirangeIsEmpty(mr2))
		return false;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

	multirange_deserialize(mr1, &range_count1, &ranges1);
	multirange_deserialize(mr2, &range_count2, &ranges2);

	PG_RETURN_BOOL(range_adjacent_internal(typcache->rngtype, ranges1[range_count1 - 1], ranges2[0]));
}

/* Btree support */

/* btree comparator */
Datum
multirange_cmp(PG_FUNCTION_ARGS)
{
	MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
	MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);
	int32		range_count_1;
	int32		range_count_2;
	int32		range_count_max;
	int32		i;
	RangeType **ranges1;
	RangeType **ranges2;
	RangeType  *r1;
	RangeType  *r2;
	TypeCacheEntry *typcache;
	int			cmp = 0;		/* If both are empty we'll use this. */

	/* Different types should be prevented by ANYMULTIRANGE matching rules */
	if (MultirangeTypeGetOid(mr1) != MultirangeTypeGetOid(mr2))
		elog(ERROR, "multirange types do not match");

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

	multirange_deserialize(mr1, &range_count_1, &ranges1);
	multirange_deserialize(mr2, &range_count_2, &ranges2);

	/* Loop over source data */
	range_count_max = Max(range_count_1, range_count_2);
	for (i = 0; i < range_count_max; i++)
	{
		/*
		 * If one multirange is shorter, it's as if it had empty ranges at the
		 * end to extend its length. An empty range compares earlier than any
		 * other range, so the shorter multirange comes before the longer.
		 * This is the same behavior as in other types, e.g. in strings 'aaa'
		 * < 'aaaaaa'.
		 */
		if (i >= range_count_1)
		{
			cmp = -1;
			break;
		}
		if (i >= range_count_2)
		{
			cmp = 1;
			break;
		}
		r1 = ranges1[i];
		r2 = ranges2[i];

		cmp = range_cmp_internal(typcache->rngtype, r1, r2);
		if (cmp != 0)
			break;
	}

	PG_FREE_IF_COPY(mr1, 0);
	PG_FREE_IF_COPY(mr2, 1);

	PG_RETURN_INT32(cmp);
}

/* inequality operators using the multirange_cmp function */
Datum
multirange_lt(PG_FUNCTION_ARGS)
{
	int			cmp = multirange_cmp(fcinfo);

	PG_RETURN_BOOL(cmp < 0);
}

Datum
multirange_le(PG_FUNCTION_ARGS)
{
	int			cmp = multirange_cmp(fcinfo);

	PG_RETURN_BOOL(cmp <= 0);
}

Datum
multirange_ge(PG_FUNCTION_ARGS)
{
	int			cmp = multirange_cmp(fcinfo);

	PG_RETURN_BOOL(cmp >= 0);
}

Datum
multirange_gt(PG_FUNCTION_ARGS)
{
	int			cmp = multirange_cmp(fcinfo);

	PG_RETURN_BOOL(cmp > 0);
}

/* multirange -> range functions */

/* Find the smallest range that includes everything in the multirange */
Datum
range_merge_from_multirange(PG_FUNCTION_ARGS)
{
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);
	Oid			mltrngtypoid = MultirangeTypeGetOid(mr);
	TypeCacheEntry *typcache;
	int32		range_count;
	RangeType **ranges;

	typcache = multirange_get_typcache(fcinfo, mltrngtypoid);

	if (MultirangeIsEmpty(mr))
		PG_RETURN_RANGE_P(make_empty_range(typcache->rngtype));

	multirange_deserialize(mr, &range_count, &ranges);

	PG_RETURN_RANGE_P(range_union_internal(typcache->rngtype, ranges[0],
										   ranges[range_count - 1], false));
}

/* Hash support */

/* hash a multirange value */
Datum
hash_multirange(PG_FUNCTION_ARGS)
{
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);
	uint32		result = 1;
	TypeCacheEntry *typcache;
	int32		range_count;
	RangeType **ranges;
	int32		i;
	RangeType  *r;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	multirange_deserialize(mr, &range_count, &ranges);
	for (i = 0; i < range_count; i++)
	{
		uint32		elthash;

		r = ranges[i];
		elthash = hash_range_internal(typcache->rngtype, r);

		/*
		 * Use the same approach as hash_array to combine the individual
		 * elements' hash values:
		 */
		result = (result << 5) - result + elthash;
	}

	PG_FREE_IF_COPY(mr, 0);

	PG_RETURN_UINT32(result);
}

/*
 * Returns 64-bit value by hashing a value to a 64-bit value, with a seed.
 * Otherwise, similar to hash_multirange.
 */
Datum
hash_multirange_extended(PG_FUNCTION_ARGS)
{
	MultirangeType *mr = PG_GETARG_MULTIRANGE_P(0);
	Datum		seed = PG_GETARG_DATUM(1);
	uint64		result = 1;
	TypeCacheEntry *typcache;
	int32		range_count;
	RangeType **ranges;
	int32		i;
	RangeType  *r;

	typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

	multirange_deserialize(mr, &range_count, &ranges);
	for (i = 0; i < range_count; i++)
	{
		uint64		elthash;

		r = ranges[i];
		elthash = hash_range_extended_internal(typcache->rngtype, r, seed);

		/*
		 * Use the same approach as hash_array to combine the individual
		 * elements' hash values:
		 */
		result = (result << 5) - result + elthash;
	}

	PG_FREE_IF_COPY(mr, 0);

	PG_RETURN_UINT64(result);
}
