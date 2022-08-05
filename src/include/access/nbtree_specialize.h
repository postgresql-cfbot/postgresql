/*-------------------------------------------------------------------------
 *
 * nbtree_specialize.h
 *	  header file for postgres btree access method implementation.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/nbtree_specialize.h
 *
 *-------------------------------------------------------------------------
 *
 * Specialize key-accessing functions and the hot code around those.
 *
 * Key attribute iteration is specialized through the use of the following
 * macros:
 *
 * - nbts_call(function, indexrel, ...rest_of_args), and
 *   nbts_call_norel(function, indexrel, ...args)
 *     This will call the specialized variant of 'function' based on the index
 *     relation data.
 *     The difference between nbts_call and nbts_call_norel is that _call
 *     uses indexrel as first argument in the function call, whereas
 *     nbts_call_norel does not.
 * - nbts_attiterdeclare(itup)
 *   Declare the variables required to iterate over the provided IndexTuple's
 *   key attributes. Many tuples may have their attributes iterated over at the
 *   same time.
 * - nbts_attiterinit(itup, initAttNum, tupDesc)
 *   Initialize the attribute iterator for the provided IndexTuple at
 *   the provided AttributeNumber.
 * - nbts_foreachattr(initAttNum, endAttNum)
 *   Start a loop over the attributes, starting at initAttNum and ending at
 *   endAttNum, inclusive. It also takes care of truncated attributes.
 * - nbts_attiter_attnum
 *   The current attribute number
 * - nbts_attiter_nextattdatum(itup, tupDesc)
 *   Updates the attribute iterator state to the next attribute. Returns the
 *   datum of the next attribute, which might be null (see below)
 * - nbts_attiter_curattisnull(itup)
 *   Returns whether the result from the last nbts_attiter_nextattdatum is
 *   null.
 *
 * example usage:
 *
 * kwithnulls = nbts_call_norel(_bt_key_hasnulls, myindex, mytuple, tupDesc);
 *
 * NBTS_FUNCTION(_bt_key_hasnulls)(IndexTuple mytuple, TupleDesc tupDesc)
 * {
 *    nbts_attiterdeclare(mytuple);
 *    nbts_attiterinit(mytuple, 1, tupDesc);
 *    nbts_foreachattr(1, 10)
 *    {
 *        Datum it = nbts_attiter_nextattdatum(tuple, tupDesc);
 *        if (nbts_attiter_curattisnull(tuple))
 *           return true;
 *    }
 *    return false
 * }
 */

/*
 * Call a potentially specialized function for a given btree operation.
 *
 * NB: the rel argument is evaluated multiple times.
 */
#define nbts_call(name, rel, ...) \
	nbts_call_norel(name, (rel), (rel), __VA_ARGS__)

#ifdef NBTS_ENABLED

#define NBTS_FUNCTION(name) NBTS_MAKE_NAME(name, NBTS_TYPE)

#ifdef nbts_call_norel
#undef nbts_call_norel
#endif

#define nbts_call_norel(name, rel, ...) \
	(NBTS_FUNCTION(name)(__VA_ARGS__))

/*
 * Optimized access for indexes with a single key column.
 *
 * Note that this path may never be used for indexes with multiple key
 *  columns, because it does not ever continue to a next column.
 */

#define NBTS_SPECIALIZING_SINGLE_COLUMN
#define NBTS_TYPE NBTS_TYPE_SINGLE_COLUMN

#define nbts_attiterdeclare(itup) \
	bool	NBTS_MAKE_NAME(itup, isNull)

#define nbts_attiterinit(itup, initAttNum, tupDesc)

/*
 * We void endAttNum to prevent unused variable warnings.
 * The if- and for-loop are structured like this to make the compiler
 * unroll the loop and detect only one single iteration. We need `break`
 * in the following code block, so just a plain 'if' statement would
 * not work.
 */
#define nbts_foreachattr(initAttNum, endAttNum) \
	Assert((endAttNum) == 1); ((void) (endAttNum)); \
	if ((initAttNum) == 1) for (int spec_i = 0; spec_i < 1; spec_i++)

#define nbts_attiter_attnum 1

/*
 * Simplified (optimized) variant of index_getattr specialized for extracting
 * only the first attribute: cache offset is guaranteed to be 0, and as such
 * no cache is required.
 */
#define nbts_attiter_nextattdatum(itup, tupDesc) \
( \
	AssertMacro(spec_i == 0), \
	( \
		IndexTupleHasNulls(itup) && \
		att_isnull(0, (bits8 *) ((char *) (itup) + sizeof(IndexTupleData))) \
	) \
	? \
	( \
		(NBTS_MAKE_NAME(itup, isNull)) = true, \
		(Datum)NULL \
	) \
	: \
	( \
		(NBTS_MAKE_NAME(itup, isNull) = false), \
		(Datum) fetchatt(TupleDescAttr((tupDesc), 0), \
		(char *) (itup) + IndexInfoFindDataOffset((itup)->t_info)) \
	) \
)

#define nbts_attiter_curattisnull(tuple) \
	NBTS_MAKE_NAME(tuple, isNull)

#include NBT_SPECIALIZE_FILE

#undef NBTS_TYPE
#undef NBTS_SPECIALIZING_SINGLE_COLUMN
#undef nbts_attiterdeclare
#undef nbts_attiterinit
#undef nbts_foreachattr
#undef nbts_attiter_attnum
#undef nbts_attiter_nextattdatum
#undef nbts_attiter_curattisnull

/*
 * Multiple key columns, optimized access for attcacheoff -cacheable offsets.
 */
#define NBTS_SPECIALIZING_CACHED
#define NBTS_TYPE NBTS_TYPE_CACHED

#define nbts_attiterdeclare(itup) \
	bool	NBTS_MAKE_NAME(itup, isNull)

#define nbts_attiterinit(itup, initAttNum, tupDesc)

#define nbts_foreachattr(initAttNum, endAttNum) \
	for (int spec_i = (initAttNum); spec_i <= (endAttNum); spec_i++)

#define nbts_attiter_attnum spec_i

#define nbts_attiter_nextattdatum(itup, tupDesc) \
	index_getattr((itup), spec_i, (tupDesc), &(NBTS_MAKE_NAME(itup, isNull)))

#define nbts_attiter_curattisnull(itup) \
	NBTS_MAKE_NAME(itup, isNull)

#include NBT_SPECIALIZE_FILE

#undef NBTS_TYPE
#undef NBTS_SPECIALIZING_CACHED
#undef nbts_attiterdeclare
#undef nbts_attiterinit
#undef nbts_foreachattr
#undef nbts_attiter_attnum
#undef nbts_attiter_nextattdatum
#undef nbts_attiter_curattisnull

/*
 * Multiple key columns, but attcacheoff -optimization doesn't apply.
 */
#define NBTS_SPECIALIZING_UNCACHED
#define NBTS_TYPE NBTS_TYPE_UNCACHED

#define nbts_attiterdeclare(itup) \
	IAttrIterStateData	NBTS_MAKE_NAME(itup, iter)

#define nbts_attiterinit(itup, initAttNum, tupDesc) \
	index_attiterinit((itup), (initAttNum), (tupDesc), &(NBTS_MAKE_NAME(itup, iter)))

#define nbts_foreachattr(initAttNum, endAttNum) \
	for (int spec_i = (initAttNum); spec_i <= (endAttNum); spec_i++)

#define nbts_attiter_attnum spec_i

#define nbts_attiter_nextattdatum(itup, tupDesc) \
	index_attiternext((itup), spec_i, (tupDesc), &(NBTS_MAKE_NAME(itup, iter)))

#define nbts_attiter_curattisnull(itup) \
	NBTS_MAKE_NAME(itup, iter).isNull

#include NBT_SPECIALIZE_FILE

#undef NBTS_TYPE
#undef NBTS_SPECIALIZING_UNCACHED
#undef nbts_attiterdeclare
#undef nbts_attiterinit
#undef nbts_foreachattr
#undef nbts_attiter_attnum
#undef nbts_attiter_nextattdatum
#undef nbts_attiter_curattisnull

/* reset call to SPECIALIZE_CALL for default behaviour */
#undef nbts_call_norel
#define nbts_call_norel(name, rel, ...) \
	NBT_SPECIALIZE_CALL(name, (rel), __VA_ARGS__)

/*
 * "Default", externally accessible, not so much optimized functions
 */

#define NBTS_SPECIALIZING_DEFAULT
#define NBTS_TYPE NBTS_TYPE_DEFAULT

/* for the default functions, we want to use the unspecialized name. */
#undef NBTS_FUNCTION
#define NBTS_FUNCTION(name) name


#define nbts_attiterdeclare(itup) \
	bool	NBTS_MAKE_NAME(itup, isNull)

#define nbts_attiterinit(itup, initAttNum, tupDesc)

#define nbts_foreachattr(initAttNum, endAttNum) \
	for (int spec_i = (initAttNum); spec_i <= (endAttNum); spec_i++)

#define nbts_attiter_attnum spec_i

#define nbts_attiter_nextattdatum(itup, tupDesc) \
	index_getattr((itup), spec_i, (tupDesc), &(NBTS_MAKE_NAME(itup, isNull)))

#define nbts_attiter_curattisnull(itup) \
	NBTS_MAKE_NAME(itup, isNull)

#include NBT_SPECIALIZE_FILE

#undef NBTS_TYPE
#undef NBTS_SPECIALIZING_DEFAULT
#undef nbts_attiterdeclare
#undef nbts_attiterinit
#undef nbts_foreachattr
#undef nbts_attiter_attnum
#undef nbts_attiter_nextattdatum
#undef nbts_attiter_curattisnull

/* from here on there are no more NBTS_FUNCTIONs */
#undef NBTS_FUNCTION

#else /* not defined NBTS_ENABLED */

/*
 * NBTS_ENABLE is not defined, so we don't want to use the specializations.
 * We revert to the behaviour from PG14 and earlier, which only uses
 * attcacheoff.
 */

#define NBTS_FUNCTION(name) name

#define nbts_call_norel(name, rel, ...) \
	name(__VA_ARGS__)

#define NBTS_TYPE NBTS_TYPE_CACHED

#define nbts_attiterdeclare(itup) \
	bool	NBTS_MAKE_NAME(itup, isNull)

#define nbts_attiterinit(itup, initAttNum, tupDesc)

#define nbts_foreachattr(initAttNum, endAttNum) \
	for (int spec_i = (initAttNum); spec_i <= (endAttNum); spec_i++)

#define nbts_attiter_attnum spec_i

#define nbts_attiter_nextattdatum(itup, tupDesc) \
	index_getattr((itup), spec_i, (tupDesc), &(NBTS_MAKE_NAME(itup, isNull)))

#define nbts_attiter_curattisnull(itup) \
	NBTS_MAKE_NAME(itup, isNull)

#include NBT_SPECIALIZE_FILE

#undef NBTS_TYPE
#undef nbts_attiterdeclare
#undef nbts_attiterinit
#undef nbts_foreachattr
#undef nbts_attiter_attnum
#undef nbts_attiter_nextattdatum
#undef nbts_attiter_curattisnull


#endif /* !NBTS_ENABLED */
