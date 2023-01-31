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
 * - nbts_context(irel)
 *   Constructs a context that is used to call specialized functions.
 *   Note that this is optional in paths that are inaccessible to unspecialized
 *   code paths, but should be included in NBTS_BUILD_GENERIC.
 */

/*
 * Macros used in the nbtree specialization code.
 */
#define NBTS_TYPE_SINGLE_KEYATT single_keyatt
#define NBTS_TYPE_UNCACHED uncached
#define NBTS_TYPE_CACHED cached
#define NBTS_TYPE_DEFAULT default
#define NBTS_CTX_NAME __nbts_ctx

/* contextual specializations */
#define NBTS_MAKE_CTX(rel) const NBTS_CTX NBTS_CTX_NAME = _nbt_spec_context(rel)
#define NBTS_SPECIALIZE_NAME(name) ( \
	(NBTS_CTX_NAME) == NBTS_CTX_SINGLE_KEYATT ? (NBTS_MAKE_NAME(name, NBTS_TYPE_SINGLE_KEYATT)) : ( \
		(NBTS_CTX_NAME) == NBTS_CTX_UNCACHED ? (NBTS_MAKE_NAME(name, NBTS_TYPE_UNCACHED)) : ( \
			(NBTS_CTX_NAME) == NBTS_CTX_CACHED ? (NBTS_MAKE_NAME(name, NBTS_TYPE_CACHED)) : ( \
				NBTS_MAKE_NAME(name, NBTS_TYPE_DEFAULT) \
			) \
		) \
	) \
)

/* how do we make names? */
#define NBTS_MAKE_PREFIX(a) CppConcat(a,_)
#define NBTS_MAKE_NAME_(a,b) CppConcat(a,b)
#define NBTS_MAKE_NAME(a,b) NBTS_MAKE_NAME_(NBTS_MAKE_PREFIX(a),b)

#define nbt_opt_specialize(rel) \
do { \
	Assert(PointerIsValid(rel)); \
	if (unlikely((rel)->rd_indam->aminsert == btinsert_default)) \
	{ \
		Assert(PointerIsValid(rel)); \
		PopulateTupleDescCacheOffsets(rel->rd_att); \
		{ \
			nbts_prep_ctx(rel); \
			_bt_specialize(rel); \
		} \
	} \
} while (false)

/*
 * Protections against multiple inclusions - the definition of this macro is
 * different for files included with the templating mechanism vs the users
 * of this template, so redefine these macros at top and bottom.
 */
#ifdef NBTS_FUNCTION
#undef NBTS_FUNCTION
#endif
#define NBTS_FUNCTION(name) NBTS_MAKE_NAME(name, NBTS_TYPE)

/* While specializing, the context is the local context */
#ifdef nbts_prep_ctx
#undef nbts_prep_ctx
#endif
#define nbts_prep_ctx(rel)

/*
 * Specialization 1: CACHED
 *
 * Multiple key columns, optimized access for attcacheoff -cacheable offsets.
 */
#define NBTS_SPECIALIZING_CACHED
#define NBTS_TYPE NBTS_TYPE_CACHED

#define nbts_attiterdeclare(itup) \
	bool	NBTS_MAKE_NAME(itup, isNull)

#define nbts_attiterinit(itup, initAttNum, tupDesc) do {} while (false)

#define nbts_foreachattr(initAttNum, endAttNum) \
	for (int spec_i = (initAttNum); spec_i <= (endAttNum); spec_i++)

#define nbts_attiter_attnum spec_i

#define nbts_attiter_nextattdatum(itup, tupDesc) \
	index_getattr((itup), spec_i, (tupDesc), &(NBTS_MAKE_NAME(itup, isNull)))

#define nbts_attiter_curattisnull(itup) \
	NBTS_MAKE_NAME(itup, isNull)

#include NBT_SPECIALIZE_FILE

#undef NBTS_SPECIALIZING_CACHED
#undef NBTS_TYPE
#undef nbts_attiterdeclare
#undef nbts_attiterinit
#undef nbts_foreachattr
#undef nbts_attiter_attnum
#undef nbts_attiter_nextattdatum
#undef nbts_attiter_curattisnull

/*
 * Specialization 2: DEFAULT
 *
 * "Default", externally accessible, not so much optimized functions
 */

/* the default context needs to specialize, so here's that */
#undef nbts_prep_ctx
#define nbts_prep_ctx(rel) NBTS_MAKE_CTX(rel)

#define NBTS_SPECIALIZING_DEFAULT
#define NBTS_TYPE NBTS_TYPE_DEFAULT

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

/* un-define the optimization macros */
#undef nbts_attiterdeclare
#undef nbts_attiterinit
#undef nbts_foreachattr
#undef nbts_attiter_attnum
#undef nbts_attiter_nextattdatum
#undef nbts_attiter_curattisnull

/*
 * Specialization 3: SINGLE_KEYATT
 *
 * Optimized access for indexes with a single key column.
 *
 * Note that this path cannot be used for indexes with multiple key
+ *  columns, because it never considers the next column.
 */

/* the default context (and later contexts) do need to specialize, so here's that */
#undef nbts_prep_ctx
#define nbts_prep_ctx(rel)

#define NBTS_SPECIALIZING_SINGLE_KEYATT
#define NBTS_TYPE NBTS_TYPE_SINGLE_KEYATT

#define nbts_attiterdeclare(itup) \
	bool	NBTS_MAKE_NAME(itup, isNull)

#define nbts_attiterinit(itup, initAttNum, tupDesc)

#define nbts_foreachattr(initAttNum, endAttNum) \
	Assert((endAttNum) == 1); ((void) (endAttNum)); \
	if ((initAttNum) == 1) for (int spec_i = 0; spec_i < 1; spec_i++)

#define nbts_attiter_attnum 1

#define nbts_attiter_nextattdatum(itup, tupDesc) \
( \
	AssertMacro(spec_i == 0), \
	_bt_getfirstatt(itup, tupDesc, &NBTS_MAKE_NAME(itup, isNull)) \
)

#define nbts_attiter_curattisnull(itup) \
	NBTS_MAKE_NAME(itup, isNull)

#include NBT_SPECIALIZE_FILE

#undef NBTS_TYPE
#undef NBTS_SPECIALIZING_SINGLE_KEYATT

/* un-define the optimization macros */
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

/*
 * All subsequent contexts are from non-templated code, so
 * they need to actually include the context.
 */
#undef nbts_prep_ctx
#define nbts_prep_ctx(rel) NBTS_MAKE_CTX(rel)
/*
 * from here on all NBTS_FUNCTIONs are from specialized function names that
 * are being called. Change the result of those macros from a direct call
 * call to a conditional call to the right place, depending on the correct
 * context.
 */
#undef NBTS_FUNCTION
#define NBTS_FUNCTION(name) NBTS_SPECIALIZE_NAME(name)

#undef NBT_SPECIALIZE_FILE
