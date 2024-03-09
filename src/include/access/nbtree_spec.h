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
 * Specialize key-accessing functions and the hot code around those. The file
 * that's marked for specialization is either
 * - NBTS_HEADER, a quoted header inclusion path; or
 * - NBTS_FILE, the include path of the to-be-specialized code relative to
 *		the access/nbtree_spec.h file.
 *
 * Specialized files define their functions like usual, with an additional
 * requirement that they #define their function name at the top, with
 * #define funcname NBTS_FUNCTION(funcname).
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
 *   Note that this is unneeded in paths that are inaccessible to unspecialized
 *   code paths (i.e. code included through nbtree_spec.h), because that
 *   always calls the optimized functions directly.
 */

#if defined(NBT_FILE)
#define NBT_SPECIALIZE_FILE NBT_FILE
#else
#error "No specializable file defined"
#endif

/* how do we make names? */
#define NBTS_MAKE_PREFIX(a) CppConcat(a,_)
#define NBTS_MAKE_NAME_(a,b) CppConcat(a,b)
#define NBTS_MAKE_NAME(a,b) NBTS_MAKE_NAME_(NBTS_MAKE_PREFIX(a),b)


/*
 * Protections against multiple inclusions - the definition of this macro is
 * different for files included by this file as template, versus those that
 * include this file for the definitions, so we redefine these macros at top and
 * bottom.
 */
#ifdef NBTS_FUNCTION
#undef NBTS_FUNCTION
#endif
#define NBTS_FUNCTION(name) NBTS_MAKE_NAME(name, NBTS_TYPE)

/* In a specialized file, specialization contexts are meaningless */
#ifdef nbts_prep_ctx
#undef nbts_prep_ctx
#endif

/*
 * Specialization 1: CACHED
 *
 * Multiple key columns, optimized access for attcacheoff -cacheable offsets.
 */
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

#undef NBTS_TYPE
#undef nbts_attiterdeclare
#undef nbts_attiterinit
#undef nbts_foreachattr
#undef nbts_attiter_attnum
#undef nbts_attiter_nextattdatum
#undef nbts_attiter_curattisnull

/*
 * Specialization 2: SINGLE_KEYATT
 *
 * Optimized access for indexes with a single key column.
 *
 * Note that this path may never be used for indexes with multiple key
 * columns, because it only considers the first column (plus any TIDs for tie
 * breaking).
 */
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

/* un-define the optimization macros */
#undef nbts_attiterdeclare
#undef nbts_attiterinit
#undef nbts_foreachattr
#undef nbts_attiter_attnum
#undef nbts_attiter_nextattdatum
#undef nbts_attiter_curattisnull

/*
 * Specialization 3: UNCACHED
 *
 * Multiple key columns, but attcacheoff -optimization doesn't apply because
 * some intermediate or prefixing key attributes that don't have a fixed size
 * at the type level (i.e. their attlen < 0).
 */
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
#undef nbts_attiterdeclare
#undef nbts_attiterinit
#undef nbts_foreachattr
#undef nbts_attiter_attnum
#undef nbts_attiter_nextattdatum
#undef nbts_attiter_curattisnull

/*
 * We're done with templating, so restore or create the macros which can be
 * used in non-template sources.
 */
#define nbts_prep_ctx(rel) NBTS_MAKE_CTX(rel)

/*
 * from here on all NBTS_FUNCTIONs are from specialized function names that
 * are being called. Change the result of those macros from a direct call
 * call to a conditional call to the right place, depending on the correct
 * context.
 */
#undef NBTS_FUNCTION
#define NBTS_FUNCTION(name) NBTS_SPECIALIZE_NAME(name)

/* cleanup unwanted definitions from file inclusion */
#undef NBT_SPECIALIZE_FILE
#ifdef NBT_HEADERS
#undef NBT_HEADERS
#endif
#ifdef NBT_FILE
#undef NBT_FILE
#endif
