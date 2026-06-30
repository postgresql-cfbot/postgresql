/*
 * File: vect_staple_templ.h
 */

#include "c.h"

/******************************************************************************
 * NONREUSABLE CODE, allowed to be included only once
 *
 * This code should appear only once in a file (or in a chain of files)
 * including this header with #include. So, we protect it from duplicates by
 * means of macro
 *
 *****************************************************************************/
#ifndef _VECT_STAPLE_TEMPL_H_UNIQUE_CODE_
#define _VECT_STAPLE_TEMPL_H_UNIQUE_CODE_

/*
 * SEARCH in Vector of Unique Sorted members
 */
typedef enum
{
	USV_SRCH_ERROR = -1,
	USV_SRCH_FOUND = 0,
	USV_SRCH_EMPTY,
	USV_SRCH_NOT_FOUND,
	USV_SRCH_NOT_FOUND_SMALLEST,
	USV_SRCH_NOT_FOUND_LARGEST
} usv_srch_stat_t;

typedef struct
{
	usv_srch_stat_t st;
	size_t pos; /* position (index) of a member that is equal to searched value
				 * or that is nearest greater member */
} usv_srch_res_t;

/*
 * INSERT  in Vector of Unique Sorted members
 */
typedef enum
{
	USV_INS_ERROR = -1,
	USV_INS_EXISTS = 0,
	USV_INS_NEW
} usv_ins_stat_t;

typedef struct
{
	usv_ins_stat_t st;
	size_t pos; /* position (index) of a member that was inserted or that proved
				 * to be equal to inserted value
				 */
} usv_ins_res_t;

#endif /* _VECT_STAPLE_TEMPL_H_UNIQUE_CODE_ */

/******************************************************************************
 * End of Code, allowed to be included only once
 *****************************************************************************/

/******************************************************************************
 * REUSABLE CODE
 *
 * This code can be reused with #include directive in a file if VECT_MARKER
 * redefined. This allow creation of several types of vectors with different
 * types of members.
 *
 *****************************************************************************/

#ifndef VECT_ITEM_TYPE
#error "VECT_ITEM_TYPE macro is indefined."
#endif

#ifndef VECT_ITEM_FORMAT_SPECIFIER
#error "VECT_ITEM_FORMAT_SPECIFIER macro is indefined."
#endif

#ifndef VECT_MARKER
#error "VECT_MARKER macro is indefined."
#endif

#ifndef VECT_MEMALLOCSTEP
#error "VECT_MEMALLOCSTEP macro is indefined."
#endif

#ifndef VECT_MALLOC
#error "VECT_MALLOC macro is indefined."
#endif

#ifndef VECT_FREE
#error "VECT_FREE macro is indefined."
#endif

/*
 * The Vector type itself,
 * The Vector of Unique Sorted Items type
 * and the Item type
 *
 * In fact, vectors's names looks like vect_u16_t where:
 *     vect_ - common prefix,
 *     u16 - marker,
 *     _t - suffix
 */
#define vect_t		   CppConcatTriple2(vect_, VECT_MARKER, _t)
#define uniqsortvect_t CppConcatTriple2(uniqsortvect_, VECT_MARKER, _t)
#define item_t		   VECT_ITEM_TYPE

typedef struct
{
	size_t cnt;		   /* number of items */
	size_t cap;		   /* capacity */
	bool mem_is_outer; /* flag about an external memory is used */
	item_t *m;		   /* items (members) */
} vect_t;

typedef vect_t uniqsortvect_t;

#define vect_init		   CppConcatTriple2(vect_, VECT_MARKER, _init)
#define vect_fill		   CppConcatTriple2(vect_, VECT_MARKER, _fill)
#define vect_reserve	   CppConcatTriple2(vect_, VECT_MARKER, _reserve)
#define vect_append		   CppConcatTriple2(vect_, VECT_MARKER, _append)
#define vect_print		   CppConcatTriple2(vect_, VECT_MARKER, _print)
#define vect_compare	   CppConcatTriple2(vect_, VECT_MARKER, _compare)
#define vect_insert		   CppConcatTriple2(vect_, VECT_MARKER, _insert)
#define vect_clear		   CppConcatTriple2(vect_, VECT_MARKER, _clear)

#define usv_insert CppConcatTriple2(usv_, VECT_MARKER, _insert)
#define usv_search CppConcatTriple2(usv_, VECT_MARKER, _search)

/******************************************************************************
 * End of Reusable Code
 *****************************************************************************/

/*
 * Don't forget to include
 * 		#include "vect_templ_undef.h"
 * in your file that uses this header
 *
 */
