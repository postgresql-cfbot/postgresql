/*
 * File: dfor_staple_templ.h
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
#ifndef _DFOR_STAPLE_TEMPL_H_UNIQUE_CODE_
#define _DFOR_STAPLE_TEMPL_H_UNIQUE_CODE_

typedef struct {
	size_t item_cnt;
	size_t delta_wid;
	size_t exc_cnt;
	size_t exc_wid;
	size_t exc_pos_wid;
	size_t nbytes; /* size of pack in bytes */
	uint8_t *pack;
	bool outer_mem;
	bool signed_deltas;
} dfor_meta_t;

typedef struct {
	size_t nbits;  /* size of pack in bits used in fact */
	size_t delta_pack_nbits; /* in bits */
	size_t exc_pack_nbits; /* in bits */
	size_t exc_pos_pack_nbits; /* in bits */
	float ratio;  /* compression ratio */
} dfor_stats_t;

typedef enum {
	DFOR_EXC_DONT_USE = 0,
	DFOR_EXC_USE = 1
} excalg_t;

#endif /* _DFOR_STAPLE_TEMPL_H_UNIQUE_CODE_ */

/******************************************************************************
 * End of Code, allowed to be included only once
 *****************************************************************************/

/******************************************************************************
 * REUSABLE CODE
 *
 * This code can be reused with #include directive in a file if DFOR_MARKER is
 * redefined. This allows creation of several types of vectors with different
 * types of members.
 *
 *****************************************************************************/

#ifndef DFOR_ITEM_TYPE
#error "DFOR_ITEM_TYPE macro is indefined."
#endif

#ifndef DFOR_MARKER
#error "DFOR_MARKER macro is indefined."
#endif

#ifndef DFOR_MALLOC
#error "DFOR_MALLOC macro is indefined."
#endif

#ifndef DFOR_FREE
#error "DFOR_FREE macro is indefined."
#endif

#define MAKE_HEADER_NAME(v, m) CppAsString2(CppConcat2(v, m).h)

/*
 * Headers from vect and bitpack units
 *
 * Example: dfor_u16.c and dfor_u16.h need vect_u16.h and bitpack_u16.h
 */
#include MAKE_HEADER_NAME(lib/vect_, DFOR_MARKER)
#include MAKE_HEADER_NAME(lib/bitpack_, DFOR_MARKER)

/* Types */
#define item_t		   DFOR_ITEM_TYPE
#define vect_t		   CppConcatTriple2(vect_, DFOR_MARKER, _t)
#define uniqsortvect_t CppConcatTriple2(uniqsortvect_, DFOR_MARKER, _t)

/* Functions */
#define dfor_calc_deltas CppConcatTriple2(dfor_, DFOR_MARKER, _calc_deltas)
#define dfor_calc_width	 CppConcatTriple2(dfor_, DFOR_MARKER, _calc_width)
#define dfor_pack		 CppConcatTriple2(dfor_, DFOR_MARKER, _pack)
#define dfor_unpack		 CppConcatTriple2(dfor_, DFOR_MARKER, _unpack)
#define dfor_analyze	 CppConcatTriple2(dfor_, DFOR_MARKER, _analyze)
#define dfor_clear_meta	 CppConcatTriple2(dfor_, DFOR_MARKER, _clear_meta)
#define dfor_calc_stats	 CppConcatTriple2(dfor_, DFOR_MARKER, _calc_stats)
#define dfor_calc_nbytes CppConcatTriple2(dfor_, DFOR_MARKER, _calc_nbytes)

/* Functions of the vect unit */
#define vect_init		   CppConcatTriple2(vect_, DFOR_MARKER, _init)
#define vect_fill		   CppConcatTriple2(vect_, DFOR_MARKER, _fill)
#define vect_reserve	   CppConcatTriple2(vect_, DFOR_MARKER, _reserve)
#define vect_append		   CppConcatTriple2(vect_, DFOR_MARKER, _append)
#define vect_print		   CppConcatTriple2(vect_, DFOR_MARKER, _print)
#define vect_compare	   CppConcatTriple2(vect_, DFOR_MARKER, _compare)
#define vect_insert		   CppConcatTriple2(vect_, DFOR_MARKER, _insert)
#define vect_clear		   CppConcatTriple2(vect_, DFOR_MARKER, _clear)

#define usv_insert		   CppConcatTriple2(usv_, DFOR_MARKER, _insert)
#define usv_search		   CppConcatTriple2(usv_, DFOR_MARKER, _search)

/* Functions of the bitpack unit */
#define width_from_val CppConcatTriple2(width_, DFOR_MARKER, _from_val)
#define width_to_mask  CppConcatTriple2(width_, DFOR_MARKER, _to_mask)
#define bitpack_pack   CppConcatTriple2(bitpack_, DFOR_MARKER, _pack)
#define bitpack_unpack CppConcatTriple2(bitpack_, DFOR_MARKER, _unpack)

/******************************************************************************
 * End of Reusable Code
 *****************************************************************************/

/*
 * Don't forget to include this code in your file that uses this header
 *     #include "dfor_templ_undef.h"
 *
 */
