/*
 * File: bitpack_templ_staple.h
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
#ifndef _BITPACK_TEMPL_STAPLE_H_UNIQUE_CODE_
#define _BITPACK_TEMPL_STAPLE_H_UNIQUE_CODE_

/* No code here yet */

#endif /* _BITPACK_TEMPL_STAPLE_H_UNIQUE_CODE_ */

/******************************************************************************
 * End of Code, allowed to be included only once
 *****************************************************************************/

/******************************************************************************
 * REUSABLE CODE
 *
 * This code can be reused with #include directive in a file if BITPACK_MARKER
 * redefined. This allow creation of several types of vectors with different
 * types of members.
 *
 *****************************************************************************/

#ifndef BITPACK_ITEM_TYPE
#error "BITPACK_ITEM_TYPE macro is indefined."
#endif
#ifndef BITPACK_MARKER
#error "BITPACK_MARKER macro is indefined."
#endif

#define item_t		   BITPACK_ITEM_TYPE
#define width_from_val CppConcatTriple2(width_, BITPACK_MARKER, _from_val)
#define width_to_mask  CppConcatTriple2(width_, BITPACK_MARKER, _to_mask)
#define bitpack_pack   CppConcatTriple2(bitpack_, BITPACK_MARKER, _pack)
#define bitpack_unpack CppConcatTriple2(bitpack_, BITPACK_MARKER, _unpack)

/******************************************************************************
 * End of Reusable Code
 *****************************************************************************/

/*
 * Don't forget to include this code in your file that uses this header
 *
 * #include "lib/bitpack_templ_undef.h"
 *
 */
