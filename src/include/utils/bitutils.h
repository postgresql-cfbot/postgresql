/*------------------------------------------------------------------------ -
 *
 * bitutils.h
 *	  miscellaneous functions bit-wise operations.
  *
 *
 * Portions Copyright(c) 2019, PostgreSQL Global Development Group
 *
 * src/include/utils/bitutils.h
 *
 *------------------------------------------------------------------------ -
 */

#ifndef BITUTILS_H
#define BITUTILS_H

extern int pg_popcount32_slow(uint32 word);
#ifdef HAVE__BUILTIN_POPCOUNT
extern int pg_popcount32_sse42(uint32 word);
#endif
extern int pg_popcount64_slow(uint64 word);
#ifdef HAVE__BUILTIN_POPCOUNTL
extern int pg_popcount64_sse42(uint64 word);
#endif

extern int pg_rightmost_one32_slow(uint32 word);
#ifdef HAVE__BUILTIN_CTZ
extern int pg_rightmost_one32_abm(uint32 word);
#endif
extern int pg_rightmost_one64_slow(uint64 word);
#ifdef HAVE__BUILTIN_CTZL
extern int pg_rightmost_one64_abm(uint64 word);
#endif

extern int pg_leftmost_one32_slow(uint32 word);
#ifdef HAVE__BUILTIN_CLZ
extern int pg_leftmost_one32_abm(uint32 word);
#endif
extern int pg_leftmost_one64_slow(uint64 word);
#ifdef HAVE__BUILTIN_CLZL
extern int pg_leftmost_one64_abm(uint64 word);
#endif


extern int (*pg_popcount32) (uint32 word);
extern int (*pg_popcount64) (uint64 word);
extern int (*pg_rightmost_one32) (uint32 word);
extern int (*pg_rightmost_one64) (uint64 word);
extern int (*pg_leftmost_one32) (uint32 word);
extern int (*pg_leftmost_one64) (uint64 word);

#endif							/* BITUTILS_H */
