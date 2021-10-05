/*-------------------------------------------------------------------------
 *
 * pg_sse42_utils.h
 *	  Convenience functions to wrap SSE 4.2 intrinsics.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/pg_sse42_utils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_SSE42_UTILS
#define PG_SSE42_UTILS

#include <nmmintrin.h>


/* assign the arguments to the lanes in the register */
#define vset(...)       _mm_setr_epi8(__VA_ARGS__)

/* return a zeroed register */
static inline const __m128i
vzero()
{
	return _mm_setzero_si128();
}

/* perform an unaligned load from memory into a register */
static inline const __m128i
vload(const unsigned char *raw_input)
{
	return _mm_loadu_si128((const __m128i *) raw_input);
}

/* return a vector with each 8-bit lane populated with the input scalar */
static inline __m128i
splat(char byte)
{
	return _mm_set1_epi8(byte);
}

/* return false if a register is zero, true otherwise */
static inline bool
to_bool(const __m128i v)
{
	/*
	 * _mm_testz_si128 returns 1 if the bitwise AND of the two arguments is
	 * zero. Zero is the only value whose bitwise AND with itself is zero.
	 */
	return !_mm_testz_si128(v, v);
}

/* vector version of IS_HIGHBIT_SET() */
static inline bool
is_highbit_set(const __m128i v)
{
	return _mm_movemask_epi8(v) != 0;
}

/* bitwise vector operations */

static inline __m128i
bitwise_and(const __m128i v1, const __m128i v2)
{
	return _mm_and_si128(v1, v2);
}

static inline __m128i
bitwise_or(const __m128i v1, const __m128i v2)
{
	return _mm_or_si128(v1, v2);
}

static inline __m128i
bitwise_xor(const __m128i v1, const __m128i v2)
{
	return _mm_xor_si128(v1, v2);
}

/* perform signed greater-than on all 8-bit lanes */
static inline __m128i
greater_than(const __m128i v1, const __m128i v2)
{
	return _mm_cmpgt_epi8(v1, v2);
}

/* set bits in the error vector where bytes in the input are zero */
static inline void
check_for_zeros(const __m128i v, __m128i * error)
{
	const		__m128i cmp = _mm_cmpeq_epi8(v, vzero());

	*error = bitwise_or(*error, cmp);
}

/*
 * Do unsigned subtraction, but instead of wrapping around
 * on overflow, stop at zero. Useful for emulating unsigned
 * comparison.
 */
static inline __m128i
saturating_sub(const __m128i v1, const __m128i v2)
{
	return _mm_subs_epu8(v1, v2);
}

/*
 * Shift right each 8-bit lane
 *
 * There is no intrinsic to do this on 8-bit lanes, so shift
 * right in each 16-bit lane then apply a mask in each 8-bit
 * lane shifted the same amount.
 */
static inline __m128i
shift_right(const __m128i v, const int n)
{
	const		__m128i shift16 = _mm_srli_epi16(v, n);
	const		__m128i mask = splat(0xFF >> n);

	return bitwise_and(shift16, mask);
}

/*
 * Shift entire 'input' register right by N 8-bit lanes, and
 * replace the first N lanes with the last N lanes from the
 * 'prev' register. Could be stated in C thusly:
 *
 * ((prev << 128) | input) >> (N * 8)
 *
 * The third argument to the intrinsic must be a numeric constant, so
 * we must have separate functions for different shift amounts.
 */
static inline __m128i
prev1(__m128i prev, __m128i input)
{
	return _mm_alignr_epi8(input, prev, sizeof(__m128i) - 1);
}

static inline __m128i
prev2(__m128i prev, __m128i input)
{
	return _mm_alignr_epi8(input, prev, sizeof(__m128i) - 2);
}

static inline __m128i
prev3(__m128i prev, __m128i input)
{
	return _mm_alignr_epi8(input, prev, sizeof(__m128i) - 3);
}

/*
 * For each 8-bit lane in the input, use that value as an index
 * into the lookup vector as if it were a 16-element byte array.
 */
static inline __m128i
lookup(const __m128i input, const __m128i lookup)
{
	return _mm_shuffle_epi8(lookup, input);
}

#endif							/* PG_SSE42_UTILS */
