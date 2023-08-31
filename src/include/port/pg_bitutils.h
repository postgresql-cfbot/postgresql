/*-------------------------------------------------------------------------
 *
 * pg_bitutils.h
 *	  Miscellaneous functions for bit-wise operations.
 *
 *
 * Copyright (c) 2019-2023, PostgreSQL Global Development Group
 *
 * src/include/port/pg_bitutils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_BITUTILS_H
#define PG_BITUTILS_H

#ifdef _MSC_VER
#include <intrin.h>
#define HAVE_BITSCAN_FORWARD
#define HAVE_BITSCAN_REVERSE

#else
#if defined(HAVE__BUILTIN_CTZ)
#define HAVE_BITSCAN_FORWARD
#endif

#if defined(HAVE__BUILTIN_CLZ)
#define HAVE_BITSCAN_REVERSE
#endif
#endif							/* _MSC_VER */

extern PGDLLIMPORT const uint8 pg_leftmost_one_pos[256];
extern PGDLLIMPORT const uint8 pg_rightmost_one_pos[256];
extern PGDLLIMPORT const uint8 pg_number_of_ones[256];

/*
 * pg_leftmost_one_pos32
 *		Returns the position of the most significant set bit in "word",
 *		measured from the least significant bit.  word must not be 0.
 */
static inline int
pg_leftmost_one_pos32(uint32 word)
{
#ifdef HAVE__BUILTIN_CLZ
	Assert(word != 0);

	return 31 - __builtin_clz(word);
#elif defined(_MSC_VER)
	unsigned long result;
	bool		non_zero;

	Assert(word != 0);

	non_zero = _BitScanReverse(&result, word);
	return (int) result;
#else
	int			shift = 32 - 8;

	Assert(word != 0);

	while ((word >> shift) == 0)
		shift -= 8;

	return shift + pg_leftmost_one_pos[(word >> shift) & 255];
#endif							/* HAVE__BUILTIN_CLZ */
}

/*
 * pg_leftmost_one_pos64
 *		As above, but for a 64-bit word.
 */
static inline int
pg_leftmost_one_pos64(uint64 word)
{
#ifdef HAVE__BUILTIN_CLZ
	Assert(word != 0);

#if defined(HAVE_LONG_INT_64)
	return 63 - __builtin_clzl(word);
#elif defined(HAVE_LONG_LONG_INT_64)
	return 63 - __builtin_clzll(word);
#else
#error must have a working 64-bit integer datatype
#endif							/* HAVE_LONG_INT_64 */

#elif defined(_MSC_VER) && (defined(_M_AMD64) || defined(_M_ARM64))
	unsigned long result;
	bool		non_zero;

	Assert(word != 0);

	non_zero = _BitScanReverse64(&result, word);
	return (int) result;
#else
	int			shift = 64 - 8;

	Assert(word != 0);

	while ((word >> shift) == 0)
		shift -= 8;

	return shift + pg_leftmost_one_pos[(word >> shift) & 255];
#endif							/* HAVE__BUILTIN_CLZ */
}

/*
 * pg_rightmost_one_pos32
 *		Returns the position of the least significant set bit in "word",
 *		measured from the least significant bit.  word must not be 0.
 */
static inline int
pg_rightmost_one_pos32(uint32 word)
{
#ifdef HAVE__BUILTIN_CTZ
	Assert(word != 0);

	return __builtin_ctz(word);
#elif defined(_MSC_VER)
	unsigned long result;
	bool		non_zero;

	Assert(word != 0);

	non_zero = _BitScanForward(&result, word);
	return (int) result;
#else
	int			result = 0;

	Assert(word != 0);

	while ((word & 255) == 0)
	{
		word >>= 8;
		result += 8;
	}
	result += pg_rightmost_one_pos[word & 255];
	return result;
#endif							/* HAVE__BUILTIN_CTZ */
}

/*
 * pg_rightmost_one_pos64
 *		As above, but for a 64-bit word.
 */
static inline int
pg_rightmost_one_pos64(uint64 word)
{
#ifdef HAVE__BUILTIN_CTZ
	Assert(word != 0);

#if defined(HAVE_LONG_INT_64)
	return __builtin_ctzl(word);
#elif defined(HAVE_LONG_LONG_INT_64)
	return __builtin_ctzll(word);
#else
#error must have a working 64-bit integer datatype
#endif							/* HAVE_LONG_INT_64 */

#elif defined(_MSC_VER) && (defined(_M_AMD64) || defined(_M_ARM64))
	unsigned long result;
	bool		non_zero;

	Assert(word != 0);

	non_zero = _BitScanForward64(&result, word);
	return (int) result;
#else
	int			result = 0;

	Assert(word != 0);

	while ((word & 255) == 0)
	{
		word >>= 8;
		result += 8;
	}
	result += pg_rightmost_one_pos[word & 255];
	return result;
#endif							/* HAVE__BUILTIN_CTZ */
}

/*
 * pg_nextpower2_32
 *		Returns the next higher power of 2 above 'num', or 'num' if it's
 *		already a power of 2.
 *
 * 'num' mustn't be 0 or be above PG_UINT32_MAX / 2 + 1.
 */
static inline uint32
pg_nextpower2_32(uint32 num)
{
	Assert(num > 0 && num <= PG_UINT32_MAX / 2 + 1);

	/*
	 * A power 2 number has only 1 bit set.  Subtracting 1 from such a number
	 * will turn on all previous bits resulting in no common bits being set
	 * between num and num-1.
	 */
	if ((num & (num - 1)) == 0)
		return num;				/* already power 2 */

	return ((uint32) 1) << (pg_leftmost_one_pos32(num) + 1);
}

/*
 * pg_nextpower2_64
 *		Returns the next higher power of 2 above 'num', or 'num' if it's
 *		already a power of 2.
 *
 * 'num' mustn't be 0 or be above PG_UINT64_MAX / 2  + 1.
 */
static inline uint64
pg_nextpower2_64(uint64 num)
{
	Assert(num > 0 && num <= PG_UINT64_MAX / 2 + 1);

	/*
	 * A power 2 number has only 1 bit set.  Subtracting 1 from such a number
	 * will turn on all previous bits resulting in no common bits being set
	 * between num and num-1.
	 */
	if ((num & (num - 1)) == 0)
		return num;				/* already power 2 */

	return ((uint64) 1) << (pg_leftmost_one_pos64(num) + 1);
}

/*
 * pg_prevpower2_32
 *		Returns the next lower power of 2 below 'num', or 'num' if it's
 *		already a power of 2.
 *
 * 'num' mustn't be 0.
 */
static inline uint32
pg_prevpower2_32(uint32 num)
{
	return ((uint32) 1) << pg_leftmost_one_pos32(num);
}

/*
 * pg_prevpower2_64
 *		Returns the next lower power of 2 below 'num', or 'num' if it's
 *		already a power of 2.
 *
 * 'num' mustn't be 0.
 */
static inline uint64
pg_prevpower2_64(uint64 num)
{
	return ((uint64) 1) << pg_leftmost_one_pos64(num);
}

/*
 * pg_ceil_log2_32
 *		Returns equivalent of ceil(log2(num))
 */
static inline uint32
pg_ceil_log2_32(uint32 num)
{
	if (num < 2)
		return 0;
	else
		return pg_leftmost_one_pos32(num - 1) + 1;
}

/*
 * pg_ceil_log2_64
 *		Returns equivalent of ceil(log2(num))
 */
static inline uint64
pg_ceil_log2_64(uint64 num)
{
	if (num < 2)
		return 0;
	else
		return pg_leftmost_one_pos64(num - 1) + 1;
}

/*
 * With MSVC on x86_64 builds, try using native popcnt instructions via the
 * __popcnt and __popcnt64 intrinsics.  These don't work the same as GCC's
 * __builtin_popcount* intrinsic functions as they always emit popcnt
 * instructions.
 */
#if defined(_MSC_VER) && defined(_M_AMD64)
#define HAVE_X86_64_POPCNTQ
#endif

/*
 * On x86_64, we can use the hardware popcount instruction, but only if
 * we can verify that the CPU supports it via the cpuid instruction.
 *
 * Otherwise, we fall back to a hand-rolled implementation.
 */
#ifdef HAVE_X86_64_POPCNTQ
#if defined(HAVE__GET_CPUID) || defined(HAVE__CPUID)
#define TRY_POPCNT_FAST 1
#endif
#endif

#ifdef TRY_POPCNT_FAST
/* Attempt to use the POPCNT instruction, but perform a runtime check first */
extern int	(*pg_popcount32) (uint32 word);
extern int	(*pg_popcount64) (uint64 word);

#else
/* Use a portable implementation -- no need for a function pointer. */
extern int	pg_popcount32(uint32 word);
extern int	pg_popcount64(uint64 word);

#endif							/* TRY_POPCNT_FAST */

/* Count the number of one-bits in a byte array */
extern uint64 pg_popcount(const char *buf, int bytes);

/*
 * Rotate the bits of "word" to the right/left by n bits.
 */
static inline uint32
pg_rotate_right32(uint32 word, int n)
{
	return (word >> n) | (word << (32 - n));
}

static inline uint32
pg_rotate_left32(uint32 word, int n)
{
	return (word << n) | (word >> (32 - n));
}

/* size_t variants of the above, as required */

#if SIZEOF_SIZE_T == 4
#define pg_leftmost_one_pos_size_t pg_leftmost_one_pos32
#define pg_nextpower2_size_t pg_nextpower2_32
#define pg_prevpower2_size_t pg_prevpower2_32
#else
#define pg_leftmost_one_pos_size_t pg_leftmost_one_pos64
#define pg_nextpower2_size_t pg_nextpower2_64
#define pg_prevpower2_size_t pg_prevpower2_64
#endif


/* integer division speedups for constant but runtime divisors */

/*
 * This value should cached globally and used in the other routines to find
 * the div/mod quickly relative to `div` operand.  TODO: might have some other
 * asm-tuned things in port maybe?  general-purpose solution should be ok
 * though.
 */
static inline uint64 pg_fastinverse(uint32 divisor)
{
	return UINT64_C(0xFFFFFFFFFFFFFFFF) / divisor + 1;
}

/*
 * pg_fastdiv - calculates the quotient of a 32-bit number against a constant
 * divisor without using the division operator
 */
static inline uint32 pg_fastdiv(uint32 n, uint32 divisor, uint64 fastinv)
{
#ifdef HAVE_INT128
	return (((uint128)(fastinv - 1) * n)) >> 64;
#else
	return n / divisor;
#endif
}

/*
 * pg_fastmod - calculates the modulus of a 32-bit number against a constant
 * divisor without using the division operator
 */
static inline uint32 pg_fastmod(uint32 n, uint32 divisor, uint64 fastinv)
{
#ifdef HAVE_INT128
	uint64_t lowbits = fastinv * n;
	return ((uint128)lowbits * divisor) >> 64;
#else
	return n % divisor;
#endif
}

#endif							/* PG_BITUTILS_H */
